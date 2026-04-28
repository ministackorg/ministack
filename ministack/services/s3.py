"""
S3 Service Emulator – AWS-compatible.
Supports: CreateBucket, DeleteBucket, ListBuckets, HeadBucket,
          PutObject, GetObject, DeleteObject, HeadObject, CopyObject,
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
import copy
import datetime as _dt
import hashlib
import json
import logging
import os
import re
import threading
import time
from urllib.parse import quote as url_quote, unquote as url_unquote, parse_qs as _parse_qs
from defusedxml.ElementTree import fromstring
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.sax.saxutils import escape as _esc

from ministack.core.persistence import load_state, PERSIST_STATE
from ministack.core.responses import (
    AccountScopedDict,
    get_account_id,
    md5_hash,
    sha256_hash,
    now_iso,
    new_uuid,
    iso_to_rfc7231,
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
_object_versions = AccountScopedDict()  # (bucket, key) -> [{version_id, obj_record}, ...]

_bucket_object_lock = AccountScopedDict()
_bucket_replication = AccountScopedDict()
_object_retention = AccountScopedDict()
_object_legal_hold = AccountScopedDict()

_multipart_uploads = AccountScopedDict()

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
)

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
                return v.get("data")
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
    return url_quote(value, safe="")


def _parse_bucket_key(path: str, headers: dict):
    # Vhost extraction lives in app.py:_extract_s3_vhost_bucket, which
    # rewrites the path to /{bucket}{key} before this handler runs. By the
    # time we get here, every request is path-style.
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


def _validate_content_md5(headers: dict, body: bytes):
    md5_header = headers.get("content-md5", "")
    if not md5_header:
        return None
    try:
        expected = base64.b64decode(md5_header)
    except Exception:
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


def _find_xml_tag(parent, tag_name, ns=S3_NS):
    el = parent.find("{%s}%s" % (ns, tag_name))
    if el is None:
        el = parent.find(tag_name)
    return el


def _parse_tags_xml(body: bytes) -> dict:
    xml_root = fromstring(body)
    tags = {}
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
                tags[key_text] = val_text or ""
    return tags


def _extract_user_metadata(headers: dict) -> dict:
    meta = {}
    for k, v in headers.items():
        if k.lower().startswith("x-amz-meta-"):
            meta[k] = v
    return meta


def _build_object_record(body: bytes, headers: dict, etag: str = None) -> dict:
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
    }


def _object_response_headers(obj: dict, bucket_name: str = "", key: str = "") -> dict:
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
    if bucket_name and key:
        retention = _object_retention.get((bucket_name, key))
        if retention:
            h["x-amz-object-lock-mode"] = retention["Mode"]
            h["x-amz-object-lock-retain-until-date"] = retention["RetainUntilDate"]
        hold = _object_legal_hold.get((bucket_name, key))
        if hold:
            h["x-amz-object-lock-legal-hold"] = hold
    return h


# ---------------------------------------------------------------------------
# Request router
# ---------------------------------------------------------------------------


async def handle_request(
    method: str, path: str, headers: dict, body: bytes, query_params: dict
) -> tuple:
    bucket, key = _parse_bucket_key(path, headers)

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
                return _get_object_tagging(bucket, key)
            if "retention" in query_params:
                return _get_object_retention(bucket, key)
            if "legal-hold" in query_params:
                return _get_object_legal_hold(bucket, key)
            return _get_object(bucket, key, headers, query_params)

        if method == "PUT":
            if "partNumber" in query_params and "uploadId" in query_params:
                if "x-amz-copy-source" in headers:
                    return _upload_part_copy(bucket, key, query_params, headers)
                return _upload_part(bucket, key, body, query_params, headers)
            if "tagging" in query_params:
                return _put_object_tagging(bucket, key, body)
            if "retention" in query_params:
                return _put_object_retention(bucket, key, body, headers)
            if "legal-hold" in query_params:
                return _put_object_legal_hold(bucket, key, body)
            if "x-amz-copy-source" in headers:
                return _copy_object(bucket, key, headers)
            return _put_object(bucket, key, body, headers)

        if method == "POST":
            if "uploads" in query_params:
                return _create_multipart_upload(bucket, key, headers)
            if "uploadId" in query_params:
                return _complete_multipart_upload(bucket, key, body, query_params)
            return _error(
                "MethodNotAllowed",
                "The specified method is not allowed against this resource.",
                405,
            )

        if method == "HEAD":
            return _head_object(bucket, key)

        if method == "DELETE":
            if "uploadId" in query_params:
                return _abort_multipart_upload(bucket, key, query_params)
            if "tagging" in query_params:
                return _delete_object_tagging(bucket, key)
            return _delete_object(bucket, key, headers)

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
            return _put_bucket_acl(bucket, body)
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


def _list_buckets():
    root = Element("ListAllMyBucketsResult", xmlns=S3_NS)
    owner = SubElement(root, "Owner")
    SubElement(owner, "ID").text = "owner-id"
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
    if name in _buckets:
        # Idempotent: same account already owns it — return 200 like real AWS
        return 200, {"Location": f"/{name}"}, b""

    region = None
    if body:
        try:
            xml_root = fromstring(body)
            loc_el = _find_xml_tag(xml_root, "LocationConstraint")
            if loc_el is not None and loc_el.text:
                region = loc_el.text
        except Exception:
            pass

    _buckets[name] = {"created": now_iso(), "objects": {}, "region": region}

    if headers.get("x-amz-bucket-object-lock-enabled", "").lower() == "true":
        _bucket_object_lock[name] = {"enabled": True, "default_retention": None}
        _bucket_versioning[name] = "Enabled"

    if S3_PERSIST:
        os.makedirs(os.path.join(DATA_DIR, name), exist_ok=True)
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
    for k in [k for k in _object_retention if k[0] == name]:
        del _object_retention[k]
    for k in [k for k in _object_legal_hold if k[0] == name]:
        del _object_legal_hold[k]
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
    region = _buckets[name].get("region")
    # AWS returns empty LocationConstraint for us-east-1.
    if region and region != os.environ.get("MINISTACK_REGION", "us-east-1"):
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
    return _error(
        "ServerSideEncryptionConfigurationNotFoundError",
        "The server side encryption configuration was not found",
        404,
        f"/{name}",
    )


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
            if "And" in filt:
                xml += "<And>"
                if "Prefix" in filt["And"]:
                    xml += f"<Prefix>{_esc(filt['And']['Prefix'])}</Prefix>"
                for tag in filt["And"].get("Tags", []):
                    xml += f"<Tag><Key>{_esc(tag['Key'])}</Key><Value>{_esc(tag['Value'])}</Value></Tag>"
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
            _lc_text = lambda el, tag: (el.findtext(tag) or el.findtext(f"s3:{tag}", namespaces=ns) or "")
            _lc_find = lambda el, tag: (el.find(tag) or el.find(f"s3:{tag}", ns))
            _lc_findall = lambda el, tag: (el.findall(tag) or el.findall(f"s3:{tag}", ns))
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
    body = (
        XML_DECL + b"\n"
        b'<AccessControlPolicy xmlns="' + S3_NS.encode() + b'">'
        b"<Owner><ID>owner-id</ID><DisplayName>ministack</DisplayName></Owner>"
        b"<AccessControlList><Grant>"
        b'<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">'
        b"<ID>owner-id</ID><DisplayName>ministack</DisplayName></Grantee>"
        b"<Permission>FULL_CONTROL</Permission>"
        b"</Grant></AccessControlList></AccessControlPolicy>"
    )
    return 200, {"Content-Type": "application/xml"}, body


def _put_bucket_acl(name: str, body: bytes):
    if name not in _buckets:
        return _no_such_bucket(name)
    if body:
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
    return 200, {}, b""


def _get_bucket_ownership_controls(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    stored = _buckets[name].get("_ownership_controls")
    if stored:
        return 200, {"Content-Type": "application/xml"}, stored
    root = Element("OwnershipControls", xmlns=S3_NS)
    rule = SubElement(root, "Rule")
    SubElement(rule, "ObjectOwnership").text = "BucketOwnerEnforced"
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _delete_bucket_ownership_controls(name: str):
    if name not in _buckets:
        return _no_such_bucket(name)
    _buckets[name].pop("_ownership_controls", None)
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
    # Default: all public access blocked
    root = Element("PublicAccessBlockConfiguration", xmlns=S3_NS)
    SubElement(root, "BlockPublicAcls").text = "true"
    SubElement(root, "IgnorePublicAcls").text = "true"
    SubElement(root, "BlockPublicPolicy").text = "true"
    SubElement(root, "RestrictPublicBuckets").text = "true"
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


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
    _bucket_notifications[name] = body.decode("utf-8", errors="replace")
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

    # Collect all keys: from objects AND from version history (deleted objects)
    all_keys = set(k for k in bucket["objects"] if k.startswith(prefix) and k > key_marker)
    for (bn, k) in _object_versions:
        if bn == bucket_name and k.startswith(prefix) and k > key_marker:
            all_keys.add(k)
    keys = sorted(all_keys)

    is_truncated = False
    SubElement(root, "IsTruncated").text = "false"

    count = 0
    for k in keys:
        if count >= max_keys:
            is_truncated = True
            break
        vkey = (bucket_name, k)
        versions = _object_versions.get(vkey)
        if versions:
            # Return all stored versions (newest first)
            for v in reversed(versions):
                if count >= max_keys:
                    is_truncated = True
                    break
                if v.get("is_delete_marker"):
                    dm = SubElement(root, "DeleteMarker")
                    SubElement(dm, "Key").text = k
                    SubElement(dm, "VersionId").text = v["version_id"]
                    SubElement(dm, "IsLatest").text = "true" if v["is_latest"] else "false"
                    SubElement(dm, "LastModified").text = v["last_modified"]
                    owner = SubElement(dm, "Owner")
                    SubElement(owner, "ID").text = "owner-id"
                    SubElement(owner, "DisplayName").text = "ministack"
                else:
                    ver = SubElement(root, "Version")
                    SubElement(ver, "Key").text = k
                    SubElement(ver, "VersionId").text = v["version_id"]
                    SubElement(ver, "IsLatest").text = "true" if v["is_latest"] else "false"
                    SubElement(ver, "LastModified").text = v["last_modified"]
                    SubElement(ver, "ETag").text = v["etag"]
                    SubElement(ver, "Size").text = str(v["size"])
                    SubElement(ver, "StorageClass").text = "STANDARD"
                    owner = SubElement(ver, "Owner")
                    SubElement(owner, "ID").text = "owner-id"
                    SubElement(owner, "DisplayName").text = "ministack"
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
            SubElement(ver, "StorageClass").text = "STANDARD"
            owner = SubElement(ver, "Owner")
            SubElement(owner, "ID").text = "owner-id"
            SubElement(owner, "DisplayName").text = "ministack"
            count += 1

    # Update IsTruncated after actual count
    root.find("IsTruncated").text = "true" if is_truncated else "false"

    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


# ---------------------------------------------------------------------------
# S3 Event Notifications
# ---------------------------------------------------------------------------


def _parse_notification_config(bucket_name: str) -> list[dict]:
    """Parse the raw notification XML into structured config dicts."""
    raw = _bucket_notifications.get(bucket_name)
    if not raw:
        return []

    try:
        root = fromstring(raw)
    except Exception:
        return []

    configs: list[dict] = []

    _CONFIG_MAP = {
        "QueueConfiguration": ("sqs", ("Queue",)),
        "TopicConfiguration": ("sns", ("Topic",)),
        "CloudFunctionConfiguration": ("lambda", ("CloudFunction", "Function")),
        "LambdaFunctionConfiguration": ("lambda", ("Function", "CloudFunction")),
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


def _fire_s3_event(
    bucket_name: str, key: str, event_name: str, size: int = 0, etag: str = ""
) -> None:
    """Build and deliver an S3 event notification. Best-effort — errors are logged."""
    try:
        configs = _parse_notification_config(bucket_name)
        raw_xml = _bucket_notifications.get(bucket_name, "")
        has_eventbridge = "EventBridgeConfiguration" in raw_xml
        if not configs and not has_eventbridge:
            return

        short_event = event_name.replace("s3:", "", 1)
        event_time = now_iso()
        request_id = new_uuid()
        clean_etag = etag.strip('"')

        event_payload = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": os.environ.get("MINISTACK_REGION", "us-east-1"),
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
                    _deliver_event_to_sqs(cfg["arn"], payload)
                elif cfg["type"] == "sns":
                    _deliver_event_to_sns(cfg["arn"], payload)
                elif cfg["type"] == "lambda":
                    _deliver_event_to_lambda(cfg["arn"], payload)
            except Exception:
                logger.exception(
                    "S3 notification delivery failed for config %s", cfg.get("id")
                )

        # S3 → EventBridge delivery (if EventBridgeConfiguration is enabled)
        try:
            if has_eventbridge:
                from ministack.services import eventbridge as _eb
                eb_event = {
                    "EventId": request_id,
                    "Source": "aws.s3",
                    "DetailType": event_name.replace("s3:", "Object ").replace(":", " ").replace("*", ""),
                    "Detail": json.dumps({
                        "version": "0",
                        "bucket": {"name": bucket_name},
                        "object": {"key": key, "size": size, "etag": clean_etag, "sequencer": "0"},
                        "request-id": request_id,
                        "requester": get_account_id(),
                        "source-ip-address": "127.0.0.1",
                        "reason": "PutObject",
                    }),
                    "EventBusName": "default",
                    "Time": event_time,
                    "Resources": [f"arn:aws:s3:::{bucket_name}"],
                    "Account": get_account_id(),
                    "Region": os.environ.get("MINISTACK_REGION", "us-east-1"),
                }
                _eb._dispatch_event(eb_event)
                logger.debug("S3→EventBridge: %s for %s/%s", event_name, bucket_name, key)
        except Exception:
            logger.exception("S3→EventBridge delivery failed for %s/%s", bucket_name, key)

    except Exception:
        logger.exception(
            "S3 event notification fire failed for %s/%s", bucket_name, key
        )


def _deliver_event_to_sqs(arn: str, event_payload: dict) -> None:
    from ministack.services import sqs as _sqs

    queue_name = arn.rsplit(":", 1)[-1]
    queue_url = _sqs._queue_url(queue_name)
    queue = _sqs._queues.get(queue_url)
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


def _deliver_event_to_sns(arn: str, event_payload: dict) -> None:
    from ministack.services import sns as _sns

    topic = _sns._topics.get(arn)
    if not topic:
        logger.warning("S3 notification: SNS topic %s not found", arn)
        return

    message = json.dumps(event_payload)
    msg_id = new_uuid()
    _sns._fanout(arn, msg_id, message, subject="Amazon S3 Notification")
    logger.info("S3 notification → SNS %s", arn)


def _deliver_event_to_lambda(arn: str, event_payload: dict) -> None:
    from ministack.services import lambda_svc as _lambda

    func_name = arn.rsplit(":", 1)[-1]
    func = _lambda._functions.get(func_name)
    if not func:
        logger.warning("S3 notification: Lambda function %s not found", func_name)
        return

    # Real S3 → Lambda uses asynchronous invocation, which means retries
    # (MaximumRetryAttempts, default 2) and routing to the function's DLQ /
    # DestinationConfig.OnFailure on final failure. Shared helper keeps the
    # semantics identical to direct Invoke(InvocationType=Event).
    _lambda.invoke_async_with_retry(func, event_payload)
    logger.info("S3 notification → Lambda %s (async with retry+DLQ)", func_name)


def _fire_s3_event_async(
    bucket_name: str, key: str, event_name: str, size: int = 0, etag: str = ""
) -> None:
    """Fire S3 event notification in a background thread (non-blocking)."""
    if bucket_name not in _bucket_notifications:
        return
    t = threading.Thread(
        target=_fire_s3_event,
        args=(bucket_name, key, event_name, size, etag),
        daemon=True,
    )
    t.start()


def _fire_s3_test_event(bucket_name: str) -> None:
    """Deliver an s3:TestEvent to every destination in the bucket notification config."""
    try:
        configs = _parse_notification_config(bucket_name)
        if not configs:
            return
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
                    _deliver_event_to_sqs(cfg["arn"], payload)
                elif cfg["type"] == "sns":
                    _deliver_event_to_sns(cfg["arn"], payload)
                elif cfg["type"] == "lambda":
                    _deliver_event_to_lambda(cfg["arn"], payload)
            except Exception:
                logger.exception(
                    "S3 test-event delivery failed for config %s", cfg.get("id")
                )
    except Exception:
        logger.exception("S3 test-event fire failed for %s", bucket_name)


def _fire_s3_test_event_async(bucket_name: str) -> None:
    """Fire s3:TestEvent in a background thread (non-blocking)."""
    t = threading.Thread(target=_fire_s3_test_event, args=(bucket_name,), daemon=True)
    t.start()


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

    etag = f'"{md5_hash(body)}"'
    obj = _build_object_record(body, headers, etag=etag)
    bucket["objects"][key] = obj

    # --- Object Lock headers on PutObject ---
    _apply_object_lock_from_headers(bucket_name, key, headers)

    # --- x-amz-tagging header on PutObject ---
    tagging_header = headers.get("x-amz-tagging", "")
    if tagging_header:
        tags = {k: v[0] for k, v in _parse_qs(tagging_header).items()}
        if len(tags) > 10:
            return _error("BadRequest", "Object tags cannot be greater than 10", 400)
        _object_tags[(bucket_name, key)] = tags

    if S3_PERSIST:
        _persist_object(bucket_name, key, obj)

    _fire_s3_event_async(
        bucket_name, key, "s3:ObjectCreated:Put", size=obj["size"], etag=obj["etag"]
    )

    resp_headers = {"ETag": obj["etag"], "Content-Length": "0"}
    if _bucket_versioning.get(bucket_name) in ("Enabled", "Suspended"):
        version_id = new_uuid()
        obj["version_id"] = version_id
        resp_headers["x-amz-version-id"] = version_id
        vkey = (bucket_name, key)
        if vkey not in _object_versions:
            _object_versions[vkey] = []
        _object_versions[vkey].append({
            "version_id": version_id,
            "last_modified": obj["last_modified"],
            "etag": obj["etag"],
            "size": obj["size"],
            "is_latest": True,
            "data": body,
        })
        # Mark all previous versions as not latest
        for v in _object_versions[vkey][:-1]:
            v["is_latest"] = False
    return 200, resp_headers, b""


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


def _get_object(bucket_name: str, key: str, headers: dict, query_params: dict = None):
    query_params = query_params or {}
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    version_id = _qp(query_params, "versionId", "")
    if version_id:
        vkey = (bucket_name, key)
        versions = _object_versions.get(vkey, [])
        for v in versions:
            if v["version_id"] == version_id:
                resp_headers = {
                    "Content-Type": "application/octet-stream",
                    "ETag": v["etag"],
                    "Content-Length": str(v["size"]),
                    "Last-Modified": v["last_modified"],
                    "x-amz-version-id": v["version_id"],
                }
                return 200, resp_headers, v["data"]
        return _error("NoSuchVersion", "The specified version does not exist.", 404, f"/{bucket_name}/{key}")

    if key not in bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{bucket_name}/{key}",
        )

    obj = bucket["objects"][key]
    resp_headers = _object_response_headers(obj, bucket_name, key)

    range_header = headers.get("range", "")
    body = _read_body(bucket_name, key, obj)
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
        return 206, resp_headers, slice_body

    return 200, resp_headers, body


def _range_error_xml(bucket_name: str, key: str) -> Element:
    root = Element("Error")
    SubElement(root, "Code").text = "InvalidRange"
    SubElement(root, "Message").text = "The requested range is not satisfiable"
    SubElement(root, "Resource").text = f"/{bucket_name}/{key}"
    SubElement(root, "RequestId").text = new_uuid()
    return root


def _head_object(bucket_name: str, key: str):
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

    obj = bucket["objects"][key]
    return 200, _object_response_headers(obj, bucket_name, key), b""


def _delete_object(bucket_name: str, key: str, headers: dict | None = None):
    headers = headers or {}
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    if key in bucket["objects"]:
        lock_err = _check_object_lock(bucket_name, key, headers)
        if lock_err:
            return lock_err

    versioning = _bucket_versioning.get(bucket_name, "")
    if versioning in ("Enabled", "Suspended"):
        # Add a delete marker instead of removing version history
        delete_marker_id = new_uuid()
        vkey = (bucket_name, key)
        if vkey not in _object_versions:
            _object_versions[vkey] = []
        # Mark all previous versions as not latest
        for v in _object_versions[vkey]:
            v["is_latest"] = False
        _object_versions[vkey].append({
            "version_id": delete_marker_id,
            "last_modified": now_iso(),
            "etag": "",
            "size": 0,
            "is_latest": True,
            "is_delete_marker": True,
        })
        existed = key in bucket["objects"]
        bucket["objects"].pop(key, None)
        if existed:
            _fire_s3_event_async(bucket_name, key, "s3:ObjectRemoved:Delete")
        return 204, {"x-amz-delete-marker": "true", "x-amz-version-id": delete_marker_id}, b""

    existed = key in bucket["objects"]
    bucket["objects"].pop(key, None)
    _object_tags.pop((bucket_name, key), None)
    _object_retention.pop((bucket_name, key), None)
    _object_legal_hold.pop((bucket_name, key), None)
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


def _copy_object(bucket_name: str, dest_key: str, headers: dict):
    source = url_unquote(headers.get("x-amz-copy-source", "").lstrip("/"))
    src_parts = source.split("?", 1)[0].split("/", 1)
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
    if src_key not in src_bucket["objects"]:
        return _error(
            "NoSuchKey",
            "The specified key does not exist.",
            404,
            f"/{src_bucket_name}/{src_key}",
        )

    dest_bucket = _ensure_bucket(bucket_name)
    if dest_bucket is None:
        return _no_such_bucket(bucket_name)

    src_obj = src_bucket["objects"][src_key]

    # Precondition: x-amz-copy-source-if-match
    if_match = headers.get("x-amz-copy-source-if-match", "")
    if if_match and if_match.strip('"') != src_obj["etag"].strip('"'):
        return _error(
            "PreconditionFailed",
            "At least one of the pre-conditions you specified did not hold",
            412,
        )

    # Precondition: x-amz-copy-source-if-none-match — 412 for PUT-like operations.
    if_none_match = headers.get("x-amz-copy-source-if-none-match", "")
    if if_none_match and if_none_match.strip('"') == src_obj["etag"].strip('"'):
        return _error(
            "PreconditionFailed",
            "At least one of the pre-conditions you specified did not hold",
            412,
        )

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

    new_etag = src_obj["etag"]
    last_modified = now_iso()
    src_body = _read_body(src_bucket_name, src_key, src_obj)
    dest_obj = {
        "body": src_body,
        "content_type": content_type,
        "content_encoding": content_encoding,
        "etag": new_etag,
        "last_modified": last_modified,
        "size": src_obj["size"],
        "metadata": metadata,
        "preserved_headers": preserved,
    }
    dest_bucket["objects"][dest_key] = dest_obj

    # --- Preserve / replace tags ---
    tagging_directive = headers.get("x-amz-tagging-directive", "COPY").upper()
    if tagging_directive == "REPLACE":
        tagging_header = headers.get("x-amz-tagging", "")
        if tagging_header:
            _object_tags[(bucket_name, dest_key)] = {
                k: v[0] for k, v in _parse_qs(tagging_header).items()
            }
        else:
            _object_tags.pop((bucket_name, dest_key), None)
    else:
        src_tags = _object_tags.get((src_bucket_name, src_key))
        if src_tags:
            _object_tags[(bucket_name, dest_key)] = dict(src_tags)
        else:
            _object_tags.pop((bucket_name, dest_key), None)

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

    if S3_PERSIST:
        _persist_object(bucket_name, dest_key, dest_obj)

    _fire_s3_event_async(
        bucket_name,
        dest_key,
        "s3:ObjectCreated:Copy",
        size=dest_obj["size"],
        etag=new_etag,
    )

    resp_headers = {"Content-Type": "application/xml"}
    if _bucket_versioning.get(bucket_name) in ("Enabled", "Suspended"):
        version_id = new_uuid()
        dest_obj["version_id"] = version_id
        resp_headers["x-amz-version-id"] = version_id
        vkey = (bucket_name, dest_key)
        if vkey not in _object_versions:
            _object_versions[vkey] = []
        _object_versions[vkey].append({
            "version_id": version_id,
            "last_modified": dest_obj["last_modified"],
            "etag": dest_obj["etag"],
            "size": dest_obj["size"],
            "is_latest": True,
            "data": src_body,
        })
        for v in _object_versions[vkey][:-1]:
            v["is_latest"] = False

    root = Element("CopyObjectResult", xmlns=S3_NS)
    SubElement(root, "LastModified").text = last_modified
    SubElement(root, "ETag").text = new_etag
    return 200, resp_headers, _xml_body(root)


# ---------------------------------------------------------------------------
# Object tagging
# ---------------------------------------------------------------------------


def _get_object_tagging(bucket_name: str, key: str):
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

    tags = _object_tags.get((bucket_name, key), {})
    root = Element("Tagging", xmlns=S3_NS)
    tag_set = SubElement(root, "TagSet")
    for k, v in tags.items():
        tag = SubElement(tag_set, "Tag")
        SubElement(tag, "Key").text = k
        SubElement(tag, "Value").text = v
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _put_object_tagging(bucket_name: str, key: str, body: bytes):
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
    _object_tags[(bucket_name, key)] = tags
    return 200, {"Content-Type": "application/xml"}, b""


def _delete_object_tagging(bucket_name: str, key: str):
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
    _object_tags.pop((bucket_name, key), None)
    return 204, {}, b""


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
    """Walk sorted keys, collecting contents and common prefixes with correct
    pagination.  When a key falls under a common-prefix group the iterator
    advances past *all* remaining keys in that group so the next page's
    marker cleanly skips the entire prefix.

    Returns (contents, common_prefixes, is_truncated, next_marker).
    """
    all_keys = sorted(
        k for k in bucket_objects if k.startswith(prefix) and k > start_after
    )
    contents: list[str] = []
    common_prefixes: set[str] = set()
    is_truncated = False
    count = 0
    next_marker = ""

    i = 0
    while i < len(all_keys):
        k = all_keys[i]

        if delimiter:
            suffix = k[len(prefix) :]
            delim_idx = suffix.find(delimiter)
            if delim_idx >= 0:
                cp = prefix + suffix[: delim_idx + len(delimiter)]
                is_new_prefix = cp not in common_prefixes
                if is_new_prefix:
                    if count >= max_keys:
                        is_truncated = True
                        break
                    common_prefixes.add(cp)
                    count += 1
                # Advance past every remaining key belonging to this prefix
                # group so the marker lands after the whole group.
                next_marker = k
                i += 1
                while i < len(all_keys) and all_keys[i].startswith(cp):
                    next_marker = all_keys[i]
                    i += 1
                continue

        if count >= max_keys:
            is_truncated = True
            break
        contents.append(k)
        count += 1
        next_marker = k
        i += 1

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
        SubElement(c, "StorageClass").text = "STANDARD"
        owner = SubElement(c, "Owner")
        SubElement(owner, "ID").text = "owner-id"
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
        SubElement(c, "StorageClass").text = "STANDARD"
        if fetch_owner:
            owner = SubElement(c, "Owner")
            SubElement(owner, "ID").text = "owner-id"
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

    deleted_keys: list[str] = []
    errors: list[tuple] = []
    for obj_el in list(xml_root.findall("{%s}Object" % S3_NS)) or list(
        xml_root.findall("Object")
    ):
        key_el = _find_xml_tag(obj_el, "Key")
        if key_el is not None and key_el.text:
            k = key_el.text
            if k in bucket["objects"]:
                lock_err = _check_object_lock(bucket_name, k, headers)
                if lock_err:
                    errors.append(
                        (
                            k,
                            "AccessDenied",
                            "Access Denied because object protected by object lock.",
                        )
                    )
                    continue
            bucket["objects"].pop(k, None)
            _object_tags.pop((bucket_name, k), None)
            _object_retention.pop((bucket_name, k), None)
            _object_legal_hold.pop((bucket_name, k), None)
            deleted_keys.append(k)

    resp = Element("DeleteResult", xmlns=S3_NS)
    if not quiet:
        for k in deleted_keys:
            d = SubElement(resp, "Deleted")
            SubElement(d, "Key").text = k
    for k, code, msg in errors:
        e = SubElement(resp, "Error")
        SubElement(e, "Key").text = k
        SubElement(e, "Code").text = code
        SubElement(e, "Message").text = msg

    return 200, {"Content-Type": "application/xml"}, _xml_body(resp)


# ---------------------------------------------------------------------------
# Multipart upload
# ---------------------------------------------------------------------------


def _create_multipart_upload(bucket_name: str, key: str, headers: dict):
    bucket = _ensure_bucket(bucket_name)
    if bucket is None:
        return _no_such_bucket(bucket_name)

    upload_id = new_uuid()
    content_type = headers.get("content-type", "application/octet-stream")
    content_encoding = headers.get("content-encoding")
    metadata = _extract_user_metadata(headers)
    preserved = {}
    for h in _PRESERVED_HEADERS:
        val = headers.get(h)
        if val is not None:
            preserved[h] = val

    _multipart_uploads[upload_id] = {
        "bucket": bucket_name,
        "key": key,
        "parts": {},
        "metadata": metadata,
        "content_type": content_type,
        "content_encoding": content_encoding,
        "preserved_headers": preserved,
        "created": now_iso(),
    }

    root = Element("InitiateMultipartUploadResult", xmlns=S3_NS)
    SubElement(root, "Bucket").text = bucket_name
    SubElement(root, "Key").text = key
    SubElement(root, "UploadId").text = upload_id
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


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

    etag = f'"{md5_hash(body)}"'
    upload["parts"][pn] = {
        "body": body,
        "etag": etag,
        "size": len(body),
        "last_modified": now_iso(),
    }
    return 200, {"ETag": etag}, b""


def _upload_part_copy(bucket_name: str, dest_key: str, query_params: dict, headers: dict):
    """UploadPartCopy — copy a range from an existing object as a multipart part."""
    upload_id = _qp(query_params, "uploadId")
    part_number = int(_qp(query_params, "partNumber", "1"))

    if upload_id not in _multipart_uploads:
        return _error("NoSuchUpload", "The specified multipart upload does not exist.", 404)

    source = url_unquote(headers.get("x-amz-copy-source", "").lstrip("/"))
    src_parts = source.split("?", 1)[0].split("/", 1)
    if len(src_parts) < 2:
        return _error("InvalidArgument", "Copy Source must mention the source bucket and key", 400)

    src_bucket_name, src_key = src_parts
    src_bucket = _ensure_bucket(src_bucket_name)
    if src_bucket is None:
        return _no_such_bucket(src_bucket_name)
    if src_key not in src_bucket["objects"]:
        return _error("NoSuchKey", "The specified key does not exist.", 404)

    src_obj = src_bucket["objects"][src_key]
    src_body = _read_body(src_bucket_name, src_key, src_obj)

    # Handle x-amz-copy-source-range
    copy_range = headers.get("x-amz-copy-source-range", "")
    if copy_range and copy_range.startswith("bytes="):
        rng = copy_range[6:]
        start, end = rng.split("-")
        src_body = src_body[int(start):int(end) + 1]

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
    return 200, {"Content-Type": "application/xml"}, _xml_body(root)


def _complete_multipart_upload(
    bucket_name: str, key: str, body: bytes, query_params: dict
):
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

    xml_root = fromstring(body)
    ordered_parts: list[tuple[int, str | None]] = []
    for part_el in xml_root.iter():
        local = part_el.tag.split("}")[-1] if "}" in part_el.tag else part_el.tag
        if local == "Part":
            pn_text = etag_text = None
            for child in part_el:
                child_local = (
                    child.tag.split("}")[-1] if "}" in child.tag else child.tag
                )
                if child_local == "PartNumber":
                    pn_text = child.text
                elif child_local == "ETag":
                    etag_text = child.text
            if pn_text is not None:
                ordered_parts.append((int(pn_text), etag_text))

    ordered_parts.sort(key=lambda x: x[0])

    md5_digests = b""
    combined = b""
    for pn, req_etag in ordered_parts:
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
        md5_digests += hashlib.md5(stored["body"]).digest()
        combined += stored["body"]

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
    }
    bucket["objects"][key] = obj

    if S3_PERSIST:
        _persist_object(bucket_name, key, obj)

    del _multipart_uploads[upload_id]

    _fire_s3_event_async(
        bucket_name,
        key,
        "s3:ObjectCreated:CompleteMultipartUpload",
        size=obj["size"],
        etag=final_etag,
    )

    resp_headers = {"Content-Type": "application/xml"}
    if _bucket_versioning.get(bucket_name) in ("Enabled", "Suspended"):
        version_id = new_uuid()
        obj["version_id"] = version_id
        resp_headers["x-amz-version-id"] = version_id
        vkey = (bucket_name, key)
        if vkey not in _object_versions:
            _object_versions[vkey] = []
        _object_versions[vkey].append({
            "version_id": version_id,
            "last_modified": obj["last_modified"],
            "etag": obj["etag"],
            "size": obj["size"],
            "is_latest": True,
            "data": combined,
        })
        for v in _object_versions[vkey][:-1]:
            v["is_latest"] = False

    root = Element("CompleteMultipartUploadResult", xmlns=S3_NS)
    s3_host = os.environ.get("MINISTACK_HOST", os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566"))
    SubElement(root, "Location").text = f"{s3_host}/{bucket_name}/{key}"
    SubElement(root, "Bucket").text = bucket_name
    SubElement(root, "Key").text = key
    SubElement(root, "ETag").text = final_etag
    return 200, resp_headers, _xml_body(root)


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
        SubElement(initiator, "ID").text = "owner-id"
        SubElement(initiator, "DisplayName").text = "ministack"
        owner = SubElement(u, "Owner")
        SubElement(owner, "ID").text = "owner-id"
        SubElement(owner, "DisplayName").text = "ministack"
        SubElement(u, "StorageClass").text = "STANDARD"
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
    SubElement(initiator, "ID").text = "owner-id"
    SubElement(initiator, "DisplayName").text = "ministack"
    owner = SubElement(root, "Owner")
    SubElement(owner, "ID").text = "owner-id"
    SubElement(owner, "DisplayName").text = "ministack"
    SubElement(root, "StorageClass").text = "STANDARD"
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
            }


_load_persisted_data()


def reset():
    """Wipe all in-memory state (used by /_ministack/reset)."""
    global _buckets, _bucket_policies, _bucket_notifications, _bucket_tags
    global _bucket_versioning, _bucket_encryption, _bucket_lifecycle, _bucket_cors
    global _bucket_acl, _bucket_websites, _bucket_logging_config
    global _bucket_accelerate_config, _bucket_request_payment_config
    global _object_tags, _multipart_uploads, _object_versions
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
        _multipart_uploads,
        _bucket_object_lock,
        _bucket_replication,
        _object_retention,
        _object_legal_hold,
        _object_versions,
    ):
        d.clear()
