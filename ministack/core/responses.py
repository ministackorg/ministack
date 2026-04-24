"""
AWS Response formatting utilities.
Handles XML responses (S3, SQS, SNS, IAM, STS, CloudWatch) and
JSON responses (DynamoDB, Lambda, SecretsManager, CloudWatch Logs).
"""

import contextvars
import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement, tostring

# Request-scoped account ID for multi-tenancy.
# Set per-request in app.py from the Authorization header.
_request_account_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "_request_account_id",
    default=os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000"),
)

_12_DIGIT_RE = re.compile(r"^\d{12}$")


def set_request_account_id(access_key_id: str) -> None:
    """Set the account ID for the current request from the access key.
    If the access key is a 12-digit number, use it as the account ID.
    Otherwise fall back to the MINISTACK_ACCOUNT_ID env var or 000000000000."""
    if access_key_id and _12_DIGIT_RE.match(access_key_id):
        _request_account_id.set(access_key_id)
    else:
        _request_account_id.set(
            os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
        )


def get_account_id() -> str:
    """Return the account ID for the current request."""
    return _request_account_id.get()


# Request-scoped region. Set per-request in app.py from the SigV4 Authorization
# header's Credential scope. Fixes #398 (CDK bootstrap resources inheriting the
# wrong region when MINISTACK_REGION differs from the caller's AWS_REGION).
_request_region: contextvars.ContextVar[str] = contextvars.ContextVar(
    "_request_region",
    default=os.environ.get("MINISTACK_REGION", "us-east-1"),
)


def set_request_region(region: str | None) -> None:
    """Set the region for the current request. Falls back to MINISTACK_REGION /
    ``us-east-1`` when the caller supplies nothing."""
    if region:
        _request_region.set(region)
    else:
        _request_region.set(os.environ.get("MINISTACK_REGION", "us-east-1"))


def get_region() -> str:
    """Return the region for the current request."""
    return _request_region.get()


class AccountScopedDict:
    """A dict-like container that namespaces keys by the current request's account ID.

    Stores data as ``{account_id}\\x00{key}`` internally so that identical
    resource names in different accounts never collide. All standard dict
    operations (get, set, delete, iteration, ``in``, ``len``) are scoped to the
    caller's account automatically via ``get_account_id()``.

    This is a drop-in replacement for ``dict`` in service module-level state,
    e.g. ``_roles = AccountScopedDict()`` instead of ``_roles: dict = {}``.
    """

    __slots__ = ("_data",)

    def __init__(self):
        self._data: dict = {}

    # -- internal helpers --------------------------------------------------

    def _scoped(self, key):
        return (get_account_id(), key)

    def _unscope(self, scoped_key):
        return scoped_key[1]

    def _prefix(self):
        return get_account_id()

    def _is_mine(self, scoped_key):
        return scoped_key[0] == get_account_id()

    # -- dict interface ----------------------------------------------------

    def __setitem__(self, key, value):
        self._data[self._scoped(key)] = value

    def __getitem__(self, key):
        return self._data[self._scoped(key)]

    def __delitem__(self, key):
        del self._data[self._scoped(key)]

    def __contains__(self, key):
        return self._scoped(key) in self._data

    def __len__(self):
        return sum(1 for k in self._data if self._is_mine(k))

    def __bool__(self):
        return any(self._is_mine(k) for k in self._data)

    def __iter__(self):
        for k in self._data:
            if self._is_mine(k):
                yield self._unscope(k)

    def get(self, key, default=None):
        return self._data.get(self._scoped(key), default)

    def pop(self, key, *args):
        return self._data.pop(self._scoped(key), *args)

    def setdefault(self, key, default=None):
        return self._data.setdefault(self._scoped(key), default)

    def keys(self):
        return [self._unscope(k) for k in self._data if self._is_mine(k)]

    def values(self):
        return [v for k, v in self._data.items() if self._is_mine(k)]

    def items(self):
        return [(self._unscope(k), v) for k, v in self._data.items() if self._is_mine(k)]

    def update(self, other):
        if isinstance(other, AccountScopedDict):
            self._data.update(other._data)
        elif isinstance(other, dict):
            for k, v in other.items():
                self[k] = v

    def clear(self):
        """Clear ALL accounts' data (used by reset)."""
        self._data.clear()

    def to_dict(self):
        """Convert ALL accounts' data to a plain dict for serialization.
        Keys are stored as (account_id, original_key) tuples."""
        return dict(self._data)

    @classmethod
    def from_dict(cls, data):
        """Restore from a plain dict produced by to_dict()."""
        obj = cls()
        obj._data = dict(data)
        return obj

    def __repr__(self):
        return f"AccountScopedDict({dict(self.items())})"


def xml_response(root_tag: str, namespace: str, children: dict, status: int = 200) -> tuple:
    """Build an AWS-style XML response."""
    root = Element(root_tag, xmlns=namespace)
    _dict_to_xml(root, children)

    # Add RequestId in ResponseMetadata
    metadata = SubElement(root, "ResponseMetadata")
    req_id = SubElement(metadata, "RequestId")
    req_id.text = str(uuid.uuid4())

    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _dict_to_xml(parent: Element, data):
    """Recursively convert dict/list to XML elements."""
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                for item in value:
                    child = SubElement(parent, key)
                    if isinstance(item, dict):
                        _dict_to_xml(child, item)
                    else:
                        child.text = str(item)
            elif isinstance(value, dict):
                child = SubElement(parent, key)
                _dict_to_xml(child, value)
            else:
                child = SubElement(parent, key)
                child.text = str(value) if value is not None else ""
    elif isinstance(data, str):
        parent.text = data


def json_response(data: dict, status: int = 200) -> tuple:
    """Build an AWS-style JSON response."""
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    return status, {"Content-Type": "application/x-amz-json-1.0"}, body


def error_response_xml(code: str, message: str, status: int, namespace: str = "http://s3.amazonaws.com/doc/2006-03-01/") -> tuple:
    """AWS-style XML error response."""
    root = Element("ErrorResponse", xmlns=namespace)
    error = SubElement(root, "Error")
    t = SubElement(error, "Type")
    t.text = "Sender" if status < 500 else "Receiver"
    c = SubElement(error, "Code")
    c.text = code
    m = SubElement(error, "Message")
    m.text = message
    req = SubElement(root, "RequestId")
    req.text = str(uuid.uuid4())

    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def error_response_json(code: str, message: str, status: int = 400) -> tuple:
    """AWS-style JSON error response."""
    data = {
        "__type": code,
        "message": message,
    }
    return json_response(data, status)


def now_iso() -> str:
    """Current time in AWS ISO format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def now_rfc7231() -> str:
    """Current time in RFC 7231 format for HTTP headers (e.g. Last-Modified)."""
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def iso_to_rfc7231(iso_str: str) -> str:
    """Convert an ISO 8601 timestamp to RFC 7231 format."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
    except (ValueError, AttributeError):
        return iso_str


def now_epoch() -> float:
    return datetime.now(timezone.utc).timestamp()


def md5_hash(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def apply_image_prefix(image: str) -> str:
    """Prepend ``MINISTACK_IMAGE_PREFIX`` (trailing slash optional) to a Docker
    image reference so real-infra services route through a private registry.

    Mirrors Testcontainers' ``hub.image.name.prefix`` semantics: when the
    prefix is set, every nested container image (RDS postgres/mysql/mariadb,
    ElastiCache redis/memcached, EKS k3s) is rewritten to
    ``{prefix}/{image}``. Images already prefixed are left alone so repeated
    application is idempotent.

    Examples (``MINISTACK_IMAGE_PREFIX=proxy.corp.net``):
        ``postgres:15-alpine`` → ``proxy.corp.net/postgres:15-alpine``
        ``rancher/k3s:v1.31.4-k3s1`` → ``proxy.corp.net/rancher/k3s:v1.31.4-k3s1``
        ``proxy.corp.net/postgres:15-alpine`` → unchanged
    """
    prefix = os.environ.get("MINISTACK_IMAGE_PREFIX", "").strip().rstrip("/")
    if not prefix:
        return image
    if image.startswith(prefix + "/"):
        return image
    return f"{prefix}/{image}"


def sha256_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def new_uuid() -> str:
    return str(uuid.uuid4())
