"""
AWS Response formatting utilities.
Handles XML responses (S3, SQS, SNS, IAM, STS, CloudWatch) and
JSON responses (DynamoDB, Lambda, SecretsManager, CloudWatch Logs).
"""

import json
import uuid
import hashlib
from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement, tostring


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


def sha256_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def new_uuid() -> str:
    return str(uuid.uuid4())
