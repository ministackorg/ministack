"""
CloudFormation helpers — XML response formatting and parameter extraction utilities.
"""

import logging
from html import escape as _esc
from urllib.parse import urlparse

from ministack.core.responses import new_uuid

logger = logging.getLogger("cloudformation")

CFN_NS = "http://cloudformation.amazonaws.com/doc/2010-05-08/"


def _p(params, key, default=""):
    """Extract a single value from parsed query-string params."""
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _xml(status, root_tag, inner):
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<{root_tag} xmlns="{CFN_NS}">'
        f'{inner}'
        f'<ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>'
        f'</{root_tag}>'
    ).encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status=400):
    t = "Sender" if status < 500 else "Receiver"
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<ErrorResponse xmlns="{CFN_NS}">'
        f'<Error><Type>{t}</Type><Code>{code}</Code>'
        f'<Message>{_esc(message)}</Message></Error>'
        f'<RequestId>{new_uuid()}</RequestId>'
        f'</ErrorResponse>'
    ).encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _extract_members(params, prefix):
    """Extract Parameters.member.N.Key/Value or Tags.member.N.Key/Value."""
    result = []
    i = 1
    while True:
        key = (_p(params, f"{prefix}.member.{i}.ParameterKey")
               or _p(params, f"{prefix}.member.{i}.Key"))
        if not key:
            break
        value = (_p(params, f"{prefix}.member.{i}.ParameterValue")
                 or _p(params, f"{prefix}.member.{i}.Value"))
        use_prev = _p(params, f"{prefix}.member.{i}.UsePreviousValue")
        result.append({"Key": key, "Value": value or "",
                       "UsePreviousValue": str(use_prev).lower() == "true"})
        i += 1
    return result


def _resolve_template(params):
    """Resolve TemplateBody or TemplateURL to a template string.
    If TemplateURL is provided, fetch the template from S3.
    Returns (template_body, error_tuple) — error_tuple is None on success."""
    template_body = _p(params, "TemplateBody")
    template_url = _p(params, "TemplateURL")

    if template_body:
        return template_body, None

    if template_url:
        try:
            from ministack.services import s3 as _s3
            parsed = urlparse(template_url)
            # Support formats:
            #   http://localhost:4566/bucket/key
            #   https://s3.amazonaws.com/bucket/key
            #   https://bucket.s3.amazonaws.com/key
            path = parsed.path.lstrip("/")
            parts = path.split("/", 1)
            if len(parts) < 2:
                return None, _error("ValidationError",
                                    f"Invalid TemplateURL: {template_url}")
            bucket_name, key = parts[0], parts[1]
            obj_data = _s3._get_object_data(bucket_name, key)
            if obj_data is None:
                return None, _error("ValidationError",
                                    f"Template not found at {template_url}")
            return obj_data.decode("utf-8"), None
        except Exception as e:
            logger.warning("Failed to fetch TemplateURL %s: %s", template_url, e)
            return None, _error("ValidationError",
                                f"Error fetching TemplateURL: {e}")

    return None, None  # neither provided


def _extract_stack_status_filters(params):
    """Extract StackStatusFilter.member.N values."""
    filters = []
    i = 1
    while True:
        val = _p(params, f"StackStatusFilter.member.{i}")
        if not val:
            break
        filters.append(val)
        i += 1
    return filters
