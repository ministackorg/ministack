# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
CloudFormation helpers — XML response formatting and parameter extraction utilities.
"""

import logging
import re
from html import escape as _esc
from urllib.parse import urlparse

from ministack.core.responses import new_uuid

logger = logging.getLogger("cloudformation")

CFN_NS = "http://cloudformation.amazonaws.com/doc/2010-05-08/"

# The CloudFormation quotas page: the template body of a request and of an
# S3 object behind TemplateURL. CreateStack documents the stack name as
# "only alphanumeric characters (case sensitive) and hyphens", starting with
# a letter and no longer than 128 characters.
TEMPLATE_BODY_MAX_BYTES = 51200
TEMPLATE_URL_MAX_BYTES = 1000000
STACK_NAME_MAX_CHARS = 128
STACK_NAME_PATTERN = "[a-zA-Z][-a-zA-Z0-9]*"
STACK_NAME_RE = re.compile(STACK_NAME_PATTERN)


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

def _validate_stack_tags(tags):
    """The checks CreateStack, UpdateStack and CreateChangeSet apply to the
    stack-level ``Tags``: at most 50, no ``aws:`` prefix (reserved for AWS),
    keys up to 128 and values up to 256 characters. Returns the error
    response, or None when the tags pass."""
    if len(tags) > 50:
        return _error("ValidationError", "A maximum number of 50 tags can be specified")
    for tag in tags:
        key = str(tag.get("Key", ""))
        value = str(tag.get("Value", ""))
        if key.lower().startswith("aws:"):
            return _error(
                "ValidationError",
                f"Tag key '{key}' is reserved: the aws: prefix cannot be used")
        if len(key) > 128:
            return _error(
                "ValidationError",
                f"Tag key '{key[:32]}...' exceeds the maximum length of 128 characters")
        if len(value) > 256:
            return _error(
                "ValidationError",
                f"Tag value for key '{key}' exceeds the maximum length of 256 characters")
    return None



def validation_error_message(problems: list[str]) -> str:
    """The API's parameter validation joins every request-level violation into
    one message (quoted: ``2 validation errors detected: Value '' at
    'stackName' failed to satisfy constraint: ...; Value '' at 'stackName'
    failed to satisfy constraint: ...``)."""
    plural = "s" if len(problems) > 1 else ""
    return f"{len(problems)} validation error{plural} detected: " + "; ".join(problems)


def stack_name_problems(stack_name: str) -> list[str]:
    """The constraints CreateStack documents for ``StackName``: alphanumeric
    characters and hyphens, a letter first, at most 128 characters. One
    sentence per violated constraint, in the API's parameter validation
    wording (quoted for the pattern: ``Value 'x_y' at 'stackName' failed to
    satisfy constraint: Member must satisfy regular expression pattern:
    [a-zA-Z][-a-zA-Z0-9]*``; the length sentence follows the shape of other
    API parameters, unmeasured)."""
    problems = []
    if len(stack_name) > STACK_NAME_MAX_CHARS:
        problems.append(
            f"Value '{stack_name}' at 'stackName' failed to satisfy constraint: Member "
            f"must have length less than or equal to {STACK_NAME_MAX_CHARS}")
    if not STACK_NAME_RE.fullmatch(stack_name):
        problems.append(
            f"Value '{stack_name}' at 'stackName' failed to satisfy constraint: Member "
            f"must satisfy regular expression pattern: {STACK_NAME_PATTERN}")
    return problems


CAPABILITY_VALUES = ("CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND")


def capabilities_problems(values: list[str]) -> list[str]:
    """The enum constraint on ``Capabilities``. CreateStack, UpdateStack and
    CreateChangeSet document exactly three values, so anything else is a
    parameter validation error rather than a capability the stack quietly
    keeps. One sentence for the whole member, in the shape the API uses for a
    list of enums (unmeasured wording; the enum itself is the API reference's
    Valid Values)."""
    unknown = [v for v in values if v not in CAPABILITY_VALUES]
    if not unknown:
        return []
    return [f"Value '[{', '.join(values)}]' at 'capabilities' failed to satisfy "
            f"constraint: Member must satisfy enum value set: "
            f"[{', '.join(CAPABILITY_VALUES)}]"]


def _template_body_problem(body: str) -> str | None:
    """The request-level constraint on ``TemplateBody`` (the quota page:
    "Template body size in a request", 51,200 bytes), as one sentence of the
    API's parameter validation, which echoes the value (quoted: ``Value
    '<the template>' at 'templateBody' failed to satisfy constraint: Member
    must have length less than or equal to 51200``); None when it holds.
    CreateStack joins it with the stack name problems into one message."""
    if body and len(body.encode("utf-8")) > TEMPLATE_BODY_MAX_BYTES:
        return (f"Value '{body}' at 'templateBody' failed to satisfy constraint: Member "
                f"must have length less than or equal to {TEMPLATE_BODY_MAX_BYTES}")
    return None


def _request_problems(params, stack_name: str = ""):
    """The request-level constraints of a call that carries a template: the
    stack name where the call names one, the size of an inline
    ``TemplateBody`` and the ``Capabilities`` enum. The API reports every
    violation of a request in one message, so all of them are collected
    before it answers. Returns the error response, or None when the request
    passes."""
    problems = stack_name_problems(stack_name) if stack_name else []
    body_problem = _template_body_problem(_p(params, "TemplateBody"))
    if body_problem:
        problems.append(body_problem)
    problems.extend(capabilities_problems(_extract_string_members(params, "Capabilities")))
    if problems:
        return _error("ValidationError", validation_error_message(problems))
    return None


def _resolve_template(params):
    """Resolve TemplateBody or TemplateURL to a template string.
    If TemplateURL is provided, fetch the template from S3.
    Returns (template_body, error_tuple) — error_tuple is None on success.

    The request-level constraints are checked here, so every action that reads
    a template applies them. CreateStack and a CREATE change set check them
    earlier, together with the stack name, and return before this point."""
    request_error = _request_problems(params)
    if request_error:
        return None, request_error
    return _resolve_document(params, "TemplateBody", "TemplateURL", "Template")


def _resolve_document(params, body_key, url_key, label):
    """Resolve an inline document or its S3 URL (``TemplateBody``/``TemplateURL``,
    ``StackPolicyBody``/``StackPolicyURL``) to a string; ``label`` names the
    document in the errors. Returns (body, error_tuple)."""
    body = _p(params, body_key)
    url = _p(params, url_key)

    if body:
        return body, None

    if url:
        try:
            from ministack.services import s3 as _s3
            parsed = urlparse(url)
            # Support formats:
            #   http://localhost:4566/bucket/key
            #   https://s3.amazonaws.com/bucket/key
            #   https://bucket.s3.amazonaws.com/key
            path = parsed.path.lstrip("/")
            parts = path.split("/", 1)
            if len(parts) < 2:
                return None, _error("ValidationError", f"Invalid {url_key}: {url}")
            bucket_name, key = parts[0], parts[1]
            obj_data = _s3._get_object_data(bucket_name, key)
            if obj_data is None:
                return None, _error("ValidationError", f"{label} not found at {url}")
            if body_key == "TemplateBody" and len(obj_data) > TEMPLATE_URL_MAX_BYTES:
                # "Template body size in an Amazon S3 object", 1 MB on the quota
                # page; the service enforces 1,000,000 bytes and answers with
                # this sentence (quoted from reports of the error).
                return None, _error(
                    "ValidationError",
                    f"Template may not exceed {TEMPLATE_URL_MAX_BYTES} bytes in size.")
            return obj_data.decode("utf-8"), None
        except Exception as e:
            logger.warning("Failed to fetch %s %s: %s", url_key, url, e)
            return None, _error("ValidationError", f"Error fetching {url_key}: {e}")

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


def _extract_string_members(params, prefix):
    """Extract a plain string list (``RetainResources.member.N``); the JSON
    protocol sends it as a list under the bare key."""
    direct = params.get(prefix)
    if isinstance(direct, list):
        return [str(v) for v in direct]
    result = []
    i = 1
    while True:
        value = _p(params, f"{prefix}.member.{i}")
        if not value:
            break
        result.append(value)
        i += 1
    return result


PAGE_SIZE = 100


def _page(items, params, action):
    """Cut ``items`` down to the page a request asks for.

    Real CloudFormation pages ``ListExports`` at 100 values and the other
    list and describe actions at 1 MB of output; here every one of them pages
    at 100 items. The token is ``<Action>:<offset of the next page>``, so a
    token of one action is refused by another. Returns
    ``(page, next_token_xml, error)`` — ``next_token_xml`` is empty on the
    last page and ``error`` is the response for a token the service did not
    hand out.

    The offset is taken over the store as it is at the time of the call: an
    item added or removed between two pages shifts the later pages, so an
    item can be skipped or repeated (events are listed newest first, so an
    appended event shifts every offset by one)."""
    token = _p(params, "NextToken")
    start = 0
    if token:
        tagged, _, offset = token.partition(":")
        if tagged != action or not offset.isdigit():
            return [], "", _error("ValidationError", "Invalid NextToken")
        start = int(offset)
    page = items[start:start + PAGE_SIZE]
    next_token_xml = ""
    if start + PAGE_SIZE < len(items):
        next_token_xml = f"<NextToken>{action}:{start + PAGE_SIZE}</NextToken>"
    return page, next_token_xml, None
