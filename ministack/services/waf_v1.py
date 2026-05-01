"""
WAF Classic + WAF Regional (v1) stub.

Minimal AWS-shape-correct responses for the v1 API. AWS deprecated v1 in favour
of WAFv2; most modern code paths use ``wafv2``. This module exists so SDK
clients targeting v1 (Terraform legacy, old CFN, JDK examples) get clean
empty-state responses instead of a 405.

Target prefixes:
- ``AWSWAF_20150824``         (waf — classic / global)
- ``AWSWAF_Regional_20161128`` (waf-regional)
"""

import json
import logging

from ministack.core.responses import (
    error_response_json,
    new_uuid,
)

logger = logging.getLogger("waf_v1")


def _v1_response(data: dict, status: int = 200) -> tuple:
    """WAF v1 uses application/x-amz-json-1.1 (botocore metadata.jsonVersion)."""
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    return status, {"Content-Type": "application/x-amz-json-1.1"}, body

# Map List* operation -> response member name. From botocore service-2.json
# (waf-regional 2016-11-28). Identical between waf and waf-regional.
_LIST_RESULT_KEYS = {
    "ListActivatedRulesInRuleGroup": "ActivatedRules",
    "ListByteMatchSets": "ByteMatchSets",
    "ListGeoMatchSets": "GeoMatchSets",
    "ListIPSets": "IPSets",
    "ListLoggingConfigurations": "LoggingConfigurations",
    "ListRateBasedRules": "Rules",
    "ListRegexMatchSets": "RegexMatchSets",
    "ListRegexPatternSets": "RegexPatternSets",
    "ListResourcesForWebACL": "ResourceArns",
    "ListRuleGroups": "RuleGroups",
    "ListRules": "Rules",
    "ListSizeConstraintSets": "SizeConstraintSets",
    "ListSqlInjectionMatchSets": "SqlInjectionMatchSets",
    "ListSubscribedRuleGroups": "RuleGroups",
    "ListTagsForResource": "TagInfoForResource",
    "ListWebACLs": "WebACLs",
    "ListXssMatchSets": "XssMatchSets",
}

_NOT_FOUND = "WAFNonexistentItemException"


def _change_token():
    return new_uuid()


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("X-Amz-Target") or headers.get("x-amz-target") or ""
    op = target.split(".", 1)[1] if "." in target else target
    if not op:
        return error_response_json("InvalidAction", "missing X-Amz-Target", 400)

    try:
        body_text = body.decode("utf-8") if isinstance(body, bytes) else (body or "")
    except UnicodeDecodeError:
        body_text = ""
    try:
        _ = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "invalid JSON body", 400)

    if op in _LIST_RESULT_KEYS:
        key = _LIST_RESULT_KEYS[op]
        # ListTagsForResource returns a struct, not a list
        if op == "ListTagsForResource":
            return _v1_response({
                "NextMarker": None,
                "TagInfoForResource": {"ResourceARN": "", "TagList": []},
            })
        return _v1_response({key: [], "NextMarker": None})

    if op == "GetChangeToken":
        return _v1_response({"ChangeToken": _change_token()})

    if op == "GetChangeTokenStatus":
        # INSYNC is the terminal success status per the spec.
        return _v1_response({"ChangeTokenStatus": "INSYNC"})

    if op == "GetPermissionPolicy":
        return _v1_response({"Policy": ""})

    if op.startswith("Get"):
        return error_response_json(_NOT_FOUND,
                                   "The referenced item does not exist.", 400)

    if op.startswith("Create"):
        # Real AWS returns a typed structure with the created resource +
        # ChangeToken. The exact field name varies; the safest stub is to
        # return a generic shape — most clients only check the response code.
        return _v1_response({"ChangeToken": _change_token()})

    if op.startswith(("Update", "Delete", "Associate", "Disassociate",
                      "Put", "TagResource", "UntagResource")):
        return _v1_response({"ChangeToken": _change_token()})

    return error_response_json("WAFInvalidOperationException",
                               f"Operation '{op}' is not implemented in this stub.",
                               400)
