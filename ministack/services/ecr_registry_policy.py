# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
ECR registry policy and account setting (part of the ECR service; ``ecr_pull_through.py`` dispatches here).

Supports: PutRegistryPolicy, GetRegistryPolicy, DeleteRegistryPolicy, GetAccountSetting.

The policy text is validated and stored re-serialised the way AWS returns it (issue #2617 captures).
"""

import copy
import functools
import json
import re

from ministack.core import responses as _responses
from ministack.core.responses import AccountRegionScopedDict, get_account_id

_SURROGATE_RE = re.compile("[\ud800-\udfff]")


def _scrub(value):
    """*value* with every lone surrogate written as "?", the way an errors="replace" encode writes it."""
    if isinstance(value, str):
        return _SURROGATE_RE.sub("?", value)
    if isinstance(value, dict):
        return {_scrub(k): _scrub(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_scrub(v) for v in value]
    return value


def json_response(data, status=200, **kwargs):
    """``responses.json_response`` for ECR: a client string holding a lone surrogate never makes the encode raise."""
    return _responses.json_response(_scrub(data), status, **kwargs)


def error_response_json(code, message, status=400, extra=None, **kwargs):
    """``responses.error_response_json`` for ECR, with the same surrogate handling."""
    return _responses.error_response_json(code, _scrub(message), status, _scrub(extra), **kwargs)


def _has_surrogate(value):
    if isinstance(value, str):
        return _SURROGATE_RE.search(value) is not None
    if isinstance(value, dict):
        return any(_has_surrogate(k) or _has_surrogate(v) for k, v in value.items())
    if isinstance(value, list):
        return any(_has_surrogate(v) for v in value)
    return False


_policies = AccountRegionScopedDict()  # "policy" -> {"policyText": str}, one registry policy per account and region

MAX_POLICY_LENGTH = 10240
_VERSIONS = ("2012-10-17", "2008-10-17")
_STATEMENT_KEYS = ("Sid", "Effect", "Principal", "Action", "NotAction", "Resource", "Condition")
_ACCOUNT_ID_RE = re.compile(r"[0-9]{12}")

# ECR actions that are IAM permissions without an API operation of their own.
_IAM_ONLY_ACTIONS = ("BatchImportUpstreamImage", "ReplicateImage")


@functools.lru_cache(maxsize=1)
def known_actions():
    """The ECR actions a policy may name: botocore's ECR operation names plus the IAM-only actions.
    ``ecr:*`` style wildcards match against these."""
    import botocore.session

    operations = botocore.session.get_session().get_service_model("ecr").operation_names
    return frozenset((*operations, *_IAM_ONLY_ACTIONS))


_MSG_NOT_FOUND = "Registry policy does not exist in the registry with id '{}'"


def get_state():
    return {"registry_policies": copy.deepcopy(_policies)}


def load_persisted_state(data):
    if data:
        _policies.update(data.get("registry_policies", {}))


def reset():
    _policies.clear()


def _invalid_policy_text(constraint):
    return error_response_json(
        "InvalidParameterException",
        f"Invalid parameter at 'PolicyText' failed to satisfy constraint: '{constraint}'", 400)


def _as_list(value):
    return value if isinstance(value, list) else [value]


def _collapse(values):
    return values[0] if len(values) == 1 else values


@functools.lru_cache(maxsize=256)
def _wildcard_re(pattern):
    """Case-insensitive regex of an action pattern in which only ``*`` and ``?`` are wildcards."""
    parts = [".*" if c == "*" else "." if c == "?" else re.escape(c) for c in pattern]
    return re.compile("".join(parts), re.IGNORECASE | re.DOTALL)


def _action_known(action):
    if not isinstance(action, str):
        return False
    if action == "*":
        return True
    if ":" not in action:
        return False
    service, name = action.split(":", 1)
    if service.lower() != "ecr":
        return True  # actions of other services (and wildcard services) are not checked
    pattern = _wildcard_re(name)
    return any(pattern.fullmatch(known) for known in known_actions())


def _refuse_constant(constant):
    raise ValueError(f"{constant} is not JSON")


def _parse_float(token):
    value = float(token)
    if value in (float("inf"), float("-inf")):
        raise ValueError(f"{token} is not a finite number")
    return value


def _principal(value):
    """The normalised Principal, or None when the statement is invalid."""
    if value == "*":
        return value
    if not isinstance(value, dict) or not value:
        return None
    result = {}
    for kind, entries in value.items():
        if kind not in ("AWS", "Service", "Federated", "CanonicalUser"):
            return None
        entries = _as_list(entries)
        if not entries or not all(isinstance(e, str) and e for e in entries):
            return None
        if kind == "AWS":
            fixed = []
            for entry in entries:
                if _ACCOUNT_ID_RE.fullmatch(entry):
                    entry = f"arn:aws:iam::{entry}:root"
                elif entry != "*" and not entry.startswith("arn:"):
                    return None
                fixed.append(entry)
            entries = fixed
        result[kind] = _collapse(entries)
    return result


def _statement(raw):
    if not isinstance(raw, dict) or any(key not in _STATEMENT_KEYS for key in raw):
        return None
    if raw.get("Effect") not in ("Allow", "Deny"):
        return None
    if "Sid" in raw and not isinstance(raw["Sid"], str):
        return None
    result = {}
    for key in _STATEMENT_KEYS:
        if key not in raw:
            continue
        value = raw[key]
        if key == "Principal":
            value = _principal(value)
            if value is None:
                return None
        elif key in ("Action", "NotAction"):
            items = _as_list(value)
            if not items or not all(_action_known(a) for a in items):
                return None
            value = _collapse(items)
        elif key == "Resource":
            items = _as_list(value)
            if not items or not all(isinstance(r, str) and (r == "*" or r.startswith("arn:")) for r in items):
                return None
            value = _collapse(items)
        elif key == "Condition" and not isinstance(value, dict):
            return None
        result[key] = value
    if "Action" not in result and "NotAction" not in result:
        return None
    return result


def _normalise(text):
    """The policy document as AWS stores it, or None when it is not a valid registry policy."""
    try:
        # Python accepts NaN, Infinity and -Infinity; a strict JSON parser does not.
        document = json.loads(text, parse_constant=_refuse_constant, parse_float=_parse_float)
    except (ValueError, RecursionError):
        return None
    if _has_surrogate(document):
        return None
    if not isinstance(document, dict) or any(key not in ("Version", "Id", "Statement") for key in document):
        return None
    version = document.get("Version", "2008-10-17")
    if version not in _VERSIONS:
        return None
    statements = document.get("Statement")
    if isinstance(statements, dict):
        statements = [statements]
    if not isinstance(statements, list) or not statements:
        return None
    fixed = [_statement(s) for s in statements]
    if any(s is None for s in fixed):
        return None
    result = {"Version": version}
    if "Id" in document:
        result["Id"] = document["Id"]
    result["Statement"] = fixed
    return result


def _dump(value, level=0):
    """JSON the way the registry returns it: 2-space indent, ' : ' separators, arrays on one line."""
    if isinstance(value, dict):
        if not value:
            return "{ }"
        pad = "  " * (level + 1)
        body = ",\n".join(f"{pad}{json.dumps(k, ensure_ascii=False)} : {_dump(v, level + 1)}"
                          for k, v in value.items())
        return "{\n" + body + "\n" + "  " * level + "}"
    if isinstance(value, list):
        if not value:
            return "[ ]"
        return "[ " + ", ".join(_dump(v, level) for v in value) + " ]"
    return json.dumps(value, ensure_ascii=False)


def _put(data):
    text = data.get("policyText")
    if text is None:
        return _invalid_policy_text("Cannot be null")
    if text == "":
        return _invalid_policy_text("Must not be empty")
    if len(text) > MAX_POLICY_LENGTH:
        return _invalid_policy_text(f"Must have length less than or equal to {MAX_POLICY_LENGTH}")
    document = _normalise(text)
    if document is None:
        return _invalid_policy_text("Invalid registry policy provided")
    stored = _dump(document)
    _policies["policy"] = {"policyText": stored}
    return json_response({"registryId": get_account_id(), "policyText": stored})


def _not_found():
    return error_response_json("RegistryPolicyNotFoundException", _MSG_NOT_FOUND.format(get_account_id()), 400)


def _get(data):
    policy = _policies.get("policy")
    if policy is None:
        return _not_found()
    return json_response({"registryId": get_account_id(), "policyText": policy["policyText"]})


def _delete(data):
    policy = _policies.pop("policy", None)
    if policy is None:
        return _not_found()
    return json_response({"registryId": get_account_id(), "policyText": policy["policyText"]})


def _account_setting(data):
    name = data.get("name")
    if name == "REGISTRY_POLICY_SCOPE":
        return json_response({"name": name, "value": "V2"})
    return error_response_json(
        "InvalidParameterException", f"Invalid parameter at 'name' failed to satisfy constraint: '{name}'", 400)


HANDLERS = {
    "PutRegistryPolicy": _put,
    "GetRegistryPolicy": _get,
    "DeleteRegistryPolicy": _delete,
    "GetAccountSetting": _account_setting,
}
