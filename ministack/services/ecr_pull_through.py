# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
ECR pull-through cache rules (part of the ECR service; ``ecr.py`` dispatches here).

Supports: CreatePullThroughCacheRule, UpdatePullThroughCacheRule,
DeletePullThroughCacheRule, DescribePullThroughCacheRules.

Every check, message and check order below comes from AWS captures of the
five rule actions (issue #2617). The request body is read with a strict
parser first, because AWS reports malformed JSON, duplicate keys and member
type errors (SerializationException) before any validation.
"""

import asyncio
import base64
import concurrent.futures
import contextvars
import copy
import hashlib
import http.client
import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    get_account_id,
    get_region,
)
from ministack.services import ecr_registry_policy
from ministack.services.ecr_registry_policy import error_response_json, json_response

logger = logging.getLogger("ecr")

_rules = AccountRegionScopedDict()  # ecrRepositoryPrefix -> rule record, in creation order
_origins = AccountRegionScopedDict()  # cache repository name -> {host, path, credentialArn} it was filled from

MAX_RULES = 50
SERVICE_LINKED_ROLE_SERVICE = "pullthroughcache.ecr.amazonaws.com"
SERVICE_LINKED_ROLE_NAME = "AWSServiceRoleForECRPullThroughCache"
SERVICE_LINKED_ROLE_POLICY = "arn:aws:iam::aws:policy/aws-service-role/AWSECRPullThroughCache_ServiceRolePolicy"

_PREFIX_PATTERN = (
    r"^([a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(\/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*\/?|ROOT)$"
)
_PREFIX_RE = re.compile(_PREFIX_PATTERN)
_CREDENTIAL_PATTERN = (
    r"^arn:aws(-\w+)*:secretsmanager:[a-zA-Z0-9-:]+:secret:ecr\-pullthroughcache\/[a-zA-Z0-9\/_+=.@-]+$"
)
_CREDENTIAL_RE = re.compile(_CREDENTIAL_PATTERN, re.ASCII)
_REGISTRY_ID_RE = re.compile(r"[0-9]{12}")
_ECR_URL_RE = re.compile(r"^([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com$")
_AZURE_URL_RE = re.compile(r"^[a-z0-9]+\.azurecr\.io$")

_UPSTREAM_REGISTRIES = (
    "ecr-public", "chainguard", "k8s", "azure-container-registry", "docker-hub", "ecr-public-premium",
    "github-container-registry", "ecr", "gitlab-container-registry", "quay",
)
_URL_TO_UPSTREAM = {
    "public.ecr.aws": "ecr-public",
    "quay.io": "quay",
    "registry.k8s.io": "k8s",
    "registry-1.docker.io": "docker-hub",
    "ghcr.io": "github-container-registry",
    "registry.gitlab.com": "gitlab-container-registry",
    "cgr.dev": "chainguard",
}
_CREDENTIAL_UPSTREAMS = {
    "docker-hub", "github-container-registry", "gitlab-container-registry",
    "chainguard", "azure-container-registry",
}

_MSG_NEEDS_SECRET = (
    "The specified upstream registry requires authentication. Specify a valid Secrets Manager ARN "
    "containing the upstream registry credentials and try again."
)
_MSG_NEEDS_NO_AUTH = (
    "The specified upstream registry doesn't require authentication. Verify the upstream registry URL "
    "and the credential ARN value and try again."
)
_MSG_NEEDS_ROLE = (
    "Invalid input: The specified upstream registry requires authentication. "
    "Specify a valid IAM role ARN and try again."
)
_MSG_NEEDS_SECRET_INPUT = "Invalid input: " + _MSG_NEEDS_SECRET
_MSG_TYPE_MISMATCH = (
    "Invalid input: Upstream registry URL and provided credential type do not match. "
    "Please verify input and try again."
)
_MSG_ONE_CREDENTIAL = "Invalid input: Please set either credential arn or role arn to update the credentials"
_MSG_UNAUTHENTICATED_RULE = (
    "Invalid input: Please create an authenticated pull through cache rule to update the credentials"
)
_MSG_SECRET_REGION = (
    "Please verify the secret exists in the same region and account that the pullthrough cache rule "
    "will be created in, and try again."
)
_MSG_SECRET_NOT_FOUND = (
    "The ARN of the secret specified in the pull through cache rule was not found. "
    "Update the pull through cache rule with a valid secret ARN and try again."
)
_MSG_SECRET_UNREADABLE = (
    "The secret is unable to be accessed. Verify the resource permissions for the secret and try again."
)
_MSG_SECRET_ROLE_UNREADABLE = (
    "The secret can't be accessed. Verify the resource permissions for the secret and try again."
)
_MSG_UPSTREAM_DENIED = "Failed to get upstream bearer token. Verify the credentials provided and try again."
_MSG_ASSUME_ROLE = "Unable to access the IAM role. Please verify trust policy and try again."
_MSG_UNABLE_TO_VALIDATE = "Failed to get response from upstream registry authorization endpoint. The upstream registry may be unavailable. Try again."
_MSG_PULL_DENIED = (
    "The authentication to the upstream registry failed. Verify that the credentials provided in the "
    "pull through cache rule are correct, and try again."
)
_MSG_SECRET_UNDECRYPTABLE = (
    "The secret is accessible but is unable to be decrypted. Verify the resource permisisons and try again."
)


# ── Persistence ────────────────────────────────────────────

def get_state():
    import copy

    return {"pull_through_cache_rules": copy.deepcopy(_rules), "pull_through_cache_origins": copy.deepcopy(_origins),
            **ecr_registry_policy.get_state()}


def load_persisted_state(data):
    if data:
        _rules.update(data.get("pull_through_cache_rules", {}))
        _origins.update(data.get("pull_through_cache_origins", {}))
        ecr_registry_policy.load_persisted_state(data)


def reset():
    _rules.clear()
    _origins.clear()
    ecr_registry_policy.reset()


# ── Wire format ────────────────────────────────────────────

class _Wire(Exception):
    """A request AWS refuses before validation (SerializationException) or any other error response."""

    def __init__(self, response):
        super().__init__("wire error")
        self.response = response


def _serialization_error(message=None):
    body = {"__type": "SerializationException"}
    if message is not None:
        body["Message"] = message
    return 400, {"Content-Type": "application/x-amz-json-1.1", "x-amzn-errortype": "SerializationException"}, \
        json.dumps(body).encode()


def _error(code, message, status=400):
    return error_response_json(code, message, status)


def _internal_failure():
    return 500, {"Content-Type": "application/x-amz-json-1.1", "x-amzn-errortype": "InternalFailure"}, \
        json.dumps({"__type": "InternalFailure"}).encode()


def _access_denied(message):
    return error_response_json("AccessDeniedException", message, 400, message_key="Message")


_HUGE_INTEGER = 10 ** 40  # stands for an integer token too long for int(); out of range like any 40-digit value


class _Reader:
    """Strict JSON reader with AWS's error positions and duplicate-key refusal."""

    _NUMBER = re.compile(r"-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?")

    def __init__(self, text):
        self.text = text
        self.pos = 0

    def _fail_at(self, index):
        if index >= len(self.text):
            raise _Wire(_serialization_error("Unexpected end of payload"))
        raise _Wire(_serialization_error(f"Unexpected Character in input string at byte {len(self.text[:index].encode('utf-8', 'replace'))}"))

    def _skip(self):
        while self.pos < len(self.text) and self.text[self.pos] in " \t\r\n":
            self.pos += 1

    def parse(self):
        self._skip()
        if self.pos >= len(self.text):
            raise _Wire(_serialization_error())
        value = self._value(0)
        self._skip()
        if self.pos < len(self.text):
            self._fail_at(self.pos)
        return value

    def _value(self, depth):
        if depth > 64:
            raise _Wire(_serialization_error())
        self._skip()
        if self.pos >= len(self.text):
            self._fail_at(self.pos)
        ch = self.text[self.pos]
        if ch == "{":
            return self._object(depth)
        if ch == "[":
            return self._array(depth)
        if ch == '"':
            return self._string()
        for literal, value in (("true", True), ("false", False), ("null", None)):
            if ch == literal[0]:
                for offset, expected in enumerate(literal):
                    index = self.pos + offset
                    if index >= len(self.text) or self.text[index] != expected:
                        self._fail_at(index)
                self.pos += len(literal)
                return value
        match = self._NUMBER.match(self.text, self.pos)
        if not match:
            self._fail_at(self.pos)
        self.pos = match.end()
        token = match.group(0)
        if match.group(1) or match.group(2):
            return float(token)
        try:
            return int(token)
        except ValueError:
            # Past Python's 4300-digit limit: only the sign and "far beyond any range" matter.
            return -_HUGE_INTEGER if token.startswith("-") else _HUGE_INTEGER

    def _string(self):
        start = self.pos
        index = start + 1
        while index < len(self.text):
            ch = self.text[index]
            if ch == "\\":
                index += 2
                continue
            if ch == '"':
                self.pos = index + 1
                try:
                    return json.loads(self.text[start:index + 1])
                except ValueError:
                    self._fail_at(start)
            index += 1
        self._fail_at(len(self.text))

    def _array(self, depth):
        self.pos += 1
        items = []
        self._skip()
        if self.pos < len(self.text) and self.text[self.pos] == "]":
            self.pos += 1
            return items
        while True:
            items.append(self._value(depth + 1))
            self._skip()
            if self.pos >= len(self.text):
                self._fail_at(self.pos)
            ch = self.text[self.pos]
            self.pos += 1
            if ch == "]":
                return items
            if ch != ",":
                self._fail_at(self.pos - 1)

    def _object(self, depth):
        self.pos += 1
        result = {}
        self._skip()
        if self.pos < len(self.text) and self.text[self.pos] == "}":
            self.pos += 1
            return result
        while True:
            self._skip()
            if self.pos >= len(self.text) or self.text[self.pos] != '"':
                self._fail_at(self.pos)
            key = self._string()
            if key in result:
                raise _Wire(_serialization_error("Duplicate key found in payload"))
            self._skip()
            if self.pos >= len(self.text) or self.text[self.pos] != ":":
                self._fail_at(self.pos)
            self.pos += 1
            result[key] = self._value(depth + 1)
            self._skip()
            if self.pos >= len(self.text):
                self._fail_at(self.pos)
            ch = self.text[self.pos]
            self.pos += 1
            if ch == "}":
                return result
            if ch != ",":
                self._fail_at(self.pos - 1)


def _scalar_name(value):
    if isinstance(value, bool):
        return "TRUE_VALUE" if value else "FALSE_VALUE"
    if isinstance(value, int):
        return "NUMBER_VALUE"
    if isinstance(value, float):
        return "DECIMAL_VALUE"
    return "STRING_VALUE"


def _check_string(value):
    if value is None or isinstance(value, str):
        return
    if isinstance(value, list):
        raise _Wire(_serialization_error())
    if isinstance(value, dict):
        raise _Wire(_serialization_error("Start of structure or map found where not expected"))
    raise _Wire(_serialization_error(f"{_scalar_name(value)} cannot be converted to String"))


def _check_integer(value):
    if value is None or (isinstance(value, (int, float)) and not isinstance(value, bool)):
        return
    if isinstance(value, list):
        raise _Wire(_serialization_error())
    if isinstance(value, dict):
        raise _Wire(_serialization_error("Start of structure or map found where not expected"))
    raise _Wire(_serialization_error(f"{_scalar_name(value)} cannot be converted to Integer"))


def _check_string_list(value):
    if value is None:
        return
    if isinstance(value, dict):
        raise _Wire(_serialization_error("Start of structure or map found where not expected"))
    if not isinstance(value, list):
        raise _Wire(_serialization_error("Unexpected field type"))
    for item in value:
        _check_string(item)


_MEMBER_TYPES = {
    "CreatePullThroughCacheRule": {
        "registryId": _check_string, "upstreamRegistryUrl": _check_string, "upstreamRegistry": _check_string,
        "ecrRepositoryPrefix": _check_string, "credentialArn": _check_string, "customRoleArn": _check_string,
        "upstreamRepositoryPrefix": _check_string,
    },
    "UpdatePullThroughCacheRule": {
        "registryId": _check_string, "ecrRepositoryPrefix": _check_string, "credentialArn": _check_string,
        "customRoleArn": _check_string,
    },
    "DeletePullThroughCacheRule": {"registryId": _check_string, "ecrRepositoryPrefix": _check_string},
    "ValidatePullThroughCacheRule": {"registryId": _check_string, "ecrRepositoryPrefix": _check_string},
    "PutRegistryPolicy": {"policyText": _check_string},
    "GetRegistryPolicy": {},
    "DeleteRegistryPolicy": {},
    "GetAccountSetting": {"name": _check_string},
    "DescribePullThroughCacheRules": {
        "registryId": _check_string, "ecrRepositoryPrefixes": _check_string_list, "nextToken": _check_string,
        "maxResults": _check_integer,
    },
}


def _load_body(action, body):
    """The request members of *action* with null members dropped, or a refusing response."""
    text = body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else (body or "")
    try:
        data = _Reader(text).parse()
        if isinstance(data, list):
            raise _Wire(_serialization_error())
        if data is None:
            raise _Wire(_serialization_error("Request payload does not contain the input model"))
        if not isinstance(data, dict):
            raise _Wire(_serialization_error("Unexpected value type in payload"))
        checks = _MEMBER_TYPES[action]
        for name, value in data.items():
            if name in checks:
                checks[name](value)
    except _Wire as exc:
        return None, exc.response
    return {k: v for k, v in data.items() if v is not None}, None


# ── Validation helpers ─────────────────────────────────────

def _param(name, constraint):
    return f"Invalid parameter at '{name}' failed to satisfy constraint: '{constraint}'"


def _prefix_errors(name, value):
    """The joined-message entries for a repository prefix member (null, pattern, then length)."""
    if value is None:
        return [_param(name, "Member must not be null")]
    errors = []
    if not _PREFIX_RE.fullmatch(value):
        errors.append(_param(name, f"Member must satisfy regular expression pattern: {_PREFIX_PATTERN}"))
    # r33-create-slash-only: the length is measured on the raw value, a lone "/" counting as 1.
    if len(value) < 2:
        errors.append(_param(name, "Member must have length greater than or equal to 2"))
    elif len(value) > 30:
        errors.append(_param(name, "Member must have length less than or equal to 30"))
    return errors


def _credential_errors(value):
    if value is None:
        return []
    errors = []
    if not _CREDENTIAL_RE.fullmatch(value):
        errors.append(_param("credentialArn", f"Member must satisfy regular expression pattern: {_CREDENTIAL_PATTERN}"))
    if len(value) < 50:
        errors.append(_param("credentialArn", "Member must have length greater than or equal to 50"))
    elif len(value) > 612:
        errors.append(_param("credentialArn", "Member must have length less than or equal to 612"))
    return errors


def _role_length_errors(value):
    if value is not None and len(value) > 2048:
        return [_param("customRoleArn", "Member must have length less than or equal to 2048")]
    return []


def _invalid(message):
    return _error("InvalidParameterException", message)


def _normalise_prefix(prefix):
    return prefix[:-1] if prefix.endswith("/") else prefix


def _rule_missing(prefix, registry_id):
    return _error(
        "PullThroughCacheRuleNotFoundException",
        f"A pull-through cache rule with repository prefix '{prefix}' does not exist in the registry "
        f"with id '{registry_id}'",
    )


def _registry_checks(action, data):
    """The registryId checks every action starts with: pattern alone, then another account's registry."""
    registry_id = data.get("registryId")
    if registry_id is not None and not _REGISTRY_ID_RE.fullmatch(registry_id):
        return _invalid(_param("registryId", "must satisfy regular expression [0-9]{12}"))
    return None


def _other_registry(action, data):
    registry_id = data.get("registryId")
    if registry_id is not None and registry_id != get_account_id():
        from ministack.core.iam_evaluator import caller_arn

        return _access_denied(
            f"User: {caller_arn()} is not authorized to perform: ecr:{action} on resource: * "
            f"because no resource-based policy allows the ecr:{action} action"
        )
    return None


def _role_account_checks(data):
    """customRoleArn must be a role ARN (step 3) in the target registry's account (step 4)."""
    role = data.get("customRoleArn")
    if not role:
        data.pop("customRoleArn", None)
        return None
    try:
        spec = parse_arn(role)
    except ArnParseError:
        spec = None
    if spec is None or spec.service != "iam" or not spec.resource.startswith("role/"):
        return _invalid("Invalid role ARN supplied in request.")
    if spec.account_id != data.get("registryId", get_account_id()):
        return _access_denied("Cross-account pass role is not allowed.")
    return None


def _pass_role_check(data):
    """Under AUTH=true the caller must be able to pass customRoleArn to ECR (captured r40)."""
    role = data.get("customRoleArn")
    from ministack.app import AUTH

    if not role or not AUTH:
        return None
    from ministack.core.iam_evaluator import _verified_key, enforce

    denied = enforce(_verified_key.get(), "iam:PassRole", "iam", get_region(), role,
                     service_context={"iam:PassedToService": "ecr.amazonaws.com"})
    if denied is None or not hasattr(denied, "principal_arn"):
        return None
    reason = (
        "with an explicit deny in an identity-based policy" if denied.decision == "Deny"
        else "because no identity-based policy allows the iam:PassRole action"
    )
    return _access_denied(
        f"User: {denied.principal_arn} is not authorized to perform: iam:PassRole on resource: {role} {reason}"
    )


def _credential_region_check(credential):
    """The secret must be in the registry's own region and account (same partition)."""
    try:
        spec = parse_arn(credential)
    except ArnParseError:
        return _invalid(_MSG_SECRET_REGION)
    if spec.region != get_region() or spec.account_id != get_account_id():
        return _invalid(_MSG_SECRET_REGION)
    return None


def _caller_identity_denied(credential):
    """Under AUTH=true, whether the caller's own identity policies refuse GetSecretValue on the secret through
    ECR (aws:CalledVia ecr.amazonaws.com). Root passes, as for every check ``enforce`` runs (captured r38, r40)."""
    from ministack.app import AUTH

    if not AUTH:
        return False
    from ministack.core.iam_evaluator import _verified_key, enforce

    denied = enforce(_verified_key.get(), "secretsmanager:GetSecretValue", "secretsmanager", get_region(),
                     credential, service_context={"aws:CalledVia": ["ecr.amazonaws.com"]})
    return denied is not None and hasattr(denied, "principal_arn")


def _caller_policy_denied(credential):
    """Under AUTH=true, whether an explicit Deny in the secret's resource policy refuses the caller's read through
    ECR, the root key included, with aws:CalledVia ecr.amazonaws.com (captured r28)."""
    from ministack.app import AUTH

    if not AUTH:
        return False
    from ministack.core.iam_evaluator import EvalContext, caller_arn, evaluate_resource_policy
    from ministack.services import secretsmanager

    caller = caller_arn()
    found = secretsmanager.secret_policy_for_arn(credential)
    if found and found[0]:
        ctx = EvalContext(principal_arn=caller, principal_account=caller.split(":")[4],
                          principal_type="Root" if caller.endswith(":root") else
                          "AssumedRole" if ":assumed-role/" in caller else "User",
                          action="secretsmanager:GetSecretValue", resource_arn=credential, region=get_region(),
                          service_context={"aws:calledvia": ["ecr.amazonaws.com"]})
        return evaluate_resource_policy(found[0], ctx).decision == "Deny"
    return False


def _caller_read_denied(credential):
    """Under AUTH=true, whether the caller's read of the secret through ECR is refused by its resource policy or
    its own identity policies (the order of the two lives in ``_secret_checks``)."""
    return _caller_policy_denied(credential) or _caller_identity_denied(credential)


def _role_read_denied(credential, key_id):
    """Under AUTH=true, whether the service-linked role's own read of the secret fails: an explicit Deny in the
    secret's resource policy naming it, or a customer managed key MiniStack KMS holds whose key policy does not
    name it. The role is evaluated as a fixed principal; it does not pass through aws:CalledVia."""
    from ministack.app import AUTH

    if not AUTH:
        return False
    from ministack.core.iam_evaluator import EvalContext, evaluate_resource_policy
    from ministack.services import kms, secretsmanager

    account = get_account_id()
    role = f"arn:aws:iam::{account}:role/aws-service-role/{SERVICE_LINKED_ROLE_SERVICE}/{SERVICE_LINKED_ROLE_NAME}"
    found = secretsmanager.secret_policy_for_arn(credential)
    if found and found[0]:
        ctx = EvalContext(principal_arn=role, principal_account=account, principal_type="AssumedRole",
                          action="secretsmanager:GetSecretValue", resource_arn=credential, region=get_region())
        if evaluate_resource_policy(found[0], ctx).decision == "Deny":
            return True
    return bool(key_id) and kms.key_policy_grants_named_principal(key_id, "Decrypt", role) is False


def _secret_checks(credential):
    """Secret lookup and read, as ECR reads it for the caller: not found, unreadable, key state."""
    from ministack.services import kms, secretsmanager

    try:
        spec = parse_arn(credential)
    except ArnParseError:
        return _error("SecretNotFoundException", _MSG_SECRET_NOT_FOUND)
    if spec.partition != "aws":
        return _error("SecretNotFoundException", _MSG_SECRET_NOT_FOUND)
    if _caller_identity_denied(credential):
        # A non-root caller whose identity policy does not cover the ARN is refused before the lookup (captured r40).
        return _error("UnableToAccessSecretException", _MSG_SECRET_UNREADABLE)
    state, key_id = secretsmanager.pull_through_secret_state(credential)
    if state == "missing" and len(spec.resource[len("secret:"):]) > 519:
        # A real secret's ARN resource is at most 519 characters (512-character name, a dash, a 6-character
        # suffix); a longer one names no secret, and AWS answers that it cannot be accessed (r02-cred-len-612).
        return _error("UnableToAccessSecretException", _MSG_SECRET_UNREADABLE)
    if state == "missing":
        return _error("SecretNotFoundException", _MSG_SECRET_NOT_FOUND)
    if state == "unreadable":
        return _error("UnableToAccessSecretException", _MSG_SECRET_UNREADABLE)
    if key_id and kms.held_key_state(key_id) in ("Disabled", "PendingDeletion"):
        return _error("UnableToDecryptSecretValueException", _MSG_SECRET_UNDECRYPTABLE)
    if _caller_policy_denied(credential):
        # The secret's resource policy comes after the key state (ledger 169 interim order, uncaptured).
        return _error("UnableToAccessSecretException", _MSG_SECRET_UNREADABLE)
    return None


def _ensure_service_linked_role():
    from ministack.services import iam

    iam.ensure_service_linked_role(SERVICE_LINKED_ROLE_SERVICE, SERVICE_LINKED_ROLE_NAME, SERVICE_LINKED_ROLE_POLICY)


def _upstream_for_url(url):
    """The upstream registry a URL names, or None when it is not a supported registry."""
    if url in _URL_TO_UPSTREAM:
        return _URL_TO_UPSTREAM[url]
    if _ECR_URL_RE.fullmatch(url):
        return "ecr"
    if _AZURE_URL_RE.fullmatch(url):
        return "azure-container-registry"
    return None


# ── Create ────────────────────────────────────────────────

def _create(data):
    # 2-5: registryId pattern, role ARN, role account, another account's registry, PassRole.
    for check in (
        lambda: _registry_checks("CreatePullThroughCacheRule", data),
        lambda: _role_account_checks(data),
        lambda: _other_registry("CreatePullThroughCacheRule", data),
        lambda: _pass_role_check(data),
    ):
        refused = check()
        if refused:
            return refused

    # 6: the joined shape errors, in AWS's fixed member order.
    errors = _role_length_errors(data.get("customRoleArn"))
    url = data.get("upstreamRegistryUrl")
    if url is None:
        errors.append(_param("upstreamRegistryUrl", "Member must not be null"))
    upstream_registry = data.get("upstreamRegistry")
    if upstream_registry is not None and upstream_registry not in _UPSTREAM_REGISTRIES:
        errors.append(_param(
            "upstreamRegistry",
            f"Member must satisfy enum value set: [{', '.join(_UPSTREAM_REGISTRIES)}]",
        ))
    upstream_prefix = data.get("upstreamRepositoryPrefix")
    if upstream_prefix is not None:
        errors += _prefix_errors("upstreamRepositoryPrefix", upstream_prefix)
    prefix = data.get("ecrRepositoryPrefix")
    errors += _prefix_errors("ecrRepositoryPrefix", prefix)
    credential = data.get("credentialArn")
    errors += _credential_errors(credential)
    if errors:
        return _invalid("; ".join(errors))
    prefix = _normalise_prefix(prefix)
    registry_id = get_account_id()

    _ensure_service_linked_role()

    # 7-8: the secret.
    if credential is not None:
        refused = _credential_region_check(credential) or _secret_checks(credential)
        if refused:
            return refused

    # 9: the URL.
    if not url.strip() or len(url) >= 2048:
        return _internal_failure()
    upstream = _upstream_for_url(url)
    if upstream is None:
        return _error(
            "UnsupportedUpstreamRegistryException",
            f"The upstream registry URL {url} is invalid. Specify a valid upstream registry URL and try again",
        )

    # 10-11: same registry, authentication requirement and credential type.
    role = data.get("customRoleArn")
    if upstream == "ecr":
        url_account, url_region = _ECR_URL_RE.fullmatch(url).groups()
        if url_account == registry_id and url_region == get_region():
            return _error("UnsupportedUpstreamRegistryException",
                          "Upstream registry URL cannot be the same as the cache registry.")
        if credential is not None:
            return _invalid(_MSG_TYPE_MISMATCH)
        if role is None:
            return _invalid(_MSG_NEEDS_ROLE)
        if url_account == registry_id:
            return _invalid(_MSG_TYPE_MISMATCH)
    elif upstream in _CREDENTIAL_UPSTREAMS:
        if credential is None:
            return _error("UnsupportedUpstreamRegistryException", _MSG_NEEDS_SECRET)
        if role is not None:
            return _invalid(_MSG_TYPE_MISMATCH)
    elif credential is not None or role is not None:
        return _error("UnsupportedUpstreamRegistryException", _MSG_NEEDS_NO_AUTH)

    # 12-14: ROOT with an upstream prefix, the quota, the duplicate.
    if prefix == "ROOT" and upstream_prefix is not None:
        return _invalid("Invalid input: Must specify ecr-repository-prefix when specify upstream-repository-prefix.")
    if len(_rules) >= MAX_RULES:
        return _error(
            "LimitExceededException",
            f"Registry id '{registry_id}' has the maximum number of pull-through cache rules allowed, "
            f"which is {MAX_RULES}.",
        )
    if prefix in _rules:
        return _error(
            "PullThroughCacheRuleAlreadyExistsException",
            f"A pull-through cache rule with repository prefix '{prefix}' already exists in the registry "
            f"with id '{registry_id}'",
        )

    now = round(time.time(), 3)
    rule = {
        "ecrRepositoryPrefix": prefix,
        "upstreamRegistryUrl": url,
        "upstreamRegistry": upstream,
        "createdAt": now,
        "updatedAt": now,
    }
    if credential is not None:
        rule["credentialArn"] = credential
    if role is not None:
        rule["customRoleArn"] = role
    if upstream_prefix is not None:
        rule["upstreamRepositoryPrefix"] = upstream_prefix
    _rules[prefix] = rule

    response = {
        "ecrRepositoryPrefix": prefix,
        "upstreamRegistryUrl": url,
        "createdAt": now,
        "registryId": registry_id,
    }
    if _authenticated(rule):
        response["upstreamRegistry"] = upstream
    for member in ("credentialArn", "customRoleArn", "upstreamRepositoryPrefix"):
        if member in rule:
            response[member] = rule[member]
    return json_response(response, content_type="application/x-amz-json-1.1")


def _authenticated(rule):
    return "credentialArn" in rule or "customRoleArn" in rule


# ── Update ────────────────────────────────────────────────

def _update(data):
    for check in (
        lambda: _registry_checks("UpdatePullThroughCacheRule", data),
        lambda: _role_account_checks(data),
        lambda: _other_registry("UpdatePullThroughCacheRule", data),
        lambda: _pass_role_check(data),
    ):
        refused = check()
        if refused:
            return refused

    # 6: the joined shape errors (upstream members of an update body are ignored, not validated).
    errors = _role_length_errors(data.get("customRoleArn"))
    prefix = data.get("ecrRepositoryPrefix")
    errors += _prefix_errors("ecrRepositoryPrefix", prefix)
    credential = data.get("credentialArn")
    errors += _credential_errors(credential)
    if errors:
        return _invalid("; ".join(errors))
    prefix = _normalise_prefix(prefix)
    registry_id = get_account_id()
    role = data.get("customRoleArn")

    # 7: one of the two credentials.
    if credential is None and role is None:
        return _invalid(_MSG_ONE_CREDENTIAL)

    # 8-9: the secret.
    if credential is not None:
        refused = _credential_region_check(credential) or _secret_checks(credential)
        if refused:
            return refused

    # 10-11: not both, then the rule.
    if credential is not None and role is not None:
        return _invalid(_MSG_ONE_CREDENTIAL)
    rule = _rules.get(prefix)
    if rule is None:
        return _rule_missing(prefix, registry_id)

    # 12: the rule's type decides which credential it takes.
    if not _authenticated(rule):
        return _invalid(_MSG_UNAUTHENTICATED_RULE)
    if "credentialArn" in rule and role is not None:
        return _invalid(_MSG_NEEDS_SECRET_INPUT)
    if "customRoleArn" in rule and credential is not None:
        return _invalid(_MSG_NEEDS_ROLE)

    # 13: write.
    now = round(time.time(), 3)
    if credential is not None:
        rule["credentialArn"] = credential
    else:
        rule["customRoleArn"] = role
    rule["updatedAt"] = now
    response = {"ecrRepositoryPrefix": prefix, "registryId": registry_id, "updatedAt": now}
    for member in ("credentialArn", "customRoleArn", "upstreamRepositoryPrefix"):
        if member in rule:
            response[member] = rule[member]
    return json_response(response, content_type="application/x-amz-json-1.1")


# ── Delete ────────────────────────────────────────────────

def _delete(data):
    action = "DeletePullThroughCacheRule"
    refused = _registry_checks(action, data) or _other_registry(action, data)
    if refused:
        return refused
    errors = _prefix_errors("ecrRepositoryPrefix", data.get("ecrRepositoryPrefix"))
    if errors:
        return _invalid("; ".join(errors))
    prefix = _normalise_prefix(data["ecrRepositoryPrefix"])
    registry_id = get_account_id()
    rule = _rules.get(prefix)
    if rule is None:
        return _rule_missing(prefix, registry_id)
    del _rules[prefix]
    response = {
        "ecrRepositoryPrefix": prefix,
        "upstreamRegistryUrl": rule["upstreamRegistryUrl"],
        "createdAt": rule["createdAt"],
        "registryId": registry_id,
    }
    for member in ("credentialArn", "customRoleArn", "upstreamRepositoryPrefix"):
        if member in rule:
            response[member] = rule[member]
    return json_response(response, content_type="application/x-amz-json-1.1")


# ── Describe ──────────────────────────────────────────────

_TOKEN_TAG = "ptc-rules:"


def _describe_item(rule, registry_id):
    item = {
        "ecrRepositoryPrefix": rule["ecrRepositoryPrefix"],
        "upstreamRegistryUrl": rule["upstreamRegistryUrl"],
        "createdAt": rule["createdAt"],
        "registryId": registry_id,
    }
    if _authenticated(rule):
        item["upstreamRegistry"] = rule["upstreamRegistry"]
        item["updatedAt"] = rule["updatedAt"]
    for member in ("credentialArn", "customRoleArn", "upstreamRepositoryPrefix"):
        if member in rule:
            item[member] = rule[member]
    return item


def _decode_token(token):
    try:
        text = base64.b64decode(token.encode(), validate=True).decode()
        if text.startswith(_TOKEN_TAG):
            offset = int(text[len(_TOKEN_TAG):])
            if offset >= 0:
                return offset
    except (ValueError, UnicodeError):
        pass
    return None


def _describe(data):
    action = "DescribePullThroughCacheRules"
    refused = _registry_checks(action, data) or _other_registry(action, data)
    if refused:
        return refused
    registry_id = get_account_id()

    errors = []
    max_results = data.get("maxResults")
    if max_results is not None:
        if max_results in (float("inf"), float("-inf")):  # answers as out of range, by sign
            max_results = 1001 if max_results > 0 else 0
        else:
            max_results = int(max_results)
        if max_results < 1:
            errors.append(_param("maxResults", "Member must have value greater than or equal to 1"))
        elif max_results > 1000:
            errors.append(_param("maxResults", "Member must have value less than or equal to 1000"))
    prefixes = data.get("ecrRepositoryPrefixes")
    if prefixes is not None:
        if any(item is None for item in prefixes):
            errors.append(_param("ecrRepositoryPrefix", "must not be null"))
        elif not prefixes:
            errors.append(_param("ecrRepositoryPrefixes", "Member must have length greater than or equal to 1"))
        elif len(prefixes) > 100:
            errors.append(_param("ecrRepositoryPrefixes", "Member must have length less than or equal to 100"))
        elif any(_prefix_errors("ecrRepositoryPrefixes", item) for item in prefixes):
            errors.append(_param(
                "ecrRepositoryPrefixes",
                "Member must satisfy constraint: [Member must have length less than or equal to 30, "
                "Member must have length greater than or equal to 2, "
                f"Member must satisfy regular expression pattern: {_PREFIX_PATTERN}]",
            ))
    if errors:
        return _invalid("; ".join(errors))

    if prefixes is not None:
        if max_results is not None:
            return _invalid(_param("MaxResults", "Cannot provide MaxResults if providing RepositoryPrefixes"))
        if "nextToken" in data:
            return _invalid(_param("NextToken", "Cannot provide NextToken if providing RepositoryPrefixes"))

    offset = 0
    token = data.get("nextToken")
    if token is not None and token.strip():
        offset = _decode_token(token)
        if offset is None:
            return _invalid(_param("NextToken", "Invalid next token provided"))

    rules = [rule for _, rule in _rules.items()]
    if prefixes is not None:
        wanted = []
        for item in prefixes:
            item = _normalise_prefix(item)
            if item not in wanted:
                wanted.append(item)
        held = {rule["ecrRepositoryPrefix"] for rule in rules}
        for item in wanted:
            if item not in held:
                return _rule_missing(item, registry_id)
        rules = [rule for rule in rules if rule["ecrRepositoryPrefix"] in wanted]
        page, more = rules, False
    else:
        size = max_results or 100
        page = rules[offset:offset + size]
        more = offset + size < len(rules)

    response = {"pullThroughCacheRules": [_describe_item(rule, registry_id) for rule in page]}
    if more:
        response["nextToken"] = base64.b64encode(f"{_TOKEN_TAG}{offset + len(page)}".encode()).decode()
    return json_response(response, content_type="application/x-amz-json-1.1")


# ── Validate ──────────────────────────────────────────────

_MSG_PARSE_SECRET = "Unable to parse secret value. Secret must contain 'username' and 'accessToken' keys."


def _secret_parses(value):
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return False
    return isinstance(parsed, dict) and "username" in parsed and "accessToken" in parsed


def _validate(data):
    action = "ValidatePullThroughCacheRule"
    refused = _registry_checks(action, data) or _other_registry(action, data)
    if refused:
        return refused
    errors = _prefix_errors("ecrRepositoryPrefix", data.get("ecrRepositoryPrefix"))
    if errors:
        return _invalid("; ".join(errors))
    prefix = _normalise_prefix(data["ecrRepositoryPrefix"])
    registry_id = get_account_id()
    rule = _rules.get(prefix)
    if rule is None:
        return _rule_missing(prefix, registry_id)

    response = {"ecrRepositoryPrefix": prefix, "registryId": registry_id}
    failure = None
    credential = rule.get("credentialArn")
    if credential is not None:
        from ministack.services import kms, secretsmanager

        state, key_id = secretsmanager.pull_through_secret_state(credential)
        if state == "ok" and key_id and kms.held_key_state(key_id) in ("Disabled", "PendingDeletion"):
            state = "undecryptable"
        if state == "ok" and _caller_read_denied(credential):
            state = "denied"
        if state != "ok":
            failure = ("UNABLE_TO_ACCESS_SECRET", _MSG_SECRET_UNREADABLE)
        elif _role_read_denied(credential, key_id):
            failure = ("UNABLE_TO_ACCESS_SECRET", _MSG_SECRET_ROLE_UNREADABLE)
        elif not _secret_parses(secretsmanager.pull_through_secret_string(credential)):
            failure = ("UNABLE_TO_PARSE_SECRET", _MSG_PARSE_SECRET)
    elif "customRoleArn" in rule:
        from ministack.services import iam

        if not iam.role_trusts_service(rule["customRoleArn"], SERVICE_LINKED_ROLE_SERVICE):
            failure = ("UNABLE_TO_ASSUME_ROLE", _MSG_ASSUME_ROLE)
    if failure is None and credential is not None:
        outcome = _bearer_token(_upstream_host(rule), _credentials(credential))[0]
        if outcome == "denied":
            failure = ("UPSTREAM_REGISTRY_ACCESS_DENIED", _MSG_UPSTREAM_DENIED)
        elif outcome == "unavailable":
            failure = ("UNABLE_TO_VALIDATE", _MSG_UNABLE_TO_VALIDATE)
    if failure is None or failure[1] != _MSG_SECRET_UNREADABLE:
        response["upstreamRegistryUrl"] = rule["upstreamRegistryUrl"]
        if credential is not None:
            response["credentialArn"] = credential
    if "customRoleArn" in rule:
        response["customRoleArn"] = rule["customRoleArn"]
    if "upstreamRepositoryPrefix" in rule:
        response["upstreamRepositoryPrefix"] = rule["upstreamRepositoryPrefix"]
    response["isValid"] = failure is None
    response["failure"] = (
        "" if failure is None else json.dumps({"failureCode": failure[0], "failureReason": failure[1]}, separators=(",", ":"))
    )
    return json_response(response, content_type="application/x-amz-json-1.1")


# ── Upstream registry client ──────────────────────────────

_INDEX_TYPES = frozenset((
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.index.v1+json",
))
_UPSTREAM_TIMEOUT = 15


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_opener = urllib.request.build_opener(_NoRedirect)


_RETRIES_ON_429 = 3
_RETRY_AFTER_CAP = 10.0
_MAX_BODY = 1 << 20  # an upstream answer (probe, token or error body) larger than 1 MiB is no answer


def _read_capped(stream):
    """The body of *stream*, or raises OSError when it holds more than 1 MiB (v20 line 188: no answer)."""
    body = stream.read(_MAX_BODY + 1)
    if len(body) > _MAX_BODY:
        raise OSError("upstream answer over 1 MiB")
    return body


def _http_get(url, headers):
    """One GET, retried a fixed bounded number of times while the upstream answers 429 (honouring Retry-After)."""
    for attempt in range(_RETRIES_ON_429 + 1):
        status, reply, body = _http_get_once(url, headers)
        if status != 429 or attempt == _RETRIES_ON_429:
            return status, reply, body
        try:
            delay = float(reply.get("retry-after", ""))
        except ValueError:
            delay = float(2 ** attempt)
        time.sleep(max(0.0, min(delay, _RETRY_AFTER_CAP)))
    return status, reply, body


def _http_get_once(url, headers):
    """``(status, lower-cased headers, body)`` of one GET; raises OSError when nothing answers.
    Redirects are followed without the Authorization header (blob stores refuse it)."""
    for _ in range(5):
        if not url.lower().startswith("https://"):
            raise OSError("redirect to a non-https URL")
        request = urllib.request.Request(url, headers=headers)
        try:
            with _opener.open(request, timeout=_UPSTREAM_TIMEOUT) as response:
                return response.status, {k.lower(): v for k, v in response.headers.items()}, _read_capped(response)
        except urllib.error.HTTPError as err:
            reply = {k.lower(): v for k, v in err.headers.items()}
            if err.code in (301, 302, 303, 307, 308) and reply.get("location"):
                url = urllib.parse.urljoin(url, reply["location"])
                headers = {k: v for k, v in headers.items() if k.lower() != "authorization"}
                continue
            return err.code, reply, _read_capped(err)
    raise OSError("too many redirects")


def _upstream_host(rule):
    return rule["upstreamRegistryUrl"]


def _credentials(credential):
    """``(username, accessToken)`` held by the rule's secret, or None when it holds none."""
    from ministack.services import secretsmanager

    try:
        parsed = json.loads(secretsmanager.pull_through_secret_string(credential) or "")
    except (TypeError, ValueError):
        return None
    if isinstance(parsed, dict) and isinstance(parsed.get("username"), str) and isinstance(parsed.get("accessToken"), str):
        return parsed["username"], parsed["accessToken"]
    return None


def _challenge(headers):
    value = headers.get("www-authenticate", "")
    if not value.lower().startswith("bearer "):
        return None
    return dict(re.findall(r'(\w+)="([^"]*)"', value))


def _bearer_token(host, credentials, scope=None):
    """``(outcome, token)``: outcome is "ok" (token is None when the registry wants none), "denied"
    (the registry refused the credentials) or "unavailable" (no answer)."""
    try:
        status, headers, _ = _http_get(f"https://{host}/v2/", {})
        if status != 401:
            return ("ok", None) if status < 400 else ("unavailable", None)
        challenge = _challenge(headers)
        if challenge is None or "realm" not in challenge:
            return "denied", None
        query = {"service": challenge["service"]} if "service" in challenge else {}
        if scope:
            query["scope"] = scope
        request_headers = {}
        if credentials:
            raw = f"{credentials[0]}:{credentials[1]}".encode()
            request_headers["Authorization"] = "Basic " + base64.b64encode(raw).decode()
        status, _, body = _http_get(challenge["realm"] + "?" + urllib.parse.urlencode(query), request_headers)
    except (OSError, http.client.HTTPException):  # a truncated HTTP answer is no answer (v20 line 195)
        return "unavailable", None
    if status in (401, 403):
        return "denied", None
    if status != 200:
        return "unavailable", None
    try:
        reply = json.loads(body)
    except ValueError:
        return "unavailable", None
    # A token answer that is not an object or holds no string token or access_token is no answer (v20 line 195).
    if not isinstance(reply, dict):
        return "unavailable", None
    # An empty token or access_token is no answer either; the other member is tried (v20 lines 195, 188).
    for member in ("token", "access_token"):
        if isinstance(reply.get(member), str) and reply[member]:
            return "ok", reply[member]
    return "unavailable", None


_DOCKER_HINT = ("no Docker daemon answered; mount the Docker socket into MiniStack "
                "(-v /var/run/docker.sock:/var/run/docker.sock) so the pull-through cache can pull through the host daemon")
_CHUNK = 1 << 20
_TAR_SPOOL = 64 << 20  # a saved image stays in memory up to this size, then spills to a temporary file


def _docker_client():
    """The host daemon's client, or None when the docker package is missing or no daemon answers (hint logged)."""
    try:
        import docker

        client = docker.from_env(timeout=_UPSTREAM_TIMEOUT * 4)
        try:
            client.ping()
        except BaseException:
            client.close()
            raise
        return client
    except Exception:  # noqa: BLE001 - ImportError, DockerException and connection errors all mean no daemon
        logger.warning("pull-through cache: %s", _DOCKER_HINT)
        return None


_DAEMON_THROTTLE_WORDS = ("toomanyrequests", "too many requests")


def _daemon_outcome(message, has_credentials):
    text = message.lower()
    # Not-found words first: Docker Hub's "pull access denied ... repository does not exist" is a missing name.
    if any(word in text for word in ("not found", "manifest unknown", "name unknown", "does not exist")):
        return "notfound"
    if any(word in text for word in _DAEMON_THROTTLE_WORDS):
        return "throttled"
    if any(word in text for word in ("denied", "unauthorized", "authentication required", "forbidden")):
        return "denied" if has_credentials else "notfound"
    return "unavailable"


def _read_member(tar, member):
    out = bytearray()
    stream = tar.extractfile(member)
    while True:
        chunk = stream.read(_CHUNK)
        if not chunk:
            return bytes(out)
        out.extend(chunk)


def _image_from_tar(tar):
    """``(media_type, manifest_bytes, blobs)`` from a saved image: the OCI layout's own index and blobs, or a
    docker-v2 manifest rebuilt from ``manifest.json`` (uncompressed layer digests) when the daemon holds no manifest."""
    names = {m.name: m for m in tar.getmembers() if m.isfile()}
    blobs = {}
    for name, member in names.items():
        if name.startswith("blobs/sha256/"):
            blobs["sha256:" + name.rsplit("/", 1)[1]] = member
    if "index.json" in names:
        index = json.loads(_read_member(tar, names["index.json"]))
        descriptor = (index.get("manifests") or [{}])[0]
        digest = descriptor.get("digest")
        if digest in blobs:
            raw = _read_member(tar, blobs[digest])
            media_type = descriptor.get("mediaType") or json.loads(raw).get("mediaType") or ""
            return media_type, raw, {d: _read_member(tar, m) for d, m in blobs.items() if d != digest} | {digest: raw}
    entry = json.loads(_read_member(tar, names["manifest.json"]))[0]

    def blob(path):
        data = _read_member(tar, names[path])
        return {"mediaType": "", "digest": "sha256:" + hashlib.sha256(data).hexdigest(), "size": len(data)}, data

    held = {}
    config, config_bytes = blob(entry["Config"])
    config["mediaType"] = "application/vnd.docker.container.image.v1+json"
    held[config["digest"]] = config_bytes
    layers = []
    for path in entry["Layers"]:
        layer, layer_bytes = blob(path)
        layer["mediaType"] = "application/vnd.docker.image.rootfs.diff.tar"
        held[layer["digest"]] = layer_bytes
        layers.append(layer)
    raw = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                      "config": config, "layers": layers}, indent=3).encode()
    return "application/vnd.docker.distribution.manifest.v2+json", raw, held


_DAEMON_BACKOFF = (1.0, 2.0, 4.0)  # the daemon gives no Retry-After: wait 1 s, 2 s, 4 s (v20 line 187)
_PULL_DEADLINE = 300.0  # [ministack-policy] a pull (retries included) that runs past this many seconds is "unavailable"
_FILL_WORKERS = 4  # [ministack-policy] requests that may fetch from an upstream registry share this many threads
_fill_executor = concurrent.futures.ThreadPoolExecutor(max_workers=_FILL_WORKERS, thread_name_prefix="ecr-fill")


def may_fetch(name):
    """Whether a request on repository *name* may start a fill or an on-demand layer fetch: a rule covers the
    name or the repository already holds an origin."""
    return isinstance(name, str) and (fillable(name) or _current_origin(name) is not None)


async def run_in_fill_pool(fn, *args):
    """Run blocking *fn* on the bounded fill executor, with the caller's request scope."""
    context = contextvars.copy_context()
    return await asyncio.get_running_loop().run_in_executor(_fill_executor, context.run, fn, *args)


async def run_fetching(name, needed, fn, *args):
    """Run *fn* on the fill executor when a request on repository *name* will contact an upstream registry
    (*needed*, a callable, says so), else inline as every other repository's request runs."""
    if may_fetch(name) and needed():
        return await run_in_fill_pool(fn, *args)
    return fn(*args)


def images_need_fetch(name, image_ids):
    """Whether BatchGetImage of *image_ids* on *name* would contact the upstream: a tag or digest not held."""
    from ministack.services import ecr

    if not may_fetch(name):
        return False
    if not isinstance(image_ids, list):
        return False  # a request the handler refuses before any fill
    try:
        for image_id in image_ids:
            if not isinstance(image_id, dict) or _unfillable_image_id(image_id):
                continue
            if name in ecr._repositories and ecr._find_image(name, image_id) is not None:
                continue
            digest = image_id.get("imageDigest")
            if (not image_id.get("imageTag") and isinstance(digest, str) and name in _origins
                    and not _named_by_stored_index(name, digest)):
                continue  # a digest the stored index does not name is answered without a fetch
            return True
    except Exception:  # noqa: BLE001 - an odd request goes to the pool, where the handler answers it
        return True
    return False


def manifest_needs_fetch(name, ref):
    """Whether a V2 manifest request for *ref* on *name* would contact the upstream."""
    from ministack.services import ecr

    if not may_fetch(name):
        return False
    if name in ecr._repositories and ecr._resolve_manifest(name, ref)[1] is not None:
        return False
    return not (ref.startswith("sha256:") and name in _origins and not _named_by_stored_index(name, ref))


def layers_need_fetch(name, digests):
    """Whether a request for layer *digests* of *name* would contact the upstream: a layer a stored manifest
    names that is not held."""
    if not may_fetch(name):
        return False
    return any(isinstance(d, str) and layer_fetch_blocked(name, d) for d in digests)


def _daemon_pull(host, path, reference, credentials):
    """A daemon pull, retried up to three times after the first attempt while the upstream rate-limits it.
    The whole pull has a deadline of ``_PULL_DEADLINE`` seconds; past it the outcome is "unavailable"."""
    deadline = time.monotonic() + _PULL_DEADLINE
    for delay in (*_DAEMON_BACKOFF, None):
        result = _daemon_pull_once(host, path, reference, credentials, deadline)
        if delay is None or result[0] != "throttled":
            break
        if time.monotonic() + delay >= deadline:
            return "unavailable", "", b"", {}
        time.sleep(delay)
    return ("unavailable", *result[1:]) if result[0] == "throttled" else result


def _daemon_pull_once(host, path, reference, credentials, deadline=None):
    """Pull ``<host>/<path>:<tag>`` (or ``@<digest>``) through the host Docker daemon: ``(outcome, media_type,
    manifest_bytes, blobs)`` with outcome "ok", "notfound", "denied", "unavailable" or "throttled". *blobs* maps digest to bytes
    for every manifest, config and layer the daemon holds for the reference (one platform of an index)."""
    import tarfile
    import tempfile

    client = _docker_client()
    if client is None:
        return "unavailable", "", b"", {}
    try:
        repository = f"{host}/{path}"
        by_digest = reference.startswith("sha256:")
        auth = {"username": credentials[0], "password": credentials[1]} if credentials else None
        try:
            events = client.api.pull(repository + ("@" + reference if by_digest else ""), tag=None if by_digest else reference,
                                     stream=True, decode=True, auth_config=auth)
            for event in events:
                if deadline is not None and time.monotonic() > deadline:
                    logger.warning("pull-through cache: pulling %s:%s ran past its %s s deadline", repository, reference,
                                   _PULL_DEADLINE)
                    return "unavailable", "", b"", {}
                if isinstance(event, dict) and event.get("error"):
                    return _daemon_outcome(str(event["error"]), bool(credentials)), "", b"", {}
            with tempfile.SpooledTemporaryFile(max_size=_TAR_SPOOL) as spool:
                for chunk in client.api.get_image(repository + ("@" + reference if by_digest else ":" + reference)):
                    if deadline is not None and time.monotonic() > deadline:
                        logger.warning("pull-through cache: saving %s:%s ran past its %s s deadline", repository,
                                       reference, _PULL_DEADLINE)
                        return "unavailable", "", b"", {}
                    spool.write(chunk)
                spool.seek(0)
                with tarfile.open(fileobj=spool, mode="r:") as tar:
                    media_type, raw, blobs = _image_from_tar(tar)
        except Exception as err:  # noqa: BLE001 - a daemon refusal or a broken tar is an outcome, never a crash
            status = getattr(err, "status_code", None)
            if status == 404:
                return "notfound", "", b"", {}
            if status == 429:
                return "throttled", "", b"", {}
            if status in (401, 403):
                return ("denied" if credentials else "notfound"), "", b"", {}
            if status is not None:
                return _daemon_outcome(str(getattr(err, "explanation", "") or err), bool(credentials)), "", b"", {}
            logger.warning("pull-through cache: pulling %s:%s through the daemon failed: %s", repository, reference, err)
            return "unavailable", "", b"", {}
        return "ok", media_type, raw, blobs
    finally:
        try:
            client.close()
        except Exception:  # noqa: BLE001 - closing is best effort
            pass


def _fetch(origin, reference):
    """One image from the origin registry through the host daemon: ``(outcome, media_type, manifest, blobs)``."""
    credentials = _credentials(origin["credentialArn"]) if origin.get("credentialArn") else None
    return _daemon_pull(origin["host"], origin["path"], reference, credentials)


# ── First-pull fill ───────────────────────────────────────

def match_rule(name):
    """``(prefix, rule, upstream_path)`` of the most specific rule covering cache repository *name*, or None.
    ``<prefix>/<rest>`` maps to ``<upstreamRepositoryPrefix>/<rest>``; ROOT covers every name unchanged."""
    if not isinstance(name, str):
        return None
    best = None
    for prefix, rule in _rules.items():
        if prefix == "ROOT":
            rest, rank = name, 0
        elif name.startswith(prefix + "/") and len(name) > len(prefix) + 1:
            rest, rank = name[len(prefix) + 1:], len(prefix) + 1
        else:
            continue
        if best is None or rank > best[0]:
            best = (rank, prefix, rule, rest)
    if best is None:
        return None
    _, prefix, rule, rest = best
    upstream_prefix = rule.get("upstreamRepositoryPrefix")
    if prefix != "ROOT" and upstream_prefix and upstream_prefix != "ROOT":
        rest = f"{upstream_prefix}/{rest}"
    return prefix, rule, rest


def fillable(name):
    """Whether a rule covers *name* and maps it to an upstream name a fill can fetch (no '//', r33)."""
    found = match_rule(name)
    return found is not None and "//" not in found[2]


def _is_size(value):
    return isinstance(value, int) and not isinstance(value, bool)


def manifest_is_wellformed(raw):
    """Whether *raw* is a manifest object whose config, layers and manifests are objects with integer sizes and
    string digests and media types."""
    try:
        manifest = json.loads(raw)
    except (ValueError, RecursionError):
        return False
    if not isinstance(manifest, dict):
        return False
    if manifest.get("mediaType") is not None and not isinstance(manifest["mediaType"], str):
        return False
    config = manifest.get("config")
    if config is not None and not isinstance(config, dict):
        return False
    descriptors = [config] if config else []
    for member in ("layers", "manifests"):
        listed = manifest.get(member)
        if listed is None:
            continue
        if not isinstance(listed, list):
            return False
        descriptors.extend(listed)
    return all(
        isinstance(d, dict)
        and (d.get("size") is None or _is_size(d["size"]))
        and (d.get("digest") is None or isinstance(d["digest"], str))
        and (d.get("mediaType") is None or isinstance(d["mediaType"], str))
        for d in descriptors)


def is_cache_repository(name):
    """Whether repository *name* was filled by a pull-through cache rule (an origin or a cache-only image)."""
    from ministack.services import ecr

    return name in _origins or any(img.get("cacheOnly") for img in ecr._images.get(name, []))


def _descriptor_size(manifest):
    config = manifest.get("config") or {}
    return (config.get("size") or 0) + sum((layer.get("size") or 0) for layer in manifest.get("layers") or [])


def _manifest_row(digest, raw, media_type, tags, pushed_at, manifest, size):
    row = {
        "registryId": get_account_id(),
        "imageId": {"imageDigest": digest},
        "imageManifest": raw.decode("utf-8", "replace"),
        "imageManifestMediaType": media_type,
        "imageTags": list(tags),
        "imagePushedAt": pushed_at,
        "imageDigest": digest,
        "detailOverrides": {"imageSizeInBytes": size, "imageStatus": "ACTIVE"},
    }
    config_type = (manifest.get("config") or {}).get("mediaType")
    row["detailOverrides"]["artifactMediaType"] = config_type if media_type not in _INDEX_TYPES else None
    return row


def _move_tag(images, digest, tag):
    """Take *tag* off every image but *digest*, as a push of the same tag does."""
    if tag:
        for img in images:
            if img["imageDigest"] != digest and tag in img.get("imageTags", []):
                img["imageTags"].remove(tag)


def _store(name, origin, raw, media_type, tag, blobs):
    """Store the pulled manifest in the cache repository with the blobs the daemon holds for it. An index keeps
    only the child manifests the daemon holds; the others are pulled by digest when asked for (v20 line 90)."""
    from ministack.services import ecr

    digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    try:
        manifest = json.loads(raw)
    except ValueError:
        manifest = {}
    now = round(time.time(), 3)
    rows = []
    size = _descriptor_size(manifest)
    manifests = [manifest]
    if media_type in _INDEX_TYPES or "manifests" in manifest:
        sizes = []
        manifests = []
        for descriptor in manifest.get("manifests", []):
            child_digest = descriptor.get("digest")
            child_raw = blobs.get(child_digest)
            if child_raw is None or "sha256:" + hashlib.sha256(child_raw).hexdigest() != child_digest:
                continue
            if not manifest_is_wellformed(child_raw):
                logger.warning("pull-through cache: malformed upstream child manifest %s skipped", child_digest)
                continue
            child = json.loads(child_raw)
            manifests.append(child)
            child_size = _descriptor_size(child)
            sizes.append(child_size)
            rows.append(_manifest_row(child_digest, child_raw, descriptor.get("mediaType", ""), [], now, child, child_size))
            ecr._manifest_blobs.setdefault(name, {})[child_digest] = child_raw
        size = max(sizes) if sizes else len(raw)
    for held_manifest in manifests:
        for descriptor in [held_manifest.get("config") or {}, *(held_manifest.get("layers") or [])]:
            if isinstance(descriptor, dict) and descriptor.get("digest") in blobs:
                ecr._layer_blobs.setdefault(name, {})[descriptor["digest"]] = blobs[descriptor["digest"]]
    rows.append(_manifest_row(digest, raw, media_type, [tag] if tag else [], now, manifest, size))
    ecr._manifest_blobs.setdefault(name, {})[digest] = raw
    images = ecr._images[name]
    for row in rows:
        held = next((img for img in images if img["imageDigest"] == row["imageDigest"]), None)
        if held is None:
            row["repositoryName"] = name
            images.append(row)
            continue
        if row["imageTags"]:
            held["imageTags"] = sorted(set(held.get("imageTags", [])) | set(row["imageTags"]))
    _move_tag(images, digest, tag)
    for img in images:
        if img["imageDigest"] == digest and tag:
            img["imageId"] = {"imageDigest": digest, "imageTag": tag}
    return digest


def _ecr_upstream_grants(rule, owner, region, path):
    """True when the rule's role trusts the pull-through service and the upstream repository policy allows it."""
    from ministack.core.iam_evaluator import EvalContext, evaluate_resource_policy
    from ministack.services import ecr, iam

    role_arn = rule.get("customRoleArn")
    if not role_arn or not iam.role_trusts_service(role_arn, SERVICE_LINKED_ROLE_SERVICE):
        return False
    policy = ecr._repo_policies.get_scoped(owner, region, path)
    if not policy:
        return False
    ctx = EvalContext(principal_arn=role_arn, principal_account=get_account_id(), principal_type="AssumedRole",
                      action="ecr:BatchGetImage", resource_arn=f"arn:aws:ecr:{region}:{owner}:repository/{path}",
                      region=region)
    return evaluate_resource_policy(policy, ctx).decision == "Allow"


def _fill_from_ecr(name, rule, host, path, image_id):
    """Copy one image in-process from the ECR registry named by *host* into cache repository *name*.

    Returns None when the image is now held, else the ImageNotFound failure (a failed role or policy check,
    a missing upstream repository or a missing image all look the same, and leave no repository)."""
    from ministack.services import ecr

    missing = ("ImageNotFound", "Requested image not found")
    if name in ecr._repositories and ecr._find_image(name, image_id) is not None:
        return None
    owner, region = _ECR_URL_RE.fullmatch(host).groups()
    if not _ecr_upstream_grants(rule, owner, region, path):
        return missing
    if not ecr._repositories.contains_scoped(owner, region, path):
        return missing
    digest = image_id.get("imageDigest")
    tag = image_id.get("imageTag")
    source = next((img for img in ecr._images.get_scoped(owner, region, path, [])
                   if (digest and img["imageDigest"] == digest) or (tag and tag in img.get("imageTags", []))), None)
    if source is None:
        return missing
    if name not in ecr._repositories:
        ecr._create_repository({"repositoryName": name})
    row = copy.deepcopy(source)
    row["registryId"] = get_account_id()
    row["repositoryName"] = name
    row["cacheOnly"] = True
    row["imageTags"] = [tag] if tag else []
    row["imageId"] = {"imageDigest": row["imageDigest"], **({"imageTag": tag} if tag else {})}
    images = ecr._images[name]
    held = next((img for img in images if img["imageDigest"] == row["imageDigest"]), None)
    if held is None:
        images.append(row)
    elif tag and tag not in held.get("imageTags", []):
        held["imageTags"] = sorted({*held.get("imageTags", []), tag})
    _move_tag(images, row["imageDigest"], tag)
    source_manifests = ecr._manifest_blobs.get_scoped(owner, region, path, {})
    if row["imageDigest"] in source_manifests:
        ecr._manifest_blobs.setdefault(name, {})[row["imageDigest"]] = source_manifests[row["imageDigest"]]
    source_layers = ecr._layer_blobs.get_scoped(owner, region, path, {})
    try:
        manifest = json.loads(row.get("imageManifest") or "{}")
    except ValueError:
        manifest = {}
    for descriptor in [manifest.get("config") or {}, *manifest.get("layers", [])]:
        if descriptor.get("digest") in source_layers:
            ecr._layer_blobs.setdefault(name, {})[descriptor["digest"]] = source_layers[descriptor["digest"]]
    return None


def cache_only_layers(name, manifest_text):
    """Digests of a manifest's layers and config that repository *name* holds only through cache-only
    (ECR-upstream fill) images, layers first then the config. PutImage refuses these (captured r26)."""
    from ministack.services import ecr

    def referenced(text):
        try:
            manifest = json.loads(text or "{}")
        except ValueError:
            return []
        if not isinstance(manifest, dict):
            return []
        config = manifest.get("config")
        found = [d.get("digest") for d in manifest.get("layers") or [] if isinstance(d, dict)]
        if isinstance(config, dict):
            found.append(config.get("digest"))
        return [d for d in found if isinstance(d, str)]

    images = ecr._images.get(name, [])
    cached = {d for img in images if img.get("cacheOnly") for d in referenced(img.get("imageManifest"))}
    own = {d for img in images if not img.get("cacheOnly") for d in referenced(img.get("imageManifest"))}
    result = []
    for digest in referenced(manifest_text):
        if digest in cached and digest not in own and digest not in result:
            result.append(digest)
    return result


def _pull_secret_unreadable(credential):
    """Under AUTH=true, whether the secret read of a first pull fails: the caller's read through ECR, the
    service-linked role's read, or a disabled or pending-deletion key (ledger line 166; no read under AUTH=false)."""
    from ministack.app import AUTH

    if not AUTH:
        return False
    from ministack.services import kms, secretsmanager

    state, key_id = secretsmanager.pull_through_secret_state(credential)
    if state == "unreadable" or _caller_read_denied(credential) or _role_read_denied(credential, key_id):
        return True
    return bool(key_id and kms.held_key_state(key_id) in ("Disabled", "PendingDeletion"))


_IMAGE_TAG_RE = re.compile(r"[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}")
_IMAGE_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_IMAGE_NOT_FOUND = ("ImageNotFound", "Requested image not found")


def _unfillable_image_id(image_id):
    """True when *image_id* cannot name an upstream image, so it must never start a fill: not an object holding
    a string imageTag or imageDigest, a tag outside the imageTag pattern or a digest that is not sha256:<64 hex>."""
    if not isinstance(image_id, dict):
        return True
    tag, digest = image_id.get("imageTag"), image_id.get("imageDigest")
    if not isinstance(tag, str) and not isinstance(digest, str):
        return True
    if tag and (not isinstance(tag, str) or not _IMAGE_TAG_RE.fullmatch(tag)):
        return True
    return bool(digest) and (not isinstance(digest, str) or not _IMAGE_DIGEST_RE.fullmatch(digest))


def fill(name, image_ids):
    """Fetch the requested images of cache repository *name* from the rule's upstream into MiniStack ECR.

    Returns None when no rule covers *name*, else one entry per image id: None when the image is now held,
    or ``(failureCode, failureReason)``. The repository is created on the first image that arrives; a name the
    upstream does not know or refuses leaves no repository."""
    found = match_rule(name)
    if found is None:
        return None
    from ministack.services import ecr

    _, rule, path = found
    if not fillable(name):
        # A trailing '/' on upstreamRepositoryPrefix makes '<prefix>//<rest>', not an upstream name: AWS fills
        # nothing and answers as if no rule covered the name (r33).
        return None
    host = _upstream_host(rule)
    results = []
    if _ECR_URL_RE.fullmatch(host):
        return [_IMAGE_NOT_FOUND if _unfillable_image_id(image_id) else _fill_from_ecr(name, rule, host, path, image_id)
                for image_id in image_ids]
    origin = {"host": host, "path": path, "credentialArn": rule.get("credentialArn")}
    for image_id in image_ids:
        if _unfillable_image_id(image_id):
            results.append(_IMAGE_NOT_FOUND)
            continue
        tag = image_id.get("imageTag") or None
        reference = tag or image_id.get("imageDigest")
        held = ecr._find_image(name, image_id) if name in ecr._repositories else None
        if held is not None:
            results.append(None)
            continue
        if rule.get("credentialArn") and _pull_secret_unreadable(rule["credentialArn"]):
            # Uncaptured: ANSWERS pull-secret-unreadable proposed default (ledger line 166).
            results.append(("UpstreamAccessDenied", _MSG_PULL_DENIED))
            continue
        child = False
        if reference.startswith("sha256:") and name in _origins:
            if not _named_by_stored_index(name, reference):
                results.append(_IMAGE_NOT_FOUND)
                continue
            child = True
        outcome, media_type, raw, blobs = _fetch(origin, reference)
        if child and (outcome != "ok" or not manifest_is_wellformed(raw)):
            # Child-platform-on-demand: a child of a stored index the daemon cannot fetch (an attestation
            # manifest, "mismatched image rootfs", unreachable, denied) answers as a missing image.
            logger.warning("pull-through cache: child %s of %s could not be fetched from %s (%s)",
                           reference, name, host, outcome)
            results.append(_IMAGE_NOT_FOUND)
            continue
        if outcome == "denied":
            results.append(("UpstreamAccessDenied", _MSG_PULL_DENIED))
            continue
        if outcome != "ok":
            if outcome == "unavailable":
                logger.warning("pull-through cache: upstream %s did not answer for %s", host, path)
                results.append(("UpstreamUnavailable", "Unable to reach the upstream registry."))
            else:
                results.append(("ImageNotFound", "Requested image not found"))
            continue
        if not manifest_is_wellformed(raw):
            logger.warning("pull-through cache: malformed upstream manifest from %s for %s:%s; nothing was stored",
                           host, path, reference)
            results.append(("UpstreamUnavailable", "Unable to reach the upstream registry."))
            continue
        if name not in ecr._repositories:
            ecr._create_repository({"repositoryName": name})
        _origins[name] = origin
        media_type = media_type or "application/vnd.docker.distribution.manifest.v2+json"
        _store(name, origin, raw, media_type, tag, blobs)
        results.append(None)
    return results


def _named_by_stored_index(name, digest):
    """Whether a stored index of cache repository *name* lists *digest* as a child manifest."""
    from ministack.services import ecr

    for img in ecr._images.get(name, []):
        try:
            manifest = json.loads(img.get("imageManifest") or "{}")
        except ValueError:
            continue
        if isinstance(manifest, dict) and any(
                isinstance(d, dict) and d.get("digest") == digest for d in manifest.get("manifests") or []):
            return True
    return False


def forget_repository(name):
    """DeleteRepository drops the origin of a cache repository (ANSWERS origins-after-repository-delete)."""
    _origins.pop(name, None)


def _current_origin(name):
    """The origin of a cache repository at fetch time: the matching rule's credentialArn, the stored origin
    only when no rule matches (ANSWERS origins-after-repository-delete)."""
    origin = _origins.get(name)
    if origin is None:
        return None
    found = match_rule(name)
    if found is not None:
        return {**origin, "credentialArn": found[1].get("credentialArn")}
    return origin


def ensure_layer(name, digest):
    """Fetch a layer or config blob of a cache repository from its origin when a stored manifest names it and
    it is not held (best effort; a registry that does not answer leaves it missing)."""
    from ministack.services import ecr

    origin = _current_origin(name)
    if origin is None or digest in ecr._layer_blobs.get(name, {}):
        return
    owners = []
    for img in ecr._images.get(name, []):
        try:
            manifest = json.loads(img.get("imageManifest") or "{}")
        except ValueError:
            continue
        if not isinstance(manifest, dict):
            continue
        descriptors = [manifest.get("config") or {}, *(manifest.get("layers") or [])]
        if any(isinstance(d, dict) and d.get("digest") == digest for d in descriptors):
            owners.append(img["imageDigest"])
    if not owners:
        return
    if origin.get("credentialArn") and _pull_secret_unreadable(origin["credentialArn"]):
        # Uncaptured user decision layer-fetch-secret-unreadable (ledger line 168): the layer stays missing.
        logger.warning("pull-through cache: the secret of the rule for %s cannot be read, so layer %s was not "
                       "fetched; the layer stays missing until the image is pulled again with a readable secret",
                       name, digest)
        return
    outcome, _, _, blobs = _fetch(origin, owners[0])
    body = blobs.get(digest)
    if outcome == "ok" and body is not None and "sha256:" + hashlib.sha256(body).hexdigest() == digest:
        ecr._layer_blobs.setdefault(name, {})[digest] = body
        return
    logger.warning("pull-through cache: could not fetch layer %s of %s from %s (%s); check that the upstream "
                   "registry is reachable from MiniStack, then pull again", digest, name, origin["host"], outcome)


def layer_fetch_blocked(name, digest):
    """Whether *digest* is a layer of a stored manifest of a cache repository that is still not held after the
    on-demand fetch (unreachable, 404, digest mismatch, malformed or unreadable secret; v20 line 196)."""
    from ministack.services import ecr

    if _current_origin(name) is None or digest in ecr._layer_blobs.get(name, {}):
        return False
    for img in ecr._images.get(name, []):
        try:
            manifest = json.loads(img.get("imageManifest") or "{}")
        except ValueError:
            continue
        if not isinstance(manifest, dict):
            continue
        listed = [manifest.get("config") or {}, *(manifest.get("layers") or [])]
        if any(isinstance(d, dict) and d.get("digest") == digest for d in listed):
            return True
    return False


# ── Dispatch ──────────────────────────────────────────────

_HANDLERS = {
    "CreatePullThroughCacheRule": _create,
    "UpdatePullThroughCacheRule": _update,
    "DeletePullThroughCacheRule": _delete,
    "DescribePullThroughCacheRules": _describe,
    "ValidatePullThroughCacheRule": _validate,
    **ecr_registry_policy.HANDLERS,
}

ACTIONS = frozenset(_HANDLERS)


def handle_action(action, body):
    """Answer a pull-through cache action from the raw request body."""
    data, refused = _load_body(action, body)
    if refused:
        return refused
    return _HANDLERS[action](data)
