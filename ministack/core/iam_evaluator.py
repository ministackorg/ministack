# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""IAM policy evaluation engine.

Implements the AWS IAM policy evaluation algorithm (single-account,
no SCPs) behind the AUTH=true flag.

Evaluation order:
  1. Explicit Deny in any applicable policy, boundary or session policy → DENY
  2. Explicit Allow in an identity policy, also allowed by the boundary
     and the session policies → ALLOW
  3. No matching Allow → implicit DENY
"""

import contextvars
import datetime as _dt
import hmac
import importlib
import ipaddress
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field, replace
from typing import Any

logger = logging.getLogger("ministack")

_request_origin: contextvars.ContextVar[tuple[str, bool]] = contextvars.ContextVar(
    "ministack_request_origin", default=("127.0.0.1", False))


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ParsedStatement:
    sid: str
    effect: str  # "Allow" or "Deny"
    actions: list[str] = field(default_factory=list)
    not_actions: list[str] = field(default_factory=list)
    resources: list[str] = field(default_factory=list)
    not_resources: list[str] = field(default_factory=list)
    conditions: dict = field(default_factory=dict)


@dataclass
class EvalContext:
    principal_arn: str
    principal_type: str  # "User", "AssumedRole", "FederatedUser", "Root"
    principal_account: str
    action: str  # "s3:PutObject"
    resource_arn: str  # "*" until per-service resource ARN construction
    region: str
    source_ip: str = field(default_factory=lambda: _request_origin.get()[0])
    secure_transport: bool = field(default_factory=lambda: _request_origin.get()[1])
    request_tags: dict[str, str] = field(default_factory=dict)
    tag_keys: list[str] = field(default_factory=list)
    service_context: dict[str, Any] = field(default_factory=dict)
    principal_keys: dict[str, Any] = field(default_factory=dict)


@dataclass
class EvalResult:
    decision: str  # "Allow", "Deny", "ImplicitDeny"
    principal_arn: str = ""
    matched_sid: str = ""
    reason: str = ""
    resource_policy_arn: str = ""  # set when the resource's own policy denied


@dataclass
class PrincipalInfo:
    arn: str
    type: str  # "User", "AssumedRole", "FederatedUser", "Root"
    account: str
    policies: list[list[ParsedStatement]] | None  # None = root (allow-all)
    keys: dict[str, Any] = field(default_factory=dict)
    limits: list[list[list[ParsedStatement]]] = field(default_factory=list)


@dataclass
class AuthError:
    """Authentication failure (before policy evaluation)."""
    code: str  # "InvalidClientTokenId", "ExpiredTokenException", etc.
    message: str
    details: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Wildcard matching (IAM-style)
# ---------------------------------------------------------------------------

_FNMATCH_CACHE: dict[tuple[str, bool], re.Pattern] = {}


def _fnmatch_compile(pattern: str, ignore_case: bool) -> re.Pattern:
    key = (pattern.lower() if ignore_case else pattern, ignore_case)
    compiled = _FNMATCH_CACHE.get(key)
    if compiled is None:
        regex = ""
        for ch in key[0]:
            if ch == "*":
                regex += ".*"
            elif ch == "?":
                regex += "."
            else:
                regex += re.escape(ch)
        compiled = re.compile(f"^{regex}$", re.IGNORECASE if ignore_case else 0)
        if len(_FNMATCH_CACHE) < 4096:
            _FNMATCH_CACHE[key] = compiled
    return compiled


def fnmatch_iam(value: str, pattern: str) -> bool:
    """Case-insensitive IAM wildcard match, for Action/NotAction.

    ``*`` = any chars, ``?`` = one char.
    """
    if pattern == "*":
        return True
    return _fnmatch_compile(pattern, True).match(value) is not None


def fnmatch_iam_cs(value: str, pattern: str) -> bool:
    """Case-sensitive IAM wildcard match, for Resource/NotResource, StringLike
    and the Arn operators: "In the Resource element, the IAM user name is case
    sensitive" (reference_policies_elements_resource), "Case-sensitive matching"
    for StringLike and "Case-sensitive matching of the ARN" for ArnEquals/ArnLike
    (reference_policies_elements_condition_operators).
    """
    if pattern == "*":
        return True
    return _fnmatch_compile(pattern, False).match(value) is not None


# ---------------------------------------------------------------------------
# Statement matching
# ---------------------------------------------------------------------------

def _action_matches(request_action: str, actions: list[str],
                    not_actions: list[str]) -> bool:
    if actions:
        return any(fnmatch_iam(request_action, p) for p in actions)
    if not_actions:
        return not any(fnmatch_iam(request_action, p) for p in not_actions)
    return True


def _resource_matches(resource_arn: str, resources: list[str],
                      not_resources: list[str]) -> bool:
    if resources:
        return any(fnmatch_iam_cs(resource_arn, p) for p in resources)
    if not_resources:
        return not any(fnmatch_iam_cs(resource_arn, p) for p in not_resources)
    return True


# ---------------------------------------------------------------------------
# Condition evaluation
# ---------------------------------------------------------------------------

_ACCOUNT_ID_RE = re.compile(r"^\d{12}$")


def _account_from_arn(arn: str) -> str | None:
    """The account field of an ARN, or None when the ARN carries none: ``*``,
    partition-only ARNs such as S3 (``arn:aws:s3:::bucket``), AWS-owned ARNs
    (``arn:aws:iam::aws:policy/...``) and malformed values.

    An account id is twelve digits, so anything else standing in that field —
    ``aws`` on an AWS-owned ARN, a service name, an empty segment — reads as
    "this ARN names no account" rather than as an account called ``aws``.
    """
    if not arn or arn == "*" or not arn.startswith("arn:"):
        return None
    parts = arn.split(":", 5)
    if len(parts) < 6:
        return None
    return parts[4] if _ACCOUNT_ID_RE.fullmatch(parts[4]) else None


def _principal_org_id(account_id: str) -> str | None:
    """aws:PrincipalOrgID, the Organization the calling account belongs to.
    In this emulator every account is the master of its own org the moment it
    calls Organizations and member accounts are not modelled, so the id
    resolves from that account's own scope, and an account that never called
    Organizations has none."""
    try:
        from ministack.services import organizations as org_svc
        org = org_svc._orgs.get_scoped(account_id, None, "self")
    except Exception:
        return None
    return org.get("Id") if isinstance(org, dict) else None


def _resolve_condition_key(key: str, ctx: EvalContext) -> Any:
    """Resolve a global condition context key to its value."""
    k = key.lower()
    if k == "aws:principalarn":
        return ctx.principal_arn
    if k == "aws:principalaccount":
        return ctx.principal_account
    if k == "aws:principaltype":
        return ctx.principal_type
    if k == "aws:currenttime":
        return _dt.datetime.now(_dt.timezone.utc).isoformat()
    if k == "aws:epochtime":
        return str(int(_dt.datetime.now(_dt.timezone.utc).timestamp()))
    if k == "aws:requestedregion":
        return ctx.region
    if k == "aws:securetransport":
        return str(ctx.secure_transport).lower()
    if k == "aws:sourceip":
        return ctx.source_ip
    if k == "aws:tagkeys":
        return ctx.service_context.get(k, ctx.tag_keys)
    if k.startswith("aws:requesttag/"):
        if k in ctx.service_context:
            return ctx.service_context[k]
        tag_key = key[len("aws:RequestTag/"):]
        values = [value for name, value in ctx.request_tags.items() if name.lower() == tag_key.lower()]
        if len(values) == 1:
            return values[0]
        return values if values else None
    if k in ("aws:resourceaccount", "s3:resourceaccount"):
        # The account that owns the resource. The emulator hosts one account
        # per request and models no cross-account access, so it is the
        # requesting account — unless the resource ARN carries a different
        # account field, which then wins. CDK's bootstrap file-publishing
        # role conditions its S3 grant on this key, so leaving it unresolved
        # denied every `cdk deploy` under AUTH=true.
        return _account_from_arn(ctx.resource_arn) or ctx.principal_account
    if k in ctx.principal_keys:
        return ctx.principal_keys[k]
    if k in ctx.service_context:
        return ctx.service_context[k]
    if k == "aws:principalorgid":
        return _principal_org_id(ctx.principal_account)
    return None  # key not present


# -- Operator implementations --

def _op_string_equals(actual: str, expected: str) -> bool:
    return actual == expected


def _op_string_not_equals(actual: str, expected: str) -> bool:
    return actual != expected


def _op_string_equals_ignore_case(actual: str, expected: str) -> bool:
    return actual.lower() == expected.lower()


def _op_string_not_equals_ignore_case(actual: str, expected: str) -> bool:
    return actual.lower() != expected.lower()


def _op_string_like(actual: str, expected: str) -> bool:
    return fnmatch_iam_cs(actual, expected)


def _op_string_not_like(actual: str, expected: str) -> bool:
    return not fnmatch_iam_cs(actual, expected)


def _op_numeric(actual: str, expected: str, cmp: str) -> bool:
    try:
        a, e = float(actual), float(expected)
    except (TypeError, ValueError):
        return False
    if cmp == "eq":
        return a == e
    if cmp == "neq":
        return a != e
    if cmp == "lt":
        return a < e
    if cmp == "lte":
        return a <= e
    if cmp == "gt":
        return a > e
    if cmp == "gte":
        return a >= e
    return False


def _parse_date(s: str) -> _dt.datetime | None:
    if not s:
        return None
    try:
        return _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass
    try:
        return _dt.datetime.fromtimestamp(float(s), tz=_dt.timezone.utc)
    except (ValueError, TypeError, OSError):
        pass
    return None


def _op_date(actual: str, expected: str, cmp: str) -> bool:
    a = _parse_date(str(actual)) if actual else None
    e = _parse_date(str(expected)) if expected else None
    if a is None or e is None:
        return False
    if a.tzinfo is None:
        a = a.replace(tzinfo=_dt.timezone.utc)
    if e.tzinfo is None:
        e = e.replace(tzinfo=_dt.timezone.utc)
    if cmp == "eq":
        return a == e
    if cmp == "neq":
        return a != e
    if cmp == "lt":
        return a < e
    if cmp == "lte":
        return a <= e
    if cmp == "gt":
        return a > e
    if cmp == "gte":
        return a >= e
    return False


def _op_bool(actual: str, expected: str) -> bool:
    return str(actual).lower() == str(expected).lower()


def _op_ip_address(actual: str, expected: str) -> bool:
    try:
        addr = ipaddress.ip_address(actual)
        net = ipaddress.ip_network(expected, strict=False)
        return addr in net
    except (ValueError, TypeError):
        return False


def _op_not_ip_address(actual: str, expected: str) -> bool:
    return not _op_ip_address(actual, expected)


def _op_arn_like(actual: str, expected: str) -> bool:
    return fnmatch_iam_cs(actual, expected)


def _op_arn_not_like(actual: str, expected: str) -> bool:
    return not fnmatch_iam_cs(actual, expected)


# Operator dispatch table
_CONDITION_OPS: dict[str, Any] = {
    "stringequals": _op_string_equals,
    "stringnotequals": _op_string_not_equals,
    "stringequalsignorecase": _op_string_equals_ignore_case,
    "stringnotequalsignorecase": _op_string_not_equals_ignore_case,
    "stringlike": _op_string_like,
    "stringnotlike": _op_string_not_like,
    "numericequals": lambda a, e: _op_numeric(a, e, "eq"),
    "numericnotequals": lambda a, e: _op_numeric(a, e, "neq"),
    "numericlessthan": lambda a, e: _op_numeric(a, e, "lt"),
    "numericlessthanequals": lambda a, e: _op_numeric(a, e, "lte"),
    "numericgreaterthan": lambda a, e: _op_numeric(a, e, "gt"),
    "numericgreaterthanequals": lambda a, e: _op_numeric(a, e, "gte"),
    "dateequals": lambda a, e: _op_date(a, e, "eq"),
    "datenotequals": lambda a, e: _op_date(a, e, "neq"),
    "datelessthan": lambda a, e: _op_date(a, e, "lt"),
    "datelessthanequals": lambda a, e: _op_date(a, e, "lte"),
    "dategreaterthan": lambda a, e: _op_date(a, e, "gt"),
    "dategreaterthanequals": lambda a, e: _op_date(a, e, "gte"),
    "bool": _op_bool,
    "ipaddress": _op_ip_address,
    "notipaddress": _op_not_ip_address,
    "arnequals": _op_arn_like,
    "arnlike": _op_arn_like,
    "arnnotequals": _op_arn_not_like,
    "arnnotlike": _op_arn_not_like,
    "binaryequals": _op_string_equals,
}

# Operators that require the key NOT to match
_NEGATED_OPS = frozenset({
    "stringnotequals", "stringnotequalsignorecase", "stringnotlike",
    "numericnotequals", "datenotequals", "arnnotequals", "arnnotlike",
    "notipaddress",
})


def _evaluate_single_condition(operator: str, actual: Any,
                               expected_values: list[str]) -> bool:
    """Compare policy values using OR for affirmative operators and NOR for negated ones."""
    op_lower = operator.lower()

    # Null check: tests key presence/absence
    if op_lower == "null":
        want_null = str(expected_values[0]).lower() == "true" if expected_values else True
        return (actual is None) == want_null

    # IfExists: if the key is absent, the condition is satisfied
    if_exists = False
    if op_lower.endswith("ifexists"):
        if_exists = True
        op_lower = op_lower[:-8]  # strip "ifexists"

    # ForAllValues / ForAnyValue set operators
    for_all = False
    for_any = False
    if op_lower.startswith("forallvalues:"):
        for_all = True
        op_lower = op_lower[13:]
    elif op_lower.startswith("foranyvalue:"):
        for_any = True
        op_lower = op_lower[12:]

    op_func = _CONDITION_OPS.get(op_lower)
    if op_func is None:
        logger.debug("AUTH: unsupported condition operator %s — treating as no-match", operator)
        return False

    if actual is None:
        # An absent key satisfies ...IfExists, ForAllValues (empty set) and a
        # negated single-valued operator; ForAnyValue and affirmative ones fail.
        return if_exists or for_all or (not for_any and op_lower in _NEGATED_OPS)

    # Multi-valued context key
    if isinstance(actual, list):
        actual_list = [str(v) for v in actual]
    else:
        actual_list = [str(actual)]

    expected_str = [str(v) for v in expected_values]
    combine = all if op_lower in _NEGATED_OPS else any

    def matches_expected(value: str) -> bool:
        return combine(op_func(value, expected) for expected in expected_str)

    if for_all:
        return all(matches_expected(value) for value in actual_list)
    if for_any:
        return any(matches_expected(value) for value in actual_list)

    return matches_expected(str(actual))


def _conditions_met(conditions: dict, ctx: EvalContext) -> bool:
    """All condition blocks must be satisfied (AND between operators).
    Policy values use OR for affirmative operators and NOR for negated ones."""
    if not conditions:
        return True
    for operator, key_values in conditions.items():
        if not isinstance(key_values, dict):
            continue
        for key, expected in key_values.items():
            actual = _resolve_condition_key(key, ctx)
            if isinstance(expected, str):
                expected = [expected]
            elif not isinstance(expected, list):
                expected = [str(expected)]
            if key.lower().startswith("aws:requesttag/") and isinstance(actual, list):
                # Case variants are distinct request tags, each with one
                # value. Evaluate even set operators against each individual
                # tag, then match any variant for affirmative operators and
                # none for negated operators, as AWS does.
                op = operator.lower()
                if op.endswith("ifexists"):
                    op = op[:-8]
                if op.startswith("foranyvalue:"):
                    op = op[12:]
                elif op.startswith("forallvalues:"):
                    op = op[13:]
                combine = all if op in _NEGATED_OPS else any
                if not combine(_evaluate_single_condition(operator, value, expected)
                               for value in actual):
                    return False
                continue
            if not _evaluate_single_condition(operator, actual, expected):
                return False
    return True


# ---------------------------------------------------------------------------
# Policy parsing
# ---------------------------------------------------------------------------

def parse_policy_document(doc: str | dict) -> list[ParsedStatement]:
    """Parse a JSON policy document into a list of ParsedStatements."""
    if isinstance(doc, str):
        try:
            doc = json.loads(doc)
        except (json.JSONDecodeError, TypeError):
            return []
    if not isinstance(doc, dict):
        return []

    statements = doc.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    if not isinstance(statements, list):
        return []

    result = []
    for stmt in statements:
        if not isinstance(stmt, dict):
            continue
        effect = stmt.get("Effect", "")
        if effect not in ("Allow", "Deny"):
            continue

        def _as_list(val):
            if val is None:
                return []
            if isinstance(val, str):
                return [val]
            if isinstance(val, list):
                return [str(v) for v in val]
            return []

        result.append(ParsedStatement(
            sid=stmt.get("Sid", ""),
            effect=effect,
            actions=_as_list(stmt.get("Action")),
            not_actions=_as_list(stmt.get("NotAction")),
            resources=_as_list(stmt.get("Resource")),
            not_resources=_as_list(stmt.get("NotResource")),
            conditions=stmt.get("Condition", {}),
        ))
    return result


def validate_policy_document(doc: str | dict) -> str | None:
    """Validate a policy document.  Returns an error message or None."""
    if isinstance(doc, str):
        try:
            doc = json.loads(doc)
        except (json.JSONDecodeError, TypeError):
            return "Policy document is not valid JSON"
    if not isinstance(doc, dict):
        return "Policy document must be a JSON object"

    statements = doc.get("Statement")
    if statements is None:
        return "Policy document must contain a Statement element"
    if isinstance(statements, dict):
        statements = [statements]
    if not isinstance(statements, list) or not statements:
        return "Policy document must contain at least one Statement"

    for i, stmt in enumerate(statements):
        if not isinstance(stmt, dict):
            return f"Statement {i} must be a JSON object"
        effect = stmt.get("Effect")
        if effect not in ("Allow", "Deny"):
            return f"Statement {i} has invalid Effect: {effect}"
        has_action = "Action" in stmt or "NotAction" in stmt
        if not has_action:
            return f"Statement {i} must contain an Action or NotAction element"
        has_resource = "Resource" in stmt or "NotResource" in stmt
        if not has_resource:
            return f"Statement {i} must contain a Resource or NotResource element"
        if "Action" in stmt and "NotAction" in stmt:
            return f"Statement {i} cannot have both Action and NotAction"
        if "Resource" in stmt and "NotResource" in stmt:
            return f"Statement {i} cannot have both Resource and NotResource"

    return None


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------

def _first_match(ctx: EvalContext, stmts: list[ParsedStatement],
                 effect: str) -> ParsedStatement | None:
    for stmt in stmts:
        if (stmt.effect == effect
                and _action_matches(ctx.action, stmt.actions, stmt.not_actions)
                and _resource_matches(ctx.resource_arn, stmt.resources, stmt.not_resources)
                and _conditions_met(stmt.conditions, ctx)):
            return stmt
    return None


def evaluate(ctx: EvalContext,
             policies: list[list[ParsedStatement]],
             *limits: list[list[ParsedStatement]]) -> EvalResult:
    """Evaluate all policies against a request context.

    ``policies`` is a list of parsed-statement lists (one per policy document).
    Each of ``limits`` (a permissions boundary, the session policies) has the same shape: a Deny in
    it denies, and an Allow in ``policies`` counts only if every limit allows.
    Returns Allow, Deny, or ImplicitDeny.
    """
    layers = [[stmt for stmts in layer for stmt in stmts] for layer in (policies, *limits)]
    for stmts in layers:
        deny = _first_match(ctx, stmts, "Deny")
        if deny:
            return EvalResult("Deny", ctx.principal_arn, deny.sid, "Explicit deny")
    allow = _first_match(ctx, layers[0], "Allow")
    if allow and all(_first_match(ctx, stmts, "Allow") for stmts in layers[1:]):
        return EvalResult("Allow", ctx.principal_arn, allow.sid, "Explicit allow")
    return EvalResult("ImplicitDeny", ctx.principal_arn, "",
                      "No matching Allow statement")


def request_caller_arn(headers: dict, query_params: dict | None = None) -> str:
    """Best-effort principal ARN for resource-policy evaluation.

    Resolves the request's access key to its IAM user/role ARN when it maps
    to one, else the caller account's root ARN. Never raises: any failure
    falls back to the root ARN, which is all account-ID and wildcard
    principals (everything ``AddPermission`` can express) need.
    """
    from ministack.core.responses import get_account_id

    fallback = f"arn:aws:iam::{get_account_id()}:root"
    try:
        from ministack.core.router import extract_access_key_id
        access_key = extract_access_key_id(headers or {}, query_params or {})
        principal = resolve_principal(access_key, get_account_id()) if access_key else None
    except Exception:
        return fallback
    if (isinstance(principal, AuthError) or principal is None
            or not getattr(principal, "arn", "")):
        return fallback
    return principal.arn


_request_caller_arn: contextvars.ContextVar[str] = contextvars.ContextVar(
    "ministack_request_caller_arn", default="")


def pin_request_caller(headers: dict, query_params: dict | None = None) -> None:
    """Remember the caller's principal ARN for this request's policy checks."""
    from ministack.app import AUTH
    if not AUTH:
        return
    _request_caller_arn.set(request_caller_arn(headers, query_params))


def pin_request_origin(source_ip: str, secure_transport: bool) -> None:
    """Remember the caller's address and transport for this request's policy checks."""
    _request_origin.set((source_ip, secure_transport))


def caller_arn() -> str:
    """The pinned caller ARN, or the caller account's root ARN."""
    from ministack.core.responses import get_account_id

    return _request_caller_arn.get() or f"arn:aws:iam::{get_account_id()}:root"


def resource_policy_allows(raw_policy: str | dict | None, ctx: EvalContext,
                           same_account: bool) -> bool:
    """Whether a direct API call passes its SQS/SNS resource policy.

    Same-account callers pass unless the policy explicitly denies them;
    cross-account callers need an explicit Allow. A missing policy allows
    same-account callers and denies cross-account ones.
    """
    if not raw_policy:
        return same_account
    decision = evaluate_resource_policy(raw_policy, ctx).decision
    if same_account:
        return decision != "Deny"
    return decision == "Allow"


def _service_principal_matches(principal: Any, service: str) -> bool:
    """Whether a statement Principal names the calling AWS service.

    Compared case-insensitively against the full service principal
    (``s3.amazonaws.com``), never through the trust-policy matcher, which
    lets any ``Service`` entry match an account root.
    """
    if not isinstance(principal, dict):
        return False
    services = principal.get("Service", [])
    if isinstance(services, str):
        services = [services]
    return any(s.lower() == service.lower() for s in services
               if isinstance(s, str))


_ACCOUNT_PRINCIPAL = re.compile(r"\d{12}|arn:aws[a-z-]*:iam::\d{12}:root")


def _names_caller(principal: Any, caller_arn: str) -> bool:
    """Whether a statement Principal names the caller itself (its ARN, its role, or ``*``), not only its account."""
    if isinstance(principal, dict):
        principal = principal.get("AWS", [])
    if isinstance(principal, str):
        principal = [principal]
    return any(isinstance(p, str) and not _ACCOUNT_PRINCIPAL.fullmatch(p)
               and _principal_matches({"AWS": p}, caller_arn) for p in principal)


def _names_caller_directly(principal: Any, caller_arn: str) -> bool:
    """Whether a statement Principal names the caller's own IAM user or role-session ARN, which no limit narrows."""
    if not re.fullmatch(r"arn:aws[a-z-]*:(iam::\d{12}:user/.+|sts::\d{12}:assumed-role/.+)", caller_arn):
        return False
    if isinstance(principal, dict):
        principal = principal.get("AWS", [])
    if isinstance(principal, str):
        principal = [principal]
    return caller_arn in principal


def evaluate_resource_policy(doc: str | dict | None, ctx: EvalContext,
                             service: str | None = None,
                             named_only: bool = False,
                             direct_only: bool = False) -> EvalResult:
    """Evaluate a resource-based policy for the principal in *ctx*.

    Such a document names the principals it applies to, so a statement counts
    only when its ``AWS`` principal (or ``*``) matches the caller; action,
    resource, conditions and deny-before-allow are then the ordinary
    evaluation. When *service* names the calling AWS service (``s3.amazonaws.com``
    for an S3 notification delivery), a statement whose ``Service`` principal
    names it counts too. Without *service*, ``Service`` principals are ignored,
    since the trust-policy matcher lets them match any account root. A statement
    carrying only ``NotPrincipal`` never matches, leaving that unsupported form
    closed."""
    if isinstance(doc, str):
        try:
            doc = json.loads(doc)
        except (json.JSONDecodeError, TypeError):
            doc = None
    statements = doc.get("Statement", []) if isinstance(doc, dict) else []
    if isinstance(statements, dict):
        statements = [statements]
    applicable = []
    for stmt in statements if isinstance(statements, list) else []:
        if not isinstance(stmt, dict):
            continue
        principal = stmt.get("Principal", {})
        if direct_only:
            if _names_caller_directly(principal, ctx.principal_arn):
                applicable.append(stmt)
        elif named_only:
            if _names_caller(principal, ctx.principal_arn):
                applicable.append(stmt)
        elif isinstance(principal, dict):
            if _principal_matches({"AWS": principal.get("AWS", [])},
                                  ctx.principal_arn):
                applicable.append(stmt)
            elif service and _service_principal_matches(principal, service):
                applicable.append(stmt)
        elif _principal_matches(principal, ctx.principal_arn):
            applicable.append(stmt)
    return evaluate(ctx, [parse_policy_document({"Statement": applicable})])


def combine_resource_policy(identity: EvalResult, ctx: EvalContext,
                            policy: str | dict | None, owner_account: str | None,
                            required: bool = False,
                            limits: tuple = ()) -> EvalResult:
    """Combine an identity decision with the resource's own policy: a Deny in either wins."""
    # In the owner's account an Allow in either suffices, but a resource statement naming
    # only the account delegates to identity policies; across accounts both must allow.
    # With *required* (a KMS key policy) the resource policy must allow in the owner's account too.
    # *limits* (boundary, session policies) narrow a resource Allow unless it names the user or session itself.
    if identity.decision == "Deny":
        return identity
    resource = evaluate_resource_policy(policy, ctx)
    if resource.decision == "Deny":
        resource.resource_policy_arn = ctx.resource_arn
        return resource
    same_account = owner_account in (None, ctx.principal_account)
    if identity.decision == "Allow" and (
            resource.decision == "Allow" or (same_account and not required)):
        return identity
    if same_account and resource.decision == "Allow":
        named = evaluate_resource_policy(policy, ctx, named_only=True)
        within_limits = all(_first_match(ctx, [stmt for stmts in layer for stmt in stmts], "Allow")
                            for layer in limits)
        if named.decision == "Allow" and (
                within_limits
                or evaluate_resource_policy(policy, ctx, direct_only=True).decision == "Allow"):
            return named
    return resource if identity.decision == "Allow" else identity


# Services whose resources carry their own policy: the module and the function
# returning ``(policy, owner account)`` for a resource ARN, or None.
_RESOURCE_POLICY_LOOKUPS = {
    "sqs": ("sqs", "queue_policy_for_arn"),
    "sns": ("sns", "topic_policy_for_arn"),
    "s3": ("s3", "bucket_policy_for_arn"),
    "lambda": ("lambda_svc", "function_policy_for_arn"),
    "secretsmanager": ("secretsmanager", "secret_policy_for_arn"),
    "ecr": ("ecr", "repository_policy_for_arn"),
    "kms": ("kms", "key_policy_for_arn"),
    "apigateway": ("apigateway_v1", "rest_api_policy_for_arn"),
    "kinesis": ("kinesis", "stream_policy_for_arn"),
}

# A key policy must allow even the owner's own users and roles.
_POLICY_REQUIRED = frozenset({"kms"})


def _resource_policy(service: str, resource_arn: str) -> tuple | None:
    """The request resource's own policy and owner account, when its service keeps one."""
    lookup = _RESOURCE_POLICY_LOOKUPS.get(service)
    if lookup is None or not resource_arn.startswith("arn:"):
        return None
    module = importlib.import_module(f"ministack.services.{lookup[0]}")
    return getattr(module, lookup[1])(resource_arn)


# ---------------------------------------------------------------------------
# Principal resolution
# ---------------------------------------------------------------------------

def _role_name_from_assumed_arn(assumed_arn: str) -> str:
    """Extract role name from ``arn:aws:sts::ACCT:assumed-role/RoleName/Session``."""
    parts = assumed_arn.split(":")
    if len(parts) >= 6:
        resource = parts[5]
        segs = resource.split("/")
        if len(segs) >= 2:
            return segs[1]
    return ""


def _gather_role_policies(role_name: str, account_id: str) -> list[list[ParsedStatement]]:
    from ministack.services import iam as iam_svc
    # get_scoped takes (account_id, region, key) — region is ignored for AccountScopedDict
    role = iam_svc._roles.get_scoped(account_id, None, role_name)
    if role is None:
        return []
    policies = []
    for doc in (role.get("InlinePolicies") or {}).values():
        stmts = parse_policy_document(doc)
        if stmts:
            policies.append(stmts)
    for policy_arn in role.get("AttachedPolicies", []):
        doc = _resolve_managed_policy_document(policy_arn, account_id)
        if doc:
            stmts = parse_policy_document(doc)
            if stmts:
                policies.append(stmts)
    return policies


def _gather_user_policies(user_name: str, account_id: str) -> list[list[ParsedStatement]]:
    from ministack.services import iam as iam_svc
    user = iam_svc._users.get_scoped(account_id, None, user_name)
    if user is None:
        return []
    policies = []
    # User inline policies live in a separate AccountScopedDict, not on the user object
    for doc in (iam_svc._user_inline_policies.get(user_name) or {}).values():
        stmts = parse_policy_document(doc)
        if stmts:
            policies.append(stmts)
    for policy_arn in user.get("AttachedPolicies", []):
        doc = _resolve_managed_policy_document(policy_arn, account_id)
        if doc:
            stmts = parse_policy_document(doc)
            if stmts:
                policies.append(stmts)
    # Group policies — iterate all groups in this account, check if user is a member
    for group_name, group in iam_svc._groups.items():
        if user_name in group.get("Users", []):
            # Group inline policies live in a separate AccountScopedDict
            for doc in (iam_svc._group_inline_policies.get(group_name) or {}).values():
                stmts = parse_policy_document(doc)
                if stmts:
                    policies.append(stmts)
            for policy_arn in group.get("AttachedPolicies", []):
                doc = _resolve_managed_policy_document(policy_arn, account_id)
                if doc:
                    stmts = parse_policy_document(doc)
                    if stmts:
                        policies.append(stmts)
    return policies


def _permissions_boundary(store, name: str,
                          account_id: str) -> list[list[list[ParsedStatement]]]:
    """The evaluation limits a user's or role's permissions boundary sets."""
    arn = (store.get_scoped(account_id, None, name) or {}).get("PermissionsBoundary")
    if not arn:
        return []
    doc = _resolve_managed_policy_document(arn, account_id)
    return [[parse_policy_document(doc) if doc else []]]


def _session_policy_limits(session: dict,
                           account_id: str) -> list[list[list[ParsedStatement]]]:
    """The evaluation limits the session policies of an STS session set."""
    passed = session.get("SessionPolicies")
    if passed is None:
        return []
    docs = [passed["Policy"]] if passed.get("Policy") else []
    docs += [_resolve_managed_policy_document(arn, account_id) for arn in passed.get("PolicyArns", [])]
    return [[parse_policy_document(doc) for doc in docs if doc]]


def _resolve_managed_policy_document(policy_arn: str,
                                     account_id: str) -> dict | str | None:
    """Resolve a managed policy ARN to its default version's document."""
    from ministack.services import iam as iam_svc

    # AWS-managed policy — through the service's own lookup so that
    # MINISTACK_AUTOCREATE_AWS_MANAGED applies to enforcement exactly as it
    # applies to GetPolicy (an attached-but-never-fetched ARN used to resolve
    # to nothing here while GetPolicy would have autocreated it).
    if policy_arn.startswith("arn:aws:iam::aws:policy/"):
        mp = iam_svc._lookup_policy(policy_arn)
        if mp:
            default_vid = mp.get("DefaultVersionId", "v1")
            versions = mp.get("Versions", {})
            ver = versions.get(default_vid, {})
            return ver.get("Document")
        return None

    # Customer-managed policies are keyed by full ARN in the owner's account.
    policy = iam_svc._policies.get_scoped(account_id, None, policy_arn)
    if policy:
        default_vid = policy.get("DefaultVersionId", "v1")
        versions = policy.get("Versions", {})
        ver = versions.get(default_vid, {})
        return ver.get("Document")
    return None


# ---------------------------------------------------------------------------
# Access-key resolution — the credentials and principals MiniStack issued
# ---------------------------------------------------------------------------

_SESSION_TOKEN_NOT_CHECKED = object()


@dataclass(frozen=True)
class ResolvedCredential:
    access_key_id: str
    secret_access_key: str
    account_id: str
    principal_arn: str
    principal_id: str
    principal_type: str
    principal_name: str = ""
    session_token: str | None = None
    expiration: float | None = None
    source_access_key_id: str | None = None


@dataclass(frozen=True)
class CredentialResolutionError:
    code: str
    message: str


class AmbiguousAccessKeyError(ValueError):
    """Raised when more than one tenant owns the same IAM access key."""


def is_root_access_key(access_key_id: str) -> bool:
    """Return whether a key represents MiniStack's configured root caller."""
    configured_key = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    return (
        not access_key_id
        or access_key_id == "test"
        or access_key_id == configured_key
        or bool(_ACCOUNT_ID_RE.fullmatch(access_key_id))
    )


def _unknown_access_key() -> CredentialResolutionError:
    return CredentialResolutionError(
        "UnrecognizedClientException",
        "The security token included in the request is invalid.",
    )


def _invalid_session_token() -> CredentialResolutionError:
    return CredentialResolutionError(
        "InvalidToken",
        "The provided token is malformed or otherwise invalid.",
    )


def _principal_type(arn: str, recorded_type: str) -> str:
    if recorded_type:
        return recorded_type
    if ":assumed-role/" in arn:
        return "AssumedRole"
    if ":user/" in arn:
        return "User"
    if arn.endswith(":root"):
        return "Root"
    return ""


def _validate_session_token(expected: str | None, supplied) -> CredentialResolutionError | None:
    if supplied is _SESSION_TOKEN_NOT_CHECKED:
        return None
    supplied = supplied or ""
    if expected is None:
        return _invalid_session_token() if supplied else None
    if not isinstance(expected, str) or not isinstance(supplied, str) or not hmac.compare_digest(
        expected.encode("utf-8"), supplied.encode("utf-8")
    ):
        return _invalid_session_token()
    return None


def find_iam_access_key_account(access_key_id: str) -> str | None:
    """Return the sole account that owns an IAM access key, if known.

    AWS access key IDs are globally unique. Duplicate emulator records raise
    instead of selecting either tenant.
    """
    if is_root_access_key(access_key_id):
        return None

    from ministack.services import sts as sts_svc

    if access_key_id in sts_svc._sessions:
        return None

    from ministack.services import iam as iam_svc

    accounts = {
        account_id
        for (account_id, stored_key) in iam_svc._access_keys.to_dict()
        if stored_key == access_key_id
    }
    if len(accounts) > 1:
        raise AmbiguousAccessKeyError(access_key_id)
    if accounts:
        return accounts.pop()
    return None


def resolve_credential(
    access_key_id: str,
    account_id: str | None = None,
    session_token=_SESSION_TOKEN_NOT_CHECKED,
) -> ResolvedCredential | CredentialResolutionError:
    """Resolve a root, IAM, or STS key in an explicit account.

    Passing ``session_token`` also validates whether the caller supplied the
    exact token attached to temporary credentials. Omitting it leaves token
    validation to the caller, which keeps policy-only identity lookups usable.
    """
    from ministack.core.responses import get_account_id

    account_id = account_id or get_account_id()

    if is_root_access_key(access_key_id):
        if _ACCOUNT_ID_RE.fullmatch(access_key_id):
            if account_id != access_key_id:
                return _unknown_access_key()
            account_id = access_key_id
        configured_key = os.environ.get("AWS_ACCESS_KEY_ID", "test")
        configured_token = os.environ.get("AWS_SESSION_TOKEN") or None
        numeric_token_supplied = (
            bool(_ACCOUNT_ID_RE.fullmatch(access_key_id))
            and isinstance(session_token, str)
            and bool(session_token)
        )
        expected_token = (
            configured_token
            if access_key_id == configured_key or numeric_token_supplied
            else None
        )
        token_error = _validate_session_token(expected_token, session_token)
        if token_error:
            return token_error
        return ResolvedCredential(
            access_key_id=access_key_id,
            secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
            session_token=expected_token,
            account_id=account_id,
            principal_arn=f"arn:aws:iam::{account_id}:root",
            principal_id=account_id,
            principal_type="Root",
            source_access_key_id=access_key_id,
        )

    from ministack.services import sts as sts_svc

    session = sts_svc._sessions.get(access_key_id)
    if session is not None:
        principal_arn = str(session.get("Arn", ""))
        session_account = session.get("AccountId") or _account_from_arn(principal_arn)
        if session_account and session_account != account_id:
            return _unknown_access_key()
        expiration = session.get("Expiration")
        if isinstance(expiration, (int, float)) and time.time() > expiration:
            return CredentialResolutionError(
                "ExpiredTokenException",
                "The security token included in the request is expired",
            )
        expected_token = session.get("SessionToken")
        token_error = _validate_session_token(expected_token, session_token)
        if token_error:
            return token_error
        secret = session.get("SecretAccessKey")
        principal_type = _principal_type(principal_arn, session.get("PrincipalType", ""))
        if not secret or not principal_type:
            return _unknown_access_key()
        principal_name = ""
        if principal_type == "User" and ":user/" in principal_arn:
            principal_name = str(session.get("PrincipalName") or "")
            if not principal_name:
                principal_name = principal_arn.rsplit("/", 1)[-1]
        return ResolvedCredential(
            access_key_id=access_key_id,
            secret_access_key=secret,
            session_token=expected_token,
            expiration=expiration if isinstance(expiration, (int, float)) else None,
            account_id=session_account or account_id,
            principal_arn=principal_arn,
            principal_id=str(session.get("UserId", "")),
            principal_type=principal_type,
            principal_name=principal_name,
            source_access_key_id=session.get("SourceAccessKeyId"),
        )

    from ministack.services import iam as iam_svc

    key_record = iam_svc._access_keys.get_scoped(account_id, None, access_key_id)
    if key_record is None:
        return _unknown_access_key()
    if key_record.get("Status") != "Active":
        return CredentialResolutionError(
            "InvalidClientTokenId",
            "The security token included in the request is invalid.",
        )
    token_error = _validate_session_token(None, session_token)
    if token_error:
        return token_error
    secret = key_record.get("SecretAccessKey")
    if not secret:
        return _unknown_access_key()
    user_name = str(key_record.get("UserName", ""))
    user = iam_svc._users.get_scoped(account_id, None, user_name) or {}
    principal_arn = str(
        user.get("Arn") or f"arn:aws:iam::{account_id}:user/{user_name}"
    )
    return ResolvedCredential(
        access_key_id=access_key_id,
        secret_access_key=secret,
        account_id=account_id,
        principal_arn=principal_arn,
        principal_id=str(user.get("UserId", "")),
        principal_type="User",
        principal_name=user_name,
        source_access_key_id=access_key_id,
    )


def _is_root_key(access_key_id: str) -> bool:
    """Keys treated as root: empty, ``test``, configured, or account IDs."""
    return is_root_access_key(access_key_id)


def _principal_keys(credential: ResolvedCredential) -> dict[str, Any]:
    """The condition keys that describe the caller, keyed in lower case."""
    from ministack.services import iam as iam_svc
    from ministack.services import sts as sts_svc

    keys: dict[str, Any] = {"aws:userid": credential.principal_id}
    entity = {}
    if credential.principal_type == "User":
        keys["aws:username"] = credential.principal_name
        entity = iam_svc._users.get_scoped(credential.account_id, None, credential.principal_name)
    elif credential.principal_type == "AssumedRole":
        entity = iam_svc._roles.get_scoped(
            credential.account_id, None, _role_name_from_assumed_arn(credential.principal_arn))
    tags = list((entity or {}).get("Tags") or [])
    session = sts_svc._sessions.get(credential.access_key_id)
    if session is not None:
        tags += session.get("SessionTags") or []
        keys["aws:multifactorauthpresent"] = "false"
        if isinstance(session.get("IssuedAt"), (int, float)):
            keys["aws:tokenissuetime"] = _dt.datetime.fromtimestamp(
                session["IssuedAt"], _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    keys.update({f"aws:principaltag/{t['Key'].lower()}": t["Value"] for t in tags})
    return keys


def resolve_principal(access_key_id: str,
                      account_id: str, credential: ResolvedCredential | None = None) -> PrincipalInfo | AuthError:
    """Resolve an access key to a principal and their policies.

    Returns a ``PrincipalInfo`` on success or an ``AuthError`` when
    authentication fails (unknown key, inactive key, expired session).
    A ``credential`` the caller already resolved skips the key lookup.
    """
    from ministack.core.responses import _account_from_sts_session

    account_id = _account_from_sts_session(access_key_id) or account_id
    from ministack.services import iam as iam_svc
    from ministack.services import sts as sts_svc

    credential = credential or resolve_credential(access_key_id, account_id)
    if isinstance(credential, CredentialResolutionError):
        return AuthError(credential.code, credential.message)
    session = sts_svc._sessions.get(access_key_id) or {}
    limits = []
    if credential.principal_type == "Root":
        policies = None
    elif credential.principal_type == "User":
        policies = _gather_user_policies(credential.principal_name, account_id)
        limits = _permissions_boundary(iam_svc._users, credential.principal_name, account_id)
    elif credential.principal_type == "AssumedRole":
        role_name = _role_name_from_assumed_arn(credential.principal_arn)
        policies = _gather_role_policies(role_name, account_id)
        limits = _permissions_boundary(iam_svc._roles, role_name, account_id)
    elif credential.principal_type == "FederatedUser":
        user_name = session.get("PrincipalName")
        if user_name:
            policies = _gather_user_policies(user_name, account_id)
            limits = _permissions_boundary(iam_svc._users, user_name, account_id)
        else:
            # Federated by the account root, which holds every permission.
            policies = [parse_policy_document({"Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}})]
    else:
        return AuthError(
            "UnrecognizedClientException",
            "The security token included in the request is invalid.",
        )
    if policies is not None:
        limits += _session_policy_limits(session, account_id)
    return PrincipalInfo(
        arn=credential.principal_arn,
        type=credential.principal_type,
        account=credential.account_id,
        policies=policies,
        keys=_principal_keys(credential),
        limits=limits,
    )


@dataclass(frozen=True)
class SignedRequest:
    """A header- or query-signed request as it arrived."""

    access_key_id: str
    method: str
    raw_path: str
    query_params: dict
    headers: dict
    body: bytes = b""
    presigned: bool = False
    handshake: bool = False
    # Credentials this server cannot verify (another algorithm, a key the signature does not cover).
    unverifiable: bool = False
    # A SigV2 (HmacV1) header or query signature, checked against the path S3 signs.
    sigv2: bool = False
    auth_path: str = ""
    # A SigV4 presigned query scoped to s3: S3 checks its own, any other service refuses its scope.
    s3_scoped: bool = False


_signed_request: contextvars.ContextVar[SignedRequest | None] = contextvars.ContextVar(
    "ministack_signed_request", default=None)
# The access key whose signature this request already passed, once checked.
_verified_key: contextvars.ContextVar[str] = contextvars.ContextVar("ministack_verified_key", default="")
# Whether the request carries no credentials: no Authorization header and no query signature.
_anonymous: contextvars.ContextVar[bool] = contextvars.ContextVar("ministack_anonymous", default=False)


def caller_verified(access_key_id: str, account_id: str) -> bool:
    """Whether the request's SigV4/SigV2 signature for this key checks out (root keys are not checked)."""
    if not access_key_id:
        return False
    if _is_root_key(access_key_id) or _verified_key.get() == access_key_id:
        return True
    checked = authenticate(access_key_id, account_id)
    return checked is not None and not isinstance(checked, AuthError)


def pin_signed_request(
    method: str, raw_path: str, query_params: dict, raw_headers: list, handshake: bool = False,
) -> None:
    """Keep a request signed with an IAM user or STS session key as sent, for its signature check."""
    from ministack.core.router import extract_access_key_id
    from ministack.core.sigv4 import decode_header_value

    _verified_key.set("")
    authorization = decode_header_value(next((v for n, v in raw_headers if n.lower() == b"authorization"), b""))
    payload_hash = decode_header_value(next((v for n, v in raw_headers if n.lower() == b"x-amz-content-sha256"), b""))
    presigned = not authorization.startswith("AWS4-HMAC-SHA256") and (
        _query_signed(query_params) or _sigv4a_s3_presign(query_params))
    access_key_id = extract_access_key_id({"authorization": authorization}, query_params if presigned else None)
    # AWS reads query credentials without X-Amz-Algorithm as none at all.
    _anonymous.set(not authorization and not any(
        _query_value(query_params or {}, name) for name in ("X-Amz-Algorithm", "AWSAccessKeyId")))
    # The key the policy check will use; it must be the one the signature is checked for.
    claimed = extract_access_key_id({"authorization": authorization}, query_params)
    header_signed = authorization.startswith("AWS4-HMAC-SHA256") or _sigv4a_header(authorization, payload_hash)
    verifiable = (header_signed or presigned) and bool(claimed) and access_key_id == claimed
    sigv2 = authorization.startswith("AWS ") or bool(
        _query_value(query_params or {}, "AWSAccessKeyId") and _query_value(query_params or {}, "Signature")
        and not _query_value(query_params or {}, "SignatureVersion"))
    if sigv2 and claimed and not _is_root_key(claimed) and not authorization.startswith("AWS4-"):
        headers = {name.decode("latin-1").lower(): decode_header_value(value) for name, value in raw_headers}
        from ministack.app import _ACCESS_POINT_HOST_RE, _extract_s3_vhost_bucket
        from ministack.services.s3 import resolve_access_point_host
        bucket = _extract_s3_vhost_bucket(headers.get("host", ""))
        access_point = _ACCESS_POINT_HOST_RE.match(bucket or "")
        if access_point and resolve_access_point_host(access_point["name"], access_point["account"], None):
            bucket = None  # an access point host signs its path without a bucket
        _signed_request.set(SignedRequest(claimed, method, raw_path, query_params, headers, sigv2=True,
                                          auth_path=f"/{bucket}{raw_path}" if bucket else raw_path))
        return
    if not authorization and claimed and not _is_root_key(claimed) and _s3_presigned_query(query_params):
        _signed_request.set(SignedRequest(claimed, method, raw_path, query_params, {}, presigned=True, s3_scoped=True))
        return
    # A SigV4 signature that names no key is checked too: it is incomplete or names an unknown key.
    names_no_key = not claimed and (authorization.startswith("AWS4-HMAC-SHA256") or presigned)
    if not verifiable and not names_no_key and _signature_attempted(authorization, query_params) and not (
            claimed and _is_root_key(claimed)) and not _s3_presigned_query(query_params):
        _signed_request.set(SignedRequest(claimed, method, raw_path, query_params, {}, unverifiable=True))
        return
    if not (verifiable or names_no_key) or (access_key_id and _is_root_key(access_key_id)):
        _signed_request.set(None)
        return
    headers: dict[str, str] = {}
    for name, value in raw_headers:
        key, text = name.decode("latin-1").lower(), decode_header_value(value)
        # SigV4 signs a header sent on several lines as its values joined by a bare comma.
        headers[key] = f"{headers[key]},{text}" if key in headers else text
    _signed_request.set(SignedRequest(
        access_key_id, method, raw_path, query_params, headers, presigned=presigned, handshake=handshake))


def _sigv2_check(request: SignedRequest, account_id: str) -> AuthError | ResolvedCredential:
    """Verify a SigV2 (HmacV1) header or query signature as S3 computes it."""
    from urllib.parse import urlencode, urlsplit

    from botocore.auth import HmacV1Auth
    from botocore.awsrequest import HTTPHeaders
    from botocore.credentials import Credentials

    from ministack.core.responses import _account_from_sts_session

    query = request.query_params or {}
    presigned = not request.headers.get("authorization", "").startswith("AWS ")
    token = (_query_value(query, "x-amz-security-token") if presigned
             else request.headers.get("x-amz-security-token", ""))
    credential = resolve_credential(request.access_key_id,
                                    _account_from_sts_session(request.access_key_id) or account_id, token)
    if isinstance(credential, CredentialResolutionError):
        return AuthError(credential.code, credential.message)
    headers = HTTPHeaders()
    for name, value in request.headers.items():
        headers[name] = value
    pairs = [(k, v) for k, values in query.items() for v in (values if isinstance(values, list) else [values])]
    split = urlsplit("http://host" + request.auth_path + ("?" + urlencode(pairs) if pairs else ""))
    if presigned:
        # A query signature signs Expires where a header signature signs Date.
        signed_date = _query_value(query, "Expires")
        if not signed_date.isdigit() or int(signed_date) < time.time():
            return AuthError("SignatureDoesNotMatch", "Request has expired")
        given = _query_value(query, "Signature")
    else:
        signed_date = request.headers.get("date", "")
        given = request.headers["authorization"].split(":", 1)[-1]

    class _Signer(HmacV1Auth):
        def _get_date(self):  # the date the client signed, not the time of this check
            return signed_date

    signer = _Signer(Credentials(credential.access_key_id, credential.secret_access_key, token or None))
    expected = signer.get_signature(request.method, split, headers)
    if not hmac.compare_digest(given.encode(), expected.encode()):
        return AuthError("SignatureDoesNotMatch", "The request signature we calculated does not match the signature "
                                                  "you provided. Check your key and signing method.")
    _verified_key.set(request.access_key_id)
    return credential


def _query_value(query_params: dict, name: str) -> str:
    found = query_params.get(name) or query_params.get(name.lower()) or [""]
    return found[0] if isinstance(found, list) else found


def _signature_attempted(authorization: str, query_params: dict) -> bool:
    """Whether a request carries any AWS signature or credential, valid or not."""
    if authorization.startswith(("AWS4-", "AWS ")):
        return True
    return any(_query_value(query_params or {}, name) for name in (
        "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Signature", "AWSAccessKeyId", "Signature"))


def _s3_presigned_query(query_params: dict) -> bool:
    """A complete SigV4 presigned S3 URL, which S3 verifies itself."""
    query_params = query_params or {}
    scope = _query_value(query_params, "X-Amz-Credential").split("/")
    return (_query_value(query_params, "X-Amz-Algorithm") == "AWS4-HMAC-SHA256" and bool(scope[0])
            and len(scope) == 5 and scope[3] == "s3" and bool(_query_value(query_params, "X-Amz-Signature")))


def _sigv4a_header(authorization: str, payload_hash: str) -> bool:
    """A SigV4a header signature that signs its region set and does not sign its body in chunks."""
    from ministack.core.sigv4 import SIGV4A

    if not authorization.startswith(SIGV4A + " "):
        return False
    fields = dict(part.strip().partition("=")[::2] for part in authorization.split(" ", 1)[1].split(","))
    return ("x-amz-region-set" in fields.get("SignedHeaders", "").split(";")
            and not payload_hash.startswith("STREAMING-AWS4-ECDSA"))


def _sigv4a_s3_presign(query_params: dict) -> bool:
    """A SigV4a presigned S3 URL; the S3 handler checks only SigV4 ones itself."""
    from ministack.core.sigv4 import SIGV4A

    scope = _query_value(query_params or {}, "X-Amz-Credential").split("/")
    return _query_value(query_params or {}, "X-Amz-Algorithm") == SIGV4A and len(scope) == 4 and scope[2] == "s3"


def _query_signed(query_params: dict) -> bool:
    """A SigV4 query-string signature; S3 checks its own presigned URLs."""
    def value(name):
        found = query_params.get(name) or query_params.get(name.lower()) or [""]
        return found[0] if isinstance(found, list) else found

    scope = value("X-Amz-Credential").split("/")
    return value("X-Amz-Algorithm") == "AWS4-HMAC-SHA256" and (len(scope) < 4 or scope[3] != "s3")


def pin_signed_body(body: bytes) -> None:
    """Add the body as it arrived, before any decoding, to the pinned request."""
    request = _signed_request.get()
    if request is not None:
        _signed_request.set(replace(request, body=body))


def _s3_scope_failure(service: str | None) -> AuthError | None:
    """An s3-scoped presigned query: S3 verifies it itself, any other service refuses the scope."""
    request = _signed_request.get()
    if request is None or not request.s3_scoped:
        return None
    _signed_request.set(None)
    if service == "s3":
        return None
    from ministack.core.iam_actions import SERVICE_TO_IAM_NAMESPACE

    name = SERVICE_TO_IAM_NAMESPACE.get(service or "", service or "")
    return AuthError("SignatureDoesNotMatch", f"Credential should be scoped to correct service: '{name}'. ")


def authenticate_handshake(scope: dict, service: str | None = None) -> AuthError | ResolvedCredential | None:
    """Check a WebSocket upgrade signed in its Authorization header or query string, as ``authenticate`` does."""
    from urllib.parse import parse_qs, quote

    from ministack.core.responses import get_account_id

    raw_path = scope.get("raw_path") or quote(scope.get("path", "")).encode()
    query_params = parse_qs(scope.get("query_string", b"").decode("utf-8", "replace"), keep_blank_values=True)
    pin_signed_request("GET", raw_path.decode("latin-1"), query_params, scope.get("headers", []), handshake=True)
    failure = _s3_scope_failure(service)
    if failure:
        return failure
    request = _signed_request.get()
    if request is None:
        return None
    try:
        account_id = find_iam_access_key_account(request.access_key_id) or get_account_id()
    except AmbiguousAccessKeyError:
        account_id = get_account_id()
    return authenticate(request.access_key_id, account_id)


def authenticate(access_key_id: str, account_id: str) -> AuthError | ResolvedCredential | None:
    """Check the pinned request's time, session token and signature: its credential, a failure, or None."""
    from ministack.core.responses import _account_from_sts_session, get_region
    from ministack.core.sigv4 import (
        header_signature_failure,
        parse_header_signature,
        parse_query_signature,
        presigned_request_is_expired,
        region_set_covers,
        request_time_failure,
    )

    request = _signed_request.get()
    if request is None or request.access_key_id != access_key_id:
        return None
    _signed_request.set(None)  # checked once; the request's later checks skip it
    if request.unverifiable:
        return AuthError("UnsupportedSignature", "The request signature does not conform to AWS standards.")
    if request.sigv2:
        return _sigv2_check(request, account_id)
    if request.presigned:
        signature = parse_query_signature(request.query_params)
    else:
        signature = parse_header_signature(request.headers)
    if isinstance(signature, tuple):
        return AuthError(*signature)
    if request.presigned and signature.region_set is not None:
        # A SigV4a presigned URL is good until its X-Amz-Expires, however long ago it was signed.
        expires = _query_value(request.query_params, "X-Amz-Expires")
        try:
            expired = bool(expires) and presigned_request_is_expired(signature.amz_date, expires)
        except ValueError:
            expired = True
        if expired:
            return AuthError("RequestExpired", "Request has expired")
    else:
        skewed = request_time_failure(signature)
        if skewed:
            return AuthError(*skewed)
    if signature.region_set is not None and signature.service != "s3" and not region_set_covers(
            signature.region_set, get_region()):
        # A region set without the region serving the request is refused like an unknown key; S3 checks its own.
        unknown = _unknown_access_key()
        return AuthError(unknown.code, unknown.message)
    if request.presigned:
        token = (request.query_params.get("X-Amz-Security-Token") or [""])[0]
    else:
        token = request.headers.get("x-amz-security-token", "")
    credential = resolve_credential(
        access_key_id, _account_from_sts_session(access_key_id) or account_id, token,
    ) if access_key_id else _unknown_access_key()
    if isinstance(credential, CredentialResolutionError):
        if credential.code == "InvalidToken":
            # AWS treats a session key without its token as unknown, and any other token as invalid.
            if token:
                return AuthError("InvalidToken", "The security token included in the request is invalid")
            credential = _unknown_access_key()
        return AuthError(credential.code, credential.message)
    failure = header_signature_failure(
        signature, credential.secret_access_key, request.method, request.raw_path,
        request.query_params, request.headers, request.body, presigned=request.presigned,
        explain=request.handshake,
    )
    if failure:
        details = {}
        if signature.service == "s3" and failure[0] == "SignatureDoesNotMatch":
            from ministack.core.sigv4 import header_signing_documents

            canonical, string_to_sign = header_signing_documents(
                signature, request.method, request.raw_path, request.query_params, request.headers, request.body,
                presigned=request.presigned,
            )
            details = {
                "AWSAccessKeyId": access_key_id,
                "StringToSign": string_to_sign,
                "SignatureProvided": signature.signature,
                "StringToSignBytes": string_to_sign.encode("utf-8").hex(" "),
                "CanonicalRequest": canonical,
                "CanonicalRequestBytes": canonical.encode("utf-8").hex(" "),
            }
        return AuthError(*failure, details=details)
    _verified_key.set(access_key_id)
    return credential


def check_credentials(access_key_id: str, service: str | None = None) -> AuthError | None:
    """Authenticate a request no IAM action covers, with the checks enforce() makes before any policy."""
    from ministack.core.responses import get_account_id

    failure = _s3_scope_failure(service)
    if failure:
        return failure
    account_id = get_account_id()
    checked = authenticate(access_key_id or "", account_id)
    if checked is None:
        checked = resolve_principal(access_key_id or "", account_id)
    return checked if isinstance(checked, AuthError) else None


# ---------------------------------------------------------------------------
# Top-level enforcement entry point
# ---------------------------------------------------------------------------

# Signed in the SDK models, yet served without credentials: the links in SNS subscription messages.
_UNSIGNED_LINK_ACTIONS = frozenset({"sns:ConfirmSubscription", "sns:Unsubscribe"})


def _served_unsigned(service: str, iam_action: str) -> bool:
    """Whether AWS serves this action without credentials: an operation its SDK model sends unsigned, or a link."""
    from ministack.core.iam_actions import unsigned_operations

    return iam_action in _UNSIGNED_LINK_ACTIONS or iam_action.partition(":")[2] in unsigned_operations(service)


# Actions that AWS always allows regardless of IAM policies.
# https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html
_ALWAYS_ALLOWED_ACTIONS = frozenset({
    "sts:GetCallerIdentity",
    "sts:GetSessionToken",
})


def resolve_caller_identity(access_key_id: str) -> dict | None:
    """Display identity for a caller's access key — no signature verification
    and no policy evaluation, just the same key resolution the
    evaluator uses, shaped for API Gateway's IAM-authorized proxy events.

    Returns None for an unknown key. The ``session`` value is the raw
    ``sts._sessions`` record when the key is a temporary session (AssumeRole or
    Cognito identity-pool credentials), letting callers surface the cognito*
    identity fields it carries.
    """
    from ministack.core.responses import get_account_id
    from ministack.services import iam as iam_svc
    from ministack.services import sts as sts_svc

    if not access_key_id:
        return None
    account_id = get_account_id()

    if _is_root_key(access_key_id):
        return {
            "accessKey": access_key_id,
            "accountId": account_id,
            "userArn": f"arn:aws:iam::{account_id}:root",
            "userId": account_id,
            "principalOrgId": _principal_org_id(account_id),
            "session": None,
        }
    session = sts_svc._sessions.get(access_key_id)
    if session is not None:
        return {
            "accessKey": access_key_id,
            "accountId": account_id,
            "userArn": session.get("Arn", ""),
            "userId": session.get("UserId", ""),
            "principalOrgId": _principal_org_id(account_id),
            "session": session,
        }
    key_record = iam_svc._access_keys.get_scoped(account_id, None, access_key_id)
    if key_record is not None:
        user_name = key_record.get("UserName", "")
        user = iam_svc._users.get_scoped(account_id, None, user_name) or {}
        return {
            "accessKey": access_key_id,
            "accountId": account_id,
            "userArn": user.get("Arn") or f"arn:aws:iam::{account_id}:user/{user_name}",
            "userId": user.get("UserId", ""),
            "principalOrgId": _principal_org_id(account_id),
            "session": None,
        }
    return None


def _names_condition_key(policies: list[list[ParsedStatement]], prefix: str) -> bool:
    """Whether a condition in *policies* uses a key starting with *prefix* (lower case)."""
    return any(key.lower().startswith(prefix) for stmts in policies for stmt in stmts
               for block in stmt.conditions.values() if isinstance(block, dict) for key in block)


def enforce(access_key_id: str, iam_action: str, service: str,
            region: str, resource_arn: str = "*",
            service_context: dict[str, Any] | None = None) -> EvalResult | AuthError | None:
    """Check whether the request should be allowed.

    Returns ``None`` if allowed, an ``AuthError`` for authentication failures,
    or an ``EvalResult`` for authorization denials.
    """
    from ministack.core.responses import get_account_id

    account_id = get_account_id()
    failure = _s3_scope_failure(service)
    if failure:
        return failure
    # S3, API Gateway methods and function URLs can be called anonymously: an unsigned request is not checked.
    anonymous_ok = service == "s3" or iam_action in ("execute-api:Invoke", "lambda:InvokeFunctionUrl")
    if _anonymous.get() and not anonymous_ok and not _served_unsigned(service, iam_action):
        from ministack.core.iam_actions import anonymous_denial_message

        return AuthError("MissingAuthenticationToken", anonymous_denial_message(service, iam_action, resource_arn))
    unsigned = anonymous_ok and not access_key_id and _signed_request.get() is None
    checked = None if unsigned else authenticate(access_key_id or "", account_id)
    if isinstance(checked, AuthError):
        return checked

    # Actions that AWS never denies, regardless of policies
    if iam_action in _ALWAYS_ALLOWED_ACTIONS:
        return None

    principal = resolve_principal(access_key_id or "", account_id, checked)

    # Authentication failure
    if isinstance(principal, AuthError):
        return principal

    # Root principal: allow everything
    if principal.policies is None:
        return None

    context = {key.lower(): value for key, value in (service_context or {}).items()}
    # A request that names no tags has no aws:TagKeys.
    context.setdefault("aws:tagkeys", None)
    if _names_condition_key(principal.policies, "aws:resourcetag/"):
        from ministack.services.tagging import resource_tags
        for name, value in (resource_tags(resource_arn) or {}).items():
            context.setdefault(f"aws:resourcetag/{name}".lower(), value)
    ctx = EvalContext(
        principal_arn=principal.arn,
        principal_type=principal.type,
        principal_account=principal.account,
        action=iam_action,
        resource_arn=resource_arn,
        region=region,
        service_context=context,
        principal_keys=principal.keys,
    )

    result = evaluate(ctx, principal.policies, *principal.limits)
    found = _resource_policy(service, resource_arn)
    if found is not None and service == "s3" and "MultiRegionAccessPoint" in iam_action:
        found = None  # The MRAP policy governs data requests, not S3 Control's MRAP calls.
    if found is None and service == "s3" and "s3:dataaccesspointarn" in context:
        if (iam_action in ("s3:PutBucketPolicy", "s3:DeleteBucketPolicy") and result.decision != "Deny"
                and resource_arn == context["s3:dataaccesspointarn"]):
            # An access point refuses bucket policy writes as an explicit Deny of its own.
            return EvalResult("Deny", principal_arn=principal.arn, resource_policy_arn=resource_arn)
        # A data request through an access point meets the access point policy on the access point ARN.
        from ministack.services.s3 import access_point_policy_for_arn

        found = access_point_policy_for_arn(resource_arn)
    if found is not None:
        result = combine_resource_policy(result, ctx, *found, required=service in _POLICY_REQUIRED,
                                         limits=principal.limits)

    if result.decision == "Allow":
        return None

    # Deny or ImplicitDeny
    result.principal_arn = principal.arn
    return result


# ---------------------------------------------------------------------------
# Role ARN existence validation (cross-service)
# ---------------------------------------------------------------------------

def validate_role_arn(role_arn: str) -> str | None:
    """Validate that a role ARN points to an existing IAM role.

    Returns an error message if the role doesn't exist, or None if it does.
    Only validates when AUTH=true; returns None (no error) otherwise.
    """
    from ministack.app import AUTH
    if not AUTH:
        return None

    if not role_arn:
        return None

    from ministack.services import iam as iam_svc

    # Extract role name from ARN: arn:aws:iam::ACCT:role[/path]/NAME
    parts = role_arn.split(":")
    if len(parts) < 6:
        return f"Invalid role ARN: {role_arn}"
    resource = parts[5]
    if not resource.startswith("role/"):
        return f"Invalid role ARN: {role_arn}"
    # The role name is everything after "role/" (may include a path like /path/name)
    role_path_name = resource[5:]  # strip "role/"
    # The actual role name is the last segment
    role_name = role_path_name.rsplit("/", 1)[-1]

    if iam_svc._roles.get(role_name) is None:
        return (f"The role with name {role_name} cannot be found. "
                f"(Service: IAM, Status Code: 404)")
    return None


# ---------------------------------------------------------------------------
# Trust policy evaluation
# ---------------------------------------------------------------------------

def evaluate_trust_policy(trust_doc: str | dict,
                          caller_arn: str, ctx: EvalContext | None = None) -> bool:
    """Check if a role's trust policy allows the given caller to assume it.

    Matching denies override allows, regardless of statement order. Conditions
    use the shared evaluator; omitted request context leaves STS keys absent.

    Supports Principal forms:
      - ``"*"`` — anyone
      - ``{"AWS": "arn:aws:iam::ACCT:root"}`` — account root
      - ``{"AWS": "arn:aws:iam::ACCT:user/NAME"}`` — specific user
      - ``{"AWS": "ACCT"}`` — account ID shorthand
      - ``{"Service": "lambda.amazonaws.com"}`` — service principal
      - ``{"AWS": ["arn1", "arn2"]}`` — list of principals
      - ``{"Federated": "PROVIDER"}`` — web identity provider, for
        ``sts:AssumeRoleWithWebIdentity`` only
    """
    if isinstance(trust_doc, str):
        try:
            trust_doc = json.loads(trust_doc)
        except (json.JSONDecodeError, TypeError):
            return False
    if not isinstance(trust_doc, dict):
        return False

    statements = trust_doc.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]

    if ctx is None:
        ctx = EvalContext(
            principal_arn=caller_arn,
            principal_type=("User" if ":user/" in caller_arn else
                            "AssumedRole" if ":assumed-role/" in caller_arn else "Root"),
            principal_account=_account_from_arn(caller_arn) or "",
            action="sts:AssumeRole", resource_arn="*", region="",
        )
    federated = ctx.action == "sts:AssumeRoleWithWebIdentity"
    allowed = False
    for stmt in statements:
        if not isinstance(stmt, dict):
            continue
        if stmt.get("Effect") not in ("Allow", "Deny"):
            continue
        # Match the requested action, including exclusions in NotAction.
        actions = stmt.get("Action", [])
        not_actions = stmt.get("NotAction", [])
        if isinstance(actions, str):
            actions = [actions]
        if isinstance(not_actions, str):
            not_actions = [not_actions]
        if not (actions or not_actions) or not _action_matches(ctx.action, actions, not_actions):
            continue
        # Check Principal
        principal = stmt.get("Principal", {})
        if not _trust_principal_matches(principal, caller_arn, federated):
            continue
        if not _conditions_met(stmt.get("Condition", {}), ctx):
            continue
        if stmt["Effect"] == "Deny":
            return False
        allowed = True

    return allowed


def _trust_principal_matches(principal: Any, caller_arn: str, federated: bool) -> bool:
    """Whether a trust-policy Principal names the caller or, if *federated*, the provider."""
    if not federated:
        return _principal_matches(principal, caller_arn)
    if principal == "*":
        return True
    providers = principal.get("Federated", []) if isinstance(principal, dict) else []
    return caller_arn in ([providers] if isinstance(providers, str) else providers)


def _principal_matches(principal: Any, caller_arn: str) -> bool:
    """Check if a trust policy Principal matches the caller."""
    if principal == "*":
        return True
    if isinstance(principal, str):
        return fnmatch_iam(caller_arn, principal)
    if isinstance(principal, dict):
        # Check AWS principals
        aws_principals = principal.get("AWS", [])
        if isinstance(aws_principals, str):
            aws_principals = [aws_principals]
        for p in aws_principals:
            if p == "*":
                return True
            # Account ID shorthand
            if re.match(r"^\d{12}$", p):
                if f":{p}:" in caller_arn:
                    return True
                continue
            if fnmatch_iam(caller_arn, p):
                return True
            # A resource policy names an IAM role ARN, while a request made
            # after AssumeRole carries the matching STS assumed-role ARN.
            # AWS treats those as the same principal for resource policies.
            if ":role/" in p and ":assumed-role/" in caller_arn:
                principal_account = p.split(":")[4]
                caller_account = caller_arn.split(":")[4]
                role_name = p.rsplit("/", 1)[-1]
                assumed_role = caller_arn.split(":assumed-role/", 1)[1].split("/", 1)[0]
                if principal_account == caller_account and role_name == assumed_role:
                    return True
            # Also match account root against any principal in that account
            if p.endswith(":root") and f":{p.split(':')[4]}:" in caller_arn:
                return True
        # No IAM principal is a service, but MiniStack's own service calls
        # use root credentials, so a Service principal lets root through.
        if principal.get("Service") and caller_arn.endswith(":root"):
            return True
    return False
