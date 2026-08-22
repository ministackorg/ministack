"""IAM policy evaluation engine.

Implements the AWS IAM policy evaluation algorithm (single-account,
no SCPs/permissions boundaries/session policies) behind the AUTH=true flag.

Evaluation order:
  1. Explicit Deny in any applicable policy → DENY
  2. Explicit Allow in any applicable policy → ALLOW
  3. No matching Allow → implicit DENY
"""

import datetime as _dt
import ipaddress
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("ministack")


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
    principal_type: str  # "User", "AssumedRole", "Root"
    principal_account: str
    action: str  # "s3:PutObject"
    resource_arn: str  # "*" until per-service resource ARN construction
    region: str
    source_ip: str = "127.0.0.1"
    secure_transport: bool = False
    request_tags: dict[str, str] = field(default_factory=dict)
    tag_keys: list[str] = field(default_factory=list)


@dataclass
class EvalResult:
    decision: str  # "Allow", "Deny", "ImplicitDeny"
    principal_arn: str = ""
    matched_sid: str = ""
    reason: str = ""


@dataclass
class PrincipalInfo:
    arn: str
    type: str  # "User", "AssumedRole", "Root"
    account: str
    policies: list[list[ParsedStatement]] | None  # None = root (allow-all)


@dataclass
class AuthError:
    """Authentication failure (before policy evaluation)."""
    code: str  # "InvalidClientTokenId", "ExpiredTokenException", etc.
    message: str


# Default access keys that are always treated as root (backwards compat)
_ROOT_ACCESS_KEYS = frozenset({"test", ""})


# ---------------------------------------------------------------------------
# Wildcard matching (IAM-style)
# ---------------------------------------------------------------------------

_FNMATCH_CACHE: dict[str, re.Pattern] = {}


def fnmatch_iam(value: str, pattern: str) -> bool:
    """Case-insensitive IAM wildcard match.  ``*`` = any chars, ``?`` = one char."""
    if pattern == "*":
        return True
    key = pattern.lower()
    compiled = _FNMATCH_CACHE.get(key)
    if compiled is None:
        regex = ""
        for ch in key:
            if ch == "*":
                regex += ".*"
            elif ch == "?":
                regex += "."
            else:
                regex += re.escape(ch)
        compiled = re.compile(f"^{regex}$", re.IGNORECASE)
        if len(_FNMATCH_CACHE) < 4096:
            _FNMATCH_CACHE[key] = compiled
    return compiled.match(value) is not None


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
        return any(fnmatch_iam(resource_arn, p) for p in resources)
    if not_resources:
        return not any(fnmatch_iam(resource_arn, p) for p in not_resources)
    return True


# ---------------------------------------------------------------------------
# Condition evaluation
# ---------------------------------------------------------------------------

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
        return ctx.tag_keys
    if k.startswith("aws:requesttag/"):
        tag_key = key[len("aws:RequestTag/"):]
        return ctx.request_tags.get(tag_key)
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
    return fnmatch_iam(actual, expected)


def _op_string_not_like(actual: str, expected: str) -> bool:
    return not fnmatch_iam(actual, expected)


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
    return fnmatch_iam(actual, expected)


def _op_arn_not_like(actual: str, expected: str) -> bool:
    return not fnmatch_iam(actual, expected)


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


def _evaluate_single_condition(operator: str, actual: Any,
                               expected_values: list[str]) -> bool:
    """One condition key vs its expected values.  Values are OR'd."""
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
        return if_exists or for_all  # ForAllValues on missing key = true (empty set)

    # Multi-valued context key
    if isinstance(actual, list):
        actual_list = [str(v) for v in actual]
    else:
        actual_list = [str(actual)]

    expected_str = [str(v) for v in expected_values]

    if for_all:
        # Every actual value must match at least one expected value
        return all(
            any(op_func(av, ev) for ev in expected_str)
            for av in actual_list
        )
    if for_any:
        # At least one actual value must match at least one expected value
        return any(
            any(op_func(av, ev) for ev in expected_str)
            for av in actual_list
        )

    # Standard: any expected value matching any actual value satisfies the condition
    return any(
        op_func(str(actual), ev) for ev in expected_str
    )


def _conditions_met(conditions: dict, ctx: EvalContext) -> bool:
    """All condition blocks must be satisfied (AND between operators).
    Multiple values within a key are OR'd."""
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

def evaluate(ctx: EvalContext,
             policies: list[list[ParsedStatement]]) -> EvalResult:
    """Evaluate all policies against a request context.

    ``policies`` is a list of parsed-statement lists (one per policy document).
    Returns Allow, Deny, or ImplicitDeny.
    """
    # Flatten all statements
    all_stmts: list[ParsedStatement] = []
    for stmts in policies:
        all_stmts.extend(stmts)

    # Step 1: explicit Deny
    for stmt in all_stmts:
        if stmt.effect != "Deny":
            continue
        if not _action_matches(ctx.action, stmt.actions, stmt.not_actions):
            continue
        if not _resource_matches(ctx.resource_arn, stmt.resources, stmt.not_resources):
            continue
        if not _conditions_met(stmt.conditions, ctx):
            continue
        return EvalResult("Deny", ctx.principal_arn, stmt.sid, "Explicit deny")

    # Step 2: explicit Allow
    for stmt in all_stmts:
        if stmt.effect != "Allow":
            continue
        if not _action_matches(ctx.action, stmt.actions, stmt.not_actions):
            continue
        if not _resource_matches(ctx.resource_arn, stmt.resources, stmt.not_resources):
            continue
        if not _conditions_met(stmt.conditions, ctx):
            continue
        return EvalResult("Allow", ctx.principal_arn, stmt.sid, "Explicit allow")

    # Step 3: implicit deny
    return EvalResult("ImplicitDeny", ctx.principal_arn, "",
                      "No matching Allow statement")


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


def _resolve_managed_policy_document(policy_arn: str,
                                     account_id: str) -> dict | str | None:
    """Resolve a managed policy ARN to its default version's document."""
    from ministack.services import iam as iam_svc

    # AWS-managed policy
    if policy_arn.startswith("arn:aws:iam::aws:policy/"):
        mp = iam_svc._aws_managed_policies.get(policy_arn)
        if mp:
            default_vid = mp.get("DefaultVersionId", "v1")
            versions = mp.get("Versions", {})
            ver = versions.get(default_vid, {})
            return ver.get("Document")
        return None

    # Customer-managed policy — look up by ARN
    # The _policies dict is keyed by policy name; .items() returns current account's
    for _name, policy in iam_svc._policies.items():
        if policy.get("Arn") == policy_arn:
            default_vid = policy.get("DefaultVersionId", "v1")
            versions = policy.get("Versions", {})
            ver = versions.get(default_vid, {})
            return ver.get("Document")
    return None


def _is_root_key(access_key_id: str) -> bool:
    """Keys that are always treated as root: empty, 'test', 12-digit account IDs."""
    if not access_key_id or access_key_id in _ROOT_ACCESS_KEYS:
        return True
    if re.match(r"^\d{12}$", access_key_id):
        return True
    return False


def resolve_principal(access_key_id: str,
                      account_id: str) -> PrincipalInfo | AuthError:
    """Resolve an access key to a principal and their policies.

    Returns a ``PrincipalInfo`` on success or an ``AuthError`` when
    authentication fails (unknown key, inactive key, expired session).
    """
    import time

    from ministack.services import iam as iam_svc
    from ministack.services import sts as sts_svc

    # Root / default keys — allow-all, no checks
    if _is_root_key(access_key_id):
        return PrincipalInfo(
            arn=f"arn:aws:iam::{account_id}:root",
            type="Root",
            account=account_id,
            policies=None,
        )

    # Session credentials (AssumeRole) — ASIA prefix
    if access_key_id in sts_svc._sessions:
        session = sts_svc._sessions[access_key_id]
        # Check expiry
        expiration = session.get("Expiration")
        if expiration is not None and time.time() > expiration:
            return AuthError(
                "ExpiredTokenException",
                "The security token included in the request is expired",
            )
        assumed_arn = session.get("Arn", "")
        role_name = _role_name_from_assumed_arn(assumed_arn)
        policies = _gather_role_policies(role_name, account_id)
        return PrincipalInfo(
            arn=assumed_arn,
            type="AssumedRole",
            account=account_id,
            policies=policies,
        )

    # IAM user access key — AKIA prefix
    key_record = iam_svc._access_keys.get_scoped(account_id, None, access_key_id)
    if key_record is not None:
        # Check key status
        if key_record.get("Status") == "Inactive":
            return AuthError(
                "InvalidClientTokenId",
                "The security token included in the request is invalid.",
            )
        user_name = key_record.get("UserName", "")
        policies = _gather_user_policies(user_name, account_id)
        return PrincipalInfo(
            arn=f"arn:aws:iam::{account_id}:user/{user_name}",
            type="User",
            account=account_id,
            policies=policies,
        )

    # Unknown access key — reject
    return AuthError(
        "UnrecognizedClientException",
        "The security token included in the request is invalid.",
    )


# ---------------------------------------------------------------------------
# Top-level enforcement entry point
# ---------------------------------------------------------------------------

# Actions that AWS always allows regardless of IAM policies.
# https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic.html
_ALWAYS_ALLOWED_ACTIONS = frozenset({
    "sts:GetCallerIdentity",
    "sts:GetSessionToken",
    "sts:GetAccessKeyInfo",
})


def enforce(access_key_id: str, iam_action: str, service: str,
            region: str, resource_arn: str = "*") -> EvalResult | AuthError | None:
    """Check whether the request should be allowed.

    Returns ``None`` if allowed, an ``AuthError`` for authentication failures,
    or an ``EvalResult`` for authorization denials.
    """
    # Actions that AWS never denies, regardless of policies
    if iam_action in _ALWAYS_ALLOWED_ACTIONS:
        return None

    from ministack.core.responses import get_account_id

    account_id = get_account_id()
    principal = resolve_principal(access_key_id or "", account_id)

    # Authentication failure
    if isinstance(principal, AuthError):
        return principal

    # Root principal: allow everything
    if principal.policies is None:
        return None

    ctx = EvalContext(
        principal_arn=principal.arn,
        principal_type=principal.type,
        principal_account=principal.account,
        action=iam_action,
        resource_arn=resource_arn,
        region=region,
    )

    result = evaluate(ctx, principal.policies)

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
                          caller_arn: str) -> bool:
    """Check if a role's trust policy allows the given caller to assume it.

    Supports Principal forms:
      - ``"*"`` — anyone
      - ``{"AWS": "arn:aws:iam::ACCT:root"}`` — account root
      - ``{"AWS": "arn:aws:iam::ACCT:user/NAME"}`` — specific user
      - ``{"AWS": "ACCT"}`` — account ID shorthand
      - ``{"Service": "lambda.amazonaws.com"}`` — service principal
      - ``{"AWS": ["arn1", "arn2"]}`` — list of principals
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

    for stmt in statements:
        if not isinstance(stmt, dict):
            continue
        if stmt.get("Effect") != "Allow":
            continue
        # Check Action includes sts:AssumeRole (or *)
        actions = stmt.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        if not any(fnmatch_iam("sts:AssumeRole", a) for a in actions):
            continue
        # Check Principal
        principal = stmt.get("Principal", {})
        if _principal_matches(principal, caller_arn):
            return True

    return False


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
            # Also match account root against any principal in that account
            if p.endswith(":root") and f":{p.split(':')[4]}:" in caller_arn:
                return True
        # Service principals — match if the caller ARN looks like it came from
        # that service (e.g., lambda.amazonaws.com allows Lambda-invoked assumes).
        # In local dev, service principals are typically used by MiniStack's own
        # service dispatch (Lambda, ECS, etc.), so we also allow if the caller
        # is root (service-to-service calls use root credentials internally).
        svc_principals = principal.get("Service", [])
        if isinstance(svc_principals, str):
            svc_principals = [svc_principals]
        if svc_principals:
            if ":root" in caller_arn:
                return True
            for svc in svc_principals:
                # Extract service prefix: "lambda.amazonaws.com" → "lambda"
                svc_prefix = svc.split(".")[0] if "." in svc else svc
                if svc_prefix in caller_arn:
                    return True
    return False
