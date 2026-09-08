# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
CloudFormation engine — pure functions for template parsing, parameter resolution,
condition evaluation, intrinsic function resolution, and topological sorting.
"""

import base64
import copy
import heapq
import ipaddress
import itertools
import json
import logging
import os
import re
from types import SimpleNamespace

import yaml

from ministack.core.responses import get_account_id, get_region

logger = logging.getLogger("cloudformation")

# Sentinel for AWS::NoValue
_NO_VALUE = object()

# REGION kept for backwards compat with old imports; new code must prefer
# get_region() so AWS::Region reflects the caller's request region (#398).
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")


# ===========================================================================
# YAML Parser -- CloudFormation tag support
# ===========================================================================

class CfnLoader(yaml.SafeLoader):
    """YAML loader that handles CloudFormation intrinsic function tags."""
    pass


def _construct_cfn_tag(tag_name):
    """Build a constructor that wraps the value in {tag_name: value}."""
    def constructor(loader, node):
        if isinstance(node, yaml.ScalarNode):
            val = loader.construct_scalar(node)
        elif isinstance(node, yaml.SequenceNode):
            val = loader.construct_sequence(node, deep=True)
        elif isinstance(node, yaml.MappingNode):
            val = loader.construct_mapping(node, deep=True)
        else:
            val = loader.construct_scalar(node)
        return {tag_name: val}
    return constructor


def _construct_getatt(loader, node):
    """!GetAtt -- scalar 'A.B' splits on first dot; sequence passes through."""
    if isinstance(node, yaml.ScalarNode):
        val = loader.construct_scalar(node)
        parts = val.split(".", 1)
        if len(parts) == 2:
            return {"Fn::GetAtt": [parts[0], parts[1]]}
        return {"Fn::GetAtt": [val, ""]}
    if isinstance(node, yaml.SequenceNode):
        val = loader.construct_sequence(node, deep=True)
        return {"Fn::GetAtt": val}
    val = loader.construct_scalar(node)
    return {"Fn::GetAtt": [val, ""]}


def _construct_timestamp(loader, node):
    """Override timestamp to preserve date strings as plain strings."""
    return loader.construct_scalar(node)


# Register all CFN tags
_SIMPLE_TAGS = {
    "!Ref": "Ref",
    "!Sub": "Fn::Sub",
    "!Join": "Fn::Join",
    "!Split": "Fn::Split",
    "!Select": "Fn::Select",
    "!If": "Fn::If",
    "!Equals": "Fn::Equals",
    "!And": "Fn::And",
    "!Or": "Fn::Or",
    "!Not": "Fn::Not",
    "!Base64": "Fn::Base64",
    "!FindInMap": "Fn::FindInMap",
    "!ImportValue": "Fn::ImportValue",
    "!GetAZs": "Fn::GetAZs",
    "!Condition": "Condition",
    "!Cidr": "Fn::Cidr",
}

for _tag, _fn_name in _SIMPLE_TAGS.items():
    CfnLoader.add_constructor(_tag, _construct_cfn_tag(_fn_name))

CfnLoader.add_constructor("!GetAtt", _construct_getatt)
# Preserve date strings -- override the implicit timestamp resolver
CfnLoader.add_constructor("tag:yaml.org,2002:timestamp", _construct_timestamp)


def _parse_template(template_body: str) -> dict:
    """Parse a CFN template from JSON or YAML."""
    template_body = template_body.strip()
    if template_body.startswith("{"):
        result = json.loads(template_body)
    else:
        result = yaml.load(template_body, Loader=CfnLoader)
    if not isinstance(result, dict):
        raise ValueError("Template must be a JSON or YAML mapping")
    return result


# ===========================================================================
# Template pre-flight
# ===========================================================================

_DYNAMIC_REFERENCE = re.compile(r"\{\{resolve:([a-z-]+):([^}]*)\}\}")
_DYNAMIC_SERVICES = ("ssm", "ssm-secure", "secretsmanager")


def _find_dynamic_references(value, found: set) -> None:
    if isinstance(value, str):
        for m in _DYNAMIC_REFERENCE.finditer(value):
            found.add(m.group(0))
    elif isinstance(value, dict):
        for v in value.values():
            _find_dynamic_references(v, found)
    elif isinstance(value, list):
        for v in value:
            _find_dynamic_references(v, found)


def _has_dynamic_references(template: dict) -> bool:
    refs: set[str] = set()
    _find_dynamic_references((template or {}).get("Resources"), refs)
    return bool(refs)


def _resolve_dynamic_reference(literal: str) -> str:
    """Resolve one ``{{resolve:...}}`` literal against the in-process SSM /
    Secrets Manager stores. Raises ``ValueError`` when it cannot be resolved,
    which fails the resource the way an unresolvable reference does on AWS.
    ``ssm`` / ``ssm-secure`` take ``name[:version]`` (no version = latest);
    ``secretsmanager`` takes
    ``secret-id[:SecretString[:json-key[:version-stage[:version-id]]]]``,
    the secret id being a name or an ARN."""
    from ministack.services import secretsmanager, ssm

    m = _DYNAMIC_REFERENCE.fullmatch(literal)
    service, body = m.group(1), m.group(2)
    if service in ("ssm", "ssm-secure"):
        name, _, version = body.partition(":")
        value = ssm.resolve_parameter_value(name, version or None, decrypt=True)
        if value is None:
            raise ValueError(
                f"Dynamic reference {literal} could not be resolved: parameter "
                f"{name}" + (f" version {version}" if version else "") + " not found")
        return value
    if body.startswith("arn:"):
        parts = body.split(":")
        secret_id, rest = ":".join(parts[:7]), parts[7:]
    else:
        parts = body.split(":")
        secret_id, rest = parts[0], parts[1:]
    rest += [""] * (4 - len(rest))
    secret_string, json_key, version_stage, version_id = rest[:4]
    if secret_string and secret_string != "SecretString":
        raise ValueError(
            f"Dynamic reference {literal} is invalid: the secret-string segment "
            "must be SecretString")
    if version_stage and version_id:
        raise ValueError(
            f"Dynamic reference {literal} is invalid: specify either a version "
            "stage or a version id, not both")
    value = secretsmanager.resolve_secret_string(
        secret_id, version_stage or "AWSCURRENT", version_id or None)
    if value is None:
        raise ValueError(
            f"Dynamic reference {literal} could not be resolved: secret "
            f"{secret_id} not found")
    if json_key:
        try:
            value = json.loads(value)[json_key]
        except (ValueError, KeyError, TypeError):
            raise ValueError(
                f"Dynamic reference {literal} could not be resolved: key "
                f"{json_key} not found in the secret") from None
        if not isinstance(value, str):
            value = json.dumps(value)
    return value


def _resolve_dynamic_references(value, previous: dict | None = None,
                                reuse_ssm: bool = False,
                                reuse_secrets: bool = False):
    """Substitute every ``{{resolve:...}}`` inside ``value`` (recursively).

    Returns ``(resolved_value, {literal: value})``. ``previous`` is the map a
    prior deployment of the same resource recorded; ``reuse_ssm`` /
    ``reuse_secrets`` keep those values instead of resolving again, which is
    how the two kinds differ on update: an ``ssm`` reference re-resolves
    whenever the stack is updated with a changed template, a
    ``secretsmanager`` reference only when the resource that carries it
    changes.
    """
    previous = previous or {}
    resolved: dict = {}

    def one(literal):
        if literal in resolved:
            return resolved[literal]
        service = _DYNAMIC_REFERENCE.fullmatch(literal).group(1)
        reuse = reuse_secrets if service == "secretsmanager" else reuse_ssm
        if reuse and literal in previous:
            resolved[literal] = previous[literal]
        else:
            resolved[literal] = _resolve_dynamic_reference(literal)
        return resolved[literal]

    def walk(node):
        if isinstance(node, str):
            if "{{resolve:" not in node:
                return node
            return _DYNAMIC_REFERENCE.sub(lambda m: one(m.group(0)), node)
        if isinstance(node, list):
            return [walk(v) for v in node]
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        return node

    return walk(value), resolved


def validate_template_support(template: dict, conditions: dict,
                              params: dict | None = None) -> None:
    """Reject up front what provisioning could only fail on halfway through.

    Real CloudFormation validates resource types before it touches anything:
    CreateStack, UpdateStack, CreateChangeSet and ValidateTemplate all answer
    ``Template format error: Unrecognized resource types: [...]`` and no stack
    record is created. Doing the same here means an unsupported type no longer
    provisions its predecessors first and rolls them back. Condition-false
    resources are exempt, as they are during provisioning.

    Dynamic references are resolved at provisioning time (``ssm``,
    ``ssm-secure`` and ``secretsmanager``); a reference to any other service
    is refused here, as is a malformed one.

    Raises ``ValueError`` with the message the caller wraps as a
    ``ValidationError``.

    A template that still declares a ``Transform`` is exempt from the type
    check: a macro can rewrite any resource, so real CloudFormation cannot
    (and does not) pre-validate types through one — ``sam validate`` sends
    SAM templates with ``AWS::Serverless::*`` resources to ValidateTemplate
    and they pass. CreateStack, UpdateStack and CreateChangeSet apply the SAM
    transform before calling this, so their expanded templates are validated
    as usual.

    ``params`` (the resolved ``{name: {Value, NoEcho}}`` map) turns on the
    ``Rules`` section: CreateStack, UpdateStack and CreateChangeSet pass it,
    ValidateTemplate (no parameter values) does not.
    """
    from .provisioners import _RESOURCE_HANDLERS

    unrecognized: set[str] = set()
    if not template.get("Transform"):
        for res in (template.get("Resources") or {}).values():
            if not isinstance(res, dict):
                continue
            cond = res.get("Condition")
            if cond and not conditions.get(cond, True):
                continue
            rtype = res.get("Type", "AWS::CloudFormation::CustomResource")
            # The registered AWS::CloudFormation::* types (WaitCondition,
            # WaitConditionHandle, Stack, CustomResource) pass like any other
            # handler; an unregistered one (Macro, HookVersion, a typo) is
            # unrecognized, not a silent placeholder.
            if rtype in _RESOURCE_HANDLERS or rtype.startswith("Custom::"):
                continue
            unrecognized.add(rtype)
    if unrecognized:
        raise ValueError(
            "Template format error: Unrecognized resource types: ["
            + ", ".join(sorted(unrecognized)) + "]"
        )
    _validate_template_statics(template)

    refs: set[str] = set()
    _find_dynamic_references(template.get("Resources"), refs)
    _find_dynamic_references(template.get("Outputs"), refs)
    unsupported = sorted(
        r for r in refs if _DYNAMIC_REFERENCE.fullmatch(r).group(1) not in _DYNAMIC_SERVICES)
    if unsupported:
        raise ValueError(
            "Template format error: unsupported dynamic reference(s): "
            + ", ".join(unsupported)
            + " (supported: ssm, ssm-secure, secretsmanager)"
        )
    if params is not None:
        _evaluate_rules(template, params, conditions)


# ===========================================================================
# Parameter Resolver
# ===========================================================================

_AWS_SPECIFIC_TYPES = {
    "AWS::SSM::Parameter::Type",
    "AWS::SSM::Parameter::Value<String>",
    "AWS::SSM::Parameter::Value<List<String>>",
    "AWS::SSM::Parameter::Value<AWS::EC2::Image::Id>",
    "AWS::EC2::AvailabilityZone::Name",
    "AWS::EC2::Image::Id",
    "AWS::EC2::Instance::Id",
    "AWS::EC2::KeyPair::KeyName",
    "AWS::EC2::SecurityGroup::GroupName",
    "AWS::EC2::SecurityGroup::Id",
    "AWS::EC2::Subnet::Id",
    "AWS::EC2::Volume::Id",
    "AWS::EC2::VPC::Id",
    "AWS::Route53::HostedZone::Id",
}

# Any ``AWS::SSM::Parameter::Value<...>`` type resolves its given value (an SSM
# parameter *name*) against SSM Parameter Store before Ref ever sees it — this
# is true for every inner type (String, List<String>, CommaDelimitedList, and
# the AWS-specific and List<AWS-specific> forms), so match on the prefix rather
# than an enumerated subset. The ``Value<`` in the prefix deliberately excludes
# ``AWS::SSM::Parameter::Name`` (Ref returns the name) and
# ``AWS::SSM::Parameter::Type`` (a template-side type constraint, not a lookup).
_SSM_PARAMETER_VALUE_PREFIX = "AWS::SSM::Parameter::Value<"


def _constraint_error(name: str, defn: dict, reason: str) -> ValueError:
    """The ValidationError a violated parameter constraint raises: real
    CloudFormation answers ``Parameter 'P' must match pattern ^[a-z]+$`` (measured),
    and a ``ConstraintDescription`` replaces the generic reason."""
    description = defn.get("ConstraintDescription")
    return ValueError(f"Parameter '{name}' {description or reason}")


def _check_parameter_constraints(name: str, defn: dict, ptype: str, value: str) -> None:
    """Apply AllowedPattern, MinLength, MaxLength, MinValue and MaxValue the way
    the Parameters reference defines them: the pattern matches the whole value
    (each member of a CommaDelimitedList), the lengths apply to String types,
    the bounds to Number types (each member of a List<Number>)."""
    is_list = ptype in ("CommaDelimitedList", "List<Number>")
    members = [m.strip() for m in value.split(",")] if is_list else [value]
    pattern = defn.get("AllowedPattern")
    if pattern is not None and ptype != "Number":
        try:
            compiled = re.compile(str(pattern))
        except re.error as exc:
            raise ValueError(
                f"Parameter '{name}' has an invalid AllowedPattern: {exc}") from None
        if not all(compiled.fullmatch(m) for m in members):
            raise _constraint_error(name, defn, f"must match pattern {pattern}")
    if ptype in ("Number", "List<Number>"):
        numbers = [float(m) for m in members]
        if "MinValue" in defn and any(n < float(defn["MinValue"]) for n in numbers):
            raise _constraint_error(
                name, defn, f"must be a number not less than {defn['MinValue']}")
        if "MaxValue" in defn and any(n > float(defn["MaxValue"]) for n in numbers):
            raise _constraint_error(
                name, defn, f"must be a number not greater than {defn['MaxValue']}")
    elif not is_list:
        if "MinLength" in defn and len(value) < int(defn["MinLength"]):
            raise _constraint_error(
                name, defn, f"must contain at least {defn['MinLength']} characters")
        if "MaxLength" in defn and len(value) > int(defn["MaxLength"]):
            raise _constraint_error(
                name, defn, f"must contain at most {defn['MaxLength']} characters")


def _resolve_parameters(template: dict, provided_params: list[dict],
                        previous_params: dict | None = None) -> dict:
    """Resolve template parameters with provided values and defaults.

    ``previous_params`` (a prior {name: {Value, NoEcho}} map) is consulted for
    parameters sent with ``UsePreviousValue=true`` — which is how
    ``aws cloudformation deploy`` re-sends existing parameters when no
    ``--parameter-overrides`` are given.

    Returns dict of param_name -> {Value, NoEcho}.
    """
    param_defs = template.get("Parameters", {})
    provided_map = {p["Key"]: p for p in provided_params if "Key" in p}
    previous_params = previous_params or {}
    resolved = {}

    for name, defn in param_defs.items():
        ptype = defn.get("Type", "String")
        no_echo = str(defn.get("NoEcho", "false")).lower() == "true"

        entry = provided_map.get(name)
        # Whether `value` below still needs SSM-name resolution, or is
        # already a final value — a `previous_params` hit is always the
        # latter: it's this same parameter's *already-resolved* Value from
        # the prior deployment (see the end of this loop, where `resolved`
        # is built), not the SSM parameter name again.
        already_resolved = False
        if entry is not None and entry.get("UsePreviousValue"):
            prev = previous_params.get(name)
            if prev is not None:
                if (ptype.startswith(_SSM_PARAMETER_VALUE_PREFIX)
                        and isinstance(prev, dict) and "SsmName" in prev):
                    # An `AWS::SSM::Parameter::Value<...>` type is re-resolved on
                    # every stack operation, as real CloudFormation does: the
                    # prior deployment stored the SSM parameter NAME (`SsmName`)
                    # for exactly this, so `UsePreviousValue=true` (what `cdk
                    # deploy` sends for every non-overridden parameter) picks up
                    # a value that changed in Parameter Store since the last
                    # deploy. Leaving `already_resolved` False re-runs the SSM
                    # lookup below. Stacks persisted before `SsmName` was tracked
                    # fall through to the last resolved value (the old behavior).
                    value = prev["SsmName"]
                else:
                    value = prev["Value"] if isinstance(prev, dict) else prev
                    already_resolved = True
            elif "Default" in defn:
                value = defn["Default"]
            else:
                raise ValueError(
                    f"Parameter '{name}' has no previous value and no Default")
        elif entry is not None:
            value = entry.get("Value", "")
        elif "Default" in defn:
            value = defn["Default"]
        else:
            raise ValueError(f"Parameter '{name}' has no Default and was not provided")

        value = str(value) if value is not None else ""

        ssm_name = None
        if ptype.startswith(_SSM_PARAMETER_VALUE_PREFIX) and not already_resolved:
            # `value` up to here is the SSM parameter *name* (the template
            # parameter's Default, a caller-supplied override, or the previous
            # deployment's stored `SsmName`) — resolve it against the SSM store
            # the same way real CloudFormation does before Ref ever sees it, and
            # remember the name so the next update can re-resolve it. Local
            # import to avoid a cloudformation/ssm circular import at module load
            # time (see ecs.py's identical pattern for the same reason).
            from ministack.services import ssm
            param_name = value
            ssm_name = value
            resolved_value = ssm.resolve_parameter_value(param_name)
            if resolved_value is None:
                raise ValueError(
                    f"Parameter '{name}' failed to resolve: SSM parameter "
                    f"'{param_name}' does not exist"
                )
            value = resolved_value

        # Validate AllowedValues
        allowed = defn.get("AllowedValues")
        if allowed and value not in [str(a) for a in allowed]:
            raise ValueError(
                f"Parameter '{name}' value '{value}' is not in AllowedValues: {allowed}"
            )

        # Type coercion
        if ptype == "Number":
            # Validate it's numeric but keep as string for consistency
            try:
                float(value)
            except ValueError:
                raise ValueError(f"Parameter '{name}' value '{value}' is not a valid Number")
        elif ptype == "List<Number>":
            for member in value.split(","):
                try:
                    float(member.strip())
                except ValueError:
                    raise ValueError(
                        f"Parameter '{name}' value '{value}' is not a valid List<Number>")
        elif ptype == "CommaDelimitedList":
            # Keep as string; Fn::Select will split
            pass
        # AWS-specific types treated as String -- no extra validation

        _check_parameter_constraints(name, defn, ptype, value)

        out = {"Value": value, "NoEcho": no_echo}
        if ssm_name is not None:
            # Persisted so a later UpdateStack with UsePreviousValue re-resolves
            # the SSM name instead of reusing this now-stale resolved value.
            out["SsmName"] = ssm_name
        resolved[name] = out

    return resolved


# ===========================================================================
# Condition Evaluator
# ===========================================================================

def _evaluate_conditions(template: dict, params: dict) -> dict:
    """Evaluate all conditions in the template. Returns {name: bool}."""
    cond_defs = template.get("Conditions", {})
    evaluated: dict[str, bool] = {}

    def _eval(expr):
        if isinstance(expr, dict):
            if "Fn::Equals" in expr:
                args = expr["Fn::Equals"]
                left = _resolve_cond_value(args[0])
                right = _resolve_cond_value(args[1])
                return str(left) == str(right)
            if "Fn::And" in expr:
                return all(_eval(c) for c in expr["Fn::And"])
            if "Fn::Or" in expr:
                return any(_eval(c) for c in expr["Fn::Or"])
            if "Fn::Not" in expr:
                return not _eval(expr["Fn::Not"][0])
            if "Condition" in expr:
                cname = expr["Condition"]
                if cname not in evaluated:
                    evaluated[cname] = _eval(cond_defs[cname])
                return evaluated[cname]
            if "Ref" in expr:
                return _resolve_cond_value(expr)
        return bool(expr)

    def _resolve_cond_value(val):
        if isinstance(val, dict):
            if "Ref" in val:
                pname = val["Ref"]
                if pname in params:
                    return params[pname]["Value"]
                return pname
            if "Fn::Equals" in val:
                return _eval(val)
            if "Condition" in val:
                return _eval(val)
        return val

    for name, defn in cond_defs.items():
        if name not in evaluated:
            evaluated[name] = _eval(defn)

    return evaluated


# ===========================================================================
# Rules section
# ===========================================================================

# The functions the Rules section accepts (rules-section-structure.html lists
# them; Ref may be nested in all but Fn::ValueOf and Fn::ValueOfAll).
_RULE_FUNCTIONS = frozenset({
    "Fn::And", "Fn::Or", "Fn::Not", "Fn::Equals", "Fn::If", "Fn::Contains",
    "Fn::EachMemberEquals", "Fn::EachMemberIn", "Fn::RefAll", "Fn::ValueOf",
    "Fn::ValueOfAll", "Ref",
})

# Fn::RefAll / Fn::ValueOf / Fn::ValueOfAll read the account: the three
# parameter types whose attributes the reference documents are served from the
# in-process EC2 store, keyed by the attribute names the Rules reference lists.
_RULE_ACCOUNT_TYPES = {
    "AWS::EC2::VPC::Id": ("_vpcs", {"DefaultNetworkAcl": "DefaultNetworkAclId",
                                     "DefaultSecurityGroup": "DefaultSecurityGroupId"}),
    "AWS::EC2::Subnet::Id": ("_subnets", {"AvailabilityZone": "AvailabilityZone",
                                           "VpcId": "VpcId"}),
    "AWS::EC2::SecurityGroup::Id": ("_security_groups", {}),
}


class _RuleUnsupported(Exception):
    """A rule needs an account lookup this emulator does not serve; the rule is
    skipped with a warning instead of failing the stack operation."""


# The argument list each rule function takes (intrinsic-function-reference-rules):
# ``Fn::Not`` one condition, ``Fn::If`` a condition and two values, the rest
# two elements; ``Fn::And`` / ``Fn::Or`` any list, ``Ref`` / ``Fn::RefAll`` a name.
_RULE_ARITY = {
    "Fn::Equals": 2, "Fn::Contains": 2, "Fn::EachMemberEquals": 2,
    "Fn::EachMemberIn": 2, "Fn::ValueOf": 2, "Fn::ValueOfAll": 2,
    "Fn::Not": 1, "Fn::If": 3,
}


def _check_rule_expression(node, rule: str) -> None:
    """Arity and argument types of every rule function under ``rule``: a
    malformed argument is a template format error (unmeasured wording), not
    an evaluation crash. ``Fn::ValueOf`` and ``Fn::ValueOfAll`` take two
    strings: the reference says no other function can be used within them."""
    if isinstance(node, list):
        for value in node:
            _check_rule_expression(value, rule)
        return
    if not isinstance(node, dict):
        return
    for fn, args in node.items():
        if fn in ("Ref", "Fn::RefAll"):
            if not isinstance(args, str):
                raise ValueError(
                    f"Template format error: {fn} in rule {rule} must name a "
                    + ("parameter" if fn == "Ref" else "parameter type"))
            continue
        if fn in ("Fn::And", "Fn::Or"):
            if not isinstance(args, list) or not args:
                raise ValueError(
                    f"Template format error: {fn} in rule {rule} must be a list of "
                    "conditions")
        elif fn in _RULE_ARITY:
            if not isinstance(args, list) or len(args) != _RULE_ARITY[fn]:
                raise ValueError(
                    f"Template format error: {fn} in rule {rule} must be a list of "
                    f"{_RULE_ARITY[fn]} elements")
            if fn in ("Fn::ValueOf", "Fn::ValueOfAll") and not all(
                    isinstance(a, str) for a in args):
                raise ValueError(
                    f"Template format error: {fn} in rule {rule} takes two strings; "
                    "no function can be used within it")
        _check_rule_expression(args, rule)


def _rule_functions_used(node, found: set) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key.startswith("Fn::") or key == "Ref":
                found.add(key)
            _rule_functions_used(value, found)
    elif isinstance(node, list):
        for value in node:
            _rule_functions_used(value, found)


def _check_rules_section(template: dict) -> None:
    """The shape checks that need nothing but the template: every rule carries
    an ``Assertions`` list, every assertion an ``Assert``, only the rule
    functions appear, and every function has the arguments it takes. The
    function message is measured (a real account answered ``Template format
    error: Following functions are not supported in the Rules block of the
    template: [Fn::If]``, cloudformation-coverage-roadmap issue 921, 2021;
    the bracket holds the offending names sorted and comma-joined, the join
    unmeasured); the shape messages are unmeasured."""
    rules = template.get("Rules")
    if rules is None:
        return
    if not isinstance(rules, dict):
        raise ValueError("Template format error: Rules must be a map of rule name to rule")
    for name, rule in rules.items():
        if not isinstance(rule, dict) or not isinstance(rule.get("Assertions"), list):
            raise ValueError(
                f"Template format error: Rule {name} must contain an Assertions list")
        for assertion in rule["Assertions"]:
            if not isinstance(assertion, dict) or "Assert" not in assertion:
                raise ValueError(
                    f"Template format error: Every assertion of rule {name} must "
                    "contain an Assert")
    used: set[str] = set()
    _rule_functions_used(rules, used)
    unsupported = sorted(used - _RULE_FUNCTIONS)
    if unsupported:
        raise ValueError(
            "Template format error: Following functions are not supported in the "
            "Rules block of the template: [" + ", ".join(unsupported) + "]")
    for name, rule in rules.items():
        _check_rule_expression(rule.get("RuleCondition"), name)
        for assertion in rule["Assertions"]:
            _check_rule_expression(assertion["Assert"], name)


def _rule_inner_type(ptype: str) -> tuple[str, bool]:
    """The type a parameter's declared type wraps and whether it is a list:
    the SSM ``Value<...>`` wrapper is stripped first, then ``List<...>``
    (a ``CommaDelimitedList`` is a list of strings)."""
    if ptype.startswith(_SSM_PARAMETER_VALUE_PREFIX) and ptype.endswith(">"):
        ptype = ptype[len(_SSM_PARAMETER_VALUE_PREFIX):-1]
    if ptype == "CommaDelimitedList":
        return "String", True
    if ptype.startswith("List<") and ptype.endswith(">"):
        return ptype[len("List<"):-1], True
    return ptype, False


def _rule_param_value(name: str, defn: dict, params: dict):
    """A parameter as ``Ref`` sees it inside Rules: list-typed parameters are
    lists of their trimmed members, everything else the string value."""
    value = params[name]["Value"]
    _, is_list = _rule_inner_type(str(defn.get("Type", "String")))
    if is_list:
        return [m.strip() for m in str(value).split(",")] if value != "" else []
    return str(value)


def _rule_account_records(parameter_type: str):
    """The EC2 records behind an AWS-specific parameter type, or
    ``_RuleUnsupported`` for a type this emulator does not list."""
    spec = _RULE_ACCOUNT_TYPES.get(parameter_type)
    if spec is None:
        raise _RuleUnsupported(
            f"Fn::RefAll / Fn::ValueOfAll over {parameter_type} is not supported")
    from ministack.services import ec2
    store = getattr(ec2, spec[0])
    return {rid: store[rid] for rid in list(store)}, spec[1]


def _rule_attribute(parameter_type: str, resource_id: str, attribute: str) -> str:
    """One attribute of one account resource, as ``Fn::ValueOf`` returns it;
    a resource the store does not hold yields an empty string."""
    from ministack.services import ec2
    records, attributes = _rule_account_records(parameter_type)
    record = records.get(resource_id) or {}
    if attribute.startswith("Tags."):
        key = attribute[len("Tags."):]
        for tag in ec2._tags.get(resource_id, []) or []:
            if tag.get("Key") == key:
                return str(tag.get("Value", ""))
        return ""
    field = attributes.get(attribute)
    if field is None:
        raise _RuleUnsupported(
            f"attribute {attribute} of {parameter_type} is not supported")
    return str(record.get(field, ""))


def _evaluate_rules(template: dict, params: dict, conditions: dict) -> None:
    """Evaluate the ``Rules`` section against the resolved parameters.

    The Rules reference: a rule's ``Assertions`` are checked when its
    ``RuleCondition`` is absent or evaluates to true; every ``Assert`` must
    evaluate to true or the stack is neither created nor updated, and the
    ``AssertDescription`` is the message. ``Fn::RefAll``, ``Fn::ValueOf`` and
    ``Fn::ValueOfAll`` are served for ``AWS::EC2::VPC::Id``,
    ``AWS::EC2::Subnet::Id`` and ``AWS::EC2::SecurityGroup::Id`` from the
    in-process EC2 store; a rule over any other type is skipped with a
    warning. Raises ``ValueError`` with the message the caller wraps as a
    ``ValidationError`` (the failure text is unmeasured).
    """
    rules = template.get("Rules")
    if not rules:
        return
    param_defs = template.get("Parameters") or {}
    pseudo = {
        "AWS::Region": get_region(),
        "AWS::AccountId": get_account_id(),
        "AWS::URLSuffix": "amazonaws.com",
        "AWS::Partition": "aws",
    }

    def as_list(value, fn):
        if not isinstance(value, list):
            raise ValueError(
                f"Template error: {fn} expects a list as its first argument, got "
                f"'{value}'")
        return [str(v) for v in value]

    def as_bool(value, fn):
        if not isinstance(value, bool):
            raise ValueError(
                f"Template error: every argument of {fn} must evaluate to true or "
                f"false, got '{value}'")
        return value

    def ev(node):
        if not isinstance(node, dict) or len(node) != 1:
            return [ev(v) for v in node] if isinstance(node, list) else node
        fn, args = next(iter(node.items()))
        if fn == "Ref":
            if args in params:
                return _rule_param_value(args, param_defs.get(args, {}), params)
            if args in pseudo:
                return pseudo[args]
            raise ValueError(
                f"Template format error: Unresolved resource dependencies [{args}] "
                "in the Rules block of the template")
        if fn == "Fn::Equals":
            left, right = ev(args[0]), ev(args[1])
            if isinstance(left, list) or isinstance(right, list):
                return left == right
            return str(left) == str(right)
        if fn == "Fn::And":
            return all(as_bool(ev(a), fn) for a in args)
        if fn == "Fn::Or":
            return any(as_bool(ev(a), fn) for a in args)
        if fn == "Fn::Not":
            return not as_bool(ev(args[0]), fn)
        if fn == "Fn::If":
            chosen = ev(args[0])
            if isinstance(args[0], str):
                if args[0] not in conditions:
                    raise ValueError(
                        f"Template error: Fn::If refers to condition {args[0]} "
                        "which is not defined in the Conditions block")
                chosen = conditions[args[0]]
            return ev(args[1]) if as_bool(chosen, fn) else ev(args[2])
        if fn == "Fn::Contains":
            return str(ev(args[1])) in as_list(ev(args[0]), fn)
        if fn == "Fn::EachMemberEquals":
            wanted = str(ev(args[1]))
            return all(m == wanted for m in as_list(ev(args[0]), fn))
        if fn == "Fn::EachMemberIn":
            allowed = as_list(ev(args[1]), fn)
            return all(m in allowed for m in as_list(ev(args[0]), fn))
        if fn == "Fn::RefAll":
            records, _ = _rule_account_records(str(args))
            return list(records)
        if fn == "Fn::ValueOfAll":
            ptype, attribute = str(args[0]), str(args[1])
            records, _ = _rule_account_records(ptype)
            return [_rule_attribute(ptype, rid, attribute) for rid in records]
        if fn == "Fn::ValueOf":
            pname, attribute = str(args[0]), str(args[1])
            if pname not in params:
                raise ValueError(
                    f"Template format error: Unresolved resource dependencies "
                    f"[{pname}] in the Rules block of the template")
            base, _ = _rule_inner_type(str(param_defs.get(pname, {}).get("Type", "String")))
            value = _rule_param_value(pname, param_defs.get(pname, {}), params)
            if isinstance(value, list):
                return [_rule_attribute(base, v, attribute) for v in value]
            return _rule_attribute(base, value, attribute)
        raise ValueError(
            "Template format error: Following functions are not supported in the "
            f"Rules block of the template: [{fn}]")

    for name, rule in rules.items():
        try:
            condition = rule.get("RuleCondition")
            if condition is not None:
                chosen = ev(condition)
                if not isinstance(chosen, bool):
                    raise ValueError(
                        f"Template error: the RuleCondition of rule {name} must "
                        f"evaluate to true or false, got '{chosen}'")
                if not chosen:
                    continue
            for index, assertion in enumerate(rule["Assertions"], 1):
                result = ev(assertion["Assert"])
                if not isinstance(result, bool):
                    raise ValueError(
                        f"Template error: assertion {index} of rule {name} must "
                        f"evaluate to true or false, got '{result}'")
                if not result:
                    description = assertion.get("AssertDescription") \
                        or f"assertion {index} evaluated to false"
                    raise ValueError(f"Template error: rule {name} failed: {description}")
        except _RuleUnsupported as exc:
            logger.warning("Rules: skipping rule %s: %s", name, exc)
        except (TypeError, IndexError, KeyError) as exc:
            # The shape check above refuses what it knows; anything that still
            # does not evaluate is a template error, never an internal one.
            raise ValueError(
                f"Template format error: rule {name} could not be evaluated: "
                f"{exc}") from exc


# ===========================================================================
# Intrinsic Function Resolver
# ===========================================================================

def _cidr_blocks(ip_block: str, count: int, cidr_bits: int) -> list[str]:
    """``Fn::Cidr``: ``count`` consecutive subnets of ``ip_block`` whose mask is
    ``cidr_bits`` shorter than the address length (``cidrBits`` are subnet
    bits: 8 on an IPv4 /16 gives /24 blocks — measured on AWS:
    ``192.168.0.0/16, 2, 8`` is ``192.168.0.0/24,192.168.1.0/24``)."""
    if not 1 <= count <= 256:
        raise ValueError(
            f"Template error: Fn::Cidr count must be between 1 and 256, got {count}")
    try:
        network = ipaddress.ip_network(ip_block, strict=False)
    except ValueError:
        raise ValueError(f"Template error: Fn::Cidr ipBlock {ip_block!r} is not a CIDR block") from None
    new_prefix = network.max_prefixlen - cidr_bits
    if not network.prefixlen < new_prefix <= network.max_prefixlen:
        raise ValueError(
            f"Template error: Fn::Cidr cannot split {ip_block} into /{new_prefix} blocks")
    subnets = list(itertools.islice(network.subnets(new_prefix=new_prefix), count))
    if len(subnets) < count:
        raise ValueError(
            f"Template error: Fn::Cidr {ip_block} holds only {len(subnets)} /{new_prefix} blocks, "
            f"{count} requested")
    return [str(subnet) for subnet in subnets]


_CONDITION_FUNCTIONS = ("Fn::And", "Fn::Or", "Fn::Not", "Fn::Equals")


def _validate_template_statics(template: dict) -> None:
    """The template errors a real account raises before any stack exists and
    that need nothing but the template: a condition function as an Output
    value, and an Fn::FindInMap with literal keys that the Mappings section
    does not hold (both measured)."""
    for out in (template.get("Outputs") or {}).values():
        val = out.get("Value") if isinstance(out, dict) else None
        if isinstance(val, dict) and len(val) == 1 and next(iter(val)) in _CONDITION_FUNCTIONS:
            raise ValueError("Template format error: The Value field of every "
                             "Outputs member must evaluate to a String.")
    mappings = template.get("Mappings") or {}

    def walk(node):
        if isinstance(node, dict):
            if len(node) == 1 and "Fn::FindInMap" in node:
                args = node["Fn::FindInMap"]
                if (isinstance(args, list) and len(args) >= 3
                        and all(isinstance(a, str) for a in args[:3])
                        and not (len(args) > 3 and isinstance(args[3], dict)
                                 and "DefaultValue" in args[3])):
                    name, k1, k2 = args[:3]
                    if k1 not in mappings.get(name, {}) or k2 not in mappings.get(name, {}).get(k1, {}):
                        raise ValueError(
                            f"Template error: Unable to get mapping for {name}::{k1}::{k2}")
                for arg in args if isinstance(args, list) else []:
                    walk(arg)
                return
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    for section in ("Resources", "Outputs", "Conditions"):
        walk(template.get(section) or {})
    _check_rules_section(template)


def _unknown_attribute_message(res: dict, logical_id: str, attr: str) -> str:
    # Verbatim what CloudFormation reports in the stack event that starts the
    # rollback (measured on a real account against AWS::SQS::Queue).
    rtype = res.get("ResourceType") or logical_id
    return f"Requested attribute {attr} does not exist in schema for {rtype}"


def _resolve_refs(value, resources, params, conditions, mappings,
                  stack_name, stack_id):
    """Recursively resolve CloudFormation intrinsic functions."""
    if isinstance(value, str):
        return value

    if isinstance(value, list):
        resolved = [
            _resolve_refs(item, resources, params, conditions, mappings,
                          stack_name, stack_id)
            for item in value
        ]
        return [r for r in resolved if r is not _NO_VALUE]

    if not isinstance(value, dict):
        return value

    # --- Ref ---
    if "Ref" in value:
        ref = value["Ref"]
        # Pseudo-parameters
        pseudo = {
            "AWS::StackName": stack_name,
            "AWS::StackId": stack_id,
            "AWS::Region": get_region(),
            "AWS::AccountId": get_account_id(),
            "AWS::NoValue": _NO_VALUE,
            "AWS::URLSuffix": "amazonaws.com",
            "AWS::Partition": "aws",
            "AWS::NotificationARNs": [],
        }
        if ref in pseudo:
            return pseudo[ref]
        if ref in params:
            return params[ref]["Value"]
        # Resource physical ID
        if ref in resources and "PhysicalResourceId" in resources[ref]:
            return resources[ref]["PhysicalResourceId"]
        return ref

    # --- Fn::GetAtt ---
    if "Fn::GetAtt" in value:
        args = value["Fn::GetAtt"]
        if isinstance(args, str):
            parts = args.split(".", 1)
            logical_id = parts[0]
            attr = parts[1] if len(parts) > 1 else ""
        else:
            logical_id = args[0]
            attr = args[1] if len(args) > 1 else ""
        res = resources.get(logical_id, {})
        attrs = res.get("Attributes", {})
        if attr in attrs:
            return attrs[attr]
        if "PhysicalResourceId" in res:
            # The resource exists and does not expose the attribute. Real
            # CloudFormation fails the operation here (the stack rolls back);
            # returning the physical id instead handed callers a silently wrong
            # value.
            raise ValueError(_unknown_attribute_message(res, logical_id, attr))
        # Not provisioned yet (change-set diff): nothing to resolve against.
        return ""

    # --- Fn::Join ---
    if "Fn::Join" in value:
        args = value["Fn::Join"]
        delimiter = args[0]
        items = _resolve_refs(args[1], resources, params, conditions,
                              mappings, stack_name, stack_id)
        return delimiter.join(str(i) for i in items if i is not _NO_VALUE)

    # --- Fn::Sub ---
    if "Fn::Sub" in value:
        sub_val = value["Fn::Sub"]
        if isinstance(sub_val, list):
            template_str = sub_val[0]
            var_map = sub_val[1] if len(sub_val) > 1 else {}
            # Resolve values in the var_map first
            resolved_map = {}
            for k, v in var_map.items():
                resolved_map[k] = _resolve_refs(v, resources, params,
                                                conditions, mappings,
                                                stack_name, stack_id)
        else:
            template_str = sub_val
            resolved_map = {}

        def _sub_replace(match):
            var = match.group(1)
            # ${!Literal} escape: emit ${Literal} without substituting
            if var.startswith("!"):
                return "${" + var[1:] + "}"
            # Check explicit var map first
            if var in resolved_map:
                return str(resolved_map[var])
            # Pseudo-params
            pseudo = {
                "AWS::StackName": stack_name,
                "AWS::StackId": stack_id,
                "AWS::Region": get_region(),
                "AWS::AccountId": get_account_id(),
                "AWS::URLSuffix": "amazonaws.com",
                "AWS::Partition": "aws",
            }
            if var in pseudo:
                return str(pseudo[var])
            # Param
            if var in params:
                return str(params[var]["Value"])
            # Resource.Attr
            if "." in var:
                parts = var.split(".", 1)
                res = resources.get(parts[0], {})
                attrs = res.get("Attributes", {})
                if parts[1] in attrs:
                    return str(attrs[parts[1]])
                if "PhysicalResourceId" in res:
                    raise ValueError(
                        _unknown_attribute_message(res, parts[0], parts[1]))
                return var
            # Resource physical ID
            if var in resources and "PhysicalResourceId" in resources[var]:
                return str(resources[var]["PhysicalResourceId"])
            return var

        return re.sub(r"\$\{([^}]+)\}", _sub_replace, str(template_str))

    # --- Fn::Select ---
    if "Fn::Select" in value:
        args = value["Fn::Select"]
        index = int(_resolve_refs(args[0], resources, params, conditions,
                                  mappings, stack_name, stack_id))
        items = _resolve_refs(args[1], resources, params, conditions,
                              mappings, stack_name, stack_id)
        if isinstance(items, str):
            items = [s.strip() for s in items.split(",")]
        if 0 <= index < len(items):
            return items[index]
        return ""

    # --- Fn::Split ---
    if "Fn::Split" in value:
        args = value["Fn::Split"]
        delimiter = args[0]
        source = _resolve_refs(args[1], resources, params, conditions,
                               mappings, stack_name, stack_id)
        return str(source).split(delimiter)

    # --- Fn::If ---
    if "Fn::If" in value:
        args = value["Fn::If"]
        cond_name = args[0]
        cond_val = conditions.get(cond_name, False)
        branch = args[1] if cond_val else args[2]
        result = _resolve_refs(branch, resources, params, conditions,
                               mappings, stack_name, stack_id)
        return result

    # --- Fn::Base64 ---
    if "Fn::Base64" in value:
        inner = _resolve_refs(value["Fn::Base64"], resources, params,
                              conditions, mappings, stack_name, stack_id)
        return base64.b64encode(str(inner).encode("utf-8")).decode("utf-8")

    # --- Fn::FindInMap ---
    if "Fn::FindInMap" in value:
        args = value["Fn::FindInMap"]
        map_name = _resolve_refs(args[0], resources, params, conditions,
                                 mappings, stack_name, stack_id)
        key1 = _resolve_refs(args[1], resources, params, conditions,
                             mappings, stack_name, stack_id)
        key2 = _resolve_refs(args[2], resources, params, conditions,
                             mappings, stack_name, stack_id)
        top = mappings.get(str(map_name), {})
        if str(key1) in top and str(key2) in top[str(key1)]:
            return top[str(key1)][str(key2)]
        # The optional fourth argument is {"DefaultValue": ...}; without it a
        # missing key is a template error, as on AWS (measured: "Template
        # error: Unable to get mapping for M::x::y").
        if len(args) > 3 and isinstance(args[3], dict) and "DefaultValue" in args[3]:
            return _resolve_refs(args[3]["DefaultValue"], resources, params,
                                 conditions, mappings, stack_name, stack_id)
        raise ValueError(
            f"Template error: Unable to get mapping for {map_name}::{key1}::{key2}")

    # --- Fn::ImportValue ---
    if "Fn::ImportValue" in value:
        from ministack.services.cloudformation import _exports
        export_name = _resolve_refs(value["Fn::ImportValue"], resources,
                                    params, conditions, mappings,
                                    stack_name, stack_id)
        export = _exports.get(str(export_name))
        if export:
            return export["Value"]
        raise ValueError(f"Export '{export_name}' not found")

    # --- Fn::GetStackOutput (aws-cdk-local cross-stack reference) ---
    # Not an AWS intrinsic: aws-cdk-local rewrites CDK cross-stack references
    # into this instead of Fn::ImportValue, and resolves it to a named output
    # of another already-deployed stack (deploy order is dependency-sorted, so
    # the producer stack's outputs are present by the time the consumer stack
    # resolves this). Left unresolved, the dict reaches provisioners as e.g. a
    # Lambda::Permission FunctionName and crashes with
    # "'dict' object has no attribute 'startswith'".
    if "Fn::GetStackOutput" in value:
        from ministack.services.cloudformation import _stacks
        spec = value["Fn::GetStackOutput"]
        if isinstance(spec, dict):
            target_stack = _resolve_refs(spec.get("StackName", ""), resources,
                                         params, conditions, mappings,
                                         stack_name, stack_id)
            output_name = _resolve_refs(spec.get("OutputName", ""), resources,
                                        params, conditions, mappings,
                                        stack_name, stack_id)
            stack = _stacks.get(str(target_stack))
            if stack:
                for o in stack.get("Outputs", []):
                    if o.get("OutputKey") == output_name:
                        return o.get("OutputValue", "")
            raise ValueError(
                f"Output '{output_name}' not found in stack '{target_stack}'")
        return ""

    # --- Fn::GetAZs ---
    if "Fn::GetAZs" in value:
        region = _resolve_refs(value["Fn::GetAZs"], resources, params,
                               conditions, mappings, stack_name, stack_id)
        if not region:
            region = get_region()
        # The zones the emulator's EC2 DescribeAvailabilityZones reports for
        # the stack's region. For any other region a real account answered an
        # empty list (measured), not a fabricated a/b/c.
        if str(region) != get_region():
            return []
        return [f"{region}a", f"{region}b", f"{region}c"]

    # --- Fn::Cidr ---
    if "Fn::Cidr" in value:
        args = value["Fn::Cidr"]
        ip_block = _resolve_refs(args[0], resources, params, conditions,
                                 mappings, stack_name, stack_id)
        count = int(_resolve_refs(args[1], resources, params, conditions,
                                  mappings, stack_name, stack_id))
        cidr_bits = int(_resolve_refs(args[2], resources, params, conditions,
                                      mappings, stack_name, stack_id))
        return _cidr_blocks(str(ip_block), count, cidr_bits)

    # --- Fn::Equals (condition-like in non-condition context) ---
    if "Fn::Equals" in value:
        args = value["Fn::Equals"]
        left = _resolve_refs(args[0], resources, params, conditions,
                             mappings, stack_name, stack_id)
        right = _resolve_refs(args[1], resources, params, conditions,
                              mappings, stack_name, stack_id)
        return str(left) == str(right)
    for fn in ("Fn::And", "Fn::Or", "Fn::Not"):
        if fn in value:
            # Condition functions belong in Conditions and Fn::If. In a value
            # position they evaluate to a boolean here, like Fn::Equals above,
            # instead of leaking the unresolved call through to the service.
            # What AWS does with one in a property was not measured; as an
            # Output value it is refused up front (see _validate_template_statics).
            operands = [
                _resolve_refs(arg, resources, params, conditions, mappings,
                              stack_name, stack_id)
                for arg in value[fn]
            ]
            if fn == "Fn::And":
                return all(bool(o) for o in operands)
            if fn == "Fn::Or":
                return any(bool(o) for o in operands)
            return not bool(operands[0]) if operands else True

    # --- Condition (reference) ---
    if "Condition" in value and len(value) == 1:
        return conditions.get(value["Condition"], False)

    # Recurse into plain dicts
    result = {}
    for k, v in value.items():
        resolved = _resolve_refs(v, resources, params, conditions,
                                 mappings, stack_name, stack_id)
        if resolved is not _NO_VALUE:
            result[k] = resolved
    return result


# ===========================================================================
# Dependency Extractor + Topological Sort
# ===========================================================================

def _extract_deps(resource_def: dict, all_resource_names: set) -> set:
    """Walk a resource definition and extract dependency logical IDs."""
    deps = set()

    def _walk(obj):
        if isinstance(obj, dict):
            if "Ref" in obj:
                ref = obj["Ref"]
                if ref in all_resource_names:
                    deps.add(ref)
            if "Fn::GetAtt" in obj:
                args = obj["Fn::GetAtt"]
                if isinstance(args, list) and args:
                    if args[0] in all_resource_names:
                        deps.add(args[0])
                elif isinstance(args, str):
                    logical = args.split(".")[0]
                    if logical in all_resource_names:
                        deps.add(logical)
            if "Fn::Sub" in obj:
                sub_val = obj["Fn::Sub"]
                template_str = sub_val[0] if isinstance(sub_val, list) else sub_val
                for match in re.finditer(r"\$\{([^}]+)\}", str(template_str)):
                    var = match.group(1)
                    if var.startswith("!"):
                        continue
                    base = var.split(".")[0]
                    if base in all_resource_names:
                        deps.add(base)
            # Walk ALL branches of Fn::If
            if "Fn::If" in obj:
                args = obj["Fn::If"]
                for branch in args[1:]:
                    _walk(branch)
            for k, v in obj.items():
                if k not in ("Ref", "Fn::GetAtt", "Fn::Sub", "Fn::If"):
                    _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    # DependsOn
    depends_on = resource_def.get("DependsOn", [])
    if isinstance(depends_on, str):
        depends_on = [depends_on]
    for d in depends_on:
        if d in all_resource_names:
            deps.add(d)

    # Walk Properties
    _walk(resource_def.get("Properties", {}))

    return deps


def _topological_sort(resources: dict, conditions: dict) -> list:
    """Kahn's algorithm for topological sort of resources."""
    all_names = set(resources.keys())
    # Filter out resources whose condition evaluates to false
    active = set()
    for name, defn in resources.items():
        cond = defn.get("Condition")
        if cond and not conditions.get(cond, True):
            continue
        active.add(name)

    in_degree = {name: 0 for name in active}
    adj: dict[str, list[str]] = {name: [] for name in active}

    for name in active:
        deps = _extract_deps(resources[name], active)
        for dep in deps:
            if dep in active and dep != name:
                adj[dep].append(name)
                in_degree[name] += 1

    queue = sorted(n for n in active if in_degree[n] == 0)
    heapq.heapify(queue)
    result = []

    while queue:
        node = heapq.heappop(queue)
        result.append(node)
        for neighbor in adj[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                heapq.heappush(queue, neighbor)

    if len(result) != len(active):
        remaining = active - set(result)
        raise ValueError(
            f"Circular dependency detected among resources: {', '.join(sorted(remaining))}"
        )

    return result


# ===========================================================================
# SAM Transform
# ===========================================================================

_SAM_TRANSFORM = "AWS::Serverless-2016-10-31"

# Supress benign errors from samtranslator
logging.getLogger("samtranslator.feature_toggle.feature_toggle").setLevel(logging.ERROR)

_NO_IAM_POLICY_LOADER = SimpleNamespace(load=lambda: {})

def _apply_sam_transform_if_applicable(template: dict) -> dict:
    declared = template.get("Transform")
    transforms = declared if isinstance(declared, list) else [declared]
    if _SAM_TRANSFORM not in transforms:
        return template

    try:
        from samtranslator.translator.transform import transform as _sam_transform
    except ImportError as e:
        raise ValueError(
            "Template uses the AWS::Serverless-2016-10-31 transform, but the SAM "
            "transform was not applied because the optional 'aws-sam-translator' "
            "package is not installed. Either run ministack's 'full' image (or "
            "`pip install ministack[full]`) to enable SAM support, or expand the "
            "template to native CloudFormation first (e.g. `sam build` / "
            "`aws cloudformation package`). See https://ministack.org/docs/iac#sam"
        ) from e

    prev_region = os.environ.get("AWS_DEFAULT_REGION")
    os.environ["AWS_DEFAULT_REGION"] = get_region()
    try:
        return _sam_transform(copy.deepcopy(template), {}, _NO_IAM_POLICY_LOADER)
    finally:
        if prev_region is None:
            os.environ.pop("AWS_DEFAULT_REGION", None)
        else:
            os.environ["AWS_DEFAULT_REGION"] = prev_region
