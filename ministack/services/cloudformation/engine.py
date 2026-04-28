"""
CloudFormation engine — pure functions for template parsing, parameter resolution,
condition evaluation, intrinsic function resolution, and topological sorting.
"""

import base64
import heapq
import json
import os
import re
from collections import defaultdict

import yaml

from ministack.core.responses import get_account_id, get_region, new_uuid

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


def _resolve_parameters(template: dict, provided_params: list[dict]) -> dict:
    """Resolve template parameters with provided values and defaults.

    Returns dict of param_name -> {Value, NoEcho}.
    """
    param_defs = template.get("Parameters", {})
    provided_map = {p["Key"]: p["Value"] for p in provided_params if "Key" in p}
    resolved = {}

    for name, defn in param_defs.items():
        ptype = defn.get("Type", "String")
        no_echo = str(defn.get("NoEcho", "false")).lower() == "true"

        if name in provided_map:
            value = provided_map[name]
        elif "Default" in defn:
            value = defn["Default"]
        else:
            raise ValueError(f"Parameter '{name}' has no Default and was not provided")

        value = str(value) if value is not None else ""

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
        elif ptype == "CommaDelimitedList":
            # Keep as string; Fn::Select will split
            pass
        # AWS-specific types treated as String -- no extra validation

        resolved[name] = {"Value": value, "NoEcho": no_echo}

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
# Intrinsic Function Resolver
# ===========================================================================

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
        # Fallback: try PhysicalResourceId
        return res.get("PhysicalResourceId", "")

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
                return str(res.get("PhysicalResourceId", var))
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
        return mappings.get(str(map_name), {}).get(str(key1), {}).get(str(key2), "")

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

    # --- Fn::GetAZs ---
    if "Fn::GetAZs" in value:
        region = _resolve_refs(value["Fn::GetAZs"], resources, params,
                               conditions, mappings, stack_name, stack_id)
        if not region:
            region = get_region()
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
        # Simplified CIDR generation
        return [f"10.0.{i}.0/{32 - cidr_bits}" for i in range(count)]

    # --- Fn::Equals (condition-like in non-condition context) ---
    if "Fn::Equals" in value:
        args = value["Fn::Equals"]
        left = _resolve_refs(args[0], resources, params, conditions,
                             mappings, stack_name, stack_id)
        right = _resolve_refs(args[1], resources, params, conditions,
                              mappings, stack_name, stack_id)
        return str(left) == str(right)

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
