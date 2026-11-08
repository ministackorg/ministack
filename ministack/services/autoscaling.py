# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
AutoScaling Service Emulator.
Query API (Action=...) — groups, launch configs, policies, hooks, scheduled actions.
All in-memory, no actual instance scaling.

Supports:
  ASG:       CreateAutoScalingGroup, DescribeAutoScalingGroups, UpdateAutoScalingGroup,
             DeleteAutoScalingGroup, SetDesiredCapacity, DescribeAutoScalingInstances,
             DescribeScalingActivities
  Refresh:   StartInstanceRefresh, DescribeInstanceRefreshes, CancelInstanceRefresh
  LC:        CreateLaunchConfiguration, DescribeLaunchConfigurations, DeleteLaunchConfiguration
  Policies:  PutScalingPolicy, DescribePolicies, DeletePolicy
  Hooks:     PutLifecycleHook, DescribeLifecycleHooks, DeleteLifecycleHook,
             CompleteLifecycleAction, RecordLifecycleActionHeartbeat
  Schedule:  PutScheduledUpdateGroupAction, DescribeScheduledActions, DeleteScheduledAction
  Tags:      CreateOrUpdateTags, DescribeTags, DeleteTags
"""

import copy
import logging
import os
import re
import time
from decimal import Decimal
from xml.sax.saxutils import escape

from ministack.core.responses import (
    DECIMAL,
    INTEGER,
    AccountRegionScopedDict,
    AccountScopedDict,
    get_account_id,
    get_region,
    member_index,
    member_numbering_error,
    new_uuid,
    now_iso,
)
from ministack.core.responses import query_value as _query_value

logger = logging.getLogger("autoscaling")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_asgs = AccountRegionScopedDict()
_launch_configs = AccountRegionScopedDict()
_policies = AccountRegionScopedDict()
_hooks = AccountRegionScopedDict()
_scheduled_actions = AccountRegionScopedDict()
_tags = AccountRegionScopedDict()  # asg_name -> [{"Key":..., "Value":...}, ...]


def _clear_state():
    _asgs.clear()
    _launch_configs.clear()
    _policies.clear()
    _hooks.clear()
    _scheduled_actions.clear()
    _tags.clear()


def get_state():
    return {
        "asgs": copy.deepcopy(_asgs),
        "launch_configs": copy.deepcopy(_launch_configs),
        "policies": copy.deepcopy(_policies),
        "hooks": copy.deepcopy(_hooks),
        "scheduled_actions": copy.deepcopy(_scheduled_actions),
        "tags": copy.deepcopy(_tags),
    }


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not data:
        return

    _clear_state()
    _asgs.update(data.get("asgs", {}))
    _launch_configs.update(data.get("launch_configs", {}))
    # v1.5.38 saved DesiredCapacityType "units" on every group. A group without a MixedInstancesPolicy never
    # has a type on AWS (captured, us-east-2, 2026-11-05), so restored state drops it there.
    for asg in _asgs.all_values():
        if not asg.get("MixedInstancesPolicy"):
            asg["DesiredCapacityType"] = None

    asg_regions = {
        (account_id, asg_name): region
        for (account_id, region, asg_name), _asg in _asgs.all_items()
    }
    for store, key in (
        (_policies, "policies"),
        (_hooks, "hooks"),
        (_scheduled_actions, "scheduled_actions"),
        (_tags, "tags"),
    ):
        _restore_asg_child_store(store, data.get(key, {}), asg_regions)


def _restore_asg_child_store(store, restored, asg_regions):
    """Adopt legacy name-keyed child state into its parent ASG's region."""
    if isinstance(restored, AccountRegionScopedDict):
        store.update(restored)
        return

    if isinstance(restored, AccountScopedDict):
        items = restored._data.items()
    else:
        account_id = get_account_id()
        items = (((account_id, key), value) for key, value in restored.items())

    for (account_id, key), value in items:
        asg_name = value.get("AutoScalingGroupName") if isinstance(value, dict) else key
        region = asg_regions.get(
            (account_id, asg_name),
            store._region_for_legacy_value(key, value),
        )
        store.set_scoped(account_id, region, key, value)




def reset():
    _clear_state()


def _p(params, key):
    v = params.get(key, "")
    return v[0] if isinstance(v, list) else v


def _parse_member_list(params, prefix):
    items = []
    i = 1
    while True:
        key = f"{prefix}.member.{i}"
        val = _p(params, key)
        if not val:
            break
        items.append(val)
        i += 1
    return items


def _parse_filters(params):
    """Filters.member.N.Name / .Values.member.M as [(name, [values])]."""
    filters = []
    i = 1
    while _p(params, f"Filters.member.{i}.Name"):
        filters.append((_p(params, f"Filters.member.{i}.Name"),
                        _parse_member_list(params, f"Filters.member.{i}.Values")))
        i += 1
    return filters


def _group_matches_filters(asg, filters):
    """DescribeAutoScalingGroups tag filters: tag-key, tag-value, tag:<key>; values OR'd, filters AND'd."""
    tags = asg.get("Tags", [])
    for name, values in filters:
        if name == "tag-key":
            ok = any(t["Key"] in values for t in tags)
        elif name == "tag-value":
            ok = any(t["Value"] in values for t in tags)
        elif name.startswith("tag:"):
            ok = any(t["Key"] == name[4:] and t["Value"] in values for t in tags)
        else:
            ok = True
        if not ok:
            return False
    return True


_TAG_FILTER_FIELDS = {"auto-scaling-group": "ResourceId", "key": "Key", "value": "Value"}


def _tag_matches_filters(tag, filters):
    """DescribeTags filters: auto-scaling-group, key, value, propagate-at-launch."""
    for name, values in filters:
        if name in _TAG_FILTER_FIELDS:
            ok = tag[_TAG_FILTER_FIELDS[name]] in values
        elif name == "propagate-at-launch":
            ok = ("true" if tag.get("PropagateAtLaunch") else "false") in [v.lower() for v in values]
        else:
            ok = True
        if not ok:
            return False
    return True


def _page(p, items, default=50, high=100):
    """MaxRecords/NextToken offset paging; returns (page, NextToken XML or "")."""
    raw = _p(p, "MaxRecords")
    size = int(raw) if re.fullmatch(r"-?[0-9]+", raw or "") else default
    token = _p(p, "NextToken")
    start = int(token) if token.isdigit() else 0
    end = start + min(max(size, 1), high)
    return items[start:end], (f"<NextToken>{end}</NextToken>" if end < len(items) else "")


def _xml(status, root_tag, inner):
    body = (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<{root_tag} xmlns="http://autoscaling.amazonaws.com/doc/2011-01-01/">'
            f'{inner}'
            f'<ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>'
            f'</{root_tag}>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status=400):
    # Query error envelope: <Type> in <Error>, no <?xml?> (AWS captures).
    fault_type = "Sender" if status < 500 else "Receiver"
    # An empty message has no <Message> element (AWS MalformedInput for a non-numeric value).
    message_xml = f"<Message>{escape(message)}</Message>" if message else ""
    body = (f'<ErrorResponse xmlns="http://autoscaling.amazonaws.com/doc/2011-01-01/">'
            f'<Error><Type>{fault_type}</Type><Code>{code}</Code>{message_xml}</Error>'
            f'<RequestId>{new_uuid()}</RequestId>'
            f'</ErrorResponse>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _asg_arn(name):
    return f"arn:aws:autoscaling:{get_region()}:{get_account_id()}:autoScalingGroup:{new_uuid()}:autoScalingGroupName/{name}"


# ---------------------------------------------------------------------------
# AutoScalingGroup
#
# No real instances run, but terraform-provider-aws's aws_autoscaling_group
# capacity waiter polls DescribeAutoScalingGroups until DesiredCapacity
# instances report InService/Healthy. A group that reports zero forever blocks
# every apply for the full wait_for_capacity_timeout (10m) and then fails, so we
# materialize DesiredCapacity mock instances the same way a real ASG launches
# them. They ride the group record, inheriting the existing persistence / reset
# plumbing (like InstanceRefreshes).
# ---------------------------------------------------------------------------

def _reconcile_instances(asg):
    """Resize the group's Instances list to exactly DesiredCapacity entries.

    Scale-up appends InService/Healthy instances round-robined across the
    group's AZs; scale-down removes from the end, matching a Default
    termination policy.
    """
    desired = asg["DesiredCapacity"]
    instances = asg["Instances"]
    if len(instances) > desired:
        del instances[desired:]
        return
    azs = asg["AvailabilityZones"] or [f"{get_region()}a"]
    launch_template = (asg.get("LaunchTemplate")
                       or ((asg.get("MixedInstancesPolicy") or {}).get("LaunchTemplate") or {}).get(
                           "LaunchTemplateSpecification") or {})
    launch_config = asg.get("LaunchConfigurationName", "")
    while len(instances) < desired:
        instances.append({
            "InstanceId": "i-" + new_uuid().replace("-", "")[:17],
            "LifecycleState": "InService",
            "HealthStatus": "Healthy",
            "AvailabilityZone": azs[len(instances) % len(azs)],
            # Real ASGs stamp new instances with the group's scale-in setting.
            "ProtectedFromScaleIn": asg["NewInstancesProtectedFromScaleIn"],
            "LaunchTemplate": launch_template,
            "LaunchConfigurationName": launch_config,
        })


def _missing_template_id_message(lt_id):
    """AWS (captured) calls an unknown id malformed unless it is lt-<lowercase hex> shorter than 17 digits,
    or one that exists in another region of the account, which are missing."""
    from ministack.services import ec2

    if re.fullmatch(r"lt-[0-9a-f]+", lt_id):
        elsewhere = any(key[0] == get_account_id() and rec.get("LaunchTemplateId") == lt_id
                        for key, rec in ec2._launch_templates.all_items())
        if len(lt_id) - 3 < 17 or elsewhere:
            return f"The specified launch template, with template ID {lt_id}, does not exist."
    return f"Malformed launch template Id: {lt_id}"


def resolve_launch_template(lt_id, lt_name, version=None):
    """(spec, error message): the group's LaunchTemplate with the EC2 template's id and name, and the version
    as given (absent when not given), or AWS's ValidationError text (captured, us-east-1, 2026-10-28/29)."""
    if not (lt_id or lt_name or version):
        return {}, None
    if bool(lt_id) == bool(lt_name):
        return None, "Valid requests must contain either launchTemplateId or LaunchTemplateName"
    if version and version not in ("$Default", "$Latest") and not version.isdigit():
        return None, ("Invalid launch template version: either '$Default', '$Latest', or a numeric version "
                      "are allowed.")
    from ministack.services import ec2

    if lt_id:
        record = ec2._launch_templates.get(lt_id)
        if not record:
            return None, _missing_template_id_message(lt_id)
    else:
        record = next((t for t in ec2._launch_templates.values() if t.get("LaunchTemplateName") == lt_name), None)
        if not record:
            return None, f"The specified launch template, with template name {lt_name}, does not exist."
    if version and version not in ("$Default", "$Latest") and not any(
            str(v.get("VersionNumber")) == version for v in record.get("Versions", [])):
        return None, (f"Could not find the specified version {version} for the launch template with ID "
                      f"{record['LaunchTemplateId']}.")
    spec = {"LaunchTemplateId": record["LaunchTemplateId"], "LaunchTemplateName": record["LaunchTemplateName"]}
    if version:
        spec["Version"] = version
    return spec, None


def _launch_template_spec(p):
    return resolve_launch_template(_p(p, "LaunchTemplate.LaunchTemplateId"),
                                   _p(p, "LaunchTemplate.LaunchTemplateName"),
                                   _p(p, "LaunchTemplate.Version"))


_SOURCE_ERROR = ("Valid requests must contain either LaunchTemplate, LaunchConfigurationName, InstanceId or "
                 "MixedInstancesPolicy parameter.")
# UpdateAutoScalingGroup words it without InstanceId (captured, us-east-2, 2026-11-05).
_UPDATE_SOURCE_ERROR = ("Valid requests must contain either LaunchTemplate, LaunchConfigurationName or "
                        "MixedInstancesPolicy parameter.")
_INSTANCES_DISTRIBUTION_DEFAULTS = {
    "OnDemandAllocationStrategy": "prioritized", "OnDemandBaseCapacity": 0,
    "OnDemandPercentageAboveBaseCapacity": 100, "SpotAllocationStrategy": "lowest-price",
}


def _model_shape(name):
    import botocore.session

    global _MODEL
    if "_MODEL" not in globals():
        _MODEL = botocore.session.get_session().get_service_model("autoscaling")
    return _MODEL.shape_for(name)


def _shape_xml(value, shape):
    if shape.type_name == "structure":
        return "".join(f"<{k}>{_shape_xml(value[k], shape.members[k])}</{k}>" for k in shape.members if k in value)
    if shape.type_name == "list":
        return "".join(f"<member>{_shape_xml(v, shape.member)}</member>" for v in value)
    if shape.type_name == "boolean":
        return "true" if value else "false"
    return escape(str(value))


_SPOT_STRATEGIES = ("lowest-price", "capacity-optimized", "capacity-optimized-prioritized", "price-capacity-optimized")


def resolve_mixed_instances_policy(policy, stored=None):
    """(policy, error message): the policy as AWS stores it, its launch templates resolved like a group's.
    On an update, *stored* is the group's policy and the members not sent are kept from it."""
    stored = stored or {}
    sent_lt = policy.get("LaunchTemplate") or {}
    lt = {**(stored.get("LaunchTemplate") or {}), **sent_lt}
    if "LaunchTemplateSpecification" in sent_lt or not stored:
        spec = sent_lt.get("LaunchTemplateSpecification") or {}
        resolved, error = resolve_launch_template(spec.get("LaunchTemplateId"), spec.get("LaunchTemplateName"),
                                                  spec.get("Version"))
        if error:
            return None, error
        lt["LaunchTemplateSpecification"] = resolved
    if "Overrides" in sent_lt or not stored:
        overrides = []
        for override in sent_lt.get("Overrides") or []:
            override = dict(override)
            if override.get("LaunchTemplateSpecification"):
                o_spec = override["LaunchTemplateSpecification"]
                o_resolved, error = resolve_launch_template(o_spec.get("LaunchTemplateId"),
                                                            o_spec.get("LaunchTemplateName"), o_spec.get("Version"))
                if error:
                    return None, error
                override["LaunchTemplateSpecification"] = o_resolved
            overrides.append(override)
        lt["Overrides"] = overrides
    sent = policy.get("InstancesDistribution") or {}
    strategy = sent.get("SpotAllocationStrategy")
    if strategy is not None and strategy not in _SPOT_STRATEGIES:
        return None, f"SpotAllocationStrategy is not valid. Valid options are: [{', '.join(_SPOT_STRATEGIES)}]."
    distribution = {**_INSTANCES_DISTRIBUTION_DEFAULTS, **(stored.get("InstancesDistribution") or {}), **sent}
    if distribution.get("SpotMaxPrice") == "":
        del distribution["SpotMaxPrice"]
    if distribution["SpotAllocationStrategy"] != "lowest-price":
        if "SpotInstancePools" in sent:
            return None, "SpotInstancePools option is only available with the lowest-price allocation strategy."
        distribution.pop("SpotInstancePools", None)
    else:
        distribution.setdefault("SpotInstancePools", 2)
    return {"LaunchTemplate": lt, "InstancesDistribution": distribution}, None


def _mixed_instances_policy(p, stored=None):
    policy = _query_value(p, "MixedInstancesPolicy", _model_shape("MixedInstancesPolicy"))
    if not policy:
        return None, None
    if not stored and not policy.get("LaunchTemplate"):
        return None, ("A launch template is required when configuring an Auto Scaling group with a mixed instances "
                      "policy. Add a launch template to the mixed instances policy and try again.")
    if not stored and not policy["LaunchTemplate"].get("LaunchTemplateSpecification"):
        return None, "Valid requests must contain either launchTemplateId or LaunchTemplateName"
    return resolve_mixed_instances_policy(policy, stored)


def _has_member(p, name):
    return any(k == name or k.startswith(name + ".") for k in p)


def _input_members(p, operation, members):
    """The *members* sent to *operation*, typed by its botocore input shape."""
    _model_shape("AutoScalingGroup")
    shape = _MODEL.operation_model(operation).input_shape
    out = {}
    for member in members:
        value = _query_value(p, member, shape.members[member])
        if value is not None:
            out[member] = value
    return out


_GROUP_MEMBERS = (
    "LoadBalancerNames", "TargetGroupARNs", "PlacementGroup", "ServiceLinkedRoleARN", "MaxInstanceLifetime",
    "CapacityRebalance", "Context", "DesiredCapacityType", "DefaultInstanceWarmup", "TrafficSources",
    "InstanceMaintenancePolicy", "DeletionProtection", "AvailabilityZoneDistribution",
    "AvailabilityZoneImpairmentPolicy", "CapacityReservationSpecification", "InstanceLifecyclePolicy",
    "AvailabilityZoneIds",
)
_GROUP_UPDATE_MEMBERS = tuple(m for m in _GROUP_MEMBERS if m not in ("LoadBalancerNames", "TargetGroupARNs",
                                                                      "TrafficSources", "DesiredCapacityType"))


def _group_defaults():
    return {
        "ServiceLinkedRoleARN": (f"arn:aws:iam::{get_account_id()}:role/aws-service-role/"
                                 "autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling"),
        "DeletionProtection": "none",
        "AvailabilityZoneDistribution": {"CapacityDistributionStrategy": "balanced-best-effort"},
    }


def _group_members_xml(asg):
    shape = _model_shape("AutoScalingGroup")
    return "".join(f"<{m}>{_shape_xml(asg[m], shape.members[m])}</{m}>"
                   for m in _GROUP_MEMBERS if asg.get(m) not in (None, ""))


_DESIRED_CAPACITY_TYPES = ("vcpu", "memory-mib", "units")
_REQUIREMENT_CAPACITY_TYPES = ("vcpu", "memory-mib")    # need InstanceRequirements


def _length_constraint(p, member, wire_name):
    value = _p(p, member)
    if value == "":
        return f"Value '' at '{wire_name}' failed to satisfy constraint: Member must have length greater than or equal to 1"
    if len(value) > 255:
        return (f"Value '{value}' at '{wire_name}' failed to satisfy constraint: "
                "Member must have length less than or equal to 255")
    return None


def _constraint_error(p):
    """AWS's request constraint errors for DesiredCapacityType and AutoScalingGroupName (missing name, empty,
    longer than 255), reported together and before the launch source checks (captured, us-east-2, 2026-11-05)."""
    errors = []
    if _has_member(p, "DesiredCapacityType"):
        errors.append(_length_constraint(p, "DesiredCapacityType", "desiredCapacityType"))
    if not _has_member(p, "AutoScalingGroupName"):
        errors.append("Value null at 'autoScalingGroupName' failed to satisfy constraint: Member must not be null")
    else:
        errors.append(_length_constraint(p, "AutoScalingGroupName", "autoScalingGroupName"))
    errors = [e for e in errors if e]
    if not errors:
        return None
    return f"{len(errors)} validation error{'s' if len(errors) > 1 else ''} detected: {'; '.join(errors)}"


def _capacity_type_value_error(p):
    value = _p(p, "DesiredCapacityType")
    if value and value not in _DESIRED_CAPACITY_TYPES:
        return (f"Invalid DesiredCapacityType value '{value}', it should be one of "
                f"{', '.join(_DESIRED_CAPACITY_TYPES)}")
    return None


# DesiredCapacityType as AWS handles it (captured, us-east-2, 2026-11-05): only a group with a
# MixedInstancesPolicy has one; "units" on an update takes effect only with a MixedInstancesPolicy; vcpu and
# memory-mib need InstanceRequirements, in the policy's overrides or, without overrides, its launch template.
def _template_has_requirements(spec):
    from ministack.services import ec2

    record = ec2._launch_templates.get(spec.get("LaunchTemplateId") or "")
    if not record:
        return False
    version = spec.get("Version") or "$Default"
    number = {"$Default": record.get("DefaultVersionNumber"), "$Latest": record.get("LatestVersionNumber")}.get(
        version, version)
    data = next((v.get("LaunchTemplateData") or {} for v in record.get("Versions", [])
                 if str(v.get("VersionNumber")) == str(number)), {})
    return bool(data.get("InstanceRequirements"))


def _has_instance_requirements(mixed):
    launch_template = (mixed or {}).get("LaunchTemplate") or {}
    overrides = launch_template.get("Overrides") or []
    if overrides:
        return any(o.get("InstanceRequirements") for o in overrides)
    return _template_has_requirements(launch_template.get("LaunchTemplateSpecification") or {})


def _capacity_type_requirements_error(capacity_type, mixed, operation):
    if capacity_type not in _REQUIREMENT_CAPACITY_TYPES or _has_instance_requirements(mixed):
        return None
    if operation == "create":
        return (f"The DesiredCapacityType {capacity_type} is only allowed in mixed instances groups containing "
                "InstanceRequirements. Please include a MixedInstancesPolicy with InstanceRequirements for your "
                "group and try again.")
    return (f"The DesiredCapacityType {capacity_type} is only allowed in mixed instances groups using instance "
            "requirements. Please include instance requirements in the launch template or launch template "
            "overrides and try again.")


def _launch_template_xml(lt):
    fields = "".join(f"<{k}>{escape(str(lt[k]))}</{k}>" for k in ("LaunchTemplateId", "LaunchTemplateName", "Version") if lt.get(k))
    return f"<LaunchTemplate>{fields}</LaunchTemplate>"


def _instance_member_xml(inst, include_group_name=""):
    """Render one instance. DescribeAutoScalingInstances also carries the
    owning AutoScalingGroupName, so callers pass it when needed."""
    launch_template = inst.get("LaunchTemplate") or {}
    lt_xml = ""
    if launch_template:
        lt_xml = _launch_template_xml(launch_template)
    launch_config = inst.get("LaunchConfigurationName", "")
    lc_xml = f"<LaunchConfigurationName>{escape(launch_config)}</LaunchConfigurationName>" if launch_config else ""
    group_xml = f"<AutoScalingGroupName>{escape(include_group_name)}</AutoScalingGroupName>" if include_group_name else ""
    return (f"<member>"
            f"<InstanceId>{inst['InstanceId']}</InstanceId>"
            f"{group_xml}"
            f"<AvailabilityZone>{escape(inst['AvailabilityZone'])}</AvailabilityZone>"
            f"<LifecycleState>{inst['LifecycleState']}</LifecycleState>"
            f"<HealthStatus>{inst['HealthStatus']}</HealthStatus>"
            f"<ProtectedFromScaleIn>{'true' if inst['ProtectedFromScaleIn'] else 'false'}</ProtectedFromScaleIn>"
            f"{lt_xml}{lc_xml}"
            f"</member>")


def _instances_xml(asg):
    if not asg["Instances"]:
        return "<Instances/>"
    return "<Instances>" + "".join(_instance_member_xml(i) for i in asg["Instances"]) + "</Instances>"


_OVERRIDES_PREFIX = "MixedInstancesPolicy.LaunchTemplate.Overrides.member."
_OVERRIDE_SHAPE_ERROR = "A launch template override must contain one of InstanceType or InstanceRequirements"
_OVERRIDE_MIX_ERROR = ("You cannot mix single instance and instance requirements launch template override in the "
                       "same mixed instance policy. Remove the launch template overrides that use single instance "
                       "types, or the ones using instance requirements.")
_OVERRIDE_BOTH_ERROR = ("The parameter InstanceType cannot be used in a launch template override using instance "
                        "requirements.")
_OVERRIDE_WEIGHT_ERROR = ("Not all instance types have a defined WeightedCapacity. Specify missing weights, or "
                          "remove all weights, and try again.")
_LTNC_ERROR = ("The launch template is not compatible with the configuration of the Auto Scaling group. This launch "
               "template can only be used if MixedInstancesPolicy is set on the Auto Scaling group. Specify the "
               "MixedInstancesPolicy parameter or use a launch template that contains the InstanceType parameter "
               "instead of the InstanceRequirements parameter, and try again.")


def _duplicate_type_error(types):
    """AWS names every InstanceType sent more than once, in the order each is first detected as a repeat, that is
    at its second occurrence (captured, us-east-2, 2026-11-06: edges3 d-c-abba, d-c-baab, d-c-abccb)."""
    seen, duplicated = set(), []
    for t in types:
        if t in seen and t not in duplicated:
            duplicated.append(t)
        seen.add(t)
    if not duplicated:
        return None
    return ("Cannot add same instance type override more than once. Remove these duplicates from the request "
            f"and try again: [{', '.join(duplicated)}]")


_REQUIREMENT_NUMBERS = ("VCpuCount.Min", "VCpuCount.Max", "MemoryMiB.Min", "MemoryMiB.Max")


def _scan_overrides(p):
    """The override entries sent, from the numbering alone: ({index: {member names}}, {index: InstanceType},
    modelled). Nothing proportional to the highest index is allocated. *modelled* is False for input whose
    AWS answer is a field-level or wire-format error that is not reproduced (a scalar where a structure belongs,
    an empty InstanceType, InstanceRequirements without MemoryMiB or VCpuCount, non-numeric requirement values);
    those requests get no override checks, as before the checks existed."""
    members, types, requirement_parts, modelled, illegal = {}, {}, {}, True, None
    for key, value in p.items():
        if not key.startswith(_OVERRIDES_PREFIX):
            continue
        index, _, rest = key[len(_OVERRIDES_PREFIX):].partition(".")
        if not index.isdigit():
            continue
        value = _p(p, key)      # a repeated key counts once, as _query_value reads it
        number = member_index(index)
        if number is None:
            illegal = illegal or index
            continue
        parts = rest.split(".")
        name = parts[0]
        members.setdefault(number, set()).add(name)
        if (not rest and value) or (name in ("InstanceRequirements", "LaunchTemplateSpecification") and len(parts) == 1):
            modelled = False
        elif name == "" and not value:      # a blank member.N= is an entry with no member (edges3 e-u-wire-member1-blank)
            pass
        elif name == "WeightedCapacity" and not value:     # blank: a field-length error first (edges3 b-c-blank-wc-*)
            modelled = False
        elif name == "InstanceType":
            types[number] = value
            modelled = modelled and bool(value)
        elif name == "InstanceRequirements":
            requirement_parts.setdefault(number, set()).add(parts[1])
            if ".".join(parts[1:]) in _REQUIREMENT_NUMBERS and not str(value).isdigit():
                modelled = False
    for number, present in requirement_parts.items():
        if not {"VCpuCount", "MemoryMiB"} <= present:
            modelled = False
    return members, types, modelled, illegal


_INTEGER_REQUIREMENTS = ("VCpuCount", "MemoryMiB", "NetworkInterfaceCount", "BaselineEbsBandwidthMbps",
                         "AcceleratorCount", "AcceleratorTotalMemoryMiB")
_DECIMAL_REQUIREMENTS = ("MemoryGiBPerVCpu", "TotalLocalStorageGB", "NetworkBandwidthGbps")
_REQUIREMENT_KEY = re.compile(r"\.InstanceRequirements\.(\w+)\.(Min|Max)$")


def _malformed_number_error(p):
    """The message of the MalformedInput AWS answers for a non-numeric wire value, or None. It covers the group size
    parameters and every numeric InstanceRequirements field, and comes right after the index errors, before every
    other check (captured, us-east-2, 2026-11-06: edges7 b7-*, g7-*; the order against the index errors is not
    captured). The message is empty, except for a blank VCpuCount.Min (b7-create-vcpumin-blank)."""
    for key, value in p.items():
        value = _p(p, key)
        if key in ("MinSize", "MaxSize", "DesiredCapacity"):
            if value != "" and not INTEGER.fullmatch(value):
                return ""
        elif match := _REQUIREMENT_KEY.search(key):
            if value == "" and key.endswith(".VCpuCount.Min"):
                return "missing value for decimal type"
            if match[1] in _INTEGER_REQUIREMENTS and value != "" and not INTEGER.fullmatch(value):
                return ""
            if match[1] in _DECIMAL_REQUIREMENTS and value != "" and not DECIMAL.fullmatch(value):
                return ""
    return None


_OVERRIDE_COUNT_ERROR = "The number of LaunchTemplateOverrides must be fewer than 40."
_UPDATE_OVERRIDE_COUNT_ERROR = "The number of LaunchTemplateOverrides must be between 1 and 40."


def _override_shape_error(p, scan, count_error=None):
    """(code, message) for a bad override list (captured, us-east-2, 2026-11-05/06). Order: a duplicated InstanceType; partial WeightedCapacity; then the entries in index
    order, the first wrong one decides (neither InstanceType nor InstanceRequirements, or both); then InstanceType
    entries mixed with InstanceRequirements ones. A gap in the numbering is an empty override; the shared
    _query_value parser is left alone."""
    members, types, modelled = scan[:3]
    if not members or not modelled:
        return None
    # More than 40 overrides: AWS counts the list size, the highest index, not the entries present (edges7 a7-*:
    # {1,11,21,31,41} and 1..40 plus 50 are refused; 40 are accepted). On create (edges5 i-c-41-*, edges4 p-c-dense-41, edges6 c6-41-*) it
    # comes after the source and DesiredCapacityType errors; on update (edges6 u6-41-*) after the index, name
    # constraint and source errors and before the DesiredCapacityType enum. Either way before the duplicate,
    # neither, mix and weight errors and before every lookup.
    if count_error and max(members) > 40:
        return "ValidationError", count_error
    if duplicate_error := _duplicate_type_error([types[i] for i in sorted(types)]):
        return "ValidationError", duplicate_error
    entries = [members.get(index, set()) for index in range(1, max(members) + 1)]
    weighted = sum("WeightedCapacity" in present for present in entries)
    if 0 < weighted < len(entries):
        return "ValidationError", _OVERRIDE_WEIGHT_ERROR
    for present in entries:
        has_type, has_requirements = "InstanceType" in present, "InstanceRequirements" in present
        if not (has_type or has_requirements):
            return "ValidationError", _OVERRIDE_SHAPE_ERROR
        if has_type and has_requirements:
            return "ValidationError", _OVERRIDE_BOTH_ERROR
    if any("InstanceType" in present for present in entries) and any(
            "InstanceRequirements" in present for present in entries):
        return "ValidationError", _OVERRIDE_MIX_ERROR
    return None


def _create_asg(p):
    name = _p(p, "AutoScalingGroupName")
    scan = _scan_overrides(p)
    if numbering_error := member_numbering_error(scan[0], scan[3]):
        return _error(*numbering_error)
    if (malformed := _malformed_number_error(p)) is not None:
        return _error("MalformedInput", malformed)
    if constraint_error := _constraint_error(p):
        return _error("ValidationError", constraint_error)
    sources = [_has_member(p, m) for m in ("LaunchTemplate", "MixedInstancesPolicy")] + [
        bool(_p(p, "LaunchConfigurationName"))]
    if sum(sources) > 1 or not (any(sources) or _p(p, "InstanceId")):
        return _error("ValidationError", _SOURCE_ERROR)
    if value_error := _capacity_type_value_error(p):
        return _error("ValidationError", value_error)
    if shape_error := _override_shape_error(p, scan, _OVERRIDE_COUNT_ERROR):
        return _error(*shape_error)
    launch_template, lt_error = _launch_template_spec(p)
    if lt_error:
        return _error("ValidationError", lt_error)
    try:
        mixed, mixed_error = _mixed_instances_policy(p)
    except ValueError:      # a non-numeric requirement value (edges4 v-c-memory-max-abc)
        return _error("MalformedInput", "")
    if mixed_error:
        return _error("ValidationError", mixed_error)
    capacity_type = _p(p, "DesiredCapacityType") or None
    if launch_template and not _p(p, "InstanceId") and _template_has_requirements(launch_template):
        return _error("InvalidQueryParameter", _LTNC_ERROR)
    if requirements_error := _capacity_type_requirements_error(capacity_type, mixed, "create"):
        return _error("ValidationError", requirements_error)
    if name in _asgs:
        return _error("AlreadyExists", "AutoScalingGroup by this name already exists - A group with the name "
                                       f"{name} already exists")

    arn = _asg_arn(name)
    _asgs[name] = {
        "AutoScalingGroupName": name,
        "AutoScalingGroupARN": arn,
        "LaunchConfigurationName": _p(p, "LaunchConfigurationName"),
        "LaunchTemplate": {},
        "MinSize": int(_p(p, "MinSize") or 0),
        "MaxSize": int(_p(p, "MaxSize") or 0),
        "DesiredCapacity": int(_p(p, "DesiredCapacity") or _p(p, "MinSize") or 0),
        "DefaultCooldown": int(_p(p, "DefaultCooldown") or 300),
        "AvailabilityZones": _parse_member_list(p, "AvailabilityZones") or [f"{get_region()}a"],
        "HealthCheckType": _p(p, "HealthCheckType") or "EC2",
        "HealthCheckGracePeriod": int(_p(p, "HealthCheckGracePeriod") or 0),
        "Instances": [],
        "CreatedTime": now_iso(),
        "VPCZoneIdentifier": _p(p, "VPCZoneIdentifier") or "",
        "TerminationPolicies": _parse_member_list(p, "TerminationPolicies") or ["Default"],
        "NewInstancesProtectedFromScaleIn": _p(p, "NewInstancesProtectedFromScaleIn") == "true",
        **_group_defaults(),
        **_input_members(p, "CreateAutoScalingGroup", _GROUP_MEMBERS),
        "DesiredCapacityType": capacity_type if mixed else None,
        "Tags": [],
        "Status": "",
    }

    if launch_template:
        _asgs[name]["LaunchTemplate"] = launch_template
    if mixed:
        _asgs[name]["MixedInstancesPolicy"] = mixed

    # Parse tags
    i = 1
    tags = []
    while _p(p, f"Tags.member.{i}.Key"):
        tags.append({
            "Key": _p(p, f"Tags.member.{i}.Key"),
            "Value": _p(p, f"Tags.member.{i}.Value"),
            "ResourceId": name,
            "ResourceType": "auto-scaling-group",
            "PropagateAtLaunch": _p(p, f"Tags.member.{i}.PropagateAtLaunch") == "true",
        })
        i += 1
    _asgs[name]["Tags"] = tags
    _tags[name] = tags
    for spec in _input_members(p, "CreateAutoScalingGroup", ("LifecycleHookSpecificationList",)).get(
            "LifecycleHookSpecificationList", []):
        _hooks[f"{name}/{spec['LifecycleHookName']}"] = _hook_record(name, spec["LifecycleHookName"], spec)

    _reconcile_instances(_asgs[name])

    logger.info("CreateAutoScalingGroup: %s", name)
    return _xml(200, "CreateAutoScalingGroupResponse", "<CreateAutoScalingGroupResult/>")


def _describe_asgs(p):
    names = _parse_member_list(p, "AutoScalingGroupNames")
    filters = _parse_filters(p)
    members = ""
    groups, next_xml = _page(p, [(n, a) for n, a in _asgs.items()
                                 if (not names or n in names) and _group_matches_filters(a, filters)])
    for name, asg in groups:
        azs = "".join(f"<member>{escape(az)}</member>" for az in asg["AvailabilityZones"])
        tp = "".join(f"<member>{escape(t)}</member>" for t in asg["TerminationPolicies"])
        tags_xml = "".join(
            f"<member><Key>{escape(t['Key'])}</Key><Value>{escape(t['Value'])}</Value>"
            f"<ResourceId>{escape(t['ResourceId'])}</ResourceId><ResourceType>{t['ResourceType']}</ResourceType>"
            f"<PropagateAtLaunch>{'true' if t.get('PropagateAtLaunch') else 'false'}</PropagateAtLaunch></member>"
            for t in asg.get("Tags", [])
        )
        lt = asg.get("LaunchTemplate", {})
        lt_xml = ""
        if asg.get("MixedInstancesPolicy"):
            lt_xml = (f"<MixedInstancesPolicy>{_shape_xml(asg['MixedInstancesPolicy'], _model_shape('MixedInstancesPolicy'))}"
                      f"</MixedInstancesPolicy>")
        elif lt:
            lt_xml = _launch_template_xml(lt)
        members += (f"<member>"
                    f"<AutoScalingGroupName>{escape(name)}</AutoScalingGroupName>"
                    f"<AutoScalingGroupARN>{escape(asg['AutoScalingGroupARN'])}</AutoScalingGroupARN>"
                    f"<MinSize>{asg['MinSize']}</MinSize>"
                    f"<MaxSize>{asg['MaxSize']}</MaxSize>"
                    f"<DesiredCapacity>{asg['DesiredCapacity']}</DesiredCapacity>"
                    f"<DefaultCooldown>{asg['DefaultCooldown']}</DefaultCooldown>"
                    f"<AvailabilityZones>{azs}</AvailabilityZones>"
                    f"<HealthCheckType>{escape(asg['HealthCheckType'])}</HealthCheckType>"
                    f"<HealthCheckGracePeriod>{asg['HealthCheckGracePeriod']}</HealthCheckGracePeriod>"
                    f"<CreatedTime>{asg['CreatedTime']}</CreatedTime>"
                    f"<VPCZoneIdentifier>{escape(asg['VPCZoneIdentifier'])}</VPCZoneIdentifier>"
                    f"<TerminationPolicies>{tp}</TerminationPolicies>"
                    f"<NewInstancesProtectedFromScaleIn>{'true' if asg['NewInstancesProtectedFromScaleIn'] else 'false'}</NewInstancesProtectedFromScaleIn>"
                    f"{_group_members_xml(asg)}"
                    f"<Tags>{tags_xml}</Tags>"
                    f"{_instances_xml(asg)}"
                    f"{lt_xml}"
                    f"<LaunchConfigurationName>{escape(asg.get('LaunchConfigurationName', ''))}</LaunchConfigurationName>"
                    f"</member>")
    return _xml(200, "DescribeAutoScalingGroupsResponse",
                f"<DescribeAutoScalingGroupsResult><AutoScalingGroups>{members}</AutoScalingGroups>{next_xml}</DescribeAutoScalingGroupsResult>")


def _update_asg(p):
    name = _p(p, "AutoScalingGroupName")
    scan = _scan_overrides(p)
    if numbering_error := member_numbering_error(scan[0], scan[3]):
        return _error(*numbering_error)
    if (malformed := _malformed_number_error(p)) is not None:
        return _error("MalformedInput", malformed)
    if constraint_error := _constraint_error(p):
        return _error("ValidationError", constraint_error)
    # Any two of LaunchTemplate, MixedInstancesPolicy and LaunchConfigurationName (captured, us-east-2, 2026-11-06:
    # edges3 u-mip-and-lc, u-lt-it-and-lc, u-all-three, u-lt-ir-and-lc-vcpu).
    if sum([_has_member(p, "LaunchTemplate"), _has_member(p, "MixedInstancesPolicy"),
            bool(_p(p, "LaunchConfigurationName"))]) > 1:
        return _error("ValidationError", _UPDATE_SOURCE_ERROR)
    # On update every override error, duplicate InstanceTypes included, comes before the DesiredCapacityType
    # enum check (a-* captures; d-u-aa-* carry no enum value).
    if shape_error := _override_shape_error(p, scan, _UPDATE_OVERRIDE_COUNT_ERROR):
        return _error(*shape_error)
    if value_error := _capacity_type_value_error(p):
        return _error("ValidationError", value_error)
    asg = _asgs.get(name)
    if not asg:
        # UpdateAutoScalingGroup's text for a missing group (captured, us-east-2, 2026-11-05).
        return _error("ValidationError", "AutoScalingGroup name not found - null")
    launch_template, lt_error = _launch_template_spec(p)
    if lt_error:
        return _error("ValidationError", lt_error)
    try:
        mixed, mixed_error = _mixed_instances_policy(p, asg.get("MixedInstancesPolicy"))
    except ValueError:      # a non-numeric requirement value (edges4 v-u-vcpu-min-abc)
        return _error("MalformedInput", "")
    if mixed_error:
        return _error("ValidationError", mixed_error)
    sent_type = _p(p, "DesiredCapacityType") or None
    replaces_source = launch_template or _p(p, "LaunchConfigurationName")
    final_mixed = mixed or (None if replaces_source else asg.get("MixedInstancesPolicy"))
    # The stored type is checked again only when the launch source changes (captured, us-east-2, 2026-11-05).
    source_changed = replaces_source or _has_member(p, "MixedInstancesPolicy.LaunchTemplate")
    checked_type = sent_type or (asg.get("DesiredCapacityType") if source_changed else None)
    if requirements_error := _capacity_type_requirements_error(checked_type, final_mixed, "update"):
        return _error("ValidationError", requirements_error)
    if launch_template and _template_has_requirements(launch_template):
        return _error("InvalidQueryParameter", _LTNC_ERROR)
    if not final_mixed:
        capacity_type = None
    elif sent_type and (sent_type != "units" or mixed):
        capacity_type = sent_type
    else:
        capacity_type = asg.get("DesiredCapacityType")
    if mixed:
        asg["MixedInstancesPolicy"] = mixed
        asg["LaunchTemplate"] = {}
    elif launch_template:
        asg.pop("MixedInstancesPolicy", None)
    if mixed or launch_template:
        asg["LaunchConfigurationName"] = ""
    elif _p(p, "LaunchConfigurationName"):
        asg["LaunchConfigurationName"] = _p(p, "LaunchConfigurationName")
        asg["LaunchTemplate"] = {}
        asg.pop("MixedInstancesPolicy", None)
    for k, pk in [("MinSize", "MinSize"), ("MaxSize", "MaxSize"), ("DesiredCapacity", "DesiredCapacity"),
                   ("DefaultCooldown", "DefaultCooldown"), ("HealthCheckGracePeriod", "HealthCheckGracePeriod")]:
        v = _p(p, pk)
        if v != "":
            asg[k] = int(v)
    if _p(p, "HealthCheckType"):
        asg["HealthCheckType"] = _p(p, "HealthCheckType")
    if _p(p, "VPCZoneIdentifier"):
        asg["VPCZoneIdentifier"] = _p(p, "VPCZoneIdentifier")
    if _parse_member_list(p, "AvailabilityZones"):
        asg["AvailabilityZones"] = _parse_member_list(p, "AvailabilityZones")
    if _parse_member_list(p, "TerminationPolicies"):
        asg["TerminationPolicies"] = _parse_member_list(p, "TerminationPolicies")
    if _p(p, "NewInstancesProtectedFromScaleIn"):
        asg["NewInstancesProtectedFromScaleIn"] = _p(p, "NewInstancesProtectedFromScaleIn") == "true"
    asg.update(_input_members(p, "UpdateAutoScalingGroup", _GROUP_UPDATE_MEMBERS))
    asg["DesiredCapacityType"] = capacity_type
    if launch_template:
        asg["LaunchTemplate"] = launch_template
    _reconcile_instances(asg)
    return _xml(200, "UpdateAutoScalingGroupResponse", "<UpdateAutoScalingGroupResult/>")


def _set_desired_capacity(p):
    name = _p(p, "AutoScalingGroupName")
    asg = _asgs.get(name)
    if not asg:
        return _error("ValidationError", f"AutoScalingGroup {name} not found")
    desired = _p(p, "DesiredCapacity")
    if desired == "":
        return _error("ValidationError", "DesiredCapacity is required")
    asg["DesiredCapacity"] = int(desired)
    _reconcile_instances(asg)
    return _xml(200, "SetDesiredCapacityResponse", "")


def _delete_asg(p):
    name = _p(p, "AutoScalingGroupName")
    asg = _asgs.get(name)
    if asg and asg["Instances"] and _p(p, "ForceDelete") != "true":
        return _error("ResourceInUse", "You cannot delete an AutoScalingGroup while there are instances "
                                       "or pending Spot instance request(s) still in the group.")
    _asgs.pop(name, None)
    _tags.pop(name, None)
    _delete_group_hooks(name)
    _delete_group_policies(name)
    return _xml(200, "DeleteAutoScalingGroupResponse", "<DeleteAutoScalingGroupResult/>")


def _describe_asg_instances(p):
    wanted = _parse_member_list(p, "InstanceIds")
    found = [(name, inst) for name, asg in _asgs.items() for inst in asg["Instances"]
             if not wanted or inst["InstanceId"] in wanted]
    found, next_xml = _page(p, found, 50, 50)
    members = "".join(_instance_member_xml(inst, include_group_name=name) for name, inst in found)
    return _xml(200, "DescribeAutoScalingInstancesResponse",
                f"<DescribeAutoScalingInstancesResult><AutoScalingInstances>{members}</AutoScalingInstances>{next_xml}</DescribeAutoScalingInstancesResult>")


def _describe_scaling_activities(p):
    return _xml(200, "DescribeScalingActivitiesResponse",
                "<DescribeScalingActivitiesResult><Activities/></DescribeScalingActivitiesResult>")


# ---------------------------------------------------------------------------
# Instance Refresh
#
# No real instances run, so a refresh has nothing to roll. We record it and
# report it as immediately Successful (100% complete) to satisfy the
# terraform-provider-aws contract, which polls DescribeInstanceRefreshes until
# the refresh reaches a terminal state.
# ---------------------------------------------------------------------------

def _refresh_preferences(asg, sent):
    maintenance = asg.get("InstanceMaintenancePolicy") or {}
    prefs = {"MinHealthyPercentage": maintenance.get("MinHealthyPercentage", 90),
             "MaxHealthyPercentage": maintenance.get("MaxHealthyPercentage", 100),
             "SkipMatching": False, "AutoRollback": False,
             "ScaleInProtectedInstances": "Wait", "StandbyInstances": "Wait",
             **sent.get("Preferences", {})}
    if prefs.get("CheckpointPercentages"):
        prefs.setdefault("CheckpointDelay", 3600)
    return prefs


def _start_instance_refresh(p):
    name = _p(p, "AutoScalingGroupName")
    asg = _asgs.get(name)
    if not asg:
        return _error("ValidationError", f"AutoScalingGroup {name} not found")
    refresh_id = new_uuid()
    ts = now_iso()
    refresh = {
        "InstanceRefreshId": refresh_id,
        "AutoScalingGroupName": name,
        "Status": "Successful",
        "StatusReason": "Refresh completed",
        "StartTime": ts,
        "EndTime": ts,
        "PercentageComplete": 100,
        "InstancesToUpdate": 0,
        "Strategy": "Rolling",
        **_input_members(p, "StartInstanceRefresh", ("Strategy", "DesiredConfiguration")),
        "Preferences": _refresh_preferences(asg, _input_members(p, "StartInstanceRefresh", ("Preferences",))),
    }
    # Most-recent-first, matching AWS describe ordering.
    asg.setdefault("InstanceRefreshes", []).insert(0, refresh)
    return _xml(200, "StartInstanceRefreshResponse",
                f"<StartInstanceRefreshResult><InstanceRefreshId>{refresh_id}</InstanceRefreshId></StartInstanceRefreshResult>")


def _describe_instance_refreshes(p):
    name = _p(p, "AutoScalingGroupName")
    asg = _asgs.get(name)
    if not asg:
        return _error("ValidationError", f"AutoScalingGroup {name} not found")
    wanted = _parse_member_list(p, "InstanceRefreshIds")
    shape = _model_shape("InstanceRefresh")
    members = ""
    refreshes, next_xml = _page(p, [r for r in asg.get("InstanceRefreshes", [])
                                    if not wanted or r["InstanceRefreshId"] in wanted])
    for r in refreshes:
        members += (f"<member>"
                    f"<InstanceRefreshId>{r['InstanceRefreshId']}</InstanceRefreshId>"
                    f"<AutoScalingGroupName>{escape(r['AutoScalingGroupName'])}</AutoScalingGroupName>"
                    f"<Status>{r['Status']}</Status>"
                    f"<StatusReason>{r['StatusReason']}</StatusReason>"
                    f"<StartTime>{r['StartTime']}</StartTime>"
                    f"<EndTime>{r['EndTime']}</EndTime>"
                    f"<PercentageComplete>{r['PercentageComplete']}</PercentageComplete>"
                    f"<InstancesToUpdate>{r['InstancesToUpdate']}</InstancesToUpdate>"
                    f"{''.join(f'<{m}>{_shape_xml(r[m], shape.members[m])}</{m}>' for m in ('Preferences', 'DesiredConfiguration', 'Strategy') if r.get(m))}"
                    f"</member>")
    return _xml(200, "DescribeInstanceRefreshesResponse",
                f"<DescribeInstanceRefreshesResult><InstanceRefreshes>{members}</InstanceRefreshes>{next_xml}</DescribeInstanceRefreshesResult>")


def _cancel_instance_refresh(p):
    name = _p(p, "AutoScalingGroupName")
    asg = _asgs.get(name)
    if not asg:
        return _error("ValidationError", f"AutoScalingGroup {name} not found")
    refreshes = asg.get("InstanceRefreshes", [])
    active = next((r for r in refreshes if r["Status"] not in
                   ("Successful", "Failed", "Cancelled")), None)
    if not active:
        return _error("ActiveInstanceRefreshNotFound",
                      f"No active Instance Refresh for Auto Scaling group {name}")
    active["Status"] = "Cancelled"
    active["StatusReason"] = "Cancelled by user"
    active["EndTime"] = now_iso()
    return _xml(200, "CancelInstanceRefreshResponse",
                f"<CancelInstanceRefreshResult><InstanceRefreshId>{active['InstanceRefreshId']}</InstanceRefreshId></CancelInstanceRefreshResult>")


# ---------------------------------------------------------------------------
# LaunchConfiguration
# ---------------------------------------------------------------------------

_LC_MEMBERS = ("KeyName", "UserData", "KernelId", "RamdiskId", "BlockDeviceMappings", "InstanceMonitoring",
               "SpotPrice", "IamInstanceProfile", "EbsOptimized", "AssociatePublicIpAddress", "PlacementTenancy",
               "MetadataOptions")


def _create_lc(p):
    name = _p(p, "LaunchConfigurationName")
    if not name:
        return _error("ValidationError", "LaunchConfigurationName is required")
    if name in _launch_configs:
        return _error("AlreadyExists", f"LaunchConfiguration {name} already exists")
    arn = f"arn:aws:autoscaling:{get_region()}:{get_account_id()}:launchConfiguration:{new_uuid()}:launchConfigurationName/{name}"
    _launch_configs[name] = {
        "LaunchConfigurationName": name,
        "LaunchConfigurationARN": arn,
        "ImageId": _p(p, "ImageId") or "ami-00000000",
        "InstanceType": _p(p, "InstanceType") or "t2.micro",
        "KeyName": _p(p, "KeyName") or "",
        "SecurityGroups": _parse_member_list(p, "SecurityGroups"),
        "UserData": _p(p, "UserData") or "",
        "CreatedTime": now_iso(),
        "InstanceMonitoring": {"Enabled": True},
        "EbsOptimized": False,
        **_input_members(p, "CreateLaunchConfiguration", _LC_MEMBERS),
    }
    return _xml(200, "CreateLaunchConfigurationResponse", "<CreateLaunchConfigurationResult/>")


def _describe_lcs(p):
    names = _parse_member_list(p, "LaunchConfigurationNames")
    shape = _model_shape("LaunchConfiguration")
    members = ""
    configs, next_xml = _page(p, [(n, lc) for n, lc in _launch_configs.items() if not names or n in names])
    for name, lc in configs:
        sgs = "".join(f"<member>{escape(sg)}</member>" for sg in lc.get("SecurityGroups", []))
        members += (f"<member>"
                    f"<LaunchConfigurationName>{escape(name)}</LaunchConfigurationName>"
                    f"<LaunchConfigurationARN>{escape(lc['LaunchConfigurationARN'])}</LaunchConfigurationARN>"
                    f"<ImageId>{escape(lc['ImageId'])}</ImageId>"
                    f"<InstanceType>{escape(lc['InstanceType'])}</InstanceType>"
                    f"<CreatedTime>{lc['CreatedTime']}</CreatedTime>"
                    f"<SecurityGroups>{sgs}</SecurityGroups>"
                    f"{''.join(f'<{m}>{_shape_xml(lc[m], shape.members[m])}</{m}>' for m in _LC_MEMBERS if lc.get(m) not in (None, ''))}"
                    f"</member>")
    return _xml(200, "DescribeLaunchConfigurationsResponse",
                f"<DescribeLaunchConfigurationsResult><LaunchConfigurations>{members}</LaunchConfigurations>{next_xml}</DescribeLaunchConfigurationsResult>")


def _delete_lc(p):
    name = _p(p, "LaunchConfigurationName")
    _launch_configs.pop(name, None)
    return _xml(200, "DeleteLaunchConfigurationResponse", "<DeleteLaunchConfigurationResult/>")


# ---------------------------------------------------------------------------
# Scaling Policy
# ---------------------------------------------------------------------------

# Typed per the autoscaling service-2.json shapes; every other leaf is a string.
_POLICY_FLOATS = frozenset({"TargetValue", "MetricIntervalLowerBound", "MetricIntervalUpperBound"})
_POLICY_INTS = frozenset({"ScalingAdjustment", "EstimatedInstanceWarmup",
                          "MinAdjustmentMagnitude", "Period", "SchedulingBufferTime",
                          "MaxCapacityBuffer"})
_POLICY_BOOLS = frozenset({"DisableScaleIn", "ReturnData", "Enabled"})
_POLICY_OPTIONAL = ("EstimatedInstanceWarmup", "MetricAggregationType",
                    "MinAdjustmentMagnitude", "PredictiveScalingConfiguration",
                    "StepAdjustments", "TargetTrackingConfiguration")


def _policy_typed(value, key=None):
    if isinstance(value, dict):
        return {k: _policy_typed(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_policy_typed(v) for v in value]
    if key in _POLICY_FLOATS:
        return float(value)
    if key in _POLICY_INTS:
        return int(value)
    if key in _POLICY_BOOLS:
        return value if isinstance(value, bool) else str(value).lower() == "true"
    return value


def _parse_nested(params, prefix):
    """The query members under ``prefix`` as nested dicts, ``.member.N`` levels as lists."""
    tree = {}
    for key in params:
        if not key.startswith(prefix + "."):
            continue
        tokens = key[len(prefix) + 1:].split(".")
        path = []
        i = 0
        while i < len(tokens):
            if tokens[i] == "member" and i + 1 < len(tokens) and tokens[i + 1].isdigit():
                path.append(int(tokens[i + 1]))
                i += 2
            else:
                path.append(tokens[i])
                i += 1
        node = tree
        for step in path[:-1]:
            node = node.setdefault(step, {})
        node[path[-1]] = _p(params, key)

    def listify(node):
        if isinstance(node, dict):
            if node and all(isinstance(k, int) for k in node):
                return [listify(node[k]) for k in sorted(node)]
            return {k: listify(v) for k, v in node.items()}
        return node

    return listify(tree)


def _policy_record(asg_name, policy_name, arn, fields):
    """A policy record with only the members its type has, from API members or template properties."""
    policy_type = fields.get("PolicyType") or "SimpleScaling"
    record = {
        "PolicyARN": arn,
        "PolicyName": policy_name,
        "AutoScalingGroupName": asg_name,
        "PolicyType": policy_type,
        "Enabled": _policy_typed(fields.get("Enabled", True), "Enabled"),
    }
    if policy_type in ("SimpleScaling", "StepScaling"):
        record["AdjustmentType"] = fields.get("AdjustmentType") or "ChangeInCapacity"
    if policy_type == "SimpleScaling":
        record["ScalingAdjustment"] = int(fields.get("ScalingAdjustment") or 0)
        record["Cooldown"] = int(fields.get("Cooldown") or 300)
    for member in _POLICY_OPTIONAL:
        value = fields.get(member)
        if value not in (None, "", [], {}):
            record[member] = _policy_typed(value, member)
    tracking = record.get("TargetTrackingConfiguration")
    if tracking is not None:
        tracking.setdefault("DisableScaleIn", False)
    predictive = record.get("PredictiveScalingConfiguration")
    if predictive is not None:
        # "Defaults to HonorMaxCapacity if not specified" (service-2.json).
        predictive.setdefault("MaxCapacityBreachBehavior", "HonorMaxCapacity")
    return record


def _put_scaling_policy(p):
    asg_name = _p(p, "AutoScalingGroupName")
    policy_name = _p(p, "PolicyName")
    if not policy_name:
        return _error("ValidationError", "PolicyName is required")
    arn = f"arn:aws:autoscaling:{get_region()}:{get_account_id()}:scalingPolicy:{new_uuid()}:autoScalingGroupName/{asg_name}:policyName/{policy_name}"
    key = f"{asg_name}/{policy_name}"
    fields = {k: _p(p, k) for k in ("PolicyType", "AdjustmentType", "ScalingAdjustment",
                                    "Cooldown", "Enabled", "EstimatedInstanceWarmup",
                                    "MetricAggregationType", "MinAdjustmentMagnitude")
              if _p(p, k) != ""}
    fields["StepAdjustments"] = _parse_nested(p, "StepAdjustments")
    fields["TargetTrackingConfiguration"] = _parse_nested(p, "TargetTrackingConfiguration")
    fields["PredictiveScalingConfiguration"] = _parse_nested(p, "PredictiveScalingConfiguration")
    try:
        record = _policy_record(asg_name, policy_name, arn, fields)
    except ValueError as exc:
        return _error("ValidationError", f"Invalid numeric value in the scaling policy: {exc}")
    _delete_policy_alarms(_policies.get(key))
    _put_tracking_alarms(record)
    _policies[key] = record
    return _xml(200, "PutScalingPolicyResponse",
                f"<PutScalingPolicyResult><PolicyARN>{escape(arn)}</PolicyARN>"
                f"<Alarms>{_member_xml(record.get('Alarms', []))}</Alarms></PutScalingPolicyResult>")


_TRACKING_METRICS = {
    "ASGAverageCPUUtilization": ("CPUUtilization", None),
    "ASGAverageNetworkIn": ("NetworkIn", "Bytes"),
    "ASGAverageNetworkOut": ("NetworkOut", "Bytes"),
}


def _tracking_metric(tracking, asg_name):
    """The alarm metric of a target tracking configuration, or None for types without modelled alarms."""
    predefined = tracking.get("PredefinedMetricSpecification") or {}
    if predefined.get("PredefinedMetricType") in _TRACKING_METRICS:
        name, unit = _TRACKING_METRICS[predefined["PredefinedMetricType"]]
        return {"Namespace": "AWS/EC2", "MetricName": name, "Statistic": "Average", "Unit": unit,
                "Dimensions": [{"Name": "AutoScalingGroupName", "Value": asg_name}]}
    custom = tracking.get("CustomizedMetricSpecification") or {}
    if custom.get("MetricName"):
        return {"Namespace": custom.get("Namespace"), "MetricName": custom["MetricName"],
                "Statistic": custom.get("Statistic"), "Unit": custom.get("Unit"),
                "Dimensions": custom.get("Dimensions", [])}
    return None


def _put_tracking_alarms(record):
    """Create the AlarmHigh and, unless scale-in is disabled, AlarmLow alarms of a target tracking policy."""
    tracking = record.get("TargetTrackingConfiguration")
    metric = tracking and _tracking_metric(tracking, record["AutoScalingGroupName"])
    if not metric:
        return
    from ministack.services import cloudwatch as _cw
    target = Decimal(str(tracking.get("TargetValue", 0)))
    levels = [("High", 3, target, "GreaterThanThreshold")]
    if not tracking.get("DisableScaleIn"):
        levels.append(("Low", 15, target * Decimal("0.9"), "LessThanThreshold"))
    record["Alarms"] = []
    for level, periods, threshold, operator in levels:
        name = f"TargetTracking-{record['AutoScalingGroupName']}-Alarm{level}-{new_uuid()}"
        now = int(time.time())
        alarm = {k: v for k, v in {
            "AlarmName": name,
            "AlarmArn": f"arn:aws:cloudwatch:{get_region()}:{get_account_id()}:alarm:{name}",
            "AlarmDescription": f"DO NOT EDIT OR DELETE. For TargetTrackingScaling policy {record['PolicyARN']}.",
            "ActionsEnabled": True,
            "OKActions": [],
            "AlarmActions": [record["PolicyARN"]],
            "InsufficientDataActions": [],
            "StateValue": "INSUFFICIENT_DATA",
            "StateReason": "Unchecked: Initial alarm creation",
            "StateUpdatedTimestamp": now,
            "AlarmConfigurationUpdatedTimestamp": now,
            **metric,
            "Period": 60,
            "EvaluationPeriods": periods,
            "Threshold": float(threshold),
            "ComparisonOperator": operator,
        }.items() if v is not None}
        _cw.cloudformation_put_metric_alarm(alarm)
        record["Alarms"].append({"AlarmName": name, "AlarmARN": alarm["AlarmArn"]})


def _delete_policy_alarms(record):
    """Delete the alarms a policy created."""
    from ministack.services import cloudwatch as _cw
    for alarm in (record or {}).get("Alarms", []):
        _cw.cloudformation_delete_metric_alarm(alarm["AlarmName"])


def _delete_group_policies(asg_name):
    """Delete the policies of a group and their alarms."""
    for k, policy in list(_policies.items()):
        if policy.get("AutoScalingGroupName") == asg_name:
            _delete_policy_alarms(_policies.pop(k, None))


def _delete_group_hooks(asg_name):
    """Delete the lifecycle hooks of a group."""
    for k, hook in list(_hooks.items()):
        if hook.get("AutoScalingGroupName") == asg_name:
            _hooks.pop(k, None)


def _member_xml(value):
    """A stored member as query-protocol XML."""
    if isinstance(value, dict):
        return "".join(f"<{k}>{_member_xml(v)}</{k}>" for k, v in value.items())
    if isinstance(value, list):
        return "".join(f"<member>{_member_xml(v)}</member>" for v in value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return escape(str(value))


def _describe_policies(p):
    asg_name = _p(p, "AutoScalingGroupName")
    names = _parse_member_list(p, "PolicyNames")
    types = _parse_member_list(p, "PolicyTypes")
    members = ""
    policies, next_xml = _page(p, [pol for pol in _policies.values()
                                   if (not asg_name or pol["AutoScalingGroupName"] == asg_name)
                                   and (not names or pol["PolicyName"] in names)
                                   and (not types or pol["PolicyType"] in types)])
    for pol in policies:
        body = dict(pol)
        body.setdefault("Enabled", True)
        body.setdefault("StepAdjustments", [])
        body.setdefault("Alarms", [])
        if "MinAdjustmentMagnitude" in body:
            body["MinAdjustmentStep"] = body["MinAdjustmentMagnitude"]
        members += f"<member>{_member_xml(body)}</member>"
    return _xml(200, "DescribePoliciesResponse",
                f"<DescribePoliciesResult><ScalingPolicies>{members}</ScalingPolicies>{next_xml}</DescribePoliciesResult>")


def _delete_policy(p):
    policy_name = _p(p, "PolicyName")
    asg_name = _p(p, "AutoScalingGroupName")
    key = f"{asg_name}/{policy_name}"
    _delete_policy_alarms(_policies.pop(key, None))
    return _xml(200, "DeletePolicyResponse", "<DeletePolicyResult/>")


# ---------------------------------------------------------------------------
# Lifecycle Hook
# ---------------------------------------------------------------------------

def _hook_record(asg_name, hook_name, fields):
    """A lifecycle hook record, from API members or template properties."""
    return {
        "LifecycleHookName": hook_name,
        "AutoScalingGroupName": asg_name,
        "LifecycleTransition": fields.get("LifecycleTransition") or "autoscaling:EC2_INSTANCE_LAUNCHING",
        "HeartbeatTimeout": int(fields.get("HeartbeatTimeout") or 3600),
        "DefaultResult": fields.get("DefaultResult") or "ABANDON",
        "NotificationTargetARN": fields.get("NotificationTargetARN") or "",
        "RoleARN": fields.get("RoleARN") or "",
        "NotificationMetadata": fields.get("NotificationMetadata") or "",
    }


def _put_lifecycle_hook(p):
    asg_name = _p(p, "AutoScalingGroupName")
    hook_name = _p(p, "LifecycleHookName")
    key = f"{asg_name}/{hook_name}"
    _hooks[key] = _hook_record(asg_name, hook_name, {
        member: _p(p, member) for member in (
            "LifecycleTransition", "HeartbeatTimeout", "DefaultResult",
            "NotificationTargetARN", "RoleARN", "NotificationMetadata")})
    return _xml(200, "PutLifecycleHookResponse", "<PutLifecycleHookResult/>")


def _describe_lifecycle_hooks(p):
    asg_name = _p(p, "AutoScalingGroupName")
    names = _parse_member_list(p, "LifecycleHookNames")
    members = ""
    for key, hook in _hooks.items():
        if hook["AutoScalingGroupName"] != asg_name or (names and hook["LifecycleHookName"] not in names):
            continue
        metadata = hook.get("NotificationMetadata")
        metadata_xml = f"<NotificationMetadata>{escape(metadata)}</NotificationMetadata>" if metadata else ""
        members += (f"<member>"
                    f"<LifecycleHookName>{escape(hook['LifecycleHookName'])}</LifecycleHookName>"
                    f"<AutoScalingGroupName>{escape(hook['AutoScalingGroupName'])}</AutoScalingGroupName>"
                    f"<LifecycleTransition>{escape(hook['LifecycleTransition'])}</LifecycleTransition>"
                    f"{''.join(f'<{m}>{escape(hook[m])}</{m}>' for m in ('NotificationTargetARN', 'RoleARN') if hook.get(m))}"
                    f"{metadata_xml}"
                    f"<HeartbeatTimeout>{hook['HeartbeatTimeout']}</HeartbeatTimeout>"
                    f"<DefaultResult>{escape(hook['DefaultResult'])}</DefaultResult>"
                    f"</member>")
    return _xml(200, "DescribeLifecycleHooksResponse",
                f"<DescribeLifecycleHooksResult><LifecycleHooks>{members}</LifecycleHooks></DescribeLifecycleHooksResult>")


def _delete_lifecycle_hook(p):
    asg_name = _p(p, "AutoScalingGroupName")
    hook_name = _p(p, "LifecycleHookName")
    _hooks.pop(f"{asg_name}/{hook_name}", None)
    return _xml(200, "DeleteLifecycleHookResponse", "<DeleteLifecycleHookResult/>")


def _complete_lifecycle_action(p):
    return _xml(200, "CompleteLifecycleActionResponse", "<CompleteLifecycleActionResult/>")


def _record_lifecycle_heartbeat(p):
    return _xml(200, "RecordLifecycleActionHeartbeatResponse", "<RecordLifecycleActionHeartbeatResult/>")


# ---------------------------------------------------------------------------
# Scheduled Action
# ---------------------------------------------------------------------------

def _scheduled_action_record(asg_name, action_name, arn, fields):
    """A scheduled action record; an omitted capacity is -1 and left out of the response."""
    def capacity(member):
        value = fields.get(member)
        return -1 if value in (None, "") else int(value)

    record = {
        "ScheduledActionARN": arn,
        "ScheduledActionName": action_name,
        "AutoScalingGroupName": asg_name,
        "Recurrence": fields.get("Recurrence") or "",
        "MinSize": capacity("MinSize"),
        "MaxSize": capacity("MaxSize"),
        "DesiredCapacity": capacity("DesiredCapacity"),
    }
    for member in ("StartTime", "EndTime", "TimeZone"):
        if fields.get(member):
            record[member] = str(fields[member])
    return record


def _put_scheduled_action(p):
    asg_name = _p(p, "AutoScalingGroupName")
    action_name = _p(p, "ScheduledActionName")
    key = f"{asg_name}/{action_name}"
    arn = f"arn:aws:autoscaling:{get_region()}:{get_account_id()}:scheduledUpdateGroupAction:{new_uuid()}:autoScalingGroupName/{asg_name}:scheduledActionName/{action_name}"
    _scheduled_actions[key] = _scheduled_action_record(asg_name, action_name, arn, {
        member: _p(p, member) for member in (
            "Recurrence", "MinSize", "MaxSize", "DesiredCapacity",
            "StartTime", "EndTime", "TimeZone")})
    return _xml(200, "PutScheduledUpdateGroupActionResponse", "<PutScheduledUpdateGroupActionResult/>")


def _describe_scheduled_actions(p):
    asg_name = _p(p, "AutoScalingGroupName")
    names = _parse_member_list(p, "ScheduledActionNames")
    members = ""
    actions, next_xml = _page(p, [sa for sa in _scheduled_actions.values()
                                  if (not asg_name or sa["AutoScalingGroupName"] == asg_name)
                                  and (not names or sa["ScheduledActionName"] in names)])
    for sa in actions:
        members += (f"<member>"
                    f"<ScheduledActionARN>{escape(sa['ScheduledActionARN'])}</ScheduledActionARN>"
                    f"<ScheduledActionName>{escape(sa['ScheduledActionName'])}</ScheduledActionName>"
                    f"<AutoScalingGroupName>{escape(sa['AutoScalingGroupName'])}</AutoScalingGroupName>"
                    f"<Recurrence>{escape(sa.get('Recurrence', ''))}</Recurrence>")
        for member in ("MinSize", "MaxSize", "DesiredCapacity"):
            value = sa.get(member, -1)
            if value is not None and int(value) >= 0:
                members += f"<{member}>{int(value)}</{member}>"
        # StartTime is also answered as the deprecated Time.
        if sa.get("StartTime"):
            members += (f"<StartTime>{escape(sa['StartTime'])}</StartTime>"
                        f"<Time>{escape(sa['StartTime'])}</Time>")
        for member in ("EndTime", "TimeZone"):
            if sa.get(member):
                members += f"<{member}>{escape(sa[member])}</{member}>"
        members += "</member>"
    return _xml(200, "DescribeScheduledActionsResponse",
                f"<DescribeScheduledActionsResult><ScheduledUpdateGroupActions>{members}</ScheduledUpdateGroupActions>{next_xml}</DescribeScheduledActionsResult>")


def _delete_scheduled_action(p):
    asg_name = _p(p, "AutoScalingGroupName")
    action_name = _p(p, "ScheduledActionName")
    _scheduled_actions.pop(f"{asg_name}/{action_name}", None)
    return _xml(200, "DeleteScheduledActionResponse", "<DeleteScheduledActionResult/>")


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _create_or_update_tags(p):
    i = 1
    while _p(p, f"Tags.member.{i}.Key"):
        asg_name = _p(p, f"Tags.member.{i}.ResourceId")
        tag = {
            "Key": _p(p, f"Tags.member.{i}.Key"),
            "Value": _p(p, f"Tags.member.{i}.Value"),
            "ResourceId": asg_name,
            "ResourceType": "auto-scaling-group",
            "PropagateAtLaunch": _p(p, f"Tags.member.{i}.PropagateAtLaunch") == "true",
        }
        existing = _tags.setdefault(asg_name, [])
        existing = [t for t in existing if t["Key"] != tag["Key"]]
        existing.append(tag)
        _tags[asg_name] = existing
        if asg_name in _asgs:
            _asgs[asg_name]["Tags"] = existing
        i += 1
    return _xml(200, "CreateOrUpdateTagsResponse", "<CreateOrUpdateTagsResult/>")


def _describe_tags(p):
    filters = _parse_filters(p)
    members = ""
    tags, next_xml = _page(p, [t for tag_list in _tags.values() for t in tag_list
                               if _tag_matches_filters(t, filters)])
    for t in tags:
        members += (f"<member>"
                    f"<Key>{escape(t['Key'])}</Key><Value>{escape(t['Value'])}</Value>"
                    f"<ResourceId>{escape(t['ResourceId'])}</ResourceId>"
                    f"<ResourceType>{t['ResourceType']}</ResourceType>"
                    f"<PropagateAtLaunch>{'true' if t.get('PropagateAtLaunch') else 'false'}</PropagateAtLaunch>"
                    f"</member>")
    return _xml(200, "DescribeTagsResponse",
                f"<DescribeTagsResult><Tags>{members}</Tags>{next_xml}</DescribeTagsResult>")


def _delete_tags(p):
    i = 1
    while _p(p, f"Tags.member.{i}.Key"):
        asg_name = _p(p, f"Tags.member.{i}.ResourceId")
        key = _p(p, f"Tags.member.{i}.Key")
        existing = _tags.get(asg_name, [])
        _tags[asg_name] = [t for t in existing if t["Key"] != key]
        if asg_name in _asgs:
            _asgs[asg_name]["Tags"] = _tags[asg_name]
        i += 1
    return _xml(200, "DeleteTagsResponse", "<DeleteTagsResult/>")


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    "CreateAutoScalingGroup": _create_asg,
    "DescribeAutoScalingGroups": _describe_asgs,
    "UpdateAutoScalingGroup": _update_asg,
    "DeleteAutoScalingGroup": _delete_asg,
    "SetDesiredCapacity": _set_desired_capacity,
    "DescribeAutoScalingInstances": _describe_asg_instances,
    "DescribeScalingActivities": _describe_scaling_activities,
    "StartInstanceRefresh": _start_instance_refresh,
    "DescribeInstanceRefreshes": _describe_instance_refreshes,
    "CancelInstanceRefresh": _cancel_instance_refresh,
    "CreateLaunchConfiguration": _create_lc,
    "DescribeLaunchConfigurations": _describe_lcs,
    "DeleteLaunchConfiguration": _delete_lc,
    "PutScalingPolicy": _put_scaling_policy,
    "DescribePolicies": _describe_policies,
    "DeletePolicy": _delete_policy,
    "PutLifecycleHook": _put_lifecycle_hook,
    "DescribeLifecycleHooks": _describe_lifecycle_hooks,
    "DeleteLifecycleHook": _delete_lifecycle_hook,
    "CompleteLifecycleAction": _complete_lifecycle_action,
    "RecordLifecycleActionHeartbeat": _record_lifecycle_heartbeat,
    "PutScheduledUpdateGroupAction": _put_scheduled_action,
    "DescribeScheduledActions": _describe_scheduled_actions,
    "DeleteScheduledAction": _delete_scheduled_action,
    "CreateOrUpdateTags": _create_or_update_tags,
    "DescribeTags": _describe_tags,
    "DeleteTags": _delete_tags,
}


async def handle_request(method, path, headers, body, query_params):
    from urllib.parse import parse_qs
    if body:
        params = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
        params = {k: v[0] if len(v) == 1 else v for k, v in params.items()}
    else:
        params = dict(query_params) if query_params else {}

    action = params.get("Action", "")
    if isinstance(action, list):
        action = action[0]

    handler = _ACTION_MAP.get(action)
    if not handler:
        s, h, b = _error("InvalidAction", f"Unknown AutoScaling action: {action}")
        return s, h, b

    s, h, b = handler(params)
    return s, h, b
