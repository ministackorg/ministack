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
import time
from collections import defaultdict

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    get_account_id,
    get_region,
    new_uuid,
    now_iso,
)

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


def restore_state(data):
    if not data:
        return

    _clear_state()
    _asgs.update(data.get("asgs", {}))
    _launch_configs.update(data.get("launch_configs", {}))

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


try:
    _restored = load_state("autoscaling")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted autoscaling state; continuing fresh")


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


def _xml(status, root_tag, inner):
    body = (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<{root_tag} xmlns="http://autoscaling.amazonaws.com/doc/2011-01-01/">'
            f'{inner}'
            f'<ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>'
            f'</{root_tag}>').encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status=400):
    return _xml(status, "ErrorResponse",
                f'<Error><Type>Sender</Type><Code>{code}</Code><Message>{message}</Message></Error>')


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
    launch_template = asg.get("LaunchTemplate") or {}
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


def _instance_member_xml(inst, include_group_name=""):
    """Render one instance. DescribeAutoScalingInstances also carries the
    owning AutoScalingGroupName, so callers pass it when needed."""
    launch_template = inst.get("LaunchTemplate") or {}
    lt_xml = ""
    if launch_template:
        lt_xml = (f"<LaunchTemplate>"
                  f"<LaunchTemplateId>{launch_template.get('LaunchTemplateId', '')}</LaunchTemplateId>"
                  f"<LaunchTemplateName>{launch_template.get('LaunchTemplateName', '')}</LaunchTemplateName>"
                  f"<Version>{launch_template.get('Version', '')}</Version></LaunchTemplate>")
    launch_config = inst.get("LaunchConfigurationName", "")
    lc_xml = f"<LaunchConfigurationName>{launch_config}</LaunchConfigurationName>" if launch_config else ""
    group_xml = f"<AutoScalingGroupName>{include_group_name}</AutoScalingGroupName>" if include_group_name else ""
    return (f"<member>"
            f"<InstanceId>{inst['InstanceId']}</InstanceId>"
            f"{group_xml}"
            f"<AvailabilityZone>{inst['AvailabilityZone']}</AvailabilityZone>"
            f"<LifecycleState>{inst['LifecycleState']}</LifecycleState>"
            f"<HealthStatus>{inst['HealthStatus']}</HealthStatus>"
            f"<ProtectedFromScaleIn>{'true' if inst['ProtectedFromScaleIn'] else 'false'}</ProtectedFromScaleIn>"
            f"{lt_xml}{lc_xml}"
            f"</member>")


def _instances_xml(asg):
    if not asg["Instances"]:
        return "<Instances/>"
    return "<Instances>" + "".join(_instance_member_xml(i) for i in asg["Instances"]) + "</Instances>"


def _create_asg(p):
    name = _p(p, "AutoScalingGroupName")
    if not name:
        return _error("ValidationError", "AutoScalingGroupName is required")
    if name in _asgs:
        return _error("AlreadyExistsFault", f"AutoScalingGroup {name} already exists")

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
        "HealthCheckGracePeriod": int(_p(p, "HealthCheckGracePeriod") or 300),
        "Instances": [],
        "CreatedTime": now_iso(),
        "VPCZoneIdentifier": _p(p, "VPCZoneIdentifier") or "",
        "TerminationPolicies": _parse_member_list(p, "TerminationPolicies") or ["Default"],
        "NewInstancesProtectedFromScaleIn": _p(p, "NewInstancesProtectedFromScaleIn") == "true",
        "ServiceLinkedRoleARN": _p(p, "ServiceLinkedRoleARN") or "",
        "Tags": [],
        "Status": "",
    }

    # Parse launch template
    lt_id = _p(p, "LaunchTemplate.LaunchTemplateId") or _p(p, "LaunchTemplate.LaunchTemplateName")
    lt_ver = _p(p, "LaunchTemplate.Version") or "$Default"
    if lt_id:
        _asgs[name]["LaunchTemplate"] = {
            "LaunchTemplateId": lt_id,
            "LaunchTemplateName": lt_id,
            "Version": lt_ver,
        }

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

    _reconcile_instances(_asgs[name])

    logger.info("CreateAutoScalingGroup: %s", name)
    return _xml(200, "CreateAutoScalingGroupResponse", "<CreateAutoScalingGroupResult/>")


def _describe_asgs(p):
    names = _parse_member_list(p, "AutoScalingGroupNames")
    members = ""
    for name, asg in _asgs.items():
        if names and name not in names:
            continue
        azs = "".join(f"<member>{az}</member>" for az in asg["AvailabilityZones"])
        tp = "".join(f"<member>{t}</member>" for t in asg["TerminationPolicies"])
        tags_xml = "".join(
            f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value>"
            f"<ResourceId>{t['ResourceId']}</ResourceId><ResourceType>{t['ResourceType']}</ResourceType>"
            f"<PropagateAtLaunch>{'true' if t.get('PropagateAtLaunch') else 'false'}</PropagateAtLaunch></member>"
            for t in asg.get("Tags", [])
        )
        lt = asg.get("LaunchTemplate", {})
        lt_xml = ""
        if lt:
            lt_xml = (f"<LaunchTemplate>"
                      f"<LaunchTemplateId>{lt.get('LaunchTemplateId', '')}</LaunchTemplateId>"
                      f"<LaunchTemplateName>{lt.get('LaunchTemplateName', '')}</LaunchTemplateName>"
                      f"<Version>{lt.get('Version', '')}</Version></LaunchTemplate>")
        members += (f"<member>"
                    f"<AutoScalingGroupName>{name}</AutoScalingGroupName>"
                    f"<AutoScalingGroupARN>{asg['AutoScalingGroupARN']}</AutoScalingGroupARN>"
                    f"<MinSize>{asg['MinSize']}</MinSize>"
                    f"<MaxSize>{asg['MaxSize']}</MaxSize>"
                    f"<DesiredCapacity>{asg['DesiredCapacity']}</DesiredCapacity>"
                    f"<DefaultCooldown>{asg['DefaultCooldown']}</DefaultCooldown>"
                    f"<AvailabilityZones>{azs}</AvailabilityZones>"
                    f"<HealthCheckType>{asg['HealthCheckType']}</HealthCheckType>"
                    f"<HealthCheckGracePeriod>{asg['HealthCheckGracePeriod']}</HealthCheckGracePeriod>"
                    f"<CreatedTime>{asg['CreatedTime']}</CreatedTime>"
                    f"<VPCZoneIdentifier>{asg['VPCZoneIdentifier']}</VPCZoneIdentifier>"
                    f"<TerminationPolicies>{tp}</TerminationPolicies>"
                    f"<NewInstancesProtectedFromScaleIn>{'true' if asg['NewInstancesProtectedFromScaleIn'] else 'false'}</NewInstancesProtectedFromScaleIn>"
                    f"<Tags>{tags_xml}</Tags>"
                    f"{_instances_xml(asg)}"
                    f"{lt_xml}"
                    f"<LaunchConfigurationName>{asg.get('LaunchConfigurationName', '')}</LaunchConfigurationName>"
                    f"</member>")
    return _xml(200, "DescribeAutoScalingGroupsResponse",
                f"<DescribeAutoScalingGroupsResult><AutoScalingGroups>{members}</AutoScalingGroups></DescribeAutoScalingGroupsResult>")


def _update_asg(p):
    name = _p(p, "AutoScalingGroupName")
    asg = _asgs.get(name)
    if not asg:
        return _error("ValidationError", f"AutoScalingGroup {name} not found")
    for k, pk in [("MinSize", "MinSize"), ("MaxSize", "MaxSize"), ("DesiredCapacity", "DesiredCapacity"),
                   ("DefaultCooldown", "DefaultCooldown"), ("HealthCheckGracePeriod", "HealthCheckGracePeriod")]:
        v = _p(p, pk)
        if v:
            asg[k] = int(v)
    if _p(p, "HealthCheckType"):
        asg["HealthCheckType"] = _p(p, "HealthCheckType")
    if _p(p, "VPCZoneIdentifier"):
        asg["VPCZoneIdentifier"] = _p(p, "VPCZoneIdentifier")
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
    _asgs.pop(name, None)
    _tags.pop(name, None)
    # Remove associated hooks
    keys_to_del = [k for k in _hooks if k.startswith(f"{name}/")]
    for k in keys_to_del:
        del _hooks[k]
    return _xml(200, "DeleteAutoScalingGroupResponse", "<DeleteAutoScalingGroupResult/>")


def _describe_asg_instances(p):
    wanted = _parse_member_list(p, "InstanceIds")
    members = ""
    for name, asg in _asgs.items():
        for inst in asg["Instances"]:
            if wanted and inst["InstanceId"] not in wanted:
                continue
            members += _instance_member_xml(inst, include_group_name=name)
    return _xml(200, "DescribeAutoScalingInstancesResponse",
                f"<DescribeAutoScalingInstancesResult><AutoScalingInstances>{members}</AutoScalingInstances></DescribeAutoScalingInstancesResult>")


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
        "Preferences": {
            "MinHealthyPercentage": _p(p, "Preferences.MinHealthyPercentage"),
            "InstanceWarmup": _p(p, "Preferences.InstanceWarmup"),
        },
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
    members = ""
    for r in asg.get("InstanceRefreshes", []):
        if wanted and r["InstanceRefreshId"] not in wanted:
            continue
        members += (f"<member>"
                    f"<InstanceRefreshId>{r['InstanceRefreshId']}</InstanceRefreshId>"
                    f"<AutoScalingGroupName>{r['AutoScalingGroupName']}</AutoScalingGroupName>"
                    f"<Status>{r['Status']}</Status>"
                    f"<StatusReason>{r['StatusReason']}</StatusReason>"
                    f"<StartTime>{r['StartTime']}</StartTime>"
                    f"<EndTime>{r['EndTime']}</EndTime>"
                    f"<PercentageComplete>{r['PercentageComplete']}</PercentageComplete>"
                    f"<InstancesToUpdate>{r['InstancesToUpdate']}</InstancesToUpdate>"
                    f"</member>")
    return _xml(200, "DescribeInstanceRefreshesResponse",
                f"<DescribeInstanceRefreshesResult><InstanceRefreshes>{members}</InstanceRefreshes></DescribeInstanceRefreshesResult>")


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

def _create_lc(p):
    name = _p(p, "LaunchConfigurationName")
    if not name:
        return _error("ValidationError", "LaunchConfigurationName is required")
    if name in _launch_configs:
        return _error("AlreadyExistsFault", f"LaunchConfiguration {name} already exists")
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
    }
    return _xml(200, "CreateLaunchConfigurationResponse", "<CreateLaunchConfigurationResult/>")


def _describe_lcs(p):
    names = _parse_member_list(p, "LaunchConfigurationNames")
    members = ""
    for name, lc in _launch_configs.items():
        if names and name not in names:
            continue
        sgs = "".join(f"<member>{sg}</member>" for sg in lc.get("SecurityGroups", []))
        members += (f"<member>"
                    f"<LaunchConfigurationName>{name}</LaunchConfigurationName>"
                    f"<LaunchConfigurationARN>{lc['LaunchConfigurationARN']}</LaunchConfigurationARN>"
                    f"<ImageId>{lc['ImageId']}</ImageId>"
                    f"<InstanceType>{lc['InstanceType']}</InstanceType>"
                    f"<CreatedTime>{lc['CreatedTime']}</CreatedTime>"
                    f"<SecurityGroups>{sgs}</SecurityGroups>"
                    f"</member>")
    return _xml(200, "DescribeLaunchConfigurationsResponse",
                f"<DescribeLaunchConfigurationsResult><LaunchConfigurations>{members}</LaunchConfigurations></DescribeLaunchConfigurationsResult>")


def _delete_lc(p):
    name = _p(p, "LaunchConfigurationName")
    _launch_configs.pop(name, None)
    return _xml(200, "DeleteLaunchConfigurationResponse", "<DeleteLaunchConfigurationResult/>")


# ---------------------------------------------------------------------------
# Scaling Policy
# ---------------------------------------------------------------------------

def _put_scaling_policy(p):
    asg_name = _p(p, "AutoScalingGroupName")
    policy_name = _p(p, "PolicyName")
    if not policy_name:
        return _error("ValidationError", "PolicyName is required")
    arn = f"arn:aws:autoscaling:{get_region()}:{get_account_id()}:scalingPolicy:{new_uuid()}:autoScalingGroupName/{asg_name}:policyName/{policy_name}"
    key = f"{asg_name}/{policy_name}"
    _policies[key] = {
        "PolicyARN": arn,
        "PolicyName": policy_name,
        "AutoScalingGroupName": asg_name,
        "PolicyType": _p(p, "PolicyType") or "SimpleScaling",
        "AdjustmentType": _p(p, "AdjustmentType") or "ChangeInCapacity",
        "ScalingAdjustment": int(_p(p, "ScalingAdjustment") or 0),
        "Cooldown": int(_p(p, "Cooldown") or 300),
    }
    return _xml(200, "PutScalingPolicyResponse",
                f"<PutScalingPolicyResult><PolicyARN>{arn}</PolicyARN></PutScalingPolicyResult>")


def _describe_policies(p):
    asg_name = _p(p, "AutoScalingGroupName")
    members = ""
    for key, pol in _policies.items():
        if asg_name and pol["AutoScalingGroupName"] != asg_name:
            continue
        members += (f"<member>"
                    f"<PolicyARN>{pol['PolicyARN']}</PolicyARN>"
                    f"<PolicyName>{pol['PolicyName']}</PolicyName>"
                    f"<AutoScalingGroupName>{pol['AutoScalingGroupName']}</AutoScalingGroupName>"
                    f"<PolicyType>{pol['PolicyType']}</PolicyType>"
                    f"<AdjustmentType>{pol.get('AdjustmentType', '')}</AdjustmentType>"
                    f"<ScalingAdjustment>{pol.get('ScalingAdjustment', 0)}</ScalingAdjustment>"
                    f"<Cooldown>{pol.get('Cooldown', 300)}</Cooldown>"
                    f"</member>")
    return _xml(200, "DescribePoliciesResponse",
                f"<DescribePoliciesResult><ScalingPolicies>{members}</ScalingPolicies></DescribePoliciesResult>")


def _delete_policy(p):
    policy_name = _p(p, "PolicyName")
    asg_name = _p(p, "AutoScalingGroupName")
    key = f"{asg_name}/{policy_name}"
    _policies.pop(key, None)
    return _xml(200, "DeletePolicyResponse", "<DeletePolicyResult/>")


# ---------------------------------------------------------------------------
# Lifecycle Hook
# ---------------------------------------------------------------------------

def _put_lifecycle_hook(p):
    asg_name = _p(p, "AutoScalingGroupName")
    hook_name = _p(p, "LifecycleHookName")
    key = f"{asg_name}/{hook_name}"
    _hooks[key] = {
        "LifecycleHookName": hook_name,
        "AutoScalingGroupName": asg_name,
        "LifecycleTransition": _p(p, "LifecycleTransition") or "autoscaling:EC2_INSTANCE_LAUNCHING",
        "HeartbeatTimeout": int(_p(p, "HeartbeatTimeout") or 3600),
        "DefaultResult": _p(p, "DefaultResult") or "ABANDON",
        "NotificationTargetARN": _p(p, "NotificationTargetARN") or "",
        "RoleARN": _p(p, "RoleARN") or "",
    }
    return _xml(200, "PutLifecycleHookResponse", "<PutLifecycleHookResult/>")


def _describe_lifecycle_hooks(p):
    asg_name = _p(p, "AutoScalingGroupName")
    members = ""
    for key, hook in _hooks.items():
        if hook["AutoScalingGroupName"] != asg_name:
            continue
        members += (f"<member>"
                    f"<LifecycleHookName>{hook['LifecycleHookName']}</LifecycleHookName>"
                    f"<AutoScalingGroupName>{hook['AutoScalingGroupName']}</AutoScalingGroupName>"
                    f"<LifecycleTransition>{hook['LifecycleTransition']}</LifecycleTransition>"
                    f"<HeartbeatTimeout>{hook['HeartbeatTimeout']}</HeartbeatTimeout>"
                    f"<DefaultResult>{hook['DefaultResult']}</DefaultResult>"
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

def _put_scheduled_action(p):
    asg_name = _p(p, "AutoScalingGroupName")
    action_name = _p(p, "ScheduledActionName")
    key = f"{asg_name}/{action_name}"
    arn = f"arn:aws:autoscaling:{get_region()}:{get_account_id()}:scheduledUpdateGroupAction:{new_uuid()}:autoScalingGroupName/{asg_name}:scheduledActionName/{action_name}"
    _scheduled_actions[key] = {
        "ScheduledActionARN": arn,
        "ScheduledActionName": action_name,
        "AutoScalingGroupName": asg_name,
        "Recurrence": _p(p, "Recurrence") or "",
        "MinSize": int(_p(p, "MinSize") or -1),
        "MaxSize": int(_p(p, "MaxSize") or -1),
        "DesiredCapacity": int(_p(p, "DesiredCapacity") or -1),
    }
    return _xml(200, "PutScheduledUpdateGroupActionResponse", "<PutScheduledUpdateGroupActionResult/>")


def _describe_scheduled_actions(p):
    asg_name = _p(p, "AutoScalingGroupName")
    members = ""
    for key, sa in _scheduled_actions.items():
        if asg_name and sa["AutoScalingGroupName"] != asg_name:
            continue
        members += (f"<member>"
                    f"<ScheduledActionARN>{sa['ScheduledActionARN']}</ScheduledActionARN>"
                    f"<ScheduledActionName>{sa['ScheduledActionName']}</ScheduledActionName>"
                    f"<AutoScalingGroupName>{sa['AutoScalingGroupName']}</AutoScalingGroupName>"
                    f"</member>")
    return _xml(200, "DescribeScheduledActionsResponse",
                f"<DescribeScheduledActionsResult><ScheduledUpdateGroupActions>{members}</ScheduledUpdateGroupActions></DescribeScheduledActionsResult>")


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
    members = ""
    for asg_name, tag_list in _tags.items():
        for t in tag_list:
            members += (f"<member>"
                        f"<Key>{t['Key']}</Key><Value>{t['Value']}</Value>"
                        f"<ResourceId>{t['ResourceId']}</ResourceId>"
                        f"<ResourceType>{t['ResourceType']}</ResourceType>"
                        f"<PropagateAtLaunch>{'true' if t.get('PropagateAtLaunch') else 'false'}</PropagateAtLaunch>"
                        f"</member>")
    return _xml(200, "DescribeTagsResponse",
                f"<DescribeTagsResult><Tags>{members}</Tags></DescribeTagsResult>")


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
