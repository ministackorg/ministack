"""
AutoScaling Service Emulator.
Query API (Action=...) — groups, launch configs, policies, hooks, scheduled actions.
All in-memory, no actual instance scaling.

Supports:
  ASG:       CreateAutoScalingGroup, DescribeAutoScalingGroups, UpdateAutoScalingGroup,
             DeleteAutoScalingGroup, DescribeAutoScalingInstances, DescribeScalingActivities
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

from ministack.core.responses import AccountScopedDict, get_account_id, new_uuid, now_iso, get_region
from ministack.core.persistence import load_state

logger = logging.getLogger("autoscaling")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_asgs = AccountScopedDict()
_launch_configs = AccountScopedDict()
_policies = AccountScopedDict()
_hooks = AccountScopedDict()
_scheduled_actions = AccountScopedDict()
_tags = AccountScopedDict()  # asg_name -> [{"Key":..., "Value":...}, ...]


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
    if data:
        _asgs.update(data.get("asgs", {}))
        _launch_configs.update(data.get("launch_configs", {}))
        _policies.update(data.get("policies", {}))
        _hooks.update(data.get("hooks", {}))
        _scheduled_actions.update(data.get("scheduled_actions", {}))
        _tags.update(data.get("tags", {}))


try:
    _restored = load_state("autoscaling")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted autoscaling state; continuing fresh")


def reset():
    _asgs.clear()
    _launch_configs.clear()
    _policies.clear()
    _hooks.clear()
    _scheduled_actions.clear()
    _tags.clear()


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
# ---------------------------------------------------------------------------

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
                    f"<Instances/>"
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
    return _xml(200, "UpdateAutoScalingGroupResponse", "<UpdateAutoScalingGroupResult/>")


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
    return _xml(200, "DescribeAutoScalingInstancesResponse",
                "<DescribeAutoScalingInstancesResult><AutoScalingInstances/></DescribeAutoScalingInstancesResult>")


def _describe_scaling_activities(p):
    return _xml(200, "DescribeScalingActivitiesResponse",
                "<DescribeScalingActivitiesResult><Activities/></DescribeScalingActivitiesResult>")


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
    "DescribeAutoScalingInstances": _describe_asg_instances,
    "DescribeScalingActivities": _describe_scaling_activities,
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
