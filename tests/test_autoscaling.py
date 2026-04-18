import uuid

import pytest
from botocore.exceptions import ClientError


def _uid(prefix="test"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# AutoScalingGroup: Create, Describe, Update, Delete
# ---------------------------------------------------------------------------


def test_create_and_describe_asg(autoscaling):
    name = _uid("asg")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=1,
        MaxSize=5,
        DesiredCapacity=2,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
        groups = resp["AutoScalingGroups"]
        assert len(groups) == 1
        g = groups[0]
        assert g["AutoScalingGroupName"] == name
        assert g["MinSize"] == 1
        assert g["MaxSize"] == 5
        assert g["DesiredCapacity"] == 2
        assert g["AutoScalingGroupARN"].startswith("arn:aws:autoscaling:")
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)


def test_create_asg_duplicate_fails(autoscaling):
    name = _uid("asg-dup")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        with pytest.raises(ClientError) as exc:
            autoscaling.create_auto_scaling_group(
                AutoScalingGroupName=name,
                MinSize=0,
                MaxSize=1,
                AvailabilityZones=["us-east-1a"],
                LaunchConfigurationName="dummy-lc",
            )
        assert "AlreadyExists" in str(exc.value)
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)


def test_describe_asgs_empty(autoscaling):
    bogus = _uid("no-exist")
    resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[bogus])
    assert resp["AutoScalingGroups"] == []


def test_describe_asgs_all(autoscaling):
    names = [_uid("asg-all") for _ in range(3)]
    for n in names:
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=n,
            MinSize=0,
            MaxSize=1,
            AvailabilityZones=["us-east-1a"],
            LaunchConfigurationName="dummy-lc",
        )
    try:
        resp = autoscaling.describe_auto_scaling_groups()
        returned_names = [g["AutoScalingGroupName"] for g in resp["AutoScalingGroups"]]
        for n in names:
            assert n in returned_names
    finally:
        for n in names:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=n)


def test_update_asg(autoscaling):
    name = _uid("asg-upd")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=0,
        MaxSize=1,
        DesiredCapacity=0,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=name,
            MinSize=2,
            MaxSize=10,
            DesiredCapacity=5,
        )
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
        g = resp["AutoScalingGroups"][0]
        assert g["MinSize"] == 2
        assert g["MaxSize"] == 10
        assert g["DesiredCapacity"] == 5
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)


def test_update_nonexistent_asg_fails(autoscaling):
    with pytest.raises(ClientError) as exc:
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName="nonexistent-asg",
            MinSize=1,
        )
    assert "ValidationError" in str(exc.value) or "not found" in str(exc.value).lower()


def test_delete_asg(autoscaling):
    name = _uid("asg-del")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)
    resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
    assert resp["AutoScalingGroups"] == []


def test_delete_asg_idempotent(autoscaling):
    """Deleting a non-existent ASG should not error."""
    autoscaling.delete_auto_scaling_group(AutoScalingGroupName=_uid("ghost"))


# ---------------------------------------------------------------------------
# DescribeAutoScalingInstances
# ---------------------------------------------------------------------------


def test_describe_auto_scaling_instances_empty(autoscaling):
    resp = autoscaling.describe_auto_scaling_instances()
    assert resp["AutoScalingInstances"] == []


# ---------------------------------------------------------------------------
# DescribeScalingActivities
# ---------------------------------------------------------------------------


def test_describe_scaling_activities_empty(autoscaling):
    resp = autoscaling.describe_scaling_activities()
    assert resp["Activities"] == []


def test_describe_scaling_activities_for_asg(autoscaling):
    name = _uid("asg-act")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=0,
        MaxSize=1,
        DesiredCapacity=0,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        resp = autoscaling.describe_scaling_activities(AutoScalingGroupName=name)
        assert resp["Activities"] == []
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)


# ---------------------------------------------------------------------------
# LaunchConfiguration: Create, Describe, Delete
# ---------------------------------------------------------------------------


def test_create_and_describe_launch_configuration(autoscaling):
    name = _uid("lc")
    autoscaling.create_launch_configuration(
        LaunchConfigurationName=name,
        ImageId="ami-12345678",
        InstanceType="t3.micro",
    )
    try:
        resp = autoscaling.describe_launch_configurations(
            LaunchConfigurationNames=[name]
        )
        configs = resp["LaunchConfigurations"]
        assert len(configs) == 1
        lc = configs[0]
        assert lc["LaunchConfigurationName"] == name
        assert lc["ImageId"] == "ami-12345678"
        assert lc["InstanceType"] == "t3.micro"
        assert lc["LaunchConfigurationARN"].startswith("arn:aws:autoscaling:")
    finally:
        autoscaling.delete_launch_configuration(LaunchConfigurationName=name)


def test_create_launch_configuration_duplicate_fails(autoscaling):
    name = _uid("lc-dup")
    autoscaling.create_launch_configuration(
        LaunchConfigurationName=name,
        ImageId="ami-00000000",
        InstanceType="t2.micro",
    )
    try:
        with pytest.raises(ClientError) as exc:
            autoscaling.create_launch_configuration(
                LaunchConfigurationName=name,
                ImageId="ami-00000000",
                InstanceType="t2.micro",
            )
        assert "AlreadyExists" in str(exc.value)
    finally:
        autoscaling.delete_launch_configuration(LaunchConfigurationName=name)


def test_describe_launch_configurations_empty(autoscaling):
    resp = autoscaling.describe_launch_configurations(
        LaunchConfigurationNames=[_uid("no-lc")]
    )
    assert resp["LaunchConfigurations"] == []


def test_delete_launch_configuration(autoscaling):
    name = _uid("lc-del")
    autoscaling.create_launch_configuration(
        LaunchConfigurationName=name,
        ImageId="ami-00000000",
        InstanceType="t2.micro",
    )
    autoscaling.delete_launch_configuration(LaunchConfigurationName=name)
    resp = autoscaling.describe_launch_configurations(
        LaunchConfigurationNames=[name]
    )
    assert resp["LaunchConfigurations"] == []


def test_delete_launch_configuration_idempotent(autoscaling):
    autoscaling.delete_launch_configuration(LaunchConfigurationName=_uid("ghost-lc"))


# ---------------------------------------------------------------------------
# Scaling Policies: Put, Describe, Delete
# ---------------------------------------------------------------------------


def test_put_and_describe_scaling_policy(autoscaling):
    asg = _uid("asg-pol")
    pol = _uid("pol")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=10,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        resp = autoscaling.put_scaling_policy(
            AutoScalingGroupName=asg,
            PolicyName=pol,
            PolicyType="SimpleScaling",
            AdjustmentType="ChangeInCapacity",
            ScalingAdjustment=2,
            Cooldown=60,
        )
        assert "PolicyARN" in resp
        assert resp["PolicyARN"].startswith("arn:aws:autoscaling:")

        desc = autoscaling.describe_policies(AutoScalingGroupName=asg)
        policies = desc["ScalingPolicies"]
        assert len(policies) >= 1
        found = [p for p in policies if p["PolicyName"] == pol]
        assert len(found) == 1
        assert found[0]["AdjustmentType"] == "ChangeInCapacity"
        assert found[0]["ScalingAdjustment"] == 2
    finally:
        autoscaling.delete_policy(AutoScalingGroupName=asg, PolicyName=pol)
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_describe_policies_empty(autoscaling):
    resp = autoscaling.describe_policies(AutoScalingGroupName=_uid("no-asg"))
    assert resp["ScalingPolicies"] == []


def test_delete_policy(autoscaling):
    asg = _uid("asg-dpol")
    pol = _uid("dpol")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=asg,
            PolicyName=pol,
            AdjustmentType="ChangeInCapacity",
            ScalingAdjustment=1,
        )
        autoscaling.delete_policy(AutoScalingGroupName=asg, PolicyName=pol)
        desc = autoscaling.describe_policies(AutoScalingGroupName=asg)
        names = [p["PolicyName"] for p in desc["ScalingPolicies"]]
        assert pol not in names
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_delete_policy_idempotent(autoscaling):
    autoscaling.delete_policy(
        AutoScalingGroupName="ghost-asg",
        PolicyName="ghost-pol",
    )


# ---------------------------------------------------------------------------
# Lifecycle Hooks: Put, Describe, Delete, Complete, Heartbeat
# ---------------------------------------------------------------------------


def test_put_and_describe_lifecycle_hook(autoscaling):
    asg = _uid("asg-hook")
    hook = _uid("hook")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.put_lifecycle_hook(
            AutoScalingGroupName=asg,
            LifecycleHookName=hook,
            LifecycleTransition="autoscaling:EC2_INSTANCE_LAUNCHING",
            HeartbeatTimeout=300,
            DefaultResult="CONTINUE",
        )
        resp = autoscaling.describe_lifecycle_hooks(AutoScalingGroupName=asg)
        hooks = resp["LifecycleHooks"]
        assert len(hooks) >= 1
        found = [h for h in hooks if h["LifecycleHookName"] == hook]
        assert len(found) == 1
        assert found[0]["LifecycleTransition"] == "autoscaling:EC2_INSTANCE_LAUNCHING"
        assert found[0]["DefaultResult"] == "CONTINUE"
        assert found[0]["HeartbeatTimeout"] == 300
    finally:
        autoscaling.delete_lifecycle_hook(
            AutoScalingGroupName=asg, LifecycleHookName=hook
        )
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_describe_lifecycle_hooks_empty(autoscaling):
    asg = _uid("asg-nohook")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        resp = autoscaling.describe_lifecycle_hooks(AutoScalingGroupName=asg)
        assert resp["LifecycleHooks"] == []
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_delete_lifecycle_hook(autoscaling):
    asg = _uid("asg-dhook")
    hook = _uid("dhook")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.put_lifecycle_hook(
            AutoScalingGroupName=asg,
            LifecycleHookName=hook,
            LifecycleTransition="autoscaling:EC2_INSTANCE_TERMINATING",
        )
        autoscaling.delete_lifecycle_hook(
            AutoScalingGroupName=asg, LifecycleHookName=hook
        )
        resp = autoscaling.describe_lifecycle_hooks(AutoScalingGroupName=asg)
        names = [h["LifecycleHookName"] for h in resp["LifecycleHooks"]]
        assert hook not in names
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_delete_lifecycle_hook_idempotent(autoscaling):
    autoscaling.delete_lifecycle_hook(
        AutoScalingGroupName="ghost-asg",
        LifecycleHookName="ghost-hook",
    )


def test_complete_lifecycle_action(autoscaling):
    asg = _uid("asg-cla")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        resp = autoscaling.complete_lifecycle_action(
            AutoScalingGroupName=asg,
            LifecycleHookName="any-hook",
            LifecycleActionResult="CONTINUE",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_record_lifecycle_action_heartbeat(autoscaling):
    asg = _uid("asg-hb")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        resp = autoscaling.record_lifecycle_action_heartbeat(
            AutoScalingGroupName=asg,
            LifecycleHookName="any-hook",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_delete_asg_cleans_up_hooks(autoscaling):
    """Deleting an ASG should also remove its lifecycle hooks."""
    asg = _uid("asg-hclean")
    hook = _uid("hclean")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    autoscaling.put_lifecycle_hook(
        AutoScalingGroupName=asg,
        LifecycleHookName=hook,
        LifecycleTransition="autoscaling:EC2_INSTANCE_LAUNCHING",
    )
    autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)
    # Re-create the ASG to query hooks — should be empty
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        resp = autoscaling.describe_lifecycle_hooks(AutoScalingGroupName=asg)
        names = [h["LifecycleHookName"] for h in resp["LifecycleHooks"]]
        assert hook not in names
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


# ---------------------------------------------------------------------------
# Scheduled Actions: Put, Describe, Delete
# ---------------------------------------------------------------------------


def test_put_and_describe_scheduled_action(autoscaling):
    asg = _uid("asg-sched")
    action = _uid("sched")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=10,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.put_scheduled_update_group_action(
            AutoScalingGroupName=asg,
            ScheduledActionName=action,
            Recurrence="0 8 * * *",
            MinSize=2,
            MaxSize=8,
            DesiredCapacity=4,
        )
        resp = autoscaling.describe_scheduled_actions(AutoScalingGroupName=asg)
        actions = resp["ScheduledUpdateGroupActions"]
        assert len(actions) >= 1
        found = [a for a in actions if a["ScheduledActionName"] == action]
        assert len(found) == 1
        assert found[0]["ScheduledActionARN"].startswith("arn:aws:autoscaling:")
    finally:
        autoscaling.delete_scheduled_action(
            AutoScalingGroupName=asg, ScheduledActionName=action
        )
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_describe_scheduled_actions_empty(autoscaling):
    resp = autoscaling.describe_scheduled_actions(
        AutoScalingGroupName=_uid("no-sched")
    )
    assert resp["ScheduledUpdateGroupActions"] == []


def test_delete_scheduled_action(autoscaling):
    asg = _uid("asg-dsched")
    action = _uid("dsched")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.put_scheduled_update_group_action(
            AutoScalingGroupName=asg,
            ScheduledActionName=action,
            MinSize=1,
            MaxSize=5,
        )
        autoscaling.delete_scheduled_action(
            AutoScalingGroupName=asg, ScheduledActionName=action
        )
        resp = autoscaling.describe_scheduled_actions(AutoScalingGroupName=asg)
        names = [a["ScheduledActionName"] for a in resp["ScheduledUpdateGroupActions"]]
        assert action not in names
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_delete_scheduled_action_idempotent(autoscaling):
    autoscaling.delete_scheduled_action(
        AutoScalingGroupName="ghost-asg",
        ScheduledActionName="ghost-action",
    )


# ---------------------------------------------------------------------------
# Tags: CreateOrUpdate, Describe, Delete
# ---------------------------------------------------------------------------


def test_create_or_update_tags_and_describe(autoscaling):
    asg = _uid("asg-tag")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.create_or_update_tags(
            Tags=[
                {
                    "ResourceId": asg,
                    "ResourceType": "auto-scaling-group",
                    "Key": "Environment",
                    "Value": "test",
                    "PropagateAtLaunch": True,
                },
                {
                    "ResourceId": asg,
                    "ResourceType": "auto-scaling-group",
                    "Key": "Team",
                    "Value": "platform",
                    "PropagateAtLaunch": False,
                },
            ]
        )
        resp = autoscaling.describe_tags()
        all_tags = resp["Tags"]
        my_tags = [t for t in all_tags if t["ResourceId"] == asg]
        assert len(my_tags) == 2
        keys = {t["Key"] for t in my_tags}
        assert keys == {"Environment", "Team"}
    finally:
        autoscaling.delete_tags(
            Tags=[
                {"ResourceId": asg, "ResourceType": "auto-scaling-group", "Key": "Environment"},
                {"ResourceId": asg, "ResourceType": "auto-scaling-group", "Key": "Team"},
            ]
        )
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_update_existing_tag(autoscaling):
    asg = _uid("asg-tagup")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.create_or_update_tags(
            Tags=[
                {
                    "ResourceId": asg,
                    "ResourceType": "auto-scaling-group",
                    "Key": "Version",
                    "Value": "v1",
                    "PropagateAtLaunch": False,
                },
            ]
        )
        # Update same key with new value
        autoscaling.create_or_update_tags(
            Tags=[
                {
                    "ResourceId": asg,
                    "ResourceType": "auto-scaling-group",
                    "Key": "Version",
                    "Value": "v2",
                    "PropagateAtLaunch": True,
                },
            ]
        )
        resp = autoscaling.describe_tags()
        my_tags = [t for t in resp["Tags"] if t["ResourceId"] == asg and t["Key"] == "Version"]
        assert len(my_tags) == 1
        assert my_tags[0]["Value"] == "v2"
    finally:
        autoscaling.delete_tags(
            Tags=[{"ResourceId": asg, "ResourceType": "auto-scaling-group", "Key": "Version"}]
        )
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_delete_tags(autoscaling):
    asg = _uid("asg-dtag")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.create_or_update_tags(
            Tags=[
                {
                    "ResourceId": asg,
                    "ResourceType": "auto-scaling-group",
                    "Key": "Ephemeral",
                    "Value": "yes",
                    "PropagateAtLaunch": False,
                },
            ]
        )
        autoscaling.delete_tags(
            Tags=[
                {
                    "ResourceId": asg,
                    "ResourceType": "auto-scaling-group",
                    "Key": "Ephemeral",
                },
            ]
        )
        resp = autoscaling.describe_tags()
        my_tags = [t for t in resp["Tags"] if t["ResourceId"] == asg and t["Key"] == "Ephemeral"]
        assert my_tags == []
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_tags_reflect_in_asg_describe(autoscaling):
    """Tags added via CreateOrUpdateTags should appear in DescribeAutoScalingGroups."""
    asg = _uid("asg-tagsync")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.create_or_update_tags(
            Tags=[
                {
                    "ResourceId": asg,
                    "ResourceType": "auto-scaling-group",
                    "Key": "Sync",
                    "Value": "check",
                    "PropagateAtLaunch": False,
                },
            ]
        )
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[asg])
        asg_tags = resp["AutoScalingGroups"][0]["Tags"]
        keys = [t["Key"] for t in asg_tags]
        assert "Sync" in keys
    finally:
        autoscaling.delete_tags(
            Tags=[{"ResourceId": asg, "ResourceType": "auto-scaling-group", "Key": "Sync"}]
        )
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_create_asg_with_inline_tags(autoscaling):
    """Tags passed at ASG creation time should be visible."""
    asg = _uid("asg-itag")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
        Tags=[
            {
                "ResourceId": asg,
                "ResourceType": "auto-scaling-group",
                "Key": "Inline",
                "Value": "yes",
                "PropagateAtLaunch": True,
            }
        ],
    )
    try:
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[asg])
        tags = resp["AutoScalingGroups"][0]["Tags"]
        assert any(t["Key"] == "Inline" and t["Value"] == "yes" for t in tags)

        # Also visible in DescribeTags
        tresp = autoscaling.describe_tags()
        my = [t for t in tresp["Tags"] if t["ResourceId"] == asg and t["Key"] == "Inline"]
        assert len(my) == 1
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


# ---------------------------------------------------------------------------
# Multiple policies on same ASG
# ---------------------------------------------------------------------------


def test_multiple_policies_on_same_asg(autoscaling):
    asg = _uid("asg-mpol")
    p1 = _uid("scale-up")
    p2 = _uid("scale-down")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=10,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=asg,
            PolicyName=p1,
            AdjustmentType="ChangeInCapacity",
            ScalingAdjustment=2,
        )
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=asg,
            PolicyName=p2,
            AdjustmentType="ChangeInCapacity",
            ScalingAdjustment=-1,
        )
        desc = autoscaling.describe_policies(AutoScalingGroupName=asg)
        names = [p["PolicyName"] for p in desc["ScalingPolicies"]]
        assert p1 in names
        assert p2 in names
    finally:
        autoscaling.delete_policy(AutoScalingGroupName=asg, PolicyName=p1)
        autoscaling.delete_policy(AutoScalingGroupName=asg, PolicyName=p2)
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


# ---------------------------------------------------------------------------
# Multiple hooks on same ASG
# ---------------------------------------------------------------------------


def test_multiple_hooks_on_same_asg(autoscaling):
    asg = _uid("asg-mhook")
    h1 = _uid("launch-hook")
    h2 = _uid("term-hook")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.put_lifecycle_hook(
            AutoScalingGroupName=asg,
            LifecycleHookName=h1,
            LifecycleTransition="autoscaling:EC2_INSTANCE_LAUNCHING",
        )
        autoscaling.put_lifecycle_hook(
            AutoScalingGroupName=asg,
            LifecycleHookName=h2,
            LifecycleTransition="autoscaling:EC2_INSTANCE_TERMINATING",
        )
        resp = autoscaling.describe_lifecycle_hooks(AutoScalingGroupName=asg)
        names = [h["LifecycleHookName"] for h in resp["LifecycleHooks"]]
        assert h1 in names
        assert h2 in names
    finally:
        autoscaling.delete_lifecycle_hook(AutoScalingGroupName=asg, LifecycleHookName=h1)
        autoscaling.delete_lifecycle_hook(AutoScalingGroupName=asg, LifecycleHookName=h2)
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


# ---------------------------------------------------------------------------
# Multiple scheduled actions on same ASG
# ---------------------------------------------------------------------------


def test_multiple_scheduled_actions(autoscaling):
    asg = _uid("asg-msched")
    a1 = _uid("morning")
    a2 = _uid("evening")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=10,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.put_scheduled_update_group_action(
            AutoScalingGroupName=asg,
            ScheduledActionName=a1,
            Recurrence="0 8 * * *",
            MinSize=5,
            MaxSize=10,
        )
        autoscaling.put_scheduled_update_group_action(
            AutoScalingGroupName=asg,
            ScheduledActionName=a2,
            Recurrence="0 20 * * *",
            MinSize=1,
            MaxSize=3,
        )
        resp = autoscaling.describe_scheduled_actions(AutoScalingGroupName=asg)
        names = [a["ScheduledActionName"] for a in resp["ScheduledUpdateGroupActions"]]
        assert a1 in names
        assert a2 in names
    finally:
        autoscaling.delete_scheduled_action(AutoScalingGroupName=asg, ScheduledActionName=a1)
        autoscaling.delete_scheduled_action(AutoScalingGroupName=asg, ScheduledActionName=a2)
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


# ---------------------------------------------------------------------------
# ASG with launch template reference
# ---------------------------------------------------------------------------


def test_create_asg_with_launch_template(autoscaling):
    asg = _uid("asg-lt")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchTemplate={
            "LaunchTemplateName": "my-template",
            "Version": "$Latest",
        },
    )
    try:
        resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[asg])
        g = resp["AutoScalingGroups"][0]
        lt = g.get("LaunchTemplate", {})
        assert lt.get("LaunchTemplateName") == "my-template"
        assert lt.get("Version") == "$Latest"
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)
