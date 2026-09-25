"""CloudFormation updates of Auto Scaling groups, scaling policies and scheduled actions."""

import json
import uuid as _uuid_mod

from test_cfn import (
    _FAILING_RESOURCE,
    _cfn_output,
    _cfn_with_failing_resource,
    _delete_cfn_test_stack,
    _wait_stack,
)


def test_cfn_asg_update_keeps_arn_and_created_time(cfn, autoscaling):
    """MinSize, MaxSize and DesiredCapacity are No interruption on
    AWS::AutoScaling::AutoScalingGroup. The create mints a fresh ARN, resets
    CreatedTime and empties the Instances list, so the fallback handed every
    consumer of the ARN a new value and dropped the group's instances."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-{suffix}"
    group = f"cfn-asg-{suffix}"

    def template(max_size):
        return json.dumps({
            "Resources": {
                "LC": {
                    "Type": "AWS::AutoScaling::LaunchConfiguration",
                    "Properties": {"ImageId": "ami-12345678",
                                   "InstanceType": "t3.micro"},
                },
                "ASG": {
                    "Type": "AWS::AutoScaling::AutoScalingGroup",
                    "Properties": {
                        "AutoScalingGroupName": group,
                        "LaunchConfigurationName": {"Ref": "LC"},
                        "MinSize": "1",
                        "MaxSize": str(max_size),
                        "AvailabilityZones": ["us-east-1a"],
                    },
                },
            },
        })

    def described():
        return autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[group])["AutoScalingGroups"][0]

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template(3))
        _wait_stack(cfn, stack_name)
        before = described()

        cfn.update_stack(StackName=stack_name, TemplateBody=template(5))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        after = described()
        assert after["MaxSize"] == 5
        assert after["AutoScalingGroupARN"] == before["AutoScalingGroupARN"], \
            "the group came back under a new ARN"
        assert after["CreatedTime"] == before["CreatedTime"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_scaling_policy_update_keeps_arn(cfn, autoscaling):
    """Cooldown and ScalingAdjustment are No interruption on
    AWS::AutoScaling::ScalingPolicy, and Ref answers the policy ARN, which the
    create re-minted on every change."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-pol-{suffix}"
    group = f"cfn-asg-pol-{suffix}"

    def template(cooldown):
        return json.dumps({
            "Resources": {
                "LC": {
                    "Type": "AWS::AutoScaling::LaunchConfiguration",
                    "Properties": {"ImageId": "ami-12345678",
                                   "InstanceType": "t3.micro"},
                },
                "ASG": {
                    "Type": "AWS::AutoScaling::AutoScalingGroup",
                    "Properties": {
                        "AutoScalingGroupName": group,
                        "LaunchConfigurationName": {"Ref": "LC"},
                        "MinSize": "1", "MaxSize": "3",
                        "AvailabilityZones": ["us-east-1a"],
                    },
                },
                "Policy": {
                    "Type": "AWS::AutoScaling::ScalingPolicy",
                    "Properties": {
                        # No PolicyName, so the name is generated.
                        "AutoScalingGroupName": {"Ref": "ASG"},
                        "AdjustmentType": "ChangeInCapacity",
                        "ScalingAdjustment": 1,
                        "Cooldown": str(cooldown),
                    },
                },
            },
            "Outputs": {"PolicyArn": {"Value": {"Ref": "Policy"}}},
        })

    def policy_arn():
        return next(
            o["OutputValue"]
            for o in cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
            if o["OutputKey"] == "PolicyArn")

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template(300))
        _wait_stack(cfn, stack_name)
        before = policy_arn()

        cfn.update_stack(StackName=stack_name, TemplateBody=template(600))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        policies = autoscaling.describe_policies(
            AutoScalingGroupName=group)["ScalingPolicies"]
        assert len(policies) == 1
        assert policies[0]["Cooldown"] == 600
        assert policies[0]["PolicyARN"] == before, \
            "the policy came back under a new ARN"
        assert policies[0]["PolicyName"].startswith(f"{stack_name}-Policy-")
        assert policy_arn() == before
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_scheduled_action_update_keeps_arn(cfn, autoscaling):
    """MinSize, MaxSize, DesiredCapacity and Recurrence are No interruption on
    AWS::AutoScaling::ScheduledAction; the create re-minted the ARN Ref
    answers."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-sched-{suffix}"
    group = f"cfn-asg-sched-{suffix}"

    def template(max_size):
        return json.dumps({
            "Resources": {
                "LC": {
                    "Type": "AWS::AutoScaling::LaunchConfiguration",
                    "Properties": {"ImageId": "ami-12345678",
                                   "InstanceType": "t3.micro"},
                },
                "ASG": {
                    "Type": "AWS::AutoScaling::AutoScalingGroup",
                    "Properties": {
                        "AutoScalingGroupName": group,
                        "LaunchConfigurationName": {"Ref": "LC"},
                        "MinSize": "1", "MaxSize": "9",
                        "AvailabilityZones": ["us-east-1a"],
                    },
                },
                "Sched": {
                    "Type": "AWS::AutoScaling::ScheduledAction",
                    "Properties": {
                        "AutoScalingGroupName": {"Ref": "ASG"},
                        "Recurrence": "0 9 * * *",
                        "MinSize": 1,
                        "MaxSize": max_size,
                    },
                },
            },
            "Outputs": {"Sched": {"Value": {"Ref": "Sched"}}},
        })

    def action():
        actions = autoscaling.describe_scheduled_actions(
            AutoScalingGroupName=group)["ScheduledUpdateGroupActions"]
        assert len(actions) == 1
        return actions[0]

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template(4))
        _wait_stack(cfn, stack_name)
        before = action()["ScheduledActionARN"]

        cfn.update_stack(StackName=stack_name, TemplateBody=template(6))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        after = action()
        assert after["MaxSize"] == 6
        assert after["ScheduledActionARN"] == before, \
            "the scheduled action came back under a new ARN"
        assert _cfn_output(cfn, stack_name, "Sched") == after["ScheduledActionName"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _cfn_asg_policy_template(group, policies):
    return json.dumps({
        "Resources": {
            "LC": {"Type": "AWS::AutoScaling::LaunchConfiguration",
                   "Properties": {"ImageId": "ami-12345678", "InstanceType": "t3.micro"}},
            "ASG": {"Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": {
                "AutoScalingGroupName": group, "LaunchConfigurationName": {"Ref": "LC"},
                "MinSize": "0", "MaxSize": "1", "DesiredCapacity": "0",
                "AvailabilityZones": ["us-east-1a"]}},
            **{name: {"Type": "AWS::AutoScaling::ScalingPolicy",
                      "Properties": {"AutoScalingGroupName": {"Ref": "ASG"}, **props}}
               for name, props in policies.items()},
        },
        "Outputs": {name: {"Value": {"Ref": name}} for name in policies},
    })


def _cfn_asg_policies_by_arn(autoscaling, group):
    return {p["PolicyARN"]: p for p in autoscaling.describe_policies(
        AutoScalingGroupName=group)["ScalingPolicies"]}


def test_cfn_asg_target_tracking_policy_reads_back_and_updates_in_place(cfn, autoscaling):
    """TargetTrackingConfiguration is No interruption on
    AWS::AutoScaling::ScalingPolicy, and it was dropped by the create, the
    update and DescribePolicies alike, so the policy read back without the
    one member it is for. The shape is AWS's (measured 2026-09-21):
    TargetValue a double, DisableScaleIn false when unset, and no
    AdjustmentType, ScalingAdjustment or Cooldown on a target-tracking
    policy; round 51 measured TargetValue 40.0 -> 60.0 under an unchanged
    ARN."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-tt-{suffix}"
    group = f"cfn-asg-tt-{suffix}"

    def template(target):
        return _cfn_asg_policy_template(group, {"Tt": {
            "PolicyType": "TargetTrackingScaling",
            "EstimatedInstanceWarmup": 120,
            "TargetTrackingConfiguration": {
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": "ASGAverageCPUUtilization"},
                "TargetValue": target}}})

    cfn.create_stack(StackName=stack_name, TemplateBody=template(40))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        arn = _cfn_output(cfn, stack_name, "Tt")
        policy = _cfn_asg_policies_by_arn(autoscaling, group)[arn]
        assert policy["PolicyName"].startswith(f"{stack_name}-Tt-")
        assert policy["TargetTrackingConfiguration"] == {
            "PredefinedMetricSpecification": {
                "PredefinedMetricType": "ASGAverageCPUUtilization"},
            "TargetValue": 40.0,
            "DisableScaleIn": False,
        }
        assert policy["EstimatedInstanceWarmup"] == 120
        assert policy["Enabled"] is True
        assert not {"AdjustmentType", "ScalingAdjustment", "Cooldown"} & set(policy)

        cfn.update_stack(StackName=stack_name, TemplateBody=template(60))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_output(cfn, stack_name, "Tt") == arn
        policy = _cfn_asg_policies_by_arn(autoscaling, group)[arn]
        assert policy["TargetTrackingConfiguration"]["TargetValue"] == 60.0
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_scaling_policy_keeps_a_template_policy_name(cfn, autoscaling):
    """The registry lists PolicyName as read-only on
    AWS::AutoScaling::ScalingPolicy, but AWS applies one the template sets
    (measured 2026-09-21: the policy carries exactly that name), so the
    provisioner keeps honouring it. A guard, not a fix: main honours it too."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-pname-{suffix}"
    group = f"cfn-asg-pname-{suffix}"
    cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_asg_policy_template(group, {"Named": {
        "PolicyName": f"named-{suffix}", "AdjustmentType": "ChangeInCapacity",
        "ScalingAdjustment": 1}}))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        policies = autoscaling.describe_policies(AutoScalingGroupName=group)["ScalingPolicies"]
        assert [p["PolicyName"] for p in policies] == [f"named-{suffix}"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_scaling_policy_name_change_is_not_applied(cfn, autoscaling):
    """PolicyName is honoured when the policy is created and never after:
    AWS keeps the policy's name and ARN when the template changes the name or
    drops it (measured 2026-09-21, UPDATE_COMPLETE both times). The handler
    keyed a replacement on it, so the policy came back renamed under a new
    ARN."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-pren-{suffix}"
    group = f"cfn-asg-pren-{suffix}"

    def template(name):
        props = {"AdjustmentType": "ChangeInCapacity", "ScalingAdjustment": 1}
        if name:
            props["PolicyName"] = name
        return _cfn_asg_policy_template(group, {"Policy": props})

    cfn.create_stack(StackName=stack_name, TemplateBody=template(f"pa-{suffix}"))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        arn = _cfn_output(cfn, stack_name, "Policy")
        for name in (f"pb-{suffix}", None):
            cfn.update_stack(StackName=stack_name, TemplateBody=template(name))
            stack = _wait_stack(cfn, stack_name)
            assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
            assert _cfn_output(cfn, stack_name, "Policy") == arn
            policies = autoscaling.describe_policies(AutoScalingGroupName=group)["ScalingPolicies"]
            assert [(p["PolicyName"], p["PolicyARN"]) for p in policies] == [(f"pa-{suffix}", arn)]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _cfn_asg_scheduled_template(group, props):
    return json.dumps({
        "Resources": {
            "LC": {"Type": "AWS::AutoScaling::LaunchConfiguration",
                   "Properties": {"ImageId": "ami-12345678", "InstanceType": "t3.micro"}},
            "ASG": {"Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": {
                "AutoScalingGroupName": group, "LaunchConfigurationName": {"Ref": "LC"},
                "MinSize": "0", "MaxSize": "1", "DesiredCapacity": "0",
                "AvailabilityZones": ["us-east-1a"]}},
            "Sched": {"Type": "AWS::AutoScaling::ScheduledAction", "Properties": {
                "AutoScalingGroupName": {"Ref": "ASG"}, "MinSize": 0, "MaxSize": 1,
                "DesiredCapacity": 0, "Recurrence": "0 9 * * *", **props}},
        },
        "Outputs": {"Sched": {"Value": {"Ref": "Sched"}},
                    "Name": {"Value": {"Fn::GetAtt": ["Sched", "ScheduledActionName"]}}},
    })


def test_cfn_asg_scheduled_action_is_named_by_cloudformation(cfn, autoscaling):
    """CloudFormation names a scheduled action itself and Ref answers that
    name: the registry lists ScheduledActionName as read-only, and AWS ignores
    one the template sets, at create and on every update, with the ARN and
    name unchanged (measured 2026-09-21). The provisioner used the template's
    name, answered the ARN through Ref and replaced the action when the name
    changed."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-sname-{suffix}"
    group = f"cfn-asg-sname-{suffix}"

    def action():
        actions = autoscaling.describe_scheduled_actions(
            AutoScalingGroupName=group)["ScheduledUpdateGroupActions"]
        assert len(actions) == 1
        return actions[0]

    cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_asg_scheduled_template(
        group, {"ScheduledActionName": f"sa-{suffix}"}))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        created = action()
        assert created["ScheduledActionName"].startswith(f"{stack_name}-Sched-")
        assert _cfn_output(cfn, stack_name, "Sched") == created["ScheduledActionName"]
        assert _cfn_output(cfn, stack_name, "Name") == created["ScheduledActionName"]
        for props in ({"ScheduledActionName": f"sb-{suffix}"}, {}):
            cfn.update_stack(StackName=stack_name,
                             TemplateBody=_cfn_asg_scheduled_template(group, props))
            stack = _wait_stack(cfn, stack_name)
            assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
            assert action() == created
            assert _cfn_output(cfn, stack_name, "Sched") == created["ScheduledActionName"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
    assert autoscaling.describe_scheduled_actions(
        AutoScalingGroupName=group)["ScheduledUpdateGroupActions"] == []


def test_cfn_asg_scheduled_action_times_read_back_and_update(cfn, autoscaling):
    """StartTime, EndTime and TimeZone are No interruption on
    AWS::AutoScaling::ScheduledAction. AWS answers all three (StartTime also
    as Time), applies each change in place and drops EndTime when the
    template removes it (measured 2026-09-21); the provisioner and
    PutScheduledUpdateGroupAction stored none of them."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-stimes-{suffix}"
    group = f"cfn-asg-stimes-{suffix}"

    def times():
        action = autoscaling.describe_scheduled_actions(
            AutoScalingGroupName=group)["ScheduledUpdateGroupActions"][0]
        return {key: (value.isoformat() if hasattr(value, "isoformat") else value)
                for key, value in action.items()
                if key in ("StartTime", "Time", "EndTime", "TimeZone", "ScheduledActionARN")}

    start = {"StartTime": "2027-01-04T09:00:00Z"}
    cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_asg_scheduled_template(group, {
        **start, "EndTime": "2027-06-01T00:00:00Z", "TimeZone": "Europe/Berlin"}))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        before = times()
        arn = before.pop("ScheduledActionARN")
        assert before == {"StartTime": "2027-01-04T09:00:00+00:00",
                          "Time": "2027-01-04T09:00:00+00:00",
                          "EndTime": "2027-06-01T00:00:00+00:00",
                          "TimeZone": "Europe/Berlin"}
        steps = [
            ({**start, "EndTime": "2027-07-01T00:00:00Z", "TimeZone": "Europe/Berlin"},
             {"EndTime": "2027-07-01T00:00:00+00:00"}),
            ({**start, "EndTime": "2027-07-01T00:00:00Z", "TimeZone": "Etc/UTC"},
             {"TimeZone": "Etc/UTC"}),
            ({"StartTime": "2027-02-01T09:00:00Z", "EndTime": "2027-07-01T00:00:00Z",
              "TimeZone": "Etc/UTC"},
             {"StartTime": "2027-02-01T09:00:00+00:00", "Time": "2027-02-01T09:00:00+00:00"}),
            ({"StartTime": "2027-02-01T09:00:00Z", "TimeZone": "Etc/UTC"}, {"EndTime": None}),
        ]
        for props, change in steps:
            cfn.update_stack(StackName=stack_name,
                             TemplateBody=_cfn_asg_scheduled_template(group, props))
            assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
            before.update(change)
            after = times()
            assert after.pop("ScheduledActionARN") == arn
            assert after == {k: v for k, v in before.items() if v is not None}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_predictive_scaling_policy_reads_back_and_updates_in_place(cfn, autoscaling):
    """PredictiveScalingConfiguration is No interruption. AWS answers it with
    MaxCapacityBreachBehavior defaulted to HonorMaxCapacity and TargetValue as
    a double, and a TargetValue change keeps the ARN (measured 2026-09-21);
    the configuration was dropped by the provisioner, PutScalingPolicy and
    DescribePolicies alike."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-pred-{suffix}"
    group = f"cfn-asg-pred-{suffix}"

    def template(target):
        return _cfn_asg_policy_template(group, {"Pred": {
            "PolicyType": "PredictiveScaling",
            "PredictiveScalingConfiguration": {
                "Mode": "ForecastOnly",
                "MetricSpecifications": [{
                    "TargetValue": target,
                    "PredefinedMetricPairSpecification": {
                        "PredefinedMetricType": "ASGCPUUtilization"}}]}}})

    def config(target):
        return {"Mode": "ForecastOnly", "MaxCapacityBreachBehavior": "HonorMaxCapacity",
                "MetricSpecifications": [{
                    "TargetValue": target,
                    "PredefinedMetricPairSpecification": {
                        "PredefinedMetricType": "ASGCPUUtilization"}}]}

    cfn.create_stack(StackName=stack_name, TemplateBody=template(40))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        arn = _cfn_output(cfn, stack_name, "Pred")
        policy = _cfn_asg_policies_by_arn(autoscaling, group)[arn]
        assert policy["PredictiveScalingConfiguration"] == config(40.0)
        assert not {"AdjustmentType", "ScalingAdjustment", "Cooldown"} & set(policy)
        cfn.update_stack(StackName=stack_name, TemplateBody=template(50))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert _cfn_output(cfn, stack_name, "Pred") == arn
        policy = _cfn_asg_policies_by_arn(autoscaling, group)[arn]
        assert policy["PredictiveScalingConfiguration"] == config(50.0)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_launch_template_reads_back_its_name_and_id(cfn, autoscaling):
    """DescribeAutoScalingGroups answers the launch template's id and name
    whichever of the two the group was given. A template references its
    launch template through Ref, the id, and the provisioner copied only that:
    LaunchTemplateName read "" where AWS answers the real name (measured
    2026-09-21)."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-ltname-{suffix}"
    lt_name = f"cfn-asg-ltname-{suffix}"

    def group(logical_id, reference):
        return {"Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": {
            "AutoScalingGroupName": f"{lt_name}-{logical_id.lower()}",
            "LaunchTemplate": {**reference, "Version": {"Fn::GetAtt": ["LT", "LatestVersionNumber"]}},
            "MinSize": "0", "MaxSize": "1", "DesiredCapacity": "0",
            "AvailabilityZones": ["us-east-1a"]}}

    template = json.dumps({"Resources": {
        "LT": {"Type": "AWS::EC2::LaunchTemplate", "Properties": {
            "LaunchTemplateName": lt_name,
            "LaunchTemplateData": {"ImageId": "ami-12345678", "InstanceType": "t3.micro"}}},
        "ById": group("ById", {"LaunchTemplateId": {"Ref": "LT"}}),
        "ByName": group("ByName", {"LaunchTemplateName": lt_name}),
    }, "Outputs": {"Lt": {"Value": {"Ref": "LT"}}}})
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        lt_id = _cfn_output(cfn, stack_name, "Lt")
        groups = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[
            f"{lt_name}-byid", f"{lt_name}-byname"])["AutoScalingGroups"]
        assert [g["LaunchTemplate"] for g in groups] == [
            {"LaunchTemplateId": lt_id, "LaunchTemplateName": lt_name, "Version": "1"}] * 2
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_instance_id_change_replaces_the_group(cfn, autoscaling):
    """InstanceId is create-only on AWS::AutoScaling::AutoScalingGroup (the
    registry's createOnlyProperties), so a change replaces the group: a new
    ARN under the generated name. The handler applied every other change in
    place and ignored this one."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-inst-{suffix}"

    def template(instance_id):
        return json.dumps({"Resources": {"ASG": {
            "Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": {
                "InstanceId": instance_id, "MinSize": "0", "MaxSize": "1",
                "DesiredCapacity": "0", "AvailabilityZones": ["us-east-1a"]}}},
            "Outputs": {"Name": {"Value": {"Ref": "ASG"}}}})

    def arn():
        # The stack's Ref, read each time: the replacement is the group the
        # stack names now, whatever name it was given.
        name = _cfn_output(cfn, stack_name, "Name")
        groups = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[name])["AutoScalingGroups"]
        assert len(groups) == 1, f"no group under the stack's Ref {name}"
        return groups[0]["AutoScalingGroupARN"]

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("i-0123456789abcdef0"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        before = arn()
        cfn.update_stack(StackName=stack_name, TemplateBody=template("i-0fedcba9876543210"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert arn() != before
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_step_and_customized_metric_policies_read_back(cfn, autoscaling):
    """A step policy's StepAdjustments, MetricAggregationType,
    MinAdjustmentMagnitude and EstimatedInstanceWarmup, and a customized
    metric's Dimensions list, read back in the shape AWS answers (measured
    2026-09-21 on the same two policies): the bounds as doubles, an open
    upper bound left out, and no ScalingAdjustment or Cooldown on a step
    policy."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-step-{suffix}"
    group = f"cfn-asg-step-{suffix}"
    body = _cfn_asg_policy_template(group, {
        "Step": {
            "PolicyType": "StepScaling",
            "AdjustmentType": "PercentChangeInCapacity",
            "MinAdjustmentMagnitude": 1,
            "MetricAggregationType": "Average",
            "EstimatedInstanceWarmup": 60,
            "StepAdjustments": [
                {"MetricIntervalLowerBound": 0, "MetricIntervalUpperBound": 10,
                 "ScalingAdjustment": 10},
                {"MetricIntervalLowerBound": 10, "ScalingAdjustment": 20},
            ]},
        "Tc": {
            "PolicyType": "TargetTrackingScaling",
            "TargetTrackingConfiguration": {
                "CustomizedMetricSpecification": {
                    "MetricName": "CPUUtilization", "Namespace": "AWS/EC2",
                    "Dimensions": [{"Name": "AutoScalingGroupName", "Value": group}],
                    "Statistic": "Average", "Unit": "Percent"},
                "TargetValue": 50.5,
                "DisableScaleIn": True}},
    })
    cfn.create_stack(StackName=stack_name, TemplateBody=body)
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        policies = _cfn_asg_policies_by_arn(autoscaling, group)
        step = policies[_cfn_output(cfn, stack_name, "Step")]
        assert step["AdjustmentType"] == "PercentChangeInCapacity"
        assert step["MinAdjustmentMagnitude"] == 1
        assert step["MetricAggregationType"] == "Average"
        assert step["EstimatedInstanceWarmup"] == 60
        assert step["StepAdjustments"] == [
            {"MetricIntervalLowerBound": 0.0, "MetricIntervalUpperBound": 10.0,
             "ScalingAdjustment": 10},
            {"MetricIntervalLowerBound": 10.0, "ScalingAdjustment": 20},
        ]
        assert not {"ScalingAdjustment", "Cooldown"} & set(step)
        tracking = policies[_cfn_output(cfn, stack_name, "Tc")]["TargetTrackingConfiguration"]
        assert tracking == {
            "CustomizedMetricSpecification": {
                "MetricName": "CPUUtilization", "Namespace": "AWS/EC2",
                "Dimensions": [{"Name": "AutoScalingGroupName", "Value": group}],
                "Statistic": "Average", "Unit": "Percent"},
            "TargetValue": 50.5,
            "DisableScaleIn": True,
        }
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _cfn_asg_group_template(group, max_size="3", tags=None, with_bad=False):
    props = {"AutoScalingGroupName": group, "LaunchConfigurationName": {"Ref": "LC"},
             "MinSize": "0", "MaxSize": max_size, "DesiredCapacity": "0",
             "AvailabilityZones": ["us-east-1a"]}
    if tags is not None:
        props["Tags"] = tags
    resources = {
        "LC": {"Type": "AWS::AutoScaling::LaunchConfiguration",
               "Properties": {"ImageId": "ami-12345678", "InstanceType": "t3.micro"}},
        "ASG": {"Type": "AWS::AutoScaling::AutoScalingGroup", "Properties": props},
    }
    if with_bad:
        resources["Bad"] = {**_FAILING_RESOURCE, "DependsOn": "ASG"}
    return json.dumps({"Resources": resources})


def test_cfn_asg_tag_change_keeps_tags_added_through_the_api(cfn, autoscaling):
    """Tags is No interruption on AWS::AutoScaling::AutoScalingGroup, and a
    template tag change on AWS leaves a tag added through CreateOrUpdateTags
    alone while the template's tags take their new values and
    PropagateAtLaunch (measured 2026-09-21). The update overwrote the whole
    tag list."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-tags-{suffix}"
    group = f"cfn-asg-tags-{suffix}"

    def own_tags():
        described = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[group])["AutoScalingGroups"][0]
        return sorted((t["Key"], t["Value"], t["PropagateAtLaunch"])
                      for t in described["Tags"] if not t["Key"].startswith("aws:"))

    cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_asg_group_template(
        group, tags=[{"Key": "a", "Value": "1", "PropagateAtLaunch": False}]))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        autoscaling.create_or_update_tags(Tags=[{
            "ResourceId": group, "ResourceType": "auto-scaling-group",
            "Key": "api", "Value": "kept", "PropagateAtLaunch": False}])

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_asg_group_template(
            group, tags=[{"Key": "a", "Value": "2", "PropagateAtLaunch": False},
                         {"Key": "b", "Value": "3", "PropagateAtLaunch": True}]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert own_tags() == [("a", "2", False), ("api", "kept", False), ("b", "3", True)]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_max_size_change_is_rolled_back(cfn, autoscaling):
    """MaxSize is applied in place, so a later failure in the same update has
    to set it back. Measured on AWS 2026-09-21: MaxSize reads 1 again after
    UPDATE_ROLLBACK_COMPLETE, under the same group ARN."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-rb-{suffix}"
    group = f"cfn-asg-rb-{suffix}"

    def described():
        return autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[group])["AutoScalingGroups"][0]

    cfn.create_stack(StackName=stack_name,
                     TemplateBody=_cfn_asg_group_template(group, max_size="1"))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        arn = described()["AutoScalingGroupARN"]

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_asg_group_template(
            group, max_size="2", with_bad=True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert described()["MaxSize"] == 1
        assert described()["AutoScalingGroupARN"] == arn
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_policy_target_value_change_is_rolled_back(cfn, autoscaling):
    """A TargetValue change is applied in place, so a later failure in the
    same update has to set it back. Measured on AWS 2026-09-21: TargetValue
    reads 40.0 again after UPDATE_ROLLBACK_COMPLETE, one policy under the same
    ARN."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-pol-rb-{suffix}"
    group = f"cfn-asg-pol-rb-{suffix}"

    def template(target):
        return _cfn_asg_policy_template(group, {"Tt": {
            "PolicyType": "TargetTrackingScaling",
            "TargetTrackingConfiguration": {
                "PredefinedMetricSpecification": {
                    "PredefinedMetricType": "ASGAverageCPUUtilization"},
                "TargetValue": target}}})

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template(40))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        arn = _cfn_output(cfn, stack_name, "Tt")

        cfn.update_stack(StackName=stack_name,
                         TemplateBody=_cfn_with_failing_resource(template(60), "Tt"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        policies = _cfn_asg_policies_by_arn(autoscaling, group)
        assert list(policies) == [arn]
        assert policies[arn]["TargetTrackingConfiguration"]["TargetValue"] == 40.0
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_asg_scheduled_action_recurrence_change_is_rolled_back(cfn, autoscaling):
    """A Recurrence change is applied in place, so a later failure in the same
    update has to set it back. Measured on AWS 2026-09-21: the action reads
    "0 9 * * *" again after UPDATE_ROLLBACK_COMPLETE, one action under the
    same ARN."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-asg-sched-rb-{suffix}"
    group = f"cfn-asg-sched-rb-{suffix}"

    def actions():
        return autoscaling.describe_scheduled_actions(
            AutoScalingGroupName=group)["ScheduledUpdateGroupActions"]

    try:
        cfn.create_stack(StackName=stack_name,
                         TemplateBody=_cfn_asg_scheduled_template(group, {}))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        [before] = actions()

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_with_failing_resource(
            _cfn_asg_scheduled_template(group, {"Recurrence": "0 10 * * *"}), "Sched"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        [after] = actions()
        assert after["Recurrence"] == "0 9 * * *"
        assert after["ScheduledActionARN"] == before["ScheduledActionARN"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
