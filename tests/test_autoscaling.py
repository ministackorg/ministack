import datetime
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import make_client


def _uid(prefix="test"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _client(region):
    return boto3.client(
        "autoscaling",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"mode": "standard"}),
    )


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
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)


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


def test_autoscaling_groups_and_tags_are_region_scoped():
    east = _client("us-east-1")
    west = _client("us-west-2")
    name = _uid("asg-region")

    east.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=1,
        MaxSize=3,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
        Tags=[{"Key": "region", "Value": "east", "PropagateAtLaunch": True}],
    )
    west.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=2,
        MaxSize=4,
        AvailabilityZones=["us-west-2a"],
        LaunchConfigurationName="dummy-lc",
        Tags=[{"Key": "region", "Value": "west", "PropagateAtLaunch": True}],
    )
    try:
        east_group = east.describe_auto_scaling_groups(
            AutoScalingGroupNames=[name]
        )["AutoScalingGroups"][0]
        west_group = west.describe_auto_scaling_groups(
            AutoScalingGroupNames=[name]
        )["AutoScalingGroups"][0]

        assert east_group["MinSize"] == 1
        assert west_group["MinSize"] == 2
        assert ":us-east-1:" in east_group["AutoScalingGroupARN"]
        assert ":us-west-2:" in west_group["AutoScalingGroupARN"]
        assert east_group["Tags"][0]["Value"] == "east"
        assert west_group["Tags"][0]["Value"] == "west"
    finally:
        east.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
        west.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)


def test_restore_legacy_child_state_uses_parent_asg_region():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import autoscaling as service

    account_id = "111111111111"
    boot_region = "us-east-1"
    resource_region = "us-west-2"
    asg_name = "legacy-asg"
    child_key = f"{asg_name}/legacy-child"
    original_account = get_account_id()
    original_region = get_region()

    asgs = AccountScopedDict()
    policies = AccountScopedDict()
    hooks = AccountScopedDict()
    scheduled_actions = AccountScopedDict()
    tags = AccountScopedDict()

    set_request_account_id(account_id)
    set_request_region(boot_region)
    asgs[asg_name] = {
        "AutoScalingGroupName": asg_name,
        "AutoScalingGroupARN": (
            f"arn:aws:autoscaling:{resource_region}:{account_id}:"
            f"autoScalingGroup:legacy:autoScalingGroupName/{asg_name}"
        ),
    }
    policies[child_key] = {"AutoScalingGroupName": asg_name}
    hooks[child_key] = {"AutoScalingGroupName": asg_name}
    scheduled_actions[child_key] = {"AutoScalingGroupName": asg_name}
    tags[asg_name] = [{"Key": "legacy", "Value": "true"}]

    service.reset()
    try:
        service.load_persisted_state(
            {
                "asgs": asgs,
                "policies": policies,
                "hooks": hooks,
                "scheduled_actions": scheduled_actions,
                "tags": tags,
            }
        )
        assert service._asgs.get_scoped(
            account_id, resource_region, asg_name
        )["AutoScalingGroupName"] == asg_name
        for store in (
            service._policies,
            service._hooks,
            service._scheduled_actions,
        ):
            assert (
                store.get_scoped(account_id, resource_region, child_key) is not None
            )
            assert store.get_scoped(account_id, boot_region, child_key) is None
        assert service._tags.get_scoped(account_id, resource_region, asg_name) == [
            {"Key": "legacy", "Value": "true"}
        ]
        assert service._tags.get_scoped(account_id, boot_region, asg_name) is None
    finally:
        service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_reset_clears_autoscaling_state_across_regions():
    from ministack.core.responses import get_region, set_request_region
    from ministack.services import autoscaling as service

    original_region = get_region()
    stores = (
        service._asgs,
        service._launch_configs,
        service._policies,
        service._hooks,
        service._scheduled_actions,
        service._tags,
    )
    service.reset()
    try:
        for region in ("us-east-1", "us-west-2"):
            set_request_region(region)
            for store in stores:
                store[f"resource-{region}"] = {"region": region}

        service.reset()
        assert all(not store.has_any() for store in stores)
    finally:
        service.reset()
        set_request_region(original_region)


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
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)


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


def test_put_scaling_policy_target_tracking_and_step_read_back(autoscaling):
    """Target-tracking and step policies read back with only their type's members."""
    asg = _uid("asg-ttpol")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg, MinSize=0, MaxSize=2,
        AvailabilityZones=["us-east-1a"], LaunchConfigurationName="dummy-lc",
    )
    tracking = {
        "CustomizedMetricSpecification": {
            "MetricName": "CPUUtilization", "Namespace": "AWS/EC2",
            "Dimensions": [{"Name": "AutoScalingGroupName", "Value": asg}],
            "Statistic": "Average", "Unit": "Percent",
        },
        "TargetValue": 50.5,
        "DisableScaleIn": True,
    }
    steps = [
        {"MetricIntervalLowerBound": 0.0, "MetricIntervalUpperBound": 10.0, "ScalingAdjustment": 10},
        {"MetricIntervalLowerBound": 10.0, "ScalingAdjustment": 20},
    ]
    try:
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=asg, PolicyName="tt", PolicyType="TargetTrackingScaling",
            TargetTrackingConfiguration=tracking, EstimatedInstanceWarmup=120,
        )
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=asg, PolicyName="step", PolicyType="StepScaling",
            AdjustmentType="PercentChangeInCapacity", MinAdjustmentMagnitude=1,
            MetricAggregationType="Average", StepAdjustments=steps,
        )
        policies = {p["PolicyName"]: p for p in autoscaling.describe_policies(
            AutoScalingGroupName=asg)["ScalingPolicies"]}
        tt, step = policies["tt"], policies["step"]
        assert tt["TargetTrackingConfiguration"] == tracking
        assert tt["EstimatedInstanceWarmup"] == 120
        assert tt["Enabled"] is True and tt["StepAdjustments"] == []
        assert not {"AdjustmentType", "ScalingAdjustment", "Cooldown"} & set(tt)
        assert step["StepAdjustments"] == steps
        assert step["AdjustmentType"] == "PercentChangeInCapacity"
        assert step["MinAdjustmentMagnitude"] == 1
        assert step["MetricAggregationType"] == "Average"
        assert not {"ScalingAdjustment", "Cooldown"} & set(step)
    finally:
        for name in ("tt", "step"):
            autoscaling.delete_policy(AutoScalingGroupName=asg, PolicyName=name)
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_put_scaling_policy_predictive_read_back(autoscaling):
    """PredictiveScalingConfiguration reads back with MaxCapacityBreachBehavior defaulted."""
    asg = _uid("asg-pred")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg, MinSize=0, MaxSize=2,
        AvailabilityZones=["us-east-1a"], LaunchConfigurationName="dummy-lc",
    )
    config = {"Mode": "ForecastOnly", "MetricSpecifications": [{
        "TargetValue": 40.0,
        "PredefinedMetricPairSpecification": {"PredefinedMetricType": "ASGCPUUtilization"}}]}
    try:
        autoscaling.put_scaling_policy(
            AutoScalingGroupName=asg, PolicyName="pred", PolicyType="PredictiveScaling",
            PredictiveScalingConfiguration=config)
        policy = autoscaling.describe_policies(AutoScalingGroupName=asg)["ScalingPolicies"][0]
        assert policy["PredictiveScalingConfiguration"] == {
            **config, "MaxCapacityBreachBehavior": "HonorMaxCapacity"}
        assert not {"AdjustmentType", "ScalingAdjustment", "Cooldown"} & set(policy)
    finally:
        autoscaling.delete_policy(AutoScalingGroupName=asg, PolicyName="pred")
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


_CPU_TRACKING = {"PredefinedMetricSpecification": {"PredefinedMetricType": "ASGAverageCPUUtilization"},
                 "TargetValue": 50.0}


def _tracking_group(autoscaling):
    asg = _uid("asg-ttalarm")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg, MinSize=0, MaxSize=2,
        AvailabilityZones=["us-east-1a"], LaunchConfigurationName="dummy-lc",
    )
    return asg


def _put_tracking(autoscaling, asg, name, tracking):
    return autoscaling.put_scaling_policy(
        AutoScalingGroupName=asg, PolicyName=name, PolicyType="TargetTrackingScaling",
        TargetTrackingConfiguration=tracking)


def test_target_tracking_policy_creates_alarms(autoscaling, cw):
    """A target tracking policy creates an AlarmHigh and an AlarmLow alarm and lists them."""
    asg = _tracking_group(autoscaling)
    try:
        put = _put_tracking(autoscaling, asg, "cpu", _CPU_TRACKING)
        arn = put["PolicyARN"]
        names = [a["AlarmName"] for a in put["Alarms"]]
        assert [n.rsplit("-", 5)[0] for n in names] == [
            f"TargetTracking-{asg}-AlarmHigh", f"TargetTracking-{asg}-AlarmLow"]
        assert autoscaling.describe_policies(
            AutoScalingGroupName=asg)["ScalingPolicies"][0]["Alarms"] == put["Alarms"]
        alarms = {a["AlarmName"]: a for a in cw.describe_alarms(AlarmNames=names)["MetricAlarms"]}
        dims = [{"Name": "AutoScalingGroupName", "Value": asg}]
        assert [(a["AlarmArn"], a["Namespace"], a["MetricName"], a["Statistic"], a["Dimensions"],
                 a["Period"], a["EvaluationPeriods"], a["Threshold"], a["ComparisonOperator"],
                 a["AlarmActions"]) for a in (alarms[n] for n in names)] == [
            (put["Alarms"][0]["AlarmARN"], "AWS/EC2", "CPUUtilization", "Average", dims,
             60, 3, 50.0, "GreaterThanThreshold", [arn]),
            (put["Alarms"][1]["AlarmARN"], "AWS/EC2", "CPUUtilization", "Average", dims,
             60, 15, 45.0, "LessThanThreshold", [arn]),
        ]
        for alarm in alarms.values():
            assert alarm["AlarmDescription"] == (
                f"DO NOT EDIT OR DELETE. For TargetTrackingScaling policy {arn}.")
            assert not {"Unit", "DatapointsToAlarm", "TreatMissingData"} & set(alarm)
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


@pytest.mark.parametrize("tracking,metric,thresholds", [
    ({"PredefinedMetricSpecification": {"PredefinedMetricType": "ASGAverageNetworkIn"},
      "TargetValue": 1000000.0},
     ("AWS/EC2", "NetworkIn", "Average", "Bytes"), [1000000.0, 900000.0]),
    ({"PredefinedMetricSpecification": {"PredefinedMetricType": "ASGAverageNetworkOut"},
      "TargetValue": 1000000.0},
     ("AWS/EC2", "NetworkOut", "Average", "Bytes"), [1000000.0, 900000.0]),
    ({"CustomizedMetricSpecification": {"MetricName": "Depth", "Namespace": "Test/App",
                                        "Dimensions": [{"Name": "Q", "Value": "q1"}],
                                        "Statistic": "Sum", "Unit": "Count"},
      "TargetValue": 0.1},
     ("Test/App", "Depth", "Sum", "Count"), [0.1, 0.09]),
    ({**_CPU_TRACKING, "DisableScaleIn": True},
     ("AWS/EC2", "CPUUtilization", "Average", None), [50.0]),
], ids=["network-in", "network-out", "customized", "disable-scale-in"])
def test_target_tracking_alarm_metric_and_thresholds(autoscaling, cw, tracking, metric, thresholds):
    """The alarms follow the tracked metric and target; DisableScaleIn leaves only AlarmHigh."""
    asg = _tracking_group(autoscaling)
    try:
        names = [a["AlarmName"] for a in _put_tracking(autoscaling, asg, "p", tracking)["Alarms"]]
        alarms = {a["AlarmName"]: a for a in cw.describe_alarms(AlarmNames=names)["MetricAlarms"]}
        assert [(a["Namespace"], a["MetricName"], a["Statistic"], a.get("Unit"))
                for a in (alarms[n] for n in names)] == [metric] * len(thresholds)
        assert [alarms[n]["Threshold"] for n in names] == thresholds
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_target_tracking_alarms_follow_policy_lifecycle(autoscaling, cw):
    """Updating a policy replaces its alarms; deleting the policy or the group deletes them."""
    asg = _tracking_group(autoscaling)

    def existing(put):
        names = [a["AlarmName"] for a in put["Alarms"]]
        return [a["AlarmName"] for a in cw.describe_alarms(AlarmNames=names)["MetricAlarms"]]

    try:
        first = _put_tracking(autoscaling, asg, "cpu", _CPU_TRACKING)
        second = _put_tracking(autoscaling, asg, "cpu", {**_CPU_TRACKING, "TargetValue": 60.0})
        assert existing(first) == []
        assert len(existing(second)) == 2
        autoscaling.delete_policy(AutoScalingGroupName=asg, PolicyName="cpu")
        assert existing(second) == []
        kept = [_put_tracking(autoscaling, asg, name, _CPU_TRACKING) for name in ("a", "b")]
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)
    assert [existing(put) for put in kept] == [[], []]
    assert cw.describe_alarms(AlarmNamePrefix=f"TargetTracking-{asg}-")["MetricAlarms"] == []


def test_group_delete_keeps_the_alarms_of_a_group_with_a_longer_name(autoscaling, cw):
    """Deleting a group leaves the policies and alarms of a group whose name starts with it."""
    asg = _tracking_group(autoscaling)
    other = f"{asg}/blue"
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=other, MinSize=0, MaxSize=2,
        AvailabilityZones=["us-east-1a"], LaunchConfigurationName="dummy-lc",
    )
    try:
        _put_tracking(autoscaling, asg, "cpu", _CPU_TRACKING)
        kept = _put_tracking(autoscaling, other, "cpu", _CPU_TRACKING)
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)
        names = [a["AlarmName"] for a in kept["Alarms"]]
        assert len(cw.describe_alarms(AlarmNames=names)["MetricAlarms"]) == 2
        assert [p["PolicyName"] for p in autoscaling.describe_policies(
            AutoScalingGroupName=other)["ScalingPolicies"]] == ["cpu"]
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=other)
    assert cw.describe_alarms(AlarmNamePrefix=f"TargetTracking-{other}-")["MetricAlarms"] == []


@pytest.mark.parametrize("members", [
    {"PolicyType": "TargetTrackingScaling",
     "TargetTrackingConfiguration.PredefinedMetricSpecification.PredefinedMetricType":
         "ASGAverageCPUUtilization",
     "TargetTrackingConfiguration.TargetValue": "fifty"},
    {"PolicyType": "StepScaling", "AdjustmentType": "ChangeInCapacity",
     "StepAdjustments.member.1.MetricIntervalLowerBound": "zero",
     "StepAdjustments.member.1.ScalingAdjustment": "1"},
    {"PolicyType": "SimpleScaling", "ScalingAdjustment": "one"},
], ids=["target-value", "step-bound", "scaling-adjustment"])
def test_put_scaling_policy_non_numeric_member_is_a_validation_error(autoscaling, members):
    """A non-numeric number member is a ValidationError, not a 500."""
    asg = _uid("asg-badnum")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg, MinSize=0, MaxSize=2,
        AvailabilityZones=["us-east-1a"], LaunchConfigurationName="dummy-lc",
    )
    body = urllib.parse.urlencode({
        "Action": "PutScalingPolicy", "Version": "2011-01-01",
        "AutoScalingGroupName": asg, "PolicyName": "bad", **members,
    }).encode()
    req = urllib.request.Request(
        os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566") + "/",
        data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Authorization": ("AWS4-HMAC-SHA256 Credential=test/20260921/"
                                   "us-east-1/autoscaling/aws4_request, "
                                   "SignedHeaders=, Signature=x")},
    )
    try:
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(req, timeout=10)
        assert exc.value.code == 400
        assert b"<Code>ValidationError</Code>" in exc.value.read()
        assert autoscaling.describe_policies(AutoScalingGroupName=asg)["ScalingPolicies"] == []
    finally:
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


def test_put_lifecycle_hook_notification_metadata_reads_back(autoscaling):
    asg = _uid("asg-hmeta")
    hook = _uid("hmeta")
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
            NotificationMetadata='{"drain": "<all> & wait"}',
        )
        [found] = autoscaling.describe_lifecycle_hooks(AutoScalingGroupName=asg)["LifecycleHooks"]
        assert found["NotificationMetadata"] == '{"drain": "<all> & wait"}'
    finally:
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


def test_scheduled_action_times_read_back(autoscaling):
    """StartTime (also as Time), EndTime and TimeZone read back."""
    asg = _uid("asg-stimes")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg, MinSize=0, MaxSize=2,
        AvailabilityZones=["us-east-1a"], LaunchConfigurationName="dummy-lc",
    )
    start = datetime.datetime(2027, 1, 4, 9, tzinfo=datetime.timezone.utc)
    end = datetime.datetime(2027, 6, 1, tzinfo=datetime.timezone.utc)
    try:
        autoscaling.put_scheduled_update_group_action(
            AutoScalingGroupName=asg, ScheduledActionName="times",
            Recurrence="0 9 * * *", StartTime=start, EndTime=end,
            TimeZone="Europe/Berlin", MinSize=0, MaxSize=1)
        action = autoscaling.describe_scheduled_actions(
            AutoScalingGroupName=asg)["ScheduledUpdateGroupActions"][0]
        assert (action["StartTime"], action["Time"], action["EndTime"], action["TimeZone"]) == (
            start, start, end, "Europe/Berlin")
    finally:
        autoscaling.delete_scheduled_action(AutoScalingGroupName=asg, ScheduledActionName="times")
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


def _ensure_my_template(ec2):
    try:
        ec2.create_launch_template(LaunchTemplateName="my-template",
                                   LaunchTemplateData={"InstanceType": "t3.micro", "ImageId": "ami-12345678"})
    except ClientError as exc:
        if "AlreadyExists" not in exc.response["Error"]["Code"]:
            raise


def test_create_asg_with_launch_template(autoscaling, ec2):
    _ensure_my_template(ec2)
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


def test_asg_launch_template_by_name_reports_its_id(autoscaling, ec2):
    """A group given the template's name (or id) reports both, and UpdateAutoScalingGroup can switch it."""
    first = ec2.create_launch_template(LaunchTemplateName=_uid("lt-a"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    second = ec2.create_launch_template(LaunchTemplateName=_uid("lt-b"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    asg = _uid("asg-lt-name")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg, MinSize=0, MaxSize=1, AvailabilityZones=["us-east-1a"],
        LaunchTemplate={"LaunchTemplateName": first["LaunchTemplateName"], "Version": "$Default"})
    try:
        def group_lt():
            return autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[asg])["AutoScalingGroups"][0][
                "LaunchTemplate"]

        assert group_lt() == {"LaunchTemplateId": first["LaunchTemplateId"],
                              "LaunchTemplateName": first["LaunchTemplateName"], "Version": "$Default"}
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=asg, LaunchTemplate={"LaunchTemplateId": second["LaunchTemplateId"], "Version": "1"})
        assert group_lt() == {"LaunchTemplateId": second["LaunchTemplateId"],
                              "LaunchTemplateName": second["LaunchTemplateName"], "Version": "1"}
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)
        for lt in (first, second):
            ec2.delete_launch_template(LaunchTemplateId=lt["LaunchTemplateId"])


def test_asg_launch_template_errors_and_version_follow_aws(autoscaling, ec2):
    """Captured on AWS (us-east-1, 2026-10-28): a missing template or version is a ValidationError, and a group
    given no Version reports none."""
    lt = ec2.create_launch_template(LaunchTemplateName=_uid("lt-err"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    asg = _uid("asg-lt-err")
    try:
        cases = [
            ({"LaunchTemplateName": "no-such-template", "Version": "$Default"},
             "The specified launch template, with template name no-such-template, does not exist."),
            ({"LaunchTemplateId": "lt-0123456789abcdef0", "Version": "$Default"},
             "Malformed launch template Id: lt-0123456789abcdef0"),
            ({"LaunchTemplateId": lt["LaunchTemplateId"], "Version": "99"},
             f"Could not find the specified version 99 for the launch template with ID {lt['LaunchTemplateId']}."),
        ]
        for spec, message in cases:
            with pytest.raises(ClientError) as exc:
                autoscaling.create_auto_scaling_group(AutoScalingGroupName=asg, MinSize=0, MaxSize=0,
                                                      AvailabilityZones=["us-east-1a"], LaunchTemplate=spec)
            assert exc.value.response["Error"]["Code"] == "ValidationError"
            assert exc.value.response["Error"]["Message"] == message
        autoscaling.create_auto_scaling_group(AutoScalingGroupName=asg, MinSize=0, MaxSize=0,
                                              AvailabilityZones=["us-east-1a"],
                                              LaunchTemplate={"LaunchTemplateName": lt["LaunchTemplateName"]})
        group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[asg])["AutoScalingGroups"][0]
        assert group["LaunchTemplate"] == {"LaunchTemplateId": lt["LaunchTemplateId"],
                                           "LaunchTemplateName": lt["LaunchTemplateName"]}
        with pytest.raises(ClientError) as exc:
            autoscaling.update_auto_scaling_group(AutoScalingGroupName=asg,
                                                  LaunchTemplate={"LaunchTemplateName": "no-such-template"})
        assert exc.value.response["Error"]["Message"] == (
            "The specified launch template, with template name no-such-template, does not exist.")
    finally:
        try:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)
        except ClientError:
            pass
        ec2.delete_launch_template(LaunchTemplateId=lt["LaunchTemplateId"])


def test_asg_mixed_instances_policy_follows_aws(autoscaling, ec2):
    """#2452, captured on AWS (us-east-1, 2026-10-29): a MixedInstancesPolicy is kept with its launch
    template resolved, AWS's InstancesDistribution defaults and Overrides [] when none; the group has no
    LaunchTemplate; both or no launch source is refused."""
    lt = ec2.create_launch_template(LaunchTemplateName=_uid("lt-mix"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    base = dict(MinSize=0, MaxSize=0, DesiredCapacity=0, AvailabilityZones=["us-east-1a"])
    defaults = {"OnDemandAllocationStrategy": "prioritized", "OnDemandBaseCapacity": 0,
                "OnDemandPercentageAboveBaseCapacity": 100, "SpotAllocationStrategy": "lowest-price",
                "SpotInstancePools": 2}
    groups = []
    try:
        for suffix, policy, expected in (
                ("min", {"LaunchTemplate": {
                    "LaunchTemplateSpecification": {"LaunchTemplateName": lt["LaunchTemplateName"], "Version": "$Default"},
                    "Overrides": [{"InstanceType": "t3.micro"}, {"InstanceType": "t3a.micro"}]}},
                 {"LaunchTemplate": {"LaunchTemplateSpecification": {
                     "LaunchTemplateId": lt["LaunchTemplateId"], "LaunchTemplateName": lt["LaunchTemplateName"],
                     "Version": "$Default"}, "Overrides": [{"InstanceType": "t3.micro"}, {"InstanceType": "t3a.micro"}]},
                  "InstancesDistribution": defaults}),
                ("id", {"LaunchTemplate": {"LaunchTemplateSpecification": {"LaunchTemplateId": lt["LaunchTemplateId"]}},
                        "InstancesDistribution": {"OnDemandPercentageAboveBaseCapacity": 50}},
                 {"LaunchTemplate": {"LaunchTemplateSpecification": {
                     "LaunchTemplateId": lt["LaunchTemplateId"], "LaunchTemplateName": lt["LaunchTemplateName"]},
                     "Overrides": []},
                  "InstancesDistribution": {**defaults, "OnDemandPercentageAboveBaseCapacity": 50}})):
            name = _uid(f"asg-mix-{suffix}")
            autoscaling.create_auto_scaling_group(AutoScalingGroupName=name, MixedInstancesPolicy=policy, **base)
            groups.append(name)
            group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"][0]
            assert group["MixedInstancesPolicy"] == expected
            assert "LaunchTemplate" not in group

        message = ("Valid requests must contain either LaunchTemplate, LaunchConfigurationName, InstanceId or "
                   "MixedInstancesPolicy parameter.")
        for kwargs in ({"LaunchTemplate": {"LaunchTemplateId": lt["LaunchTemplateId"]},
                        "MixedInstancesPolicy": {"LaunchTemplate": {"LaunchTemplateSpecification": {
                            "LaunchTemplateId": lt["LaunchTemplateId"]}}}}, {}):
            with pytest.raises(ClientError) as exc:
                autoscaling.create_auto_scaling_group(AutoScalingGroupName=_uid("asg-mix-bad"), **base, **kwargs)
            assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
                "ValidationError", message)
    finally:
        for name in groups:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)
        ec2.delete_launch_template(LaunchTemplateId=lt["LaunchTemplateId"])


_POOLS_MESSAGE = "SpotInstancePools option is only available with the lowest-price allocation strategy."


def _mixed_policy(lt_id, overrides=("t3.micro", "t3a.micro"), **distribution):
    return {"LaunchTemplate": {"LaunchTemplateSpecification": {"LaunchTemplateId": lt_id, "Version": "$Latest"},
                               "Overrides": [{"InstanceType": t} for t in overrides]},
            "InstancesDistribution": {"OnDemandBaseCapacity": 0, "OnDemandPercentageAboveBaseCapacity": 0,
                                      **distribution}}


def _distribution(autoscaling, name):
    return autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"][0][
        "MixedInstancesPolicy"]


def test_asg_spot_instance_pools_follow_allocation_strategy(autoscaling, ec2):
    """#2577, captured on AWS (us-east-2, 2026-11-04): SpotInstancePools defaults to 2 only for lowest-price
    (m1b-describe-{pco-omit,co-omit,lp-omit,lp-three,default-omit}, m2-describe-nostrat-pools,
    m2-describe-pco-spotmax), any other strategy with it is
    refused (m1b-create-pco-zero, m5-create-pco-two, m2-create-cop-pools), and SpotMaxPrice "" stores none
    (m2-describe-spotmax-empty)."""
    lt = ec2.create_launch_template(LaunchTemplateName=_uid("lt-pools"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    base = dict(MinSize=0, MaxSize=0, DesiredCapacity=0, AvailabilityZones=["us-east-1a"])
    common = {"OnDemandAllocationStrategy": "prioritized", "OnDemandBaseCapacity": 0,
              "OnDemandPercentageAboveBaseCapacity": 0}
    groups = []
    try:
        for suffix, sent, expected in (
                ("pco", {"SpotAllocationStrategy": "price-capacity-optimized"},
                 {"SpotAllocationStrategy": "price-capacity-optimized"}),
                ("co", {"SpotAllocationStrategy": "capacity-optimized"}, {"SpotAllocationStrategy": "capacity-optimized"}),
                ("lp", {"SpotAllocationStrategy": "lowest-price"},
                 {"SpotAllocationStrategy": "lowest-price", "SpotInstancePools": 2}),
                ("lp3", {"SpotAllocationStrategy": "lowest-price", "SpotInstancePools": 3},
                 {"SpotAllocationStrategy": "lowest-price", "SpotInstancePools": 3}),
                ("none", {}, {"SpotAllocationStrategy": "lowest-price", "SpotInstancePools": 2}),
                ("none4", {"SpotInstancePools": 4}, {"SpotAllocationStrategy": "lowest-price", "SpotInstancePools": 4}),
                ("pco-max", {"SpotAllocationStrategy": "price-capacity-optimized", "SpotMaxPrice": "0.05"},
                 {"SpotAllocationStrategy": "price-capacity-optimized", "SpotMaxPrice": "0.05"}),
                ("max-empty", {"SpotMaxPrice": ""}, {"SpotAllocationStrategy": "lowest-price", "SpotInstancePools": 2})):
            name = _uid(f"asg-pools-{suffix}")
            autoscaling.create_auto_scaling_group(AutoScalingGroupName=name,
                                                  MixedInstancesPolicy=_mixed_policy(lt["LaunchTemplateId"], **sent), **base)
            groups.append(name)
            assert _distribution(autoscaling, name)["InstancesDistribution"] == {**common, **expected}

        for strategy, pools in (("price-capacity-optimized", 0), ("price-capacity-optimized", 2),
                                ("capacity-optimized-prioritized", 2)):
            name = _uid("asg-pools-bad")
            with pytest.raises(ClientError) as exc:
                autoscaling.create_auto_scaling_group(AutoScalingGroupName=name, MixedInstancesPolicy=_mixed_policy(
                    lt["LaunchTemplateId"], SpotAllocationStrategy=strategy, SpotInstancePools=pools), **base)
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
            assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
                "ValidationError", _POOLS_MESSAGE)
            assert not autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"]
    finally:
        for name in groups:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
        ec2.delete_launch_template(LaunchTemplateId=lt["LaunchTemplateId"])


def test_asg_update_merges_mixed_instances_policy(autoscaling, ec2):
    """#2577, captured on AWS (us-east-2, 2026-11-04): UpdateAutoScalingGroup keeps the members it is not sent
    (m1b-describe-lp-to-pco, m2-describe-after-a-*, m4-describe-after-c-spec-only, m8-describe-after-pools-only-lp;
    a spec without Version drops the stored one, m8-describe-after-spec-no-version), drops SpotInstancePools
    for another strategy and restores 2 for lowest-price, clears SpotMaxPrice on "", and refuses
    SpotInstancePools under the stored or sent strategy without changing the group (m2-update-a-pools-on-pco,
    m2-update-a-co-with-pools)."""
    lt = ec2.create_launch_template(LaunchTemplateName=_uid("lt-merge"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    lt_id = lt["LaunchTemplateId"]
    name = _uid("asg-merge")
    spec = {"LaunchTemplateId": lt_id, "LaunchTemplateName": lt["LaunchTemplateName"], "Version": "$Latest"}
    overrides = [{"InstanceType": "t3.micro"}, {"InstanceType": "t3a.micro"}]
    dist = {"OnDemandAllocationStrategy": "lowest-price", "OnDemandBaseCapacity": 0,
            "OnDemandPercentageAboveBaseCapacity": 0, "SpotAllocationStrategy": "lowest-price",
            "SpotInstancePools": 3, "SpotMaxPrice": "0.05"}
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name, MinSize=0, MaxSize=0, DesiredCapacity=0, AvailabilityZones=["us-east-1a"],
        MixedInstancesPolicy=_mixed_policy(lt_id, OnDemandAllocationStrategy="lowest-price",
                                           SpotAllocationStrategy="lowest-price", SpotInstancePools=3,
                                           SpotMaxPrice="0.05"))
    try:
        def update(policy):
            autoscaling.update_auto_scaling_group(AutoScalingGroupName=name, MixedInstancesPolicy=policy)
            return _distribution(autoscaling, name)

        def refused(policy):
            before = _distribution(autoscaling, name)
            with pytest.raises(ClientError) as exc:
                autoscaling.update_auto_scaling_group(AutoScalingGroupName=name, MixedInstancesPolicy=policy)
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
            assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
                "ValidationError", _POOLS_MESSAGE)
            assert _distribution(autoscaling, name) == before

        dist.pop("SpotInstancePools")
        dist["SpotAllocationStrategy"] = "price-capacity-optimized"
        assert update({"InstancesDistribution": {"SpotAllocationStrategy": "price-capacity-optimized"}}) == {
            "LaunchTemplate": {"LaunchTemplateSpecification": spec, "Overrides": overrides},
            "InstancesDistribution": dist}
        refused({"InstancesDistribution": {"SpotInstancePools": 4}})
        dist.update(SpotAllocationStrategy="lowest-price", SpotInstancePools=2)
        assert update({"InstancesDistribution": {"SpotAllocationStrategy": "lowest-price"}})["InstancesDistribution"] == dist
        refused({"InstancesDistribution": {"SpotAllocationStrategy": "capacity-optimized", "SpotInstancePools": 2}})
        del dist["SpotMaxPrice"]
        assert update({"InstancesDistribution": {"SpotMaxPrice": ""}})["InstancesDistribution"] == dist

        overrides = overrides + [{"InstanceType": "t3.small"}]
        assert update({"LaunchTemplate": {"LaunchTemplateSpecification": {"LaunchTemplateId": lt_id, "Version": "$Latest"},
                                          "Overrides": overrides}}) == {
            "LaunchTemplate": {"LaunchTemplateSpecification": spec, "Overrides": overrides},
            "InstancesDistribution": dist}
        overrides = [{"InstanceType": "t3.micro"}]
        assert update({"LaunchTemplate": {"Overrides": overrides}})["LaunchTemplate"] == {
            "LaunchTemplateSpecification": spec, "Overrides": overrides}
        spec["Version"] = "1"
        assert update({"LaunchTemplate": {"LaunchTemplateSpecification": {"LaunchTemplateId": lt_id, "Version": "1"}}})[
            "LaunchTemplate"] == {"LaunchTemplateSpecification": spec, "Overrides": overrides}
        dist["OnDemandPercentageAboveBaseCapacity"] = 50
        assert update({"InstancesDistribution": {"OnDemandPercentageAboveBaseCapacity": 50}}) == {
            "LaunchTemplate": {"LaunchTemplateSpecification": spec, "Overrides": overrides},
            "InstancesDistribution": dist}
        dist["SpotInstancePools"] = 6
        assert update({"InstancesDistribution": {"SpotInstancePools": 6}})["InstancesDistribution"] == dist
        del spec["Version"]
        assert update({"LaunchTemplate": {"LaunchTemplateSpecification": {"LaunchTemplateId": lt_id}}}) == {
            "LaunchTemplate": {"LaunchTemplateSpecification": spec, "Overrides": overrides},
            "InstancesDistribution": dist}
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
        ec2.delete_launch_template(LaunchTemplateId=lt_id)


def test_asg_mixed_instances_policy_needs_a_launch_template(autoscaling, ec2):
    """#2577, captured on AWS (us-east-2, 2026-11-04): with no stored policy, on create or on update of a group
    with a plain LaunchTemplate, a MixedInstancesPolicy without LaunchTemplate is refused before SpotInstancePools
    (m6-create-no-lt, m10-create-no-lt-pools, m5-update-plain-dist-only, m6-update-plain-no-lt-pools) and one with
    only Overrides needs an
    id or name, also before SpotInstancePools (m6-create-overrides-only, m6-update-plain-overrides,
    m7-create-overrides-only-pools, m7-update-plain-overrides-pools); the group is unchanged
    (m5-describe-after-plain-dist-only, m5-describe-after-plain-pools, m6-describe-after-plain-overrides,
    m6-describe-after-plain-no-lt-pools, m7-describe-after-plain-overrides-pools). A full policy replaces the LaunchTemplate (m5-describe-after-plain-full),
    and SpotInstancePools is refused there too (m5-update-plain-pools)."""
    lt = ec2.create_launch_template(LaunchTemplateName=_uid("lt-need"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    lt_id = lt["LaunchTemplateId"]
    base = dict(MinSize=0, MaxSize=0, DesiredCapacity=0, AvailabilityZones=["us-east-1a"])
    required = ("A launch template is required when configuring an Auto Scaling group with a mixed instances policy. "
                "Add a launch template to the mixed instances policy and try again.")
    id_or_name = "Valid requests must contain either launchTemplateId or LaunchTemplateName"
    dist = {"OnDemandBaseCapacity": 0, "OnDemandPercentageAboveBaseCapacity": 0,
            "SpotAllocationStrategy": "price-capacity-optimized"}
    overrides = [{"InstanceType": "t3.micro"}, {"InstanceType": "t3a.micro"}]
    name = _uid("asg-need")

    def refused(call, message, **kwargs):
        with pytest.raises(ClientError) as exc:
            call(AutoScalingGroupName=name, **kwargs)
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
            "ValidationError", message)

    try:
        refused(autoscaling.create_auto_scaling_group, required, MixedInstancesPolicy={"InstancesDistribution": dist},
                **base)
        refused(autoscaling.create_auto_scaling_group, required, MixedInstancesPolicy={
            "InstancesDistribution": {**dist, "SpotInstancePools": 2}}, **base)
        refused(autoscaling.create_auto_scaling_group, id_or_name, MixedInstancesPolicy={
            "LaunchTemplate": {"Overrides": overrides}, "InstancesDistribution": dist}, **base)
        refused(autoscaling.create_auto_scaling_group, id_or_name, MixedInstancesPolicy={
            "LaunchTemplate": {"Overrides": overrides}, "InstancesDistribution": {**dist, "SpotInstancePools": 2}}, **base)
        assert not autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"]

        autoscaling.create_auto_scaling_group(AutoScalingGroupName=name, LaunchTemplate={
            "LaunchTemplateId": lt_id, "Version": "$Latest"}, **base)
        before = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"][0]
        update = autoscaling.update_auto_scaling_group
        refused(update, required, MixedInstancesPolicy={"InstancesDistribution": dist})
        refused(update, required, MixedInstancesPolicy={"InstancesDistribution": {**dist, "SpotInstancePools": 2}})
        refused(update, id_or_name, MixedInstancesPolicy={"LaunchTemplate": {"Overrides": overrides},
                                                          "InstancesDistribution": dist})
        refused(update, id_or_name, MixedInstancesPolicy={"LaunchTemplate": {"Overrides": overrides},
                                                          "InstancesDistribution": {**dist, "SpotInstancePools": 2}})
        refused(update, _POOLS_MESSAGE, MixedInstancesPolicy={
            "LaunchTemplate": {"LaunchTemplateSpecification": {"LaunchTemplateId": lt_id, "Version": "$Latest"}},
            "InstancesDistribution": {"SpotAllocationStrategy": "capacity-optimized", "SpotInstancePools": 2}})
        assert autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"][0] == before

        update(AutoScalingGroupName=name, MixedInstancesPolicy={
            "LaunchTemplate": {"LaunchTemplateSpecification": {"LaunchTemplateId": lt_id, "Version": "$Latest"},
                               "Overrides": overrides},
            "InstancesDistribution": {"SpotAllocationStrategy": "price-capacity-optimized"}})
        group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"][0]
        assert "LaunchTemplate" not in group
        assert group["MixedInstancesPolicy"] == {
            "LaunchTemplate": {"LaunchTemplateSpecification": {
                "LaunchTemplateId": lt_id, "LaunchTemplateName": lt["LaunchTemplateName"], "Version": "$Latest"},
                "Overrides": overrides},
            "InstancesDistribution": {"OnDemandAllocationStrategy": "prioritized", "OnDemandBaseCapacity": 0,
                                      "OnDemandPercentageAboveBaseCapacity": 100,
                                      "SpotAllocationStrategy": "price-capacity-optimized"}}
    finally:
        try:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
        except ClientError:
            pass
        ec2.delete_launch_template(LaunchTemplateId=lt_id)


def test_asg_update_to_lowest_price_with_pools(autoscaling, ec2):
    """#2577, captured on AWS (us-east-2, 2026-11-04, m2-describe-after-b-to-lp-pools): a price-capacity-optimized
    group updated to lowest-price with SpotInstancePools keeps the pools sent, and re-sending lowest-price keeps them
    (m11-describe-after-lp3-resend-lp, whose group was created lowest-price/3; that it holds after this update is
    inferred). It also keeps the launch template it
    was not sent, including an override naming another template by name, stored resolved (m1b-describe-lp-to-pco,
    m5-describe-own-lt, m5-describe-after-own-lt-dist-only). The override capture is of an update to
    price-capacity-optimized; that it holds for this update to lowest-price is inferred."""
    lt = ec2.create_launch_template(LaunchTemplateName=_uid("lt-tolp"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    lt2 = ec2.create_launch_template(LaunchTemplateName=_uid("lt-tolp2"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    name = _uid("asg-tolp")
    policy = _mixed_policy(lt["LaunchTemplateId"], SpotAllocationStrategy="price-capacity-optimized")
    policy["LaunchTemplate"]["Overrides"][1]["LaunchTemplateSpecification"] = {
        "LaunchTemplateName": lt2["LaunchTemplateName"], "Version": "1"}
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name, MinSize=0, MaxSize=0, DesiredCapacity=0, AvailabilityZones=["us-east-1a"],
        MixedInstancesPolicy=policy)
    try:
        before = _distribution(autoscaling, name)["LaunchTemplate"]
        assert before["Overrides"][1] == {"InstanceType": "t3a.micro", "LaunchTemplateSpecification": {
            "LaunchTemplateId": lt2["LaunchTemplateId"], "LaunchTemplateName": lt2["LaunchTemplateName"],
            "Version": "1"}}
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=name, MixedInstancesPolicy={"InstancesDistribution": {
            "SpotAllocationStrategy": "lowest-price", "SpotInstancePools": 5}})
        assert _distribution(autoscaling, name)["LaunchTemplate"] == before
        assert _distribution(autoscaling, name)["InstancesDistribution"] == {
            "OnDemandAllocationStrategy": "prioritized", "OnDemandBaseCapacity": 0,
            "OnDemandPercentageAboveBaseCapacity": 0, "SpotAllocationStrategy": "lowest-price", "SpotInstancePools": 5}
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=name, MixedInstancesPolicy={"InstancesDistribution": {
            "SpotAllocationStrategy": "lowest-price"}})
        assert _distribution(autoscaling, name)["InstancesDistribution"]["SpotInstancePools"] == 5
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
        ec2.delete_launch_template(LaunchTemplateId=lt["LaunchTemplateId"])
        ec2.delete_launch_template(LaunchTemplateId=lt2["LaunchTemplateId"])


# ---------------------------------------------------------------------------
# Instance Refresh
# ---------------------------------------------------------------------------


def test_start_and_describe_instance_refresh(autoscaling, ec2):
    _ensure_my_template(ec2)
    asg = _uid("asg-refresh")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchTemplate={"LaunchTemplateName": "my-template", "Version": "$Latest"},
    )
    try:
        start = autoscaling.start_instance_refresh(
            AutoScalingGroupName=asg,
            Preferences={"MinHealthyPercentage": 90, "InstanceWarmup": 0},
        )
        refresh_id = start["InstanceRefreshId"]
        assert refresh_id

        resp = autoscaling.describe_instance_refreshes(AutoScalingGroupName=asg)
        refreshes = resp["InstanceRefreshes"]
        assert len(refreshes) == 1
        r = refreshes[0]
        assert r["InstanceRefreshId"] == refresh_id
        assert r["AutoScalingGroupName"] == asg
        assert r["Status"] == "Successful"
        assert r["PercentageComplete"] == 100

        # Filter by id round-trips.
        filtered = autoscaling.describe_instance_refreshes(
            AutoScalingGroupName=asg, InstanceRefreshIds=[refresh_id]
        )
        assert filtered["InstanceRefreshes"][0]["InstanceRefreshId"] == refresh_id
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


def test_start_instance_refresh_unknown_asg_fails(autoscaling):
    with pytest.raises(ClientError) as exc:
        autoscaling.start_instance_refresh(AutoScalingGroupName=_uid("missing"))
    assert "ValidationError" in str(exc.value)


def test_cancel_instance_refresh_when_none_active(autoscaling, ec2):
    _ensure_my_template(ec2)
    asg = _uid("asg-cancel")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=asg,
        MinSize=0,
        MaxSize=1,
        AvailabilityZones=["us-east-1a"],
        LaunchTemplate={"LaunchTemplateName": "my-template", "Version": "$Latest"},
    )
    try:
        # Refresh completes immediately, so there is nothing active to cancel.
        autoscaling.start_instance_refresh(AutoScalingGroupName=asg)
        with pytest.raises(ClientError) as exc:
            autoscaling.cancel_instance_refresh(AutoScalingGroupName=asg)
        assert "ActiveInstanceRefreshNotFound" in str(exc.value)
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)


# ---------------------------------------------------------------------------
# Mock in-service instances (capacity waiters)
# ---------------------------------------------------------------------------


def test_asg_reports_desired_capacity_instances(autoscaling):
    name = _uid("asg-cap")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=1,
        MaxSize=5,
        DesiredCapacity=3,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        g = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[name]
        )["AutoScalingGroups"][0]
        instances = g["Instances"]
        assert len(instances) == 3
        for inst in instances:
            assert inst["InstanceId"].startswith("i-")
            assert inst["LifecycleState"] == "InService"
            assert inst["HealthStatus"] == "Healthy"
            assert inst["AvailabilityZone"] == "us-east-1a"

        described = autoscaling.describe_auto_scaling_instances()["AutoScalingInstances"]
        for_group = [i for i in described if i["AutoScalingGroupName"] == name]
        assert len(for_group) == 3
        assert {i["InstanceId"] for i in for_group} == {i["InstanceId"] for i in instances}
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)


def test_set_desired_capacity_reconciles_instances(autoscaling):
    name = _uid("asg-set")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=0,
        MaxSize=5,
        DesiredCapacity=1,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        autoscaling.set_desired_capacity(AutoScalingGroupName=name, DesiredCapacity=4)
        g = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[name]
        )["AutoScalingGroups"][0]
        assert len(g["Instances"]) == 4
        assert all(i["LifecycleState"] == "InService" for i in g["Instances"])

        autoscaling.set_desired_capacity(AutoScalingGroupName=name, DesiredCapacity=2)
        g = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[name]
        )["AutoScalingGroups"][0]
        assert len(g["Instances"]) == 2
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)


def test_update_asg_reconciles_instances(autoscaling):
    name = _uid("asg-upd-cap")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=0,
        MaxSize=5,
        DesiredCapacity=0,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        assert autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[name]
        )["AutoScalingGroups"][0]["Instances"] == []
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=name, DesiredCapacity=2)
        g = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[name]
        )["AutoScalingGroups"][0]
        assert len(g["Instances"]) == 2
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)


def test_asg_instances_round_robin_azs(autoscaling):
    name = _uid("asg-az")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=0,
        MaxSize=5,
        DesiredCapacity=4,
        AvailabilityZones=["us-east-1a", "us-east-1b"],
        LaunchConfigurationName="dummy-lc",
    )
    try:
        instances = autoscaling.describe_auto_scaling_groups(
            AutoScalingGroupNames=[name]
        )["AutoScalingGroups"][0]["Instances"]
        azs = [i["AvailabilityZone"] for i in instances]
        assert azs == ["us-east-1a", "us-east-1b", "us-east-1a", "us-east-1b"]
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)


def test_asg_launch_template_request_validation_follows_aws(autoscaling, ec2):
    """#2482, captured on AWS: both or neither of id and name, a non-numeric version, a short lt- id and
    another region's id answer AWS's ValidationError messages; malformed ids keep theirs."""
    lt = ec2.create_launch_template(LaunchTemplateName=_uid("lt-2482"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    import boto3
    from conftest import ENDPOINT

    west = boto3.client("ec2", endpoint_url=ENDPOINT, region_name="us-west-2",
                        aws_access_key_id="test", aws_secret_access_key="test")
    west_lt = west.create_launch_template(LaunchTemplateName=_uid("lt-2482-west"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    asg = _uid("asg-2482")
    either = "Valid requests must contain either launchTemplateId or LaunchTemplateName"
    cases = [
        ({"LaunchTemplateId": lt["LaunchTemplateId"], "LaunchTemplateName": lt["LaunchTemplateName"]}, either),
        ({"Version": "1"}, either),
        ({"LaunchTemplateId": lt["LaunchTemplateId"], "Version": "abc"},
         "Invalid launch template version: either '$Default', '$Latest', or a numeric version are allowed."),
        ({"LaunchTemplateId": "lt-0123"}, "The specified launch template, with template ID lt-0123, does not exist."),
        ({"LaunchTemplateId": west_lt["LaunchTemplateId"]},
         f"The specified launch template, with template ID {west_lt['LaunchTemplateId']}, does not exist."),
        ({"LaunchTemplateId": lt["LaunchTemplateName"]}, f"Malformed launch template Id: {lt['LaunchTemplateName']}"),
        ({"LaunchTemplateId": "bogus"}, "Malformed launch template Id: bogus"),
        ({"LaunchTemplateId": "lt-0123456789abcdef0"}, "Malformed launch template Id: lt-0123456789abcdef0"),
        # Captured 2026-10-29: 17 or more digits, or not lowercase hex, is malformed whatever the length.
        ({"LaunchTemplateId": "lt-0fedcba98765432100"}, "Malformed launch template Id: lt-0fedcba98765432100"),
        ({"LaunchTemplateId": "lt-0123456789abcdef0123"}, "Malformed launch template Id: lt-0123456789abcdef0123"),
        ({"LaunchTemplateId": "LT-0123456789ABCDEF0"}, "Malformed launch template Id: LT-0123456789ABCDEF0"),
    ]
    try:
        for spec, message in cases:
            with pytest.raises(ClientError) as exc:
                autoscaling.create_auto_scaling_group(AutoScalingGroupName=asg, MinSize=0, MaxSize=0,
                                                      AvailabilityZones=["us-east-1a"], LaunchTemplate=spec)
            assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
                "ValidationError", message), spec
        autoscaling.create_auto_scaling_group(AutoScalingGroupName=asg, MinSize=0, MaxSize=0,
                                              AvailabilityZones=["us-east-1a"],
                                              LaunchTemplate={"LaunchTemplateId": lt["LaunchTemplateId"]})
        with pytest.raises(ClientError) as exc:
            autoscaling.update_auto_scaling_group(AutoScalingGroupName=asg, LaunchTemplate={"Version": "1"})
        assert exc.value.response["Error"]["Message"] == either
    finally:
        try:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=asg)
        except ClientError:
            pass
        ec2.delete_launch_template(LaunchTemplateId=lt["LaunchTemplateId"])
        west.delete_launch_template(LaunchTemplateId=west_lt["LaunchTemplateId"])


def test_xml_special_characters_are_escaped(autoscaling):
    name = _uid("asg-esc")
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name, MinSize=0, MaxSize=1, AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc", Tags=[{"Key": "team", "Value": "R&D <ops>"}])
    try:
        autoscaling.put_lifecycle_hook(AutoScalingGroupName=name, LifecycleHookName="hook",
                                       LifecycleTransition="autoscaling:EC2_INSTANCE_LAUNCHING")
        group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"][0]
        assert group["Tags"][0]["Value"] == "R&D <ops>"
        tags = autoscaling.describe_tags(Filters=[{"Name": "auto-scaling-group", "Values": [name]}])["Tags"]
        assert [t["Value"] for t in tags] == ["R&D <ops>"]
        with pytest.raises(ClientError) as exc:
            autoscaling.set_desired_capacity(AutoScalingGroupName="no&such<group>", DesiredCapacity=0)
        assert "no&such<group>" in exc.value.response["Error"]["Message"]
        # UpdateAutoScalingGroup doesn't echo the name (captured, us-east-2, 2026-11-05).
        with pytest.raises(ClientError) as exc:
            autoscaling.update_auto_scaling_group(AutoScalingGroupName="no&such<group>", MinSize=0)
        assert exc.value.response["Error"]["Message"] == "AutoScalingGroup name not found - null"
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)


def test_describe_filters_and_names(autoscaling):
    a, b = _uid("asg-fa"), _uid("asg-fb")
    for name, env in ((a, "prod"), (b, "dev")):
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=name, MinSize=0, MaxSize=1, AvailabilityZones=["us-east-1a"],
            LaunchConfigurationName="dummy-lc",
            Tags=[{"Key": "env", "Value": env, "PropagateAtLaunch": env == "prod"}])
    try:
        def groups(filters):
            resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[a, b], Filters=filters)
            return {g["AutoScalingGroupName"] for g in resp["AutoScalingGroups"]}

        assert groups([{"Name": "tag:env", "Values": ["prod"]}]) == {a}
        assert groups([{"Name": "tag-value", "Values": ["dev", "prod"]}]) == {a, b}
        assert groups([{"Name": "tag-key", "Values": ["nope"]}]) == set()

        tags = autoscaling.describe_tags(Filters=[{"Name": "auto-scaling-group", "Values": [a, b]},
                                                  {"Name": "propagate-at-launch", "Values": ["true"]}])["Tags"]
        assert [t["ResourceId"] for t in tags] == [a]

        for policy in ("p1", "p2"):
            autoscaling.put_scaling_policy(AutoScalingGroupName=a, PolicyName=policy,
                                           AdjustmentType="ChangeInCapacity", ScalingAdjustment=1)
        policies = autoscaling.describe_policies(AutoScalingGroupName=a, PolicyNames=["p2", "ghost"])
        assert [p["PolicyName"] for p in policies["ScalingPolicies"]] == ["p2"]

        for hook in ("h1", "h2"):
            autoscaling.put_lifecycle_hook(AutoScalingGroupName=a, LifecycleHookName=hook,
                                           LifecycleTransition="autoscaling:EC2_INSTANCE_LAUNCHING")
        hooks = autoscaling.describe_lifecycle_hooks(AutoScalingGroupName=a, LifecycleHookNames=["h1"])
        assert [h["LifecycleHookName"] for h in hooks["LifecycleHooks"]] == ["h1"]

        for action in ("s1", "s2"):
            autoscaling.put_scheduled_update_group_action(AutoScalingGroupName=a, ScheduledActionName=action,
                                                          Recurrence="0 9 * * *", MinSize=0)
        actions = autoscaling.describe_scheduled_actions(AutoScalingGroupName=a, ScheduledActionNames=["s2"])
        assert [s["ScheduledActionName"] for s in actions["ScheduledUpdateGroupActions"]] == ["s2"]
    finally:
        for name in (a, b):
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)


def test_delete_asg_with_instances_requires_force_delete(autoscaling):
    name = _uid("asg-inuse")
    autoscaling.create_auto_scaling_group(AutoScalingGroupName=name, MinSize=1, MaxSize=1,
                                          AvailabilityZones=["us-east-1a"], LaunchConfigurationName="dummy-lc")
    with pytest.raises(ClientError) as exc:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)
    err = exc.value.response
    assert (err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"]) == (400, "ResourceInUse")
    autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
    assert autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"] == []

_IR = {"VCpuCount": {"Min": 1, "Max": 2}, "MemoryMiB": {"Min": 512}}


def _capacity_type(autoscaling, name):
    return autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"][0].get(
        "DesiredCapacityType")


def _validation_message(exc):
    assert exc.value.response["Error"]["Code"] == "ValidationError"
    return exc.value.response["Error"]["Message"]


def _unvalidated_autoscaling():
    return make_client("autoscaling", {"parameter_validation": False})


_TYPE_EMPTY = ("Value '' at 'desiredCapacityType' failed to satisfy constraint: Member must have length greater "
               "than or equal to 1")
_NAME_NULL = "Value null at 'autoScalingGroupName' failed to satisfy constraint: Member must not be null"
_LENGTH_MESSAGE = f"1 validation error detected: {_TYPE_EMPTY}"


def _enum_message(value):
    return f"Invalid DesiredCapacityType value '{value}', it should be one of vcpu, memory-mib, units"


def _create_requirements_message(value):
    return (f"The DesiredCapacityType {value} is only allowed in mixed instances groups containing "
            "InstanceRequirements. Please include a MixedInstancesPolicy with InstanceRequirements for your group "
            "and try again.")


def _update_requirements_message(value):
    return (f"The DesiredCapacityType {value} is only allowed in mixed instances groups using instance "
            "requirements. Please include instance requirements in the launch template or launch template "
            "overrides and try again.")


@pytest.fixture
def capacity_type_templates(ec2):
    plain = ec2.create_launch_template(LaunchTemplateName=_uid("lt-dct"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    with_ir = ec2.create_launch_template(LaunchTemplateName=_uid("lt-dct-ir"), LaunchTemplateData={
        "InstanceRequirements": _IR, "ImageId": "ami-12345678"})["LaunchTemplate"]
    spec = {"LaunchTemplateId": plain["LaunchTemplateId"], "Version": "$Latest"}
    ir_spec = {"LaunchTemplateId": with_ir["LaunchTemplateId"], "Version": "$Latest"}
    yield {
        "spec": spec,
        "ir": {"LaunchTemplate": {"LaunchTemplateSpecification": spec, "Overrides": [{"InstanceRequirements": _IR}]}},
        "types": {"LaunchTemplate": {"LaunchTemplateSpecification": spec,
                                     "Overrides": [{"InstanceType": "t3.micro"}]}},
        "template_ir": {"LaunchTemplate": {"LaunchTemplateSpecification": ir_spec}},
        "template_ir_types": {"LaunchTemplate": {"LaunchTemplateSpecification": ir_spec,
                                                 "Overrides": [{"InstanceType": "t3.micro"}]}},
    }
    ec2.delete_launch_template(LaunchTemplateId=plain["LaunchTemplateId"])
    ec2.delete_launch_template(LaunchTemplateId=with_ir["LaunchTemplateId"])


_DCT_BASE = dict(MinSize=0, MaxSize=1, DesiredCapacity=0, AvailabilityZones=["us-east-1a"])


def test_asg_desired_capacity_type_follows_aws(autoscaling, capacity_type_templates):
    """#2604, captured on AWS (us-east-2, 2026-11-05): only a group with a MixedInstancesPolicy reports a
    DesiredCapacityType, "units" included; vcpu and memory-mib need InstanceRequirements in the policy's
    overrides or, without overrides, in its launch template."""
    t = capacity_type_templates
    groups = []
    try:
        for suffix, kwargs, expected in (
                ("unset", {"LaunchTemplate": t["spec"]}, None),
                ("units", {"LaunchTemplate": t["spec"], "DesiredCapacityType": "units"}, None),
                ("unset-mip", {"MixedInstancesPolicy": t["ir"]}, None),
                ("units-mip", {"MixedInstancesPolicy": t["types"], "DesiredCapacityType": "units"}, "units"),
                ("vcpu", {"MixedInstancesPolicy": t["ir"], "DesiredCapacityType": "vcpu"}, "vcpu"),
                ("memory", {"MixedInstancesPolicy": t["ir"], "DesiredCapacityType": "memory-mib"}, "memory-mib"),
                ("lt-ir", {"MixedInstancesPolicy": t["template_ir"], "DesiredCapacityType": "vcpu"}, "vcpu")):
            name = _uid(f"asg-dct-{suffix}")
            autoscaling.create_auto_scaling_group(AutoScalingGroupName=name, **_DCT_BASE, **kwargs)
            groups.append(name)
            assert _capacity_type(autoscaling, name) == expected

        for kwargs, value in (({"LaunchTemplate": t["spec"]}, "vcpu"), ({"LaunchTemplate": t["spec"]}, "memory-mib"),
                              ({"MixedInstancesPolicy": t["types"]}, "vcpu"),
                              ({"MixedInstancesPolicy": t["types"]}, "memory-mib"),
                              ({"MixedInstancesPolicy": t["template_ir_types"]}, "vcpu")):
            with pytest.raises(ClientError) as exc:
                autoscaling.create_auto_scaling_group(AutoScalingGroupName=_uid("asg-dct-bad"), **_DCT_BASE,
                                                      **kwargs, DesiredCapacityType=value)
            assert _validation_message(exc) == _create_requirements_message(value)
        for value in ("bogus", "Units"):
            with pytest.raises(ClientError) as exc:
                autoscaling.create_auto_scaling_group(AutoScalingGroupName=_uid("asg-dct-bad"), **_DCT_BASE,
                                                      LaunchTemplate=t["spec"], DesiredCapacityType=value)
            assert _validation_message(exc) == _enum_message(value)
    finally:
        for name in groups:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)


def test_asg_desired_capacity_type_validation_order_follows_aws(autoscaling, capacity_type_templates):
    """#2604, captured on AWS (us-east-2, 2026-11-05). CreateAutoScalingGroup: the empty value, then the launch
    source, the enum, the launch template, InstanceRequirements and last an existing name.
    UpdateAutoScalingGroup: the empty value, both launch sources, the enum, then the group ("AutoScalingGroup name
    not found - null")."""
    t = capacity_type_templates
    unvalidated = _unvalidated_autoscaling()
    both = {"LaunchTemplate": t["spec"], "MixedInstancesPolicy": t["ir"]}
    missing_lt = {"LaunchTemplate": {"LaunchTemplateName": _uid("missing-lt")}}
    source_message = ("Valid requests must contain either LaunchTemplate, LaunchConfigurationName, InstanceId or "
                      "MixedInstancesPolicy parameter.")
    existing = _uid("asg-dct-order")
    autoscaling.create_auto_scaling_group(AutoScalingGroupName=existing, LaunchTemplate=t["spec"], **_DCT_BASE)
    try:
        for name, kwargs, value, message in (
                (_uid("asg-dct-order"), both, "", _LENGTH_MESSAGE),
                (_uid("asg-dct-order"), {}, "", _LENGTH_MESSAGE),
                (_uid("asg-dct-order"), both, "bogus", source_message),
                (_uid("asg-dct-order"), {}, "vcpu", source_message),
                (_uid("asg-dct-order"), missing_lt, "bogus", _enum_message("bogus")),
                (_uid("asg-dct-order"), missing_lt, "vcpu",
                 f"The specified launch template, with template name {missing_lt['LaunchTemplate']['LaunchTemplateName']}, "
                 "does not exist."),
                (existing, {"LaunchTemplate": t["spec"]}, "", _LENGTH_MESSAGE),
                (existing, {"LaunchTemplate": t["spec"]}, "bogus", _enum_message("bogus")),
                (existing, {"LaunchTemplate": t["spec"]}, "vcpu", _create_requirements_message("vcpu"))):
            with pytest.raises(ClientError) as exc:
                unvalidated.create_auto_scaling_group(AutoScalingGroupName=name, **_DCT_BASE, **kwargs,
                                                      DesiredCapacityType=value)
            assert _validation_message(exc) == message

        missing = _uid("no-such-group")
        update_source = ("Valid requests must contain either LaunchTemplate, LaunchConfigurationName or "
                         "MixedInstancesPolicy parameter.")
        for name, kwargs, message in (
                (missing, {"DesiredCapacityType": ""}, _LENGTH_MESSAGE),
                (existing, {**both, "DesiredCapacityType": ""}, _LENGTH_MESSAGE),
                (missing, both, update_source),
                (existing, {**both, "DesiredCapacityType": "bogus"}, update_source),
                (missing, {"DesiredCapacityType": "bogus"}, _enum_message("bogus")),
                (missing, {"DesiredCapacityType": "vcpu"}, "AutoScalingGroup name not found - null"),
                (missing, {"MaxSize": 1}, "AutoScalingGroup name not found - null"),
                (missing, {**missing_lt, "DesiredCapacityType": "vcpu"}, "AutoScalingGroup name not found - null"),
                (existing, {**missing_lt, "DesiredCapacityType": "bogus"}, _enum_message("bogus"))):
            with pytest.raises(ClientError) as exc:
                unvalidated.update_auto_scaling_group(AutoScalingGroupName=name, **kwargs)
            assert _validation_message(exc) == message
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=existing, ForceDelete=True)


def test_asg_update_desired_capacity_type_follows_aws(autoscaling, capacity_type_templates):
    """#2604, captured on AWS (us-east-2, 2026-11-05): an update sets vcpu or memory-mib alone, but "units"
    only together with a MixedInstancesPolicy; leaving the member out keeps the type; a group moved off its
    MixedInstancesPolicy loses it, and a vcpu or memory-mib group can't move to a source without
    InstanceRequirements."""
    t = capacity_type_templates
    plain, mixed, typed = _uid("asg-dct-up"), _uid("asg-dct-up-mip"), _uid("asg-dct-up-types")
    try:
        autoscaling.create_auto_scaling_group(AutoScalingGroupName=plain, LaunchTemplate=t["spec"], **_DCT_BASE)
        autoscaling.create_auto_scaling_group(AutoScalingGroupName=mixed, MixedInstancesPolicy=t["ir"],
                                              DesiredCapacityType="vcpu", **_DCT_BASE)
        autoscaling.create_auto_scaling_group(AutoScalingGroupName=typed, MixedInstancesPolicy=t["types"],
                                              **_DCT_BASE)

        autoscaling.update_auto_scaling_group(AutoScalingGroupName=plain, DesiredCapacityType="units")
        assert _capacity_type(autoscaling, plain) is None
        for value, message in (("vcpu", _update_requirements_message("vcpu")),
                               ("memory-mib", _update_requirements_message("memory-mib")),
                               ("bogus", _enum_message("bogus"))):
            with pytest.raises(ClientError) as exc:
                autoscaling.update_auto_scaling_group(AutoScalingGroupName=plain, DesiredCapacityType=value)
            assert _validation_message(exc) == message
        assert _capacity_type(autoscaling, plain) is None

        dist = {"InstancesDistribution": {"OnDemandBaseCapacity": 0}}
        for kwargs, expected in (({"MaxSize": 1}, "vcpu"), ({"DesiredCapacityType": "memory-mib"}, "memory-mib"),
                                 ({"DesiredCapacityType": "units"}, "memory-mib"),
                                 ({"MixedInstancesPolicy": dist, "DesiredCapacityType": "units"}, "units"),
                                 ({"DesiredCapacityType": "vcpu"}, "vcpu"),
                                 ({"MixedInstancesPolicy": t["template_ir"]}, "vcpu")):
            autoscaling.update_auto_scaling_group(AutoScalingGroupName=mixed, **kwargs)
            assert _capacity_type(autoscaling, mixed) == expected
        for kwargs in ({"LaunchTemplate": t["spec"]}, {"MixedInstancesPolicy": t["types"]},
                       {"LaunchTemplate": t["spec"], "DesiredCapacityType": "vcpu"}):
            with pytest.raises(ClientError) as exc:
                autoscaling.update_auto_scaling_group(AutoScalingGroupName=mixed, **kwargs)
            assert _validation_message(exc) == _update_requirements_message("vcpu")
        group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[mixed])["AutoScalingGroups"][0]
        assert (group["DesiredCapacityType"], "MixedInstancesPolicy" in group) == ("vcpu", True)
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=mixed, LaunchTemplate=t["spec"],
                                              DesiredCapacityType="units")
        group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[mixed])["AutoScalingGroups"][0]
        assert ("DesiredCapacityType" in group, "MixedInstancesPolicy" in group) == (False, False)

        autoscaling.update_auto_scaling_group(AutoScalingGroupName=typed, DesiredCapacityType="units")
        assert _capacity_type(autoscaling, typed) is None
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=typed, MixedInstancesPolicy=t["types"],
                                              DesiredCapacityType="units")
        assert _capacity_type(autoscaling, typed) == "units"
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=typed, LaunchTemplate=t["spec"])
        assert _capacity_type(autoscaling, typed) is None
    finally:
        for name in (plain, mixed, typed):
            try:
                autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
            except ClientError:
                pass


def test_asg_desired_capacity_type_template_and_override_rules_follow_aws(autoscaling, ec2, capacity_type_templates):
    """#2604, captured on AWS (us-east-2, 2026-11-05): the policy's launch template counts at the version it
    names ($Default when none); an override's own launch template doesn't count; a memory-mib group can't move
    to a source without InstanceRequirements; a missing name and an empty type are reported together."""
    t = capacity_type_templates
    versioned = ec2.create_launch_template(LaunchTemplateName=_uid("lt-dct-ver"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    ec2.create_launch_template_version(LaunchTemplateId=versioned["LaunchTemplateId"], LaunchTemplateData={
        "InstanceRequirements": _IR, "ImageId": "ami-12345678"})
    ir_template = t["template_ir"]["LaunchTemplate"]["LaunchTemplateSpecification"]
    groups = []
    try:
        for version, accepted in ((None, False), ("$Default", False), ("1", False), ("$Latest", True), ("2", True)):
            spec = {"LaunchTemplateId": versioned["LaunchTemplateId"], **({"Version": version} if version else {})}
            kwargs = dict(AutoScalingGroupName=_uid("asg-dct-ver"), DesiredCapacityType="vcpu", **_DCT_BASE,
                          MixedInstancesPolicy={"LaunchTemplate": {"LaunchTemplateSpecification": spec}})
            if accepted:
                autoscaling.create_auto_scaling_group(**kwargs)
                groups.append(kwargs["AutoScalingGroupName"])
                assert _capacity_type(autoscaling, kwargs["AutoScalingGroupName"]) == "vcpu"
            else:
                with pytest.raises(ClientError) as exc:
                    autoscaling.create_auto_scaling_group(**kwargs)
                assert _validation_message(exc) == _create_requirements_message("vcpu")

        for override, accepted in (({"InstanceType": "t3.micro", "LaunchTemplateSpecification": ir_template}, False),
                                   ({"InstanceRequirements": _IR, "LaunchTemplateSpecification": ir_template}, True)):
            kwargs = dict(AutoScalingGroupName=_uid("asg-dct-ov"), DesiredCapacityType="vcpu", **_DCT_BASE,
                          MixedInstancesPolicy={"LaunchTemplate": {"LaunchTemplateSpecification": t["spec"],
                                                                   "Overrides": [override]}})
            if accepted:
                autoscaling.create_auto_scaling_group(**kwargs)
                groups.append(kwargs["AutoScalingGroupName"])
                assert _capacity_type(autoscaling, kwargs["AutoScalingGroupName"]) == "vcpu"
            else:
                with pytest.raises(ClientError) as exc:
                    autoscaling.create_auto_scaling_group(**kwargs)
                assert _validation_message(exc) == _create_requirements_message("vcpu")

        memory = _uid("asg-dct-mem")
        autoscaling.create_auto_scaling_group(AutoScalingGroupName=memory, MixedInstancesPolicy=t["ir"],
                                              DesiredCapacityType="memory-mib", **_DCT_BASE)
        groups.append(memory)
        for kwargs in ({"LaunchTemplate": t["spec"]}, {"MixedInstancesPolicy": t["types"]}):
            with pytest.raises(ClientError) as exc:
                autoscaling.update_auto_scaling_group(AutoScalingGroupName=memory, **kwargs)
            assert _validation_message(exc) == _update_requirements_message("memory-mib")
        assert _capacity_type(autoscaling, memory) == "memory-mib"

        unvalidated = _unvalidated_autoscaling()
        for call, kwargs in ((unvalidated.create_auto_scaling_group, dict(**_DCT_BASE, LaunchTemplate=t["spec"])),
                             (unvalidated.update_auto_scaling_group, {})):
            for value, message in (("", f"2 validation errors detected: {_TYPE_EMPTY}; {_NAME_NULL}"),
                                   ("bogus", f"1 validation error detected: {_NAME_NULL}"),
                                   ("vcpu", f"1 validation error detected: {_NAME_NULL}")):
                with pytest.raises(ClientError) as exc:
                    call(**kwargs, DesiredCapacityType=value)
                assert _validation_message(exc) == message
    finally:
        for name in groups:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
        ec2.delete_launch_template(LaunchTemplateId=versioned["LaunchTemplateId"])


def test_asg_desired_capacity_type_recheck_and_names_follow_aws(autoscaling, ec2, capacity_type_templates):
    """#2604, captured on AWS (us-east-2, 2026-11-05): a stored vcpu type is checked again only when an update
    changes the launch source or sends a type, so a template whose $Latest lost InstanceRequirements still
    takes unrelated updates; AlreadyExists comes after the launch source checks; an empty group name has its
    own constraint error."""
    t = capacity_type_templates
    versioned = ec2.create_launch_template(LaunchTemplateName=_uid("lt-dct-re"), LaunchTemplateData={
        "InstanceRequirements": _IR, "ImageId": "ami-12345678"})["LaunchTemplate"]
    latest = {"LaunchTemplateId": versioned["LaunchTemplateId"], "Version": "$Latest"}
    first, second = _uid("asg-dct-re"), _uid("asg-dct-re-v1")
    try:
        for name in (first, second):
            autoscaling.create_auto_scaling_group(AutoScalingGroupName=name, DesiredCapacityType="vcpu", **_DCT_BASE,
                                                  MixedInstancesPolicy={"LaunchTemplate": {
                                                      "LaunchTemplateSpecification": latest}})
        ec2.create_launch_template_version(LaunchTemplateId=versioned["LaunchTemplateId"], LaunchTemplateData={
            "InstanceType": "t3.micro", "ImageId": "ami-12345678"})

        for kwargs in ({"MaxSize": 1}, {"MixedInstancesPolicy": {"InstancesDistribution": {"OnDemandBaseCapacity": 0}}},
                       {"DesiredCapacityType": "units"}):
            autoscaling.update_auto_scaling_group(AutoScalingGroupName=first, **kwargs)
            assert _capacity_type(autoscaling, first) == "vcpu"
        for kwargs, value in (({"DesiredCapacityType": "vcpu"}, "vcpu"),
                              ({"DesiredCapacityType": "memory-mib"}, "memory-mib"),
                              ({"MixedInstancesPolicy": {"LaunchTemplate": {
                                  "Overrides": [{"InstanceType": "t3.micro"}]}}}, "vcpu"),
                              ({"MixedInstancesPolicy": {"LaunchTemplate": {"LaunchTemplateSpecification": latest}}},
                               "vcpu"),
                              ({"LaunchTemplate": t["spec"]}, "vcpu")):
            with pytest.raises(ClientError) as exc:
                autoscaling.update_auto_scaling_group(AutoScalingGroupName=first, **kwargs)
            assert _validation_message(exc) == _update_requirements_message(value)
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=first, MixedInstancesPolicy={
            "LaunchTemplate": {"Overrides": [{"InstanceRequirements": _IR}]}})
        assert _capacity_type(autoscaling, first) == "vcpu"
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=second, MixedInstancesPolicy={"LaunchTemplate": {
            "LaunchTemplateSpecification": {"LaunchTemplateId": versioned["LaunchTemplateId"], "Version": "1"}}})
        assert _capacity_type(autoscaling, second) == "vcpu"

        missing_lt = {"LaunchTemplate": {"LaunchTemplateName": _uid("missing-lt")}}
        source_message = ("Valid requests must contain either LaunchTemplate, LaunchConfigurationName, InstanceId or "
                          "MixedInstancesPolicy parameter.")
        for kwargs, message in (
                (missing_lt, f"The specified launch template, with template name "
                             f"{missing_lt['LaunchTemplate']['LaunchTemplateName']}, does not exist."),
                ({"LaunchTemplate": t["spec"], "MixedInstancesPolicy": t["ir"]}, source_message),
                ({}, source_message),
                ({"MixedInstancesPolicy": {"InstancesDistribution": {"OnDemandBaseCapacity": 0}}},
                 "A launch template is required when configuring an Auto Scaling group with a mixed instances policy. "
                 "Add a launch template to the mixed instances policy and try again.")):
            with pytest.raises(ClientError) as exc:
                autoscaling.create_auto_scaling_group(AutoScalingGroupName=first, **_DCT_BASE, **kwargs)
            assert _validation_message(exc) == message
        with pytest.raises(ClientError) as exc:
            autoscaling.create_auto_scaling_group(AutoScalingGroupName=first, **_DCT_BASE, LaunchTemplate=t["spec"])
        assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
            "AlreadyExists", f"AutoScalingGroup by this name already exists - A group with the name {first} already "
                             "exists")

        unvalidated = _unvalidated_autoscaling()
        name_empty = ("Value '' at 'autoScalingGroupName' failed to satisfy constraint: Member must have length "
                      "greater than or equal to 1")
        for call, kwargs in ((unvalidated.create_auto_scaling_group, dict(**_DCT_BASE, LaunchTemplate=t["spec"])),
                             (unvalidated.update_auto_scaling_group, {})):
            for extra, message in (({}, f"1 validation error detected: {name_empty}"),
                                   ({"DesiredCapacityType": "bogus"}, f"1 validation error detected: {name_empty}"),
                                   ({"DesiredCapacityType": ""},
                                    f"2 validation errors detected: {_TYPE_EMPTY}; {name_empty}")):
                with pytest.raises(ClientError) as exc:
                    call(AutoScalingGroupName="", **kwargs, **extra)
                assert _validation_message(exc) == message
    finally:
        for name in (first, second):
            try:
                autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
            except ClientError:
                pass
        ec2.delete_launch_template(LaunchTemplateId=versioned["LaunchTemplateId"])


def test_asg_desired_capacity_type_lengths_and_spot_order_follow_aws(autoscaling, capacity_type_templates):
    """#2604, captured on AWS (us-east-2, 2026-11-05): a DesiredCapacityType or group name longer than 255
    characters (code points) is a constraint error, reported with the others; the enum check comes before the
    MixedInstancesPolicy's Spot checks, and the InstanceRequirements check and AlreadyExists after them."""
    t = capacity_type_templates
    unvalidated = _unvalidated_autoscaling()
    long_type, long_name = "a" * 256, "n" * 256
    type_long = (f"Value '{long_type}' at 'desiredCapacityType' failed to satisfy constraint: Member must have "
                 "length less than or equal to 255")
    name_long = (f"Value '{long_name}' at 'autoScalingGroupName' failed to satisfy constraint: Member must have "
                 "length less than or equal to 255")
    group = _uid("asg-dct-len")
    autoscaling.create_auto_scaling_group(AutoScalingGroupName=group, MixedInstancesPolicy=t["types"], **_DCT_BASE)
    try:
        with pytest.raises(ClientError) as exc:
            unvalidated.create_auto_scaling_group(AutoScalingGroupName=_uid("asg-dct-len"), **_DCT_BASE,
                                                  LaunchTemplate=t["spec"], DesiredCapacityType="a" * 255)
        assert _validation_message(exc) == _enum_message("a" * 255)
        # Length counts code points: 128 emoji are 256 UTF-16 units but still reach the enum check.
        emoji = "\U0001F600"
        with pytest.raises(ClientError) as exc:
            unvalidated.create_auto_scaling_group(AutoScalingGroupName=_uid("asg-dct-len"), **_DCT_BASE,
                                                  LaunchTemplate=t["spec"], DesiredCapacityType=emoji * 128)
        assert _validation_message(exc) == _enum_message(emoji * 128)
        with pytest.raises(ClientError) as exc:
            unvalidated.create_auto_scaling_group(AutoScalingGroupName=_uid("asg-dct-len"), **_DCT_BASE,
                                                  LaunchTemplate=t["spec"], DesiredCapacityType=emoji * 256)
        assert _validation_message(exc) == (f"1 validation error detected: Value '{emoji * 256}' at "
                                            "'desiredCapacityType' failed to satisfy constraint: Member must have "
                                            "length less than or equal to 255")
        create = dict(**_DCT_BASE, LaunchTemplate=t["spec"])
        for call, kwargs, message in (
                (unvalidated.create_auto_scaling_group, {**create, "AutoScalingGroupName": _uid("asg-dct-len"),
                                                         "DesiredCapacityType": long_type},
                 f"1 validation error detected: {type_long}"),
                (unvalidated.create_auto_scaling_group, {**create, "DesiredCapacityType": long_type},
                 f"2 validation errors detected: {type_long}; {_NAME_NULL}"),
                (unvalidated.update_auto_scaling_group, {"AutoScalingGroupName": group, "DesiredCapacityType": long_type},
                 f"1 validation error detected: {type_long}")):
            with pytest.raises(ClientError) as exc:
                call(**kwargs)
            assert _validation_message(exc) == message
        for call, kwargs in ((unvalidated.create_auto_scaling_group, create),
                             (unvalidated.update_auto_scaling_group, {})):
            for extra, message in (({}, f"1 validation error detected: {name_long}"),
                                   ({"DesiredCapacityType": "bogus"}, f"1 validation error detected: {name_long}"),
                                   ({"DesiredCapacityType": ""}, f"2 validation errors detected: {_TYPE_EMPTY}; {name_long}"),
                                   ({"DesiredCapacityType": long_type},
                                    f"2 validation errors detected: {type_long}; {name_long}")):
                with pytest.raises(ClientError) as exc:
                    call(AutoScalingGroupName=long_name, **kwargs, **extra)
                assert _validation_message(exc) == message

        strategy_message = ("SpotAllocationStrategy is not valid. Valid options are: [lowest-price, capacity-optimized, "
                            "capacity-optimized-prioritized, price-capacity-optimized].")
        pools_message = "SpotInstancePools option is only available with the lowest-price allocation strategy."
        for dist, message in (({"SpotAllocationStrategy": "bogus"}, strategy_message),
                              ({"SpotAllocationStrategy": "capacity-optimized", "SpotInstancePools": 3}, pools_message)):
            # As captured: bogus with InstanceRequirements overrides, vcpu with InstanceType overrides.
            for value, policy, expected in (("bogus", t["ir"], _enum_message("bogus")), ("vcpu", t["types"], message)):
                with pytest.raises(ClientError) as exc:
                    autoscaling.create_auto_scaling_group(
                        AutoScalingGroupName=_uid("asg-dct-spot"), **_DCT_BASE, DesiredCapacityType=value,
                        MixedInstancesPolicy={"LaunchTemplate": policy["LaunchTemplate"], "InstancesDistribution": dist})
                assert _validation_message(exc) == expected
                with pytest.raises(ClientError) as exc:
                    autoscaling.update_auto_scaling_group(AutoScalingGroupName=group, DesiredCapacityType=value,
                                                          MixedInstancesPolicy={"InstancesDistribution": dist})
                assert _validation_message(exc) == expected
            for extra in ({}, {"DesiredCapacityType": "units"}):
                with pytest.raises(ClientError) as exc:
                    autoscaling.create_auto_scaling_group(
                        AutoScalingGroupName=group, **_DCT_BASE, **extra,
                        MixedInstancesPolicy={"LaunchTemplate": t["types"]["LaunchTemplate"], "InstancesDistribution": dist})
                assert _validation_message(exc) == message
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=group, ForceDelete=True)


def test_restore_drops_desired_capacity_type_from_groups_without_a_policy():
    """#2604: v1.5.38 saved DesiredCapacityType "units" on every group. On AWS (captured, us-east-2, 2026-11-05)
    a group without a MixedInstancesPolicy never has a type, so restored state drops it there, in every
    account and region; policy groups keep theirs."""
    from ministack.core.responses import AccountRegionScopedDict
    from ministack.services import autoscaling as service

    policy = {"LaunchTemplate": {"LaunchTemplateSpecification": {"LaunchTemplateId": "lt-0123456789abcdef0"}}}
    asgs = AccountRegionScopedDict()
    for account_id, region in (("111111111111", "us-east-1"), ("222222222222", "eu-west-1")):
        asgs.set_scoped(account_id, region, "plain", {"AutoScalingGroupName": "plain", "DesiredCapacityType": "units"})
        asgs.set_scoped(account_id, region, "mixed-units", {"AutoScalingGroupName": "mixed-units",
                                                            "DesiredCapacityType": "units",
                                                            "MixedInstancesPolicy": policy})
        asgs.set_scoped(account_id, region, "mixed-vcpu", {"AutoScalingGroupName": "mixed-vcpu",
                                                           "DesiredCapacityType": "vcpu",
                                                           "MixedInstancesPolicy": policy})
    service.reset()
    try:
        service.load_persisted_state({"asgs": asgs})
        for account_id, region in (("111111111111", "us-east-1"), ("222222222222", "eu-west-1")):
            types = {name: service._asgs.get_scoped(account_id, region, name).get("DesiredCapacityType")
                     for name in ("plain", "mixed-units", "mixed-vcpu")}
            assert types == {"plain": None, "mixed-units": "units", "mixed-vcpu": "vcpu"}
    finally:
        service.reset()
