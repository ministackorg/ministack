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
    error = exc.value.response["Error"]
    assert (exc.value.response["ResponseMetadata"]["HTTPStatusCode"], error["Code"], error["Type"]) == (
        400, "ValidationError", "Sender")
    return error["Message"]


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
        "name_spec": {"LaunchTemplateName": plain["LaunchTemplateName"], "Version": "$Latest"},
        "ir_name_spec": {"LaunchTemplateName": with_ir["LaunchTemplateName"], "Version": "$Latest"},
        "ir_name_only": {"LaunchTemplateName": with_ir["LaunchTemplateName"]},
        "ir_name_default": {"LaunchTemplateName": with_ir["LaunchTemplateName"], "Version": "$Default"},
        "ir_id_spec": ir_spec,
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


_LTNC = ("The launch template is not compatible with the configuration of the Auto Scaling group. This launch "
         "template can only be used if MixedInstancesPolicy is set on the Auto Scaling group. Specify the "
         "MixedInstancesPolicy parameter or use a launch template that contains the InstanceType parameter "
         "instead of the InstanceRequirements parameter, and try again.")
_BOTH = "The parameter InstanceType cannot be used in a launch template override using instance requirements."
_WGT = ("Not all instance types have a defined WeightedCapacity. Specify missing weights, or remove all weights, "
        "and try again.")
_OVR1 = "A launch template override must contain one of InstanceType or InstanceRequirements"
_MIX = ("You cannot mix single instance and instance requirements launch template override in the same mixed "
        "instance policy. Remove the launch template overrides that use single instance types, or the ones using "
        "instance requirements.")


def _dup(types="t3.micro"):
    return ("Cannot add same instance type override more than once. Remove these duplicates from the request and "
            f"try again: [{types}]")


_DUP = _dup()
_SOURCE = ("Valid requests must contain either LaunchTemplate, LaunchConfigurationName or MixedInstancesPolicy "
           "parameter.")
_CREATE_SOURCE = ("Valid requests must contain either LaunchTemplate, LaunchConfigurationName, InstanceId or "
                  "MixedInstancesPolicy parameter.")
_IR2 = {"VCpuCount": {"Min": 2, "Max": 4}, "MemoryMiB": {"Min": 1024}}
_W2 = {"WeightedCapacity": "2"}


def _assert_refusal(exc, code, message):
    error = exc.value.response["Error"]
    assert (exc.value.response["ResponseMetadata"]["HTTPStatusCode"], error["Code"], error["Type"],
            error["Message"]) == (400, code, "Sender", message)


def _refused(autoscaling, code, message, **kwargs):
    name = _uid("asg-ir")
    with pytest.raises(ClientError) as exc:
        autoscaling.create_auto_scaling_group(AutoScalingGroupName=name, **_DCT_BASE, **kwargs)
    _assert_refusal(exc, code, message)
    assert autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"] == []


def _policy(spec, overrides):
    return {"LaunchTemplate": {"LaunchTemplateSpecification": spec, "Overrides": overrides}}


def _stored_overrides(autoscaling, name):
    group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"][0]
    return group["MixedInstancesPolicy"]["LaunchTemplate"]["Overrides"]


@pytest.fixture
def plain_and_mip_groups(autoscaling, capacity_type_templates):
    """The groups the edge captures update: a plain LaunchTemplate group and a one-override policy group."""
    t = capacity_type_templates
    plain, mip = _uid("asg-plain"), _uid("asg-mip")
    autoscaling.create_auto_scaling_group(AutoScalingGroupName=plain, **_DCT_BASE, LaunchTemplate=t["name_spec"])
    autoscaling.create_auto_scaling_group(AutoScalingGroupName=mip, **_DCT_BASE,
                                          MixedInstancesPolicy=_policy(t["name_spec"], [{"InstanceType": "t3.micro"}]))
    yield plain, mip
    for name in (plain, mip):
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)


def test_asg_plain_launch_template_with_instance_requirements_refused(autoscaling, capacity_type_templates):
    """#2607 (lt-plain p-*): a plain LaunchTemplate resolving to an InstanceRequirements version is refused."""
    t = capacity_type_templates
    for spec in (t["ir_name_spec"], t["ir_name_only"], t["ir_name_default"], t["ir_id_spec"]):
        _refused(autoscaling, "InvalidQueryParameter", _LTNC, LaunchTemplate=spec)
    # on create the template check comes before vcpu / memory-mib
    _refused(autoscaling, "InvalidQueryParameter", _LTNC, LaunchTemplate=t["ir_id_spec"],
             DesiredCapacityType="vcpu")                                        # p-ir-id-latest-vcpu
    _refused(autoscaling, "InvalidQueryParameter", _LTNC, LaunchTemplate=t["ir_name_spec"],
             DesiredCapacityType="vcpu")                                        # p-ir-name-latest-vcpu
    _refused(autoscaling, "InvalidQueryParameter", _LTNC, LaunchTemplate=t["ir_id_spec"],
             DesiredCapacityType="memory-mib")                                  # l-lt-ir-id-memory-mib
    # an invalid DesiredCapacityType value is answered before it (p-ir-name-latest-bogus-dct)
    _refused(autoscaling, "ValidationError", _enum_message("bogus"), LaunchTemplate=t["ir_name_spec"],
             DesiredCapacityType="bogus")
    # controls: a template with InstanceType (p-ctl-it-latest, p-ctl-it-noversion, p-ctl-it-vcpu)
    for spec in (t["name_spec"], {"LaunchTemplateName": t["name_spec"]["LaunchTemplateName"]}):
        name = _uid("asg-ir-ok")
        autoscaling.create_auto_scaling_group(AutoScalingGroupName=name, **_DCT_BASE, LaunchTemplate=spec)
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
    _refused(autoscaling, "ValidationError", _create_requirements_message("vcpu"), LaunchTemplate=t["name_spec"],
             DesiredCapacityType="vcpu")


def test_asg_update_plain_group_to_instance_requirements_template(autoscaling, plain_and_mip_groups,
                                                                  capacity_type_templates):
    """#2607 (update c-plain-to-ir-*, u-plain-to-ltir*): LTNC, except that vcpu / memory-mib is answered first."""
    t, (plain, _) = capacity_type_templates, plain_and_mip_groups
    cases = (
        (t["ir_name_spec"], {"DesiredCapacityType": "units"}, _LTNC),                       # c-plain-to-ir-units
        (t["ir_name_spec"], {}, _LTNC),                                                     # u-plain-to-ltir
        (t["ir_name_default"], {}, _LTNC),                                                  # c-plain-to-ir-default
        (t["ir_name_only"], {}, _LTNC),                                                     # c-plain-to-ir-noversion
        (t["ir_id_spec"], {}, _LTNC),                                                       # c-plain-to-ir-byid
        (t["ir_name_spec"], {"DesiredCapacityType": "memory-mib"},
         _update_requirements_message("memory-mib")),                                       # c-plain-to-ir-memory
        (t["ir_name_spec"], {"DesiredCapacityType": "vcpu"}, _update_requirements_message("vcpu")),  # u-plain-to-ltir-vcpu
        (t["ir_name_default"], {"DesiredCapacityType": "vcpu"}, _update_requirements_message("vcpu")),
        (t["ir_name_only"], {"DesiredCapacityType": "vcpu"}, _update_requirements_message("vcpu")),
        (t["ir_id_spec"], {"DesiredCapacityType": "vcpu"}, _update_requirements_message("vcpu")),
    )
    for spec, extra, message in cases:
        with pytest.raises(ClientError) as exc:
            autoscaling.update_auto_scaling_group(AutoScalingGroupName=plain, LaunchTemplate=spec, **extra)
        _assert_refusal(exc, "InvalidQueryParameter" if message == _LTNC else "ValidationError", message)
    # nothing changed (c-plain-describe-after)
    group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[plain])["AutoScalingGroups"][0]
    assert group["LaunchTemplate"]["LaunchTemplateName"] == t["name_spec"]["LaunchTemplateName"]
    assert "MixedInstancesPolicy" not in group


def test_asg_create_override_shape_errors(autoscaling, capacity_type_templates):
    """#2607 (mixed-ov m-*, overrides b-/c-/w-*, ov-lt-only o-*): the override errors AWS answers on create."""
    t = capacity_type_templates
    spec, ir_spec = t["name_spec"], t["ir_name_spec"]
    it, ir, ir2 = {"InstanceType": "t3.micro"}, {"InstanceRequirements": _IR}, {"InstanceRequirements": _IR2}
    both, lto, lto_ir = dict(it, **ir), {"LaunchTemplateSpecification": spec}, {"LaunchTemplateSpecification": ir_spec}
    small = {"InstanceType": "t3.small"}
    cases = [
        # neither InstanceType nor InstanceRequirements
        (_OVR1, [lto], {}),                                                  # b-ctl-lto
        (_OVR1, [lto_ir], {"DesiredCapacityType": "vcpu"}),                  # o-lt-only-vcpu
        (_OVR1, [dict(it, **_W2), {"WeightedCapacity": "3"}], {}),           # w-it-w-weight-only
        (_OVR1, [ir, lto_ir], {}),                                           # m-ir-lt-only
        (_OVR1, [it, lto_ir], {}),                                           # m-it-lt-only
        (_OVR1, [lto, both], {}),                                            # b-lto-then-both
        (_OVR1, [lto, both], {"DesiredCapacityType": "vcpu"}),               # b-lto-then-both-vcpu
        # InstanceType entries mixed with InstanceRequirements ones
        (_MIX, [ir, it, ir2], {}),                                           # m-ir-it-ir
        (_MIX, [ir, ir2, it], {}),                                           # m-ir-ir-it
        (_MIX, [it, small, {"InstanceType": "t3.medium"}, {"InstanceType": "t3.large"},
                {"InstanceType": "t3a.micro"}, ir], {}),                     # m-it-it-it-ir
        (_MIX, [ir, dict(it, LaunchTemplateSpecification=ir_spec)], {}),     # m-ir-it-with-tpl
        # one entry holding both
        (_BOTH, [both], {}),                                                 # c-both
        (_BOTH, [both], {"DesiredCapacityType": "vcpu"}),                    # m-both-in-one-vcpu
        (_BOTH, [both, small], {}),                                          # m-both-in-one-plus-it
        (_BOTH, [both, ir2], {}),                                            # m-both-in-one-plus-ir
        (_BOTH, [both, lto], {}),                                            # b-both-then-lto
        (_BOTH, [both, lto], {"DesiredCapacityType": "vcpu"}),               # b-both-then-lto-vcpu
        (_BOTH, [dict(both, **_W2)], {}),                                    # w-both-w-alone
        # WeightedCapacity on only some entries
        (_WGT, [dict(it, **_W2), small], {}),                                # w-it-w-it
        (_WGT, [small, dict(it, **_W2)], {}),                                # w-it-it-w
        (_WGT, [dict(it, **_W2), lto], {}),                                  # w-it-w-neither
        (_WGT, [lto, dict(it, **_W2)], {}),                                  # w-neither-it-w
        (_WGT, [dict(it, **_W2), small], {"DesiredCapacityType": "vcpu"}),   # w-it-w-it-vcpu
        (_WGT, [dict(it, **_W2), small], {"DesiredCapacityType": "units"}),  # w-it-w-it-units
        (_WGT, [dict(both, **_W2), small], {}),                              # w-both-w-it
        (_WGT, [small, dict(both, **_W2)], {}),                              # w-it-both-w
        (_WGT, [dict(both, **_W2), lto], {}),                                # w-both-w-neither
        (_WGT, [lto, dict(both, **_W2)], {}),                                # w-neither-both-w
        (_WGT, [dict(both, **_W2), ir2], {}),                                # w-both-w-ir
        (_WGT, [ir2, dict(both, **_W2)], {}),                                # w-ir-both-w
        (_WGT, [dict(both, **_W2), small], {"DesiredCapacityType": "vcpu"}),  # w-both-w-it-vcpu
        (_WGT, [ir, _W2], {}),                                               # m-ir-weight-only
        (_WGT, [ir, dict(it, **_W2)], {}),                                   # m-ir-it-weighted
        (_WGT, [dict(ir, **_W2), ir2], {}),                                  # c-wgt
        # the same InstanceType twice comes before every other override check (create only)
        (_DUP, [it, it], {}),                                                # m-it-it-same-ok, e-dup-it-it
        (_DUP, [it, it, ir], {}),                                            # e-dup-it-it-ir
        (_DUP, [it, it, lto], {}),                                           # e-dup-it-it-lto
        (_DUP, [dict(it, **_W2), dict(it, WeightedCapacity="3")], {}),       # e-dup-it-w1-it-w2
        (_DUP, [dict(it, **_W2), it], {}),                                   # e-dup-it-w1-it
        (_DUP, [dict(both, **_W2), both], {}),                               # w-both-w-both
        (_DUP, [dict(both, **_W2), dict(both, WeightedCapacity="3")], {}),   # w-both-all-weighted
        # an invalid DesiredCapacityType value comes first on create (c-both-bogus-dct, o-lt-only-bogus-dct,
        # m-ir-it-bogus-dct)
        (_enum_message("bogus"), [both], {"DesiredCapacityType": "bogus"}),
        (_enum_message("bogus"), [lto_ir], {"DesiredCapacityType": "bogus"}),
        (_enum_message("bogus"), [ir, it], {"DesiredCapacityType": "bogus"}),
    ]
    for message, overrides, extra in cases:
        _refused(autoscaling, "ValidationError", message, MixedInstancesPolicy=_policy(spec, overrides), **extra)


def test_asg_create_accepts_distinct_override_lists(autoscaling, capacity_type_templates):
    """#2607 (mixed-ov m-*-ok, overrides w-ctl-*, followup f-ir-base-*): lists AWS accepts, stored as sent."""
    t = capacity_type_templates
    spec, ir_spec = t["name_spec"], t["ir_name_spec"]
    types = [{"InstanceType": name} for name in ("t3.micro", "t3.small", "t3.medium")]
    ir, ir2 = {"InstanceRequirements": _IR}, {"InstanceRequirements": _IR2}
    cases = [
        (spec, types[:2], {}),                                                           # m-it-it-ok
        (spec, types, {}),                                                               # m-it-it-it-ok
        (spec, [dict(types[0], WeightedCapacity="2"), dict(types[1], WeightedCapacity="3")], {}),  # w-ctl-it-all-weighted
        (spec, [ir], {"DesiredCapacityType": "vcpu"}),                                   # m-ir-ok
        (spec, [ir, ir2], {}),                                                           # m-ir-ir-ok-unset
        (spec, [ir, ir2], {"DesiredCapacityType": "vcpu"}),                              # m-ir-ir-ok-vcpu
        (spec, [ir, ir], {}),                                                            # m-ir-ir-same-ok
        (ir_spec, types[:2], {}),                                                        # f-ir-base-it-it
        (ir_spec, types[:1], {"DesiredCapacityType": "units"}),                          # f-ir-base-it-units
    ]
    for base, overrides, extra in cases:
        name = _uid("asg-ovr-ok")
        autoscaling.create_auto_scaling_group(AutoScalingGroupName=name, **_DCT_BASE,
                                              MixedInstancesPolicy=_policy(base, overrides), **extra)
        try:
            assert _stored_overrides(autoscaling, name) == overrides
        finally:
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
    # an InstanceRequirements base with an InstanceType override and vcpu / memory-mib (f-ir-base-it-vcpu, -memory)
    for capacity_type in ("vcpu", "memory-mib"):
        _refused(autoscaling, "ValidationError", _create_requirements_message(capacity_type),
                 MixedInstancesPolicy=_policy(ir_spec, types[:1]), DesiredCapacityType=capacity_type)


def test_asg_update_override_shape_errors_come_before_the_capacity_type_value(autoscaling, plain_and_mip_groups,
                                                                              capacity_type_templates):
    """#2607 (edges a-{plain,mip,nx}-bogus-*, overrides u-ex-*): on update every override shape error is answered
    before an invalid DesiredCapacityType value, on a plain group, a policy group and a group that does not
    exist."""
    t, (plain, mip) = capacity_type_templates, plain_and_mip_groups
    spec = t["name_spec"]
    it, ir, ir2 = {"InstanceType": "t3.micro"}, {"InstanceRequirements": _IR}, {"InstanceRequirements": _IR2}
    shapes = [
        (_OVR1, [{"LaunchTemplateSpecification": spec}]),                     # a-*-bogus-lto
        (_MIX, [ir, it]),                                                     # a-*-bogus-ir-it
        (_WGT, [dict(it, **_W2), ir]),                                        # a-*-bogus-itw-ir
        (_BOTH, [dict(it, **ir)]),                                            # a-*-bogus-both
    ]
    for group in (plain, mip, _uid("asg-nx")):
        for message, overrides in shapes:
            with pytest.raises(ClientError) as exc:
                autoscaling.update_auto_scaling_group(AutoScalingGroupName=group,
                                                      MixedInstancesPolicy=_policy(spec, overrides),
                                                      DesiredCapacityType="bogus")
            _assert_refusal(exc, "ValidationError", message)
    # without the bad value, on the policy group (u-ex-both, u-ex-both-vcpu, u-ex-wgt, u-ex-wgt-vcpu,
    # u-ex-b-both-then-lto, u-ex-b-lto-then-both)
    both_then_lto = [dict(it, **ir), {"LaunchTemplateSpecification": spec}]
    lto_then_both = [{"LaunchTemplateSpecification": spec}, dict(it, **ir)]
    for message, overrides, extras in ((_BOTH, [dict(it, **ir)], ({}, {"DesiredCapacityType": "vcpu"})),
                                       (_WGT, [dict(ir, **_W2), ir2], ({}, {"DesiredCapacityType": "vcpu"})),
                                       (_BOTH, both_then_lto, ({},)), (_OVR1, lto_then_both, ({},))):
        for extra in extras:
            with pytest.raises(ClientError) as exc:
                autoscaling.update_auto_scaling_group(AutoScalingGroupName=mip,
                                                      MixedInstancesPolicy=_policy(spec, overrides), **extra)
            _assert_refusal(exc, "ValidationError", message)
    # the source error comes before the shape errors (b-mip-lt-and-mip-lto, b-plain-lt-and-mip-ir-it)
    for group in (plain, mip):
        for overrides in ([{"LaunchTemplateSpecification": spec}], [ir, it]):
            with pytest.raises(ClientError) as exc:
                autoscaling.update_auto_scaling_group(AutoScalingGroupName=group, LaunchTemplate=spec,
                                                      MixedInstancesPolicy=_policy(spec, overrides))
            assert _validation_message(exc) == _SOURCE
    assert _stored_overrides(autoscaling, mip) == [it]


def _raw_query(action, region="us-east-1", pairs=(), **params):
    """POST a Query body; *pairs* are extra (key, value) pairs, which may repeat a key."""
    body = urllib.parse.urlencode([("Action", action), ("Version", "2011-01-01"), *params.items(), *pairs]).encode()
    req = urllib.request.Request(
        os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566") + "/", data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Authorization": ("AWS4-HMAC-SHA256 Credential=test/20260921/"
                                   f"{region}/autoscaling/aws4_request, SignedHeaders=, Signature=x")})
    try:
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()
    return 200, ""


def _raw_error(body):
    import re
    return tuple(re.search(f"<{tag}>(.*?)</{tag}>", body, re.S).group(1) for tag in ("Type", "Code", "Message"))


_WIRE_PREFIX = "MixedInstancesPolicy.LaunchTemplate."


def _wire_policy(template_name, overrides=()):
    """MixedInstancesPolicy query keys; *overrides* maps the member number to {member name: value}."""
    params = {_WIRE_PREFIX + "LaunchTemplateSpecification.LaunchTemplateName": template_name,
              _WIRE_PREFIX + "LaunchTemplateSpecification.Version": "$Latest"}
    for number, members in dict(overrides).items():
        params.update({f"{_WIRE_PREFIX}Overrides.member.{number}.{name}": value for name, value in members.items()})
    return params


def _wire_refusal(action, group, params, code, message):
    """Send a captured wire body and assert the whole refusal: status, Type, Code and Message."""
    base = {"AutoScalingGroupName": group, "MinSize": "0", "MaxSize": "1", "DesiredCapacity": "0"}
    if action == "CreateAutoScalingGroup":
        base["AvailabilityZones.member.1"] = "us-east-1a"
    status, body = _raw_query(action, **base, **params)
    assert (status, *_raw_error(body)) == (400, "Sender", code, message)


_IT = {"InstanceType": "t3.micro"}
_SMALL = {"InstanceType": "t3.small"}
_MALFORMED = "MalformedInput"


def _sparse(skipped):
    return f"Excessively sparse input would skip {skipped} list elements"


def test_asg_override_numbering_gap_is_an_empty_override(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607 (edges d-c-wire-gap-*, d-u-wire-gap-*, mixed-ov m-wire-gap-it-ir, ov-lt-only o-wire-override-gap,
    edges2 u-u-idx-2): a gap in Overrides.member.N counts as an override with neither member."""
    _, mip = plain_and_mip_groups
    name_spec = capacity_type_templates["name_spec"]["LaunchTemplateName"]
    requirements = {"InstanceRequirements.VCpuCount.Min": "1", "InstanceRequirements.VCpuCount.Max": "2",
                    "InstanceRequirements.MemoryMiB.Min": "512"}
    weighted = {1: {**_IT, "WeightedCapacity": "2"}, 3: {**_SMALL, "WeightedCapacity": "3"}}
    both_at_2 = {2: {**_IT, **requirements}}
    # d-*-wire-gap-itw-1-3 and d-*-wire-gap-both-at-2, on create and update
    for overrides, message in ((weighted, _WGT), (both_at_2, _OVR1)):
        _wire_refusal("CreateAutoScalingGroup", _uid("asg-gap"), _wire_policy(name_spec, overrides),
                      "ValidationError", message)
        _wire_refusal("UpdateAutoScalingGroup", mip, _wire_policy(name_spec, overrides), "ValidationError", message)
    # o-wire-override-gap (create) and u-u-idx-2 (update)
    _wire_refusal("CreateAutoScalingGroup", _uid("asg-gap"), _wire_policy(name_spec, {2: _IT}),
                  "ValidationError", _OVR1)
    _wire_refusal("UpdateAutoScalingGroup", mip, _wire_policy(name_spec, {2: _IT}), "ValidationError", _OVR1)
    assert _stored_overrides(autoscaling, mip) == [{"InstanceType": "t3.micro"}]
    # a gap before an InstanceRequirements entry, with an InstanceType before it (m-wire-gap-it-ir)
    _wire_refusal("CreateAutoScalingGroup", _uid("asg-gap"),
                  _wire_policy(name_spec, {1: _IT, 3: requirements}), "ValidationError", _OVR1)


def test_asg_override_numbering_errors_follow_aws(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607 (edges2 s-c-idx-*, u-u-idx-*, edges d-*-wire-idx-*, edges3 t-c-idx-*, t-u-idx-*): member 0 is not
    a valid index, and a single member N >= 12 skips N-1 list elements (the count carries thousands
    separators). Indexes 2 to 11 give the override shape error. The general rule is in
    test_asg_sparse_override_shapes_follow_aws."""
    _, mip = plain_and_mip_groups
    name_spec = capacity_type_templates["name_spec"]["LaunchTemplateName"]
    zero = "0 is not a valid index"
    create = (
        ({0: _IT}, zero),                                   # d-c-wire-idx-0
        ({0: _IT, 1: _SMALL}, zero),                        # s-c-idx-0-and-1
        ({50: _IT}, _sparse(49)),                           # d-c-wire-idx-50
        *(({number: _IT}, _sparse(number - 1)) for number in range(12, 21)),   # t-c-idx-12 ... t-c-idx-20
        ({21: _IT}, _sparse(20)),                           # s-c-idx-21
        ({33: _IT}, _sparse(32)),                           # s-c-idx-33
        ({49: _IT}, _sparse(48)),                           # s-c-idx-49
        ({51: _IT}, _sparse(50)),                           # s-c-idx-51
        ({100: _IT}, _sparse(99)),                          # s-c-idx-100
        ({1000: _IT}, _sparse(999)),                        # s-c-idx-1000
        ({100000: _IT}, _sparse("99,999")),                 # s-c-idx-100000
        ({1: _IT, 60: _SMALL}, _sparse(59)),                # s-c-idx-1-and-60
    )
    for overrides, message in create:
        _wire_refusal("CreateAutoScalingGroup", _uid("asg-idx"), _wire_policy(name_spec, overrides), _MALFORMED,
                      message)
    for number in (2, 3, 4, 5, 6, 8, 11):                   # s-c-idx-2 ... s-c-idx-11
        _wire_refusal("CreateAutoScalingGroup", _uid("asg-idx"), _wire_policy(name_spec, {number: _IT}),
                      "ValidationError", _OVR1)
    update = (
        ({0: _IT}, _MALFORMED, zero),                       # u-u-idx-0, d-u-wire-idx-0
        ({50: _IT}, _MALFORMED, _sparse(49)),               # u-u-idx-50, d-u-wire-idx-50
        ({11: _IT}, "ValidationError", _OVR1),              # t-u-idx-11
        ({12: _IT}, _MALFORMED, _sparse(11)),               # t-u-idx-12
        ({15: _IT}, _MALFORMED, _sparse(14)),               # t-u-idx-15
        ({20: _IT}, _MALFORMED, _sparse(19)),               # t-u-idx-20
        ({21: _IT}, _MALFORMED, _sparse(20)),               # t-u-idx-21
        ({2: _IT}, "ValidationError", _OVR1),               # u-u-idx-2
        ({3: _IT}, "ValidationError", _OVR1),               # u-u-idx-3
    )
    for overrides, code, message in update:
        _wire_refusal("UpdateAutoScalingGroup", mip, _wire_policy(name_spec, overrides), code, message)
    assert _stored_overrides(autoscaling, mip) == [_IT]

_TYPES = ["t3.nano", "t3.micro", "t3.small", "t3.medium", "t3.large", "t3.xlarge", "t3.2xlarge", "t3a.nano",
          "t3a.micro", "t3a.small", "t3a.medium", "t3a.large", "t3a.xlarge", "t3a.2xlarge", "m5.large", "m5.xlarge",
          "m5.2xlarge", "m5.4xlarge", "c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge", "c5.9xlarge", "m5a.large",
          "m5a.xlarge", "m5a.2xlarge", "m5a.4xlarge", "c5a.large", "c5a.xlarge", "c5a.2xlarge", "c5a.4xlarge",
          "r5.large", "r5.xlarge", "r5.2xlarge", "r5.4xlarge", "m6i.large", "m6i.xlarge", "m6i.2xlarge",
          "m6i.4xlarge", "c6i.large", "c6i.xlarge"]


def _typed(*indexes):
    """Overrides at the given member numbers, each with a different InstanceType (edges4 p-*, q-*)."""
    return {number: {"InstanceType": _TYPES[position]} for position, number in enumerate(indexes)}


def _wire_accepted(autoscaling, action, group, params):
    """Send a captured wire body that AWS accepts; a created group is deleted again."""
    base = {"AutoScalingGroupName": group, "MinSize": "0", "MaxSize": "1", "DesiredCapacity": "0"}
    if action == "CreateAutoScalingGroup":
        base["AvailabilityZones.member.1"] = "us-east-1a"
    pairs = params.pop("pairs", ()) if isinstance(params, dict) else ()
    status, body = _raw_query(action, pairs=pairs, **base, **params)
    assert (status, body) == (200, "")
    if action == "CreateAutoScalingGroup":
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=group, ForceDelete=True)


def test_asg_dense_override_lists_are_accepted(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607 (edges4 p-c-dense-10, -11, -12, -13, -20, -40, p-u-dense-12, p-u-dense-40, p-describe-after-dense): a
    list numbered 1..N without gaps is never sparse, up to the 40 overrides AWS takes. 41 overrides are refused by
    AWS (p-c-dense-41, known difference)."""
    _, mip = plain_and_mip_groups
    name = capacity_type_templates["name_spec"]["LaunchTemplateName"]
    for count in (10, 11, 12, 13, 20, 40):
        _wire_accepted(autoscaling, "CreateAutoScalingGroup", _uid("asg-dense"),
                       _wire_policy(name, _typed(*range(1, count + 1))))
    for count in (12, 40):
        _wire_accepted(autoscaling, "UpdateAutoScalingGroup", mip, _wire_policy(name, _typed(*range(1, count + 1))))
    assert _stored_overrides(autoscaling, mip) == [{"InstanceType": t} for t in _TYPES[:40]]


def test_asg_sparse_override_shapes_follow_aws(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607 (edges4 q-c-*): with the member numbers sorted, an index that skips 11 or more list elements after the
    previous one (the first being 1) is excessively sparse and the count of the first such gap is reported;
    anything less is a gap, an override with neither member. The order the members arrive in does not matter.
    Which gap AWS reports when several are too wide is not captured."""
    name = capacity_type_templates["name_spec"]["LaunchTemplateName"]
    sparse = (
        ((1, 13), 12),                       # q-c-1-13
        ((1, 2, 3, 15), 12),                 # q-c-1-2-3-15
        ((5, 17), 12),                       # q-c-5-17
        ((1, 12), 11),                       # q-c-1-12
    )
    for indexes, skipped in sparse:
        _wire_refusal("CreateAutoScalingGroup", _uid("asg-q"), _wire_policy(name, _typed(*indexes)), _MALFORMED,
                      _sparse(skipped))
    neither = (
        (1, 11),                             # q-c-1-11
        (*range(1, 12), 13),                 # q-c-1-11-13
        (*range(1, 13), 14),                 # q-c-1-12-14
        tuple(range(2, 14)),                 # q-c-2-13-no1
        (1, 5, 9, 13, 17, 21, 25),           # q-c-step4-1-25
    )
    for indexes in neither:
        _wire_refusal("CreateAutoScalingGroup", _uid("asg-q"), _wire_policy(name, _typed(*indexes)),
                      "ValidationError", _OVR1)
    order = (3, 1, 2, *range(12, 3, -1))     # q-c-order-3-1-2-then-12-4: dense, arriving unordered
    _wire_accepted(autoscaling, "CreateAutoScalingGroup", _uid("asg-q"), _wire_policy(name, _typed(*order)))


def test_asg_index_errors_come_before_the_name_constraint_and_the_source_check(autoscaling, plain_and_mip_groups,
                                                                                capacity_type_templates):
    """#2607 (edges4 r-c-long-name-idx0, -idx60, -control, r-u-long-name-*, r-u-mipg-idx0, -idx60, s-u-lt-mip-idx0,
    -idx60, s-u-lt-lc-idx0): the index errors are answered before the group-name length constraint and, on
    update, before the source error and a missing group."""
    plain, mip = plain_and_mip_groups
    name = capacity_type_templates["name_spec"]["LaunchTemplateName"]
    long_name = "a" * 256
    zero = "0 is not a valid index"
    length = (f"1 validation error detected: Value '{long_name}' at 'autoScalingGroupName' failed to satisfy "
              "constraint: Member must have length less than or equal to 255")
    for action in ("CreateAutoScalingGroup", "UpdateAutoScalingGroup"):
        for index, message in ((0, zero), (60, _sparse(59))):
            _wire_refusal(action, long_name, _wire_policy(name, {index: _IT}), _MALFORMED, message)
        _wire_refusal(action, long_name, _wire_policy(name, {1: _IT}), "ValidationError", length)    # -control
    template = {"LaunchTemplate.LaunchTemplateName": name, "LaunchTemplate.Version": "$Latest"}
    for index, message in ((0, zero), (60, _sparse(59))):
        _wire_refusal("UpdateAutoScalingGroup", mip, {**_wire_policy(name, {index: _IT}), **template},
                      _MALFORMED, message)                                         # s-u-lt-mip-idx0, -idx60
        _wire_refusal("UpdateAutoScalingGroup", mip, _wire_policy(name, {index: _IT}), _MALFORMED, message)   # r-u-mipg-*
    _wire_refusal("UpdateAutoScalingGroup", plain, {
        **_wire_policy(name, {0: _IT}), **template, "LaunchConfigurationName": "no-such-lc"}, _MALFORMED, zero)
    assert _stored_overrides(autoscaling, mip) == [_IT]


def test_asg_repeated_override_key_and_blank_weight_do_not_crash(autoscaling, plain_and_mip_groups,
                                                                 capacity_type_templates):
    """#2607 (edges4 t-c-repeat-key, t-u-repeat-key: AWS answers 200; t-u-blank-wc-no-it, t-u-blank-wc-with-it and
    edges3 b-c-blank-wc-*: AWS answers a field-length error that MiniStack does not reproduce, known difference).
    Neither request may end in a server error."""
    _, mip = plain_and_mip_groups
    name = capacity_type_templates["name_spec"]["LaunchTemplateName"]
    key = _WIRE_PREFIX + "Overrides.member.1.InstanceType"
    pairs = ((key, "t3.micro"), (key, "t3.small"))
    _wire_accepted(autoscaling, "CreateAutoScalingGroup", _uid("asg-repeat"), {**_wire_policy(name), "pairs": pairs})
    _wire_accepted(autoscaling, "UpdateAutoScalingGroup", mip, {**_wire_policy(name), "pairs": pairs})
    weight = _WIRE_PREFIX + "Overrides.member.1.WeightedCapacity"
    for instance_type in (None, "t3.micro"):             # t-u-blank-wc-no-it, t-u-blank-wc-with-it
        params = {**_wire_policy(name), weight: "", **({key: instance_type} if instance_type else {})}
        status, _ = _raw_query("UpdateAutoScalingGroup", AutoScalingGroupName=mip, MinSize="0", MaxSize="1",
                               DesiredCapacity="0", **params)
        assert status < 500        # AWS refuses these; MiniStack's answer is not pinned, only that it does not crash


def test_asg_non_numeric_requirement_value_is_malformed_input(autoscaling, plain_and_mip_groups,
                                                              capacity_type_templates):
    """#2607 (edges4 v-c-memory-max-abc, v-u-memory-max-abc, v-u-vcpu-min-abc): AWS answers MalformedInput with an
    empty message, where MiniStack ended in a server error."""
    _, mip = plain_and_mip_groups
    name = capacity_type_templates["name_spec"]["LaunchTemplateName"]
    requirements = {"InstanceRequirements.VCpuCount.Min": "1", "InstanceRequirements.MemoryMiB.Min": "512"}
    for action, group, member, value in (("CreateAutoScalingGroup", _uid("asg-abc"), "MemoryMiB.Max", "abc"),
                                         ("UpdateAutoScalingGroup", mip, "MemoryMiB.Max", "abc"),
                                         ("UpdateAutoScalingGroup", mip, "VCpuCount.Min", "abc")):
        _wire_refusal(action, group, _wire_policy(name, {1: {**requirements, f"InstanceRequirements.{member}": value}}),
                      _MALFORMED, "")
    assert _stored_overrides(autoscaling, mip) == [_IT]


def test_asg_source_check_with_launch_configuration(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607: a LaunchConfigurationName next to a LaunchTemplate or MixedInstancesPolicy is a source error on
    create (p-ord-lc-too, o-ord-lc-too, m-ord-lc-too, c-both-lc-too) and, with a LaunchTemplate, on update
    (v-lt-ir-name-and-lc, which words it without InstanceId). A launch configuration cannot be created in the
    capture account, so only these invalid-body shapes are asserted."""
    t, (plain, mip) = capacity_type_templates, plain_and_mip_groups
    spec, ir_spec = t["name_spec"], t["ir_name_spec"]
    ir, it = {"InstanceRequirements": _IR}, {"InstanceType": "t3.micro"}
    missing = "msc-2607-no-such-lc"
    for kwargs in (
            {"LaunchTemplate": ir_spec},                                                      # p-ord-lc-too
            {"MixedInstancesPolicy": _policy(spec, [{"LaunchTemplateSpecification": ir_spec}])},  # o-ord-lc-too
            {"MixedInstancesPolicy": _policy(spec, [ir, it])},                                # m-ord-lc-too
            {"MixedInstancesPolicy": _policy(spec, [dict(it, **ir)])}):                       # c-both-lc-too
        _refused(autoscaling, "ValidationError", _CREATE_SOURCE, LaunchConfigurationName=missing, **kwargs)
    with pytest.raises(ClientError) as exc:                                                   # v-lt-ir-name-and-lc
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=plain, LaunchTemplate=ir_spec,
                                              LaunchConfigurationName="msc-2607-edges2-lc-never-created")
    assert _validation_message(exc) == _SOURCE
    group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[plain])["AutoScalingGroups"][0]
    assert group["LaunchTemplate"]["LaunchTemplateName"] == spec["LaunchTemplateName"]


def test_asg_duplicate_instance_types_follow_aws(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607 (edges2 d-c-*, d-u-*): the duplicated InstanceTypes are named once each, in the order their repeats are detected (edges3 test below), on
    create and on update; on update before the group lookup. On create they come after the DesiredCapacityType
    value and the source error and before the base and override template lookups."""
    t, (_, mip) = capacity_type_templates, plain_and_mip_groups
    spec = t["name_spec"]
    micro, small = {"InstanceType": "t3.micro"}, {"InstanceType": "t3.small"}
    ir = {"InstanceRequirements": _IR}
    never = {"LaunchTemplateName": _uid("lt-never-created"), "Version": "$Latest"}
    for overrides, message in (([micro, micro, small, small], _dup("t3.micro, t3.small")),   # d-c-aabb
                               ([micro, micro, micro], _dup()),                              # d-c-aaa
                               ([micro, small, micro], _dup())):                             # d-c-aba
        _refused(autoscaling, "ValidationError", message, MixedInstancesPolicy=_policy(spec, overrides))
    _refused(autoscaling, "ValidationError", _enum_message("bogus"), DesiredCapacityType="bogus",
             MixedInstancesPolicy=_policy(spec, [micro, micro]))                              # d-c-aa-bogus-dct
    _refused(autoscaling, "ValidationError", _CREATE_SOURCE, LaunchTemplate=spec,
             MixedInstancesPolicy=_policy(spec, [micro, micro]))                              # d-c-aa-lt-and-mip
    _refused(autoscaling, "ValidationError", _DUP,
             MixedInstancesPolicy=_policy(never, [micro, micro]))                             # d-c-aa-missing-base
    _refused(autoscaling, "ValidationError", _DUP, MixedInstancesPolicy=_policy(
        spec, [micro, micro, {"LaunchTemplateSpecification": never}]))                        # d-c-aa-missing-override
    for group, overrides in ((mip, [micro, micro]),                                           # d-u-aa-mip
                             (_uid("asg-nx-never-created"), [micro, micro]),                  # d-u-aa-nx
                             (mip, [micro, micro, ir])):                                      # d-u-aa-ir-mip
        with pytest.raises(ClientError) as exc:
            autoscaling.update_auto_scaling_group(AutoScalingGroupName=group,
                                                  MixedInstancesPolicy=_policy(spec, overrides))
        assert _validation_message(exc) == _DUP
    assert _stored_overrides(autoscaling, mip) == [micro]


def test_asg_refused_update_leaves_the_policy_group_unchanged(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607 (edges2 m-refused-update-lto, m-describe-before, m-describe-after)."""
    spec, (_, mip) = capacity_type_templates["name_spec"], plain_and_mip_groups

    def describe():
        return autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[mip])["AutoScalingGroups"][0]

    before = describe()["MixedInstancesPolicy"]
    with pytest.raises(ClientError) as exc:
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=mip,
            MixedInstancesPolicy=_policy(spec, [{"LaunchTemplateSpecification": spec}]))
    assert _validation_message(exc) == _OVR1
    assert describe()["MixedInstancesPolicy"] == before


def test_asg_weighted_and_instance_requirements_policies_describe(autoscaling, capacity_type_templates):
    """#2607 (edges2 w-c-weighted, w-describe-weighted, w-c-ir-units, w-c-ir-units-mip, w-describe-ir-units-mip):
    weights read back as strings; an InstanceRequirements template is refused as a plain launch template with
    units and accepted as the base of a policy without overrides."""
    t = capacity_type_templates
    weighted = [{"InstanceType": "t3.micro", "WeightedCapacity": "2"},
                {"InstanceType": "t3.small", "WeightedCapacity": "3"}]
    weighted_name, units_name = _uid("asg-weighted"), _uid("asg-ir-units")
    autoscaling.create_auto_scaling_group(AutoScalingGroupName=weighted_name, **_DCT_BASE,
                                          MixedInstancesPolicy=_policy(t["name_spec"], weighted))
    _refused(autoscaling, "InvalidQueryParameter", _LTNC, LaunchTemplate=t["ir_name_spec"],
             DesiredCapacityType="units")                                                     # w-c-ir-units
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=units_name, **_DCT_BASE, DesiredCapacityType="units",
        MixedInstancesPolicy={"LaunchTemplate": {"LaunchTemplateSpecification": t["ir_name_spec"]}})
    try:
        assert _stored_overrides(autoscaling, weighted_name) == weighted
        group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[units_name])["AutoScalingGroups"][0]
        assert group["DesiredCapacityType"] == "units"
        assert group["MixedInstancesPolicy"]["LaunchTemplate"]["Overrides"] == []
    finally:
        for name in (weighted_name, units_name):
            autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)


def test_asg_launch_template_id_with_policy_is_a_source_error(autoscaling, capacity_type_templates):
    """#2607 (edges2 l-lt-id-and-mip): a LaunchTemplate by id next to a MixedInstancesPolicy."""
    t = capacity_type_templates
    _refused(autoscaling, "ValidationError", _CREATE_SOURCE, LaunchTemplate=t["spec"],
             MixedInstancesPolicy=_policy(t["name_spec"], [{"InstanceType": "t3.micro"}]))


def test_asg_stored_group_is_not_rechecked_for_launch_template_drift(autoscaling, ec2):
    """#2607 (update u-drift-*): LTNC looks only at the template sent; a stored $Latest that became
    InstanceRequirements stays."""
    lt = ec2.create_launch_template(LaunchTemplateName=_uid("lt-drift"), LaunchTemplateData={
        "InstanceType": "t3.micro", "ImageId": "ami-12345678"})["LaunchTemplate"]
    name = _uid("asg-drift")
    latest = {"LaunchTemplateName": lt["LaunchTemplateName"], "Version": "$Latest"}
    try:
        autoscaling.create_auto_scaling_group(AutoScalingGroupName=name, **_DCT_BASE, LaunchTemplate=latest)
        ec2.create_launch_template_version(LaunchTemplateId=lt["LaunchTemplateId"], LaunchTemplateData={
            "InstanceRequirements": _IR, "ImageId": "ami-12345678"})
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=name, MaxSize=2)
        autoscaling.set_desired_capacity(AutoScalingGroupName=name, DesiredCapacity=0)
        assert autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])["AutoScalingGroups"][0][
            "MaxSize"] == 2
        with pytest.raises(ClientError) as exc:
            autoscaling.update_auto_scaling_group(AutoScalingGroupName=name, LaunchTemplate=latest)
        _assert_refusal(exc, "InvalidQueryParameter", _LTNC)
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName=name,
            LaunchTemplate={"LaunchTemplateName": lt["LaunchTemplateName"], "Version": "1"})
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
        ec2.delete_launch_template(LaunchTemplateId=lt["LaunchTemplateId"])


def test_asg_template_lookup_stays_in_the_request_region(autoscaling, capacity_type_templates):
    """#2607 (cross-region x-plain-*, x-mip-base-*): a template of another region does not exist here."""
    t = capacity_type_templates
    name, template_id = t["name_spec"]["LaunchTemplateName"], t["spec"]["LaunchTemplateId"]
    other = boto3.client("autoscaling", endpoint_url=autoscaling.meta.endpoint_url, region_name="eu-west-1",
                         aws_access_key_id="test", aws_secret_access_key="test")
    by_name = f"The specified launch template, with template name {name}, does not exist."
    by_id = f"The specified launch template, with template ID {template_id}, does not exist."
    for kwargs, message in (
            ({"LaunchTemplate": t["name_spec"]}, by_name),                                       # x-plain-name-other-region
            ({"LaunchTemplate": t["spec"]}, by_id),                                              # x-plain-id-other-region
            ({"LaunchTemplate": t["spec"], "DesiredCapacityType": "vcpu"}, by_id),               # x-plain-id-other-region-vcpu
            ({"MixedInstancesPolicy": _policy(t["name_spec"], [{"InstanceType": "t3.micro"}])}, by_name),
            ({"MixedInstancesPolicy": _policy(t["spec"], [{"InstanceType": "t3.micro"}])}, by_id)):
        with pytest.raises(ClientError) as exc:
            other.create_auto_scaling_group(AutoScalingGroupName=_uid("asg-region"), MinSize=0, MaxSize=1,
                                            AvailabilityZones=["eu-west-1a"], **kwargs)
        _assert_refusal(exc, "ValidationError", message)


def _update_refused(autoscaling, group, code, message, **kwargs):
    with pytest.raises(ClientError) as exc:
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=group, **kwargs)
    _assert_refusal(exc, code, message)


def test_asg_index_errors_come_before_every_other_check(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607 (edges3 o-c-idx0-bogus-dct, o-c-idx60-bogus-dct, o-c-idx0-lt-and-mip, o-c-idx60-lt-and-mip,
    o-c-idx0-missing-base, o-c-idx60-missing-base, o-c-idx0-dup-pair, o-c-idx0-and-60, o-u-idx0-bogus-dct,
    o-u-idx60-bogus-dct, -nx variants): an index error wins over the DesiredCapacityType value, the source
    error, a missing base template and duplicates, on create and update, existing group or not."""
    _, mip = plain_and_mip_groups
    name = capacity_type_templates["name_spec"]["LaunchTemplateName"]
    never = _uid("lt-never-created")
    zero, sparse60 = "0 is not a valid index", _sparse(59)
    for index, message in ((0, zero), (60, sparse60)):
        _wire_refusal("CreateAutoScalingGroup", _uid("asg-idx"), {
            **_wire_policy(name, {index: _IT}), "DesiredCapacityType": "bogus"}, _MALFORMED, message)
        _wire_refusal("CreateAutoScalingGroup", _uid("asg-idx"), {
            **_wire_policy(name, {index: _IT}), "LaunchTemplate.LaunchTemplateName": name,
            "LaunchTemplate.Version": "$Latest"}, _MALFORMED, message)
        _wire_refusal("CreateAutoScalingGroup", _uid("asg-idx"), _wire_policy(never, {index: _IT}),
                      _MALFORMED, message)
        for group in (mip, _uid("asg-nx-never-created")):
            _wire_refusal("UpdateAutoScalingGroup", group, {
                **_wire_policy(name, {index: _IT}), "DesiredCapacityType": "bogus"}, _MALFORMED, message)
    _wire_refusal("CreateAutoScalingGroup", _uid("asg-idx"),
                  _wire_policy(name, {0: _IT, 1: _SMALL, 2: _SMALL}), _MALFORMED, zero)       # o-c-idx0-dup-pair
    _wire_refusal("CreateAutoScalingGroup", _uid("asg-idx"),
                  _wire_policy(name, {0: _IT, 60: _SMALL}), _MALFORMED, zero)                 # o-c-idx0-and-60
    assert _stored_overrides(autoscaling, mip) == [_IT]


def test_asg_duplicate_types_are_named_in_the_order_detected(autoscaling, plain_and_mip_groups,
                                                               capacity_type_templates):
    """#2607 (edges3 d-c-abba, d-c-baab, d-c-abccb, d-u-aa-bogus-dct, d-u-aa-bogus-dct-nx, d-u-aa-lt-and-mip,
    d-u-aa-wonly, d-u-aw1-aw2-ir): each duplicated type is listed when its repeat is first seen; on update the
    duplicates come after the source error and before the DesiredCapacityType value, the group lookup and
    weight or InstanceRequirements entries."""
    t, (_, mip) = capacity_type_templates, plain_and_mip_groups
    spec = t["name_spec"]
    micro, small, large = _IT, _SMALL, {"InstanceType": "t3.large"}
    for overrides, message in (([micro, small, small, micro], _dup("t3.small, t3.micro")),           # d-c-abba
                               ([small, micro, micro, small], _dup("t3.micro, t3.small")),           # d-c-baab
                               ([micro, small, large, large, small], _dup("t3.large, t3.small"))):   # d-c-abccb
        _refused(autoscaling, "ValidationError", message, MixedInstancesPolicy=_policy(spec, overrides))
    for group in (mip, _uid("asg-nx-never-created")):                                  # d-u-aa-bogus-dct(-nx)
        _update_refused(autoscaling, group, "ValidationError", _DUP, DesiredCapacityType="bogus",
                        MixedInstancesPolicy=_policy(spec, [micro, micro]))
    _update_refused(autoscaling, mip, "ValidationError", _SOURCE, LaunchTemplate=spec,
                    MixedInstancesPolicy=_policy(spec, [micro, micro]))                # d-u-aa-lt-and-mip
    _update_refused(autoscaling, mip, "ValidationError", _DUP,
                    MixedInstancesPolicy=_policy(spec, [micro, micro, {"WeightedCapacity": "2"}]))  # d-u-aa-wonly
    _update_refused(autoscaling, mip, "ValidationError", _DUP, MixedInstancesPolicy=_policy(spec, [
        {**micro, "WeightedCapacity": "1"}, {**micro, "WeightedCapacity": "2"},
        {"InstanceRequirements": _IR}]))                                               # d-u-aw1-aw2-ir
    assert _stored_overrides(autoscaling, mip) == [micro]


def test_asg_three_entry_override_lists_follow_aws(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607 (edges3 m-c-*, m-u-*): with three entries the first wrong entry in index order decides, a
    duplicate type comes first, and InstanceType entries next to InstanceRequirements ones are a mix."""
    t, (_, mip) = capacity_type_templates, plain_and_mip_groups
    spec = t["name_spec"]
    ir, lts, both = {"InstanceRequirements": _IR}, {"LaunchTemplateSpecification": spec}, {**_IT, "InstanceRequirements": _IR}
    for overrides, message in (([_IT, ir, lts], _OVR1),            # m-c-it-ir-lts, m-u-it-ir-lts
                               ([lts, _IT, ir], _OVR1),            # m-c-lts-it-ir
                               ([ir, lts, _IT], _OVR1),            # m-c-ir-lts-it
                               ([_IT, ir, both], _DUP),            # m-c-it-ir-both
                               ([both, _IT, ir], _DUP),            # m-c-both-it-ir, m-u-both-it-ir
                               ([_IT, _IT, ir], _DUP),             # m-c-it-it-ir
                               ([ir, ir, _IT], _MIX)):             # m-c-ir-ir-it
        _refused(autoscaling, "ValidationError", message, MixedInstancesPolicy=_policy(spec, overrides))
    for overrides, message in (([_IT, ir, lts], _OVR1), ([both, _IT, ir], _DUP)):
        _update_refused(autoscaling, mip, "ValidationError", message, MixedInstancesPolicy=_policy(spec, overrides))
    assert _stored_overrides(autoscaling, mip) == [_IT]


def test_asg_update_source_error_for_any_two_sources(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607 (edges3 u-mip-and-lc, u-lt-it-and-lc, u-all-three, u-lt-ir-and-lc-vcpu)."""
    t, (plain, mip) = capacity_type_templates, plain_and_mip_groups
    spec, policy, lc = t["name_spec"], _policy(t["name_spec"], [_IT]), "msc-2607-no-such-lc"
    # u-lt-it-and-lc and u-lt-ir-and-lc-vcpu ran on the plain group, u-mip-and-lc and u-all-three on the policy group
    for group, kwargs in (
            (mip, {"MixedInstancesPolicy": policy, "LaunchConfigurationName": lc}),
            (plain, {"LaunchTemplate": spec, "LaunchConfigurationName": lc}),
            (mip, {"LaunchTemplate": spec, "MixedInstancesPolicy": policy, "LaunchConfigurationName": lc}),
            (plain, {"LaunchTemplate": t["ir_name_spec"], "LaunchConfigurationName": lc,
                     "DesiredCapacityType": "vcpu"})):
        _update_refused(autoscaling, group, "ValidationError", _SOURCE, **kwargs)
    assert _stored_overrides(autoscaling, mip) == [_IT]


def test_asg_update_empty_overrides_and_blank_member(autoscaling, plain_and_mip_groups, capacity_type_templates):
    """#2607 (edges3 e-u-overrides-empty-list, e-u-overrides-empty-struct, e-u-wire-overrides-blank accepted;
    e-u-wire-member1-blank refused as an entry with neither member)."""
    spec, (_, mip) = capacity_type_templates["name_spec"], plain_and_mip_groups
    def unchanged():       # e-describe-after-empty-list, -empty-struct, -wire: the stored override is untouched
        group = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[mip])["AutoScalingGroups"][0]
        assert (_stored_overrides(autoscaling, mip), group["MinSize"], group["MaxSize"]) == ([_IT], 0, 1)

    for overrides in ([], [{}]):
        autoscaling.update_auto_scaling_group(AutoScalingGroupName=mip,
                                              MixedInstancesPolicy=_policy(spec, overrides))
        unchanged()
    name = spec["LaunchTemplateName"]
    status, _ = _raw_query("UpdateAutoScalingGroup", AutoScalingGroupName=mip,
                           **{**_wire_policy(name), _WIRE_PREFIX + "Overrides": ""})
    assert status == 200
    _wire_refusal("UpdateAutoScalingGroup", mip, {**_wire_policy(name), _WIRE_PREFIX + "Overrides.member.1": ""},
                  "ValidationError", _OVR1)
    unchanged()


def test_asg_update_override_shape_comes_before_size_checks(autoscaling, plain_and_mip_groups,
                                                              capacity_type_templates):
    """#2607 (edges3 c-u-min-gt-max-lts, c-u-min-neg-lts, c-u-name256-bad-ov)."""
    spec, (_, mip) = capacity_type_templates["name_spec"], plain_and_mip_groups
    bad = _policy(spec, [{"LaunchTemplateSpecification": spec}])
    for group in (mip, _uid("asg-nx-never-created")):
        for sizes in ({"MinSize": 5, "MaxSize": 1}, {"MinSize": -1}):
            _update_refused(autoscaling, group, "ValidationError", _OVR1, MixedInstancesPolicy=bad, **sizes)
    long_name = "a" * 256
    _update_refused(autoscaling, long_name, "ValidationError",
                    f"1 validation error detected: Value '{long_name}' at 'autoScalingGroupName' failed to "
                    "satisfy constraint: Member must have length less than or equal to 255",
                    MixedInstancesPolicy=bad)


def test_asg_policy_base_template_without_instance_type_is_accepted(autoscaling, ec2):
    """#2607 (edges3 n-c-mip-image-only-base): a template with an image and no instance type is accepted as
    the base of a policy with InstanceType overrides. AWS refuses it for a plain group (n-c-plain-image-only,
    n-u-plain-to-image-only); MiniStack deliberately accepts it there too (known difference)."""
    lt = ec2.create_launch_template(LaunchTemplateName=_uid("lt-image-only"), LaunchTemplateData={
        "ImageId": "ami-12345678"})["LaunchTemplate"]
    name = _uid("asg-image-only")
    try:
        autoscaling.create_auto_scaling_group(
            AutoScalingGroupName=name, **_DCT_BASE, MixedInstancesPolicy=_policy(
                {"LaunchTemplateName": lt["LaunchTemplateName"], "Version": "$Latest"}, [_IT, _SMALL]))
        assert _stored_overrides(autoscaling, name) == [_IT, _SMALL]
    finally:
        autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
        ec2.delete_launch_template(LaunchTemplateId=lt["LaunchTemplateId"])
