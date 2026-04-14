from botocore.exceptions import ClientError


def test_autoscaling_describe_scaling_activities_empty(autoscaling):
    """DescribeScalingActivities returns an empty list when no ASG exists."""
    resp = autoscaling.describe_scaling_activities()
    assert resp["Activities"] == []


def test_autoscaling_create_and_describe_asg(autoscaling):
    name = "test-asg-basic"
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=0,
        MaxSize=1,
        DesiredCapacity=0,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    resp = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
    groups = resp["AutoScalingGroups"]
    assert len(groups) == 1
    assert groups[0]["AutoScalingGroupName"] == name
    assert groups[0]["MinSize"] == 0
    assert groups[0]["MaxSize"] == 1

    autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)


def test_autoscaling_describe_scaling_activities_after_create(autoscaling):
    """Terraform polls DescribeScalingActivities right after ASG creation."""
    name = "test-asg-activities"
    autoscaling.create_auto_scaling_group(
        AutoScalingGroupName=name,
        MinSize=0,
        MaxSize=1,
        DesiredCapacity=0,
        AvailabilityZones=["us-east-1a"],
        LaunchConfigurationName="dummy-lc",
    )
    resp = autoscaling.describe_scaling_activities(AutoScalingGroupName=name)
    assert resp["Activities"] == []

    autoscaling.delete_auto_scaling_group(AutoScalingGroupName=name)
