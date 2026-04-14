import json
import time
import pytest
import boto3
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


@pytest.fixture(scope="module")
def scheduler():
    return boto3.client("scheduler", endpoint_url=ENDPOINT,
                        aws_access_key_id="test", aws_secret_access_key="test",
                        region_name=REGION)


@pytest.fixture(scope="module")
def cfn():
    return boto3.client("cloudformation", endpoint_url=ENDPOINT,
                        aws_access_key_id="test", aws_secret_access_key="test",
                        region_name=REGION)


def _uid():
    import uuid
    return uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# Schedule Groups
# ---------------------------------------------------------------------------

def test_scheduler_default_group_exists(scheduler):
    """The 'default' group should always exist."""
    resp = scheduler.get_schedule_group(Name="default")
    assert resp["Name"] == "default"
    assert resp["State"] == "ACTIVE"
    assert "Arn" in resp
    assert "schedule-group/default" in resp["Arn"]


def test_scheduler_create_get_delete_group(scheduler):
    name = f"test-group-{_uid()}"
    resp = scheduler.create_schedule_group(Name=name)
    arn = resp["ScheduleGroupArn"]
    assert f"schedule-group/{name}" in arn

    # Get
    resp = scheduler.get_schedule_group(Name=name)
    assert resp["Name"] == name
    assert resp["State"] == "ACTIVE"
    assert resp["Arn"] == arn

    # Delete
    scheduler.delete_schedule_group(Name=name)
    with pytest.raises(ClientError) as exc:
        scheduler.get_schedule_group(Name=name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_scheduler_create_duplicate_group(scheduler):
    name = f"dup-group-{_uid()}"
    scheduler.create_schedule_group(Name=name)
    with pytest.raises(ClientError) as exc:
        scheduler.create_schedule_group(Name=name)
    assert exc.value.response["Error"]["Code"] == "ConflictException"
    scheduler.delete_schedule_group(Name=name)


def test_scheduler_cannot_delete_default_group(scheduler):
    with pytest.raises(ClientError) as exc:
        scheduler.delete_schedule_group(Name="default")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_scheduler_list_groups(scheduler):
    name = f"list-group-{_uid()}"
    scheduler.create_schedule_group(Name=name)
    resp = scheduler.list_schedule_groups()
    names = [g["Name"] for g in resp["ScheduleGroups"]]
    assert "default" in names
    assert name in names
    scheduler.delete_schedule_group(Name=name)


def test_scheduler_list_groups_name_prefix(scheduler):
    prefix = f"pfx-{_uid()}"
    scheduler.create_schedule_group(Name=f"{prefix}-a")
    scheduler.create_schedule_group(Name=f"{prefix}-b")
    resp = scheduler.list_schedule_groups(NamePrefix=prefix)
    names = [g["Name"] for g in resp["ScheduleGroups"]]
    assert len(names) == 2
    assert all(n.startswith(prefix) for n in names)
    scheduler.delete_schedule_group(Name=f"{prefix}-a")
    scheduler.delete_schedule_group(Name=f"{prefix}-b")


# ---------------------------------------------------------------------------
# Schedules — CRUD
# ---------------------------------------------------------------------------

def test_scheduler_create_get_delete_schedule(scheduler):
    name = f"test-sched-{_uid()}"
    resp = scheduler.create_schedule(
        Name=name,
        ScheduleExpression="rate(1 hour)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
            "RoleArn": "arn:aws:iam::000000000000:role/test",
        },
    )
    arn = resp["ScheduleArn"]
    assert f"schedule/default/{name}" in arn

    # Get
    resp = scheduler.get_schedule(Name=name)
    assert resp["Name"] == name
    assert resp["GroupName"] == "default"
    assert resp["ScheduleExpression"] == "rate(1 hour)"
    assert resp["State"] == "ENABLED"
    assert resp["FlexibleTimeWindow"]["Mode"] == "OFF"
    assert resp["Target"]["Arn"] == "arn:aws:lambda:us-east-1:000000000000:function:noop"
    assert resp["Target"]["RoleArn"] == "arn:aws:iam::000000000000:role/test"
    assert "CreationDate" in resp
    assert "LastModificationDate" in resp

    # Delete
    scheduler.delete_schedule(Name=name)
    with pytest.raises(ClientError) as exc:
        scheduler.get_schedule(Name=name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_scheduler_create_schedule_in_custom_group(scheduler):
    group = f"custom-group-{_uid()}"
    name = f"sched-{_uid()}"
    scheduler.create_schedule_group(Name=group)

    resp = scheduler.create_schedule(
        Name=name,
        GroupName=group,
        ScheduleExpression="cron(0 12 * * ? *)",
        FlexibleTimeWindow={"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15},
        Target={
            "Arn": "arn:aws:sqs:us-east-1:000000000000:my-queue",
            "RoleArn": "arn:aws:iam::000000000000:role/test",
            "Input": '{"key":"value"}',
        },
    )
    assert f"schedule/{group}/{name}" in resp["ScheduleArn"]

    resp = scheduler.get_schedule(Name=name, GroupName=group)
    assert resp["GroupName"] == group
    assert resp["ScheduleExpression"] == "cron(0 12 * * ? *)"
    assert resp["FlexibleTimeWindow"]["Mode"] == "FLEXIBLE"
    assert resp["FlexibleTimeWindow"]["MaximumWindowInMinutes"] == 15
    assert resp["Target"]["Input"] == '{"key":"value"}'

    scheduler.delete_schedule(Name=name, GroupName=group)
    scheduler.delete_schedule_group(Name=group)


def test_scheduler_create_duplicate_schedule(scheduler):
    name = f"dup-sched-{_uid()}"
    scheduler.create_schedule(
        Name=name,
        ScheduleExpression="rate(5 minutes)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                "RoleArn": "arn:aws:iam::000000000000:role/test"},
    )
    with pytest.raises(ClientError) as exc:
        scheduler.create_schedule(
            Name=name,
            ScheduleExpression="rate(10 minutes)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                    "RoleArn": "arn:aws:iam::000000000000:role/test"},
        )
    assert exc.value.response["Error"]["Code"] == "ConflictException"
    scheduler.delete_schedule(Name=name)


def test_scheduler_delete_nonexistent_schedule(scheduler):
    with pytest.raises(ClientError) as exc:
        scheduler.delete_schedule(Name="nonexistent-schedule-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_scheduler_get_nonexistent_schedule(scheduler):
    with pytest.raises(ClientError) as exc:
        scheduler.get_schedule(Name="nonexistent-schedule-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# UpdateSchedule
# ---------------------------------------------------------------------------

def test_scheduler_update_schedule(scheduler):
    name = f"upd-sched-{_uid()}"
    scheduler.create_schedule(
        Name=name,
        ScheduleExpression="rate(1 hour)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:old",
                "RoleArn": "arn:aws:iam::000000000000:role/test"},
        State="ENABLED",
    )

    resp = scheduler.update_schedule(
        Name=name,
        ScheduleExpression="rate(30 minutes)",
        FlexibleTimeWindow={"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 5},
        Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:new",
                "RoleArn": "arn:aws:iam::000000000000:role/test"},
        State="DISABLED",
        Description="Updated schedule",
    )
    assert "ScheduleArn" in resp

    resp = scheduler.get_schedule(Name=name)
    assert resp["ScheduleExpression"] == "rate(30 minutes)"
    assert resp["State"] == "DISABLED"
    assert resp["Description"] == "Updated schedule"
    assert resp["Target"]["Arn"] == "arn:aws:lambda:us-east-1:000000000000:function:new"

    scheduler.delete_schedule(Name=name)


def test_scheduler_update_nonexistent_schedule(scheduler):
    with pytest.raises(ClientError) as exc:
        scheduler.update_schedule(
            Name="nonexistent-xyz",
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                    "RoleArn": "arn:aws:iam::000000000000:role/test"},
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# ListSchedules
# ---------------------------------------------------------------------------

def test_scheduler_list_schedules(scheduler):
    prefix = f"ls-{_uid()}"
    for i in range(3):
        scheduler.create_schedule(
            Name=f"{prefix}-{i}",
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                    "RoleArn": "arn:aws:iam::000000000000:role/test"},
        )
    resp = scheduler.list_schedules(NamePrefix=prefix)
    names = [s["Name"] for s in resp["Schedules"]]
    assert len(names) == 3
    # Each item should have abbreviated Target with just Arn
    for s in resp["Schedules"]:
        assert "Arn" in s["Target"]
        assert "CreationDate" in s

    for i in range(3):
        scheduler.delete_schedule(Name=f"{prefix}-{i}")


def test_scheduler_list_schedules_filter_by_group(scheduler):
    group = f"filter-group-{_uid()}"
    scheduler.create_schedule_group(Name=group)
    scheduler.create_schedule(
        Name=f"in-group-{_uid()}", GroupName=group,
        ScheduleExpression="rate(1 hour)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                "RoleArn": "arn:aws:iam::000000000000:role/test"},
    )
    scheduler.create_schedule(
        Name=f"in-default-{_uid()}",
        ScheduleExpression="rate(1 hour)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                "RoleArn": "arn:aws:iam::000000000000:role/test"},
    )

    resp = scheduler.list_schedules(GroupName=group)
    assert all(s["GroupName"] == group for s in resp["Schedules"])
    assert len(resp["Schedules"]) == 1

    # Cleanup
    scheduler.delete_schedule_group(Name=group)


def test_scheduler_list_schedules_filter_by_state(scheduler):
    name_e = f"enabled-{_uid()}"
    name_d = f"disabled-{_uid()}"
    for n, s in [(name_e, "ENABLED"), (name_d, "DISABLED")]:
        scheduler.create_schedule(
            Name=n, State=s,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                    "RoleArn": "arn:aws:iam::000000000000:role/test"},
        )
    resp = scheduler.list_schedules(State="DISABLED", NamePrefix="disabled-")
    assert all(s["State"] == "DISABLED" for s in resp["Schedules"])
    scheduler.delete_schedule(Name=name_e)
    scheduler.delete_schedule(Name=name_d)


# ---------------------------------------------------------------------------
# Schedule with at() expression
# ---------------------------------------------------------------------------

def test_scheduler_at_expression(scheduler):
    name = f"at-sched-{_uid()}"
    scheduler.create_schedule(
        Name=name,
        ScheduleExpression="at(2030-01-01T00:00:00)",
        ScheduleExpressionTimezone="America/New_York",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                "RoleArn": "arn:aws:iam::000000000000:role/test"},
        ActionAfterCompletion="DELETE",
    )
    resp = scheduler.get_schedule(Name=name)
    assert resp["ScheduleExpression"] == "at(2030-01-01T00:00:00)"
    assert resp["ScheduleExpressionTimezone"] == "America/New_York"
    assert resp["ActionAfterCompletion"] == "DELETE"
    scheduler.delete_schedule(Name=name)


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def test_scheduler_tag_schedule(scheduler):
    name = f"tag-sched-{_uid()}"
    resp = scheduler.create_schedule(
        Name=name,
        ScheduleExpression="rate(1 hour)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                "RoleArn": "arn:aws:iam::000000000000:role/test"},
    )
    arn = resp["ScheduleArn"]

    scheduler.tag_resource(ResourceArn=arn, Tags=[
        {"Key": "env", "Value": "test"},
        {"Key": "team", "Value": "platform"},
    ])

    resp = scheduler.list_tags_for_resource(ResourceArn=arn)
    tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tags == {"env": "test", "team": "platform"}

    scheduler.untag_resource(ResourceArn=arn, TagKeys=["team"])
    resp = scheduler.list_tags_for_resource(ResourceArn=arn)
    tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tags == {"env": "test"}

    scheduler.delete_schedule(Name=name)


def test_scheduler_tag_group(scheduler):
    name = f"tag-group-{_uid()}"
    scheduler.create_schedule_group(Name=name, Tags=[
        {"Key": "env", "Value": "prod"},
    ])
    arn = scheduler.get_schedule_group(Name=name)["Arn"]
    resp = scheduler.list_tags_for_resource(ResourceArn=arn)
    tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tags == {"env": "prod"}
    scheduler.delete_schedule_group(Name=name)


# ---------------------------------------------------------------------------
# Delete group cascades to schedules
# ---------------------------------------------------------------------------

def test_scheduler_delete_group_deletes_schedules(scheduler):
    group = f"cascade-group-{_uid()}"
    scheduler.create_schedule_group(Name=group)
    for i in range(3):
        scheduler.create_schedule(
            Name=f"cascade-{i}", GroupName=group,
            ScheduleExpression="rate(1 hour)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
                    "RoleArn": "arn:aws:iam::000000000000:role/test"},
        )
    resp = scheduler.list_schedules(GroupName=group)
    assert len(resp["Schedules"]) == 3

    scheduler.delete_schedule_group(Name=group)

    # Group gone
    with pytest.raises(ClientError) as exc:
        scheduler.get_schedule_group(Name=group)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# CloudFormation integration
# ---------------------------------------------------------------------------

def test_scheduler_cfn_creates_schedule(cfn, scheduler):
    """AWS::Scheduler::Schedule via CFN should be queryable via Scheduler API."""
    uid = _uid()
    sched_name = f"cfn-sched-{uid}"
    group_name = f"cfn-group-{uid}"

    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Group": {
                "Type": "AWS::Scheduler::ScheduleGroup",
                "Properties": {"Name": group_name},
            },
            "Schedule": {
                "Type": "AWS::Scheduler::Schedule",
                "Properties": {
                    "Name": sched_name,
                    "GroupName": group_name,
                    "ScheduleExpression": "rate(10 minutes)",
                    "FlexibleTimeWindow": {"Mode": "OFF"},
                    "Target": {
                        "Arn": "arn:aws:lambda:us-east-1:000000000000:function:cfn-target",
                        "RoleArn": "arn:aws:iam::000000000000:role/test",
                    },
                },
            },
        },
    })

    stack_name = f"sched-stack-{uid}"
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    time.sleep(2)

    stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    # Verify via Scheduler API
    resp = scheduler.get_schedule(Name=sched_name, GroupName=group_name)
    assert resp["Name"] == sched_name
    assert resp["GroupName"] == group_name
    assert resp["ScheduleExpression"] == "rate(10 minutes)"

    resp = scheduler.get_schedule_group(Name=group_name)
    assert resp["Name"] == group_name

    # Delete stack
    cfn.delete_stack(StackName=stack_name)
    time.sleep(1)
