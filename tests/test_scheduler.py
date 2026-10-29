import asyncio
import json
import os
import time

import boto3
import pytest
from botocore.exceptions import ClientError

import ministack.services.scheduler as scheduler_mod
from ministack.core.responses import (
    get_account_id,
    get_region,
    set_request_account_id,
    set_request_region,
)

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
REGION = "us-east-1"


def _scheduler_client(region):
    return boto3.client(
        "scheduler",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
    )


@pytest.fixture(scope="module")
def scheduler():
    return _scheduler_client(REGION)


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


def test_scheduler_groups_schedules_and_tags_are_region_scoped(scheduler):
    west = _scheduler_client("us-west-2")
    group = f"region-group-{_uid()}"
    name = f"region-schedule-{_uid()}"

    east_group_arn = scheduler.create_schedule_group(Name=group)["ScheduleGroupArn"]
    west_group_arn = west.create_schedule_group(Name=group)["ScheduleGroupArn"]
    assert east_group_arn == f"arn:aws:scheduler:us-east-1:000000000000:schedule-group/{group}"
    assert west_group_arn == f"arn:aws:scheduler:us-west-2:000000000000:schedule-group/{group}"
    assert [item["Arn"] for item in scheduler.list_schedule_groups(NamePrefix=group)["ScheduleGroups"]] == [
        east_group_arn
    ]
    assert [item["Arn"] for item in west.list_schedule_groups(NamePrefix=group)["ScheduleGroups"]] == [
        west_group_arn
    ]

    east_schedule_arn = scheduler.create_schedule(
        Name=name,
        GroupName=group,
        ScheduleExpression="rate(1 hour)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": "arn:aws:lambda:us-east-1:000000000000:function:east-target",
            "RoleArn": "arn:aws:iam::000000000000:role/test",
        },
    )["ScheduleArn"]
    west_schedule_arn = west.create_schedule(
        Name=name,
        GroupName=group,
        ScheduleExpression="rate(2 hours)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": "arn:aws:lambda:us-west-2:000000000000:function:west-target",
            "RoleArn": "arn:aws:iam::000000000000:role/test",
        },
    )["ScheduleArn"]
    assert east_schedule_arn == f"arn:aws:scheduler:us-east-1:000000000000:schedule/{group}/{name}"
    assert west_schedule_arn == f"arn:aws:scheduler:us-west-2:000000000000:schedule/{group}/{name}"

    assert scheduler.get_schedule(Name=name, GroupName=group)["ScheduleExpression"] == "rate(1 hour)"
    assert west.get_schedule(Name=name, GroupName=group)["ScheduleExpression"] == "rate(2 hours)"
    assert [item["Arn"] for item in scheduler.list_schedules(GroupName=group)["Schedules"]] == [
        east_schedule_arn
    ]
    assert [item["Arn"] for item in west.list_schedules(GroupName=group)["Schedules"]] == [
        west_schedule_arn
    ]

    scheduler.tag_resource(ResourceArn=east_schedule_arn, Tags=[{"Key": "region", "Value": "east"}])
    west.tag_resource(ResourceArn=west_schedule_arn, Tags=[{"Key": "region", "Value": "west"}])
    assert scheduler.list_tags_for_resource(ResourceArn=east_schedule_arn)["Tags"] == [
        {"Key": "region", "Value": "east"}
    ]
    assert west.list_tags_for_resource(ResourceArn=west_schedule_arn)["Tags"] == [
        {"Key": "region", "Value": "west"}
    ]

    scheduler.update_schedule(
        Name=name,
        GroupName=group,
        ScheduleExpression="rate(3 hours)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": "arn:aws:lambda:us-east-1:000000000000:function:east-target-updated",
            "RoleArn": "arn:aws:iam::000000000000:role/test",
        },
    )
    assert scheduler.get_schedule(Name=name, GroupName=group)["ScheduleExpression"] == "rate(3 hours)"
    assert west.get_schedule(Name=name, GroupName=group)["ScheduleExpression"] == "rate(2 hours)"

    scheduler.delete_schedule(Name=name, GroupName=group)
    with pytest.raises(ClientError) as exc:
        scheduler.get_schedule(Name=name, GroupName=group)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    with pytest.raises(ClientError) as exc:
        scheduler.update_schedule(
            Name=name,
            GroupName=group,
            ScheduleExpression="rate(4 hours)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": "arn:aws:lambda:us-east-1:000000000000:function:missing",
                "RoleArn": "arn:aws:iam::000000000000:role/test",
            },
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    with pytest.raises(ClientError) as exc:
        scheduler.delete_schedule(Name=name, GroupName=group)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    scheduler.delete_schedule_group(Name=group)
    assert west.get_schedule_group(Name=group)["Arn"] == west_group_arn
    assert west.get_schedule(Name=name, GroupName=group)["Arn"] == west_schedule_arn
    assert west.list_tags_for_resource(ResourceArn=west_schedule_arn)["Tags"] == [
        {"Key": "region", "Value": "west"}
    ]
    west.delete_schedule_group(Name=group)


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


def test_scheduler_fires_at_expression_to_sqs(scheduler):
    """A standalone schedule must actually fire: an at() schedule in the near past
    delivers to its SQS target within one tick of the daemon — #958."""
    import datetime as _dt

    sqs = boto3.client("sqs", endpoint_url=ENDPOINT, aws_access_key_id="test",
                       aws_secret_access_key="test", region_name=REGION)
    name = f"fire-{_uid()}"
    q_url = sqs.create_queue(QueueName=f"sched-target-{_uid()}")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    past = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%S")
    scheduler.create_schedule(
        Name=name,
        ScheduleExpression=f"at({past})",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={"Arn": q_arn, "RoleArn": "arn:aws:iam::000000000000:role/r",
                "Input": "hello-from-schedule"},
    )
    try:
        deadline = time.time() + 20
        body = None
        while time.time() < deadline:
            msgs = sqs.receive_message(
                QueueUrl=q_url, WaitTimeSeconds=1, MaxNumberOfMessages=1
            ).get("Messages", [])
            if msgs:
                body = msgs[0]["Body"]
                break
        assert body == "hello-from-schedule", f"schedule did not fire (body={body!r})"
    finally:
        try:
            scheduler.delete_schedule(Name=name)
        except ClientError:
            pass
        sqs.delete_queue(QueueUrl=q_url)


# ---------------------------------------------------------------------------
# ARN adoption, region scoping, and the firing ticker. Folded from
# test_scheduler_arn_adoption.py: these drive the scheduler module
# in-process (scheduler_mod.*) rather than through boto3.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_scheduler():
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    scheduler_mod.reset()
    scheduler_mod._schedule_last_fired.clear()
    scheduler_mod._ticker_thread = None
    yield
    scheduler_mod.reset()
    scheduler_mod._schedule_last_fired.clear()
    scheduler_mod._ticker_thread = None


def _request(method, path, body=None, query=None):
    payload = json.dumps(body or {}).encode()
    return asyncio.run(
        scheduler_mod.handle_request(method, path, {}, payload, query or {})
    )


def _body(resp):
    return json.loads(resp[2].decode())


def _create_schedule(group="default"):
    name = f"sched-{_uid()}"
    body = {
        "ScheduleExpression": "rate(1 hour)",
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "Target": {
            "Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop",
            "RoleArn": "arn:aws:iam::000000000000:role/test",
        },
    }
    if group != "default":
        body["GroupName"] = group
    resp = _request("POST", f"/schedules/{name}", body)
    assert resp[0] == 200
    return name, _body(resp)["ScheduleArn"]


def _create_group():
    name = f"group-{_uid()}"
    resp = _request("POST", f"/schedule-groups/{name}")
    assert resp[0] == 200
    return name, _body(resp)["ScheduleGroupArn"]


def _assert_error(resp, status, code):
    assert resp[0] == status
    assert _body(resp)["__type"] == code


def test_scheduler_tag_apis_accept_local_schedule_arn():
    name, arn = _create_schedule()

    resp = _request("POST", f"/tags/{arn}", {"Tags": [{"Key": "env", "Value": "test"}]})
    assert resp[0] == 200

    resp = _request("GET", f"/tags/{arn}")
    assert _body(resp)["Tags"] == [{"Key": "env", "Value": "test"}]

    resp = _request("DELETE", f"/tags/{arn}", query={"TagKeys": ["env"]})
    assert resp[0] == 200
    assert _body(_request("GET", f"/tags/{arn}"))["Tags"] == []
    assert f"default/{name}" in scheduler_mod._schedules


def test_scheduler_tag_apis_accept_local_schedule_group_arn():
    _name, arn = _create_group()

    resp = _request("POST", f"/tags/{arn}", {"Tags": [{"Key": "team", "Value": "platform"}]})
    assert resp[0] == 200

    resp = _request("GET", f"/tags/{arn}")
    assert _body(resp)["Tags"] == [{"Key": "team", "Value": "platform"}]


def test_scheduler_tag_apis_accept_default_schedule_group_arn():
    arn = "arn:aws:scheduler:us-east-1:000000000000:schedule-group/default"

    resp = _request("POST", f"/tags/{arn}", {"Tags": [{"Key": "default", "Value": "yes"}]})
    assert resp[0] == 200

    resp = _request("GET", f"/tags/{arn}")
    assert _body(resp)["Tags"] == [{"Key": "default", "Value": "yes"}]


def test_scheduler_tag_apis_do_not_resolve_same_named_resources_from_other_region():
    set_request_region("us-west-2")
    _schedule_name, west_schedule_arn = _create_schedule()
    _group_name, west_group_arn = _create_group()

    set_request_region("us-east-1")
    east_schedule_arn = west_schedule_arn.replace(":us-west-2:", ":us-east-1:")
    east_group_arn = west_group_arn.replace(":us-west-2:", ":us-east-1:")

    for arn in (east_schedule_arn, east_group_arn):
        resp = _request("POST", f"/tags/{arn}", {"Tags": [{"Key": "env", "Value": "bad"}]})
        _assert_error(resp, 404, "ResourceNotFoundException")
        assert scheduler_mod._tags.get(arn) is None

    assert scheduler_mod._tags.get(west_schedule_arn) is None
    assert scheduler_mod._tags.get(west_group_arn) is None


def test_scheduler_ticker_dispatches_in_schedule_region_and_restores_context(monkeypatch):
    from ministack.services import eventbridge

    account_id = "000000000000"
    name = f"ticker-{_uid()}"
    key = f"default/{name}"
    expected = {}
    for region in ("us-east-1", "us-west-2"):
        schedule_arn = f"arn:aws:scheduler:{region}:{account_id}:schedule/{key}"
        target_arn = f"arn:aws:lambda:{region}:{account_id}:function:ticker-target"
        expected[region] = (schedule_arn, target_arn)
        scheduler_mod._schedules.set_scoped(
            account_id,
            region,
            key,
            {
                "Arn": schedule_arn,
                "Name": name,
                "GroupName": "default",
                "ScheduleExpression": "at(1970-01-01T00:00:01)",
                "Target": {"Arn": target_arn},
                "State": "ENABLED",
                "ActionAfterCompletion": "NONE",
                "CreationDate": 1,
            },
        )
    set_request_account_id("111111111111")
    set_request_region("eu-west-1")

    calls = []

    def _capture_target(target, event, schedule):
        calls.append((get_account_id(), get_region(), target, event, schedule))

    monkeypatch.setattr(eventbridge, "_invoke_target", _capture_target)
    monkeypatch.setattr(scheduler_mod.time, "time", lambda: 2.0)

    scheduler_mod._tick_schedules()

    assert len(calls) == 2
    calls_by_region = {call[1]: call for call in calls}
    assert set(calls_by_region) == set(expected)
    for region, (schedule_arn, target_arn) in expected.items():
        dispatch_account, dispatch_region, target, event, schedule = calls_by_region[region]
        assert (dispatch_account, dispatch_region) == (account_id, region)
        assert target["Arn"] == target_arn
        assert event["Account"] == account_id
        assert event["Region"] == region
        assert event["Resources"] == [schedule_arn]
        assert schedule["Arn"] == schedule_arn
    assert (get_account_id(), get_region()) == ("111111111111", "eu-west-1")


def test_scheduler_start_scheduler_starts_daemon_once(monkeypatch):
    created_threads = []

    class _FakeThread:
        def __init__(self, *, target, daemon, name):
            self.target = target
            self.daemon = daemon
            self.name = name
            self.started = False
            created_threads.append(self)

        def start(self):
            self.started = True

        def is_alive(self):
            return self.started

    monkeypatch.setattr(scheduler_mod.threading, "Thread", _FakeThread)

    scheduler_mod.start_scheduler()
    scheduler_mod.start_scheduler()

    assert len(created_threads) == 1
    thread = created_threads[0]
    assert thread.target is scheduler_mod._ticker_loop
    assert thread.daemon is True
    assert thread.name == "evb-scheduler-ticker"
    assert thread.started is True


def test_scheduler_unknown_route_returns_validation_error():
    response = _request("GET", "/unknown")

    _assert_error(response, 400, "ValidationException")


@pytest.mark.parametrize(
    "bad_arn, expected_status, expected_code",
    [
        ("not-an-arn", 400, "ValidationException"),
        (
            "arn:aws-us-gov:scheduler:us-east-1:000000000000:schedule/default/{name}",
            404,
            "ResourceNotFoundException",
        ),
        (
            "arn:aws:events:us-east-1:000000000000:schedule/default/{name}",
            404,
            "ResourceNotFoundException",
        ),
        (
            "arn:aws:scheduler:us-east-1:111111111111:schedule/default/{name}",
            404,
            "ResourceNotFoundException",
        ),
        (
            "arn:aws:scheduler:us-west-2:000000000000:schedule/default/{name}",
            404,
            "ResourceNotFoundException",
        ),
        (
            "arn:aws:scheduler:us-east-1:000000000000:schedule/default",
            404,
            "ResourceNotFoundException",
        ),
        (
            "arn:aws:scheduler:us-east-1:000000000000:schedule/default/{name}/extra",
            404,
            "ResourceNotFoundException",
        ),
        (
            "arn:aws:scheduler:us-east-1:000000000000:rule/default/{name}",
            404,
            "ResourceNotFoundException",
        ),
        (
            "arn:aws:scheduler:us-east-1:000000000000:schedule/default/missing-{name}",
            404,
            "ResourceNotFoundException",
        ),
        (
            "arn:aws:scheduler:us-east-1:000000000000:schedule-group/missing-{name}",
            404,
            "ResourceNotFoundException",
        ),
    ],
)
@pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
def test_scheduler_tag_apis_reject_invalid_resource_arns_before_touching_tags(
    method, bad_arn, expected_status, expected_code
):
    name, good_arn = _create_schedule()
    _request("POST", f"/tags/{good_arn}", {"Tags": [{"Key": "env", "Value": "good"}]})
    baseline = dict(scheduler_mod._tags.get(good_arn, {}))
    arn = bad_arn.format(name=name)

    body = {"Tags": [{"Key": "env", "Value": "bad"}]} if method == "POST" else None
    query = {"TagKeys": ["env"]} if method == "DELETE" else None
    resp = _request(method, f"/tags/{arn}", body=body, query=query)

    _assert_error(resp, expected_status, expected_code)
    assert scheduler_mod._tags.get(good_arn, {}) == baseline
    assert scheduler_mod._tags.get(arn) is None


def test_scheduler_schedule_error_messages(scheduler):
    """Real AWS (us-east-1, #2467 r2 q12/q13/q15/q16/q17): schedule messages do not name the group."""
    import uuid as _uuid

    group = f"msg-grp-{_uuid.uuid4().hex[:8]}"
    name, missing = f"msg-{_uuid.uuid4().hex[:8]}", f"msg-missing-{_uuid.uuid4().hex[:8]}"
    scheduler.create_schedule_group(Name=group)
    common = {
        "GroupName": group,
        "ScheduleExpression": "at(2030-01-01T00:00:00)",
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "Target": {"Arn": "arn:aws:sqs:us-east-1:000000000000:q", "RoleArn": "arn:aws:iam::000000000000:role/r"},
    }
    try:
        scheduler.create_schedule(Name=name, **common)
        calls = [
            (lambda: scheduler.get_schedule(Name=missing, GroupName=group), "ResourceNotFoundException",
             f"Schedule {missing} does not exist."),
            (lambda: scheduler.create_schedule(Name=name, **common), "ConflictException",
             f"Schedule {name} already exists."),
            (lambda: scheduler.update_schedule(Name=missing, **common), "ResourceNotFoundException",
             f"Schedule {missing} does not exist."),
            (lambda: scheduler.delete_schedule(Name=missing, GroupName=group), "ResourceNotFoundException",
             f"Schedule {missing} does not exist."),
            (lambda: scheduler.get_schedule(Name=name), "ResourceNotFoundException",
             f"Schedule {name} does not exist."),
        ]
        for call, code, message in calls:
            with pytest.raises(ClientError) as exc:
                call()
            assert exc.value.response["Error"]["Code"] == code
            assert exc.value.response["Error"]["Message"] == message
    finally:
        scheduler.delete_schedule_group(Name=group)


def test_scheduler_event_bus_target_uses_eventbridge_parameters(scheduler):
    """#2466: a schedule whose target is an event bus publishes with the Source and DetailType of
    its EventBridgeParameters, and Input as the detail, so a rule matching them sees the event."""
    import datetime as _dt
    import json as _json

    kw = dict(endpoint_url=ENDPOINT, aws_access_key_id="test", aws_secret_access_key="test", region_name=REGION)
    sqs, events = boto3.client("sqs", **kw), boto3.client("events", **kw)
    bus = f"sched-bus-{_uid()}"
    bus_arn = events.create_event_bus(Name=bus)["EventBusArn"]
    q_url = sqs.create_queue(QueueName=f"sched-bus-q-{_uid()}")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    events.put_rule(Name="obs", EventBusName=bus,
                    EventPattern=_json.dumps({"source": ["my.source"], "detail-type": ["delayed-event-publish"]}))
    events.put_targets(Rule="obs", EventBusName=bus, Targets=[{"Id": "1", "Arn": q_arn}])
    name = f"fire-bus-{_uid()}"
    past = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%S")
    scheduler.create_schedule(
        Name=name, ScheduleExpression=f"at({past})", FlexibleTimeWindow={"Mode": "OFF"},
        Target={"Arn": bus_arn, "RoleArn": "arn:aws:iam::000000000000:role/r",
                "EventBridgeParameters": {"DetailType": "delayed-event-publish", "Source": "my.source"},
                "Input": _json.dumps({"run": "x"})})
    try:
        deadline, event = time.time() + 20, None
        while time.time() < deadline and event is None:
            msgs = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=1, MaxNumberOfMessages=1).get("Messages", [])
            if msgs:
                event = _json.loads(msgs[0]["Body"])
        assert event is not None, "the rule matching EventBridgeParameters never saw the event"
        assert (event["source"], event["detail-type"], event["detail"]) == (
            "my.source", "delayed-event-publish", {"run": "x"})
    finally:
        try:
            scheduler.delete_schedule(Name=name)
        except ClientError:
            pass
        events.remove_targets(Rule="obs", EventBusName=bus, Ids=["1"])
        events.delete_rule(Name="obs", EventBusName=bus)
        events.delete_event_bus(Name=bus)
        sqs.delete_queue(QueueUrl=q_url)


_EB_PATTERN = r"(?=[/\.\-_A-Za-z0-9]+)((?!aws\.).*)|(\$(\.[\w_-]+(\[(\d+|\*)\])*)*)"
_EB_SOURCE = "at 'target.eventBridgeParameters.source' failed to satisfy constraint"
_EB_DETAIL_TYPE = "at 'target.eventBridgeParameters.detailType' failed to satisfy constraint"
_EMOJI = "\U0001F600"  # one code point, two UTF-16 units


@pytest.fixture(scope="module")
def scheduler_raw():
    from botocore.config import Config

    return boto3.client("scheduler", endpoint_url=ENDPOINT, aws_access_key_id="test",
                        aws_secret_access_key="test", region_name=REGION,
                        config=Config(parameter_validation=False))


def _eb_target(arn="arn:aws:events:us-east-1:000000000000:event-bus/default", **params):
    return {"Arn": arn, "RoleArn": "arn:aws:iam::000000000000:role/r", "EventBridgeParameters": params}


def _eb_schedule(client, name, target, **extra):
    return client.create_schedule(Name=name, ScheduleExpression="at(2030-01-01T00:00:00)",
                                  FlexibleTimeWindow={"Mode": "OFF"}, Target=target, **extra)


@pytest.mark.parametrize("params, message", [
    ({"DetailType": "dt", "Source": "aws.verify"},
     f"1 validation error detected: Value 'aws.verify' {_EB_SOURCE}: "
     f"Member must satisfy regular expression pattern: {_EB_PATTERN}"),
    ({"DetailType": "dt", "Source": "aws."},
     f"1 validation error detected: Value 'aws.' {_EB_SOURCE}: "
     f"Member must satisfy regular expression pattern: {_EB_PATTERN}"),
    ({"DetailType": "dt", "Source": "!bad"},
     f"1 validation error detected: Value '!bad' {_EB_SOURCE}: "
     f"Member must satisfy regular expression pattern: {_EB_PATTERN}"),
    ({"DetailType": "dt", "Source": " x"},
     f"1 validation error detected: Value ' x' {_EB_SOURCE}: "
     f"Member must satisfy regular expression pattern: {_EB_PATTERN}"),
    ({"DetailType": "dt", "Source": "$foo"},
     f"1 validation error detected: Value '$foo' {_EB_SOURCE}: "
     f"Member must satisfy regular expression pattern: {_EB_PATTERN}"),
    ({"DetailType": "d" * 129, "Source": "ministack.verify"},
     f"1 validation error detected: Value '{'d' * 129}' {_EB_DETAIL_TYPE}: "
     "Member must have length less than or equal to 128"),
    ({"DetailType": "dt", "Source": "a" * 257},
     f"1 validation error detected: Value '{'a' * 257}' {_EB_SOURCE}: "
     "Member must have length less than or equal to 256"),
    ({"DetailType": "", "Source": "ministack.verify"},
     f"1 validation error detected: Value '' {_EB_DETAIL_TYPE}: "
     "Member must have length greater than or equal to 1"),
    ({"DetailType": "dt", "Source": ""},
     f"2 validation errors detected: Value '' {_EB_SOURCE}: "
     f"Member must satisfy regular expression pattern: {_EB_PATTERN}; Value '' {_EB_SOURCE}: "
     "Member must have length greater than or equal to 1"),
    ({"DetailType": "", "Source": "aws.verify"},
     f"2 validation errors detected: Value '' {_EB_DETAIL_TYPE}: "
     f"Member must have length greater than or equal to 1; Value 'aws.verify' {_EB_SOURCE}: "
     f"Member must satisfy regular expression pattern: {_EB_PATTERN}"),
    ({"Source": "ministack.verify"},
     f"1 validation error detected: Value null {_EB_DETAIL_TYPE}: Member must not be null"),
    ({"DetailType": "dt"},
     f"1 validation error detected: Value null {_EB_SOURCE}: Member must not be null"),
    ({"DetailType": _EMOJI * 129, "Source": "ministack.verify"},
     f"1 validation error detected: Value '{_EMOJI * 129}' {_EB_DETAIL_TYPE}: "
     "Member must have length less than or equal to 128"),
], ids=["aws-prefix", "aws-dot", "bang", "leading-space", "dollar-no-path", "detail-type-129",
        "source-257", "detail-type-empty", "source-empty", "detail-type-empty-and-aws",
        "detail-type-null", "source-null", "detail-type-129-emoji"])
def test_scheduler_eventbridge_parameters_validation(scheduler_raw, params, message):
    """Real AWS (us-east-1, #2466 r1 v1, r2 v4/v7/v8, r3 w5..w10/w12/w13, r7 d2): CreateSchedule checks
    Target.EventBridgeParameters and aggregates the errors."""
    name = f"ebp-bad-{_uid()}"
    with pytest.raises(ClientError) as exc:
        _eb_schedule(scheduler_raw, name, _eb_target(**params))
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert exc.value.response["Error"]["Message"] == message


@pytest.mark.parametrize("source, detail_type", [
    ("$.detail.src", "dt"), ("AWS.verify", "dt"), ("aws", "dt"), ("a!b", "dt"), ("d" * 256, "d" * 128),
    ("ministack.verify", _EMOJI * 128),
], ids=["json-path", "upper-aws", "aws-no-dot", "bang-inside", "max-lengths", "detail-type-128-emoji"])
def test_scheduler_eventbridge_parameters_accepted(scheduler, source, detail_type):
    """Real AWS (us-east-1, #2466 r1 v3, r2 v5/v6, r3 w4/w11, r7 d1): these EventBridgeParameters are accepted;
    DetailType length counts code points."""
    name = f"ebp-ok-{_uid()}"
    _eb_schedule(scheduler, name, _eb_target(DetailType=detail_type, Source=source))
    try:
        got = scheduler.get_schedule(Name=name)["Target"]["EventBridgeParameters"]
        assert got == {"DetailType": detail_type, "Source": source}
    finally:
        scheduler.delete_schedule(Name=name)


def test_scheduler_eventbridge_parameters_checked_before_lookups(scheduler, scheduler_raw):
    """Real AWS (us-east-1, #2466 r2 u1, r3 w1/w2/w3/w14): an aws. Source is refused before the
    conflict, missing schedule and missing group checks, for any target, and an update leaves
    the schedule unchanged."""
    message = (f"1 validation error detected: Value 'aws.verify' {_EB_SOURCE}: "
               f"Member must satisfy regular expression pattern: {_EB_PATTERN}")
    bad = _eb_target(DetailType="dt", Source="aws.verify")
    name = f"ebp-order-{_uid()}"
    _eb_schedule(scheduler, name, _eb_target(DetailType="dt", Source="ministack.verify"))
    common = {"ScheduleExpression": "at(2030-01-01T00:00:00)", "FlexibleTimeWindow": {"Mode": "OFF"}}
    calls = [
        lambda: _eb_schedule(scheduler_raw, name, bad),
        lambda: scheduler_raw.update_schedule(Name=name, Target=bad, **common),
        lambda: scheduler_raw.update_schedule(Name=f"ebp-missing-{_uid()}", Target=bad, **common),
        lambda: _eb_schedule(scheduler_raw, f"ebp-g-{_uid()}", bad, GroupName=f"ebp-nogroup-{_uid()}"),
        lambda: _eb_schedule(scheduler_raw, f"ebp-q-{_uid()}",
                             {**bad, "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}),
    ]
    try:
        for call in calls:
            with pytest.raises(ClientError) as exc:
                call()
            assert exc.value.response["Error"]["Code"] == "ValidationException"
            assert exc.value.response["Error"]["Message"] == message
        got = scheduler.get_schedule(Name=name)["Target"]["EventBridgeParameters"]
        assert got == {"DetailType": "dt", "Source": "ministack.verify"}
    finally:
        scheduler.delete_schedule(Name=name)


def test_scheduler_event_bus_target_envelope(scheduler):
    """Real AWS (us-east-1, #2466 r1 f1/f2, r2 f3/f4): an event bus target publishes with no
    resources, Input as the detail or {} without it, and publishes nothing without
    EventBridgeParameters."""
    import datetime as _dt
    import json as _json

    kw = dict(endpoint_url=ENDPOINT, aws_access_key_id="test", aws_secret_access_key="test", region_name=REGION)
    sqs, events = boto3.client("sqs", **kw), boto3.client("events", **kw)
    account = boto3.client("sts", **kw).get_caller_identity()["Account"]
    bus = f"sched-env-{_uid()}"
    bus_arn = events.create_event_bus(Name=bus)["EventBusArn"]
    q_url = sqs.create_queue(QueueName=f"sched-env-q-{_uid()}")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    events.put_rule(Name="all", EventBusName=bus, EventPattern=_json.dumps({"account": [account]}))
    events.put_targets(Rule="all", EventBusName=bus, Targets=[{"Id": "q", "Arn": q_arn}])
    past = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%S")
    role = "arn:aws:iam::000000000000:role/r"
    ebp = {"EventBridgeParameters": {"DetailType": "verify-detail-type", "Source": "ministack.verify"}}
    targets = {
        "f1": {"Arn": bus_arn, "RoleArn": role, **ebp, "Input": _json.dumps({"run": "f1", "n": 1})},
        "f2": {"Arn": bus_arn, "RoleArn": role, **ebp},
        "f3": {"Arn": bus_arn, "RoleArn": role, "Input": _json.dumps({"run": "f3"})},
        "f4": {"Arn": bus_arn, "RoleArn": role},
    }
    names = {k: f"env-{k}-{_uid()}" for k in targets}
    for k, target in targets.items():
        scheduler.create_schedule(Name=names[k], ScheduleExpression=f"at({past})",
                                  FlexibleTimeWindow={"Mode": "OFF"}, Target=target)
    try:
        received, deadline = [], time.time() + 25
        while time.time() < deadline and len(received) < 2:
            msgs = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=1, MaxNumberOfMessages=10).get("Messages", [])
            received += [_json.loads(m["Body"]) for m in msgs]
        # f1..f4 fire in the same sweep; give it a moment for anything extra to land.
        received += [_json.loads(m["Body"]) for m in
                     sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2, MaxNumberOfMessages=10).get("Messages", [])]
        got = sorted(((e["source"], e["detail-type"], _json.dumps(e["detail"], sort_keys=True), e["resources"])
                      for e in received), key=lambda t: t[2])
        assert got == [
            ("ministack.verify", "verify-detail-type", '{"n": 1, "run": "f1"}', []),
            ("ministack.verify", "verify-detail-type", "{}", []),
        ]
    finally:
        for name in names.values():
            try:
                scheduler.delete_schedule(Name=name)
            except ClientError:
                pass
        events.remove_targets(Rule="all", EventBusName=bus, Ids=["q"])
        events.delete_rule(Name="all", EventBusName=bus)
        events.delete_event_bus(Name=bus)
        sqs.delete_queue(QueueUrl=q_url)


def _create_raw_eb(client, name, eb_params_json, op="CreateSchedule"):
    """CreateSchedule (or UpdateSchedule) whose Target.EventBridgeParameters is the raw JSON text given."""
    import uuid as _uuid

    body = ('{"ScheduleExpression": "at(2030-01-01T00:00:00)", "FlexibleTimeWindow": {"Mode": "OFF"}, '
            f'"ClientToken": "{_uuid.uuid4().hex}", "Target": {{"Arn": '
            '"arn:aws:events:us-east-1:000000000000:event-bus/default", '
            f'"RoleArn": "arn:aws:iam::000000000000:role/r", "EventBridgeParameters": {eb_params_json}}}}}')

    def override(params, **_):
        params["body"] = body.encode("utf-8")

    event = f"before-call.scheduler.{op}"
    call = client.create_schedule if op == "CreateSchedule" else client.update_schedule
    client.meta.events.register(event, override)
    try:
        return call(Name=name, ScheduleExpression="at(2030-01-01T00:00:00)",
                    FlexibleTimeWindow={"Mode": "OFF"}, Target={"Arn": "x", "RoleArn": "x"})
    finally:
        client.meta.events.unregister(event, override)


_SRC = '"Source": "ministack.verify"'


@pytest.mark.parametrize("eb_json, code, message", [
    ('{"DetailType": 5, %s}' % _SRC, "SerializationException", "NUMBER_VALUE can not be converted to a String"),
    ('{"DetailType": 1.5, %s}' % _SRC, "SerializationException", "NUMBER_VALUE can not be converted to a String"),
    ('{"DetailType": "dt", "Source": true}', "SerializationException", "TRUE_VALUE can not be converted to a String"),
    ('{"DetailType": false, %s}' % _SRC, "SerializationException", "FALSE_VALUE can not be converted to a String"),
    ('{"DetailType": {}, %s}' % _SRC, "SerializationException", "Start of structure or map found where not expected."),
    ('{"DetailType": [], %s}' % _SRC, "SerializationException", "Start of list found where not expected"),
    ('{"DetailType": "", "Source": 7}', "SerializationException", "NUMBER_VALUE can not be converted to a String"),
    ('{"Source": true, "DetailType": 5}', "SerializationException", "NUMBER_VALUE can not be converted to a String"),
    ('"str"', "SerializationException", "Expected null"),
    ("5", "SerializationException", "Expected null"),
    ("1.5", "SerializationException", "Expected null"),
    ("true", "SerializationException", "Expected null"),
    ("false", "SerializationException", "Expected null"),
    ("[]", "SerializationException", "Start of list found where not expected"),
    ("{}", "ValidationException",
     f"2 validation errors detected: Value null {_EB_DETAIL_TYPE}: Member must not be null; "
     f"Value null {_EB_SOURCE}: Member must not be null"),
    ('{"DetailType": null, %s}' % _SRC, "ValidationException",
     f"1 validation error detected: Value null {_EB_DETAIL_TYPE}: Member must not be null"),
    ('{"DetailType": "dt", "Source": "aws.%s"}' % ("a" * 253), "ValidationException",
     f"2 validation errors detected: Value 'aws.{'a' * 253}' {_EB_SOURCE}: Member must satisfy regular "
     f"expression pattern: {_EB_PATTERN}; Value 'aws.{'a' * 253}' {_EB_SOURCE}: "
     "Member must have length less than or equal to 256"),
    ('{"DetailType": "dt", "Source": "$.\\u00e9"}', "ValidationException",
     f"1 validation error detected: Value '$.é' {_EB_SOURCE}: "
     f"Member must satisfy regular expression pattern: {_EB_PATTERN}"),
    ('{"DetailType": "dt", "Source": "%s"}' % ("\\u00e9" * 129 + "a"), "ValidationException",
     f"1 validation error detected: Value '{chr(0xe9) * 129}a' {_EB_SOURCE}: "
     f"Member must satisfy regular expression pattern: {_EB_PATTERN}"),
] + [
    ('{"DetailType": "dt", "Source": "a%sb"}' % esc, "ValidationException",
     f"1 validation error detected: Value 'a{char}b' {_EB_SOURCE}: "
     f"Member must satisfy regular expression pattern: {_EB_PATTERN}")
    for esc, char in (("\\r", "\r"), ("\\n", "\n"), ("\\r\\n", "\r\n"), ("\\u0085", "\u0085"), ("\\u2028", "\u2028"),
                      ("\\u2029", "\u2029"))
], ids=["detail-type-int", "detail-type-float", "source-true", "detail-type-false", "detail-type-object",
        "detail-type-list", "type-before-validation", "detail-type-reported-first", "params-string",
        "params-number", "params-float", "params-true", "params-false", "params-list", "params-empty", "detail-type-explicit-null",
        "pattern-before-length", "json-path-non-ascii", "non-ascii-first",
        "cr", "lf", "crlf", "nel", "line-separator", "paragraph-separator"])
def test_scheduler_eventbridge_parameters_raw_json(scheduler_raw, eb_json, code, message):
    """Real AWS (us-east-1, #2466 r4 x1..x7/x9..x13, r5 y3..y10/y12..y14, r6 z6/z8): a member of the wrong JSON
    type is a SerializationException before validation; Source's "." stops at Java line terminators."""
    with pytest.raises(ClientError) as exc:
        _create_raw_eb(scheduler_raw, f"ebp-raw-{_uid()}", eb_json)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == code
    assert exc.value.response["Error"]["Message"] == message


@pytest.mark.parametrize("eb_json", [
    "null",
    '{"DetailType": "dt", "Source": "a\\u00e9"}',
    '{"DetailType": "dt", "Source": "a\\u000bb"}',
    '{"DetailType": "dt", "Source": "a%s"}' % ("\\ud83d\\ude00" * 128),
    '{"DetailType": "dt", "Source": "$"}',
], ids=["params-null", "non-ascii-after-first", "vertical-tab", "257-utf16-units", "dollar-alone"])
def test_scheduler_eventbridge_parameters_raw_json_accepted(scheduler, scheduler_raw, eb_json):
    """Real AWS (us-east-1, #2466 r5 y1/y2/y11/y15, r6 z7): these are accepted; Source length counts code points."""
    name = f"ebp-raw-ok-{_uid()}"
    _create_raw_eb(scheduler_raw, name, eb_json)
    scheduler.delete_schedule(Name=name)


@pytest.mark.parametrize("eb_json, code, message", [
    ('{"DetailType": 5, %s}' % _SRC, "SerializationException", "NUMBER_VALUE can not be converted to a String"),
    ("[]", "SerializationException", "Start of list found where not expected"),
    ('"str"', "SerializationException", "Expected null"),
    ("{}", "ValidationException",
     f"2 validation errors detected: Value null {_EB_DETAIL_TYPE}: Member must not be null; "
     f"Value null {_EB_SOURCE}: Member must not be null"),
], ids=["detail-type-int", "params-list", "params-string", "params-empty"])
def test_scheduler_update_eventbridge_parameters_raw_json(scheduler, scheduler_raw, eb_json, code, message):
    """Real AWS (us-east-1, #2466 r6 z1..z4): UpdateSchedule answers the same errors and keeps the schedule."""
    name = f"ebp-upd-{_uid()}"
    _create_raw_eb(scheduler_raw, name, '{"DetailType": "dt", %s}' % _SRC)
    try:
        with pytest.raises(ClientError) as exc:
            _create_raw_eb(scheduler_raw, name, eb_json, op="UpdateSchedule")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert exc.value.response["Error"]["Code"] == code
        assert exc.value.response["Error"]["Message"] == message
        got = scheduler.get_schedule(Name=name)["Target"]["EventBridgeParameters"]
        assert got == {"DetailType": "dt", "Source": "ministack.verify"}
    finally:
        scheduler.delete_schedule(Name=name)
