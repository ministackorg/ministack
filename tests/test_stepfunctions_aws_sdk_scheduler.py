"""Step Functions aws-sdk:scheduler tasks (issue #2467).

Capture keys (rN pNN / qNN) refer to real AWS runs against us-east-1 with
Standard state machines whose single Task state calls
arn:aws:states:::aws-sdk:scheduler:<action>.
"""
import datetime
import json
import re
import uuid

import pytest

ROLE = "arn:aws:iam::000000000000:role/sfn-role"
TARGET_ROLE = "arn:aws:iam::000000000000:role/scheduler-role"
BUS = "arn:aws:events:us-east-1:000000000000:event-bus/default"
AT = "at(2030-01-01T00:00:00)"
ISO_MILLIS = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d{3})?Z$")
CAUSE_SUFFIX = re.compile(
    r" \(Service: Scheduler, Status Code: (\d{3}), Request ID: "
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\) \(SDK Attempt Count: 1\)$"
)
TARGET = {
    "Arn": BUS,
    "RoleArn": TARGET_ROLE,
    "EventBridgeParameters": {"DetailType": "delayed-event-publish", "Source": "ministack.verify"},
    "Input": '{"run": "x"}',
}


@pytest.fixture
def run_task(sfn_sync):
    machines = []

    def run(action, parameters=None, arguments=None, sm_input=None):
        state = {"Type": "Task", "Resource": f"arn:aws:states:::aws-sdk:scheduler:{action}", "End": True}
        definition = {"StartAt": "Call", "States": {"Call": state}}
        if arguments is not None:
            definition["QueryLanguage"] = "JSONata"
            state["Arguments"] = arguments
        else:
            state["Parameters"] = parameters
        arn = sfn_sync.create_state_machine(
            name=f"sdk-scheduler-{uuid.uuid4().hex[:8]}",
            definition=json.dumps(definition),
            roleArn=ROLE,
        )["stateMachineArn"]
        machines.append(arn)
        return sfn_sync.start_sync_execution(stateMachineArn=arn, input=json.dumps(sm_input or {}))

    yield run
    for arn in machines:
        sfn_sync.delete_state_machine(stateMachineArn=arn)


@pytest.fixture
def group(scheduler):
    name = f"sfn-sdk-grp-{uuid.uuid4().hex[:8]}"
    scheduler.create_schedule_group(Name=name)
    yield name
    try:
        scheduler.delete_schedule_group(Name=name)
    except scheduler.exceptions.ResourceNotFoundException:
        pass


def _output(resp):
    assert resp["status"] == "SUCCEEDED", f"{resp.get('error')}: {resp.get('cause')}"
    return json.loads(resp["output"])


def _failure(resp):
    assert resp["status"] == "FAILED"
    match = CAUSE_SUFFIX.search(resp["cause"])
    assert match, resp["cause"]
    return resp["error"], resp["cause"][:match.start()], int(match.group(1))


def _schedule(name, group, **extra):
    return {"Name": name, "GroupName": group, "ScheduleExpression": AT,
            "FlexibleTimeWindow": {"Mode": "OFF"}, "Target": TARGET, **extra}


def test_schedule_group_tasks(run_task, scheduler):
    """r1 p01 createScheduleGroup, p02 getScheduleGroup, p03 listScheduleGroups, p19 deleteScheduleGroup."""
    name = f"sfn-sdk-grp-{uuid.uuid4().hex[:8]}"
    created = _output(run_task("createScheduleGroup", {
        "Name": name, "Tags": [{"Key": "Owner", "Value": "ministack"}]}))
    assert created == {"ScheduleGroupArn": f"arn:aws:scheduler:us-east-1:000000000000:schedule-group/{name}"}

    got = _output(run_task("getScheduleGroup", {"Name": name}))
    assert set(got) == {"Arn", "CreationDate", "LastModificationDate", "Name", "State"}
    assert got["Arn"] == created["ScheduleGroupArn"]
    assert got["Name"] == name
    assert got["State"] == "ACTIVE"
    assert ISO_MILLIS.match(got["CreationDate"])
    assert ISO_MILLIS.match(got["LastModificationDate"])

    listed = _output(run_task("listScheduleGroups", {"NamePrefix": name}))
    assert listed == {"ScheduleGroups": [got]}

    assert _output(run_task("deleteScheduleGroup", {"Name": name})) == {}


def test_create_and_get_schedule_tasks(run_task, scheduler, group):
    """r1 p04 createSchedule outputs ScheduleArn; p05 getSchedule outputs the Scheduler
    members unchanged with ISO-8601 UTC timestamps."""
    name = f"s-{uuid.uuid4().hex[:8]}"
    created = _output(run_task("createSchedule", _schedule(name, group, ActionAfterCompletion="DELETE")))
    arn = f"arn:aws:scheduler:us-east-1:000000000000:schedule/{group}/{name}"
    assert created == {"ScheduleArn": arn}

    got = _output(run_task("getSchedule", {"Name": name, "GroupName": group}))
    assert ISO_MILLIS.match(got.pop("CreationDate"))
    assert ISO_MILLIS.match(got.pop("LastModificationDate"))
    for key, value in {
        "ActionAfterCompletion": "DELETE",
        "Arn": arn,
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "GroupName": group,
        "Name": name,
        "ScheduleExpression": AT,
        "ScheduleExpressionTimezone": "UTC",
        "State": "ENABLED",
    }.items():
        assert got[key] == value
    assert {k: got["Target"][k] for k in TARGET} == TARGET

    # r1 p16: the JSONata Arguments form sends the same request.
    assert _output(run_task("getSchedule", arguments={"Name": name, "GroupName": group}))["Arn"] == arn


def test_list_schedules_task(run_task, scheduler, group):
    """r1 p07 listSchedules by GroupName and p08 by NamePrefix; r3 t01 by State."""
    name = f"s-{uuid.uuid4().hex[:8]}"
    _output(run_task("createSchedule", _schedule(name, group)))
    for parameters in ({"GroupName": group}, {"NamePrefix": name}):
        listed = _output(run_task("listSchedules", parameters))
        assert len(listed["Schedules"]) == 1
        item = listed["Schedules"][0]
        assert set(item) == {"Arn", "CreationDate", "GroupName", "LastModificationDate", "Name",
                             "State", "Target"}
        assert item["Name"] == name
        assert item["GroupName"] == group
        assert item["State"] == "ENABLED"
        assert item["Target"] == {"Arn": BUS}
        assert ISO_MILLIS.match(item["CreationDate"])

    disabled = f"s-{uuid.uuid4().hex[:8]}"
    _output(run_task("createSchedule", _schedule(disabled, group, State="DISABLED")))
    listed = _output(run_task("listSchedules", {"GroupName": group, "State": "DISABLED"}))
    assert [item["Name"] for item in listed["Schedules"]] == [disabled]


def test_update_and_delete_schedule_tasks(run_task, scheduler, group):
    """r1 p14 updateSchedule outputs ScheduleArn and replaces the schedule; p17 deleteSchedule outputs {}."""
    name = f"s-{uuid.uuid4().hex[:8]}"
    _output(run_task("createSchedule", _schedule(name, group, ActionAfterCompletion="DELETE")))
    updated = _output(run_task("updateSchedule", _schedule(
        name, group, State="DISABLED",
        FlexibleTimeWindow={"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 5})))
    assert updated == {"ScheduleArn": f"arn:aws:scheduler:us-east-1:000000000000:schedule/{group}/{name}"}
    got = scheduler.get_schedule(Name=name, GroupName=group)
    assert got["State"] == "DISABLED"
    assert got["FlexibleTimeWindow"] == {"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 5}
    assert got["ActionAfterCompletion"] == "NONE"

    assert _output(run_task("deleteSchedule", {"Name": name, "GroupName": group})) == {}
    with pytest.raises(scheduler.exceptions.ResourceNotFoundException):
        scheduler.get_schedule(Name=name, GroupName=group)


def test_schedule_timestamps_round_trip(run_task, scheduler, group):
    """r2 q06/q07: StartDate and EndDate are ISO-8601 strings in Parameters and in the
    getSchedule output (milliseconds only when non-zero); q08: an offset is read as that instant."""
    name, other = f"s-{uuid.uuid4().hex[:8]}", f"s-{uuid.uuid4().hex[:8]}"
    _output(run_task("createSchedule", _schedule(
        name, group, ScheduleExpression="rate(1 day)", Description="round2",
        StartDate="2030-01-02T03:04:05Z", EndDate="2030-01-06T03:04:05.000Z")))
    direct = scheduler.get_schedule(Name=name, GroupName=group)
    assert direct["StartDate"] == datetime.datetime(2030, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    assert direct["EndDate"] == datetime.datetime(2030, 1, 6, 3, 4, 5, tzinfo=datetime.timezone.utc)

    got = _output(run_task("getSchedule", {"Name": name, "GroupName": group}))
    assert got["StartDate"] == "2030-01-02T03:04:05Z"
    assert got["EndDate"] == "2030-01-06T03:04:05Z"
    assert got["Description"] == "round2"

    _output(run_task("createSchedule", _schedule(
        other, group, ScheduleExpression="rate(1 day)", StartDate="2030-01-02T05:04:05+02:00")))
    direct = scheduler.get_schedule(Name=other, GroupName=group)
    assert direct["StartDate"] == datetime.datetime(2030, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)


@pytest.mark.parametrize("start, millis", [
    ("2030-01-02T03:04:05.500Z", 500),     # r3 t11
    ("2030-01-02T03:04:05.5Z", 500),       # r3 t12 (and t08 from a .$ path)
    ("2030-01-02T03:04:05.123456Z", 123),  # r3 t10: cut to milliseconds
    ("2030-01-02T03:04:05.1239Z", 123),    # r4 u01: cut, not rounded (literal; same via path, t08)
    ("2030-01-02T03:04:05.9999Z", 999),    # r4 u02 (literal; same via path, t08)
])
def test_schedule_start_date_fractions(run_task, scheduler, group, start, millis):
    """r3 t08/t10/t11/t12, r4 u01/u02: fractional seconds of any length are read and cut to milliseconds;
    t14: getSchedule outputs all three digits (.500Z)."""
    name = f"s-{uuid.uuid4().hex[:8]}"
    _output(run_task("createSchedule", {**_schedule(name, group, ScheduleExpression="rate(1 day)"),
                                        "StartDate.$": "$.start"}, sm_input={"start": start}))
    direct = scheduler.get_schedule(Name=name, GroupName=group)
    assert direct["StartDate"] == datetime.datetime(
        2030, 1, 2, 3, 4, 5, millis * 1000, tzinfo=datetime.timezone.utc)
    got = _output(run_task("getSchedule", {"Name": name, "GroupName": group}))
    assert got["StartDate"] == f"2030-01-02T03:04:05.{millis:03d}Z"


def test_target_input_object_is_sent_as_compact_json(run_task, scheduler, group):
    """r2 q11 (and r1 p13): Target.Input from a JSON object path is stored as compact JSON text."""
    name = f"s-{uuid.uuid4().hex[:8]}"
    _output(run_task("createSchedule", {
        "Name": name, "GroupName": group, "ScheduleExpression": AT,
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "Target": {"Arn": BUS, "RoleArn": TARGET_ROLE, "Input.$": "$.payload"},
    }, sm_input={"payload": {"run": "x", "n": 1}}))
    assert scheduler.get_schedule(Name=name, GroupName=group)["Target"]["Input"] == '{"run":"x","n":1}'


@pytest.mark.parametrize("value, stored", [
    ([1, "a", {"b": 2}], '[1,"a",{"b":2}]'),     # r3 t02
    ({"k": "zażółć €"}, '{"k":"zażółć €"}'),     # r3 t04: non-ASCII text is kept
])
def test_target_input_array_and_unicode(run_task, scheduler, group, value, stored):
    """r3 t02/t04: a JSON array or object as Target.Input is stored as compact JSON text."""
    name = f"s-{uuid.uuid4().hex[:8]}"
    _output(run_task("createSchedule", {
        "Name": name, "GroupName": group, "ScheduleExpression": AT,
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "Target": {"Arn": BUS, "RoleArn": TARGET_ROLE, "Input.$": "$.v"},
    }, sm_input={"v": value}))
    assert scheduler.get_schedule(Name=name, GroupName=group)["Target"]["Input"] == stored


def test_schedule_task_errors(run_task, scheduler, group):
    """Error name, message and cause suffix of failed scheduler tasks:
    r1 p09 duplicate, p10 get missing, p06 get without GroupName, p15 update missing,
    p18 delete missing; r2 q05 create in a missing group."""
    name = f"s-{uuid.uuid4().hex[:8]}"
    missing = f"s-{uuid.uuid4().hex[:8]}"
    nogroup = f"g-{uuid.uuid4().hex[:8]}"
    _output(run_task("createSchedule", _schedule(name, group)))

    cases = [
        (run_task("createSchedule", _schedule(name, group)),
         ("Scheduler.ConflictException", f"Schedule {name} already exists.", 409)),
        (run_task("getSchedule", {"Name": missing, "GroupName": group}),
         ("Scheduler.ResourceNotFoundException", f"Schedule {missing} does not exist.", 404)),
        (run_task("getSchedule", {"Name": name}),
         ("Scheduler.ResourceNotFoundException", f"Schedule {name} does not exist.", 404)),
        (run_task("updateSchedule", _schedule(missing, group)),
         ("Scheduler.ResourceNotFoundException", f"Schedule {missing} does not exist.", 404)),
        (run_task("deleteSchedule", {"Name": missing, "GroupName": group}),
         ("Scheduler.ResourceNotFoundException", f"Schedule {missing} does not exist.", 404)),
        (run_task("createSchedule", _schedule(missing, nogroup)),
         ("Scheduler.ResourceNotFoundException", f"Schedule group {nogroup} does not exist.", 404)),
    ]
    for resp, expected in cases:
        assert _failure(resp) == expected


def test_schedule_group_task_errors(run_task, group):
    """r2 q02 duplicate group, q03 getScheduleGroup missing, q04 deleteScheduleGroup missing."""
    nogroup = f"g-{uuid.uuid4().hex[:8]}"
    assert _failure(run_task("createScheduleGroup", {"Name": group})) == (
        "Scheduler.ConflictException", f"Schedule group {group} already exists.", 409)
    for action in ("getScheduleGroup", "deleteScheduleGroup"):
        assert _failure(run_task(action, {"Name": nogroup})) == (
            "Scheduler.ResourceNotFoundException", f"Schedule group {nogroup} does not exist.", 404)


def test_scheduler_task_error_can_be_caught(sfn_sync, group):
    """The r1 p09 error name, Scheduler.ConflictException, matches a Catch on that name.

    The Catch itself is generic Step Functions behaviour (inference; not captured for scheduler).
    """
    name = f"s-{uuid.uuid4().hex[:8]}"
    create = {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:scheduler:createSchedule",
              "Parameters": _schedule(name, group)}
    definition = {"StartAt": "First", "States": {
        "First": {**create, "ResultPath": None, "Next": "Again"},
        "Again": {**create, "Catch": [{"ErrorEquals": ["Scheduler.ConflictException"],
                                       "ResultPath": "$.error", "Next": "Caught"}], "End": True},
        "Caught": {"Type": "Pass", "End": True},
    }}
    arn = sfn_sync.create_state_machine(name=f"sdk-scheduler-{uuid.uuid4().hex[:8]}",
                                        definition=json.dumps(definition), roleArn=ROLE)["stateMachineArn"]
    try:
        resp = sfn_sync.start_sync_execution(stateMachineArn=arn, input="{}")
        assert resp["status"] == "SUCCEEDED", resp.get("cause")
        assert json.loads(resp["output"])["error"]["Error"] == "Scheduler.ConflictException"
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=arn)
