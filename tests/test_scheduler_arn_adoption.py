import asyncio
import json
import uuid

import pytest

import ministack.services.scheduler as scheduler
from ministack.core.responses import (
    get_account_id,
    get_region,
    set_request_account_id,
    set_request_region,
)


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture(autouse=True)
def _reset_scheduler():
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    scheduler.reset()
    scheduler._schedule_last_fired.clear()
    scheduler._ticker_thread = None
    yield
    scheduler.reset()
    scheduler._schedule_last_fired.clear()
    scheduler._ticker_thread = None


def _request(method, path, body=None, query=None):
    payload = json.dumps(body or {}).encode()
    return asyncio.run(
        scheduler.handle_request(method, path, {}, payload, query or {})
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
    assert f"default/{name}" in scheduler._schedules


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
        assert scheduler._tags.get(arn) is None

    assert scheduler._tags.get(west_schedule_arn) is None
    assert scheduler._tags.get(west_group_arn) is None


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
        scheduler._schedules.set_scoped(
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
    monkeypatch.setattr(scheduler.time, "time", lambda: 2.0)

    scheduler._tick_schedules()

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

    monkeypatch.setattr(scheduler.threading, "Thread", _FakeThread)

    scheduler.start_scheduler()
    scheduler.start_scheduler()

    assert len(created_threads) == 1
    thread = created_threads[0]
    assert thread.target is scheduler._ticker_loop
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
    baseline = dict(scheduler._tags.get(good_arn, {}))
    arn = bad_arn.format(name=name)

    body = {"Tags": [{"Key": "env", "Value": "bad"}]} if method == "POST" else None
    query = {"TagKeys": ["env"]} if method == "DELETE" else None
    resp = _request(method, f"/tags/{arn}", body=body, query=query)

    _assert_error(resp, expected_status, expected_code)
    assert scheduler._tags.get(good_arn, {}) == baseline
    assert scheduler._tags.get(arn) is None
