import asyncio
import json
import uuid

import pytest

import ministack.services.scheduler as scheduler
from ministack.core.responses import set_request_account_id, set_request_region


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture(autouse=True)
def _reset_scheduler():
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    scheduler.reset()
    yield
    scheduler.reset()


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
