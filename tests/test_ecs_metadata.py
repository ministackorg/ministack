"""Unit tests for the ECS Task Metadata V4 emulator (services/ecs_metadata.py).

These exercise the registry + handler directly, without a running Docker
container or boto client, since the module is reachable only from inside an
ECS task in production.
"""

import asyncio
import json

import pytest

from ministack.services import ecs_metadata


def _call(method, path):
    status, _headers, body = asyncio.run(
        ecs_metadata.handle_request(method, path, {}, b"", {})
    )
    payload = json.loads(body) if body else None
    return status, payload


@pytest.fixture(autouse=True)
def _reset_registry():
    ecs_metadata.reset()
    yield
    ecs_metadata.reset()


def _register(token, task_arn, container_name, **container_overrides):
    container = {
        "DockerId": "",
        "Name": container_name,
        "Image": "alpine:latest",
        "Labels": {"com.amazonaws.ecs.task-arn": task_arn},
        "DesiredStatus": "RUNNING",
        "KnownStatus": "RUNNING",
        "Type": "NORMAL",
    }
    container.update(container_overrides)
    ecs_metadata.register_container(
        token,
        task_arn,
        task_payload={
            "Cluster": "arn:aws:ecs:us-east-1:000000000000:cluster/c",
            "TaskARN": task_arn,
            "Family": "fam",
            "Revision": "1",
            "DesiredStatus": "RUNNING",
            "KnownStatus": "RUNNING",
            "AvailabilityZone": "us-east-1a",
            "LaunchType": "FARGATE",
        },
        container_payload=container,
    )


def test_unknown_path_returns_404():
    status, body = _call("GET", "/v4/")
    assert status == 404
    assert body["message"] == "not found"


def test_unknown_token_returns_404():
    status, body = _call("GET", "/v4/abcdefgh/task")
    assert status == 404
    assert body["message"] == "unknown token"


def test_root_returns_container_payload():
    _register("tok-aaaaaaaa", "arn:task/1", "web", DockerId="d1")
    status, body = _call("GET", "/v4/tok-aaaaaaaa")
    assert status == 200
    assert body["Name"] == "web"
    assert body["DockerId"] == "d1"


def test_task_endpoint_lists_all_sibling_containers():
    _register("tok-aaaaaaaa", "arn:task/1", "web")
    _register("tok-bbbbbbbb", "arn:task/1", "sidecar")
    _register("tok-cccccccc", "arn:task/2", "other")

    status, body = _call("GET", "/v4/tok-aaaaaaaa/task")
    assert status == 200
    names = {c["Name"] for c in body["Containers"]}
    assert names == {"web", "sidecar"}

    # Task 2 sees only its own container.
    status, body = _call("GET", "/v4/tok-cccccccc/task")
    assert status == 200
    assert [c["Name"] for c in body["Containers"]] == ["other"]


def test_set_docker_id_mutates_container_payload():
    _register("tok-aaaaaaaa", "arn:task/1", "web")
    ecs_metadata.set_docker_id("tok-aaaaaaaa", "deadbeef")
    _, body = _call("GET", "/v4/tok-aaaaaaaa")
    assert body["DockerId"] == "deadbeef"


def test_set_docker_id_unknown_token_is_noop():
    # Should not raise; just does nothing.
    ecs_metadata.set_docker_id("tok-zzzzzzzz", "x")


def test_stats_endpoints_return_empty_object():
    _register("tok-aaaaaaaa", "arn:task/1", "web")
    for path in ("/v4/tok-aaaaaaaa/stats", "/v4/tok-aaaaaaaa/task/stats"):
        status, body = _call("GET", path)
        assert status == 200
        assert body == {}


def test_unregister_removes_only_that_container():
    _register("tok-aaaaaaaa", "arn:task/1", "web")
    _register("tok-bbbbbbbb", "arn:task/1", "sidecar")

    ecs_metadata.unregister_token("tok-aaaaaaaa")

    status, _ = _call("GET", "/v4/tok-aaaaaaaa")
    assert status == 404

    status, body = _call("GET", "/v4/tok-bbbbbbbb/task")
    assert status == 200
    assert [c["Name"] for c in body["Containers"]] == ["sidecar"]


def test_unregister_last_token_drops_task_entry():
    _register("tok-aaaaaaaa", "arn:task/1", "web")
    ecs_metadata.unregister_token("tok-aaaaaaaa")
    # Internal: the task arn entry should be cleaned up so the registry
    # doesn't grow unbounded across run/stop cycles.
    assert "arn:task/1" not in ecs_metadata._TASKS


def test_unregister_unknown_token_is_noop():
    ecs_metadata.unregister_token("tok-zzzzzzzz")  # must not raise


def test_reset_clears_all_state():
    _register("tok-aaaaaaaa", "arn:task/1", "web")
    _register("tok-bbbbbbbb", "arn:task/2", "other")
    ecs_metadata.reset()
    assert ecs_metadata._TASKS == {}
    assert ecs_metadata._TOKEN_TO_TASK == {}
    assert ecs_metadata._TOKEN_TO_CONTAINER == {}


def test_path_regex_rejects_short_token():
    # Token must be at least 8 chars per the regex.
    status, _ = _call("GET", "/v4/short/task")
    assert status == 404


def test_trailing_slash_on_root_is_tolerated():
    _register("tok-aaaaaaaa", "arn:task/1", "web")
    status, body = _call("GET", "/v4/tok-aaaaaaaa/")
    assert status == 200
    assert body["Name"] == "web"


def test_status_is_pushed_onto_the_payload_as_the_task_moves():
    """The endpoint reports the task's status instead of always saying RUNNING."""
    arn = "arn:aws:ecs:us-east-1:000000000000:task/c/statusprobe01"
    _register("statusprobetoken01", arn, "probe", KnownStatus="PENDING")

    for status in ("PENDING", "ACTIVATING", "RUNNING"):
        ecs_metadata.set_task_status(arn, known_status=status)
        _, body = _call("GET", "/v4/statusprobetoken01/task")
        assert body["KnownStatus"] == status

    # On AWS a starting task serves "NONE" for itself and "RUNNING" for the
    # container reading the endpoint, in one payload.
    ecs_metadata.set_task_status(arn, known_status="ACTIVATING")
    ecs_metadata.set_container_status("statusprobetoken01", "RUNNING")
    _, body = _call("GET", "/v4/statusprobetoken01/task")
    assert body["KnownStatus"] == "ACTIVATING"
    assert body["Containers"][0]["KnownStatus"] == "RUNNING"
    assert _call("GET", "/v4/statusprobetoken01")[1]["KnownStatus"] == "RUNNING"

    # DesiredStatus is the task's on both.
    ecs_metadata.set_task_status(arn, desired_status="STOPPED")
    _, body = _call("GET", "/v4/statusprobetoken01/task")
    assert body["DesiredStatus"] == "STOPPED"
    assert body["Containers"][0]["DesiredStatus"] == "STOPPED"
    assert _call("GET", "/v4/statusprobetoken01")[1]["DesiredStatus"] == "STOPPED"
    # ... and it did not drag the container's KnownStatus with it.
    assert body["Containers"][0]["KnownStatus"] == "RUNNING"


def test_stop_pushes_every_container_to_stopped():
    """The stop path moves every container of the task at once."""
    arn = "arn:aws:ecs:us-east-1:000000000000:task/c/stopprobe01"
    _register("stopprobetoken01", arn, "web")
    _register("stopprobetoken02", arn, "sidecar")

    ecs_metadata.set_task_status(arn, known_status="STOPPED", desired_status="STOPPED")
    ecs_metadata.set_all_container_status(arn, "STOPPED")

    _, body = _call("GET", "/v4/stopprobetoken01/task")
    assert body["KnownStatus"] == "STOPPED"
    assert [c["KnownStatus"] for c in body["Containers"]] == ["STOPPED", "STOPPED"]


def test_a_push_for_an_unregistered_task_is_a_no_op():
    """A task whose tokens are already unregistered must not resurrect an entry."""
    ecs_metadata.set_task_status("arn:aws:ecs:us-east-1:000000000000:task/c/none", known_status="STOPPED")
    ecs_metadata.set_all_container_status("arn:aws:ecs:us-east-1:000000000000:task/c/none", "STOPPED")
    ecs_metadata.set_container_status("no-such-token", "STOPPED")
    assert ecs_metadata._TASKS == {}


def test_status_falls_back_to_what_was_registered_when_the_task_is_gone():
    """No task record, no overlay: the endpoint serves what was registered."""
    arn = "arn:aws:ecs:us-east-1:000000000000:task/c/goneprobe01"
    _register("goneprobetoken01", arn, "probe", KnownStatus="ACTIVATING")
    _, body = _call("GET", "/v4/goneprobetoken01/task")
    assert body["Containers"][0]["KnownStatus"] == "ACTIVATING"


def test_seeding_finds_a_task_outside_the_default_account_and_region():
    """The seed is keyed off the task ARN, not the request, which carries no SigV4."""
    from ministack.services import ecs

    account, region = "111122223333", "eu-central-1"
    arn = f"arn:aws:ecs:{region}:{account}:task/c/otherprobe01"
    ecs._tasks.set_scoped(account, region, arn, {
        "taskArn": arn,
        "desiredStatus": "RUNNING",
        "lastStatus": "ACTIVATING",
        "containers": [{"name": "probe", "lastStatus": "PENDING"}],
    })
    try:
        assert ecs._task_status_snapshot(arn) == (
            "RUNNING", "ACTIVATING", {"probe": "PENDING"},
        )
    finally:
        ecs._tasks.pop_scoped(account, region, arn, None)


def test_seeding_returns_none_for_an_unparseable_or_missing_task():
    from ministack.services import ecs

    assert ecs._task_status_snapshot("not-an-arn") is None
    assert ecs._task_status_snapshot(
        "arn:aws:ecs:us-east-1:000000000000:task/c/absent01"
    ) is None
