"""DescribeEvents wire contract checked with the botocore Query parser."""
import time
from types import SimpleNamespace

import botocore.session
import pytest
from botocore.parsers import QueryParser

from ministack.services import rds


@pytest.fixture(autouse=True)
def clean_events():
    saved = dict(rds._events.items())
    for key in list(rds._events):
        del rds._events[key]
    yield
    for key in list(rds._events):
        del rds._events[key]
    rds._events.update(saved)


def describe(**params):
    status, headers, body = rds._describe_events(params)
    shape = botocore.session.get_session().get_service_model("rds").operation_model("DescribeEvents").output_shape
    return status, QueryParser().parse({"status_code": status, "headers": headers, "body": body}, shape)


def record(name="example"):
    rds._record_reboot_event({"DBInstanceIdentifier": name, "DBInstanceArn": f"arn:aws:rds:us-east-1:000000000000:db:{name}"})


def test_reboot_event_wire_shape_and_filters():
    record("example")
    record("other")
    status, result = describe(SourceIdentifier="example", SourceType="db-instance")
    assert status == 200
    event, = result["Events"]
    assert event["Message"] == "DB instance restarted"
    assert event["EventCategories"] == ["availability"]
    assert event["SourceArn"].endswith(":db:example")
    assert abs(event["Date"].timestamp() - time.time()) < 2
    assert describe(SourceType="db-cluster")[1]["Events"] == []
    assert describe(**{"EventCategories.member.1": "failure"})[1]["Events"] == []
    assert describe(StartTime="2100-01-01T00:00:00Z", EndTime="2100-01-02T00:00:00Z")[1]["Events"] == []


def test_pagination_has_no_duplicates():
    for i in range(23):
        record(str(i))
    _, first = describe(MaxRecords="20")
    _, second = describe(MaxRecords="20", Marker=first["Marker"])
    assert len(first["Events"]) == 20
    assert len(second["Events"]) == 3
    assert "Marker" not in second
    assert len({e["SourceIdentifier"] for e in first["Events"] + second["Events"]}) == 23


@pytest.mark.parametrize("params", [{"SourceIdentifier": "x"}, {"MaxRecords": "19"}, {"MaxRecords": "101"}, {"Duration": "bad"}, {"Marker": "missing"}, {"StartTime": "bad"}])
def test_invalid_requests(params):
    assert describe(**params)[0] == 400


def test_events_survive_persistence_and_are_region_scoped():
    from ministack.core.responses import get_region, set_request_region
    region = get_region()
    record()
    snapshot = rds.get_state()
    try:
        set_request_region("eu-north-1" if region != "eu-north-1" else "us-east-2")
        assert describe()[1]["Events"] == []
    finally:
        set_request_region(region)
    for key in list(rds._events):
        del rds._events[key]
    rds.load_persisted_state(snapshot)
    assert len(describe()[1]["Events"]) == 1


def test_completion_only_after_success(monkeypatch):
    instance = {"DBInstanceIdentifier": "event-reboot", "DBInstanceStatus": "rebooting"}
    rds._instances["event-reboot"] = instance
    try:
        monkeypatch.setattr(rds, "_get_docker", lambda: None)
        rds._finish_instance_reboot(instance, None, 0)
        assert len(describe()[1]["Events"]) == 1
        instance["DBInstanceStatus"] = "rebooting"
        instance["_docker_container_id"] = "missing"
        def fail(_):
            raise RuntimeError("missing container")
        monkeypatch.setattr(rds, "_get_docker", lambda: SimpleNamespace(containers=SimpleNamespace(get=fail)))
        rds._finish_instance_reboot(instance, None, 0)
        assert len(describe()[1]["Events"]) == 1
        assert instance["DBInstanceStatus"] == "failed"
    finally:
        rds._instances.pop("event-reboot", None)
