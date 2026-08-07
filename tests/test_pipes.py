import asyncio
import json
from urllib.parse import quote

import pytest

from ministack.core.responses import (
    _request_region,
    set_request_account_id,
    set_request_region,
)
from ministack.services import dynamodb as _ddb
from ministack.services import pipes as _pipes


def _stream_arn(region, table_name="PipeTable"):
    return (
        f"arn:aws:dynamodb:{region}:000000000000:"
        f"table/{table_name}/stream/2026-05-22T00:00:00.000"
    )


def _topic_arn(region, topic_name="PipeTopic"):
    return f"arn:aws:sns:{region}:000000000000:{topic_name}"


def test_register_pipe_rejects_cross_region_target(monkeypatch):
    _pipes.reset()
    monkeypatch.setattr(_pipes, "_ensure_poller", lambda: None)
    region_token = _request_region.set("us-east-1")
    try:
        with pytest.raises(ValueError) as exc:
            _pipes.register_pipe(
                name="CrossRegionTargetPipe",
                source=_stream_arn("us-east-1"),
                target=_topic_arn("us-west-2"),
                role_arn="arn:aws:iam::000000000000:role/test-pipe-role",
            )

        assert str(exc.value) == _pipes.CROSS_REGION_PIPE_ERROR
        assert _pipes._pipes.get("CrossRegionTargetPipe") is None
        assert not _pipes._positions.has_any()
    finally:
        _request_region.reset(region_token)
        _pipes.reset()


def test_register_pipe_rejects_cross_region_source(monkeypatch):
    _pipes.reset()
    monkeypatch.setattr(_pipes, "_ensure_poller", lambda: None)
    region_token = _request_region.set("us-east-1")
    try:
        with pytest.raises(ValueError) as exc:
            _pipes.register_pipe(
                name="CrossRegionSourcePipe",
                source=_stream_arn("us-west-2"),
                target=_topic_arn("us-east-1"),
                role_arn="arn:aws:iam::000000000000:role/test-pipe-role",
            )

        assert str(exc.value) == _pipes.CROSS_REGION_PIPE_ERROR
        assert _pipes._pipes.get("CrossRegionSourcePipe") is None
        assert not _pipes._positions.has_any()
    finally:
        _request_region.reset(region_token)
        _pipes.reset()


def test_register_pipe_allows_same_region_components(monkeypatch):
    _pipes.reset()
    monkeypatch.setattr(_pipes, "_ensure_poller", lambda: None)
    region_token = _request_region.set("us-east-1")
    try:
        pipe = _pipes.register_pipe(
            name="SameRegionPipe",
            source=_stream_arn("us-east-1"),
            target=_topic_arn("us-east-1"),
            role_arn="arn:aws:iam::000000000000:role/test-pipe-role",
        )

        assert pipe["Arn"] == "arn:aws:pipes:us-east-1:000000000000:pipe/SameRegionPipe"
        assert _pipes._pipes.get("SameRegionPipe") == pipe
        assert _pipes._positions.get(pipe["Arn"]) == 0
    finally:
        _request_region.reset(region_token)
        _pipes.reset()


def test_register_pipe_region_guard_ignores_malformed_or_global_arns(monkeypatch):
    _pipes.reset()
    monkeypatch.setattr(_pipes, "_ensure_poller", lambda: None)
    region_token = _request_region.set("us-east-1")
    try:
        pipe = _pipes.register_pipe(
            name="GlobalOrMalformedPipe",
            source="arn:aws:s3:::pipe-source-bucket/key",
            target="not-an-arn",
            role_arn="arn:aws:iam::000000000000:role/test-pipe-role",
        )

        assert _pipes._pipes.get("GlobalOrMalformedPipe") == pipe
    finally:
        _request_region.reset(region_token)
        _pipes.reset()


def test_pipes_stream_table_name_parser_requires_dynamodb_stream_arn():
    stream_arn = (
        "arn:aws:dynamodb:us-east-1:000000000000:"
        "table/PipeTable/stream/2026-05-22T00:00:00.000"
    )
    assert _pipes._table_name_from_stream_arn(stream_arn) == "PipeTable"
    assert _pipes._table_name_from_stream_arn("not-an-arn") == ""

    wrong_service_arn = (
        "arn:aws:sns:us-east-1:000000000000:"
        "table/PipeTable/stream/2026-05-22T00:00:00.000"
    )
    missing_stream_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/PipeTable"
    assert _pipes._table_name_from_stream_arn(wrong_service_arn) == ""
    assert _pipes._table_name_from_stream_arn(missing_stream_arn) == ""


def test_pipes_dynamodb_stream_reads_use_source_arn_region(monkeypatch):
    _pipes.reset()
    _ddb._stream_records.clear()
    region_token = _request_region.set("us-east-1")
    try:
        table_name = "PipeTable"
        source_region = "us-west-2"
        source_arn = (
            f"arn:aws:dynamodb:{source_region}:000000000000:"
            f"table/{table_name}/stream/2026-05-22T00:00:00.000"
        )
        target_arn = "arn:aws:sns:us-east-1:000000000000:PipeTopic"
        pipe_arn = "arn:aws:pipes:us-east-1:000000000000:pipe/PipeName"
        record = {"eventID": "evt-1", "eventSource": "aws:dynamodb"}

        _ddb._stream_records.set_scoped(
            "000000000000", source_region, table_name, [record]
        )
        _pipes._pipes["PipeName"] = {
            "Name": "PipeName",
            "Arn": pipe_arn,
            "Source": source_arn,
            "Target": target_arn,
            "CurrentState": "RUNNING",
            "StartingPosition": "TRIM_HORIZON",
        }
        _pipes._positions[pipe_arn] = 0

        assert _pipes._initial_position({
            "Source": source_arn,
            "StartingPosition": "LATEST",
        }) == 1
        assert _pipes._initial_position({
            "Source": source_arn,
            "StartingPosition": "TRIM_HORIZON",
        }) == 0

        delivered = []

        def _record_publish(topic_arn, pipe, published_record):
            delivered.append((topic_arn, pipe["Arn"], published_record))

        monkeypatch.setattr(_pipes, "_publish_record_to_sns", _record_publish)

        _pipes._poll_once()

        assert delivered == [(target_arn, pipe_arn, record)]
        assert _pipes._positions[pipe_arn] == 1
    finally:
        _request_region.reset(region_token)
        _pipes.reset()
        _ddb._stream_records.clear()


def test_pipes_dynamodb_stream_read_rejects_cross_account_source(monkeypatch):
    _pipes.reset()
    _ddb._stream_records.clear()
    try:
        table_name = "PipeTable"
        source_arn = (
            "arn:aws:dynamodb:us-west-2:111111111111:"
            f"table/{table_name}/stream/2026-05-22T00:00:00.000"
        )
        target_arn = "arn:aws:sns:us-east-1:000000000000:PipeTopic"
        pipe_arn = "arn:aws:pipes:us-east-1:000000000000:pipe/PipeName"
        record = {"eventID": "evt-1", "eventSource": "aws:dynamodb"}

        _ddb._stream_records.set_scoped("111111111111", "us-west-2", table_name, [record])
        _pipes._pipes["PipeName"] = {
            "Name": "PipeName",
            "Arn": pipe_arn,
            "Source": source_arn,
            "Target": target_arn,
            "CurrentState": "RUNNING",
            "StartingPosition": "TRIM_HORIZON",
        }
        _pipes._positions[pipe_arn] = 0

        delivered = []
        monkeypatch.setattr(
            _pipes,
            "_publish_record_to_sns",
            lambda topic_arn, pipe, published_record: delivered.append(published_record),
        )

        assert _pipes._initial_position(_pipes._pipes["PipeName"]) == 0
        _pipes._poll_once()

        assert delivered == []
        assert _pipes._positions[pipe_arn] == 0
    finally:
        _pipes.reset()
        _ddb._stream_records.clear()


def test_pipes_poller_processes_same_name_pipes_in_each_region(monkeypatch):
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    table_name = "SamePipeTable"

    _pipes.reset()
    _ddb._stream_records.clear()
    try:
        set_request_account_id(account_id)
        for region, label in (("us-east-1", "east"), ("us-west-2", "west")):
            source_arn = (
                f"arn:aws:dynamodb:{region}:{account_id}:"
                f"table/{table_name}/stream/2026-05-22T00:00:00.000"
            )
            target_arn = f"arn:aws:sns:{region}:{account_id}:PipeTopic"
            pipe_arn = f"arn:aws:pipes:{region}:{account_id}:pipe/SamePipe"
            record = {
                "eventID": f"evt-{label}",
                "eventSource": "aws:dynamodb",
            }

            _ddb._stream_records.set_scoped(account_id, region, table_name, [record])
            _pipes._pipes.set_scoped(
                account_id,
                region,
                "SamePipe",
                {
                    "Name": "SamePipe",
                    "Arn": pipe_arn,
                    "Source": source_arn,
                    "Target": target_arn,
                    "CurrentState": "RUNNING",
                    "StartingPosition": "TRIM_HORIZON",
                },
            )
            _pipes._positions.set_scoped(account_id, region, pipe_arn, 0)

        set_request_region("eu-central-1")
        delivered = []
        monkeypatch.setattr(
            _pipes,
            "_publish_record_to_sns",
            lambda topic_arn, pipe, record: delivered.append(
                (topic_arn, pipe["Arn"], record["eventID"])
            ),
        )

        _pipes._poll_once()

        assert sorted(delivered) == [
            (
                f"arn:aws:sns:us-east-1:{account_id}:PipeTopic",
                f"arn:aws:pipes:us-east-1:{account_id}:pipe/SamePipe",
                "evt-east",
            ),
            (
                f"arn:aws:sns:us-west-2:{account_id}:PipeTopic",
                f"arn:aws:pipes:us-west-2:{account_id}:pipe/SamePipe",
                "evt-west",
            ),
        ]
        assert _pipes._positions.get_scoped(
            account_id,
            "us-east-1",
            f"arn:aws:pipes:us-east-1:{account_id}:pipe/SamePipe",
        ) == 1
        assert _pipes._positions.get_scoped(
            account_id,
            "us-west-2",
            f"arn:aws:pipes:us-west-2:{account_id}:pipe/SamePipe",
        ) == 1
    finally:
        _pipes.reset()
        _ddb._stream_records.clear()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_pipes_restore_legacy_account_scoped_state_uses_pipe_arn_region():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    pipe_arn = f"arn:aws:pipes:us-west-2:{account_id}:pipe/LegacyPipe"

    legacy_pipes = AccountScopedDict()
    legacy_pipes._data[(account_id, "LegacyPipe")] = {
        "Name": "LegacyPipe",
        "Arn": pipe_arn,
        "Source": (
            f"arn:aws:dynamodb:us-west-2:{account_id}:"
            "table/LegacyPipeTable/stream/2026-05-22T00:00:00.000"
        ),
        "Target": f"arn:aws:sns:us-west-2:{account_id}:PipeTopic",
        "CurrentState": "STOPPED",
        "StartingPosition": "LATEST",
    }
    legacy_positions = AccountScopedDict()
    legacy_positions._data[(account_id, pipe_arn)] = 7

    _pipes.reset()
    try:
        set_request_account_id(account_id)
        set_request_region("us-east-1")

        _pipes.restore_state({
            "pipes": legacy_pipes,
            "positions": legacy_positions,
        })

        assert _pipes._pipes.get_scoped(account_id, "us-east-1", "LegacyPipe") is None
        assert _pipes._pipes.get_scoped(account_id, "us-west-2", "LegacyPipe")[
            "Arn"
        ] == pipe_arn
        assert _pipes._positions.get_scoped(account_id, "us-west-2", pipe_arn) == 7
    finally:
        _pipes.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_pipes_position_is_absolute_across_record_expiry(monkeypatch):
    """A pipe's read position is an absolute stream position: records aging off
    the front of the stream must not shift where the pipe resumes."""
    import time

    from ministack.core.responses import set_request_account_id

    _pipes.reset()
    _ddb._stream_records.clear()
    _ddb._stream_trimmed.clear()
    region_token = _request_region.set("us-east-1")
    try:
        set_request_account_id("000000000000")
        table_name = "PipeTable"
        source_arn = _stream_arn("us-east-1")
        target_arn = _topic_arn("us-east-1")
        pipe_arn = "arn:aws:pipes:us-east-1:000000000000:pipe/PipeName"

        now = time.time()
        expired = now - _ddb._STREAM_RETENTION_SECONDS - 60

        def _record(seq, created_at):
            return {
                "eventID": f"evt-{seq}",
                "eventSource": "aws:dynamodb",
                "dynamodb": {"ApproximateCreationDateTime": int(created_at)},
            }

        _ddb._stream_records[table_name] = [
            _record(0, expired), _record(1, expired), _record(2, now),
        ]
        _pipes._pipes["PipeName"] = {
            "Name": "PipeName",
            "Arn": pipe_arn,
            "Source": source_arn,
            "Target": target_arn,
            "CurrentState": "RUNNING",
            "StartingPosition": "TRIM_HORIZON",
        }
        # The pipe had consumed the first record before the other two expired.
        _pipes._positions[pipe_arn] = 1

        delivered = []
        monkeypatch.setattr(
            _pipes, "_publish_record_to_sns",
            lambda _topic, _pipe, record: delivered.append(record["eventID"]),
        )

        _pipes._poll_once()

        # Resumes at the trim horizon; the surviving record is delivered once.
        assert delivered == ["evt-2"]
        assert _pipes._positions[pipe_arn] == 3
        _pipes._poll_once()
        assert delivered == ["evt-2"]

        # A pipe starting now anchors past everything, trimmed records included.
        assert _pipes._initial_position({
            "Source": source_arn, "StartingPosition": "LATEST",
        }) == 3
        assert _pipes._initial_position({
            "Source": source_arn, "StartingPosition": "TRIM_HORIZON",
        }) == 2
    finally:
        _request_region.reset(region_token)
        _pipes.reset()
        _ddb._stream_records.clear()
        _ddb._stream_trimmed.clear()


# ---------------------------------------------------------------------------
# REST/JSON handler (handle_request) — SDK surface
# ---------------------------------------------------------------------------

ROLE_ARN = "arn:aws:iam::000000000000:role/test-pipe-role"


@pytest.fixture
def handler_env(monkeypatch):
    _pipes.reset()
    monkeypatch.setattr(_pipes, "_ensure_poller", lambda: None)
    # register_pipe computes an initial stream position from DynamoDB; keep it
    # deterministic and decoupled from the DDB store for handler tests.
    monkeypatch.setattr(_pipes, "_initial_position", lambda pipe: 0)
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    yield
    _pipes.reset()


def _req(method, path, body=None, query=None):
    payload = json.dumps(body or {}).encode() if body is not None else b""
    return asyncio.run(_pipes.handle_request(method, path, {}, payload, query or {}))


def _body(resp):
    return json.loads(resp[2].decode())


def _create(name, desired="RUNNING", **extra):
    body = {
        "Source": _stream_arn("us-east-1"),
        "Target": _topic_arn("us-east-1"),
        "RoleArn": ROLE_ARN,
        "DesiredState": desired,
    }
    body.update(extra)
    return _req("POST", f"/v1/pipes/{name}", body)


def test_create_pipe_returns_lifecycle_shape(handler_env):
    status, _hdrs, _b = _create("p1")
    body = _body((status, _hdrs, _b))
    assert status == 200
    assert body["Arn"] == "arn:aws:pipes:us-east-1:000000000000:pipe/p1"
    assert body["Name"] == "p1"
    assert body["DesiredState"] == "RUNNING"
    assert body["CurrentState"] == "RUNNING"
    assert isinstance(body["CreationTime"], int)
    assert isinstance(body["LastModifiedTime"], int)
    # CreatePipeResponse must NOT carry the full describe shape.
    assert "Source" not in body and "Tags" not in body


def test_create_pipe_reuses_register_pipe(handler_env):
    _create("p1", Tags={"env": "test"}, Description="hi")
    rec = _pipes._pipes.get("p1")
    assert rec is not None
    assert rec["Tags"] == {"env": "test"}
    assert rec["Description"] == "hi"
    assert rec["RoleArn"] == ROLE_ARN


def test_create_pipe_conflict(handler_env):
    _create("p1")
    status, _hdrs, b = _create("p1")
    assert status == 409
    assert _body((status, _hdrs, b))["__type"] == "ConflictException"


def test_list_pipes_summary_shape(handler_env):
    _create("alpha")
    _create("beta", desired="STOPPED")
    status, _hdrs, b = _req("GET", "/v1/pipes")
    assert status == 200
    body = _body((status, _hdrs, b))
    names = [p["Name"] for p in body["Pipes"]]
    assert names == ["alpha", "beta"]
    summary = body["Pipes"][0]
    assert set(summary) >= {
        "Name", "Arn", "DesiredState", "CurrentState", "StateReason",
        "CreationTime", "LastModifiedTime", "Source", "Target",
    }
    assert isinstance(summary["CreationTime"], int)


def test_list_pipes_filters(handler_env):
    _create("alpha")
    _create("beta", desired="STOPPED")
    body = _body(_req("GET", "/v1/pipes", query={"NamePrefix": ["al"]}))
    assert [p["Name"] for p in body["Pipes"]] == ["alpha"]
    body = _body(_req("GET", "/v1/pipes", query={"DesiredState": ["STOPPED"]}))
    assert [p["Name"] for p in body["Pipes"]] == ["beta"]
    body = _body(_req("GET", "/v1/pipes", query={"CurrentState": ["RUNNING"]}))
    assert [p["Name"] for p in body["Pipes"]] == ["alpha"]


def test_list_pipes_limit_paginates(handler_env):
    for n in ("a", "b", "c"):
        _create(n)
    body = _body(_req("GET", "/v1/pipes", query={"Limit": ["2"]}))
    assert [p["Name"] for p in body["Pipes"]] == ["a", "b"]
    assert body["NextToken"] == "c"


def test_describe_pipe_full_shape(handler_env):
    _create("p1", Description="desc", Tags={"k": "v"})
    status, _hdrs, b = _req("GET", "/v1/pipes/p1")
    assert status == 200
    body = _body((status, _hdrs, b))
    assert body["Name"] == "p1"
    assert body["Description"] == "desc"
    assert body["RoleArn"] == ROLE_ARN
    assert body["Source"] == _stream_arn("us-east-1")
    assert body["Target"] == _topic_arn("us-east-1")
    assert body["Tags"] == {"k": "v"}
    assert isinstance(body["CreationTime"], int)


def test_describe_unknown_pipe_not_found(handler_env):
    status, hdrs, b = _req("GET", "/v1/pipes/missing")
    assert status == 404
    body = _body((status, hdrs, b))
    assert body["__type"] == "NotFoundException"
    assert body["message"] == "Pipe missing does not exist."
    assert hdrs["x-amzn-errortype"] == "NotFoundException"


def test_start_stop_pipe(handler_env):
    _create("p1", desired="STOPPED")
    status, _hdrs, b = _req("POST", "/v1/pipes/p1/start")
    assert status == 200
    body = _body((status, _hdrs, b))
    assert body["DesiredState"] == "RUNNING"
    assert body["CurrentState"] == "RUNNING"
    assert _pipes._pipes.get("p1")["CurrentState"] == "RUNNING"

    body = _body(_req("POST", "/v1/pipes/p1/stop"))
    assert body["DesiredState"] == "STOPPED"
    assert _pipes._pipes.get("p1")["CurrentState"] == "STOPPED"


def test_start_unknown_pipe_not_found(handler_env):
    status, _hdrs, b = _req("POST", "/v1/pipes/nope/start")
    assert status == 404
    assert _body((status, _hdrs, b))["__type"] == "NotFoundException"


def test_update_pipe(handler_env):
    _create("p1")
    status, _hdrs, b = _req(
        "PUT", "/v1/pipes/p1", body={"RoleArn": ROLE_ARN, "Description": "new", "DesiredState": "STOPPED"}
    )
    assert status == 200
    assert _body((status, _hdrs, b))["DesiredState"] == "STOPPED"
    rec = _pipes._pipes.get("p1")
    assert rec["Description"] == "new"
    assert rec["CurrentState"] == "STOPPED"


def test_delete_pipe(handler_env):
    _create("p1")
    status, _hdrs, b = _req("DELETE", "/v1/pipes/p1")
    assert status == 200
    body = _body((status, _hdrs, b))
    assert body["DesiredState"] == "DELETED"
    assert body["CurrentState"] == "DELETING"
    assert _pipes._pipes.get("p1") is None


def test_delete_unknown_pipe_not_found(handler_env):
    status, _hdrs, b = _req("DELETE", "/v1/pipes/nope")
    assert status == 404
    assert _body((status, _hdrs, b))["__type"] == "NotFoundException"


def test_lifecycle_create_list_describe_start_stop_delete(handler_env):
    _create("flow", desired="STOPPED")
    assert [p["Name"] for p in _body(_req("GET", "/v1/pipes"))["Pipes"]] == ["flow"]
    assert _body(_req("GET", "/v1/pipes/flow"))["Name"] == "flow"
    assert _body(_req("POST", "/v1/pipes/flow/start"))["CurrentState"] == "RUNNING"
    assert _body(_req("POST", "/v1/pipes/flow/stop"))["CurrentState"] == "STOPPED"
    assert _body(_req("DELETE", "/v1/pipes/flow"))["CurrentState"] == "DELETING"
    assert _body(_req("GET", "/v1/pipes"))["Pipes"] == []


def test_tag_untag_list_tags(handler_env):
    _create("p1", Tags={"a": "1"})
    arn = "arn:aws:pipes:us-east-1:000000000000:pipe/p1"
    enc = quote(arn, safe="")
    body = _body(_req("GET", f"/tags/{enc}"))
    assert body["tags"] == {"a": "1"}

    status, _hdrs, _b = _req("POST", f"/tags/{enc}", body={"tags": {"b": "2"}})
    assert status == 200
    assert _pipes._pipes.get("p1")["Tags"] == {"a": "1", "b": "2"}

    _req("DELETE", f"/tags/{enc}", query={"tagKeys": ["a"]})
    assert _pipes._pipes.get("p1")["Tags"] == {"b": "2"}


def test_list_tags_unknown_arn_not_found(handler_env):
    enc = quote("arn:aws:pipes:us-east-1:000000000000:pipe/ghost", safe="")
    status, _hdrs, b = _req("GET", f"/tags/{enc}")
    assert status == 404
    assert _body((status, _hdrs, b))["__type"] == "NotFoundException"
