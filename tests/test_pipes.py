import pytest

from ministack.core.responses import _request_region
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
