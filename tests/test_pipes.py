from ministack.core.responses import _request_region
from ministack.services import dynamodb as _ddb
from ministack.services import pipes as _pipes


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
