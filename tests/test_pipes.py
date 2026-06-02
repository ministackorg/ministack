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
