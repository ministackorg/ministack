import asyncio
import xml.etree.ElementTree as ET

import pytest

from ministack import app as app_module


@pytest.mark.parametrize(
    ("body", "headers"),
    [
        (b"3", {"content-encoding": "aws-chunked"}),
        (b"x\r\n", {"content-encoding": "aws-chunked"}),
        (b"3\r\nab", {"content-encoding": "aws-chunked"}),
        (b"3\r\nabc\r\n", {"content-encoding": "aws-chunked"}),
        (b"3\r\nabc\r\n0\r\n", {"content-encoding": "aws-chunked"}),
        (
            b"1\r\na\r\n0\r\n\r\n",
            {"content-encoding": "aws-chunked", "x-amz-decoded-content-length": "2"},
        ),
        (
            b"1\r\na\r\n0\r\n\r\n",
            {"content-encoding": "aws-chunked", "x-amz-trailer": "x-amz-checksum-crc32"},
        ),
    ],
)
def test_decode_aws_chunked_rejects_incomplete_or_inconsistent_body(body, headers):
    with pytest.raises(app_module._IncompleteRequestBody):
        app_module._decode_aws_chunked_body(body, headers)


def test_decode_aws_chunked_accepts_complete_body_and_announced_trailer():
    body = (
        b"3;chunk-signature=ignored\r\nabc\r\n"
        b"0;chunk-signature=ignored\r\n"
        b"x-amz-checksum-crc32: value\r\n\r\n"
    )
    headers = {
        "content-encoding": "aws-chunked",
        "x-amz-decoded-content-length": "3",
        "x-amz-trailer": "x-amz-checksum-crc32",
    }

    assert app_module._decode_aws_chunked_body(body, headers) == b"abc"
    assert headers["x-amz-checksum-crc32"] == "value"


def _run_asgi_request(monkeypatch, headers, body_message):
    messages = []
    received = iter([body_message])

    async def receive():
        return next(received)

    async def send(message):
        messages.append(message)

    async def unexpected_dispatch(*_args):
        pytest.fail("incomplete request body reached service dispatch")

    monkeypatch.setattr(app_module, "_dispatch_service_request", unexpected_dispatch)
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "PUT",
        "scheme": "http",
        "path": "/bucket/key",
        "raw_path": b"/bucket/key",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"host", b"localhost:4566"), *headers],
        "client": ("127.0.0.1", 55555),
        "server": ("127.0.0.1", 4566),
    }

    asyncio.run(app_module.app(scope, receive, send))
    return messages


@pytest.mark.parametrize(
    ("headers", "body"),
    [
        ([(b"content-length", b"5")], b"abc"),
        ([(b"content-encoding", b"aws-chunked")], b"3\r\nab"),
    ],
)
def test_incomplete_s3_body_returns_incomplete_body_before_dispatch(monkeypatch, headers, body):
    messages = _run_asgi_request(
        monkeypatch,
        headers,
        {"type": "http.request", "body": body, "more_body": False},
    )
    response = next(message for message in messages if message["type"] == "http.response.start")
    response_body = next(message["body"] for message in messages if message["type"] == "http.response.body")

    assert response["status"] == 400
    assert ET.fromstring(response_body).findtext("Code") == "IncompleteBody"


def test_s3_body_disconnect_stops_before_dispatch(monkeypatch):
    messages = _run_asgi_request(monkeypatch, [], {"type": "http.disconnect"})

    assert messages == []