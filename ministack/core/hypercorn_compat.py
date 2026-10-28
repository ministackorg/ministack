# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Compatibility shims for the hypercorn/h11 stack.

h11 serialises ``InformationalResponse`` with an empty reason phrase by
default, producing ``HTTP/1.1 100 \\r\\n`` on the wire. boto3 < 1.40's
bundled urllib3 parses this strictly and aborts with ``BadStatusLine``
when the client is waiting on ``Expect: 100-continue`` (e.g. S3
``upload_file``). Injecting the standard reason phrase makes the wire
output ``HTTP/1.1 100 Continue\\r\\n``, which every SDK version accepts.

Final responses get their standard phrase the same way: AWS endpoints answer
``HTTP/1.1 200 OK``, and a CloudFront Function reads it as ``statusDescription``.

Issue: https://github.com/ministackorg/ministack/issues/389
Remove the reason-phrase patch if h11 ever ships a default reason upstream.

On HTTP/2, Hypercorn 0.18.0 sends GOAWAY as soon as a connection passes
``keep_alive_max_requests`` (default 1000), so the streams in flight lose their
responses although the GOAWAY counts them as processed. The patch below stops
new streams at the limit and sends GOAWAY once every stream's last frame is
written. Remove it once Hypercorn fixes this itself
(https://github.com/pgjones/hypercorn/pull/377 proposes a fix).
"""

from http import HTTPStatus

import h11
from h2.settings import SettingCodes
from hypercorn.events import Closed
from hypercorn.protocol.h2 import H2Protocol


def _with_default_reason(original_post_init):
    def _patched_post_init(self) -> None:
        original_post_init(self)
        if not self.reason:
            try:
                default = HTTPStatus(self.status_code).phrase.encode()
            except ValueError:
                return
            # Frozen dataclass — bypass normal attribute protection.
            object.__setattr__(self, "reason", default)

    _patched_post_init._ministack_patched = True  # type: ignore[attr-defined]
    return _patched_post_init


def _held_at_max_requests(original_create_stream):
    async def _patched_create_stream(self, request) -> None:
        await original_create_stream(self, request)
        limit = self.config.keep_alive_max_requests
        if self.keep_alive_requests > limit:
            # Held at the limit, Hypercorn's own check never closes the connection under the streams in flight.
            self.keep_alive_requests = limit
        elif self.keep_alive_requests == limit:
            self.connection.update_settings({SettingCodes.MAX_CONCURRENT_STREAMS: 0})

    _patched_create_stream._ministack_patched = True  # type: ignore[attr-defined]
    return _patched_create_stream


def _unsent(protocol) -> bool:
    # A buffer is removed once its last DATA frame and END_STREAM are written (or sending fails); a GOAWAY before that
    # moves h2 to CLOSED and the end of the response is dropped. A stream the client has reset is never written.
    for stream_id in protocol.stream_buffers:
        stream = protocol.connection.streams.get(stream_id)
        if stream is not None and not stream.closed:
            return True
    return False


async def _goaway_once_drained(protocol) -> None:
    if (
        protocol.keep_alive_requests >= protocol.config.keep_alive_max_requests
        and not protocol.context.terminated.is_set()
        and not protocol.streams
        and not _unsent(protocol)
        and not getattr(protocol, "_ministack_goaway_sent", False)
    ):
        protocol._ministack_goaway_sent = True
        protocol.connection.close_connection()
        await protocol._flush()
        await protocol.send(Closed())


def _then_goaway_once_drained(original):
    async def _patched(self, *args) -> None:
        await original(self, *args)
        await _goaway_once_drained(self)

    _patched._ministack_patched = True  # type: ignore[attr-defined]
    return _patched


def install() -> None:
    """Install the reason-phrase and HTTP/2 max-requests patches. Idempotent."""
    for event in (h11.InformationalResponse, h11.Response):
        if not getattr(event.__post_init__, "_ministack_patched", False):
            event.__post_init__ = _with_default_reason(event.__post_init__)
    if not getattr(H2Protocol._create_stream, "_ministack_patched", False):
        H2Protocol._create_stream = _held_at_max_requests(H2Protocol._create_stream)
        H2Protocol.stream_send = _then_goaway_once_drained(H2Protocol.stream_send)
        H2Protocol._send_data = _then_goaway_once_drained(H2Protocol._send_data)
