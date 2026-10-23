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
Remove this module if h11 ever ships a default reason upstream.
"""

from http import HTTPStatus

import h11


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


def install() -> None:
    """Install the reason-phrase patch. Idempotent."""
    for event in (h11.InformationalResponse, h11.Response):
        if not getattr(event.__post_init__, "_ministack_patched", False):
            event.__post_init__ = _with_default_reason(event.__post_init__)
