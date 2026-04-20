"""Compatibility shims for the hypercorn/h11 stack.

h11 serialises ``InformationalResponse`` with an empty reason phrase by
default, producing ``HTTP/1.1 100 \\r\\n`` on the wire. boto3 < 1.40's
bundled urllib3 parses this strictly and aborts with ``BadStatusLine``
when the client is waiting on ``Expect: 100-continue`` (e.g. S3
``upload_file``). Injecting the standard reason phrase makes the wire
output ``HTTP/1.1 100 Continue\\r\\n``, which every SDK version accepts.

Issue: https://github.com/ministackorg/ministack/issues/389
Remove this module if h11 ever ships a default reason upstream.
"""

import h11

# RFC 9110 § 15.2 informational response reason phrases.
_DEFAULT_REASONS = {
    100: b"Continue",
    101: b"Switching Protocols",
    102: b"Processing",
    103: b"Early Hints",
}

_original_post_init = h11.InformationalResponse.__post_init__


def _patched_post_init(self) -> None:
    _original_post_init(self)
    if not self.reason:
        default = _DEFAULT_REASONS.get(self.status_code)
        if default is not None:
            # Frozen dataclass — bypass normal attribute protection.
            object.__setattr__(self, "reason", default)


_patched_post_init._ministack_patched = True  # type: ignore[attr-defined]


def install() -> None:
    """Install the reason-phrase patch. Idempotent."""
    if getattr(h11.InformationalResponse.__post_init__, "_ministack_patched", False):
        return
    h11.InformationalResponse.__post_init__ = _patched_post_init
