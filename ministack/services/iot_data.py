"""IoT Data Plane HTTP API (``iot-data`` AWS service).

Implements the HTTP-side of the AWS IoT Data Plane. The data plane mirrors
the broker over a small REST surface (``Publish`` is the only Phase 1b
operation; ``GetRetainedMessage`` / ``ListRetainedMessages`` are Phase 2).

Routing reaches us either through credential-scope detection (the SDK signs
requests with the ``iotdata`` scope) or via the host pattern
``data-ats.iot.{region}.{host}`` / ``data.iot.{region}.{host}``.

Bridges into the in-memory MQTT broker via :mod:`ministack.services.iot_broker`,
which applies account-scoped topic prefixing transparently.
"""

from __future__ import annotations

import json
import logging
from urllib.parse import unquote

from ministack.core.responses import (
    error_response_json,
    get_account_id,
    json_response,
)
from ministack.services import iot_broker

logger = logging.getLogger("iot_data")


# AWS IoT topics: max 7 segments, max 256 UTF-8 bytes total.
_MAX_TOPIC_BYTES = 256


def _validate_topic(topic: str) -> tuple | None:
    """Return an error response if ``topic`` is invalid, else None."""
    if not topic:
        return error_response_json(
            "InvalidRequestException", "Topic must not be empty", 400
        )
    if "+" in topic or "#" in topic:
        return error_response_json(
            "InvalidRequestException",
            "Topic must not contain wildcard characters",
            400,
        )
    if len(topic.encode("utf-8")) > _MAX_TOPIC_BYTES:
        return error_response_json(
            "InvalidRequestException",
            f"Topic exceeds {_MAX_TOPIC_BYTES} bytes",
            400,
        )
    return None


# ---------------------------------------------------------------------------
# Persistence (no state of our own — broker holds it)
# ---------------------------------------------------------------------------


def get_state() -> dict:
    return {}


def restore_state(data: dict | None) -> None:
    return None


def reset() -> None:
    iot_broker.reset()


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


async def handle_request(
    method: str, path: str, headers: dict, body: bytes, query_params: dict
) -> tuple:
    qp = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}

    if method == "POST" and path.startswith("/topics/"):
        return await _publish(path[len("/topics/"):], body, qp)

    # Phase 2 — placeholders return 501 so SDKs that probe these endpoints
    # get a clear "not implemented" rather than a generic 404.
    if path == "/retainedMessage":
        return error_response_json(
            "InvalidRequestException",
            "ListRetainedMessages is a Phase 2 feature",
            501,
        )
    if path.startswith("/retainedMessage/"):
        return error_response_json(
            "InvalidRequestException",
            "GetRetainedMessage is a Phase 2 feature",
            501,
        )

    return error_response_json(
        "InvalidRequestException", f"Unsupported iot-data path: {method} {path}", 400
    )


async def _publish(raw_topic: str, body: bytes, qp: dict) -> tuple:
    """``POST /topics/{topic}`` — forward body to the broker.

    ``raw_topic`` is what came after ``/topics/`` in the URL. The SDK URL-
    encodes ``/`` separators so we decode here and validate before relaying.
    """
    topic = unquote(raw_topic or "")
    err = _validate_topic(topic)
    if err is not None:
        return err

    try:
        qos = int(qp.get("qos", 0))
    except (TypeError, ValueError):
        return error_response_json(
            "InvalidRequestException", "qos must be 0 or 1", 400
        )
    if qos not in (0, 1):
        return error_response_json(
            "InvalidRequestException", "qos must be 0 or 1", 400
        )
    retain = str(qp.get("retain", "")).lower() in ("1", "true", "yes")

    if not iot_broker.is_available():
        return error_response_json(
            "InternalFailureException",
            "IoT broker is not available",
            503,
        )

    try:
        await iot_broker.publish(
            get_account_id(), topic, body or b"", qos=qos, retain=retain
        )
    except Exception as e:
        logger.exception("iot-data publish failed")
        return error_response_json(
            "InternalFailureException", f"Publish failed: {e}", 503,
        )
    # AWS returns 200 with empty body for Publish.
    return 200, {"Content-Type": "application/json"}, b""
