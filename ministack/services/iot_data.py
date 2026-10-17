# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""IoT Data Plane HTTP API (``iot-data`` AWS service).

Implements the HTTP-side of the AWS IoT Data Plane. The data plane mirrors
the broker over a small REST surface (``Publish`` is the primary
operation; ``GetRetainedMessage`` / ``ListRetainedMessages`` read the broker's
retained messages).

Routing reaches us either through credential-scope detection (the SDK signs
requests with the ``iotdata`` scope) or via the host pattern
``data-ats.iot.{region}.{host}`` / ``data.iot.{region}.{host}``.

Bridges into the in-memory MQTT broker via :mod:`ministack.services.iot`,
which applies account-and-region-scoped topic prefixing transparently.
"""

from __future__ import annotations

import base64
import copy
import json
import logging
import re
from urllib.parse import unquote

from ministack.core.responses import (
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)
from ministack.core.router import extract_access_key_id
from ministack.services import iot as _iot_module

logger = logging.getLogger("iot_data")


# AWS IoT topics: max 7 segments, max 256 UTF-8 bytes total.
_MAX_TOPIC_BYTES = 256

# ListRetainedMessages page size without maxResults, and the most it allows.
_RETAINED_PAGE_DEFAULT = 20
_RETAINED_PAGE_MAX = 200

# HTTPS Publish to a topic starting with "$" is refused unless the topic sits
# under one of these reserved families. Within them, the device-side jobs,
# Device Defender and commands topics are MQTT-only.
_HTTP_RESERVED_PREFIXES = _iot_module.RESERVED_PUBLISH_PREFIXES
# Families that take only their request topics, over HTTPS as over MQTT.
_HTTP_REQUEST_ONLY_PREFIXES = ("$aws/certificates/", "$aws/provisioning-templates/", "$aws/device_location/")
_HTTP_RESTRICTED_TOPIC_RE = re.compile(
    r"^\$aws/(things/[^/]+/(jobs|defender)/|commands/)"
)

# Publish's x-amz-mqtt5-payload-format-indicator values and the byte each one stands for.
_PAYLOAD_FORMAT_INDICATORS = {"UNSPECIFIED_BYTES": 0, "UTF8_DATA": 1}


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
    if topic.startswith("$"):
        if not topic.startswith(_HTTP_RESERVED_PREFIXES) or (
            topic.startswith(_HTTP_REQUEST_ONLY_PREFIXES)
            and _iot_module._RESERVED_REQUEST_TOPIC_RE.fullmatch(topic) is None
        ):
            return error_response_json(
                "InvalidRequestException", "Topic can't start with $", 400
            )
        if _HTTP_RESTRICTED_TOPIC_RE.match(topic):
            return error_response_json(
                "InvalidRequestException",
                "Invalid publish to restricted topic using HTTP",
                400,
            )
    return None


# ---------------------------------------------------------------------------
# Persistence (no state of our own — broker holds it)
# ---------------------------------------------------------------------------


def get_state() -> dict:
    return {}


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data: dict | None) -> None:
    return None


def reset() -> None:
    _iot_module.broker_reset()


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


async def handle_request(
    method: str, path: str, headers: dict, body: bytes, query_params: dict
) -> tuple:
    qp = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}
    # The errorAction document reports the publisher's IP; same header the
    # API Gateway integration reads.
    _iot_module._publish_source_ip.set(
        (headers.get("x-forwarded-for") or "127.0.0.1").split(",")[0].strip()
    )

    if method == "POST" and path.startswith("/topics/"):
        access_key = extract_access_key_id(headers, query_params)
        return await _publish(path[len("/topics/"):], body, qp, headers, access_key)

    # Device Shadow: /things/{thingName}/shadow[?name={shadowName}]
    if path.startswith("/things/") and path.endswith("/shadow"):
        return await _shadow(method, path, body, qp)

    if method == "GET" and path == "/retainedMessage":
        return _list_retained_messages(qp)
    if method == "GET" and path.startswith("/retainedMessage/"):
        return _get_retained_message(path[len("/retainedMessage/"):])

    return error_response_json(
        "InvalidRequestException", f"Unsupported iot-data path: {method} {path}", 400
    )


async def _shadow(method: str, path: str, body: bytes, qp: dict) -> tuple:
    """Route ``GetThingShadow`` / ``UpdateThingShadow`` / ``DeleteThingShadow``.

    An accepted update or delete also publishes the reserved-topic
    notifications through the broker (``update/accepted``/``delta``/
    ``documents``, ``delete/accepted``): on AWS those are state-change
    messages the Device Shadow service emits however the change arrived, so a
    topic rule on them fires for HTTP callers too. A ``get`` publishes
    nothing — ``get/accepted`` is the MQTT request's response channel and an
    HTTP read is answered by its own response body. HTTP failures publish no
    ``rejected`` envelope either; the HTTP status already carries them."""
    thing = unquote(path[len("/things/"):-len("/shadow")])
    shadow_name = qp.get("name", "") or ""
    base = f"{_iot_module.SHADOW_TOPIC_PREFIX}{thing}/shadow" + (
        f"/name/{shadow_name}" if shadow_name else ""
    )
    account_id, region = get_account_id(), get_region()

    if method == "GET":
        status, doc = _iot_module.get_thing_shadow(thing, shadow_name)
    elif method == "POST":
        try:
            request = json.loads(body) if body else {}
        except json.JSONDecodeError:
            return error_response_json("InvalidRequestException", "Invalid JSON", 400)
        # Snapshot around the mutation: the store hands back live dicts, and
        # the documents notification needs the before/after pair (same
        # discipline as the MQTT bridge).
        pre_status, pre_doc = _iot_module.get_thing_shadow(thing, shadow_name)
        pre_doc = copy.deepcopy(pre_doc)
        status, doc = _iot_module.update_thing_shadow(thing, shadow_name, request)
        if status == 200:
            post_status, post_doc = _iot_module.get_thing_shadow(thing, shadow_name)
            post_doc = copy.deepcopy(post_doc)
            await _iot_module.shadow_emit_update_responses(
                account_id, region, base, doc,
                pre_status, pre_doc, post_status, post_doc,
                request.get("clientToken") if isinstance(request, dict) else None,
            )
    elif method == "DELETE":
        status, doc = _iot_module.delete_thing_shadow(thing, shadow_name)
        if status == 200:
            await _iot_module.broker_publish(
                account_id,
                region,
                f"{base}/delete/accepted",
                json.dumps(doc).encode("utf-8"),
                qos=1,
            )
    else:
        return error_response_json(
            "InvalidRequestException", f"Unsupported iot-data path: {method} {path}", 400
        )

    if status == 404:
        return error_response_json("ResourceNotFoundException", doc["message"], 404)
    if status == 409:
        return error_response_json("ConflictException", doc["message"], 409)
    if status >= 400:
        return error_response_json("InvalidRequestException", doc["message"], status)
    return json_response(doc)


async def _publish(topic: str, body: bytes, qp: dict, headers: dict, access_key: str) -> tuple:
    """``POST /topics/{topic}``: forward the body to the broker; the path arrives decoded, so ``%41`` stays literal."""
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
    properties, err = _publish_properties(headers, qp)
    if err is not None:
        return err

    if not _iot_module.broker_is_available():
        return error_response_json(
            "InternalFailureException",
            "IoT broker is not available",
            503,
        )

    try:
        await _iot_module.broker_publish(
            get_account_id(),
            get_region(),
            topic,
            body or b"",
            qos=qos,
            retain=retain,
            properties=properties,
            principal=_iot_module._caller_principal(access_key),
        )
    except Exception as e:
        logger.exception("iot-data publish failed")
        return error_response_json(
            "InternalFailureException", f"Publish failed: {e}", 503,
        )
    # AWS returns 200 with empty body for Publish.
    return 200, {"Content-Type": "application/json"}, b""


def _b64decode(value: str) -> bytes | None:
    """Base64 with or without its padding, or None when the value is not base64."""
    try:
        return base64.b64decode(value + "=" * (-len(value) % 4), validate=True)
    except ValueError:
        return None


def _publish_properties(headers: dict, qp: dict) -> tuple[bytes, tuple | None]:
    """The MQTT 5 property block a Publish request asks for, or the error response refusing it."""

    def invalid(message: str) -> tuple[bytes, tuple]:
        return b"", error_response_json("InvalidRequestException", message, 400)

    props: list[tuple[int, object]] = []
    indicator = headers.get("x-amz-mqtt5-payload-format-indicator")
    if indicator:
        if indicator not in _PAYLOAD_FORMAT_INDICATORS:
            return invalid("Invalid Payload Format Indicator")
        props.append((_iot_module.PROP_PAYLOAD_FORMAT_INDICATOR, _PAYLOAD_FORMAT_INDICATORS[indicator]))
    expiry = qp.get("messageExpiry")
    if expiry is not None:
        seconds = int(expiry) if expiry.isascii() and expiry.isdigit() else -1
        if not 0 <= seconds < 2**32:
            return invalid("Message expiry interval must be an unsigned 4-byte integer")
        props.append((_iot_module.PROP_MESSAGE_EXPIRY_INTERVAL, seconds))
    response_topic = qp.get("responseTopic")
    if response_topic:
        if "+" in response_topic or "#" in response_topic:
            return invalid(
                "This response topic isn't valid. Response topics must not contain"
                " MQTT wildcard characters (+ or #)"
            )
        props.append((_iot_module.PROP_RESPONSE_TOPIC, response_topic))
    correlation = headers.get("x-amz-mqtt5-correlation-data")
    if correlation:
        data = _b64decode(correlation)
        if data is None:
            return invalid("Invalid Base64 string")
        props.append((_iot_module.PROP_CORRELATION_DATA, data))
    if qp.get("contentType"):
        props.append((_iot_module.PROP_CONTENT_TYPE, qp["contentType"]))
    encoded = headers.get("x-amz-mqtt5-user-properties")
    if encoded:
        raw = _b64decode(encoded)
        if raw is None:
            return invalid("Invalid Base64 string")
        try:
            pairs = json.loads(raw)
        except ValueError:
            pairs = None
        if not isinstance(pairs, list) or not all(
            isinstance(pair, dict) and all(isinstance(v, str) for v in pair.values())
            for pair in pairs
        ):
            return invalid("Invalid JSON string for User Properties")
        if any(len(pair) > 1 for pair in pairs):
            return invalid("Only one User Property is allowed in single JSONObject")
        props += [(_iot_module.PROP_USER_PROPERTY, item) for pair in pairs for item in pair.items()]
    return (_iot_module._encode_properties(props) if props else b""), None


def _user_properties(properties: bytes) -> bytes:
    """MQTT 5 user properties as the JSON array AWS returns, or b"" without any."""
    if not properties:
        return b""
    props, _ = _iot_module._decode_properties(properties, 0)
    pairs = [
        {value[0]: value[1]}
        for identifier, value in props
        if identifier == _iot_module.PROP_USER_PROPERTY
    ]
    if not pairs:
        return b""
    return json.dumps(pairs, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _get_retained_message(topic: str) -> tuple:
    """``GET /retainedMessage/{topic}``: the retained message stored for the topic."""
    if not topic:
        return error_response_json("InvalidRequestException", "unknown", 400)
    if "+" in topic or "#" in topic:
        return error_response_json(
            "InvalidRequestException", "Invalid topic filter", 400
        )
    msg = _iot_module.broker_get_retained(get_account_id(), get_region(), topic)
    if msg is None:
        return error_response_json(
            "ResourceNotFoundException", "Retained message not found", 404
        )
    body = {
        "topic": topic,
        "qos": msg.qos,
        "payload": base64.b64encode(msg.payload).decode("ascii"),
    }
    user_properties = _user_properties(msg.properties)
    if user_properties:
        body["userProperties"] = base64.b64encode(user_properties).decode("ascii")
    body["lastModifiedTime"] = int(msg.ts * 1000)
    return json_response(body)


def _list_retained_messages(qp: dict) -> tuple:
    """``GET /retainedMessage``: retained topics, newest first, paged by an offset token."""
    raw = qp.get("maxResults")
    try:
        limit = _RETAINED_PAGE_DEFAULT if raw is None else int(raw)
    except (TypeError, ValueError):
        return error_response_json(
            "InvalidRequestException", f"Invalid maxResults value: {raw}", 400
        )
    if not 1 <= limit <= _RETAINED_PAGE_MAX:
        return error_response_json(
            "InvalidRequestException",
            "Invalid maxResults value: Invalid max results parameter",
            400,
        )
    start = 0
    token = qp.get("nextToken")
    if token:
        try:
            raw_token = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
            prefix, _, offset = raw_token.partition(":")
            start = int(offset) if prefix == "retainedmessages" else -1
        except ValueError:  # binascii.Error and UnicodeDecodeError included
            start = -1
        if start < 0:
            return error_response_json("InvalidRequestException", "unknown", 400)
    ordered = _iot_module.broker_list_retained(get_account_id(), get_region())
    body: dict = {
        "retainedTopics": [
            {
                "payloadSize": len(msg.payload),
                "lastModifiedTime": int(msg.ts * 1000),
                "topic": topic,
                "qos": msg.qos,
            }
            for topic, msg in ordered[start:start + limit]
        ],
        # The last page carries an empty token.
        "nextToken": "",
    }
    if start + limit < len(ordered):
        body["nextToken"] = base64.urlsafe_b64encode(
            f"retainedmessages:{start + limit}".encode()
        ).decode().rstrip("=")
    return json_response(body)
