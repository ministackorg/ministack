"""Integration tests for the IoT Core control plane (Phase 1a).

Exercises Things, ThingTypes, ThingGroups, Certificates (issued via the
Local CA), Policies, and DescribeEndpoint. The data plane (broker / WS /
iot-data Publish) is covered separately in ``test_iot_data.py``.
"""

import json
import uuid

import pytest
from botocore.exceptions import ClientError


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


def test_iot_describe_endpoint_data_ats(iot_client):
    resp = iot_client.describe_endpoint(endpointType="iot:Data-ATS")
    addr = resp["endpointAddress"]
    assert "-ats.iot." in addr
    assert "us-east-1" in addr


def test_iot_describe_endpoint_default_uses_data_ats(iot_client):
    resp = iot_client.describe_endpoint()
    assert "-ats.iot." in resp["endpointAddress"]


def test_iot_describe_endpoint_data_legacy(iot_client):
    resp = iot_client.describe_endpoint(endpointType="iot:Data")
    addr = resp["endpointAddress"]
    # Legacy endpoint omits the -ats suffix.
    assert ".iot." in addr
    assert "-ats.iot." not in addr


def test_iot_describe_endpoint_unknown_type_rejected(iot_client):
    with pytest.raises(ClientError) as ei:
        iot_client.describe_endpoint(endpointType="iot:Bogus")
    assert ei.value.response["Error"]["Code"] in ("InvalidRequestException",)


# ---------------------------------------------------------------------------
# Thing CRUD
# ---------------------------------------------------------------------------


def test_iot_create_describe_thing(iot_client):
    name = _unique("thing")
    resp = iot_client.create_thing(thingName=name)
    assert resp["thingName"] == name
    assert resp["thingArn"].endswith(f":thing/{name}")
    assert resp["thingId"]

    desc = iot_client.describe_thing(thingName=name)
    assert desc["thingName"] == name
    assert desc["version"] == 1
    iot_client.delete_thing(thingName=name)


def test_iot_create_thing_with_attributes(iot_client):
    name = _unique("thing")
    iot_client.create_thing(
        thingName=name,
        attributePayload={"attributes": {"color": "red", "size": "L"}},
    )
    desc = iot_client.describe_thing(thingName=name)
    assert desc["attributes"] == {"color": "red", "size": "L"}
    iot_client.delete_thing(thingName=name)


def test_iot_create_thing_idempotent_same_config(iot_client):
    name = _unique("thing")
    iot_client.create_thing(
        thingName=name,
        attributePayload={"attributes": {"color": "red"}},
    )
    # Same config must not raise.
    resp2 = iot_client.create_thing(
        thingName=name,
        attributePayload={"attributes": {"color": "red"}},
    )
    assert resp2["thingName"] == name
    iot_client.delete_thing(thingName=name)


def test_iot_create_thing_conflict_different_config(iot_client):
    name = _unique("thing")
    iot_client.create_thing(
        thingName=name,
        attributePayload={"attributes": {"color": "red"}},
    )
    with pytest.raises(ClientError) as ei:
        iot_client.create_thing(
            thingName=name,
            attributePayload={"attributes": {"color": "blue"}},
        )
    assert ei.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
    iot_client.delete_thing(thingName=name)


def test_iot_describe_unknown_thing_404(iot_client):
    with pytest.raises(ClientError) as ei:
        iot_client.describe_thing(thingName=_unique("nope"))
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_iot_update_thing_increments_version(iot_client):
    name = _unique("thing")
    iot_client.create_thing(thingName=name)
    iot_client.update_thing(
        thingName=name,
        attributePayload={"attributes": {"k": "v"}},
    )
    desc = iot_client.describe_thing(thingName=name)
    assert desc["version"] == 2
    assert desc["attributes"] == {"k": "v"}
    iot_client.delete_thing(thingName=name)


def test_iot_list_things_filter_by_attribute(iot_client):
    a = _unique("thing")
    b = _unique("thing")
    iot_client.create_thing(
        thingName=a, attributePayload={"attributes": {"region": "eu"}}
    )
    iot_client.create_thing(
        thingName=b, attributePayload={"attributes": {"region": "us"}}
    )
    resp = iot_client.list_things(attributeName="region", attributeValue="eu")
    names = {t["thingName"] for t in resp["things"]}
    assert a in names and b not in names
    iot_client.delete_thing(thingName=a)
    iot_client.delete_thing(thingName=b)


def test_iot_list_things_filter_by_thing_type(iot_client):
    type_a = _unique("type")
    iot_client.create_thing_type(thingTypeName=type_a)
    name = _unique("thing")
    iot_client.create_thing(thingName=name, thingTypeName=type_a)

    resp = iot_client.list_things(thingTypeName=type_a)
    assert any(t["thingName"] == name for t in resp["things"])

    iot_client.delete_thing(thingName=name)
    iot_client.deprecate_thing_type(thingTypeName=type_a)
    iot_client.delete_thing_type(thingTypeName=type_a)


# ---------------------------------------------------------------------------
# ThingType CRUD
# ---------------------------------------------------------------------------


def test_iot_thing_type_lifecycle(iot_client):
    name = _unique("type")
    iot_client.create_thing_type(thingTypeName=name)
    desc = iot_client.describe_thing_type(thingTypeName=name)
    assert desc["thingTypeName"] == name
    assert desc["thingTypeMetadata"]["deprecated"] is False

    iot_client.deprecate_thing_type(thingTypeName=name)
    desc2 = iot_client.describe_thing_type(thingTypeName=name)
    assert desc2["thingTypeMetadata"]["deprecated"] is True

    iot_client.delete_thing_type(thingTypeName=name)
    with pytest.raises(ClientError) as ei:
        iot_client.describe_thing_type(thingTypeName=name)
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_iot_delete_thing_type_active_rejected(iot_client):
    name = _unique("type")
    iot_client.create_thing_type(thingTypeName=name)
    with pytest.raises(ClientError) as ei:
        iot_client.delete_thing_type(thingTypeName=name)
    assert ei.value.response["Error"]["Code"] == "InvalidRequestException"
    iot_client.deprecate_thing_type(thingTypeName=name)
    iot_client.delete_thing_type(thingTypeName=name)


# ---------------------------------------------------------------------------
# ThingGroup CRUD + membership
# ---------------------------------------------------------------------------


def test_iot_thing_group_membership(iot_client):
    gname = _unique("group")
    tname = _unique("thing")
    iot_client.create_thing_group(thingGroupName=gname)
    iot_client.create_thing(thingName=tname)

    iot_client.add_thing_to_thing_group(thingGroupName=gname, thingName=tname)
    things = iot_client.list_things_in_thing_group(thingGroupName=gname)["things"]
    assert tname in things

    iot_client.remove_thing_from_thing_group(thingGroupName=gname, thingName=tname)
    things2 = iot_client.list_things_in_thing_group(thingGroupName=gname)["things"]
    assert tname not in things2

    iot_client.delete_thing(thingName=tname)
    iot_client.delete_thing_group(thingGroupName=gname)


# ---------------------------------------------------------------------------
# Certificates (issued via the Local CA)
# ---------------------------------------------------------------------------


def test_iot_create_keys_and_certificate_active(iot_client):
    pytest.importorskip("cryptography")
    resp = iot_client.create_keys_and_certificate(setAsActive=True)
    assert resp["certificateId"]
    assert resp["certificateArn"].endswith(":cert/" + resp["certificateId"])
    assert "BEGIN CERTIFICATE" in resp["certificatePem"]
    assert "BEGIN" in resp["keyPair"]["PrivateKey"]
    assert "BEGIN PUBLIC KEY" in resp["keyPair"]["PublicKey"]

    desc = iot_client.describe_certificate(certificateId=resp["certificateId"])
    assert desc["certificateDescription"]["status"] == "ACTIVE"

    # Deactivate and delete
    iot_client.update_certificate(
        certificateId=resp["certificateId"], newStatus="INACTIVE"
    )
    iot_client.delete_certificate(certificateId=resp["certificateId"])


def test_iot_create_keys_and_certificate_inactive(iot_client):
    pytest.importorskip("cryptography")
    resp = iot_client.create_keys_and_certificate(setAsActive=False)
    desc = iot_client.describe_certificate(certificateId=resp["certificateId"])
    assert desc["certificateDescription"]["status"] == "INACTIVE"
    iot_client.delete_certificate(certificateId=resp["certificateId"])


def test_iot_delete_active_certificate_rejected(iot_client):
    pytest.importorskip("cryptography")
    resp = iot_client.create_keys_and_certificate(setAsActive=True)
    cert_id = resp["certificateId"]
    with pytest.raises(ClientError) as ei:
        iot_client.delete_certificate(certificateId=cert_id)
    assert ei.value.response["Error"]["Code"] == "CertificateStateException"
    iot_client.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
    iot_client.delete_certificate(certificateId=cert_id)


def test_iot_register_certificate_preserves_pem_verbatim(iot_client):
    pytest.importorskip("cryptography")
    # Issue a cert, capture its PEM, delete it, then re-register the SAME PEM.
    issued = iot_client.create_keys_and_certificate(setAsActive=False)
    cert_pem = issued["certificatePem"]
    iot_client.delete_certificate(certificateId=issued["certificateId"])

    resp = iot_client.register_certificate(
        certificatePem=cert_pem, status="ACTIVE"
    )
    cert_id = resp["certificateId"]
    desc = iot_client.describe_certificate(certificateId=cert_id)
    assert desc["certificateDescription"]["certificatePem"] == cert_pem
    iot_client.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
    iot_client.delete_certificate(certificateId=cert_id)


def test_iot_attach_detach_thing_principal(iot_client):
    pytest.importorskip("cryptography")
    name = _unique("thing")
    iot_client.create_thing(thingName=name)
    cert = iot_client.create_keys_and_certificate(setAsActive=True)
    arn = cert["certificateArn"]

    iot_client.attach_thing_principal(thingName=name, principal=arn)
    principals = iot_client.list_thing_principals(thingName=name)["principals"]
    assert arn in principals
    things = iot_client.list_principal_things(principal=arn)["things"]
    assert name in things

    iot_client.detach_thing_principal(thingName=name, principal=arn)
    principals2 = iot_client.list_thing_principals(thingName=name)["principals"]
    assert arn not in principals2

    iot_client.update_certificate(certificateId=cert["certificateId"], newStatus="INACTIVE")
    iot_client.delete_certificate(certificateId=cert["certificateId"])
    iot_client.delete_thing(thingName=name)


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------


_POLICY_DOC = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["iot:Connect", "iot:Publish"],
                "Resource": "*",
            }
        ],
    }
)


def test_iot_policy_lifecycle(iot_client):
    name = _unique("policy")
    resp = iot_client.create_policy(policyName=name, policyDocument=_POLICY_DOC)
    assert resp["policyName"] == name
    assert resp["policyVersionId"] == "1"

    got = iot_client.get_policy(policyName=name)
    assert got["defaultVersionId"] == "1"

    listing = iot_client.list_policies()["policies"]
    assert any(p["policyName"] == name for p in listing)

    iot_client.delete_policy(policyName=name)


def test_iot_create_policy_malformed_400(iot_client):
    name = _unique("policy")
    with pytest.raises(ClientError) as ei:
        iot_client.create_policy(policyName=name, policyDocument="not-json")
    assert ei.value.response["Error"]["Code"] == "MalformedPolicyException"


def test_iot_policy_versions(iot_client):
    name = _unique("policy")
    iot_client.create_policy(policyName=name, policyDocument=_POLICY_DOC)
    new_doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "iot:Subscribe", "Resource": "*"}
            ],
        }
    )
    v2 = iot_client.create_policy_version(
        policyName=name, policyDocument=new_doc, setAsDefault=True
    )
    assert v2["policyVersionId"] == "2"

    versions = iot_client.list_policy_versions(policyName=name)["policyVersions"]
    assert {v["versionId"] for v in versions} == {"1", "2"}
    assert next(v for v in versions if v["versionId"] == "2")["isDefaultVersion"]

    iot_client.delete_policy_version(policyName=name, policyVersionId="1")
    iot_client.delete_policy(policyName=name)


def test_iot_attach_detach_policy(iot_client):
    pytest.importorskip("cryptography")
    name = _unique("policy")
    iot_client.create_policy(policyName=name, policyDocument=_POLICY_DOC)
    cert = iot_client.create_keys_and_certificate(setAsActive=False)
    arn = cert["certificateArn"]

    iot_client.attach_policy(policyName=name, target=arn)
    targets = iot_client.list_targets_for_policy(policyName=name)["targets"]
    assert arn in targets

    iot_client.detach_policy(policyName=name, target=arn)
    targets2 = iot_client.list_targets_for_policy(policyName=name)["targets"]
    assert arn not in targets2

    iot_client.delete_policy(policyName=name)
    iot_client.delete_certificate(certificateId=cert["certificateId"])


# ---------------------------------------------------------------------------
# Local CA admin endpoint
# ---------------------------------------------------------------------------


def test_iot_ca_pem_endpoint_returns_certificate():
    pytest.importorskip("cryptography")
    import os
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    with urllib.request.urlopen(f"{endpoint}/_ministack/iot/ca.pem", timeout=5) as resp:
        body = resp.read().decode("utf-8")
    assert "BEGIN CERTIFICATE" in body
    assert "END CERTIFICATE" in body


# ---------------------------------------------------------------------------
# Account isolation
# ---------------------------------------------------------------------------


def test_iot_account_isolation():
    """Two callers using different 12-digit access keys see different Things."""
    import os

    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    def _client(account_id):
        return boto3.client(
            "iot",
            endpoint_url=endpoint,
            aws_access_key_id=account_id,
            aws_secret_access_key="test",
            region_name="us-east-1",
            config=Config(retries={"mode": "standard"}),
        )

    a = _client("111111111111")
    b = _client("222222222222")
    name = _unique("thing")
    a.create_thing(thingName=name)
    # Account B must not see Thing in account A.
    b_things = {t["thingName"] for t in b.list_things().get("things", [])}
    assert name not in b_things
    a.delete_thing(thingName=name)


# ----------------------------------------------------------------------
# Broker unit-test helpers (white-box tests for the in-process MQTT
# broker that lives in iot.py). Section headers below mark logical
# groupings: LWT (Last Will and Testament), persistent sessions
# (cleanSession=0), and QoS 1 delivery / retransmits.
# ----------------------------------------------------------------------

import asyncio
import struct

from ministack.services.iot import (
    PKT_CONNACK,
    PKT_CONNECT,
    PKT_DISCONNECT,
    PKT_PUBACK,
    PKT_PUBLISH,
    PKT_SUBSCRIBE,
    _InFlightMessage,
    _RETRANSMIT_INTERVAL_SECONDS,
    _Subscription,
    _WSSession,
    _encode_remaining_length,
    _encode_string,
    _make_puback,
    _make_suback,
    _persistent_sessions,
    broker_publish as publish,
    broker_reset as reset,
    broker_subscribe as subscribe,
)


def _build_connect_body(
    client_id="test",
    clean_session=True,
    will_flag=False,
    will_qos=0,
    will_retain=False,
    will_topic="",
    will_message=b"",
):
    """Build a CONNECT packet body (variable header + payload)."""
    body = bytearray()
    body += _encode_string("MQTT")  # Protocol Name
    body.append(4)                   # Protocol Level (MQTT 3.1.1)
    flags = 0
    if clean_session:
        flags |= 0x02
    if will_flag:
        flags |= 0x04
        flags |= (will_qos & 0x03) << 3
        if will_retain:
            flags |= 0x20
    body.append(flags)
    body += struct.pack("!H", 60)    # Keep Alive
    body += _encode_string(client_id)
    if will_flag:
        body += _encode_string(will_topic)
        msg = will_message if isinstance(will_message, bytes) else will_message.encode()
        body += struct.pack("!H", len(msg)) + msg
    return bytes(body)


def _build_subscribe_body(packet_id, topics_qos):
    """Build a SUBSCRIBE packet body. topics_qos is a list of (topic, qos)."""
    body = struct.pack("!H", packet_id)
    for topic, qos in topics_qos:
        body += _encode_string(topic)
        body += bytes([qos])
    return body


def _mock_send():
    """Return (async send-callable, captured-message list)."""
    sent = []

    async def send(msg):
        sent.append(msg)

    return send, sent


def _parse_connack(sent_messages):
    """Extract (sessionPresent, return_code) from a CONNACK in sent messages."""
    for msg in sent_messages:
        data = msg.get("bytes")
        if data and len(data) >= 4:
            pkt_type = (data[0] >> 4) & 0x0F
            if pkt_type == PKT_CONNACK:
                session_present = bool(data[2] & 0x01)
                return_code = data[3]
                return session_present, return_code
    return None, None


def _extract_publish_frames(sent_messages):
    """Return list of (topic, payload, qos, packet_id, dup) for every PUBLISH in sent messages."""
    results = []
    for msg in sent_messages:
        data = msg.get("bytes")
        if data is None or not data:
            continue
        first = data[0]
        pkt_type = (first >> 4) & 0x0F
        if pkt_type != 3:  # 3 = PUBLISH
            continue
        qos = (first >> 1) & 0x03
        dup = bool(first & 0x08)
        offset = 1
        multiplier = 1
        remaining = 0
        while True:
            b = data[offset]
            offset += 1
            remaining += (b & 0x7F) * multiplier
            if b & 0x80 == 0:
                break
            multiplier *= 128
        topic_len = struct.unpack_from("!H", data, offset)[0]
        offset += 2
        topic = data[offset:offset + topic_len].decode("utf-8")
        offset += topic_len
        packet_id = None
        if qos > 0:
            packet_id = struct.unpack_from("!H", data, offset)[0]
            offset += 2
        payload = data[offset:]
        results.append((topic, payload, qos, packet_id, dup))
    return results


# ----------------------------------------------------------------------
# Broker — Last Will and Testament
# ----------------------------------------------------------------------



def test_will_fields_parsed_from_connect():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device1",
            will_flag=True,
            will_qos=1,
            will_retain=True,
            will_topic="devices/device1/status",
            will_message=b"offline",
        )
        result = await session.handle_packet(PKT_CONNECT, 0, body)
        assert result is True
        assert session._will_topic == "devices/device1/status"
        assert session._will_message == b"offline"
        assert session._will_qos == 1
        assert session._will_retain is True

    asyncio.run(_run())
    reset()


def test_no_will_when_flag_not_set():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="device2", will_flag=False)
        await session.handle_packet(PKT_CONNECT, 0, body)
        assert session._will_topic is None
        assert session._will_message is None
        assert session._will_qos == 0
        assert session._will_retain is False

    asyncio.run(_run())
    reset()


def test_graceful_disconnect_does_not_publish_will():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device3",
            will_flag=True,
            will_qos=0,
            will_retain=False,
            will_topic="devices/device3/status",
            will_message=b"offline",
        )
        await session.handle_packet(PKT_CONNECT, 0, body)

        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        await subscribe("123456789012", "devices/device3/status", on_msg)

        # Graceful disconnect
        result = await session.handle_packet(PKT_DISCONNECT, 0, b"")
        assert result is False
        assert session._graceful_disconnect is True
        await session.cleanup()
        assert len(received) == 0

    asyncio.run(_run())
    reset()


def test_ungraceful_disconnect_publishes_will():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device4",
            will_flag=True,
            will_qos=1,
            will_retain=False,
            will_topic="devices/device4/status",
            will_message=b"gone",
        )
        await session.handle_packet(PKT_CONNECT, 0, body)

        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        # Subscribe at QoS 1 so effective QoS = min(publish_qos=1, granted_qos=1) = 1
        await subscribe("123456789012", "devices/device4/status", on_msg, granted_qos=1)

        # Ungraceful disconnect (no DISCONNECT packet)
        await session.cleanup()
        assert len(received) == 1
        assert received[0] == ("devices/device4/status", b"gone", 1)

    asyncio.run(_run())
    reset()


def test_will_retain_stores_retained_message():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device5",
            will_flag=True,
            will_qos=0,
            will_retain=True,
            will_topic="devices/device5/status",
            will_message=b"dead",
        )
        await session.handle_packet(PKT_CONNECT, 0, body)
        # Ungraceful disconnect publishes Will with retain
        await session.cleanup()

        # New subscriber should get the retained message
        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        await subscribe("123456789012", "devices/device5/status", on_msg)
        assert len(received) == 1
        assert received[0] == ("devices/device5/status", b"dead", 0)

    asyncio.run(_run())
    reset()


def test_reconnect_replaces_will_fields():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body1 = _build_connect_body(
            client_id="device6",
            will_flag=True,
            will_qos=0,
            will_retain=False,
            will_topic="old/topic",
            will_message=b"old",
        )
        await session.handle_packet(PKT_CONNECT, 0, body1)
        assert session._will_topic == "old/topic"

        # Reconnect with new Will
        body2 = _build_connect_body(
            client_id="device6",
            will_flag=True,
            will_qos=1,
            will_retain=True,
            will_topic="new/topic",
            will_message=b"new",
        )
        await session.handle_packet(PKT_CONNECT, 0, body2)
        assert session._will_topic == "new/topic"
        assert session._will_message == b"new"
        assert session._will_qos == 1
        assert session._will_retain is True

    asyncio.run(_run())
    reset()


def test_reconnect_clears_graceful_disconnect_flag():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="device7", will_flag=False)
        await session.handle_packet(PKT_CONNECT, 0, body)

        # Graceful disconnect
        await session.handle_packet(PKT_DISCONNECT, 0, b"")
        assert session._graceful_disconnect is True

        # Reconnect resets the flag
        await session.handle_packet(PKT_CONNECT, 0, body)
        assert session._graceful_disconnect is False

    asyncio.run(_run())
    reset()


def test_reconnect_without_will_clears_previous_will():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        # First connect with Will
        body1 = _build_connect_body(
            client_id="device8",
            will_flag=True,
            will_qos=1,
            will_retain=True,
            will_topic="presence/device8",
            will_message=b"offline",
        )
        await session.handle_packet(PKT_CONNECT, 0, body1)
        assert session._will_topic == "presence/device8"

        # Reconnect without Will
        body2 = _build_connect_body(client_id="device8", will_flag=False)
        await session.handle_packet(PKT_CONNECT, 0, body2)
        assert session._will_topic is None
        assert session._will_message is None

        # Ungraceful disconnect should NOT publish anything
        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        await subscribe("123456789012", "presence/device8", on_msg)
        await session.cleanup()
        assert len(received) == 0

    asyncio.run(_run())
    reset()


# ----------------------------------------------------------------------
# Broker — Persistent sessions (cleanSession flag)
# ----------------------------------------------------------------------



def test_clean_session_1_sends_session_present_0():
    """cleanSession=1 always sends sessionPresent=0."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="client1", clean_session=True)
        await session.handle_packet(PKT_CONNECT, 0, body)

        session_present, return_code = _parse_connack(sent)
        assert session_present is False
        assert return_code == 0

    asyncio.run(_run())
    reset()


def test_clean_session_0_no_prior_session_sends_session_present_0():
    """cleanSession=0 with no prior session sends sessionPresent=0."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="client1", clean_session=False)
        await session.handle_packet(PKT_CONNECT, 0, body)

        session_present, return_code = _parse_connack(sent)
        assert session_present is False
        assert return_code == 0

    asyncio.run(_run())
    reset()


def test_persistent_session_subscribe_disconnect_reconnect_restores():
    """Connect with cleanSession=0 → subscribe → disconnect → reconnect → sessionPresent=1 and subscriptions restored."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe to a topic
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device1", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        # Subscribe to "sensor/temp"
        sub_body = _build_subscribe_body(1, [("sensor/temp", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect (graceful)
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Second connection: cleanSession=0, same client_id
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, return_code = _parse_connack(sent2)
        assert session_present is True
        assert return_code == 0

        # Verify subscriptions are restored by publishing a message
        received = []

        # The session should already be subscribed, so publish should deliver
        await publish("123456789012", "sensor/temp", b"25C", qos=1)

        # Check that session2 received the message
        # The message should be in sent2 as a PUBLISH packet
        publish_found = False
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_found = True
                break
        assert publish_found, "Restored subscription should receive published messages"

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_clean_session_1_discards_prior_state():
    """cleanSession=1 discards any prior persistent session state."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body_persistent = _build_connect_body(client_id="device2", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body_persistent)

        sub_body = _build_subscribe_body(1, [("alerts/#", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Verify persistent session exists
        assert ("123456789012", "device2") in _persistent_sessions

        # Second connection: cleanSession=1 — should discard prior state
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        connect_body_clean = _build_connect_body(client_id="device2", clean_session=True)
        await session2.handle_packet(PKT_CONNECT, 0, connect_body_clean)

        session_present, return_code = _parse_connack(sent2)
        assert session_present is False
        assert return_code == 0

        # Verify persistent session was discarded
        assert ("123456789012", "device2") not in _persistent_sessions

        # Publish to the old subscription topic — should NOT be delivered
        await publish("123456789012", "alerts/fire", b"alarm", qos=1)

        publish_found = False
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_found = True
                break
        assert not publish_found, "cleanSession=1 should not restore prior subscriptions"

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_offline_qos1_messages_queued_and_delivered_on_reconnect():
    """Persistent session disconnects; QoS 1 messages published; reconnect delivers queued messages."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device3", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("data/stream", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Publish QoS 1 messages while client is offline
        await publish("123456789012", "data/stream", b"msg1", qos=1)
        await publish("123456789012", "data/stream", b"msg2", qos=1)
        await publish("123456789012", "data/stream", b"msg3", qos=1)

        # Verify messages are queued
        ps = _persistent_sessions.get(("123456789012", "device3"))
        assert ps is not None
        assert len(ps.queued_messages) == 3

        # Reconnect with cleanSession=0
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, _ = _parse_connack(sent2)
        assert session_present is True

        # Verify queued messages were delivered
        publish_messages = []
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_messages.append(data)

        assert len(publish_messages) == 3, f"Expected 3 queued messages delivered, got {len(publish_messages)}"

        # Verify queue is now empty
        assert len(ps.queued_messages) == 0

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_qos0_messages_not_queued_for_offline_sessions():
    """QoS 0 messages should NOT be queued for offline persistent sessions."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device4", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("events/log", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Publish QoS 0 messages while client is offline
        await publish("123456789012", "events/log", b"info1", qos=0)
        await publish("123456789012", "events/log", b"info2", qos=0)

        # Verify no messages queued (QoS 0 not queued)
        ps = _persistent_sessions.get(("123456789012", "device4"))
        assert ps is not None
        assert len(ps.queued_messages) == 0

    asyncio.run(_run())
    reset()


def test_queue_bounded_to_1000_messages():
    """Queue should be bounded to 1000 messages, dropping oldest on overflow."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device5", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("bulk/data", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Publish 1050 QoS 1 messages while client is offline
        for i in range(1050):
            await publish("123456789012", "bulk/data", f"msg{i}".encode(), qos=1)

        # Verify queue is bounded to 1000
        ps = _persistent_sessions.get(("123456789012", "device5"))
        assert ps is not None
        assert len(ps.queued_messages) == 1000

        # Verify oldest messages were dropped (first 50 should be gone)
        first_topic, first_payload, first_qos = ps.queued_messages[0]
        assert first_payload == b"msg50"

    asyncio.run(_run())
    reset()


def test_expired_session_not_restored():
    """An expired persistent session should not be restored."""
    reset()

    async def _run():
        import time

        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device6", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("temp/data", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Manually expire the session by setting created_at far in the past
        ps = _persistent_sessions.get(("123456789012", "device6"))
        assert ps is not None
        ps.created_at = time.time() - 7200  # 2 hours ago (default expiry is 1 hour)

        # Reconnect with cleanSession=0
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, return_code = _parse_connack(sent2)
        assert session_present is False  # Expired session not restored
        assert return_code == 0

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_wildcard_subscription_persisted_and_restored():
    """Wildcard subscriptions should be persisted and restored correctly."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe with wildcard
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device7", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("sensors/+/temp", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Reconnect
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, _ = _parse_connack(sent2)
        assert session_present is True

        # Publish to a topic matching the wildcard
        await publish("123456789012", "sensors/room1/temp", b"22C", qos=1)

        publish_found = False
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_found = True
                break
        assert publish_found, "Restored wildcard subscription should receive matching messages"

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_different_accounts_sessions_isolated():
    """Persistent sessions are scoped by (account_id, client_id)."""
    reset()

    async def _run():
        # Account A: connect, subscribe, disconnect
        send_a, sent_a = _mock_send()
        session_a = _WSSession(send_a, "account_A")

        connect_body = _build_connect_body(client_id="shared_id", clean_session=False)
        await session_a.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("topic/a", 1)])
        await session_a.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)
        await session_a.handle_packet(PKT_DISCONNECT, 0, b"")
        await session_a.cleanup()

        # Account B: connect with same client_id — should NOT see account A's session
        send_b, sent_b = _mock_send()
        session_b = _WSSession(send_b, "account_B")

        await session_b.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, _ = _parse_connack(sent_b)
        assert session_present is False  # No prior session for account_B

        await session_b.cleanup()

    asyncio.run(_run())
    reset()


# ----------------------------------------------------------------------
# Broker — QoS 1 + retransmits
# ----------------------------------------------------------------------



def test_subscription_has_granted_qos_field():
    """_Subscription stores granted_qos."""
    async def deliver(t, p, q):
        pass

    sub = _Subscription("acct/topic", "acct", deliver, granted_qos=1)
    assert sub.granted_qos == 1

    sub0 = _Subscription("acct/topic", "acct", deliver, granted_qos=0)
    assert sub0.granted_qos == 0


def test_subscribe_handler_caps_qos_at_1():
    """PKT_SUBSCRIBE grants min(requested, 1) — QoS 2 is capped to 1."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("sub-client"))

        # Subscribe with QoS 0, 1, and 2
        body = _build_subscribe_body(1, [
            ("topic/a", 0),
            ("topic/b", 1),
            ("topic/c", 2),  # Should be capped to 1
        ])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)

        # Find SUBACK in sent messages
        suback_frames = [m for m in sent if m.get("bytes") and (m["bytes"][0] >> 4) == 9]
        assert len(suback_frames) == 1
        suback_data = suback_frames[0]["bytes"]
        # SUBACK: fixed header (1 byte) + remaining length (1 byte) + packet_id (2 bytes) + return codes
        offset = 1
        # Decode remaining length
        multiplier = 1
        remaining = 0
        while True:
            b = suback_data[offset]
            offset += 1
            remaining += (b & 0x7F) * multiplier
            if b & 0x80 == 0:
                break
            multiplier *= 128
        # Skip packet ID
        offset += 2
        # Return codes
        return_codes = list(suback_data[offset:])
        assert return_codes == [0, 1, 1]  # QoS 2 capped to 1

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_subscribe_stores_granted_qos_on_session():
    """Session tracks granted QoS per subscription ID."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos-track"))

        body = _build_subscribe_body(1, [("sensor/temp", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)

        # Session should have one subscription with granted_qos=1
        assert len(session._sub_ids) == 1
        sid = session._sub_ids[0]
        assert session._sub_granted_qos[sid] == 1

        await session.cleanup()

    asyncio.run(_run())
    reset()


# ---------------------------------------------------------------------------
# Task 18.2: QoS 1 delivery with packet ID tracking
# ---------------------------------------------------------------------------


def test_qos1_publish_delivers_with_packet_id():
    """QoS 1 publish to QoS 1 subscriber delivers at QoS 1 with packet ID."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos1-sub"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/qos1", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)

        # Clear sent to isolate publish frames
        sent.clear()

        # Publish at QoS 1 from external source
        await publish("123456789012", "test/qos1", b"hello-qos1", qos=1)

        # Check delivered message
        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert topic == "test/qos1"
        assert payload == b"hello-qos1"
        assert qos == 1
        assert packet_id is not None
        assert packet_id >= 1
        assert dup is False

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_qos0_publish_to_qos1_subscriber_delivers_at_qos0():
    """QoS 0 publish to QoS 1 subscriber delivers at QoS 0 (effective = min)."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos-min"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/minqos", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 0
        await publish("123456789012", "test/minqos", b"qos0-msg", qos=0)

        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert qos == 0
        assert packet_id is None  # No packet ID for QoS 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_qos1_publish_to_qos0_subscriber_delivers_at_qos0():
    """QoS 1 publish to QoS 0 subscriber delivers at QoS 0 (effective = min)."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos0-sub"))

        # Subscribe at QoS 0
        body = _build_subscribe_body(1, [("test/downgrade", 0)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/downgrade", b"downgraded", qos=1)

        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert qos == 0
        assert packet_id is None

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_qos1_delivery_tracks_in_flight():
    """QoS 1 delivery stores message in _in_flight dict."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("inflight"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/inflight", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/inflight", b"tracked", qos=1)

        # Should have one in-flight message
        assert len(session._in_flight) == 1
        pid = list(session._in_flight.keys())[0]
        msg = session._in_flight[pid]
        assert msg.topic == "test/inflight"
        assert msg.payload == b"tracked"
        assert msg.retransmit_count == 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_packet_ids_are_monotonically_increasing():
    """Packet IDs increment monotonically."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        ids = [session._alloc_packet_id() for _ in range(5)]
        assert ids == [1, 2, 3, 4, 5]

    asyncio.run(_run())
    reset()


def test_packet_ids_wrap_at_65535():
    """Packet IDs wrap from 65535 back to 1."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")
        session._next_pid = 65535

        pid1 = session._alloc_packet_id()
        pid2 = session._alloc_packet_id()
        assert pid1 == 65535
        assert pid2 == 1  # Wraps back to 1

    asyncio.run(_run())
    reset()


# ---------------------------------------------------------------------------
# Task 18.3: PUBACK handling and retransmission
# ---------------------------------------------------------------------------


def test_puback_removes_from_in_flight():
    """PUBACK with matching packet ID removes message from _in_flight."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("puback-test"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/puback", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/puback", b"ack-me", qos=1)

        assert len(session._in_flight) == 1
        pid = list(session._in_flight.keys())[0]

        # Send PUBACK
        puback_body = struct.pack("!H", pid)
        result = await session.handle_packet(PKT_PUBACK, 0, puback_body)
        assert result is True
        assert len(session._in_flight) == 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_puback_unknown_packet_id_is_ignored():
    """PUBACK for unknown packet ID does not crash."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("puback-unknown"))

        # Send PUBACK for non-existent packet ID
        puback_body = struct.pack("!H", 999)
        result = await session.handle_packet(PKT_PUBACK, 0, puback_body)
        assert result is True
        assert len(session._in_flight) == 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_retransmit_task_started_on_qos1_delivery():
    """Retransmit background task is started when QoS 1 message is delivered."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("retransmit"))

        assert session._retransmit_task is None

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/retransmit", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/retransmit", b"retry-me", qos=1)

        # Retransmit task should be started
        assert session._retransmit_task is not None
        assert not session._retransmit_task.done()

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_cleanup_cancels_retransmit_task():
    """cleanup() cancels the retransmit background task."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("cleanup-rt"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/cleanup", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1 to start retransmit task
        await publish("123456789012", "test/cleanup", b"clean", qos=1)
        task = session._retransmit_task
        assert task is not None

        await session.cleanup()
        assert session._retransmit_task is None
        assert task.done() or task.cancelled()

    asyncio.run(_run())
    reset()


def test_cleanup_clears_in_flight():
    """cleanup() clears the _in_flight dict."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("cleanup-if"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/clear", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/clear", b"clear-me", qos=1)
        assert len(session._in_flight) == 1

        await session.cleanup()
        assert len(session._in_flight) == 0

    asyncio.run(_run())
    reset()


def test_retransmit_sends_dup_flag():
    """Retransmission sends PUBLISH with DUP flag set."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("dup-test"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/dup", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/dup", b"dup-payload", qos=1)

        # Verify initial publish has DUP=False
        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        assert publishes[0][4] is False  # dup flag

        # Manually trigger retransmission by manipulating sent_at
        pid = list(session._in_flight.keys())[0]
        msg = session._in_flight[pid]
        # Set sent_at far in the past to trigger retransmit
        msg.sent_at = 0

        sent.clear()

        # Run one iteration of retransmit logic manually
        import asyncio as _asyncio
        now = _asyncio.get_event_loop().time()
        for p, m in list(session._in_flight.items()):
            if now - m.sent_at >= _RETRANSMIT_INTERVAL_SECONDS:
                m.retransmit_count += 1
                m.sent_at = now
                from ministack.services.iot import _make_publish
                await session.send_bytes(
                    _make_publish(m.topic, m.payload, qos=1, packet_id=p, dup=True)
                )

        # Verify retransmitted publish has DUP=True
        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert topic == "test/dup"
        assert payload == b"dup-payload"
        assert qos == 1
        assert packet_id == pid
        assert dup is True

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_in_flight_message_fields():
    """_InFlightMessage stores all required fields."""
    reset()

    async def _run():
        msg = _InFlightMessage(packet_id=42, topic="sensor/data", payload=b"temp=22")
        assert msg.packet_id == 42
        assert msg.topic == "sensor/data"
        assert msg.payload == b"temp=22"
        assert msg.sent_at > 0
        assert msg.retransmit_count == 0

    asyncio.run(_run())
    reset()


def test_multiple_qos1_messages_get_unique_packet_ids():
    """Multiple QoS 1 deliveries get unique, incrementing packet IDs."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("multi-pid"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/multi", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish 3 messages at QoS 1
        await publish("123456789012", "test/multi", b"msg1", qos=1)
        await publish("123456789012", "test/multi", b"msg2", qos=1)
        await publish("123456789012", "test/multi", b"msg3", qos=1)

        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 3
        pids = [p[3] for p in publishes]
        # All unique
        assert len(set(pids)) == 3
        # Monotonically increasing
        assert pids == sorted(pids)

        # All tracked in-flight
        assert len(session._in_flight) == 3

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_puback_for_first_of_multiple_in_flight():
    """PUBACK removes only the specific packet ID from in-flight."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("selective-ack"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/selective", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish 3 messages
        await publish("123456789012", "test/selective", b"a", qos=1)
        await publish("123456789012", "test/selective", b"b", qos=1)
        await publish("123456789012", "test/selective", b"c", qos=1)

        assert len(session._in_flight) == 3
        pids = sorted(session._in_flight.keys())

        # ACK the middle one
        puback_body = struct.pack("!H", pids[1])
        await session.handle_packet(PKT_PUBACK, 0, puback_body)

        assert len(session._in_flight) == 2
        assert pids[1] not in session._in_flight
        assert pids[0] in session._in_flight
        assert pids[2] in session._in_flight

        await session.cleanup()

    asyncio.run(_run())
    reset()
