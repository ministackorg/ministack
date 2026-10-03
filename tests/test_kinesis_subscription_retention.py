# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Local dispatch regressions for retention during an active subscription."""

import asyncio
import base64
import json
from types import SimpleNamespace

import cbor2
import pytest
from botocore.eventstream import EventStreamBuffer

from ministack.core.responses import AccountRegionScopedDict, request_scope
from ministack.services import kinesis

STARTS = ("TRIM_HORIZON", "LATEST", "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER", "AT_TIMESTAMP")
SHARD = "shardId-000000000000"


@pytest.fixture
def subscription_state(monkeypatch):
    clock = [1_700_000_000.0]
    monkeypatch.setattr(kinesis, "time", SimpleNamespace(time=lambda: clock[0]))
    monkeypatch.setattr(kinesis, "_streams", AccountRegionScopedDict())
    monkeypatch.setattr(kinesis, "_consumers", AccountRegionScopedDict())
    monkeypatch.setattr(kinesis, "_subscriptions", {})
    monkeypatch.setattr(kinesis, "_SUBSCRIPTION_IDLE_SECONDS", 0)
    return clock


async def _call(action, data, is_cbor=False):
    encode = cbor2.dumps if is_cbor else lambda value: json.dumps(value).encode()
    headers = {"x-amz-target": f"Kinesis_20131202.{action}"}
    if is_cbor:
        headers["content-type"] = "application/x-amz-cbor-1.1"
    result = await kinesis.handle_request("POST", "/", headers, encode(data), {})
    assert result[0] == 200, result
    if action == "SubscribeToShard":
        return result[2]
    return cbor2.loads(result[2]) if is_cbor else json.loads(result[2])


async def _setup():
    await _call("CreateStream", {"StreamName": "retention", "ShardCount": 1})
    arn = (await _call("DescribeStream", {"StreamName": "retention"}))["StreamDescription"]["StreamARN"]
    consumer = await _call("RegisterStreamConsumer", {"StreamARN": arn, "ConsumerName": "reader"})
    return consumer["Consumer"]["ConsumerARN"]


async def _put(value):
    return (await _call("PutRecord", {
        "StreamName": "retention", "PartitionKey": "p", "Data": base64.b64encode(value).decode(),
    }))["SequenceNumber"]


def _starting(kind, sequence, timestamp):
    result = {"Type": kind}
    if "SEQUENCE_NUMBER" in kind:
        result["SequenceNumber"] = sequence
    if kind == "AT_TIMESTAMP":
        result["Timestamp"] = timestamp
    return result


async def _run(response, is_cbor, on_event):
    disconnect = asyncio.Event()
    events = []

    async def receive():
        await disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        if not message.get("body"):
            return
        buffer = EventStreamBuffer()
        buffer.add_data(message["body"])
        for frame in buffer:
            assert frame.headers[":message-type"] == "event", frame.headers
            assert frame.headers[":content-type"] == (
                "application/x-amz-cbor-1.1" if is_cbor else "application/json")
            if frame.headers[":event-type"] == "initial-response":
                continue
            assert frame.headers[":event-type"] == "SubscribeToShardEvent"
            event = cbor2.loads(frame.payload) if is_cbor else json.loads(frame.payload)
            events.append(event)
            if await on_event(event, len(events)):
                disconnect.set()

    await asyncio.wait_for(response.runner(send, receive), timeout=5)
    return events


@pytest.mark.parametrize("is_cbor", [False, True], ids=["json", "cbor"])
@pytest.mark.parametrize("kind", STARTS)
def test_subscription_delivers_after_repeated_retention_and_idle_polls(subscription_state, kind, is_cbor):
    clock = subscription_state

    async def exercise():
        with request_scope("000000000000", "us-east-1"):
            consumer = await _setup()
            old = await _put(b"old")
            timestamp = clock[0]
            clock[0] += 3
            retained = await _put(b"retained")
            clock[0] += 24 * 3600 - 4
            response = await _call("SubscribeToShard", {
                "ConsumerARN": consumer, "ShardId": SHARD,
                "StartingPosition": _starting(kind, old, timestamp),
            }, is_cbor)
            expected = []

            async def on_event(event, number):
                # The two backlog records expire separately within the normal
                # five-minute subscription lifetime. Idle and append after
                # each pruning, while keeping newly delivered records alive.
                if number == 1:
                    clock[0] += 2
                    kinesis._expire_records(kinesis._streams["retention"])
                elif number == 4:
                    clock[0] += 3
                    kinesis._expire_records(kinesis._streams["retention"])
                elif number in (2, 5):
                    expected.append(await _put(f"new-{number}".encode()))
                return number == 7

            events = await _run(response, is_cbor, on_event)
            initial = ([] if kind == "LATEST" else [retained] if kind == "AFTER_SEQUENCE_NUMBER"
                       else [old, retained])
            assert [r["SequenceNumber"] for r in events[0]["Records"]] == initial
            assert [r["SequenceNumber"] for e in events[1:] for r in e["Records"]] == expected
            assert events[1]["Records"] == events[4]["Records"] == events[6]["Records"] == []
            assert events[1]["ContinuationSequenceNumber"] == events[0]["ContinuationSequenceNumber"]
            assert events[2]["ContinuationSequenceNumber"] == expected[0]
            assert events[3]["ContinuationSequenceNumber"] == expected[0]
            assert events[4]["ContinuationSequenceNumber"] == expected[0]
            assert events[5]["ContinuationSequenceNumber"] == expected[1]
            assert events[6]["ContinuationSequenceNumber"] == expected[1]
            assert all(e["MillisBehindLatest"] == 0 and "ChildShards" not in e for e in events)
            records = events[2]["Records"] + events[5]["Records"]
            assert [r["Data"] if is_cbor else base64.b64decode(r["Data"]) for r in records] == [
                b"new-2", b"new-5"]
            assert not kinesis._subscriptions

    asyncio.run(exercise())


@pytest.mark.parametrize("is_cbor", [False, True], ids=["json", "cbor"])
@pytest.mark.parametrize("kind", STARTS)
def test_subscription_keeps_checkpoint_when_all_records_expire_before_runner(subscription_state, kind, is_cbor):
    clock = subscription_state

    async def exercise():
        with request_scope("000000000000", "us-east-1"):
            consumer = await _setup()
            old = await _put(b"old")
            timestamp = clock[0]
            shard_start = kinesis._streams["retention"]["shards"][SHARD]["starting_sequence_number"]
            clock[0] += 24 * 3600 - 1
            response = await _call("SubscribeToShard", {
                "ConsumerARN": consumer, "ShardId": SHARD,
                "StartingPosition": _starting(kind, old, timestamp),
            }, is_cbor)
            clock[0] += 2
            kinesis._expire_records(kinesis._streams["retention"])

            async def on_event(event, number):
                return True

            events = await _run(response, is_cbor, on_event)
            assert events[0]["Records"] == []
            # Preserve the existing selected checkpoint value; do not invent
            # an AWS empty-shard checkpoint number.
            checkpoint = old if kind in ("LATEST", "AFTER_SEQUENCE_NUMBER") else shard_start
            assert events[0]["ContinuationSequenceNumber"] == checkpoint

    asyncio.run(exercise())


@pytest.mark.parametrize("is_cbor", [False, True], ids=["json", "cbor"])
@pytest.mark.parametrize("kind", STARTS)
def test_subscription_captures_selection_before_runner_and_pruning(subscription_state, kind, is_cbor):
    clock = subscription_state

    async def exercise():
        with request_scope("000000000000", "us-east-1"):
            consumer = await _setup()
            await _put(b"expired-prefix")
            clock[0] += 1
            selected = await _put(b"selected")
            timestamp = clock[0]
            clock[0] += 24 * 3600 - 1
            response = await _call("SubscribeToShard", {
                "ConsumerARN": consumer, "ShardId": SHARD,
                "StartingPosition": _starting(kind, selected, timestamp),
            }, is_cbor)
            # Only the prefix expires. Append between response creation and
            # runner startup, so LATEST must retain the establishment boundary.
            clock[0] += 1
            appended = await _put(b"appended")

            async def on_event(event, number):
                return True

            # Run with another request's context to exercise the saved tenant.
            with request_scope("111111111111", "eu-west-1"):
                events = await _run(response, is_cbor, on_event)
            expected = [appended] if kind in ("LATEST", "AFTER_SEQUENCE_NUMBER") else [selected, appended]
            assert [r["SequenceNumber"] for r in events[0]["Records"]] == expected
            assert events[0]["ContinuationSequenceNumber"] == appended

    asyncio.run(exercise())
