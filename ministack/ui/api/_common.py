"""Shared utilities for the UI API — JSON responses, SSE streaming, serialization."""

import asyncio
import json


async def json_response(send, data: dict, status: int = 200):
    """Send a JSON response."""
    body = json.dumps(data).encode()
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
            (b"access-control-allow-origin", b"*"),
        ],
    })
    await send({"type": "http.response.body", "body": body})


async def handle_sse(receive, send, subscribe_fn):
    """Generic SSE handler — streams events from an async generator."""
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            (b"content-type", b"text/event-stream"),
            (b"cache-control", b"no-cache"),
            (b"connection", b"keep-alive"),
            (b"access-control-allow-origin", b"*"),
        ],
    })

    await send({
        "type": "http.response.body",
        "body": b": connected\n\n",
        "more_body": True,
    })

    async def _watch_disconnect():
        while True:
            msg = await receive()
            if msg.get("type") == "http.disconnect":
                return

    disconnect_task = asyncio.create_task(_watch_disconnect())

    try:
        async for data in subscribe_fn():
            if disconnect_task.done():
                break
            payload = f"data: {json.dumps(data)}\n\n".encode()
            await send({
                "type": "http.response.body",
                "body": payload,
                "more_body": True,
            })
    except (asyncio.CancelledError, Exception):
        pass
    finally:
        disconnect_task.cancel()
        try:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        except Exception:
            pass


def safe_serialize(obj):
    """Convert an object to JSON-serializable form, handling bytes and non-serializable types."""
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except UnicodeDecodeError:
            return f"<bytes: {len(obj)} bytes>"
    if isinstance(obj, dict):
        return {k: safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [safe_serialize(v) for v in obj]
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    return str(obj)


async def binary_response(send, body: bytes, content_type: str, filename: str):
    """Send a binary file download response."""
    disp = f'attachment; filename="{filename}"'.encode()
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            (b"content-type", content_type.encode()),
            (b"content-length", str(len(body)).encode()),
            (b"content-disposition", disp),
            (b"access-control-allow-origin", b"*"),
        ],
    })
    await send({"type": "http.response.body", "body": body})


def get_query_param(query_params: dict, name: str, default: str = "") -> str:
    """Extract a single query parameter value, handling list/string formats."""
    val = query_params.get(name, default)
    if isinstance(val, list):
        return val[0] if val else default
    return val
