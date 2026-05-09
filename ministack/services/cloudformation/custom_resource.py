"""
CloudFormation Custom Resource — ResponseURL intercept and Lambda invocation.
"""
import logging
import os
import threading

logger = logging.getLogger("cloudformation")

_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_PORT = os.environ.get("GATEWAY_PORT", "4566")

_lock = threading.Lock()
# token → {"event": threading.Event, "result": dict | None}
_pending: dict = {}


def register_token(token: str) -> threading.Event:
    evt = threading.Event()
    with _lock:
        _pending[token] = {"event": evt, "result": None}
    return evt


def deliver_response(token: str, response: dict) -> bool:
    """Called by the HTTP handler when ResponseURL is PUT to. Returns False if token unknown."""
    with _lock:
        entry = _pending.get(token)
        if entry is None:
            return False
        entry["result"] = response
        entry["event"].set()
    return True


def reset():
    with _lock:
        _pending.clear()


def _response_url(token: str) -> str:
    return f"http://{_HOST}:{_PORT}/_ministack/cfn-response/{token}"


def _func_name_from_arn(service_token: str) -> str:
    """Extract function name from a Lambda ARN, or return as-is."""
    if service_token.startswith("arn:"):
        parts = service_token.split(":")
        # arn:aws:lambda:region:account:function:<name>[:<qualifier>]
        return parts[6] if len(parts) >= 7 else parts[-1]
    return service_token


def invoke_custom_resource(
    request_type: str,
    logical_id: str,
    props: dict,
    stack_name: str,
    stack_id: str,
    resource_type: str,
    physical_id: str | None = None,
    old_props: dict | None = None,
) -> tuple:
    """Invoke the ServiceToken Lambda and block until it PUTs to the ResponseURL.

    CALLER CONTRACT: Must be run in a worker thread (via asyncio.to_thread), never
    directly from a coroutine — it blocks on threading.Event.wait() so the event
    loop must remain free to receive the ResponseURL PUT callback.

    Returns (physical_resource_id, data_attributes).
    Raises ValueError if the Lambda is not found.
    Raises TimeoutError if no response arrives within ServiceTimeout seconds.
    Raises RuntimeError if the Lambda responds with Status=FAILED.
    """
    import ministack.services.lambda_svc as _lambda_svc
    from ministack.core.responses import new_uuid

    service_token = props.get("ServiceToken", "")
    func_name = _func_name_from_arn(service_token)

    if func_name not in _lambda_svc._functions:
        raise ValueError(
            f"Custom resource ServiceToken {service_token!r} not found. "
            "Ensure the Lambda function is provisioned before the custom resource."
        )

    func_record = _lambda_svc._functions[func_name]
    try:
        service_timeout = int(props.get("ServiceTimeout", 3600))
    except (ValueError, TypeError):
        service_timeout = 3600

    token = new_uuid()
    request_id = new_uuid()

    cfn_event = {
        "RequestType": request_type,
        "RequestId": request_id,
        "StackId": stack_id,
        "ResponseURL": _response_url(token),
        "ResourceType": resource_type,
        "LogicalResourceId": logical_id,
        "ResourceProperties": dict(props),
    }
    if physical_id is not None:
        cfn_event["PhysicalResourceId"] = physical_id
    if old_props is not None:
        cfn_event["OldResourceProperties"] = dict(old_props)

    event_obj = register_token(token)

    try:
        _lambda_svc._execute_function(func_record, cfn_event)
    except Exception as exc:
        logger.warning("Custom resource Lambda raised synchronously: %s", exc)

    signalled = event_obj.wait(timeout=service_timeout)

    with _lock:
        entry = _pending.pop(token, None)

    if not signalled or entry is None or entry["result"] is None:
        raise TimeoutError(
            f"Custom resource {logical_id!r} timed out after {service_timeout}s "
            "waiting for ResponseURL callback"
        )

    result = entry["result"]
    if result.get("Status") == "FAILED":
        raise RuntimeError(result.get("Reason", "Custom resource reported FAILED"))

    pid = result.get("PhysicalResourceId") or (physical_id if physical_id else request_id)
    return pid, result.get("Data") or {}
