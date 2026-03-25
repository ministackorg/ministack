"""
Lambda Service Emulator.
Supports: CreateFunction, DeleteFunction, GetFunction, ListFunctions,
          Invoke, UpdateFunctionCode, UpdateFunctionConfiguration,
          CreateEventSourceMapping, DeleteEventSourceMapping,
          GetEventSourceMapping, ListEventSourceMappings, UpdateEventSourceMapping.
Functions are stored in-memory. Invocation runs code in a subprocess (Python only) or returns mock results.
SQS event source mappings poll the queue in a background thread and invoke the Lambda with batches.
"""

import os
import json
import time
import base64
import zipfile
import tempfile
import subprocess
import threading
import logging

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("lambda")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_functions: dict = {}       # function_name -> {config, code_zip}
_esms: dict = {}            # uuid -> esm dict
_poller_started = False
_poller_lock = threading.Lock()


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    """Route Lambda REST API requests."""

    # Parse path: /2015-03-31/functions[/name[/invocations|/code|/configuration]]
    parts = path.rstrip("/").split("/")
    # parts: ['', '2015-03-31', 'functions', ...]

    if len(parts) < 3:
        return error_response_json("InvalidRequest", "Invalid path", 400)

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    # POST /2015-03-31/functions — CreateFunction
    if method == "POST" and len(parts) == 3 and parts[2] == "functions":
        return _create_function(data)

    # GET /2015-03-31/functions — ListFunctions
    if method == "GET" and len(parts) == 3 and parts[2] == "functions":
        return _list_functions()

    # Event Source Mappings: /2015-03-31/event-source-mappings
    if len(parts) >= 3 and parts[2] == "event-source-mappings":
        esm_id = parts[3] if len(parts) > 3 else None
        if method == "POST" and not esm_id:
            return _create_esm(data)
        if method == "GET" and not esm_id:
            return _list_esms(data)
        if method == "GET" and esm_id:
            return _get_esm(esm_id)
        if method == "PUT" and esm_id:
            return _update_esm(esm_id, data)
        if method == "DELETE" and esm_id:
            return _delete_esm(esm_id)

    func_name = parts[3] if len(parts) > 3 else None
    if not func_name:
        return error_response_json("InvalidRequest", "Missing function name", 400)

    # POST /2015-03-31/functions/{name}/invocations — Invoke
    if method == "POST" and len(parts) >= 5 and parts[4] == "invocations":
        return _invoke(func_name, data, headers)

    # GET /2015-03-31/functions/{name} — GetFunction
    if method == "GET" and len(parts) == 4:
        return _get_function(func_name)

    # GET /2015-03-31/functions/{name}/configuration
    if method == "GET" and len(parts) >= 5 and parts[4] == "configuration":
        return _get_function_config(func_name)

    # DELETE /2015-03-31/functions/{name}
    if method == "DELETE" and len(parts) == 4:
        return _delete_function(func_name)

    # PUT /2015-03-31/functions/{name}/code
    if method == "PUT" and len(parts) >= 5 and parts[4] == "code":
        return _update_code(func_name, data)

    # PUT /2015-03-31/functions/{name}/configuration
    if method == "PUT" and len(parts) >= 5 and parts[4] == "configuration":
        return _update_config(func_name, data)

    return error_response_json("InvalidRequest", f"Unhandled Lambda path: {path}", 400)


# ---- Event Source Mappings ----

def _create_esm(data):
    esm_id = new_uuid()
    func_name = data.get("FunctionName", "")
    # Resolve to just the name if ARN given
    if ":" in func_name:
        func_name = func_name.split(":")[-1]

    esm = {
        "UUID": esm_id,
        "EventSourceArn": data.get("EventSourceArn", ""),
        "FunctionArn": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{func_name}",
        "FunctionName": func_name,
        "State": "Enabled",
        "BatchSize": data.get("BatchSize", 10),
        "MaximumBatchingWindowInSeconds": data.get("MaximumBatchingWindowInSeconds", 0),
        "LastModified": time.time(),
        "LastProcessingResult": "No records processed",
        "StartingPosition": data.get("StartingPosition", "LATEST"),
        "Enabled": True,
    }
    _esms[esm_id] = esm
    _ensure_poller()
    return json_response(esm)


def _get_esm(esm_id):
    esm = _esms.get(esm_id)
    if not esm:
        return error_response_json("ResourceNotFoundException", f"ESM {esm_id} not found", 404)
    return json_response(esm)


def _list_esms(data):
    func = data.get("FunctionName", "")
    if ":" in func:
        func = func.split(":")[-1]
    result = [e for e in _esms.values() if not func or e["FunctionName"] == func]
    return json_response({"EventSourceMappings": result})


def _update_esm(esm_id, data):
    esm = _esms.get(esm_id)
    if not esm:
        return error_response_json("ResourceNotFoundException", f"ESM {esm_id} not found", 404)
    if "BatchSize" in data:
        esm["BatchSize"] = data["BatchSize"]
    if "Enabled" in data:
        esm["Enabled"] = data["Enabled"]
        esm["State"] = "Enabled" if data["Enabled"] else "Disabled"
    esm["LastModified"] = time.time()
    return json_response(esm)


def _delete_esm(esm_id):
    esm = _esms.pop(esm_id, None)
    if not esm:
        return error_response_json("ResourceNotFoundException", f"ESM {esm_id} not found", 404)
    return json_response(esm)


def _ensure_poller():
    global _poller_started
    with _poller_lock:
        if not _poller_started:
            t = threading.Thread(target=_poll_loop, daemon=True)
            t.start()
            _poller_started = True


def _poll_loop():
    """Background thread: polls SQS queues for active ESMs and invokes Lambda."""
    while True:
        try:
            _poll_once()
        except Exception as e:
            logger.error(f"ESM poller error: {e}")
        time.sleep(1)


def _poll_once():
    from services import sqs as _sqs

    for esm in list(_esms.values()):
        if not esm.get("Enabled", True):
            continue

        source_arn = esm.get("EventSourceArn", "")
        if "sqs" not in source_arn:
            continue

        func_name = esm["FunctionName"]
        if func_name not in _functions:
            continue

        # Resolve queue URL from ARN: arn:aws:sqs:region:account:queue-name
        queue_name = source_arn.split(":")[-1]
        queue_url = _sqs._queue_url(queue_name)
        queue = _sqs._queues.get(queue_url)
        if not queue:
            continue

        batch_size = esm.get("BatchSize", 10)
        now = time.time()

        # Grab up to batch_size visible messages
        batch = []
        for msg in list(queue["messages"]):
            if len(batch) >= batch_size:
                break
            if msg["visible_at"] <= now and msg.get("receipt_handle") is None:
                batch.append(msg)

        if not batch:
            continue

        # Mark as in-flight
        for msg in batch:
            msg["receipt_handle"] = new_uuid()
            msg["visible_at"] = now + 30  # visibility timeout

        # Build SQS event payload
        records = []
        for msg in batch:
            records.append({
                "messageId": msg["id"],
                "receiptHandle": msg["receipt_handle"],
                "body": msg["body"],
                "attributes": {
                    "ApproximateReceiveCount": str(msg.get("receive_count", 1)),
                    "SentTimestamp": str(int(msg["sent_at"] * 1000)),
                    "SenderId": ACCOUNT_ID,
                    "ApproximateFirstReceiveTimestamp": str(int(now * 1000)),
                },
                "messageAttributes": msg.get("attributes", {}),
                "md5OfBody": msg["md5"],
                "eventSource": "aws:sqs",
                "eventSourceARN": source_arn,
                "awsRegion": REGION,
            })

        event = {"Records": records}
        result = _execute_function(_functions[func_name], event)

        if result.get("error"):
            # On failure, make messages visible again immediately
            for msg in batch:
                msg["receipt_handle"] = None
                msg["visible_at"] = now
            esm["LastProcessingResult"] = "FAILED"
            logger.warning(f"ESM: Lambda {func_name} failed processing batch from {queue_name}")
        else:
            # Success — delete processed messages
            receipt_handles = {msg["receipt_handle"] for msg in batch}
            queue["messages"] = [m for m in queue["messages"] if m.get("receipt_handle") not in receipt_handles]
            esm["LastProcessingResult"] = f"OK — {len(batch)} records"
            logger.info(f"ESM: Lambda {func_name} processed {len(batch)} messages from {queue_name}")


def _create_function(data):
    name = data.get("FunctionName")
    if not name:
        return error_response_json("InvalidParameterValueException", "FunctionName required", 400)
    if name in _functions:
        return error_response_json("ResourceConflictException", f"Function {name} already exists", 409)

    func_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"
    config = {
        "FunctionName": name,
        "FunctionArn": func_arn,
        "Runtime": data.get("Runtime", "python3.9"),
        "Role": data.get("Role", f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-role"),
        "Handler": data.get("Handler", "index.handler"),
        "CodeSize": 0,
        "Description": data.get("Description", ""),
        "Timeout": data.get("Timeout", 3),
        "MemorySize": data.get("MemorySize", 128),
        "LastModified": time.strftime("%Y-%m-%dT%H:%M:%S.000+0000"),
        "Version": "$LATEST",
        "State": "Active",
        "PackageType": "Zip",
        "Environment": data.get("Environment", {"Variables": {}}),
        "Layers": data.get("Layers", []),
    }

    code_zip = None
    code_data = data.get("Code", {})
    if "ZipFile" in code_data:
        code_zip = base64.b64decode(code_data["ZipFile"])
        config["CodeSize"] = len(code_zip)
        config["CodeSha256"] = base64.b64encode(b"fakehash").decode()

    _functions[name] = {"config": config, "code_zip": code_zip}

    return json_response(config, 201)


def _get_function(name):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)
    func = _functions[name]
    return json_response({
        "Configuration": func["config"],
        "Code": {"RepositoryType": "S3", "Location": ""},
    })


def _get_function_config(name):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)
    return json_response(_functions[name]["config"])


def _list_functions():
    funcs = [f["config"] for f in _functions.values()]
    return json_response({"Functions": funcs})


def _delete_function(name):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)
    del _functions[name]
    return 204, {}, b""


def _update_code(name, data):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)
    if "ZipFile" in data:
        code_zip = base64.b64decode(data["ZipFile"])
        _functions[name]["code_zip"] = code_zip
        _functions[name]["config"]["CodeSize"] = len(code_zip)
    _functions[name]["config"]["LastModified"] = time.strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    return json_response(_functions[name]["config"])


def _update_config(name, data):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)
    config = _functions[name]["config"]
    for key in ["Runtime", "Handler", "Description", "Timeout", "MemorySize", "Role", "Environment", "Layers"]:
        if key in data:
            config[key] = data[key]
    config["LastModified"] = time.strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    return json_response(config)


def _invoke(name, event, headers):
    if name not in _functions:
        return error_response_json("ResourceNotFoundException", f"Function {name} not found", 404)

    func = _functions[name]
    invocation_type = headers.get("x-amz-invocation-type", "RequestResponse")

    if invocation_type == "Event":
        # Async — just acknowledge
        return 202, {"X-Amz-Function-Error": ""}, b""

    # Try to actually execute the function if we have the code
    result = _execute_function(func, event)

    resp_headers = {
        "Content-Type": "application/json",
        "X-Amz-Executed-Version": "$LATEST",
    }
    if result.get("error"):
        resp_headers["X-Amz-Function-Error"] = "Unhandled"

    return 200, resp_headers, json.dumps(result.get("body", {})).encode("utf-8")


def _execute_function(func, event):
    """Attempt to execute a Python Lambda function."""
    code_zip = func.get("code_zip")
    if not code_zip:
        return {"body": {"statusCode": 200, "body": "Mock response — no code deployed"}}

    handler = func["config"]["Handler"]
    runtime = func["config"]["Runtime"]
    env_vars = func["config"].get("Environment", {}).get("Variables", {})

    if not runtime.startswith("python"):
        return {"body": {"statusCode": 200, "body": f"Mock response — {runtime} not supported for local execution"}}

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "code.zip")
            with open(zip_path, "wb") as f:
                f.write(code_zip)

            code_dir = os.path.join(tmpdir, "code")
            os.makedirs(code_dir)
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(code_dir)

            module_name, func_name = handler.rsplit(".", 1)
            event_json = json.dumps(event)

            script = f"""
import sys, json
sys.path.insert(0, '{code_dir}')
import {module_name}
event = json.loads('''{event_json}''')
context = type('Context', (), {{'function_name': '{func["config"]["FunctionName"]}', 'memory_limit_in_mb': {func["config"]["MemorySize"]}, 'invoked_function_arn': '{func["config"]["FunctionArn"]}', 'aws_request_id': '{new_uuid()}'}})()
result = {module_name}.{func_name}(event, context)
print(json.dumps(result))
"""
            env = dict(os.environ)
            env.update(env_vars)

            proc = subprocess.run(
                ["python3", "-c", script],
                capture_output=True, text=True, timeout=func["config"]["Timeout"],
                env=env,
            )

            if proc.returncode == 0:
                try:
                    return {"body": json.loads(proc.stdout.strip())}
                except json.JSONDecodeError:
                    return {"body": proc.stdout.strip()}
            else:
                return {"body": {"errorMessage": proc.stderr.strip(), "errorType": "RuntimeError"}, "error": True}

    except subprocess.TimeoutExpired:
        return {"body": {"errorMessage": "Task timed out", "errorType": "TimeoutError"}, "error": True}
    except Exception as e:
        logger.error(f"Lambda execution error: {e}")
        return {"body": {"errorMessage": str(e), "errorType": type(e).__name__}, "error": True}
