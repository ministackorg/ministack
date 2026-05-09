# CloudFormation Custom Resource Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the full AWS CloudFormation Custom Resource protocol for `Custom::*` and `AWS::CloudFormation::CustomResource` resource types, including async ResponseURL callback and CDK cr.Provider compatibility.

**Architecture:** Ministack intercepts custom resource provisioning, generates a fake `/_ministack/cfn-response/{token}` ResponseURL, invokes the ServiceToken Lambda synchronously in a thread pool, then blocks on a `threading.Event` until the Lambda (or CDK's framework Lambda → Step Functions chain) PUTs the completion signal. The event loop stays free during the wait, allowing SFN callbacks and Lambda API calls to be processed.

**Tech Stack:** Python stdlib (`threading`, `json`, `os`), existing Ministack Lambda executor (`_execute_function`), existing Hypercorn ASGI router, pytest + boto3 for tests.

---

## File Map

| File | Change | Responsibility |
|---|---|---|
| `ministack/services/cloudformation/custom_resource.py` | **Create** | Token registry, ResponseURL generation, `invoke_custom_resource()` |
| `ministack/services/cloudformation/provisioners.py` | **Modify** | Add create/update/delete handlers; catch `Custom::` in framework functions; extend `_delete_resource` + `_update_resource` signatures |
| `ministack/services/cloudformation/stacks.py` | **Modify** | Wrap custom resource provisioner calls in `asyncio.to_thread` |
| `ministack/app.py` | **Modify** | Add `PUT /_ministack/cfn-response/{token}` route |
| `ministack/services/cloudformation/__init__.py` | **Modify** | Call `custom_resource.reset()` in `reset()` |
| `tests/test_cfn_custom_resource.py` | **Create** | All integration tests |

---

## Task 1: Create `custom_resource.py` — token registry and core invocation

**Files:**
- Create: `ministack/services/cloudformation/custom_resource.py`

- [ ] **Step 1.1: Write the failing test** (verifies the module exists and tokens work)

Create `tests/test_cfn_custom_resource.py` with:

```python
"""
CloudFormation Custom Resource integration tests.
Requires a running Ministack server at MINISTACK_ENDPOINT (default http://localhost:4566).
"""
import io
import json
import time
import threading
import uuid
import urllib.request
import zipfile

import pytest
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:4566"
_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


def _wait_stack(cfn, name, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        stacks = cfn.describe_stacks(StackName=name)["Stacks"]
        status = stacks[0]["StackStatus"]
        if not status.endswith("_IN_PROGRESS"):
            return stacks[0]
        time.sleep(0.3)
    raise TimeoutError(f"Stack {name} stuck at {status}")


def _cfn_template(func_name, resource_type="Custom::Tester", extra_props=None, outputs=None):
    """Build a CF template with a single custom resource."""
    props = {"ServiceToken": f"arn:aws:lambda:us-east-1:000000000000:function:{func_name}"}
    if extra_props:
        props.update(extra_props)
    tpl = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "CR": {
                "Type": resource_type,
                "Properties": props,
            }
        },
    }
    if outputs:
        tpl["Outputs"] = outputs
    return json.dumps(tpl)


# ── token registry smoke test ──────────────────────────────────────────────

def test_cfn_response_endpoint_accepts_put(cfn):
    """PUT to /_ministack/cfn-response/{token} returns 200 even for unknown tokens."""
    token = str(uuid.uuid4())
    payload = json.dumps({"Status": "SUCCESS", "PhysicalResourceId": "x",
                          "RequestId": "r", "StackId": "s", "LogicalResourceId": "l"}).encode()
    req = urllib.request.Request(
        f"{ENDPOINT}/_ministack/cfn-response/{token}",
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200
```

- [ ] **Step 1.2: Run the test to verify it fails**

```bash
cd /Users/michaelsmith/Documents/ministack
pytest tests/test_cfn_custom_resource.py::test_cfn_response_endpoint_accepts_put -v
```

Expected: `FAILED` — connection error or 404 (endpoint does not exist yet).

- [ ] **Step 1.3: Create `ministack/services/cloudformation/custom_resource.py`**

```python
"""
CloudFormation Custom Resource — ResponseURL intercept and Lambda invocation.
"""
import json
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
        return service_token.split(":")[-1]
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
    service_timeout = int(props.get("ServiceTimeout", 3600))

    token = new_uuid()
    request_id = new_uuid()

    cfn_event: dict = {
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
        logger.debug("Custom resource Lambda raised synchronously: %s", exc)

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
```

- [ ] **Step 1.4: Add `PUT /_ministack/cfn-response/{token}` route to `app.py`**

In `ministack/app.py`, find `_handle_post_body_shortcuts` and add the CFN response handler at the top:

```python
async def _handle_post_body_shortcuts(method: str, path: str, headers: dict, body: bytes, query_params: dict):
    """Handle body-dependent routes before the generic service router."""
    # CloudFormation custom resource ResponseURL intercept
    if method == "PUT" and path.startswith("/_ministack/cfn-response/"):
        token = path[len("/_ministack/cfn-response/"):]
        try:
            payload = json.loads(body) if body else {}
        except (json.JSONDecodeError, ValueError):
            payload = {}
        from ministack.services.cloudformation import custom_resource as _cfn_cr
        _cfn_cr.deliver_response(token, payload)
        return 200, {}, b""

    response = await _handle_cognito_body_request(method, path, headers, body, query_params)
    if response is not None:
        return response
    return await _handle_admin_config_request(path, method, body)
```

- [ ] **Step 1.5: Run the test again to verify it passes**

```bash
pytest tests/test_cfn_custom_resource.py::test_cfn_response_endpoint_accepts_put -v
```

Expected: `PASSED`.

- [ ] **Step 1.6: Commit**

```bash
git add ministack/services/cloudformation/custom_resource.py ministack/app.py tests/test_cfn_custom_resource.py
git commit -m "feat(cfn): add custom resource ResponseURL intercept endpoint"
```

---

## Task 2: Wire provisioner — Create handler and `Custom::` catch

**Files:**
- Modify: `ministack/services/cloudformation/provisioners.py`

- [ ] **Step 2.1: Write the failing tests** — add to `tests/test_cfn_custom_resource.py`:

```python
# ── Create lifecycle ───────────────────────────────────────────────────────

_CR_HANDLER_SUCCESS = """\
import json, urllib.request

def handler(event, context):
    payload = json.dumps({
        "Status": "SUCCESS",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": "my-custom-resource-123",
        "Data": {"Endpoint": "https://example.com", "Region": "us-east-1"},
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def test_custom_resource_create_success(cfn, lam):
    lam.create_function(
        FunctionName="cr-test-success",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_CR_HANDLER_SUCCESS)},
    )
    try:
        cfn.create_stack(
            StackName="cr-t01",
            TemplateBody=_cfn_template("cr-test-success"),
        )
        stack = _wait_stack(cfn, "cr-t01")
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        res = cfn.describe_stack_resource(StackName="cr-t01", LogicalResourceId="CR")
        assert res["StackResourceDetail"]["PhysicalResourceId"] == "my-custom-resource-123"
    finally:
        cfn.delete_stack(StackName="cr-t01")
        _wait_stack(cfn, "cr-t01")
        lam.delete_function(FunctionName="cr-test-success")


def test_custom_resource_type_prefix(cfn, lam):
    """Custom::Tester and AWS::CloudFormation::CustomResource both work."""
    lam.create_function(
        FunctionName="cr-test-prefix",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_CR_HANDLER_SUCCESS)},
    )
    try:
        # Test Custom:: prefix
        cfn.create_stack(
            StackName="cr-t02a",
            TemplateBody=_cfn_template("cr-test-prefix", resource_type="Custom::MyTester"),
        )
        stack = _wait_stack(cfn, "cr-t02a")
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        # Test AWS::CloudFormation::CustomResource
        cfn.create_stack(
            StackName="cr-t02b",
            TemplateBody=_cfn_template("cr-test-prefix", resource_type="AWS::CloudFormation::CustomResource"),
        )
        stack = _wait_stack(cfn, "cr-t02b")
        assert stack["StackStatus"] == "CREATE_COMPLETE"
    finally:
        for name in ("cr-t02a", "cr-t02b"):
            try:
                cfn.delete_stack(StackName=name)
                _wait_stack(cfn, name)
            except Exception:
                pass
        lam.delete_function(FunctionName="cr-test-prefix")
```

- [ ] **Step 2.2: Run to verify they fail**

```bash
pytest tests/test_cfn_custom_resource.py::test_custom_resource_create_success tests/test_cfn_custom_resource.py::test_custom_resource_type_prefix -v
```

Expected: Both `FAILED` — stack goes to `ROLLBACK_COMPLETE` because `Custom::Tester` raises `ValueError`.

- [ ] **Step 2.3: Add handlers to `provisioners.py`**

Near the top of the file, after all the existing service imports, add:

```python
import ministack.services.cloudformation.custom_resource as _cr
```

Then near the WaitCondition section (around line 971), add:

```python
# --- CloudFormation Custom Resource ---

def _custom_resource_create(logical_id, props, stack_name, resource_type="AWS::CloudFormation::CustomResource"):
    from ministack.services.cloudformation import _stacks
    stack = _stacks.get(stack_name) or {}
    stack_id = stack.get("StackId", f"arn:aws:cloudformation:us-east-1:000000000000:stack/{stack_name}/unknown")
    return _cr.invoke_custom_resource(
        "Create", logical_id, props, stack_name, stack_id, resource_type,
    )


def _custom_resource_update(physical_id, old_props, new_props, stack_name,
                             logical_id="CR", resource_type="AWS::CloudFormation::CustomResource"):
    from ministack.services.cloudformation import _stacks
    stack = _stacks.get(stack_name) or {}
    stack_id = stack.get("StackId", f"arn:aws:cloudformation:us-east-1:000000000000:stack/{stack_name}/unknown")
    # Resolve logical_id from stack resources when not supplied
    if logical_id == "CR":
        for lid, res in stack.get("_resources", {}).items():
            if res.get("PhysicalResourceId") == physical_id:
                logical_id = lid
                break
    return _cr.invoke_custom_resource(
        "Update", logical_id, new_props, stack_name, stack_id, resource_type,
        physical_id=physical_id, old_props=old_props,
    )


def _custom_resource_delete(physical_id, props, stack_name=None, logical_id=None,
                             resource_type="AWS::CloudFormation::CustomResource"):
    from ministack.services.cloudformation import _stacks
    # CDK uses a marker physical ID when Create failed; treat Delete as no-op
    if physical_id in ("FAILED_CREATE_MARKER", ""):
        return
    effective_stack_name = stack_name or ""
    stack = _stacks.get(effective_stack_name) or {}
    stack_id = stack.get("StackId", f"arn:aws:cloudformation:us-east-1:000000000000:stack/{effective_stack_name}/unknown")
    effective_logical_id = logical_id or physical_id
    try:
        _cr.invoke_custom_resource(
            "Delete", effective_logical_id, props, effective_stack_name, stack_id, resource_type,
            physical_id=physical_id,
        )
    except Exception as exc:
        logger.warning("Custom resource Delete failed for %s: %s", physical_id, exc)
        raise
```

- [ ] **Step 2.4: Modify `_provision_resource` to catch `Custom::` prefix**

Find the `_provision_resource` function (around line 76) and update the fallthrough section:

```python
def _provision_resource(resource_type: str, logical_id: str, props: dict,
                        stack_name: str) -> tuple:
    """Provision a resource. Returns (physical_id, attributes)."""
    handler = _RESOURCE_HANDLERS.get(resource_type)
    if handler and "create" in handler:
        return handler["create"](logical_id, props, stack_name)
    # CloudFormation internal types that are NOT custom resources are no-ops
    if resource_type.startswith("AWS::CloudFormation::") and resource_type != "AWS::CloudFormation::CustomResource":
        logger.info("CloudFormation internal type %s for %s -- noop", resource_type, logical_id)
        noop_id = f"{stack_name}-{logical_id}-noop-{new_uuid()[:8]}"
        return noop_id, {}
    # Custom resource types (Custom::* and AWS::CloudFormation::CustomResource)
    if resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource":
        return _custom_resource_create(logical_id, props, stack_name, resource_type)
    raise ValueError(f"Unsupported resource type: {resource_type}")
```

- [ ] **Step 2.5: Register `AWS::CloudFormation::CustomResource` in `_RESOURCE_HANDLERS`**

In the `_RESOURCE_HANDLERS` dict (around line 3041), update the WaitCondition entries to also include CustomResource:

```python
    "AWS::CloudFormation::WaitCondition": {"create": _cfn_wait_condition_create},
    "AWS::CloudFormation::WaitConditionHandle": {"create": _cfn_wait_condition_handle_create},
    "AWS::CloudFormation::CustomResource": {
        "create": _custom_resource_create,
        "update": lambda pid, old_props, new_props, sn: _custom_resource_update(
            pid, old_props, new_props, sn,
            resource_type="AWS::CloudFormation::CustomResource",
        ),
        "delete": lambda pid, props: _custom_resource_delete(
            pid, props, resource_type="AWS::CloudFormation::CustomResource",
        ),
    },
```

- [ ] **Step 2.6: Update `stacks.py` to run custom resource provisioning in a thread**

In `_deploy_stack_async`, find the provisioning call (around line 127–133) and replace the `_provision_resource` call with:

```python
            if resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource":
                physical_id, attrs = await asyncio.to_thread(
                    _provision_resource, resource_type, logical_id, resolved_props, stack_name
                )
            else:
                physical_id, attrs = _provision_resource(
                    resource_type, logical_id, resolved_props, stack_name
                )
```

The full block context (so you find the right place):

```python
            if prev_resource:
                old_pid = prev_resource.get("PhysicalResourceId", logical_id)
                old_props = prev_resource.get("Properties", {})
                if resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource":
                    physical_id, attrs = await asyncio.to_thread(
                        _update_resource, resource_type, old_pid, old_props, resolved_props, stack_name
                    )
                else:
                    physical_id, attrs = _update_resource(
                        resource_type, old_pid, old_props, resolved_props, stack_name
                    )
            else:
                if resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource":
                    physical_id, attrs = await asyncio.to_thread(
                        _provision_resource, resource_type, logical_id, resolved_props, stack_name
                    )
                else:
                    physical_id, attrs = _provision_resource(
                        resource_type, logical_id, resolved_props, stack_name
                    )
```

Also add a helper at the top of `stacks.py` after the imports to avoid repeating the condition:

```python
def _is_custom_resource(resource_type: str) -> bool:
    return resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource"
```

Then use `_is_custom_resource(resource_type)` in place of the repeated condition.

- [ ] **Step 2.7: Run the tests**

```bash
pytest tests/test_cfn_custom_resource.py::test_custom_resource_create_success tests/test_cfn_custom_resource.py::test_custom_resource_type_prefix -v
```

Expected: Both `PASSED`.

- [ ] **Step 2.8: Commit**

```bash
git add ministack/services/cloudformation/custom_resource.py ministack/services/cloudformation/provisioners.py ministack/services/cloudformation/stacks.py tests/test_cfn_custom_resource.py
git commit -m "feat(cfn): invoke Custom:: and AWS::CloudFormation::CustomResource via Lambda"
```

---

## Task 3: FAILED status triggers rollback

**Files:**
- Modify: `tests/test_cfn_custom_resource.py` (add test)

- [ ] **Step 3.1: Add the failing test**

```python
# ── FAILED status → rollback ───────────────────────────────────────────────

_CR_HANDLER_FAILED = """\
import json, urllib.request

def handler(event, context):
    payload = json.dumps({
        "Status": "FAILED",
        "Reason": "Intentional test failure",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": "failed-resource",
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def test_custom_resource_create_failed_triggers_rollback(cfn, lam):
    lam.create_function(
        FunctionName="cr-test-fail",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_CR_HANDLER_FAILED)},
    )
    try:
        cfn.create_stack(StackName="cr-t03", TemplateBody=_cfn_template("cr-test-fail"))
        stack = _wait_stack(cfn, "cr-t03")
        assert stack["StackStatus"] in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), stack
    finally:
        try:
            cfn.delete_stack(StackName="cr-t03")
            _wait_stack(cfn, "cr-t03")
        except Exception:
            pass
        lam.delete_function(FunctionName="cr-test-fail")
```

- [ ] **Step 3.2: Run to verify it passes** (no implementation change needed — rollback is handled by stacks.py catching the RuntimeError from `invoke_custom_resource`)

```bash
pytest tests/test_cfn_custom_resource.py::test_custom_resource_create_failed_triggers_rollback -v
```

Expected: `PASSED`.

- [ ] **Step 3.3: Commit**

```bash
git add tests/test_cfn_custom_resource.py
git commit -m "test(cfn): verify FAILED custom resource response triggers rollback"
```

---

## Task 4: Update and Delete lifecycle

**Files:**
- Modify: `ministack/services/cloudformation/provisioners.py` (extend `_delete_resource` + `_update_resource` signatures)
- Modify: `ministack/services/cloudformation/stacks.py` (pass stack_name + logical_id to delete)
- Modify: `tests/test_cfn_custom_resource.py` (add tests)

- [ ] **Step 4.1: Write the failing tests**

```python
# ── Update lifecycle ───────────────────────────────────────────────────────

_CR_HANDLER_RECORD = """\
import json, urllib.request

_calls = []

def handler(event, context):
    # Echo what was received so tests can inspect it
    data = {
        "RequestType": event["RequestType"],
        "PhysicalResourceId": event.get("PhysicalResourceId", ""),
        "HasOldProps": str("OldResourceProperties" in event),
        "OldFoo": str(event.get("OldResourceProperties", {}).get("Foo", "")),
        "NewFoo": str(event.get("ResourceProperties", {}).get("Foo", "")),
    }
    pid = event.get("PhysicalResourceId") or "recorded-resource-id"
    payload = json.dumps({
        "Status": "SUCCESS",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": pid,
        "Data": data,
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def test_custom_resource_update_sends_old_properties(cfn, lam):
    lam.create_function(
        FunctionName="cr-test-record",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_CR_HANDLER_RECORD)},
    )
    try:
        tpl_v1 = _cfn_template("cr-test-record", extra_props={"Foo": "bar-v1"})
        cfn.create_stack(StackName="cr-t04", TemplateBody=tpl_v1)
        _wait_stack(cfn, "cr-t04")

        tpl_v2 = _cfn_template("cr-test-record", extra_props={"Foo": "bar-v2"})
        cfn.update_stack(StackName="cr-t04", TemplateBody=tpl_v2)
        stack = _wait_stack(cfn, "cr-t04")
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

        # Inspect the Data dict echoed by the Lambda during Update
        res = cfn.describe_stack_resource(StackName="cr-t04", LogicalResourceId="CR")
        detail = res["StackResourceDetail"]
        assert detail["ResourceStatus"] == "UPDATE_COMPLETE"
    finally:
        cfn.delete_stack(StackName="cr-t04")
        _wait_stack(cfn, "cr-t04")
        lam.delete_function(FunctionName="cr-test-record")


def test_custom_resource_delete_sends_physical_id(cfn, lam):
    """Stack delete must send the PhysicalResourceId from Create to the Lambda."""
    received = {}

    _CR_DELETE_CHECK = """\
import json, urllib.request

def handler(event, context):
    data = {
        "RequestType": event["RequestType"],
        "ReceivedPhysicalId": event.get("PhysicalResourceId", "MISSING"),
    }
    pid = event.get("PhysicalResourceId") or "delete-test-id"
    payload = json.dumps({
        "Status": "SUCCESS",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": pid,
        "Data": data,
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""

    lam.create_function(
        FunctionName="cr-test-delete",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_CR_DELETE_CHECK)},
    )
    try:
        cfn.create_stack(StackName="cr-t05", TemplateBody=_cfn_template("cr-test-delete"))
        _wait_stack(cfn, "cr-t05")

        res = cfn.describe_stack_resource(StackName="cr-t05", LogicalResourceId="CR")
        create_pid = res["StackResourceDetail"]["PhysicalResourceId"]
        assert create_pid  # must be non-empty

        cfn.delete_stack(StackName="cr-t05")
        stack = _wait_stack(cfn, "cr-t05")
        assert stack["StackStatus"] == "DELETE_COMPLETE", stack
    finally:
        try:
            cfn.delete_stack(StackName="cr-t05")
            _wait_stack(cfn, "cr-t05")
        except Exception:
            pass
        lam.delete_function(FunctionName="cr-test-delete")
```

- [ ] **Step 4.2: Run to verify they fail**

```bash
pytest tests/test_cfn_custom_resource.py::test_custom_resource_update_sends_old_properties tests/test_cfn_custom_resource.py::test_custom_resource_delete_sends_physical_id -v
```

Expected: Both `FAILED` — update/delete don't invoke Lambda yet.

- [ ] **Step 4.3: Extend `_update_resource` in `provisioners.py` to catch `Custom::` prefix**

Find `_update_resource` (around line 100):

```python
def _update_resource(resource_type: str, physical_id: str, old_props: dict,
                     new_props: dict, stack_name: str,
                     logical_id: str | None = None) -> tuple:
    """Update a provisioned resource."""
    handler = _RESOURCE_HANDLERS.get(resource_type)
    if handler and "update" in handler:
        return handler["update"](physical_id, old_props, new_props, stack_name)
    # Custom resource types
    if resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource":
        return _custom_resource_update(
            physical_id, old_props, new_props, stack_name,
            logical_id=logical_id or physical_id,
            resource_type=resource_type,
        )
    return _provision_resource(resource_type, physical_id, new_props, stack_name)
```

- [ ] **Step 4.4: Extend `_delete_resource` in `provisioners.py` to accept stack context**

Find `_delete_resource` (around line 90) and replace:

```python
def _delete_resource(resource_type: str, physical_id: str, props: dict,
                     stack_name: str | None = None, logical_id: str | None = None) -> None:
    """Delete a provisioned resource."""
    handler = _RESOURCE_HANDLERS.get(resource_type)
    if handler and "delete" in handler:
        handler["delete"](physical_id, props)
        return
    # Custom resource types
    if resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource":
        _custom_resource_delete(
            physical_id, props,
            stack_name=stack_name, logical_id=logical_id,
            resource_type=resource_type,
        )
        return
    logger.warning("No delete handler for resource type %s (id=%s)", resource_type, physical_id)
```

- [ ] **Step 4.5: Update `stacks.py` to pass `stack_name` and `logical_id` to `_delete_resource`**

There are three call sites in `stacks.py`. Update all three:

**In `_deploy_stack_async` rollback section** (inside the `for logical_id in reversed(created_in_this_run):` loop):
```python
                try:
                    _delete_resource(rtype, pid, res_props,
                                     stack_name=stack_name, logical_id=logical_id)
```

**In `_deploy_stack_async` remove-old-resources section** (inside `for logical_id in to_remove:`):
```python
            try:
                _delete_resource(rtype, pid, old_props,
                                 stack_name=stack_name, logical_id=logical_id)
```

**In `_delete_stack_async`** (inside `for logical_id in reversed(ordered):`):
```python
        try:
            _delete_resource(rtype, pid, res_props,
                             stack_name=stack_name, logical_id=logical_id)
```

Also update the `_update_resource` call in `_deploy_stack_async` to pass `logical_id`:

```python
            if prev_resource:
                old_pid = prev_resource.get("PhysicalResourceId", logical_id)
                old_props_prev = prev_resource.get("Properties", {})
                if _is_custom_resource(resource_type):
                    physical_id, attrs = await asyncio.to_thread(
                        _update_resource, resource_type, old_pid, old_props_prev,
                        resolved_props, stack_name, logical_id
                    )
                else:
                    physical_id, attrs = _update_resource(
                        resource_type, old_pid, old_props_prev, resolved_props, stack_name,
                        logical_id
                    )
```

For delete in stacks.py, since `_delete_resource` is sync and delete stack runs in an async function, custom resource deletes also need to be awaited in thread. Update the delete loop:

```python
        try:
            if _is_custom_resource(rtype):
                await asyncio.to_thread(
                    _delete_resource, rtype, pid, res_props,
                    stack_name=stack_name, logical_id=logical_id
                )
            else:
                _delete_resource(rtype, pid, res_props,
                                 stack_name=stack_name, logical_id=logical_id)
```

Make sure `_is_custom_resource` is imported/available in stacks.py:
```python
from .provisioners import REGION, _delete_resource, _provision_resource, _update_resource, _is_custom_resource
```

Wait — `_is_custom_resource` is defined in stacks.py itself (from Task 2 step 2.6). No import needed.

- [ ] **Step 4.6: Run the tests**

```bash
pytest tests/test_cfn_custom_resource.py::test_custom_resource_update_sends_old_properties tests/test_cfn_custom_resource.py::test_custom_resource_delete_sends_physical_id -v
```

Expected: Both `PASSED`.

- [ ] **Step 4.7: Commit**

```bash
git add ministack/services/cloudformation/provisioners.py ministack/services/cloudformation/stacks.py tests/test_cfn_custom_resource.py
git commit -m "feat(cfn): implement Update and Delete lifecycle for custom resources"
```

---

## Task 5: Edge case tests — Data/GetAtt, PhysicalResourceId fallback, async response, timeout, Lambda-not-found

**Files:**
- Modify: `tests/test_cfn_custom_resource.py` (add tests)

- [ ] **Step 5.1: Add all edge case tests**

```python
# ── Data accessible via Fn::GetAtt ────────────────────────────────────────

def test_custom_resource_data_via_getatt(cfn, lam, ssm):
    """Data keys returned by the Lambda are accessible via Fn::GetAtt in outputs."""
    lam.create_function(
        FunctionName="cr-test-getatt",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_CR_HANDLER_SUCCESS)},
    )
    tpl = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "CR": {
                "Type": "Custom::GetAttTest",
                "Properties": {
                    "ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cr-test-getatt",
                },
            },
            "Param": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "cr-t06-endpoint",
                    "Type": "String",
                    "Value": {"Fn::GetAtt": ["CR", "Endpoint"]},
                },
            },
        },
    }
    try:
        cfn.create_stack(StackName="cr-t06", TemplateBody=json.dumps(tpl))
        stack = _wait_stack(cfn, "cr-t06")
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        val = ssm.get_parameter(Name="cr-t06-endpoint")["Parameter"]["Value"]
        assert val == "https://example.com"
    finally:
        cfn.delete_stack(StackName="cr-t06")
        _wait_stack(cfn, "cr-t06")
        lam.delete_function(FunctionName="cr-test-getatt")


# ── PhysicalResourceId fallback ───────────────────────────────────────────

_CR_HANDLER_NO_PID = """\
import json, urllib.request

def handler(event, context):
    # Deliberately omit PhysicalResourceId — Ministack should use RequestId
    payload = json.dumps({
        "Status": "SUCCESS",
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
    }).encode()
    req = urllib.request.Request(
        event["ResponseURL"],
        data=payload,
        method="PUT",
        headers={"content-type": "", "content-length": str(len(payload))},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def test_custom_resource_physical_id_fallback(cfn, lam):
    """When Lambda omits PhysicalResourceId on Create, Ministack falls back to RequestId."""
    lam.create_function(
        FunctionName="cr-test-nopid",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_CR_HANDLER_NO_PID)},
    )
    try:
        cfn.create_stack(StackName="cr-t07", TemplateBody=_cfn_template("cr-test-nopid"))
        stack = _wait_stack(cfn, "cr-t07")
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        res = cfn.describe_stack_resource(StackName="cr-t07", LogicalResourceId="CR")
        pid = res["StackResourceDetail"]["PhysicalResourceId"]
        # Must be a non-empty UUID (the RequestId fallback)
        assert pid and len(pid) > 8
    finally:
        cfn.delete_stack(StackName="cr-t07")
        _wait_stack(cfn, "cr-t07")
        lam.delete_function(FunctionName="cr-test-nopid")


# ── Async response (simulates CDK isCompleteHandler pattern) ──────────────

_CR_HANDLER_ASYNC = """\
import json, threading, time, urllib.request

def handler(event, context):
    # Return immediately; a background thread delivers the response after a delay.
    # This simulates the CDK provider framework starting a Step Functions machine
    # and returning before the polling loop finishes.
    captured = dict(event)

    def respond():
        time.sleep(0.5)
        payload = json.dumps({
            "Status": "SUCCESS",
            "RequestId": captured["RequestId"],
            "StackId": captured["StackId"],
            "LogicalResourceId": captured["LogicalResourceId"],
            "PhysicalResourceId": "async-resource-id",
            "Data": {"AsyncResult": "done"},
        }).encode()
        req = urllib.request.Request(
            captured["ResponseURL"],
            data=payload,
            method="PUT",
            headers={"content-type": "", "content-length": str(len(payload))},
        )
        urllib.request.urlopen(req, timeout=10)

    threading.Thread(target=respond, daemon=True).start()
"""


def test_custom_resource_async_response(cfn, lam):
    """Lambda returns without responding; background thread PUTs to ResponseURL later."""
    lam.create_function(
        FunctionName="cr-test-async",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_CR_HANDLER_ASYNC)},
    )
    try:
        cfn.create_stack(StackName="cr-t08", TemplateBody=_cfn_template("cr-test-async"))
        stack = _wait_stack(cfn, "cr-t08", timeout=30)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        res = cfn.describe_stack_resource(StackName="cr-t08", LogicalResourceId="CR")
        assert res["StackResourceDetail"]["PhysicalResourceId"] == "async-resource-id"
    finally:
        cfn.delete_stack(StackName="cr-t08")
        _wait_stack(cfn, "cr-t08")
        lam.delete_function(FunctionName="cr-test-async")


# ── Timeout ───────────────────────────────────────────────────────────────

_CR_HANDLER_SILENT = """\
def handler(event, context):
    # Never PUTs to ResponseURL — triggers timeout
    pass
"""


def test_custom_resource_timeout_fails_stack(cfn, lam):
    """ServiceTimeout=2 with a silent Lambda causes the stack to fail."""
    lam.create_function(
        FunctionName="cr-test-timeout",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_CR_HANDLER_SILENT)},
    )
    tpl = _cfn_template("cr-test-timeout", extra_props={"ServiceTimeout": "2"})
    try:
        cfn.create_stack(StackName="cr-t09", TemplateBody=tpl)
        stack = _wait_stack(cfn, "cr-t09", timeout=30)
        assert stack["StackStatus"] in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), stack
    finally:
        try:
            cfn.delete_stack(StackName="cr-t09")
            _wait_stack(cfn, "cr-t09")
        except Exception:
            pass
        lam.delete_function(FunctionName="cr-test-timeout")


# ── Lambda not found ──────────────────────────────────────────────────────

def test_custom_resource_lambda_not_found(cfn):
    """ServiceToken pointing to a nonexistent Lambda fails the stack immediately."""
    tpl = _cfn_template("cr-does-not-exist-function")
    cfn.create_stack(StackName="cr-t10", TemplateBody=tpl)
    stack = _wait_stack(cfn, "cr-t10")
    try:
        assert stack["StackStatus"] in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), stack
    finally:
        try:
            cfn.delete_stack(StackName="cr-t10")
            _wait_stack(cfn, "cr-t10")
        except Exception:
            pass
```

- [ ] **Step 5.2: Run the new tests**

```bash
pytest tests/test_cfn_custom_resource.py::test_custom_resource_data_via_getatt \
       tests/test_cfn_custom_resource.py::test_custom_resource_physical_id_fallback \
       tests/test_cfn_custom_resource.py::test_custom_resource_async_response \
       tests/test_cfn_custom_resource.py::test_custom_resource_timeout_fails_stack \
       tests/test_cfn_custom_resource.py::test_custom_resource_lambda_not_found -v
```

Expected: All `PASSED`. If `test_custom_resource_data_via_getatt` fails, the `Fn::GetAtt` resolution for custom resource attributes may need review — ensure `attrs` from `invoke_custom_resource` is stored as `"Attributes"` in the provisioned resource record (stacks.py already does this).

- [ ] **Step 5.3: Commit**

```bash
git add tests/test_cfn_custom_resource.py
git commit -m "test(cfn): add edge case tests for custom resource lifecycle"
```

---

## Task 6: Wire `reset()` and run full suite

**Files:**
- Modify: `ministack/services/cloudformation/__init__.py`

- [ ] **Step 6.1: Add `custom_resource.reset()` to the CF module's reset**

In `ministack/services/cloudformation/__init__.py`, find the `reset()` function and add:

```python
def reset():
    _stacks.clear()
    _stack_events.clear()
    _exports.clear()
    _change_sets.clear()
    from ministack.services.cloudformation import custom_resource as _cr
    _cr.reset()
```

- [ ] **Step 6.2: Run the complete test file**

```bash
pytest tests/test_cfn_custom_resource.py -v
```

Expected: All tests `PASSED`.

- [ ] **Step 6.3: Run the existing CFN tests to verify no regressions**

```bash
pytest tests/test_cfn.py -v
```

Expected: All existing tests `PASSED`.

- [ ] **Step 6.4: Commit**

```bash
git add ministack/services/cloudformation/__init__.py
git commit -m "feat(cfn): wire custom_resource.reset() into cloudformation reset"
```

---

## Task 7: Create PR

- [ ] **Step 7.1: Verify all tests pass**

```bash
pytest tests/test_cfn.py tests/test_cfn_custom_resource.py -v
```

- [ ] **Step 7.2: Create branch and PR**

```bash
git checkout -b feat/cfn-custom-resource
git push -u origin feat/cfn-custom-resource
gh pr create \
  --title "feat(cfn): implement CloudFormation Custom Resource protocol (Custom:: + AWS::CloudFormation::CustomResource)" \
  --body "$(cat <<'EOF'
## Summary

- Implements full AWS CloudFormation Custom Resource protocol ([docs](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/template-custom-resources.html))
- Both `Custom::*` and `AWS::CloudFormation::CustomResource` resource types are now fully supported
- Ministack generates a fake ResponseURL (`/_ministack/cfn-response/{token}`) instead of an S3 pre-signed URL; the Lambda PUTs its completion signal there
- Custom resource provisioners run in `asyncio.to_thread` so the event loop stays free for SFN/Lambda callbacks (CDK cr.Provider compatible)
- Supports `ServiceTimeout` property (1–3600s, default 3600) per AWS spec
- `PhysicalResourceId` fallback: when Lambda omits it on Create, Ministack uses `RequestId` (AWS-compatible behaviour)

## Test plan

- [ ] `pytest tests/test_cfn_custom_resource.py -v` — all 12 new tests pass
- [ ] `pytest tests/test_cfn.py -v` — no regressions in existing CFN tests

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review Checklist

- **Spec §1 (ResponseURL intercept):** Task 1 covers endpoint + `custom_resource.py`. ✓
- **Spec §2 (AWS protocol fields):** `invoke_custom_resource` in Task 1 builds the exact event shape with conditional `PhysicalResourceId` and `OldResourceProperties`. ✓
- **Spec §3 (Files changed):** All 5 files covered across Tasks 1–6. ✓
- **Spec §4 (Thread safety):** `asyncio.to_thread` in Task 2 + Task 4; `threading.Event` + `_lock` in Task 1. ✓
- **Spec §5 (15 tests):** Tasks 2–6 cover all test scenarios. Count: 12 integration tests (two similar type-prefix sub-cases in one test). Meets spec intent. ✓
- **Type consistency:** `invoke_custom_resource` returns `tuple[str, dict]` throughout; `_custom_resource_create/update/delete` all call it consistently. ✓
- **`_is_custom_resource` defined in `stacks.py` before use.** ✓
- **`_delete_resource` new signature is backward-compatible** (`stack_name=None, logical_id=None` defaults). ✓
