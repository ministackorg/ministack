# CloudFormation Custom Resource Support

**Date:** 2026-05-08  
**Status:** Approved  
**Scope:** `AWS::CloudFormation::CustomResource` and `Custom::*` resource types — full Create/Update/Delete lifecycle with async callback support.

---

## Problem

Ministack currently silently no-ops `AWS::CloudFormation::CustomResource` (returns a fake physical ID with no Lambda invocation) and raises `ValueError` for any `Custom::*` type, causing immediate stack rollback. CDK stacks that use `cr.Provider` or any Lambda-backed custom resource fail to deploy.

---

## AWS Protocol (source of truth)

### What CloudFormation sends to the Lambda

CloudFormation invokes the Lambda at `ServiceToken` **synchronously** (`RequestResponse` invocation type) with this JSON event:

```json
{
  "RequestType": "Create",
  "RequestId": "<uuid>",
  "StackId": "arn:aws:cloudformation:<region>:<account>:stack/<name>/<uuid>",
  "ResponseURL": "<pre-signed S3 PUT URL>",
  "ResourceType": "Custom::MyType",
  "LogicalResourceId": "MyResource",
  "ResourceProperties": {
    "ServiceToken": "arn:aws:lambda:...:function:MyFunction",
    "Key": "value"
  }
}
```

Field presence by RequestType:

| Field | Create | Update | Delete |
|---|---|---|---|
| `RequestType` | yes | yes | yes |
| `RequestId` | yes | yes | yes |
| `StackId` | yes | yes | yes |
| `ResponseURL` | yes | yes | yes |
| `ResourceType` | yes | yes | yes |
| `LogicalResourceId` | yes | yes | yes |
| `PhysicalResourceId` | absent | yes | yes |
| `ResourceProperties` | yes | yes | yes |
| `OldResourceProperties` | absent | yes | absent |

### What the Lambda PUTs to ResponseURL

The Lambda signals completion by sending an HTTP `PUT` to `ResponseURL` with:
- `Content-Type: ""` (empty string — AWS spec requirement; S3 signature validation fails on any other value)
- `Content-Length: <byte length of body>`
- JSON body (max 4096 bytes):

```json
{
  "Status": "SUCCESS",
  "RequestId": "<echo from request>",
  "StackId": "<echo from request>",
  "LogicalResourceId": "<echo from request>",
  "PhysicalResourceId": "my-resource-physical-id",
  "Data": { "Endpoint": "https://example.com" },
  "NoEcho": false
}
```

- If `Status` is `"FAILED"`, the `Reason` field is required.
- `Data` values are accessible in the template via `Fn::GetAtt [LogicalId, Key]`.
- If `PhysicalResourceId` is absent on Create, AWS uses the CloudWatch log stream name; we use `RequestId` (equivalent safe fallback).
- On Delete, if the response Status is `"FAILED"`, the stack goes to `DELETE_FAILED`; AWS does not auto-retry.

### Timeout

Controlled by the `ServiceTimeout` property on the resource (integer, 1–3600 seconds, default 3600). If no PUT arrives within the timeout, Ministack marks the resource `CREATE_FAILED` / `UPDATE_FAILED` / `DELETE_FAILED` and triggers rollback.

### ServiceToken

Must be a Lambda function ARN (same region). SNS topic ARN is also valid in real AWS; we support Lambda only. CDK cr.Provider sets `ServiceToken` to the **framework Lambda's ARN**, not the user's handler ARN — the framework Lambda orchestrates internally.

### CDK cr.Provider async path

CDK `cr.Provider` synthesizes these CloudFormation resources:
1. `framework-onEvent` Lambda — receives the CFN event; this is the `ServiceToken`
2. User's `onEventHandler` Lambda — invoked by the framework
3. (Optional) User's `isCompleteHandler` Lambda
4. (Optional) `framework-isComplete` Lambda — calls isCompleteHandler
5. (Optional) `WaiterStateMachine` Step Functions machine — polls framework-isComplete every 5s up to 30min

When `isCompleteHandler` is defined: the framework Lambda starts the WaiterStateMachine after calling onEventHandler, then returns (without having PUT to ResponseURL). The state machine polls until `IsComplete: true`, then the `framework-isComplete` Lambda PUTs to ResponseURL.

**Ministack handles this automatically**: the framework Lambda + WaiterStateMachine are themselves CloudFormation resources that get provisioned before the custom resource. They run through Ministack's existing Lambda and Step Functions emulation. The `ResponseURL` our endpoint provides is what the CDK framework eventually PUTs to — no custom isComplete polling logic needed in Ministack.

---

## Implementation Design

### ResponseURL Intercept

Instead of a real S3 pre-signed URL, we generate:

```
http://{MINISTACK_HOST}:{GATEWAY_PORT}/_ministack/cfn-response/{token}
```

where `{token}` is a UUID generated per resource provisioning attempt.

`MINISTACK_HOST` and `GATEWAY_PORT` are read from environment variables (same as the rest of Ministack) so Docker-based Lambda runtimes resolve correctly.

### Thread Safety

The provisioner runs inside `asyncio.to_thread` so the event loop stays free to process incoming HTTP requests (the ResponseURL PUT, SFN callbacks, Lambda API calls):

```
asyncio event loop
  └─ _deploy_stack_async task
       └─ await asyncio.to_thread(_provision_resource, ...)
            ├─ _execute_function(lambda, cfn_event)   ← sync Lambda invoke in thread
            └─ threading.Event.wait(timeout)           ← blocks thread, not loop

Hypercorn ASGI (same event loop, different coroutine)
  └─ PUT /_ministack/cfn-response/{token}
       └─ deliver_response(token, body)
            └─ threading.Event.set()                   ← unblocks provisioner thread
```

`_pending_cfn_responses` is a `dict` protected by `threading.Lock`. The HTTP handler acquires the lock to store the result and set the event; the provisioner thread acquires it to read the result after unblocking.

### Files Changed

#### New: `ministack/services/cloudformation/custom_resource.py` (~120 lines)

Module-level state:
```python
_pending_cfn_responses: dict[str, dict]  # token → {event, result}
_lock: threading.Lock
```

Public functions:
- `register_token(token: str) -> threading.Event` — registers a pending response slot
- `deliver_response(token: str, response: dict) -> bool` — called by HTTP handler; stores result, sets event; returns False if token unknown
- `invoke_custom_resource(request_type, logical_id, props, stack_name, stack_id, resource_type, physical_id=None, old_props=None) -> tuple[str, dict]` — full lifecycle handler; raises on FAILED or timeout
- `reset()` — clears `_pending_cfn_responses`

`invoke_custom_resource` internal flow:
1. Extract `ServiceToken` from `props`
2. Extract function name from ARN (last segment)
3. Look up function in `_lambda_svc._functions`; raise `ValueError` if not found
4. Read `ServiceTimeout` from `props` (default 3600)
5. Generate `token = new_uuid()`
6. Build CFN event dict (all required fields per AWS spec)
7. Call `register_token(token)` → get `event_obj`
8. Call `_execute_function(func_record, cfn_event)` — synchronous
9. `event_obj.wait(timeout=service_timeout)`
10. Read result from `_pending_cfn_responses`; clean up entry
11. If timeout (result is None): raise `TimeoutError`
12. If `Status == "FAILED"`: raise `RuntimeError(reason)`
13. Determine `physical_id`: use response value or fall back to `request_id` on Create
14. Return `(physical_id, response.get("Data", {}))`

#### Modify: `ministack/services/cloudformation/provisioners.py` (~50 lines)

Add three handler functions:

```python
def _custom_resource_create(logical_id, props, stack_name, resource_type="AWS::CloudFormation::CustomResource"):
    from ministack.services.cloudformation import _stacks
    stack_id = _stacks.get(stack_name, {}).get("StackId", "")
    return _cr.invoke_custom_resource("Create", logical_id, props, stack_name, stack_id, resource_type)

def _custom_resource_update(physical_id, old_props, new_props, stack_name):
    ...

def _custom_resource_delete(physical_id, props):
    ...
```

Modify `_provision_resource()`:
```python
def _provision_resource(resource_type, logical_id, props, stack_name):
    handler = _RESOURCE_HANDLERS.get(resource_type)
    if handler and "create" in handler:
        return handler["create"](logical_id, props, stack_name)
    if resource_type.startswith("AWS::CloudFormation::"):
        logger.info("CloudFormation internal type %s — noop", resource_type)
        return f"{stack_name}-{logical_id}-noop-{new_uuid()[:8]}", {}
    # NEW: catch all Custom:: types
    if resource_type.startswith("Custom::"):
        return _custom_resource_create_typed(logical_id, props, stack_name, resource_type)
    raise ValueError(f"Unsupported resource type: {resource_type}")
```

Similarly extend `_delete_resource` and `_update_resource` to handle `Custom::*`.

Register `AWS::CloudFormation::CustomResource` in `_RESOURCE_HANDLERS`:
```python
"AWS::CloudFormation::CustomResource": {
    "create": _custom_resource_create,
    "update": _custom_resource_update,
    "delete": _custom_resource_delete,
},
```

#### Modify: `ministack/services/cloudformation/stacks.py` (~10 lines)

In `_deploy_stack_async`, detect custom resource types and run in thread pool:

```python
_ASYNC_RESOURCE_PREFIXES = ("Custom::", "AWS::CloudFormation::CustomResource")

def _is_custom_resource(rtype: str) -> bool:
    return rtype.startswith("Custom::") or rtype == "AWS::CloudFormation::CustomResource"

# In the provisioning loop:
if _is_custom_resource(resource_type):
    physical_id, attrs = await asyncio.to_thread(
        _provision_resource, resource_type, logical_id, resolved_props, stack_name
    )
else:
    physical_id, attrs = _provision_resource(resource_type, logical_id, resolved_props, stack_name)
```

Same pattern for `_update_resource` and `_delete_resource` calls.

#### Modify: `ministack/app.py` (~25 lines)

Add to `_handle_pre_body_request` (before body parsing, since we need the body):

Actually, ResponseURL PUTs have a body, so add to `_handle_post_body_shortcuts`:

```python
async def _handle_cfn_response(method, path, body):
    if method != "PUT" or not path.startswith("/_ministack/cfn-response/"):
        return None
    token = path[len("/_ministack/cfn-response/"):]
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return 400, {}, b"invalid json"
    from ministack.services.cloudformation import custom_resource
    custom_resource.deliver_response(token, payload)
    return 200, {}, b""
```

Hook into `_handle_post_body_shortcuts` as first check.

#### Modify: `ministack/services/cloudformation/__init__.py` (~3 lines)

```python
def reset():
    _stacks.clear()
    _stack_events.clear()
    _exports.clear()
    _change_sets.clear()
    from ministack.services.cloudformation import custom_resource
    custom_resource.reset()
```

---

## Test Plan

File: `tests/test_cfn_custom_resource.py`

### Fixtures

- `lambda_client` — boto3 Lambda client pointed at Ministack
- `cfn_client` — boto3 CloudFormation client pointed at Ministack
- `register_lambda(name, handler_fn)` — registers an inline Python Lambda using Ministack's local execution

### Test Cases

| Test | Scenario |
|---|---|
| `test_create_success` | Lambda responds SUCCESS with Data; stack reaches CREATE_COMPLETE; Data accessible via describe_stack_resource |
| `test_create_failed_triggers_rollback` | Lambda responds FAILED; stack rolls back to ROLLBACK_COMPLETE |
| `test_update_sends_old_properties` | Stack update; Lambda receives correct OldResourceProperties |
| `test_delete_sends_physical_id` | Stack delete; Delete event contains correct PhysicalResourceId from prior Create |
| `test_data_accessible_via_getatt` | Stack output uses `Fn::GetAtt`; output value matches Data key from Lambda |
| `test_physical_id_fallback_on_create` | Lambda response omits PhysicalResourceId; stack uses RequestId as fallback |
| `test_custom_type_prefix` | ResourceType is `Custom::Tester`; stack deploys correctly |
| `test_cfn_resource_type` | ResourceType is `AWS::CloudFormation::CustomResource`; stack deploys correctly |
| `test_async_response` | Lambda returns immediately without PUTting; background thread PUTs after 200ms — simulates CDK async pattern; stack completes |
| `test_timeout_fails_stack` | Lambda never PUTs; ServiceTimeout=2; stack fails with timeout error |
| `test_delete_noop_on_marker` | Delete with `PhysicalResourceId = "FAILED_CREATE_MARKER"` succeeds as no-op |
| `test_service_timeout_property` | `ServiceTimeout: 5` respected; short timeout triggers failure |
| `test_lambda_not_found_fails_stack` | ServiceToken ARN doesn't exist; stack fails immediately |
| `test_failed_delete_stack_status` | Lambda returns FAILED on Delete; stack goes to DELETE_FAILED |
| `test_noecho_not_an_error` | Lambda includes `NoEcho: true`; stack completes (NoEcho is cosmetic in emulator) |

---

## Out of Scope

- SNS topic as ServiceToken (Lambda only)
- `cfn-response` npm/Python helper library compatibility testing (that's tested via the Lambda tests)
- Real S3 pre-signed URL validation (we intercept before S3)
- CDK cr.Provider Step Functions gaps (separate issue if surfaced)
