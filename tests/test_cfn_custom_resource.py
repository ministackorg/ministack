"""
CloudFormation Custom Resource integration tests.
Requires a running Ministack server at MINISTACK_ENDPOINT (default http://localhost:4566).
"""
import io
import json
import threading
import time
import urllib.request
import uuid
import zipfile

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:4566"
_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


def _regional_client(service, region):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


def _wait_stack(cfn, name, timeout=30):
    deadline = time.time() + timeout
    status = "UNKNOWN"
    while time.time() < deadline:
        try:
            stacks = cfn.describe_stacks(StackName=name)["Stacks"]
        except ClientError as exc:
            if "does not exist" in str(exc):
                return {"StackStatus": "DELETE_COMPLETE", "StackName": name}
            raise
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
        cfn.create_stack(
            StackName="cr-t02a",
            TemplateBody=_cfn_template("cr-test-prefix", resource_type="Custom::MyTester"),
        )
        stack = _wait_stack(cfn, "cr-t02a")
        assert stack["StackStatus"] == "CREATE_COMPLETE"

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


# ── Update lifecycle ───────────────────────────────────────────────────────

_CR_HANDLER_RECORD = """\
import json, urllib.request

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

        tpl_v2 = _cfn_template(
            "cr-test-record",
            extra_props={"Foo": "bar-v2"},
            outputs={
                "HasOldPropsOut": {"Value": {"Fn::GetAtt": ["CR", "HasOldProps"]}},
                "OldFooOut":      {"Value": {"Fn::GetAtt": ["CR", "OldFoo"]}},
                "NewFooOut":      {"Value": {"Fn::GetAtt": ["CR", "NewFoo"]}},
            },
        )
        cfn.update_stack(StackName="cr-t04", TemplateBody=tpl_v2)
        stack = _wait_stack(cfn, "cr-t04")
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")

        res = cfn.describe_stack_resource(StackName="cr-t04", LogicalResourceId="CR")
        assert res["StackResourceDetail"]["ResourceStatus"] == "UPDATE_COMPLETE"

        # Verify OldResourceProperties were forwarded to the Lambda on Update
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert outputs.get("HasOldPropsOut") == "True", f"OldResourceProperties missing: {outputs}"
        assert outputs.get("OldFooOut") == "bar-v1", f"OldFoo wrong: {outputs}"
        assert outputs.get("NewFooOut") == "bar-v2", f"NewFoo wrong: {outputs}"
    finally:
        cfn.delete_stack(StackName="cr-t04")
        _wait_stack(cfn, "cr-t04")
        lam.delete_function(FunctionName="cr-test-record")


def test_custom_resource_delete_sends_physical_id(cfn, lam):
    """Stack delete must send the PhysicalResourceId from Create to the Lambda."""
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


# ── Async response (Lambda returns before PUTting ResponseURL) ─────────────

_CR_HANDLER_ASYNC = """\
import json, threading, time, urllib.request

def handler(event, context):
    # Return immediately; a background thread delivers the response after a delay.
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
    try:
        cfn.create_stack(StackName="cr-t10", TemplateBody=tpl)
        stack = _wait_stack(cfn, "cr-t10")
        assert stack["StackStatus"] in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), stack
    finally:
        try:
            cfn.delete_stack(StackName="cr-t10")
            _wait_stack(cfn, "cr-t10")
        except Exception:
            pass


def test_custom_resource_rejects_cross_region_lambda_token(cfn):
    west_lam = _regional_client("lambda", "us-west-2")
    fn_name = f"cr-cross-region-{uuid.uuid4().hex[:8]}"
    stack_name = f"cr-cross-{uuid.uuid4().hex[:8]}"
    west_arn = west_lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_CR_HANDLER_SUCCESS)},
    )["FunctionArn"]

    tpl = _cfn_template(fn_name, extra_props={"ServiceToken": west_arn})
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=tpl)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] in ("ROLLBACK_COMPLETE", "CREATE_FAILED"), stack
    finally:
        try:
            cfn.delete_stack(StackName=stack_name)
            _wait_stack(cfn, stack_name)
        except Exception:
            pass
        west_lam.delete_function(FunctionName=fn_name)
