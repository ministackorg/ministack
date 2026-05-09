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
