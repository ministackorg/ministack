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
