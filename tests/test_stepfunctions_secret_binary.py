"""Secrets Manager binary transport at the Step Functions AWS SDK boundary.

The four read fixtures and two write representations reproduce a disposable
real-AWS Standard-workflow probe. AWS passes UTF-8 text here, not the base64
representation used by the Secrets Manager HTTP API. No encoding heuristic is
valid: a password can itself be valid base64, including valid UTF-8 when decoded.
"""

import base64
import json
import time
import uuid
from contextlib import contextmanager

import pytest


@contextmanager
def _machine(sfn, start, states):
    arn = sfn.create_state_machine(
        name=f"sfn-binary-{uuid.uuid4().hex}",
        roleArn="arn:aws:iam::000000000000:role/R",
        type="STANDARD",
        definition=json.dumps({"StartAt": start, "States": states}),
        loggingConfiguration={"level": "OFF", "includeExecutionData": False},
    )["stateMachineArn"]
    try:
        yield arn
    finally:
        sfn.delete_state_machine(stateMachineArn=arn)


def _run(sfn, arn, data):
    execution = sfn.start_execution(stateMachineArn=arn, input=json.dumps(data))["executionArn"]
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            result = sfn.describe_execution(executionArn=execution)
            if result["status"] != "RUNNING":
                assert result["status"] == "SUCCEEDED", result
                return json.loads(result["output"])
            time.sleep(0.05)
        pytest.fail("synthetic execution timed out")
    finally:
        if sfn.describe_execution(executionArn=execution)["status"] == "RUNNING":
            sfn.stop_execution(executionArn=execution)


_READ_STATES = {
    "Read": {
        "Type": "Task",
        "Resource": "arn:aws:states:::aws-sdk:secretsmanager:getSecretValue",
        "Parameters": {"SecretId.$": "$.secretId"},
        "End": True,
    },
}
_WRITE_STATES = {
    "Create": {
        "Type": "Task",
        "Resource": "arn:aws:states:::aws-sdk:secretsmanager:createSecret",
        "Parameters": {"Name.$": "$.secretId", "SecretBinary.$": "$.binary"},
        "ResultPath": None,
        "Next": "Put",
    },
    "Put": {
        "Type": "Task",
        "Resource": "arn:aws:states:::aws-sdk:secretsmanager:putSecretValue",
        "Parameters": {
            "SecretId.$": "$.secretId",
            "SecretBinary.$": "$.updatedBinary",
            "VersionStages": ["AWSPENDING"],
        },
        "End": True,
    },
}


@pytest.mark.parametrize("value", [
    pytest.param("abcdefghijklmnop", id="base64-invalid-utf8"),
    pytest.param("cmF3cGFzc3dvcmQ=", id="base64-valid-utf8"),
    pytest.param("not-base64!", id="non-base64"),
    pytest.param("quote'and\\slash", id="sql-metacharacters"),
])
def test_sfn_secret_binary_read_is_raw_text(sfn, sm, value):
    name = f"sfn-binary-{uuid.uuid4().hex}"
    created = sm.create_secret(Name=name, SecretBinary=value.encode("utf-8"))
    try:
        before = sm.get_secret_value(SecretId=name)
        assert before["SecretBinary"] == value.encode("utf-8")
        with _machine(sfn, "Read", _READ_STATES) as arn:
            result = _run(sfn, arn, {"secretId": name})
        assert result["SecretBinary"] == value
        assert result["SecretBinary"] != base64.b64encode(before["SecretBinary"]).decode("ascii")
        assert result["VersionId"] == created["VersionId"]
        assert "SecretString" not in result
        after = sm.get_secret_value(SecretId=name)
        assert after["SecretBinary"] == before["SecretBinary"]
        assert after["VersionId"] == before["VersionId"]
    finally:
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)


@pytest.mark.parametrize("representation", ["raw", "base64"])
def test_sfn_secret_binary_writes_store_supplied_text(sfn, sm, representation):
    name = f"sfn-binary-{uuid.uuid4().hex}"
    original, updated = "abcdefghijklmnop", "not-base64!-updated"
    if representation == "base64":
        supplied = base64.b64encode(original.encode()).decode("ascii")
        supplied_update = base64.b64encode(updated.encode()).decode("ascii")
    else:
        supplied, supplied_update = original, updated
    try:
        with _machine(sfn, "Create", _WRITE_STATES) as arn:
            result = _run(sfn, arn, {
                "secretId": name, "binary": supplied, "updatedBinary": supplied_update,
            })
        current = sm.get_secret_value(SecretId=name, VersionStage="AWSCURRENT")
        pending = sm.get_secret_value(SecretId=name, VersionStage="AWSPENDING")
        assert current["SecretBinary"] == supplied.encode("utf-8")
        assert pending["SecretBinary"] == supplied_update.encode("utf-8")
        assert (current["SecretBinary"] == original.encode()) == (representation == "raw")
        assert (pending["SecretBinary"] == updated.encode()) == (representation == "raw")
        assert current["VersionId"] != pending["VersionId"]
        assert pending["VersionId"] == result["VersionId"]
        assert current["VersionStages"] == ["AWSCURRENT"]
        assert pending["VersionStages"] == ["AWSPENDING"]
    finally:
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)


def test_sfn_secret_string_is_not_transformed(sfn, sm):
    name = f"sfn-binary-{uuid.uuid4().hex}"
    value = '{"password":"cmF3cGFzc3dvcmQ="}'
    sm.create_secret(Name=name, SecretString=value)
    try:
        with _machine(sfn, "Read", _READ_STATES) as arn:
            result = _run(sfn, arn, {"secretId": name})
        assert result["SecretString"] == value
        assert "SecretBinary" not in result
    finally:
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)


def test_direct_secret_binary_sdk_keeps_arbitrary_bytes(sm):
    """The HTTP API must still base64-encode binary, including non-UTF-8 bytes."""
    name = f"sfn-binary-{uuid.uuid4().hex}"
    value = bytes(range(256))
    sm.create_secret(Name=name, SecretBinary=value)
    try:
        assert sm.get_secret_value(SecretId=name)["SecretBinary"] == value
        updated = sm.put_secret_value(SecretId=name, SecretBinary=value[::-1])
        result = sm.get_secret_value(SecretId=name)
        assert result["SecretBinary"] == value[::-1]
        assert result["VersionId"] == updated["VersionId"]
    finally:
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
