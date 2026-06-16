import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()

_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"

def _wait_sfn(sfn, exec_arn, timeout=10):
    """Poll DescribeExecution until terminal state."""
    for _ in range(int(timeout / 0.1)):
        time.sleep(0.1)
        desc = sfn.describe_execution(executionArn=exec_arn)
        if desc["status"] != "RUNNING":
            return desc
    return desc

def test_sfn_create_execute(sfn):
    definition = json.dumps(
        {
            "Comment": "Simple state machine",
            "StartAt": "HelloWorld",
            "States": {"HelloWorld": {"Type": "Pass", "End": True}},
        }
    )
    resp = sfn.create_state_machine(
        name="test-machine",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/StepFunctionsRole",
    )
    sm_arn = resp["stateMachineArn"]
    exec_resp = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"key": "value"}))
    exec_arn = exec_resp["executionArn"]
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] in ("RUNNING", "SUCCEEDED")

def test_sfn_list(sfn):
    machines = sfn.list_state_machines()
    assert any(m["name"] == "test-machine" for m in machines["stateMachines"])
    sm_arn = next(m["stateMachineArn"] for m in machines["stateMachines"] if m["name"] == "test-machine")
    execs = sfn.list_executions(stateMachineArn=sm_arn)
    assert len(execs["executions"]) >= 1

def test_sfn_create_state_machine_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "Init",
            "States": {"Init": {"Type": "Pass", "Result": "ok", "End": True}},
        }
    )
    resp = sfn.create_state_machine(
        name="sfn-csm-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    assert "stateMachineArn" in resp
    assert "sfn-csm-v2" in resp["stateMachineArn"]

def test_sfn_list_state_machines_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "X",
            "States": {"X": {"Type": "Pass", "End": True}},
        }
    )
    sfn.create_state_machine(
        name="sfn-ls-v2a",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    sfn.create_state_machine(
        name="sfn-ls-v2b",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn.list_state_machines()
    names = [m["name"] for m in resp["stateMachines"]]
    assert "sfn-ls-v2a" in names
    assert "sfn-ls-v2b" in names

def test_sfn_describe_state_machine_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "D",
            "States": {"D": {"Type": "Pass", "End": True}},
        }
    )
    create = sfn.create_state_machine(
        name="sfn-desc-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn.describe_state_machine(stateMachineArn=create["stateMachineArn"])
    assert resp["name"] == "sfn-desc-v2"
    assert resp["status"] == "ACTIVE"
    assert resp["definition"] == definition
    assert resp["roleArn"] == "arn:aws:iam::000000000000:role/R"

def test_sfn_start_execution_pass_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "P",
            "States": {"P": {"Type": "Pass", "Result": {"msg": "done"}, "End": True}},
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-pass-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")
    for _ in range(50):
        time.sleep(0.1)
        desc = sfn.describe_execution(executionArn=ex["executionArn"])
        if desc["status"] != "RUNNING":
            break
    assert desc["status"] == "SUCCEEDED"
    assert json.loads(desc["output"]) == {"msg": "done"}

def test_sfn_execution_choice_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "Check",
            "States": {
                "Check": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.x", "NumericEquals": 1, "Next": "One"},
                        {"Variable": "$.x", "NumericGreaterThan": 1, "Next": "Many"},
                    ],
                    "Default": "Zero",
                },
                "One": {"Type": "Pass", "Result": "one", "End": True},
                "Many": {"Type": "Pass", "Result": "many", "End": True},
                "Zero": {"Type": "Pass", "Result": "zero", "End": True},
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-choice-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    arn = sm["stateMachineArn"]

    ex1 = sfn.start_execution(stateMachineArn=arn, input='{"x":1}')
    for _ in range(50):
        time.sleep(0.1)
        d1 = sfn.describe_execution(executionArn=ex1["executionArn"])
        if d1["status"] != "RUNNING":
            break
    assert d1["status"] == "SUCCEEDED"
    assert json.loads(d1["output"]) == "one"

    ex2 = sfn.start_execution(stateMachineArn=arn, input='{"x":5}')
    for _ in range(50):
        time.sleep(0.1)
        d2 = sfn.describe_execution(executionArn=ex2["executionArn"])
        if d2["status"] != "RUNNING":
            break
    assert d2["status"] == "SUCCEEDED"
    assert json.loads(d2["output"]) == "many"

    ex3 = sfn.start_execution(stateMachineArn=arn, input='{"x":0}')
    for _ in range(50):
        time.sleep(0.1)
        d3 = sfn.describe_execution(executionArn=ex3["executionArn"])
        if d3["status"] != "RUNNING":
            break
    assert d3["status"] == "SUCCEEDED"
    assert json.loads(d3["output"]) == "zero"

def test_sfn_stop_execution_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "W",
            "States": {"W": {"Type": "Wait", "Seconds": 120, "End": True}},
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-stop-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"])
    time.sleep(0.3)
    sfn.stop_execution(executionArn=ex["executionArn"], error="UserAbort", cause="test stop")
    desc = sfn.describe_execution(executionArn=ex["executionArn"])
    assert desc["status"] == "ABORTED"

def test_sfn_get_execution_history_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "A",
            "States": {
                "A": {"Type": "Pass", "Next": "B"},
                "B": {"Type": "Pass", "End": True},
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-hist-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")
    for _ in range(50):
        time.sleep(0.1)
        desc = sfn.describe_execution(executionArn=ex["executionArn"])
        if desc["status"] != "RUNNING":
            break
    assert desc["status"] == "SUCCEEDED"

    history = sfn.get_execution_history(executionArn=ex["executionArn"])
    types = [e["type"] for e in history["events"]]
    assert "ExecutionStarted" in types
    assert "ExecutionSucceeded" in types
    assert any("Pass" in t for t in types)

def test_sfn_tags_v2(sfn):
    definition = json.dumps(
        {
            "StartAt": "T",
            "States": {"T": {"Type": "Pass", "End": True}},
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-tag-v2",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
        tags=[{"key": "init", "value": "yes"}],
    )
    arn = sm["stateMachineArn"]
    tags = sfn.list_tags_for_resource(resourceArn=arn)["tags"]
    assert any(t["key"] == "init" and t["value"] == "yes" for t in tags)

    sfn.tag_resource(resourceArn=arn, tags=[{"key": "env", "value": "test"}])
    tags2 = sfn.list_tags_for_resource(resourceArn=arn)["tags"]
    assert any(t["key"] == "env" for t in tags2)

    sfn.untag_resource(resourceArn=arn, tagKeys=["init"])
    tags3 = sfn.list_tags_for_resource(resourceArn=arn)["tags"]
    assert not any(t["key"] == "init" for t in tags3)
    assert any(t["key"] == "env" for t in tags3)

def test_sfn_intrinsic_string_to_json(sfn, sfn_sync):
    """States.StringToJson parses a JSON string into structured data."""
    definition = json.dumps({
        "StartAt": "Parse",
        "States": {
            "Parse": {
                "Type": "Pass",
                "Parameters": {
                    "parsed.$": "States.StringToJson($.raw)"
                },
                "End": True,
            }
        },
    })
    sm = sfn.create_state_machine(
        name="sfn-intrinsic-s2j",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn_sync.start_sync_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"raw": '{"a":1,"b":2}'}),
    )
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    assert output["parsed"] == {"a": 1, "b": 2}

def test_sfn_pass_parameters_resolve_context_object(sfn):
    """Pass state Parameters can resolve Step Functions context object paths."""
    unique = str(time.time_ns())
    execution_name = f"pass-context-{unique}"
    definition = json.dumps({
        "StartAt": "Build",
        "States": {
            "Build": {
                "Type": "Pass",
                "Parameters": {
                    "executionName.$": "$$.Execution.Name",
                    "inputValue.$": "$.value",
                },
                "End": True,
            }
        },
    })
    sm = sfn.create_state_machine(
        name=f"sfn-pass-context-{unique}",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(
        stateMachineArn=sm["stateMachineArn"],
        name=execution_name,
        input=json.dumps({"value": "from-input"}),
    )

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    assert json.loads(desc["output"]) == {
        "executionName": execution_name,
        "inputValue": "from-input",
    }

def test_sfn_intrinsic_json_merge(sfn, sfn_sync):
    """States.JsonMerge shallow-merges two objects."""
    definition = json.dumps({
        "StartAt": "Merge",
        "States": {
            "Merge": {
                "Type": "Pass",
                "Parameters": {
                    "merged.$": "States.JsonMerge($.obj1, $.obj2, false)"
                },
                "End": True,
            }
        },
    })
    sm = sfn.create_state_machine(
        name="sfn-intrinsic-jm",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn_sync.start_sync_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"obj1": {"a": 1, "c": 3}, "obj2": {"b": 2, "c": 99}}),
    )
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    assert output["merged"] == {"a": 1, "b": 2, "c": 99}

def test_sfn_intrinsic_format(sfn, sfn_sync):
    """States.Format interpolates arguments into a template string."""
    definition = json.dumps({
        "StartAt": "Fmt",
        "States": {
            "Fmt": {
                "Type": "Pass",
                "Parameters": {
                    "greeting.$": "States.Format('Hello {} from {}', $.name, $.city)"
                },
                "End": True,
            }
        },
    })
    sm = sfn.create_state_machine(
        name="sfn-intrinsic-fmt",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn_sync.start_sync_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"name": "Jay", "city": "SF"}),
    )
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    assert output["greeting"] == "Hello Jay from SF"

def test_sfn_intrinsic_format_escapes(sfn, sfn_sync):
    """States.Format handles \\' \\{ \\} \\\\ escapes in template only."""
    definition = json.dumps({
        "StartAt": "Fmt",
        "States": {
            "Fmt": {
                "Type": "Pass",
                "Parameters": {
                    "quoted.$": "States.Format('it\\'s {}', $.x)",
                    "braces.$": "States.Format('\\{literal\\}')",
                    "backslash.$": "States.Format('C:\\\\tmp')",
                    "preserved.$": "States.Format('path: {}', $.path)",
                },
                "End": True,
            }
        },
    })
    sm = sfn.create_state_machine(
        name="sfn-intrinsic-fmt-esc",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn_sync.start_sync_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"x": "fine", "path": "C:\\tmp\\file"}),
    )
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    assert output["quoted"] == "it's fine"
    assert output["braces"] == "{literal}"
    assert output["backslash"] == "C:\\tmp"
    assert output["preserved"] == "path: C:\\tmp\\file"

def test_sfn_intrinsic_nested(sfn, sfn_sync):
    """Nested intrinsic: States.StringToJson(States.Format(...))"""
    definition = json.dumps({
        "StartAt": "Nested",
        "States": {
            "Nested": {
                "Type": "Pass",
                "Parameters": {
                    "result.$": "States.StringToJson(States.Format('{\"key\":\"{}\"}', $.val))"
                },
                "End": True,
            }
        },
    })
    sm = sfn.create_state_machine(
        name="sfn-intrinsic-nested",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn_sync.start_sync_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"val": "hello"}),
    )
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    assert output["result"] == {"key": "hello"}

def test_sfn_aws_sdk_secretsmanager_create_and_get(sfn, sfn_sync, sm):
    """aws-sdk:secretsmanager integration creates and retrieves a secret."""
    import uuid as _uuid

    secret_name = f"sfn-sdk-test-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-sm-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "CreateSecret",
        "States": {
            "CreateSecret": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:secretsmanager:CreateSecret",
                "Parameters": {
                    "Name": secret_name,
                    "SecretString": "hunter2",
                },
                "ResultPath": "$.createResult",
                "Next": "DescribeSecret",
            },
            "DescribeSecret": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:secretsmanager:DescribeSecret",
                "Parameters": {
                    "SecretId": secret_name,
                },
                "ResultPath": "$.describeResult",
                "Next": "Done",
            },
            "Done": {"Type": "Succeed"},
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])
    assert "createResult" in output
    assert output["createResult"]["Name"] == secret_name
    assert "describeResult" in output
    assert output["describeResult"]["Name"] == secret_name

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_jsonata_arguments_output_and_catch_output(sfn, sfn_sync, sm):
    """JSONata Task states evaluate Arguments and Output for aws-sdk integrations."""
    import uuid as _uuid

    secret_name = f"sfn-jsonata-secret-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sfn-jsonata-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "CreateSecret",
        "States": {
            "CreateSecret": {
                "Type": "Task",
                "QueryLanguage": "JSONata",
                "Resource": "arn:aws:states:::aws-sdk:secretsmanager:createSecret",
                "Arguments": "{% $merge([{'Name': $states.input.secretName, 'SecretString': $states.input.secretValue}, $states.input.kmsKeyId != '' ? {'KmsKeyId': $states.input.kmsKeyId} : {}]) %}",
                "Output": "{% $merge([$states.input, {'createResult': $states.result}]) %}",
                "Next": "CreateDuplicate",
            },
            "CreateDuplicate": {
                "Type": "Task",
                "QueryLanguage": "JSONata",
                "Resource": "arn:aws:states:::aws-sdk:secretsmanager:createSecret",
                "Arguments": {
                    "Name": "{% $states.input.secretName %}",
                    "SecretString": "duplicate",
                },
                "Catch": [
                    {
                        "ErrorEquals": ["SecretsManager.ResourceExistsException"],
                        "Output": "{% $merge([$states.input, {'createError': $states.errorOutput}]) %}",
                        "Next": "Done",
                    }
                ],
                "End": True,
            },
            "Done": {"Type": "Succeed"},
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    try:
        resp = sfn_sync.start_sync_execution(
            stateMachineArn=sm_arn,
            input=json.dumps({
                "secretName": secret_name,
                "secretValue": "jsonata-value",
                "kmsKeyId": "",
            }),
        )
        assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} - {resp.get('cause')}"
        output = json.loads(resp["output"])

        assert output["createResult"]["Name"] == secret_name
        assert output["createError"]["Error"] == "SecretsManager.ResourceExistsException"
        assert sm.get_secret_value(SecretId=secret_name)["SecretString"] == "jsonata-value"
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)
        try:
            sm.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)
        except ClientError:
            pass


def test_sfn_jsonata_pass_output_string_concat_and_arithmetic(sfn, sfn_sync):
    """JSONata Pass state evaluates Output with `&` concat and `*` arithmetic
    (the exact form from issue #636 Test 1)."""
    import uuid as _uuid

    sm_name = f"sfn-jsonata-pass-{_uuid.uuid4().hex[:8]}"
    definition = json.dumps({
        "QueryLanguage": "JSONata",
        "StartAt": "Transform",
        "States": {
            "Transform": {
                "Type": "Pass",
                "Output": {
                    "greeting": "{% 'Hello, ' & $states.input.name & '!' %}",
                    "doubled": "{% $states.input.value * 2 %}",
                },
                "End": True,
            }
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]
    try:
        resp = sfn_sync.start_sync_execution(
            stateMachineArn=sm_arn,
            input=json.dumps({"name": "World", "value": 21}),
        )
        assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} - {resp.get('cause')}"
        out = json.loads(resp["output"])
        assert out == {"greeting": "Hello, World!", "doubled": 42}
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_jsonata_choice_condition_routes_branches(sfn, sfn_sync):
    """JSONata Choice state evaluates per-branch `Condition` and routes correctly
    (issue #636 Test 3)."""
    import uuid as _uuid

    sm_name = f"sfn-jsonata-choice-{_uuid.uuid4().hex[:8]}"
    definition = json.dumps({
        "QueryLanguage": "JSONata",
        "StartAt": "Route",
        "States": {
            "Route": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Condition": "{% $states.input.value > 10 %}",
                        "Next": "BigValue",
                    }
                ],
                "Default": "SmallValue",
            },
            "BigValue": {
                "Type": "Pass",
                "Output": {"branch": "big"},
                "End": True,
            },
            "SmallValue": {
                "Type": "Pass",
                "Output": {"branch": "small"},
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]
    try:
        big = sfn_sync.start_sync_execution(
            stateMachineArn=sm_arn,
            input=json.dumps({"value": 50}),
        )
        assert big["status"] == "SUCCEEDED"
        assert json.loads(big["output"]) == {"branch": "big"}

        small = sfn_sync.start_sync_execution(
            stateMachineArn=sm_arn,
            input=json.dumps({"value": 5}),
        )
        assert small["status"] == "SUCCEEDED"
        assert json.loads(small["output"]) == {"branch": "small"}
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_jsonata_choice_with_per_branch_output(sfn, sfn_sync):
    """Per-branch JSONata Output transforms input on the matched Choice rule."""
    import uuid as _uuid

    sm_name = f"sfn-jsonata-choice-output-{_uuid.uuid4().hex[:8]}"
    definition = json.dumps({
        "QueryLanguage": "JSONata",
        "StartAt": "Route",
        "States": {
            "Route": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Condition": "{% $states.input.priority = 'high' %}",
                        "Output": {"tier": "premium", "name": "{% $states.input.name %}"},
                        "Next": "End",
                    }
                ],
                "Default": "End",
            },
            "End": {"Type": "Pass", "End": True},
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]
    try:
        resp = sfn_sync.start_sync_execution(
            stateMachineArn=sm_arn,
            input=json.dumps({"name": "Alice", "priority": "high"}),
        )
        assert resp["status"] == "SUCCEEDED"
        assert json.loads(resp["output"]) == {"tier": "premium", "name": "Alice"}
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_dynamodb_put_and_get(sfn, sfn_sync, ddb):
    """aws-sdk:dynamodb integration puts and gets an item."""
    import uuid as _uuid

    table_name = f"sfn-sdk-ddb-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-ddb-{_uuid.uuid4().hex[:8]}"

    ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    definition = json.dumps({
        "StartAt": "PutItem",
        "States": {
            "PutItem": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:dynamodb:PutItem",
                "Parameters": {
                    "TableName": table_name,
                    "Item": {
                        "pk": {"S": "key1"},
                        "data": {"S": "hello"},
                    },
                },
                "ResultPath": "$.putResult",
                "Next": "GetItem",
            },
            "GetItem": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:dynamodb:GetItem",
                "Parameters": {
                    "TableName": table_name,
                    "Key": {
                        "pk": {"S": "key1"},
                    },
                },
                "ResultPath": "$.getResult",
                "Next": "Done",
            },
            "Done": {"Type": "Succeed"},
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])
    assert "getResult" in output
    item = output["getResult"].get("Item", {})
    assert item.get("pk", {}).get("S") == "key1"
    assert item.get("data", {}).get("S") == "hello"

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)

def test_sfn_aws_sdk_unknown_service_fails(sfn, sfn_sync):
    """aws-sdk integration with unsupported service returns clean error."""
    import uuid as _uuid

    sm_name = f"sdk-unknown-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "BadCall",
        "States": {
            "BadCall": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:neptune:DescribeDBClusters",
                "Parameters": {},
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "FAILED"
    assert "neptune" in resp.get("cause", "").lower() or "neptune" in resp.get("error", "").lower()

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_lambda_get_alias_and_configuration(sfn_sync, lam):
    """aws-sdk:lambda getAlias/getFunctionConfiguration supports JSONPath params."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"sfn-sdk-lambda-{suffix}"
    sm_name = f"sfn-sdk-lambda-{suffix}"

    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip("def handler(e, c): return {'ok': True}")},
    )
    version = lam.publish_version(FunctionName=fn)["Version"]
    lam.create_alias(FunctionName=fn, Name="live", FunctionVersion=version)

    function_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"
    definition = json.dumps({
        "StartAt": "ReadAlias",
        "States": {
            "ReadAlias": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:lambda:getAlias",
                "Parameters": {
                    "FunctionName.$": "$.functionName",
                    "Name.$": "$.aliasName",
                },
                "ResultPath": "$.alias",
                "Next": "ReadConfig",
            },
            "ReadConfig": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:lambda:getFunctionConfiguration",
                "Parameters": {
                    "FunctionName.$": "$.functionName",
                    "Qualifier.$": "$.alias.FunctionVersion",
                },
                "ResultPath": "$.config",
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(
        stateMachineArn=sm_arn,
        input=json.dumps({"functionName": function_arn, "aliasName": "live"}),
    )
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])
    assert output["alias"]["Name"] == "live"
    assert output["alias"]["FunctionVersion"] == version
    assert output["config"]["FunctionName"] == fn
    assert output["config"]["Version"] == version


def test_sfn_aws_sdk_lambda_respects_caller_account():
    """aws-sdk:lambda dispatch threads the caller's account through to the
    Lambda lookup. Previously the dispatcher hardcoded ``Credential=test/...``
    in its synthetic Authorization header, so an SFN execution running under
    a non-default 12-digit account would resolve into the default account
    and fail to find Lambdas created in the caller's own account.
    """
    import boto3
    from botocore.config import Config as _Config

    ACCOUNT = "987654321098"
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"sfn-acct-{suffix}"
    sm_name = f"sfn-acct-{suffix}"

    def _client(service):
        return boto3.client(
            service,
            endpoint_url="http://localhost:4566",
            aws_access_key_id=ACCOUNT,
            aws_secret_access_key="secret",
            region_name="us-east-1",
            config=_Config(
                retries={"mode": "standard"},
                # SFN's start_sync_execution prepends `sync-` to the host; the
                # default sfn_sync fixture disables that, we need the same.
                inject_host_prefix=False,
            ),
        )

    lam_a = _client("lambda")
    sfn_a = _client("stepfunctions")

    lam_a.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip("def handler(e, c): return {'ok': True}")},
    )

    function_arn = f"arn:aws:lambda:us-east-1:{ACCOUNT}:function:{fn}"
    definition = json.dumps({
        "StartAt": "ReadConfig",
        "States": {
            "ReadConfig": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:lambda:getFunctionConfiguration",
                "Parameters": {"FunctionName.$": "$.functionName"},
                "End": True,
            },
        },
    })
    sm_arn = sfn_a.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn=f"arn:aws:iam::{ACCOUNT}:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_a.start_sync_execution(
        stateMachineArn=sm_arn,
        input=json.dumps({"functionName": function_arn}),
    )
    assert resp["status"] == "SUCCEEDED", (
        f"Execution failed under non-default account: "
        f"{resp.get('error')} — {resp.get('cause')}"
    )
    output = json.loads(resp["output"])
    assert output["FunctionName"] == fn
    # FunctionArn must come back scoped to OUR account, not the default
    assert output["FunctionArn"] == function_arn

    lam_a.delete_function(FunctionName=fn)
    sfn_a.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_optimized_start_execution_returns_execution_arn(sfn, sfn_sync):
    """Optimized states:startExecution returns the child ExecutionArn."""
    suffix = _uuid_mod.uuid4().hex[:8]
    child = sfn.create_state_machine(
        name=f"sfn-child-opt-{suffix}",
        definition=json.dumps({
            "StartAt": "Done",
            "States": {"Done": {"Type": "Pass", "End": True}},
        }),
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    parent = sfn.create_state_machine(
        name=f"sfn-parent-opt-{suffix}",
        definition=json.dumps({
            "StartAt": "Child",
            "States": {
                "Child": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::states:startExecution",
                    "Parameters": {
                        "StateMachineArn": child["stateMachineArn"],
                        "Input": "{\"source\":\"optimized\"}",
                    },
                    "ResultPath": "$.child",
                    "End": True,
                },
            },
        }),
        roleArn="arn:aws:iam::000000000000:role/R",
    )

    resp = sfn_sync.start_sync_execution(
        stateMachineArn=parent["stateMachineArn"],
        input="{}",
    )
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])
    child_arn = output["child"]["ExecutionArn"]
    assert child_arn.startswith("arn:aws:states:us-east-1:000000000000:execution:sfn-child-opt-")
    assert "StartDate" in output["child"]

    sfn.describe_execution(executionArn=child_arn)
    sfn_sync.delete_state_machine(stateMachineArn=parent["stateMachineArn"])
    sfn_sync.delete_state_machine(stateMachineArn=child["stateMachineArn"])


def test_sfn_aws_sdk_sfn_start_execution_accepts_pascal_case(sfn, sfn_sync):
    """aws-sdk:sfn:startExecution accepts Step Functions PascalCase Parameters."""
    suffix = _uuid_mod.uuid4().hex[:8]
    child = sfn.create_state_machine(
        name=f"sfn-child-sdk-{suffix}",
        definition=json.dumps({
            "StartAt": "Done",
            "States": {"Done": {"Type": "Pass", "End": True}},
        }),
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    parent = sfn.create_state_machine(
        name=f"sfn-parent-sdk-{suffix}",
        definition=json.dumps({
            "StartAt": "Child",
            "States": {
                "Child": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:sfn:startExecution",
                    "Parameters": {
                        "StateMachineArn": child["stateMachineArn"],
                        "Input": "{\"source\":\"aws-sdk\"}",
                    },
                    "ResultPath": "$.child",
                    "End": True,
                },
            },
        }),
        roleArn="arn:aws:iam::000000000000:role/R",
    )

    resp = sfn_sync.start_sync_execution(
        stateMachineArn=parent["stateMachineArn"],
        input="{}",
    )
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])
    child_arn = output["child"]["ExecutionArn"]
    assert child_arn.startswith("arn:aws:states:us-east-1:000000000000:execution:sfn-child-sdk-")
    assert "StartDate" in output["child"]

    sfn.describe_execution(executionArn=child_arn)
    sfn_sync.delete_state_machine(stateMachineArn=parent["stateMachineArn"])
    sfn_sync.delete_state_machine(stateMachineArn=child["stateMachineArn"])


def test_sfn_aws_sdk_rds_create_and_describe_cluster(sfn, sfn_sync):
    """aws-sdk:rds CreateDBCluster + DescribeDBClusters via query-protocol dispatch."""
    import uuid as _uuid

    cluster_id = f"sfn-rds-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-rds-create-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "CreateCluster",
        "States": {
            "CreateCluster": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:CreateDBCluster",
                "Parameters": {
                    "DBClusterIdentifier": cluster_id,
                    "Engine": "aurora-postgresql",
                    "MasterUsername": "admin",
                    "MasterUserPassword": "testpass123",
                },
                "ResultPath": "$.createResult",
                "Next": "DescribeClusters",
            },
            "DescribeClusters": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:DescribeDBClusters",
                "Parameters": {
                    "DBClusterIdentifier": cluster_id,
                },
                "ResultPath": "$.describeResult",
                "Next": "Done",
            },
            "Done": {"Type": "Succeed"},
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])

    # Verify create result contains the cluster (SFN SDK convention keys)
    create_cluster = output["createResult"]["DbCluster"]
    assert create_cluster["DbClusterIdentifier"] == cluster_id
    assert create_cluster["Engine"] == "aurora-postgresql"

    # Verify describe result contains cluster data (list-wrapper fidelity)
    describe_clusters = output["describeResult"]["DbClusters"]
    assert isinstance(describe_clusters, list)
    assert len(describe_clusters) >= 1

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_ec2_security_group_create_and_describe(sfn_sync, ec2):
    """aws-sdk:ec2 CreateSecurityGroup + DescribeSecurityGroups use EC2 query shapes."""
    import uuid as _uuid

    vpc_id = ec2.create_vpc(CidrBlock="10.91.0.0/16")["Vpc"]["VpcId"]
    group_name = f"sfn-ec2-sg-{_uuid.uuid4().hex[:8]}"
    description = "created through sfn aws-sdk ec2"
    sm_name = f"sdk-ec2-sg-{_uuid.uuid4().hex[:8]}"
    sm_arn = None
    sg_id = None

    definition = json.dumps({
        "StartAt": "CreateSecurityGroup",
        "States": {
            "CreateSecurityGroup": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ec2:createSecurityGroup",
                "Parameters": {
                    "GroupName": group_name,
                    "Description": description,
                    "VpcId": vpc_id,
                    "TagSpecifications": [
                        {
                            "ResourceType": "security-group",
                            "Tags": [{"Key": "Name", "Value": group_name}],
                        }
                    ],
                },
                "ResultPath": "$.createResult",
                "Next": "DescribeSecurityGroups",
            },
            "DescribeSecurityGroups": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ec2:describeSecurityGroups",
                "Parameters": {
                    "Filters": [
                        {"Name": "vpc-id", "Values": [vpc_id]},
                        {"Name": "group-name", "Values": [group_name]},
                    ],
                },
                "ResultPath": "$.describeResult",
                "Next": "Done",
            },
            "Done": {"Type": "Succeed"},
        },
    })

    try:
        sm_arn = sfn_sync.create_state_machine(
            name=sm_name,
            definition=definition,
            roleArn="arn:aws:iam::000000000000:role/sfn-role",
        )["stateMachineArn"]

        resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
        assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
        output = json.loads(resp["output"])

        sg_id = output["createResult"]["GroupId"]
        assert sg_id.startswith("sg-")

        describe_result = output["describeResult"]
        assert "SecurityGroups" in describe_result
        assert "SecurityGroupInfo" not in describe_result
        groups = describe_result["SecurityGroups"]
        assert isinstance(groups, list)
        assert len(groups) == 1
        assert groups[0]["GroupId"] == sg_id
        assert groups[0]["GroupName"] == group_name
        assert groups[0]["Description"] == description
        assert groups[0]["VpcId"] == vpc_id
        assert groups[0]["Tags"] == [{"Key": "Name", "Value": group_name}]
    finally:
        if sg_id:
            ec2.delete_security_group(GroupId=sg_id)
        ec2.delete_vpc(VpcId=vpc_id)
        if sm_arn:
            sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_ec2_security_group_duplicate_error(sfn_sync, ec2):
    """aws-sdk:ec2 CreateSecurityGroup preserves AWS duplicate-name errors."""
    import uuid as _uuid

    group_name = f"sfn-ec2-sg-dup-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-ec2-sg-dup-{_uuid.uuid4().hex[:8]}"
    sm_arn = None

    definition = json.dumps({
        "StartAt": "CreateFirst",
        "States": {
            "CreateFirst": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ec2:createSecurityGroup",
                "Parameters": {
                    "GroupName": group_name,
                    "Description": "first",
                    "VpcId": "vpc-00000001",
                },
                "Next": "CreateDuplicate",
            },
            "CreateDuplicate": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ec2:createSecurityGroup",
                "Parameters": {
                    "GroupName": group_name,
                    "Description": "second",
                    "VpcId": "vpc-00000001",
                },
                "End": True,
            },
        },
    })

    try:
        sm_arn = sfn_sync.create_state_machine(
            name=sm_name,
            definition=definition,
            roleArn="arn:aws:iam::000000000000:role/sfn-role",
        )["stateMachineArn"]

        resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
        assert resp["status"] == "FAILED"
        assert resp["error"] == "Ec2.InvalidGroup.Duplicate"
        assert "already exists" in resp["cause"]
    finally:
        groups = ec2.describe_security_groups(Filters=[
            {"Name": "vpc-id", "Values": ["vpc-00000001"]},
            {"Name": "group-name", "Values": [group_name]},
        ])["SecurityGroups"]
        for group in groups:
            ec2.delete_security_group(GroupId=group["GroupId"])
        if sm_arn:
            sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_rds_create_and_describe_instance(sfn, sfn_sync):
    """aws-sdk:rds CreateDBInstance + DescribeDBInstances via query-protocol dispatch."""
    import uuid as _uuid

    instance_id = f"sfn-inst-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-rds-inst-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "CreateInstance",
        "States": {
            "CreateInstance": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:CreateDBInstance",
                "Parameters": {
                    "DBInstanceIdentifier": instance_id,
                    "DBInstanceClass": "db.t3.micro",
                    "Engine": "postgres",
                },
                "ResultPath": "$.createResult",
                "Next": "DescribeInstances",
            },
            "DescribeInstances": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:DescribeDBInstances",
                "Parameters": {
                    "DBInstanceIdentifier": instance_id,
                },
                "ResultPath": "$.describeResult",
                "Next": "Done",
            },
            "Done": {"Type": "Succeed"},
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])

    create_inst = output["createResult"]["DbInstance"]
    assert create_inst["DbInstanceIdentifier"] == instance_id
    assert create_inst["Engine"] == "postgres"

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)

def test_sfn_aws_sdk_rds_modify_cluster(sfn, sfn_sync, rds):
    """aws-sdk:rds ModifyDBCluster via query-protocol dispatch."""
    import uuid as _uuid

    cluster_id = f"sfn-mod-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-rds-mod-{_uuid.uuid4().hex[:8]}"

    # Pre-create cluster directly
    rds.create_db_cluster(
        DBClusterIdentifier=cluster_id,
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="testpass123",
    )

    definition = json.dumps({
        "StartAt": "ModifyCluster",
        "States": {
            "ModifyCluster": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:ModifyDBCluster",
                "Parameters": {
                    "DBClusterIdentifier": cluster_id,
                    "BackupRetentionPeriod": "7",
                },
                "ResultPath": "$.modifyResult",
                "Next": "Done",
            },
            "Done": {"Type": "Succeed"},
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])
    assert output["modifyResult"]["DbCluster"]["BackupRetentionPeriod"] == 7

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)

def test_sfn_aws_sdk_rds_remove_from_global_cluster(sfn, sfn_sync, rds):
    """aws-sdk:rds RemoveFromGlobalCluster preserves RDS' DbClusterIdentifier shape."""
    cluster_id = f"sfn-global-{_uuid_mod.uuid4().hex[:8]}"
    global_id = f"sfn-global-{_uuid_mod.uuid4().hex[:8]}"
    sm_name = f"sdk-rds-global-{_uuid_mod.uuid4().hex[:8]}"
    sm_arn = None

    rds.create_db_cluster(
        DBClusterIdentifier=cluster_id,
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="testpass123",
    )
    rds.create_global_cluster(
        GlobalClusterIdentifier=global_id,
        SourceDBClusterIdentifier=cluster_id,
    )

    definition = json.dumps({
        "StartAt": "RemovePrimary",
        "States": {
            "RemovePrimary": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:removeFromGlobalCluster",
                "Parameters": {
                    "GlobalClusterIdentifier": global_id,
                    "DbClusterIdentifier": cluster_id,
                },
                "ResultPath": "$.removeResult",
                "Next": "DeleteGlobal",
            },
            "DeleteGlobal": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:deleteGlobalCluster",
                "Parameters": {
                    "GlobalClusterIdentifier": global_id,
                },
                "ResultPath": "$.deleteResult",
                "End": True,
            },
        },
    })

    try:
        sm_arn = sfn_sync.create_state_machine(
            name=sm_name,
            definition=definition,
            roleArn="arn:aws:iam::000000000000:role/sfn-role",
        )["stateMachineArn"]

        resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
        assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
        output = json.loads(resp["output"])
        assert output["removeResult"]["GlobalCluster"]["GlobalClusterMembers"] == []
    finally:
        if sm_arn:
            sfn_sync.delete_state_machine(stateMachineArn=sm_arn)
        try:
            rds.remove_from_global_cluster(
                GlobalClusterIdentifier=global_id,
                DbClusterIdentifier=cluster_id,
            )
        except ClientError:
            pass
        try:
            rds.delete_global_cluster(GlobalClusterIdentifier=global_id)
        except ClientError:
            pass
        rds.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)

def test_sfn_xml_list_wrapper_single_element(sfn, sfn_sync):
    """DescribeDBClusters returns a JSON list even when only one cluster exists."""
    import uuid as _uuid

    cluster_id = f"sfn-wrap-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-rds-wrap-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "CreateCluster",
        "States": {
            "CreateCluster": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:CreateDBCluster",
                "Parameters": {
                    "DBClusterIdentifier": cluster_id,
                    "Engine": "aurora-postgresql",
                    "MasterUsername": "admin",
                    "MasterUserPassword": "testpass123",
                },
                "ResultPath": "$.createResult",
                "Next": "DescribeClusters",
            },
            "DescribeClusters": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:DescribeDBClusters",
                "Parameters": {
                    "DBClusterIdentifier": cluster_id,
                },
                "ResultPath": "$.describeResult",
                "Next": "Done",
            },
            "Done": {"Type": "Succeed"},
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])

    # Even with a single cluster, DbClusters must be a list (not a dict).
    db_clusters = output["describeResult"]["DbClusters"]
    assert isinstance(db_clusters, list), f"Expected list, got {type(db_clusters)}: {db_clusters}"
    assert len(db_clusters) == 1
    assert db_clusters[0]["DbClusterIdentifier"] == cluster_id

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)

def test_sfn_aws_sdk_rds_not_found_error(sfn, sfn_sync):
    """aws-sdk:rds DescribeDBClusters on missing cluster propagates error."""
    import uuid as _uuid

    sm_name = f"sdk-rds-notfound-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "DescribeMissing",
        "States": {
            "DescribeMissing": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:DescribeDBClusters",
                "Parameters": {
                    "DBClusterIdentifier": "this-cluster-does-not-exist",
                },
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "FAILED"
    assert "DBClusterNotFoundFault" in (resp.get("error", "") + resp.get("cause", ""))

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)

def test_sfn_start_sync_execution(sfn_sync):
    import uuid as _uuid

    sm_name = f"intg-sync-sm-{_uuid.uuid4().hex[:8]}"
    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=json.dumps(
            {
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "Result": {"msg": "done"}, "End": True}},
            }
        ),
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]
    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({"test": True}))
    assert resp["status"] == "SUCCEEDED"
    assert "output" in resp
    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)

def test_sfn_describe_state_machine_for_execution(sfn):
    import uuid as _uuid

    sm_name = f"intg-desc-sm-exec-{_uuid.uuid4().hex[:8]}"
    sm_arn = sfn.create_state_machine(
        name=sm_name,
        definition=json.dumps(
            {
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}},
            }
        ),
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]
    exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
    time.sleep(0.5)
    resp = sfn.describe_state_machine_for_execution(executionArn=exec_resp["executionArn"])
    assert resp["stateMachineArn"] == sm_arn
    assert "definition" in resp
    sfn.delete_state_machine(stateMachineArn=sm_arn)

def test_sfn_integration_sqs_send_message(sfn, sqs):
    """Task state sends a message to SQS via arn:aws:states:::sqs:sendMessage."""
    queue_name = "sfn-integ-sqs-test"
    q = sqs.create_queue(QueueName=queue_name)
    queue_url = q["QueueUrl"]

    definition = json.dumps(
        {
            "StartAt": "Send",
            "States": {
                "Send": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                        "QueueUrl": queue_url,
                        "MessageBody.$": "$.body",
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-sqs-integ",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"body": "hello from sfn"}),
    )

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert "MessageId" in output

    # Verify the message actually landed in the queue
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert len(msgs.get("Messages", [])) == 1
    assert msgs["Messages"][0]["Body"] == "hello from sfn"

def test_sfn_integration_sns_publish(sfn, sns):
    """Task state publishes to SNS via arn:aws:states:::sns:publish."""
    topic = sns.create_topic(Name="sfn-integ-sns-test")
    topic_arn = topic["TopicArn"]

    definition = json.dumps(
        {
            "StartAt": "Publish",
            "States": {
                "Publish": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sns:publish",
                    "Parameters": {
                        "TopicArn": topic_arn,
                        "Message.$": "$.msg",
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-sns-integ",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"msg": "hello from sfn"}),
    )

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert "MessageId" in output

def test_sfn_integration_dynamodb_put_get(sfn, ddb):
    """Task states write and read from DynamoDB."""
    table_name = "sfn-integ-ddb-test"
    ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    # State machine: PutItem then GetItem
    definition = json.dumps(
        {
            "StartAt": "Put",
            "States": {
                "Put": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:putItem",
                    "Parameters": {
                        "TableName": table_name,
                        "Item": {
                            "pk": {"S.$": "$.id"},
                            "data": {"S.$": "$.value"},
                        },
                    },
                    "ResultPath": "$.putResult",
                    "Next": "Get",
                },
                "Get": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:getItem",
                    "Parameters": {
                        "TableName": table_name,
                        "Key": {"pk": {"S.$": "$.id"}},
                    },
                    "ResultPath": "$.getResult",
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ddb-integ",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"id": "item-1", "value": "test-value"}),
    )

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    item = output["getResult"]["Item"]
    assert item["pk"]["S"] == "item-1"
    assert item["data"]["S"] == "test-value"

def test_sfn_integration_dynamodb_error_catch(sfn, ddb):
    """Task state catches DynamoDB error and routes to fallback."""
    definition = json.dumps(
        {
            "StartAt": "GetMissing",
            "States": {
                "GetMissing": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:getItem",
                    "Parameters": {
                        "TableName": "nonexistent-table-sfn",
                        "Key": {"pk": {"S": "x"}},
                    },
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "Fallback",
                            "ResultPath": "$.error",
                        }
                    ],
                    "End": True,
                },
                "Fallback": {
                    "Type": "Pass",
                    "Result": "caught",
                    "ResultPath": "$.recovered",
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ddb-catch",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert output["recovered"] == "caught"
    assert "Error" in output["error"]

def test_sfn_integration_ecs_run_task(sfn, ecs):
    """Task state triggers ecs:runTask (fire-and-forget, no Docker needed)."""
    ecs.create_cluster(clusterName="sfn-ecs-test")
    ecs.register_task_definition(
        family="sfn-task",
        containerDefinitions=[
            {
                "name": "main",
                "image": "alpine:latest",
                "command": ["echo", "hi"],
                "memory": 128,
            }
        ],
    )

    definition = json.dumps(
        {
            "StartAt": "RunTask",
            "States": {
                "RunTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::ecs:runTask",
                    "Parameters": {
                        "Cluster": "sfn-ecs-test",
                        "TaskDefinition": "sfn-task",
                        "LaunchType": "FARGATE",
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ecs-integ",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert "tasks" in output

def test_sfn_integration_ecs_run_task_sync_success(sfn, ecs):
    """ecs:runTask.sync waits for task STOPPED, then returns task result."""
    import threading

    ecs.create_cluster(clusterName="sfn-ecs-sync-ok")
    ecs.register_task_definition(
        family="sfn-sync-ok",
        containerDefinitions=[
            {
                "name": "main",
                "image": "alpine",
                "memory": 128,
            }
        ],
    )

    definition = json.dumps(
        {
            "StartAt": "Run",
            "States": {
                "Run": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::ecs:runTask.sync",
                    "Parameters": {
                        "Cluster": "sfn-ecs-sync-ok",
                        "TaskDefinition": "sfn-sync-ok",
                        "LaunchType": "FARGATE",
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ecs-sync-ok",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )

    # Background thread: poll list_tasks until task appears, then stop it
    def stop_task_when_ready():
        for _ in range(30):
            time.sleep(0.5)
            try:
                tasks = ecs.list_tasks(cluster="sfn-ecs-sync-ok")
                if tasks.get("taskArns"):
                    ecs.stop_task(
                        cluster="sfn-ecs-sync-ok",
                        task=tasks["taskArns"][0],
                        reason="Test: simulating completion",
                    )
                    return
            except Exception:
                pass

    stopper = threading.Thread(target=stop_task_when_ready, daemon=True)
    stopper.start()

    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")

    desc = _wait_sfn(sfn, ex["executionArn"], timeout=20)
    stopper.join(timeout=5)
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert "tasks" in output
    task_out = output["tasks"][0]
    assert task_out["lastStatus"] == "STOPPED"
    # Containers should have exitCode 0 (stop_task sets this)
    for c in task_out.get("containers", []):
        assert c.get("exitCode") == 0

def test_sfn_integration_ecs_run_task_output_contains_status(sfn, ecs):
    """Fire-and-forget ecs:runTask output contains task status and container info."""
    ecs.create_cluster(clusterName="sfn-ecs-status")
    ecs.register_task_definition(
        family="sfn-status-task",
        containerDefinitions=[
            {
                "name": "app",
                "image": "nginx:latest",
                "memory": 256,
            }
        ],
    )

    definition = json.dumps(
        {
            "StartAt": "Run",
            "States": {
                "Run": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::ecs:runTask",
                    "Parameters": {
                        "Cluster": "sfn-ecs-status",
                        "TaskDefinition": "sfn-status-task",
                        "LaunchType": "FARGATE",
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ecs-status",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input="{}")

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert "tasks" in output
    assert len(output["tasks"]) == 1
    task_out = output["tasks"][0]
    assert "taskArn" in task_out
    assert "containers" in task_out
    assert task_out["containers"][0]["name"] == "app"
    assert task_out["lastStatus"] == "RUNNING"
    assert "failures" in output

def test_sfn_integration_ecs_run_task_container_overrides_reach_the_task(sfn, ecs):
    """PascalCase ContainerOverrides must survive the SFN->ECS hand-off instead of being silently dropped."""
    ecs.create_cluster(clusterName="sfn-ecs-overrides")
    ecs.register_task_definition(
        family="sfn-overrides-task",
        containerDefinitions=[
            {
                "name": "main",
                "image": "alpine:latest",
                "command": ["echo", "hi"],
                "memory": 128,
                "environment": [{"name": "FROM_TASKDEF", "value": "yes"}],
            }
        ],
    )

    definition = json.dumps(
        {
            "StartAt": "Run",
            "States": {
                "Run": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::ecs:runTask",
                    "Parameters": {
                        "Cluster": "sfn-ecs-overrides",
                        "TaskDefinition": "sfn-overrides-task",
                        "LaunchType": "FARGATE",
                        "Overrides": {
                            "ContainerOverrides": [
                                {
                                    "Name": "main",
                                    "Environment": [
                                        {"Name": "FROM_OVERRIDES", "Value": "yes"},
                                        {"Name": "RUN_ID", "Value.$": "$$.Execution.Name"},
                                    ],
                                }
                            ]
                        },
                    },
                    "End": True,
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-ecs-overrides",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(
        stateMachineArn=sm["stateMachineArn"], name="overrides-exec-1", input="{}"
    )

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    task_out = output["tasks"][0]

    env = {
        e["name"]: e["value"]
        for ov in task_out["overrides"]["containerOverrides"]
        for e in ov["environment"]
    }
    assert env == {"FROM_OVERRIDES": "yes", "RUN_ID": "overrides-exec-1"}
    assert task_out["overrides"]["containerOverrides"][0]["name"] == "main"

    # boto3 parses DescribeTasks against the real ECS model (camelCase only)
    described = ecs.describe_tasks(
        cluster="sfn-ecs-overrides", tasks=[task_out["taskArn"]]
    )
    described_overrides = described["tasks"][0]["overrides"]["containerOverrides"]
    assert described_overrides[0]["name"] == "main"
    described_env = {e["name"]: e["value"] for e in described_overrides[0]["environment"]}
    assert described_env == {"FROM_OVERRIDES": "yes", "RUN_ID": "overrides-exec-1"}

def test_sfn_integration_nested_start_execution_sync_returns_string_output(sfn):
    """states:startExecution.sync should return the child Output as a JSON string."""
    unique = str(time.time_ns())

    child_definition = json.dumps(
        {
            "StartAt": "BuildResult",
            "States": {
                "BuildResult": {
                    "Type": "Pass",
                    "Result": {"message": "child-ok", "version": 1},
                    "End": True,
                }
            },
        }
    )
    child = sfn.create_state_machine(
        name=f"sfn-child-sync-{unique}",
        definition=child_definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )

    parent_definition = json.dumps(
        {
            "StartAt": "RunChild",
            "States": {
                "RunChild": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::states:startExecution.sync",
                    "Parameters": {
                        "StateMachineArn": child["stateMachineArn"],
                        "Input": {"requestId.$": "$.requestId"},
                    },
                    "End": True,
                }
            },
        }
    )
    parent = sfn.create_state_machine(
        name=f"sfn-parent-sync-{unique}",
        definition=parent_definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )

    ex = sfn.start_execution(
        stateMachineArn=parent["stateMachineArn"],
        input=json.dumps({"requestId": "req-123"}),
    )

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"

    output = json.loads(desc["output"])
    assert output["Status"] == "SUCCEEDED"
    assert isinstance(output["Output"], str)
    assert json.loads(output["Output"]) == {"message": "child-ok", "version": 1}

    child_execs = sfn.list_executions(
        stateMachineArn=child["stateMachineArn"],
        statusFilter="SUCCEEDED",
    )["executions"]
    assert any(e["executionArn"] == output["ExecutionArn"] for e in child_execs)

def test_sfn_integration_nested_start_execution_sync2_returns_json_output(sfn):
    """states:startExecution.sync:2 should expose the child Output as JSON."""
    unique = str(time.time_ns())

    child_definition = json.dumps(
        {
            "StartAt": "Echo",
            "States": {
                "Echo": {
                    "Type": "Pass",
                    "Parameters": {
                        "childValue.$": "$.value",
                        "source": "child",
                    },
                    "End": True,
                }
            },
        }
    )
    child = sfn.create_state_machine(
        name=f"sfn-child-sync2-{unique}",
        definition=child_definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )

    parent_definition = json.dumps(
        {
            "StartAt": "RunChild",
            "States": {
                "RunChild": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::states:startExecution.sync:2",
                    "Parameters": {
                        "StateMachineArn": child["stateMachineArn"],
                        "Input": {"value.$": "$.value"},
                    },
                    "ResultPath": "$.child",
                    "Next": "CheckChild",
                },
                "CheckChild": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.child.Output.childValue",
                            "StringEquals": "expected",
                            "Next": "Done",
                        }
                    ],
                    "Default": "WrongChildOutput",
                },
                "WrongChildOutput": {
                    "Type": "Fail",
                    "Error": "WrongChildOutput",
                },
                "Done": {
                    "Type": "Succeed",
                },
            },
        }
    )
    parent = sfn.create_state_machine(
        name=f"sfn-parent-sync2-{unique}",
        definition=parent_definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )

    ex = sfn.start_execution(
        stateMachineArn=parent["stateMachineArn"],
        input=json.dumps({"value": "expected"}),
    )

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"

    output = json.loads(desc["output"])
    assert output["child"]["Status"] == "SUCCEEDED"
    assert output["child"]["Output"] == {
        "childValue": "expected",
        "source": "child",
    }

    child_execs = sfn.list_executions(
        stateMachineArn=child["stateMachineArn"],
        statusFilter="SUCCEEDED",
    )["executions"]
    assert any(e["executionArn"] == output["child"]["ExecutionArn"] for e in child_execs)

def test_sfn_integration_multi_service_pipeline(sfn, sqs, ddb):
    """End-to-end: Pass → DynamoDB putItem → SQS sendMessage → Succeed."""
    table_name = "sfn-pipeline-test"
    ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    queue_name = "sfn-pipeline-queue"
    q = sqs.create_queue(QueueName=queue_name)
    queue_url = q["QueueUrl"]

    definition = json.dumps(
        {
            "StartAt": "Enrich",
            "States": {
                "Enrich": {
                    "Type": "Pass",
                    "Result": "enriched",
                    "ResultPath": "$.status",
                    "Next": "SaveToDB",
                },
                "SaveToDB": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:putItem",
                    "Parameters": {
                        "TableName": table_name,
                        "Item": {
                            "pk": {"S.$": "$.id"},
                            "status": {"S.$": "$.status"},
                        },
                    },
                    "ResultPath": "$.dbResult",
                    "Next": "Notify",
                },
                "Notify": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                        "QueueUrl": queue_url,
                        "MessageBody.$": "$.id",
                    },
                    "ResultPath": "$.sqsResult",
                    "Next": "Done",
                },
                "Done": {
                    "Type": "Succeed",
                },
            },
        }
    )
    sm = sfn.create_state_machine(
        name="sfn-pipeline",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(stateMachineArn=sm["stateMachineArn"], input=json.dumps({"id": "order-42"}))

    desc = _wait_sfn(sfn, ex["executionArn"])
    assert desc["status"] == "SUCCEEDED"

    # Verify DynamoDB
    item = ddb.get_item(TableName=table_name, Key={"pk": {"S": "order-42"}})
    assert item["Item"]["status"]["S"] == "enriched"

    # Verify SQS
    msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert len(msgs.get("Messages", [])) == 1
    assert msgs["Messages"][0]["Body"] == "order-42"

def test_sfn_integration_lambda_invoke(sfn, lam):
    """Step Functions Task state invoking Lambda must return the function result."""
    import uuid as _uuid

    fn = f"intg-sfn-lam-{_uuid.uuid4().hex[:8]}"
    code = "def handler(event, context):\n    return {'doubled': event.get('value', 0) * 2}\n"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    func_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"

    definition = json.dumps(
        {
            "StartAt": "InvokeLambda",
            "States": {
                "InvokeLambda": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": func_arn,
                        "Payload.$": "$",
                    },
                    "ResultSelector": {"doubled.$": "$.Payload.doubled"},
                    "ResultPath": "$.result",
                    "End": True,
                }
            },
        }
    )
    sm = sfn.create_state_machine(
        name=f"sfn-lam-{_uuid.uuid4().hex[:8]}",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    ex = sfn.start_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"value": 21}),
    )
    desc = _wait_sfn(sfn, ex["executionArn"], timeout=10)
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert output["result"]["doubled"] == 42

def test_sfn_choice_state(sfn):
    """Choice state routes to correct branch based on input."""
    definition = json.dumps(
        {
            "StartAt": "Check",
            "States": {
                "Check": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.value",
                            "NumericGreaterThan": 10,
                            "Next": "High",
                        },
                        {
                            "Variable": "$.value",
                            "NumericLessThanEquals": 10,
                            "Next": "Low",
                        },
                    ],
                },
                "High": {"Type": "Pass", "Result": {"result": "high"}, "End": True},
                "Low": {"Type": "Pass", "Result": {"result": "low"}, "End": True},
            },
        }
    )
    arn = sfn.create_state_machine(
        name="qa-sfn-choice",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]
    exec_arn = sfn.start_execution(stateMachineArn=arn, input=json.dumps({"value": 15}))["executionArn"]
    time.sleep(0.5)
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] == "SUCCEEDED"
    assert json.loads(desc["output"])["result"] == "high"
    exec_arn2 = sfn.start_execution(stateMachineArn=arn, input=json.dumps({"value": 5}))["executionArn"]
    time.sleep(0.5)
    desc2 = sfn.describe_execution(executionArn=exec_arn2)
    assert desc2["status"] == "SUCCEEDED"
    assert json.loads(desc2["output"])["result"] == "low"

def test_sfn_pass_state_result(sfn):
    """Pass state with Result injects static data into output."""
    definition = json.dumps(
        {
            "StartAt": "Inject",
            "States": {
                "Inject": {
                    "Type": "Pass",
                    "Result": {"injected": True, "count": 42},
                    "End": True,
                }
            },
        }
    )
    arn = sfn.create_state_machine(
        name="qa-sfn-pass-result",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]
    exec_arn = sfn.start_execution(stateMachineArn=arn, input="{}")["executionArn"]
    time.sleep(0.5)
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert output["injected"] is True
    assert output["count"] == 42

def test_sfn_fail_state(sfn):
    """Fail state transitions execution to FAILED."""
    definition = json.dumps(
        {
            "StartAt": "Boom",
            "States": {
                "Boom": {
                    "Type": "Fail",
                    "Error": "CustomError",
                    "Cause": "Something went wrong",
                }
            },
        }
    )
    arn = sfn.create_state_machine(
        name="qa-sfn-fail",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]
    exec_arn = sfn.start_execution(stateMachineArn=arn, input="{}")["executionArn"]
    time.sleep(0.5)
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] == "FAILED"

def test_sfn_stop_execution(sfn):
    """StopExecution transitions a RUNNING execution to ABORTED."""
    definition = json.dumps(
        {
            "StartAt": "Wait",
            "States": {"Wait": {"Type": "Wait", "Seconds": 60, "End": True}},
        }
    )
    arn = sfn.create_state_machine(
        name="qa-sfn-stop",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]
    exec_arn = sfn.start_execution(stateMachineArn=arn, input="{}")["executionArn"]
    time.sleep(0.2)
    sfn.stop_execution(executionArn=exec_arn, cause="test stop")
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] == "ABORTED"

def test_sfn_list_executions_filter(sfn):
    """ListExecutions with statusFilter returns only matching executions."""
    definition = json.dumps(
        {
            "StartAt": "Done",
            "States": {"Done": {"Type": "Succeed"}},
        }
    )
    arn = sfn.create_state_machine(
        name="qa-sfn-list-filter",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]
    sfn.start_execution(stateMachineArn=arn, input="{}")
    time.sleep(0.5)
    succeeded = sfn.list_executions(stateMachineArn=arn, statusFilter="SUCCEEDED")["executions"]
    assert all(e["status"] == "SUCCEEDED" for e in succeeded)

def test_sfn_timestamp_fields_are_sdk_compatible(sfn, sfn_sync):
    """SFN timestamp fields must deserialize as datetimes, not fail as strings."""
    import datetime

    def assert_dt(value, field_name):
        assert isinstance(value, datetime.datetime), (
            f"{field_name} should be datetime, got {type(value)}"
        )

    unique = str(time.time_ns())
    definition = json.dumps(
        {
            "StartAt": "Done",
            "States": {"Done": {"Type": "Succeed"}},
        }
    )

    create = sfn.create_state_machine(
        name=f"qa-sfn-ts-{unique}",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    assert_dt(create["creationDate"], "CreateStateMachine.creationDate")

    arn = create["stateMachineArn"]
    desc = sfn.describe_state_machine(stateMachineArn=arn)
    assert_dt(desc["creationDate"], "DescribeStateMachine.creationDate")

    updated = sfn.update_state_machine(stateMachineArn=arn, definition=definition)
    assert_dt(updated["updateDate"], "UpdateStateMachine.updateDate")

    machines = sfn.list_state_machines()["stateMachines"]
    listed_sm = next(sm for sm in machines if sm["stateMachineArn"] == arn)
    assert_dt(listed_sm["creationDate"], "ListStateMachines.creationDate")

    start = sfn.start_execution(stateMachineArn=arn, input="{}")
    assert_dt(start["startDate"], "StartExecution.startDate")

    exec_arn = start["executionArn"]
    exec_desc = _wait_sfn(sfn, exec_arn)
    assert_dt(exec_desc["startDate"], "DescribeExecution.startDate")
    assert_dt(exec_desc["stopDate"], "DescribeExecution.stopDate")

    sm_for_exec = sfn.describe_state_machine_for_execution(executionArn=exec_arn)
    assert_dt(
        sm_for_exec["updateDate"],
        "DescribeStateMachineForExecution.updateDate",
    )

    executions = sfn.list_executions(stateMachineArn=arn)["executions"]
    listed_exec = next(ex for ex in executions if ex["executionArn"] == exec_arn)
    assert_dt(listed_exec["startDate"], "ListExecutions.startDate")
    assert_dt(listed_exec["stopDate"], "ListExecutions.stopDate")

    history = sfn.get_execution_history(executionArn=exec_arn)["events"]
    assert history, "GetExecutionHistory should return at least one event"
    assert_dt(history[0]["timestamp"], "GetExecutionHistory.events[].timestamp")

    sync = sfn_sync.start_sync_execution(stateMachineArn=arn, input="{}")
    assert_dt(sync["startDate"], "StartSyncExecution.startDate")
    assert_dt(sync["stopDate"], "StartSyncExecution.stopDate")

    wait_definition = json.dumps(
        {
            "StartAt": "Wait",
            "States": {"Wait": {"Type": "Wait", "Seconds": 60, "End": True}},
        }
    )
    wait_sm = sfn.create_state_machine(
        name=f"qa-sfn-ts-stop-{unique}",
        definition=wait_definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    wait_exec = sfn.start_execution(
        stateMachineArn=wait_sm["stateMachineArn"],
        input="{}",
    )
    stopped = sfn.stop_execution(executionArn=wait_exec["executionArn"], cause="test stop")
    assert_dt(stopped["stopDate"], "StopExecution.stopDate")

def test_sfn_activity_timestamp_fields_are_sdk_compatible(sfn):
    """SFN activity timestamp fields must deserialize as datetimes."""
    import datetime

    def assert_dt(value, field_name):
        assert isinstance(value, datetime.datetime), (
            f"{field_name} should be datetime, got {type(value)}"
        )

    unique = str(time.time_ns())
    created = sfn.create_activity(name=f"qa-sfn-activity-ts-{unique}")
    assert_dt(created["creationDate"], "CreateActivity.creationDate")

    arn = created["activityArn"]
    desc = sfn.describe_activity(activityArn=arn)
    assert_dt(desc["creationDate"], "DescribeActivity.creationDate")

    activities = sfn.list_activities()["activities"]
    listed = next(act for act in activities if act["activityArn"] == arn)
    assert_dt(listed["creationDate"], "ListActivities.creationDate")

def test_sfn_activity_create_describe_delete(sfn):
    resp = sfn.create_activity(name="qa-act-crud")
    arn = resp["activityArn"]
    assert ":activity:qa-act-crud" in arn

    desc = sfn.describe_activity(activityArn=arn)
    assert desc["name"] == "qa-act-crud"
    assert desc["activityArn"] == arn

    sfn.delete_activity(activityArn=arn)
    with pytest.raises(ClientError) as exc:
        sfn.describe_activity(activityArn=arn)
    assert exc.value.response["Error"]["Code"] == "ActivityDoesNotExist"

def test_sfn_activity_list(sfn):
    sfn.create_activity(name="qa-act-list-1")
    sfn.create_activity(name="qa-act-list-2")
    acts = sfn.list_activities()["activities"]
    names = [a["name"] for a in acts]
    assert "qa-act-list-1" in names
    assert "qa-act-list-2" in names

def test_sfn_activity_create_already_exists(sfn):
    sfn.create_activity(name="qa-act-idem")
    with pytest.raises(ClientError) as exc:
        sfn.create_activity(name="qa-act-idem")
    assert exc.value.response["Error"]["Code"] == "ActivityAlreadyExists"

def test_sfn_activity_worker_flow(sfn):
    """Worker calls GetActivityTask, then SendTaskSuccess — execution succeeds."""
    import threading

    act_arn = sfn.create_activity(name="qa-act-worker")["activityArn"]

    definition = json.dumps(
        {
            "StartAt": "DoWork",
            "States": {
                "DoWork": {"Type": "Task", "Resource": act_arn, "End": True},
            },
        }
    )
    sm_arn = sfn.create_state_machine(
        name="qa-sfn-act-worker",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]

    exec_arn = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"msg": "hello"}))["executionArn"]

    def worker():
        task = sfn.get_activity_task(activityArn=act_arn, workerName="test-worker")
        assert task["taskToken"] != ""
        assert json.loads(task["input"])["msg"] == "hello"
        sfn.send_task_success(
            taskToken=task["taskToken"],
            output=json.dumps({"result": "done"}),
        )

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join(timeout=10)

    for _ in range(20):
        time.sleep(0.3)
        status = sfn.describe_execution(executionArn=exec_arn)["status"]
        if status != "RUNNING":
            break

    assert sfn.describe_execution(executionArn=exec_arn)["status"] == "SUCCEEDED"
    output = json.loads(sfn.describe_execution(executionArn=exec_arn)["output"])
    assert output["result"] == "done"

def test_sfn_activity_worker_failure(sfn):
    """Worker calls GetActivityTask then SendTaskFailure — execution fails."""
    import threading

    act_arn = sfn.create_activity(name="qa-act-fail")["activityArn"]

    definition = json.dumps(
        {
            "StartAt": "DoWork",
            "States": {
                "DoWork": {"Type": "Task", "Resource": act_arn, "End": True},
            },
        }
    )
    sm_arn = sfn.create_state_machine(
        name="qa-sfn-act-fail",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]

    exec_arn = sfn.start_execution(stateMachineArn=sm_arn, input="{}")["executionArn"]

    def worker():
        task = sfn.get_activity_task(activityArn=act_arn, workerName="test-worker")
        sfn.send_task_failure(
            taskToken=task["taskToken"],
            error="WorkerError",
            cause="something went wrong",
        )

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join(timeout=10)

    for _ in range(20):
        time.sleep(0.3)
        status = sfn.describe_execution(executionArn=exec_arn)["status"]
        if status != "RUNNING":
            break

    assert sfn.describe_execution(executionArn=exec_arn)["status"] == "FAILED"

def test_sfn_mock_config_return(sfn):
    """SFN_MOCK_CONFIG Return — AWS SFN Local format with #TestCase ARN suffix."""
    from conftest import _ministack_config

    mock_cfg = {
        "StateMachines": {
            "qa-sfn-mock": {
                "TestCases": {
                    "HappyPath": {
                        "CallService": "MockedSuccess",
                    }
                }
            }
        },
        "MockedResponses": {
            "MockedSuccess": {
                "0": {"Return": {"status": "mocked", "value": 42}},
            }
        },
    }
    _ministack_config({"stepfunctions._sfn_mock_config": mock_cfg})

    definition = json.dumps({
        "StartAt": "CallService",
        "States": {
            "CallService": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:000000000000:function:nonexistent",
                "End": True,
            }
        },
    })
    sm_arn = sfn.create_state_machine(
        name="qa-sfn-mock",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]

    # Execute with #HappyPath test case
    exec_arn = sfn.start_execution(
        stateMachineArn=sm_arn + "#HappyPath", input="{}",
    )["executionArn"]
    for _ in range(20):
        time.sleep(0.3)
        desc = sfn.describe_execution(executionArn=exec_arn)
        if desc["status"] != "RUNNING":
            break

    assert desc["status"] == "SUCCEEDED"
    output = json.loads(desc["output"])
    assert output["status"] == "mocked"
    assert output["value"] == 42
    _ministack_config({"stepfunctions._sfn_mock_config": {}})

def test_sfn_mock_config_throw(sfn):
    """SFN_MOCK_CONFIG Throw — AWS SFN Local format with invocation indexing."""
    from conftest import _ministack_config

    mock_cfg = {
        "StateMachines": {
            "qa-sfn-mock-throw": {
                "TestCases": {
                    "FailPath": {
                        "CallService": "MockedFailure",
                    }
                }
            }
        },
        "MockedResponses": {
            "MockedFailure": {
                "0": {"Throw": {"Error": "ServiceDown", "Cause": "mocked failure"}},
            }
        },
    }
    _ministack_config({"stepfunctions._sfn_mock_config": mock_cfg})

    definition = json.dumps({
        "StartAt": "CallService",
        "States": {
            "CallService": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:000000000000:function:nonexistent",
                "End": True,
            }
        },
    })
    sm_arn = sfn.create_state_machine(
        name="qa-sfn-mock-throw",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]

    exec_arn = sfn.start_execution(
        stateMachineArn=sm_arn + "#FailPath", input="{}",
    )["executionArn"]
    for _ in range(20):
        time.sleep(0.3)
        desc = sfn.describe_execution(executionArn=exec_arn)
        if desc["status"] != "RUNNING":
            break

    assert desc["status"] == "FAILED"
    _ministack_config({"stepfunctions._sfn_mock_config": {}})


def test_sfn_mock_config_throw_routes_to_catch(sfn):
    """SFN_MOCK_CONFIG Throw must route to a matching Catch (not bypass it). #903."""
    from conftest import _ministack_config

    mock_cfg = {
        "StateMachines": {
            "qa-sfn-mock-throw-catch": {
                "TestCases": {"FailPath": {"CallService": "MockedFailure"}}
            }
        },
        "MockedResponses": {
            "MockedFailure": {
                "0": {"Throw": {"Error": "ServiceDown", "Cause": "mocked failure"}},
            }
        },
    }
    _ministack_config({"stepfunctions._sfn_mock_config": mock_cfg})

    definition = json.dumps({
        "StartAt": "CallService",
        "States": {
            "CallService": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:000000000000:function:nonexistent",
                "Catch": [{"ErrorEquals": ["ServiceDown"], "Next": "Recovered"}],
                "End": True,
            },
            "Recovered": {"Type": "Pass", "Result": {"recovered": True}, "End": True},
        },
    })
    sm_arn = sfn.create_state_machine(
        name="qa-sfn-mock-throw-catch",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]

    exec_arn = sfn.start_execution(
        stateMachineArn=sm_arn + "#FailPath", input="{}",
    )["executionArn"]
    for _ in range(20):
        time.sleep(0.3)
        desc = sfn.describe_execution(executionArn=exec_arn)
        if desc["status"] != "RUNNING":
            break

    assert desc["status"] == "SUCCEEDED", f"Throw bypassed Catch: {desc.get('status')}"
    # Reached the Catch's Next state ("Recovered"), proving the throw routed
    # through Catch rather than failing the execution.
    assert json.loads(desc["output"]) == {"recovered": True}
    _ministack_config({"stepfunctions._sfn_mock_config": {}})


def test_sfn_mock_config_jsonata_assign_applied(sfn):
    """SFN_MOCK_CONFIG + JSONata Task with Assign — variable must be visible downstream.

    Regression: the mock execution path called _apply_jsonata_output but never
    called _apply_state_assign, so any Assign block was silently skipped.
    A downstream state reading the assigned variable would fail with
    States.QueryEvaluationError: Undefined variable.
    """
    from conftest import _ministack_config

    mock_cfg = {
        "StateMachines": {
            "qa-sfn-mock-jsonata-assign": {
                "TestCases": {
                    "HappyPath": {
                        "FetchData": "MockedData",
                    }
                }
            }
        },
        "MockedResponses": {
            "MockedData": {
                "0": {"Return": {"value": 99}},
            }
        },
    }
    _ministack_config({"stepfunctions._sfn_mock_config": mock_cfg})

    definition = json.dumps({
        "QueryLanguage": "JSONata",
        "StartAt": "FetchData",
        "States": {
            "FetchData": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:000000000000:function:nonexistent",
                "Assign": {"fetchedValue": "{% $states.result.value %}"},
                "Next": "UseValue",
            },
            "UseValue": {
                "Type": "Pass",
                "Output": {"result": "{% $fetchedValue %}"},
                "End": True,
            },
        },
    })
    sm_arn = sfn.create_state_machine(
        name="qa-sfn-mock-jsonata-assign",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )["stateMachineArn"]

    exec_arn = sfn.start_execution(
        stateMachineArn=sm_arn + "#HappyPath", input="{}",
    )["executionArn"]
    for _ in range(20):
        time.sleep(0.3)
        desc = sfn.describe_execution(executionArn=exec_arn)
        if desc["status"] != "RUNNING":
            break

    assert desc["status"] == "SUCCEEDED", (
        f"Execution failed — Assign likely not applied in mock path: {desc.get('cause')}"
    )
    output = json.loads(desc["output"])
    assert output["result"] == 99, f"Expected 99 from assigned variable, got: {output}"
    _ministack_config({"stepfunctions._sfn_mock_config": {}})

def test_sfn_test_state_pass(sfn_sync):
    """TestState API — Pass state returns transformed output."""
    resp = sfn_sync.test_state(
        definition=json.dumps({
            "Type": "Pass",
            "Result": {"greeting": "hello"},
            "ResultPath": "$.result",
            "Next": "NextStep",
        }),
        input=json.dumps({"existing": "data"}),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    assert output["result"]["greeting"] == "hello"
    assert output["existing"] == "data"
    assert resp["nextState"] == "NextStep"

def test_sfn_test_state_choice(sfn_sync):
    """TestState API — Choice state routes to correct next state."""
    resp = sfn_sync.test_state(
        definition=json.dumps({
            "Type": "Choice",
            "Choices": [
                {"Variable": "$.val", "NumericEquals": 1, "Next": "One"},
                {"Variable": "$.val", "NumericEquals": 2, "Next": "Two"},
            ],
            "Default": "Other",
        }),
        input=json.dumps({"val": 2}),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    assert resp["status"] == "SUCCEEDED"
    assert resp["nextState"] == "Two"

def test_sfn_test_state_fail(sfn_sync):
    """TestState API — Fail state returns FAILED status."""
    resp = sfn_sync.test_state(
        definition=json.dumps({
            "Type": "Fail",
            "Error": "CustomError",
            "Cause": "Something went wrong",
        }),
        input="{}",
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    assert resp["status"] == "FAILED"
    assert resp["error"] == "CustomError"
    assert resp["cause"] == "Something went wrong"

def test_sfn_test_state_task_with_mock_return(sfn_sync):
    """TestState API — Task state with mock.result returns mocked output."""
    resp = sfn_sync.test_state(
        definition=json.dumps({
            "Type": "Task",
            "Resource": "arn:aws:lambda:us-east-1:000000000000:function:MyFunc",
            "End": True,
        }),
        input=json.dumps({"key": "value"}),
        roleArn="arn:aws:iam::000000000000:role/r",
        inspectionLevel="DEBUG",
        mock={"result": json.dumps({"Payload": {"statusCode": 200, "body": "mocked"}})},
    )
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    assert output["Payload"]["body"] == "mocked"

def test_sfn_test_state_task_with_mock_error(sfn_sync):
    """TestState API — Task state with mock.errorOutput and Catch."""
    resp = sfn_sync.test_state(
        definition=json.dumps({
            "Type": "Task",
            "Resource": "arn:aws:lambda:us-east-1:000000000000:function:MyFunc",
            "Catch": [{"ErrorEquals": ["Lambda.ServiceException"], "Next": "HandleError"}],
            "Next": "Done",
        }),
        input=json.dumps({"key": "value"}),
        roleArn="arn:aws:iam::000000000000:role/r",
        mock={"errorOutput": {"error": "Lambda.ServiceException", "cause": "Service unavailable"}},
    )
    assert resp["status"] == "CAUGHT_ERROR"
    assert resp["nextState"] == "HandleError"
    assert resp["error"] == "Lambda.ServiceException"

def test_sfn_test_state_debug_inspection(sfn_sync):
    """TestState API — DEBUG inspectionLevel returns data transformation details."""
    resp = sfn_sync.test_state(
        definition=json.dumps({
            "Type": "Pass",
            "InputPath": "$.payload",
            "Result": {"data": 1},
            "ResultPath": "$.result",
            "Next": "Done",
        }),
        input=json.dumps({"payload": {"foo": "bar"}}),
        roleArn="arn:aws:iam::000000000000:role/r",
        inspectionLevel="DEBUG",
    )
    assert resp["status"] == "SUCCEEDED"
    assert "inspectionData" in resp
    assert "input" in resp["inspectionData"]

def test_sfn_test_state_from_full_definition(sfn_sync):
    """TestState API — extract specific state from full state machine definition."""
    resp = sfn_sync.test_state(
        definition=json.dumps({
            "StartAt": "First",
            "States": {
                "First": {"Type": "Pass", "Result": "first", "Next": "Second"},
                "Second": {"Type": "Pass", "Result": "second", "End": True},
            }
        }),
        input="{}",
        roleArn="arn:aws:iam::000000000000:role/r",
        stateName="Second",
    )
    assert resp["status"] == "SUCCEEDED"
    assert json.loads(resp["output"]) == "second"

def test_sfn_update_state_machine(sfn):
    """Create SM, update definition, describe and verify new definition."""
    defn_v1 = json.dumps({
        "StartAt": "A",
        "States": {"A": {"Type": "Pass", "Result": "v1", "End": True}},
    })
    create = sfn.create_state_machine(
        name="sfn-update-test",
        definition=defn_v1,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    arn = create["stateMachineArn"]

    defn_v2 = json.dumps({
        "StartAt": "B",
        "States": {"B": {"Type": "Pass", "Result": "v2", "End": True}},
    })
    sfn.update_state_machine(stateMachineArn=arn, definition=defn_v2)

    desc = sfn.describe_state_machine(stateMachineArn=arn)
    assert desc["definition"] == defn_v2

def test_sfn_create_duplicate_name(sfn):
    """CreateStateMachine with duplicate name should fail."""
    defn = json.dumps({
        "StartAt": "X",
        "States": {"X": {"Type": "Pass", "End": True}},
    })
    sfn.create_state_machine(
        name="sfn-dup-err-test",
        definition=defn,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    with pytest.raises(ClientError) as exc:
        sfn.create_state_machine(
            name="sfn-dup-err-test",
            definition=defn,
            roleArn="arn:aws:iam::000000000000:role/R",
        )
    assert "StateMachineAlreadyExists" in str(exc.value) or "Conflict" in str(exc.value) or exc.value.response["Error"]["Code"]

def test_sfn_describe_not_found(sfn):
    """DescribeStateMachine on non-existent ARN should fail."""
    with pytest.raises(ClientError) as exc:
        sfn.describe_state_machine(stateMachineArn="arn:aws:states:us-east-1:000000000000:stateMachine:nonexistent-99")
    err = exc.value.response["Error"]["Code"]
    assert "StateMachineDoesNotExist" in err or "NotFound" in err or "ResourceNotFound" in err
    # Real AWS sends `x-amzn-errortype` on JSON-protocol errors; Java/Go SDK v2 read it.
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "StateMachineDoesNotExist"

def test_sfn_start_execution_not_found(sfn):
    """StartExecution on non-existent SM should fail."""
    with pytest.raises(ClientError) as exc:
        sfn.start_execution(stateMachineArn="arn:aws:states:us-east-1:000000000000:stateMachine:nonexistent-99")
    err = exc.value.response["Error"]["Code"]
    assert "StateMachineDoesNotExist" in err or "NotFound" in err or "ResourceNotFound" in err


def test_sfn_intrinsic_json_to_string(sfn, sfn_sync):
    """States.JsonToString serializes structured data to a compact JSON string."""
    definition = json.dumps({
        "StartAt": "Serialize",
        "States": {
            "Serialize": {
                "Type": "Pass",
                "Parameters": {
                    "serialized.$": "States.JsonToString($.obj)"
                },
                "End": True,
            }
        },
    })
    sm = sfn.create_state_machine(
        name="sfn-intrinsic-j2s",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn_sync.start_sync_execution(
        stateMachineArn=sm["stateMachineArn"],
        input=json.dumps({"obj": {"a": 1, "b": [2, 3]}}),
    )
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    # Should be compact JSON (no spaces)
    parsed = json.loads(output["serialized"])
    assert parsed == {"a": 1, "b": [2, 3]}
    assert " " not in output["serialized"]


def test_sfn_aws_sdk_query_pascal_case(sfn, sfn_sync, ssm):
    """SFN aws-sdk integration converts camelCase action to PascalCase for query-protocol services."""
    definition = json.dumps({
        "StartAt": "PutParam",
        "States": {
            "PutParam": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ssm:putParameter",
                "Parameters": {
                    "Name": "sfn-pascal-test-param",
                    "Value": "hello-from-sfn",
                    "Type": "String",
                    "Overwrite": True,
                },
                "ResultPath": "$.putResult",
                "Next": "GetParam",
            },
            "GetParam": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ssm:getParameter",
                "Parameters": {
                    "Name": "sfn-pascal-test-param",
                },
                "End": True,
            },
        },
    })
    sm = sfn.create_state_machine(
        name="sfn-pascal-query",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn_sync.start_sync_execution(
        stateMachineArn=sm["stateMachineArn"],
        input="{}",
    )
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    assert output["Parameter"]["Value"] == "hello-from-sfn"
    # Cleanup
    ssm.delete_parameter(Name="sfn-pascal-test-param")


def test_sfn_aws_sdk_json_pascal_case(sfn, sfn_sync, sm):
    """SFN aws-sdk integration converts camelCase action to PascalCase for JSON-protocol services."""
    definition = json.dumps({
        "StartAt": "CreateSecret",
        "States": {
            "CreateSecret": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:secretsmanager:createSecret",
                "Parameters": {
                    "Name": "sfn-pascal-json-secret",
                    "SecretString": "my-secret-value",
                },
                "ResultPath": "$.createResult",
                "Next": "GetSecret",
            },
            "GetSecret": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:secretsmanager:getSecretValue",
                "Parameters": {
                    "SecretId": "sfn-pascal-json-secret",
                },
                "End": True,
            },
        },
    })
    sm_resp = sfn.create_state_machine(
        name="sfn-pascal-json",
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )
    resp = sfn_sync.start_sync_execution(
        stateMachineArn=sm_resp["stateMachineArn"],
        input="{}",
    )
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    assert output["SecretString"] == "my-secret-value"
    # Cleanup
    sm.delete_secret(SecretId="sfn-pascal-json-secret", ForceDeleteWithoutRecovery=True)


def test_sfn_aws_sdk_query_acronym_param_mapping(sfn, sfn_sync, rds):
    """SFN aws-sdk query dispatch maps SDK-style param names to wire-format names."""
    import uuid as _uuid
    cluster_id = f"acronym-test-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-acronym-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "CreateCluster",
        "States": {
            "CreateCluster": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:createDBCluster",
                "Parameters": {
                    "DbClusterIdentifier": cluster_id,
                    "Engine": "aurora-postgresql",
                    "MasterUsername": "admin",
                    "MasterUserPassword": "testpass123",
                },
                "ResultPath": "$.createResult",
                "Next": "DescribeClusters",
            },
            "DescribeClusters": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:describeDBClusters",
                "Parameters": {
                    "DbClusterIdentifier": cluster_id,
                },
                "ResultPath": "$.describeResult",
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])

    create_cluster = output["createResult"]["DbCluster"]
    assert create_cluster["DbClusterIdentifier"] == cluster_id
    assert create_cluster["Engine"] == "aurora-postgresql"

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_key_to_api_name_must_convert():
    """Verify _sfn_key_to_api_name expands known acronyms to uppercase."""
    from ministack.services.stepfunctions import _sfn_key_to_api_name

    cases = [
        ("DbSubnetGroupName", "DBSubnetGroupName"),
        ("DbClusterIdentifier", "DBClusterIdentifier"),
        ("DbClusterArn", "DBClusterArn"),
        ("IamDatabaseAuthenticationEnabled", "IAMDatabaseAuthenticationEnabled"),
        ("DomainIamRoleName", "DomainIAMRoleName"),
        ("CaCertificateIdentifier", "CACertificateIdentifier"),
        ("VpcSecurityGroupIds", "VPCSecurityGroupIds"),
        ("KmsKeyId", "KMSKeyId"),
        ("SslMode", "SSLMode"),
        ("EbsOptimized", "EBSOptimized"),
        ("IoOptimizedNextAllowedModificationTime", "IOOptimizedNextAllowedModificationTime"),
        ("DnsName", "DNSName"),
        ("AzMode", "AZMode"),
        ("TtlSeconds", "TTLSeconds"),
        ("SgId", "SGId"),
        ("AclName", "ACLName"),
    ]
    for sfn, expected in cases:
        assert _sfn_key_to_api_name(sfn) == expected, f"{sfn} → expected {expected}"


def test_sfn_key_to_api_name_must_not_convert():
    """Verify _sfn_key_to_api_name leaves non-acronym names unchanged."""
    from ministack.services.stepfunctions import _sfn_key_to_api_name

    cases = [
        "Engine", "MasterUsername", "Port", "SubnetIds",
        "HttpEndpointEnabled", "StorageEncrypted", "DeletionProtection",
        "BackupRetentionPeriod", "PreferredBackupWindow",
    ]
    for name in cases:
        assert _sfn_key_to_api_name(name) == name, f"{name} should be unchanged"


def test_sfn_key_to_api_name_idempotent():
    """Verify _sfn_key_to_api_name is idempotent on wire-format names."""
    from ministack.services.stepfunctions import _sfn_key_to_api_name

    wire_names = [
        "DBSubnetGroupName", "IAMDatabaseAuthenticationEnabled",
        "VPCSecurityGroupIds", "KMSKeyId", "CACertificateIdentifier",
    ]
    for name in wire_names:
        assert _sfn_key_to_api_name(name) == name, f"{name} should be idempotent"


def test_sfn_key_to_api_name_round_trip():
    """Verify _sfn_key_to_api_name correctly reverses _api_name_to_sfn_key."""
    from ministack.services.stepfunctions import _api_name_to_sfn_key, _sfn_key_to_api_name

    wire_names = [
        "DBSubnetGroupName", "DBClusterIdentifier", "DBClusterArn",
        "IAMDatabaseAuthenticationEnabled", "VPCSecurityGroupIds",
        "KMSKeyId", "SSLMode", "EBSOptimized", "IOOptimizedNextAllowedModificationTime",
        "CACertificateIdentifier", "DNSName", "AZMode", "TTLSeconds",
        "Engine", "MasterUsername", "Port", "SubnetIds", "HttpEndpointEnabled",
    ]
    for wire in wire_names:
        sfn = _api_name_to_sfn_key(wire)
        back = _sfn_key_to_api_name(sfn)
        assert back == wire, f"Round-trip failed: {wire} → {sfn} → {back}"


def test_convert_params_to_api_names_nested():
    """Verify _convert_params_to_api_names handles nested dicts and lists."""
    from ministack.services.stepfunctions import _convert_params_to_api_names

    result = _convert_params_to_api_names({
        "DbClusterIdentifier": "my-cluster",
        "VpcSecurityGroupIds": [{"SgId": "sg-123"}],
        "Tags": [{"Key": "env", "Value": "test"}],
    })
    assert result == {
        "DBClusterIdentifier": "my-cluster",
        "VPCSecurityGroupIds": [{"SGId": "sg-123"}],
        "Tags": [{"Key": "env", "Value": "test"}],
    }


def test_sfn_aws_sdk_rdsdata_execute_statement(sfn, sfn_sync, rds, sm):
    """SFN aws-sdk:rdsdata:executeStatement dispatches via REST-JSON protocol."""
    import uuid as _uuid
    cluster_id = f"rdsdata-sfn-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-rdsdata-{_uuid.uuid4().hex[:8]}"

    rds.create_db_cluster(
        DBClusterIdentifier=cluster_id,
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="testpass123",
    )
    secret_arn = sm.create_secret(
        Name=f"rdsdata-secret-{_uuid.uuid4().hex[:8]}",
        SecretString='{"username":"admin","password":"testpass123"}',
    )["ARN"]
    cluster_arn = f"arn:aws:rds:us-east-1:000000000000:cluster:{cluster_id}"

    definition = json.dumps({
        "StartAt": "ExecuteSQL",
        "States": {
            "ExecuteSQL": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rdsdata:executeStatement",
                "Parameters": {
                    "resourceArn": cluster_arn,
                    "secretArn": secret_arn,
                    "sql": "SELECT 1",
                    "database": "testdb",
                },
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])
    assert "NumberOfRecordsUpdated" in output
    assert "GeneratedFields" in output
    assert "Records" in output

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_rdsdata_unknown_action_fails(sfn, sfn_sync):
    """SFN aws-sdk:rdsdata with unknown action fails with deterministic error."""
    import uuid as _uuid
    sm_name = f"sdk-rdsdata-bad-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "BadAction",
        "States": {
            "BadAction": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rdsdata:notARealAction",
                "Parameters": {"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:fake"},
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "FAILED"

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_rdsdata_output_uses_sfn_key_convention(sfn, sfn_sync, rds, sm):
    """REST-JSON aws-sdk output is exposed with AWS SFN SDK-shaped keys."""
    import uuid as _uuid

    cluster_id = f"rdsdata-sfn-output-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-rdsdata-output-{_uuid.uuid4().hex[:8]}"

    rds.create_db_cluster(
        DBClusterIdentifier=cluster_id,
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="testpass123",
    )
    secret_arn = sm.create_secret(
        Name=f"rdsdata-output-secret-{_uuid.uuid4().hex[:8]}",
        SecretString='{"username":"admin","password":"testpass123"}',
    )["ARN"]
    cluster_arn = f"arn:aws:rds:us-east-1:000000000000:cluster:{cluster_id}"

    definition = json.dumps({
        "StartAt": "CreateUser",
        "States": {
            "CreateUser": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rdsdata:executeStatement",
                "Parameters": {
                    "ResourceArn": cluster_arn,
                    "SecretArn": secret_arn,
                    "Sql": "CREATE USER IF NOT EXISTS 'alice'",
                    "Database": "testdb",
                },
                "Next": "ObserveUser",
            },
            "ObserveUser": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rdsdata:executeStatement",
                "Parameters": {
                    "ResourceArn": cluster_arn,
                    "SecretArn": secret_arn,
                    "Sql": "SELECT User FROM mysql.user WHERE User = 'alice'",
                    "Database": "testdb",
                },
                "ResultSelector": {
                    "Records.$": "$.Records",
                    "Updated.$": "$.NumberOfRecordsUpdated",
                },
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} — {resp.get('cause')}"
    output = json.loads(resp["output"])
    assert output["Records"] == [[{"StringValue": "alice"}]]
    assert output["Updated"] == 0

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_rdsdata_path_mapping():
    """Verify REST-JSON action→path mappings are correct for rds-data."""
    from ministack.services.stepfunctions import _REST_JSON_ACTION_PATHS

    rds_data_paths = _REST_JSON_ACTION_PATHS["rds-data"]
    assert rds_data_paths["ExecuteStatement"] == "/Execute"
    assert rds_data_paths["BatchExecuteStatement"] == "/BatchExecute"
    assert rds_data_paths["BeginTransaction"] == "/BeginTransaction"
    assert rds_data_paths["CommitTransaction"] == "/CommitTransaction"
    assert rds_data_paths["RollbackTransaction"] == "/RollbackTransaction"


# ---------------------------------------------------------------------------
# REST-XML aws-sdk dispatch (S3) — Issue #573
# ---------------------------------------------------------------------------


def test_sfn_aws_sdk_s3_list_objects_v2(sfn_sync, s3):
    """aws-sdk:s3:listObjectsV2 returns object listing through REST-XML dispatch."""
    import uuid as _uuid

    bucket = f"sfn-s3-list-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-s3-list-{_uuid.uuid4().hex[:8]}"
    sm_arn = None
    try:
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="alpha/one.txt", Body=b"a")
        s3.put_object(Bucket=bucket, Key="alpha/two.txt", Body=b"b")
        s3.put_object(Bucket=bucket, Key="beta/three.txt", Body=b"c")

        definition = json.dumps({
            "StartAt": "List",
            "States": {
                "List": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:s3:listObjectsV2",
                    "Parameters": {"Bucket": bucket, "Prefix": "alpha/"},
                    "End": True,
                },
            },
        })

        sm_arn = sfn_sync.create_state_machine(
            name=sm_name,
            definition=definition,
            roleArn="arn:aws:iam::000000000000:role/sfn-role",
        )["stateMachineArn"]

        resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
        assert resp["status"] == "SUCCEEDED", f"{resp.get('error')} — {resp.get('cause')}"
        output = json.loads(resp["output"])
        keys = sorted(item["Key"] for item in output.get("Contents", []))
        assert keys == ["alpha/one.txt", "alpha/two.txt"]
    finally:
        for k in ("alpha/one.txt", "alpha/two.txt", "beta/three.txt"):
            try:
                s3.delete_object(Bucket=bucket, Key=k)
            except Exception:
                pass
        try:
            s3.delete_bucket(Bucket=bucket)
        except Exception:
            pass
        if sm_arn:
            sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_s3_copy_object(sfn_sync, s3):
    """aws-sdk:s3:copyObject duplicates an object via x-amz-copy-source header."""
    import uuid as _uuid

    bucket = f"sfn-s3-copy-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-s3-copy-{_uuid.uuid4().hex[:8]}"
    sm_arn = None
    try:
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="src.txt", Body=b"hello")

        definition = json.dumps({
            "StartAt": "Copy",
            "States": {
                "Copy": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:s3:copyObject",
                    "Parameters": {
                        "Bucket": bucket,
                        "Key": "dst.txt",
                        "CopySource": f"{bucket}/src.txt",
                    },
                    "End": True,
                },
            },
        })

        sm_arn = sfn_sync.create_state_machine(
            name=sm_name,
            definition=definition,
            roleArn="arn:aws:iam::000000000000:role/sfn-role",
        )["stateMachineArn"]

        resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
        assert resp["status"] == "SUCCEEDED", f"{resp.get('error')} — {resp.get('cause')}"

        copied = s3.get_object(Bucket=bucket, Key="dst.txt")["Body"].read()
        assert copied == b"hello"
    finally:
        for k in ("src.txt", "dst.txt"):
            try:
                s3.delete_object(Bucket=bucket, Key=k)
            except Exception:
                pass
        try:
            s3.delete_bucket(Bucket=bucket)
        except Exception:
            pass
        if sm_arn:
            sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_s3_list_buckets(sfn_sync, s3):
    """aws-sdk:s3:listBuckets returns the bucket list via REST-XML."""
    import uuid as _uuid

    marker_bucket = f"sfn-s3-marker-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-s3-listbuckets-{_uuid.uuid4().hex[:8]}"
    sm_arn = None
    try:
        s3.create_bucket(Bucket=marker_bucket)

        definition = json.dumps({
            "StartAt": "ListBuckets",
            "States": {
                "ListBuckets": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:s3:listBuckets",
                    "Parameters": {},
                    "End": True,
                },
            },
        })

        sm_arn = sfn_sync.create_state_machine(
            name=sm_name,
            definition=definition,
            roleArn="arn:aws:iam::000000000000:role/sfn-role",
        )["stateMachineArn"]

        resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
        assert resp["status"] == "SUCCEEDED", f"{resp.get('error')} — {resp.get('cause')}"
        output = json.loads(resp["output"])
        names = [b["Name"] for b in output.get("Buckets", [])]
        assert marker_bucket in names
    finally:
        try:
            s3.delete_bucket(Bucket=marker_bucket)
        except Exception:
            pass
        if sm_arn:
            sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_s3_unsupported_op_returns_helpful_error(sfn_sync):
    """aws-sdk:s3:getObject (Phase 2) returns a clear 'not yet implemented' error."""
    import uuid as _uuid

    sm_name = f"sdk-s3-unsupported-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "Get",
        "States": {
            "Get": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
                "Parameters": {"Bucket": "x", "Key": "y"},
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "FAILED"
    cause = (resp.get("cause") or "").lower()
    assert "not yet implemented" in cause and "getobject" in cause

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_s3_op_specs_cover_issue_573_request():
    """The two ops the original bug report named must be in the Phase 1 spec table."""
    from ministack.services.stepfunctions import _S3_OP_SPECS

    assert "ListObjectsV2" in _S3_OP_SPECS
    assert "CopyObject" in _S3_OP_SPECS
# ---------------------------------------------------------------------------
# Terraform compatibility tests
# ---------------------------------------------------------------------------


def test_sfn_validate_state_machine_definition(sfn):
    """ValidateStateMachineDefinition must return OK (required by Terraform v5.42.0+)."""
    definition = json.dumps({
        "StartAt": "Pass",
        "States": {"Pass": {"Type": "Succeed"}},
    })
    resp = sfn.validate_state_machine_definition(definition=definition)
    assert resp["result"] == "OK"
    assert resp["diagnostics"] == []


def test_sfn_rest_json_pascal_to_camel_conversion(sfn, sfn_sync, rds, sm):
    """PascalCase params in SFN are converted to camelCase for REST-JSON dispatch."""
    import uuid as _uuid

    cluster_id = f"rdsdata-camel-{_uuid.uuid4().hex[:8]}"
    sm_name = f"sdk-rdsdata-camel-{_uuid.uuid4().hex[:8]}"

    rds.create_db_cluster(
        DBClusterIdentifier=cluster_id,
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="testpass123",
    )
    secret_arn = sm.create_secret(
        Name=f"rdsdata-camel-secret-{_uuid.uuid4().hex[:8]}",
        SecretString='{"username":"admin","password":"testpass123"}',
    )["ARN"]
    cluster_arn = f"arn:aws:rds:us-east-1:000000000000:cluster:{cluster_id}"

    # Use PascalCase keys — the dispatcher must convert them to camelCase
    definition = json.dumps({
        "StartAt": "ExecuteSQL",
        "States": {
            "ExecuteSQL": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rdsdata:executeStatement",
                "Parameters": {
                    "ResourceArn": cluster_arn,
                    "SecretArn": secret_arn,
                    "Sql": "SELECT 1",
                    "Database": "testdb",
                },
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input=json.dumps({}))
    assert resp["status"] == "SUCCEEDED", f"Execution failed: {resp.get('error')} \u2014 {resp.get('cause')}"
    output = json.loads(resp["output"])
    assert "NumberOfRecordsUpdated" in output
    assert "GeneratedFields" in output
    assert "Records" in output

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_validate_state_machine_definition_with_type(sfn):
    """ValidateStateMachineDefinition should accept optional type parameter."""
    definition = json.dumps({
        "StartAt": "Hello",
        "States": {
            "Hello": {"Type": "Pass", "Result": "world", "End": True},
        },
    })
    resp = sfn.validate_state_machine_definition(
        definition=definition,
        type="STANDARD",
    )
    assert resp["result"] == "OK"
    assert isinstance(resp["diagnostics"], list)



def test_sfn_intrinsic_functions_batch_2(sfn, sfn_sync):
    """Test batch 2 intrinsic functions."""
    import uuid as _uuid
    sm_name = f"intrinsics-b2-{_uuid.uuid4().hex[:8]}"
    definition = json.dumps({
        "StartAt": "Test",
        "States": {
            "Test": {
                "Type": "Pass",
                "Parameters": {
                    "contains.$": "States.ArrayContains(States.Array(1, 2, 3), 2)",
                    "containsMiss.$": "States.ArrayContains(States.Array(1, 2, 3), 5)",
                    "unique.$": "States.ArrayUnique(States.Array(1, 2, 2, 3, 3))",
                    "partition.$": "States.ArrayPartition(States.Array(1, 2, 3, 4, 5), 2)",
                    "range.$": "States.ArrayRange(1, 9, 2)",
                    "add.$": "States.MathAdd(5, 3)",
                    "uuid.$": "States.UUID()",
                },
                "End": True,
            },
        },
    })
    sm_arn = sfn_sync.create_state_machine(name=sm_name, definition=definition, roleArn="arn:aws:iam::000000000000:role/sfn-role")["stateMachineArn"]
    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input="{}")
    assert resp["status"] == "SUCCEEDED"
    output = json.loads(resp["output"])
    assert output["contains"] is True
    assert output["containsMiss"] is False
    assert output["unique"] == [1, 2, 3]
    assert output["partition"] == [[1, 2], [3, 4], [5]]
    assert output["range"] == [1, 3, 5, 7, 9]
    assert output["add"] == 8
    assert len(output["uuid"]) == 36  # UUID format
    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_aws_sdk_error_prefix_catch(sfn, sm):
    """aws-sdk errors are prefixed with the service name so Catch blocks match.

    Real AWS SFN surfaces SDK errors as "<ServiceName>.<ErrorCode>" (e.g.,
    "SecretsManager.ResourceExistsException").  Verify that a Catch block
    matching the prefixed form works correctly.
    """
    import uuid as _uuid

    secret_name = f"sdk-err-prefix-{_uuid.uuid4().hex[:8]}"

    # Pre-create the secret so the SFN's CreateSecret will fail.
    sm.create_secret(Name=secret_name, SecretString='{"test":"value"}')

    definition = json.dumps({
        "StartAt": "CreateDuplicate",
        "States": {
            "CreateDuplicate": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:secretsmanager:createSecret",
                "Parameters": {
                    "Name": secret_name,
                    "SecretString": '{"dup":"true"}',
                },
                "Catch": [
                    {
                        "ErrorEquals": ["SecretsManager.ResourceExistsException"],
                        "ResultPath": "$.error",
                        "Next": "Caught",
                    }
                ],
                "End": True,
            },
            "Caught": {
                "Type": "Pass",
                "Result": "handled",
                "ResultPath": "$.recovered",
                "End": True,
            },
        },
    })

    sm_name = f"sdk-err-prefix-{_uuid.uuid4().hex[:8]}"
    sm_resp = sfn.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )

    ex = sfn.start_execution(stateMachineArn=sm_resp["stateMachineArn"], input="{}")
    desc = _wait_sfn(sfn, ex["executionArn"])

    assert desc["status"] == "SUCCEEDED", f"Expected SUCCEEDED, got {desc['status']}: {desc.get('cause', '')}"
    output = json.loads(desc["output"])
    assert output["recovered"] == "handled"
    assert "SecretsManager.ResourceExistsException" in output["error"]["Error"]

    # Cleanup.
    sfn.delete_state_machine(stateMachineArn=sm_resp["stateMachineArn"])
    sm.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)


def test_sfn_aws_sdk_error_prefix_in_failed_execution(sfn, sfn_sync):
    """When no Catch matches, the prefixed error code appears in the execution failure."""
    import uuid as _uuid

    sm_name = f"sdk-err-nocatch-{_uuid.uuid4().hex[:8]}"

    definition = json.dumps({
        "StartAt": "DescribeMissing",
        "States": {
            "DescribeMissing": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:rds:DescribeDBClusters",
                "Parameters": {
                    "DBClusterIdentifier": "nonexistent-cluster-prefix-test",
                },
                "End": True,
            },
        },
    })

    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
    )["stateMachineArn"]

    resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input="{}")
    assert resp["status"] == "FAILED"
    # Error should have the "Rds." prefix.
    assert resp.get("error", "").startswith("Rds."), f"Expected Rds. prefix, got: {resp.get('error', '')}"

    sfn_sync.delete_state_machine(stateMachineArn=sm_arn)

def test_sfn_wait_scale_zero_does_not_timeout_lambda_tasks(sfn, lam):
    """SFN_WAIT_SCALE=0 must not cause Lambda Task states to timeout.

    _scaled_timeout was previously applied to activity and callback waits,
    causing 0.01s timeouts that raced against Lambda execution.  Task
    states that invoke Lambda synchronously should be unaffected by the
    wait scale factor.
    """
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    def _set_wait_scale(val):
        req = urllib.request.Request(
            f"{endpoint}/_ministack/config",
            data=json.dumps({"stepfunctions._SFN_WAIT_SCALE": val}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)

    # Create a Lambda that sleeps briefly to simulate real work.
    code = (
        "import time\n"
        "def handler(event, context):\n"
        "    time.sleep(0.5)\n"
        "    return {'done': True}\n"
    )
    lam.create_function(
        FunctionName="sfn-timeout-test-fn",
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    fn_arn = "arn:aws:lambda:us-east-1:000000000000:function:sfn-timeout-test-fn"

    _set_wait_scale(0)
    try:
        definition = json.dumps({
            "StartAt": "CallLambda",
            "States": {
                "CallLambda": {
                    "Type": "Task",
                    "Resource": fn_arn,
                    "End": True,
                },
            },
        })
        sm = sfn.create_state_machine(
            name="qa-sfn-timeout-test",
            definition=definition,
            roleArn="arn:aws:iam::000000000000:role/R",
        )
        sm_arn = sm["stateMachineArn"]

        exec_resp = sfn.start_execution(stateMachineArn=sm_arn, input="{}")
        desc = _wait_sfn(sfn, exec_resp["executionArn"], timeout=10)

        assert desc["status"] == "SUCCEEDED", (
            f"Lambda Task should succeed with SFN_WAIT_SCALE=0, "
            f"got {desc['status']}"
        )
        assert json.loads(desc["output"]) == {"done": True}

        sfn.delete_state_machine(stateMachineArn=sm_arn)
    finally:
        _set_wait_scale(1.0)


def test_sfn_wait_scale_zero_skips_wait(sfn):
    """SFN_WAIT_SCALE=0 skips Wait state sleeps entirely.

    Uses /_ministack/config to set the scale on the running server,
    then starts an async execution with a 60s Wait that should complete
    almost instantly. Marked serial via conftest._SERIAL_TESTS because
    it mutates server-global state.
    """
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    def _set_wait_scale(val):
        req = urllib.request.Request(
            f"{endpoint}/_ministack/config",
            data=json.dumps({"stepfunctions._SFN_WAIT_SCALE": val}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)

    _set_wait_scale(0)
    try:
        definition = json.dumps({
            "StartAt": "LongWait",
            "States": {
                "LongWait": {
                    "Type": "Wait",
                    "Seconds": 60,
                    "Next": "Done",
                },
                "Done": {"Type": "Pass", "Result": "ok", "End": True},
            },
        })
        sm = sfn.create_state_machine(
            name="qa-sfn-wait-scale",
            definition=definition,
            roleArn="arn:aws:iam::000000000000:role/R",
        )
        sm_arn = sm["stateMachineArn"]

        t0 = time.time()
        exec_resp = sfn.start_execution(stateMachineArn=sm_arn, input="{}")
        exec_arn = exec_resp["executionArn"]

        # Poll until complete (should be near-instant with scale=0).
        for _ in range(30):
            desc = sfn.describe_execution(executionArn=exec_arn)
            if desc["status"] != "RUNNING":
                break
            time.sleep(0.2)
        elapsed = time.time() - t0

        assert desc["status"] == "SUCCEEDED", f"Expected SUCCEEDED, got {desc['status']}"
        assert json.loads(desc["output"]) == "ok"
        assert elapsed < 5, f"Expected < 5s with scale=0, took {elapsed:.1f}s"

        sfn.delete_state_machine(stateMachineArn=sm_arn)
    finally:
        _set_wait_scale(1.0)


# ---------------------------------------------------------------------------
# Step Functions versioning (PublishStateMachineVersion / List / Delete)
# ---------------------------------------------------------------------------

def test_sfn_publish_state_machine_version(sfn):
    """Publish two versions, list them, delete one, re-list.

    Without this action AWS SDK calls (boto3, botocore, etc.) to
    ``ListStateMachineVersions`` returned ``InvalidAction: Unknown
    action``. (terraform only) terraform-provider-aws 6.x calls this
    action unconditionally on every ``aws_sfn_state_machine`` refresh,
    so the missing action also broke ``terraform plan`` even when no
    versioning was configured.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    name = f"version-test-{uid}"
    definition = json.dumps({
        "StartAt": "P",
        "States": {"P": {"Type": "Pass", "End": True}},
    })
    sm = sfn.create_state_machine(
        name=name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]

    try:
        # List with no versions → empty.
        listed = sfn.list_state_machine_versions(stateMachineArn=sm_arn)
        assert listed["stateMachineVersions"] == []

        # Publish v1 + v2.
        v1 = sfn.publish_state_machine_version(
            stateMachineArn=sm_arn, description="first",
        )
        assert v1["stateMachineVersionArn"] == f"{sm_arn}:1"

        v2 = sfn.publish_state_machine_version(
            stateMachineArn=sm_arn, description="second",
        )
        assert v2["stateMachineVersionArn"] == f"{sm_arn}:2"

        # List — newest first.
        listed = sfn.list_state_machine_versions(stateMachineArn=sm_arn)
        arns = [v["stateMachineVersionArn"] for v in listed["stateMachineVersions"]]
        assert arns == [f"{sm_arn}:2", f"{sm_arn}:1"]

        # Delete v1; v2 remains.
        sfn.delete_state_machine_version(stateMachineVersionArn=f"{sm_arn}:1")
        listed = sfn.list_state_machine_versions(stateMachineArn=sm_arn)
        assert [v["stateMachineVersionArn"] for v in listed["stateMachineVersions"]] == [f"{sm_arn}:2"]
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_publish_state_machine_version_revisionid_precondition(sfn):
    """revisionId is an AWS optimistic-concurrency precondition.

    Publish with a stale revisionId raises ConflictException. Publish with
    no revisionId (or the current one) succeeds. After UpdateStateMachine
    the revisionId rotates, so a previously valid revisionId becomes stale.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    name = f"version-rev-{uid}"
    sm = sfn.create_state_machine(
        name=name,
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]

    try:
        current_rev = sfn.describe_state_machine(stateMachineArn=sm_arn)["revisionId"]

        # Supplying the current revisionId passes the precondition.
        v1 = sfn.publish_state_machine_version(
            stateMachineArn=sm_arn, revisionId=current_rev,
        )
        assert v1["stateMachineVersionArn"] == f"{sm_arn}:1"

        # UpdateStateMachine rotates revisionId.
        sfn.update_state_machine(
            stateMachineArn=sm_arn,
            definition=json.dumps({
                "StartAt": "P",
                "States": {"P": {"Type": "Pass", "Result": "changed", "End": True}},
            }),
        )
        rev_after_update = sfn.describe_state_machine(stateMachineArn=sm_arn)["revisionId"]
        assert rev_after_update != current_rev

        # The old revisionId is now stale; Publish with it raises.
        with pytest.raises(ClientError) as exc:
            sfn.publish_state_machine_version(
                stateMachineArn=sm_arn, revisionId=current_rev,
            )
        assert exc.value.response["Error"]["Code"] == "ConflictException"

        # Publish without revisionId always succeeds (no precondition).
        v2 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        assert v2["stateMachineVersionArn"] == f"{sm_arn}:2"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_describe_state_machine_on_version_arn(sfn):
    """DescribeStateMachine accepts a qualified version ARN and returns
    the snapshot captured at publish time."""
    uid = _uuid_mod.uuid4().hex[:8]
    name = f"describe-version-{uid}"
    initial_definition = json.dumps({
        "StartAt": "P",
        "States": {"P": {"Type": "Pass", "Result": "v1", "End": True}},
    })
    sm = sfn.create_state_machine(
        name=name, definition=initial_definition,
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]

    try:
        v1 = sfn.publish_state_machine_version(
            stateMachineArn=sm_arn, description="first-snapshot",
        )

        # Mutate the base state machine so v1's definition differs from
        # the current one — tests that describe-on-version returns the
        # snapshot, not the live state.
        sfn.update_state_machine(
            stateMachineArn=sm_arn,
            definition=json.dumps({
                "StartAt": "P",
                "States": {"P": {"Type": "Pass", "Result": "v-live", "End": True}},
            }),
        )

        described = sfn.describe_state_machine(
            stateMachineArn=v1["stateMachineVersionArn"],
        )
        assert described["stateMachineArn"] == v1["stateMachineVersionArn"]
        assert described["description"] == "first-snapshot"
        # Definition echoed back is v1's snapshot, not the live one.
        assert json.loads(described["definition"])["States"]["P"]["Result"] == "v1"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_list_state_machine_versions_missing_state_machine(sfn):
    """List against a nonexistent state machine ARN → StateMachineDoesNotExist."""
    missing_arn = "arn:aws:states:us-east-1:000000000000:stateMachine:does-not-exist-xyz"
    with pytest.raises(ClientError) as exc:
        sfn.list_state_machine_versions(stateMachineArn=missing_arn)
    assert exc.value.response["Error"]["Code"] == "StateMachineDoesNotExist"


def test_sfn_create_state_machine_publish_true_returns_version_arn(sfn):
    """CreateStateMachine with publish=True auto-creates v1 and carries
    stateMachineVersionArn in the response, matching AWS's contract
    for the publish parameter."""
    uid = _uuid_mod.uuid4().hex[:8]
    name = f"pub-create-{uid}"
    resp = sfn.create_state_machine(
        name=name,
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn="arn:aws:iam::000000000000:role/r",
        publish=True,
    )
    sm_arn = resp["stateMachineArn"]
    try:
        assert resp["stateMachineVersionArn"] == f"{sm_arn}:1"
        listed = sfn.list_state_machine_versions(stateMachineArn=sm_arn)
        assert len(listed["stateMachineVersions"]) == 1
        assert listed["stateMachineVersions"][0]["stateMachineVersionArn"] == f"{sm_arn}:1"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_publish_state_machine_version_numbers_never_reused(sfn):
    """AWS never reuses a version number after delete: publish v1, v2,
    v3, delete v3, publish again → v4 (not v3)."""
    uid = _uuid_mod.uuid4().hex[:8]
    name = f"version-monotonic-{uid}"
    sm = sfn.create_state_machine(
        name=name,
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]
    try:
        v1 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        v2 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        v3 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        assert v3["stateMachineVersionArn"] == f"{sm_arn}:3"

        # Delete latest, then publish again — must be v4 (not v3).
        sfn.delete_state_machine_version(stateMachineVersionArn=v3["stateMachineVersionArn"])
        v4 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        assert v4["stateMachineVersionArn"] == f"{sm_arn}:4"

        # Delete a middle one and republish — also v5, not v3.
        sfn.delete_state_machine_version(stateMachineVersionArn=v2["stateMachineVersionArn"])
        v5 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        assert v5["stateMachineVersionArn"] == f"{sm_arn}:5"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


# ---------------------------------------------------------------------------
# Step Functions aliasing (Create/Describe/Update/Delete/List)
# ---------------------------------------------------------------------------

def test_sfn_state_machine_alias_single_version(sfn):
    """Create an alias pointing 100% at one version, then update to two.

    Without this family any AWS SDK call to ``ListStateMachineAliases``
    returns ``InvalidAction: Unknown action``. (terraform only)
    terraform-provider-aws 6.x calls this action unconditionally on
    every ``aws_sfn_state_machine`` refresh, so the missing action also
    broke ``terraform plan`` for any aws_sfn_state_machine resource.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    name = f"alias-test-{uid}"
    sm = sfn.create_state_machine(
        name=name,
        definition=json.dumps({
            "StartAt": "P",
            "States": {"P": {"Type": "Pass", "End": True}},
        }),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]

    try:
        v1 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        v2 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)

        # Empty initial list.
        listed = sfn.list_state_machine_aliases(stateMachineArn=sm_arn)
        assert listed["stateMachineAliases"] == []

        create = sfn.create_state_machine_alias(
            name="stable",
            description="Points 100% at v1",
            routingConfiguration=[
                {"stateMachineVersionArn": v1["stateMachineVersionArn"], "weight": 100},
            ],
        )
        alias_arn = create["stateMachineAliasArn"]
        assert alias_arn == f"{sm_arn}:stable"

        described = sfn.describe_state_machine_alias(stateMachineAliasArn=alias_arn)
        assert described["name"] == "stable"
        assert described["routingConfiguration"] == [
            {"stateMachineVersionArn": v1["stateMachineVersionArn"], "weight": 100},
        ]

        # Update to weighted two-version routing.
        sfn.update_state_machine_alias(
            stateMachineAliasArn=alias_arn,
            routingConfiguration=[
                {"stateMachineVersionArn": v1["stateMachineVersionArn"], "weight": 30},
                {"stateMachineVersionArn": v2["stateMachineVersionArn"], "weight": 70},
            ],
        )
        updated = sfn.describe_state_machine_alias(stateMachineAliasArn=alias_arn)
        weights = {r["stateMachineVersionArn"]: r["weight"]
                   for r in updated["routingConfiguration"]}
        assert weights[v1["stateMachineVersionArn"]] == 30
        assert weights[v2["stateMachineVersionArn"]] == 70

        listed = sfn.list_state_machine_aliases(stateMachineArn=sm_arn)
        assert [a["stateMachineAliasArn"] for a in listed["stateMachineAliases"]] == [alias_arn]

        sfn.delete_state_machine_alias(stateMachineAliasArn=alias_arn)
        # Idempotent delete.
        sfn.delete_state_machine_alias(stateMachineAliasArn=alias_arn)
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_alias_rejects_weights_not_summing_to_100(sfn):
    """Alias creation requires routing weights sum to 100."""
    uid = _uuid_mod.uuid4().hex[:8]
    name = f"alias-weight-{uid}"
    sm = sfn.create_state_machine(
        name=name,
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]
    try:
        v1 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        with pytest.raises(ClientError) as exc:
            sfn.create_state_machine_alias(
                name="bad-weights",
                routingConfiguration=[
                    {"stateMachineVersionArn": v1["stateMachineVersionArn"], "weight": 50},
                ],
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_alias_rejects_invalid_name(sfn):
    """Alias names must match AWS regex [0-9A-Za-z_-]+ within 1-80 chars."""
    uid = _uuid_mod.uuid4().hex[:8]
    sm = sfn.create_state_machine(
        name=f"alias-bad-name-{uid}",
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]
    try:
        v1 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        routing = [{"stateMachineVersionArn": v1["stateMachineVersionArn"], "weight": 100}]
        # Space is not in the allowed pattern.
        with pytest.raises(ClientError) as exc:
            sfn.create_state_machine_alias(name="bad name", routingConfiguration=routing)
        assert exc.value.response["Error"]["Code"] == "ValidationException"
        # Empty is rejected by length guard (and botocore typically blocks
        # the call client-side — but if it slips through our server
        # enforces). Skipping the empty-name test because boto3 rejects
        # ParamValidationError before reaching the wire.
        # Length > 80 is rejected.
        with pytest.raises(ClientError) as exc:
            sfn.create_state_machine_alias(name="a" * 81, routingConfiguration=routing)
        assert exc.value.response["Error"]["Code"] == "ValidationException"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_alias_rejects_duplicate_version_entries(sfn):
    """routingConfiguration must not contain the same version ARN twice."""
    uid = _uuid_mod.uuid4().hex[:8]
    sm = sfn.create_state_machine(
        name=f"alias-dup-{uid}",
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]
    try:
        v1 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        with pytest.raises(ClientError) as exc:
            sfn.create_state_machine_alias(
                name="dup",
                routingConfiguration=[
                    {"stateMachineVersionArn": v1["stateMachineVersionArn"], "weight": 50},
                    {"stateMachineVersionArn": v1["stateMachineVersionArn"], "weight": 50},
                ],
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_describe_state_machine_on_alias_arn(sfn):
    """DescribeStateMachine(alias_arn) returns the alias's routing plus
    the base state machine's definition/roleArn."""
    uid = _uuid_mod.uuid4().hex[:8]
    sm = sfn.create_state_machine(
        name=f"desc-alias-{uid}",
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]
    try:
        v1 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        alias = sfn.create_state_machine_alias(
            name="live",
            description="100% v1 for now",
            routingConfiguration=[
                {"stateMachineVersionArn": v1["stateMachineVersionArn"], "weight": 100},
            ],
        )
        alias_arn = alias["stateMachineAliasArn"]

        described = sfn.describe_state_machine(stateMachineArn=alias_arn)
        assert described["stateMachineArn"] == alias_arn
        assert described["description"] == "100% v1 for now"
        # Non-routing fields fall back to the base state machine.
        assert described["definition"] == json.dumps(
            {"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}
        )
        assert described["roleArn"] == "arn:aws:iam::000000000000:role/r"
        # routingConfiguration is NOT part of DescribeStateMachine's response
        # shape — AWS doesn't carry it, boto3 strips unknown fields. Callers
        # who need routing data call DescribeStateMachineAlias instead.
        assert "routingConfiguration" not in described
    finally:
        sfn.delete_state_machine_alias(stateMachineAliasArn=f"{sm_arn}:live")
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_delete_state_machine_version_blocked_by_alias(sfn):
    """DeleteStateMachineVersion raises ConflictException while an alias
    still routes to that version."""
    uid = _uuid_mod.uuid4().hex[:8]
    sm = sfn.create_state_machine(
        name=f"del-blocked-{uid}",
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]
    try:
        v1 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
        alias = sfn.create_state_machine_alias(
            name="pin",
            routingConfiguration=[
                {"stateMachineVersionArn": v1["stateMachineVersionArn"], "weight": 100},
            ],
        )
        alias_arn = alias["stateMachineAliasArn"]

        # Blocked while alias still references v1.
        with pytest.raises(ClientError) as exc:
            sfn.delete_state_machine_version(
                stateMachineVersionArn=v1["stateMachineVersionArn"],
            )
        assert exc.value.response["Error"]["Code"] == "ConflictException"

        # Unblocks once alias removed.
        sfn.delete_state_machine_alias(stateMachineAliasArn=alias_arn)
        sfn.delete_state_machine_version(
            stateMachineVersionArn=v1["stateMachineVersionArn"],
        )  # no-raise
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_create_alias_rejects_malformed_routing_config(sfn):
    """A non-list routingConfiguration must surface as ValidationException
    rather than crashing on routing[0] indexing."""
    uid = _uuid_mod.uuid4().hex[:8]
    sm = sfn.create_state_machine(
        name=f"alias-malformed-{uid}",
        definition=json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}),
        roleArn="arn:aws:iam::000000000000:role/r",
    )
    sm_arn = sm["stateMachineArn"]
    try:
        # boto3 client-side may reject malformed shapes before hitting
        # the wire (it knows the SDK model). Test by hand-rolling the
        # request via boto3's low-level invoke.
        # Simpler: pass a list whose first entry isn't a dict.
        with pytest.raises((ClientError, Exception)) as exc_info:
            # Some shapes get caught by boto3 ParamValidationError; we
            # accept either ClientError or ParamValidationError as proof
            # that the request didn't reach our server crashed.
            sfn.create_state_machine_alias(
                name="bad",
                routingConfiguration=[
                    {"weight": 100},  # missing stateMachineVersionArn
                ],
            )
        # If it was a ClientError, confirm code; if ParamValidationError,
        # boto3 caught it client-side which is also acceptable.
        if isinstance(exc_info.value, ClientError):
            assert exc_info.value.response["Error"]["Code"] == "ValidationException"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


# ---------------------------------------------------------------------------
# Regression: contextvars propagation to background execution thread (#639)
# ---------------------------------------------------------------------------


def test_sfn_execution_proceeds_under_non_default_account_id():
    """Execution background thread must inherit the request's account context.

    When a caller uses a 12-digit access-key as their account ID (the
    documented per-account-isolation pattern), the execution record is stored
    in AccountScopedDict under that account. Without contextvars propagation
    into the threading.Thread that runs _run_execution, the worker thread
    looks up the execution under the default account and silently returns,
    leaving the execution stuck at ExecutionStarted forever.

    Regression for #639. The fix wraps the background thread target with
    contextvars.copy_context().run so the request's account/region context
    survives the thread hop.
    """
    import boto3
    from botocore.config import Config

    # Borrow ENDPOINT + REGION from conftest's _default_kwargs without
    # importing private names; rebuild a client with a 12-digit access key.
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    region = os.environ.get("MINISTACK_REGION", "us-east-1")

    alt_account_id = "123456789012"  # AWS docs placeholder account
    alt_kwargs = dict(
        endpoint_url=endpoint,
        aws_access_key_id=alt_account_id,
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"max_attempts": 0}),
    )
    sfn = boto3.client("stepfunctions", **alt_kwargs)

    uid = _uuid_mod.uuid4().hex[:8]
    sm_name = f"ctxvars-{uid}"
    sm = sfn.create_state_machine(
        name=sm_name,
        definition=json.dumps(
            {
                "StartAt": "P",
                "States": {"P": {"Type": "Pass", "End": True}},
            }
        ),
        roleArn=f"arn:aws:iam::{alt_account_id}:role/r",
        type="STANDARD",
    )
    sm_arn = sm["stateMachineArn"]
    try:
        # ARN must be scoped to the alt account, not 000000000000.
        assert f":states:{region}:{alt_account_id}:stateMachine:" in sm_arn

        exec_resp = sfn.start_execution(
            stateMachineArn=sm_arn,
            name=f"e-{uid}",
            input="{}",
        )
        desc = _wait_sfn(sfn, exec_resp["executionArn"], timeout=10)
        assert desc["status"] == "SUCCEEDED", (
            f"Execution under account {alt_account_id} stalled at "
            f"{desc['status']!r}. Background thread lost the account "
            f"contextvars — see issue #639."
        )

        # History must have progressed past ExecutionStarted.
        hist = sfn.get_execution_history(executionArn=exec_resp["executionArn"])
        event_types = [e["type"] for e in hist["events"]]
        assert "ExecutionStarted" in event_types
        assert "ExecutionSucceeded" in event_types, (
            f"Execution never reached terminal Succeed; events: {event_types}"
        )
    finally:
        sfn.delete_state_machine(stateMachineArn=sm_arn)


def _alt_account_sfn_client(account_id):
    import boto3
    from botocore.config import Config
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    region = os.environ.get("MINISTACK_REGION", "us-east-1")
    return boto3.client(
        "stepfunctions",
        endpoint_url=endpoint,
        aws_access_key_id=account_id,
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"max_attempts": 0}),
    )


def test_sfn_parallel_branches_proceed_under_non_default_account_id():
    """Each Parallel branch thread must inherit the request's account context.

    Before #640 the Parallel branch wrapper shared a single Context across N
    concurrent threads — the second branch to start raised RuntimeError in
    Thread._bootstrap_inner, which was logged to stderr but never reached
    errors[idx], so the Parallel state returned [result_0, None, None, ...]
    silently. Per-branch contextvars.copy_context() fixes both the account
    lookup and the shared-context contention.
    """
    sfn = _alt_account_sfn_client("123456789012")
    uid = _uuid_mod.uuid4().hex[:8]
    sm_name = f"ctxvars-parallel-{uid}"
    sm = sfn.create_state_machine(
        name=sm_name,
        definition=json.dumps({
            "StartAt": "Fan",
            "States": {
                "Fan": {
                    "Type": "Parallel",
                    "End": True,
                    "Branches": [
                        {"StartAt": f"B{i}", "States": {f"B{i}": {"Type": "Pass", "Result": i, "End": True}}}
                        for i in range(4)
                    ],
                }
            },
        }),
        roleArn="arn:aws:iam::123456789012:role/r",
        type="STANDARD",
    )["stateMachineArn"]
    try:
        exec_resp = sfn.start_execution(stateMachineArn=sm, name=f"e-{uid}", input="{}")
        desc = _wait_sfn(sfn, exec_resp["executionArn"], timeout=10)
        assert desc["status"] == "SUCCEEDED", f"Parallel stalled: {desc.get('status')}"
        out = json.loads(desc["output"])
        assert out == [0, 1, 2, 3], f"Branches dropped silently: {out}"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm)


def test_sfn_map_iterations_proceed_under_non_default_account_id():
    """Each Map ThreadPoolExecutor worker must inherit the request's account
    context. Same shared-Context regression mode as Parallel, but for Map.
    Use MaxConcurrency: 0 (unbounded) so workers definitely run concurrently.
    """
    sfn = _alt_account_sfn_client("123456789012")
    uid = _uuid_mod.uuid4().hex[:8]
    sm_name = f"ctxvars-map-{uid}"
    sm = sfn.create_state_machine(
        name=sm_name,
        definition=json.dumps({
            "StartAt": "Loop",
            "States": {
                "Loop": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "MaxConcurrency": 0,
                    "Iterator": {
                        "StartAt": "Echo",
                        "States": {"Echo": {"Type": "Pass", "End": True}},
                    },
                    "End": True,
                }
            },
        }),
        roleArn="arn:aws:iam::123456789012:role/r",
        type="STANDARD",
    )["stateMachineArn"]
    try:
        exec_resp = sfn.start_execution(
            stateMachineArn=sm,
            name=f"e-{uid}",
            input=json.dumps({"items": [1, 2, 3, 4, 5]}),
        )
        desc = _wait_sfn(sfn, exec_resp["executionArn"], timeout=10)
        assert desc["status"] == "SUCCEEDED", f"Map stalled: {desc.get('status')}"
        out = json.loads(desc["output"])
        assert out == [1, 2, 3, 4, 5], f"Map items dropped silently: {out}"
    finally:
        sfn.delete_state_machine(stateMachineArn=sm)


# ---------------------------------------------------------------------------
# JSONata variable assignment (`Assign` field + `$variable` refs) — issue #645
# ---------------------------------------------------------------------------

def test_sfn_jsonata_assign_pass_then_choice_references_variable(sfn, sfn_sync):
    """Regression for #645: exact issue repro. A Pass state's `Assign` binds
    a variable from `$states.result`; a downstream Choice's `Condition`
    references it via `$myList`. Before the fix the evaluator raised
    `States.QueryEvaluationError: Unsupported JSONata expression: $myList`.
    """
    import uuid as _uuid_local
    sm_name = f"jsonata-assign-{_uuid_local.uuid4().hex[:8]}"
    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        roleArn="arn:aws:iam::000000000000:role/test-role",
        definition=json.dumps({
            "QueryLanguage": "JSONata",
            "StartAt": "SetVars",
            "States": {
                "SetVars": {
                    "Type": "Pass",
                    "Output": {"items": ["a", "b"]},
                    "Assign": {"myList": "{% $states.result.items %}"},
                    "Next": "CheckList",
                },
                "CheckList": {
                    "Type": "Choice",
                    "Choices": [{
                        "Next": "HasItems",
                        "Condition": "{% $count($myList) > 0 %}",
                    }],
                    "Default": "Empty",
                },
                "HasItems": {"Type": "Succeed"},
                "Empty": {"Type": "Succeed"},
            },
        }),
    )["stateMachineArn"]
    try:
        resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input="{}")
        assert resp["status"] == "SUCCEEDED", f"Status: {resp.get('status')} cause={resp.get('cause')}"
        # `Empty` is the fallback — if the variable lookup or $count failed,
        # we'd land there instead of `HasItems`.
        events = sfn.get_execution_history(executionArn=resp["executionArn"])["events"]
        entered = [e["stateEnteredEventDetails"]["name"]
                   for e in events if "stateEnteredEventDetails" in e]
        assert "HasItems" in entered, f"Expected HasItems; entered={entered}"
        assert "Empty" not in entered
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_jsonata_assign_variable_survives_across_states(sfn_sync):
    """Variables set on one state must be visible on every subsequent state
    (execution-scoped, not state-scoped)."""
    import uuid as _uuid_local
    sm_name = f"jsonata-assign-chain-{_uuid_local.uuid4().hex[:8]}"
    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        roleArn="arn:aws:iam::000000000000:role/test-role",
        definition=json.dumps({
            "QueryLanguage": "JSONata",
            "StartAt": "A",
            "States": {
                "A": {
                    "Type": "Pass",
                    "Assign": {"x": 41},
                    "Next": "B",
                },
                "B": {
                    "Type": "Pass",
                    "Assign": {"y": "{% $x + 1 %}"},
                    "Next": "C",
                },
                "C": {
                    "Type": "Pass",
                    "Output": "{% $y %}",
                    "End": True,
                },
            },
        }),
    )["stateMachineArn"]
    try:
        resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input="{}")
        assert resp["status"] == "SUCCEEDED"
        assert json.loads(resp["output"]) == 42
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_jsonata_assign_dotted_variable_path(sfn_sync):
    """`$user.email` resolves the `email` key inside the assigned `$user` dict."""
    import uuid as _uuid_local
    sm_name = f"jsonata-assign-dotted-{_uuid_local.uuid4().hex[:8]}"
    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        roleArn="arn:aws:iam::000000000000:role/test-role",
        definition=json.dumps({
            "QueryLanguage": "JSONata",
            "StartAt": "Bind",
            "States": {
                "Bind": {
                    "Type": "Pass",
                    "Assign": {"user": {"email": "alice@example.com", "id": 7}},
                    "Next": "Use",
                },
                "Use": {
                    "Type": "Pass",
                    "Output": "{% $user.email %}",
                    "End": True,
                },
            },
        }),
    )["stateMachineArn"]
    try:
        resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input="{}")
        assert resp["status"] == "SUCCEEDED"
        assert json.loads(resp["output"]) == "alice@example.com"
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_jsonata_undefined_variable_raises_query_eval_error(sfn_sync):
    """Referencing an unbound `$var` must surface as `States.QueryEvaluationError`
    (the documented AWS error code for JSONata evaluation failures), not as a
    generic Runtime error or silent fallthrough.
    """
    import uuid as _uuid_local
    sm_name = f"jsonata-undef-{_uuid_local.uuid4().hex[:8]}"
    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        roleArn="arn:aws:iam::000000000000:role/test-role",
        definition=json.dumps({
            "QueryLanguage": "JSONata",
            "StartAt": "Use",
            "States": {
                "Use": {
                    "Type": "Pass",
                    "Output": "{% $never_set %}",
                    "End": True,
                },
            },
        }),
    )["stateMachineArn"]
    try:
        resp = sfn_sync.start_sync_execution(stateMachineArn=sm_arn, input="{}")
        assert resp["status"] == "FAILED"
        assert resp.get("error") == "States.QueryEvaluationError"
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_jsonata_assign_reuses_input(sfn_sync):
    """`Assign` can reference `$states.input` (always available)."""
    import uuid as _uuid_local
    sm_name = f"jsonata-input-{_uuid_local.uuid4().hex[:8]}"
    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        roleArn="arn:aws:iam::000000000000:role/test-role",
        definition=json.dumps({
            "QueryLanguage": "JSONata",
            "StartAt": "Bind",
            "States": {
                "Bind": {
                    "Type": "Pass",
                    "Assign": {"name": "{% $states.input.who %}"},
                    "Next": "Out",
                },
                "Out": {
                    "Type": "Pass",
                    "Output": "{% 'hello ' & $name %}",
                    "End": True,
                },
            },
        }),
    )["stateMachineArn"]
    try:
        resp = sfn_sync.start_sync_execution(
            stateMachineArn=sm_arn,
            input=json.dumps({"who": "world"}),
        )
        assert resp["status"] == "SUCCEEDED"
        assert json.loads(resp["output"]) == "hello world"
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


# ---------------------------------------------------------------------------
# JSONata standard library — issue #669
# ---------------------------------------------------------------------------

def _sfn_run_pass_output(sfn_sync, output_expr, input_obj=None):
    """Round-trip helper: build a single Pass state whose Output is `output_expr`,
    run it sync, return the parsed JSON output."""
    sm_name = f"jsonata-fn-{_uuid_mod.uuid4().hex[:8]}"
    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
        definition=json.dumps({
            "QueryLanguage": "JSONata",
            "StartAt": "Run",
            "States": {
                "Run": {
                    "Type": "Pass",
                    "Output": output_expr,
                    "End": True,
                },
            },
        }),
    )["stateMachineArn"]
    try:
        resp = sfn_sync.start_sync_execution(
            stateMachineArn=sm_arn,
            input=json.dumps(input_obj or {}),
        )
        assert resp["status"] == "SUCCEEDED", \
            f"Execution failed: {resp.get('error')} - {resp.get('cause')}"
        return json.loads(resp["output"])
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_jsonata_exists_in_choice_condition_issue_669(sfn_sync):
    """The exact example from issue #669: `$exists($states.input.userId)` used
    as a Choice `Condition` must route correctly whether the field is present
    or missing (returning false on missing rather than failing the execution)."""
    sm_name = f"jsonata-exists-{_uuid_mod.uuid4().hex[:8]}"
    sm_arn = sfn_sync.create_state_machine(
        name=sm_name,
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
        definition=json.dumps({
            "QueryLanguage": "JSONata",
            "StartAt": "Route",
            "States": {
                "Route": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Condition": "{% $exists($states.input.userId) %}",
                            "Next": "HasUser",
                        },
                    ],
                    "Default": "NoUser",
                },
                "HasUser": {"Type": "Pass", "Output": {"branch": "has"}, "End": True},
                "NoUser":  {"Type": "Pass", "Output": {"branch": "missing"}, "End": True},
            },
        }),
    )["stateMachineArn"]
    try:
        has = sfn_sync.start_sync_execution(
            stateMachineArn=sm_arn, input=json.dumps({"userId": "u-42"}))
        assert has["status"] == "SUCCEEDED"
        assert json.loads(has["output"]) == {"branch": "has"}

        miss = sfn_sync.start_sync_execution(
            stateMachineArn=sm_arn, input=json.dumps({}))
        assert miss["status"] == "SUCCEEDED"
        assert json.loads(miss["output"]) == {"branch": "missing"}
    finally:
        sfn_sync.delete_state_machine(stateMachineArn=sm_arn)


def test_sfn_jsonata_string_functions(sfn_sync):
    out = _sfn_run_pass_output(sfn_sync, {
        "upper":     "{% $uppercase('hello') %}",
        "lower":     "{% $lowercase('HELLO') %}",
        "sub2":      "{% $substring('abcdef', 2) %}",
        "sub3":      "{% $substring('abcdef', 1, 3) %}",
        "trim":      "{% $trim('  a   b  ') %}",
        "contains":  "{% $contains('hello world', 'world') %}",
        "split":     "{% $split('a,b,c', ',') %}",
        "join":      "{% $join(['a', 'b', 'c'], '-') %}",
        "replace":   "{% $replace('aXbXc', 'X', '_') %}",
        "pad_right": "{% $pad('x', 4, '.') %}",
        "pad_left":  "{% $pad('x', -4, '.') %}",
    })
    assert out == {
        "upper": "HELLO", "lower": "hello",
        "sub2": "cdef", "sub3": "bcd",
        "trim": "a b", "contains": True,
        "split": ["a", "b", "c"], "join": "a-b-c",
        "replace": "a_b_c", "pad_right": "x...", "pad_left": "...x",
    }


def test_sfn_jsonata_numeric_functions(sfn_sync):
    out = _sfn_run_pass_output(sfn_sync, {
        "sum":     "{% $sum([1, 2, 3, 4]) %}",
        "avg":     "{% $average([2, 4, 6]) %}",
        "max":     "{% $max([3, 1, 2]) %}",
        "min":     "{% $min([3, 1, 2]) %}",
        "abs":     "{% $abs(-7) %}",
        "floor":   "{% $floor(2.9) %}",
        "ceil":    "{% $ceil(2.1) %}",
        "round":   "{% $round(2.5) %}",
        "roundp":  "{% $round(2.345, 2) %}",
        "power":   "{% $power(2, 8) %}",
        "sqrt":    "{% $sqrt(16) %}",
    })
    # $round uses banker's rounding: 2.5 -> 2 (round-half-to-even)
    assert out == {
        "sum": 10, "avg": 4, "max": 3, "min": 1,
        "abs": 7, "floor": 2, "ceil": 3,
        "round": 2, "roundp": 2.35,
        "power": 256, "sqrt": 4,
    }


def test_sfn_jsonata_array_functions(sfn_sync):
    out = _sfn_run_pass_output(sfn_sync, {
        "sort":     "{% $sort([3, 1, 2]) %}",
        "reverse":  "{% $reverse([1, 2, 3]) %}",
        "distinct": "{% $distinct([1, 2, 2, 3, 1]) %}",
        "append":   "{% $append([1, 2], [3, 4]) %}",
    })
    assert out == {
        "sort": [1, 2, 3],
        "reverse": [3, 2, 1],
        "distinct": [1, 2, 3],
        "append": [1, 2, 3, 4],
    }


def test_sfn_jsonata_object_functions(sfn_sync):
    out = _sfn_run_pass_output(
        sfn_sync,
        {
            "keys":   "{% $keys($states.input.obj) %}",
            "values": "{% $values($states.input.obj) %}",
            "look":   "{% $lookup($states.input.obj, 'a') %}",
        },
        input_obj={"obj": {"a": 1, "b": 2}},
    )
    assert out == {"keys": ["a", "b"], "values": [1, 2], "look": 1}


def test_sfn_jsonata_type_and_boolean(sfn_sync):
    out = _sfn_run_pass_output(sfn_sync, {
        "t_str":  "{% $type('s') %}",
        "t_num":  "{% $type(1) %}",
        "t_arr":  "{% $type([1]) %}",
        "t_obj":  "{% $type({'k': 1}) %}",
        "b_empty": "{% $boolean('') %}",
        "b_str":   "{% $boolean('x') %}",
        "b_zero":  "{% $boolean(0) %}",
    })
    assert out == {
        "t_str": "string", "t_num": "number",
        "t_arr": "array", "t_obj": "object",
        "b_empty": False, "b_str": True, "b_zero": False,
    }


def test_sfn_jsonata_datetime_and_util(sfn_sync):
    out = _sfn_run_pass_output(sfn_sync, {
        "now":          "{% $now() %}",
        "millis":       "{% $millis() %}",
        "uuid":         "{% $uuid() %}",
        "b64_encode":   "{% $base64encode('hello') %}",
        "b64_decode":   "{% $base64decode('aGVsbG8=') %}",
    })
    # $now: ISO-8601 UTC with millisecond precision ending in 'Z'
    assert isinstance(out["now"], str) and out["now"].endswith("Z")
    assert "T" in out["now"]
    assert isinstance(out["millis"], int) and out["millis"] > 0
    assert isinstance(out["uuid"], str) and len(out["uuid"]) == 36
    assert out["b64_encode"] == "aGVsbG8="
    assert out["b64_decode"] == "hello"


def test_sfn_jsonata_exists_distinguishes_null_from_missing(sfn_sync):
    """JSONata spec: `$exists` returns true for an explicit null value (the path
    matches a value), and false only for a missing path. The previous fix
    collapsed both to false — this regression-locks the spec distinction."""
    out = _sfn_run_pass_output(
        sfn_sync,
        {
            "present_value": "{% $exists($states.input.a) %}",
            "explicit_null": "{% $exists($states.input.b) %}",
            "missing_key":   "{% $exists($states.input.c) %}",
            "nested_null":   "{% $exists($states.input.d.x) %}",
            "nested_miss":   "{% $exists($states.input.d.y) %}",
        },
        input_obj={"a": 1, "b": None, "d": {"x": None}},
    )
    assert out == {
        "present_value": True,
        "explicit_null": True,
        "missing_key": False,
        "nested_null": True,
        "nested_miss": False,
    }


def test_sfn_jsonata_regex_literal_in_string_fns(sfn_sync):
    """$contains/$split/$replace accept `/pattern/flags` regex literals."""
    out = _sfn_run_pass_output(sfn_sync, {
        "ci_match":  "{% $contains('Hello World', /hello/i) %}",
        "no_match":  "{% $contains('Hello World', /xyz/) %}",
        "split_ws":  "{% $split('a   b    c', /\\s+/) %}",
        "replace":   "{% $replace('foo123bar456', /[0-9]+/, '#') %}",
        "backref":   "{% $replace('John Smith', /(\\w+) (\\w+)/, '$2 $1') %}",
    })
    assert out == {
        "ci_match": True,
        "no_match": False,
        "split_ws": ["a", "b", "c"],
        "replace": "foo#bar#",
        "backref": "Smith John",
    }


def test_sfn_jsonata_sort_with_comparator_function(sfn_sync):
    """$sort accepts a `function($l, $r){...}` comparator. Sorting descending
    via a custom comparator is the canonical JSONata example."""
    out = _sfn_run_pass_output(sfn_sync, {
        "desc": "{% $sort([3, 1, 4, 1, 5, 9, 2, 6], function($l, $r){$l < $r}) %}",
    })
    assert out == {"desc": [9, 6, 5, 4, 3, 2, 1, 1]}


def test_sfn_jsonata_join_strict_and_lookup_array(sfn_sync):
    """$join requires all-string arrays; $lookup over an array of objects
    returns the matching values."""
    out = _sfn_run_pass_output(
        sfn_sync,
        {
            "join":   "{% $join($states.input.names, '|') %}",
            "look1":  "{% $lookup($states.input.users, 'id') %}",
        },
        input_obj={
            "names": ["a", "b", "c"],
            "users": [{"id": 1}, {"id": 2}, {"id": 3}],
        },
    )
    assert out == {"join": "a|b|c", "look1": [1, 2, 3]}


def test_sfn_jsonata_boolean_array_of_falsy_is_false(sfn_sync):
    """JSONata: array of only-false values is itself false; an array with at
    least one truthy value is true."""
    out = _sfn_run_pass_output(sfn_sync, {
        "all_false": "{% $boolean([false, false, 0, '']) %}",
        "any_true":  "{% $boolean([false, 'x']) %}",
    })
    assert out == {"all_false": False, "any_true": True}


def test_sfn_jsonata_now_picture_and_timezone(sfn_sync):
    """$now(picture) and $now(picture, timezone) format per XPath picture."""
    out = _sfn_run_pass_output(sfn_sync, {
        "date_only":   "{% $now('[Y0001]-[M01]-[D01]') %}",
        "with_tz":     "{% $now('[H01]:[m01][Z]', '+05:30') %}",
        "default":     "{% $now() %}",
    })
    import re as _re
    assert _re.fullmatch(r"\d{4}-\d{2}-\d{2}", out["date_only"])
    assert _re.fullmatch(r"\d{2}:\d{2}\+05:30", out["with_tz"])
    assert out["default"].endswith("Z")


def test_sfn_jsonata_format_number(sfn_sync):
    """$formatNumber supports grouping, decimals, percent, and pos;neg subpictures."""
    out = _sfn_run_pass_output(sfn_sync, {
        "grouped":  "{% $formatNumber(12345.6, '#,###.00') %}",
        "percent":  "{% $formatNumber(0.5, '00.0%') %}",
        "pad":      "{% $formatNumber(7, '000') %}",
        "neg_sub":  "{% $formatNumber(-1234.5, '#,##0.00;(#,##0.00)') %}",
        "plain":    "{% $formatNumber(1234, '0,000') %}",
    })
    assert out == {
        "grouped": "12,345.60",
        "percent": "50.0%",
        "pad": "007",
        "neg_sub": "(1,234.50)",
        "plain": "1,234",
    }
