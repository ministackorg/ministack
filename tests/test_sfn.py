import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

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
    assert desc["status"] == "RUNNING"

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

    # Verify describe result contains cluster data
    describe_clusters = output["describeResult"]["DbClusters"]
    assert "DbCluster" in describe_clusters

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
    assert output["modifyResult"]["DbCluster"]["BackupRetentionPeriod"] == "7"

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
        Runtime="python3.9",
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
