"""Exact service integration contracts, based on official AWS API documentation."""

import io
import json
import time
import uuid
import zipfile

import pytest


def _wait(sfn, arn):
    for _ in range(100):
        response = sfn.describe_execution(executionArn=arn)
        if response["status"] != "RUNNING":
            return response
        time.sleep(0.1)
    pytest.fail("execution did not reach a terminal state")


def _execute(sfn, state, expected_error):
    state = dict(state, Catch=[{
        "ErrorEquals": [expected_error], "ResultPath": "$.caught", "Next": "Recovered",
    }], End=True)
    arn = sfn.create_state_machine(
        name=f"service-fidelity-{uuid.uuid4().hex[:12]}",
        roleArn="arn:aws:iam::000000000000:role/sfn-role",
        definition=json.dumps({"StartAt": "Call", "States": {
            "Call": state, "Recovered": {"Type": "Succeed"},
        }}),
    )["stateMachineArn"]
    try:
        execution = sfn.start_execution(stateMachineArn=arn, input="{}")
        return _wait(sfn, execution["executionArn"])
    finally:
        sfn.delete_state_machine(stateMachineArn=arn)


@pytest.mark.parametrize("operation", ["putItem", "updateItem", "deleteItem"])
def test_optimized_dynamodb_conditional_failure_exact_catch(sfn, ddb, operation):
    """Optimized integration uses DynamoDB.*, not a bare or SDK DynamoDb.* error."""
    table = f"conditional-{uuid.uuid4().hex[:12]}"
    ddb.create_table(TableName=table, BillingMode="PAY_PER_REQUEST",
                     KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                     AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}])
    key = {"pk": {"S": "occupied"}}
    try:
        ddb.put_item(TableName=table, Item=key)
        params = {"TableName": table, "ConditionExpression": "attribute_not_exists(pk)"}
        if operation == "putItem":
            params["Item"] = key
        else:
            params["Key"] = key
        if operation == "updateItem":
            params.update(UpdateExpression="SET counter = :v", ExpressionAttributeValues={":v": {"N": "1"}})
        result = _execute(sfn, {"Type": "Task", "Resource": f"arn:aws:states:::dynamodb:{operation}",
                                "Parameters": params}, "DynamoDB.ConditionalCheckFailedException")
        assert result["status"] == "SUCCEEDED", result
        assert json.loads(result["output"])["caught"]["Error"] == "DynamoDB.ConditionalCheckFailedException"
        assert ddb.get_item(TableName=table, Key=key)["Item"] == key
    finally:
        ddb.delete_table(TableName=table)


def test_optimized_dynamodb_missing_table_exact_catch(sfn):
    result = _execute(sfn, {"Type": "Task", "Resource": "arn:aws:states:::dynamodb:getItem",
                            "Parameters": {"TableName": f"missing-{uuid.uuid4().hex[:12]}",
                                           "Key": {"pk": {"S": "x"}}}}, "DynamoDB.ResourceNotFoundException")
    assert result["status"] == "SUCCEEDED", result
    assert json.loads(result["output"])["caught"]["Error"] == "DynamoDB.ResourceNotFoundException"


@pytest.mark.parametrize("qualifier", [None, "version", "alias"])
def test_sdk_lambda_get_function_reads_configuration_code_and_literal_maps(sfn_sync, lam, qualifier):
    suffix = uuid.uuid4().hex[:12]
    function = f"get-function-{suffix}"
    tags = {"owner.name": "test", "lower-case": "literal", "UPPER": "unchanged"}
    variables = {"lowercase": "value", "UPPER": "other"}
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("index.py", "def handler(e, c): return e")
    lam.create_function(FunctionName=function, Runtime="python3.12", Handler="index.handler",
                        Role="arn:aws:iam::000000000000:role/lambda-role", Code={"ZipFile": buffer.getvalue()},
                        Tags=tags, Environment={"Variables": variables}, Description="published")
    machine = None
    try:
        version = lam.publish_version(FunctionName=function)["Version"]
        lam.create_alias(FunctionName=function, Name="live", FunctionVersion=version)
        lam.update_function_configuration(FunctionName=function, Description="latest")
        params = {"FunctionName": f"arn:aws:lambda:us-east-1:000000000000:function:{function}"}
        if qualifier is not None:
            params["Qualifier"] = version if qualifier == "version" else "live"
        machine = sfn_sync.create_state_machine(
            name=f"get-function-{suffix}", roleArn="arn:aws:iam::000000000000:role/sfn-role",
            definition=json.dumps({"StartAt": "Read", "States": {"Read": {
                "Type": "Task", "Resource": "arn:aws:states:::aws-sdk:lambda:getFunction",
                "Parameters": params, "End": True,
            }}}),
        )["stateMachineArn"]
        result = sfn_sync.start_sync_execution(stateMachineArn=machine, input="{}")
        assert result["status"] == "SUCCEEDED", result
        output = json.loads(result["output"])
        assert output["Configuration"]["FunctionName"] == function
        assert output["Configuration"]["Version"] == (version if qualifier else "$LATEST")
        assert output["Configuration"]["Description"] == ("published" if qualifier else "latest")
        assert output["Configuration"]["Environment"]["Variables"] == variables
        assert output["Code"]["Location"]
        if qualifier is None:
            assert output["Tags"] == tags
    finally:
        if machine:
            sfn_sync.delete_state_machine(stateMachineArn=machine)
        lam.delete_function(FunctionName=function)


def test_sdk_lambda_get_function_missing_function_exact_catch(sfn):
    result = _execute(sfn, {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:lambda:getFunction",
                            "Parameters": {"FunctionName": f"missing-{uuid.uuid4().hex[:12]}"}},
                      "Lambda.ResourceNotFoundException")
    assert result["status"] == "SUCCEEDED", result
    assert json.loads(result["output"])["caught"]["Error"] == "Lambda.ResourceNotFoundException"
