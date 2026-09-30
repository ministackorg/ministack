"""RollbackStack and stack drift detection (DetectStackDrift,
DescribeStackDriftDetectionStatus, DetectStackResourceDrift,
DescribeStackResourceDrifts, DriftInformation)."""

import json
import time
import uuid

import pytest
from botocore.exceptions import ClientError

_FAILING = {
    "Type": "AWS::CloudFormation::CustomResource",
    "Properties": {
        "ServiceToken": "arn:aws:lambda:us-east-1:000000000000:function:cfn-drift-does-not-exist",
    },
}


def _name(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _wait(cfn, name, timeout=30):
    deadline = time.time() + timeout
    status = "UNKNOWN"
    while time.time() < deadline:
        try:
            stack = cfn.describe_stacks(StackName=name)["Stacks"][0]
        except ClientError as exc:
            if "does not exist" in str(exc):
                return {"StackStatus": "DELETE_COMPLETE"}
            raise
        status = stack["StackStatus"]
        if not status.endswith("_IN_PROGRESS"):
            return stack
        time.sleep(0.3)
    raise TimeoutError(f"{name} stuck at {status}")


def _cleanup(cfn, name):
    try:
        cfn.delete_stack(StackName=name)
        _wait(cfn, name)
    except ClientError:
        pass


def _statuses(cfn, name):
    events = cfn.describe_stack_events(StackName=name)["StackEvents"]
    return [(e["LogicalResourceId"], e["ResourceStatus"], e.get("ResourceStatusReason", ""))
            for e in reversed(events)]


def _queue_exists(sqs, name):
    try:
        sqs.get_queue_url(QueueName=name)
        return True
    except ClientError:
        return False


def _detect(cfn, name, **kwargs):
    detection_id = cfn.detect_stack_drift(StackName=name, **kwargs)["StackDriftDetectionId"]
    for _ in range(50):
        status = cfn.describe_stack_drift_detection_status(StackDriftDetectionId=detection_id)
        if status["DetectionStatus"] != "DETECTION_IN_PROGRESS":
            return status
        time.sleep(0.2)
    raise TimeoutError("drift detection did not finish")


def _drifts(cfn, name, **kwargs):
    return {d["LogicalResourceId"]: d for d in
            cfn.describe_stack_resource_drifts(StackName=name, **kwargs)["StackResourceDrifts"]}


# ---------------------------------------------------------------------------
# RollbackStack
# ---------------------------------------------------------------------------

def test_rollback_stack_from_create_failed_deletes_what_the_create_made(cfn, sqs):
    name = _name("rb-create")
    queue = f"{name}-q"
    template = {"Resources": {
        "Q": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": queue}},
        "Bad": {**_FAILING, "DependsOn": "Q"},
    }}
    try:
        cfn.create_stack(StackName=name, TemplateBody=json.dumps(template),
                         DisableRollback=True)
        assert _wait(cfn, name)["StackStatus"] == "CREATE_FAILED"
        assert _queue_exists(sqs, queue)

        stack_id = cfn.rollback_stack(StackName=name)["StackId"]
        assert stack_id.startswith("arn:aws:cloudformation:")
        assert _wait(cfn, name)["StackStatus"] == "ROLLBACK_COMPLETE"
        assert not _queue_exists(sqs, queue)
        events = _statuses(cfn, name)
        assert (name, "ROLLBACK_IN_PROGRESS", "User Initiated") in events
        assert ("Q", "DELETE_COMPLETE", "") in events
        assert events[-1][:2] == (name, "ROLLBACK_COMPLETE")
        assert cfn.describe_stack_resources(StackName=name)["StackResources"] == []
    finally:
        _cleanup(cfn, name)


def test_rollback_stack_from_update_failed_restores_the_previous_stack(cfn, sqs, ssm):
    name = _name("rb-update")
    queue = f"{name}-q"
    added = f"{name}-added"
    param = f"/{name}/p"

    def template(visibility, value, extra):
        resources = {
            "Q": {"Type": "AWS::SQS::Queue",
                  "Properties": {"QueueName": queue, "VisibilityTimeout": visibility}},
            "P": {"Type": "AWS::SSM::Parameter",
                  "Properties": {"Name": param, "Type": "String", "Value": value}},
        }
        if extra:
            resources["Added"] = {"Type": "AWS::SQS::Queue",
                                  "Properties": {"QueueName": added}}
            resources["Bad"] = {**_FAILING, "DependsOn": ["P", "Q", "Added"]}
        return json.dumps({"Resources": resources})

    try:
        cfn.create_stack(StackName=name, TemplateBody=template(30, "v1", False))
        assert _wait(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
        cfn.update_stack(StackName=name, TemplateBody=template(60, "v2", True),
                         DisableRollback=True)
        failed = _wait(cfn, name)
        assert failed["StackStatus"] == "UPDATE_FAILED"
        # DisableRollback kept the partial update in place.
        assert ssm.get_parameter(Name=param)["Parameter"]["Value"] == "v2"
        assert _queue_exists(sqs, added)

        cfn.rollback_stack(StackName=name)
        assert _wait(cfn, name)["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
        assert ssm.get_parameter(Name=param)["Parameter"]["Value"] == "v1"
        url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
        attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["VisibilityTimeout"])
        assert attrs["Attributes"]["VisibilityTimeout"] == "30"
        assert not _queue_exists(sqs, added)
        body = cfn.get_template(StackName=name)["TemplateBody"]
        assert "Bad" not in json.dumps(body)
        listed = {r["LogicalResourceId"] for r in
                  cfn.list_stack_resources(StackName=name)["StackResourceSummaries"]}
        assert listed == {"Q", "P"}
        statuses = [s for lid, s, _ in _statuses(cfn, name) if lid == name]
        assert statuses[-3:] == ["UPDATE_ROLLBACK_IN_PROGRESS",
                                 "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
                                 "UPDATE_ROLLBACK_COMPLETE"]
    finally:
        _cleanup(cfn, name)


def test_rollback_stack_retain_except_on_create_deletes_retained_new_resources(cfn, sqs):
    name = _name("rb-retain")
    queue = f"{name}-q"
    template = {"Resources": {
        "Q": {"Type": "AWS::SQS::Queue", "DeletionPolicy": "Retain",
              "Properties": {"QueueName": queue}},
        "Bad": {**_FAILING, "DependsOn": "Q"},
    }}
    try:
        cfn.create_stack(StackName=name, TemplateBody=json.dumps(template),
                         DisableRollback=True)
        assert _wait(cfn, name)["StackStatus"] == "CREATE_FAILED"
        cfn.rollback_stack(StackName=name, RetainExceptOnCreate=True)
        assert _wait(cfn, name)["StackStatus"] == "ROLLBACK_COMPLETE"
        assert not _queue_exists(sqs, queue)
    finally:
        _cleanup(cfn, name)


def test_rollback_stack_wrong_state_and_missing_stack(cfn):
    name = _name("rb-wrong")
    try:
        cfn.create_stack(StackName=name, TemplateBody=json.dumps(
            {"Resources": {"Q": {"Type": "AWS::SQS::Queue"}}}))
        assert _wait(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
        with pytest.raises(ClientError) as exc:
            cfn.rollback_stack(StackName=name)
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert "cannot be called from current stack status" in exc.value.response["Error"]["Message"]
        with pytest.raises(ClientError) as exc:
            cfn.rollback_stack(StackName=_name("rb-missing"))
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert "does not exist" in exc.value.response["Error"]["Message"]
    finally:
        _cleanup(cfn, name)


# ---------------------------------------------------------------------------
# Drift detection
# ---------------------------------------------------------------------------

def _drift_template(name):
    return json.dumps({"Resources": {
        "Q": {"Type": "AWS::SQS::Queue", "Properties": {
            "QueueName": f"{name}-q", "VisibilityTimeout": 45,
            "Tags": [{"Key": "team", "Value": "a"}]}},
        "P": {"Type": "AWS::SSM::Parameter", "Properties": {
            "Name": f"/{name}/p", "Type": "String", "Value": "expected"}},
        # No drift reader for queue policies: NOT_CHECKED.
        "Policy": {"Type": "AWS::SQS::QueuePolicy", "Properties": {
            "Queues": [{"Ref": "Q"}],
            "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": "*", "Action": "sqs:SendMessage",
                "Resource": "*"}]}}},
    }})


@pytest.fixture
def drift_stack(cfn):
    name = _name("drift")
    cfn.create_stack(StackName=name, TemplateBody=_drift_template(name),
                     Tags=[{"Key": "env", "Value": "test"}])
    assert _wait(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
    yield name
    _cleanup(cfn, name)


def test_drift_in_sync_after_create(cfn, drift_stack):
    name = drift_stack
    stack = cfn.describe_stacks(StackName=name)["Stacks"][0]
    assert stack["DriftInformation"] == {"StackDriftStatus": "NOT_CHECKED"}
    summary = next(s for s in cfn.list_stacks()["StackSummaries"] if s["StackName"] == name)
    assert summary["DriftInformation"]["StackDriftStatus"] == "NOT_CHECKED"

    status = _detect(cfn, name)
    assert status["DetectionStatus"] == "DETECTION_COMPLETE"
    assert status["StackDriftStatus"] == "IN_SYNC"
    assert status["DriftedStackResourceCount"] == 0
    assert status["StackId"] == stack["StackId"]

    drifts = _drifts(cfn, name)
    # Only the checked resources are listed; the queue policy has no support.
    assert set(drifts) == {"Q", "P"}
    q = drifts["Q"]
    assert q["StackResourceDriftStatus"] == "IN_SYNC"
    assert json.loads(q["ExpectedProperties"]) == json.loads(q["ActualProperties"])
    # The stack-level tag is part of the expected tags; aws: tags are not.
    assert json.loads(q["ExpectedProperties"])["Tags"] == [
        {"Key": "team", "Value": "a"}, {"Key": "env", "Value": "test"}]
    assert "PropertyDifferences" not in q or q["PropertyDifferences"] == []

    after = cfn.describe_stacks(StackName=name)["Stacks"][0]["DriftInformation"]
    assert after["StackDriftStatus"] == "IN_SYNC"
    assert "LastCheckTimestamp" in after
    resources = {r["LogicalResourceId"]: r["DriftInformation"]
                 for r in cfn.describe_stack_resources(StackName=name)["StackResources"]}
    assert resources["Q"]["StackResourceDriftStatus"] == "IN_SYNC"
    assert "LastCheckTimestamp" in resources["Q"]
    assert resources["Policy"] == {"StackResourceDriftStatus": "NOT_CHECKED"}
    summaries = {r["LogicalResourceId"]: r["DriftInformation"]
                 for r in cfn.list_stack_resources(StackName=name)["StackResourceSummaries"]}
    assert summaries["P"]["StackResourceDriftStatus"] == "IN_SYNC"
    detail = cfn.describe_stack_resource(StackName=name, LogicalResourceId="Policy")
    assert detail["StackResourceDetail"]["DriftInformation"]["StackResourceDriftStatus"] == "NOT_CHECKED"


def test_drift_modified_after_out_of_band_changes(cfn, sqs, ssm, drift_stack):
    name = drift_stack
    url = sqs.get_queue_url(QueueName=f"{name}-q")["QueueUrl"]
    sqs.set_queue_attributes(QueueUrl=url, Attributes={"VisibilityTimeout": "99"})
    sqs.tag_queue(QueueUrl=url, Tags={"added": "x"})
    ssm.put_parameter(Name=f"/{name}/p", Value="changed", Type="String", Overwrite=True)

    status = _detect(cfn, name)
    assert status["StackDriftStatus"] == "DRIFTED"
    assert status["DriftedStackResourceCount"] == 2

    drifts = _drifts(cfn, name)
    q = drifts["Q"]
    assert q["StackResourceDriftStatus"] == "MODIFIED"
    diffs = {d["PropertyPath"]: d for d in q["PropertyDifferences"]}
    assert diffs["/VisibilityTimeout"] == {
        "PropertyPath": "/VisibilityTimeout", "ExpectedValue": "45",
        "ActualValue": "99", "DifferenceType": "NOT_EQUAL"}
    added = [d for d in q["PropertyDifferences"] if d["DifferenceType"] == "ADD"]
    assert len(added) == 1 and added[0]["PropertyPath"].startswith("/Tags/")
    assert json.loads(added[0]["ActualValue"]) == {"Key": "added", "Value": "x"}
    assert json.loads(q["ActualProperties"])["VisibilityTimeout"] == 99
    assert json.loads(q["ExpectedProperties"])["VisibilityTimeout"] == 45

    p = drifts["P"]
    assert p["StackResourceDriftStatus"] == "MODIFIED"
    assert p["PropertyDifferences"] == [{
        "PropertyPath": "/Value", "ExpectedValue": "expected",
        "ActualValue": "changed", "DifferenceType": "NOT_EQUAL"}]

    assert cfn.describe_stacks(StackName=name)["Stacks"][0]["DriftInformation"][
        "StackDriftStatus"] == "DRIFTED"
    modified = _drifts(cfn, name, StackResourceDriftStatusFilters=["MODIFIED"])
    assert set(modified) == {"Q", "P"}
    assert _drifts(cfn, name, StackResourceDriftStatusFilters=["IN_SYNC", "DELETED"]) == {}
    assert _drifts(cfn, name, StackResourceDriftStatusFilters=["NOT_CHECKED"]) == {}


def test_drift_removed_tag_is_reported_as_remove(cfn, sqs, drift_stack):
    name = drift_stack
    url = sqs.get_queue_url(QueueName=f"{name}-q")["QueueUrl"]
    sqs.untag_queue(QueueUrl=url, TagKeys=["team"])
    drift = cfn.detect_stack_resource_drift(StackName=name, LogicalResourceId="Q")[
        "StackResourceDrift"]
    assert drift["StackResourceDriftStatus"] == "MODIFIED"
    assert drift["PropertyDifferences"] == [{
        "PropertyPath": "/Tags/0", "ExpectedValue": json.dumps(
            {"Key": "team", "Value": "a"}, sort_keys=True),
        "ActualValue": "null", "DifferenceType": "REMOVE"}]


def test_drift_deleted_after_out_of_band_delete(cfn, sqs, drift_stack):
    name = drift_stack
    url = sqs.get_queue_url(QueueName=f"{name}-q")["QueueUrl"]
    sqs.delete_queue(QueueUrl=url)

    drift = cfn.detect_stack_resource_drift(StackName=name, LogicalResourceId="Q")[
        "StackResourceDrift"]
    assert drift["StackResourceDriftStatus"] == "DELETED"
    assert "ExpectedProperties" not in drift and "ActualProperties" not in drift
    assert drift["PhysicalResourceId"] == url
    # A single-resource check moves the stack's LastCheckTimestamp only.
    info = cfn.describe_stacks(StackName=name)["Stacks"][0]["DriftInformation"]
    assert info["StackDriftStatus"] == "NOT_CHECKED" and "LastCheckTimestamp" in info
    assert set(_drifts(cfn, name, StackResourceDriftStatusFilters=["DELETED"])) == {"Q"}

    status = _detect(cfn, name)
    assert status["StackDriftStatus"] == "DRIFTED"
    assert status["DriftedStackResourceCount"] == 1


def test_drift_logical_resource_ids_filter(cfn, ssm, drift_stack):
    name = drift_stack
    ssm.put_parameter(Name=f"/{name}/p", Value="changed", Type="String", Overwrite=True)
    status = _detect(cfn, name, LogicalResourceIds=["Q"])
    assert status["StackDriftStatus"] == "IN_SYNC"
    assert set(_drifts(cfn, name)) == {"Q"}
    with pytest.raises(ClientError) as exc:
        cfn.detect_stack_drift(StackName=name, LogicalResourceIds=["Nope"])
    assert exc.value.response["Error"]["Code"] == "ValidationError"


def test_drift_unsupported_type_and_missing_resource(cfn, drift_stack):
    name = drift_stack
    with pytest.raises(ClientError) as exc:
        cfn.detect_stack_resource_drift(StackName=name, LogicalResourceId="Policy")
    assert exc.value.response["Error"]["Code"] == "ValidationError"
    assert "AWS::SQS::QueuePolicy" in exc.value.response["Error"]["Message"]
    with pytest.raises(ClientError) as exc:
        cfn.detect_stack_resource_drift(StackName=name, LogicalResourceId="Nope")
    assert exc.value.response["Error"]["Code"] == "ValidationError"
    with pytest.raises(ClientError) as exc:
        cfn.describe_stack_drift_detection_status(StackDriftDetectionId=str(uuid.uuid4()))
    assert exc.value.response["Error"]["Code"] == "ValidationError"


def test_drift_refused_on_unstable_stack(cfn):
    name = _name("drift-failed")
    template = {"Resources": {"Q": {"Type": "AWS::SQS::Queue"}, "Bad": _FAILING}}
    try:
        cfn.create_stack(StackName=name, TemplateBody=json.dumps(template),
                         DisableRollback=True)
        assert _wait(cfn, name)["StackStatus"] == "CREATE_FAILED"
        with pytest.raises(ClientError) as exc:
            cfn.detect_stack_drift(StackName=name)
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert "CREATE_FAILED" in exc.value.response["Error"]["Message"]
    finally:
        _cleanup(cfn, name)


def test_drift_property_readers_for_more_types(cfn, iam, lam, ddb, sns, s3):
    name = _name("drift-types")
    template = {"Resources": {
        "Role": {"Type": "AWS::IAM::Role", "Properties": {
            "RoleName": f"{name}-role", "MaxSessionDuration": 3600,
            "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"}]}}},
        "Fn": {"Type": "AWS::Lambda::Function", "Properties": {
            "FunctionName": f"{name}-fn", "Runtime": "python3.12", "Handler": "index.handler",
            "Role": {"Fn::GetAtt": ["Role", "Arn"]}, "Timeout": 10,
            "Code": {"ZipFile": "def handler(e, c):\n    return 1\n"},
            "Environment": {"Variables": {"A": "1"}}}},
        "Table": {"Type": "AWS::DynamoDB::Table", "Properties": {
            "TableName": f"{name}-t", "BillingMode": "PROVISIONED",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}}},
        "Topic": {"Type": "AWS::SNS::Topic", "Properties": {
            "TopicName": f"{name}-topic", "DisplayName": "before"}},
        "Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": f"{name}-b"}},
    }}
    try:
        cfn.create_stack(StackName=name, TemplateBody=json.dumps(template),
                         Capabilities=["CAPABILITY_NAMED_IAM"])
        assert _wait(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
        assert _detect(cfn, name)["StackDriftStatus"] == "IN_SYNC"
        assert {d["StackResourceDriftStatus"] for d in _drifts(cfn, name).values()} == {"IN_SYNC"}

        iam.update_role(RoleName=f"{name}-role", MaxSessionDuration=7200)
        lam.update_function_configuration(FunctionName=f"{name}-fn", Timeout=20,
                                          Environment={"Variables": {"A": "2"}})
        ddb.update_table(TableName=f"{name}-t", ProvisionedThroughput={
            "ReadCapacityUnits": 7, "WriteCapacityUnits": 5})
        topic_arn = cfn.describe_stack_resource(StackName=name, LogicalResourceId="Topic")[
            "StackResourceDetail"]["PhysicalResourceId"]
        sns.set_topic_attributes(TopicArn=topic_arn, AttributeName="DisplayName",
                                 AttributeValue="after")
        s3.delete_bucket(Bucket=f"{name}-b")

        status = _detect(cfn, name)
        assert status["DriftedStackResourceCount"] == 5
        drifts = _drifts(cfn, name)
        paths = {lid: {(d["PropertyPath"], d["ExpectedValue"], d["ActualValue"])
                       for d in d_.get("PropertyDifferences", [])}
                 for lid, d_ in drifts.items()}
        assert paths["Role"] == {("/MaxSessionDuration", "3600", "7200")}
        assert paths["Fn"] == {("/Timeout", "10", "20"), ("/Environment/Variables/A", "1", "2")}
        assert paths["Table"] == {("/ProvisionedThroughput/ReadCapacityUnits", "5", "7")}
        assert paths["Topic"] == {("/DisplayName", "before", "after")}
        assert drifts["Bucket"]["StackResourceDriftStatus"] == "DELETED"
    finally:
        try:
            s3.create_bucket(Bucket=f"{name}-b")
        except ClientError:
            pass
        _cleanup(cfn, name)


def test_describe_stack_resource_drifts_pagination(cfn):
    name = _name("drift-page")
    resources = {
        f"P{i}": {"Type": "AWS::SSM::Parameter", "Properties": {
            "Name": f"/{name}/p{i}", "Type": "String", "Value": str(i)}}
        for i in range(3)
    }
    try:
        cfn.create_stack(StackName=name, TemplateBody=json.dumps({"Resources": resources}))
        assert _wait(cfn, name)["StackStatus"] == "CREATE_COMPLETE"
        _detect(cfn, name)
        first = cfn.describe_stack_resource_drifts(StackName=name, MaxResults=2)
        assert len(first["StackResourceDrifts"]) == 2
        assert first.get("NextToken")
        second = cfn.describe_stack_resource_drifts(StackName=name, MaxResults=2,
                                                    NextToken=first["NextToken"])
        assert len(second["StackResourceDrifts"]) == 1
        assert "NextToken" not in second
        ids = {d["LogicalResourceId"] for d in
               first["StackResourceDrifts"] + second["StackResourceDrifts"]}
        assert ids == {"P0", "P1", "P2"}
        with pytest.raises(ClientError) as exc:
            cfn.describe_stack_resource_drifts(StackName=name, NextToken="ListStacks:1")
        assert exc.value.response["Error"]["Code"] == "ValidationError"
    finally:
        _cleanup(cfn, name)
