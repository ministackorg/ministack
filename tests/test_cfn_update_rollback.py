"""A failed CloudFormation update rolls back its in-place changes and its parameters."""

import json
import time
import uuid as _uuid_mod

from botocore.exceptions import ClientError
from test_cfn import (
    _CR_LAMBDA_ROLE,
    _FAILING_RESOURCE,
    _cfn_appsync_members_template,
    _cr_make_zip,
    _delete_cfn_test_stack,
    _wait_stack,
)


def test_cfn_update_rollback_records_the_cleanup_phase(cfn):
    """The events of an update rollback as AWS records them (measured
    2026-09-21): the stack's UPDATE_ROLLBACK_IN_PROGRESS names the resource
    that failed, "failed to update" for one that existed before the update and
    "failed to create" for one the update added; what the update created is
    deleted in UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS, each resource with
    DELETE_IN_PROGRESS then DELETE_COMPLETE; the final UPDATE_ROLLBACK_COMPLETE
    carries no reason."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rb-events-{uid}"
    table_name = f"cfn-rb-events-{uid}"

    def template(key_type, extra=None):
        resources = {
            "Table": {"Type": "AWS::DynamoDB::Table", "Properties": {
                "TableName": table_name,
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": key_type}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
            }},
        }
        if extra == "wait":
            resources["Handle"] = {"Type": "AWS::CloudFormation::WaitConditionHandle"}
            resources["Wait"] = {"Type": "AWS::CloudFormation::WaitCondition",
                                 "Properties": {"Handle": {"Ref": "Handle"}, "Timeout": "1"}}
        elif extra == "added":
            resources["Added"] = {"Type": "AWS::SQS::Queue", "Properties": {}}
            resources["Table"]["DependsOn"] = "Added"
        return json.dumps({"Resources": resources})

    def rollback_events():
        events = list(reversed(cfn.describe_stack_events(StackName=stack_name)["StackEvents"]))
        start = max(i for i, e in enumerate(events)
                    if e["LogicalResourceId"] == stack_name
                    and e["ResourceStatus"] == "UPDATE_IN_PROGRESS")
        return events[start:]

    cfn.create_stack(StackName=stack_name, TemplateBody=template("S"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        # An existing resource fails: the table's key type change is refused,
        # after the update created a queue.
        cfn.update_stack(StackName=stack_name, TemplateBody=template("N", "added"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
        events = rollback_events()
        stack_events = [(e["ResourceStatus"], e.get("ResourceStatusReason", ""))
                        for e in events if e["LogicalResourceId"] == stack_name]
        assert stack_events[1:] == [
            ("UPDATE_ROLLBACK_IN_PROGRESS", "The following resource(s) failed to update: [Table]. "),
            ("UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS", ""),
            ("UPDATE_ROLLBACK_COMPLETE", ""),
        ]
        cleanup = next(i for i, e in enumerate(events)
                       if e["ResourceStatus"] == "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS")
        assert [(e["LogicalResourceId"], e["ResourceStatus"]) for e in events[cleanup + 1:]] == [
            ("Added", "DELETE_IN_PROGRESS"), ("Added", "DELETE_COMPLETE"),
            (stack_name, "UPDATE_ROLLBACK_COMPLETE")]

        # A resource the update adds fails: the wait condition times out.
        cfn.update_stack(StackName=stack_name, TemplateBody=template("S", "wait"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
        reasons = [e.get("ResourceStatusReason", "") for e in rollback_events()
                   if e["LogicalResourceId"] == stack_name
                   and e["ResourceStatus"] == "UPDATE_ROLLBACK_IN_PROGRESS"]
        assert reasons == ["The following resource(s) failed to create: [Wait]. "]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_change_set_rollback_restores_the_parameters(cfn, sqs):
    """An executed change set that rolls back leaves the stack reporting the
    parameters it had before, as UpdateStack's rollback does (measured on AWS
    2026-09-21: DescribeStacks answers P=30 after a change set set it to 45
    and failed). The execute path kept no copy of them, so the stack reported
    the values of the update it had just undone."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-cs-rb-params-{uid}"
    queue_name = f"cfn-cs-rb-params-{uid}"
    template = json.dumps({
        "Parameters": {"P": {"Type": "String", "Default": "30"},
                       "K": {"Type": "String", "Default": "S"}},
        "Resources": {
            "Queue": {"Type": "AWS::SQS::Queue", "Properties": {
                "QueueName": queue_name, "VisibilityTimeout": {"Ref": "P"}}},
            "Table": {"Type": "AWS::DynamoDB::Table", "DependsOn": "Queue", "Properties": {
                "TableName": f"cfn-cs-rb-params-{uid}",
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": {"Ref": "K"}}],
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST",
            }},
        },
    })
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        cfn.create_change_set(
            StackName=stack_name, ChangeSetName="raise", UsePreviousTemplate=True,
            Parameters=[{"ParameterKey": "P", "ParameterValue": "45"},
                        {"ParameterKey": "K", "ParameterValue": "N"}])
        for _ in range(60):
            change_set = cfn.describe_change_set(StackName=stack_name, ChangeSetName="raise")
            if not change_set["Status"].endswith(("_PENDING", "_IN_PROGRESS")):
                break
            time.sleep(0.5)
        assert change_set["Status"] == "CREATE_COMPLETE", change_set.get("StatusReason")
        cfn.execute_change_set(StackName=stack_name, ChangeSetName="raise")
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
        assert sorted((p["ParameterKey"], p["ParameterValue"])
                      for p in stack["Parameters"]) == [("K", "S"), ("P", "30")]
        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        assert sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["VisibilityTimeout"]
        )["Attributes"]["VisibilityTimeout"] == "30"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _cr_mode_handler(fail_mode):
    """A custom-resource handler that refuses an Update whose target Mode is
    ``fail_mode`` and accepts every other request, keeping its physical id."""
    return f"""\
import json, urllib.request

FAIL_MODE = {fail_mode!r}

def handler(event, context):
    mode = event.get("ResourceProperties", {{}}).get("Mode")
    failed = event["RequestType"] == "Update" and mode == FAIL_MODE
    payload = json.dumps({{
        "Status": "FAILED" if failed else "SUCCESS",
        "Reason": "update to %s refused for testing" % mode,
        "RequestId": event["RequestId"],
        "StackId": event["StackId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": event.get("PhysicalResourceId") or "mode-cr",
        "Data": {{"Mode": str(mode)}},
    }}).encode()
    req = urllib.request.Request(
        event["ResponseURL"], data=payload, method="PUT",
        headers={{"content-type": "", "content-length": str(len(payload))}},
    )
    urllib.request.urlopen(req, timeout=10)
"""


def _cfn_mode_cr_function(lam, fn, fail_mode):
    lam.create_function(
        FunctionName=fn, Runtime="python3.12", Role=_CR_LAMBDA_ROLE, Handler="index.handler",
        Code={"ZipFile": _cr_make_zip(_cr_mode_handler(fail_mode))},
    )


def _cfn_mode_cr_accept_all(lam, fn):
    lam.update_function_code(FunctionName=fn, ZipFile=_cr_make_zip(_cr_mode_handler(None)))


def _cfn_events_since_rollback(cfn, stack_name, logical_id):
    """The statuses one resource recorded after the stack's latest
    UPDATE_ROLLBACK_IN_PROGRESS, oldest first."""
    events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
    rollback = next(i for i, e in enumerate(events)
                    if e["LogicalResourceId"] == stack_name
                    and e["ResourceStatus"] == "UPDATE_ROLLBACK_IN_PROGRESS")
    return [e["ResourceStatus"] for e in reversed(events[:rollback])
            if e["LogicalResourceId"] == logical_id]


def test_cfn_update_rollback_failed_revert_is_retried_by_continue_update_rollback(cfn, lam):
    """A revert the rollback cannot apply lands the stack in
    UPDATE_ROLLBACK_FAILED with the resource named as failed to update, and
    ContinueUpdateRollback retries that update (not a delete) until it
    succeeds. The custom resource takes the forward update to "new" and
    refuses the update back to "old"."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-revert-fails-{suffix}"
    stack_name = f"cfn-revert-fails-{suffix}"
    _cfn_mode_cr_function(lam, fn, fail_mode="old")
    token = f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"

    def template(mode, with_bad):
        resources = {"CR": {"Type": "Custom::Tester",
                            "Properties": {"ServiceToken": token, "Mode": mode}}}
        if with_bad:
            resources["Bad"] = {**_FAILING_RESOURCE, "DependsOn": "CR"}
        return json.dumps({"Resources": resources})

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("old", False))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")

        cfn.update_stack(StackName=stack_name, TemplateBody=template("new", True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_FAILED", stack.get("StackStatusReason")
        assert stack["StackStatusReason"] == (
            "The following resource(s) failed to update: [CR].")
        statuses = _cfn_events_since_rollback(cfn, stack_name, "CR")
        assert statuses[:2] == ["UPDATE_IN_PROGRESS", "UPDATE_FAILED"], statuses

        # Still refused: the retry fails the same way.
        cfn.continue_update_rollback(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_FAILED", stack.get("StackStatusReason")
        assert "failed to update: [CR]" in stack["StackStatusReason"]

        # Accepted now: the retry is an update, so the resource is not deleted.
        _cfn_mode_cr_accept_all(lam, fn)
        cfn.continue_update_rollback(StackName=stack_name)
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        statuses = _cfn_events_since_rollback(cfn, stack_name, "CR")
        assert statuses == ["UPDATE_IN_PROGRESS", "UPDATE_COMPLETE"], statuses
        resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
        assert [r["LogicalResourceId"] for r in resources] == ["CR"]
    finally:
        try:
            _cfn_mode_cr_accept_all(lam, fn)
        except ClientError:
            pass
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_update_rollback_failed_revert_can_be_skipped(cfn, lam):
    """ResourcesToSkip accepts a resource whose revert failed: it keeps the
    state the update left (an UPDATE_COMPLETE event, no delete) and the stack
    reaches UPDATE_ROLLBACK_COMPLETE."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-revert-skip-{suffix}"
    stack_name = f"cfn-revert-skip-{suffix}"
    _cfn_mode_cr_function(lam, fn, fail_mode="old")
    token = f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"
    base = {"Resources": {"CR": {"Type": "Custom::Tester",
                                 "Properties": {"ServiceToken": token, "Mode": "old"}}}}
    updated = json.loads(json.dumps(base))
    updated["Resources"]["CR"]["Properties"]["Mode"] = "new"
    updated["Resources"]["Bad"] = {**_FAILING_RESOURCE, "DependsOn": "CR"}
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(base))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(updated))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_ROLLBACK_FAILED"

        cfn.continue_update_rollback(StackName=stack_name, ResourcesToSkip=["CR"])
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_events_since_rollback(cfn, stack_name, "CR") == ["UPDATE_COMPLETE"]
    finally:
        try:
            _cfn_mode_cr_accept_all(lam, fn)
        except ClientError:
            pass
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_update_rollback_reverts_the_resource_whose_update_failed(cfn, lam):
    """The resource whose update fails the stack may have changed state before
    it failed, so the rollback sends it the update back to its previous
    properties too. The custom resource refuses the update to "new"; the
    rollback's update back to "old" is accepted."""
    suffix = _uuid_mod.uuid4().hex[:8]
    fn = f"cr-fwd-fails-{suffix}"
    stack_name = f"cfn-fwd-fails-{suffix}"
    _cfn_mode_cr_function(lam, fn, fail_mode="new")
    token = f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"

    def template(mode):
        return json.dumps({"Resources": {"CR": {
            "Type": "Custom::Tester", "Properties": {"ServiceToken": token, "Mode": mode}}}})

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("old"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"

        cfn.update_stack(StackName=stack_name, TemplateBody=template("new"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_events_since_rollback(cfn, stack_name, "CR") == [
            "UPDATE_IN_PROGRESS", "UPDATE_COMPLETE"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        try:
            lam.delete_function(FunctionName=fn)
        except ClientError:
            pass


def test_cfn_update_rollback_does_not_resend_a_refused_resource_update(cfn):
    """A resource type's own handler that refused the update changed nothing,
    so the rollback records it complete without calling it again. Measured on
    AWS 2026-09-21: a GraphQL API whose Visibility change the handler refuses
    ("can only be set when creating a GraphQL API") gets a lone
    UPDATE_COMPLETE in the rollback, and so did a scaling policy refused with
    AlreadyExists. Calling the handler again would only be refused the other
    way round and fail the rollback."""
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rb-refused-{uid}"
    oidc = {"Issuer": "https://issuer.example.com", "ClientId": "c1"}
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_appsync_members_template(
            f"refused_{uid}", {}, oidc))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_appsync_members_template(
            f"refused_{uid}", {"Visibility": "PRIVATE"}, oidc))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_events_since_rollback(cfn, stack_name, "Api") == ["UPDATE_COMPLETE"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
