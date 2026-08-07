"""Direct-handler unit tests for the Cloud Control API emulator.

These call ``ministack.services.cloudcontrol.handle_request`` directly (no
server), asserting botocore-correct shapes: Properties as a JSON string and
ProgressEvent OperationStatus=SUCCESS.
"""

import asyncio
import json

import pytest

from ministack.services import cloudcontrol

TARGET_PREFIX = "CloudApiService"
TYPE_NAME = "AWS::S3::Bucket"


def _call(action, payload):
    """Invoke the handler for one op and return (status, headers, parsed_body)."""
    headers = {"x-amz-target": f"{TARGET_PREFIX}.{action}"}
    body = json.dumps(payload).encode("utf-8")
    status, resp_headers, resp_body = asyncio.run(
        cloudcontrol.handle_request("POST", "/", headers, body, {})
    )
    return status, resp_headers, json.loads(resp_body)


@pytest.fixture(autouse=True)
def _reset():
    cloudcontrol.reset()
    yield
    cloudcontrol.reset()


def test_create_get_list_update_delete_round_trip():
    # Create
    desired = {"BucketName": "my-bucket", "VersioningConfiguration": {"Status": "Suspended"}}
    status, _, body = _call("CreateResource", {
        "TypeName": TYPE_NAME,
        "DesiredState": json.dumps(desired),
    })
    assert status == 200
    event = body["ProgressEvent"]
    assert event["TypeName"] == TYPE_NAME
    assert event["Operation"] == "CREATE"
    assert event["OperationStatus"] == "SUCCESS"
    assert event["Identifier"] == "my-bucket"
    assert event["RequestToken"]
    assert isinstance(event["EventTime"], int)
    # ResourceModel is a JSON string
    assert isinstance(event["ResourceModel"], str)
    json.loads(event["ResourceModel"])

    # Get
    status, _, body = _call("GetResource", {
        "TypeName": TYPE_NAME,
        "Identifier": "my-bucket",
    })
    assert status == 200
    assert body["TypeName"] == TYPE_NAME
    desc = body["ResourceDescription"]
    assert desc["Identifier"] == "my-bucket"
    # Properties is a JSON string, per botocore
    assert isinstance(desc["Properties"], str)
    assert json.loads(desc["Properties"])["BucketName"] == "my-bucket"

    # List
    status, _, body = _call("ListResources", {"TypeName": TYPE_NAME})
    assert status == 200
    assert body["TypeName"] == TYPE_NAME
    descs = body["ResourceDescriptions"]
    assert len(descs) == 1
    assert descs[0]["Identifier"] == "my-bucket"
    assert isinstance(descs[0]["Properties"], str)

    # Update via RFC 6902 patch
    patch = [{"op": "replace", "path": "/VersioningConfiguration/Status", "value": "Enabled"}]
    status, _, body = _call("UpdateResource", {
        "TypeName": TYPE_NAME,
        "Identifier": "my-bucket",
        "PatchDocument": json.dumps(patch),
    })
    assert status == 200
    uevent = body["ProgressEvent"]
    assert uevent["Operation"] == "UPDATE"
    assert uevent["OperationStatus"] == "SUCCESS"
    props = json.loads(uevent["ResourceModel"])
    assert props["VersioningConfiguration"]["Status"] == "Enabled"

    # Confirm persisted mutation
    _, _, body = _call("GetResource", {"TypeName": TYPE_NAME, "Identifier": "my-bucket"})
    assert json.loads(body["ResourceDescription"]["Properties"])["VersioningConfiguration"]["Status"] == "Enabled"

    # Delete
    status, _, body = _call("DeleteResource", {
        "TypeName": TYPE_NAME,
        "Identifier": "my-bucket",
    })
    assert status == 200
    devent = body["ProgressEvent"]
    assert devent["Operation"] == "DELETE"
    assert devent["OperationStatus"] == "SUCCESS"

    # Gone
    status, _, body = _call("GetResource", {"TypeName": TYPE_NAME, "Identifier": "my-bucket"})
    assert status == 400
    assert body["__type"] == "ResourceNotFoundException"

    # And list is empty
    _, _, body = _call("ListResources", {"TypeName": TYPE_NAME})
    assert body["ResourceDescriptions"] == []


def test_get_unknown_resource_returns_not_found():
    status, headers, body = _call("GetResource", {
        "TypeName": TYPE_NAME,
        "Identifier": "does-not-exist",
    })
    assert status == 400
    assert body["__type"] == "ResourceNotFoundException"
    assert headers.get("x-amzn-errortype") == "ResourceNotFoundException"


def test_delete_unknown_resource_returns_not_found():
    status, _, body = _call("DeleteResource", {
        "TypeName": TYPE_NAME,
        "Identifier": "nope",
    })
    assert status == 400
    assert body["__type"] == "ResourceNotFoundException"


def test_request_tracking_and_status():
    _, _, body = _call("CreateResource", {
        "TypeName": TYPE_NAME,
        "DesiredState": json.dumps({"BucketName": "tracked-bucket"}),
    })
    token = body["ProgressEvent"]["RequestToken"]

    # GetResourceRequestStatus replays the ProgressEvent
    status, _, body = _call("GetResourceRequestStatus", {"RequestToken": token})
    assert status == 200
    assert body["ProgressEvent"]["RequestToken"] == token
    assert body["ProgressEvent"]["OperationStatus"] == "SUCCESS"

    # ListResourceRequests includes it, and filters work
    _, _, body = _call("ListResourceRequests", {})
    tokens = [e["RequestToken"] for e in body["ResourceRequestStatusSummaries"]]
    assert token in tokens

    _, _, body = _call("ListResourceRequests", {
        "ResourceRequestStatusFilter": {"Operations": ["CREATE"]},
    })
    assert all(e["Operation"] == "CREATE" for e in body["ResourceRequestStatusSummaries"])

    _, _, body = _call("ListResourceRequests", {
        "ResourceRequestStatusFilter": {"Operations": ["DELETE"]},
    })
    assert body["ResourceRequestStatusSummaries"] == []


def test_get_request_status_unknown_token():
    status, _, body = _call("GetResourceRequestStatus", {"RequestToken": "missing"})
    assert status == 400
    assert body["__type"] == "RequestTokenNotFoundException"


def test_cancel_terminal_request_conflicts():
    _, _, body = _call("CreateResource", {
        "TypeName": TYPE_NAME,
        "DesiredState": json.dumps({"BucketName": "cancel-bucket"}),
    })
    token = body["ProgressEvent"]["RequestToken"]
    # Synchronous requests are already SUCCESS, so cancel must conflict.
    status, _, body = _call("CancelResourceRequest", {"RequestToken": token})
    assert status == 400
    assert body["__type"] == "ConflictException"


def test_create_duplicate_conflicts():
    payload = {"TypeName": TYPE_NAME, "DesiredState": json.dumps({"BucketName": "dup"})}
    status, _, _ = _call("CreateResource", payload)
    assert status == 200
    status, _, body = _call("CreateResource", payload)
    assert status == 400
    assert body["__type"] == "AlreadyExistsException"
