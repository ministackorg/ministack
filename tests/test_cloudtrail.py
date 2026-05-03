"""
CloudTrail integration tests.

Recording must be enabled for event-recording tests. The module-scoped
`enable_recording` fixture toggles it on via /_ministack/config before any
test runs and resets state after the module completes, so these tests are
safe to run alongside the rest of the suite.

Tests are split into two sections:
  - Control plane: trail CRUD, stubs (always available)
  - Event recording: LookupEvents and filter variants (require recording enabled)
"""

import time

import boto3
import pytest
import requests
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


def _client(service):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
    )


def _uid():
    import uuid

    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def ct():
    return _client("cloudtrail")


@pytest.fixture(scope="module")
def s3():
    return _client("s3")


@pytest.fixture(scope="module")
def ddb():
    return _client("dynamodb")


@pytest.fixture(scope="module", autouse=True)
def enable_recording():
    """Enable CloudTrail recording for this test module via the runtime config endpoint."""
    resp = requests.post(
        f"{ENDPOINT}/_ministack/config",
        json={"cloudtrail._recording_enabled": "true"},
    )
    assert resp.status_code == 200, f"Failed to enable recording: {resp.text}"
    yield
    # Disable recording after module; reset clears events
    requests.post(
        f"{ENDPOINT}/_ministack/config",
        json={"cloudtrail._recording_enabled": "false"},
    )


# ---------------------------------------------------------------------------
# Control plane — trail CRUD
# ---------------------------------------------------------------------------


def test_create_trail(ct):
    name = f"trail-{_uid()}"
    resp = ct.create_trail(Name=name, S3BucketName="my-logs")
    assert resp["Name"] == name
    assert "TrailARN" in resp
    assert "cloudtrail" in resp["TrailARN"]
    assert f"/{name}" in resp["TrailARN"]


def test_create_trail_duplicate(ct):
    name = f"trail-dup-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    with pytest.raises(ClientError) as exc:
        ct.create_trail(Name=name, S3BucketName="bucket")
    assert "TrailAlreadyExistsException" in str(exc.value)


def test_get_trail(ct):
    name = f"trail-get-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    resp = ct.get_trail(Name=name)
    assert resp["Trail"]["Name"] == name
    assert "TrailARN" in resp["Trail"]


def test_get_trail_not_found(ct):
    with pytest.raises(ClientError) as exc:
        ct.get_trail(Name=f"nonexistent-{_uid()}")
    assert "TrailNotFoundException" in str(exc.value)


def test_delete_trail(ct):
    name = f"trail-del-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    ct.delete_trail(Name=name)
    resp = ct.describe_trails(trailNameList=[name])
    assert not any(t["Name"] == name for t in resp["trailList"])


def test_delete_trail_not_found(ct):
    with pytest.raises(ClientError) as exc:
        ct.delete_trail(Name=f"nonexistent-{_uid()}")
    assert "TrailNotFoundException" in str(exc.value)


def test_describe_trails_all(ct):
    name = f"trail-desc-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    resp = ct.describe_trails()
    assert any(t["Name"] == name for t in resp["trailList"])


def test_describe_trails_by_name(ct):
    name = f"trail-byname-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    resp = ct.describe_trails(trailNameList=[name])
    assert len(resp["trailList"]) == 1
    assert resp["trailList"][0]["Name"] == name


def test_describe_trails_name_not_found(ct):
    resp = ct.describe_trails(trailNameList=[f"nonexistent-{_uid()}"])
    assert resp["trailList"] == []


def test_get_trail_status(ct):
    name = f"trail-status-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    resp = ct.get_trail_status(Name=name)
    assert resp["IsLogging"] is True


def test_get_trail_status_not_found(ct):
    with pytest.raises(ClientError):
        ct.get_trail_status(Name=f"nonexistent-{_uid()}")


def test_start_stop_logging(ct):
    name = f"trail-log-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    ct.start_logging(Name=name)
    ct.stop_logging(Name=name)


def test_start_logging_not_found(ct):
    with pytest.raises(ClientError) as exc:
        ct.start_logging(Name=f"nonexistent-{_uid()}")
    assert "TrailNotFoundException" in str(exc.value)


def test_stop_logging_not_found(ct):
    with pytest.raises(ClientError) as exc:
        ct.stop_logging(Name=f"nonexistent-{_uid()}")
    assert "TrailNotFoundException" in str(exc.value)


def test_put_get_event_selectors(ct):
    name = f"trail-sel-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    selectors = [{"ReadWriteType": "All", "IncludeManagementEvents": True, "DataResources": []}]
    put_resp = ct.put_event_selectors(TrailName=name, EventSelectors=selectors)
    assert "TrailARN" in put_resp
    assert put_resp["EventSelectors"] == selectors

    get_resp = ct.get_event_selectors(TrailName=name)
    assert get_resp["EventSelectors"] == selectors
    assert get_resp["AdvancedEventSelectors"] == []


def test_get_event_selectors_empty(ct):
    name = f"trail-nosel-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    resp = ct.get_event_selectors(TrailName=name)
    assert resp["EventSelectors"] == []


def test_add_list_remove_tags(ct):
    name = f"trail-tags-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    arn = ct.get_trail(Name=name)["Trail"]["TrailARN"]

    ct.add_tags(ResourceId=arn, TagsList=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "ops"}])
    list_resp = ct.list_tags(ResourceIdList=[arn])
    tags = {t["Key"]: t["Value"] for item in list_resp["ResourceTagList"] for t in item["TagsList"]}
    assert tags["env"] == "test"
    assert tags["team"] == "ops"

    ct.remove_tags(ResourceId=arn, TagsList=[{"Key": "env", "Value": "test"}])
    list_resp2 = ct.list_tags(ResourceIdList=[arn])
    tags2 = {t["Key"]: t["Value"] for item in list_resp2["ResourceTagList"] for t in item["TagsList"]}
    assert "env" not in tags2
    assert tags2["team"] == "ops"


# ---------------------------------------------------------------------------
# Event recording — LookupEvents and filters
# ---------------------------------------------------------------------------


def test_lookup_all_events_s3(ct, s3):
    bucket = f"ct-all-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    resp = ct.lookup_events()
    assert any(e["EventName"] == "CreateBucket" for e in resp["Events"])


def test_lookup_filter_event_name(ct, s3):
    bucket = f"ct-ename-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "EventName", "AttributeValue": "CreateBucket"}]
    )
    assert len(resp["Events"]) > 0
    assert all(e["EventName"] == "CreateBucket" for e in resp["Events"])


def test_lookup_filter_resource_name(ct, s3):
    bucket = f"ct-rname-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": bucket}]
    )
    assert len(resp["Events"]) > 0
    for ev in resp["Events"]:
        assert any(r.get("ResourceName") == bucket for r in ev.get("Resources", []))


def test_lookup_filter_resource_type(ct, s3):
    bucket = f"ct-rtype-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "ResourceType", "AttributeValue": "AWS::S3::Bucket"}]
    )
    assert len(resp["Events"]) > 0
    for ev in resp["Events"]:
        assert any(r.get("ResourceType") == "AWS::S3::Bucket" for r in ev.get("Resources", []))


def test_lookup_filter_username(ct, s3):
    bucket = f"ct-user-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "Username", "AttributeValue": "test"}]
    )
    assert len(resp["Events"]) > 0
    assert all(e["Username"] == "test" for e in resp["Events"])


def test_lookup_filter_access_key_id(ct, s3):
    bucket = f"ct-akid-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "AccessKeyId", "AttributeValue": "test"}]
    )
    assert len(resp["Events"]) > 0
    assert all(e.get("AccessKeyId") == "test" for e in resp["Events"])


def test_lookup_filter_readonly_false(ct, s3):
    bucket = f"ct-rw-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "ReadOnly", "AttributeValue": "false"}]
    )
    assert len(resp["Events"]) > 0
    assert all(e.get("ReadOnly") == "false" for e in resp["Events"])


def test_lookup_filter_event_source(ct, s3):
    bucket = f"ct-src-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "EventSource", "AttributeValue": "s3.amazonaws.com"}]
    )
    assert len(resp["Events"]) > 0
    for ev in resp["Events"]:
        assert ev.get("EventSource") == "s3.amazonaws.com"


def test_lookup_filter_event_id(ct, s3):
    bucket = f"ct-eid-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    all_resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": bucket}]
    )
    assert len(all_resp["Events"]) >= 1
    target_id = all_resp["Events"][0]["EventId"]

    id_resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "EventId", "AttributeValue": target_id}]
    )
    assert len(id_resp["Events"]) == 1
    assert id_resp["Events"][0]["EventId"] == target_id


def test_lookup_no_match(ct):
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "EventName", "AttributeValue": f"NoSuchAction{_uid()}"}]
    )
    assert resp["Events"] == []


def test_lookup_time_range_match(ct, s3):
    bucket = f"ct-time-{_uid()}"
    from datetime import datetime, timezone, timedelta

    before = datetime.now(timezone.utc) - timedelta(seconds=2)
    s3.create_bucket(Bucket=bucket)
    after = datetime.now(timezone.utc) + timedelta(seconds=2)

    resp = ct.lookup_events(
        StartTime=before,
        EndTime=after,
        LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": bucket}],
    )
    assert len(resp["Events"]) >= 1


def test_lookup_time_range_future_empty(ct):
    from datetime import datetime, timezone, timedelta

    future_start = datetime.now(timezone.utc) + timedelta(hours=1)
    future_end = datetime.now(timezone.utc) + timedelta(hours=2)
    resp = ct.lookup_events(StartTime=future_start, EndTime=future_end)
    assert resp["Events"] == []


def test_lookup_max_results(ct, s3):
    for _ in range(6):
        s3.create_bucket(Bucket=f"ct-maxr-{_uid()}")
    resp = ct.lookup_events(MaxResults=3)
    assert len(resp["Events"]) <= 3


def test_lookup_newest_first(ct, s3):
    b1 = f"ct-ord1-{_uid()}"
    b2 = f"ct-ord2-{_uid()}"
    s3.create_bucket(Bucket=b1)
    s3.create_bucket(Bucket=b2)
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "EventName", "AttributeValue": "CreateBucket"}],
        MaxResults=10,
    )
    events = resp["Events"]
    assert len(events) >= 2
    # EventTime should be non-increasing (newest first)
    ts = [e["EventTime"].timestamp() if hasattr(e["EventTime"], "timestamp") else float(e["EventTime"]) for e in events]
    assert ts == sorted(ts, reverse=True)


def test_dynamodb_create_table_recorded(ct, ddb):
    table = f"ct-tbl-{_uid()}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "EventName", "AttributeValue": "CreateTable"}]
    )
    assert any(e["EventName"] == "CreateTable" for e in resp["Events"])


def test_dynamodb_resource_captured(ct, ddb):
    table = f"ct-res-{_uid()}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": table}]
    )
    assert len(resp["Events"]) >= 1
    ev = resp["Events"][0]
    assert any(r["ResourceName"] == table for r in ev["Resources"])
    assert any(r["ResourceType"] == "AWS::DynamoDB::Table" for r in ev["Resources"])


def test_event_record_full_structure(ct, s3):
    import json as _json

    bucket = f"ct-struct-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    resp = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": bucket}]
    )
    assert len(resp["Events"]) >= 1
    ev = resp["Events"][0]

    # Top-level required fields
    for field in ("EventId", "EventName", "EventSource", "EventTime", "Username", "CloudTrailEvent"):
        assert field in ev, f"Missing field: {field}"

    # Full CloudTrailEvent JSON shape
    ct_ev = _json.loads(ev["CloudTrailEvent"])
    for field in (
        "eventVersion",
        "userIdentity",
        "eventTime",
        "eventSource",
        "eventName",
        "awsRegion",
        "sourceIPAddress",
        "userAgent",
        "requestParameters",
        "responseElements",
        "requestID",
        "eventID",
        "eventType",
        "recipientAccountId",
    ):
        assert field in ct_ev, f"Missing CloudTrailEvent field: {field}"

    assert ct_ev["eventType"] == "AwsApiCall"
    assert ct_ev["eventSource"].endswith(".amazonaws.com")
    assert ct_ev["userIdentity"]["type"] == "IAMUser"
    # readOnly is stored as a string ("true"/"false") in both the event record and CloudTrailEvent
    assert ev.get("ReadOnly") in ("true", "false")
    assert ct_ev.get("readOnly") in ("true", "false")


def test_cloudtrail_calls_not_self_recorded(ct):
    """CloudTrail management calls (DescribeTrails) must not appear in LookupEvents."""
    before = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "EventName", "AttributeValue": "DescribeTrails"}]
    )["Events"]
    ct.describe_trails()
    after = ct.lookup_events(
        LookupAttributes=[{"AttributeKey": "EventName", "AttributeValue": "DescribeTrails"}]
    )["Events"]
    assert len(after) == len(before)


def test_recording_disabled_no_new_events(ct, s3):
    """Disabling recording stops new events from being appended."""
    requests.post(
        f"{ENDPOINT}/_ministack/config",
        json={"cloudtrail._recording_enabled": "false"},
    )
    try:
        bucket = f"ct-off-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        resp = ct.lookup_events(
            LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": bucket}]
        )
        assert resp["Events"] == []
    finally:
        # Re-enable so subsequent tests in this module still work
        requests.post(
            f"{ENDPOINT}/_ministack/config",
            json={"cloudtrail._recording_enabled": "true"},
        )
