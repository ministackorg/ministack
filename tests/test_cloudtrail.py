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
from datetime import datetime, timedelta, timezone

import boto3
import pytest
import requests
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


def _client(service, region=REGION):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
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


def test_get_trail_by_arn(ct):
    name = f"trail-get-arn-{_uid()}"
    created = ct.create_trail(Name=name, S3BucketName="bucket")
    resp = ct.get_trail(Name=created["TrailARN"])
    assert resp["Trail"]["Name"] == name


def test_get_trail_rejects_wrong_service_arn(ct):
    name = f"trail-wrong-service-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    with pytest.raises(ClientError) as exc:
        ct.get_trail(Name=f"arn:aws:sns:{REGION}:000000000000:trail/{name}")
    assert exc.value.response["Error"]["Code"] == "CloudTrailARNInvalidException"


def test_get_trail_rejects_malformed_arn_identifier(ct):
    with pytest.raises(ClientError) as exc:
        ct.get_trail(Name="arn:nope")
    assert exc.value.response["Error"]["Code"] == "CloudTrailARNInvalidException"


@pytest.mark.parametrize("partition", ["aws-cn", "notaws"])
def test_get_trail_rejects_non_aws_partition_as_invalid_trail_name(ct, partition):
    arn = f"arn:{partition}:cloudtrail:{REGION}:000000000000:trail/missing-{_uid()}"
    with pytest.raises(ClientError) as exc:
        ct.get_trail(Name=arn)
    assert exc.value.response["Error"]["Code"] == "InvalidTrailNameException"


def test_get_trail_does_not_resolve_foreign_region_arn_by_tail(ct):
    name = f"trail-foreign-region-{_uid()}"
    arn = ct.create_trail(Name=name, S3BucketName="bucket")["TrailARN"]
    foreign_arn = arn.replace(f":{REGION}:", ":us-west-2:")
    with pytest.raises(ClientError) as exc:
        ct.get_trail(Name=foreign_arn)
    assert exc.value.response["Error"]["Code"] == "TrailNotFoundException"


def test_read_trail_by_arn_from_different_request_region(ct):
    name = f"trail-cross-region-read-{_uid()}"
    arn = ct.create_trail(Name=name, S3BucketName="bucket")["TrailARN"]
    west_ct = _client("cloudtrail", region="us-west-2")

    get_resp = west_ct.get_trail(Name=arn)
    assert get_resp["Trail"]["Name"] == name

    status_resp = west_ct.get_trail_status(Name=arn)
    assert status_resp["IsLogging"] is True

    desc_resp = west_ct.describe_trails(trailNameList=[arn])
    assert [trail["TrailARN"] for trail in desc_resp["trailList"]] == [arn]


def test_get_trail_not_found(ct):
    with pytest.raises(ClientError) as exc:
        ct.get_trail(Name=f"nonexistent-{_uid()}")
    assert "TrailNotFoundException" in str(exc.value)


def test_get_trail_missing_aws_partition_arn_returns_not_found(ct):
    arn = f"arn:aws:cloudtrail:{REGION}:000000000000:trail/missing-{_uid()}"
    with pytest.raises(ClientError) as exc:
        ct.get_trail(Name=arn)
    assert exc.value.response["Error"]["Code"] == "TrailNotFoundException"


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


def test_start_stop_logging_flips_is_logging_state(ct):
    """StopLogging / StartLogging actually transition IsLogging on the trail
    record so subsequent GetTrailStatus reflects the change. Terraform's
    aws_cloudtrail data source asserts on this."""
    name = f"trail-state-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    assert ct.get_trail_status(Name=name)["IsLogging"] is True

    ct.stop_logging(Name=name)
    after_stop = ct.get_trail_status(Name=name)
    assert after_stop["IsLogging"] is False
    assert after_stop.get("StopLoggingTime") is not None

    ct.start_logging(Name=name)
    assert ct.get_trail_status(Name=name)["IsLogging"] is True


def test_list_trails_returns_summaries(ct):
    name = f"trail-list-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    out = ct.list_trails()
    arns = [t["TrailARN"] for t in out["Trails"]]
    assert any(arn.endswith(f":trail/{name}") for arn in arns)


def test_update_trail_persists_fields(ct):
    name = f"trail-update-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="orig")
    out = ct.update_trail(
        Name=name,
        S3BucketName="updated",
        IsMultiRegionTrail=True,
        EnableLogFileValidation=True,
    )
    assert out["S3BucketName"] == "updated"
    assert out["IsMultiRegionTrail"] is True
    assert out["LogFileValidationEnabled"] is True

    desc = ct.get_trail(Name=name)["Trail"]
    assert desc["S3BucketName"] == "updated"
    assert desc["IsMultiRegionTrail"] is True


def test_create_trail_persists_kms_key_id(ct):
    """CreateTrail persists KmsKeyId so DescribeTrails/GetTrail echo it. Without this a
    CMK-encrypted trail reads back with no KmsKeyId, so Terraform's aws_cloudtrail does
    not converge in one apply (the value only lands via a later UpdateTrail)."""
    name = f"trail-kms-{_uid()}"
    kms_arn = f"arn:aws:kms:{REGION}:000000000000:key/{_uid()}"
    resp = ct.create_trail(Name=name, S3BucketName="bucket", KmsKeyId=kms_arn)
    assert resp["KmsKeyId"] == kms_arn

    desc = ct.describe_trails(trailNameList=[name])["trailList"][0]
    assert desc["KmsKeyId"] == kms_arn

    got = ct.get_trail(Name=name)["Trail"]
    assert got["KmsKeyId"] == kms_arn


def test_create_trail_normalizes_bare_kms_key_id(ct):
    """A bare KMS key id is returned as a full key ARN, matching real AWS, which
    always echoes the CMK as an ARN — so a trail created with a key id shows no diff."""
    name = f"trail-kmsid-{_uid()}"
    key_id = f"{_uid()}-{_uid()}"
    resp = ct.create_trail(Name=name, S3BucketName="bucket", KmsKeyId=key_id)
    assert resp["KmsKeyId"] == f"arn:aws:kms:{REGION}:000000000000:key/{key_id}"
    desc = ct.describe_trails(trailNameList=[name])["trailList"][0]
    assert desc["KmsKeyId"] == resp["KmsKeyId"]


def test_create_trail_without_cmk_omits_kms_key_id(ct):
    """A trail created without a CMK omits KmsKeyId entirely rather than returning "".
    Real AWS omits unset optional fields; emitting an empty string for an ARN-typed
    field makes the Terraform aws provider fail (`parsing ... ARN (): arn: invalid
    prefix`), so guard that neither the create response nor the read-back carries one."""
    name = f"trail-nocmk-{_uid()}"
    resp = ct.create_trail(Name=name, S3BucketName="bucket")
    assert "KmsKeyId" not in resp
    assert "SnsTopicARN" not in resp

    desc = ct.describe_trails(trailNameList=[name])["trailList"][0]
    assert "KmsKeyId" not in desc
    assert "SnsTopicARN" not in desc


def test_create_trail_keeps_kms_alias(ct):
    """An ``alias/...`` reference is echoed verbatim — real AWS resolves it to the
    target key ARN, but the emulator keeps the caller's value rather than fabricate a
    wrong ARN (resolving the alias would need a KMS lookup it does not do)."""
    name = f"trail-alias-{_uid()}"
    alias = f"alias/ct-{_uid()}"
    resp = ct.create_trail(Name=name, S3BucketName="bucket", KmsKeyId=alias)
    assert resp["KmsKeyId"] == alias
    assert ct.describe_trails(trailNameList=[name])["trailList"][0]["KmsKeyId"] == alias


def test_update_trail_normalizes_kms_key_id(ct):
    """UpdateTrail normalizes KmsKeyId the same way CreateTrail does: a bare key id
    becomes a full key ARN; an already-full ARN is left as-is."""
    name = f"trail-upkms-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    key_id = f"{_uid()}-{_uid()}"
    out = ct.update_trail(Name=name, KmsKeyId=key_id)
    assert out["KmsKeyId"] == f"arn:aws:kms:{REGION}:000000000000:key/{key_id}"
    assert ct.describe_trails(trailNameList=[name])["trailList"][0]["KmsKeyId"] == out["KmsKeyId"]
    full = f"arn:aws:kms:{REGION}:000000000000:key/{_uid()}"
    assert ct.update_trail(Name=name, KmsKeyId=full)["KmsKeyId"] == full


def test_update_trail_without_cmk_omits_kms_key_id(ct):
    """UpdateTrail on a trail with no CMK omits KmsKeyId rather than returning "".
    AWS omits unset optional fields; an empty ARN makes the Terraform aws provider fail
    (`parsing ... ARN (): arn: invalid prefix`), so neither the UpdateTrail response nor
    the read-back may carry one — matching CreateTrail and DescribeTrails/GetTrail."""
    name = f"trail-upnocmk-{_uid()}"
    ct.create_trail(Name=name, S3BucketName="bucket")
    out = ct.update_trail(Name=name, S3KeyPrefix="logs/")
    assert "KmsKeyId" not in out
    assert "KmsKeyId" not in ct.describe_trails(trailNameList=[name])["trailList"][0]


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


def test_put_event_selectors_rejects_foreign_region_trail_arn(ct):
    name = f"trail-sel-foreign-{_uid()}"
    arn = ct.create_trail(Name=name, S3BucketName="bucket")["TrailARN"]
    foreign_arn = arn.replace(f":{REGION}:", ":us-west-2:")
    selectors = [{"ReadWriteType": "All", "IncludeManagementEvents": True, "DataResources": []}]

    with pytest.raises(ClientError) as exc:
        ct.put_event_selectors(TrailName=foreign_arn, EventSelectors=selectors)
    assert exc.value.response["Error"]["Code"] == "TrailNotFoundException"

    assert ct.get_event_selectors(TrailName=name)["EventSelectors"] == []


def test_get_event_selectors_by_arn_from_different_request_region(ct):
    name = f"trail-sel-cross-region-{_uid()}"
    arn = ct.create_trail(Name=name, S3BucketName="bucket")["TrailARN"]
    selectors = [{"ReadWriteType": "All", "IncludeManagementEvents": True, "DataResources": []}]
    ct.put_event_selectors(TrailName=name, EventSelectors=selectors)

    west_ct = _client("cloudtrail", region="us-west-2")
    resp = west_ct.get_event_selectors(TrailName=arn)
    assert resp["TrailARN"] == arn
    assert resp["EventSelectors"] == selectors


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


def test_add_tags_rejects_malformed_trail_arn(ct):
    with pytest.raises(ClientError) as exc:
        ct.add_tags(ResourceId="not-an-arn", TagsList=[{"Key": "env", "Value": "test"}])
    assert exc.value.response["Error"]["Code"] == "CloudTrailARNInvalidException"


@pytest.mark.parametrize("partition", ["aws-cn", "notaws"])
def test_add_tags_rejects_non_aws_partition_trail_arn(ct, partition):
    arn = f"arn:{partition}:cloudtrail:{REGION}:000000000000:trail/missing-{_uid()}"
    with pytest.raises(ClientError) as exc:
        ct.add_tags(ResourceId=arn, TagsList=[{"Key": "env", "Value": "test"}])
    assert exc.value.response["Error"]["Code"] == "CloudTrailARNInvalidException"


def test_add_tags_missing_aws_partition_trail_arn_returns_resource_not_found(ct):
    arn = f"arn:aws:cloudtrail:{REGION}:000000000000:trail/missing-{_uid()}"
    with pytest.raises(ClientError) as exc:
        ct.add_tags(ResourceId=arn, TagsList=[{"Key": "env", "Value": "test"}])
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_add_tags_rejects_foreign_region_trail_arn(ct):
    name = f"trail-tags-foreign-{_uid()}"
    arn = ct.create_trail(Name=name, S3BucketName="bucket")["TrailARN"]
    foreign_arn = arn.replace(f":{REGION}:", ":us-west-2:")

    with pytest.raises(ClientError) as exc:
        ct.add_tags(ResourceId=foreign_arn, TagsList=[{"Key": "env", "Value": "test"}])
    assert exc.value.response["Error"]["Code"] == "CloudTrailARNInvalidException"


def test_list_tags_rejects_wrong_service_trail_arn(ct):
    with pytest.raises(ClientError) as exc:
        ct.list_tags(ResourceIdList=[f"arn:aws:sns:{REGION}:000000000000:trail/not-a-trail"])
    assert exc.value.response["Error"]["Code"] == "CloudTrailARNInvalidException"


@pytest.mark.parametrize("partition", ["aws-cn", "notaws"])
def test_list_tags_rejects_non_aws_partition_trail_arn(ct, partition):
    arn = f"arn:{partition}:cloudtrail:{REGION}:000000000000:trail/missing-{_uid()}"
    with pytest.raises(ClientError) as exc:
        ct.list_tags(ResourceIdList=[arn])
    assert exc.value.response["Error"]["Code"] == "CloudTrailARNInvalidException"


def test_list_tags_missing_aws_partition_trail_arn_returns_resource_not_found(ct):
    arn = f"arn:aws:cloudtrail:{REGION}:000000000000:trail/missing-{_uid()}"
    with pytest.raises(ClientError) as exc:
        ct.list_tags(ResourceIdList=[arn])
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


@pytest.mark.parametrize("partition", ["aws-cn", "notaws"])
def test_remove_tags_rejects_non_aws_partition_trail_arn(ct, partition):
    arn = f"arn:{partition}:cloudtrail:{REGION}:000000000000:trail/missing-{_uid()}"
    with pytest.raises(ClientError) as exc:
        ct.remove_tags(ResourceId=arn, TagsList=[{"Key": "env"}])
    assert exc.value.response["Error"]["Code"] == "CloudTrailARNInvalidException"


def test_remove_tags_missing_aws_partition_trail_arn_returns_resource_not_found(ct):
    arn = f"arn:aws:cloudtrail:{REGION}:000000000000:trail/missing-{_uid()}"
    with pytest.raises(ClientError) as exc:
        ct.remove_tags(ResourceId=arn, TagsList=[{"Key": "env"}])
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


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
