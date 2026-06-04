import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _regional_client(service: str, region: str):
    return boto3.client(
        service,
        endpoint_url=_endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
        config=Config(
            region_name=region,
            retries={"mode": "standard"},
            max_pool_connections=50,
        ),
    )


def test_logs_put_get(logs):
    logs.create_log_group(logGroupName="/test/ministack")
    logs.create_log_stream(logGroupName="/test/ministack", logStreamName="stream1")
    logs.put_log_events(
        logGroupName="/test/ministack",
        logStreamName="stream1",
        logEvents=[
            {"timestamp": int(time.time() * 1000), "message": "Hello from MiniStack"},
            {"timestamp": int(time.time() * 1000), "message": "Second log line"},
        ],
    )
    resp = logs.get_log_events(logGroupName="/test/ministack", logStreamName="stream1")
    assert len(resp["events"]) == 2

def test_logs_filter(logs):
    resp = logs.filter_log_events(logGroupName="/test/ministack", filterPattern="MiniStack")
    assert len(resp["events"]) >= 1


def test_logs_get_log_events_by_identifier_arn(logs):
    logs.create_log_group(logGroupName="/cwl/ident-arn")
    logs.create_log_stream(logGroupName="/cwl/ident-arn", logStreamName="s1")
    logs.put_log_events(
        logGroupName="/cwl/ident-arn",
        logStreamName="s1",
        logEvents=[{"timestamp": int(time.time() * 1000), "message": "hi"}],
    )
    arn = "arn:aws:logs:us-east-1:000000000000:log-group:/cwl/ident-arn:*"
    resp = logs.get_log_events(logGroupIdentifier=arn, logStreamName="s1")
    assert len(resp["events"]) == 1
    # Bare name accepted as identifier too.
    resp2 = logs.get_log_events(logGroupIdentifier="/cwl/ident-arn", logStreamName="s1")
    assert len(resp2["events"]) == 1


def test_logs_identifier_arn_scope_does_not_fallback_to_local_group(logs):
    group = f"/cwl/ident-arn-scope-{_uuid_mod.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    logs.create_log_stream(logGroupName=group, logStreamName="s1")
    logs.put_log_events(
        logGroupName=group,
        logStreamName="s1",
        logEvents=[{"timestamp": int(time.time() * 1000), "message": "hi"}],
    )

    arn = f"arn:aws:logs:us-east-1:000000000000:log-group:{group}:*"
    wrong_region = arn.replace(":us-east-1:", ":us-west-2:")
    wrong_account = arn.replace(":000000000000:", ":111111111111:")
    wrong_service = arn.replace(":logs:", ":lambda:")
    wrong_resource = arn.replace(":log-group:", ":delivery:")
    for bad_ref in (wrong_region, wrong_account, wrong_service, wrong_resource):
        with pytest.raises(ClientError) as exc:
            logs.get_log_events(logGroupIdentifier=bad_ref, logStreamName="s1")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_logs_filter_log_events_by_identifier_arn(logs):
    logs.create_log_group(logGroupName="/cwl/ident-arn-flt")
    logs.create_log_stream(logGroupName="/cwl/ident-arn-flt", logStreamName="s1")
    logs.put_log_events(
        logGroupName="/cwl/ident-arn-flt",
        logStreamName="s1",
        logEvents=[{"timestamp": int(time.time() * 1000), "message": "ERROR boom"}],
    )
    arn = "arn:aws:logs:us-east-1:000000000000:log-group:/cwl/ident-arn-flt"
    resp = logs.filter_log_events(logGroupIdentifier=arn, filterPattern="ERROR")
    assert len(resp["events"]) >= 1

def test_logs_create_group_v2(logs):
    logs.create_log_group(logGroupName="/cwl/cg-v2")
    resp = logs.describe_log_groups(logGroupNamePrefix="/cwl/cg-v2")
    assert any(g["logGroupName"] == "/cwl/cg-v2" for g in resp["logGroups"])

def test_logs_create_group_duplicate_v2(logs):
    logs.create_log_group(logGroupName="/cwl/dup-v2")
    with pytest.raises(ClientError) as exc:
        logs.create_log_group(logGroupName="/cwl/dup-v2")
    assert exc.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"

def test_logs_delete_group_v2(logs):
    logs.create_log_group(logGroupName="/cwl/del-v2")
    logs.delete_log_group(logGroupName="/cwl/del-v2")
    resp = logs.describe_log_groups(logGroupNamePrefix="/cwl/del-v2")
    assert not any(g["logGroupName"] == "/cwl/del-v2" for g in resp["logGroups"])

def test_logs_describe_groups_v2(logs):
    logs.create_log_group(logGroupName="/cwl/dg-a")
    logs.create_log_group(logGroupName="/cwl/dg-b")
    resp = logs.describe_log_groups(logGroupNamePrefix="/cwl/dg-")
    names = [g["logGroupName"] for g in resp["logGroups"]]
    assert "/cwl/dg-a" in names
    assert "/cwl/dg-b" in names

def test_logs_create_stream_v2(logs):
    logs.create_log_group(logGroupName="/cwl/str-v2")
    logs.create_log_stream(logGroupName="/cwl/str-v2", logStreamName="stream-a")
    logs.create_log_stream(logGroupName="/cwl/str-v2", logStreamName="stream-b")
    resp = logs.describe_log_streams(logGroupName="/cwl/str-v2")
    names = [s["logStreamName"] for s in resp["logStreams"]]
    assert "stream-a" in names
    assert "stream-b" in names

def test_logs_put_get_events_v2(logs):
    logs.create_log_group(logGroupName="/cwl/pge-v2")
    logs.create_log_stream(logGroupName="/cwl/pge-v2", logStreamName="s1")
    now = int(time.time() * 1000)
    logs.put_log_events(
        logGroupName="/cwl/pge-v2",
        logStreamName="s1",
        logEvents=[
            {"timestamp": now, "message": "first line"},
            {"timestamp": now + 1, "message": "second line"},
            {"timestamp": now + 2, "message": "third line"},
        ],
    )
    resp = logs.get_log_events(logGroupName="/cwl/pge-v2", logStreamName="s1")
    assert len(resp["events"]) == 3
    assert resp["events"][0]["message"] == "first line"
    assert resp["events"][2]["message"] == "third line"

def test_logs_filter_events_v2(logs):
    logs.create_log_group(logGroupName="/cwl/flt-v2")
    logs.create_log_stream(logGroupName="/cwl/flt-v2", logStreamName="s1")
    now = int(time.time() * 1000)
    logs.put_log_events(
        logGroupName="/cwl/flt-v2",
        logStreamName="s1",
        logEvents=[
            {"timestamp": now, "message": "ERROR disk full"},
            {"timestamp": now + 1, "message": "INFO all clear"},
            {"timestamp": now + 2, "message": "ERROR timeout"},
        ],
    )
    resp = logs.filter_log_events(logGroupName="/cwl/flt-v2", filterPattern="ERROR")
    assert len(resp["events"]) == 2
    msgs = [e["message"] for e in resp["events"]]
    assert "ERROR disk full" in msgs
    assert "ERROR timeout" in msgs

def test_logs_retention_policy_v2(logs):
    logs.create_log_group(logGroupName="/cwl/ret-v2")
    logs.put_retention_policy(logGroupName="/cwl/ret-v2", retentionInDays=30)
    resp = logs.describe_log_groups(logGroupNamePrefix="/cwl/ret-v2")
    grp = next(g for g in resp["logGroups"] if g["logGroupName"] == "/cwl/ret-v2")
    assert grp["retentionInDays"] == 30

    logs.delete_retention_policy(logGroupName="/cwl/ret-v2")
    resp2 = logs.describe_log_groups(logGroupNamePrefix="/cwl/ret-v2")
    grp2 = next(g for g in resp2["logGroups"] if g["logGroupName"] == "/cwl/ret-v2")
    assert "retentionInDays" not in grp2

def test_logs_tags_v2(logs):
    logs.create_log_group(logGroupName="/cwl/tag-v2", tags={"env": "prod"})
    resp = logs.list_tags_log_group(logGroupName="/cwl/tag-v2")
    assert resp["tags"]["env"] == "prod"

    logs.tag_log_group(logGroupName="/cwl/tag-v2", tags={"team": "infra"})
    resp2 = logs.list_tags_log_group(logGroupName="/cwl/tag-v2")
    assert resp2["tags"]["env"] == "prod"
    assert resp2["tags"]["team"] == "infra"

    logs.untag_log_group(logGroupName="/cwl/tag-v2", tags=["env"])
    resp3 = logs.list_tags_log_group(logGroupName="/cwl/tag-v2")
    assert "env" not in resp3["tags"]
    assert resp3["tags"]["team"] == "infra"

def test_logs_put_requires_group_v2(logs):
    with pytest.raises(ClientError) as exc:
        logs.put_log_events(
            logGroupName="/cwl/nonexistent-xyz",
            logStreamName="s1",
            logEvents=[{"timestamp": int(time.time() * 1000), "message": "fail"}],
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

def test_logs_retention_policy(logs):
    import uuid as _uuid

    group = f"/intg/retention/{_uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    logs.put_retention_policy(logGroupName=group, retentionInDays=7)
    groups = logs.describe_log_groups(logGroupNamePrefix=group)["logGroups"]
    assert groups[0].get("retentionInDays") == 7
    logs.delete_retention_policy(logGroupName=group)
    groups2 = logs.describe_log_groups(logGroupNamePrefix=group)["logGroups"]
    assert groups2[0].get("retentionInDays") is None

def test_logs_subscription_filter(logs):
    import uuid as _uuid

    group = f"/intg/subfilter/{_uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    logs.put_subscription_filter(
        logGroupName=group,
        filterName="my-filter",
        filterPattern="ERROR",
        destinationArn="arn:aws:lambda:us-east-1:000000000000:function:log-handler",
    )
    resp = logs.describe_subscription_filters(logGroupName=group)
    assert any(f["filterName"] == "my-filter" for f in resp["subscriptionFilters"])
    logs.delete_subscription_filter(logGroupName=group, filterName="my-filter")
    resp2 = logs.describe_subscription_filters(logGroupName=group)
    assert not any(f["filterName"] == "my-filter" for f in resp2["subscriptionFilters"])

def test_logs_metric_filter(logs):
    import uuid as _uuid

    group = f"/intg/metricfilter/{_uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    logs.put_metric_filter(
        logGroupName=group,
        filterName="error-count",
        filterPattern="[ERROR]",
        metricTransformations=[
            {
                "metricName": "ErrorCount",
                "metricNamespace": "MyApp",
                "metricValue": "1",
            }
        ],
    )
    resp = logs.describe_metric_filters(logGroupName=group)
    assert any(f["filterName"] == "error-count" for f in resp["metricFilters"])
    logs.delete_metric_filter(logGroupName=group, filterName="error-count")
    resp2 = logs.describe_metric_filters(logGroupName=group)
    assert not any(f["filterName"] == "error-count" for f in resp2.get("metricFilters", []))


def test_logs_metric_filters_are_region_scoped():
    group = f"/intg/metricfilter-region/{_uuid_mod.uuid4().hex[:8]}"
    east = _regional_client("logs", "us-east-1")
    west = _regional_client("logs", "us-west-2")

    east.create_log_group(logGroupName=group)
    west.create_log_group(logGroupName=group)
    east.put_metric_filter(
        logGroupName=group,
        filterName="error-count",
        filterPattern="ERROR",
        metricTransformations=[{
            "metricName": "EastErrors",
            "metricNamespace": "MyApp",
            "metricValue": "1",
        }],
    )
    west.put_metric_filter(
        logGroupName=group,
        filterName="error-count",
        filterPattern="WARN",
        metricTransformations=[{
            "metricName": "WestWarnings",
            "metricNamespace": "MyApp",
            "metricValue": "1",
        }],
    )

    east_filters = east.describe_metric_filters(logGroupName=group)["metricFilters"]
    west_filters = west.describe_metric_filters(logGroupName=group)["metricFilters"]

    assert [f["filterPattern"] for f in east_filters] == ["ERROR"]
    assert [f["filterPattern"] for f in west_filters] == ["WARN"]
    assert east.describe_log_groups(logGroupNamePrefix=group)["logGroups"][0]["metricFilterCount"] == 1
    assert west.describe_log_groups(logGroupNamePrefix=group)["logGroups"][0]["metricFilterCount"] == 1


def test_logs_tag_log_group(logs):
    import uuid as _uuid

    group = f"/intg/tagging/{_uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    logs.tag_log_group(logGroupName=group, tags={"project": "ministack", "env": "test"})
    resp = logs.list_tags_log_group(logGroupName=group)
    assert resp["tags"].get("project") == "ministack"
    logs.untag_log_group(logGroupName=group, tags=["project"])
    resp2 = logs.list_tags_log_group(logGroupName=group)
    assert "project" not in resp2["tags"]

def test_logs_insights_start_query(logs):
    import uuid as _uuid

    group = f"/intg/insights/{_uuid.uuid4().hex[:8]}"
    logs.create_log_group(logGroupName=group)
    resp = logs.start_query(
        logGroupName=group,
        startTime=int(time.time()) - 3600,
        endTime=int(time.time()),
        queryString="fields @timestamp, @message | limit 10",
    )
    assert "queryId" in resp
    results = logs.get_query_results(queryId=resp["queryId"])
    assert results["status"] in ("Complete", "Running", "Scheduled")

def test_logs_filter_with_wildcard(logs):
    """FilterLogEvents with wildcard pattern matches correctly."""
    logs.create_log_group(logGroupName="/qa/logs/wildcard")
    logs.create_log_stream(logGroupName="/qa/logs/wildcard", logStreamName="stream1")
    logs.put_log_events(
        logGroupName="/qa/logs/wildcard",
        logStreamName="stream1",
        logEvents=[
            {"timestamp": int(time.time() * 1000), "message": "ERROR: disk full"},
            {"timestamp": int(time.time() * 1000), "message": "INFO: all good"},
            {"timestamp": int(time.time() * 1000), "message": "ERROR: timeout"},
        ],
    )
    resp = logs.filter_log_events(logGroupName="/qa/logs/wildcard", filterPattern="ERROR*")
    messages = [e["message"] for e in resp["events"]]
    assert all("ERROR" in m for m in messages)
    assert len(messages) == 2

def test_logs_describe_log_groups_prefix(logs):
    """DescribeLogGroups with logGroupNamePrefix filters correctly."""
    logs.create_log_group(logGroupName="/qa/logs/prefix/alpha")
    logs.create_log_group(logGroupName="/qa/logs/prefix/beta")
    logs.create_log_group(logGroupName="/qa/logs/other/gamma")
    resp = logs.describe_log_groups(logGroupNamePrefix="/qa/logs/prefix")
    names = [g["logGroupName"] for g in resp["logGroups"]]
    assert "/qa/logs/prefix/alpha" in names
    assert "/qa/logs/prefix/beta" in names
    assert "/qa/logs/other/gamma" not in names

def test_logs_retention_policy_invalid_value(logs):
    """PutRetentionPolicy with invalid days raises InvalidParameterException."""
    logs.create_log_group(logGroupName="/qa/logs/retention-invalid")
    with pytest.raises(ClientError) as exc:
        logs.put_retention_policy(logGroupName="/qa/logs/retention-invalid", retentionInDays=999)
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"

def test_logs_list_tags_for_resource_arn_without_star(logs):
    name = "/tf/regression/arn-no-star"
    logs.create_log_group(logGroupName=name, tags={"env": "test"})
    # Get the ARN as stored (includes :*)
    groups = logs.describe_log_groups(logGroupNamePrefix=name)["logGroups"]
    stored_arn = groups[0]["arn"]
    assert stored_arn.endswith(":*"), f"Expected stored ARN to end with :*, got {stored_arn}"

    # Terraform sends the ARN without :* — this must not raise ResourceNotFoundException
    arn_no_star = stored_arn[:-2]  # strip ':*'
    resp = logs.list_tags_for_resource(resourceArn=arn_no_star)
    assert resp["tags"]["env"] == "test"
    logs.delete_log_group(logGroupName=name)

def test_logs_get_log_events_pagination_stops(logs):
    """GetLogEvents must return the caller's token when at end of stream to stop SDK pagination."""
    group = "/test/pagination-stop"
    stream = "s1"
    logs.create_log_group(logGroupName=group)
    logs.create_log_stream(logGroupName=group, logStreamName=stream)
    logs.put_log_events(
        logGroupName=group, logStreamName=stream,
        logEvents=[
            {"timestamp": 1000, "message": "msg1"},
            {"timestamp": 2000, "message": "msg2"},
        ],
    )

    # First call — get all events
    resp = logs.get_log_events(logGroupName=group, logStreamName=stream, startFromHead=True)
    assert len(resp["events"]) == 2
    fwd_token = resp["nextForwardToken"]

    # Second call with forward token — no more events, token must match what we sent
    resp2 = logs.get_log_events(logGroupName=group, logStreamName=stream, nextToken=fwd_token)
    assert len(resp2["events"]) == 0
    assert resp2["nextForwardToken"] == fwd_token  # same token = stop paginating


# ---------------------------------------------------------------------------
# Destination operations
# ---------------------------------------------------------------------------

def test_logs_put_destination(logs):
    """PutDestination creates a destination and returns its metadata."""
    uid = _uuid_mod.uuid4().hex[:8]
    dest_name = f"test-dest-{uid}"
    target_arn = f"arn:aws:kinesis:us-east-1:000000000000:stream/dest-stream-{uid}"
    role_arn = f"arn:aws:iam::000000000000:role/dest-role-{uid}"

    resp = logs.put_destination(
        destinationName=dest_name,
        targetArn=target_arn,
        roleArn=role_arn,
    )
    dest = resp["destination"]
    assert dest["destinationName"] == dest_name
    assert dest["targetArn"] == target_arn
    assert dest["roleArn"] == role_arn
    assert "arn" in dest
    assert "creationTime" in dest

    # cleanup
    logs.delete_destination(destinationName=dest_name)


def test_logs_delete_destination(logs):
    """DeleteDestination removes a destination; deleting again raises ResourceNotFoundException."""
    uid = _uuid_mod.uuid4().hex[:8]
    dest_name = f"test-dest-del-{uid}"
    logs.put_destination(
        destinationName=dest_name,
        targetArn="arn:aws:kinesis:us-east-1:000000000000:stream/s1",
        roleArn="arn:aws:iam::000000000000:role/r1",
    )

    logs.delete_destination(destinationName=dest_name)

    with pytest.raises(ClientError) as exc:
        logs.delete_destination(destinationName=dest_name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_logs_describe_destinations(logs):
    """DescribeDestinations lists destinations filtered by prefix."""
    uid = _uuid_mod.uuid4().hex[:8]
    name_a = f"desc-dest-{uid}-alpha"
    name_b = f"desc-dest-{uid}-beta"
    name_c = f"other-dest-{uid}"

    for n in (name_a, name_b, name_c):
        logs.put_destination(
            destinationName=n,
            targetArn="arn:aws:kinesis:us-east-1:000000000000:stream/s1",
            roleArn="arn:aws:iam::000000000000:role/r1",
        )

    resp = logs.describe_destinations(DestinationNamePrefix=f"desc-dest-{uid}")
    names = [d["destinationName"] for d in resp["destinations"]]
    assert name_a in names
    assert name_b in names
    assert name_c not in names

    # cleanup
    for n in (name_a, name_b, name_c):
        logs.delete_destination(destinationName=n)


def test_logs_put_destination_policy(logs):
    """PutDestinationPolicy updates the accessPolicy on an existing destination."""
    uid = _uuid_mod.uuid4().hex[:8]
    dest_name = f"test-dest-pol-{uid}"
    logs.put_destination(
        destinationName=dest_name,
        targetArn="arn:aws:kinesis:us-east-1:000000000000:stream/s1",
        roleArn="arn:aws:iam::000000000000:role/r1",
    )

    policy = json.dumps({"Statement": [{"Effect": "Allow", "Principal": "*", "Action": "logs:PutSubscriptionFilter"}]})
    logs.put_destination_policy(destinationName=dest_name, accessPolicy=policy)

    resp = logs.describe_destinations(DestinationNamePrefix=dest_name)
    dest = next(d for d in resp["destinations"] if d["destinationName"] == dest_name)
    assert dest["accessPolicy"] == policy

    # cleanup
    logs.delete_destination(destinationName=dest_name)


# ---------------------------------------------------------------------------
# ARN-based tagging operations (TagResource / UntagResource)
# ---------------------------------------------------------------------------

def test_logs_tag_resource(logs):
    """TagResource adds tags to a log group resolved by ARN."""
    uid = _uuid_mod.uuid4().hex[:8]
    group = f"/intg/tag-resource/{uid}"
    logs.create_log_group(logGroupName=group)

    groups = logs.describe_log_groups(logGroupNamePrefix=group)["logGroups"]
    arn = groups[0]["arn"]

    logs.tag_resource(resourceArn=arn, tags={"team": "platform", "env": "staging"})

    resp = logs.list_tags_for_resource(resourceArn=arn)
    assert resp["tags"]["team"] == "platform"
    assert resp["tags"]["env"] == "staging"

    # cleanup
    logs.delete_log_group(logGroupName=group)


def test_logs_untag_resource(logs):
    """UntagResource removes tags from a log group resolved by ARN."""
    uid = _uuid_mod.uuid4().hex[:8]
    group = f"/intg/untag-resource/{uid}"
    logs.create_log_group(logGroupName=group, tags={"keep": "yes", "remove": "me"})

    groups = logs.describe_log_groups(logGroupNamePrefix=group)["logGroups"]
    arn = groups[0]["arn"]

    logs.untag_resource(resourceArn=arn, tagKeys=["remove"])

    resp = logs.list_tags_for_resource(resourceArn=arn)
    assert resp["tags"]["keep"] == "yes"
    assert "remove" not in resp["tags"]

    # cleanup
    logs.delete_log_group(logGroupName=group)


# ---------------------------------------------------------------------------
# StopQuery
# ---------------------------------------------------------------------------

def test_logs_stop_query(logs):
    """StopQuery cancels a running query and sets its status to Cancelled."""
    uid = _uuid_mod.uuid4().hex[:8]
    group = f"/intg/stop-query/{uid}"
    logs.create_log_group(logGroupName=group)

    start_resp = logs.start_query(
        logGroupName=group,
        startTime=int(time.time()) - 3600,
        endTime=int(time.time()),
        queryString="fields @timestamp | limit 5",
    )
    query_id = start_resp["queryId"]

    stop_resp = logs.stop_query(queryId=query_id)
    assert stop_resp["success"] is True

    results = logs.get_query_results(queryId=query_id)
    assert results["status"] == "Cancelled"

    # cleanup
    logs.delete_log_group(logGroupName=group)


# ---------------------------------------------------------------------------
# Log Delivery API (PutDeliverySource / DeliveryDestination / Create+Describe)
# ---------------------------------------------------------------------------

def test_logs_delivery_source_crud(logs):
    """Put/Get/Describe/Delete round-trip for a delivery source.

    Per AWS's contract ``PutDeliverySource`` is idempotent (upsert)
    and ``DescribeDeliverySources`` must include the record after
    creation.
    """
    uid = _uuid_mod.uuid4().hex[:8]
    src_name = f"intg-src-{uid}"
    resource_arn = f"arn:aws:bedrock:us-east-1:000000000000:model/anthropic.x-{uid}"

    put_resp = logs.put_delivery_source(
        name=src_name,
        resourceArn=resource_arn,
        logType="APPLICATION_LOGS",
    )
    assert put_resp["deliverySource"]["name"] == src_name
    assert put_resp["deliverySource"]["resourceArns"] == [resource_arn]
    assert put_resp["deliverySource"]["logType"] == "APPLICATION_LOGS"
    assert put_resp["deliverySource"]["arn"].endswith(f":delivery-source:{src_name}")

    get_resp = logs.get_delivery_source(name=src_name)
    assert get_resp["deliverySource"]["logType"] == "APPLICATION_LOGS"

    describe_resp = logs.describe_delivery_sources()
    assert any(s["name"] == src_name for s in describe_resp["deliverySources"])

    logs.delete_delivery_source(name=src_name)
    with pytest.raises(ClientError) as exc:
        logs.get_delivery_source(name=src_name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_logs_delivery_destination_crud(logs):
    """Put/Get/Describe/Delete round-trip for a delivery destination."""
    uid = _uuid_mod.uuid4().hex[:8]
    dest_name = f"intg-dest-{uid}"
    dest_resource_arn = f"arn:aws:logs:us-east-1:000000000000:log-group:/intg/delivery-{uid}:*"

    put_resp = logs.put_delivery_destination(
        name=dest_name,
        deliveryDestinationConfiguration={
            "destinationResourceArn": dest_resource_arn,
        },
    )
    assert put_resp["deliveryDestination"]["name"] == dest_name
    assert (
        put_resp["deliveryDestination"]["deliveryDestinationConfiguration"]["destinationResourceArn"]
        == dest_resource_arn
    )
    assert put_resp["deliveryDestination"]["arn"].endswith(f":delivery-destination:{dest_name}")

    get_resp = logs.get_delivery_destination(name=dest_name)
    assert (
        get_resp["deliveryDestination"]["deliveryDestinationConfiguration"]["destinationResourceArn"]
        == dest_resource_arn
    )

    describe_resp = logs.describe_delivery_destinations()
    assert any(d["name"] == dest_name for d in describe_resp["deliveryDestinations"])

    logs.delete_delivery_destination(name=dest_name)


def test_logs_delivery_create_binds_source_and_destination(logs):
    """CreateDelivery wires a source to a destination and returns a delivery id/ARN."""
    uid = _uuid_mod.uuid4().hex[:8]
    src_name = f"intg-src-{uid}"
    dest_name = f"intg-dest-{uid}"

    logs.put_delivery_source(
        name=src_name,
        resourceArn=f"arn:aws:bedrock:us-east-1:000000000000:model/test-{uid}",
        logType="APPLICATION_LOGS",
    )
    dest_resp = logs.put_delivery_destination(
        name=dest_name,
        deliveryDestinationConfiguration={
            "destinationResourceArn": f"arn:aws:logs:us-east-1:000000000000:log-group:/intg/d-{uid}:*",
        },
    )
    dest_arn = dest_resp["deliveryDestination"]["arn"]

    create_resp = logs.create_delivery(
        deliverySourceName=src_name,
        deliveryDestinationArn=dest_arn,
    )
    delivery = create_resp["delivery"]
    assert delivery["deliverySourceName"] == src_name
    assert delivery["deliveryDestinationArn"] == dest_arn
    assert delivery["arn"].startswith("arn:aws:logs:")

    describe_resp = logs.describe_deliveries()
    assert any(d["id"] == delivery["id"] for d in describe_resp["deliveries"])

    logs.delete_delivery(id=delivery["id"])
    logs.delete_delivery_destination(name=dest_name)
    logs.delete_delivery_source(name=src_name)


# ---------------------------------------------------------------------------
# Log Delivery — validation & AWS-derived fields (hardening)
# ---------------------------------------------------------------------------

def test_logs_delivery_source_service_derived_from_resource_arn(logs):
    """PutDeliverySource must set ``service`` from the resource ARN, ignoring
    any value the caller sends. AWS treats this as a server-computed
    field — callers cannot override it."""
    uid = _uuid_mod.uuid4().hex[:8]
    src_name = f"intg-svc-{uid}"
    resp = logs.put_delivery_source(
        name=src_name,
        resourceArn=f"arn:aws:bedrock:us-east-1:000000000000:model/anthropic.claude-{uid}",
        logType="APPLICATION_LOGS",
    )
    assert resp["deliverySource"]["service"] == "bedrock"
    logs.delete_delivery_source(name=src_name)


def test_logs_delivery_source_rejects_malformed_resource_arn(logs):
    """Malformed delivery source ARNs fail before any source is stored."""
    uid = _uuid_mod.uuid4().hex[:8]
    with pytest.raises(ClientError) as exc:
        logs.put_delivery_source(
            name=f"intg-svc-malformed-{uid}",
            resourceArn="not-an-arn",
            logType="APPLICATION_LOGS",
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"


@pytest.mark.parametrize(
    ("resource_arn", "expected_code"),
    [
        ("arn:aws:bedrock:us-west-2:000000000000:knowledge-base/test", "ValidationException"),
        ("arn:aws:bedrock:us-east-1:111111111111:knowledge-base/test", "ValidationException"),
        ("arn:aws:sns:us-east-1:000000000000:test", "ResourceNotFoundException"),
    ],
)
def test_logs_delivery_source_rejects_wrong_scope_resource_arns(logs, resource_arn, expected_code):
    uid = _uuid_mod.uuid4().hex[:8]
    with pytest.raises(ClientError) as exc:
        logs.put_delivery_source(
            name=f"intg-svc-scope-{uid}",
            resourceArn=resource_arn,
            logType="APPLICATION_LOGS",
        )
    assert exc.value.response["Error"]["Code"] == expected_code


def test_logs_delivery_destination_type_derived_from_arn(logs):
    """PutDeliveryDestination must compute ``deliveryDestinationType`` from
    the destinationResourceArn (S3 / CWL / FH)."""
    uid = _uuid_mod.uuid4().hex[:8]
    cases = [
        (f"arn:aws:s3:::bucket-{uid}", "S3"),
        (f"arn:aws:logs:us-east-1:000000000000:log-group:/intg/{uid}:*", "CWL"),
        (f"arn:aws:firehose:us-east-1:000000000000:deliverystream/{uid}", "FH"),
    ]
    for i, (arn, expected_type) in enumerate(cases):
        dest_name = f"intg-type-{uid}-{i}"
        resp = logs.put_delivery_destination(
            name=dest_name,
            deliveryDestinationConfiguration={"destinationResourceArn": arn},
        )
        assert resp["deliveryDestination"]["deliveryDestinationType"] == expected_type
        logs.delete_delivery_destination(name=dest_name)


@pytest.mark.parametrize(
    "destination_resource_arn",
    [
        "arn:aws:logs:us-west-2:000000000000:log-group:/intg/foreign:*",
        "arn:aws:logs:us-east-1:111111111111:log-group:/intg/bogus:*",
        "arn:aws:firehose:us-west-2:000000000000:deliverystream/foreign",
        "arn:aws:firehose:us-east-1:111111111111:deliverystream/bogus",
        "arn:aws:s3:us-east-1:000000000000:bucket-with-region",
    ],
)
def test_logs_delivery_destination_rejects_wrong_scope_target_arns(logs, destination_resource_arn):
    uid = _uuid_mod.uuid4().hex[:8]
    with pytest.raises(ClientError) as exc:
        logs.put_delivery_destination(
            name=f"intg-dest-scope-{uid}",
            deliveryDestinationConfiguration={"destinationResourceArn": destination_resource_arn},
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_logs_delivery_destination_rejects_unknown_output_format(logs):
    """outputFormat outside the AWS-allowed set is rejected."""
    uid = _uuid_mod.uuid4().hex[:8]
    with pytest.raises(ClientError) as exc:
        logs.put_delivery_destination(
            name=f"intg-bad-{uid}",
            outputFormat="yaml",
            deliveryDestinationConfiguration={
                "destinationResourceArn": f"arn:aws:logs:us-east-1:000000000000:log-group:/x/{uid}:*",
            },
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_logs_delivery_destination_rejects_unsupported_target(logs):
    """destinationResourceArn that isn't S3/CWL/FH is rejected upfront."""
    uid = _uuid_mod.uuid4().hex[:8]
    cases = [
        "not-an-arn",
        f"arn:aws:lambda:us-east-1:000000000000:function:anything-{uid}",
    ]
    for i, arn in enumerate(cases):
        with pytest.raises(ClientError) as exc:
            logs.put_delivery_destination(
                name=f"intg-bad-target-{uid}-{i}",
                deliveryDestinationConfiguration={
                    "destinationResourceArn": arn,
                },
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_logs_create_delivery_requires_destination_to_exist(logs):
    """CreateDelivery pointed at a non-existent destination must raise
    ResourceNotFoundException (real AWS cannot ship to an unknown sink)."""
    uid = _uuid_mod.uuid4().hex[:8]
    src_name = f"intg-src-{uid}"
    logs.put_delivery_source(
        name=src_name,
        resourceArn=f"arn:aws:bedrock:us-east-1:000000000000:model/claude-{uid}",
        logType="APPLICATION_LOGS",
    )
    try:
        with pytest.raises(ClientError) as exc:
            logs.create_delivery(
                deliverySourceName=src_name,
                deliveryDestinationArn=f"arn:aws:logs:us-east-1:000000000000:delivery-destination:never-created-{uid}",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        logs.delete_delivery_source(name=src_name)


@pytest.mark.parametrize(
    ("delivery_destination_arn", "expected_code"),
    [
        ("arn:aws:logs:us-west-2:000000000000:delivery-destination:foreign", "ValidationException"),
        ("arn:aws:logs:us-east-1:111111111111:delivery-destination:bogus", "AccessDeniedException"),
        ("not-an-arn", "ValidationException"),
        ("arn:aws:sns:us-east-1:000000000000:not-a-delivery-destination", "ValidationException"),
    ],
)
def test_logs_create_delivery_rejects_invalid_destination_arn_before_source(logs, delivery_destination_arn, expected_code):
    uid = _uuid_mod.uuid4().hex[:8]
    with pytest.raises(ClientError) as exc:
        logs.create_delivery(
            deliverySourceName=f"missing-src-{uid}",
            deliveryDestinationArn=delivery_destination_arn,
        )
    assert exc.value.response["Error"]["Code"] == expected_code


def test_logs_create_delivery_rejects_duplicate_pair(logs):
    """AWS allows at most one Delivery per (source, destination) pair;
    a second CreateDelivery against the same pair raises
    ConflictException."""
    uid = _uuid_mod.uuid4().hex[:8]
    src_name = f"intg-dup-src-{uid}"
    dest_name = f"intg-dup-dest-{uid}"

    logs.put_delivery_source(
        name=src_name,
        resourceArn=f"arn:aws:bedrock:us-east-1:000000000000:model/x-{uid}",
        logType="APPLICATION_LOGS",
    )
    dest_resp = logs.put_delivery_destination(
        name=dest_name,
        deliveryDestinationConfiguration={
            "destinationResourceArn": f"arn:aws:logs:us-east-1:000000000000:log-group:/intg/d-{uid}:*",
        },
    )
    dest_arn = dest_resp["deliveryDestination"]["arn"]

    try:
        first = logs.create_delivery(
            deliverySourceName=src_name,
            deliveryDestinationArn=dest_arn,
        )

        with pytest.raises(ClientError) as exc:
            logs.create_delivery(
                deliverySourceName=src_name,
                deliveryDestinationArn=dest_arn,
            )
        assert exc.value.response["Error"]["Code"] == "ConflictException"

        logs.delete_delivery(id=first["delivery"]["id"])
    finally:
        logs.delete_delivery_destination(name=dest_name)
        logs.delete_delivery_source(name=src_name)


# ========== from test_cloudwatch_logs_persistence.py ==========
# CloudWatch Logs persistence symmetry.
import importlib

import pytest

from ministack.core import persistence


def _module():
    return importlib.import_module("ministack.services.cloudwatch_logs")


@pytest.fixture(autouse=True)
def _enable_persistence(monkeypatch, tmp_path):
    """Force PERSIST_STATE on and point STATE_DIR at a tmp dir for the
    duration of each test so save_state / load_state actually write and
    read JSON instead of short-circuiting."""
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))


def _round_trip(mod, svc_key="cloudwatch_logs"):
    """Simulate a full warm-boot through the on-disk JSON path."""
    persistence.save_state(svc_key, mod.get_state())
    mod.reset()
    loaded = persistence.load_state(svc_key)
    assert loaded is not None, (
        f"persistence.load_state({svc_key!r}) returned None — state "
        "file was not written by save_state(). Check get_state() "
        "correctness and that PERSIST_STATE is True."
    )
    mod.restore_state(loaded)


# ── _destinations ──────────────────────────────────────────────────────

def test_destinations_survive_warm_boot():
    mod = _module()
    mod.reset()
    mod._destinations["my-dest"] = {
        "destinationName": "my-dest",
        "targetArn": "arn:aws:kinesis:us-east-1:000000000000:stream/log-stream",
        "roleArn": "arn:aws:iam::000000000000:role/CWLtoKinesis",
        "accessPolicy": "",
        "arn": "arn:aws:logs:us-east-1:000000000000:destination:my-dest",
        "creationTime": 1700000000000,
    }

    _round_trip(mod)

    assert "my-dest" in mod._destinations, (
        "CloudWatch Logs destination lost across get_state → restore_state — "
        "_destinations must be in both."
    )
    assert mod._destinations["my-dest"]["targetArn"].endswith(":stream/log-stream")
    mod.reset()


# ── _metric_filters ────────────────────────────────────────────────────

def test_metric_filters_survive_warm_boot():
    mod = _module()
    mod.reset()
    # Create the parent log group first — _put_metric_filter would normally
    # require it; we mirror that pre-condition for realism.
    mod._log_groups["/aws/lambda/foo"] = {
        "arn": "arn:aws:logs:us-east-1:000000000000:log-group:/aws/lambda/foo:*",
        "creationTime": 1700000000000,
        "retentionInDays": None,
        "tags": {},
        "subscriptionFilters": {},
        "streams": {},
    }
    mod._metric_filters[("/aws/lambda/foo", "ErrorCount")] = {
        "filterName": "ErrorCount",
        "logGroupName": "/aws/lambda/foo",
        "filterPattern": "ERROR",
        "metricTransformations": [{
            "metricName": "Errors",
            "metricNamespace": "Lambda",
            "metricValue": "1",
        }],
        "creationTime": 1700000000000,
    }

    _round_trip(mod)

    assert ("/aws/lambda/foo", "ErrorCount") in mod._metric_filters, (
        "Metric filter lost across get_state → restore_state — "
        "_metric_filters must be in both. Tuple keys are round-tripped "
        "by AccountRegionScopedDict's JSON encoder hook."
    )
    mod.reset()


def test_legacy_metric_filters_restore_to_log_group_region():
    from ministack.core.responses import AccountScopedDict, get_region, set_request_region

    mod = _module()
    mod.reset()
    group = f"/aws/lambda/legacy-filter-{_uuid_mod.uuid4().hex[:8]}"
    original_region = get_region()

    legacy_metric_filters = AccountScopedDict()
    legacy_metric_filters[(group, "ErrorCount")] = {
        "filterName": "ErrorCount",
        "logGroupName": group,
        "filterPattern": "ERROR",
        "metricTransformations": [{
            "metricName": "Errors",
            "metricNamespace": "Lambda",
            "metricValue": "1",
        }],
        "creationTime": 1700000000000,
    }

    try:
        set_request_region("us-east-1")
        mod.restore_state({
            "log_groups": {
                group: {
                    "arn": f"arn:aws:logs:us-west-2:000000000000:log-group:{group}:*",
                    "creationTime": 1700000000000,
                    "retentionInDays": None,
                    "tags": {},
                    "subscriptionFilters": {},
                    "streams": {},
                },
            },
            "metric_filters": legacy_metric_filters,
        })

        set_request_region("us-west-2")
        assert (group, "ErrorCount") in mod._metric_filters
        set_request_region("us-east-1")
        assert (group, "ErrorCount") not in mod._metric_filters
    finally:
        set_request_region(original_region)
        mod.reset()


# ── _queries ───────────────────────────────────────────────────────────

def test_queries_survive_warm_boot():
    mod = _module()
    mod.reset()
    mod._queries["q-12345"] = {
        "queryId": "q-12345",
        "logGroupName": "/aws/lambda/foo",
        "startTime": 1700000000,
        "endTime": 1700001000,
        "queryString": "fields @timestamp, @message | limit 20",
        "status": "Complete",
    }

    _round_trip(mod)

    assert "q-12345" in mod._queries, (
        "CloudWatch Logs Insights query lost across get_state → "
        "restore_state — _queries must be in both."
    )
    mod.reset()


# ── subscription-filter ↔ destination consistency ──────────────────────

def test_subscription_filter_destination_resolvable_after_warm_boot():
    """A subscription filter on a log group references a destination ARN.
    The filter lives inside _log_groups (persisted), the destination lives
    in _destinations (was NOT persisted). After warm-boot the filter
    pointed at a vanished destination — split-brain. With _destinations
    persistence, the destination must still resolve."""
    mod = _module()
    mod.reset()

    dest_arn = "arn:aws:logs:us-east-1:000000000000:destination:cross-account"
    mod._destinations["cross-account"] = {
        "destinationName": "cross-account",
        "targetArn": "arn:aws:kinesis:us-east-1:222222222222:stream/audit",
        "roleArn": "arn:aws:iam::000000000000:role/CWLtoKinesis",
        "accessPolicy": "",
        "arn": dest_arn,
        "creationTime": 1700000000000,
    }
    mod._log_groups["/aws/lambda/audited"] = {
        "arn": "arn:aws:logs:us-east-1:000000000000:log-group:/aws/lambda/audited:*",
        "creationTime": 1700000000000,
        "retentionInDays": None,
        "tags": {},
        "subscriptionFilters": {
            "to-cross-account": {
                "filterName": "to-cross-account",
                "logGroupName": "/aws/lambda/audited",
                "filterPattern": "",
                "destinationArn": dest_arn,
                "roleArn": "",
                "distribution": "ByLogStream",
                "creationTime": 1700000000000,
            },
        },
        "streams": {},
    }

    _round_trip(mod)

    # The log-group side already round-tripped on main; what was missing
    # is the destination it references.
    assert "/aws/lambda/audited" in mod._log_groups
    sub_filter = mod._log_groups["/aws/lambda/audited"]["subscriptionFilters"]["to-cross-account"]
    referenced_arn = sub_filter["destinationArn"]

    # Find the destination that ought to back this ARN.
    matching = [d for d in mod._destinations.values() if d.get("arn") == referenced_arn]
    assert matching, (
        "Subscription filter references a destination ARN that no "
        "longer exists in _destinations after warm-boot — split-brain "
        "state. _destinations must be persisted alongside _log_groups."
    )
    mod.reset()


def test_logs_describe_streams_on_nonexistent_group_carries_errortype(logs):
    """Real AWS sends `x-amzn-errortype` on JSON-protocol errors. Java/Go SDK
    v2 read it; without it they raise SdkClientException(unknown error type)."""
    with pytest.raises(ClientError) as exc:
        logs.describe_log_streams(logGroupName="missing-lg")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "ResourceNotFoundException"
