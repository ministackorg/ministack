import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


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
    with pytest.raises(ClientError) as exc:
        logs.put_delivery_destination(
            name=f"intg-bad-target-{uid}",
            deliveryDestinationConfiguration={
                "destinationResourceArn": f"arn:aws:lambda:us-east-1:000000000000:function:anything-{uid}",
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
