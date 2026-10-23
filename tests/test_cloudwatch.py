import html
import json
import re
import time
import uuid as _uuid_mod

import pytest
from botocore.exceptions import ClientError
from conftest import sqs_policy_allow_sns


def test_cloudwatch_metrics(cw):
    cw.put_metric_data(
        Namespace="MyApp",
        MetricData=[
            {"MetricName": "RequestCount", "Value": 42.0, "Unit": "Count"},
            {"MetricName": "Latency", "Value": 123.5, "Unit": "Milliseconds"},
        ],
    )
    resp = cw.list_metrics(Namespace="MyApp")
    names = [m["MetricName"] for m in resp["Metrics"]]
    assert "RequestCount" in names
    assert "Latency" in names

def test_cloudwatch_alarm(cw):
    cw.put_metric_alarm(
        AlarmName="high-latency",
        MetricName="Latency",
        Namespace="MyApp",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=500.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    resp = cw.describe_alarms(AlarmNames=["high-latency"])
    assert len(resp["MetricAlarms"]) == 1

def test_cloudwatch_logs_metric_filter(logs):
    logs.create_log_group(logGroupName="/test/mf")
    logs.put_metric_filter(
        logGroupName="/test/mf",
        filterName="err-count",
        filterPattern="ERROR",
        metricTransformations=[{"metricName": "ErrorCount", "metricNamespace": "Test", "metricValue": "1"}],
    )
    resp = logs.describe_metric_filters(logGroupName="/test/mf")
    assert len(resp["metricFilters"]) == 1
    assert resp["metricFilters"][0]["filterName"] == "err-count"
    logs.delete_metric_filter(logGroupName="/test/mf", filterName="err-count")
    resp2 = logs.describe_metric_filters(logGroupName="/test/mf")
    assert len(resp2["metricFilters"]) == 0

def test_cloudwatch_logs_insights_stub(logs):
    logs.create_log_group(logGroupName="/test/insights")
    resp = logs.start_query(
        logGroupName="/test/insights",
        startTime=0,
        endTime=9999999999,
        queryString="fields @timestamp | limit 10",
    )
    query_id = resp["queryId"]
    assert query_id
    results = logs.get_query_results(queryId=query_id)
    assert results["status"] in ("Complete", "Running")

def test_cloudwatch_dashboard(cw):
    body = json.dumps({"widgets": [{"type": "text", "properties": {"markdown": "Hello"}}]})
    cw.put_dashboard(DashboardName="test-dash", DashboardBody=body)
    resp = cw.get_dashboard(DashboardName="test-dash")
    assert resp["DashboardName"] == "test-dash"
    assert "DashboardBody" in resp
    listed = cw.list_dashboards()
    assert any(d["DashboardName"] == "test-dash" for d in listed["DashboardEntries"])
    cw.delete_dashboards(DashboardNames=["test-dash"])

# Migrated from test_cw.py
def test_cloudwatch_put_list_metrics_v2(cw):
    cw.put_metric_data(
        Namespace="CWv2",
        MetricData=[
            {
                "MetricName": "Reqs",
                "Value": 100.0,
                "Unit": "Count",
                "Dimensions": [{"Name": "API", "Value": "/users"}],
            },
            {"MetricName": "Errs", "Value": 5.0, "Unit": "Count"},
        ],
    )
    resp = cw.list_metrics(Namespace="CWv2")
    names = [m["MetricName"] for m in resp["Metrics"]]
    assert "Reqs" in names
    assert "Errs" in names

    resp_filtered = cw.list_metrics(Namespace="CWv2", MetricName="Reqs")
    assert all(m["MetricName"] == "Reqs" for m in resp_filtered["Metrics"])

def test_cloudwatch_get_metric_statistics_v2(cw):
    cw.put_metric_data(
        Namespace="CWStat2",
        MetricData=[
            {"MetricName": "Duration", "Value": 100.0, "Unit": "Milliseconds"},
            {"MetricName": "Duration", "Value": 200.0, "Unit": "Milliseconds"},
        ],
    )
    resp = cw.get_metric_statistics(
        Namespace="CWStat2",
        MetricName="Duration",
        Period=60,
        StartTime=time.time() - 600,
        EndTime=time.time() + 600,
        Statistics=["Average", "Sum", "SampleCount", "Minimum", "Maximum"],
    )
    assert len(resp["Datapoints"]) >= 1
    dp = resp["Datapoints"][0]
    assert "Average" in dp
    assert "Sum" in dp
    assert "SampleCount" in dp
    assert "Minimum" in dp
    assert "Maximum" in dp

def test_cloudwatch_put_metric_alarm_v2(cw):
    cw.put_metric_alarm(
        AlarmName="cw-v2-high-err",
        MetricName="Errors",
        Namespace="CWv2Alarms",
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=2,
        Threshold=10.0,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=["arn:aws:sns:us-east-1:000000000000:alarm-topic"],
        AlarmDescription="Fires when errors >= 10",
    )
    resp = cw.describe_alarms(AlarmNames=["cw-v2-high-err"])
    alarm = resp["MetricAlarms"][0]
    assert alarm["AlarmName"] == "cw-v2-high-err"
    assert alarm["Threshold"] == 10.0
    assert alarm["ComparisonOperator"] == "GreaterThanOrEqualToThreshold"
    assert alarm["EvaluationPeriods"] == 2

def test_cloudwatch_describe_alarms_v2(cw):
    for i in range(3):
        cw.put_metric_alarm(
            AlarmName=f"cw-da-v2-{i}",
            MetricName="M",
            Namespace="N",
            Statistic="Sum",
            Period=60,
            EvaluationPeriods=1,
            Threshold=float(i),
            ComparisonOperator="GreaterThanThreshold",
        )
    resp = cw.describe_alarms(AlarmNamePrefix="cw-da-v2-")
    names = [a["AlarmName"] for a in resp["MetricAlarms"]]
    for i in range(3):
        assert f"cw-da-v2-{i}" in names


def _put_paging_alarm(cw, name):
    cw.put_metric_alarm(
        AlarmName=name, MetricName="M", Namespace="N", Statistic="Sum", Period=60,
        EvaluationPeriods=1, Threshold=1.0, ComparisonOperator="GreaterThanThreshold",
    )


def test_cloudwatch_describe_alarms_pages_both_types_in_name_order(cw):
    prefix = f"cw-page-{_uuid_mod.uuid4().hex[:8]}-"
    metric = [f"{prefix}m-{i}" for i in (3, 0, 4, 1, 2)]
    for name in metric + [f"{prefix}A-upper"]:
        _put_paging_alarm(cw, name)
    for name in (f"{prefix}c-1", f"{prefix}z-2"):
        cw.put_composite_alarm(AlarmName=name, AlarmRule=f'ALARM("{metric[0]}")')
    try:
        pages, token = [], None
        while True:
            kwargs = {"NextToken": token} if token else {}
            resp = cw.describe_alarms(AlarmNamePrefix=prefix, AlarmTypes=["MetricAlarm", "CompositeAlarm"],
                                      MaxRecords=3, **kwargs)
            pages.append(([a["AlarmName"][len(prefix):] for a in resp["MetricAlarms"]],
                          [a["AlarmName"][len(prefix):] for a in resp["CompositeAlarms"]]))
            token = resp.get("NextToken")
            if not token:
                break
        # One byte-ordered listing across both types; MaxRecords counts both.
        assert pages == [
            (["A-upper", "m-0"], ["c-1"]),
            (["m-1", "m-2", "m-3"], []),
            (["m-4"], ["z-2"]),
        ]

        paginated = cw.get_paginator("describe_alarms").paginate(
            AlarmNamePrefix=prefix, AlarmTypes=["MetricAlarm", "CompositeAlarm"], PaginationConfig={"PageSize": 2})
        assert len(paginated.build_full_result()["MetricAlarms"]) == 6
    finally:
        for name in (f"{prefix}c-1", f"{prefix}z-2"):
            cw.delete_alarms(AlarmNames=[name])
        cw.delete_alarms(AlarmNames=metric + [f"{prefix}A-upper"])


def test_cloudwatch_describe_alarms_next_token_survives_changes_between_pages(cw):
    prefix = f"cw-page-chg-{_uuid_mod.uuid4().hex[:8]}-"
    names = [f"{prefix}m-{i}" for i in range(6)]
    for name in names:
        _put_paging_alarm(cw, name)
    try:
        first = cw.describe_alarms(AlarmNamePrefix=prefix, MaxRecords=2)
        assert [a["AlarmName"] for a in first["MetricAlarms"]] == names[:2]
        assert first["NextToken"] == names[2]
        # Delete the alarm the token names and the page before it, add one alarm on each side of the token.
        cw.delete_alarms(AlarmNames=names[:3])
        _put_paging_alarm(cw, f"{prefix}m-1a")
        _put_paging_alarm(cw, f"{prefix}m-2a")
        second = cw.describe_alarms(AlarmNamePrefix=prefix, MaxRecords=2, NextToken=first["NextToken"])
        assert [a["AlarmName"] for a in second["MetricAlarms"]] == [f"{prefix}m-2a", names[3]]
        third = cw.describe_alarms(AlarmNamePrefix=prefix, MaxRecords=2, NextToken=second["NextToken"])
        assert [a["AlarmName"] for a in third["MetricAlarms"]] == names[4:6]
        assert "NextToken" not in third
    finally:
        cw.delete_alarms(AlarmNames=names + [f"{prefix}m-1a", f"{prefix}m-2a"])


def test_cloudwatch_describe_alarms_defaults_to_50_metric_alarms(cw):
    prefix = f"cw-page-def-{_uuid_mod.uuid4().hex[:8]}-"
    names = [f"{prefix}{i:02d}" for i in range(52)]
    for name in names:
        _put_paging_alarm(cw, name)
    cw.put_composite_alarm(AlarmName=f"{prefix}comp", AlarmRule=f'ALARM("{names[0]}")')
    try:
        first = cw.describe_alarms(AlarmNamePrefix=prefix)
        assert [a["AlarmName"] for a in first["MetricAlarms"]] == names[:50]
        assert first["CompositeAlarms"] == []
        second = cw.describe_alarms(AlarmNamePrefix=prefix, NextToken=first["NextToken"])
        assert [a["AlarmName"] for a in second["MetricAlarms"]] == names[50:]
        assert "NextToken" not in second
    finally:
        cw.delete_alarms(AlarmNames=[f"{prefix}comp"])
        cw.delete_alarms(AlarmNames=names)


def test_cloudwatch_describe_alarms_max_records_bounds_and_any_token():
    from conftest import make_client

    raw = make_client("cloudwatch", {"parameter_validation": False})
    for value, bound in ((0, "greater than or equal to 1"), (101, "less than or equal to 100")):
        with pytest.raises(ClientError) as exc:
            raw.describe_alarms(MaxRecords=value)
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert exc.value.response["Error"]["Message"] == (
            f"1 validation error detected: Value '{value}' at 'maxRecords' failed to satisfy constraint: "
            f"Member must have value {bound}"
        )
    prefix = f"cw-page-tok-{_uuid_mod.uuid4().hex[:8]}-"
    names = [f"{prefix}{i}" for i in range(2)]
    for name in names:
        _put_paging_alarm(raw, name)
    try:
        # Any string is a name to start from: one after every name gives an empty page, one before them the first.
        resp = raw.describe_alarms(AlarmNamePrefix=prefix, NextToken="zzzz")
        assert resp["MetricAlarms"] == [] and "NextToken" not in resp
        resp = raw.describe_alarms(AlarmNamePrefix=prefix, NextToken="aaaa")
        assert [a["AlarmName"] for a in resp["MetricAlarms"]] == names
    finally:
        raw.delete_alarms(AlarmNames=names)

def test_cloudwatch_delete_alarms_v2(cw):
    cw.put_metric_alarm(
        AlarmName="cw-del-v2",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    cw.delete_alarms(AlarmNames=["cw-del-v2"])
    resp = cw.describe_alarms(AlarmNames=["cw-del-v2"])
    assert len(resp["MetricAlarms"]) == 0

def test_cloudwatch_set_alarm_state_v2(cw):
    cw.put_metric_alarm(
        AlarmName="cw-state-v2",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    initial = cw.describe_alarms(AlarmNames=["cw-state-v2"])["MetricAlarms"][0]
    assert initial["StateValue"] == "INSUFFICIENT_DATA"

    cw.set_alarm_state(
        AlarmName="cw-state-v2",
        StateValue="ALARM",
        StateReason="Manual trigger for testing",
    )
    after = cw.describe_alarms(AlarmNames=["cw-state-v2"])["MetricAlarms"][0]
    assert after["StateValue"] == "ALARM"
    assert after["StateReason"] == "Manual trigger for testing"

def test_cloudwatch_put_metric_alarm_keeps_state_of_existing_alarm(cw):
    """PutMetricAlarm on an existing alarm: "its state is left unchanged, but
    the update completely overwrites the previous configuration of the alarm"
    (https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricAlarm.html).
    The state timestamp is part of the state: only the configuration timestamp
    moves. Goes over the wire, which botocore sends as smithy-rpc-v2-cbor."""
    name = f"cw-keep-state-{_uuid_mod.uuid4().hex[:8]}"

    def put(threshold):
        cw.put_metric_alarm(
            AlarmName=name,
            MetricName="Errors",
            Namespace=f"CwKeepState/{name}",
            Statistic="Sum",
            Period=60,
            EvaluationPeriods=1,
            Threshold=threshold,
            ComparisonOperator="GreaterThanThreshold",
        )

    put(1.0)
    try:
        cw.set_alarm_state(AlarmName=name, StateValue="ALARM", StateReason="seeded")
        seeded = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]
        time.sleep(1.1)  # timestamps are whole seconds; a reset must be visible

        put(2.0)
        after = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]
        assert after["Threshold"] == 2.0
        assert after["StateValue"] == "ALARM"
        assert after["StateReason"] == "seeded"
        assert after["StateUpdatedTimestamp"] == seeded["StateUpdatedTimestamp"]
        assert after["AlarmConfigurationUpdatedTimestamp"] > seeded["AlarmConfigurationUpdatedTimestamp"]
    finally:
        cw.delete_alarms(AlarmNames=[name])


def test_cloudwatch_put_metric_alarm_query_keeps_state_of_existing_alarm():
    """The query-protocol branch of PutMetricAlarm (what the AWS CLI v1 and
    older SDKs send) keeps the state of an existing alarm the same way:
    "its state is left unchanged" per the PutMetricAlarm reference."""
    from ministack.core import responses as _resp
    from ministack.services import cloudwatch as _cw

    acct_tok = _resp._request_account_id.set("000000000000")
    region_tok = _resp._request_region.set("us-east-1")
    name = f"query-keep-state-{_uuid_mod.uuid4().hex[:8]}"

    def put(threshold):
        status, _headers, _body = _cw._put_metric_alarm({
            "AlarmName": [name],
            "MetricName": ["Errors"],
            "Namespace": [f"QueryKeepState/{name}"],
            "Statistic": ["Sum"],
            "Period": ["60"],
            "EvaluationPeriods": ["1"],
            "Threshold": [threshold],
            "ComparisonOperator": ["GreaterThanThreshold"],
        }, {}, is_cbor=False)
        assert status == 200

    try:
        put("1")
        _cw._alarms[name]["StateValue"] = "ALARM"
        _cw._alarms[name]["StateReason"] = "seeded"
        _cw._alarms[name]["StateUpdatedTimestamp"] = 1_000_000
        _cw._alarms[name]["AlarmConfigurationUpdatedTimestamp"] = 1_000_000

        put("2")
        alarm = _cw._alarms[name]
        assert alarm["Threshold"] == 2.0
        assert alarm["StateValue"] == "ALARM"
        assert alarm["StateReason"] == "seeded"
        assert alarm["StateUpdatedTimestamp"] == 1_000_000
        assert alarm["AlarmConfigurationUpdatedTimestamp"] > 1_000_000
    finally:
        _cw._alarms.pop_scoped("000000000000", "us-east-1", name, None)
        _resp._request_region.reset(region_tok)
        _resp._request_account_id.reset(acct_tok)


def test_cloudwatch_get_metric_data_v2(cw):
    cw.put_metric_data(
        Namespace="CWData2",
        MetricData=[{"MetricName": "Hits", "Value": 42.0, "Unit": "Count"}],
    )
    resp = cw.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "q1",
                "MetricStat": {
                    "Metric": {"Namespace": "CWData2", "MetricName": "Hits"},
                    "Period": 60,
                    "Stat": "Sum",
                },
                "ReturnData": True,
            }
        ],
        StartTime=time.time() - 600,
        EndTime=time.time() + 600,
    )
    assert len(resp["MetricDataResults"]) == 1
    assert resp["MetricDataResults"][0]["Id"] == "q1"
    assert resp["MetricDataResults"][0]["StatusCode"] == "Complete"
    assert len(resp["MetricDataResults"][0]["Values"]) >= 1

def test_cloudwatch_get_metric_data_extended_percentile(cw):
    """Extended-statistic percentiles (pNN) must be computed from the raw sample
    values via linear interpolation between the two nearest ranks, not silently
    aliased to Average regardless of which percentile is requested.
    Expected values below are hand-computed for the dataset
    [1, 2, 3, 4, 90, 95, 98, 99] (n=8):
      p50: rank=3.5  -> 4 + 0.5*(90-4) = 47.0
      p95: rank=6.65 -> 98 + 0.65*(99-98) = 98.65
      p5:  rank=0.35 -> 1 + 0.35*(2-1) = 1.35
    """
    ns = "Test/GMDPercentile"
    now = time.time()
    for i, v in enumerate([1, 2, 3, 4, 90, 95, 98, 99]):
        cw.put_metric_data(Namespace=ns, MetricData=[{
            "MetricName": "Latency",
            "Dimensions": [{"Name": "Host", "Value": "h1"}],
            "Timestamp": now - i,
            "Value": float(v),
        }])

    def _value(stat):
        resp = cw.get_metric_data(
            MetricDataQueries=[{
                "Id": "q1",
                "MetricStat": {
                    "Metric": {
                        "Namespace": ns, "MetricName": "Latency",
                        "Dimensions": [{"Name": "Host", "Value": "h1"}],
                    },
                    "Period": 3600,
                    "Stat": stat,
                },
                "ReturnData": True,
            }],
            StartTime=now - 3600,
            EndTime=now + 3600,
        )
        return resp["MetricDataResults"][0]["Values"][0]

    assert _value("Average") == pytest.approx(49.0)
    assert _value("p50") == pytest.approx(47.0)
    assert _value("p95") == pytest.approx(98.65)
    assert _value("p5") == pytest.approx(1.35)
    assert _value("p95") != _value("p5")

def test_cloudwatch_get_metric_data_honors_dimensions(cw):
    """GetMetricData resolves by the query's dimension set, like GetMetricStatistics.

    Same namespace + name, two dimension values published; a query for a third,
    unpublished value must return no data rather than leaking another series'.
    """
    ns = "Test/GMDDims"
    cw.put_metric_data(Namespace=ns, MetricData=[
        {"MetricName": "Requests", "Dimensions": [{"Name": "Series", "Value": "A"}], "Value": 100.0},
        {"MetricName": "Requests", "Dimensions": [{"Name": "Series", "Value": "B"}], "Value": 7.0},
    ])

    def _q(qid, series):
        return {"Id": qid, "MetricStat": {"Metric": {
            "Namespace": ns, "MetricName": "Requests",
            "Dimensions": [{"Name": "Series", "Value": series}]},
            "Period": 3600, "Stat": "Maximum"}}

    resp = cw.get_metric_data(
        MetricDataQueries=[_q("a", "A"), _q("b", "B"), _q("c", "C")],
        StartTime=time.time() - 3600, EndTime=time.time() + 3600,
    )
    values = {r["Id"]: r["Values"] for r in resp["MetricDataResults"]}
    assert values["a"] == [100.0]
    assert values["b"] == [7.0]
    assert values["c"] == []  # dimension value never published

def test_cloudwatch_tags_v2(cw):
    cw.put_metric_alarm(
        AlarmName="cw-tag-v2",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    arn = cw.describe_alarms(AlarmNames=["cw-tag-v2"])["MetricAlarms"][0]["AlarmArn"]
    cw.tag_resource(
        ResourceARN=arn,
        Tags=[
            {"Key": "env", "Value": "prod"},
            {"Key": "team", "Value": "sre"},
        ],
    )
    resp = cw.list_tags_for_resource(ResourceARN=arn)
    tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tag_map["env"] == "prod"
    assert tag_map["team"] == "sre"

    cw.untag_resource(ResourceARN=arn, TagKeys=["env"])
    resp2 = cw.list_tags_for_resource(ResourceARN=arn)
    assert not any(t["Key"] == "env" for t in resp2["Tags"])
    assert any(t["Key"] == "team" for t in resp2["Tags"])


def test_cloudwatch_alarms_are_region_isolated(cw):
    """Alarms are region-specific: DescribeAlarms in another region must not
    list an alarm created here (was account-scoped, so it leaked across regions)."""
    import uuid as _uuid

    import boto3
    from conftest import ENDPOINT

    name = f"region-iso-alarm-{_uuid.uuid4().hex[:8]}"
    cw.put_metric_alarm(
        AlarmName=name, MetricName="M", Namespace="N", Statistic="Sum",
        Period=60, EvaluationPeriods=1, Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    west = boto3.client(
        "cloudwatch", endpoint_url=ENDPOINT, region_name="us-west-2",
        aws_access_key_id="test", aws_secret_access_key="test",
    )
    east_names = [a["AlarmName"] for a in cw.describe_alarms(AlarmNames=[name])["MetricAlarms"]]
    west_names = [a["AlarmName"] for a in west.describe_alarms()["MetricAlarms"]]
    assert name in east_names
    assert name not in west_names


def test_cloudwatch_tags_reject_wrong_region_alarm_arn(cw):
    import boto3
    from conftest import ENDPOINT

    cw.put_metric_alarm(
        AlarmName="cw-tag-wrong-region",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    arn = cw.describe_alarms(AlarmNames=["cw-tag-wrong-region"])["MetricAlarms"][0]["AlarmArn"]
    arn_parts = arn.split(":")
    arn_parts[3] = "us-west-2"
    wrong_region_arn = ":".join(arn_parts)
    west_cw = boto3.client(
        "cloudwatch",
        endpoint_url=ENDPOINT,
        region_name="us-west-2",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    with pytest.raises(ClientError) as exc:
        west_cw.tag_resource(ResourceARN=wrong_region_arn, Tags=[{"Key": "env", "Value": "prod"}])

    assert exc.value.response["Error"]["Code"] == "ResourceNotFound"


def test_cloudwatch_composite_alarm(cw):
    import uuid as _uuid

    child = f"intg-child-alarm-{_uuid.uuid4().hex[:8]}"
    composite = f"intg-comp-alarm-{_uuid.uuid4().hex[:8]}"
    cw.put_metric_alarm(
        AlarmName=child,
        ComparisonOperator="GreaterThanThreshold",
        EvaluationPeriods=1,
        MetricName="CPUUtilization",
        Namespace="AWS/EC2",
        Period=60,
        Statistic="Average",
        Threshold=80.0,
    )
    child_arn = cw.describe_alarms(AlarmNames=[child])["MetricAlarms"][0]["AlarmArn"]
    cw.put_composite_alarm(
        AlarmName=composite,
        AlarmRule=f"ALARM({child_arn})",
        AlarmDescription="composite test",
    )
    resp = cw.describe_alarms(AlarmNames=[composite], AlarmTypes=["CompositeAlarm"])
    assert any(a["AlarmName"] == composite for a in resp.get("CompositeAlarms", []))
    cw.delete_alarms(AlarmNames=[composite])
    cw.delete_alarms(AlarmNames=[child])


def _composite(cw, name):
    return cw.describe_alarms(AlarmNames=[name], AlarmTypes=["CompositeAlarm"])["CompositeAlarms"][0]


def _triggering(alarm):
    return [(t["arn"].rsplit(":", 1)[1], t["state"]["value"])
            for t in json.loads(alarm["StateReasonData"])["triggeringAlarms"]]


def _delete_composites_then_children(cw, composites, children):
    for name in composites:
        cw.delete_alarms(AlarmNames=[name])
    cw.delete_alarms(AlarmNames=children)


def test_cloudwatch_composite_alarm_follows_child_states(cw):
    p = f"cw-comp-{_uuid_mod.uuid4().hex[:8]}-"
    a, b = f"{p}a", f"{p}b"
    for name in (a, b):
        _put_paging_alarm(cw, name)
    a_arn = cw.describe_alarms(AlarmNames=[a])["MetricAlarms"][0]["AlarmArn"]
    rules = {
        "and": f'ALARM("{a}") AND ALARM("{b}")',
        "or": f"ALARM({a_arn}) OR ALARM({b})",
        "not": f'NOT ALARM("{a}")',
        "ok": f'OK("{a}")',
        "ins": f'INSUFFICIENT_DATA("{b}")',
        "true": "TRUE",
        "false": "FALSE",
        "mix": f'(ALARM("{a}") OR OK("{b}")) AND NOT INSUFFICIENT_DATA("{b}")',
        "least": f'AT_LEAST(2, ALARM, ("{a}", "{b}"))',
        "pct": f"AT_LEAST(50%, NOT OK, ({a}, {b}))",
        "prec": "TRUE OR TRUE AND FALSE",
        "notprec": "NOT TRUE AND FALSE",
    }
    for key, rule in rules.items():
        cw.put_composite_alarm(AlarmName=f"{p}{key}", AlarmRule=rule)
    cw.put_composite_alarm(AlarmName=f"{p}nest", AlarmRule=f'ALARM("{p}or")')
    names = [f"{p}{key}" for key in rules] + [f"{p}nest"]

    def states():
        resp = cw.describe_alarms(AlarmNames=names, AlarmTypes=["CompositeAlarm"])
        return {x["AlarmName"][len(p):]: x["StateValue"] for x in resp["CompositeAlarms"]}

    try:
        assert states() == {"and": "OK", "or": "OK", "not": "ALARM", "ok": "OK", "ins": "ALARM", "true": "ALARM",
                            "false": "OK", "mix": "OK", "least": "OK", "pct": "ALARM", "prec": "ALARM",
                            "notprec": "OK", "nest": "OK"}
        created = _composite(cw, f"{p}and")
        assert created["StateReason"] == f"{created['AlarmArn']} was created and its alarm rule evaluates to OK"
        assert created["StateUpdatedTimestamp"] == created["StateTransitionedTimestamp"]
        assert sorted(_triggering(created)) == [(a, "INSUFFICIENT_DATA"), (b, "INSUFFICIENT_DATA")]
        assert _triggering(_composite(cw, f"{p}true")) == []

        cw.set_alarm_state(AlarmName=a, StateValue="ALARM", StateReason="test")
        assert states() == {"and": "OK", "or": "ALARM", "not": "OK", "ok": "OK", "ins": "ALARM", "true": "ALARM",
                            "false": "OK", "mix": "OK", "least": "OK", "pct": "ALARM", "prec": "ALARM",
                            "notprec": "OK", "nest": "ALARM"}
        nest = _composite(cw, f"{p}nest")
        assert re.fullmatch(
            rf"{re.escape(nest['AlarmArn'][:-4])}or transitioned to ALARM at "
            r"[A-Z][a-z]+day \d\d [A-Z][a-z]+, \d{4} \d\d:\d\d:\d\d UTC", nest["StateReason"])
        assert _triggering(nest) == [(f"{p}or", "ALARM")]
        timestamp = json.loads(nest["StateReasonData"])["triggeringAlarms"][0]["state"]["timestamp"]
        assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}\+0000", timestamp)

        cw.set_alarm_state(AlarmName=b, StateValue="ALARM", StateReason="test")
        assert states()["and"] == "ALARM" and states()["mix"] == "ALARM" and states()["ins"] == "OK"
        # Both children changed since the last transition of "and".
        assert sorted(_triggering(_composite(cw, f"{p}and"))) == [(a, "ALARM"), (b, "ALARM")]

        cw.set_alarm_state(AlarmName=a, StateValue="OK", StateReason="test")
        assert states() == {"and": "OK", "or": "ALARM", "not": "ALARM", "ok": "ALARM", "ins": "OK", "true": "ALARM",
                            "false": "OK", "mix": "OK", "least": "OK", "pct": "ALARM", "prec": "ALARM",
                            "notprec": "OK", "nest": "ALARM"}
        # Only the child that changed since the last transition is named.
        assert _triggering(_composite(cw, f"{p}mix")) == [(a, "OK")]

        cw.set_alarm_state(AlarmName=b, StateValue="OK", StateReason="test")
        assert states()["or"] == "OK" and states()["nest"] == "OK" and states()["pct"] == "OK"
        assert sorted(_triggering(_composite(cw, f"{p}or"))) == [(a, "OK"), (b, "OK")]
        # A composite whose state did not change keeps its reason.
        assert _composite(cw, f"{p}true")["StateReason"].endswith("was created and its alarm rule evaluates to ALARM")
    finally:
        _delete_composites_then_children(cw, [f"{p}nest"] + names[:-1], [a, b])


def test_cloudwatch_composite_alarm_update_and_set_state(cw):
    p = f"cw-comp-upd-{_uuid_mod.uuid4().hex[:8]}-"
    a, b = f"{p}a", f"{p}b"
    for name in (a, b):
        _put_paging_alarm(cw, name)
    cw.set_alarm_state(AlarmName=a, StateValue="ALARM", StateReason="test")
    comp = f"{p}comp"
    cw.put_composite_alarm(AlarmName=comp, AlarmRule=f'ALARM("{a}") AND ALARM("{b}")')
    try:
        assert _composite(cw, comp)["StateValue"] == "OK"
        created = _composite(cw, comp)

        # An update that leaves the result alone keeps state and reason.
        cw.put_composite_alarm(AlarmName=comp, AlarmRule=f'ALARM("{b}")')
        same = _composite(cw, comp)
        assert (same["StateValue"], same["StateReason"]) == ("OK", created["StateReason"])

        # An update that changes the result moves the state at once, naming the children that agree with it.
        cw.put_composite_alarm(AlarmName=comp, AlarmRule=f'ALARM("{a}") OR ALARM("{b}")')
        updated = _composite(cw, comp)
        assert updated["StateValue"] == "ALARM"
        assert updated["StateReason"] == f"{updated['AlarmArn']} was updated and its new alarm rule evaluates to ALARM"
        assert _triggering(updated) == [(a, "ALARM")]

        # Of the children that changed since the last transition, only those that agree with the new state are named.
        cw.set_alarm_state(AlarmName=a, StateValue="OK", StateReason="test")
        cw.set_alarm_state(AlarmName=b, StateValue="OK", StateReason="test")
        cw.set_alarm_state(AlarmName=a, StateValue="ALARM", StateReason="test")
        assert _triggering(_composite(cw, comp)) == [(a, "ALARM")]

        # SetAlarmState holds until a child changes state.
        cw.set_alarm_state(AlarmName=comp, StateValue="OK", StateReason="manual")
        manual = _composite(cw, comp)
        assert (manual["StateValue"], manual["StateReason"]) == ("OK", "manual")
        assert "StateReasonData" not in manual
        cw.set_alarm_state(AlarmName=b, StateValue="ALARM", StateReason="test")
        back = _composite(cw, comp)
        assert back["StateValue"] == "ALARM"
        assert back["StateReason"].startswith(f"{back['AlarmArn'][:-4]}b transitioned to ALARM at ")
        assert _triggering(back) == [(b, "ALARM")]
    finally:
        _delete_composites_then_children(cw, [comp], [a, b])


def test_cloudwatch_put_composite_alarm_validates_rule(cw):
    p = f"cw-comp-val-{_uuid_mod.uuid4().hex[:8]}-"
    a = f"{p}a"
    _put_paging_alarm(cw, a)
    try:
        cases = {
            f'ALARM("{p}missing")': (
                "Could not save the composite alarm as alarms "
                f"[arn:aws:cloudwatch:us-east-1:000000000000:alarm:{p}missing] in the alarm rule do not exist"),
            f'ALARM("{a}") AND': (
                f"Error in AlarmRule [Unsupported token '<EOF>' at char {len(a) + 13}, must be: '(', 'NOT', "
                "AT_LEAST, TRUE or FALSE, ALARM, OK, or INSUFFICIENT_DATA]"),
            f'ALARM("{a}"': f"Error in AlarmRule [Unsupported token '<EOF>' at char {len(a) + 8}]",
            f'ALARM("{a}") XOR TRUE': (
                f"Error in AlarmRule [Unsupported token 'XOR' at char {len(a) + 10}, must be: <EOF>, 'AND', 'OR']"),
            "false": (
                "Error in AlarmRule [Unsupported token 'false' at char 0, must be: '(', 'NOT', AT_LEAST, "
                "TRUE or FALSE, ALARM, OK, or INSUFFICIENT_DATA]"),
        }
        for rule, message in cases.items():
            with pytest.raises(ClientError) as exc:
                cw.put_composite_alarm(AlarmName=f"{p}bad", AlarmRule=rule)
            assert exc.value.response["Error"]["Code"] == "ValidationError"
            assert exc.value.response["Error"]["Message"] == message
        with pytest.raises(ClientError) as exc:
            cw.put_composite_alarm(AlarmName=f"{p}bad", AlarmRule="(" * 2000 + "TRUE" + ")" * 2000)
        assert exc.value.response["Error"]["Code"] == "ValidationError"
        assert cw.describe_alarms(AlarmNames=[f"{p}bad"], AlarmTypes=["CompositeAlarm"])["CompositeAlarms"] == []
        # A long flat chain within the 10240-character limit is not nesting.
        cw.put_composite_alarm(AlarmName=f"{p}chain", AlarmRule=" AND ".join(["TRUE"] * 1100))
        cw.delete_alarms(AlarmNames=[f"{p}chain"])
    finally:
        cw.delete_alarms(AlarmNames=[a])


def test_cloudwatch_delete_alarms_refuses_children_and_several_composites(cw):
    p = f"cw-comp-del-{_uuid_mod.uuid4().hex[:8]}-"
    a = f"{p}a"
    _put_paging_alarm(cw, a)
    cw.put_composite_alarm(AlarmName=f"{p}c1", AlarmRule=f'ALARM("{a}")')
    cw.put_composite_alarm(AlarmName=f"{p}c2", AlarmRule=f'ALARM("{p}c1")')
    cw.put_composite_alarm(AlarmName=f"{p}c3", AlarmRule="TRUE")
    refused = {
        (a,): f"Cannot delete {a} as there are composite alarm(s) depending on it.",
        (a, f"{p}c1"): f"Cannot delete {a} as there are composite alarm(s) depending on it.",
        (f"{p}c1",): f"Cannot delete {p}c1 as there are composite alarm(s) depending on it.",
        (f"{p}c2", f"{p}c3"): (
            "Cannot delete [2] Composite Alarms as only one composite alarm delete is allowed in single request"),
    }
    try:
        for names, message in refused.items():
            with pytest.raises(ClientError) as exc:
                cw.delete_alarms(AlarmNames=list(names))
            assert exc.value.response["Error"]["Code"] == "ValidationError"
            assert exc.value.response["Error"]["Message"] == message
        assert len(cw.describe_alarms(AlarmNamePrefix=p, AlarmTypes=["CompositeAlarm"])["CompositeAlarms"]) == 3
        assert cw.describe_alarms(AlarmNames=[a])["MetricAlarms"]
        cw.delete_alarms(AlarmNames=[f"{p}missing", f"{p}c3"])
    finally:
        _delete_composites_then_children(cw, [f"{p}c2", f"{p}c1", f"{p}c3"], [a])
    left = cw.describe_alarms(AlarmNamePrefix=p, AlarmTypes=["MetricAlarm", "CompositeAlarm"])
    assert left["MetricAlarms"] == left["CompositeAlarms"] == []


def test_cloudwatch_composite_alarm_history_needs_alarm_types(cw):
    p = f"cw-comp-hist-{_uuid_mod.uuid4().hex[:8]}-"
    a, comp = f"{p}a", f"{p}comp"
    _put_paging_alarm(cw, a)
    cw.put_composite_alarm(AlarmName=comp, AlarmRule=f'ALARM("{a}")')
    cw.set_alarm_state(AlarmName=a, StateValue="ALARM", StateReason="test")
    try:
        assert cw.describe_alarm_history(AlarmName=comp)["AlarmHistoryItems"] == []
        items = cw.describe_alarm_history(AlarmName=comp, AlarmTypes=["CompositeAlarm"],
                                          ScanBy="TimestampAscending")["AlarmHistoryItems"]
        assert [(i["AlarmType"], i["HistorySummary"]) for i in items] == [
            ("CompositeAlarm", "Alarm updated from INSUFFICIENT_DATA to OK"),
            ("CompositeAlarm", "Alarm updated from OK to ALARM"),
        ]
        new_state = json.loads(items[1]["HistoryData"])["newState"]
        assert new_state["stateReasonData"]["triggeringAlarms"][0]["state"]["value"] == "ALARM"
        assert [i["AlarmType"] for i in cw.describe_alarm_history(AlarmName=a)["AlarmHistoryItems"]] == [
            "MetricAlarm", "MetricAlarm"]
    finally:
        _delete_composites_then_children(cw, [comp], [a])


def test_cloudwatch_composite_alarm_triggers_survive_persistence():
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import cloudwatch as service

    set_request_account_id("111111111111")
    set_request_region("us-east-1")
    service.reset()
    try:
        for name in ("a", "b"):
            service._put_metric_alarm({}, {"AlarmName": name, "MetricName": "M", "Namespace": "N", "Period": 60,
                                          "EvaluationPeriods": 1, "Threshold": 1.0, "Statistic": "Sum",
                                          "ComparisonOperator": "GreaterThanThreshold"}, False, True)
        service._put_composite_alarm({}, {"AlarmName": "comp", "AlarmRule": "ALARM(a) AND ALARM(b)"}, False, True)
        service._set_alarm_state({}, {"AlarmName": "b", "StateValue": "ALARM", "StateReason": "t"}, False, True)
        state = service.get_state()
        service.reset()
        service.load_persisted_state(state)
        service._set_alarm_state({}, {"AlarmName": "a", "StateValue": "ALARM", "StateReason": "t"}, False, True)
        comp = service._composite_alarms["comp"]
        assert comp["StateValue"] == "ALARM"
        assert [t["arn"].rsplit(":", 1)[1] for t in json.loads(comp["StateReasonData"])["triggeringAlarms"]] == [
            "a", "b"]
    finally:
        service.reset()

def test_cloudwatch_describe_alarms_for_metric(cw):
    import uuid as _uuid

    alarm_name = f"intg-afm-{_uuid.uuid4().hex[:8]}"
    cw.put_metric_alarm(
        AlarmName=alarm_name,
        ComparisonOperator="GreaterThanThreshold",
        EvaluationPeriods=1,
        MetricName="NetworkIn",
        Namespace="AWS/EC2",
        Period=60,
        Statistic="Sum",
        Threshold=1000.0,
    )
    resp = cw.describe_alarms_for_metric(
        MetricName="NetworkIn",
        Namespace="AWS/EC2",
    )
    assert any(a["AlarmName"] == alarm_name for a in resp.get("MetricAlarms", []))
    cw.delete_alarms(AlarmNames=[alarm_name])

def test_cloudwatch_describe_alarm_history(cw):
    import uuid as _uuid

    alarm_name = f"intg-hist-{_uuid.uuid4().hex[:8]}"
    cw.put_metric_alarm(
        AlarmName=alarm_name,
        ComparisonOperator="GreaterThanThreshold",
        EvaluationPeriods=1,
        MetricName="DiskReadOps",
        Namespace="AWS/EC2",
        Period=60,
        Statistic="Average",
        Threshold=50.0,
    )
    cw.set_alarm_state(AlarmName=alarm_name, StateValue="ALARM", StateReason="test")
    resp = cw.describe_alarm_history(AlarmName=alarm_name)
    assert "AlarmHistoryItems" in resp
    cw.delete_alarms(AlarmNames=[alarm_name])


def test_cloudwatch_describe_alarm_history_pages_newest_first(cw):
    alarm_name = f"intg-hist-page-{_uuid_mod.uuid4().hex[:8]}"
    _put_paging_alarm(cw, alarm_name)
    for state in ("ALARM", "OK", "ALARM"):
        cw.set_alarm_state(AlarmName=alarm_name, StateValue=state, StateReason="paging")
    try:
        query = {"AlarmName": alarm_name, "HistoryItemType": "StateUpdate"}
        everything = cw.describe_alarm_history(**query)["AlarmHistoryItems"]
        assert [h["HistorySummary"] for h in everything[:3]] == [
            "Alarm updated from OK to ALARM",
            "Alarm updated from ALARM to OK",
            "Alarm updated from INSUFFICIENT_DATA to ALARM",
        ]
        ascending = cw.describe_alarm_history(ScanBy="TimestampAscending", **query)["AlarmHistoryItems"]
        assert ascending == everything[::-1]

        first = cw.describe_alarm_history(MaxRecords=2, **query)
        assert first["AlarmHistoryItems"] == everything[:2]
        # A state change between pages adds a newer entry; the scan goes on where it stopped.
        cw.set_alarm_state(AlarmName=alarm_name, StateValue="OK", StateReason="paging")
        second = cw.describe_alarm_history(MaxRecords=2, NextToken=first["NextToken"], **query)
        assert second["AlarmHistoryItems"] == everything[2:4]
        everything = cw.describe_alarm_history(**query)["AlarmHistoryItems"]
        paginated = cw.get_paginator("describe_alarm_history").paginate(
            PaginationConfig={"PageSize": 2}, **query)
        assert paginated.build_full_result()["AlarmHistoryItems"] == everything
    finally:
        cw.delete_alarms(AlarmNames=[alarm_name])


def test_cloudwatch_describe_alarm_history_max_records_bounds_and_unknown_token():
    from conftest import make_client

    raw = make_client("cloudwatch", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        raw.describe_alarm_history(MaxRecords=101)
    assert exc.value.response["Error"]["Code"] == "ValidationError"
    assert "Member must have value less than or equal to 100" in exc.value.response["Error"]["Message"]
    with pytest.raises(ClientError) as exc:
        raw.describe_alarm_history(NextToken="bogus-token")
    assert exc.value.response["Error"]["Code"] == "InvalidNextToken"
    assert exc.value.response["Error"]["Message"] == "Invalid NextToken : bogus-token"

def test_cloudwatch_get_metric_data_time_range(cw):
    """GetMetricData respects StartTime/EndTime filtering."""
    import datetime

    now = datetime.datetime.utcnow()
    past = now - datetime.timedelta(hours=2)
    cw.put_metric_data(
        Namespace="qa/cw",
        MetricData=[{"MetricName": "Requests", "Value": 100.0, "Unit": "Count"}],
    )
    resp = cw.get_metric_data(
        MetricDataQueries=[
            {
                "Id": "m1",
                "MetricStat": {
                    "Metric": {"Namespace": "qa/cw", "MetricName": "Requests"},
                    "Period": 60,
                    "Stat": "Sum",
                },
            }
        ],
        StartTime=past,
        EndTime=now + datetime.timedelta(minutes=5),
    )
    result = next((r for r in resp["MetricDataResults"] if r["Id"] == "m1"), None)
    assert result is not None
    assert result["StatusCode"] == "Complete"
    assert len(result["Values"]) >= 1
    assert sum(result["Values"]) >= 100.0

def test_cloudwatch_alarm_state_transitions(cw):
    """SetAlarmState changes alarm state correctly."""
    cw.put_metric_alarm(
        AlarmName="qa-cw-state-alarm",
        MetricName="Errors",
        Namespace="qa/cw",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=10.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    cw.set_alarm_state(AlarmName="qa-cw-state-alarm", StateValue="ALARM", StateReason="Testing")
    alarms = cw.describe_alarms(AlarmNames=["qa-cw-state-alarm"])["MetricAlarms"]
    assert alarms[0]["StateValue"] == "ALARM"
    cw.set_alarm_state(AlarmName="qa-cw-state-alarm", StateValue="OK", StateReason="Resolved")
    alarms2 = cw.describe_alarms(AlarmNames=["qa-cw-state-alarm"])["MetricAlarms"]
    assert alarms2[0]["StateValue"] == "OK"

def test_cloudwatch_list_metrics_namespace_filter(cw):
    """ListMetrics with Namespace filter returns only matching metrics."""
    cw.put_metric_data(Namespace="qa/ns-a", MetricData=[{"MetricName": "MetA", "Value": 1.0}])
    cw.put_metric_data(Namespace="qa/ns-b", MetricData=[{"MetricName": "MetB", "Value": 1.0}])
    resp = cw.list_metrics(Namespace="qa/ns-a")
    names = [m["MetricName"] for m in resp["Metrics"]]
    assert "MetA" in names
    assert "MetB" not in names

def test_cloudwatch_put_metric_data_statistics_values(cw):
    """PutMetricData with Values/Counts array stores multiple data points."""
    cw.put_metric_data(
        Namespace="qa/cw-multi",
        MetricData=[
            {
                "MetricName": "Latency",
                "Values": [10.0, 20.0, 30.0],
                "Counts": [1.0, 2.0, 1.0],
                "Unit": "Milliseconds",
            }
        ],
    )
    resp = cw.list_metrics(Namespace="qa/cw-multi")
    assert any(m["MetricName"] == "Latency" for m in resp["Metrics"])


def test_cloudwatch_enable_alarm_actions(cw):
    cw.put_metric_alarm(
        AlarmName="heimdall-enable-actions",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
        ActionsEnabled=False,
    )
    alarm = cw.describe_alarms(AlarmNames=["heimdall-enable-actions"])["MetricAlarms"][0]
    assert alarm["ActionsEnabled"] is False

    cw.enable_alarm_actions(AlarmNames=["heimdall-enable-actions"])
    alarm = cw.describe_alarms(AlarmNames=["heimdall-enable-actions"])["MetricAlarms"][0]
    assert alarm["ActionsEnabled"] is True
    cw.delete_alarms(AlarmNames=["heimdall-enable-actions"])


def test_cloudwatch_alarm_actions_publish_to_sns(cw, sns, sqs):
    """AlarmActions/OKActions: state transition fans out to the SNS topic.

    Subscribes an SQS queue to a topic, sets that topic ARN as the alarm's
    AlarmActions + OKActions, flips state ALARM → OK, and asserts the SQS
    queue received both notifications with the AWS-shaped JSON payload.
    """
    topic_arn = sns.create_topic(Name="cw-alarm-actions-topic")["TopicArn"]
    queue_url = sqs.create_queue(QueueName="cw-alarm-actions-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(sqs_policy_allow_sns(queue_arn, topic_arn))})
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

    cw.put_metric_alarm(
        AlarmName="cw-actions-fanout",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
        ActionsEnabled=True,
        AlarmActions=[topic_arn],
        OKActions=[topic_arn],
    )

    cw.set_alarm_state(
        AlarmName="cw-actions-fanout",
        StateValue="ALARM",
        StateReason="forced",
    )
    cw.set_alarm_state(
        AlarmName="cw-actions-fanout",
        StateValue="OK",
        StateReason="recovered",
    )

    seen_states = set()
    deadline = time.time() + 5
    while time.time() < deadline and len(seen_states) < 2:
        msgs = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
        ).get("Messages", [])
        for m in msgs:
            envelope = json.loads(m["Body"])
            payload = json.loads(envelope["Message"])
            seen_states.add(payload["NewStateValue"])
            assert payload["AlarmName"] == "cw-actions-fanout"
            assert payload["Trigger"]["MetricName"] == "M"
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
    assert seen_states == {"ALARM", "OK"}, f"got states: {seen_states}"
    cw.delete_alarms(AlarmNames=["cw-actions-fanout"])


def test_cloudwatch_alarm_actions_disabled_does_not_publish(cw, sns, sqs):
    """ActionsEnabled=False suppresses dispatch even on state transition."""
    topic_arn = sns.create_topic(Name="cw-alarm-disabled-topic")["TopicArn"]
    queue_url = sqs.create_queue(QueueName="cw-alarm-disabled-q")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

    cw.put_metric_alarm(
        AlarmName="cw-actions-disabled",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
        ActionsEnabled=False,
        AlarmActions=[topic_arn],
    )
    cw.set_alarm_state(
        AlarmName="cw-actions-disabled",
        StateValue="ALARM",
        StateReason="forced",
    )
    time.sleep(0.5)
    msgs = sqs.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1,
    ).get("Messages", [])
    assert msgs == []
    cw.delete_alarms(AlarmNames=["cw-actions-disabled"])


def test_cloudwatch_disable_alarm_actions(cw):
    cw.put_metric_alarm(
        AlarmName="heimdall-disable-actions",
        MetricName="M",
        Namespace="N",
        Statistic="Sum",
        Period=60,
        EvaluationPeriods=1,
        Threshold=1.0,
        ComparisonOperator="GreaterThanThreshold",
        ActionsEnabled=True,
    )
    alarm = cw.describe_alarms(AlarmNames=["heimdall-disable-actions"])["MetricAlarms"][0]
    assert alarm["ActionsEnabled"] is True

    cw.disable_alarm_actions(AlarmNames=["heimdall-disable-actions"])
    alarm = cw.describe_alarms(AlarmNames=["heimdall-disable-actions"])["MetricAlarms"][0]
    assert alarm["ActionsEnabled"] is False
    cw.delete_alarms(AlarmNames=["heimdall-disable-actions"])


def test_cloudwatch_describe_alarms_cbor_timestamps_are_tag1():
    """DescribeAlarms over smithy-rpc-v2-cbor must encode Timestamp members as
    CBOR tag 1 (epoch datetime), not a bare unsigned int, or the Terraform AWS
    provider >= 6.50 fails with `unexpected value type cbor.Uint` (issue #1261).
    Integer members (Period, EvaluationPeriods) stay CBOR integers, which the
    Smithy Integer decoder accepts.
    """
    import datetime as _dt

    import cbor2

    from ministack.core import responses as _resp
    from ministack.services import cloudwatch as _cw

    acct_tok = _resp._request_account_id.set("000000000000")
    region_tok = _resp._request_region.set("us-east-1")
    name = f"cbor-alarm-{_uuid_mod.uuid4().hex[:8]}"
    try:
        _cw._put_metric_alarm({}, {
            "AlarmName": name,
            "ComparisonOperator": "GreaterThanThreshold",
            "EvaluationPeriods": 1,
            "MetricName": "Errors",
            "Namespace": "AWS/Lambda",
            "Period": 300,
            "Statistic": "Sum",
            "Threshold": 1,
        }, is_cbor=True)

        status, _headers, body = _cw._describe_alarms({}, {"AlarmNames": [name]}, is_cbor=True)
        assert status == 200
        decoded = cbor2.loads(body)
        alarm = next(a for a in decoded["MetricAlarms"] if a["AlarmName"] == name)

        # cbor2 decodes a CBOR tag-1 value back into a datetime — proof the wire
        # bytes were tag 1, not a bare uint.
        assert isinstance(alarm["StateUpdatedTimestamp"], _dt.datetime)
        assert isinstance(alarm["AlarmConfigurationUpdatedTimestamp"], _dt.datetime)
        # Integer members remain plain integers.
        assert isinstance(alarm["Period"], int) and alarm["Period"] == 300
        assert isinstance(alarm["EvaluationPeriods"], int)
    finally:
        _cw._alarms.pop_scoped("000000000000", "us-east-1", name, None)
        _resp._request_region.reset(region_tok)
        _resp._request_account_id.reset(acct_tok)


def _cbor_has_nil(value):
    if value is None:
        return True
    if isinstance(value, dict):
        return any(_cbor_has_nil(v) for v in value.values())
    if isinstance(value, list):
        return any(_cbor_has_nil(v) for v in value)
    return False


def test_cloudwatch_describe_alarms_cbor_omits_absent_optional_fields():
    """DescribeAlarms over smithy-rpc-v2-cbor must OMIT absent optional fields
    (ExtendedStatistic, Unit) rather than serialize them as CBOR Nil, or the
    Terraform AWS provider >= 6.50 fails the read-back with
    `unexpected value type *cbor.Nil` (issue #1261). Real AWS omits absent
    optional members entirely.
    """
    import cbor2

    from ministack.core import responses as _resp
    from ministack.services import cloudwatch as _cw

    acct_tok = _resp._request_account_id.set("000000000000")
    region_tok = _resp._request_region.set("us-east-1")
    name = f"cbor-nil-{_uuid_mod.uuid4().hex[:8]}"
    try:
        # No ExtendedStatistic / Unit -> stored as None on the alarm record.
        _cw._put_metric_alarm({}, {
            "AlarmName": name,
            "ComparisonOperator": "GreaterThanThreshold",
            "EvaluationPeriods": 1,
            "MetricName": "Errors",
            "Namespace": "AWS/Lambda",
            "Period": 300,
            "Statistic": "Sum",
            "Threshold": 1,
        }, is_cbor=True)

        status, _headers, body = _cw._describe_alarms({}, {"AlarmNames": [name]}, is_cbor=True)
        assert status == 200
        decoded = cbor2.loads(body)
        alarm = next(a for a in decoded["MetricAlarms"] if a["AlarmName"] == name)

        assert "ExtendedStatistic" not in alarm
        assert "Unit" not in alarm
        assert not _cbor_has_nil(decoded)
    finally:
        _cw._alarms.pop_scoped("000000000000", "us-east-1", name, None)
        _resp._request_region.reset(region_tok)
        _resp._request_account_id.reset(acct_tok)


def test_cloudwatch_describe_alarms_next_token_over_cbor_and_query():
    import re as _re

    import cbor2

    from ministack.core import responses as _resp
    from ministack.services import cloudwatch as _cw

    acct_tok = _resp._request_account_id.set("000000000000")
    region_tok = _resp._request_region.set("us-east-1")
    prefix = f"wire-page-{_uuid_mod.uuid4().hex[:8]}-"
    names = [f"{prefix}{i}" for i in range(3)]
    try:
        for name in names:
            _cw._put_metric_alarm({}, {
                "AlarmName": name, "ComparisonOperator": "GreaterThanThreshold", "EvaluationPeriods": 1,
                "MetricName": "Errors", "Namespace": "AWS/Lambda", "Period": 300, "Statistic": "Sum",
                "Threshold": 1,
            }, is_cbor=True)

        _, _, body = _cw._describe_alarms({}, {"AlarmNamePrefix": prefix, "MaxRecords": 2}, is_cbor=True)
        decoded = cbor2.loads(body)
        assert [a["AlarmName"] for a in decoded["MetricAlarms"]] == names[:2]
        _, _, body = _cw._describe_alarms(
            {}, {"AlarmNamePrefix": prefix, "MaxRecords": 2, "NextToken": decoded["NextToken"]}, is_cbor=True)
        decoded = cbor2.loads(body)
        assert [a["AlarmName"] for a in decoded["MetricAlarms"]] == names[2:]
        assert "NextToken" not in decoded

        _, _, body = _cw._describe_alarms({"AlarmNamePrefix": [prefix], "MaxRecords": ["2"]}, {}, is_cbor=False)
        token = _re.search(r"<NextToken>([^<]+)</NextToken>", body.decode()).group(1)
        _, _, body = _cw._describe_alarms(
            {"AlarmNamePrefix": [prefix], "MaxRecords": ["2"], "NextToken": [token]}, {}, is_cbor=False)
        assert _re.findall(r"<AlarmName>([^<]+)</AlarmName>", body.decode()) == names[2:]
        assert "<NextToken>" not in body.decode()
    finally:
        for name in names:
            _cw._alarms.pop_scoped("000000000000", "us-east-1", name, None)
        _resp._request_region.reset(region_tok)
        _resp._request_account_id.reset(acct_tok)


def test_cloudwatch_get_metric_statistics_cbor_timestamps_are_tag1():
    """GetMetricStatistics over smithy-rpc-v2-cbor must encode
    Datapoint.Timestamp as CBOR tag 1, not an ISO string, so the provider's
    typed decoder accepts it (same class as issue #1261)."""
    import datetime as _dt
    import time as _time

    import cbor2

    from ministack.core import responses as _resp
    from ministack.services import cloudwatch as _cw

    acct_tok = _resp._request_account_id.set("000000000000")
    region_tok = _resp._request_region.set("us-east-1")
    namespace = f"MSTest/{_uuid_mod.uuid4().hex[:8]}"
    now = int(_time.time())
    try:
        _cw._put_metric_data({}, {
            "Namespace": namespace,
            "MetricData": [{"MetricName": "M", "Value": 1.0, "Timestamp": now}],
        }, is_cbor=True)

        status, _headers, body = _cw._get_metric_statistics({}, {
            "Namespace": namespace,
            "MetricName": "M",
            "Period": 60,
            "StartTime": now - 3600,
            "EndTime": now + 3600,
            "Statistics": ["Sum"],
        }, is_cbor=True)
        assert status == 200
        datapoints = cbor2.loads(body)["Datapoints"]
        assert datapoints, "expected at least one datapoint"
        assert isinstance(datapoints[0]["Timestamp"], _dt.datetime)
    finally:
        for key in list(_cw._metrics.keys()):
            if key[0] == namespace:
                _cw._metrics.pop_scoped("000000000000", "us-east-1", key, None)
        _resp._request_region.reset(region_tok)
        _resp._request_account_id.reset(acct_tok)


def test_cloudwatch_get_metric_data_cbor_timestamps_are_tag1():
    """GetMetricData over smithy-rpc-v2-cbor must encode
    MetricDataResult.Timestamps as CBOR tag 1 values, not ISO strings (same
    class as issue #1261)."""
    import datetime as _dt
    import time as _time

    import cbor2

    from ministack.core import responses as _resp
    from ministack.services import cloudwatch as _cw

    acct_tok = _resp._request_account_id.set("000000000000")
    region_tok = _resp._request_region.set("us-east-1")
    namespace = f"MSTest/{_uuid_mod.uuid4().hex[:8]}"
    now = int(_time.time())
    try:
        _cw._put_metric_data({}, {
            "Namespace": namespace,
            "MetricData": [{"MetricName": "M", "Value": 2.0, "Timestamp": now}],
        }, is_cbor=True)

        status, _headers, body = _cw._get_metric_data({}, {
            "StartTime": now - 3600,
            "EndTime": now + 3600,
            "MetricDataQueries": [{
                "Id": "m1",
                "ReturnData": True,
                "MetricStat": {
                    "Metric": {"Namespace": namespace, "MetricName": "M"},
                    "Period": 60,
                    "Stat": "Sum",
                },
            }],
        }, is_cbor=True)
        assert status == 200
        results = cbor2.loads(body)["MetricDataResults"]
        timestamps = results[0]["Timestamps"]
        assert timestamps, "expected at least one timestamp"
        assert all(isinstance(t, _dt.datetime) for t in timestamps)
    finally:
        for key in list(_cw._metrics.keys()):
            if key[0] == namespace:
                _cw._metrics.pop_scoped("000000000000", "us-east-1", key, None)
        _resp._request_region.reset(region_tok)
        _resp._request_account_id.reset(acct_tok)


def test_cloudwatch_percentile_alarm_reason_reports_statistic(cw):
    """A percentile alarm's StateReason must name the actual extended statistic
    (e.g. p90), not fall back to Average. Regression for the label that stayed
    'Average' after the percentile value itself was fixed."""
    ns = "Test/PctAlarmReason"
    now = time.time()
    for i, v in enumerate([1, 2, 3, 4, 90, 95, 98, 99]):
        cw.put_metric_data(Namespace=ns, MetricData=[{
            "MetricName": "Latency",
            "Timestamp": now - i,
            "Value": float(v),
        }])
    # p90 of the set is 98.3; threshold 50 -> breaches -> ALARM.
    cw.put_metric_alarm(
        AlarmName="pct-reason",
        MetricName="Latency",
        Namespace=ns,
        ExtendedStatistic="p90",
        Period=3600,
        EvaluationPeriods=1,
        Threshold=50.0,
        ComparisonOperator="GreaterThanThreshold",
    )
    alarm = cw.describe_alarms(AlarmNames=["pct-reason"])["MetricAlarms"][0]
    assert alarm["StateValue"] == "ALARM"
    assert "p90" in alarm["StateReason"]
    assert "Average" not in alarm["StateReason"]


def test_cloudwatch_json_timestamps_are_epoch_numbers(cw):
    """Over awsJson1_0 a smithy timestamp is epoch seconds; the JSON branches
    used to leak the XML path's ISO strings, which every SDK's number parser
    rejects. Raw HTTP because boto3 speaks Query here and would mask the shape.
    Int (not float) epoch is the project convention for JSON bodies."""
    import os
    import urllib.request

    ns = f"JsonTs{_uuid_mod.uuid4().hex[:8]}"
    cw.put_metric_data(
        Namespace=ns,
        MetricData=[{"MetricName": "Latency", "Value": 42.0}],
    )
    now = int(time.time())
    body = json.dumps({
        "Namespace": ns,
        "MetricName": "Latency",
        "StartTime": now - 600,
        "EndTime": now + 60,
        "Period": 60,
        "Statistics": ["Average"],
    }).encode()
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        f"{endpoint}/",
        data=body,
        headers={
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Target": "GraniteServiceVersion20100801.GetMetricStatistics",
            "x-amzn-query-mode": "true",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/monitoring/aws4_request, SignedHeaders=host, Signature=00",
        },
    )
    with urllib.request.urlopen(req) as resp:
        payload = json.loads(resp.read())
    datapoints = payload.get("Datapoints", [])
    assert datapoints, "expected at least one datapoint"
    for dp in datapoints:
        assert isinstance(dp["Timestamp"], int), (
            f"Timestamp must be int epoch over JSON, got {type(dp['Timestamp']).__name__}: {dp['Timestamp']}")


def _cw_period_start():
    return (int(time.time()) // 3600 - 1) * 3600


def test_cloudwatch_values_counts_and_statistic_sets_aggregate_weighted(cw):
    ns = f"Test/Weighted{_uuid_mod.uuid4().hex[:8]}"
    ts = _cw_period_start() + 60
    cw.put_metric_data(Namespace=ns, MetricData=[
        {"MetricName": "Counted", "Timestamp": ts, "Values": [1.0, 2.0], "Counts": [3.0, 1.0]},
        {"MetricName": "Set", "Timestamp": ts,
         "StatisticValues": {"SampleCount": 4, "Sum": 10, "Minimum": 1, "Maximum": 4}},
        {"MetricName": "Repeated", "Timestamp": ts,
         "StatisticValues": {"SampleCount": 3, "Sum": 6, "Minimum": 2, "Maximum": 2}},
        {"MetricName": "Negative", "Timestamp": ts, "Values": [-1.0, 5.0]},
    ])

    def stats(name, **kw):
        return cw.get_metric_statistics(Namespace=ns, MetricName=name, StartTime=ts - 60,
                                         EndTime=ts + 3000, Period=3600, **kw)["Datapoints"][0]

    dp = stats("Counted", Statistics=["SampleCount", "Sum", "Average", "Minimum", "Maximum"])
    assert (dp["SampleCount"], dp["Sum"], dp["Average"], dp["Minimum"], dp["Maximum"]) == (4, 5, 1.25, 1, 2)
    assert stats("Counted", ExtendedStatistics=["p50", "p100"])["ExtendedStatistics"] == {"p50": 1, "p100": 2}

    dp = stats("Set", Statistics=["SampleCount", "Sum", "Average", "Minimum", "Maximum"])
    assert (dp["SampleCount"], dp["Sum"], dp["Average"], dp["Minimum"], dp["Maximum"]) == (4, 10, 2.5, 1, 4)
    assert "ExtendedStatistics" not in stats("Set", ExtendedStatistics=["p50"])
    assert stats("Repeated", ExtendedStatistics=["p50"])["ExtendedStatistics"] == {"p50": 2}
    assert "ExtendedStatistics" not in stats("Negative", ExtendedStatistics=["p50"])

    resp = cw.get_metric_data(StartTime=ts - 60, EndTime=ts + 3000, MetricDataQueries=[
        {"Id": "neg", "MetricStat": {"Metric": {"Namespace": ns, "MetricName": "Negative"},
                                     "Period": 3600, "Stat": "p50"}},
        {"Id": "avg", "MetricStat": {"Metric": {"Namespace": ns, "MetricName": "Negative"},
                                     "Period": 3600, "Stat": "Average"}}])
    by_id = {r["Id"]: r["Values"] for r in resp["MetricDataResults"]}
    assert by_id == {"neg": [], "avg": [2.0]}


def test_cloudwatch_query_protocol_put_metric_data_values_and_statistic_values():
    import urllib.parse
    import urllib.request

    from conftest import ENDPOINT
    ns = f"Test/QueryWeighted{_uuid_mod.uuid4().hex[:8]}"
    ts = _cw_period_start() + 60
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
    form = {
        "Action": "PutMetricData", "Version": "2010-08-01", "Namespace": ns,
        "MetricData.member.1.MetricName": "Counted", "MetricData.member.1.Timestamp": stamp,
        "MetricData.member.1.Values.member.1": "1", "MetricData.member.1.Values.member.2": "2",
        "MetricData.member.1.Counts.member.1": "3", "MetricData.member.1.Counts.member.2": "1",
        "MetricData.member.2.MetricName": "Set", "MetricData.member.2.Timestamp": stamp,
        "MetricData.member.2.StatisticValues.SampleCount": "4",
        "MetricData.member.2.StatisticValues.Sum": "10",
        "MetricData.member.2.StatisticValues.Minimum": "1",
        "MetricData.member.2.StatisticValues.Maximum": "4",
    }
    req = urllib.request.Request(
        ENDPOINT, data=urllib.parse.urlencode(form).encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/monitoring/aws4_request, "
                                  "SignedHeaders=host, Signature=x"})
    assert urllib.request.urlopen(req).status == 200
    cw = __import__("conftest").make_client("cloudwatch")
    for name, expected in (("Counted", (4, 5)), ("Set", (4, 10))):
        dp = cw.get_metric_statistics(Namespace=ns, MetricName=name, StartTime=ts - 60, EndTime=ts + 3000,
                                      Period=3600, Statistics=["SampleCount", "Sum"])["Datapoints"][0]
        assert (dp["SampleCount"], dp["Sum"]) == expected


def test_cloudwatch_parse_ts_accepts_cbor_datetime():
    from datetime import datetime, timezone

    from ministack.services.cloudwatch import _parse_ts
    ts = datetime(2026, 10, 6, 12, 30, tzinfo=timezone.utc)
    assert _parse_ts(ts) == ts.timestamp()
    assert _parse_ts(ts.replace(tzinfo=None)) == ts.timestamp()


_ALARM_BASE = dict(MetricName="CPUUtilization", Namespace="AWS/EC2", Statistic="Average", Period=60,
                   EvaluationPeriods=3, Threshold=80, ComparisonOperator="GreaterThanThreshold")


def _cw_tag_set(cw, arn):
    """ListTagsForResource as a {Key: Value} dict: AWS lists alarm tags in no
    order we could pin down, so tests compare sets."""
    return {t["Key"]: t["Value"] for t in cw.list_tags_for_resource(ResourceARN=arn)["Tags"]}


def _cw_raw(protocol, action, params):
    """A CloudWatch call over Query or JSON 1.0 (boto3 speaks CBOR). Returns
    (status, body text)."""
    import urllib.error
    import urllib.parse
    import urllib.request

    from conftest import ENDPOINT
    auth = ("AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/monitoring/aws4_request, "
            "SignedHeaders=host, Signature=x")
    if protocol == "query":
        body = urllib.parse.urlencode({"Action": action, "Version": "2010-08-01", **params}).encode()
        headers = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": auth}
    else:
        body = json.dumps(params).encode()
        headers = {"Content-Type": "application/x-amz-json-1.0", "x-amzn-query-mode": "true",
                   "X-Amz-Target": f"GraniteServiceVersion20100801.{action}", "Authorization": auth}
    req = urllib.request.Request(ENDPOINT, data=body, headers=headers)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def test_cloudwatch_put_metric_alarm_applies_tags_on_create_only(cw):
    """PutMetricAlarm's Tags are stored when it creates the alarm and ignored
    on an existing one; TagResource still adds to them.
    AWS: captured 2026-10-20 (cloudfront-function-and-alarm-tags C8, C9, C10)."""
    name = f"tagged-alarm-{_uuid_mod.uuid4().hex[:8]}"
    try:
        cw.put_metric_alarm(AlarmName=name, Tags=[{"Key": "team", "Value": "platform"}], **_ALARM_BASE)
        arn = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["AlarmArn"]
        assert _cw_tag_set(cw, arn) == {"team": "platform"}

        cw.put_metric_alarm(AlarmName=name, Tags=[{"Key": "other", "Value": "x"}], **_ALARM_BASE)
        assert _cw_tag_set(cw, arn) == {"team": "platform"}

        cw.tag_resource(ResourceARN=arn, Tags=[{"Key": "other", "Value": "x"}])
        assert _cw_tag_set(cw, arn) == {"team": "platform", "other": "x"}
    finally:
        cw.delete_alarms(AlarmNames=[name])


def test_cloudwatch_put_metric_alarm_tags_ignored_on_alarm_created_without_tags(cw):
    """AWS: captured 2026-10-21 (2309-uncertainties U9:cbor-created-untagged-then-update-with-tags)."""
    name = f"untagged-alarm-{_uuid_mod.uuid4().hex[:8]}"
    try:
        cw.put_metric_alarm(AlarmName=name, **_ALARM_BASE)
        cw.put_metric_alarm(AlarmName=name, Tags=[{"Key": "team", "Value": "platform"}], **_ALARM_BASE)
        arn = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["AlarmArn"]
        assert cw.list_tags_for_resource(ResourceARN=arn)["Tags"] == []
    finally:
        cw.delete_alarms(AlarmNames=[name])


def test_cloudwatch_put_metric_alarm_create_tags_empty_and_repeated_keys(cw):
    """An empty Tags list stores nothing. A key sent twice keeps the last
    value, and keys that differ only in case are two tags.
    AWS: captured 2026-10-21 (2309-uncertainties U7:cbor-empty-tags-on-create,
    U6:cbor-duplicate-keys, U6:cbor-keys-differing-in-case)."""
    uid = _uuid_mod.uuid4().hex[:8]
    names = [f"empty-tags-{uid}", f"dup-tags-{uid}", f"case-tags-{uid}"]
    try:
        cw.put_metric_alarm(AlarmName=names[0], Tags=[], **_ALARM_BASE)
        cw.put_metric_alarm(AlarmName=names[1], Tags=[{"Key": "dup", "Value": "a"}, {"Key": "dup", "Value": "b"}],
                            **_ALARM_BASE)
        cw.put_metric_alarm(AlarmName=names[2], Tags=[{"Key": "dup", "Value": "a"}, {"Key": "DUP", "Value": "b"}],
                            **_ALARM_BASE)
        by_name = {a["AlarmName"]: a["AlarmArn"] for a in cw.describe_alarms(AlarmNames=names)["MetricAlarms"]}
        assert cw.list_tags_for_resource(ResourceARN=by_name[names[0]])["Tags"] == []
        assert _cw_tag_set(cw, by_name[names[1]]) == {"dup": "b"}
        assert _cw_tag_set(cw, by_name[names[2]]) == {"dup": "a", "DUP": "b"}
    finally:
        cw.delete_alarms(AlarmNames=names)


def test_cloudwatch_alarm_created_again_after_delete_has_only_its_own_tags(cw):
    """AWS: captured 2026-10-20 (cloudfront-function-and-alarm-tags C12)."""
    name = f"recreated-alarm-{_uuid_mod.uuid4().hex[:8]}"
    try:
        cw.put_metric_alarm(AlarmName=name, Tags=[{"Key": "team", "Value": "platform"}], **_ALARM_BASE)
        cw.delete_alarms(AlarmNames=[name])
        cw.put_metric_alarm(AlarmName=name, Tags=[{"Key": "contact", "Value": ""}], **_ALARM_BASE)
        arn = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["AlarmArn"]
        assert cw.list_tags_for_resource(ResourceARN=arn)["Tags"] == [{"Key": "contact", "Value": ""}]
    finally:
        cw.delete_alarms(AlarmNames=[name])


def test_cloudwatch_put_metric_alarm_returns_datapoints_to_alarm_only_when_set(cw):
    """An update that leaves DatapointsToAlarm out clears it; a value equal to
    EvaluationPeriods is still returned.
    AWS: captured 2026-10-20 (cloudfront-function-and-alarm-tags C13, C14, C15)
    and 2026-10-21 (2309-uncertainties U8:cbor-dta-equal-to-evaluation-periods)."""
    name = f"m-of-n-alarm-{_uuid_mod.uuid4().hex[:8]}"
    try:
        cw.put_metric_alarm(AlarmName=name, **_ALARM_BASE)
        assert "DatapointsToAlarm" not in cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]

        cw.put_metric_alarm(AlarmName=name, DatapointsToAlarm=2, **_ALARM_BASE)
        assert cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["DatapointsToAlarm"] == 2

        cw.put_metric_alarm(AlarmName=name, **_ALARM_BASE)
        assert "DatapointsToAlarm" not in cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]

        cw.put_metric_alarm(AlarmName=name, DatapointsToAlarm=3, **_ALARM_BASE)
        assert cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["DatapointsToAlarm"] == 3
    finally:
        cw.delete_alarms(AlarmNames=[name])


_QUERY_ALARM = {"MetricName": "Errors", "Namespace": "AWS/Lambda", "Statistic": "Sum", "Period": "60",
                "EvaluationPeriods": "3", "Threshold": "1", "ComparisonOperator": "GreaterThanThreshold"}


def test_cloudwatch_query_put_metric_alarm_tags_and_datapoints_to_alarm(cw):
    """Query: Tags.member.N are stored on create, an empty value included;
    DescribeAlarms has no <DatapointsToAlarm> until an update sets it, and that
    update's Tags are ignored.
    AWS: captured 2026-10-20 (cloudfront-function-and-alarm-tags C16, C17, C18)."""
    name = f"query-alarm-{_uuid_mod.uuid4().hex[:8]}"
    try:
        status, body = _cw_raw("query", "PutMetricAlarm", {
            "AlarmName": name, **_QUERY_ALARM, "Tags.member.1.Key": "team", "Tags.member.1.Value": "platform",
            "Tags.member.2.Key": "contact", "Tags.member.2.Value": ""})
        assert status == 200
        assert re.search(r"<PutMetricAlarmResponse[^>]*>\s*<ResponseMetadata>", body)
        arn = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["AlarmArn"]
        status, body = _cw_raw("query", "ListTagsForResource", {"ResourceARN": arn})
        assert status == 200
        members = re.findall(r"<member>(.*?)</member>", body, re.S)
        tags = {re.search(r"<Key>(.*?)</Key>", m).group(1): re.search(r"<Value>(.*?)</Value>|<Value/>", m).group(1) or ""
                for m in members}
        assert tags == {"team": "platform", "contact": ""}
        status, body = _cw_raw("query", "DescribeAlarms", {"AlarmNames.member.1": name})
        assert status == 200 and "<EvaluationPeriods>3</EvaluationPeriods>" in body
        assert "<DatapointsToAlarm>" not in body

        status, _ = _cw_raw("query", "PutMetricAlarm", {
            "AlarmName": name, **_QUERY_ALARM, "DatapointsToAlarm": "2",
            "Tags.member.1.Key": "other", "Tags.member.1.Value": "x"})
        assert status == 200
        _, body = _cw_raw("query", "DescribeAlarms", {"AlarmNames.member.1": name})
        assert "<DatapointsToAlarm>2</DatapointsToAlarm>" in body
        assert _cw_tag_set(cw, arn) == {"team": "platform", "contact": ""}
    finally:
        cw.delete_alarms(AlarmNames=[name])


def test_cloudwatch_query_put_metric_alarm_tags_ignored_on_alarm_created_without_tags(cw):
    """AWS: captured 2026-10-21 (2309-uncertainties U9:query-created-untagged-then-update-with-tags)."""
    name = f"query-untagged-{_uuid_mod.uuid4().hex[:8]}"
    try:
        assert _cw_raw("query", "PutMetricAlarm", {"AlarmName": name, **_QUERY_ALARM})[0] == 200
        assert _cw_raw("query", "PutMetricAlarm", {"AlarmName": name, **_QUERY_ALARM, "Tags.member.1.Key": "team",
                                                   "Tags.member.1.Value": "platform"})[0] == 200
        arn = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["AlarmArn"]
        assert cw.list_tags_for_resource(ResourceARN=arn)["Tags"] == []
    finally:
        cw.delete_alarms(AlarmNames=[name])


def test_cloudwatch_json_put_metric_alarm_tags_and_datapoints_to_alarm(cw):
    """JSON 1.0: Tags are stored on create and ignored by an update, whether
    the alarm had tags or not; DescribeAlarms leaves DatapointsToAlarm out when
    it is absent or null.
    AWS: captured 2026-10-20 (cloudfront-function-and-alarm-tags C19) and
    2026-10-21 (2309-uncertainties U4:json-update-different-tags,
    U9:json-created-untagged-then-update-with-tags, U8:json-dta-null)."""
    uid = _uuid_mod.uuid4().hex[:8]
    tagged, untagged = f"json-tagged-{uid}", f"json-untagged-{uid}"
    base = {**_ALARM_BASE}
    try:
        status, _ = _cw_raw("json", "PutMetricAlarm", {"AlarmName": tagged, **base,
                                                       "Tags": [{"Key": "team", "Value": "platform"}]})
        assert status == 200
        status, body = _cw_raw("json", "DescribeAlarms", {"AlarmNames": [tagged]})
        alarm = json.loads(body)["MetricAlarms"][0]
        assert alarm["EvaluationPeriods"] == 3 and "DatapointsToAlarm" not in alarm
        status, body = _cw_raw("json", "ListTagsForResource", {"ResourceARN": alarm["AlarmArn"]})
        assert status == 200 and json.loads(body) == {"Tags": [{"Key": "team", "Value": "platform"}]}

        assert _cw_raw("json", "PutMetricAlarm", {"AlarmName": tagged, **base,
                                                  "Tags": [{"Key": "other", "Value": "x"}]})[0] == 200
        assert _cw_tag_set(cw, alarm["AlarmArn"]) == {"team": "platform"}

        assert _cw_raw("json", "PutMetricAlarm", {"AlarmName": untagged, **base, "DatapointsToAlarm": None})[0] == 200
        assert "DatapointsToAlarm" not in cw.describe_alarms(AlarmNames=[untagged])["MetricAlarms"][0]
        assert _cw_raw("json", "PutMetricAlarm", {"AlarmName": untagged, **base,
                                                  "Tags": [{"Key": "team", "Value": "platform"}]})[0] == 200
        arn = cw.describe_alarms(AlarmNames=[untagged])["MetricAlarms"][0]["AlarmArn"]
        assert cw.list_tags_for_resource(ResourceARN=arn)["Tags"] == []
    finally:
        cw.delete_alarms(AlarmNames=[tagged, untagged])


def test_cloudwatch_put_metric_alarm_repeated_key_keeps_last_value_over_json_and_query(cw):
    """AWS: captured 2026-10-21 (2309-uncertainties U15:json-duplicate-keys,
    U15:query-duplicate-keys)."""
    uid = _uuid_mod.uuid4().hex[:8]
    json_name, query_name = f"json-dup-{uid}", f"query-dup-{uid}"
    try:
        assert _cw_raw("json", "PutMetricAlarm", {"AlarmName": json_name, **_ALARM_BASE, "Tags": [
            {"Key": "dup", "Value": "a"}, {"Key": "dup", "Value": "b"}]})[0] == 200
        assert _cw_raw("query", "PutMetricAlarm", {
            "AlarmName": query_name, **_QUERY_ALARM, "Tags.member.1.Key": "dup", "Tags.member.1.Value": "a",
            "Tags.member.2.Key": "dup", "Tags.member.2.Value": "b"})[0] == 200
        by_name = {a["AlarmName"]: a["AlarmArn"]
                   for a in cw.describe_alarms(AlarmNames=[json_name, query_name])["MetricAlarms"]}
        assert _cw_tag_set(cw, by_name[json_name]) == {"dup": "b"}
        assert _cw_tag_set(cw, by_name[query_name]) == {"dup": "b"}
    finally:
        cw.delete_alarms(AlarmNames=[json_name, query_name])


def test_cloudwatch_json_put_metric_alarm_null_tags(cw):
    """"Tags": null creates the alarm without tags, and on an update leaves the
    alarm's tags as they were.
    AWS: captured 2026-10-21 (2309-uncertainties U14:json-tags-null-on-create,
    U14:json-tags-null-on-update)."""
    name = f"json-null-tags-{_uuid_mod.uuid4().hex[:8]}"
    try:
        assert _cw_raw("json", "PutMetricAlarm", {"AlarmName": name, **_ALARM_BASE, "Tags": None})[0] == 200
        arn = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["AlarmArn"]
        assert cw.list_tags_for_resource(ResourceARN=arn)["Tags"] == []

        cw.tag_resource(ResourceARN=arn, Tags=[{"Key": "team", "Value": "platform"}])
        assert _cw_raw("json", "PutMetricAlarm", {"AlarmName": name, **_ALARM_BASE, "Tags": None})[0] == 200
        assert _cw_tag_set(cw, arn) == {"team": "platform"}
    finally:
        cw.delete_alarms(AlarmNames=[name])


# Two leading tags put the tag under test at index 3, as in the captures.
_FILLER_TAGS = [{"Key": "f1", "Value": "v"}, {"Key": "f2", "Value": "v"}]
_NULL_KEY = "Value null at 'tags.{}.member.key' failed to satisfy constraint: Member must not be null"
_NULL_VALUE = "Value null at 'tags.{}.member.value' failed to satisfy constraint: Member must not be null"
_DTA_MIN = ("Value '{}' at 'datapointsToAlarm' failed to satisfy constraint: "
            "Member must have value greater than or equal to 1")
_AWS_PREFIX = 'The provided tag keys must not start with any case of "aws:"'
_TAG_LIMIT = "The resultant tag set must not have more than 50 user tags."


def _cw_unvalidated():
    """boto3 over CBOR without client-side checks, so it sends what AWS refuses."""
    return __import__("conftest").make_client("cloudwatch", additional_config_kwargs={"parameter_validation": False})


def _cw_json_put(name, tags, **extra):
    """JSON 1.0 PutMetricAlarm; returns (status, message). The message is read
    as botocore reads it: "message" or "Message", "" when there is none (AWS
    sends no message for some errors), None on success."""
    status, body = _cw_raw("json", "PutMetricAlarm", {"AlarmName": name, **_ALARM_BASE, "Tags": tags, **extra})
    if status < 300:
        return status, None
    payload = json.loads(body)
    return status, payload.get("message", payload.get("Message")) or ""


def _cw_alarm_exists(cw, name):
    return bool(cw.describe_alarms(AlarmNames=[name])["MetricAlarms"])


def test_cloudwatch_put_metric_alarm_refuses_tag_constraint_violations(cw):
    """Each tag is checked Value first, then Key; datapointsToAlarm comes after
    the tags; all violations are joined in one message, and nothing is created.
    AWS: captured 2026-10-21 (2309-uncertainties V:json-two-missing-key,
    V:json-missing-key-and-value-same-tag, V:json-missing-value-then-missing-key,
    V:json-missing-key-first, V:json-empty-key, V:json-key-129, V:json-value-257,
    V:json-four-missing-values, V:json-missing-value-and-dta-0, X:json-dta-negative,
    X:json-value-explicit-null, U5:json-tag-without-value, U19:json-tag-without-key,
    U18:json-dta-0)."""
    cases = [
        (_FILLER_TAGS + [{"Value": "x"}, {"Value": "y"}], {},
         f"2 validation errors detected: {_NULL_KEY.format(3)}; {_NULL_KEY.format(4)}"),
        (_FILLER_TAGS + [{}], {}, f"2 validation errors detected: {_NULL_VALUE.format(3)}; {_NULL_KEY.format(3)}"),
        (_FILLER_TAGS + [{"Key": "a"}, {"Value": "y"}], {},
         f"2 validation errors detected: {_NULL_VALUE.format(3)}; {_NULL_KEY.format(4)}"),
        ([{"Value": "x"}] + _FILLER_TAGS, {}, f"1 validation error detected: {_NULL_KEY.format(1)}"),
        (_FILLER_TAGS + [{"Key": "a", "Value": None}], {}, f"1 validation error detected: {_NULL_VALUE.format(3)}"),
        (_FILLER_TAGS + [{"Key": "", "Value": "x"}], {},
         "1 validation error detected: Value '' at 'tags.3.member.key' failed to satisfy constraint: "
         "Member must have length greater than or equal to 1"),
        (_FILLER_TAGS + [{"Key": "k" * 129, "Value": "x"}], {},
         f"1 validation error detected: Value '{'k' * 129}' at 'tags.3.member.key' failed to satisfy constraint: "
         "Member must have length less than or equal to 128"),
        (_FILLER_TAGS + [{"Key": "a", "Value": "v" * 257}], {},
         f"1 validation error detected: Value '{'v' * 257}' at 'tags.3.member.value' failed to satisfy constraint: "
         "Member must have length less than or equal to 256"),
        (_FILLER_TAGS + [{"Key": k} for k in "abcd"], {},
         "4 validation errors detected: " + "; ".join(_NULL_VALUE.format(i) for i in range(3, 7))),
        (_FILLER_TAGS + [{"Key": "a"}], {"DatapointsToAlarm": 0},
         f"2 validation errors detected: {_NULL_VALUE.format(3)}; {_DTA_MIN.format(0)}"),
        (_FILLER_TAGS, {"DatapointsToAlarm": -1}, f"1 validation error detected: {_DTA_MIN.format(-1)}"),
        (_FILLER_TAGS, {"DatapointsToAlarm": 0}, f"1 validation error detected: {_DTA_MIN.format(0)}"),
    ]
    for i, (tags, extra, message) in enumerate(cases):
        name = f"tag-constraint-{i}-{_uuid_mod.uuid4().hex[:8]}"
        assert _cw_json_put(name, tags, **extra) == (400, message), i
        assert not _cw_alarm_exists(cw, name), i


def test_cloudwatch_put_metric_alarm_tag_constraint_bounds_are_accepted(cw):
    """A 128-character key, a 256-character value, an empty value and an
    unknown member inside a tag are accepted.
    AWS: captured 2026-10-21 (2309-uncertainties V:json-key-128, V:json-value-256,
    X:json-unknown-tag-member) and 2026-10-20 (cloudfront-function-and-alarm-tags C12)."""
    name = f"tag-bounds-{_uuid_mod.uuid4().hex[:8]}"
    try:
        tags = [{"Key": "k" * 128, "Value": "x"}, {"Key": "a", "Value": "v" * 256}, {"Key": "e", "Value": ""},
                {"Key": "b", "Value": "c", "Extra": "d"}]
        assert _cw_json_put(name, tags)[0] == 200
        arn = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["AlarmArn"]
        assert _cw_tag_set(cw, arn) == {"k" * 128: "x", "a": "v" * 256, "e": "", "b": "c"}
    finally:
        cw.delete_alarms(AlarmNames=[name])


def test_cloudwatch_put_metric_alarm_tag_errors_over_cbor(cw):
    """boto3's codes and messages over CBOR, as AWS answered them.
    AWS: captured 2026-10-21 through boto3 (2309-uncertainties Z:boto3-cbor-missing-key,
    Z:boto3-cbor-aws-prefix, Z:boto3-cbor-dta-0, Z:boto3-cbor-missing-value) and
    2026-10-20 (I11:cbor-51-tags); messages also V:cbor-two-missing-key,
    V:cbor-missing-key-and-value-same-tag."""
    raw = _cw_unvalidated()
    cases = [
        (_FILLER_TAGS + [{"Value": "x"}, {"Value": "y"}], "ValidationError", 400,
         f"2 validation errors detected: {_NULL_KEY.format(3)}; {_NULL_KEY.format(4)}"),
        (_FILLER_TAGS + [{}], "ValidationError", 400,
         f"2 validation errors detected: {_NULL_VALUE.format(3)}; {_NULL_KEY.format(3)}"),
        (_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}], "AccessDenied", 403, _AWS_PREFIX),
        (_FILLER_TAGS + [{"Key": f"k{i:02d}", "Value": "v"} for i in range(49)], "ValidationError", 400, _TAG_LIMIT),
        (_FILLER_TAGS + [{"Key": "a"}], "ValidationError", 400, f"1 validation error detected: {_NULL_VALUE.format(3)}"),
        (_FILLER_TAGS + [{"Value": "x"}], "ValidationError", 400, f"1 validation error detected: {_NULL_KEY.format(3)}"),
    ]
    for i, (tags, code, status, message) in enumerate(cases):
        name = f"cbor-tag-error-{i}-{_uuid_mod.uuid4().hex[:8]}"
        with pytest.raises(ClientError) as exc:
            raw.put_metric_alarm(AlarmName=name, Tags=tags, **_ALARM_BASE)
        err = exc.value.response
        assert (err["Error"]["Code"], err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Message"]) == (
            code, status, message), i
        assert not _cw_alarm_exists(cw, name), i
    name = f"cbor-dta-0-{_uuid_mod.uuid4().hex[:8]}"
    with pytest.raises(ClientError) as exc:
        raw.put_metric_alarm(AlarmName=name, Tags=_FILLER_TAGS, DatapointsToAlarm=0, **_ALARM_BASE)
    assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
        "ValidationError", f"1 validation error detected: {_DTA_MIN.format(0)}")


def test_cloudwatch_put_metric_alarm_aws_prefix_refused_on_create_only(cw):
    """An aws: key (any case) is refused with 403 on create, before the tag
    constraints and datapointsToAlarm; an update ignores it. A value or a key
    like "awsx" is fine.
    AWS: captured 2026-10-21 (2309-uncertainties W:json-aws-prefix,
    W:json-AWS-upper-prefix, W:json-aws-prefix-and-missing-value,
    W:json-aws-prefix-and-51-tags, X:json-aws-prefix-and-dta-0,
    W:json-aws-prefix-in-value-only, W:json-key-awsx-no-colon, W:json-update-aws-prefix,
    X:json-update-missing-value-and-aws-prefix)."""
    uid = _uuid_mod.uuid4().hex[:8]
    for i, (tags, extra) in enumerate([
        (_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}], {}),
        (_FILLER_TAGS + [{"Key": "AWS:x", "Value": "x"}], {}),
        (_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}, {"Key": "a"}], {}),
        (_FILLER_TAGS + [{"Key": f"k{n:02d}", "Value": "v"} for n in range(48)] + [{"Key": "aws:x", "Value": "x"}], {}),
        (_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}], {"DatapointsToAlarm": 0}),
    ]):
        name = f"aws-prefix-{i}-{uid}"
        assert _cw_json_put(name, tags, **extra) == (403, _AWS_PREFIX), i
        assert not _cw_alarm_exists(cw, name), i

    ok, existing = f"aws-in-value-{uid}", f"aws-update-{uid}"
    try:
        assert _cw_json_put(ok, [{"Key": "a", "Value": "aws:x"}, {"Key": "awsx", "Value": "x"}])[0] == 200
        assert _cw_json_put(existing, [{"Key": "team", "Value": "platform"}])[0] == 200
        assert _cw_json_put(existing, [{"Key": "aws:x", "Value": "x"}], Threshold=82)[0] == 200
        alarm = cw.describe_alarms(AlarmNames=[existing])["MetricAlarms"][0]
        assert alarm["Threshold"] == 82
        assert _cw_tag_set(cw, alarm["AlarmArn"]) == {"team": "platform"}
        assert _cw_json_put(existing, [{"Key": "aws:x", "Value": "x"}, {"Key": "a"}], Threshold=84) == (
            400, f"1 validation error detected: {_NULL_VALUE.format(2)}")
        assert cw.describe_alarms(AlarmNames=[existing])["MetricAlarms"][0]["Threshold"] == 82
    finally:
        cw.delete_alarms(AlarmNames=[ok, existing])


def test_cloudwatch_put_metric_alarm_tag_limit_on_create_only(cw):
    """More than 50 unique keys are refused on create, after the tag
    constraints; repeated keys count once; an update with 51 tags succeeds and
    keeps the alarm's tags; a refused update changes nothing.
    AWS: captured 2026-10-21 (2309-uncertainties W:json-51-tags,
    V:json-51-tags-and-missing-value, V:json-51-tags-and-key-129,
    V:json-51-tags-repeated-key-50-unique, V:json-update-51-tags,
    V:json-update-missing-key, X:json-update-dta-0) and 2026-10-20 (I11:cbor-50-tags)."""
    uid = _uuid_mod.uuid4().hex[:8]
    fifty_one = _FILLER_TAGS + [{"Key": f"k{n:02d}", "Value": "v"} for n in range(49)]
    assert _cw_json_put(f"limit-51-{uid}", fifty_one) == (400, _TAG_LIMIT)
    assert not _cw_alarm_exists(cw, f"limit-51-{uid}")
    assert _cw_json_put(f"limit-mv-{uid}", fifty_one[:50] + [{"Key": "z"}]) == (
        400, f"1 validation error detected: {_NULL_VALUE.format(51)}")
    names = [f"limit-dup-{uid}", f"limit-upd-{uid}"]
    try:
        assert _cw_json_put(names[0], fifty_one[:50] + [{"Key": "k00", "Value": "again"}])[0] == 200
        arn = cw.describe_alarms(AlarmNames=[names[0]])["MetricAlarms"][0]["AlarmArn"]
        assert len(_cw_tag_set(cw, arn)) == 50

        assert _cw_json_put(names[1], [{"Key": "team", "Value": "platform"}])[0] == 200
        assert _cw_json_put(names[1], fifty_one, Threshold=81)[0] == 200
        alarm = cw.describe_alarms(AlarmNames=[names[1]])["MetricAlarms"][0]
        assert alarm["Threshold"] == 81 and _cw_tag_set(cw, alarm["AlarmArn"]) == {"team": "platform"}
        assert _cw_json_put(names[1], _FILLER_TAGS + [{"Value": "x"}], Threshold=85) == (
            400, f"1 validation error detected: {_NULL_KEY.format(3)}")
        assert _cw_json_put(names[1], [], DatapointsToAlarm=0, Threshold=86) == (
            400, f"1 validation error detected: {_DTA_MIN.format(0)}")
        assert cw.describe_alarms(AlarmNames=[names[1]])["MetricAlarms"][0]["Threshold"] == 81
    finally:
        cw.delete_alarms(AlarmNames=names)


def test_cloudwatch_put_metric_alarm_refuses_wrong_json_types_in_tags(cw):
    """The first wrong JSON type in Tags, in document order, before any other
    check; an update is refused the same way and changes nothing.
    AWS: captured 2026-10-21 (2309-uncertainties V:json-tags-not-a-list,
    V:json-tag-not-an-object, V:json-key-number, W:json-value-number, X:json-key-true,
    Y:json-key-false, X:json-key-list, X:json-key-object, X:json-value-float,
    Y:json-value-true, Y:json-value-list, Y:json-value-object, X:json-tags-object,
    Y:json-tags-number, Y:json-tags-true, Y:json-element-number, Y:json-element-list,
    Y:json-element-true, X:json-tag-null-element, X:json-key-number-and-aws-prefix,
    Y:json-key-number-then-value-true, Y:json-value-true-then-key-number,
    W:json-update-key-number)."""
    t = _FILLER_TAGS
    cases = [
        ("x", 400, "Unexpected field type"),
        (5, 400, "Unexpected field type"),
        (True, 400, "Unexpected field type"),
        ({"Key": "a", "Value": "b"}, 400, "Start of structure or map found where not expected"),
        (t + ["x"], 400, "Unexpected value type in payload"),
        (t + [5], 400, "Unexpected value type in payload"),
        (t + [True], 400, "Unexpected value type in payload"),
        (t + [["a"]], 400, ""),
        (t + [None], 500, ""),
        (t + [{"Key": 5, "Value": "x"}], 400, "NUMBER_VALUE cannot be converted to String"),
        (t + [{"Key": "a", "Value": 5}], 400, "NUMBER_VALUE cannot be converted to String"),
        (t + [{"Key": "a", "Value": 1.5}], 400, "DECIMAL_VALUE cannot be converted to String"),
        (t + [{"Key": True, "Value": "x"}], 400, "TRUE_VALUE cannot be converted to String"),
        (t + [{"Key": False, "Value": "x"}], 400, "FALSE_VALUE cannot be converted to String"),
        (t + [{"Key": "a", "Value": True}], 400, "TRUE_VALUE cannot be converted to String"),
        (t + [{"Key": ["a"], "Value": "x"}], 400, ""),
        (t + [{"Key": "a", "Value": ["x"]}], 400, ""),
        (t + [{"Key": {"a": 1}, "Value": "x"}], 400, "Start of structure or map found where not expected"),
        (t + [{"Key": "a", "Value": {"x": 1}}], 400, "Start of structure or map found where not expected"),
        (t + [{"Key": "aws:x", "Value": "x"}, {"Key": 5, "Value": "x"}], 400, "NUMBER_VALUE cannot be converted to String"),
        (t + [{"Key": 5, "Value": True}], 400, "NUMBER_VALUE cannot be converted to String"),
        (t + [{"Value": True, "Key": 5}], 400, "TRUE_VALUE cannot be converted to String"),
    ]
    for i, (tags, status, message) in enumerate(cases):
        name = f"tag-type-{i}-{_uuid_mod.uuid4().hex[:8]}"
        assert _cw_json_put(name, tags) == (status, message), i
        assert not _cw_alarm_exists(cw, name), i

    name = f"tag-type-update-{_uuid_mod.uuid4().hex[:8]}"
    try:
        assert _cw_json_put(name, [{"Key": "team", "Value": "platform"}])[0] == 200
        assert _cw_json_put(name, t + [{"Key": 5, "Value": "x"}], Threshold=82) == (
            400, "NUMBER_VALUE cannot be converted to String")
        assert cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["Threshold"] == 80
    finally:
        cw.delete_alarms(AlarmNames=[name])


def test_cloudwatch_query_put_metric_alarm_tag_and_datapoints_errors(cw):
    """Query: a member without Key or Value is null; DatapointsToAlarm 0 breaks
    the minimum.
    AWS: captured 2026-10-21 (2309-uncertainties V:query-member-3-key-only-empty-value-param,
    U5:query-tag-without-value, U19:query-tag-without-key, U18:query-dta-0, W:query-51-tags,
    W:query-aws-prefix)."""
    def put(name, **params):
        status, body = _cw_raw("query", "PutMetricAlarm", {"AlarmName": name, **_QUERY_ALARM, **params})
        code = re.search(r"<Code>(.*?)</Code>", body)
        message = re.search(r"<Message>(.*?)</Message>", body, re.S)
        return status, code and code.group(1), message and html.unescape(message.group(1))

    filler = {"Tags.member.1.Key": "f1", "Tags.member.1.Value": "v", "Tags.member.2.Key": "f2", "Tags.member.2.Value": "v"}
    fifty_one = {}
    for n in range(51):
        fifty_one.update({f"Tags.member.{n + 1}.Key": f"k{n:02d}", f"Tags.member.{n + 1}.Value": "v"})
    cases = [
        ({**filler, "Tags.member.3.Key": "team"}, 400, "ValidationError",
         f"1 validation error detected: {_NULL_VALUE.format(3)}"),
        ({**filler, "Tags.member.3.Value": "x"}, 400, "ValidationError",
         f"1 validation error detected: {_NULL_KEY.format(3)}"),
        ({**filler, "DatapointsToAlarm": "0"}, 400, "ValidationError", f"1 validation error detected: {_DTA_MIN.format(0)}"),
        (fifty_one, 400, "ValidationError", _TAG_LIMIT),
        ({**filler, "Tags.member.3.Key": "aws:x", "Tags.member.3.Value": "x"}, 403, "AccessDenied",
         _AWS_PREFIX),
    ]
    for i, (params, status, code, message) in enumerate(cases):
        name = f"query-tag-error-{i}-{_uuid_mod.uuid4().hex[:8]}"
        assert put(name, **params) == (status, code, message), i
        assert not _cw_alarm_exists(cw, name), i


def _cw_cbor_put(name, tags, **extra):
    """A raw CBOR PutMetricAlarm with query mode, for values boto3 can't send;
    returns (status, __type, message)."""
    import urllib.error
    import urllib.request

    import cbor2
    from conftest import ENDPOINT
    req = urllib.request.Request(
        f"{ENDPOINT}/service/GraniteServiceVersion20100801/operation/PutMetricAlarm",
        data=cbor2.dumps({"AlarmName": name, **_ALARM_BASE, "Tags": tags, **extra}),
        headers={"Content-Type": "application/cbor", "Accept": "application/cbor", "smithy-protocol": "rpc-v2-cbor",
                 "x-amzn-query-mode": "true",
                 "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/monitoring/aws4_request, "
                                  "SignedHeaders=host, Signature=x"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, None, None
    except urllib.error.HTTPError as e:
        payload = cbor2.loads(e.read())
        return e.code, payload.get("__type"), payload.get("message", payload.get("Message")) or ""


def test_cloudwatch_cbor_put_metric_alarm_wrong_types_in_tags(cw):
    """Over CBOR a wrong type is MalformedInput with no message, a null tag is
    a 500, and bytes count as a wrong type.
    AWS: captured 2026-10-21 (2309-uncertainties Z:cbor-qm-key-number,
    Z:cbor-qm-tags-not-a-list, Z:cbor-qm-null-element, Z:cbor-qm-key-bytes,
    Z:cbor-qm-value-bytes; x-amzn-query-error MalformedInput;Sender and
    InternalFailure;Receiver)."""
    for i, (tags, status, code) in enumerate([
        (_FILLER_TAGS + [{"Key": 5, "Value": "x"}], 400, "MalformedInput"),
        ("x", 400, "MalformedInput"),
        (_FILLER_TAGS + [None], 500, "InternalFailure"),
        (_FILLER_TAGS + [{"Key": b"ab", "Value": "x"}], 400, "MalformedInput"),
        (_FILLER_TAGS + [{"Key": "a", "Value": b"ab"}], 400, "MalformedInput"),
    ]):
        name = f"cbor-type-{i}-{_uuid_mod.uuid4().hex[:8]}"
        assert _cw_cbor_put(name, tags) == (status, code, ""), i
        assert not _cw_alarm_exists(cw, name), i


def test_cloudwatch_put_metric_alarm_lists_at_most_ten_validation_errors(cw):
    """AWS: captured 2026-10-21 (2309-uncertainties S:json-10-missing-values,
    S:json-11-missing-values, S:json-12-missing-values, S:json-5-missing-and-dta-0)."""
    for n in (10, 11, 12):
        name = f"ten-errors-{n}-{_uuid_mod.uuid4().hex[:8]}"
        message = "10 validation errors detected: " + "; ".join(_NULL_VALUE.format(i) for i in range(3, 13))
        assert _cw_json_put(name, _FILLER_TAGS + [{"Key": f"m{i}"} for i in range(n)]) == (400, message), n
    name = f"ten-errors-dta-{_uuid_mod.uuid4().hex[:8]}"
    assert _cw_json_put(name, _FILLER_TAGS + [{"Key": f"m{i}"} for i in range(10)], DatapointsToAlarm=0) == (
        400, "10 validation errors detected: " + "; ".join(_NULL_VALUE.format(i) for i in range(3, 13)))


def test_cloudwatch_put_metric_alarm_tag_character_and_utf16_length_rules(cw):
    """On create, keys and values may hold letters, numbers and spaces of any
    script plus _ . : / = + - @; length is counted in UTF-16 units. The check
    runs after the member constraints and datapointsToAlarm, before the tag
    limit, on the tags de-duplicated by key; a character error anywhere wins,
    then the first too-long value, then the first too-long key, in Java
    HashMap order of the keys; an update ignores it.
    AWS: captured 2026-10-21 (2309-uncertainties C:json-key-*, C:json-value-*,
    C:json-key-127-plus-emoji, C:json-key-hash-and-missing-value, C:json-key-hash-and-51,
    C:json-key-hash-and-dta-0, C:json-two-bad-char-keys, C:json-key-65-emoji-and-hash,
    C:json-update-key-hash, Z:json-key-128-e-acute, Z:json-key-129-e-acute,
    Z:json-key-64-emoji, Z:json-key-65-emoji, S:json-key-and-value-too-long-same-tag,
    S:json-value-too-long-and-key-bad-char-same-tag, S:json-too-long-then-bad-char-next-tag,
    S:query-key-hash; which too-long tag is named: M:json-two-too-long-keys,
    M:json-too-long-value-then-too-long-key, N:json-three-too-long-keys,
    N:json-two-too-long-keys-reversed-letters, N:json-too-long-key-then-too-long-value,
    L:json-keys-c-a-b, L:json-value-then-key-repeat-1, L:json-value-then-key-repeat-2,
    L:json-value-under-z-and-key-a, L:json-key-m-value-b-key-x, L:json-values-under-a-and-b,
    L:json-values-under-b-and-a, E:json-keys-a-and-e, E:json-keys-e-and-a, E:json-keys-a-d-e-q,
    E:json-values-under-i-and-x, E:json-values-under-x-and-i, E:json-values-under-k-m-z,
    E:json-dup-key-short-then-long-value, E:json-bad-char-value-next-to-long-key,
    E:json-dup-key-long-value-then-short, E:json-dup-key-bad-char-value-then-ok;
    past twelve keys: Z9:metric-13-keys-long-b-and-a)."""
    chars = ("Tags can only contain letters, numbers, spaces, and the following special characters: "
             "_ . : / = + - @")
    emoji = "\U0001F600"

    def too_long(text, kind):
        return f"'{text}' is not a valid value for com.amazon.aws.platform.billing.tagset.type.{kind} - it is too long"

    uid = _uuid_mod.uuid4().hex[:8]
    accepted = [("a b", "x"), ("é", "x"), ("123", "x"), ("_.:/=+-@", "x"), ("日本", "x"),
                ("a b", "x"), ("１", "x"), ("é" * 128, "x")]
    names = []
    try:
        for i, (key, value) in enumerate(accepted):
            names.append(f"chars-ok-{i}-{uid}")
            assert _cw_json_put(names[-1], _FILLER_TAGS + [{"Key": key, "Value": value}])[0] == 200, i
        refused = [
            ([{"Key": k, "Value": "x"}], chars) for k in ("a&b", "a<b", "a#b", emoji, "a\tb", "a,b", "a*b")
        ] + [
            ([{"Key": "a", "Value": v}], chars) for v in ("a&b", emoji, "a#b", emoji * 128)
        ] + [
            ([{"Key": "a", "Value": emoji * 129}], too_long(emoji * 129, "TagValue")),
            ([{"Key": "k" * 127 + emoji, "Value": "x"}], too_long("k" * 127 + emoji, "TagKey")),
            ([{"Key": emoji * 64, "Value": "x"}], chars),
            ([{"Key": emoji * 65, "Value": "x"}], too_long(emoji * 65, "TagKey")),
            ([{"Key": "k" * 127 + emoji, "Value": emoji * 129}], too_long("k" * 127 + emoji, "TagKey")),
            ([{"Key": "a#b", "Value": emoji * 129}], chars),
            ([{"Key": "k" * 127 + emoji, "Value": "x"}, {"Key": "a#b", "Value": "x"}], chars),
            ([{"Key": "a#b", "Value": "x"}, {"Key": emoji * 65, "Value": "x"}], chars),
            ([{"Key": "a" * 127 + emoji, "Value": "x"}, {"Key": "b" * 127 + emoji, "Value": "x"},
              {"Key": "c" * 127 + emoji, "Value": "x"}], too_long("c" * 127 + emoji, "TagKey")),
            ([{"Key": "c" * 127 + emoji, "Value": "x"}, {"Key": "a" * 127 + emoji, "Value": "x"},
              {"Key": "b" * 127 + emoji, "Value": "x"}], too_long("c" * 127 + emoji, "TagKey")),
            ([{"Key": "a" * 127 + emoji, "Value": "x"}, {"Key": "e" * 127 + emoji, "Value": "x"}],
             too_long("a" * 127 + emoji, "TagKey")),
            ([{"Key": "e" * 127 + emoji, "Value": "x"}, {"Key": "a" * 127 + emoji, "Value": "x"}],
             too_long("a" * 127 + emoji, "TagKey")),
            ([{"Key": c * 127 + emoji, "Value": "x"} for c in "adeq"], too_long("d" * 127 + emoji, "TagKey")),
            ([{"Key": "i", "Value": emoji * 129}, {"Key": "x", "Value": emoji * 130}], too_long(emoji * 130, "TagValue")),
            ([{"Key": "x", "Value": emoji * 129}, {"Key": "i", "Value": emoji * 130}], too_long(emoji * 129, "TagValue")),
            ([{"Key": "k", "Value": emoji * 129}, {"Key": "m", "Value": emoji * 130}, {"Key": "z", "Value": emoji * 131}],
             too_long(emoji * 131, "TagValue")),
            ([{"Key": "a", "Value": "x"}, {"Key": "a", "Value": emoji * 129}], too_long(emoji * 129, "TagValue")),
            ([{"Key": "a" * 127 + emoji, "Value": "x"}, {"Key": "b", "Value": "c#d"}], chars),
            ([{"Key": f"g{n:02d}", "Value": "v"} for n in range(9)] + [{"Key": "b" * 127 + emoji, "Value": "x"},
              {"Key": "a" * 127 + emoji, "Value": "x"}], too_long("a" * 127 + emoji, "TagKey")),
            ([{"Key": "a", "Value": emoji * 129}, {"Key": "b" * 127 + emoji, "Value": "x"}], too_long(emoji * 129, "TagValue")),
            ([{"Key": "m" * 127 + emoji, "Value": "x"}, {"Key": "b", "Value": emoji * 129},
              {"Key": "x" * 127 + emoji, "Value": "x"}], too_long(emoji * 129, "TagValue")),
            ([{"Key": "b", "Value": emoji * 129}, {"Key": "a", "Value": emoji * 130}], too_long(emoji * 130, "TagValue")),
            ([{"Key": "a#b", "Value": "x"}, {"Key": "c*d", "Value": "x"}], chars),
            ([{"Key": "a#b", "Value": "x"}] + [{"Key": f"k{n:02d}", "Value": "v"} for n in range(48)], chars),
            ([{"Key": "a#b", "Value": "x"}, {"Key": "c"}], f"1 validation error detected: {_NULL_VALUE.format(4)}"),
            ([{"Key": "é" * 129, "Value": "x"}],
             f"1 validation error detected: Value '{chr(0xe9) * 129}' at 'tags.3.member.key' failed to satisfy "
             "constraint: Member must have length less than or equal to 128"),
        ]
        for i, (tags, message) in enumerate(refused):
            name = f"chars-bad-{i}-{uid}"
            assert _cw_json_put(name, _FILLER_TAGS + tags) == (400, message), i
            assert not _cw_alarm_exists(cw, name), i
        for tags in ([{"Key": "a", "Value": emoji * 129}, {"Key": "a", "Value": "x"}],
                     [{"Key": "a", "Value": "a#b"}, {"Key": "a", "Value": "x"}]):
            names.append(f"chars-dup-{len(names)}-{uid}")
            assert _cw_json_put(names[-1], _FILLER_TAGS + tags)[0] == 200
        name = f"chars-dta-{uid}"
        assert _cw_json_put(name, _FILLER_TAGS + [{"Key": "a#b", "Value": "x"}], DatapointsToAlarm=0) == (
            400, f"1 validation error detected: {_DTA_MIN.format(0)}")

        names.append(f"chars-update-{uid}")
        assert _cw_json_put(names[-1], [{"Key": "team", "Value": "platform"}])[0] == 200
        assert _cw_json_put(names[-1], [{"Key": "a#b", "Value": "x"}], Threshold=87)[0] == 200
        alarm = cw.describe_alarms(AlarmNames=[names[-1]])["MetricAlarms"][0]
        assert alarm["Threshold"] == 87 and _cw_tag_set(cw, alarm["AlarmArn"]) == {"team": "platform"}

        status, body = _cw_raw("query", "PutMetricAlarm", {
            "AlarmName": f"chars-query-{uid}", **_QUERY_ALARM, "Tags.member.1.Key": "a#b", "Tags.member.1.Value": "x"})
        assert status == 400 and html.unescape(re.search(r"<Message>(.*?)</Message>", body).group(1)) == chars
    finally:
        cw.delete_alarms(AlarmNames=names)


def test_cloudwatch_query_put_metric_alarm_escapes_and_blank_values(cw):
    """An echoed key is XML-escaped in a Query error, and a member's Value sent
    with no "=" is an empty value.
    AWS: captured 2026-10-21 (2309-uncertainties Z:query-key-amp-lt-129, Z:query-bare-value)."""
    import urllib.error
    import urllib.parse
    import urllib.request

    from conftest import ENDPOINT
    auth = ("AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/monitoring/aws4_request, "
            "SignedHeaders=host, Signature=x")

    def put(name, tail):
        body = (f"Action=PutMetricAlarm&Version=2010-08-01&{urllib.parse.urlencode(_QUERY_ALARM)}"
                f"&AlarmName={name}{tail}").encode()
        req = urllib.request.Request(ENDPOINT, data=body, headers={
            "Content-Type": "application/x-www-form-urlencoded", "Authorization": auth})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, None, None
        except urllib.error.HTTPError as e:
            text = e.read().decode()
            return (e.code, re.search(r"<Code>(.*?)</Code>", text).group(1),
                    re.search(r"<Message>(.*?)</Message>", text, re.S).group(1))

    two = "&Tags.member.1.Key=f1&Tags.member.1.Value=v&Tags.member.2.Key=f2&Tags.member.2.Value=v"
    uid = _uuid_mod.uuid4().hex[:8]
    name = f"query-escape-{uid}"
    status, code, raw_message = put(name, two + "&Tags.member.3.Key=" + urllib.parse.quote("&<" + "k" * 127)
                                    + "&Tags.member.3.Value=x")
    assert (status, code) == (400, "ValidationError")
    assert raw_message.startswith("1 validation error detected: Value '&amp;&lt;kkk")
    assert html.unescape(raw_message) == (f"1 validation error detected: Value '&<{'k' * 127}' at 'tags.3.member.key' "
                                          "failed to satisfy constraint: Member must have length less than or equal to 128")
    assert not _cw_alarm_exists(cw, name)

    bare = f"query-bare-{uid}"
    try:
        assert put(bare, two + "&Tags.member.3.Key=a&Tags.member.3.Value")[0] == 200
        arn = cw.describe_alarms(AlarmNames=[bare])["MetricAlarms"][0]["AlarmArn"]
        assert _cw_tag_set(cw, arn) == {"f1": "v", "f2": "v", "a": ""}
    finally:
        cw.delete_alarms(AlarmNames=[bare])


def _cw_composite_base(cw, uid):
    name = f"composite-base-{uid}"
    cw.put_metric_alarm(AlarmName=name, **_ALARM_BASE)
    return name, f'ALARM("{name}")'


def test_cloudwatch_put_composite_alarm_applies_tags_on_create_only(cw):
    """PutCompositeAlarm stores Tags when it creates the alarm and ignores them
    on an update, whether the alarm had tags or not; a repeated key keeps its
    last value; an empty list or null stores none.
    AWS: captured 2026-10-21 (2309-uncertainties I11:put-composite-alarm-tags-on-create,
    I11:put-composite-alarm-update-different-tags) and 2026-10-22 (K:json-create-with-tags,
    K:json-update-other-tags, K:json-create-untagged, K:json-untagged-then-update-with-tags,
    K:json-dup-keys, K:json-tags-empty, K:json-tags-null, K:query-create-with-tags)."""
    uid = _uuid_mod.uuid4().hex[:8]
    base, rule = _cw_composite_base(cw, uid)
    names = [f"composite-{n}-{uid}" for n in ("tagged", "untagged", "dup", "empty", "null", "query")]

    def arn(name):
        return f"arn:aws:cloudwatch:us-east-1:000000000000:alarm:{name}"

    try:
        cw.put_composite_alarm(AlarmName=names[0], AlarmRule=rule, Tags=[{"Key": "team", "Value": "platform"}])
        assert _cw_tag_set(cw, arn(names[0])) == {"team": "platform"}
        cw.put_composite_alarm(AlarmName=names[0], AlarmRule=rule, Tags=[{"Key": "other", "Value": "x"}])
        assert _cw_tag_set(cw, arn(names[0])) == {"team": "platform"}

        cw.put_composite_alarm(AlarmName=names[1], AlarmRule=rule)
        cw.put_composite_alarm(AlarmName=names[1], AlarmRule=rule, Tags=[{"Key": "team", "Value": "platform"}])
        assert _cw_tag_set(cw, arn(names[1])) == {}

        cw.put_composite_alarm(AlarmName=names[2], AlarmRule=rule,
                               Tags=[{"Key": "d", "Value": "1"}, {"Key": "d", "Value": "2"}])
        assert _cw_tag_set(cw, arn(names[2])) == {"d": "2"}
        cw.put_composite_alarm(AlarmName=names[3], AlarmRule=rule, Tags=[])
        assert _cw_tag_set(cw, arn(names[3])) == {}
        assert _cw_raw("json", "PutCompositeAlarm", {"AlarmName": names[4], "AlarmRule": rule, "Tags": None})[0] == 200
        assert _cw_tag_set(cw, arn(names[4])) == {}
        assert _cw_raw("query", "PutCompositeAlarm", {"AlarmName": names[5], "AlarmRule": rule,
                                                      "Tags.member.1.Key": "team", "Tags.member.1.Value": ""})[0] == 200
        assert _cw_tag_set(cw, arn(names[5])) == {"team": ""}
    finally:
        for name in names:  # DeleteAlarms takes one composite alarm per call
            cw.delete_alarms(AlarmNames=[name])
        cw.delete_alarms(AlarmNames=[base])


def test_cloudwatch_put_composite_alarm_refuses_invalid_tags(cw):
    """PutCompositeAlarm checks Tags like PutMetricAlarm (types, member
    constraints, the tag-set check, then at most 50 tags), before its
    AlarmRule; an aws: key is a ValidationError after the tag-set check, not
    PutMetricAlarm's AccessDenied. An update checks the member constraints
    only. Nothing is created or changed on a refusal.
    AWS: captured 2026-10-22 (2309-uncertainties K:json-missing-value, K:json-missing-key,
    K:json-key-129, K:json-key-hash, K:json-value-129-emoji, K:json-51-tags, K:json-50-tags,
    K:json-aws-prefix, K:json-tags-string, K:json-key-number, K:json-bad-rule-and-missing-value,
    K:json-missing-child-and-missing-value, K:json-missing-child-and-aws-prefix,
    K:json-missing-child-and-51-tags, K:json-missing-child-and-key-hash,
    K:json-bad-rule-and-key-number, K:query-missing-value, K:cbor-missing-key,
    K:json-update-missing-key, K:json-update-aws-prefix, K:json-update-51-tags,
    K2:json-AWS-upper, K2:json-aws-vendor-key, K2:json-aws-colon-only,
    K2:json-aws-and-missing-value, K2:json-aws-and-key-hash, K2:json-key-hash-then-aws,
    K2:json-aws-and-51-tags, K2:json-aws-and-too-long-value, K2:json-aws-and-key-number,
    Z9:composite-AWS-x-then-aws-y, Z9:composite-aws-zz-then-aws-a-b, Z9:composite-Aws-q-then-AWS-b)."""
    uid = _uuid_mod.uuid4().hex[:8]
    base, rule = _cw_composite_base(cw, uid)
    emoji = "\U0001F600"
    many = [{"Key": f"k{n:02d}", "Value": "v"} for n in range(49)]
    chars = ("Tags can only contain letters, numbers, spaces, and the following special characters: "
             "_ . : / = + - @")
    missing = f'ALARM("composite-missing-{uid}")'

    def put(name, tags, alarm_rule=rule):
        status, body = _cw_raw("json", "PutCompositeAlarm", {"AlarmName": name, "AlarmRule": alarm_rule, "Tags": tags})
        if status < 300:
            return status, None
        payload = json.loads(body)
        return status, payload.get("message", payload.get("Message")) or ""

    cases = [
        (_FILLER_TAGS + [{"Key": "a"}], rule, (400, f"1 validation error detected: {_NULL_VALUE.format(3)}")),
        (_FILLER_TAGS + [{"Value": "x"}], rule, (400, f"1 validation error detected: {_NULL_KEY.format(3)}")),
        (_FILLER_TAGS + [{"Key": "k" * 129, "Value": "x"}], rule,
         (400, f"1 validation error detected: Value '{'k' * 129}' at 'tags.3.member.key' failed to satisfy constraint: "
               "Member must have length less than or equal to 128")),
        (_FILLER_TAGS + [{"Key": "a#b", "Value": "x"}], rule, (400, chars)),
        (_FILLER_TAGS + [{"Key": "a", "Value": emoji * 129}], rule,
         (400, f"'{emoji * 129}' is not a valid value for com.amazon.aws.platform.billing.tagset.type.TagValue - it is too long")),
        (_FILLER_TAGS + many, rule, (400, _TAG_LIMIT)),
        (_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}], rule, (400, "Invalid System Tag: aws:x is missing Vendor Id.")),
        (_FILLER_TAGS + [{"Key": "aws:", "Value": "x"}], rule, (400, "Invalid System Tag: aws: is missing Vendor Id.")),
        (_FILLER_TAGS + [{"Key": "aws:vendor:x", "Value": "x"}], rule,
         (400, "Invalid System Tag: aws:vendor:x is not whitelisted for vendor:cloudwatch")),
        (_FILLER_TAGS + [{"Key": "AWS:x", "Value": "x"}], rule,
         (400, 'Invalid System Tag: AWS:x must be prefixed with "aws:" in lowercase.')),
        ([{"Key": "AWS:x", "Value": "x"}, {"Key": "aws:y", "Value": "x"}], rule,
         (400, "Invalid System Tag: aws:y is missing Vendor Id.")),
        ([{"Key": "aws:zz", "Value": "x"}, {"Key": "aws:a:b", "Value": "x"}], rule,
         (400, "Invalid System Tag: aws:a:b is not whitelisted for vendor:cloudwatch")),
        ([{"Key": "Aws:q", "Value": "x"}, {"Key": "AWS:b", "Value": "x"}], rule,
         (400, 'Invalid System Tag: AWS:b must be prefixed with "aws:" in lowercase.')),
        (_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}, {"Key": "a"}], rule,
         (400, f"1 validation error detected: {_NULL_VALUE.format(4)}")),
        (_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}, {"Key": "a#b", "Value": "x"}], rule, (400, chars)),
        (_FILLER_TAGS + [{"Key": "a#b", "Value": "x"}, {"Key": "aws:x", "Value": "x"}], rule, (400, chars)),
        (_FILLER_TAGS + many[:48] + [{"Key": "aws:x", "Value": "x"}], rule,
         (400, "Invalid System Tag: aws:x is missing Vendor Id.")),
        (_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}, {"Key": "b", "Value": emoji * 129}], rule,
         (400, f"'{emoji * 129}' is not a valid value for com.amazon.aws.platform.billing.tagset.type.TagValue - it is too long")),
        ("x", rule, (400, "Unexpected field type")),
        (_FILLER_TAGS + [{"Key": 5, "Value": "x"}], rule, (400, "NUMBER_VALUE cannot be converted to String")),
        (_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}, {"Key": 5, "Value": "x"}], rule,
         (400, "NUMBER_VALUE cannot be converted to String")),
        (_FILLER_TAGS + [{"Key": "a"}], "NOT A RULE (((", (400, f"1 validation error detected: {_NULL_VALUE.format(3)}")),
        (_FILLER_TAGS + [{"Key": "a"}], missing, (400, f"1 validation error detected: {_NULL_VALUE.format(3)}")),
        (_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}], missing, (400, "Invalid System Tag: aws:x is missing Vendor Id.")),
        (_FILLER_TAGS + many, missing, (400, _TAG_LIMIT)),
        (_FILLER_TAGS + [{"Key": "a#b", "Value": "x"}], missing, (400, chars)),
        (_FILLER_TAGS + [{"Key": 5, "Value": "x"}], "NOT A RULE (((", (400, "NUMBER_VALUE cannot be converted to String")),
    ]
    for i, (tags, alarm_rule, expected) in enumerate(cases):
        name = f"composite-refused-{i}-{uid}"
        assert put(name, tags, alarm_rule) == expected, i
        assert not cw.describe_alarms(AlarmNames=[name], AlarmTypes=["CompositeAlarm"])["CompositeAlarms"], i

    status, body = _cw_raw("query", "PutCompositeAlarm", {"AlarmName": f"composite-q-{uid}", "AlarmRule": rule,
                                                          "Tags.member.1.Key": "f1", "Tags.member.1.Value": "v",
                                                          "Tags.member.2.Key": "f2", "Tags.member.2.Value": "v",
                                                          "Tags.member.3.Key": "a"})
    assert status == 400 and html.unescape(re.search(r"<Message>(.*?)</Message>", body).group(1)) == (
        f"1 validation error detected: {_NULL_VALUE.format(3)}")
    with pytest.raises(ClientError) as exc:
        _cw_unvalidated().put_composite_alarm(AlarmName=f"composite-c-{uid}", AlarmRule=rule,
                                              Tags=_FILLER_TAGS + [{"Value": "x"}])
    assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
        "ValidationError", f"1 validation error detected: {_NULL_KEY.format(3)}")

    existing = f"composite-update-{uid}"
    try:
        assert put(existing, [{"Key": "team", "Value": "platform"}])[0] == 200
        assert put(existing, _FILLER_TAGS + [{"Value": "x"}], f'OK("{base}")') == (
            400, f"1 validation error detected: {_NULL_KEY.format(3)}")
        alarm = cw.describe_alarms(AlarmNames=[existing], AlarmTypes=["CompositeAlarm"])["CompositeAlarms"][0]
        assert alarm["AlarmRule"] == rule
        for tags, new_rule in ((_FILLER_TAGS + [{"Key": "aws:x", "Value": "x"}], f'OK("{base}")'),
                               (_FILLER_TAGS + many, f'INSUFFICIENT_DATA("{base}")')):
            assert put(existing, tags, new_rule)[0] == 200
            alarm = cw.describe_alarms(AlarmNames=[existing], AlarmTypes=["CompositeAlarm"])["CompositeAlarms"][0]
            assert alarm["AlarmRule"] == new_rule
            assert _cw_tag_set(cw, alarm["AlarmArn"]) == {"team": "platform"}
    finally:
        cw.delete_alarms(AlarmNames=[existing])
        cw.delete_alarms(AlarmNames=[base])
