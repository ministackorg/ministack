import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

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
    cw.delete_alarms(AlarmNames=[child, composite])

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

