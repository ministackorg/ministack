import json
import time
import uuid as _uuid_mod

import pytest
from botocore.exceptions import ClientError


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
