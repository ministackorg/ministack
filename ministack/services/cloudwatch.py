"""
CloudWatch Metrics Service Emulator.
Supports legacy Query API (form-encoded), smithy-rpc-v2-cbor (botocore 1.42+),
and JSON/X-Amz-Target protocol.
Operations: PutMetricData, GetMetricStatistics, GetMetricData, ListMetrics,
            PutMetricAlarm, PutCompositeAlarm, DescribeAlarms, DescribeAlarmsForMetric,
            DescribeAlarmHistory, DeleteAlarms,
            EnableAlarmActions, DisableAlarmActions, SetAlarmState,
            TagResource, UntagResource, ListTagsForResource,
            PutDashboard, GetDashboard, DeleteDashboards, ListDashboards.
"""

import copy
import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import parse_qs

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import AccountScopedDict, get_account_id, get_region, new_uuid

logger = logging.getLogger("cloudwatch")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
TWO_WEEKS_SECONDS = 14 * 24 * 3600

# Per-tenant metric store — keyed by (namespace, metric_name, dims_key) per
# account via AccountScopedDict so GetMetricStatistics / ListMetrics from one
# account cannot see another account's data points.
_metrics = AccountScopedDict()
_alarms = AccountScopedDict()
_composite_alarms = AccountScopedDict()
# Alarm state-change history, per-account. Stored as AccountScopedDict under
# a single key "entries" so the standard list manipulation still applies to
# the caller's tenant only.
_alarm_history = AccountScopedDict()
_resource_tags = AccountScopedDict()
_dashboards = AccountScopedDict()  # dashboard_name -> {DashboardName, DashboardBody, LastModified}


def _metric_bucket(key: tuple) -> list:
    """Return the per-account point list for a metric key, creating it on first write."""
    pts = _metrics.get(key)
    if pts is None:
        pts = []
        _metrics[key] = pts
    return pts


def _history_entries() -> list:
    entries = _alarm_history.get("entries")
    if entries is None:
        entries = []
        _alarm_history["entries"] = entries
    return entries


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "metrics": copy.deepcopy(_metrics),
        "alarms": copy.deepcopy(_alarms),
        "composite_alarms": copy.deepcopy(_composite_alarms),
        "alarm_history": copy.deepcopy(_alarm_history),
        "dashboards": copy.deepcopy(_dashboards),
        "resource_tags": copy.deepcopy(_resource_tags),
    }


def restore_state(data):
    if data:
        _metrics.update(data.get("metrics", {}))
        _alarms.update(data.get("alarms", {}))
        _composite_alarms.update(data.get("composite_alarms", {}))
        _alarm_history.update(data.get("alarm_history", {}))
        _dashboards.update(data.get("dashboards", {}))
        _resource_tags.update(data.get("resource_tags", {}))


try:
    _restored = load_state("cloudwatch")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------


def _parse_ts(value):
    """Parse ISO-8601 string, epoch float, or None into a Unix timestamp."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            pass
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                dt = datetime.strptime(value, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp()
            except ValueError:
                continue
    return None


def _ts_iso(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Metric eviction
# ---------------------------------------------------------------------------


def _evict_old_metrics():
    cutoff = time.time() - TWO_WEEKS_SECONDS
    empty_keys = []
    for key, pts in _metrics.items():
        _metrics[key] = [p for p in pts if p["Timestamp"] >= cutoff]
        if not _metrics[key]:
            empty_keys.append(key)
    for key in empty_keys:
        del _metrics[key]


# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------


def _calc_stats(values):
    if not values:
        return {}
    return {
        "SampleCount": float(len(values)),
        "Sum": sum(values),
        "Average": sum(values) / len(values),
        "Minimum": min(values),
        "Maximum": max(values),
    }


def _stat_value(stats, stat_name):
    """Extract a single statistic from a stats dict. Handles pNN percentiles as Average."""
    if stat_name in stats:
        return stats[stat_name]
    if stat_name.startswith("p") and stat_name[1:].replace(".", "").isdigit():
        return stats.get("Average", 0)
    return stats.get("Average", 0)


# ---------------------------------------------------------------------------
# Alarm evaluation
# ---------------------------------------------------------------------------


def _evaluate_alarm(alarm):
    ns = alarm.get("Namespace")
    mn = alarm.get("MetricName")
    if not ns or not mn:
        return

    all_pts = []
    for (k_ns, k_mn, _), pts in _metrics.items():
        if k_ns == ns and k_mn == mn:
            all_pts.extend(pts)
    if not all_pts:
        return

    period = alarm.get("Period", 60)
    eval_periods = alarm.get("EvaluationPeriods", 1)
    cutoff = time.time() - period * eval_periods
    recent = [p for p in all_pts if p["Timestamp"] >= cutoff]
    if not recent:
        return

    stats = _calc_stats([p["Value"] for p in recent])
    val = _stat_value(stats, alarm.get("Statistic", "Average"))
    threshold = alarm.get("Threshold", 0)
    op = alarm.get("ComparisonOperator", "")

    cmp = {
        "GreaterThanOrEqualToThreshold": val >= threshold,
        "GreaterThanThreshold": val > threshold,
        "LessThanThreshold": val < threshold,
        "LessThanOrEqualToThreshold": val <= threshold,
        "GreaterThanUpperThreshold": val > threshold,
        "LessThanLowerThreshold": val < threshold,
        "LessThanLowerOrGreaterThanUpperThreshold": val < threshold,
    }
    breaching = cmp.get(op, False)
    old_state = alarm["StateValue"]
    new_state = "ALARM" if breaching else "OK"
    if old_state != new_state:
        reason = f"Threshold Crossed: {alarm.get('Statistic','Average')} {val} {op} {threshold}"
        alarm["StateValue"] = new_state
        alarm["StateReason"] = reason
        alarm["StateUpdatedTimestamp"] = int(time.time())
        _record_history(alarm["AlarmName"], old_state, new_state, reason)


def _evaluate_all_alarms():
    for alarm in _alarms.values():
        try:
            _evaluate_alarm(alarm)
        except Exception:
            logger.debug("alarm eval error", exc_info=True)


def _record_history(alarm_name, old_state, new_state, reason):
    _history_entries().append(
        {
            "AlarmName": alarm_name,
            "AlarmType": "MetricAlarm",
            "Timestamp": _ts_iso(time.time()),
            "HistoryItemType": "StateUpdate",
            "HistorySummary": f"Alarm updated from {old_state} to {new_state}",
            "HistoryData": json.dumps(
                {
                    "version": "1.0",
                    "oldState": {"stateValue": old_state},
                    "newState": {"stateValue": new_state, "stateReason": reason},
                }
            ),
        }
    )


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------


async def handle_request(method, path, headers, body, query_params):
    content_type = headers.get("content-type", "")
    target = headers.get("x-amz-target", "")
    is_cbor = "cbor" in content_type or "cbor" in headers.get("smithy-protocol", "")
    is_json = (not is_cbor) and ("json" in content_type or bool(target))

    params = dict(query_params)
    cbor_data = {}

    if body:
        if is_cbor:
            try:
                import cbor2

                cbor_data = cbor2.loads(body) or {}
            except Exception as e:
                logger.error("CBOR decode error: %s", e)
                cbor_data = {}
        elif is_json:
            try:
                cbor_data = json.loads(body) or {}
            except Exception as e:
                logger.error("JSON decode error: %s", e)
                cbor_data = {}
        else:
            for k, v in parse_qs(body.decode("utf-8", errors="replace")).items():
                params[k] = v

    action = ""
    if target and "." in target:
        action = target.split(".")[-1]
    if not action:
        m = re.search(r"/operation/([^/?]+)", path)
        if m:
            action = m.group(1)
    if not action:
        action = _p(params, "Action")

    _evict_old_metrics()

    handlers = {
        "PutMetricData": _put_metric_data,
        "GetMetricStatistics": _get_metric_statistics,
        "GetMetricData": _get_metric_data,
        "ListMetrics": _list_metrics,
        "PutMetricAlarm": _put_metric_alarm,
        "PutCompositeAlarm": _put_composite_alarm,
        "DescribeAlarms": _describe_alarms,
        "DescribeAlarmsForMetric": _describe_alarms_for_metric,
        "DescribeAlarmHistory": _describe_alarm_history,
        "DeleteAlarms": _delete_alarms,
        "EnableAlarmActions": _enable_alarm_actions,
        "DisableAlarmActions": _disable_alarm_actions,
        "SetAlarmState": _set_alarm_state,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsForResource": _list_tags_for_resource,
        "PutDashboard": _put_dashboard,
        "GetDashboard": _get_dashboard,
        "DeleteDashboards": _delete_dashboards,
        "ListDashboards": _list_dashboards,
    }

    handler = handlers.get(action)
    if not handler:
        return _error(
            "InvalidAction", f"Unknown action: {action}", 400, use_json=is_json, use_cbor=is_cbor
        )
    return handler(params, cbor_data, is_cbor, is_json)


# ---------------------------------------------------------------------------
# PutMetricData
# ---------------------------------------------------------------------------


def _put_metric_data(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        namespace = cbor_data.get("Namespace", "")
        for md in cbor_data.get("MetricData", []):
            mn = md.get("MetricName", "")
            dims = {d["Name"]: d["Value"] for d in md.get("Dimensions", [])}

            if "Values" in md:
                values = md["Values"]
                counts = md.get("Counts", [1.0] * len(values))
                for v, c in zip(values, counts):
                    for _ in range(int(c)):
                        _metric_bucket((namespace, mn, _dims_key(dims))).append(
                            {
                                "Timestamp": _parse_ts(md.get("Timestamp"))
                                or time.time(),
                                "Value": float(v),
                                "Unit": md.get("Unit", "None"),
                                "Dimensions": dims,
                            }
                        )
            elif "StatisticValues" in md:
                sv = md["StatisticValues"]
                _metric_bucket((namespace, mn, _dims_key(dims))).append(
                    {
                        "Timestamp": _parse_ts(md.get("Timestamp")) or time.time(),
                        "Value": sv.get("Sum", 0) / max(sv.get("SampleCount", 1), 1),
                        "Unit": md.get("Unit", "None"),
                        "Dimensions": dims,
                        "_stat": sv,
                    }
                )
            else:
                _metric_bucket((namespace, mn, _dims_key(dims))).append(
                    {
                        "Timestamp": _parse_ts(md.get("Timestamp")) or time.time(),
                        "Value": float(md.get("Value", 0)),
                        "Unit": md.get("Unit", "None"),
                        "Dimensions": dims,
                    }
                )
    else:
        namespace = _p(params, "Namespace")
        i = 1
        while _p(params, f"MetricData.member.{i}.MetricName"):
            mn = _p(params, f"MetricData.member.{i}.MetricName")
            value = float(_p(params, f"MetricData.member.{i}.Value") or "0")
            unit = _p(params, f"MetricData.member.{i}.Unit") or "None"
            ts_str = _p(params, f"MetricData.member.{i}.Timestamp")
            ts = _parse_ts(ts_str) if ts_str else time.time()
            dims = {}
            j = 1
            while _p(params, f"MetricData.member.{i}.Dimensions.member.{j}.Name"):
                dims[
                    _p(params, f"MetricData.member.{i}.Dimensions.member.{j}.Name")
                ] = _p(params, f"MetricData.member.{i}.Dimensions.member.{j}.Value")
                j += 1
            _metric_bucket((namespace, mn, _dims_key(dims))).append(
                {
                    "Timestamp": ts,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": dims,
                }
            )
            i += 1

    _evaluate_all_alarms()

    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "PutMetricDataResponse", "")


# ---------------------------------------------------------------------------
# ListMetrics
# ---------------------------------------------------------------------------


def _list_metrics(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        namespace = cbor_data.get("Namespace")
        metric_name = cbor_data.get("MetricName")
        req_dims = cbor_data.get("Dimensions")
    else:
        namespace = _p(params, "Namespace")
        metric_name = _p(params, "MetricName")
        req_dims = None

    seen = set()
    result = []
    for (ns, mn, dk), points in _metrics.items():
        if namespace and ns != namespace:
            continue
        if metric_name and mn != metric_name:
            continue
        key = (ns, mn, dk)
        if key in seen:
            continue
        seen.add(key)
        dims = [
            {"Name": k, "Value": v}
            for k, v in (points[0].get("Dimensions", {}) if points else {}).items()
        ]

        if req_dims:
            match = all(
                any(
                    d["Name"] == rd.get("Name")
                    and (not rd.get("Value") or d["Value"] == rd["Value"])
                    for d in dims
                )
                for rd in req_dims
            )
            if not match:
                continue

        result.append({"Namespace": ns, "MetricName": mn, "Dimensions": dims})

    if is_cbor:
        return _cbor_ok({"Metrics": result})
    if is_json:
        return _json_ok({"Metrics": result})

    members = ""
    for item in result:
        dims_xml = "".join(
            f"<member><Name>{d['Name']}</Name><Value>{d['Value']}</Value></member>"
            for d in item["Dimensions"]
        )
        members += (
            f"<member><Namespace>{item['Namespace']}</Namespace>"
            f"<MetricName>{item['MetricName']}</MetricName>"
            f"<Dimensions>{dims_xml}</Dimensions></member>"
        )
    return _xml(
        200,
        "ListMetricsResponse",
        f"<ListMetricsResult><Metrics>{members}</Metrics></ListMetricsResult>",
    )


# ---------------------------------------------------------------------------
# GetMetricStatistics — with time-range filtering + period aggregation
# ---------------------------------------------------------------------------


def _get_metric_statistics(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        namespace = cbor_data.get("Namespace")
        metric_name = cbor_data.get("MetricName")
        period = int(cbor_data.get("Period") or 60)
        start_time = _parse_ts(cbor_data.get("StartTime"))
        end_time = _parse_ts(cbor_data.get("EndTime"))
        req_stats = cbor_data.get("Statistics", [])
    else:
        namespace = _p(params, "Namespace")
        metric_name = _p(params, "MetricName")
        period = int(_p(params, "Period") or 60)
        start_time = _parse_ts(_p(params, "StartTime"))
        end_time = _parse_ts(_p(params, "EndTime"))
        req_stats = []
        si = 1
        while _p(params, f"Statistics.member.{si}"):
            req_stats.append(_p(params, f"Statistics.member.{si}"))
            si += 1

    if not req_stats:
        req_stats = ["SampleCount", "Sum", "Average", "Minimum", "Maximum"]

    all_points = []
    for (ns, mn, _), pts in _metrics.items():
        if ns == namespace and mn == metric_name:
            all_points.extend(pts)

    if start_time is not None:
        all_points = [p for p in all_points if p["Timestamp"] >= start_time]
    if end_time is not None:
        all_points = [p for p in all_points if p["Timestamp"] < end_time]

    buckets = defaultdict(list)
    for pt in all_points:
        bucket_ts = int(pt["Timestamp"] // period) * period
        buckets[bucket_ts].append(pt["Value"])

    datapoints = []
    for ts in sorted(buckets):
        vals = buckets[ts]
        stats = _calc_stats(vals)
        dp = {
            "Timestamp": _ts_iso(ts),
            "Unit": all_points[0]["Unit"] if all_points else "None",
        }
        for s in req_stats:
            if s in stats:
                dp[s] = stats[s]
        datapoints.append(dp)

    if is_cbor:
        return _cbor_ok({"Datapoints": datapoints, "Label": metric_name})
    if is_json:
        return _json_ok({"Datapoints": datapoints, "Label": metric_name})

    if not datapoints:
        return _xml(
            200,
            "GetMetricStatisticsResponse",
            f"<GetMetricStatisticsResult><Datapoints/><Label>{metric_name}</Label>"
            f"</GetMetricStatisticsResult>",
        )
    dps = ""
    for dp in datapoints:
        inner = f"<Timestamp>{dp['Timestamp']}</Timestamp>"
        for k, v in dp.items():
            if k not in ("Timestamp",):
                inner += f"<{k}>{v}</{k}>"
        dps += f"<member>{inner}</member>"
    return _xml(
        200,
        "GetMetricStatisticsResponse",
        f"<GetMetricStatisticsResult><Datapoints>{dps}</Datapoints>"
        f"<Label>{metric_name}</Label></GetMetricStatisticsResult>",
    )


# ---------------------------------------------------------------------------
# GetMetricData — modern multi-query API
# ---------------------------------------------------------------------------


def _get_metric_data(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        queries = cbor_data.get("MetricDataQueries", [])
        start_time = _parse_ts(cbor_data.get("StartTime"))
        end_time = _parse_ts(cbor_data.get("EndTime"))
    else:
        # Query protocol (form-encoded). Support the subset used by our tests.
        queries = []
        qi = 1
        while _p(params, f"MetricDataQueries.member.{qi}.Id"):
            qid = _p(params, f"MetricDataQueries.member.{qi}.Id")
            label = _p(params, f"MetricDataQueries.member.{qi}.Label") or qid
            return_data = _p(params, f"MetricDataQueries.member.{qi}.ReturnData")
            return_data = return_data != "false"
            ns = _p(
                params,
                f"MetricDataQueries.member.{qi}.MetricStat.Metric.Namespace",
            )
            mn = _p(
                params,
                f"MetricDataQueries.member.{qi}.MetricStat.Metric.MetricName",
            )
            period = int(
                _p(params, f"MetricDataQueries.member.{qi}.MetricStat.Period") or "60"
            )
            stat_name = (
                _p(params, f"MetricDataQueries.member.{qi}.MetricStat.Stat")
                or "Average"
            )
            queries.append(
                {
                    "Id": qid,
                    "Label": label,
                    "ReturnData": return_data,
                    "MetricStat": {
                        "Metric": {"Namespace": ns, "MetricName": mn},
                        "Period": period,
                        "Stat": stat_name,
                    },
                }
            )
            qi += 1

        start_time = _parse_ts(_p(params, "StartTime"))
        end_time = _parse_ts(_p(params, "EndTime"))

    results = []
    for q in queries:
        qid = q.get("Id", "")
        label = q.get("Label", qid)
        return_data = q.get("ReturnData", True)

        if q.get("Expression"):
            results.append(
                {
                    "Id": qid,
                    "Label": label,
                    "StatusCode": "InternalError",
                    "Messages": [
                        {"Code": "Unsupported", "Value": "Expressions not implemented"}
                    ],
                    "Timestamps": [],
                    "Values": [],
                }
            )
            continue

        ms = q.get("MetricStat", {})
        metric = ms.get("Metric", {})
        ns = metric.get("Namespace", "")
        mn = metric.get("MetricName", "")
        period = int(ms.get("Period", 60))
        stat_name = ms.get("Stat", "Average")

        all_pts = []
        for (k_ns, k_mn, _), pts in _metrics.items():
            if k_ns == ns and k_mn == mn:
                all_pts.extend(pts)

        if start_time is not None:
            all_pts = [p for p in all_pts if p["Timestamp"] >= start_time]
        if end_time is not None:
            all_pts = [p for p in all_pts if p["Timestamp"] < end_time]

        buckets = defaultdict(list)
        for pt in all_pts:
            buckets[int(pt["Timestamp"] // period) * period].append(pt["Value"])

        timestamps = []
        values = []
        for ts in sorted(buckets):
            stats = _calc_stats(buckets[ts])
            timestamps.append(_ts_iso(ts))
            values.append(_stat_value(stats, stat_name))

        if return_data:
            results.append(
                {
                    "Id": qid,
                    "Label": label,
                    "Timestamps": timestamps,
                    "Values": values,
                    "StatusCode": "Complete",
                }
            )

    if is_cbor:
        return _cbor_ok({"MetricDataResults": results})
    if is_json:
        return _json_ok({"MetricDataResults": results})

    members = ""
    for r in results:
        ts_members = "".join(f"<member>{t}</member>" for t in r.get("Timestamps", []))
        val_members = "".join(f"<member>{v}</member>" for v in r.get("Values", []))
        members += (
            "<member>"
            f"<Id>{r.get('Id','')}</Id>"
            f"<Label>{r.get('Label','')}</Label>"
            f"<StatusCode>{r.get('StatusCode','Complete')}</StatusCode>"
            f"<Timestamps>{ts_members}</Timestamps>"
            f"<Values>{val_members}</Values>"
            "</member>"
        )
    return _xml(
        200,
        "GetMetricDataResponse",
        f"<GetMetricDataResult><MetricDataResults>{members}</MetricDataResults></GetMetricDataResult>",
    )


# ---------------------------------------------------------------------------
# Alarms
# ---------------------------------------------------------------------------


def _put_metric_alarm(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        name = cbor_data.get("AlarmName", "")
        alarm = {
            "AlarmName": name,
            "AlarmArn": f"arn:aws:cloudwatch:{get_region()}:{get_account_id()}:alarm:{name}",
            "AlarmDescription": cbor_data.get("AlarmDescription", ""),
            "MetricName": cbor_data.get("MetricName"),
            "Namespace": cbor_data.get("Namespace"),
            "Statistic": cbor_data.get("Statistic", "Average"),
            "ExtendedStatistic": cbor_data.get("ExtendedStatistic"),
            "Period": int(cbor_data.get("Period", 60)),
            "EvaluationPeriods": int(cbor_data.get("EvaluationPeriods", 1)),
            "DatapointsToAlarm": int(
                cbor_data.get("DatapointsToAlarm")
                or cbor_data.get("EvaluationPeriods", 1)
            ),
            "Threshold": float(cbor_data.get("Threshold", 0)),
            "ComparisonOperator": cbor_data.get("ComparisonOperator"),
            "TreatMissingData": cbor_data.get("TreatMissingData", "missing"),
            "StateValue": _alarms[name]["StateValue"]
            if name in _alarms
            else "INSUFFICIENT_DATA",
            "StateReason": _alarms[name]["StateReason"]
            if name in _alarms
            else "Unchecked: Initial alarm creation",
            "StateUpdatedTimestamp": int(time.time()),
            "ActionsEnabled": cbor_data.get("ActionsEnabled", True),
            "AlarmActions": cbor_data.get("AlarmActions", []),
            "OKActions": cbor_data.get("OKActions", []),
            "InsufficientDataActions": cbor_data.get("InsufficientDataActions", []),
            "Dimensions": cbor_data.get("Dimensions", []),
            "Unit": cbor_data.get("Unit"),
            "AlarmConfigurationUpdatedTimestamp": int(time.time()),
        }
    else:
        name = _p(params, "AlarmName")
        dims = []
        di = 1
        while _p(params, f"Dimensions.member.{di}.Name"):
            dims.append(
                {
                    "Name": _p(params, f"Dimensions.member.{di}.Name"),
                    "Value": _p(params, f"Dimensions.member.{di}.Value"),
                }
            )
            di += 1
        alarm_actions = []
        ai = 1
        while _p(params, f"AlarmActions.member.{ai}"):
            alarm_actions.append(_p(params, f"AlarmActions.member.{ai}"))
            ai += 1
        ok_actions = []
        oi = 1
        while _p(params, f"OKActions.member.{oi}"):
            ok_actions.append(_p(params, f"OKActions.member.{oi}"))
            oi += 1
        alarm = {
            "AlarmName": name,
            "AlarmArn": f"arn:aws:cloudwatch:{get_region()}:{get_account_id()}:alarm:{name}",
            "AlarmDescription": _p(params, "AlarmDescription"),
            "MetricName": _p(params, "MetricName"),
            "Namespace": _p(params, "Namespace"),
            "Statistic": _p(params, "Statistic") or "Average",
            "ExtendedStatistic": _p(params, "ExtendedStatistic") or None,
            "Period": int(_p(params, "Period") or "60"),
            "EvaluationPeriods": int(_p(params, "EvaluationPeriods") or "1"),
            "DatapointsToAlarm": int(
                _p(params, "DatapointsToAlarm")
                or _p(params, "EvaluationPeriods")
                or "1"
            ),
            "Threshold": float(_p(params, "Threshold") or "0"),
            "ComparisonOperator": _p(params, "ComparisonOperator"),
            "TreatMissingData": _p(params, "TreatMissingData") or "missing",
            "StateValue": _alarms[name]["StateValue"]
            if name in _alarms
            else "INSUFFICIENT_DATA",
            "StateReason": _alarms[name]["StateReason"]
            if name in _alarms
            else "Unchecked: Initial alarm creation",
            "StateUpdatedTimestamp": int(time.time()),
            "ActionsEnabled": _p(params, "ActionsEnabled") != "false",
            "AlarmActions": alarm_actions,
            "OKActions": ok_actions,
            "InsufficientDataActions": [],
            "Dimensions": dims,
            "Unit": _p(params, "Unit") or None,
            "AlarmConfigurationUpdatedTimestamp": int(time.time()),
        }

    is_new = name not in _alarms
    _alarms[name] = alarm

    if is_new:
        _record_history(
            name,
            "INSUFFICIENT_DATA",
            "INSUFFICIENT_DATA",
            "Unchecked: Initial alarm creation",
        )

    _evaluate_alarm(alarm)

    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "PutMetricAlarmResponse", "")


def _put_composite_alarm(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        name = cbor_data.get("AlarmName", "")
        alarm_rule = cbor_data.get("AlarmRule", "")
        desc = cbor_data.get("AlarmDescription", "")
        actions_enabled = cbor_data.get("ActionsEnabled", True)
        alarm_actions = cbor_data.get("AlarmActions", [])
        ok_actions = cbor_data.get("OKActions", [])
        insuff_actions = cbor_data.get("InsufficientDataActions", [])
    else:
        name = _p(params, "AlarmName")
        alarm_rule = _p(params, "AlarmRule")
        desc = _p(params, "AlarmDescription")
        actions_enabled = _p(params, "ActionsEnabled") != "false"
        alarm_actions = []
        ai = 1
        while _p(params, f"AlarmActions.member.{ai}"):
            alarm_actions.append(_p(params, f"AlarmActions.member.{ai}"))
            ai += 1
        ok_actions = []
        oi = 1
        while _p(params, f"OKActions.member.{oi}"):
            ok_actions.append(_p(params, f"OKActions.member.{oi}"))
            oi += 1
        insuff_actions = []
        ii = 1
        while _p(params, f"InsufficientDataActions.member.{ii}"):
            insuff_actions.append(_p(params, f"InsufficientDataActions.member.{ii}"))
            ii += 1

    _composite_alarms[name] = {
        "AlarmName": name,
        "AlarmArn": f"arn:aws:cloudwatch:{get_region()}:{get_account_id()}:alarm:{name}",
        "AlarmDescription": desc,
        "AlarmRule": alarm_rule,
        "StateValue": "INSUFFICIENT_DATA",
        "StateReason": "Unchecked: Initial alarm creation",
        "StateUpdatedTimestamp": int(time.time()),
        "ActionsEnabled": actions_enabled,
        "AlarmActions": alarm_actions,
        "OKActions": ok_actions,
        "InsufficientDataActions": insuff_actions,
        "AlarmConfigurationUpdatedTimestamp": int(time.time()),
    }
    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "PutCompositeAlarmResponse", "<PutCompositeAlarmResult/>")


# ---------------------------------------------------------------------------
# DescribeAlarms
# ---------------------------------------------------------------------------


def _describe_alarms(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        names = cbor_data.get("AlarmNames", [])
        prefix = cbor_data.get("AlarmNamePrefix")
        state = cbor_data.get("StateValue")
        alarm_types = cbor_data.get("AlarmTypes", ["MetricAlarm", "CompositeAlarm"])
        max_records = cbor_data.get("MaxRecords", 100)
    else:
        names = [
            _p(params, f"AlarmNames.member.{i}")
            for i in range(1, 101)
            if _p(params, f"AlarmNames.member.{i}")
        ]
        prefix = _p(params, "AlarmNamePrefix")
        state = _p(params, "StateValue")
        alarm_types = [
            _p(params, f"AlarmTypes.member.{i}")
            for i in range(1, 11)
            if _p(params, f"AlarmTypes.member.{i}")
        ] or ["MetricAlarm", "CompositeAlarm"]
        max_records = int(_p(params, "MaxRecords") or "100")

    metric_alarms = []
    composite_results = []

    if "MetricAlarm" in alarm_types:
        for aname, alarm in _alarms.items():
            if names and aname not in names:
                continue
            if prefix and not aname.startswith(prefix):
                continue
            if state and alarm["StateValue"] != state:
                continue
            metric_alarms.append(alarm)

    if "CompositeAlarm" in alarm_types:
        for aname, alarm in _composite_alarms.items():
            if names and aname not in names:
                continue
            if prefix and not aname.startswith(prefix):
                continue
            if state and alarm["StateValue"] != state:
                continue
            composite_results.append(alarm)

    metric_alarms = metric_alarms[:max_records]

    if is_cbor:
        return _cbor_ok(
            {"MetricAlarms": metric_alarms, "CompositeAlarms": composite_results}
        )
    if is_json:
        return _json_ok(
            {"MetricAlarms": metric_alarms, "CompositeAlarms": composite_results}
        )

    metric_members = "".join(
        f"<member><AlarmName>{a['AlarmName']}</AlarmName><AlarmArn>{a['AlarmArn']}</AlarmArn>"
        f"<StateValue>{a['StateValue']}</StateValue><MetricName>{a.get('MetricName','')}</MetricName>"
        f"<Namespace>{a.get('Namespace','')}</Namespace><Threshold>{a.get('Threshold','')}</Threshold>"
        f"<ComparisonOperator>{a.get('ComparisonOperator','')}</ComparisonOperator>"
        f"<EvaluationPeriods>{a.get('EvaluationPeriods','')}</EvaluationPeriods>"
        f"<StateReason>{a.get('StateReason','')}</StateReason>"
        f"</member>"
        for a in metric_alarms
    )
    comp_members = "".join(
        f"<member><AlarmName>{a['AlarmName']}</AlarmName><AlarmArn>{a['AlarmArn']}</AlarmArn>"
        f"<AlarmRule>{a.get('AlarmRule','')}</AlarmRule>"
        f"<StateValue>{a.get('StateValue','')}</StateValue>"
        f"<StateReason>{a.get('StateReason','')}</StateReason>"
        f"</member>"
        for a in composite_results
    )
    return _xml(
        200,
        "DescribeAlarmsResponse",
        f"<DescribeAlarmsResult>"
        f"<MetricAlarms>{metric_members}</MetricAlarms>"
        f"<CompositeAlarms>{comp_members}</CompositeAlarms>"
        f"</DescribeAlarmsResult>",
    )


# ---------------------------------------------------------------------------
# DescribeAlarmsForMetric
# ---------------------------------------------------------------------------


def _describe_alarms_for_metric(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        namespace = cbor_data.get("Namespace", "")
        metric_name = cbor_data.get("MetricName", "")
    else:
        namespace = _p(params, "Namespace")
        metric_name = _p(params, "MetricName")

    result = [
        a
        for a in _alarms.values()
        if a.get("Namespace") == namespace and a.get("MetricName") == metric_name
    ]

    if is_cbor:
        return _cbor_ok({"MetricAlarms": result})
    if is_json:
        return _json_ok({"MetricAlarms": result})

    members = "".join(
        f"<member><AlarmName>{a['AlarmName']}</AlarmName><AlarmArn>{a['AlarmArn']}</AlarmArn>"
        f"<StateValue>{a['StateValue']}</StateValue></member>"
        for a in result
    )
    return _xml(
        200,
        "DescribeAlarmsForMetricResponse",
        f"<DescribeAlarmsForMetricResult><MetricAlarms>{members}</MetricAlarms>"
        f"</DescribeAlarmsForMetricResult>",
    )


# ---------------------------------------------------------------------------
# DescribeAlarmHistory
# ---------------------------------------------------------------------------


def _describe_alarm_history(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        alarm_name = cbor_data.get("AlarmName")
        history_type = cbor_data.get("HistoryItemType")
        start_date = _parse_ts(cbor_data.get("StartDate"))
        end_date = _parse_ts(cbor_data.get("EndDate"))
        max_records = int(cbor_data.get("MaxRecords", 100))
    else:
        alarm_name = _p(params, "AlarmName")
        history_type = _p(params, "HistoryItemType")
        start_date = _parse_ts(_p(params, "StartDate"))
        end_date = _parse_ts(_p(params, "EndDate"))
        max_records = int(_p(params, "MaxRecords") or "100")

    items = list(_history_entries())
    if alarm_name:
        items = [h for h in items if h["AlarmName"] == alarm_name]
    if history_type:
        items = [h for h in items if h["HistoryItemType"] == history_type]
    if start_date is not None:
        items = [h for h in items if _parse_ts(h["Timestamp"]) >= start_date]
    if end_date is not None:
        items = [h for h in items if _parse_ts(h["Timestamp"]) <= end_date]
    items = items[:max_records]

    if is_cbor:
        return _cbor_ok({"AlarmHistoryItems": items})
    if is_json:
        return _json_ok({"AlarmHistoryItems": items})

    members = "".join(
        f"<member><AlarmName>{h['AlarmName']}</AlarmName>"
        f"<Timestamp>{h['Timestamp']}</Timestamp>"
        f"<HistoryItemType>{h['HistoryItemType']}</HistoryItemType>"
        f"<HistorySummary>{h['HistorySummary']}</HistorySummary></member>"
        for h in items
    )
    return _xml(
        200,
        "DescribeAlarmHistoryResponse",
        f"<DescribeAlarmHistoryResult><AlarmHistoryItems>{members}</AlarmHistoryItems>"
        f"</DescribeAlarmHistoryResult>",
    )


# ---------------------------------------------------------------------------
# DeleteAlarms / Enable / Disable
# ---------------------------------------------------------------------------


def _delete_alarms(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        names = cbor_data.get("AlarmNames", [])
    else:
        names = []
        i = 1
        while _p(params, f"AlarmNames.member.{i}"):
            names.append(_p(params, f"AlarmNames.member.{i}"))
            i += 1

    for n in names:
        _alarms.pop(n, None)
        _composite_alarms.pop(n, None)
        _resource_tags.pop(f"arn:aws:cloudwatch:{get_region()}:{get_account_id()}:alarm:{n}", None)

    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "DeleteAlarmsResponse", "")


def _enable_alarm_actions(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        names = cbor_data.get("AlarmNames", [])
    else:
        names = [
            _p(params, f"AlarmNames.member.{i}")
            for i in range(1, 101)
            if _p(params, f"AlarmNames.member.{i}")
        ]
    for n in names:
        if n in _alarms:
            _alarms[n]["ActionsEnabled"] = True
        if n in _composite_alarms:
            _composite_alarms[n]["ActionsEnabled"] = True
    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "EnableAlarmActionsResponse", "")


def _disable_alarm_actions(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        names = cbor_data.get("AlarmNames", [])
    else:
        names = [
            _p(params, f"AlarmNames.member.{i}")
            for i in range(1, 101)
            if _p(params, f"AlarmNames.member.{i}")
        ]
    for n in names:
        if n in _alarms:
            _alarms[n]["ActionsEnabled"] = False
        if n in _composite_alarms:
            _composite_alarms[n]["ActionsEnabled"] = False
    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "DisableAlarmActionsResponse", "")


# ---------------------------------------------------------------------------
# SetAlarmState — with history recording
# ---------------------------------------------------------------------------


def _set_alarm_state(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        name = cbor_data.get("AlarmName", "")
        new_state = cbor_data.get("StateValue", "")
        reason = cbor_data.get("StateReason", "")
        reason_data = cbor_data.get("StateReasonData", "")
    else:
        name = _p(params, "AlarmName")
        new_state = _p(params, "StateValue")
        reason = _p(params, "StateReason")
        reason_data = _p(params, "StateReasonData")

    alarm = _alarms.get(name) or _composite_alarms.get(name)
    if not alarm:
        return _error(
            "ResourceNotFound", f"Alarm {name} not found", 404, use_json=is_json, use_cbor=is_cbor
        )

    old_state = alarm["StateValue"]
    alarm["StateValue"] = new_state
    alarm["StateReason"] = reason
    if reason_data:
        alarm["StateReasonData"] = reason_data
    alarm["StateUpdatedTimestamp"] = int(time.time())

    if old_state != new_state:
        _record_history(name, old_state, new_state, reason)

    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "SetAlarmStateResponse", "")


# ---------------------------------------------------------------------------
# TagResource / UntagResource / ListTagsForResource
# ---------------------------------------------------------------------------


def _tag_resource(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        arn = cbor_data.get("ResourceARN", "")
        tags = cbor_data.get("Tags", [])
    else:
        arn = _p(params, "ResourceARN")
        tags = []
        i = 1
        while _p(params, f"Tags.member.{i}.Key"):
            tags.append(
                {
                    "Key": _p(params, f"Tags.member.{i}.Key"),
                    "Value": _p(params, f"Tags.member.{i}.Value"),
                }
            )
            i += 1

    if arn not in _resource_tags:
        _resource_tags[arn] = {}
    for t in tags:
        _resource_tags[arn][t["Key"]] = t.get("Value", "")

    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "TagResourceResponse", "<TagResourceResult/>")


def _untag_resource(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        arn = cbor_data.get("ResourceARN", "")
        keys = cbor_data.get("TagKeys", [])
    else:
        arn = _p(params, "ResourceARN")
        keys = []
        i = 1
        while _p(params, f"TagKeys.member.{i}"):
            keys.append(_p(params, f"TagKeys.member.{i}"))
            i += 1

    tag_map = _resource_tags.get(arn, {})
    for k in keys:
        tag_map.pop(k, None)

    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "UntagResourceResponse", "<UntagResourceResult/>")


def _list_tags_for_resource(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        arn = cbor_data.get("ResourceARN", "")
    else:
        arn = _p(params, "ResourceARN")

    tags = [{"Key": k, "Value": v} for k, v in _resource_tags.get(arn, {}).items()]

    if is_cbor:
        return _cbor_ok({"Tags": tags})
    if is_json:
        return _json_ok({"Tags": tags})

    members = "".join(
        f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
        for t in tags
    )
    return _xml(
        200,
        "ListTagsForResourceResponse",
        f"<ListTagsForResourceResult><Tags>{members}</Tags></ListTagsForResourceResult>",
    )


# ---------------------------------------------------------------------------
# Dashboards
# ---------------------------------------------------------------------------


def _put_dashboard(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        name = cbor_data.get("DashboardName", "")
        body = cbor_data.get("DashboardBody", "")
    else:
        name = _p(params, "DashboardName")
        body = _p(params, "DashboardBody")

    if not name:
        return _error(
            "InvalidParameterValue",
            "DashboardName is required",
            400,
            use_json=is_json, use_cbor=is_cbor,
        )

    _dashboards[name] = {
        "DashboardName": name,
        "DashboardBody": body,
        "DashboardArn": f"arn:aws:cloudwatch::{get_account_id()}:dashboard/{name}",
        "LastModified": int(time.time()),
        "Size": len(body),
    }

    if is_cbor:
        return _cbor_ok({"DashboardValidationMessages": []})
    if is_json:
        return _json_ok({"DashboardValidationMessages": []})
    return _xml(
        200,
        "PutDashboardResponse",
        "<PutDashboardResult><DashboardValidationMessages/></PutDashboardResult>",
    )


def _get_dashboard(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        name = cbor_data.get("DashboardName", "")
    else:
        name = _p(params, "DashboardName")

    dash = _dashboards.get(name)
    if not dash:
        return _error(
            "ResourceNotFound",
            f"Dashboard {name} does not exist",
            404,
            use_json=is_json, use_cbor=is_cbor,
        )

    if is_cbor:
        return _cbor_ok(
            {
                "DashboardArn": dash["DashboardArn"],
                "DashboardBody": dash["DashboardBody"],
                "DashboardName": dash["DashboardName"],
            }
        )
    if is_json:
        return _json_ok(
            {
                "DashboardArn": dash["DashboardArn"],
                "DashboardBody": dash["DashboardBody"],
                "DashboardName": dash["DashboardName"],
            }
        )
    return _xml(
        200,
        "GetDashboardResponse",
        f"<GetDashboardResult>"
        f"<DashboardArn>{dash['DashboardArn']}</DashboardArn>"
        f"<DashboardBody>{dash['DashboardBody']}</DashboardBody>"
        f"<DashboardName>{dash['DashboardName']}</DashboardName>"
        f"</GetDashboardResult>",
    )


def _delete_dashboards(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        names = cbor_data.get("DashboardNames", [])
    else:
        names = []
        i = 1
        while _p(params, f"DashboardNames.member.{i}"):
            names.append(_p(params, f"DashboardNames.member.{i}"))
            i += 1

    missing = [n for n in names if n not in _dashboards]
    if missing:
        return _error("DashboardNotFoundError",
                       f"Dashboard {', '.join(missing)} does not exist",
                       404, use_json=is_json, use_cbor=is_cbor)

    for n in names:
        _dashboards.pop(n, None)

    if is_cbor:
        return _cbor_ok({})
    if is_json:
        return _json_ok({})
    return _xml(200, "DeleteDashboardsResponse", "<DeleteDashboardsResult/>")


def _list_dashboards(params, cbor_data, is_cbor, is_json=False):
    if is_cbor or is_json:
        prefix = cbor_data.get("DashboardNamePrefix", "")
    else:
        prefix = _p(params, "DashboardNamePrefix")

    entries = []
    for name in sorted(_dashboards):
        if prefix and not name.startswith(prefix):
            continue
        dash = _dashboards[name]
        entries.append(
            {
                "DashboardName": dash["DashboardName"],
                "DashboardArn": dash["DashboardArn"],
                "Size": dash["Size"],
                "LastModified": _ts_iso(dash["LastModified"]),
            }
        )

    if is_cbor:
        return _cbor_ok({"DashboardEntries": entries})
    if is_json:
        return _json_ok({"DashboardEntries": entries})

    members = ""
    for e in entries:
        members += (
            f"<member>"
            f"<DashboardName>{e['DashboardName']}</DashboardName>"
            f"<DashboardArn>{e['DashboardArn']}</DashboardArn>"
            f"<Size>{e['Size']}</Size>"
            f"<LastModified>{e['LastModified']}</LastModified>"
            f"</member>"
        )
    return _xml(
        200,
        "ListDashboardsResponse",
        f"<ListDashboardsResult><DashboardEntries>{members}</DashboardEntries></ListDashboardsResult>",
    )


# ---------------------------------------------------------------------------
# Protocol / encoding helpers
# ---------------------------------------------------------------------------


def _dims_key(dims: dict) -> str:
    return "|".join(f"{k}={v}" for k, v in sorted(dims.items()))


def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _cbor_ok(data: dict):
    try:
        import cbor2

        body = cbor2.dumps(data)
    except Exception:
        body = json.dumps(data).encode()
    return (
        200,
        {"Content-Type": "application/cbor", "smithy-protocol": "rpc-v2-cbor"},
        body,
    )


def _json_ok(data: dict):
    return 200, {"Content-Type": "application/json"}, json.dumps(data).encode()


def _xml(status, root_tag, inner):
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<{root_tag} xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">\n'
        f"    {inner}\n"
        f"    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>\n"
        f"</{root_tag}>"
    ).encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status, use_json=False, use_cbor=False):
    if use_cbor:
        try:
            import cbor2
            body = cbor2.dumps({"__type": code, "message": message})
            return status, {"Content-Type": "application/cbor", "smithy-protocol": "rpc-v2-cbor"}, body
        except ImportError:
            pass
    if use_json or use_cbor:
        return (
            status,
            {"Content-Type": "application/x-amz-json-1.0"},
            json.dumps({"__type": code, "message": message}).encode(),
        )
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<ErrorResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">\n'
        f"    <Error><Code>{code}</Code><Message>{message}</Message></Error>\n"
        f"    <RequestId>{new_uuid()}</RequestId>\n"
        f"</ErrorResponse>"
    ).encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


# ---------------------------------------------------------------------------
# CloudFormation integration
# ---------------------------------------------------------------------------


def cloudformation_put_metric_alarm(alarm: dict) -> None:
    """Store a PutMetricAlarm-shaped alarm dict (CloudFormation create/update)."""
    name = alarm["AlarmName"]
    is_new = name not in _alarms
    _alarms[name] = alarm
    if is_new:
        _record_history(
            name,
            "INSUFFICIENT_DATA",
            "INSUFFICIENT_DATA",
            "Unchecked: Initial alarm creation",
        )
    _evaluate_alarm(alarm)


def cloudformation_delete_metric_alarm(name: str) -> None:
    """Remove a metric alarm created from a template (not composite alarms)."""
    _alarms.pop(name, None)
    _resource_tags.pop(
        f"arn:aws:cloudwatch:{get_region()}:{get_account_id()}:alarm:{name}", None
    )


def reset():
    _alarms.clear()
    _composite_alarms.clear()
    _alarm_history.clear()
    _metrics.clear()
    _resource_tags.clear()
    _dashboards.clear()
