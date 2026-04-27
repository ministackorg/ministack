"""
Regression tests for the CloudWatch Logs persistence drops.

Three module-level AccountScopedDicts are mutated by public APIs but
were missing from `get_state()` / `restore_state()`:

  - cloudwatch_logs._destinations      (PutDestination)
  - cloudwatch_logs._metric_filters    (PutMetricFilter)
  - cloudwatch_logs._queries           (StartQuery)

Plus a follow-on consistency bug:

  - Subscription filters live inside _log_groups (persisted) but
    reference destination ARNs in _destinations (not persisted).
    After warm-boot you get split-brain: the filter on the log group
    still references a destination that no longer exists in
    _destinations.

Each test exercises the FULL warm-boot path:
  1. populate the in-memory dict
  2. `get_state()` snapshot
  3. `persistence.save_state()` → JSON-encode to a tmp `STATE_DIR`
  4. `mod.reset()` (simulate process restart)
  5. `persistence.load_state()` → JSON-decode from disk
  6. `restore_state(loaded)`

Going through `save_state` / `load_state` (rather than just calling
get_state / restore_state in-memory) is what catches encoder /
decoder regressions — most notably the tuple-key path used by
`_metric_filters`, which round-trips through repr → JSON string →
ast.literal_eval in `core/persistence.py::_json_default` /
`_json_object_hook`.
"""
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
        "by AccountScopedDict's JSON encoder hook."
    )
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
