"""
Unit tests for the AWS Config service module (ministack.services.config).

These call the handler directly (no live server / router needed), matching the
direct-dispatch style so they run before the router is wired. Each test sets the
request account + region contextvars, then invokes handle_request with a
JSON-RPC X-Amz-Target header and asserts botocore-verified AWS shapes.
"""

import asyncio
import json

import pytest

from ministack.core.responses import set_request_account_id, set_request_region
from ministack.services import config as config_svc

TARGET_PREFIX = "StarlingDoveService"


def _call(action, payload):
    set_request_account_id("test")  # -> resolves to a stable 12-digit account
    set_request_region("us-east-1")
    headers = {"x-amz-target": f"{TARGET_PREFIX}.{action}"}
    body = json.dumps(payload).encode("utf-8")
    status, resp_headers, resp_body = asyncio.run(
        config_svc.handle_request("POST", "/", headers, body, {})
    )
    parsed = json.loads(resp_body) if resp_body else {}
    return status, parsed


@pytest.fixture(autouse=True)
def _reset():
    set_request_account_id("test")
    set_request_region("us-east-1")
    config_svc.reset()
    yield
    config_svc.reset()


# ── Config Rules ──────────────────────────────────────────────

def test_put_and_describe_config_rule_roundtrip():
    status, _ = _call("PutConfigRule", {
        "ConfigRule": {
            "ConfigRuleName": "s3-bucket-versioning-enabled",
            "Source": {
                "Owner": "AWS",
                "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
            },
        }
    })
    assert status == 200

    status, out = _call("DescribeConfigRules", {})
    assert status == 200
    assert "ConfigRules" in out
    rules = out["ConfigRules"]
    assert len(rules) == 1
    rule = rules[0]
    assert rule["ConfigRuleName"] == "s3-bucket-versioning-enabled"
    assert rule["ConfigRuleArn"].startswith("arn:aws:config:us-east-1:")
    assert ":config-rule/" in rule["ConfigRuleArn"]
    assert rule["ConfigRuleId"]
    assert rule["ConfigRuleState"] == "ACTIVE"
    assert rule["Source"]["Owner"] == "AWS"


def test_describe_config_rules_empty():
    status, out = _call("DescribeConfigRules", {})
    assert status == 200
    assert out == {"ConfigRules": []}


def test_describe_config_rules_unknown_name_errors():
    status, out = _call("DescribeConfigRules", {"ConfigRuleNames": ["nope"]})
    assert status == 400
    assert out["__type"] == "NoSuchConfigRuleException"


def test_delete_config_rule():
    _call("PutConfigRule", {
        "ConfigRule": {
            "ConfigRuleName": "r1",
            "Source": {"Owner": "AWS", "SourceIdentifier": "X"},
        }
    })
    status, _ = _call("DeleteConfigRule", {"ConfigRuleName": "r1"})
    assert status == 200
    status, out = _call("DescribeConfigRules", {})
    assert out["ConfigRules"] == []


def test_delete_unknown_config_rule_errors():
    status, out = _call("DeleteConfigRule", {"ConfigRuleName": "ghost"})
    assert status == 400
    assert out["__type"] == "NoSuchConfigRuleException"


def test_put_config_rule_requires_name():
    status, out = _call("PutConfigRule", {"ConfigRule": {"Source": {"Owner": "AWS"}}})
    assert status == 400
    assert out["__type"] == "InvalidParameterValueException"


# ── Configuration Recorder ────────────────────────────────────

def test_recorder_put_describe_start_stop():
    status, _ = _call("PutConfigurationRecorder", {
        "ConfigurationRecorder": {
            "name": "default",
            "roleARN": "arn:aws:iam::123456789012:role/config-role",
            "recordingGroup": {"allSupported": True},
        }
    })
    assert status == 200

    status, out = _call("DescribeConfigurationRecorders", {})
    assert status == 200
    recorders = out["ConfigurationRecorders"]
    assert len(recorders) == 1
    assert recorders[0]["name"] == "default"
    assert recorders[0]["roleARN"].endswith("config-role")

    # Status before start: not recording.
    status, out = _call("DescribeConfigurationRecorderStatus", {})
    assert status == 200
    statuses = out["ConfigurationRecordersStatus"]
    assert len(statuses) == 1
    assert statuses[0]["name"] == "default"
    assert statuses[0]["recording"] is False

    status, _ = _call("StartConfigurationRecorder",
                      {"ConfigurationRecorderName": "default"})
    assert status == 200
    status, out = _call("DescribeConfigurationRecorderStatus", {})
    st = out["ConfigurationRecordersStatus"][0]
    assert st["recording"] is True
    assert st["lastStatus"] == "Success"
    assert isinstance(st["lastStartTime"], int)

    status, _ = _call("StopConfigurationRecorder",
                      {"ConfigurationRecorderName": "default"})
    assert status == 200
    status, out = _call("DescribeConfigurationRecorderStatus", {})
    assert out["ConfigurationRecordersStatus"][0]["recording"] is False


def test_describe_recorders_empty():
    status, out = _call("DescribeConfigurationRecorders", {})
    assert status == 200
    assert out == {"ConfigurationRecorders": []}


def test_start_unknown_recorder_errors():
    status, out = _call("StartConfigurationRecorder",
                        {"ConfigurationRecorderName": "missing"})
    assert status == 400
    assert out["__type"] == "NoSuchConfigurationRecorderException"


def test_describe_recorder_status_unknown_name_errors():
    status, out = _call("DescribeConfigurationRecorderStatus",
                        {"ConfigurationRecorderNames": ["missing"]})
    assert status == 400
    assert out["__type"] == "NoSuchConfigurationRecorderException"


# ── Delivery Channel ──────────────────────────────────────────

def test_delivery_channel_put_describe():
    status, _ = _call("PutDeliveryChannel", {
        "DeliveryChannel": {
            "name": "default",
            "s3BucketName": "config-bucket",
        }
    })
    assert status == 200

    status, out = _call("DescribeDeliveryChannels", {})
    assert status == 200
    channels = out["DeliveryChannels"]
    assert len(channels) == 1
    assert channels[0]["name"] == "default"
    assert channels[0]["s3BucketName"] == "config-bucket"

    status, out = _call("DescribeDeliveryChannelStatus", {})
    assert status == 200
    statuses = out["DeliveryChannelsStatus"]
    assert len(statuses) == 1
    assert statuses[0]["name"] == "default"


def test_describe_delivery_channels_empty():
    status, out = _call("DescribeDeliveryChannels", {})
    assert status == 200
    assert out == {"DeliveryChannels": []}


def test_describe_delivery_channels_unknown_name_errors():
    status, out = _call("DescribeDeliveryChannels", {"DeliveryChannelNames": ["x"]})
    assert status == 400
    assert out["__type"] == "NoSuchDeliveryChannelException"


# ── Compliance / evaluation status ────────────────────────────

def test_describe_compliance_by_config_rule():
    _call("PutConfigRule", {
        "ConfigRule": {
            "ConfigRuleName": "r1",
            "Source": {"Owner": "AWS", "SourceIdentifier": "X"},
        }
    })
    status, out = _call("DescribeComplianceByConfigRule", {})
    assert status == 200
    items = out["ComplianceByConfigRules"]
    assert len(items) == 1
    assert items[0]["ConfigRuleName"] == "r1"
    assert items[0]["Compliance"]["ComplianceType"] == "INSUFFICIENT_DATA"


def test_describe_compliance_unknown_rule_errors():
    status, out = _call("DescribeComplianceByConfigRule",
                        {"ConfigRuleNames": ["ghost"]})
    assert status == 400
    assert out["__type"] == "NoSuchConfigRuleException"


def test_get_compliance_details_by_config_rule():
    _call("PutConfigRule", {
        "ConfigRule": {
            "ConfigRuleName": "r1",
            "Source": {"Owner": "AWS", "SourceIdentifier": "X"},
        }
    })
    status, out = _call("GetComplianceDetailsByConfigRule", {"ConfigRuleName": "r1"})
    assert status == 200
    assert out == {"EvaluationResults": []}


def test_get_compliance_details_unknown_rule_errors():
    status, out = _call("GetComplianceDetailsByConfigRule", {"ConfigRuleName": "ghost"})
    assert status == 400
    assert out["__type"] == "NoSuchConfigRuleException"


def test_describe_config_rule_evaluation_status():
    _call("PutConfigRule", {
        "ConfigRule": {
            "ConfigRuleName": "r1",
            "Source": {"Owner": "AWS", "SourceIdentifier": "X"},
        }
    })
    status, out = _call("DescribeConfigRuleEvaluationStatus", {})
    assert status == 200
    items = out["ConfigRulesEvaluationStatus"]
    assert len(items) == 1
    assert items[0]["ConfigRuleName"] == "r1"
    assert items[0]["ConfigRuleArn"].startswith("arn:aws:config:")
    assert items[0]["FirstEvaluationStarted"] is False


def test_evaluation_status_unknown_rule_errors():
    status, out = _call("DescribeConfigRuleEvaluationStatus",
                        {"ConfigRuleNames": ["ghost"]})
    assert status == 400
    assert out["__type"] == "NoSuchConfigRuleException"


def test_unknown_action_errors():
    status, out = _call("BogusAction", {})
    assert status == 400
    assert out["__type"] == "InvalidAction"
