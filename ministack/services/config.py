"""
AWS Config Service Emulator.
JSON-based API via X-Amz-Target (prefix: StarlingDoveService).

Verified against botocore config/2014-11-12/service-2.json:
  metadata.targetPrefix = "StarlingDoveService"
  metadata.jsonVersion  = "1.1"
  metadata.protocol     = "json"
  metadata.endpointPrefix = "config"

Supports the read-focused control plane the compliance/IaC benchmarks reach
for, plus minimal CRUD to keep reads coherent:
  PutConfigRule, DescribeConfigRules, DeleteConfigRule,
  PutConfigurationRecorder, DescribeConfigurationRecorders,
  DescribeConfigurationRecorderStatus,
  PutDeliveryChannel, DescribeDeliveryChannels, DescribeDeliveryChannelStatus,
  StartConfigurationRecorder, StopConfigurationRecorder,
  DescribeComplianceByConfigRule, GetComplianceDetailsByConfigRule,
  DescribeConfigRuleEvaluationStatus.

JSON-protocol timestamps are INT epoch seconds. Stores are per account+region.
"""

import copy
import json
import logging
import time

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("config")


# ── Stores (per account + region) ─────────────────────────────
# AWS Config: at most one recorder and one delivery channel per region, but the
# describe APIs still return lists. Config rules are keyed by name.
_config_rules = AccountRegionScopedDict()          # ConfigRuleName -> ConfigRule dict
_recorders = AccountRegionScopedDict()             # name -> recorder dict {rec, recording, status...}
_delivery_channels = AccountRegionScopedDict()     # name -> delivery channel dict


def _config_rule_arn(rule_id):
    return (
        f"arn:aws:config:{get_region()}:{get_account_id()}"
        f":config-rule/{rule_id}"
    )


# ── Persistence ───────────────────────────────────────────────

def get_state():
    return copy.deepcopy(
        {
            "_config_rules": _config_rules,
            "_recorders": _recorders,
            "_delivery_channels": _delivery_channels,
        }
    )


def restore_state(data):
    if not data:
        return
    _config_rules.clear()
    _recorders.clear()
    _delivery_channels.clear()
    for store, key in (
        (_config_rules, "_config_rules"),
        (_recorders, "_recorders"),
        (_delivery_channels, "_delivery_channels"),
    ):
        restored = data.get(key, {})
        if isinstance(restored, AccountRegionScopedDict):
            store.update(restored)
        else:
            for k, v in restored.items():
                store[k] = v


try:
    _restored = load_state("config")
    if _restored:
        restore_state(_restored)
except Exception:
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


# ── Config Rules ──────────────────────────────────────────────

def _put_config_rule(data):
    rule = data.get("ConfigRule") or {}
    name = rule.get("ConfigRuleName")
    if not name:
        return error_response_json(
            "InvalidParameterValueException",
            "ConfigRule.ConfigRuleName is a required field",
            400,
        )
    existing = _config_rules.get(name)
    rule_id = existing["ConfigRuleId"] if existing else "config-rule-" + new_uuid()[:6]

    stored = {
        "ConfigRuleName": name,
        "ConfigRuleArn": _config_rule_arn(rule_id),
        "ConfigRuleId": rule_id,
        "Source": rule.get("Source", {}),
        "ConfigRuleState": rule.get("ConfigRuleState", "ACTIVE"),
    }
    for optional in ("Description", "Scope", "InputParameters",
                     "MaximumExecutionFrequency", "CreatedBy", "EvaluationModes"):
        if optional in rule:
            stored[optional] = rule[optional]
    _config_rules[name] = stored
    logger.info("Put config rule %s", name)
    return json_response({})


def _describe_config_rules(data):
    names = data.get("ConfigRuleNames") or []
    if names:
        rules = []
        for n in names:
            rec = _config_rules.get(n)
            if not rec:
                return error_response_json(
                    "NoSuchConfigRuleException",
                    f"The ConfigRule '{n}' provided in the request is invalid. "
                    "Please check the configRule name",
                    400,
                )
            rules.append(rec)
    else:
        rules = list(_config_rules.values())
    return json_response({"ConfigRules": rules})


def _delete_config_rule(data):
    name = data.get("ConfigRuleName")
    if name not in _config_rules:
        return error_response_json(
            "NoSuchConfigRuleException",
            f"The ConfigRule '{name}' provided in the request is invalid. "
            "Please check the configRule name",
            400,
        )
    del _config_rules[name]
    return json_response({})


# ── Configuration Recorder ────────────────────────────────────

def _put_configuration_recorder(data):
    recorder = data.get("ConfigurationRecorder") or {}
    name = recorder.get("name") or "default"
    existing = _recorders.get(name)
    stored = existing or {
        "recording": False,
        "lastStatus": "Pending",
        "lastStartTime": None,
        "lastStopTime": None,
        "lastStatusChangeTime": int(time.time()),
    }
    rec = {"name": name}
    for optional in ("roleARN", "recordingGroup", "recordingMode"):
        if optional in recorder:
            rec[optional] = recorder[optional]
    stored["recorder"] = rec
    _recorders[name] = stored
    logger.info("Put configuration recorder %s", name)
    return json_response({})


def _describe_configuration_recorders(data):
    names = data.get("ConfigurationRecorderNames") or []
    if names:
        out = []
        for n in names:
            rec = _recorders.get(n)
            if not rec:
                return error_response_json(
                    "NoSuchConfigurationRecorderException",
                    f"Cannot find configuration recorder with the specified name '{n}'.",
                    400,
                )
            out.append(rec["recorder"])
    else:
        out = [r["recorder"] for r in _recorders.values()]
    return json_response({"ConfigurationRecorders": out})


def _describe_configuration_recorder_status(data):
    names = data.get("ConfigurationRecorderNames") or []
    if names:
        recorders = []
        for n in names:
            rec = _recorders.get(n)
            if not rec:
                return error_response_json(
                    "NoSuchConfigurationRecorderException",
                    f"Cannot find configuration recorder with the specified name '{n}'.",
                    400,
                )
            recorders.append(rec)
    else:
        recorders = list(_recorders.values())
    statuses = [_recorder_status(r) for r in recorders]
    return json_response({"ConfigurationRecordersStatus": statuses})


def _recorder_status(rec):
    status = {
        "name": rec["recorder"]["name"],
        "recording": rec.get("recording", False),
        "lastStatus": rec.get("lastStatus", "Pending"),
    }
    for ts_field in ("lastStartTime", "lastStopTime", "lastStatusChangeTime"):
        if rec.get(ts_field) is not None:
            status[ts_field] = rec[ts_field]
    return status


def _start_configuration_recorder(data):
    name = data.get("ConfigurationRecorderName")
    rec = _recorders.get(name)
    if not rec:
        return error_response_json(
            "NoSuchConfigurationRecorderException",
            f"Cannot find configuration recorder with the specified name '{name}'.",
            400,
        )
    now = int(time.time())
    rec["recording"] = True
    rec["lastStatus"] = "Success"
    rec["lastStartTime"] = now
    rec["lastStatusChangeTime"] = now
    return json_response({})


def _stop_configuration_recorder(data):
    name = data.get("ConfigurationRecorderName")
    rec = _recorders.get(name)
    if not rec:
        return error_response_json(
            "NoSuchConfigurationRecorderException",
            f"Cannot find configuration recorder with the specified name '{name}'.",
            400,
        )
    now = int(time.time())
    rec["recording"] = False
    rec["lastStatus"] = "Success"
    rec["lastStopTime"] = now
    rec["lastStatusChangeTime"] = now
    return json_response({})


# ── Delivery Channel ──────────────────────────────────────────

def _put_delivery_channel(data):
    channel = data.get("DeliveryChannel") or {}
    name = channel.get("name") or "default"
    stored = {"name": name}
    for optional in ("s3BucketName", "s3KeyPrefix", "s3KmsKeyArn",
                     "snsTopicARN", "configSnapshotDeliveryProperties"):
        if optional in channel:
            stored[optional] = channel[optional]
    _delivery_channels[name] = stored
    logger.info("Put delivery channel %s", name)
    return json_response({})


def _describe_delivery_channels(data):
    names = data.get("DeliveryChannelNames") or []
    if names:
        out = []
        for n in names:
            ch = _delivery_channels.get(n)
            if not ch:
                return error_response_json(
                    "NoSuchDeliveryChannelException",
                    f"Cannot find delivery channel with specified name '{n}'.",
                    400,
                )
            out.append(ch)
    else:
        out = list(_delivery_channels.values())
    return json_response({"DeliveryChannels": out})


def _describe_delivery_channel_status(data):
    names = data.get("DeliveryChannelNames") or []
    if names:
        channels = []
        for n in names:
            ch = _delivery_channels.get(n)
            if not ch:
                return error_response_json(
                    "NoSuchDeliveryChannelException",
                    f"Cannot find delivery channel with specified name '{n}'.",
                    400,
                )
            channels.append(ch)
    else:
        channels = list(_delivery_channels.values())
    statuses = [{"name": ch["name"]} for ch in channels]
    return json_response({"DeliveryChannelsStatus": statuses})


# ── Compliance ────────────────────────────────────────────────

def _describe_compliance_by_config_rule(data):
    names = data.get("ConfigRuleNames") or []
    if names:
        results = []
        for n in names:
            if n not in _config_rules:
                return error_response_json(
                    "NoSuchConfigRuleException",
                    f"The ConfigRule '{n}' provided in the request is invalid. "
                    "Please check the configRule name",
                    400,
                )
            results.append(_compliance_by_rule(n))
    else:
        results = [_compliance_by_rule(n) for n in _config_rules.keys()]
    return json_response({"ComplianceByConfigRules": results})


def _compliance_by_rule(name):
    # No evaluations have run in the emulator, so compliance is INSUFFICIENT_DATA
    # — the shape real AWS returns for a rule that has never been evaluated.
    return {
        "ConfigRuleName": name,
        "Compliance": {"ComplianceType": "INSUFFICIENT_DATA"},
    }


def _get_compliance_details_by_config_rule(data):
    name = data.get("ConfigRuleName")
    if name not in _config_rules:
        return error_response_json(
            "NoSuchConfigRuleException",
            f"The ConfigRule '{name}' provided in the request is invalid. "
            "Please check the configRule name",
            400,
        )
    # No evaluation engine — no per-resource results yet.
    return json_response({"EvaluationResults": []})


def _describe_config_rule_evaluation_status(data):
    names = data.get("ConfigRuleNames") or []
    if names:
        statuses = []
        for n in names:
            rec = _config_rules.get(n)
            if not rec:
                return error_response_json(
                    "NoSuchConfigRuleException",
                    f"The ConfigRule '{n}' provided in the request is invalid. "
                    "Please check the configRule name",
                    400,
                )
            statuses.append(_evaluation_status(rec))
    else:
        statuses = [_evaluation_status(r) for r in _config_rules.values()]
    return json_response({"ConfigRulesEvaluationStatus": statuses})


def _evaluation_status(rec):
    return {
        "ConfigRuleName": rec["ConfigRuleName"],
        "ConfigRuleArn": rec["ConfigRuleArn"],
        "ConfigRuleId": rec["ConfigRuleId"],
        "FirstEvaluationStarted": False,
    }


# ── Request handler ───────────────────────────────────────────

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "PutConfigRule": _put_config_rule,
        "DescribeConfigRules": _describe_config_rules,
        "DeleteConfigRule": _delete_config_rule,
        "PutConfigurationRecorder": _put_configuration_recorder,
        "DescribeConfigurationRecorders": _describe_configuration_recorders,
        "DescribeConfigurationRecorderStatus": _describe_configuration_recorder_status,
        "PutDeliveryChannel": _put_delivery_channel,
        "DescribeDeliveryChannels": _describe_delivery_channels,
        "DescribeDeliveryChannelStatus": _describe_delivery_channel_status,
        "StartConfigurationRecorder": _start_configuration_recorder,
        "StopConfigurationRecorder": _stop_configuration_recorder,
        "DescribeComplianceByConfigRule": _describe_compliance_by_config_rule,
        "GetComplianceDetailsByConfigRule": _get_compliance_details_by_config_rule,
        "DescribeConfigRuleEvaluationStatus": _describe_config_rule_evaluation_status,
    }

    handler = handlers.get(action)
    if not handler:
        logger.warning("Unknown Config action: %s", action)
        return error_response_json(
            "InvalidAction", f"Unknown Config action: {action}", 400
        )
    return handler(data)


def reset():
    _config_rules.clear()
    _recorders.clear()
    _delivery_channels.clear()
