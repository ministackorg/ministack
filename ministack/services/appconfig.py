# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
AppConfig Service Emulator.
REST/JSON protocol — path-based routing.

Control Plane (appconfig):
  Applications:              CreateApplication, GetApplication, ListApplications,
                             UpdateApplication, DeleteApplication
  Environments:              CreateEnvironment, GetEnvironment, ListEnvironments,
                             UpdateEnvironment, DeleteEnvironment
  Configuration Profiles:    CreateConfigurationProfile, GetConfigurationProfile,
                             ListConfigurationProfiles, UpdateConfigurationProfile,
                             DeleteConfigurationProfile
  Hosted Configuration Versions: CreateHostedConfigurationVersion,
                             GetHostedConfigurationVersion,
                             ListHostedConfigurationVersions,
                             DeleteHostedConfigurationVersion
  Deployment Strategies:     CreateDeploymentStrategy, GetDeploymentStrategy,
                             ListDeploymentStrategies, UpdateDeploymentStrategy,
                             DeleteDeploymentStrategy
  Deployments:               StartDeployment, GetDeployment, ListDeployments,
                             StopDeployment
  Tags:                      TagResource, UntagResource, ListTagsForResource

Data Plane (appconfigdata):
  StartConfigurationSession, GetLatestConfiguration
"""

import copy
import hashlib
import json
import logging
import os
import re
import time
import uuid

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import AccountRegionScopedDict, get_account_id, get_region

logger = logging.getLogger("appconfig")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# APPCONFIG_DEPLOYMENT_MINUTE_SECONDS: real seconds that stand in for one minute of a
# deployment strategy's DeploymentDurationInMinutes and FinalBakeTimeInMinutes. 0 (the
# default) runs every deployment to COMPLETE as it starts; 60 keeps AWS's pace. Also
# settable at runtime through /_ministack/config, so a test can pin the pace.
_DEPLOYMENT_MINUTE_SECONDS = float(os.environ.get("APPCONFIG_DEPLOYMENT_MINUTE_SECONDS", "0"))

# Next-Poll-Interval-In-Seconds for a session that set no RequiredMinimumPollIntervalInSeconds.
_DEFAULT_POLL_INTERVAL_SECONDS = 30

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_applications = AccountRegionScopedDict()
_environments = AccountRegionScopedDict()          # "{app_id}/{env_id}" -> record
_config_profiles = AccountRegionScopedDict()       # "{app_id}/{profile_id}" -> record
_hosted_versions = AccountRegionScopedDict()       # "{app_id}/{profile_id}/{version}" -> record
_hosted_version_counters = AccountRegionScopedDict()  # "{app_id}/{profile_id}" -> last issued version
_deployment_strategies = AccountRegionScopedDict()
_deployments = AccountRegionScopedDict()           # "{app_id}/{env_id}/{deploy_num}" -> record
_tags = AccountRegionScopedDict()                  # arn -> {key: value}
_sessions = AccountRegionScopedDict()              # token -> session record

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def get_state():
    return copy.deepcopy({
        "applications": _applications,
        "environments": _environments,
        "config_profiles": _config_profiles,
        "hosted_versions": _hosted_versions,
        "hosted_version_counters": _hosted_version_counters,
        "deployment_strategies": _deployment_strategies,
        "deployments": _deployments,
        "tags": _tags,
    })


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    _applications.update(data.get("applications", {}))
    _environments.update(data.get("environments", {}))
    _config_profiles.update(data.get("config_profiles", {}))
    _hosted_versions.update(data.get("hosted_versions", {}))
    _hosted_version_counters.update(data.get("hosted_version_counters", {}))
    _deployment_strategies.update(data.get("deployment_strategies", {}))
    _deployments.update(data.get("deployments", {}))
    _tags.update(data.get("tags", {}))



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _gen_id():
    return uuid.uuid4().hex[:7]


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def _iso(epoch):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(epoch)) + f".{int(epoch * 1000) % 1000:03d}Z"


def _app_arn(app_id):
    return f"arn:aws:appconfig:{get_region()}:{get_account_id()}:application/{app_id}"


def _env_arn(app_id, env_id):
    return f"arn:aws:appconfig:{get_region()}:{get_account_id()}:application/{app_id}/environment/{env_id}"


def _profile_arn(app_id, profile_id):
    return f"arn:aws:appconfig:{get_region()}:{get_account_id()}:application/{app_id}/configurationprofile/{profile_id}"


def _strategy_arn(strategy_id):
    return f"arn:aws:appconfig:{get_region()}:{get_account_id()}:deploymentstrategy/{strategy_id}"


def _deployment_arn(app_id, env_id, deploy_num):
    return (
        f"arn:aws:appconfig:{get_region()}:{get_account_id()}:"
        f"application/{app_id}/environment/{env_id}/deployment/{deploy_num}"
    )


def _resolve_application_id(identifier):
    if identifier in _applications:
        return identifier

    for app_id, record in _applications.items():
        if record.get("Name") == identifier:
            return app_id

    return None


def _resolve_environment_id(app_id, identifier):
    if f"{app_id}/{identifier}" in _environments:
        return identifier

    for record in _environments.values():
        if record.get("ApplicationId") == app_id and record.get("Name") == identifier:
            return record.get("Id")

    return None


def _resolve_configuration_profile_id(app_id, identifier):
    if f"{app_id}/{identifier}" in _config_profiles:
        return identifier

    for record in _config_profiles.values():
        if record.get("ApplicationId") == app_id and record.get("Name") == identifier:
            return record.get("Id")

    return None


# ---------------------------------------------------------------------------
# Applications
# ---------------------------------------------------------------------------


def _create_application(body):
    name = body.get("Name")
    if not name:
        return _error(400, "BadRequestException", "Name is required")
    app_id = _gen_id()
    record = {
        "Id": app_id,
        "Name": name,
        "Description": body.get("Description", ""),
    }
    _applications[app_id] = record
    _apply_tags(_app_arn(app_id), body.get("Tags", {}))
    logger.info("CreateApplication: %s (%s)", name, app_id)
    return _json(201, record)


def _get_application(app_id):
    app = _applications.get(app_id)
    if not app:
        return _error(404, "ResourceNotFoundException", f"Application {app_id} not found")
    return _json(200, app)


def _list_applications(query):
    max_results = int(query.get("max_results", 50))
    items = list(_applications.values())
    return _json(200, {"Items": items[:max_results]})


def _update_application(app_id, body):
    app = _applications.get(app_id)
    if not app:
        return _error(404, "ResourceNotFoundException", f"Application {app_id} not found")
    if "Name" in body:
        app["Name"] = body["Name"]
    if "Description" in body:
        app["Description"] = body["Description"]
    return _json(200, app)


def _drop_deployments(prefix):
    """Drop the deployments under an application or environment key prefix, with their tags."""
    for key in [k for k in _deployments if k.startswith(prefix)]:
        del _deployments[key]
        _tags.pop(_deployment_arn(*key.split("/")), None)


def _delete_application(app_id):
    if app_id not in _applications:
        return _error(404, "ResourceNotFoundException", f"Application {app_id} not found")
    del _applications[app_id]
    _tags.pop(_app_arn(app_id), None)
    keys_to_remove = [k for k in _environments if k.startswith(f"{app_id}/")]
    for k in keys_to_remove:
        _environments.pop(k, None)
    keys_to_remove = [k for k in _config_profiles if k.startswith(f"{app_id}/")]
    for k in keys_to_remove:
        _config_profiles.pop(k, None)
    keys_to_remove = [k for k in _hosted_versions if k.startswith(f"{app_id}/")]
    for k in keys_to_remove:
        _hosted_versions.pop(k, None)
    keys_to_remove = [k for k in _hosted_version_counters if k.startswith(f"{app_id}/")]
    for k in keys_to_remove:
        _hosted_version_counters.pop(k, None)
    _drop_deployments(f"{app_id}/")
    return _json(204, {})


# ---------------------------------------------------------------------------
# Environments
# ---------------------------------------------------------------------------


def _create_environment(app_id, body):
    if app_id not in _applications:
        return _error(404, "ResourceNotFoundException", f"Application {app_id} not found")
    name = body.get("Name")
    if not name:
        return _error(400, "BadRequestException", "Name is required")
    env_id = _gen_id()
    record = {
        "ApplicationId": app_id,
        "Id": env_id,
        "Name": name,
        "Description": body.get("Description", ""),
        "State": "READY_FOR_DEPLOYMENT",
        "Monitors": body.get("Monitors", []),
    }
    _environments[f"{app_id}/{env_id}"] = record
    _apply_tags(_env_arn(app_id, env_id), body.get("Tags", {}))
    logger.info("CreateEnvironment: %s/%s (%s)", app_id, name, env_id)
    return _json(201, record)


def _get_environment(app_id, env_id):
    env = _environments.get(f"{app_id}/{env_id}")
    if not env:
        return _error(404, "ResourceNotFoundException", f"Environment {env_id} not found")
    return _json(200, env)


def _list_environments(app_id, query):
    if app_id not in _applications:
        return _error(404, "ResourceNotFoundException", f"Application {app_id} not found")
    max_results = int(query.get("max_results", 50))
    items = [e for e in _environments.values() if e["ApplicationId"] == app_id]
    return _json(200, {"Items": items[:max_results]})


def _update_environment(app_id, env_id, body):
    env = _environments.get(f"{app_id}/{env_id}")
    if not env:
        return _error(404, "ResourceNotFoundException", f"Environment {env_id} not found")
    if "Name" in body:
        env["Name"] = body["Name"]
    if "Description" in body:
        env["Description"] = body["Description"]
    if "Monitors" in body:
        env["Monitors"] = body["Monitors"]
    return _json(200, env)


def _delete_environment(app_id, env_id):
    key = f"{app_id}/{env_id}"
    if key not in _environments:
        return _error(404, "ResourceNotFoundException", f"Environment {env_id} not found")
    del _environments[key]
    _tags.pop(_env_arn(app_id, env_id), None)
    _drop_deployments(f"{key}/")
    return _json(204, {})


# ---------------------------------------------------------------------------
# Configuration Profiles
# ---------------------------------------------------------------------------


def _create_configuration_profile(app_id, body):
    if app_id not in _applications:
        return _error(404, "ResourceNotFoundException", f"Application {app_id} not found")
    name = body.get("Name")
    if not name:
        return _error(400, "BadRequestException", "Name is required")
    location_uri = body.get("LocationUri", "hosted")
    profile_id = _gen_id()
    record = {
        "ApplicationId": app_id,
        "Id": profile_id,
        "Name": name,
        "Description": body.get("Description", ""),
        "LocationUri": location_uri,
        "RetrievalRoleArn": body.get("RetrievalRoleArn", ""),
        "Validators": body.get("Validators", []),
        "Type": body.get("Type", "AWS.Freeform"),
    }
    _config_profiles[f"{app_id}/{profile_id}"] = record
    _apply_tags(_profile_arn(app_id, profile_id), body.get("Tags", {}))
    logger.info("CreateConfigurationProfile: %s/%s (%s)", app_id, name, profile_id)
    return _json(201, record)


def _get_configuration_profile(app_id, profile_id):
    profile = _config_profiles.get(f"{app_id}/{profile_id}")
    if not profile:
        return _error(404, "ResourceNotFoundException", f"Configuration profile {profile_id} not found")
    return _json(200, profile)


def _list_configuration_profiles(app_id, query):
    if app_id not in _applications:
        return _error(404, "ResourceNotFoundException", f"Application {app_id} not found")
    max_results = int(query.get("max_results", 50))
    items = [p for p in _config_profiles.values() if p["ApplicationId"] == app_id]
    return _json(200, {"Items": items[:max_results]})


def _update_configuration_profile(app_id, profile_id, body):
    profile = _config_profiles.get(f"{app_id}/{profile_id}")
    if not profile:
        return _error(404, "ResourceNotFoundException", f"Configuration profile {profile_id} not found")
    for field in ("Name", "Description", "RetrievalRoleArn", "Validators"):
        if field in body:
            profile[field] = body[field]
    return _json(200, profile)


def _delete_configuration_profile(app_id, profile_id):
    key = f"{app_id}/{profile_id}"
    if key not in _config_profiles:
        return _error(404, "ResourceNotFoundException", f"Configuration profile {profile_id} not found")
    del _config_profiles[key]
    _hosted_version_counters.pop(key, None)
    _tags.pop(_profile_arn(app_id, profile_id), None)
    keys_to_remove = [k for k in _hosted_versions if k.startswith(f"{app_id}/{profile_id}/")]
    for k in keys_to_remove:
        _hosted_versions.pop(k, None)
    return _json(204, {})


# ---------------------------------------------------------------------------
# Hosted Configuration Versions
# ---------------------------------------------------------------------------


def _latest_hosted_version_number(app_id, profile_id):
    """The highest existing version number of a profile, 0 when it has none."""
    prefix = f"{app_id}/{profile_id}/"
    return max((v["VersionNumber"] for k, v in _hosted_versions.items()
                if k.startswith(prefix)), default=0)


def _next_hosted_version_number(app_id, profile_id):
    """Issue a profile's next version number; a deleted number is never reused."""
    key = f"{app_id}/{profile_id}"
    number = max(_hosted_version_counters.get(key, 0),
                 _latest_hosted_version_number(app_id, profile_id)) + 1
    _hosted_version_counters[key] = number
    return number


def _create_hosted_configuration_version(app_id, profile_id, body, content_type, version_label=None):
    if f"{app_id}/{profile_id}" not in _config_profiles:
        return _error(404, "ResourceNotFoundException", f"Configuration profile {profile_id} not found")

    version_number = _next_hosted_version_number(app_id, profile_id)

    record = {
        "ApplicationId": app_id,
        "ConfigurationProfileId": profile_id,
        "VersionNumber": version_number,
        "ContentType": content_type,
        "Content": body,
        "Description": "",
    }
    if version_label:
        record["VersionLabel"] = version_label
    _hosted_versions[f"{app_id}/{profile_id}/{version_number}"] = record
    logger.info("CreateHostedConfigurationVersion: %s/%s v%d", app_id, profile_id, version_number)

    resp_headers = {
        "Content-Type": content_type,
        "Application-Id": app_id,
        "Configuration-Profile-Id": profile_id,
        "Version-Number": str(version_number),
    }
    if version_label:
        resp_headers["VersionLabel"] = version_label
    return 201, resp_headers, body if isinstance(body, bytes) else body.encode("utf-8")


def _get_hosted_configuration_version(app_id, profile_id, version_number):
    key = f"{app_id}/{profile_id}/{version_number}"
    record = _hosted_versions.get(key)
    if not record:
        return _error(404, "ResourceNotFoundException",
                      f"Hosted configuration version {version_number} not found")
    content = record["Content"]
    resp_headers = {
        "Content-Type": record["ContentType"],
        "Application-Id": app_id,
        "Configuration-Profile-Id": profile_id,
        "Version-Number": str(version_number),
    }
    if record.get("VersionLabel"):
        resp_headers["VersionLabel"] = record["VersionLabel"]
    return 200, resp_headers, content if isinstance(content, bytes) else content.encode("utf-8")


def _list_hosted_configuration_versions(app_id, profile_id, query):
    if f"{app_id}/{profile_id}" not in _config_profiles:
        return _error(404, "ResourceNotFoundException", f"Configuration profile {profile_id} not found")
    max_results = int(query.get("max_results", 50))
    items = []
    for k, v in _hosted_versions.items():
        if k.startswith(f"{app_id}/{profile_id}/"):
            items.append({
                "ApplicationId": app_id,
                "ConfigurationProfileId": profile_id,
                "VersionNumber": v["VersionNumber"],
                "ContentType": v["ContentType"],
                "Description": v.get("Description", ""),
                **({"VersionLabel": v["VersionLabel"]} if v.get("VersionLabel") else {}),
            })
    return _json(200, {"Items": items[:max_results]})


def _delete_hosted_configuration_version(app_id, profile_id, version_number):
    key = f"{app_id}/{profile_id}/{version_number}"
    if key not in _hosted_versions:
        return _error(404, "ResourceNotFoundException",
                      f"Hosted configuration version {version_number} not found")
    del _hosted_versions[key]
    return _json(204, {})


# ---------------------------------------------------------------------------
# Deployment Strategies
# ---------------------------------------------------------------------------


def _predefined_strategy(strategy_id, fields):
    """A predefined strategy record; AWS names it by its id."""
    return {
        "Id": strategy_id,
        "Name": strategy_id,
        **fields,
        "ReplicateTo": "NONE",
    }


# Values as ListDeploymentStrategies returns them on AWS.
_PREDEFINED_DEPLOYMENT_STRATEGIES = {
    strategy_id: _predefined_strategy(strategy_id, fields)
    for strategy_id, fields in {
        "AppConfig.AllAtOnce": {
            "Description": "Quick", "DeploymentDurationInMinutes": 0, "GrowthType": "LINEAR",
            "GrowthFactor": 100.0, "FinalBakeTimeInMinutes": 10,
        },
        "AppConfig.Linear50PercentEvery30Seconds": {
            "Description": "Test/Demo", "DeploymentDurationInMinutes": 1, "GrowthType": "LINEAR",
            "GrowthFactor": 50.0, "FinalBakeTimeInMinutes": 1,
        },
        "AppConfig.Canary10Percent20Minutes": {
            "Description": "AWS Recommended", "DeploymentDurationInMinutes": 20, "GrowthType": "EXPONENTIAL",
            "GrowthFactor": 10.0, "FinalBakeTimeInMinutes": 10,
        },
        "AppConfig.Linear20PercentEvery6Minutes": {
            "Description": "AWS Recommended", "DeploymentDurationInMinutes": 30, "GrowthType": "LINEAR",
            "GrowthFactor": 20.0, "FinalBakeTimeInMinutes": 30,
        },
    }.items()
}


def _create_deployment_strategy(body):
    name = body.get("Name")
    if not name:
        return _error(400, "BadRequestException", "Name is required")
    strategy_id = _gen_id()
    record = {
        "Id": strategy_id,
        "Name": name,
        "Description": body.get("Description", ""),
        "DeploymentDurationInMinutes": body.get("DeploymentDurationInMinutes", 0),
        "GrowthType": body.get("GrowthType", "LINEAR"),
        "GrowthFactor": body.get("GrowthFactor", 100.0),
        "FinalBakeTimeInMinutes": body.get("FinalBakeTimeInMinutes", 0),
        "ReplicateTo": body.get("ReplicateTo", "NONE"),
    }
    _deployment_strategies[strategy_id] = record
    _apply_tags(_strategy_arn(strategy_id), body.get("Tags", {}))
    logger.info("CreateDeploymentStrategy: %s (%s)", name, strategy_id)
    return _json(201, record)


def _find_deployment_strategy(strategy_id):
    """A stored strategy of this account and region, or else the predefined one with that id."""
    return _deployment_strategies.get(strategy_id) or _PREDEFINED_DEPLOYMENT_STRATEGIES.get(strategy_id)


def _deployment_params_from_strategy(strategy):
    """The strategy fields a deployment copies when it starts; later strategy changes do not reach it."""
    return {
        "DeploymentDurationInMinutes": strategy["DeploymentDurationInMinutes"],
        "GrowthType": strategy["GrowthType"],
        "GrowthFactor": strategy["GrowthFactor"],
        "FinalBakeTimeInMinutes": strategy["FinalBakeTimeInMinutes"],
    }


def _get_deployment_strategy(strategy_id):
    strategy = _find_deployment_strategy(strategy_id)
    if not strategy:
        return _error(404, "ResourceNotFoundException", f"Deployment strategy {strategy_id} not found")
    return _json(200, strategy)


def _list_deployment_strategies(query):
    max_results = int(query.get("max_results", 50))
    items = [*_deployment_strategies.values(), *_PREDEFINED_DEPLOYMENT_STRATEGIES.values()]
    return _json(200, {"Items": items[:max_results]})


def _update_deployment_strategy(strategy_id, body):
    if strategy_id in _PREDEFINED_DEPLOYMENT_STRATEGIES:
        return _error(400, "BadRequestException", f"Cannot update predefined Deployment Strategy {strategy_id}")
    strategy = _deployment_strategies.get(strategy_id)
    if not strategy:
        return _error(404, "ResourceNotFoundException", f"Deployment strategy {strategy_id} not found")
    for field in ("Description", "DeploymentDurationInMinutes", "GrowthType",
                  "GrowthFactor", "FinalBakeTimeInMinutes"):
        if field in body:
            strategy[field] = body[field]
    return _json(200, strategy)


def _delete_deployment_strategy(strategy_id):
    # AWS ends this message with a period; the update message has none.
    if strategy_id in _PREDEFINED_DEPLOYMENT_STRATEGIES:
        return _error(400, "BadRequestException", f"Cannot delete predefined Deployment Strategy {strategy_id}.")
    if strategy_id not in _deployment_strategies:
        return _error(404, "ResourceNotFoundException", f"Deployment strategy {strategy_id} not found")
    del _deployment_strategies[strategy_id]
    _tags.pop(_strategy_arn(strategy_id), None)
    return _json(204, {})


# ---------------------------------------------------------------------------
# Deployments
# ---------------------------------------------------------------------------


_IN_PROGRESS_STATES = ("DEPLOYING", "BAKING")
_ENVIRONMENT_STATE_BY_DEPLOYMENT_STATE = {
    "DEPLOYING": "DEPLOYING",
    "BAKING": "DEPLOYING",
    "COMPLETE": "READY_FOR_DEPLOYMENT",
    "ROLLED_BACK": "ROLLED_BACK",
    "REVERTED": "REVERTED",
}
# StopDeployment's docs: "AppConfig only allows a revert within 72 hours of
# deployment completion."
_REVERT_WINDOW_SECONDS = 72 * 3600


def _rollout_steps(growth_type, growth_factor):
    """The percentages a deployment passes through, ending at 100.

    LINEAR adds GrowthFactor each step; EXPONENTIAL follows the G*(2^N) formula
    from the CreateDeploymentStrategy docs.
    """
    # The model's floor for GrowthFactor; CreateDeploymentStrategy stores what it is given.
    growth_factor = max(float(growth_factor or 100.0), 1.0)
    steps = []
    step = 1
    while True:
        if growth_type == "EXPONENTIAL":
            pct = growth_factor * (2 ** (step - 1))
        else:
            pct = growth_factor * step
        if pct >= 100.0:
            break
        steps.append(round(pct, 2))
        step += 1
    steps.append(100.0)
    return steps


def _add_deployment_event(record, event_type, triggered_by, description, at):
    record.setdefault("_Events", []).append({
        "EventType": event_type,
        "TriggeredBy": triggered_by,
        "Description": description,
        "ActionInvocations": [],
        "OccurredAt": _iso(at),
    })


def _first_monitor_alarm(app_id, env_id, start, end):
    """The (alarm ARN, time) of the first environment monitor in ALARM in [start, end]."""
    from ministack.services import cloudwatch

    env = _environments.get(f"{app_id}/{env_id}") or {}
    fired = []
    for monitor in env.get("Monitors") or []:
        alarm_arn = monitor.get("AlarmArn", "")
        at = cloudwatch.first_alarm_time(alarm_arn, start, end)
        if at is not None:
            fired.append((at, alarm_arn))
    if not fired:
        return None
    at, alarm_arn = min(fired)
    return alarm_arn, at


def _roll_back(record, triggered_by, initiator, at):
    record["State"] = "ROLLED_BACK"
    _add_deployment_event(record, "ROLLBACK_STARTED", triggered_by, f"Rollback initiated by {initiator}", at)
    _add_deployment_event(record, "ROLLBACK_COMPLETED", triggered_by, "Rollback completed", at)


def _advance_deployment(record, now):
    """Bring an in-progress deployment up to ``now``.

    Steps are spread evenly over DeploymentDurationInMinutes, the last one
    reaching 100% when the duration ends; the deployment then bakes for
    FinalBakeTimeInMinutes. A monitored alarm in ALARM at any point before the
    bake ends rolls it back at that moment.
    """
    if record.get("State") not in _IN_PROGRESS_STATES or "_StartedEpoch" not in record:
        return
    minute = _DEPLOYMENT_MINUTE_SECONDS
    started = record["_StartedEpoch"]
    steps = _rollout_steps(record["GrowthType"], record["GrowthFactor"])
    duration = float(record["DeploymentDurationInMinutes"]) * minute
    step_gap = duration / len(steps)
    bake = float(record["FinalBakeTimeInMinutes"]) * minute
    complete_at = started + duration + bake

    alarm = _first_monitor_alarm(record["ApplicationId"], record["EnvironmentId"],
                                 started, min(now, complete_at))
    horizon = alarm[1] if alarm else now

    for index, pct in enumerate(steps, start=1):
        at = started + index * step_gap
        # A step due at the moment an alarm fires is not taken.
        if at > horizon or (alarm and at >= horizon):
            break
        if pct > record["PercentageComplete"]:
            record["PercentageComplete"] = pct
            _add_deployment_event(record, "PERCENTAGE_UPDATED", "APPCONFIG",
                                  f"Deployed to {pct:g}% of targets", at)
    if record["State"] == "DEPLOYING" and record["PercentageComplete"] >= 100.0:
        record["State"] = "BAKING"
        if bake > 0:
            _add_deployment_event(record, "BAKE_TIME_STARTED", "APPCONFIG", "Bake time started",
                                  started + duration)

    if alarm:
        _roll_back(record, "CLOUDWATCH_ALARM", alarm[0], alarm[1])
        return
    if complete_at <= now:
        record["State"] = "COMPLETE"
        record["_CompletedEpoch"] = complete_at
        record["CompletedAt"] = _iso(complete_at)
        _add_deployment_event(record, "DEPLOYMENT_COMPLETED", "APPCONFIG", "Deployment completed", complete_at)


def _environment_deployments(app_id, env_id):
    prefix = f"{app_id}/{env_id}/"
    return sorted((v for k, v in _deployments.items() if k.startswith(prefix)),
                  key=lambda d: d["DeploymentNumber"])


def _refresh_environment(app_id, env_id):
    """Advance the environment's deployments to now and derive its State from the latest."""
    deployments = _environment_deployments(app_id, env_id)
    now = time.time()
    for deployment in deployments:
        _advance_deployment(deployment, now)
    env = _environments.get(f"{app_id}/{env_id}")
    if env is not None and deployments:
        env["State"] = _ENVIRONMENT_STATE_BY_DEPLOYMENT_STATE.get(deployments[-1]["State"], env["State"])
    return deployments


def _deployment_view(record):
    view = {k: v for k, v in record.items() if not k.startswith("_")}
    view["EventLog"] = list(reversed(record.get("_Events", [])))  # most recent first
    return view


def _start_deployment(app_id, env_id, body):
    if app_id not in _applications:
        return _error(404, "ResourceNotFoundException", f"Application {app_id} not found")
    if f"{app_id}/{env_id}" not in _environments:
        return _error(404, "ResourceNotFoundException", f"Environment {env_id} not found")

    strategy_id = body.get("DeploymentStrategyId", "")
    profile_id = body.get("ConfigurationProfileId", "")
    version = body.get("ConfigurationVersion", "")

    if profile_id and f"{app_id}/{profile_id}" not in _config_profiles:
        return _error(404, "ResourceNotFoundException", f"Configuration profile {profile_id} not found")

    strategy = _find_deployment_strategy(strategy_id)
    if not strategy:
        return _error(404, "ResourceNotFoundException",
                      f"DeploymentStrategy with Id {strategy_id} could not be found.")

    existing = _refresh_environment(app_id, env_id)
    latest_number = existing[-1]["DeploymentNumber"] if existing else 0
    expected = body.get("LatestDeploymentNumber")
    if expected is not None and expected != latest_number:
        return _error(409, "ConflictException",
                      f"LatestDeploymentNumber {expected} does not match the latest deployment "
                      f"number {latest_number}.")
    in_progress = [d for d in existing if d["State"] in _IN_PROGRESS_STATES]
    if in_progress:
        return _error(409, "ConflictException",
                      f"Deployment {in_progress[-1]['DeploymentNumber']} is already in progress "
                      f"in environment {env_id}.")
    deploy_num = latest_number + 1

    now = time.time()
    record = {
        "ApplicationId": app_id,
        "EnvironmentId": env_id,
        "DeploymentStrategyId": strategy_id,
        "ConfigurationProfileId": profile_id,
        "DeploymentNumber": deploy_num,
        "ConfigurationName": _config_profiles.get(f"{app_id}/{profile_id}", {}).get("Name", ""),
        "ConfigurationLocationUri": "hosted",
        "ConfigurationVersion": version,
        "Description": body.get("Description", ""),
        **_deployment_params_from_strategy(strategy),
        "State": "DEPLOYING",
        "PercentageComplete": 0.0,
        "StartedAt": _iso(now),
        "_StartedEpoch": now,
    }
    version_label = _hosted_versions.get(f"{app_id}/{profile_id}/{version}", {}).get("VersionLabel")
    if version_label:
        record["VersionLabel"] = version_label
    _add_deployment_event(record, "DEPLOYMENT_STARTED", "USER", "Deployment started", now)
    _deployments[f"{app_id}/{env_id}/{deploy_num}"] = record
    _refresh_environment(app_id, env_id)
    logger.info("StartDeployment: %s/%s #%d (profile=%s, version=%s)",
                app_id, env_id, deploy_num, profile_id, version)
    return _json(201, _deployment_view(record))


def _get_deployment(app_id, env_id, deploy_num):
    _refresh_environment(app_id, env_id)
    record = _deployments.get(f"{app_id}/{env_id}/{deploy_num}")
    if not record:
        return _error(404, "ResourceNotFoundException", f"Deployment {deploy_num} not found")
    return _json(200, _deployment_view(record))


def _list_deployments(app_id, env_id, query):
    if f"{app_id}/{env_id}" not in _environments:
        return _error(404, "ResourceNotFoundException", f"Environment {env_id} not found")
    max_results = int(query.get("max_results", 50))
    items = []
    for v in _refresh_environment(app_id, env_id):
        item = {
            "DeploymentNumber": v["DeploymentNumber"],
            "ConfigurationName": v.get("ConfigurationName", ""),
            "ConfigurationVersion": v.get("ConfigurationVersion", ""),
            "DeploymentDurationInMinutes": v.get("DeploymentDurationInMinutes", 0),
            "GrowthType": v.get("GrowthType", "LINEAR"),
            "GrowthFactor": v.get("GrowthFactor", 100.0),
            "FinalBakeTimeInMinutes": v.get("FinalBakeTimeInMinutes", 0),
            "State": v.get("State", "COMPLETE"),
            "PercentageComplete": v.get("PercentageComplete", 100.0),
            "StartedAt": v.get("StartedAt", ""),
        }
        for optional in ("CompletedAt", "VersionLabel"):
            if v.get(optional):
                item[optional] = v[optional]
        items.append(item)
    return _json(200, {"Items": items[:max_results]})


def _stop_deployment(app_id, env_id, deploy_num, allow_revert=False):
    _refresh_environment(app_id, env_id)
    record = _deployments.get(f"{app_id}/{env_id}/{deploy_num}")
    if not record:
        return _error(404, "ResourceNotFoundException", f"Deployment {deploy_num} not found")
    now = time.time()
    state = record["State"]
    if state in _IN_PROGRESS_STATES:
        _roll_back(record, "USER", get_account_id(), now)
    elif state == "COMPLETE" and allow_revert:
        completed = record.get("_CompletedEpoch")
        if completed is not None and now - completed > _REVERT_WINDOW_SECONDS:
            return _error(400, "BadRequestException",
                          f"Deployment {deploy_num} completed more than 72 hours ago and can no longer be reverted.")
        record["State"] = "REVERTED"
        _add_deployment_event(record, "REVERT_COMPLETED", "USER", f"Revert initiated by {get_account_id()}", now)
    else:
        return _error(400, "BadRequestException",
                      f"Deployment {deploy_num} is in state {state} and cannot be stopped."
                      + ("" if state != "COMPLETE" else " Pass AllowRevert to revert a completed deployment."))
    _refresh_environment(app_id, env_id)
    return _json(202, _deployment_view(record))


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def _apply_tags(arn, tags_dict):
    if tags_dict:
        if arn not in _tags:
            _tags[arn] = {}
        _tags[arn].update(tags_dict)


def _invalid_tag_resource_arn(resource_arn):
    return _error(400, "BadRequestException", f"Invalid resource ARN: {resource_arn}")


def _missing_tag_resource(resource_arn):
    return _error(404, "ResourceNotFoundException", f"Resource not found: {resource_arn}")


def _resolve_tag_resource_arn(resource_arn):
    try:
        spec = parse_arn(resource_arn)
    except ArnParseError:
        return None, _invalid_tag_resource_arn(resource_arn)

    if (
        spec.partition != "aws"
        or spec.service != "appconfig"
        or spec.account_id != get_account_id()
        or spec.region != get_region()
    ):
        return None, _invalid_tag_resource_arn(resource_arn)

    parts = spec.resource.split("/")
    if len(parts) == 2 and parts[0] == "application" and parts[1]:
        if parts[1] in _applications:
            return str(spec), None
        return None, _missing_tag_resource(resource_arn)

    if (
        len(parts) == 4
        and parts[0] == "application"
        and parts[1]
        and parts[2] == "environment"
        and parts[3]
    ):
        if f"{parts[1]}/{parts[3]}" in _environments:
            return str(spec), None
        return None, _missing_tag_resource(resource_arn)

    if (
        len(parts) == 4
        and parts[0] == "application"
        and parts[1]
        and parts[2] == "configurationprofile"
        and parts[3]
    ):
        if f"{parts[1]}/{parts[3]}" in _config_profiles:
            return str(spec), None
        return None, _missing_tag_resource(resource_arn)

    if len(parts) == 2 and parts[0] == "deploymentstrategy" and parts[1]:
        if _find_deployment_strategy(parts[1]):
            return str(spec), None
        return None, _missing_tag_resource(resource_arn)

    if (
        len(parts) == 6
        and parts[0] == "application"
        and parts[1]
        and parts[2] == "environment"
        and parts[3]
        and parts[4] == "deployment"
        and parts[5]
    ):
        if f"{parts[1]}/{parts[3]}/{parts[5]}" in _deployments:
            return str(spec), None
        return None, _missing_tag_resource(resource_arn)

    return None, _invalid_tag_resource_arn(resource_arn)


def _tag_resource(resource_arn, body):
    resource_arn, err = _resolve_tag_resource_arn(resource_arn)
    if err:
        return err
    tags_dict = body.get("Tags", {})
    _apply_tags(resource_arn, tags_dict)
    return _json(204, {})


def _untag_resource(resource_arn, tag_keys):
    resource_arn, err = _resolve_tag_resource_arn(resource_arn)
    if err:
        return err
    if resource_arn in _tags:
        for key in tag_keys:
            _tags[resource_arn].pop(key, None)
    return _json(204, {})


def _list_tags_for_resource(resource_arn):
    resource_arn, err = _resolve_tag_resource_arn(resource_arn)
    if err:
        return err
    return _json(200, {"Tags": _tags.get(resource_arn, {})})


# ---------------------------------------------------------------------------
# Data Plane — StartConfigurationSession / GetLatestConfiguration
# ---------------------------------------------------------------------------


def _start_configuration_session(body):
    app_identifier = body.get("ApplicationIdentifier", "")
    env_identifier = body.get("EnvironmentIdentifier", "")
    profile_identifier = body.get("ConfigurationProfileIdentifier", "")

    if not app_identifier or not env_identifier or not profile_identifier:
        return _error(400, "BadRequestException",
                      "ApplicationIdentifier, EnvironmentIdentifier, and "
                      "ConfigurationProfileIdentifier are required")

    poll_interval = body.get("RequiredMinimumPollIntervalInSeconds")
    if poll_interval is not None and (not isinstance(poll_interval, int) or not 15 <= poll_interval <= 86400):
        return _error(400, "BadRequestException",
                      "RequiredMinimumPollIntervalInSeconds must be between 15 and 86400")

    app_id = _resolve_application_id(app_identifier)
    if not app_id:
        return _error(404, "ResourceNotFoundException", f"Application {app_identifier} not found")

    env_id = _resolve_environment_id(app_id, env_identifier)
    if not env_id:
        return _error(404, "ResourceNotFoundException", f"Environment {env_identifier} not found")

    profile_id = _resolve_configuration_profile_id(app_id, profile_identifier)
    if not profile_id:
        return _error(404, "ResourceNotFoundException", f"Configuration profile {profile_identifier} not found")

    token = uuid.uuid4().hex
    _sessions[token] = {
        "ApplicationIdentifier": app_id,
        "EnvironmentIdentifier": env_id,
        "ConfigurationProfileIdentifier": profile_id,
        # Survives token rotation, so a client keeps its place in a gradual rollout.
        "SessionId": token,
        "PollIntervalInSeconds": poll_interval or _DEFAULT_POLL_INTERVAL_SECONDS,
        "LastServedVersion": None,
    }
    logger.info("StartConfigurationSession: app=%s env=%s profile=%s", app_id, env_id, profile_id)
    return _json(201, {"InitialConfigurationToken": token})


def _rollout_bucket(session_id):
    """A stable 0-99 slot for a session; it receives a DEPLOYING version once
    PercentageComplete passes its slot. AWS does not document how it picks
    targets, so this is MiniStack's choice."""
    return int(hashlib.sha256(session_id.encode("utf-8")).hexdigest(), 16) % 100


def _served_deployment(app_id, env_id, profile_id, session_id):
    """The deployment whose configuration this session receives: the newest
    one that is COMPLETE or BAKING, or DEPLOYING far enough to reach the session."""
    deployments = [d for d in _refresh_environment(app_id, env_id)
                   if d.get("ConfigurationProfileId") == profile_id]
    for deployment in reversed(deployments):
        state = deployment.get("State")
        if state in ("COMPLETE", "BAKING"):
            return deployment
        if state == "DEPLOYING" and _rollout_bucket(session_id) < deployment.get("PercentageComplete", 0.0):
            return deployment
    return None


def _retrieval_time_content(app_id, profile_id, content: bytes) -> bytes:
    """Feature flags are served in retrieval-time format: the `values` map
    lifted to the top level. Anything else is served verbatim."""
    profile = _config_profiles.get(f"{app_id}/{profile_id}") or {}
    if profile.get("Type") != "AWS.AppConfig.FeatureFlags":
        return content
    try:
        document = json.loads(content)
    except (ValueError, TypeError):
        return content
    if not isinstance(document, dict) or not isinstance(document.get("values"), dict):
        return content
    return json.dumps(document["values"]).encode("utf-8")


def _get_latest_configuration(token):
    session = _sessions.get(token)
    if not session:
        return _error(400, "BadRequestException", "Invalid or expired configuration token")

    # Each token is good for one call.
    del _sessions[token]

    app_id = session["ApplicationIdentifier"]
    env_id = session["EnvironmentIdentifier"]
    profile_id = session["ConfigurationProfileIdentifier"]
    session_id = session.get("SessionId", token)
    session = {**session, "SessionId": session_id}

    content = b""
    content_type = "application/octet-stream"
    version_label = None
    deployment = _served_deployment(app_id, env_id, profile_id, session_id)
    if deployment:
        cfg_version = deployment.get("ConfigurationVersion", "")
        version_record = _hosted_versions.get(f"{app_id}/{profile_id}/{cfg_version}")
        if version_record:
            content_type = version_record.get("ContentType", "application/octet-stream")
            version_label = version_record.get("VersionLabel")
            # GetLatestConfiguration "may return empty configuration data if the
            # client already has the latest version".
            if session.get("LastServedVersion") != cfg_version:
                raw = version_record["Content"]
                content = raw if isinstance(raw, bytes) else raw.encode("utf-8")
                content = _retrieval_time_content(app_id, profile_id, content)
                session["LastServedVersion"] = cfg_version

    next_token = uuid.uuid4().hex
    _sessions[next_token] = session

    resp_headers = {
        "Content-Type": content_type,
        "Next-Poll-Configuration-Token": next_token,
        "Next-Poll-Interval-In-Seconds": str(session.get("PollIntervalInSeconds", _DEFAULT_POLL_INTERVAL_SECONDS)),
    }
    if version_label:
        resp_headers["Version-Label"] = version_label
    return 200, resp_headers, content


# ---------------------------------------------------------------------------
# Request router
# ---------------------------------------------------------------------------


async def handle_request(method, path, headers, body_bytes, query_params):
    query = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}

    # --- Data plane paths ---
    if path == "/configurationsessions" and method == "POST":
        try:
            data = json.loads(body_bytes) if body_bytes else {}
        except json.JSONDecodeError:
            return _error(400, "BadRequestException", "Invalid JSON")
        return await _a(_start_configuration_session(data))

    if path == "/configuration" and method == "GET":
        token = query.get("configuration_token", "")
        if not token:
            return _error(400, "BadRequestException", "configuration_token is required")
        return await _a(_get_latest_configuration(token))

    # --- Control plane: tags ---
    m = re.fullmatch(r"/tags/(.+)", path)
    if m:
        resource_arn = m.group(1)
        if method == "POST":
            try:
                data = json.loads(body_bytes) if body_bytes else {}
            except json.JSONDecodeError:
                return _error(400, "BadRequestException", "Invalid JSON")
            return await _a(_tag_resource(resource_arn, data))
        if method == "GET":
            return await _a(_list_tags_for_resource(resource_arn))
        if method == "DELETE":
            tag_keys = query.get("tagKeys", "")
            keys = [k.strip() for k in tag_keys.split(",") if k.strip()] if tag_keys else []
            return await _a(_untag_resource(resource_arn, keys))

    # --- Control plane: parse JSON body for non-hosted-version routes ---
    content_type = headers.get("content-type", "")

    # Hosted configuration versions — body is raw content, not JSON
    m = re.fullmatch(
        r"/applications/([^/]+)/configurationprofiles/([^/]+)/hostedconfigurationversions",
        path,
    )
    if m and method == "POST":
        app_id, profile_id = m.group(1), m.group(2)
        ct = content_type or "application/octet-stream"
        return await _a(_create_hosted_configuration_version(app_id, profile_id, body_bytes, ct,
                                                             headers.get("versionlabel")))

    m = re.fullmatch(
        r"/applications/([^/]+)/configurationprofiles/([^/]+)/hostedconfigurationversions/(\d+)",
        path,
    )
    if m:
        app_id, profile_id, ver = m.group(1), m.group(2), int(m.group(3))
        if method == "GET":
            return await _a(_get_hosted_configuration_version(app_id, profile_id, ver))
        if method == "DELETE":
            return await _a(_delete_hosted_configuration_version(app_id, profile_id, ver))

    m = re.fullmatch(
        r"/applications/([^/]+)/configurationprofiles/([^/]+)/hostedconfigurationversions",
        path,
    )
    if m and method == "GET":
        app_id, profile_id = m.group(1), m.group(2)
        return await _a(_list_hosted_configuration_versions(app_id, profile_id, query))

    # JSON body for remaining routes
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {}

    # --- Applications ---
    if path == "/applications":
        if method == "POST":
            return await _a(_create_application(body))
        if method == "GET":
            return await _a(_list_applications(query))

    m = re.fullmatch(r"/applications/([^/]+)", path)
    if m:
        app_id = m.group(1)
        if method == "GET":
            return await _a(_get_application(app_id))
        if method == "PATCH":
            return await _a(_update_application(app_id, body))
        if method == "DELETE":
            return await _a(_delete_application(app_id))

    # --- Deployments (must be checked before environments) ---
    m = re.fullmatch(r"/applications/([^/]+)/environments/([^/]+)/deployments", path)
    if m:
        app_id, env_id = m.group(1), m.group(2)
        if method == "POST":
            return await _a(_start_deployment(app_id, env_id, body))
        if method == "GET":
            return await _a(_list_deployments(app_id, env_id, query))

    m = re.fullmatch(r"/applications/([^/]+)/environments/([^/]+)/deployments/(\d+)", path)
    if m:
        app_id, env_id, deploy_num = m.group(1), m.group(2), int(m.group(3))
        if method == "GET":
            return await _a(_get_deployment(app_id, env_id, deploy_num))
        if method == "DELETE":
            allow_revert = str(headers.get("allow-revert", "")).lower() == "true"
            return await _a(_stop_deployment(app_id, env_id, deploy_num, allow_revert))

    # --- Environments ---
    m = re.fullmatch(r"/applications/([^/]+)/environments", path)
    if m:
        app_id = m.group(1)
        if method == "POST":
            return await _a(_create_environment(app_id, body))
        if method == "GET":
            return await _a(_list_environments(app_id, query))

    m = re.fullmatch(r"/applications/([^/]+)/environments/([^/]+)", path)
    if m:
        app_id, env_id = m.group(1), m.group(2)
        if method == "GET":
            return await _a(_get_environment(app_id, env_id))
        if method == "PATCH":
            return await _a(_update_environment(app_id, env_id, body))
        if method == "DELETE":
            return await _a(_delete_environment(app_id, env_id))

    # --- Configuration Profiles ---
    m = re.fullmatch(r"/applications/([^/]+)/configurationprofiles", path)
    if m:
        app_id = m.group(1)
        if method == "POST":
            return await _a(_create_configuration_profile(app_id, body))
        if method == "GET":
            return await _a(_list_configuration_profiles(app_id, query))

    m = re.fullmatch(r"/applications/([^/]+)/configurationprofiles/([^/]+)", path)
    if m:
        app_id, profile_id = m.group(1), m.group(2)
        if method == "GET":
            return await _a(_get_configuration_profile(app_id, profile_id))
        if method == "PATCH":
            return await _a(_update_configuration_profile(app_id, profile_id, body))
        if method == "DELETE":
            return await _a(_delete_configuration_profile(app_id, profile_id))

    # --- Deployment Strategies ---
    # botocore's model uses the misspelled path "/deployementstrategies" for
    # DeleteDeploymentStrategy (and possibly others), so accept both spellings.
    if path in ("/deploymentstrategies", "/deployementstrategies"):
        if method == "POST":
            return await _a(_create_deployment_strategy(body))
        if method == "GET":
            return await _a(_list_deployment_strategies(query))

    m = re.fullmatch(r"/deploy(?:e?)mentstrategies/([^/]+)", path)
    if m:
        strategy_id = m.group(1)
        if method == "GET":
            return await _a(_get_deployment_strategy(strategy_id))
        if method == "PATCH":
            return await _a(_update_deployment_strategy(strategy_id, body))
        if method == "DELETE":
            return await _a(_delete_deployment_strategy(strategy_id))

    return _error(400, "BadRequestException", f"Unknown AppConfig path: {method} {path}")


async def _a(result):
    return result


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


def _json(status, data):
    if status == 204:
        return status, {}, b""
    body = json.dumps(data).encode("utf-8")
    return status, {"Content-Type": "application/json"}, body


def _error(status, code, message):
    body = json.dumps({"__type": code, "Message": message, "Code": code, "message": message}).encode("utf-8")
    return status, {"Content-Type": "application/json", "x-amzn-errortype": code}, body


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------


def reset():
    _applications.clear()
    _environments.clear()
    _config_profiles.clear()
    _hosted_versions.clear()
    _hosted_version_counters.clear()
    _deployment_strategies.clear()
    _deployments.clear()
    _tags.clear()
    _sessions.clear()
