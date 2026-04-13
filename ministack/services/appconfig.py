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
import json
import logging
import os
import re
import time
import uuid

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import AccountScopedDict, get_account_id

logger = logging.getLogger("appconfig")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_applications = AccountScopedDict()
_environments = AccountScopedDict()          # "{app_id}/{env_id}" -> record
_config_profiles = AccountScopedDict()       # "{app_id}/{profile_id}" -> record
_hosted_versions = AccountScopedDict()       # "{app_id}/{profile_id}/{version}" -> record
_deployment_strategies = AccountScopedDict()
_deployments = AccountScopedDict()           # "{app_id}/{env_id}/{deploy_num}" -> record
_tags = AccountScopedDict()                  # arn -> {key: value}
_sessions = AccountScopedDict()              # token -> session record

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def get_state():
    return copy.deepcopy({
        "applications": _applications,
        "environments": _environments,
        "config_profiles": _config_profiles,
        "hosted_versions": _hosted_versions,
        "deployment_strategies": _deployment_strategies,
        "deployments": _deployments,
        "tags": _tags,
    })


def restore_state(data):
    _applications.update(data.get("applications", {}))
    _environments.update(data.get("environments", {}))
    _config_profiles.update(data.get("config_profiles", {}))
    _hosted_versions.update(data.get("hosted_versions", {}))
    _deployment_strategies.update(data.get("deployment_strategies", {}))
    _deployments.update(data.get("deployments", {}))
    _tags.update(data.get("tags", {}))


_restored = load_state("appconfig")
if _restored:
    restore_state(_restored)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _gen_id():
    return uuid.uuid4().hex[:7]


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def _app_arn(app_id):
    return f"arn:aws:appconfig:{REGION}:{get_account_id()}:application/{app_id}"


def _env_arn(app_id, env_id):
    return f"arn:aws:appconfig:{REGION}:{get_account_id()}:application/{app_id}/environment/{env_id}"


def _profile_arn(app_id, profile_id):
    return f"arn:aws:appconfig:{REGION}:{get_account_id()}:application/{app_id}/configurationprofile/{profile_id}"


def _strategy_arn(strategy_id):
    return f"arn:aws:appconfig:{REGION}:{get_account_id()}:deploymentstrategy/{strategy_id}"


def _deployment_arn(app_id, env_id, deploy_num):
    return (
        f"arn:aws:appconfig:{REGION}:{get_account_id()}:"
        f"application/{app_id}/environment/{env_id}/deployment/{deploy_num}"
    )


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
    keys_to_remove = [k for k in _deployments if k.startswith(f"{app_id}/")]
    for k in keys_to_remove:
        _deployments.pop(k, None)
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
    _tags.pop(_profile_arn(app_id, profile_id), None)
    keys_to_remove = [k for k in _hosted_versions if k.startswith(f"{app_id}/{profile_id}/")]
    for k in keys_to_remove:
        _hosted_versions.pop(k, None)
    return _json(204, {})


# ---------------------------------------------------------------------------
# Hosted Configuration Versions
# ---------------------------------------------------------------------------


def _create_hosted_configuration_version(app_id, profile_id, body, content_type):
    if f"{app_id}/{profile_id}" not in _config_profiles:
        return _error(404, "ResourceNotFoundException", f"Configuration profile {profile_id} not found")

    existing = [
        v for k, v in _hosted_versions.items()
        if k.startswith(f"{app_id}/{profile_id}/")
    ]
    version_number = len(existing) + 1

    record = {
        "ApplicationId": app_id,
        "ConfigurationProfileId": profile_id,
        "VersionNumber": version_number,
        "ContentType": content_type,
        "Content": body,
        "Description": "",
    }
    _hosted_versions[f"{app_id}/{profile_id}/{version_number}"] = record
    logger.info("CreateHostedConfigurationVersion: %s/%s v%d", app_id, profile_id, version_number)

    resp_headers = {
        "Content-Type": content_type,
        "Application-Id": app_id,
        "Configuration-Profile-Id": profile_id,
        "Version-Number": str(version_number),
    }
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


def _get_deployment_strategy(strategy_id):
    strategy = _deployment_strategies.get(strategy_id)
    if not strategy:
        return _error(404, "ResourceNotFoundException", f"Deployment strategy {strategy_id} not found")
    return _json(200, strategy)


def _list_deployment_strategies(query):
    max_results = int(query.get("max_results", 50))
    items = list(_deployment_strategies.values())
    return _json(200, {"Items": items[:max_results]})


def _update_deployment_strategy(strategy_id, body):
    strategy = _deployment_strategies.get(strategy_id)
    if not strategy:
        return _error(404, "ResourceNotFoundException", f"Deployment strategy {strategy_id} not found")
    for field in ("Description", "DeploymentDurationInMinutes", "GrowthType",
                  "GrowthFactor", "FinalBakeTimeInMinutes"):
        if field in body:
            strategy[field] = body[field]
    return _json(200, strategy)


def _delete_deployment_strategy(strategy_id):
    if strategy_id not in _deployment_strategies:
        return _error(404, "ResourceNotFoundException", f"Deployment strategy {strategy_id} not found")
    del _deployment_strategies[strategy_id]
    _tags.pop(_strategy_arn(strategy_id), None)
    return _json(204, {})


# ---------------------------------------------------------------------------
# Deployments
# ---------------------------------------------------------------------------


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

    existing = [
        v for k, v in _deployments.items()
        if k.startswith(f"{app_id}/{env_id}/")
    ]
    deploy_num = len(existing) + 1

    now = _now_iso()
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
        "DeploymentDurationInMinutes": 0,
        "GrowthType": "LINEAR",
        "GrowthFactor": 100.0,
        "FinalBakeTimeInMinutes": 0,
        "State": "COMPLETE",
        "PercentageComplete": 100.0,
        "StartedAt": now,
        "CompletedAt": now,
    }
    _deployments[f"{app_id}/{env_id}/{deploy_num}"] = record
    logger.info("StartDeployment: %s/%s #%d (profile=%s, version=%s)",
                app_id, env_id, deploy_num, profile_id, version)
    return _json(201, record)


def _get_deployment(app_id, env_id, deploy_num):
    record = _deployments.get(f"{app_id}/{env_id}/{deploy_num}")
    if not record:
        return _error(404, "ResourceNotFoundException", f"Deployment {deploy_num} not found")
    return _json(200, record)


def _list_deployments(app_id, env_id, query):
    if f"{app_id}/{env_id}" not in _environments:
        return _error(404, "ResourceNotFoundException", f"Environment {env_id} not found")
    max_results = int(query.get("max_results", 50))
    items = []
    for k, v in _deployments.items():
        if k.startswith(f"{app_id}/{env_id}/"):
            items.append({
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
                "CompletedAt": v.get("CompletedAt", ""),
            })
    return _json(200, {"Items": items[:max_results]})


def _stop_deployment(app_id, env_id, deploy_num):
    record = _deployments.get(f"{app_id}/{env_id}/{deploy_num}")
    if not record:
        return _error(404, "ResourceNotFoundException", f"Deployment {deploy_num} not found")
    record["State"] = "ROLLED_BACK"
    return _json(202, record)


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def _apply_tags(arn, tags_dict):
    if tags_dict:
        if arn not in _tags:
            _tags[arn] = {}
        _tags[arn].update(tags_dict)


def _tag_resource(resource_arn, body):
    tags_dict = body.get("Tags", {})
    _apply_tags(resource_arn, tags_dict)
    return _json(204, {})


def _untag_resource(resource_arn, tag_keys):
    if resource_arn in _tags:
        for key in tag_keys:
            _tags[resource_arn].pop(key, None)
    return _json(204, {})


def _list_tags_for_resource(resource_arn):
    return _json(200, {"Tags": _tags.get(resource_arn, {})})


# ---------------------------------------------------------------------------
# Data Plane — StartConfigurationSession / GetLatestConfiguration
# ---------------------------------------------------------------------------


def _start_configuration_session(body):
    app_id = body.get("ApplicationIdentifier", "")
    env_id = body.get("EnvironmentIdentifier", "")
    profile_id = body.get("ConfigurationProfileIdentifier", "")

    if not app_id or not env_id or not profile_id:
        return _error(400, "BadRequestException",
                      "ApplicationIdentifier, EnvironmentIdentifier, and "
                      "ConfigurationProfileIdentifier are required")

    token = uuid.uuid4().hex
    _sessions[token] = {
        "ApplicationIdentifier": app_id,
        "EnvironmentIdentifier": env_id,
        "ConfigurationProfileIdentifier": profile_id,
    }
    logger.info("StartConfigurationSession: app=%s env=%s profile=%s", app_id, env_id, profile_id)
    return _json(201, {"InitialConfigurationToken": token})


def _get_latest_configuration(token):
    session = _sessions.get(token)
    if not session:
        return _error(400, "BadRequestException", "Invalid or expired configuration token")

    # Remove the used token
    del _sessions[token]

    app_id = session["ApplicationIdentifier"]
    env_id = session["EnvironmentIdentifier"]
    profile_id = session["ConfigurationProfileIdentifier"]

    # Find the latest completed deployment for this app/env
    latest_deploy = None
    for k, v in _deployments.items():
        if (k.startswith(f"{app_id}/{env_id}/")
                and v.get("State") == "COMPLETE"
                and v.get("ConfigurationProfileId") == profile_id):
            if latest_deploy is None or v["DeploymentNumber"] > latest_deploy["DeploymentNumber"]:
                latest_deploy = v

    content = b""
    content_type = "application/octet-stream"
    if latest_deploy:
        cfg_version = latest_deploy.get("ConfigurationVersion", "")
        version_key = f"{app_id}/{profile_id}/{cfg_version}"
        version_record = _hosted_versions.get(version_key)
        if version_record:
            raw = version_record["Content"]
            content = raw if isinstance(raw, bytes) else raw.encode("utf-8")
            content_type = version_record.get("ContentType", "application/octet-stream")

    next_token = uuid.uuid4().hex
    _sessions[next_token] = session.copy()

    resp_headers = {
        "Content-Type": content_type,
        "Next-Poll-Configuration-Token": next_token,
        "Next-Poll-Interval-In-Seconds": "30",
    }
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
        return await _a(_create_hosted_configuration_version(app_id, profile_id, body_bytes, ct))

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
            return await _a(_stop_deployment(app_id, env_id, deploy_num))

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
    body = json.dumps({"Message": message, "Code": code}).encode("utf-8")
    return status, {"Content-Type": "application/json", "x-amzn-errortype": code}, body


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------


def reset():
    _applications.clear()
    _environments.clear()
    _config_profiles.clear()
    _hosted_versions.clear()
    _deployment_strategies.clear()
    _deployments.clear()
    _tags.clear()
    _sessions.clear()
