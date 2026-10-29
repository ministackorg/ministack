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

import base64
import binascii
import copy
import hashlib
import json
import logging
import os
import re
import time
import unicodedata
import uuid

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import AccountRegionScopedDict, get_account_id, get_region, request_scope
from ministack.services import kms, secretsmanager

logger = logging.getLogger("appconfig")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# A strategy minute is a real minute, as on AWS.
_DEPLOYMENT_MINUTE_SECONDS = 60.0

# Next-Poll-Interval-In-Seconds for a session that set no RequiredMinimumPollIntervalInSeconds.
_DEFAULT_POLL_INTERVAL_SECONDS = 30

_SECRETS_MANAGER_SCHEME = "secretsmanager://"

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


_RETRIEVAL_ROLE_ARN_PATTERN = (
    r"^((arn):(aws|aws-cn|aws-iso|aws-iso-[a-z]{1}|aws-us-gov|aws-eusc):(iam)::\d{12}:role[/].*)$")


def _is_secrets_manager_uri(location_uri):
    if location_uri.startswith(_SECRETS_MANAGER_SCHEME):
        return True
    try:
        return parse_arn(location_uri).service == "secretsmanager"
    except ArnParseError:
        return False


def _string_member_type_error(body, members):
    """AWS's error for a string member sent as another JSON type, which it
    reports before any other check, for the first such member by name."""
    for member in sorted(members):
        value = body.get(member)
        if value is None or isinstance(value, str):
            continue
        if isinstance(value, bool):
            message = f"{'TRUE' if value else 'FALSE'}_VALUE can not be converted to a String"
        elif isinstance(value, (int, float)):
            message = "NUMBER_VALUE can not be converted to a String"
        elif isinstance(value, dict):
            message = "Start of structure or map found where not expected."
        else:
            message = "Start of list found where not expected"
        return _error(400, "BadRequestException", message)
    return None


def _validation_error(errors):
    count = "1 validation error" if len(errors) == 1 else f"{len(errors)} validation errors"
    return _error(400, "BadRequestException", f"{count} detected: " + "; ".join(errors))


def _secrets_manager_error(message, error_code="ValidationException"):
    """A Secrets Manager error as AppConfig passes it on."""
    return _error(400, "BadRequestException",
                  f"{message} (Service: AWSSecretsManager; Status Code: 400; Error Code: "
                  f"{error_code}; Request ID: {uuid.uuid4()}; Proxy: null)")


def _secret_account(secret_id):
    """The account whose Secrets Manager holds *secret_id*: an ARN's own
    account, otherwise the caller's."""
    try:
        return parse_arn(secret_id).account_id
    except ArnParseError:
        return get_account_id()


def _retrieval_session(role_arn):
    role = parse_arn(role_arn)
    return (f"arn:{role.partition}:sts::{role.account_id}:assumed-role/"
            f"{role.resource.rsplit('/', 1)[-1]}/AwsAppConfig-ConfigurationRetriever")


def _retrieval_denied(secret_id, role_arn):
    return _secrets_manager_error(
        f"User: {_retrieval_session(role_arn)} is not authorized to perform: secretsmanager:GetSecretValue "
        f"on resource: {secret_id} because no resource-based policy allows the secretsmanager:GetSecretValue action",
        "AccessDeniedException")


def _secret_policy_denies(secret, role_arn):
    """Under AUTH=true another account's secret is read only if its resource policy lets the
    retrieval role call GetSecretValue. Called in the secret's own account scope."""
    from ministack.app import AUTH

    if not AUTH:
        return False
    from ministack.core.iam_evaluator import EvalContext, evaluate_resource_policy

    policy = secretsmanager._resource_policies.get(secret["ARN"])
    ctx = EvalContext(principal_arn=_retrieval_session(role_arn), principal_type="AssumedRole",
                      principal_account=parse_arn(role_arn).account_id, action="secretsmanager:GetSecretValue",
                      resource_arn=secret["ARN"], region=get_region())
    return not policy or evaluate_resource_policy(policy, ctx).decision != "Allow"


def _cross_account_secret_error(secret_id, location_uri, role_arn):
    """AWS reads another account's secret as the retrieval role. A secret
    that isn't named by its exact ARN can't be shared, so it is refused; one
    that can't be read (scheduled for deletion, without a value, under that
    account's AWS managed key) is not found."""
    secret = secretsmanager.find_secret(secret_id)
    if not secret or secret["ARN"] != secret_id or _secret_policy_denies(secret, role_arn):
        return _retrieval_denied(secret_id, role_arn)
    # Scheduled for deletion, without a value, or under that account's AWS
    # managed key, it is not found.
    if not secretsmanager.secret_exists(secret_id) or not _is_customer_managed_key(secret.get("KmsKeyId")):
        return _error(404, "ResourceNotFoundException", f"Secret {location_uri} not found")
    return None


def _kms_key_fields(identifier):
    """KmsKeyIdentifier and the KmsKeyArn AWS derives from it as given, without looking the key up."""
    if identifier.startswith("arn:"):
        arn = identifier
    else:
        prefix = f"arn:aws:kms:{get_region()}:{get_account_id()}:"
        arn = prefix + (identifier if identifier.startswith("alias/") else f"key/{identifier}")
    return {"KmsKeyIdentifier": identifier, "KmsKeyArn": arn}


def _is_customer_managed_key(key_id):
    """True if *key_id* names a key in MiniStack's KMS, which holds only
    customer managed keys; a key ARN is looked up in its own account. A key it
    can't resolve, such as the AWS managed aws/secretsmanager key, is not one."""
    try:
        spec = parse_arn(key_id or "")
    except ArnParseError:
        return kms._resolve_key(key_id) is not None
    with request_scope(spec.account_id, spec.region):
        return kms._resolve_key(key_id) is not None


_ACCOUNT_ID = re.compile(r"[0-9]{12}")
# The partitions AWS accepts in a secret ARN location.
_PARTITION = re.compile(r"aws|aws-cn|aws-iso|aws-iso-[a-z]|aws-us-gov|aws-eusc", re.ASCII)
_SECRET_NAME_CHARS = re.compile(r"[A-Za-z0-9/_+=.@!-]+", re.ASCII)
_SECRET_NAME = re.compile(r"[A-Za-z0-9/_+=.@!-]{1,512}", re.ASCII)


def _is_secret_arn(value):
    try:
        spec = parse_arn(value)
    except ArnParseError:
        return False
    return (spec.service == "secretsmanager" and bool(spec.region) and _ACCOUNT_ID.fullmatch(spec.account_id)
            and _PARTITION.fullmatch(spec.partition) is not None and spec.resource.startswith("secret:")
            and _SECRET_NAME_CHARS.fullmatch(spec.resource[len("secret:"):]) is not None)


def _secret_profile_request_error(location_uri, body):
    """CreateConfigurationProfile's request checks for a Secrets Manager
    location, which AWS makes before it looks up the application: the
    RetrievalRoleArn's format with the Name, the profile type, then a
    missing RetrievalRoleArn."""
    role, name = body.get("RetrievalRoleArn"), body.get("Name")
    errors = []
    if role is not None:
        # ASCII: Java's \d, which AWS's pattern uses, matches only 0-9.
        if not re.fullmatch(_RETRIEVAL_ROLE_ARN_PATTERN, role, re.ASCII):
            errors.append(f"Value '{role}' at 'retrievalRoleArn' failed to satisfy constraint: "
                          f"Member must satisfy regular expression pattern: {_RETRIEVAL_ROLE_ARN_PATTERN}")
        if len(role) < 20:
            errors.append(f"Value '{role}' at 'retrievalRoleArn' failed to satisfy constraint: "
                          f"Member must have length greater than or equal to 20")
    if name is None:
        errors.append("Value null at 'name' failed to satisfy constraint: Member must not be null")
    elif name == "":
        errors.append("Value '' at 'name' failed to satisfy constraint: Member must have length "
                      "greater than or equal to 1")
    if errors:
        return _validation_error(errors)
    if body.get("Type") == "AWS.AppConfig.FeatureFlags":
        return _error(400, "BadRequestException",
                      "ConfigurationProfiles of type 'AWS.AppConfig.FeatureFlags' can only have a "
                      "locationUri of 'hosted'")
    if role is None:
        return _validation_error([
            f"Value null at 'retrievalRoleArn' failed to satisfy constraint: Member must not be null "
            f"if the request has the following uri: {location_uri}"])
    return None


def _secret_location_error(location_uri, body):
    """CreateConfigurationProfile's checks of a Secrets Manager location
    itself, after the application is found."""
    role = body["RetrievalRoleArn"]
    secret_id = _secret_id_for_location(location_uri)
    if secret_id is None:
        return _error(400, "BadRequestException", f"The Uri: {location_uri} is of an unsupported type")
    # Secrets Manager reads what follows secretsmanager:// as a secret ARN or
    # name; an ARN location's name must be a valid name too.
    if location_uri.startswith(_SECRETS_MANAGER_SCHEME):
        bad_name = not _is_secret_arn(secret_id) and not _SECRET_NAME.fullmatch(secret_id)
    else:
        bad_name = not _is_secret_arn(secret_id)
    if bad_name:
        return _secrets_manager_error(
            "Invalid name. Must be a valid name containing alphanumeric characters, or any of the "
            "following: -/_+=.@!")
    account = _secret_account(secret_id)
    if account != get_account_id():
        with request_scope(account, get_region()):
            return _cross_account_secret_error(secret_id, location_uri, role)
    if not secretsmanager.secret_exists(secret_id):
        return _error(404, "ResourceNotFoundException", f"Secret {location_uri} not found")
    return None


def _create_configuration_profile(app_id, body):
    err = _string_member_type_error(body, ("LocationUri", "RetrievalRoleArn"))
    if err:
        return err
    location_uri = body.get("LocationUri", "hosted")
    secret_location = isinstance(location_uri, str) and _is_secrets_manager_uri(location_uri)
    if secret_location:
        err = _secret_profile_request_error(location_uri, body)
        if err:
            return err
    if app_id not in _applications:
        return _error(404, "ResourceNotFoundException", f"Application {app_id} not found")
    name = body.get("Name")
    if not name:
        return _error(400, "BadRequestException", "Name is required")
    if secret_location:
        err = _secret_location_error(location_uri, body)
        if err:
            return err
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
    if body.get("KmsKeyIdentifier"):
        record.update(_kms_key_fields(body["KmsKeyIdentifier"]))
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
    if "KmsKeyIdentifier" in body:
        profile.pop("KmsKeyIdentifier", None)
        profile.pop("KmsKeyArn", None)
        if body["KmsKeyIdentifier"]:
            profile.update(_kms_key_fields(body["KmsKeyIdentifier"]))
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


def _create_hosted_configuration_version(app_id, profile_id, body, content_type, version_label=None,
                                         description=None):
    if f"{app_id}/{profile_id}" not in _config_profiles:
        return _error(404, "ResourceNotFoundException", f"Configuration profile {profile_id} not found")

    version_number = _next_hosted_version_number(app_id, profile_id)

    record = {
        "ApplicationId": app_id,
        "ConfigurationProfileId": profile_id,
        "VersionNumber": version_number,
        "ContentType": content_type,
        "Content": body,
        "Description": description or "",
    }
    if version_label:
        record["VersionLabel"] = version_label
    kms_key_arn = _config_profiles[f"{app_id}/{profile_id}"].get("KmsKeyArn")
    if kms_key_arn:
        record["KmsKeyArn"] = kms_key_arn
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
    if description:
        resp_headers["Description"] = description
    if kms_key_arn:
        resp_headers["KmsKeyArn"] = kms_key_arn
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
    if record.get("Description"):
        resp_headers["Description"] = record["Description"]
    if record.get("KmsKeyArn"):
        resp_headers["KmsKeyArn"] = record["KmsKeyArn"]
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
                **({"KmsKeyArn": v["KmsKeyArn"]} if v.get("KmsKeyArn") else {}),
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


def _secret_id_for_location(location_uri):
    """The Secrets Manager secret a profile's LocationUri names
    (`secretsmanager://<name or ARN>` or a secret ARN), or None. AWS calls an
    ARN without a known partition, a region, a 12-digit account or a `secret:`
    resource "of an unsupported type"."""
    if location_uri.startswith(_SECRETS_MANAGER_SCHEME):
        return location_uri[len(_SECRETS_MANAGER_SCHEME):]
    try:
        spec = parse_arn(location_uri)
    except ArnParseError:
        return None
    if (spec.service != "secretsmanager" or not spec.region or not _ACCOUNT_ID.fullmatch(spec.account_id)
            or not _PARTITION.fullmatch(spec.partition) or not spec.resource.startswith("secret:")):
        return None
    return location_uri


def _first_utf16_units(text, count):
    """The first *count* UTF-16 code units of *text*, as Secrets Manager cuts
    a long value in its message; half of a split character shows as "?"."""
    units = text.encode("utf-16-le", "surrogatepass")[:count * 2]
    return units.decode("utf-16-le", "surrogatepass").encode("utf-8", "replace").decode("utf-8")


def _secret_version_length_error(version):
    """Secrets Manager's VersionId length check, as AppConfig passes it on."""
    if len(version) < 32:
        constraint = "Member must have length greater than or equal to 32"
    elif len(version) > 64:
        constraint = "Member must have length less than or equal to 64"
    else:
        return None
    shown = version if len(version) <= 64 else _first_utf16_units(version, 64) + " ..."
    return _error(400, "BadRequestException",
                  f"1 validation error detected: Value '{shown}' at 'versionId' failed to satisfy "
                  f"constraint: {constraint} (Service: AWSSecretsManager; Status Code: 400; "
                  f"Error Code: ValidationException; Request ID: {uuid.uuid4()}; Proxy: null)")


# What AWS types as JSON when it serves a SecretString. Its parser reads only
# the first JSON value: whitespace alone counts, anything after an array or
# object or a string is ignored, a number at the top level must be followed by
# whitespace or the end, and a literal can't run on into an identifier character. It allows 1000 levels of
# nesting, an integer of up to 1000 digits, and a number with a fraction or
# exponent of up to 1001 digits in all (signs don't count). Checked without
# recursion, so the depth doesn't depend on Python's recursion limit.
_MAX_JSON_DEPTH = 1000
_JSON_SPACE = " \t\n\r"
_JSON_NUMBER = re.compile(r"-?(0|[1-9][0-9]*)(?:\.([0-9]+))?(?:[eE][+-]?([0-9]+))?", re.ASCII)
_JSON_STRING = re.compile(r'"(?:[^"\\\x00-\x1f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"')
_JSON_LITERALS = ("true", "false", "null")


# Unicode categories Java counts as part of an identifier, besides letters and
# digits: currency signs, connectors, letter numbers, marks and format characters.
_JAVA_IDENTIFIER_CATEGORIES = {"Sc", "Pc", "Nl", "Mn", "Mc", "Cf"}


def _runs_into_literal(ch):
    """Whether *ch* right after true/false/null makes AWS refuse the literal:
    a character from "0" up, other than "]" and "}", that Java counts as part
    of an identifier (so "$" and "." don't, a letter, digit, "_", DEL or a
    C1 control does)."""
    if ch < "0" or ch in "]}":
        return False
    if ch.isalnum() or ch == "_" or "\x7f" <= ch <= "\x9f":
        return True
    return unicodedata.category(ch) in _JAVA_IDENTIFIER_CATEGORIES


def _json_scalar_end(text, i):
    """The end of the JSON number, string or literal at *i*, or -1."""
    match = _JSON_STRING.match(text, i)
    if match:
        return match.end()
    match = _JSON_NUMBER.match(text, i)
    if match:
        whole, fraction, exponent = match.groups()
        if fraction is None and exponent is None:
            too_long = len(whole) > 1000
        else:
            too_long = len(whole) + len(fraction or "") + len(exponent or "") > 1001
        return -1 if too_long else match.end()
    for literal in _JSON_LITERALS:
        if text.startswith(literal, i):
            end = i + len(literal)
            if end < len(text) and _runs_into_literal(text[end]):
                return -1
            return end
    return -1


def _is_json(text):
    def skip(i):
        while i < len(text) and text[i] in _JSON_SPACE:
            i += 1
        return i

    i = skip(0)
    if i == len(text):
        return True
    stack = []          # open "[" / "{"
    expect = "value"    # value | key | colon | next
    while True:
        i = skip(i)
        if i == len(text):
            return False
        ch = text[i]
        if expect == "value":
            if ch in "[{":
                stack.append(ch)
                if len(stack) > _MAX_JSON_DEPTH:
                    return False
                i = skip(i + 1)
                if i < len(text) and text[i] == ("]" if ch == "[" else "}"):
                    stack.pop()
                    i += 1
                    if not stack:
                        return True
                    expect = "next"
                else:
                    expect = "value" if ch == "[" else "key"
                continue
            end = _json_scalar_end(text, i)
            if end < 0:
                return False
            if not stack:
                return text[i].isalpha() or end == len(text) or text[end] in _JSON_SPACE or text[i] == '"'
            i, expect = end, "next"
        elif expect == "key":
            match = _JSON_STRING.match(text, i)
            if not match:
                return False
            i, expect = match.end(), "colon"
        elif expect == "colon":
            if ch != ":":
                return False
            i, expect = i + 1, "value"
        else:  # next: a comma or the close of the open container
            if ch == ",":
                i, expect = i + 1, ("value" if stack[-1] == "[" else "key")
            elif ch == ("]" if stack[-1] == "[" else "}"):
                stack.pop()
                i += 1
                if not stack:
                    return True
            else:
                return False


def _deployed_secret_content(secret_id, location_uri, version, body, role_arn=""):
    """Returns ((content, content_type), None) for the secret version a
    deployment names, or (None, error)."""
    err = _secret_version_length_error(version)
    if err:
        return None, err
    # Another account's secret, and its key, are read in that account.
    caller, owner = get_account_id(), _secret_account(secret_id)
    with request_scope(owner, get_region()):
        secret, secret_version = secretsmanager.resolve_secret_version(secret_id, version)
        customer_key = secret is not None and _is_customer_managed_key(secret.get("KmsKeyId"))
        denied = (secret is not None and owner != caller and role_arn
                  and _secret_policy_denies(secret, role_arn))
    if denied:
        return None, _retrieval_denied(secret_id, role_arn)
    # A version without a value (MiniStack's Secrets Manager makes one for a
    # secret created without a value) doesn't exist on AWS; the version is
    # checked before the key.
    if not secret_version or (secret_version.get("SecretString") is None
                              and secret_version.get("SecretBinary") is None):
        return None, _error(404, "ResourceNotFoundException", f"Secret {location_uri} not found")
    if customer_key and not body.get("KmsKeyIdentifier"):
        return None, _error(400, "BadRequestException",
                            "KmsKeyIdentifier required when deploying an AWS Secrets Manager secret "
                            "that is encrypted with a customer managed key (CMK)")
    if secret_version.get("SecretString") is None:
        try:
            content = base64.b64decode(secret_version["SecretBinary"], validate=True)
        except (binascii.Error, TypeError, ValueError):
            # AWS refuses such a SecretBinary at CreateSecret; MiniStack's
            # Secrets Manager stores it, so there is no version to serve.
            return None, _error(404, "ResourceNotFoundException", f"Secret {location_uri} not found")
        return (content, "application/octet-stream"), None
    text = secret_version["SecretString"]
    content_type = "application/json" if _is_json(text) else "application/octet-stream"
    # A lone surrogate comes back from AWS as "?".
    return (text.encode("utf-8", "replace"), content_type), None


_DEPLOYMENT_STRATEGY_ID_PATTERN = r"(^[a-z0-9]{4,7}$|^AppConfig\.[A-Za-z0-9]{9,40}$)"


def _kms_error(message, code):
    return _error(400, "BadRequestException",
                  f"{message} (Service: AWSKMS; Status Code: 400; Error Code: {code}; "
                  f"Request ID: {uuid.uuid4()}; Proxy: null)")


def _hosted_version_key_error(key_identifier, caller_arn):
    """StartDeployment of a hosted version encrypted with a customer managed key (captured on AWS)."""
    if not key_identifier:
        return _error(400, "BadRequestException",
                      "KmsKeyIdentifier required when deploying a hosted configuration version that is "
                      "encrypted with a customer managed key (CMK)")
    try:
        spec = parse_arn(key_identifier)
        scope = (spec.account_id, spec.region)
    except ArnParseError:
        scope = (get_account_id(), get_region())
    if scope[0] != get_account_id():
        return _kms_error(
            f"User: {caller_arn} is not authorized to perform: kms:GenerateDataKey on this resource because the "
            "resource does not exist in this Region, no resource-based policies allow access, or a resource-based "
            "policy explicitly denies access", "AccessDeniedException")
    with request_scope(*scope):
        key = kms._resolve_key(key_identifier)
    if key is None:
        return _kms_error(f"Key '{key_identifier}' does not exist", "NotFoundException")
    if key.get("KeyState") == "Disabled" or key.get("Enabled") is False:
        return _kms_error(f"{key_identifier} is disabled.", "DisabledException")
    return None


def _caller_arn(headers):
    from ministack.services import sts

    identity = sts._caller_identity(headers or {})
    if isinstance(identity, dict) and identity.get("userArn"):
        return identity["userArn"]
    return f"arn:aws:iam::{get_account_id()}:root"


def _start_deployment_validation_errors(body):
    """StartDeployment's member constraints, listed in the order AWS reports them."""
    errors = []
    for member, wire, required, max_length in (("ConfigurationVersion", "configurationVersion", True, 1024),
                                               ("KmsKeyIdentifier", "kmsKeyIdentifier", False, 2048),
                                               ("ConfigurationProfileId", "configurationProfileId", True, 128),
                                               ("Description", "description", False, 1024),
                                               ("DeploymentStrategyId", "deploymentStrategyId", True, None)):
        value = body.get(member)
        if value is None:
            if required:
                errors.append(f"Value null at '{wire}' failed to satisfy constraint: Member must not be null")
        elif member == "DeploymentStrategyId":
            if not re.fullmatch(_DEPLOYMENT_STRATEGY_ID_PATTERN, value, re.ASCII):
                errors.append(f"Value '{value}' at '{wire}' failed to satisfy constraint: Member must "
                              f"satisfy regular expression pattern: {_DEPLOYMENT_STRATEGY_ID_PATTERN}")
        elif value == "" and member != "Description":
            errors.append(f"Value '' at '{wire}' failed to satisfy constraint: Member must have "
                          f"length greater than or equal to 1")
        elif len(value) > max_length:
            errors.append(f"Value '{value}' at '{wire}' failed to satisfy constraint: Member must have "
                          f"length less than or equal to {max_length}")
    return errors


def _start_deployment(app_id, env_id, body, caller_arn=None):
    err = _string_member_type_error(body, ("ConfigurationVersion", "KmsKeyIdentifier", "ConfigurationProfileId",
                                           "DeploymentStrategyId", "Description"))
    if err:
        return err
    errors = _start_deployment_validation_errors(body)
    if errors:
        return _validation_error(errors)
    version = body["ConfigurationVersion"]

    if app_id not in _applications:
        return _error(404, "ResourceNotFoundException", f"Application {app_id} not found")
    if f"{app_id}/{env_id}" not in _environments:
        return _error(404, "ResourceNotFoundException", f"Environment {env_id} not found")

    strategy_id = body.get("DeploymentStrategyId", "")
    profile_id = body.get("ConfigurationProfileId", "")

    # After the request itself, AWS checks for a deployment in progress, then
    # the profile, the secret, the strategy and LatestDeploymentNumber.
    existing = _refresh_environment(app_id, env_id)
    in_progress = [d for d in existing if d["State"] in _IN_PROGRESS_STATES]
    if in_progress:
        state = _ENVIRONMENT_STATE_BY_DEPLOYMENT_STATE[in_progress[-1]["State"]]
        return _error(409, "ConflictException",
                      f"Environment {env_id} currently has a state of {state}. The current "
                      f"deployment must finish before a new deployment can be started.")

    if profile_id and f"{app_id}/{profile_id}" not in _config_profiles:
        return _error(404, "ResourceNotFoundException", f"Configuration profile {profile_id} not found")

    location_uri = _config_profiles.get(f"{app_id}/{profile_id}", {}).get("LocationUri", "hosted")
    secret_id = _secret_id_for_location(location_uri) if isinstance(location_uri, str) else None
    secret_content = None
    if secret_id is not None:
        role_arn = _config_profiles[f"{app_id}/{profile_id}"].get("RetrievalRoleArn", "")
        secret_content, err = _deployed_secret_content(secret_id, location_uri, version, body, role_arn)
        if err:
            return err
    elif _hosted_versions.get(f"{app_id}/{profile_id}/{version}", {}).get("KmsKeyArn"):
        err = _hosted_version_key_error(body.get("KmsKeyIdentifier"),
                                        caller_arn or f"arn:aws:iam::{get_account_id()}:root")
        if err:
            return err

    strategy = _find_deployment_strategy(strategy_id)
    if not strategy:
        return _error(404, "ResourceNotFoundException",
                      f"DeploymentStrategy with Id {strategy_id} could not be found.")

    latest_number = existing[-1]["DeploymentNumber"] if existing else 0
    expected = body.get("LatestDeploymentNumber")
    if expected is not None and expected != latest_number:
        return _error(409, "ConflictException",
                      f"LatestDeploymentNumber {expected} does not match the latest deployment "
                      f"number {latest_number}.")
    deploy_num = latest_number + 1

    now = time.time()
    record = {
        "ApplicationId": app_id,
        "EnvironmentId": env_id,
        "DeploymentStrategyId": strategy_id,
        "ConfigurationProfileId": profile_id,
        "DeploymentNumber": deploy_num,
        "ConfigurationName": _config_profiles.get(f"{app_id}/{profile_id}", {}).get("Name", ""),
        "ConfigurationLocationUri": location_uri if secret_id is not None else "hosted",
        "ConfigurationVersion": version,
        "Description": body.get("Description", ""),
        **_deployment_params_from_strategy(strategy),
        "State": "DEPLOYING",
        "PercentageComplete": 0.0,
        "StartedAt": _iso(now),
        "_StartedEpoch": now,
    }
    if secret_content is not None:
        # Kept with the deployment: AWS still serves it after the secret is deleted.
        record["_Content"], record["_ContentType"] = secret_content
    version_label = _hosted_versions.get(f"{app_id}/{profile_id}/{version}", {}).get("VersionLabel")
    if version_label:
        record["VersionLabel"] = version_label
    if body.get("KmsKeyIdentifier"):
        record.update(_kms_key_fields(body["KmsKeyIdentifier"]))
    _apply_tags(_deployment_arn(app_id, env_id, deploy_num), body.get("Tags") or {})
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
    for v in reversed(_refresh_environment(app_id, env_id)):
        item = {
            "DeploymentNumber": v["DeploymentNumber"],
            "ConfigurationProfileId": v.get("ConfigurationProfileId", ""),
            "ConfigurationName": v.get("ConfigurationName", ""),
            "ConfigurationVersion": v.get("ConfigurationVersion", ""),
            "DeploymentDurationInMinutes": v.get("DeploymentDurationInMinutes", 0),
            "GrowthType": v.get("GrowthType", "LINEAR"),
            "GrowthFactor": v.get("GrowthFactor", 100.0),
            "FinalBakeTimeInMinutes": v.get("FinalBakeTimeInMinutes", 0),
            "State": v.get("State", "COMPLETE"),
            "PercentageComplete": v.get("PercentageComplete", 100.0),
            "StartedAt": v.get("StartedAt", ""),
            "Type": "USER",
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
    # Only DEPLOYING stops without AllowRevert; with it, BAKING rolls back too.
    if state == "DEPLOYING" or (state == "BAKING" and allow_revert):
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
    err = _tag_arn_account_error(resource_arn)
    if err:
        return err
    resource_arn, err = _resolve_tag_resource_arn(resource_arn)
    if err:
        return err
    tags_dict = body.get("Tags", {})
    _apply_tags(resource_arn, tags_dict)
    return _json(204, {})


def _untag_resource(resource_arn, tag_keys):
    err = _tag_arn_account_error(resource_arn)
    if err:
        return err
    resource_arn, err = _resolve_tag_resource_arn(resource_arn)
    if err:
        return err
    if resource_arn in _tags:
        for key in tag_keys:
            _tags[resource_arn].pop(key, None)
    return _json(204, {})


def _tag_arn_account_error(resource_arn):
    """The tag APIs' answer for an AppConfig ARN without this account (captured on AWS)."""
    try:
        spec = parse_arn(resource_arn)
    except ArnParseError:
        return None
    if spec.service != "appconfig":
        return None
    if not spec.account_id:
        return _error(400, "BadRequestException",
                      f"ARN {resource_arn} is invalid due to not containing an account ID.")
    if spec.account_id != get_account_id():
        return _error(403, "AccessDeniedException",
                      f"Cross-account resources are not permitted for this API. ARN {resource_arn} references "
                      f"a resource outside of account {get_account_id()}.")
    return None


def _list_tags_for_resource(resource_arn):
    err = _tag_arn_account_error(resource_arn)
    if err:
        return err
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
        if "_Content" in deployment:
            version_record = {"Content": deployment["_Content"], "ContentType": deployment["_ContentType"]}
        else:
            version_record = _hosted_versions.get(f"{app_id}/{profile_id}/{cfg_version}")
        if version_record:
            content_type = version_record.get("ContentType", "application/octet-stream")
            version_label = version_record.get("VersionLabel")
            # GetLatestConfiguration "may return empty configuration data if the
            # client already has the latest version".
            if session.get("LastServedVersion") != cfg_version:
                raw = version_record["Content"]
                content = raw if isinstance(raw, bytes) else raw.encode("utf-8")
                if "_Content" not in deployment:  # only a hosted profile can be AWS.AppConfig.FeatureFlags
                    content = _retrieval_time_content(app_id, profile_id, content)
                session["LastServedVersion"] = cfg_version
            else:
                # AWS types the empty body of an unchanged poll as JSON.
                content_type = "application/json"

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
                                                             headers.get("versionlabel"),
                                                             headers.get("description")))

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
            return await _a(_start_deployment(app_id, env_id, body, _caller_arn(headers)))
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
