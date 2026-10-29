import asyncio
import json
import os
import re
import time

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from ministack.core.responses import get_account_id
from ministack.services import appconfig as appconfig_service
from ministack.services import cloudwatch as cloudwatch_service

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")


def _make_appconfig_client(region_name):
    return boto3.client(
        "appconfig",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region_name,
        config=Config(retries={"max_attempts": 0}),
    )


def _status_and_body(response):
    status, _, body = response
    return status, json.loads(body) if body else {}


def _tag_snapshot():
    return {arn: dict(tags) for arn, tags in appconfig_service._tags.items()}


@pytest.fixture
def appconfig_service_state():
    appconfig_service.reset()
    yield
    appconfig_service.reset()


# ---------------------------------------------------------------------------
# Applications
# ---------------------------------------------------------------------------


def test_appconfig_create_application(appconfig_client):
    resp = appconfig_client.create_application(Name="my-app", Description="Test app")
    assert resp["Id"]
    assert resp["Name"] == "my-app"
    assert resp["Description"] == "Test app"


def test_appconfig_get_application(appconfig_client):
    created = appconfig_client.create_application(Name="get-app")
    app_id = created["Id"]
    resp = appconfig_client.get_application(ApplicationId=app_id)
    assert resp["Id"] == app_id
    assert resp["Name"] == "get-app"


def test_appconfig_list_applications(appconfig_client):
    appconfig_client.create_application(Name="list-app-1")
    appconfig_client.create_application(Name="list-app-2")
    resp = appconfig_client.list_applications()
    names = [a["Name"] for a in resp["Items"]]
    assert "list-app-1" in names
    assert "list-app-2" in names


def test_appconfig_update_application(appconfig_client):
    created = appconfig_client.create_application(Name="update-app")
    app_id = created["Id"]
    resp = appconfig_client.update_application(ApplicationId=app_id, Name="renamed-app", Description="new desc")
    assert resp["Name"] == "renamed-app"
    assert resp["Description"] == "new desc"


def test_appconfig_delete_application(appconfig_client):
    created = appconfig_client.create_application(Name="delete-app")
    app_id = created["Id"]
    appconfig_client.delete_application(ApplicationId=app_id)
    with pytest.raises(ClientError) as exc:
        appconfig_client.get_application(ApplicationId=app_id)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Environments
# ---------------------------------------------------------------------------


def test_appconfig_create_environment(appconfig_client):
    app = appconfig_client.create_application(Name="env-app")
    resp = appconfig_client.create_environment(
        ApplicationId=app["Id"],
        Name="dev",
        Description="Development",
    )
    assert resp["Id"]
    assert resp["Name"] == "dev"
    assert resp["State"] == "READY_FOR_DEPLOYMENT"


def test_appconfig_get_environment(appconfig_client):
    app = appconfig_client.create_application(Name="env-get-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="staging")
    resp = appconfig_client.get_environment(ApplicationId=app["Id"], EnvironmentId=env["Id"])
    assert resp["Name"] == "staging"


def test_appconfig_list_environments(appconfig_client):
    app = appconfig_client.create_application(Name="env-list-app")
    appconfig_client.create_environment(ApplicationId=app["Id"], Name="env-a")
    appconfig_client.create_environment(ApplicationId=app["Id"], Name="env-b")
    resp = appconfig_client.list_environments(ApplicationId=app["Id"])
    names = [e["Name"] for e in resp["Items"]]
    assert "env-a" in names
    assert "env-b" in names


def test_appconfig_update_environment(appconfig_client):
    app = appconfig_client.create_application(Name="env-update-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="old-name")
    resp = appconfig_client.update_environment(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
        Name="new-name",
        Description="updated",
    )
    assert resp["Name"] == "new-name"
    assert resp["Description"] == "updated"


def test_appconfig_delete_environment(appconfig_client):
    app = appconfig_client.create_application(Name="env-delete-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="to-delete")
    appconfig_client.delete_environment(ApplicationId=app["Id"], EnvironmentId=env["Id"])
    with pytest.raises(ClientError) as exc:
        appconfig_client.get_environment(ApplicationId=app["Id"], EnvironmentId=env["Id"])
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_appconfig_delete_environment_drops_its_deployments(appconfig_client):
    """A deployment of a deleted environment is not found."""
    app = appconfig_client.create_application(Name="env-delete-deploy-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="to-delete")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="env-delete-deploy-profile", LocationUri="hosted")
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"], ConfigurationProfileId=profile["Id"],
        Content=b"config", ContentType="text/plain")
    strategy = appconfig_client.create_deployment_strategy(
        Name="env-delete-deploy-strat", DeploymentDurationInMinutes=0,
        GrowthFactor=100.0, ReplicateTo="NONE")
    ids = {"ApplicationId": app["Id"], "EnvironmentId": env["Id"]}
    number = appconfig_client.start_deployment(
        **ids, DeploymentStrategyId=strategy["Id"], ConfigurationProfileId=profile["Id"],
        ConfigurationVersion="1")["DeploymentNumber"]
    appconfig_client.delete_environment(**ids)
    with pytest.raises(ClientError) as exc:
        appconfig_client.get_deployment(**ids, DeploymentNumber=number)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Configuration Profiles
# ---------------------------------------------------------------------------


def test_appconfig_create_configuration_profile(appconfig_client):
    app = appconfig_client.create_application(Name="profile-app")
    resp = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"],
        Name="my-config",
        LocationUri="hosted",
        Type="AWS.Freeform",
    )
    assert resp["Id"]
    assert resp["Name"] == "my-config"
    assert resp["LocationUri"] == "hosted"


def test_appconfig_get_configuration_profile(appconfig_client):
    app = appconfig_client.create_application(Name="profile-get-app")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="get-profile", LocationUri="hosted",
    )
    resp = appconfig_client.get_configuration_profile(
        ApplicationId=app["Id"], ConfigurationProfileId=profile["Id"],
    )
    assert resp["Name"] == "get-profile"


def test_appconfig_list_configuration_profiles(appconfig_client):
    app = appconfig_client.create_application(Name="profile-list-app")
    appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="profile-1", LocationUri="hosted",
    )
    appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="profile-2", LocationUri="hosted",
    )
    resp = appconfig_client.list_configuration_profiles(ApplicationId=app["Id"])
    names = [p["Name"] for p in resp["Items"]]
    assert "profile-1" in names
    assert "profile-2" in names


def test_appconfig_update_configuration_profile(appconfig_client):
    app = appconfig_client.create_application(Name="profile-update-app")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="old-profile", LocationUri="hosted",
    )
    resp = appconfig_client.update_configuration_profile(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Name="new-profile",
        Description="updated desc",
    )
    assert resp["Name"] == "new-profile"
    assert resp["Description"] == "updated desc"


def test_appconfig_delete_configuration_profile(appconfig_client):
    app = appconfig_client.create_application(Name="profile-delete-app")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="to-delete", LocationUri="hosted",
    )
    appconfig_client.delete_configuration_profile(
        ApplicationId=app["Id"], ConfigurationProfileId=profile["Id"],
    )
    with pytest.raises(ClientError) as exc:
        appconfig_client.get_configuration_profile(
            ApplicationId=app["Id"], ConfigurationProfileId=profile["Id"],
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Hosted Configuration Versions
# ---------------------------------------------------------------------------


def test_appconfig_create_hosted_configuration_version(appconfig_client):
    app = appconfig_client.create_application(Name="hcv-app")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="hcv-profile", LocationUri="hosted",
    )
    content = json.dumps({"feature_flag": True}).encode("utf-8")
    resp = appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=content,
        ContentType="application/json",
    )
    assert resp["VersionNumber"] == 1
    assert resp["ContentType"] == "application/json"
    assert resp["Content"].read() == content


def test_appconfig_get_hosted_configuration_version(appconfig_client):
    app = appconfig_client.create_application(Name="hcv-get-app")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="hcv-get-profile", LocationUri="hosted",
    )
    content = b'{"key":"value"}'
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=content,
        ContentType="application/json",
    )
    resp = appconfig_client.get_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        VersionNumber=1,
    )
    assert resp["Content"].read() == content


def test_appconfig_list_hosted_configuration_versions(appconfig_client):
    app = appconfig_client.create_application(Name="hcv-list-app")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="hcv-list-profile", LocationUri="hosted",
    )
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=b"v1",
        ContentType="text/plain",
    )
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=b"v2",
        ContentType="text/plain",
    )
    resp = appconfig_client.list_hosted_configuration_versions(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
    )
    assert len(resp["Items"]) == 2
    versions = [i["VersionNumber"] for i in resp["Items"]]
    assert 1 in versions
    assert 2 in versions


def test_appconfig_delete_hosted_configuration_version(appconfig_client):
    app = appconfig_client.create_application(Name="hcv-del-app")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="hcv-del-profile", LocationUri="hosted",
    )
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=b"data",
        ContentType="text/plain",
    )
    appconfig_client.delete_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        VersionNumber=1,
    )
    with pytest.raises(ClientError) as exc:
        appconfig_client.get_hosted_configuration_version(
            ApplicationId=app["Id"],
            ConfigurationProfileId=profile["Id"],
            VersionNumber=1,
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_appconfig_hosted_version_number_is_never_reused(appconfig_client):
    """A version created after deleting one gets a fresh number."""
    app = appconfig_client.create_application(Name="hcv-reuse-app")
    ids = {"ApplicationId": app["Id"],
           "ConfigurationProfileId": appconfig_client.create_configuration_profile(
               ApplicationId=app["Id"], Name="hcv-reuse-profile", LocationUri="hosted")["Id"]}

    def create(content):
        return appconfig_client.create_hosted_configuration_version(
            **ids, Content=content, ContentType="text/plain")["VersionNumber"]

    assert [create(b"v1"), create(b"v2")] == [1, 2]
    appconfig_client.delete_hosted_configuration_version(**ids, VersionNumber=1)
    assert create(b"v3") == 3
    appconfig_client.delete_hosted_configuration_version(**ids, VersionNumber=3)
    assert create(b"v4") == 4
    listed = appconfig_client.list_hosted_configuration_versions(**ids)["Items"]
    assert sorted(i["VersionNumber"] for i in listed) == [2, 4]
    assert appconfig_client.get_hosted_configuration_version(
        **ids, VersionNumber=2)["Content"].read() == b"v2"


# ---------------------------------------------------------------------------
# Deployment Strategies
# ---------------------------------------------------------------------------


def test_appconfig_create_deployment_strategy(appconfig_client):
    resp = appconfig_client.create_deployment_strategy(
        Name="quick-deploy",
        DeploymentDurationInMinutes=0,
        GrowthFactor=100.0,
        ReplicateTo="NONE",
    )
    assert resp["Id"]
    assert resp["Name"] == "quick-deploy"
    assert resp["GrowthFactor"] == 100.0


def test_appconfig_get_deployment_strategy(appconfig_client):
    created = appconfig_client.create_deployment_strategy(
        Name="get-strategy",
        DeploymentDurationInMinutes=10,
        GrowthFactor=50.0,
        ReplicateTo="NONE",
    )
    resp = appconfig_client.get_deployment_strategy(DeploymentStrategyId=created["Id"])
    assert resp["Name"] == "get-strategy"
    assert resp["DeploymentDurationInMinutes"] == 10


def test_appconfig_list_deployment_strategies(appconfig_client):
    appconfig_client.create_deployment_strategy(
        Name="list-strat-1", DeploymentDurationInMinutes=0, GrowthFactor=100.0, ReplicateTo="NONE",
    )
    resp = appconfig_client.list_deployment_strategies()
    assert len(resp["Items"]) >= 1


def test_appconfig_update_deployment_strategy(appconfig_client):
    created = appconfig_client.create_deployment_strategy(
        Name="upd-strategy", DeploymentDurationInMinutes=5, GrowthFactor=50.0, ReplicateTo="NONE",
    )
    resp = appconfig_client.update_deployment_strategy(
        DeploymentStrategyId=created["Id"],
        Description="updated",
        GrowthFactor=75.0,
    )
    assert resp["Description"] == "updated"
    assert resp["GrowthFactor"] == 75.0


def test_appconfig_delete_deployment_strategy(appconfig_client):
    created = appconfig_client.create_deployment_strategy(
        Name="del-strategy", DeploymentDurationInMinutes=0, GrowthFactor=100.0, ReplicateTo="NONE",
    )
    appconfig_client.delete_deployment_strategy(DeploymentStrategyId=created["Id"])
    with pytest.raises(ClientError) as exc:
        appconfig_client.get_deployment_strategy(DeploymentStrategyId=created["Id"])
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# As ListDeploymentStrategies returned them on AWS, in AWS's order.
_AWS_PREDEFINED_STRATEGIES = [
    {"Id": "AppConfig.AllAtOnce", "Name": "AppConfig.AllAtOnce", "Description": "Quick",
     "DeploymentDurationInMinutes": 0, "GrowthType": "LINEAR", "GrowthFactor": 100.0,
     "FinalBakeTimeInMinutes": 10, "ReplicateTo": "NONE"},
    {"Id": "AppConfig.Linear50PercentEvery30Seconds", "Name": "AppConfig.Linear50PercentEvery30Seconds",
     "Description": "Test/Demo", "DeploymentDurationInMinutes": 1, "GrowthType": "LINEAR",
     "GrowthFactor": 50.0, "FinalBakeTimeInMinutes": 1, "ReplicateTo": "NONE"},
    {"Id": "AppConfig.Canary10Percent20Minutes", "Name": "AppConfig.Canary10Percent20Minutes",
     "Description": "AWS Recommended", "DeploymentDurationInMinutes": 20, "GrowthType": "EXPONENTIAL",
     "GrowthFactor": 10.0, "FinalBakeTimeInMinutes": 10, "ReplicateTo": "NONE"},
    {"Id": "AppConfig.Linear20PercentEvery6Minutes", "Name": "AppConfig.Linear20PercentEvery6Minutes",
     "Description": "AWS Recommended", "DeploymentDurationInMinutes": 30, "GrowthType": "LINEAR",
     "GrowthFactor": 20.0, "FinalBakeTimeInMinutes": 30, "ReplicateTo": "NONE"},
]


def _strategy_fields(response):
    return {k: v for k, v in response.items() if k != "ResponseMetadata"}


def test_appconfig_get_predefined_deployment_strategies(appconfig_client):
    for expected in _AWS_PREDEFINED_STRATEGIES:
        resp = appconfig_client.get_deployment_strategy(DeploymentStrategyId=expected["Id"])
        assert _strategy_fields(resp) == expected


def test_appconfig_list_deployment_strategies_puts_predefined_after_own(appconfig_service_state):
    status, created = _status_and_body(
        appconfig_service._create_deployment_strategy({"Name": "own-strategy", "ReplicateTo": "NONE"})
    )
    assert status == 201
    status, body = _status_and_body(appconfig_service._list_deployment_strategies({}))
    assert status == 200
    assert [s["Id"] for s in body["Items"]] == [
        created["Id"],
        "AppConfig.AllAtOnce",
        "AppConfig.Linear50PercentEvery30Seconds",
        "AppConfig.Canary10Percent20Minutes",
        "AppConfig.Linear20PercentEvery6Minutes",
    ]


def test_appconfig_predefined_deployment_strategy_cannot_be_updated(appconfig_client):
    strategy_id = "AppConfig.AllAtOnce"
    with pytest.raises(ClientError) as exc:
        appconfig_client.update_deployment_strategy(DeploymentStrategyId=strategy_id, Description="changed")
    assert exc.value.response["Error"]["Code"] == "BadRequestException"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Message"] == f"Cannot update predefined Deployment Strategy {strategy_id}"
    resp = appconfig_client.get_deployment_strategy(DeploymentStrategyId=strategy_id)
    assert _strategy_fields(resp) == _AWS_PREDEFINED_STRATEGIES[0]


def test_appconfig_predefined_deployment_strategy_cannot_be_deleted(appconfig_client):
    strategy_id = "AppConfig.AllAtOnce"
    with pytest.raises(ClientError) as exc:
        appconfig_client.delete_deployment_strategy(DeploymentStrategyId=strategy_id)
    assert exc.value.response["Error"]["Code"] == "BadRequestException"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Message"] == f"Cannot delete predefined Deployment Strategy {strategy_id}."
    resp = appconfig_client.get_deployment_strategy(DeploymentStrategyId=strategy_id)
    assert _strategy_fields(resp) == _AWS_PREDEFINED_STRATEGIES[0]


def test_appconfig_predefined_deployment_strategies_stay_out_of_saved_state(appconfig_service_state):
    status, body = _status_and_body(appconfig_service._get_deployment_strategy("AppConfig.AllAtOnce"))
    assert status == 200
    assert body["Id"] == "AppConfig.AllAtOnce"
    assert not appconfig_service.get_state()["deployment_strategies"]


# ---------------------------------------------------------------------------
# Deployments
# ---------------------------------------------------------------------------


def test_appconfig_start_deployment(appconfig_client):
    app = appconfig_client.create_application(Name="deploy-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="prod")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="deploy-profile", LocationUri="hosted",
    )
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=b'{"enabled":true}',
        ContentType="application/json",
    )
    strategy = appconfig_client.create_deployment_strategy(
        Name="instant", DeploymentDurationInMinutes=0, GrowthFactor=100.0, ReplicateTo="NONE",
    )
    resp = appconfig_client.start_deployment(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
        DeploymentStrategyId=strategy["Id"],
        ConfigurationProfileId=profile["Id"],
        ConfigurationVersion="1",
    )
    assert resp["DeploymentNumber"] == 1
    assert resp["State"] == "COMPLETE"
    assert resp["PercentageComplete"] == 100.0


def test_appconfig_get_deployment(appconfig_client):
    app = appconfig_client.create_application(Name="deploy-get-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="staging")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="deploy-get-profile", LocationUri="hosted",
    )
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=b"config",
        ContentType="text/plain",
    )
    strategy = appconfig_client.create_deployment_strategy(
        Name="get-strat", DeploymentDurationInMinutes=0, GrowthFactor=100.0, ReplicateTo="NONE",
    )
    deploy = appconfig_client.start_deployment(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
        DeploymentStrategyId=strategy["Id"],
        ConfigurationProfileId=profile["Id"],
        ConfigurationVersion="1",
    )
    resp = appconfig_client.get_deployment(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
        DeploymentNumber=deploy["DeploymentNumber"],
    )
    assert resp["State"] == "COMPLETE"


def test_appconfig_list_deployments(appconfig_client):
    app = appconfig_client.create_application(Name="deploy-list-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="dev")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="deploy-list-profile", LocationUri="hosted",
    )
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=b"c1",
        ContentType="text/plain",
    )
    strategy = appconfig_client.create_deployment_strategy(
        Name="list-strat", DeploymentDurationInMinutes=0, GrowthFactor=100.0, ReplicateTo="NONE",
    )
    appconfig_client.start_deployment(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
        DeploymentStrategyId=strategy["Id"],
        ConfigurationProfileId=profile["Id"],
        ConfigurationVersion="1",
    )
    resp = appconfig_client.list_deployments(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
    )
    assert len(resp["Items"]) >= 1


def test_appconfig_stop_deployment(appconfig_client):
    app = appconfig_client.create_application(Name="deploy-stop-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="qa")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="deploy-stop-profile", LocationUri="hosted",
    )
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=b"data",
        ContentType="text/plain",
    )
    strategy = appconfig_client.create_deployment_strategy(
        Name="stop-strat", DeploymentDurationInMinutes=0, GrowthFactor=100.0, ReplicateTo="NONE",
    )
    deploy = appconfig_client.start_deployment(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
        DeploymentStrategyId=strategy["Id"],
        ConfigurationProfileId=profile["Id"],
        ConfigurationVersion="1",
    )
    assert deploy["State"] == "COMPLETE"
    # StopDeployment "works only on deployments that have a status of
    # DEPLOYING, unless an AllowRevert parameter is supplied" (botocore docs).
    with pytest.raises(ClientError) as exc:
        appconfig_client.stop_deployment(
            ApplicationId=app["Id"],
            EnvironmentId=env["Id"],
            DeploymentNumber=deploy["DeploymentNumber"],
        )
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

    resp = appconfig_client.stop_deployment(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
        DeploymentNumber=deploy["DeploymentNumber"],
        AllowRevert=True,
    )
    assert resp["State"] == "REVERTED"
    assert resp["EventLog"][0]["EventType"] == "REVERT_COMPLETED"
    environment = appconfig_client.get_environment(ApplicationId=app["Id"], EnvironmentId=env["Id"])
    assert environment["State"] == "REVERTED"


def _deployment_target(appconfig_client, name):
    app = appconfig_client.create_application(Name=name)
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="env")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="profile", LocationUri="hosted",
    )
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"], ConfigurationProfileId=profile["Id"],
        Content=b"{}", ContentType="application/json",
    )
    return {"ApplicationId": app["Id"], "EnvironmentId": env["Id"]}, profile["Id"]


def _rollout_params(deployment):
    return (deployment["DeploymentDurationInMinutes"], deployment["GrowthType"],
            deployment["GrowthFactor"], deployment["FinalBakeTimeInMinutes"])


def test_appconfig_start_deployment_records_predefined_strategy_parameters(appconfig_client):
    ids, profile_id = _deployment_target(appconfig_client, "deploy-predefined-app")
    deploy = appconfig_client.start_deployment(
        **ids, DeploymentStrategyId="AppConfig.Canary10Percent20Minutes",
        ConfigurationProfileId=profile_id, ConfigurationVersion="1",
    )
    assert deploy["DeploymentStrategyId"] == "AppConfig.Canary10Percent20Minutes"
    got = appconfig_client.get_deployment(**ids, DeploymentNumber=deploy["DeploymentNumber"])
    listed = appconfig_client.list_deployments(**ids)["Items"][0]
    for record in (deploy, got, listed):
        assert _rollout_params(record) == (20, "EXPONENTIAL", 10.0, 10)


def test_appconfig_start_deployment_records_own_strategy_parameters(appconfig_client):
    ids, profile_id = _deployment_target(appconfig_client, "deploy-own-strategy-app")
    strategy = appconfig_client.create_deployment_strategy(
        Name="own-params", DeploymentDurationInMinutes=5, GrowthType="LINEAR",
        GrowthFactor=25.0, FinalBakeTimeInMinutes=3, ReplicateTo="NONE",
    )
    deploy = appconfig_client.start_deployment(
        **ids, DeploymentStrategyId=strategy["Id"],
        ConfigurationProfileId=profile_id, ConfigurationVersion="1",
    )
    assert _rollout_params(deploy) == (5, "LINEAR", 25.0, 3)


def test_appconfig_start_deployment_rejects_missing_strategy(appconfig_client):
    ids, profile_id = _deployment_target(appconfig_client, "deploy-missing-strategy-app")
    for strategy_id in ("abcdefg", "AppConfig.DoesNotExist"):
        with pytest.raises(ClientError) as exc:
            appconfig_client.start_deployment(
                **ids, DeploymentStrategyId=strategy_id,
                ConfigurationProfileId=profile_id, ConfigurationVersion="1",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert exc.value.response["Error"]["Message"] == (
            f"DeploymentStrategy with Id {strategy_id} could not be found.")
    assert appconfig_client.list_deployments(**ids)["Items"] == []


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def test_appconfig_tag_resource(appconfig_client):
    app = appconfig_client.create_application(Name="tag-app", Tags={"env": "test"})
    app_arn = f"arn:aws:appconfig:us-east-1:000000000000:application/{app['Id']}"
    resp = appconfig_client.list_tags_for_resource(ResourceArn=app_arn)
    assert resp["Tags"]["env"] == "test"

    appconfig_client.tag_resource(ResourceArn=app_arn, Tags={"team": "platform"})
    resp = appconfig_client.list_tags_for_resource(ResourceArn=app_arn)
    assert resp["Tags"]["team"] == "platform"
    assert resp["Tags"]["env"] == "test"

    appconfig_client.untag_resource(ResourceArn=app_arn, TagKeys=["env"])
    resp = appconfig_client.list_tags_for_resource(ResourceArn=app_arn)
    assert "env" not in resp["Tags"]
    assert resp["Tags"]["team"] == "platform"


def test_appconfig_tag_resource_accepts_supported_local_arn_shapes(appconfig_service_state):
    status, app = _status_and_body(
        appconfig_service._create_application({"Name": "arn-tag-app", "Tags": {"seed": "application"}})
    )
    assert status == 201
    status, env = _status_and_body(
        appconfig_service._create_environment(app["Id"], {"Name": "live", "Tags": {"seed": "environment"}})
    )
    assert status == 201
    status, profile = _status_and_body(
        appconfig_service._create_configuration_profile(
            app["Id"],
            {
                "Name": "profile",
                "LocationUri": "hosted",
                "RetrievalRoleArn": "not-an-arn",
                "Tags": {"seed": "configurationprofile"},
            },
        )
    )
    assert status == 201
    assert profile["RetrievalRoleArn"] == "not-an-arn"
    status, strategy = _status_and_body(
        appconfig_service._create_deployment_strategy(
            {"Name": "strategy", "ReplicateTo": "NONE", "Tags": {"seed": "deploymentstrategy"}}
        )
    )
    assert status == 201

    cases = {
        appconfig_service._app_arn(app["Id"]): "application",
        appconfig_service._env_arn(app["Id"], env["Id"]): "environment",
        appconfig_service._profile_arn(app["Id"], profile["Id"]): "configurationprofile",
        appconfig_service._strategy_arn(strategy["Id"]): "deploymentstrategy",
    }

    for resource_arn, seed in cases.items():
        status, body = _status_and_body(appconfig_service._list_tags_for_resource(resource_arn))
        assert status == 200
        assert body["Tags"] == {"seed": seed}

        status, _ = _status_and_body(appconfig_service._tag_resource(resource_arn, {"Tags": {"owner": seed}}))
        assert status == 204
        status, body = _status_and_body(appconfig_service._list_tags_for_resource(resource_arn))
        assert body["Tags"]["owner"] == seed

        status, _ = _status_and_body(appconfig_service._untag_resource(resource_arn, ["owner"]))
        assert status == 204
        status, body = _status_and_body(appconfig_service._list_tags_for_resource(resource_arn))
        assert "owner" not in body["Tags"]


def test_appconfig_tag_resource_rejects_invalid_arns_before_mutating_tags(appconfig_service_state):
    status, app = _status_and_body(
        appconfig_service._create_application({"Name": "invalid-arn-app", "Tags": {"seed": "application"}})
    )
    assert status == 201
    valid_arn = appconfig_service._app_arn(app["Id"])
    before = _tag_snapshot()

    invalid_arns = [
        "not-an-arn",
        valid_arn.replace("arn:aws:", "arn:aws-cn:", 1),
        valid_arn.replace(":appconfig:", ":ssm:", 1),
        valid_arn.replace(":000000000000:", ":111111111111:", 1),
        valid_arn.replace(":us-east-1:", ":us-west-2:", 1),
    ]

    for resource_arn in invalid_arns:
        for call, response in (
            ("tag", appconfig_service._tag_resource(resource_arn, {"Tags": {"bad": "tag"}})),
            ("untag", appconfig_service._untag_resource(resource_arn, ["seed"])),
            ("list", appconfig_service._list_tags_for_resource(resource_arn)),
        ):
            status, body = _status_and_body(response)
            # #2484: the tag APIs answer another account's ARN with 403, as AWS does.
            if ":111111111111:" in resource_arn:
                assert (status, body["Code"]) == (403, "AccessDeniedException")
            else:
                assert status == 400
                assert body["Code"] == "BadRequestException"
            assert _tag_snapshot() == before


def test_appconfig_tag_resource_rejects_missing_local_resources_before_touching_tags(appconfig_service_state):
    status, app = _status_and_body(
        appconfig_service._create_application({"Name": "missing-resource-app", "Tags": {"seed": "application"}})
    )
    assert status == 201

    missing_arns = [
        "arn:aws:appconfig:us-east-1:000000000000:application/missing-app",
        f"arn:aws:appconfig:us-east-1:000000000000:application/{app['Id']}/environment/missing-env",
        f"arn:aws:appconfig:us-east-1:000000000000:application/{app['Id']}/configurationprofile/missing-profile",
        "arn:aws:appconfig:us-east-1:000000000000:deploymentstrategy/missing-strategy",
        f"arn:aws:appconfig:us-east-1:000000000000:application/{app['Id']}/environment/missing-env/deployment/1",
    ]
    for resource_arn in missing_arns:
        appconfig_service._tags[resource_arn] = {"legacy": "keep"}
    before = _tag_snapshot()

    for resource_arn in missing_arns:
        for response in (
            appconfig_service._tag_resource(resource_arn, {"Tags": {"bad": "tag"}}),
            appconfig_service._untag_resource(resource_arn, ["legacy"]),
            appconfig_service._list_tags_for_resource(resource_arn),
        ):
            status, body = _status_and_body(response)
            assert status == 404
            assert body["Code"] == "ResourceNotFoundException"
            assert _tag_snapshot() == before


# ---------------------------------------------------------------------------
# Data Plane — full end-to-end workflow
# ---------------------------------------------------------------------------


def test_appconfig_data_plane_e2e(appconfig_client, appconfigdata_client):
    app = appconfig_client.create_application(Name="data-plane-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="live")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="data-profile", LocationUri="hosted",
    )
    config_content = json.dumps({"feature_x": True, "max_retries": 3}).encode("utf-8")
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=config_content,
        ContentType="application/json",
    )
    strategy = appconfig_client.create_deployment_strategy(
        Name="e2e-strategy",
        DeploymentDurationInMinutes=0,
        GrowthFactor=100.0,
        ReplicateTo="NONE",
    )
    appconfig_client.start_deployment(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
        DeploymentStrategyId=strategy["Id"],
        ConfigurationProfileId=profile["Id"],
        ConfigurationVersion="1",
    )

    session = appconfigdata_client.start_configuration_session(
        ApplicationIdentifier=app["Id"],
        EnvironmentIdentifier=env["Id"],
        ConfigurationProfileIdentifier=profile["Id"],
    )
    token = session["InitialConfigurationToken"]
    assert token

    latest = appconfigdata_client.get_latest_configuration(ConfigurationToken=token)
    body = latest["Configuration"].read()
    assert json.loads(body) == {"feature_x": True, "max_retries": 3}
    assert latest["ContentType"] == "application/json"
    assert latest["NextPollConfigurationToken"]

    # Second call with new token should also work
    latest2 = appconfigdata_client.get_latest_configuration(
        ConfigurationToken=latest["NextPollConfigurationToken"],
    )
    assert latest2["NextPollConfigurationToken"]


def test_appconfig_data_plane_e2e_with_names(appconfig_client, appconfigdata_client):
    app = appconfig_client.create_application(Name="data-plane-app-by-name")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="live")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="data-profile", LocationUri="hosted",
    )
    config_content = json.dumps({"feature_x": True, "max_retries": 3}).encode("utf-8")
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"],
        ConfigurationProfileId=profile["Id"],
        Content=config_content,
        ContentType="application/json",
    )
    strategy = appconfig_client.create_deployment_strategy(
        Name="e2e-strategy-by-name",
        DeploymentDurationInMinutes=0,
        GrowthFactor=100.0,
        ReplicateTo="NONE",
    )
    appconfig_client.start_deployment(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
        DeploymentStrategyId=strategy["Id"],
        ConfigurationProfileId=profile["Id"],
        ConfigurationVersion="1",
    )

    session = appconfigdata_client.start_configuration_session(
        ApplicationIdentifier=app["Name"],
        EnvironmentIdentifier=env["Name"],
        ConfigurationProfileIdentifier=profile["Name"],
    )
    token = session["InitialConfigurationToken"]
    assert token

    latest = appconfigdata_client.get_latest_configuration(ConfigurationToken=token)
    body = latest["Configuration"].read()
    assert json.loads(body) == {"feature_x": True, "max_retries": 3}
    assert latest["ContentType"] == "application/json"
    assert latest["NextPollConfigurationToken"]


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_appconfig_get_nonexistent_application(appconfig_client):
    with pytest.raises(ClientError) as exc:
        appconfig_client.get_application(ApplicationId="nonexistent")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    # Real AWS sends `x-amzn-errortype` on REST-JSON errors; Java/Go SDK v2 read it.
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "ResourceNotFoundException"
    # Body must also include `__type` (was previously only `Code`/`Message`,
    # which generic JSON-error parsers miss). Verify via raw HTTP since boto3
    # surfaces only Error.Code/Message.
    import json
    import urllib.error
    import urllib.request
    req = urllib.request.Request(
        f"{ENDPOINT}/applications/nonexistent",
        headers={"Authorization": "AWS4-HMAC-SHA256 Credential=test/20260501/us-east-1/appconfig/aws4_request"},
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        assert False, "expected 404"
    except urllib.error.HTTPError as e:
        body = json.loads(e.read())
        assert body.get("__type") == "ResourceNotFoundException"


def test_appconfig_get_nonexistent_environment(appconfig_client):
    app = appconfig_client.create_application(Name="err-env-app")
    with pytest.raises(ClientError) as exc:
        appconfig_client.get_environment(ApplicationId=app["Id"], EnvironmentId="nonexistent")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_appconfig_get_nonexistent_deployment(appconfig_client):
    app = appconfig_client.create_application(Name="err-deploy-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="err-env")
    with pytest.raises(ClientError) as exc:
        appconfig_client.get_deployment(
            ApplicationId=app["Id"], EnvironmentId=env["Id"], DeploymentNumber=999,
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_appconfig_start_configuration_session_rejects_missing_application(appconfigdata_client):
    with pytest.raises(ClientError) as exc:
        appconfigdata_client.start_configuration_session(
            ApplicationIdentifier="missing-app",
            EnvironmentIdentifier="live",
            ConfigurationProfileIdentifier="data-profile",
        )

    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_appconfig_start_configuration_session_rejects_missing_environment(appconfig_client, appconfigdata_client):
    app = appconfig_client.create_application(Name="session-env-app")

    with pytest.raises(ClientError) as exc:
        appconfigdata_client.start_configuration_session(
            ApplicationIdentifier=app["Name"],
            EnvironmentIdentifier="missing-env",
            ConfigurationProfileIdentifier="data-profile",
        )

    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_appconfig_start_configuration_session_rejects_missing_configuration_profile(
        appconfig_client,
        appconfigdata_client,
):
    app = appconfig_client.create_application(Name="session-profile-app")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="live")

    with pytest.raises(ClientError) as exc:
        appconfigdata_client.start_configuration_session(
            ApplicationIdentifier=app["Name"],
            EnvironmentIdentifier=env["Name"],
            ConfigurationProfileIdentifier="missing-profile",
        )

    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


# ---------------------------------------------------------------------------
# Region isolation (multi-region)
# ---------------------------------------------------------------------------


def test_appconfig_applications_are_region_scoped():
    """AppConfig state is region-scoped: an application created in one region is
    not visible from another, and a cross-region read is a clean 404. Guards the
    AccountRegionScopedDict conversion against regressing to account-only."""
    east = _make_appconfig_client("us-east-1")
    west = _make_appconfig_client("us-west-2")

    east_id = east.create_application(Name="region-iso-app")["Id"]
    west_id = west.create_application(Name="region-iso-app")["Id"]
    try:
        assert east_id != west_id

        east_ids = {a["Id"] for a in east.list_applications()["Items"]}
        west_ids = {a["Id"] for a in west.list_applications()["Items"]}
        assert east_id in east_ids and west_id not in east_ids
        assert west_id in west_ids and east_id not in west_ids

        # Cross-region read must not resolve.
        with pytest.raises(ClientError) as exc:
            east.get_application(ApplicationId=west_id)
        assert exc.value.response["Error"]["Code"] in ("ResourceNotFoundException", "404")
    finally:
        for client, app_id in ((east, east_id), (west, west_id)):
            try:
                client.delete_application(ApplicationId=app_id)
            except Exception:
                pass


def test_appconfig_predefined_deployment_strategy_tags_are_region_scoped():
    """Tags on a predefined strategy belong to the account and region that set them."""
    east = _make_appconfig_client("us-east-1")
    west = _make_appconfig_client("us-west-2")
    strategy_id = "AppConfig.Linear20PercentEvery6Minutes"
    east_arn = f"arn:aws:appconfig:us-east-1:000000000000:deploymentstrategy/{strategy_id}"
    west_arn = f"arn:aws:appconfig:us-west-2:000000000000:deploymentstrategy/{strategy_id}"

    east.tag_resource(ResourceArn=east_arn, Tags={"probe": "east"})
    try:
        assert east.list_tags_for_resource(ResourceArn=east_arn)["Tags"] == {"probe": "east"}
        assert west.list_tags_for_resource(ResourceArn=west_arn)["Tags"] == {}
    finally:
        east.untag_resource(ResourceArn=east_arn, TagKeys=["probe"])
    assert east.list_tags_for_resource(ResourceArn=east_arn)["Tags"] == {}


def _deploy_and_fetch(appconfig_client, appconfigdata_client, profile_type, content,
                      suffix):
    """Create app/env/profile, deploy `content`, and return the served bytes."""
    app = appconfig_client.create_application(Name=f"ff-app-{suffix}")
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="env")
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name=f"p-{suffix}", LocationUri="hosted",
        Type=profile_type,
    )
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"], ConfigurationProfileId=profile["Id"],
        Content=json.dumps(content).encode("utf-8"), ContentType="application/json",
    )
    strategy = appconfig_client.create_deployment_strategy(
        Name=f"s-{suffix}", DeploymentDurationInMinutes=0, GrowthFactor=100.0,
        ReplicateTo="NONE",
    )
    appconfig_client.start_deployment(
        ApplicationId=app["Id"], EnvironmentId=env["Id"],
        DeploymentStrategyId=strategy["Id"],
        ConfigurationProfileId=profile["Id"], ConfigurationVersion="1",
    )
    session = appconfigdata_client.start_configuration_session(
        ApplicationIdentifier=app["Id"], EnvironmentIdentifier=env["Id"],
        ConfigurationProfileIdentifier=profile["Id"],
    )
    latest = appconfigdata_client.get_latest_configuration(
        ConfigurationToken=session["InitialConfigurationToken"])
    return json.loads(latest["Configuration"].read())


_FEATURE_FLAGS_DEPLOYMENT_TIME = {
    "flags": {
        "my_flag": {"name": "my_flag", "attributes": {"level": {"constraints": {"type": "string"}}}},
        "off_flag": {"name": "off_flag"},
    },
    "values": {
        "my_flag": {"enabled": True, "level": "INFO"},
        "off_flag": {"enabled": False},
    },
    "version": "1",
}


def test_appconfig_feature_flags_are_served_in_retrieval_time_format(
        appconfig_client, appconfigdata_client):
    """A feature-flag profile is stored in deployment-time format and served in
    retrieval-time format, "which only contains the flag's value" -- the values
    map lifted to the top level, with no flags/version wrapper. A disabled flag
    is still served, carrying enabled false."""
    served = _deploy_and_fetch(
        appconfig_client, appconfigdata_client, "AWS.AppConfig.FeatureFlags",
        _FEATURE_FLAGS_DEPLOYMENT_TIME, "ff",
    )
    assert served == {
        "my_flag": {"enabled": True, "level": "INFO"},
        "off_flag": {"enabled": False},
    }
    assert "flags" not in served
    assert "version" not in served


def test_appconfig_freeform_content_is_served_verbatim(
        appconfig_client, appconfigdata_client):
    """Only a feature-flag profile is transformed; a freeform profile that
    happens to carry a `values` key keeps it."""
    document = {"values": {"not": "a flag"}, "greeting": "hello"}
    assert _deploy_and_fetch(
        appconfig_client, appconfigdata_client, "AWS.Freeform", document, "free",
    ) == document


def test_appconfig_feature_flags_without_values_are_served_verbatim(
        appconfig_client, appconfigdata_client):
    """Content that does not carry the deployment-time shape is passed through
    rather than emptied."""
    document = {"my_flag": {"enabled": True}}
    assert _deploy_and_fetch(
        appconfig_client, appconfigdata_client, "AWS.AppConfig.FeatureFlags",
        document, "noval",
    ) == document


# ---------------------------------------------------------------------------
# Gradual rollout
# ---------------------------------------------------------------------------

MINUTE = 60.0
START = 1_800_000_000.0


@pytest.fixture
def clock(monkeypatch):
    now = [START]
    monkeypatch.setattr(time, "time", lambda: now[0])
    appconfig_service.reset()
    cloudwatch_service.reset()
    yield now
    appconfig_service.reset()
    cloudwatch_service.reset()


def _appconfig(method, path, body=None, headers=None, query=None):
    raw = json.dumps(body).encode() if isinstance(body, dict) else (body or b"")
    status, resp_headers, resp_body = asyncio.run(appconfig_service.handle_request(
        method, path, {k.lower(): v for k, v in (headers or {}).items()}, raw, query or {}))
    return status, resp_headers, resp_body


def _appconfig_json(method, path, body=None, headers=None):
    status, _, resp_body = _appconfig(method, path, body, headers)
    return status, json.loads(resp_body) if resp_body else {}


def _cloudwatch(action, body):
    headers = {"x-amz-target": f"GraniteServiceVersion20100801.{action}",
               "content-type": "application/x-amz-json-1.0"}
    status, _, _ = asyncio.run(cloudwatch_service.handle_request(
        "POST", "/", headers, json.dumps(body).encode(), {}))
    assert status == 200


def _alarm(name):
    _cloudwatch("PutMetricAlarm", {
        "AlarmName": name, "Namespace": "Orders", "MetricName": "Errors",
        "Statistic": "Sum", "Period": 60, "EvaluationPeriods": 1,
        "Threshold": 1.0, "ComparisonOperator": "GreaterThanOrEqualToThreshold",
    })
    return f"arn:aws:cloudwatch:us-east-1:{get_account_id()}:alarm:{name}"


def _set_alarm(name, state):
    _cloudwatch("SetAlarmState", {"AlarmName": name, "StateValue": state, "StateReason": "test"})


class Target:
    """An application, environment and hosted profile holding versions 1 and 2."""

    def __init__(self, monitors=None):
        _, app = _appconfig_json("POST", "/applications", {"Name": "orders"})
        _, env = _appconfig_json("POST", f"/applications/{app['Id']}/environments",
                                 {"Name": "prod", "Monitors": monitors or []})
        _, profile = _appconfig_json("POST", f"/applications/{app['Id']}/configurationprofiles",
                                     {"Name": "settings", "LocationUri": "hosted"})
        self.app, self.env, self.profile = app["Id"], env["Id"], profile["Id"]
        for version in (1, 2):
            status, _, _ = _appconfig(
                "POST",
                f"/applications/{self.app}/configurationprofiles/{self.profile}/hostedconfigurationversions",
                json.dumps({"version": version}).encode(),
                {"Content-Type": "application/json", "VersionLabel": f"v{version}"},
            )
            assert status == 201

    @property
    def deployments_path(self):
        return f"/applications/{self.app}/environments/{self.env}/deployments"

    def deploy(self, version, strategy="AppConfig.Linear20PercentEvery6Minutes", **extra):
        return _appconfig_json("POST", self.deployments_path, {
            "DeploymentStrategyId": strategy, "ConfigurationProfileId": self.profile,
            "ConfigurationVersion": str(version), **extra,
        })

    def deployment(self, number):
        status, body = _appconfig_json("GET", f"{self.deployments_path}/{number}")
        assert status == 200
        return body

    def stop(self, number, allow_revert=False):
        headers = {"Allow-Revert": "true"} if allow_revert else {}
        return _appconfig_json("DELETE", f"{self.deployments_path}/{number}", headers=headers)

    def environment_state(self):
        return _appconfig_json("GET", f"/applications/{self.app}/environments/{self.env}")[1]["State"]

    def session(self, **extra):
        status, body = _appconfig_json("POST", "/configurationsessions", {
            "ApplicationIdentifier": self.app, "EnvironmentIdentifier": self.env,
            "ConfigurationProfileIdentifier": self.profile, **extra,
        })
        assert status == 201
        return Client(body["InitialConfigurationToken"])


class Client:
    """One configuration session, polling with the token it was last handed."""

    def __init__(self, token):
        self.session_id = token
        self.token = token

    def poll(self):
        status, headers, body = _appconfig("GET", "/configuration", query={"configuration_token": self.token})
        assert status == 200
        self.token = headers["Next-Poll-Configuration-Token"]
        return headers, json.loads(body) if body else None


def _event_types(deployment):
    return [event["EventType"] for event in deployment["EventLog"]]


def test_rollout_steps_follow_the_growth_type():
    assert appconfig_service._rollout_steps("LINEAR", 20.0) == [20.0, 40.0, 60.0, 80.0, 100.0]
    assert appconfig_service._rollout_steps("LINEAR", 100.0) == [100.0]
    # G*(2^N) from the CreateDeploymentStrategy docs.
    assert appconfig_service._rollout_steps("EXPONENTIAL", 10.0) == [10.0, 20.0, 40.0, 80.0, 100.0]
    assert appconfig_service._rollout_steps("EXPONENTIAL", 2.0) == [2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 100.0]
    # Below the model's 1.0 minimum (stored unvalidated) still terminates.
    assert appconfig_service._rollout_steps("LINEAR", -5.0)[-1] == 100.0


def test_linear_deployment_steps_bakes_and_completes(clock):
    target = Target()
    status, deployment = target.deploy(1)
    assert status == 201
    assert deployment["State"] == "DEPLOYING"
    assert deployment["PercentageComplete"] == 0.0
    assert _event_types(deployment) == ["DEPLOYMENT_STARTED"]
    assert target.environment_state() == "DEPLOYING"

    # Linear20PercentEvery6Minutes: five 20% steps over 30 minutes, then a 30-minute bake.
    clock[0] = START + 6 * MINUTE
    assert target.deployment(1)["PercentageComplete"] == 20.0
    clock[0] = START + 12 * MINUTE - 1
    assert target.deployment(1)["PercentageComplete"] == 20.0
    clock[0] = START + 12 * MINUTE
    assert target.deployment(1)["PercentageComplete"] == 40.0

    clock[0] = START + 30 * MINUTE
    deployment = target.deployment(1)
    assert (deployment["State"], deployment["PercentageComplete"]) == ("BAKING", 100.0)
    assert target.environment_state() == "DEPLOYING"

    clock[0] = START + 60 * MINUTE
    deployment = target.deployment(1)
    assert deployment["State"] == "COMPLETE"
    assert deployment["CompletedAt"] == appconfig_service._iso(START + 60 * MINUTE)
    assert target.environment_state() == "READY_FOR_DEPLOYMENT"
    # Most recent first.
    assert _event_types(deployment) == [
        "DEPLOYMENT_COMPLETED", "BAKE_TIME_STARTED",
        *["PERCENTAGE_UPDATED"] * 5,
        "DEPLOYMENT_STARTED",
    ]
    first_step = deployment["EventLog"][-2]
    assert first_step["OccurredAt"] == appconfig_service._iso(START + 6 * MINUTE)
    assert first_step["TriggeredBy"] == "APPCONFIG"


def test_list_deployments_reports_rollout_progress(clock):
    target = Target()
    target.deploy(1, strategy="AppConfig.Canary10Percent20Minutes")
    clock[0] = START + 4 * MINUTE
    status, body = _appconfig_json("GET", target.deployments_path)
    assert status == 200
    [summary] = body["Items"]
    assert (summary["State"], summary["PercentageComplete"]) == ("DEPLOYING", 10.0)
    assert summary["VersionLabel"] == "v1"
    assert "CompletedAt" not in summary


def test_start_deployment_conflicts_with_one_in_progress(clock):
    target = Target()
    target.deploy(1)
    status, body = target.deploy(2)
    assert status == 409
    assert body["__type"] == "ConflictException"

    # Still in progress while baking.
    clock[0] = START + 30 * MINUTE
    assert target.deploy(2)[0] == 409

    clock[0] = START + 60 * MINUTE
    status, body = target.deploy(2)
    assert status == 201
    assert body["DeploymentNumber"] == 2


def test_start_deployment_checks_latest_deployment_number(clock):
    target = Target()
    assert target.deploy(1, strategy="AppConfig.AllAtOnce", LatestDeploymentNumber=0)[0] == 201
    clock[0] = START + 10 * MINUTE
    status, body = target.deploy(2, strategy="AppConfig.AllAtOnce", LatestDeploymentNumber=0)
    assert status == 409
    assert body["__type"] == "ConflictException"
    assert target.deploy(2, strategy="AppConfig.AllAtOnce", LatestDeploymentNumber=1)[0] == 201


def test_stop_rolls_back_an_in_progress_deployment(clock):
    target = Target()
    target.deploy(1, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 10 * MINUTE
    client = target.session()
    assert client.poll()[1] == {"version": 1}

    target.deploy(2)
    clock[0] = START + 40 * MINUTE  # 100%, baking
    assert client.poll()[1] == {"version": 2}

    # Baking stops only with AllowRevert ("works only on deployments that have a status of DEPLOYING").
    assert target.stop(2)[0] == 400
    status, deployment = target.stop(2, allow_revert=True)
    assert status == 202
    assert deployment["State"] == "ROLLED_BACK"
    assert _event_types(deployment)[:2] == ["ROLLBACK_COMPLETED", "ROLLBACK_STARTED"]
    assert deployment["EventLog"][0]["TriggeredBy"] == "USER"
    assert target.environment_state() == "ROLLED_BACK"
    assert client.poll()[1] == {"version": 1}

    # A stopped deployment cannot be stopped again, and the environment takes a new one.
    assert target.stop(2)[0] == 400
    assert target.deploy(2)[0] == 201


def test_revert_requires_allow_revert_and_the_72_hour_window(clock):
    target = Target()
    target.deploy(1, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 10 * MINUTE
    target.deploy(2, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 20 * MINUTE
    assert target.deployment(2)["State"] == "COMPLETE"

    status, body = target.stop(2)
    assert status == 400
    assert body["__type"] == "BadRequestException"

    clock[0] = START + 20 * MINUTE + 72 * 3600 + 1
    assert target.stop(2, allow_revert=True)[0] == 400

    clock[0] = START + 20 * MINUTE + 72 * 3600
    status, deployment = target.stop(2, allow_revert=True)
    assert status == 202
    assert deployment["State"] == "REVERTED"
    assert target.environment_state() == "REVERTED"
    assert target.session().poll()[1] == {"version": 1}


def test_monitor_alarm_rolls_back_at_the_moment_it_fires(clock):
    alarm_arn = _alarm("orders-errors")
    target = Target(monitors=[{"AlarmArn": alarm_arn}])
    target.deploy(1, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 10 * MINUTE
    target.deploy(2)
    deploy_start = clock[0]

    clock[0] = deploy_start + 7 * MINUTE
    _cloudwatch("PutMetricData", {"Namespace": "Orders", "MetricData": [{"MetricName": "Errors", "Value": 5}]})
    clock[0] = deploy_start + 20 * MINUTE

    deployment = target.deployment(2)
    assert deployment["State"] == "ROLLED_BACK"
    assert deployment["PercentageComplete"] == 20.0  # the 12-minute step never came
    rollback_started = deployment["EventLog"][1]
    assert rollback_started["EventType"] == "ROLLBACK_STARTED"
    assert rollback_started["TriggeredBy"] == "CLOUDWATCH_ALARM"
    assert alarm_arn in rollback_started["Description"]
    assert rollback_started["OccurredAt"] == appconfig_service._iso(deploy_start + 7 * MINUTE)
    assert target.environment_state() == "ROLLED_BACK"
    assert target.session().poll()[1] == {"version": 1}


def test_alarm_that_recovers_during_the_bake_still_rolls_back(clock):
    target = Target(monitors=[{"AlarmArn": _alarm("orders-errors")}])
    target.deploy(1)
    clock[0] = START + 35 * MINUTE
    _set_alarm("orders-errors", "ALARM")
    clock[0] = START + 36 * MINUTE
    _set_alarm("orders-errors", "OK")

    clock[0] = START + 90 * MINUTE
    deployment = target.deployment(1)
    assert deployment["State"] == "ROLLED_BACK"
    assert deployment["PercentageComplete"] == 100.0
    assert "BAKE_TIME_STARTED" in _event_types(deployment)


def test_alarm_after_completion_does_not_roll_back(clock):
    target = Target(monitors=[{"AlarmArn": _alarm("orders-errors")}])
    target.deploy(1)
    clock[0] = START + 61 * MINUTE
    _set_alarm("orders-errors", "ALARM")
    clock[0] = START + 62 * MINUTE
    assert target.deployment(1)["State"] == "COMPLETE"


def test_alarm_already_firing_rolls_back_before_any_step(clock):
    target = Target(monitors=[{"AlarmArn": _alarm("orders-errors")}])
    _set_alarm("orders-errors", "ALARM")
    status, deployment = target.deploy(1, strategy="AppConfig.AllAtOnce")
    assert status == 201
    assert deployment["State"] == "ROLLED_BACK"
    assert deployment["PercentageComplete"] == 0.0


def test_monitor_on_an_unknown_alarm_is_ignored(clock):
    target = Target(monitors=[{"AlarmArn": "arn:aws:cloudwatch:us-east-1:000000000000:alarm:missing"}])
    target.deploy(1, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 10 * MINUTE
    assert target.deployment(1)["State"] == "COMPLETE"


def test_gradual_rollout_reaches_each_client_once_its_slot_is_covered(clock):
    target = Target()
    target.deploy(1, strategy="AppConfig.AllAtOnce")
    clock[0] = START + 10 * MINUTE
    clients = [target.session() for _ in range(60)]
    for client in clients:
        assert client.poll()[1] == {"version": 1}

    target.deploy(2)
    deploy_start = clock[0]
    clock[0] = deploy_start + 12 * MINUTE  # 40%
    reached = set()
    for client in clients:
        served = client.poll()[1]
        if appconfig_service._rollout_bucket(client.session_id) < 40:
            assert served == {"version": 2}
            reached.add(client.session_id)
        else:
            assert served is None  # still on version 1, which it already has
    assert 0 < len(reached) < len(clients)

    clock[0] = deploy_start + 30 * MINUTE  # 100%
    for client in clients:
        served = client.poll()[1]
        assert served == (None if client.session_id in reached else {"version": 2})


def test_poll_returns_empty_configuration_until_it_changes(clock):
    target = Target()
    client = target.session()
    headers, served = client.poll()
    assert served is None  # nothing deployed yet

    target.deploy(1, strategy="AppConfig.AllAtOnce")
    headers, served = client.poll()
    assert served == {"version": 1}
    assert headers["Version-Label"] == "v1"
    headers, served = client.poll()
    assert served is None
    assert headers["Version-Label"] == "v1"


def test_session_poll_interval(clock):
    target = Target()
    assert target.session().poll()[0]["Next-Poll-Interval-In-Seconds"] == "30"
    assert target.session(RequiredMinimumPollIntervalInSeconds=90).poll()[0]["Next-Poll-Interval-In-Seconds"] == "90"
    status, body = _appconfig_json("POST", "/configurationsessions", {
        "ApplicationIdentifier": target.app, "EnvironmentIdentifier": target.env,
        "ConfigurationProfileIdentifier": target.profile, "RequiredMinimumPollIntervalInSeconds": 5,
    })
    assert status == 400
    assert body["__type"] == "BadRequestException"


# ---------------------------------------------------------------------------
# boto3 against the shared server
# ---------------------------------------------------------------------------


def _live_target(appconfig_client, name, monitors=None):
    app = appconfig_client.create_application(Name=name)
    env = appconfig_client.create_environment(ApplicationId=app["Id"], Name="prod", Monitors=monitors or [])
    profile = appconfig_client.create_configuration_profile(
        ApplicationId=app["Id"], Name="settings", LocationUri="hosted")
    appconfig_client.create_hosted_configuration_version(
        ApplicationId=app["Id"], ConfigurationProfileId=profile["Id"],
        Content=b'{"version": 1}', ContentType="application/json", VersionLabel="v1")
    return app["Id"], env["Id"], profile["Id"]


def test_live_alarm_in_alarm_rolls_back_deployment(appconfig_client, cw):
    alarm_name = "appconfig-rollout-live-errors"
    cw.put_metric_alarm(
        AlarmName=alarm_name, Namespace="Orders", MetricName="Errors", Statistic="Sum",
        Period=60, EvaluationPeriods=1, Threshold=1.0, ComparisonOperator="GreaterThanOrEqualToThreshold")
    alarm_arn = cw.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"][0]["AlarmArn"]
    cw.set_alarm_state(AlarmName=alarm_name, StateValue="ALARM", StateReason="test")
    try:
        app_id, env_id, profile_id = _live_target(
            appconfig_client, "rollout-live-alarm", monitors=[{"AlarmArn": alarm_arn}])
        deployment = appconfig_client.start_deployment(
            ApplicationId=app_id, EnvironmentId=env_id, DeploymentStrategyId="AppConfig.AllAtOnce",
            ConfigurationProfileId=profile_id, ConfigurationVersion="1")
        assert deployment["State"] == "ROLLED_BACK"
        assert deployment["EventLog"][0]["TriggeredBy"] == "CLOUDWATCH_ALARM"
        environment = appconfig_client.get_environment(ApplicationId=app_id, EnvironmentId=env_id)
        assert environment["State"] == "ROLLED_BACK"
    finally:
        cw.delete_alarms(AlarmNames=[alarm_name])


def test_live_data_plane_version_label_and_unchanged_poll(appconfig_client, appconfigdata_client):
    app_id, env_id, profile_id = _live_target(appconfig_client, "rollout-live-poll")
    appconfig_client.start_deployment(
        ApplicationId=app_id, EnvironmentId=env_id, DeploymentStrategyId="AppConfig.AllAtOnce",
        ConfigurationProfileId=profile_id, ConfigurationVersion="1")
    token = appconfigdata_client.start_configuration_session(
        ApplicationIdentifier=app_id, EnvironmentIdentifier=env_id,
        ConfigurationProfileIdentifier=profile_id, RequiredMinimumPollIntervalInSeconds=45,
    )["InitialConfigurationToken"]

    first = appconfigdata_client.get_latest_configuration(ConfigurationToken=token)
    assert json.loads(first["Configuration"].read()) == {"version": 1}
    assert first["VersionLabel"] == "v1"
    assert first["NextPollIntervalInSeconds"] == 45

    second = appconfigdata_client.get_latest_configuration(
        ConfigurationToken=first["NextPollConfigurationToken"])
    assert second["Configuration"].read() == b""


def test_live_concurrent_deployment_conflicts(appconfig_client):
    app_id, env_id, profile_id = _live_target(appconfig_client, "rollout-live-conflict")
    appconfig_client.start_deployment(
        ApplicationId=app_id, EnvironmentId=env_id, DeploymentStrategyId="AppConfig.AllAtOnce",
        ConfigurationProfileId=profile_id, ConfigurationVersion="1")
    with pytest.raises(ClientError) as exc:
        appconfig_client.start_deployment(
            ApplicationId=app_id, EnvironmentId=env_id, DeploymentStrategyId="AppConfig.AllAtOnce",
            ConfigurationProfileId=profile_id, ConfigurationVersion="1", LatestDeploymentNumber=0)
    assert exc.value.response["Error"]["Code"] == "ConflictException"


# ---------------------------------------------------------------------------
# Secrets Manager locations (#2450). Capture keys name the real-AWS runs in the
# PR's evidence: r1-r13, us-east-1 (r6 also us-west-2, r12-r13 a second account).
# ---------------------------------------------------------------------------

_UUID0 = "00000000-0000-0000-0000-000000000000"
_REQUEST_ID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


def _delete_application(ac, app):
    """Delete an application with everything under it, so later tests that list
    applications aren't pushed off the first page."""
    for env in ac.list_environments(ApplicationId=app)["Items"]:
        for d in ac.list_deployments(ApplicationId=app, EnvironmentId=env["Id"])["Items"]:
            if d["State"] in ("DEPLOYING", "BAKING"):
                try:
                    ac.stop_deployment(ApplicationId=app, EnvironmentId=env["Id"], DeploymentNumber=d["DeploymentNumber"])
                except ClientError:
                    pass
        ac.delete_environment(ApplicationId=app, EnvironmentId=env["Id"])
    for profile in ac.list_configuration_profiles(ApplicationId=app)["Items"]:
        for version in ac.list_hosted_configuration_versions(
                ApplicationId=app, ConfigurationProfileId=profile["Id"])["Items"]:
            ac.delete_hosted_configuration_version(ApplicationId=app, ConfigurationProfileId=profile["Id"],
                                                   VersionNumber=version["VersionNumber"])
        ac.delete_configuration_profile(ApplicationId=app, ConfigurationProfileId=profile["Id"])
    ac.delete_application(ApplicationId=app)


class _SecretTarget:
    """An application with an instant strategy, a slow one, and helpers that
    make a profile per secret, as the capture runs did."""

    def __init__(self, appconfig_client, appconfigdata_client, sm, name):
        self.ac, self.acd, self.sm = appconfig_client, appconfigdata_client, sm
        self.name = f"{name}-{os.urandom(4).hex()}"
        self.app = self.ac.create_application(Name=self.name)["Id"]
        self.now = self.ac.create_deployment_strategy(
            Name=f"{self.name}-now", DeploymentDurationInMinutes=0, FinalBakeTimeInMinutes=0,
            GrowthFactor=100, GrowthType="LINEAR", ReplicateTo="NONE")["Id"]
        self.slow = self.ac.create_deployment_strategy(
            Name=f"{self.name}-slow", DeploymentDurationInMinutes=10, FinalBakeTimeInMinutes=0,
            GrowthFactor=100, GrowthType="LINEAR", ReplicateTo="NONE")["Id"]
        self.role = f"arn:aws:iam::{get_account_id()}:role/{self.name}-role"
        self._n = 0
        self.created = []

    def secret(self, label, **value):
        s = self.sm.create_secret(Name=f"{self.name}-{label}", **value)
        self.created.append(s["ARN"])
        return {"arn": s["ARN"], "name": s["Name"], "v1": s.get("VersionId")}

    def cleanup(self):
        for arn in self.created:
            try:
                self.sm.delete_secret(SecretId=arn, ForceDeleteWithoutRecovery=True)
            except ClientError:
                pass
        _delete_application(self.ac, self.app)
        for strategy in self.ac.list_deployment_strategies()["Items"]:
            if strategy["Name"].startswith(self.name):
                try:
                    self.ac.delete_deployment_strategy(DeploymentStrategyId=strategy["Id"])
                except ClientError:
                    pass

    def env(self):
        self._n += 1
        return self.ac.create_environment(ApplicationId=self.app, Name=f"e{self._n}")["Id"]

    def profile(self, uri):
        self._n += 1
        return self.ac.create_configuration_profile(
            ApplicationId=self.app, Name=f"p{self._n}", LocationUri=uri, Type="AWS.Freeform",
            RetrievalRoleArn=self.role)["Id"]

    def deploy(self, env, profile, version, strategy=None, **extra):
        return self.ac.start_deployment(
            ApplicationId=self.app, EnvironmentId=env, ConfigurationProfileId=profile,
            DeploymentStrategyId=strategy or self.now, ConfigurationVersion=version, **extra)

    def deploy_error(self, env, profile, version, **extra):
        with pytest.raises(ClientError) as exc:
            self.deploy(env, profile, version, **extra)
        err = exc.value.response
        return err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"], err["Error"]["Message"]

    def session(self, env, profile):
        return self.acd.start_configuration_session(
            ApplicationIdentifier=self.app, EnvironmentIdentifier=env,
            ConfigurationProfileIdentifier=profile)["InitialConfigurationToken"]

    def poll(self, token):
        r = self.acd.get_latest_configuration(ConfigurationToken=token)
        return r["Configuration"].read(), r["ContentType"], r["NextPollConfigurationToken"]

    def served(self, uri, version, **extra):
        env, profile = self.env(), self.profile(uri)
        self.deploy(env, profile, version, **extra)
        return self.poll(self.session(env, profile))[:2]


@pytest.fixture
def secret_target(appconfig_client, appconfigdata_client, sm):
    target = _SecretTarget(appconfig_client, appconfigdata_client, sm, "secret-loc")
    yield target
    target.cleanup()


def test_secret_location_serves_the_deployed_version(secret_target):
    """r1 A.deploy.start, A.poll1, A.poll2-unchanged, A.put-secret-value,
    A.poll3-after-put, A.new.poll1-after-put: the deployed VersionId is served,
    an unchanged poll is empty, and a newer secret version is not picked up."""
    t = secret_target
    s = t.secret("json", SecretString='{"k": "v1"}')
    env, profile = t.env(), t.profile(s["arn"])
    deployment = t.deploy(env, profile, s["v1"])
    assert deployment["ConfigurationLocationUri"] == s["arn"]
    assert deployment["ConfigurationVersion"] == s["v1"]
    got = t.ac.get_deployment(ApplicationId=t.app, EnvironmentId=env,
                              DeploymentNumber=deployment["DeploymentNumber"])
    assert (got["State"], got["ConfigurationLocationUri"]) == ("COMPLETE", s["arn"])

    body, content_type, token = t.poll(t.session(env, profile))
    assert (body, content_type) == (b'{"k": "v1"}', "application/json")
    body, content_type, token = t.poll(token)
    assert (body, content_type) == (b"", "application/json")

    t.sm.put_secret_value(SecretId=s["arn"], SecretString='{"k": "v2"}')
    assert t.poll(token)[:2] == (b"", "application/json")
    assert t.poll(t.session(env, profile))[:2] == (b'{"k": "v1"}', "application/json")


def test_secret_location_serves_a_previous_version(secret_target):
    """r1 B.deploy-previous-id.start, B.poll1: a VersionId that is no longer
    AWSCURRENT is served as deployed."""
    t = secret_target
    s = t.secret("json", SecretString='{"k": "v1"}')
    t.sm.put_secret_value(SecretId=s["arn"], SecretString='{"k": "v2"}')
    assert t.served(f"secretsmanager://{s['name']}", s["v1"]) == (b'{"k": "v1"}', "application/json")


@pytest.mark.parametrize("form", ["arn", "name", "partial-arn", "scheme-arn"])
def test_secret_location_forms(secret_target, form):
    """arn: r1 A.deploy.start; name: r1 B.deploy-previous-id.start; partial-arn
    (no random suffix): r2 I.profile, I.deploy.final, I.poll1; scheme-arn: r2
    L.deploy.final, L.poll1. Each form is served and reported as the profile's LocationUri."""
    t = secret_target
    s = t.secret("json", SecretString='{"k": "v1"}')
    uri = {"arn": s["arn"], "name": f"secretsmanager://{s['name']}",
           "partial-arn": s["arn"].rsplit("-", 1)[0],
           "scheme-arn": f"secretsmanager://{s['arn']}"}[form]
    env, profile = t.env(), t.profile(uri)
    assert t.deploy(env, profile, s["v1"])["ConfigurationLocationUri"] == uri
    assert t.poll(t.session(env, profile))[:2] == (b'{"k": "v1"}', "application/json")


@pytest.mark.parametrize("value,content_type", [
    ({"SecretString": '{"k": "v1"}'}, "application/json"),          # r1 A.poll1
    ({"SecretString": "[1, 2]"}, "application/json"),               # r2 K.poll1
    ({"SecretString": "42"}, "application/json"),                   # r3 T-s-num.poll1
    ({"SecretString": '"x"'}, "application/json"),                  # r3 T-s-str.poll1
    ({"SecretString": "true"}, "application/json"),                 # r3 T-s-true.poll1
    ({"SecretString": "null"}, "application/json"),                 # r3 T-s-null.poll1
    ({"SecretString": '  {"a": 1}  '}, "application/json"),         # r3 T-s-ws.poll1
    ({"SecretString": "{}"}, "application/json"),                   # r3 T-s-empty-obj.poll1
    ({"SecretString": "plain text value"}, "application/octet-stream"),  # r2 F.poll1
    ({"SecretString": '{"k":'}, "application/octet-stream"),        # r2 J.poll1
    ({"SecretString": "NaN"}, "application/octet-stream"),          # r4 T-s-nan.poll1
    ({"SecretString": "Infinity"}, "application/octet-stream"),     # r4 T-s-inf.poll1
    ({"SecretBinary": b"\x00\x01binary\xff"}, "application/octet-stream"),  # r2 E.poll1
    ({"SecretBinary": b'{"a": 1}'}, "application/octet-stream"),    # r4 T-s-binjson.poll1
])
def test_secret_location_content_type(secret_target, value, content_type):
    """The body is the secret's value; it is typed JSON only when SecretString parses as JSON."""
    t = secret_target
    s = t.secret("value", **value)
    expected = value.get("SecretString", "").encode("utf-8") or value.get("SecretBinary")
    assert t.served(s["arn"], s["v1"]) == (expected, content_type)


@pytest.mark.parametrize("version,constraint,shown", [
    ("AWSCURRENT", "greater than or equal to 32", "AWSCURRENT"),     # r1 B.deploy-AWSCURRENT.start
    ("not-a-version", "greater than or equal to 32", "not-a-version"),  # r1 C.deploy-missing-label.start
    ("a" * 31, "greater than or equal to 32", "a" * 31),             # r2 M5.deploy.start
    ("c" * 65, "less than or equal to 64", "c" * 64 + " ..."),       # r3 O7-json-65.start
    ("abcdefghij" * 7, "less than or equal to 64", ("abcdefghij" * 7)[:64] + " ..."),  # r3 O6-json-70.start
])
def test_secret_location_version_length(secret_target, version, constraint, shown):
    t = secret_target
    s = t.secret("json", SecretString='{"k": "v1"}')
    status, code, message = t.deploy_error(t.env(), t.profile(s["arn"]), version)
    assert (status, code) == (400, "BadRequestException")
    assert re.fullmatch(
        re.escape(f"1 validation error detected: Value '{shown}' at 'versionId' failed to satisfy "
                  f"constraint: Member must have length {constraint} (Service: AWSSecretsManager; "
                  f"Status Code: 400; Error Code: ValidationException; Request ID: ")
        + _REQUEST_ID + re.escape("; Proxy: null)"), message)


@pytest.mark.parametrize("form,version", [
    ("arn", _UUID0),             # r1 C.deploy-missing-uuid.start
    ("name", _UUID0),            # r2 M1.deploy.start
    ("arn", "a" * 32),           # r2 M2.deploy.start
    ("arn", "a" * 64),           # r2 M3.deploy.start
    ("partial-arn", _UUID0),     # r2 M8.deploy.start
    ("arn", "other-secret"),     # r3 O4-json-other-secret-version.start
])
def test_secret_location_missing_version(secret_target, form, version):
    t = secret_target
    s = t.secret("json", SecretString='{"k": "v1"}')
    if version == "other-secret":
        version = t.secret("other", SecretString="{}")["v1"]
    uri = {"arn": s["arn"], "name": f"secretsmanager://{s['name']}",
           "partial-arn": s["arn"].rsplit("-", 1)[0]}[form]
    assert t.deploy_error(t.env(), t.profile(uri), version) == (
        404, "ResourceNotFoundException", f"Secret {uri} not found")


def test_secret_location_deleted_secret(secret_target):
    """r3 N.poll1, N.after-delete.poll1, N.redeploy-after-delete.start: a deployed
    value is still served once the secret is gone; a new deployment is not found."""
    t = secret_target
    s = t.secret("del", SecretString='{"k": "del"}')
    env, profile = t.env(), t.profile(s["arn"])
    t.deploy(env, profile, s["v1"])
    t.sm.delete_secret(SecretId=s["arn"], ForceDeleteWithoutRecovery=True)
    assert t.poll(t.session(env, profile))[:2] == (b'{"k": "del"}', "application/json")
    assert t.deploy_error(env, profile, s["v1"]) == (
        404, "ResourceNotFoundException", f"Secret {s['arn']} not found")


def test_secret_location_scheduled_for_deletion(secret_target):
    """r4 S.deploy-scheduled.start"""
    t = secret_target
    s = t.secret("sched", SecretString='{"k": "sched"}')
    env, profile = t.env(), t.profile(s["arn"])
    t.sm.delete_secret(SecretId=s["arn"], RecoveryWindowInDays=7)
    assert t.deploy_error(env, profile, s["v1"]) == (
        404, "ResourceNotFoundException", f"Secret {s['arn']} not found")


_CMK_REQUIRED = ("KmsKeyIdentifier required when deploying an AWS Secrets Manager secret "
                 "that is encrypted with a customer managed key (CMK)")


def test_secret_location_customer_managed_key(secret_target, kms_client):
    """r2 D.deploy.start; r3 D2.poll1, O1-cmk-missing-uuid.start, O2-cmk-short.start,
    O9-cmk-missing-uuid-with-key.start: a CMK secret needs KmsKeyIdentifier, checked
    after the version."""
    t = secret_target
    key = kms_client.create_key(Description=t.name)["KeyMetadata"]["Arn"]
    s = t.secret("cmk", SecretString='{"k": "cmk"}', KmsKeyId=key)
    env, profile = t.env(), t.profile(s["arn"])
    assert t.deploy_error(env, profile, s["v1"]) == (400, "BadRequestException", _CMK_REQUIRED)
    assert t.deploy_error(env, profile, _UUID0) == (
        404, "ResourceNotFoundException", f"Secret {s['arn']} not found")
    assert t.deploy_error(env, profile, _UUID0, KmsKeyIdentifier=key) == (
        404, "ResourceNotFoundException", f"Secret {s['arn']} not found")
    assert t.deploy_error(env, profile, "short")[:2] == (400, "BadRequestException")
    t.deploy(env, profile, s["v1"], KmsKeyIdentifier=key)
    assert t.poll(t.session(env, profile))[:2] == (b'{"k": "cmk"}', "application/json")


def test_secret_location_aws_managed_key_alias(secret_target):
    """r4 T-s-alias.poll1: the AWS managed key needs no KmsKeyIdentifier."""
    t = secret_target
    s = t.secret("alias", SecretString='{"k": "alias"}', KmsKeyId="alias/aws/secretsmanager")
    assert t.served(s["arn"], s["v1"]) == (b'{"k": "alias"}', "application/json")


def _in_progress(env):
    return (409, "ConflictException",
            f"Environment {env} currently has a state of DEPLOYING. The current deployment "
            f"must finish before a new deployment can be started.")


def test_secret_location_check_order(secret_target):
    """r4 Q1, Q2, Q3, Q4, Q6, Q7 (message also r5 R-secret.5): the version is checked after a deployment in
    progress and before the strategy and LatestDeploymentNumber."""
    t = secret_target
    s = t.secret("json", SecretString='{"k": "v1"}')
    env, profile = t.env(), t.profile(s["arn"])
    missing = (404, "ResourceNotFoundException", f"Secret {s['arn']} not found")
    assert t.deploy_error(env, profile, "short", strategy="zzzzzzz")[:2] == (400, "BadRequestException")
    assert t.deploy_error(env, profile, _UUID0, strategy="zzzzzzz") == missing
    assert t.deploy_error(env, profile, "short", LatestDeploymentNumber=5)[:2] == (400, "BadRequestException")
    assert t.deploy_error(env, profile, _UUID0, LatestDeploymentNumber=5) == missing

    t.deploy(env, profile, s["v1"], strategy=t.slow)
    assert t.deploy_error(env, profile, "short") == _in_progress(env)
    assert t.deploy_error(env, profile, _UUID0) == _in_progress(env)


def test_deployment_in_progress_is_checked_before_strategy_and_latest_number(secret_target):
    """r5 R-hosted.3-in-progress-missing-strategy, R-hosted.4-in-progress-latest-mismatch,
    R-secret.3, R-secret.4, R-hosted.5, R-secret.5 (message); r5 R-hosted.1 and R-secret.1 keep the strategy check
    ahead of LatestDeploymentNumber."""
    t = secret_target
    s = t.secret("json", SecretString='{"k": "v1"}')
    hosted = t.ac.create_configuration_profile(
        ApplicationId=t.app, Name="hosted", LocationUri="hosted", Type="AWS.Freeform")["Id"]
    t.ac.create_hosted_configuration_version(
        ApplicationId=t.app, ConfigurationProfileId=hosted, Content=b"{}",
        ContentType="application/json")
    for profile, version in ((hosted, "1"), (t.profile(s["arn"]), s["v1"])):
        env = t.env()
        assert t.deploy_error(env, profile, version, strategy="zzzzzzz", LatestDeploymentNumber=5) == (
            404, "ResourceNotFoundException", "DeploymentStrategy with Id zzzzzzz could not be found.")
        t.deploy(env, profile, version, strategy=t.slow)
        assert t.deploy_error(env, profile, version, strategy="zzzzzzz") == _in_progress(env)
        assert t.deploy_error(env, profile, version, LatestDeploymentNumber=5) == _in_progress(env)
        assert t.deploy_error(env, profile, version, LatestDeploymentNumber=1) == _in_progress(env)


def test_unchanged_poll_of_a_hosted_profile_is_typed_json(secret_target):
    """r3 HOSTED.poll1, HOSTED.poll2-unchanged"""
    t = secret_target
    env = t.env()
    profile = t.ac.create_configuration_profile(
        ApplicationId=t.app, Name="hosted", LocationUri="hosted", Type="AWS.Freeform")["Id"]
    t.ac.create_hosted_configuration_version(
        ApplicationId=t.app, ConfigurationProfileId=profile, Content=b"hello", ContentType="text/plain")
    t.deploy(env, profile, "1")
    body, content_type, token = t.poll(t.session(env, profile))
    assert (body, content_type) == (b"hello", "text/plain")
    assert t.poll(token)[:2] == (b"", "application/json")


def test_secret_deployment_value_survives_persistence(appconfig_service_state):
    """MiniStack contract, not an AWS behaviour: the value a secret deployment
    keeps, bytes included, round-trips through the persistence encoder."""
    import base64

    from ministack.core import persistence
    from ministack.services import secretsmanager as secretsmanager_service

    status, _, body = asyncio.run(secretsmanager_service.handle_request(
        "POST", "/", {"x-amz-target": "secretsmanager.CreateSecret"},
        json.dumps({"Name": "persisted-bin",
                    "SecretBinary": base64.b64encode(b"\x00\xff").decode()}).encode(), {}))
    assert status == 200
    secret = json.loads(body)
    try:
        _, app = _appconfig_json("POST", "/applications", {"Name": "persisted"})
        _, env = _appconfig_json("POST", f"/applications/{app['Id']}/environments", {"Name": "e"})
        _, profile = _appconfig_json("POST", f"/applications/{app['Id']}/configurationprofiles", {
            "Name": "p", "LocationUri": secret["ARN"],
            "RetrievalRoleArn": "arn:aws:iam::000000000000:role/r"})
        _, strategy = _appconfig_json("POST", "/deploymentstrategies", {
            "Name": "now", "DeploymentDurationInMinutes": 0, "FinalBakeTimeInMinutes": 0,
            "GrowthFactor": 100, "GrowthType": "LINEAR", "ReplicateTo": "NONE"})
        status, _ = _appconfig_json(
            "POST", f"/applications/{app['Id']}/environments/{env['Id']}/deployments", {
                "DeploymentStrategyId": strategy["Id"], "ConfigurationProfileId": profile["Id"],
                "ConfigurationVersion": secret["VersionId"]})
        assert status == 201

        state = json.loads(json.dumps(appconfig_service.get_state(), default=persistence._json_default),
                           object_hook=persistence._json_object_hook)
        appconfig_service.reset()
        appconfig_service.load_persisted_state(state)

        _, session = _appconfig_json("POST", "/configurationsessions", {
            "ApplicationIdentifier": app["Id"], "EnvironmentIdentifier": env["Id"],
            "ConfigurationProfileIdentifier": profile["Id"]})
        status, headers, served = _appconfig(
            "GET", "/configuration", query={"configuration_token": session["InitialConfigurationToken"]})
        assert (status, served, headers["Content-Type"]) == (200, b"\x00\xff", "application/octet-stream")
    finally:
        secretsmanager_service.reset()


def test_deployment_in_progress_is_checked_before_the_profile(secret_target):
    """r6 PNF.2-in-progress-missing-profile, PNF.3; r7 BK-hosted.6, BK-secret.6"""
    t = secret_target
    s = t.secret("json", SecretString='{"k": "v1"}')
    env, profile = t.env(), t.profile(s["arn"])
    t.deploy(env, profile, s["v1"], strategy=t.slow)
    assert t.deploy_error(env, "zzzzzzz", s["v1"]) == _in_progress(env)
    assert t.deploy_error(env, "zzzzzzz", "short") == _in_progress(env)


def test_conflict_while_baking(secret_target):
    """r7 BK-hosted.2-get (BAKING), BK-hosted.4, BK-secret.4, BK-secret.5: the
    environment still reports DEPLOYING during bake time."""
    t = secret_target
    bake = t.ac.create_deployment_strategy(
        Name=f"{t.name}-bake", DeploymentDurationInMinutes=0, FinalBakeTimeInMinutes=3,
        GrowthFactor=100, GrowthType="LINEAR", ReplicateTo="NONE")["Id"]
    s = t.secret("json", SecretString='{"k": "v1"}')
    env, profile = t.env(), t.profile(s["arn"])
    number = t.deploy(env, profile, s["v1"], strategy=bake)["DeploymentNumber"]
    assert t.ac.get_deployment(ApplicationId=t.app, EnvironmentId=env,
                               DeploymentNumber=number)["State"] == "BAKING"
    assert t.deploy_error(env, profile, s["v1"]) == _in_progress(env)
    assert t.deploy_error(env, profile, "short") == _in_progress(env)


@pytest.mark.parametrize("kind", ["secret", "hosted"])
def test_redeploying_the_same_version_is_an_unchanged_poll(secret_target, kind):
    """r7 REDEPLOY-secret.4-poll-after-redeploy, REDEPLOY-hosted.4"""
    t = secret_target
    env = t.env()
    if kind == "secret":
        s = t.secret("json", SecretString='{"k": "v1"}')
        profile, version, first = t.profile(s["arn"]), s["v1"], b'{"k": "v1"}'
    else:
        profile = t.ac.create_configuration_profile(
            ApplicationId=t.app, Name="hosted", LocationUri="hosted", Type="AWS.Freeform")["Id"]
        t.ac.create_hosted_configuration_version(
            ApplicationId=t.app, ConfigurationProfileId=profile, Content=b'{"h": 1}',
            ContentType="application/json")
        version, first = "1", b'{"h": 1}'
    t.deploy(env, profile, version)
    body, _, token = t.poll(t.session(env, profile))
    assert body == first
    t.deploy(env, profile, version)
    assert t.poll(token)[:2] == (b"", "application/json")


@pytest.mark.parametrize("kind", ["secret", "hosted"])
def test_start_deployment_requires_configuration_version(secret_target, kind):
    """r6 NOVER.missing, NOVER.empty, NOVER.hosted-missing; r7 NOVER.missing-app,
    missing-env, missing-profile, missing-strategy, NOVER-busy.2, NOVER-busy.3:
    checked before everything else."""
    t = secret_target
    raw = boto3.client("appconfig", endpoint_url=ENDPOINT, region_name="us-east-1",
                       aws_access_key_id="test", aws_secret_access_key="test",
                       config=Config(parameter_validation=False, retries={"max_attempts": 0}))
    s = t.secret("json", SecretString='{"k": "v1"}')
    if kind == "secret":
        profile = t.profile(s["arn"])
    else:
        profile = t.ac.create_configuration_profile(
            ApplicationId=t.app, Name="hosted", LocationUri="hosted", Type="AWS.Freeform")["Id"]
    busy = t.env()
    t.deploy(busy, t.profile(s["arn"]), s["v1"], strategy=t.slow)
    good = dict(ApplicationId=t.app, EnvironmentId=t.env(), ConfigurationProfileId=profile,
                DeploymentStrategyId=t.now)
    null = ("1 validation error detected: Value null at 'configurationVersion' failed to satisfy "
            "constraint: Member must not be null")
    empty = ("1 validation error detected: Value '' at 'configurationVersion' failed to satisfy "
             "constraint: Member must have length greater than or equal to 1")
    for over in ({}, {"ApplicationId": "zzzzzzz"}, {"EnvironmentId": "zzzzzzz"},
                 {"ConfigurationProfileId": "zzzzzzz"}, {"DeploymentStrategyId": "zzzzzzz"},
                 {"EnvironmentId": busy}):
        for extra, message in (({}, null), ({"ConfigurationVersion": ""}, empty)):
            with pytest.raises(ClientError) as exc:
                raw.start_deployment(**{**good, **over, **extra})
            err = exc.value.response
            assert (err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"],
                    err["Error"]["Message"]) == (400, "BadRequestException", message)


@pytest.mark.parametrize("form", ["alias", "key-id", "arn"])
def test_secret_location_customer_managed_key_forms(secret_target, kms_client, form):
    """r6 CMK-s-cmk-alias.deploy-no-key / deploy-key, CMK-s-cmk-id.*; r2 D.deploy.start (ARN)"""
    t = secret_target
    key = kms_client.create_key(Description=t.name)["KeyMetadata"]
    alias = f"alias/{t.name}-{form}"
    kms_client.create_alias(AliasName=alias, TargetKeyId=key["KeyId"])
    key_id = {"alias": alias, "key-id": key["KeyId"], "arn": key["Arn"]}[form]
    s = t.secret("cmk", SecretString='{"k": "cmk"}', KmsKeyId=key_id)
    env, profile = t.env(), t.profile(s["arn"])
    assert t.deploy_error(env, profile, s["v1"]) == (400, "BadRequestException", _CMK_REQUIRED)
    t.deploy(env, profile, s["v1"], KmsKeyIdentifier=key["Arn"])
    assert t.poll(t.session(env, profile))[:2] == (b'{"k": "cmk"}', "application/json")


@pytest.mark.parametrize("form", ["key-arn", "key-id", "alias-arn"])
def test_secret_location_aws_managed_key_forms(secret_target, form):
    """r6 MK-s-mk-arn.poll1, MK-s-mk-id.poll1, MK-s-mk-aliasarn.poll1: the AWS
    managed key in any form needs no KmsKeyIdentifier. MiniStack's KMS holds no
    AWS managed key, so these names resolve to nothing there."""
    t = secret_target
    acct, key = get_account_id(), "0f0e0d0c-0b0a-4090-8070-605040302010"
    key_id = {"key-arn": f"arn:aws:kms:us-east-1:{acct}:key/{key}", "key-id": key,
              "alias-arn": f"arn:aws:kms:us-east-1:{acct}:alias/aws/secretsmanager"}[form]
    s = t.secret("mk", SecretString='{"k": "mk"}', KmsKeyId=key_id)
    assert t.served(s["arn"], s["v1"]) == (b'{"k": "mk"}', "application/json")


def test_secret_location_any_kms_key_identifier_is_accepted(secret_target, kms_client):
    """r6 WRONGKEY2.default-with-cmk, CMKALIAS-KEY.cmk-with-own-alias"""
    t = secret_target
    key = kms_client.create_key(Description=t.name)["KeyMetadata"]
    alias = f"alias/{t.name}-own"
    kms_client.create_alias(AliasName=alias, TargetKeyId=key["KeyId"])
    plain = t.secret("plain", SecretString='{"k": "plain"}')
    assert t.served(plain["arn"], plain["v1"], KmsKeyIdentifier=key["Arn"])[0] == b'{"k": "plain"}'
    cmk = t.secret("cmk", SecretString='{"k": "cmk"}', KmsKeyId=key["Arn"])
    assert t.served(cmk["arn"], cmk["v1"], KmsKeyIdentifier=alias)[0] == b'{"k": "cmk"}'


_ROLE_PATTERN = r"^((arn):(aws|aws-cn|aws-iso|aws-iso-[a-z]{1}|aws-us-gov|aws-eusc):(iam)::\d{12}:role[/].*)$"
_STRATEGY_PATTERN = r"(^[a-z0-9]{4,7}$|^AppConfig\.[A-Za-z0-9]{9,40}$)"
_FEATURE_FLAGS_HOSTED_ONLY = ("ConfigurationProfiles of type 'AWS.AppConfig.FeatureFlags' can only "
                              "have a locationUri of 'hosted'")
_INVALID_NAME = re.escape(
    "Invalid name. Must be a valid name containing alphanumeric characters, or any of the following: "
    "-/_+=.@! (Service: AWSSecretsManager; Status Code: 400; Error Code: ValidationException; "
    "Request ID: ") + _REQUEST_ID + re.escape("; Proxy: null)")


def _raw_appconfig():
    return boto3.client("appconfig", endpoint_url=ENDPOINT, region_name="us-east-1",
                        aws_access_key_id="test", aws_secret_access_key="test",
                        config=Config(parameter_validation=False, retries={"max_attempts": 0}))


def _profile_error(t, uri, role="default", **extra):
    params = dict(ApplicationId=t.app, Name=f"p-{os.urandom(3).hex()}", LocationUri=uri,
                  Type=extra.pop("Type", "AWS.Freeform"), **extra)
    if role == "default":
        params["RetrievalRoleArn"] = t.role
    elif role is not None:
        params["RetrievalRoleArn"] = role
    with pytest.raises(ClientError) as exc:
        _raw_appconfig().create_configuration_profile(**params)
    err = exc.value.response
    return err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"], err["Error"]["Message"]


def _role_null(uri):
    return (400, "BadRequestException",
            "1 validation error detected: Value null at 'retrievalRoleArn' failed to satisfy constraint: "
            f"Member must not be null if the request has the following uri: {uri}")


def _empty_role():
    return (400, "BadRequestException",
            "2 validation errors detected: Value '' at 'retrievalRoleArn' failed to satisfy constraint: "
            f"Member must satisfy regular expression pattern: {_ROLE_PATTERN}; Value '' at "
            "'retrievalRoleArn' failed to satisfy constraint: Member must have length greater than or "
            "equal to 20")


def test_secret_profile_requires_a_retrieval_role(secret_target):
    """r2 G.profile; r10 C1 (unsupported ARN), C4 (aws-cn), C5 (empty scheme),
    C6 (other account), r3 O10 (missing secret): the role is checked before the location."""
    t = secret_target
    s = t.secret("json", SecretString="{}")
    acct = get_account_id()
    for uri in (s["arn"], f"arn:aws:secretsmanager:us-east-1:{acct}:foo:my-secret",
                f"arn:aws-cn:secretsmanager:cn-north-1:{acct}:secret:my-secret-AbCdEf",
                "secretsmanager://", "arn:aws:secretsmanager:us-east-1:123456789012:secret:x-AbCdEf",
                f"secretsmanager://{t.name}-missing"):
        assert _profile_error(t, uri, role=None) == _role_null(uri)


def test_secret_profile_retrieval_role_format(secret_target):
    """r10 C10; r11 F3 (before the FeatureFlags check), F8 (before the location
    type), F9 (before existence), F10, F11"""
    t = secret_target
    s = t.secret("json", SecretString="{}")
    acct = get_account_id()
    assert _profile_error(t, s["arn"], role="") == _empty_role()
    assert _profile_error(t, s["arn"], role="", Type="AWS.AppConfig.FeatureFlags") == _empty_role()
    assert _profile_error(t, f"arn:aws:secretsmanager:us-east-1:{acct}:foo:x", role="") == _empty_role()
    assert _profile_error(t, f"secretsmanager://{t.name}-missing", role="") == _empty_role()
    for role in ("notarole-but-long-enough", "arn:aws:iam::1:role/"):
        assert _profile_error(t, s["arn"], role=role) == (
            400, "BadRequestException",
            f"1 validation error detected: Value '{role}' at 'retrievalRoleArn' failed to satisfy "
            f"constraint: Member must satisfy regular expression pattern: {_ROLE_PATTERN}")


def test_feature_flags_profile_cannot_use_a_secret(secret_target):
    """r6 FF.profile; r10 C2 (missing secret), C3 (no role), C7 (unsupported),
    C8 (other account), C9 (empty scheme); r11 F13: checked before the role and the location."""
    t = secret_target
    s = t.secret("ff", SecretString="{}")
    acct = get_account_id()
    ff = "AWS.AppConfig.FeatureFlags"
    expected = (400, "BadRequestException", _FEATURE_FLAGS_HOSTED_ONLY)
    assert _profile_error(t, s["arn"], Type=ff) == expected
    assert _profile_error(t, s["arn"], role=None, Type=ff) == expected
    for uri in (f"secretsmanager://{t.name}-missing", f"arn:aws:secretsmanager:us-east-1:{acct}:foo:x",
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:x-AbCdEf", "secretsmanager://"):
        assert _profile_error(t, uri, Type=ff) == expected


@pytest.mark.parametrize("uri", [
    "arn:aws:secretsmanager:us-east-1:{acct}:foo:my-secret",    # r6 ARN-not-secret-resource.profile
    "arn:aws:secretsmanager::{acct}:secret:my-secret",          # r6 ARN-no-region.profile
])
def test_secret_profile_unsupported_arn(secret_target, uri):
    t = secret_target
    uri = uri.format(acct=get_account_id())
    assert _profile_error(t, uri) == (400, "BadRequestException", f"The Uri: {uri} is of an unsupported type")


@pytest.mark.parametrize("uri", [
    "arn:aws:secretsmanager:us-east-1:{acct}:secret:",   # r6 ARN-empty-name.profile
    "secretsmanager://",                                  # r6 SCHEME-empty.profile
])
def test_secret_profile_empty_secret_name(secret_target, uri):
    t = secret_target
    status, code, message = _profile_error(t, uri.format(acct=get_account_id()))
    assert (status, code) == (400, "BadRequestException")
    assert re.fullmatch(_INVALID_NAME, message)


def test_secret_profile_secret_must_exist(secret_target):
    """r2 M6.profile (ARN), M7.profile (secretsmanager://name); r10 C11 (scheduled
    for deletion); r6 WEST-arn, WEST-scheme-arn, WEST-scheme-name (another region);
    r6 ARN-other-partition (aws-cn)"""
    t = secret_target
    acct = get_account_id()
    west = boto3.client("secretsmanager", endpoint_url=ENDPOINT, region_name="us-west-2",
                        aws_access_key_id="test", aws_secret_access_key="test")
    w = west.create_secret(Name=f"{t.name}-west", SecretString="{}")
    sched = t.secret("sched", SecretString="{}")
    t.sm.delete_secret(SecretId=sched["arn"], RecoveryWindowInDays=7)
    try:
        for uri in (f"arn:aws:secretsmanager:us-east-1:{acct}:secret:{t.name}-missing-AbCdEf",
                    f"secretsmanager://{t.name}-missing", sched["arn"],
                    w["ARN"], f"secretsmanager://{w['ARN']}", f"secretsmanager://{w['Name']}",
                    f"arn:aws-cn:secretsmanager:cn-north-1:{acct}:secret:{t.name}-AbCdEf"):
            assert _profile_error(t, uri) == (404, "ResourceNotFoundException", f"Secret {uri} not found")
    finally:
        west.delete_secret(SecretId=w["ARN"], ForceDeleteWithoutRecovery=True)


def test_start_deployment_member_validation(secret_target):
    """r9 V1-V7, K3; r10 W1-W9: every failing member is reported, in the order
    configurationVersion, kmsKeyIdentifier, configurationProfileId,
    deploymentStrategyId, before the application is looked up."""
    t = secret_target
    s = t.secret("json", SecretString='{"k": "v1"}')
    env, profile = t.env(), t.profile(s["arn"])
    raw = _raw_appconfig()
    base = dict(ApplicationId=t.app, EnvironmentId=env, ConfigurationProfileId=profile,
                DeploymentStrategyId=t.now, ConfigurationVersion=s["v1"])
    null = "Value null at '{}' failed to satisfy constraint: Member must not be null"
    empty = "Value '' at '{}' failed to satisfy constraint: Member must have length greater than or equal to 1"
    pattern = ("Value '<v>' at 'deploymentStrategyId' failed to satisfy constraint: Member must satisfy "
               "regular expression pattern: " + _STRATEGY_PATTERN)
    drop = object()
    cases = [
        ({"DeploymentStrategyId": drop}, [null.format("deploymentStrategyId")]),                     # V1
        ({"ConfigurationProfileId": drop}, [null.format("configurationProfileId")]),                 # V2
        ({"DeploymentStrategyId": drop, "ConfigurationVersion": drop},
         [null.format("configurationVersion"), null.format("deploymentStrategyId")]),                # V3
        ({"ConfigurationProfileId": drop, "ConfigurationVersion": drop},
         [null.format("configurationVersion"), null.format("configurationProfileId")]),              # V4
        ({"DeploymentStrategyId": drop, "ConfigurationProfileId": drop, "ConfigurationVersion": drop},
         [null.format("configurationVersion"), null.format("configurationProfileId"),
          null.format("deploymentStrategyId")]),                                                     # V5
        ({"DeploymentStrategyId": ""}, [pattern.replace("<v>", "")]),                                        # V6
        ({"ConfigurationProfileId": ""}, [empty.format("configurationProfileId")]),                  # V7
        ({"KmsKeyIdentifier": ""}, [empty.format("kmsKeyIdentifier")]),                              # K3, W7
        ({"ConfigurationVersion": "", "DeploymentStrategyId": drop},
         [empty.format("configurationVersion"), null.format("deploymentStrategyId")]),               # W1
        ({"ConfigurationVersion": drop, "ConfigurationProfileId": ""},
         [null.format("configurationVersion"), empty.format("configurationProfileId")]),             # W2
        ({"KmsKeyIdentifier": "", "ConfigurationVersion": drop},
         [null.format("configurationVersion"), empty.format("kmsKeyIdentifier")]),                   # W3
        ({"KmsKeyIdentifier": "", "DeploymentStrategyId": ""},
         [empty.format("kmsKeyIdentifier"), pattern.replace("<v>", "")]),                                    # W4
        ({"ConfigurationVersion": "", "ConfigurationProfileId": "", "DeploymentStrategyId": "",
          "KmsKeyIdentifier": ""},
         [empty.format("configurationVersion"), empty.format("kmsKeyIdentifier"),
          empty.format("configurationProfileId"), pattern.replace("<v>", "")]),                              # W5
        ({"ConfigurationProfileId": drop, "DeploymentStrategyId": "", "KmsKeyIdentifier": ""},
         [empty.format("kmsKeyIdentifier"), null.format("configurationProfileId"), pattern.replace("<v>", "")]),  # W6
        ({"DeploymentStrategyId": "ZZZ"}, [pattern.replace("<v>", "ZZZ")]),                                  # W8
        ({"KmsKeyIdentifier": "", "ApplicationId": "zzzzzzz"}, [empty.format("kmsKeyIdentifier")]),  # W9
    ]
    for over, errors in cases:
        params = {k: v for k, v in {**base, **over}.items() if v is not drop}
        with pytest.raises(ClientError) as exc:
            raw.start_deployment(**params)
        err = exc.value.response
        count = "1 validation error" if len(errors) == 1 else f"{len(errors)} validation errors"
        assert (err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"], err["Error"]["Message"]) == (
            400, "BadRequestException", f"{count} detected: " + "; ".join(errors)), over


def test_secret_location_kms_check_comes_before_strategy_and_latest_number(secret_target, kms_client):
    """r9 K1-cmk-no-key-missing-strategy, K2-cmk-no-key-latest-mismatch"""
    t = secret_target
    key = kms_client.create_key(Description=t.name)["KeyMetadata"]["Arn"]
    s = t.secret("cmk", SecretString="{}", KmsKeyId=key)
    env, profile = t.env(), t.profile(s["arn"])
    expected = (400, "BadRequestException", _CMK_REQUIRED)
    assert t.deploy_error(env, profile, s["v1"], strategy="zzzzzzz") == expected
    assert t.deploy_error(env, profile, s["v1"], LatestDeploymentNumber=5) == expected


def test_secret_location_cmk_by_alias_arn_and_disabled(secret_target, kms_client):
    """r9 K4-cmk-aliasarn-no-key; D1-disabled-cmk-no-key"""
    t = secret_target
    key = kms_client.create_key(Description=t.name)["KeyMetadata"]
    alias = f"alias/{t.name}-aliasarn"
    kms_client.create_alias(AliasName=alias, TargetKeyId=key["KeyId"])
    alias_arn = key["Arn"].split(":key/")[0] + ":" + alias
    s = t.secret("cmk", SecretString="{}", KmsKeyId=alias_arn)
    assert t.deploy_error(t.env(), t.profile(s["arn"]), s["v1"]) == (400, "BadRequestException", _CMK_REQUIRED)
    kms_client.disable_key(KeyId=key["KeyId"])
    assert t.deploy_error(t.env(), t.profile(s["arn"]), s["v1"]) == (400, "BadRequestException", _CMK_REQUIRED)


@pytest.mark.parametrize("version,expected", [
    ("é" * 32, "not found"),        # r9 N1-e-acute-32
    ("é" * 31, "short"),            # r9 N2-e-acute-31
    ("\U0001F600" * 16, "short"),        # r9 N3-emoji-16: code points, not UTF-16 units
    ("\U0001F600" * 32, "not found"),    # r9 N4-emoji-32
    ("é" * 65, "long"),             # r9 N5-e-acute-65
])
def test_secret_location_version_length_counts_code_points(secret_target, version, expected):
    t = secret_target
    s = t.secret("json", SecretString="{}")
    status, code, message = t.deploy_error(t.env(), t.profile(s["arn"]), version)
    if expected == "not found":
        assert (status, code, message) == (404, "ResourceNotFoundException", f"Secret {s['arn']} not found")
    else:
        shown = version if expected == "short" else version[:64] + " ..."
        constraint = "greater than or equal to 32" if expected == "short" else "less than or equal to 64"
        assert (status, code) == (400, "BadRequestException")
        assert message.startswith(f"1 validation error detected: Value '{shown}' at 'versionId' failed "
                                  f"to satisfy constraint: Member must have length {constraint} ")


@pytest.mark.parametrize("value,body,content_type", [
    ("1e999", b"1e999", "application/json"),            # r9 J-s-1e999.poll1
    ("\ud800", b"?", "application/octet-stream"),        # r9 J-s-surrogate.poll1
])
def test_secret_location_json_edges(secret_target, value, body, content_type):
    t = secret_target
    s = t.secret("edge", SecretString=value)
    assert t.served(s["arn"], s["v1"]) == (body, content_type)


_ACCOUNT_B = "444455556666"


def _account_b_clients():
    kw = dict(endpoint_url=ENDPOINT, region_name="us-east-1", aws_access_key_id=_ACCOUNT_B,
              aws_secret_access_key="test")
    return boto3.client("secretsmanager", **kw), boto3.client("kms", **kw)


def _access_denied(t, resource):
    role_name = t.role.rsplit("/", 1)[-1]
    return re.escape(
        f"User: arn:aws:sts::{get_account_id()}:assumed-role/{role_name}/AwsAppConfig-ConfigurationRetriever "
        f"is not authorized to perform: secretsmanager:GetSecretValue on resource: {resource} because no "
        "resource-based policy allows the secretsmanager:GetSecretValue action (Service: AWSSecretsManager; "
        "Status Code: 400; Error Code: AccessDeniedException; Request ID: ") + _REQUEST_ID + re.escape("; Proxy: null)")


@pytest.fixture
def account_b_secrets(secret_target):
    """Account B's secrets, as round 12/13 set them up: under a customer managed
    key with a resource policy, the same without a policy, and under B's default key."""
    t = secret_target
    sm_b, kms_b = _account_b_clients()
    key = kms_b.create_key(Description=t.name)["KeyMetadata"]
    policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": t.role}, "Action": "secretsmanager:GetSecretValue",
        "Resource": "*"}]})
    made = {}
    for label, extra, with_policy in (("shared", {"KmsKeyId": key["Arn"]}, True),
                                      ("keyid", {"KmsKeyId": key["KeyId"]}, True),
                                      ("nopolicy", {"KmsKeyId": key["Arn"]}, False),
                                      ("default", {}, True),
                                      ("nopolicy-default", {}, False)):
        s = sm_b.create_secret(Name=f"{t.name}-b-{label}", SecretString=json.dumps({"k": label}), **extra)
        if with_policy:
            sm_b.put_resource_policy(SecretId=s["ARN"], ResourcePolicy=policy)
        made[label] = {"arn": s["ARN"], "v1": s["VersionId"]}
    yield made
    for s in made.values():
        sm_b.delete_secret(SecretId=s["arn"], ForceDeleteWithoutRecovery=True)


def test_cross_account_secret_is_served(secret_target, account_b_secrets, kms_client):
    """r12 X1-arn.profile, X1-arn.deploy-no-key, X2-scheme-arn.profile; r13
    Y1-arn-a-key.poll-a-key, Y2-scheme-arn-a-key.poll-a-key, Y3-keyid.deploy-no-key,
    Y3-keyid.poll-a-key: B's customer managed key (by ARN or key id) needs a
    KmsKeyIdentifier, and any key the caller names serves the value."""
    t = secret_target
    a_key = kms_client.create_key(Description=t.name)["KeyMetadata"]["Arn"]
    for label, uri in (("shared", account_b_secrets["shared"]["arn"]),
                       ("shared", "secretsmanager://" + account_b_secrets["shared"]["arn"]),
                       ("keyid", account_b_secrets["keyid"]["arn"])):
        s = account_b_secrets[label]
        env, profile = t.env(), t.profile(uri)
        assert t.deploy_error(env, profile, s["v1"]) == (400, "BadRequestException", _CMK_REQUIRED)
        t.deploy(env, profile, s["v1"], KmsKeyIdentifier=a_key)
        assert t.poll(t.session(env, profile))[:2] == (json.dumps({"k": label}).encode(), "application/json")


def test_cross_account_secret_needs_its_full_arn(secret_target, account_b_secrets):
    """r12 X3-partial-arn.profile, X6-missing-in-b.profile: refused whatever the
    resource policy says. (r12 X4 / r13 Y4, refused for having no policy, are
    IAM's to decide; MiniStack allows them.)"""
    t = secret_target
    partial = account_b_secrets["shared"]["arn"].rsplit("-", 1)[0]
    missing = f"arn:aws:secretsmanager:us-east-1:{_ACCOUNT_B}:secret:{t.name}-b-missing-AbCdEf"
    for resource in (partial, missing):
        status, code, message = _profile_error(t, resource)
        assert (status, code) == (400, "BadRequestException")
        assert re.fullmatch(_access_denied(t, resource), message), message


def test_cross_account_secret_under_the_default_key_is_not_found(secret_target, account_b_secrets):
    """r12 X5-default-key.profile: another account's AWS managed key can't be shared."""
    t = secret_target
    uri = account_b_secrets["default"]["arn"]
    assert _profile_error(t, uri) == (404, "ResourceNotFoundException", f"Secret {uri} not found")


def test_cross_account_access_denied_names_the_role_session(secret_target):
    """r14 P1 (role with a path: the session names the role alone), P2/P3 (a
    missing secret in another region of the other account), P4 (secretsmanager://
    names the ARN as the resource)"""
    t = secret_target
    east = f"arn:aws:secretsmanager:us-east-1:{_ACCOUNT_B}:secret:{t.name}-missing-AbCdEf"
    west = f"arn:aws:secretsmanager:us-west-2:{_ACCOUNT_B}:secret:{t.name}-missing-AbCdEf"
    pathed = t.role.rsplit("/", 1)[0] + "/ministack/verify/" + t.role.rsplit("/", 1)[1]
    for uri, role, resource in ((east, pathed, east), (west, t.role, west), (west, pathed, west),
                                ("secretsmanager://" + east, t.role, east)):
        status, code, message = _profile_error(t, uri, role=role)
        assert (status, code) == (400, "BadRequestException")
        assert re.fullmatch(_access_denied(t, resource), message), message


@pytest.mark.parametrize("depth,content_type", [
    (1000, "application/json"),            # r16 J-s-arr-1000.poll1, J-s-obj-1000, J-s-mixed-1000
    (1001, "application/octet-stream"),    # r16 J-s-arr-1001.poll1, J-s-obj-1001, J-s-mixed-1001
    (2000, "application/octet-stream"),    # r15 J-s-deep-valid.poll1
    (20000, "application/octet-stream"),   # r15 J-s-deep-valid-20k.poll1
])
def test_secret_location_json_nesting_limit(secret_target, depth, content_type):
    t = secret_target
    for value in ("[" * depth + "]" * depth, '{"a":' * (depth - 1) + "{}" + "}" * (depth - 1)):
        s = t.secret(f"deep-{len(value)}", SecretString=value)
        assert t.served(s["arn"], s["v1"]) == (value.encode(), content_type)


def test_secret_location_unclosed_deep_json_is_not_json(secret_target):
    """r15 J-s-deep-invalid.poll1"""
    t = secret_target
    s = t.secret("deep-open", SecretString="[" * 30000)
    assert t.served(s["arn"], s["v1"]) == (b"[" * 30000, "application/octet-stream")


def test_wrongly_typed_members(secret_target):
    """r15 T1-T5; r16 T6-T15; r17 U1-U7: a string member sent as another JSON
    type is reported before any other check, for the first such member by name."""
    t = secret_target
    s = t.secret("json", SecretString="{}")
    env, profile = t.env(), t.profile(s["arn"])
    raw = _raw_appconfig()
    base = dict(ApplicationId=t.app, EnvironmentId=env, ConfigurationProfileId=profile,
                DeploymentStrategyId=t.now, ConfigurationVersion=s["v1"])
    number = "NUMBER_VALUE can not be converted to a String"
    struct = "Start of structure or map found where not expected."
    for over, message in (
            ({"DeploymentStrategyId": 1234}, number),                                   # T1
            ({"ConfigurationVersion": 12345678901234567890123456789012}, number),       # T2
            ({"ConfigurationProfileId": ["x"]}, "Start of list found where not expected"),  # T3
            ({"KmsKeyIdentifier": 5}, number),                                          # T4
            ({"DeploymentStrategyId": {"a": 1}}, struct),                               # T6
            ({"DeploymentStrategyId": True}, "TRUE_VALUE can not be converted to a String"),   # T7
            ({"DeploymentStrategyId": 1.5}, number),                                    # T8
            ({"ConfigurationVersion": 5, "DeploymentStrategyId": None}, number),        # T9
            ({"ConfigurationVersion": "", "DeploymentStrategyId": 7}, number),          # T10
            ({"Description": 5}, number),                                               # T12
            ({"DeploymentStrategyId": False}, "FALSE_VALUE can not be converted to a String"),  # U1
            ({"DeploymentStrategyId": 1, "ConfigurationVersion": {"a": 1}}, struct),    # U2
            ({"DeploymentStrategyId": {"a": 1}, "ConfigurationVersion": 1}, number),    # U3
            ({"ConfigurationProfileId": ["x"], "ConfigurationVersion": True},
             "Start of list found where not expected"),                                 # U4
            ({"KmsKeyIdentifier": {"a": 1}, "Description": 1}, number),                 # U5
            ({"ApplicationId": "zzzzzzz", "DeploymentStrategyId": 1}, number)):         # before the app
        params = {k: v for k, v in {**base, **over}.items() if v is not None}
        with pytest.raises(ClientError) as exc:
            raw.start_deployment(**params)
        err = exc.value.response
        assert (err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"], err["Error"]["Message"]) == (
            400, "BadRequestException", message), over
    for over, message in (({"RetrievalRoleArn": 123456789012345678901}, number),       # T5
                          ({"RetrievalRoleArn": {"a": 1}}, struct),                    # T13
                          ({"RetrievalRoleArn": True}, "TRUE_VALUE can not be converted to a String"),  # T14
                          ({"LocationUri": 5}, number),                                # T15
                          ({"LocationUri": 1, "RetrievalRoleArn": {"a": 1}}, number),  # U6
                          ({"LocationUri": {"a": 1}, "RetrievalRoleArn": 1}, struct)): # U7
        params = {"LocationUri": s["arn"], "RetrievalRoleArn": t.role, **over}
        with pytest.raises(ClientError) as exc:
            raw.create_configuration_profile(ApplicationId=t.app, Name="typed", Type="AWS.Freeform", **params)
        err = exc.value.response
        assert (err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"], err["Error"]["Message"]) == (
            400, "BadRequestException", message), over


@pytest.mark.parametrize("rest", [
    "arn:aws:secretsmanager:us-east-1:{acct}:foo:x",          # r15 S1
    "arn:aws:secretsmanager:us-east-1:{acct}:secret:",        # r15 S2
    "arn:aws:secretsmanager::{acct}:secret:x",                # r15 S3
    "arn:aws:ssm:us-east-1:{acct}:parameter/x",               # r15 S4
    "has space",                                              # r16 S5
    "café",                                              # r16 S6
    "a:b",                                                    # r16 S8
    "n" * 513,                                                # r16 S10
])
def test_secret_profile_scheme_needs_a_secret_name_or_arn(secret_target, rest):
    t = secret_target
    status, code, message = _profile_error(t, "secretsmanager://" + rest.format(acct=get_account_id()))
    assert (status, code) == (400, "BadRequestException")
    assert re.fullmatch(_INVALID_NAME, message)


@pytest.mark.parametrize("rest", ["a-b_c+d=e.f@g!h/i", "n" * 512])   # r16 S7, S9: valid names
def test_secret_profile_scheme_accepts_any_valid_name(secret_target, rest):
    """AWS went on to read these names (its answer then depends on the role's
    IAM policy, r16 S7/S9); a missing one is not found, as for r2 M7."""
    t = secret_target
    uri = "secretsmanager://" + rest
    assert _profile_error(t, uri) == (404, "ResourceNotFoundException", f"Secret {uri} not found")


def test_cross_account_secret_in_another_region_is_refused(secret_target, kms_client):
    """r18 Z1-west-arn.profile, Z2-west-scheme-arn.profile: an existing secret in
    another account and region (with a resource policy) is refused like a missing one."""
    t = secret_target
    kw = dict(endpoint_url=ENDPOINT, region_name="us-west-2", aws_access_key_id=_ACCOUNT_B,
              aws_secret_access_key="test")
    sm_w, kms_w = boto3.client("secretsmanager", **kw), boto3.client("kms", **kw)
    key = kms_w.create_key(Description=t.name)["KeyMetadata"]["Arn"]
    s = sm_w.create_secret(Name=f"{t.name}-bw", SecretString="{}", KmsKeyId=key)
    sm_w.put_resource_policy(SecretId=s["ARN"], ResourcePolicy=json.dumps({
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"AWS": t.role},
                                                "Action": "secretsmanager:GetSecretValue", "Resource": "*"}]}))
    try:
        for uri in (s["ARN"], "secretsmanager://" + s["ARN"]):
            status, code, message = _profile_error(t, uri)
            assert (status, code) == (400, "BadRequestException")
            assert re.fullmatch(_access_denied(t, s["ARN"]), message), message
    finally:
        sm_w.delete_secret(SecretId=s["ARN"], ForceDeleteWithoutRecovery=True)


def test_cross_account_secret_at_start_deployment(secret_target, account_b_secrets, kms_client):
    """r18 D1-missing-uuid, D1-short, D1-missing-uuid-no-key (the version before
    the key), D1-new-version (a newer version is served; AWS may serve the previous
    value to a new session for a second or two, r18 D1-new-version.poll / r19
    R4.5, MiniStack has no such delay), D2-scheduled. (r18 D3, a resource policy
    removed after the profile was made, is IAM's to decide.)"""
    t = secret_target
    sm_b, _ = _account_b_clients()
    a_key = {"KmsKeyIdentifier": kms_client.create_key(Description=t.name)["KeyMetadata"]["Arn"]}
    s = account_b_secrets["shared"]
    env, profile = t.env(), t.profile(s["arn"])
    missing = (404, "ResourceNotFoundException", f"Secret {s['arn']} not found")
    assert t.deploy_error(env, profile, _UUID0, **a_key) == missing
    assert t.deploy_error(env, profile, _UUID0) == missing
    assert t.deploy_error(env, profile, "short", **a_key)[:2] == (400, "BadRequestException")
    v2 = sm_b.put_secret_value(SecretId=s["arn"], SecretString='{"k": "v2"}')["VersionId"]
    t.deploy(env, profile, s["v1"], **a_key)
    assert t.poll(t.session(env, profile))[0] == b'{"k": "shared"}'
    t.deploy(env, profile, v2, **a_key)
    assert t.poll(t.session(env, profile))[0] == b'{"k": "v2"}'
    k = account_b_secrets["keyid"]
    env, profile = t.env(), t.profile(k["arn"])
    sm_b.delete_secret(SecretId=k["arn"], RecoveryWindowInDays=7)
    assert t.deploy_error(env, profile, k["v1"], **a_key) == (
        404, "ResourceNotFoundException", f"Secret {k['arn']} not found")


def test_newer_secret_version_reaches_running_and_new_sessions(secret_target):
    """r19 R1.5-old-session-after-v2, R1.6-new-session-t*, R2.2 (fresh environment),
    R3.2 (fresh profile), R1.9 (a third version)"""
    t = secret_target
    s = t.secret("rot", SecretString='{"k": "v1"}')
    env, profile = t.env(), t.profile(s["arn"])
    t.deploy(env, profile, s["v1"])
    body, _, old = t.poll(t.session(env, profile))
    assert body == b'{"k": "v1"}'
    v2 = t.sm.put_secret_value(SecretId=s["arn"], SecretString='{"k": "v2"}')["VersionId"]
    t.deploy(env, profile, v2)
    assert t.poll(old)[:2] == (b'{"k": "v2"}', "application/json")
    assert t.poll(t.session(env, profile))[0] == b'{"k": "v2"}'
    assert t.served(s["arn"], v2) == (b'{"k": "v2"}', "application/json")
    v3 = t.sm.put_secret_value(SecretId=s["arn"], SecretString='{"k": "v3"}')["VersionId"]
    t.deploy(env, profile, v3)
    assert t.poll(t.session(env, profile))[0] == b'{"k": "v3"}'


def test_start_deployment_maximum_lengths(secret_target):
    """r20 L1-L11; r24 D1-D5 (description sits before deploymentStrategyId)"""
    t = secret_target
    s = t.secret("json", SecretString="{}")
    env, profile = t.env(), t.profile(s["arn"])
    raw = _raw_appconfig()
    base = dict(ApplicationId=t.app, EnvironmentId=env, ConfigurationProfileId=profile,
                DeploymentStrategyId=t.now, ConfigurationVersion=s["v1"])

    def too_long(wire, value, limit):
        return (f"Value '{value}' at '{wire}' failed to satisfy constraint: Member must have length "
                f"less than or equal to {limit}")
    empty = "Value '' at '{}' failed to satisfy constraint: Member must have length greater than or equal to 1"
    d = "d" * 1025
    cases = [
        ({"ConfigurationVersion": "x" * 1025}, [too_long("configurationVersion", "x" * 1025, 1024)]),   # L2
        ({"ConfigurationVersion": "x" * 2000}, [too_long("configurationVersion", "x" * 2000, 1024)]),   # L3
        ({"Description": d}, [too_long("description", d, 1024)]),                                       # L4
        ({"KmsKeyIdentifier": "k" * 2049}, [too_long("kmsKeyIdentifier", "k" * 2049, 2048)]),           # L6
        ({"ConfigurationProfileId": "p" * 129}, [too_long("configurationProfileId", "p" * 129, 128)]),  # L8
        ({"ConfigurationVersion": "x" * 1025, "DeploymentStrategyId": None},
         [too_long("configurationVersion", "x" * 1025, 1024),
          "Value null at 'deploymentStrategyId' failed to satisfy constraint: Member must not be null"]),  # L10
        ({"ConfigurationVersion": "x" * 1025, "KmsKeyIdentifier": "k" * 2049},
         [too_long("configurationVersion", "x" * 1025, 1024), too_long("kmsKeyIdentifier", "k" * 2049, 2048)]),  # L11
        ({"Description": d, "ConfigurationVersion": ""},
         [empty.format("configurationVersion"), too_long("description", d, 1024)]),                     # D1
        ({"Description": d, "KmsKeyIdentifier": ""},
         [empty.format("kmsKeyIdentifier"), too_long("description", d, 1024)]),                         # D2
        ({"Description": d, "ConfigurationProfileId": ""},
         [empty.format("configurationProfileId"), too_long("description", d, 1024)]),                   # D3
        ({"Description": d, "DeploymentStrategyId": None},
         [too_long("description", d, 1024),
          "Value null at 'deploymentStrategyId' failed to satisfy constraint: Member must not be null"]),  # D4
        ({"Description": d, "ConfigurationVersion": "", "KmsKeyIdentifier": "", "ConfigurationProfileId": "",
          "DeploymentStrategyId": ""},
         [empty.format("configurationVersion"), empty.format("kmsKeyIdentifier"),
          empty.format("configurationProfileId"), too_long("description", d, 1024),
          "Value '' at 'deploymentStrategyId' failed to satisfy constraint: Member must satisfy regular "
          "expression pattern: " + _STRATEGY_PATTERN]),                                                 # D5
    ]
    for over, errors in cases:
        params = {k: v for k, v in {**base, **over}.items() if v is not None}
        with pytest.raises(ClientError) as exc:
            raw.start_deployment(**params)
        count = "1 validation error" if len(errors) == 1 else f"{len(errors)} validation errors"
        assert exc.value.response["Error"]["Message"] == f"{count} detected: " + "; ".join(errors), over
    # r20 L1: 1024 passes AppConfig's check and reaches Secrets Manager's.
    status, code, message = t.deploy_error(env, profile, "x" * 1024)
    assert (status, code) == (400, "BadRequestException") and "at 'versionId'" in message
    # r20 L5: a 1024-character description is accepted.
    t.deploy(env, profile, s["v1"], Description="d" * 1024)


@pytest.mark.parametrize("account", ["", "abc", "12345"])   # r20 M1, M2, M3
def test_secret_profile_arn_account_must_be_12_digits(secret_target, account):
    t = secret_target
    uri = f"arn:aws:secretsmanager:us-east-1:{account}:secret:{t.name}-x"
    assert _profile_error(t, uri) == (400, "BadRequestException", f"The Uri: {uri} is of an unsupported type")
    # r20 M4: inside secretsmanager:// it is read as a name.
    status, code, message = _profile_error(t, "secretsmanager://" + uri)
    assert (status, code) == (400, "BadRequestException") and re.fullmatch(_INVALID_NAME, message)


def test_secret_profile_on_a_secret_without_a_value(secret_target):
    """r20 N1-novalue.profile, N2-novalue-scheme.profile"""
    t = secret_target
    s = t.secret("novalue-profile")
    for uri in (s["arn"], "secretsmanager://" + s["name"]):
        assert _profile_error(t, uri) == (404, "ResourceNotFoundException", f"Secret {uri} not found")


def test_secret_profile_checks_around_the_application_lookup(secret_target):
    """r20 A1-A5, r21 A6-A11: the RetrievalRoleArn and type checks come before the
    application lookup, the location checks after it."""
    t = secret_target
    s = t.secret("json", SecretString="{}")
    acct = get_account_id()
    raw = _raw_appconfig()

    def create(**params):
        params.setdefault("Type", "AWS.Freeform")
        with pytest.raises(ClientError) as exc:
            raw.create_configuration_profile(ApplicationId="zzzzzzz", Name="a", **params)
        err = exc.value.response
        return err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"], err["Error"]["Message"]

    assert create(LocationUri=s["arn"], RetrievalRoleArn="x")[2].startswith("2 validation errors detected: Value 'x'")
    assert create(LocationUri=s["arn"], RetrievalRoleArn=t.role, Type="AWS.AppConfig.FeatureFlags") == (
        400, "BadRequestException", _FEATURE_FLAGS_HOSTED_ONLY)
    assert create(LocationUri=s["arn"]) == _role_null(s["arn"])
    assert create(LocationUri=5, RetrievalRoleArn=t.role)[2] == "NUMBER_VALUE can not be converted to a String"
    for uri in (f"secretsmanager://{t.name}-nope", f"arn:aws:secretsmanager:us-east-1:{acct}:foo:x",
                "secretsmanager://has space", f"arn:aws:secretsmanager:us-east-1:{acct}:secret:",
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:x-AbCdEf",
                "arn:aws:secretsmanager:us-east-1:abc:secret:x"):
        assert create(LocationUri=uri, RetrievalRoleArn=t.role)[:2] == (404, "ResourceNotFoundException"), uri


def test_secret_profile_name_validation_joins_the_role_checks(secret_target):
    """r22 B1-B5"""
    t = secret_target
    s = t.secret("json", SecretString="{}")
    raw = _raw_appconfig()
    role_errors = (f"Value 'x' at 'retrievalRoleArn' failed to satisfy constraint: Member must satisfy regular "
                   f"expression pattern: {_ROLE_PATTERN}; Value 'x' at 'retrievalRoleArn' failed to satisfy "
                   "constraint: Member must have length greater than or equal to 20")
    null_name = "Value null at 'name' failed to satisfy constraint: Member must not be null"
    empty_name = "Value '' at 'name' failed to satisfy constraint: Member must have length greater than or equal to 1"
    for params, message in (
            (dict(RetrievalRoleArn="x"), f"3 validation errors detected: {role_errors}; {null_name}"),         # B1
            (dict(RetrievalRoleArn=t.role, Type="AWS.AppConfig.FeatureFlags"),
             f"1 validation error detected: {null_name}"),                                                  # B2
            (dict(), f"1 validation error detected: {null_name}"),                                          # B3
            (dict(Name="", RetrievalRoleArn="x"), f"3 validation errors detected: {role_errors}; {empty_name}"),  # B4
            (dict(Name="", RetrievalRoleArn=t.role), f"1 validation error detected: {empty_name}")):        # B5
        params.setdefault("Type", "AWS.Freeform")
        with pytest.raises(ClientError) as exc:
            raw.create_configuration_profile(ApplicationId=t.app, LocationUri=s["arn"], **params)
        assert exc.value.response["Error"]["Message"] == message, params


@pytest.mark.parametrize("value,content_type", [
    ("1" * 1000, "application/json"),                       # r21 n-1000
    ("1" * 1001, "application/octet-stream"),               # r21 n-1001
    ("-" + "1" * 1000, "application/json"),                 # r21 n-neg-1000
    ("-" + "1" * 1001, "application/octet-stream"),         # r21 n-neg-1001
    ("[" + "1" * 1001 + "]", "application/octet-stream"),   # r21 n-in-array
    ('{"a":' + "1" * 1001 + "}", "application/octet-stream"),  # r21 n-in-obj
    ("0." + "1" * 2000, "application/octet-stream"),        # r21 n-frac-long
    ("1" * 999 + "e5", "application/json"),                 # r21 n-exp
    ("0." + "1" * 1000, "application/json"),                # r22 n-frac-d1000
    ("0." + "1" * 1001, "application/octet-stream"),        # r22 n-frac-d1001
    ("1" * 1000 + "." + "1" * 1000, "application/octet-stream"),  # r22 n-int1000-frac1000
    ("1e" + "0" * 1000, "application/json"),                # r22 n-exp-d1000
    ("1e" + "0" * 1001, "application/octet-stream"),        # r22 n-exp-d1001
    ("1" * 1000 + "e1", "application/json"),                # r22 n-int1000-exp
    ("1." + "1" * 1000, "application/json"),                # r23 n-1-f1000
    ("11." + "1" * 999, "application/json"),                # r23 n-11-f999
    ("1" * 500 + "." + "1" * 501, "application/json"),      # r23 n-500-f501
    ("-0." + "1" * 1000, "application/json"),               # r23 n-neg0-f1000
    ("0." + "1" * 1000 + "e5", "application/octet-stream"), # r23 n-0-f1000-e5
    ("1" * 4300, "application/octet-stream"),               # r20 J-s-digits-4300
    ("﻿{}", "application/octet-stream"),               # r20 J-s-bom
    ('{"a":"\u0001"}', "application/octet-stream"),         # r20 J-s-ctrl
    ('{"a":1,"a":2}', "application/json"),                  # r20 J-s-dup
    ('"\t"', "application/octet-stream"),                   # r20 J-s-tab
])
def test_secret_location_json_number_and_text_rules(secret_target, value, content_type):
    t = secret_target
    s = t.secret("num", SecretString=value)
    assert t.served(s["arn"], s["v1"]) == (value.encode("utf-8"), content_type)


def test_cross_account_secret_that_cannot_be_read_is_not_found(secret_target, account_b_secrets):
    """r25 S1-scheduled-at-create, S2-scheduled-at-create-scheme, S3-novalue-at-create"""
    t = secret_target
    sm_b, _ = _account_b_clients()
    s = account_b_secrets["keyid"]
    sm_b.delete_secret(SecretId=s["arn"], RecoveryWindowInDays=7)
    nv = sm_b.create_secret(Name=f"{t.name}-b-novalue")
    try:
        for uri in (s["arn"], "secretsmanager://" + s["arn"], nv["ARN"]):
            assert _profile_error(t, uri) == (404, "ResourceNotFoundException", f"Secret {uri} not found")
    finally:
        sm_b.delete_secret(SecretId=nv["ARN"], ForceDeleteWithoutRecovery=True)


def test_version_left_by_a_valueless_create_is_not_found_before_the_key_check(secret_target, kms_client):
    """r8 NOVAL.create / NOVAL.list-versions (AWS makes no version for a secret
    created without a value) with r3 O1, O9 (a missing version is reported before
    the key check). MiniStack's Secrets Manager returns a VersionId for it."""
    t = secret_target
    key = kms_client.create_key(Description=t.name)["KeyMetadata"]["Arn"]
    s = t.secret("late-value", KmsKeyId=key)
    t.sm.put_secret_value(SecretId=s["arn"], SecretString="{}")
    env, profile = t.env(), t.profile(s["arn"])
    assert t.deploy_error(env, profile, s["v1"]) == (
        404, "ResourceNotFoundException", f"Secret {s['arn']} not found")


def test_null_location_uri_profile_deploys_as_before(appconfig_client):
    """MiniStack contract: a profile stored with a null LocationUri (raw request)
    keeps its earlier hosted handling instead of failing with a 500."""
    import urllib.request
    app = appconfig_client.create_application(Name=f"nulluri-{os.urandom(3).hex()}")["Id"]
    env = appconfig_client.create_environment(ApplicationId=app, Name="e")["Id"]
    req = urllib.request.Request(f"{ENDPOINT}/applications/{app}/configurationprofiles", method="POST",
                                 data=json.dumps({"Name": "n", "LocationUri": None}).encode(),
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/appconfig/aws4_request, SignedHeaders=host, Signature=x"})
    profile = json.loads(urllib.request.urlopen(req).read())["Id"]
    try:
        deployment = appconfig_client.start_deployment(
            ApplicationId=app, EnvironmentId=env, ConfigurationProfileId=profile,
            DeploymentStrategyId="AppConfig.AllAtOnce", ConfigurationVersion="1")
        assert deployment["ConfigurationLocationUri"] == "hosted"
    finally:
        _delete_application(appconfig_client, app)


@pytest.mark.parametrize("value,content_type", [
    ("1e+" + "0" * 1000, "application/json"),            # r26 J-e-plus-1000
    ("1e+" + "0" * 1001, "application/octet-stream"),    # r26 J-e-plus-1001
    ("1e-" + "0" * 1000, "application/json"),            # r26 J-e-minus-1000
    ("1e-" + "0" * 1001, "application/octet-stream"),    # r26 J-e-minus-1001
])
def test_secret_location_exponent_sign_does_not_count(secret_target, value, content_type):
    t = secret_target
    s = t.secret("exp", SecretString=value)
    assert t.served(s["arn"], s["v1"]) == (value.encode(), content_type)


def test_thirteen_digit_account_is_unsupported(secret_target):
    """r26 M5-13-digit-account.profile"""
    t = secret_target
    uri = f"arn:aws:secretsmanager:us-east-1:1234567890123:secret:{t.name}-x"
    assert _profile_error(t, uri) == (400, "BadRequestException", f"The Uri: {uri} is of an unsupported type")


def test_maximum_lengths_count_code_points(secret_target):
    """r26 U1-U6: 1024 four-byte characters are within AppConfig's 1024 limit;
    r9 N1-N5, r27, r28 for Secrets Manager's versionId limits."""
    t = secret_target
    s = t.secret("json", SecretString="{}")
    env, profile = t.env(), t.profile(s["arn"])
    emoji = "\U0001F600"
    t.deploy(env, profile, s["v1"], Description=emoji * 1024)
    raw = _raw_appconfig()
    for over, wire in (({"Description": emoji * 1025}, "description"),
                       ({"ConfigurationVersion": emoji * 1025}, "configurationVersion")):
        params = {**dict(ApplicationId=t.app, EnvironmentId=t.env(), ConfigurationProfileId=profile,
                         DeploymentStrategyId=t.now, ConfigurationVersion=s["v1"]), **over}
        with pytest.raises(ClientError) as exc:
            raw.start_deployment(**params)
        assert exc.value.response["Error"]["Message"] == (
            f"1 validation error detected: Value '{emoji * 1025}' at '{wire}' failed to satisfy constraint: "
            "Member must have length less than or equal to 1024")
    # r26 U5, r27 V3/V4, r28 W1/W2: Secrets Manager counts code points but cuts the
    # echoed value at 64 UTF-16 units, showing half of a split character as "?".
    for version, shown, constraint in ((emoji * 513, emoji * 32, "less than or equal to 64"),
                                       (emoji * 65, emoji * 32, "less than or equal to 64"),
                                       ("a" + emoji * 64, "a" + emoji * 31 + "?", "less than or equal to 64"),
                                       ("a" + emoji * 70, "a" + emoji * 31 + "?", "less than or equal to 64")):
        status, code, message = t.deploy_error(t.env(), profile, version)
        assert (status, code) == (400, "BadRequestException")
        assert message.startswith(f"1 validation error detected: Value '{shown} ...' at 'versionId' failed to "
                                  f"satisfy constraint: Member must have length {constraint} "), message
    for version in (emoji * 33, emoji * 40, emoji * 64, "a" + emoji * 40, "\u00e9" * 64):   # r27 V1-V3, V5, V6
        assert t.deploy_error(t.env(), profile, version) == (
            404, "ResourceNotFoundException", f"Secret {s['arn']} not found")


@pytest.mark.parametrize("binary", ["abc", 123, [1], {"a": 1}, True])
def test_unusable_stored_secret_binary_is_not_found(secret_target, binary):
    """MiniStack contract, not an AWS behaviour: AWS refuses such a SecretBinary
    at CreateSecret, but MiniStack's Secrets Manager stores it raw; deploying it
    answers "Secret <uri> not found" instead of failing with a 500."""
    t = secret_target
    import urllib.request
    s = t.secret("bin", SecretString="{}")
    env, profile = t.env(), t.profile(s["arn"])
    req = urllib.request.Request(ENDPOINT + "/", method="POST",
                                 data=json.dumps({"SecretId": s["arn"], "SecretBinary": binary}).encode(),
                                 headers={"Content-Type": "application/x-amz-json-1.1",
                                          "X-Amz-Target": "secretsmanager.PutSecretValue",
                                          "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/"
                                                           "secretsmanager/aws4_request, SignedHeaders=host, Signature=x"})
    v2 = json.loads(urllib.request.urlopen(req).read())["VersionId"]
    assert t.deploy_error(env, profile, v2) == (404, "ResourceNotFoundException", f"Secret {s['arn']} not found")


@pytest.mark.parametrize("value,content_type", [
    ('{"a":"' + "1" * 1001 + '"}', "application/json"),           # r29 j-digits-in-string
    ('["' + "[" * 1001 + '"]', "application/json"),               # r29 j-brackets-in-string
    ('"\\u00e9\\/\\b\\f\\n\\r\\t\\"\\\\"', "application/json"),   # r29 j-escapes
    ('"\\x"', "application/octet-stream"),                        # r29 j-bad-escape
    ('"\\u12"', "application/octet-stream"),                      # r29 j-short-unicode
    ("01", "application/octet-stream"),                           # r29 j-leading-zero
    ("-", "application/octet-stream"),                            # r29 j-minus
    (".5", "application/octet-stream"),                           # r29 j-dot5
    ("1.", "application/octet-stream"),                           # r29 j-1dot
    ("+1", "application/octet-stream"),                           # r29 j-plus1
    ("[1,]", "application/octet-stream"),                         # r29 j-trailing-comma
    ("'a'", "application/octet-stream"),                          # r29 j-single-quote
    ("\r\n{}\r\n", "application/json"),                           # r29 j-cr-ws
    (" {}", "application/octet-stream"),                     # r29 j-nbsp-ws
    ("1 2", "application/json"),                                  # r29 j-two-values
    (" ", "application/json"),                                    # r29 j-empty
    ('"\\ud800"', "application/json"),                            # r29 j-surrogate-escape
    ('"\u007f"', "application/json"),                             # r29 j-del-char
    ("1E5", "application/json"),                                  # r29 j-upper-e
    ('{"a":[1,{"b":null}],"c":true}', "application/json"),        # r29 j-nested-mixed
    ("{} x", "application/json"),                                 # r30 t-obj-space-x
    ("1 x", "application/json"),                                  # r30 t-num-space-x
    ("[1] ]", "application/json"),                                # r30 t-arr-extra-close
    ("1,2", "application/octet-stream"),                          # r30 t-comma
    ("\t", "application/json"),                                   # r30 t-tab
    ("{}{}", "application/json"),                                 # r30 t-two-objs
    ("nul", "application/octet-stream"),                          # r30 t-nul
    ("true false", "application/json"),                           # r30 t-true-false
    ("1x", "application/octet-stream"),                           # r30 t-num-x
    ("truex", "application/octet-stream"),                        # r30 t-true-x
    ("[1]x", "application/json"),                                 # r30 t-arr-x
    ("{}x", "application/json"),                                  # r30 t-obj-x
    ("1 /*c*/", "application/json"),                              # r30 t-comment
    ('{} {"a":', "application/json"),                             # r30 t-bad-after
    ("1 .", "application/json"),                                  # r30 t-num-dot-after
    ("\r\n", "application/json"),                                 # r30 t-ws-only-crlf
    ('"x"y', "application/json"),                                 # r31 t-str-y
    ('"x",', "application/json"),                                 # r31 t-str-comma
    ("true,", "application/json"),                                # r31 t-true-comma
    ("null]", "application/json"),                                # r31 t-null-bracket
    ("1}", "application/octet-stream"),                           # r31 t-num-brace
    ("false1", "application/octet-stream"),                       # r31 t-false-1
    ("true$", "application/json"),                                # r31 t-true-dollar
    ("true_", "application/octet-stream"),                        # r31 t-true-underscore
    ('"x" y', "application/json"),                                # r31 t-str-space-y
    ("1\tx", "application/json"),                                 # r31 t-num-tab-x
    ("true.", "application/json"),                                # r31 t-true-dot
])
def test_secret_location_json_grammar(secret_target, value, content_type):
    t = secret_target
    s = t.secret("grammar", SecretString=value)
    assert t.served(s["arn"], s["v1"]) == (value.encode("utf-8"), content_type)


@pytest.mark.parametrize("suffix,found", [
    ("abcdef", False), ("ab_cde", False),            # r30 Q-abcdef, Q-ab_cde: read as a random suffix
    ("abcdefg", True), ("abcde", True), ("ab-def", True),   # r30 Q-abcdefg, Q-abcde, Q-ab-def
])
def test_secret_profile_partial_arn_of_a_name_ending_in_six_characters(secret_target, suffix, found):
    t = secret_target
    s = t.secret(f"sfx-{suffix}", SecretString="{}")
    partial = s["arn"].rsplit("-", 1)[0]
    if found:
        t.profile(partial)
    else:
        assert _profile_error(t, partial) == (404, "ResourceNotFoundException", f"Secret {partial} not found")
    t.profile("secretsmanager://" + s["name"])       # by name it is always found


def test_secret_profile_partial_arn_must_not_be_a_mere_prefix(secret_target):
    """r29 P1-partial-exact (a name ending in "-secret" is read as suffixed), P2-partial-prefix-only"""
    t = secret_target
    s = t.secret("prefixed-secret", SecretString="{}")
    exact = s["arn"].rsplit("-", 1)[0]
    for uri in (exact, exact[: -len("secret")]):
        assert _profile_error(t, uri) == (404, "ResourceNotFoundException", f"Secret {uri} not found")


@pytest.mark.parametrize("partition,supported", [
    ("aws-us-gov", True), ("aws-iso", True), ("aws-iso-b", True), ("aws-iso-f", True), ("aws-eusc", True),
    ("aws-cn", True), ("aws-x", False), ("AWS", False), ("foo", False),   # r30 R-*; r29 P3
])
def test_secret_profile_arn_partition(secret_target, partition, supported):
    t = secret_target
    uri = f"arn:{partition}:secretsmanager:us-east-1:{get_account_id()}:secret:{t.name}-AbCdEf"
    if supported:
        assert _profile_error(t, uri) == (404, "ResourceNotFoundException", f"Secret {uri} not found")
    else:
        assert _profile_error(t, uri) == (400, "BadRequestException", f"The Uri: {uri} is of an unsupported type")


@pytest.mark.parametrize("name", ["has space", "a:b"])   # r29 P4-arn-name-space, P5-arn-name-colon
def test_secret_profile_arn_name_must_be_a_valid_name(secret_target, name):
    t = secret_target
    status, code, message = _profile_error(t, f"arn:aws:secretsmanager:us-east-1:{get_account_id()}:secret:{name}")
    assert (status, code) == (400, "BadRequestException") and re.fullmatch(_INVALID_NAME, message)


def test_type_error_comes_before_the_application_in_start_deployment(secret_target):
    """r29 T16-type-error-missing-app"""
    raw = _raw_appconfig()
    with pytest.raises(ClientError) as exc:
        raw.start_deployment(ApplicationId="zzzzzzz", EnvironmentId="zzzzzzz", ConfigurationProfileId="zzzzzzz",
                             DeploymentStrategyId=secret_target.now, ConfigurationVersion=5)
    assert exc.value.response["Error"]["Message"] == "NUMBER_VALUE can not be converted to a String"


@pytest.mark.parametrize("value,content_type", [
    ("trueé", "application/octet-stream"),        # r32 l-true-eacute
    ("null€", "application/octet-stream"),        # r32 l-null-euro
    ("false\u007f", "application/octet-stream"),       # r32 l-false-del
    ("true١", "application/octet-stream"),        # r32 l-true-arabic-digit
    ("true‍", "application/octet-stream"),        # r32 l-true-zwj
    ("true\u0085", "application/octet-stream"),        # r32 l-true-c1
    ("true ", "application/json"),                # r32 l-true-nbsp
    ("true\U0001F600", "application/json"),            # r32 l-true-emoji
])
def test_secret_location_literal_followed_by_non_ascii(secret_target, value, content_type):
    t = secret_target
    s = t.secret("lit", SecretString=value)
    assert t.served(s["arn"], s["v1"]) == (value.encode("utf-8"), content_type)


@pytest.mark.parametrize("segment", ["ab/cde", "ab+cde", "ab.cde"])   # r32 Q-sfx-*-partial
def test_secret_profile_six_character_rule_counts_word_characters_only(secret_target, segment):
    t = secret_target
    s = t.secret(f"sfx-{segment}", SecretString="{}")
    t.profile(s["arn"].rsplit("-", 1)[0])


@pytest.mark.parametrize("partition,supported", [
    ("aws-iso-e", True), ("aws-iso-x", True), ("aws-iso-ab", False), ("aws-iso-", False),   # r32 R-*
])
def test_secret_profile_aws_iso_partitions(secret_target, partition, supported):
    t = secret_target
    uri = f"arn:{partition}:secretsmanager:us-east-1:{get_account_id()}:secret:{t.name}-AbCdEf"
    status, code, message = _profile_error(t, uri)
    if supported:
        assert (status, code) == (404, "ResourceNotFoundException")
    else:
        assert (status, code, message) == (400, "BadRequestException", f"The Uri: {uri} is of an unsupported type")


@pytest.mark.auth_contract
@pytest.mark.parametrize("label", ["nopolicy", "shared"])
def test_cross_account_secret_needs_its_resource_policy_under_auth(secret_target, account_b_secrets,
                                                                   auth_enabled, label):
    """Under AUTH=true another account's secret is read only when its resource policy lets the retrieval
    role call GetSecretValue (r12 X4); with AUTH off MiniStack does not evaluate the policy."""
    t = secret_target
    arn = account_b_secrets[label]["arn"]
    if auth_enabled and label == "nopolicy":
        with pytest.raises(ClientError) as exc:
            t.profile(arn)
        assert exc.value.response["Error"]["Code"] == "BadRequestException"
        assert re.fullmatch(_access_denied(t, arn), exc.value.response["Error"]["Message"])
    else:
        assert t.profile(arn)


def test_kms_key_version_description_and_deployment_tags_round_trip(appconfig_client, kms_client):
    """#2449: KmsKeyIdentifier (with its KmsKeyArn), a hosted version's Description and a deployment's Tags
    are stored and read back, so Terraform does not re-plan them."""
    ac = appconfig_client
    key = kms_client.create_key()["KeyMetadata"]
    alias = f"alias/appconfig-{os.urandom(3).hex()}"
    kms_client.create_alias(AliasName=alias, TargetKeyId=key["KeyId"])
    app = ac.create_application(Name=f"kms-{os.urandom(3).hex()}")["Id"]
    try:
        env = ac.create_environment(ApplicationId=app, Name="e")["Id"]
        profile = ac.create_configuration_profile(ApplicationId=app, Name="p", LocationUri="hosted",
                                                  KmsKeyIdentifier=alias)
        # #2483: KmsKeyArn is derived from the identifier as given, so an alias stays an alias ARN.
        alias_arn = key["Arn"].split(":key/")[0] + ":" + alias
        assert (profile["KmsKeyIdentifier"], profile["KmsKeyArn"]) == (alias, alias_arn)
        got = ac.get_configuration_profile(ApplicationId=app, ConfigurationProfileId=profile["Id"])
        assert (got["KmsKeyIdentifier"], got["KmsKeyArn"]) == (alias, alias_arn)
        updated = ac.update_configuration_profile(ApplicationId=app, ConfigurationProfileId=profile["Id"],
                                                  KmsKeyIdentifier=key["KeyId"])
        assert (updated["KmsKeyIdentifier"], updated["KmsKeyArn"]) == (key["KeyId"], key["Arn"])

        version = ac.create_hosted_configuration_version(
            ApplicationId=app, ConfigurationProfileId=profile["Id"], Content=b"{}",
            ContentType="application/json", Description="my version")
        assert version["Description"] == "my version"
        assert ac.get_hosted_configuration_version(
            ApplicationId=app, ConfigurationProfileId=profile["Id"],
            VersionNumber=version["VersionNumber"])["Description"] == "my version"
        assert ac.list_hosted_configuration_versions(
            ApplicationId=app, ConfigurationProfileId=profile["Id"])["Items"][0]["Description"] == "my version"

        deployment = ac.start_deployment(
            ApplicationId=app, EnvironmentId=env, ConfigurationProfileId=profile["Id"],
            DeploymentStrategyId="AppConfig.AllAtOnce", ConfigurationVersion=str(version["VersionNumber"]),
            KmsKeyIdentifier=key["Arn"], Tags={"Name": "x"})
        assert (deployment["KmsKeyIdentifier"], deployment["KmsKeyArn"]) == (key["Arn"], key["Arn"])
        got = ac.get_deployment(ApplicationId=app, EnvironmentId=env, DeploymentNumber=deployment["DeploymentNumber"])
        assert got["KmsKeyArn"] == key["Arn"]
        region, account = key["Arn"].split(":")[3:5]
        arn = (f"arn:aws:appconfig:{region}:{account}:application/{app}/environment/{env}"
               f"/deployment/{deployment['DeploymentNumber']}")
        assert ac.list_tags_for_resource(ResourceArn=arn)["Tags"] == {"Name": "x"}
    finally:
        _delete_application(ac, app)



def test_kms_key_arn_is_derived_from_the_identifier_as_given(appconfig_client, kms_client):
    """#2483, captured on AWS: KmsKeyArn is the identifier as an ARN (alias ARNs stay aliases, anything else is
    key/<value>), with no lookup, so a key that does not exist is accepted at CreateConfigurationProfile."""
    ac = appconfig_client
    key = kms_client.create_key()["KeyMetadata"]
    prefix = key["Arn"].split(":key/")[0]
    app = ac.create_application(Name=f"kmsarn-{os.urandom(3).hex()}")["Id"]
    try:
        for identifier, arn in ((key["Arn"], key["Arn"]), (key["KeyId"], key["Arn"]),
                                ("alias/my-key", f"{prefix}:alias/my-key"),
                                (f"{prefix}:alias/my-key", f"{prefix}:alias/my-key"),
                                ("alias/does-not-exist", f"{prefix}:alias/does-not-exist"),
                                ("not-a-key", f"{prefix}:key/not-a-key")):
            profile = ac.create_configuration_profile(ApplicationId=app, Name=f"p-{os.urandom(3).hex()}",
                                                      LocationUri="hosted", KmsKeyIdentifier=identifier)
            assert profile["KmsKeyArn"] == arn, identifier
    finally:
        _delete_application(ac, app)


def test_hosted_version_carries_the_profile_key_and_deployment_checks_it(appconfig_client, kms_client):
    """#2483, captured on AWS: a hosted version under a profile with a key carries the profile's KmsKeyArn
    (headers and list), and StartDeployment of it needs a KmsKeyIdentifier that KMS holds."""
    ac = appconfig_client
    key = kms_client.create_key()["KeyMetadata"]
    app = ac.create_application(Name=f"kmsver-{os.urandom(3).hex()}")["Id"]
    try:
        env = ac.create_environment(ApplicationId=app, Name="e")["Id"]
        profile = ac.create_configuration_profile(ApplicationId=app, Name="p", LocationUri="hosted")["Id"]
        ac.update_configuration_profile(ApplicationId=app, ConfigurationProfileId=profile,
                                        KmsKeyIdentifier=key["Arn"])
        version = ac.create_hosted_configuration_version(ApplicationId=app, ConfigurationProfileId=profile,
                                                         Content=b"{}", ContentType="application/json")
        assert version["KmsKeyArn"] == key["Arn"]
        assert ac.get_hosted_configuration_version(ApplicationId=app, ConfigurationProfileId=profile,
                                                   VersionNumber=version["VersionNumber"])["KmsKeyArn"] == key["Arn"]
        listed = ac.list_hosted_configuration_versions(ApplicationId=app, ConfigurationProfileId=profile)["Items"]
        assert listed[0]["KmsKeyArn"] == key["Arn"]

        deploy = dict(ApplicationId=app, EnvironmentId=env, ConfigurationProfileId=profile,
                      DeploymentStrategyId="AppConfig.AllAtOnce", ConfigurationVersion=str(version["VersionNumber"]))
        with pytest.raises(ClientError) as exc:
            ac.start_deployment(**deploy)
        assert exc.value.response["Error"]["Message"] == (
            "KmsKeyIdentifier required when deploying a hosted configuration version that is encrypted with a "
            "customer managed key (CMK)")
        missing = key["Arn"].split(":key/")[0] + ":key/00000000-0000-0000-0000-000000000000"
        with pytest.raises(ClientError) as exc:
            ac.start_deployment(**deploy, KmsKeyIdentifier=missing)
        assert exc.value.response["Error"]["Code"] == "BadRequestException"
        assert re.fullmatch(
            re.escape(f"Key '{missing}' does not exist (Service: AWSKMS; Status Code: 400; Error Code: "
                      "NotFoundException; Request ID: ") + r"[0-9a-f-]{36}; Proxy: null\)",
            exc.value.response["Error"]["Message"])
        other = key["Arn"].replace(key["Arn"].split(":")[4], "111111111111")
        with pytest.raises(ClientError) as exc:
            ac.start_deployment(**deploy, KmsKeyIdentifier=other)
        assert re.fullmatch(
            r"User: arn:aws:iam::\d{12}:\S+ is not authorized to perform: kms:GenerateDataKey on this resource "
            r"because the resource does not exist in this Region, no resource-based policies allow access, or a "
            r"resource-based policy explicitly denies access \(Service: AWSKMS; Status Code: 400; Error Code: "
            r"AccessDeniedException; Request ID: [0-9a-f-]{36}; Proxy: null\)",
            exc.value.response["Error"]["Message"])
        kms_client.disable_key(KeyId=key["KeyId"])
        with pytest.raises(ClientError) as exc:
            ac.start_deployment(**deploy, KmsKeyIdentifier=key["Arn"])
        assert re.fullmatch(
            re.escape(f"{key['Arn']} is disabled. (Service: AWSKMS; Status Code: 400; Error Code: "
                      "DisabledException; Request ID: ") + r"[0-9a-f-]{36}; Proxy: null\)",
            exc.value.response["Error"]["Message"])
        kms_client.enable_key(KeyId=key["KeyId"])
        assert ac.start_deployment(**deploy, KmsKeyIdentifier=key["Arn"])["KmsKeyArn"] == key["Arn"]
    finally:
        _delete_application(ac, app)


def test_version_created_before_the_key_is_not_encrypted(appconfig_client, kms_client):
    """Captured on AWS: a version created before its profile got a key has no KmsKeyArn, and deploys
    without a KmsKeyIdentifier."""
    ac = appconfig_client
    key = kms_client.create_key()["KeyMetadata"]
    app = ac.create_application(Name=f"kmsold-{os.urandom(3).hex()}")["Id"]
    try:
        env = ac.create_environment(ApplicationId=app, Name="e")["Id"]
        profile = ac.create_configuration_profile(ApplicationId=app, Name="p", LocationUri="hosted")["Id"]
        version = ac.create_hosted_configuration_version(ApplicationId=app, ConfigurationProfileId=profile,
                                                         Content=b"{}", ContentType="application/json")
        ac.update_configuration_profile(ApplicationId=app, ConfigurationProfileId=profile, KmsKeyIdentifier=key["Arn"])
        got = ac.get_hosted_configuration_version(ApplicationId=app, ConfigurationProfileId=profile,
                                                  VersionNumber=version["VersionNumber"])
        assert "KmsKeyArn" not in got
        assert ac.start_deployment(ApplicationId=app, EnvironmentId=env, ConfigurationProfileId=profile,
                                   DeploymentStrategyId="AppConfig.AllAtOnce",
                                   ConfigurationVersion=str(version["VersionNumber"]))["DeploymentNumber"] == 1
    finally:
        _delete_application(ac, app)


def test_list_deployments_newest_first_with_profile_and_type(appconfig_client):
    """#2484, captured on AWS: ListDeployments lists newest first, each with ConfigurationProfileId and Type USER."""
    ac = appconfig_client
    app = ac.create_application(Name=f"listdep-{os.urandom(3).hex()}")["Id"]
    try:
        env = ac.create_environment(ApplicationId=app, Name="e")["Id"]
        profile = ac.create_configuration_profile(ApplicationId=app, Name="p", LocationUri="hosted")["Id"]
        ac.create_hosted_configuration_version(ApplicationId=app, ConfigurationProfileId=profile,
                                               Content=b"{}", ContentType="application/json")
        strategy = ac.create_deployment_strategy(Name=f"now-{os.urandom(3).hex()}", DeploymentDurationInMinutes=0,
                                                 FinalBakeTimeInMinutes=0, GrowthFactor=100, GrowthType="LINEAR",
                                                 ReplicateTo="NONE")["Id"]
        for _ in range(2):
            ac.start_deployment(ApplicationId=app, EnvironmentId=env, ConfigurationProfileId=profile,
                                DeploymentStrategyId=strategy, ConfigurationVersion="1")
        items = ac.list_deployments(ApplicationId=app, EnvironmentId=env)["Items"]
        assert [i["DeploymentNumber"] for i in items] == [2, 1]
        assert all((i["ConfigurationProfileId"], i["Type"]) == (profile, "USER") for i in items)
    finally:
        _delete_application(ac, app)


def test_list_tags_for_an_arn_without_this_account(appconfig_client):
    """#2484, captured on AWS: an AppConfig ARN with no account answers 400, another account's answers 403."""
    ac = appconfig_client
    app = ac.create_application(Name=f"tagarn-{os.urandom(3).hex()}")["Id"]
    try:
        region = ac.meta.region_name
        caller = boto3.client("sts", endpoint_url=ENDPOINT, region_name=region, aws_access_key_id="test",
                              aws_secret_access_key="test").get_caller_identity()["Account"]
        for arn in (f"arn:aws:appconfig:{region}::application/{app}",
                    f"arn:aws:appconfig:{region}::deploymentstrategy/AppConfig.AllAtOnce"):
            with pytest.raises(ClientError) as exc:
                ac.list_tags_for_resource(ResourceArn=arn)
            err = exc.value.response
            assert (err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"], err["Error"]["Message"]) == (
                400, "BadRequestException", f"ARN {arn} is invalid due to not containing an account ID.")
        other = "444455556666" if caller != "444455556666" else "111122223333"
        arn = f"arn:aws:appconfig:{region}:{other}:application/{app}"
        with pytest.raises(ClientError) as exc:
            ac.list_tags_for_resource(ResourceArn=arn)
        err = exc.value.response
        assert (err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"], err["Error"]["Message"]) == (
            403, "AccessDeniedException",
            f"Cross-account resources are not permitted for this API. ARN {arn} references a resource outside "
            f"of account {caller}.")
    finally:
        _delete_application(ac, app)
