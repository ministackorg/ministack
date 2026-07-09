import json
import os

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from ministack.services import appconfig as appconfig_service

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


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
    resp = appconfig_client.stop_deployment(
        ApplicationId=app["Id"],
        EnvironmentId=env["Id"],
        DeploymentNumber=deploy["DeploymentNumber"],
    )
    assert resp["State"] == "ROLLED_BACK"


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
        for response in (
            appconfig_service._tag_resource(resource_arn, {"Tags": {"bad": "tag"}}),
            appconfig_service._untag_resource(resource_arn, ["seed"]),
            appconfig_service._list_tags_for_resource(resource_arn),
        ):
            status, body = _status_and_body(response)
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
        "http://localhost:4566/applications/nonexistent",
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
