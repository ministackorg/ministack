"""CloudFormation updates of AppConfig resources, in place and rolled back."""

import json
import uuid as _uuid_mod

import pytest
from botocore.exceptions import ClientError
from test_cfn import (
    _FAILING_RESOURCE,
    _cfn_output,
    _delete_cfn_test_stack,
    _wait_stack,
)


def test_cfn_appconfig_application_updates_in_place(cfn, appconfig_client):
    """Name, Description and Tags are No interruption on
    AWS::AppConfig::Application, and the type has no replacing property: the
    application id survives the update. Without the handler the create ran
    again and minted a second application."""
    # Unique per run, so a rerun or a parallel worker cannot collide.
    suffix = _uuid_mod.uuid4().hex[:8]
    name = f"cfn-ac-app-update-{suffix}"

    def template(description):
        return json.dumps({
            "Resources": {
                "App": {
                    "Type": "AWS::AppConfig::Application",
                    "Properties": {
                        "Name": name,
                        "Description": description,
                        "Tags": [{"Key": "stage", "Value": description}],
                    },
                },
            },
            "Outputs": {"AppId": {"Value": {"Ref": "App"}}},
        })

    try:
        cfn.create_stack(StackName=f"cfn-ac-app-{suffix}", TemplateBody=template("before"))
        _wait_stack(cfn, f"cfn-ac-app-{suffix}")
        app_id = _cfn_output(cfn, f"cfn-ac-app-{suffix}", "AppId")
        assert appconfig_client.get_application(ApplicationId=app_id)["Name"] == name

        cfn.update_stack(StackName=f"cfn-ac-app-{suffix}", TemplateBody=template("after"))
        assert _wait_stack(cfn, f"cfn-ac-app-{suffix}")["StackStatus"] == "UPDATE_COMPLETE"

        assert _cfn_output(cfn, f"cfn-ac-app-{suffix}", "AppId") == app_id, (
            "the update minted a second application")
        assert appconfig_client.get_application(ApplicationId=app_id)["Description"] == "after"

        arn = f"arn:aws:appconfig:us-east-1:000000000000:application/{app_id}"
        assert appconfig_client.list_tags_for_resource(
            ResourceArn=arn)["Tags"]["stage"] == "after"
    finally:
        _delete_cfn_test_stack(cfn, f"cfn-ac-app-{suffix}")


def test_cfn_appconfig_application_description_change_is_rolled_back(cfn, appconfig_client):
    """A description change is applied in place, so a later failure in the
    same update has to set it back. Measured on AWS 2026-09-21: the
    application reads its old description after UPDATE_ROLLBACK_COMPLETE,
    under the same id."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-ac-rb-{suffix}"

    def template(description, with_bad):
        resources = {"App": {"Type": "AWS::AppConfig::Application", "Properties": {
            "Name": f"cfn-ac-rb-{suffix}", "Description": description}}}
        if with_bad:
            resources["Bad"] = {**_FAILING_RESOURCE, "DependsOn": "App"}
        return json.dumps({"Resources": resources,
                           "Outputs": {"AppId": {"Value": {"Ref": "App"}}}})

    cfn.create_stack(StackName=stack_name, TemplateBody=template("before", False))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        app_id = next(o["OutputValue"] for o in
                      cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
                      if o["OutputKey"] == "AppId")

        cfn.update_stack(StackName=stack_name, TemplateBody=template("after", True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert appconfig_client.get_application(ApplicationId=app_id)["Description"] == "before"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_appconfig_environment_updates_in_place(cfn, appconfig_client):
    """Description, Monitors and Name are No interruption on
    AWS::AppConfig::Environment, so the environment id survives."""
    # Unique per run, so a rerun or a parallel worker cannot collide.
    suffix = _uuid_mod.uuid4().hex[:8]
    def template(description):
        return json.dumps({
            "Resources": {
                "App": {
                    "Type": "AWS::AppConfig::Application",
                    "Properties": {"Name": f"cfn-ac-env-app-{suffix}"},
                },
                "Env": {
                    "Type": "AWS::AppConfig::Environment",
                    "Properties": {
                        "ApplicationId": {"Ref": "App"},
                        "Name": f"cfn-ac-env-{suffix}",
                        "Description": description,
                    },
                },
            },
            "Outputs": {"AppId": {"Value": {"Ref": "App"}},
                        "EnvId": {"Value": {"Ref": "Env"}}},
        })

    try:
        cfn.create_stack(StackName=f"cfn-ac-env-{suffix}", TemplateBody=template("before"))
        _wait_stack(cfn, f"cfn-ac-env-{suffix}")
        app_id = _cfn_output(cfn, f"cfn-ac-env-{suffix}", "AppId")
        env_id = _cfn_output(cfn, f"cfn-ac-env-{suffix}", "EnvId")

        cfn.update_stack(StackName=f"cfn-ac-env-{suffix}", TemplateBody=template("after"))
        assert _wait_stack(cfn, f"cfn-ac-env-{suffix}")["StackStatus"] == "UPDATE_COMPLETE"

        envs = appconfig_client.list_environments(ApplicationId=app_id)["Items"]
        assert len(envs) == 1, "the update minted a second environment"
        assert envs[0]["Id"] == env_id
        assert envs[0]["Description"] == "after"
    finally:
        _delete_cfn_test_stack(cfn, f"cfn-ac-env-{suffix}")


def test_cfn_appconfig_profile_update_keeps_hosted_versions(cfn, appconfig_client):
    """A Description change on AWS::AppConfig::ConfigurationProfile is No
    interruption. Hosted configuration versions are keyed by application and
    profile id, so the create-fallback's fresh profile id orphaned every
    version the stack had written."""
    # Unique per run, so a rerun or a parallel worker cannot collide.
    suffix = _uuid_mod.uuid4().hex[:8]
    def template(description):
        return json.dumps({
            "Resources": {
                "App": {
                    "Type": "AWS::AppConfig::Application",
                    "Properties": {"Name": f"cfn-ac-prof-app-{suffix}"},
                },
                "Profile": {
                    "Type": "AWS::AppConfig::ConfigurationProfile",
                    "Properties": {
                        "ApplicationId": {"Ref": "App"},
                        "Name": f"cfn-ac-prof-{suffix}",
                        "LocationUri": "hosted",
                        "Description": description,
                    },
                },
                "HCV": {
                    "Type": "AWS::AppConfig::HostedConfigurationVersion",
                    "Properties": {
                        "ApplicationId": {"Ref": "App"},
                        "ConfigurationProfileId": {"Ref": "Profile"},
                        "ContentType": "application/json",
                        "Content": "{\"flag\": true}",
                    },
                },
            },
            "Outputs": {"AppId": {"Value": {"Ref": "App"}},
                        "ProfileId": {"Value": {"Ref": "Profile"}}},
        })

    try:
        cfn.create_stack(StackName=f"cfn-ac-prof-{suffix}", TemplateBody=template("before"))
        _wait_stack(cfn, f"cfn-ac-prof-{suffix}")
        app_id = _cfn_output(cfn, f"cfn-ac-prof-{suffix}", "AppId")
        profile_id = _cfn_output(cfn, f"cfn-ac-prof-{suffix}", "ProfileId")
        assert appconfig_client.list_hosted_configuration_versions(
            ApplicationId=app_id, ConfigurationProfileId=profile_id)["Items"]

        cfn.update_stack(StackName=f"cfn-ac-prof-{suffix}", TemplateBody=template("after"))
        assert _wait_stack(cfn, f"cfn-ac-prof-{suffix}")["StackStatus"] == "UPDATE_COMPLETE"

        profiles = appconfig_client.list_configuration_profiles(
            ApplicationId=app_id)["Items"]
        assert len(profiles) == 1, "the update minted a second profile"
        assert profiles[0]["Id"] == profile_id
        # Description is not a member of ConfigurationProfileSummary, so the
        # read that shows the change is GetConfigurationProfile.
        assert appconfig_client.get_configuration_profile(
            ApplicationId=app_id,
            ConfigurationProfileId=profile_id)["Description"] == "after"
        assert appconfig_client.list_hosted_configuration_versions(
            ApplicationId=app_id, ConfigurationProfileId=profile_id
        )["Items"], "the hosted configuration version was orphaned"
    finally:
        _delete_cfn_test_stack(cfn, f"cfn-ac-prof-{suffix}")


def test_cfn_appconfig_deployment_strategy_update_and_rename(cfn, appconfig_client):
    """GrowthFactor is No interruption on AWS::AppConfig::DeploymentStrategy
    and Name is Replacement. The name is not the physical id, so the rename is
    a plain replacement: a new strategy id, and the predecessor removed."""
    # Unique per run, so a rerun or a parallel worker cannot collide.
    suffix = _uuid_mod.uuid4().hex[:8]
    def template(name, growth):
        return json.dumps({
            "Resources": {
                "Strategy": {
                    "Type": "AWS::AppConfig::DeploymentStrategy",
                    "Properties": {
                        "Name": name,
                        "DeploymentDurationInMinutes": 1,
                        "GrowthFactor": growth,
                        "ReplicateTo": "NONE",
                    },
                },
            },
            "Outputs": {"StrategyId": {"Value": {"Ref": "Strategy"}}},
        })

    def strategy():
        # Through the stack's Ref: the list action pages at 50 without a
        # NextToken, so a name scan misses strategies on a busy server.
        strategy_id = _cfn_output(cfn, f"cfn-ac-strat-{suffix}", "StrategyId")
        return appconfig_client.get_deployment_strategy(DeploymentStrategyId=strategy_id)

    try:
        cfn.create_stack(StackName=f"cfn-ac-strat-{suffix}",
                         TemplateBody=template(f"cfn-ac-strat-a-{suffix}", 10))
        _wait_stack(cfn, f"cfn-ac-strat-{suffix}")
        created = strategy()
        assert created["Name"] == f"cfn-ac-strat-a-{suffix}"
        strategy_id = created["Id"]

        # In place: the growth factor changes under the same id.
        cfn.update_stack(StackName=f"cfn-ac-strat-{suffix}",
                         TemplateBody=template(f"cfn-ac-strat-a-{suffix}", 25))
        assert _wait_stack(cfn, f"cfn-ac-strat-{suffix}")["StackStatus"] == "UPDATE_COMPLETE"
        same = strategy()
        assert same["Id"] == strategy_id
        assert same["GrowthFactor"] == 25

        # Replacement: a new id, and the old strategy gone.
        cfn.update_stack(StackName=f"cfn-ac-strat-{suffix}",
                         TemplateBody=template(f"cfn-ac-strat-b-{suffix}", 25))
        assert _wait_stack(cfn, f"cfn-ac-strat-{suffix}")["StackStatus"] == "UPDATE_COMPLETE"
        renamed = strategy()
        assert renamed["Name"] == f"cfn-ac-strat-b-{suffix}"
        assert renamed["Id"] != strategy_id
        with pytest.raises(ClientError) as exc:
            appconfig_client.get_deployment_strategy(DeploymentStrategyId=strategy_id)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException", (
            "the predecessor survived")
    finally:
        _delete_cfn_test_stack(cfn, f"cfn-ac-strat-{suffix}")
