import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


def _emr_client(region):
    return boto3.client(
        "emr",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


def _run_regional_cluster(client, name):
    return client.run_job_flow(
        Name=name,
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )


def test_emr_state_and_block_public_access_are_region_scoped():
    east = _emr_client("us-east-1")
    west = _emr_client("us-west-2")
    east_cluster = _run_regional_cluster(east, "same-name-regional-cluster")
    west_cluster = _run_regional_cluster(west, "same-name-regional-cluster")
    east_step = east.add_job_flow_steps(
        JobFlowId=east_cluster["JobFlowId"],
        Steps=[{"Name": "east-step", "HadoopJarStep": {"Jar": "command-runner.jar"}}],
    )["StepIds"][0]
    west_step = west.add_job_flow_steps(
        JobFlowId=west_cluster["JobFlowId"],
        Steps=[{"Name": "west-step", "HadoopJarStep": {"Jar": "command-runner.jar"}}],
    )["StepIds"][0]

    try:
        east_ids = {cluster["Id"] for cluster in east.list_clusters()["Clusters"]}
        west_ids = {cluster["Id"] for cluster in west.list_clusters()["Clusters"]}
        assert east_cluster["JobFlowId"] in east_ids
        assert east_cluster["JobFlowId"] not in west_ids
        assert west_cluster["JobFlowId"] in west_ids
        assert west_cluster["JobFlowId"] not in east_ids
        assert ":us-east-1:" in east_cluster["ClusterArn"]
        assert ":us-west-2:" in west_cluster["ClusterArn"]
        assert east.list_steps(ClusterId=east_cluster["JobFlowId"])["Steps"][0]["Id"] == east_step
        assert west.list_steps(ClusterId=west_cluster["JobFlowId"])["Steps"][0]["Id"] == west_step

        assert east.get_block_public_access_configuration()["BlockPublicAccessConfiguration"] == {
            "BlockPublicSecurityGroupRules": False,
            "PermittedPublicSecurityGroupRuleRanges": [],
        }
        assert west.get_block_public_access_configuration()["BlockPublicAccessConfiguration"] == {
            "BlockPublicSecurityGroupRules": False,
            "PermittedPublicSecurityGroupRuleRanges": [],
        }
        east.put_block_public_access_configuration(
            BlockPublicAccessConfiguration={
                "BlockPublicSecurityGroupRules": True,
                "PermittedPublicSecurityGroupRuleRanges": [{"MinRange": 22, "MaxRange": 22}],
            }
        )
        assert east.get_block_public_access_configuration()["BlockPublicAccessConfiguration"][
            "BlockPublicSecurityGroupRules"
        ] is True
        assert west.get_block_public_access_configuration()["BlockPublicAccessConfiguration"] == {
            "BlockPublicSecurityGroupRules": False,
            "PermittedPublicSecurityGroupRuleRanges": [],
        }
    finally:
        east.put_block_public_access_configuration(
            BlockPublicAccessConfiguration={"BlockPublicSecurityGroupRules": False}
        )
        west.put_block_public_access_configuration(
            BlockPublicAccessConfiguration={"BlockPublicSecurityGroupRules": False}
        )
        east.terminate_job_flows(JobFlowIds=[east_cluster["JobFlowId"]])
        west.terminate_job_flows(JobFlowIds=[west_cluster["JobFlowId"]])


def test_emr_legacy_state_restores_children_beside_parent():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import emr as service

    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    boot_region = "us-east-1"
    cluster_region = "us-west-2"
    cluster_id = "j-LEGACYCLUSTER"
    orphan_id = "j-ORPHANCLUSTER"
    clusters = AccountScopedDict()
    steps = AccountScopedDict()

    set_request_account_id(account_id)
    set_request_region(boot_region)
    clusters[cluster_id] = {
        "Id": cluster_id,
        "ClusterArn": f"arn:aws:elasticmapreduce:{cluster_region}:{account_id}:cluster/{cluster_id}",
    }
    steps[cluster_id] = [{"Id": "s-LEGACY", "Name": "legacy-step"}]
    steps[orphan_id] = [
        {
            "Id": "s-ORPHAN",
            "Name": "orphan-step",
            "Config": {
                "Args": [
                    f"arn:aws:lambda:{cluster_region}:{account_id}:function:incidental"
                ]
            },
        }
    ]

    service.reset()
    try:
        service.restore_state(
            {
                "_clusters": clusters,
                "_steps": steps,
                "_block_public_access": {
                    "BlockPublicSecurityGroupRules": True,
                    "PermittedPublicSecurityGroupRuleRanges": [{"MinRange": 22, "MaxRange": 22}],
                },
            }
        )
        assert service._clusters.get_scoped(account_id, cluster_region, cluster_id)["Id"] == cluster_id
        assert service._steps.get_scoped(account_id, cluster_region, cluster_id)[0]["Id"] == "s-LEGACY"
        assert service._steps.get_scoped(account_id, boot_region, orphan_id)[0]["Id"] == "s-ORPHAN"
        assert service._block_public_access.get_scoped(
            account_id, boot_region, service._BLOCK_PUBLIC_ACCESS_KEY
        )["BlockPublicSecurityGroupRules"] is True
        assert service._block_public_access.get_scoped(
            account_id, cluster_region, service._BLOCK_PUBLIC_ACCESS_KEY
        ) is None
    finally:
        service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_emr_reset_clears_state_across_regions():
    from ministack.core.responses import get_region, set_request_region
    from ministack.services import emr as service

    original_region = get_region()
    stores = (service._clusters, service._steps, service._block_public_access)
    service.reset()
    try:
        for region in ("us-east-1", "us-west-2"):
            set_request_region(region)
            for store in stores:
                store[f"resource-{region}"] = {"region": region}
        service.reset()
        assert all(not store.has_any() for store in stores)
    finally:
        service.reset()
        set_request_region(original_region)


def test_emr_run_job_flow_simple(emr):
    resp = emr.run_job_flow(
        Name="test-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "SlaveInstanceType": "m5.xlarge",
            "InstanceCount": 3,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    assert resp["JobFlowId"].startswith("j-")
    assert "ClusterArn" in resp
    assert "elasticmapreduce" in resp["ClusterArn"]

def test_emr_describe_cluster(emr):
    jf = emr.run_job_flow(
        Name="describe-test",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    desc = emr.describe_cluster(ClusterId=cluster_id)
    cluster = desc["Cluster"]
    assert cluster["Id"] == cluster_id
    assert cluster["Name"] == "describe-test"
    assert cluster["Status"]["State"] == "WAITING"
    assert cluster["ReleaseLabel"] == "emr-6.10.0"

def test_emr_list_clusters(emr):
    emr.run_job_flow(
        Name="list-test",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    resp = emr.list_clusters()
    assert len(resp["Clusters"]) >= 1
    assert all("Id" in c for c in resp["Clusters"])

def test_emr_terminate_job_flows(emr):
    jf = emr.run_job_flow(
        Name="terminate-test",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    emr.terminate_job_flows(JobFlowIds=[cluster_id])
    desc = emr.describe_cluster(ClusterId=cluster_id)
    assert desc["Cluster"]["Status"]["State"] == "TERMINATED"

def test_emr_termination_protection(emr):
    jf = emr.run_job_flow(
        Name="protected-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    import botocore.exceptions

    try:
        emr.terminate_job_flows(JobFlowIds=[cluster_id])
        assert False, "should have raised"
    except botocore.exceptions.ClientError as e:
        assert "ValidationException" in str(e) or "protected" in str(e).lower()

def test_emr_add_and_list_steps(emr):
    jf = emr.run_job_flow(
        Name="steps-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    step_resp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "my-spark-step",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--class",
                        "com.example.Main",
                        "s3://bucket/app.jar",
                    ],
                },
            }
        ],
    )
    assert len(step_resp["StepIds"]) == 1
    step_id = step_resp["StepIds"][0]
    assert step_id.startswith("s-")

    steps = emr.list_steps(ClusterId=cluster_id)
    assert any(s["Id"] == step_id for s in steps["Steps"])

def test_emr_describe_step(emr):
    jf = emr.run_job_flow(
        Name="describe-step-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    step_resp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "step1",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": []},
            }
        ],
    )
    step_id = step_resp["StepIds"][0]
    desc = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
    assert desc["Step"]["Id"] == step_id
    assert desc["Step"]["Status"]["State"] == "COMPLETED"

def test_emr_tags(emr):
    jf = emr.run_job_flow(
        Name="tagged-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        Tags=[{"Key": "env", "Value": "test"}],
    )
    cluster_id = jf["JobFlowId"]
    emr.add_tags(ResourceId=cluster_id, Tags=[{"Key": "team", "Value": "data"}])
    desc = emr.describe_cluster(ClusterId=cluster_id)
    tag_map = {t["Key"]: t["Value"] for t in desc["Cluster"]["Tags"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "data"

    emr.remove_tags(ResourceId=cluster_id, TagKeys=["env"])
    desc2 = emr.describe_cluster(ClusterId=cluster_id)
    tag_keys = [t["Key"] for t in desc2["Cluster"]["Tags"]]
    assert "env" not in tag_keys
    assert "team" in tag_keys

def test_emr_auto_terminate_state(emr):
    """Cluster with KeepJobFlowAliveWhenNoSteps=False starts as TERMINATED."""
    jf = emr.run_job_flow(
        Name="auto-terminate-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": False,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    desc = emr.describe_cluster(ClusterId=cluster_id)
    assert desc["Cluster"]["Status"]["State"] == "TERMINATED"
    assert desc["Cluster"]["AutoTerminate"] is True

def test_emr_modify_cluster(emr):
    jf = emr.run_job_flow(
        Name="modify-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    resp = emr.modify_cluster(ClusterId=cluster_id, StepConcurrencyLevel=5)
    assert resp["StepConcurrencyLevel"] == 5

def test_emr_block_public_access(emr):
    resp = emr.get_block_public_access_configuration()
    assert "BlockPublicAccessConfiguration" in resp
    assert resp["BlockPublicAccessConfiguration"]["BlockPublicSecurityGroupRules"] is False

    emr.put_block_public_access_configuration(
        BlockPublicAccessConfiguration={
            "BlockPublicSecurityGroupRules": True,
            "PermittedPublicSecurityGroupRuleRanges": [{"MinRange": 22, "MaxRange": 22}],
        }
    )
    resp2 = emr.get_block_public_access_configuration()
    assert resp2["BlockPublicAccessConfiguration"]["BlockPublicSecurityGroupRules"] is True

def test_emr_instance_groups(emr):
    jf = emr.run_job_flow(
        Name="ig-cluster",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]
    groups = emr.list_instance_groups(ClusterId=cluster_id)
    assert len(groups["InstanceGroups"]) >= 2

    new_group_resp = emr.add_instance_groups(
        JobFlowId=cluster_id,
        InstanceGroups=[
            {
                "Name": "Task",
                "InstanceRole": "TASK",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            }
        ],
    )
    assert len(new_group_resp["InstanceGroupIds"]) == 1
    groups2 = emr.list_instance_groups(ClusterId=cluster_id)
    assert len(groups2["InstanceGroups"]) == 3

def test_emr_instance_fleets(emr):
    """AddInstanceFleet / ListInstanceFleets / ModifyInstanceFleet."""
    resp = emr.run_job_flow(
        Name="fleet-test-v44",
        ReleaseLabel="emr-6.15.0",
        Instances={
            "KeepJobFlowAliveWhenNoSteps": True,
            "InstanceFleets": [
                {"InstanceFleetType": "MASTER", "Name": "master-fleet",
                 "TargetOnDemandCapacity": 1,
                 "InstanceTypeConfigs": [{"InstanceType": "m5.xlarge"}]},
            ],
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = resp["JobFlowId"]

    # Add a CORE fleet
    add_resp = emr.add_instance_fleet(
        ClusterId=cluster_id,
        InstanceFleet={
            "InstanceFleetType": "CORE", "Name": "core-fleet",
            "TargetOnDemandCapacity": 2,
            "InstanceTypeConfigs": [{"InstanceType": "m5.xlarge"}],
        },
    )
    fleet_id = add_resp["InstanceFleetId"]
    assert fleet_id

    # List fleets
    fleets = emr.list_instance_fleets(ClusterId=cluster_id)
    fleet_types = [f["InstanceFleetType"] for f in fleets["InstanceFleets"]]
    assert "MASTER" in fleet_types
    assert "CORE" in fleet_types

    emr.terminate_job_flows(JobFlowIds=[cluster_id])


def test_emr_set_visible_to_all_users(emr):
    """SetVisibleToAllUsers toggles visibility on and off."""
    jf = emr.run_job_flow(
        Name="visible-test",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]

    # Default is visible
    desc = emr.describe_cluster(ClusterId=cluster_id)
    assert desc["Cluster"]["VisibleToAllUsers"] is True

    # Set to False
    emr.set_visible_to_all_users(JobFlowIds=[cluster_id], VisibleToAllUsers=False)
    desc = emr.describe_cluster(ClusterId=cluster_id)
    assert desc["Cluster"]["VisibleToAllUsers"] is False

    # Set back to True
    emr.set_visible_to_all_users(JobFlowIds=[cluster_id], VisibleToAllUsers=True)
    desc = emr.describe_cluster(ClusterId=cluster_id)
    assert desc["Cluster"]["VisibleToAllUsers"] is True

    emr.terminate_job_flows(JobFlowIds=[cluster_id])


def test_emr_cancel_steps(emr):
    """CancelSteps returns info list for each requested step."""
    jf = emr.run_job_flow(
        Name="cancel-steps-test",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]

    step_resp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "cancel-me",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "hi"]},
            },
            {
                "Name": "cancel-me-too",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["echo", "bye"]},
            },
        ],
    )
    step_ids = step_resp["StepIds"]
    assert len(step_ids) == 2

    # Steps are already COMPLETED in ministack, so cancel returns FAILED_TO_CANCEL
    cancel_resp = emr.cancel_steps(ClusterId=cluster_id, StepIds=step_ids)
    info_list = cancel_resp["CancelStepsInfoList"]
    assert len(info_list) == 2
    for info in info_list:
        assert info["StepId"] in step_ids
        assert info["Status"] == "FAILED_TO_CANCEL"
        assert "Reason" in info

    emr.terminate_job_flows(JobFlowIds=[cluster_id])


def test_emr_modify_instance_fleet(emr):
    """ModifyInstanceFleet updates on-demand/spot capacity."""
    jf = emr.run_job_flow(
        Name="modify-fleet-test",
        ReleaseLabel="emr-6.15.0",
        Instances={
            "KeepJobFlowAliveWhenNoSteps": True,
            "InstanceFleets": [
                {
                    "InstanceFleetType": "MASTER",
                    "Name": "master-fleet",
                    "TargetOnDemandCapacity": 1,
                    "InstanceTypeConfigs": [{"InstanceType": "m5.xlarge"}],
                },
            ],
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]

    # Add a CORE fleet to modify
    add_resp = emr.add_instance_fleet(
        ClusterId=cluster_id,
        InstanceFleet={
            "InstanceFleetType": "CORE",
            "Name": "core-fleet",
            "TargetOnDemandCapacity": 2,
            "InstanceTypeConfigs": [{"InstanceType": "m5.xlarge"}],
        },
    )
    fleet_id = add_resp["InstanceFleetId"]

    # Modify capacity
    emr.modify_instance_fleet(
        ClusterId=cluster_id,
        InstanceFleet={
            "InstanceFleetId": fleet_id,
            "TargetOnDemandCapacity": 5,
            "TargetSpotCapacity": 3,
        },
    )

    # Verify the modification
    fleets = emr.list_instance_fleets(ClusterId=cluster_id)
    core_fleet = [f for f in fleets["InstanceFleets"] if f["Id"] == fleet_id][0]
    assert core_fleet["TargetOnDemandCapacity"] == 5
    assert core_fleet["TargetSpotCapacity"] == 3
    assert core_fleet["ProvisionedOnDemandCapacity"] == 5
    assert core_fleet["ProvisionedSpotCapacity"] == 3

    emr.terminate_job_flows(JobFlowIds=[cluster_id])


def test_emr_modify_instance_groups(emr):
    """ModifyInstanceGroups updates instance counts."""
    jf = emr.run_job_flow(
        Name="modify-groups-test",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
    )
    cluster_id = jf["JobFlowId"]

    # Find the CORE group id
    groups = emr.list_instance_groups(ClusterId=cluster_id)
    core_group = [g for g in groups["InstanceGroups"] if g["InstanceGroupType"] == "CORE"][0]
    group_id = core_group["Id"]
    assert core_group["RequestedInstanceCount"] == 2

    # Modify the group count
    emr.modify_instance_groups(
        ClusterId=cluster_id,
        InstanceGroups=[{"InstanceGroupId": group_id, "InstanceCount": 6}],
    )

    # Verify the modification
    groups2 = emr.list_instance_groups(ClusterId=cluster_id)
    core_group2 = [g for g in groups2["InstanceGroups"] if g["Id"] == group_id][0]
    assert core_group2["RequestedInstanceCount"] == 6
    assert core_group2["RunningInstanceCount"] == 6

    emr.terminate_job_flows(JobFlowIds=[cluster_id])


def test_emr_list_bootstrap_actions(emr):
    """ListBootstrapActions returns actions created with the cluster."""
    jf = emr.run_job_flow(
        Name="bootstrap-test",
        ReleaseLabel="emr-6.10.0",
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "InstanceCount": 1,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        BootstrapActions=[
            {
                "Name": "install-deps",
                "ScriptBootstrapAction": {
                    "Path": "s3://my-bucket/bootstrap/install.sh",
                    "Args": ["--env", "prod"],
                },
            },
            {
                "Name": "setup-monitoring",
                "ScriptBootstrapAction": {
                    "Path": "s3://my-bucket/bootstrap/monitor.sh",
                    "Args": [],
                },
            },
        ],
    )
    cluster_id = jf["JobFlowId"]

    actions = emr.list_bootstrap_actions(ClusterId=cluster_id)
    ba_list = actions["BootstrapActions"]
    assert len(ba_list) == 2

    assert ba_list[0]["Name"] == "install-deps"
    assert ba_list[0]["ScriptPath"] == "s3://my-bucket/bootstrap/install.sh"
    assert ba_list[0]["Args"] == ["--env", "prod"]

    assert ba_list[1]["Name"] == "setup-monitoring"
    assert ba_list[1]["ScriptPath"] == "s3://my-bucket/bootstrap/monitor.sh"
    assert ba_list[1]["Args"] == []

    emr.terminate_job_flows(JobFlowIds=[cluster_id])
