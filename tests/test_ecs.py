import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

def test_ecs_cluster(ecs):
    ecs.create_cluster(clusterName="test-cluster")
    clusters = ecs.list_clusters()
    assert any("test-cluster" in arn for arn in clusters["clusterArns"])

def test_ecs_task_def(ecs):
    resp = ecs.register_task_definition(
        family="test-task",
        containerDefinitions=[
            {
                "name": "web",
                "image": "nginx:alpine",
                "cpu": 128,
                "memory": 256,
                "portMappings": [{"containerPort": 80, "hostPort": 8080}],
            }
        ],
        requiresCompatibilities=["EC2"],
        cpu="256",
        memory="512",
    )
    assert resp["taskDefinition"]["family"] == "test-task"
    assert resp["taskDefinition"]["revision"] == 1

def test_ecs_list_task_defs(ecs):
    resp = ecs.list_task_definitions(familyPrefix="test-task")
    assert len(resp["taskDefinitionArns"]) >= 1

def test_ecs_run_task_stops_after_exit(ecs):
    """DescribeTasks transitions to STOPPED after Docker container exits."""
    ecs.create_cluster(clusterName="task-lifecycle")
    ecs.register_task_definition(
        family="short-lived",
        containerDefinitions=[
            {
                "name": "worker",
                "image": "alpine:latest",
                "command": ["sh", "-c", "echo done"],
                "essential": True,
            }
        ],
    )
    resp = ecs.run_task(cluster="task-lifecycle", taskDefinition="short-lived")
    task_arn = resp["tasks"][0]["taskArn"]
    assert resp["tasks"][0]["lastStatus"] == "RUNNING"

    # Poll until STOPPED (container exits almost immediately)
    stopped = False
    for _ in range(30):
        time.sleep(2)
        desc = ecs.describe_tasks(cluster="task-lifecycle", tasks=[task_arn])
        task = desc["tasks"][0]
        if task["lastStatus"] == "STOPPED":
            stopped = True
            assert task["desiredStatus"] == "STOPPED"
            assert task["stopCode"] == "EssentialContainerExited"
            assert task["containers"][0]["lastStatus"] == "STOPPED"
            assert task["containers"][0]["exitCode"] == 0
            break
    assert stopped, "Task should transition to STOPPED after container exits"

def test_ecs_run_task_network_connectivity(ecs):
    """ECS container can reach Ministack (proves network detection works)."""
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    # When Ministack runs on the host (CI), containers need host.docker.internal.
    # When Ministack runs in Docker (compose), network detection handles it.
    host = os.environ.get("MINISTACK_HOST_FROM_CONTAINER", "host.docker.internal")
    parsed = urlparse(endpoint)
    container_endpoint = f"{parsed.scheme}://{host}:{parsed.port}"

    ecs.create_cluster(clusterName="net-test")
    ecs.register_task_definition(
        family="net-probe",
        containerDefinitions=[
            {
                "name": "probe",
                "image": "alpine:latest",
                "command": ["sh", "-c", f"wget -q -O /dev/null {container_endpoint}/_ministack/health"],
                "essential": True,
            }
        ],
    )
    resp = ecs.run_task(cluster="net-test", taskDefinition="net-probe")
    task_arn = resp["tasks"][0]["taskArn"]
    assert resp["tasks"][0]["lastStatus"] == "RUNNING"

    # Poll until STOPPED — wget should succeed (exit 0) if network is correct
    success = False
    for _ in range(30):
        time.sleep(2)
        desc = ecs.describe_tasks(cluster="net-test", tasks=[task_arn])
        task = desc["tasks"][0]
        if task["lastStatus"] == "STOPPED":
            exit_code = task["containers"][0].get("exitCode")
            assert exit_code == 0, (
                f"Container could not reach Ministack at {container_endpoint} "
                f"(exit code {exit_code}) — network detection may be broken"
            )
            success = True
            break
    assert success, "Task should transition to STOPPED"

def test_ecs_service(ecs):
    ecs.create_service(
        cluster="test-cluster",
        serviceName="test-service",
        taskDefinition="test-task",
        desiredCount=1,
    )
    resp = ecs.describe_services(cluster="test-cluster", services=["test-service"])
    assert len(resp["services"]) == 1
    assert resp["services"][0]["serviceName"] == "test-service"

def test_ecs_create_cluster_v2(ecs):
    resp = ecs.create_cluster(clusterName="ecs-cc-v2")
    assert resp["cluster"]["clusterName"] == "ecs-cc-v2"
    assert resp["cluster"]["status"] == "ACTIVE"
    assert "clusterArn" in resp["cluster"]

def test_ecs_list_clusters_v2(ecs):
    ecs.create_cluster(clusterName="ecs-lc-v2a")
    ecs.create_cluster(clusterName="ecs-lc-v2b")
    resp = ecs.list_clusters()
    arns = resp["clusterArns"]
    assert any("ecs-lc-v2a" in a for a in arns)
    assert any("ecs-lc-v2b" in a for a in arns)

def test_ecs_register_task_def_v2(ecs):
    resp = ecs.register_task_definition(
        family="ecs-td-v2",
        containerDefinitions=[
            {
                "name": "web",
                "image": "nginx:alpine",
                "cpu": 256,
                "memory": 512,
                "portMappings": [{"containerPort": 80, "hostPort": 8080}],
            },
            {"name": "sidecar", "image": "envoy:latest", "cpu": 128, "memory": 256},
        ],
        requiresCompatibilities=["EC2"],
        cpu="512",
        memory="1024",
    )
    td = resp["taskDefinition"]
    assert td["family"] == "ecs-td-v2"
    assert td["revision"] == 1
    assert td["status"] == "ACTIVE"
    assert len(td["containerDefinitions"]) == 2

    resp2 = ecs.register_task_definition(
        family="ecs-td-v2",
        containerDefinitions=[{"name": "web", "image": "nginx:latest", "cpu": 256, "memory": 512}],
    )
    assert resp2["taskDefinition"]["revision"] == 2

def test_ecs_list_task_defs_v2(ecs):
    ecs.register_task_definition(
        family="ecs-ltd-v2",
        containerDefinitions=[{"name": "app", "image": "img", "cpu": 64, "memory": 128}],
    )
    resp = ecs.list_task_definitions(familyPrefix="ecs-ltd-v2")
    assert len(resp["taskDefinitionArns"]) >= 1
    assert all("ecs-ltd-v2" in a for a in resp["taskDefinitionArns"])

def test_ecs_create_service_v2(ecs):
    ecs.create_cluster(clusterName="ecs-svc-v2c")
    ecs.register_task_definition(
        family="ecs-svc-v2td",
        containerDefinitions=[{"name": "w", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    resp = ecs.create_service(
        cluster="ecs-svc-v2c",
        serviceName="ecs-svc-v2",
        taskDefinition="ecs-svc-v2td",
        desiredCount=2,
    )
    svc = resp["service"]
    assert svc["serviceName"] == "ecs-svc-v2"
    assert svc["status"] == "ACTIVE"
    assert svc["desiredCount"] == 2

def test_ecs_describe_services_v2(ecs):
    ecs.create_cluster(clusterName="ecs-ds-v2c")
    ecs.register_task_definition(
        family="ecs-ds-v2td",
        containerDefinitions=[{"name": "w", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster="ecs-ds-v2c",
        serviceName="ecs-ds-v2a",
        taskDefinition="ecs-ds-v2td",
        desiredCount=1,
    )
    ecs.create_service(
        cluster="ecs-ds-v2c",
        serviceName="ecs-ds-v2b",
        taskDefinition="ecs-ds-v2td",
        desiredCount=3,
    )
    resp = ecs.describe_services(cluster="ecs-ds-v2c", services=["ecs-ds-v2a", "ecs-ds-v2b"])
    assert len(resp["services"]) == 2
    svc_map = {s["serviceName"]: s for s in resp["services"]}
    assert svc_map["ecs-ds-v2a"]["desiredCount"] == 1
    assert svc_map["ecs-ds-v2b"]["desiredCount"] == 3

def test_ecs_update_service_v2(ecs):
    ecs.create_cluster(clusterName="ecs-us-v2c")
    ecs.register_task_definition(
        family="ecs-us-v2td",
        containerDefinitions=[{"name": "w", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster="ecs-us-v2c",
        serviceName="ecs-us-v2",
        taskDefinition="ecs-us-v2td",
        desiredCount=1,
    )
    ecs.update_service(cluster="ecs-us-v2c", service="ecs-us-v2", desiredCount=5)
    resp = ecs.describe_services(cluster="ecs-us-v2c", services=["ecs-us-v2"])
    assert resp["services"][0]["desiredCount"] == 5

def test_ecs_tags_v2(ecs):
    resp = ecs.create_cluster(
        clusterName="ecs-tag-v2c",
        tags=[{"key": "env", "value": "staging"}],
    )
    arn = resp["cluster"]["clusterArn"]

    tags = ecs.list_tags_for_resource(resourceArn=arn)["tags"]
    assert any(t["key"] == "env" and t["value"] == "staging" for t in tags)

    ecs.tag_resource(resourceArn=arn, tags=[{"key": "team", "value": "platform"}])
    tags2 = ecs.list_tags_for_resource(resourceArn=arn)["tags"]
    tag_map = {t["key"]: t["value"] for t in tags2}
    assert tag_map["env"] == "staging"
    assert tag_map["team"] == "platform"

    ecs.untag_resource(resourceArn=arn, tagKeys=["env"])
    tags3 = ecs.list_tags_for_resource(resourceArn=arn)["tags"]
    assert not any(t["key"] == "env" for t in tags3)
    assert any(t["key"] == "team" for t in tags3)

def test_ecs_capacity_provider(ecs):
    resp = ecs.create_capacity_provider(
        name="test-cp",
        autoScalingGroupProvider={
            "autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:xxx:autoScalingGroupName/asg-1",
            "managedScaling": {"status": "ENABLED"},
        },
    )
    assert resp["capacityProvider"]["name"] == "test-cp"
    desc = ecs.describe_capacity_providers(capacityProviders=["test-cp"])
    assert any(cp["name"] == "test-cp" for cp in desc["capacityProviders"])
    ecs.delete_capacity_provider(capacityProvider="test-cp")

def test_ecs_update_cluster(ecs):
    ecs.create_cluster(clusterName="upd-cl")
    resp = ecs.update_cluster(
        cluster="upd-cl",
        settings=[{"name": "containerInsights", "value": "enabled"}],
    )
    assert resp["cluster"]["clusterName"] == "upd-cl"

def test_ecs_timestamps_are_epoch(ecs):
    """ECS timestamps should be epoch numbers, not ISO strings."""
    ecs.create_cluster(clusterName="ts-test-v44")
    clusters = ecs.describe_clusters(clusters=["ts-test-v44"])
    registered = clusters["clusters"][0].get("registeredContainerInstancesCount", 0)
    # registeredAt might not be present on cluster, test on task def
    ecs.register_task_definition(
        family="ts-td-v44",
        containerDefinitions=[{"name": "app", "image": "nginx", "memory": 256}],
    )
    td = ecs.describe_task_definition(taskDefinition="ts-td-v44")
    registered_at = td["taskDefinition"].get("registeredAt")
    if registered_at is not None:
        from datetime import datetime
        assert isinstance(registered_at, datetime), f"registeredAt should be datetime, got {type(registered_at)}"


# ---------------------------------------------------------------------------
# Service task spawning tests
# ---------------------------------------------------------------------------

def test_ecs_service_spawns_tasks(ecs):
    """Creating a service should spawn tasks matching desiredCount."""
    cluster = "svc-spawn-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="svc-spawn-td",
        containerDefinitions=[{"name": "app", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster,
        serviceName="svc-spawn",
        taskDefinition="svc-spawn-td",
        desiredCount=2,
    )
    tasks = ecs.list_tasks(cluster=cluster, serviceName="svc-spawn")
    assert len(tasks["taskArns"]) == 2

    # Verify describe_tasks returns correct metadata
    desc = ecs.describe_tasks(cluster=cluster, tasks=tasks["taskArns"])
    for t in desc["tasks"]:
        assert t["lastStatus"] == "RUNNING"
        assert t["group"] == "service:svc-spawn"
        assert t["startedBy"] == "svc-spawn"


def test_ecs_list_services(ecs):
    """list_services should return ARNs of services in the cluster."""
    cluster = "ls-svc-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="ls-svc-td",
        containerDefinitions=[{"name": "app", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="ls-svc-a", taskDefinition="ls-svc-td", desiredCount=1,
    )
    ecs.create_service(
        cluster=cluster, serviceName="ls-svc-b", taskDefinition="ls-svc-td", desiredCount=1,
    )
    resp = ecs.list_services(cluster=cluster)
    arns = resp["serviceArns"]
    assert len(arns) == 2
    assert any("ls-svc-a" in a for a in arns)
    assert any("ls-svc-b" in a for a in arns)


def test_ecs_service_running_count(ecs):
    """Service runningCount should match the number of actual running tasks."""
    cluster = "rc-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="rc-td",
        containerDefinitions=[{"name": "app", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="rc-svc", taskDefinition="rc-td", desiredCount=3,
    )
    resp = ecs.describe_services(cluster=cluster, services=["rc-svc"])
    svc = resp["services"][0]
    assert svc["runningCount"] == 3
    assert svc["desiredCount"] == 3


def test_ecs_service_scale_up(ecs):
    """Updating desiredCount should spawn additional tasks."""
    cluster = "su-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="su-td",
        containerDefinitions=[{"name": "app", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="su-svc", taskDefinition="su-td", desiredCount=1,
    )
    tasks_before = ecs.list_tasks(cluster=cluster, serviceName="su-svc")
    assert len(tasks_before["taskArns"]) == 1

    ecs.update_service(cluster=cluster, service="su-svc", desiredCount=3)
    tasks_after = ecs.list_tasks(cluster=cluster, serviceName="su-svc")
    assert len(tasks_after["taskArns"]) == 3

    resp = ecs.describe_services(cluster=cluster, services=["su-svc"])
    assert resp["services"][0]["runningCount"] == 3


def test_ecs_service_scale_down(ecs):
    """Scaling down desiredCount should stop excess tasks."""
    cluster = "sd-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="sd-td",
        containerDefinitions=[{"name": "app", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="sd-svc", taskDefinition="sd-td", desiredCount=3,
    )
    tasks_before = ecs.list_tasks(cluster=cluster, serviceName="sd-svc")
    assert len(tasks_before["taskArns"]) == 3

    ecs.update_service(cluster=cluster, service="sd-svc", desiredCount=1)
    tasks_after = ecs.list_tasks(cluster=cluster, serviceName="sd-svc")
    assert len(tasks_after["taskArns"]) == 1

    resp = ecs.describe_services(cluster=cluster, services=["sd-svc"])
    assert resp["services"][0]["runningCount"] == 1


def test_ecs_service_td_update_replaces_tasks(ecs):
    """Updating task definition should replace old tasks with new ones."""
    cluster = "tdu-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="tdu-td",
        containerDefinitions=[{"name": "app", "image": "nginx:1.0", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="tdu-svc", taskDefinition="tdu-td:1", desiredCount=2,
    )
    old_tasks = ecs.list_tasks(cluster=cluster, serviceName="tdu-svc")
    assert len(old_tasks["taskArns"]) == 2

    # Register new revision and update service
    resp2 = ecs.register_task_definition(
        family="tdu-td",
        containerDefinitions=[{"name": "app", "image": "nginx:2.0", "cpu": 64, "memory": 128}],
    )
    new_td_arn = resp2["taskDefinition"]["taskDefinitionArn"]
    ecs.update_service(cluster=cluster, service="tdu-svc", taskDefinition="tdu-td:2")

    # New tasks should be on the new TD
    new_tasks = ecs.list_tasks(cluster=cluster, serviceName="tdu-svc")
    assert len(new_tasks["taskArns"]) == 2

    # Verify all running tasks use the new task definition
    desc = ecs.describe_tasks(cluster=cluster, tasks=new_tasks["taskArns"])
    for t in desc["tasks"]:
        assert t["taskDefinitionArn"] == new_td_arn, \
            f"Task still on old TD: {t['taskDefinitionArn']}"
        assert t["lastStatus"] == "RUNNING"

    # Old tasks should be stopped
    old_desc = ecs.describe_tasks(cluster=cluster, tasks=old_tasks["taskArns"])
    for t in old_desc["tasks"]:
        assert t["lastStatus"] == "STOPPED"

    # Service should reflect correct counts
    svc = ecs.describe_services(cluster=cluster, services=["tdu-svc"])
    assert svc["services"][0]["runningCount"] == 2


def test_ecs_service_delete_stops_tasks(ecs):
    """Deleting a service should stop all its tasks."""
    cluster = "del-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="del-td",
        containerDefinitions=[{"name": "app", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="del-svc", taskDefinition="del-td", desiredCount=2,
    )
    tasks = ecs.list_tasks(cluster=cluster, serviceName="del-svc")
    assert len(tasks["taskArns"]) == 2

    ecs.delete_service(cluster=cluster, service="del-svc", force=True)
    tasks_after = ecs.list_tasks(cluster=cluster, serviceName="del-svc")
    assert len(tasks_after["taskArns"]) == 0

    # Verify tasks are STOPPED, not deleted
    desc = ecs.describe_tasks(cluster=cluster, tasks=tasks["taskArns"])
    for t in desc["tasks"]:
        assert t["lastStatus"] == "STOPPED"


def test_ecs_service_scale_to_zero(ecs):
    """Scaling to zero should stop all tasks without deleting the service."""
    cluster = "z-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="z-td",
        containerDefinitions=[{"name": "app", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="z-svc", taskDefinition="z-td", desiredCount=2,
    )
    ecs.update_service(cluster=cluster, service="z-svc", desiredCount=0)

    tasks = ecs.list_tasks(cluster=cluster, serviceName="z-svc")
    assert len(tasks["taskArns"]) == 0

    resp = ecs.describe_services(cluster=cluster, services=["z-svc"])
    svc = resp["services"][0]
    assert svc["status"] == "ACTIVE"
    assert svc["desiredCount"] == 0
    assert svc["runningCount"] == 0


def test_ecs_cluster_task_counts(ecs):
    """Cluster runningTasksCount should reflect service-spawned tasks."""
    cluster = "ct-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="ct-td",
        containerDefinitions=[{"name": "app", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="ct-svc", taskDefinition="ct-td", desiredCount=3,
    )
    resp = ecs.describe_clusters(clusters=[cluster])
    cl = resp["clusters"][0]
    assert cl["runningTasksCount"] == 3
    assert cl["activeServicesCount"] == 1


def test_ecs_cfn_service_visible(ecs, cfn):
    """Services created via CloudFormation should be visible in list-services and list-tasks."""
    stack_name = "ecs-cfn-test"
    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Cluster": {
                "Type": "AWS::ECS::Cluster",
                "Properties": {"ClusterName": "cfn-ecs-c"},
            },
            "TaskDef": {
                "Type": "AWS::ECS::TaskDefinition",
                "Properties": {
                    "Family": "cfn-ecs-td",
                    "ContainerDefinitions": [
                        {"Name": "app", "Image": "nginx", "Cpu": 64, "Memory": 128},
                    ],
                },
            },
            "Service": {
                "Type": "AWS::ECS::Service",
                "DependsOn": ["Cluster", "TaskDef"],
                "Properties": {
                    "Cluster": {"Ref": "Cluster"},
                    "ServiceName": "cfn-ecs-svc",
                    "TaskDefinition": {"Ref": "TaskDef"},
                    "DesiredCount": 1,
                    "LaunchType": "EC2",
                },
            },
        },
    })
    cfn.create_stack(StackName=stack_name, TemplateBody=template)

    # Verify service is visible
    svcs = ecs.list_services(cluster="cfn-ecs-c")
    assert any("cfn-ecs-svc" in a for a in svcs["serviceArns"]), \
        f"Service not found in list_services: {svcs['serviceArns']}"

    # Verify tasks were spawned
    tasks = ecs.list_tasks(cluster="cfn-ecs-c")
    assert len(tasks["taskArns"]) >= 1, "No tasks spawned for CF-created service"

    # Cleanup
    cfn.delete_stack(StackName=stack_name)
