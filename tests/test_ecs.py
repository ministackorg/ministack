import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

from ministack.services import ecs as ecs_service


def _replace_arn_section(arn, index, value):
    parts = arn.split(":", 5)
    parts[index] = value
    return ":".join(parts)


def _different_region(region):
    return "us-west-2" if region != "us-west-2" else "us-east-1"


def _replace_arn_region(arn):
    return _replace_arn_section(arn, 3, _different_region(arn.split(":", 5)[3]))


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


def test_ecs_list_tasks_reflects_natural_container_exit(ecs):
    """ListTasks must also reconcile lifecycle when a container has exited
    on its own. Previously only DescribeTasks ran the reconciler, so a user
    who only ever called ListTasks(desiredStatus=RUNNING) saw the dead task
    forever, and ListTasks(desiredStatus=STOPPED) returned an empty list.

    Reference: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-lifecycle-explanation.html
    "Some tasks are meant to run as batch jobs that naturally progress
    through from PENDING to RUNNING to STOPPED."
    """
    cluster = "task-lifecycle-listtasks"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="short-lived-list",
        containerDefinitions=[{
            "name": "worker",
            "image": "alpine:latest",
            "command": ["sh", "-c", "echo done"],
            "essential": True,
        }],
    )
    resp = ecs.run_task(cluster=cluster, taskDefinition="short-lived-list")
    task_arn = resp["tasks"][0]["taskArn"]

    # Give the container time to actually exit before we test the reconciler.
    # 6s is enough for `echo done` + Docker bookkeeping on every CI host
    # the existing run_task tests already pass on.
    time.sleep(6)

    # NOTE: explicitly NOT calling describe_tasks — the bug is that
    # list_tasks alone never reconciled.
    running = ecs.list_tasks(cluster=cluster, desiredStatus="RUNNING")["taskArns"]
    assert task_arn not in running, (
        "list_tasks(RUNNING) should not return a task whose container exited"
    )
    stopped = ecs.list_tasks(cluster=cluster, desiredStatus="STOPPED")["taskArns"]
    assert task_arn in stopped, (
        "list_tasks(STOPPED) should surface the naturally-exited task"
    )


def test_ecs_run_task_network_connectivity(ecs):
    """ECS container can reach Ministack (proves network detection works)."""
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    # Determine how a container can reach the host where Ministack runs.
    # Docker Desktop (macOS/Windows): host.docker.internal works.
    # Linux: use the Docker bridge gateway IP (typically 172.17.0.1).
    host = os.environ.get("MINISTACK_HOST_FROM_CONTAINER", "")
    if not host:
        import platform
        if platform.system() == "Linux":
            # Docker bridge gateway — how containers reach the host on Linux
            host = "172.17.0.1"
        else:
            host = "host.docker.internal"
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

def test_ecs_run_task_metadata_v4(ecs):
    """Container can resolve and read its V4 task-metadata URI end-to-end.

    Proves the full wiring: env-var injection in _run_task, the
    host.docker.internal/host-gateway extra_hosts mapping, the gateway
    routing /v4/<token>/task to ecs_metadata.handle_request, and the task
    payload containing the Containers array.
    """
    ecs.create_cluster(clusterName="metadata-test")
    ecs.register_task_definition(
        family="metadata-probe",
        containerDefinitions=[
            {
                "name": "probe",
                "image": "alpine:latest",
                # wget -O /tmp/r exits 0 only if the URI is reachable and
                # returns 200; grep then proves the body is the V4 task
                # shape (with a Containers array) rather than something
                # else returning 200.
                "command": [
                    "sh", "-c",
                    'wget -q -O /tmp/r "$ECS_CONTAINER_METADATA_URI_V4/task" '
                    '&& grep -q \'"Containers"\' /tmp/r',
                ],
                "essential": True,
            }
        ],
    )
    resp = ecs.run_task(cluster="metadata-test", taskDefinition="metadata-probe")
    task_arn = resp["tasks"][0]["taskArn"]
    assert resp["tasks"][0]["lastStatus"] == "RUNNING"

    success = False
    for _ in range(30):
        time.sleep(2)
        desc = ecs.describe_tasks(cluster="metadata-test", tasks=[task_arn])
        task = desc["tasks"][0]
        if task["lastStatus"] == "STOPPED":
            exit_code = task["containers"][0].get("exitCode")
            assert exit_code == 0, (
                f"Container could not read ECS_CONTAINER_METADATA_URI_V4/task "
                f"(exit code {exit_code}) — env-var injection, host-gateway "
                "mapping, or the /v4/<token> route may be broken"
            )
            success = True
            break
    assert success, "Task should transition to STOPPED"


def test_ecs_run_task_applies_container_command_overrides(monkeypatch):
    """RunTask containerOverrides.command should reach Docker run kwargs."""
    from ministack.services import ecs as _ecs

    class FakeContainers:
        def __init__(self):
            self.calls = []

        def get(self, _name):
            raise Exception("not found")

        def list(self, *args, **kwargs):
            return []

        def run(self, image, **kwargs):
            self.calls.append((image, kwargs))
            return SimpleNamespace(id=f"container-{len(self.calls):012d}")

    fake_containers = FakeContainers()
    fake_docker = SimpleNamespace(containers=fake_containers)

    monkeypatch.setattr(_ecs, "_get_docker", lambda: fake_docker)

    _ecs._register_task_definition({
        "family": "cmd-override-td",
        "containerDefinitions": [
            {
                "name": "web",
                "image": "busybox",
                "command": ["echo", "default-web"],
            },
            {
                "name": "worker",
                "image": "busybox",
                "command": ["echo", "default-worker"],
            },
        ],
    })

    _ecs._run_task({
        "cluster": "cmd-override-c",
        "taskDefinition": "cmd-override-td",
        "overrides": {
            "containerOverrides": [
                {"name": "web", "command": ["echo", "override-web"]},
            ],
        },
    })

    calls_by_name = {
        kwargs["labels"]["com.amazonaws.ecs.container-name"]: kwargs
        for _image, kwargs in fake_containers.calls
    }
    assert calls_by_name["web"]["command"] == ["echo", "override-web"]
    assert calls_by_name["worker"]["command"] == ["echo", "default-worker"]

    td = _ecs._task_defs["cmd-override-td:1"]
    assert td["containerDefinitions"][0]["command"] == ["echo", "default-web"]

def test_ecs_run_task_command_override_allows_empty_command(monkeypatch):
    """An explicit empty command override must replace the task definition command."""
    from ministack.services import ecs as _ecs

    class FakeContainers:
        def __init__(self):
            self.calls = []

        def get(self, _name):
            raise Exception("not found")

        def list(self, *args, **kwargs):
            return []

        def run(self, image, **kwargs):
            self.calls.append((image, kwargs))
            return SimpleNamespace(id="container-empty-command")

    fake_containers = FakeContainers()
    fake_docker = SimpleNamespace(containers=fake_containers)

    monkeypatch.setattr(_ecs, "_get_docker", lambda: fake_docker)

    _ecs._register_task_definition({
        "family": "empty-cmd-override-td",
        "containerDefinitions": [
            {
                "name": "web",
                "image": "busybox",
                "command": ["echo", "default"],
            },
        ],
    })

    _ecs._run_task({
        "cluster": "empty-cmd-override-c",
        "taskDefinition": "empty-cmd-override-td",
        "overrides": {
            "containerOverrides": [
                {"name": "web", "command": []},
            ],
        },
    })

    assert fake_containers.calls[0][1]["command"] == []

def test_ecs_run_task_injects_secrets_manager_secrets(monkeypatch):
    """RunTask must resolve containerDefinitions[].secrets (Secrets Manager
    valueFrom) and inject them into the container environment, including the
    json-key form."""
    from ministack.services import ecs as _ecs
    from ministack.services import secretsmanager as _sm

    _sm._create_secret({"Name": "ecs-secret-plain", "SecretString": "s3cr3t"})
    _sm._create_secret({"Name": "ecs-secret-json",
                        "SecretString": json.dumps({"password": "pa55"})})
    plain_arn = _sm._resolve("ecs-secret-plain")[1]["ARN"]
    json_arn = _sm._resolve("ecs-secret-json")[1]["ARN"]

    class FakeContainers:
        def __init__(self):
            self.calls = []

        def get(self, _name):
            raise Exception("not found")

        def list(self, *args, **kwargs):
            return []

        def run(self, image, **kwargs):
            self.calls.append((image, kwargs))
            return SimpleNamespace(id=f"container-{len(self.calls):012d}")

    fake_containers = FakeContainers()
    monkeypatch.setattr(_ecs, "_get_docker",
                        lambda: SimpleNamespace(containers=fake_containers))

    _ecs._register_task_definition({
        "family": "secrets-td",
        "containerDefinitions": [
            {
                "name": "app",
                "image": "busybox",
                "environment": [{"name": "FOO", "value": "bar"}],
                "secrets": [
                    {"name": "SECRET_VAL", "valueFrom": plain_arn},
                    {"name": "DB_PASS", "valueFrom": f"{json_arn}:password::"},
                ],
            },
        ],
    })

    _ecs._run_task({"cluster": "secrets-c", "taskDefinition": "secrets-td"})

    env = fake_containers.calls[0][1]["environment"]
    assert env["FOO"] == "bar"
    assert env["SECRET_VAL"] == "s3cr3t"
    assert env["DB_PASS"] == "pa55"

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


def test_ecs_cluster_arn_parser_does_not_tail_resolve_invalid_arns(ecs):
    resp = ecs.create_cluster(clusterName="ecs-arn-cluster")
    cluster_arn = resp["cluster"]["clusterArn"]
    valid = ecs.describe_clusters(clusters=[cluster_arn])
    assert valid["clusters"][0]["clusterName"] == "ecs-arn-cluster"

    wrong_service = _replace_arn_section(cluster_arn, 2, "lambda")
    wrong_partition = _replace_arn_section(cluster_arn, 1, "aws-cn")
    wrong_region = _replace_arn_region(cluster_arn)
    wrong_account = _replace_arn_section(cluster_arn, 4, "111111111111")
    wrong_resource = cluster_arn.replace(":cluster/", ":service/")
    malformed_resource = f"{cluster_arn}/extra"
    malformed = "arn:aws:ecs:us-east-1"

    for ref in [wrong_service, wrong_partition, wrong_region, wrong_account, wrong_resource, malformed_resource, malformed]:
        resp = ecs.describe_clusters(clusters=[ref])
        assert resp["clusters"] == []
        assert resp["failures"] == [{"arn": ref, "reason": "MISSING"}]


def test_ecs_service_arn_parser_does_not_tail_resolve_invalid_arns(ecs):
    cluster = "ecs-arn-service-cluster"
    cluster_arn = ecs.create_cluster(clusterName=cluster)["cluster"]["clusterArn"]
    ecs.register_task_definition(
        family="ecs-arn-service-td",
        containerDefinitions=[{"name": "app", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    created = ecs.create_service(
        cluster=cluster,
        serviceName="ecs-arn-service",
        taskDefinition="ecs-arn-service-td",
        desiredCount=0,
    )
    service_arn = created["service"]["serviceArn"]
    valid = ecs.describe_services(cluster=cluster, services=[service_arn])
    assert valid["services"][0]["serviceName"] == "ecs-arn-service"
    ecs.create_service(
        cluster=cluster,
        serviceName="None",
        taskDefinition="ecs-arn-service-td",
        desiredCount=0,
    )
    ecs.create_cluster(clusterName="None")
    ecs.create_service(
        cluster="None",
        serviceName="ecs-arn-service",
        taskDefinition="ecs-arn-service-td",
        desiredCount=0,
    )
    other_cluster = "ecs-arn-other-service-cluster"
    ecs.create_cluster(clusterName=other_cluster)
    other_service = ecs.create_service(
        cluster=other_cluster,
        serviceName="ecs-arn-service",
        taskDefinition="ecs-arn-service-td",
        desiredCount=0,
    )["service"]

    wrong_service = _replace_arn_section(service_arn, 2, "lambda")
    wrong_partition = _replace_arn_section(service_arn, 1, "aws-cn")
    wrong_region = _replace_arn_region(service_arn)
    wrong_account = _replace_arn_section(service_arn, 4, "111111111111")
    wrong_resource = service_arn.replace(":service/", ":cluster/")
    wrong_cluster_service = other_service["serviceArn"]
    malformed_resource = service_arn.replace(f":service/{cluster}/", f":service/extra/{cluster}/")
    malformed = "arn:aws:ecs:us-east-1"

    for ref in [
        wrong_service,
        wrong_partition,
        wrong_region,
        wrong_account,
        wrong_resource,
        wrong_cluster_service,
        malformed_resource,
        malformed,
    ]:
        resp = ecs.describe_services(cluster=cluster, services=[ref])
        assert resp["services"] == []
        assert resp["failures"] == [{"arn": ref, "reason": "MISSING"}]

    wrong_cluster = _replace_arn_region(cluster_arn)
    with pytest.raises(ClientError) as exc:
        ecs.describe_services(cluster=wrong_cluster, services=["ecs-arn-service"])
    assert exc.value.response["Error"]["Code"] == "ClusterNotFoundException"

    with pytest.raises(ClientError) as exc:
        ecs.update_service(cluster=cluster, service=wrong_service, desiredCount=1)
    assert exc.value.response["Error"]["Code"] == "ServiceNotFoundException"
    none_service = ecs.describe_services(cluster=cluster, services=["None"])
    assert none_service["services"][0]["desiredCount"] == 0

    with pytest.raises(ClientError) as exc:
        ecs.delete_service(cluster=cluster, service=wrong_service, force=True)
    assert exc.value.response["Error"]["Code"] == "ServiceNotFoundException"
    none_service = ecs.describe_services(cluster=cluster, services=["None"])
    assert none_service["services"][0]["status"] == "ACTIVE"

    with pytest.raises(ClientError) as exc:
        ecs.update_service(cluster=cluster, service=wrong_cluster_service, desiredCount=1)
    assert exc.value.response["Error"]["Code"] == "ServiceNotFoundException"
    service = ecs.describe_services(cluster=cluster, services=["ecs-arn-service"])
    assert service["services"][0]["desiredCount"] == 0

    with pytest.raises(ClientError) as exc:
        ecs.delete_service(cluster=cluster, service=wrong_cluster_service, force=True)
    assert exc.value.response["Error"]["Code"] == "ServiceNotFoundException"
    service = ecs.describe_services(cluster=cluster, services=["ecs-arn-service"])
    assert service["services"][0]["status"] == "ACTIVE"


def test_ecs_task_definition_arn_parser_does_not_tail_resolve_invalid_arns(ecs):
    resp = ecs.register_task_definition(
        family="ecs-arn-td",
        containerDefinitions=[{"name": "app", "image": "nginx", "cpu": 64, "memory": 128}],
    )
    task_definition_arn = resp["taskDefinition"]["taskDefinitionArn"]
    valid = ecs.describe_task_definition(taskDefinition=task_definition_arn)
    assert valid["taskDefinition"]["family"] == "ecs-arn-td"

    wrong_service = _replace_arn_section(task_definition_arn, 2, "lambda")
    wrong_partition = _replace_arn_section(task_definition_arn, 1, "aws-cn")
    wrong_region = _replace_arn_region(task_definition_arn)
    wrong_account = _replace_arn_section(task_definition_arn, 4, "111111111111")
    wrong_resource = task_definition_arn.replace(":task-definition/", ":cluster/")
    malformed_resource = f"{task_definition_arn}/extra"
    malformed = "arn:aws:ecs:us-east-1"

    for ref in [wrong_service, wrong_partition, wrong_region, wrong_account, wrong_resource, malformed_resource, malformed]:
        with pytest.raises(ClientError) as exc:
            ecs.describe_task_definition(taskDefinition=ref)
        assert exc.value.response["Error"]["Code"] == "ClientException"

    delete_resp = ecs.delete_task_definitions(taskDefinitions=[wrong_service])
    assert delete_resp["taskDefinitions"] == []
    assert delete_resp["failures"] == [
        {"arn": wrong_service, "reason": "TASK_DEFINITION_NOT_FOUND"},
    ]
    delete_resp = ecs.delete_task_definitions(taskDefinitions=["ecs-arn-td"])
    assert delete_resp["taskDefinitions"] == []
    assert delete_resp["failures"] == [
        {"arn": "ecs-arn-td", "reason": "TASK_DEFINITION_NOT_FOUND"},
    ]
    valid = ecs.describe_task_definition(taskDefinition=task_definition_arn)
    assert valid["taskDefinition"]["status"] == "ACTIVE"

    cluster = "ecs-arn-td-service-cluster"
    ecs.create_cluster(clusterName=cluster)
    with pytest.raises(ClientError) as exc:
        ecs.create_service(
            cluster=cluster,
            serviceName="ecs-arn-invalid-td-service",
            taskDefinition=wrong_region,
            desiredCount=0,
        )
    assert exc.value.response["Error"]["Code"] == "ClientException"

    created = ecs.create_service(
        cluster=cluster,
        serviceName="ecs-arn-valid-td-service",
        taskDefinition=task_definition_arn,
        desiredCount=0,
    )
    assert created["service"]["taskDefinition"] == task_definition_arn
    with pytest.raises(ClientError) as exc:
        ecs.update_service(
            cluster=cluster,
            service="ecs-arn-valid-td-service",
            taskDefinition=wrong_region,
        )
    assert exc.value.response["Error"]["Code"] == "ClientException"
    described = ecs.describe_services(cluster=cluster, services=["ecs-arn-valid-td-service"])
    assert described["services"][0]["taskDefinition"] == task_definition_arn


def test_ecs_task_arn_parser_does_not_tail_resolve_invalid_arns():
    cluster_name = "ecs-arn-task-cluster"
    region = ecs_service.get_region()
    account_id = ecs_service.get_account_id()
    task_id = str(_uuid_mod.uuid4())
    cluster_arn = f"arn:aws:ecs:{region}:{account_id}:cluster/{cluster_name}"
    task_arn = f"arn:aws:ecs:{region}:{account_id}:task/{cluster_name}/{task_id}"
    task = {"taskArn": task_arn, "clusterArn": cluster_arn}
    foreign_region = _different_region(region)
    foreign_region_task_arn = f"arn:aws:ecs:{foreign_region}:{account_id}:task/{cluster_name}/{task_id}"
    foreign_region_task = {
        "taskArn": foreign_region_task_arn,
        "clusterArn": f"arn:aws:ecs:{foreign_region}:{account_id}:cluster/{cluster_name}",
    }

    ecs_service._clusters[cluster_name] = {"clusterArn": cluster_arn, "status": "ACTIVE"}
    ecs_service._tasks[task_arn] = task
    ecs_service._tasks[foreign_region_task_arn] = foreign_region_task
    try:
        assert ecs_service._resolve_task(task_arn, cluster_name) == task
        assert ecs_service._resolve_task(task_id, cluster_name) == task

        wrong_service = _replace_arn_section(task_arn, 2, "lambda")
        wrong_partition = _replace_arn_section(task_arn, 1, "aws-cn")
        wrong_region = _replace_arn_region(task_arn)
        wrong_account = _replace_arn_section(task_arn, 4, "111111111111")
        wrong_resource = task_arn.replace(":task/", ":service/")
        wrong_cluster = f"arn:aws:ecs:{region}:{account_id}:task/other-cluster/{task_id}"
        slash_ref = f"other-cluster/{task_id}"
        empty_task_id = f"arn:aws:ecs:{region}:{account_id}:task/{cluster_name}/"
        malformed = "arn:aws:ecs:us-east-1"

        for ref in [
            wrong_service,
            wrong_partition,
            wrong_region,
            wrong_account,
            wrong_resource,
            wrong_cluster,
            slash_ref,
            empty_task_id,
            malformed,
            foreign_region_task_arn,
        ]:
            assert ecs_service._resolve_task(ref, cluster_name) is None
    finally:
        ecs_service._tasks.pop(task_arn, None)
        ecs_service._tasks.pop(foreign_region_task_arn, None)
        ecs_service._clusters.pop(cluster_name, None)


def test_ecs_capacity_provider_arn_parser_does_not_tail_resolve_invalid_arns(ecs):
    resp = ecs.create_capacity_provider(
        name="ecs-arn-cp",
        autoScalingGroupProvider={
            "autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:xxx:autoScalingGroupName/asg-1",
        },
    )
    capacity_provider_arn = resp["capacityProvider"]["capacityProviderArn"]
    valid = ecs.describe_capacity_providers(capacityProviders=[capacity_provider_arn])
    assert valid["capacityProviders"][0]["name"] == "ecs-arn-cp"

    wrong_service = _replace_arn_section(capacity_provider_arn, 2, "lambda")
    wrong_partition = _replace_arn_section(capacity_provider_arn, 1, "aws-cn")
    wrong_region = _replace_arn_region(capacity_provider_arn)
    wrong_account = _replace_arn_section(capacity_provider_arn, 4, "111111111111")
    wrong_resource = capacity_provider_arn.replace(":capacity-provider/", ":cluster/")
    malformed_resource = f"{capacity_provider_arn}/extra"
    slash_ref = "bogus/ecs-arn-cp"
    malformed = "arn:aws:ecs:us-east-1"

    for ref in [
        wrong_service,
        wrong_partition,
        wrong_region,
        wrong_account,
        wrong_resource,
        malformed_resource,
        slash_ref,
        malformed,
    ]:
        resp = ecs.describe_capacity_providers(capacityProviders=[ref])
        assert resp["capacityProviders"] == []

    with pytest.raises(ClientError) as exc:
        ecs.delete_capacity_provider(capacityProvider=wrong_service)
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"
    with pytest.raises(ClientError) as exc:
        ecs.delete_capacity_provider(capacityProvider=slash_ref)
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"
    valid = ecs.describe_capacity_providers(capacityProviders=["ecs-arn-cp"])
    assert valid["capacityProviders"][0]["name"] == "ecs-arn-cp"
    ecs.delete_capacity_provider(capacityProvider="ecs-arn-cp")


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

    # Real AWS keeps the service record around with status INACTIVE so
    # DescribeServices keeps working for ~1h after delete.
    svc_desc = ecs.describe_services(cluster=cluster, services=["del-svc"])
    assert svc_desc["failures"] == []
    assert svc_desc["services"][0]["status"] == "INACTIVE"


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


def test_ecs_cfn_taskdef_populates_registered_fields(ecs, cfn):
    """CFN-created TaskDefinitions must surface registeredAt/registeredBy/compatibilities,
    matching what RegisterTaskDefinition emits. Workloads like Go-SDK reconcilers fall
    back to time.Now() and emit warnings when registeredAt is missing."""
    from datetime import datetime
    stack_name = "ecs-cfn-td-fields"
    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "TaskDef": {
                "Type": "AWS::ECS::TaskDefinition",
                "Properties": {
                    "Family": "cfn-td-fields",
                    "ContainerDefinitions": [
                        {"Name": "app", "Image": "nginx", "Memory": 128},
                    ],
                },
            },
        },
    })
    cfn.create_stack(StackName=stack_name, TemplateBody=template)
    try:
        td = ecs.describe_task_definition(taskDefinition="cfn-td-fields")["taskDefinition"]
        assert isinstance(td.get("registeredAt"), datetime), \
            f"registeredAt missing or wrong type: {td.get('registeredAt')!r}"
        assert td.get("registeredBy", "").startswith("arn:aws:iam::"), \
            f"registeredBy missing: {td.get('registeredBy')!r}"
        assert "EC2" in td.get("compatibilities", []), \
            f"compatibilities missing/empty: {td.get('compatibilities')!r}"
    finally:
        cfn.delete_stack(StackName=stack_name)

def test_ecs_container_secret_ssm_valuefrom_is_region_scoped():
    """ECS ``secrets[].valueFrom`` SSM references honor the parameter's region.

    A bare parameter name resolves only in the task's request region — a
    same-name parameter in another region is not visible, and a missing one
    fails the launch. A full ARN resolves by the ARN's embedded region
    regardless of the request region."""
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import ssm as ssm_service

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    name = f"/ecs/secret/{_uuid_mod.uuid4().hex[:8]}"
    name_cdef = {"secrets": [{"name": "DB_PASS", "valueFrom": name}]}

    try:
        set_request_account_id(account_id)
        ssm_service.reset()

        # Parameter created in us-east-1 only.
        set_request_region("us-east-1")
        ssm_service._put_parameter({"Name": name, "Type": "String", "Value": "east-secret"})
        east_arn = ssm_service._parameters.get_scoped(account_id, "us-east-1", name)["ARN"]

        # Same request region → bare-name valueFrom resolves.
        assert ecs_service._resolve_container_secrets(name_cdef) == {"DB_PASS": "east-secret"}

        # Different request region, no same-name parameter → launch fails.
        set_request_region("us-west-2")
        with pytest.raises(ecs_service._SecretResolutionError):
            ecs_service._resolve_container_secrets(name_cdef)

        # A same-name parameter in the other region is isolated (west, not east).
        ssm_service._put_parameter({"Name": name, "Type": "String", "Value": "west-secret"})
        assert ecs_service._resolve_container_secrets(name_cdef) == {"DB_PASS": "west-secret"}

        # Full ARN resolves by the ARN's region regardless of request region.
        arn_cdef = {"secrets": [{"name": "DB_PASS", "valueFrom": east_arn}]}
        assert ecs_service._resolve_container_secrets(arn_cdef) == {"DB_PASS": "east-secret"}
    finally:
        ssm_service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)
