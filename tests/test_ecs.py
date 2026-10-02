import copy
import json
import os
import time
import uuid as _uuid_mod
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError
from conftest import LoopProbe

from ministack.services import ecs as ecs_service


def _replace_arn_section(arn, index, value):
    parts = arn.split(":", 5)
    parts[index] = value
    return ":".join(parts)


def _different_region(region):
    return "us-west-2" if region != "us-west-2" else "us-east-1"


def _wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate(), "condition did not become true before timeout"


def _ecs_docker_reachable():
    """Whether this end-to-end ECS lifecycle test can start real containers."""
    try:
        import docker
        docker.from_env(timeout=2).ping()
    except Exception:
        return False
    return True


requires_ecs_docker = pytest.mark.skipif(
    not _ecs_docker_reachable(), reason="requires a reachable Docker daemon")


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

@pytest.mark.parametrize("cpu", [None, "512"])
@pytest.mark.parametrize("memory", [None, "1024"])
@pytest.mark.parametrize("requires_compatibilities", [None, ["EC2"]])
def test_ecs_task_definition_optional_fields_readback(
    ecs, cpu, memory, requires_compatibilities,
):
    # AWS RegisterTaskDefinition documents optional EC2 sizing and omission of
    # unspecified requiresCompatibilities. Its register/describe/deregister
    # examples omit task-level sizing when only container resources are given.
    optional_fields = {
        field: value for field, value in {
            "cpu": cpu,
            "memory": memory,
            "requiresCompatibilities": requires_compatibilities,
        }.items() if value is not None
    }
    registered = ecs.register_task_definition(
        family=f"optional-fields-{_uuid_mod.uuid4().hex[:8]}",
        containerDefinitions=[{
            "name": "web", "image": "nginx:alpine", "cpu": 128, "memory": 256,
        }],
        **optional_fields,
    )["taskDefinition"]
    arn = registered["taskDefinitionArn"]
    described = ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]
    deregistered = ecs.deregister_task_definition(taskDefinition=arn)["taskDefinition"]

    for td in (registered, described, deregistered):
        for field in ("cpu", "memory", "requiresCompatibilities"):
            if field in optional_fields:
                assert td[field] == optional_fields[field]
            else:
                assert field not in td
        assert td["containerDefinitions"][0]["cpu"] == 128
        assert td["containerDefinitions"][0]["memory"] == 256
        assert td["networkMode"] == "bridge"
    assert registered["status"] == described["status"] == "ACTIVE"
    assert deregistered["status"] == "INACTIVE"


def test_ecs_task_definition_explicit_fargate_sizing_readback(ecs):
    registered = ecs.register_task_definition(
        family=f"explicit-fargate-{_uuid_mod.uuid4().hex[:8]}",
        containerDefinitions=[{"name": "web", "image": "nginx:alpine"}],
        networkMode="awsvpc",
        requiresCompatibilities=["FARGATE"],
        cpu="512",
        memory="1024",
    )["taskDefinition"]
    arn = registered["taskDefinitionArn"]
    described = ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]
    deregistered = ecs.deregister_task_definition(taskDefinition=arn)["taskDefinition"]

    for td in (registered, described, deregistered):
        assert td["cpu"] == "512"
        assert td["memory"] == "1024"
        assert td["requiresCompatibilities"] == ["FARGATE"]
        assert td["networkMode"] == "awsvpc"


def test_ecs_list_task_defs(ecs):
    resp = ecs.list_task_definitions(familyPrefix="test-task")
    assert len(resp["taskDefinitionArns"]) >= 1

@pytest.mark.data_plane
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
    assert resp["tasks"][0]["lastStatus"] in ("PROVISIONING", "PENDING", "RUNNING")

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


@pytest.mark.data_plane
def test_ecs_run_task_forwards_awslogs_to_cloudwatch_logs(ecs, logs):
    cluster = f"awslogs-{_uuid_mod.uuid4().hex[:8]}"
    family = f"{cluster}-td"
    group = f"/ecs/{cluster}"
    marker = f"ECS-AWSLOGS-{_uuid_mod.uuid4().hex[:8]}"
    stream_prefix = "ecs"

    logs.create_log_group(logGroupName=group)
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family=family,
        containerDefinitions=[{
            "name": "app",
            "image": "alpine:latest",
            "command": ["sh", "-c", f"echo {marker}"],
            "essential": True,
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": group,
                    "awslogs-region": "us-east-1",
                    "awslogs-stream-prefix": stream_prefix,
                },
            },
        }],
    )

    try:
        resp = ecs.run_task(cluster=cluster, taskDefinition=family)
    except Exception as exc:
        pytest.skip(f"ECS RunTask unavailable in this environment: {exc}")

    task_arn = resp["tasks"][0]["taskArn"]
    task_id = task_arn.rsplit("/", 1)[-1]
    stream_name = f"{stream_prefix}/app/{task_id}"

    def marker_reached_cloudwatch_logs():
        streams = logs.describe_log_streams(
            logGroupName=group,
            logStreamNamePrefix=stream_name,
        )["logStreams"]
        if not streams:
            return False
        events = logs.get_log_events(
            logGroupName=group,
            logStreamName=stream_name,
        )["events"]
        return any(marker in event["message"] for event in events)

    _wait_until(marker_reached_cloudwatch_logs, timeout=20)


def _logs_client(region):
    import boto3
    from botocore.config import Config
    from conftest import ENDPOINT
    return boto3.client("logs", endpoint_url=ENDPOINT, region_name=region,
                        aws_access_key_id="test", aws_secret_access_key="test",
                        config=Config(region_name=region, inject_host_prefix=False))


@pytest.mark.data_plane
def test_ecs_awslogs_without_stream_prefix_names_the_stream_after_the_container_id(ecs, logs):
    """AWS: "If you don't specify a prefix with this option, then the log stream
    is named after the container ID that's assigned by the Docker daemon"."""
    cluster = f"awslogs-noprefix-{_uuid_mod.uuid4().hex[:8]}"
    family = f"{cluster}-td"
    group = f"/ecs/{cluster}"
    marker = f"ECS-NOPREFIX-{_uuid_mod.uuid4().hex[:8]}"

    logs.create_log_group(logGroupName=group)
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family=family,
        containerDefinitions=[{
            "name": "app",
            "image": "alpine:latest",
            "command": ["sh", "-c", f"echo {marker}"],
            "essential": True,
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {"awslogs-group": group, "awslogs-region": "us-east-1"},
            },
        }],
    )

    task_arn = ecs.run_task(cluster=cluster, taskDefinition=family)["tasks"][0]["taskArn"]

    def runtime_id():
        containers = ecs.describe_tasks(cluster=cluster, tasks=[task_arn])["tasks"][0]["containers"]
        return containers[0].get("runtimeId")

    _wait_until(runtime_id, timeout=30)
    short_id = runtime_id()

    def stream_named_after_the_container():
        streams = logs.describe_log_streams(logGroupName=group)["logStreams"]
        names = {s["logStreamName"] for s in streams}
        if not names:
            return False
        assert not any("/" in n for n in names), f"expected a bare container id, got {names}"
        assert names == {n for n in names if n.startswith(short_id)}, names
        stream = next(iter(names))
        assert len(stream) == 64, f"expected the full docker container id, got {stream}"
        events = logs.get_log_events(logGroupName=group, logStreamName=stream)["events"]
        return any(marker in e["message"] for e in events)

    _wait_until(stream_named_after_the_container, timeout=30)


@pytest.mark.data_plane
def test_ecs_awslogs_region_option_decides_where_the_logs_land(ecs, logs):
    """awslogs-region is where the driver ships the logs, not where the task ran."""
    target_region = _different_region("us-east-1")
    remote_logs = _logs_client(target_region)
    cluster = f"awslogs-region-{_uuid_mod.uuid4().hex[:8]}"
    family = f"{cluster}-td"
    group = f"/ecs/{cluster}"
    marker = f"ECS-REGION-{_uuid_mod.uuid4().hex[:8]}"

    remote_logs.create_log_group(logGroupName=group)
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family=family,
        containerDefinitions=[{
            "name": "app",
            "image": "alpine:latest",
            "command": ["sh", "-c", f"echo {marker}"],
            "essential": True,
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": group,
                    "awslogs-region": target_region,
                    "awslogs-stream-prefix": "ecs",
                },
            },
        }],
    )

    task_arn = ecs.run_task(cluster=cluster, taskDefinition=family)["tasks"][0]["taskArn"]
    stream = f"ecs/app/{task_arn.rsplit('/', 1)[-1]}"

    def marker_in_target_region():
        streams = remote_logs.describe_log_streams(
            logGroupName=group, logStreamNamePrefix=stream)["logStreams"]
        if not streams:
            return False
        events = remote_logs.get_log_events(logGroupName=group, logStreamName=stream)["events"]
        return any(marker in e["message"] for e in events)

    _wait_until(marker_in_target_region, timeout=30)

    with pytest.raises(ClientError) as exc:
        logs.describe_log_streams(logGroupName=group)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


@pytest.mark.data_plane
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


@pytest.mark.data_plane
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
    assert resp["tasks"][0]["lastStatus"] in ("PROVISIONING", "PENDING", "RUNNING")

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

@pytest.mark.data_plane
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
    assert resp["tasks"][0]["lastStatus"] in ("PROVISIONING", "PENDING", "RUNNING")

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


@pytest.mark.parametrize("has_services", [False, True])
def test_ecs_central_restore_reconciles_after_loading_state(monkeypatch, tmp_path, has_services):
    """Boot restores tasks and attributes before scheduling service relaunch."""
    import ministack.app as app
    from ministack.core import persistence
    from ministack.core.responses import AccountRegionScopedDict

    account, region = "222222222222", "eu-west-1"
    services = AccountRegionScopedDict()
    tasks = AccountRegionScopedDict()
    attributes = AccountRegionScopedDict()
    if has_services:
        services.set_scoped(account, region, "cluster/service", {"status": "ACTIVE"})
    tasks.set_scoped(account, region, "task", {"lastStatus": "RUNNING", "version": 1})
    attributes.set_scoped(account, region, "instance:attr", {"name": "attr", "value": "v"})
    for name in ("_services", "_tasks", "_attributes"):
        monkeypatch.setattr(ecs_service, name, AccountRegionScopedDict())
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(app, "_state_map", {"ecs": "ecs"})
    monkeypatch.setattr(app, "_loaded_modules", {})
    persistence.save_state("ecs", {"services": services, "tasks": tasks, "attributes": attributes})

    scheduled = []

    def schedule():
        # Capture what the worker would see at the moment it is scheduled.
        scheduled.append(copy.deepcopy(ecs_service._tasks.get_scoped(account, region, "task")))

    monkeypatch.setattr(ecs_service, "_start_restored_service_reconciler", schedule)
    app._load_persisted_state()

    task = ecs_service._tasks.get_scoped(account, region, "task")
    assert task["lastStatus"] == "STOPPED"
    assert task["version"] == 2
    assert ecs_service._attributes.get_scoped(account, region, "instance:attr")["value"] == "v"
    assert scheduled == ([task] if has_services else [])


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

    _wait_until(lambda: len(fake_containers.calls) == 2)
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

    _wait_until(lambda: fake_containers.calls)
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

    _wait_until(lambda: fake_containers.calls)
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
        containerDefinitions=[{"name": "web", "image": "nginx:alpine", "cpu": 256, "memory": 512}],
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
        containerDefinitions=[{"name": "w", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
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
        containerDefinitions=[{"name": "w", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
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
        containerDefinitions=[{"name": "w", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
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
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
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
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
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


@pytest.mark.parametrize("include", [
    [], ["ATTACHMENTS"], ["CONFIGURATIONS"], ["SETTINGS"], ["STATISTICS"], ["TAGS"],
])
def test_ecs_describe_clusters_include_gates_fields(ecs, include):
    """Each include value returns only its own field; the others stay empty or absent."""
    name = f"incl-{_uuid_mod.uuid4().hex[:8]}"
    settings = [{"name": "containerInsights", "value": "enabled"}]
    tags = [{"key": "k", "value": "v"}]
    configuration = {"executeCommandConfiguration": {"logging": "DEFAULT"}}
    ecs.create_cluster(clusterName=name, tags=tags, settings=settings, configuration=configuration)
    try:
        c = ecs.describe_clusters(clusters=[name], include=include)["clusters"][0]
        assert c["settings"] == (settings if "SETTINGS" in include else [])
        assert c["tags"] == (tags if "TAGS" in include else [])
        assert c.get("configuration") == (configuration if "CONFIGURATIONS" in include else None)
        assert ("attachments" in c) == ("ATTACHMENTS" in include)
        assert "attachmentsStatus" not in c
        if "STATISTICS" not in include:
            assert c["statistics"] == []
    finally:
        ecs.delete_cluster(cluster=name)


def test_ecs_describe_clusters_statistics_count_services_by_launch_type(ecs):
    """STATISTICS lists the sixteen task and service counters in the AWS order."""
    name = f"stats-{_uuid_mod.uuid4().hex[:8]}"
    ecs.create_cluster(clusterName=name)
    td = ecs.register_task_definition(
        family=name, containerDefinitions=[{"name": "app", "image": "alpine", "memory": 128}],
    )["taskDefinition"]["taskDefinitionArn"]
    ecs.create_service(cluster=name, serviceName="svc", taskDefinition=td,
                       desiredCount=0, launchType="EC2")
    try:
        stats = ecs.describe_clusters(clusters=[name], include=["STATISTICS"])["clusters"][0]["statistics"]
        assert [s["name"] for s in stats] == [
            "runningEC2TasksCount", "runningFargateTasksCount",
            "pendingEC2TasksCount", "pendingFargateTasksCount",
            "runningExternalTasksCount", "pendingExternalTasksCount",
            "runningManagedInstancesTasksCount", "pendingManagedInstancesTasksCount",
            "activeEC2ServiceCount", "activeFargateServiceCount",
            "drainingEC2ServiceCount", "drainingFargateServiceCount",
            "activeExternalServiceCount", "drainingExternalServiceCount",
            "activeManagedInstancesServiceCount", "drainingManagedInstancesServiceCount",
        ]
        assert {s["name"]: s["value"] for s in stats if s["value"] != "0"} == {"activeEC2ServiceCount": "1"}
    finally:
        ecs.delete_service(cluster=name, service="svc")
        ecs.delete_cluster(cluster=name)
        ecs.deregister_task_definition(taskDefinition=td)


def test_ecs_timestamps_are_epoch(ecs):
    """ECS timestamps should be epoch numbers, not ISO strings."""
    ecs.create_cluster(clusterName="ts-test-v44")
    clusters = ecs.describe_clusters(clusters=["ts-test-v44"])
    registered = clusters["clusters"][0].get("registeredContainerInstancesCount", 0)
    # registeredAt might not be present on cluster, test on task def
    ecs.register_task_definition(
        family="ts-td-v44",
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "memory": 256}],
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
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster,
        serviceName="svc-spawn",
        taskDefinition="svc-spawn-td",
        desiredCount=2,
    )
    _wait_until(lambda: len(ecs.list_tasks(cluster=cluster, serviceName="svc-spawn")["taskArns"]) == 2, timeout=30)
    tasks = ecs.list_tasks(cluster=cluster, serviceName="svc-spawn")

    # Verify describe_tasks returns correct metadata
    _wait_until(
        lambda: all(
            task["lastStatus"] == "RUNNING"
            for task in ecs.describe_tasks(
                cluster=cluster, tasks=tasks["taskArns"]
            )["tasks"]
        ),
        timeout=30,
    )
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
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
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
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="rc-svc", taskDefinition="rc-td", desiredCount=3,
    )
    _wait_until(
        lambda: ecs.describe_services(
            cluster=cluster, services=["rc-svc"]
        )["services"][0]["runningCount"] == 3,
        timeout=30,
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
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="su-svc", taskDefinition="su-td", desiredCount=1,
    )
    tasks_before = ecs.list_tasks(cluster=cluster, serviceName="su-svc")
    assert len(tasks_before["taskArns"]) == 1

    ecs.update_service(cluster=cluster, service="su-svc", desiredCount=3)
    _wait_until(lambda: len(ecs.list_tasks(cluster=cluster, serviceName="su-svc")["taskArns"]) == 3, timeout=30)
    tasks_after = ecs.list_tasks(cluster=cluster, serviceName="su-svc")

    _wait_until(
        lambda: ecs.describe_services(
            cluster=cluster, services=["su-svc"]
        )["services"][0]["runningCount"] == 3,
        timeout=30,
    )
    resp = ecs.describe_services(cluster=cluster, services=["su-svc"])
    assert resp["services"][0]["runningCount"] == 3


def test_ecs_service_scale_down(ecs):
    """Scaling down desiredCount should stop excess tasks."""
    cluster = "sd-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="sd-td",
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="sd-svc", taskDefinition="sd-td", desiredCount=3,
    )
    _wait_until(lambda: len(ecs.list_tasks(cluster=cluster, serviceName="sd-svc")["taskArns"]) == 3, timeout=30)
    tasks_before = ecs.list_tasks(cluster=cluster, serviceName="sd-svc")

    ecs.update_service(cluster=cluster, service="sd-svc", desiredCount=1)
    tasks_after = ecs.list_tasks(cluster=cluster, serviceName="sd-svc")
    assert len(tasks_after["taskArns"]) == 1

    _wait_until(
        lambda: ecs.describe_services(
            cluster=cluster, services=["sd-svc"]
        )["services"][0]["runningCount"] == 1,
        timeout=30,
    )
    resp = ecs.describe_services(cluster=cluster, services=["sd-svc"])
    assert resp["services"][0]["runningCount"] == 1


def test_ecs_service_td_update_replaces_tasks(ecs):
    """Updating task definition should replace old tasks with new ones."""
    cluster = "tdu-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="tdu-td",
        containerDefinitions=[{"name": "app", "image": "nginx:latest", "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="tdu-svc", taskDefinition="tdu-td:1", desiredCount=2,
    )
    _wait_until(lambda: len(ecs.list_tasks(cluster=cluster, serviceName="tdu-svc")["taskArns"]) == 2, timeout=30)
    old_tasks = ecs.list_tasks(cluster=cluster, serviceName="tdu-svc")

    # Register new revision and update service
    resp2 = ecs.register_task_definition(
        family="tdu-td",
        containerDefinitions=[{"name": "app", "image": "nginx:alpine", "cpu": 64, "memory": 128}],
    )
    new_td_arn = resp2["taskDefinition"]["taskDefinitionArn"]
    ecs.update_service(cluster=cluster, service="tdu-svc", taskDefinition="tdu-td:2")

    # A rolling deployment keeps the old tasks until the replacement is
    # healthy.  Wait specifically for two RUNNING tasks on the new revision
    # instead of treating the still-running old tasks as replacements.
    _wait_until(
        lambda: len([
            task for task in ecs.describe_tasks(
                cluster=cluster,
                tasks=ecs.list_tasks(
                    cluster=cluster, serviceName="tdu-svc"
                )["taskArns"],
            )["tasks"]
            if task["taskDefinitionArn"] == new_td_arn
            and task["lastStatus"] == "RUNNING"
        ]) == 2,
        timeout=30,
    )

    # Old tasks should be stopped
    _wait_until(
        lambda: all(
            task["lastStatus"] == "STOPPED"
            for task in ecs.describe_tasks(
                cluster=cluster, tasks=old_tasks["taskArns"]
            )["tasks"]
        ),
        timeout=30,
    )

    new_tasks = ecs.list_tasks(cluster=cluster, serviceName="tdu-svc")
    assert len(new_tasks["taskArns"]) == 2
    desc = ecs.describe_tasks(cluster=cluster, tasks=new_tasks["taskArns"])
    assert all(t["taskDefinitionArn"] == new_td_arn for t in desc["tasks"])

    # Service should reflect correct counts
    _wait_until(
        lambda: ecs.describe_services(
            cluster=cluster, services=["tdu-svc"]
        )["services"][0]["runningCount"] == 2,
        timeout=30,
    )
    svc = ecs.describe_services(cluster=cluster, services=["tdu-svc"])
    assert svc["services"][0]["runningCount"] == 2
    deployments = svc["services"][0]["deployments"]
    assert len(deployments) == 1
    assert deployments[0]["taskDefinition"] == new_td_arn
    assert deployments[0]["status"] == "PRIMARY"
    assert deployments[0]["rolloutState"] == "COMPLETED"


@requires_ecs_docker
@pytest.mark.data_plane
@pytest.mark.serial
def test_ecs_service_circuit_breaker_rolls_back_crashing_revision(ecs):
    """A real container exit fails the new deployment and restores the old one.

    This deliberately observes only DescribeServices after UpdateService.  The
    service's background lifecycle watcher, rather than an incidental
    DescribeTasks request, must discover every ``exit 1`` and give the circuit
    breaker the failure signal.
    """
    cluster = "circuit-breaker-c"
    family = "circuit-breaker-td"
    service = "circuit-breaker-svc"
    ecs.create_cluster(clusterName=cluster)
    healthy = ecs.register_task_definition(
        family=family,
        requiresCompatibilities=["FARGATE"],
        networkMode="awsvpc",
        containerDefinitions=[{
            "name": "app",
            "image": "alpine:latest",
            "command": ["sh", "-c", "sleep 600"],
        }],
    )["taskDefinition"]["taskDefinitionArn"]
    ecs.create_service(
        cluster=cluster,
        serviceName=service,
        taskDefinition=healthy,
        desiredCount=1,
        launchType="FARGATE",
        networkConfiguration={"awsvpcConfiguration": {"subnets": ["subnet-test"]}},
    )
    _wait_until(
        lambda: ecs.describe_services(cluster=cluster, services=[service])
        ["services"][0]["runningCount"] == 1,
        timeout=30,
    )

    crashing = ecs.register_task_definition(
        family=family,
        requiresCompatibilities=["FARGATE"],
        networkMode="awsvpc",
        containerDefinitions=[{
            "name": "app",
            "image": "alpine:latest",
            "command": ["sh", "-c", "exit 1"],
        }],
    )["taskDefinition"]["taskDefinitionArn"]
    ecs.update_service(
        cluster=cluster,
        service=service,
        taskDefinition=crashing,
        deploymentConfiguration={
            "deploymentCircuitBreaker": {"enable": True, "rollback": True},
        },
    )

    final_service = None

    def rolled_back():
        nonlocal final_service
        final_service = ecs.describe_services(
            cluster=cluster, services=[service]
        )["services"][0]
        deployments = final_service["deployments"]
        failed = next(
            (deployment for deployment in deployments
             if deployment["taskDefinition"] == crashing
             and deployment.get("rolloutState") == "FAILED"),
            None,
        )
        primary = next(
            (deployment for deployment in deployments
             if deployment.get("status") == "PRIMARY"),
            None,
        )
        return bool(
            failed and failed.get("rolloutStateReason")
            and primary and primary["taskDefinition"] == healthy
            and primary.get("rolloutState") == "COMPLETED"
            and final_service["taskDefinition"] == healthy
            and final_service["runningCount"] == 1
        )

    _wait_until(rolled_back, timeout=30)
    failed = next(
        deployment for deployment in final_service["deployments"]
        if deployment["taskDefinition"] == crashing
    )
    assert failed["failedTasks"] == 3
    assert failed["rolloutState"] == "FAILED"
    assert "circuit breaker" in failed["rolloutStateReason"]


@pytest.mark.parametrize(
    ("reset_on_healthy", "expected_failures"),
    [(None, 0), (True, 0), (False, 2)],
)
def test_ecs_circuit_breaker_reset_on_healthy_task(
        reset_on_healthy, expected_failures):
    """A stable task resets failures unless cumulative mode is requested."""
    from ministack.services import ecs as _ecs

    cluster = f"reset-healthy-{_uuid_mod.uuid4().hex[:8]}"
    service = "svc"
    svc_key = f"{cluster}/{service}"
    deployment = _ecs._make_deployment("reset-healthy-td:2", 2)
    deployment["rolloutState"] = "IN_PROGRESS"
    deployment["rolloutStateReason"] = ""
    deployment["failedTasks"] = 2
    breaker = {"enable": True, "rollback": True}
    if reset_on_healthy is not None:
        breaker["resetOnHealthyTask"] = reset_on_healthy
    svc = {
        "serviceName": service,
        "clusterArn": (
            f"arn:aws:ecs:us-east-1:000000000000:cluster/{cluster}"
        ),
        "status": "ACTIVE",
        "deploymentConfiguration": {"deploymentCircuitBreaker": breaker},
        "deployments": [deployment],
    }
    task = {
        "taskArn": f"arn:aws:ecs:us-east-1:000000000000:task/{cluster}/healthy",
        "taskDefinitionArn": "reset-healthy-td:2",
        "_deployment_id": deployment["id"],
        "lastStatus": "RUNNING",
    }
    _ecs._services[svc_key] = svc
    _ecs._tasks[task["taskArn"]] = task
    try:
        _ecs._record_service_task_healthy(svc_key, task)
        assert deployment["failedTasks"] == expected_failures
    finally:
        _ecs._tasks.pop(task["taskArn"], None)
        _ecs._services.pop(svc_key, None)


def test_ecs_service_delete_stops_tasks(ecs):
    """Deleting a service should stop all its tasks."""
    cluster = "del-c"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family="del-td",
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="del-svc", taskDefinition="del-td", desiredCount=2,
    )
    _wait_until(lambda: len(ecs.list_tasks(cluster=cluster, serviceName="del-svc")["taskArns"]) == 2, timeout=30)
    tasks = ecs.list_tasks(cluster=cluster, serviceName="del-svc")

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
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
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
        containerDefinitions=[{"name": "app", "image": "alpine:latest", "command": ["sleep", "3600"], "cpu": 64, "memory": 128}],
    )
    ecs.create_service(
        cluster=cluster, serviceName="ct-svc", taskDefinition="ct-td", desiredCount=3,
    )
    _wait_until(
        lambda: ecs.describe_clusters(
            clusters=[cluster]
        )["clusters"][0]["runningTasksCount"] == 3,
        timeout=30,
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


def test_ecs_cfn_service_deployment_circuit_breaker_create_and_update(ecs, cfn):
    """CloudFormation maps circuit-breaker fields and updates them in place."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"ecs-cfn-circuit-{suffix}"
    cluster_name = f"ecs-cfn-circuit-c-{suffix}"
    service_name = f"ecs-cfn-circuit-s-{suffix}"
    first_family = f"ecs-cfn-circuit-one-{suffix}"
    second_family = f"ecs-cfn-circuit-two-{suffix}"

    def template(task_definition, configuration):
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Cluster": {
                    "Type": "AWS::ECS::Cluster",
                    "Properties": {"ClusterName": cluster_name},
                },
                "FirstTaskDefinition": {
                    "Type": "AWS::ECS::TaskDefinition",
                    "Properties": {
                        "Family": first_family,
                        "ContainerDefinitions": [{"Name": "app", "Image": "busybox"}],
                    },
                },
                "SecondTaskDefinition": {
                    "Type": "AWS::ECS::TaskDefinition",
                    "Properties": {
                        "Family": second_family,
                        "ContainerDefinitions": [{"Name": "app", "Image": "busybox"}],
                    },
                },
                "Service": {
                    "Type": "AWS::ECS::Service",
                    "DependsOn": [
                        "Cluster", "FirstTaskDefinition", "SecondTaskDefinition",
                    ],
                    "Properties": {
                        "Cluster": {"Ref": "Cluster"},
                        "ServiceName": service_name,
                        "TaskDefinition": {"Ref": task_definition},
                        "DesiredCount": 0,
                        "DeploymentConfiguration": configuration,
                    },
                },
            },
        }

    initial = {
        "MaximumPercent": 150,
        "MinimumHealthyPercent": 50,
        "DeploymentCircuitBreaker": {
            "Enable": True,
            "Rollback": True,
            "ResetOnHealthyTask": False,
            "ThresholdConfiguration": {"Type": "COUNT", "Value": 5},
        },
    }
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(template("FirstTaskDefinition", initial)),
    )
    _wait_until(
        lambda: cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["StackStatus"]
        == "CREATE_COMPLETE",
        timeout=30,
    )
    service = ecs.describe_services(
        cluster=cluster_name, services=[service_name]
    )["services"][0]
    assert service["deploymentConfiguration"] == {
        "maximumPercent": 150,
        "minimumHealthyPercent": 50,
        "deploymentCircuitBreaker": {
            "enable": True,
            "rollback": True,
            "resetOnHealthyTask": False,
            "thresholdConfiguration": {"type": "COUNT", "value": 5},
        },
    }

    updated = {
        "DeploymentCircuitBreaker": {
            "Enable": True,
            "Rollback": False,
            "ResetOnHealthyTask": True,
            "ThresholdConfiguration": {
                "Type": "UNBOUNDED_PERCENT", "Value": 25,
            },
        },
    }
    cfn.update_stack(
        StackName=stack_name,
        TemplateBody=json.dumps(template("SecondTaskDefinition", updated)),
    )
    _wait_until(
        lambda: cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["StackStatus"]
        == "UPDATE_COMPLETE",
        timeout=30,
    )
    service = ecs.describe_services(
        cluster=cluster_name, services=[service_name]
    )["services"][0]
    second_arn = ecs.describe_task_definition(
        taskDefinition=second_family
    )["taskDefinition"]["taskDefinitionArn"]
    assert service["taskDefinition"] == second_arn
    cfn.delete_stack(StackName=stack_name)


def test_ecs_cfn_service_update_propagates_ecs_errors(monkeypatch):
    """A rejected ECS UpdateService must fail the CloudFormation update."""
    from ministack.services.cloudformation import provisioners

    monkeypatch.setattr(
        ecs_service,
        "_update_service",
        lambda _request: (
            400,
            {"Content-Type": "application/x-amz-json-1.0"},
            b'{"__type":"ClientException","message":"task definition not found"}',
        ),
    )

    with pytest.raises(ValueError, match="AWS::ECS::Service update failed"):
        provisioners._ecs_service_update(
            "arn:aws:ecs:us-east-1:000000000000:service/default/example",
            {"TaskDefinition": "example:1"},
            {"TaskDefinition": "example:99"},
            "stack",
        )


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


def test_ecs_resource_state_is_region_scoped(monkeypatch):
    """Same-named ECS resources coexist without leaking across regions."""
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    cluster_name = f"scope-cluster-{_uuid_mod.uuid4().hex[:8]}"
    family = f"scope-family-{_uuid_mod.uuid4().hex[:8]}"
    service_name = f"scope-service-{_uuid_mod.uuid4().hex[:8]}"
    capacity_provider = f"scope-cp-{_uuid_mod.uuid4().hex[:8]}"
    attribute_key = "i-scope:zone"
    task_definition = {
        "family": family,
        "containerDefinitions": [{"name": "app", "image": "busybox"}],
    }

    monkeypatch.setattr(ecs_service, "_get_docker", lambda: None)
    ecs_service.reset()
    try:
        set_request_account_id(account_id)
        set_request_region("us-east-1")
        east_cluster = json.loads(ecs_service._create_cluster({
            "clusterName": cluster_name,
            "tags": [{"key": "region", "value": "east"}],
        })[2])["cluster"]
        ecs_service._register_task_definition(copy.deepcopy(task_definition))
        east_td = json.loads(ecs_service._register_task_definition(
            copy.deepcopy(task_definition)
        )[2])["taskDefinition"]
        east_service = json.loads(ecs_service._create_service({
            "cluster": cluster_name,
            "serviceName": service_name,
            "taskDefinition": family,
            "desiredCount": 0,
        })[2])["service"]
        east_task = json.loads(ecs_service._run_task({
            "cluster": cluster_name,
            "taskDefinition": family,
        })[2])["tasks"][0]
        east_cp = json.loads(ecs_service._create_capacity_provider({
            "name": capacity_provider,
        })[2])["capacityProvider"]
        ecs_service._put_attributes({"attributes": [{
            "targetId": "i-scope",
            "name": "zone",
            "value": "east",
            "targetType": "container-instance",
        }]})
        ecs_service._put_account_setting({"name": "containerInsights", "value": "east"})

        set_request_region("us-west-2")
        assert cluster_name not in ecs_service._clusters
        assert family not in ecs_service._task_def_latest
        assert f"{cluster_name}/{service_name}" not in ecs_service._services
        assert capacity_provider not in ecs_service._capacity_providers
        assert attribute_key not in ecs_service._attributes
        assert "containerInsights" not in ecs_service._account_settings
        assert json.loads(ecs_service._list_tasks({"cluster": cluster_name})[2])["taskArns"] == []

        west_cluster = json.loads(ecs_service._create_cluster({
            "clusterName": cluster_name,
            "tags": [{"key": "region", "value": "west"}],
        })[2])["cluster"]
        west_td = json.loads(ecs_service._register_task_definition(
            copy.deepcopy(task_definition)
        )[2])["taskDefinition"]
        west_service = json.loads(ecs_service._create_service({
            "cluster": cluster_name,
            "serviceName": service_name,
            "taskDefinition": family,
            "desiredCount": 0,
        })[2])["service"]
        west_task = json.loads(ecs_service._run_task({
            "cluster": cluster_name,
            "taskDefinition": family,
        })[2])["tasks"][0]
        west_cp = json.loads(ecs_service._create_capacity_provider({
            "name": capacity_provider,
        })[2])["capacityProvider"]
        ecs_service._put_attributes({"attributes": [{
            "targetId": "i-scope",
            "name": "zone",
            "value": "west",
            "targetType": "container-instance",
        }]})
        ecs_service._put_account_setting({"name": "containerInsights", "value": "west"})

        assert west_cluster["clusterArn"] != east_cluster["clusterArn"]
        assert west_td["revision"] == 1
        assert east_td["revision"] == 2
        assert west_service["serviceArn"] != east_service["serviceArn"]
        assert west_task["taskArn"] != east_task["taskArn"]
        assert west_cp["capacityProviderArn"] != east_cp["capacityProviderArn"]
        assert ecs_service._attributes[attribute_key]["value"] == "west"
        assert ecs_service._account_settings["containerInsights"] == "west"
        assert json.loads(ecs_service._list_tasks({"cluster": cluster_name})[2])["taskArns"] == [
            west_task["taskArn"]
        ]

        # Tags remain account-scoped because their keys are region-bearing ARNs.
        assert json.loads(ecs_service._list_tags_for_resource({
            "resourceArn": east_cluster["clusterArn"],
        })[2])["tags"] == [{"key": "region", "value": "east"}]
        ecs_service._untag_resource({
            "resourceArn": east_cluster["clusterArn"],
            "tagKeys": ["region"],
        })
        assert json.loads(ecs_service._list_tags_for_resource({
            "resourceArn": west_cluster["clusterArn"],
        })[2])["tags"] == [{"key": "region", "value": "west"}]

        set_request_region("us-east-1")
        assert ecs_service._task_def_latest[family] == 2
        assert ecs_service._attributes[attribute_key]["value"] == "east"
        assert ecs_service._account_settings["containerInsights"] == "east"
        assert json.loads(ecs_service._list_tasks({"cluster": cluster_name})[2])["taskArns"] == [
            east_task["taskArn"]
        ]
        assert json.loads(ecs_service._list_tags_for_resource({
            "resourceArn": east_cluster["clusterArn"],
        })[2])["tags"] == []
    finally:
        ecs_service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_ecs_container_secret_arn_selects_the_requested_region():
    """Full secret ARNs select their embedded region without same-name leaks."""
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import secretsmanager as sm_service

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    name = f"ecs-secret-scope-{_uuid_mod.uuid4().hex[:8]}"

    sm_service.reset()
    try:
        set_request_account_id(account_id)
        set_request_region("us-east-1")
        east_arn = json.loads(sm_service._create_secret({
            "Name": name,
            "SecretString": "east-secret",
        })[2])["ARN"]

        set_request_region("us-west-2")
        west_arn = json.loads(sm_service._create_secret({
            "Name": name,
            "SecretString": "west-secret",
        })[2])["ARN"]

        assert ecs_service._resolve_container_secrets({
            "secrets": [{"name": "DB_PASS", "valueFrom": west_arn}],
        }) == {"DB_PASS": "west-secret"}
        assert ecs_service._resolve_container_secrets({
            "secrets": [{"name": "DB_PASS", "valueFrom": east_arn}],
        }) == {"DB_PASS": "east-secret"}
    finally:
        sm_service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


# ---------------------------------------------------------------------------
# Re-entrancy under concurrency — the rest of the suite issues one request at
# a time, so a service whose blocking work is misclassified passes serially
# and only wedges under load. These fire N callers at once and assert both
# that every caller completes and that the event loop keeps serving.
# ---------------------------------------------------------------------------
@pytest.mark.serial
def test_ecs_run_task_does_not_block_the_loop(ecs):
    """RunTask talks to the Docker daemon; that must never happen on the loop."""
    cluster = f"conc-{_uuid_mod.uuid4().hex[:8]}"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family=f"{cluster}-td",
        containerDefinitions=[{"name": "app", "image": "alpine:latest",
                               "command": ["sleep", "3600"], "memory": 64, "essential": True}],
    )
    with LoopProbe() as probe:
        try:
            ecs.run_task(cluster=cluster, taskDefinition=f"{cluster}-td", count=1)
        except Exception as exc:                      # no daemon / image pull refused
            pytest.skip(f"ECS RunTask unavailable in this environment: {exc}")
        finally:
            for arn in ecs.list_tasks(cluster=cluster).get("taskArns", []):
                try:
                    ecs.stop_task(cluster=cluster, task=arn)
                except Exception:
                    pass
            try:
                ecs.delete_cluster(cluster=cluster)
            except Exception:
                pass
    probe.assert_responsive("ECS RunTask")


def _fake_docker_recorder(run_impl=None):
    """Docker double that records containers.run kwargs (see command-override tests)."""
    class FakeContainers:
        def __init__(self):
            self.calls = []

        def get(self, _name):
            raise Exception("not found")

        def list(self, *args, **kwargs):
            return []

        def run(self, image, **kwargs):
            self.calls.append((image, kwargs))
            if run_impl is not None:
                return run_impl(image, kwargs)
            return SimpleNamespace(id=f"container{len(self.calls):03d}")

    fake_containers = FakeContainers()
    return fake_containers, SimpleNamespace(containers=fake_containers)


def test_ecs_run_task_returns_pending_before_docker_start(monkeypatch):
    """Registration is visible while a slow Docker start is still blocked, and
    the task reports ACTIVATING for the length of the pull."""
    import threading

    from ministack.services import ecs as _ecs

    started = threading.Event()
    release = threading.Event()
    containers = {}

    class FakeContainer:
        def __init__(self, cid):
            self.id = cid
            self.status = "running"
            self.attrs = {"NetworkSettings": {"Networks": {}}}
            self.removed = False

        def reload(self):
            pass

        def wait(self):
            return {"StatusCode": 0}

        def stop(self, timeout=5):
            self.status = "exited"

        def remove(self, **kwargs):
            self.removed = True

    class FakeContainers:
        def get(self, name):
            if name in containers:
                return containers[name]
            raise Exception("not found")

        def list(self, *args, **kwargs):
            return list(containers.values())

        def run(self, image, **kwargs):
            started.set()
            assert release.wait(timeout=5)
            container = FakeContainer("pending-test-container")
            containers[container.id] = container
            return container

    monkeypatch.setattr(
        _ecs, "_get_docker", lambda: SimpleNamespace(containers=FakeContainers())
    )
    _ecs._register_task_definition({
        "family": "pending-test-td",
        "containerDefinitions": [{"name": "app", "image": "busybox"}],
    })

    response = _ecs._run_task({
        "cluster": "pending-test-c",
        "taskDefinition": "pending-test-td",
    })
    task = json.loads(response[2])["tasks"][0]
    assert task["lastStatus"] == "PROVISIONING"
    assert task["containers"][0]["lastStatus"] == "PENDING"
    # AWS omits a timestamp it has no value for rather than sending a null:
    # the task has not started, pulled or stopped yet.
    for member in ("startedAt", "pullStartedAt", "pullStoppedAt",
                   "stoppingAt", "stoppedAt"):
        assert member not in task
    assert started.wait(timeout=2)
    # The image pull is the ACTIVATING phase on AWS: "This is the state where
    # Amazon ECS pulls the container images, creates the containers ...".
    assert _ecs._tasks[task["taskArn"]]["lastStatus"] == "ACTIVATING"
    assert _ecs._tasks[task["taskArn"]]["pullStartedAt"]

    release.set()
    _wait_until(lambda: _ecs._tasks[task["taskArn"]]["lastStatus"] == "RUNNING")

    containers["pending-test-container"].status = "exited"
    described = json.loads(_ecs._describe_tasks({
        "cluster": "pending-test-c",
        "tasks": [task["taskArn"]],
    })[2])["tasks"][0]
    assert described["lastStatus"] == "STOPPED"
    assert described["containers"][0]["exitCode"] == 0


def test_ecs_run_task_starts_provisioning_and_metadata_follows(monkeypatch):
    """A task starts PROVISIONING and the metadata endpoint follows it,
    reporting the container's own KnownStatus apart from the task's.
    Reported by @iot-rocket."""
    import threading

    from ministack.services import ecs as _ecs
    from ministack.services import ecs_metadata as _md

    started = threading.Event()
    release = threading.Event()
    containers = {}

    class FakeContainer:
        def __init__(self, cid):
            self.id = cid
            self.status = "running"
            self.attrs = {"NetworkSettings": {"Networks": {}}}

        def reload(self):
            pass

        def wait(self):
            return {"StatusCode": 0}

        def stop(self, timeout=5):
            self.status = "exited"

        def remove(self, **kwargs):
            pass

    class FakeContainers:
        def get(self, name):
            if name in containers:
                return containers[name]
            raise Exception("not found")

        def list(self, *args, **kwargs):
            return list(containers.values())

        def run(self, image, **kwargs):
            started.set()
            assert release.wait(timeout=5)
            container = FakeContainer("awsvpc-test-container")
            containers[container.id] = container
            return container

    monkeypatch.setattr(
        _ecs, "_get_docker", lambda: SimpleNamespace(containers=FakeContainers())
    )
    _ecs._register_task_definition({
        "family": "awsvpc-test-td",
        "networkMode": "awsvpc",
        "containerDefinitions": [{"name": "app", "image": "busybox"}],
    })

    response = _ecs._run_task({
        "cluster": "awsvpc-test-c",
        "taskDefinition": "awsvpc-test-td",
    })
    task = json.loads(response[2])["tasks"][0]
    arn = task["taskArn"]
    assert task["lastStatus"] == "PROVISIONING"

    assert started.wait(timeout=2)
    assert _ecs._tasks[arn]["lastStatus"] == "ACTIVATING"
    # Seeded from the container's own record entry, still PENDING while the
    # task pulls: AWS reports the two apart.
    assert _md._TASKS[arn]["KnownStatus"] == "ACTIVATING"
    assert _md._TASKS[arn]["DesiredStatus"] == "RUNNING"
    assert all(c["KnownStatus"] == "PENDING" for c in _md._TASKS[arn]["Containers"])

    release.set()
    _wait_until(lambda: _ecs._tasks[arn]["lastStatus"] == "RUNNING")
    _wait_until(lambda: _md._TASKS[arn]["KnownStatus"] == "RUNNING")
    assert all(c["KnownStatus"] == "RUNNING" for c in _md._TASKS[arn]["Containers"])

    # DesiredStatus reaches the container payloads; KnownStatus stays per-container.
    _ecs._stop_task({"cluster": "awsvpc-test-c", "task": arn})
    assert _ecs._tasks[arn]["lastStatus"] == "STOPPED"


def test_ecs_run_task_bridge_mode_also_starts_provisioning(monkeypatch):
    """Every network mode starts PROVISIONING, not only awsvpc: the ENI is one
    example of the "additional steps before the task is launched", not the
    condition for the state (task-lifecycle)."""
    import threading

    from ministack.services import ecs as _ecs

    release = threading.Event()

    class FakeContainers:
        def get(self, _name):
            raise Exception("not found")

        def list(self, *args, **kwargs):
            return []

        def run(self, image, **kwargs):
            assert release.wait(timeout=5)
            raise RuntimeError("stopped by the test")

    monkeypatch.setattr(
        _ecs, "_get_docker", lambda: SimpleNamespace(containers=FakeContainers())
    )
    _ecs._register_task_definition({
        "family": "bridge-test-td",
        "networkMode": "bridge",
        "containerDefinitions": [{"name": "app", "image": "busybox"}],
    })
    response = _ecs._run_task({
        "cluster": "bridge-test-c",
        "taskDefinition": "bridge-test-td",
    })
    assert json.loads(response[2])["tasks"][0]["lastStatus"] == "PROVISIONING"
    release.set()


def test_ecs_run_task_startup_failure_is_a_stopped_task(monkeypatch):
    from ministack.services import ecs as _ecs

    class FakeContainers:
        def get(self, _name):
            raise Exception("not found")

        def run(self, image, **kwargs):
            raise RuntimeError("image pull failed")

    monkeypatch.setattr(
        _ecs, "_get_docker", lambda: SimpleNamespace(containers=FakeContainers())
    )
    _ecs._register_task_definition({
        "family": "startup-failure-td",
        "containerDefinitions": [{"name": "app", "image": "missing"}],
    })

    response = _ecs._run_task({
        "cluster": "startup-failure-c",
        "taskDefinition": "startup-failure-td",
    })
    task = json.loads(response[2])["tasks"][0]
    _wait_until(lambda: _ecs._tasks[task["taskArn"]]["lastStatus"] == "STOPPED")
    stopped = _ecs._tasks[task["taskArn"]]
    assert stopped["stopCode"] == "TaskFailedToStart"
    assert "image pull failed" in stopped["stoppedReason"]
    assert stopped["containers"][0]["lastStatus"] == "STOPPED"


def test_ecs_stop_task_during_pending_start_cleans_late_container(monkeypatch):
    import threading

    from ministack.services import ecs as _ecs

    started = threading.Event()
    release = threading.Event()
    containers = {}

    class FakeContainer:
        def __init__(self):
            self.id = "late-container"
            self.status = "running"
            self.attrs = {"NetworkSettings": {"Networks": {}}}
            self.removed = False

        def reload(self):
            pass

        def stop(self, timeout=5):
            self.status = "exited"

        def remove(self, **kwargs):
            self.removed = True

    class FakeContainers:
        def get(self, name):
            if name in containers:
                return containers[name]
            raise Exception("not found")

        def run(self, image, **kwargs):
            started.set()
            assert release.wait(timeout=5)
            container = FakeContainer()
            containers[container.id] = container
            return container

    fake_docker = SimpleNamespace(containers=FakeContainers())
    monkeypatch.setattr(_ecs, "_get_docker", lambda: fake_docker)
    _ecs._register_task_definition({
        "family": "stop-pending-td",
        "containerDefinitions": [{"name": "app", "image": "busybox"}],
    })

    response = _ecs._run_task({
        "cluster": "stop-pending-c",
        "taskDefinition": "stop-pending-td",
    })
    task = json.loads(response[2])["tasks"][0]
    assert started.wait(timeout=2)
    stopped = json.loads(_ecs._stop_task({
        "cluster": "stop-pending-c",
        "task": task["taskArn"],
    })[2])["task"]
    assert stopped["lastStatus"] == "STOPPED"

    release.set()
    _wait_until(lambda: "late-container" in containers)
    _wait_until(lambda: containers["late-container"].removed)
    assert _ecs._tasks[task["taskArn"]]["lastStatus"] == "STOPPED"


def test_ecs_reset_during_pending_start_cleans_late_container(monkeypatch):
    import threading

    from ministack.services import ecs as _ecs

    started = threading.Event()
    release = threading.Event()
    containers = {}

    class FakeContainer:
        id = "reset-late-container"
        status = "running"
        attrs = {"NetworkSettings": {"Networks": {}}}

        def __init__(self):
            self.removed = False

        def reload(self):
            pass

        def stop(self, timeout=5):
            self.status = "exited"

        def remove(self, **kwargs):
            self.removed = True

    class FakeContainers:
        def get(self, name):
            if name in containers:
                return containers[name]
            raise Exception("not found")

        def list(self, *args, **kwargs):
            return list(containers.values())

        def run(self, image, **kwargs):
            started.set()
            assert release.wait(timeout=5)
            container = FakeContainer()
            containers[container.id] = container
            return container

    fake_docker = SimpleNamespace(containers=FakeContainers())
    monkeypatch.setattr(_ecs, "_get_docker", lambda: fake_docker)
    _ecs.reset()
    _ecs._register_task_definition({
        "family": "reset-pending-td",
        "containerDefinitions": [{"name": "app", "image": "busybox"}],
    })

    response = _ecs._run_task({
        "cluster": "reset-pending-c",
        "taskDefinition": "reset-pending-td",
    })
    task_arn = json.loads(response[2])["tasks"][0]["taskArn"]
    assert started.wait(timeout=2)

    _ecs.reset()
    release.set()
    _wait_until(lambda: "reset-late-container" in containers)
    _wait_until(lambda: containers["reset-late-container"].removed)
    assert task_arn not in _ecs._tasks


def _version_probe_container(cid):
    class FakeContainer:
        def __init__(self):
            self.id = cid
            self.status = "running"
            self.attrs = {"NetworkSettings": {"Networks": {}}}
            self.removed = False

        def reload(self):
            pass

        def wait(self):
            return {"StatusCode": 0}

        def stop(self, timeout=5):
            self.status = "exited"

        def remove(self, **kwargs):
            self.removed = True

    return FakeContainer()


def _version_probe_docker(container):
    class FakeContainers:
        def get(self, name):
            if name == container.id:
                return container
            raise Exception("not found")

        def list(self, *args, **kwargs):
            return [container]

        def run(self, image, **kwargs):
            return container

    return SimpleNamespace(containers=FakeContainers())


def test_ecs_task_version_counts_state_changes(monkeypatch):
    """The version counter moves on every transition the record reports, so a
    consumer can tell a stale copy from the current one."""
    from ministack.services import ecs as _ecs

    container = _version_probe_container("version-counter-container")
    monkeypatch.setattr(_ecs, "_get_docker", lambda: _version_probe_docker(container))
    _ecs._register_task_definition({
        "family": "version-counter-td",
        "containerDefinitions": [{"name": "app", "image": "busybox"}],
    })

    response = _ecs._run_task({
        "cluster": "version-counter-c",
        "taskDefinition": "version-counter-td",
    })
    task = json.loads(response[2])["tasks"][0]
    task_arn = task["taskArn"]
    assert task["lastStatus"] == "PROVISIONING"
    assert task["version"] == 1

    # 3, not 4: ACTIVATING raises no state-change event, so the bumps to RUNNING
    # are PROVISIONING, PENDING and RUNNING, which is what a real task reads.
    _wait_until(lambda: _ecs._tasks[task_arn]["lastStatus"] == "RUNNING")
    assert _ecs._tasks[task_arn]["version"] == 3

    stopped = json.loads(_ecs._stop_task({
        "cluster": "version-counter-c",
        "task": task_arn,
        "reason": "done here",
    })[2])["task"]
    assert stopped["lastStatus"] == "STOPPED"
    assert stopped["version"] == 6


def test_ecs_task_version_moves_once_for_a_natural_exit(monkeypatch):
    """The exit is observed by whichever DescribeTasks notices it first; the
    ones after it describe the same version. 6, as on a real task: the
    desiredStatus flip, DEPROVISIONING and STOPPED each count."""
    from ministack.services import ecs as _ecs

    container = _version_probe_container("version-exit-container")
    monkeypatch.setattr(_ecs, "_get_docker", lambda: _version_probe_docker(container))
    _ecs._register_task_definition({
        "family": "version-exit-td",
        "containerDefinitions": [{"name": "app", "image": "busybox"}],
    })

    task_arn = json.loads(_ecs._run_task({
        "cluster": "version-exit-c",
        "taskDefinition": "version-exit-td",
    })[2])["tasks"][0]["taskArn"]
    _wait_until(lambda: _ecs._tasks[task_arn]["lastStatus"] == "RUNNING")

    container.status = "exited"
    described = json.loads(_ecs._describe_tasks({
        "cluster": "version-exit-c",
        "tasks": [task_arn],
    })[2])["tasks"][0]
    assert described["lastStatus"] == "STOPPED"
    assert described["version"] == 6

    again = json.loads(_ecs._describe_tasks({
        "cluster": "version-exit-c",
        "tasks": [task_arn],
    })[2])["tasks"][0]
    assert again["version"] == 6


def test_ecs_secret_resolution_failure_stops_before_docker_run(monkeypatch):
    from ministack.services import ecs as _ecs

    fake_containers, fake_docker = _fake_docker_recorder()
    monkeypatch.setattr(_ecs, "_get_docker", lambda: fake_docker)
    _ecs._register_task_definition({
        "family": "secret-failure-td",
        "containerDefinitions": [{
            "name": "app",
            "image": "busybox",
            "secrets": [{
                "name": "MISSING",
                "valueFrom": "arn:aws:secretsmanager:us-east-1:000000000000:secret:missing",
            }],
        }],
    })

    response = _ecs._run_task({
        "cluster": "secret-failure-c",
        "taskDefinition": "secret-failure-td",
    })
    task = json.loads(response[2])["tasks"][0]
    _wait_until(lambda: _ecs._tasks[task["taskArn"]]["lastStatus"] == "STOPPED")
    stopped = _ecs._tasks[task["taskArn"]]
    assert not fake_containers.calls
    assert stopped["stopCode"] == "TaskFailedToStart"
    assert "unable to retrieve secret" in stopped["stoppedReason"]


def test_ecs_run_task_count_and_multi_container_startup_are_independent(monkeypatch):
    from ministack.services import ecs as _ecs

    fake_containers, fake_docker = _fake_docker_recorder()
    monkeypatch.setattr(_ecs, "_get_docker", lambda: fake_docker)
    _ecs._register_task_definition({
        "family": "multi-start-td",
        "containerDefinitions": [
            {"name": "app", "image": "busybox"},
            {"name": "sidecar", "image": "busybox"},
        ],
    })

    response = _ecs._run_task({
        "cluster": "multi-start-c",
        "taskDefinition": "multi-start-td",
        "count": 2,
    })
    tasks = json.loads(response[2])["tasks"]
    assert all(task["lastStatus"] == "PROVISIONING" for task in tasks)
    _wait_until(lambda: len(fake_containers.calls) == 4)
    _wait_until(
        lambda: all(
            _ecs._tasks[task["taskArn"]]["lastStatus"] == "RUNNING"
            for task in tasks
        )
    )
    runtime_ids = {
        container["runtimeId"]
        for task in tasks
        for container in _ecs._tasks[task["taskArn"]]["containers"]
    }
    assert len(runtime_ids) == 4


def test_ecs_run_task_preserves_nonzero_exit_code(monkeypatch):
    from ministack.services import ecs as _ecs

    class FakeContainer:
        id = "nonzero-container"
        status = "exited"
        attrs = {"NetworkSettings": {"Networks": {}}}

        def reload(self):
            pass

        def wait(self):
            return {"StatusCode": 17}

    container = FakeContainer()

    class FakeContainers:
        def get(self, name):
            if name == container.id:
                return container
            raise Exception("not found")

        def run(self, image, **kwargs):
            return container

    monkeypatch.setattr(
        _ecs, "_get_docker", lambda: SimpleNamespace(containers=FakeContainers())
    )
    _ecs._register_task_definition({
        "family": "nonzero-td",
        "containerDefinitions": [{"name": "app", "image": "busybox"}],
    })
    response = _ecs._run_task({
        "cluster": "nonzero-c",
        "taskDefinition": "nonzero-td",
    })
    task = json.loads(response[2])["tasks"][0]
    _wait_until(
        lambda: json.loads(_ecs._describe_tasks({
            "cluster": "nonzero-c", "tasks": [task["taskArn"]],
        })[2])["tasks"][0]["lastStatus"] == "STOPPED"
    )
    stopped = json.loads(_ecs._describe_tasks({
        "cluster": "nonzero-c", "tasks": [task["taskArn"]],
    })[2])["tasks"][0]
    assert stopped["containers"][0]["exitCode"] == 17


def test_ecs_awsvpc_task_does_not_publish_host_ports(monkeypatch):
    """awsvpc tasks get their own ENI, so container ports are not bound on the host.

    Publishing them means two tasks sharing a container port collide on the host —
    something that cannot happen on Fargate.
    """
    from ministack.services import ecs as _ecs

    fake_containers, fake_docker = _fake_docker_recorder()
    monkeypatch.setattr(_ecs, "_get_docker", lambda: fake_docker)

    _ecs._register_task_definition({
        "family": "awsvpc-ports-td",
        "networkMode": "awsvpc",
        "containerDefinitions": [{
            "name": "web",
            "image": "busybox",
            "portMappings": [{"containerPort": 80, "hostPort": 80, "protocol": "tcp"}],
        }],
    })
    _ecs._run_task({"cluster": "awsvpc-ports-c", "taskDefinition": "awsvpc-ports-td"})

    _wait_until(lambda: fake_containers.calls)
    assert fake_containers.calls, "expected the container to be launched"
    _image, kwargs = fake_containers.calls[0]
    assert not kwargs.get("ports"), (
        f"awsvpc task must not publish host ports, got {kwargs.get('ports')!r}"
    )


def test_ecs_bridge_task_still_publishes_host_ports(monkeypatch):
    """bridge networking does publish to the host — the awsvpc fix must not affect it."""
    from ministack.services import ecs as _ecs

    fake_containers, fake_docker = _fake_docker_recorder()
    monkeypatch.setattr(_ecs, "_get_docker", lambda: fake_docker)

    _ecs._register_task_definition({
        "family": "bridge-ports-td",
        "networkMode": "bridge",
        "containerDefinitions": [{
            "name": "web",
            "image": "busybox",
            "portMappings": [{"containerPort": 80, "hostPort": 8080, "protocol": "tcp"}],
        }],
    })
    _ecs._run_task({"cluster": "bridge-ports-c", "taskDefinition": "bridge-ports-td"})

    _wait_until(lambda: fake_containers.calls)
    _image, kwargs = fake_containers.calls[0]
    assert kwargs.get("ports") == {"80/tcp": 8080}

def test_ecs_service_registers_tasks_in_target_group(monkeypatch):
    """A service with loadBalancers must register its running tasks as targets.

    Without this the ALB data plane has nothing to forward to and every request
    falls through to the listener's default action.
    """
    from ministack.services import alb as _alb
    from ministack.services import ecs as _ecs

    task_ip = "172.30.0.7"

    class FakeContainer:
        def __init__(self, cid):
            self.id = cid
            self.attrs = {"NetworkSettings": {"Networks": {"ministack_default": {"IPAddress": task_ip}}}}

        def reload(self):
            pass

    class FakeContainers:
        def __init__(self):
            self.n = 0

        def get(self, _name):
            raise Exception("not found")

        def list(self, *a, **k):
            return []

        def run(self, image, **kwargs):
            self.n += 1
            return FakeContainer(f"container-{self.n:012d}")

    images = SimpleNamespace(
        get_registry_data=lambda image: SimpleNamespace(id="sha256:" + "a" * 64),
        pull=lambda image, **kwargs: SimpleNamespace(tag=lambda repository, **kwargs: True),
    )
    monkeypatch.setattr(_ecs, "_get_docker", lambda: SimpleNamespace(
        containers=FakeContainers(), images=images,
    ))

    tg_arn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg-reg/abc123"
    _alb._tgs[tg_arn] = {"TargetGroupArn": tg_arn, "Port": 80, "TargetType": "ip"}
    _alb._targets[tg_arn] = []

    _ecs._register_task_definition({
        "family": "lb-reg-td",
        "networkMode": "awsvpc",
        "containerDefinitions": [{"name": "web", "image": "busybox"}],
    })
    _ecs._create_service({
        "cluster": "lb-reg-c",
        "serviceName": "lb-reg-svc",
        "taskDefinition": "lb-reg-td",
        "desiredCount": 1,
        "loadBalancers": [
            {"targetGroupArn": tg_arn, "containerName": "web", "containerPort": 80},
        ],
    })

    _wait_until(lambda: _alb._targets.get(tg_arn) == [{"Id": task_ip, "Port": 80}])
    registered = _alb._targets.get(tg_arn, [])
    assert registered == [{"Id": task_ip, "Port": 80}], registered

    # ...and deleting the service leaves nothing registered behind it.
    _ecs._delete_service({"cluster": "lb-reg-c", "service": "lb-reg-svc", "force": True})
    assert _alb._targets.get(tg_arn) == []


def test_ecs_task_records_private_ipv4_attachment(monkeypatch):
    """awsvpc tasks expose their address the way real ECS does."""
    from ministack.services import ecs as _ecs

    class FakeContainer:
        id = "container-000000000001"
        attrs = {"NetworkSettings": {"Networks": {"ministack_default": {"IPAddress": "172.30.0.9"}}}}

        def reload(self):
            pass

    class FakeContainers:
        def get(self, _name):
            raise Exception("not found")

        def list(self, *a, **k):
            return []

        def run(self, image, **kwargs):
            return FakeContainer()

    monkeypatch.setattr(_ecs, "_get_docker", lambda: SimpleNamespace(containers=FakeContainers()))

    _ecs._register_task_definition({
        "family": "eni-td",
        "networkMode": "awsvpc",
        "containerDefinitions": [{"name": "web", "image": "busybox"}],
    })
    resp = _ecs._run_task({"cluster": "eni-c", "taskDefinition": "eni-td"})
    _status, _headers, raw = resp
    task = json.loads(raw)["tasks"][0]

    _wait_until(lambda: _ecs._task_ip(_ecs._tasks.get(task["taskArn"])) == "172.30.0.9")
    task = _ecs._tasks[task["taskArn"]]
    assert _ecs._task_ip(task) == "172.30.0.9"
    # Not a member of the Task shape; the emulator used to invent it.
    assert "attachmentsStatus" not in task
    assert task["attachments"][0]["type"] == "ElasticNetworkInterface"


def _eni_probe_docker(ip):
    class FakeContainer:
        id = "container-000000000002"
        attrs = {"NetworkSettings": {"Networks": {"ministack_default": {"IPAddress": ip}}}}

        def reload(self):
            pass

    class FakeContainers:
        def get(self, _name):
            raise Exception("not found")

        def list(self, *a, **k):
            return []

        def run(self, image, **kwargs):
            return FakeContainer()

    class FakeImages:
        def get_registry_data(self, image):
            return SimpleNamespace(id="sha256:" + "a" * 64)

        def pull(self, image, **kwargs):
            return SimpleNamespace(tag=lambda repository, **kwargs: True)

    return SimpleNamespace(containers=FakeContainers(), images=FakeImages())


def test_ecs_awsvpc_attachment_carries_the_subnet_it_was_placed_in(monkeypatch):
    """The attachment names the subnet the request asked for, one of the members
    the Attachment reference lists for an elastic network interface."""
    from ministack.services import ecs as _ecs

    monkeypatch.setattr(_ecs, "_get_docker", lambda: _eni_probe_docker("172.30.0.11"))
    _ecs._register_task_definition({
        "family": "eni-subnet-td",
        "networkMode": "awsvpc",
        "containerDefinitions": [{"name": "web", "image": "busybox"}],
    })
    task_arn = json.loads(_ecs._run_task({
        "cluster": "eni-subnet-c",
        "taskDefinition": "eni-subnet-td",
        "networkConfiguration": {"awsvpcConfiguration": {
            "subnets": ["subnet-0a1b2c3d", "subnet-9f8e7d6c"],
        }},
    })[2])["tasks"][0]["taskArn"]

    _wait_until(lambda: _ecs._tasks[task_arn].get("attachments"))
    details = {d["name"]: d["value"]
               for d in _ecs._tasks[task_arn]["attachments"][0]["details"]}
    assert details["subnetId"] == "subnet-0a1b2c3d"
    assert details["privateIPv4Address"] == "172.30.0.11"


def test_ecs_cloudformation_service_places_and_registers_its_tasks(monkeypatch):
    """A CloudFormation service's tasks get the template's subnet and join its target group."""
    from ministack.services import alb as _alb
    from ministack.services import ecs as _ecs
    from ministack.services.cloudformation.provisioners import _RESOURCE_HANDLERS

    monkeypatch.setattr(_ecs, "_get_docker", lambda: _eni_probe_docker("172.30.0.41"))
    tg_arn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg-cfncase/abc123"
    _alb._tgs[tg_arn] = {"TargetGroupArn": tg_arn, "Port": 80, "TargetType": "ip"}
    _alb._targets[tg_arn] = []
    _ecs._register_task_definition({
        "family": "eni-cfncase-td",
        "networkMode": "awsvpc",
        "containerDefinitions": [{"name": "web", "image": "busybox"}],
    })
    _RESOURCE_HANDLERS["AWS::ECS::Service"]["create"]("Service", {
        "Cluster": "eni-cfncase-c",
        "ServiceName": "eni-cfncase-svc",
        "TaskDefinition": "eni-cfncase-td",
        "DesiredCount": 1,
        "NetworkConfiguration": {"AwsvpcConfiguration": {"Subnets": ["subnet-cfn00001"]}},
        "LoadBalancers": [{"TargetGroupArn": tg_arn, "ContainerName": "web", "ContainerPort": 80}],
    }, "eni-cfncase")
    _wait_until(lambda: _alb._targets.get(tg_arn) == [{"Id": "172.30.0.41", "Port": 80}])
    task = next(t for t in _ecs._tasks.values() if t.get("group") == "service:eni-cfncase-svc")
    details = {d["name"]: d["value"] for d in task["attachments"][0]["details"]}
    assert details["subnetId"] == "subnet-cfn00001"
    _ecs._delete_service({"cluster": "eni-cfncase-c", "service": "eni-cfncase-svc", "force": True})


@pytest.mark.parametrize("network_configuration", [
    None,
    {},
    {"awsvpcConfiguration": {}},
    {"awsvpcConfiguration": {"subnets": []}},
])
def test_ecs_awsvpc_attachment_without_a_usable_subnet(monkeypatch, network_configuration):
    """A guard: the request's network configuration is not read anywhere else,
    so a body that leaves it out or leaves it empty must still start the task
    and report the attachment, just without a subnet."""
    from ministack.services import ecs as _ecs

    uid = _uuid_mod.uuid4().hex[:8]
    monkeypatch.setattr(_ecs, "_get_docker", lambda: _eni_probe_docker("172.30.0.13"))
    _ecs._register_task_definition({
        "family": f"eni-nosubnet-td-{uid}",
        "networkMode": "awsvpc",
        "containerDefinitions": [{"name": "web", "image": "busybox"}],
    })
    request = {"cluster": "eni-nosubnet-c", "taskDefinition": f"eni-nosubnet-td-{uid}"}
    if network_configuration is not None:
        request["networkConfiguration"] = network_configuration
    task_arn = json.loads(_ecs._run_task(request)[2])["tasks"][0]["taskArn"]

    _wait_until(lambda: _ecs._tasks[task_arn].get("attachments"))
    details = {d["name"]: d["value"]
               for d in _ecs._tasks[task_arn]["attachments"][0]["details"]}
    assert details == {"privateIPv4Address": "172.30.0.13"}


def test_ecs_restore_stops_a_running_task_and_counts_it(monkeypatch):
    """A restart stops every restored task, which is a change to the record:
    without the bump a consumer's pre-restart copy still compares equal to a
    task that is no longer running.

    The saved state also drops the address the container had. That key is what
    the live target-group sync reads, and the container it named is gone with
    the process. The attachment keeps its own copy, which is right: AWS reports
    a stopped task's attachment too, with the interface already DELETED.
    """
    from ministack.services import ecs as _ecs

    container = _version_probe_container("restore-version-container")
    container.attrs = {"NetworkSettings": {"Networks": {"n": {"IPAddress": "172.30.0.31"}}}}
    monkeypatch.setattr(_ecs, "_get_docker", lambda: _version_probe_docker(container))
    _ecs._register_task_definition({
        "family": "restore-version-td",
        "networkMode": "bridge",
        "containerDefinitions": [{"name": "app", "image": "busybox"}],
    })
    task_arn = json.loads(_ecs._run_task({
        "cluster": "restore-version-c",
        "taskDefinition": "restore-version-td",
    })[2])["tasks"][0]["taskArn"]
    _wait_until(lambda: _ecs._tasks[task_arn]["lastStatus"] == "RUNNING")
    _wait_until(lambda: _ecs._tasks[task_arn].get("_container_ip"))
    running_version = _ecs._tasks[task_arn]["version"]

    state = _ecs.get_state()
    saved = [t for t in state["tasks"]._data.values() if t["taskArn"] == task_arn][0]
    assert _ecs._tasks[task_arn]["_container_ip"] == "172.30.0.31"
    assert "_container_ip" not in saved
    _ecs.reset()
    _ecs.load_persisted_state(state)

    restored = _ecs._tasks[task_arn]
    assert restored["lastStatus"] == "STOPPED"
    assert restored["version"] == running_version + 1

    # Restoring an already stopped task is not a transition.
    state = _ecs.get_state()
    _ecs.reset()
    _ecs.load_persisted_state(state)
    assert _ecs._tasks[task_arn]["version"] == running_version + 1


def test_ecs_bridge_task_reports_no_eni_attachment(monkeypatch):
    """`attachments` is documented as the adapter a task has "if the task uses
    the awsvpc network mode", so a bridge task reports none. Its address is still
    known, which is what the target-group sync reads."""
    from ministack.services import ecs as _ecs

    monkeypatch.setattr(_ecs, "_get_docker", lambda: _eni_probe_docker("172.30.0.12"))
    _ecs._register_task_definition({
        "family": "eni-bridge-td",
        "networkMode": "bridge",
        "containerDefinitions": [{"name": "web", "image": "busybox"}],
    })
    task_arn = json.loads(_ecs._run_task({
        "cluster": "eni-bridge-c",
        "taskDefinition": "eni-bridge-td",
    })[2])["tasks"][0]["taskArn"]

    _wait_until(lambda: _ecs._task_ip(_ecs._tasks[task_arn]) == "172.30.0.12")
    task = _ecs._tasks[task_arn]
    assert task["attachments"] == []
    assert "attachmentsStatus" not in task
    described = json.loads(_ecs._describe_tasks({
        "cluster": "eni-bridge-c",
        "tasks": [task_arn],
    })[2])["tasks"][0]
    assert described["attachments"] == []
    assert "_container_ip" not in described


@pytest.mark.parametrize("mode", ["awsvpc", "bridge"])
def test_ecs_first_container_to_report_an_address_owns_it(mode):
    """A task has one address, not one per container.

    A two-container awsvpc task on AWS reports a single attachment, and both
    containers name that one attachment and that one private address. Each
    container here calls _record_task_ip from its own thread, so without the
    guard a sidecar coming up second would move the task's address, and with it
    the target a load balancer is pointed at.
    """
    from ministack.services import ecs as _ecs

    class FakeContainer:
        def __init__(self, ip):
            self.attrs = {"NetworkSettings": {"Networks": {"n": {"IPAddress": ip}}}}

        def reload(self):
            pass

    task = {
        "taskArn": f"arn:aws:ecs:us-east-1:000000000000:task/c/first-owner-{mode}",
        "_network_mode": mode,
        "_subnet": "subnet-0a1b2c3d",
        "attachments": [],
    }
    _ecs._record_task_ip(task, FakeContainer("172.30.0.21"), "n")
    _ecs._record_task_ip(task, FakeContainer("172.30.0.22"), "n")

    assert _ecs._task_ip(task) == "172.30.0.21"
    assert len(task["attachments"]) == (1 if mode == "awsvpc" else 0)


def test_ecs_sync_service_targets_selects_tasks_by_group(monkeypatch):
    """Target selection must use the same predicate as the service's own recount.

    _reconcile_service_tasks counts a service's tasks by `group`; selecting targets
    by any other field lets the registered targets disagree with the runningCount
    reported next to them.
    """
    from ministack.services import alb as _alb
    from ministack.services import ecs as _ecs

    tg_arn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg-pred/abc"
    _alb._tgs[tg_arn] = {"TargetGroupArn": tg_arn, "Port": 80, "TargetType": "ip"}
    _alb._targets[tg_arn] = []

    svc = {
        "serviceName": "pred-svc",
        "clusterArn": "arn:aws:ecs:us-east-1:000000000000:cluster/pred-c",
        "status": "ACTIVE",
        "loadBalancers": [{"targetGroupArn": tg_arn, "containerName": "web", "containerPort": 80}],
    }

    def _task(ip, **over):
        t = {
            "group": "service:pred-svc",
            "clusterArn": svc["clusterArn"],
            "lastStatus": "RUNNING",
            "attachments": [{
                "type": "ElasticNetworkInterface",
                "details": [{"name": "privateIPv4Address", "value": ip}],
            }],
        }
        t.update(over)
        return t

    monkeypatch.setattr(_ecs, "_tasks", {
        "a": _task("10.0.0.1"),
        # carries the group but no startedBy — still this service's task
        "b": _task("10.0.0.2", startedBy=None),
        # right group, wrong cluster
        "c": _task("10.0.0.3", clusterArn="arn:aws:ecs:us-east-1:000000000000:cluster/other"),
        # right cluster, different service
        "d": _task("10.0.0.4", group="service:someone-else"),
        # this service, not running
        "e": _task("10.0.0.5", lastStatus="STOPPED"),
    })

    _ecs._sync_service_targets("pred-c", svc)
    assert sorted(t["Id"] for t in _alb._targets[tg_arn]) == ["10.0.0.1", "10.0.0.2"]


def test_ecs_sync_service_targets_does_not_publish_a_stale_view(monkeypatch):
    """A slow reconciliation must not overwrite a newer one's registration.

    The failure this guards is last-writer-wins on a stale read: a reconcile that
    sampled the tasks before a scale-in, but publishes after it, would re-register
    the tasks that scale-in removed. Serialising the read and the publish means the
    last write is also the last read, so the final registration matches the final
    task state.
    """
    import threading

    from ministack.services import alb as _alb
    from ministack.services import ecs as _ecs

    tg_arn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg-stale/abc"
    _alb._tgs[tg_arn] = {"TargetGroupArn": tg_arn, "Port": 80, "TargetType": "ip"}
    _alb._targets[tg_arn] = []

    svc = {
        "serviceName": "stale-svc",
        "clusterArn": "arn:aws:ecs:us-east-1:000000000000:cluster/stale-c",
        "status": "ACTIVE",
        "loadBalancers": [{"targetGroupArn": tg_arn, "containerName": "web", "containerPort": 80}],
    }

    def _task(ip):
        return {
            "group": "service:stale-svc",
            "clusterArn": svc["clusterArn"],
            "lastStatus": "RUNNING",
            "attachments": [{
                "type": "ElasticNetworkInterface",
                "details": [{"name": "privateIPv4Address", "value": ip}],
            }],
        }

    before = {ip: _task(ip) for ip in ("10.3.0.1", "10.3.0.2", "10.3.0.3")}
    after = {"10.3.0.1": _task("10.3.0.1")}          # scaled in to one task
    shared = dict(before)
    monkeypatch.setattr(_ecs, "_tasks", shared, raising=False)

    sampled = threading.Event()
    scaled_in = threading.Event()

    real_task_ip = _ecs._task_ip
    slowed = {"done": False}

    def slow_task_ip(task):
        # Let the first reconcile sample the pre-scale-in state, then stall it
        # until the scale-in has happened and been published by the other thread.
        if not slowed["done"]:
            slowed["done"] = True
            sampled.set()
            scaled_in.wait(timeout=5)
        return real_task_ip(task)

    monkeypatch.setattr(_ecs, "_task_ip", slow_task_ip)

    slow = threading.Thread(target=_ecs._sync_service_targets, args=("stale-c", svc))
    slow.start()

    assert sampled.wait(timeout=5), "the slow reconcile never sampled"
    shared.clear()
    shared.update(after)
    _ecs._sync_service_targets("stale-c", svc)   # the newer, post-scale-in view
    scaled_in.set()
    slow.join(timeout=10)

    final = sorted(t["Id"] for t in _alb._targets[tg_arn])
    assert final == ["10.3.0.1"], f"a stale view was published: {final}"


def test_ecs_restored_services_relaunch_their_tasks(monkeypatch):
    """A service must satisfy desiredCount again after a restore — in every
    account and region, not only the default.

    restore_state marks every restored task STOPPED, because its container is
    gone with the process that ran it. Nothing then reconciles the services, so
    a restarted ministack reports runningCount from the persisted record while
    no container exists, and the load balancer keeps forwarding to addresses
    nothing is listening on. Real ECS relaunches: the service scheduler exists
    to keep desiredCount satisfied.

    The reconciler runs on a daemon thread with no request scope, so it must
    walk the stores with all_items() and pin each service's own account and
    region — a plain .items() only ever saw the default tenant, and a service
    persisted under any other account or region never came back.
    """
    from ministack.core.responses import (
        AccountRegionScopedDict,
        get_account_id,
        get_region,
    )
    from ministack.services import ecs as _ecs

    launched = []

    def _capture_run_task(data):
        # Record the scope the reconciler pinned: the spawned task must land in
        # the service's own account and region.
        launched.append({**data, "_scope": (get_account_id(), get_region())})

    monkeypatch.setattr(_ecs, "_run_task", _capture_run_task)
    monkeypatch.setattr(_ecs, "_clusters", {"c1": {"clusterName": "c1"}})

    task_defs = AccountRegionScopedDict()
    task_defs.set_scoped(
        "000000000000", "us-east-1", "web:1",
        {"taskDefinitionArn": "arn:aws:ecs:us-east-1:000000000000:task-definition/web:1",
         "family": "web"})
    task_defs.set_scoped(
        "222222222222", "eu-west-1", "api:1",
        {"taskDefinitionArn": "arn:aws:ecs:eu-west-1:222222222222:task-definition/api:1",
         "family": "api"})
    monkeypatch.setattr(_ecs, "_task_defs", task_defs)

    services = AccountRegionScopedDict()
    services.set_scoped(
        "000000000000", "us-east-1", "c1/web",
        {"serviceName": "web", "status": "ACTIVE", "desiredCount": 2,
         "taskDefinition": "arn:aws:ecs:us-east-1:000000000000:task-definition/web:1",
         "clusterArn": "arn:cluster/c1",
         "launchType": "FARGATE", "deployments": [{"runningCount": 2}]})
    # An inactive service must not be relaunched.
    services.set_scoped(
        "000000000000", "us-east-1", "c1/old",
        {"serviceName": "old", "status": "INACTIVE", "desiredCount": 3,
         "taskDefinition": "arn:aws:ecs:us-east-1:000000000000:task-definition/web:1",
         "clusterArn": "arn:cluster/c1",
         "launchType": "FARGATE", "deployments": []})
    # A service persisted by another tenant, in another region.
    services.set_scoped(
        "222222222222", "eu-west-1", "c2/api",
        {"serviceName": "api", "status": "ACTIVE", "desiredCount": 1,
         "taskDefinition": "arn:aws:ecs:eu-west-1:222222222222:task-definition/api:1",
         "clusterArn": "arn:cluster/c2",
         "launchType": "FARGATE", "deployments": [{"runningCount": 1}]})
    monkeypatch.setattr(_ecs, "_services", services)

    # What restore_state leaves behind: the tasks exist but are STOPPED.
    tasks = AccountRegionScopedDict()
    tasks.set_scoped(
        "000000000000", "us-east-1", "arn:task/1",
        {"group": "service:web", "clusterArn": "arn:cluster/c1",
         "lastStatus": "STOPPED",
         "taskDefinitionArn": "arn:aws:ecs:us-east-1:000000000000:task-definition/web:1"})
    tasks.set_scoped(
        "000000000000", "us-east-1", "arn:task/2",
        {"group": "service:web", "clusterArn": "arn:cluster/c1",
         "lastStatus": "STOPPED",
         "taskDefinitionArn": "arn:aws:ecs:us-east-1:000000000000:task-definition/web:1"})
    monkeypatch.setattr(_ecs, "_tasks", tasks)

    _ecs._reconcile_restored_services()

    by_group = {c["group"]: c for c in launched}
    assert "service:old" not in by_group
    assert set(by_group) == {"service:web", "service:api"}, \
        f"expected both ACTIVE services relaunched, got {launched}"

    web = by_group["service:web"]
    assert web["count"] == 2, "both stopped tasks must be replaced"
    assert web["_scope"] == ("000000000000", "us-east-1")

    api = by_group["service:api"]
    assert api["count"] == 1
    assert api["_scope"] == ("222222222222", "eu-west-1"), \
        "the relaunch must run pinned to the service's own account and region"
def test_ecs_service_reconcile_spares_foreign_targets(monkeypatch):
    """A service withdraws only its own registrations: targets registered by
    hand (or by another service sharing the group) survive its reconcile and
    its deletion — real ECS deregisters only its own tasks."""
    from ministack.services import alb as _alb
    from ministack.services import ecs as _ecs

    task_ip = "172.30.0.21"

    class FakeContainer:
        def __init__(self, cid):
            self.id = cid
            self.attrs = {"NetworkSettings": {"Networks": {"ministack_default": {"IPAddress": task_ip}}}}

        def reload(self):
            pass

    class FakeContainers:
        def __init__(self):
            self.n = 0

        def get(self, _name):
            raise Exception("not found")

        def list(self, *a, **k):
            return []

        def run(self, image, **kwargs):
            self.n += 1
            return FakeContainer(f"container-{self.n:012d}")

    images = SimpleNamespace(
        get_registry_data=lambda image: SimpleNamespace(id="sha256:" + "a" * 64),
        pull=lambda image, **kwargs: SimpleNamespace(tag=lambda repository, **kwargs: True),
    )
    monkeypatch.setattr(_ecs, "_get_docker", lambda: SimpleNamespace(
        containers=FakeContainers(), images=images,
    ))

    tg_arn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/tg-shared/def456"
    _alb._tgs[tg_arn] = {"TargetGroupArn": tg_arn, "Port": 80, "TargetType": "ip"}
    # A registration the service does not own.
    _alb._targets[tg_arn] = [{"Id": "10.9.9.9", "Port": 80}]

    _ecs._register_task_definition({
        "family": "lb-shared-td",
        "networkMode": "awsvpc",
        "containerDefinitions": [{"name": "web", "image": "busybox"}],
    })
    _ecs._create_service({
        "cluster": "lb-shared-c",
        "serviceName": "lb-shared-svc",
        "taskDefinition": "lb-shared-td",
        "desiredCount": 1,
        "loadBalancers": [
            {"targetGroupArn": tg_arn, "containerName": "web", "containerPort": 80},
        ],
    })
    _wait_until(
        lambda: _alb._targets.get(tg_arn) == [
            {"Id": "10.9.9.9", "Port": 80},
            {"Id": task_ip, "Port": 80},
        ]
    )
    registered = sorted(t["Id"] for t in _alb._targets.get(tg_arn, []))
    assert registered == ["10.9.9.9", task_ip], registered

    _ecs._delete_service({"cluster": "lb-shared-c", "service": "lb-shared-svc", "force": True})
    assert _alb._targets.get(tg_arn) == [{"Id": "10.9.9.9", "Port": 80}]
@pytest.mark.parametrize(
    "cpu_architecture,expected_platform",
    [
        ("ARM64", "linux/arm64"),
        ("X86_64", "linux/amd64"),
        (None, None),
    ],
)
def test_ecs_task_runs_on_its_declared_runtime_platform(cpu_architecture, expected_platform):
    """A task definition's runtimePlatform decides the container's platform.

    It was stored and never read, so Docker chose the host's architecture. An
    ARM64 task definition on an x86_64 host then started a container that could
    not execute its own entrypoint — and the task still reported as started,
    because creating the container is the part that succeeds.

    An absent runtimePlatform must stay absent rather than defaulting to the
    host explicitly, so existing single-architecture setups are untouched.
    """
    td = {"networkMode": "bridge"}
    if cpu_architecture:
        td["runtimePlatform"] = {"cpuArchitecture": cpu_architecture}

    kwargs = ecs_service._build_run_kwargs(
        {"name": "app", "image": "busybox:latest"},
        td,
        {},                      # env
        {},                      # port_bindings
        None,                    # ecs_network
        False,                   # host_mode
        "task1234",              # task_id
        "arn:aws:ecs:us-east-1:000000000000:task/c/task1234",
        None,                    # ministack_net_ip
        "arn:aws:ecs:us-east-1:000000000000:cluster/c",
    )

    assert kwargs.get("platform") == expected_platform


# ---- Forced rolling deployments (simulated workers, no Docker) ----
def _decoded(response):
    assert response[0] == 200, response
    return json.loads(response[2])


@pytest.fixture
def service(monkeypatch):
    monkeypatch.setattr(ecs_service, "_get_docker", lambda: None)
    cluster = f"force-{_uuid_mod.uuid4().hex[:8]}"
    td = _decoded(ecs_service._register_task_definition({
        "family": cluster,
        "containerDefinitions": [{"name": "app", "image": "example.invalid/app:latest"}],
    }))["taskDefinition"]
    _decoded(ecs_service._create_service({
        "cluster": cluster, "serviceName": "app",
        "taskDefinition": td["taskDefinitionArn"], "desiredCount": 2,
    }))
    svc = ecs_service._services[f"{cluster}/app"]
    yield cluster, svc
    for arn, task in list(ecs_service._tasks.items()):
        if task["clusterArn"] == svc["clusterArn"]:
            ecs_service._tasks.pop(arn)
    ecs_service._services.pop(f"{cluster}/app", None)
    ecs_service._clusters.pop(cluster, None)
    for key, task_def in list(ecs_service._task_defs.items()):
        if task_def["family"] == cluster:
            ecs_service._task_defs.pop(key)
    ecs_service._task_def_latest.pop(cluster, None)


def _tasks(svc, status=None, deployment=None):
    return {
        arn: task for arn, task in ecs_service._tasks.items()
        if task["clusterArn"] == svc["clusterArn"]
        and (status is None or task["lastStatus"] == status)
        and (deployment is None or task.get("_deployment_id") == deployment["id"])
    }


def _update(cluster, **kwargs):
    return _decoded(ecs_service._update_service({"cluster": cluster, "service": "app", **kwargs}))["service"]


@pytest.mark.parametrize("definition", [None, "arn", "family_revision"])
def test_ecs_force_same_definition_replaces_tasks(service, definition):
    cluster, svc = service
    old_id = ecs_service._primary_deployment(svc)["id"]
    old_tasks = set(_tasks(svc, "RUNNING"))
    request = {"forceNewDeployment": True}
    if definition:
        request["taskDefinition"] = (
            svc["taskDefinition"] if definition == "arn" else f"{cluster}:1"
        )
    response = _update(cluster, **request)
    assert response["taskDefinition"] == svc["taskDefinition"]
    assert len(response["deployments"]) == 1
    primary = response["deployments"][0]
    assert primary["id"] != old_id
    assert primary["rolloutState"] == "COMPLETED"
    assert primary["runningCount"] == response["runningCount"] == 2
    assert primary["pendingCount"] == 0
    assert set(_tasks(svc, "RUNNING")).isdisjoint(old_tasks)
    assert set(_tasks(svc, "STOPPED")) == old_tasks


@pytest.mark.parametrize("explicit_definition", [False, True])
@pytest.mark.parametrize("force", [None, False])
def test_ecs_unchanged_update_does_not_deploy(service, explicit_definition, force):
    cluster, svc = service
    deployment = ecs_service._primary_deployment(svc)
    tasks = set(_tasks(svc))
    request = {}
    if explicit_definition:
        request["taskDefinition"] = f"{cluster}:1"
    if force is not None:
        request["forceNewDeployment"] = force
    _update(cluster, **request)
    assert ecs_service._primary_deployment(svc) is deployment
    assert set(_tasks(svc)) == tasks


@pytest.mark.parametrize("force", [False, True])
def test_ecs_changed_definition_creates_one_deployment(service, force):
    cluster, svc = service
    old_tasks = set(_tasks(svc))
    td = _decoded(ecs_service._register_task_definition({
        "family": cluster,
        "containerDefinitions": [{"name": "app", "image": "example.invalid/app:v2"}],
    }))["taskDefinition"]["taskDefinitionArn"]
    _update(cluster, taskDefinition=td, forceNewDeployment=force)
    assert len(svc["deployments"]) == 1
    assert svc["deployments"][0]["taskDefinition"] == td
    assert set(_tasks(svc, "RUNNING")).isdisjoint(old_tasks)
    assert all(task["taskDefinitionArn"] == td for task in _tasks(svc, "RUNNING").values())


def test_ecs_repeated_force_uses_distinct_task_and_deployment_identities(service):
    cluster, svc = service
    deployment_ids = {ecs_service._primary_deployment(svc)["id"]}
    seen_tasks = set(_tasks(svc))
    for _ in range(3):
        _update(cluster, forceNewDeployment=True)
        primary = ecs_service._primary_deployment(svc)
        assert primary["id"] not in deployment_ids
        deployment_ids.add(primary["id"])
        current = set(_tasks(svc, "RUNNING"))
        assert len(current) == 2
        assert current.isdisjoint(seen_tasks)
        seen_tasks.update(current)


def test_ecs_force_at_zero_desired_and_scale_afterwards(service):
    cluster, svc = service
    _update(cluster, desiredCount=0)
    old_id = ecs_service._primary_deployment(svc)["id"]
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    assert primary["id"] != old_id
    assert primary["rolloutState"] == "COMPLETED"
    assert primary["runningCount"] == primary["pendingCount"] == 0
    assert not _tasks(svc, "RUNNING")
    _update(cluster, desiredCount=2)
    assert ecs_service._primary_deployment(svc) is primary
    assert len(_tasks(svc, "RUNNING", primary)) == 2


@pytest.mark.parametrize("scale_first", [False, True])
def test_ecs_docker_force_at_zero_drains_predecessors_without_a_startup_callback(
        service, pending_launcher, scale_first):
    cluster, svc = service
    old_tasks = set(_tasks(svc))
    if scale_first:
        _update(cluster, desiredCount=0)
    _update(cluster, desiredCount=0, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    assert primary["rolloutState"] == "COMPLETED"
    assert svc["runningCount"] == svc["pendingCount"] == 0
    assert svc["deployments"] == [primary]
    assert set(_tasks(svc, "STOPPED")) == old_tasks
    assert pending_launcher == []
    _update(cluster, desiredCount=2)
    assert ecs_service._primary_deployment(svc) is primary
    assert len(_tasks(svc, deployment=primary)) == 2
    assert pending_launcher == [2]


@pytest.mark.parametrize("crash", [False, True])
def test_ecs_first_task_callback_cannot_approve_a_newly_running_second_task(
        service, pending_launcher, monkeypatch, crash):
    from ministack.core.responses import get_account_id, get_region

    cluster, svc = service
    old = ecs_service._primary_deployment(svc)
    old_tasks = set(_tasks(svc, "RUNNING"))
    callbacks = []
    monkeypatch.setattr(ecs_service, "spawn_background", lambda callback, **kwargs: callbacks.append(callback))
    monkeypatch.setattr(ecs_service.time, "sleep", lambda delay: None)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "deploymentCircuitBreaker": {
            "enable": True, "rollback": True,
            "thresholdConfiguration": {"type": "COUNT", "value": 1},
        },
    })
    primary = ecs_service._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    primary["_image_digests"] = {"app": "sha256:" + "a" * 64}
    ecs_service._mark_task_running(first["taskArn"], first)
    ecs_service._schedule_service_deployment_completion(
        cluster, f"{cluster}/app", get_account_id(), get_region(), healthy_task=first)
    launch = ecs_service._run_task

    def start_during_reconcile(request):
        response = launch(request)
        for task in _decoded(response)["tasks"]:
            second = ecs_service._tasks[task["taskArn"]]
            ecs_service._mark_task_running(second["taskArn"], second)
            ecs_service._schedule_service_deployment_completion(
                cluster, f"{cluster}/app", get_account_id(), get_region(), healthy_task=second)
        return response

    monkeypatch.setattr(ecs_service, "_run_task", start_during_reconcile)
    callbacks[0]()
    second = next(task for task in _tasks(svc, deployment=primary).values()
                  if task is not first)
    assert primary["runningCount"] == 2
    assert primary["rolloutState"] == "IN_PROGRESS"
    assert old_tasks <= set(_tasks(svc, "RUNNING"))
    if crash:
        ecs_service._mark_task_stopped(second["taskArn"], second,
                              "Essential container exited", "EssentialContainerExited", 1)
        callbacks[1]()  # A stopped task's late callback cannot certify a retry.
        assert primary["rolloutState"] == "FAILED"
        assert primary["failedTasks"] == 1
        assert ecs_service._primary_deployment(svc) is old
        callbacks[-1]()
        assert old["rolloutState"] == "COMPLETED"
        assert set(_tasks(svc, "RUNNING")) == old_tasks
    else:
        callbacks[1]()
        assert primary["rolloutState"] == "COMPLETED"
        assert old_tasks == set(_tasks(svc, "STOPPED"))
        assert len(_tasks(svc, "RUNNING", primary)) == 2


@pytest.fixture
def pending_launcher(monkeypatch):
    original_run = ecs_service._run_task
    calls = []

    def launch(request):
        # Production registration, with only the asynchronous Docker worker
        # replaced. PROVISIONING is already reserved scheduler capacity.
        with monkeypatch.context() as patch:
            patch.setattr(ecs_service, "_get_docker", lambda: None)
            response = original_run(request)
        for task in _decoded(response)["tasks"]:
            ecs_service._tasks[task["taskArn"]]["lastStatus"] = "PROVISIONING"
            ecs_service._tasks[task["taskArn"]]["_startup_stable"] = False
        calls.append(request["count"])
        return response

    monkeypatch.setattr(ecs_service, "_get_docker", lambda: object())
    monkeypatch.setattr(ecs_service, "_run_task", launch)
    return calls


@pytest.mark.parametrize("pending_status", ["PROVISIONING", "PENDING", "ACTIVATING"])
def test_ecs_force_keeps_old_tasks_until_stable_and_reserves_pending_capacity(
        service, pending_launcher, pending_status):
    cluster, svc = service
    old = ecs_service._primary_deployment(svc)
    old_tasks = set(_tasks(svc, "RUNNING"))
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    current = _tasks(svc, deployment=primary)
    assert len(current) == 1
    assert old["runningCount"] == 2
    assert primary["runningCount"] == 0
    for task in current.values():
        task["lastStatus"] = pending_status
    for _ in range(3):
        ecs_service._reconcile_service_tasks(cluster, f"{cluster}/app")
    assert pending_launcher == [1]
    assert old_tasks <= set(_tasks(svc, "RUNNING"))
    assert primary["pendingCount"] == (1 if pending_status == "PENDING" else 0)
    assert primary["rolloutState"] == "IN_PROGRESS"
    for task in current.values():
        td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
        ecs_service._prepare_service_images(task, td, SimpleNamespace(images=SimpleNamespace(
            get_registry_data=lambda image: SimpleNamespace(id="sha256:" + "a" * 64),
        )))
        ecs_service._mark_task_running(task["taskArn"], task)
    ecs_service._refresh_service_state(cluster, "service:app")
    # RUNNING registration alone does not drain the predecessor. The worker's
    # steady-state callback invokes completion after its existing grace window.
    assert old_tasks <= set(_tasks(svc, "RUNNING"))
    ecs_service._reconcile_service_tasks(cluster, f"{cluster}/app")
    current = _tasks(svc, deployment=primary)
    assert len(current) == 2
    for task in current.values():
        task["lastStatus"] = "RUNNING"
        ecs_service._record_service_task_healthy(f"{cluster}/app", task)
    ecs_service._refresh_service_state(cluster, "service:app")
    ecs_service._complete_service_deployment(cluster, f"{cluster}/app")
    assert set(_tasks(svc, "RUNNING")) == set(current)
    assert old_tasks == set(_tasks(svc, "STOPPED"))
    assert svc["deployments"] == [primary]


@pytest.mark.parametrize("minimum,maximum,spawned,stopped", [(100, 100, 0, 0), (50, 100, 1, 1), (100, 150, 1, 0)])
def test_ecs_force_respects_rolling_capacity_limits(
        service, pending_launcher, minimum, maximum, spawned, stopped):
    cluster, svc = service
    old = ecs_service._primary_deployment(svc)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "minimumHealthyPercent": minimum, "maximumPercent": maximum,
    })
    primary = ecs_service._primary_deployment(svc)
    assert len(_tasks(svc, deployment=primary)) == spawned
    assert len(_tasks(svc, "STOPPED", old)) == stopped
    assert len(_tasks(svc, "RUNNING")) >= (2 * minimum + 99) // 100
    live = sum(task["lastStatus"] in ecs_service._PRE_STOP_STATUSES for task in _tasks(svc).values())
    assert live <= 2 * maximum // 100


def test_ecs_force_attributes_legacy_tasks_before_same_definition_is_ambiguous(service, pending_launcher):
    cluster, svc = service
    old = ecs_service._primary_deployment(svc)
    legacy = list(_tasks(svc).values())
    for task in legacy:
        task.pop("_deployment_id")
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    assert all(task["_deployment_id"] == old["id"] for task in legacy)
    assert old["runningCount"] == 2
    assert primary["runningCount"] == 0
    assert len(_tasks(svc, deployment=primary)) == 1


@pytest.mark.parametrize("rollback", [False, True])
def test_ecs_same_definition_failure_stops_retries_and_rolls_back_by_deployment(
        service, pending_launcher, monkeypatch, rollback):
    cluster, svc = service
    old = ecs_service._primary_deployment(svc)
    old_tasks = set(_tasks(svc))
    monkeypatch.setattr(ecs_service, "_schedule_service_deployment_completion", lambda *args, **kwargs: None)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "maximumPercent": 200, "minimumHealthyPercent": 100,
        "deploymentCircuitBreaker": {"enable": True, "rollback": rollback},
    })
    failed = ecs_service._primary_deployment(svc)
    for _ in range(3):
        task = next(task for task in _tasks(svc, deployment=failed).values()
                    if task["lastStatus"] in ecs_service._PRE_STOP_STATUSES)
        task["lastStatus"] = "STOPPED"
        ecs_service._record_service_task_failure(task)
    assert failed["rolloutState"] == "FAILED"
    assert failed["failedTasks"] == 3
    if rollback:
        assert ecs_service._primary_deployment(svc) is old
        ecs_service._refresh_service_state(cluster, "service:app")
        ecs_service._complete_service_deployment(cluster, f"{cluster}/app")
        assert set(_tasks(svc, "RUNNING")) == old_tasks
        assert old["rolloutState"] == "COMPLETED"
    else:
        assert ecs_service._primary_deployment(svc) is failed
        calls_before = list(pending_launcher)
        ecs_service._reconcile_service_tasks(cluster, f"{cluster}/app")
        assert pending_launcher == calls_before


class _Images:
    def __init__(self):
        self.digest = "sha256:" + "a" * 64
        self.registry_calls = []
        self.pulls = []
        self.cached = []
        self.tags = []
        self.manifest_error = False
        self.pull_error = False
        self.cache_missing = False

    def get_registry_data(self, image, **kwargs):
        self.registry_calls.append(image)
        if self.manifest_error:
            raise RuntimeError("registry unavailable")
        return SimpleNamespace(id=self.digest)

    def get(self, image):
        if self.cache_missing:
            from docker.errors import ImageNotFound
            raise ImageNotFound("cached image absent")
        return SimpleNamespace(attrs={"RepoDigests": list(self.cached)})

    def pull(self, image, **kwargs):
        self.pulls.append((image, kwargs))
        if self.pull_error:
            raise RuntimeError("image pull failed")
        def tag(repository, tag=None):
            self.tags.append((repository, tag))
            return True
        return SimpleNamespace(id="sha256:config-is-not-a-manifest", attrs={}, tag=tag)


@pytest.fixture
def docker_worker(monkeypatch):
    """Real ECS worker + Docker SDK run path; simulated engine/registry only."""
    from docker.models.containers import ContainerCollection

    images = _Images()
    created = []
    client = SimpleNamespace(images=images, api=SimpleNamespace(_version="1.45"))
    containers = ContainerCollection(client)

    def create(**kwargs):
        created.append(kwargs)
        return SimpleNamespace(
            id=f"container-{len(created)}", status="running",
            attrs={"NetworkSettings": {"Networks": {}}},
            start=lambda: None, reload=lambda: None,
            stop=lambda **kwargs: None, remove=lambda **kwargs: None,
        )

    containers.create = create
    containers.get = lambda name: (_ for _ in ()).throw(RuntimeError("not found"))
    client.containers = containers
    monkeypatch.setattr(ecs_service, "_start_awslogs_forwarder", lambda *args: None)
    yield client, created
    # Workers register metadata even though no containers exist on a daemon.
    from ministack.services import ecs_metadata
    for task in list(ecs_service._tasks.values()):
        for token in task.pop("_metadata_tokens", []):
            ecs_metadata.unregister_token(token)


def _start_worker(svc, task, client):
    td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
    ecs_service._start_task_worker(task, td, [], client)
    return task


def test_ecs_force_refreshes_manifest_and_pins_following_tasks(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    old = ecs_service._primary_deployment(svc)
    old["_image_digests"] = {"app": "sha256:" + "0" * 64}
    old_tasks = set(_tasks(svc))
    client.images.digest = "sha256:" + "b" * 64
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, first, client)
    assert primary["_image_digests"] == {"app": client.images.digest}
    # Changing the tag again between tasks must not change this deployment.
    client.images.digest = "sha256:" + "c" * 64
    ecs_service._reconcile_service_tasks(cluster, f"{cluster}/app")
    second = next(task for arn, task in _tasks(svc, deployment=primary).items()
                  if arn != first["taskArn"])
    _start_worker(svc, second, client)
    assert client.images.registry_calls == ["example.invalid/app:latest"]
    expected = "example.invalid/app@sha256:" + "b" * 64
    assert [kwargs["image"] for kwargs in created] == [expected, expected]
    assert [image for image, kwargs in client.images.pulls] == [expected, expected]
    assert all(task["containers"][0]["image"] == "example.invalid/app:latest"
               for task in (first, second))
    for task in (first, second):
        ecs_service._record_service_task_healthy(f"{cluster}/app", task)
    ecs_service._complete_service_deployment(cluster, f"{cluster}/app")
    assert set(_tasks(svc, "RUNNING")) == {first["taskArn"], second["taskArn"]}
    assert old_tasks == set(_tasks(svc, "STOPPED"))
    # Normal scaling keeps the established deployment's manifest, too.
    _update(cluster, desiredCount=3)
    third = next(task for task in _tasks(svc, deployment=primary).values()
                 if task["lastStatus"] == "PROVISIONING")
    _start_worker(svc, third, client)
    assert created[-1]["image"] == expected
    assert len(client.images.registry_calls) == 1


def test_ecs_ec2_manifest_failure_continues_by_tag_with_cached_execution(service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    client.images.manifest_error = True
    client.images.pull_error = True
    digest = "sha256:" + "d" * 64
    client.images.cached = [f"example.invalid/app@{digest}"]
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, first, client)
    assert first["lastStatus"] == "RUNNING"
    assert not primary["_image_digests"]
    assert primary["_image_resolution_disabled"]
    assert created[0]["image"] == "example.invalid/app:latest"
    assert "imageDigest" not in first["containers"][0]
    assert len(client.images.registry_calls) == 3
    assert len(client.images.pulls) == 1


@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE"])
@pytest.mark.parametrize("image", [
    "example.invalid/app:latest",
    "000000000000.dkr.ecr.us-east-1.amazonaws.com/offline-app:latest",
])
@pytest.mark.parametrize("breaker", [False, True])
def test_ecs_offline_cached_images_complete_for_both_launch_types_and_registries(
        service, pending_launcher, docker_worker, launch_type, image, breaker):
    cluster, svc = service
    client, created = docker_worker
    svc["launchType"] = launch_type
    td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
    td["containerDefinitions"][0]["image"] = image
    client.images.manifest_error = True
    client.images.pull_error = True
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "deploymentCircuitBreaker": {"enable": breaker, "rollback": False},
    })
    primary = ecs_service._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, first, client)
    assert first["lastStatus"] == "RUNNING"
    assert primary["rolloutState"] != "FAILED"
    assert not primary["_image_digests"]
    assert "imageDigest" not in first["containers"][0]
    ecs_service._reconcile_service_tasks(cluster, f"{cluster}/app")
    second = next(task for task in _tasks(svc, deployment=primary).values()
                  if task["taskArn"] != first["taskArn"])
    _start_worker(svc, second, client)
    for task in (first, second):
        ecs_service._record_service_task_healthy(f"{cluster}/app", task)
    ecs_service._complete_service_deployment(cluster, f"{cluster}/app")
    assert primary["rolloutState"] == "COMPLETED"
    assert all(task["lastStatus"] == "RUNNING"
               for task in _tasks(svc, deployment=primary).values())
    assert [kwargs["image"] for kwargs in created] == [image, image]
    assert len(client.images.registry_calls) == 3
    assert [uri for uri, _ in client.images.pulls] == [image, image]


def test_ecs_offline_cache_does_not_hide_an_uncached_second_container(
        service, pending_launcher, docker_worker, monkeypatch):
    from docker.errors import ImageNotFound

    cluster, svc = service
    client, _ = docker_worker
    td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
    td["containerDefinitions"].append({
        "name": "sidecar", "image": "example.invalid/sidecar:missing", "essential": True,
    })
    client.images.manifest_error = True
    original_get = client.images.get

    def get(image):
        if image == "example.invalid/sidecar:missing":
            raise ImageNotFound("sidecar is not cached")
        return original_get(image)

    monkeypatch.setattr(client.images, "get", get)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "deploymentCircuitBreaker": {"enable": True, "rollback": False},
    })
    primary = ecs_service._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))
    ecs_service._prepare_service_images(task, td, client)
    assert primary["rolloutState"] == "FAILED"
    assert not primary["_image_digests"]
    assert len(client.images.registry_calls) == 6


@pytest.mark.parametrize("breaker,rollback", [(False, False), (True, False), (True, True)])
def test_ecs_three_failed_manifest_attempts_continue_or_fail_and_roll_back(
        service, pending_launcher, docker_worker, monkeypatch, breaker, rollback):
    cluster, svc = service
    client, created = docker_worker
    client.images.manifest_error = True
    client.images.cache_missing = True
    # An unrelated RepoDigest, and even an image's local config ID, cannot
    # establish a deployment's registry manifest.
    client.images.cached = ["other.invalid/app@sha256:wrong"]
    old = ecs_service._primary_deployment(svc)
    monkeypatch.setattr(ecs_service, "_schedule_service_deployment_completion", lambda *args, **kwargs: None)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "maximumPercent": 200, "minimumHealthyPercent": 100,
        "deploymentCircuitBreaker": {"enable": breaker, "rollback": rollback},
    })
    primary = ecs_service._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, first, client)
    assert len(client.images.registry_calls) == 3
    assert primary["_image_resolution_disabled"]
    assert not primary["_image_digests"]
    if breaker:
        assert primary["rolloutState"] == "FAILED"
        assert primary["failedTasks"] == 0  # no task-start failure was invented
        if rollback:
            assert ecs_service._primary_deployment(svc) is old
            ecs_service._complete_service_deployment(cluster, f"{cluster}/app")
            assert first["lastStatus"] == "STOPPED"
        else:
            assert ecs_service._primary_deployment(svc) is primary
    else:
        assert primary["rolloutState"] == "IN_PROGRESS"
        assert created[0]["image"] == "example.invalid/app:latest"
        ecs_service._reconcile_service_tasks(cluster, f"{cluster}/app")
        second = next(task for task in _tasks(svc, deployment=primary).values()
                      if task["lastStatus"] == "PROVISIONING")
        _start_worker(svc, second, client)
        assert len(client.images.registry_calls) == 3
        assert all(kwargs["image"] == "example.invalid/app:latest" for kwargs in created)


@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE"])
def test_ecs_service_pull_failure_cache_policy(
        service, pending_launcher, docker_worker, launch_type):
    cluster, svc = service
    client, created = docker_worker
    svc["launchType"] = launch_type
    client.images.pull_error = True
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, first, client)
    assert first["lastStatus"] == "RUNNING"
    assert len(created) == 1
    assert primary["launchType"] == launch_type
    assert len(client.images.pulls) == 1


def test_ecs_disabled_version_consistency_pulls_tag_for_each_task(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
    td["containerDefinitions"][0]["versionConsistency"] = "disabled"
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    assert len(_tasks(svc, deployment=primary)) == 2
    for task in _tasks(svc, deployment=primary).values():
        _start_worker(svc, task, client)
    assert client.images.registry_calls == []
    assert len(client.images.pulls) == 2
    assert [kwargs["image"] for kwargs in created] == ["example.invalid/app:latest"] * 2


def test_ecs_digest_reference_skips_manifest_lookup(service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
    digest = "sha256:" + "e" * 64
    td["containerDefinitions"][0]["image"] = f"example.invalid/app:latest@{digest}"
    _update(cluster, forceNewDeployment=True)
    assert len(_tasks(svc, deployment=ecs_service._primary_deployment(svc))) == 2
    assert pending_launcher == [2]
    task = next(iter(_tasks(svc, deployment=ecs_service._primary_deployment(svc)).values()))
    _start_worker(svc, task, client)
    assert client.images.registry_calls == []
    assert created[0]["image"] == f"example.invalid/app@{digest}"


def test_ecs_zero_size_deployment_needs_new_deployment_to_establish_digest(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    _update(cluster, desiredCount=0, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    assert primary["_image_resolution_disabled"]
    _update(cluster, desiredCount=2)
    for task in _tasks(svc, deployment=primary).values():
        _start_worker(svc, task, client)
    assert client.images.registry_calls == []
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, task, client)
    assert client.images.registry_calls == ["example.invalid/app:latest"]


def test_ecs_pull_preserves_declared_platform_and_existing_fallback(docker_worker, monkeypatch):
    client, created = docker_worker
    # The normal run path retains its declared platform during image pull.
    monkeypatch.setattr(ecs_service.time, "sleep", lambda seconds: None)
    ecs_service._run_docker_container(client, {"image": "example.invalid/app:latest"},
                              {"detach": True, "platform": "linux/arm64"}, refresh_image=True)
    assert client.images.pulls == [("example.invalid/app:latest", {"platform": "linux/arm64"})]
    assert created[0]["platform"] == "linux/arm64"
    # Host-architecture retry remains available on a platform execution error.
    original_create = client.containers.create

    def reject_pinned(**kwargs):
        if kwargs.get("platform"):
            raise RuntimeError("unsupported platform")
        return original_create(**kwargs)

    client.containers.create = reject_pinned
    ecs_service._run_docker_container(client, {"image": "example.invalid/app:latest"},
                              {"detach": True, "platform": "linux/arm64"}, refresh_image=True)
    assert client.images.pulls[-2:] == [
        ("example.invalid/app:latest", {"platform": "linux/arm64"}),
        ("example.invalid/app:latest", {}),
    ]
    assert "platform" not in created[-1]


@pytest.mark.parametrize("allow_cached", [False, True])
def test_ecs_platform_pull_failure_retains_existing_host_architecture_fallback(
        docker_worker, monkeypatch, allow_cached):
    from docker.errors import ImageNotFound

    client, created = docker_worker
    original_pull = client.images.pull

    def pull(image, **kwargs):
        if kwargs.get("platform") == "linux/arm64":
            client.images.pulls.append((image, kwargs))
            raise RuntimeError("no matching manifest for linux/arm64")
        return original_pull(image, **kwargs)

    def missing(image):
        raise ImageNotFound("image not cached")

    monkeypatch.setattr(client.images, "pull", pull)
    monkeypatch.setattr(client.images, "get", missing)
    ecs_service._run_docker_container(
        client, {"image": "example.invalid/app:latest"},
        {"detach": True, "platform": "linux/arm64"},
        refresh_image=True, allow_cached=allow_cached,
    )
    assert client.images.pulls == [
        ("example.invalid/app:latest", {"platform": "linux/arm64"}),
        ("example.invalid/app:latest", {}),
    ]
    assert len(created) == 1
    assert "platform" not in created[0]


@pytest.mark.parametrize("controller", ["CODE_DEPLOY", "EXTERNAL"])
def test_ecs_force_is_scoped_to_supported_rolling_controller(service, controller):
    cluster, svc = service
    svc["deploymentController"] = {"type": controller}
    primary = ecs_service._primary_deployment(svc)
    tasks = set(_tasks(svc))
    _update(cluster, forceNewDeployment=True)
    assert ecs_service._primary_deployment(svc) is primary
    assert set(_tasks(svc)) == tasks


def test_ecs_fargate_manifest_failure_does_not_use_prior_task_cache(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    svc["launchType"] = "FARGATE"
    client.images.manifest_error = True
    client.images.cached = ["example.invalid/app@sha256:" + "f" * 64]
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, task, client)
    assert len(client.images.registry_calls) == 3
    assert not primary["_image_digests"]
    assert created[0]["image"] == "example.invalid/app:latest"
    assert client.images.pulls == [("example.invalid/app:latest", {})]


def test_ecs_deployment_manifest_survives_persistence_and_scopes(service, pending_launcher, docker_worker):
    from ministack.core.responses import request_scope

    cluster, svc = service
    client, created = docker_worker
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, task, client)
    state = ecs_service.get_state()
    persisted = state["services"][f"{cluster}/app"]["deployments"][0]
    assert persisted["_image_digests"] == {"app": client.images.digest}
    # Snapshot and wire output are independent; private digest bookkeeping is
    # persisted but never becomes a new API member.
    primary["_image_digests"]["app"] = "sha256:changed"
    assert persisted["_image_digests"]["app"] == client.images.digest
    assert "_image_digests" not in ecs_service._sanitize(primary)
    with request_scope("222222222222", "us-west-2"):
        assert f"{cluster}/app" not in ecs_service._services
        assert not _tasks(svc)
    primary["_image_digests"]["app"] = client.images.digest


def test_ecs_initial_deployment_resolution_failure_triggers_enabled_breaker(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    client.images.manifest_error = True
    client.images.cache_missing = True
    initial = ecs_service._primary_deployment(svc)
    svc["deploymentConfiguration"]["deploymentCircuitBreaker"]["enable"] = True
    task = next(iter(_tasks(svc).values()))
    task["lastStatus"] = "PROVISIONING"
    _start_worker(svc, task, client)
    assert initial["rolloutState"] == "FAILED"
    assert initial["failedTasks"] == 0
    assert len(client.images.registry_calls) == 3


@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE"])
@pytest.mark.parametrize("architecture", [None, "ARM64"])
def test_ecs_terminal_service_pull_failure_has_documented_task_error_category(
        service, pending_launcher, docker_worker, monkeypatch, launch_type, architecture):
    from docker.errors import ImageNotFound

    from ministack.core.responses import get_account_id, get_region

    cluster, svc = service
    client, created = docker_worker
    td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
    if architecture:
        td["runtimePlatform"] = {"cpuArchitecture": architecture}
    svc["launchType"] = launch_type
    client.images.pull_error = True
    def missing(image):
        raise ImageNotFound("cached image absent")
    monkeypatch.setattr(client.images, "get", missing)
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))
    ecs_service._run_task_worker(task, td, [], client, get_account_id(), get_region())
    assert task["lastStatus"] == "STOPPED"
    assert task["stopCode"] == "TaskFailedToStart"
    assert task["stoppedReason"] == "CannotPullContainerError: image pull failed"
    assert not created
    assert len(client.images.pulls) == (2 if architecture else 1)


@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE"])
def test_ecs_all_tasks_can_use_cached_tag_after_a_resolved_digest_pull_fails(
        service, pending_launcher, docker_worker, monkeypatch, launch_type):
    from docker.errors import ImageNotFound

    cluster, svc = service
    svc["launchType"] = launch_type
    client, created = docker_worker
    client.images.digest = "sha256:" + "b" * 64
    client.images.pull_error = True

    def only_old_tag(image):
        if image != "example.invalid/app:latest":
            raise ImageNotFound("new digest B not cached")
        return SimpleNamespace(id="old-image-A")

    monkeypatch.setattr(client.images, "get", only_old_tag)
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, first, client)
    assert first["lastStatus"] == "RUNNING"
    assert primary["_image_digests"] == {"app": client.images.digest}
    assert client.images.pulls[0][0] == "example.invalid/app@" + client.images.digest
    assert created[0]["image"] == "example.invalid/app:latest"
    assert first["containers"][0]["imageDigest"] == client.images.digest
    assert not client.images.tags
    # Later tasks retain the resolved digest in their response while also
    # permitting the requested tag's cached execution for offline use.
    ecs_service._reconcile_service_tasks(cluster, f"{cluster}/app")
    second = next(task for task in _tasks(svc, deployment=primary).values()
                  if task["lastStatus"] == "PROVISIONING")
    _start_worker(svc, second, client)
    assert second["lastStatus"] == "RUNNING"
    assert second["containers"][0]["imageDigest"] == client.images.digest
    assert created[1]["image"] == "example.invalid/app:latest"
    assert len(created) == 2


def test_ecs_successful_first_task_pull_refreshes_original_tag_cache(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    _update(cluster, forceNewDeployment=True)
    first = next(iter(_tasks(svc, deployment=ecs_service._primary_deployment(svc)).values()))
    _start_worker(svc, first, client)
    assert client.images.tags == [("example.invalid/app", "latest")]
    assert created[0]["image"] == "example.invalid/app@" + client.images.digest
    described = _decoded(ecs_service._describe_tasks({
        "cluster": cluster, "tasks": [first["taskArn"]],
    }))["tasks"][0]["containers"][0]
    assert described["image"] == "example.invalid/app:latest"
    assert described["imageDigest"] == client.images.digest


def test_ecs_following_tasks_wait_for_first_running_task_even_after_manifest_is_known(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
    ecs_service._mark_task_activating(first["taskArn"], first)
    ecs_service._prepare_service_images(first, td, client)
    assert primary["_image_digests"] == {"app": client.images.digest}
    _update(cluster, desiredCount=2)
    assert pending_launcher == [1]
    assert len(_tasks(svc, deployment=primary)) == 1
    ecs_service._mark_task_running(first["taskArn"], first)
    ecs_service._reconcile_service_tasks(cluster, f"{cluster}/app")
    assert pending_launcher == [1, 1]
    assert len(_tasks(svc, deployment=primary)) == 2


def test_ecs_canceled_manifest_worker_cannot_publish_or_fail_deployment(
        service, pending_launcher, docker_worker, monkeypatch):
    cluster, svc = service
    client, created = docker_worker
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "maximumPercent": 200, "minimumHealthyPercent": 100,
        "deploymentCircuitBreaker": {"enable": True, "rollback": True},
    })
    primary = ecs_service._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))

    def cancel_during_lookup(image):
        client.images.registry_calls.append(image)
        ecs_service._mark_task_stopped(task["taskArn"], task, "User canceled task", "UserInitiated")
        raise RuntimeError("registry unavailable after cancellation")

    monkeypatch.setattr(client.images, "get_registry_data", cancel_during_lookup)
    _start_worker(svc, task, client)
    assert task["lastStatus"] == "STOPPED"
    assert task["stopCode"] == "UserInitiated"
    assert len(client.images.registry_calls) == 1
    assert "_image_digests" not in primary
    assert primary["rolloutState"] == "IN_PROGRESS"
    assert primary["failedTasks"] == 0
    assert ecs_service._primary_deployment(svc) is primary
    assert not created
    # The digest-only transition also rejects a stale completion delivered
    # after cancellation, while ordinary stopped-task failures stay supported.
    ecs_service._record_service_task_failure(task, digest_resolution_failed=True)
    assert primary["rolloutState"] == "IN_PROGRESS"


@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize("schedule_after_update", [False, True])
def test_ecs_predecessor_startup_callback_cannot_complete_crashing_replacement(
        service, pending_launcher, monkeypatch, force, schedule_after_update):
    cluster, svc = service
    _update(cluster, desiredCount=1)
    old = ecs_service._primary_deployment(svc)
    old_task = next(iter(_tasks(svc, "RUNNING").values()))
    callbacks = []
    monkeypatch.setattr(ecs_service, "spawn_background", lambda callback, **kwargs: callbacks.append(callback))
    monkeypatch.setattr(ecs_service.time, "sleep", lambda delay: None)

    def schedule_old():
        from ministack.core.responses import get_account_id, get_region
        ecs_service._schedule_service_deployment_completion(
            cluster, f"{cluster}/app", get_account_id(), get_region(), healthy_task=old_task)

    if not schedule_after_update:
        schedule_old()
    request = {"forceNewDeployment": True} if force else {
        "taskDefinition": _decoded(ecs_service._register_task_definition({
            "family": cluster,
            "containerDefinitions": [{
                "name": "app", "image": "example.invalid/app:latest",
                "command": ["sh", "-c", "exit 1"],
            }],
        }))["taskDefinition"]["taskDefinitionArn"],
    }
    _update(cluster, **request, deploymentConfiguration={
        "deploymentCircuitBreaker": {"enable": True, "rollback": True},
    })
    primary = ecs_service._primary_deployment(svc)
    if schedule_after_update:
        schedule_old()
    first = next(iter(_tasks(svc, deployment=primary).values()))
    # The Docker worker publishes RUNNING before its watcher detects exit 1.
    ecs_service._mark_task_running(first["taskArn"], first)
    callbacks[0]()
    assert primary["rolloutState"] == "IN_PROGRESS"
    assert old_task["lastStatus"] == "RUNNING"

    for _ in range(3):
        task = next(task for task in _tasks(svc, deployment=primary).values()
                    if task["lastStatus"] in ecs_service._PRE_STOP_STATUSES)
        ecs_service._mark_task_stopped(task["taskArn"], task,
                              "Essential container in task exited", "EssentialContainerExited", 1)
    assert primary["rolloutState"] == "FAILED"
    assert primary["failedTasks"] == 3
    assert ecs_service._primary_deployment(svc) is old
    # The rollback's own scheduled callback still completes its deployment.
    callbacks[-1]()
    assert old["rolloutState"] == "COMPLETED"
    assert old_task["lastStatus"] == "RUNNING"


@pytest.mark.parametrize("platform,os_family,resolves,reported_platform,family", [
    ("1.2.0", "LINUX", False, "1.2.0", "Linux"),
    ("1.3.0", "LINUX", True, "1.3.0", "Linux"),
    ("1.4.0", "LINUX", True, "1.4.0", "Linux"),
    ("LATEST", "LINUX", True, "1.4.0", "Linux"),
    ("", "LINUX", True, "1.4.0", "Linux"),
    ("1.0.0", "WINDOWS_SERVER_2022_CORE", True, "1.0.0", ""),
    ("LATEST", "WINDOWS_SERVER_2022_CORE", True, "1.0.0", ""),
    ("", "WINDOWS_SERVER_2022_CORE", True, "1.0.0", ""),
])
def test_ecs_fargate_platform_controls_resolution_and_first_task_reservation(
        service, pending_launcher, docker_worker, platform, os_family, resolves,
        reported_platform, family):
    cluster, svc = service
    svc["launchType"] = "FARGATE"
    svc["platformVersion"] = platform
    client, created = docker_worker
    td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
    td["runtimePlatform"] = {"operatingSystemFamily": os_family}
    _update(cluster, forceNewDeployment=True)
    primary = ecs_service._primary_deployment(svc)
    current = list(_tasks(svc, deployment=primary).values())
    assert len(current) == (1 if resolves else 2)
    assert all(task["platformVersion"] == reported_platform for task in current)
    assert all(task["platformFamily"] == family for task in current)
    _start_worker(svc, current[0], client)
    assert bool(client.images.registry_calls) is resolves
    assert bool(primary.get("_image_digests")) is resolves
    container = current[0]["containers"][0]
    assert ("imageDigest" in container) is resolves
    assert container["image"] == "example.invalid/app:latest"
    assert created[0]["image"] == (
        "example.invalid/app@" + client.images.digest if resolves
        else "example.invalid/app:latest")


def test_ecs_reported_manifest_digest_survives_persistence(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, _ = docker_worker
    _update(cluster, forceNewDeployment=True)
    task = next(iter(_tasks(svc, deployment=ecs_service._primary_deployment(svc)).values()))
    _start_worker(svc, task, client)
    state = ecs_service.get_state()
    persisted = state["tasks"][task["taskArn"]]["containers"][0]
    assert persisted["imageDigest"] == client.images.digest
    task["containers"][0]["imageDigest"] = "sha256:" + "f" * 64
    assert persisted["imageDigest"] == client.images.digest


@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE"])
@pytest.mark.parametrize("consistency", ["enabled", "disabled", "digest"])
def test_ecs_private_registry_credentials_reach_lookup_and_pull_without_leaking(
        service, pending_launcher, docker_worker, monkeypatch, launch_type, consistency):
    cluster, svc = service
    svc["launchType"] = launch_type
    client, created = docker_worker
    td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
    cdef = td["containerDefinitions"][0]
    secret_id = "arn:aws:secretsmanager:us-east-1:000000000000:secret:registry"
    credentials = {"username": "repro-user", "password": "private-registry-password"}
    cdef["repositoryCredentials"] = {"credentialsParameter": secret_id}
    if consistency == "digest":
        cdef["image"] = "example.invalid/app@" + client.images.digest
    else:
        cdef["versionConsistency"] = consistency
    reads = []

    def resolve(secret):
        reads.append(secret)
        return json.dumps(credentials)

    monkeypatch.setattr(ecs_service.secretsmanager, "resolve_secret_string", resolve)
    lookup = client.images.get_registry_data
    lookup_auth = []

    def authenticated_lookup(image, **kwargs):
        lookup_auth.append(kwargs)
        assert kwargs == {"auth_config": credentials}
        return lookup(image)

    monkeypatch.setattr(client.images, "get_registry_data", authenticated_lookup)
    _update(cluster, forceNewDeployment=True)
    task = next(iter(_tasks(svc, deployment=ecs_service._primary_deployment(svc)).values()))
    _start_worker(svc, task, client)
    assert task["lastStatus"] == "RUNNING"
    assert bool(lookup_auth) is (consistency == "enabled")
    assert client.images.pulls[-1][1]["auth_config"] == credentials
    assert set(reads) == {secret_id}
    assert "auth_config" not in created[0]
    assert credentials["password"] not in repr(ecs_service.get_state())
    assert credentials["password"] not in repr(ecs_service._sanitize(task))


@pytest.mark.parametrize("secret", [None, "not-json", "{}", '{"username":"user"}'])
def test_ecs_unavailable_registry_secret_fails_start_without_exposing_secret(
        service, pending_launcher, docker_worker, monkeypatch, secret):
    cluster, svc = service
    client, created = docker_worker
    td = ecs_service._task_defs[ecs_service._resolve_td_key(svc["taskDefinition"])]
    td["containerDefinitions"][0]["repositoryCredentials"] = {"credentialsParameter": "registry"}
    monkeypatch.setattr(ecs_service.secretsmanager, "resolve_secret_string", lambda secret_id: secret)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "deploymentCircuitBreaker": {"enable": True, "rollback": False},
    })
    task = next(iter(_tasks(svc, deployment=ecs_service._primary_deployment(svc)).values()))
    # Exercise the worker's existing secret-retrieval error category without
    # starting real Docker watchers or hiding the initialization failure.
    ecs_service._run_task_worker(task, td, [], client, "000000000000", "us-east-1")
    assert task["lastStatus"] == "STOPPED"
    assert task["stopCode"] == "TaskFailedToStart"
    assert task["stoppedReason"].startswith("ResourceInitializationError: unable to pull secrets or registry auth")
    assert not created
    assert not client.images.pulls
    assert not client.images.registry_calls
    if secret:
        assert secret not in task["stoppedReason"]
