"""Rolling-deployment regressions with simulated workers, without Docker/AWS."""

import json
import uuid
from types import SimpleNamespace

import pytest

from ministack.services import ecs


def _decoded(response):
    assert response[0] == 200, response
    return json.loads(response[2])


@pytest.fixture
def service(monkeypatch):
    monkeypatch.setattr(ecs, "_get_docker", lambda: None)
    cluster = f"force-{uuid.uuid4().hex[:8]}"
    td = _decoded(ecs._register_task_definition({
        "family": cluster,
        "containerDefinitions": [{"name": "app", "image": "example.invalid/app:latest"}],
    }))["taskDefinition"]
    _decoded(ecs._create_service({
        "cluster": cluster, "serviceName": "app",
        "taskDefinition": td["taskDefinitionArn"], "desiredCount": 2,
    }))
    svc = ecs._services[f"{cluster}/app"]
    yield cluster, svc
    for arn, task in list(ecs._tasks.items()):
        if task["clusterArn"] == svc["clusterArn"]:
            ecs._tasks.pop(arn)
    ecs._services.pop(f"{cluster}/app", None)
    ecs._clusters.pop(cluster, None)
    for key, task_def in list(ecs._task_defs.items()):
        if task_def["family"] == cluster:
            ecs._task_defs.pop(key)
    ecs._task_def_latest.pop(cluster, None)


def _tasks(svc, status=None, deployment=None):
    return {
        arn: task for arn, task in ecs._tasks.items()
        if task["clusterArn"] == svc["clusterArn"]
        and (status is None or task["lastStatus"] == status)
        and (deployment is None or task.get("_deployment_id") == deployment["id"])
    }


def _update(cluster, **kwargs):
    return _decoded(ecs._update_service({"cluster": cluster, "service": "app", **kwargs}))["service"]


@pytest.mark.parametrize("definition", [None, "arn", "family_revision"])
def test_force_same_definition_replaces_tasks(service, definition):
    cluster, svc = service
    old_id = ecs._primary_deployment(svc)["id"]
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
def test_unchanged_update_does_not_deploy(service, explicit_definition, force):
    cluster, svc = service
    deployment = ecs._primary_deployment(svc)
    tasks = set(_tasks(svc))
    request = {}
    if explicit_definition:
        request["taskDefinition"] = f"{cluster}:1"
    if force is not None:
        request["forceNewDeployment"] = force
    _update(cluster, **request)
    assert ecs._primary_deployment(svc) is deployment
    assert set(_tasks(svc)) == tasks


@pytest.mark.parametrize("force", [False, True])
def test_changed_definition_creates_one_deployment(service, force):
    cluster, svc = service
    old_tasks = set(_tasks(svc))
    td = _decoded(ecs._register_task_definition({
        "family": cluster,
        "containerDefinitions": [{"name": "app", "image": "example.invalid/app:v2"}],
    }))["taskDefinition"]["taskDefinitionArn"]
    _update(cluster, taskDefinition=td, forceNewDeployment=force)
    assert len(svc["deployments"]) == 1
    assert svc["deployments"][0]["taskDefinition"] == td
    assert set(_tasks(svc, "RUNNING")).isdisjoint(old_tasks)
    assert all(task["taskDefinitionArn"] == td for task in _tasks(svc, "RUNNING").values())


def test_repeated_force_uses_distinct_task_and_deployment_identities(service):
    cluster, svc = service
    deployment_ids = {ecs._primary_deployment(svc)["id"]}
    seen_tasks = set(_tasks(svc))
    for _ in range(3):
        _update(cluster, forceNewDeployment=True)
        primary = ecs._primary_deployment(svc)
        assert primary["id"] not in deployment_ids
        deployment_ids.add(primary["id"])
        current = set(_tasks(svc, "RUNNING"))
        assert len(current) == 2
        assert current.isdisjoint(seen_tasks)
        seen_tasks.update(current)


def test_force_at_zero_desired_and_scale_afterwards(service):
    cluster, svc = service
    _update(cluster, desiredCount=0)
    old_id = ecs._primary_deployment(svc)["id"]
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    assert primary["id"] != old_id
    assert primary["rolloutState"] == "COMPLETED"
    assert primary["runningCount"] == primary["pendingCount"] == 0
    assert not _tasks(svc, "RUNNING")
    _update(cluster, desiredCount=2)
    assert ecs._primary_deployment(svc) is primary
    assert len(_tasks(svc, "RUNNING", primary)) == 2


@pytest.mark.parametrize("scale_first", [False, True])
def test_docker_force_at_zero_drains_predecessors_without_a_startup_callback(
        service, pending_launcher, scale_first):
    cluster, svc = service
    old_tasks = set(_tasks(svc))
    if scale_first:
        _update(cluster, desiredCount=0)
    _update(cluster, desiredCount=0, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    assert primary["rolloutState"] == "COMPLETED"
    assert svc["runningCount"] == svc["pendingCount"] == 0
    assert svc["deployments"] == [primary]
    assert set(_tasks(svc, "STOPPED")) == old_tasks
    assert pending_launcher == []
    _update(cluster, desiredCount=2)
    assert ecs._primary_deployment(svc) is primary
    assert len(_tasks(svc, deployment=primary)) == 2
    assert pending_launcher == [2]


@pytest.mark.parametrize("crash", [False, True])
def test_first_task_callback_cannot_approve_a_newly_running_second_task(
        service, pending_launcher, monkeypatch, crash):
    from ministack.core.responses import get_account_id, get_region

    cluster, svc = service
    old = ecs._primary_deployment(svc)
    old_tasks = set(_tasks(svc, "RUNNING"))
    callbacks = []
    monkeypatch.setattr(ecs, "spawn_background", lambda callback, **kwargs: callbacks.append(callback))
    monkeypatch.setattr(ecs.time, "sleep", lambda delay: None)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "deploymentCircuitBreaker": {
            "enable": True, "rollback": True,
            "thresholdConfiguration": {"type": "COUNT", "value": 1},
        },
    })
    primary = ecs._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    primary["_image_digests"] = {"app": "sha256:" + "a" * 64}
    ecs._mark_task_running(first["taskArn"], first)
    ecs._schedule_service_deployment_completion(
        cluster, f"{cluster}/app", get_account_id(), get_region(), healthy_task=first)
    launch = ecs._run_task

    def start_during_reconcile(request):
        response = launch(request)
        for task in _decoded(response)["tasks"]:
            second = ecs._tasks[task["taskArn"]]
            ecs._mark_task_running(second["taskArn"], second)
            ecs._schedule_service_deployment_completion(
                cluster, f"{cluster}/app", get_account_id(), get_region(), healthy_task=second)
        return response

    monkeypatch.setattr(ecs, "_run_task", start_during_reconcile)
    callbacks[0]()
    second = next(task for task in _tasks(svc, deployment=primary).values()
                  if task is not first)
    assert primary["runningCount"] == 2
    assert primary["rolloutState"] == "IN_PROGRESS"
    assert old_tasks <= set(_tasks(svc, "RUNNING"))
    if crash:
        ecs._mark_task_stopped(second["taskArn"], second,
                              "Essential container exited", "EssentialContainerExited", 1)
        callbacks[1]()  # A stopped task's late callback cannot certify a retry.
        assert primary["rolloutState"] == "FAILED"
        assert primary["failedTasks"] == 1
        assert ecs._primary_deployment(svc) is old
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
    original_run = ecs._run_task
    calls = []

    def launch(request):
        # Production registration, with only the asynchronous Docker worker
        # replaced. PROVISIONING is already reserved scheduler capacity.
        with monkeypatch.context() as patch:
            patch.setattr(ecs, "_get_docker", lambda: None)
            response = original_run(request)
        for task in _decoded(response)["tasks"]:
            ecs._tasks[task["taskArn"]]["lastStatus"] = "PROVISIONING"
            ecs._tasks[task["taskArn"]]["_startup_stable"] = False
        calls.append(request["count"])
        return response

    monkeypatch.setattr(ecs, "_get_docker", lambda: object())
    monkeypatch.setattr(ecs, "_run_task", launch)
    return calls


@pytest.mark.parametrize("pending_status", ["PROVISIONING", "PENDING", "ACTIVATING"])
def test_force_keeps_old_tasks_until_stable_and_reserves_pending_capacity(
        service, pending_launcher, pending_status):
    cluster, svc = service
    old = ecs._primary_deployment(svc)
    old_tasks = set(_tasks(svc, "RUNNING"))
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    current = _tasks(svc, deployment=primary)
    assert len(current) == 1
    assert old["runningCount"] == 2
    assert primary["runningCount"] == 0
    for task in current.values():
        task["lastStatus"] = pending_status
    for _ in range(3):
        ecs._reconcile_service_tasks(cluster, f"{cluster}/app")
    assert pending_launcher == [1]
    assert old_tasks <= set(_tasks(svc, "RUNNING"))
    assert primary["pendingCount"] == (1 if pending_status == "PENDING" else 0)
    assert primary["rolloutState"] == "IN_PROGRESS"
    for task in current.values():
        td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
        ecs._prepare_service_images(task, td, SimpleNamespace(images=SimpleNamespace(
            get_registry_data=lambda image: SimpleNamespace(id="sha256:" + "a" * 64),
        )))
        ecs._mark_task_running(task["taskArn"], task)
    ecs._refresh_service_state(cluster, "service:app")
    # RUNNING registration alone does not drain the predecessor. The worker's
    # steady-state callback invokes completion after its existing grace window.
    assert old_tasks <= set(_tasks(svc, "RUNNING"))
    ecs._reconcile_service_tasks(cluster, f"{cluster}/app")
    current = _tasks(svc, deployment=primary)
    assert len(current) == 2
    for task in current.values():
        task["lastStatus"] = "RUNNING"
        ecs._record_service_task_healthy(f"{cluster}/app", task)
    ecs._refresh_service_state(cluster, "service:app")
    ecs._complete_service_deployment(cluster, f"{cluster}/app")
    assert set(_tasks(svc, "RUNNING")) == set(current)
    assert old_tasks == set(_tasks(svc, "STOPPED"))
    assert svc["deployments"] == [primary]


@pytest.mark.parametrize("minimum,maximum,spawned,stopped", [(100, 100, 0, 0), (50, 100, 1, 1), (100, 150, 1, 0)])
def test_force_respects_rolling_capacity_limits(
        service, pending_launcher, minimum, maximum, spawned, stopped):
    cluster, svc = service
    old = ecs._primary_deployment(svc)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "minimumHealthyPercent": minimum, "maximumPercent": maximum,
    })
    primary = ecs._primary_deployment(svc)
    assert len(_tasks(svc, deployment=primary)) == spawned
    assert len(_tasks(svc, "STOPPED", old)) == stopped
    assert len(_tasks(svc, "RUNNING")) >= (2 * minimum + 99) // 100
    live = sum(task["lastStatus"] in ecs._PRE_STOP_STATUSES for task in _tasks(svc).values())
    assert live <= 2 * maximum // 100


def test_force_attributes_legacy_tasks_before_same_definition_is_ambiguous(service, pending_launcher):
    cluster, svc = service
    old = ecs._primary_deployment(svc)
    legacy = list(_tasks(svc).values())
    for task in legacy:
        task.pop("_deployment_id")
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    assert all(task["_deployment_id"] == old["id"] for task in legacy)
    assert old["runningCount"] == 2
    assert primary["runningCount"] == 0
    assert len(_tasks(svc, deployment=primary)) == 1


@pytest.mark.parametrize("rollback", [False, True])
def test_same_definition_failure_stops_retries_and_rolls_back_by_deployment(
        service, pending_launcher, monkeypatch, rollback):
    cluster, svc = service
    old = ecs._primary_deployment(svc)
    old_tasks = set(_tasks(svc))
    monkeypatch.setattr(ecs, "_schedule_service_deployment_completion", lambda *args, **kwargs: None)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "maximumPercent": 200, "minimumHealthyPercent": 100,
        "deploymentCircuitBreaker": {"enable": True, "rollback": rollback},
    })
    failed = ecs._primary_deployment(svc)
    for _ in range(3):
        task = next(task for task in _tasks(svc, deployment=failed).values()
                    if task["lastStatus"] in ecs._PRE_STOP_STATUSES)
        task["lastStatus"] = "STOPPED"
        ecs._record_service_task_failure(task)
    assert failed["rolloutState"] == "FAILED"
    assert failed["failedTasks"] == 3
    if rollback:
        assert ecs._primary_deployment(svc) is old
        ecs._refresh_service_state(cluster, "service:app")
        ecs._complete_service_deployment(cluster, f"{cluster}/app")
        assert set(_tasks(svc, "RUNNING")) == old_tasks
        assert old["rolloutState"] == "COMPLETED"
    else:
        assert ecs._primary_deployment(svc) is failed
        calls_before = list(pending_launcher)
        ecs._reconcile_service_tasks(cluster, f"{cluster}/app")
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
    monkeypatch.setattr(ecs, "_start_awslogs_forwarder", lambda *args: None)
    yield client, created
    # Workers register metadata even though no containers exist on a daemon.
    from ministack.services import ecs_metadata
    for task in list(ecs._tasks.values()):
        for token in task.pop("_metadata_tokens", []):
            ecs_metadata.unregister_token(token)


def _start_worker(svc, task, client):
    td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
    ecs._start_task_worker(task, td, [], client)
    return task


def test_force_refreshes_manifest_and_pins_following_tasks(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    old = ecs._primary_deployment(svc)
    old["_image_digests"] = {"app": "sha256:" + "0" * 64}
    old_tasks = set(_tasks(svc))
    client.images.digest = "sha256:" + "b" * 64
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, first, client)
    assert primary["_image_digests"] == {"app": client.images.digest}
    # Changing the tag again between tasks must not change this deployment.
    client.images.digest = "sha256:" + "c" * 64
    ecs._reconcile_service_tasks(cluster, f"{cluster}/app")
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
        ecs._record_service_task_healthy(f"{cluster}/app", task)
    ecs._complete_service_deployment(cluster, f"{cluster}/app")
    assert set(_tasks(svc, "RUNNING")) == {first["taskArn"], second["taskArn"]}
    assert old_tasks == set(_tasks(svc, "STOPPED"))
    # Normal scaling keeps the established deployment's manifest, too.
    _update(cluster, desiredCount=3)
    third = next(task for task in _tasks(svc, deployment=primary).values()
                 if task["lastStatus"] == "PROVISIONING")
    _start_worker(svc, third, client)
    assert created[-1]["image"] == expected
    assert len(client.images.registry_calls) == 1


def test_ec2_manifest_failure_continues_by_tag_with_cached_execution(service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    client.images.manifest_error = True
    client.images.pull_error = True
    digest = "sha256:" + "d" * 64
    client.images.cached = [f"example.invalid/app@{digest}"]
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
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
def test_offline_cached_images_complete_for_both_launch_types_and_registries(
        service, pending_launcher, docker_worker, launch_type, image, breaker):
    cluster, svc = service
    client, created = docker_worker
    svc["launchType"] = launch_type
    td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
    td["containerDefinitions"][0]["image"] = image
    client.images.manifest_error = True
    client.images.pull_error = True
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "deploymentCircuitBreaker": {"enable": breaker, "rollback": False},
    })
    primary = ecs._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, first, client)
    assert first["lastStatus"] == "RUNNING"
    assert primary["rolloutState"] != "FAILED"
    assert not primary["_image_digests"]
    assert "imageDigest" not in first["containers"][0]
    ecs._reconcile_service_tasks(cluster, f"{cluster}/app")
    second = next(task for task in _tasks(svc, deployment=primary).values()
                  if task["taskArn"] != first["taskArn"])
    _start_worker(svc, second, client)
    for task in (first, second):
        ecs._record_service_task_healthy(f"{cluster}/app", task)
    ecs._complete_service_deployment(cluster, f"{cluster}/app")
    assert primary["rolloutState"] == "COMPLETED"
    assert all(task["lastStatus"] == "RUNNING"
               for task in _tasks(svc, deployment=primary).values())
    assert [kwargs["image"] for kwargs in created] == [image, image]
    assert len(client.images.registry_calls) == 3
    assert [uri for uri, _ in client.images.pulls] == [image, image]


def test_offline_cache_does_not_hide_an_uncached_second_container(
        service, pending_launcher, docker_worker, monkeypatch):
    from docker.errors import ImageNotFound

    cluster, svc = service
    client, _ = docker_worker
    td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
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
    primary = ecs._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))
    ecs._prepare_service_images(task, td, client)
    assert primary["rolloutState"] == "FAILED"
    assert not primary["_image_digests"]
    assert len(client.images.registry_calls) == 6


@pytest.mark.parametrize("breaker,rollback", [(False, False), (True, False), (True, True)])
def test_three_failed_manifest_attempts_continue_or_fail_and_roll_back(
        service, pending_launcher, docker_worker, monkeypatch, breaker, rollback):
    cluster, svc = service
    client, created = docker_worker
    client.images.manifest_error = True
    client.images.cache_missing = True
    # An unrelated RepoDigest, and even an image's local config ID, cannot
    # establish a deployment's registry manifest.
    client.images.cached = ["other.invalid/app@sha256:wrong"]
    old = ecs._primary_deployment(svc)
    monkeypatch.setattr(ecs, "_schedule_service_deployment_completion", lambda *args, **kwargs: None)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "maximumPercent": 200, "minimumHealthyPercent": 100,
        "deploymentCircuitBreaker": {"enable": breaker, "rollback": rollback},
    })
    primary = ecs._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, first, client)
    assert len(client.images.registry_calls) == 3
    assert primary["_image_resolution_disabled"]
    assert not primary["_image_digests"]
    if breaker:
        assert primary["rolloutState"] == "FAILED"
        assert primary["failedTasks"] == 0  # no task-start failure was invented
        if rollback:
            assert ecs._primary_deployment(svc) is old
            ecs._complete_service_deployment(cluster, f"{cluster}/app")
            assert first["lastStatus"] == "STOPPED"
        else:
            assert ecs._primary_deployment(svc) is primary
    else:
        assert primary["rolloutState"] == "IN_PROGRESS"
        assert created[0]["image"] == "example.invalid/app:latest"
        ecs._reconcile_service_tasks(cluster, f"{cluster}/app")
        second = next(task for task in _tasks(svc, deployment=primary).values()
                      if task["lastStatus"] == "PROVISIONING")
        _start_worker(svc, second, client)
        assert len(client.images.registry_calls) == 3
        assert all(kwargs["image"] == "example.invalid/app:latest" for kwargs in created)


@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE"])
def test_service_pull_failure_cache_policy(
        service, pending_launcher, docker_worker, launch_type):
    cluster, svc = service
    client, created = docker_worker
    svc["launchType"] = launch_type
    client.images.pull_error = True
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, first, client)
    assert first["lastStatus"] == "RUNNING"
    assert len(created) == 1
    assert primary["launchType"] == launch_type
    assert len(client.images.pulls) == 1


def test_disabled_version_consistency_pulls_tag_for_each_task(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
    td["containerDefinitions"][0]["versionConsistency"] = "disabled"
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    assert len(_tasks(svc, deployment=primary)) == 2
    for task in _tasks(svc, deployment=primary).values():
        _start_worker(svc, task, client)
    assert client.images.registry_calls == []
    assert len(client.images.pulls) == 2
    assert [kwargs["image"] for kwargs in created] == ["example.invalid/app:latest"] * 2


def test_digest_reference_skips_manifest_lookup(service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
    digest = "sha256:" + "e" * 64
    td["containerDefinitions"][0]["image"] = f"example.invalid/app:latest@{digest}"
    _update(cluster, forceNewDeployment=True)
    assert len(_tasks(svc, deployment=ecs._primary_deployment(svc))) == 2
    assert pending_launcher == [2]
    task = next(iter(_tasks(svc, deployment=ecs._primary_deployment(svc)).values()))
    _start_worker(svc, task, client)
    assert client.images.registry_calls == []
    assert created[0]["image"] == f"example.invalid/app@{digest}"


def test_zero_size_deployment_needs_new_deployment_to_establish_digest(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    _update(cluster, desiredCount=0, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    assert primary["_image_resolution_disabled"]
    _update(cluster, desiredCount=2)
    for task in _tasks(svc, deployment=primary).values():
        _start_worker(svc, task, client)
    assert client.images.registry_calls == []
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, task, client)
    assert client.images.registry_calls == ["example.invalid/app:latest"]


def test_pull_preserves_declared_platform_and_existing_fallback(docker_worker, monkeypatch):
    client, created = docker_worker
    # The normal run path retains its declared platform during image pull.
    monkeypatch.setattr(ecs.time, "sleep", lambda seconds: None)
    ecs._run_docker_container(client, {"image": "example.invalid/app:latest"},
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
    ecs._run_docker_container(client, {"image": "example.invalid/app:latest"},
                              {"detach": True, "platform": "linux/arm64"}, refresh_image=True)
    assert client.images.pulls[-2:] == [
        ("example.invalid/app:latest", {"platform": "linux/arm64"}),
        ("example.invalid/app:latest", {}),
    ]
    assert "platform" not in created[-1]


@pytest.mark.parametrize("allow_cached", [False, True])
def test_platform_pull_failure_retains_existing_host_architecture_fallback(
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
    ecs._run_docker_container(
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
def test_force_is_scoped_to_supported_rolling_controller(service, controller):
    cluster, svc = service
    svc["deploymentController"] = {"type": controller}
    primary = ecs._primary_deployment(svc)
    tasks = set(_tasks(svc))
    _update(cluster, forceNewDeployment=True)
    assert ecs._primary_deployment(svc) is primary
    assert set(_tasks(svc)) == tasks


def test_fargate_manifest_failure_does_not_use_prior_task_cache(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    svc["launchType"] = "FARGATE"
    client.images.manifest_error = True
    client.images.cached = ["example.invalid/app@sha256:" + "f" * 64]
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, task, client)
    assert len(client.images.registry_calls) == 3
    assert not primary["_image_digests"]
    assert created[0]["image"] == "example.invalid/app:latest"
    assert client.images.pulls == [("example.invalid/app:latest", {})]


def test_deployment_manifest_survives_persistence_and_scopes(service, pending_launcher, docker_worker):
    from ministack.core.responses import request_scope

    cluster, svc = service
    client, created = docker_worker
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))
    _start_worker(svc, task, client)
    state = ecs.get_state()
    persisted = state["services"][f"{cluster}/app"]["deployments"][0]
    assert persisted["_image_digests"] == {"app": client.images.digest}
    # Snapshot and wire output are independent; private digest bookkeeping is
    # persisted but never becomes a new API member.
    primary["_image_digests"]["app"] = "sha256:changed"
    assert persisted["_image_digests"]["app"] == client.images.digest
    assert "_image_digests" not in ecs._sanitize(primary)
    with request_scope("222222222222", "us-west-2"):
        assert f"{cluster}/app" not in ecs._services
        assert not _tasks(svc)
    primary["_image_digests"]["app"] = client.images.digest


def test_initial_deployment_resolution_failure_triggers_enabled_breaker(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    client.images.manifest_error = True
    client.images.cache_missing = True
    initial = ecs._primary_deployment(svc)
    svc["deploymentConfiguration"]["deploymentCircuitBreaker"]["enable"] = True
    task = next(iter(_tasks(svc).values()))
    task["lastStatus"] = "PROVISIONING"
    _start_worker(svc, task, client)
    assert initial["rolloutState"] == "FAILED"
    assert initial["failedTasks"] == 0
    assert len(client.images.registry_calls) == 3


@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE"])
@pytest.mark.parametrize("architecture", [None, "ARM64"])
def test_terminal_service_pull_failure_has_documented_task_error_category(
        service, pending_launcher, docker_worker, monkeypatch, launch_type, architecture):
    from docker.errors import ImageNotFound

    from ministack.core.responses import get_account_id, get_region

    cluster, svc = service
    client, created = docker_worker
    td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
    if architecture:
        td["runtimePlatform"] = {"cpuArchitecture": architecture}
    svc["launchType"] = launch_type
    client.images.pull_error = True
    def missing(image):
        raise ImageNotFound("cached image absent")
    monkeypatch.setattr(client.images, "get", missing)
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))
    ecs._run_task_worker(task, td, [], client, get_account_id(), get_region())
    assert task["lastStatus"] == "STOPPED"
    assert task["stopCode"] == "TaskFailedToStart"
    assert task["stoppedReason"] == "CannotPullContainerError: image pull failed"
    assert not created
    assert len(client.images.pulls) == (2 if architecture else 1)


@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE"])
def test_all_tasks_can_use_cached_tag_after_a_resolved_digest_pull_fails(
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
    primary = ecs._primary_deployment(svc)
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
    ecs._reconcile_service_tasks(cluster, f"{cluster}/app")
    second = next(task for task in _tasks(svc, deployment=primary).values()
                  if task["lastStatus"] == "PROVISIONING")
    _start_worker(svc, second, client)
    assert second["lastStatus"] == "RUNNING"
    assert second["containers"][0]["imageDigest"] == client.images.digest
    assert created[1]["image"] == "example.invalid/app:latest"
    assert len(created) == 2


def test_successful_first_task_pull_refreshes_original_tag_cache(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    _update(cluster, forceNewDeployment=True)
    first = next(iter(_tasks(svc, deployment=ecs._primary_deployment(svc)).values()))
    _start_worker(svc, first, client)
    assert client.images.tags == [("example.invalid/app", "latest")]
    assert created[0]["image"] == "example.invalid/app@" + client.images.digest
    described = _decoded(ecs._describe_tasks({
        "cluster": cluster, "tasks": [first["taskArn"]],
    }))["tasks"][0]["containers"][0]
    assert described["image"] == "example.invalid/app:latest"
    assert described["imageDigest"] == client.images.digest


def test_following_tasks_wait_for_first_running_task_even_after_manifest_is_known(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, created = docker_worker
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
    first = next(iter(_tasks(svc, deployment=primary).values()))
    td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
    ecs._mark_task_activating(first["taskArn"], first)
    ecs._prepare_service_images(first, td, client)
    assert primary["_image_digests"] == {"app": client.images.digest}
    _update(cluster, desiredCount=2)
    assert pending_launcher == [1]
    assert len(_tasks(svc, deployment=primary)) == 1
    ecs._mark_task_running(first["taskArn"], first)
    ecs._reconcile_service_tasks(cluster, f"{cluster}/app")
    assert pending_launcher == [1, 1]
    assert len(_tasks(svc, deployment=primary)) == 2


def test_canceled_manifest_worker_cannot_publish_or_fail_deployment(
        service, pending_launcher, docker_worker, monkeypatch):
    cluster, svc = service
    client, created = docker_worker
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "maximumPercent": 200, "minimumHealthyPercent": 100,
        "deploymentCircuitBreaker": {"enable": True, "rollback": True},
    })
    primary = ecs._primary_deployment(svc)
    task = next(iter(_tasks(svc, deployment=primary).values()))

    def cancel_during_lookup(image):
        client.images.registry_calls.append(image)
        ecs._mark_task_stopped(task["taskArn"], task, "User canceled task", "UserInitiated")
        raise RuntimeError("registry unavailable after cancellation")

    monkeypatch.setattr(client.images, "get_registry_data", cancel_during_lookup)
    _start_worker(svc, task, client)
    assert task["lastStatus"] == "STOPPED"
    assert task["stopCode"] == "UserInitiated"
    assert len(client.images.registry_calls) == 1
    assert "_image_digests" not in primary
    assert primary["rolloutState"] == "IN_PROGRESS"
    assert primary["failedTasks"] == 0
    assert ecs._primary_deployment(svc) is primary
    assert not created
    # The digest-only transition also rejects a stale completion delivered
    # after cancellation, while ordinary stopped-task failures stay supported.
    ecs._record_service_task_failure(task, digest_resolution_failed=True)
    assert primary["rolloutState"] == "IN_PROGRESS"


@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize("schedule_after_update", [False, True])
def test_predecessor_startup_callback_cannot_complete_crashing_replacement(
        service, pending_launcher, monkeypatch, force, schedule_after_update):
    cluster, svc = service
    _update(cluster, desiredCount=1)
    old = ecs._primary_deployment(svc)
    old_task = next(iter(_tasks(svc, "RUNNING").values()))
    callbacks = []
    monkeypatch.setattr(ecs, "spawn_background", lambda callback, **kwargs: callbacks.append(callback))
    monkeypatch.setattr(ecs.time, "sleep", lambda delay: None)

    def schedule_old():
        from ministack.core.responses import get_account_id, get_region
        ecs._schedule_service_deployment_completion(
            cluster, f"{cluster}/app", get_account_id(), get_region(), healthy_task=old_task)

    if not schedule_after_update:
        schedule_old()
    request = {"forceNewDeployment": True} if force else {
        "taskDefinition": _decoded(ecs._register_task_definition({
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
    primary = ecs._primary_deployment(svc)
    if schedule_after_update:
        schedule_old()
    first = next(iter(_tasks(svc, deployment=primary).values()))
    # The Docker worker publishes RUNNING before its watcher detects exit 1.
    ecs._mark_task_running(first["taskArn"], first)
    callbacks[0]()
    assert primary["rolloutState"] == "IN_PROGRESS"
    assert old_task["lastStatus"] == "RUNNING"

    for _ in range(3):
        task = next(task for task in _tasks(svc, deployment=primary).values()
                    if task["lastStatus"] in ecs._PRE_STOP_STATUSES)
        ecs._mark_task_stopped(task["taskArn"], task,
                              "Essential container in task exited", "EssentialContainerExited", 1)
    assert primary["rolloutState"] == "FAILED"
    assert primary["failedTasks"] == 3
    assert ecs._primary_deployment(svc) is old
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
def test_fargate_platform_controls_resolution_and_first_task_reservation(
        service, pending_launcher, docker_worker, platform, os_family, resolves,
        reported_platform, family):
    cluster, svc = service
    svc["launchType"] = "FARGATE"
    svc["platformVersion"] = platform
    client, created = docker_worker
    td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
    td["runtimePlatform"] = {"operatingSystemFamily": os_family}
    _update(cluster, forceNewDeployment=True)
    primary = ecs._primary_deployment(svc)
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


def test_reported_manifest_digest_survives_persistence(
        service, pending_launcher, docker_worker):
    cluster, svc = service
    client, _ = docker_worker
    _update(cluster, forceNewDeployment=True)
    task = next(iter(_tasks(svc, deployment=ecs._primary_deployment(svc)).values()))
    _start_worker(svc, task, client)
    state = ecs.get_state()
    persisted = state["tasks"][task["taskArn"]]["containers"][0]
    assert persisted["imageDigest"] == client.images.digest
    task["containers"][0]["imageDigest"] = "sha256:" + "f" * 64
    assert persisted["imageDigest"] == client.images.digest


@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE"])
@pytest.mark.parametrize("consistency", ["enabled", "disabled", "digest"])
def test_private_registry_credentials_reach_lookup_and_pull_without_leaking(
        service, pending_launcher, docker_worker, monkeypatch, launch_type, consistency):
    cluster, svc = service
    svc["launchType"] = launch_type
    client, created = docker_worker
    td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
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

    monkeypatch.setattr(ecs.secretsmanager, "resolve_secret_string", resolve)
    lookup = client.images.get_registry_data
    lookup_auth = []

    def authenticated_lookup(image, **kwargs):
        lookup_auth.append(kwargs)
        assert kwargs == {"auth_config": credentials}
        return lookup(image)

    monkeypatch.setattr(client.images, "get_registry_data", authenticated_lookup)
    _update(cluster, forceNewDeployment=True)
    task = next(iter(_tasks(svc, deployment=ecs._primary_deployment(svc)).values()))
    _start_worker(svc, task, client)
    assert task["lastStatus"] == "RUNNING"
    assert bool(lookup_auth) is (consistency == "enabled")
    assert client.images.pulls[-1][1]["auth_config"] == credentials
    assert set(reads) == {secret_id}
    assert "auth_config" not in created[0]
    assert credentials["password"] not in repr(ecs.get_state())
    assert credentials["password"] not in repr(ecs._sanitize(task))


@pytest.mark.parametrize("secret", [None, "not-json", "{}", '{"username":"user"}'])
def test_unavailable_registry_secret_fails_start_without_exposing_secret(
        service, pending_launcher, docker_worker, monkeypatch, secret):
    cluster, svc = service
    client, created = docker_worker
    td = ecs._task_defs[ecs._resolve_td_key(svc["taskDefinition"])]
    td["containerDefinitions"][0]["repositoryCredentials"] = {"credentialsParameter": "registry"}
    monkeypatch.setattr(ecs.secretsmanager, "resolve_secret_string", lambda secret_id: secret)
    _update(cluster, forceNewDeployment=True, deploymentConfiguration={
        "deploymentCircuitBreaker": {"enable": True, "rollback": False},
    })
    task = next(iter(_tasks(svc, deployment=ecs._primary_deployment(svc)).values()))
    # Exercise the worker's existing secret-retrieval error category without
    # starting real Docker watchers or hiding the initialization failure.
    ecs._run_task_worker(task, td, [], client, "000000000000", "us-east-1")
    assert task["lastStatus"] == "STOPPED"
    assert task["stopCode"] == "TaskFailedToStart"
    assert task["stoppedReason"].startswith("ResourceInitializationError: unable to pull secrets or registry auth")
    assert not created
    assert not client.images.pulls
    assert not client.images.registry_calls
    if secret:
        assert secret not in task["stoppedReason"]
