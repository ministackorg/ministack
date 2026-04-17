"""
ECS (Elastic Container Service) Emulator.
REST JSON API — X-Amz-Target header routing with path-based fallback.
Supports 47 operations:
  Clusters:     CreateCluster, DeleteCluster, DescribeClusters, ListClusters,
                UpdateCluster, UpdateClusterSettings, PutClusterCapacityProviders
  Task Defs:    RegisterTaskDefinition, DeregisterTaskDefinition, DescribeTaskDefinition,
                ListTaskDefinitions, ListTaskDefinitionFamilies, DeleteTaskDefinitions
  Services:     CreateService, DeleteService, DescribeServices, UpdateService,
                ListServices, ListServicesByNamespace
  Tasks:        RunTask, StopTask, DescribeTasks, ListTasks, ExecuteCommand,
                UpdateTaskProtection, GetTaskProtection
  Capacity:     CreateCapacityProvider, UpdateCapacityProvider, DeleteCapacityProvider,
                DescribeCapacityProviders
  Tags:         TagResource, UntagResource, ListTagsForResource
  Account:      ListAccountSettings, PutAccountSetting, PutAccountSettingDefault,
                DeleteAccountSetting
  Attributes:   PutAttributes, DeleteAttributes, ListAttributes
  Deployments:  DescribeServiceDeployments, ListServiceDeployments, DescribeServiceRevisions
  Agent:        SubmitTaskStateChange, SubmitContainerStateChange,
                SubmitAttachmentStateChanges, DiscoverPollEndpoint

Container execution: if Docker socket is available, RunTask actually runs containers.
"""

import copy
import json
import logging
import os
import time

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    get_account_id,
    error_response_json,
    json_response,
    new_uuid,
    now_iso,
)

logger = logging.getLogger("ecs")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_clusters = AccountScopedDict()
_task_defs = AccountScopedDict()
_task_def_latest = AccountScopedDict()
_services = AccountScopedDict()
_tasks = AccountScopedDict()
_tags = AccountScopedDict()
_account_settings = AccountScopedDict()
_capacity_providers = AccountScopedDict()

_docker = None


# ── Persistence ────────────────────────────────────────────

def get_state():
    state = {
        "clusters": copy.deepcopy(_clusters),
        "task_defs": copy.deepcopy(_task_defs),
        "task_def_latest": copy.deepcopy(_task_def_latest),
        "services": copy.deepcopy(_services),
        "tags": copy.deepcopy(_tags),
        "account_settings": copy.deepcopy(_account_settings),
        "capacity_providers": copy.deepcopy(_capacity_providers),
    }
    # Save tasks but strip Docker container IDs.
    # Iterate _data directly to capture ALL accounts.
    from ministack.core.responses import AccountScopedDict
    tasks = AccountScopedDict()
    for scoped_key, task in _tasks._data.items():
        t = copy.deepcopy(task)
        t.pop("_docker_ids", None)
        tasks._data[scoped_key] = t
    state["tasks"] = tasks
    return state


def restore_state(data):
    if not data:
        return
    _clusters.update(data.get("clusters", {}))
    _task_defs.update(data.get("task_defs", {}))
    _task_def_latest.update(data.get("task_def_latest", {}))
    _services.update(data.get("services", {}))
    _tags.update(data.get("tags", {}))
    _account_settings.update(data.get("account_settings", {}))
    _capacity_providers.update(data.get("capacity_providers", {}))
    from ministack.core.responses import AccountScopedDict
    tasks_data = data.get("tasks", {})
    if isinstance(tasks_data, AccountScopedDict):
        for scoped_key, task in tasks_data._data.items():
            task["_docker_ids"] = []
            task["lastStatus"] = "STOPPED"
            _tasks._data[scoped_key] = task
    else:
        for arn, task in tasks_data.items():
            task["_docker_ids"] = []
            task["lastStatus"] = "STOPPED"
            _tasks[arn] = task


_restored = load_state("ecs")
if _restored:
    restore_state(_restored)


def _get_docker():
    global _docker
    if _docker is None:
        try:
            import docker
            _docker = docker.from_env()
        except Exception:
            pass
    return _docker


def _ts():
    return time.time()


def _iso():
    return now_iso()


# ---------------------------------------------------------------------------
# Request routing
# ---------------------------------------------------------------------------

_ECS_TIMESTAMP_FIELDS = {
    "createdAt", "startedAt", "stoppedAt", "registeredAt",
    "deregisteredAt", "updatedAt", "lastModified", "runningAt",
    "pullStartedAt", "pullStoppedAt", "executionStoppedAt",
}


def _ts_to_epoch(value):
    if not isinstance(value, str):
        return value
    try:
        from datetime import datetime, timezone
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except (ValueError, TypeError):
        return value


def _normalize_ecs_timestamps(payload, field_name=None):
    if isinstance(payload, dict):
        return {k: _normalize_ecs_timestamps(v, k) for k, v in payload.items()}
    if isinstance(payload, list):
        return [_normalize_ecs_timestamps(item, field_name) for item in payload]
    if field_name in _ECS_TIMESTAMP_FIELDS:
        return _ts_to_epoch(payload)
    return payload


def _finalize_ecs_response(response):
    status, headers_resp, body = response
    if not body:
        return response
    try:
        payload = json.loads(body)
    except (TypeError, ValueError):
        return response
    normalized = _normalize_ecs_timestamps(payload)
    if normalized == payload:
        return response
    return json_response(normalized, status)


async def handle_request(method, path, headers, body, query_params):
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    if method == "GET":
        for k, v in query_params.items():
            if k not in data:
                data[k] = v[0] if isinstance(v, list) and len(v) == 1 else v

    target = headers.get("x-amz-target", "") or headers.get("X-Amz-Target", "")
    if target:
        action = target.split(".")[-1]
        return _finalize_ecs_response(_dispatch_action(action, data))

    parts = [p for p in path.strip("/").split("/") if p]
    if not parts:
        return error_response_json("InvalidRequest", "Missing path", 400)

    return _finalize_ecs_response(_dispatch_path(method, parts, data))


def _dispatch_action(action, data):
    handler = _ACTION_MAP.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown ECS action: {action}", 400)
    return handler(data)


def _dispatch_path(method, parts, data):
    resource = parts[0]

    if resource == "clusters":
        if method == "GET" and len(parts) == 1:
            return _list_clusters(data)
        if method == "POST" and len(parts) == 2 and parts[1] == "delete":
            return _delete_cluster(data)
        if method == "POST" and len(parts) == 1:
            if "clusters" in data and isinstance(data["clusters"], list):
                return _describe_clusters(data)
            return _create_cluster(data)

    if resource == "taskdefinitions":
        if method == "POST" and len(parts) == 1:
            return _register_task_definition(data)
        if method == "GET" and len(parts) == 1:
            return _list_task_definitions(data)
        if method == "GET" and len(parts) == 2:
            return _describe_task_definition({"taskDefinition": parts[1]})
        if method == "DELETE" and len(parts) == 2:
            return _deregister_task_definition({"taskDefinition": parts[1]})

    if resource == "tasks":
        if method == "POST" and len(parts) == 1:
            if "taskDefinition" in data:
                return _run_task(data)
            if "tasks" in data:
                return _describe_tasks(data)
        if method == "GET" and len(parts) == 1:
            return _list_tasks(data)

    if resource == "services":
        if method == "GET" and len(parts) == 1:
            return _list_services(data)
        if method == "POST" and len(parts) == 1:
            if "services" in data and isinstance(data["services"], list):
                return _describe_services(data)
            if "serviceName" in data:
                return _create_service(data)
        if method == "PUT" and len(parts) == 2:
            data.setdefault("service", parts[1])
            return _update_service(data)
        if method == "DELETE" and len(parts) == 2:
            data.setdefault("service", parts[1])
            return _delete_service(data)

    if resource == "stoptask":
        return _stop_task(data)

    if resource == "tags":
        if method == "POST":
            return _tag_resource(data)
        if method == "DELETE":
            return _untag_resource(data)
        if method == "GET":
            return _list_tags_for_resource(data)

    return error_response_json("InvalidRequest", f"Unknown ECS path: /{'/'.join(parts)}", 400)


# ---------------------------------------------------------------------------
# Clusters
# ---------------------------------------------------------------------------

def _create_cluster(data):
    name = data.get("clusterName", "default")
    if name in _clusters and _clusters[name]["status"] == "ACTIVE":
        return json_response({"cluster": _clusters[name]})

    arn = f"arn:aws:ecs:{REGION}:{get_account_id()}:cluster/{name}"
    cluster = {
        "clusterArn": arn,
        "clusterName": name,
        "status": "ACTIVE",
        "registeredContainerInstancesCount": 0,
        "runningTasksCount": 0,
        "pendingTasksCount": 0,
        "activeServicesCount": 0,
        "tags": data.get("tags", []),
        "settings": data.get("settings", [
            {"name": "containerInsights", "value": "disabled"},
        ]),
        "capacityProviders": data.get("capacityProviders", []),
        "defaultCapacityProviderStrategy": data.get("defaultCapacityProviderStrategy", []),
        "statistics": [],
        "attachments": [],
        "attachmentsStatus": "",
    }
    _clusters[name] = cluster
    if cluster["tags"]:
        _tags[arn] = list(cluster["tags"])
    return json_response({"cluster": cluster})


def _delete_cluster(data):
    name = _resolve_cluster_name(data.get("cluster", "default"))
    cluster = _clusters.pop(name, None)
    if not cluster:
        return error_response_json("ClusterNotFoundException", "Cluster not found.", 400)
    cluster["status"] = "INACTIVE"
    _tags.pop(cluster["clusterArn"], None)
    return json_response({"cluster": cluster})


def _describe_clusters(data):
    names = data.get("clusters", ["default"])
    include = set(data.get("include", []))
    result = []
    failures = []
    for ref in names:
        n = _resolve_cluster_name(ref)
        if n in _clusters:
            c = dict(_clusters[n])
            if "TAGS" in include:
                c["tags"] = _tags.get(c["clusterArn"], [])
            _recount_cluster(n)
            c.update({
                "runningTasksCount": _clusters[n]["runningTasksCount"],
                "pendingTasksCount": _clusters[n]["pendingTasksCount"],
                "activeServicesCount": _clusters[n]["activeServicesCount"],
            })
            result.append(c)
        else:
            arn = ref if ref.startswith("arn:") else f"arn:aws:ecs:{REGION}:{get_account_id()}:cluster/{ref}"
            failures.append({"arn": arn, "reason": "MISSING"})
    return json_response({"clusters": result, "failures": failures})


def _list_clusters(data):
    arns = [c["clusterArn"] for c in _clusters.values() if c["status"] == "ACTIVE"]
    return json_response({"clusterArns": arns})


def _recount_cluster(cluster_name):
    """Recompute cluster-level counts from live tasks and services."""
    cluster = _clusters.get(cluster_name)
    if not cluster:
        return
    cluster_arn = cluster["clusterArn"]
    running = 0
    pending = 0
    for t in _tasks.values():
        if t.get("clusterArn") != cluster_arn:
            continue
        if t["lastStatus"] == "RUNNING":
            running += 1
        elif t["lastStatus"] == "PENDING":
            pending += 1
    cluster["runningTasksCount"] = running
    cluster["pendingTasksCount"] = pending
    active_svcs = sum(
        1 for k, s in _services.items()
        if k.startswith(f"{cluster_name}/") and s["status"] == "ACTIVE"
    )
    cluster["activeServicesCount"] = active_svcs


# ---------------------------------------------------------------------------
# Task Definitions
# ---------------------------------------------------------------------------

def _register_task_definition(data):
    family = data.get("family")
    if not family:
        return error_response_json("ClientException", "family is required", 400)

    container_defs = data.get("containerDefinitions", [])
    if not container_defs:
        return error_response_json("ClientException",
            "TaskDefinition must contain at least one container definition.", 400)
    for idx, cdef in enumerate(container_defs):
        if "name" not in cdef:
            return error_response_json("ClientException",
                f"Container definition {idx}: name is required.", 400)
        if "image" not in cdef:
            return error_response_json("ClientException",
                f"Container definition {idx} ({cdef['name']}): image is required.", 400)
        cdef.setdefault("cpu", 0)
        cdef.setdefault("essential", True)

    rev = _task_def_latest.get(family, 0) + 1
    _task_def_latest[family] = rev
    td_key = f"{family}:{rev}"
    arn = f"arn:aws:ecs:{REGION}:{get_account_id()}:task-definition/{td_key}"

    compat = data.get("requiresCompatibilities", ["EC2"])
    network_mode = data.get("networkMode", "awsvpc" if "FARGATE" in compat else "bridge")

    td = {
        "taskDefinitionArn": arn,
        "family": family,
        "revision": rev,
        "status": "ACTIVE",
        "containerDefinitions": container_defs,
        "volumes": data.get("volumes", []),
        "placementConstraints": data.get("placementConstraints", []),
        "networkMode": network_mode,
        "requiresCompatibilities": compat,
        "cpu": data.get("cpu", "256"),
        "memory": data.get("memory", "512"),
        "executionRoleArn": data.get("executionRoleArn", ""),
        "taskRoleArn": data.get("taskRoleArn", ""),
        "pidMode": data.get("pidMode", ""),
        "ipcMode": data.get("ipcMode", ""),
        "proxyConfiguration": data.get("proxyConfiguration", None),
        "runtimePlatform": data.get("runtimePlatform", None),
        "ephemeralStorage": data.get("ephemeralStorage", None),
        "registeredAt": _iso(),
        "registeredBy": f"arn:aws:iam::{get_account_id()}:root",
        "compatibilities": compat + (["EC2"] if "FARGATE" in compat and "EC2" not in compat else []),
    }
    _task_defs[td_key] = td

    req_tags = data.get("tags", [])
    if req_tags:
        _tags[arn] = list(req_tags)
    return json_response({"taskDefinition": td, "tags": req_tags})


def _deregister_task_definition(data):
    td_ref = data.get("taskDefinition", "")
    key = _resolve_td_key(td_ref)
    td = _task_defs.get(key)
    if not td:
        return error_response_json("ClientException",
            f"Unable to describe task definition: {td_ref}", 400)
    td["status"] = "INACTIVE"
    td["deregisteredAt"] = _iso()
    return json_response({"taskDefinition": td})


def _describe_task_definition(data):
    td_ref = data.get("taskDefinition", "") if isinstance(data, dict) else data
    key = _resolve_td_key(td_ref)
    td = _task_defs.get(key)
    if not td:
        return error_response_json("ClientException",
            f"Unable to describe task definition: {td_ref}", 400)
    resp = {"taskDefinition": td}
    resp["tags"] = _tags.get(td["taskDefinitionArn"], [])
    return json_response(resp)


def _list_task_definitions(data):
    family_prefix = data.get("familyPrefix", "")
    status_filter = data.get("status", "ACTIVE")
    sort = data.get("sort", "ASC")
    arns = [
        td["taskDefinitionArn"] for td in _task_defs.values()
        if (not family_prefix or td["family"].startswith(family_prefix))
        and td["status"] == status_filter
    ]
    if sort == "DESC":
        arns.reverse()
    max_results = data.get("maxResults", 100)
    next_token = data.get("nextToken")
    start = int(next_token) if next_token else 0
    page = arns[start:start + max_results]
    resp = {"taskDefinitionArns": page}
    if start + max_results < len(arns):
        resp["nextToken"] = str(start + max_results)
    return json_response(resp)


# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------

def _make_deployment(task_definition, desired_count, status="PRIMARY"):
    dep_id = f"ecs-svc/{new_uuid().replace('-', '')[:20]}"
    now = _iso()
    return {
        "id": dep_id,
        "status": status,
        "taskDefinition": task_definition,
        "desiredCount": desired_count,
        "runningCount": 0,
        "pendingCount": 0,
        "failedTasks": 0,
        "launchType": "EC2",
        "createdAt": now,
        "updatedAt": now,
        "rolloutState": "COMPLETED" if status == "PRIMARY" else "IN_PROGRESS",
        "rolloutStateReason": "ECS deployment completed." if status == "PRIMARY" else "",
    }


def _reconcile_service_tasks(cluster_name, svc_key):
    """Spawn or stop tasks so running tasks match desiredCount and task definition."""
    svc = _services.get(svc_key)
    if not svc or svc["status"] != "ACTIVE":
        return

    svc_name = svc["serviceName"]
    desired = svc.get("desiredCount", 0)
    td_arn = svc.get("taskDefinition", "")
    cluster_arn = svc.get("clusterArn", "")
    launch_type = svc.get("launchType", "EC2")
    network_cfg = svc.get("networkConfiguration", {})

    # Resolve the target task definition ARN for comparison
    td_key = _resolve_td_key(td_arn)
    td = _task_defs.get(td_key)
    target_td_arn = td["taskDefinitionArn"] if td else td_arn

    # Partition running service tasks into current-TD and stale-TD
    current_tasks = []
    stale_tasks = []
    for arn, t in _tasks.items():
        if (t.get("group") == f"service:{svc_name}"
                and t.get("clusterArn") == cluster_arn
                and t.get("lastStatus") == "RUNNING"):
            if t.get("taskDefinitionArn") == target_td_arn:
                current_tasks.append((arn, t))
            else:
                stale_tasks.append((arn, t))

    # Stop tasks running on a stale task definition
    for task_arn, _ in stale_tasks:
        _stop_task({"task": task_arn, "cluster": cluster_name,
                     "reason": "Task definition updated"})

    # Scale up: spawn tasks to reach desiredCount
    to_spawn = desired - len(current_tasks)
    if to_spawn > 0:
        if not td:
            svc["runningCount"] = desired
            if svc["deployments"]:
                svc["deployments"][0]["runningCount"] = desired
            return
        _run_task({
            "cluster": cluster_name,
            "taskDefinition": td_arn,
            "count": to_spawn,
            "group": f"service:{svc_name}",
            "startedBy": svc_name,
            "launchType": launch_type,
            "networkConfiguration": network_cfg,
            "enableExecuteCommand": svc.get("enableExecuteCommand", False),
        })
    elif to_spawn < 0:
        # Scale down: stop newest tasks first
        excess = -to_spawn
        current_tasks.sort(key=lambda x: x[1].get("createdAt", ""), reverse=True)
        for task_arn, _ in current_tasks[:excess]:
            _stop_task({"task": task_arn, "cluster": cluster_name,
                         "reason": "Service scaling down"})

    # Recount actual running tasks
    running = sum(
        1 for t in _tasks.values()
        if t.get("group") == f"service:{svc_name}"
        and t.get("clusterArn") == cluster_arn
        and t.get("lastStatus") == "RUNNING"
    )
    svc["runningCount"] = running
    if svc["deployments"]:
        svc["deployments"][0]["runningCount"] = running
    _recount_cluster(cluster_name)


def _create_service(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    if cluster_name not in _clusters:
        _create_cluster({"clusterName": cluster_name})

    name = data.get("serviceName")
    if not name:
        return error_response_json("ClientException", "serviceName is required", 400)

    svc_key = f"{cluster_name}/{name}"
    if svc_key in _services and _services[svc_key]["status"] == "ACTIVE":
        return error_response_json("ServiceAlreadyExists",
            "Creation of service was not idempotent.", 400)

    td_ref = data.get("taskDefinition", "")
    td_key = _resolve_td_key(td_ref)
    td_arn = _task_defs[td_key]["taskDefinitionArn"] if td_key in _task_defs else td_ref

    desired = data.get("desiredCount", 1)
    arn = f"arn:aws:ecs:{REGION}:{get_account_id()}:service/{cluster_name}/{name}"
    now = _iso()
    launch_type = data.get("launchType", "EC2")

    deployment = _make_deployment(td_arn, desired)
    deployment["launchType"] = launch_type

    svc = {
        "serviceArn": arn,
        "serviceName": name,
        "clusterArn": _clusters[cluster_name]["clusterArn"],
        "taskDefinition": td_arn,
        "desiredCount": desired,
        "runningCount": 0,
        "pendingCount": 0,
        "status": "ACTIVE",
        "launchType": launch_type,
        "platformVersion": data.get("platformVersion", ""),
        "platformFamily": data.get("platformFamily", ""),
        "networkConfiguration": data.get("networkConfiguration", {}),
        "loadBalancers": data.get("loadBalancers", []),
        "serviceRegistries": data.get("serviceRegistries", []),
        "healthCheckGracePeriodSeconds": data.get("healthCheckGracePeriodSeconds", 0),
        "schedulingStrategy": data.get("schedulingStrategy", "REPLICA"),
        "deploymentController": data.get("deploymentController", {"type": "ECS"}),
        "deploymentConfiguration": data.get("deploymentConfiguration", {
            "maximumPercent": 200,
            "minimumHealthyPercent": 100,
            "deploymentCircuitBreaker": {"enable": False, "rollback": False},
        }),
        "deployments": [deployment],
        "events": [],
        "roleArn": data.get("role", ""),
        "createdAt": now,
        "createdBy": f"arn:aws:iam::{get_account_id()}:root",
        "enableECSManagedTags": data.get("enableECSManagedTags", False),
        "propagateTags": data.get("propagateTags", "NONE"),
        "enableExecuteCommand": data.get("enableExecuteCommand", False),
        "tags": data.get("tags", []),
    }
    _services[svc_key] = svc

    if svc["tags"]:
        _tags[arn] = list(svc["tags"])

    _reconcile_service_tasks(cluster_name, svc_key)
    return json_response({"service": _sanitize(svc)})


def _delete_service(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    service_ref = data.get("service", "")
    svc_name = _resolve_service_name(service_ref)
    svc_key = f"{cluster_name}/{svc_name}"
    svc = _services.get(svc_key)
    if not svc:
        return error_response_json("ServiceNotFoundException",
            "Service not found.", 400)

    force = data.get("force", False)
    if not force and svc.get("desiredCount", 0) > 0:
        return error_response_json("InvalidParameterException",
            "The service cannot be stopped while it is scaled above 0.", 400)

    svc["status"] = "DRAINING"
    svc["desiredCount"] = 0

    # Stop all tasks belonging to this service
    cluster_arn = svc.get("clusterArn", "")
    for task_arn in [
        a for a, t in _tasks.items()
        if t.get("group") == f"service:{svc_name}"
        and t.get("clusterArn") == cluster_arn
        and t.get("lastStatus") == "RUNNING"
    ]:
        _stop_task({"task": task_arn, "cluster": cluster_name, "reason": "Service deleted"})

    svc["runningCount"] = 0
    _tags.pop(svc["serviceArn"], None)
    del _services[svc_key]

    _recount_cluster(cluster_name)
    return json_response({"service": _sanitize(svc)})


def _describe_services(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    refs = data.get("services", [])
    include = set(data.get("include", []))
    result = []
    failures = []
    for ref in refs:
        svc_name = _resolve_service_name(ref)
        svc_key = f"{cluster_name}/{svc_name}"
        if svc_key in _services:
            s = dict(_services[svc_key])
            if "TAGS" in include:
                s["tags"] = _tags.get(s["serviceArn"], [])
            result.append(_sanitize(s))
        else:
            arn = ref if ref.startswith("arn:") else \
                f"arn:aws:ecs:{REGION}:{get_account_id()}:service/{cluster_name}/{ref}"
            failures.append({"arn": arn, "reason": "MISSING"})
    return json_response({"services": result, "failures": failures})


def _update_service(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    service_ref = data.get("service", "")
    svc_name = _resolve_service_name(service_ref)
    svc_key = f"{cluster_name}/{svc_name}"
    svc = _services.get(svc_key)
    if not svc:
        return error_response_json("ServiceNotFoundException", "Service not found.", 400)

    changed = False
    new_td = data.get("taskDefinition")
    new_desired = data.get("desiredCount")

    if new_td is not None:
        td_key = _resolve_td_key(new_td)
        td_arn = _task_defs[td_key]["taskDefinitionArn"] if td_key in _task_defs else new_td
        if td_arn != svc["taskDefinition"]:
            for dep in svc["deployments"]:
                if dep["status"] == "PRIMARY":
                    dep["status"] = "ACTIVE"
            new_dep = _make_deployment(td_arn, svc["desiredCount"])
            svc["deployments"].insert(0, new_dep)
            svc["taskDefinition"] = td_arn
            changed = True

    if new_desired is not None:
        svc["desiredCount"] = new_desired
        if svc["deployments"]:
            svc["deployments"][0]["desiredCount"] = new_desired
            svc["deployments"][0]["updatedAt"] = _iso()
        changed = True

    if "networkConfiguration" in data:
        svc["networkConfiguration"] = data["networkConfiguration"]
    if "healthCheckGracePeriodSeconds" in data:
        svc["healthCheckGracePeriodSeconds"] = data["healthCheckGracePeriodSeconds"]
    if "enableExecuteCommand" in data:
        svc["enableExecuteCommand"] = data["enableExecuteCommand"]
    if "deploymentConfiguration" in data:
        svc["deploymentConfiguration"] = data["deploymentConfiguration"]
    if "platformVersion" in data:
        svc["platformVersion"] = data["platformVersion"]
    if "loadBalancers" in data:
        svc["loadBalancers"] = data["loadBalancers"]
    if "capacityProviderStrategy" in data:
        svc["capacityProviderStrategy"] = data["capacityProviderStrategy"]

    if changed:
        svc["events"].insert(0, {
            "id": new_uuid(),
            "createdAt": _iso(),
            "message": f"(service {svc['serviceName']}) has begun draining connections on 1 tasks.",
        })

    _reconcile_service_tasks(cluster_name, svc_key)
    return json_response({"service": _sanitize(svc)})


def _list_services(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    launch_type = data.get("launchType")
    scheduling = data.get("schedulingStrategy")
    arns = []
    for k, s in _services.items():
        if not k.startswith(f"{cluster_name}/"):
            continue
        if s["status"] != "ACTIVE":
            continue
        if launch_type and s.get("launchType") != launch_type:
            continue
        if scheduling and s.get("schedulingStrategy") != scheduling:
            continue
        arns.append(s["serviceArn"])
    return json_response({"serviceArns": arns})


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

def _build_task_containers(td, container_overrides):
    """Build the containers list for a task from its task definition."""
    containers = []
    if not td:
        return containers
    for cdef in td.get("containerDefinitions", []):
        env_override = {}
        for ov in container_overrides:
            if ov.get("name") == cdef["name"]:
                for e in ov.get("environment", []):
                    env_override[e["name"]] = e["value"]

        env = {e["name"]: e["value"] for e in cdef.get("environment", [])}
        env.update(env_override)

        containers.append({
            "containerArn": f"arn:aws:ecs:{REGION}:{get_account_id()}:container/{new_uuid()}",
            "taskArn": "",
            "name": cdef["name"],
            "image": cdef.get("image", ""),
            "lastStatus": "RUNNING",
            "exitCode": None,
            "networkBindings": [],
            "networkInterfaces": [],
            "cpu": str(cdef.get("cpu", 0)),
            "memory": str(cdef.get("memory") or cdef.get("memoryReservation", 0)),
            "runtimeId": new_uuid()[:12],
            "healthStatus": "UNKNOWN",
        })
    return containers


def _run_task(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    if cluster_name not in _clusters:
        _create_cluster({"clusterName": cluster_name})

    td_ref = data.get("taskDefinition", "")
    td_key = _resolve_td_key(td_ref)
    td = _task_defs.get(td_key)
    if not td:
        return error_response_json("ClientException",
            f"Unable to find task definition: {td_ref}", 400)

    count = data.get("count", 1)
    container_overrides = data.get("overrides", {}).get("containerOverrides", [])
    launch_type = data.get("launchType", "EC2")
    group = data.get("group", "")
    started_by = data.get("startedBy", "")
    enable_exec = data.get("enableExecuteCommand", False)
    network_cfg = data.get("networkConfiguration", {})
    req_tags = data.get("tags", [])

    tasks = []
    failures = []

    for _ in range(count):
        task_id = new_uuid()
        task_arn = f"arn:aws:ecs:{REGION}:{get_account_id()}:task/{cluster_name}/{task_id}"
        now = _iso()

        containers = _build_task_containers(td, container_overrides)
        for c in containers:
            c["taskArn"] = task_arn

        task = {
            "taskArn": task_arn,
            "clusterArn": _clusters[cluster_name]["clusterArn"],
            "taskDefinitionArn": td["taskDefinitionArn"],
            "containerInstanceArn": f"arn:aws:ecs:{REGION}:{get_account_id()}:container-instance/{cluster_name}/{new_uuid()}",
            "overrides": data.get("overrides", {"containerOverrides": [], "inferenceAcceleratorOverrides": []}),
            "lastStatus": "RUNNING",
            "desiredStatus": "RUNNING",
            "launchType": launch_type,
            "cpu": td.get("cpu", "256"),
            "memory": td.get("memory", "512"),
            "platformVersion": data.get("platformVersion", ""),
            "platformFamily": "",
            "connectivity": "CONNECTED",
            "connectivityAt": now,
            "pullStartedAt": now,
            "pullStoppedAt": now,
            "createdAt": now,
            "startedAt": now,
            "stoppingAt": None,
            "stoppedAt": None,
            "stoppedReason": "",
            "stopCode": "",
            "group": group,
            "startedBy": started_by,
            "version": 1,
            "containers": containers,
            "attachments": [],
            "availabilityZone": f"{REGION}a",
            "enableExecuteCommand": enable_exec,
            "tags": req_tags,
            "healthStatus": "UNKNOWN",
            "ephemeralStorage": td.get("ephemeralStorage", {"sizeInGiB": 20}),
            "_docker_ids": [],
        }

        docker_client = _get_docker()
        if docker_client and td:
            # Detect the Docker network Ministack is running on,
            # so ECS containers can reach sibling services (S3, etc.)
            ecs_network = None
            try:
                self_container = docker_client.containers.get(os.environ.get("HOSTNAME", ""))
                nets = list(self_container.attrs["NetworkSettings"]["Networks"].keys())
                if nets:
                    ecs_network = nets[0]
                    logger.debug("ECS: detected Ministack network: %s", ecs_network)
            except Exception:
                logger.debug("ECS: could not detect Ministack network, using default")

            for i, cdef in enumerate(td.get("containerDefinitions", [])):
                env_override = {}
                for ov in container_overrides:
                    if ov.get("name") == cdef["name"]:
                        for e in ov.get("environment", []):
                            env_override[e["name"]] = e["value"]

                env = {e["name"]: e["value"] for e in cdef.get("environment", [])}
                env.update(env_override)

                port_bindings = {}
                for pm in cdef.get("portMappings", []):
                    host_port = pm.get("hostPort", pm.get("containerPort"))
                    port_bindings[f"{pm['containerPort']}/tcp"] = host_port

                try:
                    container = docker_client.containers.run(
                        cdef["image"], detach=True,
                        environment=env,
                        ports=port_bindings or None,
                        name=f"ministack-ecs-{task_id[:8]}-{cdef['name']}",
                        labels={"ministack": "ecs", "task_arn": task_arn},
                        network=ecs_network,
                        command=cdef["command"]
                    )
                    task["_docker_ids"].append(container.id)
                    if i < len(task["containers"]):
                        task["containers"][i]["runtimeId"] = container.id[:12]
                    logger.info("ECS: started container %s for task %s", cdef['image'], task_id[:8])
                except Exception as e:
                    logger.warning("ECS: Docker run failed for %s: %s", cdef.get('image'), e)

        _tasks[task_arn] = task
        if req_tags:
            _tags[task_arn] = list(req_tags)
        tasks.append(_sanitize(task))

    _recount_cluster(cluster_name)
    return json_response({"tasks": tasks, "failures": failures})


def _stop_task(data):
    task_ref = data.get("task", "")
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    reason = data.get("reason", "Task stopped by user")

    task = _resolve_task(task_ref, cluster_name)
    if not task:
        return error_response_json("InvalidParameterException",
            "The referenced task was not found.", 400)

    if task["lastStatus"] == "STOPPED":
        return json_response({"task": _sanitize(task)})

    docker_client = _get_docker()
    if docker_client:
        for docker_id in task.get("_docker_ids", []):
            try:
                c = docker_client.containers.get(docker_id)
                c.stop(timeout=5)
                c.remove(v=True)
            except Exception as e:
                logger.warning("ECS: failed to stop container %s: %s", docker_id, e)

    now = _iso()
    task["lastStatus"] = "STOPPED"
    task["desiredStatus"] = "STOPPED"
    task["stoppingAt"] = now
    task["stoppedAt"] = now
    task["stoppedReason"] = reason
    task["stopCode"] = "UserInitiated"
    for c in task.get("containers", []):
        c["lastStatus"] = "STOPPED"
        c["exitCode"] = 0

    cname = _cluster_name_from_arn(task.get("clusterArn", ""))
    if cname:
        _recount_cluster(cname)

    return json_response({"task": _sanitize(task)})


def _describe_tasks(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    task_refs = data.get("tasks", [])
    include = set(data.get("include", []))
    result = []
    failures = []
    for ref in task_refs:
        task = _resolve_task(ref, cluster_name)
        if task:
            _maybe_mark_stopped(task)
            t = _sanitize(task)
            if "TAGS" in include:
                t["tags"] = _tags.get(task["taskArn"], [])
            result.append(t)
        else:
            arn = ref if ref.startswith("arn:") else \
                f"arn:aws:ecs:{REGION}:{get_account_id()}:task/{cluster_name}/{ref}"
            failures.append({"arn": arn, "reason": "MISSING"})
    return json_response({"tasks": result, "failures": failures})


def _maybe_mark_stopped(task):
    """Check Docker containers and transition task to STOPPED if all have exited."""
    if task.get("lastStatus") != "RUNNING" or not task.get("_docker_ids"):
        return

    docker_client = _get_docker()
    if not docker_client:
        return

    all_stopped = True
    exit_code = 0
    for docker_id in task["_docker_ids"]:
        try:
            container = docker_client.containers.get(docker_id)
            # docker SDK caches status; refresh before checking lifecycle
            try:
                container.reload()
            except Exception:
                pass
            if getattr(container, "status", None) != "exited":
                all_stopped = False
                break
            result = container.wait()
            exit_code = max(exit_code, result.get("StatusCode", 0))
        except Exception:
            # Container removed or unreachable — treat as stopped
            pass

    if not all_stopped:
        return

    now = _iso()
    task["lastStatus"] = "STOPPED"
    task["desiredStatus"] = "STOPPED"
    task["stoppingAt"] = task.get("stoppingAt") or now
    task["stoppedAt"] = now
    task["stoppedReason"] = "Essential container exited"
    task["stopCode"] = "EssentialContainerExited"
    for c in task.get("containers", []):
        c["lastStatus"] = "STOPPED"
        c["exitCode"] = exit_code

    cname = _cluster_name_from_arn(task.get("clusterArn", ""))
    if cname:
        _recount_cluster(cname)


def _list_tasks(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    cluster_arn = f"arn:aws:ecs:{REGION}:{get_account_id()}:cluster/{cluster_name}"
    status_filter = data.get("desiredStatus", "RUNNING")
    family = data.get("family", "")
    service_name = data.get("serviceName", "")
    started_by = data.get("startedBy", "")

    arns = []
    for arn, t in _tasks.items():
        if t.get("clusterArn") != cluster_arn:
            continue
        if t.get("desiredStatus") != status_filter:
            continue
        if family:
            td_arn = t.get("taskDefinitionArn", "")
            if f"/{family}:" not in td_arn:
                continue
        if service_name and t.get("group") != f"service:{service_name}":
            if t.get("startedBy") != service_name:
                continue
        if started_by and t.get("startedBy") != started_by:
            continue
        arns.append(arn)

    max_results = data.get("maxResults", 100)
    next_token = data.get("nextToken")
    start = int(next_token) if next_token else 0
    page = arns[start:start + max_results]
    resp = {"taskArns": page}
    if start + max_results < len(arns):
        resp["nextToken"] = str(start + max_results)
    return json_response(resp)


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(data):
    arn = data.get("resourceArn", "")
    new_tags = data.get("tags", [])
    if not arn:
        return error_response_json("InvalidParameterException", "resourceArn is required", 400)
    # Validate the resource exists by checking all known ARN stores
    found = (
        any(c.get("clusterArn") == arn for c in _clusters.values())
        or any(td.get("taskDefinitionArn") == arn for td in _task_defs.values())
        or any(svc.get("serviceArn") == arn for svc in _services.values())
        or any(t.get("taskArn") == arn for t in _tasks.values())
        or arn in _tags
    )
    if not found:
        return error_response_json("InvalidParameterException", f"The specified resource is not valid.", 400)
    existing = _tags.get(arn, [])
    existing_keys = {t["key"]: i for i, t in enumerate(existing)}
    for tag in new_tags:
        k = tag.get("key", "")
        if k in existing_keys:
            existing[existing_keys[k]] = tag
        else:
            existing.append(tag)
            existing_keys[k] = len(existing) - 1
    _tags[arn] = existing
    return json_response({})


def _untag_resource(data):
    arn = data.get("resourceArn", "")
    keys_to_remove = set(data.get("tagKeys", []))
    if not arn:
        return error_response_json("InvalidParameterException", "resourceArn is required", 400)
    existing = _tags.get(arn, [])
    _tags[arn] = [t for t in existing if t.get("key") not in keys_to_remove]
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("resourceArn", "")
    if not arn:
        return error_response_json("InvalidParameterException", "resourceArn is required", 400)
    return json_response({"tags": _tags.get(arn, [])})


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

def _execute_command(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    task_ref = data.get("task", "")
    task = _resolve_task(task_ref, cluster_name)
    if not task:
        return error_response_json("InvalidParameterException",
            "The referenced task was not found.", 400)
    container_name = data.get("container", "")
    if not container_name and task.get("containers"):
        container_name = task["containers"][0]["name"]
    return json_response({
        "clusterArn": task["clusterArn"],
        "taskArn": task["taskArn"],
        "containerArn": next(
            (c["containerArn"] for c in task.get("containers", []) if c["name"] == container_name),
            "",
        ),
        "containerName": container_name,
        "interactive": data.get("interactive", True),
        "session": {
            "sessionId": new_uuid(),
            "streamUrl": f"wss://ssmmessages.{REGION}.amazonaws.com/v1/data-channel/{new_uuid()}",
            "tokenValue": new_uuid(),
        },
    })


def _list_account_settings(data):
    name = data.get("name", "")
    effective = data.get("effectiveSettings", False)
    settings = []
    all_names = [
        "serviceLongArnFormat", "taskLongArnFormat",
        "containerInstanceLongArnFormat", "awsvpcTrunking",
        "containerInsights", "fargateTaskRetirementWaitPeriod",
        "dualStackIPv6", "fargateFIPSMode", "tagResourceAuthorization",
        "guardDutyActivate",
    ]
    for setting_name in all_names:
        if name and setting_name != name:
            continue
        settings.append({
            "name": setting_name,
            "value": _account_settings.get(setting_name, "enabled"),
            "principalArn": f"arn:aws:iam::{get_account_id()}:root",
            "type": "user" if setting_name in _account_settings else "aws",
        })
    return json_response({"settings": settings})


def _put_account_setting(data):
    name = data.get("name", "")
    value = data.get("value", "enabled")
    if not name:
        return error_response_json("InvalidParameterException", "name is required", 400)
    _account_settings[name] = value
    return json_response({"setting": {
        "name": name,
        "value": value,
        "principalArn": f"arn:aws:iam::{get_account_id()}:root",
        "type": "user",
    }})


def _describe_capacity_providers(data):
    names = data.get("capacityProviders", [])
    include = data.get("include", [])
    providers = []
    defaults = [
        {"name": "FARGATE", "status": "ACTIVE", "autoScalingGroupProvider": {}},
        {"name": "FARGATE_SPOT", "status": "ACTIVE", "autoScalingGroupProvider": {}},
    ]
    for p in defaults:
        if not names or p["name"] in names:
            cp = {
                "capacityProviderArn": f"arn:aws:ecs:{REGION}:{get_account_id()}:capacity-provider/{p['name']}",
                "name": p["name"],
                "status": p["status"],
                "autoScalingGroupProvider": p["autoScalingGroupProvider"],
                "updateStatus": "UPDATE_COMPLETE",
            }
            if "TAGS" in include:
                cp["tags"] = _tags.get(cp["capacityProviderArn"], [])
            providers.append(cp)

    for cp_name, cp in _capacity_providers.items():
        if not names or cp_name in names:
            entry = dict(cp)
            if "TAGS" in include:
                entry["tags"] = _tags.get(cp["capacityProviderArn"], [])
            providers.append(entry)

    return json_response({"capacityProviders": providers})


def _create_capacity_provider(data):
    name = data.get("name", "")
    if not name:
        return error_response_json("InvalidParameterException", "name is required", 400)
    if name in _capacity_providers:
        return error_response_json("InvalidParameterException",
            f"Capacity provider {name} already exists.", 400)

    arn = f"arn:aws:ecs:{REGION}:{get_account_id()}:capacity-provider/{name}"
    asg_provider = data.get("autoScalingGroupProvider", {})

    cp = {
        "capacityProviderArn": arn,
        "name": name,
        "status": "ACTIVE",
        "autoScalingGroupProvider": {
            "autoScalingGroupArn": asg_provider.get("autoScalingGroupArn", ""),
            "managedScaling": asg_provider.get("managedScaling", {
                "status": "DISABLED",
                "targetCapacity": 100,
                "minimumScalingStepSize": 1,
                "maximumScalingStepSize": 10000,
                "instanceWarmupPeriod": 300,
            }),
            "managedTerminationProtection": asg_provider.get("managedTerminationProtection", "DISABLED"),
        },
        "updateStatus": "UPDATE_COMPLETE",
        "tags": data.get("tags", []),
    }
    _capacity_providers[name] = cp

    if cp["tags"]:
        _tags[arn] = list(cp["tags"])

    return json_response({"capacityProvider": cp})


def _delete_capacity_provider(data):
    name = data.get("capacityProvider", "")
    if name.startswith("arn:"):
        name = name.split("/")[-1]

    cp = _capacity_providers.pop(name, None)
    if not cp:
        return error_response_json("InvalidParameterException",
            f"Capacity provider {name} not found.", 400)

    _tags.pop(cp.get("capacityProviderArn", ""), None)
    cp["status"] = "INACTIVE"
    return json_response({"capacityProvider": cp})


def _put_cluster_capacity_providers(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    cluster = _clusters.get(cluster_name)
    if not cluster:
        return error_response_json("ClusterNotFoundException", "Cluster not found.", 400)

    cluster["capacityProviders"] = data.get("capacityProviders", [])
    cluster["defaultCapacityProviderStrategy"] = data.get("defaultCapacityProviderStrategy", [])

    return json_response({"cluster": cluster})


def _update_cluster(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    cluster = _clusters.get(cluster_name)
    if not cluster:
        return error_response_json("ClusterNotFoundException", "Cluster not found.", 400)

    if "configuration" in data:
        cluster["configuration"] = data["configuration"]
    if "settings" in data:
        cluster["settings"] = data["settings"]
    if "serviceConnectDefaults" in data:
        cluster["serviceConnectDefaults"] = data["serviceConnectDefaults"]

    return json_response({"cluster": cluster})


def _update_cluster_settings(data):
    cluster_name = _resolve_cluster_name(data.get("cluster", "default"))
    cluster = _clusters.get(cluster_name)
    if not cluster:
        return error_response_json("ClusterNotFoundException", "Cluster not found.", 400)

    settings = data.get("settings", [])
    if settings:
        cluster["settings"] = settings

    return json_response({"cluster": cluster})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_cluster_name(ref):
    if not ref:
        return "default"
    if ref.startswith("arn:"):
        return ref.split("/")[-1]
    if "/" in ref:
        return ref.split("/")[-1]
    return ref


def _resolve_service_name(ref):
    if ref.startswith("arn:"):
        return ref.split("/")[-1]
    if "/" in ref:
        return ref.split("/")[-1]
    return ref


def _resolve_td_key(ref):
    if not ref:
        return ""
    if "task-definition/" in ref:
        ref = ref.split("task-definition/")[-1]
    if ":" not in ref:
        rev = _task_def_latest.get(ref)
        if rev is None:
            return ref
        return f"{ref}:{rev}"
    return ref


def _resolve_task(ref, cluster_name="default"):
    """Look up a task by full ARN or short ID, optionally scoped to a cluster."""
    task = _tasks.get(ref)
    if task:
        return task
    cluster_arn = f"arn:aws:ecs:{REGION}:{get_account_id()}:cluster/{cluster_name}"
    for arn, t in _tasks.items():
        if t.get("clusterArn") != cluster_arn:
            continue
        if arn.endswith(f"/{ref}") or arn.endswith(ref):
            return t
    for arn, t in _tasks.items():
        if arn.endswith(f"/{ref}") or arn.endswith(ref):
            return t
    return None


def _cluster_name_from_arn(arn):
    if not arn:
        return ""
    return arn.split("/")[-1] if "/" in arn else arn


def _sanitize(obj):
    """Remove internal keys (prefixed with _) from a dict for API responses."""
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [_sanitize(i) for i in obj]
    return obj


# ---------------------------------------------------------------------------
# ListTaskDefinitionFamilies / DeleteTaskDefinitions
# ---------------------------------------------------------------------------

def _list_task_definition_families(data):
    family_prefix = data.get("familyPrefix", "")
    status_filter = data.get("status", "ACTIVE")
    families = set()
    for td in _task_defs.values():
        if status_filter and td.get("status") != status_filter:
            continue
        fam = td.get("family", "")
        if family_prefix and not fam.startswith(family_prefix):
            continue
        families.add(fam)
    return json_response({"families": sorted(families)})


def _delete_task_definitions(data):
    arns = data.get("taskDefinitions", [])
    failures = []
    for arn in arns:
        key = arn.split("/")[-1] if "/" in arn else arn
        if key in _task_defs:
            _task_defs[key]["status"] = "DELETE_IN_PROGRESS"
        else:
            failures.append({"arn": arn, "reason": "TASK_DEFINITION_NOT_FOUND"})
    return json_response({"taskDefinitions": [_task_defs.get(a.split("/")[-1], {}) for a in arns if a.split("/")[-1] in _task_defs], "failures": failures})


# ---------------------------------------------------------------------------
# ListServicesByNamespace
# ---------------------------------------------------------------------------

def _list_services_by_namespace(data):
    namespace = data.get("namespace", "")
    items = []
    for svc in _services.values():
        if namespace and svc.get("_namespace", "") != namespace:
            continue
        items.append({"serviceArn": svc["serviceArn"], "clusterArn": svc.get("clusterArn", "")})
    return json_response({"serviceArns": [s["serviceArn"] for s in items]})


# ---------------------------------------------------------------------------
# PutAccountSettingDefault / DeleteAccountSetting
# ---------------------------------------------------------------------------

def _put_account_setting_default(data):
    name = data.get("name", "")
    value = data.get("value", "")
    _account_settings[name] = value
    return json_response({"setting": {
        "name": name,
        "value": value,
        "principalArn": f"arn:aws:iam::{get_account_id()}:root",
        "type": "account",
    }})


def _delete_account_setting(data):
    name = data.get("name", "")
    _account_settings.pop(name, None)
    return json_response({"setting": {"name": name, "value": ""}})


# ---------------------------------------------------------------------------
# Attributes (PutAttributes / DeleteAttributes / ListAttributes)
# ---------------------------------------------------------------------------

_attributes = AccountScopedDict()

def _put_attributes(data):
    attrs = data.get("attributes", [])
    for attr in attrs:
        target_id = attr.get("targetId", "")
        name = attr.get("name", "")
        _attributes[f"{target_id}:{name}"] = attr
    return json_response({"attributes": attrs})


def _delete_attributes(data):
    attrs = data.get("attributes", [])
    for attr in attrs:
        target_id = attr.get("targetId", "")
        name = attr.get("name", "")
        _attributes.pop(f"{target_id}:{name}", None)
    return json_response({"attributes": attrs})


def _list_attributes(data):
    target_type = data.get("targetType", "")
    attr_name = data.get("attributeName", "")
    results = []
    for attr in _attributes.values():
        if target_type and attr.get("targetType", "") != target_type:
            continue
        if attr_name and attr.get("name", "") != attr_name:
            continue
        results.append(attr)
    return json_response({"attributes": results})


# ---------------------------------------------------------------------------
# UpdateCapacityProvider
# ---------------------------------------------------------------------------

def _update_capacity_provider(data):
    name = data.get("name", "")
    cp = _capacity_providers.get(name)
    if not cp:
        return error_response_json("ClientException", f"Capacity provider {name} not found", 400)
    auto_scaling = data.get("autoScalingGroupProvider")
    if auto_scaling:
        cp["autoScalingGroupProvider"].update(auto_scaling)
    cp["updateStatus"] = "UPDATE_COMPLETE"
    return json_response({"capacityProvider": cp})


# ---------------------------------------------------------------------------
# ServiceDeployments (stubs)
# ---------------------------------------------------------------------------

def _describe_service_deployments(data):
    return json_response({"serviceDeployments": []})


def _list_service_deployments(data):
    return json_response({"serviceDeployments": []})


def _describe_service_revisions(data):
    return json_response({"serviceRevisions": []})


# ---------------------------------------------------------------------------
# Agent stubs
# ---------------------------------------------------------------------------

def _submit_task_state_change(data):
    return json_response({"acknowledgment": "ACCEPT"})


def _submit_container_state_change(data):
    return json_response({"acknowledgment": "ACCEPT"})


def _submit_attachment_state_changes(data):
    return json_response({"acknowledgment": "ACCEPT"})


def _discover_poll_endpoint(data):
    return json_response({"endpoint": "http://localhost:4566", "telemetryEndpoint": "http://localhost:4566"})


# ---------------------------------------------------------------------------
# Task protection stubs
# ---------------------------------------------------------------------------

def _update_task_protection(data):
    return json_response({"protectedTasks": [], "failures": []})


def _get_task_protection(data):
    return json_response({"protectedTasks": [], "failures": []})


# ---------------------------------------------------------------------------
# Container instances (stub — MiniStack runs tasks as Docker containers
# directly, there are no EC2 container instances to register)
# ---------------------------------------------------------------------------

def _list_container_instances(data):
    return json_response({"containerInstanceArns": []})


def _describe_container_instances(data):
    return json_response({"containerInstances": [], "failures": []})


# ---------------------------------------------------------------------------
# Action map (X-Amz-Target dispatch)
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    "CreateCluster": _create_cluster,
    "DeleteCluster": _delete_cluster,
    "DescribeClusters": _describe_clusters,
    "ListClusters": _list_clusters,
    "UpdateCluster": _update_cluster,
    "UpdateClusterSettings": _update_cluster_settings,
    "RegisterTaskDefinition": _register_task_definition,
    "DeregisterTaskDefinition": _deregister_task_definition,
    "DescribeTaskDefinition": _describe_task_definition,
    "ListTaskDefinitions": _list_task_definitions,
    "CreateService": _create_service,
    "DeleteService": _delete_service,
    "DescribeServices": _describe_services,
    "UpdateService": _update_service,
    "ListServices": _list_services,
    "RunTask": _run_task,
    "StopTask": _stop_task,
    "DescribeTasks": _describe_tasks,
    "ListTasks": _list_tasks,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
    "ExecuteCommand": _execute_command,
    "ListAccountSettings": _list_account_settings,
    "PutAccountSetting": _put_account_setting,
    "CreateCapacityProvider": _create_capacity_provider,
    "DeleteCapacityProvider": _delete_capacity_provider,
    "DescribeCapacityProviders": _describe_capacity_providers,
    "PutClusterCapacityProviders": _put_cluster_capacity_providers,
    "ListTaskDefinitionFamilies": _list_task_definition_families,
    "DeleteTaskDefinitions": _delete_task_definitions,
    "ListServicesByNamespace": _list_services_by_namespace,
    "PutAccountSettingDefault": _put_account_setting_default,
    "DeleteAccountSetting": _delete_account_setting,
    "PutAttributes": _put_attributes,
    "DeleteAttributes": _delete_attributes,
    "ListAttributes": _list_attributes,
    "UpdateCapacityProvider": _update_capacity_provider,
    "DescribeServiceDeployments": _describe_service_deployments,
    "ListServiceDeployments": _list_service_deployments,
    "DescribeServiceRevisions": _describe_service_revisions,
    "SubmitTaskStateChange": _submit_task_state_change,
    "SubmitContainerStateChange": _submit_container_state_change,
    "SubmitAttachmentStateChanges": _submit_attachment_state_changes,
    "DiscoverPollEndpoint": _discover_poll_endpoint,
    "ListContainerInstances": _list_container_instances,
    "DescribeContainerInstances": _describe_container_instances,
    "UpdateTaskProtection": _update_task_protection,
    "GetTaskProtection": _get_task_protection,
}


def reset():
    docker_client = _get_docker()
    if docker_client:
        for task in _tasks.values():
            for cid in task.get("_docker_ids", []):
                try:
                    c = docker_client.containers.get(cid)
                    c.stop(timeout=2)
                    c.remove(v=True)
                except Exception as e:
                    logger.warning("reset: failed to stop/remove container %s: %s", cid, e)
    _clusters.clear()
    _task_defs.clear()
    _task_def_latest.clear()
    _services.clear()
    _tasks.clear()
    _tags.clear()
    _account_settings.clear()
    _capacity_providers.clear()
    _attributes.clear()
