"""
ECS (Elastic Container Service) Emulator.
REST JSON API — path-based routing like real ECS.
Supports: CreateCluster, DeleteCluster, DescribeCluster, ListClusters,
          RegisterTaskDefinition, DeregisterTaskDefinition, DescribeTaskDefinition, ListTaskDefinitions,
          CreateService, DeleteService, DescribeServices, UpdateService, ListServices,
          RunTask, StopTask, DescribeTasks, ListTasks.

Container execution: if Docker socket is available, RunTask actually runs containers.
"""

import os
import json
import time
import logging

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("ecs")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_clusters: dict = {}        # cluster_name -> cluster dict
_task_defs: dict = {}       # family:revision -> task def dict
_task_def_latest: dict = {} # family -> latest revision number
_services: dict = {}        # cluster_name/service_name -> service dict
_tasks: dict = {}           # task_arn -> task dict

# Docker client (optional)
_docker = None
def _get_docker():
    global _docker
    if _docker is None:
        try:
            import docker
            _docker = docker.from_env()
        except Exception:
            pass
    return _docker


async def handle_request(method, path, headers, body, query_params):
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    # ECS uses path-based routing: /clusters, /taskdefinitions, etc.
    # Strip leading slash and split
    parts = [p for p in path.strip("/").split("/") if p]

    # Also support X-Amz-Target for some SDKs
    target = headers.get("x-amz-target", "")
    if target:
        action = target.split(".")[-1]
        return _dispatch_action(action, data, parts, method)

    # Path-based dispatch
    if not parts:
        return error_response_json("InvalidRequest", "Missing path", 400)

    resource = parts[0]

    # POST /clusters -> CreateCluster
    # GET  /clusters -> ListClusters
    if resource == "clusters":
        if method == "POST" and len(parts) == 1:
            return _create_cluster(data)
        if method == "GET" and len(parts) == 1:
            return _list_clusters(data)
        if method == "POST" and len(parts) == 2 and parts[1] == "delete":
            return _delete_cluster(data)
        if method == "POST" and len(parts) == 1 and "describe" in data:
            return _describe_clusters(data)
        # POST /clusters with clusters key = describe
        if method == "POST" and "clusters" in data:
            return _describe_clusters(data)

    # /taskdefinitions
    if resource == "taskdefinitions":
        if method == "POST" and len(parts) == 1:
            return _register_task_definition(data)
        if method == "GET" and len(parts) == 1:
            return _list_task_definitions(data)
        if method == "GET" and len(parts) == 2:
            return _describe_task_definition(parts[1])
        if method == "DELETE" and len(parts) == 2:
            return _deregister_task_definition(parts[1])

    # /tasks
    if resource == "tasks":
        if method == "POST" and len(parts) == 1:
            # Could be RunTask or DescribeTasks depending on body
            if "taskDefinition" in data:
                return _run_task(data)
            if "tasks" in data:
                return _describe_tasks(data)
        if method == "GET" and len(parts) == 1:
            return _list_tasks(data)

    # /services
    if resource == "services":
        if method == "POST" and len(parts) == 1:
            if "serviceName" in data and "taskDefinition" in data:
                return _create_service(data)
            if "services" in data:
                return _describe_services(data)
        if method == "GET" and len(parts) == 1:
            return _list_services(data)
        if method == "PUT" and len(parts) == 2:
            return _update_service(parts[1], data)
        if method == "DELETE" and len(parts) == 2:
            return _delete_service(parts[1], data)

    # /stoptask
    if resource == "stoptask":
        return _stop_task(data)

    return error_response_json("InvalidRequest", f"Unknown ECS path: {path}", 400)


def _dispatch_action(action, data, parts, method):
    """Handle X-Amz-Target based dispatch."""
    handlers = {
        "CreateCluster": _create_cluster,
        "DeleteCluster": _delete_cluster,
        "DescribeClusters": _describe_clusters,
        "ListClusters": _list_clusters,
        "RegisterTaskDefinition": _register_task_definition,
        "DeregisterTaskDefinition": lambda d: _deregister_task_definition(d.get("taskDefinition", "")),
        "DescribeTaskDefinition": lambda d: _describe_task_definition(d.get("taskDefinition", "")),
        "ListTaskDefinitions": _list_task_definitions,
        "CreateService": _create_service,
        "DeleteService": lambda d: _delete_service(d.get("service", ""), d),
        "DescribeServices": _describe_services,
        "UpdateService": lambda d: _update_service(d.get("service", ""), d),
        "ListServices": _list_services,
        "RunTask": _run_task,
        "StopTask": _stop_task,
        "DescribeTasks": _describe_tasks,
        "ListTasks": _list_tasks,
    }
    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown ECS action: {action}", 400)
    return handler(data)


# ---- Clusters ----

def _create_cluster(data):
    name = data.get("clusterName", "default")
    arn = f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:cluster/{name}"
    _clusters[name] = {
        "clusterArn": arn, "clusterName": name,
        "status": "ACTIVE", "registeredContainerInstancesCount": 0,
        "runningTasksCount": 0, "pendingTasksCount": 0,
        "activeServicesCount": 0, "tags": data.get("tags", []),
        "settings": data.get("settings", []),
        "capacityProviders": data.get("capacityProviders", []),
    }
    return json_response({"cluster": _clusters[name]})


def _delete_cluster(data):
    name = data.get("cluster", "default")
    name = _resolve_cluster_name(name)
    cluster = _clusters.pop(name, None)
    if not cluster:
        return error_response_json("ClusterNotFoundException", f"Cluster {name} not found", 400)
    return json_response({"cluster": cluster})


def _describe_clusters(data):
    names = data.get("clusters", ["default"])
    result = []
    failures = []
    for name in names:
        n = _resolve_cluster_name(name)
        if n in _clusters:
            result.append(_clusters[n])
        else:
            failures.append({"arn": name, "reason": "MISSING"})
    return json_response({"clusters": result, "failures": failures})


def _list_clusters(data):
    arns = [c["clusterArn"] for c in _clusters.values()]
    return json_response({"clusterArns": arns})


# ---- Task Definitions ----

def _register_task_definition(data):
    family = data.get("family")
    if not family:
        return error_response_json("ValidationException", "family is required", 400)

    rev = _task_def_latest.get(family, 0) + 1
    _task_def_latest[family] = rev
    td_key = f"{family}:{rev}"
    arn = f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:task-definition/{td_key}"

    td = {
        "taskDefinitionArn": arn,
        "family": family,
        "revision": rev,
        "status": "ACTIVE",
        "containerDefinitions": data.get("containerDefinitions", []),
        "volumes": data.get("volumes", []),
        "networkMode": data.get("networkMode", "bridge"),
        "requiresCompatibilities": data.get("requiresCompatibilities", ["EC2"]),
        "cpu": data.get("cpu", "256"),
        "memory": data.get("memory", "512"),
        "executionRoleArn": data.get("executionRoleArn", ""),
        "taskRoleArn": data.get("taskRoleArn", ""),
        "registeredAt": time.time(),
    }
    _task_defs[td_key] = td
    return json_response({"taskDefinition": td, "tags": data.get("tags", [])})


def _deregister_task_definition(td_ref):
    key = _resolve_td_key(td_ref)
    td = _task_defs.get(key)
    if not td:
        return error_response_json("InvalidParameterException", f"Task definition {td_ref} not found", 400)
    td["status"] = "INACTIVE"
    return json_response({"taskDefinition": td})


def _describe_task_definition(td_ref):
    if isinstance(td_ref, dict):
        td_ref = td_ref.get("taskDefinition", "")
    key = _resolve_td_key(td_ref)
    td = _task_defs.get(key)
    if not td:
        return error_response_json("InvalidParameterException", f"Task definition {td_ref} not found", 400)
    return json_response({"taskDefinition": td, "tags": []})


def _list_task_definitions(data):
    family = data.get("familyPrefix", "")
    status = data.get("status", "ACTIVE")
    arns = [
        td["taskDefinitionArn"] for td in _task_defs.values()
        if (not family or td["family"].startswith(family)) and td["status"] == status
    ]
    return json_response({"taskDefinitionArns": arns})


# ---- Services ----

def _create_service(data):
    cluster = _resolve_cluster_name(data.get("cluster", "default"))
    if cluster not in _clusters:
        _create_cluster({"clusterName": cluster})

    name = data.get("serviceName")
    td_key = _resolve_td_key(data.get("taskDefinition", ""))
    arn = f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:service/{cluster}/{name}"
    svc_key = f"{cluster}/{name}"

    _services[svc_key] = {
        "serviceArn": arn, "serviceName": name, "clusterArn": _clusters[cluster]["clusterArn"],
        "taskDefinition": _task_defs.get(td_key, {}).get("taskDefinitionArn", data.get("taskDefinition", "")),
        "desiredCount": data.get("desiredCount", 1),
        "runningCount": 0, "pendingCount": 0,
        "status": "ACTIVE",
        "launchType": data.get("launchType", "EC2"),
        "networkConfiguration": data.get("networkConfiguration", {}),
        "loadBalancers": data.get("loadBalancers", []),
        "createdAt": time.time(),
        "deployments": [],
        "events": [],
    }
    _clusters[cluster]["activeServicesCount"] += 1
    return json_response({"service": _services[svc_key]})


def _delete_service(service_name, data):
    cluster = _resolve_cluster_name(data.get("cluster", "default"))
    svc_key = f"{cluster}/{service_name}"
    svc = _services.pop(svc_key, None)
    if not svc:
        return error_response_json("ServiceNotFoundException", f"Service {service_name} not found", 400)
    if cluster in _clusters:
        _clusters[cluster]["activeServicesCount"] = max(0, _clusters[cluster]["activeServicesCount"] - 1)
    return json_response({"service": svc})


def _describe_services(data):
    cluster = _resolve_cluster_name(data.get("cluster", "default"))
    services = data.get("services", [])
    result = []
    failures = []
    for svc_name in services:
        svc_key = f"{cluster}/{_resolve_service_name(svc_name)}"
        if svc_key in _services:
            result.append(_services[svc_key])
        else:
            failures.append({"arn": svc_name, "reason": "MISSING"})
    return json_response({"services": result, "failures": failures})


def _update_service(service_name, data):
    cluster = _resolve_cluster_name(data.get("cluster", "default"))
    svc_key = f"{cluster}/{_resolve_service_name(service_name)}"
    svc = _services.get(svc_key)
    if not svc:
        return error_response_json("ServiceNotFoundException", f"Service {service_name} not found", 400)
    if "desiredCount" in data:
        svc["desiredCount"] = data["desiredCount"]
    if "taskDefinition" in data:
        svc["taskDefinition"] = data["taskDefinition"]
    return json_response({"service": svc})


def _list_services(data):
    cluster = _resolve_cluster_name(data.get("cluster", "default"))
    arns = [s["serviceArn"] for k, s in _services.items() if k.startswith(f"{cluster}/")]
    return json_response({"serviceArns": arns})


# ---- Tasks ----

def _run_task(data):
    cluster = _resolve_cluster_name(data.get("cluster", "default"))
    if cluster not in _clusters:
        _create_cluster({"clusterName": cluster})

    td_ref = data.get("taskDefinition", "")
    td_key = _resolve_td_key(td_ref)
    td = _task_defs.get(td_key)

    count = data.get("count", 1)
    tasks = []
    failures = []

    for _ in range(count):
        task_id = new_uuid()
        task_arn = f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:task/{cluster}/{task_id}"
        container_overrides = data.get("overrides", {}).get("containerOverrides", [])

        task = {
            "taskArn": task_arn,
            "clusterArn": f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:cluster/{cluster}",
            "taskDefinitionArn": td["taskDefinitionArn"] if td else td_ref,
            "lastStatus": "RUNNING",
            "desiredStatus": "RUNNING",
            "launchType": data.get("launchType", "EC2"),
            "cpu": td.get("cpu", "256") if td else "256",
            "memory": td.get("memory", "512") if td else "512",
            "createdAt": time.time(),
            "startedAt": time.time(),
            "group": data.get("group", ""),
            "containers": [],
            "_docker_ids": [],
        }

        # Try to actually run containers via Docker
        docker_client = _get_docker()
        if docker_client and td:
            for cdef in td.get("containerDefinitions", []):
                # Apply overrides
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
                        cdef["image"],
                        detach=True,
                        environment=env,
                        ports=port_bindings if port_bindings else None,
                        name=f"ministack-ecs-{task_id[:8]}-{cdef['name']}",
                        labels={"ministack": "ecs", "task_arn": task_arn},
                    )
                    task["containers"].append({
                        "name": cdef["name"], "image": cdef["image"],
                        "lastStatus": "RUNNING", "exitCode": None,
                        "networkBindings": [],
                        "containerArn": f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:container/{new_uuid()}",
                        "_docker_id": container.id,
                    })
                    task["_docker_ids"].append(container.id)
                    logger.info(f"ECS: started container {cdef['image']} for task {task_id[:8]}")
                except Exception as e:
                    logger.warning(f"ECS: Docker run failed for {cdef.get('image')}: {e}")
                    task["containers"].append({
                        "name": cdef["name"], "image": cdef["image"],
                        "lastStatus": "RUNNING", "exitCode": None,
                        "networkBindings": [],
                        "containerArn": f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:container/{new_uuid()}",
                    })
        elif td:
            for cdef in td.get("containerDefinitions", []):
                task["containers"].append({
                    "name": cdef["name"], "image": cdef["image"],
                    "lastStatus": "RUNNING", "exitCode": None,
                    "networkBindings": [],
                    "containerArn": f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:container/{new_uuid()}",
                })

        _tasks[task_arn] = task
        _clusters[cluster]["runningTasksCount"] += 1
        tasks.append({k: v for k, v in task.items() if not k.startswith("_")})

    return json_response({"tasks": tasks, "failures": failures})


def _stop_task(data):
    task_ref = data.get("task", "")
    cluster = _resolve_cluster_name(data.get("cluster", "default"))
    reason = data.get("reason", "")

    # Resolve by ARN or short ID
    task = _tasks.get(task_ref)
    if not task:
        for arn, t in _tasks.items():
            if arn.endswith(task_ref):
                task = t
                task_ref = arn
                break

    if not task:
        return error_response_json("InvalidParameterException", f"Task {task_ref} not found", 400)

    # Stop Docker containers
    docker_client = _get_docker()
    if docker_client:
        for docker_id in task.get("_docker_ids", []):
            try:
                container = docker_client.containers.get(docker_id)
                container.stop(timeout=5)
                container.remove()
            except Exception as e:
                logger.warning(f"ECS: failed to stop container {docker_id}: {e}")

    task["lastStatus"] = "STOPPED"
    task["desiredStatus"] = "STOPPED"
    task["stoppedAt"] = time.time()
    task["stoppedReason"] = reason

    cluster_name = _resolve_cluster_name(cluster)
    if cluster_name in _clusters:
        _clusters[cluster_name]["runningTasksCount"] = max(0, _clusters[cluster_name]["runningTasksCount"] - 1)

    return json_response({"task": {k: v for k, v in task.items() if not k.startswith("_")}})


def _describe_tasks(data):
    cluster = _resolve_cluster_name(data.get("cluster", "default"))
    task_refs = data.get("tasks", [])
    result = []
    failures = []
    for ref in task_refs:
        task = _tasks.get(ref)
        if not task:
            for arn, t in _tasks.items():
                if arn.endswith(ref):
                    task = t
                    break
        if task:
            _maybe_mark_stopped(task)
            result.append({k: v for k, v in task.items() if not k.startswith("_")})
        else:
            failures.append({"arn": ref, "reason": "MISSING"})
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
            if container.status != "exited":
                all_stopped = False
                break
            result = container.wait()
            exit_code = max(exit_code, result.get("StatusCode", 0))
        except Exception:
            # Container removed or unreachable — treat as stopped
            pass

    if not all_stopped:
        return

    task["lastStatus"] = "STOPPED"
    task["desiredStatus"] = "STOPPED"
    task["stoppedAt"] = time.time()
    task["stoppedReason"] = "Essential container exited"
    task["stopCode"] = "EssentialContainerExited"
    for c in task.get("containers", []):
        c["lastStatus"] = "STOPPED"
        c["exitCode"] = exit_code

    cluster_name = _resolve_cluster_name(task.get("clusterArn", "").split("/")[-1] if "/" in task.get("clusterArn", "") else "")
    if cluster_name in _clusters:
        _clusters[cluster_name]["runningTasksCount"] = max(0, _clusters[cluster_name]["runningTasksCount"] - 1)


def _list_tasks(data):
    cluster = _resolve_cluster_name(data.get("cluster", "default"))
    cluster_arn = f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:cluster/{cluster}"
    status = data.get("desiredStatus", "RUNNING")
    arns = [
        arn for arn, t in _tasks.items()
        if t.get("clusterArn") == cluster_arn and t.get("desiredStatus") == status
    ]
    return json_response({"taskArns": arns})


# ---- Helpers ----

def _resolve_cluster_name(ref):
    if not ref:
        return "default"
    if "/" in ref:
        return ref.split("/")[-1]
    return ref


def _resolve_service_name(ref):
    if "/" in ref:
        return ref.split("/")[-1]
    return ref


def _resolve_td_key(ref):
    """Resolve task definition ARN or family[:revision] to internal key."""
    if not ref:
        return ""
    if "task-definition/" in ref:
        ref = ref.split("task-definition/")[-1]
    if ":" not in ref:
        # Latest revision
        rev = _task_def_latest.get(ref, 1)
        return f"{ref}:{rev}"
    return ref
