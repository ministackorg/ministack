"""
EMR Containers (EMR on EKS) Service Emulator.

EMR Containers is a separate AWS service from classic EMR (EC2). It uses a REST/JSON
API (not JSON-RPC like EMR EC2) with credential scope "emr-containers" and URL path
routing under /virtualclusters/...

This service is used by DPC (Data Processing Controller) to submit Spark jobs via
the EMR on EKS API. In mock mode (default), jobs are immediately marked COMPLETED
without running any actual Spark workloads — identical to how the classic EMR service
works in MiniStack.

Supported operations (7 REST API endpoints):
  - POST   /virtualclusters                              CreateVirtualCluster
  - GET    /virtualclusters/{id}                          DescribeVirtualCluster
  - DELETE /virtualclusters/{id}                          DeleteVirtualCluster
  - POST   /virtualclusters/{id}/jobruns                  StartJobRun
  - GET    /virtualclusters/{vcId}/jobruns/{jrId}         DescribeJobRun
  - DELETE /virtualclusters/{vcId}/jobruns/{jrId}         CancelJobRun
  - GET    /virtualclusters/{id}/jobruns                  ListJobRuns
"""

import json
import logging
import os
import re

from ministack.core.responses import json_response, new_uuid, now_iso
from ministack.core import k8s_spark

logger = logging.getLogger("emr-containers")

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# In-memory state — wiped by POST /_ministack/reset (see reset() below).
# Virtual clusters: maps vc_id -> cluster dict (name, state, containerProvider, etc.)
_virtual_clusters: dict = {}
# Job runs: maps (vc_id, jr_id) tuple -> job run dict (state, jobDriver, etc.)
_job_runs: dict = {}


def _vc_arn(vc_id):
    return f"arn:aws:emr-containers:{REGION}:{ACCOUNT_ID}:/virtualclusters/{vc_id}"


def _jr_arn(vc_id, jr_id):
    return f"arn:aws:emr-containers:{REGION}:{ACCOUNT_ID}:/virtualclusters/{vc_id}/jobruns/{jr_id}"


def _json_err(code, message, status=400):
    body = json.dumps({"message": message, "code": code}).encode("utf-8")
    return status, {"Content-Type": "application/json"}, body


async def handle_request(method, path, headers, body, query_params):
    """Route incoming EMR Containers REST requests to the appropriate handler.

    Called by app.py when the path starts with /virtualclusters. The path prefix
    is stripped to extract the sub-path, then matched against the 7 supported
    operations using method + path pattern.
    """
    sub = path[len("/virtualclusters"):]

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}

    # POST /virtualclusters  (CreateVirtualCluster)
    if sub == "" and method == "POST":
        return _create_virtual_cluster(data)

    # GET /virtualclusters  (ListVirtualClusters)
    if sub == "" and method == "GET":
        return _list_virtual_clusters(query_params)

    # Match /virtualclusters/{id}/jobruns patterns first (more specific)
    m = re.match(r"^/([^/]+)/jobruns(?:/([^/]+))?$", sub)
    if m:
        vc_id = m.group(1)
        jr_id = m.group(2)

        if vc_id not in _virtual_clusters:
            return _json_err("ResourceNotFoundException",
                             f"Virtual cluster {vc_id} does not exist", 404)

        if jr_id is None:
            # POST /virtualclusters/{id}/jobruns  (StartJobRun)
            if method == "POST":
                return _start_job_run(vc_id, data)
            # GET /virtualclusters/{id}/jobruns  (ListJobRuns)
            if method == "GET":
                return _list_job_runs(vc_id, query_params)
        else:
            # GET /virtualclusters/{vcId}/jobruns/{jrId}  (DescribeJobRun)
            if method == "GET":
                return _describe_job_run(vc_id, jr_id)
            # DELETE /virtualclusters/{vcId}/jobruns/{jrId}  (CancelJobRun)
            if method == "DELETE":
                return _cancel_job_run(vc_id, jr_id)

    # Match /virtualclusters/{id}
    m = re.match(r"^/([^/]+)$", sub)
    if m:
        vc_id = m.group(1)
        # GET /virtualclusters/{id}  (DescribeVirtualCluster)
        if method == "GET":
            return _describe_virtual_cluster(vc_id)
        # DELETE /virtualclusters/{id}  (DeleteVirtualCluster)
        if method == "DELETE":
            return _delete_virtual_cluster(vc_id)

    return _json_err("ResourceNotFoundException",
                     f"Unknown EMR Containers path: {method} {path}", 404)


# --- Virtual Cluster operations ---

def _create_virtual_cluster(data):
    vc_id = new_uuid()[:13]  # EMR uses short IDs like "a1b2c3d4e5f6g"
    name = data.get("name", "")
    container_provider = data.get("containerProvider", {})
    tags = data.get("tags", {})
    now = now_iso()

    vc = {
        "id": vc_id,
        "name": name,
        "arn": _vc_arn(vc_id),
        "state": "RUNNING",
        "containerProvider": container_provider,
        "createdAt": now,
        "tags": tags,
    }
    _virtual_clusters[vc_id] = vc
    logger.info("CreateVirtualCluster: id=%s name=%s", vc_id, name)
    return json_response({"id": vc_id, "name": name, "arn": vc["arn"]})


def _describe_virtual_cluster(vc_id):
    vc = _virtual_clusters.get(vc_id)
    if not vc:
        return _json_err("ResourceNotFoundException",
                         f"Virtual cluster {vc_id} does not exist", 404)
    return json_response({"virtualCluster": vc})


def _delete_virtual_cluster(vc_id):
    vc = _virtual_clusters.get(vc_id)
    if not vc:
        return _json_err("ResourceNotFoundException",
                         f"Virtual cluster {vc_id} does not exist", 404)
    vc["state"] = "TERMINATED"
    logger.info("DeleteVirtualCluster: id=%s", vc_id)
    return json_response({"id": vc_id})


def _list_virtual_clusters(query_params):
    states_filter = query_params.get("states", [])
    if isinstance(states_filter, str):
        states_filter = [states_filter]

    clusters = list(_virtual_clusters.values())
    if states_filter:
        clusters = [vc for vc in clusters if vc["state"] in states_filter]

    return json_response({
        "virtualClusters": clusters,
        "nextToken": None,
    })


# --- Job Run operations ---

def _start_job_run(vc_id, data):
    """Create a new job run.

    If spark config is present (k8s mode), creates a real K8s Job via k8s_spark.
    Otherwise, the job is immediately marked COMPLETED (mock mode).
    """
    jr_id = new_uuid()[:13]
    name = data.get("name", "")
    release_label = data.get("releaseLabel", "")
    execution_role_arn = data.get("executionRoleArn", "")
    job_driver = data.get("jobDriver", {})
    configuration_overrides = data.get("configurationOverrides", {})
    tags = data.get("tags", {})
    now = now_iso()

    if k8s_spark.is_k8s_mode():
        # Real execution: extract Spark params and create K8s Job
        spark_submit = job_driver.get("sparkSubmitJobDriver", {})
        entry_point = spark_submit.get("entryPoint", "")
        submit_params = spark_submit.get("sparkSubmitParameters", "")

        # Parse --class from sparkSubmitParameters
        class_name = ""
        if "--class " in submit_params:
            parts = submit_params.split("--class ")
            class_name = parts[1].split()[0] if len(parts) > 1 else ""

        # Extract spark conf from configurationOverrides
        spark_conf = {}
        app_config = configuration_overrides.get("applicationConfiguration", {})
        for cfg in app_config.get("configurations", []) if isinstance(app_config, dict) else []:
            if cfg.get("classification") == "spark-defaults":
                spark_conf.update(cfg.get("properties", {}))

        k8s_job_name = f"emr-jr-{jr_id}"
        k8s_spark.create_spark_job(
            job_name=k8s_job_name,
            entry_point=entry_point,
            class_name=class_name,
            spark_conf=spark_conf,
            labels={
                "ministack/service": "emr-containers",
                "ministack/virtual-cluster": vc_id,
                "ministack/job-run": jr_id,
            },
        )
        initial_state = "PENDING"
        state_details = "K8s Job created"
        finished_at = None
    else:
        # Mock mode: immediately completed
        initial_state = "COMPLETED"
        state_details = "Job completed successfully (mock)"
        finished_at = now

    jr = {
        "id": jr_id,
        "name": name,
        "arn": _jr_arn(vc_id, jr_id),
        "virtualClusterId": vc_id,
        "state": initial_state,
        "stateDetails": state_details,
        "releaseLabel": release_label,
        "executionRoleArn": execution_role_arn,
        "jobDriver": job_driver,
        "configurationOverrides": configuration_overrides,
        "tags": tags,
        "createdAt": now,
        "finishedAt": finished_at,
        "_k8s_job_name": f"emr-jr-{jr_id}" if k8s_spark.is_k8s_mode() else None,
    }
    _job_runs[(vc_id, jr_id)] = jr
    logger.info("StartJobRun: vc=%s jr=%s name=%s (mode=%s)",
                vc_id, jr_id, name, "k8s" if k8s_spark.is_k8s_mode() else "mock")
    return json_response({
        "id": jr_id,
        "name": name,
        "arn": jr["arn"],
        "virtualClusterId": vc_id,
    })


def _describe_job_run(vc_id, jr_id):
    jr = _job_runs.get((vc_id, jr_id))
    if not jr:
        return _json_err("ResourceNotFoundException",
                         f"Job run {jr_id} does not exist", 404)
    # In k8s mode, update state from K8s before returning
    k8s_job_name = jr.get("_k8s_job_name")
    if k8s_job_name and jr["state"] not in ("COMPLETED", "FAILED", "CANCELLED"):
        k8s_state = k8s_spark.get_job_state(k8s_job_name)
        jr["state"] = k8s_state["state"]
        jr["stateDetails"] = k8s_state["stateDetails"]
        if jr["state"] in ("COMPLETED", "FAILED", "CANCELLED"):
            jr["finishedAt"] = now_iso()
    return json_response({"jobRun": jr})


def _cancel_job_run(vc_id, jr_id):
    jr = _job_runs.get((vc_id, jr_id))
    if not jr:
        return _json_err("ResourceNotFoundException",
                         f"Job run {jr_id} does not exist", 404)
    # In k8s mode, delete the K8s Job
    k8s_job_name = jr.get("_k8s_job_name")
    if k8s_job_name:
        k8s_spark.delete_job(k8s_job_name)
    jr["state"] = "CANCELLED"
    jr["stateDetails"] = "Job cancelled by user"
    jr["finishedAt"] = now_iso()
    logger.info("CancelJobRun: vc=%s jr=%s", vc_id, jr_id)
    return json_response({"id": jr_id})


def _list_job_runs(vc_id, query_params):
    states_filter = query_params.get("states", [])
    if isinstance(states_filter, str):
        states_filter = [states_filter]

    runs = [jr for (vc, _), jr in _job_runs.items() if vc == vc_id]
    if states_filter:
        runs = [jr for jr in runs if jr["state"] in states_filter]

    return json_response({
        "jobRuns": runs,
        "nextToken": None,
    })


def reset():
    """Wipe all in-memory state. Called by POST /_ministack/reset."""
    _virtual_clusters.clear()
    _job_runs.clear()
