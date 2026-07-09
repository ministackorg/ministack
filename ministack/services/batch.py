"""
AWS Batch stub (rest-json).

Endpoints under ``/v1/``. Stores compute environments, job queues, job
definitions, and jobs in account-scoped state. Submitted jobs immediately
transition to ``SUCCEEDED`` — Batch is a control-plane/scheduler emulator
here, not a real container runner.
"""

import copy
import json
import logging
import re
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    new_uuid,
)

logger = logging.getLogger("batch")

_compute_envs = AccountScopedDict()   # name -> dict
_job_queues = AccountScopedDict()     # name -> dict
_job_definitions = AccountScopedDict()  # name -> [revisions]
_jobs = AccountScopedDict()           # job_id -> dict

_JOB_QUEUE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def reset():
    _compute_envs.clear()
    _job_queues.clear()
    _job_definitions.clear()
    _jobs.clear()


def get_state():
    return {
        "compute_envs": copy.deepcopy(_compute_envs),
        "job_queues": copy.deepcopy(_job_queues),
        "job_definitions": copy.deepcopy(_job_definitions),
        "jobs": copy.deepcopy(_jobs),
    }


def restore_state(data):
    if not data:
        return
    for store, key in (
        (_compute_envs, "compute_envs"),
        (_job_queues, "job_queues"),
        (_job_definitions, "job_definitions"),
        (_jobs, "jobs"),
    ):
        store.clear()
        for k, v in (data.get(key) or {}).items():
            store[k] = v


def _json(status, body):
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _ce_arn(name):
    return f"arn:aws:batch:{get_region()}:{get_account_id()}:compute-environment/{name}"


def _jq_arn(name):
    return f"arn:aws:batch:{get_region()}:{get_account_id()}:job-queue/{name}"


def _jd_arn(name, revision):
    return f"arn:aws:batch:{get_region()}:{get_account_id()}:job-definition/{name}:{revision}"


def _job_arn(job_id):
    return f"arn:aws:batch:{get_region()}:{get_account_id()}:job/{job_id}"


def _job_queue_name_from_ref(ref):
    if not ref or not ref.startswith("arn:"):
        return ref, None
    try:
        spec = parse_arn(ref)
    except ArnParseError:
        return "", _batch_client_exception(ref)
    if spec.service != "batch" or spec.account_id != get_account_id():
        return "", _batch_client_exception(ref)
    if spec.region != get_region():
        return None, None
    if not spec.resource.startswith("job-queue/"):
        return "", _batch_client_exception(ref)
    name = spec.resource.split("/", 1)[1]
    if not _JOB_QUEUE_NAME_RE.fullmatch(name):
        return "", _batch_client_exception(ref)
    return name, None


def _batch_client_exception(identifier):
    return error_response_json(
        "ClientException",
        f"Invalid job queue identifier: {identifier}",
        400,
    )


def _job_queue_matches(stored_ref, query_ref, queue_name):
    if not queue_name:
        return False
    return stored_ref in {query_ref, queue_name, _jq_arn(queue_name)}


def _now_ms():
    return int(time.time() * 1000)


# ─── compute environments ───────────────────────────────────────
def _create_compute_environment(p):
    name = p.get("computeEnvironmentName")
    if not name:
        return error_response_json("ClientException", "computeEnvironmentName is required", 400)
    if name in _compute_envs:
        return error_response_json("ClientException",
                                   f"Object already exists: {name}", 400)
    rec = {
        "computeEnvironmentName": name,
        "computeEnvironmentArn": _ce_arn(name),
        "ecsClusterArn": f"arn:aws:ecs:{get_region()}:{get_account_id()}:cluster/{name}",
        "type": p.get("type", "MANAGED"),
        "state": p.get("state", "ENABLED"),
        "status": "VALID",
        "statusReason": "ComputeEnvironment is ready",
        "computeResources": p.get("computeResources", {}),
        "serviceRole": p.get("serviceRole", ""),
        "tags": p.get("tags", {}),
    }
    _compute_envs[name] = rec
    return _json(200, {"computeEnvironmentName": name,
                       "computeEnvironmentArn": rec["computeEnvironmentArn"]})


def _describe_compute_environments(p):
    names = p.get("computeEnvironments") or []
    if names:
        out = [_compute_envs[n] for n in names if n in _compute_envs]
    else:
        out = list(_compute_envs.values())
    return _json(200, {"computeEnvironments": out})


# ─── job queues ─────────────────────────────────────────────────
def _create_job_queue(p):
    name = p.get("jobQueueName")
    if not name:
        return error_response_json("ClientException", "jobQueueName is required", 400)
    if name in _job_queues:
        return error_response_json("ClientException",
                                   f"Object already exists: {name}", 400)
    rec = {
        "jobQueueName": name,
        "jobQueueArn": _jq_arn(name),
        "state": p.get("state", "ENABLED"),
        "status": "VALID",
        "statusReason": "JobQueue is ready",
        "priority": p.get("priority", 1),
        "computeEnvironmentOrder": p.get("computeEnvironmentOrder", []),
        "tags": p.get("tags", {}),
    }
    _job_queues[name] = rec
    return _json(200, {"jobQueueName": name, "jobQueueArn": rec["jobQueueArn"]})


def _describe_job_queues(p):
    names = p.get("jobQueues") or []
    if names:
        out = []
        for n in names:
            # Accept both name and ARN per AWS behaviour.
            short, error = _job_queue_name_from_ref(n)
            if error:
                return error
            if short in _job_queues:
                out.append(_job_queues[short])
    else:
        out = list(_job_queues.values())
    return _json(200, {"jobQueues": out})


# ─── job definitions ────────────────────────────────────────────
def _register_job_definition(p):
    name = p.get("jobDefinitionName")
    if not name:
        return error_response_json("ClientException", "jobDefinitionName is required", 400)
    revisions = _job_definitions.setdefault(name, [])
    revision = len(revisions) + 1
    rec = {
        "jobDefinitionName": name,
        "jobDefinitionArn": _jd_arn(name, revision),
        "revision": revision,
        "status": "ACTIVE",
        "type": p.get("type", "container"),
        "containerProperties": p.get("containerProperties", {}),
        "tags": p.get("tags", {}),
    }
    revisions.append(rec)
    return _json(200, {"jobDefinitionName": name,
                       "jobDefinitionArn": rec["jobDefinitionArn"],
                       "revision": revision})


def _describe_job_definitions(p):
    name = p.get("jobDefinitionName")
    revs = _job_definitions.get(name, []) if name else []
    if not name:
        # all
        for n, rs in _job_definitions.items():
            revs.extend(rs)
    return _json(200, {"jobDefinitions": revs})


# ─── jobs ───────────────────────────────────────────────────────
def _submit_job(p):
    name = p.get("jobName")
    if not name:
        return error_response_json("ClientException", "jobName is required", 400)
    job_id = new_uuid()
    now = _now_ms()
    rec = {
        "jobId": job_id,
        "jobArn": _job_arn(job_id),
        "jobName": name,
        "jobQueue": p.get("jobQueue", ""),
        "jobDefinition": p.get("jobDefinition", ""),
        "status": "SUCCEEDED",
        "statusReason": "Stub job completed immediately",
        "createdAt": now,
        "startedAt": now,
        "stoppedAt": now,
        "container": {"exitCode": 0},
        "tags": p.get("tags", {}),
    }
    _jobs[job_id] = rec
    return _json(200, {"jobId": job_id, "jobName": name, "jobArn": rec["jobArn"]})


def _describe_jobs(p):
    ids = p.get("jobs") or []
    out = [_jobs[j] for j in ids if j in _jobs]
    return _json(200, {"jobs": out})


def _list_jobs(p):
    queue = p.get("jobQueue", "")
    queue_name, queue_error = _job_queue_name_from_ref(queue) if queue else ("", None)
    if queue_error:
        return queue_error
    status_filter = p.get("jobStatus")
    out = []
    for j in _jobs.values():
        if queue and not _job_queue_matches(j.get("jobQueue"), queue, queue_name):
            continue
        if status_filter and j.get("status") != status_filter:
            continue
        out.append({
            "jobId": j["jobId"], "jobArn": j["jobArn"], "jobName": j["jobName"],
            "status": j["status"], "createdAt": j["createdAt"],
        })
    return _json(200, {"jobSummaryList": out})


_DISPATCH = {
    "/v1/createcomputeenvironment": _create_compute_environment,
    "/v1/describecomputeenvironments": _describe_compute_environments,
    "/v1/createjobqueue": _create_job_queue,
    "/v1/describejobqueues": _describe_job_queues,
    "/v1/registerjobdefinition": _register_job_definition,
    "/v1/describejobdefinitions": _describe_job_definitions,
    "/v1/submitjob": _submit_job,
    "/v1/describejobs": _describe_jobs,
    "/v1/listjobs": _list_jobs,
}


async def handle_request(method, path, headers, body, query_params):
    if method != "POST":
        return error_response_json("ClientException",
                                   f"Unsupported method {method}", 400)
    fn = _DISPATCH.get(path.rstrip("/").lower())
    if fn is None:
        return error_response_json("InvalidAction",
                                   f"Unsupported batch path: {path}", 400)
    body_text = body.decode("utf-8") if isinstance(body, bytes) else (body or "")
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "invalid JSON body", 400)
    return fn(payload)
