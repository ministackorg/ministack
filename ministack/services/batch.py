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
import os
import time

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
            short = n.split("/")[-1]
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


def _detect_host_limits():
    """Return the host's installed CPU/memory ceiling MS can compare against.

    Reports *installed* capacity, not currently-available — the gate's purpose
    is "could this jobdef ever run on this host", which is a stable property,
    not a fluctuating one. A dimension that can't be confidently measured on
    this platform returns ``None`` and the corresponding check is skipped, so
    we don't false-positive on exotic environments.
    """
    vcpu = None
    try:
        vcpu = os.cpu_count()
    except Exception:
        vcpu = None

    memory_mib = None
    try:
        page = os.sysconf("SC_PAGE_SIZE")
        pages = os.sysconf("SC_PHYS_PAGES")
        if isinstance(page, int) and isinstance(pages, int) and page > 0 and pages > 0:
            memory_mib = (page * pages) // (1024 * 1024)
    except (AttributeError, ValueError, OSError):
        memory_mib = None
    return {"vcpu": vcpu, "memory_mib": memory_mib}


def _parse_resource_requirements(container_props):
    """Pull VCPU + MEMORY out of a jobdef's ``containerProperties.resourceRequirements``.
    Returns ``(vcpu_or_None, memory_mib_or_None)``. Malformed entries are
    treated as absent so we never block on parser quirks.
    """
    reqs = (container_props or {}).get("resourceRequirements") or []
    vcpu = None
    memory_mib = None
    for r in reqs:
        if not isinstance(r, dict):
            continue
        kind = (r.get("type") or "").upper()
        raw = r.get("value")
        if raw is None:
            continue
        try:
            num = float(raw)
        except (TypeError, ValueError):
            continue
        if kind == "VCPU":
            vcpu = num
        elif kind == "MEMORY":
            try:
                memory_mib = int(num)
            except (TypeError, ValueError):
                continue
    return vcpu, memory_mib


def _resolve_jobdef(ref):
    """Look up a jobdef record by ``name``, ``name:revision``, or full ARN.
    Returns the record or ``None`` — never raises so callers can fall through.
    """
    if not ref:
        return None
    short = ref.split("/")[-1]
    if ":" in short:
        name, _, rev = short.rpartition(":")
        try:
            target_rev = int(rev)
        except ValueError:
            return None
        for r in _job_definitions.get(name, []):
            if r.get("revision") == target_rev:
                return r
        return None
    revs = _job_definitions.get(short, [])
    return revs[-1] if revs else None


def _check_host_fit(container_props):
    """Return an error message if the jobdef can't physically run on this host,
    else ``None``. Both dimensions must be measurable on both sides for the
    check to fire — anything ambiguous passes through.
    """
    limits = _detect_host_limits()
    req_vcpu, req_mem = _parse_resource_requirements(container_props)

    over = []
    if req_vcpu is not None and limits["vcpu"] is not None and req_vcpu > limits["vcpu"]:
        over.append(f"VCPU={req_vcpu:g} (host has {limits['vcpu']})")
    if req_mem is not None and limits["memory_mib"] is not None and req_mem > limits["memory_mib"]:
        over.append(f"MEMORY={req_mem} MiB (host has {limits['memory_mib']} MiB)")
    if not over:
        return None
    return (
        "SubmitJob refused: jobdef requests "
        + ", ".join(over)
        + ". Reduce resourceRequirements for local testing, "
        "or run this jobdef against real AWS Batch."
    )


# ─── jobs ───────────────────────────────────────────────────────
def _submit_job(p):
    name = p.get("jobName")
    if not name:
        return error_response_json("ClientException", "jobName is required", 400)

    # Honest fail-fast for the local-emulator case where the jobdef can't
    # physically fit on this host. Real AWS Batch has unlimited compute so
    # this failure mode doesn't exist there; in MS we'd otherwise return
    # SUCCEEDED for a job that could never actually run, hiding the problem
    # behind a second describe_jobs call. Issue #650.
    jobdef = _resolve_jobdef(p.get("jobDefinition"))
    if jobdef is not None:
        fit_err = _check_host_fit(jobdef.get("containerProperties"))
        if fit_err is not None:
            logger.warning("Batch SubmitJob refused (%s): %s", name, fit_err)
            return error_response_json("ClientException", fit_err, 400)

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
    status_filter = p.get("jobStatus")
    out = []
    for j in _jobs.values():
        if queue and j.get("jobQueue") not in (queue, _jq_arn(queue.split("/")[-1])):
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
        return error_response_json("InvalidRequest",
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
