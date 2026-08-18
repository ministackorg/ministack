"""IoT Jobs Data Plane HTTP API (``iot-jobs-data`` AWS service).

Implements the device-side REST surface of AWS IoT Jobs:
``GetPendingJobExecutions``, ``StartNextPendingJobExecution``,
``DescribeJobExecution``, and ``UpdateJobExecution``.

Routing reaches us either through credential-scope detection (the SDK signs
requests with the ``iot-jobs-data`` scope — botocore signingName) or via the
host pattern ``{prefix}.jobs.iot.{region}.{host}``, which is what
``DescribeEndpoint(endpointType='iot:Jobs')`` hands out.

This module is a wire adapter: it parses requests, shapes responses, and logs.
The job store, the execution store, and every rule of the execution state
machine live in :mod:`ministack.services.iot`, reached through its public
``jobs_*`` seam (the same way ``iot_data.py`` reaches the shadow store), so
nothing here can mutate an execution behind the control plane's back.

Timestamp contract: the shared records store epoch MILLISECONDS. This data
plane models timestamps as raw ``long`` shapes, so responses emit those
millisecond integers unchanged — unlike the ``iot`` control plane, whose
``timestamp`` shapes botocore parses as epoch SECONDS. Mixing the units up
makes botocore explode while parsing ("year 58580 is out of range"); see
``iot._jobs_now_ms``.
"""

from __future__ import annotations

import logging
from urllib.parse import unquote

from ministack.core.responses import error_response_json, json_response
from ministack.services import iot as _iot_module

logger = logging.getLogger("iot_jobs_data")


# ---------------------------------------------------------------------------
# Persistence (no state of our own — the iot module holds it)
# ---------------------------------------------------------------------------


def get_state() -> dict:
    return {}


def restore_state(data: dict | None) -> None:
    return None


def reset() -> None:
    return None


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


async def handle_request(
    method: str, path: str, headers: dict, body: bytes, query_params: dict
) -> tuple:
    qp = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}

    if not path.startswith("/things/"):
        return _unsupported_path(method, path)
    rest = path[len("/things/"):]
    thing, separator, sub = rest.partition("/")
    thing = unquote(thing)
    if not separator:
        return _unsupported_path(method, path)

    if sub == "jobs":
        # GetPendingJobExecutions is the only operation on the bare
        # /things/{thingName}/jobs collection; anything else (a POST, say)
        # is not part of this API and must not fall through to the {jobId}
        # handlers with an empty job id.
        if method == "GET":
            return _get_pending(thing)
        return _unsupported_path(method, path)

    if not sub.startswith("jobs/"):
        return _unsupported_path(method, path)

    job_id = unquote(sub[len("jobs/"):])
    # `$next` is a sentinel, not a job id, so it must be matched before the
    # generic {jobId} routes.
    if job_id == "$next":
        if method == "GET":
            return _start_next(thing, {}, peek=True)
        if method == "PUT":
            return _start_next(thing, _iot_module.parse_json_body(body), peek=False)
        return _unsupported_path(method, path)
    if method == "GET":
        return _describe_execution(thing, job_id, qp)
    if method == "POST":
        return _update_execution(thing, job_id, _iot_module.parse_json_body(body))
    return _unsupported_path(method, path)


def _unsupported_path(method: str, path: str) -> tuple:
    return error_response_json(
        "InvalidRequestException",
        f"Unsupported iot-jobs-data path: {method} {path}",
        400,
    )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _execution_view(execution: dict, document: str | None) -> dict:
    """Data-plane JobExecution — millisecond `long` timestamps, flat
    statusDetails map."""
    view = {
        "jobId": execution["jobId"],
        "thingName": execution["thingName"],
        "status": execution["status"],
        "statusDetails": execution.get("statusDetails") or {},
        "queuedAt": execution["queuedAt"],
        "lastUpdatedAt": execution["lastUpdatedAt"],
        "executionNumber": execution["executionNumber"],
        "versionNumber": execution["versionNumber"],
    }
    if execution.get("startedAt") is not None:
        view["startedAt"] = execution["startedAt"]
    if document is not None:
        view["jobDocument"] = document
    return view


def _get_pending(thing: str) -> tuple:
    """GetPendingJobExecutions — queued and in-progress, split, ms stamps."""

    def summary(execution: dict) -> dict:
        entry = {
            "jobId": execution["jobId"],
            "queuedAt": execution["queuedAt"],
            "lastUpdatedAt": execution["lastUpdatedAt"],
            "versionNumber": execution["versionNumber"],
            "executionNumber": execution["executionNumber"],
        }
        if execution.get("startedAt") is not None:
            entry["startedAt"] = execution["startedAt"]
        return entry

    pending = _iot_module.jobs_pending_for_thing(thing)
    return json_response({
        "inProgressJobs": [
            summary(e) for e in pending if e["status"] == "IN_PROGRESS"
        ],
        "queuedJobs": [summary(e) for e in pending if e["status"] == "QUEUED"],
    })


def _start_next(thing: str, payload: dict, *, peek: bool) -> tuple:
    """StartNextPendingJobExecution (PUT) / DescribeJobExecution of ``$next``
    (GET, ``peek=True`` — reads without starting)."""
    execution = _iot_module.jobs_start_next_execution(
        thing, status_details=payload.get("statusDetails"), peek=peek
    )
    if execution is None:
        # AWS returns an empty structure when nothing is pending.
        return json_response({})
    if not peek:
        logger.info(
            "IoT Jobs: handed job %s to thing %s (status %s, version %s)",
            execution["jobId"], thing, execution["status"],
            execution["versionNumber"],
        )
    document = _iot_module.jobs_job_document(execution["jobId"])
    return json_response({"execution": _execution_view(execution, document)})


def _describe_execution(thing: str, job_id: str, qp: dict) -> tuple:
    execution = _iot_module.jobs_describe_execution(thing, job_id)
    if execution is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"No job execution found for thing {thing} and job {job_id}",
            404,
        )
    execution_number = qp.get("executionNumber")
    if execution_number is not None:
        try:
            execution_number = int(execution_number)
        except (TypeError, ValueError):
            return error_response_json(
                "InvalidRequestException",
                f"Invalid executionNumber: {execution_number!r}",
                400,
            )
        if execution_number != execution["executionNumber"]:
            return error_response_json(
                "ResourceNotFoundException",
                f"No job execution {execution_number} found for thing {thing} "
                f"and job {job_id}",
                404,
            )
    include_document = str(qp.get("includeJobDocument", "true")).lower() != "false"
    document = _iot_module.jobs_job_document(job_id) if include_document else None
    return json_response({"execution": _execution_view(execution, document)})


def _update_execution(thing: str, job_id: str, payload: dict) -> tuple:
    execution, error = _iot_module.jobs_update_execution(
        thing,
        job_id,
        status=payload.get("status"),
        expected_version=payload.get("expectedVersion"),
        status_details=payload.get("statusDetails"),
    )
    if error:
        return error
    logger.info(
        "IoT Jobs: thing %s reported job %s as %s (version %s)",
        thing, job_id, execution["status"], execution["versionNumber"],
    )

    response: dict = {}
    if str(payload.get("includeJobExecutionState", "")).lower() in ("true", "1"):
        response["executionState"] = {
            "status": execution["status"],
            "statusDetails": execution.get("statusDetails") or {},
            "versionNumber": execution["versionNumber"],
        }
    if str(payload.get("includeJobDocument", "")).lower() in ("true", "1"):
        response["jobDocument"] = _iot_module.jobs_job_document(job_id)
    return json_response(response)
