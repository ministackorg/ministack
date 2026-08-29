"""Integration tests for AWS IoT Jobs.

Covers the control plane on the `iot` client (CreateJob, DescribeJob,
ListJobs, GetJobDocument, CancelJob, DeleteJob, ListJobExecutionsForThing,
DescribeJobExecution, CancelJobExecution) and the `iot-jobs-data` device
data plane (GetPendingJobExecutions, StartNextPendingJobExecution,
DescribeJobExecution incl. the `$next` sentinel, UpdateJobExecution).

The timestamp contract is asserted explicitly: control-plane responses use
`timestamp` shapes (epoch seconds — botocore parses them to datetimes),
data-plane responses use raw `long` shapes carrying whole epoch seconds
(the API reference words them as "the time, in seconds since the epoch").
"""

import json
import os
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

import pytest
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

_DOCUMENT = json.dumps({"operation": "reboot", "when": "now"})

# A syntactically plausible SigV4 header whose credential scope routes the
# request to the iot-jobs-data service in the default account and region
# (signatures are not verified) — for raw-HTTP cases boto3 cannot produce.
_JOBS_DATA_AUTH = (
    "AWS4-HMAC-SHA256 "
    "Credential=test/20260811/us-east-1/iot-jobs-data/aws4_request, "
    "SignedHeaders=host, Signature=fake"
)


_IOT_AUTH = (
    "AWS4-HMAC-SHA256 "
    "Credential=test/20260811/us-east-1/iot/aws4_request, "
    "SignedHeaders=host, Signature=fake"
)


def _raw(auth, method, path, payload=None):
    """Raw HTTP with an explicit credential scope; returns (status, body)."""
    req = urllib.request.Request(
        f"{ENDPOINT}{path}",
        data=json.dumps(payload or {}).encode() if method != "GET" else None,
        method=method,
        headers={"Authorization": auth, "content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def _raw_jobs_data(method, path, payload=None):
    """Raw HTTP against the iot-jobs-data plane; returns (status, body dict)."""
    return _raw(_JOBS_DATA_AUTH, method, path, payload)


def _swap_arn_field(arn, index, value):
    """Rewrite one top-level ARN field (3 = region, 4 = account)."""
    fields = arn.split(":", 5)
    fields[index] = value
    return ":".join(fields)


def _account_client(service, account_id, region="us-east-1"):
    """A client whose 12-digit access key selects the MiniStack account."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id=account_id,
        aws_secret_access_key="test",
        region_name=region,
        config=Config(retries={"mode": "standard"}),
    )


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_thing(iot_client, name):
    return iot_client.create_thing(thingName=name)["thingArn"]


def _cleanup(iot_client, jobs=(), things=(), groups=()):
    for job_id in jobs:
        try:
            iot_client.delete_job(jobId=job_id, force=True)
        except ClientError:
            pass
    for thing in things:
        try:
            iot_client.delete_thing(thingName=thing)
        except ClientError:
            pass
    for group in groups:
        try:
            iot_client.delete_thing_group(thingGroupName=group)
        except ClientError:
            pass


def _assert_epoch_seconds(value):
    """A data-plane stamp must be a whole epoch-seconds integer (a `long` on
    the wire, documented "in seconds since the epoch") — milliseconds here
    would be off by 1000x."""
    assert isinstance(value, int)
    assert abs(value - time.time()) < 5 * 60


# ---------------------------------------------------------------------------
# Create / describe / document — and the timestamp contract
# ---------------------------------------------------------------------------


def test_iot_jobs_create_describe_and_pending(iot_client, iot_jobs_data):
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        resp = iot_client.create_job(
            jobId=job_id,
            targets=[thing_arn],
            document=_DOCUMENT,
            description="reboot fleet",
        )
        assert resp["jobId"] == job_id
        assert resp["jobArn"].endswith(f":job/{job_id}")

        desc = iot_client.describe_job(jobId=job_id)["job"]
        assert desc["status"] == "IN_PROGRESS"
        assert desc["targetSelection"] == "SNAPSHOT"
        assert desc["targets"] == [thing_arn]
        assert desc["jobProcessDetails"]["numberOfQueuedThings"] == 1
        # Control plane emits `timestamp` shapes (epoch seconds): botocore
        # must parse createdAt into a datetime near now — a millisecond
        # value here would blow up as "year 58580 is out of range".
        created = desc["createdAt"]
        assert isinstance(created, datetime)
        assert abs((created - datetime.now(timezone.utc)).total_seconds()) < 300

        pending = iot_jobs_data.get_pending_job_executions(thingName=thing)
        assert pending["inProgressJobs"] == []
        queued = pending["queuedJobs"]
        assert [q["jobId"] for q in queued] == [job_id]
        assert queued[0]["versionNumber"] == 1
        assert queued[0]["executionNumber"] == 1
        # Data plane emits `long` shapes: whole epoch seconds.
        _assert_epoch_seconds(queued[0]["queuedAt"])
        _assert_epoch_seconds(queued[0]["lastUpdatedAt"])
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_get_job_document(iot_client):
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
        assert iot_client.get_job_document(jobId=job_id)["document"] == _DOCUMENT
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_document_source_serves_a_placeholder_and_is_not_fetched(
    iot_client, iot_jobs_data
):
    """Documented divergence: AWS fetches `documentSource` from S3 and serves
    its CONTENT to devices. MiniStack does not fetch it — it serves a
    placeholder naming the source, so a `documentSource` job still creates,
    describes, and runs its whole execution lifecycle locally. This test pins
    that choice; changing it means changing README + CHANGELOG too."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    source = "https://ministack-jobs.s3.us-east-1.amazonaws.com/reboot.json"
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(
            jobId=job_id, targets=[thing_arn], documentSource=source
        )
        # The control plane echoes the source it was handed...
        assert iot_client.describe_job(jobId=job_id)["documentSource"] == source
        # ...and both document reads serve the placeholder, never S3 content.
        assert json.loads(iot_client.get_job_document(jobId=job_id)["document"]) == {
            "documentSource": source
        }
        execution = iot_jobs_data.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert json.loads(execution["jobDocument"]) == {"documentSource": source}
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_create_duplicate_and_unknown_target_rejected(iot_client):
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
        with pytest.raises(ClientError) as ei:
            iot_client.create_job(
                jobId=job_id, targets=[thing_arn], document=_DOCUMENT
            )
        assert ei.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"

        ghost_arn = thing_arn.rsplit("/", 1)[0] + "/" + _unique("no-such-thing")
        with pytest.raises(ClientError) as ei:
            iot_client.create_job(
                jobId=_unique("job"), targets=[ghost_arn], document=_DOCUMENT
            )
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


# ---------------------------------------------------------------------------
# Device lifecycle: start-next → update (optimistic concurrency) → job done
# ---------------------------------------------------------------------------


def test_iot_jobs_start_next_update_and_auto_complete(iot_client, iot_jobs_data):
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        started = iot_jobs_data.start_next_pending_job_execution(thingName=thing)
        execution = started["execution"]
        assert execution["jobId"] == job_id
        assert execution["status"] == "IN_PROGRESS"
        assert execution["versionNumber"] == 2  # start bumped it from 1
        assert execution["jobDocument"] == _DOCUMENT
        _assert_epoch_seconds(execution["startedAt"])

        pending = iot_jobs_data.get_pending_job_executions(thingName=thing)
        assert [e["jobId"] for e in pending["inProgressJobs"]] == [job_id]
        assert pending["queuedJobs"] == []

        # A wrong expectedVersion is rejected with the code the iot-jobs-data
        # model declares for UpdateJobExecution — InvalidStateTransitionException,
        # NOT the VersionConflictException the control plane uses. botocore only
        # synthesizes exception classes for modeled errors, so an unmodeled code
        # would make the device's `except client.exceptions.…` raise
        # AttributeError instead of catching. The message carries the current
        # version so the device can resync without a separate describe.
        assert not hasattr(iot_jobs_data.exceptions, "VersionConflictException")
        with pytest.raises(
            iot_jobs_data.exceptions.InvalidStateTransitionException
        ) as ei:
            iot_jobs_data.update_job_execution(
                jobId=job_id, thingName=thing, status="SUCCEEDED",
                expectedVersion=1,
            )
        error = ei.value.response["Error"]
        assert error["Code"] == "InvalidStateTransitionException"
        assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        assert "found version 2" in error["Message"]

        updated = iot_jobs_data.update_job_execution(
            jobId=job_id,
            thingName=thing,
            status="SUCCEEDED",
            statusDetails={"progress": "100"},
            expectedVersion=2,
            includeJobExecutionState=True,
        )
        state = updated["executionState"]
        assert state["status"] == "SUCCEEDED"
        assert state["versionNumber"] == 3
        assert state["statusDetails"] == {"progress": "100"}

        # A terminal execution cannot be updated again.
        with pytest.raises(ClientError) as ei:
            iot_jobs_data.update_job_execution(
                jobId=job_id, thingName=thing, status="FAILED"
            )
        assert (
            ei.value.response["Error"]["Code"] == "InvalidStateTransitionException"
        )

        # All executions terminal → the (SNAPSHOT) job auto-completes.
        job = iot_client.describe_job(jobId=job_id)["job"]
        assert job["status"] == "COMPLETED"
        assert job["jobProcessDetails"]["numberOfSucceededThings"] == 1
        assert isinstance(job["completedAt"], datetime)
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_update_rejects_service_side_statuses(
    iot_client, iot_jobs_data
):
    """A device may only report IN_PROGRESS / SUCCEEDED / FAILED / REJECTED;
    the service-side statuses (CANCELED, TIMED_OUT, REMOVED) must be rejected
    with InvalidRequestException (400), as on AWS."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        for status in ("CANCELED", "TIMED_OUT", "REMOVED"):
            with pytest.raises(ClientError) as ei:
                iot_jobs_data.update_job_execution(
                    jobId=job_id, thingName=thing, status=status
                )
            error = ei.value.response["Error"]
            assert error["Code"] == "InvalidRequestException", status
            assert (
                ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
            ), status

        # The rejected updates must not have touched the execution.
        execution = iot_jobs_data.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert execution["status"] == "QUEUED"
        assert execution["versionNumber"] == 1
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_update_non_numeric_expected_version_is_400(iot_client):
    """A non-numeric expectedVersion (only reachable outside boto3's client-
    side typing) must be a clean InvalidRequestException, not a 500."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        status, body = _raw_jobs_data(
            "POST",
            f"/things/{quote(thing)}/jobs/{quote(job_id)}",
            {"status": "SUCCEEDED", "expectedVersion": "not-a-number"},
        )
        assert status == 400
        assert body["__type"] == "InvalidRequestException"
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_data_post_to_jobs_collection_is_unsupported(iot_client):
    """POST /things/{t}/jobs is not an iot-jobs-data operation — it must get
    the standard unsupported-path 400, not fall through to UpdateJobExecution
    with an empty job id (which used to 404)."""
    thing = _unique("jobs-thing")
    try:
        _create_thing(iot_client, thing)
        status, body = _raw_jobs_data(
            "POST", f"/things/{quote(thing)}/jobs", {"status": "SUCCEEDED"}
        )
        assert status == 400
        assert "Unsupported iot-jobs-data path" in json.dumps(body)
    finally:
        _cleanup(iot_client, things=[thing])


def test_iot_jobs_device_cannot_move_execution_back_to_queued(
    iot_client, iot_jobs_data
):
    """QUEUED is a real execution status but not a legal device transition: a
    device rewinding its own execution gets InvalidStateTransitionException
    (409) — distinct from the 400 the service-side statuses get, because the
    status is valid and only the transition is not."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
        started = iot_jobs_data.start_next_pending_job_execution(thingName=thing)
        assert started["execution"]["status"] == "IN_PROGRESS"

        with pytest.raises(
            iot_jobs_data.exceptions.InvalidStateTransitionException
        ) as ei:
            iot_jobs_data.update_job_execution(
                jobId=job_id, thingName=thing, status="QUEUED"
            )
        assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409

        execution = iot_jobs_data.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert execution["status"] == "IN_PROGRESS"
        assert execution["versionNumber"] == 2  # the refusal changed nothing
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_fleet_completes_only_when_every_execution_is_terminal(
    iot_client, iot_jobs_data
):
    """A job over two things stays IN_PROGRESS while one execution is still
    outstanding: auto-completion keys off ALL executions being terminal, not
    the first one to report."""
    first = _unique("jobs-thing")
    second = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        first_arn = _create_thing(iot_client, first)
        second_arn = _create_thing(iot_client, second)
        iot_client.create_job(
            jobId=job_id, targets=[first_arn, second_arn], document=_DOCUMENT
        )

        iot_jobs_data.update_job_execution(
            jobId=job_id, thingName=first, status="SUCCEEDED"
        )
        job = iot_client.describe_job(jobId=job_id)["job"]
        assert job["status"] == "IN_PROGRESS"
        assert "completedAt" not in job
        details = job["jobProcessDetails"]
        assert details["numberOfSucceededThings"] == 1
        assert details["numberOfQueuedThings"] == 1

        # The second device fails — either terminal status finishes the job.
        iot_jobs_data.update_job_execution(
            jobId=job_id, thingName=second, status="FAILED"
        )
        job = iot_client.describe_job(jobId=job_id)["job"]
        assert job["status"] == "COMPLETED"
        assert job["jobProcessDetails"]["numberOfFailedThings"] == 1
        assert isinstance(job["completedAt"], datetime)
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[first, second])


# ---------------------------------------------------------------------------
# Routing: the jobs routes must not shadow thing CRUD
# ---------------------------------------------------------------------------


def test_iot_jobs_advertised_endpoint_host_reaches_the_data_plane(iot_client):
    """The documented device flow: `DescribeEndpoint(endpointType='iot:Jobs')`
    hands out `{prefix}.jobs.iot.{region}`, and a request carrying that Host
    must land on the jobs data plane. Routed to the `iot` control plane
    instead, `GET /things/{t}/jobs` is ListJobExecutionsForThing and silently
    answers a different envelope."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        endpoint = iot_client.describe_endpoint(endpointType="iot:Jobs")[
            "endpointAddress"
        ]
        assert ".jobs.iot." in endpoint
        request = urllib.request.Request(
            f"{ENDPOINT}/things/{quote(thing)}/jobs",
            method="GET",
            headers={"Host": endpoint},
        )
        with urllib.request.urlopen(request, timeout=5) as resp:
            body = json.loads(resp.read())

        # The GetPendingJobExecutions envelope — not `executionSummaries`.
        assert [q["jobId"] for q in body["queuedJobs"]] == [job_id]
        assert body["inProgressJobs"] == []
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_thing_literally_named_jobs_still_does_thing_crud(
    iot_client, iot_jobs_data
):
    """`jobs` is a legal thing name, so `/things/jobs` is thing CRUD — not the
    jobs collection. Recognizing the jobs routes by a bare `jobs` substring
    instead of the segment AFTER the thing name broke create/describe/delete
    for such a thing with an `Unsupported IoT path` 400."""
    thing = "jobs"
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        assert iot_client.describe_thing(thingName=thing)["thingName"] == thing

        # One segment deeper, the thing's own job routes still work.
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
        assert [
            s["jobId"]
            for s in iot_client.list_job_executions_for_thing(thingName=thing)[
                "executionSummaries"
            ]
        ] == [job_id]
        assert [
            q["jobId"]
            for q in iot_jobs_data.get_pending_job_executions(thingName=thing)[
                "queuedJobs"
            ]
        ] == [job_id]

        _cleanup(iot_client, jobs=[job_id])
        iot_client.delete_thing(thingName=thing)
        with pytest.raises(ClientError) as ei:
            iot_client.describe_thing(thingName=thing)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_next_sentinel_peek_then_start(iot_client, iot_jobs_data):
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        # GET $next is a peek: it must not start the execution.
        peeked = iot_jobs_data.describe_job_execution(
            jobId="$next", thingName=thing
        )["execution"]
        assert peeked["jobId"] == job_id
        assert peeked["status"] == "QUEUED"
        assert peeked["versionNumber"] == 1
        assert peeked["jobDocument"] == _DOCUMENT

        # PUT $next starts it.
        started = iot_jobs_data.start_next_pending_job_execution(thingName=thing)
        assert started["execution"]["status"] == "IN_PROGRESS"

        # Nothing further queued: peeking now returns the in-progress one.
        again = iot_jobs_data.describe_job_execution(
            jobId="$next", thingName=thing
        )["execution"]
        assert again["status"] == "IN_PROGRESS"
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_start_next_empty_for_idle_thing(iot_client, iot_jobs_data):
    thing = _unique("jobs-thing")
    try:
        _create_thing(iot_client, thing)
        resp = iot_jobs_data.start_next_pending_job_execution(thingName=thing)
        assert "execution" not in resp
    finally:
        _cleanup(iot_client, things=[thing])


# ---------------------------------------------------------------------------
# SNAPSHOT vs CONTINUOUS group targeting
# ---------------------------------------------------------------------------


def test_iot_jobs_snapshot_vs_continuous_group_targets(iot_client, iot_jobs_data):
    group = _unique("jobs-group")
    early = _unique("jobs-thing")
    late = _unique("jobs-thing")
    snapshot_job = _unique("job-snap")
    continuous_job = _unique("job-cont")
    try:
        group_arn = iot_client.create_thing_group(thingGroupName=group)[
            "thingGroupArn"
        ]
        _create_thing(iot_client, early)
        iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=early)

        iot_client.create_job(
            jobId=snapshot_job, targets=[group_arn], document=_DOCUMENT,
            targetSelection="SNAPSHOT",
        )
        iot_client.create_job(
            jobId=continuous_job, targets=[group_arn], document=_DOCUMENT,
            targetSelection="CONTINUOUS",
        )

        # A thing added AFTER job creation: the SNAPSHOT job resolved its
        # membership once at create time and never sees it; the CONTINUOUS
        # job re-resolves lazily and does.
        _create_thing(iot_client, late)
        iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=late)

        late_jobs = {
            q["jobId"]
            for q in iot_jobs_data.get_pending_job_executions(thingName=late)[
                "queuedJobs"
            ]
        }
        assert continuous_job in late_jobs
        assert snapshot_job not in late_jobs

        early_jobs = {
            s["jobId"]
            for s in iot_client.list_job_executions_for_thing(thingName=early)[
                "executionSummaries"
            ]
        }
        assert {snapshot_job, continuous_job} <= early_jobs

        # ListJobs filters on targetSelection, not just status.
        continuous_listed = {
            j["jobId"]
            for j in iot_client.list_jobs(targetSelection="CONTINUOUS")["jobs"]
        }
        assert continuous_job in continuous_listed
        assert snapshot_job not in continuous_listed
        snapshot_listed = {
            j["jobId"]
            for j in iot_client.list_jobs(targetSelection="SNAPSHOT")["jobs"]
        }
        assert snapshot_job in snapshot_listed
        assert continuous_job not in snapshot_listed
    finally:
        _cleanup(
            iot_client,
            jobs=[snapshot_job, continuous_job],
            things=[early, late],
            groups=[group],
        )


def test_iot_jobs_continuous_late_thing_first_call_is_update(
    iot_client, iot_jobs_data
):
    """A thing added to a CONTINUOUS job's target group after creation must
    be servable even when its very FIRST data-plane call is
    UpdateJobExecution — the execution materializes lazily on that call, like
    on every other data-plane read."""
    group = _unique("jobs-group")
    late = _unique("jobs-thing")
    job_id = _unique("job-cont")
    try:
        group_arn = iot_client.create_thing_group(thingGroupName=group)[
            "thingGroupArn"
        ]
        iot_client.create_job(
            jobId=job_id, targets=[group_arn], document=_DOCUMENT,
            targetSelection="CONTINUOUS",
        )

        _create_thing(iot_client, late)
        iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=late)

        updated = iot_jobs_data.update_job_execution(
            jobId=job_id,
            thingName=late,
            status="IN_PROGRESS",
            includeJobExecutionState=True,
        )
        state = updated["executionState"]
        assert state["status"] == "IN_PROGRESS"
        assert state["versionNumber"] == 2  # materialized at 1, update bumped
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[late], groups=[group])


# ---------------------------------------------------------------------------
# Listing + cancel/delete paths
# ---------------------------------------------------------------------------


def test_iot_jobs_list_executions_and_cancel_execution(iot_client):
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        listed = iot_client.list_job_executions_for_thing(thingName=thing)
        summaries = [
            s for s in listed["executionSummaries"] if s["jobId"] == job_id
        ]
        assert len(summaries) == 1
        summary = summaries[0]["jobExecutionSummary"]
        assert summary["status"] == "QUEUED"
        assert summary["executionNumber"] == 1
        assert isinstance(summary["queuedAt"], datetime)

        iot_client.cancel_job_execution(jobId=job_id, thingName=thing)
        execution = iot_client.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert execution["status"] == "CANCELED"
        assert execution["versionNumber"] == 2
        assert execution["thingArn"] == thing_arn

        with pytest.raises(ClientError) as ei:
            iot_client.cancel_job_execution(jobId=job_id, thingName=thing)
        assert (
            ei.value.response["Error"]["Code"] == "InvalidStateTransitionException"
        )
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_cancel_job_and_delete(iot_client, iot_jobs_data):
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        # An IN_PROGRESS job cannot be deleted without force.
        with pytest.raises(ClientError) as ei:
            iot_client.delete_job(jobId=job_id)
        assert (
            ei.value.response["Error"]["Code"] == "InvalidStateTransitionException"
        )

        canceled = iot_client.cancel_job(jobId=job_id, comment="rollback")
        assert canceled["jobId"] == job_id

        job = iot_client.describe_job(jobId=job_id)["job"]
        assert job["status"] == "CANCELED"
        # Canceling the job canceled its QUEUED execution too.
        assert job["jobProcessDetails"]["numberOfCanceledThings"] == 1
        assert (
            iot_jobs_data.get_pending_job_executions(thingName=thing)["queuedJobs"]
            == []
        )

        listed = iot_client.list_jobs(status="CANCELED")
        assert job_id in {j["jobId"] for j in listed["jobs"]}

        # A canceled job deletes without force, and is then really gone.
        iot_client.delete_job(jobId=job_id)
        with pytest.raises(ClientError) as ei:
            iot_client.describe_job(jobId=job_id)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
        assert (
            iot_client.list_job_executions_for_thing(thingName=thing)[
                "executionSummaries"
            ]
            == []
        )
    finally:
        _cleanup(iot_client, things=[thing])


# ---------------------------------------------------------------------------
# CancelJob with in-flight executions + control-plane version conflicts
# ---------------------------------------------------------------------------


def test_iot_jobs_cancel_job_without_force_leaves_in_progress(
    iot_client, iot_jobs_data
):
    """CancelJob without force cancels the job but leaves an IN_PROGRESS
    execution untouched — only QUEUED executions are swept, as on AWS."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
        started = iot_jobs_data.start_next_pending_job_execution(thingName=thing)
        assert started["execution"]["status"] == "IN_PROGRESS"

        iot_client.cancel_job(jobId=job_id)

        job = iot_client.describe_job(jobId=job_id)["job"]
        assert job["status"] == "CANCELED"
        execution = iot_client.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert execution["status"] == "IN_PROGRESS"
        assert execution["versionNumber"] == 2  # only the start bumped it
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_cancel_job_force_cancels_in_progress(
    iot_client, iot_jobs_data
):
    """CancelJob with force=True also cancels IN_PROGRESS executions and
    bumps their versionNumber."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
        started = iot_jobs_data.start_next_pending_job_execution(thingName=thing)
        assert started["execution"]["versionNumber"] == 2

        iot_client.cancel_job(jobId=job_id, force=True)

        execution = iot_client.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert execution["status"] == "CANCELED"
        assert execution["versionNumber"] == 3  # force-cancel bumped it again
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_cancel_execution_wrong_expected_version(iot_client):
    """Control-plane CancelJobExecution with a stale expectedVersion is
    rejected with VersionConflictException (modeled in the `iot` service
    model, so boto3 surfaces the code) and leaves the execution untouched."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        with pytest.raises(ClientError) as ei:
            iot_client.cancel_job_execution(
                jobId=job_id, thingName=thing, force=True, expectedVersion=5
            )
        error = ei.value.response["Error"]
        assert error["Code"] == "VersionConflictException"
        assert "found version 1" in error["Message"]

        execution = iot_client.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert execution["status"] == "QUEUED"
        assert execution["versionNumber"] == 1
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


# ---------------------------------------------------------------------------
# Request validation: targets, jobId, targetSelection
# ---------------------------------------------------------------------------


def test_iot_jobs_create_rejects_targets_outside_the_callers_scope(iot_client):
    """A thing ARN from another region or account names nothing this caller can
    target — the resolver skips it, so a job accepted on such a target
    materializes zero executions, never auto-completes, and can only be deleted
    with force. AWS answers ResourceNotFoundException; validation therefore
    gates on the same scope check the resolver applies, not on the ARN's
    resource segment alone."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        foreign_arns = [
            _swap_arn_field(thing_arn, 3, "eu-central-1"),   # another region
            _swap_arn_field(thing_arn, 4, "999999999999"),   # another account
        ]
        for arn in foreign_arns:
            with pytest.raises(ClientError) as ei:
                iot_client.create_job(
                    jobId=job_id, targets=[arn], document=_DOCUMENT
                )
            assert (
                ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
            ), arn
            assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

        # A mixed target list is rejected on the foreign member too — one bad
        # target must not create a job whose fleet is quietly short.
        with pytest.raises(ClientError) as ei:
            iot_client.create_job(
                jobId=job_id,
                targets=[thing_arn, foreign_arns[0]],
                document=_DOCUMENT,
            )
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # Nothing was created — not even a shell of a job to get stuck.
        with pytest.raises(ClientError) as ei:
            iot_client.describe_job(jobId=job_id)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_create_rejects_job_ids_the_models_forbid(iot_client):
    """Both service models declare JobId as `[a-zA-Z0-9_-]` with max 64 —
    stricter than the thing-name pattern. A jobId containing `:` would be
    accepted control-side and then be unreachable for the device that has to
    ask for it by id."""
    thing = _unique("jobs-thing")
    accepted = "a" * 64
    try:
        thing_arn = _create_thing(iot_client, thing)
        for job_id in ("job:with:colons", "j" * 65, "job.with.dots"):
            with pytest.raises(ClientError) as ei:
                iot_client.create_job(
                    jobId=job_id, targets=[thing_arn], document=_DOCUMENT
                )
            error = ei.value.response["Error"]
            assert error["Code"] == "InvalidRequestException", job_id
            assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
            assert "jobId" in error["Message"]

        # The 64-character boundary itself is legal.
        iot_client.create_job(
            jobId=accepted, targets=[thing_arn], document=_DOCUMENT
        )
        assert iot_client.describe_job(jobId=accepted)["job"]["jobId"] == accepted
    finally:
        _cleanup(iot_client, jobs=[accepted], things=[thing])


def test_iot_jobs_create_rejects_unknown_target_selection(iot_client):
    """targetSelection is a two-value enum (SNAPSHOT | CONTINUOUS) that botocore
    does not enforce client-side. An unrecognized value used to be stored
    verbatim, where it read as "not CONTINUOUS" — silently snapshotting a job
    the caller asked to be something else."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        for selection in ("ROLLING", "snapshot", ""):
            with pytest.raises(ClientError) as ei:
                iot_client.create_job(
                    jobId=job_id,
                    targets=[thing_arn],
                    document=_DOCUMENT,
                    targetSelection=selection,
                )
            error = ei.value.response["Error"]
            assert error["Code"] == "InvalidRequestException", selection
            assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        with pytest.raises(ClientError):
            iot_client.describe_job(jobId=job_id)
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


# ---------------------------------------------------------------------------
# Thing deletion sweeps the thing's executions
# ---------------------------------------------------------------------------


def test_iot_jobs_deleting_a_thing_sweeps_its_execution_and_unblocks_the_job(
    iot_client, iot_jobs_data
):
    """A job execution belongs to its thing: deleting the thing must take the
    execution with it. Left behind, the execution of a thing that no longer
    exists is a non-terminal execution nobody can ever report on, holding its
    job out of COMPLETED forever."""
    reporting = _unique("jobs-thing")
    deleted = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        reporting_arn = _create_thing(iot_client, reporting)
        deleted_arn = _create_thing(iot_client, deleted)
        iot_client.create_job(
            jobId=job_id,
            targets=[reporting_arn, deleted_arn],
            document=_DOCUMENT,
        )
        iot_jobs_data.update_job_execution(
            jobId=job_id, thingName=reporting, status="SUCCEEDED"
        )
        job = iot_client.describe_job(jobId=job_id)["job"]
        assert job["status"] == "IN_PROGRESS"
        assert job["jobProcessDetails"]["numberOfQueuedThings"] == 1

        iot_client.delete_thing(thingName=deleted)

        job = iot_client.describe_job(jobId=job_id)["job"]
        assert job["status"] == "COMPLETED"
        details = job["jobProcessDetails"]
        assert details["numberOfQueuedThings"] == 0
        assert details["numberOfSucceededThings"] == 1
        with pytest.raises(ClientError) as ei:
            iot_client.describe_job_execution(jobId=job_id, thingName=deleted)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[reporting, deleted])


def test_iot_jobs_recreated_thing_starts_with_no_execution_history(
    iot_client, iot_jobs_data
):
    """Thing names are reusable, so a swept execution must really be gone: a
    new thing registered under a deleted one's name would otherwise inherit its
    predecessor's in-flight job and be handed a rollout it was never a target
    of."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
        started = iot_jobs_data.start_next_pending_job_execution(thingName=thing)
        assert started["execution"]["status"] == "IN_PROGRESS"

        iot_client.delete_thing(thingName=thing)
        _create_thing(iot_client, thing)

        assert (
            iot_client.list_job_executions_for_thing(thingName=thing)[
                "executionSummaries"
            ]
            == []
        )
        pending = iot_jobs_data.get_pending_job_executions(thingName=thing)
        assert pending["queuedJobs"] == []
        assert pending["inProgressJobs"] == []
        with pytest.raises(ClientError) as ei:
            iot_jobs_data.describe_job_execution(jobId=job_id, thingName=thing)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


# ---------------------------------------------------------------------------
# statusDetails: nested on the control plane, flat on the device plane
# ---------------------------------------------------------------------------


def test_iot_jobs_status_details_are_nested_on_the_control_plane_only(
    iot_client, iot_jobs_data
):
    """The same map has two shapes: the `iot` model wraps it in
    JobExecutionStatusDetails (`{"detailsMap": {...}}`) while `iot-jobs-data`
    returns it flat. Emitting the flat map on the control plane would leave
    boto3's `statusDetails["detailsMap"]` missing; emitting the nested one on
    the device plane would break every device reading its own progress keys."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    details = {"step": "download", "percent": "40"}
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        iot_jobs_data.update_job_execution(
            jobId=job_id,
            thingName=thing,
            status="IN_PROGRESS",
            statusDetails=details,
        )
        control = iot_client.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert control["statusDetails"] == {"detailsMap": details}
        device = iot_jobs_data.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert device["statusDetails"] == details

        # The control plane's own writer (CancelJobExecution takes a FLAT
        # statusDetails map) round-trips into the same two shapes.
        cancel_details = {"reason": "superseded"}
        iot_client.cancel_job_execution(
            jobId=job_id, thingName=thing, force=True, statusDetails=cancel_details
        )
        control = iot_client.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert control["status"] == "CANCELED"
        assert control["statusDetails"] == {"detailsMap": cancel_details}
        device = iot_jobs_data.describe_job_execution(
            jobId=job_id, thingName=thing
        )["execution"]
        assert device["statusDetails"] == cancel_details
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


# ---------------------------------------------------------------------------
# Not-found paths and list filters
# ---------------------------------------------------------------------------


def test_iot_jobs_unknown_job_is_not_found_on_every_path(
    iot_client, iot_jobs_data
):
    """Every read and mutate path answers ResourceNotFoundException (404) for a
    job that does not exist — no 400s, no empty 200s, on either plane."""
    thing = _unique("jobs-thing")
    ghost = _unique("ghost-job")
    try:
        _create_thing(iot_client, thing)
        calls = {
            "DescribeJob": lambda: iot_client.describe_job(jobId=ghost),
            "GetJobDocument": lambda: iot_client.get_job_document(jobId=ghost),
            "CancelJob": lambda: iot_client.cancel_job(jobId=ghost),
            "DeleteJob": lambda: iot_client.delete_job(jobId=ghost),
            "DescribeJobExecution": lambda: iot_client.describe_job_execution(
                jobId=ghost, thingName=thing
            ),
            "CancelJobExecution": lambda: iot_client.cancel_job_execution(
                jobId=ghost, thingName=thing
            ),
            "data:DescribeJobExecution": (
                lambda: iot_jobs_data.describe_job_execution(
                    jobId=ghost, thingName=thing
                )
            ),
            "data:UpdateJobExecution": (
                lambda: iot_jobs_data.update_job_execution(
                    jobId=ghost, thingName=thing, status="SUCCEEDED"
                )
            ),
        }
        for label, call in calls.items():
            with pytest.raises(ClientError) as ei:
                call()
            assert (
                ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
            ), label
            assert (
                ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
            ), label
    finally:
        _cleanup(iot_client, things=[thing])


def test_iot_jobs_list_executions_for_thing_filters(iot_client, iot_jobs_data):
    """ListJobExecutionsForThing narrows by `status` (modeled) and by `jobId`
    (a MiniStack convenience the `iot` model does not declare, so only raw HTTP
    can send it) — an unfiltered list must not be served for either."""
    thing = _unique("jobs-thing")
    queued_job = _unique("job-queued")
    done_job = _unique("job-done")
    try:
        thing_arn = _create_thing(iot_client, thing)
        for job_id in (queued_job, done_job):
            iot_client.create_job(
                jobId=job_id, targets=[thing_arn], document=_DOCUMENT
            )
        iot_jobs_data.update_job_execution(
            jobId=done_job, thingName=thing, status="SUCCEEDED"
        )

        def listed(**kwargs):
            return {
                s["jobId"]
                for s in iot_client.list_job_executions_for_thing(
                    thingName=thing, **kwargs
                )["executionSummaries"]
            }

        assert listed() == {queued_job, done_job}
        assert listed(status="QUEUED") == {queued_job}
        assert listed(status="SUCCEEDED") == {done_job}
        assert listed(status="FAILED") == set()

        status, body = _raw(
            _IOT_AUTH, "GET", f"/things/{quote(thing)}/jobs?jobId={quote(done_job)}"
        )
        assert status == 200
        assert [s["jobId"] for s in body["executionSummaries"]] == [done_job]
    finally:
        _cleanup(iot_client, jobs=[queued_job, done_job], things=[thing])


# ---------------------------------------------------------------------------
# Account / region isolation
# ---------------------------------------------------------------------------


def test_iot_jobs_do_not_bleed_across_accounts_or_regions():
    """Two accounts may hold the same jobId over a same-named thing without
    seeing each other's job, document, or execution — and the same account in
    another region is a third, separate scope. Both stores are keyed by
    (account, region), and the ARNs each caller reads back must say so."""
    job_id = _unique("job")
    thing = _unique("jobs-thing")
    a = _account_client("iot", "111111111111")
    b = _account_client("iot", "222222222222")
    a_eu = _account_client("iot", "111111111111", region="eu-west-1")
    a_device = _account_client("iot-jobs-data", "111111111111")
    try:
        for client, document in ((a, '{"op": "a"}'), (b, '{"op": "b"}')):
            thing_arn = client.create_thing(thingName=thing)["thingArn"]
            client.create_job(
                jobId=job_id, targets=[thing_arn], document=document
            )

        assert a.get_job_document(jobId=job_id)["document"] == '{"op": "a"}'
        assert b.get_job_document(jobId=job_id)["document"] == '{"op": "b"}'
        assert a.describe_job(jobId=job_id)["job"]["targets"] == [
            f"arn:aws:iot:us-east-1:111111111111:thing/{thing}"
        ]
        assert b.describe_job(jobId=job_id)["job"]["jobArn"].startswith(
            "arn:aws:iot:us-east-1:222222222222:"
        )

        # Another region of account A is a scope of its own.
        with pytest.raises(ClientError) as ei:
            a_eu.describe_job(jobId=job_id)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
        assert job_id not in {j["jobId"] for j in a_eu.list_jobs()["jobs"]}

        # A's device reporting done completes A's job only.
        a_device.update_job_execution(
            jobId=job_id, thingName=thing, status="SUCCEEDED"
        )
        assert a.describe_job(jobId=job_id)["job"]["status"] == "COMPLETED"
        b_job = b.describe_job(jobId=job_id)["job"]
        assert b_job["status"] == "IN_PROGRESS"
        assert b_job["jobProcessDetails"]["numberOfQueuedThings"] == 1

        # Deleting A's job leaves B's standing.
        a.delete_job(jobId=job_id, force=True)
        assert b.describe_job(jobId=job_id)["job"]["jobId"] == job_id
    finally:
        for client in (a, b):
            _cleanup(client, jobs=[job_id], things=[thing])


def test_iot_jobs_lists_do_not_paginate_and_return_no_token(iot_client):
    """ListJobs / ListJobExecutionsForThing serve one unbounded page.

    ``maxResults`` and ``nextToken`` are accepted (botocore validates them
    client-side) and ignored, and no ``nextToken`` is ever returned - a
    paginator terminates after one page instead of looping. Pinned so a
    future partial implementation cannot change the shape silently.
    """
    thing = _unique("jobs-thing")
    job_a = _unique("job")
    job_b = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        for job_id in (job_a, job_b):
            iot_client.create_job(
                jobId=job_id, targets=[thing_arn], document=_DOCUMENT
            )

        listed = iot_client.list_jobs(maxResults=1)
        ours = {j["jobId"] for j in listed["jobs"]} & {job_a, job_b}
        assert ours == {job_a, job_b}, "maxResults is ignored, not honored"
        assert "nextToken" not in listed

        executions = iot_client.list_job_executions_for_thing(
            thingName=thing, maxResults=1
        )
        assert len(executions["executionSummaries"]) == 2
        assert "nextToken" not in executions
    finally:
        _cleanup(iot_client, jobs=[job_a, job_b], things=[thing])


# ---------------------------------------------------------------------------
# Jobs over MQTT: the reserved $aws/things/<t>/jobs/# bridge
# ---------------------------------------------------------------------------
from test_iot_data import _collect_shadow_frames  # noqa: E402

_DOCUMENT_OBJECT = json.loads(_DOCUMENT)


def _frames_by_topic(received):
    return {topic: json.loads(payload) for topic, payload in received}


def _assert_epoch_seconds(value):
    """Stamps must be whole epoch seconds — milliseconds are off by 1000x."""
    assert isinstance(value, int)
    assert abs(value - time.time()) < 5 * 60


def test_jobs_mqtt_create_job_notifies_and_get_lists(iot_client, iot_data_client):
    """CreateJob publishes notify (per-status aggregate) + notify-next (full
    execution incl. the jobDocument as an object) to each target thing; a
    publish on jobs/get answers get/accepted with the queued summary."""
    thing = _unique("jobs-mqtt")
    job_id = _unique("job")
    thing_arn = _create_thing(iot_client, thing)
    base = f"$aws/things/{thing}/jobs"

    received = _collect_shadow_frames(
        f"{base}/#",
        lambda: iot_client.create_job(
            jobId=job_id, targets=[thing_arn], document=_DOCUMENT
        ),
        want=2,
    )
    frames = _frames_by_topic(received)

    notify = frames[f"{base}/notify"]
    # Empty status lists are omitted: only QUEUED appears.
    assert set(notify["jobs"]) == {"QUEUED"}
    summary = notify["jobs"]["QUEUED"][0]
    assert summary["jobId"] == job_id
    _assert_epoch_seconds(notify["timestamp"])
    _assert_epoch_seconds(summary["queuedAt"])
    _assert_epoch_seconds(summary["lastUpdatedAt"])

    nn = frames[f"{base}/notify-next"]
    execution = nn["execution"]
    assert execution["jobId"] == job_id
    assert execution["status"] == "QUEUED"
    # Over MQTT the job document is a JSON object (a string over HTTP).
    assert execution["jobDocument"] == _DOCUMENT_OBJECT
    _assert_epoch_seconds(nn["timestamp"])
    _assert_epoch_seconds(execution["queuedAt"])

    received = _collect_shadow_frames(
        f"{base}/get/accepted",
        lambda: iot_data_client.publish(
            topic=f"{base}/get",
            payload=json.dumps({"clientToken": "tok-get"}).encode(),
        ),
        want=1,
    )
    doc = _frames_by_topic(received)[f"{base}/get/accepted"]
    assert doc["clientToken"] == "tok-get"
    assert [q["jobId"] for q in doc["queuedJobs"]] == [job_id]
    assert doc["inProgressJobs"] == []
    _assert_epoch_seconds(doc["timestamp"])
    _assert_epoch_seconds(doc["queuedJobs"][0]["queuedAt"])


def test_jobs_mqtt_create_behind_existing_job_no_notify_next(
    iot_client, iot_data_client
):
    """A job created behind an existing pending job changes the pending set
    (notify) but not its front — no notify-next."""
    thing = _unique("jobs-mqtt-2nd")
    first_job = _unique("job")
    second_job = _unique("job")
    thing_arn = _create_thing(iot_client, thing)
    base = f"$aws/things/{thing}/jobs"

    # Drain the first job's own notify + notify-next before subscribing again.
    _collect_shadow_frames(
        f"{base}/#",
        lambda: iot_client.create_job(
            jobId=first_job, targets=[thing_arn], document=_DOCUMENT
        ),
        want=2,
    )

    received = _collect_shadow_frames(
        f"{base}/#",
        lambda: iot_client.create_job(
            jobId=second_job, targets=[thing_arn], document=_DOCUMENT
        ),
        want=2,
        timeout=3.0,
    )
    topics = [topic for topic, _ in received]
    assert f"{base}/notify-next" not in topics
    notify = _frames_by_topic(received)[f"{base}/notify"]
    assert [s["jobId"] for s in notify["jobs"]["QUEUED"]] == [first_job, second_job]


def test_jobs_mqtt_start_next_of_first_job_is_silent(iot_client, iot_data_client):
    """start-next hands out the execution on start-next/accepted, but fires
    NEITHER notify nor notify-next: QUEUED -> IN_PROGRESS keeps the execution
    both pending and at the front of the queue (live-captured silence)."""
    thing = _unique("jobs-mqtt-sn")
    job_id = _unique("job")
    thing_arn = _create_thing(iot_client, thing)
    base = f"$aws/things/{thing}/jobs"
    _collect_shadow_frames(
        f"{base}/#",
        lambda: iot_client.create_job(
            jobId=job_id, targets=[thing_arn], document=_DOCUMENT
        ),
        want=2,
    )

    received = _collect_shadow_frames(
        f"{base}/#",
        lambda: iot_data_client.publish(
            topic=f"{base}/start-next",
            payload=json.dumps({"clientToken": "tok-sn"}).encode(),
        ),
        want=3,  # request echo + accepted; a 3rd frame would be a bug
        timeout=3.0,
    )
    topics = [topic for topic, _ in received]
    assert f"{base}/notify" not in topics
    assert f"{base}/notify-next" not in topics

    doc = _frames_by_topic(received)[f"{base}/start-next/accepted"]
    execution = doc["execution"]
    assert execution["jobId"] == job_id
    assert execution["status"] == "IN_PROGRESS"
    assert execution["jobDocument"] == _DOCUMENT_OBJECT
    assert execution["versionNumber"] == 2
    _assert_epoch_seconds(doc["timestamp"])
    _assert_epoch_seconds(execution["startedAt"])


def test_jobs_mqtt_update_include_flags_and_terminal_notify(
    iot_client, iot_data_client
):
    """update/accepted is minimal unless the include flags ask for more; a
    terminal report then publishes accepted first, notify with the emptied
    aggregate, and a bare notify-next (captured order)."""
    thing = _unique("jobs-mqtt-rt")
    job_id = _unique("job")
    thing_arn = _create_thing(iot_client, thing)
    base = f"$aws/things/{thing}/jobs"
    _collect_shadow_frames(
        f"{base}/#",
        lambda: iot_client.create_job(
            jobId=job_id, targets=[thing_arn], document=_DOCUMENT
        ),
        want=2,
    )
    _collect_shadow_frames(
        f"{base}/start-next/accepted",
        lambda: iot_data_client.publish(topic=f"{base}/start-next", payload=b"{}"),
        want=1,
    )

    received = _collect_shadow_frames(
        f"{base}/{job_id}/update/accepted",
        lambda: iot_data_client.publish(
            topic=f"{base}/{job_id}/update",
            payload=json.dumps(
                {
                    "status": "IN_PROGRESS",
                    "expectedVersion": 2,
                    "includeJobExecutionState": True,
                    "includeJobDocument": True,
                    "clientToken": "tok-up",
                }
            ).encode(),
        ),
        want=1,
    )
    doc = _frames_by_topic(received)[f"{base}/{job_id}/update/accepted"]
    assert doc["executionState"]["status"] == "IN_PROGRESS"
    assert doc["executionState"]["versionNumber"] == 3
    assert doc["jobDocument"] == _DOCUMENT_OBJECT
    assert doc["clientToken"] == "tok-up"

    received = _collect_shadow_frames(
        f"{base}/#",
        lambda: iot_data_client.publish(
            topic=f"{base}/{job_id}/update",
            payload=json.dumps(
                {
                    "status": "SUCCEEDED",
                    "expectedVersion": 3,
                    "clientToken": "tok-done",
                }
            ).encode(),
        ),
        want=4,  # request echo + accepted + notify + notify-next
    )
    topics = [topic for topic, _ in received]
    frames = _frames_by_topic(received)

    # Without the include flags the terminal accepted carries nothing else
    # (live capture: clientToken + timestamp only).
    accepted = frames[f"{base}/{job_id}/update/accepted"]
    assert set(accepted) == {"clientToken", "timestamp"}
    assert accepted["clientToken"] == "tok-done"
    _assert_epoch_seconds(accepted["timestamp"])

    # The last pending job left the set: {} aggregate, bare notify-next.
    notify = frames[f"{base}/notify"]
    assert notify["jobs"] == {}
    assert set(notify) == {"jobs", "timestamp"}
    nn = frames[f"{base}/notify-next"]
    assert set(nn) == {"timestamp"}

    # accepted first, then notify, then notify-next — the captured order.
    assert topics.index(f"{base}/{job_id}/update/accepted") < topics.index(
        f"{base}/notify"
    ) < topics.index(f"{base}/notify-next")


def test_jobs_mqtt_version_mismatch_rejected(iot_client, iot_data_client):
    """A stale expectedVersion answers update/rejected with the string
    VersionMismatch code, the captured message wording, and the execution's
    current state."""
    thing = _unique("jobs-mqtt-ver")
    job_id = _unique("job")
    thing_arn = _create_thing(iot_client, thing)
    iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
    base = f"$aws/things/{thing}/jobs"

    received = _collect_shadow_frames(
        f"{base}/{job_id}/update/rejected",
        lambda: iot_data_client.publish(
            topic=f"{base}/{job_id}/update",
            payload=json.dumps(
                {
                    "status": "SUCCEEDED",
                    "expectedVersion": 99,
                    "clientToken": "tok-ver",
                }
            ).encode(),
        ),
        want=1,
    )
    doc = _frames_by_topic(received)[f"{base}/{job_id}/update/rejected"]
    assert doc["code"] == "VersionMismatch"
    assert doc["message"] == "Expected version 99 but found version 1"
    assert doc["executionState"] == {"status": "QUEUED", "versionNumber": 1}
    assert doc["clientToken"] == "tok-ver"
    _assert_epoch_seconds(doc["timestamp"])


def test_jobs_mqtt_update_unknown_job_rejected(iot_client, iot_data_client):
    """An update for a job that does not exist answers update/rejected with
    the string ResourceNotFound code — never a silent drop."""
    thing = _unique("jobs-mqtt-rej")
    _create_thing(iot_client, thing)
    base = f"$aws/things/{thing}/jobs"

    received = _collect_shadow_frames(
        f"{base}/no-such-job/update/rejected",
        lambda: iot_data_client.publish(
            topic=f"{base}/no-such-job/update",
            payload=json.dumps({"status": "SUCCEEDED", "clientToken": "tok-x"}).encode(),
        ),
        want=1,
    )
    doc = _frames_by_topic(received)[f"{base}/no-such-job/update/rejected"]
    assert doc["code"] == "ResourceNotFound"
    assert doc["clientToken"] == "tok-x"


def test_jobs_mqtt_next_sentinel_get(iot_client, iot_data_client):
    """``$next`` as the jobId on the get topic resolves the front of the
    pending queue WITHOUT starting it — the documented MQTT counterpart of
    the HTTP plane's DescribeJobExecution sentinel."""
    thing = _unique("jobs-mqtt-next")
    job_id = _unique("job")
    thing_arn = _create_thing(iot_client, thing)
    iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
    base = f"$aws/things/{thing}/jobs"

    received = _collect_shadow_frames(
        f"{base}/$next/get/accepted",
        lambda: iot_data_client.publish(
            topic=f"{base}/$next/get",
            payload=json.dumps({"clientToken": "tok-next"}).encode(),
        ),
        want=1,
    )
    doc = _frames_by_topic(received)[f"{base}/$next/get/accepted"]
    execution = doc["execution"]
    assert execution["jobId"] == job_id
    assert execution["status"] == "QUEUED"  # peeked, not started
    assert execution["versionNumber"] == 1
    assert execution["jobDocument"] == _DOCUMENT_OBJECT
    assert doc["clientToken"] == "tok-next"

    received = _collect_shadow_frames(
        f"{base}/get/accepted",
        lambda: iot_data_client.publish(topic=f"{base}/get", payload=b"{}"),
        want=1,
    )
    doc = _frames_by_topic(received)[f"{base}/get/accepted"]
    assert [q["jobId"] for q in doc["queuedJobs"]] == [job_id]
    assert doc["inProgressJobs"] == []


def test_jobs_mqtt_non_object_payload_rejected(iot_client, iot_data_client):
    """A payload that parses as JSON but is not an object (an array, say) is
    rejected with the InvalidJson code instead of being coerced to {}."""
    thing = _unique("jobs-mqtt-json")
    _create_thing(iot_client, thing)
    base = f"$aws/things/{thing}/jobs"

    received = _collect_shadow_frames(
        f"{base}/get/rejected",
        lambda: iot_data_client.publish(topic=f"{base}/get", payload=b"[1, 2]"),
        want=1,
    )
    doc = _frames_by_topic(received)[f"{base}/get/rejected"]
    assert doc["code"] == "InvalidJson"
    _assert_epoch_seconds(doc["timestamp"])


def _qa_publish(iot_data_client, topic, payload):
    iot_data_client.publish(topic=topic, qos=1, payload=payload)


def test_jobs_mqtt_malformed_json_is_rejected_invalidjson(iot_client, iot_data_client):
    thing = _unique("qa-bad-json")
    _create_thing(iot_client, thing)
    base = f"$aws/things/{thing}/jobs"
    received = _collect_shadow_frames(
        f"{base}/get/rejected",
        lambda: _qa_publish(iot_data_client, f"{base}/get", b"{not json"),
        want=1,
    )
    doc = json.loads(received[0][1])
    assert doc["code"] == "InvalidJson"
    assert isinstance(doc["timestamp"], int)


def test_jobs_mqtt_terminal_update_rejected_terminalstatereached(iot_client, iot_data_client):
    thing = _unique("qa-terminal")
    job_id = _unique("job")
    arn = _create_thing(iot_client, thing)
    iot_client.create_job(jobId=job_id, targets=[arn], document=_DOCUMENT)
    base = f"$aws/things/{thing}/jobs"
    _collect_shadow_frames(
        f"{base}/{job_id}/update/accepted",
        lambda: _qa_publish(iot_data_client, f"{base}/{job_id}/update",
                            json.dumps({"status": "SUCCEEDED"}).encode()),
        want=1,
    )
    received = _collect_shadow_frames(
        f"{base}/{job_id}/update/rejected",
        lambda: _qa_publish(iot_data_client, f"{base}/{job_id}/update",
                            json.dumps({"status": "FAILED"}).encode()),
        want=1,
    )
    assert json.loads(received[0][1])["code"] == "TerminalStateReached"


def test_jobs_mqtt_wrong_version_carries_execution_state(iot_client, iot_data_client):
    thing = _unique("qa-version")
    job_id = _unique("job")
    arn = _create_thing(iot_client, thing)
    iot_client.create_job(jobId=job_id, targets=[arn], document=_DOCUMENT)
    base = f"$aws/things/{thing}/jobs"
    received = _collect_shadow_frames(
        f"{base}/{job_id}/update/rejected",
        lambda: _qa_publish(iot_data_client, f"{base}/{job_id}/update",
                            json.dumps({"status": "IN_PROGRESS",
                                        "expectedVersion": 99,
                                        "clientToken": "qa-1"}).encode()),
        want=1,
    )
    doc = json.loads(received[0][1])
    assert doc["code"] == "VersionMismatch"
    assert doc["clientToken"] == "qa-1"
    assert set(doc["executionState"]) == {"status", "versionNumber"}


def test_jobs_mqtt_job_named_get_does_not_break_thing_level_get(iot_client, iot_data_client):
    thing = _unique("qa-collide")
    arn = _create_thing(iot_client, thing)
    iot_client.create_job(jobId="get", targets=[arn], document=_DOCUMENT)
    try:
        base = f"$aws/things/{thing}/jobs"
        received = _collect_shadow_frames(
            f"{base}/get/accepted",
            lambda: _qa_publish(iot_data_client, f"{base}/get", b"{}"),
            want=1,
        )
        doc = json.loads(received[0][1])
        assert [s["jobId"] for s in doc["queuedJobs"]] == ["get"]
    finally:
        iot_client.delete_job(jobId="get", force=True)


def test_jobs_mqtt_next_get_on_idle_thing_answers_bare_timestamp(iot_client, iot_data_client):
    thing = _unique("qa-idle")
    _create_thing(iot_client, thing)
    base = f"$aws/things/{thing}/jobs"
    received = _collect_shadow_frames(
        f"{base}/$next/get/accepted",
        lambda: _qa_publish(iot_data_client, f"{base}/$next/get",
                            json.dumps({"clientToken": "t0"}).encode()),
        want=1,
    )
    doc = json.loads(received[0][1])
    assert "execution" not in doc
    assert doc["clientToken"] == "t0"


def test_jobs_mqtt_start_next_twice_rehands_in_progress_silently(iot_client, iot_data_client):
    thing = _unique("qa-rehand")
    job_id = _unique("job")
    arn = _create_thing(iot_client, thing)
    iot_client.create_job(jobId=job_id, targets=[arn], document=_DOCUMENT)
    base = f"$aws/things/{thing}/jobs"
    first = _collect_shadow_frames(
        f"{base}/start-next/accepted",
        lambda: _qa_publish(iot_data_client, f"{base}/start-next",
                            json.dumps({"statusDetails": {"step": "a"}}).encode()),
        want=1,
    )
    d1 = json.loads(first[0][1])["execution"]
    assert d1["status"] == "IN_PROGRESS" and d1["statusDetails"] == {"step": "a"}
    second = _collect_shadow_frames(
        f"{base}/start-next/accepted",
        lambda: _qa_publish(iot_data_client, f"{base}/start-next",
                            json.dumps({"statusDetails": {"step": "b"}}).encode()),
        want=1,
    )
    d2 = json.loads(second[0][1])["execution"]
    assert d2["status"] == "IN_PROGRESS"
    assert d2["statusDetails"] == {"step": "a"}
    assert d2["versionNumber"] == d1["versionNumber"]
