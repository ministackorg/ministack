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
# Job templates — shapes and messages measured on AWS (eu-central-1,
# 2026-10-05)
# ---------------------------------------------------------------------------

_TEMPLATE_ROLE = "arn:aws:iam::000000000000:role/presign"
_TEMPLATE_CONFIG = {
    "presignedUrlConfig": {"roleArn": _TEMPLATE_ROLE, "expiresInSec": 300},
    "jobExecutionsRolloutConfig": {
        "maximumPerMinute": 50,
        "exponentialRate": {
            "baseRatePerMinute": 5, "incrementFactor": 2,
            "rateIncreaseCriteria": {"numberOfNotifiedThings": 10},
        },
    },
    "abortConfig": {"criteriaList": [{
        "failureType": "FAILED", "action": "CANCEL",
        "thresholdPercentage": 50, "minNumberOfExecutedThings": 5,
    }]},
    "timeoutConfig": {"inProgressTimeoutInMinutes": 30},
    "jobExecutionsRetryConfig": {
        "criteriaList": [{"failureType": "FAILED", "numberOfRetries": 2}]
    },
}


def _delete_templates(iot_client, *template_ids):
    for template_id in template_ids:
        try:
            iot_client.delete_job_template(jobTemplateId=template_id)
        except ClientError:
            pass


def _error(ei):
    err = ei.value.response
    return (
        err["ResponseMetadata"]["HTTPStatusCode"],
        err["Error"]["Code"],
        err["Error"]["Message"],
    )


def _raw_bytes(method, path):
    """Raw HTTP against the iot control plane; returns (status, body bytes)."""
    req = urllib.request.Request(
        f"{ENDPOINT}{path}", method=method, headers={"Authorization": _IOT_AUTH}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, resp.read()


def test_iot_job_template_create_describe_list_delete(iot_client):
    full, minimal = _unique("tmpl-full"), _unique("tmpl-min")
    try:
        created = iot_client.create_job_template(
            jobTemplateId=full, description="full", document=_DOCUMENT,
            tags=[{"Key": "k", "Value": "v"}], **_TEMPLATE_CONFIG,
        )
        assert created["jobTemplateId"] == full
        assert created["jobTemplateArn"] == (
            f"arn:aws:iot:us-east-1:000000000000:jobtemplate/{full}"
        )
        desc = iot_client.describe_job_template(jobTemplateId=full)
        assert desc["description"] == "full"
        assert desc["document"] == _DOCUMENT
        assert isinstance(desc["createdAt"], datetime)
        for member in ("presignedUrlConfig", "abortConfig", "timeoutConfig",
                       "jobExecutionsRetryConfig"):
            assert desc[member] == _TEMPLATE_CONFIG[member], member
        rate = desc["jobExecutionsRolloutConfig"]["exponentialRate"]
        assert rate["rateIncreaseCriteria"] == {"numberOfNotifiedThings": 10}
        # The two `double` members read back as floats, as on AWS.
        assert isinstance(rate["incrementFactor"], float)
        assert isinstance(
            desc["abortConfig"]["criteriaList"][0]["thresholdPercentage"], float
        )

        iot_client.create_job_template(
            jobTemplateId=minimal, description="minimal", document=_DOCUMENT
        )
        # AWS writes every member, an unset one as null, and three structures
        # as objects of nulls.
        status, body = _raw(_IOT_AUTH, "GET", f"/job-templates/{minimal}")
        assert status == 200
        assert body["abortConfig"] is None
        assert body["documentSource"] is None
        assert body["maintenanceWindows"] is None
        assert body["timeoutConfig"] == {"inProgressTimeoutInMinutes": None}
        assert body["presignedUrlConfig"] == {"expiresInSec": None, "roleArn": None}
        assert body["jobExecutionsRolloutConfig"] == {
            "exponentialRate": None, "maximumPerMinute": None
        }

        # Newest first, one per page, behind an opaque token.
        seen, token = [], None
        while True:
            page = iot_client.list_job_templates(
                maxResults=1, **({"nextToken": token} if token else {})
            )
            assert len(page["jobTemplates"]) <= 1
            seen += [t for t in page["jobTemplates"]
                     if t["jobTemplateId"] in (full, minimal)]
            token = page.get("nextToken")
            if not token:
                break
        assert [t["jobTemplateId"] for t in seen] == [minimal, full]
        assert set(seen[0]) == {
            "jobTemplateArn", "jobTemplateId", "description", "createdAt"
        }

        # Delete answers 200 with an empty body.
        assert _raw_bytes("DELETE", f"/job-templates/{full}") == (200, b"")
        with pytest.raises(ClientError) as ei:
            iot_client.describe_job_template(jobTemplateId=full)
        assert _error(ei) == (
            404, "ResourceNotFoundException", f"Job Template {full} cannot be found."
        )
    finally:
        _delete_templates(iot_client, full, minimal)


def test_iot_job_template_from_a_job(iot_client):
    thing, job_id = _unique("jobs-thing"), _unique("job")
    from_job, with_doc = _unique("tmpl-job"), _unique("tmpl-jobdoc")
    try:
        job_arn = iot_client.create_job(
            jobId=job_id, targets=[_create_thing(iot_client, thing)],
            document=_DOCUMENT,
            timeoutConfig={"inProgressTimeoutInMinutes": 15},
            jobExecutionsRetryConfig=_TEMPLATE_CONFIG["jobExecutionsRetryConfig"],
        )["jobArn"]
        iot_client.create_job_template(
            jobTemplateId=from_job, description="from job", jobArn=job_arn
        )
        desc = iot_client.describe_job_template(jobTemplateId=from_job)
        assert desc["description"] == "from job"
        assert desc["document"] == _DOCUMENT
        assert desc["timeoutConfig"] == {"inProgressTimeoutInMinutes": 15}
        assert desc["jobExecutionsRetryConfig"] == (
            _TEMPLATE_CONFIG["jobExecutionsRetryConfig"]
        )
        assert "abortConfig" not in desc

        # A document in the request replaces the job's; the rest still comes
        # from the job.
        iot_client.create_job_template(
            jobTemplateId=with_doc, description="d", jobArn=job_arn, document="{}"
        )
        desc = iot_client.describe_job_template(jobTemplateId=with_doc)
        assert desc["document"] == "{}"
        assert desc["timeoutConfig"] == {"inProgressTimeoutInMinutes": 15}

        with pytest.raises(ClientError) as ei:
            iot_client.create_job_template(
                jobTemplateId=_unique("tmpl"), description="d",
                jobArn=f"{job_arn}-gone",
            )
        assert _error(ei) == (
            404, "ResourceNotFoundException", f"Job {job_id}-gone cannot be found."
        )
    finally:
        _delete_templates(iot_client, from_job, with_doc)
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_job_template_rejects_what_aws_rejects(iot_client):
    template_id = _unique("tmpl")
    try:
        iot_client.create_job_template(
            jobTemplateId=template_id, description="d", document=_DOCUMENT
        )
        with pytest.raises(ClientError) as ei:
            iot_client.create_job_template(
                jobTemplateId=template_id, description="d", document=_DOCUMENT
            )
        assert _error(ei) == (
            409, "ConflictException", f"Job Template {template_id} already exists."
        )
        cases = [
            ({"description": "d", "document": "{}", "documentSource": "https://x/y"},
             "Job document and job document source cannot be specified at the same "
             "time."),
            ({"description": "d"},
             "Neither job document nor job document source is specified."),
            ({"document": "{}"},
             "1 validation error detected: Value null at 'description' failed to "
             "satisfy constraint: Member must not be null"),
            ({"description": "d", "document": "{}",
              "timeoutConfig": {"inProgressTimeoutInMinutes": 0}},
             "Provide valid timeout value, inProgressTimeoutInMinutes cannot be 0."),
            ({"description": "d", "document": "{}",
              "timeoutConfig": {"inProgressTimeoutInMinutes": 10081}},
             "Provide valid timeout value, inProgressTimeoutInMinutes cannot be "
             "10081."),
        ]
        for payload, message in cases:
            status, body = _raw(
                _IOT_AUTH, "PUT", f"/job-templates/{_unique('tmpl')}", payload
            )
            assert (status, body["message"]) == (400, message), payload

        for bad_id, constraint in (
            ("has.dot", "Member must satisfy regular expression pattern: "
                        "[a-zA-Z0-9_-]+"),
            ("a" * 65, "Member must have length less than or equal to 64"),
        ):
            for method in ("PUT", "GET"):
                status, body = _raw(
                    _IOT_AUTH, method, f"/job-templates/{bad_id}",
                    {"description": "d", "document": "{}"},
                )
                assert (status, body["message"]) == (
                    400,
                    f"1 validation error detected: Value '{bad_id}' at "
                    f"'jobTemplateId' failed to satisfy constraint: {constraint}",
                ), (bad_id, method)

        for call in (iot_client.describe_job_template, iot_client.delete_job_template):
            with pytest.raises(ClientError) as ei:
                call(jobTemplateId=f"{template_id}-unknown")
            assert _error(ei) == (
                404, "ResourceNotFoundException",
                f"Job Template {template_id}-unknown cannot be found.",
            )

        for query, message in (
            ("maxResults=0", "1 validation error detected: Value '0' at 'maxResults' "
             "failed to satisfy constraint: Member must have value greater than or "
             "equal to 1"),
            ("maxResults=251", "1 validation error detected: Value '251' at "
             "'maxResults' failed to satisfy constraint: Member must have value less "
             "than or equal to 250"),
            ("nextToken=garbage", "Next token is invalid."),
        ):
            status, body = _raw(_IOT_AUTH, "GET", f"/job-templates?{query}")
            assert (status, body["message"]) == (400, message), query
    finally:
        _delete_templates(iot_client, template_id)


def test_iot_jobs_create_from_a_job_template(iot_client):
    thing = _unique("jobs-thing")
    template_id, mw_template = _unique("tmpl"), _unique("tmpl-mw")
    plain, overridden = _unique("job"), _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        template_arn = iot_client.create_job_template(
            jobTemplateId=template_id, description="from template",
            document=_DOCUMENT, **_TEMPLATE_CONFIG,
        )["jobTemplateArn"]

        iot_client.create_job(jobId=plain, targets=[thing_arn],
                              jobTemplateArn=template_arn)
        job = iot_client.describe_job(jobId=plain)["job"]
        assert job["jobTemplateArn"] == template_arn
        assert job["timeoutConfig"] == {"inProgressTimeoutInMinutes": 30}
        assert job["presignedUrlConfig"]["roleArn"] == _TEMPLATE_ROLE
        assert job["jobExecutionsRolloutConfig"]["maximumPerMinute"] == 50
        assert job["abortConfig"]["criteriaList"][0]["minNumberOfExecutedThings"] == 5
        assert job["jobExecutionsRetryConfig"] == (
            _TEMPLATE_CONFIG["jobExecutionsRetryConfig"]
        )
        assert "description" not in job, "the template's description is not inherited"
        assert iot_client.get_job_document(jobId=plain)["document"] == _DOCUMENT

        # A member the request names replaces the template's whole.
        iot_client.create_job(
            jobId=overridden, targets=[thing_arn], jobTemplateArn=template_arn,
            document='{"own": true}', timeoutConfig={"inProgressTimeoutInMinutes": 99},
            jobExecutionsRetryConfig={"criteriaList": [
                {"failureType": "ALL", "numberOfRetries": 3}]},
        )
        job = iot_client.describe_job(jobId=overridden)["job"]
        assert job["timeoutConfig"] == {"inProgressTimeoutInMinutes": 99}
        assert job["jobExecutionsRetryConfig"] == {"criteriaList": [
            {"failureType": "ALL", "numberOfRetries": 3}]}
        assert job["jobExecutionsRolloutConfig"]["maximumPerMinute"] == 50
        assert iot_client.get_job_document(jobId=overridden)["document"] == (
            '{"own": true}'
        )

        # Deleting the template leaves the jobs made from it alone.
        iot_client.delete_job_template(jobTemplateId=template_id)
        assert iot_client.describe_job(jobId=plain)["job"]["jobTemplateArn"] == (
            template_arn
        )

        mw_arn = iot_client.create_job_template(
            jobTemplateId=mw_template, description="mw", document=_DOCUMENT,
            maintenanceWindows=[{"startTime": "cron(0 2 ? * MON *)",
                                 "durationInMinutes": 60}],
        )["jobTemplateArn"]
        job_arn = template_arn.replace(f":jobtemplate/{template_id}", f":job/{plain}")
        for arn, status, code, message in (
            (template_arn, 404, "ResourceNotFoundException",
             f"Job Template {template_id} cannot be found."),
            (job_arn, 400, "InvalidRequestException",
             "Resource type should be jobtemplate but found invalid resource type job"),
            (mw_arn, 400, "InvalidRequestException",
             "TargetSelection is invalid. MaintenanceWindow cannot be used with "
             "SNAPSHOT job."),
        ):
            with pytest.raises(ClientError) as ei:
                iot_client.create_job(jobId=_unique("job"), targets=[thing_arn],
                                      jobTemplateArn=arn)
            assert _error(ei) == (status, code, message), arn
    finally:
        _delete_templates(iot_client, template_id, mw_template)
        _cleanup(iot_client, jobs=[plain, overridden], things=[thing])


def test_iot_job_templates_are_scoped_and_persisted():
    """A template is visible to its own account and region only, travels
    through get_state / load_persisted_state, and reset() drops it."""
    from ministack.services import iot as iot_module

    template_id = _unique("tmpl")
    a = _account_client("iot", "111111111111")
    b = _account_client("iot", "222222222222")
    a_eu = _account_client("iot", "111111111111", region="eu-west-1")
    try:
        a.create_job_template(jobTemplateId=template_id, description="a",
                              document=_DOCUMENT)
        for other in (b, a_eu):
            with pytest.raises(ClientError) as ei:
                other.describe_job_template(jobTemplateId=template_id)
            assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
            assert template_id not in {
                t["jobTemplateId"] for t in other.list_job_templates()["jobTemplates"]
            }
    finally:
        _delete_templates(a, template_id)

    record = {"jobTemplateId": template_id, "description": "restored"}
    iot_module._job_templates[template_id] = record
    state = iot_module.get_state()
    iot_module.reset()
    assert template_id not in iot_module._job_templates
    try:
        iot_module.load_persisted_state(state)
        assert iot_module._job_templates[template_id] == record
    finally:
        iot_module.reset()


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
    with InvalidStateTransitionException (409) and the message "The status of job
    execution cannot be changed to be X" — captured eu-north-1 2026-09-19."""
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
            assert error["Code"] == "InvalidStateTransitionException", status
            assert (
                ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
            ), status
            assert f"cannot be changed to be {status}" in error["Message"], status

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


def test_iot_jobs_update_rejects_a_mismatched_execution_number(
    iot_client, iot_jobs_data
):
    """UpdateJobExecution's executionNumber identifies one execution on the
    device. Nothing re-queues here, so only number 1 exists and any other
    number is ResourceNotFoundException, as the describe path already answers."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        with pytest.raises(ClientError) as ei:
            iot_jobs_data.update_job_execution(
                jobId=job_id, thingName=thing, status="IN_PROGRESS",
                executionNumber=7,
            )
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # Number 1 is the one that exists.
        iot_jobs_data.update_job_execution(
            jobId=job_id, thingName=thing, status="IN_PROGRESS", executionNumber=1,
        )
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_describe_job_steady_state_shape(iot_client):
    """Captured eu-north-1 2026-09-19 across QUEUED, IN_PROGRESS and SUCCEEDED
    executions: isConcurrent is false throughout, processingTargets is null
    throughout, and timeoutConfig/schedulingConfig come back as {} while
    abortConfig is omitted."""
def test_iot_jobs_endpoint_host_reaches_the_data_plane(iot_client):
    """A request carrying the jobs endpoint Host `{prefix}.jobs.iot.{region}`
    must land on the jobs data plane. Routed to the `iot` control plane
    instead, `GET /things/{t}/jobs` is ListJobExecutionsForThing and silently
    answers a different envelope."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
        job = iot_client.describe_job(jobId=job_id)["job"]
        assert job["isConcurrent"] is False
        assert "processingTargets" not in job["jobProcessDetails"]
        assert job["timeoutConfig"] == {}
        assert job["schedulingConfig"] == {}
        assert "abortConfig" not in job
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_endpoint_type_is_retired(iot_client):
    """AWS retired `iot:Jobs`: DescribeEndpoint answers InvalidRequestException
    and points at iot:Data-ATS (captured eu-north-1 2026-09-19). Jobs and
    Commands are served from the Data-ATS endpoint, where the `iot-jobs-data`
    signing scope selects the data plane."""
    with pytest.raises(ClientError) as exc:
        iot_client.describe_endpoint(endpointType="iot:Jobs")
    assert exc.value.response["Error"]["Code"] == "InvalidRequestException"
    assert "iot:Data-ATS" in exc.value.response["Error"]["Message"]


def test_iot_jobs_data_plane_reachable_on_the_data_ats_endpoint(
    iot_client, iot_jobs_data
):
    """The replacement flow: the Data-ATS endpoint serves the jobs data plane.
    A request signed with the `iot-jobs-data` scope must answer the
    GetPendingJobExecutions envelope, not the control plane's
    `executionSummaries`."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)

        endpoint = iot_client.describe_endpoint(endpointType="iot:Data-ATS")[
            "endpointAddress"
        ]
        assert ".jobs.iot." not in endpoint
        endpoint = "a1b2c3.jobs.iot.us-east-1.localhost"
        request = urllib.request.Request(
            f"{ENDPOINT}/things/{quote(thing)}/jobs",
            method="GET",
            headers={"Host": endpoint},
        )
        with urllib.request.urlopen(request, timeout=5) as resp:
            body = json.loads(resp.read())

        body = iot_jobs_data.get_pending_job_executions(thingName=thing)
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


def _executions_for(iot_client, thing, job_id):
    """(executionNumber, status) pairs ListJobExecutionsForThing lists."""
    summaries = iot_client.list_job_executions_for_thing(
        thingName=thing, jobId=job_id
    )["executionSummaries"]
    return [
        (s["jobExecutionSummary"]["executionNumber"], s["jobExecutionSummary"]["status"])
        for s in summaries
    ]


def _drive(iot_jobs_data, thing, job_id, status):
    """Start the thing's pending execution, then report `status` unless it
    is IN_PROGRESS."""
    iot_jobs_data.start_next_pending_job_execution(thingName=thing)
    if status != "IN_PROGRESS":
        iot_jobs_data.update_job_execution(
            thingName=thing, jobId=job_id, status=status
        )


def test_iot_jobs_continuous_thing_rejoining_group_runs_job_again(
    iot_client, iot_jobs_data
):
    """A thing that leaves a CONTINUOUS job's target group and rejoins it
    after its execution finished gets the next execution number, QUEUED.
    Leaving moves a QUEUED execution to REMOVED; IN_PROGRESS, SUCCEEDED and
    FAILED keep their status, and a thing rejoining while IN_PROGRESS gets
    nothing new (measured eu-central-1 2026-10-05)."""
    group = _unique("jobs-group")
    job_id = _unique("job-cont")
    cases = ["queued", "inprogress", "succeeded", "failed", "stays"]
    things = {case: _unique(f"jobs-{case}") for case in cases}
    try:
        group_arn = iot_client.create_thing_group(thingGroupName=group)[
            "thingGroupArn"
        ]
        iot_client.create_job(
            jobId=job_id, targets=[group_arn], document=_DOCUMENT,
            targetSelection="CONTINUOUS",
        )
        for thing in things.values():
            _create_thing(iot_client, thing)
            iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=thing)
        _drive(iot_jobs_data, things["inprogress"], job_id, "IN_PROGRESS")
        _drive(iot_jobs_data, things["succeeded"], job_id, "SUCCEEDED")
        _drive(iot_jobs_data, things["failed"], job_id, "FAILED")
        _drive(iot_jobs_data, things["stays"], job_id, "SUCCEEDED")

        movers = [things[case] for case in cases if case != "stays"]
        for thing in movers:
            iot_client.remove_thing_from_thing_group(
                thingGroupName=group, thingName=thing
            )
        removed = iot_client.describe_job_execution(
            jobId=job_id, thingName=things["queued"]
        )["execution"]
        assert (removed["status"], removed["versionNumber"]) == ("REMOVED", 1)
        pending = iot_jobs_data.get_pending_job_executions(thingName=things["queued"])
        assert pending["queuedJobs"] == []
        assert _executions_for(iot_client, things["inprogress"], job_id) == [
            (1, "IN_PROGRESS")
        ]
        assert _executions_for(iot_client, things["failed"], job_id) == [(1, "FAILED")]

        for thing in movers:
            iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=thing)
        assert _executions_for(iot_client, things["queued"], job_id) == [
            (2, "QUEUED"), (1, "REMOVED"),
        ]
        assert _executions_for(iot_client, things["succeeded"], job_id) == [
            (2, "QUEUED"), (1, "SUCCEEDED"),
        ]
        assert _executions_for(iot_client, things["failed"], job_id) == [
            (2, "QUEUED"), (1, "FAILED"),
        ]
        assert _executions_for(iot_client, things["inprogress"], job_id) == [
            (1, "IN_PROGRESS")
        ]
        assert _executions_for(iot_client, things["stays"], job_id) == [
            (1, "SUCCEEDED")
        ]

        # The device sees the new execution; the newest one answers a
        # describe without executionNumber, a number picks an older one.
        pending = iot_jobs_data.get_pending_job_executions(
            thingName=things["succeeded"]
        )
        assert [(q["jobId"], q["executionNumber"]) for q in pending["queuedJobs"]] == [
            (job_id, 2)
        ]
        newest = iot_client.describe_job_execution(
            jobId=job_id, thingName=things["succeeded"]
        )["execution"]
        assert (newest["executionNumber"], newest["status"]) == (2, "QUEUED")
        assert newest["versionNumber"] == 1
        first = iot_client.describe_job_execution(
            jobId=job_id, thingName=things["succeeded"], executionNumber=1
        )["execution"]
        assert (first["executionNumber"], first["status"]) == (1, "SUCCEEDED")
        with pytest.raises(ClientError) as exc:
            iot_client.describe_job_execution(
                jobId=job_id, thingName=things["succeeded"], executionNumber=3
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # Every execution counts, the earlier ones of a rejoined thing too.
        details = iot_client.describe_job(jobId=job_id)["job"]["jobProcessDetails"]
        assert details["numberOfQueuedThings"] == 3
        assert details["numberOfInProgressThings"] == 1
        assert details["numberOfRemovedThings"] == 1
        assert details["numberOfSucceededThings"] == 2
        assert details["numberOfFailedThings"] == 1

        # The IN_PROGRESS execution that rejoined finishes as a member: no new
        # execution follows. A second leave and rejoin runs the job again.
        _drive(iot_jobs_data, things["inprogress"], job_id, "SUCCEEDED")
        assert _executions_for(iot_client, things["inprogress"], job_id) == [
            (1, "SUCCEEDED")
        ]
        _drive(iot_jobs_data, things["succeeded"], job_id, "SUCCEEDED")
        for thing in (things["inprogress"], things["succeeded"]):
            iot_client.remove_thing_from_thing_group(
                thingGroupName=group, thingName=thing
            )
            iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=thing)
        assert _executions_for(iot_client, things["inprogress"], job_id) == [
            (2, "QUEUED"), (1, "SUCCEEDED"),
        ]
        assert _executions_for(iot_client, things["succeeded"], job_id) == [
            (3, "QUEUED"), (2, "SUCCEEDED"), (1, "SUCCEEDED"),
        ]
    finally:
        _cleanup(iot_client, jobs=[job_id], things=list(things.values()), groups=[group])


def test_iot_jobs_continuous_execution_finished_outside_group_runs_again(
    iot_client, iot_jobs_data
):
    """An IN_PROGRESS execution can still finish after its thing left the
    target group; rejoining afterwards queues execution 2 (measured
    eu-central-1 2026-10-05)."""
    group = _unique("jobs-group")
    thing = _unique("jobs-thing")
    job_id = _unique("job-cont")
    try:
        group_arn = iot_client.create_thing_group(thingGroupName=group)[
            "thingGroupArn"
        ]
        iot_client.create_job(
            jobId=job_id, targets=[group_arn], document=_DOCUMENT,
            targetSelection="CONTINUOUS",
        )
        _create_thing(iot_client, thing)
        iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=thing)
        _drive(iot_jobs_data, thing, job_id, "IN_PROGRESS")
        iot_client.remove_thing_from_thing_group(thingGroupName=group, thingName=thing)

        state = iot_jobs_data.update_job_execution(
            thingName=thing, jobId=job_id, status="SUCCEEDED",
            includeJobExecutionState=True,
        )["executionState"]
        assert state["status"] == "SUCCEEDED"
        assert _executions_for(iot_client, thing, job_id) == [(1, "SUCCEEDED")]

        iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=thing)
        assert _executions_for(iot_client, thing, job_id) == [
            (2, "QUEUED"), (1, "SUCCEEDED"),
        ]
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing], groups=[group])


def test_iot_jobs_continuous_join_event_queues_only_without_pending_execution(
    iot_client, iot_jobs_data
):
    """The job runs again when a thing JOINS one of its target groups while
    its newest execution is finished; membership alone does not re-run it
    (measured eu-central-1 2026-10-05). In both groups and finished, leaving
    one adds nothing and rejoining it queues #2; AWS also queued #2 right at
    the leave in 2 of 4 runs, and this pins the other answer. Finished in
    one group, joining the other queues #2. Adding a thing to a group it is
    already in is no join. IN_PROGRESS through a leave and rejoin stays the
    only execution, also after it finishes."""
    groups = [_unique("jobs-group"), _unique("jobs-group")]
    job_id = _unique("job-cont")
    cases = ["both", "second", "again", "busy"]
    things = {case: _unique(f"jobs-{case}") for case in cases}
    try:
        arns = [
            iot_client.create_thing_group(thingGroupName=g)["thingGroupArn"]
            for g in groups
        ]
        iot_client.create_job(
            jobId=job_id, targets=arns, document=_DOCUMENT,
            targetSelection="CONTINUOUS",
        )
        for case, thing in things.items():
            _create_thing(iot_client, thing)
            for g in groups if case in ("both", "busy") else groups[:1]:
                iot_client.add_thing_to_thing_group(thingGroupName=g, thingName=thing)
        for case in ("both", "second", "again"):
            _drive(iot_jobs_data, things[case], job_id, "SUCCEEDED")
        _drive(iot_jobs_data, things["busy"], job_id, "IN_PROGRESS")

        for case in ("both", "busy"):
            iot_client.remove_thing_from_thing_group(
                thingGroupName=groups[0], thingName=things[case]
            )
        iot_client.add_thing_to_thing_group(
            thingGroupName=groups[1], thingName=things["second"]
        )
        iot_client.add_thing_to_thing_group(
            thingGroupName=groups[0], thingName=things["again"]
        )
        assert _executions_for(iot_client, things["both"], job_id) == [(1, "SUCCEEDED")]
        assert _executions_for(iot_client, things["busy"], job_id) == [
            (1, "IN_PROGRESS")
        ]
        assert _executions_for(iot_client, things["second"], job_id) == [
            (2, "QUEUED"), (1, "SUCCEEDED"),
        ]
        assert _executions_for(iot_client, things["again"], job_id) == [
            (1, "SUCCEEDED")
        ]

        for case in ("both", "busy"):
            iot_client.add_thing_to_thing_group(
                thingGroupName=groups[0], thingName=things[case]
            )
        assert _executions_for(iot_client, things["both"], job_id) == [
            (2, "QUEUED"), (1, "SUCCEEDED"),
        ]
        assert _executions_for(iot_client, things["busy"], job_id) == [
            (1, "IN_PROGRESS")
        ]
        _drive(iot_jobs_data, things["busy"], job_id, "SUCCEEDED")
        assert _executions_for(iot_client, things["busy"], job_id) == [
            (1, "SUCCEEDED")
        ]
    finally:
        _cleanup(iot_client, jobs=[job_id], things=list(things.values()), groups=groups)


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
# UpdateJob
# ---------------------------------------------------------------------------


def _patch_job(job_id, payload):
    """Raw UpdateJob, so botocore's own validation does not get in the way."""
    return _raw(_IOT_AUTH, "PATCH", f"/jobs/{job_id}", payload)


def test_iot_jobs_update_job_replaces_each_member(iot_client, iot_jobs_data):
    """UpdateJob replaces each given member whole and answers with an empty body."""
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(
            jobId=job_id, targets=[thing_arn], document=_DOCUMENT,
            description="before", timeoutConfig={"inProgressTimeoutInMinutes": 60},
        )
        iot_jobs_data.start_next_pending_job_execution(thingName=thing)
        before = iot_client.describe_job(jobId=job_id)["job"]
        time.sleep(1.1)
        req = urllib.request.Request(
            f"{ENDPOINT}/jobs/{job_id}", data=b'{"description": "after"}',
            method="PATCH",
            headers={"Authorization": _IOT_AUTH, "content-type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert (resp.status, resp.read()) == (200, b"")
        after = iot_client.describe_job(jobId=job_id)["job"]
        assert after["description"] == "after"
        assert after["lastUpdatedAt"] > before["lastUpdatedAt"]
        assert after["jobProcessDetails"] == before["jobProcessDetails"]
        assert after["timeoutConfig"] == {"inProgressTimeoutInMinutes": 60}

        role = "arn:aws:iam::000000000000:role/presign"
        rate = {"baseRatePerMinute": 5, "incrementFactor": 2,
                "rateIncreaseCriteria": {"numberOfNotifiedThings": 10}}
        abort = {"criteriaList": [{"failureType": "FAILED", "action": "CANCEL",
                                   "thresholdPercentage": 50,
                                   "minNumberOfExecutedThings": 3}]}
        for member, value, stored in (
            ("presignedUrlConfig", {"roleArn": role, "expiresInSec": 600}, None),
            ("jobExecutionsRolloutConfig",
             {"maximumPerMinute": 20, "exponentialRate": rate},
             {"maximumPerMinute": 20,
              "exponentialRate": {**rate, "incrementFactor": 2.0}}),
            # A member given replaces the stored one: the exponentialRate goes.
            ("jobExecutionsRolloutConfig", {"maximumPerMinute": 30}, None),
            ("abortConfig", abort, None),
            ("timeoutConfig", {"inProgressTimeoutInMinutes": 120}, None),
        ):
            iot_client.update_job(jobId=job_id, **{member: value})
            status, body = _raw(_IOT_AUTH, "GET", f"/jobs/{job_id}")
            assert body["job"][member] == (stored or value), member
        threshold = body["job"]["abortConfig"]["criteriaList"][0]["thresholdPercentage"]
        assert isinstance(threshold, float)
        assert body["job"]["jobProcessDetails"]["numberOfInProgressThings"] == 1
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_update_job_rejects_invalid_values(iot_client):
    thing = _unique("jobs-thing")
    job_id = _unique("job")
    role = "arn:aws:iam::000000000000:role/presign"
    rate = {"maximumPerMinute": 20, "exponentialRate": {
        "baseRatePerMinute": 5, "incrementFactor": 2,
        "rateIncreaseCriteria": {"numberOfNotifiedThings": 10}}}

    def with_rate(**over):
        return {"jobExecutionsRolloutConfig": {
            **rate, "exponentialRate": {**rate["exponentialRate"], **over}}}

    def clause(value, member, rule):
        return f"Value '{value}' at '{member}' failed to satisfy constraint: Member must {rule}"

    def constraint(value, member, rule):
        return f"1 validation error detected: {clause(value, member, rule)}"

    abort = "abortConfig.criteriaList.1.member"
    retry = "jobExecutionsRetryConfig.criteriaList.1.member"
    cases = [
        ({}, f"Update Job request for job {job_id} cannot be empty."),
        ({"timeoutConfig": {"inProgressTimeoutInMinutes": 0}},
         "Provide valid timeout value, inProgressTimeoutInMinutes cannot be 0."),
        ({"timeoutConfig": {"inProgressTimeoutInMinutes": 10081}},
         "Provide valid timeout value, inProgressTimeoutInMinutes cannot be 10081."),
        ({"timeoutConfig": {}},
         "Provide valid timeout value, inProgressTimeoutInMinutes cannot be null."),
        ({"jobExecutionsRolloutConfig": {"maximumPerMinute": 0}},
         constraint(0, "jobExecutionsRolloutConfig.maximumPerMinute",
                    "have value greater than or equal to 1")),
        ({"jobExecutionsRolloutConfig": {"maximumPerMinute": 1001}},
         constraint(1001, "jobExecutionsRolloutConfig.maximumPerMinute",
                    "have value less than or equal to 1000")),
        ({"jobExecutionsRolloutConfig": {}},
         "Provide MaximumPerMinute value or provide ExponentialRate and "
         "MaximumPerMinute"),
        ({"jobExecutionsRolloutConfig": {"exponentialRate": rate["exponentialRate"]}},
         "Provide MaximumPerMinute value when ExponentialRate is defined"),
        (with_rate(baseRatePerMinute=50),
         "Exponential rollout baseRatePerMinute should be less than maximumPerMinute."),
        (with_rate(incrementFactor=5.1),
         constraint(5.1, "jobExecutionsRolloutConfig.exponentialRate.incrementFactor",
                    "have value less than or equal to 5")),
        (with_rate(rateIncreaseCriteria={}),
         "Provide either NumberOfNotifiedThings or NumberOfSucceededThings when "
         "RateIncreaseCriteria is defined"),
        (with_rate(rateIncreaseCriteria={"numberOfNotifiedThings": 10,
                                         "numberOfSucceededThings": 5}),
         "Provide only one of NumberOfNotifiedThings or NumberOfSucceededThings when "
         "RateIncreaseCriteria is defined"),
        ({"abortConfig": {"criteriaList": [{"failureType": "BAD", "action": "CANCEL",
                                            "thresholdPercentage": 50,
                                            "minNumberOfExecutedThings": 3}]}},
         constraint("BAD", f"{abort}.failureType",
                    "satisfy enum value set: [ALL, TIMED_OUT, FAILED, REJECTED]")),
        ({"abortConfig": {"criteriaList": [{"failureType": "FAILED", "action": "CANCEL",
                                            "thresholdPercentage": 101,
                                            "minNumberOfExecutedThings": 3}]}},
         constraint(101.0, f"{abort}.thresholdPercentage",
                    "have value less than or equal to 100")),
        ({"abortConfig": {"criteriaList": []}},
         constraint("[]", "abortConfig.criteriaList",
                    "have length greater than or equal to 1")),
        ({"jobExecutionsRetryConfig": {"criteriaList": [
            {"failureType": "REJECTED", "numberOfRetries": 0}]}},
         constraint("REJECTED", f"{retry}.failureType",
                    "satisfy enum value set: [ALL, TIMED_OUT, FAILED]")),
        ({"jobExecutionsRetryConfig": {"criteriaList": [
            {"failureType": "FAILED", "numberOfRetries": 11}]}},
         constraint(11, f"{retry}.numberOfRetries", "have value less than or equal to 10")),
        ({"jobExecutionsRetryConfig": {"criteriaList": [
            {"failureType": t, "numberOfRetries": 1} for t in ("FAILED", "TIMED_OUT", "ALL")
        ]}},
         constraint("[RetryCriteria(failureType=FAILED, numberOfRetries=1), "
                    "RetryCriteria(failureType=TIMED_OUT, numberOfRetries=1), "
                    "RetryCriteria(failureType=ALL, numberOfRetries=1)]",
                    "jobExecutionsRetryConfig.criteriaList",
                    "have length less than or equal to 2")),
        ({"presignedUrlConfig": {"roleArn": role, "expiresInSec": 59}},
         constraint(59, "presignedUrlConfig.expiresInSec",
                    "have value greater than or equal to 60")),
        ({"presignedUrlConfig": {"roleArn": "short", "expiresInSec": 600}},
         "Given role short is invalid."),
        ({"description": "x" * 2029},
         constraint("x" * 2029, "description", "have length less than or equal to 2028")),
        ({"description": "a\u0001b"},
         constraint("a\u0001b", "description",
                    r"satisfy regular expression pattern: [^\p{C}]+")),
        ({"jobExecutionsRolloutConfig": {"maximumPerMinute": 0},
          "presignedUrlConfig": {"roleArn": role, "expiresInSec": 59}},
         "2 validation errors detected: Value '0' at "
         "'jobExecutionsRolloutConfig.maximumPerMinute' failed to satisfy constraint: "
         "Member must have value greater than or equal to 1; Value '59' at "
         "'presignedUrlConfig.expiresInSec' failed to satisfy constraint: Member must "
         "have value greater than or equal to 60"),
        # The same order whatever order the request sends the members in.
        ({"abortConfig": {"criteriaList": [{"minNumberOfExecutedThings": 3,
                                            "thresholdPercentage": 50,
                                            "failureType": "BAD", "action": "CANCEL"}]},
          "presignedUrlConfig": {"roleArn": role, "expiresInSec": 59},
          "jobExecutionsRetryConfig": {"criteriaList": [
              {"failureType": "FAILED", "numberOfRetries": 11}]},
          "jobExecutionsRolloutConfig": {"maximumPerMinute": 0}},
         "4 validation errors detected: " + "; ".join([
             clause(0, "jobExecutionsRolloutConfig.maximumPerMinute",
                    "have value greater than or equal to 1"),
             clause(59, "presignedUrlConfig.expiresInSec",
                    "have value greater than or equal to 60"),
             clause(11, f"{retry}.numberOfRetries", "have value less than or equal to 10"),
             clause("BAD", f"{abort}.failureType",
                    "satisfy enum value set: [ALL, TIMED_OUT, FAILED, REJECTED]"),
         ])),
        ({"abortConfig": {"criteriaList": [{"failureType": "BAD", "action": "BAD",
                                            "thresholdPercentage": 101,
                                            "minNumberOfExecutedThings": 0}]}},
         "4 validation errors detected: " + "; ".join([
             clause(0, f"{abort}.minNumberOfExecutedThings",
                    "have value greater than or equal to 1"),
             clause("BAD", f"{abort}.failureType",
                    "satisfy enum value set: [ALL, TIMED_OUT, FAILED, REJECTED]"),
             clause("BAD", f"{abort}.action", "satisfy enum value set: [CANCEL]"),
             clause(101.0, f"{abort}.thresholdPercentage",
                    "have value less than or equal to 100"),
         ])),
    ]
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=job_id, targets=[thing_arn], document=_DOCUMENT)
        before = iot_client.describe_job(jobId=job_id)["job"]
        for payload, message in cases:
            status, body = _patch_job(job_id, payload)
            assert (status, body["message"]) == (400, message), payload
        assert iot_client.describe_job(jobId=job_id)["job"] == before

        # The request checks answer before the job is looked up.
        assert _patch_job("bad.id", {"description": "x"})[1]["message"] == constraint(
            "bad.id", "jobId", "satisfy regular expression pattern: [a-zA-Z0-9_-]+")
        missing = _unique("job")
        assert _patch_job(missing, {})[1]["message"] == (
            f"Update Job request for job {missing} cannot be empty.")
        status, body = _patch_job(missing, {"description": "x"})
        assert (status, body["message"]) == (404, f"Job {missing} cannot be found.")

        # CreateJob refuses a bad timeout with the same message.
        status, body = _raw(_IOT_AUTH, "PUT", f"/jobs/{missing}", {
            "targets": [thing_arn], "document": _DOCUMENT,
            "timeoutConfig": {"inProgressTimeoutInMinutes": 0}})
        assert (status, body["message"]) == (
            400, "Provide valid timeout value, inProgressTimeoutInMinutes cannot be 0.")
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing])


def test_iot_jobs_update_job_only_while_in_progress(iot_client, iot_jobs_data):
    """A CANCELED or COMPLETED job, or one in another account, is not updated."""
    thing = _unique("jobs-thing")
    canceled, done = _unique("job"), _unique("job")
    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=canceled, targets=[thing_arn], document=_DOCUMENT)
        iot_client.cancel_job(jobId=canceled)
        iot_client.create_job(jobId=done, targets=[thing_arn], document=_DOCUMENT)
        iot_jobs_data.start_next_pending_job_execution(thingName=thing)
        iot_jobs_data.update_job_execution(thingName=thing, jobId=done, status="SUCCEEDED")
        for job_id, status in ((canceled, "CANCELED"), (done, "COMPLETED")):
            assert iot_client.describe_job(jobId=job_id)["job"]["status"] == status
            for payload in ({"description": "late"},
                            {"timeoutConfig": {"inProgressTimeoutInMinutes": 5}}):
                with pytest.raises(ClientError) as ei:
                    iot_client.update_job(jobId=job_id, **payload)
                assert _error(ei) == (
                    400, "InvalidRequestException",
                    f"Job {job_id} in status {status} cannot be updated.",
                )
        with pytest.raises(ClientError) as ei:
            _account_client("iot", "222222222222").update_job(
                jobId=done, description="x")
        assert _error(ei) == (
            404, "ResourceNotFoundException", f"Job {done} cannot be found.")
    finally:
        _cleanup(iot_client, jobs=[canceled, done], things=[thing])


def test_iot_jobs_update_job_retry_config_only_drops_to_zero(iot_client):
    thing = _unique("jobs-thing")
    plain, single, with_retry = _unique("job"), _unique("job"), _unique("job")

    def retry(*criteria):
        return {"criteriaList": [
            {"failureType": failure, "numberOfRetries": number}
            for failure, number in criteria]}

    try:
        thing_arn = _create_thing(iot_client, thing)
        iot_client.create_job(jobId=plain, targets=[thing_arn], document=_DOCUMENT)
        iot_client.create_job(jobId=single, targets=[thing_arn], document=_DOCUMENT,
                              jobExecutionsRetryConfig=retry(("FAILED", 2)))
        iot_client.create_job(
            jobId=with_retry, targets=[thing_arn], document=_DOCUMENT,
            timeoutConfig={"inProgressTimeoutInMinutes": 10},
            jobExecutionsRetryConfig=retry(("FAILED", 2), ("TIMED_OUT", 3)),
        )
        for job_id, config, message in (
            (plain, retry(("FAILED", 2)),
             "The number of retries cannot be updated to any number other than 0."),
            (plain, retry(("FAILED", 0)),
             "RetryConfig cannot be updated if the job has no RetryConfig defined "
             "during creation."),
            (with_retry, retry(("FAILED", 1)),
             "The number of retries cannot be updated to any number other than 0."),
            (single, retry(("TIMED_OUT", 0)),
             "FailureTypes must match existing FailureTypes defined in RetryConfig."),
            (single, retry(("FAILED", 0), ("TIMED_OUT", 0)),
             "FailureTypes must match existing FailureTypes defined in RetryConfig."),
            (with_retry, retry(("FAILED", 0)),
             "FailureTypes must match existing FailureTypes defined in RetryConfig."),
            (with_retry, retry(("FAILED", 0), ("ALL", 0)),
             "A retryCriteria with failure type ALL must be used by itself."),
        ):
            with pytest.raises(ClientError) as ei:
                iot_client.update_job(jobId=job_id, jobExecutionsRetryConfig=config)
            assert _error(ei) == (400, "InvalidRequestException", message), config

        # The same failure types in any order; stored as sent.
        both = retry(("TIMED_OUT", 0), ("FAILED", 0))
        iot_client.update_job(jobId=with_retry, jobExecutionsRetryConfig=both)
        job = iot_client.describe_job(jobId=with_retry)["job"]
        assert job["jobExecutionsRetryConfig"] == both
    finally:
        _cleanup(iot_client, jobs=[plain, single, with_retry], things=[thing])


def test_iot_jobs_update_job_timeout_reaches_executions_that_start_later(
    iot_client, iot_jobs_data
):
    """A new timeout reaches later starts; a running execution keeps its own."""
    things = [_unique("jobs-thing") for _ in range(2)]
    job_id = _unique("job")

    def seconds_left(thing):
        execution = iot_jobs_data.describe_job_execution(
            thingName=thing, jobId=job_id)["execution"]
        return execution.get("approximateSecondsBeforeTimedOut")

    try:
        arns = [_create_thing(iot_client, thing) for thing in things]
        iot_client.create_job(jobId=job_id, targets=arns, document=_DOCUMENT,
                              timeoutConfig={"inProgressTimeoutInMinutes": 60})
        iot_jobs_data.start_next_pending_job_execution(thingName=things[0])
        iot_client.update_job(jobId=job_id,
                              timeoutConfig={"inProgressTimeoutInMinutes": 120})
        iot_jobs_data.start_next_pending_job_execution(thingName=things[1])
        assert 3500 < seconds_left(things[0]) <= 3600
        assert 7100 < seconds_left(things[1]) <= 7200
        iot_client.update_job(jobId=job_id,
                              timeoutConfig={"inProgressTimeoutInMinutes": 30})
        assert 3500 < seconds_left(things[0]) <= 3600
        assert 7100 < seconds_left(things[1]) <= 7200
    finally:
        _cleanup(iot_client, jobs=[job_id], things=things)


def test_iot_jobs_update_job_timeout_on_a_continuous_job(iot_client, iot_jobs_data):
    """A running execution stays without a timeout; a later joiner gets the new one."""
    group = _unique("jobs-group")
    early, late = _unique("jobs-thing"), _unique("jobs-thing")
    job_id = _unique("job")

    def seconds_left(thing):
        execution = iot_jobs_data.describe_job_execution(
            thingName=thing, jobId=job_id)["execution"]
        return execution.get("approximateSecondsBeforeTimedOut")

    try:
        group_arn = iot_client.create_thing_group(thingGroupName=group)["thingGroupArn"]
        for thing in (early, late):
            _create_thing(iot_client, thing)
        iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=early)
        iot_client.create_job(jobId=job_id, targets=[group_arn], document=_DOCUMENT,
                              targetSelection="CONTINUOUS")
        iot_jobs_data.start_next_pending_job_execution(thingName=early)
        iot_client.update_job(jobId=job_id,
                              timeoutConfig={"inProgressTimeoutInMinutes": 20})
        assert seconds_left(early) is None
        iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=late)
        iot_jobs_data.start_next_pending_job_execution(thingName=late)
        assert 1100 < seconds_left(late) <= 1200
        iot_client.update_job(jobId=job_id,
                              timeoutConfig={"inProgressTimeoutInMinutes": 40})
        assert seconds_left(early) is None
        assert 1100 < seconds_left(late) <= 1200
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[early, late], groups=[group])


def test_iot_jobs_update_job_survives_persistence():
    """Updated members and a kept timeout survive get_state / load_persisted_state."""
    from ministack.core.responses import request_scope
    from ministack.services import iot as iot_module

    iot_module.reset()
    try:
        with request_scope("123456789012", "us-east-1"):
            now = iot_module._jobs_now_ms()
            iot_module._jobs["j1"] = {
                "jobId": "j1", "targets": [], "targetSelection": "SNAPSHOT",
                "status": "IN_PROGRESS", "document": "{}", "snapshotted": True,
                "timeoutConfig": {"inProgressTimeoutInMinutes": 1},
            }
            iot_module._job_executions[("t1", "j1")] = {
                "jobId": "j1", "thingName": "t1", "status": "IN_PROGRESS",
                "statusDetails": {}, "queuedAt": now, "startedAt": now,
                "lastUpdatedAt": now, "executionNumber": 1, "versionNumber": 2,
            }
            status, _, body = iot_module._update_job("j1", {
                "description": "d", "timeoutConfig": {"inProgressTimeoutInMinutes": 9}})
            assert (status, body) == (200, b"")
            state = iot_module.get_state()
            iot_module.reset()
            iot_module.load_persisted_state(state)
            job = iot_module._jobs["j1"]
            assert job["description"] == "d"
            assert job["timeoutConfig"] == {"inProgressTimeoutInMinutes": 9}
            execution = iot_module._job_executions[("t1", "j1")]
            assert iot_module._jobs_seconds_before_timeout(execution) == 60
    finally:
        iot_module.reset()


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
import asyncio  # noqa: E402

from conftest import patch_endpoint_dns  # noqa: E402
from test_iot_data import (  # noqa: E402
    _WS_HANDSHAKE_TIMEOUT,
    _collect_shadow_frames,
    _make_publish,
    _mqtt_connect,
    _mqtt_disconnect,
)

_DOCUMENT_OBJECT = json.loads(_DOCUMENT)


def _mqtt_publish(topic, payload):
    """Publish at QoS 1 over the MQTT-over-WebSocket broker and wait for the
    PUBACK. The jobs topics are MQTT-only: AWS refuses them over HTTPS."""

    async def _run():
        ws = await _mqtt_connect(_unique("jobs-device"))
        try:
            await ws.send(_make_publish(topic, payload, qos=1, packet_id=1))
            await asyncio.wait_for(ws.recv(), timeout=_WS_HANDSHAKE_TIMEOUT)
        finally:
            await _mqtt_disconnect(ws)

    with patch_endpoint_dns():
        asyncio.run(_run())


def _frames_by_topic(received):
    return {topic: json.loads(payload) for topic, payload in received}


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
        lambda: _mqtt_publish(
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
        lambda: _mqtt_publish(
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
        lambda: _mqtt_publish(topic=f"{base}/start-next", payload=b"{}"),
        want=1,
    )

    received = _collect_shadow_frames(
        f"{base}/{job_id}/update/accepted",
        lambda: _mqtt_publish(
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
        lambda: _mqtt_publish(
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


def test_jobs_mqtt_leaving_continuous_job_group_notifies(iot_client, iot_data_client):
    """A thing leaving a CONTINUOUS job's target group loses its QUEUED
    execution (REMOVED): its pending set and front changed, so notify carries
    the emptied aggregate and notify-next goes bare."""
    group = _unique("jobs-mqtt-group")
    thing = _unique("jobs-mqtt-leave")
    job_id = _unique("job-cont")
    base = f"$aws/things/{thing}/jobs"
    try:
        group_arn = iot_client.create_thing_group(thingGroupName=group)[
            "thingGroupArn"
        ]
        iot_client.create_job(
            jobId=job_id, targets=[group_arn], document=_DOCUMENT,
            targetSelection="CONTINUOUS",
        )
        _create_thing(iot_client, thing)
        iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=thing)

        received = _collect_shadow_frames(
            f"{base}/#",
            lambda: iot_client.remove_thing_from_thing_group(
                thingGroupName=group, thingName=thing
            ),
            want=2,
        )
        frames = _frames_by_topic(received)
        assert frames[f"{base}/notify"]["jobs"] == {}
        assert set(frames[f"{base}/notify-next"]) == {"timestamp"}
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing], groups=[group])


def test_jobs_mqtt_rejoining_continuous_job_group_notifies(
    iot_client, iot_data_client, iot_jobs_data
):
    """A thing that finished a CONTINUOUS job and rejoins its target group
    gets execution 2 QUEUED: notify lists it and notify-next carries it."""
    group = _unique("jobs-mqtt-group")
    thing = _unique("jobs-mqtt-rejoin")
    job_id = _unique("job-cont")
    base = f"$aws/things/{thing}/jobs"
    try:
        group_arn = iot_client.create_thing_group(thingGroupName=group)[
            "thingGroupArn"
        ]
        iot_client.create_job(
            jobId=job_id, targets=[group_arn], document=_DOCUMENT,
            targetSelection="CONTINUOUS",
        )
        _create_thing(iot_client, thing)
        iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=thing)
        _drive(iot_jobs_data, thing, job_id, "SUCCEEDED")
        iot_client.remove_thing_from_thing_group(thingGroupName=group, thingName=thing)

        received = _collect_shadow_frames(
            f"{base}/#",
            lambda: iot_client.add_thing_to_thing_group(
                thingGroupName=group, thingName=thing
            ),
            want=2,
        )
        frames = _frames_by_topic(received)
        queued = frames[f"{base}/notify"]["jobs"]["QUEUED"]
        assert [(q["jobId"], q["executionNumber"]) for q in queued] == [(job_id, 2)]
        execution = frames[f"{base}/notify-next"]["execution"]
        assert (execution["executionNumber"], execution["status"]) == (2, "QUEUED")
        assert execution["jobDocument"] == _DOCUMENT_OBJECT
        # The members AWS sends (measured eu-central-1 2026-10-05): no
        # thingName, and no statusDetails while it is empty.
        assert set(execution) == {
            "jobId", "status", "queuedAt", "lastUpdatedAt", "versionNumber",
            "executionNumber", "jobDocument",
        }
    finally:
        _cleanup(iot_client, jobs=[job_id], things=[thing], groups=[group])


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
        lambda: _mqtt_publish(
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
        lambda: _mqtt_publish(
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
        lambda: _mqtt_publish(
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
        lambda: _mqtt_publish(topic=f"{base}/get", payload=b"{}"),
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
        lambda: _mqtt_publish(topic=f"{base}/get", payload=b"[1, 2]"),
        want=1,
    )
    doc = _frames_by_topic(received)[f"{base}/get/rejected"]
    assert doc["code"] == "InvalidJson"
    _assert_epoch_seconds(doc["timestamp"])


def _qa_publish(iot_data_client, topic, payload):
    _mqtt_publish(topic=topic, payload=payload)


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
