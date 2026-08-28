"""IoT Jobs over MQTT: the reserved ``$aws/things/<t>/jobs/#`` bridge.

Request/response round-trips ride the same WS collector the shadow-bridge
tests use; the notify/notify-next state-change publishes are asserted around
control-plane CreateJob and data-plane terminal updates. The contract under
test is the one captured against live AWS: every timestamp — envelope and
execution fields alike — is whole epoch seconds, ``jobDocument`` is a JSON
object over MQTT, rejections carry the string ErrorCode enum, ``notify``
fires only when the pending set's membership changes, and ``notify-next``
only when the job at the front of the queue changes.
"""

import json
import time

from test_iot_data import _collect_shadow_frames
from test_iot_jobs import _DOCUMENT, _create_thing, _unique

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
