"""
Managed Service for Apache Flink data plane: real jobs in the official Flink
image, driven through the kinesisanalyticsv2 API.

The test job (tests/fixtures/flink) is compiled at setup against the Flink
classes in the runtime image and aws-kinesisanalytics-runtime, neither of them
bundled, so no binary is committed and the jar is built the way AWS documents.
It reads the "Test" runtime property group through KinesisAnalyticsRuntime and
writes a checkpointed counter to a MiniStack Kinesis stream, which covers the
runtime library, the property file, the endpoint injection, and restore from
a savepoint.
"""

import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

import boto3
import pytest
from botocore.config import Config
from conftest import ENDPOINT, REGION

from ministack.services import kinesisanalyticsv2_flink as dataplane

RUNTIME = "FLINK-1_20"
ROLE = "arn:aws:iam::000000000000:role/flink-app"
SOURCE_DIR = Path(__file__).parent / "fixtures" / "flink" / "src"
MAIN_CLASS = "ministack.flinktest.CounterJob"

requires_docker = pytest.mark.skipif(
    not os.environ.get("DOCKER_NETWORK"), reason="DOCKER_NETWORK not set -- skipping Flink data-plane tests"
)


def _wait_status(client, name, wanted, timeout=240):
    deadline = time.monotonic() + timeout
    status = None
    while time.monotonic() < deadline:
        status = client.describe_application(ApplicationName=name)["ApplicationDetail"]["ApplicationStatus"]
        if status == wanted:
            return
        time.sleep(1)
    raise AssertionError(f"{name} is {status}, expected {wanted} within {timeout}s")


def _wait_snapshot(client, name, snapshot, wanted="READY", timeout=180):
    deadline = time.monotonic() + timeout
    status = None
    while time.monotonic() < deadline:
        status = client.describe_application_snapshot(ApplicationName=name, SnapshotName=snapshot)[
            "SnapshotDetails"]["SnapshotStatus"]
        if status == wanted:
            return
        time.sleep(1)
    raise AssertionError(f"snapshot {snapshot} of {name} is {status}, expected {wanted} within {timeout}s")


def _containers(name):
    import docker

    client = docker.from_env()
    filters = {"label": ["ministack=kinesisanalyticsv2", f"application={name}"]}
    return client.containers.list(all=True, filters=filters)


class _Stream:
    """Reads the counter records the job writes, in order."""

    def __init__(self, kin, name):
        self.kin = kin
        self.name = name
        self.iterator = kin.get_shard_iterator(
            StreamName=name, ShardId="shardId-000000000000", ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]

    def read(self):
        records = []
        while True:
            resp = self.kin.get_records(ShardIterator=self.iterator)
            self.iterator = resp["NextShardIterator"]
            records += [json.loads(r["Data"]) for r in resp["Records"]]
            if not resp["Records"]:
                return records

    def wait_for(self, count, timeout=60):
        records = []
        deadline = time.monotonic() + timeout
        while len(records) < count and time.monotonic() < deadline:
            records += self.read()
            time.sleep(0.5)
        assert len(records) >= count, f"expected {count} records from the job, got {len(records)}"
        return records


@pytest.fixture(scope="module")
def counter_jar():
    """Compile the test job against the Flink classes of the runtime image."""
    if shutil.which("javac") is None or shutil.which("jar") is None:
        pytest.skip("a JDK (javac and jar) is needed to build the Flink test job")
    libraries = sorted(Path(dataplane._FLINK_LIB_DIR).glob("aws-kinesisanalytics-runtime-*.jar"))
    if not libraries:
        pytest.skip(f"aws-kinesisanalytics-runtime is not in {dataplane._FLINK_LIB_DIR}; the test job needs it")
    import docker

    client = docker.from_env()
    image = dataplane._image_for({"RuntimeEnvironment": RUNTIME})
    try:
        client.images.pull(image)
    except Exception as e:
        try:
            client.images.get(image)
        except Exception:
            pytest.skip(f"could not pull {image}: {e}")
    work = Path(tempfile.mkdtemp(prefix="flink-test-job-"))
    container = client.containers.create(image)
    try:
        stream, _ = container.get_archive("/opt/flink/lib")
        with tarfile.open(fileobj=io.BytesIO(b"".join(stream))) as tar:
            tar.extractall(work, filter="data")
        # Compiled against the runtime library but not bundled: MiniStack, like
        # Managed Flink, provides it on the cluster classpath.
        sources = [str(p) for p in SOURCE_DIR.rglob("*.java")]
        subprocess.run(
            ["javac", "--release", "11", "-nowarn", "-cp", os.pathsep.join([f"{work / 'lib'}{os.sep}*", *map(str, libraries)]), "-d", str(work / "classes"),
             *sources],
            check=True, capture_output=True,
        )
        jar_path = work / "counter-job.jar"
        subprocess.run(
            ["jar", "--create", "--file", str(jar_path), "--main-class", MAIN_CLASS, "-C", str(work / "classes"), "."],
            check=True, capture_output=True,
        )
        yield jar_path.read_bytes()
    finally:
        container.remove(force=True)
        shutil.rmtree(work, ignore_errors=True)


def _create_app(kinesisanalyticsv2, s3, kin, jar, checkpoint=None, name=None):
    """An application for the counter job, writing to a fresh Kinesis stream."""
    suffix = uuid.uuid4().hex[:8]
    bucket, stream = f"flink-jars-{suffix}", f"flink-out-{suffix}"
    name = name or f"flink-dp-{suffix}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="counter-job.jar", Body=jar)
    kin.create_stream(StreamName=stream, ShardCount=1)
    kinesisanalyticsv2.create_application(
        ApplicationName=name,
        RuntimeEnvironment=RUNTIME,
        ServiceExecutionRole=ROLE,
        ApplicationConfiguration={
            "ApplicationCodeConfiguration": {
                "CodeContentType": "ZIPFILE",
                "CodeContent": {
                    "S3ContentLocation": {"BucketARN": f"arn:aws:s3:::{bucket}", "FileKey": "counter-job.jar"}
                },
            },
            "EnvironmentProperties": {
                "PropertyGroups": [
                    {"PropertyGroupId": "Test", "PropertyMap": {"StreamName": stream, "Message": f"hello-{suffix}"}}
                ]
            },
            "FlinkApplicationConfiguration": {
                "ParallelismConfiguration": {"ConfigurationType": "CUSTOM", "Parallelism": 2,
                                             "ParallelismPerKPU": 1, "AutoScalingEnabled": False},
                **({"CheckpointConfiguration": checkpoint} if checkpoint else {}),
            },
            "ApplicationSnapshotConfiguration": {"SnapshotsEnabled": True},
        },
    )
    return {"name": name, "stream": stream, "message": f"hello-{suffix}"}


def _cleanup(kinesisanalyticsv2, name):
    try:
        status = kinesisanalyticsv2.describe_application(ApplicationName=name)["ApplicationDetail"]["ApplicationStatus"]
        if status in ("RUNNING", "STARTING"):
            kinesisanalyticsv2.stop_application(ApplicationName=name, Force=True)
            _wait_status(kinesisanalyticsv2, name, "READY", timeout=60)
        created = kinesisanalyticsv2.describe_application(ApplicationName=name)["ApplicationDetail"]["CreateTimestamp"]
        kinesisanalyticsv2.delete_application(ApplicationName=name, CreateTimestamp=created)
    except Exception:
        pass


@pytest.fixture
def flink_app(kinesisanalyticsv2, s3, kin, counter_jar):
    app = _create_app(kinesisanalyticsv2, s3, kin, counter_jar)
    yield app
    _cleanup(kinesisanalyticsv2, app["name"])


def _start_and_wait(kinesisanalyticsv2, name):
    kinesisanalyticsv2.start_application(
        ApplicationName=name,
        RunConfiguration={"ApplicationRestoreConfiguration": {"ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"}},
    )
    _wait_status(kinesisanalyticsv2, name, "RUNNING")


def _jobmanager_rest(name):
    """The Flink REST API of the application's JobManager, from the host."""
    [jm] = [c for c in _containers(name) if c.labels.get("role") == "jobmanager"]
    jm.reload()
    port = jm.attrs["NetworkSettings"]["Ports"]["8081/tcp"][0]["HostPort"]
    base = f"http://127.0.0.1:{port}"

    def get(path):
        with urllib.request.urlopen(base + path, timeout=5) as resp:
            return json.loads(resp.read())

    return get


@requires_docker
@pytest.mark.data_plane
def test_job_runs_with_runtime_properties_and_writes_to_kinesis(kinesisanalyticsv2, kin, flink_app):
    name = flink_app["name"]
    stream = _Stream(kin, flink_app["stream"])
    kinesisanalyticsv2.start_application(
        ApplicationName=name,
        RunConfiguration={"ApplicationRestoreConfiguration": {"ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"}},
    )
    _wait_status(kinesisanalyticsv2, name, "RUNNING")
    assert {c.labels.get("role") for c in _containers(name)} == {"jobmanager", "taskmanager"}

    records = stream.wait_for(3)
    assert records[0] == {"message": flink_app["message"], "n": 0}

    kinesisanalyticsv2.stop_application(ApplicationName=name, Force=True)
    _wait_status(kinesisanalyticsv2, name, "READY", timeout=60)
    assert _containers(name) == []


@requires_docker
@pytest.mark.data_plane
def test_restore_from_latest_snapshot_resumes_the_job(kinesisanalyticsv2, kin, flink_app):
    name = flink_app["name"]
    stream = _Stream(kin, flink_app["stream"])
    kinesisanalyticsv2.start_application(
        ApplicationName=name,
        RunConfiguration={"ApplicationRestoreConfiguration": {"ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"}},
    )
    _wait_status(kinesisanalyticsv2, name, "RUNNING")
    before = stream.wait_for(4)

    kinesisanalyticsv2.create_application_snapshot(ApplicationName=name, SnapshotName="mid-run")
    _wait_snapshot(kinesisanalyticsv2, name, "mid-run")

    # A graceful stop with snapshots enabled takes the latest snapshot.
    kinesisanalyticsv2.stop_application(ApplicationName=name)
    _wait_status(kinesisanalyticsv2, name, "READY", timeout=180)
    last_before = max(r["n"] for r in before + stream.read())

    kinesisanalyticsv2.start_application(
        ApplicationName=name,
        RunConfiguration={"ApplicationRestoreConfiguration": {"ApplicationRestoreType": "RESTORE_FROM_LATEST_SNAPSHOT"}},
    )
    _wait_status(kinesisanalyticsv2, name, "RUNNING")
    resumed = stream.wait_for(1)
    assert resumed[0]["n"] > 0, "the job started from scratch instead of the snapshot"
    assert resumed[0]["n"] >= last_before


@requires_docker
@pytest.mark.data_plane
def test_start_with_missing_code_returns_to_ready(kinesisanalyticsv2, s3, flink_app):
    name = flink_app["name"]
    detail = kinesisanalyticsv2.describe_application(ApplicationName=name)["ApplicationDetail"]
    loc = detail["ApplicationConfigurationDescription"]["ApplicationCodeConfigurationDescription"][
        "CodeContentDescription"]["S3ApplicationCodeLocationDescription"]
    s3.delete_object(Bucket=loc["BucketARN"].split(":::")[-1], Key=loc["FileKey"])

    kinesisanalyticsv2.start_application(ApplicationName=name)
    _wait_status(kinesisanalyticsv2, name, "READY", timeout=60)
    assert _containers(name) == []


@requires_docker
@pytest.mark.data_plane
def test_job_writes_into_the_applications_own_account(counter_jar):
    """A job runs with the application's account, so its writes land there."""
    kwargs = dict(endpoint_url=ENDPOINT, aws_access_key_id="111111111111", aws_secret_access_key="test",
                  region_name=REGION, config=Config(retries={"mode": "standard"}))
    other_kda, other_s3, other_kin = (boto3.client(svc, **kwargs) for svc in ("kinesisanalyticsv2", "s3", "kinesis"))
    app = _create_app(other_kda, other_s3, other_kin, counter_jar)
    try:
        assert other_kda.describe_application(ApplicationName=app["name"])["ApplicationDetail"][
            "ApplicationARN"].split(":")[4] == "111111111111"
        stream = _Stream(other_kin, app["stream"])
        _start_and_wait(other_kda, app["name"])
        assert stream.wait_for(2)[0]["message"] == app["message"]
    finally:
        _cleanup(other_kda, app["name"])


@requires_docker
@pytest.mark.data_plane
def test_checkpoint_configuration_reaches_the_cluster(kinesisanalyticsv2, s3, kin, counter_jar):
    app = _create_app(kinesisanalyticsv2, s3, kin, counter_jar, checkpoint={
        "ConfigurationType": "CUSTOM", "CheckpointingEnabled": True,
        "CheckpointInterval": 2000, "MinPauseBetweenCheckpoints": 500})
    try:
        _start_and_wait(kinesisanalyticsv2, app["name"])
        rest = _jobmanager_rest(app["name"])
        [job] = rest("/jobs")["jobs"]
        assert rest(f"/jobs/{job['id']}/checkpoints/config")["interval"] == 2000
        deadline = time.monotonic() + 30
        while rest(f"/jobs/{job['id']}/checkpoints")["counts"]["completed"] < 1:
            assert time.monotonic() < deadline, "no checkpoint completed within 30s"
            time.sleep(1)
    finally:
        _cleanup(kinesisanalyticsv2, app["name"])


@requires_docker
@pytest.mark.data_plane
def test_runtime_without_an_image_runs_no_containers(kinesisanalyticsv2, s3, kin, counter_jar):
    app = _create_app(kinesisanalyticsv2, s3, kin, counter_jar)
    name = app["name"]
    try:
        created = kinesisanalyticsv2.describe_application(ApplicationName=name)["ApplicationDetail"]
        kinesisanalyticsv2.update_application(
            ApplicationName=name, CurrentApplicationVersionId=created["ApplicationVersionId"],
            RuntimeEnvironmentUpdate="FLINK-1_18")
        kinesisanalyticsv2.start_application(ApplicationName=name)
        _wait_status(kinesisanalyticsv2, name, "RUNNING", timeout=10)
        assert _containers(name) == []
        kinesisanalyticsv2.stop_application(ApplicationName=name)
        _wait_status(kinesisanalyticsv2, name, "READY", timeout=10)
        [snap] = kinesisanalyticsv2.list_application_snapshots(ApplicationName=name)["SnapshotSummaries"]
        assert snap["SnapshotStatus"] == "READY"
    finally:
        _cleanup(kinesisanalyticsv2, name)


@requires_docker
@pytest.mark.data_plane
def test_long_application_name_with_dots_and_underscores_runs(kinesisanalyticsv2, s3, kin, counter_jar):
    """Application names allow 128 characters including '.' and '_'; the
    JobManager's Docker name must still be a valid DNS label."""
    name = f"Long.App_Name-{uuid.uuid4().hex[:8]}-" + "x" * 105
    assert len(name) == 128
    app = _create_app(kinesisanalyticsv2, s3, kin, counter_jar, name=name)
    try:
        stream = _Stream(kin, app["stream"])
        _start_and_wait(kinesisanalyticsv2, name)
        assert stream.wait_for(1)[0]["message"] == app["message"]
    finally:
        _cleanup(kinesisanalyticsv2, name)


@requires_docker
@pytest.mark.data_plane
def test_force_stop_while_starting_then_start_again(kinesisanalyticsv2, kin, flink_app):
    """A start cancelled mid-launch must not leave containers that block the next start."""
    name = flink_app["name"]
    stream = _Stream(kin, flink_app["stream"])
    kinesisanalyticsv2.start_application(ApplicationName=name)
    kinesisanalyticsv2.stop_application(ApplicationName=name, Force=True)
    _wait_status(kinesisanalyticsv2, name, "READY", timeout=60)

    _start_and_wait(kinesisanalyticsv2, name)
    assert stream.wait_for(1)
    assert {c.labels.get("role") for c in _containers(name)} == {"jobmanager", "taskmanager"}
