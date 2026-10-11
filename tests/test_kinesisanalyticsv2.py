"""
Managed Service for Apache Flink (kinesisanalyticsv2) control plane.

Without the Docker data plane, a transitional status (STARTING, STOPPING,
UPDATING) settles on the next read, so each test sees it once and then the
settled status.
"""

import uuid

import pytest
from botocore.exceptions import ClientError

from ministack.core.responses import set_request_account_id, set_request_region
from ministack.services import kinesisanalyticsv2 as kda

RUNTIME = "FLINK-1_20"
ROLE = "arn:aws:iam::000000000000:role/flink-app"
CODE = {
    "ApplicationCodeConfiguration": {
        "CodeContentType": "ZIPFILE",
        "CodeContent": {"S3ContentLocation": {"BucketARN": "arn:aws:s3:::jars", "FileKey": "app.jar"}},
    },
}


def _name(prefix="app"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create(client, name=None, **config):
    name = name or _name()
    client.create_application(
        ApplicationName=name, RuntimeEnvironment=RUNTIME, ServiceExecutionRole=ROLE,
        ApplicationConfiguration={**CODE, **config},
    )
    return name


def _detail(client, name):
    return client.describe_application(ApplicationName=name)["ApplicationDetail"]


def _code(exc):
    return exc.value.response["Error"]["Code"]


# ---------------------------------------------------------------------------
# Create / describe / delete
# ---------------------------------------------------------------------------

def test_create_and_describe_application(kinesisanalyticsv2):
    name = _name()
    created = kinesisanalyticsv2.create_application(
        ApplicationName=name, RuntimeEnvironment=RUNTIME, ServiceExecutionRole=ROLE,
        ApplicationDescription="orders job",
        ApplicationConfiguration={
            **CODE,
            "EnvironmentProperties": {"PropertyGroups": [
                {"PropertyGroupId": "app", "PropertyMap": {"stream": "orders"}}]},
            "ApplicationSnapshotConfiguration": {"SnapshotsEnabled": True},
        },
    )["ApplicationDetail"]
    assert created["ApplicationARN"].endswith(f":application/{name}")
    assert created["ApplicationARN"].startswith("arn:aws:kinesisanalytics:us-east-1:")
    assert created["ApplicationStatus"] == "READY"
    assert created["ApplicationVersionId"] == 1
    assert created["ApplicationMode"] == "STREAMING"
    assert created["ConditionalToken"]

    detail = _detail(kinesisanalyticsv2, name)
    config = detail["ApplicationConfigurationDescription"]
    assert config["ApplicationCodeConfigurationDescription"] == {
        "CodeContentType": "ZIPFILE",
        "CodeContentDescription": {"S3ApplicationCodeLocationDescription": {
            "BucketARN": "arn:aws:s3:::jars", "FileKey": "app.jar"}},
    }
    assert config["EnvironmentPropertyDescriptions"]["PropertyGroupDescriptions"] == [
        {"PropertyGroupId": "app", "PropertyMap": {"stream": "orders"}}]
    assert config["ApplicationSnapshotConfigurationDescription"] == {"SnapshotsEnabled": True}
    flink = config["FlinkApplicationConfigurationDescription"]
    assert flink["CheckpointConfigurationDescription"] == {
        "ConfigurationType": "DEFAULT", "CheckpointingEnabled": True,
        "CheckpointInterval": 60000, "MinPauseBetweenCheckpoints": 5000}
    assert flink["ParallelismConfigurationDescription"]["Parallelism"] == 1
    assert flink["MonitoringConfigurationDescription"]["LogLevel"] == "INFO"


def test_custom_parallelism_is_reported(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2, FlinkApplicationConfiguration={
        "ParallelismConfiguration": {"ConfigurationType": "CUSTOM", "Parallelism": 4,
                                     "ParallelismPerKPU": 2, "AutoScalingEnabled": False}})
    parallelism = (_detail(kinesisanalyticsv2, name)["ApplicationConfigurationDescription"]
                   ["FlinkApplicationConfigurationDescription"]["ParallelismConfigurationDescription"])
    assert parallelism == {"ConfigurationType": "CUSTOM", "Parallelism": 4, "ParallelismPerKPU": 2,
                           "AutoScalingEnabled": False, "CurrentParallelism": 4}


def test_create_rejects_duplicates_and_non_flink_runtimes(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2)
    with pytest.raises(ClientError) as exc:
        _create(kinesisanalyticsv2, name=name)
    assert _code(exc) == "ResourceInUseException"
    with pytest.raises(ClientError) as exc:
        kinesisanalyticsv2.create_application(
            ApplicationName=_name(), RuntimeEnvironment="SQL-1_0", ServiceExecutionRole=ROLE)
    assert _code(exc) == "InvalidArgumentException"


def test_describe_unknown_application(kinesisanalyticsv2):
    with pytest.raises(ClientError) as exc:
        _detail(kinesisanalyticsv2, _name("missing"))
    assert _code(exc) == "ResourceNotFoundException"


def test_delete_requires_the_create_timestamp(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2)
    created = _detail(kinesisanalyticsv2, name)["CreateTimestamp"]
    with pytest.raises(ClientError) as exc:
        kinesisanalyticsv2.delete_application(ApplicationName=name, CreateTimestamp=0)
    assert _code(exc) == "ConcurrentModificationException"  # observed on AWS
    kinesisanalyticsv2.delete_application(ApplicationName=name, CreateTimestamp=created)
    with pytest.raises(ClientError) as exc:
        _detail(kinesisanalyticsv2, name)
    assert _code(exc) == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Start / stop / update
# ---------------------------------------------------------------------------

def test_start_and_stop_move_through_statuses(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2)
    assert kinesisanalyticsv2.start_application(
        ApplicationName=name,
        RunConfiguration={"ApplicationRestoreConfiguration": {
            "ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"}},
    )["OperationId"]
    with pytest.raises(ClientError) as exc:
        kinesisanalyticsv2.start_application(ApplicationName=name)
    assert _code(exc) == "ResourceInUseException"
    detail = _detail(kinesisanalyticsv2, name)
    assert detail["ApplicationStatus"] == "RUNNING"
    assert (detail["ApplicationConfigurationDescription"]["RunConfigurationDescription"]
            ["ApplicationRestoreConfigurationDescription"]["ApplicationRestoreType"]
            == "SKIP_RESTORE_FROM_SNAPSHOT")

    kinesisanalyticsv2.stop_application(ApplicationName=name)
    assert _detail(kinesisanalyticsv2, name)["ApplicationStatus"] == "READY"
    with pytest.raises(ClientError) as exc:
        kinesisanalyticsv2.stop_application(ApplicationName=name)
    assert _code(exc) == "ResourceInUseException"


def test_force_stop(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2, ApplicationSnapshotConfiguration={"SnapshotsEnabled": True})
    kinesisanalyticsv2.start_application(ApplicationName=name)
    assert _detail(kinesisanalyticsv2, name)["ApplicationStatus"] == "RUNNING"
    kinesisanalyticsv2.stop_application(ApplicationName=name, Force=True)
    assert _detail(kinesisanalyticsv2, name)["ApplicationStatus"] == "READY"
    # StopApplication with Force "stops the application without taking a snapshot".
    assert kinesisanalyticsv2.list_application_snapshots(ApplicationName=name)["SnapshotSummaries"] == []


def test_update_bumps_version_and_replaces_properties(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2, EnvironmentProperties={"PropertyGroups": [
        {"PropertyGroupId": "app", "PropertyMap": {"a": "1"}},
        {"PropertyGroupId": "old", "PropertyMap": {"b": "2"}}]})
    before = _detail(kinesisanalyticsv2, name)
    updated = kinesisanalyticsv2.update_application(
        ApplicationName=name, CurrentApplicationVersionId=1,
        ApplicationConfigurationUpdate={
            "EnvironmentPropertyUpdates": {"PropertyGroups": [
                {"PropertyGroupId": "app", "PropertyMap": {"a": "9"}}]},
            "FlinkApplicationConfigurationUpdate": {"ParallelismConfigurationUpdate": {
                "ConfigurationTypeUpdate": "CUSTOM", "ParallelismUpdate": 3}},
            "ApplicationCodeConfigurationUpdate": {"CodeContentUpdate": {
                "S3ContentLocationUpdate": {"FileKeyUpdate": "app-v2.jar"}}},
        },
    )
    detail = updated["ApplicationDetail"]
    assert updated["OperationId"]
    assert detail["ApplicationVersionId"] == 2
    assert detail["ConditionalToken"] != before["ConditionalToken"]
    config = detail["ApplicationConfigurationDescription"]
    # EnvironmentPropertyUpdates replaces every property group.
    assert config["EnvironmentPropertyDescriptions"]["PropertyGroupDescriptions"] == [
        {"PropertyGroupId": "app", "PropertyMap": {"a": "9"}}]
    assert (config["FlinkApplicationConfigurationDescription"]["ParallelismConfigurationDescription"]
            ["Parallelism"]) == 3
    assert (config["ApplicationCodeConfigurationDescription"]["CodeContentDescription"]
            ["S3ApplicationCodeLocationDescription"]) == {"BucketARN": "arn:aws:s3:::jars",
                                                          "FileKey": "app-v2.jar"}


def test_update_rejects_stale_version_and_token(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2)
    token = _detail(kinesisanalyticsv2, name)["ConditionalToken"]
    kinesisanalyticsv2.update_application(ApplicationName=name, ConditionalToken=token,
                                          ServiceExecutionRoleUpdate=ROLE + "-2")
    for kwargs in ({"CurrentApplicationVersionId": 1}, {"ConditionalToken": token}):
        with pytest.raises(ClientError) as exc:
            kinesisanalyticsv2.update_application(ApplicationName=name,
                                                  ServiceExecutionRoleUpdate=ROLE, **kwargs)
        assert _code(exc) == "ConcurrentModificationException"


def test_update_of_running_application_passes_through_updating(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2)
    kinesisanalyticsv2.start_application(ApplicationName=name)
    assert _detail(kinesisanalyticsv2, name)["ApplicationStatus"] == "RUNNING"
    updated = kinesisanalyticsv2.update_application(ApplicationName=name, ServiceExecutionRoleUpdate=ROLE)
    assert updated["ApplicationDetail"]["ApplicationStatus"] == "UPDATING"
    assert _detail(kinesisanalyticsv2, name)["ApplicationStatus"] == "RUNNING"


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def test_snapshot_lifecycle(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2, ApplicationSnapshotConfiguration={"SnapshotsEnabled": True})
    with pytest.raises(ClientError) as exc:
        kinesisanalyticsv2.create_application_snapshot(ApplicationName=name, SnapshotName="s1")
    assert _code(exc) == "InvalidRequestException"  # not running yet (observed on AWS)

    kinesisanalyticsv2.start_application(ApplicationName=name)
    _detail(kinesisanalyticsv2, name)
    kinesisanalyticsv2.create_application_snapshot(ApplicationName=name, SnapshotName="s1")
    with pytest.raises(ClientError) as exc:
        kinesisanalyticsv2.create_application_snapshot(ApplicationName=name, SnapshotName="s1")
    assert _code(exc) == "ResourceInUseException"

    snap = kinesisanalyticsv2.describe_application_snapshot(
        ApplicationName=name, SnapshotName="s1")["SnapshotDetails"]
    assert snap["SnapshotStatus"] == "READY"
    assert snap["ApplicationVersionId"] == 1
    assert snap["RuntimeEnvironment"] == RUNTIME
    assert snap["ApplicationEncryptionConfigurationDescription"] == {"KeyType": "AWS_OWNED_KEY"}

    # A graceful stop with snapshots enabled takes another snapshot.
    kinesisanalyticsv2.stop_application(ApplicationName=name)
    summaries = kinesisanalyticsv2.list_application_snapshots(ApplicationName=name)["SnapshotSummaries"]
    assert len(summaries) == 2
    assert summaries[0]["SnapshotName"] == "s1"  # oldest first, as AWS lists them
    assert summaries[1]["SnapshotName"].startswith(f"STOP-{name}-")

    with pytest.raises(ClientError) as exc:
        kinesisanalyticsv2.delete_application_snapshot(
            ApplicationName=name, SnapshotName="s1", SnapshotCreationTimestamp=0)
    assert _code(exc) == "InvalidArgumentException"
    kinesisanalyticsv2.delete_application_snapshot(
        ApplicationName=name, SnapshotName="s1",
        SnapshotCreationTimestamp=snap["SnapshotCreationTimestamp"])
    with pytest.raises(ClientError) as exc:
        kinesisanalyticsv2.describe_application_snapshot(ApplicationName=name, SnapshotName="s1")
    assert _code(exc) == "ResourceNotFoundException"


def test_manual_snapshot_with_snapshots_disabled(kinesisanalyticsv2):
    """AWS accepts CreateApplicationSnapshot on a RUNNING app with SnapshotsEnabled false."""
    name = _create(kinesisanalyticsv2)
    kinesisanalyticsv2.start_application(ApplicationName=name)
    _detail(kinesisanalyticsv2, name)
    kinesisanalyticsv2.create_application_snapshot(ApplicationName=name, SnapshotName="s1")
    snap = kinesisanalyticsv2.describe_application_snapshot(ApplicationName=name, SnapshotName="s1")
    assert snap["SnapshotDetails"]["SnapshotStatus"] == "READY"


def test_update_of_running_application_takes_a_snapshot(kinesisanalyticsv2):
    """With snapshots enabled, AWS records UPDATEAPPLICATION-<app>-<ms> with the pre-update version."""
    name = _create(kinesisanalyticsv2, ApplicationSnapshotConfiguration={"SnapshotsEnabled": True})
    kinesisanalyticsv2.start_application(ApplicationName=name)
    _detail(kinesisanalyticsv2, name)
    kinesisanalyticsv2.update_application(
        ApplicationName=name, CurrentApplicationVersionId=1,
        ApplicationConfigurationUpdate={"EnvironmentPropertyUpdates": {"PropertyGroups": [
            {"PropertyGroupId": "g", "PropertyMap": {"k": "v"}}]}})
    [snap] = kinesisanalyticsv2.list_application_snapshots(ApplicationName=name)["SnapshotSummaries"]
    assert snap["SnapshotName"].startswith(f"UPDATEAPPLICATION-{name}-")
    assert snap["ApplicationVersionId"] == 1


def test_describe_reports_aws_defaults(kinesisanalyticsv2, s3):
    """Code size and MD5 of the S3 object, the AWS-owned key and the maintenance window."""
    import hashlib

    bucket = f"kda-code-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="app.jar", Body=b"jar-bytes")
    name = _name()
    kinesisanalyticsv2.create_application(
        ApplicationName=name, RuntimeEnvironment=RUNTIME, ServiceExecutionRole=ROLE,
        ApplicationConfiguration={"ApplicationCodeConfiguration": {
            "CodeContentType": "ZIPFILE",
            "CodeContent": {"S3ContentLocation": {"BucketARN": f"arn:aws:s3:::{bucket}", "FileKey": "app.jar"}}}})
    detail = _detail(kinesisanalyticsv2, name)
    code = detail["ApplicationConfigurationDescription"]["ApplicationCodeConfigurationDescription"][
        "CodeContentDescription"]
    assert code["CodeSize"] == len(b"jar-bytes")
    assert code["CodeMD5"] == hashlib.md5(b"jar-bytes").hexdigest()
    assert detail["ApplicationConfigurationDescription"]["ApplicationEncryptionConfigurationDescription"] == {
        "KeyType": "AWS_OWNED_KEY"}
    assert detail["ApplicationMaintenanceConfigurationDescription"] == {
        "ApplicationMaintenanceWindowStartTime": "03:00", "ApplicationMaintenanceWindowEndTime": "11:00"}


def test_restore_from_unknown_custom_snapshot(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2, ApplicationSnapshotConfiguration={"SnapshotsEnabled": True})
    with pytest.raises(ClientError) as exc:
        kinesisanalyticsv2.start_application(ApplicationName=name, RunConfiguration={
            "ApplicationRestoreConfiguration": {"ApplicationRestoreType": "RESTORE_FROM_CUSTOM_SNAPSHOT",
                                                "SnapshotName": "nope"}})
    assert _code(exc) == "InvalidArgumentException"


def test_snapshot_list_pagination(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2, ApplicationSnapshotConfiguration={"SnapshotsEnabled": True})
    kinesisanalyticsv2.start_application(ApplicationName=name)
    _detail(kinesisanalyticsv2, name)
    for i in range(3):
        kinesisanalyticsv2.create_application_snapshot(ApplicationName=name, SnapshotName=f"s{i}")
    first = kinesisanalyticsv2.list_application_snapshots(ApplicationName=name, Limit=2)
    assert len(first["SnapshotSummaries"]) == 2
    rest = kinesisanalyticsv2.list_application_snapshots(
        ApplicationName=name, Limit=2, NextToken=first["NextToken"])
    assert len(rest["SnapshotSummaries"]) == 1
    assert "NextToken" not in rest


# ---------------------------------------------------------------------------
# Logging options and tags
# ---------------------------------------------------------------------------

def test_cloudwatch_logging_options(kinesisanalyticsv2):
    name = _create(kinesisanalyticsv2)
    stream = "arn:aws:logs:us-east-1:000000000000:log-group:flink:log-stream:app"
    added = kinesisanalyticsv2.add_application_cloud_watch_logging_option(
        ApplicationName=name, CurrentApplicationVersionId=1,
        CloudWatchLoggingOption={"LogStreamARN": stream})
    assert added["ApplicationVersionId"] == 2
    assert "OperationId" not in added
    [option] = added["CloudWatchLoggingOptionDescriptions"]
    assert option["LogStreamARN"] == stream
    assert option["CloudWatchLoggingOptionId"] == "2.1"  # the version the add created
    assert _detail(kinesisanalyticsv2, name)["CloudWatchLoggingOptionDescriptions"] == [option]

    with pytest.raises(ClientError) as exc:  # AWS allows only one logging option
        kinesisanalyticsv2.add_application_cloud_watch_logging_option(
            ApplicationName=name, CloudWatchLoggingOption={"LogStreamARN": stream + "-other"})
    assert _code(exc) == "InvalidArgumentException"

    removed = kinesisanalyticsv2.delete_application_cloud_watch_logging_option(
        ApplicationName=name, CloudWatchLoggingOptionId=option["CloudWatchLoggingOptionId"])
    assert removed["ApplicationVersionId"] == 3
    assert "CloudWatchLoggingOptionDescriptions" not in removed
    assert _detail(kinesisanalyticsv2, name)["CloudWatchLoggingOptionDescriptions"] == []


def test_tags(kinesisanalyticsv2):
    name = _name()
    kinesisanalyticsv2.create_application(
        ApplicationName=name, RuntimeEnvironment=RUNTIME, ServiceExecutionRole=ROLE,
        Tags=[{"Key": "team", "Value": "data"}])
    arn = _detail(kinesisanalyticsv2, name)["ApplicationARN"]
    kinesisanalyticsv2.tag_resource(ResourceARN=arn, Tags=[{"Key": "env", "Value": "dev"}])
    kinesisanalyticsv2.untag_resource(ResourceARN=arn, TagKeys=["team"])
    assert kinesisanalyticsv2.list_tags_for_resource(ResourceARN=arn)["Tags"] == [
        {"Key": "env", "Value": "dev"}]
    with pytest.raises(ClientError) as exc:
        kinesisanalyticsv2.list_tags_for_resource(ResourceARN=arn + "-missing")
    assert _code(exc) == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Isolation and persistence
# ---------------------------------------------------------------------------

def test_applications_are_region_scoped(kinesisanalyticsv2):
    import boto3
    from botocore.config import Config

    name = _create(kinesisanalyticsv2)
    west = boto3.client(
        "kinesisanalyticsv2", endpoint_url=kinesisanalyticsv2.meta.endpoint_url,
        region_name="us-west-2", aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 0}))
    with pytest.raises(ClientError) as exc:
        west.describe_application(ApplicationName=name)
    assert _code(exc) == "ResourceNotFoundException"


@pytest.fixture
def local_state():
    set_request_account_id("test")
    set_request_region("us-east-1")
    saved = kda.get_state()
    kda.reset()
    yield
    kda.reset()
    kda.load_persisted_state(saved)


def test_restored_running_application_comes_back_ready(local_state):
    kda.create_application({"ApplicationName": "persisted", "RuntimeEnvironment": RUNTIME,
                            "ServiceExecutionRole": ROLE})
    kda._applications["persisted"]["ApplicationStatus"] = "RUNNING"
    state = kda.get_state()
    kda.reset()
    kda.load_persisted_state(state)
    assert kda._applications["persisted"]["ApplicationStatus"] == "READY"


# ---------------------------------------------------------------------------
# CloudFormation (the resources a CDK Flink stack synthesizes)
# ---------------------------------------------------------------------------

def _wait_stack(cfn, name, timeout=30):
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            stack = cfn.describe_stacks(StackName=name)["Stacks"][0]
        except ClientError as exc:
            if "does not exist" in str(exc):
                return {"StackStatus": "DELETE_COMPLETE"}
            raise
        if not stack["StackStatus"].endswith("_IN_PROGRESS"):
            return stack
        time.sleep(0.3)
    raise AssertionError(f"stack {name} did not settle")


def _flink_template(app_name, group, value):
    import json

    return json.dumps({"Resources": {
        "Logs": {"Type": "AWS::Logs::LogGroup", "Properties": {"LogGroupName": group}},
        "Stream": {"Type": "AWS::Logs::LogStream",
                   "Properties": {"LogGroupName": {"Ref": "Logs"}, "LogStreamName": "flink"}},
        "App": {"Type": "AWS::KinesisAnalyticsV2::Application", "Properties": {
            "ApplicationName": app_name,
            "RuntimeEnvironment": RUNTIME,
            "ServiceExecutionRole": ROLE,
            "ApplicationConfiguration": {
                **CODE,
                "ApplicationSnapshotConfiguration": {"SnapshotsEnabled": True},
                "EnvironmentProperties": {"PropertyGroups": [
                    {"PropertyGroupId": "app", "PropertyMap": {"value": value}}]},
            },
        }},
        "AppLogging": {"Type": "AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption",
                       "DependsOn": "Stream",
                       "Properties": {"ApplicationName": {"Ref": "App"}, "CloudWatchLoggingOption": {
                           "LogStreamARN": {"Fn::Sub": "arn:aws:logs:${AWS::Region}:${AWS::AccountId}:"
                                                       f"log-group:{group}:log-stream:flink"}}}},
    }})


def test_cloudformation_flink_stack(cfn, logs, kinesisanalyticsv2):
    stack, app, group = _name("flink-stack"), _name("cfn-app"), f"/flink/{_name()}"
    cfn.create_stack(StackName=stack, TemplateBody=_flink_template(app, group, "1"))
    assert _wait_stack(cfn, stack)["StackStatus"] == "CREATE_COMPLETE"

    detail = _detail(kinesisanalyticsv2, app)
    assert detail["ApplicationStatus"] == "READY"
    groups = detail["ApplicationConfigurationDescription"]["EnvironmentPropertyDescriptions"]
    assert groups["PropertyGroupDescriptions"][0]["PropertyMap"] == {"value": "1"}
    [option] = detail["CloudWatchLoggingOptionDescriptions"]
    assert option["LogStreamARN"].endswith(f"log-group:{group}:log-stream:flink")
    streams = logs.describe_log_streams(logGroupName=group)["logStreams"]
    assert [s["logStreamName"] for s in streams] == ["flink"]
    version = detail["ApplicationVersionId"]

    cfn.update_stack(StackName=stack, TemplateBody=_flink_template(app, group, "2"))
    assert _wait_stack(cfn, stack)["StackStatus"] == "UPDATE_COMPLETE"
    detail = _detail(kinesisanalyticsv2, app)
    groups = detail["ApplicationConfigurationDescription"]["EnvironmentPropertyDescriptions"]
    assert groups["PropertyGroupDescriptions"][0]["PropertyMap"] == {"value": "2"}
    assert detail["ApplicationVersionId"] > version
    assert len(detail["CloudWatchLoggingOptionDescriptions"]) == 1

    cfn.delete_stack(StackName=stack)
    assert _wait_stack(cfn, stack)["StackStatus"] == "DELETE_COMPLETE"
    with pytest.raises(ClientError) as exc:
        _detail(kinesisanalyticsv2, app)
    assert _code(exc) == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Data plane hooks (a fake data plane, in process)
# ---------------------------------------------------------------------------

class _FakeDataPlane:
    """Stands in for kinesisanalyticsv2_flink: slow savepoints, optional failure."""

    def __init__(self, savepoint="file:///savepoints/sp-1", delay=0.5):
        import threading

        self.savepoint = savepoint
        self.delay = delay
        self.started = []
        self.release = threading.Event()

    def available(self):
        return True

    def supports(self, runtime):
        return runtime in ("FLINK-1_19", "FLINK-1_20")

    def start(self, app, savepoint_path, on_status):
        self.started.append(savepoint_path)
        on_status("RUNNING")

    def _slow(self):
        self.release.wait(self.delay)
        return self.savepoint

    def stop(self, app, force, take_snapshot):
        return self._slow() if take_snapshot else None

    def snapshot(self, app, snapshot_name):
        return self._slow()

    def delete(self, app):
        pass

    def reset(self):
        pass


def _wait_until(predicate, timeout=5):
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _call(action, payload):
    import asyncio
    import json

    status, _, body = asyncio.run(kda.handle_request(
        "POST", "/", {"x-amz-target": f"KinesisAnalytics_20180523.{action}"}, json.dumps(payload).encode(), {}))
    return status, json.loads(body) if body else {}


@pytest.fixture
def fake_dataplane(local_state, monkeypatch):
    fake = _FakeDataPlane()
    monkeypatch.setattr(kda, "_dataplane", fake)
    kda.create_application({"ApplicationName": "dp", "RuntimeEnvironment": RUNTIME, "ServiceExecutionRole": ROLE,
                            "ApplicationConfiguration": {"ApplicationSnapshotConfiguration": {"SnapshotsEnabled": True}}})
    assert _call("StartApplication", {"ApplicationName": "dp"})[0] == 200
    assert kda._applications["dp"]["ApplicationStatus"] == "RUNNING"
    return fake


def _status(name="dp"):
    return kda._applications[name]["ApplicationStatus"]


def _snapshot_status(snapshot_name):
    return kda._snapshots["dp"][snapshot_name]["SnapshotStatus"]


def test_snapshot_runs_in_background(fake_dataplane):
    import time

    started = time.time()
    assert _call("CreateApplicationSnapshot", {"ApplicationName": "dp", "SnapshotName": "s1"})[0] == 200
    assert time.time() - started < 0.3  # the request does not wait for the savepoint
    assert _snapshot_status("s1") == "CREATING"
    assert _wait_until(lambda: _snapshot_status("s1") == "READY")
    assert kda._snapshots["dp"]["s1"]["_SavepointPath"] == "file:///savepoints/sp-1"


def test_failed_snapshot_is_marked_failed_and_not_restored(fake_dataplane):
    fake_dataplane.savepoint = None
    _call("CreateApplicationSnapshot", {"ApplicationName": "dp", "SnapshotName": "bad"})
    assert _wait_until(lambda: _snapshot_status("bad") == "FAILED")
    _call("StopApplication", {"ApplicationName": "dp", "Force": True})
    assert _wait_until(lambda: _status() == "READY")
    status, body = _call("StartApplication", {"ApplicationName": "dp", "RunConfiguration": {
        "ApplicationRestoreConfiguration": {"ApplicationRestoreType": "RESTORE_FROM_CUSTOM_SNAPSHOT",
                                            "SnapshotName": "bad"}}})
    assert status == 400
    assert body["__type"] == "InvalidArgumentException"


def test_graceful_stop_runs_in_background_and_restores_latest(fake_dataplane):
    import time

    started = time.time()
    _call("StopApplication", {"ApplicationName": "dp"})
    assert time.time() - started < 0.3
    assert _status() == "STOPPING"
    assert _wait_until(lambda: _status() == "READY")
    [snap] = kda._snapshots["dp"].values()
    assert snap["SnapshotStatus"] == "READY"

    _call("StartApplication", {"ApplicationName": "dp", "RunConfiguration": {
        "ApplicationRestoreConfiguration": {"ApplicationRestoreType": "RESTORE_FROM_LATEST_SNAPSHOT"}}})
    assert fake_dataplane.started[-1] == "file:///savepoints/sp-1"


def test_data_plane_start_failure_returns_to_ready(local_state, monkeypatch):
    fake = _FakeDataPlane()
    fake.start = lambda app, savepoint, on_status: on_status("FAILED", "jar not found")
    monkeypatch.setattr(kda, "_dataplane", fake)
    kda.create_application({"ApplicationName": "dp", "RuntimeEnvironment": RUNTIME, "ServiceExecutionRole": ROLE})
    _call("StartApplication", {"ApplicationName": "dp"})
    assert _status() == "READY"


def test_runtime_without_an_image_stays_control_plane_only(local_state, monkeypatch):
    fake = _FakeDataPlane()
    monkeypatch.setattr(kda, "_dataplane", fake)
    kda.create_application({"ApplicationName": "dp", "RuntimeEnvironment": "FLINK-1_18", "ServiceExecutionRole": ROLE,
                            "ApplicationConfiguration": {"ApplicationSnapshotConfiguration": {"SnapshotsEnabled": True}}})
    _call("StartApplication", {"ApplicationName": "dp"})
    assert fake.started == []

    def described():
        return _call("DescribeApplication", {"ApplicationName": "dp"})[1]["ApplicationDetail"]["ApplicationStatus"]

    assert described() == "RUNNING"  # settles on read, as without Docker
    _call("StopApplication", {"ApplicationName": "dp"})
    assert described() == "READY"
    [snap] = kda._snapshots["dp"].values()
    assert snap["SnapshotStatus"] == "READY"
