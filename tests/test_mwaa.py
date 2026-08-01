"""Tests for MWAA (Managed Workflows for Apache Airflow) service."""

import threading

import boto3
import pytest
from botocore.config import Config
from conftest import ENDPOINT, make_client


def _mwaa_client(region):
    return boto3.client(
        "mwaa",
        endpoint_url=ENDPOINT,
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"mode": "standard"}),
    )

mwaa_client = make_client("mwaa")
s3_client = make_client("s3")


ENV_NAME = "test-airflow-env"


class TestCreateEnvironment:
    def test_create_returns_arn(self):
        resp = mwaa_client.create_environment(
            Name=ENV_NAME,
            DagS3Path="dags/",
            ExecutionRoleArn="arn:aws:iam::000000000000:role/test-role",
            SourceBucketArn="arn:aws:s3:::test-bucket",
            NetworkConfiguration={"SubnetIds": ["subnet-1", "subnet-2"], "SecurityGroupIds": ["sg-1"]},
        )
        assert "Arn" in resp
        assert ENV_NAME in resp["Arn"]

    def test_duplicate_create_fails(self):
        with pytest.raises(mwaa_client.exceptions.ClientError) as exc_info:
            mwaa_client.create_environment(
                Name=ENV_NAME,
                DagS3Path="dags/",
                ExecutionRoleArn="arn:aws:iam::000000000000:role/test-role",
                SourceBucketArn="arn:aws:s3:::test-bucket",
                NetworkConfiguration={"SubnetIds": ["subnet-1", "subnet-2"], "SecurityGroupIds": ["sg-1"]},
            )
        assert "already exists" in str(exc_info.value).lower() or "ResourceAlreadyExists" in str(exc_info.value)

    @pytest.mark.parametrize("source_bucket_arn", [
        "not-an-arn",
        "arn:aws:sns:us-east-1:000000000000:topic-name",
        "arn:aws:s3:::test-bucket/dags",
    ])
    def test_create_rejects_invalid_source_bucket_arn(self, source_bucket_arn):
        with pytest.raises(mwaa_client.exceptions.ClientError) as exc_info:
            mwaa_client.create_environment(
                Name="bad-source-bucket-arn",
                DagS3Path="dags/",
                ExecutionRoleArn="arn:aws:iam::000000000000:role/test-role",
                SourceBucketArn=source_bucket_arn,
                NetworkConfiguration={"SubnetIds": ["subnet-1", "subnet-2"], "SecurityGroupIds": ["sg-1"]},
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


class TestGetEnvironment:
    def test_get_returns_environment(self):
        resp = mwaa_client.get_environment(Name=ENV_NAME)
        env = resp["Environment"]
        assert env["Name"] == ENV_NAME
        assert "Arn" in env
        assert env["Status"] in ("CREATING", "AVAILABLE", "CREATE_FAILED")
        assert env["AirflowVersion"] == "3.0.6"

    def test_get_nonexistent_fails(self):
        with pytest.raises(mwaa_client.exceptions.ClientError) as exc_info:
            mwaa_client.get_environment(Name="nonexistent-env")
        assert "ResourceNotFound" in str(exc_info.value) or "not found" in str(exc_info.value).lower()


class TestListEnvironments:
    def test_list_includes_created(self):
        resp = mwaa_client.list_environments()
        assert ENV_NAME in resp["Environments"]


class TestUpdateEnvironment:
    def test_update_workers(self):
        resp = mwaa_client.update_environment(
            Name=ENV_NAME,
            MaxWorkers=10,
        )
        assert "Arn" in resp

        updated = mwaa_client.get_environment(Name=ENV_NAME)["Environment"]
        assert updated["MaxWorkers"] == 10

    def test_update_rejects_invalid_source_bucket_arn(self):
        with pytest.raises(mwaa_client.exceptions.ClientError) as exc_info:
            mwaa_client.update_environment(
                Name=ENV_NAME,
                SourceBucketArn="arn:aws:s3:us-east-1:000000000000:test-bucket",
            )
        assert exc_info.value.response["Error"]["Code"] == "ValidationException"


class TestCreateWebLoginToken:
    def test_returns_token(self):
        resp = mwaa_client.create_web_login_token(Name=ENV_NAME)
        assert "WebToken" in resp
        assert "WebServerHostname" in resp


class TestCreateCliToken:
    def test_returns_token(self):
        resp = mwaa_client.create_cli_token(Name=ENV_NAME)
        assert "CliToken" in resp
        assert "WebServerHostname" in resp


class TestDeleteEnvironment:
    def test_delete_succeeds(self):
        mwaa_client.delete_environment(Name=ENV_NAME)
        # Verify it's gone
        resp = mwaa_client.list_environments()
        assert ENV_NAME not in resp["Environments"]

    def test_delete_nonexistent_fails(self):
        with pytest.raises(mwaa_client.exceptions.ClientError):
            mwaa_client.delete_environment(Name="nonexistent-env")


class TestAirflow2Environment:
    """Verify Airflow 2.x environment creation works with v2 config."""

    ENV_V2 = "test-airflow2-env"

    def test_create_v2_returns_arn(self):
        resp = mwaa_client.create_environment(
            Name=self.ENV_V2,
            AirflowVersion="2.10.4",
            DagS3Path="dags/",
            ExecutionRoleArn="arn:aws:iam::000000000000:role/test-role",
            SourceBucketArn="arn:aws:s3:::test-bucket",
            NetworkConfiguration={"SubnetIds": ["subnet-1", "subnet-2"], "SecurityGroupIds": ["sg-1"]},
        )
        assert "Arn" in resp

    def test_v2_version_stored(self):
        env = mwaa_client.get_environment(Name=self.ENV_V2)["Environment"]
        assert env["AirflowVersion"] == "2.10.4"

    def test_cleanup_v2(self):
        mwaa_client.delete_environment(Name=self.ENV_V2)
        resp = mwaa_client.list_environments()
        assert self.ENV_V2 not in resp["Environments"]


class TestEnvironmentWithDags:
    """Test DAG sync from S3 when Docker is available."""

    def test_create_with_s3_dags(self):
        bucket_name = "mwaa-test-dags-bucket"
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

        # Upload a simple DAG
        dag_content = '''
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG("test_dag", start_date=datetime(2026, 1, 1), schedule=None, catchup=False):
    EmptyOperator(task_id="hello")
'''
        s3_client.put_object(Bucket=bucket_name, Key="dags/test_dag.py", Body=dag_content.encode())

        env_name = "test-env-with-dags"
        try:
            resp = mwaa_client.create_environment(
                Name=env_name,
                DagS3Path="dags/",
                ExecutionRoleArn="arn:aws:iam::000000000000:role/test-role",
                SourceBucketArn=f"arn:aws:s3:::{bucket_name}",
                NetworkConfiguration={"SubnetIds": ["subnet-1", "subnet-2"], "SecurityGroupIds": ["sg-1"]},
            )
            assert "Arn" in resp

            env = mwaa_client.get_environment(Name=env_name)["Environment"]
            assert env["SourceBucketArn"] == f"arn:aws:s3:::{bucket_name}"
            assert env["DagS3Path"] == "dags/"
            listed = mwaa_client.list_environments()["Environments"]
            assert env_name in listed
        finally:
            try:
                mwaa_client.delete_environment(Name=env_name)
            except Exception:
                pass


def test_same_name_environments_are_isolated_by_region():
    env_name = "regional-airflow-env"
    east = _mwaa_client("us-east-1")
    west = _mwaa_client("us-west-2")
    create_kwargs = {
        "Name": env_name,
        "DagS3Path": "dags/",
        "ExecutionRoleArn": "arn:aws:iam::000000000000:role/test-role",
        "SourceBucketArn": "arn:aws:s3:::regional-airflow-bucket",
        "NetworkConfiguration": {
            "SubnetIds": ["subnet-1", "subnet-2"],
            "SecurityGroupIds": ["sg-1"],
        },
    }

    try:
        east_arn = east.create_environment(**create_kwargs)["Arn"]
        west_arn = west.create_environment(**create_kwargs)["Arn"]

        assert ":us-east-1:" in east_arn
        assert ":us-west-2:" in west_arn
        assert east.get_environment(Name=env_name)["Environment"]["Arn"] == east_arn
        assert west.get_environment(Name=env_name)["Environment"]["Arn"] == west_arn
        assert east.list_environments()["Environments"] == [env_name]
        assert west.list_environments()["Environments"] == [env_name]

        east.delete_environment(Name=env_name)
        assert west.get_environment(Name=env_name)["Environment"]["Arn"] == west_arn
    finally:
        for client in (east, west):
            try:
                client.delete_environment(Name=env_name)
            except Exception:
                pass


@pytest.mark.parametrize("legacy_volumes", [False, True])
def test_airflow_runtime_identity_and_background_scope_include_region(
    monkeypatch,
    legacy_volumes,
):
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import mwaa as mod

    class FakeContainer:
        id = "container-id"
        attrs = {}

        def __init__(self):
            self.remove_calls = []

        def remove(self, **kwargs):
            self.remove_calls.append(kwargs)

    class FakeContainers:
        def __init__(self, legacy_name, legacy_container):
            self.get_calls = []
            self.run_kwargs = None
            self.legacy_name = legacy_name
            self.legacy_container = legacy_container

        def get(self, name):
            self.get_calls.append(name)
            if name == self.legacy_name:
                return self.legacy_container
            raise RuntimeError("not found")

        def run(self, **kwargs):
            self.run_kwargs = kwargs
            return FakeContainer()

    class FakeDocker:
        def __init__(self, legacy_name, legacy_container):
            self.containers = FakeContainers(legacy_name, legacy_container)

    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    region = "us-west-2"
    env_name = "regional-runtime"
    expected_name = f"ministack-mwaa-{region}-{env_name}"
    legacy_name = f"ministack-mwaa-{env_name}"
    env = {
        "AirflowVersion": "3.0.6",
        "AirflowConfigurationOptions": {},
        "Status": "CREATING",
    }
    if legacy_volumes:
        env["_docker_dags_volume_name"] = f"{legacy_name}-dags"
        env["_docker_db_volume_name"] = f"{legacy_name}-db"
    legacy_container = FakeContainer()
    docker_client = FakeDocker(legacy_name, legacy_container)
    dag_sync_scope = {}
    dag_sync_complete = threading.Event()

    def capture_dag_sync(*_args):
        dag_sync_scope["value"] = (get_account_id(), get_region())
        dag_sync_complete.set()

    monkeypatch.setattr(mod, "_get_docker", lambda: docker_client)
    monkeypatch.setattr(mod, "_ministack_network", "")
    monkeypatch.setattr(mod, "_wait_for_port", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(mod, "_sync_dags_from_s3", capture_dag_sync)
    monkeypatch.setattr(mod, "MWAA_PERSIST", True)

    try:
        set_request_account_id("222222222222")
        set_request_region("us-east-1")
        mod._start_airflow_container(account_id, region, env_name, env)

        assert dag_sync_complete.wait(timeout=2)
        assert docker_client.containers.get_calls == [expected_name, legacy_name]
        assert legacy_container.remove_calls == [{"force": True}]
        assert docker_client.containers.run_kwargs["name"] == expected_name
        assert docker_client.containers.run_kwargs["labels"] == {
            "ministack": "mwaa",
            "region": region,
            "env_name": env_name,
        }
        expected_volume_prefix = legacy_name if legacy_volumes else expected_name
        assert set(docker_client.containers.run_kwargs["volumes"]) == {
            f"{expected_volume_prefix}-dags",
            f"{expected_volume_prefix}-db",
        }
        assert dag_sync_scope["value"] == (account_id, region)
        assert (get_account_id(), get_region()) == ("222222222222", "us-east-1")
    finally:
        mod._release_port(env.get("_host_port"))
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_reset_stops_containers_in_every_region(monkeypatch):
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import mwaa as mod

    class FakeContainer:
        def __init__(self):
            self.stop_calls = []
            self.remove_calls = []

        def stop(self, timeout):
            self.stop_calls.append(timeout)

        def remove(self, force):
            self.remove_calls.append(force)

    class FakeContainers:
        def __init__(self, containers):
            self._containers = containers

        def get(self, container_id):
            return self._containers[container_id]

    class FakeDocker:
        def __init__(self, containers):
            self.containers = FakeContainers(containers)

    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    containers = {
        "east-container": FakeContainer(),
        "west-container": FakeContainer(),
    }
    monkeypatch.setattr(mod, "_get_docker", lambda: FakeDocker(containers))
    mod._environments.clear()

    try:
        mod._environments.set_scoped(
            account_id,
            "us-east-1",
            "shared-name",
            {"_docker_container_id": "east-container"},
        )
        mod._environments.set_scoped(
            account_id,
            "us-west-2",
            "shared-name",
            {"_docker_container_id": "west-container"},
        )
        set_request_account_id(account_id)
        set_request_region("us-east-1")

        mod.reset()

        assert not mod._environments.has_any()
        for container in containers.values():
            assert container.stop_calls == [5]
            assert container.remove_calls == [True]
    finally:
        mod._environments.clear()
        set_request_account_id(original_account)
        set_request_region(original_region)
