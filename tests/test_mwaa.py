"""Tests for MWAA (Managed Workflows for Apache Airflow) service."""

import time

import pytest

from conftest import make_client

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
