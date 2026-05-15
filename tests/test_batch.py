import boto3
import pytest

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


def _client(service, account="test"):
    return boto3.client(service, endpoint_url=ENDPOINT, region_name=REGION,
                        aws_access_key_id=account, aws_secret_access_key="test")


@pytest.fixture(scope="module")
def batch():
    return _client("batch")


def _client_for(account):
    return _client("batch", account=account)


def _uid():
    import uuid
    return uuid.uuid4().hex[:8]


def test_batch_describe_empty_lists(batch):
    assert isinstance(batch.describe_compute_environments()["computeEnvironments"], list)
    assert isinstance(batch.describe_job_queues()["jobQueues"], list)


def test_batch_full_lifecycle(batch):
    """ComputeEnv -> JobQueue -> JobDefinition -> SubmitJob (auto-SUCCEEDED) -> ListJobs."""
    ce_name = f"ce-{_uid()}"
    jq_name = f"jq-{_uid()}"
    jd_name = f"jd-{_uid()}"
    job_name = f"j-{_uid()}"

    ce = batch.create_compute_environment(
        computeEnvironmentName=ce_name,
        type="MANAGED",
        serviceRole="arn:aws:iam::000000000000:role/batch",
    )
    assert ce["computeEnvironmentArn"].endswith(f"compute-environment/{ce_name}")

    jq = batch.create_job_queue(
        jobQueueName=jq_name,
        priority=1,
        computeEnvironmentOrder=[{"order": 1, "computeEnvironment": ce["computeEnvironmentArn"]}],
    )
    assert jq["jobQueueArn"].endswith(f"job-queue/{jq_name}")

    jd = batch.register_job_definition(
        jobDefinitionName=jd_name,
        type="container",
        containerProperties={"image": "busybox", "memory": 128, "vcpus": 1},
    )
    assert jd["revision"] == 1

    sj = batch.submit_job(jobName=job_name, jobQueue=jq["jobQueueArn"], jobDefinition=jd["jobDefinitionArn"])
    job_id = sj["jobId"]

    described = batch.describe_jobs(jobs=[job_id])["jobs"]
    assert len(described) == 1
    assert described[0]["status"] == "SUCCEEDED"
    assert described[0]["container"]["exitCode"] == 0

    listed = batch.list_jobs(jobQueue=jq_name)["jobSummaryList"]
    assert any(j["jobId"] == job_id for j in listed)


def test_batch_register_job_definition_revisions(batch):
    name = f"rev-{_uid()}"
    r1 = batch.register_job_definition(jobDefinitionName=name, type="container",
                                       containerProperties={"image": "a", "memory": 128, "vcpus": 1})
    r2 = batch.register_job_definition(jobDefinitionName=name, type="container",
                                       containerProperties={"image": "b", "memory": 128, "vcpus": 1})
    assert r1["revision"] == 1
    assert r2["revision"] == 2


def test_batch_describe_job_queue_by_name_or_arn(batch):
    name = f"lookup-{_uid()}"
    batch.create_job_queue(jobQueueName=name, priority=1, computeEnvironmentOrder=[])
    by_name = batch.describe_job_queues(jobQueues=[name])["jobQueues"]
    assert any(q["jobQueueName"] == name for q in by_name)
    arn = by_name[0]["jobQueueArn"]
    by_arn = batch.describe_job_queues(jobQueues=[arn])["jobQueues"]
    assert any(q["jobQueueName"] == name for q in by_arn)


def test_batch_account_isolation():
    a = _client_for("555555555555")
    b = _client_for("666666666666")
    name = f"iso-{_uid()}"
    a.create_job_queue(jobQueueName=name, priority=1, computeEnvironmentOrder=[])
    a_qs = [q["jobQueueName"] for q in a.describe_job_queues()["jobQueues"]]
    b_qs = [q["jobQueueName"] for q in b.describe_job_queues()["jobQueues"]]
    assert name in a_qs
    assert name not in b_qs
