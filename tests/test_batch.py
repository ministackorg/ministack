import os

import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
REGION = "us-east-1"


def _client(service, account="test", region=REGION):
    return boto3.client(service, endpoint_url=ENDPOINT, region_name=region,
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


def test_batch_list_jobs_by_job_queue_arn(batch):
    jq_name = f"jq-arn-{_uid()}"
    jd_name = f"jd-arn-{_uid()}"
    job_name = f"j-arn-{_uid()}"
    jq = batch.create_job_queue(jobQueueName=jq_name, priority=1, computeEnvironmentOrder=[])
    jd = batch.register_job_definition(
        jobDefinitionName=jd_name,
        type="container",
        containerProperties={"image": "busybox", "memory": 128, "vcpus": 1},
    )
    submitted = batch.submit_job(
        jobName=job_name,
        jobQueue=jq["jobQueueArn"],
        jobDefinition=jd["jobDefinitionArn"],
    )

    listed = batch.list_jobs(jobQueue=jq["jobQueueArn"])["jobSummaryList"]

    assert any(j["jobId"] == submitted["jobId"] for j in listed)


def test_batch_job_queue_arn_inputs_do_not_tail_match(batch):
    jq_name = f"jq-bad-arn-{_uid()}"
    jd_name = f"jd-bad-arn-{_uid()}"
    job_name = f"j-bad-arn-{_uid()}"
    jq = batch.create_job_queue(jobQueueName=jq_name, priority=1, computeEnvironmentOrder=[])
    jd = batch.register_job_definition(
        jobDefinitionName=jd_name,
        type="container",
        containerProperties={"image": "busybox", "memory": 128, "vcpus": 1},
    )
    submitted = batch.submit_job(
        jobName=job_name,
        jobQueue=jq["jobQueueArn"],
        jobDefinition=jd["jobDefinitionArn"],
    )

    wrong_service = f"arn:aws:sqs:us-east-1:000000000000:job-queue/{jq_name}"
    wrong_account = f"arn:aws:batch:us-east-1:111111111111:job-queue/{jq_name}"
    wrong_resource = f"arn:aws:batch:us-east-1:000000000000:compute-environment/{jq_name}"
    malformed = f"arn:aws:batch:us-east-1:000000000000:job-queue/{jq_name}/extra"
    foreign_region = f"arn:aws:batch:us-west-2:000000000000:job-queue/{jq_name}"

    def _assert_client_exception(call):
        with pytest.raises(ClientError) as exc:
            call()
        assert exc.value.response["Error"]["Code"] == "ClientException"

    for bad_ref in [wrong_service, wrong_account, wrong_resource, malformed]:
        _assert_client_exception(
            lambda bad_ref=bad_ref: batch.describe_job_queues(jobQueues=[bad_ref])
        )
        _assert_client_exception(lambda bad_ref=bad_ref: batch.list_jobs(jobQueue=bad_ref))

    assert batch.describe_job_queues(jobQueues=[foreign_region])["jobQueues"] == []
    assert batch.list_jobs(jobQueue=foreign_region)["jobSummaryList"] == []
    listed_by_name = batch.list_jobs(jobQueue=jq_name)["jobSummaryList"]
    assert any(j["jobId"] == submitted["jobId"] for j in listed_by_name)


def test_batch_account_isolation():
    a = _client_for("555555555555")
    b = _client_for("666666666666")
    name = f"iso-{_uid()}"
    a.create_job_queue(jobQueueName=name, priority=1, computeEnvironmentOrder=[])
    a_qs = [q["jobQueueName"] for q in a.describe_job_queues()["jobQueues"]]
    b_qs = [q["jobQueueName"] for q in b.describe_job_queues()["jobQueues"]]
    assert name in a_qs
    assert name not in b_qs


def test_batch_update_compute_environment_create_update_describe(batch):
    """UpdateComputeEnvironment persists fields that DescribeComputeEnvironments returns."""
    name = f"ce-upd-{_uid()}"
    role = "arn:aws:iam::000000000000:role/batch"
    new_role = "arn:aws:iam::000000000000:role/batch-updated"
    created = batch.create_compute_environment(
        computeEnvironmentName=name,
        type="MANAGED",
        state="ENABLED",
        serviceRole=role,
        computeResources={
            "type": "EC2",
            "minvCpus": 0,
            "maxvCpus": 4,
            "instanceTypes": ["m5.large"],
            "subnets": ["subnet-aaa"],
            "securityGroupIds": ["sg-aaa"],
        },
    )
    assert created["computeEnvironmentName"] == name
    assert created["computeEnvironmentArn"].endswith(f"compute-environment/{name}")
    assert "updatePolicy" not in batch.describe_compute_environments(
        computeEnvironments=[name]
    )["computeEnvironments"][0]

    updated = batch.update_compute_environment(
        computeEnvironment=name,
        state="DISABLED",
        serviceRole=new_role,
        updatePolicy={
            "jobExecutionTimeoutMinutes": 60,
            "terminateJobsOnUpdate": True,
        },
        unmanagedvCpus=2,
        context="tf-update",
    )
    assert updated["computeEnvironmentName"] == name
    assert updated["computeEnvironmentArn"] == created["computeEnvironmentArn"]

    described = batch.describe_compute_environments(computeEnvironments=[name])[
        "computeEnvironments"
    ]
    assert len(described) == 1
    ce = described[0]
    assert ce["state"] == "DISABLED"
    assert ce["serviceRole"] == new_role
    assert ce["updatePolicy"] == {
        "jobExecutionTimeoutMinutes": 60,
        "terminateJobsOnUpdate": True,
    }
    assert ce["unmanagedvCpus"] == 2
    assert ce["context"] == "tf-update"


def test_batch_update_policy_is_update_only(batch):
    model = batch.meta.service_model
    create = model.operation_model("CreateComputeEnvironment").input_shape.members
    update = model.operation_model("UpdateComputeEnvironment").input_shape.members
    assert "updatePolicy" not in create
    assert "updatePolicy" in update

    from ministack.services import batch as batch_svc

    name = f"ce-policy-{_uid()}"
    policy = {
        "jobExecutionTimeoutMinutes": 30,
        "terminateJobsOnUpdate": False,
    }
    status, _, _ = batch_svc._create_compute_environment({
        "computeEnvironmentName": name,
        "type": "MANAGED",
        "serviceRole": "arn:aws:iam::000000000000:role/batch",
        "updatePolicy": policy,
    })
    assert status == 200
    assert "updatePolicy" not in batch_svc._compute_envs[name]

    status, _, _ = batch_svc._update_compute_environment({
        "computeEnvironment": name,
        "updatePolicy": policy,
    })
    assert status == 200
    assert batch_svc._compute_envs[name]["updatePolicy"] == policy


def test_batch_update_compute_environment_by_arn(batch):
    name = f"ce-arn-{_uid()}"
    created = batch.create_compute_environment(
        computeEnvironmentName=name,
        type="MANAGED",
        serviceRole="arn:aws:iam::000000000000:role/batch",
    )
    arn = created["computeEnvironmentArn"]

    updated = batch.update_compute_environment(
        computeEnvironment=arn,
        state="DISABLED",
    )
    assert updated["computeEnvironmentName"] == name
    assert updated["computeEnvironmentArn"] == arn

    described = batch.describe_compute_environments(computeEnvironments=[name])[
        "computeEnvironments"
    ][0]
    assert described["state"] == "DISABLED"


def test_batch_update_compute_environment_missing(batch):
    with pytest.raises(ClientError) as exc:
        batch.update_compute_environment(
            computeEnvironment=f"missing-ce-{_uid()}",
            state="DISABLED",
        )
    assert exc.value.response["Error"]["Code"] == "ClientException"


def test_batch_update_compute_environment_response_shape(batch):
    name = f"ce-shape-{_uid()}"
    batch.create_compute_environment(
        computeEnvironmentName=name,
        type="MANAGED",
        serviceRole="arn:aws:iam::000000000000:role/batch",
    )
    updated = batch.update_compute_environment(
        computeEnvironment=name,
        state="ENABLED",
    )
    assert set(updated.keys()) == {"computeEnvironmentName", "computeEnvironmentArn", "ResponseMetadata"}
    assert updated["computeEnvironmentName"] == name
    assert updated["computeEnvironmentArn"].endswith(f"compute-environment/{name}")


def test_batch_update_compute_environment_merges_compute_resources(batch):
    """Partial computeResources updates merge into existing resources (AWS ComputeResourceUpdate)."""
    name = f"ce-merge-{_uid()}"
    batch.create_compute_environment(
        computeEnvironmentName=name,
        type="MANAGED",
        serviceRole="arn:aws:iam::000000000000:role/batch",
        computeResources={
            "type": "EC2",
            "minvCpus": 0,
            "maxvCpus": 8,
            "desiredvCpus": 2,
            "instanceTypes": ["m5.large"],
            "subnets": ["subnet-aaa"],
            "securityGroupIds": ["sg-aaa"],
        },
    )

    batch.update_compute_environment(
        computeEnvironment=name,
        computeResources={
            "maxvCpus": 16,
            "desiredvCpus": 4,
        },
    )

    ce = batch.describe_compute_environments(computeEnvironments=[name])[
        "computeEnvironments"
    ][0]
    assert ce["computeResources"]["type"] == "EC2"
    assert ce["computeResources"]["minvCpus"] == 0
    assert ce["computeResources"]["maxvCpus"] == 16
    assert ce["computeResources"]["desiredvCpus"] == 4
    assert ce["computeResources"]["instanceTypes"] == ["m5.large"]
    assert ce["computeResources"]["subnets"] == ["subnet-aaa"]
    assert ce["computeResources"]["securityGroupIds"] == ["sg-aaa"]

def test_batch_resources_are_region_scoped():
    east = _client("batch", region="us-east-1")
    west = _client("batch", region="us-west-2")
    suffix = _uid()
    ce_name = f"ce-region-{suffix}"
    jq_name = f"jq-region-{suffix}"
    jd_name = f"jd-region-{suffix}"
    job_name = f"job-region-{suffix}"

    for client in (east, west):
        ce = client.create_compute_environment(
            computeEnvironmentName=ce_name,
            type="MANAGED",
            serviceRole="arn:aws:iam::000000000000:role/batch",
        )
        jq = client.create_job_queue(
            jobQueueName=jq_name,
            priority=1,
            computeEnvironmentOrder=[{"order": 1, "computeEnvironment": ce["computeEnvironmentArn"]}],
        )
        jd = client.register_job_definition(
            jobDefinitionName=jd_name,
            type="container",
            containerProperties={"image": "busybox", "memory": 128, "vcpus": 1},
        )
        client.submit_job(
            jobName=job_name,
            jobQueue=jq["jobQueueArn"],
            jobDefinition=jd["jobDefinitionArn"],
        )

    for client, region in ((east, "us-east-1"), (west, "us-west-2")):
        compute_envs = client.describe_compute_environments(
            computeEnvironments=[ce_name]
        )["computeEnvironments"]
        queues = client.describe_job_queues(jobQueues=[jq_name])["jobQueues"]
        definitions = client.describe_job_definitions(
            jobDefinitionName=jd_name,
        )["jobDefinitions"]
        jobs = client.list_jobs(jobQueue=jq_name)["jobSummaryList"]

        assert len(compute_envs) == 1
        assert len(queues) == 1
        assert len(definitions) == 1
        assert len(jobs) == 1
        assert f":{region}:" in compute_envs[0]["computeEnvironmentArn"]
        assert f":{region}:" in queues[0]["jobQueueArn"]
        assert f":{region}:" in definitions[0]["jobDefinitionArn"]
        assert f":{region}:" in jobs[0]["jobArn"]


def test_batch_restore_legacy_state_uses_resource_arn_region():
    from ministack.core.responses import (
        AccountScopedDict,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import batch as service

    account_id = "111111111111"
    resource_region = "us-west-2"
    boot_region = "us-east-1"
    stores = {
        "compute_envs": (
            "legacy-ce",
            {
                "computeEnvironmentArn": (
                    f"arn:aws:batch:{resource_region}:{account_id}:"
                    "compute-environment/legacy-ce"
                )
            },
        ),
        "job_queues": (
            "legacy-queue",
            {
                "jobQueueArn": (
                    f"arn:aws:batch:{resource_region}:{account_id}:"
                    "job-queue/legacy-queue"
                )
            },
        ),
        "job_definitions": (
            "legacy-definition",
            [
                {
                    "jobDefinitionArn": (
                        f"arn:aws:batch:{resource_region}:{account_id}:job-definition/legacy-definition:1"
                    )
                }
            ],
        ),
        "jobs": (
            "legacy-job-id",
            {
                "jobArn": (
                    f"arn:aws:batch:{resource_region}:{account_id}:"
                    "job/legacy-job-id"
                )
            },
        ),
    }

    set_request_account_id(account_id)
    set_request_region(boot_region)
    legacy_state = {}
    for state_key, (resource_key, value) in stores.items():
        store = AccountScopedDict()
        store[resource_key] = value
        legacy_state[state_key] = store

    service.reset()
    try:
        service.restore_state(legacy_state)
        for state_key, (resource_key, value) in stores.items():
            store = getattr(service, f"_{state_key}")
            assert store.get_scoped(account_id, resource_region, resource_key) == value
            assert store.get_scoped(account_id, boot_region, resource_key) is None
    finally:
        service.reset()


def test_batch_restore_current_state_preserves_non_boot_regions():
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import batch as service

    account_id = "111111111111"
    boot_region = "us-east-1"
    resource_region = "us-west-2"
    resources = {
        "compute_envs": ("regional-ce", {"state": "ENABLED"}),
        "job_queues": ("regional-queue", {"state": "ENABLED"}),
        "job_definitions": ("regional-definition", [{"revision": 1}]),
        "jobs": ("regional-job", {"status": "SUCCEEDED"}),
    }

    set_request_account_id(account_id)
    set_request_region(boot_region)
    service.reset()
    try:
        for state_key, (resource_key, value) in resources.items():
            getattr(service, f"_{state_key}").set_scoped(
                account_id, resource_region, resource_key, value
            )

        snapshot = service.get_state()
        assert not snapshot["compute_envs"]

        service.reset()
        service.restore_state(snapshot)

        for state_key, (resource_key, value) in resources.items():
            store = getattr(service, f"_{state_key}")
            assert store.get_scoped(
                account_id, resource_region, resource_key
            ) == value
    finally:
        service.reset()
