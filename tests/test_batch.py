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


# ---------------------------------------------------------------------------
# SubmitJob host-resource gate (issue #650)
# ---------------------------------------------------------------------------

def _register(batch, name, reqs=None):
    container = {"image": "alpine:3.20", "command": ["echo", "hi"]}
    if reqs is not None:
        container["resourceRequirements"] = reqs
    return batch.register_job_definition(
        jobDefinitionName=name, type="container", containerProperties=container,
    )


def test_batch_submit_job_refused_when_jobdef_exceeds_host(batch):
    """Issue #650: jobdef requesting more resources than MS' host can provide
    must surface at the SubmitJob call (400 ClientException) — not silently
    return SUCCEEDED for a job that could never run.
    """
    from botocore.exceptions import ClientError
    name = f"oversize-{_uid()}"
    _register(batch, name, reqs=[
        {"type": "VCPU", "value": "256"},     # impossibly high
        {"type": "MEMORY", "value": "9999999"},
    ])
    with pytest.raises(ClientError) as exc:
        batch.submit_job(jobName=f"r-{_uid()}", jobQueue="q",
                         jobDefinition=f"{name}:1")
    err = exc.value.response["Error"]
    assert err["Code"] == "ClientException"
    msg = err["Message"]
    assert "SubmitJob refused" in msg
    assert "VCPU=256" in msg
    assert "MEMORY=9999999" in msg


def test_batch_submit_job_vcpu_only_over_fails(batch):
    """When only VCPU exceeds, the message reports only VCPU."""
    from botocore.exceptions import ClientError
    name = f"vcpu-{_uid()}"
    _register(batch, name, reqs=[
        {"type": "VCPU", "value": "256"},
        {"type": "MEMORY", "value": "64"},
    ])
    with pytest.raises(ClientError) as exc:
        batch.submit_job(jobName=f"r-{_uid()}", jobQueue="q",
                         jobDefinition=f"{name}:1")
    msg = exc.value.response["Error"]["Message"]
    assert "VCPU=256" in msg
    assert "MEMORY" not in msg  # only the over-capacity dimension surfaces


def test_batch_submit_job_memory_only_over_fails(batch):
    from botocore.exceptions import ClientError
    name = f"mem-{_uid()}"
    _register(batch, name, reqs=[
        {"type": "VCPU", "value": "1"},
        {"type": "MEMORY", "value": "9999999"},
    ])
    with pytest.raises(ClientError) as exc:
        batch.submit_job(jobName=f"r-{_uid()}", jobQueue="q",
                         jobDefinition=f"{name}:1")
    msg = exc.value.response["Error"]["Message"]
    assert "MEMORY=9999999" in msg
    assert "VCPU" not in msg


def test_batch_submit_job_fitting_resources_succeeds(batch):
    """Regression: a jobdef well within the host's ceiling must still pass
    through to the stub-SUCCEEDED behavior MS has always had.
    """
    name = f"fit-{_uid()}"
    _register(batch, name, reqs=[
        {"type": "VCPU", "value": "1"},
        {"type": "MEMORY", "value": "128"},
    ])
    resp = batch.submit_job(jobName=f"r-{_uid()}", jobQueue="q",
                            jobDefinition=f"{name}:1")
    assert "jobId" in resp
    desc = batch.describe_jobs(jobs=[resp["jobId"]])["jobs"][0]
    assert desc["status"] == "SUCCEEDED"


def test_batch_submit_job_no_resource_requirements_succeeds(batch):
    """A jobdef without `resourceRequirements` has nothing to compare against;
    pass through unchanged.
    """
    name = f"noreqs-{_uid()}"
    _register(batch, name, reqs=None)
    resp = batch.submit_job(jobName=f"r-{_uid()}", jobQueue="q",
                            jobDefinition=f"{name}:1")
    assert "jobId" in resp


def test_batch_submit_job_unknown_jobdef_passes_through(batch):
    """When the `jobDefinition` reference can't be resolved, MS preserves its
    existing permissive behavior (don't introduce a *new* failure mode while
    fixing a different one).
    """
    resp = batch.submit_job(jobName=f"r-{_uid()}", jobQueue="q",
                            jobDefinition="does-not-exist:99")
    assert "jobId" in resp


def test_batch_submit_job_resolves_by_name_takes_latest_revision(batch):
    """`jobDefinition='name'` (no `:rev`) resolves to the latest revision."""
    name = f"byname-{_uid()}"
    _register(batch, name, reqs=[{"type": "VCPU", "value": "1"},
                                  {"type": "MEMORY", "value": "128"}])
    # Bump revision; the latest one is still small, gate must still pass.
    _register(batch, name, reqs=[{"type": "VCPU", "value": "1"},
                                  {"type": "MEMORY", "value": "64"}])
    resp = batch.submit_job(jobName=f"r-{_uid()}", jobQueue="q",
                            jobDefinition=name)  # no :rev
    assert "jobId" in resp


def test_batch_resource_check_skipped_when_host_unmeasurable(monkeypatch):
    """If host detection returns None for every dimension (exotic platform),
    the gate must skip silently — no false-positive refusals.
    """
    from ministack.services import batch as batch_mod
    monkeypatch.setattr(
        batch_mod, "_detect_host_limits",
        lambda: {"vcpu": None, "memory_mib": None},
    )
    container = {
        "image": "alpine:3.20",
        "command": ["echo", "hi"],
        "resourceRequirements": [
            {"type": "VCPU", "value": "9999"},
            {"type": "MEMORY", "value": "9999999"},
        ],
    }
    assert batch_mod._check_host_fit(container) is None


def test_batch_resource_check_skipped_when_jobdef_dimension_unmeasurable(monkeypatch):
    """If the jobdef declares one dimension but not the other, only the
    declared one is compared — no spurious failures from absent fields.
    """
    from ministack.services import batch as batch_mod
    monkeypatch.setattr(
        batch_mod, "_detect_host_limits",
        lambda: {"vcpu": 2, "memory_mib": 1024},
    )
    container = {
        "image": "alpine:3.20",
        "command": ["echo", "hi"],
        "resourceRequirements": [{"type": "VCPU", "value": "1"}],  # only VCPU
    }
    assert batch_mod._check_host_fit(container) is None
