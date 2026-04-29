import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


@pytest.fixture(scope="module")
def backup():
    return boto3.client(
        "backup",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
    )


def _uid():
    import uuid
    return uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# Vault tests
# ---------------------------------------------------------------------------

def test_backup_create_vault(backup):
    name = f"test-vault-{_uid()}"
    resp = backup.create_backup_vault(BackupVaultName=name)
    assert resp["BackupVaultName"] == name
    assert f"backup-vault:{name}" in resp["BackupVaultArn"]
    assert "CreationDate" in resp


def test_backup_describe_vault(backup):
    name = f"desc-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=name)
    desc = backup.describe_backup_vault(BackupVaultName=name)
    assert desc["BackupVaultName"] == name
    assert f"backup-vault:{name}" in desc["BackupVaultArn"]
    assert desc["NumberOfRecoveryPoints"] == 0


def test_backup_describe_vault_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.describe_backup_vault(BackupVaultName="no-such-vault-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_create_vault_duplicate(backup):
    name = f"dup-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=name)
    with pytest.raises(ClientError) as exc:
        backup.create_backup_vault(BackupVaultName=name)
    assert exc.value.response["Error"]["Code"] == "AlreadyExistsException"


def test_backup_list_vaults(backup):
    name = f"list-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=name)
    resp = backup.list_backup_vaults()
    names = [v["BackupVaultName"] for v in resp["BackupVaultList"]]
    assert name in names


def test_backup_delete_vault(backup):
    name = f"del-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=name)
    backup.delete_backup_vault(BackupVaultName=name)
    with pytest.raises(ClientError) as exc:
        backup.describe_backup_vault(BackupVaultName=name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_delete_vault_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.delete_backup_vault(BackupVaultName="no-such-vault-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Plan tests
# ---------------------------------------------------------------------------

def _plan_body(name):
    return {
        "BackupPlanName": name,
        "Rules": [
            {
                "RuleName": "daily",
                "TargetBackupVaultName": "Default",
                "ScheduleExpression": "cron(0 12 * * ? *)",
            }
        ],
    }


def test_backup_create_plan(backup):
    resp = backup.create_backup_plan(BackupPlan=_plan_body(f"plan-{_uid()}"))
    assert "BackupPlanId" in resp
    assert "BackupPlanArn" in resp
    assert "VersionId" in resp


def test_backup_get_plan(backup):
    name = f"plan-get-{_uid()}"
    create = backup.create_backup_plan(BackupPlan=_plan_body(name))
    plan_id = create["BackupPlanId"]
    resp = backup.get_backup_plan(BackupPlanId=plan_id)
    assert resp["BackupPlanId"] == plan_id
    assert resp["BackupPlan"]["BackupPlanName"] == name


def test_backup_get_plan_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.get_backup_plan(BackupPlanId="no-such-plan-id")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_update_plan(backup):
    name = f"plan-upd-{_uid()}"
    plan_id = backup.create_backup_plan(BackupPlan=_plan_body(name))["BackupPlanId"]
    v1 = backup.get_backup_plan(BackupPlanId=plan_id)["VersionId"]
    backup.update_backup_plan(
        BackupPlanId=plan_id,
        BackupPlan=_plan_body(f"{name}-updated"),
    )
    v2 = backup.get_backup_plan(BackupPlanId=plan_id)["VersionId"]
    assert v1 != v2


def test_backup_list_plan_versions(backup):
    plan_id = backup.create_backup_plan(BackupPlan=_plan_body(f"ver-{_uid()}"))["BackupPlanId"]
    backup.update_backup_plan(BackupPlanId=plan_id, BackupPlan=_plan_body(f"ver-v2-{_uid()}"))
    resp = backup.list_backup_plan_versions(BackupPlanId=plan_id)
    assert len(resp["BackupPlanVersionsList"]) == 2


def test_backup_list_plan_versions_single_version(backup):
    plan_id = backup.create_backup_plan(BackupPlan=_plan_body(f"ver-single-{_uid()}"))["BackupPlanId"]
    resp = backup.list_backup_plan_versions(BackupPlanId=plan_id)
    assert len(resp["BackupPlanVersionsList"]) == 1


def test_backup_list_plan_versions_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.list_backup_plan_versions(BackupPlanId="no-such-plan-id")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_delete_plan_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.delete_backup_plan(BackupPlanId="no-such-plan-id")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_update_plan_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.update_backup_plan(
            BackupPlanId="no-such-plan-id",
            BackupPlan=_plan_body(f"upd-missing-{_uid()}"),
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_list_plans(backup):
    plan_id = backup.create_backup_plan(BackupPlan=_plan_body(f"listed-{_uid()}"))["BackupPlanId"]
    resp = backup.list_backup_plans()
    ids = [p["BackupPlanId"] for p in resp["BackupPlansList"]]
    assert plan_id in ids


def test_backup_delete_plan(backup):
    plan_id = backup.create_backup_plan(BackupPlan=_plan_body(f"del-{_uid()}"))["BackupPlanId"]
    backup.delete_backup_plan(BackupPlanId=plan_id)
    with pytest.raises(ClientError) as exc:
        backup.get_backup_plan(BackupPlanId=plan_id)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Selection tests
# ---------------------------------------------------------------------------

def _make_plan(backup):
    return backup.create_backup_plan(BackupPlan=_plan_body(f"sel-plan-{_uid()}"))["BackupPlanId"]


def test_backup_create_selection_plan_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.create_backup_selection(
            BackupPlanId="no-such-plan-id",
            BackupSelection={
                "SelectionName": "sel-x",
                "IamRoleArn": "arn:aws:iam::000000000000:role/BackupRole",
            },
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_create_selection(backup):
    plan_id = _make_plan(backup)
    resp = backup.create_backup_selection(
        BackupPlanId=plan_id,
        BackupSelection={
            "SelectionName": "sel-1",
            "IamRoleArn": "arn:aws:iam::000000000000:role/BackupRole",
            "Resources": ["arn:aws:dynamodb:us-east-1:000000000000:table/MyTable"],
        },
    )
    assert "SelectionId" in resp
    assert resp["BackupPlanId"] == plan_id


def test_backup_get_selection(backup):
    plan_id = _make_plan(backup)
    sel_id = backup.create_backup_selection(
        BackupPlanId=plan_id,
        BackupSelection={
            "SelectionName": "sel-get",
            "IamRoleArn": "arn:aws:iam::000000000000:role/BackupRole",
        },
    )["SelectionId"]
    resp = backup.get_backup_selection(BackupPlanId=plan_id, SelectionId=sel_id)
    assert resp["SelectionId"] == sel_id
    assert resp["BackupSelection"]["SelectionName"] == "sel-get"


def test_backup_list_selections(backup):
    plan_id = _make_plan(backup)
    sel_id = backup.create_backup_selection(
        BackupPlanId=plan_id,
        BackupSelection={
            "SelectionName": "sel-list",
            "IamRoleArn": "arn:aws:iam::000000000000:role/BackupRole",
        },
    )["SelectionId"]
    resp = backup.list_backup_selections(BackupPlanId=plan_id)
    ids = [s["SelectionId"] for s in resp["BackupSelectionsList"]]
    assert sel_id in ids


def test_backup_list_selections_plan_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.list_backup_selections(BackupPlanId="no-such-plan-id")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_get_selection_not_found(backup):
    plan_id = _make_plan(backup)
    with pytest.raises(ClientError) as exc:
        backup.get_backup_selection(BackupPlanId=plan_id, SelectionId="no-such-sel-id")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_delete_selection(backup):
    plan_id = _make_plan(backup)
    sel_id = backup.create_backup_selection(
        BackupPlanId=plan_id,
        BackupSelection={
            "SelectionName": "sel-del",
            "IamRoleArn": "arn:aws:iam::000000000000:role/BackupRole",
        },
    )["SelectionId"]
    backup.delete_backup_selection(BackupPlanId=plan_id, SelectionId=sel_id)
    with pytest.raises(ClientError) as exc:
        backup.get_backup_selection(BackupPlanId=plan_id, SelectionId=sel_id)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Job tests
# ---------------------------------------------------------------------------

def test_backup_start_job(backup):
    vault_name = f"job-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=vault_name)
    resp = backup.start_backup_job(
        BackupVaultName=vault_name,
        ResourceArn="arn:aws:dynamodb:us-east-1:000000000000:table/MyTable",
        IamRoleArn="arn:aws:iam::000000000000:role/BackupRole",
    )
    assert "BackupJobId" in resp
    assert "RecoveryPointArn" in resp


def test_backup_describe_job(backup):
    vault_name = f"desc-job-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=vault_name)
    job_id = backup.start_backup_job(
        BackupVaultName=vault_name,
        ResourceArn="arn:aws:dynamodb:us-east-1:000000000000:table/MyTable",
        IamRoleArn="arn:aws:iam::000000000000:role/BackupRole",
    )["BackupJobId"]
    resp = backup.describe_backup_job(BackupJobId=job_id)
    assert resp["BackupJobId"] == job_id
    assert resp["State"] == "COMPLETED"
    assert resp["BackupVaultName"] == vault_name


def test_backup_describe_job_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.describe_backup_job(BackupJobId="no-such-job-id")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_list_jobs(backup):
    vault_name = f"list-job-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=vault_name)
    job_id = backup.start_backup_job(
        BackupVaultName=vault_name,
        ResourceArn="arn:aws:s3:::my-bucket",
        IamRoleArn="arn:aws:iam::000000000000:role/BackupRole",
    )["BackupJobId"]
    resp = backup.list_backup_jobs(ByBackupVaultName=vault_name)
    ids = [j["BackupJobId"] for j in resp["BackupJobs"]]
    assert job_id in ids


def test_backup_start_job_vault_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.start_backup_job(
            BackupVaultName="no-such-vault-xyz",
            ResourceArn="arn:aws:s3:::my-bucket",
            IamRoleArn="arn:aws:iam::000000000000:role/BackupRole",
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_start_job_increments_recovery_point_count(backup):
    vault_name = f"rp-count-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=vault_name)
    before = backup.describe_backup_vault(BackupVaultName=vault_name)["NumberOfRecoveryPoints"]
    backup.start_backup_job(
        BackupVaultName=vault_name,
        ResourceArn="arn:aws:dynamodb:us-east-1:000000000000:table/MyTable",
        IamRoleArn="arn:aws:iam::000000000000:role/BackupRole",
    )
    after = backup.describe_backup_vault(BackupVaultName=vault_name)["NumberOfRecoveryPoints"]
    assert after == before + 1


def test_backup_stop_job_not_found(backup):
    with pytest.raises(ClientError) as exc:
        backup.stop_backup_job(BackupJobId="no-such-job-id")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"



def test_backup_list_jobs_by_state(backup):
    vault_name = f"state-filter-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=vault_name)
    job_id = backup.start_backup_job(
        BackupVaultName=vault_name,
        ResourceArn="arn:aws:s3:::my-bucket",
        IamRoleArn="arn:aws:iam::000000000000:role/BackupRole",
    )["BackupJobId"]
    resp = backup.list_backup_jobs(ByState="COMPLETED")
    ids = [j["BackupJobId"] for j in resp["BackupJobs"]]
    assert job_id in ids
    resp_fail = backup.list_backup_jobs(ByState="FAILED")
    ids_fail = [j["BackupJobId"] for j in resp_fail["BackupJobs"]]
    assert job_id not in ids_fail


def test_backup_list_jobs_by_resource_arn(backup):
    vault_name = f"res-filter-vault-{_uid()}"
    resource = f"arn:aws:dynamodb:us-east-1:000000000000:table/table-{_uid()}"
    backup.create_backup_vault(BackupVaultName=vault_name)
    job_id = backup.start_backup_job(
        BackupVaultName=vault_name,
        ResourceArn=resource,
        IamRoleArn="arn:aws:iam::000000000000:role/BackupRole",
    )["BackupJobId"]
    resp = backup.list_backup_jobs(ByResourceArn=resource)
    ids = [j["BackupJobId"] for j in resp["BackupJobs"]]
    assert job_id in ids


def test_backup_stop_job_already_completed(backup):
    vault_name = f"stop-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=vault_name)
    job_id = backup.start_backup_job(
        BackupVaultName=vault_name,
        ResourceArn="arn:aws:s3:::my-bucket",
        IamRoleArn="arn:aws:iam::000000000000:role/BackupRole",
    )["BackupJobId"]
    with pytest.raises(ClientError) as exc:
        backup.stop_backup_job(BackupJobId=job_id)
    assert exc.value.response["Error"]["Code"] == "InvalidRequestException"


# ---------------------------------------------------------------------------
# Tag tests
# ---------------------------------------------------------------------------

def test_backup_tag_vault(backup):
    name = f"tag-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=name)
    arn = f"arn:aws:backup:{REGION}:000000000000:backup-vault:{name}"
    backup.tag_resource(ResourceArn=arn, Tags={"Env": "test", "Team": "platform"})
    resp = backup.list_tags(ResourceArn=arn)
    assert resp["Tags"]["Env"] == "test"
    assert resp["Tags"]["Team"] == "platform"


def test_backup_untag_vault(backup):
    name = f"untag-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=name)
    arn = f"arn:aws:backup:{REGION}:000000000000:backup-vault:{name}"
    backup.tag_resource(ResourceArn=arn, Tags={"Env": "test", "Team": "platform"})
    backup.untag_resource(ResourceArn=arn, TagKeyList=["Team"])
    resp = backup.list_tags(ResourceArn=arn)
    assert "Team" not in resp["Tags"]
    assert resp["Tags"]["Env"] == "test"


def test_backup_tag_plan(backup):
    plan_id = backup.create_backup_plan(BackupPlan=_plan_body(f"tag-plan-{_uid()}"))["BackupPlanId"]
    arn = f"arn:aws:backup:{REGION}:000000000000:backup-plan:{plan_id}"
    backup.tag_resource(ResourceArn=arn, Tags={"Owner": "data-team"})
    resp = backup.list_tags(ResourceArn=arn)
    assert resp["Tags"]["Owner"] == "data-team"


def test_backup_tag_not_found(backup):
    arn = "arn:aws:backup:us-east-1:000000000000:backup-vault:no-such-vault"
    with pytest.raises(ClientError) as exc:
        backup.tag_resource(ResourceArn=arn, Tags={"k": "v"})
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_untag_resource_not_found(backup):
    arn = "arn:aws:backup:us-east-1:000000000000:backup-vault:no-such-vault"
    with pytest.raises(ClientError) as exc:
        backup.untag_resource(ResourceArn=arn, TagKeyList=["k"])
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_list_tags_not_found(backup):
    arn = "arn:aws:backup:us-east-1:000000000000:backup-vault:no-such-vault"
    with pytest.raises(ClientError) as exc:
        backup.list_tags(ResourceArn=arn)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_backup_list_tags_empty(backup):
    name = f"empty-tag-vault-{_uid()}"
    backup.create_backup_vault(BackupVaultName=name)
    arn = f"arn:aws:backup:{REGION}:000000000000:backup-vault:{name}"
    resp = backup.list_tags(ResourceArn=arn)
    assert resp["Tags"] == {}
