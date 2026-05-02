import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


def test_secretsmanager_resource_policy(sm):
    sm.create_secret(Name="sm-pol-sec", SecretString="secret-val")
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": "*",
                }
            ],
        }
    )
    sm.put_resource_policy(SecretId="sm-pol-sec", ResourcePolicy=policy)
    resp = sm.get_resource_policy(SecretId="sm-pol-sec")
    assert resp["Name"] == "sm-pol-sec"
    assert "ResourcePolicy" in resp
    sm.delete_resource_policy(SecretId="sm-pol-sec")

def test_secretsmanager_validate_resource_policy(sm):
    policy = json.dumps({"Version": "2012-10-17", "Statement": []})
    resp = sm.validate_resource_policy(ResourcePolicy=policy)
    assert resp["PolicyValidationPassed"] is True

def test_secretsmanager_rotate_secret(sm):
    """RotateSecret creates a new version and promotes it to AWSCURRENT."""
    sm.create_secret(Name="rotate-test-v39", SecretString="original")
    resp = sm.rotate_secret(
        SecretId="rotate-test-v39",
        RotationLambdaARN="arn:aws:lambda:us-east-1:000000000000:function:rotator",
        RotationRules={"AutomaticallyAfterDays": 30},
    )
    assert "VersionId" in resp
    desc = sm.describe_secret(SecretId="rotate-test-v39")
    assert desc["RotationEnabled"] is True
    assert desc["RotationLambdaARN"] == "arn:aws:lambda:us-east-1:000000000000:function:rotator"
    current = sm.get_secret_value(SecretId="rotate-test-v39", VersionStage="AWSCURRENT")
    assert current["SecretString"] == "original"
    sm.delete_secret(SecretId="rotate-test-v39", ForceDeleteWithoutRecovery=True)

# Migrated from test_secrets.py
def test_secretsmanager_create_get(sm):
    sm.create_secret(Name="test-secret-1", SecretString='{"user":"admin"}')
    resp = sm.get_secret_value(SecretId="test-secret-1")
    assert json.loads(resp["SecretString"])["user"] == "admin"

def test_secretsmanager_update_list(sm):
    sm.create_secret(Name="test-secret-2", SecretString="original")
    sm.update_secret(SecretId="test-secret-2", SecretString="updated")
    resp = sm.get_secret_value(SecretId="test-secret-2")
    assert resp["SecretString"] == "updated"
    listed = sm.list_secrets()
    assert any(s["Name"] == "test-secret-2" for s in listed["SecretList"])

def test_secretsmanager_create_get_v2(sm):
    sm.create_secret(Name="sm-cg-v2", SecretString='{"user":"admin","pass":"s3cr3t"}')
    resp = sm.get_secret_value(SecretId="sm-cg-v2")
    parsed = json.loads(resp["SecretString"])
    assert parsed["user"] == "admin"
    assert parsed["pass"] == "s3cr3t"
    assert "VersionId" in resp
    assert "ARN" in resp

    sm.create_secret(Name="sm-cg-bin", SecretBinary=b"\x00\x01\x02")
    resp_bin = sm.get_secret_value(SecretId="sm-cg-bin")
    assert resp_bin["SecretBinary"] == b"\x00\x01\x02"

def test_secretsmanager_update_v2(sm):
    sm.create_secret(Name="sm-upd-v2", SecretString="original")
    sm.update_secret(SecretId="sm-upd-v2", SecretString="updated", Description="new desc")
    resp = sm.get_secret_value(SecretId="sm-upd-v2")
    assert resp["SecretString"] == "updated"
    desc = sm.describe_secret(SecretId="sm-upd-v2")
    assert desc["Description"] == "new desc"

def test_secretsmanager_list_v2(sm):
    sm.create_secret(Name="sm-list-a", SecretString="a")
    sm.create_secret(Name="sm-list-b", SecretString="b")
    listed = sm.list_secrets()
    names = [s["Name"] for s in listed["SecretList"]]
    assert "sm-list-a" in names
    assert "sm-list-b" in names

def test_secretsmanager_delete_v2(sm):
    sm.create_secret(Name="sm-del-v2", SecretString="gone")
    sm.delete_secret(SecretId="sm-del-v2", ForceDeleteWithoutRecovery=True)
    with pytest.raises(ClientError) as exc:
        sm.get_secret_value(SecretId="sm-del-v2")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    # Real AWS sends `x-amzn-errortype` on JSON-protocol errors; Java/Go SDK v2 read it.
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "ResourceNotFoundException"

def test_secretsmanager_delete_with_recovery(sm):
    sm.create_secret(Name="sm-del-rec", SecretString="recoverable")
    sm.delete_secret(SecretId="sm-del-rec", RecoveryWindowInDays=7)
    with pytest.raises(ClientError) as exc:
        sm.get_secret_value(SecretId="sm-del-rec")
    assert (
        "marked for deletion" in exc.value.response["Error"]["Message"].lower()
        or exc.value.response["Error"]["Code"] == "InvalidRequestException"
    )
    desc = sm.describe_secret(SecretId="sm-del-rec")
    assert "DeletedDate" in desc

    sm.restore_secret(SecretId="sm-del-rec")
    resp = sm.get_secret_value(SecretId="sm-del-rec")
    assert resp["SecretString"] == "recoverable"

def test_secretsmanager_put_value_version_stages_v2(sm):
    sm.create_secret(Name="sm-pvs-v2", SecretString="v1")
    sm.put_secret_value(SecretId="sm-pvs-v2", SecretString="v2")

    desc = sm.describe_secret(SecretId="sm-pvs-v2")
    stages = desc["VersionIdsToStages"]
    current_vids = [vid for vid, s in stages.items() if "AWSCURRENT" in s]
    previous_vids = [vid for vid, s in stages.items() if "AWSPREVIOUS" in s]
    assert len(current_vids) == 1
    assert len(previous_vids) == 1
    assert current_vids[0] != previous_vids[0]

    cur = sm.get_secret_value(SecretId="sm-pvs-v2", VersionStage="AWSCURRENT")
    assert cur["SecretString"] == "v2"
    prev = sm.get_secret_value(SecretId="sm-pvs-v2", VersionStage="AWSPREVIOUS")
    assert prev["SecretString"] == "v1"

def test_secretsmanager_describe_v2(sm):
    sm.create_secret(
        Name="sm-dsc-v2",
        SecretString="val",
        Description="detailed desc",
        Tags=[{"Key": "Env", "Value": "dev"}],
    )
    resp = sm.describe_secret(SecretId="sm-dsc-v2")
    assert resp["Name"] == "sm-dsc-v2"
    assert resp["Description"] == "detailed desc"
    assert any(t["Key"] == "Env" for t in resp["Tags"])
    assert "VersionIdsToStages" in resp
    assert "ARN" in resp

def test_secretsmanager_tags_v2(sm):
    sm.create_secret(Name="sm-tag-v2", SecretString="val")
    sm.tag_resource(SecretId="sm-tag-v2", Tags=[{"Key": "team", "Value": "backend"}])
    sm.tag_resource(SecretId="sm-tag-v2", Tags=[{"Key": "env", "Value": "prod"}])

    desc = sm.describe_secret(SecretId="sm-tag-v2")
    assert any(t["Key"] == "team" and t["Value"] == "backend" for t in desc["Tags"])
    assert any(t["Key"] == "env" and t["Value"] == "prod" for t in desc["Tags"])

    sm.untag_resource(SecretId="sm-tag-v2", TagKeys=["team"])
    desc2 = sm.describe_secret(SecretId="sm-tag-v2")
    assert not any(t["Key"] == "team" for t in desc2.get("Tags", []))
    assert any(t["Key"] == "env" for t in desc2.get("Tags", []))

def test_secretsmanager_get_random_password_v2(sm):
    resp = sm.get_random_password(PasswordLength=32)
    assert len(resp["RandomPassword"]) == 32

    resp2 = sm.get_random_password(PasswordLength=20, ExcludeCharacters="aeiou")
    pw = resp2["RandomPassword"]
    assert len(pw) == 20
    for c in "aeiou":
        assert c not in pw


# Migrated from test_sm.py
def test_secretsmanager_put_secret_value_stages(sm):
    """PutSecretValue stages manage AWSCURRENT/AWSPREVIOUS correctly."""
    sm.create_secret(Name="qa-sm-stages", SecretString="v1")
    sm.put_secret_value(SecretId="qa-sm-stages", SecretString="v2")
    sm.put_secret_value(SecretId="qa-sm-stages", SecretString="v3")
    current = sm.get_secret_value(SecretId="qa-sm-stages", VersionStage="AWSCURRENT")
    assert current["SecretString"] == "v3"
    previous = sm.get_secret_value(SecretId="qa-sm-stages", VersionStage="AWSPREVIOUS")
    assert previous["SecretString"] == "v2"

def test_secretsmanager_list_secret_version_ids(sm):
    """ListSecretVersionIds returns all versions."""
    sm.create_secret(Name="qa-sm-versions", SecretString="initial")
    sm.put_secret_value(SecretId="qa-sm-versions", SecretString="second")
    resp = sm.list_secret_version_ids(SecretId="qa-sm-versions")
    assert len(resp["Versions"]) >= 2

def test_secretsmanager_update_secret_version_stage_moves_current(sm):
    """UpdateSecretVersionStage can move AWSCURRENT and refresh AWSPREVIOUS."""
    first = sm.create_secret(Name="qa-sm-stage-move-current", SecretString="v1")
    first_vid = first["VersionId"]
    second_vid = "22222222-2222-2222-2222-222222222222"
    sm.put_secret_value(
        SecretId="qa-sm-stage-move-current",
        SecretString="v2",
        ClientRequestToken=second_vid,
    )

    sm.update_secret_version_stage(
        SecretId="qa-sm-stage-move-current",
        VersionStage="AWSCURRENT",
        RemoveFromVersionId=second_vid,
        MoveToVersionId=first_vid,
    )

    current = sm.get_secret_value(SecretId="qa-sm-stage-move-current", VersionStage="AWSCURRENT")
    assert current["SecretString"] == "v1"
    previous = sm.get_secret_value(SecretId="qa-sm-stage-move-current", VersionStage="AWSPREVIOUS")
    assert previous["SecretString"] == "v2"

    versions = sm.list_secret_version_ids(SecretId="qa-sm-stage-move-current")["Versions"]
    version_stages = {v["VersionId"]: set(v["VersionStages"]) for v in versions}
    assert version_stages[first_vid] == {"AWSCURRENT"}
    assert version_stages[second_vid] == {"AWSPREVIOUS"}

def test_secretsmanager_update_secret_version_stage_moves_and_removes_custom_label(sm):
    """UpdateSecretVersionStage can move a custom label and then detach it."""
    first = sm.create_secret(Name="qa-sm-stage-custom", SecretString="v1")
    first_vid = first["VersionId"]
    second_vid = "33333333-3333-3333-3333-333333333333"
    sm.put_secret_value(
        SecretId="qa-sm-stage-custom",
        SecretString="v2",
        ClientRequestToken=second_vid,
        VersionStages=["BLUE"],
    )

    before = sm.get_secret_value(SecretId="qa-sm-stage-custom", VersionStage="BLUE")
    assert before["SecretString"] == "v2"

    sm.update_secret_version_stage(
        SecretId="qa-sm-stage-custom",
        VersionStage="BLUE",
        RemoveFromVersionId=second_vid,
        MoveToVersionId=first_vid,
    )

    moved = sm.get_secret_value(SecretId="qa-sm-stage-custom", VersionStage="BLUE")
    assert moved["SecretString"] == "v1"

    sm.update_secret_version_stage(
        SecretId="qa-sm-stage-custom",
        VersionStage="BLUE",
        RemoveFromVersionId=first_vid,
    )

    versions = sm.list_secret_version_ids(SecretId="qa-sm-stage-custom")["Versions"]
    version_stages = {v["VersionId"]: set(v["VersionStages"]) for v in versions}
    assert "BLUE" not in version_stages[first_vid]
    assert "BLUE" not in version_stages[second_vid]

    with pytest.raises(ClientError) as exc:
        sm.get_secret_value(SecretId="qa-sm-stage-custom", VersionStage="BLUE")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

def test_secretsmanager_update_secret_version_stage_requires_matching_remove_version(sm):
    """Moving an attached label requires RemoveFromVersionId to match the current owner."""
    first = sm.create_secret(Name="qa-sm-stage-guard", SecretString="v1")
    first_vid = first["VersionId"]
    second_vid = "44444444-4444-4444-4444-444444444444"
    sm.put_secret_value(
        SecretId="qa-sm-stage-guard",
        SecretString="v2",
        ClientRequestToken=second_vid,
    )

    with pytest.raises(ClientError) as exc:
        sm.update_secret_version_stage(
            SecretId="qa-sm-stage-guard",
            VersionStage="AWSCURRENT",
            MoveToVersionId=first_vid,
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"

def test_secretsmanager_delete_and_restore(sm):
    """DeleteSecret schedules deletion; RestoreSecret cancels it."""
    sm.create_secret(Name="qa-sm-restore", SecretString="data")
    sm.delete_secret(SecretId="qa-sm-restore", RecoveryWindowInDays=7)
    with pytest.raises(ClientError) as exc:
        sm.get_secret_value(SecretId="qa-sm-restore")
    assert exc.value.response["Error"]["Code"] == "InvalidRequestException"
    sm.restore_secret(SecretId="qa-sm-restore")
    val = sm.get_secret_value(SecretId="qa-sm-restore")
    assert val["SecretString"] == "data"

def test_secretsmanager_get_random_password(sm):
    """GetRandomPassword returns a password of the requested length."""
    resp = sm.get_random_password(PasswordLength=24, ExcludeNumbers=True)
    pwd = resp["RandomPassword"]
    assert len(pwd) == 24
    assert not any(c.isdigit() for c in pwd)

def test_secretsmanager_batch_get_secret_value(sm):
    sm.create_secret(Name="batch-s1", SecretString="val1")
    sm.create_secret(Name="batch-s2", SecretString="val2")
    resp = sm.batch_get_secret_value(SecretIdList=["batch-s1", "batch-s2"])
    assert len(resp["SecretValues"]) == 2
    names = {s["Name"] for s in resp["SecretValues"]}
    assert "batch-s1" in names
    assert "batch-s2" in names
    assert len(resp.get("Errors", [])) == 0

def test_secretsmanager_batch_get_secret_value_with_missing(sm):
    resp = sm.batch_get_secret_value(SecretIdList=["batch-s1", "nonexistent-secret"])
    assert len(resp["SecretValues"]) == 1
    assert len(resp["Errors"]) == 1
    assert resp["Errors"][0]["SecretId"] == "nonexistent-secret"

def test_secretsmanager_kms_key_id_on_create_and_describe(sm):
    sm.create_secret(Name="kms-test-secret", SecretString="val", KmsKeyId="alias/my-key")
    resp = sm.describe_secret(SecretId="kms-test-secret")
    assert resp["KmsKeyId"] == "alias/my-key"

def test_secretsmanager_kms_key_id_on_update(sm):
    sm.update_secret(SecretId="kms-test-secret", KmsKeyId="alias/other-key")
    resp = sm.describe_secret(SecretId="kms-test-secret")
    assert resp["KmsKeyId"] == "alias/other-key"


def test_secretsmanager_get_by_partial_arn(sm):
    """GetSecretValue with a partial ARN (no random suffix) must resolve the secret."""
    import uuid as _uuid
    name = f"partial-arn-test/{_uuid.uuid4().hex[:8]}"
    created = sm.create_secret(Name=name, SecretString="partial-arn-value")
    full_arn = created["ARN"]

    # Full ARN works
    assert sm.get_secret_value(SecretId=full_arn)["SecretString"] == "partial-arn-value"

    # Partial ARN: strip the random suffix (last hyphen + 6 chars)
    partial_arn = full_arn.rsplit("-", 1)[0]
    assert partial_arn != full_arn
    assert sm.get_secret_value(SecretId=partial_arn)["SecretString"] == "partial-arn-value"


def test_secretsmanager_list_include_planned_deletion(sm):
    """ListSecrets honors IncludePlannedDeletion per the AWS SecretListEntry spec.

    When a secret is soft-deleted (scheduled for deletion with a recovery
    window), it must:
      - be hidden from ListSecrets by default, and
      - be returned by ListSecrets(IncludePlannedDeletion=True) with its
        DeletedDate populated so clients can distinguish it.
    """
    sm.create_secret(Name="sm-list-pd", SecretString="soft")
    sm.delete_secret(SecretId="sm-list-pd", RecoveryWindowInDays=7)

    # Default: soft-deleted secret is hidden.
    names = [s["Name"] for s in sm.list_secrets()["SecretList"]]
    assert "sm-list-pd" not in names

    # IncludePlannedDeletion=True: soft-deleted secret is visible with DeletedDate.
    resp = sm.list_secrets(IncludePlannedDeletion=True)
    entry = next((s for s in resp["SecretList"] if s["Name"] == "sm-list-pd"), None)
    assert entry is not None
    assert "DeletedDate" in entry



# ========== from test_misc_medium_low_fixes.py ==========
# ForceDeleteWithoutRecovery must clean up the orphan _resource_policies[arn]
# entry, not just the _secrets[name] entry. Tests the in-process module
# directly so it can observe both dicts together.

import asyncio as _sm_asyncio
import importlib as _sm_importlib
import json as _sm_json


def _sm_invoke_action(mod, action, params):
    """Run a service module's action handler synchronously, return raw JSON body."""
    headers = {"x-amz-target": f"secretsmanager.{action}"}
    body = _sm_json.dumps(params).encode()
    status, _resp_headers, resp_body = _sm_asyncio.run(
        mod.handle_request("POST", "/", headers, body, {})
    )
    if status >= 300:
        raise AssertionError(f"{action} failed: {status} {resp_body!r}")
    return resp_body.decode() if isinstance(resp_body, bytes) else resp_body


def test_secretsmanager_force_delete_removes_resource_policy():
    sm = _sm_importlib.import_module("ministack.services.secretsmanager")
    sm.reset()

    create_resp = _sm_json.loads(_sm_invoke_action(
        sm, "CreateSecret",
        {"Name": "force-delete-canary", "SecretString": "x"},
    ))
    arn = create_resp["ARN"]
    _sm_invoke_action(sm, "PutResourcePolicy", {
        "SecretId": arn,
        "ResourcePolicy": '{"Version":"2012-10-17","Statement":[]}',
    })
    assert arn in sm._resource_policies, "Test setup failed — policy didn't register"

    _sm_invoke_action(sm, "DeleteSecret", {
        "SecretId": arn,
        "ForceDeleteWithoutRecovery": True,
    })

    assert arn not in sm._resource_policies, (
        "Force-deleting a secret left an orphan entry in _resource_policies "
        "keyed by the now-deleted ARN. The delete path must pop both "
        "_secrets[name] AND _resource_policies[arn]."
    )
    sm.reset()
