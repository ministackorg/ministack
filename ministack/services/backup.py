"""
AWS Backup Service Emulator.
REST/JSON protocol — /backup-vaults/*, /backup/plans/*, /backup-jobs/*, /tags/* paths.

Supports:
  Vaults:     CreateBackupVault, DescribeBackupVault, DeleteBackupVault, ListBackupVaults
  Plans:      CreateBackupPlan, GetBackupPlan, UpdateBackupPlan, DeleteBackupPlan,
              ListBackupPlans, ListBackupPlanVersions
  Selections: CreateBackupSelection, GetBackupSelection, DeleteBackupSelection,
              ListBackupSelections
  Jobs:       StartBackupJob, StopBackupJob, DescribeBackupJob, ListBackupJobs
  Tags:       TagResource, UntagResource, ListTags
"""

import copy
import json
import logging
import time

from ministack.core.responses import AccountScopedDict, get_account_id, get_region, new_uuid
from ministack.core.persistence import load_state

logger = logging.getLogger("backup")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_vaults     = AccountScopedDict()     # vault_name -> vault record
_plans      = AccountScopedDict()     # plan_id -> plan record
_selections = AccountScopedDict()     # selection_id -> selection record
_jobs       = AccountScopedDict()     # job_id -> job record


def reset():
    _vaults.clear()
    _plans.clear()
    _selections.clear()
    _jobs.clear()


def get_state():
    # Preserve AccountScopedDict wrappers; casting to a plain dict drops
    # the per-account scoping and would persist only the current request's
    # tenants. AccountScopedDict has a JSON encoder hook that round-trips
    # the (account, key) tuple correctly.
    return {
        "vaults":     copy.deepcopy(_vaults),
        "plans":      copy.deepcopy(_plans),
        "selections": copy.deepcopy(_selections),
        "jobs":       copy.deepcopy(_jobs),
    }


def restore_state(data):
    _vaults.update(data.get("vaults", {}))
    _plans.update(data.get("plans", {}))
    _selections.update(data.get("selections", {}))
    _jobs.update(data.get("jobs", {}))


def load_persisted_state(data):
    restore_state(data)


try:
    _restored = load_state("backup")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted backup state; continuing fresh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def _epoch():
    return time.time()


def _vault_arn(name):
    return f"arn:aws:backup:{get_region()}:{get_account_id()}:backup-vault:{name}"


def _plan_arn(plan_id):
    return f"arn:aws:backup:{get_region()}:{get_account_id()}:backup-plan:{plan_id}"


def _selection_arn(plan_id, selection_id):
    return f"arn:aws:backup:{get_region()}:{get_account_id()}:backup-selection:{plan_id}/{selection_id}"


def _job_arn(job_id):
    return f"arn:aws:backup:{get_region()}:{get_account_id()}:backup-job:{job_id}"


def _recovery_point_arn(vault_name):
    return (
        f"arn:aws:backup:{get_region()}:{get_account_id()}:"
        f"recovery-point:{vault_name}/{new_uuid()}"
    )


def _ok(body):
    return 200, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _no_content():
    return 200, {"Content-Type": "application/json"}, b""


def _err(code, msg, status=400):
    return (
        status,
        {"Content-Type": "application/json"},
        json.dumps({"__type": code, "Message": msg}).encode(),
    )


def _paginate(items, query, key):
    """Simple token-based pagination over a list."""
    max_results = int(query.get("maxResults", query.get("MaxResults", 100)))
    next_token = query.get("nextToken", query.get("NextToken", ""))
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = items[start:start + max_results]
    out = {key: page}
    if start + max_results < len(items):
        out["NextToken"] = str(start + max_results)
    return _ok(out)


# ---------------------------------------------------------------------------
# Vaults
# ---------------------------------------------------------------------------

def _create_vault(name, body):
    if name in _vaults:
        return _err("AlreadyExistsException", f"Vault '{name}' already exists.", 409)
    vault = {
        "BackupVaultName": name,
        "BackupVaultArn": _vault_arn(name),
        "CreationDate": _epoch(),
        "EncryptionKeyArn": body.get("EncryptionKeyArn", ""),
        "BackupVaultTags": body.get("BackupVaultTags", {}),
        "CreatorRequestId": body.get("CreatorRequestId", ""),
        "NumberOfRecoveryPoints": 0,
        "Locked": False,
    }
    _vaults[name] = vault
    logger.info("CreateBackupVault: %s", name)
    return _ok({
        "BackupVaultName": vault["BackupVaultName"],
        "BackupVaultArn": vault["BackupVaultArn"],
        "CreationDate": vault["CreationDate"],
    })


def _describe_vault(name):
    v = _vaults.get(name)
    if v is None:
        return _err("ResourceNotFoundException", f"Vault '{name}' not found.", 404)
    return _ok(copy.copy(v))


def _delete_vault(name):
    if name not in _vaults:
        return _err("ResourceNotFoundException", f"Vault '{name}' not found.", 404)
    del _vaults[name]
    logger.info("DeleteBackupVault: %s", name)
    return _no_content()


def _list_vaults(query):
    items = list(_vaults.values())
    return _paginate(items, query, "BackupVaultList")


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------

def _create_plan(body):
    plan_data = body.get("BackupPlan", {})
    if not plan_data:
        return _err("InvalidParameterValueException", "BackupPlan is required.", 400)
    plan_id = new_uuid()
    version_id = new_uuid()
    now = _epoch()
    record = {
        "BackupPlanId": plan_id,
        "BackupPlanArn": _plan_arn(plan_id),
        "BackupPlanName": plan_data.get("BackupPlanName", ""),
        "CreationDate": now,
        "LastExecutionDate": now,
        "VersionId": version_id,
        "BackupPlan": plan_data,
        "Tags": body.get("BackupPlanTags", {}),
        "AdvancedBackupSettings": plan_data.get("AdvancedBackupSettings", []),
        "Versions": [{
            "BackupPlanId": plan_id,
            "BackupPlanArn": _plan_arn(plan_id),
            "BackupPlanName": plan_data.get("BackupPlanName", ""),
            "CreationDate": now,
            "VersionId": version_id,
            "BackupPlan": copy.deepcopy(plan_data),
        }],
    }
    _plans[plan_id] = record
    logger.info("CreateBackupPlan: %s (%s)", plan_data.get("BackupPlanName"), plan_id)
    return _ok({
        "BackupPlanId": plan_id,
        "BackupPlanArn": record["BackupPlanArn"],
        "CreationDate": now,
        "VersionId": version_id,
    })


def _get_plan(plan_id):
    p = _plans.get(plan_id)
    if p is None:
        return _err("ResourceNotFoundException", f"Plan '{plan_id}' not found.", 404)
    return _ok({
        "BackupPlanId": p["BackupPlanId"],
        "BackupPlanArn": p["BackupPlanArn"],
        "BackupPlanName": p["BackupPlanName"],
        "CreationDate": p["CreationDate"],
        "LastExecutionDate": p["LastExecutionDate"],
        "VersionId": p["VersionId"],
        "BackupPlan": p["BackupPlan"],
        "AdvancedBackupSettings": p["AdvancedBackupSettings"],
    })


def _update_plan(plan_id, body):
    p = _plans.get(plan_id)
    if p is None:
        return _err("ResourceNotFoundException", f"Plan '{plan_id}' not found.", 404)
    plan_data = body.get("BackupPlan", {})
    if not plan_data:
        return _err("InvalidParameterValueException", "BackupPlan is required.", 400)
    new_version = new_uuid()
    now = _epoch()
    p["BackupPlan"] = plan_data
    p["BackupPlanName"] = plan_data.get("BackupPlanName", p["BackupPlanName"])
    p["AdvancedBackupSettings"] = plan_data.get("AdvancedBackupSettings", [])
    p["VersionId"] = new_version
    p["LastExecutionDate"] = now
    p["Versions"].append({
        "BackupPlanId": plan_id,
        "BackupPlanArn": p["BackupPlanArn"],
        "BackupPlanName": p["BackupPlanName"],
        "CreationDate": now,
        "VersionId": new_version,
        "BackupPlan": copy.deepcopy(plan_data),
    })
    logger.info("UpdateBackupPlan: %s", plan_id)
    return _ok({
        "BackupPlanId": plan_id,
        "BackupPlanArn": p["BackupPlanArn"],
        "CreationDate": p["CreationDate"],
        "VersionId": new_version,
    })


def _delete_plan(plan_id):
    p = _plans.get(plan_id)
    if p is None:
        return _err("ResourceNotFoundException", f"Plan '{plan_id}' not found.", 404)
    del _plans[plan_id]
    logger.info("DeleteBackupPlan: %s", plan_id)
    return _ok({
        "BackupPlanId": plan_id,
        "BackupPlanArn": p["BackupPlanArn"],
        "DeletionDate": _epoch(),
        "VersionId": p["VersionId"],
    })


def _list_plans(query):
    items = [
        {
            "BackupPlanId": p["BackupPlanId"],
            "BackupPlanArn": p["BackupPlanArn"],
            "BackupPlanName": p["BackupPlanName"],
            "CreationDate": p["CreationDate"],
            "LastExecutionDate": p["LastExecutionDate"],
            "VersionId": p["VersionId"],
        }
        for p in _plans.values()
    ]
    return _paginate(items, query, "BackupPlansList")


def _list_plan_versions(plan_id):
    p = _plans.get(plan_id)
    if p is None:
        return _err("ResourceNotFoundException", f"Plan '{plan_id}' not found.", 404)
    return _ok({"BackupPlanVersionsList": p["Versions"]})


# ---------------------------------------------------------------------------
# Selections
# ---------------------------------------------------------------------------

def _create_selection(plan_id, body):
    if plan_id not in _plans:
        return _err("ResourceNotFoundException", f"Plan '{plan_id}' not found.", 404)
    sel = body.get("BackupSelection", {})
    if not sel:
        return _err("InvalidParameterValueException", "BackupSelection is required.", 400)
    selection_id = new_uuid()
    record = {
        "SelectionId": selection_id,
        "SelectionName": sel.get("SelectionName", ""),
        "BackupPlanId": plan_id,
        "IamRoleArn": sel.get("IamRoleArn", ""),
        "Resources": sel.get("Resources", []),
        "ListOfTags": sel.get("ListOfTags", []),
        "NotResources": sel.get("NotResources", []),
        "Conditions": sel.get("Conditions", {}),
        "CreationDate": _epoch(),
        "CreatorRequestId": body.get("CreatorRequestId", ""),
    }
    _selections[selection_id] = record
    logger.info("CreateBackupSelection: %s under plan %s", selection_id, plan_id)
    return _ok({
        "SelectionId": selection_id,
        "BackupPlanId": plan_id,
        "CreationDate": record["CreationDate"],
    })


def _get_selection(plan_id, selection_id):
    if plan_id not in _plans:
        return _err("ResourceNotFoundException", f"Plan '{plan_id}' not found.", 404)
    s = _selections.get(selection_id)
    if s is None or s["BackupPlanId"] != plan_id:
        return _err("ResourceNotFoundException", f"Selection '{selection_id}' not found.", 404)
    return _ok({
        "SelectionId": s["SelectionId"],
        "BackupPlanId": s["BackupPlanId"],
        "BackupSelection": {
            "SelectionName": s["SelectionName"],
            "IamRoleArn": s["IamRoleArn"],
            "Resources": s["Resources"],
            "ListOfTags": s["ListOfTags"],
            "NotResources": s["NotResources"],
            "Conditions": s["Conditions"],
        },
        "CreationDate": s["CreationDate"],
        "CreatorRequestId": s["CreatorRequestId"],
    })


def _delete_selection(plan_id, selection_id):
    if plan_id not in _plans:
        return _err("ResourceNotFoundException", f"Plan '{plan_id}' not found.", 404)
    s = _selections.get(selection_id)
    if s is None or s["BackupPlanId"] != plan_id:
        return _err("ResourceNotFoundException", f"Selection '{selection_id}' not found.", 404)
    del _selections[selection_id]
    logger.info("DeleteBackupSelection: %s", selection_id)
    return _no_content()


def _list_selections(plan_id):
    if plan_id not in _plans:
        return _err("ResourceNotFoundException", f"Plan '{plan_id}' not found.", 404)
    items = [
        {
            "SelectionId": s["SelectionId"],
            "SelectionName": s["SelectionName"],
            "BackupPlanId": s["BackupPlanId"],
            "IamRoleArn": s["IamRoleArn"],
            "CreationDate": s["CreationDate"],
        }
        for s in _selections.values()
        if s["BackupPlanId"] == plan_id
    ]
    return _ok({"BackupSelectionsList": items})


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def _start_job(body):
    vault_name = body.get("BackupVaultName", "")
    if vault_name and vault_name not in _vaults:
        return _err("ResourceNotFoundException", f"Vault '{vault_name}' not found.", 404)
    job_id = new_uuid()
    now = _epoch()
    record = {
        "BackupJobId": job_id,
        "BackupJobArn": _job_arn(job_id),
        "BackupVaultName": vault_name,
        "BackupVaultArn": _vault_arn(vault_name) if vault_name else "",
        "ResourceArn": body.get("ResourceArn", ""),
        "IamRoleArn": body.get("IamRoleArn", ""),
        "StartBy": now,
        "CreationDate": now,
        "CompletionDate": now,
        "State": "COMPLETED",
        "StatusMessage": "",
        "PercentDone": "100.0",
        "BackupSizeInBytes": 0,
        "IsParent": False,
        "RecoveryPointArn": _recovery_point_arn(vault_name) if vault_name else "",
        "RecoveryPointTags": body.get("RecoveryPointTags", {}),
        "IdempotencyToken": body.get("IdempotencyToken", ""),
    }
    _jobs[job_id] = record
    # increment recovery point count on the vault
    if vault_name and vault_name in _vaults:
        _vaults[vault_name]["NumberOfRecoveryPoints"] += 1
    logger.info("StartBackupJob: %s vault=%s", job_id, vault_name)
    return _ok({
        "BackupJobId": job_id,
        "RecoveryPointArn": record["RecoveryPointArn"],
        "CreationDate": now,
        "IsParent": False,
    })


def _stop_job(job_id):
    j = _jobs.get(job_id)
    if j is None:
        return _err("ResourceNotFoundException", f"Job '{job_id}' not found.", 404)
    if j["State"] in ("COMPLETED", "FAILED", "ABORTED"):
        return _err(
            "InvalidRequestException",
            f"Job '{job_id}' is already in terminal state '{j['State']}'.",
            400,
        )
    j["State"] = "ABORTED"
    j["CompletionDate"] = _epoch()
    logger.info("StopBackupJob: %s", job_id)
    return _no_content()


def _describe_job(job_id):
    j = _jobs.get(job_id)
    if j is None:
        return _err("ResourceNotFoundException", f"Job '{job_id}' not found.", 404)
    return _ok(copy.copy(j))


def _list_jobs(query):
    vault_filter = query.get("backupVaultName", "")
    state_filter = query.get("state", "")
    resource_filter = query.get("resourceArn", "")
    items = list(_jobs.values())
    if vault_filter:
        items = [j for j in items if j["BackupVaultName"] == vault_filter]
    if state_filter:
        items = [j for j in items if j["State"] == state_filter]
    if resource_filter:
        items = [j for j in items if j["ResourceArn"] == resource_filter]
    return _paginate(items, query, "BackupJobs")


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _resolve_resource(arn):
    """Return the mutable tag dict for a supported backup ARN, or None."""
    if ":backup-vault:" in arn:
        name = arn.split(":")[-1]
        v = _vaults.get(name)
        return v["BackupVaultTags"] if v is not None else None
    if ":backup-plan:" in arn:
        pid = arn.split(":")[-1]
        p = _plans.get(pid)
        return p["Tags"] if p is not None else None
    return None


def _tag_resource(arn, body):
    tags = body.get("Tags", {})
    tag_dict = _resolve_resource(arn)
    if tag_dict is None:
        return _err("ResourceNotFoundException", f"Resource '{arn}' not found.", 404)
    tag_dict.update(tags)
    return _no_content()


def _untag_resource(arn, body):
    keys = body.get("TagKeyList", [])
    tag_dict = _resolve_resource(arn)
    if tag_dict is None:
        return _err("ResourceNotFoundException", f"Resource '{arn}' not found.", 404)
    for k in keys:
        tag_dict.pop(k, None)
    return _no_content()


def _list_tags(arn):
    tag_dict = _resolve_resource(arn)
    if tag_dict is None:
        return _err("ResourceNotFoundException", f"Resource '{arn}' not found.", 404)
    return _ok({"Tags": dict(tag_dict)})


# ---------------------------------------------------------------------------
# Request Router
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body_bytes, query_params):
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {}

    query = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}
    parts = [p for p in path.strip("/").split("/") if p]

    # /backup-vaults
    if parts and parts[0] == "backup-vaults":
        if len(parts) == 1 and method == "GET":
            return _list_vaults(query)
        if len(parts) == 2:
            name = parts[1]
            if method == "PUT":
                return _create_vault(name, body)
            if method == "GET":
                return _describe_vault(name)
            if method == "DELETE":
                return _delete_vault(name)

    # /backup/plans (and nested selections/versions)
    if len(parts) >= 2 and parts[0] == "backup" and parts[1] == "plans":
        # /backup/plans
        if len(parts) == 2:
            if method == "PUT":
                return _create_plan(body)
            if method == "GET":
                return _list_plans(query)

        # /backup/plans/{planId}
        if len(parts) == 3:
            plan_id = parts[2]
            if method == "GET":
                return _get_plan(plan_id)
            if method == "POST":
                return _update_plan(plan_id, body)
            if method == "DELETE":
                return _delete_plan(plan_id)

        # /backup/plans/{planId}/versions
        if len(parts) == 4 and parts[3] == "versions" and method == "GET":
            return _list_plan_versions(parts[2])

        # /backup/plans/{planId}/selections
        if len(parts) >= 4 and parts[3] == "selections":
            plan_id = parts[2]
            if len(parts) == 4:
                if method == "PUT":
                    return _create_selection(plan_id, body)
                if method == "GET":
                    return _list_selections(plan_id)
            if len(parts) == 5:
                selection_id = parts[4]
                if method == "GET":
                    return _get_selection(plan_id, selection_id)
                if method == "DELETE":
                    return _delete_selection(plan_id, selection_id)

    # /backup-jobs
    if parts and parts[0] == "backup-jobs":
        if len(parts) == 1:
            if method == "PUT":
                return _start_job(body)
            if method == "GET":
                return _list_jobs(query)
        if len(parts) == 2:
            job_id = parts[1]
            if method == "POST":
                return _stop_job(job_id)
            if method == "GET":
                return _describe_job(job_id)

    # /tags/{resourceArn} — TagResource (POST) and ListTags (GET)
    if parts and parts[0] == "tags":
        arn = "/".join(parts[1:])
        if method == "GET":
            return _list_tags(arn)
        if method == "POST":
            return _tag_resource(arn, body)

    # /untag/{resourceArn} — UntagResource (POST)
    if parts and parts[0] == "untag":
        arn = "/".join(parts[1:])
        if method == "POST":
            return _untag_resource(arn, body)

    return _err("ValidationException", f"No route for {method} {path}", 400)
