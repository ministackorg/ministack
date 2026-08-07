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

Read-only surface (empty/default documents — no persisted state for these
resource families yet, so lists come back empty and singleton lookups return
ResourceNotFoundException, matching real AWS):
  Copy/Restore/Report/Scan jobs, Frameworks, Report plans, Recovery points,
  Protected resources, Legal holds, Restore-testing plans/selections,
  Backup-plan templates, Tiering configurations, account/region/global
  settings, supported resource types, vault access-policy / notifications.
"""

import copy
import json
import logging
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    get_account_id,
    get_region,
    new_uuid,
)

logger = logging.getLogger("backup")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_vaults     = AccountRegionScopedDict()     # vault_name -> vault record
_plans      = AccountRegionScopedDict()     # plan_id -> plan record
_selections = AccountRegionScopedDict()     # selection_id -> selection record
_jobs       = AccountRegionScopedDict()     # job_id -> job record


def reset():
    _vaults.clear()
    _plans.clear()
    _selections.clear()
    _jobs.clear()


def get_state():
    # Preserve scoped wrappers; casting to a plain dict drops the per-account
    # and per-region scoping and would persist only the current request's
    # tenants. Scoped dicts have JSON encoder hooks that round-trip the
    # tuple keys correctly.
    return {
        "vaults":     copy.deepcopy(_vaults),
        "plans":      copy.deepcopy(_plans),
        "selections": copy.deepcopy(_selections),
        "jobs":       copy.deepcopy(_jobs),
    }


def restore_state(data):
    _vaults.update(data.get("vaults", {}))
    _plans.update(data.get("plans", {}))
    _restore_selections(data.get("selections", {}))
    _jobs.update(data.get("jobs", {}))


def _restore_selections(restored):
    if isinstance(restored, AccountRegionScopedDict):
        _selections.update(restored)
        return

    if isinstance(restored, AccountScopedDict):
        selection_items = restored._data.items()
    elif isinstance(restored, dict):
        account_id = get_account_id()
        selection_items = [((account_id, selection_id), record) for selection_id, record in restored.items()]
    else:
        return

    plan_regions = {
        (account_id, plan_id): region
        for (account_id, region, _plan_id), plan in _plans.all_items()
        for plan_id in (plan.get("BackupPlanId", _plan_id),)
    }
    fallback_region = get_region()
    for (account_id, selection_id), record in selection_items:
        region = plan_regions.get(
            (account_id, record.get("BackupPlanId")),
            fallback_region,
        )
        _selections.set_scoped(account_id, region, selection_id, record)


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
    return int(time.time())


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
        {"Content-Type": "application/json", "x-amzn-errortype": code},
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
    try:
        spec = parse_arn(arn)
    except ArnParseError as exc:
        raise ValueError("ResourceArn must be a Backup ARN.") from exc

    if spec.service != "backup":
        raise ValueError("ResourceArn must be a Backup ARN.")
    if spec.region != get_region() or spec.account_id != get_account_id():
        return None

    if spec.resource.startswith("backup-vault:"):
        name = spec.resource.removeprefix("backup-vault:")
        v = _vaults.get(name)
        return v["BackupVaultTags"] if v is not None else None
    if spec.resource.startswith("backup-plan:"):
        pid = spec.resource.removeprefix("backup-plan:")
        p = _plans.get(pid)
        return p["Tags"] if p is not None else None
    return None


def _tag_resource(arn, body):
    tags = body.get("Tags", {})
    try:
        tag_dict = _resolve_resource(arn)
    except ValueError as exc:
        return _err("InvalidParameterValueException", str(exc), 400)
    if tag_dict is None:
        return _err("ResourceNotFoundException", f"Resource '{arn}' not found.", 404)
    tag_dict.update(tags)
    return _no_content()


def _untag_resource(arn, body):
    keys = body.get("TagKeyList", [])
    try:
        tag_dict = _resolve_resource(arn)
    except ValueError as exc:
        return _err("InvalidParameterValueException", str(exc), 400)
    if tag_dict is None:
        return _err("ResourceNotFoundException", f"Resource '{arn}' not found.", 404)
    for k in keys:
        tag_dict.pop(k, None)
    return _no_content()


def _list_tags(arn):
    try:
        tag_dict = _resolve_resource(arn)
    except ValueError as exc:
        return _err("InvalidParameterValueException", str(exc), 400)
    if tag_dict is None:
        return _err("ResourceNotFoundException", f"Resource '{arn}' not found.", 404)
    return _ok({"Tags": dict(tag_dict)})


# ---------------------------------------------------------------------------
# Read-only surface (empty / default documents)
#
# MiniStack does not yet persist copy jobs, restore jobs, report jobs/plans,
# frameworks, recovery points, protected resources, legal holds,
# restore-testing plans/selections, backup-plan templates or tiering
# configurations. Real AWS returns empty lists for the List* operations on
# these families and ResourceNotFoundException for a Get*/Describe* of a
# specific (necessarily absent) resource. AWS Backup returns HTTP 400 for
# ResourceNotFoundException (not 404), so these handlers use _not_found().
#
# Shapes and JSON key casing are taken verbatim from the botocore
# backup/2018-11-15 service-2.json (rest-json) output shapes.
# ---------------------------------------------------------------------------

# AWS resource types supported by AWS Backup (GetSupportedResourceTypes).
_SUPPORTED_RESOURCE_TYPES = [
    "Aurora", "CloudFormation", "DocumentDB", "DynamoDB", "EBS", "EC2",
    "EFS", "EKS", "FSx", "Neptune", "RDS", "Redshift", "S3",
    "SAP HANA on Amazon EC2", "Storage Gateway", "Timestream", "VirtualMachine",
]


def _not_found(msg):
    # AWS Backup returns HTTP 400 for ResourceNotFoundException.
    return _err("ResourceNotFoundException", msg, 400)


def _empty_list(query, key):
    """List* on a family MiniStack does not persist yet: AWS-correct empty page."""
    return _paginate([], query, key)


# --- Copy jobs -------------------------------------------------------------

def _list_copy_jobs(query):
    return _empty_list(query, "CopyJobs")


def _describe_copy_job(copy_job_id):
    return _not_found(f"Copy job '{copy_job_id}' not found.")


# --- Restore jobs ----------------------------------------------------------

def _list_restore_jobs(query):
    return _empty_list(query, "RestoreJobs")


def _describe_restore_job(restore_job_id):
    return _not_found(f"Restore job '{restore_job_id}' not found.")


def _get_restore_job_metadata(restore_job_id):
    return _not_found(f"Restore job '{restore_job_id}' not found.")


def _list_restore_jobs_by_resource(query):
    return _empty_list(query, "RestoreJobs")


# --- Report jobs / plans ---------------------------------------------------

def _list_report_jobs(query):
    return _empty_list(query, "ReportJobs")


def _describe_report_job(report_job_id):
    return _not_found(f"Report job '{report_job_id}' not found.")


def _list_report_plans(query):
    return _empty_list(query, "ReportPlans")


def _describe_report_plan(report_plan_name):
    return _not_found(f"Report plan '{report_plan_name}' not found.")


# --- Frameworks ------------------------------------------------------------

def _list_frameworks(query):
    return _empty_list(query, "Frameworks")


def _describe_framework(framework_name):
    return _not_found(f"Framework '{framework_name}' not found.")


# --- Recovery points -------------------------------------------------------

def _list_recovery_points_by_vault(vault_name, query):
    if vault_name not in _vaults:
        return _not_found(f"Vault '{vault_name}' not found.")
    return _empty_list(query, "RecoveryPoints")


def _describe_recovery_point(vault_name, recovery_point_arn):
    if vault_name not in _vaults:
        return _not_found(f"Vault '{vault_name}' not found.")
    return _not_found(f"Recovery point '{recovery_point_arn}' not found.")


def _get_recovery_point_index_details(vault_name, recovery_point_arn):
    if vault_name not in _vaults:
        return _not_found(f"Vault '{vault_name}' not found.")
    return _not_found(f"Recovery point '{recovery_point_arn}' not found.")


def _get_recovery_point_restore_metadata(vault_name, recovery_point_arn):
    if vault_name not in _vaults:
        return _not_found(f"Vault '{vault_name}' not found.")
    return _not_found(f"Recovery point '{recovery_point_arn}' not found.")


def _list_recovery_points_by_resource(query):
    return _empty_list(query, "RecoveryPoints")


# --- Protected resources ---------------------------------------------------

def _list_protected_resources(query):
    return _empty_list(query, "Results")


def _list_protected_resources_by_vault(vault_name, query):
    if vault_name not in _vaults:
        return _not_found(f"Vault '{vault_name}' not found.")
    return _empty_list(query, "Results")


def _describe_protected_resource(resource_arn):
    return _not_found(f"Protected resource '{resource_arn}' not found.")


# --- Legal holds -----------------------------------------------------------

def _list_legal_holds(query):
    return _empty_list(query, "LegalHolds")


def _get_legal_hold(legal_hold_id):
    return _not_found(f"Legal hold '{legal_hold_id}' not found.")


def _list_recovery_points_by_legal_hold(legal_hold_id, query):
    return _not_found(f"Legal hold '{legal_hold_id}' not found.")


# --- Restore testing -------------------------------------------------------

def _list_restore_testing_plans(query):
    return _empty_list(query, "RestoreTestingPlans")


def _get_restore_testing_plan(plan_name):
    return _not_found(f"Restore testing plan '{plan_name}' not found.")


def _list_restore_testing_selections(plan_name, query):
    return _not_found(f"Restore testing plan '{plan_name}' not found.")


def _get_restore_testing_selection(plan_name, selection_name):
    return _not_found(f"Restore testing plan '{plan_name}' not found.")


# --- Scan jobs -------------------------------------------------------------

def _list_scan_jobs(query):
    return _empty_list(query, "ScanJobs")


def _describe_scan_job(scan_job_id):
    return _not_found(f"Scan job '{scan_job_id}' not found.")


# --- Audit summaries (aggregations) ----------------------------------------

def _list_backup_job_summaries(query):
    return _ok({"BackupJobSummaries": []})


def _list_copy_job_summaries(query):
    return _ok({"CopyJobSummaries": []})


def _list_restore_job_summaries(query):
    return _ok({"RestoreJobSummaries": []})


def _list_scan_job_summaries(query):
    return _ok({"ScanJobSummaries": []})


# --- Backup plan templates -------------------------------------------------

def _list_backup_plan_templates(query):
    return _paginate([], query, "BackupPlanTemplatesList")


# --- Indexed recovery points -----------------------------------------------

def _list_indexed_recovery_points(query):
    return _empty_list(query, "IndexedRecoveryPoints")


# --- Tiering configurations ------------------------------------------------

def _list_tiering_configurations(query):
    return _empty_list(query, "TieringConfigurations")


def _get_tiering_configuration(name):
    return _not_found(f"Tiering configuration '{name}' not found.")


# --- Logically air-gapped restore access vaults ----------------------------

def _list_restore_access_backup_vaults(vault_name, query):
    if vault_name not in _vaults:
        return _not_found(f"Vault '{vault_name}' not found.")
    return _empty_list(query, "RestoreAccessBackupVaults")


# --- Vault access policy / notifications -----------------------------------

def _get_vault_access_policy(vault_name):
    # Real AWS returns ResourceNotFoundException when no policy is set (and when
    # the vault itself is absent). MiniStack persists no vault access policies.
    if vault_name not in _vaults:
        return _not_found(f"Vault '{vault_name}' not found.")
    return _not_found(f"No access policy is associated with vault '{vault_name}'.")


def _get_vault_notifications(vault_name):
    # Real AWS returns ResourceNotFoundException when no notification config is
    # set. MiniStack persists no vault notification configurations.
    if vault_name not in _vaults:
        return _not_found(f"Vault '{vault_name}' not found.")
    return _not_found(f"No notifications are associated with vault '{vault_name}'.")


# --- Account / region / global settings (always-present defaults) ----------

def _describe_region_settings():
    return _ok({
        "ResourceTypeOptInPreference": {},
        "ResourceTypeManagementPreference": {},
    })


def _describe_global_settings():
    return _ok({"GlobalSettings": {}})


def _get_supported_resource_types():
    return _ok({"ResourceTypes": list(_SUPPORTED_RESOURCE_TYPES)})


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
        # /backup-vaults/{vaultName}/<sub-resource>
        if len(parts) >= 3:
            name = parts[1]
            sub = parts[2]
            if method == "GET":
                if sub == "access-policy" and len(parts) == 3:
                    return _get_vault_access_policy(name)
                if sub == "notification-configuration" and len(parts) == 3:
                    return _get_vault_notifications(name)
                if sub == "resources" and len(parts) == 3:
                    return _list_protected_resources_by_vault(name, query)
                if sub == "recovery-points":
                    if len(parts) == 3:
                        return _list_recovery_points_by_vault(name, query)
                    # /backup-vaults/{name}/recovery-points/{rpArn}[/index|/restore-metadata]
                    if parts[-1] == "index":
                        rp = "/".join(parts[3:-1])
                        return _get_recovery_point_index_details(name, rp)
                    if parts[-1] == "restore-metadata":
                        rp = "/".join(parts[3:-1])
                        return _get_recovery_point_restore_metadata(name, rp)
                    rp = "/".join(parts[3:])
                    return _describe_recovery_point(name, rp)

    # /logically-air-gapped-backup-vaults/{vaultName}/restore-access-backup-vaults
    if (
        len(parts) == 3
        and parts[0] == "logically-air-gapped-backup-vaults"
        and parts[2] == "restore-access-backup-vaults"
        and method == "GET"
    ):
        return _list_restore_access_backup_vaults(parts[1], query)

    # /backup/template/plans — ListBackupPlanTemplates
    if (
        len(parts) == 3
        and parts[0] == "backup"
        and parts[1] == "template"
        and parts[2] == "plans"
        and method == "GET"
    ):
        return _list_backup_plan_templates(query)

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

    # -------------------------------------------------------------------
    # Read-only families with no persisted state (empty / not-found docs).
    # -------------------------------------------------------------------

    # /copy-jobs and /copy-jobs/{copyJobId}
    if parts and parts[0] == "copy-jobs" and method == "GET":
        if len(parts) == 1:
            return _list_copy_jobs(query)
        if len(parts) == 2:
            return _describe_copy_job(parts[1])

    # /restore-jobs, /restore-jobs/{id}, /restore-jobs/{id}/metadata
    if parts and parts[0] == "restore-jobs" and method == "GET":
        if len(parts) == 1:
            return _list_restore_jobs(query)
        if len(parts) == 2:
            return _describe_restore_job(parts[1])
        if len(parts) == 3 and parts[2] == "metadata":
            return _get_restore_job_metadata(parts[1])

    # /resources and /resources/{resourceArn}[/recovery-points|/restore-jobs]
    if parts and parts[0] == "resources" and method == "GET":
        if len(parts) == 1:
            return _list_protected_resources(query)
        if parts[-1] == "recovery-points":
            return _list_recovery_points_by_resource(query)
        if parts[-1] == "restore-jobs":
            return _list_restore_jobs_by_resource(query)
        return _describe_protected_resource("/".join(parts[1:]))

    # /legal-holds, /legal-holds/{id}, /legal-holds/{id}/recovery-points
    if parts and parts[0] == "legal-holds" and method == "GET":
        if len(parts) == 1:
            return _list_legal_holds(query)
        if len(parts) == 2:
            return _get_legal_hold(parts[1])
        if len(parts) == 3 and parts[2] == "recovery-points":
            return _list_recovery_points_by_legal_hold(parts[1], query)

    # /restore-testing/plans[/{name}[/selections[/{sel}]]]
    if len(parts) >= 2 and parts[0] == "restore-testing" and parts[1] == "plans" and method == "GET":
        if len(parts) == 2:
            return _list_restore_testing_plans(query)
        if len(parts) == 3:
            return _get_restore_testing_plan(parts[2])
        if len(parts) == 4 and parts[3] == "selections":
            return _list_restore_testing_selections(parts[2], query)
        if len(parts) == 5 and parts[3] == "selections":
            return _get_restore_testing_selection(parts[2], parts[4])

    # /scan/jobs and /scan/jobs/{scanJobId}
    if len(parts) >= 2 and parts[0] == "scan" and parts[1] == "jobs" and method == "GET":
        if len(parts) == 2:
            return _list_scan_jobs(query)
        if len(parts) == 3:
            return _describe_scan_job(parts[2])

    # /tiering-configurations and /tiering-configurations/{name}
    if parts and parts[0] == "tiering-configurations" and method == "GET":
        if len(parts) == 1:
            return _list_tiering_configurations(query)
        if len(parts) == 2:
            return _get_tiering_configuration(parts[1])

    # /indexes/recovery-point — ListIndexedRecoveryPoints
    if (
        len(parts) == 2
        and parts[0] == "indexes"
        and parts[1] == "recovery-point"
        and method == "GET"
    ):
        return _list_indexed_recovery_points(query)

    # /audit/* — frameworks, report-jobs, report-plans and summaries
    if len(parts) >= 2 and parts[0] == "audit" and method == "GET":
        sub = parts[1]
        if sub == "frameworks":
            if len(parts) == 2:
                return _list_frameworks(query)
            if len(parts) == 3:
                return _describe_framework(parts[2])
        if sub == "report-jobs":
            if len(parts) == 2:
                return _list_report_jobs(query)
            if len(parts) == 3:
                return _describe_report_job(parts[2])
        if sub == "report-plans":
            if len(parts) == 2:
                return _list_report_plans(query)
            if len(parts) == 3:
                return _describe_report_plan(parts[2])
        if sub == "backup-job-summaries":
            return _list_backup_job_summaries(query)
        if sub == "copy-job-summaries":
            return _list_copy_job_summaries(query)
        if sub == "restore-job-summaries":
            return _list_restore_job_summaries(query)
        if sub == "scan-job-summaries":
            return _list_scan_job_summaries(query)

    # Singleton account / global settings and supported resource types.
    if parts and method == "GET":
        if parts == ["account-settings"]:
            return _describe_region_settings()
        if parts == ["global-settings"]:
            return _describe_global_settings()
        if parts == ["supported-resource-types"]:
            return _get_supported_resource_types()

    return _err("ValidationException", f"No route for {method} {path}", 400)
