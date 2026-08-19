"""
CloudFormation change set handlers — Create, Describe, Execute, Delete, List change sets.
"""

import copy
import json
import logging

from ministack.core.responses import get_account_id, get_region, new_uuid, now_iso

from .engine import (
    _NO_VALUE,
    _apply_sam_transform_if_applicable,
    _evaluate_conditions,
    _parse_template,
    _resolve_parameters,
    _resolve_refs,
)
from .helpers import CFN_NS, _error, _esc, _extract_members, _p, _resolve_template, _xml
from .stacks import (
    _add_event,
    _create_stack_task_in_region,
    _deploy_stack_async,
    _diff_resources,
    _stack_region,
    _stack_region_context,
)

logger = logging.getLogger("cloudformation")


def _find_change_set(cs_name, stack_name=""):
    """Look up a change set by ID or by name+stack. Returns (cs_id, cs_dict) or (None, None)."""
    from ministack.services.cloudformation import _change_sets
    if cs_name in _change_sets:
        return cs_name, _change_sets[cs_name]
    for cid, c in _change_sets.items():
        if c["ChangeSetName"] == cs_name:
            if not stack_name or c["StackName"] == stack_name:
                return cid, c
    return None, None


# --- CreateChangeSet ---

def _resolve_props_for_diff(template, params, stack_name, stack_id):
    """Return ``template`` with each resource's Properties intrinsics resolved
    against ``params``, so change detection compares *effective* values (e.g. a
    parameter-driven ``Code.S3Key``) instead of identical raw ``Ref`` nodes.

    Without this, ``aws cloudformation deploy`` (which drives updates through
    ``--parameter-overrides``) produced an empty change set and silently no-oped
    Lambda code updates, while ``update-stack`` worked (#897). Resolution is
    best-effort: refs that can't resolve at change-set time (e.g. GetAtt to a
    not-yet-provisioned resource) fall back to the raw Properties on both sides.
    """
    if not template:
        return {}
    try:
        conditions = _evaluate_conditions(template, params)
    except Exception:
        conditions = {}
    mappings = template.get("Mappings", {})
    resolved = {}
    for lid, res in template.get("Resources", {}).items():
        new_res = dict(res)
        try:
            new_res["Properties"] = _resolve_refs(
                copy.deepcopy(res.get("Properties", {})),
                {}, params, conditions, mappings, stack_name, stack_id)
        except Exception:
            new_res["Properties"] = res.get("Properties", {})
        resolved[lid] = new_res
    return {"Resources": resolved}


def _create_change_set(params):
    from ministack.services.cloudformation import _change_sets, _stack_events, _stacks
    stack_name = _p(params, "StackName")
    cs_name = _p(params, "ChangeSetName")
    cs_type = _p(params, "ChangeSetType", "UPDATE")

    if not stack_name:
        return _error("ValidationError", "StackName is required")
    if not cs_name:
        return _error("ValidationError", "ChangeSetName is required")

    # A change set name is unique per stack while it exists; real CloudFormation
    # answers AlreadyExistsException for a duplicate name (modeled on
    # CreateChangeSet). #1418
    for _existing in _change_sets.values():
        if (_existing["ChangeSetName"] == cs_name
                and _existing["StackName"] == stack_name):
            return _error("AlreadyExistsException",
                          f"ChangeSet [{cs_name}] already exists")

    template_body, resolve_err = _resolve_template(params)
    if resolve_err:
        return resolve_err

    provided_params = _extract_members(params, "Parameters")
    tags = _extract_members(params, "Tags")

    stack = _stacks.get(stack_name)

    if cs_type == "CREATE":
        if stack and stack.get("StackStatus") not in (
            "DELETE_COMPLETE", "ROLLBACK_COMPLETE", "REVIEW_IN_PROGRESS"
        ):
            return _error("AlreadyExistsException",
                          f"Stack [{stack_name}] already exists")
        if not template_body:
            return _error("ValidationError", "TemplateBody or TemplateURL is required")

        # Create a placeholder stack in REVIEW_IN_PROGRESS
        stack_id = (
            f"arn:aws:cloudformation:{get_region()}:{get_account_id()}:"
            f"stack/{stack_name}/{new_uuid()}"
        )
        stack = {
            "StackName": stack_name,
            "StackId": stack_id,
            "StackStatus": "REVIEW_IN_PROGRESS",
            "StackStatusReason": "",
            "CreationTime": now_iso(),
            "LastUpdatedTime": now_iso(),
            "Description": "",
            "Parameters": [],
            "Tags": tags,
            "Outputs": [],
            "DisableRollback": False,
            "_region": get_region(),
            "_resources": {},
            "_template": {},
            "_template_body": "",
            "_resolved_params": {},
            "_conditions": {},
        }
        _stacks[stack_name] = stack
        _stack_events[stack_id] = []
        _add_event(stack_id, stack_name, stack_name,
                   "AWS::CloudFormation::Stack", "REVIEW_IN_PROGRESS",
                   physical_id=stack_id)
    else:
        # An UPDATE change set against a deleted stack: the name no longer
        # resolves (deleted stacks are addressable only by stack ID), so this is
        # "does not exist" — which steers `aws cloudformation deploy` back to a
        # CREATE change set for the re-deployed name.
        if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
            return _error("ValidationError",
                          f"Stack [{stack_name}] does not exist")
        stack_id = stack["StackId"]
        if not template_body:
            template_body = stack.get("_template_body", "{}")

    try:
        template = _parse_template(template_body)
        template = _apply_sam_transform_if_applicable(template)
    except Exception as e:
        return _error("ValidationError", f"Template format error: {e}")

    try:
        param_values = _resolve_parameters(
            template, provided_params, stack.get("_resolved_params", {}))
    except ValueError as exc:
        return _error("ValidationError", str(exc))

    # Compute changes — resolve parameters/intrinsics in BOTH templates first so
    # parameter-driven changes (the `aws cloudformation deploy
    # --parameter-overrides` pattern, e.g. a Lambda Code S3Key behind a Ref) are
    # detected instead of compared as identical raw nodes (#897).
    old_template = stack.get("_template", {}) if cs_type == "UPDATE" else {}
    old_params = stack.get("_resolved_params", {}) if cs_type == "UPDATE" else {}
    with _stack_region_context(stack, stack_id):
        old_resolved = _resolve_props_for_diff(old_template, old_params, stack_name, stack_id)
        new_resolved = _resolve_props_for_diff(template, param_values, stack_name, stack_id)
    changes = _diff_resources(old_resolved, new_resolved)

    cs_id = (
        f"arn:aws:cloudformation:{_stack_region(stack, stack_id)}:{get_account_id()}:"
        f"changeSet/{cs_name}/{new_uuid()}"
    )

    change_set = {
        "ChangeSetId": cs_id,
        "ChangeSetName": cs_name,
        "StackId": stack_id,
        "StackName": stack_name,
        "Status": "CREATE_COMPLETE",
        "ExecutionStatus": "AVAILABLE",
        "CreationTime": now_iso(),
        "Description": _p(params, "Description", ""),
        "ChangeSetType": cs_type,
        "Changes": changes,
        "Parameters": [
            {"ParameterKey": k, "ParameterValue": v["Value"]}
            for k, v in param_values.items()
        ],
        "Tags": tags,
        "_template": template,
        "_template_body": template_body,
        "_resolved_params": param_values,
    }
    _change_sets[cs_id] = change_set

    return _xml(200, "CreateChangeSetResponse",
                f"<CreateChangeSetResult>"
                f"<Id>{cs_id}</Id>"
                f"<StackId>{stack_id}</StackId>"
                f"</CreateChangeSetResult>")


# --- DescribeChangeSet ---

def _describe_change_set(params):
    cs_name = _p(params, "ChangeSetName")
    stack_name = _p(params, "StackName")
    _, cs = _find_change_set(cs_name, stack_name)
    if not cs:
        return _error("ChangeSetNotFoundException",
                      f"ChangeSet [{cs_name}] does not exist")

    params_xml = ""
    for p in cs.get("Parameters", []):
        params_xml += (
            "<member>"
            f"<ParameterKey>{_esc(p['ParameterKey'])}</ParameterKey>"
            f"<ParameterValue>{_esc(str(p['ParameterValue']))}</ParameterValue>"
            "</member>"
        )

    changes_xml = ""
    for ch in cs.get("Changes", []):
        rc = ch.get("ResourceChange", {})
        changes_xml += (
            "<member><ResourceChange>"
            f"<Action>{rc.get('Action', '')}</Action>"
            f"<LogicalResourceId>{_esc(rc.get('LogicalResourceId', ''))}</LogicalResourceId>"
            f"<ResourceType>{_esc(rc.get('ResourceType', ''))}</ResourceType>"
            f"<Replacement>{rc.get('Replacement', '')}</Replacement>"
            "</ResourceChange></member>"
        )

    tags_xml = ""
    for t in cs.get("Tags", []):
        tags_xml += (
            "<member>"
            f"<Key>{_esc(t.get('Key', ''))}</Key>"
            f"<Value>{_esc(t.get('Value', ''))}</Value>"
            "</member>"
        )

    inner = (
        f"<ChangeSetId>{_esc(cs['ChangeSetId'])}</ChangeSetId>"
        f"<ChangeSetName>{_esc(cs['ChangeSetName'])}</ChangeSetName>"
        f"<StackId>{_esc(cs['StackId'])}</StackId>"
        f"<StackName>{_esc(cs['StackName'])}</StackName>"
        f"<Status>{cs['Status']}</Status>"
        f"<ExecutionStatus>{cs['ExecutionStatus']}</ExecutionStatus>"
        f"<CreationTime>{cs['CreationTime']}</CreationTime>"
        f"<Description>{_esc(cs.get('Description', ''))}</Description>"
        f"<ChangeSetType>{cs.get('ChangeSetType', '')}</ChangeSetType>"
        f"<Parameters>{params_xml}</Parameters>"
        f"<Changes>{changes_xml}</Changes>"
        f"<Tags>{tags_xml}</Tags>"
    )

    return _xml(200, "DescribeChangeSetResponse",
                f"<DescribeChangeSetResult>{inner}</DescribeChangeSetResult>")


# --- ExecuteChangeSet ---

async def _track_change_set_execution(change_set, stack, deploy_coro):
    """Await the change set's stack deployment, then record its ExecutionStatus.

    Real CloudFormation moves a change set from EXECUTE_IN_PROGRESS to
    EXECUTE_COMPLETE only when the stack operation succeeds, and to
    EXECUTE_FAILED when it fails or rolls back. The ChangeSetStatus (``Status``)
    stays CREATE_COMPLETE throughout. #1418
    """
    try:
        await deploy_coro
    finally:
        status = stack.get("StackStatus", "")
        if status.endswith("_COMPLETE") and "ROLLBACK" not in status:
            change_set["ExecutionStatus"] = "EXECUTE_COMPLETE"
        else:
            change_set["ExecutionStatus"] = "EXECUTE_FAILED"


def _execute_change_set(params):
    from ministack.services.cloudformation import _stacks
    cs_name = _p(params, "ChangeSetName")
    stack_name = _p(params, "StackName")
    _, cs = _find_change_set(cs_name, stack_name)
    if not cs:
        return _error("ChangeSetNotFoundException",
                      f"ChangeSet [{cs_name}] does not exist")

    if cs["ExecutionStatus"] != "AVAILABLE":
        return _error("InvalidChangeSetStatusException",
                      f"ChangeSet [{cs_name}] is in {cs['ExecutionStatus']} status")

    cs["ExecutionStatus"] = "EXECUTE_IN_PROGRESS"
    real_stack_name = cs["StackName"]
    stack = _stacks.get(real_stack_name)
    if not stack:
        return _error("ValidationError",
                      f"Stack [{real_stack_name}] does not exist")

    stack_id = stack["StackId"]
    template = cs["_template"]
    template_body = cs["_template_body"]
    param_values = cs["_resolved_params"]
    tags = cs.get("Tags", [])
    cs_type = cs.get("ChangeSetType", "UPDATE")
    is_update = cs_type == "UPDATE"

    if is_update:
        previous_stack = {
            "_resources": copy.deepcopy(stack.get("_resources", {})),
            "_template": copy.deepcopy(stack.get("_template", {})),
            "_template_body": stack.get("_template_body", ""),
            "_resolved_params": copy.deepcopy(stack.get("_resolved_params", {})),
            "Outputs": copy.deepcopy(stack.get("Outputs", [])),
        }
    else:
        previous_stack = None

    status_prefix = "UPDATE" if is_update else "CREATE"
    stack["StackStatus"] = f"{status_prefix}_IN_PROGRESS"
    stack["LastUpdatedTime"] = now_iso()
    stack["_template_body"] = template_body
    if tags:
        stack["Tags"] = tags
    stack["Parameters"] = [
        {"ParameterKey": k, "ParameterValue": v["Value"], "NoEcho": v["NoEcho"]}
        for k, v in param_values.items()
    ]
    with _stack_region_context(stack, stack_id):
        stack["_conditions"] = _evaluate_conditions(template, param_values)

        _add_event(stack_id, real_stack_name, real_stack_name,
                   "AWS::CloudFormation::Stack", f"{status_prefix}_IN_PROGRESS",
                   physical_id=stack_id)

        _create_stack_task_in_region(
            _track_change_set_execution(
                cs,
                stack,
                _deploy_stack_async(real_stack_name, stack_id, template,
                                    param_values, False, tags,
                                    is_update=is_update,
                                    previous_stack=previous_stack),
            ),
            stack,
            stack_id,
        )

    # ExecutionStatus stays EXECUTE_IN_PROGRESS until the deploy finishes, when
    # _track_change_set_execution sets EXECUTE_COMPLETE or EXECUTE_FAILED. Status
    # is the ChangeSetStatus and stays CREATE_COMPLETE: EXECUTE_COMPLETE is not a
    # ChangeSetStatus value, and writing it here made the CDK reject the change
    # set as "not ready" (it gates on Status == CREATE_COMPLETE). #1418
    return _xml(200, "ExecuteChangeSetResponse",
                "<ExecuteChangeSetResult></ExecuteChangeSetResult>")


# --- DeleteChangeSet ---

def _delete_change_set(params):
    from ministack.services.cloudformation import _change_sets
    cs_name = _p(params, "ChangeSetName")
    stack_name = _p(params, "StackName")
    cs_id, cs = _find_change_set(cs_name, stack_name)
    if not cs_id:
        return _error("ChangeSetNotFoundException",
                      f"ChangeSet [{cs_name}] does not exist")
    _change_sets.pop(cs_id, None)
    return _xml(200, "DeleteChangeSetResponse", "")


# --- ListChangeSets ---

def _list_change_sets(params):
    from ministack.services.cloudformation import _change_sets, _stacks
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")

    members = ""
    for cs in _change_sets.values():
        if cs["StackName"] != stack_name:
            continue
        members += (
            "<member>"
            f"<ChangeSetId>{_esc(cs['ChangeSetId'])}</ChangeSetId>"
            f"<ChangeSetName>{_esc(cs['ChangeSetName'])}</ChangeSetName>"
            f"<StackId>{_esc(cs['StackId'])}</StackId>"
            f"<StackName>{_esc(cs['StackName'])}</StackName>"
            f"<Status>{cs['Status']}</Status>"
            f"<ExecutionStatus>{cs['ExecutionStatus']}</ExecutionStatus>"
            f"<CreationTime>{cs['CreationTime']}</CreationTime>"
            f"<Description>{_esc(cs.get('Description', ''))}</Description>"
            "</member>"
        )

    return _xml(200, "ListChangeSetsResponse",
                f"<ListChangeSetsResult>"
                f"<Summaries>{members}</Summaries>"
                f"</ListChangeSetsResult>")


# --- GetTemplateSummary ---
