"""
CloudFormation change set handlers — Create, Describe, Execute, Delete, List change sets.
"""

import asyncio
import copy
import json
import logging

from ministack.core.responses import get_account_id, new_uuid, now_iso

from .engine import (
    _NO_VALUE,
    _evaluate_conditions,
    _parse_template,
    _resolve_parameters,
    _resolve_refs,
)
from .helpers import CFN_NS, _error, _esc, _extract_members, _p, _resolve_template, _xml
from .provisioners import REGION
from .stacks import _add_event, _deploy_stack_async, _diff_resources

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

def _create_change_set(params):
    from ministack.services.cloudformation import _change_sets, _stack_events, _stacks
    stack_name = _p(params, "StackName")
    cs_name = _p(params, "ChangeSetName")
    cs_type = _p(params, "ChangeSetType", "UPDATE")

    if not stack_name:
        return _error("ValidationError", "StackName is required")
    if not cs_name:
        return _error("ValidationError", "ChangeSetName is required")

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
            f"arn:aws:cloudformation:{REGION}:{get_account_id()}:"
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
            "_resources": {},
            "_template": {},
            "_template_body": "",
            "_resolved_params": {},
            "_conditions": {},
        }
        _stacks[stack_name] = stack
        _stack_events[stack_id] = []
    else:
        if not stack:
            return _error("ValidationError",
                          f"Stack [{stack_name}] does not exist")
        stack_id = stack["StackId"]
        if not template_body:
            template_body = stack.get("_template_body", "{}")

    try:
        template = _parse_template(template_body)
    except Exception as e:
        return _error("ValidationError", f"Template format error: {e}")

    try:
        param_values = _resolve_parameters(template, provided_params)
    except ValueError as exc:
        return _error("ValidationError", str(exc))

    # Compute changes
    old_template = stack.get("_template", {}) if cs_type == "UPDATE" else {}
    changes = _diff_resources(old_template, template)

    cs_id = (
        f"arn:aws:cloudformation:{REGION}:{get_account_id()}:"
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
    stack["_conditions"] = _evaluate_conditions(template, param_values)

    _add_event(stack_id, real_stack_name, real_stack_name,
               "AWS::CloudFormation::Stack", f"{status_prefix}_IN_PROGRESS",
               physical_id=stack_id)

    asyncio.get_event_loop().create_task(
        _deploy_stack_async(real_stack_name, stack_id, template,
                            param_values, False, tags,
                            is_update=is_update,
                            previous_stack=previous_stack)
    )

    cs["ExecutionStatus"] = "EXECUTE_COMPLETE"
    cs["Status"] = "EXECUTE_COMPLETE"

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

