"""
CloudFormation handlers — API action handlers for all supported CloudFormation actions.
"""

import asyncio
import copy
import json
import logging

from ministack.core.responses import get_account_id, new_uuid, now_iso

from .engine import (
    _evaluate_conditions, _parse_template, _resolve_parameters,
    _resolve_refs, _NO_VALUE,
)
from .stacks import _add_event, _deploy_stack_async, _delete_stack_async, _diff_resources
from .provisioners import _provision_resource, REGION
from .helpers import _xml, _error, _p, _esc, _extract_members, _extract_stack_status_filters, _resolve_template, CFN_NS
from .changesets import (
    _create_change_set, _describe_change_set, _execute_change_set,
    _delete_change_set, _list_change_sets,
)

logger = logging.getLogger("cloudformation")


# --- CreateStack ---

def _create_stack(params):
    from ministack.services.cloudformation import _stacks, _stack_events, _exports, _change_sets
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")

    template_body, resolve_err = _resolve_template(params)
    if resolve_err:
        return resolve_err
    if not template_body:
        return _error("ValidationError", "TemplateBody or TemplateURL is required")

    # Check stack name uniqueness (active stacks)
    existing = _stacks.get(stack_name)
    if existing and existing.get("StackStatus", "") not in (
        "DELETE_COMPLETE", "ROLLBACK_COMPLETE"
    ):
        return _error("AlreadyExistsException",
                      f"Stack [{stack_name}] already exists")

    try:
        template = _parse_template(template_body)
    except Exception as e:
        return _error("ValidationError", f"Template format error: {e}")
    provided_params = _extract_members(params, "Parameters")
    tags = _extract_members(params, "Tags")
    disable_rollback = _p(params, "DisableRollback", "false").lower() == "true"

    # Resolve parameters
    try:
        param_values = _resolve_parameters(template, provided_params)
    except ValueError as exc:
        return _error("ValidationError", str(exc))

    stack_id = (
        f"arn:aws:cloudformation:{REGION}:{get_account_id()}:"
        f"stack/{stack_name}/{new_uuid()}"
    )

    stack = {
        "StackName": stack_name,
        "StackId": stack_id,
        "StackStatus": "CREATE_IN_PROGRESS",
        "StackStatusReason": "",
        "CreationTime": now_iso(),
        "LastUpdatedTime": now_iso(),
        "Description": template.get("Description", ""),
        "Parameters": [
            {
                "ParameterKey": k,
                "ParameterValue": v["Value"],
                "NoEcho": v["NoEcho"],
            }
            for k, v in param_values.items()
        ],
        "Tags": tags,
        "Outputs": [],
        "DisableRollback": disable_rollback,
        "_resources": {},
        "_template": template,
        "_template_body": template_body,
        "_resolved_params": param_values,
        "_conditions": _evaluate_conditions(template, param_values),
    }
    _stacks[stack_name] = stack
    _stack_events[stack_id] = []

    _add_event(stack_id, stack_name, stack_name,
               "AWS::CloudFormation::Stack", "CREATE_IN_PROGRESS",
               physical_id=stack_id)

    asyncio.get_event_loop().create_task(
        _deploy_stack_async(stack_name, stack_id, template,
                            param_values, disable_rollback, tags)
    )

    return _xml(200, "CreateStackResponse",
                f"<CreateStackResult><StackId>{stack_id}</StackId></CreateStackResult>")


# --- DescribeStacks ---

def _describe_stacks(params):
    from ministack.services.cloudformation import _stacks
    stack_name = _p(params, "StackName")

    if stack_name:
        stack = _stacks.get(stack_name)
        # Also try matching by stack ID
        if not stack:
            for s in _stacks.values():
                if s.get("StackId") == stack_name:
                    stack = s
                    break
        if not stack:
            return _error("ValidationError",
                          f"Stack with id {stack_name} does not exist")
        stacks_to_describe = [stack]
    else:
        # Return all stacks except DELETE_COMPLETE
        stacks_to_describe = [
            s for s in _stacks.values()
            if s.get("StackStatus") != "DELETE_COMPLETE"
        ]

    members = ""
    for s in stacks_to_describe:
        params_xml = ""
        for p in s.get("Parameters", []):
            val = "****" if p.get("NoEcho") else _esc(str(p.get("ParameterValue", "")))
            params_xml += (
                "<member>"
                f"<ParameterKey>{_esc(p['ParameterKey'])}</ParameterKey>"
                f"<ParameterValue>{val}</ParameterValue>"
                "</member>"
            )

        outputs_xml = ""
        for o in s.get("Outputs", []):
            export_xml = ""
            if o.get("ExportName"):
                export_xml = f"<ExportName>{_esc(o['ExportName'])}</ExportName>"
            outputs_xml += (
                "<member>"
                f"<OutputKey>{_esc(o['OutputKey'])}</OutputKey>"
                f"<OutputValue>{_esc(str(o['OutputValue']))}</OutputValue>"
                f"<Description>{_esc(o.get('Description', ''))}</Description>"
                f"{export_xml}"
                "</member>"
            )

        tags_xml = ""
        for t in s.get("Tags", []):
            tags_xml += (
                "<member>"
                f"<Key>{_esc(t.get('Key', ''))}</Key>"
                f"<Value>{_esc(t.get('Value', ''))}</Value>"
                "</member>"
            )

        members += (
            "<member>"
            f"<StackName>{_esc(s['StackName'])}</StackName>"
            f"<StackId>{_esc(s['StackId'])}</StackId>"
            f"<StackStatus>{s['StackStatus']}</StackStatus>"
            f"<StackStatusReason>{_esc(s.get('StackStatusReason', ''))}</StackStatusReason>"
            f"<CreationTime>{s.get('CreationTime', '')}</CreationTime>"
            f"<LastUpdatedTime>{s.get('LastUpdatedTime', '')}</LastUpdatedTime>"
            f"<Description>{_esc(s.get('Description', ''))}</Description>"
            f"<DisableRollback>{str(s.get('DisableRollback', False)).lower()}</DisableRollback>"
            f"<Parameters>{params_xml}</Parameters>"
            f"<Outputs>{outputs_xml}</Outputs>"
            f"<Tags>{tags_xml}</Tags>"
            "</member>"
        )

    return _xml(200, "DescribeStacksResponse",
                f"<DescribeStacksResult><Stacks>{members}</Stacks></DescribeStacksResult>")


# --- ListStacks ---

def _list_stacks(params):
    from ministack.services.cloudformation import _stacks
    status_filters = _extract_stack_status_filters(params)

    summaries = ""
    for s in _stacks.values():
        status = s.get("StackStatus", "")
        if status_filters and status not in status_filters:
            continue
        entry = (
            "<member>"
            f"<StackName>{_esc(s['StackName'])}</StackName>"
            f"<StackId>{_esc(s['StackId'])}</StackId>"
            f"<StackStatus>{status}</StackStatus>"
            f"<CreationTime>{s.get('CreationTime', '')}</CreationTime>"
        )
        if s.get("LastUpdatedTime"):
            entry += f"<LastUpdatedTime>{s['LastUpdatedTime']}</LastUpdatedTime>"
        if s.get("StackStatusReason"):
            entry += f"<StackStatusReason>{_esc(s['StackStatusReason'])}</StackStatusReason>"
        if s.get("DeletionTime"):
            entry += f"<DeletionTime>{s['DeletionTime']}</DeletionTime>"
        entry += "</member>"
        summaries += entry

    return _xml(200, "ListStacksResponse",
                f"<ListStacksResult><StackSummaries>{summaries}</StackSummaries></ListStacksResult>")


# --- DescribeStackEvents ---

def _describe_stack_events(params):
    from ministack.services.cloudformation import _stacks, _stack_events
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")

    stack = _stacks.get(stack_name)
    if not stack:
        # Try by stack ID
        for s in _stacks.values():
            if s.get("StackId") == stack_name:
                stack = s
                break
    if not stack:
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")

    stack_id = stack["StackId"]
    events = _stack_events.get(stack_id, [])
    # Newest first
    events_sorted = sorted(events, key=lambda e: e.get("Timestamp", ""),
                           reverse=True)

    members = ""
    for e in events_sorted:
        members += (
            "<member>"
            f"<StackId>{_esc(e.get('StackId', ''))}</StackId>"
            f"<StackName>{_esc(e.get('StackName', ''))}</StackName>"
            f"<EventId>{_esc(e.get('EventId', ''))}</EventId>"
            f"<LogicalResourceId>{_esc(e.get('LogicalResourceId', ''))}</LogicalResourceId>"
            f"<PhysicalResourceId>{_esc(e.get('PhysicalResourceId', ''))}</PhysicalResourceId>"
            f"<ResourceType>{_esc(e.get('ResourceType', ''))}</ResourceType>"
            f"<ResourceStatus>{e.get('ResourceStatus', '')}</ResourceStatus>"
            f"<ResourceStatusReason>{_esc(e.get('ResourceStatusReason', ''))}</ResourceStatusReason>"
            f"<Timestamp>{e.get('Timestamp', '')}</Timestamp>"
            "</member>"
        )

    return _xml(200, "DescribeStackEventsResponse",
                f"<DescribeStackEventsResult><StackEvents>{members}</StackEvents></DescribeStackEventsResult>")


# --- DescribeStackResource ---

def _describe_stack_resource(params):
    from ministack.services.cloudformation import _stacks
    stack_name = _p(params, "StackName")
    logical_id = _p(params, "LogicalResourceId")

    stack = _stacks.get(stack_name)
    if not stack:
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")

    resources = stack.get("_resources", {})
    res = resources.get(logical_id)
    if not res:
        return _error("ValidationError",
                      f"Resource [{logical_id}] does not exist in stack [{stack_name}]")

    detail = (
        f"<LogicalResourceId>{_esc(logical_id)}</LogicalResourceId>"
        f"<PhysicalResourceId>{_esc(res.get('PhysicalResourceId', ''))}</PhysicalResourceId>"
        f"<ResourceType>{_esc(res.get('ResourceType', ''))}</ResourceType>"
        f"<ResourceStatus>{res.get('ResourceStatus', '')}</ResourceStatus>"
        f"<Timestamp>{res.get('Timestamp', '')}</Timestamp>"
        f"<StackName>{_esc(stack_name)}</StackName>"
        f"<StackId>{_esc(stack['StackId'])}</StackId>"
    )

    return _xml(200, "DescribeStackResourceResponse",
                f"<DescribeStackResourceResult>"
                f"<StackResourceDetail>{detail}</StackResourceDetail>"
                f"</DescribeStackResourceResult>")


# --- DescribeStackResources ---

def _describe_stack_resources(params):
    from ministack.services.cloudformation import _stacks
    stack_name = _p(params, "StackName")

    stack = _stacks.get(stack_name)
    if not stack:
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")

    resources = stack.get("_resources", {})
    members = ""
    for logical_id, res in resources.items():
        members += (
            "<member>"
            f"<LogicalResourceId>{_esc(logical_id)}</LogicalResourceId>"
            f"<PhysicalResourceId>{_esc(res.get('PhysicalResourceId', ''))}</PhysicalResourceId>"
            f"<ResourceType>{_esc(res.get('ResourceType', ''))}</ResourceType>"
            f"<ResourceStatus>{res.get('ResourceStatus', '')}</ResourceStatus>"
            f"<Timestamp>{res.get('Timestamp', '')}</Timestamp>"
            f"<StackName>{_esc(stack_name)}</StackName>"
            f"<StackId>{_esc(stack['StackId'])}</StackId>"
            "</member>"
        )

    return _xml(200, "DescribeStackResourcesResponse",
                f"<DescribeStackResourcesResult>"
                f"<StackResources>{members}</StackResources>"
                f"</DescribeStackResourcesResult>")


# --- ListStackResources ---

def _list_stack_resources(params):
    from ministack.services.cloudformation import _stacks
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")

    stack = _stacks.get(stack_name)
    if not stack:
        for s in _stacks.values():
            if s.get("StackId") == stack_name:
                stack = s
                break
    if not stack:
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")

    resources = stack.get("_resources", {})
    members = ""
    for logical_id, res in resources.items():
        members += (
            "<member>"
            f"<LogicalResourceId>{_esc(logical_id)}</LogicalResourceId>"
            f"<PhysicalResourceId>{_esc(res.get('PhysicalResourceId', ''))}</PhysicalResourceId>"
            f"<ResourceType>{_esc(res.get('ResourceType', ''))}</ResourceType>"
            f"<ResourceStatus>{res.get('ResourceStatus', '')}</ResourceStatus>"
            f"<LastUpdatedTimestamp>{res.get('Timestamp', '')}</LastUpdatedTimestamp>"
            "</member>"
        )

    return _xml(200, "ListStackResourcesResponse",
                f"<ListStackResourcesResult>"
                f"<StackResourceSummaries>{members}</StackResourceSummaries>"
                f"</ListStackResourcesResult>")


# --- GetTemplate ---

def _get_template(params):
    from ministack.services.cloudformation import _stacks
    stack_name = _p(params, "StackName")

    stack = _stacks.get(stack_name)
    if not stack:
        for s in _stacks.values():
            if s.get("StackId") == stack_name:
                stack = s
                break
    if not stack:
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")

    template_body = stack.get("_template_body", "{}")
    return _xml(200, "GetTemplateResponse",
                f"<GetTemplateResult>"
                f"<TemplateBody>{_esc(template_body)}</TemplateBody>"
                f"</GetTemplateResult>")


# --- DeleteStack ---

def _delete_stack(params):
    from ministack.services.cloudformation import _stacks
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")

    stack = _stacks.get(stack_name)
    if not stack:
        # AWS returns success for deleting non-existent stacks
        return _xml(200, "DeleteStackResponse", "")

    if stack.get("StackStatus") == "DELETE_COMPLETE":
        return _xml(200, "DeleteStackResponse", "")

    # Check for active imports before deleting
    stack_exports = [
        out.get("ExportName") for out in stack.get("Outputs", [])
        if out.get("ExportName")
    ]
    for export_name in stack_exports:
        for other_name, other_stack in _stacks.items():
            if other_name == stack_name:
                continue
            other_status = other_stack.get("StackStatus", "")
            if other_status.endswith("_COMPLETE") and "DELETE" not in other_status:
                other_template = other_stack.get("_template", {})
                if export_name in json.dumps(other_template):
                    return _error("ValidationError",
                                  f"Export {export_name} is imported by stack {other_name}")

    stack_id = stack["StackId"]
    asyncio.get_event_loop().create_task(_delete_stack_async(stack_name, stack_id))

    return _xml(200, "DeleteStackResponse", "")


# --- UpdateStack ---

def _update_stack(params):
    from ministack.services.cloudformation import _stacks
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")

    stack = _stacks.get(stack_name)
    if not stack:
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")

    current_status = stack.get("StackStatus", "")
    if current_status not in ("CREATE_COMPLETE", "UPDATE_COMPLETE",
                               "UPDATE_ROLLBACK_COMPLETE"):
        return _error("ValidationError",
                      f"Stack [{stack_name}] is in {current_status} state "
                      f"and cannot be updated")

    template_body, resolve_err = _resolve_template(params)
    if resolve_err:
        return resolve_err
    if not template_body:
        # Use previous template if UsePreviousTemplate
        if _p(params, "UsePreviousTemplate", "false").lower() == "true":
            template_body = stack.get("_template_body", "{}")
        else:
            return _error("ValidationError", "TemplateBody or TemplateURL is required")

    try:
        template = _parse_template(template_body)
    except Exception as e:
        return _error("ValidationError", f"Template format error: {e}")
    provided_params = _extract_members(params, "Parameters")
    tags = _extract_members(params, "Tags")
    disable_rollback = _p(params, "DisableRollback", "false").lower() == "true"

    try:
        param_values = _resolve_parameters(template, provided_params)
    except ValueError as exc:
        return _error("ValidationError", str(exc))

    # Save previous state for rollback
    previous_stack = {
        "_resources": copy.deepcopy(stack.get("_resources", {})),
        "_template": copy.deepcopy(stack.get("_template", {})),
        "_template_body": stack.get("_template_body", ""),
        "_resolved_params": copy.deepcopy(stack.get("_resolved_params", {})),
        "Outputs": copy.deepcopy(stack.get("Outputs", [])),
    }

    stack_id = stack["StackId"]
    stack["StackStatus"] = "UPDATE_IN_PROGRESS"
    stack["LastUpdatedTime"] = now_iso()
    stack["_template_body"] = template_body
    if tags:
        stack["Tags"] = tags
    stack["Parameters"] = [
        {"ParameterKey": k, "ParameterValue": v["Value"], "NoEcho": v["NoEcho"]}
        for k, v in param_values.items()
    ]
    stack["_conditions"] = _evaluate_conditions(template, param_values)

    _add_event(stack_id, stack_name, stack_name,
               "AWS::CloudFormation::Stack", "UPDATE_IN_PROGRESS",
               physical_id=stack_id)

    asyncio.get_event_loop().create_task(
        _deploy_stack_async(stack_name, stack_id, template,
                            param_values, disable_rollback, tags,
                            is_update=True, previous_stack=previous_stack)
    )

    return _xml(200, "UpdateStackResponse",
                f"<UpdateStackResult><StackId>{stack_id}</StackId></UpdateStackResult>")


# --- ValidateTemplate ---

def _validate_template(params):
    template_body = _p(params, "TemplateBody")
    if not template_body:
        return _error("ValidationError", "TemplateBody is required")

    try:
        template = _parse_template(template_body)
    except Exception as e:
        return _error("ValidationError", f"Template format error: {e}")
    if "Resources" not in template:
        return _error("ValidationError",
                      "Template format error: At least one Resources member must be defined.")
    description = template.get("Description", "")
    param_defs = template.get("Parameters", {})

    params_xml = ""
    for name, defn in param_defs.items():
        default = defn.get("Default", "")
        no_echo = str(defn.get("NoEcho", "false")).lower()
        ptype = defn.get("Type", "String")
        desc = defn.get("Description", "")
        params_xml += (
            "<member>"
            f"<ParameterKey>{_esc(name)}</ParameterKey>"
            f"<DefaultValue>{_esc(str(default))}</DefaultValue>"
            f"<NoEcho>{no_echo}</NoEcho>"
            f"<ParameterType>{_esc(ptype)}</ParameterType>"
            f"<Description>{_esc(desc)}</Description>"
            "</member>"
        )

    return _xml(200, "ValidateTemplateResponse",
                f"<ValidateTemplateResult>"
                f"<Description>{_esc(description)}</Description>"
                f"<Parameters>{params_xml}</Parameters>"
                f"</ValidateTemplateResult>")


# --- ListExports ---

def _list_exports(params):
    from ministack.services.cloudformation import _exports
    members = ""
    for name, exp in _exports.items():
        members += (
            "<member>"
            f"<ExportingStackId>{_esc(exp.get('StackId', ''))}</ExportingStackId>"
            f"<Name>{_esc(name)}</Name>"
            f"<Value>{_esc(str(exp.get('Value', '')))}</Value>"
            "</member>"
        )

    return _xml(200, "ListExportsResponse",
                f"<ListExportsResult><Exports>{members}</Exports></ListExportsResult>")
# --- GetTemplateSummary ---

def _get_template_summary(params):
    from ministack.services.cloudformation import _stacks
    template_body, resolve_err = _resolve_template(params)
    if resolve_err:
        return resolve_err
    stack_name = _p(params, "StackName")

    if stack_name and not template_body:
        stack = _stacks.get(stack_name)
        if not stack:
            return _error("ValidationError",
                          f"Stack [{stack_name}] does not exist")
        template_body = stack.get("_template_body", "{}")

    if not template_body:
        return _error("ValidationError",
                      "Either TemplateBody, TemplateURL, or StackName must be provided")

    try:
        template = _parse_template(template_body)
    except Exception as e:
        return _error("ValidationError", f"Template format error: {e}")
    description = template.get("Description", "")
    resources = template.get("Resources", {})
    param_defs = template.get("Parameters", {})

    # Resource types
    resource_types = sorted(set(
        r.get("Type", "") for r in resources.values()
    ))
    types_xml = "".join(f"<member>{_esc(t)}</member>" for t in resource_types)

    # Parameters
    params_xml = ""
    for name, defn in param_defs.items():
        default = defn.get("Default", "")
        no_echo = str(defn.get("NoEcho", "false")).lower()
        ptype = defn.get("Type", "String")
        desc = defn.get("Description", "")
        params_xml += (
            "<member>"
            f"<ParameterKey>{_esc(name)}</ParameterKey>"
            f"<DefaultValue>{_esc(str(default))}</DefaultValue>"
            f"<NoEcho>{no_echo}</NoEcho>"
            f"<ParameterType>{_esc(ptype)}</ParameterType>"
            f"<Description>{_esc(desc)}</Description>"
            "</member>"
        )

    return _xml(200, "GetTemplateSummaryResponse",
                f"<GetTemplateSummaryResult>"
                f"<Description>{_esc(description)}</Description>"
                f"<ResourceTypes>{types_xml}</ResourceTypes>"
                f"<Parameters>{params_xml}</Parameters>"
                f"</GetTemplateSummaryResult>")


# ===========================================================================
# Action Handler Registry
# ===========================================================================

_ACTION_HANDLERS = {
    "CreateStack": _create_stack,
    "DescribeStacks": _describe_stacks,
    "ListStacks": _list_stacks,
    "DeleteStack": _delete_stack,
    "UpdateStack": _update_stack,
    "DescribeStackEvents": _describe_stack_events,
    "DescribeStackResource": _describe_stack_resource,
    "DescribeStackResources": _describe_stack_resources,
    "ListStackResources": _list_stack_resources,
    "GetTemplate": _get_template,
    "ValidateTemplate": _validate_template,
    "ListExports": _list_exports,
    "CreateChangeSet": _create_change_set,
    "DescribeChangeSet": _describe_change_set,
    "ExecuteChangeSet": _execute_change_set,
    "DeleteChangeSet": _delete_change_set,
    "ListChangeSets": _list_change_sets,
    "GetTemplateSummary": _get_template_summary,
}
