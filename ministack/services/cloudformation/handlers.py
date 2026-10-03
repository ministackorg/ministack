# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
CloudFormation handlers — API action handlers for all supported CloudFormation actions.
"""

import copy
import json
import logging

from ministack.core.responses import get_account_id, get_region, new_uuid, now_iso
from ministack.services.cloudformation import drift as _drift
from ministack.services.cloudformation import wait_conditions as _wc

from .changesets import (
    _create_change_set,
    _delete_change_set,
    _describe_change_set,
    _execute_change_set,
    _list_change_sets,
)
from .engine import (
    _NO_VALUE,
    _apply_transforms,
    _evaluate_conditions,
    _has_dynamic_references,
    _parse_template,
    _resolve_parameters,
    _resolve_refs,
    declared_transforms,
    refuse_undeclared_language_extensions,
    validate_template_support,
)
from .helpers import (
    DELETION_MODE_VALUES,
    ON_FAILURE_VALUES,
    TEMPLATE_STAGE_VALUES,
    _error,
    _esc,
    _extract_members,
    _extract_stack_status_filters,
    _extract_string_members,
    _p,
    _page,
    _request_problems,
    _resolve_template,
    _xml,
    enum_problems,
    validation_error_message,
)
from .stacks import (
    CLIENT_REQUEST_TOKEN,
    _add_event,
    _continue_update_rollback_async,
    _create_stack_task_in_region,
    _create_then_delete_on_failure,
    _delete_stack_async,
    _deploy_stack_async,
    _roll_back_operation,
    _stack_region_context,
    _stack_tags_changed,
)

logger = logging.getLogger("cloudformation")


# GetTemplateSummary's rule, kept as it was: every ``AWS::IAM::*`` type needs
# a capability, and these name properties make it CAPABILITY_NAMED_IAM.
_NAMED_IAM_PROPS = {
    "AWS::IAM::Role": "RoleName",
    "AWS::IAM::User": "UserName",
    "AWS::IAM::Group": "GroupName",
    "AWS::IAM::Policy": "PolicyName",
    "AWS::IAM::ManagedPolicy": "ManagedPolicyName",
    "AWS::IAM::InstanceProfile": "InstanceProfileName",
}

# The enforcement rule, from the ``Capabilities`` parameter of API_CreateStack:
# these eight types "require you to specify either the CAPABILITY_IAM or
# CAPABILITY_NAMED_IAM capability", and "If you have IAM resources with
# custom names, you must specify CAPABILITY_NAMED_IAM". ``AWS::IAM::Policy``
# is not in the named map: its ``PolicyName`` is a required property
# (aws-resource-iam-policy.html), not a custom name.
_CAPABILITY_IAM_TYPES = frozenset({
    "AWS::IAM::AccessKey",
    "AWS::IAM::Group",
    "AWS::IAM::InstanceProfile",
    "AWS::IAM::ManagedPolicy",
    "AWS::IAM::Policy",
    "AWS::IAM::Role",
    "AWS::IAM::User",
    "AWS::IAM::UserToGroupAddition",
})
_CAPABILITY_NAMED_PROPS = {
    rtype: prop for rtype, prop in _NAMED_IAM_PROPS.items()
    if rtype != "AWS::IAM::Policy"
}


def _uses_embedded_macro(template):
    """True when a section of the template (other than ``Parameters`` and
    ``AWSTemplateFormatVersion``) contains an ``Fn::Transform`` node: a macro
    called on part of the template (intrinsic-function-reference-transform),
    which API_CreateStack counts like a top-level ``Transform`` for
    ``CAPABILITY_AUTO_EXPAND`` ("one or more macros")."""
    stack = [value for section, value in template.items()
             if section not in ("Parameters", "AWSTemplateFormatVersion", "Transform")]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if "Fn::Transform" in node:
                return True
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return False


def _uses_macro(template):
    """True when the template calls one or more macros: a top-level
    ``Transform`` or an embedded ``Fn::Transform`` node. Both count for the
    ``CAPABILITY_AUTO_EXPAND`` rule of API_CreateStack."""
    return bool(template.get("Transform")) or _uses_embedded_macro(template)


def _required_capabilities(template, strict=False):
    """Return ``(capabilities, reason_types)`` for a parsed template:
    ``CAPABILITY_NAMED_IAM`` when an IAM resource carries a custom name,
    ``CAPABILITY_IAM`` for the other IAM resources, ``CAPABILITY_AUTO_EXPAND``
    when the template declares a ``Transform``. ``reason_types`` are the IAM
    types behind the IAM entry.

    The default is ``GetTemplateSummary``'s rule (``_NAMED_IAM_PROPS``, every
    ``AWS::IAM::*`` type, top-level ``Transform`` only); ``strict=True`` is
    the documented rule the enforcement uses (``_CAPABILITY_IAM_TYPES``,
    ``_CAPABILITY_NAMED_PROPS``, and an embedded ``Fn::Transform`` counts as a
    macro too).
    """
    resources = template.get("Resources", {}) or {}
    named_props = _CAPABILITY_NAMED_PROPS if strict else _NAMED_IAM_PROPS
    named_iam_ids = []
    unnamed_iam_ids = []
    for logical_id, res in resources.items():
        # A member of Resources that is not a resource definition: the
        # Fn::ForEach key of an unexpanded AWS::LanguageExtensions template,
        # which ValidateTemplate and GetTemplateSummary read as it was sent.
        if not isinstance(res, dict):
            continue
        rtype = res.get("Type", "")
        if strict:
            if rtype not in _CAPABILITY_IAM_TYPES:
                continue
        elif not rtype.startswith("AWS::IAM::"):
            continue
        name_prop = named_props.get(rtype)
        if name_prop and (res.get("Properties") or {}).get(name_prop):
            named_iam_ids.append(logical_id)
        else:
            unnamed_iam_ids.append(logical_id)

    capabilities = []
    reason_types = []
    if named_iam_ids:
        capabilities.append("CAPABILITY_NAMED_IAM")
        reason_types.extend(
            sorted(set(resources[lid].get("Type", "") for lid in named_iam_ids))
        )
    elif unnamed_iam_ids:
        capabilities.append("CAPABILITY_IAM")
        reason_types.extend(
            sorted(set(resources[lid].get("Type", "") for lid in unnamed_iam_ids))
        )
    macro = _uses_macro(template) if strict else bool(template.get("Transform"))
    if macro:
        capabilities.append("CAPABILITY_AUTO_EXPAND")
    return capabilities, reason_types


def _capabilities_xml(template):
    """The ``Capabilities`` / ``CapabilitiesReason`` pair that
    GetTemplateSummary and ValidateTemplate both report, or an empty string
    when the template needs none (AWS omits both elements then)."""
    capabilities, reason_types = _required_capabilities(template)
    if not capabilities:
        return ""
    caps_xml = "".join(f"<member>{c}</member>" for c in capabilities)
    # AWS'es behavior here is very inconsistent with their docs. AWS doesn't necessarily return
    # all of the types it should every time. We're doing the best we can here.
    reason = (
        "The following resource(s) require capabilities: [" + ", ".join(reason_types) + "]"
        if reason_types else ""
    )
    return (f"<Capabilities>{caps_xml}</Capabilities>"
            f"<CapabilitiesReason>{_esc(reason)}</CapabilitiesReason>")


def _check_capabilities(sent, template, params, macros=True):
    """Refuse a template whose required capabilities the request does not
    acknowledge, the way CreateStack does: HTTP 400
    ``InsufficientCapabilitiesException`` with ``Requires capabilities :
    [CAPABILITY_IAM]`` and no stack created (measured on AWS for the IAM case).

    ``sent`` is the template as the request sent it, ``template`` the same one
    after the SAM transform. The macro rule reads ``sent``, because the
    transform drops the ``Transform`` key it looks at; the IAM rule reads
    ``template``, because a macro may add IAM resources and AWS asks for those
    to be acknowledged as well (template-macros-overview.html).

    The check runs under ``AUTH=true`` only. ``CAPABILITY_IAM`` is
    satisfied by either IAM capability, ``CAPABILITY_NAMED_IAM`` only by
    itself. Pass ``macros=False`` for ``CreateChangeSet``: the API reference
    says ``CAPABILITY_AUTO_EXPAND`` "doesn't apply to creating change sets".
    Returns an error response or ``None``."""
    if not _capabilities_enforced():
        return None
    given = set(_extract_string_members(params, "Capabilities"))
    required = _required_iam_capabilities(template)
    if macros and _uses_macro(sent):
        required.append("CAPABILITY_AUTO_EXPAND")
    missing = _missing_capabilities(given, required)
    if not missing:
        return None
    return _error("InsufficientCapabilitiesException", _insufficient_capabilities_message(missing))


def _capabilities_enforced():
    """Capabilities are IAM scope, so they are checked under ``AUTH=true`` only."""
    from ministack.app import AUTH
    return AUTH


def _required_iam_capabilities(template):
    """The capabilities a template's IAM resources demand, which is the rule
    both the request-level check and the nested-stack check enforce.
    CAPABILITY_AUTO_EXPAND is dropped here: a transform is the caller's own
    declaration, judged per call site, not a property of the IAM resources.
    """
    return [cap for cap in _required_capabilities(template, strict=True)[0]
            if cap != "CAPABILITY_AUTO_EXPAND"]


def _missing_capabilities(given, required):
    """The capabilities of ``required`` that ``given`` does not acknowledge;
    ``CAPABILITY_IAM`` is satisfied by ``CAPABILITY_NAMED_IAM`` as well."""
    missing = []
    for cap in required:
        if cap == "CAPABILITY_IAM" and "CAPABILITY_NAMED_IAM" in given:
            continue
        if cap not in given:
            missing.append(cap)
    return missing


def _insufficient_capabilities_message(missing):
    return "Requires capabilities : [" + ", ".join(missing) + "]"


# --- CreateStack ---

def _create_stack(params):
    from ministack.services.cloudformation import _stack_events, _stacks

    from .helpers import _resolve_document
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")
    # The request-level constraints, joined into one message as the API does.
    if request_error := _request_problems(params, stack_name):
        return request_error
    if problems := enum_problems(params, "OnFailure", "onFailure", ON_FAILURE_VALUES):
        return _error("ValidationError", validation_error_message(problems))
    on_failure = _p(params, "OnFailure")
    # "You can specify either OnFailure or DisableRollback, but not both"
    # (API_CreateStack); the error wording is not measured.
    if on_failure and "DisableRollback" in params:
        return _error("ValidationError",
                      "You can specify either DisableRollback or OnFailure, but not both.")

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

    provided_params = _extract_members(params, "Parameters")
    try:
        template = sent = _parse_template(template_body)
        template = _apply_transforms(template, provided_params)
    except Exception as e:
        return _error("ValidationError", f"Template format error: {e}")
    tags = _extract_members(params, "Tags")
    # OnFailure=DO_NOTHING is DisableRollback=true; DELETE rolls back and then
    # deletes the stack; ROLLBACK is the default.
    disable_rollback = (_p(params, "DisableRollback", "false").lower() == "true"
                        or on_failure == "DO_NOTHING")
    retain_except_on_create = _p(params, "RetainExceptOnCreate", "false").lower() == "true"
    from .helpers import _validate_stack_tags
    tags_error = _validate_stack_tags(tags)
    if tags_error:
        return tags_error

    # The macro rule reads the template as sent (the SAM transform above drops
    # the Transform key), the IAM rule the transformed one (a macro can add
    # IAM resources, and AWS asks for those to be acknowledged too).
    if caps_error := _check_capabilities(sent, template, params):
        return caps_error

    # Resolve parameters
    try:
        param_values = _resolve_parameters(template, provided_params)
    except ValueError as exc:
        return _error("ValidationError", str(exc))

    conditions = _evaluate_conditions(template, param_values)
    try:
        validate_template_support(template, conditions, params=param_values)
    except ValueError as exc:
        return _error("ValidationError", str(exc))

    stack_id = (
        f"arn:aws:cloudformation:{get_region()}:{get_account_id()}:"
        f"stack/{stack_name}/{new_uuid()}"
    )

    termination_protection = (
        _p(params, "EnableTerminationProtection", "false").lower() == "true")
    stack_policy, policy_err = _resolve_document(
        params, "StackPolicyBody", "StackPolicyURL", "Stack policy")
    if policy_err:
        return policy_err

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
        "Capabilities": _extract_string_members(params, "Capabilities"),
        "Outputs": [],
        "DisableRollback": disable_rollback,
        "EnableTerminationProtection": termination_protection,
        "_stack_policy": stack_policy or "",
        "_region": get_region(),
        "_resources": {},
        "_template": template,
        "_template_body": template_body,
        "_resolved_params": param_values,
        "_conditions": conditions,
    }
    _stacks[stack_name] = stack
    _stack_events[stack_id] = []

    _add_event(stack_id, stack_name, stack_name,
               "AWS::CloudFormation::Stack", "CREATE_IN_PROGRESS",
               physical_id=stack_id)

    deploy = _deploy_stack_async(stack_name, stack_id, template,
                                 param_values, disable_rollback, tags,
                                 retain_except_on_create=retain_except_on_create)
    if on_failure == "DELETE":
        deploy = _create_then_delete_on_failure(stack_name, stack_id, deploy)
    _create_stack_task_in_region(deploy, stack, stack_id)

    return _xml(200, "CreateStackResponse",
                f"<CreateStackResult><StackId>{stack_id}</StackId></CreateStackResult>")


# --- DescribeStacks ---

def _resolve_stack(stack_name):
    """Find a stack by name or by its unique stack ID.

    Every CloudFormation operation that takes a ``StackName`` accepts "the name
    or the unique stack ID" per the API reference, and the CDK CLI uses the ID:
    it reads the stack once and then addresses it by ARN, so
    ``DeleteStack(StackName="arn:aws:cloudformation:...:stack/name/uuid")`` is
    what `cdk destroy` actually sends. Four handlers already carried this
    fallback inline; the rest looked up the name only, so an ARN missed — and in
    DeleteStack's case the miss was indistinguishable from "no such stack",
    which is answered with 200 success. `cdk destroy` therefore reported
    ``Failed to destroy <stack>: UPDATE_COMPLETE`` and left every resource in
    place, after a DeleteStack that had returned OK.
    """
    from ministack.services.cloudformation import _stacks

    stack = _stacks.get(stack_name)
    if stack is not None:
        return stack
    for candidate in _stacks.values():
        if candidate.get("StackId") == stack_name:
            return candidate
    return None


def _describe_stacks(params):
    from ministack.services.cloudformation import _stacks
    stack_name = _p(params, "StackName")

    if stack_name:
        stack = _stacks.get(stack_name)
        # A DELETE_COMPLETE stack is addressable only by its unique stack ID, not
        # by name — real CloudFormation returns "does not exist" for a deleted
        # stack's name. Drop the name match and fall through to the stack-ID
        # lookup (which a plain name never satisfies).
        if stack is not None and stack.get("StackStatus") == "DELETE_COMPLETE":
            stack = None
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

    stacks_to_describe, next_token_xml, err = _page(stacks_to_describe, params, "DescribeStacks")
    if err:
        return err
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

        caps_xml = "".join(
            f"<member>{_esc(c)}</member>" for c in s.get("Capabilities", []))
        deletion_mode_xml = (f"<DeletionMode>{s['DeletionMode']}</DeletionMode>"
                             if s.get("DeletionMode") else "")

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
            "<EnableTerminationProtection>"
            f"{str(s.get('EnableTerminationProtection', False)).lower()}"
            "</EnableTerminationProtection>"
            f"{deletion_mode_xml}"
            f"<Capabilities>{caps_xml}</Capabilities>"
            f"<Parameters>{params_xml}</Parameters>"
            f"<Outputs>{outputs_xml}</Outputs>"
            f"<Tags>{tags_xml}</Tags>"
            f"{_stack_drift_information_xml(s)}"
            "</member>"
        )

    return _xml(200, "DescribeStacksResponse",
                f"<DescribeStacksResult><Stacks>{members}</Stacks>"
                f"{next_token_xml}</DescribeStacksResult>")


# --- ListStacks ---

def _list_stacks(params):
    from ministack.services.cloudformation import _stacks
    status_filters = _extract_stack_status_filters(params)
    listed = [
        s for s in _stacks.values()
        if not status_filters or s.get("StackStatus", "") in status_filters
    ]
    listed, next_token_xml, err = _page(listed, params, "ListStacks")
    if err:
        return err

    summaries = ""
    for s in listed:
        status = s.get("StackStatus", "")
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
        entry += _stack_drift_information_xml(s)
        entry += "</member>"
        summaries += entry

    return _xml(200, "ListStacksResponse",
                f"<ListStacksResult><StackSummaries>{summaries}</StackSummaries>"
                f"{next_token_xml}</ListStacksResult>")


# --- DescribeStackEvents ---

def _describe_stack_events(params):
    from ministack.services.cloudformation import _stack_events
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")

    stack = _resolve_stack(stack_name)
    if not stack:
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")

    stack_id = stack["StackId"]
    events = _stack_events.get(stack_id, [])
    # Newest first; events of one millisecond in reverse emission order
    events_sorted = [
        e for _, e in sorted(
            enumerate(events),
            key=lambda pair: (pair[1].get("Timestamp", ""), pair[0]),
            reverse=True)
    ]
    events_sorted, next_token_xml, err = _page(events_sorted, params, "DescribeStackEvents")
    if err:
        return err

    members = ""
    for e in events_sorted:
        token_xml = (f"<ClientRequestToken>{_esc(e['ClientRequestToken'])}</ClientRequestToken>"
                     if e.get("ClientRequestToken") else "")
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
            f"{token_xml}"
            "</member>"
        )

    return _xml(200, "DescribeStackEventsResponse",
                f"<DescribeStackEventsResult><StackEvents>{members}</StackEvents>"
                f"{next_token_xml}</DescribeStackEventsResult>")


# --- DescribeStackResource ---

def _resource_status_reason_xml(res):
    """The ``ResourceStatusReason`` element of a stack resource, empty when
    the record carries none (a healthy resource has no reason on AWS)."""
    reason = res.get("ResourceStatusReason")
    if not reason:
        return ""
    return f"<ResourceStatusReason>{_esc(reason)}</ResourceStatusReason>"


def _resource_metadata_xml(stack, logical_id):
    """The ``Metadata`` element of ``StackResourceDetail``: the resource's
    ``Metadata`` attribute as a JSON string, intrinsics resolved the way a
    property is (AWS interprets ``Ref``/``Fn::GetAtt`` inside it), empty when
    the template declares none."""
    template = stack.get("_template") or {}
    res_def = (template.get("Resources") or {}).get(logical_id) or {}
    metadata = res_def.get("Metadata")
    if metadata is None:
        return ""
    try:
        resolved = _resolve_refs(
            copy.deepcopy(metadata), stack.get("_resources", {}),
            stack.get("_resolved_params", {}), stack.get("_conditions", {}),
            template.get("Mappings", {}), stack.get("StackName", ""),
            stack.get("StackId", ""))
        # ``Metadata: {"Ref": "AWS::NoValue"}`` resolves the whole attribute
        # away; the literal is what the template declared.
        body = json.dumps(metadata if resolved is _NO_VALUE else resolved)
    except Exception as exc:  # the literal is still better than nothing
        logger.warning("Metadata of %s left unresolved: %s", logical_id, exc)
        body = json.dumps(metadata)
    return f"<Metadata>{_esc(body)}</Metadata>"


def _describe_stack_resource(params):
    stack_name = _p(params, "StackName")
    logical_id = _p(params, "LogicalResourceId")

    stack = _resolve_stack(stack_name)
    if not stack:
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")
    stack_name = stack.get("StackName", stack_name)

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
        f"{_resource_status_reason_xml(res)}"
        f"<LastUpdatedTimestamp>{res.get('Timestamp', '')}</LastUpdatedTimestamp>"
        f"<StackName>{_esc(stack_name)}</StackName>"
        f"<StackId>{_esc(stack['StackId'])}</StackId>"
        f"{_resource_metadata_xml(stack, logical_id)}"
        f"{_resource_drift_information_xml(res)}"
    )

    return _xml(200, "DescribeStackResourceResponse",
                f"<DescribeStackResourceResult>"
                f"<StackResourceDetail>{detail}</StackResourceDetail>"
                f"</DescribeStackResourceResult>")


# --- DescribeStackResources ---

def _describe_stack_resources(params):
    stack_name = _p(params, "StackName")
    logical_resource_id = _p(params, "LogicalResourceId")

    stack = _resolve_stack(stack_name)
    if not stack:
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")
    stack_name = stack.get("StackName", stack_name)

    resources = stack.get("_resources", {})

    if logical_resource_id:
        if logical_resource_id not in resources:
            return _error("ValidationError",
                          f"Resource [{logical_resource_id}] does not exist in stack [{stack_name}]")
        items = [(logical_resource_id, resources[logical_resource_id])]
    else:
        items = list(resources.items())

    members = ""
    for logical_id, res in items:
        members += (
            "<member>"
            f"<LogicalResourceId>{_esc(logical_id)}</LogicalResourceId>"
            f"<PhysicalResourceId>{_esc(res.get('PhysicalResourceId', ''))}</PhysicalResourceId>"
            f"<ResourceType>{_esc(res.get('ResourceType', ''))}</ResourceType>"
            f"<ResourceStatus>{res.get('ResourceStatus', '')}</ResourceStatus>"
            f"{_resource_status_reason_xml(res)}"
            f"<Timestamp>{res.get('Timestamp', '')}</Timestamp>"
            f"<StackName>{_esc(stack_name)}</StackName>"
            f"<StackId>{_esc(stack['StackId'])}</StackId>"
            f"{_resource_drift_information_xml(res)}"
            "</member>"
        )

    return _xml(200, "DescribeStackResourcesResponse",
                f"<DescribeStackResourcesResult>"
                f"<StackResources>{members}</StackResources>"
                f"</DescribeStackResourcesResult>")


# --- ListStackResources ---

def _list_stack_resources(params):
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")

    stack = _resolve_stack(stack_name)
    if not stack:
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")

    resources = stack.get("_resources", {})
    listed, next_token_xml, err = _page(list(resources.items()), params, "ListStackResources")
    if err:
        return err
    members = ""
    for logical_id, res in listed:
        members += (
            "<member>"
            f"<LogicalResourceId>{_esc(logical_id)}</LogicalResourceId>"
            f"<PhysicalResourceId>{_esc(res.get('PhysicalResourceId', ''))}</PhysicalResourceId>"
            f"<ResourceType>{_esc(res.get('ResourceType', ''))}</ResourceType>"
            f"<ResourceStatus>{res.get('ResourceStatus', '')}</ResourceStatus>"
            f"<LastUpdatedTimestamp>{res.get('Timestamp', '')}</LastUpdatedTimestamp>"
            f"{_resource_drift_information_xml(res)}"
            "</member>"
        )

    return _xml(200, "ListStackResourcesResponse",
                f"<ListStackResourcesResult>"
                f"<StackResourceSummaries>{members}</StackResourceSummaries>"
                f"{next_token_xml}</ListStackResourcesResult>")


# --- GetTemplate ---

def _processed_template_body(record):
    """The ``Processed`` stage of a stack's or change set's template: the
    template after its transforms (SAM, ``AWS::LanguageExtensions``,
    ``AWS::Include``) as JSON. A template that declares no transform is
    returned as it was sent: "If the template doesn't include transforms,
    Original and Processed return the same template" (API_GetTemplate)."""
    body = record.get("_template_body") or "{}"
    try:
        uses_transform = _uses_macro(_parse_template(body))
    except Exception:
        uses_transform = False
    if not uses_transform or not record.get("_template"):
        return body
    return json.dumps(record["_template"], default=str)


def _get_template(params):
    from .changesets import _find_change_set
    if problems := enum_problems(params, "TemplateStage", "templateStage",
                                 TEMPLATE_STAGE_VALUES):
        return _error("ValidationError", validation_error_message(problems))
    stack_name = _p(params, "StackName")
    cs_name = _p(params, "ChangeSetName")

    if cs_name:
        # "If you specify a name, you must also specify the StackName"
        # (API_GetTemplate); the error wording is not measured.
        if not stack_name and not cs_name.startswith("arn:"):
            return _error("ValidationError",
                          "StackName must be specified if ChangeSetName is not specified as an ARN.")
        stack = _resolve_stack(stack_name) if stack_name else None
        _, record = _find_change_set(
            cs_name, stack.get("StackName", stack_name) if stack else stack_name)
        if not record:
            return _error("ChangeSetNotFound",
                          f"ChangeSet [{cs_name}] does not exist", 404)
    else:
        record = _resolve_stack(stack_name)
        if not record:
            return _error("ValidationError",
                          f"Stack [{stack_name}] does not exist")

    if _p(params, "TemplateStage", "Processed") == "Processed":
        template_body = _processed_template_body(record)
    else:
        template_body = record.get("_template_body") or "{}"
    # Both stages of a stack are always available; a change set's Processed
    # stage is available once it is created, which CreateChangeSet finishes
    # before it answers.
    stages_xml = "".join(f"<member>{s}</member>" for s in TEMPLATE_STAGE_VALUES)
    return _xml(200, "GetTemplateResponse",
                f"<GetTemplateResult>"
                f"<TemplateBody>{_esc(template_body)}</TemplateBody>"
                f"<StagesAvailable>{stages_xml}</StagesAvailable>"
                f"</GetTemplateResult>")


# --- DeleteStack ---

def _imported_export_names(stack, stack_name):
    """The export names a stack imports: every ``Fn::ImportValue`` in its
    Resources, Outputs and Conditions, with the argument resolved against
    the stack's own parameters, mappings and resources (``Fn::Sub`` and
    ``Ref`` inside the argument are common). Only such an import blocks the
    deletion of the exporting stack on AWS; a template that merely mentions
    the export name in a string, or in ``Metadata``, does not."""
    template = stack.get("_template", {})
    names = set()

    def walk(node):
        if isinstance(node, dict):
            if len(node) == 1 and "Fn::ImportValue" in node:
                arg = node["Fn::ImportValue"]
                try:
                    resolved = _resolve_refs(
                        arg, stack.get("_resources", {}),
                        stack.get("_resolved_params", {}),
                        stack.get("_conditions", {}),
                        template.get("Mappings", {}),
                        stack_name, stack.get("StackId", ""))
                except Exception:
                    resolved = arg
                names.add(str(resolved))
                return
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    for section in ("Resources", "Outputs", "Conditions"):
        walk(template.get(section, {}))
    return names


def _delete_stack(params):
    from ministack.services.cloudformation import _stacks
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")
    if problems := enum_problems(params, "DeletionMode", "deletionMode", DELETION_MODE_VALUES):
        return _error("ValidationError", validation_error_message(problems))
    force = _p(params, "DeletionMode") == "FORCE_DELETE_STACK"

    stack = _resolve_stack(stack_name)
    if not stack:
        # AWS returns success for deleting non-existent stacks
        return _xml(200, "DeleteStackResponse", "")

    # From here on, address the stack by its NAME rather than by whatever the
    # caller passed: `_stacks` is keyed by name, so a caller who supplied a
    # stack ID would otherwise sail past the lookup above and then miss in
    # `_delete_stack_async`, which does its own `_stacks.get(stack_name)` and
    # returns silently — leaving the stack untouched behind a 200.
    stack_name = stack.get("StackName", stack_name)

    if stack.get("StackStatus") == "DELETE_COMPLETE":
        return _xml(200, "DeleteStackResponse", "")

    if stack.get("EnableTerminationProtection"):
        return _error("ValidationError",
                      f"Stack [{stack['StackId']}] cannot be deleted while "
                      "TerminationProtection is enabled")

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
                if export_name in _imported_export_names(other_stack, other_name):
                    return _error("ValidationError",
                                  f"Export {export_name} is imported by stack {other_name}")

    stack_id = stack["StackId"]

    # FORCE_DELETE_STACK: only for a DELETE_FAILED stack. The wording is the
    # one a third party recorded from AWS (DevelopersIO, ap-northeast-3),
    # not measured here.
    if force and stack.get("StackStatus") != "DELETE_FAILED":
        return _error("ValidationError",
                      f"Invalid operation on stack [{stack_id}]. You can activate "
                      "DeletionMode FORCE_DELETE_STACK in a delete stack operation only "
                      "when the stack is in the DELETE_FAILED state.")

    # RetainResources: only for a DELETE_FAILED stack, only its own resources.
    retain = _extract_string_members(params, "RetainResources")
    if retain:
        if stack.get("StackStatus") != "DELETE_FAILED":
            return _error("ValidationError",
                          f"Stack [{stack_name}] is not in DELETE_FAILED state; "
                          "RetainResources can only be specified for a stack in "
                          "DELETE_FAILED state")
        unknown = sorted(set(retain) - set(stack.get("_resources", {})))
        if unknown:
            return _error("ValidationError",
                          f"Resource(s) [{', '.join(unknown)}] do not exist in "
                          f"stack [{stack_name}]")

    # Deleting a stack removes its change sets; they must not outlive it and
    # shadow a later same-named change set on a re-created stack. #1418
    from ministack.services.cloudformation import _change_sets
    for _cid in [c for c, v in _change_sets.items()
                 if v.get("StackId") == stack_id]:
        _change_sets.pop(_cid, None)

    if _p(params, "DeletionMode"):
        stack["DeletionMode"] = _p(params, "DeletionMode")

    _create_stack_task_in_region(
        _delete_stack_async(stack_name, stack_id, frozenset(retain), force=force),
        stack,
        stack_id,
    )

    return _xml(200, "DeleteStackResponse", "")


# --- UpdateStack ---

def _stack_has_no_updates(stack, template, param_values, tags,
                          use_previous_template=False, tags_given=False):
    """True when an UpdateStack would change nothing: the template equals the
    one the stack runs, every parameter resolves to its current value, and the
    request either carries no tags or the tags the stack already has, in any
    order. Real CloudFormation refuses such a request with ``No updates are to
    be performed.`` instead of running an empty update. A template body that
    carries a dynamic reference is the exception: the update is accepted
    (with ``UsePreviousTemplate`` it is still refused) — measured on AWS."""
    if template != stack.get("_template", {}):
        return False
    if not use_previous_template and _has_dynamic_references(template):
        return False
    current = {k: v.get("Value") for k, v in stack.get("_resolved_params", {}).items()}
    if {k: v.get("Value") for k, v in param_values.items()} != current:
        return False
    return not _stack_tags_changed(stack, tags, tags_given)


def _update_stack(params):

    from .helpers import _resolve_document
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")

    stack = _resolve_stack(stack_name)
    # A deleted stack is not addressable by name — updating it is "does not
    # exist", not "cannot be updated" (a deployed stack name is free to re-create).
    if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")

    # Address the stack by NAME from here on, as _delete_stack does: `_stacks`
    # is keyed by name, and `_deploy_stack_async` re-looks the stack up by this
    # value — handed the caller's stack ID it misses, returns silently, and the
    # stack is left UPDATE_IN_PROGRESS forever.
    stack_name = stack.get("StackName", stack_name)

    current_status = stack.get("StackStatus", "")
    if current_status not in ("CREATE_COMPLETE", "UPDATE_COMPLETE",
                               "UPDATE_ROLLBACK_COMPLETE", "IMPORT_COMPLETE",
                               "IMPORT_ROLLBACK_COMPLETE"):
        return _error("ValidationError",
                      f"Stack [{stack_name}] is in {current_status} state "
                      f"and cannot be updated")

    template_body, resolve_err = _resolve_template(params)
    if resolve_err:
        return resolve_err
    use_previous_template = False
    if not template_body:
        # Use previous template if UsePreviousTemplate
        if _p(params, "UsePreviousTemplate", "false").lower() == "true":
            template_body = stack.get("_template_body", "{}")
            use_previous_template = True
        else:
            return _error("ValidationError", "TemplateBody or TemplateURL is required")

    provided_params = _extract_members(params, "Parameters")
    try:
        template = sent = _parse_template(template_body)
        template = _apply_transforms(template, provided_params,
                                     stack.get("_resolved_params", {}))
    except Exception as e:
        return _error("ValidationError", f"Template format error: {e}")
    tags = _extract_members(params, "Tags")
    disable_rollback = _p(params, "DisableRollback", "false").lower() == "true"
    retain_except_on_create = _p(params, "RetainExceptOnCreate", "false").lower() == "true"
    # botocore sends an empty Tags list as ``Tags=``: given and empty clears
    # the stack's tags ("If you specify an empty value, CloudFormation
    # removes all associated tags"); an omitted Tags keeps them.
    tags_given = "Tags" in params or bool(tags)
    from .helpers import _validate_stack_tags
    tags_error = _validate_stack_tags(tags)
    if tags_error:
        return tags_error

    # The macro rule reads the template as sent (the SAM transform above drops
    # the Transform key), the IAM rule the transformed one (a macro can add
    # IAM resources, and AWS asks for those to be acknowledged too).
    if caps_error := _check_capabilities(sent, template, params):
        return caps_error

    try:
        param_values = _resolve_parameters(
            template, provided_params, stack.get("_resolved_params", {}))
    except ValueError as exc:
        return _error("ValidationError", str(exc))

    if use_previous_template and stack.get("_template"):
        # The stored template is the processed one: an AWS::Include snippet
        # edited or removed in S3 since the deploy is not picked up (the
        # transform reference: "your stack doesn't automatically pick up
        # those changes"), and an Fn::ForEach is not expanded again for a new
        # parameter value (measured).
        template = copy.deepcopy(stack["_template"])
    try:
        validate_template_support(
            template, _evaluate_conditions(template, param_values), params=param_values)
    except ValueError as exc:
        return _error("ValidationError", str(exc))

    if _stack_has_no_updates(stack, template, param_values, tags,
                             use_previous_template, tags_given):
        return _error("ValidationError", "No updates are to be performed.")

    # Save previous state for rollback
    previous_stack = {
        "_resources": copy.deepcopy(stack.get("_resources", {})),
        "_template": copy.deepcopy(stack.get("_template", {})),
        "_template_body": stack.get("_template_body", ""),
        "_resolved_params": copy.deepcopy(stack.get("_resolved_params", {})),
        "_conditions": copy.deepcopy(stack.get("_conditions", {})),
        "Parameters": copy.deepcopy(stack.get("Parameters", [])),
        "Tags": copy.deepcopy(stack.get("Tags", [])),
        "Outputs": copy.deepcopy(stack.get("Outputs", [])),
    }

    stack_id = stack["StackId"]

    # A direct stack update supersedes any pending change set on the stack —
    # real AWS marks them OBSOLETE (they no longer reflect the stack). #1418
    from ministack.services.cloudformation import _change_sets
    for _cs in _change_sets.values():
        if (_cs.get("StackId") == stack_id
                and _cs.get("ExecutionStatus") == "AVAILABLE"):
            _cs["ExecutionStatus"] = "OBSOLETE"

    stack_policy, policy_err = _resolve_document(
        params, "StackPolicyBody", "StackPolicyURL", "Stack policy")
    if policy_err:
        return policy_err
    if stack_policy:
        stack["_stack_policy"] = stack_policy

    stack["StackStatus"] = "UPDATE_IN_PROGRESS"
    stack["LastUpdatedTime"] = now_iso()
    stack["_template_body"] = template_body
    # The capabilities a stack reports are the ones its last operation
    # acknowledged, so an update replaces them rather than adding to them.
    stack["Capabilities"] = _extract_string_members(params, "Capabilities")
    if tags or tags_given:
        stack["Tags"] = tags
    stack["Parameters"] = [
        {"ParameterKey": k, "ParameterValue": v["Value"], "NoEcho": v["NoEcho"]}
        for k, v in param_values.items()
    ]
    with _stack_region_context(stack, stack_id):
        stack["_conditions"] = _evaluate_conditions(template, param_values)

        _add_event(stack_id, stack_name, stack_name,
                   "AWS::CloudFormation::Stack", "UPDATE_IN_PROGRESS",
                   physical_id=stack_id)

        _create_stack_task_in_region(
            _deploy_stack_async(stack_name, stack_id, template,
                                param_values, disable_rollback, tags,
                                is_update=True, previous_stack=previous_stack,
                                retain_except_on_create=retain_except_on_create),
            stack,
            stack_id,
        )

    return _xml(200, "UpdateStackResponse",
                f"<UpdateStackResult><StackId>{stack_id}</StackId></UpdateStackResult>")


# --- ValidateTemplate ---

def _validate_template(params):
    template_body, resolve_err = _resolve_template(params)
    if resolve_err:
        return resolve_err
    if not template_body:
        return _error("ValidationError", "TemplateBody is required")

    try:
        template = _parse_template(template_body)
    except Exception as e:
        return _error("ValidationError", f"Template format error: {e}")
    if "Resources" not in template:
        return _error("ValidationError",
                      "Template format error: At least one Resources member must be defined.")
    # An Fn::ForEach in a template that declares no transform is refused as
    # CreateStack refuses it; a declaring template is not expanded (measured).
    try:
        refuse_undeclared_language_extensions(template)
    except ValueError as exc:
        return _error("ValidationError", str(exc))
    try:
        conditions = _evaluate_conditions(
            template, _resolve_parameters(template, []))
    except Exception:
        # Parameters without defaults: validate every resource, conditions
        # unknown.
        conditions = {}
    try:
        validate_template_support(template, conditions)
    except ValueError as exc:
        return _error("ValidationError", str(exc))
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

    transforms_xml = "".join(
        f"<member>{_esc(t)}</member>" for t in declared_transforms(template))
    declared_block = (f"<DeclaredTransforms>{transforms_xml}</DeclaredTransforms>"
                      if transforms_xml else "")

    return _xml(200, "ValidateTemplateResponse",
                f"<ValidateTemplateResult>"
                f"<Description>{_esc(description)}</Description>"
                f"<Parameters>{params_xml}</Parameters>"
                f"{_capabilities_xml(template)}"
                f"{declared_block}"
                f"</ValidateTemplateResult>")


# --- ListExports ---

def _list_exports(params):
    from ministack.services.cloudformation import _exports
    listed, next_token_xml, err = _page(list(_exports.items()), params, "ListExports")
    if err:
        return err
    members = ""
    for name, exp in listed:
        members += (
            "<member>"
            f"<ExportingStackId>{_esc(exp.get('StackId', ''))}</ExportingStackId>"
            f"<Name>{_esc(name)}</Name>"
            f"<Value>{_esc(str(exp.get('Value', '')))}</Value>"
            "</member>"
        )

    return _xml(200, "ListExportsResponse",
                f"<ListExportsResult><Exports>{members}</Exports>"
                f"{next_token_xml}</ListExportsResult>")
# --- GetTemplateSummary ---

def _get_template_summary(params):
    template_body, resolve_err = _resolve_template(params)
    if resolve_err:
        return resolve_err
    stack_name = _p(params, "StackName")

    if stack_name and not template_body:
        stack = _resolve_stack(stack_name)
        if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
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

    # A template declaring AWS::LanguageExtensions is not expanded here, and an
    # account answers no ResourceTypes and no Capabilities for one (measured).
    unexpanded = "AWS::LanguageExtensions" in declared_transforms(template)

    # Resource types
    resource_types = [] if unexpanded else sorted(set(
        r.get("Type", "") for r in resources.values()
    ))
    types_xml = "".join(f"<member>{_esc(t)}</member>" for t in resource_types)
    types_block = "" if unexpanded else f"<ResourceTypes>{types_xml}</ResourceTypes>"

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

    caps_block = "" if unexpanded else _capabilities_xml(template)
    transforms_xml = "".join(
        f"<member>{_esc(t)}</member>" for t in declared_transforms(template))
    declared_block = (f"<DeclaredTransforms>{transforms_xml}</DeclaredTransforms>"
                      if transforms_xml else "")

    return _xml(200, "GetTemplateSummaryResponse",
                f"<GetTemplateSummaryResult>"
                f"<Description>{_esc(description)}</Description>"
                f"{types_block}"
                f"<Parameters>{params_xml}</Parameters>"
                f"{caps_block}"
                f"{declared_block}"
                f"</GetTemplateSummaryResult>")


# --- ListImports ---

def _list_imports(params):
    from ministack.services.cloudformation import _stacks
    export_name = _p(params, "ExportName")
    if not export_name:
        return _error("ValidationError", "ExportName is required")
    importers = sorted(
        name for name, stack in _stacks.items()
        if stack.get("StackStatus", "").endswith("_COMPLETE")
        and "DELETE" not in stack.get("StackStatus", "")
        and export_name in _imported_export_names(stack, name)
    )
    if not importers:
        return _error("ValidationError",
                      f"Export '{export_name}' is not imported by any stack.")
    importers, next_token_xml, err = _page(importers, params, "ListImports")
    if err:
        return err
    members = "".join(f"<member>{_esc(n)}</member>" for n in importers)
    return _xml(200, "ListImportsResponse",
                f"<ListImportsResult><Imports>{members}</Imports>"
                f"{next_token_xml}</ListImportsResult>")


# --- UpdateTerminationProtection / stack policy ---

def _update_termination_protection(params):
    stack_name = _p(params, "StackName")
    stack = _resolve_stack(stack_name)
    if not stack or stack.get("StackStatus") in ("DELETE_IN_PROGRESS", "DELETE_COMPLETE"):
        return _error("ValidationError", f"Stack [{stack_name}] does not exist")
    enable = _p(params, "EnableTerminationProtection")
    if not enable:
        return _error("ValidationError", "EnableTerminationProtection is required")
    stack["EnableTerminationProtection"] = enable.lower() == "true"
    return _xml(200, "UpdateTerminationProtectionResponse",
                "<UpdateTerminationProtectionResult>"
                f"<StackId>{_esc(stack['StackId'])}</StackId>"
                "</UpdateTerminationProtectionResult>")


def _set_stack_policy(params):
    from .helpers import _resolve_document
    stack_name = _p(params, "StackName")
    stack = _resolve_stack(stack_name)
    if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
        return _error("ValidationError", f"Stack [{stack_name}] does not exist")
    policy, policy_err = _resolve_document(
        params, "StackPolicyBody", "StackPolicyURL", "Stack policy")
    if policy_err:
        return policy_err
    if not policy:
        return _error("ValidationError", "StackPolicyBody or StackPolicyURL is required")
    try:
        json.loads(policy)
    except ValueError:
        return _error("ValidationError", "Error validating stack policy: Invalid stack policy")
    stack["_stack_policy"] = policy
    return _xml(200, "SetStackPolicyResponse", "")


def _get_stack_policy(params):
    stack_name = _p(params, "StackName")
    stack = _resolve_stack(stack_name)
    if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
        return _error("ValidationError", f"Stack [{stack_name}] does not exist")
    policy = stack.get("_stack_policy") or ""
    body = f"<StackPolicyBody>{_esc(policy)}</StackPolicyBody>" if policy else ""
    return _xml(200, "GetStackPolicyResponse",
                f"<GetStackPolicyResult>{body}</GetStackPolicyResult>")


# --- CancelUpdateStack / ContinueUpdateRollback ---

def _cancel_update_stack(params):
    stack_name = _p(params, "StackName")
    stack = _resolve_stack(stack_name)
    if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
        return _error("ValidationError", f"Stack [{stack_name}] does not exist")
    if stack.get("StackStatus") != "UPDATE_IN_PROGRESS":
        return _error("ValidationError",
                      "CancelUpdateStack cannot be called from current stack status")
    # The running update checks the flag before each resource and rolls back;
    # the rollback events carry the cancel's ClientRequestToken.
    stack["_cancel_requested"] = True
    stack["_cancel_token"] = CLIENT_REQUEST_TOKEN.get()
    return _xml(200, "CancelUpdateStackResponse", "")


def _continue_update_rollback(params):
    stack_name = _p(params, "StackName")
    stack = _resolve_stack(stack_name)
    if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
        return _error("ValidationError", f"Stack [{stack_name}] does not exist")
    stack_name = stack.get("StackName", stack_name)
    if stack.get("StackStatus") != "UPDATE_ROLLBACK_FAILED":
        return _error("ValidationError",
                      "ContinueUpdateRollback cannot be called from current stack status")
    # ResourcesToSkip.member.N on the query protocol, a plain list on the JSON one.
    skip = params.get("ResourcesToSkip")
    if not isinstance(skip, list):
        skip = []
        while _p(params, f"ResourcesToSkip.member.{len(skip) + 1}"):
            skip.append(_p(params, f"ResourcesToSkip.member.{len(skip) + 1}"))
    skip = [str(s) for s in skip]
    pending = stack.get("_rollback_failed", {})
    unknown = sorted(set(skip) - set(pending))
    if unknown:
        return _error("ValidationError",
                      f"Resource(s) [{', '.join(unknown)}] cannot be skipped: only "
                      "resources whose rollback failed can be skipped")
    stack_id = stack["StackId"]
    _create_stack_task_in_region(
        _continue_update_rollback_async(stack_name, stack_id, frozenset(skip)),
        stack,
        stack_id,
    )
    return _xml(200, "ContinueUpdateRollbackResponse",
                "<ContinueUpdateRollbackResult></ContinueUpdateRollbackResult>")


# --- RollbackStack ---

def _rollback_stack(params):
    """RollbackStack: roll a stack that failed with ``DisableRollback`` back
    to its last known stable state. ``CREATE_FAILED`` has none, so what the
    create made is deleted and the stack ends ``ROLLBACK_COMPLETE``;
    ``UPDATE_FAILED`` goes back to the stack as it was before the update and
    ends ``UPDATE_ROLLBACK_COMPLETE`` (API_RollbackStack). ``RoleARN`` is
    accepted and not used."""
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")
    stack = _resolve_stack(stack_name)
    if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
        return _error("ValidationError", f"Stack [{stack_name}] does not exist")
    stack_name = stack.get("StackName", stack_name)
    status = stack.get("StackStatus", "")
    if status not in ("CREATE_FAILED", "UPDATE_FAILED"):
        return _error("ValidationError",
                      "RollbackStack cannot be called from current stack status")
    operation = stack.get("_failed_operation") or {
        # A record without the failed operation (saved before RollbackStack
        # existed): nothing is known to undo, so the stack keeps what it has
        # and only the status settles.
        "is_update": status == "UPDATE_FAILED",
        "previous_stack": {
            key: copy.deepcopy(stack.get(key)) for key in (
                "_resources", "_template", "_template_body", "_resolved_params",
                "Parameters", "Tags", "Outputs") if key in stack
        } if status == "UPDATE_FAILED" else None,
        "template": stack.get("_template") or {},
        "param_values": stack.get("_resolved_params") or {},
    }
    if "RetainExceptOnCreate" in params:
        operation = {**operation, "retain_except_on_create":
                     _p(params, "RetainExceptOnCreate", "false").lower() == "true"}
    stack_id = stack["StackId"]
    stack["StackStatus"] = ("UPDATE_ROLLBACK_IN_PROGRESS" if operation.get("is_update")
                            else "ROLLBACK_IN_PROGRESS")
    _create_stack_task_in_region(
        _roll_back_operation(stack_name, stack_id, stack, operation,
                             reason_override="User Initiated"),
        stack,
        stack_id,
    )
    return _xml(200, "RollbackStackResponse",
                f"<RollbackStackResult><StackId>{_esc(stack_id)}</StackId></RollbackStackResult>")


# --- Drift detection ---

# Detection records kept per stack; the API says the number of retained
# results "may vary".
_DRIFT_DETECTIONS_KEPT = 10


def _stack_drift_information_xml(stack):
    """``DriftInformation`` of a stack: ``NOT_CHECKED`` until a detection ran."""
    info = stack.get("DriftInformation") or {}
    status = info.get("StackDriftStatus", "NOT_CHECKED")
    checked = info.get("LastCheckTimestamp")
    ts = f"<LastCheckTimestamp>{checked}</LastCheckTimestamp>" if checked else ""
    return (f"<DriftInformation><StackDriftStatus>{status}</StackDriftStatus>{ts}"
            "</DriftInformation>")


def _resource_drift_information_xml(res):
    """``DriftInformation`` of a stack resource: ``NOT_CHECKED`` until its
    type was checked (and for good for a type without drift support)."""
    info = res.get("DriftInformation") or {}
    status = info.get("StackResourceDriftStatus", "NOT_CHECKED")
    checked = info.get("LastCheckTimestamp")
    ts = f"<LastCheckTimestamp>{checked}</LastCheckTimestamp>" if checked else ""
    return (f"<DriftInformation><StackResourceDriftStatus>{status}"
            f"</StackResourceDriftStatus>{ts}</DriftInformation>")


def _stack_resource_drift_xml(drift):
    diffs = "".join(
        "<member>"
        f"<PropertyPath>{_esc(d['PropertyPath'])}</PropertyPath>"
        f"<ExpectedValue>{_esc(d['ExpectedValue'])}</ExpectedValue>"
        f"<ActualValue>{_esc(d['ActualValue'])}</ActualValue>"
        f"<DifferenceType>{d['DifferenceType']}</DifferenceType>"
        "</member>"
        for d in drift.get("PropertyDifferences", []))
    out = (
        f"<StackId>{_esc(drift['StackId'])}</StackId>"
        f"<LogicalResourceId>{_esc(drift['LogicalResourceId'])}</LogicalResourceId>"
        f"<PhysicalResourceId>{_esc(drift.get('PhysicalResourceId', ''))}</PhysicalResourceId>"
        f"<ResourceType>{_esc(drift['ResourceType'])}</ResourceType>"
        f"<StackResourceDriftStatus>{drift['StackResourceDriftStatus']}</StackResourceDriftStatus>"
        f"<Timestamp>{drift['Timestamp']}</Timestamp>"
    )
    for key in ("ExpectedProperties", "ActualProperties", "DriftStatusReason"):
        if key in drift:
            out += f"<{key}>{_esc(drift[key])}</{key}>"
    if diffs:
        out += f"<PropertyDifferences>{diffs}</PropertyDifferences>"
    return out


def _drift_target(stack_name):
    """The stack a drift call names, or the error: it has to exist and be in
    one of the statuses the drift user guide lists."""
    if not stack_name:
        return None, _error("ValidationError", "StackName is required")
    stack = _resolve_stack(stack_name)
    if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
        return None, _error("ValidationError", f"Stack [{stack_name}] does not exist")
    status = stack.get("StackStatus", "")
    if status not in _drift.DRIFT_DETECTABLE_STATUSES:
        return None, _error(
            "ValidationError",
            f"Stack [{stack.get('StackName', stack_name)}] is in {status} state; drift "
            "detection is available in CREATE_COMPLETE, UPDATE_COMPLETE, "
            "UPDATE_ROLLBACK_COMPLETE and UPDATE_ROLLBACK_FAILED")
    return stack, None


def _detect_stack_drift(params):
    """DetectStackDrift: check every resource (or the ``LogicalResourceIds``)
    whose type has a reader against its service store, synchronously; the
    detection is ``DETECTION_COMPLETE`` by the time the call returns."""
    stack, err = _drift_target(_p(params, "StackName"))
    if err:
        return err
    resources = stack.get("_resources", {})
    wanted = _extract_string_members(params, "LogicalResourceIds")
    unknown = sorted(set(wanted) - set(resources))
    if unknown:
        return _error("ValidationError",
                      f"Resource(s) [{', '.join(unknown)}] do not exist in stack "
                      f"[{stack['StackName']}]")
    timestamp = now_iso()
    detection_id = new_uuid()
    drifted = 0
    failed = []
    for logical_id, record in resources.items():
        if wanted and logical_id not in wanted:
            continue
        if not _drift.supports_drift(record.get("ResourceType", "")):
            continue
        drift = _drift.detect_resource_drift(stack, logical_id, record, timestamp)
        _drift.record_resource_drift(record, drift)
        if drift["StackResourceDriftStatus"] in ("MODIFIED", "DELETED"):
            drifted += 1
        elif drift["StackResourceDriftStatus"] == "UNKNOWN":
            failed.append(logical_id)
    if drifted:
        stack_status = "DRIFTED"
    elif failed:
        stack_status = "UNKNOWN"
    else:
        stack_status = "IN_SYNC"
    detection = {
        "StackId": stack["StackId"],
        "StackDriftDetectionId": detection_id,
        "StackDriftStatus": stack_status,
        "DetectionStatus": "DETECTION_FAILED" if failed else "DETECTION_COMPLETE",
        "Timestamp": timestamp,
    }
    if failed:
        detection["DetectionStatusReason"] = (
            f"Failed to detect drift on resources [{', '.join(sorted(failed))}]")
    else:
        detection["DriftedStackResourceCount"] = drifted
    stack["DriftInformation"] = {"StackDriftStatus": stack_status,
                                 "LastCheckTimestamp": timestamp}
    detections = stack.setdefault("_drift_detections", {})
    detections[detection_id] = detection
    while len(detections) > _DRIFT_DETECTIONS_KEPT:
        detections.pop(next(iter(detections)))
    return _xml(200, "DetectStackDriftResponse",
                "<DetectStackDriftResult>"
                f"<StackDriftDetectionId>{detection_id}</StackDriftDetectionId>"
                "</DetectStackDriftResult>")


def _describe_stack_drift_detection_status(params):
    from ministack.services.cloudformation import _stacks
    detection_id = _p(params, "StackDriftDetectionId")
    if not detection_id:
        return _error("ValidationError", "StackDriftDetectionId is required")
    detection = None
    for stack in _stacks.values():
        detection = (stack.get("_drift_detections") or {}).get(detection_id)
        if detection:
            break
    if not detection:
        return _error("ValidationError",
                      f"Drift detection with id [{detection_id}] does not exist")
    body = (
        f"<StackId>{_esc(detection['StackId'])}</StackId>"
        f"<StackDriftDetectionId>{detection_id}</StackDriftDetectionId>"
        f"<StackDriftStatus>{detection['StackDriftStatus']}</StackDriftStatus>"
        f"<DetectionStatus>{detection['DetectionStatus']}</DetectionStatus>"
        f"<Timestamp>{detection['Timestamp']}</Timestamp>"
    )
    if detection.get("DetectionStatusReason"):
        body += (f"<DetectionStatusReason>{_esc(detection['DetectionStatusReason'])}"
                 "</DetectionStatusReason>")
    if "DriftedStackResourceCount" in detection:
        body += (f"<DriftedStackResourceCount>{detection['DriftedStackResourceCount']}"
                 "</DriftedStackResourceCount>")
    return _xml(200, "DescribeStackDriftDetectionStatusResponse",
                f"<DescribeStackDriftDetectionStatusResult>{body}"
                "</DescribeStackDriftDetectionStatusResult>")


def _detect_stack_resource_drift(params):
    stack, err = _drift_target(_p(params, "StackName"))
    if err:
        return err
    logical_id = _p(params, "LogicalResourceId")
    if not logical_id:
        return _error("ValidationError", "LogicalResourceId is required")
    record = stack.get("_resources", {}).get(logical_id)
    if not record:
        return _error("ValidationError",
                      f"Resource [{logical_id}] does not exist in stack [{stack['StackName']}]")
    rtype = record.get("ResourceType", "")
    if not _drift.supports_drift(rtype):
        # "Resources that don't currently support drift detection can't be checked."
        return _error("ValidationError",
                      f"Drift detection is not supported for resource type [{rtype}]")
    timestamp = now_iso()
    drift = _drift.detect_resource_drift(stack, logical_id, record, timestamp)
    _drift.record_resource_drift(record, drift)
    # The stack's LastCheckTimestamp covers a check of any of its resources.
    info = stack.setdefault("DriftInformation", {"StackDriftStatus": "NOT_CHECKED"})
    info["LastCheckTimestamp"] = timestamp
    return _xml(200, "DetectStackResourceDriftResponse",
                "<DetectStackResourceDriftResult><StackResourceDrift>"
                f"{_stack_resource_drift_xml(drift)}"
                "</StackResourceDrift></DetectStackResourceDriftResult>")


def _describe_stack_resource_drifts(params):
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")
    stack = _resolve_stack(stack_name)
    if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
        return _error("ValidationError", f"Stack [{stack_name}] does not exist")
    filters = _extract_string_members(params, "StackResourceDriftStatusFilters")
    bad = [f for f in filters if f not in _drift.RESOURCE_DRIFT_STATUSES]
    if bad:
        return _error(
            "ValidationError",
            f"1 validation error detected: Value '[{', '.join(filters)}]' at "
            "'stackResourceDriftStatusFilters' failed to satisfy constraint: Member "
            "must satisfy enum value set: [" + ", ".join(_drift.RESOURCE_DRIFT_STATUSES) + "]")
    page_size = 100
    max_results = _p(params, "MaxResults")
    if max_results:
        if not max_results.isdigit() or not 1 <= int(max_results) <= 100:
            return _error(
                "ValidationError",
                f"1 validation error detected: Value '{max_results}' at 'maxResults' "
                "failed to satisfy constraint: Member must have value between 1 and 100")
        page_size = int(max_results)
    # Only the resources that were checked are listed (API reference).
    drifts = [
        res["_drift"] for res in stack.get("_resources", {}).values()
        if res.get("_drift")
        and (not filters or res["_drift"]["StackResourceDriftStatus"] in filters)
    ]
    drifts, next_token_xml, err = _page(drifts, params, "DescribeStackResourceDrifts",
                                        page_size)
    if err:
        return err
    members = "".join(f"<member>{_stack_resource_drift_xml(d)}</member>" for d in drifts)
    return _xml(200, "DescribeStackResourceDriftsResponse",
                "<DescribeStackResourceDriftsResult>"
                f"<StackResourceDrifts>{members}</StackResourceDrifts>"
                f"{next_token_xml}</DescribeStackResourceDriftsResult>")


# --- SignalResource ---

def _signal_resource(params):
    """Deliver a SUCCESS or FAILURE signal to the wait condition of a stack
    that is waiting under the logical id, from anywhere other than the
    handle URL (the only way in for a wait condition with a CreationPolicy)."""
    stack_name = _p(params, "StackName")
    logical_id = _p(params, "LogicalResourceId")
    unique_id = _p(params, "UniqueId")
    status = _p(params, "Status")
    for name, value in (("StackName", stack_name), ("LogicalResourceId", logical_id),
                        ("UniqueId", unique_id), ("Status", status)):
        if not value:
            return _error("ValidationError", f"{name} is required")
    if status not in ("SUCCESS", "FAILURE"):
        return _error("ValidationError",
                      f"1 validation error detected: Value '{status}' at 'status' failed to satisfy "
                      "constraint: Member must satisfy enum value set: [FAILURE, SUCCESS]")
    if len(unique_id) > 64:
        return _error("ValidationError",
                      f"1 validation error detected: Value '{unique_id}' at 'uniqueId' failed to "
                      "satisfy constraint: Member must have length less than or equal to 64")
    stack = _resolve_stack(stack_name)
    if not stack or stack.get("StackStatus") == "DELETE_COMPLETE":
        return _error("ValidationError", f"Stack [{stack_name}] does not exist")
    stack_name = stack.get("StackName", stack_name)
    if stack.get("StackStatus") not in ("CREATE_IN_PROGRESS", "UPDATE_IN_PROGRESS"):
        return _error("ValidationError",
                      f"Stack [{stack_name}] is in {stack.get('StackStatus')} state and cannot be signaled")
    if not _wc.signal_resource(stack["StackId"], logical_id, unique_id, status):
        return _error("ValidationError",
                      f"Resource [{logical_id}] in stack [{stack_name}] is not waiting for signals")
    return _xml(200, "SignalResourceResponse", "")


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
    "ListImports": _list_imports,
    "UpdateTerminationProtection": _update_termination_protection,
    "SetStackPolicy": _set_stack_policy,
    "GetStackPolicy": _get_stack_policy,
    "CancelUpdateStack": _cancel_update_stack,
    "ContinueUpdateRollback": _continue_update_rollback,
    "RollbackStack": _rollback_stack,
    "SignalResource": _signal_resource,
    "DetectStackDrift": _detect_stack_drift,
    "DescribeStackDriftDetectionStatus": _describe_stack_drift_detection_status,
    "DetectStackResourceDrift": _detect_stack_resource_drift,
    "DescribeStackResourceDrifts": _describe_stack_resource_drifts,
}
