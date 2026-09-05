# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
CloudFormation stacks — async stack lifecycle (deploy, delete, update, diff).
"""

import asyncio
import copy
import logging
from contextlib import contextmanager

from ministack.core.concurrency import run_reentrant
from ministack.core.responses import get_region, new_uuid, now_iso, set_request_region

from .engine import (
    _NO_VALUE,
    _evaluate_conditions,
    _resolve_refs,
    _topological_sort,
)
from .provisioners import (
    _RETAIN_REPLACED,
    _delete_resource,
    _provision_resource,
    _update_resource,
)

logger = logging.getLogger("cloudformation")


def _region_from_stack_id(stack_id: str | None) -> str | None:
    if not stack_id:
        return None
    parts = stack_id.split(":")
    if len(parts) > 3 and parts[3]:
        return parts[3]
    return None


def _stack_region(stack: dict | None, stack_id: str | None = None) -> str:
    if stack:
        return stack.get("_region") or _region_from_stack_id(stack.get("StackId")) or get_region()
    return _region_from_stack_id(stack_id) or get_region()


@contextmanager
def _stack_region_context(stack: dict | None, stack_id: str | None = None):
    previous_region = get_region()
    set_request_region(_stack_region(stack, stack_id))
    try:
        yield
    finally:
        set_request_region(previous_region)


def _create_stack_task_in_region(coro, stack: dict | None, stack_id: str | None = None):
    """Schedule a stack lifecycle coroutine in the stack's owning region."""
    with _stack_region_context(stack, stack_id):
        asyncio.get_event_loop().create_task(coro)


def _is_custom_resource(resource_type: str) -> bool:
    return resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource"


# ===========================================================================
# Stack Events helper
# ===========================================================================

# The policies that keep a resource. Snapshot is not among them: the emulator
# takes no snapshots, so a Snapshot resource is deleted like a Delete one.
_RETAINING_POLICIES = ("Retain", "RetainExceptOnCreate")


def _resource_policy(res_def, attribute, resources, params, conditions, mappings,
                     stack_name, stack_id):
    """A resource's ``DeletionPolicy`` / ``UpdateReplacePolicy`` as a string,
    ``Delete`` when the template sets none. An intrinsic (``Fn::If``) is
    resolved like a property; one that does not resolve counts as ``Delete``
    and is logged, since that is the destructive reading."""
    value = (res_def or {}).get(attribute)
    if value is None:
        return "Delete"
    if isinstance(value, dict):
        try:
            value = _resolve_refs(copy.deepcopy(value), resources, params, conditions,
                                  mappings, stack_name, stack_id)
        except Exception as exc:
            logger.warning("%s of %s in %s did not resolve (%s); treating it as Delete",
                           attribute, stack_name, value, exc)
            return "Delete"
    return str(value)


def _add_event(stack_id, stack_name, logical_id, resource_type, status,
               reason="", physical_id=""):
    """Record a stack event."""
    from ministack.services.cloudformation import _stack_events
    event = {
        "StackId": stack_id,
        "StackName": stack_name,
        "EventId": new_uuid(),
        "LogicalResourceId": logical_id,
        "PhysicalResourceId": physical_id,
        "ResourceType": resource_type,
        "ResourceStatus": status,
        "ResourceStatusReason": reason,
        "Timestamp": now_iso(),
    }
    if stack_id not in _stack_events:
        _stack_events[stack_id] = []
    _stack_events[stack_id].append(event)


# ===========================================================================
# Stack Deploy / Delete / Update Logic
# ===========================================================================

def _resolve_stack_outputs(outputs_defs, conditions, resources, param_values,
                           mappings, stack_name, stack_id):
    """Resolve the Outputs section against the provisioned resources.

    Returns ``(outputs, exports)``. Nothing is written to the export table here,
    so an output that fails to resolve leaves no half-registered exports behind.
    """
    resolved_outputs = []
    exports = {}
    for out_name, out_def in outputs_defs.items():
        cond = out_def.get("Condition")
        if cond and not conditions.get(cond, True):
            continue
        out_value = _resolve_refs(
            copy.deepcopy(out_def.get("Value", "")),
            resources, param_values, conditions,
            mappings, stack_name, stack_id
        )
        output = {
            "OutputKey": out_name,
            "OutputValue": str(out_value),
            "Description": out_def.get("Description", ""),
        }
        export_def = out_def.get("Export", {})
        if export_def:
            export_name = _resolve_refs(
                copy.deepcopy(export_def.get("Name", "")),
                resources, param_values, conditions,
                mappings, stack_name, stack_id
            )
            output["ExportName"] = str(export_name)
            exports[str(export_name)] = {
                "StackId": stack_id,
                "Name": str(export_name),
                "Value": str(out_value),
            }
        resolved_outputs.append(output)
    return resolved_outputs, exports


async def _deploy_stack_async(stack_name: str, stack_id: str, template: dict,
                              param_values: dict, disable_rollback: bool,
                              tags: list, is_update: bool = False,
                              previous_stack: dict | None = None,
                              retain_except_on_create: bool = False):
    """Background task: provision resources and set final stack status.

    ``retain_except_on_create`` is the API parameter of the same name: a
    rollback of this operation then deletes what the operation created even
    when the template says ``DeletionPolicy: Retain``."""
    from ministack.services.cloudformation import _exports, _stacks
    status_prefix = "UPDATE" if is_update else "CREATE"
    stack = _stacks[stack_name]

    mappings = template.get("Mappings", {})
    conditions = _evaluate_conditions(template, param_values)
    resources_defs = template.get("Resources", {})
    outputs_defs = template.get("Outputs", {})

    # Topological sort
    try:
        ordered = _topological_sort(resources_defs, conditions)
    except ValueError as exc:
        stack["StackStatus"] = f"{status_prefix}_FAILED"
        stack["StackStatusReason"] = str(exc)
        _add_event(stack_id, stack_name, stack_name,
                   "AWS::CloudFormation::Stack", f"{status_prefix}_FAILED",
                   str(exc), stack_id)
        return

    provisioned_resources: dict = stack.get("_resources", {})
    created_in_this_run = []

    # If update: figure out what to add/modify/remove
    if is_update and previous_stack:
        old_resource_names = set(previous_stack.get("_resources", {}).keys())
        new_resource_names = set(ordered)
        to_remove = old_resource_names - new_resource_names
    else:
        to_remove = set()

    failed = False
    fail_reason = ""
    replaced_resources = []

    for logical_id in ordered:
        res_def = resources_defs[logical_id]
        cond = res_def.get("Condition")
        if cond and not conditions.get(cond, True):
            continue

        resource_type = res_def.get("Type", "AWS::CloudFormation::CustomResource")
        raw_props = res_def.get("Properties", {})

        try:
            # Resolve properties
            resolved_props = _resolve_refs(
                copy.deepcopy(raw_props), provisioned_resources, param_values,
                conditions, mappings, stack_name, stack_id
            )
            # Filter out _NO_VALUE properties at top level
            if isinstance(resolved_props, dict):
                resolved_props = {
                    k: v for k, v in resolved_props.items() if v is not _NO_VALUE
                }

            _add_event(stack_id, stack_name, logical_id, resource_type,
                       f"{status_prefix}_IN_PROGRESS")

            # On stack update, route previously-provisioned resources through
            # the type's update handler when one exists; otherwise fall back
            # to (idempotent) create. New resources go straight to create.
            prev_resource = (
                previous_stack.get("_resources", {}).get(logical_id)
                if is_update and previous_stack else None
            )
            if prev_resource:
                old_pid = prev_resource.get("PhysicalResourceId", logical_id)
                old_props = prev_resource.get("Properties", {})
                old_attrs = prev_resource.get("Attributes", {})
                # A handler that replaces the resource itself (the name-keyed
                # ones) must leave the predecessor alone when the template
                # retains it; the cleanup below then records DELETE_SKIPPED.
                retain_replaced = _resource_policy(
                    res_def, "UpdateReplacePolicy", provisioned_resources,
                    param_values, conditions, mappings, stack_name, stack_id,
                ) in _RETAINING_POLICIES
                token = _RETAIN_REPLACED.set(retain_replaced)
                try:
                    if _is_custom_resource(resource_type):
                        physical_id, attrs = await run_reentrant(
                            _update_resource, resource_type, old_pid, old_props,
                            resolved_props, stack_name, logical_id, old_attrs
                        )
                    else:
                        physical_id, attrs = _update_resource(
                            resource_type, old_pid, old_props, resolved_props,
                            stack_name, logical_id, old_attrs
                        )
                finally:
                    _RETAIN_REPLACED.reset(token)
                if physical_id != old_pid:
                    # A changed physical id is a replacement. Real
                    # CloudFormation deletes the predecessor in the
                    # UPDATE_COMPLETE_CLEANUP phase; without this the old
                    # resource leaked forever, still holding its data.
                    replaced_resources.append(
                        (logical_id, resource_type, old_pid, old_props))
            else:
                if _is_custom_resource(resource_type):
                    physical_id, attrs = await run_reentrant(
                        _provision_resource, resource_type, logical_id, resolved_props, stack_name
                    )
                else:
                    physical_id, attrs = _provision_resource(
                        resource_type, logical_id, resolved_props, stack_name
                    )
        except Exception as exc:
            logger.error("Failed to provision %s (%s): %s",
                         logical_id, resource_type, exc)
            _add_event(stack_id, stack_name, logical_id, resource_type,
                       f"{status_prefix}_FAILED", str(exc))
            failed = True
            fail_reason = f"Resource {logical_id} failed: {exc}"
            break

        provisioned_resources[logical_id] = {
            "PhysicalResourceId": physical_id,
            "ResourceType": resource_type,
            "ResourceStatus": f"{status_prefix}_COMPLETE",
            "LogicalResourceId": logical_id,
            "Properties": resolved_props,
            "Attributes": attrs,
            "Timestamp": now_iso(),
        }
        created_in_this_run.append(logical_id)

        _add_event(stack_id, stack_name, logical_id, resource_type,
                   f"{status_prefix}_COMPLETE", physical_id=physical_id)

    # Replacement cleanup (update case): delete each replaced resource's
    # predecessor, as real CloudFormation does after UPDATE_COMPLETE.
    if not failed and replaced_resources:
        for logical_id, rtype, old_pid, old_props in replaced_resources:
            policy = _resource_policy(
                resources_defs.get(logical_id), "UpdateReplacePolicy",
                provisioned_resources, param_values, conditions, mappings,
                stack_name, stack_id)
            if policy in _RETAINING_POLICIES:
                # The predecessor leaves CloudFormation's scope and keeps
                # existing, as on AWS (a DELETE_SKIPPED event, no delete call).
                _add_event(stack_id, stack_name, logical_id, rtype,
                           "DELETE_SKIPPED", physical_id=old_pid)
                continue
            try:
                if _is_custom_resource(rtype):
                    await run_reentrant(
                        _delete_resource, rtype, old_pid, old_props,
                        stack_name, logical_id
                    )
                else:
                    _delete_resource(rtype, old_pid, old_props, stack_name, logical_id)
            except Exception as exc:
                logger.error("Failed to delete replaced resource %s (%s): %s",
                             logical_id, old_pid, exc)
                _add_event(stack_id, stack_name, logical_id, rtype,
                           "DELETE_FAILED", str(exc), old_pid)

    # Delete removed resources (update case)
    if not failed and to_remove:
        old_resources = previous_stack.get("_resources", {})
        old_template = previous_stack.get("_template", {}) or {}
        old_defs = old_template.get("Resources", {}) or {}
        for logical_id in to_remove:
            old_res = old_resources.get(logical_id, {})
            rtype = old_res.get("ResourceType", "")
            pid = old_res.get("PhysicalResourceId", "")
            old_props = old_res.get("Properties", {})
            policy = _resource_policy(
                old_defs.get(logical_id), "DeletionPolicy", old_resources,
                previous_stack.get("_resolved_params", {}),
                previous_stack.get("_conditions", conditions),
                old_template.get("Mappings", {}), stack_name, stack_id)
            if policy in _RETAINING_POLICIES:
                _add_event(stack_id, stack_name, logical_id, rtype,
                           "DELETE_SKIPPED", physical_id=pid)
                provisioned_resources.pop(logical_id, None)
                continue
            try:
                if _is_custom_resource(rtype):
                    await run_reentrant(
                        _delete_resource, rtype, pid, old_props,
                        stack_name, logical_id
                    )
                else:
                    _delete_resource(rtype, pid, old_props, stack_name, logical_id)
            except Exception as exc:
                # A cleanup miss doesn't fail the update — real CloudFormation
                # reports the resource DELETE_FAILED during the
                # UPDATE_COMPLETE_CLEANUP phase and still lands the stack in
                # UPDATE_COMPLETE — but it must be visible, not a warning.
                logger.error("Failed to delete old resource %s: %s",
                             logical_id, exc)
                _add_event(stack_id, stack_name, logical_id, rtype,
                           "DELETE_FAILED", str(exc), pid)
            provisioned_resources.pop(logical_id, None)

    await asyncio.sleep(0)

    resolved_outputs: list = []
    new_exports: dict = {}
    if not failed:
        try:
            resolved_outputs, new_exports = _resolve_stack_outputs(
                outputs_defs, conditions, provisioned_resources, param_values,
                mappings, stack_name, stack_id)
        except Exception as exc:
            # An output that cannot be resolved -- typically Fn::GetAtt to an
            # attribute the resource does not expose -- fails the operation
            # after every resource was created. Real CloudFormation rolls back
            # at exactly this point, with the resolution error as the reason.
            logger.error("Failed to resolve outputs of %s: %s", stack_name, exc)
            failed = True
            fail_reason = str(exc)
            _add_event(stack_id, stack_name, stack_name,
                       "AWS::CloudFormation::Stack", f"{status_prefix}_FAILED",
                       fail_reason, stack_id)

    if failed:
        if disable_rollback:
            stack["StackStatus"] = f"{status_prefix}_FAILED"
            stack["StackStatusReason"] = fail_reason
            _add_event(stack_id, stack_name, stack_name,
                       "AWS::CloudFormation::Stack", f"{status_prefix}_FAILED",
                       fail_reason, stack_id)
        else:
            # Rollback: delete resources created in this run in reverse order
            stack["StackStatus"] = "ROLLBACK_IN_PROGRESS" if not is_update else "UPDATE_ROLLBACK_IN_PROGRESS"
            _add_event(stack_id, stack_name, stack_name,
                       "AWS::CloudFormation::Stack", stack["StackStatus"],
                       "Rollback requested", stack_id)

            rollback_delete_failures = []
            previous_resources = (
                previous_stack.get("_resources", {})
                if is_update and previous_stack else {}
            )
            for logical_id in reversed(created_in_this_run):
                res = provisioned_resources.get(logical_id, {})
                rtype = res.get("ResourceType", "")
                pid = res.get("PhysicalResourceId", "")
                res_props = res.get("Properties", {})
                prev = previous_resources.get(logical_id)
                if prev is not None and prev.get("PhysicalResourceId") == pid:
                    # The resource existed before this update and kept its
                    # identity (untouched, or updated in place), so it is not
                    # something this run created: deleting it would destroy a
                    # resource the restored stack still records. Only new
                    # resources and replacements under a new physical id are
                    # undone. An in-place change is not reverted here.
                    continue
                policy = _resource_policy(
                    resources_defs.get(logical_id), "DeletionPolicy",
                    provisioned_resources, param_values, conditions, mappings,
                    stack_name, stack_id)
                if policy == "Retain" and not retain_except_on_create:
                    # ``Retain`` survives even the rollback of the operation
                    # that created it; ``RetainExceptOnCreate`` and the API
                    # parameter of that name are exactly the exception.
                    _add_event(stack_id, stack_name, logical_id, rtype,
                               "DELETE_SKIPPED", physical_id=pid)
                    provisioned_resources.pop(logical_id, None)
                    continue
                try:
                    if _is_custom_resource(rtype):
                        await run_reentrant(
                            _delete_resource, rtype, pid, res_props,
                            stack_name, logical_id
                        )
                    else:
                        _delete_resource(rtype, pid, res_props, stack_name, logical_id)
                    _add_event(stack_id, stack_name, logical_id, rtype,
                               "DELETE_COMPLETE", physical_id=pid)
                except Exception as del_exc:
                    logger.error("Rollback delete of %s failed: %s",
                                 logical_id, del_exc)
                    _add_event(stack_id, stack_name, logical_id, rtype,
                               "DELETE_FAILED", str(del_exc), pid)
                    rollback_delete_failures.append(logical_id)
                provisioned_resources.pop(logical_id, None)

            if is_update and previous_stack:
                # Restore previous resources
                stack["_resources"] = previous_stack.get("_resources", {})
                stack["_template"] = previous_stack.get("_template", {})
                stack["_resolved_params"] = previous_stack.get("_resolved_params", {})
                # What the API reports has to follow: GetTemplate serves
                # _template_body and DescribeStacks the Parameters and Tags,
                # and a rolled-back stack reports what it ran before the
                # update, not what failed.
                stack["_template_body"] = previous_stack.get(
                    "_template_body", stack.get("_template_body", ""))
                stack["Parameters"] = previous_stack.get(
                    "Parameters", stack.get("Parameters", []))
                stack["Tags"] = previous_stack.get("Tags", stack.get("Tags", []))
                stack["Outputs"] = previous_stack.get("Outputs", [])
                stack["StackStatus"] = "UPDATE_ROLLBACK_COMPLETE"
            else:
                stack["StackStatus"] = "ROLLBACK_COMPLETE"
            if rollback_delete_failures:
                # A rollback that could not undo what it created must not
                # report success — real CloudFormation lands the stack in
                # (UPDATE_)ROLLBACK_FAILED and keeps the failure visible.
                stack["StackStatus"] = (
                    "UPDATE_ROLLBACK_FAILED" if is_update and previous_stack
                    else "ROLLBACK_FAILED"
                )
                reason = ("The following resource(s) failed to delete: "
                          f"[{', '.join(sorted(rollback_delete_failures))}].")
                stack["StackStatusReason"] = reason
                _add_event(stack_id, stack_name, stack_name,
                           "AWS::CloudFormation::Stack", stack["StackStatus"],
                           reason, stack_id)
                return
            _add_event(stack_id, stack_name, stack_name,
                       "AWS::CloudFormation::Stack", stack["StackStatus"],
                       "Rollback complete", stack_id)
        return

    # Success: publish outputs and exports
    stack["_resources"] = provisioned_resources
    stack["_template"] = template
    stack["_resolved_params"] = param_values
    _exports.update(new_exports)
    stack["Outputs"] = resolved_outputs
    stack["StackStatus"] = f"{status_prefix}_COMPLETE"
    _add_event(stack_id, stack_name, stack_name,
               "AWS::CloudFormation::Stack", f"{status_prefix}_COMPLETE",
               physical_id=stack_id)


async def _delete_stack_async(stack_name: str, stack_id: str,
                              retain_resources=()):
    """Background task: delete all resources and mark stack DELETE_COMPLETE.

    A resource whose ``DeletionPolicy`` is ``Retain`` or
    ``RetainExceptOnCreate``, or whose logical id is in ``retain_resources``
    (the ``RetainResources`` parameter of a DeleteStack on a ``DELETE_FAILED``
    stack), is skipped with a ``DELETE_SKIPPED`` event and keeps existing."""
    from ministack.services.cloudformation import _exports, _stacks
    stack = _stacks.get(stack_name)
    if not stack:
        return

    stack["StackStatus"] = "DELETE_IN_PROGRESS"
    _add_event(stack_id, stack_name, stack_name,
               "AWS::CloudFormation::Stack", "DELETE_IN_PROGRESS",
               physical_id=stack_id)

    # Export-in-use check already done synchronously in _delete_stack

    resources = stack.get("_resources", {})
    template = stack.get("_template", {})
    res_defs = template.get("Resources", {}) if template else {}
    conditions = stack.get("_conditions", {})

    # Delete in reverse dependency order
    try:
        ordered = _topological_sort(res_defs, conditions) if res_defs else list(resources.keys())
    except ValueError:
        ordered = list(resources.keys())

    delete_failures = []
    for logical_id in reversed(ordered):
        res = resources.get(logical_id)
        if not res:
            continue
        rtype = res.get("ResourceType", "")
        pid = res.get("PhysicalResourceId", "")
        res_props = res.get("Properties", {})

        policy = _resource_policy(
            res_defs.get(logical_id), "DeletionPolicy", resources,
            stack.get("_resolved_params", {}), conditions,
            template.get("Mappings", {}) if template else {}, stack_name, stack_id)
        if policy in _RETAINING_POLICIES or logical_id in retain_resources:
            _add_event(stack_id, stack_name, logical_id, rtype,
                       "DELETE_SKIPPED", physical_id=pid)
            resources.pop(logical_id, None)
            continue

        _add_event(stack_id, stack_name, logical_id, rtype,
                   "DELETE_IN_PROGRESS", physical_id=pid)
        try:
            if _is_custom_resource(rtype):
                await run_reentrant(
                    _delete_resource, rtype, pid, res_props,
                    stack_name, logical_id
                )
            else:
                _delete_resource(rtype, pid, res_props, stack_name, logical_id)
            _add_event(stack_id, stack_name, logical_id, rtype,
                       "DELETE_COMPLETE", physical_id=pid)
            resources.pop(logical_id, None)
        except Exception as exc:
            logger.error("Delete of %s (%s) failed: %s", logical_id, pid, exc)
            _add_event(stack_id, stack_name, logical_id, rtype,
                       "DELETE_FAILED", str(exc), pid)
            res["ResourceStatus"] = "DELETE_FAILED"
            res["ResourceStatusReason"] = str(exc)
            delete_failures.append(logical_id)

    await asyncio.sleep(0)

    if delete_failures:
        # Real CloudFormation keeps deleting the other resources, then lands
        # the stack in DELETE_FAILED: the failed resources stay in the stack
        # (a retried DeleteStack picks them up again), everything that did
        # delete is gone, and the exports stay with the still-existing stack.
        reason = ("The following resource(s) failed to delete: "
                  f"[{', '.join(sorted(delete_failures))}].")
        stack["StackStatus"] = "DELETE_FAILED"
        stack["StackStatusReason"] = reason
        _add_event(stack_id, stack_name, stack_name,
                   "AWS::CloudFormation::Stack", "DELETE_FAILED",
                   reason, stack_id)
        return

    # Remove exports
    for out in stack.get("Outputs", []):
        export_name = out.get("ExportName")
        if export_name:
            _exports.pop(export_name, None)

    stack["StackStatus"] = "DELETE_COMPLETE"
    _add_event(stack_id, stack_name, stack_name,
               "AWS::CloudFormation::Stack", "DELETE_COMPLETE",
               physical_id=stack_id)


# ===========================================================================
# Change Set Helpers
# ===========================================================================

# The resource attributes a change set compares next to Properties; each is
# also the ResourceTargetDefinition Attribute value it reports. A change to one
# of them alone is a Modify with Replacement False (measured: an
# UpdateReplacePolicy edit lists `Modify / False / UpdateReplacePolicy`). A Type
# change is handled apart (a replacement). DependsOn and Condition are absent on
# purpose: a DependsOn-only edit did not show up as a change on AWS.
_DIFFED_ATTRIBUTES = (
    "Metadata",
    "CreationPolicy",
    "UpdatePolicy",
    "DeletionPolicy",
    "UpdateReplacePolicy",
)


def _diff_resources(old_template: dict, new_template: dict) -> list:
    """Diff two templates and return a list of change dicts.

    A resource is a ``Modify`` when its ``Properties`` differ or when one of the
    attributes in ``_DIFFED_ATTRIBUTES`` differs; each changed attribute becomes
    a ``Details`` entry (``Target.Attribute``, plus the property name for
    ``Properties``) and is listed in ``Scope``, as the API reference defines them.
    """
    old_res = old_template.get("Resources", {})
    new_res = new_template.get("Resources", {})
    changes = []

    all_keys = old_res.keys() | new_res.keys()
    for key in sorted(all_keys):
        if key not in old_res:
            changes.append({
                "ResourceChange": {
                    "Action": "Add",
                    "LogicalResourceId": key,
                    "ResourceType": new_res[key].get("Type", ""),
                    "Replacement": "False",
                }
            })
        elif key not in new_res:
            changes.append({
                "ResourceChange": {
                    "Action": "Remove",
                    "LogicalResourceId": key,
                    "ResourceType": old_res[key].get("Type", ""),
                    "PhysicalResourceId": "",
                    "Replacement": "False",
                }
            })
        else:
            details = []
            old_props = old_res[key].get("Properties", {}) or {}
            new_props = new_res[key].get("Properties", {}) or {}
            if old_props != new_props:
                for name in sorted(set(old_props) | set(new_props)):
                    if old_props.get(name) != new_props.get(name):
                        details.append({
                            "Target": {"Attribute": "Properties", "Name": name,
                                       "RequiresRecreation": "Conditionally"},
                            "Evaluation": "Static",
                            "ChangeSource": "DirectModification",
                        })
            for attr in _DIFFED_ATTRIBUTES:
                if old_res[key].get(attr) != new_res[key].get(attr):
                    details.append({
                        "Target": {"Attribute": attr},
                        "Evaluation": "Static",
                        "ChangeSource": "DirectModification",
                    })
            type_changed = old_res[key].get("Type") != new_res[key].get("Type")
            if not details and not type_changed:
                continue
            scope = []
            for d in details:
                if d["Target"]["Attribute"] not in scope:
                    scope.append(d["Target"]["Attribute"])
            if type_changed or old_props != new_props:
                replacement = "True" if type_changed else "Conditional"
            else:
                replacement = "False"
            changes.append({
                "ResourceChange": {
                    "Action": "Modify",
                    "LogicalResourceId": key,
                    "ResourceType": new_res[key].get("Type", ""),
                    "Replacement": replacement,
                    "Scope": scope,
                    "Details": details,
                }
            })
    return changes
