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
    _resolve_dynamic_references,
    _resolve_refs,
    _topological_sort,
)
from .provisioners import (
    _DEFERRED_PREDECESSOR_DELETES,
    _RETAIN_REPLACED,
    _RETAINING_POLICIES,
    _custom_named_replacement_error,
    _delete_resource,
    _property_recreation,
    _provision_resource,
    _snapshot_resource,
    _update_resource,
    _with_stack_tags,
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
    """The types whose provisioning blocks on a callback into this server, so
    they run on a worker thread: custom resources wait for the ResponseURL
    PUT, a WaitCondition for the signals on its handle, and a nested stack
    deploys inline and may contain either."""
    return (resource_type.startswith("Custom::")
            or resource_type in ("AWS::CloudFormation::CustomResource",
                                 "AWS::CloudFormation::WaitCondition",
                                 "AWS::CloudFormation::Stack"))


# ===========================================================================
# Stack Events helper
# ===========================================================================


def _default_resource_policy(res_def, attribute) -> str:
    """``Delete``, except where AWS documents ``Snapshot`` as the default."""
    if attribute != "DeletionPolicy":
        return "Delete"
    rtype = (res_def or {}).get("Type")
    props = (res_def or {}).get("Properties") or {}
    if rtype == "AWS::RDS::DBCluster":
        return "Snapshot"
    if rtype == "AWS::RDS::DBInstance" and not props.get("DBClusterIdentifier"):
        return "Snapshot"
    return "Delete"


def _resource_policy(res_def, attribute, resources, params, conditions, mappings,
                     stack_name, stack_id):
    """A resource's ``DeletionPolicy`` / ``UpdateReplacePolicy`` as a string,
    ``Delete`` when the template sets none, with one documented exception: AWS
    defaults ``DeletionPolicy`` to ``Snapshot`` for ``AWS::RDS::DBCluster`` and
    for ``AWS::RDS::DBInstance`` without ``DBClusterIdentifier``
    (aws-attribute-deletionpolicy.html; captured eu-north-1 2026-09-19, the
    stack delete left a manual snapshot behind).

    An intrinsic (``Fn::If``) is resolved like a property; one that does not
    resolve counts as ``Delete`` and is logged, since that is the destructive
    reading."""
    value = (res_def or {}).get(attribute)
    if value is None:
        return _default_resource_policy(res_def, attribute)
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
        if isinstance(out_value, (bool, list, dict)):
            raise ValueError("Template format error: The Value field of every "
                             "Outputs member must evaluate to a String.")
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


def _rollback_failure_reason(failed_records: dict) -> str:
    """The reason of a rollback that could not finish: failed reverts, then failed deletes."""
    reverts = sorted(k for k, v in failed_records.items() if "RevertTo" in v)
    deletes = sorted(k for k, v in failed_records.items() if "RevertTo" not in v)
    parts = []
    if reverts:
        parts.append(f"The following resource(s) failed to update: [{', '.join(reverts)}].")
    if deletes:
        parts.append(f"The following resource(s) failed to delete: [{', '.join(deletes)}].")
    return " ".join(parts)


def _sends_back_failed_update(resource_type: str) -> bool:
    """Whether the resource whose own update failed is sent back too: a provider
    or a child stack may have changed state before failing; a type's handler did not."""
    return (resource_type.startswith("Custom::")
            or resource_type in ("AWS::CloudFormation::CustomResource",
                                 "AWS::CloudFormation::Stack"))


def _mark_rolled_back(record):
    """The restored record of a resource the rollback updated reports UPDATE_COMPLETE."""
    if record is not None:
        record["ResourceStatus"] = "UPDATE_COMPLETE"
        record["Timestamp"] = now_iso()


async def _revert_update(stack_id, stack_name, logical_id, rtype, physical_id,
                         applied_props, previous_props, attrs, record=None):
    """Send an in-place update back to the previous properties through the
    type's update handler; ``record`` takes the attributes it answers."""
    _add_event(stack_id, stack_name, logical_id, rtype, "UPDATE_IN_PROGRESS",
               physical_id=physical_id)
    try:
        if _is_custom_resource(rtype):
            new_pid, new_attrs = await run_reentrant(
                _update_resource, rtype, physical_id, applied_props,
                previous_props, stack_name, logical_id, attrs)
        else:
            new_pid, new_attrs = _update_resource(
                rtype, physical_id, applied_props, previous_props,
                stack_name, logical_id, attrs)
    except Exception as exc:
        logger.error("Rollback update of %s failed: %s", logical_id, exc)
        _add_event(stack_id, stack_name, logical_id, rtype, "UPDATE_FAILED",
                   str(exc), physical_id)
        raise
    if record is not None:
        if new_pid != physical_id:
            logger.warning("Rollback update of %s moved it from %s to %s",
                           logical_id, physical_id, new_pid)
            record["PhysicalResourceId"] = new_pid
        if new_attrs:
            record["Attributes"] = {**record.get("Attributes", {}), **new_attrs}
    _mark_rolled_back(record)
    _add_event(stack_id, stack_name, logical_id, rtype, "UPDATE_COMPLETE",
               physical_id=new_pid)


async def _continue_update_rollback_async(stack_name: str, stack_id: str,
                                          resources_to_skip):
    """ContinueUpdateRollback: retry the failed reverts and deletes; a skipped
    resource is set to UPDATE_COMPLETE and left as it is."""
    from ministack.services.cloudformation import _stacks
    stack = _stacks.get(stack_name)
    if not stack:
        return
    pending = dict(stack.get("_rollback_failed", {}))
    stack["StackStatus"] = "UPDATE_ROLLBACK_IN_PROGRESS"
    _add_event(stack_id, stack_name, stack_name, "AWS::CloudFormation::Stack",
               "UPDATE_ROLLBACK_IN_PROGRESS", "User Initiated", stack_id)
    still_failed = {}
    for logical_id, res in pending.items():
        rtype = res.get("ResourceType", "")
        pid = res.get("PhysicalResourceId", "")
        if logical_id in resources_to_skip:
            _add_event(stack_id, stack_name, logical_id, rtype,
                       "UPDATE_COMPLETE", "Resource rollback skipped by user", pid)
            continue
        if "RevertTo" in res:
            try:
                await _revert_update(
                    stack_id, stack_name, logical_id, rtype, pid,
                    res["RevertFrom"], res["RevertTo"], res.get("Attributes"),
                    stack.get("_resources", {}).get(logical_id))
            except Exception:
                still_failed[logical_id] = res
            continue
        try:
            if _is_custom_resource(rtype):
                await run_reentrant(_delete_resource, rtype, pid,
                                    res.get("Properties", {}), stack_name, logical_id)
            else:
                _delete_resource(rtype, pid, res.get("Properties", {}),
                                 stack_name, logical_id)
            _add_event(stack_id, stack_name, logical_id, rtype,
                       "DELETE_COMPLETE", physical_id=pid)
        except Exception as exc:
            logger.error("Continue rollback delete of %s failed: %s", logical_id, exc)
            _add_event(stack_id, stack_name, logical_id, rtype,
                       "DELETE_FAILED", str(exc), pid)
            still_failed[logical_id] = res
    await asyncio.sleep(0)
    if still_failed:
        stack["_rollback_failed"] = still_failed
        reason = _rollback_failure_reason(still_failed)
        stack["StackStatus"] = "UPDATE_ROLLBACK_FAILED"
        stack["StackStatusReason"] = reason
        _add_event(stack_id, stack_name, stack_name, "AWS::CloudFormation::Stack",
                   "UPDATE_ROLLBACK_FAILED", reason, stack_id)
        return
    stack.pop("_rollback_failed", None)
    stack["StackStatus"] = "UPDATE_ROLLBACK_COMPLETE"
    stack["StackStatusReason"] = ""
    _add_event(stack_id, stack_name, stack_name, "AWS::CloudFormation::Stack",
               "UPDATE_ROLLBACK_COMPLETE", physical_id=stack_id)


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
    # Stack-level tags reach the resources through their own tag property;
    # the stack record keeps the template's properties (below), so the
    # change-set diff and the next update compare templates, not tags.
    stack_tags = stack.get("Tags") or []
    previous_tags = (previous_stack or {}).get("Tags") or []

    # If update: figure out what to add/modify/remove
    if is_update and previous_stack:
        old_resource_names = set(previous_stack.get("_resources", {}).keys())
        new_resource_names = set(ordered)
        to_remove = old_resource_names - new_resource_names
    else:
        to_remove = set()

    failed = False
    fail_reason = ""
    cancelled = False
    replaced_resources = []
    # (logical id, type, pid, applied, previous, attrs) of a pre-existing resource that failed.
    failed_update = None
    failed_logical_id = None
    stack.pop("_cancel_requested", None)

    for logical_id in ordered:
        if is_update and stack.pop("_cancel_requested", False):
            # CancelUpdateStack: stop before the next resource and roll the
            # update back, as on AWS ("User Initiated").
            failed = cancelled = True
            fail_reason = "User Initiated"
            break
        res_def = resources_defs[logical_id]
        cond = res_def.get("Condition")
        if cond and not conditions.get(cond, True):
            continue

        resource_type = res_def.get("Type", "AWS::CloudFormation::CustomResource")
        raw_props = res_def.get("Properties", {})
        update_attempt = None

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
            # Dynamic references, after the intrinsics: an `ssm` reference
            # re-resolves on an update that changed the template or its
            # parameters (an identical update keeps what the stack has), a
            # `secretsmanager` reference only when this resource's definition
            # changed. The per-resource map of resolved values is what the
            # next update reuses. (measured on AWS 2026-09-02)
            prev_for_dynamic = (
                previous_stack.get("_resources", {}).get(logical_id)
                if is_update and previous_stack else None
            )
            stack_unchanged = bool(
                is_update and previous_stack
                and template == previous_stack.get("_template")
                and param_values == previous_stack.get("_resolved_params"))
            resource_unchanged = bool(
                prev_for_dynamic is not None and previous_stack
                and (previous_stack.get("_template", {}).get("Resources", {})
                     .get(logical_id) == res_def))
            resolved_props, dynamic_values = _resolve_dynamic_references(
                resolved_props,
                (prev_for_dynamic or {}).get("_dynamic"),
                reuse_ssm=stack_unchanged,
                reuse_secrets=resource_unchanged,
            )

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
                old_tagged = _with_stack_tags(
                    resource_type, old_props, previous_tags,
                    stack_name, stack_id, logical_id)
                new_tagged = _with_stack_tags(
                    resource_type, resolved_props, stack_tags,
                    stack_name, stack_id, logical_id)
                pending_deletes = []
                deferred_token = _DEFERRED_PREDECESSOR_DELETES.set(pending_deletes)
                # A custom-named refusal never reaches the handler: nothing to send back.
                if not _custom_named_replacement_error(
                        resource_type, old_tagged, new_tagged):
                    update_attempt = (old_pid, new_tagged, old_tagged, old_attrs)
                try:
                    if _is_custom_resource(resource_type):
                        physical_id, attrs = await run_reentrant(
                            _update_resource, resource_type, old_pid, old_tagged,
                            new_tagged, stack_name, logical_id, old_attrs
                        )
                    else:
                        physical_id, attrs = _update_resource(
                            resource_type, old_pid, old_tagged, new_tagged,
                            stack_name, logical_id, old_attrs
                        )
                finally:
                    _DEFERRED_PREDECESSOR_DELETES.reset(deferred_token)
                    _RETAIN_REPLACED.reset(token)
                if physical_id != old_pid or pending_deletes:
                    # A changed physical id, or a predecessor delete the
                    # handler queued, is a replacement. Real CloudFormation
                    # deletes the predecessor in the UPDATE_COMPLETE_CLEANUP
                    # phase and keeps it when the update rolls back.
                    replaced_resources.append(
                        (logical_id, resource_type, old_pid, physical_id,
                         old_props, pending_deletes))
            else:
                new_tagged = _with_stack_tags(
                    resource_type, resolved_props, stack_tags,
                    stack_name, stack_id, logical_id)
                if _is_custom_resource(resource_type):
                    physical_id, attrs = await run_reentrant(
                        _provision_resource, resource_type, logical_id, new_tagged, stack_name
                    )
                else:
                    physical_id, attrs = _provision_resource(
                        resource_type, logical_id, new_tagged, stack_name
                    )
        except Exception as exc:
            logger.error("Failed to provision %s (%s): %s",
                         logical_id, resource_type, exc)
            _add_event(stack_id, stack_name, logical_id, resource_type,
                       f"{status_prefix}_FAILED", str(exc))
            failed = True
            fail_reason = f"Resource {logical_id} failed: {exc}"
            failed_logical_id = logical_id
            if update_attempt is not None:
                failed_update = (logical_id, resource_type, *update_attempt)
            elif is_update and previous_stack and logical_id in previous_stack.get(
                    "_resources", {}):
                failed_update = (
                    logical_id, resource_type,
                    previous_stack["_resources"][logical_id].get(
                        "PhysicalResourceId", logical_id),
                    None, None, None)
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
        if dynamic_values:
            provisioned_resources[logical_id]["_dynamic"] = dynamic_values
        created_in_this_run.append(logical_id)

        _add_event(stack_id, stack_name, logical_id, resource_type,
                   f"{status_prefix}_COMPLETE", physical_id=physical_id)

    resolved_outputs: list = []
    new_exports: dict = {}
    if not failed:
        try:
            # A resource the update removes is still recorded here (its delete
            # runs below); an output that reads it must fail, as on AWS.
            surviving = {k: v for k, v in provisioned_resources.items() if k not in to_remove}
            resolved_outputs, new_exports = _resolve_stack_outputs(
                outputs_defs, conditions, surviving, param_values,
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

    # Replacement cleanup (update case): delete each replaced resource's
    # predecessor, as real CloudFormation does after UPDATE_COMPLETE, in
    # reverse order so a dependent goes before what it depends on. Nothing
    # is deleted before this point, so a rollback keeps the old resources.
    if not failed and replaced_resources:
        for (logical_id, rtype, old_pid, new_pid, old_props,
             pending_deletes) in reversed(replaced_resources):
            policy = _resource_policy(
                resources_defs.get(logical_id), "UpdateReplacePolicy",
                provisioned_resources, param_values, conditions, mappings,
                stack_name, stack_id)
            if policy in _RETAINING_POLICIES:
                # The predecessor leaves CloudFormation's scope and keeps
                # existing, as on AWS (a DELETE_SKIPPED event, no delete call).
                if new_pid != old_pid:
                    _add_event(stack_id, stack_name, logical_id, rtype,
                               "DELETE_SKIPPED", physical_id=old_pid)
                continue
            try:
                # The handler's own delete first: it knows the old record's
                # key where the physical id does not carry it.
                for delete_fn, args, kwargs in pending_deletes:
                    delete_fn(*args, **kwargs)
                if new_pid == old_pid:
                    continue
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
        # Dependents first, as the stack delete orders it.
        try:
            removal_order = [lid for lid in _topological_sort(
                old_defs, previous_stack.get("_conditions", conditions)) if lid in to_remove]
        except ValueError:
            removal_order = []
        removal_order += [lid for lid in to_remove if lid not in removal_order]
        for logical_id in reversed(removal_order):
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
                if policy == "Snapshot":
                    _snapshot_resource(rtype, pid, old_props)
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
                # UPDATE_COMPLETE. The resource stays in the stack with that
                # status (it still exists in the service), so the next update
                # or the stack delete tries the delete again.
                logger.error("Failed to delete old resource %s: %s",
                             logical_id, exc)
                _add_event(stack_id, stack_name, logical_id, rtype,
                           "DELETE_FAILED", str(exc), pid)
                leftover = provisioned_resources.get(logical_id)
                if leftover is not None:
                    leftover["ResourceStatus"] = "DELETE_FAILED"
                    leftover["ResourceStatusReason"] = str(exc)
                    leftover["Timestamp"] = now_iso()
                continue
            provisioned_resources.pop(logical_id, None)

    await asyncio.sleep(0)

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
            if cancelled:
                rollback_reason = "User Initiated"
            elif is_update and failed_logical_id:
                verb = "update" if failed_update is not None else "create"
                rollback_reason = (f"The following resource(s) failed to {verb}: "
                                   f"[{failed_logical_id}]. ")
            else:
                rollback_reason = "Rollback requested"
            _add_event(stack_id, stack_name, stack_name,
                       "AWS::CloudFormation::Stack", stack["StackStatus"],
                       rollback_reason, stack_id)

            rollback_failed_records = {}
            previous_resources = (
                previous_stack.get("_resources", {})
                if is_update and previous_stack else {}
            )
            replaced_ids = {entry[0] for entry in replaced_resources}

            async def revert(logical_id, rtype, pid, applied, previous, attrs):
                # A failed revert is kept for ContinueUpdateRollback.
                try:
                    await _revert_update(stack_id, stack_name, logical_id, rtype,
                                         pid, applied, previous, attrs,
                                         previous_resources.get(logical_id))
                except Exception:
                    rollback_failed_records[logical_id] = {
                        "PhysicalResourceId": pid, "ResourceType": rtype,
                        "RevertFrom": applied, "RevertTo": previous,
                        "Attributes": attrs,
                    }

            # In-place changes go back first; what the update created is deleted in cleanup.
            to_delete = []
            for logical_id in created_in_this_run:
                res = provisioned_resources.get(logical_id, {})
                pid = res.get("PhysicalResourceId", "")
                prev = previous_resources.get(logical_id)
                if (prev is None or prev.get("PhysicalResourceId") != pid
                        or logical_id in replaced_ids):
                    to_delete.append(logical_id)
                    continue
                # Pre-existing and same identity: sent back with the tags each side was called with.
                rtype = res.get("ResourceType", "")
                applied = _with_stack_tags(rtype, res.get("Properties", {}), stack_tags,
                                           stack_name, stack_id, logical_id)
                previous = _with_stack_tags(rtype, prev.get("Properties", {}),
                                            previous_tags, stack_name, stack_id,
                                            logical_id)
                if applied != previous:
                    await revert(logical_id, rtype, pid, applied, previous,
                                 res.get("Attributes", {}))
            if failed_update is not None:
                logical_id, rtype, pid, applied, previous, attrs = failed_update
                if (applied is not None and applied != previous
                        and _sends_back_failed_update(rtype)):
                    await revert(logical_id, rtype, pid, applied, previous, attrs)
                else:
                    _add_event(stack_id, stack_name, logical_id, rtype,
                               "UPDATE_COMPLETE", physical_id=pid)
                    _mark_rolled_back(previous_resources.get(logical_id))
            # "rolled back to its previous working state ... still deleting any new
            # resources" (view-stack-events): a cleanup delete failure does not fail it.
            cleanup_phase = bool(is_update and previous_stack
                                 and not rollback_failed_records)
            if cleanup_phase:
                stack["StackStatus"] = "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS"
                _add_event(stack_id, stack_name, stack_name,
                           "AWS::CloudFormation::Stack", stack["StackStatus"],
                           physical_id=stack_id)
            for logical_id in reversed(to_delete):
                res = provisioned_resources.get(logical_id, {})
                rtype = res.get("ResourceType", "")
                pid = res.get("PhysicalResourceId", "")
                res_props = res.get("Properties", {})
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
                except Exception as del_exc:
                    logger.error("Rollback delete of %s failed: %s",
                                 logical_id, del_exc)
                    _add_event(stack_id, stack_name, logical_id, rtype,
                               "DELETE_FAILED", str(del_exc), pid)
                    if cleanup_phase:
                        provisioned_resources.pop(logical_id, None)
                        continue
                    rollback_failed_records[logical_id] = {
                        "PhysicalResourceId": pid, "ResourceType": rtype,
                        "Properties": res_props,
                    }
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
            if rollback_failed_records:
                # A rollback that could not undo what it created or changed
                # must not report success — real CloudFormation lands the stack
                # in (UPDATE_)ROLLBACK_FAILED and keeps the failure visible.
                stack["StackStatus"] = (
                    "UPDATE_ROLLBACK_FAILED" if is_update and previous_stack
                    else "ROLLBACK_FAILED"
                )
                reason = _rollback_failure_reason(rollback_failed_records)
                stack["StackStatusReason"] = reason
                # What ContinueUpdateRollback retries (or skips).
                stack["_rollback_failed"] = rollback_failed_records
                _add_event(stack_id, stack_name, stack_name,
                           "AWS::CloudFormation::Stack", stack["StackStatus"],
                           reason, stack_id)
                return
            _add_event(stack_id, stack_name, stack_name,
                       "AWS::CloudFormation::Stack", stack["StackStatus"],
                       "" if is_update and previous_stack else "Rollback complete",
                       stack_id)
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
    # A resource the template no longer declares but the stack still holds
    # (its cleanup delete failed on an earlier update) has no dependents left;
    # it goes first, so a retried delete reaches it.
    ordered += [lid for lid in resources if lid not in ordered]

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
            if policy == "Snapshot":
                _snapshot_resource(rtype, pid, res_props)
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
            rtype = new_res[key].get("Type", "")
            if old_props != new_props:
                for name in sorted(set(old_props) | set(new_props)):
                    if old_props.get(name) != new_props.get(name):
                        details.append({
                            "Target": {"Attribute": "Properties", "Name": name,
                                       "RequiresRecreation":
                                           _property_recreation(rtype, name)},
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
            recreation = {d["Target"].get("RequiresRecreation") for d in details}
            if type_changed or "Always" in recreation:
                replacement = "True"
            elif "Conditionally" in recreation:
                replacement = "Conditional"
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
