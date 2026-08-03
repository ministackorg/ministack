"""
CloudFormation stacks — async stack lifecycle (deploy, delete, update, diff).
"""

import asyncio
import contextvars
import copy
import json
import logging
import time
from contextlib import contextmanager

from ministack.core.concurrency import (
    LoopLocal,
    run_in_dedicated_thread_to_completion,
    run_in_thread_to_completion,
)
from ministack.core.responses import get_account_id, get_region, new_uuid, now_iso, set_request_region

from .engine import (
    _NO_VALUE,
    _evaluate_conditions,
    _parse_template,
    _resolve_parameters,
    _resolve_refs,
    _topological_sort,
)
from .provisioners import _delete_resource, _provision_resource, _update_resource

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


class _StackTaskLifecycle:
    """Track stack lifecycle tasks and close their admission during reset."""

    def __init__(self):
        self._accepting = True
        self._tasks = set()

    def create_task(self, coro):
        # Check and insertion are synchronous on the server loop: reset cannot
        # close admission between them.
        if not self._accepting:
            # The enclosing request may already have written stack metadata;
            # reset's subsequent state wipe removes it after task quiescence.
            coro.close()
            return None
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        # Keep this lifecycle alive for as long as one of its tasks exists;
        # LoopLocal intentionally stores only weak values for loop collection.
        task.add_done_callback(self._task_finished)
        return task

    def _task_finished(self, task):
        self._tasks.discard(task)

    async def begin_reset(self):
        """Close admission, cancel active tasks, and await full unwinding."""
        self._accepting = False
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if not tasks:
            return

        waiter = asyncio.ensure_future(asyncio.gather(*tasks, return_exceptions=True))
        try:
            await asyncio.shield(waiter)
        except asyncio.CancelledError as cancellation:
            # Stack tasks may be inside an uncancellable worker. Repeated reset
            # cancellation must not let the state wipe overtake that worker.
            while not waiter.done():
                try:
                    await asyncio.shield(waiter)
                except asyncio.CancelledError:
                    continue
            raise cancellation

    def finish_reset(self):
        self._accepting = True


_stack_task_lifecycles = LoopLocal(_StackTaskLifecycle)


def _get_stack_task_lifecycle() -> _StackTaskLifecycle:
    return _stack_task_lifecycles.get()


def _create_stack_task_in_region(coro, stack: dict | None, stack_id: str | None = None):
    """Schedule a tracked stack lifecycle coroutine in its owning region."""
    with _stack_region_context(stack, stack_id):
        return _get_stack_task_lifecycle().create_task(coro)


def _is_custom_resource(resource_type: str) -> bool:
    return resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource"


_STANDARD_PROVISIONER_SERVICES = {
    "AWS::ECS::Cluster": "ecs",
    "AWS::ECS::TaskDefinition": "ecs",
    "AWS::ECS::Service": "ecs",
    "AWS::RDS::DBCluster": "rds",
    "AWS::RDS::DBInstance": "rds",
    "AWS::EKS::Cluster": "eks",
    "AWS::EKS::Nodegroup": "eks",
    "AWS::OpenSearchService::Domain": "opensearch",
}

_NESTED_STACK_RESOURCE_TYPE = "AWS::CloudFormation::Stack"
_nested_stack_server_loop = contextvars.ContextVar(
    "cloudformation_nested_stack_server_loop",
    default=None,
)


def _standard_provisioner_admission_lock(resource_type: str):
    service_name = _STANDARD_PROVISIONER_SERVICES[resource_type]
    if service_name == "ecs":
        from ministack.services import ecs as service
    elif service_name == "rds":
        from ministack.services import rds as service
    elif service_name == "eks":
        from ministack.services import eks as service
    else:
        from ministack.services import opensearch as service
    return service._get_request_dispatch_lock()


async def _run_locked_standard_provisioner(resource_type, operation, *args):
    """Run a Docker-backed standard provisioner under its service admission lock."""
    async with _standard_provisioner_admission_lock(resource_type):
        return await run_in_thread_to_completion(operation, *args)


def _run_locked_standard_provisioner_sync(resource_type, operation, *args):
    """Marshal one nested-stack child onto the server admission seam."""
    server_loop = _nested_stack_server_loop.get()
    if server_loop is None:
        raise RuntimeError("nested CloudFormation provisioner has no server loop")
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None
    if running_loop is server_loop:
        raise RuntimeError("nested CloudFormation provisioner cannot marshal from the server loop")
    if server_loop.is_closed():
        raise RuntimeError("nested CloudFormation server loop is closed")

    coro = _run_locked_standard_provisioner(resource_type, operation, *args)
    try:
        future = asyncio.run_coroutine_threadsafe(coro, server_loop)
    except BaseException:
        coro.close()
        raise
    return future.result()


async def _run_nested_stack_provisioner(operation, *args):
    """Run the top-level nested core off-loop; deeper recursion stays inline."""
    server_loop = asyncio.get_running_loop()

    def install_server_loop():
        _nested_stack_server_loop.set(server_loop)

    return await run_in_dedicated_thread_to_completion(
        operation,
        *args,
        thread_name="ministack-cfn-nested-stack",
        context_setup=install_server_loop,
    )


# ===========================================================================
# Stack Events helper
# ===========================================================================

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

async def _deploy_stack_async(stack_name: str, stack_id: str, template: dict,
                              param_values: dict, disable_rollback: bool,
                              tags: list, is_update: bool = False,
                              previous_stack: dict | None = None):
    """Background task: provision resources and set final stack status."""
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
                if _is_custom_resource(resource_type):
                    physical_id, attrs = await run_in_thread_to_completion(
                        _update_resource, resource_type, old_pid, old_props,
                        resolved_props, stack_name, logical_id
                    )
                elif resource_type == _NESTED_STACK_RESOURCE_TYPE:
                    physical_id, attrs = await _run_nested_stack_provisioner(
                        _update_resource, resource_type, old_pid, old_props,
                        resolved_props, stack_name, logical_id
                    )
                elif resource_type in _STANDARD_PROVISIONER_SERVICES:
                    physical_id, attrs = await _run_locked_standard_provisioner(
                        resource_type, _update_resource, resource_type, old_pid,
                        old_props, resolved_props, stack_name, logical_id
                    )
                else:
                    physical_id, attrs = _update_resource(
                        resource_type, old_pid, old_props, resolved_props,
                        stack_name, logical_id
                    )
            else:
                if _is_custom_resource(resource_type):
                    physical_id, attrs = await run_in_thread_to_completion(
                        _provision_resource, resource_type, logical_id, resolved_props, stack_name
                    )
                elif resource_type == _NESTED_STACK_RESOURCE_TYPE:
                    physical_id, attrs = await _run_nested_stack_provisioner(
                        _provision_resource, resource_type, logical_id,
                        resolved_props, stack_name
                    )
                elif resource_type in _STANDARD_PROVISIONER_SERVICES:
                    physical_id, attrs = await _run_locked_standard_provisioner(
                        resource_type, _provision_resource, resource_type,
                        logical_id, resolved_props, stack_name
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

    # Delete removed resources (update case)
    if not failed and to_remove:
        old_resources = previous_stack.get("_resources", {})
        for logical_id in to_remove:
            old_res = old_resources.get(logical_id, {})
            rtype = old_res.get("ResourceType", "")
            pid = old_res.get("PhysicalResourceId", "")
            old_props = old_res.get("Properties", {})
            try:
                if _is_custom_resource(rtype):
                    await run_in_thread_to_completion(
                        _delete_resource, rtype, pid, old_props,
                        stack_name, logical_id
                    )
                elif rtype == _NESTED_STACK_RESOURCE_TYPE:
                    await _run_nested_stack_provisioner(
                        _delete_resource, rtype, pid, old_props,
                        stack_name, logical_id
                    )
                elif rtype in _STANDARD_PROVISIONER_SERVICES:
                    await _run_locked_standard_provisioner(
                        rtype, _delete_resource, rtype, pid, old_props,
                        stack_name, logical_id
                    )
                else:
                    _delete_resource(rtype, pid, old_props, stack_name, logical_id)
            except Exception as exc:
                logger.warning("Failed to delete old resource %s: %s",
                               logical_id, exc)
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
            _add_event(stack_id, stack_name, stack_name,
                       "AWS::CloudFormation::Stack", stack["StackStatus"],
                       "Rollback requested", stack_id)

            for logical_id in reversed(created_in_this_run):
                res = provisioned_resources.get(logical_id, {})
                rtype = res.get("ResourceType", "")
                pid = res.get("PhysicalResourceId", "")
                res_props = res.get("Properties", {})
                try:
                    if _is_custom_resource(rtype):
                        await run_in_thread_to_completion(
                            _delete_resource, rtype, pid, res_props,
                            stack_name, logical_id
                        )
                    elif rtype == _NESTED_STACK_RESOURCE_TYPE:
                        await _run_nested_stack_provisioner(
                            _delete_resource, rtype, pid, res_props,
                            stack_name, logical_id
                        )
                    elif rtype in _STANDARD_PROVISIONER_SERVICES:
                        await _run_locked_standard_provisioner(
                            rtype, _delete_resource, rtype, pid, res_props,
                            stack_name, logical_id
                        )
                    else:
                        _delete_resource(rtype, pid, res_props, stack_name, logical_id)
                    _add_event(stack_id, stack_name, logical_id, rtype,
                               "DELETE_COMPLETE", physical_id=pid)
                except Exception as del_exc:
                    logger.warning("Rollback delete of %s failed: %s",
                                   logical_id, del_exc)
                    _add_event(stack_id, stack_name, logical_id, rtype,
                               "DELETE_FAILED", str(del_exc), pid)
                provisioned_resources.pop(logical_id, None)

            if is_update and previous_stack:
                # Restore previous resources
                stack["_resources"] = previous_stack.get("_resources", {})
                stack["_template"] = previous_stack.get("_template", {})
                stack["_resolved_params"] = previous_stack.get("_resolved_params", {})
                stack["Outputs"] = previous_stack.get("Outputs", [])
                stack["StackStatus"] = "UPDATE_ROLLBACK_COMPLETE"
            else:
                stack["StackStatus"] = "ROLLBACK_COMPLETE"
            _add_event(stack_id, stack_name, stack_name,
                       "AWS::CloudFormation::Stack", stack["StackStatus"],
                       "Rollback complete", stack_id)
        return

    # Success: resolve outputs
    stack["_resources"] = provisioned_resources
    stack["_template"] = template
    stack["_resolved_params"] = param_values

    resolved_outputs = []
    for out_name, out_def in outputs_defs.items():
        cond = out_def.get("Condition")
        if cond and not conditions.get(cond, True):
            continue
        out_value = _resolve_refs(
            copy.deepcopy(out_def.get("Value", "")),
            provisioned_resources, param_values, conditions,
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
                provisioned_resources, param_values, conditions,
                mappings, stack_name, stack_id
            )
            output["ExportName"] = str(export_name)
            _exports[str(export_name)] = {
                "StackId": stack_id,
                "Name": str(export_name),
                "Value": str(out_value),
            }
        resolved_outputs.append(output)

    stack["Outputs"] = resolved_outputs
    stack["StackStatus"] = f"{status_prefix}_COMPLETE"
    _add_event(stack_id, stack_name, stack_name,
               "AWS::CloudFormation::Stack", f"{status_prefix}_COMPLETE",
               physical_id=stack_id)


async def _delete_stack_async(stack_name: str, stack_id: str):
    """Background task: delete all resources and mark stack DELETE_COMPLETE."""
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

    for logical_id in reversed(ordered):
        res = resources.get(logical_id)
        if not res:
            continue
        rtype = res.get("ResourceType", "")
        pid = res.get("PhysicalResourceId", "")
        res_props = res.get("Properties", {})

        _add_event(stack_id, stack_name, logical_id, rtype,
                   "DELETE_IN_PROGRESS", physical_id=pid)
        try:
            if _is_custom_resource(rtype):
                await run_in_thread_to_completion(
                    _delete_resource, rtype, pid, res_props,
                    stack_name, logical_id
                )
            elif rtype == _NESTED_STACK_RESOURCE_TYPE:
                await _run_nested_stack_provisioner(
                    _delete_resource, rtype, pid, res_props,
                    stack_name, logical_id
                )
            elif rtype in _STANDARD_PROVISIONER_SERVICES:
                await _run_locked_standard_provisioner(
                    rtype, _delete_resource, rtype, pid, res_props,
                    stack_name, logical_id
                )
            else:
                _delete_resource(rtype, pid, res_props, stack_name, logical_id)
            _add_event(stack_id, stack_name, logical_id, rtype,
                       "DELETE_COMPLETE", physical_id=pid)
        except Exception as exc:
            logger.warning("Delete of %s (%s) failed: %s", logical_id, pid, exc)
            _add_event(stack_id, stack_name, logical_id, rtype,
                       "DELETE_FAILED", str(exc), pid)

    # Remove exports
    for out in stack.get("Outputs", []):
        export_name = out.get("ExportName")
        if export_name:
            _exports.pop(export_name, None)

    await asyncio.sleep(0)

    stack["StackStatus"] = "DELETE_COMPLETE"
    _add_event(stack_id, stack_name, stack_name,
               "AWS::CloudFormation::Stack", "DELETE_COMPLETE",
               physical_id=stack_id)


# ===========================================================================
# Change Set Helpers
# ===========================================================================

def _diff_resources(old_template: dict, new_template: dict) -> list:
    """Diff two templates and return a list of change dicts."""
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
            old_props = old_res[key].get("Properties", {})
            new_props = new_res[key].get("Properties", {})
            if old_props != new_props:
                changes.append({
                    "ResourceChange": {
                        "Action": "Modify",
                        "LogicalResourceId": key,
                        "ResourceType": new_res[key].get("Type", ""),
                        "Replacement": "Conditional",
                    }
                })
    return changes
