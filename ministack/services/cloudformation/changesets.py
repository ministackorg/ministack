"""
CloudFormation change set handlers — Create, Describe, Execute, Delete, List change sets.
"""

import asyncio
import copy
import json
import logging

from data.iam import get_full_policy_arn
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
        _add_event(
            stack_id, stack_name, stack_name, "AWS::CloudFormation::Stack", "CREATE_IN_PROGRESS", physical_id=stack_id
        )
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

    # Create transitive resources automatically (use-case: SAM Function) .
    try:
        transitive_resources = _generate_transitive_resources(cs_name, template)
        template.get("Resources").update(transitive_resources)
        _manage_interdependent_resources(cs_name, template)
        _handle_globals(cs_name, template)
    except (ValueError, TypeError) as e:
        return _error(e.args[0], e.args[1])

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

#TODO: Ask Maintainers if we shall move this to a new file ?

# ===========================================================================
# Globals inheritance
# ===========================================================================
def _merge_property(global_val, resource_val, res_name, cs_name):
    """Merge a global property with a resource property, giving priority to the resource.
    Assumption: both global_val and resource_val are of the same type, or one of them is None. Otherwise, the behavior is undefined."""
    if resource_val is None:
        return global_val
    if global_val is None:
        return resource_val
    if isinstance(global_val, (int, float, str, bool)):
        return resource_val
    if isinstance(global_val, dict):
        keys = global_val.keys() | resource_val.keys()
        return {k: _merge_property(global_val.get(k), resource_val.get(k), res_name, cs_name) for k in keys}
    if isinstance(global_val, list):
        return list(set(global_val + resource_val))
    logger.info("Unsupported complex type when applying globals for resource %s in change-set %s", res_name, cs_name)
    raise TypeError("UnsupportedGlobalsPropertyType",
                    f"Unsupported complex type when applying globals for resource {res_name} in change-set {cs_name}")


def _handle_globals(cs_name: str, template: dict):
    """
    Handle Globals section for SAM Resources
    """
    template_globals = template.get("Globals")
    if not template_globals:
        return

    for res_name, res_def in template.get("Resources", {}).items():
        ignored_globals = res_def.get("IgnoreGlobals", [])
        if ignored_globals == "*":
            continue

        parts = res_def.get("Type", "AWS::CloudFormation::CustomResource").split("::")
        if len(parts) != 3 or parts[0] != "AWS" or parts[1] != "Serverless" or parts[2] not in template_globals:
            continue

        res_properties = res_def.setdefault("Properties", {})
        for prop_name, prop_value in template_globals[parts[2]].items():
            if prop_name in ignored_globals:
                continue
            merged = _merge_property(prop_value, res_properties.get(prop_name), res_name, cs_name)
            if merged is not None:
                res_properties[prop_name] = merged
# ===========================================================================
# Transitive resources
# ===========================================================================
def _generate_transitive_resources(cs_name: str, template: dict):
    """Generate transitive resources automatically if needed"""
    new_resources = {}
    existing_resource_names = list(template.get("Resources", dict()).keys())
    for name, res_def in template.get("Resources", {}).items():
        resource_type = res_def.get("Type", "AWS::CloudFormation::CustomResource")
        if resource_type in _TRANSITIVE_RESOURCES.keys() and _TRANSITIVE_RESOURCES[resource_type].get("generate_transitive") is not None:
            handler = _TRANSITIVE_RESOURCES[resource_type]["generate_transitive"]
            added_resources = handler(name, res_def, existing_resource_names)
            new_resources.update(added_resources)
    logger.debug("Generated %n transitive resources for changeset %s", len(new_resources), cs_name)
    return new_resources

def _generate_transitive_sam_function_resources(res_name: str,res_def: dict, prohibited_resource_names: list[str]) -> dict:
    """
    Generate transitive resources for sam function if needed.
    This will generate AWS::IAM::Role for the function if no Role is set.
    The new role will have the Policies attached to its ManagedPolicyArns, only managed policy and aws-managed policy aliases will be supported.
    #TODO: Support inline Policies and SAM Policy template.
    """
    role = res_def.get("Properties", {}).get("Role")
    new_resources = {}
    if role is None:


        role_name = res_name+"Role"
        while role_name in prohibited_resource_names:
            role_name= res_name+"Role"+ new_uuid()[:4]

        role = {
            "Metadata": {
                "SamResourceId": role_name
            },
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "lambda.amazonaws.com"
                            }
                        }
                    ],
                    "Version": "2012-10-17"
                },
                "ManagedPolicyArns": [],
                "Tags": [
                    {
                        "Value": "SAM",
                        "Key": "lambda:createdBy"
                    }
                ]
            },
            "Type": "AWS::IAM::Role"
        }

        # Attach the role to the function
        res_def.get("Properties", {}).update({"Role":{'Fn::GetAtt': [role_name, 'Arn']}})

        new_resources[role_name] = role

    return new_resources


# ===========================================================================
# Interdependent resources
# ===========================================================================
def _manage_interdependent_resources(cs_name: str, template: dict):
    """Manage relationships between interdependent resources"""
    for name, res_def in template.get("Resources", {}).items():
        resource_type = res_def.get("Type", "AWS::CloudFormation::CustomResource")
        if resource_type in _TRANSITIVE_RESOURCES.keys() and _TRANSITIVE_RESOURCES[resource_type].get("manage_interdependent") is not None:
            handler = _TRANSITIVE_RESOURCES[resource_type]["manage_interdependent"]
            handler(name, res_def, template.get("Resources",{}))
            logger.debug("Managed interdependent resource for %s in ChangeSet", name,cs_name)

def _map_policies(policies, res_name: str) -> list[str] | None:
    """Map a SAM Policies value to a list of managed policy ARNs.

    Accepts None, a single string, or a list of strings. Each entry must be
    either a known AWS-managed policy alias or a full policy ARN.
    """
    if policies is None:
        return None

    import data.iam
    from services.iam import _is_valid_policy_arn

    if isinstance(policies, str):
        policies = [policies]

    if not isinstance(policies, list):
        raise TypeError("InvalidPolicyTypeException", f"Invalid policies type attached to {res_name}")

    managed_policies: list[str] = []
    for policy in policies:
        if policy in data.iam.AWS_MANAGED_POLICY_ALIASES:
            managed_policies.append(data.iam.get_full_policy_arn(policy))
        elif _is_valid_policy_arn(policy):
            managed_policies.append(policy)
        else:
            # Todo: returning _error() in nested function to represent errors is hard to manage.
            # Todo: Ask the maintainers if it's okay to depend on exception rather than _error() ( it will be a lot of effort, because nesting level is deep ).
            raise ValueError("InvalidPolicyArnException",
                             f"Invalid policy arn: {policy} attached to {res_name}")
    return managed_policies


def _manage_interdependencies_sam_function(res_name: str,res_def: dict,resources:dict):
    def extract_role() -> dict:
        role_name = res_def["Properties"]["Role"].get("Fn::GetAtt", [])[0]

        role = resources.get(role_name, None)
        if role is None:
            raise ValueError("InvalidRoleException",
                             f"Can't find role definition for {role_name} attached to {res_name}")
        return role

    if "Policies" in res_def.get("Properties", {}):
        policies = res_def.get("Properties", {}).get("Policies")
        mapped_policies = _map_policies(policies, res_name)
        if mapped_policies is not None:
            role = extract_role()
            managed_policy_arns = set(role.get("Properties").get("ManagedPolicyArns", []))
            managed_policy_arns.update(mapped_policies)
            role.get("Properties").update({"ManagedPolicyArns": list(managed_policy_arns)})

    # Attach AWSXrayWriteOnlyAccess Policy according to Tracing, if and only if we created the role
    if res_def.get("Properties", {}).get("Tracing") in ["Active", "PassThrough"]:
        role = extract_role()
        tags = role.get("Properties", {}).get("Tags", [])
        role_created_by_sam = next((tag for tag in tags if tag["Key"] == "lambda:createdBy" and tag["Value"] == "SAM" ), None) is not None
        if role_created_by_sam:
            managed_policy_arns = set(role.get("Properties").get("ManagedPolicyArns", []))
            managed_policy_arns.add(get_full_policy_arn("AWSXrayWriteOnlyAccess"))
            role.get("Properties").update({"ManagedPolicyArns": list(managed_policy_arns)})

def _generate_transitive_sam_state_machine_resources(res_name: str, res_def: dict, prohibited_resource_names: list[str]) -> dict:
    """
    Generate transitive resources for a SAM state machine if needed.
    This will generate AWS::IAM::Role for the state machine if no Role is set.
    The new role will have the Policies attached to its ManagedPolicyArns, only managed policy and aws-managed policy aliases will be supported.
    """
    role = res_def.get("Properties", {}).get("Role")
    new_resources = {}
    if role is None:
        role_name = res_name + "Role"
        while role_name in prohibited_resource_names:
            role_name = res_name + "Role" + new_uuid()[:4]

        role = {
            "Metadata": {
                "SamResourceId": role_name
            },
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "states.amazonaws.com"
                            }
                        }
                    ],
                    "Version": "2012-10-17"
                },
                "ManagedPolicyArns": [],
                "Tags": [
                    {
                        "Value": "SAM",
                        "Key": "stateMachine:createdBy"
                    }
                ]
            },
            "Type": "AWS::IAM::Role"
        }

        # Attach the role to the state machine
        res_def.get("Properties", {}).update({"Role": {"Fn::GetAtt": [role_name, "Arn"]}})

        new_resources[role_name] = role

    return new_resources


def _manage_interdependencies_sam_state_machine(res_name: str, res_def: dict, resources: dict):
    def extract_role() -> dict:
        role_name = res_def["Properties"]["Role"].get("Fn::GetAtt", [])[0]
        role = resources.get(role_name, None)
        if role is None:
            raise ValueError("InvalidRoleException",
                             f"Can't find role definition for {role_name} attached to {res_name}")
        return role

    if "Policies" in res_def.get("Properties", {}):
        policies = res_def.get("Properties", {}).get("Policies")
        mapped_policies = _map_policies(policies, res_name)
        if mapped_policies is not None:
            role = extract_role()
            managed_policy_arns = set(role.get("Properties").get("ManagedPolicyArns", []))
            managed_policy_arns.update(mapped_policies)
            role.get("Properties").update({"ManagedPolicyArns": list(managed_policy_arns)})


_TRANSITIVE_RESOURCES = {
    "AWS::Serverless::Function": { "generate_transitive": _generate_transitive_sam_function_resources, "manage_interdependent": _manage_interdependencies_sam_function },
    "AWS::Serverless::StateMachine": { "generate_transitive": _generate_transitive_sam_state_machine_resources, "manage_interdependent": _manage_interdependencies_sam_state_machine },
}