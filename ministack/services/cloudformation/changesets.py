# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
CloudFormation change set handlers — Create, Describe, Execute, Delete, List change sets.
"""

import asyncio
import copy
import json
import logging

import ministack.services.apigateway_v1 as _apigw_v1
import ministack.services.cloudwatch_logs as _cw_logs
import ministack.services.cognito as _cognito
import ministack.services.dynamodb as _dynamodb
import ministack.services.iam as _iam
import ministack.services.iot as _iot
import ministack.services.kms as _kms
import ministack.services.lambda_svc as _lambda_svc
import ministack.services.s3 as _s3
import ministack.services.sns as _sns
import ministack.services.sqs as _sqs
import ministack.services.ssm as _ssm
from ministack.core.responses import get_account_id, get_region, new_uuid, now_iso

from .engine import (
    _NO_VALUE,
    _apply_transforms,
    _evaluate_conditions,
    _parse_template,
    _resolve_parameters,
    _resolve_refs,
    _topological_sort,
    validate_template_support,
)
from .helpers import (
    ON_FAILURE_VALUES,
    _error,
    _esc,
    _extract_members,
    _extract_string_members,
    _p,
    _page,
    _request_problems,
    _resolve_template,
    _xml,
    enum_problems,
    stack_name_problems,
    validation_error_message,
)
from .provisioners import _RESOURCE_HANDLERS, _import_resource
from .stacks import (
    _IMPORT_COMPLETED,
    _IMPORT_STARTED,
    _IMPORT_TAGS,
    _add_event,
    _create_stack_task_in_region,
    _create_then_delete_on_failure,
    _deploy_stack_async,
    _diff_resources,
    _stack_region,
    _stack_region_context,
    _stack_tags_changed,
)

logger = logging.getLogger("cloudformation")


def _extract_resources_to_import(params):
    """``ResourcesToImport.member.N`` as a list of
    ``{ResourceType, LogicalResourceId, ResourceIdentifier}``, the shape
    CreateChangeSet takes for ``ChangeSetType=IMPORT``."""
    result = []
    i = 1
    while True:
        rtype = _p(params, f"ResourcesToImport.member.{i}.ResourceType")
        logical = _p(params, f"ResourcesToImport.member.{i}.LogicalResourceId")
        if not rtype and not logical:
            break
        identifier = {}
        j = 1
        while True:
            key = _p(params, f"ResourcesToImport.member.{i}.ResourceIdentifier.entry.{j}.key")
            if not key:
                break
            identifier[key] = _p(
                params, f"ResourcesToImport.member.{i}.ResourceIdentifier.entry.{j}.value", "")
            j += 1
        result.append({
            "ResourceType": rtype,
            "LogicalResourceId": logical,
            "ResourceIdentifier": identifier,
        })
        i += 1
    return result


# AWS's answer when a ResourceIdentifier names nothing, for the types that do
# not answer with a message of their own.
_IMPORT_NOT_FOUND = "Resource of type '{type}' with identifier '{value}' was not found."


def _sdk_error(message, service, status, request_id=None):
    """A service error as the import's resource handler words it in the
    StatusReason: the service's own message, then its SDK's suffix."""
    return (f"{message} (Service: {service}, Status Code: {status}, "
            f"Request ID: {request_id or new_uuid()}) (SDK Attempt Count: 1)")


def _queue_import_problem(url):
    if not url:
        return "QueueUrl is not found"
    try:
        _sqs._get_q(url)
    except _sqs._Err:
        return _IMPORT_NOT_FOUND.format(type="AWS::SQS::Queue", value=url)
    return None


def _topic_import_problem(arn):
    elements = len(arn.split(":"))
    if elements < 6:
        return _sdk_error("Invalid parameter: TopicArn Reason: An ARN must have at least "
                          f"6 elements, not {elements}", "Sns", 400)
    if arn not in _sns._topics:
        return _sdk_error("Topic does not exist", "Sns", 404)
    return None


def _bucket_import_problem(name):
    if not name:
        return "Unable to marshall request to JSON: Bucket cannot be empty."
    return None if name in _s3._buckets else "Bucket not found"


def _table_import_problem(name):
    problems = []
    if len(name) < 3:
        problems.append(f"Value '{name}' at 'tableName' failed to satisfy constraint: "
                        "Member must have length greater than or equal to 3")
    if not _dynamodb._TABLE_NAME_RE.match(name):
        problems.append(f"Value '{name}' at 'tableName' failed to satisfy constraint: "
                        "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+")
    if problems:
        # DynamoDB's request ids are 52 upper-case letters and digits.
        request_id = (new_uuid() + new_uuid()).replace("-", "")[:52].upper()
        return _sdk_error(validation_error_message(problems), "DynamoDb", 400, request_id)
    return None if name in _dynamodb._tables else f"Table: {name} does not exist."


def _parameter_import_problem(name):
    if not name:
        return _sdk_error(
            "1 validation error detected: Value '[]' at 'names' failed to satisfy constraint: "
            "Member must satisfy constraint: [Member must have length less than or equal to "
            "2048, Member must have length greater than or equal to 1]", "Ssm", 400)
    if _ssm._lookup_parameter(name)[1] is None:
        return _IMPORT_NOT_FOUND.format(type="AWS::SSM::Parameter", value=name)
    return None


def _role_import_problem(name):
    if name not in _iam._roles:
        return _sdk_error(f"The role with name {name} cannot be found.", "Iam", 404)
    return None


def _log_group_import_problem(name):
    if name not in _cw_logs._log_groups:
        key = json.dumps({"/properties/LogGroupName": name}, separators=(",", ":"))
        return _IMPORT_NOT_FOUND.format(type="AWS::Logs::LogGroup", value=key)
    return None


def _function_import_problem(name):
    if name not in _lambda_svc._functions:
        arn = f"arn:aws:lambda:{get_region()}:{get_account_id()}:function:{name}"
        return _sdk_error(f"Function not found: {arn}", "Lambda", 404)
    return None


def _iot_policy_import_problem(name):
    if name not in _iot._policies:
        return _IMPORT_NOT_FOUND.format(type="AWS::IoT::Policy", value=name)
    return None


def _ca_certificate_import_problem(ca_id):
    if ca_id not in _iot._ca_certificates:
        return _IMPORT_NOT_FOUND.format(type="AWS::IoT::CACertificate", value=ca_id)
    return None


def _user_pool_import_problem(pool_id):
    if pool_id not in _cognito._user_pools:
        return _sdk_error(f"User pool {pool_id} does not exist.", "CognitoIdentityProvider", 400)
    return None


def _kms_key_import_problem(key_id):
    if key_id not in _kms._keys:
        arn = f"arn:aws:kms:{get_region()}:{get_account_id()}:key/{key_id}"
        return _sdk_error(f"Key '{arn}' does not exist", "Kms", 400)
    return None


def _kms_alias_import_problem(name):
    # AWS fails the change set without a StatusReason.
    return None if _kms._alias_arn(name) in _kms._aliases else ""


def _thing_type_import_problem(name):
    if name not in _iot._thing_types:
        return _IMPORT_NOT_FOUND.format(type="AWS::IoT::ThingType", value=name)
    return None


def _cognito_error(message):
    return _sdk_error(message, "CognitoIdentityProvider", 400)


def _user_pool_client_import_problem(pool_id, client_id):
    if pool_id not in _cognito._user_pools:
        return _user_pool_import_problem(pool_id)
    if client_id not in _cognito._user_pools[pool_id]["_clients"]:
        return _cognito_error("User pool client does not exist.")
    return None


def _user_pool_group_import_problem(pool_id, group_name):
    if pool_id not in _cognito._user_pools:
        return _user_pool_import_problem(pool_id)
    if group_name not in _cognito._user_pools[pool_id]["_groups"]:
        return _cognito_error("Group not found.")
    return None


def _resource_server_import_problem(pool_id, identifier):
    if pool_id not in _cognito._user_pools:
        return _user_pool_import_problem(pool_id)
    if identifier not in _cognito._pool_resource_servers(_cognito._user_pools[pool_id]):
        return _cognito_error(f"{identifier} does not exist in user pool {pool_id}.")
    return None


def _identity_pool_import_problem(pool_id):
    if pool_id not in _cognito._identity_pools:
        return _sdk_error(f"IdentityPool '{pool_id}' not found.", "CognitoIdentity", 400)
    return None


def _rest_api_import_problem(api_id):
    if api_id not in _apigw_v1._rest_apis:
        return _sdk_error(f"Invalid API identifier specified {get_account_id()}:{api_id}",
                          "ApiGateway", 404)
    return None


def _stage_import_problem(api_id, stage_name):
    if api_id not in _apigw_v1._rest_apis:
        return _rest_api_import_problem(api_id)
    if stage_name not in _apigw_v1._stages_v1.get(api_id, {}):
        return _sdk_error("Invalid stage identifier specified", "ApiGateway", 404)
    return None


# Per type: the ResourceIdentifier keys (schema primaryIdentifier), the last one being the change's
# physical id, and the FAILED StatusReason lookup (measured).
_IMPORT_LOOKUPS = {
    "AWS::SQS::Queue": (("QueueUrl",), _queue_import_problem),
    "AWS::SNS::Topic": (("TopicArn",), _topic_import_problem),
    "AWS::S3::Bucket": (("BucketName",), _bucket_import_problem),
    "AWS::DynamoDB::Table": (("TableName",), _table_import_problem),
    "AWS::SSM::Parameter": (("Name",), _parameter_import_problem),
    "AWS::IAM::Role": (("RoleName",), _role_import_problem),
    "AWS::Logs::LogGroup": (("LogGroupName",), _log_group_import_problem),
    "AWS::Lambda::Function": (("FunctionName",), _function_import_problem),
    "AWS::IoT::Policy": (("Id",), _iot_policy_import_problem),
    "AWS::IoT::CACertificate": (("Id",), _ca_certificate_import_problem),
    "AWS::Cognito::UserPool": (("UserPoolId",), _user_pool_import_problem),
    "AWS::KMS::Key": (("KeyId",), _kms_key_import_problem),
    "AWS::KMS::Alias": (("AliasName",), _kms_alias_import_problem),
    "AWS::IoT::ThingType": (("ThingTypeName",), _thing_type_import_problem),
    "AWS::Cognito::UserPoolClient": (("UserPoolId", "ClientId"), _user_pool_client_import_problem),
    "AWS::Cognito::UserPoolGroup": (("UserPoolId", "GroupName"), _user_pool_group_import_problem),
    "AWS::Cognito::UserPoolResourceServer": (
        ("UserPoolId", "Identifier"), _resource_server_import_problem),
    "AWS::Cognito::IdentityPool": (("Id",), _identity_pool_import_problem),
    "AWS::ApiGateway::RestApi": (("RestApiId",), _rest_api_import_problem),
    "AWS::ApiGateway::Stage": (("RestApiId", "StageName"), _stage_import_problem),
}


def _listed(ids):
    return "[" + ", ".join(ids) + "]"


def _tag_dict(tags):
    return {t.get("Key"): t.get("Value") for t in tags or []}


# The StatusReason AWS gives an executable IMPORT change set.
_IMPORT_REVIEW = ("Verify that resources and their properties defined in the template match "
                  "the intended configuration of the resource import to avoid unexpected changes.")


def _import_changes(resources_to_import, template, diff):
    """The Import changes of an IMPORT change set.

    Raises ValueError with the message CreateChangeSet refuses the request
    with, checking in the order AWS does where two problems meet. ``diff`` is
    the template diff against the stack: an import may add only the resources
    it imports, and must leave everything already in the stack untouched.
    """
    declared = (template or {}).get("Resources", {})
    logicals = [entry["LogicalResourceId"] for entry in resources_to_import]
    if len(set(logicals)) != len(logicals):
        raise ValueError("Every resource to import must have unique LogicalResourceId")
    undeclared = [lid for lid in logicals if lid not in declared]
    if undeclared:
        raise ValueError(f"The logical resource ids {_listed(undeclared)} provided in "
                         "ResourceToImport do not exist in the template.")
    for entry in resources_to_import:
        logical, rtype = entry["LogicalResourceId"], entry.get("ResourceType")
        identifier = entry.get("ResourceIdentifier") or {}
        keys = _IMPORT_LOOKUPS[rtype][0] if rtype in _IMPORT_LOOKUPS else None
        if keys and sorted(identifier) != sorted(keys):
            raise ValueError(f"Invalid resource identifier for resource type {rtype}. "
                             f"Expected [{', '.join(keys)}]")
        if rtype != declared[logical].get("Type"):
            raise ValueError(
                f"Resource type of [{logical}] passed in ResourceToImport does not match with "
                "resource type defined in the template. Resource type in ResourceToImport: "
                f"{rtype}; Resource type in Template: {declared[logical].get('Type')}.")

    touched = {}
    for change in diff:
        rc = change["ResourceChange"]
        touched.setdefault(rc["Action"], []).append(rc["LogicalResourceId"])
    added = touched.get("Add", [])
    if not added:
        raise ValueError("The template should contain at least one new resource to import.")
    in_stack = [lid for lid in logicals if lid not in added]
    if in_stack:
        raise ValueError(f"Resources {_listed(in_stack)} passed in ResourceToImport are already "
                         "in a stack and cannot be imported.")
    not_imported = [lid for lid in added if lid not in logicals]
    if not_imported:
        raise ValueError(f"Resources {_listed(not_imported)} is missing from ResourceToImport list")
    modified = sorted(touched.get("Modify", []) + touched.get("Remove", []))
    if modified:
        raise ValueError(f"You have modified resources {_listed(modified)} in your template that "
                         "are not being imported. Update, create or delete operations cannot be "
                         "executed during import operations.")
    no_policy = [lid for lid in logicals if "DeletionPolicy" not in declared[lid]]
    if no_policy:
        raise ValueError(f"The following resources to import {_listed(no_policy)} must have "
                         "DeletionPolicy attribute specified in the template.")

    return [_import_change(entry) for entry in resources_to_import]


def _import_change(entry):
    """The Import change of one resource to import."""
    change = {
        "Action": "Import",
        "LogicalResourceId": entry["LogicalResourceId"],
        "ResourceType": entry["ResourceType"],
        "Scope": [],
        "Details": [],
    }
    # Key joining for unlisted multi-key types is unmeasured, so none is invented.
    identifier = entry["ResourceIdentifier"]
    if entry["ResourceType"] in _IMPORT_LOOKUPS:
        change["PhysicalResourceId"] = identifier[_IMPORT_LOOKUPS[entry["ResourceType"]][0][-1]]
    elif len(identifier) == 1:
        change["PhysicalResourceId"] = next(iter(identifier.values()))
    return {"ResourceChange": change}


def _existing_resources(changes, template):
    """The added resources ImportExistingResources finds, in the stack's region."""
    found = []
    for change in changes:
        rc = change["ResourceChange"]
        rtype, logical_id = rc["ResourceType"], rc["LogicalResourceId"]
        if (rc["Action"] != "Add" or rtype not in _IMPORT_LOOKUPS
                or "import" not in _RESOURCE_HANDLERS.get(rtype, {})):
            continue
        # Only a name written as a literal counts; one from a Ref or Fn::Sub is created.
        props = template["Resources"][logical_id].get("Properties") or {}
        keys = _IMPORT_LOOKUPS[rtype][0]
        if not all(isinstance(props.get(key), str) and props[key] for key in keys):
            continue
        entry = {"ResourceType": rtype, "LogicalResourceId": logical_id,
                 "ResourceIdentifier": {key: props[key] for key in keys}}
        if _import_not_found([entry]) is None:
            found.append(entry)
    return found


def _unretained_import_reason(entries, template):
    """AWS's StatusReason when an existing resource to import is not retained, or None."""
    declared = template.get("Resources", {})
    unretained = [
        f"{e['LogicalResourceId']} ({{{', '.join(f'{k}={v}' for k, v in e['ResourceIdentifier'].items())}}})"
        for e in sorted(entries, key=lambda e: list(declared).index(e["LogicalResourceId"]))
        if declared[e["LogicalResourceId"]].get("DeletionPolicy") not in ("Retain", "RetainExceptOnCreate")
    ]
    if not unretained:
        return None
    return ("CloudFormation is attempting to import some resources because they already exist in "
            "your account. The resources must have the DeletionPolicy attribute set to 'Retain' or "
            "'RetainExceptOnCreate' in the template for successful import. The affected resources "
            f"are {', '.join(unretained)}")


def _import_ref(rtype, value):
    """What an import identifier and a stack's physical id are compared on:
    a queue's account and name, since its URL's host is the endpoint it was
    read through, and the value itself for any other type."""
    if rtype == "AWS::SQS::Queue":
        return _sqs._queue_ref_from_urlish(value)
    return value


def _import_owner(resources_to_import):
    """The StatusReason of an import naming a resource a stack already holds,
    or None. AWS fails the change set for it, as for a resource that does not
    exist. Evaluate it in the stack's region."""
    from ministack.services.cloudformation import _stacks
    for entry in resources_to_import:
        rtype, identifier = entry["ResourceType"], entry["ResourceIdentifier"]
        if rtype in _IMPORT_LOOKUPS:
            keys = _IMPORT_LOOKUPS[rtype][0]
        elif len(identifier) == 1:
            keys = tuple(identifier)
        else:
            continue
        # The last key is the physical id; the others are the resource's properties.
        *parents, last = keys
        ref = _import_ref(rtype, identifier[last])
        for stack in _stacks.values():
            if stack.get("StackStatus") == "DELETE_COMPLETE":
                continue
            for res in stack.get("_resources", {}).values():
                pid, props = res.get("PhysicalResourceId"), res.get("Properties") or {}
                if (res.get("ResourceType") == rtype and pid and _import_ref(rtype, pid) == ref
                        and all(props.get(key) == identifier[key] for key in parents)):
                    value = "|".join(identifier[key] for key in keys)
                    return f"{value} already exists in stack {stack['StackId']}"
    return None


def _import_not_found(resources_to_import):
    """The StatusReason of an import whose identifier is invalid or names
    nothing, or None.

    AWS accepts such a request and fails the change set once it looks the
    resource up, so this is not a refusal. Evaluate it in the stack's region.
    """
    for entry in resources_to_import:
        rtype, identifier = entry["ResourceType"], entry["ResourceIdentifier"]
        if rtype in _IMPORT_LOOKUPS:
            keys, problem = _IMPORT_LOOKUPS[rtype]
            reason = problem(*(identifier[key] for key in keys))
            if reason is not None:
                return reason
            continue
        # A blank name can name nothing, whatever the type.
        blank = next((value for value in identifier.values() if not value.strip()), None)
        if blank is not None:
            return _IMPORT_NOT_FOUND.format(type=rtype, value=blank)
    return None


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
    from ministack.services.cloudformation.handlers import (
        _check_capabilities,
        _resolve_stack,
    )
    stack_name = _p(params, "StackName")
    cs_name = _p(params, "ChangeSetName")
    cs_type = _p(params, "ChangeSetType", "UPDATE")
    import_existing = (cs_type in ("CREATE", "UPDATE")
                       and _p(params, "ImportExistingResources", "false").lower() == "true")

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

    if cs_type == "CREATE":
        # The request-level constraints of a new stack, joined as the API does.
        if request_error := _request_problems(params, stack_name):
            return request_error
    if problems := enum_problems(params, "OnStackFailure", "onStackFailure",
                                 ON_FAILURE_VALUES):
        return _error("ValidationError", validation_error_message(problems))
    on_stack_failure = _p(params, "OnStackFailure")
    # "DELETE ... is only valid when the ChangeSetType parameter is set to
    # CREATE" (API_CreateChangeSet); the error wording is not measured.
    if on_stack_failure == "DELETE" and cs_type != "CREATE":
        return _error("ValidationError",
                      "OnStackFailure DELETE is only valid when ChangeSetType is CREATE.")

    template_body, resolve_err = _resolve_template(params)
    if resolve_err:
        return resolve_err
    template_given = bool(template_body)

    provided_params = _extract_members(params, "Parameters")
    resources_to_import = _extract_resources_to_import(params)
    # The API's parameter validation: a member without a ResourceIdentifier.
    if missing := [
        f"Value null at 'resourcesToImport.{i}.member.resourceIdentifier' failed to satisfy "
        "constraint: Member must not be null"
        for i, entry in enumerate(resources_to_import, 1) if not entry["ResourceIdentifier"]
    ]:
        return _error("ValidationError", validation_error_message(missing))
    if cs_type == "IMPORT" and not resources_to_import:
        return _error("ValidationError", "Must Provide at least one resource to import")
    tags = _extract_members(params, "Tags")
    # An empty Tags list arrives as ``Tags=``: given-empty clears the stack's
    # tags on execute, an omitted Tags keeps them (as UpdateStack does).
    tags_given = "Tags" in params or bool(tags)
    from .helpers import _validate_stack_tags
    tags_error = _validate_stack_tags(tags)
    if tags_error:
        return tags_error

    stack = _resolve_stack(stack_name)
    if stack is not None and cs_type != "CREATE":
        # A change set is keyed by the stack's name; an UPDATE set addressed
        # by stack id carries on under the name.
        stack_name = stack.get("StackName", stack_name)

    # An IMPORT set naming a stack that does not exist creates it in
    # REVIEW_IN_PROGRESS, as a CREATE set does, and imports into it.
    new_stack_import = cs_type == "IMPORT" and (
        not stack or stack.get("StackStatus") == "DELETE_COMPLETE")
    if new_stack_import and (problems := stack_name_problems(_p(params, "StackName"))):
        return _error("ValidationError", validation_error_message(problems))

    if cs_type == "CREATE" or new_stack_import:
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
            # Measured on AWS for the stack an import creates; a CREATE set's
            # placeholder keeps the empty reason it always had.
            "StackStatusReason": "User Initiated" if new_stack_import else "",
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
                   stack["StackStatusReason"], physical_id=stack_id)
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

    def _rejected(reason):
        # A rejected CreateChangeSet leaves no stack behind on AWS; drop the
        # REVIEW_IN_PROGRESS placeholder created above for a new stack.
        # ``reason`` is a ValidationError message or a ready error response.
        if cs_type == "CREATE" or new_stack_import:
            _stacks.pop(stack_name, None)
            _stack_events.pop(stack_id, None)
        if isinstance(reason, tuple):
            return reason
        return _error("ValidationError", reason)

    try:
        template = sent = _parse_template(template_body)
        template = _apply_transforms(template, provided_params,
                                     stack.get("_resolved_params", {}))
    except Exception as e:
        return _rejected(f"Template format error: {e}")

    # Checked after the transform, as CreateStack and UpdateStack do.
    # CAPABILITY_AUTO_EXPAND does not apply to a change set
    # (API_CreateChangeSet), so only the IAM rule runs here.
    if caps_error := _check_capabilities(sent, template, params, macros=False):
        return _rejected(caps_error)

    try:
        param_values = _resolve_parameters(
            template, provided_params, stack.get("_resolved_params", {}))
    except ValueError as exc:
        return _rejected(str(exc))

    if cs_type == "UPDATE" and not template_given and stack.get("_template"):
        # As UpdateStack with UsePreviousTemplate: the stored processed
        # template, so an AWS::Include snippet changed in S3 is not picked up.
        template = copy.deepcopy(stack["_template"])
    try:
        validate_template_support(
            template, _evaluate_conditions(template, param_values), params=param_values)
    except ValueError as exc:
        return _rejected(str(exc))

    # Compute changes — resolve parameters/intrinsics in BOTH templates first so
    # parameter-driven changes (the `aws cloudformation deploy
    # --parameter-overrides` pattern, e.g. a Lambda Code S3Key behind a Ref) are
    # detected instead of compared as identical raw nodes (#897).
    # An import is checked against the stack it imports into, like an update.
    diffs_the_stack = cs_type in ("UPDATE", "IMPORT")
    old_template = stack.get("_template", {}) if diffs_the_stack else {}
    old_params = stack.get("_resolved_params", {}) if diffs_the_stack else {}
    with _stack_region_context(stack, stack_id):
        old_resolved = _resolve_props_for_diff(old_template, old_params, stack_name, stack_id)
        new_resolved = _resolve_props_for_diff(template, param_values, stack_name, stack_id)
    changed_params = {k for k, v in param_values.items()
                      if old_params.get(k, {}).get("Value") != v.get("Value")}
    retag = (stack.get("_resources", {}) if cs_type == "UPDATE"
             and _stack_tags_changed(stack, tags, tags_given) else ())
    changes = _diff_resources(old_resolved, new_resolved, stack.get("_resources"),
                              template, changed_params, retag)
    import_failure = None
    if cs_type == "IMPORT":
        previous_tags = {} if new_stack_import else _tag_dict(stack.get("Tags"))
        frozen = next((name for name, changed in (
            ("Outputs", template.get("Outputs", {}) != old_template.get("Outputs", {})),
            ("Tags", tags_given and _tag_dict(tags) != previous_tags),
        ) if changed), None)
        if frozen:
            return _rejected(f"As part of the import operation, you cannot modify or add [{frozen}]")
        # An import describes the resources being adopted, not the template
        # diff, which would call each of them `Add`; the diff only decides
        # what else the template may not do during an import.
        try:
            changes = _import_changes(resources_to_import, template, changes)
        except ValueError as exc:
            return _rejected(str(exc))
        with _stack_region_context(stack, stack_id):
            # AWS looks the resource up before it asks whether a stack holds
            # it: a stack's queue deleted behind its back is "not found".
            import_failure = _import_not_found(resources_to_import)
            if import_failure is None:
                import_failure = _import_owner(resources_to_import)
        if import_failure is not None:
            changes = []
    existing = []
    if import_existing:
        with _stack_region_context(stack, stack_id):
            existing = _existing_resources(changes, template)
            import_failure = _unretained_import_reason(existing, template) or _import_owner(existing)
        if import_failure is not None:
            changes = []
        adopted = {e["LogicalResourceId"]: _import_change(e) for e in existing}
        changes = [adopted.get(c["ResourceChange"]["LogicalResourceId"], c) for c in changes]

    cs_id = (
        f"arn:aws:cloudformation:{_stack_region(stack, stack_id)}:{get_account_id()}:"
        f"changeSet/{cs_name}/{new_uuid()}"
    )

    if import_failure is not None:
        # AWS accepts an import of a resource that does not exist and fails
        # the change set, with no changes, once it looks the resource up.
        _cs_status, _cs_exec, _cs_reason = "FAILED", "UNAVAILABLE", import_failure
    elif cs_type == "IMPORT":
        unsupported = sorted({e["ResourceType"] for e in resources_to_import
                              if "import" not in _RESOURCE_HANDLERS.get(e["ResourceType"], {})})
        if unsupported:
            _cs_status, _cs_exec = "CREATE_COMPLETE", "UNAVAILABLE"
            _cs_reason = ("Resource import is not supported by this emulator for "
                          + ", ".join(unsupported))
        else:
            _cs_status, _cs_exec, _cs_reason = "CREATE_COMPLETE", "AVAILABLE", _IMPORT_REVIEW
    elif changes:
        _cs_status, _cs_exec, _cs_reason = "CREATE_COMPLETE", "AVAILABLE", ""
    else:
        # Real AWS: a change set with no changes ends FAILED and cannot be
        # executed (ExecutionStatus UNAVAILABLE), with this exact reason.
        _cs_status, _cs_exec = "FAILED", "UNAVAILABLE"
        _cs_reason = (
            "The submitted information didn't contain changes. "
            "Submit different information to create a change set."
        )
    change_set = {
        "ChangeSetId": cs_id,
        "ChangeSetName": cs_name,
        "StackId": stack_id,
        "StackName": stack_name,
        "Status": _cs_status,
        "ExecutionStatus": _cs_exec,
        "StatusReason": _cs_reason,
        "CreationTime": now_iso(),
        "Description": _p(params, "Description", ""),
        "ChangeSetType": cs_type,
        "Changes": changes,
        "Parameters": [
            {"ParameterKey": k, "ParameterValue": v["Value"]}
            for k, v in param_values.items()
        ],
        "Tags": tags,
        "Capabilities": _extract_string_members(params, "Capabilities"),
        "OnStackFailure": on_stack_failure,
        "_tags_given": tags_given,
        "_template": template,
        "_template_body": template_body,
        "_resolved_params": param_values,
        "_resources_to_import": resources_to_import,
        "_import_existing": {e["LogicalResourceId"]: e["ResourceIdentifier"] for e in existing},
        "ImportExistingResources": import_existing,
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
        return _error("ChangeSetNotFound",
                      f"ChangeSet [{cs_name}] does not exist", 404)

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
        scope_xml = "".join(f"<member>{_esc(a)}</member>" for a in rc.get("Scope", []))
        details_xml = ""
        for d in rc.get("Details", []):
            target = d.get("Target", {})
            target_xml = f"<Attribute>{_esc(target.get('Attribute', ''))}</Attribute>"
            if target.get("Name"):
                target_xml += f"<Name>{_esc(target['Name'])}</Name>"
            if target.get("RequiresRecreation"):
                target_xml += (
                    f"<RequiresRecreation>{_esc(target['RequiresRecreation'])}"
                    "</RequiresRecreation>"
                )
            causing_xml = (
                f"<CausingEntity>{_esc(d['CausingEntity'])}</CausingEntity>"
                if d.get("CausingEntity") else ""
            )
            source_xml = (
                f"<ChangeSource>{_esc(d['ChangeSource'])}</ChangeSource>"
                if d.get("ChangeSource") else ""
            )
            details_xml += (
                "<member>"
                f"<Target>{target_xml}</Target>"
                f"<Evaluation>{_esc(d.get('Evaluation', 'Static'))}</Evaluation>"
                f"{source_xml}"
                f"{causing_xml}"
                "</member>"
            )
        # botocore reads an empty element as "", so a member the change does
        # not carry (an Import's Replacement, an Add's physical id) is left
        # out instead of written empty.
        physical_xml = (
            f"<PhysicalResourceId>{_esc(rc['PhysicalResourceId'])}</PhysicalResourceId>"
            if rc.get("PhysicalResourceId") else ""
        )
        replacement_xml = (
            f"<Replacement>{rc['Replacement']}</Replacement>" if "Replacement" in rc else ""
        )
        policy_xml = (
            f"<PolicyAction>{rc['PolicyAction']}</PolicyAction>" if "PolicyAction" in rc else ""
        )
        # "Resource" is the one ChangeType, and AWS reports it on every change.
        changes_xml += (
            "<member><Type>Resource</Type><ResourceChange>"
            f"<Action>{rc.get('Action', '')}</Action>"
            f"<LogicalResourceId>{_esc(rc.get('LogicalResourceId', ''))}</LogicalResourceId>"
            f"{physical_xml}"
            f"<ResourceType>{_esc(rc.get('ResourceType', ''))}</ResourceType>"
            f"{replacement_xml}"
            f"{policy_xml}"
            f"<Scope>{scope_xml}</Scope>"
            f"<Details>{details_xml}</Details>"
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

    import_existing_xml = (
        "<ImportExistingResources>true</ImportExistingResources>"
        if cs.get("ImportExistingResources") else ""
    )
    inner = (
        f"<ChangeSetId>{_esc(cs['ChangeSetId'])}</ChangeSetId>"
        f"<ChangeSetName>{_esc(cs['ChangeSetName'])}</ChangeSetName>"
        f"<StackId>{_esc(cs['StackId'])}</StackId>"
        f"<StackName>{_esc(cs['StackName'])}</StackName>"
        f"<Status>{cs['Status']}</Status>"
        f"<StatusReason>{_esc(cs.get('StatusReason', ''))}</StatusReason>"
        f"<ExecutionStatus>{cs['ExecutionStatus']}</ExecutionStatus>"
        f"<CreationTime>{cs['CreationTime']}</CreationTime>"
        f"<Description>{_esc(cs.get('Description', ''))}</Description>"
        f"<ChangeSetType>{cs.get('ChangeSetType', '')}</ChangeSetType>"
        f"{import_existing_xml}"
        "<Capabilities>"
        + "".join(f"<member>{_esc(c)}</member>" for c in cs.get("Capabilities", []))
        + "</Capabilities>"
        f"<Parameters>{params_xml}</Parameters>"
        f"<Changes>{changes_xml}</Changes>"
        f"<Tags>{tags_xml}</Tags>"
    )
    if cs.get("OnStackFailure"):
        inner += f"<OnStackFailure>{cs['OnStackFailure']}</OnStackFailure>"

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
        # A DELETE_* status is the OnStackFailure=DELETE cleanup of a failed create.
        if (status.endswith("_COMPLETE") and "ROLLBACK" not in status
                and not status.startswith("DELETE")):
            change_set["ExecutionStatus"] = "EXECUTE_COMPLETE"
        else:
            change_set["ExecutionStatus"] = "EXECUTE_FAILED"


def _execute_change_set(params):
    from ministack.services.cloudformation import _stacks
    cs_name = _p(params, "ChangeSetName")
    stack_name = _p(params, "StackName")
    _executed_cs_id, cs = _find_change_set(cs_name, stack_name)
    if not cs:
        return _error("ChangeSetNotFound",
                      f"ChangeSet [{cs_name}] does not exist", 404)

    if cs["ExecutionStatus"] != "AVAILABLE":
        return _error("InvalidChangeSetStatus",
                      f"ChangeSet [{cs_name}] is in {cs['ExecutionStatus']} status")

    # DisableRollback "can't be specified when the OnStackFailure parameter to
    # the CreateChangeSet API operation was specified" (API_ExecuteChangeSet);
    # the error wording is not measured.
    on_stack_failure = cs.get("OnStackFailure", "")
    if on_stack_failure and "DisableRollback" in params:
        return _error("ValidationError",
                      "You can't specify DisableRollback when the change set was "
                      "created with OnStackFailure.")
    # DO_NOTHING is DisableRollback=true, ROLLBACK is false, DELETE rolls back
    # and then deletes the new stack. Without either, the stack rolls back as
    # it always did here; the API reference says "Default: True" for
    # DisableRollback, which is not measured and not followed.
    disable_rollback = (on_stack_failure == "DO_NOTHING"
                        or _p(params, "DisableRollback", "false").lower() == "true")

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
    if cs_type == "IMPORT":
        _start_import(cs, stack)
        _drop_other_change_sets(stack_id, _executed_cs_id)
        return _xml(200, "ExecuteChangeSetResponse",
                    "<ExecuteChangeSetResult></ExecuteChangeSetResult>")

    if is_update:
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
    else:
        previous_stack = None
    retain_except_on_create = _p(params, "RetainExceptOnCreate", "false").lower() == "true"

    status_prefix = "UPDATE" if is_update else "CREATE"
    if not is_update:
        stack["DisableRollback"] = disable_rollback
    stack["StackStatus"] = f"{status_prefix}_IN_PROGRESS"
    stack["LastUpdatedTime"] = now_iso()
    stack["_template_body"] = template_body
    # The stack reports what the operation acknowledged, and for an executed
    # change set that is what the change set was created with.
    stack["Capabilities"] = list(cs.get("Capabilities", []))
    if tags or cs.get("_tags_given"):
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

        deploy = _deploy_stack_async(real_stack_name, stack_id, template,
                                     param_values, disable_rollback, tags,
                                     is_update=is_update,
                                     previous_stack=previous_stack,
                                     retain_except_on_create=retain_except_on_create,
                                     imports=cs.get("_import_existing"))
        if on_stack_failure == "DELETE" and not is_update:
            deploy = _create_then_delete_on_failure(real_stack_name, stack_id, deploy)
        _create_stack_task_in_region(
            _track_change_set_execution(cs, stack, deploy),
            stack,
            stack_id,
        )

    _drop_other_change_sets(stack_id, _executed_cs_id)

    # ExecutionStatus stays EXECUTE_IN_PROGRESS until the deploy finishes, when
    # _track_change_set_execution sets EXECUTE_COMPLETE or EXECUTE_FAILED. Status
    # is the ChangeSetStatus and stays CREATE_COMPLETE: EXECUTE_COMPLETE is not a
    # ChangeSetStatus value, and writing it here made the CDK reject the change
    # set as "not ready" (it gates on Status == CREATE_COMPLETE). #1418
    return _xml(200, "ExecuteChangeSetResponse",
                "<ExecuteChangeSetResult></ExecuteChangeSetResult>")


def _drop_other_change_sets(stack_id, executed_cs_id):
    """Delete the stack's other change sets, as AWS does on execute."""
    from ministack.services.cloudformation import _change_sets
    for cid in [c for c, v in _change_sets.items()
                if v.get("StackId") == stack_id and c != executed_cs_id]:
        _change_sets.pop(cid, None)


def _start_import(cs, stack):
    """Put the stack into IMPORT_IN_PROGRESS and adopt the change set's resources."""
    stack_id, stack_name = stack["StackId"], stack["StackName"]
    new_stack = stack["StackStatus"] == "REVIEW_IN_PROGRESS"
    stack["StackStatus"] = "IMPORT_IN_PROGRESS"
    stack["StackStatusReason"] = "User Initiated"
    stack["LastUpdatedTime"] = now_iso()
    with _stack_region_context(stack, stack_id):
        _add_event(stack_id, stack_name, stack_name, "AWS::CloudFormation::Stack",
                   "IMPORT_IN_PROGRESS", "User Initiated", stack_id)
        _create_stack_task_in_region(
            _track_change_set_execution(cs, stack, _import_resources_async(cs, stack, new_stack)),
            stack, stack_id)


async def _import_resources_async(cs, stack, new_stack):
    """Adopt an IMPORT change set's resources unchanged; one that cannot be read rolls it back."""
    stack_id, stack_name = stack["StackId"], stack["StackName"]
    template, param_values = cs["_template"], cs["_resolved_params"]
    conditions = _evaluate_conditions(template, param_values)
    definitions = template.get("Resources", {})
    entries = {e["LogicalResourceId"]: e for e in cs["_resources_to_import"]}
    order = [lid for lid in _topological_sort(definitions, conditions) if lid in entries]
    resources = stack.setdefault("_resources", {})

    def event(logical_id, status, reason="", physical_id=""):
        _add_event(stack_id, stack_name, logical_id, entries[logical_id]["ResourceType"],
                   status, reason, physical_id)

    started, failure = [], None
    for logical_id in order:
        entry = entries[logical_id]
        rtype = entry["ResourceType"]
        started.append(logical_id)
        event(logical_id, "IMPORT_IN_PROGRESS", _IMPORT_STARTED)
        failure = _import_not_found([entry])
        if failure is None:
            try:
                physical_id, attrs = _import_resource(rtype, entry["ResourceIdentifier"])
                props = _resolve_refs(
                    copy.deepcopy(definitions[logical_id].get("Properties", {})), resources,
                    param_values, conditions, template.get("Mappings", {}), stack_name, stack_id)
            except Exception as exc:
                logger.error("Failed to import %s (%s): %s", logical_id, rtype, exc)
                failure = str(exc)
        if failure is not None:
            event(logical_id, "IMPORT_FAILED", failure)
            break
        event(logical_id, "IMPORT_IN_PROGRESS", physical_id=physical_id)
        event(logical_id, "IMPORT_COMPLETE", _IMPORT_COMPLETED, physical_id)
        resources[logical_id] = {
            "PhysicalResourceId": physical_id,
            "ResourceType": rtype,
            "ResourceStatus": "IMPORT_COMPLETE",
            "LogicalResourceId": logical_id,
            "Properties": {k: v for k, v in props.items() if v is not _NO_VALUE},
            "Attributes": attrs,
            "Timestamp": now_iso(),
        }
    await asyncio.sleep(0)

    if failure is not None:
        stack["StackStatus"] = "IMPORT_ROLLBACK_IN_PROGRESS"
        stack["StackStatusReason"] = failure
        _add_event(stack_id, stack_name, stack_name, "AWS::CloudFormation::Stack",
                   "IMPORT_ROLLBACK_IN_PROGRESS", failure, stack_id)
        for logical_id in reversed(started):
            record = resources.pop(logical_id, None) or {}
            event(logical_id, "IMPORT_ROLLBACK_IN_PROGRESS",
                  physical_id=record.get("PhysicalResourceId", ""))
            event(logical_id, "IMPORT_ROLLBACK_COMPLETE",
                  physical_id=record.get("PhysicalResourceId", ""))
        stack["StackStatus"] = "ROLLBACK_COMPLETE" if new_stack else "IMPORT_ROLLBACK_COMPLETE"
        stack["StackStatusReason"] = ""
        _add_event(stack_id, stack_name, stack_name, "AWS::CloudFormation::Stack",
                   stack["StackStatus"], physical_id=stack_id)
        return

    for logical_id in order:
        record = resources[logical_id]
        event(logical_id, "UPDATE_IN_PROGRESS", _IMPORT_TAGS, record["PhysicalResourceId"])
        record["ResourceStatus"] = "UPDATE_COMPLETE"
        event(logical_id, "UPDATE_COMPLETE", physical_id=record["PhysicalResourceId"])
    stack["_template"] = template
    stack["_template_body"] = cs["_template_body"]
    stack["_resolved_params"] = param_values
    stack["_conditions"] = conditions
    stack["Capabilities"] = list(cs.get("Capabilities", []))
    stack["Parameters"] = [
        {"ParameterKey": k, "ParameterValue": v["Value"], "NoEcho": v["NoEcho"]}
        for k, v in param_values.items()
    ]
    stack["StackStatus"] = "IMPORT_COMPLETE"
    stack["StackStatusReason"] = ""
    _add_event(stack_id, stack_name, stack_name, "AWS::CloudFormation::Stack",
               "IMPORT_COMPLETE", physical_id=stack_id)


# --- DeleteChangeSet ---

def _delete_change_set(params):
    from ministack.services.cloudformation import _change_sets
    from ministack.services.cloudformation.handlers import _resolve_stack
    cs_name = _p(params, "ChangeSetName")
    stack_name = _p(params, "StackName")
    # StackName is "the name or the unique stack ID", and the CDK addresses a
    # stack it has already read by ARN, so resolve it first and look the change
    # set up under the stack's name. A stack that does not exist is a
    # ValidationError, as on AWS -- and a deleted stack counts as one, since it
    # is addressable only by stack id.
    stack = _resolve_stack(stack_name) if stack_name else None
    if stack_name and (not stack or stack.get("StackStatus") == "DELETE_COMPLETE"):
        return _error("ValidationError",
                      f"Stack [{stack_name}] does not exist")
    cs_id, _cs = _find_change_set(cs_name, stack["StackName"] if stack else stack_name)
    # Real CloudFormation answers a delete of a change set that does not exist
    # (on a stack that does) with a plain success, and the CDK relies on that:
    # before every deploy of an existing stack it removes a possible leftover
    # `cdk-deploy-change-set` and only tolerates a `ChangeSetNotFoundException`
    # -- so the 404 answered here aborted every `cdk deploy` of an already
    # deployed stack.
    if cs_id:
        _change_sets.pop(cs_id, None)
    return _xml(200, "DeleteChangeSetResponse", "<DeleteChangeSetResult/>")


# --- ListChangeSets ---

def _list_change_sets(params):
    from ministack.services.cloudformation import _change_sets
    stack_name = _p(params, "StackName")
    if not stack_name:
        return _error("ValidationError", "StackName is required")

    listed = [cs for cs in _change_sets.values() if cs["StackName"] == stack_name]
    listed, next_token_xml, err = _page(listed, params, "ListChangeSets")
    if err:
        return err
    members = ""
    for cs in listed:
        members += (
            "<member>"
            f"<ChangeSetId>{_esc(cs['ChangeSetId'])}</ChangeSetId>"
            f"<ChangeSetName>{_esc(cs['ChangeSetName'])}</ChangeSetName>"
            f"<StackId>{_esc(cs['StackId'])}</StackId>"
            f"<StackName>{_esc(cs['StackName'])}</StackName>"
            f"<Status>{cs['Status']}</Status>"
            f"<StatusReason>{_esc(cs.get('StatusReason', ''))}</StatusReason>"
            f"<ExecutionStatus>{cs['ExecutionStatus']}</ExecutionStatus>"
            f"<CreationTime>{cs['CreationTime']}</CreationTime>"
            f"<Description>{_esc(cs.get('Description', ''))}</Description>"
            "</member>"
        )

    return _xml(200, "ListChangeSetsResponse",
                f"<ListChangeSetsResult>"
                f"<Summaries>{members}</Summaries>"
                f"{next_token_xml}</ListChangeSetsResult>")


# --- GetTemplateSummary ---
