"""
CloudFormation Service Emulator -- AWS-compatible.
Supports: CreateStack, UpdateStack, DeleteStack, DescribeStacks, ListStacks,
          DescribeStackEvents, DescribeStackResource, DescribeStackResources,
          ListStackResources, GetTemplate, ValidateTemplate, ListExports,
          CreateChangeSet, DescribeChangeSet, ExecuteChangeSet,
          DeleteChangeSet, ListChangeSets,
          GetTemplateSummary.
Uses Query API (Action=...) with form-encoded body.
"""

import json
import logging
import os
from urllib.parse import parse_qs

from ministack.core.responses import AccountScopedDict

logger = logging.getLogger("cloudformation")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# In-memory state (shared across all submodules)
_stacks = AccountScopedDict()          # stack_name -> stack dict
_stack_events = AccountScopedDict()    # stack_id -> [event list]
_exports = AccountScopedDict()         # export_name -> {StackId, Name, Value}
_change_sets = AccountScopedDict()     # cs_id -> change set dict

# Re-exports for compatibility
from .engine import (  # noqa: E402
    _parse_template,
    _resolve_parameters,
    _evaluate_conditions,
    _resolve_refs,
    _extract_deps,
    _topological_sort,
    _NO_VALUE,
)

from .helpers import _p  # noqa: E402


async def handle_request(method: str, path: str, headers: dict,
                         body: bytes, query_params: dict) -> tuple:
    params = dict(query_params)
    content_type = headers.get("content-type", "")
    target = headers.get("x-amz-target", "")

    # JSON protocol (newer SDKs): X-Amz-Target: CloudFormation_20100515.ActionName
    if "amz-json" in content_type and target.startswith("CloudFormation_20100515."):
        action_name = target.split(".")[-1]
        params["Action"] = [action_name]
        if body:
            try:
                json_body = json.loads(body)
                for k, v in json_body.items():
                    params[k] = [str(v)] if not isinstance(v, list) else v
            except (json.JSONDecodeError, TypeError):
                pass
    elif method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")
    handler = _ACTION_HANDLERS.get(action)
    if not handler:
        from .helpers import _error
        return _error("InvalidAction", f"Unknown action: {action}", 400)
    return handler(params)


def reset():
    _stacks.clear()
    _stack_events.clear()
    _exports.clear()
    _change_sets.clear()


# Must be last — handlers imports from this module
from .handlers import _ACTION_HANDLERS, _validate_template  # noqa: E402
from ministack.core.responses import AccountScopedDict, get_account_id
