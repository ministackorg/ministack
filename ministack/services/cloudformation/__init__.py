"""
CloudFormation Service Emulator -- AWS-compatible.
Supports: CreateStack, UpdateStack, DeleteStack, DescribeStacks, ListStacks,
          DescribeStackEvents, DescribeStackResource, DescribeStackResources,
          GetTemplate, ValidateTemplate, ListExports,
          CreateChangeSet, DescribeChangeSet, ExecuteChangeSet,
          DeleteChangeSet, ListChangeSets,
          GetTemplateSummary.
Uses Query API (Action=...) with form-encoded body.
"""

import logging
import os
from urllib.parse import parse_qs

logger = logging.getLogger("cloudformation")

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# In-memory state (shared across all submodules)
_stacks: dict = {}          # stack_name -> stack dict
_stack_events: dict = {}    # stack_id -> [event list]
_exports: dict = {}         # export_name -> {StackId, Name, Value}
_change_sets: dict = {}     # cs_id -> change set dict

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
    if method == "POST" and body:
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
