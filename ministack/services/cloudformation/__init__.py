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

import copy
import json
import logging
import os
from urllib.parse import parse_qs

from ministack.core.persistence import load_state
from ministack.core.responses import AccountRegionScopedDict

logger = logging.getLogger("cloudformation")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# In-memory state (shared across all submodules)
_stacks = AccountRegionScopedDict()        # stack_name -> stack dict
_stack_events = AccountRegionScopedDict()  # stack_id -> [event list]
_exports = AccountRegionScopedDict()       # export_name -> {StackId, Name, Value}
_change_sets = AccountRegionScopedDict()   # cs_id -> change set dict

# Re-exports for compatibility
from .engine import (  # noqa: E402
    _NO_VALUE,
    _evaluate_conditions,
    _extract_deps,
    _parse_template,
    _resolve_parameters,
    _resolve_refs,
    _topological_sort,
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
    from ministack.services.cloudformation import custom_resource as _cr
    _cr.reset()


# Stores that need to survive a PERSIST_STATE=1 stop/restore cycle. The actual
# provisioned resources (buckets, functions, tables, …) are persisted by their
# own services; only the CloudFormation metadata (stack records, events,
# exports, and change sets) lived nowhere, so ListStacks/DescribeStacks/
# ListExports came back empty after a warm boot. (#1345, item 8)
_PERSISTED_STORES = (
    (lambda: _stacks, "stacks"),
    (lambda: _stack_events, "stack_events"),
    (lambda: _exports, "exports"),
    (lambda: _change_sets, "change_sets"),
)


def get_state():
    return {key: copy.deepcopy(store()) for store, key in _PERSISTED_STORES}


def restore_state(data):
    if not data:
        return
    for store, key in _PERSISTED_STORES:
        restored = data.get(key)
        target = store()
        if isinstance(restored, AccountRegionScopedDict):
            # Merge the (account, region, key)-scoped entries directly. Restore
            # runs at import time with no request scope, so re-scoping through
            # the public dict interface would misattribute every entry.
            target._data.update(restored._data)
        elif isinstance(restored, dict):
            for k, v in restored.items():
                target[k] = v


# Must be last — handlers imports from this module
from ministack.core.responses import get_account_id

from .handlers import _ACTION_HANDLERS, _validate_template  # noqa: E402

# Restore persisted stack metadata on first import (a CloudFormation request, or
# the eager boot import when a state file exists). Failure falls back to a fresh
# store rather than blocking startup.
try:
    _restored = load_state("cloudformation")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception(
        "Failed to restore persisted CloudFormation state; continuing with a fresh store"
    )
