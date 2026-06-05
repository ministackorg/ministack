"""
Bedrock Agent Service Emulator.
JSON REST API — signing name: bedrock. Endpoint prefix: bedrock-agent.

All 72 operations verified against botocore bedrock-agent-2023-06-05.
Lowercase path segments per AWS spec (`/agentversions`, `/agentaliases`) —
distinct from bedrock-agent-runtime which uses camelCase (`/agentAliases`).

Resource families:
  Agent + AgentVersion + AgentAlias + AgentActionGroup + AgentCollaborator
    + AgentKnowledgeBase + PrepareAgent
  KnowledgeBase + DataSource + IngestionJob + KnowledgeBaseDocuments
  Flow + FlowAlias + FlowVersion + ValidateFlowDefinition + PrepareFlow
  Prompt + PromptVersion
  Tags
"""

import copy
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    get_account_id,
    get_region,
)

logger = logging.getLogger("bedrock-agent")

# ===========================================================================
# Camelize wire-format helper
# ===========================================================================


def _to_camel(key: str) -> str:
    return key[0].lower() + key[1:] if key else key


def _camelize(obj):
    if isinstance(obj, dict):
        return {_to_camel(k): _camelize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_camelize(v) for v in obj]
    return obj


def _json(payload: dict, status: int = 200) -> tuple:
    return status, {"Content-Type": "application/json"}, json.dumps(_camelize(payload)).encode()


def _empty(status: int = 200) -> tuple:
    return status, {"Content-Type": "application/json"}, b"{}"


# ===========================================================================
# Errors
# ===========================================================================


def _error(code: str, message: str, status: int) -> tuple:
    body = json.dumps({"message": message, "__type": code}).encode()
    return status, {"Content-Type": "application/json"}, body


def _not_found(message: str) -> tuple:
    return _error("ResourceNotFoundException", message, 404)


def _conflict(message: str) -> tuple:
    return _error("ConflictException", message, 409)


def _validation(message: str) -> tuple:
    return _error("ValidationException", message, 400)


# ===========================================================================
# Helpers
# ===========================================================================


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _arn(resource_type: str, resource_path: str) -> str:
    return (f"arn:aws:bedrock:{get_region()}:{get_account_id()}:"
            f"{resource_type}/{resource_path}")


def _id(prefix: str = "") -> str:
    return (prefix + uuid.uuid4().hex)[:10].upper()


def _parse_body(body) -> tuple:
    if not body:
        return {}, None
    try:
        obj = json.loads(body)
    except json.JSONDecodeError:
        return None, _validation("Body is not valid JSON.")
    if not isinstance(obj, dict):
        return None, _validation("Body must be a JSON object.")
    return obj, None


# ===========================================================================
# State (all region-scoped)
# ===========================================================================

_agents = AccountRegionScopedDict()                  # agent_id -> Agent
_agent_versions = AccountRegionScopedDict()          # f"{agent_id}/{version}" -> AgentVersion
_agent_aliases = AccountRegionScopedDict()           # f"{agent_id}/{alias_id}" -> AgentAlias
_agent_action_groups = AccountRegionScopedDict()     # f"{agent_id}/{version}/{ag_id}" -> ActionGroup
_agent_collaborators = AccountRegionScopedDict()     # f"{agent_id}/{version}/{c_id}" -> Collaborator
_agent_knowledge_bases = AccountRegionScopedDict()   # f"{agent_id}/{version}/{kb_id}" -> AgentKB
_knowledge_bases = AccountRegionScopedDict()         # kb_id -> KnowledgeBase
_data_sources = AccountRegionScopedDict()            # f"{kb_id}/{ds_id}" -> DataSource
_ingestion_jobs = AccountRegionScopedDict()          # f"{kb_id}/{ds_id}/{job_id}" -> IngestionJob
_kb_documents = AccountRegionScopedDict()            # f"{kb_id}/{ds_id}/{doc_id}" -> Doc
_flows = AccountRegionScopedDict()                   # flow_id -> Flow
_flow_versions = AccountRegionScopedDict()           # f"{flow_id}/{version}" -> FlowVersion
_flow_aliases = AccountRegionScopedDict()            # f"{flow_id}/{alias_id}" -> FlowAlias
_prompts = AccountRegionScopedDict()                 # prompt_id -> Prompt
_prompt_versions = AccountRegionScopedDict()         # f"{prompt_id}/{version}" -> PromptVersion
_tags = AccountRegionScopedDict()                    # arn -> {key: value}


_ALL_STORES = [_agents, _agent_versions, _agent_aliases, _agent_action_groups,
                _agent_collaborators, _agent_knowledge_bases, _knowledge_bases,
                _data_sources, _ingestion_jobs, _kb_documents, _flows,
                _flow_versions, _flow_aliases, _prompts, _prompt_versions, _tags]


def reset():
    for s in _ALL_STORES:
        s.clear()


def get_state():
    return copy.deepcopy({
        "agents": _agents, "agent_versions": _agent_versions,
        "agent_aliases": _agent_aliases, "agent_action_groups": _agent_action_groups,
        "agent_collaborators": _agent_collaborators,
        "agent_knowledge_bases": _agent_knowledge_bases,
        "knowledge_bases": _knowledge_bases, "data_sources": _data_sources,
        "ingestion_jobs": _ingestion_jobs, "kb_documents": _kb_documents,
        "flows": _flows, "flow_versions": _flow_versions,
        "flow_aliases": _flow_aliases, "prompts": _prompts,
        "prompt_versions": _prompt_versions, "tags": _tags,
    })


def restore_state(data):
    if not data:
        return
    _agents.update(data.get("agents", {}))
    _agent_versions.update(data.get("agent_versions", {}))
    _agent_aliases.update(data.get("agent_aliases", {}))
    _agent_action_groups.update(data.get("agent_action_groups", {}))
    _agent_collaborators.update(data.get("agent_collaborators", {}))
    _agent_knowledge_bases.update(data.get("agent_knowledge_bases", {}))
    _knowledge_bases.update(data.get("knowledge_bases", {}))
    _data_sources.update(data.get("data_sources", {}))
    _ingestion_jobs.update(data.get("ingestion_jobs", {}))
    _kb_documents.update(data.get("kb_documents", {}))
    _flows.update(data.get("flows", {}))
    _flow_versions.update(data.get("flow_versions", {}))
    _flow_aliases.update(data.get("flow_aliases", {}))
    _prompts.update(data.get("prompts", {}))
    _prompt_versions.update(data.get("prompt_versions", {}))
    _tags.update(data.get("tags", {}))


try:
    _restored = load_state("bedrock_agent")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore bedrock_agent state; continuing fresh")


# ===========================================================================
# Agent
# ===========================================================================


def _agent_arn(agent_id: str) -> str:
    return _arn("agent", agent_id)


def _create_agent(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    name = body_obj.get("agentName")
    if not name:
        return _validation("agentName is required.")
    for r in _agents.values():
        if r["AgentName"] == name:
            return _conflict(f"Agent {name} already exists.")
    aid = _id("AG")
    now = _now_iso()
    rec = {
        "AgentArn": _agent_arn(aid),
        "AgentId": aid,
        "AgentName": name,
        "AgentResourceRoleArn": body_obj.get("agentResourceRoleArn", ""),
        "AgentStatus": "NOT_PREPARED",
        "AgentVersion": "DRAFT",
        "ClientToken": body_obj.get("clientToken", ""),
        "CreatedAt": now,
        "Description": body_obj.get("description", ""),
        "FoundationModel": body_obj.get("foundationModel", ""),
        "IdleSessionTTLInSeconds": body_obj.get("idleSessionTTLInSeconds", 600),
        "Instruction": body_obj.get("instruction", ""),
        "UpdatedAt": now,
        "AgentCollaboration": body_obj.get("agentCollaboration"),
        "CustomerEncryptionKeyArn": body_obj.get("customerEncryptionKeyArn"),
        "GuardrailConfiguration": body_obj.get("guardrailConfiguration"),
        "MemoryConfiguration": body_obj.get("memoryConfiguration"),
        "PromptOverrideConfiguration": body_obj.get("promptOverrideConfiguration"),
    }
    _agents[aid] = rec
    if body_obj.get("tags"):
        _tags[rec["AgentArn"]] = dict(body_obj["tags"])
    return _json({"Agent": rec}, status=202)


def _get_agent(agent_id: str) -> tuple:
    rec = _agents.get(agent_id)
    if rec is None:
        return _not_found(f"Agent {agent_id} not found.")
    return _json({"Agent": rec})


def _list_agents(body) -> tuple:
    summaries = []
    for r in _agents.values():
        summaries.append({
            "AgentId": r["AgentId"],
            "AgentName": r["AgentName"],
            "AgentStatus": r["AgentStatus"],
            "Description": r["Description"],
            "LatestAgentVersion": "DRAFT",
            "UpdatedAt": r["UpdatedAt"],
        })
    return _json({"AgentSummaries": summaries})


def _update_agent(agent_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _agents.get(agent_id)
    if rec is None:
        return _not_found(f"Agent {agent_id} not found.")
    for k in ("agentName", "agentResourceRoleArn", "description", "foundationModel",
              "instruction", "idleSessionTTLInSeconds", "agentCollaboration",
              "customerEncryptionKeyArn", "guardrailConfiguration",
              "memoryConfiguration", "promptOverrideConfiguration"):
        if k in body_obj:
            rec[k[0].upper() + k[1:]] = body_obj[k]
    rec["UpdatedAt"] = _now_iso()
    return _json({"Agent": rec}, status=202)


def _delete_agent(agent_id: str, query_params) -> tuple:
    if agent_id not in _agents:
        return _not_found(f"Agent {agent_id} not found.")
    del _agents[agent_id]
    return _json({"AgentId": agent_id, "AgentStatus": "DELETING"}, status=202)


def _prepare_agent(agent_id: str) -> tuple:
    rec = _agents.get(agent_id)
    if rec is None:
        return _not_found(f"Agent {agent_id} not found.")
    rec["AgentStatus"] = "PREPARED"
    rec["UpdatedAt"] = _now_iso()
    return _json({
        "AgentId": agent_id,
        "AgentStatus": "PREPARED",
        "AgentVersion": "DRAFT",
        "PreparedAt": _now_iso(),
    }, status=202)


# ===========================================================================
# Agent version
# ===========================================================================


def _get_agent_version(agent_id: str, version: str) -> tuple:
    rec = _agent_versions.get(f"{agent_id}/{version}")
    if rec is None:
        return _not_found(f"Agent {agent_id} version {version} not found.")
    return _json({"AgentVersion": rec})


def _list_agent_versions(agent_id: str, body) -> tuple:
    summaries = []
    for key, r in _agent_versions.items():
        aid, ver = key.split("/", 1)
        if aid != agent_id:
            continue
        summaries.append({
            "AgentName": r["AgentName"],
            "AgentStatus": r["AgentStatus"],
            "AgentVersion": ver,
            "CreatedAt": r["CreatedAt"],
            "Description": r["Description"],
            "UpdatedAt": r["UpdatedAt"],
        })
    return _json({"AgentVersionSummaries": summaries})


def _delete_agent_version(agent_id: str, version: str, query_params) -> tuple:
    key = f"{agent_id}/{version}"
    if key not in _agent_versions:
        return _not_found(f"Agent {agent_id} version {version} not found.")
    del _agent_versions[key]
    return _json({"AgentId": agent_id, "AgentVersion": version,
                   "AgentStatus": "DELETING"}, status=202)


# ===========================================================================
# Agent alias
# ===========================================================================


def _agent_alias_arn(agent_id: str, alias_id: str) -> str:
    return _arn("agent-alias", f"{agent_id}/{alias_id}")


def _create_agent_alias(agent_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if agent_id not in _agents:
        return _not_found(f"Agent {agent_id} not found.")
    name = body_obj.get("agentAliasName")
    if not name:
        return _validation("agentAliasName is required.")
    alias_id = _id("AL")
    now = _now_iso()
    rec = {
        "AgentAliasArn": _agent_alias_arn(agent_id, alias_id),
        "AgentAliasId": alias_id,
        "AgentAliasName": name,
        "AgentAliasStatus": "PREPARED",
        "AgentId": agent_id,
        "CreatedAt": now,
        "Description": body_obj.get("description", ""),
        "RoutingConfiguration": body_obj.get("routingConfiguration", []),
        "UpdatedAt": now,
        "ClientToken": body_obj.get("clientToken", ""),
    }
    _agent_aliases[f"{agent_id}/{alias_id}"] = rec
    return _json({"AgentAlias": rec}, status=202)


def _get_agent_alias(agent_id: str, alias_id: str) -> tuple:
    rec = _agent_aliases.get(f"{agent_id}/{alias_id}")
    if rec is None:
        return _not_found(f"Alias {alias_id} not found.")
    return _json({"AgentAlias": rec})


def _list_agent_aliases(agent_id: str, body) -> tuple:
    summaries = []
    for key, r in _agent_aliases.items():
        if not key.startswith(f"{agent_id}/"):
            continue
        summaries.append({
            "AgentAliasId": r["AgentAliasId"],
            "AgentAliasName": r["AgentAliasName"],
            "AgentAliasStatus": r["AgentAliasStatus"],
            "CreatedAt": r["CreatedAt"],
            "Description": r["Description"],
            "RoutingConfiguration": r["RoutingConfiguration"],
            "UpdatedAt": r["UpdatedAt"],
        })
    return _json({"AgentAliasSummaries": summaries})


def _update_agent_alias(agent_id: str, alias_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    key = f"{agent_id}/{alias_id}"
    rec = _agent_aliases.get(key)
    if rec is None:
        return _not_found(f"Alias {alias_id} not found.")
    if "agentAliasName" in body_obj:
        rec["AgentAliasName"] = body_obj["agentAliasName"]
    if "description" in body_obj:
        rec["Description"] = body_obj["description"]
    if "routingConfiguration" in body_obj:
        rec["RoutingConfiguration"] = body_obj["routingConfiguration"]
    rec["UpdatedAt"] = _now_iso()
    return _json({"AgentAlias": rec}, status=202)


def _delete_agent_alias(agent_id: str, alias_id: str) -> tuple:
    key = f"{agent_id}/{alias_id}"
    if key not in _agent_aliases:
        return _not_found(f"Alias {alias_id} not found.")
    del _agent_aliases[key]
    return _json({"AgentId": agent_id, "AgentAliasId": alias_id,
                   "AgentAliasStatus": "DELETING"}, status=202)


# ===========================================================================
# Agent action group
# ===========================================================================


def _create_agent_action_group(agent_id: str, version: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    name = body_obj.get("actionGroupName")
    if not name:
        return _validation("actionGroupName is required.")
    ag_id = _id("AG")
    now = _now_iso()
    rec = {
        "ActionGroupId": ag_id,
        "ActionGroupName": name,
        "ActionGroupState": body_obj.get("actionGroupState", "ENABLED"),
        "ActionGroupExecutor": body_obj.get("actionGroupExecutor"),
        "AgentId": agent_id,
        "AgentVersion": version,
        "ApiSchema": body_obj.get("apiSchema"),
        "ClientToken": body_obj.get("clientToken", ""),
        "CreatedAt": now,
        "Description": body_obj.get("description", ""),
        "FunctionSchema": body_obj.get("functionSchema"),
        "ParentActionSignature": body_obj.get("parentActionGroupSignature"),
        "UpdatedAt": now,
    }
    _agent_action_groups[f"{agent_id}/{version}/{ag_id}"] = rec
    return _json({"AgentActionGroup": rec})


def _get_agent_action_group(agent_id: str, version: str, ag_id: str) -> tuple:
    rec = _agent_action_groups.get(f"{agent_id}/{version}/{ag_id}")
    if rec is None:
        return _not_found(f"Action group {ag_id} not found.")
    return _json({"AgentActionGroup": rec})


def _list_agent_action_groups(agent_id: str, version: str, body) -> tuple:
    summaries = []
    for key, r in _agent_action_groups.items():
        parts = key.split("/")
        if len(parts) >= 3 and parts[0] == agent_id and parts[1] == version:
            summaries.append({
                "ActionGroupId": r["ActionGroupId"],
                "ActionGroupName": r["ActionGroupName"],
                "ActionGroupState": r["ActionGroupState"],
                "Description": r["Description"],
                "UpdatedAt": r["UpdatedAt"],
            })
    return _json({"ActionGroupSummaries": summaries})


def _update_agent_action_group(agent_id: str, version: str, ag_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    key = f"{agent_id}/{version}/{ag_id}"
    rec = _agent_action_groups.get(key)
    if rec is None:
        return _not_found(f"Action group {ag_id} not found.")
    for k in ("actionGroupName", "actionGroupState", "actionGroupExecutor",
              "apiSchema", "description", "functionSchema",
              "parentActionGroupSignature"):
        if k in body_obj:
            internal_key = "ParentActionSignature" if k == "parentActionGroupSignature" else (k[0].upper() + k[1:])
            rec[internal_key] = body_obj[k]
    rec["UpdatedAt"] = _now_iso()
    return _json({"AgentActionGroup": rec})


def _delete_agent_action_group(agent_id: str, version: str, ag_id: str, query_params) -> tuple:
    key = f"{agent_id}/{version}/{ag_id}"
    if key not in _agent_action_groups:
        return _not_found(f"Action group {ag_id} not found.")
    del _agent_action_groups[key]
    return _empty(status=204)


# ===========================================================================
# Agent collaborator
# ===========================================================================


def _associate_agent_collaborator(agent_id: str, version: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    c_id = _id("CO")
    now = _now_iso()
    rec = {
        "AgentDescriptor": body_obj.get("agentDescriptor", {}),
        "AgentId": agent_id,
        "AgentVersion": version,
        "ClientToken": body_obj.get("clientToken", ""),
        "CollaborationInstruction": body_obj.get("collaborationInstruction", ""),
        "CollaboratorId": c_id,
        "CollaboratorName": body_obj.get("collaboratorName", ""),
        "CreatedAt": now,
        "LastUpdatedAt": now,
        "RelayConversationHistory": body_obj.get("relayConversationHistory", "DISABLED"),
    }
    _agent_collaborators[f"{agent_id}/{version}/{c_id}"] = rec
    return _json({"AgentCollaborator": rec})


def _get_agent_collaborator(agent_id: str, version: str, c_id: str) -> tuple:
    rec = _agent_collaborators.get(f"{agent_id}/{version}/{c_id}")
    if rec is None:
        return _not_found(f"Collaborator {c_id} not found.")
    return _json({"AgentCollaborator": rec})


def _list_agent_collaborators(agent_id: str, version: str, body) -> tuple:
    summaries = []
    for key, r in _agent_collaborators.items():
        parts = key.split("/")
        if len(parts) >= 3 and parts[0] == agent_id and parts[1] == version:
            summaries.append(r)
    return _json({"AgentCollaboratorSummaries": summaries})


def _update_agent_collaborator(agent_id: str, version: str, c_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _agent_collaborators.get(f"{agent_id}/{version}/{c_id}")
    if rec is None:
        return _not_found(f"Collaborator {c_id} not found.")
    for k in ("agentDescriptor", "collaborationInstruction", "collaboratorName",
              "relayConversationHistory"):
        if k in body_obj:
            rec[k[0].upper() + k[1:]] = body_obj[k]
    rec["LastUpdatedAt"] = _now_iso()
    return _json({"AgentCollaborator": rec})


def _disassociate_agent_collaborator(agent_id: str, version: str, c_id: str) -> tuple:
    key = f"{agent_id}/{version}/{c_id}"
    if key not in _agent_collaborators:
        return _not_found(f"Collaborator {c_id} not found.")
    del _agent_collaborators[key]
    return _empty(status=204)


# ===========================================================================
# Agent knowledge base
# ===========================================================================


def _associate_agent_kb(agent_id: str, version: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    kb_id = body_obj.get("knowledgeBaseId")
    if not kb_id:
        return _validation("knowledgeBaseId is required.")
    now = _now_iso()
    rec = {
        "AgentId": agent_id,
        "AgentVersion": version,
        "KnowledgeBaseId": kb_id,
        "Description": body_obj.get("description", ""),
        "KnowledgeBaseState": body_obj.get("knowledgeBaseState", "ENABLED"),
        "CreatedAt": now,
        "UpdatedAt": now,
    }
    _agent_knowledge_bases[f"{agent_id}/{version}/{kb_id}"] = rec
    return _json({"AgentKnowledgeBase": rec})


def _get_agent_kb(agent_id: str, version: str, kb_id: str) -> tuple:
    rec = _agent_knowledge_bases.get(f"{agent_id}/{version}/{kb_id}")
    if rec is None:
        return _not_found(f"Agent KB association {kb_id} not found.")
    return _json({"AgentKnowledgeBase": rec})


def _list_agent_kbs(agent_id: str, version: str, body) -> tuple:
    summaries = []
    for key, r in _agent_knowledge_bases.items():
        parts = key.split("/")
        if len(parts) >= 3 and parts[0] == agent_id and parts[1] == version:
            summaries.append({
                "KnowledgeBaseId": r["KnowledgeBaseId"],
                "Description": r["Description"],
                "KnowledgeBaseState": r["KnowledgeBaseState"],
                "UpdatedAt": r["UpdatedAt"],
            })
    return _json({"AgentKnowledgeBaseSummaries": summaries})


def _update_agent_kb(agent_id: str, version: str, kb_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _agent_knowledge_bases.get(f"{agent_id}/{version}/{kb_id}")
    if rec is None:
        return _not_found(f"Agent KB association {kb_id} not found.")
    if "description" in body_obj:
        rec["Description"] = body_obj["description"]
    if "knowledgeBaseState" in body_obj:
        rec["KnowledgeBaseState"] = body_obj["knowledgeBaseState"]
    rec["UpdatedAt"] = _now_iso()
    return _json({"AgentKnowledgeBase": rec})


def _disassociate_agent_kb(agent_id: str, version: str, kb_id: str) -> tuple:
    key = f"{agent_id}/{version}/{kb_id}"
    if key not in _agent_knowledge_bases:
        return _not_found(f"Agent KB association {kb_id} not found.")
    del _agent_knowledge_bases[key]
    return _empty(status=204)


# ===========================================================================
# Knowledge base
# ===========================================================================


def _kb_arn(kb_id: str) -> str:
    return _arn("knowledge-base", kb_id)


def _create_kb(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("name"):
        return _validation("name is required.")
    if not body_obj.get("roleArn"):
        return _validation("roleArn is required.")
    if not body_obj.get("knowledgeBaseConfiguration"):
        return _validation("knowledgeBaseConfiguration is required.")
    kb_id = _id("KB")
    now = _now_iso()
    rec = {
        "CreatedAt": now,
        "KnowledgeBaseArn": _kb_arn(kb_id),
        "KnowledgeBaseConfiguration": body_obj["knowledgeBaseConfiguration"],
        "KnowledgeBaseId": kb_id,
        "Name": body_obj["name"],
        "RoleArn": body_obj["roleArn"],
        "Status": "ACTIVE",
        "UpdatedAt": now,
        "Description": body_obj.get("description", ""),
        "StorageConfiguration": body_obj.get("storageConfiguration"),
    }
    _knowledge_bases[kb_id] = rec
    if body_obj.get("tags"):
        _tags[rec["KnowledgeBaseArn"]] = dict(body_obj["tags"])
    return _json({"KnowledgeBase": rec}, status=202)


def _get_kb(kb_id: str) -> tuple:
    rec = _knowledge_bases.get(kb_id)
    if rec is None:
        return _not_found(f"Knowledge base {kb_id} not found.")
    return _json({"KnowledgeBase": rec})


def _list_kbs(body) -> tuple:
    summaries = []
    for r in _knowledge_bases.values():
        summaries.append({
            "KnowledgeBaseId": r["KnowledgeBaseId"],
            "Name": r["Name"],
            "Status": r["Status"],
            "Description": r["Description"],
            "UpdatedAt": r["UpdatedAt"],
        })
    return _json({"KnowledgeBaseSummaries": summaries})


def _update_kb(kb_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _knowledge_bases.get(kb_id)
    if rec is None:
        return _not_found(f"Knowledge base {kb_id} not found.")
    for k in ("name", "description", "roleArn", "knowledgeBaseConfiguration",
              "storageConfiguration"):
        if k in body_obj:
            rec[k[0].upper() + k[1:]] = body_obj[k]
    rec["UpdatedAt"] = _now_iso()
    return _json({"KnowledgeBase": rec}, status=202)


def _delete_kb(kb_id: str) -> tuple:
    if kb_id not in _knowledge_bases:
        return _not_found(f"Knowledge base {kb_id} not found.")
    del _knowledge_bases[kb_id]
    return _json({"KnowledgeBaseId": kb_id, "Status": "DELETING"}, status=202)


# ===========================================================================
# Data source
# ===========================================================================


def _create_ds(kb_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if kb_id not in _knowledge_bases:
        return _not_found(f"Knowledge base {kb_id} not found.")
    if not body_obj.get("name"):
        return _validation("name is required.")
    if not body_obj.get("dataSourceConfiguration"):
        return _validation("dataSourceConfiguration is required.")
    ds_id = _id("DS")
    now = _now_iso()
    rec = {
        "CreatedAt": now,
        "DataSourceConfiguration": body_obj["dataSourceConfiguration"],
        "DataSourceId": ds_id,
        "KnowledgeBaseId": kb_id,
        "Name": body_obj["name"],
        "Status": "AVAILABLE",
        "UpdatedAt": now,
        "Description": body_obj.get("description", ""),
        "DataDeletionPolicy": body_obj.get("dataDeletionPolicy", "RETAIN"),
        "ServerSideEncryptionConfiguration": body_obj.get("serverSideEncryptionConfiguration"),
        "VectorIngestionConfiguration": body_obj.get("vectorIngestionConfiguration"),
    }
    _data_sources[f"{kb_id}/{ds_id}"] = rec
    return _json({"DataSource": rec})


def _get_ds(kb_id: str, ds_id: str) -> tuple:
    rec = _data_sources.get(f"{kb_id}/{ds_id}")
    if rec is None:
        return _not_found(f"Data source {ds_id} not found.")
    return _json({"DataSource": rec})


def _list_ds(kb_id: str, body) -> tuple:
    if kb_id not in _knowledge_bases:
        return _not_found(f"Knowledge base {kb_id} not found.")
    summaries = []
    for key, r in _data_sources.items():
        if key.startswith(f"{kb_id}/"):
            summaries.append({
                "DataSourceId": r["DataSourceId"],
                "KnowledgeBaseId": r["KnowledgeBaseId"],
                "Name": r["Name"],
                "Status": r["Status"],
                "Description": r["Description"],
                "UpdatedAt": r["UpdatedAt"],
            })
    return _json({"DataSourceSummaries": summaries})


def _update_ds(kb_id: str, ds_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    key = f"{kb_id}/{ds_id}"
    rec = _data_sources.get(key)
    if rec is None:
        return _not_found(f"Data source {ds_id} not found.")
    for k in ("name", "description", "dataSourceConfiguration",
              "dataDeletionPolicy", "serverSideEncryptionConfiguration",
              "vectorIngestionConfiguration"):
        if k in body_obj:
            rec[k[0].upper() + k[1:]] = body_obj[k]
    rec["UpdatedAt"] = _now_iso()
    return _json({"DataSource": rec})


def _delete_ds(kb_id: str, ds_id: str) -> tuple:
    key = f"{kb_id}/{ds_id}"
    if key not in _data_sources:
        return _not_found(f"Data source {ds_id} not found.")
    del _data_sources[key]
    return _json({"KnowledgeBaseId": kb_id, "DataSourceId": ds_id,
                   "Status": "DELETING"}, status=202)


# ===========================================================================
# Ingestion jobs + KB documents
# ===========================================================================


def _start_ingestion_job(kb_id: str, ds_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if f"{kb_id}/{ds_id}" not in _data_sources:
        return _not_found(f"Data source {ds_id} not found.")
    job_id = _id("IJ")
    now = _now_iso()
    rec = {
        "DataSourceId": ds_id,
        "KnowledgeBaseId": kb_id,
        "IngestionJobId": job_id,
        "Status": "COMPLETE",
        "StartedAt": now,
        "UpdatedAt": now,
        "Description": body_obj.get("description", ""),
        "Statistics": {"NumberOfDocumentsScanned": 0,
                        "NumberOfNewDocumentsIndexed": 0,
                        "NumberOfModifiedDocumentsIndexed": 0,
                        "NumberOfDocumentsDeleted": 0,
                        "NumberOfDocumentsFailed": 0,
                        "NumberOfMetadataDocumentsScanned": 0,
                        "NumberOfMetadataDocumentsModified": 0},
        "FailureReasons": [],
    }
    _ingestion_jobs[f"{kb_id}/{ds_id}/{job_id}"] = rec
    return _json({"IngestionJob": rec}, status=202)


def _get_ingestion_job(kb_id: str, ds_id: str, job_id: str) -> tuple:
    rec = _ingestion_jobs.get(f"{kb_id}/{ds_id}/{job_id}")
    if rec is None:
        return _not_found(f"Ingestion job {job_id} not found.")
    return _json({"IngestionJob": rec})


def _list_ingestion_jobs(kb_id: str, ds_id: str, body) -> tuple:
    summaries = []
    for key, r in _ingestion_jobs.items():
        parts = key.split("/")
        if len(parts) >= 3 and parts[0] == kb_id and parts[1] == ds_id:
            summaries.append(r)
    return _json({"IngestionJobSummaries": summaries})


def _stop_ingestion_job(kb_id: str, ds_id: str, job_id: str) -> tuple:
    rec = _ingestion_jobs.get(f"{kb_id}/{ds_id}/{job_id}")
    if rec is None:
        return _not_found(f"Ingestion job {job_id} not found.")
    rec["Status"] = "STOPPED"
    rec["UpdatedAt"] = _now_iso()
    return _json({"IngestionJob": rec}, status=202)


def _ingest_kb_documents(kb_id: str, ds_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    docs = body_obj.get("documents", [])
    results = []
    for d in docs:
        doc_id = (d.get("content", {}).get("custom") or {}).get("customDocumentIdentifier", {}).get("id") or uuid.uuid4().hex
        _kb_documents[f"{kb_id}/{ds_id}/{doc_id}"] = {
            "DocumentIdentifier": d.get("content", {}).get("custom", {}).get("customDocumentIdentifier", {}),
            "Status": "INDEXED",
            "StatusReason": "",
            "UpdatedAt": _now_iso(),
        }
        results.append(_kb_documents[f"{kb_id}/{ds_id}/{doc_id}"])
    return _json({"DocumentDetails": results}, status=202)


def _get_kb_documents(kb_id: str, ds_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    ids = body_obj.get("documentIdentifiers", [])
    details = []
    for d in ids:
        doc_id = d.get("custom", {}).get("id") or d.get("s3", {}).get("uri", "")
        key = f"{kb_id}/{ds_id}/{doc_id}"
        if key in _kb_documents:
            details.append(_kb_documents[key])
    return _json({"DocumentDetails": details})


def _list_kb_documents(kb_id: str, ds_id: str, body) -> tuple:
    details = []
    for key, r in _kb_documents.items():
        parts = key.split("/")
        if len(parts) >= 3 and parts[0] == kb_id and parts[1] == ds_id:
            details.append(r)
    return _json({"DocumentDetails": details})


def _delete_kb_documents(kb_id: str, ds_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    ids = body_obj.get("documentIdentifiers", [])
    details = []
    for d in ids:
        doc_id = d.get("custom", {}).get("id") or d.get("s3", {}).get("uri", "")
        key = f"{kb_id}/{ds_id}/{doc_id}"
        if key in _kb_documents:
            r = _kb_documents.pop(key)
            r["Status"] = "DELETING"
            details.append(r)
    return _json({"DocumentDetails": details}, status=202)


# ===========================================================================
# Flow + FlowVersion + FlowAlias + ValidateFlowDefinition
# ===========================================================================


def _flow_arn(flow_id: str) -> str:
    return _arn("flow", flow_id)


def _create_flow(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("name"):
        return _validation("name is required.")
    if not body_obj.get("executionRoleArn"):
        return _validation("executionRoleArn is required.")
    flow_id = _id("FL")
    now = _now_iso()
    rec = {
        "Arn": _flow_arn(flow_id),
        "CreatedAt": now,
        "ExecutionRoleArn": body_obj["executionRoleArn"],
        "Id": flow_id,
        "Name": body_obj["name"],
        "Status": "NotPrepared",
        "UpdatedAt": now,
        "Version": "DRAFT",
        "Definition": body_obj.get("definition"),
        "Description": body_obj.get("description", ""),
        "CustomerEncryptionKeyArn": body_obj.get("customerEncryptionKeyArn"),
    }
    _flows[flow_id] = rec
    if body_obj.get("tags"):
        _tags[rec["Arn"]] = dict(body_obj["tags"])
    return _json(rec, status=201)


def _get_flow(flow_id: str) -> tuple:
    rec = _flows.get(flow_id)
    if rec is None:
        return _not_found(f"Flow {flow_id} not found.")
    return _json(rec)


def _list_flows(query_params) -> tuple:
    summaries = []
    for r in _flows.values():
        summaries.append({
            "Arn": r["Arn"],
            "CreatedAt": r["CreatedAt"],
            "Id": r["Id"],
            "Name": r["Name"],
            "Status": r["Status"],
            "UpdatedAt": r["UpdatedAt"],
            "Version": r["Version"],
            "Description": r["Description"],
        })
    return _json({"FlowSummaries": summaries})


def _update_flow(flow_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _flows.get(flow_id)
    if rec is None:
        return _not_found(f"Flow {flow_id} not found.")
    for k in ("name", "description", "definition", "executionRoleArn",
              "customerEncryptionKeyArn"):
        if k in body_obj:
            rec[k[0].upper() + k[1:]] = body_obj[k]
    rec["UpdatedAt"] = _now_iso()
    return _json(rec)


def _delete_flow(flow_id: str, query_params) -> tuple:
    if flow_id not in _flows:
        return _not_found(f"Flow {flow_id} not found.")
    del _flows[flow_id]
    return _json({"Id": flow_id})


def _prepare_flow(flow_id: str) -> tuple:
    rec = _flows.get(flow_id)
    if rec is None:
        return _not_found(f"Flow {flow_id} not found.")
    rec["Status"] = "Prepared"
    rec["UpdatedAt"] = _now_iso()
    return _json({"Id": flow_id, "Status": "Prepared"}, status=202)


def _validate_flow_definition(body) -> tuple:
    return _json({"Validations": []})


def _create_flow_version(flow_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if flow_id not in _flows:
        return _not_found(f"Flow {flow_id} not found.")
    existing = [int(k.split("/")[1]) for k in _flow_versions.keys()
                if k.startswith(f"{flow_id}/") and k.split("/")[1].isdigit()]
    next_ver = str(max(existing) + 1 if existing else 1)
    rec = dict(_flows[flow_id])
    rec["Version"] = next_ver
    rec["CreatedAt"] = _now_iso()
    _flow_versions[f"{flow_id}/{next_ver}"] = rec
    return _json(rec, status=201)


def _get_flow_version(flow_id: str, version: str) -> tuple:
    rec = _flow_versions.get(f"{flow_id}/{version}")
    if rec is None:
        return _not_found(f"Flow version {version} not found.")
    return _json(rec)


def _list_flow_versions(flow_id: str, query_params) -> tuple:
    summaries = []
    for key, r in _flow_versions.items():
        if key.startswith(f"{flow_id}/"):
            summaries.append({
                "Arn": r["Arn"], "CreatedAt": r["CreatedAt"], "Id": r["Id"],
                "Status": r["Status"], "Version": r["Version"],
            })
    return _json({"FlowVersionSummaries": summaries})


def _delete_flow_version(flow_id: str, version: str, query_params) -> tuple:
    key = f"{flow_id}/{version}"
    if key not in _flow_versions:
        return _not_found(f"Flow version {version} not found.")
    del _flow_versions[key]
    return _json({"Id": flow_id, "Version": version})


def _flow_alias_arn(flow_id: str, alias_id: str) -> str:
    return _arn("flow-alias", f"{flow_id}/{alias_id}")


def _create_flow_alias(flow_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("name"):
        return _validation("name is required.")
    if not body_obj.get("routingConfiguration"):
        return _validation("routingConfiguration is required.")
    alias_id = _id("FA")
    now = _now_iso()
    rec = {
        "Arn": _flow_alias_arn(flow_id, alias_id),
        "CreatedAt": now,
        "FlowId": flow_id,
        "Id": alias_id,
        "Name": body_obj["name"],
        "RoutingConfiguration": body_obj["routingConfiguration"],
        "UpdatedAt": now,
        "Description": body_obj.get("description", ""),
    }
    _flow_aliases[f"{flow_id}/{alias_id}"] = rec
    return _json(rec, status=201)


def _get_flow_alias(flow_id: str, alias_id: str) -> tuple:
    rec = _flow_aliases.get(f"{flow_id}/{alias_id}")
    if rec is None:
        return _not_found(f"Flow alias {alias_id} not found.")
    return _json(rec)


def _list_flow_aliases(flow_id: str, query_params) -> tuple:
    summaries = []
    for key, r in _flow_aliases.items():
        if key.startswith(f"{flow_id}/"):
            summaries.append(r)
    return _json({"FlowAliasSummaries": summaries})


def _update_flow_alias(flow_id: str, alias_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    key = f"{flow_id}/{alias_id}"
    rec = _flow_aliases.get(key)
    if rec is None:
        return _not_found(f"Flow alias {alias_id} not found.")
    for k in ("name", "description", "routingConfiguration"):
        if k in body_obj:
            rec[k[0].upper() + k[1:]] = body_obj[k]
    rec["UpdatedAt"] = _now_iso()
    return _json(rec)


def _delete_flow_alias(flow_id: str, alias_id: str) -> tuple:
    key = f"{flow_id}/{alias_id}"
    if key not in _flow_aliases:
        return _not_found(f"Flow alias {alias_id} not found.")
    del _flow_aliases[key]
    return _json({"FlowId": flow_id, "Id": alias_id})


# ===========================================================================
# Prompt + PromptVersion
# ===========================================================================


def _prompt_arn(prompt_id: str) -> str:
    return _arn("prompt", prompt_id)


def _create_prompt(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("name"):
        return _validation("name is required.")
    prompt_id = _id("PR")
    now = _now_iso()
    rec = {
        "Arn": _prompt_arn(prompt_id),
        "CreatedAt": now,
        "Id": prompt_id,
        "Name": body_obj["name"],
        "UpdatedAt": now,
        "Version": "DRAFT",
        "DefaultVariant": body_obj.get("defaultVariant"),
        "Description": body_obj.get("description", ""),
        "Variants": body_obj.get("variants", []),
        "CustomerEncryptionKeyArn": body_obj.get("customerEncryptionKeyArn"),
    }
    _prompts[prompt_id] = rec
    if body_obj.get("tags"):
        _tags[rec["Arn"]] = dict(body_obj["tags"])
    return _json(rec, status=201)


def _get_prompt(prompt_id: str, query_params) -> tuple:
    version = (query_params.get("promptVersion") or [None])[0] if isinstance(query_params, dict) else None
    if version:
        rec = _prompt_versions.get(f"{prompt_id}/{version}")
    else:
        rec = _prompts.get(prompt_id)
    if rec is None:
        return _not_found(f"Prompt {prompt_id} not found.")
    return _json(rec)


def _list_prompts(query_params) -> tuple:
    summaries = []
    for r in _prompts.values():
        summaries.append({
            "Arn": r["Arn"], "CreatedAt": r["CreatedAt"], "Id": r["Id"],
            "Name": r["Name"], "UpdatedAt": r["UpdatedAt"], "Version": r["Version"],
            "Description": r["Description"],
        })
    return _json({"PromptSummaries": summaries})


def _update_prompt(prompt_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _prompts.get(prompt_id)
    if rec is None:
        return _not_found(f"Prompt {prompt_id} not found.")
    for k in ("name", "description", "defaultVariant", "variants",
              "customerEncryptionKeyArn"):
        if k in body_obj:
            rec[k[0].upper() + k[1:]] = body_obj[k]
    rec["UpdatedAt"] = _now_iso()
    return _json(rec)


def _delete_prompt(prompt_id: str, query_params) -> tuple:
    if prompt_id not in _prompts:
        return _not_found(f"Prompt {prompt_id} not found.")
    del _prompts[prompt_id]
    return _json({"Id": prompt_id, "Version": "DRAFT"})


def _create_prompt_version(prompt_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if prompt_id not in _prompts:
        return _not_found(f"Prompt {prompt_id} not found.")
    existing = [int(k.split("/")[1]) for k in _prompt_versions.keys()
                if k.startswith(f"{prompt_id}/") and k.split("/")[1].isdigit()]
    next_ver = str(max(existing) + 1 if existing else 1)
    rec = dict(_prompts[prompt_id])
    rec["Version"] = next_ver
    rec["CreatedAt"] = _now_iso()
    _prompt_versions[f"{prompt_id}/{next_ver}"] = rec
    return _json(rec, status=201)


# ===========================================================================
# Tags
# ===========================================================================


def _tag_resource(arn: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    tags = body_obj.get("tags", {})
    if not isinstance(tags, dict):
        return _validation("tags must be an object.")
    current = dict(_tags.get(arn, {}))
    current.update(tags)
    _tags[arn] = current
    return _empty()


def _untag_resource(arn: str, query_params) -> tuple:
    keys = query_params.get("tagKeys", []) if isinstance(query_params, dict) else []
    if isinstance(keys, str):
        keys = [keys]
    current = dict(_tags.get(arn, {}))
    for k in keys:
        current.pop(k, None)
    _tags[arn] = current
    return _empty()


def _list_tags(arn: str) -> tuple:
    return 200, {"Content-Type": "application/json"}, json.dumps({
        "tags": dict(_tags.get(arn, {})),
    }).encode()


# ===========================================================================
# Dispatcher (72 ops)
# ===========================================================================


_AGENT_RE = re.compile(r"^/agents/([^/]+)/?$")
_AGENT_VERSIONS_RE = re.compile(r"^/agents/([^/]+)/agentversions/?$")
_AGENT_VERSION_RE = re.compile(r"^/agents/([^/]+)/agentversions/([^/]+)/?$")
_AGENT_ALIASES_RE = re.compile(r"^/agents/([^/]+)/agentaliases/?$")
_AGENT_ALIAS_RE = re.compile(r"^/agents/([^/]+)/agentaliases/([^/]+)/?$")
_AG_GROUPS_RE = re.compile(r"^/agents/([^/]+)/agentversions/([^/]+)/actiongroups/?$")
_AG_GROUP_RE = re.compile(r"^/agents/([^/]+)/agentversions/([^/]+)/actiongroups/([^/]+)/?$")
_AG_COLLAB_RE = re.compile(r"^/agents/([^/]+)/agentversions/([^/]+)/agentcollaborators/?$")
_AG_COLLAB_ID_RE = re.compile(r"^/agents/([^/]+)/agentversions/([^/]+)/agentcollaborators/([^/]+)/?$")
_AG_KB_RE = re.compile(r"^/agents/([^/]+)/agentversions/([^/]+)/knowledgebases/?$")
_AG_KB_ID_RE = re.compile(r"^/agents/([^/]+)/agentversions/([^/]+)/knowledgebases/([^/]+)/?$")

_KB_RE = re.compile(r"^/knowledgebases/([^/]+)$")
_DS_LIST_RE = re.compile(r"^/knowledgebases/([^/]+)/datasources/?$")
_DS_RE = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)$")
_IJ_LIST_RE = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/ingestionjobs/?$")
_IJ_RE = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/ingestionjobs/([^/]+)$")
_IJ_STOP_RE = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/ingestionjobs/([^/]+)/stop$")
_KB_DOCS_RE = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/documents$")
_KB_DOCS_GET_RE = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/documents/getDocuments$")
_KB_DOCS_DEL_RE = re.compile(r"^/knowledgebases/([^/]+)/datasources/([^/]+)/documents/deleteDocuments$")

_FLOW_RE = re.compile(r"^/flows/([^/]+)/?$")
_FLOW_ALIAS_LIST_RE = re.compile(r"^/flows/([^/]+)/aliases$")
_FLOW_ALIAS_RE = re.compile(r"^/flows/([^/]+)/aliases/([^/]+)$")
_FLOW_VERSION_LIST_RE = re.compile(r"^/flows/([^/]+)/versions$")
_FLOW_VERSION_RE = re.compile(r"^/flows/([^/]+)/versions/([^/]+)/?$")

_PROMPT_RE = re.compile(r"^/prompts/([^/]+)/?$")
_PROMPT_VERSION_RE = re.compile(r"^/prompts/([^/]+)/versions$")

_TAGS_RE = re.compile(r"^/tags/(.+)$")


async def handle_request(method, path, headers, body, query_params):
    logger.debug("%s %s", method, path)

    # --- Agents ---
    if path == "/agents/":
        if method == "PUT":
            return _create_agent(body)
        if method == "POST":
            return _list_agents(body)
    m = _AGENT_RE.match(path)
    if m and not any(s in path for s in ("/agentversions", "/agentaliases")):
        aid = unquote(m.group(1))
        if method == "GET":
            return _get_agent(aid)
        if method == "PUT":
            return _update_agent(aid, body)
        if method == "DELETE":
            return _delete_agent(aid, query_params)
        if method == "POST":
            return _prepare_agent(aid)

    # --- Agent versions ---
    m = _AGENT_VERSIONS_RE.match(path)
    if m and method == "POST":
        return _list_agent_versions(unquote(m.group(1)), body)
    m = _AGENT_VERSION_RE.match(path)
    if m and not any(s in path for s in ("/actiongroups", "/agentcollaborators", "/knowledgebases")):
        aid, ver = unquote(m.group(1)), unquote(m.group(2))
        if method == "GET":
            return _get_agent_version(aid, ver)
        if method == "DELETE":
            return _delete_agent_version(aid, ver, query_params)

    # --- Agent aliases ---
    m = _AGENT_ALIASES_RE.match(path)
    if m:
        aid = unquote(m.group(1))
        if method == "PUT":
            return _create_agent_alias(aid, body)
        if method == "POST":
            return _list_agent_aliases(aid, body)
    m = _AGENT_ALIAS_RE.match(path)
    if m:
        aid, alid = unquote(m.group(1)), unquote(m.group(2))
        if method == "GET":
            return _get_agent_alias(aid, alid)
        if method == "PUT":
            return _update_agent_alias(aid, alid, body)
        if method == "DELETE":
            return _delete_agent_alias(aid, alid)

    # --- Action groups ---
    m = _AG_GROUPS_RE.match(path)
    if m:
        aid, ver = unquote(m.group(1)), unquote(m.group(2))
        if method == "PUT":
            return _create_agent_action_group(aid, ver, body)
        if method == "POST":
            return _list_agent_action_groups(aid, ver, body)
    m = _AG_GROUP_RE.match(path)
    if m:
        aid, ver, agid = unquote(m.group(1)), unquote(m.group(2)), unquote(m.group(3))
        if method == "GET":
            return _get_agent_action_group(aid, ver, agid)
        if method == "PUT":
            return _update_agent_action_group(aid, ver, agid, body)
        if method == "DELETE":
            return _delete_agent_action_group(aid, ver, agid, query_params)

    # --- Collaborators ---
    m = _AG_COLLAB_RE.match(path)
    if m:
        aid, ver = unquote(m.group(1)), unquote(m.group(2))
        if method == "PUT":
            return _associate_agent_collaborator(aid, ver, body)
        if method == "POST":
            return _list_agent_collaborators(aid, ver, body)
    m = _AG_COLLAB_ID_RE.match(path)
    if m:
        aid, ver, cid = unquote(m.group(1)), unquote(m.group(2)), unquote(m.group(3))
        if method == "GET":
            return _get_agent_collaborator(aid, ver, cid)
        if method == "PUT":
            return _update_agent_collaborator(aid, ver, cid, body)
        if method == "DELETE":
            return _disassociate_agent_collaborator(aid, ver, cid)

    # --- Agent KBs ---
    m = _AG_KB_RE.match(path)
    if m:
        aid, ver = unquote(m.group(1)), unquote(m.group(2))
        if method == "PUT":
            return _associate_agent_kb(aid, ver, body)
        if method == "POST":
            return _list_agent_kbs(aid, ver, body)
    m = _AG_KB_ID_RE.match(path)
    if m:
        aid, ver, kbid = unquote(m.group(1)), unquote(m.group(2)), unquote(m.group(3))
        if method == "GET":
            return _get_agent_kb(aid, ver, kbid)
        if method == "PUT":
            return _update_agent_kb(aid, ver, kbid, body)
        if method == "DELETE":
            return _disassociate_agent_kb(aid, ver, kbid)

    # --- Knowledge bases ---
    if path == "/knowledgebases/":
        if method == "PUT":
            return _create_kb(body)
        if method == "POST":
            return _list_kbs(body)
    m = _KB_RE.match(path)
    if m:
        kbid = unquote(m.group(1))
        if method == "GET":
            return _get_kb(kbid)
        if method == "PUT":
            return _update_kb(kbid, body)
        if method == "DELETE":
            return _delete_kb(kbid)

    # --- Data sources + ingestion jobs + docs ---
    m = _KB_DOCS_GET_RE.match(path)
    if m and method == "POST":
        return _get_kb_documents(unquote(m.group(1)), unquote(m.group(2)), body)
    m = _KB_DOCS_DEL_RE.match(path)
    if m and method == "POST":
        return _delete_kb_documents(unquote(m.group(1)), unquote(m.group(2)), body)
    m = _KB_DOCS_RE.match(path)
    if m:
        kbid, dsid = unquote(m.group(1)), unquote(m.group(2))
        if method == "PUT":
            return _ingest_kb_documents(kbid, dsid, body)
        if method == "POST":
            return _list_kb_documents(kbid, dsid, body)
    m = _IJ_STOP_RE.match(path)
    if m and method == "POST":
        return _stop_ingestion_job(unquote(m.group(1)), unquote(m.group(2)),
                                      unquote(m.group(3)))
    m = _IJ_RE.match(path)
    if m and method == "GET":
        return _get_ingestion_job(unquote(m.group(1)), unquote(m.group(2)),
                                     unquote(m.group(3)))
    m = _IJ_LIST_RE.match(path)
    if m:
        kbid, dsid = unquote(m.group(1)), unquote(m.group(2))
        if method == "PUT":
            return _start_ingestion_job(kbid, dsid, body)
        if method == "POST":
            return _list_ingestion_jobs(kbid, dsid, body)
    m = _DS_LIST_RE.match(path)
    if m:
        kbid = unquote(m.group(1))
        if method == "PUT":
            return _create_ds(kbid, body)
        if method == "POST":
            return _list_ds(kbid, body)
    m = _DS_RE.match(path)
    if m:
        kbid, dsid = unquote(m.group(1)), unquote(m.group(2))
        if method == "GET":
            return _get_ds(kbid, dsid)
        if method == "PUT":
            return _update_ds(kbid, dsid, body)
        if method == "DELETE":
            return _delete_ds(kbid, dsid)

    # --- Flows ---
    if path == "/flows/" and method == "POST":
        return _create_flow(body)
    if path == "/flows/" and method == "GET":
        return _list_flows(query_params)
    if path == "/flows/validate-definition" and method == "POST":
        return _validate_flow_definition(body)
    m = _FLOW_VERSION_RE.match(path)
    if m:
        fid, ver = unquote(m.group(1)), unquote(m.group(2))
        if method == "GET":
            return _get_flow_version(fid, ver)
        if method == "DELETE":
            return _delete_flow_version(fid, ver, query_params)
    m = _FLOW_VERSION_LIST_RE.match(path)
    if m:
        fid = unquote(m.group(1))
        if method == "POST":
            return _create_flow_version(fid, body)
        if method == "GET":
            return _list_flow_versions(fid, query_params)
    m = _FLOW_ALIAS_RE.match(path)
    if m:
        fid, alid = unquote(m.group(1)), unquote(m.group(2))
        if method == "GET":
            return _get_flow_alias(fid, alid)
        if method == "PUT":
            return _update_flow_alias(fid, alid, body)
        if method == "DELETE":
            return _delete_flow_alias(fid, alid)
    m = _FLOW_ALIAS_LIST_RE.match(path)
    if m:
        fid = unquote(m.group(1))
        if method == "POST":
            return _create_flow_alias(fid, body)
        if method == "GET":
            return _list_flow_aliases(fid, query_params)
    m = _FLOW_RE.match(path)
    if m:
        fid = unquote(m.group(1))
        if method == "GET":
            return _get_flow(fid)
        if method == "PUT":
            return _update_flow(fid, body)
        if method == "DELETE":
            return _delete_flow(fid, query_params)
        if method == "POST":
            return _prepare_flow(fid)

    # --- Prompts ---
    if path == "/prompts/" and method == "POST":
        return _create_prompt(body)
    if path == "/prompts/" and method == "GET":
        return _list_prompts(query_params)
    m = _PROMPT_VERSION_RE.match(path)
    if m and method == "POST":
        return _create_prompt_version(unquote(m.group(1)), body)
    m = _PROMPT_RE.match(path)
    if m:
        pid = unquote(m.group(1))
        if method == "GET":
            return _get_prompt(pid, query_params)
        if method == "PUT":
            return _update_prompt(pid, body)
        if method == "DELETE":
            return _delete_prompt(pid, query_params)

    # --- Tags ---
    m = _TAGS_RE.match(path)
    if m:
        arn = unquote(m.group(1))
        if method == "POST":
            return _tag_resource(arn, body)
        if method == "DELETE":
            return _untag_resource(arn, query_params)
        if method == "GET":
            return _list_tags(arn)

    return _validation(f"No route for {method} {path}.")
