"""
Bedrock Control-Plane Service Emulator.
JSON REST API — signing name: bedrock. Endpoint prefix: bedrock.

All 66 operations verified against botocore bedrock-2023-04-20. Status codes,
HTTP methods, and request URIs per service-2.json. Wire format is camelCase
per `locationName`; service stores PascalCase internally and emits camelCase
via the _camelize helper.

Operation families:
  FoundationModel(s)             — catalog (real model IDs)
  InferenceProfile(s)            — system + application-managed cross-region routing
  Guardrail(s) + GuardrailVersion — content moderation policies
  CustomModel(s)                 — fine-tuned base models
  ImportedModel(s)               — user-imported model artifacts
  ProvisionedModelThroughput(s)  — reserved capacity allocations
  ModelCustomizationJob(s)       — fine-tuning training jobs
  ModelImportJob(s)              — model import jobs
  ModelCopyJob(s)                — cross-region model copy
  ModelInvocationJob(s)          — batch inference jobs
  EvaluationJob(s)               — model evaluation jobs
  MarketplaceModelEndpoint(s)    — third-party model endpoints
  PromptRouter(s)                — intelligent prompt routing
  ModelInvocationLoggingConfig   — invocation logging config (singleton per region)
  FoundationModelAgreement       — marketplace EULAs
  UseCaseForModelAccess          — access form
  Tags                           — tagging
"""

import copy
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    get_account_id,
    get_region,
)

logger = logging.getLogger("bedrock")

# ===========================================================================
# Foundation-model catalog (verified IDs against AWS Bedrock model list)
# ===========================================================================

_CATALOG = [
    # Anthropic Claude
    ("anthropic.claude-3-5-sonnet-20241022-v2:0", "Claude 3.5 Sonnet v2", "Anthropic",
     ["TEXT", "IMAGE"], ["TEXT"], True),
    ("anthropic.claude-3-5-sonnet-20240620-v1:0", "Claude 3.5 Sonnet", "Anthropic",
     ["TEXT", "IMAGE"], ["TEXT"], True),
    ("anthropic.claude-3-5-haiku-20241022-v1:0", "Claude 3.5 Haiku", "Anthropic",
     ["TEXT"], ["TEXT"], True),
    ("anthropic.claude-3-opus-20240229-v1:0", "Claude 3 Opus", "Anthropic",
     ["TEXT", "IMAGE"], ["TEXT"], True),
    ("anthropic.claude-3-sonnet-20240229-v1:0", "Claude 3 Sonnet", "Anthropic",
     ["TEXT", "IMAGE"], ["TEXT"], True),
    ("anthropic.claude-3-haiku-20240307-v1:0", "Claude 3 Haiku", "Anthropic",
     ["TEXT", "IMAGE"], ["TEXT"], True),
    # Amazon Nova
    ("amazon.nova-micro-v1:0", "Nova Micro", "Amazon", ["TEXT"], ["TEXT"], True),
    ("amazon.nova-lite-v1:0", "Nova Lite", "Amazon",
     ["TEXT", "IMAGE", "VIDEO"], ["TEXT"], True),
    ("amazon.nova-pro-v1:0", "Nova Pro", "Amazon",
     ["TEXT", "IMAGE", "VIDEO"], ["TEXT"], True),
    ("amazon.nova-premier-v1:0", "Nova Premier", "Amazon",
     ["TEXT", "IMAGE", "VIDEO"], ["TEXT"], True),
    # Amazon Titan
    ("amazon.titan-text-express-v1", "Titan Text G1 - Express", "Amazon",
     ["TEXT"], ["TEXT"], True),
    ("amazon.titan-text-lite-v1", "Titan Text G1 - Lite", "Amazon",
     ["TEXT"], ["TEXT"], True),
    ("amazon.titan-embed-text-v2:0", "Titan Text Embeddings V2", "Amazon",
     ["TEXT"], ["EMBEDDING"], False),
    # Meta Llama
    ("meta.llama3-1-70b-instruct-v1:0", "Llama 3.1 70B Instruct", "Meta",
     ["TEXT"], ["TEXT"], True),
    ("meta.llama3-1-8b-instruct-v1:0", "Llama 3.1 8B Instruct", "Meta",
     ["TEXT"], ["TEXT"], True),
    ("meta.llama3-2-90b-instruct-v1:0", "Llama 3.2 90B Instruct", "Meta",
     ["TEXT", "IMAGE"], ["TEXT"], True),
    ("meta.llama3-2-11b-instruct-v1:0", "Llama 3.2 11B Instruct", "Meta",
     ["TEXT", "IMAGE"], ["TEXT"], True),
    # Mistral
    ("mistral.mistral-large-2407-v1:0", "Mistral Large 2 (24.07)", "Mistral AI",
     ["TEXT"], ["TEXT"], True),
    ("mistral.mistral-small-2402-v1:0", "Mistral Small", "Mistral AI",
     ["TEXT"], ["TEXT"], True),
    # Cohere
    ("cohere.command-r-plus-v1:0", "Command R+", "Cohere", ["TEXT"], ["TEXT"], True),
    ("cohere.command-r-v1:0", "Command R", "Cohere", ["TEXT"], ["TEXT"], True),
    # AI21
    ("ai21.jamba-1-5-large-v1:0", "Jamba 1.5 Large", "AI21 Labs",
     ["TEXT"], ["TEXT"], True),
]

_INFERENCE_PROFILE_PREFIXES = ("us", "eu", "apac")


# ===========================================================================
# Camelize wire-format helper
# ===========================================================================


def _to_camel(key: str) -> str:
    if not key:
        return key
    return key[0].lower() + key[1:]


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
# Errors (per-op error shapes verified against botocore)
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


def _too_many_tags(message: str) -> tuple:
    return _error("TooManyTagsException", message, 400)


def _service_quota_exceeded(message: str) -> tuple:
    return _error("ServiceQuotaExceededException", message, 400)


# ===========================================================================
# Time + ARN + JSON helpers
# ===========================================================================


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _arn(resource_type: str, resource_path: str) -> str:
    return (f"arn:aws:bedrock:{get_region()}:{get_account_id()}:"
            f"{resource_type}/{resource_path}")


def _validate_resource_arn_for_tags(arn: str) -> tuple | None:
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return _validation(f"Invalid resourceARN: {arn}")
    if spec.service != "bedrock":
        return _validation(f"Invalid resourceARN: {arn}")
    if spec.account_id != get_account_id():
        return _not_found(f"Resource {arn} not found.")
    if spec.region != get_region():
        return _not_found(f"Resource {arn} not found.")

    parts = spec.resource.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return _validation(f"Invalid resourceARN: {arn}")

    resource_type, resource_id = parts
    if resource_type == "application-inference-profile":
        profile_id = resource_id
        rec = _inference_profiles_apps.get(profile_id)
        if rec and rec.get("InferenceProfileArn") == arn:
            return None
    elif resource_type == "guardrail":
        guardrail_id = resource_id
        rec = _guardrails.get(guardrail_id)
        if rec and rec.get("GuardrailArn") == arn:
            return None
    elif resource_type == "custom-model":
        if arn in _custom_models:
            return None
    elif resource_type == "imported-model":
        if arn in _imported_models:
            return None
    elif resource_type == "provisioned-model":
        provisioned_id = resource_id
        rec = _provisioned_throughputs.get(provisioned_id)
        if rec and rec.get("ProvisionedModelArn") == arn:
            return None
    elif resource_type == "model-customization-job":
        if arn in _model_customization_jobs:
            return None
    elif resource_type == "model-import-job":
        if arn in _model_import_jobs:
            return None
    elif resource_type == "model-copy-job":
        if arn in _model_copy_jobs:
            return None
    elif resource_type == "model-invocation-job":
        if arn in _model_invocation_jobs:
            return None
    elif resource_type == "evaluation-job":
        if arn in _evaluation_jobs:
            return None
    elif resource_type == "marketplace-model-endpoint":
        if arn in _marketplace_endpoints:
            return None
    elif resource_type == "prompt-router":
        if arn in _prompt_routers:
            return None
    else:
        return _validation(f"Invalid resourceARN: {arn}")

    return _not_found(f"Resource {arn} not found.")


def _parse_body(body) -> tuple:
    """Returns (body_obj, error_tuple). body_obj is None when error fires."""
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
# State containers — each is region-scoped via AccountRegionScopedDict
# ===========================================================================

_guardrails = AccountRegionScopedDict()           # gr_id -> guardrail dict
_guardrail_versions = AccountRegionScopedDict()   # f"{gr_id}/{version}" -> version dict
_custom_models = AccountRegionScopedDict()        # arn -> custom model dict
_imported_models = AccountRegionScopedDict()      # arn -> imported model dict
_inference_profiles_apps = AccountRegionScopedDict()  # id -> profile dict (application-managed)
_provisioned_throughputs = AccountRegionScopedDict()  # id -> provisioned dict
_model_customization_jobs = AccountRegionScopedDict()
_model_import_jobs = AccountRegionScopedDict()
_model_copy_jobs = AccountRegionScopedDict()
_model_invocation_jobs = AccountRegionScopedDict()
_evaluation_jobs = AccountRegionScopedDict()
_marketplace_endpoints = AccountRegionScopedDict()
_prompt_routers = AccountRegionScopedDict()
_invocation_logging_config = AccountRegionScopedDict()  # 'config' -> dict (singleton)
_tags = AccountRegionScopedDict()                  # arn -> {key: value}
# Defined here (with the other state containers) so it exists before the
# import-time restore_state() below — see the "Use Case for Model Access"
# section further down for its accessors.
_USE_CASE = AccountRegionScopedDict()              # 'usecase' -> dict (model-access use case)


def reset():
    for store in (_guardrails, _guardrail_versions, _custom_models,
                   _imported_models, _inference_profiles_apps,
                   _provisioned_throughputs, _model_customization_jobs,
                   _model_import_jobs, _model_copy_jobs,
                   _model_invocation_jobs, _evaluation_jobs,
                   _marketplace_endpoints, _prompt_routers,
                   _invocation_logging_config, _tags, _USE_CASE):
        store.clear()


def get_state():
    return copy.deepcopy({
        "guardrails": _guardrails,
        "guardrail_versions": _guardrail_versions,
        "custom_models": _custom_models,
        "imported_models": _imported_models,
        "inference_profiles_apps": _inference_profiles_apps,
        "provisioned_throughputs": _provisioned_throughputs,
        "model_customization_jobs": _model_customization_jobs,
        "model_import_jobs": _model_import_jobs,
        "model_copy_jobs": _model_copy_jobs,
        "model_invocation_jobs": _model_invocation_jobs,
        "evaluation_jobs": _evaluation_jobs,
        "marketplace_endpoints": _marketplace_endpoints,
        "prompt_routers": _prompt_routers,
        "invocation_logging_config": _invocation_logging_config,
        "tags": _tags,
        "use_case": _USE_CASE,
    })


def restore_state(data):
    if not data:
        return
    _guardrails.update(data.get("guardrails", {}))
    _guardrail_versions.update(data.get("guardrail_versions", {}))
    _custom_models.update(data.get("custom_models", {}))
    _imported_models.update(data.get("imported_models", {}))
    _inference_profiles_apps.update(data.get("inference_profiles_apps", {}))
    _provisioned_throughputs.update(data.get("provisioned_throughputs", {}))
    _model_customization_jobs.update(data.get("model_customization_jobs", {}))
    _model_import_jobs.update(data.get("model_import_jobs", {}))
    _model_copy_jobs.update(data.get("model_copy_jobs", {}))
    _model_invocation_jobs.update(data.get("model_invocation_jobs", {}))
    _evaluation_jobs.update(data.get("evaluation_jobs", {}))
    _marketplace_endpoints.update(data.get("marketplace_endpoints", {}))
    _prompt_routers.update(data.get("prompt_routers", {}))
    _invocation_logging_config.update(data.get("invocation_logging_config", {}))
    _tags.update(data.get("tags", {}))
    _USE_CASE.update(data.get("use_case", {}))


try:
    _restored = load_state("bedrock")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore bedrock state; continuing fresh")


# ===========================================================================
# Foundation models + Inference profiles (system-defined)
# ===========================================================================


def _model_arn(model_id: str) -> str:
    return f"arn:aws:bedrock:{get_region()}::foundation-model/{model_id}"


def _model_summary(model_id, model_name, provider, inputs, outputs, streaming) -> dict:
    return {
        "ModelArn": _model_arn(model_id),
        "ModelId": model_id,
        "ModelName": model_name,
        "ProviderName": provider,
        "InputModalities": list(inputs),
        "OutputModalities": list(outputs),
        "ResponseStreamingSupported": streaming,
        "CustomizationsSupported": [],
        "InferenceTypesSupported": ["ON_DEMAND"],
        "ModelLifecycle": {"Status": "ACTIVE"},
    }


def _foundation_model_id_from_identifier(identifier: str) -> str | None:
    if not isinstance(identifier, str):
        return None
    model_id = identifier
    if identifier.startswith("arn:"):
        try:
            spec = parse_arn(identifier)
        except ArnParseError:
            return None
        if (
            spec.partition != "aws"
            or spec.service != "bedrock"
            or spec.region != get_region()
            or spec.account_id != ""
        ):
            return None
        prefix = "foundation-model/"
        if not spec.resource.startswith(prefix):
            return None
        model_id = spec.resource[len(prefix):]
        if not model_id or "/" in model_id:
            return None
    for prefix in _INFERENCE_PROFILE_PREFIXES:
        if model_id.startswith(f"{prefix}."):
            model_id = model_id[len(prefix) + 1:]
            break
    return model_id


def _find_model(identifier: str):
    identifier = _foundation_model_id_from_identifier(identifier)
    if not identifier:
        return None
    for entry in _CATALOG:
        if entry[0] == identifier:
            return entry
    return None


def _list_foundation_models(query_params) -> tuple:
    by_provider = (query_params.get("byProvider") or [None])[0] if isinstance(query_params, dict) else None
    by_output = (query_params.get("byOutputModality") or [None])[0] if isinstance(query_params, dict) else None
    by_inference = (query_params.get("byInferenceType") or [None])[0] if isinstance(query_params, dict) else None
    by_customization = (query_params.get("byCustomizationType") or [None])[0] if isinstance(query_params, dict) else None
    summaries = []
    for entry in _CATALOG:
        model_id, _, provider, inputs, outputs, _ = entry
        if by_provider and provider.lower() != by_provider.lower():
            continue
        if by_output and by_output.upper() not in outputs:
            continue
        if by_inference and by_inference.upper() != "ON_DEMAND":
            continue
        if by_customization:
            # No catalog model supports customization in mock — filter all out
            continue
        summaries.append(_model_summary(*entry))
    return _json({"ModelSummaries": summaries})


def _get_foundation_model(identifier: str) -> tuple:
    entry = _find_model(identifier)
    if entry is None:
        return _not_found(f"Could not find model with ID {identifier}.")
    return _json({"ModelDetails": _model_summary(*entry)})


def _get_foundation_model_availability(model_id: str) -> tuple:
    if _find_model(model_id) is None:
        return _not_found(f"Could not find model {model_id}.")
    return _json({
        "ModelId": model_id,
        "AgreementAvailability": {"Status": "AVAILABLE"},
        "AuthorizationStatus": "AUTHORIZED",
        "EntitlementAvailability": "AVAILABLE",
        "RegionAvailability": "AVAILABLE",
    })


def _list_foundation_model_agreement_offers(model_id: str) -> tuple:
    if _find_model(model_id) is None:
        return _not_found(f"Could not find model {model_id}.")
    return _json({"ModelId": model_id, "Offers": []})


def _create_foundation_model_agreement(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("modelId"):
        return _validation("modelId is required.")
    return _json({"ModelId": body_obj["modelId"]}, status=202)


def _delete_foundation_model_agreement(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("modelId"):
        return _validation("modelId is required.")
    return _empty(status=202)


# ===========================================================================
# Inference profiles (system + application-managed)
# ===========================================================================


def _inference_profile_arn(profile_id: str, *, application: bool = False) -> str:
    resource_type = "application-inference-profile" if application else "inference-profile"
    return _arn(resource_type, profile_id)


def _list_inference_profiles(query_params) -> tuple:
    type_filter = (query_params.get("typeEquals") or [None])[0] if isinstance(query_params, dict) else None
    profiles = []
    # System-defined: built from catalog + region prefixes
    if not type_filter or type_filter == "SYSTEM_DEFINED":
        for entry in _CATALOG:
            model_id, model_name, provider, inputs, outputs, streaming = entry
            if not streaming:
                continue
            for prefix in _INFERENCE_PROFILE_PREFIXES:
                pid = f"{prefix}.{model_id}"
                profiles.append({
                    "InferenceProfileName": f"{prefix.upper()} {model_name}",
                    "InferenceProfileId": pid,
                    "InferenceProfileArn": _inference_profile_arn(pid),
                    "Models": [{"ModelArn": _model_arn(model_id)}],
                    "Status": "ACTIVE",
                    "Type": "SYSTEM_DEFINED",
                })
    # Application-managed: user-created
    if not type_filter or type_filter == "APPLICATION":
        for rec in _inference_profiles_apps.values():
            profiles.append(rec)
    return _json({"InferenceProfileSummaries": profiles})


def _get_inference_profile(identifier: str) -> tuple:
    # Application-managed first
    if identifier in _inference_profiles_apps:
        return _json(_inference_profiles_apps[identifier])
    # System-defined
    for prefix in _INFERENCE_PROFILE_PREFIXES:
        if identifier.startswith(f"{prefix}."):
            model_id = identifier[len(prefix) + 1:]
            for entry in _CATALOG:
                if entry[0] == model_id:
                    return _json({
                        "InferenceProfileName": f"{prefix.upper()} {entry[1]}",
                        "InferenceProfileId": identifier,
                        "InferenceProfileArn": _inference_profile_arn(identifier),
                        "Models": [{"ModelArn": _model_arn(model_id)}],
                        "Status": "ACTIVE",
                        "Type": "SYSTEM_DEFINED",
                    })
    return _not_found(f"Could not find inference profile {identifier}.")


def _create_inference_profile(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    name = body_obj.get("inferenceProfileName")
    models = body_obj.get("modelSource", {}).get("copyFrom")
    if not name:
        return _validation("inferenceProfileName is required.")
    if not models:
        return _validation("modelSource.copyFrom is required.")
    pid = f"ip-{uuid.uuid4().hex[:12]}"
    rec = {
        "InferenceProfileName": name,
        "InferenceProfileId": pid,
        "InferenceProfileArn": _inference_profile_arn(pid, application=True),
        "Models": [{"ModelArn": models}],
        "Status": "ACTIVE",
        "Type": "APPLICATION",
        "Description": body_obj.get("description", ""),
        "CreatedAt": _now_iso(),
        "UpdatedAt": _now_iso(),
    }
    _inference_profiles_apps[pid] = rec
    if body_obj.get("tags"):
        _tags[rec["InferenceProfileArn"]] = {t["key"]: t["value"] for t in body_obj["tags"]}
    return _json({
        "InferenceProfileArn": rec["InferenceProfileArn"],
        "Status": "ACTIVE",
    }, status=201)


def _delete_inference_profile(identifier: str) -> tuple:
    if identifier in _inference_profiles_apps:
        del _inference_profiles_apps[identifier]
        return _empty()
    return _not_found(f"Inference profile {identifier} not found.")


# ===========================================================================
# Guardrails (CRUD + versions)
# ===========================================================================


def _guardrail_arn(gid: str) -> str:
    return _arn("guardrail", gid)


def _create_guardrail(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    name = body_obj.get("name")
    if not name:
        return _validation("name is required.")
    if not body_obj.get("blockedInputMessaging"):
        return _validation("blockedInputMessaging is required.")
    if not body_obj.get("blockedOutputsMessaging"):
        return _validation("blockedOutputsMessaging is required.")
    for existing in _guardrails.values():
        if existing["Name"] == name:
            return _conflict(f"Guardrail {name} already exists.")
    gid = f"gr-{uuid.uuid4().hex[:12]}"
    arn = _guardrail_arn(gid)
    now = _now_iso()
    rec = {
        "GuardrailId": gid,
        "GuardrailArn": arn,
        "Name": name,
        "Description": body_obj.get("description", ""),
        "Version": "DRAFT",
        "Status": "READY",
        "CreatedAt": now,
        "UpdatedAt": now,
        "TopicPolicy": body_obj.get("topicPolicyConfig"),
        "ContentPolicy": body_obj.get("contentPolicyConfig"),
        "WordPolicy": body_obj.get("wordPolicyConfig"),
        "SensitiveInformationPolicy": body_obj.get("sensitiveInformationPolicyConfig"),
        "ContextualGroundingPolicy": body_obj.get("contextualGroundingPolicyConfig"),
        "BlockedInputMessaging": body_obj["blockedInputMessaging"],
        "BlockedOutputsMessaging": body_obj["blockedOutputsMessaging"],
        "KmsKeyArn": body_obj.get("kmsKeyId"),
    }
    _guardrails[gid] = rec
    if body_obj.get("tags"):
        _tags[arn] = {t["key"]: t["value"] for t in body_obj["tags"]}
    return _json({
        "GuardrailId": gid,
        "GuardrailArn": arn,
        "Version": "DRAFT",
        "CreatedAt": now,
    }, status=202)


def _get_guardrail(identifier: str, query_params) -> tuple:
    version = (query_params.get("guardrailVersion") or ["DRAFT"])[0] if isinstance(query_params, dict) else "DRAFT"
    rec = _guardrails.get(identifier)
    if not rec:
        # Identifier may be an ARN
        for r in _guardrails.values():
            if r["GuardrailArn"] == identifier:
                rec = r
                break
    if rec is None:
        return _not_found(f"Guardrail {identifier} not found.")
    if version != "DRAFT":
        version_key = f"{rec['GuardrailId']}/{version}"
        ver_rec = _guardrail_versions.get(version_key)
        if not ver_rec:
            return _not_found(f"Guardrail {identifier} version {version} not found.")
        rec = ver_rec
    payload = dict(rec)
    payload["Version"] = version
    return _json(payload)


def _list_guardrails(query_params) -> tuple:
    summaries = []
    for r in _guardrails.values():
        summaries.append({
            "Id": r["GuardrailId"],
            "Arn": r["GuardrailArn"],
            "Status": r["Status"],
            "Name": r["Name"],
            "Description": r["Description"],
            "Version": r["Version"],
            "CreatedAt": r["CreatedAt"],
            "UpdatedAt": r["UpdatedAt"],
        })
    return _json({"Guardrails": summaries})


def _update_guardrail(identifier: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _guardrails.get(identifier)
    if rec is None:
        for r in _guardrails.values():
            if r["GuardrailArn"] == identifier:
                rec = r
                break
    if rec is None:
        return _not_found(f"Guardrail {identifier} not found.")
    if "name" in body_obj:
        rec["Name"] = body_obj["name"]
    if "description" in body_obj:
        rec["Description"] = body_obj["description"]
    for k in ("topicPolicyConfig", "contentPolicyConfig", "wordPolicyConfig",
              "sensitiveInformationPolicyConfig", "contextualGroundingPolicyConfig",
              "blockedInputMessaging", "blockedOutputsMessaging", "kmsKeyId"):
        if k in body_obj:
            internal = _to_internal_guardrail(k)
            rec[internal] = body_obj[k]
    rec["UpdatedAt"] = _now_iso()
    return _json({
        "GuardrailId": rec["GuardrailId"],
        "GuardrailArn": rec["GuardrailArn"],
        "Version": rec["Version"],
        "UpdatedAt": rec["UpdatedAt"],
    }, status=202)


def _to_internal_guardrail(wire_key: str) -> str:
    mapping = {
        "topicPolicyConfig": "TopicPolicy",
        "contentPolicyConfig": "ContentPolicy",
        "wordPolicyConfig": "WordPolicy",
        "sensitiveInformationPolicyConfig": "SensitiveInformationPolicy",
        "contextualGroundingPolicyConfig": "ContextualGroundingPolicy",
        "blockedInputMessaging": "BlockedInputMessaging",
        "blockedOutputsMessaging": "BlockedOutputsMessaging",
        "kmsKeyId": "KmsKeyArn",
    }
    return mapping.get(wire_key, wire_key)


def _delete_guardrail(identifier: str, query_params) -> tuple:
    rec = _guardrails.get(identifier)
    if rec is None:
        for r in _guardrails.values():
            if r["GuardrailArn"] == identifier:
                rec = r
                break
    if rec is None:
        return _not_found(f"Guardrail {identifier} not found.")
    _guardrails.pop(rec["GuardrailId"], None)
    # remove its versions
    for key in list(_guardrail_versions.keys()):
        if key.startswith(f"{rec['GuardrailId']}/"):
            _guardrail_versions.pop(key, None)
    return _empty(status=202)


def _create_guardrail_version(identifier: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _guardrails.get(identifier)
    if rec is None:
        for r in _guardrails.values():
            if r["GuardrailArn"] == identifier:
                rec = r
                break
    if rec is None:
        return _not_found(f"Guardrail {identifier} not found.")
    # Find next version number
    existing_versions = [int(k.split("/", 1)[1])
                         for k in _guardrail_versions.keys()
                         if k.startswith(f"{rec['GuardrailId']}/")
                         and k.split("/", 1)[1].isdigit()]
    next_version = str(max(existing_versions) + 1 if existing_versions else 1)
    snapshot = dict(rec)
    snapshot["Version"] = next_version
    snapshot["CreatedAt"] = _now_iso()
    _guardrail_versions[f"{rec['GuardrailId']}/{next_version}"] = snapshot
    return _json({
        "GuardrailId": rec["GuardrailId"],
        "Version": next_version,
    }, status=202)


# ===========================================================================
# Custom models
# ===========================================================================


def _custom_model_arn(name: str) -> str:
    return _arn("custom-model", name)


def _create_custom_model(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    name = body_obj.get("modelName")
    if not name:
        return _validation("modelName is required.")
    if not body_obj.get("modelSourceConfig"):
        return _validation("modelSourceConfig is required.")
    arn = _custom_model_arn(name)
    if arn in _custom_models:
        return _conflict(f"Custom model {name} already exists.")
    rec = {
        "ModelArn": arn,
        "ModelName": name,
        "BaseModelArn": body_obj.get("modelSourceConfig", {}).get("baseModelArn", ""),
        "CreationTime": _now_iso(),
        "ModelKmsKeyArn": body_obj.get("modelKmsKeyArn"),
        "JobName": body_obj.get("jobName"),
        "JobArn": _arn("model-customization-job", uuid.uuid4().hex[:12]),
    }
    _custom_models[arn] = rec
    return _json({"ModelArn": arn}, status=200)


def _get_custom_model(identifier: str) -> tuple:
    rec = _custom_models.get(identifier)
    if not rec:
        # by ARN
        for r in _custom_models.values():
            if r["ModelArn"] == identifier or r["ModelName"] == identifier:
                rec = r
                break
    if rec is None:
        return _not_found(f"Custom model {identifier} not found.")
    return _json(rec)


def _list_custom_models(query_params) -> tuple:
    summaries = []
    for r in _custom_models.values():
        summaries.append({
            "ModelArn": r["ModelArn"],
            "ModelName": r["ModelName"],
            "CreationTime": r["CreationTime"],
            "BaseModelArn": r["BaseModelArn"],
            "BaseModelName": r["BaseModelArn"].rsplit("/", 1)[-1] if r["BaseModelArn"] else "",
            "CustomizationType": "FINE_TUNING",
            "OwnerAccountId": get_account_id(),
        })
    return _json({"ModelSummaries": summaries})


def _delete_custom_model(identifier: str) -> tuple:
    for arn in list(_custom_models.keys()):
        rec = _custom_models[arn]
        if arn == identifier or rec["ModelArn"] == identifier or rec["ModelName"] == identifier:
            del _custom_models[arn]
            return _empty()
    return _not_found(f"Custom model {identifier} not found.")


# ===========================================================================
# Imported models
# ===========================================================================


def _imported_model_arn(name: str) -> str:
    return _arn("imported-model", name)


def _get_imported_model(identifier: str) -> tuple:
    for r in _imported_models.values():
        if r["ModelArn"] == identifier or r["ModelName"] == identifier:
            return _json(r)
    return _not_found(f"Imported model {identifier} not found.")


def _list_imported_models(query_params) -> tuple:
    summaries = []
    for r in _imported_models.values():
        summaries.append({
            "ModelArn": r["ModelArn"],
            "ModelName": r["ModelName"],
            "CreationTime": r["CreationTime"],
            "ModelArchitecture": r.get("ModelArchitecture", "llama2"),
            "InstructSupported": r.get("InstructSupported", False),
        })
    return _json({"ModelSummaries": summaries})


def _delete_imported_model(identifier: str) -> tuple:
    for arn in list(_imported_models.keys()):
        rec = _imported_models[arn]
        if arn == identifier or rec["ModelArn"] == identifier or rec["ModelName"] == identifier:
            del _imported_models[arn]
            return _empty()
    return _not_found(f"Imported model {identifier} not found.")


# ===========================================================================
# Provisioned model throughput
# ===========================================================================


def _provisioned_arn(pid: str) -> str:
    return _arn("provisioned-model", pid)


def _create_provisioned(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    name = body_obj.get("provisionedModelName")
    if not name:
        return _validation("provisionedModelName is required.")
    if not body_obj.get("modelId"):
        return _validation("modelId is required.")
    if "modelUnits" not in body_obj:
        return _validation("modelUnits is required.")
    pid = uuid.uuid4().hex[:12]
    arn = _provisioned_arn(pid)
    now = _now_iso()
    rec = {
        "ProvisionedModelArn": arn,
        "ProvisionedModelId": pid,
        "ProvisionedModelName": name,
        "ModelArn": _model_arn(body_obj["modelId"]),
        "ModelUnits": body_obj["modelUnits"],
        "DesiredModelUnits": body_obj["modelUnits"],
        "DesiredModelArn": _model_arn(body_obj["modelId"]),
        "Status": "InService",
        "CreationTime": now,
        "LastModifiedTime": now,
        "CommitmentDuration": body_obj.get("commitmentDuration"),
    }
    _provisioned_throughputs[pid] = rec
    return _json({"ProvisionedModelArn": arn}, status=201)


def _get_provisioned(pid: str) -> tuple:
    rec = _provisioned_throughputs.get(pid)
    if rec is None:
        for r in _provisioned_throughputs.values():
            if r["ProvisionedModelArn"] == pid:
                rec = r
                break
    if rec is None:
        return _not_found(f"Provisioned model {pid} not found.")
    return _json(rec)


def _list_provisioned(query_params) -> tuple:
    summaries = list(_provisioned_throughputs.values())
    return _json({"ProvisionedModelSummaries": summaries})


def _update_provisioned(pid: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _provisioned_throughputs.get(pid)
    if rec is None:
        for r in _provisioned_throughputs.values():
            if r["ProvisionedModelArn"] == pid:
                rec = r
                break
    if rec is None:
        return _not_found(f"Provisioned model {pid} not found.")
    if "desiredProvisionedModelName" in body_obj:
        rec["ProvisionedModelName"] = body_obj["desiredProvisionedModelName"]
    if "desiredModelId" in body_obj:
        rec["DesiredModelArn"] = _model_arn(body_obj["desiredModelId"])
    rec["LastModifiedTime"] = _now_iso()
    return _empty()


def _delete_provisioned(pid: str) -> tuple:
    for k in list(_provisioned_throughputs.keys()):
        rec = _provisioned_throughputs[k]
        if k == pid or rec["ProvisionedModelArn"] == pid:
            del _provisioned_throughputs[k]
            return _empty()
    return _not_found(f"Provisioned model {pid} not found.")


# ===========================================================================
# Generic job family (customization / import / copy / invocation / evaluation)
# ===========================================================================


def _job_record(job_type: str, body_obj: dict, name_field: str = "jobName") -> dict:
    job_id = uuid.uuid4().hex[:12]
    arn = _arn(job_type, job_id)
    now = _now_iso()
    return {
        "JobArn": arn,
        "JobName": body_obj.get(name_field, f"{job_type}-{job_id}"),
        "Status": "InProgress",
        "CreationTime": now,
        "LastModifiedTime": now,
        "BodyEcho": body_obj,  # carry original input for shape introspection
    }


def _create_model_customization_job(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("baseModelIdentifier"):
        return _validation("baseModelIdentifier is required.")
    if not body_obj.get("customModelName"):
        return _validation("customModelName is required.")
    if not body_obj.get("jobName"):
        return _validation("jobName is required.")
    rec = _job_record("model-customization-job", body_obj)
    rec["BaseModelIdentifier"] = body_obj["baseModelIdentifier"]
    rec["CustomModelName"] = body_obj["customModelName"]
    rec["RoleArn"] = body_obj.get("roleArn", "")
    rec["TrainingDataConfig"] = body_obj.get("trainingDataConfig")
    rec["OutputDataConfig"] = body_obj.get("outputDataConfig")
    _model_customization_jobs[rec["JobArn"]] = rec
    return _json({"JobArn": rec["JobArn"]}, status=201)


def _get_model_customization_job(identifier: str) -> tuple:
    for k, r in _model_customization_jobs.items():
        if k == identifier or r["JobArn"] == identifier or r["JobName"] == identifier:
            return _json(r)
    return _not_found(f"Model customization job {identifier} not found.")


def _list_model_customization_jobs(query_params) -> tuple:
    return _json({"ModelCustomizationJobSummaries": list(_model_customization_jobs.values())})


def _stop_model_customization_job(identifier: str) -> tuple:
    for r in _model_customization_jobs.values():
        if r["JobArn"] == identifier or r["JobName"] == identifier:
            r["Status"] = "Stopped"
            return _empty()
    return _not_found(f"Model customization job {identifier} not found.")


def _create_model_import_job(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("jobName"):
        return _validation("jobName is required.")
    if not body_obj.get("importedModelName"):
        return _validation("importedModelName is required.")
    rec = _job_record("model-import-job", body_obj)
    rec["ImportedModelName"] = body_obj["importedModelName"]
    rec["RoleArn"] = body_obj.get("roleArn", "")
    rec["ModelDataSource"] = body_obj.get("modelDataSource", {})
    _model_import_jobs[rec["JobArn"]] = rec
    # Side effect: also create imported model record
    im_arn = _imported_model_arn(body_obj["importedModelName"])
    _imported_models[im_arn] = {
        "ModelArn": im_arn,
        "ModelName": body_obj["importedModelName"],
        "CreationTime": _now_iso(),
        "JobArn": rec["JobArn"],
        "JobName": body_obj["jobName"],
        "ModelArchitecture": "llama2",
    }
    return _json({"JobArn": rec["JobArn"]}, status=201)


def _get_model_import_job(identifier: str) -> tuple:
    for r in _model_import_jobs.values():
        if r["JobArn"] == identifier or r["JobName"] == identifier:
            return _json(r)
    return _not_found(f"Model import job {identifier} not found.")


def _list_model_import_jobs(query_params) -> tuple:
    return _json({"ModelImportJobSummaries": list(_model_import_jobs.values())})


def _create_model_copy_job(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("sourceModelArn"):
        return _validation("sourceModelArn is required.")
    if not body_obj.get("targetModelName"):
        return _validation("targetModelName is required.")
    rec = _job_record("model-copy-job", body_obj, name_field="targetModelName")
    rec["SourceModelArn"] = body_obj["sourceModelArn"]
    rec["TargetModelName"] = body_obj["targetModelName"]
    rec["TargetModelArn"] = _custom_model_arn(body_obj["targetModelName"])
    _model_copy_jobs[rec["JobArn"]] = rec
    return _json({"JobArn": rec["JobArn"]}, status=201)


def _get_model_copy_job(identifier: str) -> tuple:
    for r in _model_copy_jobs.values():
        if r["JobArn"] == identifier:
            return _json(r)
    return _not_found(f"Model copy job {identifier} not found.")


def _list_model_copy_jobs(query_params) -> tuple:
    return _json({"ModelCopyJobSummaries": list(_model_copy_jobs.values())})


def _create_model_invocation_job(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("jobName"):
        return _validation("jobName is required.")
    if not body_obj.get("modelId"):
        return _validation("modelId is required.")
    rec = _job_record("model-invocation-job", body_obj)
    rec["ModelId"] = body_obj["modelId"]
    rec["RoleArn"] = body_obj.get("roleArn", "")
    rec["InputDataConfig"] = body_obj.get("inputDataConfig")
    rec["OutputDataConfig"] = body_obj.get("outputDataConfig")
    _model_invocation_jobs[rec["JobArn"]] = rec
    return _json({"JobArn": rec["JobArn"]})


def _get_model_invocation_job(identifier: str) -> tuple:
    for r in _model_invocation_jobs.values():
        if r["JobArn"] == identifier or r["JobName"] == identifier:
            return _json(r)
    return _not_found(f"Model invocation job {identifier} not found.")


def _list_model_invocation_jobs(query_params) -> tuple:
    return _json({"InvocationJobSummaries": list(_model_invocation_jobs.values())})


def _stop_model_invocation_job(identifier: str) -> tuple:
    for r in _model_invocation_jobs.values():
        if r["JobArn"] == identifier:
            r["Status"] = "Stopped"
            return _empty()
    return _not_found(f"Model invocation job {identifier} not found.")


def _create_evaluation_job(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("jobName"):
        return _validation("jobName is required.")
    if not body_obj.get("roleArn"):
        return _validation("roleArn is required.")
    rec = _job_record("evaluation-job", body_obj)
    rec["RoleArn"] = body_obj["roleArn"]
    rec["EvaluationConfig"] = body_obj.get("evaluationConfig")
    rec["InferenceConfig"] = body_obj.get("inferenceConfig")
    rec["OutputDataConfig"] = body_obj.get("outputDataConfig")
    _evaluation_jobs[rec["JobArn"]] = rec
    return _json({"JobArn": rec["JobArn"]}, status=202)


def _get_evaluation_job(identifier: str) -> tuple:
    for r in _evaluation_jobs.values():
        if r["JobArn"] == identifier or r["JobName"] == identifier:
            return _json(r)
    return _not_found(f"Evaluation job {identifier} not found.")


def _list_evaluation_jobs(query_params) -> tuple:
    return _json({"JobSummaries": list(_evaluation_jobs.values())})


def _stop_evaluation_job(identifier: str) -> tuple:
    for r in _evaluation_jobs.values():
        if r["JobArn"] == identifier:
            r["Status"] = "Stopped"
            return _empty()
    return _not_found(f"Evaluation job {identifier} not found.")


def _batch_delete_evaluation_job(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    job_ids = body_obj.get("jobIdentifiers", [])
    deleted = []
    errors = []
    for jid in job_ids:
        found = False
        for k in list(_evaluation_jobs.keys()):
            r = _evaluation_jobs[k]
            if r["JobArn"] == jid or r["JobName"] == jid:
                del _evaluation_jobs[k]
                deleted.append({"JobIdentifier": jid, "JobStatus": "Deleting"})
                found = True
                break
        if not found:
            errors.append({"JobIdentifier": jid, "Code": "ResourceNotFound",
                            "Message": f"Job {jid} not found."})
    return _json({"EvaluationJobs": deleted, "Errors": errors}, status=202)


# ===========================================================================
# Marketplace model endpoints
# ===========================================================================


def _marketplace_arn(name: str) -> str:
    return _arn("marketplace-model-endpoint", name)


def _create_marketplace_endpoint(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("endpointName"):
        return _validation("endpointName is required.")
    if not body_obj.get("modelSourceIdentifier"):
        return _validation("modelSourceIdentifier is required.")
    name = body_obj["endpointName"]
    arn = _marketplace_arn(name)
    if arn in _marketplace_endpoints:
        return _conflict(f"Endpoint {name} already exists.")
    now = _now_iso()
    rec = {
        "EndpointArn": arn,
        "ModelSourceIdentifier": body_obj["modelSourceIdentifier"],
        "Status": "REGISTERED",
        "StatusMessage": "",
        "EndpointStatus": "InService",
        "EndpointStatusMessage": "",
        "CreatedAt": now,
        "UpdatedAt": now,
        "EndpointConfig": body_obj.get("endpointConfig"),
    }
    _marketplace_endpoints[arn] = rec
    return _json({"MarketplaceModelEndpoint": rec})


def _get_marketplace_endpoint(arn: str) -> tuple:
    rec = _marketplace_endpoints.get(arn)
    if rec is None:
        return _not_found(f"Endpoint {arn} not found.")
    return _json({"MarketplaceModelEndpoint": rec})


def _list_marketplace_endpoints(query_params) -> tuple:
    return _json({"MarketplaceModelEndpoints": list(_marketplace_endpoints.values())})


def _delete_marketplace_endpoint(arn: str) -> tuple:
    if arn not in _marketplace_endpoints:
        return _not_found(f"Endpoint {arn} not found.")
    del _marketplace_endpoints[arn]
    return _empty()


def _update_marketplace_endpoint(arn: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _marketplace_endpoints.get(arn)
    if rec is None:
        return _not_found(f"Endpoint {arn} not found.")
    if "endpointConfig" in body_obj:
        rec["EndpointConfig"] = body_obj["endpointConfig"]
    rec["UpdatedAt"] = _now_iso()
    return _json({"MarketplaceModelEndpoint": rec})


def _register_marketplace_endpoint(identifier: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _marketplace_endpoints.get(identifier)
    if rec is None:
        return _not_found(f"Endpoint {identifier} not found.")
    rec["Status"] = "REGISTERED"
    return _json({"MarketplaceModelEndpoint": rec})


def _deregister_marketplace_endpoint(arn: str) -> tuple:
    rec = _marketplace_endpoints.get(arn)
    if rec is None:
        return _not_found(f"Endpoint {arn} not found.")
    rec["Status"] = "DEREGISTERED"
    return _empty()


# ===========================================================================
# Prompt routers
# ===========================================================================


def _prompt_router_arn(name: str) -> str:
    return _arn("prompt-router", name)


def _create_prompt_router(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("promptRouterName"):
        return _validation("promptRouterName is required.")
    if not body_obj.get("models"):
        return _validation("models is required.")
    name = body_obj["promptRouterName"]
    arn = _prompt_router_arn(name)
    if arn in _prompt_routers:
        return _conflict(f"Prompt router {name} already exists.")
    now = _now_iso()
    rec = {
        "PromptRouterArn": arn,
        "PromptRouterName": name,
        "Models": body_obj["models"],
        "FallbackModel": body_obj.get("fallbackModel"),
        "RoutingCriteria": body_obj.get("routingCriteria"),
        "Description": body_obj.get("description", ""),
        "Status": "AVAILABLE",
        "Type": "custom",
        "CreatedAt": now,
        "UpdatedAt": now,
    }
    _prompt_routers[arn] = rec
    return _json({"PromptRouterArn": arn})


def _get_prompt_router(arn: str) -> tuple:
    rec = _prompt_routers.get(arn)
    if rec is None:
        return _not_found(f"Prompt router {arn} not found.")
    return _json(rec)


def _list_prompt_routers(query_params) -> tuple:
    return _json({"PromptRouterSummaries": list(_prompt_routers.values())})


def _delete_prompt_router(arn: str) -> tuple:
    if arn not in _prompt_routers:
        return _not_found(f"Prompt router {arn} not found.")
    del _prompt_routers[arn]
    return _empty()


# ===========================================================================
# Model invocation logging configuration (singleton per region)
# ===========================================================================


def _get_logging_config() -> tuple:
    rec = _invocation_logging_config.get("config")
    if rec is None:
        return _json({"LoggingConfig": None})
    return _json({"LoggingConfig": rec})


def _put_logging_config(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    config = body_obj.get("loggingConfig", {})
    _invocation_logging_config["config"] = config
    return _empty()


def _delete_logging_config() -> tuple:
    _invocation_logging_config.pop("config", None)
    return _empty()


# ===========================================================================
# Use Case for Model Access
# ===========================================================================
# (_USE_CASE is defined up top with the other state containers so it exists
#  before the import-time restore_state().)


def _get_use_case_for_model_access() -> tuple:
    rec = _USE_CASE.get("usecase")
    if rec is None:
        return _json({"FormData": ""})
    return _json(rec)


def _put_use_case_for_model_access(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if "formData" not in body_obj:
        return _validation("formData is required.")
    _USE_CASE["usecase"] = {"FormData": body_obj["formData"]}
    return _empty(status=201)


# ===========================================================================
# Tags (TagResource/UntagResource/ListTagsForResource — special paths)
# ===========================================================================


def _tag_resource(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    arn = body_obj.get("resourceARN")
    tags = body_obj.get("tags", [])
    if not arn:
        return _validation("resourceARN is required.")
    validation_error = _validate_resource_arn_for_tags(arn)
    if validation_error:
        return validation_error
    if not isinstance(tags, list):
        return _validation("tags must be an array.")
    current = dict(_tags.get(arn, {}))
    for t in tags:
        if isinstance(t, dict) and "key" in t and "value" in t:
            current[t["key"]] = t["value"]
    if len(current) > 200:
        return _too_many_tags("Maximum of 200 tags exceeded.")
    _tags[arn] = current
    return _empty()


def _untag_resource(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    arn = body_obj.get("resourceARN")
    keys = body_obj.get("tagKeys", [])
    if not arn:
        return _validation("resourceARN is required.")
    validation_error = _validate_resource_arn_for_tags(arn)
    if validation_error:
        return validation_error
    current = dict(_tags.get(arn, {}))
    for k in keys:
        current.pop(k, None)
    _tags[arn] = current
    return _empty()


def _list_tags_for_resource(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    arn = body_obj.get("resourceARN")
    if not arn:
        return _validation("resourceARN is required.")
    validation_error = _validate_resource_arn_for_tags(arn)
    if validation_error:
        return validation_error
    tags = [{"Key": k, "Value": v} for k, v in _tags.get(arn, {}).items()]
    return _json({"Tags": tags})


# ===========================================================================
# Dispatcher (66 ops, all paths verified against botocore http.requestUri)
# ===========================================================================


# Precompiled regexes for path-with-{identifier}
_GET_MODEL_RE = re.compile(r"^/foundation-models/(.+)$")
_GET_AVAIL_RE = re.compile(r"^/foundation-model-availability/(.+)$")
_LIST_AGREE_OFFERS_RE = re.compile(r"^/list-foundation-model-agreement-offers/(.+)$")
_GET_PROFILE_RE = re.compile(r"^/inference-profiles/(.+)$")
_GUARDRAIL_RE = re.compile(r"^/guardrails/([^/]+)$")
_CUSTOM_MODEL_RE = re.compile(r"^/custom-models/(.+)$")
_IMPORTED_MODEL_RE = re.compile(r"^/imported-models/(.+)$")
_PROVISIONED_RE = re.compile(r"^/provisioned-model-throughput/(.+)$")
_CUSTOMIZATION_JOB_RE = re.compile(r"^/model-customization-jobs/(.+?)(?:/stop)?$")
_CUSTOMIZATION_JOB_STOP_RE = re.compile(r"^/model-customization-jobs/(.+)/stop$")
_IMPORT_JOB_RE = re.compile(r"^/model-import-jobs/(.+)$")
_COPY_JOB_RE = re.compile(r"^/model-copy-jobs/(.+)$")
_INVOCATION_JOB_RE = re.compile(r"^/model-invocation-job/(.+?)(?:/stop)?$")
_INVOCATION_JOB_STOP_RE = re.compile(r"^/model-invocation-job/(.+)/stop$")
_EVALUATION_JOB_RE = re.compile(r"^/evaluation-jobs/(.+)$")
_EVALUATION_JOB_STOP_RE = re.compile(r"^/evaluation-job/(.+)/stop$")
_MARKETPLACE_RE = re.compile(r"^/marketplace-model/endpoints/(.+)$")
_MARKETPLACE_REGISTER_RE = re.compile(r"^/marketplace-model/endpoints/(.+)/registration$")
_PROMPT_ROUTER_RE = re.compile(r"^/prompt-routers/(.+)$")


async def handle_request(method, path, headers, body, query_params):
    logger.debug("%s %s", method, path)

    # --- Foundation models ---
    if path == "/foundation-models" and method == "GET":
        return _list_foundation_models(query_params)
    m = _GET_MODEL_RE.match(path)
    if m and method == "GET":
        return _get_foundation_model(unquote(m.group(1)))
    m = _GET_AVAIL_RE.match(path)
    if m and method == "GET":
        return _get_foundation_model_availability(unquote(m.group(1)))
    if path == "/create-foundation-model-agreement" and method == "POST":
        return _create_foundation_model_agreement(body)
    if path == "/delete-foundation-model-agreement" and method == "POST":
        return _delete_foundation_model_agreement(body)
    m = _LIST_AGREE_OFFERS_RE.match(path)
    if m and method == "GET":
        return _list_foundation_model_agreement_offers(unquote(m.group(1)))

    # --- Inference profiles ---
    if path == "/inference-profiles":
        if method == "GET":
            return _list_inference_profiles(query_params)
        if method == "POST":
            return _create_inference_profile(body)
    m = _GET_PROFILE_RE.match(path)
    if m:
        ident = unquote(m.group(1))
        if method == "GET":
            return _get_inference_profile(ident)
        if method == "DELETE":
            return _delete_inference_profile(ident)

    # --- Guardrails ---
    if path == "/guardrails":
        if method == "GET":
            return _list_guardrails(query_params)
        if method == "POST":
            return _create_guardrail(body)
    m = _GUARDRAIL_RE.match(path)
    if m:
        ident = unquote(m.group(1))
        if method == "GET":
            return _get_guardrail(ident, query_params)
        if method == "PUT":
            return _update_guardrail(ident, body)
        if method == "DELETE":
            return _delete_guardrail(ident, query_params)
        if method == "POST":
            return _create_guardrail_version(ident, body)

    # --- Custom models ---
    if path == "/custom-models" and method == "GET":
        return _list_custom_models(query_params)
    if path == "/custom-models/create-custom-model" and method == "POST":
        return _create_custom_model(body)
    m = _CUSTOM_MODEL_RE.match(path)
    if m and path != "/custom-models/create-custom-model":
        ident = unquote(m.group(1))
        if method == "GET":
            return _get_custom_model(ident)
        if method == "DELETE":
            return _delete_custom_model(ident)

    # --- Imported models ---
    if path == "/imported-models" and method == "GET":
        return _list_imported_models(query_params)
    m = _IMPORTED_MODEL_RE.match(path)
    if m:
        ident = unquote(m.group(1))
        if method == "GET":
            return _get_imported_model(ident)
        if method == "DELETE":
            return _delete_imported_model(ident)

    # --- Provisioned throughput ---
    if path == "/provisioned-model-throughput" and method == "POST":
        return _create_provisioned(body)
    if path == "/provisioned-model-throughputs" and method == "GET":
        return _list_provisioned(query_params)
    m = _PROVISIONED_RE.match(path)
    if m:
        ident = unquote(m.group(1))
        if method == "GET":
            return _get_provisioned(ident)
        if method == "PATCH":
            return _update_provisioned(ident, body)
        if method == "DELETE":
            return _delete_provisioned(ident)

    # --- Model customization jobs ---
    if path == "/model-customization-jobs":
        if method == "POST":
            return _create_model_customization_job(body)
        if method == "GET":
            return _list_model_customization_jobs(query_params)
    m = _CUSTOMIZATION_JOB_STOP_RE.match(path)
    if m and method == "POST":
        return _stop_model_customization_job(unquote(m.group(1)))
    m = _CUSTOMIZATION_JOB_RE.match(path)
    if m and method == "GET":
        return _get_model_customization_job(unquote(m.group(1)))

    # --- Model import jobs ---
    if path == "/model-import-jobs":
        if method == "POST":
            return _create_model_import_job(body)
        if method == "GET":
            return _list_model_import_jobs(query_params)
    m = _IMPORT_JOB_RE.match(path)
    if m and method == "GET":
        return _get_model_import_job(unquote(m.group(1)))

    # --- Model copy jobs ---
    if path == "/model-copy-jobs":
        if method == "POST":
            return _create_model_copy_job(body)
        if method == "GET":
            return _list_model_copy_jobs(query_params)
    m = _COPY_JOB_RE.match(path)
    if m and method == "GET":
        return _get_model_copy_job(unquote(m.group(1)))

    # --- Model invocation jobs (batch inference) ---
    if path == "/model-invocation-job" and method == "POST":
        return _create_model_invocation_job(body)
    if path == "/model-invocation-jobs" and method == "GET":
        return _list_model_invocation_jobs(query_params)
    m = _INVOCATION_JOB_STOP_RE.match(path)
    if m and method == "POST":
        return _stop_model_invocation_job(unquote(m.group(1)))
    m = _INVOCATION_JOB_RE.match(path)
    if m and method == "GET":
        return _get_model_invocation_job(unquote(m.group(1)))

    # --- Evaluation jobs ---
    if path == "/evaluation-jobs":
        if method == "POST":
            return _create_evaluation_job(body)
        if method == "GET":
            return _list_evaluation_jobs(query_params)
    if path == "/evaluation-jobs/batch-delete" and method == "POST":
        return _batch_delete_evaluation_job(body)
    m = _EVALUATION_JOB_STOP_RE.match(path)
    if m and method == "POST":
        return _stop_evaluation_job(unquote(m.group(1)))
    m = _EVALUATION_JOB_RE.match(path)
    if m and method == "GET" and path != "/evaluation-jobs/batch-delete":
        return _get_evaluation_job(unquote(m.group(1)))

    # --- Marketplace model endpoints ---
    if path == "/marketplace-model/endpoints":
        if method == "POST":
            return _create_marketplace_endpoint(body)
        if method == "GET":
            return _list_marketplace_endpoints(query_params)
    m = _MARKETPLACE_REGISTER_RE.match(path)
    if m:
        ident = unquote(m.group(1))
        if method == "POST":
            return _register_marketplace_endpoint(ident, body)
        if method == "DELETE":
            return _deregister_marketplace_endpoint(ident)
    m = _MARKETPLACE_RE.match(path)
    if m:
        ident = unquote(m.group(1))
        if method == "GET":
            return _get_marketplace_endpoint(ident)
        if method == "DELETE":
            return _delete_marketplace_endpoint(ident)
        if method == "PATCH":
            return _update_marketplace_endpoint(ident, body)

    # --- Prompt routers ---
    if path == "/prompt-routers":
        if method == "POST":
            return _create_prompt_router(body)
        if method == "GET":
            return _list_prompt_routers(query_params)
    m = _PROMPT_ROUTER_RE.match(path)
    if m:
        ident = unquote(m.group(1))
        if method == "GET":
            return _get_prompt_router(ident)
        if method == "DELETE":
            return _delete_prompt_router(ident)

    # --- Model invocation logging config ---
    if path == "/logging/modelinvocations":
        if method == "GET":
            return _get_logging_config()
        if method == "PUT":
            return _put_logging_config(body)
        if method == "DELETE":
            return _delete_logging_config()

    # --- Use case for model access ---
    if path == "/use-case-for-model-access":
        if method == "GET":
            return _get_use_case_for_model_access()
        if method == "POST":
            return _put_use_case_for_model_access(body)

    # --- Tagging (uses /tagResource, /untagResource, /listTagsForResource paths) ---
    if path == "/tagResource" and method == "POST":
        return _tag_resource(body)
    if path == "/untagResource" and method == "POST":
        return _untag_resource(body)
    if path == "/listTagsForResource" and method == "POST":
        return _list_tags_for_resource(body)

    return _validation(f"No route for {method} {path}.")
