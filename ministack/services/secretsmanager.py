"""
SecretsManager Service Emulator.
JSON-based API via X-Amz-Target.
Supports: CreateSecret, GetSecretValue, ListSecrets, DeleteSecret,
          RestoreSecret, UpdateSecret, DescribeSecret, PutSecretValue,
          TagResource, UntagResource, ListSecretVersionIds, RotateSecret,
          GetRandomPassword, ReplicateSecretToRegions,
          PutResourcePolicy, GetResourcePolicy, DeleteResourcePolicy,
          ValidateResourcePolicy.
"""

import base64
import os
import json
import logging
import secrets as stdlib_secrets
import string
import time

from ministack.core.responses import error_response_json, json_response, new_uuid

logger = logging.getLogger("secretsmanager")

ACCOUNT_ID = "000000000000"
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_secrets: dict = {}
_resource_policies: dict = {}
# name -> {
#   ARN, Name, Description, Tags: [{Key, Value}],
#   CreatedDate, LastChangedDate, LastAccessedDate,
#   DeletedDate (scheduled deletion epoch | None),
#   RotationEnabled, RotationLambdaARN, RotationRules,
#   ReplicationStatus: [{Region, Status, StatusMessage}],
#   Versions: { version_id: {SecretString, SecretBinary, CreatedDate, Stages: [str]} }
# }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve(secret_id):
    """Look up a secret by name or ARN.  Returns (storage_key, record) or (None, None)."""
    if not secret_id:
        return None, None
    if secret_id in _secrets:
        return secret_id, _secrets[secret_id]
    for key, s in _secrets.items():
        if s["ARN"] == secret_id:
            return key, s
    return None, None


def _find_stage_version(secret, stage):
    """Return (version_id, version_dict) for the version carrying *stage*."""
    for vid, ver in secret["Versions"].items():
        if stage in ver["Stages"]:
            return vid, ver
    return None, None


def _apply_current_promotion(secret, new_vid):
    """
    Promote *new_vid* to AWSCURRENT.
    * old AWSCURRENT  →  AWSPREVIOUS
    * old AWSPREVIOUS  →  removed (version pruned when stageless)
    """
    old_curr_vid = None
    old_prev_vid = None
    for vid, ver in list(secret["Versions"].items()):
        if vid == new_vid:
            continue
        if "AWSCURRENT" in ver["Stages"]:
            old_curr_vid = vid
        if "AWSPREVIOUS" in ver["Stages"]:
            old_prev_vid = vid

    if old_prev_vid and old_prev_vid in secret["Versions"]:
        stages = secret["Versions"][old_prev_vid]["Stages"]
        if "AWSPREVIOUS" in stages:
            stages.remove("AWSPREVIOUS")
        if not stages:
            del secret["Versions"][old_prev_vid]

    if old_curr_vid and old_curr_vid in secret["Versions"]:
        stages = secret["Versions"][old_curr_vid]["Stages"]
        if "AWSCURRENT" in stages:
            stages.remove("AWSCURRENT")
        if "AWSPREVIOUS" not in stages:
            stages.append("AWSPREVIOUS")

    ver = secret["Versions"].get(new_vid)
    if ver:
        if "AWSCURRENT" not in ver["Stages"]:
            ver["Stages"].append("AWSCURRENT")
        if "AWSPENDING" in ver["Stages"]:
            ver["Stages"].remove("AWSPENDING")


def _vid_to_stages(secret):
    return {vid: list(ver["Stages"]) for vid, ver in secret["Versions"].items() if ver["Stages"]}


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateSecret": _create_secret,
        "GetSecretValue": _get_secret_value,
        "ListSecrets": _list_secrets,
        "DeleteSecret": _delete_secret,
        "RestoreSecret": _restore_secret,
        "UpdateSecret": _update_secret,
        "DescribeSecret": _describe_secret,
        "PutSecretValue": _put_secret_value,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListSecretVersionIds": _list_secret_version_ids,
        "RotateSecret": _rotate_secret,
        "GetRandomPassword": _get_random_password,
        "ReplicateSecretToRegions": _replicate_secret_to_regions,
        "PutResourcePolicy": _put_resource_policy,
        "GetResourcePolicy": _get_resource_policy,
        "DeleteResourcePolicy": _delete_resource_policy,
        "ValidateResourcePolicy": _validate_resource_policy,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidRequestException", f"Unknown action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

def _create_secret(data):
    name = data.get("Name")
    if not name:
        return error_response_json("InvalidParameterException", "Name is required.", 400)
    if name in _secrets:
        return error_response_json(
            "ResourceExistsException",
            f"The operation failed because the secret {name} already exists.", 400,
        )

    arn = f"arn:aws:secretsmanager:{REGION}:{ACCOUNT_ID}:secret:{name}-{new_uuid()[:6]}"
    vid = new_uuid()
    now = time.time()

    _secrets[name] = {
        "ARN": arn,
        "Name": name,
        "Description": data.get("Description", ""),
        "Tags": list(data.get("Tags", [])),
        "CreatedDate": now,
        "LastChangedDate": now,
        "LastAccessedDate": None,
        "DeletedDate": None,
        "RotationEnabled": False,
        "RotationLambdaARN": data.get("RotationLambdaARN"),
        "RotationRules": data.get("RotationRules"),
        "KmsKeyId": data.get("KmsKeyId"),
        "ReplicationStatus": [],
        "Versions": {
            vid: {
                "SecretString": data.get("SecretString"),
                "SecretBinary": data.get("SecretBinary"),
                "CreatedDate": now,
                "Stages": ["AWSCURRENT"],
            }
        },
    }
    return json_response({"ARN": arn, "Name": name, "VersionId": vid})


def _get_secret_value(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    if secret.get("DeletedDate"):
        return error_response_json(
            "InvalidRequestException",
            "You can't perform this operation on the secret because it was marked for deletion.", 400,
        )

    secret["LastAccessedDate"] = time.time()

    req_vid = data.get("VersionId")
    req_stage = data.get("VersionStage", "AWSCURRENT")

    if req_vid:
        ver = secret["Versions"].get(req_vid)
        if not ver:
            return error_response_json(
                "ResourceNotFoundException",
                f"Secrets Manager can't find the specified secret version: {req_vid}.", 400,
            )
        vid = req_vid
    else:
        vid, ver = _find_stage_version(secret, req_stage)
        if not ver:
            return error_response_json(
                "ResourceNotFoundException",
                f"Secrets Manager can't find the specified secret value for staging label: {req_stage}.", 400,
            )

    result = {
        "ARN": secret["ARN"],
        "Name": secret["Name"],
        "VersionId": vid,
        "VersionStages": list(ver["Stages"]),
        "CreatedDate": ver["CreatedDate"],
    }
    if ver.get("SecretString") is not None:
        result["SecretString"] = ver["SecretString"]
    if ver.get("SecretBinary") is not None:
        result["SecretBinary"] = ver["SecretBinary"]
    return json_response(result)


def _list_secrets(data):
    max_results = min(data.get("MaxResults", 100), 100)
    next_token = data.get("NextToken")
    filters = data.get("Filters", [])

    names = sorted(n for n, s in _secrets.items() if not s.get("DeletedDate"))

    for f in filters:
        key = f.get("Key", "")
        values = [v.lower() for v in f.get("Values", [])]
        if key == "name":
            names = [n for n in names if any(v in n.lower() for v in values)]
        elif key == "tag-key":
            names = [n for n in names
                     if any(t.get("Key", "").lower() in values for t in _secrets[n].get("Tags", []))]
        elif key == "tag-value":
            names = [n for n in names
                     if any(t.get("Value", "").lower() in values for t in _secrets[n].get("Tags", []))]
        elif key == "description":
            names = [n for n in names
                     if any(v in _secrets[n].get("Description", "").lower() for v in values)]

    start = 0
    if next_token:
        try:
            start = int(base64.b64decode(next_token))
        except Exception:
            pass

    page = names[start:start + max_results]
    secret_list = []
    for n in page:
        s = _secrets[n]
        secret_list.append({
            "ARN": s["ARN"],
            "Name": s["Name"],
            "Description": s.get("Description", ""),
            "CreatedDate": s["CreatedDate"],
            "LastChangedDate": s["LastChangedDate"],
            "LastAccessedDate": s.get("LastAccessedDate"),
            "Tags": s.get("Tags", []),
            "SecretVersionsToStages": _vid_to_stages(s),
            "RotationEnabled": s.get("RotationEnabled", False),
        })

    resp: dict = {"SecretList": secret_list}
    end = start + max_results
    if end < len(names):
        resp["NextToken"] = base64.b64encode(str(end).encode()).decode()
    return json_response(resp)


def _delete_secret(data):
    secret_id = data.get("SecretId")
    key, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    if secret.get("DeletedDate"):
        return error_response_json(
            "InvalidRequestException",
            "You can't perform this operation on the secret because it was already scheduled for deletion.", 400,
        )

    force = data.get("ForceDeleteWithoutRecovery", False)
    window = data.get("RecoveryWindowInDays")

    if force and window is not None:
        return error_response_json(
            "InvalidParameterException",
            "You can't use ForceDeleteWithoutRecovery in conjunction with RecoveryWindowInDays.", 400,
        )
    if window is None:
        window = 30
    if not force and not (7 <= window <= 30):
        return error_response_json(
            "InvalidParameterException",
            "RecoveryWindowInDays value must be between 7 and 30 days (inclusive).", 400,
        )

    now = time.time()
    deletion_date = now if force else now + window * 86400

    if force:
        arn, sname = secret["ARN"], secret["Name"]
        del _secrets[key]
        return json_response({"ARN": arn, "Name": sname, "DeletionDate": deletion_date})

    secret["DeletedDate"] = deletion_date
    return json_response({"ARN": secret["ARN"], "Name": secret["Name"], "DeletionDate": deletion_date})


def _restore_secret(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    if not secret.get("DeletedDate"):
        return error_response_json(
            "InvalidRequestException",
            "Secret is not scheduled for deletion.", 400,
        )
    secret["DeletedDate"] = None
    return json_response({"ARN": secret["ARN"], "Name": secret["Name"]})


def _update_secret(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    if secret.get("DeletedDate"):
        return error_response_json(
            "InvalidRequestException",
            "You can't perform this operation on the secret because it was marked for deletion.", 400,
        )

    if "Description" in data:
        secret["Description"] = data["Description"]
    if "KmsKeyId" in data:
        secret["KmsKeyId"] = data["KmsKeyId"]

    has_new_value = "SecretString" in data or "SecretBinary" in data
    if not has_new_value:
        secret["LastChangedDate"] = time.time()
        return json_response({"ARN": secret["ARN"], "Name": secret["Name"]})

    vid = new_uuid()
    now = time.time()
    secret["Versions"][vid] = {
        "SecretString": data.get("SecretString"),
        "SecretBinary": data.get("SecretBinary"),
        "CreatedDate": now,
        "Stages": [],
    }
    _apply_current_promotion(secret, vid)
    secret["LastChangedDate"] = now
    return json_response({"ARN": secret["ARN"], "Name": secret["Name"], "VersionId": vid})


def _describe_secret(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )

    result = {
        "ARN": secret["ARN"],
        "Name": secret["Name"],
        "Description": secret.get("Description", ""),
        "CreatedDate": secret["CreatedDate"],
        "LastChangedDate": secret["LastChangedDate"],
        "LastAccessedDate": secret.get("LastAccessedDate"),
        "Tags": secret.get("Tags", []),
        "VersionIdsToStages": _vid_to_stages(secret),
        "RotationEnabled": secret.get("RotationEnabled", False),
    }
    if secret.get("DeletedDate"):
        result["DeletedDate"] = secret["DeletedDate"]
    if secret.get("KmsKeyId"):
        result["KmsKeyId"] = secret["KmsKeyId"]
    if secret.get("RotationLambdaARN"):
        result["RotationLambdaARN"] = secret["RotationLambdaARN"]
    if secret.get("RotationRules"):
        result["RotationRules"] = secret["RotationRules"]
    if secret.get("ReplicationStatus"):
        result["ReplicationStatus"] = secret["ReplicationStatus"]
    return json_response(result)


def _put_secret_value(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    if secret.get("DeletedDate"):
        return error_response_json(
            "InvalidRequestException",
            "You can't perform this operation on the secret because it was marked for deletion.", 400,
        )

    vid = data.get("ClientRequestToken", new_uuid())
    stages = data.get("VersionStages", ["AWSCURRENT"])
    now = time.time()

    secret["Versions"][vid] = {
        "SecretString": data.get("SecretString"),
        "SecretBinary": data.get("SecretBinary"),
        "CreatedDate": now,
        "Stages": [],
    }

    if "AWSCURRENT" in stages:
        _apply_current_promotion(secret, vid)
    else:
        secret["Versions"][vid]["Stages"] = list(stages)

    secret["LastChangedDate"] = now
    return json_response({
        "ARN": secret["ARN"],
        "Name": secret["Name"],
        "VersionId": vid,
        "VersionStages": list(secret["Versions"][vid]["Stages"]),
    })


def _tag_resource(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    existing = {t["Key"]: t for t in secret.get("Tags", [])}
    for t in data.get("Tags", []):
        existing[t["Key"]] = t
    secret["Tags"] = list(existing.values())
    return json_response({})


def _untag_resource(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    keys_to_remove = set(data.get("TagKeys", []))
    secret["Tags"] = [t for t in secret.get("Tags", []) if t["Key"] not in keys_to_remove]
    return json_response({})


def _list_secret_version_ids(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )

    max_results = min(data.get("MaxResults", 100), 100)
    next_token = data.get("NextToken")

    all_vids = sorted(secret["Versions"].keys())
    start = 0
    if next_token:
        try:
            start = int(base64.b64decode(next_token))
        except Exception:
            pass

    page = all_vids[start:start + max_results]
    versions = []
    for vid in page:
        ver = secret["Versions"][vid]
        versions.append({
            "VersionId": vid,
            "VersionStages": list(ver["Stages"]),
            "CreatedDate": ver["CreatedDate"],
        })

    resp: dict = {
        "ARN": secret["ARN"],
        "Name": secret["Name"],
        "Versions": versions,
    }
    end = start + max_results
    if end < len(all_vids):
        resp["NextToken"] = base64.b64encode(str(end).encode()).decode()
    return json_response(resp)


def _rotate_secret(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    if secret.get("DeletedDate"):
        return error_response_json(
            "InvalidRequestException",
            "You can't perform this operation on the secret because it was marked for deletion.", 400,
        )

    lambda_arn = data.get("RotationLambdaARN") or secret.get("RotationLambdaARN")
    rotation_rules = data.get("RotationRules") or secret.get("RotationRules")

    if lambda_arn:
        secret["RotationLambdaARN"] = lambda_arn
    if rotation_rules:
        secret["RotationRules"] = rotation_rules
    secret["RotationEnabled"] = True

    vid = data.get("ClientRequestToken", new_uuid())
    now = time.time()

    curr_vid, curr_ver = _find_stage_version(secret, "AWSCURRENT")
    secret["Versions"][vid] = {
        "SecretString": curr_ver["SecretString"] if curr_ver else None,
        "SecretBinary": curr_ver["SecretBinary"] if curr_ver else None,
        "CreatedDate": now,
        "Stages": ["AWSPENDING"],
    }

    logger.info("RotateSecret stub for %s (Lambda: %s)", secret["Name"], lambda_arn)

    _apply_current_promotion(secret, vid)
    secret["LastChangedDate"] = now

    return json_response({"ARN": secret["ARN"], "Name": secret["Name"], "VersionId": vid})


def _get_random_password(data):
    length = data.get("PasswordLength", 32)
    if not (1 <= length <= 4096):
        return error_response_json(
            "InvalidParameterException",
            "PasswordLength must be between 1 and 4096.", 400,
        )

    exclude_chars = set(data.get("ExcludeCharacters", ""))
    exclude_numbers = data.get("ExcludeNumbers", False)
    exclude_punctuation = data.get("ExcludePunctuation", False)
    exclude_upper = data.get("ExcludeUppercase", False)
    exclude_lower = data.get("ExcludeLowercase", False)
    include_space = data.get("IncludeSpace", False)
    require_each = data.get("RequireEachIncludedType", True)

    pools: list[list[str]] = []
    all_chars: list[str] = []

    def _add_pool(chars):
        filtered = [c for c in chars if c not in exclude_chars]
        if filtered:
            pools.append(filtered)
            all_chars.extend(filtered)

    if not exclude_lower:
        _add_pool(string.ascii_lowercase)
    if not exclude_upper:
        _add_pool(string.ascii_uppercase)
    if not exclude_numbers:
        _add_pool(string.digits)
    if not exclude_punctuation:
        _add_pool(string.punctuation)
    if include_space:
        _add_pool([" "])

    if not all_chars:
        return error_response_json(
            "InvalidParameterException",
            "No characters available to generate password.", 400,
        )
    if require_each and len(pools) > length:
        return error_response_json(
            "InvalidParameterException",
            "PasswordLength too short to include required character types.", 400,
        )

    rng = stdlib_secrets.SystemRandom()
    pw: list[str] = []
    if require_each:
        for pool in pools:
            pw.append(stdlib_secrets.choice(pool))
        for _ in range(length - len(pw)):
            pw.append(stdlib_secrets.choice(all_chars))
        rng.shuffle(pw)
    else:
        for _ in range(length):
            pw.append(stdlib_secrets.choice(all_chars))

    return json_response({"RandomPassword": "".join(pw)})


def _replicate_secret_to_regions(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )

    for r in data.get("AddReplicaRegions", []):
        region = r.get("Region")
        secret["ReplicationStatus"].append({
            "Region": region,
            "Status": "InSync",
            "StatusMessage": "Replication succeeded (stub).",
        })

    logger.info(
        "ReplicateSecretToRegions stub for %s, regions=%s",
        secret["Name"],
        [r.get("Region") for r in data.get("AddReplicaRegions", [])],
    )

    return json_response({
        "ARN": secret["ARN"],
        "ReplicationStatus": secret["ReplicationStatus"],
    })


# ---------------------------------------------------------------------------
# Resource policies
# ---------------------------------------------------------------------------

def _put_resource_policy(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    policy = data.get("ResourcePolicy", "{}")
    _resource_policies[secret["ARN"]] = policy
    return json_response({"ARN": secret["ARN"], "Name": secret["Name"]})


def _get_resource_policy(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    policy = _resource_policies.get(secret["ARN"])
    result = {"ARN": secret["ARN"], "Name": secret["Name"]}
    if policy is not None:
        result["ResourcePolicy"] = policy
    return json_response(result)


def _delete_resource_policy(data):
    secret_id = data.get("SecretId")
    _, secret = _resolve(secret_id)
    if not secret:
        return error_response_json(
            "ResourceNotFoundException",
            "Secrets Manager can't find the specified secret.", 400,
        )
    _resource_policies.pop(secret["ARN"], None)
    return json_response({"ARN": secret["ARN"], "Name": secret["Name"]})


def _validate_resource_policy(data):
    return json_response({
        "PolicyValidationPassed": True,
        "ValidationErrors": [],
    })


def reset():
    _secrets.clear()
    _resource_policies.clear()
