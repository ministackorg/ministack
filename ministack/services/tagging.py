"""
Resource Groups Tagging API emulator.
Phase 2: extends GetResources to 15 services and adds GetTagKeys, GetTagValues.
"""

import json
import logging
import os

logger = logging.getLogger("tagging")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")


# ── Tag format normalisation ──────────────────────────────────────────────────

def _normalise_flat(tag_dict):
    """Convert {k: v} flat dict to [{"Key": k, "Value": v}] list."""
    return [{"Key": k, "Value": v} for k, v in (tag_dict or {}).items()]


def _normalise_list(tag_list):
    """Pass-through [{"Key": k, "Value": v}] list (DynamoDB format)."""
    return tag_list or []


def _normalise_kms(tag_list):
    """Convert KMS [{"TagKey": k, "TagValue": v}] to standard format."""
    return [{"Key": t["TagKey"], "Value": t["TagValue"]} for t in (tag_list or [])]


def _normalise_ecs(tag_list):
    """Convert ECS [{"key": k, "value": v}] (lowercase) to standard format."""
    return [{"Key": t["key"], "Value": t["value"]} for t in (tag_list or [])]


# ── Per-service tag collectors ────────────────────────────────────────────────

def _collect_s3():
    import ministack.services.s3 as svc
    for name, tags in svc._bucket_tags.items():
        yield f"arn:aws:s3:::{name}", _normalise_flat(tags)


def _collect_lambda():
    import ministack.services.lambda_svc as svc
    for name, fn in svc._functions.items():
        arn = f"arn:aws:lambda:{REGION}:{_account()}:function:{name}"
        yield arn, _normalise_flat(fn.get("tags", {}))


def _collect_sqs():
    import ministack.services.sqs as svc
    for url, q in svc._queues.items():
        arn = q.get("attributes", {}).get("QueueArn", "")
        if arn:
            yield arn, _normalise_flat(q.get("tags", {}))


def _collect_sns():
    import ministack.services.sns as svc
    for arn, topic in svc._topics.items():
        yield arn, _normalise_flat(topic.get("tags", {}))


def _collect_dynamodb():
    import ministack.services.dynamodb as svc
    seen = set()
    # Tags set via TagResource are stored centrally, arn -> [{"Key":, "Value":}, ...]
    for arn, tags in svc._tags.items():
        seen.add(arn)
        yield arn, _normalise_list(tags)
    # CloudFormation-provisioned tables store tags on the table record as {k: v}.
    # Surface those too so CDK / Terraform-via-CFN resources show up.
    for _name, table in svc._tables.items():
        arn = table.get("TableArn")
        if not arn or arn in seen:
            continue
        cfn_tags = table.get("tags")
        if cfn_tags:
            yield arn, _normalise_flat(cfn_tags)


def _collect_eventbridge():
    import ministack.services.eventbridge as svc
    for arn, tags in svc._tags.items():
        yield arn, _normalise_flat(tags)


def _collect_kms():
    import ministack.services.kms as svc
    for key_id, rec in svc._keys.items():
        arn = f"arn:aws:kms:{REGION}:{_account()}:key/{key_id}"
        yield arn, _normalise_kms(rec.get("Tags", []))


def _collect_ecr():
    import ministack.services.ecr as svc
    for name, repo in svc._repositories.items():
        arn = f"arn:aws:ecr:{REGION}:{_account()}:repository/{name}"
        yield arn, _normalise_list(repo.get("tags", []))


def _collect_ecs():
    import ministack.services.ecs as svc
    for arn, tags in svc._tags.items():
        yield arn, _normalise_ecs(tags)


def _collect_glue():
    import ministack.services.glue as svc
    for arn, tags in svc._tags.items():
        yield arn, _normalise_flat(tags)


def _collect_cognito():
    import ministack.services.cognito as svc
    for pool_id, pool in svc._user_pools.items():
        arn = f"arn:aws:cognito-idp:{REGION}:{_account()}:userpool/{pool_id}"
        yield arn, _normalise_flat(pool.get("UserPoolTags", {}))
    for pool_id, tags in svc._identity_tags.items():
        arn = f"arn:aws:cognito-identity:{REGION}:{_account()}:identitypool/{pool_id}"
        yield arn, _normalise_flat(tags)


def _collect_appsync():
    import ministack.services.appsync as svc
    for arn, tags in svc._tags.items():
        yield arn, _normalise_flat(tags)


def _collect_scheduler():
    import ministack.services.scheduler as svc
    for arn, tags in svc._tags.items():
        yield arn, _normalise_flat(tags)


def _collect_cloudfront():
    import ministack.services.cloudfront as svc
    for arn, tags in svc._tags.items():
        yield arn, _normalise_list(tags)


def _collect_efs():
    import ministack.services.efs as svc
    for fs_id, fs in svc._file_systems.items():
        arn = f"arn:aws:elasticfilesystem:{REGION}:{_account()}:file-system/{fs_id}"
        yield arn, _normalise_list(fs.get("Tags", []))
    for ap_id, ap in svc._access_points.items():
        arn = f"arn:aws:elasticfilesystem:{REGION}:{_account()}:access-point/{ap_id}"
        yield arn, _normalise_list(ap.get("Tags", []))


# ResourceTypeFilter prefix -> collector
_COLLECTORS = {
    # Phase 1
    "s3":                _collect_s3,
    "lambda":            _collect_lambda,
    "sqs":               _collect_sqs,
    "sns":               _collect_sns,
    "dynamodb":          _collect_dynamodb,
    "events":            _collect_eventbridge,
    # Phase 2
    "kms":               _collect_kms,
    "ecr":               _collect_ecr,
    "ecs":               _collect_ecs,
    "glue":              _collect_glue,
    "cognito-idp":       _collect_cognito,
    "cognito-identity":  _collect_cognito,
    "appsync":           _collect_appsync,
    "scheduler":         _collect_scheduler,
    "cloudfront":        _collect_cloudfront,
    "elasticfilesystem": _collect_efs,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _account():
    from ministack.core.responses import get_account_id
    return get_account_id()


def _matches_type_filters(arn, type_filters):
    if not type_filters:
        return True
    for tf in type_filters:
        svc_prefix = tf.split(":")[0]
        if f"::{svc_prefix}:" in arn or f":{svc_prefix}:" in arn:
            return True
    return False


def _matches_tag_filters(tags, tag_filters):
    """AND across filter keys, OR across values within a key."""
    if not tag_filters:
        return True
    tag_map = {t["Key"]: t["Value"] for t in tags}
    for f in tag_filters:
        key = f.get("Key", "")
        values = f.get("Values", [])
        if key not in tag_map:
            return False
        if values and tag_map[key] not in values:
            return False
    return True


def _service_key_from_arn(arn):
    """Extract service segment from ARN (e.g. 'arn:aws:s3:::...' → 's3')."""
    parts = arn.split(":")
    return parts[2] if len(parts) >= 3 else ""


# ── Per-service tag writers ───────────────────────────────────────────────────

def _write_s3(arn, tags):
    import ministack.services.s3 as svc
    name = arn.split(":::")[-1]
    svc._bucket_tags.setdefault(name, {}).update(tags)


def _write_lambda(arn, tags):
    import ministack.services.lambda_svc as svc
    name = arn.split("function:")[-1]
    if name in svc._functions:
        svc._functions[name].setdefault("tags", {}).update(tags)


def _write_sqs(arn, tags):
    import ministack.services.sqs as svc
    for q in svc._queues.values():
        if q.get("attributes", {}).get("QueueArn") == arn:
            q.setdefault("tags", {}).update(tags)
            break


def _write_sns(arn, tags):
    import ministack.services.sns as svc
    if arn in svc._topics:
        svc._topics[arn].setdefault("tags", {}).update(tags)


def _write_dynamodb(arn, tags):
    import ministack.services.dynamodb as svc
    existing = {t["Key"]: t["Value"] for t in svc._tags.get(arn, [])}
    existing.update(tags)
    svc._tags[arn] = [{"Key": k, "Value": v} for k, v in existing.items()]


def _write_eventbridge(arn, tags):
    import ministack.services.eventbridge as svc
    svc._tags.setdefault(arn, {}).update(tags)


def _write_kms(arn, tags):
    import ministack.services.kms as svc
    key_id = arn.split("/")[-1]
    if key_id in svc._keys:
        existing = {t["TagKey"]: t["TagValue"] for t in svc._keys[key_id].get("Tags", [])}
        existing.update(tags)
        svc._keys[key_id]["Tags"] = [{"TagKey": k, "TagValue": v} for k, v in existing.items()]


def _write_ecr(arn, tags):
    import ministack.services.ecr as svc
    name = arn.split("repository/")[-1]
    if name in svc._repositories:
        existing = {t["Key"]: t["Value"] for t in svc._repositories[name].get("tags", [])}
        existing.update(tags)
        svc._repositories[name]["tags"] = [{"Key": k, "Value": v} for k, v in existing.items()]


def _write_ecs(arn, tags):
    import ministack.services.ecs as svc
    existing = {t["key"]: t["value"] for t in svc._tags.get(arn, [])}
    existing.update(tags)
    svc._tags[arn] = [{"key": k, "value": v} for k, v in existing.items()]


def _write_glue(arn, tags):
    import ministack.services.glue as svc
    svc._tags.setdefault(arn, {}).update(tags)


def _write_cognito_idp(arn, tags):
    import ministack.services.cognito as svc
    pool_id = arn.split("userpool/")[-1]
    if pool_id in svc._user_pools:
        svc._user_pools[pool_id].setdefault("UserPoolTags", {}).update(tags)


def _write_cognito_identity(arn, tags):
    import ministack.services.cognito as svc
    pool_id = arn.split("identitypool/")[-1]
    svc._identity_tags.setdefault(pool_id, {}).update(tags)


def _write_appsync(arn, tags):
    import ministack.services.appsync as svc
    svc._tags.setdefault(arn, {}).update(tags)


def _write_scheduler(arn, tags):
    import ministack.services.scheduler as svc
    svc._tags.setdefault(arn, {}).update(tags)


def _write_cloudfront(arn, tags):
    import ministack.services.cloudfront as svc
    existing = {t["Key"]: t["Value"] for t in svc._tags.get(arn, [])}
    existing.update(tags)
    svc._tags[arn] = [{"Key": k, "Value": v} for k, v in existing.items()]


def _write_efs(arn, tags):
    import ministack.services.efs as svc
    if ":file-system/" in arn:
        resource = svc._file_systems.get(arn.split("file-system/")[-1])
    else:
        resource = svc._access_points.get(arn.split("access-point/")[-1])
    if resource is not None:
        existing = {t["Key"]: t["Value"] for t in resource.get("Tags", [])}
        existing.update(tags)
        resource["Tags"] = [{"Key": k, "Value": v} for k, v in existing.items()]


_WRITERS = {
    "s3": _write_s3, "lambda": _write_lambda, "sqs": _write_sqs,
    "sns": _write_sns, "dynamodb": _write_dynamodb, "events": _write_eventbridge,
    "kms": _write_kms, "ecr": _write_ecr, "ecs": _write_ecs,
    "glue": _write_glue, "cognito-idp": _write_cognito_idp,
    "cognito-identity": _write_cognito_identity, "appsync": _write_appsync,
    "scheduler": _write_scheduler, "cloudfront": _write_cloudfront,
    "elasticfilesystem": _write_efs,
}


# ── Per-service tag removers ──────────────────────────────────────────────────

def _remove_s3(arn, keys):
    import ministack.services.s3 as svc
    tags = svc._bucket_tags.get(arn.split(":::")[-1], {})
    for k in keys:
        tags.pop(k, None)


def _remove_lambda(arn, keys):
    import ministack.services.lambda_svc as svc
    name = arn.split("function:")[-1]
    if name in svc._functions:
        tags = svc._functions[name].get("tags", {})
        for k in keys:
            tags.pop(k, None)


def _remove_sqs(arn, keys):
    import ministack.services.sqs as svc
    for q in svc._queues.values():
        if q.get("attributes", {}).get("QueueArn") == arn:
            tags = q.get("tags", {})
            for k in keys:
                tags.pop(k, None)
            break


def _remove_sns(arn, keys):
    import ministack.services.sns as svc
    if arn in svc._topics:
        tags = svc._topics[arn].get("tags", {})
        for k in keys:
            tags.pop(k, None)


def _remove_dynamodb(arn, keys):
    import ministack.services.dynamodb as svc
    svc._tags[arn] = [t for t in svc._tags.get(arn, []) if t["Key"] not in keys]


def _remove_eventbridge(arn, keys):
    import ministack.services.eventbridge as svc
    tags = svc._tags.get(arn, {})
    for k in keys:
        tags.pop(k, None)


def _remove_kms(arn, keys):
    import ministack.services.kms as svc
    key_id = arn.split("/")[-1]
    if key_id in svc._keys:
        svc._keys[key_id]["Tags"] = [
            t for t in svc._keys[key_id].get("Tags", []) if t["TagKey"] not in keys
        ]


def _remove_ecr(arn, keys):
    import ministack.services.ecr as svc
    name = arn.split("repository/")[-1]
    if name in svc._repositories:
        svc._repositories[name]["tags"] = [
            t for t in svc._repositories[name].get("tags", []) if t["Key"] not in keys
        ]


def _remove_ecs(arn, keys):
    import ministack.services.ecs as svc
    svc._tags[arn] = [t for t in svc._tags.get(arn, []) if t["key"] not in keys]


def _remove_glue(arn, keys):
    import ministack.services.glue as svc
    tags = svc._tags.get(arn, {})
    for k in keys:
        tags.pop(k, None)


def _remove_cognito_idp(arn, keys):
    import ministack.services.cognito as svc
    pool_id = arn.split("userpool/")[-1]
    if pool_id in svc._user_pools:
        tags = svc._user_pools[pool_id].get("UserPoolTags", {})
        for k in keys:
            tags.pop(k, None)


def _remove_cognito_identity(arn, keys):
    import ministack.services.cognito as svc
    pool_id = arn.split("identitypool/")[-1]
    tags = svc._identity_tags.get(pool_id, {})
    for k in keys:
        tags.pop(k, None)


def _remove_appsync(arn, keys):
    import ministack.services.appsync as svc
    tags = svc._tags.get(arn, {})
    for k in keys:
        tags.pop(k, None)


def _remove_scheduler(arn, keys):
    import ministack.services.scheduler as svc
    tags = svc._tags.get(arn, {})
    for k in keys:
        tags.pop(k, None)


def _remove_cloudfront(arn, keys):
    import ministack.services.cloudfront as svc
    svc._tags[arn] = [t for t in svc._tags.get(arn, []) if t["Key"] not in keys]


def _remove_efs(arn, keys):
    import ministack.services.efs as svc
    if ":file-system/" in arn:
        resource = svc._file_systems.get(arn.split("file-system/")[-1])
    else:
        resource = svc._access_points.get(arn.split("access-point/")[-1])
    if resource is not None:
        resource["Tags"] = [t for t in resource.get("Tags", []) if t["Key"] not in keys]


_REMOVERS = {
    "s3": _remove_s3, "lambda": _remove_lambda, "sqs": _remove_sqs,
    "sns": _remove_sns, "dynamodb": _remove_dynamodb, "events": _remove_eventbridge,
    "kms": _remove_kms, "ecr": _remove_ecr, "ecs": _remove_ecs,
    "glue": _remove_glue, "cognito-idp": _remove_cognito_idp,
    "cognito-identity": _remove_cognito_identity, "appsync": _remove_appsync,
    "scheduler": _remove_scheduler, "cloudfront": _remove_cloudfront,
    "elasticfilesystem": _remove_efs,
}


# ── Operation handlers ────────────────────────────────────────────────────────

def _get_resources(data):
    tag_filters = data.get("TagFilters", [])
    type_filters = data.get("ResourceTypeFilters", [])

    if type_filters:
        type_prefixes = {tf.split(":")[0] for tf in type_filters}
        active = {k: v for k, v in _COLLECTORS.items() if k in type_prefixes}
        # If none of the requested prefixes match a supported collector, return
        # an empty result — matching AWS (filter narrows the universe, it
        # never broadens it back to "everything").
    else:
        active = _COLLECTORS

    results = []
    for collector in dict.fromkeys(active.values()):
        try:
            for arn, tags in collector():
                if not _matches_type_filters(arn, type_filters):
                    continue
                if not _matches_tag_filters(tags, tag_filters):
                    continue
                results.append({"ResourceARN": arn, "Tags": tags})
        except Exception:
            pass  # service not yet initialised — skip silently

    return 200, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps({
        "ResourceTagMappingList": results,
        "PaginationToken": "",
    }).encode()


def _get_tag_keys(data):
    keys = set()
    for collector in _COLLECTORS.values():
        try:
            for _arn, tags in collector():
                for t in tags:
                    keys.add(t["Key"])
        except Exception:
            pass
    return 200, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps({
        "TagKeys": sorted(keys),
        "PaginationToken": "",
    }).encode()


def _get_tag_values(data):
    target_key = data.get("Key", "")
    values = set()
    for collector in _COLLECTORS.values():
        try:
            for _arn, tags in collector():
                for t in tags:
                    if t["Key"] == target_key:
                        values.add(t["Value"])
        except Exception:
            pass
    return 200, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps({
        "TagValues": sorted(values),
        "PaginationToken": "",
    }).encode()


# ── Entry point ───────────────────────────────────────────────────────────────

def _tag_resources(data):
    arn_list = data.get("ResourceARNList", [])
    tags = data.get("Tags", {})
    failed = {}

    for arn in arn_list:
        svc_key = _service_key_from_arn(arn)
        writer = _WRITERS.get(svc_key)
        if writer is None:
            failed[arn] = {
                "ErrorCode": "InternalServiceException",
                "ErrorMessage": f"Unsupported resource type: {svc_key}",
                "StatusCode": 501,
            }
            continue
        try:
            writer(arn, tags)
        except Exception as exc:
            failed[arn] = {
                "ErrorCode": "InternalServiceException",
                "ErrorMessage": str(exc),
                "StatusCode": 500,
            }

    return 200, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps({
        "FailedResourcesMap": failed,
    }).encode()


def _untag_resources(data):
    arn_list = data.get("ResourceARNList", [])
    tag_keys = data.get("TagKeys", [])
    failed = {}

    for arn in arn_list:
        svc_key = _service_key_from_arn(arn)
        remover = _REMOVERS.get(svc_key)
        if remover is None:
            failed[arn] = {
                "ErrorCode": "InternalServiceException",
                "ErrorMessage": f"Unsupported resource type: {svc_key}",
                "StatusCode": 501,
            }
            continue
        try:
            remover(arn, tag_keys)
        except Exception as exc:
            failed[arn] = {
                "ErrorCode": "InternalServiceException",
                "ErrorMessage": str(exc),
                "StatusCode": 500,
            }

    return 200, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps({
        "FailedResourcesMap": failed,
    }).encode()


_HANDLERS = {
    "GetResources":   _get_resources,
    "GetTagKeys":     _get_tag_keys,
    "GetTagValues":   _get_tag_values,
    "TagResources":   _tag_resources,
    "UntagResources": _untag_resources,
}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return 400, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps({
            "__type": "SerializationException",
            "message": "Invalid JSON",
        }).encode()

    handler = _HANDLERS.get(action)
    if not handler:
        return 400, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps({
            "__type": "InvalidRequestException",
            "message": f"Unknown action: {action}",
        }).encode()

    return handler(data)
