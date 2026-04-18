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
    for arn, tags in svc._tags.items():
        yield arn, _normalise_list(tags)


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


# ── Operation handlers ────────────────────────────────────────────────────────

def _get_resources(data):
    tag_filters = data.get("TagFilters", [])
    type_filters = data.get("ResourceTypeFilters", [])

    if type_filters:
        type_prefixes = {tf.split(":")[0] for tf in type_filters}
        active = {k: v for k, v in _COLLECTORS.items() if k in type_prefixes}
        if not active:
            active = _COLLECTORS
    else:
        active = _COLLECTORS

    results = []
    for collector in active.values():
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

_HANDLERS = {
    "GetResources": _get_resources,
    "GetTagKeys":   _get_tag_keys,
    "GetTagValues": _get_tag_values,
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
