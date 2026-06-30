"""
Resource Groups Tagging API emulator.

Supports the five operations of the real ResourceGroupsTaggingAPI_20170126
target (`GetResources`, `GetTagKeys`, `GetTagValues`, `TagResources`,
`UntagResources`) across the services listed in ``_COLLECTORS`` / ``_WRITERS``.

Architecture:
- **Collectors** (one per service) yield ``(arn, [{"Key":..., "Value":...}])``
  tuples by reading each service module's tag state. Used by GetResources.
- **Writers** apply a ``{key: value}`` dict onto a service's tag state for a
  given ARN. Used by TagResources. Writers raise ``_ResourceNotFound`` when
  the ARN points at a resource that does not exist in the caller's account;
  the entry point catches it and surfaces the ARN in ``FailedResourcesMap``
  with ``ResourceNotFound``, matching AWS.
- **Removers** do the inverse for UntagResources.

Each service keeps its own tag format (S3 flat dict, DynamoDB key/value list,
KMS TagKey/TagValue, ECS lowercase key/value, …); the helpers in this file
normalise to the standard ``[{"Key":..., "Value":...}]`` shape on read and
denormalise on write.
"""

import json
import logging

from ministack.core.arn import Arn, ArnParseError, parse_arn
from ministack.core.responses import get_region

logger = logging.getLogger("tagging")


_GLOBAL_RESOURCE_SERVICES = {"cloudfront", "s3"}


class _ResourceNotFound(Exception):
    """Raised by a writer/remover when the target ARN refers to a resource
    that does not exist in the caller's account. Caught by the TagResources /
    UntagResources entry points and surfaced in ``FailedResourcesMap`` with
    ``ResourceNotFound`` (matches real AWS behaviour)."""


class _InvalidResourceArn(Exception):
    """Raised when an ARN cannot be parsed or is not valid for this operation."""


class _WrongAccountArn(Exception):
    """Raised when a parsed ARN belongs to a different account."""


class _WrongRegionArn(Exception):
    """Raised when a parsed ARN belongs to a different region."""


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
        arn = f"arn:aws:lambda:{get_region()}:{_account()}:function:{name}"
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
        arn = f"arn:aws:kms:{get_region()}:{_account()}:key/{key_id}"
        yield arn, _normalise_kms(rec.get("Tags", []))


def _collect_ecr():
    import ministack.services.ecr as svc
    for name, repo in svc._repositories.items():
        arn = f"arn:aws:ecr:{get_region()}:{_account()}:repository/{name}"
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
        arn = f"arn:aws:cognito-idp:{get_region()}:{_account()}:userpool/{pool_id}"
        yield arn, _normalise_flat(pool.get("UserPoolTags", {}))
    for pool_id, tags in svc._identity_tags.items():
        arn = f"arn:aws:cognito-identity:{get_region()}:{_account()}:identitypool/{pool_id}"
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
        arn = f"arn:aws:elasticfilesystem:{get_region()}:{_account()}:file-system/{fs_id}"
        yield arn, _normalise_list(fs.get("Tags", []))
    for ap_id, ap in svc._access_points.items():
        arn = f"arn:aws:elasticfilesystem:{get_region()}:{_account()}:access-point/{ap_id}"
        yield arn, _normalise_list(ap.get("Tags", []))


def _collect_backup():
    import ministack.services.backup as svc
    for name, v in svc._vaults.items():
        arn = f"arn:aws:backup:{get_region()}:{_account()}:backup-vault:{name}"
        yield arn, _normalise_flat(v.get("BackupVaultTags", {}))
    for pid, p in svc._plans.items():
        arn = f"arn:aws:backup:{get_region()}:{_account()}:backup-plan:{pid}"
        yield arn, _normalise_flat(p.get("Tags", {}))


def _collect_elasticache():
    import ministack.services.elasticache as svc
    for arn, tags in svc._tags.items():
        yield arn, _normalise_list(tags)


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
    "backup":            _collect_backup,
    "elasticache":       _collect_elasticache,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _account():
    from ministack.core.responses import get_account_id
    return get_account_id()


def _matches_type_filters(arn, type_filters):
    if not type_filters:
        return True
    parts = arn.split(":", 5)
    arn_service = parts[2] if len(parts) > 2 else ""
    resource = parts[5] if len(parts) > 5 else ""
    if "/" in resource:
        arn_resource_type = resource.split("/", 1)[0]
    else:
        arn_resource_type = resource.split(":", 1)[0]

    for tf in type_filters:
        tf_parts = tf.split(":", 1)
        svc_prefix = tf_parts[0]
        if svc_prefix != arn_service:
            continue
        if len(tf_parts) == 1 or not tf_parts[1] or tf_parts[1] == arn_resource_type:
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


def _parse_resource_arn(arn):
    try:
        spec = parse_arn(arn)
    except ArnParseError as exc:
        raise _InvalidResourceArn(f"{arn} is not a valid AmazonResourceName (ARN)") from exc
    if (
        spec.service in _COLLECTORS
        and spec.service not in _GLOBAL_RESOURCE_SERVICES
        and (not spec.region or not spec.account_id)
    ):
        raise _InvalidResourceArn(f"{arn} is not a valid AmazonResourceName (ARN)")
    return spec


def _parse_resource_arn_list(arn_list):
    return [(arn, _parse_resource_arn(arn)) for arn in arn_list]


def _resource_tail(spec: Arn, arn: str, prefix: str) -> str:
    if not spec.resource.startswith(prefix):
        raise _ResourceNotFound(arn)
    tail = spec.resource[len(prefix):]
    if not tail:
        raise _ResourceNotFound(arn)
    return tail


def _s3_bucket_name(spec: Arn, arn: str) -> str:
    if spec.region or spec.account_id or not spec.resource or "/" in spec.resource:
        raise _ResourceNotFound(arn)
    return spec.resource


def _require_resource_scope(spec: Arn, arn: str) -> None:
    if spec.service == "s3":
        if spec.region or spec.account_id:
            raise _ResourceNotFound(arn)
        return
    if spec.service == "cloudfront":
        if spec.region or spec.account_id != _account():
            raise _WrongAccountArn(arn)
        return
    if spec.region and spec.region != get_region():
        raise _WrongRegionArn(arn)
    if spec.account_id != _account():
        raise _WrongAccountArn(arn)


def _reject_foreign_region_arn(spec: Arn, arn: str, action: str) -> None:
    if spec.service in _COLLECTORS and spec.region and spec.region != get_region():
        raise _WrongRegionArn(
            f"Region in the ARN {arn} does not match with the region in which {action} API is invoked"
        )


def _json(data, status=200):
    return status, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps(data).encode()


def _invalid_parameter(message):
    return 400, {
        "Content-Type": "application/x-amz-json-1.1",
        "x-amzn-errortype": "InvalidParameterException",
    }, json.dumps({
        "__type": "InvalidParameterException",
        "message": message,
    }).encode()


def _failed_resource(error_code, message, status_code):
    return {
        "ErrorCode": error_code,
        "ErrorMessage": message,
        "StatusCode": status_code,
    }


def _dynamodb_table_name(spec: Arn, arn: str) -> str:
    return _resource_tail(spec, arn, "table/")


def _eventbridge_resource_exists(spec: Arn, arn: str) -> bool:
    import ministack.services.eventbridge as svc
    resource = spec.resource
    if resource.startswith("event-bus/"):
        name = _resource_tail(spec, arn, "event-bus/")
        if name == "default":
            svc._ensure_default_bus()
        return name in svc._event_buses
    if resource.startswith("rule/"):
        tail = _resource_tail(spec, arn, "rule/")
        if "/" in tail:
            bus, name = tail.rsplit("/", 1)
        else:
            bus, name = "default", tail
        return svc._rule_key(name, bus) in svc._rules
    if resource.startswith("archive/"):
        return _resource_tail(spec, arn, "archive/") in svc._archives
    if resource.startswith("replay/"):
        return _resource_tail(spec, arn, "replay/") in svc._replays
    if resource.startswith("endpoint/"):
        return _resource_tail(spec, arn, "endpoint/") in svc._endpoints
    if resource.startswith("connection/"):
        return _resource_tail(spec, arn, "connection/") in svc._connections
    if resource.startswith("api-destination/"):
        return _resource_tail(spec, arn, "api-destination/") in svc._api_destinations
    return False


def _ecs_resource_exists(arn: str) -> bool:
    import ministack.services.ecs as svc
    return (
        any(c.get("clusterArn") == arn for c in svc._clusters.values())
        or any(td.get("taskDefinitionArn") == arn for td in svc._task_defs.values())
        or any(s.get("serviceArn") == arn for s in svc._services.values())
        or any(t.get("taskArn") == arn for t in svc._tasks.values())
        or any(cp.get("capacityProviderArn") == arn for cp in svc._capacity_providers.values())
    )


def _glue_resource_exists(spec: Arn, arn: str) -> bool:
    import ministack.services.glue as svc
    resource = spec.resource
    if resource.startswith("database/"):
        return _resource_tail(spec, arn, "database/") in svc._databases
    if resource.startswith("table/"):
        return _resource_tail(spec, arn, "table/") in svc._tables
    if resource.startswith("crawler/"):
        return _resource_tail(spec, arn, "crawler/") in svc._crawlers
    if resource.startswith("job/"):
        return _resource_tail(spec, arn, "job/") in svc._jobs
    if resource.startswith("connection/"):
        return _resource_tail(spec, arn, "connection/") in svc._connections
    if resource.startswith("trigger/"):
        return _resource_tail(spec, arn, "trigger/") in svc._triggers
    if resource.startswith("workflow/"):
        return _resource_tail(spec, arn, "workflow/") in svc._workflows
    return arn in svc._tags


def _appsync_resource_exists(spec: Arn, arn: str) -> bool:
    import ministack.services.appsync as svc
    if not spec.resource.startswith("apis/"):
        return False
    return _resource_tail(spec, arn, "apis/") in svc._apis


def _scheduler_resource_exists(spec: Arn, arn: str) -> bool:
    import ministack.services.scheduler as svc
    if spec.resource.startswith("schedule/"):
        tail = _resource_tail(spec, arn, "schedule/")
        parts = tail.split("/", 1)
        if len(parts) != 2:
            return False
        return f"{parts[0]}/{parts[1]}" in svc._schedules
    if spec.resource.startswith("schedule-group/"):
        name = _resource_tail(spec, arn, "schedule-group/")
        if name == "default":
            svc._ensure_default_group()
        return name in svc._schedule_groups
    return False


def _cloudfront_resource_exists(spec: Arn, arn: str) -> bool:
    import ministack.services.cloudfront as svc
    if spec.resource.startswith("distribution/"):
        return _resource_tail(spec, arn, "distribution/") in svc._distributions
    if spec.resource.startswith("function/"):
        return _resource_tail(spec, arn, "function/") in svc._functions
    if spec.resource.startswith("key-value-store/"):
        return _resource_tail(spec, arn, "key-value-store/") in svc._kvstores
    return arn in svc._tags


# ── Per-service tag writers ───────────────────────────────────────────────────

def _write_s3(spec, arn, tags):
    import ministack.services.s3 as svc
    name = _s3_bucket_name(spec, arn)
    if name not in svc._buckets:
        raise _ResourceNotFound(arn)
    svc._bucket_tags.setdefault(name, {}).update(tags)


def _write_lambda(spec, arn, tags):
    """Merge ``tags`` into the Lambda function's ``tags`` field.

    Raises ``_ResourceNotFound`` if the function does not exist in the caller's
    account (AWS returns InvalidParameterException in that case)."""
    import ministack.services.lambda_svc as svc
    name = _resource_tail(spec, arn, "function:")
    base_name = name.split(":", 1)[0]
    func = svc._functions.get(base_name)
    if func is None:
        raise _ResourceNotFound(arn)
    func.setdefault("tags", {}).update(tags)


def _write_sqs(_spec, arn, tags):
    """Merge ``tags`` into the SQS queue keyed by ``QueueArn``.

    Raises ``_ResourceNotFound`` if no queue in the caller's account matches."""
    import ministack.services.sqs as svc
    for q in svc._queues.values():
        if q.get("attributes", {}).get("QueueArn") == arn:
            q.setdefault("tags", {}).update(tags)
            return
    raise _ResourceNotFound(arn)


def _write_sns(_spec, arn, tags):
    """Merge ``tags`` into the SNS topic at ``arn``.

    Raises ``_ResourceNotFound`` if the topic does not exist in the caller's
    account."""
    import ministack.services.sns as svc
    if arn not in svc._topics:
        raise _ResourceNotFound(arn)
    svc._topics[arn].setdefault("tags", {}).update(tags)


def _write_dynamodb(spec, arn, tags):
    import ministack.services.dynamodb as svc
    table_name = _dynamodb_table_name(spec, arn)
    if table_name not in svc._tables:
        raise _ResourceNotFound(arn)
    existing = {t["Key"]: t["Value"] for t in svc._tags.get(arn, [])}
    existing.update(tags)
    svc._tags[arn] = [{"Key": k, "Value": v} for k, v in existing.items()]


def _write_eventbridge(spec, arn, tags):
    import ministack.services.eventbridge as svc
    if not _eventbridge_resource_exists(spec, arn):
        raise _ResourceNotFound(arn)
    svc._tags.setdefault(arn, {}).update(tags)


def _write_kms(spec, arn, tags):
    import ministack.services.kms as svc
    _resource_tail(spec, arn, "key/")
    key = svc._resolve_key(arn)
    if key is None:
        raise _ResourceNotFound(arn)
    existing = {t["TagKey"]: t["TagValue"] for t in key.get("Tags", [])}
    existing.update(tags)
    key["Tags"] = [{"TagKey": k, "TagValue": v} for k, v in existing.items()]


def _write_ecr(spec, arn, tags):
    import ministack.services.ecr as svc
    name = _resource_tail(spec, arn, "repository/")
    if name not in svc._repositories:
        raise _ResourceNotFound(arn)
    existing = {t["Key"]: t["Value"] for t in svc._repositories[name].get("tags", [])}
    existing.update(tags)
    svc._repositories[name]["tags"] = [{"Key": k, "Value": v} for k, v in existing.items()]


def _write_ecs(_spec, arn, tags):
    import ministack.services.ecs as svc
    if not _ecs_resource_exists(arn):
        raise _ResourceNotFound(arn)
    existing = {t["key"]: t["value"] for t in svc._tags.get(arn, [])}
    existing.update(tags)
    svc._tags[arn] = [{"key": k, "value": v} for k, v in existing.items()]


def _write_glue(spec, arn, tags):
    import ministack.services.glue as svc
    if not _glue_resource_exists(spec, arn):
        raise _ResourceNotFound(arn)
    svc._tags.setdefault(arn, {}).update(tags)


def _write_cognito_idp(spec, arn, tags):
    """Merge ``tags`` into the Cognito user pool's ``UserPoolTags`` field.

    Raises ``_ResourceNotFound`` if the pool does not exist in the caller's
    account."""
    import ministack.services.cognito as svc
    pool_id = _resource_tail(spec, arn, "userpool/")
    if pool_id not in svc._user_pools:
        raise _ResourceNotFound(arn)
    svc._user_pools[pool_id].setdefault("UserPoolTags", {}).update(tags)


def _write_cognito_identity(spec, arn, tags):
    import ministack.services.cognito as svc
    pool_id = _resource_tail(spec, arn, "identitypool/")
    if pool_id not in svc._identity_pools:
        raise _ResourceNotFound(arn)
    svc._identity_tags.setdefault(pool_id, {}).update(tags)


def _write_appsync(spec, arn, tags):
    import ministack.services.appsync as svc
    if not _appsync_resource_exists(spec, arn):
        raise _ResourceNotFound(arn)
    svc._tags.setdefault(arn, {}).update(tags)


def _write_scheduler(spec, arn, tags):
    import ministack.services.scheduler as svc
    if not _scheduler_resource_exists(spec, arn):
        raise _ResourceNotFound(arn)
    svc._tags.setdefault(arn, {}).update(tags)


def _write_cloudfront(spec, arn, tags):
    import ministack.services.cloudfront as svc
    if not _cloudfront_resource_exists(spec, arn):
        raise _ResourceNotFound(arn)
    existing = {t["Key"]: t["Value"] for t in svc._tags.get(arn, [])}
    existing.update(tags)
    svc._tags[arn] = [{"Key": k, "Value": v} for k, v in existing.items()]


def _write_efs(spec, arn, tags):
    import ministack.services.efs as svc
    if spec.resource.startswith("file-system/"):
        resource = svc._file_systems.get(_resource_tail(spec, arn, "file-system/"))
    else:
        resource = svc._access_points.get(_resource_tail(spec, arn, "access-point/"))
    if resource is None:
        raise _ResourceNotFound(arn)
    existing = {t["Key"]: t["Value"] for t in resource.get("Tags", [])}
    existing.update(tags)
    resource["Tags"] = [{"Key": k, "Value": v} for k, v in existing.items()]


def _write_backup(spec, arn, tags):
    import ministack.services.backup as svc
    if spec.resource.startswith("backup-vault:"):
        name = _resource_tail(spec, arn, "backup-vault:")
        v = svc._vaults.get(name)
        if v is None:
            raise _ResourceNotFound(arn)
        v.setdefault("BackupVaultTags", {}).update(tags)
    elif spec.resource.startswith("backup-plan:"):
        pid = _resource_tail(spec, arn, "backup-plan:")
        p = svc._plans.get(pid)
        if p is None:
            raise _ResourceNotFound(arn)
        p.setdefault("Tags", {}).update(tags)
    else:
        raise _ResourceNotFound(arn)


def _resolve_elasticache_resource(svc, arn):
    """Check that an ElastiCache ARN refers to an existing resource."""
    resource_part = arn.split(":", 5)[-1] if arn.count(":") >= 5 else ""
    if ":" not in resource_part:
        raise _ResourceNotFound(arn)
    resource_type, resource_id = resource_part.split(":", 1)
    match resource_type:
        case "cluster":
            store = svc._clusters
        case "replicationgroup":
            store = svc._replication_groups
        case "subnetgroup":
            store = svc._subnet_groups
        case "parametergroup":
            store = svc._param_groups
        case "snapshot":
            store = svc._snapshots
        case "user":
            store = svc._users
        case "usergroup":
            store = svc._user_groups
        case _:
            raise _ResourceNotFound(arn)
    if resource_id not in store:
        raise _ResourceNotFound(arn)


def _write_elasticache(_spec, arn, tags):
    import ministack.services.elasticache as svc
    _resolve_elasticache_resource(svc, arn)
    svc._merge_tags_for_arn(arn, [{"Key": k, "Value": v} for k, v in tags.items()])


_WRITERS = {
    "s3": _write_s3, "lambda": _write_lambda, "sqs": _write_sqs,
    "sns": _write_sns, "dynamodb": _write_dynamodb, "events": _write_eventbridge,
    "kms": _write_kms, "ecr": _write_ecr, "ecs": _write_ecs,
    "glue": _write_glue, "cognito-idp": _write_cognito_idp,
    "cognito-identity": _write_cognito_identity, "appsync": _write_appsync,
    "scheduler": _write_scheduler, "cloudfront": _write_cloudfront,
    "elasticfilesystem": _write_efs, "backup": _write_backup,
    "elasticache": _write_elasticache,
}


# ── Per-service tag removers ──────────────────────────────────────────────────

def _remove_s3(spec, arn, keys):
    import ministack.services.s3 as svc
    name = _s3_bucket_name(spec, arn)
    if name not in svc._buckets:
        raise _ResourceNotFound(arn)
    tags = svc._bucket_tags.get(name, {})
    for k in keys:
        tags.pop(k, None)


def _remove_lambda(spec, arn, keys):
    """Remove ``keys`` from the Lambda function's ``tags`` field.

    Raises ``_ResourceNotFound`` if the function does not exist."""
    import ministack.services.lambda_svc as svc
    name = _resource_tail(spec, arn, "function:")
    base_name = name.split(":", 1)[0]
    func = svc._functions.get(base_name)
    if func is None:
        raise _ResourceNotFound(arn)
    tags = func.get("tags", {})
    for k in keys:
        tags.pop(k, None)


def _remove_sqs(_spec, arn, keys):
    """Remove ``keys`` from the SQS queue's tags. Raises ``_ResourceNotFound``."""
    import ministack.services.sqs as svc
    for q in svc._queues.values():
        if q.get("attributes", {}).get("QueueArn") == arn:
            tags = q.get("tags", {})
            for k in keys:
                tags.pop(k, None)
            return
    raise _ResourceNotFound(arn)


def _remove_sns(_spec, arn, keys):
    """Remove ``keys`` from the SNS topic's tags. Raises ``_ResourceNotFound``."""
    import ministack.services.sns as svc
    if arn not in svc._topics:
        raise _ResourceNotFound(arn)
    tags = svc._topics[arn].get("tags", {})
    for k in keys:
        tags.pop(k, None)


def _remove_dynamodb(spec, arn, keys):
    import ministack.services.dynamodb as svc
    table_name = _dynamodb_table_name(spec, arn)
    if table_name not in svc._tables:
        raise _ResourceNotFound(arn)
    svc._tags[arn] = [t for t in svc._tags.get(arn, []) if t["Key"] not in keys]


def _remove_eventbridge(spec, arn, keys):
    import ministack.services.eventbridge as svc
    if not _eventbridge_resource_exists(spec, arn):
        raise _ResourceNotFound(arn)
    tags = svc._tags.get(arn, {})
    for k in keys:
        tags.pop(k, None)


def _remove_kms(spec, arn, keys):
    import ministack.services.kms as svc
    _resource_tail(spec, arn, "key/")
    key = svc._resolve_key(arn)
    if key is None:
        raise _ResourceNotFound(arn)
    key["Tags"] = [t for t in key.get("Tags", []) if t["TagKey"] not in keys]


def _remove_ecr(spec, arn, keys):
    import ministack.services.ecr as svc
    name = _resource_tail(spec, arn, "repository/")
    if name not in svc._repositories:
        raise _ResourceNotFound(arn)
    svc._repositories[name]["tags"] = [
        t for t in svc._repositories[name].get("tags", []) if t["Key"] not in keys
    ]


def _remove_ecs(_spec, arn, keys):
    import ministack.services.ecs as svc
    if not _ecs_resource_exists(arn):
        raise _ResourceNotFound(arn)
    svc._tags[arn] = [t for t in svc._tags.get(arn, []) if t["key"] not in keys]


def _remove_glue(spec, arn, keys):
    import ministack.services.glue as svc
    if not _glue_resource_exists(spec, arn):
        raise _ResourceNotFound(arn)
    tags = svc._tags.get(arn, {})
    for k in keys:
        tags.pop(k, None)


def _remove_cognito_idp(spec, arn, keys):
    """Remove ``keys`` from a Cognito user pool's tags. Raises ``_ResourceNotFound``."""
    import ministack.services.cognito as svc
    pool_id = _resource_tail(spec, arn, "userpool/")
    if pool_id not in svc._user_pools:
        raise _ResourceNotFound(arn)
    tags = svc._user_pools[pool_id].get("UserPoolTags", {})
    for k in keys:
        tags.pop(k, None)


def _remove_cognito_identity(spec, arn, keys):
    import ministack.services.cognito as svc
    pool_id = _resource_tail(spec, arn, "identitypool/")
    if pool_id not in svc._identity_pools:
        raise _ResourceNotFound(arn)
    tags = svc._identity_tags.get(pool_id, {})
    for k in keys:
        tags.pop(k, None)


def _remove_appsync(spec, arn, keys):
    import ministack.services.appsync as svc
    if not _appsync_resource_exists(spec, arn):
        raise _ResourceNotFound(arn)
    tags = svc._tags.get(arn, {})
    for k in keys:
        tags.pop(k, None)


def _remove_scheduler(spec, arn, keys):
    import ministack.services.scheduler as svc
    if not _scheduler_resource_exists(spec, arn):
        raise _ResourceNotFound(arn)
    tags = svc._tags.get(arn, {})
    for k in keys:
        tags.pop(k, None)


def _remove_cloudfront(spec, arn, keys):
    import ministack.services.cloudfront as svc
    if not _cloudfront_resource_exists(spec, arn):
        raise _ResourceNotFound(arn)
    svc._tags[arn] = [t for t in svc._tags.get(arn, []) if t["Key"] not in keys]


def _remove_efs(spec, arn, keys):
    import ministack.services.efs as svc
    if spec.resource.startswith("file-system/"):
        resource = svc._file_systems.get(_resource_tail(spec, arn, "file-system/"))
    else:
        resource = svc._access_points.get(_resource_tail(spec, arn, "access-point/"))
    if resource is None:
        raise _ResourceNotFound(arn)
    resource["Tags"] = [t for t in resource.get("Tags", []) if t["Key"] not in keys]


def _remove_backup(spec, arn, keys):
    import ministack.services.backup as svc
    if spec.resource.startswith("backup-vault:"):
        vault = svc._vaults.get(_resource_tail(spec, arn, "backup-vault:"))
        if vault is None:
            raise _ResourceNotFound(arn)
        tags = vault.get("BackupVaultTags", {})
    elif spec.resource.startswith("backup-plan:"):
        plan = svc._plans.get(_resource_tail(spec, arn, "backup-plan:"))
        if plan is None:
            raise _ResourceNotFound(arn)
        tags = plan.get("Tags", {})
    else:
        raise _ResourceNotFound(arn)
    for k in keys:
        tags.pop(k, None)


def _remove_elasticache(_spec, arn, keys):
    import ministack.services.elasticache as svc
    _resolve_elasticache_resource(svc, arn)
    svc._remove_tag_keys_for_arn(arn, keys)


_REMOVERS = {
    "s3": _remove_s3, "lambda": _remove_lambda, "sqs": _remove_sqs,
    "sns": _remove_sns, "dynamodb": _remove_dynamodb, "events": _remove_eventbridge,
    "kms": _remove_kms, "ecr": _remove_ecr, "ecs": _remove_ecs,
    "glue": _remove_glue, "cognito-idp": _remove_cognito_idp,
    "cognito-identity": _remove_cognito_identity, "appsync": _remove_appsync,
    "scheduler": _remove_scheduler, "cloudfront": _remove_cloudfront,
    "elasticfilesystem": _remove_efs, "backup": _remove_backup,
    "elasticache": _remove_elasticache,
}


# ── Operation handlers ────────────────────────────────────────────────────────

def _get_resources(data):
    tag_filters = data.get("TagFilters", [])
    type_filters = data.get("ResourceTypeFilters", [])
    arn_list = data.get("ResourceARNList", [])
    if arn_list:
        exclusive_params = {
            "ExcludeCompliantResources",
            "IncludeComplianceDetails",
            "PaginationToken",
            "ResourceTypeFilters",
            "ResourcesPerPage",
            "TagFilters",
            "TagsPerPage",
        }
        if any(param in data for param in exclusive_params):
            return _invalid_parameter(
                "ResourceARNList cannot be specified with filters, compliance details, or pagination parameters"
            )
    try:
        requested_arns = {arn for arn, _spec in _parse_resource_arn_list(arn_list)} if arn_list else None
    except _InvalidResourceArn as exc:
        return _invalid_parameter(str(exc))

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
                if requested_arns is not None and arn not in requested_arns:
                    continue
                if not _matches_type_filters(arn, type_filters):
                    continue
                if not _matches_tag_filters(tags, tag_filters):
                    continue
                results.append({"ResourceARN": arn, "Tags": tags})
        except Exception:
            pass  # service not yet initialised — skip silently

    return _json({
        "ResourceTagMappingList": results,
        "PaginationToken": "",
    })


def _get_tag_keys(data):
    keys = set()
    for collector in _COLLECTORS.values():
        try:
            for _arn, tags in collector():
                for t in tags:
                    keys.add(t["Key"])
        except Exception:
            pass
    return _json({
        "TagKeys": sorted(keys),
        "PaginationToken": "",
    })


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
    return _json({
        "TagValues": sorted(values),
        "PaginationToken": "",
    })


# ── Entry point ───────────────────────────────────────────────────────────────

def _tag_resources(data):
    """TagResources: apply ``Tags`` to every ARN in ``ResourceARNList``.

    Per ARN, failures are reported in ``FailedResourcesMap``:
      - Unknown service segment → ``InvalidParameterException`` (400).
      - Resource not found in caller's account → ``ResourceNotFound`` (404).
      - Anything else raised by the writer → ``InternalServiceException`` (500).
    The top-level response is always 200 with a (possibly empty) map, matching AWS."""
    arn_list = data.get("ResourceARNList", [])
    tags = data.get("Tags", {})
    failed = {}

    try:
        parsed_arns = _parse_resource_arn_list(arn_list)
        for arn, spec in parsed_arns:
            _reject_foreign_region_arn(spec, arn, "TagResources")
    except (_InvalidResourceArn, _WrongRegionArn) as exc:
        return _invalid_parameter(str(exc))

    for arn, spec in parsed_arns:
        svc_key = spec.service
        writer = _WRITERS.get(svc_key)
        if writer is None:
            failed[arn] = _failed_resource(
                "InvalidParameterException",
                "Unrecognized service or resource type for tagging",
                400,
            )
            continue
        try:
            _require_resource_scope(spec, arn)
            writer(spec, arn, tags)
        except _WrongAccountArn:
            failed[arn] = _failed_resource(
                "InvalidClientTokenId",
                "No account found for the given parameters",
                403,
            )
        except _WrongRegionArn as exc:
            return _invalid_parameter(str(exc))
        except _ResourceNotFound:
            # A well-formed, same-service ARN whose target resource does not exist.
            # AWS RGTA reports this in FailedResourcesMap as InvalidParameterException
            # (the ErrorCode enum is limited to InternalServiceException /
            # InvalidParameterException — there is no ResourceNotFound code here).
            failed[arn] = _failed_resource("InvalidParameterException", "Resource does not exist", 400)
        except Exception as exc:
            failed[arn] = _failed_resource("InternalServiceException", str(exc), 500)

    return _json({
        "FailedResourcesMap": failed,
    })


def _untag_resources(data):
    """UntagResources: remove ``TagKeys`` from every ARN in ``ResourceARNList``.

    Per-ARN failure semantics match :func:`_tag_resources`. Missing tag keys on
    an existing resource are a no-op, not a failure."""
    arn_list = data.get("ResourceARNList", [])
    tag_keys = data.get("TagKeys", [])
    failed = {}

    try:
        parsed_arns = _parse_resource_arn_list(arn_list)
        for arn, spec in parsed_arns:
            _reject_foreign_region_arn(spec, arn, "UntagResources")
    except (_InvalidResourceArn, _WrongRegionArn) as exc:
        return _invalid_parameter(str(exc))

    for arn, spec in parsed_arns:
        svc_key = spec.service
        remover = _REMOVERS.get(svc_key)
        if remover is None:
            failed[arn] = _failed_resource(
                "InvalidParameterException",
                "Unrecognized service or resource type for tagging",
                400,
            )
            continue
        try:
            _require_resource_scope(spec, arn)
            remover(spec, arn, tag_keys)
        except _WrongAccountArn:
            failed[arn] = _failed_resource(
                "InvalidClientTokenId",
                "No account found for the given parameters",
                403,
            )
        except _WrongRegionArn as exc:
            return _invalid_parameter(str(exc))
        except _ResourceNotFound:
            # A well-formed, same-service ARN whose target resource does not exist.
            # AWS RGTA reports this in FailedResourcesMap as InvalidParameterException
            # (the ErrorCode enum is limited to InternalServiceException /
            # InvalidParameterException — there is no ResourceNotFound code here).
            failed[arn] = _failed_resource("InvalidParameterException", "Resource does not exist", 400)
        except Exception as exc:
            failed[arn] = _failed_resource("InternalServiceException", str(exc), 500)

    return _json({
        "FailedResourcesMap": failed,
    })


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
        return 400, {"Content-Type": "application/x-amz-json-1.1", "x-amzn-errortype": "SerializationException"}, json.dumps({
            "__type": "SerializationException",
            "message": "Invalid JSON",
        }).encode()

    handler = _HANDLERS.get(action)
    if not handler:
        return 400, {"Content-Type": "application/x-amz-json-1.1", "x-amzn-errortype": "InvalidRequestException"}, json.dumps({
            "__type": "InvalidRequestException",
            "message": f"Unknown action: {action}",
        }).encode()

    return handler(data)


def reset():
    pass
