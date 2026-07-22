"""
CloudFormation provisioners — resource create/delete handlers for each AWS resource type.
"""

import base64
import copy
import hashlib
import io
import json
import logging
import os
import random
import re
import string
import time
import zipfile
from collections import defaultdict

import ministack.services.acm as _acm
import ministack.services.alb as _alb
import ministack.services.apigateway as _apigw_v2
import ministack.services.apigateway_v1 as _apigw_v1
import ministack.services.appconfig as _appconfig
import ministack.services.appsync as _appsync
import ministack.services.autoscaling as _asg
import ministack.services.backup as _backup
import ministack.services.cloudfront as _cf
import ministack.services.cloudwatch as _cw
import ministack.services.cloudwatch_logs as _cw_logs
import ministack.services.codebuild as _codebuild
import ministack.services.cognito as _cognito
import ministack.services.dynamodb as _dynamodb
import ministack.services.ec2 as _ec2
import ministack.services.ecr as _ecr
import ministack.services.ecs as _ecs
import ministack.services.eventbridge as _eb
import ministack.services.iam as _iam
import ministack.services.iot as _iot
import ministack.services.kinesis as _kinesis
import ministack.services.kms as _kms
import ministack.services.lambda_svc as _lambda_svc
import ministack.services.opensearch as _opensearch
import ministack.services.pipes as _pipes
import ministack.services.rds as _rds
import ministack.services.route53 as _r53
import ministack.services.s3 as _s3
import ministack.services.s3tables as _s3tables
import ministack.services.secretsmanager as _sm
import ministack.services.ses as _ses
import ministack.services.sns as _sns
import ministack.services.sqs as _sqs
import ministack.services.ssm as _ssm
import ministack.services.stepfunctions as _sfn
import ministack.services.waf as _waf
from ministack.core.responses import get_account_id, get_region, new_uuid, now_iso

logger = logging.getLogger("cloudformation")

# Module-level REGION kept for legacy imports; new code must use get_region()
# so AWS::Region / ARNs reflect the caller's request region (#398).
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")


def _physical_name(stack_name: str, logical_id: str, *,
                   lowercase: bool = False, max_len: int = 128) -> str:
    """Generate an AWS-style physical resource name: {stack}-{logicalId}-{SUFFIX}.

    Matches the pattern AWS CloudFormation uses for auto-named resources so that
    local testing with CDK (which omits explicit names) produces names that are
    immediately traceable back to the stack and logical resource.
    """
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=13))
    base = f"{stack_name}-{logical_id}-{suffix}"
    if lowercase:
        base = base.lower()
    return base[:max_len]


# ===========================================================================
# Resource Provisioner Framework
# ===========================================================================

def _provision_resource(resource_type: str, logical_id: str, props: dict,
                        stack_name: str) -> tuple:
    """Provision a resource. Returns (physical_id, attributes)."""
    handler = _RESOURCE_HANDLERS.get(resource_type)
    if handler and "create" in handler:
        return handler["create"](logical_id, props, stack_name)
    # Custom resource types (Custom::* handled here; AWS::CloudFormation::CustomResource goes through handler)
    if resource_type.startswith("Custom::"):
        return _custom_resource_create(logical_id, props, stack_name, resource_type)
    # CloudFormation internal types are no-ops
    if resource_type.startswith("AWS::CloudFormation::"):
        logger.info("CloudFormation internal type %s for %s -- noop", resource_type, logical_id)
        noop_id = f"{stack_name}-{logical_id}-noop-{new_uuid()[:8]}"
        return noop_id, {}
    raise ValueError(f"Unsupported resource type: {resource_type}")


def _delete_resource(resource_type: str, physical_id: str, props: dict,
                     stack_name: str | None = None, logical_id: str | None = None):
    """Delete a provisioned resource."""
    handler = _RESOURCE_HANDLERS.get(resource_type)
    if handler and "delete" in handler:
        handler["delete"](physical_id, props)
        return
    # Custom resource types
    if resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource":
        _custom_resource_delete(
            physical_id, props,
            stack_name=stack_name, logical_id=logical_id,
            resource_type=resource_type,
        )
        return
    logger.warning("No delete handler for resource type %s (id=%s)",
                   resource_type, physical_id)


def _update_resource(resource_type: str, physical_id: str, old_props: dict,
                     new_props: dict, stack_name: str,
                     logical_id: str | None = None) -> tuple:
    """Update a provisioned resource in place when the type provides an ``update``
    handler. Falls back to ``create`` (which provisioners are expected to
    implement idempotently) when no explicit update handler is registered.
    Returns (physical_id, attributes).
    """
    handler = _RESOURCE_HANDLERS.get(resource_type)
    if handler and "update" in handler:
        if handler.get("update_with_logical_id"):
            return handler["update"](
                physical_id, old_props, new_props, stack_name, logical_id
            )
        return handler["update"](physical_id, old_props, new_props, stack_name)
    # Custom resource types
    if resource_type.startswith("Custom::") or resource_type == "AWS::CloudFormation::CustomResource":
        return _custom_resource_update(
            physical_id, old_props, new_props, stack_name,
            logical_id=logical_id,
            resource_type=resource_type,
        )
    # No update handler — fall through to create. Most resource types make
    # their create idempotent; CFN never sees a fresh physical id this way.
    return _provision_resource(resource_type, physical_id, new_props, stack_name)


# ===========================================================================
# Resource Provisioners
# ===========================================================================

# --- OpenSearch Domain ---

_OPENSEARCH_MODELED_PROPERTIES = {
    "EngineVersion",
    "ClusterConfig",
    "EBSOptions",
    "AccessPolicies",
    "SnapshotOptions",
    "CognitoOptions",
    "EncryptionAtRestOptions",
    "NodeToNodeEncryptionOptions",
    "AdvancedOptions",
    "DomainEndpointOptions",
    "AdvancedSecurityOptions",
    "VPCOptions",
    "AutoTuneOptions",
    "OffPeakWindowOptions",
    "SoftwareUpdateOptions",
}


def _opensearch_physical_name(stack_name, logical_id):
    """Generate a valid 28-character OpenSearch name without losing its suffix."""
    source = f"{stack_name}-{logical_id}".lower()
    prefix = re.sub(r"[^a-z0-9-]", "-", source).strip("-")
    if not prefix or not prefix[0].isalpha():
        prefix = f"d-{prefix}"
    suffix = new_uuid().replace("-", "")[:8]
    prefix = prefix[:19].rstrip("-") or "domain"
    return f"{prefix}-{suffix}"


def _opensearch_create_payload(props, domain_name):
    desired = copy.deepcopy(props or {})
    tags = desired.pop("Tags", [])
    desired["DomainName"] = domain_name
    desired["TagList"] = copy.deepcopy(tags)
    if isinstance(desired.get("AccessPolicies"), (dict, list)):
        desired["AccessPolicies"] = json.dumps(
            desired["AccessPolicies"], sort_keys=True, separators=(",", ":")
        )
    compatibility = {
        key: copy.deepcopy(value)
        for key, value in (props or {}).items()
        if key not in _OPENSEARCH_MODELED_PROPERTIES
        and key not in {"DomainName", "Tags"}
    }
    return desired, compatibility


def _opensearch_attributes(rec):
    endpoint = rec.get("Endpoint") or (rec.get("Endpoints") or {}).get("vpc")
    if not endpoint:
        raise ValueError(f"OpenSearch domain {rec.get('DomainName', '')} has no endpoint")
    arn = rec["ARN"]
    return {
        "Arn": arn,
        "DomainArn": arn,
        "DomainEndpoint": endpoint,
        "Id": rec["DomainId"],
    }


def _opensearch_domain_create(logical_id, props, stack_name):
    name = props.get("DomainName") or _opensearch_physical_name(stack_name, logical_id)
    payload, compatibility = _opensearch_create_payload(props, name)
    rec = None
    try:
        rec = _opensearch.create_domain_record(payload, compatibility)
        return name, _opensearch_attributes(rec)
    except Exception:
        # Only delete if this call actually allocated the record. In
        # particular, a duplicate-name error must not delete the pre-existing
        # domain that caused it.
        if rec is not None:
            _opensearch.delete_domain_record(name, missing_ok=True)
        raise


def _opensearch_domain_update(physical_id, old_props, new_props, stack_name,
                              logical_id=None):
    old_was_explicit = "DomainName" in old_props
    new_is_explicit = "DomainName" in new_props
    replacement_required = (
        (new_is_explicit and new_props.get("DomainName") != physical_id)
        or (old_was_explicit and not new_is_explicit)
    )

    if replacement_required:
        replacement_logical_id = logical_id or physical_id
        new_id, attrs = _opensearch_domain_create(
            replacement_logical_id, new_props, stack_name
        )
        try:
            _opensearch.delete_domain_record(physical_id, missing_ok=True)
        except Exception:
            _opensearch.delete_domain_record(new_id, missing_ok=True)
            raise
        return new_id, attrs

    payload, compatibility = _opensearch_create_payload(new_props, physical_id)
    rec = _opensearch.update_domain_from_cloudformation(
        physical_id, payload, compatibility
    )
    return physical_id, _opensearch_attributes(rec)


def _opensearch_domain_delete(physical_id, props):
    _opensearch.delete_domain_record(physical_id, missing_ok=True)

# --- S3 Bucket ---

def _s3_create(logical_id, props, stack_name):
    name = props.get("BucketName") or _physical_name(stack_name, logical_id, lowercase=True, max_len=63)
    _s3._buckets.setdefault(name, {
        "created": now_iso(),
        "objects": {},
        "region": get_region(),
    })
    versioning = props.get("VersioningConfiguration", {})
    if versioning.get("Status") == "Enabled":
        _s3._bucket_versioning[name] = "Enabled"
    attrs = {
        "Arn": f"arn:aws:s3:::{name}",
        "DomainName": f"{name}.s3.amazonaws.com",
        "RegionalDomainName": f"{name}.s3.{get_region()}.amazonaws.com",
        "WebsiteURL": f"http://{name}.s3-website-{get_region()}.amazonaws.com",
    }
    return name, attrs


def _s3_update(physical_id, old_props, new_props, stack_name):
    """Update an S3 bucket in place. Real AWS CloudFormation preserves the
    physical bucket (and its name) when only mutable properties change.
    Auto-named buckets (no explicit ``BucketName``) must keep their existing
    physical resource id so that ``Ref`` keeps resolving to the same bucket
    where artifacts were uploaded."""
    old_name = old_props.get("BucketName")
    new_name = new_props.get("BucketName")
    if new_name and new_name != physical_id:
        return _s3_create(new_name, new_props, stack_name)
    name = physical_id
    if name in _s3._buckets:
        versioning = new_props.get("VersioningConfiguration", {})
        if versioning.get("Status") == "Enabled":
            _s3._bucket_versioning[name] = "Enabled"
    else:
        return _s3_create(name, new_props, stack_name)
    attrs = {
        "Arn": f"arn:aws:s3:::{name}",
        "DomainName": f"{name}.s3.amazonaws.com",
        "RegionalDomainName": f"{name}.s3.{get_region()}.amazonaws.com",
        "WebsiteURL": f"http://{name}.s3-website-{get_region()}.amazonaws.com",
    }
    return name, attrs


def _s3_bucket_policy_create(logical_id, props, stack_name):
    bucket = props.get("Bucket", "")
    policy = props.get("PolicyDocument")
    if bucket and policy:
        import json
        _s3._bucket_policies[bucket] = json.dumps(policy) if isinstance(policy, dict) else policy
    return f"{bucket}-policy", {}


def _s3_bucket_policy_delete(physical_id, props):
    bucket = props.get("Bucket", "")
    _s3._bucket_policies.pop(bucket, None)


def _s3_delete(physical_id, props):
    _s3._buckets.pop(physical_id, None)
    _s3._bucket_versioning.pop(physical_id, None)
    _s3._bucket_policies.pop(physical_id, None)
    _s3._bucket_tags.pop(physical_id, None)
    _s3._bucket_encryption.pop(physical_id, None)
    _s3._bucket_lifecycle.pop(physical_id, None)
    _s3._bucket_cors.pop(physical_id, None)
    _s3._bucket_acl.pop(physical_id, None)
    _s3._bucket_notifications.pop(physical_id, None)


# --- SQS Queue ---

def _sqs_create(logical_id, props, stack_name):
    name = props.get("QueueName") or _physical_name(stack_name, logical_id, max_len=80)
    is_fifo = name.endswith(".fifo")
    url = f"http://{_sqs.DEFAULT_HOST}:{_sqs.DEFAULT_PORT}/{get_account_id()}/{name}"
    arn = f"arn:aws:sqs:{get_region()}:{get_account_id()}:{name}"
    now_ts = str(int(time.time()))

    attributes = {
        "QueueArn": arn,
        "CreatedTimestamp": now_ts,
        "LastModifiedTimestamp": now_ts,
        "VisibilityTimeout": str(props.get("VisibilityTimeout", "30")),
        "MaximumMessageSize": str(props.get("MaximumMessageSize", "262144")),
        "MessageRetentionPeriod": str(props.get("MessageRetentionPeriod", "345600")),
        "DelaySeconds": str(props.get("DelaySeconds", "0")),
        "ReceiveMessageWaitTimeSeconds": str(props.get("ReceiveMessageWaitTimeSeconds", "0")),
    }
    if is_fifo:
        attributes["FifoQueue"] = "true"
        if props.get("ContentBasedDeduplication"):
            attributes["ContentBasedDeduplication"] = str(props["ContentBasedDeduplication"]).lower()

    queue = {
        "name": name,
        "url": url,
        "is_fifo": is_fifo,
        "attributes": attributes,
        "messages": [],
        "tags": {},
        "dedup_cache": {},
        "fifo_seq": 0,
    }
    _sqs._queues[url] = queue
    _sqs._queue_name_to_url[name] = url
    return url, {"Arn": arn, "QueueName": name, "QueueUrl": url}


def _sqs_delete(physical_id, props):
    queue = _sqs._queues.pop(physical_id, None)
    if queue:
        _sqs._queue_name_to_url.pop(queue.get("name", ""), None)


# --- SNS Topic ---

def _sns_create(logical_id, props, stack_name):
    name = props.get("TopicName") or _physical_name(stack_name, logical_id, max_len=256)
    arn = f"arn:aws:sns:{get_region()}:{get_account_id()}:{name}"
    default_policy = json.dumps({
        "Version": "2008-10-17",
        "Id": "__default_policy_ID",
        "Statement": [{
            "Sid": "__default_statement_ID",
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": ["SNS:Publish", "SNS:Subscribe", "SNS:Receive"],
            "Resource": arn,
        }],
    })
    _sns._topics[arn] = {
        "name": name,
        "arn": arn,
        "attributes": {
            "TopicArn": arn,
            "DisplayName": props.get("DisplayName", name),
            "Owner": get_account_id(),
            "Policy": default_policy,
            "SubscriptionsConfirmed": "0",
            "SubscriptionsPending": "0",
            "SubscriptionsDeleted": "0",
            "EffectiveDeliveryPolicy": json.dumps({
                "http": {
                    "defaultHealthyRetryPolicy": {
                        "minDelayTarget": 20,
                        "maxDelayTarget": 20,
                        "numRetries": 3,
                    }
                }
            }),
        },
        "subscriptions": [],
        "messages": [],
        "tags": {},
    }

    # Handle Subscription property
    subscriptions = props.get("Subscription", [])
    for sub_def in subscriptions:
        protocol = sub_def.get("Protocol", "")
        endpoint = sub_def.get("Endpoint", "")
        sub_arn = f"{arn}:{new_uuid()}"
        sub = {
            "arn": sub_arn,
            "topic_arn": arn,
            "protocol": protocol,
            "endpoint": endpoint,
            "confirmed": protocol not in ("http", "https"),
            "owner": get_account_id(),
            "attributes": {}
        }
        _sns._topics[arn]["subscriptions"].append(sub)
        _sns._sub_arn_to_topic[sub_arn] = arn

    return arn, {"TopicArn": arn, "TopicName": name}


def _sns_delete(physical_id, props):
    topic = _sns._topics.pop(physical_id, None)
    if topic:
        for sub in topic.get("subscriptions", []):
            _sns._sub_arn_to_topic.pop(sub.get("arn", ""), None)


# --- SNS Subscription (standalone) ---

def _sns_sub_create(logical_id, props, stack_name):
    topic_arn = props.get("TopicArn", "")
    protocol = props.get("Protocol", "")
    endpoint = props.get("Endpoint", "")
    topic = _sns._topics.get(topic_arn)
    if not topic:
        sub_arn = f"{topic_arn}:{new_uuid()}"
        return sub_arn, {"SubscriptionArn": sub_arn}

    sub_arn = f"{topic_arn}:{new_uuid()}"
    raw = props.get("RawMessageDelivery", False)
    raw_str = "true" if (raw is True or str(raw).lower() == "true") else "false"
    sub = {
        "arn": sub_arn,
        "topic_arn": topic_arn,
        "protocol": protocol,
        "endpoint": endpoint,
        "confirmed": protocol not in ("http", "https"),
        "owner": get_account_id(),
        "attributes": {
            "FilterPolicyScope": props.get("FilterPolicyScope", "MessageAttributes"),
            "FilterPolicy": (
                json.dumps(props.get("FilterPolicy"))
                if isinstance(props.get("FilterPolicy"), (dict, list))
                else (props.get("FilterPolicy", "") or "")
            ),
            "RawMessageDelivery": raw_str,
        },
    }
    topic["subscriptions"].append(sub)
    _sns._sub_arn_to_topic[sub_arn] = topic_arn
    return sub_arn, {"SubscriptionArn": sub_arn}


def _sns_sub_delete(physical_id, props):
    topic_arn = _sns._sub_arn_to_topic.pop(physical_id, None)
    if topic_arn:
        topic = _sns._topics.get(topic_arn)
        if topic:
            topic["subscriptions"] = [
                s for s in topic["subscriptions"] if s["arn"] != physical_id
            ]


# --- DynamoDB Table ---

def _ddb_create(logical_id, props, stack_name):
    name = props.get("TableName") or _physical_name(stack_name, logical_id, max_len=255)
    arn = f"arn:aws:dynamodb:{get_region()}:{get_account_id()}:table/{name}"

    key_schema = props.get("KeySchema", [])
    pk_name = None
    sk_name = None
    for ks in key_schema:
        if ks.get("KeyType") == "HASH":
            pk_name = ks.get("AttributeName")
        elif ks.get("KeyType") == "RANGE":
            sk_name = ks.get("AttributeName")

    attr_defs = props.get("AttributeDefinitions", [])
    gsis = props.get("GlobalSecondaryIndexes", [])
    lsis = props.get("LocalSecondaryIndexes", [])

    stream_spec = props.get("StreamSpecification", {})
    if stream_spec.get("StreamViewType") and "StreamEnabled" not in stream_spec:
        stream_spec = {**stream_spec, "StreamEnabled": True}
    stream_enabled = stream_spec.get("StreamEnabled", False)
    stream_arn = f"{arn}/stream/{now_iso()}" if stream_enabled else None

    billing = props.get("BillingMode", "PROVISIONED")

    table = {
        "TableName": name,
        "TableArn": arn,
        "TableId": new_uuid(),
        "TableStatus": "ACTIVE",
        "CreationDateTime": int(time.time()),
        "KeySchema": key_schema,
        "AttributeDefinitions": attr_defs,
        "ProvisionedThroughput": props.get("ProvisionedThroughput", {
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5,
        }),
        "BillingModeSummary": {"BillingMode": billing},
        "pk_name": pk_name,
        "sk_name": sk_name,
        "items": defaultdict(dict),
        "ItemCount": 0,
        "TableSizeBytes": 0,
        "GlobalSecondaryIndexes": gsis,
        "LocalSecondaryIndexes": lsis,
        "StreamSpecification": stream_spec if stream_enabled else None,
        "LatestStreamArn": stream_arn,
        "LatestStreamLabel": now_iso() if stream_enabled else None,
        "DeletionProtectionEnabled": props.get("DeletionProtectionEnabled", False),
        "SSEDescription": None,
        "Tags": [],
    }
    _dynamodb._tables[name] = table

    attrs = {"Arn": arn}
    if stream_arn:
        attrs["StreamArn"] = stream_arn
    return name, attrs


def _ddb_delete(physical_id, props):
    _dynamodb._tables.pop(physical_id, None)


def _ddb_global_table_create(logical_id, props, stack_name):
    """Provision an AWS::DynamoDB::GlobalTable.

    GlobalTable's CFN schema diverges from Table's in three places that affect
    a single-process emulator:
      * No `ProvisionedThroughput` — capacity comes from
        `WriteProvisionedThroughputSettings` and `ReadProvisionedThroughputSettings`,
        each wrapping a `<Read|Write>CapacityAutoScalingSettings.MinCapacity`.
      * `Replicas` is required (one entry per region). Cross-region replication
        has no meaning here, so we accept the field and ignore its contents.
      * Multi-region settings (`MultiRegionConsistency`, `GlobalTableWitnesses`,
        `GlobalTableSourceArn`, `WarmThroughput`) are accepted and ignored.

    Everything else (`KeySchema`, `AttributeDefinitions`, `BillingMode`, GSIs,
    LSIs, `StreamSpecification`, `SSESpecification`, `TimeToLiveSpecification`,
    `TableName`) routes through the regular Table provisioner.
    """
    translated = dict(props)
    translated.pop("Replicas", None)
    translated.pop("MultiRegionConsistency", None)
    translated.pop("GlobalTableWitnesses", None)
    translated.pop("GlobalTableSourceArn", None)
    translated.pop("WarmThroughput", None)
    translated.pop("WriteOnDemandThroughputSettings", None)
    translated.pop("ReadOnDemandThroughputSettings", None)

    write = (props.get("WriteProvisionedThroughputSettings") or {})
    read = (props.get("ReadProvisionedThroughputSettings") or {})
    write_cap = (write.get("WriteCapacityAutoScalingSettings") or {}).get("MinCapacity")
    read_cap = (read.get("ReadCapacityAutoScalingSettings") or {}).get("MinCapacity")
    if write_cap is not None or read_cap is not None:
        translated["ProvisionedThroughput"] = {
            "WriteCapacityUnits": int(write_cap) if write_cap is not None else 5,
            "ReadCapacityUnits": int(read_cap) if read_cap is not None else 5,
        }
    translated.pop("WriteProvisionedThroughputSettings", None)
    translated.pop("ReadProvisionedThroughputSettings", None)

    return _ddb_create(logical_id, translated, stack_name)


def _ddb_global_table_delete(physical_id, props):
    _ddb_delete(physical_id, props)


# --- Lambda Function ---

def _zip_inline(source: str | None, handler: str, runtime: str = "python3.12") -> bytes | None:
    """Wrap inline ZipFile source code into a real zip archive."""
    if not source:
        return None
    module = handler.split(".")[0] if handler and "." in handler else "index"
    ext = ".js" if runtime.startswith("nodejs") else ".py"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{module}{ext}", source)
    return buf.getvalue()


def _lambda_create(logical_id, props, stack_name):
    name = props.get("FunctionName") or _physical_name(stack_name, logical_id, max_len=64)
    arn = f"arn:aws:lambda:{get_region()}:{get_account_id()}:function:{name}"
    runtime = props.get("Runtime", "python3.12")
    handler = props.get("Handler", "index.handler")
    role = props.get("Role", f"arn:aws:iam::{get_account_id()}:role/dummy-role")
    timeout = int(props.get("Timeout", 3))
    memory = int(props.get("MemorySize", 128))
    env_vars = props.get("Environment", {}).get("Variables", {})
    description = props.get("Description", "")
    layers = props.get("Layers", [])
    code = props.get("Code", {})

    # Resolve the actual code bytes:
    #  - inline ZipFile is wrapped to a real zip archive
    #  - S3{Bucket,Key,ObjectVersion} fetches from the in-memory S3
    #    service so AWS::Lambda::Function deployments backed by S3 work
    #    end-to-end (matching real CFN). Falls back to inline if S3
    #    fetch fails.
    code_zip = _zip_inline(code.get("ZipFile"), handler, runtime)
    if code_zip is None and code.get("S3Bucket") and code.get("S3Key"):
        code_zip = _lambda_svc._fetch_code_from_s3(
            code["S3Bucket"],
            code["S3Key"],
            version_id=code.get("S3ObjectVersion"),
        )

    code_size = len(code_zip) if code_zip else 0
    code_sha = (
        base64.b64encode(hashlib.sha256(code_zip).digest()).decode()
        if code_zip else "cfn-deployed"
    )

    func = {
        "config": {
            "FunctionName": name,
            "FunctionArn": arn,
            "Runtime": runtime,
            "Role": role,
            "Handler": handler,
            "CodeSize": code_size,
            "Description": description,
            "Timeout": timeout,
            "MemorySize": memory,
            "LastModified": now_iso(),
            "CodeSha256": code_sha,
            "Version": "$LATEST",
            "Environment": {"Variables": env_vars},
            "Layers": [{"Arn": l} if isinstance(l, str) else l for l in layers],
            "State": "Active",
            "LastUpdateStatus": "Successful",
            "PackageType": "Zip",
            "Architectures": props.get("Architectures", ["x86_64"]),
            "EphemeralStorage": {"Size": props.get("EphemeralStorage", {}).get("Size", 512)},
            "TracingConfig": props.get("TracingConfig", {"Mode": "PassThrough"}),
            "LoggingConfig": props.get("LoggingConfig", {"LogFormat": "Text", "LogGroup": f"/aws/lambda/{name}"}),
            "RevisionId": new_uuid(),
        },
        "code_zip": code_zip,
        "code_s3_bucket": code.get("S3Bucket"),
        "code_s3_key": code.get("S3Key"),
        "code_s3_object_version": code.get("S3ObjectVersion"),
        "versions": {},
        "next_version": 1,
        "tags": {},
        "policy": {"Version": "2012-10-17", "Id": "default", "Statement": []},
        "event_invoke_config": None,
        "event_invoke_configs": {},
        "aliases": {},
        "concurrency": None,
        "provisioned_concurrency": {},
    }
    _lambda_svc._functions[name] = func
    # On a stack UPDATE this re-provisions over an existing function; recycle the
    # warm worker + docker pool so the new code/config load on the next invoke
    # (#897). No-op on first create (no worker spawned yet).
    _lambda_svc.invalidate_worker(name)
    _lambda_svc._pool_kill_function(get_account_id(), name)
    return name, {"Arn": arn}


def _lambda_delete(physical_id, props):
    _lambda_svc._functions.pop(physical_id, None)


# --- IAM Role ---

def _iam_role_create(logical_id, props, stack_name):
    name = props.get("RoleName") or _physical_name(stack_name, logical_id, max_len=64)
    arn = f"arn:aws:iam::{get_account_id()}:role/{name}"
    role_id = "AROA" + new_uuid().replace("-", "")[:17].upper()
    assume_doc = props.get("AssumeRolePolicyDocument", {})
    if isinstance(assume_doc, dict):
        assume_doc = json.dumps(assume_doc)

    role = {
        "RoleName": name,
        "Arn": arn,
        "RoleId": role_id,
        "CreateDate": now_iso(),
        "Path": props.get("Path", "/"),
        "AssumeRolePolicyDocument": assume_doc,
        "Description": props.get("Description", ""),
        "MaxSessionDuration": int(props.get("MaxSessionDuration", 3600)),
        "AttachedPolicies": [],
        "InlinePolicies": {},
        "Tags": [],
    }

    # ManagedPolicyArns
    managed = props.get("ManagedPolicyArns", [])
    for policy_arn in managed:
        role["AttachedPolicies"].append({
            "PolicyName": policy_arn.split("/")[-1],
            "PolicyArn": policy_arn,
        })

    # Inline Policies
    policies = props.get("Policies", [])
    for pol in policies:
        pol_name = pol.get("PolicyName", "")
        pol_doc = pol.get("PolicyDocument", {})
        if isinstance(pol_doc, dict):
            pol_doc = json.dumps(pol_doc)
        role["InlinePolicies"][pol_name] = pol_doc

    # Tags
    tags = props.get("Tags", [])
    for t in tags:
        role["Tags"].append({"Key": t.get("Key", ""), "Value": t.get("Value", "")})

    _iam._roles[name] = role
    return name, {"Arn": arn, "RoleId": role_id}


def _iam_role_delete(physical_id, props):
    _iam._roles.pop(physical_id, None)


# --- IAM Policy ---

def _iam_policy_create(logical_id, props, stack_name):
    name = props.get("PolicyName") or _physical_name(stack_name, logical_id, max_len=128)
    path = props.get("Path", "/")
    arn = f"arn:aws:iam::{get_account_id()}:policy{path}{name}"
    pol_doc = props.get("PolicyDocument", {})
    if isinstance(pol_doc, dict):
        pol_doc = json.dumps(pol_doc)

    policy = {
        "PolicyName": name,
        "PolicyId": new_uuid().replace("-", "")[:21].upper(),
        "Arn": arn,
        "Path": path,
        "DefaultVersionId": "v1",
        "AttachmentCount": 0,
        "IsAttachable": True,
        "CreateDate": now_iso(),
        "UpdateDate": now_iso(),
        "Description": props.get("Description", ""),
        "Versions": [{
            "VersionId": "v1",
            "IsDefaultVersion": True,
            "Document": pol_doc,
            "CreateDate": now_iso(),
        }],
        "Tags": [],
    }
    _iam._policies[arn] = policy

    # Attach to roles if Roles property specified
    roles = props.get("Roles", [])
    for role_name in roles:
        role = _iam._roles.get(role_name)
        if role:
            role["AttachedPolicies"].append({
                "PolicyName": name,
                "PolicyArn": arn,
            })
            policy["AttachmentCount"] += 1

    return arn, {"PolicyArn": arn}


def _iam_policy_delete(physical_id, props):
    _iam._policies.pop(physical_id, None)


# --- IAM InstanceProfile ---

def _iam_ip_create(logical_id, props, stack_name):
    name = props.get("InstanceProfileName") or _physical_name(stack_name, logical_id, max_len=128)
    path = props.get("Path", "/")
    arn = f"arn:aws:iam::{get_account_id()}:instance-profile{path}{name}"
    ip_id = new_uuid().replace("-", "")[:21].upper()

    roles = []
    for rname in props.get("Roles", []):
        role = _iam._roles.get(rname)
        if role:
            roles.append(role)

    profile = {
        "InstanceProfileName": name,
        "InstanceProfileId": ip_id,
        "Arn": arn,
        "Path": path,
        "Roles": roles,
        "CreateDate": now_iso(),
        "Tags": [],
    }
    _iam._instance_profiles[name] = profile
    return arn, {"Arn": arn}


def _iam_ip_delete(physical_id, props):
    # physical_id is the ARN -- find the name
    for name, ip in list(_iam._instance_profiles.items()):
        if ip.get("Arn") == physical_id:
            _iam._instance_profiles.pop(name, None)
            return


# --- SSM Parameter ---

def _ssm_create(logical_id, props, stack_name):
    name = props.get("Name") or f"/{stack_name}/{logical_id}"
    ptype = props.get("Type", "String")
    value = props.get("Value", "")
    description = props.get("Description", "")
    param_arn = _ssm._param_arn(name)

    _ssm._parameters[name] = {
        "Name": name,
        "Type": ptype,
        "Value": value,
        "Version": 1,
        "LastModifiedDate": _ssm._now_epoch(),
        "ARN": param_arn,
        "DataType": "text",
        "Description": description,
        "Tier": props.get("Tier", "Standard"),
        "AllowedPattern": props.get("AllowedPattern", ""),
        "Tags": [],
    }
    return name, {"Type": ptype, "Value": value}


def _ssm_delete(physical_id, props):
    _ssm._parameters.pop(physical_id, None)


# --- AppConfig Application ---

def _appconfig_application_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id)
    app_id = _appconfig._gen_id()
    _appconfig._applications[app_id] = {
        "Id": app_id,
        "Name": name,
        "Description": props.get("Description", ""),
    }
    cfn_tags = props.get("Tags") or []
    if cfn_tags:
        _appconfig._apply_tags(
            _appconfig._app_arn(app_id),
            {t["Key"]: t["Value"] for t in cfn_tags if "Key" in t},
        )
    return app_id, {"ApplicationId": app_id}


def _appconfig_application_delete(physical_id, props):
    _appconfig._applications.pop(physical_id, None)
    _appconfig._tags.pop(_appconfig._app_arn(physical_id), None)


# --- AppConfig Environment ---

def _appconfig_environment_create(logical_id, props, stack_name):
    app_id = props.get("ApplicationId")
    if not app_id:
        raise ValueError("AWS::AppConfig::Environment requires ApplicationId")
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=64)
    env_id = _appconfig._gen_id()
    _appconfig._environments[f"{app_id}/{env_id}"] = {
        "ApplicationId": app_id,
        "Id": env_id,
        "Name": name,
        "Description": props.get("Description", ""),
        "State": "READY_FOR_DEPLOYMENT",
        "Monitors": props.get("Monitors", []),
        "DeletionProtectionCheck": props.get("DeletionProtectionCheck", "ACCOUNT_DEFAULT"),
    }
    cfn_tags = props.get("Tags") or []
    if cfn_tags:
        _appconfig._apply_tags(
            _appconfig._env_arn(app_id, env_id),
            {t["Key"]: t["Value"] for t in cfn_tags if "Key" in t},
        )
    # Ref → environment ID; GetAtt EnvironmentId per AWS CFN reference.
    return env_id, {"EnvironmentId": env_id}


def _appconfig_environment_delete(physical_id, props):
    app_id = props.get("ApplicationId", "")
    _appconfig._environments.pop(f"{app_id}/{physical_id}", None)
    _appconfig._tags.pop(_appconfig._env_arn(app_id, physical_id), None)


# --- AppConfig ConfigurationProfile ---

def _appconfig_configuration_profile_create(logical_id, props, stack_name):
    app_id = props.get("ApplicationId")
    if not app_id:
        raise ValueError("AWS::AppConfig::ConfigurationProfile requires ApplicationId")
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=128)
    profile_id = _appconfig._gen_id()
    _appconfig._config_profiles[f"{app_id}/{profile_id}"] = {
        "ApplicationId": app_id,
        "Id": profile_id,
        "Name": name,
        "Description": props.get("Description", ""),
        "LocationUri": props.get("LocationUri", "hosted"),
        "RetrievalRoleArn": props.get("RetrievalRoleArn", ""),
        "Validators": props.get("Validators", []),
        "Type": props.get("Type", "AWS.Freeform"),
        "KmsKeyIdentifier": props.get("KmsKeyIdentifier", ""),
        "DeletionProtectionCheck": props.get("DeletionProtectionCheck", "ACCOUNT_DEFAULT"),
    }
    cfn_tags = props.get("Tags") or []
    if cfn_tags:
        _appconfig._apply_tags(
            _appconfig._profile_arn(app_id, profile_id),
            {t["Key"]: t["Value"] for t in cfn_tags if "Key" in t},
        )
    # Ref → configuration profile ID; GetAtt ConfigurationProfileId, KmsKeyArn.
    # KmsKeyArn is only populated when a KMS key was supplied; CDK reads it as
    # an empty string in that case.
    return profile_id, {
        "ConfigurationProfileId": profile_id,
        "KmsKeyArn": props.get("KmsKeyIdentifier", ""),
    }


def _appconfig_configuration_profile_delete(physical_id, props):
    app_id = props.get("ApplicationId", "")
    _appconfig._config_profiles.pop(f"{app_id}/{physical_id}", None)
    _appconfig._tags.pop(_appconfig._profile_arn(app_id, physical_id), None)


# --- AppConfig HostedConfigurationVersion ---

def _appconfig_hosted_version_create(logical_id, props, stack_name):
    app_id = props.get("ApplicationId")
    profile_id = props.get("ConfigurationProfileId")
    if not app_id or not profile_id:
        raise ValueError(
            "AWS::AppConfig::HostedConfigurationVersion requires "
            "ApplicationId and ConfigurationProfileId"
        )
    content = props.get("Content", "")
    # CDK / Fn::ToJsonString may pass parsed JSON; AWS wire shape is a string.
    if isinstance(content, (dict, list)):
        content = json.dumps(content)
    existing = [
        v for k, v in _appconfig._hosted_versions.items()
        if k.startswith(f"{app_id}/{profile_id}/")
    ]
    version_number = len(existing) + 1
    # AWS optimistic-concurrency: if LatestVersionNumber is supplied, it must
    # match the most-recent version_number — otherwise reject with a
    # ConflictException-shape error (mirrors real AppConfig's lock check).
    latest_lock = props.get("LatestVersionNumber")
    if latest_lock is not None and int(latest_lock) != version_number - 1:
        raise ValueError(
            f"AWS::AppConfig::HostedConfigurationVersion LatestVersionNumber "
            f"mismatch: supplied {latest_lock}, current latest is {version_number - 1}"
        )
    _appconfig._hosted_versions[f"{app_id}/{profile_id}/{version_number}"] = {
        "ApplicationId": app_id,
        "ConfigurationProfileId": profile_id,
        "VersionNumber": version_number,
        "ContentType": props.get("ContentType", "application/json"),
        "Content": content,
        "Description": props.get("Description", ""),
        "VersionLabel": props.get("VersionLabel", ""),
    }
    # Ref → version number; GetAtt VersionNumber.
    return str(version_number), {"VersionNumber": version_number}


def _appconfig_hosted_version_delete(physical_id, props):
    app_id = props.get("ApplicationId", "")
    profile_id = props.get("ConfigurationProfileId", "")
    _appconfig._hosted_versions.pop(
        f"{app_id}/{profile_id}/{physical_id}", None
    )


# --- AppConfig DeploymentStrategy ---

def _appconfig_deployment_strategy_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=64)
    strategy_id = _appconfig._gen_id()
    _appconfig._deployment_strategies[strategy_id] = {
        "Id": strategy_id,
        "Name": name,
        "Description": props.get("Description", ""),
        "DeploymentDurationInMinutes": props.get("DeploymentDurationInMinutes", 0),
        "GrowthType": props.get("GrowthType", "LINEAR"),
        "GrowthFactor": props.get("GrowthFactor", 100.0),
        "FinalBakeTimeInMinutes": props.get("FinalBakeTimeInMinutes", 0),
        "ReplicateTo": props.get("ReplicateTo", "NONE"),
    }
    cfn_tags = props.get("Tags") or []
    if cfn_tags:
        _appconfig._apply_tags(
            _appconfig._strategy_arn(strategy_id),
            {t["Key"]: t["Value"] for t in cfn_tags if "Key" in t},
        )
    # Ref → deployment strategy ID; GetAtt is `Id` (singular) per AWS reference.
    return strategy_id, {"Id": strategy_id}


def _appconfig_deployment_strategy_delete(physical_id, props):
    _appconfig._deployment_strategies.pop(physical_id, None)
    _appconfig._tags.pop(_appconfig._strategy_arn(physical_id), None)


# --- AppConfig Deployment ---

def _appconfig_deployment_create(logical_id, props, stack_name):
    app_id = props.get("ApplicationId")
    env_id = props.get("EnvironmentId")
    strategy_id = props.get("DeploymentStrategyId")
    profile_id = props.get("ConfigurationProfileId")
    if not all([app_id, env_id, strategy_id, profile_id]):
        raise ValueError(
            "AWS::AppConfig::Deployment requires ApplicationId, EnvironmentId, "
            "DeploymentStrategyId, and ConfigurationProfileId"
        )
    existing = [
        v for k, v in _appconfig._deployments.items()
        if k.startswith(f"{app_id}/{env_id}/")
    ]
    deploy_num = len(existing) + 1
    now = _appconfig._now_iso()
    _appconfig._deployments[f"{app_id}/{env_id}/{deploy_num}"] = {
        "ApplicationId": app_id,
        "EnvironmentId": env_id,
        "DeploymentStrategyId": strategy_id,
        "ConfigurationProfileId": profile_id,
        "DeploymentNumber": deploy_num,
        "ConfigurationName": _appconfig._config_profiles.get(
            f"{app_id}/{profile_id}", {}
        ).get("Name", ""),
        "ConfigurationLocationUri": "hosted",
        "ConfigurationVersion": props.get("ConfigurationVersion", ""),
        "Description": props.get("Description", ""),
        "State": "COMPLETE",
        "PercentageComplete": 100.0,
        "StartedAt": now,
        "CompletedAt": now,
        "KmsKeyIdentifier": props.get("KmsKeyIdentifier", ""),
        "DynamicExtensionParameters": props.get("DynamicExtensionParameters", []),
    }
    cfn_tags = props.get("Tags") or []
    if cfn_tags:
        # Deployment doesn't have its own ARN helper; use the standard AppConfig
        # ARN shape so ListTagsForResource keeps working post-create.
        deploy_arn = (
            f"arn:aws:appconfig:{_appconfig.get_region()}:"
            f"{_appconfig.get_account_id()}:application/{app_id}/"
            f"environment/{env_id}/deployment/{deploy_num}"
        )
        _appconfig._apply_tags(
            deploy_arn,
            {t["Key"]: t["Value"] for t in cfn_tags if "Key" in t},
        )
    # GetAtt DeploymentNumber, State. Ref is documented as having no return
    # value on the AWS CFN page; we return the deploy_num as the physical id
    # so CDK templates that Ref a Deployment still resolve.
    return str(deploy_num), {"DeploymentNumber": deploy_num, "State": "COMPLETE"}


def _appconfig_deployment_delete(physical_id, props):
    app_id = props.get("ApplicationId", "")
    env_id = props.get("EnvironmentId", "")
    _appconfig._deployments.pop(f"{app_id}/{env_id}/{physical_id}", None)
    deploy_arn = (
        f"arn:aws:appconfig:{_appconfig.get_region()}:"
        f"{_appconfig.get_account_id()}:application/{app_id}/"
        f"environment/{env_id}/deployment/{physical_id}"
    )
    _appconfig._tags.pop(deploy_arn, None)


# --- CloudWatch Logs LogGroup ---

def _cwlogs_create(logical_id, props, stack_name):
    name = props.get("LogGroupName") or f"/aws/cloudformation/{stack_name}/{logical_id}"
    arn = f"arn:aws:logs:{get_region()}:{get_account_id()}:log-group:{name}:*"
    retention = props.get("RetentionInDays")

    _cw_logs._log_groups[name] = {
        "arn": arn,
        "creationTime": int(time.time() * 1000),
        "retentionInDays": int(retention) if retention else None,
        "tags": {},
        "streams": {},
        "subscriptionFilters": {},
    }
    return name, {"Arn": arn}


def _cwlogs_delete(physical_id, props):
    _cw_logs._log_groups.pop(physical_id, None)


# --- CloudWatch Logs SubscriptionFilter (#896) ---

def _cwlogs_subfilter_create(logical_id, props, stack_name):
    group = props.get("LogGroupName")
    if not group:
        raise ValueError("AWS::Logs::SubscriptionFilter requires LogGroupName")
    # Ref returns the filter name; CFN auto-generates one when FilterName is omitted.
    filter_name = props.get("FilterName") or _physical_name(stack_name, logical_id, max_len=512)
    grp = _cw_logs._log_groups.get(group)
    if grp is None:
        # The referenced group should already exist (via Ref/DependsOn); create a
        # minimal entry if not so the filter is still recorded and queryable.
        grp = _cw_logs._log_groups[group] = {
            "arn": f"arn:aws:logs:{get_region()}:{get_account_id()}:log-group:{group}:*",
            "creationTime": int(time.time() * 1000),
            "retentionInDays": None,
            "tags": {},
            "streams": {},
            "subscriptionFilters": {},
        }
    grp.setdefault("subscriptionFilters", {})[filter_name] = {
        "filterName": filter_name,
        "logGroupName": group,
        "filterPattern": props.get("FilterPattern", ""),
        "destinationArn": props.get("DestinationArn", ""),
        "roleArn": props.get("RoleArn", ""),
        "distribution": props.get("Distribution", "ByLogStream"),
        "creationTime": int(time.time() * 1000),
    }
    return filter_name, {}


def _cwlogs_subfilter_delete(physical_id, props):
    grp = _cw_logs._log_groups.get(props.get("LogGroupName"))
    if grp:
        grp.get("subscriptionFilters", {}).pop(physical_id, None)


# --- EventBridge Rule ---

def _eb_rule_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=64)
    bus = props.get("EventBusName", "default")
    key = _eb._rule_key(name, bus)
    arn = f"arn:aws:events:{get_region()}:{get_account_id()}:rule/{bus}/{name}"

    _eb._rules[key] = {
        "Name": name,
        "Arn": arn,
        "EventBusName": bus,
        "State": props.get("State", "ENABLED"),
        "Description": props.get("Description", ""),
        "ScheduleExpression": props.get("ScheduleExpression", ""),
        "EventPattern": json.dumps(props["EventPattern"]) if isinstance(props.get("EventPattern"), dict) else props.get("EventPattern", ""),
        "RoleArn": props.get("RoleArn", ""),
    }

    targets = props.get("Targets", [])
    _eb._targets[key] = []
    for t in targets:
        _eb._targets[key].append(t)

    return name, {"Arn": arn}


def _eb_rule_delete(physical_id, props):
    bus = props.get("EventBusName", "default")
    key = _eb._rule_key(physical_id, bus)
    _eb._rules.pop(key, None)
    _eb._targets.pop(key, None)


# --- IoT Topic Rule (AWS::IoT::TopicRule) ---


def _pascal_to_camel(obj):
    """Recursively lower-case the first letter of every dict key.

    CloudFormation PascalCases the IoT API's camelCase TopicRulePayload fields
    (Sql, Actions, Lambda, FunctionArn, ...); this reverses that so the stored
    rule matches the control-plane / routing shape.
    """
    if isinstance(obj, dict):
        return {(k[:1].lower() + k[1:] if k else k): _pascal_to_camel(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_pascal_to_camel(v) for v in obj]
    return obj


def _iot_topic_rule_create(logical_id, props, stack_name):
    name = props.get("RuleName") or _physical_name(stack_name, logical_id).replace("-", "_")
    payload = _pascal_to_camel(props.get("TopicRulePayload", {}))
    _iot.put_topic_rule(name, payload)
    return name, {"Arn": _iot._topic_rule_arn(name)}


def _iot_topic_rule_delete(physical_id, props):
    _iot.delete_topic_rule(physical_id)


# --- EventBridge Scheduler (AWS::Scheduler::Schedule) ---


def _scheduler_schedule_create(logical_id, props, stack_name):
    import ministack.services.scheduler as _sched
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=64)
    group = props.get("GroupName", "default")
    _sched._ensure_default_group()
    body = {
        "ScheduleExpression": props.get("ScheduleExpression", "rate(1 hour)"),
        "FlexibleTimeWindow": props.get("FlexibleTimeWindow", {"Mode": "OFF"}),
        "Target": props.get("Target", {"Arn": "arn:aws:lambda:us-east-1:000000000000:function:noop", "RoleArn": "arn:aws:iam::000000000000:role/noop"}),
        "GroupName": group,
        "State": props.get("State", "ENABLED"),
        "Description": props.get("Description", ""),
    }
    _sched._create_schedule(name, body)
    arn = _sched._schedule_arn(group, name)
    return name, {"Arn": arn}


def _scheduler_schedule_delete(physical_id, props):
    import ministack.services.scheduler as _sched
    group = props.get("GroupName", "default")
    key = f"{group}/{physical_id}"
    sched = _sched._schedules.pop(key, None)
    if sched:
        _sched._tags.pop(sched.get("Arn", ""), None)


def _scheduler_group_create(logical_id, props, stack_name):
    import ministack.services.scheduler as _sched
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=64)
    _sched._create_schedule_group(name, {"Tags": props.get("Tags", [])})
    arn = _sched._group_arn(name)
    return name, {"Arn": arn}


def _scheduler_group_delete(physical_id, props):
    import ministack.services.scheduler as _sched
    # Cascade delete child schedules (matches REST API behavior)
    keys_to_delete = [k for k, v in _sched._schedules.items() if v["GroupName"] == physical_id]
    for k in keys_to_delete:
        arn = _sched._schedules[k]["Arn"]
        del _sched._schedules[k]
        _sched._tags.pop(arn, None)
    group = _sched._schedule_groups.pop(physical_id, None)
    if group:
        _sched._tags.pop(group.get("Arn", ""), None)


# --- EKS Cluster ---

def _eks_cluster_create(logical_id, props, stack_name):
    import ministack.services.eks as _eks
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=100)
    body = {
        "name": name,
        "version": props.get("Version", "1.30"),
        "roleArn": props.get("RoleArn", f"arn:aws:iam::{get_account_id()}:role/eks-role"),
        "resourcesVpcConfig": props.get("ResourcesVpcConfig", {}),
        "tags": {t["Key"]: t["Value"] for t in props.get("Tags", [])},
    }
    _eks._create_cluster(body)
    arn = _eks._cluster_arn(name)
    cluster = _eks._clusters.get(name, {})
    return name, {
        "Arn": arn,
        "Endpoint": cluster.get("endpoint", ""),
        "CertificateAuthorityData": cluster.get("certificateAuthority", {}).get("data", ""),
        "ClusterSecurityGroupId": cluster.get("resourcesVpcConfig", {}).get("clusterSecurityGroupId", ""),
        "OpenIdConnectIssuerUrl": cluster.get("identity", {}).get("oidc", {}).get("issuer", ""),
    }


def _eks_cluster_delete(physical_id, props):
    import ministack.services.eks as _eks
    _eks._delete_cluster(physical_id)


def _eks_nodegroup_create(logical_id, props, stack_name):
    import ministack.services.eks as _eks
    cluster_name = props.get("ClusterName", "")
    ng_name = props.get("NodegroupName") or _physical_name(stack_name, logical_id, max_len=63)
    body = {
        "nodegroupName": ng_name,
        "scalingConfig": props.get("ScalingConfig", {"minSize": 1, "maxSize": 2, "desiredSize": 1}),
        "instanceTypes": props.get("InstanceTypes", ["t3.medium"]),
        "subnets": props.get("Subnets", []),
        "nodeRole": props.get("NodeRole", f"arn:aws:iam::{get_account_id()}:role/eks-node-role"),
        "amiType": props.get("AmiType", "AL2_x86_64"),
        "diskSize": props.get("DiskSize", 20),
        "labels": props.get("Labels", {}),
        "tags": {t["Key"]: t["Value"] for t in props.get("Tags", [])},
    }
    _eks._create_nodegroup(cluster_name, body)
    key = f"{cluster_name}/{ng_name}"
    ng = _eks._nodegroups.get(key, {})
    arn = ng.get("nodegroupArn", "")
    return ng_name, {"Arn": arn}


def _eks_nodegroup_delete(physical_id, props):
    import ministack.services.eks as _eks
    cluster_name = props.get("ClusterName", "")
    _eks._delete_nodegroup(cluster_name, physical_id)


# --- EventBridge EventBus ---

def _eb_event_bus_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=256)
    if name in _eb._event_buses:
        raise ValueError(f"EventBus already exists: {name}")
    data = {
        "Name": name,
        "Description": props.get("Description", ""),
        "Tags": props.get("Tags", []),
    }
    _eb._create_event_bus(data)
    arn = f"arn:aws:events:{get_region()}:{get_account_id()}:event-bus/{name}"
    return name, {"Arn": arn, "Name": name}


def _eb_event_bus_delete(physical_id, props):
    if physical_id == "default" or physical_id not in _eb._event_buses:
        return
    _eb._delete_event_bus({"Name": physical_id})



# --- Kinesis Stream ---

def _kinesis_stream_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id, lowercase=True, max_len=128)
    smd = props.get("StreamModeDetails") or {}
    stream_mode = smd.get("StreamMode", "PROVISIONED") if isinstance(smd, dict) else "PROVISIONED"
    if stream_mode == "ON_DEMAND":
        shard_count = 4
    else:
        shard_count = int(props.get("ShardCount", 1))
    if shard_count < 1:
        shard_count = 1

    retention = int(props.get("RetentionPeriodHours", 24))
    if retention < 24:
        retention = 24
    if retention > 8760:
        retention = 8760

    arn = f"arn:aws:kinesis:{get_region()}:{get_account_id()}:stream/{name}"
    stream_id = new_uuid()

    _kinesis._streams[name] = {
        "StreamName": name,
        "StreamARN": arn,
        "StreamStatus": "ACTIVE",
        "StreamModeDetails": {"StreamMode": stream_mode},
        "RetentionPeriodHours": retention,
        "shards": _kinesis._build_shards(shard_count),
        "tags": {},
        "CreationTimestamp": int(time.time()),
        "EncryptionType": "NONE",
    }
    return name, {"Arn": arn, "StreamId": stream_id}


def _kinesis_stream_delete(physical_id, props):
    stream = _kinesis._streams.pop(physical_id, None)
    if not stream:
        return
    for tok in [t for t, s in _kinesis._shard_iterators.items() if s["stream"] == physical_id]:
        del _kinesis._shard_iterators[tok]
    for carn in [a for a, c in _kinesis._consumers.items() if c["StreamARN"] == stream["StreamARN"]]:
        del _kinesis._consumers[carn]


# --- Lambda Permission ---

def _lambda_function_for_cfn_ref(function_ref: str) -> tuple[dict | None, str, str, str | None]:
    if isinstance(function_ref, str) and function_ref.startswith("arn:"):
        name, qualifier = _lambda_svc._resolve_request_scoped_name_and_qualifier(function_ref)
        if name == function_ref:
            return None, function_ref, function_ref, None
        func = _lambda_svc._functions.get(name)
        if not _lambda_svc._function_qualifier_exists(func, qualifier):
            return None, function_ref, function_ref, qualifier
    else:
        name, qualifier = _lambda_svc._resolve_name_and_qualifier(function_ref)
        func = _lambda_svc._functions.get(name)
        if func is not None and not _lambda_svc._function_qualifier_exists(func, qualifier):
            return None, function_ref, function_ref, qualifier

    if func is None:
        return None, function_ref, function_ref, qualifier

    resource_arn = func["config"]["FunctionArn"]
    if qualifier:
        resource_arn = f"{resource_arn}:{qualifier}"
    return func, name, resource_arn, qualifier


def _lambda_permission_create(logical_id, props, stack_name):
    func, _func_name, resource_arn, _qualifier = _lambda_function_for_cfn_ref(props.get("FunctionName", ""))
    if func:
        stmt = {
            "Sid": props.get("Id") or logical_id,
            "Effect": "Allow",
            "Principal": props.get("Principal", "*"),
            "Action": props.get("Action", "lambda:InvokeFunction"),
            "Resource": resource_arn,
        }
        source_arn = props.get("SourceArn")
        if source_arn:
            stmt["Condition"] = {"ArnLike": {"AWS:SourceArn": source_arn}}
        func["policy"]["Statement"].append(stmt)
    pid = f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    return pid, {}


def _lambda_permission_delete(physical_id, props):
    func, _func_name, _resource_arn, _qualifier = _lambda_function_for_cfn_ref(props.get("FunctionName", ""))
    if func:
        sid = props.get("Id") or ""
        func["policy"]["Statement"] = [
            s for s in func["policy"]["Statement"] if s.get("Sid") != sid
        ]


# --- Lambda Version ---

def _lambda_version_create(logical_id, props, stack_name):
    func, func_name, _resource_arn, _qualifier = _lambda_function_for_cfn_ref(props.get("FunctionName", ""))
    if func:
        import copy
        ver_num = func["next_version"]
        func["next_version"] = ver_num + 1
        ver_str = str(ver_num)
        ver_config = copy.deepcopy(func["config"])
        ver_config["Version"] = ver_str
        ver_arn = f"{ver_config['FunctionArn']}"
        func["versions"][ver_str] = {
            "config": ver_config,
            "code_zip": func.get("code_zip"),
        }
        return ver_arn, {"Version": ver_str}
    ver_arn = f"arn:aws:lambda:{get_region()}:{get_account_id()}:function:{func_name}:1"
    return ver_arn, {"Version": "1"}


# --- CloudFormation WaitCondition / WaitConditionHandle (no-ops) ---

def _cfn_wait_condition_create(logical_id, props, stack_name):
    """WaitCondition — no-op, return immediately (no real signalling in local emulation)."""
    pid = f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    return pid, {"Data": "{}"}


def _cfn_wait_condition_handle_create(logical_id, props, stack_name):
    """WaitConditionHandle — no-op, return a presigned-style URL."""
    pid = f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    url = f"https://cloudformation-waitcondition-{get_region()}.s3.amazonaws.com/{pid}"
    return pid, {"Ref": url}


# --- CloudFormation Nested Stack (AWS::CloudFormation::Stack) ---

def _cfn_nested_stack_deploy(logical_id, props, parent_stack_name, *,
                             previous_physical_id=None, previous_props=None):
    """Provision an `AWS::CloudFormation::Stack` nested-stack resource.

    Mirrors the synchronous core of `stacks._deploy_stack_async` but runs
    inline so the parent's deploy loop can read the child's Outputs (exposed
    as `Outputs.<Name>` keys on the returned attrs dict so `Fn::GetAtt:
    [Nested, Outputs.X]` resolves natively).

    Returns ``(child_stack_id, attrs)`` where ``attrs["Outputs.<Name>"]``
    carries each output value. ``Outputs.<Name>`` keys match the dotted
    sub-attribute form CDK and console-built templates emit.
    """
    import copy

    from ministack.core.responses import get_account_id, get_region, new_uuid
    from ministack.services.cloudformation import (
        _stack_events,
        _stacks,
    )
    from ministack.services.cloudformation.engine import (
        _evaluate_conditions,
        _parse_template,
        _resolve_parameters,
        _resolve_refs,
        _topological_sort,
    )
    from ministack.services.cloudformation.helpers import _resolve_template
    from ministack.services.cloudformation.stacks import _add_event

    template_url = props.get("TemplateURL")
    if not template_url:
        raise ValueError(
            "AWS::CloudFormation::Stack requires TemplateURL "
            "(inline TemplateBody is not supported by real AWS either)"
        )

    template_body, err = _resolve_template({"TemplateURL": [template_url]})
    if err is not None:
        raise ValueError(f"Failed to fetch nested-stack template: {template_url}")
    if not template_body:
        raise ValueError(f"Nested-stack template empty at {template_url}")

    template = _parse_template(template_body)

    raw_param_props = props.get("Parameters") or {}
    if isinstance(raw_param_props, dict):
        provided_params = [
            {"Key": k, "Value": "" if v is None else str(v)}
            for k, v in raw_param_props.items()
        ]
    else:
        provided_params = []
    param_values = _resolve_parameters(template, provided_params)

    is_update = previous_physical_id is not None
    if is_update and previous_physical_id in _stacks:
        child_name = previous_physical_id
        previous_stack_snapshot = copy.deepcopy(_stacks[child_name])
    else:
        child_name = f"{parent_stack_name}-{logical_id}-{new_uuid()[:12]}"
        previous_stack_snapshot = None

    child_stack_id = (
        f"arn:aws:cloudformation:{get_region()}:{get_account_id()}:"
        f"stack/{child_name}/{new_uuid()}"
    )
    if previous_stack_snapshot:
        child_stack_id = previous_stack_snapshot.get("StackId", child_stack_id)

    status_prefix = "UPDATE" if is_update else "CREATE"
    child_stack = {
        "StackName": child_name,
        "StackId": child_stack_id,
        "StackStatus": f"{status_prefix}_IN_PROGRESS",
        "StackStatusReason": "",
        "CreationTime": now_iso(),
        "LastUpdatedTime": now_iso(),
        "Description": template.get("Description", ""),
        "Parameters": [
            {"ParameterKey": k, "ParameterValue": v["Value"], "NoEcho": v["NoEcho"]}
            for k, v in param_values.items()
        ],
        "Tags": [],
        "Outputs": [],
        "DisableRollback": True,
        "_resources": (previous_stack_snapshot.get("_resources", {})
                       if previous_stack_snapshot else {}),
        "_template": template,
        "_template_body": template_body,
        "_resolved_params": param_values,
        "_conditions": _evaluate_conditions(template, param_values),
        "_parent_stack_name": parent_stack_name,
        "RootId": _cr_stack_id(parent_stack_name),
        "ParentId": _cr_stack_id(parent_stack_name),
    }
    _stacks[child_name] = child_stack
    _stack_events.setdefault(child_stack_id, [])

    _add_event(child_stack_id, child_name, child_name,
               "AWS::CloudFormation::Stack", f"{status_prefix}_IN_PROGRESS",
               physical_id=child_stack_id)

    mappings = template.get("Mappings", {})
    conditions = child_stack["_conditions"]
    resources_defs = template.get("Resources", {})
    outputs_defs = template.get("Outputs", {})

    try:
        ordered = _topological_sort(resources_defs, conditions)
    except ValueError as exc:
        child_stack["StackStatus"] = f"{status_prefix}_FAILED"
        child_stack["StackStatusReason"] = str(exc)
        _add_event(child_stack_id, child_name, child_name,
                   "AWS::CloudFormation::Stack", f"{status_prefix}_FAILED",
                   str(exc), child_stack_id)
        raise

    provisioned: dict = child_stack["_resources"]
    prev_resources = (previous_stack_snapshot.get("_resources", {})
                      if previous_stack_snapshot else {})

    for child_logical_id in ordered:
        res_def = resources_defs[child_logical_id]
        cond = res_def.get("Condition")
        if cond and not conditions.get(cond, True):
            continue
        resource_type = res_def.get("Type", "AWS::CloudFormation::CustomResource")
        raw_props = res_def.get("Properties", {})
        resolved_props = _resolve_refs(
            copy.deepcopy(raw_props), provisioned, param_values,
            conditions, mappings, child_name, child_stack_id,
        )
        if isinstance(resolved_props, dict):
            resolved_props = {k: v for k, v in resolved_props.items()
                              if v is not _NO_VALUE_SENTINEL()}

        _add_event(child_stack_id, child_name, child_logical_id, resource_type,
                   f"{status_prefix}_IN_PROGRESS")
        try:
            prev = prev_resources.get(child_logical_id)
            if prev:
                physical_id, attrs = _update_resource(
                    resource_type, prev.get("PhysicalResourceId", child_logical_id),
                    prev.get("Properties", {}), resolved_props, child_name,
                    child_logical_id,
                )
            else:
                physical_id, attrs = _provision_resource(
                    resource_type, child_logical_id, resolved_props, child_name,
                )
        except Exception as exc:
            child_stack["StackStatus"] = f"{status_prefix}_FAILED"
            child_stack["StackStatusReason"] = (
                f"Resource {child_logical_id} failed: {exc}"
            )
            _add_event(child_stack_id, child_name, child_logical_id, resource_type,
                       f"{status_prefix}_FAILED", str(exc))
            raise

        provisioned[child_logical_id] = {
            "PhysicalResourceId": physical_id,
            "ResourceType": resource_type,
            "ResourceStatus": f"{status_prefix}_COMPLETE",
            "LogicalResourceId": child_logical_id,
            "Properties": resolved_props,
            "Attributes": attrs,
            "Timestamp": now_iso(),
        }
        _add_event(child_stack_id, child_name, child_logical_id, resource_type,
                   f"{status_prefix}_COMPLETE", physical_id=physical_id)

    if is_update:
        for stale_id in set(prev_resources) - set(provisioned):
            old = prev_resources[stale_id]
            try:
                _delete_resource(old.get("ResourceType", ""),
                                 old.get("PhysicalResourceId", ""),
                                 old.get("Properties", {}),
                                 child_name, stale_id)
            except Exception as exc:
                logger.warning("Nested-stack %s: failed to delete pruned %s: %s",
                               child_name, stale_id, exc)

    resolved_outputs = []
    output_attrs: dict[str, str] = {}
    for out_name, out_def in outputs_defs.items():
        cond = out_def.get("Condition")
        if cond and not conditions.get(cond, True):
            continue
        out_value = _resolve_refs(
            copy.deepcopy(out_def.get("Value", "")),
            provisioned, param_values, conditions,
            mappings, child_name, child_stack_id,
        )
        resolved_outputs.append({
            "OutputKey": out_name,
            "OutputValue": str(out_value),
            "Description": out_def.get("Description", ""),
        })
        output_attrs[f"Outputs.{out_name}"] = str(out_value)

    child_stack["Outputs"] = resolved_outputs
    child_stack["StackStatus"] = f"{status_prefix}_COMPLETE"
    _add_event(child_stack_id, child_name, child_name,
               "AWS::CloudFormation::Stack", f"{status_prefix}_COMPLETE",
               physical_id=child_stack_id)

    # Real AWS: Ref of AWS::CloudFormation::Stack returns the child StackId
    # (ARN), not the stack name. DescribeStacks/_delete handlers below accept
    # either form for lookup so callers using Ref->DescribeStacks keep working.
    return child_stack_id, output_attrs


def _NO_VALUE_SENTINEL():
    from ministack.services.cloudformation.engine import _NO_VALUE
    return _NO_VALUE


def _cfn_nested_stack_create(logical_id, props, stack_name):
    return _cfn_nested_stack_deploy(logical_id, props, stack_name)


def _cfn_nested_stack_update(physical_id, old_props, new_props, stack_name):
    return _cfn_nested_stack_deploy(
        physical_id, new_props, stack_name,
        previous_physical_id=_nested_stack_lookup_name(physical_id),
        previous_props=old_props,
    )


def _nested_stack_lookup_name(physical_id_or_arn):
    """Resolve a nested-stack physical id (StackId ARN or stack name) to its
    `_stacks` dict key. Returns the input if no ARN match is found."""
    from ministack.services.cloudformation import _stacks
    if physical_id_or_arn in _stacks:
        return physical_id_or_arn
    for name, stk in _stacks.items():
        if stk.get("StackId") == physical_id_or_arn:
            return name
    return physical_id_or_arn


def _cfn_nested_stack_delete(physical_id, props):
    from ministack.services.cloudformation import _exports, _stacks
    child_name = _nested_stack_lookup_name(physical_id)
    child_stack = _stacks.get(child_name)
    if not child_stack:
        return

    child_stack_id = child_stack.get("StackId", physical_id)
    resources = child_stack.get("_resources", {})
    template = child_stack.get("_template", {})
    res_defs = template.get("Resources", {}) if template else {}
    conditions = child_stack.get("_conditions", {})
    try:
        from ministack.services.cloudformation.engine import _topological_sort
        ordered = (_topological_sort(res_defs, conditions)
                   if res_defs else list(resources.keys()))
    except Exception:
        ordered = list(resources.keys())

    for child_logical_id in reversed(ordered):
        res = resources.get(child_logical_id)
        if not res:
            continue
        try:
            _delete_resource(
                res.get("ResourceType", ""),
                res.get("PhysicalResourceId", ""),
                res.get("Properties", {}),
                child_name, child_logical_id,
            )
        except Exception as exc:
            logger.warning("Nested-stack %s: delete of %s failed: %s",
                           child_name, child_logical_id, exc)

    for out in child_stack.get("Outputs", []):
        export_name = out.get("ExportName")
        if export_name:
            _exports.pop(export_name, None)

    child_stack["StackStatus"] = "DELETE_COMPLETE"
    child_stack["_resources"] = {}
    # Leave the stack entry in _stacks so DescribeStacks on the child id still
    # returns DELETE_COMPLETE, matching real AWS behaviour for nested stacks
    # whose parents are torn down.


# --- CloudFormation Custom Resource ---

def _cr_stack_id(stack_name: str) -> str:
    """Return the StackId for stack_name, falling back to a synthesised ARN."""
    from ministack.services.cloudformation import _stacks
    stack = _stacks.get(stack_name) or {}
    return stack.get(
        "StackId",
        f"arn:aws:cloudformation:{get_region()}:{get_account_id()}:stack/{stack_name}/unknown",
    )


def _custom_resource_create(logical_id, props, stack_name, resource_type="AWS::CloudFormation::CustomResource"):
    from ministack.services.cloudformation import custom_resource as _cr
    return _cr.invoke_custom_resource(
        "Create", logical_id, props, stack_name, _cr_stack_id(stack_name), resource_type,
    )


def _custom_resource_update(physical_id, old_props, new_props, stack_name,
                             logical_id=None, resource_type="AWS::CloudFormation::CustomResource"):
    from ministack.services.cloudformation import custom_resource as _cr
    return _cr.invoke_custom_resource(
        "Update", logical_id or physical_id, new_props, stack_name,
        _cr_stack_id(stack_name), resource_type,
        physical_id=physical_id, old_props=old_props,
    )


def _custom_resource_delete(physical_id, props, stack_name=None, logical_id=None,
                             resource_type="AWS::CloudFormation::CustomResource"):
    # CDK uses a marker physical ID when Create failed — treat as no-op
    if not physical_id or physical_id == "FAILED_CREATE_MARKER":
        return
    sname = stack_name or ""
    from ministack.services.cloudformation import custom_resource as _cr
    _cr.invoke_custom_resource(
        "Delete", logical_id or physical_id, props, sname,
        _cr_stack_id(sname), resource_type,
        physical_id=physical_id,
    )


# --- API Gateway REST API ---

def _apigw_rest_api_create(logical_id, props, stack_name):
    data = {
        "description": props.get("Description", ""),
        "endpointConfiguration": props.get("EndpointConfiguration", {"types": ["REGIONAL"]}),
        "binaryMediaTypes": props.get("BinaryMediaTypes", []),
        "minimumCompressionSize": props.get("MinimumCompressionSize"),
        "policy": props.get("Policy"),
        "tags": {t["Key"]: t["Value"] for t in props.get("Tags", [])},
    }
    if props.get("Name"):
        data["name"] = props["Name"]

    body_spec = props.get("Body")
    if isinstance(body_spec, dict):
        api_id = _apigw_v1._import_rest_api(body_spec, data)
    else:
        data.setdefault("name", _physical_name(stack_name, logical_id, max_len=64))
        status, headers, body = _apigw_v1._create_rest_api(data)
        api = json.loads(body) if isinstance(body, bytes) else json.loads(body)
        api_id = api.get("id", "")

    # Find root resource id
    root_id = ""
    for rid, res in _apigw_v1._resources.get(api_id, {}).items():
        if res.get("path") == "/":
            root_id = rid
            break
    return api_id, {
        "RootResourceId": root_id,
        "Arn": f"arn:aws:apigateway:{get_region()}::/restapis/{api_id}",
    }


def _apigw_rest_api_delete(physical_id, props):
    _apigw_v1._delete_rest_api(physical_id)


# --- API Gateway Resource ---

def _apigw_resource_create(logical_id, props, stack_name):
    api_id = props.get("RestApiId", "")
    parent_id = props.get("ParentId", "")
    path_part = props.get("PathPart", "")
    data = {"pathPart": path_part}
    status, headers, body = _apigw_v1._create_resource(api_id, parent_id, data)
    resource = json.loads(body) if isinstance(body, bytes) else json.loads(body)
    resource_id = resource.get("id", "")
    return resource_id, {"ResourceId": resource_id}


def _apigw_resource_delete(physical_id, props):
    api_id = props.get("RestApiId", "")
    _apigw_v1._delete_resource(api_id, physical_id)


# --- API Gateway Method ---

def _apigw_method_create(logical_id, props, stack_name):
    api_id = props.get("RestApiId", "")
    resource_id = props.get("ResourceId", "")
    http_method = props.get("HttpMethod", "ANY")
    data = {
        "authorizationType": props.get("AuthorizationType", "NONE"),
        "authorizerId": props.get("AuthorizerId"),
        "apiKeyRequired": props.get("ApiKeyRequired", False),
        "operationName": props.get("OperationName", ""),
        "requestParameters": props.get("RequestParameters", {}),
        "requestModels": props.get("RequestModels", {}),
    }
    _apigw_v1._put_method(api_id, resource_id, http_method, data)

    # Also set Integration if provided
    integration = props.get("Integration")
    if integration:
        int_data = {
            "type": integration.get("Type", "AWS_PROXY"),
            "httpMethod": integration.get("IntegrationHttpMethod", "POST"),
            "uri": integration.get("Uri", ""),
            "connectionType": integration.get("ConnectionType", "INTERNET"),
            "credentials": integration.get("Credentials"),
            "requestParameters": integration.get("RequestParameters", {}),
            "requestTemplates": integration.get("RequestTemplates", {}),
            "passthroughBehavior": integration.get("PassthroughBehavior", "WHEN_NO_MATCH"),
            "timeoutInMillis": integration.get("TimeoutInMillis", 29000),
            "cacheKeyParameters": integration.get("CacheKeyParameters", []),
        }
        _apigw_v1._put_integration(api_id, resource_id, http_method, int_data)

    pid = f"{api_id}-{resource_id}-{http_method}"
    return pid, {}


def _apigw_method_delete(physical_id, props):
    api_id = props.get("RestApiId", "")
    resource_id = props.get("ResourceId", "")
    http_method = props.get("HttpMethod", "ANY")
    _apigw_v1._delete_method(api_id, resource_id, http_method)


# --- API Gateway Model ---

def _apigw_model_schema(schema):
    """Convert CFN's Json-valued Schema to API Gateway's string shape."""
    if schema is None:
        return ""
    if isinstance(schema, str):
        return schema
    return json.dumps(schema, separators=(",", ":"), ensure_ascii=False)


def _apigw_model_create(logical_id, props, stack_name):
    api_id = props.get("RestApiId", "")
    model_name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=128)
    data = {
        "name": model_name,
        "description": props.get("Description", ""),
        "contentType": props.get("ContentType", "application/json"),
        "schema": _apigw_model_schema(props.get("Schema")),
    }
    status, headers, body = _apigw_v1._create_model(api_id, data)
    if status >= 400:
        raise ValueError(f"AWS::ApiGateway::Model create failed: {body!r}")
    model = json.loads(body)
    # AWS CloudFormation Ref returns the model name, not the API-generated id.
    return model.get("name", model_name), {}


def _apigw_model_update(physical_id, old_props, new_props, stack_name):
    old_api_id = old_props.get("RestApiId", "")
    new_api_id = new_props.get("RestApiId", "")
    old_content_type = old_props.get("ContentType", "application/json")
    new_content_type = new_props.get("ContentType", "application/json")
    new_name = new_props.get("Name") or physical_id

    # CloudFormation replaces models when their API, name, or content type
    # changes. The stack engine delegates that replacement lifecycle here.
    if (old_api_id != new_api_id or physical_id != new_name
            or old_content_type != new_content_type):
        _apigw_v1._delete_model(old_api_id, physical_id)
        return _apigw_model_create(physical_id, new_props, stack_name)

    patch_operations = []
    old_description = old_props.get("Description", "")
    new_description = new_props.get("Description", "")
    if old_description != new_description:
        patch_operations.append({"op": "replace", "path": "/description", "value": new_description})

    old_schema = _apigw_model_schema(old_props.get("Schema"))
    new_schema = _apigw_model_schema(new_props.get("Schema"))
    if old_schema != new_schema:
        patch_operations.append({"op": "replace", "path": "/schema", "value": new_schema})

    if patch_operations:
        status, headers, body = _apigw_v1._update_model(new_api_id, physical_id, {
            "patchOperations": patch_operations,
        })
        if status >= 400:
            raise ValueError(f"AWS::ApiGateway::Model update failed: {body!r}")
    return physical_id, {}


def _apigw_model_delete(physical_id, props):
    api_id = props.get("RestApiId", "")
    # Stack deletion is idempotent, including when its RestApi was already
    # removed or a replacement cleaned up the prior model.
    if physical_id in _apigw_v1._models.get(api_id, {}):
        _apigw_v1._delete_model(api_id, physical_id)


# --- API Gateway Authorizer ---

def _apigw_authorizer_create(logical_id, props, stack_name):
    """Provision an AWS::ApiGateway::Authorizer.

    Maps CFN properties to the existing apigateway_v1 authorizer store:
    Name, Type (TOKEN / REQUEST / COGNITO_USER_POOLS), AuthorizerUri,
    AuthorizerCredentials, IdentitySource, IdentityValidationExpression,
    AuthorizerResultTtlInSeconds, ProviderARNs, RestApiId. ``AuthType`` is
    documented in the AWS CFN spec as informational only; the underlying
    apigateway_v1._create_authorizer record does not currently expose it,
    so the field is dropped here.
    """
    api_id = props.get("RestApiId", "")
    data = {
        "name": props.get("Name", logical_id),
        "type": props.get("Type", "TOKEN"),
        "authorizerUri": props.get("AuthorizerUri", ""),
        "authorizerCredentials": props.get("AuthorizerCredentials"),
        "identitySource": props.get("IdentitySource", "method.request.header.Authorization"),
        "identityValidationExpression": props.get("IdentityValidationExpression", ""),
        "authorizerResultTtlInSeconds": props.get("AuthorizerResultTtlInSeconds", 300),
        "providerARNs": props.get("ProviderARNs", []),
    }
    status, headers, body = _apigw_v1._create_authorizer(api_id, data)
    if status >= 400:
        raise ValueError(f"AWS::ApiGateway::Authorizer create failed: {body!r}")
    authorizer = json.loads(body) if isinstance(body, (bytes, bytearray)) else json.loads(body)
    authorizer_id = authorizer.get("id", "")
    return authorizer_id, {"AuthorizerId": authorizer_id}


def _apigw_authorizer_delete(physical_id, props):
    api_id = props.get("RestApiId", "")
    authorizers = _apigw_v1._authorizers_v1.get(api_id, {})
    authorizers.pop(physical_id, None)


# --- API Gateway Deployment ---

def _apigw_deployment_create(logical_id, props, stack_name):
    api_id = props.get("RestApiId", "")
    data = {
        "description": props.get("Description", ""),
        "stageName": props.get("StageName"),
        "stageDescription": props.get("StageDescription", ""),
    }
    status, headers, body = _apigw_v1._create_deployment(api_id, data)
    deployment = json.loads(body) if isinstance(body, bytes) else json.loads(body)
    deployment_id = deployment.get("id", "")
    return deployment_id, {"DeploymentId": deployment_id}


def _apigw_deployment_delete(physical_id, props):
    api_id = props.get("RestApiId", "")
    _apigw_v1._delete_deployment(api_id, physical_id)


# --- API Gateway Stage ---

def _apigw_stage_create(logical_id, props, stack_name):
    api_id = props.get("RestApiId", "")
    stage_name = props.get("StageName", "")
    data = {
        "stageName": stage_name,
        "deploymentId": props.get("DeploymentId", ""),
        "description": props.get("Description", ""),
        "variables": props.get("Variables", {}),
        "methodSettings": props.get("MethodSettings", {}),
        "tracingEnabled": props.get("TracingEnabled", False),
        "tags": {t["Key"]: t["Value"] for t in props.get("Tags", [])},
    }
    _apigw_v1._create_stage(api_id, data)
    # AWS::ApiGateway::Stage Ref returns the stage name. The physical ID feeds
    # MiniStack's generic Ref resolver, so it must not include the REST API ID.
    return stage_name, {"StageName": stage_name}


def _apigw_stage_delete(physical_id, props):
    api_id = props.get("RestApiId", "")
    stage_name = props.get("StageName", "")
    _apigw_v1._delete_stage(api_id, stage_name)


# --- API Gateway BasePathMapping ---

def _apigw_base_path_mapping_identity(props):
    domain_name = props.get("DomainName", "")
    base_path = props.get("BasePath") or "(none)"
    return domain_name, base_path, f"{domain_name}/{base_path}"


def _apigw_base_path_mapping_create(logical_id, props, stack_name):
    domain_name, base_path, physical_id = _apigw_base_path_mapping_identity(props)
    data = {
        "basePath": base_path,
        "restApiId": props.get("RestApiId", ""),
        "stage": props.get("Stage", ""),
    }
    status, _headers, body = _apigw_v1._create_base_path_mapping(domain_name, data)
    if status >= 400:
        raise ValueError(f"AWS::ApiGateway::BasePathMapping create failed: {body!r}")
    return physical_id, {}


def _apigw_base_path_mapping_update(physical_id, old_props, new_props, stack_name):
    old_domain, old_base_path, _old_id = _apigw_base_path_mapping_identity(old_props)
    new_domain, new_base_path, _new_id = _apigw_base_path_mapping_identity(new_props)

    # BasePath and DomainName require replacement; RestApiId and Stage update
    # without interruption. The in-memory API Gateway implementation treats a
    # create against an existing key as an upsert, which gives both paths the
    # same atomic create-before-delete behavior here.
    new_id, attrs = _apigw_base_path_mapping_create(physical_id, new_props, stack_name)
    if (new_domain, new_base_path) != (old_domain, old_base_path):
        _apigw_v1._delete_base_path_mapping(old_domain, old_base_path)
    return new_id, attrs


def _apigw_base_path_mapping_delete(physical_id, props):
    domain_name, base_path, _mapping_id = _apigw_base_path_mapping_identity(props)
    _apigw_v1._delete_base_path_mapping(domain_name, base_path)


def _apigw_account_create(logical_id, props, stack_name):
    """``AWS::ApiGateway::Account`` is a singleton per AWS account storing the
    IAM role API Gateway uses to push logs to CloudWatch. CDK's
    ``RestApi({ cloudWatchRole: true })`` generates this automatically.

    We persist ``CloudWatchRoleArn`` into the same store the runtime
    ``UpdateAccount`` API writes to, so a subsequent ``GetAccount`` call
    round-trips the value. No real side effect — the role isn't used.
    """
    role_arn = props.get("CloudWatchRoleArn")
    settings = dict(_apigw_v1._account_settings.get("settings") or {})
    if role_arn is not None:
        settings["cloudwatchRoleArn"] = role_arn
    _apigw_v1._account_settings["settings"] = settings
    return logical_id, {}


def _apigw_account_delete(physical_id, props):
    settings = dict(_apigw_v1._account_settings.get("settings") or {})
    settings.pop("cloudwatchRoleArn", None)
    _apigw_v1._account_settings["settings"] = settings


# --- API Gateway DomainName ---

def _apigw_domain_name_create(logical_id, props, stack_name):
    """Provision an ``AWS::ApiGateway::DomainName`` through API Gateway v1."""
    endpoint = props.get("EndpointConfiguration") or {}
    endpoint_configuration = {
        "types": endpoint.get("Types", ["REGIONAL"]),
    }
    if "IpAddressType" in endpoint:
        endpoint_configuration["ipAddressType"] = endpoint["IpAddressType"]
    if "VpcEndpointIds" in endpoint:
        endpoint_configuration["vpcEndpointIds"] = endpoint["VpcEndpointIds"]

    mutual_tls = props.get("MutualTlsAuthentication") or {}
    mutual_tls_configuration = {}
    if "TruststoreUri" in mutual_tls:
        mutual_tls_configuration["truststoreUri"] = mutual_tls["TruststoreUri"]
    if "TruststoreVersion" in mutual_tls:
        mutual_tls_configuration["truststoreVersion"] = mutual_tls["TruststoreVersion"]

    data = {
        "domainName": props.get("DomainName", ""),
        "endpointConfiguration": endpoint_configuration,
        "tags": {tag["Key"]: tag["Value"] for tag in props.get("Tags", [])},
    }
    property_map = {
        "CertificateArn": "certificateArn",
        "EndpointAccessMode": "endpointAccessMode",
        "OwnershipVerificationCertificateArn": "ownershipVerificationCertificateArn",
        "RegionalCertificateArn": "regionalCertificateArn",
        "RoutingMode": "routingMode",
        "SecurityPolicy": "securityPolicy",
    }
    for cfn_name, api_name in property_map.items():
        if cfn_name in props:
            data[api_name] = props[cfn_name]
    if mutual_tls_configuration:
        data["mutualTlsAuthentication"] = mutual_tls_configuration

    status, _headers, body = _apigw_v1._create_domain_name(data)
    if status >= 400:
        raise ValueError(f"AWS::ApiGateway::DomainName create failed: {body!r}")
    domain = json.loads(body)
    domain_name = domain["domainName"]
    attrs = {
        "DistributionDomainName": domain["distributionDomainName"],
        "DistributionHostedZoneId": domain["distributionHostedZoneId"],
        "DomainNameArn": f"arn:aws:apigateway:{get_region()}::/domainnames/{domain_name}",
        "RegionalDomainName": domain["regionalDomainName"],
        "RegionalHostedZoneId": domain["regionalHostedZoneId"],
    }
    return domain_name, attrs


def _apigw_domain_name_update(physical_id, old_props, new_props, stack_name):
    existing_mappings = _apigw_v1._base_path_mappings.get(physical_id)
    new_domain_name = new_props.get("DomainName", "")
    new_id, attrs = _apigw_domain_name_create(physical_id, new_props, stack_name)
    if new_domain_name != physical_id:
        _apigw_domain_name_delete(physical_id, old_props)
    elif existing_mappings is not None:
        # The native create helper initializes this collection. Preserve
        # dependent mappings while mutable domain properties update in place.
        _apigw_v1._base_path_mappings[physical_id] = existing_mappings
    return new_id, attrs


def _apigw_domain_name_delete(physical_id, props):
    # Rollback and repeated stack deletion should remain harmless when the
    # underlying custom domain has already been removed.
    if physical_id in _apigw_v1._domain_names:
        _apigw_v1._delete_domain_name(physical_id)


# --- API Gateway GatewayResponse ---

def _apigw_gateway_response_create(logical_id, props, stack_name):
    """Provision an ``AWS::ApiGateway::GatewayResponse`` customization."""
    api_id = props.get("RestApiId", "")
    response_type = props.get("ResponseType", "")
    data = {
        "responseParameters": props.get("ResponseParameters", {}),
        "responseTemplates": props.get("ResponseTemplates", {}),
    }
    if "StatusCode" in props:
        data["statusCode"] = props["StatusCode"]

    status, _headers, body = _apigw_v1._put_gateway_response(api_id, response_type, data)
    if status >= 400:
        raise ValueError(f"AWS::ApiGateway::GatewayResponse create failed: {body!r}")

    physical_id = f"{api_id}/{response_type}"
    return physical_id, {"Id": physical_id}


def _apigw_gateway_response_update(physical_id, old_props, new_props, stack_name):
    # RestApiId and ResponseType require replacement in the AWS CFN resource
    # specification. Create the replacement first, then reset the old
    # customization so a failed create cannot destroy the working resource.
    if any(new_props.get(key) != old_props.get(key) for key in ("RestApiId", "ResponseType")):
        new_id, attrs = _apigw_gateway_response_create(physical_id, new_props, stack_name)
        _apigw_gateway_response_delete(physical_id, old_props)
        return new_id, attrs

    _new_id, attrs = _apigw_gateway_response_create(physical_id, new_props, stack_name)
    return physical_id, attrs


def _apigw_gateway_response_delete(physical_id, props):
    api_id = props.get("RestApiId", "")
    response_type = props.get("ResponseType", "")
    # Deletion resets the customization to API Gateway's generated default.
    # Keep CloudFormation cleanup idempotent when the parent REST API has
    # already gone away during rollback.
    if api_id in _apigw_v1._rest_apis:
        _apigw_v1._delete_gateway_response(api_id, response_type)


# --- API Gateway DocumentationPart ---

def _apigw_documentation_part_create(logical_id, props, stack_name):
    """Provision an ``AWS::ApiGateway::DocumentationPart``."""
    api_id = props.get("RestApiId", "")
    cfn_location = props.get("Location", {})
    location_keys = {
        "Type": "type",
        "Path": "path",
        "Method": "method",
        "StatusCode": "statusCode",
        "Name": "name",
    }
    data = {
        "location": {
            api_key: cfn_location[cfn_key]
            for cfn_key, api_key in location_keys.items()
            if cfn_key in cfn_location
        },
        "properties": props.get("Properties"),
    }
    status, _headers, body = _apigw_v1._create_documentation_part(api_id, data)
    if status >= 400:
        raise ValueError(f"AWS::ApiGateway::DocumentationPart create failed: {body!r}")

    part = json.loads(body) if isinstance(body, (bytes, bytearray)) else json.loads(body)
    part_id = part.get("id", "")
    # AWS exposes only DocumentationPartId via Fn::GetAtt for this resource.
    return part_id, {"DocumentationPartId": part_id}


def _apigw_documentation_part_update(physical_id, old_props, new_props, stack_name):
    # RestApiId and Location require replacement in the AWS CFN resource
    # specification; Properties is mutable in place.
    if any(new_props.get(key) != old_props.get(key) for key in ("RestApiId", "Location")):
        new_id, attrs = _apigw_documentation_part_create(physical_id, new_props, stack_name)
        _apigw_documentation_part_delete(physical_id, old_props)
        return new_id, attrs

    if new_props.get("Properties") != old_props.get("Properties"):
        api_id = new_props.get("RestApiId", "")
        data = {
            "patchOperations": [
                {
                    "op": "replace",
                    "path": "/properties",
                    "value": new_props.get("Properties", ""),
                },
            ],
        }
        status, _headers, body = _apigw_v1._update_documentation_part(
            api_id, physical_id, data,
        )
        if status >= 400:
            raise ValueError(f"AWS::ApiGateway::DocumentationPart update failed: {body!r}")
    return physical_id, {"DocumentationPartId": physical_id}


def _apigw_documentation_part_delete(physical_id, props):
    api_id = props.get("RestApiId", "")
    parts = _apigw_v1._documentation_parts.get(api_id, {})
    # Keep rollback and parent-first cleanup idempotent.
    if physical_id in parts:
        _apigw_v1._delete_documentation_part(api_id, physical_id)


# --- Lambda EventSourceMapping ---

def _lambda_esm_create(logical_id, props, stack_name):
    func, func_name, resource_arn, qualifier = _lambda_function_for_cfn_ref(props.get("FunctionName", ""))
    esm_id = new_uuid()
    func_arn = resource_arn if func else f"arn:aws:lambda:{get_region()}:{get_account_id()}:function:{func_name}"

    esm = {
        "UUID": esm_id,
        "EventSourceArn": props.get("EventSourceArn", ""),
        # resource_arn from _lambda_function_for_cfn_ref already carries the
        # qualifier — do not re-append it (that double-suffixed ":live:live").
        "FunctionArn": func_arn,
        "FunctionName": func_name,
        "Qualifier": qualifier,
        "State": "Enabled",
        "StateTransitionReason": "USER_INITIATED",
        "BatchSize": int(props.get("BatchSize", 10)),
        "MaximumBatchingWindowInSeconds": int(props.get("MaximumBatchingWindowInSeconds", 0)),
        "LastModified": int(time.time()),
        "LastProcessingResult": "No records processed",
        "StartingPosition": props.get("StartingPosition", "LATEST"),
        "Enabled": props.get("Enabled", True),
        "FunctionResponseTypes": props.get("FunctionResponseTypes", []),
    }
    # Preserve every optional ESM property the API create/update path round-trips,
    # so a CloudFormation-created mapping reads back without drift. Previously only
    # FilterCriteria was kept; DestinationConfig, ParallelizationFactor, the retry/age
    # knobs, and ScalingConfig were silently dropped -> perpetual Terraform/CDK diffs.
    # `is not None` (not truthiness) so explicit falsy values round-trip: 0 retries,
    # BisectBatchOnFunctionError=false; absent props stay absent (no spurious attrs).
    for _opt in (
        "FilterCriteria",
        "DestinationConfig",
        "ParallelizationFactor",
        "MaximumRetryAttempts",
        "MaximumRecordAgeInSeconds",
        "BisectBatchOnFunctionError",
        "ScalingConfig",
    ):
        if props.get(_opt) is not None:
            esm[_opt] = props[_opt]
    _lambda_svc._esms[esm_id] = esm
    # Anchor a DynamoDB-stream ESM's LATEST position at create time, matching the
    # API CreateEventSourceMapping path; no-op for SQS/Kinesis sources (#936).
    _lambda_svc._init_stream_position(esm_id, esm["EventSourceArn"], esm["StartingPosition"])
    _lambda_svc._ensure_poller()
    return esm_id, {"UUID": esm_id}


def _lambda_esm_delete(physical_id, props):
    _lambda_svc._esms.pop(physical_id, None)


def _lambda_esm_update(physical_id, old_props, new_props, stack_name):
    # Mutate the existing mapping in place, same as the API UpdateEventSourceMapping
    # path (_update_esm): keep the same UUID and copy the changed mutable props.
    # An immutable prop (EventSourceArn / StartingPosition[Timestamp]) forces a
    # CloudFormation replacement: create the new mapping, delete the old one.
    esm = _lambda_svc._esms.get(physical_id)
    if esm is None:
        return _lambda_esm_create(physical_id, new_props, stack_name)
    for immutable in ("EventSourceArn", "StartingPosition", "StartingPositionTimestamp"):
        if new_props.get(immutable) != old_props.get(immutable):
            new_id, attrs = _lambda_esm_create(physical_id, new_props, stack_name)
            _lambda_svc._esms.pop(physical_id, None)
            return new_id, attrs
    for key in (
        "BatchSize",
        "MaximumBatchingWindowInSeconds",
        "FunctionResponseTypes",
        "MaximumRetryAttempts",
        "MaximumRecordAgeInSeconds",
        "BisectBatchOnFunctionError",
        "ParallelizationFactor",
        "DestinationConfig",
        "FilterCriteria",
        "ScalingConfig",
    ):
        if key in new_props:
            esm[key] = new_props[key]
    if "Enabled" in new_props:
        esm["Enabled"] = new_props["Enabled"]
        esm["State"] = "Enabled" if new_props["Enabled"] else "Disabled"
    if "FunctionName" in new_props:
        func_name, qualifier = _lambda_svc._resolve_name_and_qualifier(new_props["FunctionName"])
        func = _lambda_svc._functions.get(func_name)
        func_arn = func["config"]["FunctionArn"] if func else f"arn:aws:lambda:{get_region()}:{get_account_id()}:function:{func_name}"
        esm["FunctionName"] = func_name
        esm["FunctionArn"] = func_arn + (f":{qualifier}" if qualifier else "")
    esm["LastModified"] = int(time.time())
    return physical_id, {"UUID": physical_id}


# --- Lambda EventInvokeConfig ---

def _lambda_event_invoke_config_create(logical_id, props, stack_name):
    func, func_name, _resource_arn, _embedded_qualifier = _lambda_function_for_cfn_ref(
        props.get("FunctionName", "")
    )
    qualifier = props.get("Qualifier", "")
    if func is None:
        raise ValueError(f"Lambda function not found: {props.get('FunctionName', '')}")
    if not qualifier or not _lambda_svc._function_qualifier_exists(func, qualifier):
        raise ValueError(f"Lambda function qualifier not found: {func_name}:{qualifier}")

    data = {
        key: props[key]
        for key in (
            "DestinationConfig",
            "MaximumEventAgeInSeconds",
            "MaximumRetryAttempts",
        )
        if key in props
    }
    status, _headers, body = _lambda_svc._put_event_invoke_config(
        func_name, qualifier, data
    )
    if status >= 400:
        raise ValueError(
            f"AWS::Lambda::EventInvokeConfig create failed: {body.decode('utf-8')}"
        )
    return f"{func_name}:{qualifier}", {}


def _lambda_event_invoke_config_update(physical_id, old_props, new_props, stack_name):
    replacement = any(
        old_props.get(key) != new_props.get(key)
        for key in ("FunctionName", "Qualifier")
    )
    new_id, attrs = _lambda_event_invoke_config_create(
        physical_id, new_props, stack_name
    )
    if replacement:
        _lambda_event_invoke_config_delete(physical_id, old_props)
        return new_id, attrs
    return physical_id, attrs


def _lambda_event_invoke_config_delete(physical_id, props):
    func, func_name, _resource_arn, _embedded_qualifier = _lambda_function_for_cfn_ref(
        props.get("FunctionName", "")
    )
    if func is None:
        return
    qualifier = props.get("Qualifier", "") or None
    status, _headers, _body = _lambda_svc._delete_event_invoke_config(
        func_name, qualifier
    )
    # Stack rollback/delete is idempotent when the config has already gone.
    if status not in (204, 404):
        raise ValueError(
            f"AWS::Lambda::EventInvokeConfig delete failed with status {status}"
        )


# --- EventBridge Pipes (minimal: DynamoDB Streams -> SNS) ---

def _pipes_pipe_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=64)
    source = props.get("Source", "")
    target = props.get("Target", "")
    role_arn = props.get("RoleArn", "")
    desired_state = props.get("DesiredState", "RUNNING")

    source_params = props.get("SourceParameters", {})
    ddb_params = source_params.get("DynamoDBStreamParameters", {}) if isinstance(source_params, dict) else {}
    starting_position = ddb_params.get("StartingPosition", "LATEST")

    pipe = _pipes.register_pipe(
        name=name,
        source=source,
        target=target,
        role_arn=role_arn,
        desired_state=desired_state,
        starting_position=starting_position,
    )
    return name, {"Arn": pipe["Arn"], "Name": name}


def _pipes_pipe_delete(physical_id, props):
    _pipes.delete_pipe(physical_id)


# --- Lambda Alias ---

def _lambda_alias_create(logical_id, props, stack_name):
    func, func_name, _resource_arn, _qualifier = _lambda_function_for_cfn_ref(props.get("FunctionName", ""))
    alias_name = props.get("Name", "")
    func_version = props.get("FunctionVersion", "$LATEST")

    if func:
        alias = {
            "AliasArn": f"arn:aws:lambda:{get_region()}:{get_account_id()}:function:{func_name}:{alias_name}",
            "Name": alias_name,
            "FunctionVersion": func_version,
            "Description": props.get("Description", ""),
            "RevisionId": new_uuid(),
        }
        rc = props.get("RoutingConfig")
        if rc:
            alias["RoutingConfig"] = rc
        func["aliases"][alias_name] = alias
        return alias["AliasArn"], {"AliasArn": alias["AliasArn"]}

    alias_arn = f"arn:aws:lambda:{get_region()}:{get_account_id()}:function:{func_name}:{alias_name}"
    return alias_arn, {"AliasArn": alias_arn}


def _lambda_alias_delete(physical_id, props):
    func, _func_name, _resource_arn, _qualifier = _lambda_function_for_cfn_ref(props.get("FunctionName", ""))
    alias_name = props.get("Name", "")
    if func:
        func["aliases"].pop(alias_name, None)


# --- SQS QueuePolicy ---

def _sqs_queue_policy_create(logical_id, props, stack_name):
    policy_doc = props.get("PolicyDocument", {})
    if isinstance(policy_doc, dict):
        policy_doc = json.dumps(policy_doc)
    queues = props.get("Queues", [])
    for queue_url in queues:
        queue = _sqs._queues.get(queue_url)
        if queue:
            queue["attributes"]["Policy"] = policy_doc
    pid = f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    return pid, {}


def _sqs_queue_policy_delete(physical_id, props):
    queues = props.get("Queues", [])
    for queue_url in queues:
        queue = _sqs._queues.get(queue_url)
        if queue:
            queue["attributes"].pop("Policy", None)


# --- SNS TopicPolicy ---

def _sns_topic_policy_create(logical_id, props, stack_name):
    policy_doc = props.get("PolicyDocument", {})
    if isinstance(policy_doc, dict):
        policy_doc = json.dumps(policy_doc)
    topics = props.get("Topics", [])
    for topic_arn in topics:
        topic = _sns._topics.get(topic_arn)
        if topic:
            topic["attributes"]["Policy"] = policy_doc
    pid = f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    return pid, {}


def _sns_topic_policy_delete(physical_id, props):
    topics = props.get("Topics", [])
    for topic_arn in topics:
        topic = _sns._topics.get(topic_arn)
        if topic:
            # Restore default policy
            topic["attributes"].pop("Policy", None)


# --- AppSync resource provisioners ---

def _appsync_api_create(logical_id, props, stack_name):
    import time as _time
    name = props.get("Name") or _physical_name(stack_name, logical_id)
    auth_type = props.get("AuthenticationType", "API_KEY")
    api_id = new_uuid()[:8]
    arn = f"arn:aws:appsync:{get_region()}:{get_account_id()}:apis/{api_id}"
    now = _time.time()
    _appsync._apis[api_id] = {
        "apiId": api_id, "name": name, "authenticationType": auth_type,
        "arn": arn,
        "uris": {"GRAPHQL": f"https://{api_id}.appsync-api.{get_region()}.amazonaws.com/graphql"},
        "createdAt": now, "lastUpdatedAt": now,
        "additionalAuthenticationProviders": props.get("AdditionalAuthenticationProviders", []),
        "xrayEnabled": False,
    }
    _appsync._api_keys[api_id] = {}
    _appsync._data_sources[api_id] = {}
    _appsync._resolvers[api_id] = {}
    _appsync._types[api_id] = {}
    return api_id, {"ApiId": api_id, "Arn": arn, "GraphQLUrl": f"https://{api_id}.appsync-api.{get_region()}.amazonaws.com/graphql"}


def _appsync_api_delete(physical_id, props):
    _appsync._apis.pop(physical_id, None)
    _appsync._api_keys.pop(physical_id, None)
    _appsync._data_sources.pop(physical_id, None)
    _appsync._resolvers.pop(physical_id, None)
    _appsync._types.pop(physical_id, None)


def _appsync_ds_create(logical_id, props, stack_name):
    api_id = props.get("ApiId", "")
    name = props.get("Name") or logical_id
    ds_type = props.get("Type", "NONE")
    body = {"name": name, "type": ds_type}
    if props.get("DynamoDBConfig"):
        body["dynamodbConfig"] = props["DynamoDBConfig"]
    if props.get("LambdaConfig"):
        body["lambdaConfig"] = props["LambdaConfig"]
    if props.get("ServiceRoleArn"):
        body["serviceRoleArn"] = props["ServiceRoleArn"]
    _appsync._data_sources.setdefault(api_id, {})[name] = {
        "name": name, "type": ds_type, **body,
        "dataSourceArn": f"arn:aws:appsync:{get_region()}:{get_account_id()}:apis/{api_id}/datasources/{name}",
    }
    return f"{api_id}/{name}", {"Name": name, "DataSourceArn": f"arn:aws:appsync:{get_region()}:{get_account_id()}:apis/{api_id}/datasources/{name}"}


def _appsync_ds_delete(physical_id, props):
    parts = physical_id.split("/", 1)
    if len(parts) == 2:
        _appsync._data_sources.get(parts[0], {}).pop(parts[1], None)


def _appsync_resolver_create(logical_id, props, stack_name):
    api_id = props.get("ApiId", "")
    type_name = props.get("TypeName", "Query")
    field_name = props.get("FieldName", logical_id)
    ds_name = props.get("DataSourceName", "")
    resolver = {
        "typeName": type_name, "fieldName": field_name,
        "dataSourceName": ds_name,
        "resolverArn": f"arn:aws:appsync:{get_region()}:{get_account_id()}:apis/{api_id}/types/{type_name}/resolvers/{field_name}",
    }
    if props.get("RequestMappingTemplate"):
        resolver["requestMappingTemplate"] = props["RequestMappingTemplate"]
    if props.get("ResponseMappingTemplate"):
        resolver["responseMappingTemplate"] = props["ResponseMappingTemplate"]
    _appsync._resolvers.setdefault(api_id, {}).setdefault(type_name, {})[field_name] = resolver
    return f"{api_id}/{type_name}/{field_name}", {"ResolverArn": resolver["resolverArn"]}


def _appsync_resolver_delete(physical_id, props):
    parts = physical_id.split("/", 2)
    if len(parts) == 3:
        _appsync._resolvers.get(parts[0], {}).get(parts[1], {}).pop(parts[2], None)


def _appsync_schema_create(logical_id, props, stack_name):
    api_id = props.get("ApiId", "")
    definition = props.get("Definition", "")
    _appsync._types.setdefault(api_id, {})["__schema__"] = {
        "typeName": "__schema__", "definition": definition, "format": "SDL",
    }
    return f"{api_id}/schema", {}


def _appsync_apikey_create(logical_id, props, stack_name):
    api_id = props.get("ApiId", "")
    key_id = new_uuid()[:8]
    import time
    key = {
        "id": key_id, "apiKeyId": key_id,
        "expires": props.get("Expires", int(time.time()) + 604800),
    }
    _appsync._api_keys.setdefault(api_id, {})[key_id] = key
    return key_id, {"ApiKey": key_id, "Arn": f"arn:aws:appsync:{get_region()}:{get_account_id()}:apis/{api_id}/apikeys/{key_id}"}


def _appsync_apikey_delete(physical_id, props):
    api_id = props.get("ApiId", "")
    _appsync._api_keys.get(api_id, {}).pop(physical_id, None)


# --- SecretsManager resource provisioners ---

def _sm_secret_create(logical_id, props, stack_name):
    import string as _string
    name = props.get("Name") or _physical_name(stack_name, logical_id)
    secret_string = props.get("SecretString", "")
    gen = props.get("GenerateSecretString")
    if gen and not secret_string:
        length = gen.get("PasswordLength", 32)
        exclude = gen.get("ExcludeCharacters", "")
        chars = _string.ascii_letters + _string.digits + _string.punctuation
        chars = "".join(c for c in chars if c not in exclude)
        import random
        generated = "".join(random.choices(chars, k=length))
        template = gen.get("SecretStringTemplate")
        gen_key = gen.get("GenerateStringKey", "password")
        if template:
            import json
            try:
                obj = json.loads(template)
                obj[gen_key] = generated
                secret_string = json.dumps(obj)
            except Exception:
                secret_string = generated
        else:
            secret_string = generated

    arn = f"arn:aws:secretsmanager:{get_region()}:{get_account_id()}:secret:{name}-{new_uuid()[:6]}"
    import time as _time
    _sm._secrets[name] = {
        "ARN": arn, "Name": name, "Description": props.get("Description", ""),
        "Tags": props.get("Tags", []),
        "CreatedDate": int(_time.time()), "LastChangedDate": int(_time.time()),
        "LastAccessedDate": None, "DeletedDate": None,
        "RotationEnabled": False, "RotationLambdaARN": None,
        "RotationRules": None, "ReplicationStatus": [],
        "KmsKeyId": props.get("KmsKeyId"),
        "Versions": {
            new_uuid(): {
                "SecretString": secret_string,
                "SecretBinary": None,
                "CreatedDate": int(_time.time()),
                "Stages": ["AWSCURRENT"],
            }
        },
    }
    return name, {"Arn": arn}


def _sm_secret_delete(physical_id, props):
    _sm._secrets.pop(physical_id, None)


# --- Cognito UserPool ---

def _cognito_user_pool_create(logical_id, props, stack_name):
    name = props.get("PoolName") or _physical_name(stack_name, logical_id, max_len=128)
    pid = _cognito._pool_id()
    now = _cognito._now_epoch()
    pool = {
        "Id": pid,
        "Name": name,
        "Arn": _cognito._pool_arn(pid),
        "CreationDate": now,
        "LastModifiedDate": now,
        "Policies": props.get("Policies", {
            "PasswordPolicy": {
                "MinimumLength": 8,
                "RequireUppercase": True,
                "RequireLowercase": True,
                "RequireNumbers": True,
                "RequireSymbols": True,
                "TemporaryPasswordValidityDays": 7,
            }
        }),
        "Schema": props.get("Schema", []),
        "AutoVerifiedAttributes": props.get("AutoVerifiedAttributes", []),
        "AliasAttributes": props.get("AliasAttributes", []),
        "UsernameAttributes": props.get("UsernameAttributes", []),
        "MfaConfiguration": props.get("MfaConfiguration", "OFF"),
        "EstimatedNumberOfUsers": 0,
        "UserPoolTags": props.get("UserPoolTags", {}),
        "AdminCreateUserConfig": props.get("AdminCreateUserConfig", {
            "AllowAdminCreateUserOnly": False,
            "UnusedAccountValidityDays": 7,
        }),
        "Domain": None,
        "_clients": {},
        "_users": {},
        "_groups": {},
        "_resource_servers": {},
    }
    _cognito._user_pools[pid] = pool
    arn = _cognito._pool_arn(pid)
    provider_name = f"cognito-idp.{get_region()}.amazonaws.com/{pid}"
    return pid, {"Arn": arn, "ProviderName": provider_name}


def _cognito_user_pool_delete(physical_id, props):
    pool = _cognito._user_pools.pop(physical_id, None)
    if pool and pool.get("Domain"):
        _cognito._pool_domain_map.pop(pool["Domain"], None)


# --- Cognito UserPoolClient ---

def _cognito_user_pool_client_create(logical_id, props, stack_name):
    pid = props.get("UserPoolId", "")
    pool = _cognito._user_pools.get(pid)
    if not pool:
        raise ValueError(f"UserPool {pid} not found for UserPoolClient")

    cid = _cognito._client_id()
    now = _cognito._now_epoch()
    client = {
        "UserPoolId": pid,
        "ClientName": props.get("ClientName", ""),
        "ClientId": cid,
        "ClientSecret": props.get("GenerateSecret", False) and _cognito._client_secret() or None,
        "CreationDate": now,
        "LastModifiedDate": now,
        "ExplicitAuthFlows": props.get("ExplicitAuthFlows", []),
        "AllowedOAuthFlows": props.get("AllowedOAuthFlows", []),
        "AllowedOAuthScopes": props.get("AllowedOAuthScopes", []),
        "CallbackURLs": props.get("CallbackURLs", []),
        "LogoutURLs": props.get("LogoutURLs", []),
        "SupportedIdentityProviders": props.get("SupportedIdentityProviders", []),
    }
    pool["_clients"][cid] = client
    return cid, {}


def _cognito_user_pool_client_delete(physical_id, props):
    pid = props.get("UserPoolId", "")
    pool = _cognito._user_pools.get(pid)
    if pool:
        pool["_clients"].pop(physical_id, None)


# --- Cognito UserPoolResourceServer ---

def _cognito_user_pool_resource_server_create(logical_id, props, stack_name):
    pid = props.get("UserPoolId", "")
    pool = _cognito._user_pools.get(pid)
    if not pool:
        raise ValueError(f"UserPool {pid} not found for UserPoolResourceServer")

    identifier = props.get("Identifier", "")
    server = _cognito._resource_server_dict(
        pid, identifier, props.get("Name", identifier), props.get("Scopes", []),
    )
    # Ref on this resource type returns the Identifier (matches real AWS —
    # see https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-cognito-userpoolresourceserver.html#aws-resource-cognito-userpoolresourceserver-return-values).
    _cognito._pool_resource_servers(pool)[identifier] = server
    return identifier, {}


def _cognito_user_pool_resource_server_delete(physical_id, props):
    pid = props.get("UserPoolId", "")
    pool = _cognito._user_pools.get(pid)
    if pool:
        _cognito._pool_resource_servers(pool).pop(physical_id, None)


# --- Cognito UserPoolGroup ---

def _cognito_user_pool_group_create(logical_id, props, stack_name):
    pid = props.get("UserPoolId", "")
    pool = _cognito._user_pools.get(pid)
    if not pool:
        raise ValueError(f"UserPool {pid} not found for UserPoolGroup")

    name = props.get("GroupName") or _physical_name(stack_name, logical_id, max_len=128)
    now = _cognito._now_epoch()
    group = {
        "GroupName": name,
        "UserPoolId": pid,
        "Description": props.get("Description", ""),
        "RoleArn": props.get("RoleArn", ""),
        "Precedence": props.get("Precedence", 0),
        "CreationDate": now,
        "LastModifiedDate": now,
        "_members": [],
    }
    pool["_groups"][name] = group
    # Ref on this resource type returns the GroupName (matches real AWS —
    # see https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-cognito-userpoolgroup.html#aws-resource-cognito-userpoolgroup-return-values).
    return name, {}


def _cognito_user_pool_group_delete(physical_id, props):
    pid = props.get("UserPoolId", "")
    pool = _cognito._user_pools.get(pid)
    if not pool:
        return
    group = pool["_groups"].pop(physical_id, None)
    if not group:
        return
    for username in group.get("_members", []):
        user = pool["_users"].get(username)
        if user and physical_id in user.get("_groups", []):
            user["_groups"].remove(physical_id)


# --- Cognito IdentityPool ---

def _cognito_identity_pool_create(logical_id, props, stack_name):
    name = props.get("IdentityPoolName") or _physical_name(stack_name, logical_id, max_len=128)
    iid = _cognito._identity_pool_id()
    pool = {
        "IdentityPoolId": iid,
        "IdentityPoolName": name,
        "AllowUnauthenticatedIdentities": props.get("AllowUnauthenticatedIdentities", False),
        "AllowClassicFlow": props.get("AllowClassicFlow", False),
        "SupportedLoginProviders": props.get("SupportedLoginProviders", {}),
        "DeveloperProviderName": props.get("DeveloperProviderName", ""),
        "OpenIdConnectProviderARNs": props.get("OpenIdConnectProviderARNs", []),
        "CognitoIdentityProviders": props.get("CognitoIdentityProviders", []),
        "SamlProviderARNs": props.get("SamlProviderARNs", []),
        "IdentityPoolTags": props.get("IdentityPoolTags", {}),
        "_roles": {},
        "_identities": {},
    }
    _cognito._identity_pools[iid] = pool
    return iid, {}


def _cognito_identity_pool_delete(physical_id, props):
    _cognito._identity_pools.pop(physical_id, None)
    _cognito._identity_tags.pop(physical_id, None)


# --- Cognito UserPoolDomain ---

def _cognito_user_pool_domain_create(logical_id, props, stack_name):
    pid = props.get("UserPoolId", "")
    domain = props.get("Domain", "")
    pool = _cognito._user_pools.get(pid)
    if not pool:
        raise ValueError(f"UserPool {pid} not found for UserPoolDomain")
    pool["Domain"] = domain
    _cognito._pool_domain_map[domain] = pid
    phys_id = f"{pid}-domain-{domain}"
    return phys_id, {}


def _cognito_user_pool_domain_delete(physical_id, props):
    domain = props.get("Domain", "")
    pid = _cognito._pool_domain_map.pop(domain, None)
    if pid:
        pool = _cognito._user_pools.get(pid)
        if pool:
            pool["Domain"] = None


# ===========================================================================
# --- ECR resource provisioners ---

def _ecr_repo_create(logical_id, props, stack_name):
    name = props.get("RepositoryName", f"{stack_name}-{logical_id}".lower())
    arn = f"arn:aws:ecr:{get_region()}:{get_account_id()}:repository/{name}"
    _ecr._repositories[name] = {
        "repositoryName": name,
        "repositoryArn": arn,
        "registryId": get_account_id(),
        "repositoryUri": f"{get_account_id()}.dkr.ecr.{get_region()}.amazonaws.com/{name}",
        "createdAt": __import__("time").time(),
        "imageTagMutability": props.get("ImageTagMutability", "MUTABLE"),
        "imageScanningConfiguration": props.get("ImageScanningConfiguration", {"scanOnPush": False}),
        "encryptionConfiguration": props.get("EncryptionConfiguration", {"encryptionType": "AES256"}),
        "images": [],
    }
    return name, {"Arn": arn, "RepositoryUri": _ecr._repositories[name]["repositoryUri"]}


def _ecr_repo_delete(physical_id, props):
    _ecr._repositories.pop(physical_id, None)


# --- CodeBuild Project provisioner ---

def _codebuild_project_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=255)
    
    # Pre-check for duplicates to raise exception (not just return error response)
    if name in _codebuild._projects:
        raise ValueError(f"CodeBuild project already exists: {name}")
    
    data = {
        "name": name,
        "description": props.get("Description", ""),
        "source": props.get("Source", {"type": "NO_SOURCE"}),
        "sourceVersion": props.get("SourceVersion", ""),
        "artifacts": props.get("Artifacts", {"type": "NO_ARTIFACTS"}),
        "environment": props.get("Environment", {
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        }),
        "serviceRole": props.get("ServiceRole", f"arn:aws:iam::{get_account_id()}:role/codebuild-role"),
        "timeoutInMinutes": int(props.get("TimeoutInMinutes", 60)),
        "tags": [{"key": t["Key"], "value": t["Value"]} for t in props.get("Tags", [])],
        "encryptionKey": props.get("EncryptionKey", f"arn:aws:kms:{get_region()}:{get_account_id()}:alias/aws/codebuild"),
    }
    _codebuild._create_project(data)
    arn = _codebuild._project_arn(name)
    return name, {"Arn": arn}


def _codebuild_project_delete(physical_id, props):
    _codebuild._projects.pop(physical_id, None)


# --- IAM ManagedPolicy provisioner ---

def _iam_managed_policy_create(logical_id, props, stack_name):
    name = props.get("ManagedPolicyName", f"{stack_name}-{logical_id}")
    arn = f"arn:aws:iam::{get_account_id()}:policy/{name}"
    policy_doc = props.get("PolicyDocument", {})
    _iam._policies[arn] = {
        "PolicyName": name,
        "PolicyId": new_uuid().replace("-", "")[:21].upper(),
        "Arn": arn,
        "Path": props.get("Path", "/"),
        "DefaultVersionId": "v1",
        "AttachmentCount": 0,
        "IsAttachable": True,
        "Description": props.get("Description", ""),
        "CreateDate": __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime()),
        "UpdateDate": __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime()),
        "PolicyVersions": [{"Document": json.dumps(policy_doc) if isinstance(policy_doc, dict) else policy_doc, "VersionId": "v1", "IsDefaultVersion": True}],
    }
    return arn, {"Arn": arn}


def _iam_managed_policy_delete(physical_id, props):
    _iam._policies.pop(physical_id, None)


# --- KMS resource provisioners ---

def _kms_key_create(logical_id, props, stack_name):
    key_id = new_uuid()
    arn = f"arn:aws:kms:{get_region()}:{get_account_id()}:key/{key_id}"
    _kms._keys[key_id] = {
        "KeyId": key_id,
        "Arn": arn,
        "KeyState": "Enabled",
        "Enabled": True,
        "KeySpec": "SYMMETRIC_DEFAULT",
        "KeyUsage": props.get("KeyUsage", "ENCRYPT_DECRYPT"),
        "Description": props.get("Description", ""),
        "CreationDate": __import__("time").time(),
        "Origin": "AWS_KMS",
        "_symmetric_key": __import__("os").urandom(32),
        "EncryptionAlgorithms": ["SYMMETRIC_DEFAULT"],
        "SigningAlgorithms": [],
    }
    return key_id, {"Arn": arn, "KeyId": key_id}


def _kms_key_delete(physical_id, props):
    _kms._keys.pop(physical_id, None)


def _kms_alias_create(logical_id, props, stack_name):
    alias_name = props.get("AliasName", f"alias/{stack_name}-{logical_id}")
    target_key = props.get("TargetKeyId", "")
    rec = _kms._resolve_key(target_key)
    alias_arn = _kms._alias_arn(alias_name)
    _kms._aliases[alias_arn] = rec["KeyId"] if rec else target_key
    return alias_name, {}


def _kms_alias_delete(physical_id, props):
    _kms._aliases.pop(_kms._alias_arn(physical_id), None)


# --- EC2 resource provisioners ---

def _ec2_vpc_create(logical_id, props, stack_name):
    import random
    import string
    cidr = props.get("CidrBlock", "10.0.0.0/16")
    vpc_id = _ec2._new_vpc_id()
    # Create per-VPC default resources (same as _create_vpc)
    acl_id = "acl-" + "".join(random.choices(string.hexdigits[:16], k=17))
    _ec2._network_acls[acl_id] = {
        "NetworkAclId": acl_id, "VpcId": vpc_id, "IsDefault": True,
        "Entries": [
            {"RuleNumber": 100, "Protocol": "-1", "RuleAction": "allow", "Egress": False, "CidrBlock": "0.0.0.0/0"},
            {"RuleNumber": 32767, "Protocol": "-1", "RuleAction": "deny", "Egress": False, "CidrBlock": "0.0.0.0/0"},
            {"RuleNumber": 100, "Protocol": "-1", "RuleAction": "allow", "Egress": True, "CidrBlock": "0.0.0.0/0"},
            {"RuleNumber": 32767, "Protocol": "-1", "RuleAction": "deny", "Egress": True, "CidrBlock": "0.0.0.0/0"},
        ],
        "Associations": [], "Tags": [], "OwnerId": get_account_id(),
    }
    rtb_id = "rtb-" + "".join(random.choices(string.hexdigits[:16], k=17))
    rtb_assoc_id = "rtbassoc-" + "".join(random.choices(string.hexdigits[:16], k=17))
    _ec2._route_tables[rtb_id] = {
        "RouteTableId": rtb_id, "VpcId": vpc_id, "OwnerId": get_account_id(),
        "Routes": [{"DestinationCidrBlock": cidr, "GatewayId": "local", "State": "active", "Origin": "CreateRouteTable"}],
        "Associations": [{"RouteTableAssociationId": rtb_assoc_id, "RouteTableId": rtb_id, "Main": True,
                          "AssociationState": {"State": "associated"}}],
    }
    sg_id = _ec2._new_sg_id()
    _ec2._security_groups[sg_id] = {
        "GroupId": sg_id, "GroupName": "default", "Description": "default VPC security group",
        "VpcId": vpc_id, "OwnerId": get_account_id(), "IpPermissions": [],
        "IpPermissionsEgress": [{"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
             "Ipv6Ranges": [], "PrefixListIds": [], "UserIdGroupPairs": []}],
    }
    _ec2._vpcs[vpc_id] = {
        "VpcId": vpc_id, "CidrBlock": cidr, "State": "available", "IsDefault": False,
        "DhcpOptionsId": "dopt-00000001", "InstanceTenancy": props.get("InstanceTenancy", "default"),
        "OwnerId": get_account_id(), "DefaultNetworkAclId": acl_id,
        "DefaultSecurityGroupId": sg_id, "MainRouteTableId": rtb_id,
    }
    arn = f"arn:aws:ec2:{get_region()}:{get_account_id()}:vpc/{vpc_id}"
    return vpc_id, {"VpcId": vpc_id, "DefaultSecurityGroup": sg_id, "DefaultNetworkAcl": acl_id}


def _ec2_vpc_delete(physical_id, props):
    _ec2._vpcs.pop(physical_id, None)


def _ec2_subnet_create(logical_id, props, stack_name):
    import random
    import string
    vpc_id = props.get("VpcId", "")
    cidr = props.get("CidrBlock", "10.0.1.0/24")
    az = props.get("AvailabilityZone", f"{get_region()}a")
    subnet_id = _ec2._new_subnet_id()
    _ec2._subnets[subnet_id] = {
        "SubnetId": subnet_id,
        "VpcId": vpc_id,
        "CidrBlock": cidr,
        "AvailabilityZone": az,
        "State": "available",
        "AvailableIpAddressCount": 251,
        "DefaultForAz": False,
        "MapPublicIpOnLaunch": props.get("MapPublicIpOnLaunch", False),
        "OwnerId": get_account_id(),
    }
    return subnet_id, {"SubnetId": subnet_id, "AvailabilityZone": az}


def _ec2_subnet_delete(physical_id, props):
    _ec2._subnets.pop(physical_id, None)


def _ec2_sg_create(logical_id, props, stack_name):
    name = props.get("GroupName", f"{stack_name}-{logical_id}")
    desc = props.get("GroupDescription", name)
    vpc_id = props.get("VpcId", _ec2._DEFAULT_VPC_ID)
    sg_id = _ec2._new_sg_id()
    _ec2._security_groups[sg_id] = {
        "GroupId": sg_id,
        "GroupName": name,
        "Description": desc,
        "VpcId": vpc_id,
        "OwnerId": get_account_id(),
        "IpPermissions": [],
        "IpPermissionsEgress": [
            {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
             "Ipv6Ranges": [], "PrefixListIds": [], "UserIdGroupPairs": []},
        ],
    }
    # Apply ingress rules from props
    for rule in props.get("SecurityGroupIngress", []):
        perm = {
            "IpProtocol": rule.get("IpProtocol", "tcp"),
            "IpRanges": [],
            "Ipv6Ranges": [],
            "PrefixListIds": [],
            "UserIdGroupPairs": [],
        }
        if "FromPort" in rule:
            perm["FromPort"] = int(rule["FromPort"])
        if "ToPort" in rule:
            perm["ToPort"] = int(rule["ToPort"])
        if "CidrIp" in rule:
            perm["IpRanges"].append({"CidrIp": rule["CidrIp"]})
        _ec2._security_groups[sg_id]["IpPermissions"].append(perm)

    arn = f"arn:aws:ec2:{get_region()}:{get_account_id()}:security-group/{sg_id}"
    return sg_id, {"GroupId": sg_id, "VpcId": vpc_id, "Arn": arn}


def _ec2_sg_delete(physical_id, props):
    _ec2._security_groups.pop(physical_id, None)


def _ec2_igw_create(logical_id, props, stack_name):
    import random
    import string
    igw_id = "igw-" + "".join(random.choices(string.hexdigits[:16], k=17))
    _ec2._internet_gateways[igw_id] = {
        "InternetGatewayId": igw_id,
        "OwnerId": get_account_id(),
        "Attachments": [],
    }
    return igw_id, {"InternetGatewayId": igw_id}


def _ec2_igw_delete(physical_id, props):
    _ec2._internet_gateways.pop(physical_id, None)


def _ec2_vpc_gw_attach_create(logical_id, props, stack_name):
    vpc_id = props.get("VpcId", "")
    igw_id = props.get("InternetGatewayId", "")
    igw = _ec2._internet_gateways.get(igw_id)
    if igw:
        igw["Attachments"] = [{"VpcId": vpc_id, "State": "available"}]
    physical_id = f"{igw_id}|{vpc_id}"
    return physical_id, {}


def _ec2_vpc_gw_attach_delete(physical_id, props):
    parts = physical_id.split("|")
    if len(parts) == 2:
        igw = _ec2._internet_gateways.get(parts[0])
        if igw:
            igw["Attachments"] = []


def _ec2_rtb_create(logical_id, props, stack_name):
    import random
    import string
    vpc_id = props.get("VpcId", _ec2._DEFAULT_VPC_ID)
    rtb_id = "rtb-" + "".join(random.choices(string.hexdigits[:16], k=17))
    _ec2._route_tables[rtb_id] = {
        "RouteTableId": rtb_id,
        "VpcId": vpc_id,
        "OwnerId": get_account_id(),
        "Routes": [
            {"DestinationCidrBlock": _ec2._vpcs.get(vpc_id, {}).get("CidrBlock", "10.0.0.0/16"),
             "GatewayId": "local", "State": "active", "Origin": "CreateRouteTable"},
        ],
        "Associations": [],
    }
    return rtb_id, {"RouteTableId": rtb_id}


def _ec2_rtb_delete(physical_id, props):
    _ec2._route_tables.pop(physical_id, None)


def _ec2_route_create(logical_id, props, stack_name):
    rtb_id = props.get("RouteTableId", "")
    dest = props.get("DestinationCidrBlock", "0.0.0.0/0")
    rtb = _ec2._route_tables.get(rtb_id)
    if rtb:
        route = {"DestinationCidrBlock": dest, "State": "active", "Origin": "CreateRoute"}
        if props.get("GatewayId"):
            route["GatewayId"] = props["GatewayId"]
        elif props.get("NatGatewayId"):
            route["NatGatewayId"] = props["NatGatewayId"]
        rtb["Routes"].append(route)
    physical_id = f"{rtb_id}|{dest}"
    return physical_id, {}


def _ec2_route_delete(physical_id, props):
    parts = physical_id.split("|")
    if len(parts) == 2:
        rtb = _ec2._route_tables.get(parts[0])
        if rtb:
            rtb["Routes"] = [r for r in rtb["Routes"] if r.get("DestinationCidrBlock") != parts[1]]


def _ec2_subnet_rtb_assoc_create(logical_id, props, stack_name):
    import random
    import string
    rtb_id = props.get("RouteTableId", "")
    subnet_id = props.get("SubnetId", "")
    assoc_id = "rtbassoc-" + "".join(random.choices(string.hexdigits[:16], k=17))
    rtb = _ec2._route_tables.get(rtb_id)
    if rtb:
        rtb["Associations"].append({
            "RouteTableAssociationId": assoc_id,
            "RouteTableId": rtb_id,
            "SubnetId": subnet_id,
            "Main": False,
            "AssociationState": {"State": "associated"},
        })
    return assoc_id, {}


def _ec2_subnet_rtb_assoc_delete(physical_id, props):
    for rtb in _ec2._route_tables.values():
        rtb["Associations"] = [a for a in rtb["Associations"] if a["RouteTableAssociationId"] != physical_id]


# --- ECS resource provisioners ---

def _ecs_cluster_create(logical_id, props, stack_name):
    name = props.get("ClusterName", f"{stack_name}-{logical_id}")
    arn = f"arn:aws:ecs:{get_region()}:{get_account_id()}:cluster/{name}"
    _ecs._clusters[name] = {
        "clusterArn": arn,
        "clusterName": name,
        "status": "ACTIVE",
        "registeredContainerInstancesCount": 0,
        "runningTasksCount": 0,
        "pendingTasksCount": 0,
        "activeServicesCount": 0,
        "settings": props.get("ClusterSettings", []),
        "capacityProviders": props.get("CapacityProviders", []),
        "defaultCapacityProviderStrategy": props.get("DefaultCapacityProviderStrategy", []),
        "tags": [{"key": t["Key"], "value": t["Value"]} for t in props.get("Tags", [])],
    }
    return name, {"Arn": arn, "ClusterName": name}


def _ecs_cluster_delete(physical_id, props):
    _ecs._clusters.pop(physical_id, None)


def _cfn_to_camel(key):
    """Convert a PascalCase CloudFormation key to camelCase."""
    if not key:
        return key
    return key[0].lower() + key[1:]


def _normalize_container_defs(cdefs):
    """Convert CF PascalCase container definitions to camelCase for ECS API."""
    result = []
    for cdef in cdefs:
        normalized = {}
        for k, v in cdef.items():
            camel = _cfn_to_camel(k)
            if camel == "portMappings" and isinstance(v, list):
                v = [{_cfn_to_camel(pk): pv for pk, pv in pm.items()} for pm in v]
            elif camel == "environment" and isinstance(v, list):
                v = [{_cfn_to_camel(ek): ev for ek, ev in e.items()} for e in v]
            elif camel == "mountPoints" and isinstance(v, list):
                v = [{_cfn_to_camel(mk): mv for mk, mv in m.items()} for m in v]
            elif camel == "volumesFrom" and isinstance(v, list):
                v = [{_cfn_to_camel(vk): vv for vk, vv in vf.items()} for vf in v]
            elif camel == "logConfiguration" and isinstance(v, dict):
                v = {_cfn_to_camel(lk): lv for lk, lv in v.items()}
            normalized[camel] = v
        result.append(normalized)
    return result


def _ecs_task_def_create(logical_id, props, stack_name):
    family = props.get("Family", f"{stack_name}-{logical_id}")
    revision = 1
    td_key = f"{family}:{revision}"
    arn = f"arn:aws:ecs:{get_region()}:{get_account_id()}:task-definition/{td_key}"
    compat = props.get("RequiresCompatibilities", ["EC2"])
    td = {
        "taskDefinitionArn": arn,
        "family": family,
        "revision": revision,
        "status": "ACTIVE",
        "containerDefinitions": _normalize_container_defs(props.get("ContainerDefinitions", [])),
        "requiresCompatibilities": compat,
        "compatibilities": compat + (["EC2"] if "FARGATE" in compat and "EC2" not in compat else []),
        "networkMode": props.get("NetworkMode", "bridge"),
        "cpu": props.get("Cpu", "256"),
        "memory": props.get("Memory", "512"),
        "executionRoleArn": props.get("ExecutionRoleArn", ""),
        "taskRoleArn": props.get("TaskRoleArn", ""),
        "volumes": props.get("Volumes", []),
        "pidMode": props.get("PidMode", ""),
        "ipcMode": props.get("IpcMode", ""),
        "placementConstraints": props.get("PlacementConstraints", []),
        "registeredAt": now_iso(),
        "registeredBy": f"arn:aws:iam::{get_account_id()}:root",
    }
    _ecs._task_defs[td_key] = td
    _ecs._task_def_latest[family] = revision
    return arn, {"TaskDefinitionArn": arn}


def _ecs_task_def_delete(physical_id, props):
    # physical_id is the ARN; _task_defs is keyed by "family:revision"
    td_key = physical_id.split("/")[-1] if "/" in physical_id else physical_id
    _ecs._task_defs.pop(td_key, None)


def _ecs_service_create(logical_id, props, stack_name):
    name = props.get("ServiceName", f"{stack_name}-{logical_id}")
    cluster = props.get("Cluster", "default")
    _ecs._create_service({
        "serviceName": name,
        "cluster": cluster,
        "taskDefinition": props.get("TaskDefinition", ""),
        "desiredCount": props.get("DesiredCount", 1),
        "launchType": props.get("LaunchType", "EC2"),
        "loadBalancers": props.get("LoadBalancers", []),
        "networkConfiguration": props.get("NetworkConfiguration", {}),
        "tags": [{"key": t["Key"], "value": t["Value"]} for t in props.get("Tags", [])],
    })
    arn = f"arn:aws:ecs:{get_region()}:{get_account_id()}:service/{cluster}/{name}"
    return arn, {"ServiceArn": arn, "Name": name}


def _ecs_service_delete(physical_id, props):
    cluster = props.get("Cluster", "default")
    name = props.get("ServiceName", "")
    if not name and "/" in physical_id:
        name = physical_id.split("/")[-1]
    _ecs._delete_service({"cluster": cluster, "service": name, "force": True})


# --- EC2 Launch Template provisioners ---

def _ec2_launch_template_create(logical_id, props, stack_name):
    name = props.get("LaunchTemplateName", _physical_name(stack_name, logical_id))
    lt_data = props.get("LaunchTemplateData", {})
    lt_id = _ec2._new_lt_id()
    now = __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime())
    version = {
        "LaunchTemplateId": lt_id,
        "LaunchTemplateName": name,
        "VersionNumber": 1,
        "VersionDescription": props.get("VersionDescription", ""),
        "DefaultVersion": True,
        "CreateTime": now,
        "LaunchTemplateData": lt_data,
    }
    lt = {
        "LaunchTemplateId": lt_id,
        "LaunchTemplateName": name,
        "CreateTime": now,
        "DefaultVersionNumber": 1,
        "LatestVersionNumber": 1,
        "Versions": [version],
        "Tags": [{"Key": t["Key"], "Value": t["Value"]} for t in props.get("Tags", [])],
    }
    _ec2._launch_templates[lt_id] = lt
    return lt_id, {
        "LaunchTemplateId": lt_id,
        "LaunchTemplateName": name,
        "DefaultVersionNumber": "1",
        "LatestVersionNumber": "1",
    }


def _ec2_launch_template_delete(physical_id, props):
    _ec2._launch_templates.pop(physical_id, None)


# --- ELBv2 (Load Balancer + Listener) provisioners ---

def _elbv2_as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        # CloudFormation parameters like CommaDelimitedList are often resolved as CSV strings.
        return [v.strip() for v in value.split(",") if v.strip()]
    return [value]


def _elbv2_tags(tags):
    out = []
    for tag in (tags or []):
        if isinstance(tag, dict) and "Key" in tag:
            out.append({"Key": str(tag["Key"]), "Value": str(tag.get("Value", ""))})
    return out


def _elbv2_load_balancer_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(
        stack_name,
        logical_id,
        lowercase=True,
        max_len=32,
    )
    lb_id = _alb._short_id()
    arn = (
        f"arn:aws:elasticloadbalancing:{get_region()}:{get_account_id()}:"
        f"loadbalancer/app/{name}/{lb_id}"
    )
    dns_name = f"{name}-{lb_id[:8]}.{get_region()}.elb.amazonaws.com"
    lb = {
        "LoadBalancerArn": arn,
        "LoadBalancerName": name,
        "DNSName": dns_name,
        "Scheme": props.get("Scheme", "internet-facing"),
        "VpcId": props.get("VpcId", "vpc-00000001"),
        "State": "active",
        "Type": props.get("Type", "application"),
        "Subnets": _elbv2_as_list(props.get("Subnets")),
        "SecurityGroups": _elbv2_as_list(props.get("SecurityGroups")),
        "IpAddressType": props.get("IpAddressType", "ipv4"),
        "CreatedTime": _alb._now_iso(),
    }
    _alb._lbs[arn] = lb
    _alb._tags[arn] = _elbv2_tags(props.get("Tags"))
    _alb._lb_attrs[arn] = [
        {"Key": a.get("Key", ""), "Value": str(a.get("Value", ""))}
        for a in (props.get("LoadBalancerAttributes") or [])
        if isinstance(a, dict) and a.get("Key")
    ] or [
        {"Key": "access_logs.s3.enabled", "Value": "false"},
        {"Key": "deletion_protection.enabled", "Value": "false"},
        {"Key": "idle_timeout.timeout_seconds", "Value": "60"},
    ]

    attrs = {
        "Arn": arn,
        "LoadBalancerArn": arn,
        "LoadBalancerName": name,
        "DNSName": dns_name,
        "LoadBalancerFullName": f"app/{name}/{lb_id}",
        "CanonicalHostedZoneID": "Z35SXDOTRQ7X7K",
        "SecurityGroups": lb["SecurityGroups"],
    }
    return arn, attrs


def _elbv2_load_balancer_delete(physical_id, props):
    # Clean up listeners/rules linked to this load balancer.
    listener_arns = [
        l_arn
        for l_arn, listener in list(_alb._listeners.items())
        if listener.get("LoadBalancerArn") == physical_id
    ]
    for l_arn in listener_arns:
        _alb._listeners.pop(l_arn, None)
        _alb._tags.pop(l_arn, None)
        for r_arn in [k for k, v in list(_alb._rules.items()) if v.get("ListenerArn") == l_arn]:
            _alb._rules.pop(r_arn, None)
            _alb._tags.pop(r_arn, None)

    for tg in _alb._tgs.values():
        if physical_id in tg.get("LoadBalancerArns", []):
            tg["LoadBalancerArns"] = [a for a in tg.get("LoadBalancerArns", []) if a != physical_id]

    _alb._lbs.pop(physical_id, None)
    _alb._lb_attrs.pop(physical_id, None)
    _alb._tags.pop(physical_id, None)


def _elbv2_listener_create(logical_id, props, stack_name):
    lb_arn = props.get("LoadBalancerArn", "")
    lb = _alb._lbs.get(lb_arn)
    if not lb:
        raise ValueError(f"Load balancer not found for Listener: {lb_arn}")

    listener_id = _alb._short_id()
    lb_name = lb["LoadBalancerName"]
    lb_id = _alb._load_balancer_id_from_arn(lb_arn)
    listener_arn = (
        f"arn:aws:elasticloadbalancing:{get_region()}:{get_account_id()}:"
        f"listener/app/{lb_name}/{lb_id}/{listener_id}"
    )

    actions = []
    for idx, action in enumerate(props.get("DefaultActions", []) or [], start=1):
        if not isinstance(action, dict):
            continue
        entry = {
            "Type": action.get("Type", "fixed-response"),
            "Order": int(action.get("Order", idx)),
        }
        tg_arn = action.get("TargetGroupArn")
        if not tg_arn:
            forward_cfg = action.get("ForwardConfig", {})
            tg_list = forward_cfg.get("TargetGroups", []) if isinstance(forward_cfg, dict) else []
            if tg_list and isinstance(tg_list[0], dict):
                tg_arn = tg_list[0].get("TargetGroupArn")
        if tg_arn:
            entry["TargetGroupArn"] = tg_arn
            if tg_arn in _alb._tgs and lb_arn not in _alb._tgs[tg_arn].get("LoadBalancerArns", []):
                _alb._tgs[tg_arn].setdefault("LoadBalancerArns", []).append(lb_arn)
        if isinstance(action.get("FixedResponseConfig"), dict):
            entry["FixedResponseConfig"] = action["FixedResponseConfig"]
        if isinstance(action.get("RedirectConfig"), dict):
            entry["RedirectConfig"] = action["RedirectConfig"]
        actions.append(entry)

    listener = {
        "ListenerArn": listener_arn,
        "LoadBalancerArn": lb_arn,
        "Port": int(props.get("Port", 80) or 80),
        "Protocol": props.get("Protocol", "HTTP"),
        "DefaultActions": actions,
    }
    _alb._listeners[listener_arn] = listener
    _alb._tags[listener_arn] = _elbv2_tags(props.get("Tags"))

    # Match alb service semantics: create a default rule for every listener.
    rule_id = _alb._short_id()
    rule_arn = (
        f"arn:aws:elasticloadbalancing:{get_region()}:{get_account_id()}:"
        f"listener-rule/app/{lb_name}/{lb_id}/{listener_id}/{rule_id}"
    )
    _alb._rules[rule_arn] = {
        "RuleArn": rule_arn,
        "ListenerArn": listener_arn,
        "Priority": "default",
        "Conditions": [],
        "Actions": actions,
        "IsDefault": True,
    }

    return listener_arn, {"ListenerArn": listener_arn, "Arn": listener_arn}


def _elbv2_listener_delete(physical_id, props):
    _alb._listeners.pop(physical_id, None)
    _alb._tags.pop(physical_id, None)
    for rule_arn in [k for k, v in list(_alb._rules.items()) if v.get("ListenerArn") == physical_id]:
        _alb._rules.pop(rule_arn, None)
        _alb._tags.pop(rule_arn, None)


# ---------------------------------------------------------------------------
# Lambda LayerVersion
# ---------------------------------------------------------------------------

def _lambda_layer_create(logical_id, props, stack_name):
    layer_name = props.get("LayerName") or _physical_name(stack_name, logical_id, max_len=64)
    runtimes = props.get("CompatibleRuntimes", [])
    architectures = props.get("CompatibleArchitectures", [])

    content = props.get("Content", {})
    s3_bucket = content.get("S3Bucket", "")
    s3_key = content.get("S3Key", "")

    if layer_name not in _lambda_svc._layers:
        _lambda_svc._layers[layer_name] = {"versions": [], "next_version": 1}
    layer = _lambda_svc._layers[layer_name]
    ver = layer["next_version"]
    layer["next_version"] = ver + 1

    import base64
    import hashlib
    zip_data = None
    if s3_bucket and s3_key:
        zip_data = _s3._get_object_data(s3_bucket, s3_key)

    layer_arn = f"arn:aws:lambda:{get_region()}:{get_account_id()}:layer:{layer_name}"
    version_arn = f"{layer_arn}:{ver}"

    ver_config = {
        "LayerArn": layer_arn,
        "LayerVersionArn": version_arn,
        "Version": ver,
        "Description": props.get("Description", ""),
        "CompatibleRuntimes": runtimes,
        "CompatibleArchitectures": architectures,
        "LicenseInfo": props.get("LicenseInfo", ""),
        "CreatedDate": now_iso(),
        "Content": {
            "Location": _lambda_svc._layer_content_url(layer_name, ver),
            "CodeSha256": (base64.b64encode(hashlib.sha256(zip_data).digest()).decode() if zip_data else ""),
            "CodeSize": len(zip_data) if zip_data else 0,
        },
        # Without _zip_data the layer is silently skipped at worker spawn
        # (_resolve_layer_zip returns None), so functions deployed via CDK/CFN
        # can't import their layer packages even though list-layers shows them.
        "_zip_data": zip_data,
        "_policy": {"Version": "2012-10-17", "Id": "default", "Statement": []},
    }
    layer["versions"].append(ver_config)
    return version_arn, {"LayerVersionArn": version_arn, "Arn": version_arn}


def _lambda_layer_delete(physical_id, props):
    # physical_id is the version ARN like arn:aws:lambda:...:layer:name:1
    parts = physical_id.split(":")
    if len(parts) >= 2:
        layer_name = parts[-2].split("layer:")[-1] if "layer:" in physical_id else ""
        layer = _lambda_svc._layers.get(layer_name)
        if layer:
            layer["versions"] = [v for v in layer["versions"] if v["LayerVersionArn"] != physical_id]


# ---------------------------------------------------------------------------
# StepFunctions StateMachine
# ---------------------------------------------------------------------------

def _sfn_state_machine_create(logical_id, props, stack_name):
    name = props.get("StateMachineName") or _physical_name(stack_name, logical_id, max_len=80)
    role_arn = props.get("RoleArn", f"arn:aws:iam::{get_account_id()}:role/StepFunctionsRole")
    import json as _json

    # Real CFN accepts three mutually-exclusive definition shapes:
    #   DefinitionString       (inline JSON/YAML string — pre-existing)
    #   Definition             (inline JSON object — CDK DefinitionBody.fromString uses this)
    #   DefinitionS3Location   ({Bucket, Key, Version} — CDK DefinitionBody.fromFile uses this)
    # DefinitionSubstitutions is a Map<String, String> applied to whichever
    # source produced the definition; placeholders are `${KEY}` per the AWS spec.
    definition = None
    if props.get("DefinitionS3Location"):
        loc = props["DefinitionS3Location"] or {}
        bucket = loc.get("Bucket") or ""
        key = loc.get("Key") or ""
        version = loc.get("Version")
        if bucket and key:
            from ministack.services import s3 as _s3_svc
            try:
                blob = _s3_svc._get_object_data(bucket, key, version_id=version)
            except Exception as e:
                raise ValueError(
                    f"AWS::StepFunctions::StateMachine DefinitionS3Location fetch failed "
                    f"for s3://{bucket}/{key}: {e}"
                )
            if blob is None:
                raise ValueError(
                    f"AWS::StepFunctions::StateMachine DefinitionS3Location object not found: "
                    f"s3://{bucket}/{key}"
                )
            definition = blob.decode("utf-8", errors="replace")
    if definition is None and props.get("Definition") is not None:
        d = props["Definition"]
        definition = _json.dumps(d) if isinstance(d, (dict, list)) else str(d)
    if definition is None:
        definition = props.get("DefinitionString", "{}")
        if isinstance(definition, dict):
            definition = _json.dumps(definition)

    subs = props.get("DefinitionSubstitutions") or {}
    if subs:
        for k, v in subs.items():
            definition = definition.replace("${" + str(k) + "}", str(v))

    sm_type = props.get("StateMachineType", "STANDARD")

    arn = f"arn:aws:states:{get_region()}:{get_account_id()}:stateMachine:{name}"
    ts = now_iso()
    _sfn._state_machines[arn] = {
        "stateMachineArn": arn,
        "name": name,
        "definition": definition,
        "roleArn": role_arn,
        "type": sm_type,
        "creationDate": ts,
        "status": "ACTIVE",
        "loggingConfiguration": props.get("LoggingConfiguration", {"level": "OFF", "includeExecutionData": False}),
    }
    return arn, {"Arn": arn, "Name": name}


def _sfn_state_machine_delete(physical_id, props):
    _sfn._state_machines.pop(physical_id, None)


# ---------------------------------------------------------------------------
# AWS Certificate Manager (ACM)
# ---------------------------------------------------------------------------

def _acm_certificate_create(logical_id, props, stack_name):
    """Provision an AWS::CertificateManager::Certificate.

    Maps CFN props to acm._request_certificate: DomainName, SubjectAlternativeNames,
    ValidationMethod, Tags. Returns the CertificateArn as the physical id, so
    `Ref` resolves to the ARN (matching real CFN).
    """
    domain = props.get("DomainName", "")
    if not domain:
        raise ValueError(
            "AWS::CertificateManager::Certificate requires DomainName"
        )
    sans = props.get("SubjectAlternativeNames") or [domain]
    if isinstance(sans, str):
        sans = [sans]
    if domain not in sans:
        sans = [domain] + list(sans)
    method = props.get("ValidationMethod", "DNS")

    arn = _acm._cert_arn()
    now = now_iso()
    _acm._certificates[arn] = {
        "CertificateArn": arn,
        "DomainName": domain,
        "SubjectAlternativeNames": sans,
        "Status": "ISSUED",
        "Type": "AMAZON_ISSUED",
        "CreatedAt": now,
        "IssuedAt": now,
        "NotBefore": now,
        "NotAfter": _acm._future_iso(365 * 24 * 3600),
        "DomainValidationOptions": [_acm._validation_options(d, method) for d in sans],
        "ValidationMethod": method,
        "Tags": props.get("Tags", []),
        "Options": {
            "CertificateTransparencyLoggingPreference":
                (props.get("CertificateTransparencyLoggingPreference")
                 or (props.get("Options") or {}).get("CertificateTransparencyLoggingPreference")
                 or "ENABLED"),
        },
        "KeyAlgorithm": props.get("KeyAlgorithm", "RSA_2048"),
        "_pem_body": _acm._synthetic_pem(domain),
        "_pem_chain": "",
        "_private_key": "",
    }
    return arn, {"CertificateArn": arn, "Arn": arn}


def _acm_certificate_delete(physical_id, props):
    _acm._certificates.pop(physical_id, None)


# ---------------------------------------------------------------------------
# ELBv2 TargetGroup + ListenerRule
# ---------------------------------------------------------------------------

def _elbv2_target_group_create(logical_id, props, stack_name):
    """Provision an AWS::ElasticLoadBalancingV2::TargetGroup matching what
    `CreateTargetGroup` produces; physical id = TargetGroupArn."""
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=32)
    matcher = props.get("Matcher") or {}
    import random as _random
    import string as _string
    tid = "".join(_random.choices(_string.ascii_lowercase + _string.digits, k=16))
    arn = f"arn:aws:elasticloadbalancing:{get_region()}:{get_account_id()}:targetgroup/{name}/{tid}"
    tg = {
        "TargetGroupArn": arn,
        "TargetGroupName": name,
        "Protocol": props.get("Protocol", "HTTP"),
        "Port": int(props.get("Port", 80) or 80),
        "VpcId": props.get("VpcId", ""),
        "HealthCheckProtocol": props.get("HealthCheckProtocol", "HTTP"),
        "HealthCheckPort": props.get("HealthCheckPort", "traffic-port"),
        "HealthCheckEnabled": (
            props.get("HealthCheckEnabled", True)
            if isinstance(props.get("HealthCheckEnabled"), bool)
            else str(props.get("HealthCheckEnabled", "true")).lower() == "true"
        ),
        "HealthCheckPath": props.get("HealthCheckPath", "/"),
        "HealthCheckIntervalSeconds": int(props.get("HealthCheckIntervalSeconds", 30) or 30),
        "HealthCheckTimeoutSeconds": int(props.get("HealthCheckTimeoutSeconds", 5) or 5),
        "HealthyThresholdCount": int(props.get("HealthyThresholdCount", 5) or 5),
        "UnhealthyThresholdCount": int(props.get("UnhealthyThresholdCount", 2) or 2),
        "Matcher": {"HttpCode": matcher.get("HttpCode", "200")},
        "LoadBalancerArns": [],
        "TargetType": props.get("TargetType", "instance"),
    }
    _alb._tgs[arn] = tg
    _alb._targets[arn] = []
    _alb._tags[arn] = {t["Key"]: t["Value"] for t in (props.get("Tags") or [])}
    _alb._tg_attrs[arn] = [
        {"Key": a.get("Key", ""), "Value": a.get("Value", "")}
        for a in (props.get("TargetGroupAttributes") or [])
    ] or [
        {"Key": "deregistration_delay.timeout_seconds", "Value": "300"},
        {"Key": "stickiness.enabled", "Value": "false"},
        {"Key": "stickiness.type", "Value": "lb_cookie"},
    ]
    return arn, {
        "TargetGroupArn": arn,
        "TargetGroupName": name,
        "TargetGroupFullName": _alb._target_group_full_name_from_arn(arn),
    }


def _elbv2_target_group_delete(physical_id, props):
    _alb._tgs.pop(physical_id, None)
    _alb._targets.pop(physical_id, None)
    _alb._tags.pop(physical_id, None)
    _alb._tg_attrs.pop(physical_id, None)


def _flatten_listener_rule_conditions(cfn_conditions):
    """CFN ListenerRule Conditions accept both the flat `{Field, Values}` shape
    and the per-field nested config form (`PathPatternConfig.Values`,
    `HostHeaderConfig.Values`, `HttpHeaderConfig`, `HttpRequestMethodConfig`,
    `QueryStringConfig`, `SourceIpConfig`). MS' ALB stores the simple
    `{Field, Values}` form, so collapse the nested form into Values for the
    fields where it makes sense.
    """
    out = []
    for c in cfn_conditions or []:
        field = c.get("Field", "")
        values = c.get("Values") or []
        config_key = {
            "path-pattern": "PathPatternConfig",
            "host-header": "HostHeaderConfig",
            "http-header": "HttpHeaderConfig",
            "http-request-method": "HttpRequestMethodConfig",
            "query-string": "QueryStringConfig",
            "source-ip": "SourceIpConfig",
        }.get(field)
        if not values and config_key and c.get(config_key):
            cfg = c[config_key]
            values = cfg.get("Values") or []
            if not values and field == "query-string" and cfg.get("Values") is None:
                values = [f"{q.get('Key','')}={q.get('Value','')}" for q in (cfg.get("Values") or [])]
        out.append({"Field": field, "Values": list(values)})
    return out


def _elbv2_listener_rule_create(logical_id, props, stack_name):
    """Provision an AWS::ElasticLoadBalancingV2::ListenerRule. Conditions and
    Actions match the shape MS' ALB stores in `_alb._rules`.
    """
    l_arn = props.get("ListenerArn", "")
    if not l_arn:
        raise ValueError("AWS::ElasticLoadBalancingV2::ListenerRule requires ListenerArn")
    if l_arn not in _alb._listeners:
        raise ValueError(f"Listener '{l_arn}' not found for ListenerRule {logical_id}")
    listener = _alb._listeners[l_arn]
    lb_arn = listener["LoadBalancerArn"]
    lb_name = _alb._lbs[lb_arn]["LoadBalancerName"]
    lb_id = _alb._load_balancer_id_from_arn(lb_arn)
    l_id = _alb._listener_id_from_arn(l_arn)
    import random as _random
    import string as _string
    rule_id = "".join(_random.choices(_string.ascii_lowercase + _string.digits, k=16))
    rule_arn = (f"arn:aws:elasticloadbalancing:{get_region()}:{get_account_id()}"
                f":listener-rule/app/{lb_name}/{lb_id}/{l_id}/{rule_id}")
    # CFN Actions: list of dicts with Type, Order, TargetGroupArn / RedirectConfig
    # / FixedResponseConfig. MS' native shape matches this directly.
    actions = []
    for i, a in enumerate(props.get("Actions") or [], start=1):
        record = {"Type": a.get("Type", "forward"), "Order": int(a.get("Order", i))}
        if a.get("TargetGroupArn"):
            record["TargetGroupArn"] = a["TargetGroupArn"]
        if a.get("RedirectConfig"):
            record["RedirectConfig"] = dict(a["RedirectConfig"])
        if a.get("FixedResponseConfig"):
            record["FixedResponseConfig"] = dict(a["FixedResponseConfig"])
        actions.append(record)
    rule = {
        "RuleArn": rule_arn,
        "ListenerArn": l_arn,
        "Priority": str(props.get("Priority", 1)),
        "Conditions": _flatten_listener_rule_conditions(props.get("Conditions") or []),
        "Actions": actions,
        "IsDefault": False,
    }
    _alb._rules[rule_arn] = rule
    return rule_arn, {"RuleArn": rule_arn}


def _elbv2_listener_rule_delete(physical_id, props):
    _alb._rules.pop(physical_id, None)


# ---------------------------------------------------------------------------
# Route53 HostedZone
# ---------------------------------------------------------------------------

def _r53_hosted_zone_create(logical_id, props, stack_name):
    zone_name = props.get("Name", "")
    if not zone_name.endswith("."):
        zone_name += "."

    zone_id = _r53._zone_id()
    caller_ref = new_uuid()

    _r53._zones[zone_id] = {
        "id": zone_id,
        "name": zone_name,
        "caller_reference": caller_ref,
        "comment": (props.get("HostedZoneConfig", {}) or {}).get("Comment", ""),
        "private": False,
    }
    _r53._records[zone_id] = _r53._default_records(zone_name)
    _r53._caller_refs[caller_ref] = zone_id
    return zone_id, {"Id": zone_id, "NameServers": ["ns-1.awsdns-01.org", "ns-2.awsdns-02.co.uk"]}


def _r53_hosted_zone_delete(physical_id, props):
    _r53._zones.pop(physical_id, None)
    _r53._records.pop(physical_id, None)


def _r53_normalize_hosted_zone_id(zone_ref: str) -> str:
    if not zone_ref:
        return ""
    z = str(zone_ref).strip()
    if z.startswith("/hostedzone/"):
        z = z[len("/hostedzone/"):]
    return z


def _r53_record_set_build_rs(props: dict) -> dict:
    name = _r53._normalise_name(str(props.get("Name", "") or ""))
    rtype = str(props.get("Type", "") or "").upper()
    if not name or not rtype:
        raise ValueError("CloudFormation properties 'Name' and 'Type' are required for AWS::Route53::RecordSet")
    rs: dict = {"Name": name, "Type": rtype}
    if props.get("SetIdentifier") not in (None, ""):
        rs["SetIdentifier"] = str(props["SetIdentifier"])
    if props.get("Weight") not in (None, ""):
        rs["Weight"] = int(props["Weight"])
    if props.get("Region"):
        rs["Region"] = str(props["Region"])
    if props.get("Failover"):
        rs["Failover"] = str(props["Failover"])
    if props.get("HealthCheckId"):
        rs["HealthCheckId"] = str(props["HealthCheckId"])
    if props.get("MultiValueAnswer") is not None:
        mv = props["MultiValueAnswer"]
        if isinstance(mv, str):
            rs["MultiValueAnswer"] = mv.lower() == "true"
        else:
            rs["MultiValueAnswer"] = bool(mv)
    geo = props.get("GeoLocation")
    if isinstance(geo, dict) and geo:
        rs["GeoLocation"] = {k: v for k, v in geo.items() if v not in (None, "", False)}
    crc = props.get("CidrRoutingConfig")
    if isinstance(crc, dict) and crc:
        rs["CidrRoutingConfig"] = crc
    ttl = props.get("TTL")
    if ttl not in (None, ""):
        rs["TTL"] = str(ttl)
    if props.get("ResourceRecords"):
        vals = []
        for rr in props["ResourceRecords"]:
            if isinstance(rr, dict):
                vals.append(str(rr.get("Value", "")))
            else:
                vals.append(str(rr))
        rs["ResourceRecords"] = vals
    at = props.get("AliasTarget")
    if isinstance(at, dict) and at:
        dns_name = str(at.get("DNSName", "") or "")
        if dns_name and not dns_name.endswith("."):
            dns_name += "."
        ev = at.get("EvaluateTargetHealth", False)
        if isinstance(ev, str):
            ev = ev.lower() == "true"
        rs["AliasTarget"] = {
            "HostedZoneId": str(at.get("HostedZoneId", "") or ""),
            "DNSName": dns_name,
            "EvaluateTargetHealth": bool(ev),
        }
    return rs


def _r53_resolve_hosted_zone_id(props: dict) -> str:
    hz_id = props.get("HostedZoneId")
    if hz_id not in (None, ""):
        return _r53_normalize_hosted_zone_id(str(hz_id))
    hz_name = props.get("HostedZoneName")
    if hz_name not in (None, ""):
        want = _r53._normalise_name(str(hz_name))
        with _r53._lock:
            for z in _r53._zones.values():
                if z["name"] == want:
                    return z["id"]
    raise ValueError("HostedZoneId or HostedZoneName is required for AWS::Route53::RecordSet")


def _r53_record_set_create(logical_id, props, stack_name):
    zone_id = _r53_resolve_hosted_zone_id(props)
    rs = _r53_record_set_build_rs(props)
    key = _r53._rs_key(rs)
    with _r53._lock:
        if zone_id not in _r53._zones:
            raise ValueError(f"No hosted zone with id '{zone_id}'")
        current = list(_r53._records.get(zone_id, []))
        if any(_r53._rs_key(r) == key for r in current):
            raise ValueError(
                f"Route 53 record already exists: {rs['Name']} type {rs['Type']} "
                f"set={rs.get('SetIdentifier', '')!r}"
            )
        current.append(rs)
        _r53._records[zone_id] = current
    fqdn = rs["Name"]
    return fqdn, {"Name": fqdn}


def _r53_record_set_delete(physical_id, props):
    zone_id = _r53_resolve_hosted_zone_id(props)
    rs = _r53_record_set_build_rs(props)
    key = _r53._rs_key(rs)
    with _r53._lock:
        if zone_id not in _r53._records:
            return
        _r53._records[zone_id] = [
            r for r in _r53._records[zone_id] if _r53._rs_key(r) != key
        ]


# ---------------------------------------------------------------------------
# CloudWatch Alarm (standard metric alarms)
# ---------------------------------------------------------------------------


def _cw_metric_alarm_create(logical_id, props, stack_name):
    if props.get("Metrics"):
        raise ValueError(
            "AWS::CloudWatch::Alarm Properties.Metrics (metric math) is not supported; "
            "use MetricName and Namespace."
        )
    name = props.get("AlarmName") or _physical_name(stack_name, logical_id, max_len=255)
    metric_name = props.get("MetricName")
    namespace = props.get("Namespace")
    if not metric_name or not namespace:
        raise ValueError("MetricName and Namespace are required for AWS::CloudWatch::Alarm")
    comparison = props.get("ComparisonOperator")
    if not comparison:
        raise ValueError("ComparisonOperator is required for AWS::CloudWatch::Alarm")
    if props.get("Threshold") is None:
        raise ValueError("Threshold is required for AWS::CloudWatch::Alarm")

    period = int(props.get("Period", 60))
    eval_periods = int(props.get("EvaluationPeriods", 1))
    dta = props.get("DatapointsToAlarm")
    datapoints = int(dta if dta is not None else eval_periods)
    ext_stat = props.get("ExtendedStatistic") or None
    if isinstance(ext_stat, str) and not ext_stat.strip():
        ext_stat = None
    statistic = props.get("Statistic") or "Average"

    dims = props.get("Dimensions") or []
    if not isinstance(dims, list):
        dims = []

    ae = props.get("ActionsEnabled", True)
    if isinstance(ae, str):
        ae = ae.lower() not in ("false", "0", "no")

    def _as_str_list(key):
        v = props.get(key) or []
        if isinstance(v, list):
            return [str(x) for x in v]
        if v in (None, ""):
            return []
        return [str(v)]

    alarm_actions = _as_str_list("AlarmActions")
    ok_actions = _as_str_list("OKActions")
    insuff_actions = _as_str_list("InsufficientDataActions")
    treat = props.get("TreatMissingData", "missing") or "missing"

    alarm = {
        "AlarmName": name,
        "AlarmArn": f"arn:aws:cloudwatch:{get_region()}:{get_account_id()}:alarm:{name}",
        "AlarmDescription": props.get("AlarmDescription", "") or "",
        "MetricName": metric_name,
        "Namespace": namespace,
        "Statistic": statistic,
        "ExtendedStatistic": ext_stat,
        "Period": period,
        "EvaluationPeriods": eval_periods,
        "DatapointsToAlarm": datapoints,
        "Threshold": float(props["Threshold"]),
        "ComparisonOperator": comparison,
        "TreatMissingData": treat,
        "StateValue": _cw._alarms[name]["StateValue"]
        if name in _cw._alarms
        else "INSUFFICIENT_DATA",
        "StateReason": _cw._alarms[name]["StateReason"]
        if name in _cw._alarms
        else "Unchecked: Initial alarm creation",
        "StateUpdatedTimestamp": int(time.time()),
        "ActionsEnabled": ae,
        "AlarmActions": alarm_actions,
        "OKActions": ok_actions,
        "InsufficientDataActions": insuff_actions,
        "Dimensions": dims,
        "Unit": props.get("Unit"),
        "AlarmConfigurationUpdatedTimestamp": int(time.time()),
    }
    _cw.cloudformation_put_metric_alarm(alarm)
    arn = alarm["AlarmArn"]
    return name, {"Arn": arn}


def _cw_metric_alarm_delete(physical_id, props):
    _cw.cloudformation_delete_metric_alarm(physical_id)


# ---------------------------------------------------------------------------
# CloudWatch Dashboard
# ---------------------------------------------------------------------------


def _cw_dashboard_name(logical_id, props, stack_name):
    name = props.get("DashboardName") or _physical_name(
        stack_name, logical_id, max_len=255
    )
    if not isinstance(name, str) or not 1 <= len(name) <= 255:
        raise ValueError("DashboardName must be between 1 and 255 characters")
    return name


def _cw_dashboard_body(props):
    body = props.get("DashboardBody")
    if not isinstance(body, str) or not body:
        raise ValueError("DashboardBody is required for AWS::CloudWatch::Dashboard")
    return body


def _cw_dashboard_create(logical_id, props, stack_name):
    name = _cw_dashboard_name(logical_id, props, stack_name)
    _cw.cloudformation_put_dashboard(name, _cw_dashboard_body(props))
    return name, {}


def _cw_dashboard_update(physical_id, old_props, new_props, stack_name):
    # DashboardBody updates happen in place. DashboardName changes require
    # replacement, which we model by creating the new dashboard before
    # deleting the previous physical resource.
    name = new_props.get("DashboardName") or physical_id
    if not isinstance(name, str) or not 1 <= len(name) <= 255:
        raise ValueError("DashboardName must be between 1 and 255 characters")
    _cw.cloudformation_put_dashboard(name, _cw_dashboard_body(new_props))
    if name != physical_id:
        _cw.cloudformation_delete_dashboard(physical_id)
    return name, {}


def _cw_dashboard_delete(physical_id, props):
    _cw.cloudformation_delete_dashboard(physical_id)


# ---------------------------------------------------------------------------
# ApiGatewayV2 Api
# ---------------------------------------------------------------------------

def _apigw_v2_api_create(logical_id, props, stack_name):
    api_id = _apigw_v2._resolve_custom_api_id(props.get("Tags", {}), _apigw_v2._apis) or new_uuid()[:8]
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=128)
    protocol = props.get("ProtocolType", "HTTP")
    api = {
        "apiId": api_id,
        "name": name,
        "protocolType": protocol,
        "apiEndpoint": f"http://{api_id}.execute-api.{_MINISTACK_HOST}:{os.environ.get('GATEWAY_PORT', '4566')}",
        "createdDate": now_iso(),
        "routeSelectionExpression": props.get("RouteSelectionExpression", "$request.method $request.path"),
        "apiKeySelectionExpression": props.get("ApiKeySelectionExpression", "$request.header.x-api-key"),
        "tags": props.get("Tags", {}),
        "disableSchemaValidation": props.get("DisableSchemaValidation", False),
        "disableExecuteApiEndpoint": props.get("DisableExecuteApiEndpoint", False),
        "version": props.get("Version", ""),
    }
    if props.get("CorsConfiguration"):
        api["corsConfiguration"] = props["CorsConfiguration"]
    _apigw_v2._apis[api_id] = api
    _apigw_v2._api_regions[api_id] = get_region()
    _apigw_v2._routes[api_id] = {}
    _apigw_v2._integrations[api_id] = {}
    _apigw_v2._stages[api_id] = {}
    _apigw_v2._deployments[api_id] = {}
    return api_id, {"ApiId": api_id, "ApiEndpoint": api["apiEndpoint"]}


def _apigw_v2_api_delete(physical_id, props):
    _apigw_v2._apis.pop(physical_id, None)
    _apigw_v2._api_regions.pop(physical_id, None)
    _apigw_v2._routes.pop(physical_id, None)
    _apigw_v2._integrations.pop(physical_id, None)
    _apigw_v2._stages.pop(physical_id, None)
    _apigw_v2._deployments.pop(physical_id, None)


# ---------------------------------------------------------------------------
# ApiGatewayV2 Stage
# ---------------------------------------------------------------------------

def _apigw_v2_stage_create(logical_id, props, stack_name):
    api_id = props.get("ApiId", "")
    stage_name = props.get("StageName", "$default")
    stage = {
        "stageName": stage_name,
        "autoDeploy": props.get("AutoDeploy", False),
        "createdDate": now_iso(),
        "lastUpdatedDate": now_iso(),
        "stageVariables": props.get("StageVariables", {}),
        "description": props.get("Description", ""),
        "defaultRouteSettings": props.get("DefaultRouteSettings", {}),
        "routeSettings": props.get("RouteSettings", {}),
        "tags": props.get("Tags", {}),
    }
    _apigw_v2._stages.setdefault(api_id, {})[stage_name] = stage
    physical_id = f"{api_id}/{stage_name}"
    return physical_id, {"StageName": stage_name}


def _apigw_v2_stage_delete(physical_id, props):
    parts = physical_id.split("/", 1)
    if len(parts) == 2:
        api_id, stage_name = parts
        stages = _apigw_v2._stages.get(api_id, {})
        stages.pop(stage_name, None)


# ---------------------------------------------------------------------------
# ApiGatewayV2 Integration
# ---------------------------------------------------------------------------

def _apigw_v2_integration_create(logical_id, props, stack_name):
    api_id = props.get("ApiId", "")
    int_id = new_uuid()[:8]
    integration = {
        "integrationId": int_id,
        "integrationType": props.get("IntegrationType", "AWS_PROXY"),
        "integrationUri": props.get("IntegrationUri", ""),
        "integrationMethod": props.get("IntegrationMethod", "POST"),
        "payloadFormatVersion": props.get("PayloadFormatVersion", "2.0"),
        "timeoutInMillis": props.get("TimeoutInMillis", 30000),
        "connectionType": props.get("ConnectionType", "INTERNET"),
        "connectionId": props.get("ConnectionId", ""),
        "description": props.get("Description", ""),
        "requestParameters": props.get("RequestParameters", {}),
        "requestTemplates": props.get("RequestTemplates", {}),
        "responseParameters": props.get("ResponseParameters", {}),
        "contentHandlingStrategy": props.get("ContentHandlingStrategy"),
    }
    _apigw_v2._integrations.setdefault(api_id, {})[int_id] = integration
    # AWS returns just the integration ID as the physical ID (Ref).
    # Store apiId in outputs so delete can find the right API.
    return int_id, {"IntegrationId": int_id, "ApiId": api_id}


def _apigw_v2_integration_delete(physical_id, props):
    api_id = props.get("ApiId", "")
    int_id = physical_id
    # Backwards compat: old physical IDs were "{apiId}/{integrationId}"
    if "/" in physical_id:
        parts = physical_id.split("/", 1)
        api_id, int_id = parts[0], parts[1]
    if api_id:
        integrations = _apigw_v2._integrations.get(api_id, {})
        integrations.pop(int_id, None)


# ---------------------------------------------------------------------------
# ApiGatewayV2 Route
# ---------------------------------------------------------------------------

def _apigw_v2_route_create(logical_id, props, stack_name):
    api_id = props.get("ApiId", "")
    route_id = new_uuid()[:8]
    route = {
        "routeId": route_id,
        "routeKey": props.get("RouteKey", "$default"),
        "target": props.get("Target", ""),
        "authorizationType": props.get("AuthorizationType", "NONE"),
        "authorizerId": props.get("AuthorizerId"),
        "authorizationScopes": props.get("AuthorizationScopes", []),
        "apiKeyRequired": props.get("ApiKeyRequired", False),
        "operationName": props.get("OperationName", ""),
        "requestModels": props.get("RequestModels", {}),
        "requestParameters": props.get("RequestParameters", {}),
    }
    _apigw_v2._routes.setdefault(api_id, {})[route_id] = route
    physical_id = f"{api_id}/{route_id}"
    return physical_id, {"RouteId": route_id}


def _apigw_v2_route_delete(physical_id, props):
    parts = physical_id.split("/", 1)
    if len(parts) == 2:
        api_id, route_id = parts
        routes = _apigw_v2._routes.get(api_id, {})
        routes.pop(route_id, None)


# ---------------------------------------------------------------------------
# ApiGatewayV2 Authorizer
# ---------------------------------------------------------------------------

def _apigw_v2_authorizer_create(logical_id, props, stack_name):
    """Maps CFN properties onto the same in-memory authorizer store the
    control-plane CreateAuthorizer API (and Terraform's aws_apigatewayv2_authorizer)
    already write to, so a CFN-created authorizer is enforced identically at
    request time. See _validate_jwt_authorizer / _resolve_jwks_url.

    jwtConfiguration is stored camelCase (the API's actual wire shape, which
    the JSON response is returned as-is) — CFN's JwtConfiguration.Audience /
    .Issuer are translated here rather than passed through PascalCase.
    """
    api_id = props.get("ApiId", "")
    auth_id = new_uuid()[:8]
    jwt_cfg = props.get("JwtConfiguration") or {}
    authorizer = {
        "authorizerId": auth_id,
        "authorizerType": props.get("AuthorizerType", "JWT"),
        "name": props.get("Name", logical_id),
        "identitySource": props.get("IdentitySource", ["$request.header.Authorization"]),
        "jwtConfiguration": {
            "audience": jwt_cfg.get("Audience", []),
            "issuer": jwt_cfg.get("Issuer", ""),
        },
        "authorizerUri": props.get("AuthorizerUri", ""),
        "authorizerPayloadFormatVersion": props.get("AuthorizerPayloadFormatVersion", "2.0"),
        "authorizerResultTtlInSeconds": props.get("AuthorizerResultTtlInSeconds", 300),
        "enableSimpleResponses": props.get("EnableSimpleResponses", False),
        "authorizerCredentialsArn": props.get("AuthorizerCredentialsArn", ""),
    }
    _apigw_v2._authorizers.setdefault(api_id, {})[auth_id] = authorizer
    return auth_id, {"AuthorizerId": auth_id}


def _apigw_v2_authorizer_delete(physical_id, props):
    api_id = props.get("ApiId", "")
    authorizers = _apigw_v2._authorizers.get(api_id, {})
    authorizers.pop(physical_id, None)


# ---------------------------------------------------------------------------
# SES EmailIdentity
# ---------------------------------------------------------------------------

def _ses_email_identity_create(logical_id, props, stack_name):
    identity = props.get("EmailIdentity", "")
    _ses._identities[identity] = _ses._make_identity(identity,
        "Domain" if "@" not in identity else "EmailAddress")
    return identity, {"EmailIdentity": identity}


def _ses_email_identity_delete(physical_id, props):
    _ses._identities.pop(physical_id, None)


# ---------------------------------------------------------------------------
# WAFv2 WebACL
# ---------------------------------------------------------------------------

def _waf_web_acl_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=128)
    uid = new_uuid()
    lock_token = new_uuid()
    scope = props.get("Scope", "REGIONAL")
    arn = f"arn:aws:wafv2:{get_region()}:{get_account_id()}:{scope.lower()}/webacl/{name}/{uid}"
    _waf._web_acls[uid] = {
        "ARN": arn, "Id": uid, "Name": name,
        "Description": props.get("Description", ""),
        "DefaultAction": props.get("DefaultAction", {"Allow": {}}),
        "Rules": props.get("Rules", []),
        "VisibilityConfig": props.get("VisibilityConfig", {}),
        "Capacity": 0,
        "LockToken": lock_token,
        "Scope": scope,
    }
    return uid, {"Arn": arn, "Id": uid}


def _waf_web_acl_delete(physical_id, props):
    _waf._web_acls.pop(physical_id, None)


# ---------------------------------------------------------------------------
# CloudFront Distribution
# ---------------------------------------------------------------------------

def _cf_distribution_create(logical_id, props, stack_name):
    dist_config = props.get("DistributionConfig", props)
    dist_id = _cf._dist_id()
    arn = f"arn:aws:cloudfront::{get_account_id()}:distribution/{dist_id}"

    origins = dist_config.get("Origins", [])
    default_cache = dist_config.get("DefaultCacheBehavior", {})

    _cf._distributions[dist_id] = {
        "Id": dist_id,
        "ARN": arn,
        "Status": "Deployed",
        "DomainName": f"{dist_id}.cloudfront.net",
        "LastModifiedTime": now_iso(),
        "ETag": new_uuid(),
        "config_xml": "",
        "enabled": dist_config.get("Enabled", True),
    }
    _cf._invalidations[dist_id] = []
    return dist_id, {"Arn": arn, "DomainName": f"{dist_id}.cloudfront.net", "Id": dist_id}


def _cf_distribution_delete(physical_id, props):
    _cf._distributions.pop(physical_id, None)
    _cf._invalidations.pop(physical_id, None)


# ---------------------------------------------------------------------------
# CloudFront KeyValueStore (management plane)
# ---------------------------------------------------------------------------

def _cf_kvs_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=64)
    arn = _cf._kvs_arn(name)
    if name not in _cf._kvstores:
        _cf._kvstores[name] = {
            "Id": new_uuid(),
            "Name": name,
            "Comment": props.get("Comment", ""),
            "ARN": arn,
            "Status": "READY",
            "LastModifiedTime": now_iso(),
            "ETag": new_uuid(),
        }
    record = _cf._kvstores[name]
    # Return refs the spec exposes via Fn::GetAtt: Arn, Id, Status.
    return name, {"Arn": arn, "Id": record["Id"], "Status": record["Status"]}


def _cf_kvs_update(physical_id, old_props, new_props, stack_name):
    """Update the KVS record (Comment is the only mutable field per AWS spec).

    Name and ImportSource are create-only — a name change requires replacement,
    handled at the CFN engine level by destroy+create.
    """
    record = _cf._kvstores.get(physical_id)
    if not record:
        # KVS was deleted out-of-band — recreate to converge to the new state.
        return _cf_kvs_create(physical_id, new_props, stack_name)
    if "Comment" in new_props:
        record["Comment"] = new_props["Comment"] or ""
    record["ETag"] = new_uuid()
    record["LastModifiedTime"] = now_iso()
    return physical_id, {"Arn": record["ARN"], "Id": record["Id"], "Status": record["Status"]}


def _cf_kvs_delete(physical_id, props):
    _cf._kvstores.pop(physical_id, None)


# ---------------------------------------------------------------------------
# RDS DBCluster
# ---------------------------------------------------------------------------

def _rds_db_cluster_create(logical_id, props, stack_name):
    cluster_id = props.get("DBClusterIdentifier") or _physical_name(stack_name, logical_id, lowercase=True, max_len=63)
    engine = props.get("Engine", "aurora-postgresql")
    engine_version = props.get("EngineVersion", "15.4")
    master_user = props.get("MasterUsername", "admin")
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:cluster:{cluster_id}"
    suffix = new_uuid()[:8]

    _rds._clusters[cluster_id] = {
        "DBClusterIdentifier": cluster_id,
        "DBClusterArn": arn,
        "Engine": engine,
        "EngineVersion": engine_version,
        "EngineMode": props.get("EngineMode", "provisioned"),
        "Status": "available",
        "MasterUsername": master_user,
        "DatabaseName": props.get("DatabaseName", ""),
        "Endpoint": f"{cluster_id}.cluster-{suffix}.{get_region()}.rds.amazonaws.com",
        "ReaderEndpoint": f"{cluster_id}.cluster-ro-{suffix}.{get_region()}.rds.amazonaws.com",
        "Port": int(props.get("Port", 5432)),
        "MultiAZ": props.get("MultiAZ", False),
        "AvailabilityZones": [f"{get_region()}a", f"{get_region()}b", f"{get_region()}c"],
        "DBClusterMembers": [],
        "VpcSecurityGroups": [],
        "DBSubnetGroup": props.get("DBSubnetGroupName", "default"),
        "StorageEncrypted": props.get("StorageEncrypted", False),
        "DeletionProtection": props.get("DeletionProtection", False),
        "CopyTagsToSnapshot": props.get("CopyTagsToSnapshot", False),
        "AllocatedStorage": 1,
        "ClusterCreateTime": now_iso(),
        "BackupRetentionPeriod": int(props.get("BackupRetentionPeriod", 1)),
    }
    return cluster_id, {
        "Arn": arn,
        "ClusterResourceId": f"cluster-{new_uuid()[:20]}",
        "Endpoint.Address": f"{cluster_id}.cluster-{suffix}.{get_region()}.rds.amazonaws.com",
        "Endpoint.Port": str(int(props.get("Port", 5432))),
        "ReadEndpoint.Address": f"{cluster_id}.cluster-ro-{suffix}.{get_region()}.rds.amazonaws.com",
    }


def _rds_db_cluster_delete(physical_id, props):
    _rds._clusters.pop(physical_id, None)


# ---------------------------------------------------------------------------
# RDS DBInstance
# ---------------------------------------------------------------------------

def _rds_db_instance_create(logical_id, props, stack_name):
    """Provision an AWS::RDS::DBInstance.

    Writes the instance record directly into rds._instances with the same
    shape `CreateDBInstance` produces, but does NOT spawn the Docker DB
    container (CFN provisioning is metadata-only; real DB connectivity
    happens via the CLI / SDK path which already handles container spawn).
    """
    db_id = props.get("DBInstanceIdentifier") or _physical_name(
        stack_name, logical_id, lowercase=True, max_len=63
    )
    engine = props.get("Engine", "postgres")
    engine_version = props.get("EngineVersion") or _rds._default_engine_version(engine)
    db_class = props.get("DBInstanceClass", "db.t3.micro")
    master_user = props.get("MasterUsername", "admin")
    master_pass = props.get("MasterUserPassword", "password")
    db_name = props.get("DBName", "")
    cluster_id = props.get("DBClusterIdentifier", "")

    # Aurora cluster members inherit master creds from the cluster.
    if cluster_id and cluster_id in _rds._clusters:
        parent = _rds._clusters[cluster_id]
        master_user = props.get("MasterUsername") or parent.get("MasterUsername", master_user)
        master_pass = props.get("MasterUserPassword") or parent.get("_MasterUserPassword", master_pass)
        if not db_name:
            db_name = parent.get("DatabaseName", "")

    port = int(props.get("Port") or _rds._default_port(engine))
    allocated_storage = int(props.get("AllocatedStorage") or 20)
    storage_type = props.get("StorageType", "gp2")
    subnet_group_name = props.get("DBSubnetGroupName", "default")
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:db:{db_id}"
    dbi_resource_id = f"db-{new_uuid().replace('-', '')[:20].upper()}"
    param_group_name = (
        props.get("DBParameterGroupName")
        or f"default.{engine}{engine_version.split('.')[0]}"
    )

    vpc_sgs = props.get("VPCSecurityGroups") or props.get("VpcSecurityGroupIds") or []
    if isinstance(vpc_sgs, str):
        vpc_sgs = [vpc_sgs]
    vpc_sg_list = [{"VpcSecurityGroupId": sg, "Status": "active"} for sg in vpc_sgs]

    subnet_group = _rds._subnet_groups.get(subnet_group_name, {
        "DBSubnetGroupName": subnet_group_name,
        "DBSubnetGroupDescription": "default",
        "SubnetGroupStatus": "Complete",
        "Subnets": [],
        "VpcId": "vpc-00000000",
        "DBSubnetGroupArn": f"arn:aws:rds:{get_region()}:{get_account_id()}:subgrp:{subnet_group_name}",
    })

    instance = {
        "DBInstanceIdentifier": db_id,
        "DBInstanceClass": db_class,
        "Engine": engine,
        "EngineVersion": engine_version,
        "DBInstanceStatus": "available",
        "MasterUsername": master_user,
        "_MasterUserPassword": master_pass,
        "DBName": db_name or "mydb",
        "Endpoint": {
            "Address": f"{db_id}.{new_uuid()[:8]}.{get_region()}.rds.amazonaws.com",
            "Port": port,
            "HostedZoneId": "Z2R2ITUGPM61AM",
        },
        "AllocatedStorage": allocated_storage,
        "InstanceCreateTime": _rds._format_time(time.time()),
        "PreferredBackupWindow": props.get("PreferredBackupWindow", "03:00-04:00"),
        "BackupRetentionPeriod": int(props.get("BackupRetentionPeriod", 1)),
        "DBSecurityGroups": [],
        "VpcSecurityGroups": vpc_sg_list,
        "DBParameterGroups": [{
            "DBParameterGroupName": param_group_name,
            "ParameterApplyStatus": "in-sync",
        }],
        "AvailabilityZone": props.get("AvailabilityZone", f"{get_region()}a"),
        "DBSubnetGroup": subnet_group,
        "PreferredMaintenanceWindow": props.get("PreferredMaintenanceWindow", "sun:05:00-sun:06:00"),
        "PendingModifiedValues": {},
        "MultiAZ": bool(props.get("MultiAZ", False)),
        "AutoMinorVersionUpgrade": bool(props.get("AutoMinorVersionUpgrade", True)),
        "ReadReplicaDBInstanceIdentifiers": [],
        "ReadReplicaSourceDBInstanceIdentifier": "",
        "LicenseModel": _rds._license_model(engine),
        "OptionGroupMemberships": [{
            "OptionGroupName": f"default:{engine}-{engine_version.split('.')[0]}",
            "Status": "in-sync",
        }],
        "PubliclyAccessible": bool(props.get("PubliclyAccessible", False)),
        "StorageType": storage_type,
        "StorageEncrypted": bool(props.get("StorageEncrypted", False)),
        "KmsKeyId": props.get("KmsKeyId", ""),
        "DbiResourceId": dbi_resource_id,
        "CACertificateIdentifier": "rds-ca-rsa2048-g1",
        "CopyTagsToSnapshot": bool(props.get("CopyTagsToSnapshot", False)),
        "MonitoringInterval": int(props.get("MonitoringInterval", 0)),
        "MonitoringRoleArn": props.get("MonitoringRoleArn", ""),
        "PromotionTier": int(props.get("PromotionTier", 1)),
        "DBInstanceArn": arn,
        "DBClusterIdentifier": cluster_id,
        "IAMDatabaseAuthenticationEnabled": bool(props.get("EnableIAMDatabaseAuthentication", False)),
        "DeletionProtection": bool(props.get("DeletionProtection", False)),
        "PerformanceInsightsEnabled": bool(props.get("EnablePerformanceInsights", False)),
        "TagList": props.get("Tags", []),
    }
    import time as _time
    instance["LatestRestorableTime"] = _rds._format_time(_time.time())

    _rds._instances[db_id] = instance
    if cluster_id and cluster_id in _rds._clusters:
        members = _rds._clusters[cluster_id].setdefault("DBClusterMembers", [])
        if not any(m.get("DBInstanceIdentifier") == db_id for m in members):
            members.append({
                "DBInstanceIdentifier": db_id,
                "IsClusterWriter": True,
                "DBClusterParameterGroupStatus": "in-sync",
                "PromotionTier": int(props.get("PromotionTier", 1)),
            })

    return db_id, {
        "Endpoint.Address": instance["Endpoint"]["Address"],
        "Endpoint.Port": str(port),
        "DbiResourceId": dbi_resource_id,
        "DBInstanceArn": arn,
    }


def _rds_db_instance_delete(physical_id, props):
    instance = _rds._instances.pop(physical_id, None)
    if instance:
        cluster_id = instance.get("DBClusterIdentifier")
        if cluster_id and cluster_id in _rds._clusters:
            members = _rds._clusters[cluster_id].get("DBClusterMembers", [])
            _rds._clusters[cluster_id]["DBClusterMembers"] = [
                m for m in members if m.get("DBInstanceIdentifier") != physical_id
            ]


# ---------------------------------------------------------------------------
# AutoScaling Group
# ---------------------------------------------------------------------------

def _asg_create(logical_id, props, stack_name):
    name = props.get("AutoScalingGroupName") or _physical_name(stack_name, logical_id, max_len=255)
    arn = f"arn:aws:autoscaling:{get_region()}:{get_account_id()}:autoScalingGroup:{new_uuid()}:autoScalingGroupName/{name}"
    asg = {
        "AutoScalingGroupName": name,
        "AutoScalingGroupARN": arn,
        "LaunchConfigurationName": props.get("LaunchConfigurationName", ""),
        "LaunchTemplate": {},
        "MinSize": int(props.get("MinSize", 0)),
        "MaxSize": int(props.get("MaxSize", 0)),
        "DesiredCapacity": int(props.get("DesiredCapacity", props.get("MinSize", 0))),
        "DefaultCooldown": int(props.get("Cooldown", 300)),
        "AvailabilityZones": props.get("AvailabilityZones", [f"{get_region()}a"]),
        "HealthCheckType": props.get("HealthCheckType", "EC2"),
        "HealthCheckGracePeriod": int(props.get("HealthCheckGracePeriod", 300)),
        "Instances": [],
        "CreatedTime": now_iso(),
        "VPCZoneIdentifier": ",".join(props.get("VPCZoneIdentifier", [])) if isinstance(props.get("VPCZoneIdentifier"), list) else props.get("VPCZoneIdentifier", ""),
        "TerminationPolicies": props.get("TerminationPolicies", ["Default"]),
        "NewInstancesProtectedFromScaleIn": props.get("NewInstancesProtectedFromScaleIn", False),
        "Tags": [],
        "Status": "",
    }
    lt = props.get("LaunchTemplate", {})
    if lt:
        asg["LaunchTemplate"] = {
            "LaunchTemplateId": lt.get("LaunchTemplateId", lt.get("LaunchTemplateName", "")),
            "LaunchTemplateName": lt.get("LaunchTemplateName", ""),
            "Version": lt.get("Version", "$Default"),
        }
    tags = []
    for t in props.get("Tags", []):
        tags.append({
            "Key": t.get("Key", ""),
            "Value": t.get("Value", ""),
            "ResourceId": name,
            "ResourceType": "auto-scaling-group",
            "PropagateAtLaunch": t.get("PropagateAtLaunch", False),
        })
    asg["Tags"] = tags
    _asg._asgs[name] = asg
    _asg._tags[name] = tags
    return name, {"Arn": arn}


def _asg_delete(physical_id, props):
    _asg._asgs.pop(physical_id, None)
    _asg._tags.pop(physical_id, None)


def _asg_lc_create(logical_id, props, stack_name):
    name = props.get("LaunchConfigurationName") or _physical_name(stack_name, logical_id, max_len=255)
    arn = f"arn:aws:autoscaling:{get_region()}:{get_account_id()}:launchConfiguration:{new_uuid()}:launchConfigurationName/{name}"
    _asg._launch_configs[name] = {
        "LaunchConfigurationName": name,
        "LaunchConfigurationARN": arn,
        "ImageId": props.get("ImageId", "ami-00000000"),
        "InstanceType": props.get("InstanceType", "t2.micro"),
        "KeyName": props.get("KeyName", ""),
        "SecurityGroups": props.get("SecurityGroups", []),
        "UserData": props.get("UserData", ""),
        "CreatedTime": now_iso(),
    }
    return name, {"Arn": arn}


def _asg_lc_delete(physical_id, props):
    _asg._launch_configs.pop(physical_id, None)


def _asg_policy_create(logical_id, props, stack_name):
    asg_name = props.get("AutoScalingGroupName", "")
    policy_name = props.get("PolicyName") or _physical_name(stack_name, logical_id, max_len=255)
    arn = f"arn:aws:autoscaling:{get_region()}:{get_account_id()}:scalingPolicy:{new_uuid()}:autoScalingGroupName/{asg_name}:policyName/{policy_name}"
    key = f"{asg_name}/{policy_name}"
    _asg._policies[key] = {
        "PolicyARN": arn,
        "PolicyName": policy_name,
        "AutoScalingGroupName": asg_name,
        "PolicyType": props.get("PolicyType", "SimpleScaling"),
        "AdjustmentType": props.get("AdjustmentType", "ChangeInCapacity"),
        "ScalingAdjustment": int(props.get("ScalingAdjustment", 0)),
        "Cooldown": int(props.get("Cooldown", 300)),
    }
    return arn, {"Arn": arn, "PolicyName": policy_name}


def _asg_policy_delete(physical_id, props):
    # physical_id is the ARN, find matching key
    for k, v in list(_asg._policies.items()):
        if v.get("PolicyARN") == physical_id:
            _asg._policies.pop(k, None)
            break


def _asg_hook_create(logical_id, props, stack_name):
    asg_name = props.get("AutoScalingGroupName", "")
    hook_name = props.get("LifecycleHookName") or _physical_name(stack_name, logical_id, max_len=255)
    key = f"{asg_name}/{hook_name}"
    _asg._hooks[key] = {
        "LifecycleHookName": hook_name,
        "AutoScalingGroupName": asg_name,
        "LifecycleTransition": props.get("LifecycleTransition", "autoscaling:EC2_INSTANCE_LAUNCHING"),
        "HeartbeatTimeout": int(props.get("HeartbeatTimeout", 3600)),
        "DefaultResult": props.get("DefaultResult", "ABANDON"),
        "NotificationTargetARN": props.get("NotificationTargetARN", ""),
        "RoleARN": props.get("RoleARN", ""),
    }
    return hook_name, {"LifecycleHookName": hook_name}


def _asg_hook_delete(physical_id, props):
    asg_name = props.get("AutoScalingGroupName", "")
    _asg._hooks.pop(f"{asg_name}/{physical_id}", None)


def _asg_scheduled_create(logical_id, props, stack_name):
    asg_name = props.get("AutoScalingGroupName", "")
    action_name = props.get("ScheduledActionName") or _physical_name(stack_name, logical_id, max_len=255)
    arn = f"arn:aws:autoscaling:{get_region()}:{get_account_id()}:scheduledUpdateGroupAction:{new_uuid()}:autoScalingGroupName/{asg_name}:scheduledActionName/{action_name}"
    key = f"{asg_name}/{action_name}"
    _asg._scheduled_actions[key] = {
        "ScheduledActionARN": arn,
        "ScheduledActionName": action_name,
        "AutoScalingGroupName": asg_name,
        "Recurrence": props.get("Recurrence", ""),
        "MinSize": int(props.get("MinSize", -1)),
        "MaxSize": int(props.get("MaxSize", -1)),
        "DesiredCapacity": int(props.get("DesiredCapacity", -1)),
    }
    return arn, {"Arn": arn, "ScheduledActionName": action_name}


def _asg_scheduled_delete(physical_id, props):
    for k, v in list(_asg._scheduled_actions.items()):
        if v.get("ScheduledActionARN") == physical_id:
            _asg._scheduled_actions.pop(k, None)
            break


# Resource Handler Registry
# ===========================================================================

# --- AWS Backup ---


def _backup_vault_create(logical_id, props, stack_name):
    name = props.get("BackupVaultName") or _physical_name(stack_name, logical_id, max_len=50)
    tags = {t["Key"]: t["Value"] for t in props.get("BackupVaultTags", [])} if isinstance(props.get("BackupVaultTags"), list) else props.get("BackupVaultTags", {})
    body = {
        "EncryptionKeyArn": props.get("EncryptionKeyArn", ""),
        "BackupVaultTags": tags,
    }
    _backup._create_vault(name, body)
    arn = _backup._vault_arn(name)
    return name, {"BackupVaultArn": arn, "BackupVaultName": name}


def _backup_vault_delete(physical_id, props):
    _backup._vaults.pop(physical_id, None)


def _backup_plan_create(logical_id, props, stack_name):
    plan_cfg = props.get("BackupPlan", {})
    tags = {t["Key"]: t["Value"] for t in props.get("BackupPlanTags", [])} if isinstance(props.get("BackupPlanTags"), list) else props.get("BackupPlanTags", {})
    body = {"BackupPlan": plan_cfg, "BackupPlanTags": tags}
    _, _, resp_bytes = _backup._create_plan(body)
    import json as _json
    resp = _json.loads(resp_bytes)
    plan_id = resp["BackupPlanId"]
    return plan_id, {"BackupPlanArn": resp["BackupPlanArn"], "BackupPlanId": plan_id, "VersionId": resp["VersionId"]}


def _backup_plan_delete(physical_id, props):
    _backup._plans.pop(physical_id, None)


def _s3tables_bucket_create(logical_id, props, stack_name):
    name = props.get("TableBucketName") or _physical_name(stack_name, logical_id, lowercase=True, max_len=63)
    arn = _s3tables._bucket_arn(name)
    _s3tables._table_buckets[name] = {
        "arn": arn, "name": name,
        "ownerAccountId": get_account_id(),
        "createdAt": now_iso(), "tableCount": 0,
    }
    _s3._buckets.setdefault(name, {"created": now_iso(), "objects": {}, "region": get_region()})
    return arn, {"TableBucketARN": arn}


def _s3tables_bucket_delete(physical_id, props):
    name = physical_id.rsplit("/", 1)[-1]
    _s3tables._table_buckets.pop(name, None)
    _s3._buckets.pop(name, None)


def _s3tables_namespace_create(logical_id, props, stack_name):
    bucket_arn = props.get("TableBucketARN", "")
    namespace = props.get("Namespace", "")
    key = _s3tables._ns_key(bucket_arn, namespace)
    _s3tables._namespaces[key] = {
        "namespace": [namespace], "createdAt": now_iso(),
        "createdBy": get_account_id(), "ownerAccountId": get_account_id(),
        "tableBucketARN": bucket_arn,
    }
    return f"{bucket_arn}|{namespace}", {"TableBucketARN": bucket_arn, "Namespace": namespace}


def _s3tables_namespace_delete(physical_id, props):
    bucket_arn = props.get("TableBucketARN", "")
    namespace = props.get("Namespace", "")
    _s3tables._namespaces.pop(_s3tables._ns_key(bucket_arn, namespace), None)


def _s3tables_table_create(logical_id, props, stack_name):
    bucket_arn = props.get("TableBucketARN", "")
    namespace = props.get("Namespace", "")
    table_name = props.get("TableName", "")
    bucket_name = bucket_arn.rsplit("/", 1)[-1]
    location = f"s3://{bucket_name}/{namespace}/{table_name}"
    iceberg_metadata = _s3tables._initial_iceberg_metadata(table_name, [], location)
    metadata_location = f"s3://{bucket_name}/{namespace}/{table_name}/metadata/v0.metadata.json"
    table_arn = _s3tables._table_arn(bucket_arn, namespace, table_name)
    key = _s3tables._table_key(bucket_arn, namespace, table_name)
    _s3tables._tables[key] = {
        "name": table_name, "tableARN": table_arn, "namespace": [namespace],
        "tableBucketARN": bucket_arn, "format": "ICEBERG",
        "createdAt": now_iso(), "modifiedAt": now_iso(),
        "ownerAccountId": get_account_id(),
        "metadataLocation": metadata_location, "warehouseLocation": location,
        "_iceberg_metadata": iceberg_metadata, "_metadata_version": 0,
        "_schema_fields": [],
    }
    for b in _s3tables._table_buckets.values():
        if b["arn"] == bucket_arn:
            b["tableCount"] = b.get("tableCount", 0) + 1
            break
    return table_arn, {"TableARN": table_arn, "TableBucketARN": bucket_arn,
                       "WarehouseLocation": location, "Namespace": namespace,
                       "TableName": table_name}


def _s3tables_table_delete(physical_id, props):
    bucket_arn = props.get("TableBucketARN", "")
    namespace = props.get("Namespace", "")
    table_name = props.get("TableName", "")
    _s3tables._tables.pop(_s3tables._table_key(bucket_arn, namespace, table_name), None)


_RESOURCE_HANDLERS = {
    "AWS::OpenSearchService::Domain": {
        "create": _opensearch_domain_create,
        "update": _opensearch_domain_update,
        "update_with_logical_id": True,
        "delete": _opensearch_domain_delete,
    },
    "AWS::S3::Bucket": {"create": _s3_create, "update": _s3_update, "delete": _s3_delete},
    "AWS::S3::BucketPolicy": {"create": _s3_bucket_policy_create, "delete": _s3_bucket_policy_delete},
    "AWS::S3Tables::TableBucket": {"create": _s3tables_bucket_create, "delete": _s3tables_bucket_delete},
    "AWS::S3Tables::Namespace": {"create": _s3tables_namespace_create, "delete": _s3tables_namespace_delete},
    "AWS::S3Tables::Table": {"create": _s3tables_table_create, "delete": _s3tables_table_delete},
    "AWS::SQS::Queue": {"create": _sqs_create, "delete": _sqs_delete},
    "AWS::SNS::Topic": {"create": _sns_create, "delete": _sns_delete},
    "AWS::SNS::Subscription": {"create": _sns_sub_create, "delete": _sns_sub_delete},
    "AWS::DynamoDB::Table": {"create": _ddb_create, "delete": _ddb_delete},
    # CDK TableV2 emits AWS::DynamoDB::GlobalTable, even for single-region
    # tables. The schema differs from Table (no ProvisionedThroughput; capacity
    # comes from WriteProvisionedThroughputSettings; Replicas is required and
    # ignored locally), so it gets a dedicated provisioner that translates
    # before delegating to the Table engine.
    "AWS::DynamoDB::GlobalTable": {"create": _ddb_global_table_create, "delete": _ddb_global_table_delete},
    "AWS::Lambda::Function": {"create": _lambda_create, "delete": _lambda_delete},
    "AWS::IAM::Role": {"create": _iam_role_create, "delete": _iam_role_delete},
    "AWS::IAM::Policy": {"create": _iam_policy_create, "delete": _iam_policy_delete},
    "AWS::IAM::InstanceProfile": {"create": _iam_ip_create, "delete": _iam_ip_delete},
    "AWS::SSM::Parameter": {"create": _ssm_create, "delete": _ssm_delete},
    "AWS::AppConfig::Application": {
        "create": _appconfig_application_create,
        "delete": _appconfig_application_delete,
    },
    "AWS::AppConfig::Environment": {
        "create": _appconfig_environment_create,
        "delete": _appconfig_environment_delete,
    },
    "AWS::AppConfig::ConfigurationProfile": {
        "create": _appconfig_configuration_profile_create,
        "delete": _appconfig_configuration_profile_delete,
    },
    "AWS::AppConfig::HostedConfigurationVersion": {
        "create": _appconfig_hosted_version_create,
        "delete": _appconfig_hosted_version_delete,
    },
    "AWS::AppConfig::DeploymentStrategy": {
        "create": _appconfig_deployment_strategy_create,
        "delete": _appconfig_deployment_strategy_delete,
    },
    "AWS::AppConfig::Deployment": {
        "create": _appconfig_deployment_create,
        "delete": _appconfig_deployment_delete,
    },
    "AWS::Logs::LogGroup": {"create": _cwlogs_create, "delete": _cwlogs_delete},
    "AWS::Logs::SubscriptionFilter": {"create": _cwlogs_subfilter_create, "delete": _cwlogs_subfilter_delete},
    "AWS::Events::Rule": {"create": _eb_rule_create, "delete": _eb_rule_delete},
    "AWS::Events::EventBus": {"create": _eb_event_bus_create, "delete": _eb_event_bus_delete},
    "AWS::Kinesis::Stream": {"create": _kinesis_stream_create, "delete": _kinesis_stream_delete},
    "AWS::Lambda::Permission": {"create": _lambda_permission_create, "delete": _lambda_permission_delete},
    "AWS::Lambda::Version": {"create": _lambda_version_create},
    "AWS::CloudFormation::WaitCondition": {"create": _cfn_wait_condition_create},
    "AWS::CloudFormation::WaitConditionHandle": {"create": _cfn_wait_condition_handle_create},
    "AWS::CloudFormation::Stack": {
        "create": _cfn_nested_stack_create,
        "update": _cfn_nested_stack_update,
        "delete": _cfn_nested_stack_delete,
    },
    # The "create" entry is load-bearing: without it, _provision_resource would
    # hit the generic "AWS::CloudFormation::*" no-op branch. Update and delete
    # are intentionally absent — they route through the explicit if-branches in
    # _update_resource/_delete_resource so stack_name and logical_id reach the handler.
    "AWS::CloudFormation::CustomResource": {"create": _custom_resource_create},
    "AWS::ApiGateway::RestApi": {"create": _apigw_rest_api_create, "delete": _apigw_rest_api_delete},
    "AWS::ApiGateway::Resource": {"create": _apigw_resource_create, "delete": _apigw_resource_delete},
    "AWS::ApiGateway::Method": {"create": _apigw_method_create, "delete": _apigw_method_delete},
    "AWS::ApiGateway::Model": {
        "create": _apigw_model_create,
        "update": _apigw_model_update,
        "delete": _apigw_model_delete,
    },
    "AWS::ApiGateway::Authorizer": {"create": _apigw_authorizer_create, "delete": _apigw_authorizer_delete},
    "AWS::ApiGateway::Deployment": {"create": _apigw_deployment_create, "delete": _apigw_deployment_delete},
    "AWS::ApiGateway::Stage": {"create": _apigw_stage_create, "delete": _apigw_stage_delete},
    "AWS::ApiGateway::BasePathMapping": {
        "create": _apigw_base_path_mapping_create,
        "update": _apigw_base_path_mapping_update,
        "delete": _apigw_base_path_mapping_delete,
    },
    "AWS::ApiGateway::Account": {"create": _apigw_account_create, "delete": _apigw_account_delete},
    "AWS::ApiGateway::DomainName": {
        "create": _apigw_domain_name_create,
        "update": _apigw_domain_name_update,
        "delete": _apigw_domain_name_delete,
    },
    "AWS::ApiGateway::GatewayResponse": {
        "create": _apigw_gateway_response_create,
        "update": _apigw_gateway_response_update,
        "delete": _apigw_gateway_response_delete,
    },
    "AWS::ApiGateway::DocumentationPart": {
        "create": _apigw_documentation_part_create,
        "update": _apigw_documentation_part_update,
        "delete": _apigw_documentation_part_delete,
    },
    "AWS::Lambda::EventSourceMapping": {"create": _lambda_esm_create, "update": _lambda_esm_update, "delete": _lambda_esm_delete},
    "AWS::Lambda::EventInvokeConfig": {
        "create": _lambda_event_invoke_config_create,
        "update": _lambda_event_invoke_config_update,
        "delete": _lambda_event_invoke_config_delete,
    },
    "AWS::Pipes::Pipe": {"create": _pipes_pipe_create, "delete": _pipes_pipe_delete},
    "AWS::Lambda::Alias": {"create": _lambda_alias_create, "delete": _lambda_alias_delete},
    "AWS::SQS::QueuePolicy": {"create": _sqs_queue_policy_create, "delete": _sqs_queue_policy_delete},
    "AWS::SNS::TopicPolicy": {"create": _sns_topic_policy_create, "delete": _sns_topic_policy_delete},
    "AWS::AppSync::GraphQLApi": {"create": _appsync_api_create, "delete": _appsync_api_delete},
    "AWS::AppSync::DataSource": {"create": _appsync_ds_create, "delete": _appsync_ds_delete},
    "AWS::AppSync::Resolver": {"create": _appsync_resolver_create, "delete": _appsync_resolver_delete},
    "AWS::AppSync::GraphQLSchema": {"create": _appsync_schema_create},
    "AWS::AppSync::ApiKey": {"create": _appsync_apikey_create, "delete": _appsync_apikey_delete},
    "AWS::SecretsManager::Secret": {"create": _sm_secret_create, "delete": _sm_secret_delete},
    "AWS::Cognito::UserPool": {"create": _cognito_user_pool_create, "delete": _cognito_user_pool_delete},
    "AWS::Cognito::UserPoolClient": {"create": _cognito_user_pool_client_create, "delete": _cognito_user_pool_client_delete},
    "AWS::Cognito::UserPoolResourceServer": {"create": _cognito_user_pool_resource_server_create, "delete": _cognito_user_pool_resource_server_delete},
    "AWS::Cognito::UserPoolGroup": {"create": _cognito_user_pool_group_create, "delete": _cognito_user_pool_group_delete},
    "AWS::Cognito::IdentityPool": {"create": _cognito_identity_pool_create, "delete": _cognito_identity_pool_delete},
    "AWS::Cognito::UserPoolDomain": {"create": _cognito_user_pool_domain_create, "delete": _cognito_user_pool_domain_delete},
    "AWS::ECR::Repository": {"create": _ecr_repo_create, "delete": _ecr_repo_delete},
    "AWS::CertificateManager::Certificate": {"create": _acm_certificate_create, "delete": _acm_certificate_delete},
    "AWS::ElasticLoadBalancingV2::TargetGroup": {"create": _elbv2_target_group_create, "delete": _elbv2_target_group_delete},
    "AWS::ElasticLoadBalancingV2::ListenerRule": {"create": _elbv2_listener_rule_create, "delete": _elbv2_listener_rule_delete},
    "AWS::CodeBuild::Project": {"create": _codebuild_project_create, "delete": _codebuild_project_delete},
    "AWS::IAM::ManagedPolicy": {"create": _iam_managed_policy_create, "delete": _iam_managed_policy_delete},
    "AWS::KMS::Key": {"create": _kms_key_create, "delete": _kms_key_delete},
    "AWS::KMS::Alias": {"create": _kms_alias_create, "delete": _kms_alias_delete},
    "AWS::EC2::VPC": {"create": _ec2_vpc_create, "delete": _ec2_vpc_delete},
    "AWS::EC2::Subnet": {"create": _ec2_subnet_create, "delete": _ec2_subnet_delete},
    "AWS::EC2::SecurityGroup": {"create": _ec2_sg_create, "delete": _ec2_sg_delete},
    "AWS::EC2::InternetGateway": {"create": _ec2_igw_create, "delete": _ec2_igw_delete},
    "AWS::EC2::VPCGatewayAttachment": {"create": _ec2_vpc_gw_attach_create, "delete": _ec2_vpc_gw_attach_delete},
    "AWS::EC2::RouteTable": {"create": _ec2_rtb_create, "delete": _ec2_rtb_delete},
    "AWS::EC2::Route": {"create": _ec2_route_create, "delete": _ec2_route_delete},
    "AWS::EC2::SubnetRouteTableAssociation": {"create": _ec2_subnet_rtb_assoc_create, "delete": _ec2_subnet_rtb_assoc_delete},
    "AWS::ECS::Cluster": {"create": _ecs_cluster_create, "delete": _ecs_cluster_delete},
    "AWS::ECS::TaskDefinition": {"create": _ecs_task_def_create, "delete": _ecs_task_def_delete},
    "AWS::ECS::Service": {"create": _ecs_service_create, "delete": _ecs_service_delete},
    "AWS::EC2::LaunchTemplate": {"create": _ec2_launch_template_create, "delete": _ec2_launch_template_delete},
    "AWS::ElasticLoadBalancingV2::LoadBalancer": {"create": _elbv2_load_balancer_create, "delete": _elbv2_load_balancer_delete,},
    "AWS::ElasticLoadBalancingV2::Listener": {"create": _elbv2_listener_create, "delete": _elbv2_listener_delete,},
    "AWS::Lambda::LayerVersion": {"create": _lambda_layer_create, "delete": _lambda_layer_delete},
    "AWS::StepFunctions::StateMachine": {"create": _sfn_state_machine_create, "delete": _sfn_state_machine_delete},
    "AWS::Route53::HostedZone": {"create": _r53_hosted_zone_create, "delete": _r53_hosted_zone_delete},
    "AWS::Route53::RecordSet": {"create": _r53_record_set_create, "delete": _r53_record_set_delete},
    "AWS::ApiGatewayV2::Api": {"create": _apigw_v2_api_create, "delete": _apigw_v2_api_delete},
    "AWS::ApiGatewayV2::Stage": {"create": _apigw_v2_stage_create, "delete": _apigw_v2_stage_delete},
    "AWS::ApiGatewayV2::Integration": {"create": _apigw_v2_integration_create, "delete": _apigw_v2_integration_delete},
    "AWS::ApiGatewayV2::Route": {"create": _apigw_v2_route_create, "delete": _apigw_v2_route_delete},
    "AWS::ApiGatewayV2::Authorizer": {"create": _apigw_v2_authorizer_create, "delete": _apigw_v2_authorizer_delete},
    "AWS::SES::EmailIdentity": {"create": _ses_email_identity_create, "delete": _ses_email_identity_delete},
    "AWS::WAFv2::WebACL": {"create": _waf_web_acl_create, "delete": _waf_web_acl_delete},
    "AWS::CloudFront::Distribution": {"create": _cf_distribution_create, "delete": _cf_distribution_delete},
    "AWS::CloudFront::KeyValueStore": {"create": _cf_kvs_create, "update": _cf_kvs_update, "delete": _cf_kvs_delete},
    "AWS::CloudWatch::Alarm": {"create": _cw_metric_alarm_create, "delete": _cw_metric_alarm_delete},
    "AWS::CloudWatch::Dashboard": {
        "create": _cw_dashboard_create,
        "update": _cw_dashboard_update,
        "delete": _cw_dashboard_delete,
    },
    "AWS::RDS::DBCluster": {"create": _rds_db_cluster_create, "delete": _rds_db_cluster_delete},
    "AWS::RDS::DBInstance": {"create": _rds_db_instance_create, "delete": _rds_db_instance_delete},
    "AWS::IoT::TopicRule": {"create": _iot_topic_rule_create, "delete": _iot_topic_rule_delete},
    # EventBridge Scheduler
    "AWS::Scheduler::Schedule": {"create": _scheduler_schedule_create, "delete": _scheduler_schedule_delete},
    "AWS::Scheduler::ScheduleGroup": {"create": _scheduler_group_create, "delete": _scheduler_group_delete},
    # EKS
    "AWS::EKS::Cluster": {"create": _eks_cluster_create, "delete": _eks_cluster_delete},
    "AWS::EKS::Nodegroup": {"create": _eks_nodegroup_create, "delete": _eks_nodegroup_delete},
    # AWS Backup
    "AWS::Backup::BackupVault": {"create": _backup_vault_create, "delete": _backup_vault_delete},
    "AWS::Backup::BackupPlan": {"create": _backup_plan_create, "delete": _backup_plan_delete},
    # CDK metadata — safe to ignore
    "AWS::CDK::Metadata": {"create": lambda lid, props, sn: (f"CDKMetadata-{lid}", {}), "delete": lambda pid, props: None},
    # AutoScaling
    "AWS::AutoScaling::AutoScalingGroup": {"create": _asg_create, "delete": _asg_delete},
    "AWS::AutoScaling::LaunchConfiguration": {"create": _asg_lc_create, "delete": _asg_lc_delete},
    "AWS::AutoScaling::ScalingPolicy": {"create": _asg_policy_create, "delete": _asg_policy_delete},
    "AWS::AutoScaling::LifecycleHook": {"create": _asg_hook_create, "delete": _asg_hook_delete},
    "AWS::AutoScaling::ScheduledAction": {"create": _asg_scheduled_create, "delete": _asg_scheduled_delete},
}
