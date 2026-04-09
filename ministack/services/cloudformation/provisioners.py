"""
CloudFormation provisioners — resource create/delete handlers for each AWS resource type.
"""

import io
import os
import json
import logging
import random
import string
import time
import zipfile
from collections import defaultdict

from ministack.core.responses import get_account_id, new_uuid, now_iso

import ministack.services.s3 as _s3
import ministack.services.sqs as _sqs
import ministack.services.sns as _sns
import ministack.services.dynamodb as _dynamodb
import ministack.services.lambda_svc as _lambda_svc
import ministack.services.ssm as _ssm
import ministack.services.cloudwatch_logs as _cw_logs
import ministack.services.eventbridge as _eb
import ministack.services.iam_sts as _iam_sts
import ministack.services.apigateway_v1 as _apigw_v1
import ministack.services.appsync as _appsync
import ministack.services.secretsmanager as _sm
import ministack.services.cognito as _cognito
import ministack.services.ecr as _ecr
import ministack.services.kms as _kms
import ministack.services.ec2 as _ec2
import ministack.services.ecs as _ecs
import ministack.services.alb as _alb
import ministack.services.kinesis as _kinesis


logger = logging.getLogger("cloudformation")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")


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
    # CloudFormation internal types are no-ops
    if resource_type.startswith("AWS::CloudFormation::"):
        logger.info("CloudFormation internal type %s for %s -- noop", resource_type, logical_id)
        noop_id = f"{stack_name}-{logical_id}-noop-{new_uuid()[:8]}"
        return noop_id, {}
    raise ValueError(f"Unsupported resource type: {resource_type}")


def _delete_resource(resource_type: str, physical_id: str, props: dict):
    """Delete a provisioned resource."""
    handler = _RESOURCE_HANDLERS.get(resource_type)
    if handler and "delete" in handler:
        handler["delete"](physical_id, props)
        return
    logger.warning("No delete handler for resource type %s (id=%s)",
                   resource_type, physical_id)


# ===========================================================================
# Resource Provisioners
# ===========================================================================

# --- S3 Bucket ---

def _s3_create(logical_id, props, stack_name):
    name = props.get("BucketName") or _physical_name(stack_name, logical_id, lowercase=True, max_len=63)
    _s3._buckets[name] = {
        "created": now_iso(),
        "objects": {},
        "region": REGION,
    }
    versioning = props.get("VersioningConfiguration", {})
    if versioning.get("Status") == "Enabled":
        _s3._bucket_versioning[name] = "Enabled"
    attrs = {
        "Arn": f"arn:aws:s3:::{name}",
        "DomainName": f"{name}.s3.amazonaws.com",
        "RegionalDomainName": f"{name}.s3.{REGION}.amazonaws.com",
        "WebsiteURL": f"http://{name}.s3-website-{REGION}.amazonaws.com",
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
    arn = f"arn:aws:sqs:{REGION}:{get_account_id()}:{name}"
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
    arn = f"arn:aws:sns:{REGION}:{get_account_id()}:{name}"
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
            "attributes": {},
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
    sub = {
        "arn": sub_arn,
        "topic_arn": topic_arn,
        "protocol": protocol,
        "endpoint": endpoint,
        "confirmed": protocol not in ("http", "https"),
        "owner": get_account_id(),
        "attributes": {},
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
    arn = f"arn:aws:dynamodb:{REGION}:{get_account_id()}:table/{name}"

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
    stream_enabled = stream_spec.get("StreamEnabled", False)
    stream_arn = f"{arn}/stream/{now_iso()}" if stream_enabled else None

    billing = props.get("BillingMode", "PROVISIONED")

    table = {
        "TableName": name,
        "TableArn": arn,
        "TableId": new_uuid(),
        "TableStatus": "ACTIVE",
        "CreationDateTime": time.time(),
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


# --- Lambda Function ---

def _zip_inline(source: str | None, handler: str) -> bytes | None:
    """Wrap inline ZipFile source code into a real zip archive."""
    if not source:
        return None
    module = handler.split(".")[0] if handler and "." in handler else "index"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{module}.py", source)
    return buf.getvalue()


def _lambda_create(logical_id, props, stack_name):
    name = props.get("FunctionName") or _physical_name(stack_name, logical_id, max_len=64)
    arn = f"arn:aws:lambda:{REGION}:{get_account_id()}:function:{name}"
    runtime = props.get("Runtime", "python3.9")
    handler = props.get("Handler", "index.handler")
    role = props.get("Role", f"arn:aws:iam::{get_account_id()}:role/dummy-role")
    timeout = int(props.get("Timeout", 3))
    memory = int(props.get("MemorySize", 128))
    env_vars = props.get("Environment", {}).get("Variables", {})
    description = props.get("Description", "")
    layers = props.get("Layers", [])
    code = props.get("Code", {})

    func = {
        "config": {
            "FunctionName": name,
            "FunctionArn": arn,
            "Runtime": runtime,
            "Role": role,
            "Handler": handler,
            "Description": description,
            "Timeout": timeout,
            "MemorySize": memory,
            "LastModified": now_iso(),
            "CodeSha256": "cfn-deployed",
            "Version": "$LATEST",
            "Environment": {"Variables": env_vars},
            "Layers": [{"Arn": l} if isinstance(l, str) else l for l in layers],
            "State": "Active",
            "LastUpdateStatus": "Successful",
            "PackageType": "Zip",
            "Architectures": props.get("Architectures", ["x86_64"]),
            "EphemeralStorage": {"Size": props.get("EphemeralStorage", {}).get("Size", 512)},
            "TracingConfig": props.get("TracingConfig", {"Mode": "PassThrough"}),
            "RevisionId": new_uuid(),
        },
        "code_zip": _zip_inline(code.get("ZipFile"), handler),
        "code_s3_bucket": code.get("S3Bucket"),
        "code_s3_key": code.get("S3Key"),
        "versions": {},
        "next_version": 1,
        "tags": {},
        "policy": {"Version": "2012-10-17", "Id": "default", "Statement": []},
        "aliases": {},
        "concurrency": None,
        "provisioned_concurrency": {},
    }
    _lambda_svc._functions[name] = func
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

    _iam_sts._roles[name] = role
    return name, {"Arn": arn, "RoleId": role_id}


def _iam_role_delete(physical_id, props):
    _iam_sts._roles.pop(physical_id, None)


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
    _iam_sts._policies[arn] = policy

    # Attach to roles if Roles property specified
    roles = props.get("Roles", [])
    for role_name in roles:
        role = _iam_sts._roles.get(role_name)
        if role:
            role["AttachedPolicies"].append({
                "PolicyName": name,
                "PolicyArn": arn,
            })
            policy["AttachmentCount"] += 1

    return arn, {"PolicyArn": arn}


def _iam_policy_delete(physical_id, props):
    _iam_sts._policies.pop(physical_id, None)


# --- IAM InstanceProfile ---

def _iam_ip_create(logical_id, props, stack_name):
    name = props.get("InstanceProfileName") or _physical_name(stack_name, logical_id, max_len=128)
    path = props.get("Path", "/")
    arn = f"arn:aws:iam::{get_account_id()}:instance-profile{path}{name}"
    ip_id = new_uuid().replace("-", "")[:21].upper()

    roles = []
    for rname in props.get("Roles", []):
        role = _iam_sts._roles.get(rname)
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
    _iam_sts._instance_profiles[name] = profile
    return arn, {"Arn": arn}


def _iam_ip_delete(physical_id, props):
    # physical_id is the ARN -- find the name
    for name, ip in list(_iam_sts._instance_profiles.items()):
        if ip.get("Arn") == physical_id:
            _iam_sts._instance_profiles.pop(name, None)
            return


# --- SSM Parameter ---

def _ssm_create(logical_id, props, stack_name):
    name = props.get("Name") or f"/{stack_name}/{logical_id}"
    ptype = props.get("Type", "String")
    value = props.get("Value", "")
    description = props.get("Description", "")
    # ARN: no extra slash if name starts with /
    param_arn = f"arn:aws:ssm:{REGION}:{get_account_id()}:parameter{name}"

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


# --- CloudWatch Logs LogGroup ---

def _cwlogs_create(logical_id, props, stack_name):
    name = props.get("LogGroupName") or f"/aws/cloudformation/{stack_name}/{logical_id}"
    arn = f"arn:aws:logs:{REGION}:{get_account_id()}:log-group:{name}:*"
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


# --- EventBridge Rule ---

def _eb_rule_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=64)
    bus = props.get("EventBusName", "default")
    key = f"{name}|{bus}"
    arn = f"arn:aws:events:{REGION}:{get_account_id()}:rule/{bus}/{name}"

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
        _eb._targets[key].append({
            "Id": t.get("Id", ""),
            "Arn": t.get("Arn", ""),
            "RoleArn": t.get("RoleArn", ""),
            "Input": t.get("Input", ""),
            "InputPath": t.get("InputPath", ""),
        })

    return name, {"Arn": arn}


def _eb_rule_delete(physical_id, props):
    bus = props.get("EventBusName", "default")
    key = f"{physical_id}|{bus}"
    _eb._rules.pop(key, None)
    _eb._targets.pop(key, None)


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

    arn = f"arn:aws:kinesis:{REGION}:{get_account_id()}:stream/{name}"
    stream_id = new_uuid()

    _kinesis._streams[name] = {
        "StreamName": name,
        "StreamARN": arn,
        "StreamStatus": "ACTIVE",
        "StreamModeDetails": {"StreamMode": stream_mode},
        "RetentionPeriodHours": retention,
        "shards": _kinesis._build_shards(shard_count),
        "tags": {},
        "CreationTimestamp": time.time(),
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

def _lambda_permission_create(logical_id, props, stack_name):
    func_name = props.get("FunctionName", "")
    # Resolve ARN to function name
    if func_name.startswith("arn:"):
        func_name = func_name.rsplit(":", 1)[-1]
    func = _lambda_svc._functions.get(func_name)
    if func:
        stmt = {
            "Sid": props.get("Id") or logical_id,
            "Effect": "Allow",
            "Principal": props.get("Principal", "*"),
            "Action": props.get("Action", "lambda:InvokeFunction"),
            "Resource": func["config"]["FunctionArn"],
        }
        source_arn = props.get("SourceArn")
        if source_arn:
            stmt["Condition"] = {"ArnLike": {"AWS:SourceArn": source_arn}}
        func["policy"]["Statement"].append(stmt)
    pid = f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    return pid, {}


def _lambda_permission_delete(physical_id, props):
    func_name = props.get("FunctionName", "")
    if func_name.startswith("arn:"):
        func_name = func_name.rsplit(":", 1)[-1]
    func = _lambda_svc._functions.get(func_name)
    if func:
        sid = props.get("Id") or ""
        func["policy"]["Statement"] = [
            s for s in func["policy"]["Statement"] if s.get("Sid") != sid
        ]


# --- Lambda Version ---

def _lambda_version_create(logical_id, props, stack_name):
    func_name = props.get("FunctionName", "")
    if func_name.startswith("arn:"):
        func_name = func_name.rsplit(":", 1)[-1]
    func = _lambda_svc._functions.get(func_name)
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
    ver_arn = f"arn:aws:lambda:{REGION}:{get_account_id()}:function:{func_name}:1"
    return ver_arn, {"Version": "1"}


# --- CloudFormation WaitCondition / WaitConditionHandle (no-ops) ---

def _cfn_wait_condition_create(logical_id, props, stack_name):
    """WaitCondition — no-op, return immediately (no real signalling in local emulation)."""
    pid = f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    return pid, {"Data": "{}"}


def _cfn_wait_condition_handle_create(logical_id, props, stack_name):
    """WaitConditionHandle — no-op, return a presigned-style URL."""
    pid = f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    url = f"https://cloudformation-waitcondition-{REGION}.s3.amazonaws.com/{pid}"
    return pid, {"Ref": url}


# --- API Gateway REST API ---

def _apigw_rest_api_create(logical_id, props, stack_name):
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=64)
    data = {
        "name": name,
        "description": props.get("Description", ""),
        "endpointConfiguration": props.get("EndpointConfiguration", {"types": ["REGIONAL"]}),
        "binaryMediaTypes": props.get("BinaryMediaTypes", []),
        "minimumCompressionSize": props.get("MinimumCompressionSize"),
        "policy": props.get("Policy"),
        "tags": {t["Key"]: t["Value"] for t in props.get("Tags", [])},
    }
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
        "Arn": f"arn:aws:apigateway:{REGION}::/restapis/{api_id}",
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
    pid = f"{api_id}-{stage_name}"
    return pid, {"StageName": stage_name}


def _apigw_stage_delete(physical_id, props):
    api_id = props.get("RestApiId", "")
    stage_name = props.get("StageName", "")
    _apigw_v1._delete_stage(api_id, stage_name)


# --- Lambda EventSourceMapping ---

def _lambda_esm_create(logical_id, props, stack_name):
    func_name = props.get("FunctionName", "")
    if func_name.startswith("arn:"):
        func_name = func_name.rsplit(":", 1)[-1]
    esm_id = new_uuid()
    func = _lambda_svc._functions.get(func_name)
    func_arn = func["config"]["FunctionArn"] if func else f"arn:aws:lambda:{REGION}:{get_account_id()}:function:{func_name}"

    esm = {
        "UUID": esm_id,
        "EventSourceArn": props.get("EventSourceArn", ""),
        "FunctionArn": func_arn,
        "FunctionName": func_name,
        "State": "Enabled",
        "StateTransitionReason": "USER_INITIATED",
        "BatchSize": int(props.get("BatchSize", 10)),
        "MaximumBatchingWindowInSeconds": int(props.get("MaximumBatchingWindowInSeconds", 0)),
        "LastModified": time.time(),
        "LastProcessingResult": "No records processed",
        "StartingPosition": props.get("StartingPosition", "LATEST"),
        "Enabled": props.get("Enabled", True),
        "FunctionResponseTypes": props.get("FunctionResponseTypes", []),
    }
    _lambda_svc._esms[esm_id] = esm
    return esm_id, {"UUID": esm_id}


def _lambda_esm_delete(physical_id, props):
    _lambda_svc._esms.pop(physical_id, None)


# --- Lambda Alias ---

def _lambda_alias_create(logical_id, props, stack_name):
    func_name = props.get("FunctionName", "")
    if func_name.startswith("arn:"):
        func_name = func_name.rsplit(":", 1)[-1]
    alias_name = props.get("Name", "")
    func_version = props.get("FunctionVersion", "$LATEST")

    func = _lambda_svc._functions.get(func_name)
    if func:
        alias = {
            "AliasArn": f"arn:aws:lambda:{REGION}:{get_account_id()}:function:{func_name}:{alias_name}",
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

    alias_arn = f"arn:aws:lambda:{REGION}:{get_account_id()}:function:{func_name}:{alias_name}"
    return alias_arn, {"AliasArn": alias_arn}


def _lambda_alias_delete(physical_id, props):
    func_name = props.get("FunctionName", "")
    if func_name.startswith("arn:"):
        func_name = func_name.rsplit(":", 1)[-1]
    alias_name = props.get("Name", "")
    func = _lambda_svc._functions.get(func_name)
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
    arn = f"arn:aws:appsync:{REGION}:{get_account_id()}:apis/{api_id}"
    now = _time.time()
    _appsync._apis[api_id] = {
        "apiId": api_id, "name": name, "authenticationType": auth_type,
        "arn": arn,
        "uris": {"GRAPHQL": f"https://{api_id}.appsync-api.{REGION}.amazonaws.com/graphql"},
        "createdAt": now, "lastUpdatedAt": now,
        "additionalAuthenticationProviders": props.get("AdditionalAuthenticationProviders", []),
        "xrayEnabled": False,
    }
    _appsync._api_keys[api_id] = {}
    _appsync._data_sources[api_id] = {}
    _appsync._resolvers[api_id] = {}
    _appsync._types[api_id] = {}
    return api_id, {"ApiId": api_id, "Arn": arn, "GraphQLUrl": f"https://{api_id}.appsync-api.{REGION}.amazonaws.com/graphql"}


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
        "dataSourceArn": f"arn:aws:appsync:{REGION}:{get_account_id()}:apis/{api_id}/datasources/{name}",
    }
    return f"{api_id}/{name}", {"Name": name, "DataSourceArn": f"arn:aws:appsync:{REGION}:{get_account_id()}:apis/{api_id}/datasources/{name}"}


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
        "resolverArn": f"arn:aws:appsync:{REGION}:{get_account_id()}:apis/{api_id}/types/{type_name}/resolvers/{field_name}",
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
    return key_id, {"ApiKey": key_id, "Arn": f"arn:aws:appsync:{REGION}:{get_account_id()}:apis/{api_id}/apikeys/{key_id}"}


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

    arn = f"arn:aws:secretsmanager:{REGION}:{get_account_id()}:secret:{name}-{new_uuid()[:6]}"
    import time as _time
    _sm._secrets[name] = {
        "ARN": arn, "Name": name, "Description": props.get("Description", ""),
        "Tags": props.get("Tags", []),
        "CreatedDate": _time.time(), "LastChangedDate": _time.time(),
        "LastAccessedDate": None, "DeletedDate": None,
        "RotationEnabled": False, "RotationLambdaARN": None,
        "RotationRules": None, "ReplicationStatus": [],
        "KmsKeyId": props.get("KmsKeyId"),
        "Versions": {
            new_uuid(): {
                "SecretString": secret_string,
                "SecretBinary": None,
                "CreatedDate": _time.time(),
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
    }
    _cognito._user_pools[pid] = pool
    arn = _cognito._pool_arn(pid)
    provider_name = f"cognito-idp.{REGION}.amazonaws.com/{pid}"
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
        "ClientSecret": None,
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
    arn = f"arn:aws:ecr:{REGION}:{get_account_id()}:repository/{name}"
    _ecr._repositories[name] = {
        "repositoryName": name,
        "repositoryArn": arn,
        "registryId": get_account_id(),
        "repositoryUri": f"{get_account_id()}.dkr.ecr.{REGION}.amazonaws.com/{name}",
        "createdAt": __import__("time").time(),
        "imageTagMutability": props.get("ImageTagMutability", "MUTABLE"),
        "imageScanningConfiguration": props.get("ImageScanningConfiguration", {"scanOnPush": False}),
        "encryptionConfiguration": props.get("EncryptionConfiguration", {"encryptionType": "AES256"}),
        "images": [],
    }
    return name, {"Arn": arn, "RepositoryUri": _ecr._repositories[name]["repositoryUri"]}


def _ecr_repo_delete(physical_id, props):
    _ecr._repositories.pop(physical_id, None)


# --- IAM ManagedPolicy provisioner ---

def _iam_managed_policy_create(logical_id, props, stack_name):
    name = props.get("ManagedPolicyName", f"{stack_name}-{logical_id}")
    arn = f"arn:aws:iam::{get_account_id()}:policy/{name}"
    policy_doc = props.get("PolicyDocument", {})
    _iam_sts._policies[arn] = {
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
    _iam_sts._policies.pop(physical_id, None)


# --- KMS resource provisioners ---

def _kms_key_create(logical_id, props, stack_name):
    key_id = new_uuid()
    arn = f"arn:aws:kms:{REGION}:{get_account_id()}:key/{key_id}"
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
    _kms._aliases[alias_name] = target_key
    return alias_name, {}


def _kms_alias_delete(physical_id, props):
    _kms._aliases.pop(physical_id, None)


# --- EC2 resource provisioners ---

def _ec2_vpc_create(logical_id, props, stack_name):
    import random, string
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
    arn = f"arn:aws:ec2:{REGION}:{get_account_id()}:vpc/{vpc_id}"
    return vpc_id, {"VpcId": vpc_id, "DefaultSecurityGroup": sg_id, "DefaultNetworkAcl": acl_id}


def _ec2_vpc_delete(physical_id, props):
    _ec2._vpcs.pop(physical_id, None)


def _ec2_subnet_create(logical_id, props, stack_name):
    import random, string
    vpc_id = props.get("VpcId", "")
    cidr = props.get("CidrBlock", "10.0.1.0/24")
    az = props.get("AvailabilityZone", f"{REGION}a")
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

    arn = f"arn:aws:ec2:{REGION}:{get_account_id()}:security-group/{sg_id}"
    return sg_id, {"GroupId": sg_id, "VpcId": vpc_id, "Arn": arn}


def _ec2_sg_delete(physical_id, props):
    _ec2._security_groups.pop(physical_id, None)


def _ec2_igw_create(logical_id, props, stack_name):
    import random, string
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
    import random, string
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
    import random, string
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
    arn = f"arn:aws:ecs:{REGION}:{get_account_id()}:cluster/{name}"
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


def _ecs_task_def_create(logical_id, props, stack_name):
    family = props.get("Family", f"{stack_name}-{logical_id}")
    revision = 1
    td_key = f"{family}:{revision}"
    arn = f"arn:aws:ecs:{REGION}:{get_account_id()}:task-definition/{td_key}"
    td = {
        "taskDefinitionArn": arn,
        "family": family,
        "revision": revision,
        "status": "ACTIVE",
        "containerDefinitions": props.get("ContainerDefinitions", []),
        "requiresCompatibilities": props.get("RequiresCompatibilities", ["EC2"]),
        "networkMode": props.get("NetworkMode", "bridge"),
        "cpu": props.get("Cpu", "256"),
        "memory": props.get("Memory", "512"),
        "executionRoleArn": props.get("ExecutionRoleArn", ""),
        "taskRoleArn": props.get("TaskRoleArn", ""),
        "volumes": props.get("Volumes", []),
        "placementConstraints": props.get("PlacementConstraints", []),
    }
    _ecs._task_defs[td_key] = td
    _ecs._task_def_latest[family] = td_key
    return arn, {"TaskDefinitionArn": arn}


def _ecs_task_def_delete(physical_id, props):
    _ecs._task_defs.pop(physical_id, None)


def _ecs_service_create(logical_id, props, stack_name):
    name = props.get("ServiceName", f"{stack_name}-{logical_id}")
    cluster = props.get("Cluster", "default")
    arn = f"arn:aws:ecs:{REGION}:{get_account_id()}:service/{cluster}/{name}"
    svc = {
        "serviceArn": arn,
        "serviceName": name,
        "clusterArn": f"arn:aws:ecs:{REGION}:{get_account_id()}:cluster/{cluster}",
        "taskDefinition": props.get("TaskDefinition", ""),
        "desiredCount": props.get("DesiredCount", 1),
        "runningCount": 0,
        "status": "ACTIVE",
        "launchType": props.get("LaunchType", "EC2"),
        "deployments": [{
            "id": f"ecs-svc/{new_uuid().replace('-','')[:32]}",
            "status": "PRIMARY",
            "taskDefinition": props.get("TaskDefinition", ""),
            "desiredCount": props.get("DesiredCount", 1),
            "runningCount": 0,
            "createdAt": __import__("time").time(),
            "updatedAt": __import__("time").time(),
        }],
        "loadBalancers": props.get("LoadBalancers", []),
        "networkConfiguration": props.get("NetworkConfiguration", {}),
        "tags": [{"key": t["Key"], "value": t["Value"]} for t in props.get("Tags", [])],
    }
    _ecs._services[arn] = svc
    return arn, {"ServiceArn": arn, "Name": name}


def _ecs_service_delete(physical_id, props):
    _ecs._services.pop(physical_id, None)


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
        f"arn:aws:elasticloadbalancing:{REGION}:{get_account_id()}:"
        f"loadbalancer/app/{name}/{lb_id}"
    )
    dns_name = f"{name}-{lb_id[:8]}.{REGION}.elb.amazonaws.com"
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
    lb_id = lb_arn.split("/")[-1]
    listener_arn = (
        f"arn:aws:elasticloadbalancing:{REGION}:{get_account_id()}:"
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
        f"arn:aws:elasticloadbalancing:{REGION}:{get_account_id()}:"
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


# Resource Handler Registry
# ===========================================================================

_RESOURCE_HANDLERS = {
    "AWS::S3::Bucket": {"create": _s3_create, "delete": _s3_delete},
    "AWS::S3::BucketPolicy": {"create": _s3_bucket_policy_create, "delete": _s3_bucket_policy_delete},
    "AWS::SQS::Queue": {"create": _sqs_create, "delete": _sqs_delete},
    "AWS::SNS::Topic": {"create": _sns_create, "delete": _sns_delete},
    "AWS::SNS::Subscription": {"create": _sns_sub_create, "delete": _sns_sub_delete},
    "AWS::DynamoDB::Table": {"create": _ddb_create, "delete": _ddb_delete},
    "AWS::Lambda::Function": {"create": _lambda_create, "delete": _lambda_delete},
    "AWS::IAM::Role": {"create": _iam_role_create, "delete": _iam_role_delete},
    "AWS::IAM::Policy": {"create": _iam_policy_create, "delete": _iam_policy_delete},
    "AWS::IAM::InstanceProfile": {"create": _iam_ip_create, "delete": _iam_ip_delete},
    "AWS::SSM::Parameter": {"create": _ssm_create, "delete": _ssm_delete},
    "AWS::Logs::LogGroup": {"create": _cwlogs_create, "delete": _cwlogs_delete},
    "AWS::Events::Rule": {"create": _eb_rule_create, "delete": _eb_rule_delete},
    "AWS::Kinesis::Stream": {"create": _kinesis_stream_create, "delete": _kinesis_stream_delete},
    "AWS::Lambda::Permission": {"create": _lambda_permission_create, "delete": _lambda_permission_delete},
    "AWS::Lambda::Version": {"create": _lambda_version_create},
    "AWS::CloudFormation::WaitCondition": {"create": _cfn_wait_condition_create},
    "AWS::CloudFormation::WaitConditionHandle": {"create": _cfn_wait_condition_handle_create},
    "AWS::ApiGateway::RestApi": {"create": _apigw_rest_api_create, "delete": _apigw_rest_api_delete},
    "AWS::ApiGateway::Resource": {"create": _apigw_resource_create, "delete": _apigw_resource_delete},
    "AWS::ApiGateway::Method": {"create": _apigw_method_create, "delete": _apigw_method_delete},
    "AWS::ApiGateway::Deployment": {"create": _apigw_deployment_create, "delete": _apigw_deployment_delete},
    "AWS::ApiGateway::Stage": {"create": _apigw_stage_create, "delete": _apigw_stage_delete},
    "AWS::Lambda::EventSourceMapping": {"create": _lambda_esm_create, "delete": _lambda_esm_delete},
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
    "AWS::Cognito::IdentityPool": {"create": _cognito_identity_pool_create, "delete": _cognito_identity_pool_delete},
    "AWS::Cognito::UserPoolDomain": {"create": _cognito_user_pool_domain_create, "delete": _cognito_user_pool_domain_delete},
    "AWS::ECR::Repository": {"create": _ecr_repo_create, "delete": _ecr_repo_delete},
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
    # CDK metadata — safe to ignore
    "AWS::CDK::Metadata": {"create": lambda lid, props, sn: (f"CDKMetadata-{lid}", {}), "delete": lambda pid, props: None},
    # AutoScaling stubs — allow CDK/CFN stacks with ASGs to deploy
    "AWS::AutoScaling::AutoScalingGroup": {"create": lambda lid, props, sn: (f"asg-{lid}", {}), "delete": lambda pid, props: None},
    "AWS::AutoScaling::LaunchConfiguration": {"create": lambda lid, props, sn: (f"lc-{lid}", {}), "delete": lambda pid, props: None},
    "AWS::AutoScaling::ScalingPolicy": {"create": lambda lid, props, sn: (f"sp-{lid}", {}), "delete": lambda pid, props: None},
    "AWS::AutoScaling::LifecycleHook": {"create": lambda lid, props, sn: (f"lh-{lid}", {}), "delete": lambda pid, props: None},
    "AWS::AutoScaling::ScheduledAction": {"create": lambda lid, props, sn: (f"sa-{lid}", {}), "delete": lambda pid, props: None},
}
