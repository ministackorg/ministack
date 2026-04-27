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

from ministack.core.responses import get_account_id, get_region, new_uuid, now_iso

import ministack.services.s3 as _s3
import ministack.services.sqs as _sqs
import ministack.services.sns as _sns
import ministack.services.dynamodb as _dynamodb
import ministack.services.lambda_svc as _lambda_svc
import ministack.services.ssm as _ssm
import ministack.services.cloudwatch as _cw
import ministack.services.cloudwatch_logs as _cw_logs
import ministack.services.eventbridge as _eb
import ministack.services.iam as _iam
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
import ministack.services.pipes as _pipes
import ministack.services.stepfunctions as _sfn
import ministack.services.route53 as _r53
import ministack.services.apigateway as _apigw_v2
import ministack.services.ses as _ses
import ministack.services.waf as _waf
import ministack.services.cloudfront as _cf
import ministack.services.rds as _rds
import ministack.services.autoscaling as _asg
import ministack.services.codebuild as _codebuild
import ministack.services.backup as _backup


logger = logging.getLogger("cloudformation")

# Module-level REGION kept for legacy imports; new code must use get_region()
# so AWS::Region / ARNs reflect the caller's request region (#398).
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
        "region": get_region(),
    }
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
        "code_zip": code_zip,
        "code_s3_bucket": code.get("S3Bucket"),
        "code_s3_key": code.get("S3Key"),
        "code_s3_object_version": code.get("S3ObjectVersion"),
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
    # ARN: no extra slash if name starts with /
    param_arn = f"arn:aws:ssm:{get_region()}:{get_account_id()}:parameter{name}"

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
    func_arn = func["config"]["FunctionArn"] if func else f"arn:aws:lambda:{get_region()}:{get_account_id()}:function:{func_name}"

    esm = {
        "UUID": esm_id,
        "EventSourceArn": props.get("EventSourceArn", ""),
        "FunctionArn": func_arn,
        "FunctionName": func_name,
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
    _lambda_svc._esms[esm_id] = esm
    _lambda_svc._ensure_poller()
    return esm_id, {"UUID": esm_id}


def _lambda_esm_delete(physical_id, props):
    _lambda_svc._esms.pop(physical_id, None)


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
    func_name = props.get("FunctionName", "")
    if func_name.startswith("arn:"):
        func_name = func_name.rsplit(":", 1)[-1]
    alias_name = props.get("Name", "")
    func_version = props.get("FunctionVersion", "$LATEST")

    func = _lambda_svc._functions.get(func_name)
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
    arn = f"arn:aws:ec2:{get_region()}:{get_account_id()}:vpc/{vpc_id}"
    return vpc_id, {"VpcId": vpc_id, "DefaultSecurityGroup": sg_id, "DefaultNetworkAcl": acl_id}


def _ec2_vpc_delete(physical_id, props):
    _ec2._vpcs.pop(physical_id, None)


def _ec2_subnet_create(logical_id, props, stack_name):
    import random, string
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
    td = {
        "taskDefinitionArn": arn,
        "family": family,
        "revision": revision,
        "status": "ACTIVE",
        "containerDefinitions": _normalize_container_defs(props.get("ContainerDefinitions", [])),
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
    lb_id = lb_arn.split("/")[-1]
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

    import base64, hashlib
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
            "CodeSha256": (base64.b64encode(hashlib.sha256(zip_data).digest()).decode() if zip_data else ""),
            "CodeSize": len(zip_data) if zip_data else 0,
        },
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
    definition = props.get("DefinitionString", "{}")
    if isinstance(definition, dict):
        import json as _json
        definition = _json.dumps(definition)
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
# ApiGatewayV2 Api
# ---------------------------------------------------------------------------

def _apigw_v2_api_create(logical_id, props, stack_name):
    api_id = new_uuid()[:8]
    name = props.get("Name") or _physical_name(stack_name, logical_id, max_len=128)
    protocol = props.get("ProtocolType", "HTTP")
    api = {
        "apiId": api_id,
        "name": name,
        "protocolType": protocol,
        "apiEndpoint": f"http://{api_id}.execute-api.{os.environ.get('MINISTACK_HOST', 'localhost')}:{os.environ.get('GATEWAY_PORT', '4566')}",
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
    _apigw_v2._routes[api_id] = {}
    _apigw_v2._integrations[api_id] = {}
    _apigw_v2._stages[api_id] = {}
    _apigw_v2._deployments[api_id] = {}
    return api_id, {"ApiId": api_id, "ApiEndpoint": api["apiEndpoint"]}


def _apigw_v2_api_delete(physical_id, props):
    _apigw_v2._apis.pop(physical_id, None)
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
    return dist_id, {"Arn": arn, "DomainName": f"{dist_id}.cloudfront.net", "Id": dist_id}


def _cf_distribution_delete(physical_id, props):
    _cf._distributions.pop(physical_id, None)


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
    "AWS::Events::EventBus": {"create": _eb_event_bus_create, "delete": _eb_event_bus_delete},
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
    "AWS::Cognito::IdentityPool": {"create": _cognito_identity_pool_create, "delete": _cognito_identity_pool_delete},
    "AWS::Cognito::UserPoolDomain": {"create": _cognito_user_pool_domain_create, "delete": _cognito_user_pool_domain_delete},
    "AWS::ECR::Repository": {"create": _ecr_repo_create, "delete": _ecr_repo_delete},
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
    "AWS::SES::EmailIdentity": {"create": _ses_email_identity_create, "delete": _ses_email_identity_delete},
    "AWS::WAFv2::WebACL": {"create": _waf_web_acl_create, "delete": _waf_web_acl_delete},
    "AWS::CloudFront::Distribution": {"create": _cf_distribution_create, "delete": _cf_distribution_delete},
    "AWS::CloudWatch::Alarm": {"create": _cw_metric_alarm_create, "delete": _cw_metric_alarm_delete},
    "AWS::RDS::DBCluster": {"create": _rds_db_cluster_create, "delete": _rds_db_cluster_delete},
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
