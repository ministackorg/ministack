"""
CloudFormation provisioners — resource create/delete handlers for each AWS resource type.
"""

import io
import json
import logging
import time
import zipfile
from collections import defaultdict

from ministack.core.responses import new_uuid, now_iso

import ministack.services.s3 as _s3
import ministack.services.sqs as _sqs
import ministack.services.sns as _sns
import ministack.services.dynamodb as _dynamodb
import ministack.services.lambda_svc as _lambda_svc
import ministack.services.ssm as _ssm
import ministack.services.cloudwatch_logs as _cw_logs
import ministack.services.eventbridge as _eb
import ministack.services.iam_sts as _iam_sts


logger = logging.getLogger("cloudformation")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"


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
    name = props.get("BucketName") or f"{stack_name}-{logical_id.lower()}-{new_uuid()[:8]}"
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
    name = props.get("QueueName") or f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    is_fifo = name.endswith(".fifo")
    url = f"http://{_sqs.DEFAULT_HOST}:{_sqs.DEFAULT_PORT}/{ACCOUNT_ID}/{name}"
    arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{name}"
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
    name = props.get("TopicName") or f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    arn = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:{name}"
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
            "Owner": ACCOUNT_ID,
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
            "owner": ACCOUNT_ID,
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
        "owner": ACCOUNT_ID,
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
    name = props.get("TableName") or f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    arn = f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/{name}"

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
    name = props.get("FunctionName") or f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"
    runtime = props.get("Runtime", "python3.9")
    handler = props.get("Handler", "index.handler")
    role = props.get("Role", f"arn:aws:iam::{ACCOUNT_ID}:role/dummy-role")
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
    name = props.get("RoleName") or f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{name}"
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
    name = props.get("PolicyName") or f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    path = props.get("Path", "/")
    arn = f"arn:aws:iam::{ACCOUNT_ID}:policy{path}{name}"
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
    name = props.get("InstanceProfileName") or f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    path = props.get("Path", "/")
    arn = f"arn:aws:iam::{ACCOUNT_ID}:instance-profile{path}{name}"
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
    param_arn = f"arn:aws:ssm:{REGION}:{ACCOUNT_ID}:parameter{name}"

    now = now_iso()
    _ssm._parameters[name] = {
        "Name": name,
        "Type": ptype,
        "Value": value,
        "Version": 1,
        "LastModifiedDate": now,
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
    arn = f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:{name}:*"
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
    name = props.get("Name") or f"{stack_name}-{logical_id}-{new_uuid()[:8]}"
    bus = props.get("EventBusName", "default")
    key = f"{name}|{bus}"
    arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{bus}/{name}"

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


# ===========================================================================
# Resource Handler Registry
# ===========================================================================

_RESOURCE_HANDLERS = {
    "AWS::S3::Bucket": {"create": _s3_create, "delete": _s3_delete},
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
}
