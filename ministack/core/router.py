"""
AWS API Request Router.
Routes incoming requests to the correct service handler based on:
  - Authorization header (AWS4-HMAC-SHA256 ... SignedHeaders=host;...)
  - X-Amz-Target header (e.g., DynamoDB_20120810.PutItem)
  - Host header (e.g., sqs.us-east-1.amazonaws.com)
  - URL path patterns (e.g., /2015-03-31/functions for Lambda)
"""

import logging
import re

logger = logging.getLogger("ministack")

# Service detection patterns
SERVICE_PATTERNS = {
    "s3": {
        "host_patterns": [r"s3[\.\-]", r"\.s3\."],
        "path_patterns": [r"^/(?!2\d{3}-)"],  # S3 is the fallback for non-API paths
    },
    "sqs": {
        "host_patterns": [r"sqs\."],
        "target_prefixes": ["AmazonSQS"],
        "path_patterns": [r"/queue/", r"Action="],
    },
    "sns": {
        "host_patterns": [r"sns\."],
        "target_prefixes": ["AmazonSNS"],
    },
    "dynamodb": {
        "target_prefixes": ["DynamoDB_20120810"],
        "host_patterns": [r"dynamodb\."],
    },
    "lambda": {
        "path_patterns": [r"^/2015-03-31/functions"],
        "host_patterns": [r"lambda\."],
    },
    "iam": {
        "host_patterns": [r"iam\."],
        "path_patterns": [r"Action=.*(CreateRole|GetRole|ListRoles|PutRolePolicy)"],
    },
    "sts": {
        "host_patterns": [r"sts\."],
        "target_prefixes": ["AWSSecurityTokenService"],
    },
    "secretsmanager": {
        "target_prefixes": ["secretsmanager"],
        "host_patterns": [r"secretsmanager\."],
    },
    "monitoring": {
        "host_patterns": [r"monitoring\."],
        "target_prefixes": ["GraniteServiceVersion20100801"],
    },
    "logs": {
        "target_prefixes": ["Logs_20140328"],
        "host_patterns": [r"logs\."],
    },
    "ssm": {
        "target_prefixes": ["AmazonSSM"],
        "host_patterns": [r"ssm\."],
    },
    "events": {
        "target_prefixes": ["AmazonEventBridge", "AWSEvents"],
        "host_patterns": [r"events\."],
    },
    "kinesis": {
        "target_prefixes": ["Kinesis_20131202"],
        "host_patterns": [r"kinesis\."],
    },
    "ses": {
        "host_patterns": [r"email\."],
        "path_patterns": [r"Action=Send"],
    },
    "states": {
        "target_prefixes": ["AWSStepFunctions"],
        "host_patterns": [r"states\."],
    },
    "ecs": {
        "target_prefixes": ["AmazonEC2ContainerServiceV20141113"],
        "host_patterns": [r"ecs\."],
        "path_patterns": [r"^/clusters", r"^/taskdefinitions", r"^/tasks", r"^/services", r"^/stoptask"],
    },
    "rds": {
        "host_patterns": [r"rds\."],
        "path_patterns": [r"Action=.*DB"],
    },
    "elasticache": {
        "host_patterns": [r"elasticache\."],
        "path_patterns": [r"Action=.*Cache"],
    },
    "glue": {
        "target_prefixes": ["AWSGlue"],
        "host_patterns": [r"glue\."],
    },
    "athena": {
        "target_prefixes": ["AmazonAthena"],
        "host_patterns": [r"athena\."],
    },
    "firehose": {
        "target_prefixes": ["Firehose_20150804"],
        "host_patterns": [r"firehose\.", r"kinesis-firehose\."],
    },
    "apigateway": {
        "host_patterns": [r"apigateway\.", r"execute-api\."],
        "path_patterns": [r"^/v2/apis"],
    },
    "route53": {
        "host_patterns": [r"route53\."],
        "path_patterns": [r"^/2013-04-01/"],
    },
    "cognito-idp": {
        "target_prefixes": ["AWSCognitoIdentityProviderService"],
        "host_patterns": [r"cognito-idp\."],
    },
    "cognito-identity": {
        "target_prefixes": ["AWSCognitoIdentityService"],
        "host_patterns": [r"cognito-identity\."],
    },
    "elasticmapreduce": {
        "target_prefixes": ["ElasticMapReduce"],
        "host_patterns": [r"elasticmapreduce\."],
    },
    "elasticfilesystem": {
        "host_patterns": [r"elasticfilesystem\."],
        "path_prefixes": ["/2015-02-01/"],
        "credential_scope": "elasticfilesystem",
    },
    "ec2": {
        "host_patterns": [r"ec2\."],
        "path_patterns": [r"Action=.*Instance", r"Action=.*Security", r"Action=.*KeyPair",
                          r"Action=.*Vpc", r"Action=.*Subnet", r"Action=.*Address",
                          r"Action=.*Image", r"Action=.*Tag", r"Action=.*InternetGateway",
                          r"Action=.*AvailabilityZone"],
    },
    "elasticloadbalancing": {
        "host_patterns": [r"elasticloadbalancing\."],
    },
}


def detect_service(method: str, path: str, headers: dict, query_params: dict) -> str:
    """Detect which AWS service a request is targeting."""
    host = headers.get("host", "")
    target = headers.get("x-amz-target", "")
    auth = headers.get("authorization", "")
    content_type = headers.get("content-type", "")

    # 1. Check X-Amz-Target header (most reliable for JSON-based services)
    if target:
        for svc, patterns in SERVICE_PATTERNS.items():
            for prefix in patterns.get("target_prefixes", []):
                if target.startswith(prefix):
                    return svc

    # 2. Check Authorization header for service name in credential scope
    if auth:
        match = re.search(r"Credential=[^/]+/[^/]+/[^/]+/([^/]+)/", auth)
        if match:
            svc_name = match.group(1)
            if svc_name in SERVICE_PATTERNS:
                return svc_name
            # Map common credential scope names
            scope_map = {
                "monitoring": "monitoring",
                "execute-api": "apigateway",
                "ses": "ses",
                "states": "states",
                "kinesis": "kinesis",
                "events": "events",
                "ssm": "ssm",
                "ecs": "ecs",
                "rds": "rds",
                "elasticache": "elasticache",
                "glue": "glue",
                "athena": "athena",
                "kinesis-firehose": "firehose",
                "route53": "route53",
                "cognito-idp": "cognito-idp",
                "cognito-identity": "cognito-identity",
                "elasticmapreduce": "elasticmapreduce",
                "elasticloadbalancing": "elasticloadbalancing",
                "elasticfilesystem": "elasticfilesystem",
            }
            if svc_name in scope_map:
                return scope_map[svc_name]

    # 3. Check query parameters for Action-based APIs (SQS, SNS, IAM, STS, CloudWatch)
    action = query_params.get("Action", [""])[0] if isinstance(query_params.get("Action"), list) else query_params.get("Action", "")
    if action:
        action_service_map = {
            # SQS actions
            "SendMessage": "sqs", "ReceiveMessage": "sqs", "DeleteMessage": "sqs",
            "CreateQueue": "sqs", "DeleteQueue": "sqs", "ListQueues": "sqs",
            "GetQueueUrl": "sqs", "GetQueueAttributes": "sqs", "SetQueueAttributes": "sqs",
            "PurgeQueue": "sqs", "ChangeMessageVisibility": "sqs",
            "ChangeMessageVisibilityBatch": "sqs",
            "SendMessageBatch": "sqs", "DeleteMessageBatch": "sqs",
            "ListQueueTags": "sqs", "TagQueue": "sqs", "UntagQueue": "sqs",
            # SNS actions
            "Publish": "sns", "Subscribe": "sns", "Unsubscribe": "sns",
            "CreateTopic": "sns", "DeleteTopic": "sns", "ListTopics": "sns",
            "ListSubscriptions": "sns", "ConfirmSubscription": "sns",
            "SetTopicAttributes": "sns", "GetTopicAttributes": "sns",
            "ListSubscriptionsByTopic": "sns",
            "GetSubscriptionAttributes": "sns", "SetSubscriptionAttributes": "sns",
            "PublishBatch": "sns",
            "ListTagsForResource": "sns", "TagResource": "sns", "UntagResource": "sns",
            "CreatePlatformApplication": "sns", "CreatePlatformEndpoint": "sns",
            # IAM actions
            "CreateRole": "iam", "GetRole": "iam", "ListRoles": "iam",
            "DeleteRole": "iam", "CreateUser": "iam", "GetUser": "iam",
            "ListUsers": "iam", "DeleteUser": "iam",
            "CreatePolicy": "iam", "GetPolicy": "iam", "DeletePolicy": "iam",
            "GetPolicyVersion": "iam", "ListPolicyVersions": "iam",
            "CreatePolicyVersion": "iam", "DeletePolicyVersion": "iam",
            "ListPolicies": "iam",
            "AttachRolePolicy": "iam", "DetachRolePolicy": "iam",
            "ListAttachedRolePolicies": "iam",
            "PutRolePolicy": "iam", "GetRolePolicy": "iam",
            "DeleteRolePolicy": "iam", "ListRolePolicies": "iam",
            "CreateAccessKey": "iam", "ListAccessKeys": "iam", "DeleteAccessKey": "iam",
            "CreateInstanceProfile": "iam", "DeleteInstanceProfile": "iam",
            "GetInstanceProfile": "iam", "AddRoleToInstanceProfile": "iam",
            "RemoveRoleFromInstanceProfile": "iam",
            "ListInstanceProfiles": "iam", "ListInstanceProfilesForRole": "iam",
            "UpdateAssumeRolePolicy": "iam",
            "AttachUserPolicy": "iam", "DetachUserPolicy": "iam",
            "ListAttachedUserPolicies": "iam",
            "TagRole": "iam", "UntagRole": "iam", "ListRoleTags": "iam",
            "TagUser": "iam", "UntagUser": "iam", "ListUserTags": "iam",
            "SimulatePrincipalPolicy": "iam", "SimulateCustomPolicy": "iam",
            # STS actions
            "GetCallerIdentity": "sts", "AssumeRole": "sts",
            "GetSessionToken": "sts", "AssumeRoleWithWebIdentity": "sts",
            "AssumeRoleWithSAML": "sts",
            # CloudWatch actions
            "PutMetricData": "monitoring", "GetMetricData": "monitoring",
            "ListMetrics": "monitoring", "PutMetricAlarm": "monitoring",
            "DescribeAlarms": "monitoring", "DeleteAlarms": "monitoring",
            "GetMetricStatistics": "monitoring", "SetAlarmState": "monitoring",
            "EnableAlarmActions": "monitoring", "DisableAlarmActions": "monitoring",
            "DescribeAlarmsForMetric": "monitoring", "DescribeAlarmHistory": "monitoring",
            "PutCompositeAlarm": "monitoring",
            # SES actions
            "SendEmail": "ses", "SendRawEmail": "ses",
            "VerifyEmailIdentity": "ses", "VerifyEmailAddress": "ses",
            "VerifyDomainIdentity": "ses", "VerifyDomainDkim": "ses",
            "ListIdentities": "ses", "DeleteIdentity": "ses",
            "GetSendQuota": "ses", "GetSendStatistics": "ses",
            "ListVerifiedEmailAddresses": "ses",
            "GetIdentityVerificationAttributes": "ses",
            "GetIdentityDkimAttributes": "ses",
            "SetIdentityNotificationTopic": "ses",
            "SetIdentityFeedbackForwardingEnabled": "ses",
            "CreateConfigurationSet": "ses", "DeleteConfigurationSet": "ses",
            "DescribeConfigurationSet": "ses", "ListConfigurationSets": "ses",
            "CreateTemplate": "ses", "GetTemplate": "ses",
            "DeleteTemplate": "ses", "ListTemplates": "ses", "UpdateTemplate": "ses",
            "SendTemplatedEmail": "ses", "SendBulkTemplatedEmail": "ses",
            # RDS actions
            "CreateDBInstance": "rds", "DeleteDBInstance": "rds", "DescribeDBInstances": "rds",
            "StartDBInstance": "rds", "StopDBInstance": "rds", "RebootDBInstance": "rds",
            "ModifyDBInstance": "rds", "CreateDBCluster": "rds", "DeleteDBCluster": "rds",
            "ModifyDBCluster": "rds",
            "DescribeDBClusters": "rds", "CreateDBSubnetGroup": "rds", "DescribeDBSubnetGroups": "rds",
            "DeleteDBSubnetGroup": "rds",
            "CreateDBParameterGroup": "rds", "DescribeDBParameterGroups": "rds",
            "DeleteDBParameterGroup": "rds", "DescribeDBParameters": "rds",
            "DescribeDBEngineVersions": "rds",
            "DescribeOrderableDBInstanceOptions": "rds",
            "CreateDBSnapshot": "rds", "DeleteDBSnapshot": "rds", "DescribeDBSnapshots": "rds",
            "CreateDBInstanceReadReplica": "rds", "RestoreDBInstanceFromDBSnapshot": "rds",
            "AddTagsToResource": "rds", "RemoveTagsFromResource": "rds",
            "ListTagsForResource": "rds",
            # ElastiCache actions
            "CreateCacheCluster": "elasticache", "DeleteCacheCluster": "elasticache",
            "DescribeCacheClusters": "elasticache", "ModifyCacheCluster": "elasticache",
            "RebootCacheCluster": "elasticache",
            "CreateReplicationGroup": "elasticache", "DeleteReplicationGroup": "elasticache",
            "DescribeReplicationGroups": "elasticache", "ModifyReplicationGroup": "elasticache",
            "CreateCacheSubnetGroup": "elasticache", "DescribeCacheSubnetGroups": "elasticache",
            "DeleteCacheSubnetGroup": "elasticache",
            "CreateCacheParameterGroup": "elasticache", "DescribeCacheParameterGroups": "elasticache",
            "DeleteCacheParameterGroup": "elasticache",
            "DescribeCacheParameters": "elasticache", "ModifyCacheParameterGroup": "elasticache",
            "DescribeCacheEngineVersions": "elasticache",
            "CreateSnapshot": "elasticache", "DeleteSnapshot": "elasticache",
            "DescribeSnapshots": "elasticache",
            "IncreaseReplicaCount": "elasticache", "DecreaseReplicaCount": "elasticache",
            # EC2 actions
            "RunInstances": "ec2", "DescribeInstances": "ec2", "TerminateInstances": "ec2",
            "StopInstances": "ec2", "StartInstances": "ec2", "RebootInstances": "ec2",
            "DescribeImages": "ec2",
            "CreateSecurityGroup": "ec2", "DeleteSecurityGroup": "ec2",
            "DescribeSecurityGroups": "ec2",
            "AuthorizeSecurityGroupIngress": "ec2", "RevokeSecurityGroupIngress": "ec2",
            "AuthorizeSecurityGroupEgress": "ec2", "RevokeSecurityGroupEgress": "ec2",
            "CreateKeyPair": "ec2", "DeleteKeyPair": "ec2", "DescribeKeyPairs": "ec2",
            "ImportKeyPair": "ec2",
            "DescribeVpcs": "ec2", "CreateVpc": "ec2", "DeleteVpc": "ec2",
            "DescribeSubnets": "ec2", "CreateSubnet": "ec2", "DeleteSubnet": "ec2",
            "CreateInternetGateway": "ec2", "DeleteInternetGateway": "ec2",
            "DescribeInternetGateways": "ec2",
            "AttachInternetGateway": "ec2", "DetachInternetGateway": "ec2",
            "DescribeAvailabilityZones": "ec2",
            "AllocateAddress": "ec2", "ReleaseAddress": "ec2",
            "AssociateAddress": "ec2", "DisassociateAddress": "ec2",
            "DescribeAddresses": "ec2",
            "CreateTags": "ec2", "DeleteTags": "ec2", "DescribeTags": "ec2",
            "ModifyVpcAttribute": "ec2", "ModifySubnetAttribute": "ec2",
            "CreateRouteTable": "ec2", "DeleteRouteTable": "ec2", "DescribeRouteTables": "ec2",
            "AssociateRouteTable": "ec2", "DisassociateRouteTable": "ec2",
            "CreateRoute": "ec2", "ReplaceRoute": "ec2", "DeleteRoute": "ec2",
            "CreateNetworkInterface": "ec2", "DeleteNetworkInterface": "ec2",
            "DescribeNetworkInterfaces": "ec2",
            "AttachNetworkInterface": "ec2", "DetachNetworkInterface": "ec2",
            "CreateVpcEndpoint": "ec2", "DeleteVpcEndpoints": "ec2",
            "DescribeVpcEndpoints": "ec2",
            # ELBv2 / ALB actions
            "CreateLoadBalancer": "elasticloadbalancing",
            "DescribeLoadBalancers": "elasticloadbalancing",
            "DeleteLoadBalancer": "elasticloadbalancing",
            "DescribeLoadBalancerAttributes": "elasticloadbalancing",
            "ModifyLoadBalancerAttributes": "elasticloadbalancing",
            "CreateTargetGroup": "elasticloadbalancing",
            "DescribeTargetGroups": "elasticloadbalancing",
            "ModifyTargetGroup": "elasticloadbalancing",
            "DeleteTargetGroup": "elasticloadbalancing",
            "DescribeTargetGroupAttributes": "elasticloadbalancing",
            "ModifyTargetGroupAttributes": "elasticloadbalancing",
            "CreateListener": "elasticloadbalancing",
            "DescribeListeners": "elasticloadbalancing",
            "ModifyListener": "elasticloadbalancing",
            "DeleteListener": "elasticloadbalancing",
            "CreateRule": "elasticloadbalancing",
            "DescribeRules": "elasticloadbalancing",
            "ModifyRule": "elasticloadbalancing",
            "DeleteRule": "elasticloadbalancing",
            "SetRulePriorities": "elasticloadbalancing",
            "RegisterTargets": "elasticloadbalancing",
            "DeregisterTargets": "elasticloadbalancing",
            "DescribeTargetHealth": "elasticloadbalancing",
            "AddTags": "elasticloadbalancing",
            "RemoveTags": "elasticloadbalancing",
            "DescribeTags": "elasticloadbalancing",
            # EBS Volumes
            "CreateVolume": "ec2", "DeleteVolume": "ec2", "DescribeVolumes": "ec2",
            "DescribeVolumeStatus": "ec2", "AttachVolume": "ec2", "DetachVolume": "ec2",
            "ModifyVolume": "ec2", "DescribeVolumesModifications": "ec2",
            "EnableVolumeIO": "ec2", "ModifyVolumeAttribute": "ec2",
            "DescribeVolumeAttribute": "ec2",
            # EBS Snapshots
            "CreateSnapshot": "ec2", "DeleteSnapshot": "ec2", "DescribeSnapshots": "ec2",
            "CopySnapshot": "ec2", "ModifySnapshotAttribute": "ec2",
            "DescribeSnapshotAttribute": "ec2",
        }
        if action in action_service_map:
            return action_service_map[action]

    # 4. Check URL path patterns
    path_lower = path.lower()
    if path_lower.startswith("/2013-04-01/"):
        return "route53"
    if path_lower.startswith("/v2/apis"):
        return "apigateway"
    if (path_lower.startswith("/restapis") or path_lower.startswith("/apikeys")
            or path_lower.startswith("/usageplans") or path_lower.startswith("/domainnames")):
        return "apigateway"
    if path_lower.startswith("/2015-03-31/functions"):
        return "lambda"
    if path_lower.startswith("/oauth2/token"):
        return "cognito-idp"
    if path_lower.startswith(("/clusters", "/taskdefinitions", "/tasks", "/services", "/stoptask")):
        return "ecs"
    # smithy-rpc-v2-cbor path: /service/ServiceName/operation/ActionName
    if "/service/" in path_lower and "/operation/" in path_lower:
        if "granite" in path_lower or "cloudwatch" in path_lower:
            return "monitoring"

    # 5. Check host header patterns
    for svc, patterns in SERVICE_PATTERNS.items():
        for hp in patterns.get("host_patterns", []):
            if re.search(hp, host):
                return svc

    # 6. Default to S3 (same as real LocalStack behavior)
    return "s3"


def extract_region(headers: dict) -> str:
    """Extract AWS region from the request."""
    auth = headers.get("authorization", "")
    match = re.search(r"Credential=[^/]+/[^/]+/([^/]+)/", auth)
    if match:
        return match.group(1)
    return "us-east-1"


def extract_account_id(headers: dict) -> str:
    """Extract or generate account ID."""
    return "000000000000"
