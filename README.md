<p align="center">
  <img src="ministack_logo.png" alt="MiniStack — Free Open-Source AWS Emulator" width="400"/>
</p>

<h1 align="center">MiniStack</h1>
<p align="center"><strong>Free, open-source local AWS emulator. Free forever.</strong></p>
<p align="center">35+ AWS services on a single port · Terraform compatible · Real databases · MIT licensed</p>

<p align="center">
  <a href="https://github.com/Nahuel990/ministack/releases"><img src="https://img.shields.io/github/v/release/Nahuel990/ministack" alt="GitHub release"></a>
  <a href="https://github.com/Nahuel990/ministack/actions"><img src="https://img.shields.io/github/actions/workflow/status/Nahuel990/ministack/ci.yml?branch=master" alt="Build"></a>
  <a href="https://hub.docker.com/r/nahuelnucera/ministack"><img src="https://img.shields.io/docker/pulls/nahuelnucera/ministack" alt="Docker Pulls"></a>
  <a href="https://hub.docker.com/r/nahuelnucera/ministack"><img src="https://img.shields.io/docker/image-size/nahuelnucera/ministack/latest" alt="Docker Image Size"></a>
  <a href="https://github.com/Nahuel990/ministack/blob/master/LICENSE"><img src="https://img.shields.io/github/license/Nahuel990/ministack" alt="License"></a>
  <img src="https://img.shields.io/badge/python-3.12-blue" alt="Python">
  <a href="https://github.com/Nahuel990/ministack/stargazers"><img src="https://img.shields.io/github/stars/Nahuel990/ministack" alt="GitHub stars"></a>
</p>

<p align="center">
  <a href="https://ministack.org">Website</a> · <a href="https://hub.docker.com/r/nahuelnucera/ministack">Docker Hub</a> · <a href="https://www.linkedin.com/company/ministackorg/">LinkedIn</a> · <a href="https://www.producthunt.com/products/ministack">Product Hunt</a>
</p>

---

## Why MiniStack?

LocalStack recently moved its core services behind a paid plan. If you relied on LocalStack Community for local development and CI/CD pipelines, MiniStack is your free alternative.

- **38 AWS services** emulated on a single port (4566)
- **Drop-in compatible** — works with `boto3`, AWS CLI, Terraform, CDK, Pulumi, any SDK
- **Real infrastructure** — RDS spins up actual Postgres/MySQL containers, ElastiCache spins up real Redis, Athena runs real SQL via DuckDB, ECS runs real Docker containers
- **Tiny footprint** — ~200MB image, ~30MB RAM at idle vs LocalStack's ~1GB image and ~500MB RAM
- **Fast startup** — under 2 seconds
- **MIT licensed** — use it, fork it, contribute to it

---

## Quick Start

```bash
# Option 1: PyPI (simplest)
pip install ministack
ministack
# Runs on http://localhost:4566 — use GATEWAY_PORT=XXXX to change

# Option 2: Docker Hub
docker run -p 4566:4566 nahuelnucera/ministack

# Option 2b: Docker Hub with real infrastructure (RDS, ECS, Lambda containers)
docker run -p 4566:4566 -v /var/run/docker.sock:/var/run/docker.sock nahuelnucera/ministack

# Option 3: Clone and build
git clone https://github.com/Nahuel990/ministack
cd ministack
docker compose up -d

# Verify (any option)
curl http://localhost:4566/_ministack/health
```

That's it. No account, no API key, no sign-up.

---

## Internal API

MiniStack exposes internal endpoints for test automation:

```bash
# Health check — returns service status
curl http://localhost:4566/_ministack/health

# Reset all state — wipe every service back to empty (useful between test runs)
curl -X POST http://localhost:4566/_ministack/reset

# Runtime config — change settings without restart
curl -X POST http://localhost:4566/_ministack/config \
  -H "Content-Type: application/json" \
  -d '{"MINISTACK_ACCOUNT_ID": "123456789012"}'
```

The reset endpoint is especially useful in CI pipelines and test suites — call it in `setUp`/`beforeEach` to get a clean environment for every test without restarting the container.

Also compatible with LocalStack's health endpoint:

```bash
curl http://localhost:4566/_localstack/health
curl http://localhost:4566/health
```

---

## Using with AWS CLI

```bash
# Option A — environment variables (no profile needed)
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url=http://localhost:4566 s3 mb s3://my-bucket
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name my-queue
aws --endpoint-url=http://localhost:4566 dynamodb list-tables
aws --endpoint-url=http://localhost:4566 sts get-caller-identity

# Option B — named profile (must pass --profile on every command)
aws configure --profile local
# AWS Access Key ID: test
# AWS Secret Access Key: test
# Default region: us-east-1
# Default output format: json

aws --profile local --endpoint-url=http://localhost:4566 s3 mb s3://my-bucket
aws --profile local --endpoint-url=http://localhost:4566 s3 cp ./file.txt s3://my-bucket/
aws --profile local --endpoint-url=http://localhost:4566 sqs create-queue --queue-name my-queue
aws --profile local --endpoint-url=http://localhost:4566 dynamodb list-tables
aws --profile local --endpoint-url=http://localhost:4566 sts get-caller-identity
```

### awslocal wrapper

```bash
chmod +x bin/awslocal
./bin/awslocal s3 ls
./bin/awslocal dynamodb list-tables
```

---

## Using with boto3

```python
import boto3

# All clients use the same endpoint
def client(service):
    return boto3.client(
        service,
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

# S3
s3 = client("s3")
s3.create_bucket(Bucket="my-bucket")
s3.put_object(Bucket="my-bucket", Key="hello.txt", Body=b"Hello, MiniStack!")
obj = s3.get_object(Bucket="my-bucket", Key="hello.txt")
print(obj["Body"].read())  # b'Hello, MiniStack!'

# SQS
sqs = client("sqs")
q = sqs.create_queue(QueueName="my-queue")
sqs.send_message(QueueUrl=q["QueueUrl"], MessageBody="hello")
msgs = sqs.receive_message(QueueUrl=q["QueueUrl"])
print(msgs["Messages"][0]["Body"])  # hello

# DynamoDB
ddb = client("dynamodb")
ddb.create_table(
    TableName="Users",
    KeySchema=[{"AttributeName": "userId", "KeyType": "HASH"}],
    AttributeDefinitions=[{"AttributeName": "userId", "AttributeType": "S"}],
    BillingMode="PAY_PER_REQUEST",
)
ddb.put_item(TableName="Users", Item={"userId": {"S": "u1"}, "name": {"S": "Alice"}})

# SSM Parameter Store
ssm = client("ssm")
ssm.put_parameter(Name="/app/db/host", Value="localhost", Type="String")
param = ssm.get_parameter(Name="/app/db/host")
print(param["Parameter"]["Value"])  # localhost

# Secrets Manager
sm = client("secretsmanager")
sm.create_secret(Name="db-password", SecretString='{"password":"s3cr3t"}')

# Kinesis
kin = client("kinesis")
kin.create_stream(StreamName="events", ShardCount=1)
kin.put_record(StreamName="events", Data=b'{"event":"click"}', PartitionKey="user1")

# EventBridge
eb = client("events")
eb.put_events(Entries=[{
    "Source": "myapp",
    "DetailType": "UserSignup",
    "Detail": '{"userId": "123"}',
    "EventBusName": "default",
}])

# Step Functions
sfn = client("stepfunctions")
sfn.create_state_machine(
    name="my-workflow",
    definition='{"StartAt":"Hello","States":{"Hello":{"Type":"Pass","End":true}}}',
    roleArn="arn:aws:iam::000000000000:role/role",
)

# Step Functions — TestState API (test a single state in isolation)
# Note: inject_host_prefix=False prevents boto3 from prepending "sync-" to the hostname
from botocore.config import Config as BotoConfig
sfn_test = client("stepfunctions", config=BotoConfig(inject_host_prefix=False))

result = sfn_test.test_state(
    definition='{"Type":"Pass","Result":{"greeting":"hello"},"End":true}',
    input='{"name":"world"}',
)
print(result["status"])  # SUCCEEDED
print(result["output"])  # {"greeting": "hello"}

# TestState with mock — test error handling without calling real services
result = sfn_test.test_state(
    definition=json.dumps({
        "Type": "Task",
        "Resource": "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
        "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback"}],
        "End": True
    }),
    input='{}',
    inspectionLevel="DEBUG",
    mock={"errorOutput": {"error": "ServiceError", "cause": "Timeout"}},
)
print(result["status"])  # CAUGHT_ERROR
print(result["nextState"])  # Fallback

# EC2
ec2 = client("ec2")
reservation = ec2.run_instances(
    ImageId="ami-00000001",
    MinCount=1,
    MaxCount=1,
    InstanceType="t3.micro",
)
instance_id = reservation["Instances"][0]["InstanceId"]
print(instance_id)  # i-xxxxxxxxxxxxxxxxx

# Security Groups
sg = ec2.create_security_group(GroupName="my-sg", Description="My SG")
ec2.authorize_security_group_ingress(
    GroupId=sg["GroupId"],
    IpPermissions=[{"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}],
)

# VPC / Subnet
vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
subnet = ec2.create_subnet(
    VpcId=vpc["Vpc"]["VpcId"],
    CidrBlock="10.0.1.0/24",
    AvailabilityZone="us-east-1a",
)
```

---

## Supported Services

### Core Services

| Service | Operations | Notes |
|---------|-----------|-------|
| **S3** | CreateBucket, DeleteBucket, ListBuckets, HeadBucket, PutObject, GetObject, DeleteObject, HeadObject, CopyObject, ListObjects v1/v2, DeleteObjects, GetBucketVersioning, PutBucketVersioning, GetBucketEncryption, PutBucketEncryption, DeleteBucketEncryption, GetBucketLifecycleConfiguration, PutBucketLifecycleConfiguration, DeleteBucketLifecycle, GetBucketCors, PutBucketCors, DeleteBucketCors, GetBucketAcl, PutBucketAcl, GetBucketTagging, PutBucketTagging, DeleteBucketTagging, GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy, GetBucketNotificationConfiguration, PutBucketNotificationConfiguration, GetBucketLogging, PutBucketLogging, ListObjectVersions, CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, PutObjectLockConfiguration, GetObjectLockConfiguration, PutObjectRetention, GetObjectRetention, PutObjectLegalHold, GetObjectLegalHold, PutBucketReplication, GetBucketReplication, DeleteBucketReplication | Optional disk persistence via `S3_PERSIST=1`; Object Lock with retention & legal hold enforcement on delete |
| **SQS** | CreateQueue, DeleteQueue, ListQueues, GetQueueUrl, GetQueueAttributes, SetQueueAttributes, PurgeQueue, SendMessage, ReceiveMessage, DeleteMessage, ChangeMessageVisibility, ChangeMessageVisibilityBatch, SendMessageBatch, DeleteMessageBatch, TagQueue, UntagQueue, ListQueueTags | Both Query API and JSON protocol; FIFO queues with deduplication; DLQ support |
| **SNS** | CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes, Subscribe, Unsubscribe, ListSubscriptions, ListSubscriptionsByTopic, GetSubscriptionAttributes, SetSubscriptionAttributes, ConfirmSubscription, Publish, PublishBatch, TagResource, UntagResource, ListTagsForResource, CreatePlatformApplication, CreatePlatformEndpoint | SNS→SQS fanout delivery; SNS→Lambda fanout (synchronous invocation) |
| **DynamoDB** | CreateTable, UpdateTable, DeleteTable, DescribeTable, ListTables, PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, BatchWriteItem, BatchGetItem, TransactWriteItems, TransactGetItems, DescribeTimeToLive, UpdateTimeToLive, DescribeContinuousBackups, UpdateContinuousBackups, DescribeEndpoints, TagResource, UntagResource, ListTagsOfResource | TTL enforced via thread-safe background reaper (60s cadence); DynamoDB Streams — `StreamSpecification` emits INSERT/MODIFY/REMOVE records on all write operations, respects `StreamViewType` |
| **Lambda** | CreateFunction, DeleteFunction, GetFunction, GetFunctionConfiguration, ListFunctions, Invoke, UpdateFunctionCode, UpdateFunctionConfiguration, AddPermission, RemovePermission, GetPolicy, ListVersionsByFunction, PublishVersion, CreateAlias, GetAlias, UpdateAlias, DeleteAlias, ListAliases, TagResource, UntagResource, ListTags, CreateEventSourceMapping, DeleteEventSourceMapping, GetEventSourceMapping, ListEventSourceMappings, UpdateEventSourceMapping, CreateFunctionUrlConfig, GetFunctionUrlConfig, UpdateFunctionUrlConfig, DeleteFunctionUrlConfig, ListFunctionUrlConfigs, PutFunctionConcurrency, GetFunctionConcurrency, DeleteFunctionConcurrency, PutFunctionEventInvokeConfig, GetFunctionEventInvokeConfig, DeleteFunctionEventInvokeConfig, PublishLayerVersion, GetLayerVersion, GetLayerVersionByArn, ListLayerVersions, DeleteLayerVersion, ListLayers, AddLayerVersionPermission, RemoveLayerVersionPermission, GetLayerVersionPolicy | Python and Node.js runtimes execute with warm worker pool; `provided.al2023`/`provided.al2` runtimes execute via Docker RIE (Go, Rust, C++ support); `Publish=True` creates immutable numbered versions; Code via `ZipFile`, `S3Bucket`/`S3Key`, or `ImageUri` (Docker image); `PackageType: Image` pulls and invokes user-provided Docker images via Lambda RIE; SQS, Kinesis, and DynamoDB Streams event source mappings; Function URL CRUD; Lambda Layers CRUD; Aliases; Concurrency; EventInvokeConfig |
| **IAM** | CreateUser, GetUser, ListUsers, DeleteUser, CreateRole, GetRole, ListRoles, DeleteRole, CreatePolicy, GetPolicy, DeletePolicy, AttachRolePolicy, DetachRolePolicy, PutRolePolicy, GetRolePolicy, DeleteRolePolicy, ListRolePolicies, ListAttachedRolePolicies, CreateAccessKey, ListAccessKeys, DeleteAccessKey, CreateInstanceProfile, GetInstanceProfile, DeleteInstanceProfile, AddRoleToInstanceProfile, RemoveRoleFromInstanceProfile, ListInstanceProfiles, CreateGroup, GetGroup, AddUserToGroup, RemoveUserFromGroup, CreateServiceLinkedRole, DeleteServiceLinkedRole, GetServiceLinkedRoleDeletionStatus, CreateOpenIDConnectProvider, TagRole, UntagRole, TagUser, UntagUser, TagPolicy, UntagPolicy | |
| **STS** | GetCallerIdentity, AssumeRole, GetSessionToken, AssumeRoleWithWebIdentity | |
| **SecretsManager** | CreateSecret, GetSecretValue, ListSecrets, DeleteSecret, UpdateSecret, DescribeSecret, PutSecretValue, UpdateSecretVersionStage, RestoreSecret, RotateSecret, GetRandomPassword, ListSecretVersionIds, ReplicateSecretToRegions, TagResource, UntagResource, PutResourcePolicy, GetResourcePolicy, DeleteResourcePolicy, ValidateResourcePolicy | |
| **CloudWatch Logs** | CreateLogGroup, DeleteLogGroup, DescribeLogGroups, CreateLogStream, DeleteLogStream, DescribeLogStreams, PutLogEvents, GetLogEvents, FilterLogEvents, PutRetentionPolicy, DeleteRetentionPolicy, PutSubscriptionFilter, DeleteSubscriptionFilter, DescribeSubscriptionFilters, PutMetricFilter, DeleteMetricFilter, DescribeMetricFilters, TagLogGroup, UntagLogGroup, ListTagsLogGroup, TagResource, UntagResource, ListTagsForResource, StartQuery, GetQueryResults, StopQuery, PutDestination, DeleteDestination, DescribeDestinations, PutDestinationPolicy | `FilterLogEvents` supports `*`/`?` globs, multi-term AND, `-term` exclusion |

### Extended Services

| Service | Operations | Notes |
|---------|-----------|-------|
| **SSM Parameter Store** | PutParameter, GetParameter, GetParameters, GetParametersByPath, DeleteParameter, DeleteParameters, DescribeParameters, GetParameterHistory, LabelParameterVersion, AddTagsToResource, RemoveTagsFromResource, ListTagsForResource | Supports String, SecureString, StringList |
| **EventBridge** | CreateEventBus, DeleteEventBus, ListEventBuses, DescribeEventBus, PutRule, DeleteRule, ListRules, DescribeRule, EnableRule, DisableRule, PutTargets, RemoveTargets, ListTargetsByRule, PutEvents, TagResource, UntagResource, ListTagsForResource, CreateArchive, DeleteArchive, DescribeArchive, ListArchives, PutPermission, RemovePermission, CreateConnection, DescribeConnection, DeleteConnection, UpdateConnection, ListConnections, CreateApiDestination, DescribeApiDestination, DeleteApiDestination, UpdateApiDestination, ListApiDestinations | Lambda target dispatch on PutEvents; S3 EventBridge notifications |
| **Kinesis** | CreateStream, DeleteStream, DescribeStream, DescribeStreamSummary, ListStreams, ListShards, PutRecord, PutRecords, GetShardIterator, GetRecords, IncreaseStreamRetentionPeriod, DecreaseStreamRetentionPeriod, MergeShards, SplitShard, UpdateShardCount, StartStreamEncryption, StopStreamEncryption, EnableEnhancedMonitoring, DisableEnhancedMonitoring, RegisterStreamConsumer, DeregisterStreamConsumer, ListStreamConsumers, DescribeStreamConsumer, AddTagsToStream, RemoveTagsFromStream, ListTagsForStream | Partition key → shard routing; AWS limits enforced (1 MB/record, 500 records/batch, 5 MB payload, 256-char partition key) |
| **CloudWatch Metrics** | PutMetricData, GetMetricStatistics, GetMetricData, ListMetrics, PutMetricAlarm, PutCompositeAlarm, DescribeAlarms, DescribeAlarmsForMetric, DescribeAlarmHistory, DeleteAlarms, SetAlarmState, EnableAlarmActions, DisableAlarmActions, TagResource, UntagResource, ListTagsForResource, PutDashboard, GetDashboard, DeleteDashboards, ListDashboards | CBOR and JSON protocol |
| **SES** | SendEmail, SendRawEmail, SendTemplatedEmail, SendBulkTemplatedEmail, VerifyEmailIdentity, VerifyEmailAddress, VerifyDomainIdentity, VerifyDomainDkim, ListIdentities, ListVerifiedEmailAddresses, GetIdentityVerificationAttributes, GetIdentityDkimAttributes, DeleteIdentity, GetSendQuota, GetSendStatistics, SetIdentityNotificationTopic, SetIdentityFeedbackForwardingEnabled, CreateConfigurationSet, DeleteConfigurationSet, DescribeConfigurationSet, ListConfigurationSets, CreateTemplate, GetTemplate, UpdateTemplate, DeleteTemplate, ListTemplates | Emails stored in-memory, not sent |
| **SES v2** | SendEmail, CreateEmailIdentity, GetEmailIdentity, DeleteEmailIdentity, ListEmailIdentities, CreateConfigurationSet, GetConfigurationSet, DeleteConfigurationSet, ListConfigurationSets, GetAccount, PutAccountSuppressionAttributes, ListSuppressedDestinations | REST API (`/v2/email/`); identities auto-verified; emails stored in-memory, not sent |
| **ACM** | RequestCertificate, DescribeCertificate, ListCertificates, DeleteCertificate, GetCertificate, ImportCertificate, AddTagsToCertificate, RemoveTagsFromCertificate, ListTagsForCertificate, UpdateCertificateOptions, RenewCertificate, ResendValidationEmail | Certificates auto-issued; DNS validation records generated; supports SANs |
| **WAF v2** | CreateWebACL, GetWebACL, UpdateWebACL, DeleteWebACL, ListWebACLs, AssociateWebACL, DisassociateWebACL, GetWebACLForResource, ListResourcesForWebACL, CreateIPSet, GetIPSet, UpdateIPSet, DeleteIPSet, ListIPSets, CreateRuleGroup, GetRuleGroup, UpdateRuleGroup, DeleteRuleGroup, ListRuleGroups, TagResource, UntagResource, ListTagsForResource, CheckCapacity, DescribeManagedRuleGroup | LockToken enforced on Update/Delete; resource associations tracked |
| **Step Functions** | CreateStateMachine, DeleteStateMachine, DescribeStateMachine, UpdateStateMachine, ListStateMachines, StartExecution, StartSyncExecution, StopExecution, DescribeExecution, DescribeStateMachineForExecution, ListExecutions, GetExecutionHistory, SendTaskSuccess, SendTaskFailure, SendTaskHeartbeat, CreateActivity, DeleteActivity, DescribeActivity, ListActivities, GetActivityTask, TestState, TagResource, UntagResource, ListTagsForResource | Full ASL interpreter; Retry/Catch; waitForTaskToken; Activities (worker pattern); Pass/Task/Choice/Wait/Succeed/Fail/Map/Parallel; TestState API with mock and inspectionLevel support; SFN_MOCK_CONFIG for AWS SFN Local compatible mock testing; intrinsic functions (States.StringToJson, States.JsonMerge, States.Format); nested startExecution.sync |
| **API Gateway v2** | CreateApi, GetApi, GetApis, UpdateApi, DeleteApi, CreateRoute, GetRoute, GetRoutes, UpdateRoute, DeleteRoute, CreateIntegration, GetIntegration, GetIntegrations, UpdateIntegration, DeleteIntegration, CreateStage, GetStage, GetStages, UpdateStage, DeleteStage, CreateDeployment, GetDeployment, GetDeployments, DeleteDeployment, CreateAuthorizer, GetAuthorizer, GetAuthorizers, UpdateAuthorizer, DeleteAuthorizer, TagResource, UntagResource, GetTags | HTTP API (v2) protocol; Lambda proxy (AWS_PROXY) and HTTP proxy (HTTP_PROXY) integrations; data plane via `{apiId}.execute-api.localhost`; `{param}` and `{proxy+}` path matching; JWT/Lambda authorizer CRUD |
| **API Gateway v1** | CreateRestApi, GetRestApi, GetRestApis, UpdateRestApi, DeleteRestApi, CreateResource, GetResource, GetResources, UpdateResource, DeleteResource, PutMethod, GetMethod, DeleteMethod, UpdateMethod, PutMethodResponse, GetMethodResponse, DeleteMethodResponse, PutIntegration, GetIntegration, DeleteIntegration, UpdateIntegration, PutIntegrationResponse, GetIntegrationResponse, DeleteIntegrationResponse, CreateDeployment, GetDeployment, GetDeployments, UpdateDeployment, DeleteDeployment, CreateStage, GetStage, GetStages, UpdateStage, DeleteStage, CreateAuthorizer, GetAuthorizer, GetAuthorizers, UpdateAuthorizer, DeleteAuthorizer, CreateModel, GetModel, GetModels, DeleteModel, CreateApiKey, GetApiKey, GetApiKeys, UpdateApiKey, DeleteApiKey, CreateUsagePlan, GetUsagePlan, GetUsagePlans, UpdateUsagePlan, DeleteUsagePlan, CreateUsagePlanKey, GetUsagePlanKeys, DeleteUsagePlanKey, CreateDomainName, GetDomainName, GetDomainNames, DeleteDomainName, CreateBasePathMapping, GetBasePathMapping, GetBasePathMappings, DeleteBasePathMapping, TagResource, UntagResource, GetTags | REST API (v1) protocol; Lambda proxy format 1.0 (AWS_PROXY), HTTP proxy (HTTP_PROXY), MOCK integration; data plane via `{apiId}.execute-api.localhost`; resource tree with `{param}` and `{proxy+}` path matching; JSON Patch for all PATCH operations; state persistence |
| **ELBv2 / ALB** | CreateLoadBalancer, DescribeLoadBalancers, DeleteLoadBalancer, DescribeLoadBalancerAttributes, ModifyLoadBalancerAttributes, CreateTargetGroup, DescribeTargetGroups, ModifyTargetGroup, DeleteTargetGroup, DescribeTargetGroupAttributes, ModifyTargetGroupAttributes, CreateListener, DescribeListeners, ModifyListener, DeleteListener, CreateRule, DescribeRules, ModifyRule, DeleteRule, SetRulePriorities, RegisterTargets, DeregisterTargets, DescribeTargetHealth, AddTags, RemoveTags, DescribeTags | Control plane + data plane; ALB→Lambda live traffic routing; `path-pattern`, `host-header`, `http-method`, `query-string`, `http-header` rule conditions; `forward`, `redirect`, `fixed-response` actions; data plane via `{lb-name}.alb.localhost` Host header or `/_alb/{lb-name}/` path prefix |

| **KMS** | CreateKey, ListKeys, DescribeKey, GetPublicKey, Sign, Verify, Encrypt, Decrypt, GenerateDataKey, GenerateDataKeyWithoutPlaintext, CreateAlias, DeleteAlias, ListAliases, UpdateAlias, EnableKeyRotation, DisableKeyRotation, GetKeyRotationStatus, GetKeyPolicy, PutKeyPolicy, ListKeyPolicies, EnableKey, DisableKey, ScheduleKeyDeletion, CancelKeyDeletion, TagResource, UntagResource, ListResourceTags | 27 actions; RSA (2048/4096) and symmetric keys; PKCS1v15 and PSS signing; envelope encryption; alias resolution; key rotation; key policies; tags; enable/disable/schedule deletion; full Terraform `aws_kms_key` compatible; `cryptography` package included in Docker image |

### CloudFormation

| Feature | Details |
|---------|---------|
| **Stack Operations** | CreateStack, UpdateStack, DeleteStack, DescribeStacks, ListStacks, DescribeStackEvents, DescribeStackResource, DescribeStackResources, GetTemplate, ValidateTemplate, GetTemplateSummary |
| **Change Sets** | CreateChangeSet, DescribeChangeSet, ExecuteChangeSet, DeleteChangeSet, ListChangeSets |
| **Exports** | ListExports — cross-stack references via `Fn::ImportValue` |
| **Template Formats** | JSON and YAML (including `!Ref`, `!Sub`, `!GetAtt` shorthand tags) |
| **Intrinsic Functions** | Ref, Fn::GetAtt, Fn::Join, Fn::Sub (both forms), Fn::Select, Fn::Split, Fn::If, Fn::Equals, Fn::And, Fn::Or, Fn::Not, Fn::Base64, Fn::FindInMap, Fn::ImportValue, Fn::GetAZs, Fn::Cidr |
| **Pseudo-Parameters** | AWS::StackName, AWS::StackId, AWS::Region, AWS::AccountId, AWS::URLSuffix, AWS::Partition, AWS::NoValue |
| **Parameters** | Default values, AllowedValues validation, NoEcho masking, String/Number/CommaDelimitedList types |
| **Conditions** | Fn::Equals, Fn::And, Fn::Or, Fn::Not — conditional resource creation |
| **Rollback** | Configurable via `DisableRollback` — on failure, previously created resources are cleaned up in reverse dependency order |
| **Async Status** | Stacks deploy asynchronously (`CREATE_IN_PROGRESS` → `CREATE_COMPLETE`) — poll with DescribeStacks |

**Supported Resource Types:**

| Resource Type | Ref Returns | GetAtt |
|---------------|-------------|--------|
| `AWS::S3::Bucket` | Bucket name | Arn, DomainName, RegionalDomainName, WebsiteURL |
| `AWS::SQS::Queue` | Queue URL | Arn, QueueName, QueueUrl |
| `AWS::SNS::Topic` | Topic ARN | TopicArn, TopicName |
| `AWS::SNS::Subscription` | Subscription ARN | — |
| `AWS::DynamoDB::Table` | Table name | Arn, StreamArn |
| `AWS::Lambda::Function` | Function name | Arn |
| `AWS::IAM::Role` | Role name | Arn, RoleId |
| `AWS::IAM::Policy` | Policy ARN | — |
| `AWS::IAM::InstanceProfile` | Profile name | Arn |
| `AWS::SSM::Parameter` | Parameter name | Type, Value |
| `AWS::Logs::LogGroup` | Log group name | Arn |
| `AWS::Events::Rule` | Rule name | Arn |

Unsupported resource types fail with `CREATE_FAILED` (or `ROLLBACK_COMPLETE` if rollback is enabled), so templates with unsupported types won't silently succeed.

### Infrastructure Services

| Service | Operations | Notes |
|---------|-----------|-------|
| **ECS** | CreateCluster, DeleteCluster, DescribeClusters, ListClusters, UpdateCluster, UpdateClusterSettings, PutClusterCapacityProviders, RegisterTaskDefinition, DeregisterTaskDefinition, DescribeTaskDefinition, ListTaskDefinitions, ListTaskDefinitionFamilies, DeleteTaskDefinitions, CreateService, DeleteService, DescribeServices, UpdateService, ListServices, ListServicesByNamespace, RunTask, StopTask, DescribeTasks, ListTasks, ExecuteCommand, UpdateTaskProtection, GetTaskProtection, CreateCapacityProvider, UpdateCapacityProvider, DeleteCapacityProvider, DescribeCapacityProviders, TagResource, UntagResource, ListTagsForResource, ListAccountSettings, PutAccountSetting, PutAccountSettingDefault, DeleteAccountSetting, PutAttributes, DeleteAttributes, ListAttributes, DescribeServiceDeployments, ListServiceDeployments, DescribeServiceRevisions, SubmitTaskStateChange, SubmitContainerStateChange, SubmitAttachmentStateChanges, DiscoverPollEndpoint | 47 actions; `RunTask` starts real Docker containers via Docker socket; full Terraform ECS coverage |
| **RDS** | CreateDBInstance, DeleteDBInstance, DescribeDBInstances, ModifyDBInstance, StartDBInstance, StopDBInstance, RebootDBInstance, CreateDBInstanceReadReplica, RestoreDBInstanceFromDBSnapshot, CreateDBCluster, DeleteDBCluster, DescribeDBClusters, ModifyDBCluster, StartDBCluster, StopDBCluster, CreateDBSnapshot, DeleteDBSnapshot, DescribeDBSnapshots, CreateDBClusterSnapshot, DescribeDBClusterSnapshots, DeleteDBClusterSnapshot, CreateDBSubnetGroup, DeleteDBSubnetGroup, DescribeDBSubnetGroups, ModifyDBSubnetGroup, CreateDBParameterGroup, DeleteDBParameterGroup, DescribeDBParameterGroups, DescribeDBParameters, ModifyDBParameterGroup, CreateDBClusterParameterGroup, DescribeDBClusterParameterGroups, DeleteDBClusterParameterGroup, DescribeDBClusterParameters, ModifyDBClusterParameterGroup, CreateOptionGroup, DeleteOptionGroup, DescribeOptionGroups, DescribeOptionGroupOptions, ListTagsForResource, AddTagsToResource, RemoveTagsFromResource, DescribeDBEngineVersions, DescribeOrderableDBInstanceOptions | `CreateDBInstance` spins up real Postgres/MySQL Docker container, returns actual `host:port` endpoint |
| **ElastiCache** | CreateCacheCluster, DeleteCacheCluster, DescribeCacheClusters, ModifyCacheCluster, RebootCacheCluster, CreateReplicationGroup, DeleteReplicationGroup, DescribeReplicationGroups, ModifyReplicationGroup, IncreaseReplicaCount, DecreaseReplicaCount, CreateCacheSubnetGroup, DescribeCacheSubnetGroups, ModifyCacheSubnetGroup, DeleteCacheSubnetGroup, CreateCacheParameterGroup, DescribeCacheParameterGroups, ModifyCacheParameterGroup, ResetCacheParameterGroup, DeleteCacheParameterGroup, DescribeCacheParameters, DescribeCacheEngineVersions, CreateUser, DescribeUsers, DeleteUser, ModifyUser, CreateUserGroup, DescribeUserGroups, DeleteUserGroup, ModifyUserGroup, ListTagsForResource, AddTagsToResource, RemoveTagsFromResource, CreateSnapshot, DeleteSnapshot, DescribeSnapshots, DescribeEvents | `CreateCacheCluster` spins up real Redis/Memcached Docker container |
| **Glue** | CreateDatabase, DeleteDatabase, GetDatabase, GetDatabases, UpdateDatabase, CreateTable, DeleteTable, GetTable, GetTables, UpdateTable, BatchDeleteTable, CreatePartition, DeletePartition, GetPartition, GetPartitions, BatchCreatePartition, BatchGetPartition, CreatePartitionIndex, GetPartitionIndexes, CreateConnection, DeleteConnection, GetConnection, GetConnections, CreateCrawler, DeleteCrawler, GetCrawler, GetCrawlers, UpdateCrawler, StartCrawler, StopCrawler, GetCrawlerMetrics, CreateJob, DeleteJob, GetJob, GetJobs, UpdateJob, StartJobRun, GetJobRun, GetJobRuns, BatchStopJobRun, CreateTrigger, GetTrigger, DeleteTrigger, UpdateTrigger, StartTrigger, StopTrigger, ListTriggers, BatchGetTriggers, GetTriggers, CreateWorkflow, GetWorkflow, DeleteWorkflow, UpdateWorkflow, StartWorkflowRun, CreateSecurityConfiguration, DeleteSecurityConfiguration, GetSecurityConfiguration, GetSecurityConfigurations, CreateClassifier, GetClassifier, GetClassifiers, DeleteClassifier, TagResource, UntagResource, GetTags | Python shell jobs actually execute via subprocess |
| **Athena** | StartQueryExecution, GetQueryExecution, GetQueryResults, StopQueryExecution, ListQueryExecutions, BatchGetQueryExecution, CreateWorkGroup, DeleteWorkGroup, GetWorkGroup, ListWorkGroups, UpdateWorkGroup, CreateNamedQuery, DeleteNamedQuery, GetNamedQuery, ListNamedQueries, BatchGetNamedQuery, CreateDataCatalog, GetDataCatalog, ListDataCatalogs, DeleteDataCatalog, UpdateDataCatalog, CreatePreparedStatement, GetPreparedStatement, DeletePreparedStatement, ListPreparedStatements, GetTableMetadata, ListTableMetadata, TagResource, UntagResource, ListTagsForResource | Real SQL via **DuckDB** when installed (`pip install duckdb`), otherwise returns mock results; result pagination; column type metadata |
| **Firehose** | CreateDeliveryStream, DeleteDeliveryStream, DescribeDeliveryStream, ListDeliveryStreams, PutRecord, PutRecordBatch, UpdateDestination, TagDeliveryStream, UntagDeliveryStream, ListTagsForDeliveryStream, StartDeliveryStreamEncryption, StopDeliveryStreamEncryption | S3 destinations write records to the local S3 emulator; all other destination types buffer in-memory; concurrency-safe `UpdateDestination` via `VersionId` |
| **Route53** | CreateHostedZone, GetHostedZone, DeleteHostedZone, ListHostedZones, ListHostedZonesByName, UpdateHostedZoneComment, ChangeResourceRecordSets (CREATE/UPSERT/DELETE), ListResourceRecordSets, GetChange, CreateHealthCheck, GetHealthCheck, DeleteHealthCheck, ListHealthChecks, UpdateHealthCheck, ChangeTagsForResource, ListTagsForResource | REST/XML protocol; SOA + NS records auto-created; CallerReference idempotency; alias records, weighted/failover/latency routing; marker-based pagination |
| **EC2** | RunInstances, DescribeInstances, DescribeInstanceAttribute, DescribeInstanceTypes, DescribeVpcAttribute, TerminateInstances, StopInstances, StartInstances, RebootInstances, DescribeImages, CreateSecurityGroup, DeleteSecurityGroup, DescribeSecurityGroups, AuthorizeSecurityGroupIngress, RevokeSecurityGroupIngress, AuthorizeSecurityGroupEgress, RevokeSecurityGroupEgress, DescribeSecurityGroupRules, CreateKeyPair, DeleteKeyPair, DescribeKeyPairs, ImportKeyPair, CreateVpc, DeleteVpc, DescribeVpcs, ModifyVpcAttribute, CreateSubnet, DeleteSubnet, DescribeSubnets, ModifySubnetAttribute, CreateInternetGateway, DeleteInternetGateway, DescribeInternetGateways, AttachInternetGateway, DetachInternetGateway, CreateRouteTable, DeleteRouteTable, DescribeRouteTables, AssociateRouteTable, DisassociateRouteTable, ReplaceRouteTableAssociation, CreateRoute, ReplaceRoute, DeleteRoute, CreateNetworkInterface, DeleteNetworkInterface, DescribeNetworkInterfaces, AttachNetworkInterface, DetachNetworkInterface, CreateVpcEndpoint, DeleteVpcEndpoints, DescribeVpcEndpoints, ModifyVpcEndpoint, DescribePrefixLists, DescribeAvailabilityZones, AllocateAddress, ReleaseAddress, AssociateAddress, DisassociateAddress, DescribeAddresses, DescribeAddressesAttribute, CreateTags, DeleteTags, DescribeTags, CreateNatGateway, DescribeNatGateways, DeleteNatGateway, CreateNetworkAcl, DescribeNetworkAcls, DeleteNetworkAcl, CreateNetworkAclEntry, DeleteNetworkAclEntry, ReplaceNetworkAclEntry, ReplaceNetworkAclAssociation, CreateFlowLogs, DescribeFlowLogs, DeleteFlowLogs, CreateVpcPeeringConnection, AcceptVpcPeeringConnection, DescribeVpcPeeringConnections, DeleteVpcPeeringConnection, CreateDhcpOptions, AssociateDhcpOptions, DescribeDhcpOptions, DeleteDhcpOptions, CreateEgressOnlyInternetGateway, DescribeEgressOnlyInternetGateways, DeleteEgressOnlyInternetGateway, CreateManagedPrefixList, DescribeManagedPrefixLists, GetManagedPrefixListEntries, ModifyManagedPrefixList, DeleteManagedPrefixList, CreateVpnGateway, DescribeVpnGateways, AttachVpnGateway, DetachVpnGateway, DeleteVpnGateway, EnableVgwRoutePropagation, DisableVgwRoutePropagation, CreateCustomerGateway, DescribeCustomerGateways, DeleteCustomerGateway, DescribeInstanceCreditSpecifications, DescribeInstanceMaintenanceOptions, DescribeInstanceAutoRecoveryAttribute, ModifyInstanceMaintenanceOptions, DescribeInstanceTopology, DescribeSpotInstanceRequests, DescribeCapacityReservations | 127 actions; in-memory state only — no real VMs; CreateVpc provisions per-VPC default route table, network ACL, and security group; full Terraform VPC module v6.6.0 compatible; VPN/Customer gateways, managed prefix lists, VPC endpoints with modify support |
| **EBS** | CreateVolume, DeleteVolume, DescribeVolumes, DescribeVolumeStatus, AttachVolume, DetachVolume, ModifyVolume, DescribeVolumesModifications, EnableVolumeIO, ModifyVolumeAttribute, DescribeVolumeAttribute, CreateSnapshot, DeleteSnapshot, DescribeSnapshots, CopySnapshot, ModifySnapshotAttribute, DescribeSnapshotAttribute | Part of EC2 Query/XML service; attach/detach updates volume state; snapshots stored as completed immediately; Pro-only on LocalStack — free here |
| **EFS** | CreateFileSystem, DescribeFileSystems, DeleteFileSystem, UpdateFileSystem, CreateMountTarget, DescribeMountTargets, DeleteMountTarget, DescribeMountTargetSecurityGroups, ModifyMountTargetSecurityGroups, CreateAccessPoint, DescribeAccessPoints, DeleteAccessPoint, TagResource, UntagResource, ListTagsForResource, PutLifecycleConfiguration, DescribeLifecycleConfiguration, PutBackupPolicy, DescribeBackupPolicy, DescribeAccountPreferences, PutAccountPreferences | REST/JSON `/2015-02-01/*`; CreationToken idempotency; FileSystem deletion blocked when mount targets exist; Pro-only on LocalStack — free here |
| **EMR** | RunJobFlow, DescribeCluster, ListClusters, TerminateJobFlows, ModifyCluster, SetTerminationProtection, SetVisibleToAllUsers, AddJobFlowSteps, DescribeStep, ListSteps, CancelSteps, AddInstanceFleet, ListInstanceFleets, ModifyInstanceFleet, AddInstanceGroups, ListInstanceGroups, ModifyInstanceGroups, ListBootstrapActions, AddTags, RemoveTags, GetBlockPublicAccessConfiguration, PutBlockPublicAccessConfiguration | Control plane only — no real Spark/Hadoop; clusters start in WAITING (KeepAlive=true) or TERMINATED (KeepAlive=false); steps stored as COMPLETED immediately; all three instance modes (simple, InstanceGroups, InstanceFleets); TerminationProtected enforced; Pro-only on LocalStack — free here |
| **Cognito** | **User Pools**: CreateUserPool, DeleteUserPool, DescribeUserPool, ListUserPools, UpdateUserPool, CreateUserPoolClient, DeleteUserPoolClient, DescribeUserPoolClient, ListUserPoolClients, UpdateUserPoolClient, AdminCreateUser, AdminDeleteUser, AdminGetUser, ListUsers, AdminSetUserPassword, AdminUpdateUserAttributes, AdminConfirmSignUp, AdminDisableUser, AdminEnableUser, AdminResetUserPassword, AdminUserGlobalSignOut, AdminAddUserToGroup, AdminRemoveUserFromGroup, AdminListGroupsForUser, AdminListUserAuthEvents, AdminInitiateAuth, AdminRespondToAuthChallenge, InitiateAuth, RespondToAuthChallenge, GlobalSignOut, RevokeToken, SignUp, ConfirmSignUp, ForgotPassword, ConfirmForgotPassword, ChangePassword, GetUser, UpdateUserAttributes, DeleteUser, CreateGroup, DeleteGroup, GetGroup, ListGroups, ListUsersInGroup, CreateUserPoolDomain, DeleteUserPoolDomain, DescribeUserPoolDomain, GetUserPoolMfaConfig, SetUserPoolMfaConfig, AssociateSoftwareToken, VerifySoftwareToken, AdminSetUserMFAPreference, SetUserMFAPreference, TagResource, UntagResource, ListTagsForResource; **Identity Pools**: CreateIdentityPool, DeleteIdentityPool, DescribeIdentityPool, ListIdentityPools, UpdateIdentityPool, GetId, GetCredentialsForIdentity, GetOpenIdToken, SetIdentityPoolRoles, GetIdentityPoolRoles, ListIdentities, DescribeIdentity, MergeDeveloperIdentities, UnlinkDeveloperIdentity, UnlinkIdentity, TagResource, UntagResource, ListTagsForResource; **OAuth2**: /oauth2/token (client_credentials) | Stub JWT tokens (structurally valid base64url JWTs); SRP auth returns PASSWORD_VERIFIER challenge; confirmation codes hardcoded (signup: 123456, forgot-password: 654321); TOTP SOFTWARE_TOKEN_MFA challenge flow; MFA config and per-user enrollment stored in-memory |
| **ECR** | CreateRepository, DescribeRepositories, DeleteRepository, ListImages, DescribeImages, PutImage, BatchGetImage, BatchDeleteImage, GetAuthorizationToken, GetRepositoryPolicy, SetRepositoryPolicy, DeleteRepositoryPolicy, PutLifecyclePolicy, GetLifecyclePolicy, DeleteLifecyclePolicy, ListTagsForResource, TagResource, UntagResource, PutImageTagMutability, PutImageScanningConfiguration, DescribeRegistry, GetDownloadUrlForLayer, BatchCheckLayerAvailability, InitiateLayerUpload, UploadLayerPart, CompleteLayerUpload | In-memory image registry; Docker V2 manifest support; authorization token generation; lifecycle policies; tag mutability; Pro-only on LocalStack — free here |
| **AppSync** | CreateGraphQLApi, GetGraphQLApi, ListGraphQLApis, UpdateGraphQLApi, DeleteGraphQLApi, CreateApiKey, DeleteApiKey, ListApiKeys, CreateDataSource, GetDataSource, ListDataSources, DeleteDataSource, CreateResolver, GetResolver, ListResolvers, DeleteResolver, CreateType, ListTypes, GetType, TagResource, UntagResource, ListTagsForResource | Control plane + data plane; GraphQL queries/mutations execute against DynamoDB resolvers (create/get/list/update/delete); Lambda resolvers supported; designed for Amplify/CDK CRUD patterns — not a full GraphQL spec engine |
| **Cloud Map** | CreateHttpNamespace, CreatePrivateDnsNamespace, CreatePublicDnsNamespace, GetNamespace, ListNamespaces, DeleteNamespace, UpdateHttpNamespace, UpdatePrivateDnsNamespace, UpdatePublicDnsNamespace, CreateService, GetService, ListServices, DeleteService, UpdateService, RegisterInstance, DeregisterInstance, DiscoverInstances, DiscoverInstancesRevision, ListInstances, GetInstancesHealthStatus, UpdateInstanceCustomHealthStatus, GetServiceAttributes, UpdateServiceAttributes, DeleteServiceAttributes, GetOperation, ListOperations, TagResource, UntagResource, ListTagsForResource | DNS namespaces create Route53 hosted zones; operation tracking; Terraform `aws_service_discovery_*` compatible |
| **S3 Files** | CreateFileSystem, GetFileSystem, ListFileSystems, DeleteFileSystem, CreateMountTarget, GetMountTarget, ListMountTargets, UpdateMountTarget, DeleteMountTarget, CreateAccessPoint, GetAccessPoint, ListAccessPoints, DeleteAccessPoint, GetFileSystemPolicy, PutFileSystemPolicy, DeleteFileSystemPolicy, GetSynchronizationConfiguration, PutSynchronizationConfiguration, TagResource, UntagResource, ListTagsForResource | 21 operations; control plane for the new S3 Files service (launched April 2026); file systems, mount targets, access points, policies |

---

## Real Database Endpoints (RDS)

When you create an RDS instance, MiniStack starts a real database container and returns the actual connection endpoint:

```python
import boto3
import psycopg2  # pip install psycopg2-binary

rds = boto3.client("rds", endpoint_url="http://localhost:4566",
                   aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")

resp = rds.create_db_instance(
    DBInstanceIdentifier="mydb",
    DBInstanceClass="db.t3.micro",
    Engine="postgres",
    MasterUsername="admin",
    MasterUserPassword="password",
    DBName="appdb",
    AllocatedStorage=20,
)

endpoint = resp["DBInstance"]["Endpoint"]
# Connect directly — it's a real Postgres instance
conn = psycopg2.connect(
    host=endpoint["Address"],   # localhost
    port=endpoint["Port"],      # 15432 (auto-assigned)
    user="admin",
    password="password",
    dbname="appdb",
)
```

Supported engines: `postgres`, `mysql`, `mariadb`, `aurora-postgresql`, `aurora-mysql`

---

## Real Redis Endpoints (ElastiCache)

```python
import boto3
import redis  # pip install redis

ec = boto3.client("elasticache", endpoint_url="http://localhost:4566",
                  aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")

resp = ec.create_cache_cluster(
    CacheClusterId="my-redis",
    Engine="redis",
    CacheNodeType="cache.t3.micro",
    NumCacheNodes=1,
)

node = resp["CacheCluster"]["CacheNodes"][0]["Endpoint"]
r = redis.Redis(host=node["Address"], port=node["Port"])
r.set("key", "value")
print(r.get("key"))  # b'value'
```

A Redis sidecar is also always available at `localhost:6379` when using Docker Compose.

---

## Athena with Real SQL

Athena queries run via DuckDB and can query files in your local S3 data directory:

```python
import boto3, time

athena = boto3.client("athena", endpoint_url="http://localhost:4566",
                      aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")

# Query runs real SQL via DuckDB
resp = athena.start_query_execution(
    QueryString="SELECT 42 AS answer, 'hello' AS greeting",
    ResultConfiguration={"OutputLocation": "s3://athena-results/"},
)
query_id = resp["QueryExecutionId"]

# Poll for result
while True:
    status = athena.get_query_execution(QueryExecutionId=query_id)
    if status["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
        break
    time.sleep(0.1)

results = athena.get_query_results(QueryExecutionId=query_id)
for row in results["ResultSet"]["Rows"][1:]:  # skip header
    print([col["VarCharValue"] for col in row["Data"]])
# ['42', 'hello']
```

---

## ECS with Real Containers

```python
import boto3

ecs = boto3.client("ecs", endpoint_url="http://localhost:4566",
                   aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")

ecs.create_cluster(clusterName="dev")

ecs.register_task_definition(
    family="web",
    containerDefinitions=[{
        "name": "nginx",
        "image": "nginx:alpine",
        "cpu": 128,
        "memory": 256,
        "portMappings": [{"containerPort": 80, "hostPort": 8080}],
    }],
)

# This actually runs an nginx container via Docker
resp = ecs.run_task(cluster="dev", taskDefinition="web", count=1)
task_arn = resp["tasks"][0]["taskArn"]

# Stop it (removes the container)
ecs.stop_task(cluster="dev", task=task_arn)
```

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_PORT` | `4566` | Port to listen on. Also accepts `EDGE_PORT` (LocalStack compatibility alias) |
| `MINISTACK_HOST` | `localhost` | Hostname used in response URLs (SQS queues, SNS subscriptions, API Gateway endpoints, Lambda layers) |
| `MINISTACK_REGION` | `us-east-1` | AWS region reported in ARNs and service responses across all services |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `S3_PERSIST` | `0` | Set `1` to persist S3 objects to disk |
| `S3_DATA_DIR` | `/tmp/ministack-data/s3` | S3 persistence directory |
| `REDIS_HOST` | `redis` | Redis host for ElastiCache fallback |
| `REDIS_PORT` | `6379` | Redis port for ElastiCache fallback |
| `RDS_BASE_PORT` | `15432` | Starting host port for RDS containers |
| `RDS_TMPFS_SIZE` | `256m` | Tmpfs size for RDS database containers. Set to `2g` or higher for large databases |
| `ELASTICACHE_BASE_PORT` | `16379` | Starting host port for ElastiCache containers |
| `PERSIST_STATE` | `0` | Set `1` to persist service state across restarts |
| `STATE_DIR` | `/tmp/ministack-state` | Directory for persisted state files |
| `LAMBDA_EXECUTOR` | `local` | Lambda execution mode: `local` (subprocess) or `docker` (container). `provided` runtimes and `PackageType: Image` always use Docker |
| `LAMBDA_DOCKER_NETWORK` | _(unset)_ | Docker network for Lambda containers. Set to your Docker Compose network name so Lambda can reach MiniStack |
| `SFN_MOCK_CONFIG` | _(unset)_ | Path to JSON file for Step Functions mock testing; compatible with AWS SFN Local format. Also accepts `LOCALSTACK_SFN_MOCK_CONFIG` |
| `ATHENA_ENGINE` | `auto` | SQL engine for Athena: `auto`, `duckdb`, `mock` |
| `SMTP_HOST` | _(unset)_ | SMTP server for SES email relay (e.g. `mailhog:1025`). When set, SES SendEmail/SendRawEmail actually deliver mail. When unset, emails are stored in-memory only |

### Startup Scripts

MiniStack supports two types of init scripts:

- **`/docker-entrypoint-initaws.d/*.sh`** — run before the server starts (for installing tools)
- **`/docker-entrypoint-initaws.d/ready.d/*.sh`** — run after the server is ready and accepting connections (for seeding AWS resources)

```bash
# ready.d/01-create-resources.sh
aws --endpoint-url=http://localhost:4566 s3 mb s3://my-bucket
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name my-queue
```

### Athena SQL Engines

Set `ATHENA_ENGINE` to control Athena's SQL execution engine. In `auto` mode, DuckDB is used if installed, otherwise queries return mock results.

| Capability | `duckdb` | `mock` |
|---|---|---|
| Simple SELECT / expressions | Yes | Partial (regex) |
| Arithmetic, aggregations, JOINs, CTEs | Yes | No |
| Window functions, subqueries | Yes | No |
| Parquet / CSV / JSON file queries | Yes | No |
| UNNEST, ARRAY, MAP functions | Yes | No |
| APPROX\_DISTINCT, REGEXP\_EXTRACT | Yes | No |

Install DuckDB for full Athena SQL compatibility: `pip install ministack[full]`.

### State Persistence

When `PERSIST_STATE=1`, MiniStack saves service state to `STATE_DIR` on shutdown and reloads it on startup. Writes are atomic (write-to-tmp then rename) to prevent corruption on crash.

Services currently supporting persistence: **All 36+ services** — API Gateway v1/v2, ALB, ACM, AppSync, Athena, Cloud Map, CloudFront, CloudWatch, CloudWatch Logs, Cognito, DynamoDB, EC2, ECR, ECS, EFS, ElastiCache, EMR, EventBridge, Firehose, Glue, IAM/STS, Kinesis, KMS, Lambda, RDS, Route 53, S3, Secrets Manager, SES, SES v2, SNS, SQS, SSM, Step Functions, WAF v2

```bash
docker run -p 4566:4566 \
  -e PERSIST_STATE=1 \
  -e STATE_DIR=/data/ministack-state \
  -v /tmp/ministack-data:/data \
  nahuelnucera/ministack
```

### Lambda Warm Starts

MiniStack keeps Python and Node.js Lambda functions warm between invocations. After the first call (cold start), the handler module stays loaded in a persistent subprocess. Subsequent calls skip the import/require step, matching real AWS warm-start behaviour and making test suites significantly faster.

### Lambda Node.js Runtimes

MiniStack supports Node.js Lambda runtimes (`nodejs14.x`, `nodejs16.x`, `nodejs18.x`, `nodejs20.x`, `nodejs22.x`). Functions execute via a local `node` subprocess (or Docker when `LAMBDA_EXECUTOR=docker`) — no mocking, real JS execution.

```python
import boto3, json, zipfile, io

lam = boto3.client("lambda", endpoint_url="http://localhost:4566", region_name="us-east-1",
                   aws_access_key_id="test", aws_secret_access_key="test")

code = "exports.handler = async (event) => ({ statusCode: 200, body: JSON.stringify(event) });"
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w") as zf:
    zf.writestr("index.js", code)

lam.create_function(
    FunctionName="my-node-fn",
    Runtime="nodejs20.x",
    Role="arn:aws:iam::000000000000:role/r",
    Handler="index.handler",
    Code={"ZipFile": buf.getvalue()},
)

resp = lam.invoke(FunctionName="my-node-fn", Payload=json.dumps({"hello": "world"}))
print(json.loads(resp["Payload"].read()))  # {'statusCode': 200, 'body': '{"hello": "world"}'}
```

Layers that ship npm packages work too — MiniStack resolves the `nodejs/node_modules` subdirectory inside each layer zip and prepends it to the module search path.

---

## Architecture

```
                    ┌──────────────────────────────────────────┐
 AWS CLI / boto3    │         ASGI Gateway  :4566              │
 Terraform / CDK ──►│  ┌────────────────────────────────────┐  │
 Any AWS SDK        │  │          Request Router            │  │
                    │  │  1. X-Amz-Target header            │  │
                    │  │  2. Authorization credential scope │  │
                    │  │  3. Action query param             │  │
                    │  │  4. URL path pattern               │  │
                    │  │  5. Host header                    │  │
                    │  │  6. Default → S3                   │  │
                    │  └────────────────┬───────────────────┘  │
                    │                 │                        │
                    │  ┌────────────────────────────────────┐  │
                    │  │         Service Handlers           │  │
                    │  │                                    │  │
                    │  │  S3      SQS    SNS    DynamoDB    │  │
                    │  │  Lambda  IAM    STS    Secrets     │  │
                    │  │  SSM     EventBridge   Kinesis     │  │
                    │  │  CW Logs   CW Metrics  SES  SESv2  │  │
                    │  │  Step Functions  API GW v1/v2      │  │
                    │  │  ECS   RDS   ElastiCache   Glue    │  │
                    │  │  Athena   Firehose   Route53       │  │
                    │  │  Cognito  EC2   EMR   EBS   EFS    │  │
                    │  │  ALB/ELBv2   ACM   WAF v2          │  │
                    │  │  CloudFormation  KMS  ECR          │  │
                    │  │  CloudFront   AppSync              │  │
                    │  └────────────────────────────────────┘  │
                    │                                          │
                    │  In-Memory Storage + Optional Docker     │
                    └──────────────────────────────────────────┘
                                        │
                         ┌──────────────┼──────────────┐
                         ▼              ▼              ▼
                    Redis:6379    Postgres:15432+  MySQL:15433+
                    (ElastiCache)    (RDS)           (RDS)
```

---

## Running Tests

```bash
# Install test dependencies
pip install boto3 pytest duckdb docker cbor2

# Start MiniStack
docker compose up -d

# Run the full test suite (955 tests across all 38 services)
pytest tests/ -v
```

Expected output:

```
collected 955 items

tests/test_services.py::test_s3_create_bucket PASSED
...
tests/test_services.py::test_app_asgi_callable PASSED

955 passed in ~100s
```

---

## Terraform / CDK / Pulumi

### Terraform

Works with both Terraform AWS Provider v5 and v6.

```hcl
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    acm             = "http://localhost:4566"
    apigateway      = "http://localhost:4566"
    appsync         = "http://localhost:4566"
    athena          = "http://localhost:4566"
    cloudformation  = "http://localhost:4566"
    cloudwatch      = "http://localhost:4566"
    cognitoidentity = "http://localhost:4566"
    cognitoidp      = "http://localhost:4566"
    dynamodb        = "http://localhost:4566"
    ec2             = "http://localhost:4566"
    ecr             = "http://localhost:4566"
    ecs             = "http://localhost:4566"
    efs             = "http://localhost:4566"
    elasticache     = "http://localhost:4566"
    elbv2           = "http://localhost:4566"
    emr             = "http://localhost:4566"
    events          = "http://localhost:4566"
    firehose        = "http://localhost:4566"
    glue            = "http://localhost:4566"
    iam             = "http://localhost:4566"
    kinesis         = "http://localhost:4566"
    kms             = "http://localhost:4566"
    lambda          = "http://localhost:4566"
    logs            = "http://localhost:4566"
    rds             = "http://localhost:4566"
    route53         = "http://localhost:4566"
    s3              = "http://localhost:4566"
    s3control       = "http://localhost:4566"
    secretsmanager  = "http://localhost:4566"
    ses             = "http://localhost:4566"
    sesv2           = "http://localhost:4566"
    sns             = "http://localhost:4566"
    sqs             = "http://localhost:4566"
    ssm             = "http://localhost:4566"
    stepfunctions   = "http://localhost:4566"
    sts             = "http://localhost:4566"
    wafv2           = "http://localhost:4566"
    cloudfront      = "http://localhost:4566"
  }
}
```

**Terraform VPC module** — fully supported (v6.6.0):

```hcl
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "6.6.0"

  name = "my-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["us-east-1a", "us-east-1b", "us-east-1c"]
  private_subnets = ["10.0.0.0/20", "10.0.16.0/20", "10.0.32.0/20"]
  public_subnets  = ["10.0.64.0/20", "10.0.80.0/20", "10.0.96.0/20"]

  enable_nat_gateway = true
  single_nat_gateway = true
}
```

Creates VPC with per-VPC default network ACL, security group, and main route table. All 23 resources (subnets, IGW, NAT, route tables, associations, routes, default resources) supported.

### AWS CDK

```typescript
// cdk.json or in your app
const app = new cdk.App();
// Set endpoint override via environment
process.env.AWS_ENDPOINT_URL = "http://localhost:4566";
```

### Pulumi

```yaml
# Pulumi.dev.yaml
config:
  aws:endpoints:
    - s3: http://localhost:4566
      dynamodb: http://localhost:4566
      # ... etc
```

### Amplify / CDK

MiniStack supports Amplify Gen 2 and CDK deployments. The underlying services are fully emulated:

- **Auth** — Cognito User Pools with JWKS/OIDC endpoints (`/.well-known/jwks.json`) for real JWT validation
- **Data** — AppSync GraphQL queries/mutations execute against DynamoDB resolvers (create/get/list/update/delete)
- **Storage** — S3
- **Functions** — Lambda (Python + Node.js)

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
npx ampx sandbox
```

> **Note:** AppSync supports Amplify-style CRUD operations. Advanced GraphQL features (fragments, unions, subscriptions) are not supported.

### Testcontainers (Java / Go / Python)

See [`Testcontainers/java-testcontainers`](Testcontainers/java-testcontainers), [`Testcontainers/go-testcontainers`](Testcontainers/go-testcontainers), and [`Testcontainers/python-testcontainers`](Testcontainers/python-testcontainers) for ready-to-run integration tests using Testcontainers with the AWS SDK v2.

---

## Comparison

| Feature | MiniStack | LocalStack Free | LocalStack Pro |
|---------|-----------|-----------------|----------------|
| S3, SQS, SNS, DynamoDB | ✅ | ✅ | ✅ |
| Lambda (Python + Node.js execution) | ✅ | ✅ | ✅ |
| IAM, STS, SecretsManager | ✅ | ✅ | ✅ |
| CloudWatch Logs | ✅ | ✅ | ✅ |
| SSM Parameter Store | ✅ | ✅ | ✅ |
| EventBridge | ✅ | ✅ | ✅ |
| Kinesis | ✅ | ✅ | ✅ |
| SES | ✅ | ✅ | ✅ |
| Step Functions | ✅ | ✅ | ✅ |
| **RDS (real DB containers)** | ✅ | ❌ | ✅ |
| **ElastiCache (real Redis)** | ✅ | ❌ | ✅ |
| **ECS (real Docker containers)** | ✅ | ❌ | ✅ |
| **Athena (real SQL via DuckDB)** | ✅ | ❌ | ✅ |
| **Glue Data Catalog + Jobs** | ✅ | ❌ | ✅ |
| **API Gateway v2 (HTTP API)** | ✅ | ✅ | ✅ |
| **API Gateway v1 (REST API)** | ✅ | ✅ | ✅ |
| **Firehose** | ✅ | ✅ | ✅ |
| **Route53** | ✅ | ✅ | ✅ |
| **Cognito** | ✅ | ✅ | ✅ |
| **EC2** | ✅ | ✅ | ✅ |
| **EMR** | ✅ | Paid | ✅ |
| **ELBv2 / ALB** | ✅ | ✅ | ✅ |
| **EBS** | ✅ | Paid | ✅ |
| **EFS** | ✅ | Paid | ✅ |
| **ACM** | ✅ | ✅ | ✅ |
| **SES v2** | ✅ | ✅ | ✅ |
| **WAF v2** | ✅ | Paid | ✅ |
| **CloudFormation** | **partial** | partial | ✅ Free |
| **KMS** | ✅ | Paid | ✅ Free |
| **ECR** | ✅ | ✅ | ✅ |
| **CloudFront** | ✅ | Paid | ✅ |
| **AppSync** | ✅ | NO | ✅ |
| **Cloud Map** | ✅ | ❌ | ✅ |
| **S3 Files** | ✅ | ❌ | ❌ |
| Cost | **Free forever** | Was free, now paid | $35+/mo |
| Docker image size | ~250MB | ~1GB | ~1GB |
| Memory at idle | ~40MB | ~500MB | ~500MB |
| Startup time | <1s | ~15-30s | ~15-30s |
| License | MIT | BSL (restricted) | Proprietary |

---

## Community Integrations

| Project | Description |
|---------|-------------|
| [**StackPort**](https://github.com/DaviReisVieira/stackport) | Visual dashboard to browse and inspect AWS resources in MiniStack. Available on [PyPI](https://pypi.org/project/stackport/) and [Docker Hub](https://hub.docker.com/r/davireis/stackport). |
| [**McDoit.Aspire.Hosting.Ministack**](https://github.com/McDoit/aspire-hosting-ministack) | .NET Aspire hosting integration for MiniStack. |

---

## Contributing

PRs welcome. The codebase is intentionally simple — each service is a single self-contained Python file in `ministack/services/`. Adding a new service means:

1. Create `ministack/services/myservice.py` with an `async def handle_request(...)` function and a `reset()` function
2. Add it to `SERVICE_HANDLERS` in `ministack/app.py`
3. Add detection patterns to `ministack/core/router.py`
4. Add a fixture to `tests/conftest.py` and tests to `tests/test_services.py`

See [CONTRIBUTING.md](CONTRIBUTING.md) for a full walkthrough.

---

## License

MIT — free to use, modify, and distribute. No restrictions.

```
Copyright (c) 2026 MiniStack Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
```
