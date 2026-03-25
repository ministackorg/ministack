# MiniStack — Free, Open-Source Local AWS Emulator



> **LocalStack is no longer free.** MiniStack is a fully open-source, zero-cost drop-in replacement.
> Single port · No account · No license key · No telemetry · Just AWS APIs, locally.

![GitHub release](https://img.shields.io/github/v/release/Nahuel990/ministack)
![Build](https://img.shields.io/github/actions/workflow/status/Nahuel990/ministack/ci.yml?branch=master)
![Docker Pulls](https://img.shields.io/docker/pulls/nahuelnucera/ministack)
![Docker Image Size](https://img.shields.io/docker/image-size/nahuelnucera/ministack/latest)
![License](https://img.shields.io/github/license/Nahuel990/ministack)
![Python](https://img.shields.io/badge/python-3.12-blue)
![GitHub stars](https://img.shields.io/github/stars/Nahuel990/ministack)


<p align="center">
  <img src="ministack1.png" alt="MiniStack in action" width="700"/>
</p>

---

## Why MiniStack?

LocalStack recently moved its core services behind a paid plan. If you relied on LocalStack Community for local development and CI/CD pipelines, MiniStack is your free alternative.

- **20 AWS services** emulated on a single port (4566)
- **Drop-in compatible** — works with `boto3`, AWS CLI, Terraform, CDK, Pulumi, any SDK
- **Real infrastructure** — RDS spins up actual Postgres/MySQL containers, ElastiCache spins up real Redis, Athena runs real SQL via DuckDB, ECS runs real Docker containers
- **Tiny footprint** — ~150MB image, ~30MB RAM at idle vs LocalStack's ~1GB image and ~500MB RAM
- **Fast startup** — under 2 seconds
- **MIT licensed** — use it, fork it, contribute to it

---

## Quick Start

```bash
# Option 1: Docker Hub (recommended)
docker run -p 4566:4566 nahuelnucera/ministack

# Option 2: Clone and build
git clone https://github.com/Nahuel990/ministack
cd ministack

# Start with Docker Compose (includes Redis sidecar)
docker compose up -d

# Verify
curl http://localhost:4566/_localstack/health
```

That's it. No account, no API key, no sign-up.

---

## Using with AWS CLI

```bash
# Configure a local profile (one-time)
aws configure --profile local
# AWS Access Key ID: test
# AWS Secret Access Key: test
# Default region: us-east-1
# Default output format: json

# Use --endpoint-url on any command
aws --endpoint-url=http://localhost:4566 s3 mb s3://my-bucket
aws --endpoint-url=http://localhost:4566 s3 cp ./file.txt s3://my-bucket/
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name my-queue
aws --endpoint-url=http://localhost:4566 dynamodb list-tables
aws --endpoint-url=http://localhost:4566 sts get-caller-identity

# Or set the endpoint globally for a session
export AWS_ENDPOINT_URL=http://localhost:4566
aws s3 ls
aws sqs list-queues
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
```

---

## Supported Services

### Core Services

| Service | Operations | Notes |
|---------|-----------|-------|
| **S3** | CreateBucket, DeleteBucket, ListBuckets, HeadBucket, PutObject, GetObject, DeleteObject, HeadObject, CopyObject, ListObjects v1/v2, DeleteObjects | Optional disk persistence via `S3_PERSIST=1` |
| **SQS** | CreateQueue, DeleteQueue, ListQueues, GetQueueUrl, GetQueueAttributes, SetQueueAttributes, PurgeQueue, SendMessage, ReceiveMessage, DeleteMessage, ChangeMessageVisibility, SendMessageBatch, DeleteMessageBatch | Both Query API and JSON protocol |
| **SNS** | CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes, Subscribe, Unsubscribe, ListSubscriptions, ListSubscriptionsByTopic, Publish | |
| **DynamoDB** | CreateTable, DeleteTable, DescribeTable, ListTables, PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, BatchWriteItem, BatchGetItem | |
| **Lambda** | CreateFunction, DeleteFunction, GetFunction, ListFunctions, Invoke, UpdateFunctionCode, UpdateFunctionConfiguration | Python runtimes actually execute; others return mock |
| **IAM** | CreateUser, GetUser, ListUsers, DeleteUser, CreateRole, GetRole, ListRoles, DeleteRole, CreatePolicy, AttachRolePolicy, PutRolePolicy, CreateAccessKey, ListAccessKeys | |
| **STS** | GetCallerIdentity, AssumeRole, GetSessionToken | |
| **SecretsManager** | CreateSecret, GetSecretValue, ListSecrets, DeleteSecret, UpdateSecret, DescribeSecret, PutSecretValue | |
| **CloudWatch Logs** | CreateLogGroup, DeleteLogGroup, DescribeLogGroups, CreateLogStream, DeleteLogStream, DescribeLogStreams, PutLogEvents, GetLogEvents, FilterLogEvents | |

### Extended Services

| Service | Operations | Notes |
|---------|-----------|-------|
| **SSM Parameter Store** | PutParameter, GetParameter, GetParameters, GetParametersByPath, DeleteParameter, DeleteParameters, DescribeParameters | Supports String, SecureString, StringList |
| **EventBridge** | CreateEventBus, DeleteEventBus, ListEventBuses, PutRule, DeleteRule, ListRules, DescribeRule, EnableRule, DisableRule, PutTargets, RemoveTargets, ListTargetsByRule, PutEvents | |
| **Kinesis** | CreateStream, DeleteStream, DescribeStream, ListStreams, ListShards, PutRecord, PutRecords, GetShardIterator, GetRecords | Partition key → shard routing |
| **CloudWatch Metrics** | PutMetricData, GetMetricStatistics, ListMetrics, PutMetricAlarm, DescribeAlarms, DeleteAlarms, SetAlarmState | |
| **SES** | SendEmail, SendRawEmail, VerifyEmailIdentity, ListIdentities, GetIdentityVerificationAttributes, DeleteIdentity, GetSendQuota, GetSendStatistics | Emails stored in-memory, not sent |
| **Step Functions** | CreateStateMachine, DeleteStateMachine, DescribeStateMachine, UpdateStateMachine, ListStateMachines, StartExecution, StopExecution, DescribeExecution, ListExecutions, GetExecutionHistory | |

### Infrastructure Services (with real Docker execution)

| Service | Operations | Real Execution |
|---------|-----------|----------------|
| **ECS** | CreateCluster, DeleteCluster, DescribeClusters, ListClusters, RegisterTaskDefinition, DeregisterTaskDefinition, ListTaskDefinitions, CreateService, DeleteService, DescribeServices, UpdateService, ListServices, RunTask, StopTask, DescribeTasks, ListTasks | `RunTask` starts real Docker containers via Docker socket |
| **RDS** | CreateDBInstance, DeleteDBInstance, DescribeDBInstances, StartDBInstance, StopDBInstance, RebootDBInstance, ModifyDBInstance, CreateDBCluster, DeleteDBCluster, DescribeDBClusters, CreateDBSubnetGroup, DescribeDBSubnetGroups, CreateDBParameterGroup, DescribeDBParameterGroups, DescribeDBEngineVersions | `CreateDBInstance` spins up real Postgres/MySQL Docker container, returns actual `host:port` endpoint |
| **ElastiCache** | CreateCacheCluster, DeleteCacheCluster, DescribeCacheClusters, ModifyCacheCluster, CreateReplicationGroup, DeleteReplicationGroup, DescribeReplicationGroups, CreateCacheSubnetGroup, DescribeCacheSubnetGroups, CreateCacheParameterGroup, DescribeCacheParameterGroups, DescribeCacheEngineVersions | `CreateCacheCluster` spins up real Redis/Memcached Docker container |
| **Glue** | CreateDatabase, DeleteDatabase, GetDatabase, GetDatabases, CreateTable, DeleteTable, GetTable, GetTables, UpdateTable, BatchDeleteTable, CreatePartition, GetPartitions, BatchCreatePartition, CreateConnection, GetConnections, CreateCrawler, GetCrawler, StartCrawler, CreateJob, GetJob, GetJobs, StartJobRun, GetJobRun, GetJobRuns | Python shell jobs actually execute via subprocess |
| **Athena** | StartQueryExecution, GetQueryExecution, GetQueryResults, StopQueryExecution, ListQueryExecutions, CreateWorkGroup, DeleteWorkGroup, GetWorkGroup, ListWorkGroups, CreateNamedQuery, DeleteNamedQuery, GetNamedQuery, ListNamedQueries | Real SQL via **DuckDB** when installed (`pip install duckdb`), otherwise returns mock results |

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
| `GATEWAY_PORT` | `4566` | Port to listen on |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `S3_PERSIST` | `0` | Set `1` to persist S3 objects to disk |
| `S3_DATA_DIR` | `/tmp/localstack-data/s3` | S3 persistence directory |
| `REDIS_HOST` | `redis` | Redis host for ElastiCache fallback |
| `REDIS_PORT` | `6379` | Redis port for ElastiCache fallback |
| `RDS_BASE_PORT` | `15432` | Starting host port for RDS containers |
| `ELASTICACHE_BASE_PORT` | `16379` | Starting host port for ElastiCache containers |

---

## Architecture

```
                    ┌─────────────────────────────────────────┐
 AWS CLI / boto3    │         ASGI Gateway  :4566             │
 Terraform / CDK ──►│  ┌───────────────────────────────────┐  │
 Any AWS SDK        │  │          Request Router            │  │
                    │  │  1. X-Amz-Target header            │  │
                    │  │  2. Authorization credential scope  │  │
                    │  │  3. Action query param              │  │
                    │  │  4. URL path pattern                │  │
                    │  │  5. Host header                     │  │
                    │  │  6. Default → S3                    │  │
                    │  └──────────────┬────────────────────┘  │
                    │                 │                        │
                    │  ┌──────────────▼──────────────────┐    │
                    │  │         Service Handlers         │    │
                    │  │                                  │    │
                    │  │  S3   SQS   SNS   DynamoDB       │    │
                    │  │  Lambda  IAM  STS  Secrets       │    │
                    │  │  SSM  EventBridge  Kinesis        │    │
                    │  │  CloudWatch  SES  StepFunctions  │    │
                    │  │  ECS   RDS   ElastiCache          │    │
                    │  │  Glue  Athena                     │    │
                    │  └──────────────────────────────────┘    │
                    │                                          │
                    │  In-Memory Storage + Optional Docker     │
                    └─────────────────────────────────────────┘
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

# Run the full test suite (56 tests across all 20 services)
pytest tests/ -v
```

Expected output:
```
collected 54 items

tests/test_services.py::test_s3_create_bucket PASSED
tests/test_services.py::test_s3_put_get PASSED
...
tests/test_services.py::test_athena_workgroup PASSED

56 passed in 7.6s
```

---

## Terraform / CDK / Pulumi

### Terraform

```hcl
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3             = "http://localhost:4566"
    sqs            = "http://localhost:4566"
    dynamodb       = "http://localhost:4566"
    lambda         = "http://localhost:4566"
    iam            = "http://localhost:4566"
    sts            = "http://localhost:4566"
    secretsmanager = "http://localhost:4566"
    ssm            = "http://localhost:4566"
    kinesis        = "http://localhost:4566"
    sns            = "http://localhost:4566"
    rds            = "http://localhost:4566"
    ecs            = "http://localhost:4566"
    glue           = "http://localhost:4566"
    athena         = "http://localhost:4566"
    elasticache    = "http://localhost:4566"
    stepfunctions  = "http://localhost:4566"
    cloudwatch     = "http://localhost:4566"
    logs           = "http://localhost:4566"
    events         = "http://localhost:4566"
    ses            = "http://localhost:4566"
  }
}
```

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

---

## Comparison

| Feature | MiniStack | LocalStack Free | LocalStack Pro |
|---------|-----------|-----------------|----------------|
| S3, SQS, SNS, DynamoDB | ✅ | ✅ | ✅ |
| Lambda (Python execution) | ✅ | ✅ | ✅ |
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
| API Gateway | ❌ | ✅ | ✅ |
| Cognito | ❌ | ✅ | ✅ |
| CloudFormation | ❌ | partial | ✅ |
| Cost | **Free** | Was free, now paid | $35+/mo |
| Docker image size | ~150MB | ~1GB | ~1GB |
| Memory at idle | ~30MB | ~500MB | ~500MB |
| Startup time | ~2s | ~15-30s | ~15-30s |
| License | MIT | BSL (restricted) | Proprietary |

---

## Contributing

PRs welcome. The codebase is intentionally simple — each service is a single self-contained Python file in `services/`. Adding a new service means:

1. Create `services/myservice.py` with an `async def handle_request(...)` function
2. Add it to `SERVICE_HANDLERS` in `app.py`
3. Add detection patterns to `core/router.py`
4. Add tests to `tests/test_services.py`

---

## License

MIT — free to use, modify, and distribute. No restrictions.

```
Copyright (c) 2024 MiniStack Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
```
