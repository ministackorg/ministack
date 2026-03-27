# Changelog

All notable changes to MiniStack will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [1.0.7] — 2026-03-27

### Added
- **Amazon Data Firehose** (`services/firehose.py`) — full control and data plane
  - `CreateDeliveryStream`, `DeleteDeliveryStream`, `DescribeDeliveryStream`, `ListDeliveryStreams`
  - `PutRecord`, `PutRecordBatch` — base64-encoded record ingestion; S3-destination streams write records synchronously to the local S3 emulator
  - `UpdateDestination` — concurrency-safe via `CurrentDeliveryStreamVersionId` / `VersionId`
  - `TagDeliveryStream`, `UntagDeliveryStream`, `ListTagsForDeliveryStream`
  - `StartDeliveryStreamEncryption`, `StopDeliveryStreamEncryption`
  - Destination types: `ExtendedS3`, `S3` (deprecated alias), `HttpEndpoint`, `Redshift`, `OpenSearch`, `Splunk`, `Snowflake`, `Iceberg`
  - Credential scope: `kinesis-firehose`; target prefix: `Firehose_20150804`
  - AWS-compliant `DescribeDeliveryStream` response: `EncryptionConfiguration` always present in `ExtendedS3DestinationDescription` (default `NoEncryption`); `DeliveryStreamEncryptionConfiguration` only included when encryption is configured; `Source` block populated for `KinesisStreamAsSource` streams
  - `UpdateDestination` merges fields when destination type is unchanged; replaces fully on type change — matching AWS behaviour
  - 16 integration tests, all passing
- **Virtual-hosted style S3**: `{bucket}.localhost[:{port}]` host header routing — requests are rewritten to path-style and forwarded to the S3 handler; compatible with AWS SDK virtual-hosted endpoint configuration

### Fixed
- **DynamoDB expression evaluator short-circuit bug**: `OR`/`AND` operators in `ConditionExpression` and `FilterExpression` now always consume both operands' tokens before applying the logical result — Python's boolean short-circuit was skipping right-hand token consumption when the left operand was already truthy/falsy, causing `Invalid expression: Expected RPAREN, got NAME_REF` on expressions like `attribute_not_exists(#0) OR #1 <= :0` (reported by PynamoDB users with numeric `ExpressionAttributeNames` keys)

---

## [1.0.6] — 2026-03-27

### Added
- **API Gateway REST API v1** (`services/apigateway_v1.py`) — complete control plane and data plane
  - Full resource tree: `CreateRestApi`, `GetRestApi`, `GetRestApis`, `UpdateRestApi`, `DeleteRestApi`
  - Resources: `CreateResource`, `GetResource`, `GetResources`, `UpdateResource`, `DeleteResource`
  - Methods: `PutMethod`, `GetMethod`, `DeleteMethod`, `UpdateMethod`
  - Method responses: `PutMethodResponse`, `GetMethodResponse`, `DeleteMethodResponse`
  - Integrations: `PutIntegration`, `GetIntegration`, `DeleteIntegration`, `UpdateIntegration`
  - Integration responses: `PutIntegrationResponse`, `GetIntegrationResponse`, `DeleteIntegrationResponse`
  - Stages: `CreateStage`, `GetStage`, `GetStages`, `UpdateStage`, `DeleteStage`
  - Deployments: `CreateDeployment`, `GetDeployment`, `GetDeployments`, `UpdateDeployment`, `DeleteDeployment`
  - Authorizers: `CreateAuthorizer`, `GetAuthorizer`, `GetAuthorizers`, `UpdateAuthorizer`, `DeleteAuthorizer`
  - Models: `CreateModel`, `GetModel`, `GetModels`, `DeleteModel`
  - API keys: `CreateApiKey`, `GetApiKey`, `GetApiKeys`, `UpdateApiKey`, `DeleteApiKey`
  - Usage plans: `CreateUsagePlan`, `GetUsagePlan`, `GetUsagePlans`, `UpdateUsagePlan`, `DeleteUsagePlan`, `CreateUsagePlanKey`, `GetUsagePlanKeys`, `DeleteUsagePlanKey`
  - Domain names: `CreateDomainName`, `GetDomainName`, `GetDomainNames`, `DeleteDomainName`
  - Base path mappings: `CreateBasePathMapping`, `GetBasePathMapping`, `GetBasePathMappings`, `DeleteBasePathMapping`
  - Tags: `TagResource`, `UntagResource`, `GetTags`
  - Data plane: execute-api requests routed by host header (`{apiId}.execute-api.localhost`)
  - Lambda proxy format 1.0 (AWS_PROXY) — full `requestContext` with `requestTime`, `requestTimeEpoch`, `path`, `protocol`, `multiValueHeaders`; supports both apigateway URI form and plain `arn:aws:lambda:` ARN
  - HTTP proxy (HTTP_PROXY) forwarding to arbitrary HTTP backends
  - MOCK integration — selects response by `selectionPattern`, applies `responseParameters` to HTTP response headers, returns `responseTemplates` body
  - Resource tree path matching with `{param}` placeholders and `{proxy+}` greedy segments
  - JSON Patch support for all `PATCH` operations (`patchOperations`)
  - `CreateDeployment` populates `apiSummary` from all configured resources and methods
  - All timestamps (`createdDate`, `lastUpdatedDate`) returned as ISO 8601 strings — boto3 parses them as `datetime` objects
  - Error responses use `type` field matching AWS API Gateway v1 format
  - State persistence via `get_state()` / `load_persisted_state()`
  - v1 and v2 APIs coexist on the same port without conflict
- 434 integration tests — all passing, including against Docker image

---

## [1.0.5] — 2026-03-26

### Fixed
- **DynamoDB `UpdateItem` condition expression on missing item**: `ConditionExpression` such as `attribute_exists(...)` now correctly evaluates against the existing stored item (or empty if missing) — was incorrectly evaluating against the in-progress mutation, causing `ConditionalCheckFailedException` to never fire on missing items
- **DynamoDB key schema validation**: `GetItem`, `DeleteItem`, `UpdateItem`, `BatchWriteItem`, `BatchGetItem` now validate that supplied key attributes match the table schema in name and type — returns `ValidationException: The provided key element does not match the schema`
- **ESM visibility timeout**: SQS → Lambda event source mapping now respects the queue's configured `VisibilityTimeout` instead of hardcoding 30 s — prevents retry storms and duplicate deliveries when Lambda fails
- **Lambda stdout/stderr separation**: handler logs now go to stderr, response payload to stdout — matches AWS Lambda runtime contract; fixes log pollution in response payloads
- **Lambda timeout error**: `subprocess.TimeoutExpired` path now captures and returns stdout/stderr in the error log instead of returning an empty string
- **ECS `_maybe_mark_stopped` container status**: calls `container.reload()` before checking status to get live state from Docker — was reading stale cached status
- **ECS `stoppedAt`/`stoppingAt` timestamps**: now stored as ISO 8601 strings matching AWS ECS API format — was storing Unix epoch float
- **ECS cluster task count**: `_recount_cluster()` now recomputes running/pending counts from all tasks instead of decrementing — prevents count drift on concurrent task terminations
- **Step Functions service integrations**: Task state now dispatches to real MiniStack services via `arn:aws:states:::` resource URIs — `sqs:sendMessage`, `sns:publish`, `dynamodb:putItem`, `dynamodb:getItem`, `dynamodb:deleteItem`, `dynamodb:updateItem`, `ecs:runTask`, `ecs:runTask.sync` — was returning input passthrough instead of invoking the service
- 392 integration tests — all passing, including against Docker image

---

## [1.0.4] — 2026-03-26

### Fixed
- **SQS queue URL host/port**: `QueueUrl` values now read `MINISTACK_HOST` and `GATEWAY_PORT` env vars instead of hardcoding `localhost:4566` — fixes queue URLs when running behind a custom hostname or port
- 379 integration tests — all passing, including against Docker image

---

## [1.0.3] — 2026-03-25

### Fixed
- **Test port portability**: execute-api test URLs now read port from `MINISTACK_ENDPOINT` env var instead of hardcoding 4566 — fixes all execute-api tests when running against Docker on a non-default port
- **API Gateway Authorizers**: `CreateAuthorizer`, `GetAuthorizer`, `GetAuthorizers`, `UpdateAuthorizer`, `DeleteAuthorizer` — full CRUD for JWT and Lambda authorizers; state included in persistence snapshot
- **API Gateway `{proxy+}` greedy path matching**: `_path_matches` now handles `{param+}` placeholders matching multiple path segments (e.g. `/files/{proxy+}` matches `/files/a/b/c`)
- **API Gateway `routeKey` in Lambda event**: Lambda proxy event `routeKey` now reflects the matched route key (e.g. `"GET /ping"`) instead of always being `"$default"`
- **API Gateway Authorizer `identitySource` compliance**: field now stored and returned as array of strings (`["$request.header.Authorization"]`) matching AWS spec — was incorrectly a single string
- **Lambda `DeleteFunctionUrlConfig` response**: now returns 204 with empty body (was returning 204 with `{}` body, causing `RemoteDisconnected` in boto3)
- 377 integration tests — all passing, including against Docker image

---

## [1.0.2] — 2026-03-25

### Added

**API Gateway HTTP API v2** (completing roadmap item)
- Full control plane: CreateApi, GetApi, GetApis, UpdateApi, DeleteApi
- Routes: CreateRoute, GetRoute, GetRoutes, UpdateRoute, DeleteRoute
- Integrations: CreateIntegration, GetIntegration, GetIntegrations, UpdateIntegration, DeleteIntegration
- Stages: CreateStage, GetStage, GetStages, UpdateStage, DeleteStage
- Deployments: CreateDeployment, GetDeployment, GetDeployments, DeleteDeployment
- Tags: TagResource, UntagResource, GetTags
- Data plane: execute-api requests routed by host header (`{apiId}.execute-api.localhost`)
- Lambda proxy (AWS_PROXY) invocation via API Gateway v2 payload format 2.0
- HTTP proxy (HTTP_PROXY) forwarding to arbitrary HTTP backends
- Route path parameter matching (`{param}` placeholders in route keys)
- State persistence support via `get_state()` / `load_persisted_state()`

**SNS → SQS Fanout** (completing roadmap item)
- SNS subscriptions with `sqs` protocol deliver messages directly to SQS queues
- Message envelope follows AWS SNS JSON notification format
- Fanout is synchronous within the same process

**SQS → Lambda Event Source Mapping**
- `CreateEventSourceMapping` / `DeleteEventSourceMapping` / `GetEventSourceMapping` / `ListEventSourceMappings` / `UpdateEventSourceMapping`
- Background poller delivers SQS messages to Lambda functions as batched events
- Configurable batch size and enabled/disabled state

**Lambda Warm/Cold Start Worker Pool** (`core/lambda_runtime.py`)
- Persistent Python subprocess per function — handler module imported once (cold start)
- Subsequent invocations reuse the warm worker without re-importing
- Worker respawns automatically on crash
- Accurately models AWS Lambda cold/warm start behavior

**State Persistence Infrastructure** (`core/persistence.py`)
- `PERSIST_STATE=1` environment variable enables persistence
- `STATE_DIR` environment variable controls storage location (default `/tmp/ministack-state`)
- Atomic file writes (write-to-tmp then rename) prevent corruption on crash
- API Gateway state persisted across container restarts
- Persistence framework ready for other services to adopt

### Fixed
- `_path_matches` bug in API Gateway: `re.escape` was applied before `{param}` substitution,
  causing all parameterised routes to never match. Fixed by splitting on `{param}` segments,
  escaping literal parts, then joining with `[^/]+` wildcards.
- `execute-api` credential scope in `core/router.py` incorrectly mapped to `lambda`;
  corrected to `apigateway`.

### Infrastructure
- `app.py`: API Gateway registered in `SERVICE_HANDLERS`, BANNER, and `SERVICE_NAME_ALIASES`
- `app.py`: Execute-api data plane dispatched before normal service routing via host-header match
- `app.py`: Persistence load/save wired into ASGI lifespan startup/shutdown
- `core/router.py`: API Gateway patterns added; `/v2/apis` path detection added
- `tests/conftest.py`: `apigw` fixture added (`apigatewayv2` boto3 client)
- `tests/test_services.py`: fixed 4 tests that used hardcoded resource names and collided on repeated runs (`test_kinesis_stream_encryption`, `test_kinesis_enhanced_monitoring`, `test_sfn_start_sync_execution`, `test_sfn_describe_state_machine_for_execution`)
- `tests/test_services.py`: added 10 new tests covering previously untested paths — health endpoint, STS `GetSessionToken`, DynamoDB TTL enable/disable, Lambda warm start, API Gateway execute-api Lambda proxy, `$default` catch-all route, path parameter matching, 404 on missing route, EventBridge → Lambda target dispatch
- `tests/test_services.py`: added 25 new tests covering all new operations introduced since v0.1.0 — Kinesis `SplitShard`/`MergeShards`/`UpdateShardCount`/`RegisterStreamConsumer`/`DeregisterStreamConsumer`/`ListStreamConsumers`, SSM `LabelParameterVersion`/`AddTagsToResource`/`RemoveTagsFromResource`, CloudWatch Logs retention policy/subscription filters/metric filters/tag APIs/Insights, CloudWatch composite alarms/`DescribeAlarmsForMetric`/`DescribeAlarmHistory`, EventBridge archives/permissions, DynamoDB `UpdateTable`, S3 bucket versioning/encryption/lifecycle/CORS/ACL, Athena `UpdateWorkGroup`/`BatchGetNamedQuery`/`BatchGetQueryExecution`
- `README.md`: updated supported operations tables to reflect all new operations across all 21 services
- 371 integration tests — all passing (up from 54 in v0.1.0)

### Fixed (post-release patches)
- **SNS → Lambda fanout**: `protocol == "lambda"` subscriptions now invoke the Lambda function via `_execute_function()` with a standard `Records[].Sns` event envelope (was a no-op stub)
- **DynamoDB TTL enforcement**: background daemon thread (`dynamodb-ttl-reaper`) now scans every 60 s and deletes items whose TTL attribute value is ≤ current epoch time
- **Lambda Function URLs**: `CreateFunctionUrlConfig`, `GetFunctionUrlConfig`, `UpdateFunctionUrlConfig`, `DeleteFunctionUrlConfig`, `ListFunctionUrlConfigs` — full CRUD, persisted in `_function_urls` dict; was a 404 stub
- **`/_ministack/reset` disk cleanup**: when `PERSIST_STATE=1`, reset now also deletes `STATE_DIR/*.json` and `S3_DATA_DIR` contents so a subsequent restart does not reload old state
- **API Gateway `{proxy+}` greedy path matching**: `_path_matches` now handles `{param+}` placeholders matching multiple path segments (e.g. `/files/{proxy+}` matches `/files/a/b/c`)
- **API Gateway `routeKey` in Lambda event**: Lambda proxy event `routeKey` now reflects the matched route key (e.g. `"GET /ping"`) instead of always being `"$default"`
- **API Gateway Authorizers**: `CreateAuthorizer`, `GetAuthorizer`, `GetAuthorizers`, `UpdateAuthorizer`, `DeleteAuthorizer` — full CRUD for JWT and Lambda authorizers; state included in persistence snapshot
- **Test idempotency**: added `POST /_ministack/reset` endpoint and session-scoped `autouse` fixture so the test suite passes on repeated runs against the same server without restarting
- **API Gateway Authorizer `identitySource` compliance**: field now stored and returned as array of strings (`["$request.header.Authorization"]`) matching AWS spec — was incorrectly a single string
- **Lambda `DeleteFunctionUrlConfig` response**: now returns 204 with empty body (was returning 204 with `{}` body, causing `RemoteDisconnected` in boto3)
- **Test port portability**: execute-api test URLs now read port from `MINISTACK_ENDPOINT` env var instead of hardcoding 4566 — fixes all execute-api tests when running against Docker on a non-default port
- 377 integration tests — all passing, including against Docker image

### Roadmap Update
The following roadmap items from v0.1.0 are now **completed**:
- API Gateway (HTTP API v2) — full control and data plane delivered
- SNS → SQS fan-out delivery
- DynamoDB transactions (TransactWriteItems, TransactGetItems)
- S3 multipart upload
- SQS FIFO queues
- Step Functions ASL interpreter (Pass, Task, Choice, Wait, Succeed, Fail, Parallel, Map; Retry/Catch; waitForTaskToken)

---

## [1.0.1] — 2024-03-24

Initial public release. Built as a free, open-source alternative to LocalStack.

### Services Added

**Core (9 services)**
- S3 — CreateBucket, DeleteBucket, ListBuckets, HeadBucket, PutObject, GetObject, DeleteObject, HeadObject, CopyObject, ListObjects v1/v2, DeleteObjects (batch), optional disk persistence
- SQS — Full queue lifecycle, send/receive/delete, visibility timeout, batch operations, both Query API and JSON protocol
- SNS — Topics, subscriptions, publish
- DynamoDB — Tables, PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, BatchWriteItem, BatchGetItem
- Lambda — CRUD + actual Python function execution via subprocess
- IAM — Users, roles, policies, access keys
- STS — GetCallerIdentity, AssumeRole, GetSessionToken
- SecretsManager — Full secret lifecycle
- CloudWatch Logs — Log groups, streams, PutLogEvents, GetLogEvents, FilterLogEvents

**Extended (6 services)**
- SSM Parameter Store — PutParameter, GetParameter, GetParametersByPath, DeleteParameter
- EventBridge — Event buses, rules, targets, PutEvents
- Kinesis — Streams, shards, PutRecord, PutRecords, GetShardIterator, GetRecords
- CloudWatch Metrics — PutMetricData, GetMetricStatistics, ListMetrics, alarms
- SES — SendEmail, SendRawEmail, identity verification (emails stored, not sent)
- Step Functions — State machines, executions, history

**Infrastructure (5 services)**
- ECS — Clusters, task definitions, services, RunTask with real Docker container execution
- RDS — CreateDBInstance spins up real Postgres/MySQL Docker containers with actual endpoints
- ElastiCache — CreateCacheCluster spins up real Redis/Memcached Docker containers
- Glue — Full Data Catalog (databases, tables, partitions), crawlers, jobs with Python execution
- Athena — Real SQL execution via DuckDB, s3:// path rewriting to local files

### Infrastructure
- Single ASGI app on port 4566 (LocalStack-compatible)
- Docker Compose with Redis sidecar
- Multi-arch Docker image (amd64 + arm64)
- GitHub Actions CI (test on every push/PR)
- GitHub Actions Docker publish (on tag)
- 54 integration tests, all passing
- MIT license

---

## Roadmap

### Planned
- Cognito (user pools, sign-up/sign-in)
- Route53 (hosted zones, record sets)
- ACM (certificate management)
- Virtual-hosted style S3 (`bucket.localhost:4566` routing)

