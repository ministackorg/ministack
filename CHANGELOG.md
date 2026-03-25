# Changelog

All notable changes to MiniStack will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [0.2.0] — 2026-03-25

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
- 377 integration tests — all passing (6 new tests covering the above gaps)

### Roadmap Update
The following roadmap items from v0.1.0 are now **completed**:
- ✅ API Gateway (REST + HTTP APIs) — HTTP API v2 delivered
- ✅ SNS → SQS fan-out delivery
- ✅ DynamoDB transactions (TransactWriteItems, TransactGetItems)
- ✅ S3 multipart upload
- ✅ SQS FIFO queues

---

## [0.1.0] — 2024-03-24

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

### [0.2.0] — Planned
- API Gateway (REST + HTTP APIs)
- SNS → SQS fan-out delivery
- DynamoDB transactions (TransactWriteItems, TransactGetItems)
- S3 multipart upload
- SQS FIFO queues

### [0.3.0] — Planned
- Cognito (user pools, sign-up/sign-in)
- Step Functions ASL interpreter (actually run state machines)
- Route53 (hosted zones, record sets)
- ACM (certificate management)
- Firehose
