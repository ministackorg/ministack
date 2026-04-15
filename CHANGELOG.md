# Changelog

All notable changes to MiniStack will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [1.2.16] — 2026-04-15

### Added
- **KMS ECC key support** — `CreateKey` now supports `ECC_SECG_P256K1`, `ECC_NIST_P256`, `ECC_NIST_P384`, and `ECC_NIST_P521` key specs with `ECDSA_SHA_256`, `ECDSA_SHA_384`, `ECDSA_SHA_512` signing algorithms. Sign/Verify works for both `RAW` and `DIGEST` message types. `GetPublicKey` returns DER-encoded EC public keys. Contributed by @dvrkn (#335)

### Fixed
- **Lambda endpoint URL override** — function-level `AWS_ENDPOINT_URL` environment variables no longer override MiniStack's internal endpoint. When MiniStack runs in Docker with a host-port that differs from the container port (e.g., `4568:4566`), Lambda functions would receive the host-mapped URL which is unreachable from inside the container, causing SDK callbacks to fail with "connection refused". Fix applies to all executor paths: provided runtime, Docker mode, image mode, and warm workers. Contributed by @jayjanssen (#336)
- **SFN callback/activity timeout not scaled** — `SFN_WAIT_SCALE=0` no longer causes `States.Timeout` on activity tasks and `waitForTaskToken` callbacks. The scale factor was incorrectly applied to functional timeouts (which must wait for real work to complete), not just Wait state sleeps and retry intervals. Contributed by @jayjanssen (#337)
- **Init scripts override mounted AWS credentials** — ready.d scripts no longer set `AWS_ACCESS_KEY_ID=test` when the user has mounted `~/.aws/credentials` into the container. The AWS CLI credential chain (env vars > credentials file) meant our defaults stomped on the user's configured profile. Now checks for credentials files at `~/.aws/credentials`, `/root/.aws/credentials`, and `AWS_SHARED_CREDENTIALS_FILE`. Reported by @staranto

---

## [1.2.15] — 2026-04-15

### Fixed
- **Kinesis `GetRecords` iterator handling** — shard iterators are no longer consumed (popped) on use, matching real AWS behavior where iterators remain valid until their 5-minute TTL expires. Previously, calling `GetRecords` immediately invalidated the iterator, causing `ExpiredIteratorException` on client retries. Polling consumers like Apache Camel that retry on transient failures would fail with "Iterator has expired or is invalid". Reported by @markwimpory

---

## [1.2.14] — 2026-04-15

### Added
- **Cognito federated SAML/OIDC auth flow** — `GET /oauth2/authorize` (redirects to external SAML/OIDC IdP), `POST /saml2/idpresponse` (parses SAML assertion, creates federated user, issues authorization code), and `POST /oauth2/token` now supports `grant_type=authorization_code` for full SSO flow. Also adds `GetIdentityProviderByIdentifier`. Contributed by @prandogabriel (#329)
- **EC2 AuthorizeSecurityGroup returns rules** — `AuthorizeSecurityGroupIngress` and `AuthorizeSecurityGroupEgress` now return `SecurityGroupRules` in the response with rule IDs, group ownership, protocol, port range, and CIDR details. Required by Terraform AWS provider v6. Reported by @mspiller (#325)

### Fixed
- **Cognito token claims correctness** — `origin_jti` and `auth_time` claims are now only included in `IdToken` and `AccessToken` (not `RefreshToken`), matching real AWS Cognito behavior. Refresh tokens use minimal claims with only `client_id`. 

---

## [1.2.13] — 2026-04-14

### Added
- **RDS real MySQL/MariaDB connectivity** — `pymysql` (44 KB, pure Python) is now bundled in the Docker image. When MiniStack runs inside Docker, RDS containers are attached to MiniStack's Docker network with internal IP endpoints for sibling-container connectivity. The public `localhost` endpoint remains unchanged for host-mode access. The Data API authenticates using credentials from Secrets Manager, mapping the master user to MySQL `root` for admin operations. `CreateDBCluster` stores the master password; `CreateDBInstance` inherits credentials from parent clusters; `ModifyDBCluster` propagates password changes to the real MySQL container via `ALTER USER`. Contributed by @jayjanssen (#316)
- **Cognito Identity Provider CRUD** — `CreateIdentityProvider`, `DescribeIdentityProvider`, `UpdateIdentityProvider`, `DeleteIdentityProvider`, `ListIdentityProviders`. Enables SAML/OIDC federation setup in local development. Reported by @prandogabriel (#325)
- **CodeBuild `BatchGetProjects` ARN lookup** — accepts full ARNs in addition to project names, matching real AWS behavior. Contributed by @alexanderkrum-next (#321)

### Fixed
- **SFN States.Format escape handling** — `States.Format` now correctly processes `\'`, `\{`, `\}`, and `\\` escape sequences in template strings, matching AWS behavior. Escaped quotes no longer truncate the template during intrinsic argument parsing. Interpolated values are preserved verbatim (backslashes in arguments are not interpreted as escapes). Contributed by @jayjanssen (#315)
- **S3 GetBucketLifecycleConfiguration returns canonical XML** — lifecycle rules are now parsed on PUT and reconstructed as canonical `<LifecycleConfiguration>` XML on GET, instead of echoing back the raw PUT body. Fixes Terraform Go SDK v2 deserialization failures. Reported by @alexanderkrum-next (#324)
- **Cognito AdminGetUser accepts sub UUID** — `AdminGetUser` and all user-resolving operations now accept the user's `sub` UUID as the `Username` parameter, matching real AWS behavior. Reported by @prandogabriel (#326)
- **Cognito IdToken missing user attributes** — `IdToken` now includes `email`, `cognito:username`, `email_verified`, and other user attributes. Uses `aud` claim instead of `client_id`, matching the OIDC spec and real AWS Cognito. Reported by @prandogabriel (#327)
- **Cognito AnalyticsConfiguration drift** — `AnalyticsConfiguration` defaults to `None` instead of empty dict, preventing Terraform drift on every plan. Contributed by @alexanderkrum-next (#322)

---

## [1.2.12] — 2026-04-14

### Added
- **SFN Wait state scaling** — new `SFN_WAIT_SCALE` environment variable (default `1.0`) scales Wait state durations and retry interval sleeps. Set to `0` to skip all waits for fast-forward execution in test scenarios where emulated resources are immediately available. Contributed by @jayjanssen (#310)
- **AutoScaling `DescribeScalingActivities`** — returns empty activities list. Terraform polls this after ASG creation; without it Terraform fails. Contributed by @alexanderkrum-next (#317)
- **Reset with init scripts** — `POST /_ministack/reset?init=1` re-runs boot.d and ready.d init scripts after clearing state. Without this, resources created by init scripts were lost after reset with no way to restore them. Reported by @staranto

### Fixed
- **S3 lifecycle configuration hangs Terraform** — `PutBucketLifecycleConfiguration` and `GetBucketLifecycleConfiguration` now return the `x-amz-transition-default-minimum-object-size` header. The Terraform AWS provider waits for this header and hangs indefinitely without it. Reported by @mspiller (#306)
- **Lambda Runtime API noise** — suppressed `BrokenPipeError` tracebacks from Lambda binaries disconnecting after reading the event. This is benign and expected behavior during native `provided` runtime execution. Contributed by @jayjanssen (#311)
- **RDS Data API warning spam** — the `pymysql` import warning is now logged once per process instead of on every `ExecuteStatement` call. Contributed by @jayjanssen (#311)
- **SFN Wait scaling coverage** — `SFN_WAIT_SCALE` now also applies to Activity task timeouts, waitForTaskToken timeouts, and ECS task polling intervals. Runtime config endpoint validates the value (rejects non-numeric, negative, NaN, Inf).

---

## [1.2.11] — 2026-04-14

### Fixed
- **RDS parameter group reset actions** — `ResetDBParameterGroup` and `ResetDBClusterParameterGroup` now clear either selected overrides or the full user-parameter state, matching AWS semantics. Parameter list parsing now accepts both `Parameters.member.N` and `Parameters.Parameter.N` serialization styles. Contributed by @jayjanssen (#298)
- **RDS DbiResourceId lookup** — `DescribeDBInstances` and other instance actions now accept `DbiResourceId` (e.g. `db-1AD581BD3647411AACBF`) in addition to the friendly `DBInstanceIdentifier`. Fixes Terraform/OpenTofu state refresh failures. Contributed by @alexanderkrum-next (#305)

---

## [1.2.10] — 2026-04-13

### Added
- **AppConfig service emulator** — 33 operations across control plane (`appconfig`) and data plane (`appconfigdata`). Applications, environments, configuration profiles, hosted configuration versions, deployment strategies, deployments, tags, and session-based configuration retrieval with token rotation. Contributed by @alexanderkrum-next (#284)
- **Startup `Ready.` log message** — MiniStack now outputs `Ready.` and per-service `<Service> init completed.` messages when the server is ready. Compatible with Testcontainers `LogMessageWaitStrategy` and LocalStack-style readiness detection.

### Fixed
- **SFN aws-sdk error code prefixing** — SDK errors from `aws-sdk:*` task integrations are now prefixed with the service name (e.g. `SecretsManager.ResourceExistsException` instead of bare `ResourceExistsException`), matching real AWS Step Functions behavior. Fixes `Catch` blocks that match on service-specific error codes. Contributed by @jayjanssen (#296)

---

## [1.2.9] — 2026-04-13

### Added
- **AWS CLI bundled in Docker image** — `aws` command now available inside the container for init scripts. Uses AWS CLI v1 via pip (Apache 2.0). Image size increases from 242MB to 269MB. Contributed by @AdigaAkhil (#272)
- **`.py` init scripts** — ready.d and boot.d directories now support Python scripts in addition to shell scripts. Files ending in `.py` are executed with the container's Python interpreter. Contributed by @AdigaAkhil (#272)
- **Init script environment defaults** — init scripts automatically receive `AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`, `AWS_DEFAULT_REGION`, and `AWS_ENDPOINT_URL` so `aws` CLI and boto3 work out of the box without manual configuration.

---

## [1.2.8] — 2026-04-13

### Added
- **SFN intrinsic functions batch 2** — `States.ArrayContains`, `States.ArrayUnique`, `States.ArrayPartition`, `States.ArrayRange`, `States.MathRandom`, `States.MathAdd`, `States.UUID`. Contributed by @jayjanssen (#289)
- **RDS Data API SQL-aware stubs** — when no real database endpoint is available, `ExecuteStatement` now tracks `CREATE/DROP DATABASE`, `CREATE/DROP USER`, and `GRANT/REVOKE` statements in memory per cluster. Verification queries return tracked state. Enables acceptance testing of database provisioning workflows without Docker-in-Docker. Contributed by @jayjanssen (#293)
- **RDS parameter group persistence** — `ModifyDBParameterGroup` and `ModifyDBClusterParameterGroup` now store `ApplyMethod` alongside parameter values. `DescribeDBParameters` and `DescribeDBClusterParameters` return stored parameters with `Source` filter support. Contributed by @jayjanssen (#292)
- **ELBv2 listener attributes** — `DescribeListenerAttributes` and `ModifyListenerAttributes` for ALB listeners. Contributed by @jgrumboe (#286)
- **EC2 subnet tag filtering** — `DescribeSubnets` now supports `tag:*` and `tag-key` filters. Contributed by @jgrumboe (#285)

### Fixed
- **SQS bare queue name as QueueUrl** — passing a bare queue name (e.g. `my-queue`) instead of a full URL now resolves correctly, matching AWS and LocalStack behavior. Previously returned `QueueDoesNotExist`. Reported by @RSzynal-albot
- **Lambda ESM ReportBatchItemFailures** — SQS event source mappings with `FunctionResponseTypes=["ReportBatchItemFailures"]` now parse the handler's `batchItemFailures` response. Failed messages are left on the queue for redelivery/DLQ instead of being silently deleted. Reported by @okinaka
- **SFN REST-JSON PascalCase to camelCase conversion** — `_dispatch_aws_sdk_rest_json` now converts PascalCase parameter names to camelCase before dispatching. Fixes `BadRequestException: resourceArn is required` when Step Functions dispatches to RDS Data API. Contributed by @jayjanssen (#291)
- **SFN query-protocol XML response fidelity** — `_xml_element_to_dict` now coerces known numeric fields to integers, boolean fields to booleans, and detects list-wrapper elements to produce JSON arrays even with a single child. Contributed by @jayjanssen (#290)
- **RDS DescribeDBEngineVersions family prefix** — `DBParameterGroupFamily` no longer double-prefixes the engine name. Contributed by @jayjanssen (#292)

---

## [1.2.7] — 2026-04-12

### Added
- **EC2 CreateDefaultVpc** — new action creates a default VPC with all associated resources (3 default subnets, internet gateway, route table, network ACL, security group), matching real AWS behavior. Returns `DefaultVpcAlreadyExists` if one already exists. Reported by @staranto 
- **DynamoDB ExecuteStatement (PartiQL)** — supports `SELECT`, `INSERT`, `UPDATE`, `DELETE` PartiQL statements with `?` parameter binding. Enables IntelliJ database integration and other PartiQL-based tooling. Reported by @mspiller
- **SNS FIFO topic support** — `.fifo` naming validation, `MessageGroupId`/`MessageDeduplicationId` enforcement, 5-minute deduplication window, sequence numbers, content-based deduplication, FIFO SQS subscription validation, `PublishBatch` FIFO support, thread-safe dedup cache. Contributed by @yskarparis (#279)

### Fixed
- **Lambda UpdateFunctionConfiguration Layers** — attaching layers via `update-function-configuration` no longer throws `'str' object has no attribute 'get'`. Layer ARN strings are now normalized to `{"Arn": ..., "CodeSize": 0}` dicts, matching the `create-function` path. Reported by @Vagator-Prostovich
- **EC2 default VPC network ACL** — the default VPC's network ACL (`acl-00000001`) was referenced but never initialized, causing `DescribeNetworkAcls` to omit it. Now created at startup with standard allow/deny entries.
- **S3 GetObject by VersionId** — requesting a specific version now returns the correct object data. Previously always returned the latest version, ignoring the `versionId` parameter.
- **S3 delete markers in ListObjectVersions** — deleting an object in a versioned bucket now inserts a proper delete marker. `ListObjectVersions` returns `DeleteMarker` elements. Previously delete markers were missing entirely.
- **S3 reset clears version history** — `/_ministack/reset` now clears `_object_versions` store. Previously versioned objects accumulated across resets.
- **Lambda Invoke event payload** — handler event no longer contains an internal `_request_id` field. Previously leaked into the event dict, breaking handlers that validate input shape.
- **Lambda PublishVersion ARN** — `FunctionArn` in the response now includes the version qualifier (e.g. `:1`). Previously returned the unqualified function ARN.
- **DynamoDB BatchWriteItem on nonexistent table** — returns `ResourceNotFoundException` instead of silently placing items into `UnprocessedItems`.
- **WAFv2 DeleteWebACL LockToken** — now enforces `LockToken` validation, returning `WAFOptimisticLockException` for stale tokens. `UpdateWebACL` already enforced this; `DeleteWebACL` was missing the check.
- **Step Functions duplicate execution name** — `StartExecution` with a name already in use returns `ExecutionAlreadyExists`. Previously silently created a second execution.
- **Step Functions Fail state error/cause** — `DescribeExecution` now includes `error` and `cause` fields when execution fails via a Fail state. Previously returned `null` for both.
- **API Gateway v2 CreateApi Description** — `Description` field is now stored and returned. Previously silently dropped.
- **API Gateway v1 CreateResource duplicate** — rejects duplicate `pathPart` under the same parent with `ConflictException`. Previously silently created duplicates.
- **CloudWatch DeleteDashboards nonexistent** — returns `DashboardNotFoundError` for nonexistent dashboards. Previously silently succeeded.
- **RDS DescribeDBInstances error code** — returns `DBInstanceNotFoundFault` (with `Fault` suffix) matching real AWS. Previously returned `DBInstanceNotFound`.
- **SQS CreateQueue attribute mismatch** — creating a queue with the same name but different attributes returns `QueueNameExists`. Previously silently returned the existing queue URL.
- **EC2 TagSpecifications on create operations** — `CreateVpc`, `CreateSubnet`, `CreateSecurityGroup`, `CreateKeyPair`, `CreateInternetGateway`, `CreateRouteTable`, `CreateNatGateway`, `CreateNetworkAcl` now process `TagSpecifications` and persist tags. Previously silently ignored.
- **EC2 DeleteVpc dependency check** — returns `DependencyViolation` when subnets, non-default security groups, or internet gateways are still attached. Previously silently deleted the VPC.
- **EC2 delete default security group blocked** — returns `CannotDelete` when attempting to delete a VPC's default security group. Previously silently deleted it.
- **EC2 RunInstances MinCount > MaxCount** — returns `InvalidParameterCombination` when `MinCount` exceeds `MaxCount`. Previously silently launched instances.
- **EC2 Describe tag sets** — `DescribeRouteTables`, `DescribeVolumes`, `DescribeSnapshots`, `DescribeNatGateways` now read tags from the `_tags` store. Previously returned hardcoded empty `<tagSet/>`.
- **ECS DescribeTaskDefinition tags** — always returns tags in the response. Previously only returned tags when `include=["TAGS"]` was explicitly passed.

---

## [1.2.6] — 2026-04-12

### Fixed
- **EFS timestamp format** — `CreationTime` now returns integer epoch seconds instead of ISO string, fixing Java SDK v2 unmarshalling errors.
- **ECS timestamps** — `createdAt` and other timestamp fields now return integer epoch seconds instead of floats with sub-second precision.
- **DynamoDB `X-Amz-Crc32` header** — all DynamoDB responses now include the CRC32 checksum header, fixing Go SDK v2 `failed to close HTTP response body` warnings.
- **EC2 DescribeInternetGateways not-found** — returns `InvalidInternetGatewayID.NotFound` for nonexistent IDs.
- **EC2 CreateVpc CIDR validation** — rejects invalid CIDR blocks with `InvalidParameterValue`.
- **EC2 duplicate security group rule** — `AuthorizeSecurityGroupIngress` returns `InvalidPermission.Duplicate` for existing rules.
- **EC2 CreateVolume/CreateSnapshot TagSpecifications** — tags specified in `TagSpecifications` are now persisted.
- **ElastiCache CreateCacheSubnetGroup** — `DescribeCacheSubnetGroups` now returns the `Subnets` list with subnet identifiers and availability zones.
- **SNS error code** — `GetTopicAttributes`, `Publish`, and other operations on nonexistent topics now return `NotFound` instead of `NotFoundException`, matching real AWS.
- **LocalStack init script path compatibility** — now supports `/etc/localstack/init/ready.d/` in addition to `/docker-entrypoint-initaws.d/ready.d/` for drop-in LocalStack replacement. Contributed by @AdigaAkhil (#271)
- **CloudWatch error response protocol mismatch** — error responses now match the request protocol (JSON errors for JSON requests, CBOR errors for CBOR requests). Previously, JSON-protocol requests received CBOR-encoded errors causing boto3 `UnicodeDecodeError`.
- **AppSync apiId length** — `CreateGraphQLApi` now generates 26-character alphanumeric IDs matching real AWS format. Previously 8 characters, which broke boto3 ARN validation for tag operations.
- **EC2 CreateTags persistence** — tags applied via `CreateTags` now appear in `DescribeVpcs`, `DescribeSubnets`, `DescribeSecurityGroups`, and `DescribeInternetGateways`. Previously returned empty `<tagSet/>`.
- **EC2 RunInstances TagSpecifications** — tags specified in `TagSpecifications` with `ResourceType=instance` are now persisted and returned in `DescribeInstances`.
- **EC2 Describe not-found errors** — `DescribeVpcs`, `DescribeSubnets`, `DescribeSecurityGroups`, `DescribeKeyPairs`, `DescribeInstances`, `DescribeVolumes`, `DescribeSnapshots` now return proper AWS error codes (`InvalidVpcID.NotFound`, etc.) when specific IDs are requested but don't exist.
- **EFS not-found errors** — `DescribeFileSystems` and `DescribeMountTargets` now return `FileSystemNotFound` / `MountTargetNotFound` for nonexistent IDs.
- **ELBv2 not-found errors** — `DescribeLoadBalancers`, `DescribeTargetGroups` return proper errors for nonexistent ARNs/names. `DeleteListener`, `DeleteTargetGroup` return errors for nonexistent ARNs.
- **ElastiCache not-found errors** — `DescribeCacheSubnetGroups`, `DeleteCacheSubnetGroup`, `DescribeCacheParameterGroups`, `DeleteCacheParameterGroup` now return proper `CacheSubnetGroupNotFoundFault` / `CacheParameterGroupNotFound` errors.
- **Glue validation** — `CreateTable` rejects nonexistent database, `CreateCrawler` rejects duplicate names, `DeleteTable` / `DeleteConnection` return `EntityNotFoundException` for nonexistent resources.
- **CloudFront CallerReference idempotency** — `CreateDistribution` with a duplicate `CallerReference` returns the existing distribution instead of creating a duplicate.
- **WAFv2 LockToken enforcement** — `UpdateWebACL` validates `LockToken` and returns `WAFOptimisticLockException` for stale tokens.
- **WAFv2 duplicate name** — `CreateWebACL` rejects duplicate names within the same scope with `WAFDuplicateItemException`.
- **ServiceDiscovery duplicate namespace** — `CreateHttpNamespace` rejects duplicate names with `NamespaceAlreadyExists`.
- **AutoScaling DescribePolicies** — response now includes `AdjustmentType`, `ScalingAdjustment`, and `Cooldown` fields.
- **ECS TagResource validation** — rejects nonexistent resource ARNs with `InvalidParameterException`.
- **EC2 DescribeVpcs filters** — filters parameter (`owner-id`, `vpc-id`, `cidr`, `state`, `is-default`, `tag:*`) now applied correctly. Previously silently ignored.

---

## [1.2.5] — 2026-04-12

### Fixed
- **Secrets Manager partial ARN lookup** — `GetSecretValue` and all other operations now resolve secrets by partial ARN (without the random 6-character suffix), matching real AWS behaviour. Previously returned `ResourceNotFoundException`.
- **Java SDK v2 timestamp compatibility** — all JSON-protocol services now return integer epoch seconds instead of high-precision floats. Fixes `Unable to parse date` and `Input timestamp string must be no longer than 20 characters` errors across DynamoDB, Lambda, Kinesis, CodeBuild, CloudWatch, Glue, Athena, ECR, Secrets Manager, EventBridge, KMS, SNS, Service Discovery, and CloudFormation provisioners. Python and Node.js SDKs are unaffected.
- **DELETE/GET/HEAD requests without body could hang** — ASGI body read loop now skips waiting for a body on methods that don't typically carry one, preventing timeouts under concurrent load.

---

## [1.2.4] — 2026-04-11

### Added
- **CodeBuild service** — new service with 11 API operations: CreateProject, BatchGetProjects, ListProjects, UpdateProject, DeleteProject, StartBuild, BatchGetBuilds, StopBuild, ListBuilds, ListBuildsForProject, BatchDeleteBuilds. Contributed by @Nikhiladiga (#253)
- **CloudFront Origin Access Control (OAC)** — CreateOriginAccessControl, GetOriginAccessControl, GetOriginAccessControlConfig, ListOriginAccessControls, UpdateOriginAccessControl, DeleteOriginAccessControl. Contributed by @yskarparis (#258)
- **CloudFormation `AWS::Route53::RecordSet`** — provisions A, AAAA, CNAME, and alias records with weighted/failover/geo routing support. Contributed by @aldokimi (#263)
- **CloudFormation `AWS::CloudWatch::Alarm`** — provisions metric alarms with full lifecycle (create/delete). Contributed by @aldokimi (#265)
- **Lambda ESM layer symlink** — Node.js ESM `import()` now resolves packages from Lambda Layers via symlinked `node_modules`. Contributed by @bognari (#259)

### Fixed
- **CodeBuild multitenancy** — switched from plain `dict` to `AccountScopedDict` for proper account scoping
- **CFN test merge conflict** — separated mangled CloudWatch Alarm and Route53 RecordSet tests into independent functions

---

## [1.2.3] — 2026-04-11

### Fixed
- **Go SDK v2 `failed to close HTTP response body` warning** — uvicorn's default keep-alive timeout (5s) was too short for Go/Java SDK connection pools (~90s idle). Increased to 75s to match AWS ALB defaults. Affected all services, most visible with DynamoDB health checks. Reported by @mspiller (#249)
- **SSM inline tags regression test** — added test for `PutParameter` with inline `Tags` followed by `ListTagsForResource`. Contributed by @bognari (#254)

---

## [1.2.2] — 2026-04-11

### Fixed
- **SSM `ListTagsForResource` crash** — `PutParameter` stored tags as a list but `ListTagsForResource` expected a dict, causing `AttributeError: 'list' object has no attribute 'items'`. Blocked all Terraform/OpenTofu deployments creating SSM parameters. Reported by @bognari (#248)

---

## [1.2.1] — 2026-04-11

### Added
- **Dynamic RDS storage** — new `RDS_PERSIST=1` env var switches database containers from fixed-size tmpfs to Docker named volumes for auto-growing persistent storage. Default (`RDS_PERSIST=0`) remains ephemeral tmpfs for CI/CD. Reported by @macario1983 (#248).
- **Dual Docker Hub publishing** — Docker images now publish to both `nahuelnucera/ministack` and `ministackorg/ministack` on tag push.

---

## [1.2.0] — 2026-04-11

### Added
- **AutoScaling service** — new full service with 22 API operations: CreateAutoScalingGroup, DescribeAutoScalingGroups, UpdateAutoScalingGroup, DeleteAutoScalingGroup, CreateLaunchConfiguration, DescribeLaunchConfigurations, DeleteLaunchConfiguration, PutScalingPolicy, DescribePolicies, DeletePolicy, PutLifecycleHook, DescribeLifecycleHooks, DeleteLifecycleHook, CompleteLifecycleAction, RecordLifecycleActionHeartbeat, PutScheduledUpdateGroupAction, DescribeScheduledActions, DeleteScheduledAction, CreateOrUpdateTags, DescribeTags, DeleteTags, DescribeAutoScalingInstances.
- **9 new CloudFormation provisioners** — `AWS::Lambda::LayerVersion`, `AWS::StepFunctions::StateMachine`, `AWS::Route53::HostedZone`, `AWS::ApiGatewayV2::Api`, `AWS::ApiGatewayV2::Stage`, `AWS::SES::EmailIdentity`, `AWS::WAFv2::WebACL`, `AWS::CloudFront::Distribution`, `AWS::RDS::DBCluster`. All 9 support create, delete, and Fn::GetAtt. Total provisioners: 66 (was 57).
- **5 AutoScaling CFN provisioners upgraded** — `AWS::AutoScaling::AutoScalingGroup`, `LaunchConfiguration`, `ScalingPolicy`, `LifecycleHook`, `ScheduledAction` now store real data (were stubs).
- **EC2 `DescribeInstanceStatus`** — new operation with `IncludeAllInstances` support. Returns instance state, system status, and instance status.
- **EC2 `DescribeVpcClassicLink` / `DescribeVpcClassicLinkDnsSupport`** — stubs returning empty sets. Unblocks all VPC-dependent Terraform resources (subnet, security group, instance, ALB, NLB, EFS).
- **Test parallelization** — CI now runs tests in parallel with pytest-xdist. Adjusted worker count for CFN stack reliability, added retries for flaky tests, increased CFN stack wait timeout. Contributed by @jgrumboe (#199).
- **SFN REST-JSON `aws-sdk` dispatcher + RDS Data API integration** — Step Functions `aws-sdk:rdsdata:executeStatement` and other RDS Data actions now dispatch via a new REST-JSON protocol handler. Static action→path map avoids botocore dependency. RDS Data API returns stub success when no database endpoint is available, allowing SFN workflows to proceed in mock environments. Contributed by @jayjanssen (#237).
- **Lambda warm worker layer extraction** — warm worker pool now extracts Lambda layers and makes their code available to handlers. Python layers are added to `sys.path` via `_LAMBDA_LAYERS_DIRS` env var. Node.js layers are resolved via `NODE_PATH` pointing to each layer's `nodejs/node_modules` directory. Includes zip-slip protection on extraction. Contributed by @bognari (#236).
- **Lambda Node.js ESM (.mjs) handler support** — Node.js handlers using ES modules (`.mjs` files or `package.json` with `"type": "module"`) now load correctly via dynamic `import()` fallback when `require()` fails with `ERR_REQUIRE_ESM`. Supports `export const handler`, `export default`, and cross-module ESM imports. Works in both warm worker pool and cold invocation paths. Contributed by @bognari (#238).

### Fixed
- **Terraform AWS provider v5.x compatibility (Lambda, DynamoDB, SFN, ESM)** — Lambda no longer injects default runtime/handler for Image-based functions and preserves `ImageConfigResponse` in create/update responses. ESM omits `StartingPosition` for SQS event sources (only valid for Kinesis/DynamoDB Streams). DynamoDB returns `ProvisionedThroughput` with zero values for PAY_PER_REQUEST tables and GSIs. Step Functions implements `ValidateStateMachineDefinition` stub required by provider v5.42.0+. Contributed by @DaviReisVieira (#242).
- **Kinesis `IncreaseStreamRetentionPeriod` rejects same value** — setting retention to 24h (the default) failed with "must be greater than current value". Now accepts same-value as no-op. Blocked `aws_kinesis_stream` in Terraform and Pulumi.
- **ACM `DescribeCertificate` timestamps as ISO strings** — Terraform Go SDK expects epoch floats. `CreatedAt`, `IssuedAt`, `NotBefore`, `NotAfter` now return epoch numbers. Blocked `aws_acm_certificate` in Terraform.
- **Lambda ESM `Enabled` field ignored** — creating an ESM with `Enabled: false` always returned `State: Enabled`. Now respects the request parameter.
- **Lambda ESM `Enabled` field in response** — real AWS does not include `Enabled` in ESM responses, only `State`. Extra field caused Terraform drift.
- **ECS TaskDefinition extra container fields** — `container_definitions` included `environment=[], mountPoints=[], volumesFrom=[], memoryReservation=0` when not specified. Caused Terraform replacement on every apply.
- **DynamoDB `CreateTable` ignores `Tags`** — tags passed in `CreateTable` were not stored. `ListTagsOfResource` returned empty. Terraform re-applied tags every plan.
- **SNS `CreateTopic` ignores `Tags`** — same as DynamoDB. Tags now stored on create.
- **SNS `DisplayName` defaults to topic name** — real AWS defaults to empty string. Caused Terraform drift.
- **SSM `PutParameter` ignores `Tags`** — tags now stored on create.
- **Lambda empty `Environment` block returned** — when no env vars set, response included `Environment: {Variables: {}}`. Terraform tried to remove it every plan. Now omitted when not set.
- **Lambda `DeadLetterConfig` empty object returned** — when not configured, response included `DeadLetterConfig: {}`. Now omitted when not set.
- **Lambda Function URL missing `InvokeMode`** — response lacked `InvokeMode` field. Terraform wanted to set "BUFFERED" every plan. Now defaults to "BUFFERED".
- **Lambda Function URL empty `Cors` block** — `cors: {}` returned when not configured. Now omitted.
- **API Gateway v2 empty `corsConfiguration`** — returned `{}` when not set. Caused Terraform/Pulumi drift.
- **API Gateway v2 missing `apiKeySelectionExpression`** — now defaults to `$request.header.x-api-key`.
- **Cognito UserPool extra empty blocks** — `DeviceConfiguration`, `UserPoolAddOns`, `UsernameConfiguration`, `VerificationMessageTemplate` returned when not set. Now only included when explicitly provided. Added missing `DeletionProtection` field.
- **SNS `GetTopicAttributes` 404 with empty account ARN** — SDKs that skip `GetCallerIdentity` (Pulumi with `skipRequestingAccountId`) construct ARNs with empty account ID (`arn:aws:sns:us-east-1::name`). All SNS operations now normalize these to the default account.
- **SES `DeleteIdentity` malformed XML response** — response lacked `<DeleteIdentityResult/>` element. Go SDK deserialization failed. Also fixed `SetIdentityNotificationTopic` and `SetIdentityFeedbackForwardingEnabled`.
- **Go SDK v2 "failed to close HTTP response body" warning** — all responses lacked `Content-Length` header, causing Uvicorn to use `Transfer-Encoding: chunked`. The Go AWS SDK v2 warns on every chunked response close. Now sets `Content-Length` on all responses. Affects all services. Reported by @mspiller.
- **S3 `ListObjectVersions` returns only one version** — when versioning is enabled, multiple PUTs to the same key only stored the latest object. `ListObjectVersions` returned a single version with hardcoded `VersionId: "1"`. Now maintains full version history with unique VersionIds per PUT. Reported by @aldex32.

---

## [1.1.62] — 2026-04-10

### Added
- **SFN query-protocol acronym mapper** — Step Functions `aws-sdk:*` integrations now correctly convert SDK-style parameter names (e.g. `DbSubnetGroupName`) to wire-format names (`DBSubnetGroupName`) for query-protocol services (RDS, EC2, IAM, STS, etc.). Uses a static acronym mapping — no botocore dependency. Contributed by @jayjanssen (#235).

### Fixed
- **API Gateway v1/v2 returns mock response for Node.js Lambdas** — `_invoke_lambda_proxy` in both `apigateway.py` (v2) and `apigateway_v1.py` (v1) only dispatched to the warm worker pool for Python runtimes. Node.js Lambdas received a hardcoded `"Mock response"` instead of being executed. Now checks for both `python` and `nodejs` runtimes. Contributed by @bognari (#234).
- **API Gateway v2 missing `pathParameters` in Lambda event** — Routes with path parameters (e.g. `GET /items/{itemId}`) did not extract parameter values into the Lambda proxy event's `pathParameters` field. Now extracts parameters from both `{param}` and `{proxy+}` route templates. Contributed by @bognari (#239).
- **API Gateway v2 `queryStringParameters` incorrect for multi-value params** — Multi-value query parameters (e.g. `?tag=a&tag=b`) were passed as Python lists instead of comma-joined strings. Now joins values with commas (`"tag": "a,b"`) matching the AWS API Gateway v2 payload format 2.0 spec. Contributed by @bognari (#239).
- **API Gateway v2 `rawQueryString` stringified lists** — Multi-value query parameters were rendered as `tag=['a', 'b']` instead of `tag=a&tag=b`. Now expands repeated keys correctly. Contributed by @bognari (#239).
- **Lambda Docker executor fails for `provided` runtimes** — `_execute_function_docker()` mounted Lambda code only at `/var/task` and overrode CMD to `["/var/task/bootstrap"]`, but the AWS RIE entrypoint expects the bootstrap binary at `/var/runtime/bootstrap`. Now mounts code at both `/var/task` and `/var/runtime` and passes `"bootstrap"` as CMD. Contributed by @jayjanssen (#232).
- **Lambda `print()` / `console.log()` output lost in warm worker pool** — Python handler `print()` wrote to stdout, colliding with the JSON-line protocol between worker and host. Now redirects Python stdout to stderr (matching the existing Node.js worker behavior). Worker `invoke()` drains stderr after each invocation and returns it as `log`. ESM success paths (SQS, Kinesis, DynamoDB Streams) now emit handler output to the MiniStack log. Direct `Invoke` with `LogType=Tail` returns the output in `X-Amz-Log-Result`. Reported by @PerhapsJack.

---
## [1.1.61] — 2026-04-10

### Fixed
- **EC2 `DescribeTags` ignores filters** — `DescribeTags` returned every tag for every resource regardless of `Filter` parameters. Terraform's `aws_instance` resource sends `resource-id` and `key` filters when reading launch template tags; receiving unrelated tags caused "too many results: wanted 1, got 3". Now respects `resource-id`, `resource-type`, `key`, and `value` filters. Reported by @m7w.
- **EC2 `DescribeTags` returns wrong `resourceType`** — resources with prefixes `acl-`, `nat-`, `dopt-`, `eigw-`, `lt-`, `pl-`, `vgw-`, `cgw-`, `ami-`, `tgw-` were returned as generic `"resource"` instead of their correct types (`network-acl`, `natgateway`, `launch-template`, etc.). Reported by @m7w.
- **Lambda container networking in DinD** — when MiniStack runs inside a Docker container (DinD via socket mount), `127.0.0.1` refers to the MiniStack container itself, not the Docker host where the Lambda container's port is mapped. When `LAMBDA_DOCKER_NETWORK` is set, Lambda invocations now resolve the container's IP on the shared network and connect directly on port 8080. Contributed by @DaviReisVieira. Fixes #228.

---

## [1.1.60] — 2026-04-09

### Added
- **Native `provided` / `provided.al2023` Lambda runtime** — Lambda functions using custom runtimes (Go, Rust, C++ compiled binaries) now execute natively without Docker. MiniStack implements the Lambda Runtime API (`GET /invocation/next`, `POST /invocation/{id}/response`) as a minimal HTTP server, extracts the bootstrap binary from the deployment package, and manages the invocation lifecycle. Handles Go's default chunked `Transfer-Encoding`. Contributed by @jayjanssen (#220).
- **States.ArrayGetItem, States.Array, States.ArrayLength intrinsics** — SFN state machines using `States.ArrayGetItem(array, index)`, `States.Array(val1, val2, ...)`, and `States.ArrayLength(array)` now execute correctly. Cherry-picked from @jayjanssen (#218).
- **SFN key naming convention** — API response keys like `DBClusters` are now converted to Java SDK V2 convention (`DbClusters`) matching real AWS SFN behavior. Applied to both query-protocol and JSON-protocol aws-sdk dispatchers. Cherry-picked from @jayjanssen (#218).
- **RDS `EnableHttpEndpoint` action** — stub that accepts and stores the flag on DB clusters. Cherry-picked from @jayjanssen (#218).

### Fixed
- **Lambda provided-runtime race conditions** — fixed port allocation race (socket bind-then-close replaced with `TCPServer` port 0 atomic bind) and server-ready race (bootstrap process now waits for Runtime API server to be accepting connections before starting).
- **`States.TaskFailed` treated as catch-all** — `Retry` and `Catch` blocks matching `States.TaskFailed` now catch any Task error, matching AWS behavior. Cherry-picked from @jayjanssen (#218).
- **Map state `ItemSelector` path resolution** — `$` paths in `ItemSelector` now resolve against the Map state's effective input instead of the individual item. The item is available via `$$.Map.Item.Value`. Cherry-picked from @jayjanssen (#218).
- **CFN inline ZipFile uses correct extension for Node.js** — `_zip_inline` now writes `index.js` for Node.js runtimes instead of always writing `index.py`. Fixes CDK `Code.fromInline` with Node.js failing at invocation. Reported by @jolo-dev.
- **EC2 `DescribeSubnets` filter support** — `DescribeSubnets` now respects `vpc-id`, `availability-zone`, `subnet-id`, and `default-for-az` filters. Previously all filters were silently ignored.

---

## [1.1.59] — 2026-04-09

### Added
- **EventBridge expanded API coverage** — 20 new actions: `ListRuleNamesByTarget`, `TestEventPattern`, `UpdateArchive`, `StartReplay`, `DescribeReplay`, `ListReplays`, `CancelReplay`, `CreateEndpoint`, `DeleteEndpoint`, `DescribeEndpoint`, `ListEndpoints`, `UpdateEndpoint`, `DeauthorizeConnection`, `ActivateEventSource`, `DeactivateEventSource`, `DescribeEventSource`, `CreatePartnerEventSource`, `DeletePartnerEventSource`, `DescribePartnerEventSource`, `ListPartnerEventSources`, `ListPartnerEventSourceAccounts`, `ListEventSources`, `PutPartnerEvents`. Contributed by @aldokimi (#210).
- **CloudFormation `AWS::Kinesis::Stream` provisioner** — create/delete with `ShardCount`, `Name`, `RetentionPeriodHours`, `StreamModeDetails` (ON_DEMAND/PROVISIONED); `Fn::GetAtt` for `Arn`, `StreamId`. Also registered `rds-data` in service handler routing. Contributed by @aldokimi (#207).
- **EC2 default subnets** — default VPC now creates 3 subnets (one per AZ: a/b/c) matching real AWS behavior instead of a single subnet. Contributed by @jayjanssen (#205).
- **Step Functions `States.JsonToString` intrinsic** — counterpart to `States.StringToJson`. Contributed by @jayjanssen (#215).
- **CloudFormation `AWS::ElasticLoadBalancingV2::LoadBalancer` and `::Listener` provisioners** — create/delete with full ALB lifecycle, including default rules, tag propagation, and cascading cleanup. `Fn::GetAtt` for `Arn`, `DNSName`, `LoadBalancerFullName`, `CanonicalHostedZoneID`. Contributed by @aldokimi (#217).

### Fixed
- **EventBridge ARN-as-bus-name in PutEvents** — events published with a full ARN as `EventBusName` (e.g. `arn:aws:events:us-east-1:000000000000:event-bus/my-bus`) were silently dropped because the bus name comparison against rules failed. `PutEvents` now normalizes ARN-style values to the plain bus name before dispatch. Contributed by @ctnnguyen (#208).
- **CloudFormation EventBridge rule composite key** — `_eb_rule_create` and `_eb_rule_delete` used reversed key order (`name|bus` instead of `bus|name`), making CFN-provisioned rules invisible to the EventBridge API (`DescribeRule`, `ListTargetsByRule`) and event dispatch. Now uses `_eb._rule_key()` for consistent key construction. Contributed by @ctnnguyen (#208).
- **CloudFormation EventBridge target storage** — CFN rule provisioner cherry-picked only `Id`, `Arn`, `RoleArn`, `Input`, `InputPath` from targets, dropping `InputTransformer`, `SqsParameters`, `EcsParameters`, and other properties. Now stores the full target dict. Contributed by @ctnnguyen (#208).
- **Step Functions aws-sdk action casing** — SFN ARNs use camelCase (e.g. `createDBSubnetGroup`) but query-protocol and JSON-protocol services expect PascalCase (`CreateDBSubnetGroup`). Both dispatch paths now capitalize the first letter. Contributed by @jayjanssen (#204, #215).
- **RDS `_parse_member_list` botocore format** — list parameters dispatched via Step Functions aws-sdk integrations use `Prefix.MemberName.N` format instead of `Prefix.member.N`. The parser now handles both formats.

---

## [1.1.58] — 2026-04-09

### Fixed
- **Kinesis CBOR protocol support** — `PutRecord` and `PutRecords` from the AWS Java SDK v2 failed with `'utf-8' codec can't decode byte 0xbf`. The Java SDK sends Kinesis requests as CBOR (`application/x-amz-cbor-1.1`) by default, but the handler only accepted JSON. Kinesis now detects CBOR content-type, decodes with `cbor2`, and returns CBOR-encoded responses. Reported by @markwimpory.

---

## [1.1.57] — 2026-04-09

### Fixed
- **EventBridge wildcard and content-filter patterns not matching** — event patterns using `{"wildcard": "*simple*"}`, `{"prefix": "..."}`, `{"suffix": "..."}`, etc. in top-level fields like `detail-type` and `source` were silently ignored. Content-based filters now work in all pattern fields, not just `detail`. Also added `wildcard` support to the content filter engine (uses `fnmatch` glob matching). Reported by @jfisbein
- **IAM tags not saved on CreateRole/CreateUser** — tags passed at creation time via `Tags.member.N.Key/Value` were silently ignored. `GetRole` and `GetUser` now return tags set during creation. Same pattern as the KMS and SQS tag fixes in prior releases.
- **Multi-tenant state persistence loses non-default accounts on restart** — when `PERSIST_STATE=1`, resources created under custom account IDs were restored under `000000000000` after container restart. Affected services: **S3**, **Lambda**, **ECS**, **KMS**. All four services' `get_state()` functions now iterate all accounts' data (via `_data`) instead of only the current request context. S3 file persistence (`S3_DATA_DIR`) layout changed to `DATA_DIR/<account_id>/<bucket>/<key>`; legacy flat layout auto-detected on load. The other 14 services (SQS, SNS, DynamoDB, IAM, EC2, SSM, etc.) were already safe — they use `copy.deepcopy()` which preserves all accounts.

---

## [1.1.56] — 2026-04-09

### Added
- **Multi-tenancy state isolation** — resources with the same name in different accounts no longer collide. All service state dicts use `AccountScopedDict` which namespaces by account ID automatically. Previously, multi-tenancy (v1.1.54) only changed ARN generation — the underlying state was shared. Now IAM roles, S3 buckets, SQS queues, DynamoDB tables, and all other resources are fully isolated per account. Reported by community feedback.
- **Graceful Docker container cleanup on shutdown** — RDS, ECS, and ElastiCache Docker containers are now stopped and removed when MiniStack shuts down, using Docker labels (`ministack=rds`, `ministack=ecs`, `ministack=elasticache`). Previously containers were orphaned unless `/_ministack/reset` was called explicitly.

### Fixed
- **SQS queue tags not saved on CreateQueue** — tags passed at queue creation time were silently ignored. `ListQueueTags` now returns tags set during `CreateQueue` for both JSON and Query API protocols. Reported by @jfisbein
- **PERSIST_STATE compatibility with AccountScopedDict** — state serialization and deserialization now handle the new scoped dict format correctly. All 37 service state files save and restore across restarts.

---

## [1.1.55] — 2026-04-09

### Fixed
- **IAM/CloudFormation JSON protocol support** — IAM and CloudFormation now handle `AwsJson1_1` protocol requests (used by newer AWS SDK versions and CDK CLI). v1.1.54 added JSON protocol support for STS only, but some CDK/SDK versions also send IAM and CloudFormation requests via JSON protocol, causing "The security token included in the request is invalid" errors.
- **CloudFormation AutoScaling stubs** — `AWS::AutoScaling::AutoScalingGroup`, `LaunchConfiguration`, `ScalingPolicy`, `LifecycleHook`, and `ScheduledAction` are now handled as no-ops, allowing CDK/CFN stacks with ASGs to deploy without failing. Reported by @titan1978
- **README KMS table formatting** — KMS row was detached from the services table by a blank line, causing broken rendering.

---

## [1.1.54] — 2026-04-08

### Added
- **Multi-tenancy via dynamic Account ID** — When `AWS_ACCESS_KEY_ID` is a 12-digit number (e.g. `048408301323`), MiniStack uses it as the Account ID for all ARN generation. Non-numeric keys fall back to `MINISTACK_ACCOUNT_ID` env var or `000000000000`. Enables lightweight tenant isolation on shared endpoints without configuration changes.
- **CloudFormation `TemplateURL` support** — `CreateStack`, `UpdateStack`, `CreateChangeSet`, and `GetTemplateSummary` now fetch templates from S3 when `TemplateURL` is provided instead of `TemplateBody`. This unblocks `cdk deploy` which publishes templates to S3 and passes a URL.
- **CloudFormation `AWS::CDK::Metadata` support** — CDK metadata resources are now handled as no-ops instead of failing with "Unsupported resource type".
- **STS JSON protocol support** — STS now handles `AwsJson1_1` protocol requests (used by newer AWS SDK versions and CDK CLI). Previously, STS only accepted Query/form-encoded requests, causing CDK to fail with "The security token included in the request is invalid" when it tried to AssumeRole using the JSON protocol.
- **CloudFormation AutoScaling stubs** — `AWS::AutoScaling::AutoScalingGroup`, `LaunchConfiguration`, `ScalingPolicy`, `LifecycleHook`, and `ScheduledAction` are now handled as no-ops, allowing CDK/CFN stacks with ASGs to deploy without failing. Reported by @titan1978

### Fixed
- **Test coverage for v1.1.53 fixes** — added unit tests for `_convert_parameters` (RDS Data API parameter binding) and SSM epoch timestamp in CloudFormation provisioner.

---

## [1.1.53] — 2026-04-08

### Added
- **RDS Aurora Global Clusters (5 operations)** — `CreateGlobalCluster`, `DescribeGlobalClusters`, `DeleteGlobalCluster`, `RemoveFromGlobalCluster`, `ModifyGlobalCluster`. In-memory global cluster model with member cluster membership, source cluster auto-attach, deletion protection, and rename support. Contributed by @jayjanssen (#194)
- **RDS Data API service** — `ExecuteStatement`, `BatchExecuteStatement`, `BeginTransaction`, `CommitTransaction`, `RollbackTransaction`. Routes SQL to the real database containers MiniStack spins up for RDS instances. Supports both MySQL and PostgreSQL engines. Contributed by @jayjanssen (#193)

### Fixed
- **CDK deploy "implicit NaN" deserialization error** — the CloudFormation SSM provisioner stored `LastModifiedDate` as an ISO 8601 string instead of a Unix epoch float. The JS SDK v3 (bundled in CDK CLI) uses `AwsJson1_1Protocol` for SSM and calls `parseEpochTimestamp()` on the value, which expects a number. `cdk deploy` would fail immediately after bootstrap when checking the SSM bootstrap version parameter. Reported by @youngkwangk @jolo-dev and @ben-shearlaw 
- **RDS Data API thread safety** — added `threading.Lock` to protect transaction state against concurrent access
- **RDS Data API parameter binding** — `ExecuteStatement` and `BatchExecuteStatement` now convert RDS Data API `:name` parameters to DB-API parameterized queries instead of ignoring them
- **RDS Data API connection leak** — connections are now properly closed on exceptions in non-transaction execute paths
- **RDS Data API deps** — added `psycopg2-binary` and `pymysql` to `[full]` and `[dev]` optional dependencies in `pyproject.toml`

---

## [1.1.52] — 2026-04-08

### Fixed
- **SQS queue URL hostname resolution** — `QueueUrl` with a different hostname (e.g. `http://ministack:4566/...` in docker-compose) now resolves correctly. The queue lookup extracts the queue name from the URL and falls back to name-based resolution when the exact URL doesn't match.
- **SQS FIFO dedup cache not cleared on message delete** — Deleting a FIFO message now clears its deduplication cache entry, so the same `MessageDeduplicationId` can be reused immediately. Previously, the 5-minute dedup window blocked re-sends even after the message was consumed and deleted, breaking test reruns with fixed dedup IDs. Reported by @mspiller
- **API Gateway deadlock when Lambda calls back to MiniStack** — Lambda invocations from API Gateway (both v1 REST and v2 HTTP) now run in a thread pool (`asyncio.to_thread`), preventing deadlock when the Lambda handler makes HTTP requests back to MiniStack. Contributed by @rankinjl (#191)

### Changed
- **Tests split into per-service files** — The monolithic `test_services.py` (21K lines) has been split into ~45 focused test files (`test_s3.py`, `test_sqs.py`, `test_ec2.py`, etc.). Contributed by @jgrumboe (#189)
- **Lambda runtime env vars set before handler load** — `LAMBDA_TASK_ROOT`, `AWS_LAMBDA_FUNCTION_NAME`, `AWS_LAMBDA_FUNCTION_MEMORY_SIZE`, and `_LAMBDA_FUNCTION_ARN` are now available at import time (cold start), matching real AWS Lambda behavior. Contributed by @lubond (#190)

---

## [1.1.51] — 2026-04-08

### Added
- **EC2 Launch Templates (6 operations)** — `CreateLaunchTemplate`, `CreateLaunchTemplateVersion`, `DescribeLaunchTemplates`, `DescribeLaunchTemplateVersions`, `ModifyLaunchTemplate`, `DeleteLaunchTemplate`. Full versioning support with `$Latest` / `$Default` resolution, block device mappings, network interfaces, IAM instance profiles, and tag specifications.
- **CFN `AWS::EC2::LaunchTemplate`** — Launch templates now work in CloudFormation/CDK stacks. 53 CFN resource types total.

### Fixed
- **KMS tags and policy not saved on key creation** — `CreateKey` was ignoring `Tags` and `Policy` parameters, so they were lost until explicitly set via `TagResource` / `PutKeyPolicy`. Contributed by @jgrumboe (#183)
- **SQS FIFO `ReceiveMessage` returns all messages in same group** — was incorrectly returning only 1 message per MessageGroupId per batch. AWS allows up to 10 messages from the same group in a single `ReceiveMessage` call; the per-group restriction only applies to subsequent calls while messages are in-flight. Reported by @mspiller (#179)

---

## [1.1.50] — 2026-04-08

### Added
- **CFN `AWS::ECS::Cluster`, `AWS::ECS::TaskDefinition`, `AWS::ECS::Service`** — ECS resources now work in CloudFormation/CDK stacks. 51 CFN resource types total.

---

## [1.1.49] — 2026-04-08

### Added
- **EventBridge `UpdateEventBus`** — new operation for Terraform `aws_cloudwatch_event_bus`. Contributed by @jgrumboe (#177)
- **EventBridge `Description` and `Policy` fields** — `DescribeEventBus` and `ListEventBuses` now return description, policy, and `LastModifiedTime`

### Fixed
- **Lambda `LAMBDA_EXECUTOR=docker` ignored for Python/Node runtimes** — warm pool always took priority over the Docker executor setting. Now `LAMBDA_EXECUTOR=docker` routes all runtimes through Docker for clean log output. Contributed by @PorterK (#178)
- **Lambda Docker fallback crash** — `runtime` referenced before definition when Docker SDK unavailable
- **EventBridge timestamps** — all timestamp fields now return epoch numbers instead of ISO strings. Fixes Terraform deserialization. Legacy ISO strings in persisted state auto-coerced on restore. Contributed by @jgrumboe (#177)

---

## [1.1.48] — 2026-04-07

### Added
- **S3 Files service (21 operations)** — CreateFileSystem, GetFileSystem, ListFileSystems, DeleteFileSystem, CreateMountTarget, GetMountTarget, ListMountTargets, UpdateMountTarget, DeleteMountTarget, CreateAccessPoint, GetAccessPoint, ListAccessPoints, DeleteAccessPoint, policies, synchronization config, tagging. First emulator to support AWS S3 Files (launched April 7 2026). 39 services total.
- **Step Functions query-protocol aws-sdk:* dispatcher** — extends the generic aws-sdk dispatcher to support query-protocol services: RDS, SQS, SNS, ElastiCache, EC2, IAM, STS, CloudWatch. XML responses automatically converted to JSON. Contributed by @jayjanssen (#174)
- **Cognito RSA JWT signing** — tokens now signed with the RSA private key matching the JWKS endpoint. Adds `username` claim to access tokens. Contributed by @MartinsMLX (#172)

### Tests
- Comprehensive aws-sdk:secretsmanager SFN task dispatch coverage. Contributed by @jayjanssen (#173)
- 1054 tests total

---

## [1.1.47] — 2026-04-07

### Added
- **Step Functions generic `aws-sdk:*` task dispatcher** — Task states can now call any MiniStack service via `arn:aws:states:::aws-sdk:<service>:<action>` resource ARNs. Supports all JSON-protocol services (DynamoDB, SecretsManager, ECS, KMS, etc.). Contributed by @jayjanssen (#168)
- **Step Functions sync execution error details** — `StartSyncExecution` now returns `error` and `cause` fields for failed executions, matching AWS SFN behaviour. Contributed by @jayjanssen

### Fixed
- **S3 `PutObject` missing `Content-Length: 0` header** — CDK deploy failed with `Expected real number, got implicit NaN` because the JS SDK v3 parsed the missing header as NaN. Reported by @youngkwangk (#160)
- **README reverts from stale PR branches** — restored Cloud Map, ready.d, persistence list, SFN intrinsics documentation


### Tests
- 3 new tests: SecretsManager round-trip via aws-sdk, DynamoDB round-trip via aws-sdk, unknown service error handling

---

## [1.1.46] — 2026-04-07

### Added
- **Cloud Map (Service Discovery)** — new service with namespace lifecycle (HTTP, private/public DNS), service/instance CRUD, operation tracking, tagging, Route53 hosted zone integration. Contributed by @jgrumboe (#147)
- **Step Functions intrinsic functions** — `States.StringToJson`, `States.JsonMerge`, `States.Format` in `Parameters` and `ResultSelector`. Supports nested intrinsic calls. Contributed by @jayjanssen (#167)
- **STS `GetAccessKeyInfo`** — returns account ID for a given access key
- **EC2 `ModifySnapshotAttribute` / `DescribeSnapshotAttribute`** — now actually stores and returns `createVolumePermission` instead of being stubs
- **`ready.d` scripts** — execute after server startup for resource seeding. Contributed by @kjdev (#159)

### Tests
- 3 WAF tests: check_capacity, describe_managed_rule_group, list_resources_for_web_acl. Contributed by @mvanhorn (#164)
- 2 STS tests: assume_role_returns_credentials, get_access_key_info. Contributed by @mvanhorn (#162)
- 3 EBS tests: snapshot_attribute, volume_attribute, volumes_modifications. Contributed by @mvanhorn (#163)

---
## [1.1.45] — 2026-04-07

### Added
- **CFN 8 EC2 resource types** — `AWS::EC2::VPC`, `AWS::EC2::Subnet`, `AWS::EC2::SecurityGroup`, `AWS::EC2::InternetGateway`, `AWS::EC2::VPCGatewayAttachment`, `AWS::EC2::RouteTable`, `AWS::EC2::Route`, `AWS::EC2::SubnetRouteTableAssociation`. CDK/CFN VPC stacks now deploy end-to-end. 48 CFN resource types total.
- **`ready.d` scripts** — shell scripts in `/docker-entrypoint-initaws.d/ready.d/` execute after the server is fully started and accepting connections. Enables seeding AWS resources (S3 buckets, SQS queues, etc.) on startup. Contributed by @kjdev (#159)

---

## [1.1.44] — 2026-04-06

### Added
- **CFN `AWS::IAM::ManagedPolicy`, `AWS::KMS::Key`, `AWS::KMS::Alias`** — completes full CDK bootstrap support. All 9 resource types in the CDKToolkit stack now work. Reported by @youngkwangk (#152)
- **Step Functions nested `startExecution.sync`** — parent workflows can now invoke child state machines synchronously via `arn:aws:states:::states:startExecution.sync` and `.sync:2`. Output shape matches AWS (`.sync` = JSON string, `.sync:2` = parsed JSON). Contributed by @jayjanssen (#157)

### Fixed
- **API Gateway v2 `lastUpdatedDate` returned as ISO8601 string** — Stage and Deployment `lastUpdatedDate` was returning Unix timestamp (number), causing Terraform deserialization failure on `aws_apigatewayv2_stage`. Reported by @hmarcuzzo (#132)
- **ECS timestamp wire format** — all ECS timestamp fields (`createdAt`, `startedAt`, `stoppedAt`, etc.) now return epoch numbers instead of ISO strings. Fixes SDK deserialization for Go, Java, and other typed SDKs

### Tests
- 4 new tests: EMR instance fleets, ECS timestamp format, API GW v2 stage timestamps, CDK bootstrap full stack

---

## [1.1.43] — 2026-04-06

### Added
- **CFN `AWS::ECR::Repository`** — CDK bootstrap (`cdk bootstrap`) now works. Reported by @youngkwangk (#152)
- **SecretsManager `UpdateSecretVersionStage`** — move staging labels between secret versions. Enables rotation flows with AWSCURRENT/AWSPREVIOUS rollover. Contributed by @jayjanssen (#155)

---

## [1.1.42] — 2026-04-06

### Added
- **RDS configurable tmpfs size** — `RDS_TMPFS_SIZE` env var (default `256m`). Set to `2g` or higher for large database testing
- **CloudFront tagging** — `TagResource`, `UntagResource`, `ListTagsForResource` for distributions. Enables Terraform CloudFront with tags

### Fixed
- **Step Functions timestamp wire format** — responses now return epoch numbers instead of ISO strings for timestamp fields (`creationDate`, `startDate`, `stopDate`, etc.). Fixes Go SDK v2 and botocore deserialization failures. Contributed by @jayjanssen (#151)

---

## [1.1.41] — 2026-04-06

### Fixed
- **ElastiCache persistence crash on restart** — `restore_state()` called `_get_docker()` before it was defined, causing `NameError` when `PERSIST_STATE=1`. Reported by @adamkirk (#145)
- **RDS persistence crash on restart** — same `_get_docker()` ordering issue in `restore_state()`

---

## [1.1.40] — 2026-04-06

### Added
- **State persistence for ALL services** — 11 remaining services now support `PERSIST_STATE=1`: ALB, Glue, EFS, WAF, Athena, EMR, CloudFront, ACM, Firehose, SES, SES v2. All 35+ services now persist state across restarts.
- **Step Functions persistence** — state machines, executions, tags, and activities persist. RUNNING executions restored as FAILED with `States.ServiceRestart`. Contributed by @TheJokersThief (#141)
- **IAM `ListEntitiesForPolicy`** — returns users, roles, and groups attached to a managed policy. Supports `EntityFilter` and `PathPrefix`. Contributed by @TheJokersThief (#143)

### Tests
- 5 cross-service integration tests: S3→SQS events, SNS→SQS fanout, DynamoDB streams→Lambda, SQS ESM→Lambda, CloudFormation full stack (S3+Lambda+DynamoDB). Contributed by @DaviReisVieira (#142)

---

## [1.1.39] — 2026-04-06

### Fixed
- **AppSync persistence crash on restart** — `restore_state()` called before it was defined in the file, causing `NameError` when `PERSIST_STATE=1` and restarting. Reported by @samiuoi (#66)
- **Cognito `AdminSetUserPassword` with `Permanent=false`** — now correctly sets `UserStatus` to `FORCE_CHANGE_PASSWORD`. Previously the password was updated but the status wasn't changed.

### Community
- **README: Community Integrations section** — [StackPort](https://github.com/DaviReisVieira/stackport) visual dashboard by @DaviReisVieira, [Aspire Hosting](https://github.com/McDoit/aspire-hosting-ministack) .NET integration by @McDoit

### Tests
- 10 new tests: KMS (list policies, rotation period), ElastiCache (parameter groups, snapshots, tags), Lambda (Image CRUD, update ImageUri, provided runtime), SecretsManager (rotate secret), Firehose (S3 destination writes)
- 1011 tests total

---

## [1.1.38] — 2026-04-05

### Added
- **ECS 19 new operations (47 total)** — `ListTaskDefinitionFamilies`, `DeleteTaskDefinitions`, `ListServicesByNamespace`, `PutAccountSettingDefault`, `DeleteAccountSetting`, `PutAttributes`, `DeleteAttributes`, `ListAttributes`, `UpdateCapacityProvider`, `DescribeServiceDeployments`, `ListServiceDeployments`, `DescribeServiceRevisions`, `SubmitTaskStateChange`, `SubmitContainerStateChange`, `SubmitAttachmentStateChanges`, `DiscoverPollEndpoint`, `UpdateTaskProtection`, `GetTaskProtection`. Full Terraform ECS coverage.
- **SES SMTP relay via `SMTP_HOST`** — when set (e.g. `mailhog:1025`), SendEmail/SendRawEmail/SendTemplatedEmail/SendBulkTemplatedEmail deliver to an external SMTP server. Zero impact when unset. Contributed by @kjdev (#131)
- **Docker socket documentation** — README quickstart now shows `-v /var/run/docker.sock` for RDS, ECS, and Lambda container features

### Fixed
- **API Gateway v2 `CreatedDate` returned as ISO8601 string** — was returning Unix timestamp (number), causing Terraform AWS Provider v5/v6 deserialization failure on `aws_apigatewayv2_api`. Reported by @hmarcuzzo (#132)

---

## [1.1.37] — 2026-04-05

### Added
- **Lambda `PackageType: Image` support** — Lambda functions can now be deployed as Docker images via `Code: { ImageUri: "..." }`. The user-provided image is pulled and invoked via the Lambda Runtime Interface Emulator (port 8080). Supports Go, Rust, Java, or any language packaged as a Lambda container image. `CreateFunction`, `UpdateFunctionCode`, `GetFunction` all handle `ImageUri`. Requested by @petherin (#67)

---

## [1.1.36] — 2026-04-04

### Added
- **EC2 `ReplaceRouteTableAssociation`** — moves a subnet association from one route table to another; completes full Terraform route table association lifecycle
- **EC2 `ModifyVpcEndpoint`** — add/remove route tables, subnets, and policy on existing VPC endpoints
- **EC2 `DescribePrefixLists`** — returns AWS service prefix lists (S3, DynamoDB) and user-managed prefix lists; required by Terraform for every VPC endpoint
- **EC2 Managed Prefix Lists** — `CreateManagedPrefixList`, `DescribeManagedPrefixLists`, `GetManagedPrefixListEntries`, `ModifyManagedPrefixList`, `DeleteManagedPrefixList`; supports versioned CIDR entry management
- **EC2 VPN Gateways** — `CreateVpnGateway`, `DescribeVpnGateways`, `AttachVpnGateway`, `DetachVpnGateway`, `DeleteVpnGateway`; includes attachment state tracking and `attachment.vpc-id` filter
- **EC2 VPN Route Propagation** — `EnableVgwRoutePropagation`, `DisableVgwRoutePropagation`; tracks propagating VGWs on route tables
- **EC2 Customer Gateways** — `CreateCustomerGateway`, `DescribeCustomerGateways`, `DeleteCustomerGateway`
- **Lambda `provided` runtime support** — `provided.al2023`, `provided.al2` runtimes now execute via Docker using the AWS Lambda RIE; code is mounted to `/var/task` matching real AWS behavior; Go, Rust, and C++ Lambda functions work correctly with companion files accessible at `LAMBDA_TASK_ROOT`
- **KMS Terraform support** — `EnableKeyRotation`, `DisableKeyRotation`, `GetKeyRotationStatus`, `GetKeyPolicy`, `PutKeyPolicy`, `ListKeyPolicies`, `EnableKey`, `DisableKey`, `ScheduleKeyDeletion`, `CancelKeyDeletion`, `TagResource`, `UntagResource`, `ListResourceTags`; KMS now has 27 actions (was 14). Fixes Terraform `aws_kms_key` with `enable_key_rotation = true`. Reported by @betorvs
- **Docker image: `cryptography` package included** — KMS RSA Sign/Verify/GetPublicKey now work out of the box in the Docker image (+20MB image size, 211MB → 231MB)

### Stats
- EC2 now supports **127 actions** (was 109)
- Full Terraform VPC module coverage: 98/98 actions for 20 resource types

### Tests
- 988 tests total, all passing

---

## [1.1.35] — 2026-04-04

### Fixed
- **EC2 `CreateVpc` creates per-VPC default resources** — each new VPC now gets its own main route table, default network ACL (with standard allow/deny rules), and default security group. Previously all VPCs shared global defaults, so Terraform couldn't find VPC-specific resources
- **EC2 `DescribeNetworkAcls` `default` filter** — Terraform looks up `default_network_acl_id` via `DescribeNetworkAcls` with `vpc-id` + `default=true`, not from the VPC object. Now works
- **EC2 `DescribeSecurityGroups` `vpc-id`/`group-name` filters** — Terraform looks up `default_security_group_id` via these filters. Now works
- **EC2 `DescribeRouteTables` `association.main` filter** — Terraform finds the main route table for a VPC using this filter. Now works
- **EC2 route target types preserved** — `CreateRoute`/`ReplaceRoute` now store `NatGatewayId`, `InstanceId`, `VpcPeeringConnectionId`, `TransitGatewayId` as distinct fields; XML output uses correct element names

### Reported by
- @betorvs — Terraform VPC module v6.6.0 `default_network_acl_id` missing (#107, #108)

---

## [1.1.34] — 2026-04-04

### Fixed
- **EC2 `DescribeRouteTables` filter by association ID** — `association.route-table-association-id`, `association.subnet-id`, `vpc-id` filters now supported. Fixes Terraform 5-minute timeout polling route table associations after `AssociateRouteTable`. Reported by @betorvs (#107, #108)

---

## [1.1.33] — 2026-04-04

### Added
- **DynamoDB `ScanFilter` / `QueryFilter`** — legacy filter conditions (EQ, NE, NOT_NULL, NULL, CONTAINS, BEGINS_WITH) now supported alongside FilterExpression
- **CFN `AWS::AppSync::*`** — GraphQLApi, DataSource, Resolver, GraphQLSchema, ApiKey provisioners for CDK/Amplify stacks
- **CFN `AWS::SecretsManager::Secret`** — with `GenerateSecretString` support (PasswordLength, ExcludeCharacters, SecretStringTemplate, GenerateStringKey)
- **S3 `UploadPartCopy`** — copy a range from an existing object as a multipart upload part; supports `x-amz-copy-source-range`
- **SNS FIFO dedup passthrough** — `MessageGroupId` and `MessageDeduplicationId` from SNS Publish now forwarded to SQS FIFO queues via fanout
- **AppSync GraphQL data plane** — `POST /v1/apis/{apiId}/graphql` executes queries and mutations against DynamoDB resolvers; supports create/get/list/update/delete operations, nested input objects, field selection, Lambda resolvers; enables Amplify Data runtime
- **CFN Cognito resource types** — `AWS::Cognito::UserPool`, `AWS::Cognito::UserPoolClient`, `AWS::Cognito::IdentityPool`, `AWS::Cognito::UserPoolDomain` for Amplify/CDK auth stacks

### Fixed
- **DynamoDB persistence crash** — `defaultdict(dict)` deserialized as plain `dict` after restart, causing `KeyError` on new partition keys. Now converts back to `defaultdict` on restore
- **DynamoDB `_pitr_settings` not persisted** — `DescribeContinuousBackups` now survives restarts
- **Cognito JWT `kid` mismatch** — tokens now use `kid: ministack-key-1` matching the JWKS endpoint; fixes client-side JWT validation
- **KMS RSA private keys persisted** — private keys now PEM-encoded in state; Sign/Verify work after restart (requires `cryptography` package)
- **4 duplicate test function names** — `test_lambda_publish_version`, `test_kinesis_stream_encryption`, `test_apigw_delete_route` renamed to unique names; previously only last definition ran
- **EC2 Terraform VPC module fixes** — `DescribeAddressesAttribute`, `DescribeSecurityGroupRules`, route table association state (`associated`), VPC `defaultNetworkAclId`/`defaultSecurityGroupId`/`mainRouteTableId` in CreateVpc/DescribeVpcs responses. Reported by @betorvs

### Tests
- 971 tests total, all passing

---

## [1.1.32] — 2026-04-04

### Added
- **AppSync service** — CreateGraphQLApi, GetGraphQLApi, ListGraphQLApis, UpdateGraphQLApi, DeleteGraphQLApi, CreateApiKey, ListApiKeys, DeleteApiKey, CreateDataSource, GetDataSource, ListDataSources, DeleteDataSource, CreateResolver, GetResolver, ListResolvers, DeleteResolver, CreateType, ListTypes, GetType, TagResource, UntagResource, ListTagsForResource; REST/JSON API under `/v1/apis`; in-memory state with persistence
- **Cognito JWKS/OIDC endpoints** — `/.well-known/jwks.json` returns real RSA public key; `/.well-known/openid-configuration` returns OpenID Connect discovery document; enables real JWT validation in Amplify/CDK auth flows
- **9 new CloudFormation resource types** — `AWS::ApiGateway::RestApi`, `AWS::ApiGateway::Resource`, `AWS::ApiGateway::Method`, `AWS::ApiGateway::Deployment`, `AWS::ApiGateway::Stage`, `AWS::Lambda::EventSourceMapping`, `AWS::Lambda::Alias`, `AWS::SQS::QueuePolicy`, `AWS::SNS::TopicPolicy`; unblocks Serverless Framework and CDK deployments
- **EC2 `DescribeVpcAttribute`** — returns EnableDnsSupport, EnableDnsHostnames, EnableNetworkAddressUsageMetrics; fixes Terraform VPC module failing after ModifyVpcAttribute. Reported by @betorvs

### Tests
- 955 tests total, all passing

---

## [1.1.31] — 2026-04-04

### Fixed
- **S3→Lambda notifications silently failing** — `_invoke` is async but was called from sync context; coroutine was never awaited. Now uses direct `_execute_function` in background thread
- **SNS HTTP delivery crash from background threads** — `asyncio.ensure_future` fails with no event loop when `_fanout` called from S3/EventBridge threads. Now uses `threading.Thread(target=asyncio.run, ...)`
- **ACCOUNT_ID configurable across all services** — `MINISTACK_ACCOUNT_ID` env var now respected by all 37 services and router; previously only 6 services read it
- **EventBridge SQS dispatch missing message fields** — now calls `_ensure_msg_fields` after appending, preventing KeyError on ReceiveMessage
- **README: stale test count and service count** — updated to 948 tests, 37 services
- **README: CloudFront in Terraform endpoints, architecture diagram, comparison table**

### Tests
- 948 tests total, all passing

---

## [1.1.30] — 2026-04-03

### Added
- **CloudFormation `AWS::Lambda::Permission`** — provisions Lambda invoke permissions via CFN stacks
- **CloudFormation `AWS::Lambda::Version`** — creates immutable Lambda versions via CFN stacks
- **CloudFormation `AWS::CloudFormation::WaitCondition`** — no-op stub, returns immediately
- **CloudFormation `AWS::CloudFormation::WaitConditionHandle`** — no-op stub, returns placeholder URL

### Fixed
- **Router duplicate action keys** — removed `ListTagsForResource` and `GetTemplate` from action map (shared across services, routed via credential scope instead)
- **ElastiCache reset missing state** — `_param_group_params` now cleared and `_port_counter` reset to `BASE_PORT` on reset
- **Bare except in Docker cleanup** — RDS, ECS, ElastiCache reset() now log warnings instead of silently swallowing errors
- **52 f-string logger calls** — converted to lazy % formatting across 13 service files; avoids unnecessary string formatting when log level is disabled
- **Detached mode log handle** — documented intentional fd inheritance in subprocess.Popen

Thanks to @moabukar for #104 (error handling, routing conflicts, persistence hardening)

---

## [1.1.29] — 2026-04-03

### Fixed
- **CloudFormation `AWS::S3::BucketPolicy`** — new resource type; provisions and deletes S3 bucket policies via CFN stacks. Fixes Serverless Framework deployment failures

---

## [1.1.28] — 2026-04-03

### Fixed
- **S3 aws-chunked decoding** — chunked body decoder now also triggers on `Content-Encoding: aws-chunked` and `x-amz-decoded-content-length` header, not only `STREAMING-*`; fixes AWS SDK Java v2 and Spring Boot S3Template storing raw chunk metadata in object bodies. Strips `aws-chunked` from Content-Encoding before passing to S3 handler. Contributed by @moabukar

---

## [1.1.27] — 2026-04-03

### Fixed
- **Dockerfile missing `defusedxml`** — added `defusedxml>=0.7` to pip install in Dockerfile; container was crashing on startup due to missing dependency introduced in v1.1.26

---

## [1.1.26] — 2026-04-03

### Added
- **CloudFront service** — CreateDistribution, GetDistribution, GetDistributionConfig, ListDistributions, UpdateDistribution, DeleteDistribution, CreateInvalidation, ListInvalidations, GetInvalidation; ETag-based concurrency control. Contributed by @Nikhiladiga
- **ECR service** — CreateRepository, DescribeRepositories, DeleteRepository, PutImage, BatchGetImage, BatchDeleteImage, ListImages, DescribeImages, GetAuthorizationToken, lifecycle policies, repository policies, tags, layer upload flow. Contributed by @moabukar
- **IAM DeleteServiceLinkedRole / GetServiceLinkedRoleDeletionStatus** — Contributed by @jgrumboe
- **State persistence for 10 more services** — Lambda (config + code_zip as base64), EC2, Route53, Cognito, ECR, CloudWatch Metrics, S3 metadata, RDS (reconnects Docker containers), ECS (tasks restored as stopped), ElastiCache (reconnects Docker containers) now persist when `PERSIST_STATE=1` (20 services total)
- **SNS/SFN pagination** — ListTopics, ListSubscriptions, ListStateMachines, ListExecutions now support NextToken/maxResults
- **defusedxml** — S3 and Route53 XML parsing now uses `defusedxml` to protect against billion-laughs DoS

### Fixed
- **SecretsManager `BatchGetSecretValue`** — retrieve multiple secrets in one call; returns `SecretValues` and `Errors` arrays
- **DynamoDB `WarmThroughput`** — DescribeTable now returns `WarmThroughput` field; fixes latest Terraform AWS provider compatibility. Reported by @chad-bekmezian-snap
- **Firehose deadlock** — `_next_dest_id` no longer acquires lock (always called within `_lock` context)
- **Redis bound to localhost** — docker-compose.yml Redis port now `127.0.0.1:6379:6379`
- **EDGE_PORT documented** — added to README Configuration table as LocalStack alias

### Tests
- 928 tests total, all passing

---

## [1.1.25] — 2026-04-03

### Added
- **State persistence for 10 services** — SQS, SNS, SSM, SecretsManager, IAM, DynamoDB, KMS, EventBridge, CloudWatch Logs, and Kinesis now persist state when `PERSIST_STATE=1`; state is saved on shutdown and restored on startup via atomic JSON files
- **Python Testcontainers example** — `Testcontainers/python-testcontainers/` with pytest tests for S3, SQS, DynamoDB using the `testcontainers` package
- **Detached mode** — `ministack -d` starts the server in the background with logs to `/tmp/ministack-{port}.log`; `ministack --stop` stops it. Cross-platform via `subprocess.Popen`. PID file with signal cleanup. Reported by @UdayKiranPadhy

### Fixed
- **Renamed `examples/` to `Testcontainers/`** — clearer folder name for Testcontainers examples (Java, Go, Python)
- **EventBridge SQS dispatch message schema** — fixed field names (`md5_body`, `sys`, `message_attributes`) to match SQS internal format
- **Lambda `_now_iso()` millisecond precision** — now includes real milliseconds instead of always `.000`
- **`x-amz-id-2` header** — now returns base64-encoded random bytes instead of a UUID, matching AWS format
- **Route53 `ListResourceRecordSets` ordering and pagination** — DNS names now sorted by reversed labels (`com.example.www`) matching AWS; pagination cursors point to next page start instead of current page end; fixes Terraform infinite loop on `aws_route53_record`. Contributed by @jgrumboe
- **Lazy stdlib imports removed** — moved `shutil`, `tempfile`, `argparse`, `signal`, `socket`, `sys`, `datetime` to module level across `app.py`, `lambda_svc.py`, `athena.py`
- **Flaky ESM visibility timeout test** — increased timeout headroom for CI environments

### Tests
- 887 tests total, all passing

---

## [1.1.24] — 2026-04-03

### Fixed
- **KMS aliases** — CreateAlias, DeleteAlias, ListAliases, UpdateAlias; `alias/my-key` resolves in Encrypt, Decrypt, Sign, Verify, DescribeKey and all other KMS operations
- **KMS `REGION` hardcoded** — now reads `MINISTACK_REGION` env var like all other services
- **S3 hardcoded `us-east-1`** — bucket region header, location constraint, and event notifications now use `MINISTACK_REGION`
- **Router `extract_region` fallback** — now uses `MINISTACK_REGION` instead of hardcoded `us-east-1`
- **EC2/RDS XML escaping** — user-controlled values (tags, descriptions) now escaped with `xml.sax.saxutils.escape()`
- **SQS thread safety** — added `_queues_lock` for ESM poller access
- **EC2 terminated instances cleaned up** — removed from memory after 60s
- **Step Functions execution cleanup** — cleaned up when parent state machine is deleted
- **Lambda ESM poller idle optimization** — polls every 5s when no ESMs configured
- **DynamoDB `REGION` variable ordering** — moved before `_emit_stream_event`
- **README: 55+ undocumented operations** — updated all service tables
- **README: `SFN_MOCK_CONFIG`** — added to Configuration table
- **README: KMS in Terraform endpoints**

### Tests
- 876 tests total, all passing

---

## [1.1.23] — 2026-04-03

### Added
- **KMS service** — CreateKey (RSA_2048, RSA_4096, SYMMETRIC_DEFAULT), ListKeys, DescribeKey, GetPublicKey, Sign, Verify, Encrypt, Decrypt, GenerateDataKey, GenerateDataKeyWithoutPlaintext. In-memory key storage with RSA signing via the `cryptography` package (optional dependency, guarded import). Supports JWT signing flows and S3 SSE-KMS encryption patterns. Contributed by @Jolley71717

---

## [1.1.22] — 2026-04-03

### Added
- **Step Functions mock config** — `SFN_MOCK_CONFIG` (or `LOCALSTACK_SFN_MOCK_CONFIG`) env var pointing to a JSON file that mocks Task state responses; fully compatible with the AWS Step Functions Local mock config format: `MockedResponses` with invocation indexing (`"0"`, `"1-2"`, etc.), `#TestCaseName` ARN suffix on `StartExecution`, `Return` and `Throw` per attempt. Contributed by @maxence-leblanc (issue)
- **Step Functions `TestState` API** — execute a single state in isolation without creating a state machine; supports Pass, Task, Choice, Wait, Succeed, Fail state types; `inspectionLevel` (INFO/DEBUG) returns data transformation details; `mock` parameter for Task states with `result`/`errorOutput`; `stateName` to extract a state from a full definition; Retry/Catch evaluation with `RETRIABLE`/`CAUGHT_ERROR` status

### Fixed
- **CloudWatch Logs `GetLogEvents` pagination** — `nextForwardToken` and `nextBackwardToken` now return the caller's token when at end of stream, preventing SDK clients from looping infinitely; token-based offset pagination now works correctly
- **EventBridge → Lambda crash** — `asyncio.run()` inside the running event loop replaced with direct synchronous dispatch; PutEvents with Lambda targets no longer crashes
- **Step Functions StartSyncExecution crash** — `_call_lambda` replaced `asyncio.run()` with direct `_execute_function()` call; sync Lambda Task states no longer crash
- **`/_ministack/config` endpoint hardened** — now whitelists allowed config keys instead of accepting arbitrary `__import__` + `setattr` on any module
- **S3 path traversal in persistence** — `_persist_object` validates paths stay within `DATA_DIR` using `os.path.realpath()` prefix check; blocks `../` in S3 keys
- **Lambda worker reset** — `reset()` now acquires lock and calls `worker.kill()` (cleans up temp dirs) instead of bare `_proc.terminate()`
- **DynamoDB `_stream_records` cleared on reset** — stream records no longer accumulate unboundedly across resets
- **Lambda ESM position tracking cleared on reset** — `_kinesis_positions` and `_dynamodb_stream_positions` now cleared on `reset()`
- **License** — updated year 2026

### Tests
- 851 tests total, all passing

---

## [1.1.21] — 2026-04-02

### Added
- **S3 → EventBridge notifications** — buckets with `EventBridgeConfiguration` enabled now publish events to the default EventBridge bus on object create/delete/copy; EventBridge rules with `InputTransformer` route and reshape events to downstream targets (SQS, Lambda, etc.)

### Fixed
- **S3 `PutObject` missing `VersionId` in response** — versioned buckets now return `VersionId` in the `PutObject`, `GetObject`, `HeadObject`, and `CopyObject` responses; each put generates a unique version ID. Reported by @McDoit

### Tests
- 841 tests total, all passing

---

## [1.1.20] — 2026-04-02

### Fixed
- **SecretsManager `KmsKeyId`** — `CreateSecret` and `UpdateSecret` now store `KmsKeyId`; `DescribeSecret` returns it. Previously always null.
- **Lambda env vars applied at process spawn** — Lambda environment variables are now passed to the worker subprocess at startup (`env=` on `Popen`) instead of after via `Object.assign`. `NODE_OPTIONS=--require ./init.js` and similar process-level env vars now work correctly, matching real AWS Lambda behaviour. Contributed by @jv2222

### Tests
- 838 tests total, all passing

---

## [1.1.19] — 2026-04-02

- Version bump from v1.1.18 — no code changes, re-tag for PyPI publish

---

## [1.1.18] — 2026-04-02

### Added
- **EC2 `DescribeInstanceCreditSpecifications`** — returns `standard` CPU credits; fixes Terraform v6 provider compatibility
- **EC2 Terraform v6 stubs** — `DescribeInstanceMaintenanceOptions`, `DescribeInstanceAutoRecoveryAttribute`, `ModifyInstanceMaintenanceOptions`, `DescribeInstanceTopology`, `DescribeSpotInstanceRequests`, `DescribeCapacityReservations` all return sensible empty/default responses
- **Lambda Node.js warm worker pool** — Node.js functions now use the same persistent warm worker as Python; supports async/await, Promise, and callback handlers; AWS SDK v2 endpoint patching for local development
- **Docker image includes Node.js** — `nodejs` added to Alpine base image so container-based Node.js Lambda execution works out of the box in Docker Compose / CI environments
- **Lambda S3 code fetch** — `CreateFunction` and `UpdateFunctionCode` now accept `S3Bucket`/`S3Key` in addition to `ZipFile`; returns error if S3 object not found
- **Lambda versioning** — `Publish=True` on `CreateFunction` and `UpdateFunctionCode` now creates immutable numbered versions with their own `code_zip`
- **DynamoDB Streams** — `StreamSpecification` on `CreateTable` now emits INSERT/MODIFY/REMOVE records on all write operations (`PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem`, `TransactWriteItems`); respects `StreamViewType`
- **Kinesis ESM polling** — Lambda event source mappings now support Kinesis streams in addition to SQS

### Fixed
- **SNS `Subscribe` ignores `Attributes` parameter** — `RawMessageDelivery`, `FilterPolicy`, `FilterPolicyScope`, `DeliveryPolicy`, and `RedrivePolicy` passed at subscription creation time are now applied immediately
- **Lambda warm worker not invalidated on code update** — `UpdateFunctionCode` and `DeleteFunction` now invalidate the warm worker pool so the next invocation picks up the new code
- **Lambda module-level imports** — removed lazy `from ministack.core.lambda_runtime import` inside functions; moved to module top level
- **S3 chunked transfer encoding** — AWS SDK v2 sends `PutObject` with `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` chunked encoding; body was stored with chunk headers causing corrupt `GetObject` responses; now decoded before storage
- **Kinesis validation limits** — `PutRecord` and `PutRecords` now enforce AWS limits: max 1 MB per record, max 500 records per batch, max 5 MB total payload, max 256-char partition key
- **S3 Control routing via `s3-control.localhost` host** — requests with host header `s3-control.localhost` were intercepted by the S3 virtual-hosted bucket handler instead of reaching the S3 Control API; fixes Terraform `ListTagsForResource` returning 404 `NoSuchResource`
- **EC2 security group rule deduplication** — `AuthorizeSecurityGroupIngress/Egress` no longer appends duplicate rules; fixes Terraform showing constant drift
- **EC2 default egress rule on created security groups** — non-default security groups now include the standard allow-all egress rule matching AWS behaviour
- **EC2 VPC Peering missing Region field** — `requesterVpcInfo` and `accepterVpcInfo` now include `<region>` in all responses; fixes Terraform failing to parse peering connections
- **Lambda `PublishVersion` FunctionArn** — no longer appends version number to FunctionArn (version is in the Version field); fixes Terraform ARN comparison drift
- **Lambda `FunctionUrlConfig` hardcoded region** — now uses `MINISTACK_REGION` instead of hardcoded `us-east-1`
- **Lambda handler validation** — returns proper `Runtime.InvalidEntrypoint` error if handler name has no `.` separator instead of crashing
- **RDS error code** — `DBInstanceAlreadyExists` corrected to `DBInstanceAlreadyExistsFault` matching AWS error codes

- Thanks to @lubond @jimmyd-be @abedurftig @mig_mit for reporting issues and testing                       
- Thanks to @jv2222 and @santiagodoldan for their massive contributions

### Tests
- 834 tests total, all passing

---

## [1.1.17] — 2026-04-02

### Added
- **EC2 `DescribeInstanceCreditSpecifications`** — returns `standard` CPU credits; fixes Terraform v6 provider compatibility
- **EC2 Terraform v6 stubs** — `DescribeInstanceMaintenanceOptions`, `DescribeInstanceAutoRecoveryAttribute`, `ModifyInstanceMaintenanceOptions`, `DescribeInstanceTopology`, `DescribeSpotInstanceRequests`, `DescribeCapacityReservations` all return sensible empty/default responses to prevent Terraform v6 from failing on unknown actions

### Tests
- 818 tests total, all passing

---

## [1.1.16] — 2026-04-01

### Added
- **`MINISTACK_REGION` environment variable** — all 25 services now read region from `MINISTACK_REGION` (defaulting to `us-east-1`); previously all services hardcoded the region in ARNs and response metadata. Lambda also checks `AWS_DEFAULT_REGION` as a secondary fallback. Contributed by @xingzihai and @santiagodoldan

### Tests
- 815 tests total, all passing

---

## [1.1.15] — 2026-04-01

### Added
- **Lambda Node.js runtime** — `nodejs14.x` through `nodejs22.x` (and any future `nodejsN.x`) now fully execute via local subprocess (`node`) or Docker; supports `CreateFunction`, `UpdateFunctionCode`, `Invoke` including async handlers; layers resolved to `nodejs/node_modules`; `nodejs24.x` auto-maps via pattern

### Fixed
- **CloudFormation auto-generated physical names** — resources without explicit names now follow the AWS pattern `{stackName}-{logicalId}-{SUFFIX}` with a 13-char uppercase alphanumeric suffix; service-specific rules applied (S3: lowercase, max 63; SQS: max 80; DynamoDB: max 255; Lambda/IAM/EventBridge: max 64). Fixes CDK stacks that omit explicit resource names producing untraceable `cfn-xxx` names
- **Import cleanup** — moved lazy stdlib imports (`base64`, `fnmatch`, `re`, `datetime`, `urllib`) to module level across `sqs`, `cloudwatch_logs`, `glue`, `cognito`, `rds`, `apigateway`, `apigateway_v1`; removed duplicate `os`/`re` imports in `s3`

### Tests
- 3 new Node.js Lambda tests (create+invoke, nodejs22.x, UpdateFunctionCode)
- 4 new CFN physical name tests (S3/SQS/DynamoDB auto-name pattern, explicit name not overridden)
- 815 tests total, all passing

---

## [1.1.14] — 2026-04-01

### Added
- **Lambda layer enhancements** — `GetLayerVersionByArn`, `AddLayerVersionPermission`, `RemoveLayerVersionPermission`, `GetLayerVersionPolicy`; layer zip content served via `/_ministack/lambda-layers/{name}/{ver}/content` so runtimes can fetch layers; `ListLayerVersions` and `ListLayers` now support runtime and architecture filtering with pagination. Contributed by @mickabd
- **`MINISTACK_HOST` environment variable** — controls the hostname used in all response URLs (`QueueUrl`, SNS `SubscribeURL`/`UnsubscribeURL`, API Gateway `apiEndpoint`/`domainName`, CFN-provisioned SQS queues, Lambda layer `Content.Location`). Defaults to `localhost`. Set to your Docker Compose service name (e.g. `ministack`) so other containers can reach returned URLs directly. Contributed by @santiagodoldan and @David2011Hernandez

### Fixed
- **EC2 `DescribeInstanceAttribute`** — added support for all standard attributes (`instanceType`, `instanceInitiatedShutdownBehavior`, `disableApiTermination`, `userData`, `rootDeviceName`, `blockDeviceMapping`, `sourceDestCheck`, `groupSet`, `ebsOptimized`, `enaSupport`, `sriovNetSupport`); required by Terraform AWS Provider >= 6.0.0 during state refresh. Contributed by @samiuoi
- **EC2 `DescribeInstanceTypes`** — added handler returning hardware specs (vCPU, memory, network, EBS) for 12 common instance families (t2, t3, m5, c5, r5, p3); required by Terraform AWS Provider >= 6.0.0
- **S3 Control `ListTagsForResource`** — was always returning an empty tag list; now returns tags set via `PutBucketTagging`. Fixes Terraform `aws_s3_bucket` perpetual drift when a `tags` block is configured
- **Lambda layer `Content.Location`** — URL now respects `MINISTACK_HOST` and `GATEWAY_PORT` instead of hardcoded `localhost`

### Changed
- Virtual-hosted S3 and execute-api host-header matching now respects `MINISTACK_HOST`, so `{bucket}.<host>` and `{apiId}.execute-api.<host>` patterns work with any configured hostname

### Tests
- **CloudFormation e2e suite merged** — `test_cfn_e2e.py` merged into `test_services.py`; 10 e2e tests now run within the unified test session
- 19 new tests (EC2, S3 Control, Lambda layer permissions/pagination/filtering/GetByArn/content)
- 808 tests total, all passing

---

## [1.1.13] — 2026-04-01

### Added
- **CloudFormation** — full stack lifecycle: `CreateStack`, `UpdateStack`, `DeleteStack`, `DescribeStacks`, `ListStacks`, `DescribeStackEvents`, `DescribeStackResource`, `DescribeStackResources`, `GetTemplate`, `ValidateTemplate`, `GetTemplateSummary`, `ListExports`; change sets (`CreateChangeSet`, `DescribeChangeSet`, `ExecuteChangeSet`, `DeleteChangeSet`, `ListChangeSets`); JSON and YAML template support including `!Ref`, `!Sub`, `!GetAtt` shorthand; full intrinsic function resolution (`Ref`, `Fn::GetAtt`, `Fn::Join`, `Fn::Sub`, `Fn::Select`, `Fn::Split`, `Fn::If`, `Fn::Base64`, `Fn::FindInMap`, `Fn::ImportValue`, `Fn::GetAZs`, `Fn::Cidr`); conditions (`Fn::Equals`, `Fn::And`, `Fn::Or`, `Fn::Not`); parameters with `AllowedValues`, `Default`, `NoEcho`; rollback on failure with reverse-order cleanup; cross-stack exports via `Fn::ImportValue`; 12 resource types provisioned directly into service state (`AWS::S3::Bucket`, `AWS::SQS::Queue`, `AWS::SNS::Topic`, `AWS::SNS::Subscription`, `AWS::DynamoDB::Table`, `AWS::Lambda::Function`, `AWS::IAM::Role`, `AWS::IAM::Policy`, `AWS::IAM::InstanceProfile`, `AWS::SSM::Parameter`, `AWS::Logs::LogGroup`, `AWS::Events::Rule`). Contributed by @sam-fakhreddine

### Fixed
- **CloudFormation Lambda `ZipFile`** — inline `Code.ZipFile` source is now correctly packaged into a zip archive, making CFN-deployed Lambda functions invokable
- **CloudFormation async task** — replaced deprecated `asyncio.ensure_future()` with `asyncio.get_event_loop().create_task()` in stack deploy, delete, and change set execution
- **README architecture diagram** — fixed box alignment and added CloudFormation to service list. Contributed by @oefrha (HackerNews)

### Tests
- 788 tests total (before v1.1.14 additions)

---

## [1.1.12] — 2026-03-31

### Changed
- Updated LICENSE copyright year to 2026. Contributed by @kay_o (HackerNews)

---

## [1.1.11] — 2026-03-31

### Added
- **ACM (Certificate Manager)** — full control plane: `RequestCertificate`, `DescribeCertificate`, `ListCertificates`, `DeleteCertificate`, `GetCertificate`, `ImportCertificate`, `AddTagsToCertificate`, `RemoveTagsFromCertificate`, `ListTagsForCertificate`, `UpdateCertificateOptions`, `RenewCertificate`, `ResendValidationEmail`; certificates issued immediately with status `ISSUED` and DNS validation records; compatible with Terraform `aws_acm_certificate` and CDK `Certificate`
- **SES v2** — REST API at `/v2/email/`: `SendEmail`, `CreateEmailIdentity`, `GetEmailIdentity`, `DeleteEmailIdentity`, `ListEmailIdentities`, `CreateConfigurationSet`, `GetConfigurationSet`, `DeleteConfigurationSet`, `ListConfigurationSets`, `GetAccount`, `ListSuppressedDestinations`, `TagResource`, `UntagResource`, `ListTagsForResource`; identities auto-verified; compatible with Terraform `aws_sesv2_email_identity` and CDK `EmailIdentity`
- **WAF v2** — full control plane: WebACL CRUD, IPSet CRUD, RuleGroup CRUD (including `UpdateRuleGroup`), `AssociateWebACL`/`DisassociateWebACL`, `GetWebACLForResource`, `ListResourcesForWebACL`, `TagResource`/`UntagResource`/`ListTagsForResource`, `CheckCapacity`, `DescribeManagedRuleGroup`; LockToken enforced on Update/Delete; rules stored but not enforced; compatible with Terraform `aws_wafv2_web_acl` and CDK `CfnWebACL`
- **Lambda Layers** — `PublishLayerVersion`, `GetLayerVersion`, `ListLayerVersions`, `ListLayers`, `DeleteLayerVersion`; layer zip content stored in-memory and injected into function execution environment

### Fixed
- **WAF v2 `GetWebACL`/`GetIPSet`/`GetRuleGroup`** — `LockToken` was incorrectly included inside the resource body; now only returned at the top level, matching real AWS and fixing CDK/Terraform Update flows
- **WAF v2 `GetWebACLForResource`** — now returns `WAFNonexistentItemException` when no association exists, matching real AWS behaviour
- **SES v2 `TagResource`/`UntagResource`/`ListTagsForResource`** — added; Terraform calls these after `CreateEmailIdentity`

### Tests
- 763 tests total, all passing

---

## [1.1.10] — 2026-03-31

### Fixed
- **ECS Docker network detection** — ECS containers now automatically join the same Docker network that MiniStack is running on, so containers can reach sibling services (S3, SQS, etc.) without manual network configuration. Contributed by @mickabd
- **Internal naming cleanup** — replaced all internal `localstack-*` references (logger name, default data dir `/tmp/localstack-data/s3` → `/tmp/ministack-data/s3`, healthcheck URLs, CI config) with `ministack` equivalents; `LOCALSTACK_PERSISTENCE` / `LOCALSTACK_HOSTNAME` env vars kept for migration compatibility
- **DynamoDB GSI capacity accounting** — `PutItem`, `DeleteItem`, `UpdateItem`, `GetItem`, `Query`, `Scan`, and `BatchWriteItem` now return correct `ConsumedCapacity.CapacityUnits` when a table has Global Secondary Indexes: `1 + gsi_count` per write (matching real AWS); `INDEXES` mode also returns per-GSI breakdown. Contributed by @jespinoza-shippo.
- **S3 `CreateBucket` idempotency** — creating a bucket you already own now returns 200 instead of 409 `BucketAlreadyOwnedByYou`, matching real AWS and fixing Terraform re-apply failures
- **S3 `OwnershipControls`** — `PutBucketOwnershipControls`, `GetBucketOwnershipControls`, `DeleteBucketOwnershipControls` now implemented; Terraform calls these immediately after `CreateBucket`
- **S3 Control `ListTagsForResource`** — S3 Control API (`/v20180820/tags/{arn}`) now returns empty tag list instead of 404; Terraform uses this for S3 bucket tag lookups
- **S3 `PublicAccessBlock`** — `PutPublicAccessBlock`, `GetPublicAccessBlock`, `DeletePublicAccessBlock` now implemented; CDK and Terraform call these on every bucket
- **STS `AssumeRoleWithWebIdentity`** — now implemented; CDK OIDC deployments (GitHub Actions, etc.) use this; also fixed router to detect unsigned form-encoded STS actions from request body
- **IAM `UpdateRole`** — now implemented; Terraform calls this to set role description and max session duration

### Tests
- 737 tests total, all passing

---

## [1.1.9] — 2026-03-31

### Added
- **S3 Object Lock** — full WORM enforcement on top of versioned buckets
  - `PutObjectLockConfiguration` / `GetObjectLockConfiguration` — enable Object Lock on a bucket with `COMPLIANCE` or `GOVERNANCE` default retention (days or years)
  - `PutObjectRetention` / `GetObjectRetention` — per-object retention with `COMPLIANCE` (always blocks delete) and `GOVERNANCE` (`x-amz-bypass-governance-retention` header bypasses)
  - `PutObjectLegalHold` / `GetObjectLegalHold` — `ON` status unconditionally blocks deletion regardless of retention mode
  - Default retention auto-applied on `PutObject` when bucket lock configuration is present
  @Contributed by @mickabd
- **S3 Replication** — bucket-level replication configuration CRUD
  - `PutBucketReplication` / `GetBucketReplication` / `DeleteBucketReplication`
- **S3 Tagging improvements** — URL-encoded tagging header parsing now correctly handles `x-amz-tagging` on `PutObject` and `CopyObject`

### Tests
- 16 new integration tests covering Object Lock, Replication, and Tagging — 730 tests total, all passing

---

## [1.1.8] — 2026-03-30

### Added
- **Cognito TOTP MFA** — full end-to-end Software Token MFA flow now works with CDK and boto3
  - `AssociateSoftwareToken` returns a stub TOTP secret + session (accepts `AccessToken` or `Session`)
  - `VerifySoftwareToken` accepts any code and marks the user as TOTP-enrolled (`_mfa_enabled`, `_preferred_mfa`)
  - `AdminSetUserMFAPreference` — new: enables/disables TOTP or SMS MFA per user and sets preferred method
  - `SetUserMFAPreference` — new: public (AccessToken-based) equivalent of the above
  - `AdminInitiateAuth` / `InitiateAuth` now issue `SOFTWARE_TOKEN_MFA` challenge after password auth when pool `MfaConfiguration` is `ON` or `OPTIONAL` and user has TOTP enrolled
  - `AdminRespondToAuthChallenge` / `RespondToAuthChallenge` accept any TOTP code for `SOFTWARE_TOKEN_MFA` and return tokens (emulator — no real TOTP validation)
  - `AdminGetUser` / `GetUser` now return real `UserMFASettingList` and `PreferredMfaSetting` fields
  - `MFA_SETUP` challenge handled in both respond endpoints (for pool `ON` + unenrolled users)

### Tests
- 4 new integration tests: full TOTP flow, OPTIONAL MFA, AdminGetUser MFA fields, SetUserMFAPreference via token — 714 tests total, all passing

---

## [1.1.7] — 2026-03-30

### Added
- **Athena engine control** — new `ATHENA_ENGINE` env var (`auto` | `duckdb` | `mock`) to select the SQL backend at startup; `auto` keeps existing behaviour (DuckDB if installed, mock otherwise). New `/_ministack/config` endpoint accepts `POST {"athena.ATHENA_ENGINE": "mock"}` to switch engines at runtime without restart — useful in CI to force mock mode without DuckDB installed. 
- **VPC gap coverage** — 6 new EC2 resource types, 22 new actions, 11 new tests
  - **NAT Gateways**: `CreateNatGateway`, `DescribeNatGateways`, `DeleteNatGateway` — supports `SubnetId`, `ConnectivityType` (public/private), state transitions, `vpc-id`/`subnet-id`/`state` filters
  - **Network ACLs**: `CreateNetworkAcl`, `DescribeNetworkAcls`, `DeleteNetworkAcl`, `CreateNetworkAclEntry`, `DeleteNetworkAclEntry`, `ReplaceNetworkAclEntry`, `ReplaceNetworkAclAssociation` — full CRUD with rule entries and subnet associations
  - **Flow Logs**: `CreateFlowLogs`, `DescribeFlowLogs`, `DeleteFlowLogs` — supports VPC/subnet/ENI resource targets, CloudWatch Logs and S3 destinations, `resource-id` filter
  - **VPC Peering**: `CreateVpcPeeringConnection`, `AcceptVpcPeeringConnection`, `DescribeVpcPeeringConnections`, `DeleteVpcPeeringConnection` — full lifecycle from `pending-acceptance` → `active` → `deleted`, cross-account/cross-region params accepted
  - **DHCP Options**: `CreateDhcpOptions`, `AssociateDhcpOptions`, `DescribeDhcpOptions`, `DeleteDhcpOptions` — arbitrary key/value configurations, association updates `VpcId.DhcpOptionsId`
  - **Egress-Only Internet Gateways**: `CreateEgressOnlyInternetGateway`, `DescribeEgressOnlyInternetGateways`, `DeleteEgressOnlyInternetGateway` — IPv6 egress-only IGW for VPCs. Contributed by @mickabd

### Fixed
- **SQS `awsQueryCompatible` header** — all SQS JSON error responses now include the `x-amzn-query-error: <legacy_code>;<fault>` header required by the `awsQueryCompatible` service trait. botocore reads this header and overrides `Error.Code` with the legacy `AWS.SimpleQueueService.*` namespaced code (e.g. `AWS.SimpleQueueService.NonExistentQueue` instead of `QueueDoesNotExist`). Without this header, any SDK code that matched against the legacy string worked against real AWS but silently failed against MiniStack. Full mapping of all 28 SQS error shapes sourced from `aws-sdk-go` ErrCode constants. Contributed by @jespinoza-shippo.

### Tests
- 708 integration tests — all passing

---

## [1.1.6] — 2026-03-30

### Fixed
- **XML error responses** — added `<Type>Sender</Type>` (4xx) / `<Type>Receiver</Type>` (5xx) to all XML error responses in `sqs.py` and `core/responses.py` (used by S3, SNS, IAM, STS, CloudWatch). botocore requires this element to populate typed exception classes (e.g. `client.exceptions.QueueDoesNotExist`). Without it, botocore fell back to generic `ClientError` even when the error `Code` was correct.

### Tests
- 694 integration tests — all passing

---

## [1.1.5] — 2026-03-30

### Fixed
- **API Gateway v1** — `createdDate` / `lastUpdatedDate` fields now returned as Unix timestamps (integers) instead of ISO strings. Terraform AWS provider v4+ deserializes these as JSON Numbers and raised `expected Timestamp to be a JSON Number, got string instead` on `CreateRestApi`.
- **API Gateway v2** — same fix applied to `createdDate` / `lastUpdatedDate` on APIs and stages.
- **S3 virtual-hosted style** — host pattern now also matches `{bucket}.s3.localhost[:{port}]` in addition to `{bucket}.localhost[:{port}]`. Terraform AWS provider v4+ uses the `.s3.` subdomain when `force_path_style = false`.
- **CloudWatch Logs `ListTagsForResource`** — ARN lookup now accepts both `arn:...:log-group:{name}` and `arn:...:log-group:{name}:*`. Terraform passes the ARN without the trailing `:*` that MiniStack appends internally, causing `ResourceNotFoundException`.
- **SQS `SendMessageBatch`** — now rejects batches with more than 10 entries with `AWS.SimpleQueueService.TooManyEntriesInBatchRequest`, matching real AWS behaviour. Previously MiniStack silently accepted oversized batches.
- **DynamoDB `BatchWriteItem`** — now includes `ConsumedCapacity` as a list in the response when `ReturnConsumedCapacity` is set to `TOTAL` or `INDEXES`. Previously the field was absent entirely.

### Tests
- 5 regression tests added (one per fix above) — 693 integration tests total, all passing

---

## [1.1.4] — 2026-03-30

### Added
- **Amazon ELBv2 / ALB** (`ministack/services/alb.py`) — full control plane + data plane
  - **Load Balancers**: `CreateLoadBalancer`, `DescribeLoadBalancers`, `DeleteLoadBalancer`, `DescribeLoadBalancerAttributes`, `ModifyLoadBalancerAttributes`
  - **Target Groups**: `CreateTargetGroup`, `DescribeTargetGroups`, `ModifyTargetGroup`, `DeleteTargetGroup`, `DescribeTargetGroupAttributes`, `ModifyTargetGroupAttributes`
  - **Listeners**: `CreateListener`, `DescribeListeners`, `ModifyListener`, `DeleteListener`
  - **Rules**: `CreateRule`, `DescribeRules`, `ModifyRule`, `DeleteRule`, `SetRulePriorities`
  - **Targets**: `RegisterTargets`, `DeregisterTargets`, `DescribeTargetHealth`
  - **Tags**: `AddTags`, `RemoveTags`, `DescribeTags`
  - **Data plane — ALB→Lambda live traffic routing**
    - Incoming HTTP requests matched against configured listener rules (priority order)
    - Rule conditions supported: `path-pattern`, `host-header`, `http-method`, `query-string`, `http-header` (fnmatch glob matching)
    - Actions supported: `forward` (to target group), `fixed-response`, `redirect` (301/302 with `#{host}`/`#{path}`/`#{port}` substitution)
    - `TargetType=lambda` target groups: builds ALB event payload (httpMethod, path, queryStringParameters, multiValueQueryStringParameters, headers, multiValueHeaders, body, isBase64Encoded, requestContext.elb) and invokes Lambda via the in-process Lambda runtime; translates Lambda response (statusCode, headers, multiValueHeaders, body, isBase64Encoded) back to HTTP
    - Two addressing modes — no DNS or `/etc/hosts` changes required for local testing:
      - **Host-header**: `Host: {lb-name}.alb.localhost[:{port}]` or the ALB's exact `DNSName`
      - **Path prefix**: `/_alb/{lb-name}/path` (rewrites path before rule evaluation)
  - Query/XML protocol via `Action=` parameter; credential scope `elasticloadbalancing`
  - 10 control-plane integration tests + 7 data-plane integration tests

### Tests
- 688 integration tests — all passing

---

## [1.1.3] — 2026-03-30

### Added
- **Amazon EBS** (Elastic Block Store) — added to the EC2 Query/XML service handler
  - **Volumes**: `CreateVolume`, `DeleteVolume`, `DescribeVolumes`, `DescribeVolumeStatus`,
    `AttachVolume`, `DetachVolume`, `ModifyVolume`, `DescribeVolumesModifications`,
    `EnableVolumeIO`, `ModifyVolumeAttribute`, `DescribeVolumeAttribute`
  - **Snapshots**: `CreateSnapshot`, `DeleteSnapshot`, `DescribeSnapshots`,
    `CopySnapshot`, `ModifySnapshotAttribute`, `DescribeSnapshotAttribute`
  - All three volume types supported (gp2/gp3/io1/io2/st1/sc1)
  - Attach/Detach updates volume state (available ↔ in-use)
  - ModifyVolume returns `completed` immediately
  - Snapshots store as `completed` (emulator — no real EBS)
  - Pro-only on LocalStack — free here
  - 8 integration tests

- **Amazon EFS** (Elastic File System) — new service (`ministack/services/efs.py`)
  - REST/JSON protocol via `/2015-02-01/*` paths, credential scope `elasticfilesystem`
  - **File Systems**: `CreateFileSystem`, `DescribeFileSystems`, `DeleteFileSystem`,
    `UpdateFileSystem` — CreationToken idempotency enforced
  - **Mount Targets**: `CreateMountTarget`, `DescribeMountTargets`, `DeleteMountTarget`,
    `DescribeMountTargetSecurityGroups`, `ModifyMountTargetSecurityGroups`
  - **Access Points**: `CreateAccessPoint`, `DescribeAccessPoints`, `DeleteAccessPoint`
  - **Tags**: `TagResource`, `UntagResource`, `ListTagsForResource`
  - **Lifecycle**: `PutLifecycleConfiguration`, `DescribeLifecycleConfiguration`
  - **Backup Policy**: `PutBackupPolicy`, `DescribeBackupPolicy`
  - **Account**: `DescribeAccountPreferences`, `PutAccountPreferences`
  - FileSystem with active mount targets blocks deletion (`FileSystemInUse`)
  - Pro-only on LocalStack — free here
  - 10 integration tests

### Tests
- 671 integration tests — all passing (672 - 1 flaky Docker ECS test)

---

## [1.1.2] — 2026-03-29

### Added

- **Amazon EMR** (`ministack/services/emr.py`) — full control plane emulation (no real Spark/Hadoop)
  - **Clusters**: `RunJobFlow`, `DescribeCluster`, `ListClusters`, `TerminateJobFlows`, `ModifyCluster`, `SetTerminationProtection`, `SetVisibleToAllUsers`
  - **Steps**: `AddJobFlowSteps`, `DescribeStep`, `ListSteps`, `CancelSteps` — steps stored as COMPLETED immediately (emulator behaviour)
  - **Instance Fleets**: `AddInstanceFleet`, `ListInstanceFleets`, `ModifyInstanceFleet`
  - **Instance Groups**: `AddInstanceGroups`, `ListInstanceGroups`, `ModifyInstanceGroups`
  - **Bootstrap Actions**: `ListBootstrapActions`
  - **Tags**: `AddTags`, `RemoveTags`
  - **Block Public Access**: `GetBlockPublicAccessConfiguration`, `PutBlockPublicAccessConfiguration`
  - All three instance config modes: simple (`MasterInstanceType`/`SlaveInstanceType`/`InstanceCount`), `InstanceGroups`, `InstanceFleets`
  - `KeepJobFlowAliveWhenNoSteps=True` → `WAITING`; `False` → `TERMINATED`
  - `TerminationProtected=True` raises `ValidationException` on `TerminateJobFlows`
  - JSON protocol via `X-Amz-Target: ElasticMapReduce.{Op}`, credential scope `elasticmapreduce`
  - Pro-only on LocalStack — free in MiniStack
  - 12 integration tests

### Tests

- 656 integration tests — all passing

---

## [1.1.1] — 2026-03-29

### Added

- **Amazon EC2** (`ministack/services/ec2.py`) — full API-level emulation (no real VMs)
  - **Instances**: `RunInstances`, `DescribeInstances`, `TerminateInstances`, `StopInstances`, `StartInstances`, `RebootInstances`
  - **Images**: `DescribeImages` — returns 3 stub AMIs (Amazon Linux 2, Ubuntu 22.04, Windows Server 2022)
  - **Security Groups**: `CreateSecurityGroup`, `DeleteSecurityGroup`, `DescribeSecurityGroups`, `AuthorizeSecurityGroupIngress`, `RevokeSecurityGroupIngress`, `AuthorizeSecurityGroupEgress`, `RevokeSecurityGroupEgress`
  - **Key Pairs**: `CreateKeyPair`, `DeleteKeyPair`, `DescribeKeyPairs`, `ImportKeyPair`
  - **VPC**: `CreateVpc`, `DeleteVpc`, `DescribeVpcs`, `ModifyVpcAttribute` — default VPC pre-created
  - **Subnets**: `CreateSubnet`, `DeleteSubnet`, `DescribeSubnets`, `ModifySubnetAttribute` — default subnet pre-created
  - **Internet Gateways**: `CreateInternetGateway`, `DeleteInternetGateway`, `DescribeInternetGateways`, `AttachInternetGateway`, `DetachInternetGateway`
  - **Route Tables**: `CreateRouteTable`, `DeleteRouteTable`, `DescribeRouteTables`, `AssociateRouteTable`, `DisassociateRouteTable`, `CreateRoute`, `ReplaceRoute`, `DeleteRoute` — default route table pre-created for default VPC
  - **Network Interfaces (ENI)**: `CreateNetworkInterface`, `DeleteNetworkInterface`, `DescribeNetworkInterfaces`, `AttachNetworkInterface`, `DetachNetworkInterface` — full botocore-compliant response shape (`availabilityZone`, `sourceDestCheck`, `interfaceType`, `privateIpAddressesSet`)
  - **VPC Endpoints**: `CreateVpcEndpoint`, `DeleteVpcEndpoints`, `DescribeVpcEndpoints` — Gateway and Interface types; `routeTableIdSet` / `subnetIdSet` serialized correctly
  - **Availability Zones**: `DescribeAvailabilityZones`
  - **Elastic IPs**: `AllocateAddress`, `ReleaseAddress`, `AssociateAddress`, `DisassociateAddress`, `DescribeAddresses`
  - **Tags**: `CreateTags`, `DeleteTags`, `DescribeTags`
  - Default VPC, subnet, security group, internet gateway, and route table always present
  - Rules stored but not enforced (matches LocalStack behaviour)
  - 26 integration tests
- **Step Functions Activities** — full worker-based activity task pattern
  - `CreateActivity`, `DeleteActivity`, `DescribeActivity`, `ListActivities` — full CRUD
  - `GetActivityTask` — async long-poll (up to 60 s) returning `taskToken` + `input` to worker; non-blocking (uses `asyncio.sleep` — does not stall the event loop)
  - Activity Task state execution — when a Task state's `Resource` is an activity ARN, the execution enqueues the task and waits for a worker to call `SendTaskSuccess` or `SendTaskFailure`
  - `ActivityAlreadyExists` raised on duplicate `CreateActivity` (matches AWS behaviour — not idempotent)
  - `ActivityDoesNotExist` raised on `DeleteActivity`, `DescribeActivity`, `GetActivityTask` for unknown ARN
  - Activity ARN format: `arn:aws:states:{region}:{account}:activity:{name}`
  - 5 integration tests: CRUD, list, duplicate-name error, worker success flow, worker failure flow

### Tests

- 644 integration tests — all passing

---

## [1.1.0] — 2026-03-28

### Added

- **Amazon Cognito** (`ministack/services/cognito.py`) — full User Pool and Identity Pool emulation
  - **User Pools (cognito-idp)**: CreateUserPool, DeleteUserPool, DescribeUserPool, ListUserPools, UpdateUserPool
  - **User Pool Clients**: CreateUserPoolClient, DeleteUserPoolClient, DescribeUserPoolClient, ListUserPoolClients, UpdateUserPoolClient
  - **User management**: AdminCreateUser, AdminDeleteUser, AdminGetUser, ListUsers (with filter support: `=`, `^=`, `!=`), AdminSetUserPassword, AdminUpdateUserAttributes, AdminConfirmSignUp, AdminDisableUser, AdminEnableUser, AdminResetUserPassword, AdminUserGlobalSignOut
  - **Auth flows**: AdminInitiateAuth, AdminRespondToAuthChallenge, InitiateAuth, RespondToAuthChallenge — ADMIN_USER_PASSWORD_AUTH, ADMIN_NO_SRP_AUTH, USER_PASSWORD_AUTH, REFRESH_TOKEN_AUTH / REFRESH_TOKEN (both accepted), USER_SRP_AUTH (returns PASSWORD_VERIFIER challenge); FORCE_CHANGE_PASSWORD challenge on first login
  - **Self-service**: SignUp (always UNCONFIRMED — AutoVerifiedAttributes verifies the attribute, not the account), ConfirmSignUp, ForgotPassword, ConfirmForgotPassword, ChangePassword (decodes access token and updates stored password), GetUser, UpdateUserAttributes, DeleteUser, GlobalSignOut, RevokeToken
  - **Groups**: CreateGroup, DeleteGroup, GetGroup, ListGroups, ListUsersInGroup, AdminAddUserToGroup, AdminRemoveUserFromGroup, AdminListGroupsForUser, AdminListUserAuthEvents
  - **Domain**: CreateUserPoolDomain, DeleteUserPoolDomain, DescribeUserPoolDomain
  - **MFA**: GetUserPoolMfaConfig, SetUserPoolMfaConfig, AssociateSoftwareToken, VerifySoftwareToken
  - **Tags**: TagResource, UntagResource, ListTagsForResource
  - **Identity Pools (cognito-identity)**: CreateIdentityPool, DeleteIdentityPool, DescribeIdentityPool, ListIdentityPools, UpdateIdentityPool, GetId, GetCredentialsForIdentity, GetOpenIdToken, SetIdentityPoolRoles, GetIdentityPoolRoles, ListIdentities, DescribeIdentity, MergeDeveloperIdentities, UnlinkDeveloperIdentity, UnlinkIdentity, TagResource, UntagResource, ListTagsForResource
  - **OAuth2**: `POST /oauth2/token` — client_credentials flow; returns stub Bearer token
  - Stub JWT tokens: structurally valid base64url JWTs (non-cryptographic); IDP pool ARN format `arn:aws:cognito-idp:region:account:userpool/{id}`; Identity pool ID format `region:{uuid}`
  - `_user_from_token` shared helper — decodes stub JWT payload to find user by `sub`, used by GetUser, UpdateUserAttributes, DeleteUser, ChangePassword, and REFRESH_TOKEN_AUTH
  - Wired into router, SERVICE_HANDLERS, SERVICE_NAME_ALIASES, `_reset_all_state()`, and both credential scopes (`cognito-idp`, `cognito-identity`)
  - 43 integration tests covering full CRUD lifecycle for User Pools, Pool Clients, Users, Auth flows, Refresh tokens, Groups, Domains, MFA, Tags, and Identity Pools

### Changed

- **Package restructure**: all source code moved into `ministack/` package (`ministack/app.py`, `ministack/core/`, `ministack/services/`) — fixes `pip install ministack` entrypoint crash (`app:main` was unresolvable because `app.py` was not included in the wheel)
- **Entrypoint**: `ministack = "app:main"` → `ministack = "ministack.app:main"`
- **ASGI module**: `app:app` → `ministack.app:app` in Dockerfile and CI
- **PyPI trusted publishing**: OIDC workflow added (`pypi-publish.yml`) — no API token needed, publishes on `v*.*.*` tag push

### Fixed

- **Lambda `GetFunctionConcurrency`**: returns `{}` instead of 404 after `DeleteFunctionConcurrency` — matches AWS behaviour where an unset concurrency limit returns an empty response
- **Cognito `GetCredentialsForIdentity`**: response field is `SecretKey` (correct boto3 wire name) — was incorrectly named `SecretAccessKey`
- **ElastiCache `ModifyCacheParameterGroup` / `ResetCacheParameterGroup`**: parameter list key was `ParameterNameValues.member.{n}.*` — corrected to `ParameterNameValues.ParameterNameValue.{n}.*` matching actual boto3 Query API serialisation
- **RDS / ElastiCache / ECS `reset()`**: `container.remove()` → `container.remove(v=True)` — Docker volumes created by stopped containers are now removed along with the container, preventing anonymous volume accumulation across test runs
- **RDS `containers.run()`**: added `tmpfs` mount for `/var/lib/postgresql/data` and `/var/lib/mysql` — postgres/mysql data lives in container RAM; no anonymous Docker volumes created per instance
- **Docker Compose**: added `build: .` so `docker compose up --build` uses local source instead of always pulling from Docker Hub

### Infrastructure

- **`Makefile` `purge` target**: kills all containers labelled `ministack`, prunes dangling volumes, and clears `./data/s3/` — safe to run alongside other projects (filter is label-scoped, not image-scoped)

### Tests

- 3 package structure tests: `test_package_core_importable`, `test_package_services_importable`, `test_app_asgi_callable`
- Merged all 97 tests from `test_qa_comprehensive.py` into `test_services.py` — single test file, `test_qa_comprehensive.py` deleted
- Fixed `test_cognito_get_id_and_credentials`: `SecretAccessKey` → `SecretKey`
- Fixed `test_apigwv1_usage_plan_key_crud`: `Name`/`Enabled` → `name`/`enabled` (boto3 lowercase params)
- Fixed `test_lambda_reset_terminates_workers`: timeout 5 s → 15 s with 3-attempt retry
- Fixed `test_rds_snapshot_crud` / `test_rds_deletion_protection`: added `finally` cleanup so RDS containers are deleted after each test
- 613 integration tests — all passing against Docker image (618 as of v1.1.1)

---

## [1.0.8] — 2026-03-28

### Added

- **Amazon Route53** (`services/route53.py`) — full hosted zone and DNS record management
  - Hosted zones: `CreateHostedZone`, `GetHostedZone`, `DeleteHostedZone`, `ListHostedZones`, `ListHostedZonesByName`, `UpdateHostedZoneComment`
  - Record sets: `ChangeResourceRecordSets` (CREATE / UPSERT / DELETE, atomic batch), `ListResourceRecordSets`
  - Changes: `GetChange` — changes are immediately `INSYNC`
  - Health checks: `CreateHealthCheck`, `GetHealthCheck`, `DeleteHealthCheck`, `ListHealthChecks`, `UpdateHealthCheck`
  - Tags: `ChangeTagsForResource`, `ListTagsForResource` (hostedzone and healthcheck resource types)
  - REST/XML protocol with namespace `https://route53.amazonaws.com/doc/2013-04-01/`; credential scope `route53`
  - SOA + NS records auto-created on zone creation with 4 default AWS nameservers
  - `CallerReference` idempotency for `CreateHostedZone` and `CreateHealthCheck`
  - Alias records (AliasTarget), weighted, failover, latency, geolocation, multi-value routing attributes stored and returned
  - Zone ID format `/hostedzone/Z{13chars}`, Change ID `/change/C{13chars}`
  - Marker-based pagination for `ListHostedZones` and `ListHealthChecks`; name/type pagination for `ListResourceRecordSets`
  - 16 integration tests
- **Non-ASCII / Unicode support** — seamless end-to-end handling of UTF-8 content across all services
  - Inbound header values decoded as UTF-8 (with latin-1 fallback) so `x-amz-meta-*` fields containing non-ASCII are stored correctly
  - Outbound header encoding falls back to UTF-8 when a value cannot be encoded as latin-1 — prevents `UnicodeEncodeError` on `Content-Disposition` or metadata round-trips
  - All JSON responses use `ensure_ascii=False` — raw UTF-8 characters in DynamoDB items, SQS messages, Secrets Manager values, SSM parameters, and Lambda payloads are returned as-is rather than `\uXXXX` escaped
  - 7 integration tests covering S3 keys, S3 metadata, DynamoDB, SQS, Secrets Manager, SSM, and Route53 zone comments

### Fixed

- **DynamoDB TTL reaper thread-safety**: the background reaper thread now holds `_lock` while scanning and deleting expired items — eliminates a race condition with concurrent request handlers that could corrupt table state or crash the reaper under load
- **S3 `PutObject` / `CreateBucket` spurious `Content-Type`**: these operations no longer return `Content-Type: application/xml` on success (AWS returns no Content-Type for empty 200 bodies) — prevents SDK response-parsing warnings
- **S3 `DeleteObject` delete-marker header**: non-versioned buckets now return an empty 204 with no extra headers; versioned/suspended buckets return `x-amz-delete-marker: true` — previously all buckets unconditionally returned `x-amz-delete-marker: false`
- **CloudWatch Logs `FilterLogEvents` pattern matching**: upgraded from plain substring search to proper CloudWatch filter syntax — supports `*`/`?` glob wildcards, multi-term AND (`TERM1 TERM2`), term exclusion (`-TERM`), and JSON-style patterns (matched as pass-all); previously only exact substring matches worked
- **JSON responses `ensure_ascii`**: all JSON service responses now use `ensure_ascii=False` so non-ASCII strings (Cyrillic, CJK, Arabic, etc.) are returned as raw UTF-8 rather than `\uXXXX` escape sequences — matches real AWS behaviour
- **Inbound header UTF-8 decoding**: request header values are now decoded as UTF-8 with latin-1 fallback — `x-amz-meta-*` headers containing multi-byte characters are stored and round-tripped correctly
- **Outbound header UTF-8 encoding**: response headers that cannot be encoded as latin-1 (e.g. metadata containing non-ASCII) now fall back to UTF-8 encoding instead of raising `UnicodeEncodeError`
- **API Gateway v2 / v1 Lambda response encoding**: Lambda invocation response bodies serialised via `json.dumps` now use `ensure_ascii=False` and explicit `utf-8` encoding — non-ASCII characters in Lambda responses are preserved end-to-end
- **DynamoDB `Query` pagination on hash-only tables**: `_apply_exclusive_start_key` was returning `[]` for any table without a sort key (`sk_name=None`) because `not sk_name` short-circuited to an empty-result path — hash-only tables now paginate correctly by resuming after the matching partition key value (validated against botocore `dynamodb` service model)
- **SQS `DeleteMessageBatch` silent success on invalid receipt handle**: both the found and not-found branches were appending to `Successful` (copy-paste error) — an unmatched `ReceiptHandle` now correctly populates the `Failed` list with `ReceiptHandleIsInvalid` (validated against botocore `BatchResultErrorEntry` shape)
- **SNS→Lambda `EventSubscriptionArn` hardcoded suffix**: the SNS-to-Lambda fanout envelope was setting `EventSubscriptionArn` to `"{topic_arn}:subscription"` instead of the actual subscription ARN — Lambda functions inspecting `event['Records'][0]['EventSubscriptionArn']` now receive the correct value
- **Lambda error codes**: internal path-routing fallbacks now use `InvalidParameterValueException` (400) for missing function name and `ResourceNotFoundException` (404) for unrecognised paths — previously both used the non-existent `InvalidRequest` code which is absent from the botocore Lambda model

- **Lambda worker reset**: `core/lambda_runtime.reset()` was calling `worker.proc.terminate()` (typo) instead of `worker._proc.terminate()` — the `AttributeError` was silently swallowed, leaving orphaned worker subprocesses after `/_ministack/reset`
- **Step Functions → Lambda async invocation**: `stepfunctions._call_lambda` was calling `lambda_svc._invoke` synchronously — `_invoke` is `async`, so it returned a coroutine object instead of executing; Task states invoking Lambda now use `asyncio.run()` to execute the coroutine from the background thread
- **EventBridge → Lambda async invocation**: same bug in `eventbridge._dispatch_to_lambda` — fixed with `asyncio.run()`
- **`make run` Docker socket mount**: added `-v /var/run/docker.sock:/var/run/docker.sock` so ECS `RunTask` works when running via `make run`

### Tests

- 4 regression tests added, one per botocore-confirmed bug: `test_ddb_query_pagination_hash_only`, `test_sqs_batch_delete_invalid_receipt_handle`, `test_sns_to_lambda_event_subscription_arn`, `test_lambda_unknown_path_returns_404`
- 2 regression tests for runtime fixes: `test_lambda_reset_terminates_workers`, `test_sfn_integration_lambda_invoke`
- 479 integration tests — all passing, including against Docker image

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

- ACM (certificate management)
- State persistence for Secrets Manager, SSM, DynamoDB (`PERSIST_STATE=1` currently only covers API Gateway v1/v2)
