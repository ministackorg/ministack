# Requirements Document

## Introduction

This feature adds AWS IoT Core support to Ministack, the local AWS service emulator. IoT Core has two distinct surfaces that must be emulated very differently:

- A **control plane** of AWS-shaped JSON/REST APIs (Things, ThingTypes, ThingGroups, Certificates, Policies, Endpoints, Shadows, Jobs, Rules, Fleet Provisioning, plus the `iot-data` HTTP Publish/GetRetainedMessage API). This belongs inside Ministack itself, alongside every other service module.
- A **data plane** speaking MQTT 3.1.1 / 5.0 over TCP (1883 / 8883 mTLS) and WebSockets (with optional SigV4-signed upgrades). Because MQTT is a binary protocol and Ministack has no binary codec layer, the data plane MAY be delegated to an embedded broker library, a sidecar container (e.g. Mosquitto), or a bundled binary. Ministack does not need to implement the MQTT codec itself.

The unblocking use case from issue #564 is end-to-end test of "Lambda publishes via `iot-data`, browser subscribes over MQTT-over-WebSockets". To stay shippable, work is partitioned into six sub-phases across three major phases. Each major phase ships control plane first, then data plane:

- **Phase 1a** (P1a): Control-plane CRUD — Things, ThingTypes, ThingGroups, Certificates (with Local_CA), Policies, `DescribeEndpoint`, persistence, configuration. No broker dependency; pure HTTP/JSON service module.
- **Phase 1b** (P1b): Data-plane integration — broker bridge, MQTT broker (TCP 1883 + MQTT-over-WS), SigV4-signed WS upgrades, `iot-data Publish` HTTP API. Completes the original use case end-to-end.
- **Phase 2a** (P2a): Shadows control plane — `GetThingShadow`, `UpdateThingShadow`, `DeleteThingShadow`, `ListNamedShadowsForThing` REST APIs with merge/version/delta semantics. No broker dependency for the REST path.
- **Phase 2b** (P2b): Shadows data plane + mTLS — Shadow MQTT topic wiring (`$aws/things/+/shadow/#`), mTLS on port 8883 with Local_CA validation, `GetRetainedMessage` / `ListRetainedMessages`.
- **Phase 3a** (P3a): Rules Engine + Jobs + Fleet Provisioning control plane — `CreateTopicRule` (with SQL parser), `CreateJob`, `CreateProvisioningTemplate` and their CRUD siblings. No broker dependency for the CRUD path.
- **Phase 3b** (P3b): Rules Engine + Jobs + Fleet Provisioning data plane — rule evaluation and action dispatch on matching messages, Job MQTT notifications (`$aws/things/+/jobs/notify-next`), Fleet Provisioning MQTT flow (`$aws/provisioning-templates/+/provision/json`).

Sub-phase tags (P1a, P1b, P2a, P2b, P3a, P3b) are used on each acceptance criterion so the scope of each deliverable is unambiguous. The shorthand P1/P2/P3 in existing criteria maps as follows: control-plane-only criteria become the "a" sub-phase; criteria requiring broker interaction become the "b" sub-phase.

## Glossary

- **IoT_Control_Plane**: The in-process Ministack service module that implements AWS IoT control APIs (`iot.<region>.amazonaws.com`) and the IoT data HTTP API (`data.iot.<region>.amazonaws.com`, AWS service code `iot-data`). Lives at `ministack/services/iot.py` (and optionally `iot_data.py`) and registers with `ministack/core/router.py` like every other service.
- **IoT_Data_Plane**: The MQTT broker surface — TCP on 1883, mTLS on 8883, MQTT-over-WebSockets on the Ministack gateway port. Implementation MAY be embedded (in-process broker library), bundled (sidecar binary), or external (docker-compose Mosquitto). The IoT_Data_Plane is NOT required to be implemented from scratch in Python.
- **Broker_Bridge**: The internal interface between IoT_Control_Plane and IoT_Data_Plane. Used by the IoT_Data_HTTP_API to publish, by the Rules Engine to subscribe to topic patterns, and by the Shadow Service to deliver `$aws/things/+/shadow/#` traffic.
- **IoT_Data_HTTP_API**: The `iot-data` JSON/REST API offering `Publish`, `GetRetainedMessage`, `ListRetainedMessages`. It is part of the control-plane process and translates HTTP calls into Broker_Bridge operations.
- **Thing**: An IoT device registry record with name, optional ThingTypeName, attributes, and version.
- **ThingType / ThingGroup**: Optional grouping/typing records attached to Things.
- **Certificate**: An X.509 client certificate record with status (`ACTIVE` / `INACTIVE` / `REVOKED`), PEM body, and optional attached principals (Things) and Policies.
- **Local_CA**: A self-signed root certificate authority owned by Ministack, used to sign Certificate PEMs returned from `CreateKeysAndCertificate` and to validate mTLS clients on port 8883. Reuses the existing openssl-backed pattern in `ministack/core/tls.py` and is exposed for reuse by ACM and API Gateway custom domains.
- **Policy**: A named IoT policy document (JSON, AWS-shaped). Stored verbatim; enforcement on the data plane is OPTIONAL in Phase 1 and Phase 2.
- **Endpoint**: A hostname returned by `DescribeEndpoint` that resolves to the Ministack gateway and identifies the IoT_Data_Plane transport. Endpoint type values follow AWS (`iot:Data-ATS`, `iot:Data`, `iot:CredentialProvider`, `iot:Jobs`).
- **Device_Shadow**: A JSON document keyed by ThingName plus optional shadow name, with `desired` / `reported` / `delta` sections and a monotonic `version` field.
- **Job**: A control record describing work to be executed on one or more Things, with per-Thing JobExecution status.
- **Rule**: A named record containing an SQL-like expression (`SELECT ... FROM '<topic-pattern>' WHERE ...`) and one or more action descriptors (Lambda, SQS, SNS, DynamoDB, Kinesis, Firehose, republish).
- **Rules_SQL**: The IoT SQL dialect parsed by the Rules Engine. Covers `SELECT <projection>`, `FROM '<topic-filter>'`, optional `WHERE <expr>`, and a documented subset of built-in functions.
- **MQTT_WS_Subprotocol**: The `mqtt` / `mqttv3.1` / `mqttv5` Sec-WebSocket-Protocol value used by IoT clients connecting over WebSockets.

## Requirements

### Requirement 1: Architectural split between control plane and data plane

**User Story:** As a Ministack maintainer, I want IoT Core's control plane and data plane to be cleanly separated, so that the control plane can ship as a normal Ministack service while the data plane can use an existing MQTT broker implementation.

#### Acceptance Criteria

1. THE IoT_Control_Plane SHALL be implemented as one or more Python modules under `ministack/services/` and registered through `ministack/core/router.py` using the same conventions as existing services. (P1a)
2. THE IoT_Control_Plane SHALL NOT contain a hand-written MQTT 3.1.1 or MQTT 5.0 wire-format codec. (P1a)
3. THE IoT_Data_Plane SHALL be provided by an embedded broker library, a bundled broker binary, or an external broker container, with the chosen strategy decided in the design phase. (P1b)
4. THE Broker_Bridge SHALL expose at minimum `publish(topic, payload, qos, retain)`, `subscribe(topic_filter, callback)`, and `unsubscribe(subscription_id)` operations to the IoT_Control_Plane. (P1b)
5. WHEN the IoT_Data_Plane is unavailable at request time, THE IoT_Control_Plane SHALL still respond successfully to control-plane CRUD operations that do not require broker interaction. (P1a)
6. IF the IoT_Data_Plane is unavailable WHEN an `iot-data Publish` call is received, THEN THE IoT_Data_HTTP_API SHALL return HTTP 503 with an `InternalFailureException` body. (P1b)

### Requirement 2: Thing registry CRUD

**User Story:** As a developer testing IoT applications locally, I want to manage Things and their grouping records via the AWS IoT control API, so that my Terraform / boto3 / SDK code works against Ministack without modification.

#### Acceptance Criteria

1. WHEN a `CreateThing` request is received with a unique `thingName`, THE IoT_Control_Plane SHALL persist a Thing record with the supplied attributes and ThingTypeName and SHALL return the Thing's ARN, name, and id. (P1a)
2. WHEN a `CreateThing` request is received with an already-existing `thingName` AND the same configuration (attributes, thingTypeName), THE IoT_Control_Plane SHALL return success idempotently. WHEN the `thingName` exists but the configuration differs, THE IoT_Control_Plane SHALL return HTTP 409 with a `ResourceAlreadyExistsException` body. (P1a)
3. WHEN a `DescribeThing` request is received for an existing `thingName`, THE IoT_Control_Plane SHALL return the stored attributes, ThingTypeName, version, and ARN. (P1a)
4. WHEN a `DescribeThing` request is received for an unknown `thingName`, THE IoT_Control_Plane SHALL return HTTP 404 with a `ResourceNotFoundException` body. (P1a)
5. WHEN a `ListThings` request is received, THE IoT_Control_Plane SHALL return all Things in the caller's account scope, applying `attributeName` / `attributeValue` / `thingTypeName` filters when provided. (P1a)
6. WHEN an `UpdateThing` request is received for an existing `thingName`, THE IoT_Control_Plane SHALL apply the supplied attribute merge / replace semantics and SHALL increment the Thing's `version` field by one. (P1a)
7. WHEN a `DeleteThing` request is received for an existing `thingName`, THE IoT_Control_Plane SHALL remove the Thing record and detach any attached Certificates. (P1a)
8. THE IoT_Control_Plane SHALL implement `CreateThingType`, `DescribeThingType`, `ListThingTypes`, `DeprecateThingType`, and `DeleteThingType` against the same persistence pattern. (P1a)
9. THE IoT_Control_Plane SHALL implement `CreateThingGroup`, `DescribeThingGroup`, `ListThingGroups`, `AddThingToThingGroup`, `RemoveThingFromThingGroup`, and `DeleteThingGroup` against the same persistence pattern. (P1a)
10. THE IoT_Control_Plane SHALL persist Thing, ThingType, and ThingGroup state through the existing `ministack/core/persistence.py` `get_state` / `restore_state` mechanism so records survive a warm reboot. (P1a)

### Requirement 3: Local CA and certificate issuance

**User Story:** As a developer, I want `CreateKeysAndCertificate` to return real PEM material signed by a stable local CA, so that mTLS connections to the IoT_Data_Plane succeed and my SDK certificate-handling code paths execute end-to-end.

#### Acceptance Criteria

1. THE IoT_Control_Plane SHALL maintain a single Local_CA per Ministack installation, generated lazily on first use and cached on disk so the CA certificate and key are stable across restarts. (P1a)
2. WHEN a `CreateKeysAndCertificate` request is received with `setAsActive=true`, THE IoT_Control_Plane SHALL generate an RSA or ECDSA keypair, sign a leaf certificate with the Local_CA, persist a Certificate record with status `ACTIVE`, and return `certificateArn`, `certificateId`, `certificatePem`, and `keyPair.{PublicKey,PrivateKey}` in PEM form. (P1a)
3. WHEN a `CreateKeysAndCertificate` request is received with `setAsActive=false`, THE IoT_Control_Plane SHALL behave identically to the `setAsActive=true` path except the persisted Certificate record SHALL have status `INACTIVE`. (P1a)
4. THE Local_CA root certificate SHALL be retrievable via a documented Ministack endpoint (e.g. `GET /_ministack/iot/ca.pem`) so test code can configure SDKs to trust it. (P1a)
5. WHEN a `RegisterCertificate` request is received with a PEM body, THE IoT_Control_Plane SHALL persist the Certificate record without re-signing the supplied PEM. (P1a)
6. WHEN an `UpdateCertificate` request is received for an existing `certificateId`, THE IoT_Control_Plane SHALL set the `status` field to the supplied `newStatus` value when `newStatus` is one of `ACTIVE`, `INACTIVE`, `REVOKED`, `PENDING_TRANSFER`, `PENDING_ACTIVATION`. (P1a)
7. WHEN a `DeleteCertificate` request is received for a Certificate with status `ACTIVE`, THE IoT_Control_Plane SHALL return HTTP 409 with a `CertificateStateException` body. (P1a)
8. WHEN an `AttachThingPrincipal` request is received with an existing Thing and Certificate, THE IoT_Control_Plane SHALL record the attachment and SHALL return it from `ListThingPrincipals` and `ListPrincipalThings`. (P1a)
9. THE Local_CA primitives (CA generation, leaf signing, certificate retrieval) SHALL be implemented in a module reusable by ACM and API Gateway custom domains, located under `ministack/core/` rather than inside the IoT service module. (P1a)

### Requirement 4: IoT policies

**User Story:** As a developer, I want to manage IoT policies and attach them to certificates, so that my Terraform / CloudFormation code that wires IoT IAM works without modification.

#### Acceptance Criteria

1. WHEN a `CreatePolicy` request is received with a unique `policyName` and a syntactically valid JSON `policyDocument`, THE IoT_Control_Plane SHALL persist a Policy record at version `1` and return `policyArn`, `policyName`, `policyDocument`, and `policyVersionId`. (P1a)
2. IF a `CreatePolicy` request is received with a malformed JSON `policyDocument`, THEN THE IoT_Control_Plane SHALL return HTTP 400 with a `MalformedPolicyException` body. (P1a)
3. WHEN a `CreatePolicyVersion` request is received with `setAsDefault=true`, THE IoT_Control_Plane SHALL append a new version, increment the Policy's `policyVersionId`, and update the default-version pointer. (P1a)
4. WHEN an `AttachPolicy` request is received with an existing Policy and an existing Certificate or Thing-group target, THE IoT_Control_Plane SHALL persist the attachment and return it from `ListAttachedPolicies` and `ListTargetsForPolicy`. (P1a)
5. WHEN a `DetachPolicy` request is received for an existing attachment, THE IoT_Control_Plane SHALL remove it. (P1a)
6. THE IoT_Control_Plane SHALL implement `GetPolicy`, `ListPolicies`, `ListPolicyVersions`, `DeletePolicyVersion`, `DeletePolicy`, and `SetDefaultPolicyVersion` against the same persistence pattern. (P1a)
7. THE IoT_Data_Plane SHALL NOT enforce IoT Policy documents in Phase 1 or Phase 2; enforcement is deferred and the limitation SHALL be documented in `README.md`. (P1a)

### Requirement 5: DescribeEndpoint

**User Story:** As an SDK user, I want `DescribeEndpoint` to return a hostname that actually serves IoT traffic on Ministack, so that the SDK's auto-discovery code path produces a usable endpoint without manual override.

#### Acceptance Criteria

1. WHEN a `DescribeEndpoint` request is received with `endpointType=iot:Data-ATS` (or omitted, or `iot:Data` for legacy compatibility), THE IoT_Control_Plane SHALL return an `endpointAddress` in the format `identifier.iot.region.amazonaws.com` whose hostname resolves to the Ministack gateway and whose path semantics select the IoT_Data_Plane MQTT-over-WS transport. (P1a)
2. WHEN a `DescribeEndpoint` request is received with `endpointType=iot:Jobs`, THE IoT_Control_Plane SHALL return the same gateway hostname with a path or subdomain segment indicating the Jobs endpoint. (P3a)
3. THE `endpointAddress` returned by `DescribeEndpoint` SHALL follow the existing Ministack hostname convention used by other services (e.g. `<accountId>-ats.iot.<region>.<MINISTACK_HOST>` or an equivalent scheme defined in design), and SHALL be routable by `ministack/core/router.py`. (P1a)
4. WHEN the SDK opens an MQTT-over-WS connection to the address returned by `DescribeEndpoint`, THE IoT_Data_Plane SHALL accept the upgrade and serve MQTT traffic. (P1b)

### Requirement 6: Broker bridge with anonymous authentication

**User Story:** As a developer running the issue's use case, I want any client to be able to connect to the IoT_Data_Plane and pub/sub on arbitrary topics without IoT-specific auth, so that the unblocking scenario (Lambda publishes, browser subscribes) works in Phase 1.

#### Acceptance Criteria

1. THE IoT_Data_Plane SHALL accept MQTT-over-WebSocket connections on the Ministack gateway port using the `mqtt` Sec-WebSocket-Protocol value. Account is resolved from SigV4 credentials on the WS upgrade. (P1b)
2. THE IoT_Data_Plane SHALL accept MQTT-over-TLS connections on TCP port 8883 (configurable via `IOT_MTLS_PORT`) with client certificate authentication. Account is resolved by looking up the presented certificate in the IoT Certificate registry. (P2b)
3. WHEN an MQTT CONNECT packet is received over WebSocket in Phase 1, THE IoT_Data_Plane SHALL return CONNACK with reason code `0x00` (Connection accepted). (P1b)
4. THE IoT_Data_Plane SHALL support QoS 0 and QoS 1 for PUBLISH packets. (P1b)
5. THE IoT_Data_Plane SHALL support retained messages (PUBLISH with `retain=1`) and SHALL deliver the latest retained message on a topic to any new subscriber matching that topic filter. (P1b)
6. THE IoT_Data_Plane SHALL support `+` single-level and `#` multi-level MQTT topic wildcards on SUBSCRIBE. (P1b)
7. THE IoT_Data_Plane SHALL preserve message ordering within a single (publisher, topic) pair for QoS 0 and QoS 1. (P1b)

### Requirement 7: `iot-data` HTTP Publish API

**User Story:** As a Lambda function author, I want to call `iot-data Publish` over HTTP using the AWS SDK, so that publishing a message from Lambda to a topic that the browser subscribes to works without an MQTT client in the function.

#### Acceptance Criteria

1. WHEN an HTTP `POST /topics/{topic}` request is received on the `data.iot.<region>.<MINISTACK_HOST>` host with a body, THE IoT_Data_HTTP_API SHALL forward the body as an MQTT PUBLISH on the supplied topic via the Broker_Bridge and return HTTP 200. (P1b)
2. WHEN the request includes a `qos` query parameter with value `0` or `1`, THE IoT_Data_HTTP_API SHALL forward the publish at that QoS level. (P1b)
3. WHEN the request includes a `retain` query parameter with value `true`, THE IoT_Data_HTTP_API SHALL set the MQTT retain flag on the forwarded publish. (P1b)
4. IF the topic name is empty, contains an MQTT wildcard character (`+` or `#`), or exceeds 256 bytes, THEN THE IoT_Data_HTTP_API SHALL return HTTP 400 with an `InvalidRequestException` body. (P1b)
5. WHEN a `GetRetainedMessage` request is received for a topic that has a stored retained message, THE IoT_Data_HTTP_API SHALL return the payload, qos, and last-modified timestamp. (P2b)
6. WHEN a `ListRetainedMessages` request is received, THE IoT_Data_HTTP_API SHALL return the topic, qos, and last-modified timestamp for every retained message. (P2b)
7. WHEN an MQTT subscriber is connected to the IoT_Data_Plane on a topic filter that matches the published topic, THE Broker_Bridge SHALL deliver the payload to that subscriber, satisfying the original Lambda-publishes / browser-subscribes use case end-to-end. (P1b)

### Requirement 8: SigV4-signed WebSocket upgrades

**User Story:** As an SDK user with `aws-iot-device-sdk-js-v2`, I want SigV4-signed WebSocket connections to the IoT_Data_Plane to succeed, so that the official SDK path works locally without dropping down to a raw `mqtt.js` client. This is required for the original use case (browser subscribes via the official SDK which always signs the WS upgrade with SigV4).

#### Acceptance Criteria

1. WHEN an HTTP Upgrade request is received on the IoT data hostname with an `X-Amz-Algorithm=AWS4-HMAC-SHA256` query parameter or `Authorization: AWS4-HMAC-SHA256` header, THE IoT_Data_Plane SHALL accept the upgrade and treat it as an authenticated MQTT-over-WS session. (P1b)
2. THE SigV4 verification path SHALL reuse the existing Ministack SigV4 helper used by AppSync Events and API Gateway v2 WebSocket upgrades, with no service-specific re-implementation of canonical-request hashing. (P1b)
3. IF an HTTP Upgrade request is received with an `X-Amz-Algorithm` query parameter that is not `AWS4-HMAC-SHA256`, THEN THE IoT_Data_Plane SHALL return HTTP 400 and SHALL NOT establish the WebSocket. (P1b)
4. WHEN the SigV4 credential scope's service name is not `iotdevicegateway` or `iotdata`, THE IoT_Data_Plane SHALL return HTTP 403 and SHALL NOT establish the WebSocket. (P1b)

### Requirement 9: mTLS on port 8883

**User Story:** As a developer simulating production transport security locally, I want clients holding a Local_CA-signed Certificate to connect to the IoT_Data_Plane over mTLS on port 8883, so that the SDK's `CertificateProvider` and `MqttClientConnection` mTLS code paths execute end-to-end.

#### Acceptance Criteria

1. THE IoT_Data_Plane SHALL accept MQTT-over-TLS connections on TCP port 8883 (configurable via `IOT_MTLS_PORT`). (P2b)
2. THE IoT_Data_Plane SHALL present a server certificate signed by the Local_CA on port 8883. (P2b)
3. WHEN a client presents a leaf certificate signed by the Local_CA whose `certificateId` resolves to a Certificate record with status `ACTIVE`, THE IoT_Data_Plane SHALL accept the connection. (P2b)
4. WHEN a client presents a leaf certificate whose `certificateId` resolves to a Certificate record with status `INACTIVE` or `REVOKED`, THE IoT_Data_Plane SHALL return TLS handshake failure or, if the handshake completes, MQTT CONNACK reason code `0x87` (Not Authorized). (P2b)
5. WHEN a client presents no client certificate on port 8883, THE IoT_Data_Plane SHALL return TLS handshake failure. (P2b)

### Requirement 10: Device Shadows (classic and named)

**User Story:** As a developer testing shadow-driven device sync, I want classic and named Device_Shadow APIs to behave like AWS, so that my code that reads `desired` / `reported` / `delta` and reacts to shadow updates works without modification.

#### Acceptance Criteria

1. WHEN a `GetThingShadow` request is received for an existing Thing with no stored shadow, THE IoT_Control_Plane SHALL return HTTP 404 with a `ResourceNotFoundException` body. (P2a)
2. WHEN an `UpdateThingShadow` request is received with a body containing `state.desired`, `state.reported`, or both, THE IoT_Control_Plane SHALL merge the supplied state into the stored shadow document, increment `version` by one, and return the updated document. (P2a)
3. THE Device_Shadow `delta` section SHALL be computed as the keys present in `desired` whose values differ from the corresponding values in `reported`. (P2a)
4. WHEN `state.desired` and `state.reported` have equal values for the same key after an update, THE IoT_Control_Plane SHALL omit that key from `delta`. (P2a)
5. WHEN a `DeleteThingShadow` request is received for an existing shadow, THE IoT_Control_Plane SHALL remove the document and SHALL return HTTP 200 with the prior `version`. Deleting a shadow SHALL NOT reset its version counter; if the shadow is recreated, versioning SHALL continue from the next integer. (P2a)
6. THE IoT_Control_Plane SHALL implement named-shadow variants `GetThingShadow?name=<n>`, `UpdateThingShadow?name=<n>`, `DeleteThingShadow?name=<n>`, and `ListNamedShadowsForThing` with the same merge / version / delta semantics scoped to the supplied shadow name. (P2a)
7. WHEN a shadow is updated successfully, THE IoT_Control_Plane SHALL publish the AWS-shaped accepted/delta/documents/update messages on the corresponding `$aws/things/<thingName>/shadow/...` MQTT topics via the Broker_Bridge. (P2b)
8. WHEN an MQTT message is received on `$aws/things/<thingName>/shadow/update` (or the named-shadow equivalent), THE IoT_Control_Plane SHALL apply the same merge semantics as `UpdateThingShadow` and SHALL emit the corresponding `accepted` / `delta` / `documents` messages. (P2b)

### Requirement 11: Rules Engine — parser

**User Story:** As a developer wiring an IoT topic to SQS / SNS / Lambda / DynamoDB / Kinesis / Firehose, I want Ministack to parse my rule's SQL expression, so that `CreateTopicRule` succeeds locally for the same SQL my production code uses.

#### Acceptance Criteria

1. THE IoT_Control_Plane SHALL implement a parser for a documented subset of Rules_SQL covering optional `SET <variable> = <expr>`, `SELECT <projection>`, `FROM '<topic-filter>'`, and optional `WHERE <expr>`. The `SET` clause (used for reusable variables) is a documented limitation: only literal assignments are supported in Phase 3; complex expressions in `SET` are deferred. (P3a)
2. THE Rules_SQL parser SHALL accept projections of the form `*`, `<column>`, `<column> AS <alias>`, `<expr> AS <alias>`, and comma-separated lists of these. (P3a)
3. THE Rules_SQL parser SHALL accept `WHERE` expressions composed of column references, JSON-path references (`a.b.c`), string and numeric literals, the comparison operators `=`, `<>`, `<`, `<=`, `>`, `>=`, the logical operators `AND`, `OR`, `NOT`, parenthesisation, and the boolean literals `TRUE` / `FALSE`. (P3a)
4. WHEN a `CreateTopicRule` request is received with a `sql` string that the Rules_SQL parser cannot parse, THE IoT_Control_Plane SHALL return HTTP 400 with a `SqlParseException` body. (P3a)
5. THE IoT_Control_Plane SHALL implement a Rules_SQL pretty-printer that emits a parsed Rules_SQL AST as a normalised SQL string. (P3a)
6. FOR ALL Rules_SQL strings accepted by the Rules_SQL parser, the result of `parse → pretty-print → parse` SHALL produce an AST equivalent to the AST produced by the first `parse` (round-trip property). (P3a)
7. THE Rules_SQL parser SHALL support a documented subset of AWS IoT SQL built-in functions including at minimum: `timestamp()`, `topic()`, `topic(N)`, `clientid()`, `accountid()`, `cast()`, `encode()`, `decode()`, `lower()`, `upper()`, `concat()`, `substring()`, `regexp_matches()`, `abs()`, `ceil()`, `floor()`, `round()`, `mod()`, `power()`, `sqrt()`, `newuuid()`, and `parse_time()`. Functions not in the supported subset SHALL be accepted syntactically but SHALL evaluate to `Undefined` at runtime with a logged warning. (P3a)

### Requirement 12: Rules Engine — evaluation and action dispatch

**User Story:** As a developer testing IoT rule actions, I want a published message that matches a rule's topic filter and `WHERE` clause to trigger the rule's action against the corresponding Ministack service, so that end-to-end rule-driven flows can be verified locally.

#### Acceptance Criteria

1. WHEN a Rule with topic filter `T` and SQL `S` is registered AND a message is published on a topic that matches `T` AND the `WHERE` clause of `S` evaluates to true against the message JSON, THE Rules Engine SHALL dispatch each action declared on the Rule. (P3b)
2. THE Rules Engine SHALL support action types `lambda`, `sqs`, `sns`, `dynamoDB`, `dynamoDBv2`, `kinesis`, `firehose`, and `republish` by invoking the corresponding existing Ministack service through its in-process Python module rather than over HTTP. (P3b)
3. WHEN a `republish` action is dispatched, THE Rules Engine SHALL publish the projected payload to the action's `topic` field via the Broker_Bridge with the configured `qos`. (P3b)
4. WHEN an action invocation raises an exception, THE Rules Engine SHALL fall back to the rule's `errorAction` if configured, and otherwise SHALL log the failure and continue processing other actions on the same rule. (P3b)
5. THE IoT_Control_Plane SHALL implement `CreateTopicRule`, `GetTopicRule`, `ListTopicRules`, `ReplaceTopicRule`, `EnableTopicRule`, `DisableTopicRule`, and `DeleteTopicRule`. (P3a)
6. WHEN a Rule is in `DISABLED` state, THE Rules Engine SHALL NOT dispatch any actions for matching messages. (P3b)

### Requirement 13: Jobs

**User Story:** As a developer testing fleet job orchestration, I want `CreateJob` / `DescribeJob` / `UpdateJobExecution` to track per-Thing execution state, so that my device-side job-handling code can be exercised locally.

#### Acceptance Criteria

1. WHEN a `CreateJob` request is received with a `jobId`, a `targets` list of Thing or ThingGroup ARNs, and a `document` JSON value, THE IoT_Control_Plane SHALL persist a Job record in status `IN_PROGRESS` and SHALL create a JobExecution record in status `QUEUED` for every target Thing. (P3a)
2. WHEN a `DescribeJob` request is received for an existing `jobId`, THE IoT_Control_Plane SHALL return the Job record including status, target counts, and creation timestamp. (P3a)
3. WHEN an `UpdateJobExecution` request is received with `status` in {`IN_PROGRESS`, `SUCCEEDED`, `FAILED`, `REJECTED`, `REMOVED`, `CANCELED`}, THE IoT_Control_Plane SHALL update the corresponding JobExecution and SHALL emit the AWS-shaped notification on `$aws/things/<thingName>/jobs/notify-next` via the Broker_Bridge. (P3b)
4. WHEN every JobExecution for a Job has a terminal status, THE IoT_Control_Plane SHALL transition the parent Job to status `COMPLETED`. (P3a)
5. THE IoT_Control_Plane SHALL implement `CancelJob`, `DeleteJob`, `ListJobs`, and `ListJobExecutionsForThing`. (P3a)

### Requirement 14: Fleet Provisioning

**User Story:** As a developer testing factory-flow provisioning, I want `CreateProvisioningTemplate` and the `$aws/provisioning-templates/<name>/provision` MQTT API to issue Things and Certificates, so that my device-side fleet-provisioning code can be exercised locally.

#### Acceptance Criteria

1. WHEN a `CreateProvisioningTemplate` request is received with a `templateName`, `templateBody`, and `provisioningRoleArn`, THE IoT_Control_Plane SHALL persist a ProvisioningTemplate record. (P3a)
2. WHEN an MQTT message is received on `$aws/provisioning-templates/<templateName>/provision/json` with valid `parameters`, THE IoT_Control_Plane SHALL render the template, create the Thing and Certificate it specifies, and publish the response on `$aws/provisioning-templates/<templateName>/provision/json/accepted` via the Broker_Bridge. (P3b)
3. IF template rendering fails or the supplied `parameters` do not satisfy the template's required parameters, THEN THE IoT_Control_Plane SHALL publish a response on `$aws/provisioning-templates/<templateName>/provision/json/rejected` with an `errorCode` and `errorMessage`. (P3b)
4. THE IoT_Control_Plane SHALL implement `DescribeProvisioningTemplate`, `ListProvisioningTemplates`, `UpdateProvisioningTemplate`, and `DeleteProvisioningTemplate`. (P3a)

### Requirement 15: Persistence and account scoping

**User Story:** As a developer running multiple test suites concurrently, I want IoT control-plane state to be isolated per AWS account ID and to survive a warm reboot, so that Ministack behaves like every other service module.

#### Acceptance Criteria

1. THE IoT_Control_Plane SHALL store every record (Things, ThingTypes, ThingGroups, Certificates, Policies, Rules, Jobs, Shadows, ProvisioningTemplates) in `AccountScopedDict` containers from `ministack.core.responses`. (P1a)
2. THE IoT_Control_Plane SHALL implement `get_state` and `restore_state` functions and SHALL participate in the `ministack/core/persistence.py` lifecycle. (P1a)
3. WHEN a Ministack instance restarts with persistence enabled, THE IoT_Control_Plane SHALL restore every persisted record in its prior status. (P1a)
4. THE Local_CA certificate and private key SHALL be included in the IoT service's `get_state()` output and restored via `restore_state()` so that previously issued certificates remain valid across restarts when `PERSIST_STATE=1`. (P1a)

### Requirement 16: Configuration and observability

**User Story:** As a Ministack operator, I want to configure IoT ports and broker integration through environment variables and to see broker activity in logs, so that the new feature integrates with the existing operations model.

#### Acceptance Criteria

1. THE IoT_Data_Plane SHALL read its mTLS listener port from `IOT_MTLS_PORT` (default `8883`). (P2b)
2. WHERE the chosen broker integration strategy is "external sidecar", THE IoT_Control_Plane SHALL read the broker host and port from `IOT_BROKER_HOST` and `IOT_BROKER_PORT` environment variables. (P1b)
3. THE IoT_Control_Plane SHALL emit a structured log record for every successful control-plane mutation (Create/Update/Delete) using the existing `logging.getLogger("iot")` pattern. (P1a)
4. THE IoT_Data_Plane SHALL emit a structured log record for every CONNECT, DISCONNECT, SUBSCRIBE, UNSUBSCRIBE, and rejected PUBLISH event at INFO level. (P1b)

### Requirement 17: Test coverage for the unblocking use case

**User Story:** As a Ministack maintainer, I want an automated test that exercises the issue's original use case end-to-end, so that regressions in the Lambda-publishes / browser-subscribes path are caught immediately.

#### Acceptance Criteria

1. THE test suite at `tests/test_iot.py` SHALL exercise control-plane CRUD for Things, Certificates, Policies, ThingGroups, and `DescribeEndpoint` against the in-process Ministack instance. (P1a)
2. THE test suite SHALL include an end-to-end test that subscribes a Python MQTT client to a topic over the IoT_Data_Plane, calls `iot-data Publish` over HTTP for that topic, and asserts the subscriber receives the published payload within 2 seconds. (P1b)
3. THE test suite SHALL include a SigV4-WS upgrade test that drives the AWS SDK's signing path against the IoT_Data_Plane endpoint. (P1b)
4. THE test suite SHALL include a Device_Shadow test that asserts the `delta` field is recomputed correctly across a sequence of `desired` / `reported` updates. (P2a)
5. THE test suite SHALL include a Rules_SQL round-trip property test that, for a generator of valid Rules_SQL strings, asserts `parse(prettyPrint(parse(s))) == parse(s)`. (P3a)

## Out of Scope

The following IoT Core surfaces are explicitly out of scope for this feature and may be addressed in follow-up work:

- IoT Greengrass (v1 and v2) and any IoT Greengrass-specific control or data APIs.
- IoT SiteWise, IoT TwinMaker, IoT Events, IoT Analytics, IoT Wireless, IoT FleetWise.
- IoT Defender / Device Defender audit and detect APIs.
- Custom Authorizer Lambdas on the IoT_Data_Plane CONNECT path.
- IoT MQTT 5.0-only features beyond what MQTT 3.1.1 covers (user properties, shared subscriptions, topic aliases). MQTT 5.0 support is Best-Effort and depends on the chosen broker library.
- Basic Ingest topic prefix `$aws/rules/<ruleName>/...` as a publish-only shortcut. Targeted for a follow-up phase.
- IoT Core IPv6, dual-stack, and FIPS endpoints.

## Open Decisions for Design Phase

The requirements above are deliberately silent on the following choices, which the design phase will resolve:

1. **Broker integration strategy** — embedded Python broker library (e.g. `amqtt`), bundled binary (Mosquitto, mochi-mqtt-server, rumqttd), or external sidecar via `docker-compose.yml`. Affects image size, startup time, and how the Broker_Bridge is implemented.
2. **`DescribeEndpoint` hostname scheme** — must align with existing Ministack subdomain routing in `ministack/core/router.py`.
3. **Local CA module placement** — confirmation that the new `ministack/core/local_ca.py` (or equivalent) is the right home, and the migration plan for ACM and API Gateway custom domains to consume it.
4. **MQTT-over-WebSocket path** — whether MQTT-over-WS is served on the main Ministack gateway port multiplexed with HTTP (preferred, matches AppSync Events) or on a dedicated port.
5. **Phase-1 broker auth** — whether anonymous CONNECT is the only mode in Phase 1 or whether a username/password mode is also offered for parity with sidecar Mosquitto deployments.
