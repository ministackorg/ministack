# Implementation Plan: AWS IoT Core (Phase 1a + 1b)

## Overview

This plan implements the IoT Core control plane (Phase 1a) and data plane (Phase 1b) for Ministack. Tasks are ordered so dependencies are respected: shared infrastructure first (Local CA, router registration), then control-plane CRUD, then data-plane broker integration. Each task builds incrementally on the previous, ending with full end-to-end wiring.

## Tasks

- [x] 1. Create Local CA module (`ministack/core/local_ca.py`)
  - [x] 1.1 Implement CA generation and leaf certificate signing
    - Create `ministack/core/local_ca.py` with functions: `get_ca_cert_pem()`, `get_ca_key_pem()`, `sign_leaf_certificate()`, `get_certificate_id()`
    - Use `cryptography` library for RSA/ECDSA key generation and X.509 certificate creation
    - CA is generated lazily on first use (self-signed, 10-year validity)
    - `sign_leaf_certificate(common_name, san_dns, san_ips, days_valid, key_type)` returns `(cert_pem, private_key_pem, public_key_pem)`
    - `get_certificate_id(cert_pem)` returns SHA-256 fingerprint of DER encoding
    - _Requirements: 3.1, 3.9_

  - [x] 1.2 Implement `get_state` / `restore_state` for Local CA persistence
    - `get_state()` returns `{"ca_cert_pem": ..., "ca_key_pem": ...}`
    - `restore_state(data)` restores CA from persisted PEM strings
    - CA must survive restarts when `PERSIST_STATE=1` (unlike ephemeral `tls.py`)
    - _Requirements: 3.1, 15.4_

  - [ ]* 1.3 Write property test: Certificate issuance produces valid X.509 signed by Local CA
    - **Property 6: Certificate issuance produces valid X.509 signed by Local CA**
    - Generate random common names and key types; verify returned cert is valid X.509, issuer matches CA subject, status matches `setAsActive` flag
    - **Validates: Requirements 3.2, 3.3**

- [x] 2. Register IoT services in router and app
  - [x] 2.1 Add `iot` and `iot-data` service patterns to `ministack/core/router.py`
    - Add `"iot"` entry with `host_patterns: [r"iot\."]` and `credential_scope: "iot"`
    - Add `"iot-data"` entry with `host_patterns: [r"data-ats\.iot\.", r"data\.iot\."]`, `credential_scope: "iotdata"`, and `path_prefixes: ["/topics/"]`
    - _Requirements: 1.1, 5.3_

  - [x] 2.2 Register IoT services in `ministack/app.py`
    - Add `"iot": {"module": "iot"}` and `"iot-data": {"module": "iot_data"}` to `SERVICE_REGISTRY`
    - Extend `_S3_VHOST_EXCLUDE_RE` to exclude IoT hostnames from S3 routing
    - Add `GET /_ministack/iot/ca.pem` handler to serve the Local CA root certificate
    - _Requirements: 1.1, 3.4, 5.3_

- [x] 3. Implement IoT control plane (`ministack/services/iot.py`)
  - [x] 3.1 Create service skeleton with routing and persistence
    - Create `ministack/services/iot.py` with `handle_request()`, `get_state()`, `restore_state()`, `reset()`
    - Set up `AccountScopedDict` containers for Things, ThingTypes, ThingGroups, Certificates, Policies
    - Dispatch actions via `x-amz-target` header (pattern: `AWSIotService.<Action>`)
    - Integrate Local CA state into `get_state()` / `restore_state()`
    - _Requirements: 1.1, 15.1, 15.2, 15.3_

  - [x] 3.2 Implement Thing CRUD operations
    - `CreateThing` — persist with UUID thingId, ARN, version=1, idempotency check (same config → success, different config → 409)
    - `DescribeThing` — return stored record or 404
    - `ListThings` — return all Things with optional `attributeName`/`attributeValue`/`thingTypeName` filters
    - `UpdateThing` — merge/replace attributes, increment version
    - `DeleteThing` — remove record, detach all attached certificates
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_

  - [ ]* 3.3 Write property tests for Thing CRUD
    - **Property 1: Thing CRUD round-trip**
    - **Property 2: CreateThing idempotency**
    - **Property 3: ListThings filter correctness**
    - **Property 4: UpdateThing version increment invariant**
    - **Property 5: DeleteThing detaches principals**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.5, 2.6, 2.7**

  - [x] 3.4 Implement ThingType and ThingGroup CRUD
    - `CreateThingType`, `DescribeThingType`, `ListThingTypes`, `DeprecateThingType`, `DeleteThingType`
    - `CreateThingGroup`, `DescribeThingGroup`, `ListThingGroups`, `AddThingToThingGroup`, `RemoveThingFromThingGroup`, `DeleteThingGroup`
    - Follow same persistence and error-handling patterns as Thing CRUD
    - _Requirements: 2.8, 2.9, 2.10_

  - [x] 3.5 Implement Certificate operations
    - `CreateKeysAndCertificate` — call `local_ca.sign_leaf_certificate()`, persist Certificate record with status based on `setAsActive`
    - `RegisterCertificate` — persist supplied PEM verbatim without re-signing
    - `DescribeCertificate` — return stored record or 404
    - `ListCertificates` — return all certificates for the account
    - `UpdateCertificate` — set status to supplied `newStatus`
    - `DeleteCertificate` — reject if status is ACTIVE (409 CertificateStateException), otherwise remove
    - `AttachThingPrincipal` / `DetachThingPrincipal` — manage bidirectional Thing↔Certificate links
    - `ListThingPrincipals` / `ListPrincipalThings` — query attachment links
    - _Requirements: 3.2, 3.3, 3.5, 3.6, 3.7, 3.8_

  - [ ]* 3.6 Write property tests for Certificate operations
    - **Property 7: RegisterCertificate preserves PEM verbatim**
    - **Property 8: UpdateCertificate status transition**
    - **Property 9: AttachThingPrincipal / DetachThingPrincipal round-trip**
    - **Validates: Requirements 3.5, 3.6, 3.8**

  - [x] 3.7 Implement Policy operations
    - `CreatePolicy` — validate JSON document (400 MalformedPolicyException if invalid), persist at version "1"
    - `CreatePolicyVersion` — append version, update default pointer if `setAsDefault=true`
    - `GetPolicy`, `ListPolicies`, `ListPolicyVersions`, `DeletePolicyVersion`, `DeletePolicy`, `SetDefaultPolicyVersion`
    - `AttachPolicy` / `DetachPolicy` — manage Policy↔target (Certificate ARN or ThingGroup ARN) links
    - `ListAttachedPolicies` / `ListTargetsForPolicy` — query attachment links
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [ ]* 3.8 Write property tests for Policy operations
    - **Property 10: Policy version numbering**
    - **Property 11: Policy attachment round-trip**
    - **Validates: Requirements 4.1, 4.3, 4.4, 4.5**

  - [x] 3.9 Implement DescribeEndpoint
    - Return `endpointAddress` in format `{prefix}-ats.iot.{region}.{MINISTACK_HOST}:{GATEWAY_PORT}`
    - `prefix` = `hashlib.sha256(account_id.encode()).hexdigest()[:14]`
    - Support `endpointType` parameter (`iot:Data-ATS`, `iot:Data`, or omitted)
    - _Requirements: 5.1, 5.3_

- [x] 4. Checkpoint — Control plane tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement Broker Bridge (`ministack/services/iot_broker.py`)
  - [x] 5.1 Create broker lifecycle and bridge interface
    - Create `ministack/services/iot_broker.py` with `start_broker()`, `stop_broker()`, `publish()`, `subscribe()`, `unsubscribe()`, `is_available()`
    - Lazy broker startup via `_ensure_broker()` with asyncio lock
    - Configure amqtt with loopback-only listener on ephemeral port, anonymous auth, no topic-check
    - `is_available()` returns `False` when amqtt is not installed (ImportError guard)
    - _Requirements: 1.3, 1.4, 1.5_

  - [x] 5.2 Implement topic prefixing for multi-tenancy
    - `_scoped_topic(account_id, topic)` → `"{account_id}/{topic}"`
    - `_unscope_topic(account_id, scoped_topic)` → strip prefix before delivery
    - Apply prefixing transparently in `publish()`, `subscribe()`, and WS handler
    - _Requirements: 6.1, 15.1_

  - [x] 5.3 Implement MQTT frame parser for WebSocket relay
    - Parse MQTT 3.1.1 fixed header + variable header for CONNECT, CONNACK, PUBLISH, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT
    - Rewrite topic fields in PUBLISH and SUBSCRIBE packets to add/strip account prefix
    - ~150-200 lines of binary frame parsing/rewriting
    - _Requirements: 6.1, 6.4, 6.5, 6.6_

  - [x] 5.4 Implement WebSocket handler (`handle_websocket`)
    - Accept ASGI WebSocket scope with `Sec-WebSocket-Protocol: mqtt`
    - Validate SigV4 on upgrade request (reuse existing helper), extract account_id
    - Relay MQTT binary frames between client WS and internal broker with topic rewriting
    - Return CONNACK 0x00 for anonymous CONNECT in Phase 1
    - _Requirements: 6.1, 6.3, 8.1, 8.2_

  - [ ]* 5.5 Write property test: Message ordering preservation
    - **Property 14: Message ordering preservation**
    - Publish N messages to a single topic from one publisher at QoS 1; verify subscriber receives all N in order
    - **Validates: Requirements 6.7**

- [x] 6. Implement IoT Data HTTP API (`ministack/services/iot_data.py`)
  - [x] 6.1 Create `iot_data.py` with HTTP Publish endpoint
    - Create `ministack/services/iot_data.py` with `handle_request()`
    - Route `POST /topics/{topic}` — validate topic (non-empty, no wildcards, ≤256 bytes UTF-8), call `iot_broker.publish()`
    - Support `qos` query parameter (0 or 1, default 0)
    - Support `retain` query parameter (`true`/`false`, default false)
    - Return 503 `InternalFailureException` if broker unavailable
    - Return 400 `InvalidRequestException` for invalid topics
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 1.6_

  - [ ]* 6.2 Write property test: Invalid topic rejection on HTTP Publish
    - **Property 12: Invalid topic rejection on HTTP Publish**
    - Generate topics that are empty, contain `+`, contain `#`, or exceed 256 bytes; verify HTTP 400 response
    - **Validates: Requirements 7.4**

- [x] 7. Wire WebSocket dispatch in `ministack/app.py`
  - [x] 7.1 Add IoT WebSocket routing to ASGI app
    - Add `_IOT_DATA_WS_RE` pattern matching `.iot.` in host header
    - In WebSocket scope handler, check for IoT data hostname + `Sec-WebSocket-Protocol: mqtt`
    - Route matching connections to `iot_broker.handle_websocket()`
    - Validate SigV4 query parameters on upgrade, extract account_id, reject invalid signatures (400/403)
    - _Requirements: 5.4, 6.1, 8.1, 8.2, 8.3, 8.4_

- [x] 8. Checkpoint — Data plane integration tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Write control-plane test suite (`tests/test_iot.py`)
  - [x] 9.1 Implement control-plane CRUD tests
    - Test Thing CRUD happy paths (create, describe, list, update, delete)
    - Test ThingType and ThingGroup CRUD
    - Test Certificate lifecycle (create, register, update status, delete with active guard)
    - Test Policy CRUD and versioning
    - Test DescribeEndpoint format
    - Test error cases (404, 409, 400 for each resource type)
    - Test account isolation (two accounts don't see each other's Things)
    - _Requirements: 17.1_

  - [ ]* 9.2 Write property test: Persistence round-trip
    - **Property 13: Persistence round-trip (get_state / restore_state)**
    - Create various Things, Certificates, Policies, ThingTypes, ThingGroups; call `get_state()`, `reset()`, `restore_state(saved)`; verify all records retrievable with identical field values
    - **Validates: Requirements 15.2, 15.3**

- [x] 10. Write data-plane test suite (`tests/test_iot_data.py`)
  - [x] 10.1 Implement data-plane integration tests
    - Test HTTP Publish end-to-end (publish via HTTP, receive via MQTT subscriber)
    - Test SigV4 WebSocket upgrade acceptance
    - Test SigV4 rejection (wrong algorithm → 400, wrong service name → 403)
    - Test broker unavailable → 503
    - Test invalid topic → 400
    - Test retained messages (publish with retain, new subscriber gets last retained)
    - Test wildcard subscriptions (`+` and `#` patterns)
    - _Requirements: 17.2, 17.3_

- [x] 11. Update `pyproject.toml` with new dependencies
  - [x] 11.1 Add `amqtt` to `[full]` extras and test deps to `[dev]`
    - Add `"amqtt>=0.11"` to `[project.optional-dependencies] full`
    - Add `"hypothesis>=6.100"` and `"paho-mqtt>=2.0"` to `[project.optional-dependencies] dev`
    - _Requirements: 1.3_

- [x] 12. Final checkpoint — Full test suite passes
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit/integration tests validate specific examples and edge cases
- The design uses Python throughout — all implementation is in Python
- `amqtt` is an optional dependency; control plane works without it (data plane returns 503)
