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
    - Configure in-memory pub/sub registry with topic matching and retained message store
    - `is_available()` returns `True` when `IOT_BROKER_ENABLED` is not `0`
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
  - [x] 11.1 Add test deps to `[dev]`
    - Add `"hypothesis>=6.100"` and `"paho-mqtt>=2.0"` to `[project.optional-dependencies] dev`
    - _Requirements: 1.3_

- [x] 12. Final checkpoint — Full test suite passes
  - Ensure all tests pass, ask the user if questions arise.

---

## Phase 1c: MQTT Fidelity Improvements

- [ ] 13. Fix retained message + wildcard behavior
  - [ ] 13.1 Modify `subscribe()` to skip retained message replay for wildcard filters
    - In `iot_broker.py`, update the `subscribe()` function to check if `topic_filter` contains `+` or `#`
    - When a wildcard is present, skip the retained-message replay loop (set `retained_to_send = []`)
    - Exact-match subscriptions (no wildcards) continue to receive stored retained messages as before
    - Real-time delivery of messages published with `retain=1` remains unaffected for all subscribers (including wildcard)
    - _Requirements: 18.1, 18.2, 18.3_

  - [ ]* 13.2 Write property test: Retained message wildcard exclusion
    - **Property 15: Retained message wildcard exclusion**
    - Generate topics with stored retained messages and wildcard filters; verify wildcard subscriptions do NOT receive retained replay, exact-match subscriptions DO
    - **Validates: Requirements 18.1, 18.2**

  - [ ]* 13.3 Write property test: Real-time delivery ignores retain storage semantics
    - **Property 16: Real-time delivery ignores retain storage semantics**
    - Publish with `retain=1` while wildcard subscribers are connected; verify they receive the message in real-time
    - **Validates: Requirements 18.3**

- [ ] 14. Add topic validation on MQTT PUBLISH packets (WebSocket path)
  - [ ] 14.1 Implement `_validate_publish_topic()` and integrate into PUBLISH handler
    - Add `_validate_publish_topic(topic: str) -> bool` function to `iot_broker.py`
    - Validation rules: reject empty topics, topics containing `+` or `#`, topics exceeding 256 bytes UTF-8
    - In `handle_packet` for `PKT_PUBLISH`, call `_validate_publish_topic()` before forwarding
    - If validation fails, log a warning and return `False` to terminate the session (triggers WebSocket close)
    - This matches the same validation already applied in `iot_data.py` for HTTP Publish
    - _Requirements: 23.1, 23.2, 23.3, 23.4_

  - [ ]* 14.2 Write property test: MQTT PUBLISH topic validation consistency
    - **Property 23: MQTT PUBLISH topic validation consistency**
    - Generate topic strings (empty, with wildcards, oversized, valid); verify MQTT PUBLISH handler and HTTP Publish handler make identical accept/reject decisions
    - **Validates: Requirements 23.1, 23.2, 23.3, 23.4**

- [ ] 15. Implement Client ID duplicate detection and forced disconnection
  - [ ] 15.1 Add `_connected_clients` registry and duplicate detection logic
    - Add module-level `_connected_clients: dict[tuple[str, str], _WSSession] = {}` to `iot_broker.py`
    - Implement `_register_client(account_id, client_id, session)` and `_deregister_client(account_id, client_id)`
    - Implement `_force_disconnect_duplicate(account_id, client_id)` — if an existing session is found, force-close its WebSocket and call `cleanup()`
    - Key is `(account_id, client_id)` so different accounts can use the same Client ID without conflict
    - _Requirements: 22.1, 22.2, 22.3_

  - [ ] 15.2 Parse Client ID from CONNECT and integrate registration
    - Extend `handle_packet` for `PKT_CONNECT` to parse the Client ID from the CONNECT payload
    - If Client ID is empty, auto-generate a unique ID using `uuid.uuid4().hex`
    - Call `_force_disconnect_duplicate()` before accepting the new connection
    - Call `_register_client()` after accepting
    - Call `_deregister_client()` in `cleanup()`
    - Add `_client_id` field to `_WSSession`
    - _Requirements: 22.1, 22.3, 22.4_

  - [ ]* 15.3 Write property test: Duplicate Client ID forces old connection closed
    - **Property 22: Duplicate Client ID forces old connection closed**
    - Two clients with same Client ID in same account → first is force-closed; two clients with same Client ID in different accounts → both remain active
    - **Validates: Requirements 22.1, 22.2, 22.3**

- [ ] 16. Implement Last Will and Testament (LWT)
  - [ ] 16.1 Parse Will fields from CONNECT packet
    - Extend the CONNECT handler in `_WSSession.handle_packet` to parse Connect Flags byte
    - Extract Will Flag, Will QoS, Will Retain from Connect flags
    - When Will Flag is set, parse Will Topic (UTF-8 string) and Will Message (length-prefixed bytes) from payload
    - Store in session fields: `_will_topic`, `_will_message`, `_will_qos`, `_will_retain`
    - Add `_graceful_disconnect: bool = False` field to `_WSSession`
    - _Requirements: 19.1, 19.5_

  - [ ] 16.2 Publish Will message on ungraceful disconnect
    - In `handle_packet` for `PKT_DISCONNECT`, set `self._graceful_disconnect = True`
    - In `cleanup()`, check: if NOT graceful disconnect AND Will fields are set, call `publish()` with Will Topic/Message/QoS/Retain
    - If Will Retain is set, the published Will message becomes a retained message
    - On reconnect (new CONNECT on same session), replace stored Will fields with new CONNECT's Will fields
    - _Requirements: 19.2, 19.3, 19.4, 19.5_

  - [ ]* 16.3 Write property test: Will message published iff disconnect is ungraceful
    - **Property 17: Will message published if and only if disconnect is ungraceful**
    - Connect with Will, ungraceful disconnect → Will published; graceful DISCONNECT → Will NOT published; Will Retain flag → retained message stored
    - **Validates: Requirements 19.1, 19.2, 19.3, 19.4**

- [ ] 17. Checkpoint — Retained, topic validation, Client ID, and LWT tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 18. Implement QoS 1 end-to-end delivery
  - [ ] 18.1 Track granted QoS per subscription
    - Add `granted_qos: int` field to `_Subscription` class
    - In `handle_packet` for `PKT_SUBSCRIBE`, parse the requested QoS for each topic filter
    - Grant `min(requested_qos, 1)` (cap at QoS 1, no QoS 2 support)
    - Store granted QoS on the subscription; return it in SUBACK
    - _Requirements: 21.1, 21.2_

  - [ ] 18.2 Implement QoS 1 delivery with packet ID tracking
    - Add `_in_flight: dict[int, _InFlightMessage]` and `_alloc_packet_id()` to `_WSSession`
    - In `deliver_to_client()`, compute effective QoS as `min(publish_qos, subscription_granted_qos)`
    - If effective QoS is 1: allocate packet ID, store in `_in_flight`, send PUBLISH with QoS 1 and packet ID
    - If effective QoS is 0: send PUBLISH at QoS 0 (no packet ID, no tracking)
    - _Requirements: 21.1, 21.2, 21.5_

  - [ ] 18.3 Implement PUBACK handling and retransmission
    - In `handle_packet`, add handler for `PKT_PUBACK`: extract packet ID, remove from `_in_flight`
    - Add retransmission background task: periodically check `_in_flight`, retransmit unacknowledged messages with DUP flag set
    - Retransmit interval configurable via `IOT_RETRANSMIT_SECONDS` (default 10s)
    - Clean up retransmit task in `cleanup()`
    - _Requirements: 21.3, 21.4, 21.5_

  - [ ]* 18.4 Write property test: Effective QoS is minimum of publish and subscription QoS
    - **Property 20: Effective QoS is minimum of publish and subscription QoS**
    - Publish at QoS P, subscribe at QoS S; verify delivered QoS is `min(P, S)`
    - **Validates: Requirements 21.1, 21.2**

  - [ ]* 18.5 Write property test: PUBACK stops retransmission
    - **Property 21: PUBACK stops retransmission**
    - Deliver QoS 1 message, send PUBACK; verify no retransmission. Verify packet IDs are monotonically increasing (mod 65535)
    - **Validates: Requirements 21.4, 21.5**

- [ ] 19. Implement persistent sessions (cleanSession flag)
  - [ ] 19.1 Add persistent session storage and CONNECT flow
    - Add `_persistent_sessions: dict[tuple[str, str], _PersistentSessionState]` to `iot_broker.py`
    - Add `_PersistentSessionState` class with `subscriptions`, `queued_messages`, `created_at` fields
    - Parse `cleanSession` flag from CONNECT flags byte (bit 1)
    - Add `_clean_session: bool` field to `_WSSession`
    - Read `IOT_SESSION_EXPIRY_SECONDS` from environment (default 3600)
    - _Requirements: 20.1, 20.5, 20.6_

  - [ ] 19.2 Implement session restoration on reconnect
    - On CONNECT with `cleanSession=0`: check `_persistent_sessions` for existing session
    - If found and not expired: restore subscriptions, send CONNACK with `sessionPresent=1`, deliver queued messages
    - If not found or expired: create new session, send CONNACK with `sessionPresent=0`
    - On CONNECT with `cleanSession=1`: discard any prior session state, send CONNACK with `sessionPresent=0`
    - _Requirements: 20.1, 20.2, 20.4, 20.5_

  - [ ] 19.3 Implement offline message queuing
    - In `publish()`, after delivering to connected subscribers, check `_persistent_sessions` for disconnected sessions with matching subscriptions
    - Queue QoS 1 messages for matching disconnected persistent sessions (append to `queued_messages`)
    - Bound queue to 1000 messages per session (drop oldest on overflow)
    - In `cleanup()`, if `_clean_session` is False, call `_preserve_session()` to store subscriptions and timestamp
    - Add `_is_session_expired()` helper using `IOT_SESSION_EXPIRY_SECONDS`
    - _Requirements: 20.3, 20.4, 20.6_

  - [ ]* 19.4 Write property test: Persistent session subscription round-trip
    - **Property 18: Persistent session subscription round-trip**
    - Connect with `cleanSession=0`, subscribe, disconnect, reconnect → `sessionPresent=1` and subscriptions active; reconnect with `cleanSession=1` → `sessionPresent=0` and no prior subscriptions
    - **Validates: Requirements 20.1, 20.2, 20.5**

  - [ ]* 19.5 Write property test: Offline QoS 1 message queuing and delivery
    - **Property 19: Offline QoS 1 message queuing and delivery**
    - Persistent session disconnects; QoS 1 messages published to matching topics; reconnect → all queued messages delivered
    - **Validates: Requirements 20.3, 20.4**

- [ ] 20. Checkpoint — QoS 1 and persistent sessions tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 21. Handle AWS IoT Core MQTT behavioral notes
  - [ ] 21.1 Verify QoS 2 packet handling and `#` wildcard parent-topic behavior
    - Confirm that QoS 2 packets (PUBREC=5, PUBREL=6, PUBCOMP=7) are silently ignored (no error, connection stays open)
    - Confirm that DUP flag on inbound PUBLISH is treated as informational only
    - Confirm that `sensor/#` does NOT match topic `sensor` (only matches `sensor/child` and deeper)
    - Add explicit handling for QoS 2 packet types if not already covered (return True without processing)
    - _Requirements: 24.1, 24.2, 24.3, 24.4_

  - [ ]* 21.2 Write property test: Multi-level wildcard `#` does not match parent topic
    - **Property 24: Multi-level wildcard `#` does not match parent topic**
    - For topic `T` and filter `T/#`, publishing to `T` is NOT delivered; publishing to `T/child` IS delivered
    - **Validates: Requirements 24.3**

- [ ] 22. Write Phase 1c integration tests (`tests/test_iot_broker.py`)
  - [ ] 22.1 Write integration tests for Phase 1c features
    - Test retained message NOT delivered on wildcard subscribe (`sensor/+`, `sensor/#`)
    - Test retained message IS delivered on exact-match subscribe
    - Test Will message published on ungraceful disconnect (WebSocket close without DISCONNECT)
    - Test Will message NOT published on graceful DISCONNECT
    - Test Will with retain flag stores retained message
    - Test persistent session: connect → subscribe → disconnect → reconnect → `sessionPresent=1` → messages delivered
    - Test clean session: connect with `cleanSession=1` discards prior state
    - Test QoS 1 delivery with packet ID assigned
    - Test PUBACK removes in-flight message (no retransmission)
    - Test duplicate Client ID disconnects old client
    - Test topic validation rejects empty, wildcard, and oversized topics on MQTT PUBLISH
    - Test `#` wildcard does not match parent topic
    - Test QoS 2 packets are silently ignored
    - _Requirements: 18.1, 18.2, 19.1, 19.2, 19.3, 20.1, 20.2, 20.3, 21.1, 21.4, 22.1, 23.1, 23.2, 24.1, 24.3_

- [ ] 23. Final checkpoint — Full Phase 1c test suite passes
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit/integration tests validate specific examples and edge cases
- The design uses Python throughout — all implementation is in Python
- The broker is a custom in-memory Python pub/sub registry — no external dependencies required
- Phase 1c tasks (13–23) are confined entirely to `ministack/services/iot_broker.py` and test files — no new modules introduced
- Phase 1c tasks build incrementally: simpler fixes first (retained, topic validation), then stateful features (Client ID, LWT, QoS 1, persistent sessions)


## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["13.1", "14.1"] },
    { "id": 1, "tasks": ["13.2", "13.3", "14.2", "15.1"] },
    { "id": 2, "tasks": ["15.2", "16.1"] },
    { "id": 3, "tasks": ["15.3", "16.2"] },
    { "id": 4, "tasks": ["16.3", "18.1", "21.1"] },
    { "id": 5, "tasks": ["18.2"] },
    { "id": 6, "tasks": ["18.3"] },
    { "id": 7, "tasks": ["18.4", "18.5", "19.1"] },
    { "id": 8, "tasks": ["19.2"] },
    { "id": 9, "tasks": ["19.3"] },
    { "id": 10, "tasks": ["19.4", "19.5", "21.2"] },
    { "id": 11, "tasks": ["22.1"] }
  ]
}
```
