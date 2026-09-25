# MySQL gatekeeper connection spike

Local-only experiment branched from PR #1803 at `b1f8ca2`. No runtime service,
container provisioning, CI, or existing plugin code is changed by this spike.
Do not deploy this server: its protocol support is intentionally incomplete.

The experiment is retained on the original PR branch for evaluation alongside
the existing broker-callback implementation; it does not replace or activate
that implementation. Phase 1 is a negative comparison, not a second production
adapter. Both experiments remain outside the normal runtime and CI test lane.

## Phase 2/3: accepting shim with a mandatory gatekeeper

The follow-up experiment **works without backend IAM passwords or SQL rewriting**.
It retains a minimal `AWSAuthenticationPlugin` that accepts the login after
reading the auth packet. A Python proxy handles the frontend IAM admission
decision and relays normal password authentication to MySQL unchanged.
The shim has no libcurl dependency or IAM verifier. It is unsafe by itself.

The adapter now calls the existing Python IAM decision path with SDK-generated
SigV4 tokens and fixture-owned IAM/RDS state. It no longer uses a token equality
stub or an IAM-user allowlist. The original experiment below is retained as a
negative comparison.

Local ARM64 validation on 2026-09-25: **60 passed in 21.83s** across MySQL
8.0/8.4 connection tests and offline protocol checks. Ruff passed. The final
labeled container and network inventories were empty.

Verified on both versions:

- IAM users log in as themselves, keep `AWSAuthenticationPlugin` in
  `SHOW CREATE USER`, and retain MySQL-enforced SELECT/write restrictions.
- Ordinary `caching_sha2_password` users retain their plugin type and grants.
  Correct passwords work and incorrect passwords fail with TLS on or off,
  in both strict and permissive proxy modes. No backend user password is
  provisioned to the proxy; these are real relayed password handshakes.
- An account's existing `REQUIRE SSL` rule is still enforced, even when the
  proxy is permissive. Locked password and IAM accounts cannot log in.
- Strict IAM admission rejects plaintext, incorrect tokens, and another
  user's token. Permissive IAM admission allows arbitrary tokens over plaintext.
- Real authorization rejects expired tokens, bad signatures, wrong endpoints,
  and correctly signed tokens without the required `rds-db:connect` permission.
- MySQL selects a private client authentication method for IAM accounts. The
  proxy translates that method to `mysql_clear_password` only after applying
  the TLS gate. Clients cannot claim the private method in their initial login.
- Creating an IAM account and altering it to password authentication and back
  takes effect on subsequent connections without refreshing a proxy user list.
- A native MySQL client completes a cold-cache `caching_sha2_password` RSA
  exchange over plaintext; no backend password is stored in the proxy.
- `COM_CHANGE_USER` is rejected for both account types, not silently relayed.
- Direct backend access fails from the host **even with an intentionally
  published backend port**, and from another container on the same bridge.
- Negative control: a process inside the trusted namespace can log directly
  into the accepting shim with arbitrary credentials. This is expected and
  demonstrates that the network boundary is essential.

Topology: MySQL binds `127.0.0.1:3306` inside its container. The proxy shares its
network namespace and binds externally reachable frontend ports. Backend ports
must not normally be published; the test publishes one only to verify that
the loopback binding defeats accidental forwarding. Docker administrators and
processes inside the namespace are trusted, not protected from this bypass.
This is a tested candidate topology, not a change to MiniStack provisioning.

To reproduce on the existing ARM64 spike environment:

```sh
# Cached compiler stages only; no build changes or CI additions.
docker build --file Dockerfile.full --target plugin-build-80 --tag ministack-spike-compiler80:local .
docker build --file Dockerfile.full --target plugin-build --tag ministack-iam-stage7-build:local .
uv run --extra dev pytest contrib/mysql-gatekeeper-spike/test_accepting_flow.py contrib/mysql-gatekeeper-spike/test_protocol.py -q
uv run --extra dev ruff check contrib/mysql-gatekeeper-spike/
```

Run `uv run --extra dev pytest contrib/mysql-gatekeeper-spike/ -q` to include
the retained phase-1 comparison (74 tests total). The cold-cache RSA check also
requires a native `mysql` client on PATH. These fixtures own their disposable
Docker resources; the repository's Compose stack is not required.

The fixture compiles the separate accepting shim without `-lcurl`, uses
`ghcr.io/ministackorg/ministack:full` only as a Python runtime for the proxy,
and removes its containers, anonymous volumes, and custom network afterward.
They bear `ministack.task=accepting-gate-spike`; the post-run inventory was empty.

### What this does not prove

`proof_authorization.py` bootstraps local IAM/RDS state and a resource capability,
then calls the existing private `rds_iam._decision` seam. This proves reuse of
SigV4/policy verification rather than a second implementation, not production
resource provisioning. A supported internal adapter API should replace that
private seam before integration. The proxy uses the actual `AUTH` environment
variable; the live tests cover true and false (unset follows the false branch).
The existing decision path retains capability/current-resource checks in both
modes. This suite does not separately exercise every resource lifecycle case.

The shim advertises `ministack_iam_gate_v1`, forcing a backend-selected auth
switch even when a client initially selects a standard password method. Initial
private-method claims are rejected before forwarding. This classification
depends on the trusted backend and this exact shim, not arbitrary third-party
plugins. Other cleartext-auth plugins are unsupported.

The command set is deliberately limited: no prepared statements, compression,
full reauthentication, or full client compatibility. Password coverage here is
`caching_sha2_password`, not every MySQL authentication plugin. Backend TLS is
encrypted when frontend TLS is used, but its certificate is not verified.
Host-specific MySQL account matching also needs design/tests: a proxied client
is seen from loopback, so these `%`-host fixture results do not prove preservation
of arbitrary host-specific account selection.

Packets are capped at 1 MiB and socket operations time out after five seconds;
idle command sessions close after ten seconds. These are proof bounds, not
production connection limits or an overall handshake deadline. Concurrency,
fragmentation, multi-statement behavior, and a broader client matrix still need
hardening. Compression and LOCAL INFILE capabilities are disabled.

The installed PyMySQL 1.2.3 cold RSA path returns no packet to its caller and
raises an AttributeError. The cold-cache plaintext test therefore uses the
native MySQL 8.0 client; PyMySQL covers TLS and warmed password authentication.
This does not establish compatibility with every PyMySQL authentication path.

Production still needs resource/capability provisioning, restart and
replacement handling, certificate lifecycle, integration with RDS networking,
and protection against alternate backend access paths. The accepting shim
must never ship as a replacement in the current exposed-backend topology.

### Proposed roadmap boundary for maintainer discussion

Item 7 can deliver an inactive Python MySQL adapter that reuses the existing
IAM decision code, preserves backend account selection and password logins,
and has focused real-connection tests. The accepting shim must remain coupled
to an explicit trusted-network contract; this experiment is not a safe drop-in
replacement for the currently exposed plugin.

Item 8 activates that adapter only with isolated backend networking, RDS
endpoint/resource wiring, certificate provisioning, and end-to-end connection
coverage through the normal MiniStack provisioning path. Do not mark item 7
complete or replace PR #1803 until the maintainer agrees to the revised scope.
Later hardening must be tracked explicitly rather than implied by this spike.

## Phase 1: password-backed proxy and reject-all shim

Can a Python gatekeeper approve a client, then preserve the backend MySQL
username and SQL grants without adding authentication logic to a C++ plugin?

**Yes, if the proxy has a usable backend credential for that same user.**
The experiment substitutes a private password for the frontend test token,
opens the backend connection as `app`, and relays SQL. MySQL reports
`CURRENT_USER() = app@%`, permits SELECT on the granted table, and rejects
SELECT on another table and INSERT on the granted table (error 1142).

**That does not solve existing IAM account authentication.** A user declared
with `AWSAuthenticationPlugin` still fails backend authentication when the
plugin has no broker configuration, even after the gatekeeper approves the
frontend token. A proxy does not bypass the backend authentication plugin.
The test explicitly exercises that case using the stage-7 plugin artifact.

Therefore a *plugin-free* production design still needs an account-provisioning
decision: how IAM accounts map to password-backed accounts and how their
private credentials are issued, protected, rotated, and restored. SQL such as
`CREATE/ALTER USER ... IDENTIFIED WITH AWSAuthenticationPlugin` and account
inspection would also need a compatibility strategy. This spike does not
implement SQL rewriting or change any real MiniStack-managed users.

## Scope

Local ARM64 validation on 2026-09-25: **14 passed in 18.54s** across MySQL
8.0 and 8.4; Ruff passed. The fixture removed its containers and anonymous
volumes, and the final labeled-container inventory was empty.

- Real PyMySQL client, Python wire-protocol frontend, and disposable MySQL
  8.0/8.4 containers. No backend root connection is used for client sessions;
  root is used only to set up the test fixture.
- A user allowlist and an opaque token equality check stand in for the IAM
  authorizer. This is **not** SigV4, IAM-policy, resource-binding, or AWS-parity
  validation. The strict boolean models the agreed `AUTH` boundary, not actual
  application configuration wiring.
- Strict mode requires client TLS before requesting a cleartext token. The
  client verifies a temporary self-signed certificate. Permissive mode permits
  plaintext and arbitrary tokens for provisioned test users.
- Backend grants, account locking, and password authentication remain enforced.
- A changed approval token blocks new connections without terminating an
  existing session. This simulates admission expiry, not actual token expiry.
- Only QUERY, QUIT, INIT_DB, and PING are forwarded. COM_CHANGE_USER is rejected
  instead of permitting reauthentication to bypass the gate. Other commands,
  including prepared statements, are deliberately unsupported.

## Run locally (ARM64)

Requires Docker, OpenSSL, the repo's dev dependencies, `mysql:8.0`, `mysql:8.4`,
and the stage-7 artifact image `ministack-iam-stage7-build:local`. If absent,
build that image explicitly with:

```sh
docker build --file Dockerfile.full --target plugin-build --tag ministack-iam-stage7-build:local .
uv run --extra dev pytest contrib/mysql-gatekeeper-spike/test_flow.py -q
uv run --extra dev ruff check contrib/mysql-gatekeeper-spike/test_flow.py
```

The fixture binds disposable databases to random loopback ports, generates
ephemeral passwords, and removes its own containers and anonymous volumes in
`finally` blocks. No Compose stack or MiniStack server is needed. A forcibly
killed test process may need cleanup of containers bearing the
`ministack.task=gatekeeper-spike` label.

## Original plugin-free design gates (alternative, not phase 2)

1. Backend account credentials and `AWSAuthenticationPlugin` DDL compatibility.
2. Preserve normal password-login behavior without routing every user through
   the IAM stub or changing their TLS requirements.
3. Make the backend inaccessible to ordinary clients: this loopback test setup
   is not proof of a production network boundary.
4. Real verifier/resource-binding integration and fail-closed lifecycle handling.
5. Full client/protocol compatibility, endpoint routing, certificate lifecycle,
   verified backend TLS, connection limits, timeouts, and cancellation.

Protocol references: MySQL's
[handshake packet](https://dev.mysql.com/doc/dev/mysql-server/8.0.46/page_protocol_connection_phase_packets_protocol_handshake_v10.html)
and [authentication exchange](https://dev.mysql.com/doc/dev/mysql-server/8.4.11/page_protocol_connection_phase.html).
This borrows the architectural pattern of `pgproxy.py`, not its PostgreSQL
protocol handling or fixed backend identity.
