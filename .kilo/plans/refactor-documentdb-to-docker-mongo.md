# Plan: Refactor DocumentDB service to control-plane + real Mongo Docker (mirror RDS)

## Goal
Make `ministack/services/documentdb.py` behave like the real AWS DocumentDB control plane:
- `aws docdb create-db-instance`, `create-db-cluster`, `describe-db-instances`, `describe-db-clusters`, delete, modify, tagging, engine versions, etc. work and return proper responses.
- Each "DB instance" or cluster member is backed by a **real MongoDB Docker container** (exactly like RDS backs instances with postgres/mysql containers).
- `describe-db-instances` (and equivalent) returns the real `Endpoint.Address` / `Port` (and reader endpoints for clusters) that point at the running Mongo containers.
- User applications connect **directly** to those Mongo endpoints using the MongoDB wire protocol + any Mongo driver (pymongo, etc.). MiniStack's documentdb service no longer emulates Mongo commands.
- The old in-memory Mongo command handlers (find/insert/aggregate/collMod/createIndexes/... + the entire `_state` dict of collections/users/roles) are removed.
- Full Docker lifecycle (start/stop/remove on create/delete/reset), port allocation, persistence (PERSIST_STATE), multi-account isolation via `AccountScopedDict`, and clean `reset()`.
- Compatible with the existing routing (target prefixes "AmazonRDS"/"DocDB", host patterns `docdb.` / `documentdb.`).

This enables local IaC testing (`aws docdb ...` or Terraform `aws_docdb_*` resources) without spending money or talking to real AWS, while the actual data plane is a real MongoDB.

## Why the current implementation is wrong for the stated goal
- Current `documentdb.py` (~1180 lines) is a **full MongoDB protocol emulator** over JSON/X-Amz-Target. It implements `find`, `insert`, `aggregate`, `createUser`, role commands, sessions, sharding flags, `$elemMatch`, bitwise operators, etc.
- It never implements the DocumentDB **control plane** APIs that `aws docdb` and the boto3 `docdb` client actually call (`CreateDBInstance`, `DescribeDBInstances`, `CreateDBCluster`, etc.).
- Consequently `aws docdb describe-db-instances --endpoint-url ...` currently fails or returns garbage.
- Real DocumentDB = managed Mongo-compatible DB. The interesting part for emulation is the control plane (lifecycle, endpoints, auth at the Mongo level, snapshots, etc.). Once the endpoint is returned, the customer talks real Mongo — no need to reimplement Mongo inside MiniStack.
- The user explicitly confirmed: "once the user connects to Mongo, everything will be handled in Mongo" and "should do what DocumentDB does and not what Mongo does."

## High-level design (copy RDS patterns 1:1)
- `documentdb.py` becomes a thin control-plane module.
- Use the same Docker client, port allocation, networking, tmpfs/volume persistence, background readiness threads, password rotation hooks, and container labeling as `rds.py`.
- State containers: `_instances`, `_clusters`, `_tags`, `_subnet_groups` (minimal), `_port_counter`, etc. All wrapped in `AccountScopedDict` for multi-tenancy (RDS already does this).
- `get_state()` / `restore_state()` + automatic `load_state("documentdb")` at import time (strip Docker IDs on save/restore, set status=available).
- `handle_request` supports both classic Query `Action=...` form bodies **and** SigV4 JSON bodies (copy `_flatten_json_request_params` + `_json_key_to_query_param_name` logic).
- Response format: XML in the RDS namespace (same as `rds.py`'s `_xml`, `_error`, `_instance_xml`, `_cluster_xml`, etc.). The docdb boto3 client / `aws docdb` CLI expect these shapes.
- Docker image selection: choose a real `mongo:X` image (e.g. `mongo:7` or `mongo:5` for DocDB 5.0 compat; make configurable). Enable auth. Return the connection string details in the Endpoint.
- On `CreateDBInstance` (Engine="docdb" or default): launch container, allocate host port (or use internal Docker DNS when on the MiniStack network), record `_docker_container_id`, `_internal_address`, `_MasterUserPassword`, etc.
- `Describe*` returns the live (or restored) shape with the correct Endpoint.
- `Delete*` stops/removes the container(s) and cleans tags.
- `reset()`: stops/removes **all** DocDB containers across accounts, then clears in-memory state (identical to RDS reset).
- Environment variables (new, following RDS naming):
  - `DOCDB_BASE_PORT` (default e.g. 27017 or 27117 to avoid clashing with host Mongo)
  - `DOCDB_PERSIST` (like `RDS_PERSIST`)
  - `DOCDB_TMPFS_SIZE`
  - Reuse `DOCKER_NETWORK` if set.
- Minimal cluster support: `CreateDBCluster` + `CreateDBInstance` with `DBClusterIdentifier` can create a cluster record + attach instance(s). For the emulator a "cluster" can be a single primary Mongo container (or N containers if the user creates multiple instances). This is sufficient for most IaC and driver connection tests. Real replica-set initialization is a future stretch goal.
- Tagging, ListTagsForResource/Add/RemoveTags, engine version listing (`docdb` engine), and basic subnet group support (can be minimal "default" like RDS).
- No need to implement the full 100+ Mongo operator surface or the old `_find`/`_doc_matches`/`_run_pipeline` etc.

## Detailed implementation steps (in order)

1. **Backup / archive old behavior (optional but recommended)**
   - Rename or git-move the current `documentdb.py` content to `documentdb-legacy-mongo-emulator.py` (or just delete the Mongo handlers after the refactor). Update `documentdb.md` / `documentdb-apis.md` with a deprecation note.
   - The user can resurrect the pure-emulator later under a flag if truly needed.

2. **Restructure documentdb.py**
   - Keep the module docstring but rewrite it to describe the new purpose (control plane + Docker Mongo, list the supported `aws docdb` / RDS-style actions).
   - Imports: copy the RDS block (copy, datetime, json, logging, os, socket, threading, time, parse_qs, escape, docker, AccountScopedDict, get_account_id, get_region, new_uuid, load_state).
   - Constants: `REGION`, `BASE_PORT = int(os.environ.get("DOCDB_BASE_PORT", "27117"))`, `DOCDB_PERSIST`, `DOCDB_TMPFS_SIZE`, `DOCKER_NETWORK`.
   - Module-level state (all AccountScopedDict except the counter):
     - `_instances`, `_clusters`, `_subnet_groups`, `_tags`, `_port_counter = [BASE_PORT]`
     - `_docker = None`, `_ministack_network = None` (same lazy helpers as RDS).
   - Copy/adapt the helper functions:
     - `_get_docker`, `_get_ministack_network`, `_wait_for_port`, `_next_port` (with its lock).
     - `_json_key_to_query_param_name`, `_flatten_json_request_params`.
   - `get_state()` / `restore_state()` — identical structure to RDS but only the DocDB-relevant keys. On restore, set container IDs to None and status=available.
   - Automatic restore at import time (wrap in try, like RDS).
   - `handle_request` — copy the RDS version (parse query or JSON body, resolve Action or X-Amz-Target, dispatch via a map). Target can be "AmazonRDS.CreateDBInstance" or "DocDB...."; the action name is what matters.
   - Implement the action map for the commands listed in `docdb-help.md` (start with the high-value ones):
     - CreateDBInstance, DeleteDBInstance, DescribeDBInstances, ModifyDBInstance (basic fields)
     - CreateDBCluster, DeleteDBCluster, DescribeDBClusters, ModifyDBCluster
     - Start/Stop/Reboot*Instance / *Cluster (stubs that just flip status)
     - CreateDBSubnetGroup / DescribeDBSubnetGroups (minimal)
     - CreateDBSnapshot / DescribeDBSnapshots / Delete (for instances; cluster snapshots later)
     - ListTagsForResource, AddTagsToResource, RemoveTagsFromResource
     - DescribeDBEngineVersions (return at least one "docdb" entry, versions 5.0 etc.)
     - DescribeOrderableDBInstanceOptions (can return a small static list)
   - For each create:
     - Parse identifiers, engine (force/accept "docdb"), master user/pass, port (default 27017 inside container), allocated storage (ignored or faked), etc.
     - If Docker available: pick image via a new `_docker_image_for_docdb(engine_version, user, password, db_name?)`, run container with proper env (`MONGO_INITDB_ROOT_USERNAME`, `MONGO_INITDB_ROOT_PASSWORD`), port mapping or network, tmpfs/volume, labels `{"ministack": "documentdb", "db_id": ...}`.
     - Record the same extra keys RDS uses (`_docker_container_id`, `_internal_address`, `_internal_port`, `_MasterUserPassword`).
     - For clusters: create cluster record + (optionally) auto-create a writer instance, or just let the user call CreateDBInstance with DBClusterIdentifier.
     - Return the proper XML shape (copy RDS `_single_instance_response` pattern or build `<CreateDBInstanceResponse>` etc.).
   - Describe functions: filter by identifier, apply any simple filters, return XML list.
   - Delete: stop/remove container(s), unregister from cluster members if any, delete tags, return the "deleting" shape.
   - Modify: support a few fields (instance class, backup retention, deletion protection, master password rotation — for password, you can implement a Mongo-side `db.changeUserPassword` or just update the recorded secret; real rotation against the container is a nice-to-have later).
   - Tagging helpers (copy `_parse_tags`, `_list_tags`, `_add_tags`, `_remove_tags`).
   - XML builders: copy the small `_xml`, `_error`, `_instance_xml`, `_cluster_xml`, `_esc` helpers (or factor a tiny shared module later; duplication is acceptable to start).
   - Engine version helper that returns a docdb entry.
   - `reset()`: obtain docker client, for every instance/cluster member stop+remove its container (best-effort), clear all AccountScopedDicts and reset port counter.

3. **Docker image + container details for Mongo**
   - New function `_docker_image_for_docdb(engine_version, user, password, db_name="")`.
   - Return (image, env_dict, container_port, data_path).
   - Example:
     - image = `apply_image_prefix(f"mongo:{major}-focal")` or a fixed recent `mongo:7`.
     - env = {"MONGO_INITDB_ROOT_USERNAME": user, "MONGO_INITDB_ROOT_PASSWORD": password}
     - If db_name desired, the init script can create it, or rely on the first connection.
     - container_port = 27017
     - data_path = "/data/db"
   - Same tmpfs vs named-volume logic as RDS when `DOCDB_PERSIST=1`.
   - Same background `_bg_wait` thread that logs when the port is ready (use pymongo or raw TCP check).
   - On the MiniStack Docker network: prefer internal IP like RDS does.
   - Expose the endpoint the same way: `Endpoint: {Address, Port, HostedZoneId: "Z2R2ITUGPM61AM"}` (the fake zone id is fine; drivers don't care).

4. **Routing & service registration (verify / minimal changes)**
   - Already registered in `app.py` SERVICE_REGISTRY and in `router.py` SERVICE_PATTERNS (target "AmazonRDS"/"DocDB", hosts `docdb.`, `documentdb.`).
   - Because "AmazonRDS" target prefix is also used by real RDS, the order in the dict matters. "documentdb" appears before "rds" today — this is probably intentional for DocDB calls.
   - In practice many `aws docdb` CLIs call the rds service model. When users do `boto3.client("docdb", endpoint_url=...)` or `aws docdb ... --endpoint-url`, the calls will carry the right credential scope or host, or X-Amz-Target "DocDB.*", so they should hit the documentdb handler.
   - If conflicts arise (e.g. a plain "AmazonRDS.CreateDBInstance" with Engine=docdb), the plan can later add a small heuristic in the rds module or here: if Engine=="docdb" or certain params, the rds handler can forward or error with guidance. For v1 keep the current routing and test both clients.
   - Update the big BANNER string in `app.py` to mention "DocumentDB" (and keep "RDS").

5. **Persistence integration**
   - Use the existing `load_state("documentdb")` + the `_state_map` entry that already exists (`"documentdb"` is not listed yet in the current `_state_map` in app.py — add it).
   - In `app.py` `_state_map` add `"documentdb": "documentdb",`.
   - `save_all` will pick up a `get_state` if the module provides it (the lazy load + shutdown path already calls it for registered services).
   - Follow RDS exactly for stripping container IDs.

6. **Tests**
   - Create or expand `tests/test_docdb.py` (modeled directly on `tests/test_rds.py`).
   - Basic happy-path tests (no Docker required for shape tests; Docker-required tests guarded like RDS ones):
     - `test_docdb_create_instance`, `describe`, `delete`.
     - Cluster create + describe.
     - Tagging round-trip.
     - Password in describe (never returned) and rotation stub.
     - Engine versions contain "docdb".
   - Add a Docker smoke test that actually connects with `pymongo` to the returned endpoint and does a trivial insert/find (skip if no docker or pymongo not installed — same pattern as RDS tests that use psycopg2/pymysql).
   - Update `tests/conftest.py` if needed (the `docdb` fixture already exists and returns a boto3 "docdb" client — perfect).
   - Run the full test suite + lint/typecheck after changes.

7. **Error handling & parity**
   - Re-use the same error codes and XML error envelope that RDS uses (`DBInstanceNotFound`, `DBInstanceAlreadyExistsFault`, `InvalidParameterCombination` for deletion protection, etc.).
   - DeletionProtection, MasterUserPassword updates, status transitions ("available", "deleting", "stopped").
   - Idempotency notes for drop (some are, some aren't — match real behavior where easy).

8. **Documentation & DX**
   - Rewrite the top docstring of `documentdb.py`.
   - Update `ministack/services/documentdb.md` and `documentdb-apis.md` (or add a new control-plane section) to explain that data-plane is real Mongo and list the supported control-plane actions.
   - Mention required Docker and the new env vars in README (or a small DocDB section).
   - Add a one-line note in CHANGELOG under "Unreleased".

9. **Optional follow-ups (not required for first cut)**
   - Real replica-set / sharded cluster initialization inside the containers.
   - DocDB-specific parameter groups.
   - Snapshots that actually export/import data (volume snapshot or mongodump inside container).
   - Global clusters, restore from snapshot, read replicas.
   - Share more code with rds.py (extract `_docker_image_for_*`, XML helpers, tag helpers into `ministack/services/rds_common.py` or similar).
   - Support for the legacy pure-Mongo JSON commands behind an env flag (probably not worth it).

## Trade-offs & questions for the user (answer before or during implementation)
- Mongo image: pin to a specific `mongo:7.x` or `mongo:5.0`? Or let the user override via env? (Recommendation: start with `mongo:7` and document how to change.)
- Port range: `DOCDB_BASE_PORT=27117` (or 27017)? Should it be completely independent of RDS ports?
- Cluster fidelity: for `create-db-cluster` do we automatically create 1 writer instance container, or require the caller to also call create-db-instance? (RDS does both patterns.)
- Password rotation: implement real `db.changeUserPassword` / root user alteration against the running mongo container (like RDS does for postgres/mysql)? Nice for realism but adds a pymongo dependency in the control plane.
- Response protocol: confirm we must emit RDS-style XML (not JSON). (Current evidence: RDS module does XML and docdb client inherits the shapes.)
- Backward compat: are there any existing users/tests that rely on the old in-memory Mongo command surface being served from the documentdb endpoint? If yes, we need a transition period or a separate "mongo-emulator" service.
- Should DocumentDB share the RDS `_port_lock` / counter infrastructure, or have its own? (Separate is cleaner.)
- Tagging / subnet / param groups: implement the full surface immediately or stub the describe/create and expand on demand? (Start minimal, like the first RDS PRs.)

## Success criteria / verification
- `aws docdb create-db-instance --db-instance-identifier test --engine docdb --db-instance-class db.t3.medium --master-username root --master-user-password secret --endpoint-url http://localhost:4566` succeeds and a Docker container appears (`docker ps` shows a mongo one labeled ministack=documentdb).
- `aws docdb describe-db-instances` returns the instance with a reachable `Endpoint.Address` + `Port`.
- A Python snippet using `pymongo.MongoClient(host, port, username=..., password=...)` can insert and query a document.
- `aws docdb delete-db-instance ...` removes the container.
- `/ _ministack/reset` stops and removes all DocDB containers.
- With `PERSIST_STATE=1` + restart, instances re-appear in describe (without stale container IDs) and containers are **not** auto-started on restore (status becomes available; user can start if we implement stop/start).
- Existing RDS behavior is completely unaffected.
- `pytest tests/test_docdb.py -q` (and any Docker-gated tests) pass; full `pytest` + lint still green.
- `aws docdb help` style commands listed in the provided `docdb-help.md` at least have their core paths implemented.

## File changes expected (high level)
- `ministack/services/documentdb.py` — major rewrite (new file content, ~same length as a slimmed RDS or smaller).
- `ministack/app.py` — one-line addition to `_state_map` for persistence + banner text.
- `tests/test_docdb.py` — new file (or heavy addition).
- Possibly small updates to `tests/conftest.py`, `README.md`, `CHANGELOG.md`, `ministack/services/documentdb*.md`.
- No changes needed to router.py or the core detection (already prepared for documentdb).

## Out of scope for this plan
- Implementing every single Mongo operator that DocDB supports (the point is to stop doing that).
- Full DocDB snapshot/restore data movement.
- Vector search, elastic clusters, or other 2025+ DocDB features.
- Making the old in-memory implementation coexist unless explicitly requested.

## Estimated effort
- Core control-plane + Docker launch + describe/delete + basic tests: 1–2 focused sessions.
- Polish (tagging, engine versions, clusters, persistence, password handling, docs): additional half session.
- Full test run + review: included.

This plan is self-contained. Once the user confirms (especially image choice, port default, cluster creation semantics, and whether any legacy Mongo command surface must be preserved), implementation can begin by writing the new documentdb.py following the RDS skeleton extremely closely.
