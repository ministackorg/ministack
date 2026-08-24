# DocumentDB Service — Completion Plan

## Goal

Finish the Amazon DocumentDB emulator (`ministack/services/documentdb.py`) so it imports cleanly, supports DocDB engine **v5.0** and **v8.0** backed by real MongoDB containers, covers the IaC-critical control plane, and passes tests + lint. Follows CONTRIBUTING.md conventions (service is already registered in `app.py`, `core/router.py`, conftest).

## Confirmed Decisions

| Decision | Choice |
|---|---|
| Cluster topology | **One shared mongo container per cluster** (rds.py Aurora pattern): first member starts it, other members alias its endpoint; cluster owns the volume |
| Engine versions | Exactly two: **5.0.0** (family `docdb5.0`) and **8.0.0** (family `docdb8.0`). Default stays **5.0.0** (matches AWS SDK default per AWS docs) |
| Docker images | `mongo:5.0` ↔ DocDB 5.0, `mongo:8.0` ↔ DocDB 8.0 (both wire-compatible with their Mongo majors); honor `MINISTACK_IMAGE_PREFIX` |
| Action scope | IaC-complete set (see Task 4). Explicitly deferred: global clusters, event subscriptions, `Copy*` actions, point-in-time restore |
| Warm boot | Respawn backing containers on restore under `PERSIST_STATE=1` |

## Key Facts Established

- Module is currently **broken**: `_ACTION_MAP` references `_modify_db_instance`, `_create_db_snapshot`, `_delete_db_snapshot`, `_describe_db_snapshots`; `_create_db_instance` calls `_register_instance_in_cluster`; `_delete_db_instance` calls `_unregister_instance_from_clusters` — none are defined → `NameError` at import.
- Existing infra is ready: registry entry (app.py:318), router patterns (router.py:74: targets `AmazonRDS`/`DocDB`, hosts `docdb.`/`documentdb.`), `docdb` fixture (conftest.py:464), `tests/test_docdb.py` (303 lines, expects modify/snapshot/deletion-protection behavior).
- AWS facts: default engine version 5.0.0; DocDB 8.0 = full wire compat with MongoDB 8.0 (announced Nov 2025); cluster endpoints stay stable across failover.
- State stores use `AccountScopedDict`; rds.py uses `AccountRegionScopedDict`. DocumentDB is unreleased (no persisted-state migration burden), so switch to `AccountRegionScopedDict` now.

## Tasks

### 1. Repair the module (make it import and pass current tests)

In `ministack/services/documentdb.py`:

- Implement `_modify_db_instance`: apply `DBInstanceClass`, `AllocatedStorage`, `EngineVersion`, `MasterUserPassword` (store in `_MasterUserPassword`), `DeletionProtection`, `BackupRetentionPeriod`, `PreferredMaintenanceWindow`, `MultiAZ`, `AutoMinorVersionUpgrade`, `ApplyImmediately` accepted (no pending-values staging needed — apply directly, matching current simplicity). Return ModifyDBInstanceResponse with instance XML.
- Implement `_create_db_snapshot` / `_describe_db_snapshots` / `_delete_db_snapshot` using the existing `_snapshots` store + `_snapshot_xml`. Snapshot record copies instance config (identifier, class, engine/version, storage, AZ, master user, times); errors: `DBSnapshotAlreadyExistsFault`, `DBSnapshotNotFound`, `DBInstanceNotFound`. Honor `CopyTagsToSnapshot`.
- Implement `_register_instance_in_cluster(instance)` / `_unregister_instance_from_clusters(db_id)`: maintain `DBClusterMembers` on the parent cluster dict (`DBInstanceIdentifier`, `IsClusterWriter` = true for first member, `PromotionTier`). Unregister on instance delete.
- Bug fixes while here:
  - `_delete_db_instance`: check `DeletionProtection` **before** unregistering membership (currently mutates state then errors).
  - `_instance_xml`: Endpoint Port fallback is `5432` → should be `27017`.
- Switch all stores to `AccountRegionScopedDict`; update `get_state`/`restore_state` keying accordingly.

### 2. Engine versions v5/v8

- Replace `_default_engine_version` catalog usage with a single source of truth:
  ```python
  DOCDB_ENGINE_VERSIONS = [("5.0.0", "docdb5.0"), ("8.0.0", "docdb8.0")]
  ```
- `_docker_image_for_docdb(engine_version, ...)`: map major → `mongo:5.0` / `mongo:8.0` via `apply_image_prefix`; unknown major → fall back `mongo:5.0` + warning log. Keep env vars `MONGO_INITDB_ROOT_USERNAME/PASSWORD`, port 27017, `/data/db`.
- Create-time validation in `_create_db_instance` and `_create_db_cluster`: reject unsupported `EngineVersion` with `InvalidParameterCombination` (mirror rds.py's `*_ENGINE_VERSION_SET` pattern).
- `_describe_db_engine_versions`: emit both versions with correct families; drop 4.0.0. `_describe_orderable_options`: default version param 5.0.0, accept any cataloged version.

### 3. Shared container per cluster

Model on rds.py's Aurora shared-storage design, simplified:

- Container name `ministack-docdb-cluster-{cluster_id}`, labels `{ministack: documentdb, cluster_id, account_id, region}` plus `container_reaper.own_labels("documentdb")`.
- Cluster record gains `_shared_container_id`, `_shared_endpoint`, `_shared_host_port`, `_shared_volume_name` (when `DOCDB_PERSIST=1`; tmpfs otherwise), `_shared_storage_initialized`, `_shared_container_epoch`.
- First member created on a cluster starts the container (image from cluster's EngineVersion, credentials from cluster master user/pass); its endpoint = container address/port (network mode) or host-published port. Subsequent members alias the same endpoint (no own container). Standalone instances (no cluster) keep today's per-instance container behavior.
- Member deletion: last member removed ≠ container removal unless cluster deleted or no members remain (choose: remove container when last member goes; cluster-level delete always removes).
- `StartDBCluster`/`StopDBCluster` start/stop the actual Docker container, not just flip status strings.
- `_delete_db_cluster` stops/removes the shared container.
- Update `reset()` to also stop/remove cluster-owned containers (it currently iterates instances only).
- Background port-readiness wait threads as today; log readiness.

### 4. New control-plane handlers (IaC-complete set)

All XML Query-API style, registered in `_ACTION_MAP`, shapes verified against botocore `docdb` model:

- **Cluster snapshots**: `CreateDBClusterSnapshot`, `DescribeDBClusterSnapshots`, `DeleteDBClusterSnapshot`, `ModifyDBClusterSnapshotAttribute`, `DescribeDBClusterSnapshotAttributes`. Metadata-only (record cluster config + tags; no data-plane dump). Errors: `DBClusterSnapshotAlreadyExistsFault`, `DBClusterSnapshotNotFoundFault`.
- **Cluster parameter groups**: `CreateDBClusterParameterGroup`, `DescribeDBClusterParameterGroups`, `DeleteDBClusterParameterGroup`, `ModifyDBClusterParameterGroup` (store `Parameters` list, status `in-sync`), `ResetDBClusterParameterGroup`, `DescribeDBClusterParameters` (return defaults for the family: minimal plausible parameter records).
- `FailoverDBCluster`: rotate `IsClusterWriter` to next member (lowest PromotionTier); endpoints unchanged (real DocDB behavior). Works for zero-member clusters (no-op flip).
- `RestoreDBClusterFromSnapshot`: new cluster from stored snapshot metadata (version, credentials, port, subnet group); data restore out of scope — document in docstring/README that snapshots are metadata-only.
- `ApplyPendingMaintenanceAction`: record `{resource, action, opt-in status}` in a module-level list; `DescribePendingMaintenanceActions` returns recorded items instead of hardcoded empty.
- `DescribeCertificates`: one static certificate entry (matching `CACertificateIdentifier` used elsewhere).
- `DescribeEvents`: return recorded maintenance/tag events if trivially available, else empty list shape (boto3-parseable).

### 5. Warm-boot respawn

Extend `restore_state` (pattern from rds.py, simplified):

- Instances/clusters load with `DBInstanceStatus="creating"`; group cluster members by `(account, region, cluster_id)`; spawn daemon threads that restart the cluster container (or standalone container), reuse persisted host port if free else allocate new, wait for TCP readiness, then set `available`.
- Honor `DOCDB_PERSIST` named-volume reattach (deterministic volume names: `ministack-docdb-{db_id}-data`, `ministack-docdb-cluster-{cluster_id}-data`).
- Strip non-restorable fields (`_docker_container_id` etc.) in `get_state` (already done for instances; extend to clusters).

### 6. Docstrings (PEP 8 / PEP 257)

The repo linter (`ruff`, select `E`/`F`/`I`) does not enforce docstring rules (`line-length = 120`, `E501` ignored), so these conventions are applied manually to every function written or touched by Tasks 1–5:

- **Module docstring**: replace the current `THIS IS STILL A WORK IN PROGRESS.` header with a proper PEP 257 module docstring: one-line imperative summary on the opening line (e.g. `"""Amazon DocumentDB (DocDB) service emulator."""`), blank line, then elaboration — control-plane vs real-mongo data-plane design, supported engine versions v5.0/v8.0 with their Docker images, supported action list aligned with the final `_ACTION_MAP`, env vars (`DOCDB_BASE_PORT`, `DOCDB_PERSIST`, `DOCDB_TMPFS_SIZE`, `DOCKER_NETWORK`), and reference links. Closing `"""` on its own line.
- **Public entry points** (`handle_request`, `get_state`, `restore_state`, `reset`) get complete docstrings with `Args:` / `Returns:` sections.
- **Handlers and helpers** (`_create_db_instance`, `_create_db_cluster`, snapshot/parameter-group/failover/restore handlers): one-line summary ending in a period, written in imperative mood ("Create a DB instance.", not "Creates…"); non-obvious parameters documented in an `Args:` section, response shape in `Returns:`, AWS error codes returned (`DBSnapshotAlreadyExistsFault`, `InvalidParameterCombination`, …) noted in a `Raises:`-style section where applicable.
- **Standardize the existing ad-hoc format**: `_create_db_instance` and `_create_db_cluster` currently carry free-form "Parameters and syntax" prose blocks — convert them to the same Args/Returns structure so the file is internally consistent.
- **Style rules per PEP 257**: triple double quotes everywhere; single-line docstrings keep quotes on one line; multi-line docstrings end with a blank line before the closing quotes; do not restate the signature or return statement verbatim in prose; keep docstring lines ≤ 120 chars to match repo `ruff` line-length.
- All new functions from Tasks 1–5 ship with conforming docstrings in the same commit — no separate documentation pass.

### 7. Docs & housekeeping

- README: add DocumentDB row to services table; add `DOCDB_BASE_PORT`, `DOCDB_TMPFS_SIZE`, `DOCDB_PERSIST` rows to env table; short "Real Database Endpoints" mention.
- CHANGELOG: entry under Unreleased.
- Module docstring: align supported-actions list with final `_ACTION_MAP`; note v5/v8 images and metadata-only snapshots.
- Leave `ministack/services/docdb-apis.md` and `documentdb-mongo-apis.md` in place (author's reference notes).

### 8. Tests

Update/extend `tests/test_docdb.py`:

- Fix `test_docdb_create_instance` port assertion to match final endpoint logic.
- Unit tests (no Docker): image mapping per version (5.0.0→mongo:5.0, 8.0.0→mongo:8.0), engine-version rejection (`ClientError` `InvalidParameterCombination`), cluster snapshot CRUD, snapshot attributes modify, cluster parameter group CRUD + describe/reset parameters, failover writer rotation, restore-from-snapshot, apply-pending-maintenance-action, describe-certificates/events.
- Pymongo-gated integration (existing skip pattern): cluster + 2 members share one endpoint; write via member A visible via member B; v8 instance accepts connection.

## Validation

```bash
ruff check ministack/
pytest tests/ -k docdb -v            # parallel-safe phase
pytest tests/test_docdb.py -v        # serial/global phase if needed
```

Manual smoke (Docker available): `boto3.docdb.create_db_cluster(EngineVersion="8.0.0")` + instance → connect pymongo to returned endpoint → insert/query → delete cluster → confirm container removed.

Docstring review: spot-check the module docstring and one handler per category against PEP 257 (summary line, imperative mood, Args/Returns/Raises sections).

## Risks / Notes

- Region-scoped dict switch changes persisted key layout — acceptable pre-release; ensure `restore_state` handles both old plain-dict and new keys defensively.
- Metadata-only snapshots mean `RestoreDBClusterFromSnapshot` yields an empty database — must be documented, not silent.
- `handle_request` JSON-body flattening exists for SigV4 JSON clients; boto3 `docdb` uses query protocol — no change needed.
- Out of scope (deferred): global clusters, event subscriptions, `Copy*` snapshot/parameter-group actions, point-in-time restore, in-place 5.0→8.0 upgrade API (`ModifyDBCluster` already just rewrites `EngineVersion`).
