"""
RDS Service Emulator.
Query API (Action=...) for control plane + optional Docker-based real Postgres/MySQL.
Supports: CreateDBInstance, DeleteDBInstance, DescribeDBInstances, ModifyDBInstance,
          StartDBInstance, StopDBInstance, RebootDBInstance,
          CreateDBCluster, DeleteDBCluster, DescribeDBClusters, ModifyDBCluster,
          StartDBCluster, StopDBCluster,
          CreateDBSubnetGroup, DeleteDBSubnetGroup, DescribeDBSubnetGroups, ModifyDBSubnetGroup,
          CreateDBParameterGroup, DeleteDBParameterGroup, DescribeDBParameterGroups,
          DescribeDBParameters, ModifyDBParameterGroup, ResetDBParameterGroup,
          CreateDBClusterParameterGroup, DescribeDBClusterParameterGroups,
          DeleteDBClusterParameterGroup, DescribeDBClusterParameters,
          ModifyDBClusterParameterGroup, ResetDBClusterParameterGroup,
          CreateDBSnapshot, DeleteDBSnapshot, DescribeDBSnapshots,
          CreateDBClusterSnapshot, DescribeDBClusterSnapshots, DeleteDBClusterSnapshot,
          CreateOptionGroup, DeleteOptionGroup, DescribeOptionGroups, DescribeOptionGroupOptions,
          CreateDBInstanceReadReplica (stub), RestoreDBInstanceFromDBSnapshot (stub),
          ListTagsForResource, AddTagsToResource, RemoveTagsFromResource,
          DescribeDBEngineVersions, DescribeOrderableDBInstanceOptions,
          DescribePendingMaintenanceActions,
          CreateGlobalCluster, DescribeGlobalClusters, DeleteGlobalCluster,
          RemoveFromGlobalCluster, ModifyGlobalCluster,
          SwitchoverGlobalCluster, FailoverGlobalCluster,
          EnableHttpEndpoint, DisableHttpEndpoint,
          CreateDBProxy, DescribeDBProxies, ModifyDBProxy, DeleteDBProxy,
          CreateDBProxyEndpoint, DescribeDBProxyEndpoints,
          ModifyDBProxyEndpoint, DeleteDBProxyEndpoint,
          DescribeDBProxyTargetGroups, ModifyDBProxyTargetGroup,
          RegisterDBProxyTargets, DeregisterDBProxyTargets,
          DescribeDBProxyTargets.

When Docker is available, CreateDBInstance spins up a real Postgres/MySQL container
and returns the actual host:port as the endpoint.

JSON request bodies (``application/x-amz-json-1.*``, SigV4 JSON) are accepted for the
same actions as the legacy Query API form body, so Terraform / current botocore
clients can call DescribeDBInstances and other operations without ``Action=`` query
parameters.
"""

import contextvars
import copy
import datetime
import hashlib
import json
import logging
import os
import re
import secrets as stdlib_secrets
import socket
import threading
import time
from urllib.parse import parse_qs
from xml.sax.saxutils import escape as _esc

from ministack.core import container_reaper
from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import resource_lock, run_offloop, spawn_background
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    apply_image_prefix,
    get_account_id,
    get_region,
    new_uuid,
)
from ministack.services import secretsmanager
from ministack.services.rds_iam_plugin import (
    ensure_iam_auth_plugin,
    iam_auth_plugin_enabled,
)
from ministack.services.rds_mysql_compat import (
    ensure_rds_compatibility_procedures,
)

logger = logging.getLogger("rds")

# Cap any single Docker daemon call. docker-py defaults to 60s, which turns a
# slow or wedged daemon into a minutes-long stall on a request path.
_DOCKER_TIMEOUT = float(os.environ.get("MINISTACK_DOCKER_TIMEOUT", "10"))


REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")
BASE_PORT = int(os.environ.get("RDS_BASE_PORT", "15432"))
RDS_TMPFS_SIZE = os.environ.get("RDS_TMPFS_SIZE", "256m")
RDS_PERSIST = os.environ.get("RDS_PERSIST", "0").lower() in ("1", "true", "yes")
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")
# When set, skip ministack's own Docker network auto-detect so DescribeDBInstances
# returns {MINISTACK_HOST, host_port} — the address that's actually reachable
# from outside the Docker network (remote ministack deployments, host-side
# clients of a containerised ministack). Off by default: existing in-network
# behavior unchanged.
RDS_PUBLIC_ENDPOINT = os.environ.get("MINISTACK_RDS_PUBLIC_ENDPOINT", "0").lower() in ("1", "true", "yes")
# Opt-in: per-instance Aurora PostgreSQL reader containers backed by real
# streaming replication (#1325). Scoped to aurora-postgresql by name and by
# gate: Aurora MySQL readers keep aliasing the single shared container, and
# the physical-replication bootstrap below is PostgreSQL-specific
# (pg_basebackup + hot standby). Off by default: existing behavior unchanged.
RDS_PG_CLUSTER_REPLICATION = os.environ.get(
    "MINISTACK_RDS_PG_CLUSTER_REPLICATION", "0",
).lower() in ("1", "true", "yes")

_instances = AccountRegionScopedDict()
_clusters = AccountRegionScopedDict()
_subnet_groups = AccountRegionScopedDict()
_param_groups = AccountRegionScopedDict()
_snapshots = AccountRegionScopedDict()
_db_cluster_param_groups = AccountRegionScopedDict()
_db_cluster_snapshots = AccountRegionScopedDict()
_option_groups = AccountRegionScopedDict()
_db_proxies = AccountRegionScopedDict()
_db_proxy_endpoints = AccountRegionScopedDict()
_global_clusters = AccountScopedDict()
_tags = AccountScopedDict()
_port_counter = [BASE_PORT]

_docker = None
_ministack_network = None
_shared_container_lock = threading.RLock()

_MYSQL_REPLICATION_USER = "rdsrepladmin"
_MYSQL_REPLICATION_PASSWORD = "ministack-rds-replication"
_MYSQL_CONTROL_USER = "rdsadmin"
_MYSQL_CONTROL_PASSWORD = "ministack-rds-control"
_MYSQL_REPLICATION_RETRY_ATTEMPTS = 60
_MYSQL_REPLICATION_RETRY_INTERVAL = 1
# Internal binlog retention for the local MySQL container, long enough for
# replicas to catch up. Not an AWS-facing knob: Aurora exposes retention per
# cluster through the mysql.rds_set_configuration stored procedure (hours),
# not an environment variable.
_MYSQL_BINLOG_RETENTION_SECONDS = 604800  # 7 days

# Streaming-replication credentials for per-instance Aurora PostgreSQL reader
# containers (#1325). ``rdsrepladmin`` mirrors the reserved role name real RDS
# PostgreSQL provisions for replication. Fixed local-only credentials, same
# convention as the MySQL replication user above.
_PG_REPLICATION_USER = "rdsrepladmin"
_PG_REPLICATION_PASSWORD = "rdsrepladmin-local-password"

# Runs on the writer container (docker exec). Idempotently creates the
# replication role and opens pg_hba.conf for remote replication connections:
# the stock postgres image only allows replication from localhost, so
# without the extra rule a reader's pg_basebackup is rejected. The role is
# created with an explicit scram-sha-256 verifier so the pg_hba method below
# authenticates it on every supported major (password_encryption defaulted
# to md5 before PostgreSQL 14).
_PG_REPLICATION_SOURCE_SCRIPT = f"""set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
SET password_encryption = 'scram-sha-256';
DO $do$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{_PG_REPLICATION_USER}') THEN
        CREATE ROLE {_PG_REPLICATION_USER} WITH REPLICATION LOGIN
            PASSWORD '{_PG_REPLICATION_PASSWORD}';
    END IF;
END
$do$;
SQL
if ! grep -q "host replication {_PG_REPLICATION_USER}" "$PGDATA/pg_hba.conf"; then
    printf 'host replication {_PG_REPLICATION_USER} all scram-sha-256\\n' \\
        >> "$PGDATA/pg_hba.conf"
fi
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \\
    -c "SELECT pg_reload_conf()" >/dev/null
"""

# Container command for a reader: bypass the image entrypoint (which would
# initdb a brand-new independent database) and instead clone the writer with
# pg_basebackup, then start PostgreSQL as a hot standby. The base backup is
# retried until the writer is ready and the replication role exists, so
# reader creation may safely overlap writer creation. --write-recovery-conf
# records the primary connection (including the password, because it is
# passed inside --dbname) so the standby streams WAL from the writer.
_PG_READER_BOOTSTRAP_SCRIPT = """set -eu
until pg_isready -h "$MINISTACK_PG_PRIMARY_HOST" \\
        -p "$MINISTACK_PG_PRIMARY_PORT" >/dev/null 2>&1; do
    sleep 1
done
mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"
chmod 700 "$PGDATA"
until gosu postgres pg_basebackup \\
        --dbname="host=$MINISTACK_PG_PRIMARY_HOST port=$MINISTACK_PG_PRIMARY_PORT \\
user=$MINISTACK_PG_REPLICATION_USER password=$MINISTACK_PG_REPLICATION_PASSWORD" \\
        --pgdata="$PGDATA" --write-recovery-conf --wal-method=stream \\
        --checkpoint=fast; do
    find "$PGDATA" -mindepth 1 -delete
    sleep 1
done
exec gosu postgres postgres
"""

# Aurora MySQL versions are the creatable set returned by AWS RDS as of
# 2026-07-09. Refresh with:
#   aws rds describe-db-engine-versions --engine aurora-mysql \
#     --query 'DBEngineVersions[].[EngineVersion,DBParameterGroupFamily]' \
#     --output text | sort -V
# Keep the Docker image mapping below aligned to the community MySQL major.minor:
# 5.7 -> mysql:5.7, 8.0 -> mysql:8.0, 8.4 -> mysql:8.4. AWS's default can trail
# the latest advertised version, so update _default_engine_version deliberately.
AURORA_MYSQL_ENGINE_VERSIONS = [
    ("5.7.mysql_aurora.2.11.1", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.11.2", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.11.3", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.11.4", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.11.5", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.11.6", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.12.0", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.12.1", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.12.2", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.12.3", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.12.4", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.12.5", "aurora-mysql5.7"),
    ("5.7.mysql_aurora.2.12.6", "aurora-mysql5.7"),
    ("8.0.mysql_aurora.3.04.0", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.04.1", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.04.2", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.04.3", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.04.4", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.04.6", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.08.0", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.08.1", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.08.2", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.09.0", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.10.0", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.10.1", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.10.2", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.10.3", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.10.4", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.11.1", "aurora-mysql8.0"),
    ("8.0.mysql_aurora.3.12.0", "aurora-mysql8.0"),
    ("8.4.mysql_aurora.8.4.7", "aurora-mysql8.4"),
]
AURORA_MYSQL_ENGINE_VERSION_SET = {version for version, _ in AURORA_MYSQL_ENGINE_VERSIONS}

# Aurora PostgreSQL versions are the creatable set returned by AWS RDS as of
# 2026-08-06 (single source of truth for both the catalog and create-time
# validation). Refresh with:
#   aws rds describe-db-engine-versions --engine aurora-postgresql \
#     --query 'DBEngineVersions[].[EngineVersion,DBParameterGroupFamily]' \
#     --output text | sort -V
# The Docker image is derived from the major alone (16.14 -> postgres:16-alpine),
# so new minors need no image work — but keep _default_engine_version aligned:
# AWS's default (describe-db-engine-versions --default-only) can trail the
# latest advertised version, so update it deliberately when refreshing.
AURORA_POSTGRESQL_ENGINE_VERSIONS = [
    ("11.9", "aurora-postgresql11"),
    ("11.21", "aurora-postgresql11"),
    ("12.9", "aurora-postgresql12"),
    ("12.22", "aurora-postgresql12"),
    ("13.9", "aurora-postgresql13"),
    ("13.23", "aurora-postgresql13"),
    ("14.6", "aurora-postgresql14"),
    ("14.17", "aurora-postgresql14"),
    ("14.18", "aurora-postgresql14"),
    ("14.19", "aurora-postgresql14"),
    ("14.20", "aurora-postgresql14"),
    ("14.22", "aurora-postgresql14"),
    ("14.23", "aurora-postgresql14"),
    ("15.10", "aurora-postgresql15"),
    ("15.12", "aurora-postgresql15"),
    ("15.13", "aurora-postgresql15"),
    ("15.14", "aurora-postgresql15"),
    ("15.15", "aurora-postgresql15"),
    ("15.17", "aurora-postgresql15"),
    ("15.18", "aurora-postgresql15"),
    ("16.4-limitless", "aurora-postgresql16"),
    ("16.6-limitless", "aurora-postgresql16"),
    ("16.8", "aurora-postgresql16"),
    ("16.8-limitless", "aurora-postgresql16"),
    ("16.9", "aurora-postgresql16"),
    ("16.9-limitless", "aurora-postgresql16"),
    ("16.10", "aurora-postgresql16"),
    ("16.10-limitless", "aurora-postgresql16"),
    ("16.11", "aurora-postgresql16"),
    ("16.11-limitless", "aurora-postgresql16"),
    ("16.13", "aurora-postgresql16"),
    ("16.13-limitless", "aurora-postgresql16"),
    ("16.14", "aurora-postgresql16"),
    ("17.4", "aurora-postgresql17"),
    ("17.5", "aurora-postgresql17"),
    ("17.6", "aurora-postgresql17"),
    ("17.7", "aurora-postgresql17"),
    ("17.9", "aurora-postgresql17"),
    ("17.10", "aurora-postgresql17"),
    ("18.3", "aurora-postgresql18"),
    ("18.4", "aurora-postgresql18"),
]
AURORA_POSTGRESQL_ENGINE_VERSION_SET = {
    version for version, _ in AURORA_POSTGRESQL_ENGINE_VERSIONS
}
# Defaults deliberately trail the newest advertised minor when AWS does. Keep
# this explicit instead of deriving it from the catalog ordering. Refresh with:
#   aws rds describe-db-engine-versions --engine aurora-postgresql \
#     --engine-version <major> --default-only
AURORA_POSTGRESQL_DEFAULT_ENGINE_VERSIONS = {
    "16": "16.11",
    "17": "17.7",
}

AURORA_MYSQL_IMAGE_MAP = {
    "5.6": "mysql:5.6",
    "5.7": "mysql:5.7",
    "8.0": "mysql:8.0",
    "8.4": "mysql:8.4",
}
DEFAULT_AURORA_MYSQL_IMAGE = "mysql:8.4"


# ── Persistence ────────────────────────────────────────────

def get_state():
    with _shared_container_lock:
        instances = copy.deepcopy(_instances)
        clusters = copy.deepcopy(_clusters)
        state = {
            "instances": instances,
            "clusters": clusters,
            "subnet_groups": copy.deepcopy(_subnet_groups),
            "param_groups": copy.deepcopy(_param_groups),
            "snapshots": copy.deepcopy(_snapshots),
            "db_cluster_param_groups": copy.deepcopy(_db_cluster_param_groups),
            "db_cluster_snapshots": copy.deepcopy(_db_cluster_snapshots),
            "option_groups": copy.deepcopy(_option_groups),
            "db_proxies": copy.deepcopy(_db_proxies),
            "db_proxy_endpoints": copy.deepcopy(_db_proxy_endpoints),
            "global_clusters": copy.deepcopy(_global_clusters),
            "tags": copy.deepcopy(_tags),
            "port_counter": _port_counter[0],
        }
    # Strip Docker container IDs (not restorable across restarts)
    for key in list(instances._data):
        instances._data[key].pop("_docker_container_id", None)
    for key in list(clusters._data):
        cluster = clusters._data[key]
        cluster.setdefault(
            "_shared_storage_initialized",
            bool(
                cluster.get("_shared_container_id")
                and cluster.get("_shared_container_ready")
            ),
        )
        cluster.pop("_shared_container_id", None)
    return state


def restore_state(data):
    if not data:
        return
    _clusters.update(data.get("clusters", {}))
    for key in list(_clusters._data):
        cluster = _clusters._data[key]
        cluster["_shared_container_id"] = None
        cluster.pop("_mysql_replication_retry_marker", None)
        # Re-verify replication provisioning against the respawned writer
        # (#1325): the role and pg_hba rule live in the cluster volume and
        # normally survive, but the flag must not outlive a volume that
        # did not.
        cluster.pop("_pg_replication_source_ready", None)
        if cluster.get("_mysql_replication_source_arn"):
            # A respawned MySQL container gets a new hostname, while the
            # persisted replica repository and relay-log names belong to the
            # previous container. Recover the channel from the durable GTID
            # execution set before relinking it to the restored source.
            cluster["_mysql_replication_reset_pending"] = True
        if cluster.get("_shared_endpoint"):
            cluster["_shared_container_ready"] = False
    _subnet_groups.update(data.get("subnet_groups", {}))
    _param_groups.update(data.get("param_groups", {}))
    _snapshots.update(data.get("snapshots", {}))
    _db_cluster_param_groups.update(data.get("db_cluster_param_groups", {}))
    _db_cluster_snapshots.update(data.get("db_cluster_snapshots", {}))
    _option_groups.update(data.get("option_groups", {}))
    _db_proxies.update(data.get("db_proxies", {}))
    _db_proxy_endpoints.update(data.get("db_proxy_endpoints", {}))
    _global_clusters.update(data.get("global_clusters", {}))
    # Persistence contains every account, while AccountScopedDict.values()
    # intentionally exposes only the active request account. Reconcile the
    # raw persisted records so a warm boot cannot leave secondary metadata
    # stale merely because another account happened to be active at startup.
    for global_cluster in _global_clusters._data.values():
        writer = _global_cluster_writer_cluster(global_cluster)
        if writer and _aurora_mysql_8_replication_enabled(writer):
            _sync_global_mysql_credentials(writer, global_cluster)
    _tags.update(data.get("tags", {}))
    if "port_counter" in data:
        _port_counter[0] = data["port_counter"]
    instances_data = data.get("instances", {})
    to_respawn = []
    if isinstance(instances_data, AccountRegionScopedDict):
        for key, inst in list(instances_data._data.items()):
            account_id, region, instance_id = key
            inst["_docker_container_id"] = None
            inst["DBInstanceStatus"] = "creating"
            if RDS_PERSIST:
                inst.setdefault(
                    "_docker_volume_name",
                    _legacy_scoped_rds_docker_volume_name(
                        instance_id, account_id, region,
                    ),
                )
            _instances._data[key] = inst
            to_respawn.append((account_id, region, instance_id, inst))
    elif isinstance(instances_data, AccountScopedDict):
        # Legacy account-scoped format: preserve the instance ARN region when available.
        for key, inst in list(instances_data._data.items()):
            account_id, instance_id = key
            inst["_docker_container_id"] = None
            inst["DBInstanceStatus"] = "creating"
            region = _best_effort_region_from_record_arn(inst, "DBInstanceArn")
            if RDS_PERSIST:
                inst.setdefault("_docker_volume_name", _legacy_rds_docker_volume_name(instance_id))
                inst["_legacy_docker_container_name"] = _legacy_rds_docker_name(instance_id)
            _instances._data[(account_id, region, instance_id)] = inst
            to_respawn.append((account_id, region, inst.get("DBInstanceIdentifier") or instance_id, inst))
    else:
        # Legacy format: plain dict keyed by instance name
        for name, inst in instances_data.items():
            inst["_docker_container_id"] = None
            inst["DBInstanceStatus"] = "creating"
            region = _best_effort_region_from_record_arn(inst, "DBInstanceArn")
            if RDS_PERSIST:
                inst.setdefault("_docker_volume_name", _legacy_rds_docker_volume_name(name))
                inst["_legacy_docker_container_name"] = _legacy_rds_docker_name(name)
            _instances.set_scoped(get_account_id(), region, name, inst)
            to_respawn.append((None, region, name, inst))

    # Re-spin backing containers for persisted instances. Mirrors the MWAA
    # restore pattern: persistence saves the instance metadata but the Docker
    # container itself is killed by the host restart, so the restore path has
    # to bring it back. Without this, restored instances stay marked
    # "available" with no running container, and StartDBInstance is
    # metadata-only so it can't recover them either.
    from ministack.core.responses import _request_account_id, _request_region

    shared_groups = {}
    standalone = []
    for account_id, region, db_id, inst in to_respawn:
        # `_shared_cluster_id` is new with the shared-storage model. Fall back
        # to the public cluster identifier so state saved by earlier releases
        # migrates to one cluster-owned container on its first warm boot.
        cluster_id = inst.get("_shared_cluster_id") or inst.get("DBClusterIdentifier")
        if cluster_id:
            shared_groups.setdefault((account_id, region, cluster_id), []).append(inst)
        else:
            standalone.append((account_id, region, db_id, inst))

    # A global secondary keeps applying source GTIDs after its final DB
    # instance is deleted.  There is no instance record to pull an empty
    # secondary into ``shared_groups`` on warm boot, so retain the explicit
    # headless-applier intent as its own restore work item.
    for key, cluster in list(_clusters._data.items()):
        account_id, region, cluster_id = key
        if cluster.get("_mysql_headless_applier_required"):
            shared_groups.setdefault((account_id, region, cluster_id), [])

    # A pre-namespace standalone named ``cluster-<id>`` used the same Docker
    # name now reserved for cluster ``<id>``. Remove those legacy containers
    # before any asynchronous cluster runner can claim the name, and mark the
    # standalone runner so it does not later mistake the new cluster container
    # for its own stale resource. Persistent data remains in the standalone's
    # recorded volume and is mounted under its new ``instance-`` name.
    shared_container_owners = {
        _rds_cluster_docker_name(cluster_id, account_id, region): cluster_id
        for account_id, region, cluster_id in shared_groups
    }
    docker_client = _get_docker()
    if docker_client:
        for account_id, region, db_id, inst in standalone:
            legacy_name = _legacy_scoped_rds_docker_name(
                db_id, account_id, region,
            )
            cluster_id = shared_container_owners.get(legacy_name)
            if not cluster_id:
                continue
            try:
                legacy_container = docker_client.containers.get(legacy_name)
            except Exception:
                legacy_container = None
            if legacy_container and _rds_container_is_owned_by(
                legacy_container,
                expected_db_ids={db_id},
                expected_cluster_ids={cluster_id},
                account_id=account_id,
                region=region,
            ):
                try:
                    legacy_container.remove(force=True, v=False)
                except Exception:
                    pass
            inst["_legacy_scoped_container_migrated"] = True

    for account_id, region, cluster_id in shared_groups:
        members = shared_groups[(account_id, region, cluster_id)]
        cluster = _clusters.get_scoped(
            account_id or get_account_id(), region, cluster_id,
        )
        if cluster and members:
            # Publish the restore/migration gate before the daemon starts. A
            # create arriving immediately after restore_state() must not start
            # fresh cluster storage before the writer volume is adopted.
            cluster["_shared_legacy_migration_in_progress"] = True
            cluster.pop("_shared_legacy_migration_blocked", None)
        ctx = contextvars.copy_context()

        def _cluster_runner(
            account_id=account_id,
            region=region,
            cluster_id=cluster_id,
            members=members,
        ):
            if account_id is not None:
                _request_account_id.set(account_id)
            if region is not None:
                _request_region.set(region)
            cluster = _clusters.get(cluster_id)
            if not cluster:
                for member in members:
                    member["DBInstanceStatus"] = "failed"
                return
            if cluster.get("Status") == "stopped":
                # A stopped cluster stays stopped across host restarts;
                # StartDBCluster brings compute back. (Real AWS also
                # auto-starts a cluster stopped for seven days — out of
                # scope for an emulator.)
                cluster.pop("_shared_legacy_migration_in_progress", None)
                cluster["_shared_container_ready"] = False
                for member in members:
                    member["DBInstanceStatus"] = "stopped"
                return
            restore_epoch = int(cluster.get("_shared_container_epoch", 0))

            # State from before cluster-owned storage has one container and
            # volume per member. Preserve the writer's volume as the
            # authoritative cluster volume, remove the superseded member
            # containers without deleting their volumes, and reap only the
            # non-writer volumes after the shared container starts.
            member_by_id = {
                member.get("DBInstanceIdentifier"): member
                for member in members
            }
            writer_id = next(
                (
                    item.get("DBInstanceIdentifier")
                    for item in cluster.get("DBClusterMembers", [])
                    if item.get("IsClusterWriter")
                ),
                None,
            )
            writer = member_by_id.get(writer_id) or (members[0] if members else None)
            legacy_volumes = {
                member.get("_docker_volume_name")
                for member in members
                if member.get("_docker_volume_name")
                # A replicating reader's volume is live compute state
                # (#1325), not a superseded pre-migration member volume; its
                # revival below remounts it.
                and not member.get("_pg_standby")
            }
            adopted_volume = cluster.get("_shared_volume_name")
            if not adopted_volume and writer is not None:
                adopted_volume = writer.get("_docker_volume_name")
                if adopted_volume:
                    cluster["_shared_volume_name"] = adopted_volume
                    cluster["_shared_storage_initialized"] = True
                    # The initialized writer volume is authoritative. Legacy
                    # releases allowed member connection settings to differ
                    # from the parent, and image environment variables do not
                    # rewrite users or databases on an existing volume.
                    cluster["MasterUsername"] = writer.get(
                        "MasterUsername",
                        cluster.get("MasterUsername", "admin"),
                    )
                    pending_rotation = cluster.get(
                        "_pending_master_password_rotation",
                    )
                    if pending_rotation:
                        cluster["_MasterUserPassword"] = pending_rotation[
                            "new_password"
                        ]
                    else:
                        cluster["_MasterUserPassword"] = writer.get(
                            "_MasterUserPassword",
                            cluster.get("_MasterUserPassword", "password"),
                        )
                    cluster["DatabaseName"] = (
                        writer.get("DBName")
                        or cluster.get("DatabaseName")
                        or "mydb"
                    )

            docker_client = _get_docker()
            if docker_client:
                legacy_container_owners = {}
                for member in members:
                    member_id = member.get("DBInstanceIdentifier")
                    if not member_id:
                        continue
                    scoped_legacy_name = _legacy_scoped_rds_docker_name(
                        member_id, account_id, region,
                    )
                    legacy_container_owners.setdefault(
                        scoped_legacy_name, set(),
                    ).add(member_id)
                    explicit_legacy_name = member.get(
                        "_legacy_docker_container_name",
                    )
                    if explicit_legacy_name:
                        legacy_container_owners.setdefault(
                            explicit_legacy_name, set(),
                        ).add(member_id)
                legacy_container_remains = False
                for legacy_name, expected_member_ids in (
                    legacy_container_owners.items()
                ):
                    try:
                        legacy_container = docker_client.containers.get(
                            legacy_name,
                        )
                    except Exception:
                        continue
                    if _rds_container_is_owned_by(
                        legacy_container,
                        expected_db_ids=expected_member_ids,
                        expected_cluster_ids={cluster_id},
                        account_id=account_id,
                        region=region,
                    ):
                        try:
                            legacy_container.remove(
                                force=True, v=False,
                            )
                        except Exception as e:
                            logger.warning(
                                "RDS: failed to remove legacy Aurora member "
                                "container %s: %s", legacy_name, e,
                            )
                    else:
                        logger.warning(
                            "RDS: refusing to remove container %s while "
                            "migrating Aurora members %s because its labels "
                            "do not prove ownership",
                            legacy_name,
                            sorted(expected_member_ids),
                        )
                    try:
                        docker_client.containers.get(legacy_name)
                    except Exception:
                        continue
                    logger.warning(
                        "RDS: legacy Aurora member container %s remains "
                        "after migration removal", legacy_name,
                    )
                    legacy_container_remains = True

                if legacy_container_remains:
                    cluster.pop("_shared_legacy_migration_in_progress", None)
                    cluster["_shared_legacy_migration_blocked"] = True
                    cluster["_shared_container_ready"] = False
                    for member in members:
                        member["DBInstanceStatus"] = "failed"
                    return
                cluster.pop("_shared_legacy_migration_blocked", None)
                for member in members:
                    member.pop("_legacy_docker_container_name", None)

            # The persisted member list is only a restore-time snapshot. Requests
            # can delete those members (or the entire cluster) while legacy
            # resources are being inspected above. Serialize the final check with
            # cluster teardown and start compute only for this same cluster
            # incarnation with at least one original member still attached.
            original_member_ids = set(member_by_id)
            with _shared_container_lock:
                current_cluster = _clusters.get(cluster_id)
                current_member_ids = {
                    item.get("DBInstanceIdentifier")
                    for item in cluster.get("DBClusterMembers", [])
                    if item.get("DBInstanceIdentifier")
                }
                original_member_still_attached = any(
                    member_id in current_member_ids
                    and _instances.get(member_id) is member_by_id[member_id]
                    for member_id in original_member_ids
                )
                headless_applier_still_required = bool(
                    not original_member_ids
                    and not current_member_ids
                    and cluster.get("_mysql_headless_applier_required")
                )
                if (
                    current_cluster is not cluster
                    or int(cluster.get("_shared_container_epoch", 0))
                    != restore_epoch
                    or not (
                        original_member_still_attached
                        or headless_applier_still_required
                    )
                ):
                    if current_cluster is cluster:
                        cluster.pop(
                            "_shared_legacy_migration_in_progress", None,
                        )
                    return
                result = _start_cluster_shared_container(
                    cluster_id,
                    cluster,
                    remove_stale=True,
                )
                cluster.pop("_shared_legacy_migration_in_progress", None)
            status = "failed" if result.get("failed") else "available"
            authenticated_ready = False
            if result.get("started"):
                status = "creating"
            for member in members:
                if (
                    member.get("_pg_standby")
                    and not result.get("started")
                    and not result.get("failed")
                ):
                    # Control-plane-only restore: no reader container is
                    # coming back, so the member falls back to aliasing the
                    # writer (#1325).
                    _demote_pg_standby_to_alias(
                        member, cluster, "no cluster compute on warm boot",
                    )
                _attach_instance_to_shared_cluster(member, cluster)
                member["DBInstanceStatus"] = status
            _sync_cluster_endpoints(cluster)

            standbys_to_revive = []
            if result.get("started"):
                container_id = cluster.get("_shared_container_id")
                container_epoch = result.get("container_epoch")

                def _container_alive():
                    try:
                        container = docker_client.containers.get(container_id)
                        container.reload()
                        return container.status not in (
                            "exited", "dead", "removing",
                        )
                    except Exception:
                        return False

                with _shared_container_lock:
                    pending_rotation = cluster.get(
                        "_pending_master_password_rotation",
                    )
                    readiness_password = (
                        pending_rotation["old_password"]
                        if pending_rotation
                        else cluster.get("_MasterUserPassword", "password")
                    )
                    root_password = readiness_password
                    readiness_user = cluster.get("MasterUsername", "admin")
                    if _mysql_replication_secondary(cluster):
                        # A fresh secondary volume has only the image's local
                        # root account.  Once replication setup creates the
                        # non-binlogged control account, persisted restarts use
                        # that stable credential instead of the replicated
                        # application administrator.
                        readiness_user = "root"
                        if cluster.get("_mysql_control_user_ready"):
                            readiness_user = _MYSQL_CONTROL_USER
                            readiness_password = _MYSQL_CONTROL_PASSWORD
                authenticated_ready = _wait_for_database_ready(
                    result.get("readiness_host")
                    or cluster["_shared_endpoint"]["Address"],
                    result.get("readiness_port")
                    or cluster["_shared_endpoint"]["Port"],
                    cluster.get("Engine", "aurora-postgresql"),
                    readiness_user,
                    readiness_password,
                    None
                    if _mysql_replication_secondary(cluster)
                    else cluster.get("DatabaseName") or "mydb",
                    _container_alive,
                )
                if authenticated_ready and _is_mysql_engine(
                    cluster.get("Engine", ""),
                ):
                    _ensure_mysql_compatibility(
                        container_id,
                        result.get("readiness_host")
                        or cluster["_shared_endpoint"]["Address"],
                        result.get("readiness_port")
                        or cluster["_shared_endpoint"]["Port"],
                        root_password,
                        cluster.get("EngineVersion")
                        or _default_engine_version(cluster.get("Engine", "")),
                        cluster_id,
                        engine=cluster.get("Engine", "aurora-mysql"),
                    )
                with _shared_container_lock:
                    current_cluster = _clusters.get(cluster_id)
                    if (
                        current_cluster is not cluster
                        or (
                            container_epoch is not None
                            and cluster.get("_shared_container_epoch")
                            != container_epoch
                        )
                        or cluster.get("_shared_container_id") != container_id
                    ):
                        return
                    pending_rotation = cluster.get(
                        "_pending_master_password_rotation",
                    )
                    if authenticated_ready and pending_rotation:
                        authenticated_ready = _rotate_real_password(
                            cluster,
                            pending_rotation["old_password"],
                            pending_rotation["new_password"],
                        )
                        if authenticated_ready:
                            cluster.pop(
                                "_pending_master_password_rotation", None,
                            )
                            _sync_global_mysql_credentials(cluster)
                    if authenticated_ready:
                        cluster["_shared_storage_initialized"] = True
                    if authenticated_ready and _is_mysql_engine(
                        cluster.get("Engine", ""),
                    ) and not _mysql_replication_secondary(cluster):
                        _grant_mysql_master_user_privileges(
                            result.get("readiness_host")
                            or cluster["_shared_endpoint"]["Address"],
                            result.get("readiness_port")
                            or cluster["_shared_endpoint"]["Port"],
                            cluster.get("MasterUsername", "admin"),
                            cluster.get("_MasterUserPassword", "password"),
                            cluster_id,
                        )
                    cluster["_shared_container_ready"] = authenticated_ready
                    if authenticated_ready and _aurora_mysql_8_replication_enabled(
                        cluster,
                    ):
                        _configure_or_defer_mysql_replication(cluster_id, cluster)
                    status = "available" if authenticated_ready else "failed"
                    for current_member in cluster.get(
                        "DBClusterMembers", [],
                    ):
                        member = _instances.get(
                            current_member.get("DBInstanceIdentifier"),
                        )
                        if member is None:
                            continue
                        if member.get("_pg_standby") and authenticated_ready:
                            # A replicating reader's compute is revived
                            # outside the lock below (#1325); the writer
                            # becoming ready does not make a reader whose
                            # container the host restart killed available.
                            standbys_to_revive.append(member)
                            continue
                        _attach_instance_to_shared_cluster(member, cluster)
                        member["DBInstanceStatus"] = status
                    _sync_cluster_endpoints(cluster)
                    _refresh_cluster_status(cluster_id)
                for member in standbys_to_revive:
                    if not _revive_pg_reader(
                        member["DBInstanceIdentifier"], member, cluster,
                    ):
                        # The failed revival landed the cluster back on
                        # stopped; reviving further standbys would race
                        # that transition.
                        break
            else:
                cluster["_shared_container_ready"] = status == "available"

            # Superseded member volumes are recovery copies until the adopted
            # writer volume has passed an authenticated database readiness
            # check. Docker accepting containers.run() is not sufficient.
            if docker_client and authenticated_ready:
                for volume_name in legacy_volumes - {adopted_volume}:
                    try:
                        docker_client.volumes.get(volume_name).remove()
                    except Exception as e:
                        logger.warning(
                            "RDS: failed to remove superseded Aurora member "
                            "volume %s: %s", volume_name, e,
                        )

        threading.Thread(target=ctx.run, args=(_cluster_runner,), daemon=True).start()

    for account_id, region, db_id, inst in standalone:
        ctx = contextvars.copy_context()

        def _instance_runner(account_id=account_id, region=region, db_id=db_id, inst=inst):
            if account_id is not None:
                _request_account_id.set(account_id)
            if region is not None:
                _request_region.set(region)
            _start_rds_container_for_instance(db_id, inst)

        threading.Thread(target=ctx.run, args=(_instance_runner,), daemon=True).start()


def _best_effort_region_from_record_arn(record, field):
    """Return a region while restoring legacy RDS state.

    This is intentionally best-effort persistence migration logic, not request
    validation.
    """
    arn = record.get(field, "") if isinstance(record, dict) else ""
    try:
        region = parse_arn(arn).region
    except ArnParseError:
        return get_region()
    return region or get_region()


def _rds_docker_scope(account_id=None, region=None):
    account_id = account_id or get_account_id()
    region = region or get_region()
    return hashlib.sha1(f"{account_id}:{region}".encode()).hexdigest()[:12]


def _rds_docker_name(db_id, account_id=None, region=None):
    return f"ministack-rds-{_rds_docker_scope(account_id, region)}-instance-{db_id}"


def _rds_docker_volume_name(db_id, account_id=None, region=None):
    return f"{_rds_docker_name(db_id, account_id, region)}-data"


def _legacy_scoped_rds_docker_name(db_id, account_id=None, region=None):
    """Return the pre-namespace standalone container name."""
    return f"ministack-rds-{_rds_docker_scope(account_id, region)}-{db_id}"


def _legacy_scoped_rds_docker_volume_name(db_id, account_id=None, region=None):
    return f"{_legacy_scoped_rds_docker_name(db_id, account_id, region)}-data"


def _rds_cluster_docker_name(cluster_id, account_id=None, region=None):
    return f"ministack-rds-{_rds_docker_scope(account_id, region)}-cluster-{cluster_id}"


def _rds_cluster_docker_volume_name(cluster_id, account_id=None, region=None):
    # Put the resource type before the scope hash. The legacy standalone shape
    # is ``ministack-rds-<scope>-<db-id>-data``, so merely inserting
    # ``cluster-`` after the scope still collides with a legacy instance named
    # ``cluster-<cluster-id>``.
    return (
        f"ministack-rds-cluster-{_rds_docker_scope(account_id, region)}-"
        f"{cluster_id}-data"
    )


def _legacy_rds_docker_name(db_id):
    return f"ministack-rds-{db_id}"


def _legacy_rds_docker_volume_name(db_id):
    return f"ministack-rds-{db_id}-data"


def _rds_container_is_owned_by(
    container, expected_db_ids=(), expected_cluster_ids=(),
    account_id=None, region=None,
):
    """Return whether RDS labels prove a container belongs to an owner."""
    labels = getattr(container, "labels", None)
    if not isinstance(labels, dict):
        labels = (
            getattr(container, "attrs", {})
            .get("Config", {})
            .get("Labels", {})
        )
    if not isinstance(labels, dict):
        return False
    if labels.get("ministack") != "rds":
        return False
    owns_expected_db = labels.get("db_id") in set(expected_db_ids)
    owns_expected_cluster = labels.get("cluster_id") in set(
        expected_cluster_ids,
    )
    if not owns_expected_db and not owns_expected_cluster:
        return False
    if labels.get("account_id") not in (None, account_id or get_account_id()):
        return False
    return labels.get("region") in (None, region or get_region())


def _start_cluster_shared_container(cluster_id, cluster, remove_stale=False):
    """Start the single backing container owned by an Aurora cluster.

    Cluster members are control-plane records that all point at this endpoint.
    The helper is shared by first-member creation and persisted-state restore.
    """
    engine = cluster.get("Engine", "aurora-postgresql")
    engine_version = cluster.get("EngineVersion") or _default_engine_version(engine)
    master_user = cluster.get("MasterUsername", "admin")
    master_pass = cluster.get("_MasterUserPassword", "password")
    db_name = cluster.get("DatabaseName") or "mydb"
    endpoint = {
        "Address": _MINISTACK_HOST,
        "Port": int(cluster.get("Port") or _default_port(engine)),
        "HostedZoneId": cluster.get("HostedZoneId", "Z2R2ITUGPM61AM"),
    }
    cluster.update({
        "_shared_container_id": None,
        "_shared_endpoint": endpoint,
        "_shared_internal_address": None,
        "_shared_internal_port": None,
        "_shared_container_ready": True,
    })

    docker_client = _get_docker()
    if not docker_client:
        return {"started": False, "failed": False, "readiness_host": None, "readiness_port": None}

    image, env, container_port, data_path = _docker_image_for_engine(
        engine, engine_version, master_user, master_pass, db_name,
    )
    if not image:
        return {"started": False, "failed": False, "readiness_host": None, "readiness_port": None}
    if _mysql_replication_secondary(cluster):
        # The writer's retained GTID history must be the sole creator of the
        # application database and master user on a fresh global secondary.
        # Letting both image entrypoints create them assigns different local
        # GTIDs and makes replay stop on duplicate CREATE USER / DATABASE.
        env = dict(env)
        for key in ("MYSQL_DATABASE", "MYSQL_USER", "MYSQL_PASSWORD"):
            env.pop(key, None)

    container_name = _rds_cluster_docker_name(cluster_id)
    if remove_stale:
        try:
            stale_container = docker_client.containers.get(container_name)
        except Exception:
            stale_container = None
        if stale_container:
            if _rds_container_is_owned_by(
                stale_container,
                expected_cluster_ids={cluster_id},
            ):
                try:
                    stale_container.remove(force=True, v=False)
                except Exception:
                    pass
            else:
                logger.warning(
                    "RDS: refusing to remove container %s before starting "
                    "cluster %s because its labels do not prove ownership",
                    container_name,
                    cluster_id,
                )

    host_port = cluster.get("_shared_host_port") or _next_port()
    if not _is_host_port_free(host_port):
        logger.info(
            "RDS: persisted shared host port %d for cluster %s is in use; allocating fresh free port",
            host_port, cluster_id,
        )
        host_port = _next_port()

    ms_network = _get_ministack_network(docker_client)
    container_kwargs = dict(
        image=image,
        detach=True,
        environment=env,
        ports={f"{container_port}/tcp": host_port},
        name=container_name,
        labels={
            **container_reaper.own_labels("rds"),
            "cluster_id": cluster_id,
            "account_id": get_account_id(),
            "region": get_region(),
        },
    )
    if _prepare_mysql_gtid_history(cluster):
        server_id = _mysql_replication_server_id(
            get_account_id(),
            get_region(),
            cluster_id,
        )
        container_kwargs["command"] = [
            f"--server-id={server_id}",
            "--log-bin=mysql-bin",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
            "--log-replica-updates",
            f"--binlog-expire-logs-seconds={_MYSQL_BINLOG_RETENTION_SECONDS}",
        ]
    if ms_network:
        container_kwargs["network"] = ms_network
    # Aurora storage belongs to the cluster, not to any member instance. Use a
    # named volume even when standalone RDS persistence is disabled so stopping
    # an empty cluster's compute and restarting it later cannot erase its data.
    volume_name = (
        cluster.get("_shared_volume_name")
        or _rds_cluster_docker_volume_name(cluster_id)
    )
    cluster["_shared_volume_name"] = volume_name
    container_kwargs["volumes"] = {
        volume_name: {"bind": data_path, "mode": "rw"},
    }

    try:
        container = docker_client.containers.run(**container_kwargs)
    except Exception as e:
        cluster["_shared_container_ready"] = False
        logger.warning("RDS: failed to start shared container for cluster %s: %s", cluster_id, e)
        return {"started": False, "failed": True, "readiness_host": None, "readiness_port": None}

    endpoint_host = _MINISTACK_HOST
    endpoint_port = host_port
    internal_host = None
    internal_port = None
    readiness_host = "127.0.0.1"
    readiness_port = host_port
    if ms_network:
        try:
            container.reload()
            networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
            container_ip = networks.get(ms_network, {}).get("IPAddress", "")
            if container_ip:
                endpoint_host = container_ip
                endpoint_port = container_port
                internal_host = container_ip
                internal_port = container_port
                readiness_host = container_ip
                readiness_port = container_port
        except Exception:
            pass

    container_epoch = int(cluster.get("_shared_container_epoch", 0)) + 1
    cluster.update({
        "_shared_container_id": container.id,
        "_shared_host_port": host_port,
        "_shared_endpoint": {
            "Address": endpoint_host,
            "Port": endpoint_port,
            "HostedZoneId": cluster.get("HostedZoneId", "Z2R2ITUGPM61AM"),
        },
        "_shared_internal_address": internal_host,
        "_shared_internal_port": internal_port,
        "_shared_container_ready": False,
        "_shared_container_epoch": container_epoch,
    })
    return {
        "started": True,
        "failed": False,
        "readiness_host": readiness_host,
        "readiness_port": readiness_port,
        "network": ms_network,
        "container_port": container_port,
        "container_epoch": container_epoch,
    }


def _instance_owns_container(instance):
    """True when the instance record owns its backing Docker container.

    Standalone DB instances own their container: instance-level delete and
    reset must remove it. Aurora cluster members do not — they alias the
    cluster-owned shared container (marked by ``_shared_cluster_id``), whose
    lifecycle belongs to the cluster helpers. Cluster members that own a
    container of their own (per-instance reader containers, #1325) are
    handled like standalone instances by ownership-based code paths.
    """
    return bool(
        instance.get("_docker_container_id")
        and not instance.get("_shared_cluster_id")
    )


def _cluster_owned_container_ids(cluster):
    """All Docker container IDs backing an Aurora cluster's compute.

    Today a cluster has at most one: the cluster-owned shared container.
    Member instances that own a container of their own (per-instance reader
    containers, #1325) are included so cluster-wide compute operations act
    on every container the cluster is responsible for. IDs are deduplicated
    (first occurrence wins) so a member aliasing an already-listed container
    is not operated on twice.
    """
    ids = []
    if cluster.get("_shared_container_id"):
        ids.append(cluster["_shared_container_id"])
    for member in _cluster_member_instances(cluster):
        if _instance_owns_container(member):
            container_id = member["_docker_container_id"]
            if container_id not in ids:
                ids.append(container_id)
    return ids


def _pg_cluster_replication_enabled(cluster):
    """Whether this cluster launches per-instance reader containers (#1325).

    Requires the ``MINISTACK_RDS_PG_CLUSTER_REPLICATION`` opt-in and an
    Aurora PostgreSQL cluster: the reader bootstrap is physical streaming
    replication (pg_basebackup + hot standby), which has no MySQL analog
    here — Aurora MySQL members keep aliasing the shared container.
    """
    return (
        RDS_PG_CLUSTER_REPLICATION
        and cluster.get("Engine") == "aurora-postgresql"
    )


def _ensure_pg_replication_source(cluster_id, cluster):
    """Provision streaming-replication access on the writer's container.

    Idempotent: creates the replication role and appends the pg_hba.conf
    rule that allows remote replication connections, then reloads
    configuration. Returns True once the writer can serve pg_basebackup
    for reader containers; False when provisioning could not run (no
    Docker, no writer container, or the exec failed) — callers retry.

    The check-then-provision runs under ``_shared_container_lock`` so
    concurrent reader workers on the same cluster provision once instead
    of racing the flag (the script tolerates a rerun, but two interleaved
    executions against the same writer are pointless work).
    """
    with _shared_container_lock:
        if cluster.get("_pg_replication_source_ready"):
            return True
        docker_client = _get_docker()
        container_id = cluster.get("_shared_container_id")
        if not docker_client or not container_id:
            return False
        try:
            container = docker_client.containers.get(container_id)
            exit_code, output = container.exec_run(
                ["sh", "-c", _PG_REPLICATION_SOURCE_SCRIPT],
            )
        except Exception as e:
            logger.warning(
                "RDS: failed to provision replication source for cluster %s: %s",
                cluster_id, e,
            )
            return False
        if exit_code != 0:
            logger.warning(
                "RDS: replication source provisioning for cluster %s exited %s: %s",
                cluster_id,
                exit_code,
                output.decode(errors="replace") if isinstance(output, bytes) else output,
            )
            return False
        cluster["_pg_replication_source_ready"] = True
        return True


def _start_pg_reader_container(db_id, cluster):
    """Launch a member-owned hot-standby container for an Aurora PostgreSQL
    cluster (#1325).

    Returns a start-result dict ({"started", "failed", endpoint fields}),
    or None when per-instance replication cannot run here — no Docker, no
    shared internal Docker network, or no known writer address — in which
    case the caller falls back to shared-container aliasing, the flag-off
    behavior.
    """
    docker_client = _get_docker()
    if not docker_client:
        return None
    cluster_id = cluster["DBClusterIdentifier"]
    ms_network = _get_ministack_network(docker_client)
    writer_host = cluster.get("_shared_internal_address")
    writer_port = cluster.get("_shared_internal_port")
    if not ms_network or not writer_host:
        # pg_basebackup runs inside the reader container and must reach the
        # writer container; without a shared Docker network there is no
        # address that works from both the host and the reader.
        logger.warning(
            "RDS: MINISTACK_RDS_PG_CLUSTER_REPLICATION is on but cluster %s "
            "has no internal Docker network address; reader %s falls back "
            "to the shared container",
            cluster_id, db_id,
        )
        return None
    engine = cluster.get("Engine", "aurora-postgresql")
    engine_version = cluster.get("EngineVersion") or _default_engine_version(engine)
    master_user = cluster.get("MasterUsername", "admin")
    master_pass = cluster.get("_MasterUserPassword", "password")
    db_name = cluster.get("DatabaseName") or "mydb"
    image, env, container_port, data_path = _docker_image_for_engine(
        engine, engine_version, master_user, master_pass, db_name,
    )
    if not image:
        return None
    env = dict(env)
    env.update({
        "MINISTACK_PG_PRIMARY_HOST": str(writer_host),
        "MINISTACK_PG_PRIMARY_PORT": str(writer_port or container_port),
        "MINISTACK_PG_REPLICATION_USER": _PG_REPLICATION_USER,
        "MINISTACK_PG_REPLICATION_PASSWORD": _PG_REPLICATION_PASSWORD,
    })
    host_port = _next_port()
    container_kwargs = dict(
        image=image,
        detach=True,
        environment=env,
        command=["sh", "-c", _PG_READER_BOOTSTRAP_SCRIPT],
        ports={f"{container_port}/tcp": host_port},
        name=_rds_docker_name(db_id),
        network=ms_network,
        labels={
            "ministack": "rds",
            "db_id": db_id,
            "cluster_id": cluster_id,
            "account_id": get_account_id(),
            "region": get_region(),
        },
    )
    volume_name = None
    if RDS_PERSIST:
        volume_name = _rds_docker_volume_name(db_id)
        container_kwargs["volumes"] = {
            volume_name: {"bind": data_path, "mode": "rw"},
        }
    else:
        container_kwargs["tmpfs"] = {
            data_path: f"rw,noexec,nosuid,size={RDS_TMPFS_SIZE}",
        }
    try:
        container = docker_client.containers.run(**container_kwargs)
    except Exception as e:
        logger.warning(
            "RDS: failed to start reader container for %s: %s", db_id, e,
        )
        if volume_name:
            # Docker may have auto-created the named volume before the run
            # failed; a leftover (possibly partially written) volume would
            # poison a retried CreateDBInstance under the same identifier.
            try:
                docker_client.volumes.get(volume_name).remove()
            except Exception:
                pass
        return {"started": False, "failed": True}
    endpoint_host = _MINISTACK_HOST
    endpoint_port = host_port
    internal_host = None
    internal_port = None
    try:
        container.reload()
        networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
        container_ip = networks.get(ms_network, {}).get("IPAddress", "")
        if container_ip:
            endpoint_host = container_ip
            endpoint_port = container_port
            internal_host = container_ip
            internal_port = container_port
    except Exception:
        pass
    return {
        "started": True,
        "failed": False,
        "container_id": container.id,
        "volume_name": volume_name,
        "host_port": host_port,
        "endpoint_host": endpoint_host,
        "endpoint_port": endpoint_port,
        "internal_host": internal_host,
        "internal_port": internal_port,
        "readiness_host": internal_host or "127.0.0.1",
        "readiness_port": internal_port or host_port,
    }


def _remove_failed_pg_reader_compute(db_id, container_id, volume_name):
    """Remove a failed replicating reader's container and named volume.

    A standby that never became reachable leaves nothing worth keeping:
    its data directory holds at most a partial base backup, and the volume
    name is derived from the instance identifier, so leaving either behind
    would leak a host port and poison a retried CreateDBInstance under the
    same identifier. Cleanup for healthy readers stays in
    ``_delete_db_instance``; this runs only for bootstrap failures.
    """
    docker_client = _get_docker()
    if not docker_client:
        return
    try:
        container = docker_client.containers.get(container_id)
        container.stop(timeout=5)
        container.remove(v=True)
    except Exception as e:
        logger.warning(
            "RDS: failed to remove container of failed reader %s: %s",
            db_id, e,
        )
    if volume_name:
        try:
            docker_client.volumes.get(volume_name).remove()
        except Exception as e:
            logger.warning(
                "RDS: failed to remove volume of failed reader %s: %s",
                db_id, e,
            )


def _bg_finalize_pg_reader(
    db_id, cluster_id, engine, master_user, master_pass, db_name,
    ready_host, ready_port, container_id,
):
    """Readiness worker for a member-owned replicating reader (#1325).

    Waits for the writer to become ready, provisions the replication
    source on it, then waits for the standby to accept authenticated
    connections (the reader container retries pg_basebackup internally
    until the source is provisioned). Same liveness contract as the
    generic worker: no wall clock — the instance stays ``creating`` while
    its container is up and booting, and flips to ``failed`` if the
    container dies first. Same staleness contract as the shared-container
    worker: if the instance record no longer points at this worker's
    container (deleted and recreated under the same identifier), the
    worker is superseded and must not touch the new record. A bootstrap
    failure removes the reader's own container and volume.
    """
    def _container_alive():
        client = _get_docker()
        if not client:
            return True
        try:
            c = client.containers.get(container_id)
            c.reload()
            return c.status not in ("exited", "dead", "removing")
        except Exception:
            return False

    def _stale(instance):
        return instance.get("_docker_container_id") != container_id

    while True:
        instance = _instances.get(db_id)
        cluster = _clusters.get(cluster_id)
        if instance is None or cluster is None:
            return  # deleted while bootstrapping
        if _stale(instance):
            return  # superseded: the recreated instance has its own worker
        if instance.get("DBInstanceStatus") == "failed":
            # Writer compute failed during creation; its readiness worker
            # already published the failure for every member. The standby
            # container would retry pg_basebackup against the dead writer
            # forever, so remove it.
            _remove_failed_pg_reader_compute(
                db_id, container_id, instance.get("_docker_volume_name"),
            )
            return
        if cluster.get("Status") == "stopped":
            # StopDBCluster stopped this standby's container along with the
            # writer's. That is a parked reader, not a dead one: its
            # container and volume are preserved, and StartDBCluster revives
            # it with a fresh worker.
            return
        if cluster.get("Status") == "stopping":
            # StopDBCluster is mid-flight. It either lands on ``stopped``
            # (handled above on the next pass) or fails, rolls this
            # container back to running, and restores ``available`` —
            # keep waiting rather than mistaking the stop for a death.
            time.sleep(1)
            continue
        if instance.get("DBInstanceStatus") == "stopped":
            # A failed StopDBCluster's rollback could not restart this
            # standby's container and parked the member instead. Preserved
            # compute, not a bootstrap failure: a StopDBCluster retry and
            # StartDBCluster bring it back.
            return
        if not _container_alive():
            with _shared_container_lock:
                # StopDBCluster stops containers while holding this lock and
                # publishes ``stopping`` before taking it; serialize with it
                # so a container it just stopped is not destroyed as dead.
                current = _clusters.get(cluster_id)
                if current is cluster and cluster.get("Status") in (
                    "stopping", "stopped",
                ):
                    continue
                instance["DBInstanceStatus"] = "failed"
                _remove_failed_pg_reader_compute(
                    db_id, container_id, instance.get("_docker_volume_name"),
                )
            _refresh_cluster_status(cluster_id)
            return
        if cluster.get(
            "_shared_container_ready",
        ) and _ensure_pg_replication_source(cluster_id, cluster):
            break
        time.sleep(1)

    database_ready = _wait_for_database_ready(
        ready_host, ready_port, engine, master_user, master_pass, db_name,
        _container_alive,
    )
    instance = _instances.get(db_id)
    cluster = _clusters.get(cluster_id)
    if instance is None or _stale(instance):
        return
    if not database_ready:
        with _shared_container_lock:
            # A stop landing while this worker was inside the readiness wait
            # kills the container deliberately. Same park-don't-destroy rule
            # as the pre-wait loop: the stopped container and its volume are
            # exactly what StartDBCluster revives.
            stop_intervened = (
                cluster is not None
                and _clusters.get(cluster_id) is cluster
                and cluster.get("Status") in ("stopping", "stopped")
            )
            if stop_intervened or instance.get("DBInstanceStatus") == "stopped":
                return
            logger.warning(
                "RDS: reader container for %s at %s:%s exited before "
                "becoming reachable", db_id, ready_host, ready_port,
            )
            instance["DBInstanceStatus"] = "failed"
            _remove_failed_pg_reader_compute(
                db_id, container_id, instance.get("_docker_volume_name"),
            )
        _refresh_cluster_status(cluster_id)
        return
    instance["DBInstanceStatus"] = "available"
    if cluster:
        _sync_cluster_endpoints(cluster)
    _refresh_cluster_status(cluster_id)
    logger.info(
        "RDS: replicating reader %s ready at %s:%s", db_id,
        ready_host, ready_port,
    )


def _demote_pg_standby_to_alias(instance, cluster, reason):
    """Demote a replicating reader to a shared-container alias member.

    The fallback when a reader's per-instance compute cannot come (or come
    back): the member stays usable through the writer's shared container —
    the flag-off behavior — instead of advertising an endpoint no container
    serves.

    Demotion is also the last moment the member still references its owned
    compute: ``_attach_instance_to_shared_cluster`` overwrites the container
    id and nulls the volume name, so anything not removed here is orphaned —
    and a container left holding the reserved instance name would 409 a
    later CreateDBInstance under the same identifier. Removal is best-effort
    (no Docker means nothing can be removed, and the stale-name sweep on
    creation remains the backstop).
    """
    db_id = instance.get("DBInstanceIdentifier")
    docker_client = _get_docker()
    if docker_client:
        identifiers = []
        recorded = instance.get("_docker_container_id")
        if recorded and recorded != cluster.get("_shared_container_id"):
            identifiers.append(recorded)
        reserved_name = _rds_docker_name(db_id)
        if reserved_name not in identifiers:
            identifiers.append(reserved_name)
        for identifier in identifiers:
            try:
                stale = docker_client.containers.get(identifier)
            except Exception:
                continue
            try:
                stale.remove(force=True, v=False)
            except Exception as e:
                logger.warning(
                    "RDS: failed to remove container %s of demoted reader "
                    "%s: %s", identifier, db_id, e,
                )
        volume_name = instance.get("_docker_volume_name")
        if volume_name and volume_name != cluster.get("_shared_volume_name"):
            try:
                docker_client.volumes.get(volume_name).remove()
            except Exception as e:
                logger.warning(
                    "RDS: failed to remove volume of demoted reader %s: %s",
                    db_id, e,
                )
    instance.pop("_pg_standby", None)
    _attach_instance_to_shared_cluster(instance, cluster)
    logger.warning(
        "RDS: demoted replicating reader %s to a shared-container alias "
        "member: %s",
        instance.get("DBInstanceIdentifier"), reason,
    )


def _revive_pg_reader(db_id, instance, cluster):
    """Recreate a replicating reader's compute after StopDBCluster →
    StartDBCluster or a warm boot (#1325 slice 3).

    Reader compute is stateless — as in real Aurora, where reader instances
    own no durable storage — so revival recreates the container and
    re-clones from the writer instead of restarting the stopped one. A
    restarted container would re-run its bootstrap with the writer address
    baked into its environment at creation, which the writer's own restart
    may have changed; recreation always targets the writer's current
    address. Falls back to shared-container aliasing when per-instance
    replication cannot run here, and lands the cluster back on ``stopped``
    when the launch genuinely failed, so the start can be retried.

    Returns False when the revival failed and the caller should stop
    reviving further standbys (the cluster has been returned to
    ``stopped``); True otherwise.
    """
    cluster_id = cluster["DBClusterIdentifier"]
    if not _pg_cluster_replication_enabled(cluster):
        # The opt-in can be withdrawn between runs; a persisted standby must
        # not outlive the flag that created it.
        _demote_pg_standby_to_alias(
            instance, cluster,
            "MINISTACK_RDS_PG_CLUSTER_REPLICATION is off",
        )
        instance["DBInstanceStatus"] = "available"
        _sync_cluster_endpoints(cluster)
        _refresh_cluster_status(cluster_id)
        return True
    docker_client = _get_docker()
    if docker_client:
        # Free the reserved container name (and the recorded container, if
        # different) before relaunching under the same identifier. The old
        # standby's data is at most a stale clone, so removal loses nothing.
        identifiers = []
        recorded = instance.get("_docker_container_id")
        if recorded and recorded != cluster.get("_shared_container_id"):
            identifiers.append(recorded)
        reserved_name = _rds_docker_name(db_id)
        if reserved_name not in identifiers:
            identifiers.append(reserved_name)
        for identifier in identifiers:
            try:
                stale = docker_client.containers.get(identifier)
            except Exception:
                continue
            try:
                stale.remove(force=True, v=False)
            except Exception as e:
                logger.warning(
                    "RDS: failed to remove stale reader container %s for "
                    "%s: %s", identifier, db_id, e,
                )
    revive_epoch = int(cluster.get("_shared_container_epoch", 0))
    launch = _start_pg_reader_container(db_id, cluster)
    if launch is None:
        _demote_pg_standby_to_alias(
            instance, cluster,
            "per-instance replication cannot run here",
        )
        instance["DBInstanceStatus"] = "available"
        _sync_cluster_endpoints(cluster)
        _refresh_cluster_status(cluster_id)
        return True
    if not launch.get("started"):
        # Real AWS lands a failed start back on ``stopped`` — never on a
        # transitional status (see _fail_start in _start_db_cluster). A
        # reader marked ``failed`` here would drive the cluster to
        # ``creating``, where StartDBCluster and StopDBCluster are both
        # rejected and the cluster is wedged until the instance is deleted.
        # Instead, park the cluster's compute again and land everything back
        # on ``stopped`` so StartDBCluster can simply be retried. The member
        # keeps its standby identity for that retry.
        logger.warning(
            "RDS: failed to revive replicating reader %s; returning cluster "
            "%s to stopped for retry", db_id, cluster_id,
        )
        if _stop_cluster_shared_container(cluster_id, cluster):
            cluster["Status"] = "stopped"
            for member in _cluster_member_instances(cluster):
                member["DBInstanceStatus"] = "stopped"
        else:
            # The fail-back stop itself failed: the writer (and any
            # already-revived reader) is still serving, so publishing
            # ``stopped`` would lie. Keep the cluster available, publish
            # only this member as stopped, and let the ReaderEndpoint fall
            # back to compute that exists.
            cluster["_shared_container_ready"] = True
            cluster["Status"] = "available"
            instance["DBInstanceStatus"] = "stopped"
            _sync_cluster_endpoints(cluster)
        return False
    with _shared_container_lock:
        # The Docker launch above is slow; re-verify this worker is still
        # acting on live records (mirrors the restore and start paths). A
        # concurrent DeleteDBInstance, cluster teardown, or Stop/Start
        # (epoch bump) supersedes this revival: remove the just-created
        # compute instead of publishing it onto a stale record.
        if (
            _instances.get(db_id) is not instance
            or _clusters.get(cluster_id) is not cluster
            or int(cluster.get("_shared_container_epoch", 0)) != revive_epoch
        ):
            _remove_failed_pg_reader_compute(
                db_id, launch.get("container_id"), launch.get("volume_name"),
            )
            return True
        instance.update({
            "Endpoint": {
                "Address": launch["endpoint_host"],
                "Port": int(launch["endpoint_port"]),
                "HostedZoneId": (instance.get("Endpoint") or {}).get(
                    "HostedZoneId", "Z2R2ITUGPM61AM",
                ),
            },
            "_HostPort": launch.get("host_port"),
            "_docker_container_id": launch.get("container_id"),
            "_docker_volume_name": (
                launch.get("volume_name")
                or instance.get("_docker_volume_name")
            ),
            "_internal_address": launch.get("internal_host"),
            "_internal_port": launch.get("internal_port"),
            "DBInstanceStatus": "creating",
        })
    ctx = contextvars.copy_context()
    threading.Thread(
        target=ctx.run,
        args=(
            _bg_finalize_pg_reader, db_id, cluster_id,
            cluster.get("Engine", "aurora-postgresql"),
            cluster.get("MasterUsername", "admin"),
            cluster.get("_MasterUserPassword", "password"),
            cluster.get("DatabaseName") or "mydb",
            launch.get("readiness_host"),
            launch.get("readiness_port"),
            launch.get("container_id"),
        ),
        daemon=True,
    ).start()
    return True


def _stop_cluster_shared_container(cluster_id, cluster):
    """Stop, but do not remove, an Aurora cluster's database compute.

    Aurora keeps the cluster volume when compute goes away — after the final
    DB instance is deleted, and while the cluster is stopped via
    ``StopDBCluster`` — but no SQL endpoint is reachable until compute
    returns. A stopped Docker container models that split: the cluster still
    owns its data, and the cluster-owned volume is only removed by
    DeleteDBCluster (the container itself may be replaced if a later
    StartDBCluster has to fall back to recreating compute).

    Returns True when all compute is stopped — including when there was
    nothing to stop — and False when a running container could not be
    stopped. A False return is all-or-nothing about the writer: reader
    containers stop first and the writer's shared container last, and any
    container this call already stopped is restarted before returning, so a
    failed stop never leaves the writer's published ``available`` pointing
    at exited compute. If that rollback restart itself fails, the affected
    standby member alone is published ``stopped`` and the ReaderEndpoint is
    re-synced away from it.
    """
    with _shared_container_lock:
        # Invalidate every readiness worker before the potentially slow Docker
        # stop. The worker takes the same lock for its final epoch check and
        # state transition, so it cannot publish ready=True after this point.
        cluster["_shared_container_epoch"] = int(
            cluster.get("_shared_container_epoch", 0),
        ) + 1
        cluster["_shared_container_ready"] = False
        container_ids = _cluster_owned_container_ids(cluster)
        docker_client = _get_docker()
        if not docker_client or not container_ids:
            return True
        stopped = []
        # _cluster_owned_container_ids lists the shared (writer) container
        # first; stop it last so a reader-stop failure leaves the writer
        # untouched and only reader containers ever need rolling back.
        for container_id in reversed(container_ids):
            try:
                container = docker_client.containers.get(container_id)
            except Exception:
                # A container that no longer exists is not running compute;
                # the goal state is already met.
                continue
            try:
                container.reload()
                if container.status not in ("created", "exited", "dead", "removing"):
                    container.stop(timeout=5)
                    stopped.append(container)
                    logger.info(
                        "RDS: stopped container %s for cluster %s",
                        container_id,
                        cluster_id,
                    )
            except Exception as e:
                logger.warning(
                    "RDS: failed to stop container %s for cluster %s: %s",
                    container_id,
                    cluster_id,
                    e,
                )
                # Partial stops must not survive a failed StopDBCluster:
                # the caller keeps the cluster ``available``, so every
                # container this call stopped is restarted to keep that
                # status honest.
                for prior in stopped:
                    try:
                        prior.start()
                    except Exception as restart_error:
                        logger.warning(
                            "RDS: failed to restart container %s while "
                            "rolling back a failed stop of cluster %s: %s",
                            prior.id,
                            cluster_id,
                            restart_error,
                        )
                        # The writer stops last, so an unrestartable
                        # container here is always a reader's. Publish that
                        # one member as stopped and steer the
                        # ReaderEndpoint away from its exited container.
                        for member in _cluster_member_instances(cluster):
                            if (
                                member.get("_pg_standby")
                                and member.get("_docker_container_id")
                                == prior.id
                            ):
                                member["DBInstanceStatus"] = "stopped"
                        _sync_cluster_endpoints(cluster)
                return False
        return True


def _remove_cluster_shared_resources(
    cluster_id, cluster, timeout=5, account_id=None, region=None,
):
    """Remove a cluster-owned container and volume by ID or stable name."""
    docker_client = _get_docker()
    if not docker_client:
        return

    container_identifiers = []
    if cluster.get("_shared_container_id"):
        container_identifiers.append(cluster["_shared_container_id"])
    container_name = _rds_cluster_docker_name(
        cluster_id, account_id, region,
    )
    if container_name not in container_identifiers:
        container_identifiers.append(container_name)

    for identifier in container_identifiers:
        try:
            container = docker_client.containers.get(identifier)
            container.stop(timeout=timeout)
            container.remove(v=True)
            logger.info("RDS: removed shared container for cluster %s", cluster_id)
            break
        except Exception:
            continue

    volume_name = (
        cluster.get("_shared_volume_name")
        or _rds_cluster_docker_volume_name(
            cluster_id, account_id, region,
        )
    )
    try:
        docker_client.volumes.get(volume_name).remove()
    except Exception as e:
        logger.warning(
            "RDS: failed to remove shared volume for cluster %s: %s",
            cluster_id, e,
        )


def _restart_cluster_shared_container(cluster_id, cluster):
    """Restart the preserved shared container when an empty cluster grows."""
    docker_client = _get_docker()
    container_id = cluster.get("_shared_container_id")
    if not docker_client or not container_id:
        return {
            "started": False,
            "failed": False,
            "readiness_host": None,
            "readiness_port": None,
        }

    try:
        container = docker_client.containers.get(container_id)
        container.start()
        container.reload()
    except Exception as e:
        cluster["_shared_container_ready"] = False
        logger.warning(
            "RDS: failed to restart shared container for cluster %s: %s",
            cluster_id,
            e,
        )
        return {
            "started": False,
            "failed": True,
            "readiness_host": None,
            "readiness_port": None,
        }

    engine = cluster.get("Engine", "aurora-postgresql")
    container_port = int(
        cluster.get("_shared_internal_port") or _default_port(engine),
    )
    host_port = int(
        cluster.get("_shared_host_port")
        or (cluster.get("_shared_endpoint") or {}).get("Port")
        or container_port,
    )
    ms_network = _get_ministack_network(docker_client)
    endpoint_host = _MINISTACK_HOST
    endpoint_port = host_port
    internal_host = None
    internal_port = None
    readiness_host = "127.0.0.1"
    readiness_port = host_port
    if ms_network:
        networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
        container_ip = networks.get(ms_network, {}).get("IPAddress", "")
        if container_ip:
            endpoint_host = container_ip
            endpoint_port = container_port
            internal_host = container_ip
            internal_port = container_port
            readiness_host = container_ip
            readiness_port = container_port

    container_epoch = int(cluster.get("_shared_container_epoch", 0)) + 1
    cluster.update({
        "_shared_endpoint": {
            "Address": endpoint_host,
            "Port": endpoint_port,
            "HostedZoneId": cluster.get("HostedZoneId", "Z2R2ITUGPM61AM"),
        },
        "_shared_internal_address": internal_host,
        "_shared_internal_port": internal_port,
        "_shared_container_ready": False,
        "_shared_container_epoch": container_epoch,
    })
    logger.info("RDS: restarted shared container for cluster %s", cluster_id)
    return {
        "started": True,
        "failed": False,
        "readiness_host": readiness_host,
        "readiness_port": readiness_port,
        "network": ms_network,
        "container_port": container_port,
        "container_epoch": container_epoch,
    }


def _image_is_local(docker_client, image: str) -> bool:
    """True when the image is already pulled.

    ``containers.run`` auto-pulls on ImageNotFound, so calling it with a cold
    cache blocks the request for as long as the pull takes — minutes for the
    database and Airflow images. AWS returns ``creating`` in milliseconds and
    provisions asynchronously, so a missing image must not be pulled inline.
    """
    try:
        docker_client.images.get(image)
        return True
    except Exception:
        return False


def _start_rds_container_for_instance(db_id, instance):
    """Re-spin (or re-attach to) the Docker container for a restored instance.

    Reads engine, credentials, and endpoint info from the persisted instance
    dict instead of CreateDBInstance request params. If a container with the
    deterministic account+Region scoped name already exists (e.g. host rebooted
    but Docker preserved stopped containers), it is removed first so a clean run
    can attach to the persistent named volume. Sets
    ``DBInstanceStatus`` to ``available`` on success, ``failed`` on Docker
    error.
    """
    docker_client = _get_docker()
    if not docker_client:
        instance["DBInstanceStatus"] = "available"
        return

    engine = instance.get("Engine", "postgres")
    engine_version = instance.get("EngineVersion") or _default_engine_version(engine)
    master_user = instance.get("MasterUsername", "admin")
    master_pass = instance.get("_MasterUserPassword", "password")
    db_name = instance.get("DBName") or "mydb"
    endpoint = instance.get("Endpoint") or {}
    # Host port must come from `_HostPort` (stored at create time), NOT from
    # `Endpoint.Port` — the latter is overwritten to `container_port` (e.g.
    # 5432 for postgres) to match real AWS, so reading it here would try to
    # bind 5432 on the host and collide on every respawn (#692 follow-up).
    # Legacy instances persisted before `_HostPort` was stored fall back to a
    # fresh free port from `_next_port()`.
    host_port = instance.get("_HostPort") or _next_port()
    # If the stored host port was claimed by something else between
    # restarts (another ministack, another db instance, a user app),
    # docker bind would fail with "port is already allocated". Fall
    # back to a fresh free port and persist it so subsequent restarts
    # converge on a stable mapping again.
    if not _is_host_port_free(host_port):
        logger.info("RDS: persisted host port %d for %s is in use; "
                    "allocating fresh free port", host_port, db_id)
        host_port = _next_port()
    instance["_HostPort"] = host_port

    image, env_vars, container_port, data_path = _docker_image_for_engine(
        engine, engine_version, master_user, master_pass, db_name,
    )
    if not image:
        instance["DBInstanceStatus"] = "available"
        return

    container_name = _rds_docker_name(db_id)
    stale_names = [container_name]
    if not instance.pop("_legacy_scoped_container_migrated", False):
        stale_names.append(_legacy_scoped_rds_docker_name(db_id))
    legacy_container_name = instance.pop("_legacy_docker_container_name", None)
    if legacy_container_name and legacy_container_name != container_name:
        stale_names.append(legacy_container_name)
    for stale_name in stale_names:
        try:
            existing = docker_client.containers.get(stale_name)
            # `force=True` stops AND removes in one shot, including
            # half-spawned "Created" containers that didn't fully start
            # — those still hold port mappings and would collide with
            # the next `containers.run` (#692 follow-up: doodaz saw
            # a `Created` container blocking the bind).
            try:
                existing.remove(force=True, v=False)
            except Exception as e:
                logger.warning("RDS: failed to remove stale container %s: %s",
                               stale_name, e)
            # Verify the name is actually free now; if removal silently
            # failed, abort respawn rather than crash inside `containers.run`
            # with a confusing name-conflict error.
            try:
                docker_client.containers.get(stale_name)
                logger.warning("RDS: stale container %s still present after "
                               "force-remove — aborting respawn", stale_name)
                instance["DBInstanceStatus"] = "failed"
                return
            except Exception:
                pass  # Good — name is gone.
        except Exception:
            pass  # No existing container with that name — fine

    ms_network = _get_ministack_network(docker_client)
    container_kwargs = dict(
        image=image, detach=True,
        environment=env_vars,
        ports={f"{container_port}/tcp": host_port},
        name=container_name,
        labels={
            **container_reaper.own_labels("rds"),
            "db_id": db_id,
            "account_id": get_account_id(),
            "region": get_region(),
        },
    )
    if ms_network:
        container_kwargs["network"] = ms_network
    if RDS_PERSIST:
        volume_name = instance.get("_docker_volume_name") or _rds_docker_volume_name(db_id)
        instance["_docker_volume_name"] = volume_name
        container_kwargs["volumes"] = {
            volume_name: {"bind": data_path, "mode": "rw"},
        }
    else:
        container_kwargs["tmpfs"] = {
            data_path: f"rw,noexec,nosuid,size={RDS_TMPFS_SIZE}",
        }

    try:
        container = docker_client.containers.run(**container_kwargs)
    except Exception as e:
        logger.warning("RDS: failed to respawn container for %s: %s", db_id, e)
        instance["DBInstanceStatus"] = "failed"
        return

    # Check-and-publish under the lock, so a DeleteDBInstance cannot land in the
    # gap between them and leave this container referenced by nothing. The check
    # has to be unconditional — the MySQL readiness path below has its own, but
    # Postgres would otherwise leave the container running forever.
    with resource_lock("rds", db_id):
        current = _instances.get(db_id)
        orphaned = current is not instance or instance.get("_deleting")
        if not orphaned:
            instance["_docker_container_id"] = container.id
    if orphaned:
        # Deleted (or replaced) while we were pulling and starting. Reclaim the
        # container: no record references it, and the periodic reaper never
        # touches a *running* container, so nothing else ever would. Outside the
        # lock — Docker calls never hold it.
        logger.info("RDS: instance %s vanished during start; reclaiming container", db_id)
        try:
            container.stop(timeout=2)
            container.remove(force=True, v=True)
        except Exception:
            pass
        return

    internal_host = None
    internal_port = None
    if ms_network:
        try:
            container.reload()
            networks = container.attrs.get(
                "NetworkSettings", {}).get("Networks", {})
            container_ip = networks.get(ms_network, {}).get("IPAddress", "")
            if container_ip:
                internal_host = container_ip
                internal_port = container_port
                instance.setdefault("Endpoint", {})["Address"] = container_ip
                instance["Endpoint"]["Port"] = container_port
        except Exception:
            pass
    instance["_internal_address"] = internal_host
    instance["_internal_port"] = internal_port
    if _is_mysql_engine(engine):
        _ensure_mysql_compatibility(
            container.id,
            internal_host or "127.0.0.1",
            internal_port or host_port,
            master_pass,
            engine_version,
            db_id,
            engine=engine,
            database_name=db_name,
            wait_for_ready=True,
        )
        if (
            _instances.get(db_id) is not instance
            or instance.get("_docker_container_id") != container.id
        ):
            # The instance was deleted (or replaced) while we were pulling and
            # starting. Reclaim what we started: nothing else can, because the
            # periodic reaper never touches a *running* container and no record
            # references this id any more.
            try:
                container.stop(timeout=2)
                container.remove(force=True, v=True)
            except Exception:
                pass
            return
    instance["DBInstanceStatus"] = "available"
    logger.info("RDS: respawned container %s for instance %s",
                container_name, db_id)


def _get_docker():
    global _docker
    if _docker is None:
        try:
            import docker
            _docker = docker.from_env(timeout=_DOCKER_TIMEOUT)
        except Exception:
            pass
    return _docker


def _get_ministack_network(docker_client):
    """Detect the Docker network MiniStack is running on (if containerised).

    Honors MINISTACK_RDS_PUBLIC_ENDPOINT — when set, returns None so the
    DescribeDBInstances endpoint resolves to {MINISTACK_HOST, host_port}
    instead of the container-internal address (useful for remote-ministack
    deployments where external clients can't reach the Docker network).
    """
    global _ministack_network
    if RDS_PUBLIC_ENDPOINT:
        return None
    if _ministack_network is not None:
        return _ministack_network or None
    if DOCKER_NETWORK:
        _ministack_network = DOCKER_NETWORK
        logger.debug("RDS: using DOCKER_NETWORK=%s", DOCKER_NETWORK)
        return DOCKER_NETWORK
    try:
        self_container = docker_client.containers.get(
            os.environ.get("HOSTNAME", ""))
        nets = list(
            self_container.attrs["NetworkSettings"]["Networks"].keys())
        if nets:
            _ministack_network = nets[0]
            logger.debug("RDS: detected MiniStack network: %s",
                         _ministack_network)
            return _ministack_network
    except Exception:
        logger.debug("RDS: could not detect MiniStack network, "
                     "using localhost")
    _ministack_network = ""
    return None


def _wait_for_port(host, port, timeout=60):
    """Block until a TCP connection to host:port succeeds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def _is_mysql_engine(engine):
    return any(e in engine for e in ("mysql", "aurora-mysql", "mariadb"))


def _is_postgres_engine(engine):
    return any(e in engine for e in ("postgres", "aurora-postgresql"))


def _aurora_mysql_8_replication_enabled(cluster):
    """Return whether this cluster is in the native-replication spike scope."""
    return (
        cluster.get("Engine") == "aurora-mysql"
        and _mysql_community_major_minor(cluster.get("EngineVersion")) == "8.0"
    )


def _prepare_mysql_gtid_history(cluster):
    """Enable GTID history only before a cluster volume is initialized.

    New Aurora MySQL 8 clusters always start GTID-capable so they can later be
    attached as a global source without omitting pre-attach transactions. A
    persisted volume from an older MiniStack release has anonymous history;
    starting it with GTID enabled would make auto-position appear healthy
    while leaving that history absent on a fresh secondary.
    """
    if not _aurora_mysql_8_replication_enabled(cluster):
        return False
    if cluster.get("_mysql_gtid_initialized_at_creation"):
        return True
    if cluster.get("_shared_storage_initialized"):
        return False
    cluster["_mysql_gtid_initialized_at_creation"] = True
    return True


def _mysql_gtid_history_ready(cluster):
    return bool(
        not _aurora_mysql_8_replication_enabled(cluster)
        or cluster.get("_mysql_gtid_initialized_at_creation")
    )


def _mysql_replication_server_id(account_id, region, cluster_id):
    """Derive a stable, non-zero MySQL server ID from the cluster scope."""
    identity = f"{account_id}/{region}/{cluster_id}".encode()
    return int.from_bytes(hashlib.sha256(identity).digest()[:4], "big") or 1


def _global_cluster_member_for_cluster(cluster):
    global_id = cluster.get("GlobalClusterIdentifier")
    if not global_id:
        return None, None
    global_cluster = _global_clusters.get(global_id)
    if not global_cluster:
        return None, None
    member = next(
        (
            item
            for item in global_cluster.get("GlobalClusterMembers", [])
            if item.get("DBClusterArn") == cluster.get("DBClusterArn")
        ),
        None,
    )
    return global_cluster, member


def _mysql_replication_secondary(cluster):
    if not _aurora_mysql_8_replication_enabled(cluster):
        return False
    _global_cluster, member = _global_cluster_member_for_cluster(cluster)
    return bool(member and not member.get("IsWriter"))


def _resolve_global_member_cluster(member):
    parsed = _parse_rds_arn(member.get("DBClusterArn", "")) if member else None
    if not parsed:
        return None
    spec, resource_type, cluster_id = parsed
    if resource_type != "cluster":
        return None
    return _clusters.get_scoped(spec.account_id, spec.region, cluster_id)


def _global_cluster_writer_cluster(global_cluster):
    writer_member = next(
        (
            member
            for member in global_cluster.get("GlobalClusterMembers", [])
            if member.get("IsWriter")
        ),
        None,
    )
    return _resolve_global_member_cluster(writer_member)


def _sync_global_mysql_credentials(writer, global_cluster=None):
    """Keep member metadata aligned with credentials replicated from writer."""
    if global_cluster is None:
        global_cluster, writer_member = _global_cluster_member_for_cluster(writer)
    else:
        writer_member = next(
            (
                member
                for member in global_cluster.get("GlobalClusterMembers", [])
                if member.get("DBClusterArn") == writer.get("DBClusterArn")
            ),
            None,
        )
    if not global_cluster or not writer_member or not writer_member.get("IsWriter"):
        return
    for member in global_cluster.get("GlobalClusterMembers", []):
        cluster = _resolve_global_member_cluster(member)
        if cluster is None or cluster is writer:
            continue
        cluster["MasterUsername"] = writer.get("MasterUsername", "admin")
        cluster["_MasterUserPassword"] = writer.get(
            "_MasterUserPassword",
            "password",
        )
        cluster["DatabaseName"] = writer.get("DatabaseName")
        parsed = _parse_rds_arn(cluster.get("DBClusterArn", ""))
        for cluster_member in cluster.get("DBClusterMembers", []):
            instance = None
            if parsed:
                spec, resource_type, _cluster_id = parsed
                if resource_type == "cluster":
                    instance = _instances.get_scoped(
                        spec.account_id,
                        spec.region,
                        cluster_member.get("DBInstanceIdentifier"),
                    )
            if instance is not None:
                _attach_instance_to_shared_cluster(instance, cluster)


def _mysql_cluster_connection(cluster, user, password):
    import pymysql

    endpoint = cluster.get("_shared_endpoint") or {}
    host = cluster.get("_shared_internal_address") or endpoint.get("Address")
    port = cluster.get("_shared_internal_port") or endpoint.get("Port")
    if not host or not port:
        return None
    return pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        autocommit=True,
        connect_timeout=3,
    )


def _mysql_admin_connection(cluster):
    return _mysql_cluster_connection(
        cluster,
        "root",
        cluster.get("_MasterUserPassword", "password"),
    )


def _mysql_endpoint_admin_connection(host, port, password):
    import pymysql

    return pymysql.connect(
        host=host,
        port=int(port),
        user="root",
        password=password,
        autocommit=True,
        connect_timeout=3,
    )


def _ensure_mysql_compatibility(
    container_id,
    host,
    port,
    root_password,
    engine_version,
    resource_id,
    engine="aurora-mysql",
    database_name=None,
    wait_for_ready=False,
):
    """Best-effort fidelity hook shared by every MySQL-ready path."""
    _mysql_image, engine_series = _mysql_runtime_for_engine(
        engine,
        engine_version,
    )
    plugin_enabled = bool(engine_series) and iam_auth_plugin_enabled(
        engine_series,
    )
    container = None
    if wait_for_ready or plugin_enabled:
        docker_client = _get_docker()
        if docker_client and container_id:
            try:
                container = docker_client.containers.get(container_id)
            except Exception as e:
                logger.warning(
                    "RDS: failed to inspect MySQL container for %s: %s",
                    resource_id,
                    e,
                )

    def _connection():
        if wait_for_ready:
            if container is None:
                raise RuntimeError("MySQL container is unavailable")

            def _container_alive():
                try:
                    container.reload()
                    return container.status not in (
                        "exited", "dead", "removing",
                    )
                except Exception:
                    return False

            if not _wait_for_database_ready(
                host,
                port,
                engine,
                "root",
                root_password,
                database_name,
                _container_alive,
            ):
                raise RuntimeError("container exited before plugin installation")
        return _mysql_endpoint_admin_connection(host, port, root_password)

    procedures_ready = ensure_rds_compatibility_procedures(
        _connection,
        resource_id,
        engine,
        engine_series,
    )
    plugin_ready = False
    if plugin_enabled and container is not None:
        plugin_ready = ensure_iam_auth_plugin(
            container,
            _connection,
            engine_series,
            resource_id,
        )
    return procedures_ready, plugin_ready


def _mysql_replication_connection(cluster):
    return _mysql_cluster_connection(
        cluster,
        _MYSQL_CONTROL_USER,
        _MYSQL_CONTROL_PASSWORD,
    )


def _ensure_mysql_control_user(cluster):
    """Create a local, non-replicated account for replica lifecycle SQL."""
    if cluster.get("_mysql_control_user_ready"):
        return True
    cluster_id = cluster.get("DBClusterIdentifier", "unknown")
    conn = None
    cur = None
    try:
        conn = _mysql_admin_connection(cluster)
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute("SET SESSION sql_log_bin=0")
        cur.execute(
            "CREATE USER IF NOT EXISTS %s@'%%' "
            "IDENTIFIED WITH mysql_native_password BY %s",
            (_MYSQL_CONTROL_USER, _MYSQL_CONTROL_PASSWORD),
        )
        cur.execute(
            "GRANT RELOAD ON *.* TO %s@'%%'",
            (_MYSQL_CONTROL_USER,),
        )
        cur.execute(
            "GRANT CONNECTION_ADMIN, REPLICATION_SLAVE_ADMIN, "
            "SYSTEM_VARIABLES_ADMIN ON *.* TO %s@'%%'",
            (_MYSQL_CONTROL_USER,),
        )
        cur.execute("FLUSH PRIVILEGES")
        cluster["_mysql_control_user_ready"] = True
        return True
    except Exception as e:
        logger.warning(
            "RDS: failed to prepare local MySQL control account for %s: %s",
            cluster_id,
            e,
        )
        return False
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def _ensure_mysql_replication_user(cluster):
    """Ensure the internal replication account exists on a writer cluster."""
    cluster_id = cluster.get("DBClusterIdentifier", "unknown")
    conn = None
    cur = None
    try:
        conn = _mysql_admin_connection(cluster)
        if conn is None:
            return False
        cur = conn.cursor()
        cur.execute(
            "CREATE USER IF NOT EXISTS %s@'%%' "
            "IDENTIFIED WITH mysql_native_password BY %s",
            (_MYSQL_REPLICATION_USER, _MYSQL_REPLICATION_PASSWORD),
        )
        cur.execute(
            "GRANT REPLICATION SLAVE ON *.* TO %s@'%%'",
            (_MYSQL_REPLICATION_USER,),
        )
        cur.execute("FLUSH PRIVILEGES")
        return True
    except Exception as e:
        logger.warning(
            "RDS: failed to prepare MySQL replication source %s: %s",
            cluster_id,
            e,
        )
        return False
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def _configure_mysql_replication(cluster_id, cluster):
    """Configure an Aurora MySQL 8 global member as source or replica.

    Returns True when configuration is complete, False when it should be
    retried, and None when replication does not apply to this cluster.
    """
    if not _aurora_mysql_8_replication_enabled(cluster):
        return None

    global_cluster, member = _global_cluster_member_for_cluster(cluster)
    if not global_cluster or not member:
        return None
    if not cluster.get("_shared_container_ready"):
        return False
    if not _mysql_gtid_history_ready(cluster):
        cluster["_mysql_replication_blocked_reason"] = "legacy-non-gtid-volume"
        cluster["_shared_container_ready"] = False
        _set_cluster_members_status(cluster, "failed")
        logger.error(
            "RDS: refusing MySQL replication for %s because its initialized "
            "volume predates GTID-at-creation tracking",
            cluster_id,
        )
        return None
    if not _ensure_mysql_control_user(cluster):
        return False
    cluster.pop("_mysql_replication_blocked_reason", None)

    if member.get("IsWriter"):
        if not _ensure_mysql_replication_user(cluster):
            return False
        for secondary_member in global_cluster.get("GlobalClusterMembers", []):
            if secondary_member.get("IsWriter"):
                continue
            secondary = _resolve_global_member_cluster(secondary_member)
            if secondary and secondary.get("_shared_container_ready"):
                secondary_id = secondary.get("DBClusterIdentifier", "unknown")
                if _configure_mysql_replication(secondary_id, secondary) is False:
                    _schedule_mysql_replication_retry(secondary_id, secondary)
        return True

    writer_member = next(
        (
            item
            for item in global_cluster.get("GlobalClusterMembers", [])
            if item.get("IsWriter")
        ),
        None,
    )
    writer = _resolve_global_member_cluster(writer_member)
    if (
        not writer
        or not _aurora_mysql_8_replication_enabled(writer)
        or not writer.get("_shared_container_ready")
        or not writer.get("_shared_internal_address")
        or not writer.get("_shared_internal_port")
        or not cluster.get("_shared_internal_address")
    ):
        return False
    if not _ensure_mysql_replication_user(writer):
        return False

    conn = None
    cur = None
    try:
        conn = _mysql_replication_connection(cluster)
        if conn is None:
            return False
        cur = conn.cursor()
        try:
            cur.execute("STOP REPLICA")
        except Exception as e:
            logger.debug(
                "RDS: STOP REPLICA was a no-op for %s: %s",
                cluster_id,
                e,
            )
        if cluster.get("_mysql_replication_reset_pending"):
            # RESET REPLICA ALL clears stale connection/applier repositories
            # and relay logs but deliberately preserves gtid_executed. Within
            # the source's configured retention window, auto-position can
            # request every transaction not yet applied before the restart.
            cur.execute("RESET REPLICA ALL")
        cur.execute(
            "CHANGE REPLICATION SOURCE TO "
            "SOURCE_HOST=%s, SOURCE_PORT=%s, SOURCE_USER=%s, "
            "SOURCE_PASSWORD=%s, SOURCE_AUTO_POSITION=1, "
            "GET_SOURCE_PUBLIC_KEY=1",
            (
                writer["_shared_internal_address"],
                int(writer["_shared_internal_port"]),
                _MYSQL_REPLICATION_USER,
                _MYSQL_REPLICATION_PASSWORD,
            ),
        )
        cur.execute("START REPLICA")
        cur.execute("SET GLOBAL super_read_only=ON")
    except Exception as e:
        logger.warning(
            "RDS: failed to configure MySQL replication for %s: %s",
            cluster_id,
            e,
        )
        return False
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    cluster["_mysql_replication_source_arn"] = writer.get("DBClusterArn")
    cluster.pop("_mysql_replication_reset_pending", None)
    cluster.pop("_mysql_replication_retry_marker", None)
    # If MiniStack restarted or a detach statement failed after the channel
    # had been changed, successful reconfiguration is the rollback/repair
    # point.  Global membership remains authoritative until RemoveFromGlobal
    # Cluster commits its metadata update.
    cluster.pop("_mysql_replication_detach_state", None)
    logger.info(
        "RDS: configured MySQL replication for %s from %s",
        cluster_id,
        writer.get("DBClusterIdentifier", "unknown"),
    )
    return True


def _detach_mysql_replication(cluster_id, cluster):
    """Stop a global secondary and make its data plane writable."""
    if not _mysql_replication_secondary(cluster):
        return True
    if not cluster.get("_shared_storage_initialized"):
        return True
    if not cluster.get("_shared_container_ready"):
        return False
    if not _ensure_mysql_control_user(cluster):
        return False

    # Persist the transition before destructive SQL.  A failed request keeps
    # global membership intact; immediate rollback (or the normal restore-time
    # configure path after a process restart) relinks the channel and clears
    # this marker.  Successful detach leaves the marker until the caller
    # commits the global/member metadata update under the same lifecycle lock.
    state = cluster.setdefault("_mysql_replication_detach_state", "requested")
    conn = None
    cur = None
    try:
        conn = _mysql_replication_connection(cluster)
        if conn is None:
            return False
        cur = conn.cursor()
        if state == "requested":
            cur.execute("STOP REPLICA")
            cluster["_mysql_replication_detach_state"] = "stopped"
            state = "stopped"
        if state in ("stopped", "resetting"):
            # ``resetting`` is written first so a crash between MySQL applying
            # RESET and Python publishing the next state is recoverable.
            cluster["_mysql_replication_detach_state"] = "resetting"
            cur.execute("RESET REPLICA ALL")
            cluster["_mysql_replication_detach_state"] = "reset"
            state = "reset"
        if state == "reset":
            cur.execute("SET GLOBAL super_read_only=OFF")
    except Exception as e:
        logger.warning(
            "RDS: failed to detach MySQL replication for %s: %s",
            cluster_id,
            e,
        )
        # Best-effort atomic rollback: if the connection is still usable (or a
        # fresh one can be opened), restore auto-position and read-only mode.
        # If that is temporarily impossible, the durable state remains and the
        # normal readiness/configuration path repairs it on retry or restart.
        if _configure_mysql_replication(cluster_id, cluster) is True:
            logger.info(
                "RDS: restored MySQL replication for %s after detach failed",
                cluster_id,
            )
        else:
            # Reconfiguration may fail after recreating or starting the
            # channel.  Do not trust the detach stage recorded before that
            # partial rollback: the next detach attempt must conservatively
            # stop and reset any channel that may now exist.
            cluster["_mysql_replication_detach_state"] = "requested"
        return False
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    return True


def _clear_mysql_replication_metadata(cluster):
    """Commit the metadata half of a successful global-secondary detach."""
    for field in (
        "_mysql_replication_source_arn",
        "_mysql_replication_reset_pending",
        "_mysql_replication_retry_marker",
        "_mysql_replication_blocked_reason",
        "_mysql_replication_detach_state",
        "_mysql_headless_applier_required",
    ):
        cluster.pop(field, None)


def _schedule_mysql_replication_retry(cluster_id, cluster):
    """Retry a deferred cross-region link without blocking RDS readiness."""
    parsed = _parse_rds_arn(cluster.get("DBClusterArn", ""))
    if not parsed:
        return
    spec, resource_type, parsed_cluster_id = parsed
    if resource_type != "cluster":
        return
    epoch = cluster.get("_shared_container_epoch")
    marker = (epoch, cluster.get("GlobalClusterIdentifier"))
    if cluster.get("_mysql_replication_retry_marker") == marker:
        return
    cluster["_mysql_replication_retry_marker"] = marker
    ctx = contextvars.copy_context()

    def _retry():
        for _attempt in range(_MYSQL_REPLICATION_RETRY_ATTEMPTS):
            time.sleep(_MYSQL_REPLICATION_RETRY_INTERVAL)
            with _shared_container_lock:
                current = _clusters.get_scoped(
                    spec.account_id,
                    spec.region,
                    parsed_cluster_id,
                )
                if (
                    current is not cluster
                    or current.get("_shared_container_epoch") != epoch
                    or current.get("_mysql_replication_retry_marker") != marker
                ):
                    return
                result = _configure_mysql_replication(cluster_id, current)
                if result is not False:
                    current.pop("_mysql_replication_retry_marker", None)
                    return
        with _shared_container_lock:
            current = _clusters.get_scoped(
                spec.account_id,
                spec.region,
                parsed_cluster_id,
            )
            if (
                current is cluster
                and current.get("_shared_container_epoch") == epoch
                and current.get("_mysql_replication_retry_marker") == marker
            ):
                current.pop("_mysql_replication_retry_marker", None)
        logger.warning(
            "RDS: MySQL replication for %s did not become configurable "
            "after %s attempts",
            cluster_id,
            _MYSQL_REPLICATION_RETRY_ATTEMPTS,
        )

    threading.Thread(target=ctx.run, args=(_retry,), daemon=True).start()


def _configure_or_defer_mysql_replication(cluster_id, cluster):
    if _configure_mysql_replication(cluster_id, cluster) is False:
        _schedule_mysql_replication_retry(cluster_id, cluster)


def _grant_mysql_master_user_privileges(host, port, master_user, master_pass, db_id):
    """Grant the emulated MySQL master user AWS/RDS-like admin privileges."""
    try:
        import pymysql
        conn = pymysql.connect(
            host=host, port=int(port), user="root",
            password=master_pass, autocommit=True)
        cur = conn.cursor()
        cur.execute(
            "CREATE USER IF NOT EXISTS %s@'%%' IDENTIFIED BY %s",
            (master_user, master_pass),
        )
        cur.execute(
            "GRANT ALL PRIVILEGES ON *.* TO %s@'%%' WITH GRANT OPTION",
            (master_user,),
        )
        for privilege in ("APPLICATION_PASSWORD_ADMIN",):
            try:
                cur.execute(f"GRANT {privilege} ON *.* TO %s@'%%'", (master_user,))
            except Exception as e:
                logger.debug(
                    "RDS: MySQL privilege %s unsupported for %s: %s",
                    privilege, db_id, e)
        cur.execute("FLUSH PRIVILEGES")
        cur.close()
        conn.close()
        logger.info("RDS: granted MySQL master privileges for %s", db_id)
    except Exception as e:
        logger.warning(
            "RDS: failed to grant MySQL master privileges for %s: %s",
            db_id, e)


def _try_database_connect(host, port, engine, user, password, db_name):
    """Single auth + query probe attempt.

    TCP readiness alone is not enough for MySQL/Postgres images, and MySQL can
    accept authenticated connections before it can reliably execute setup SQL.
    When the DB driver isn't installed (lightweight image), fall back to TCP.
    """
    def _execute_probe(conn):
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
        finally:
            cur.close()

    try:
        if _is_mysql_engine(engine):
            try:
                import pymysql
            except ImportError:
                return _wait_for_port(host, port, timeout=1)
            conn = pymysql.connect(
                host=host, port=int(port), user=user,
                password=password, database=db_name or None,
                connect_timeout=2, read_timeout=2, write_timeout=2,
                autocommit=True)
            try:
                _execute_probe(conn)
            finally:
                conn.close()
        elif _is_postgres_engine(engine):
            try:
                import psycopg2
            except ImportError:
                return _wait_for_port(host, port, timeout=1)
            conn = psycopg2.connect(
                host=host, port=int(port), user=user,
                password=password, dbname=db_name or "postgres",
                connect_timeout=2)
            try:
                _execute_probe(conn)
            finally:
                conn.close()
        else:
            return _wait_for_port(host, port, timeout=1)
        return True
    except Exception as e:
        # Distinguish *permanent* auth failures from transient boot-time errors.
        # A transient failure (server still starting, socket refused, etc.) is
        # expected during the readiness loop. A permanent auth failure means
        # the container's image is configured with a different password than
        # the one ministack handed it — the loop would spin forever and the
        # user would see nothing. Surface that case at WARNING level with a
        # concrete hint so it shows up in ministack logs.
        msg = str(e)
        is_auth_denied = (
            # pymysql: OperationalError with MySQL error code 1045
            (getattr(e, "args", None) and isinstance(e.args[0], int) and e.args[0] == 1045)
            # psycopg2 and generic driver messages
            or "password authentication failed" in msg.lower()
            or "access denied for user" in msg.lower()
        )
        if is_auth_denied:
            logger.warning(
                "RDS: authentication denied probing %s:%s — the container's "
                "image is configured with a different password than ministack "
                "passed at start-up. The instance will stay in `creating` until "
                "the container exits. Driver error: %s",
                host, port, msg,
            )
        else:
            logger.debug("RDS: readiness probe transient failure: %s", e)
        return False


def _wait_for_database_ready(host, port, engine, user, password, db_name,
                             is_container_alive):
    """Poll until the database accepts an authenticated connection. No wall
    clock — real AWS `CreateDBInstance` has no caller-visible timeout, so
    neither do we. The loop terminates on success or when the backing
    container stops being alive (mirrors how real RDS flips an instance to
    `failed` based on hardware state, not a fixed deadline).
    """
    while True:
        if not is_container_alive():
            return False
        if _try_database_connect(host, port, engine, user, password, db_name):
            return True
        time.sleep(0.5)


def _refresh_cluster_status(cluster_id):
    if not cluster_id:
        return
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return
    if cluster.get("Status") in ("stopping", "stopped", "deleting"):
        return
    member_ids = {
        m.get("DBInstanceIdentifier")
        for m in cluster.get("DBClusterMembers", [])
        if m.get("DBInstanceIdentifier")
    }
    if any(
        inst.get("DBInstanceIdentifier") in member_ids
        # A member in ``stopped`` was parked deliberately (a failed
        # StopDBCluster's rollback could not restart its container); it is
        # not converging toward available, so it must not hold the cluster
        # in ``creating`` — that would reject the StopDBCluster retry that
        # repairs it.
        and inst.get("DBInstanceStatus") not in ("available", "stopped")
        for inst in _instances.values()
    ):
        cluster["Status"] = "creating"
    else:
        cluster["Status"] = "available"
_port_lock = threading.Lock()


def _is_host_port_free(port: int) -> bool:
    """Probe that no other process holds host TCP `port`. Best-effort
    pre-flight check so respawn can pick a different port instead of
    failing at `docker run` with `port is already allocated` (#692
    follow-up). There is a small TOCTOU window between probe and
    `containers.run`, but it closes the common case of stale or
    user-process bindings."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("0.0.0.0", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def _next_port():
    """Return the next free host port for an RDS container. Increments
    the persisted counter, but skips ports that are already bound on the
    host (e.g. by another ministack instance or the user's own services).
    Caps probing to avoid infinite loops if the entire upper range is
    saturated — in that pathological case the caller will get a port and
    likely fail at `docker run`, but we won't spin forever."""
    with _port_lock:
        for _ in range(200):
            port = _port_counter[0]
            _port_counter[0] += 1
            if _is_host_port_free(port):
                return port
        # Saturated — return the next counter value and let docker surface
        # whatever it surfaces. Better than a silent hang.
        port = _port_counter[0]
        _port_counter[0] += 1
        return port


# ---------------------------------------------------------------------------
# Request routing
# ---------------------------------------------------------------------------

def _json_key_to_query_param_name(key: str) -> str:
    """Map JSON / Smithy body keys to Query-API parameter names."""
    lk = key.lower()
    if lk == "dbinstanceidentifier":
        return "DBInstanceIdentifier"
    if lk == "filters":
        return "Filters"
    return key


def _flatten_json_scalar(params, key, val):
    if isinstance(val, bool):
        params[key] = ["true" if val else "false"]
    elif isinstance(val, (int, float)):
        params[key] = [str(val)]
    elif isinstance(val, str):
        params[key] = [val]
    else:
        return False
    return True


def _flatten_json_struct(params, prefix, data):
    """Flatten a nested JSON object to Query-API ``Prefix.Member`` params."""
    for key, val in data.items():
        if val is None:
            continue
        qkey = f"{prefix}.{key}"
        if _flatten_json_scalar(params, qkey, val):
            continue
        if isinstance(val, list):
            _flatten_json_list(params, qkey, val)
        elif isinstance(val, dict):
            _flatten_json_struct(params, qkey, val)


def _flatten_json_list(params, prefix, values):
    """Flatten a JSON array to the Query-API ``Prefix.member.N`` form."""
    for i, item in enumerate(values, 1):
        base = f"{prefix}.member.{i}"
        if item is None:
            continue
        if _flatten_json_scalar(params, base, item):
            continue
        if isinstance(item, dict):
            _flatten_json_struct(params, base, item)


def _flatten_json_request_params(params, data):
    """Merge SigV4 JSON (``application/x-amz-json-1.*``) bodies into query-style params.

    Botocore's JSON protocol sends a JSON object; our handlers expect the same
    keys as the Query API with list-shaped values (``_p`` reads ``[0]``).
    """
    if not isinstance(data, dict):
        return
    for key, val in data.items():
        if val is None:
            continue
        qkey = _json_key_to_query_param_name(key)
        if _flatten_json_scalar(params, qkey, val):
            continue
        if isinstance(val, list) and qkey == "Filters":
            for i, f in enumerate(val, 1):
                if not isinstance(f, dict):
                    continue
                name = f.get("Name") or f.get("name")
                if not name:
                    continue
                params[f"Filters.member.{i}.Name"] = [name]
                values = f.get("Values") or f.get("values") or []
                for j, v in enumerate(values, 1):
                    params[f"Filters.member.{i}.Values.member.{j}"] = [str(v)]
        elif isinstance(val, list):
            _flatten_json_list(params, qkey, val)
        elif isinstance(val, dict):
            _flatten_json_struct(params, qkey, val)


def _handle_request_sync(method, path, headers, body, query_params):
    params = dict(query_params)
    if method == "POST" and body:
        raw = body if isinstance(body, str) else body.decode("utf-8-sig", errors="replace")
        stripped = raw.lstrip()
        ct = (headers.get("content-type") or headers.get("Content-Type") or "").lower()
        merged_json = False
        # Prefer JSON when it looks like JSON, or when the client declares AWS/JSON.
        if stripped.startswith("{") or ("json" in ct and stripped):
            try:
                payload = json.loads(stripped)
                if isinstance(payload, dict):
                    _flatten_json_request_params(params, payload)
                    merged_json = True
            except json.JSONDecodeError:
                pass
        if not merged_json:
            form_params = parse_qs(raw)
            for k, v in form_params.items():
                params[k] = v

    target = headers.get("x-amz-target", "") or headers.get("X-Amz-Target", "")
    if target:
        action = target.split(".")[-1]
    else:
        action = _p(params, "Action")

    handler = _ACTION_MAP.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown RDS action: {action}", 400)
    return handler(params)


# ---------------------------------------------------------------------------
# Instance resolution helpers
# ---------------------------------------------------------------------------

def _parse_rds_arn(value):
    try:
        spec = parse_arn(value)
    except ArnParseError:
        return None
    if spec.service != "rds":
        return None
    resource_type, sep, resource_id = spec.resource.partition(":")
    if not sep or not resource_type or not resource_id:
        return None
    return spec, resource_type, resource_id


def _regional_get(store, identifier, resource_type):
    parsed = _parse_rds_arn(identifier)
    if parsed:
        spec, parsed_type, resource_id = parsed
        if parsed_type != resource_type:
            return None
        if spec.account_id != get_account_id():
            return None
        return store.get_scoped(spec.account_id, spec.region, resource_id)
    return store.get(identifier)


def _request_region_get(store, identifier, resource_type):
    parsed = _parse_rds_arn(identifier)
    if parsed:
        spec, parsed_type, resource_id = parsed
        if parsed_type != resource_type:
            return None
        if spec.account_id != get_account_id():
            return None
        if spec.region != get_region():
            return None
        return store.get(resource_id)
    return store.get(identifier)


def _request_region_identifier(identifier, resource_type, store=None, arn_key=None):
    resource_id = _request_region_resource_identifier(identifier, resource_type)
    if resource_id is None:
        return None
    if store is not None and arn_key is not None and _parse_rds_arn(identifier):
        resource = store.get(resource_id)
        if not resource or resource.get(arn_key) != identifier:
            return None
    return resource_id


def _request_region_resource_identifier(identifier, resource_type):
    parsed = _parse_rds_arn(identifier)
    if not parsed:
        return identifier
    spec, parsed_type, resource_id = parsed
    if (
        parsed_type == resource_type
        and spec.account_id == get_account_id()
        and spec.region == get_region()
    ):
        return resource_id
    return None


def _record_arn_in_request_scope(record, arn_key):
    parsed = _parse_rds_arn(record.get(arn_key, ""))
    if not parsed:
        return False
    spec, _resource_type, _resource_id = parsed
    return spec.account_id == get_account_id() and spec.region == get_region()


def _same_account_foreign_region_arn(identifier, resource_type):
    parsed = _parse_rds_arn(identifier)
    if not parsed:
        return None
    spec, parsed_type, _ = parsed
    if parsed_type != resource_type:
        return None
    if spec.account_id != get_account_id():
        return None
    if spec.region == get_region():
        return None
    return spec


def _request_scope_mismatch_arn(identifier, resource_type):
    parsed = _parse_rds_arn(identifier)
    if not parsed:
        return None
    spec, parsed_type, _ = parsed
    if parsed_type != resource_type:
        return None
    if spec.account_id != get_account_id() or spec.region != get_region():
        return spec
    return None


def _invalid_region_arn_error(identifier, parameter_name):
    spec = _same_account_foreign_region_arn(identifier, "cluster")
    if not spec:
        return None
    return _error(
        "InvalidParameterValue",
        f"The provided ARN ({identifier}) is invalid for this parameter "
        f"({parameter_name}). Expected region = {get_region()}, "
        f"actual region = {spec.region}",
        400,
    )


def _invalid_db_instance_identifier_error(identifier, parameter_name="DBInstanceIdentifier"):
    if not _request_scope_mismatch_arn(identifier, "db"):
        return None
    return _error(
        "InvalidParameterValue",
        f"The parameter {parameter_name} is not a valid identifier because it is longer than 63 characters.",
        400,
    )


def _invalid_cluster_identifier_error(identifier):
    if not _same_account_foreign_region_arn(identifier, "cluster"):
        return None
    return _error(
        "InvalidParameterValue",
        f"Invalid database cluster identifier:  {identifier}",
        400,
    )


def _resource_not_found_error_for_arn(identifier):
    if not _same_account_foreign_region_arn(identifier, "cluster"):
        return None
    return _error("ResourceNotFoundFault", f"DB cluster ARN {identifier} wasn't found.", 404)


def _resolve_cluster(cluster_id):
    """Look up a DB cluster by identifier in the request Region or by ARN Region.

    Use this only for data-plane or global-topology operations whose AWS
    semantics intentionally follow same-account member ARNs across Regions.
    Normal regional control-plane APIs should use
    ``_resolve_cluster_in_request_region``.
    """
    return _regional_get(_clusters, cluster_id, "cluster")


def _resolve_cluster_in_request_region(cluster_id):
    """Look up a DB cluster only in the request Region."""
    return _request_region_get(_clusters, cluster_id, "cluster")


def _resolve_global_cluster(global_id):
    """Look up a global cluster by identifier."""
    if _parse_rds_arn(global_id):
        return None
    return _global_clusters.get(global_id)


def _global_cluster_member(cluster, is_writer):
    return {
        "DBClusterArn": cluster["DBClusterArn"],
        "Readers": [],
        "IsWriter": is_writer,
        "GlobalWriteForwardingStatus": cluster.get("GlobalWriteForwardingStatus", "disabled"),
        "SynchronizationStatus": "connected",
    }


def _global_cluster_member_in_request_region(global_cluster):
    for member in global_cluster.get("GlobalClusterMembers", []):
        parsed = _parse_rds_arn(member.get("DBClusterArn", ""))
        if not parsed:
            continue
        spec, resource_type, _resource_id = parsed
        if (
            resource_type == "cluster"
            and spec.account_id == get_account_id()
            and spec.region == get_region()
        ):
            return member
    return None


def _refresh_global_cluster_readers(global_cluster):
    members = global_cluster.get("GlobalClusterMembers", [])
    reader_arns = [m["DBClusterArn"] for m in members if not m.get("IsWriter")]
    for member in members:
        member["Readers"] = reader_arns if member.get("IsWriter") else []


def _set_global_cluster_writer(global_cluster, target_member):
    for member in global_cluster.get("GlobalClusterMembers", []):
        member["IsWriter"] = member["DBClusterArn"] == target_member["DBClusterArn"]
    _refresh_global_cluster_readers(global_cluster)


def _attach_cluster_to_global(global_cluster, cluster, is_writer):
    members = [
        m for m in global_cluster.setdefault("GlobalClusterMembers", [])
        if m.get("DBClusterArn") != cluster["DBClusterArn"]
    ]
    members.append(_global_cluster_member(cluster, is_writer))
    global_cluster["GlobalClusterMembers"] = members
    _refresh_global_cluster_readers(global_cluster)
    cluster["GlobalClusterIdentifier"] = global_cluster["GlobalClusterIdentifier"]
    cluster["GlobalWriteForwardingStatus"] = "disabled"


def _resolve_instance(db_id):
    """Look up an instance by DBInstanceIdentifier or DbiResourceId.

    AWS accepts either value for the DBInstanceIdentifier parameter in
    DescribeDBInstances and related APIs.
    """
    inst = _request_region_get(_instances, db_id, "db")
    if inst:
        return inst
    if isinstance(db_id, str) and db_id.startswith("db-"):
        for inst in _instances.values():
            if inst.get("DbiResourceId") == db_id:
                return inst
    return None


def _set_cluster_members_status(cluster, status):
    for member in cluster.get("DBClusterMembers", []):
        instance = _instances.get(member.get("DBInstanceIdentifier"))
        if instance is not None:
            instance["DBInstanceStatus"] = status


def _attach_instance_to_shared_cluster(instance, cluster):
    if instance.get("_pg_standby"):
        # A replicating reader owns its container and endpoint (#1325);
        # aliasing would clobber them with the writer's.
        return
    endpoint = cluster.get("_shared_endpoint")
    if not endpoint:
        return
    instance["Endpoint"] = copy.deepcopy(endpoint)
    instance["_HostPort"] = cluster.get("_shared_host_port")
    instance["_docker_container_id"] = cluster.get("_shared_container_id")
    instance["_docker_volume_name"] = None
    instance["_internal_address"] = cluster.get("_shared_internal_address")
    instance["_internal_port"] = cluster.get("_shared_internal_port")
    instance["_shared_cluster_id"] = cluster["DBClusterIdentifier"]
    instance["MasterUsername"] = cluster.get(
        "MasterUsername",
        instance.get("MasterUsername", "admin"),
    )
    instance["_MasterUserPassword"] = cluster.get(
        "_MasterUserPassword",
        instance.get("_MasterUserPassword", "password"),
    )
    instance["DBName"] = (
        cluster.get("DatabaseName")
        or instance.get("DBName")
        or "mydb"
    )


def _cluster_reader_endpoint(cluster):
    """The endpoint a cluster's ``ReaderEndpoint`` should resolve to.

    When a member owns a replicating reader container (#1325) and it is
    available, the reader endpoint resolves there and is genuinely
    read-only. Otherwise it falls back to the writer's shared endpoint —
    matching real Aurora, where the reader endpoint follows the writer in
    a single-instance cluster, but read/write because it resolves to the
    same database process as the writer.

    A DBCluster advertises a single ``Port`` (the writer's, applied by
    ``_sync_cluster_endpoints``), so a standby only reachable on a
    different port — the host-port fallback when its network IP lookup
    failed — must not win the ReaderEndpoint address: pairing its Address
    with the writer's Port would be unreachable.
    """
    shared = cluster.get("_shared_endpoint")
    for member in _cluster_member_instances(cluster):
        if (
            member.get("_pg_standby")
            and member.get("DBInstanceStatus") == "available"
            and member.get("Endpoint")
        ):
            endpoint = member["Endpoint"]
            if shared and endpoint.get("Port") != shared.get("Port"):
                continue
            return endpoint
    return shared


def _sync_cluster_endpoints(cluster):
    """Point the Aurora writer and reader endpoints at cluster compute.

    The writer endpoint resolves to the cluster-owned shared container; the
    reader endpoint resolves to whatever ``_cluster_reader_endpoint``
    selects (currently the same container — see its docstring).
    """
    endpoint = cluster.get("_shared_endpoint")
    if not endpoint:
        return
    reader_endpoint = _cluster_reader_endpoint(cluster) or endpoint
    cluster["Endpoint"] = endpoint.get("Address", cluster.get("Endpoint", ""))
    cluster["ReaderEndpoint"] = reader_endpoint.get(
        "Address", cluster.get("ReaderEndpoint", ""),
    )
    cluster["Port"] = int(endpoint.get("Port", cluster.get("Port", 0)))


def _register_instance_in_cluster(instance):
    """Append instance to parent cluster ``DBClusterMembers`` (Aurora parity)."""
    cid = instance.get("DBClusterIdentifier")
    if not cid:
        return
    cluster = _resolve_cluster_in_request_region(cid)
    if not cluster:
        return
    members = cluster.setdefault("DBClusterMembers", [])
    db_id = instance["DBInstanceIdentifier"]
    members[:] = [m for m in members if m.get("DBInstanceIdentifier") != db_id]
    any_writer = any(m.get("IsClusterWriter") for m in members)
    is_writer = not any_writer
    members.append({
        "DBInstanceIdentifier": db_id,
        "IsClusterWriter": is_writer,
        "PromotionTier": int(instance.get("PromotionTier", 1)),
    })
    cluster.pop("_mysql_headless_applier_required", None)
    _sync_cluster_endpoints(cluster)
    _refresh_cluster_status(cid)


def _unregister_instance_from_clusters(db_id):
    """Remove instance from any cluster member list."""
    for cl in _clusters.values():
        mem = cl.get("DBClusterMembers") or []
        cl["DBClusterMembers"] = [m for m in mem if m.get("DBInstanceIdentifier") != db_id]
        _sync_cluster_endpoints(cl)
        _refresh_cluster_status(cl.get("DBClusterIdentifier"))


# ---------------------------------------------------------------------------
# DB Instances
# ---------------------------------------------------------------------------

def _create_db_instance(p):
    return _create_db_instance_impl(p)


def _create_db_instance_impl(p):
    db_id = _p(p, "DBInstanceIdentifier")
    if not db_id:
        return _error("MissingParameter", "DBInstanceIdentifier is required", 400)
    if db_id in _instances:
        return _error("DBInstanceAlreadyExists", f"DB instance {db_id} already exists", 400)

    engine = _p(p, "Engine") or "postgres"
    explicit_engine_version = _p(p, "EngineVersion")
    engine_version_error = _unsupported_aurora_engine_version_error(engine, explicit_engine_version)
    if engine_version_error:
        return engine_version_error
    engine_version = explicit_engine_version or _default_engine_version(engine)
    db_class = _p(p, "DBInstanceClass") or "db.t3.micro"
    master_user = _p(p, "MasterUsername") or "admin"
    master_pass = _p(p, "MasterUserPassword") or "password"
    db_name = _p(p, "DBName") or ""
    port = int(_p(p, "Port") or _default_port(engine))

    # Inherit credentials from cluster when instance is a cluster member.
    cluster_id_param = _p(p, "DBClusterIdentifier")
    parent = _resolve_cluster_in_request_region(cluster_id_param) if cluster_id_param else None
    if parent:
        if parent.get("_shared_legacy_migration_in_progress"):
            return _error(
                "InvalidDBClusterStateFault",
                "Cannot add a DB instance while legacy shared-storage "
                "migration is in progress.",
                400,
            )
        if parent.get("_shared_legacy_migration_blocked"):
            return _error(
                "InvalidDBClusterStateFault",
                "Cannot add a DB instance while legacy member storage "
                "migration is blocked.",
                400,
            )
        if parent.get("Status") == "stopped":
            # Real AWS rejects instance additions to a stopped cluster; the
            # cluster must be started first. Without this guard the new
            # member would land as ``creating`` with no readiness worker to
            # ever complete it.
            return _error(
                "InvalidDBClusterStateFault",
                f"DbCluster {parent['DBClusterIdentifier']} is in stopped "
                "state but expected it to be one of available.",
                400,
            )
        cluster_id_param = parent["DBClusterIdentifier"]
        engine = parent.get("Engine", engine)
        engine_version = parent.get("EngineVersion", engine_version)
        port = int(parent.get("Port") or _default_port(engine))
        # Aurora credentials and the initial database belong to the cluster,
        # not individual members. Ignore conflicting member request fields so
        # readiness always authenticates against the shared process.
        master_user = parent.get("MasterUsername", master_user)
        master_pass = parent.get("_MasterUserPassword", master_pass)
        db_name = parent.get("DatabaseName") or "mydb"
    elif _parse_rds_arn(cluster_id_param):
        wrong_region = _invalid_cluster_identifier_error(cluster_id_param)
        if wrong_region:
            return wrong_region
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id_param} not found.", 404)
    if not db_name:
        db_name = "mydb"
    allocated_storage = int(_p(p, "AllocatedStorage") or "20")
    storage_type = _p(p, "StorageType") or "gp2"
    subnet_group_name = _p(p, "DBSubnetGroupName") or "default"
    explicit_pg = _p(p, "DBParameterGroupName")
    # Validate every request field before shared compute can start or restart.
    # Otherwise an invalid member request can make an empty cluster reachable
    # even though no member record is ultimately created.
    if (
        explicit_pg
        and not explicit_pg.startswith("default.")
        and explicit_pg not in _param_groups
    ):
        return _error(
            "DBParameterGroupNotFound",
            f"DBParameterGroup {explicit_pg} not found.",
            404,
        )
    param_group_name = (
        explicit_pg
        or f"default.{engine}{engine_version.split('.')[0]}"
    )

    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:db:{db_id}"
    dbi_resource_id = f"db-{new_uuid().replace('-', '')[:20].upper()}"
    endpoint_host = _MINISTACK_HOST
    endpoint_port = port
    host_port = None
    docker_container_id = None
    docker_volume_name = None
    internal_host = None
    internal_port = None
    real_container_started = False
    readiness_host = None
    readiness_port = None
    readiness_master_pass = master_pass
    ms_network = None

    deferred_container_start = False
    docker_client = _get_docker()
    reader_launch = None
    if (
        parent
        and parent.get("DBClusterMembers")
        and _pg_cluster_replication_enabled(parent)
    ):
        # Second and later members of a flag-enabled Aurora PostgreSQL
        # cluster get their own hot-standby container instead of aliasing
        # the writer's (#1325). Returns None when that cannot run here,
        # falling back to the aliasing branch below.
        reader_launch = _start_pg_reader_container(db_id, parent)
    if parent and reader_launch:
        start_result = reader_launch
        endpoint_host = reader_launch.get("endpoint_host", endpoint_host)
        endpoint_port = int(reader_launch.get("endpoint_port", endpoint_port))
        host_port = reader_launch.get("host_port")
        docker_container_id = reader_launch.get("container_id")
        docker_volume_name = reader_launch.get("volume_name")
        internal_host = reader_launch.get("internal_host")
        internal_port = reader_launch.get("internal_port")
        real_container_started = bool(reader_launch.get("started"))
        readiness_host = reader_launch.get("readiness_host")
        readiness_port = reader_launch.get("readiness_port")
        ms_network = _get_ministack_network(docker_client)
    elif parent:
        pending_rotation = parent.get("_pending_master_password_rotation")
        if pending_rotation:
            readiness_master_pass = pending_rotation["old_password"]
        # RDS action dispatch is synchronous and await-free, so this check and
        # the shared metadata update cannot interleave with another create.
        resume_control_plane_only = (
            not docker_client
            and not parent.get("DBClusterMembers")
            and not parent.get("_shared_container_ready", True)
        )
        has_failed_shared_member = any(
            (_instances.get(member.get("DBInstanceIdentifier")) or {}).get(
                "DBInstanceStatus",
            ) == "failed"
            for member in parent.get("DBClusterMembers", [])
            if member.get("DBInstanceIdentifier")
        )
        restart_unhealthy_container = (
            docker_client
            and parent.get("_shared_container_id")
            and not parent.get("_shared_container_ready", True)
            and (
                not parent.get("DBClusterMembers")
                or has_failed_shared_member
            )
        )
        if resume_control_plane_only:
            parent["_shared_container_ready"] = True
            start_result = {"started": False, "failed": False}
        elif restart_unhealthy_container:
            start_result = _restart_cluster_shared_container(
                cluster_id_param,
                parent,
            )
            if start_result.get("failed"):
                # A preserved container can become unrestartable (for example,
                # its old host port may have been claimed). Recreate only the
                # compute layer; the cluster-owned named volume retains data.
                start_result = _start_cluster_shared_container(
                    cluster_id_param,
                    parent,
                    remove_stale=True,
                )
        else:
            needs_shared_start = (
                not parent.get("_shared_endpoint")
                or (docker_client and not parent.get("_shared_container_id"))
            )
            start_result = (
                _start_cluster_shared_container(
                    cluster_id_param,
                    parent,
                    remove_stale=bool(parent.get("_shared_endpoint")),
                )
                if needs_shared_start
                else {"started": False, "failed": False}
            )
        shared_endpoint = parent.get("_shared_endpoint") or {}
        endpoint_host = shared_endpoint.get("Address", endpoint_host)
        endpoint_port = int(shared_endpoint.get("Port", endpoint_port))
        host_port = parent.get("_shared_host_port")
        docker_container_id = parent.get("_shared_container_id")
        internal_host = parent.get("_shared_internal_address")
        internal_port = parent.get("_shared_internal_port")
        real_container_started = bool(start_result.get("started"))
        readiness_host = start_result.get("readiness_host")
        readiness_port = start_result.get("readiness_port")
        ms_network = start_result.get("network")
    elif docker_client:
        ms_network = _get_ministack_network(docker_client)
        image, env, container_port, data_path = _docker_image_for_engine(
            engine, engine_version, master_user, master_pass, db_name
        )
        if image and not _image_is_local(docker_client, image):
            # Cold image cache: pulling here would block CreateDBInstance for
            # minutes (measured: 187s for postgres:15-alpine). AWS returns
            # `creating` at once, so allocate the port, defer the whole start to
            # the background finaliser below, and let DescribeDBInstances show
            # `available` when it lands.
            host_port = _next_port()
            endpoint_port = host_port
            deferred_container_start = True
        elif image:
            try:
                # Create path: the `instance` dict doesn't exist yet
                # (it's built ~70 lines below). Allocate a fresh free port
                # and we'll stamp `_HostPort` onto the instance dict at
                # construction time so subsequent respawns reuse it.
                host_port = _next_port()
                endpoint_port = host_port
                container_kwargs = dict(
                    image=image, detach=True,
                    environment=env,
                    ports={f"{container_port}/tcp": host_port},
                    name=_rds_docker_name(db_id),
                    labels={
                        **container_reaper.own_labels("rds"),
                        "db_id": db_id,
                        "account_id": get_account_id(),
                        "region": get_region(),
                    },
                )
                if ms_network:
                    container_kwargs["network"] = ms_network
                # Mount only the engine-appropriate data path. Previously both
                # postgres and mysql paths were mounted unconditionally, which
                # is harmless but wasteful and complicates the Postgres 18+
                # layout change (where the path differs from earlier majors).
                if RDS_PERSIST:
                    docker_volume_name = _rds_docker_volume_name(db_id)
                    container_kwargs["volumes"] = {
                        docker_volume_name: {"bind": data_path, "mode": "rw"},
                    }
                else:
                    container_kwargs["tmpfs"] = {
                        data_path: f"rw,noexec,nosuid,size={RDS_TMPFS_SIZE}",
                    }
                container = docker_client.containers.run(**container_kwargs)
                docker_container_id = container.id
                real_container_started = True
                if ms_network:
                    container.reload()
                    networks = container.attrs.get(
                        "NetworkSettings", {}).get("Networks", {})
                    container_ip = networks.get(
                        ms_network, {}).get("IPAddress", "")
                    if container_ip:
                        internal_host = container_ip
                        internal_port = container_port
                        endpoint_host = container_ip
                        endpoint_port = container_port
                        readiness_host = container_ip
                        readiness_port = container_port
                    else:
                        logger.info(
                            "RDS: started %s container for %s on port %s",
                            engine, db_id, host_port)
                else:
                    readiness_host = "127.0.0.1"
                    readiness_port = host_port
            except Exception as e:
                logger.warning("RDS: Docker failed for %s: %s", db_id, e)

    cluster_id = cluster_id_param
    now_ts = time.time()

    vpc_sgs = _parse_member_list(p, "VpcSecurityGroupIds")
    vpc_sg_list = [{"VpcSecurityGroupId": sg, "Status": "active"} for sg in vpc_sgs] if vpc_sgs else []

    subnet_group = _subnet_groups.get(subnet_group_name, {
        "DBSubnetGroupName": subnet_group_name,
        "DBSubnetGroupDescription": "default",
        "SubnetGroupStatus": "Complete",
        "Subnets": [],
        "VpcId": "vpc-00000000",
        "DBSubnetGroupArn": f"arn:aws:rds:{get_region()}:{get_account_id()}:subgrp:{subnet_group_name}",
    })
    instance_status = "creating" if real_container_started else "available"
    if deferred_container_start:
        # The container is being pulled and started in the background; AWS
        # reports `creating` until provisioning completes.
        instance_status = "creating"
    if parent and not parent.get("_shared_container_ready", True):
        instance_status = "creating"
    if parent and start_result.get("failed"):
        instance_status = "failed"

    instance = {
        "DBInstanceIdentifier": db_id,
        "DBInstanceClass": db_class,
        "Engine": engine,
        "EngineVersion": engine_version,
        "DBInstanceStatus": instance_status,
        "MasterUsername": master_user,
        "DBName": db_name,
        "Endpoint": {
            "Address": endpoint_host,
            "Port": endpoint_port,
            "HostedZoneId": "Z2R2ITUGPM61AM",
        },
        # `_HostPort` is the actual docker host port; `Endpoint.Port` gets
        # overwritten to the engine's container port later (5432 for
        # postgres) to match real AWS, so respawn after restart needs the
        # original host mapping stored separately (#692 follow-up).
        "_HostPort": host_port,
        "AllocatedStorage": allocated_storage,
        "InstanceCreateTime": _format_time(now_ts),
        "PreferredBackupWindow": "03:00-04:00",
        "BackupRetentionPeriod": int(_p(p, "BackupRetentionPeriod") or "1"),
        "DBSecurityGroups": [],
        "VpcSecurityGroups": vpc_sg_list,
        "DBParameterGroups": [{
            "DBParameterGroupName": param_group_name,
            "ParameterApplyStatus": "in-sync",
        }],
        "AvailabilityZone": _p(p, "AvailabilityZone") or f"{get_region()}a",
        "DBSubnetGroup": subnet_group,
        "PreferredMaintenanceWindow": _p(p, "PreferredMaintenanceWindow") or "sun:05:00-sun:06:00",
        "PendingModifiedValues": {},
        "LatestRestorableTime": _format_time(now_ts),
        "MultiAZ": _p(p, "MultiAZ") == "true",
        "AutoMinorVersionUpgrade": _p(p, "AutoMinorVersionUpgrade") != "false",
        "ReadReplicaDBInstanceIdentifiers": [],
        "ReadReplicaSourceDBInstanceIdentifier": "",
        "ReadReplicaDBClusterIdentifiers": [],
        "ReplicaMode": "",
        "LicenseModel": _license_model(engine),
        "Iops": int(_p(p, "Iops") or "0") if _p(p, "Iops") else None,
        "OptionGroupMemberships": [{
            "OptionGroupName": f"default:{engine}-{engine_version.split('.')[0]}",
            "Status": "in-sync",
        }],
        "CharacterSetName": "",
        "NcharCharacterSetName": "",
        "SecondaryAvailabilityZone": "",
        "PubliclyAccessible": _p(p, "PubliclyAccessible") == "true",
        "StatusInfos": [],
        "StorageType": storage_type,
        "TdeCredentialArn": "",
        "DbInstancePort": 0,
        "DBClusterIdentifier": cluster_id,
        "StorageEncrypted": _p(p, "StorageEncrypted") == "true",
        "KmsKeyId": _p(p, "KmsKeyId") or "",
        "DbiResourceId": dbi_resource_id,
        "CACertificateIdentifier": "rds-ca-rsa2048-g1",
        "DomainMemberships": [],
        "CopyTagsToSnapshot": _p(p, "CopyTagsToSnapshot") == "true",
        "MonitoringInterval": int(_p(p, "MonitoringInterval") or "0"),
        "EnhancedMonitoringResourceArn": "",
        "MonitoringRoleArn": _p(p, "MonitoringRoleArn") or "",
        "PromotionTier": int(_p(p, "PromotionTier") or "1"),
        "DBInstanceArn": arn,
        "Timezone": "",
        "IAMDatabaseAuthenticationEnabled": _p(p, "EnableIAMDatabaseAuthentication") == "true",
        "PerformanceInsightsEnabled": _p(p, "EnablePerformanceInsights") == "true",
        "PerformanceInsightsKMSKeyId": "",
        "PerformanceInsightsRetentionPeriod": 7,
        "EnabledCloudwatchLogsExports": [],
        "ProcessorFeatures": [],
        "DeletionProtection": _p(p, "DeletionProtection") == "true",
        "AssociatedRoles": [],
        "MaxAllocatedStorage": int(_p(p, "MaxAllocatedStorage") or str(allocated_storage)),
        "TagList": [],
        "CustomerOwnedIpEnabled": False,
        "ActivityStreamStatus": "stopped",
        "BackupTarget": "region",
        "NetworkType": "IPV4",
        "StorageThroughput": 0,
        "CertificateDetails": {
            "CAIdentifier": "rds-ca-rsa2048-g1",
            "ValidTill": "2061-01-01T00:00:00Z",
        },
        "IsStorageConfigUpgradeAvailable": False,
        "MultiTenant": False,
        "_docker_container_id": docker_container_id,
        "_docker_volume_name": docker_volume_name,
        "_internal_address": internal_host,
        "_internal_port": internal_port,
        "_MasterUserPassword": master_pass,
    }
    if parent and reader_launch:
        # A replicating reader keeps its own endpoint and container fields;
        # the marker excludes it from shared-container aliasing and makes
        # the cluster ReaderEndpoint resolve to it once available.
        instance["_pg_standby"] = True
    elif parent:
        _attach_instance_to_shared_cluster(instance, parent)
    _instances[db_id] = instance
    _register_instance_in_cluster(instance)

    if deferred_container_start:
        # Cold image cache. Pull and start on a background thread — the same
        # finaliser persistence restore uses — so CreateDBInstance returns now
        # with status="creating", as AWS does, instead of blocking on the pull.
        ctx = contextvars.copy_context()

        def _deferred_start():
            _start_rds_container_for_instance(db_id, instance)

        spawn_background(ctx.run, _deferred_start,
                         thread_name=f"ministack-rds-start-{db_id}")

    if real_container_started and reader_launch:
        # Replicating readers have their own readiness contract (wait for
        # the writer, provision replication, wait for the standby), so they
        # get a dedicated worker instead of the generic one below.
        reader_ctx = contextvars.copy_context()
        threading.Thread(
            target=reader_ctx.run,
            args=(
                _bg_finalize_pg_reader, db_id, cluster_id, engine,
                master_user, master_pass, db_name,
                readiness_host or endpoint_host,
                readiness_port or endpoint_port,
                docker_container_id,
            ),
            daemon=True,
        ).start()
    elif real_container_started:
        # Real AWS CreateDBInstance returns immediately with status="creating"
        # and the caller polls (or uses get_waiter('db_instance_available'))
        # until the database becomes reachable. Do the same: run the readiness
        # wait + grant on a daemon thread so the request handler returns now.
        #
        # contextvars.copy_context() carries the request's account_id into
        # the daemon — _instances is account-scoped, so without the snapshot
        # the worker would look the instance up under the default account
        # and silently fail to flip status to "available".
        ready_host = readiness_host or endpoint_host
        ready_port = readiness_port or endpoint_port
        ctx = contextvars.copy_context()

        def _bg_finalize_ready(
            db_id=db_id, cluster_id=cluster_id, engine=engine,
            engine_version=engine_version,
            master_user=master_user, master_pass=master_pass,
            readiness_master_pass=readiness_master_pass,
            db_name=db_name, ready_host=ready_host, ready_port=ready_port,
            ms_network=ms_network, internal_host=internal_host,
            internal_port=internal_port, endpoint_port=endpoint_port,
            container_id=docker_container_id,
            container_epoch=(
                start_result.get("container_epoch") if parent else None
            ),
        ):
            # Tie readiness to backing-container liveness rather than a wall
            # clock: real RDS `CreateDBInstance` has no caller-visible timeout
            # and flips status to `failed` based on hardware state. We do the
            # same — instance stays `creating` while the container is up and
            # booting, transitions to `failed` if the container dies before
            # accepting an authenticated connection.
            def _container_alive():
                client = _get_docker()
                if not client or not container_id:
                    return True  # control-plane-only — nothing to monitor
                try:
                    c = client.containers.get(container_id)
                    c.reload()
                    return c.status not in ("exited", "dead", "removing")
                except Exception:
                    return False
            readiness_cluster = _clusters.get(cluster_id) if cluster_id else None
            readiness_user = master_user
            root_password = readiness_master_pass
            if readiness_cluster and _mysql_replication_secondary(readiness_cluster):
                readiness_user = "root"
                if readiness_cluster.get("_mysql_control_user_ready"):
                    readiness_user = _MYSQL_CONTROL_USER
                    readiness_master_pass = _MYSQL_CONTROL_PASSWORD
            readiness_db_name = (
                None
                if readiness_cluster and _mysql_replication_secondary(readiness_cluster)
                else db_name
            )
            database_ready = _wait_for_database_ready(
                ready_host, ready_port, engine, readiness_user,
                readiness_master_pass, readiness_db_name, _container_alive,
            )
            if database_ready and _is_mysql_engine(engine):
                _ensure_mysql_compatibility(
                    container_id,
                    ready_host,
                    ready_port,
                    root_password,
                    engine_version,
                    cluster_id or db_id,
                    engine=engine,
                )
            cluster = readiness_cluster
            if cluster:
                with _shared_container_lock:
                    if (
                        (
                            container_epoch is not None
                            and cluster.get("_shared_container_epoch")
                            != container_epoch
                        )
                        or cluster.get("_shared_container_id") != container_id
                    ):
                        logger.info(
                            "RDS: ignoring stale readiness result for cluster %s "
                            "epoch %s container %s",
                            cluster_id,
                            container_epoch,
                            container_id,
                        )
                        return
                    if not database_ready:
                        logger.warning(
                            "RDS: %s container for %s at %s:%s exited before "
                            "becoming reachable", engine, db_id,
                            ready_host, ready_port,
                        )
                        cluster["_shared_container_ready"] = False
                        _set_cluster_members_status(cluster, "failed")
                        _refresh_cluster_status(cluster_id)
                        return

                    cluster["_shared_storage_initialized"] = True
                    pending_rotation = cluster.get(
                        "_pending_master_password_rotation",
                    )
                    if pending_rotation and not _rotate_real_password(
                        cluster,
                        pending_rotation["old_password"],
                        pending_rotation["new_password"],
                    ):
                        cluster["_shared_container_ready"] = False
                        _set_cluster_members_status(cluster, "failed")
                        _refresh_cluster_status(cluster_id)
                        return
                    if pending_rotation:
                        cluster.pop("_pending_master_password_rotation", None)
                        _sync_global_mysql_credentials(cluster)

                    if _is_mysql_engine(engine) and not _mysql_replication_secondary(
                        cluster,
                    ):
                        _grant_mysql_master_user_privileges(
                            ready_host, ready_port, master_user,
                            cluster.get("_MasterUserPassword", master_pass),
                            cluster_id,
                        )
                    cluster["_shared_container_ready"] = True
                    if _aurora_mysql_8_replication_enabled(cluster):
                        _configure_or_defer_mysql_replication(cluster_id, cluster)
                    for member in cluster.get("DBClusterMembers", []):
                        inst = _instances.get(member.get("DBInstanceIdentifier"))
                        if inst is None or inst.get("_pg_standby"):
                            # Replicating readers publish their own
                            # readiness (#1325); the writer becoming ready
                            # does not make a still-bootstrapping standby
                            # available.
                            continue
                        _attach_instance_to_shared_cluster(inst, cluster)
                        inst["DBInstanceStatus"] = "available"
                    _sync_cluster_endpoints(cluster)
                    _refresh_cluster_status(cluster_id)
                if ms_network and internal_host:
                    logger.info(
                        "RDS: %s container for %s ready at %s:%s (network %s)",
                        engine, db_id, internal_host, internal_port, ms_network,
                    )
                else:
                    logger.info(
                        "RDS: %s container for %s ready on port %s",
                        engine, db_id, endpoint_port,
                    )
                return
            if not database_ready:
                logger.warning(
                    "RDS: %s container for %s at %s:%s exited before becoming reachable",
                    engine, db_id, ready_host, ready_port,
                )
                inst = _instances.get(db_id)
                if inst is not None:
                    inst["DBInstanceStatus"] = "failed"
                _refresh_cluster_status(cluster_id)
                return
            if _is_mysql_engine(engine):
                _grant_mysql_master_user_privileges(
                    ready_host, ready_port, master_user, master_pass,
                    cluster_id or db_id,
                )
            inst = _instances.get(db_id)
            if inst is not None:
                inst["DBInstanceStatus"] = "available"
            _refresh_cluster_status(cluster_id)
            if ms_network and internal_host:
                logger.info(
                    "RDS: %s container for %s ready at %s:%s (network %s)",
                    engine, db_id, internal_host, internal_port, ms_network,
                )
            else:
                logger.info(
                    "RDS: %s container for %s ready on port %s",
                    engine, db_id, endpoint_port,
                )

        threading.Thread(
            target=ctx.run, args=(_bg_finalize_ready,), daemon=True,
        ).start()

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[arn] = req_tags
        instance["TagList"] = req_tags

    return _single_instance_response("CreateDBInstanceResponse", "CreateDBInstanceResult", instance)


def _delete_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        invalid_arn = _invalid_db_instance_identifier_error(db_id)
        if invalid_arn:
            return invalid_arn
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance_id = instance["DBInstanceIdentifier"]

    if instance.get("DeletionProtection"):
        return _error("InvalidParameterCombination",
            "Cannot delete a DB instance when DeletionProtection is enabled.", 400)

    parent_cluster_id = (
        instance.get("_shared_cluster_id")
        or instance.get("DBClusterIdentifier")
    )
    if parent_cluster_id:
        parent = _resolve_cluster_in_request_region(parent_cluster_id)
        if parent and parent.get("Status") == "stopped":
            # Real AWS rejects member deletion from a stopped cluster; the
            # cluster must be started first. Without this guard, deleting
            # the last member of a stopped cluster would let a later
            # CreateDBInstance restart compute behind the ``stopped``
            # status.
            return _error(
                "InvalidDBClusterStateFault",
                f"DbCluster {parent['DBClusterIdentifier']} is in stopped "
                "state but expected it to be one of available.",
                400,
            )

    _unregister_instance_from_clusters(instance_id)

    shared_cluster_id = (
        instance.get("_shared_cluster_id")
        or instance.get("DBClusterIdentifier")
    )
    if shared_cluster_id:
        cluster = _resolve_cluster_in_request_region(shared_cluster_id)
        if cluster and not cluster.get("DBClusterMembers"):
            global_cluster, _member = _global_cluster_member_for_cluster(cluster)
            if (
                global_cluster
                and len(global_cluster.get("GlobalClusterMembers", [])) > 1
            ):
                # Aurora global headless secondaries have no query compute but
                # their storage continues to synchronize. MiniStack's shared
                # process also sustains the global topology, so preserve it
                # for both writers and secondaries while other members exist.
                if _mysql_replication_secondary(cluster):
                    cluster["_mysql_headless_applier_required"] = True
            else:
                _stop_cluster_shared_container(shared_cluster_id, cluster)

    # Tombstone first, under the lock: a deferred start still pulling would
    # otherwise publish its container id after we read it here, and the record
    # is gone by the time it lands — leaving a running container nothing
    # references. With the tombstone that finisher reclaims what it started.
    with resource_lock("rds", instance_id):
        instance["_deleting"] = True
        owned_id = instance.get("_docker_container_id") if _instance_owns_container(instance) else None

    docker_client = _get_docker()
    if docker_client:
        # By id, else by the deterministic name — the id may never have been
        # published if the delete beat the deferred start.
        for locate in (
            (lambda: docker_client.containers.get(owned_id)) if owned_id else None,
            lambda: docker_client.containers.get(_rds_docker_name(instance_id)),
        ):
            if locate is None:
                continue
            try:
                c = locate()
            except Exception:
                continue
            try:
                c.stop(timeout=5)
                c.remove(v=True)
                logger.info("RDS: removed container for %s", instance_id)
            except Exception as e:
                logger.warning("RDS: failed to remove container for %s: %s", instance_id, e)
            break

    skip_snapshot = _p(p, "SkipFinalSnapshot") == "true"
    final_snap_id = _p(p, "FinalDBSnapshotIdentifier")
    if not skip_snapshot and final_snap_id:
        _create_snapshot_internal(final_snap_id, instance)

    instance["DBInstanceStatus"] = "deleting"
    arn = instance["DBInstanceArn"]
    _tags.pop(arn, None)
    del _instances[instance_id]
    return _single_instance_response("DeleteDBInstanceResponse", "DeleteDBInstanceResult", instance)


def _describe_db_instances(p):
    db_id = _p(p, "DBInstanceIdentifier")
    if db_id:
        instance = _resolve_instance(db_id)
        if not instance:
            invalid_arn = _invalid_db_instance_identifier_error(db_id, "Filter: db-instance-id")
            if invalid_arn:
                return invalid_arn
            return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
        instances = [instance]
    else:
        instances = list(_instances.values())
        filters = _parse_filters(p)
        if filters:
            instances = _apply_instance_filters(instances, filters)

    members = "".join(f"<DBInstance>{_instance_xml(i)}</DBInstance>" for i in instances)
    return _xml(200, "DescribeDBInstancesResponse",
        f"<DescribeDBInstancesResult><DBInstances>{members}</DBInstances></DescribeDBInstancesResult>")


def _rotate_instance_password(instance, old_pass, new_pass):
    """Alter the root password on the real DB container for a standalone instance."""
    db_id = instance.get("DBInstanceIdentifier", "")
    engine = instance.get("Engine", "")
    host = instance.get("_internal_address")
    port = instance.get("_internal_port")
    if not host or not port:
        endpoint = instance.get("Endpoint", {})
        if not isinstance(endpoint, dict) or not endpoint.get("Port"):
            return
        host = endpoint.get("Address", "localhost")
        port = int(endpoint.get("Port", 3306))
    if any(e in engine for e in ("mysql", "aurora-mysql", "mariadb")):
        try:
            import pymysql
            conn = pymysql.connect(
                host=host, port=port, user="root",
                password=old_pass, autocommit=True)
            cur = conn.cursor()
            cur.execute(
                "ALTER USER 'root'@'%%' IDENTIFIED BY %s", (new_pass,))
            cur.close()
            conn.close()
            logger.info("RDS: rotated root password on instance %s", db_id)
        except Exception as e:
            # Error (not warning) — the stored master password no longer matches
            # the real DB container, so follow-up connections will fail.
            logger.error("RDS: password rotation failed on instance %s: %s",
                         db_id, e)
    elif any(e in engine for e in ("postgres", "aurora-postgresql")):
        try:
            import psycopg2
            from psycopg2 import sql as _pgsql
            master_user = instance.get("MasterUsername", "admin")
            conn = psycopg2.connect(
                host=host, port=port, user=master_user,
                password=old_pass, dbname=instance.get("DBName", "postgres"))
            conn.autocommit = True
            cur = conn.cursor()
            # Use psycopg2.sql.Identifier to quote the role name safely — AsIs
            # skips quoting entirely and is a SQL-injection hazard when
            # MasterUsername comes from user input.
            cur.execute(
                _pgsql.SQL("ALTER USER {role} WITH PASSWORD %s").format(
                    role=_pgsql.Identifier(master_user)),
                (new_pass,))
            cur.close()
            conn.close()
            logger.info("RDS: rotated password on instance %s", db_id)
        except Exception as e:
            logger.error("RDS: password rotation failed on instance %s: %s",
                         db_id, e)


def _modify_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        invalid_arn = _invalid_db_instance_identifier_error(db_id)
        if invalid_arn:
            return invalid_arn
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)

    new_pass = _p(p, "MasterUserPassword")
    if new_pass and (
        instance.get("_shared_cluster_id")
        or instance.get("DBClusterIdentifier")
    ):
        return _error(
            "InvalidParameterCombination",
            "MasterUserPassword cannot be modified on a DB instance that is "
            "a member of a DB cluster. Use ModifyDBCluster instead.",
            400,
        )

    engine_version = _p(p, "EngineVersion")
    engine_version_error = _unsupported_aurora_engine_version_error(
        instance.get("Engine"),
        engine_version,
        current_version=instance.get("EngineVersion"),
    )
    if engine_version_error:
        return engine_version_error

    apply_immediately = _p(p, "ApplyImmediately") == "true"

    field_map = {
        "DBInstanceClass": "DBInstanceClass",
        "AllocatedStorage": "AllocatedStorage",
        "MasterUserPassword": None,
        "MultiAZ": "MultiAZ",
        "EngineVersion": "EngineVersion",
        "StorageType": "StorageType",
        "Iops": "Iops",
        "DBParameterGroupName": None,
        "BackupRetentionPeriod": "BackupRetentionPeriod",
        "PreferredBackupWindow": "PreferredBackupWindow",
        "PreferredMaintenanceWindow": "PreferredMaintenanceWindow",
        "PubliclyAccessible": "PubliclyAccessible",
        "CACertificateIdentifier": "CACertificateIdentifier",
        "DeletionProtection": "DeletionProtection",
        "MaxAllocatedStorage": "MaxAllocatedStorage",
        "MonitoringInterval": "MonitoringInterval",
        "MonitoringRoleArn": "MonitoringRoleArn",
        "CopyTagsToSnapshot": "CopyTagsToSnapshot",
    }

    pending = {}
    for param_key, instance_key in field_map.items():
        val = _p(p, param_key)
        if not val:
            continue
        if instance_key is None:
            continue
        if param_key in ("AllocatedStorage", "BackupRetentionPeriod",
                         "MonitoringInterval", "Iops", "MaxAllocatedStorage"):
            val = int(val)
        elif param_key in ("MultiAZ", "PubliclyAccessible",
                           "DeletionProtection", "CopyTagsToSnapshot"):
            val = val == "true"

        if apply_immediately:
            instance[instance_key] = val
        else:
            pending[instance_key] = val

    if new_pass:
        old_pass = instance.get("_MasterUserPassword", "password")
        instance["_MasterUserPassword"] = new_pass
        _rotate_instance_password(instance, old_pass, new_pass)

    if _p(p, "DBParameterGroupName"):
        instance["DBParameterGroups"] = [{
            "DBParameterGroupName": _p(p, "DBParameterGroupName"),
            "ParameterApplyStatus": "applying" if apply_immediately else "pending-reboot",
        }]

    vpc_sgs = _parse_member_list(p, "VpcSecurityGroupIds")
    if vpc_sgs:
        instance["VpcSecurityGroups"] = [
            {"VpcSecurityGroupId": sg, "Status": "active"} for sg in vpc_sgs
        ]

    if pending:
        instance["PendingModifiedValues"] = pending

    return _single_instance_response("ModifyDBInstanceResponse", "ModifyDBInstanceResult", instance)


def _start_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        invalid_arn = _invalid_db_instance_identifier_error(db_id)
        if invalid_arn:
            return invalid_arn
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "available"
    return _single_instance_response("StartDBInstanceResponse", "StartDBInstanceResult", instance)


def _stop_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        invalid_arn = _invalid_db_instance_identifier_error(db_id)
        if invalid_arn:
            return invalid_arn
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "stopped"
    return _single_instance_response("StopDBInstanceResponse", "StopDBInstanceResult", instance)


def _reboot_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        invalid_arn = _invalid_db_instance_identifier_error(db_id)
        if invalid_arn:
            return invalid_arn
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "available"
    return _single_instance_response("RebootDBInstanceResponse", "RebootDBInstanceResult", instance)


# ---------------------------------------------------------------------------
# Read Replica (stub)
# ---------------------------------------------------------------------------

def _create_read_replica(p):
    source_id = _p(p, "SourceDBInstanceIdentifier")
    replica_id = _p(p, "DBInstanceIdentifier")

    source = _resolve_instance(source_id)
    if not source:
        invalid_arn = _invalid_db_instance_identifier_error(source_id, "SourceDBInstanceIdentifier")
        if invalid_arn:
            return invalid_arn
        return _error("DBInstanceNotFound", f"DBInstance {source_id} not found.", 404)
    source_id = source["DBInstanceIdentifier"]
    if replica_id in _instances:
        return _error("DBInstanceAlreadyExists", f"DBInstance {replica_id} already exists.", 400)

    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:db:{replica_id}"
    replica = dict(source)
    replica.update({
        "DBInstanceIdentifier": replica_id,
        "DBInstanceArn": arn,
        "ReadReplicaSourceDBInstanceIdentifier": source_id,
        "DBInstanceStatus": "available",
        "DbiResourceId": f"db-{new_uuid().replace('-', '')[:20].upper()}",
        "InstanceCreateTime": _format_time(time.time()),
        "ReadReplicaDBInstanceIdentifiers": [],
        "Endpoint": {
            "Address": _MINISTACK_HOST,
            "Port": _next_port(),
            "HostedZoneId": "Z2R2ITUGPM61AM",
        },
        "TagList": [],
        "_docker_container_id": None,
    })
    _instances[replica_id] = replica
    source.setdefault("ReadReplicaDBInstanceIdentifiers", []).append(replica_id)

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[arn] = req_tags
        replica["TagList"] = req_tags

    return _single_instance_response("CreateDBInstanceReadReplicaResponse",
                                     "CreateDBInstanceReadReplicaResult", replica)


# ---------------------------------------------------------------------------
# Restore from Snapshot (stub)
# ---------------------------------------------------------------------------

def _restore_from_snapshot(p):
    db_id = _p(p, "DBInstanceIdentifier")
    snap_id = _p(p, "DBSnapshotIdentifier")

    if db_id in _instances:
        return _error("DBInstanceAlreadyExists", f"DBInstance {db_id} already exists.", 400)

    snap = _snapshots.get(snap_id)
    if not snap:
        return _error("DBSnapshotNotFound", f"DBSnapshot {snap_id} not found.", 404)

    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:db:{db_id}"
    instance = {
        "DBInstanceIdentifier": db_id,
        "DBInstanceClass": _p(p, "DBInstanceClass") or snap.get("DBInstanceClass", "db.t3.micro"),
        "Engine": snap.get("Engine", "postgres"),
        "EngineVersion": snap.get("EngineVersion")
        or _default_engine_version(snap.get("Engine", "postgres")),
        "DBInstanceStatus": "available",
        "MasterUsername": snap.get("MasterUsername", "admin"),
        "DBName": snap.get("DBName", ""),
        "Endpoint": {
            "Address": _MINISTACK_HOST,
            "Port": _next_port(),
            "HostedZoneId": "Z2R2ITUGPM61AM",
        },
        "AllocatedStorage": snap.get("AllocatedStorage", 20),
        "InstanceCreateTime": _format_time(time.time()),
        "PreferredBackupWindow": "03:00-04:00",
        "BackupRetentionPeriod": 1,
        "DBSecurityGroups": [],
        "VpcSecurityGroups": [],
        "DBParameterGroups": [{
            "DBParameterGroupName": f"default.{snap.get('Engine', 'postgres')}",
            "ParameterApplyStatus": "in-sync",
        }],
        "AvailabilityZone": _p(p, "AvailabilityZone") or f"{get_region()}a",
        "DBSubnetGroup": {"DBSubnetGroupName": _p(p, "DBSubnetGroupName") or "default",
                          "SubnetGroupStatus": "Complete", "Subnets": [], "VpcId": "vpc-00000000",
                          "DBSubnetGroupArn": ""},
        "PreferredMaintenanceWindow": "sun:05:00-sun:06:00",
        "PendingModifiedValues": {},
        "MultiAZ": _p(p, "MultiAZ") == "true",
        "AutoMinorVersionUpgrade": True,
        "ReadReplicaDBInstanceIdentifiers": [],
        "ReadReplicaSourceDBInstanceIdentifier": "",
        "ReadReplicaDBClusterIdentifiers": [],
        "LicenseModel": _license_model(snap.get("Engine", "postgres")),
        "OptionGroupMemberships": [],
        "PubliclyAccessible": _p(p, "PubliclyAccessible") == "true",
        "StorageType": _p(p, "StorageType") or snap.get("StorageType", "gp2"),
        "StorageEncrypted": snap.get("StorageEncrypted", False),
        "DbiResourceId": f"db-{new_uuid().replace('-', '')[:20].upper()}",
        "CACertificateIdentifier": "rds-ca-rsa2048-g1",
        "DomainMemberships": [],
        "CopyTagsToSnapshot": False,
        "MonitoringInterval": 0,
        "DBInstanceArn": arn,
        "IAMDatabaseAuthenticationEnabled": False,
        "PerformanceInsightsEnabled": False,
        "DeletionProtection": False,
        "TagList": [],
        "_docker_container_id": None,
    }
    _instances[db_id] = instance
    return _single_instance_response("RestoreDBInstanceFromDBSnapshotResponse",
                                     "RestoreDBInstanceFromDBSnapshotResult", instance)


# ---------------------------------------------------------------------------
# DB Clusters
# ---------------------------------------------------------------------------

def _create_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    if not cluster_id:
        return _error("MissingParameter", "DBClusterIdentifier is required", 400)
    if cluster_id in _clusters:
        return _error("DBClusterAlreadyExistsFault",
            f"DB cluster {cluster_id} already exists.", 400)

    global_cluster_id = _p(p, "GlobalClusterIdentifier")
    invalid_global_id = _invalid_global_cluster_identifier_error(global_cluster_id)
    if invalid_global_id:
        return invalid_global_id
    global_cluster = _resolve_global_cluster(global_cluster_id) if global_cluster_id else None
    if global_cluster_id and not global_cluster:
        return _error("GlobalClusterNotFoundFault",
            f"Global cluster {global_cluster_id} not found.", 404)
    if global_cluster and _global_cluster_member_in_request_region(global_cluster):
        return _error(
            "InvalidParameterValue",
            f"Global cluster {global_cluster_id} already has a member in {get_region()}.",
            400,
        )

    explicit_cpg = _p(p, "DBClusterParameterGroupName")
    if (explicit_cpg and not explicit_cpg.startswith("default.")
            and explicit_cpg not in _db_cluster_param_groups):
        return _error("DBClusterParameterGroupNotFound",
                      f"DBClusterParameterGroup {explicit_cpg} not found.", 404)

    engine = _p(p, "Engine") or "aurora-postgresql"
    if global_cluster:
        expected_engine = global_cluster.get("Engine")
        if _p(p, "Engine") and expected_engine and engine != expected_engine:
            return _error(
                # Real AWS message shape (InvalidParameterValue, Sender, 400),
                # verbatim template from a captured CreateDBCluster transcript.
                "InvalidParameterValue",
                "Value for engine should match setting for global cluster "
                f"{global_cluster_id}",
                400,
            )
        engine = expected_engine or engine
    explicit_engine_version = _p(p, "EngineVersion")
    engine_version_error = _unsupported_aurora_engine_version_error(engine, explicit_engine_version)
    if engine_version_error:
        return engine_version_error
    engine_version = explicit_engine_version or _default_engine_version(engine)
    if global_cluster:
        expected_engine_version = global_cluster.get("EngineVersion")
        inherited_version_error = _unsupported_aurora_engine_version_error(
            engine, expected_engine_version
        )
        if inherited_version_error:
            return inherited_version_error
        if (
            explicit_engine_version
            and expected_engine_version
            and engine_version != expected_engine_version
        ):
            return _error(
                # Verbatim real-AWS message (InvalidParameterValue, Sender, 400)
                # from a captured CreateDBCluster-into-global transcript.
                "InvalidParameterValue",
                "Value for engineVersion should match setting for global "
                f"cluster {global_cluster_id}",
                400,
            )
        engine_version = expected_engine_version or engine_version
    port = int(_p(p, "Port") or _default_port(engine))
    explicit_master_user = _p(p, "MasterUsername") or None
    explicit_master_pass = _p(p, "MasterUserPassword") or None
    explicit_database_name = _p(p, "DatabaseName") or None
    manage_master_pass = _p(p, "ManageMasterUserPassword") == "true"
    if manage_master_pass and explicit_master_pass:
        return _error(
            "InvalidParameterCombination",
            "You can't specify MasterUserPassword when ManageMasterUserPassword "
            "is enabled.",
            400,
        )
    global_writer = (
        _global_cluster_writer_cluster(global_cluster)
        if global_cluster
        else None
    )
    if manage_master_pass and global_writer:
        return _error(
            "InvalidParameterCombination",
            "You can't manage the master user password on an Aurora global "
            "database secondary cluster.",
            400,
        )
    if global_writer:
        inherited_fields = (
            ("MasterUsername", explicit_master_user, global_writer.get("MasterUsername")),
            (
                "MasterUserPassword",
                explicit_master_pass,
                global_writer.get("_MasterUserPassword"),
            ),
            ("DatabaseName", explicit_database_name, global_writer.get("DatabaseName")),
        )
        conflicting_field = next(
            (
                field
                for field, explicit_value, inherited_value in inherited_fields
                if explicit_value is not None
                and explicit_value != inherited_value
            ),
            None,
        )
        if conflicting_field:
            return _error(
                "InvalidParameterValue",
                f"{conflicting_field} must match the global writer when "
                "creating a secondary cluster.",
                400,
            )
        if not _mysql_gtid_history_ready(global_writer):
            return _error(
                "InvalidDBClusterStateFault",
                "The global writer volume predates GTID-at-creation tracking "
                "and cannot safely seed a secondary.",
                400,
            )
    master_user = (
        global_writer.get("MasterUsername", "admin")
        if global_writer
        else explicit_master_user or "admin"
    )
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:cluster:{cluster_id}"
    unique_suffix = new_uuid()[:8]
    now_ts = time.time()

    vpc_sgs = _parse_member_list(p, "VpcSecurityGroupIds")
    vpc_sg_list = [{"VpcSecurityGroupId": sg, "Status": "active"} for sg in vpc_sgs] if vpc_sgs else []
    az_list = _parse_member_list(p, "AvailabilityZones")
    if not az_list:
        az_list = [f"{get_region()}a", f"{get_region()}b", f"{get_region()}c"]

    master_pass = (
        global_writer.get("_MasterUserPassword", "password")
        if global_writer
        else explicit_master_pass or "password"
    )
    master_user_secret = None
    if manage_master_pass:
        master_pass = _generate_master_user_password()
        secret_arn = secretsmanager.create_secret_in_process(
            f"rds!cluster-{new_uuid()}",
            json.dumps({"username": master_user, "password": master_pass}),
            description=f"Secret managed by RDS for DB cluster {cluster_id}",
        )
        master_user_secret = {
            "SecretArn": secret_arn,
            "SecretStatus": "active",
            "KmsKeyId": _p(p, "MasterUserSecretKmsKeyId")
            or f"arn:aws:kms:{get_region()}:{get_account_id()}:key/aws-secretsmanager-default",
        }
    database_name = (
        global_writer.get("DatabaseName")
        if global_writer
        else explicit_database_name
    )

    cluster = {
        "DBClusterIdentifier": cluster_id,
        "DBClusterArn": arn,
        "Engine": engine,
        "EngineVersion": engine_version,
        "EngineMode": _p(p, "EngineMode") or "provisioned",
        "Status": "available",
        "MasterUsername": master_user,
        "_MasterUserPassword": master_pass,
        "MasterUserSecret": master_user_secret,
        "DatabaseName": database_name,
        "NetworkType": _p(p, "NetworkType") or "IPV4",
        "EngineLifecycleSupport": _p(p, "EngineLifecycleSupport") or "open-source-rds-extended-support",
        "Endpoint": f"{cluster_id}.cluster-{unique_suffix}.{get_region()}.rds.amazonaws.com",
        "ReaderEndpoint": f"{cluster_id}.cluster-ro-{unique_suffix}.{get_region()}.rds.amazonaws.com",
        "Port": port,
        "MultiAZ": _p(p, "MultiAZ") == "true",
        "AvailabilityZones": az_list,
        "DBClusterMembers": [],
        "VpcSecurityGroups": vpc_sg_list,
        "DBSubnetGroup": _p(p, "DBSubnetGroupName") or "default",
        "DBClusterParameterGroup": _p(p, "DBClusterParameterGroupName") or f"default.{engine}",
        "BackupRetentionPeriod": int(_p(p, "BackupRetentionPeriod") or "1"),
        "PreferredBackupWindow": _p(p, "PreferredBackupWindow") or "03:00-04:00",
        "PreferredMaintenanceWindow": _p(p, "PreferredMaintenanceWindow") or "sun:05:00-sun:06:00",
        "ClusterCreateTime": _format_time(now_ts),
        "EarliestRestorableTime": _format_time(now_ts),
        "LatestRestorableTime": _format_time(now_ts),
        "StorageEncrypted": _p(p, "StorageEncrypted") == "true",
        "KmsKeyId": _p(p, "KmsKeyId") or "",
        "DeletionProtection": _p(p, "DeletionProtection") == "true",
        "IAMDatabaseAuthenticationEnabled": _p(p, "EnableIAMDatabaseAuthentication") == "true",
        "EnabledCloudwatchLogsExports": [],
        "HttpEndpointEnabled": _p(p, "EnableHttpEndpoint") == "true",
        "CopyTagsToSnapshot": _p(p, "CopyTagsToSnapshot") == "true",
        "CrossAccountClone": False,
        "DbClusterResourceId": f"cluster-{new_uuid().replace('-', '')[:20].upper()}",
        "TagList": [],
        "HostedZoneId": "Z2R2ITUGPM61AM",
        "AssociatedRoles": [],
        "ActivityStreamStatus": "stopped",
        "AllocatedStorage": 1,
        "Capacity": 0,
        "ClusterScalabilityType": "standard",
        "_shared_container_id": None,
        "_shared_host_port": None,
        "_shared_endpoint": None,
        "_shared_volume_name": None,
        "_shared_internal_address": None,
        "_shared_internal_port": None,
        "_shared_container_ready": True,
        "_shared_container_epoch": 0,
        "_shared_storage_initialized": False,
    }
    _prepare_mysql_gtid_history(cluster)
    _clusters[cluster_id] = cluster
    if global_cluster:
        is_first_member = not global_cluster.get("GlobalClusterMembers")
        _attach_cluster_to_global(global_cluster, cluster, is_writer=is_first_member)

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[arn] = req_tags
        cluster["TagList"] = req_tags

    return _xml(200, "CreateDBClusterResponse",
        f"<CreateDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></CreateDBClusterResult>")


def _delete_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    cluster = _resolve_cluster_in_request_region(cluster_id)
    if not cluster:
        wrong_region = _invalid_cluster_identifier_error(cluster_id)
        if wrong_region:
            return wrong_region
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    if cluster.get("DeletionProtection"):
        return _error("InvalidParameterCombination",
            "Cannot delete a DB cluster when DeletionProtection is enabled.", 400)

    if cluster.get("GlobalClusterIdentifier"):
        return _error("InvalidDBClusterStateFault",
            "Cannot delete a DB cluster while it is a member of a global cluster.", 400)

    if cluster.get("DBClusterMembers"):
        return _error(
            "InvalidDBClusterStateFault",
            "Cannot delete a DB cluster while it contains DB instances.",
            400,
        )

    skip_snapshot = _p(p, "SkipFinalSnapshot") == "true"
    final_snap_id = _p(p, "FinalDBSnapshotIdentifier")
    if not skip_snapshot and final_snap_id:
        pass

    # Serialize cluster identity and resource teardown with warm-boot startup.
    # The membership can become empty between the initial check and this point
    # when the last member is being deleted concurrently.
    with _shared_container_lock:
        current_cluster = _resolve_cluster_in_request_region(cluster_id)
        if current_cluster is not cluster:
            return _error(
                "DBClusterNotFoundFault",
                f"DBCluster {cluster_id} not found.",
                404,
            )
        if cluster.get("DBClusterMembers"):
            return _error(
                "InvalidDBClusterStateFault",
                "Cannot delete a DB cluster while it contains DB instances.",
                400,
            )
        cluster["Status"] = "deleting"
        if any(
            cluster.get(field)
            for field in (
                "_shared_container_id",
                "_shared_endpoint",
                "_shared_volume_name",
            )
        ):
            _remove_cluster_shared_resources(
                cluster["DBClusterIdentifier"],
                cluster,
            )
        _tags.pop(cluster["DBClusterArn"], None)
        del _clusters[cluster["DBClusterIdentifier"]]
        master_user_secret = cluster.get("MasterUserSecret")
        if master_user_secret and master_user_secret.get("SecretArn"):
            # RDS deletes its managed master user secret with the cluster.
            secretsmanager.delete_secret_in_process(master_user_secret["SecretArn"])
    return _xml(200, "DeleteDBClusterResponse",
        f"<DeleteDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></DeleteDBClusterResult>")


def _describe_db_clusters(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    if cluster_id:
        cluster = _resolve_cluster_in_request_region(cluster_id)
        if not cluster:
            wrong_region = _invalid_region_arn_error(cluster_id, "DBClusterIdentifier")
            if wrong_region:
                return wrong_region
            return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
        clusters = [cluster]
    else:
        clusters = list(_clusters.values())
        filters = _parse_filters(p)
        if filters:
            clusters = _apply_cluster_filters(clusters, filters)

    members = "".join(f"<DBCluster>{_cluster_xml(c)}</DBCluster>" for c in clusters)
    return _xml(200, "DescribeDBClustersResponse",
        f"<DescribeDBClustersResult><DBClusters>{members}</DBClusters></DescribeDBClustersResult>")


def _generate_master_user_password():
    """Generate a random master user password for a managed secret.

    Stays within RDS's password rules (printable ASCII excluding '/', '@',
    '"', and spaces) — token_urlsafe only emits letters, digits, '-' and '_'.
    """
    return stdlib_secrets.token_urlsafe(24)


def _rotate_real_password(cluster, old_pass, new_pass):
    """Rotate the real cluster master login before publishing new metadata."""
    cluster_id = cluster.get("DBClusterIdentifier", "")
    engine = cluster.get("Engine", "")
    master_user = cluster.get("MasterUsername", "admin")
    db_name = cluster.get("DatabaseName") or "mydb"
    host = cluster.get("_shared_internal_address")
    port = cluster.get("_shared_internal_port")
    for inst in _instances.values():
        if inst.get("DBClusterIdentifier") != cluster_id:
            continue
        engine = engine or inst.get("Engine", "")
        host = host or inst.get("_internal_address")
        port = port or inst.get("_internal_port")
        if not host or not port:
            endpoint = inst.get("Endpoint", {})
            if not isinstance(endpoint, dict) or not endpoint.get("Port"):
                continue
            host = endpoint.get("Address", "localhost")
            port = int(endpoint["Port"])
        break
    if not host or not port:
        logger.warning(
            "RDS: password rotation failed on %s: no reachable endpoint",
            cluster_id,
        )
        return False

    conn = None
    cur = None
    try:
        if _is_mysql_engine(engine):
            import pymysql

            conn = pymysql.connect(
                host=host,
                port=int(port),
                user="root",
                password=old_pass,
                autocommit=True,
            )
            cur = conn.cursor()
            if master_user != "root":
                cur.execute(
                    "ALTER USER %s@'%%' IDENTIFIED BY %s",
                    (master_user, new_pass),
                )
            cur.execute(
                "ALTER USER 'root'@'%%' IDENTIFIED BY %s",
                (new_pass,),
            )
        elif _is_postgres_engine(engine):
            import psycopg2
            from psycopg2 import sql as _pgsql

            conn = psycopg2.connect(
                host=host,
                port=int(port),
                user=master_user,
                password=old_pass,
                dbname=db_name,
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(
                _pgsql.SQL("ALTER USER {role} WITH PASSWORD %s").format(
                    role=_pgsql.Identifier(master_user),
                ),
                (new_pass,),
            )
        else:
            return True

        cur.close()
        cur = None
        conn.close()
        conn = None
        logger.info("RDS: rotated master password on %s", cluster_id)
        return True
    except Exception as e:
        logger.warning(
            "RDS: password rotation failed on %s: %s", cluster_id, e,
        )
        return False
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _apply_cluster_master_password_change(cluster, new_pass):
    """Publish *new_pass* as the cluster's master password and rotate the real
    database login.

    Shared by the explicit ``MasterUserPassword`` and the managed
    ``RotateMasterUserPassword`` paths so the compute-availability and
    pending-rotation semantics cannot drift between them. When compute is
    down but storage survives, the rotation is parked as pending: the first
    replacement member authenticates with the old password, applies the
    pending rotation, then publishes readiness using the new one.
    """
    with _shared_container_lock:
        old_pass = cluster.get("_MasterUserPassword", "password")
        cluster["_MasterUserPassword"] = new_pass
        pending_rotation = cluster.get(
            "_pending_master_password_rotation",
        )
        rotation_old_pass = (
            pending_rotation["old_password"]
            if pending_rotation
            else old_pass
        )
        has_available_compute = bool(
            cluster.get("DBClusterMembers"),
        ) and bool(
            cluster.get("_shared_container_id"),
        ) and cluster.get("_shared_container_ready", True)
        if has_available_compute and _rotate_real_password(
            cluster, rotation_old_pass, new_pass,
        ):
            cluster.pop("_pending_master_password_rotation", None)
            _sync_global_mysql_credentials(cluster)
        elif (
            cluster.get("_shared_container_id")
            or cluster.get("_shared_storage_initialized")
        ):
            # The stopped preserved container still has rotation_old_pass.
            # The first replacement member authenticates with it, applies
            # the pending rotation, then publishes readiness using new_pass.
            cluster["_pending_master_password_rotation"] = {
                "old_password": rotation_old_pass,
                "new_password": new_pass,
            }


def _modify_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    cluster = _resolve_cluster_in_request_region(cluster_id)
    if not cluster:
        wrong_region = _invalid_cluster_identifier_error(cluster_id)
        if wrong_region:
            return wrong_region
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    # Validate the password parameters together before mutating anything, so
    # a rejected combination cannot leave a half-applied password change.
    if _p(p, "MasterUserPassword") and _p(p, "RotateMasterUserPassword") == "true":
        return _error(
            "InvalidParameterCombination",
            "You can't specify MasterUserPassword and RotateMasterUserPassword "
            "in the same request.",
            400,
        )
    engine_version = _p(p, "EngineVersion")
    engine_version_error = _unsupported_aurora_engine_version_error(
        cluster.get("Engine"), engine_version,
        current_version=cluster.get("EngineVersion"),
    )
    if engine_version_error:
        return engine_version_error
    global_version_conflict = _global_member_engine_version_conflict_error(
        cluster, engine_version
    )
    if global_version_conflict:
        return global_version_conflict
    if _p(p, "MasterUserPassword"):
        if cluster.get("MasterUserSecret"):
            return _error(
                "InvalidParameterCombination",
                "You can't specify MasterUserPassword for a cluster with "
                "ManageMasterUserPassword enabled.",
                400,
            )
        if _mysql_replication_secondary(cluster):
            return _error(
                "InvalidDBClusterStateFault",
                "MasterUserPassword must be changed on the global writer.",
                400,
            )
        _apply_cluster_master_password_change(cluster, _p(p, "MasterUserPassword"))
    if _p(p, "RotateMasterUserPassword") == "true":
        master_user_secret = cluster.get("MasterUserSecret")
        if not master_user_secret:
            return _error(
                "InvalidParameterCombination",
                "You can only rotate the master user password when it's "
                "managed by RDS in AWS Secrets Manager.",
                400,
            )
        if _p(p, "ApplyImmediately") != "true":
            return _error(
                "InvalidParameterCombination",
                "You must specify apply immediately when rotating the master "
                "user password.",
                400,
            )
        new_pass = _generate_master_user_password()
        secret_updated = secretsmanager.put_secret_value_in_process(
            master_user_secret["SecretArn"],
            json.dumps({
                "username": cluster.get("MasterUsername", "admin"),
                "password": new_pass,
            }),
        )
        if not secret_updated:
            # The secret was deleted out from under RDS. AWS reports such a
            # secret as impaired: usable state is gone and it can't be rotated.
            master_user_secret["SecretStatus"] = "impaired"
            return _error(
                "InvalidDBClusterStateFault",
                f"The master user secret for DB cluster {cluster_id} "
                "can't be rotated.",
                400,
            )
        _apply_cluster_master_password_change(cluster, new_pass)
    if engine_version:
        cluster["EngineVersion"] = engine_version
    if _p(p, "Port"):
        cluster["Port"] = int(_p(p, "Port"))
    if _p(p, "BackupRetentionPeriod"):
        cluster["BackupRetentionPeriod"] = int(_p(p, "BackupRetentionPeriod"))
    if _p(p, "PreferredBackupWindow"):
        cluster["PreferredBackupWindow"] = _p(p, "PreferredBackupWindow")
    if _p(p, "PreferredMaintenanceWindow"):
        cluster["PreferredMaintenanceWindow"] = _p(p, "PreferredMaintenanceWindow")
    if _p(p, "DeletionProtection"):
        cluster["DeletionProtection"] = _p(p, "DeletionProtection") == "true"
    if _p(p, "EnableIAMDatabaseAuthentication"):
        cluster["IAMDatabaseAuthenticationEnabled"] = _p(p, "EnableIAMDatabaseAuthentication") == "true"
    if _p(p, "EnableHttpEndpoint"):
        cluster["HttpEndpointEnabled"] = _p(p, "EnableHttpEndpoint") == "true"
    if _p(p, "CopyTagsToSnapshot"):
        cluster["CopyTagsToSnapshot"] = _p(p, "CopyTagsToSnapshot") == "true"
    if _p(p, "DBClusterParameterGroupName"):
        cluster["DBClusterParameterGroup"] = _p(p, "DBClusterParameterGroupName")

    vpc_sgs = _parse_member_list(p, "VpcSecurityGroupIds")
    if vpc_sgs:
        cluster["VpcSecurityGroups"] = [
            {"VpcSecurityGroupId": sg, "Status": "active"} for sg in vpc_sgs
        ]

    return _xml(200, "ModifyDBClusterResponse",
        f"<ModifyDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></ModifyDBClusterResult>")


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def _create_snapshot_internal(snap_id, instance):
    """Internal helper — creates a snapshot dict from an instance."""
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:snapshot:{snap_id}"
    now_ts = time.time()
    snap = {
        "DBSnapshotIdentifier": snap_id,
        "DBInstanceIdentifier": instance["DBInstanceIdentifier"],
        "DBSnapshotArn": arn,
        "Engine": instance["Engine"],
        "EngineVersion": instance["EngineVersion"],
        "SnapshotCreateTime": _format_time(now_ts),
        "InstanceCreateTime": instance.get("InstanceCreateTime", _format_time(now_ts)),
        "Status": "available",
        "AllocatedStorage": instance.get("AllocatedStorage", 20),
        "AvailabilityZone": instance.get("AvailabilityZone", f"{get_region()}a"),
        "VpcId": "vpc-00000000",
        "Port": instance.get("Endpoint", {}).get("Port", 5432),
        "MasterUsername": instance.get("MasterUsername", "admin"),
        "DBName": instance.get("DBName", ""),
        "SnapshotType": "manual",
        "LicenseModel": instance.get("LicenseModel", "general-public-license"),
        "StorageType": instance.get("StorageType", "gp2"),
        "DBInstanceClass": instance.get("DBInstanceClass", "db.t3.micro"),
        "StorageEncrypted": instance.get("StorageEncrypted", False),
        "KmsKeyId": instance.get("KmsKeyId", ""),
        "Encrypted": instance.get("StorageEncrypted", False),
        "IAMDatabaseAuthenticationEnabled": instance.get("IAMDatabaseAuthenticationEnabled", False),
        "PercentProgress": 100,
        "DbiResourceId": instance.get("DbiResourceId", ""),
        "TagList": list(_tags.get(instance.get("DBInstanceArn", ""), [])),
        "OriginalSnapshotCreateTime": _format_time(now_ts),
        "SnapshotDatabaseTime": _format_time(now_ts),
        "SnapshotTarget": "region",
    }
    _snapshots[snap_id] = snap
    return snap


def _create_db_snapshot(p):
    snap_id = _p(p, "DBSnapshotIdentifier")
    db_id = _p(p, "DBInstanceIdentifier")
    if not snap_id:
        return _error("MissingParameter", "DBSnapshotIdentifier is required", 400)
    if snap_id in _snapshots:
        return _error("DBSnapshotAlreadyExists", f"Snapshot {snap_id} already exists.", 400)

    instance = _resolve_instance(db_id)
    if not instance:
        invalid_arn = _invalid_db_instance_identifier_error(db_id)
        if invalid_arn:
            return invalid_arn
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)

    snap = _create_snapshot_internal(snap_id, instance)

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[snap["DBSnapshotArn"]] = req_tags
        snap["TagList"] = req_tags

    return _xml(200, "CreateDBSnapshotResponse",
        f"<CreateDBSnapshotResult><DBSnapshot>{_snapshot_xml(snap)}</DBSnapshot></CreateDBSnapshotResult>")


def _delete_db_snapshot(p):
    snap_id = _p(p, "DBSnapshotIdentifier")
    snap = _snapshots.pop(snap_id, None)
    if not snap:
        return _error("DBSnapshotNotFound", f"Snapshot {snap_id} not found.", 404)
    _tags.pop(snap.get("DBSnapshotArn", ""), None)
    snap["Status"] = "deleted"
    return _xml(200, "DeleteDBSnapshotResponse",
        f"<DeleteDBSnapshotResult><DBSnapshot>{_snapshot_xml(snap)}</DBSnapshot></DeleteDBSnapshotResult>")


def _describe_db_snapshots(p):
    snap_id = _p(p, "DBSnapshotIdentifier")
    db_id = _p(p, "DBInstanceIdentifier")
    snap_type = _p(p, "SnapshotType")

    if snap_id:
        snap = _snapshots.get(snap_id)
        if not snap:
            return _error("DBSnapshotNotFound", f"Snapshot {snap_id} not found.", 404)
        snaps = [snap]
    else:
        snaps = list(_snapshots.values())
        if db_id:
            invalid_arn = _invalid_db_instance_identifier_error(db_id)
            if invalid_arn:
                return invalid_arn
            filter_by_arn = _parse_rds_arn(db_id) is not None
            db_id = _request_region_resource_identifier(db_id, "db")
            snaps = [s for s in snaps if s["DBInstanceIdentifier"] == db_id]
            if filter_by_arn:
                snaps = [s for s in snaps if _record_arn_in_request_scope(s, "DBSnapshotArn")]
        if snap_type:
            snaps = [s for s in snaps if s["SnapshotType"] == snap_type]

    members = "".join(f"<DBSnapshot>{_snapshot_xml(s)}</DBSnapshot>" for s in snaps)
    return _xml(200, "DescribeDBSnapshotsResponse",
        f"<DescribeDBSnapshotsResult><DBSnapshots>{members}</DBSnapshots></DescribeDBSnapshotsResult>")


# ---------------------------------------------------------------------------
# Subnet Groups
# ---------------------------------------------------------------------------

_INVALID_SUBNET_MESSAGE = (
    "The requested subnet is invalid, or multiple subnets were requested that "
    "are not all in a common VPC."
)


def _resolve_subnet_group_members(subnet_ids):
    from ministack.services import ec2

    ec2._ensure_defaults_initialized()
    subnets = []
    vpc_ids = set()
    for subnet_id in subnet_ids:
        subnet = ec2._subnets.get(subnet_id)
        if not subnet:
            return None
        vpc_ids.add(subnet["VpcId"])
        subnets.append({
            "SubnetIdentifier": subnet_id,
            "SubnetAvailabilityZone": {
                "Name": subnet.get("AvailabilityZone", f"{get_region()}a")
            },
            "SubnetOutpost": {},
            "SubnetStatus": "Active",
        })
    if len(vpc_ids) != 1:
        return None
    return subnets, vpc_ids.pop()

def _create_subnet_group(p):
    name = _p(p, "DBSubnetGroupName")
    if not name:
        return _error("MissingParameter", "DBSubnetGroupName is required", 400)
    desc = _p(p, "DBSubnetGroupDescription") or name
    subnet_ids = _parse_member_list(p, "SubnetIds")
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:subgrp:{name}"

    resolved_subnets = _resolve_subnet_group_members(subnet_ids)
    if resolved_subnets is None:
        return _error("InvalidSubnet", _INVALID_SUBNET_MESSAGE, 400)
    subnets, vpc_id = resolved_subnets

    _subnet_groups[name] = {
        "DBSubnetGroupName": name,
        "DBSubnetGroupDescription": desc,
        "VpcId": vpc_id,
        "SubnetGroupStatus": "Complete",
        "Subnets": subnets,
        "DBSubnetGroupArn": arn,
        "SupportedNetworkTypes": ["IPV4"],
    }

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[arn] = req_tags

    sg = _subnet_groups[name]
    return _xml(200, "CreateDBSubnetGroupResponse",
        f"<CreateDBSubnetGroupResult><DBSubnetGroup>{_subnet_group_xml(sg)}</DBSubnetGroup></CreateDBSubnetGroupResult>")


def _delete_subnet_group(p):
    name = _p(p, "DBSubnetGroupName")
    sg = _subnet_groups.pop(name, None)
    if not sg:
        return _error("DBSubnetGroupNotFoundFault", f"Subnet group {name} not found.", 404)
    _tags.pop(sg.get("DBSubnetGroupArn", ""), None)
    return _xml(200, "DeleteDBSubnetGroupResponse", "")


def _describe_subnet_groups(p):
    name = _p(p, "DBSubnetGroupName")
    if name:
        sg = _subnet_groups.get(name)
        if not sg:
            return _error("DBSubnetGroupNotFoundFault", f"Subnet group {name} not found.", 404)
        groups = [sg]
    else:
        groups = list(_subnet_groups.values())

    members = "".join(
        f"<DBSubnetGroup>{_subnet_group_xml(g)}</DBSubnetGroup>" for g in groups
    )
    return _xml(200, "DescribeDBSubnetGroupsResponse",
        f"<DescribeDBSubnetGroupsResult><DBSubnetGroups>{members}</DBSubnetGroups></DescribeDBSubnetGroupsResult>")


# ---------------------------------------------------------------------------
# Parameter Groups
# ---------------------------------------------------------------------------

def _create_param_group(p):
    name = _p(p, "DBParameterGroupName")
    if not name:
        return _error("MissingParameter", "DBParameterGroupName is required", 400)
    family = _p(p, "DBParameterGroupFamily") or "postgres15"
    desc = _p(p, "Description") or name
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:pg:{name}"

    _param_groups[name] = {
        "DBParameterGroupName": name,
        "DBParameterGroupFamily": family,
        "Description": desc,
        "DBParameterGroupArn": arn,
        "Parameters": {},
    }

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[arn] = req_tags

    return _xml(200, "CreateDBParameterGroupResponse",
        f"""<CreateDBParameterGroupResult><DBParameterGroup>
            <DBParameterGroupName>{name}</DBParameterGroupName>
            <DBParameterGroupFamily>{family}</DBParameterGroupFamily>
            <Description>{_esc(desc)}</Description>
            <DBParameterGroupArn>{arn}</DBParameterGroupArn>
        </DBParameterGroup></CreateDBParameterGroupResult>""")


def _delete_param_group(p):
    name = _p(p, "DBParameterGroupName")
    pg = _param_groups.pop(name, None)
    if not pg:
        return _error("DBParameterGroupNotFound", f"Parameter group {name} not found.", 404)
    _tags.pop(pg.get("DBParameterGroupArn", ""), None)
    return _xml(200, "DeleteDBParameterGroupResponse", "")


def _describe_param_groups(p):
    name = _p(p, "DBParameterGroupName")
    if name:
        pg = _param_groups.get(name)
        if not pg:
            return _error("DBParameterGroupNotFound", f"Parameter group {name} not found.", 404)
        groups = [pg]
    else:
        groups = list(_param_groups.values())

    members = "".join(f"""<DBParameterGroup>
        <DBParameterGroupName>{g['DBParameterGroupName']}</DBParameterGroupName>
        <DBParameterGroupFamily>{g['DBParameterGroupFamily']}</DBParameterGroupFamily>
        <Description>{_esc(g['Description'])}</Description>
        <DBParameterGroupArn>{g.get('DBParameterGroupArn','')}</DBParameterGroupArn>
    </DBParameterGroup>""" for g in groups)
    return _xml(200, "DescribeDBParameterGroupsResponse",
        f"<DescribeDBParameterGroupsResult><DBParameterGroups>{members}</DBParameterGroups></DescribeDBParameterGroupsResult>")


def _describe_db_parameters(p):
    name = _p(p, "DBParameterGroupName")
    pg = _param_groups.get(name)
    if not pg:
        return _error("DBParameterGroupNotFound", f"Parameter group {name} not found.", 404)

    source_filter = _p(p, "Source")  # "user", "engine-default", or None (all)
    params_xml = _parameter_group_parameters_xml(pg, source_filter)

    return _xml(200, "DescribeDBParametersResponse",
        f"<DescribeDBParametersResult><Parameters>{params_xml}</Parameters></DescribeDBParametersResult>")


# ---------------------------------------------------------------------------
# ModifyDBParameterGroup
# ---------------------------------------------------------------------------

def _modify_param_group(p):
    name = _p(p, "DBParameterGroupName")
    pg = _param_groups.get(name)
    if not pg:
        return _error("DBParameterGroupNotFound", f"Parameter group {name} not found.", 404)

    params = pg.setdefault("Parameters", {})
    prefix = _parameter_member_prefix(p)
    idx = 1
    while _p(p, f"{prefix}.{idx}.ParameterName"):
        pname = _p(p, f"{prefix}.{idx}.ParameterName")
        pvalue = _p(p, f"{prefix}.{idx}.ParameterValue")
        apply_method = _p(p, f"{prefix}.{idx}.ApplyMethod") or "immediate"
        params[pname] = {"ParameterValue": pvalue, "ApplyMethod": apply_method}
        idx += 1

    return _xml(200, "ModifyDBParameterGroupResponse",
        f"<ModifyDBParameterGroupResult><DBParameterGroupName>{name}</DBParameterGroupName></ModifyDBParameterGroupResult>")


def _reset_param_group(p):
    name = _p(p, "DBParameterGroupName")
    pg = _param_groups.get(name)
    if not pg:
        return _error("DBParameterGroupNotFound", f"Parameter group {name} not found.", 404)

    params = pg.setdefault("Parameters", {})
    prefix = _parameter_member_prefix(p)
    has_explicit_parameters = bool(_p(p, f"{prefix}.1.ParameterName"))
    reset_all = _p(p, "ResetAllParameters", "").lower() == "true"
    if reset_all and has_explicit_parameters:
        return _error(
            "InvalidParameterCombination",
            "You can't specify both ResetAllParameters and Parameters.",
            400,
        )

    if reset_all or not has_explicit_parameters:
        params.clear()
    else:
        idx = 1
        while _p(p, f"{prefix}.{idx}.ParameterName"):
            params.pop(_p(p, f"{prefix}.{idx}.ParameterName"), None)
            idx += 1

    return _xml(200, "ResetDBParameterGroupResponse",
        f"<ResetDBParameterGroupResult><DBParameterGroupName>{name}</DBParameterGroupName></ResetDBParameterGroupResult>")


# ---------------------------------------------------------------------------
# DB Cluster Parameter Groups
# ---------------------------------------------------------------------------

def _create_db_cluster_param_group(p):
    name = _p(p, "DBClusterParameterGroupName")
    if not name:
        return _error("MissingParameter", "DBClusterParameterGroupName is required", 400)
    family = _p(p, "DBParameterGroupFamily") or "aurora-postgresql15"
    desc = _p(p, "Description") or name
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:cluster-pg:{name}"

    _db_cluster_param_groups[name] = {
        "DBClusterParameterGroupName": name,
        "DBParameterGroupFamily": family,
        "Description": desc,
        "DBClusterParameterGroupArn": arn,
        "Parameters": {},
    }

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[arn] = req_tags

    return _xml(200, "CreateDBClusterParameterGroupResponse",
        f"""<CreateDBClusterParameterGroupResult><DBClusterParameterGroup>
            <DBClusterParameterGroupName>{name}</DBClusterParameterGroupName>
            <DBParameterGroupFamily>{family}</DBParameterGroupFamily>
            <Description>{_esc(desc)}</Description>
            <DBClusterParameterGroupArn>{arn}</DBClusterParameterGroupArn>
        </DBClusterParameterGroup></CreateDBClusterParameterGroupResult>""")


def _describe_db_cluster_param_groups(p):
    name = _p(p, "DBClusterParameterGroupName")
    if name:
        pg = _db_cluster_param_groups.get(name)
        if not pg:
            return _error("DBParameterGroupNotFound",
                f"DB cluster parameter group {name} not found.", 404)
        groups = [pg]
    else:
        groups = list(_db_cluster_param_groups.values())

    members = "".join(f"""<DBClusterParameterGroup>
        <DBClusterParameterGroupName>{g['DBClusterParameterGroupName']}</DBClusterParameterGroupName>
        <DBParameterGroupFamily>{g['DBParameterGroupFamily']}</DBParameterGroupFamily>
        <Description>{_esc(g['Description'])}</Description>
        <DBClusterParameterGroupArn>{g.get('DBClusterParameterGroupArn','')}</DBClusterParameterGroupArn>
    </DBClusterParameterGroup>""" for g in groups)
    return _xml(200, "DescribeDBClusterParameterGroupsResponse",
        f"<DescribeDBClusterParameterGroupsResult><DBClusterParameterGroups>{members}</DBClusterParameterGroups></DescribeDBClusterParameterGroupsResult>")


def _delete_db_cluster_param_group(p):
    name = _p(p, "DBClusterParameterGroupName")
    pg = _db_cluster_param_groups.pop(name, None)
    if not pg:
        return _error("DBParameterGroupNotFound",
            f"DB cluster parameter group {name} not found.", 404)
    _tags.pop(pg.get("DBClusterParameterGroupArn", ""), None)
    return _xml(200, "DeleteDBClusterParameterGroupResponse", "")


def _describe_db_cluster_parameters(p):
    name = _p(p, "DBClusterParameterGroupName")
    source_filter = _p(p, "Source")
    pg = _db_cluster_param_groups.get(name)
    if not pg:
        return _error("DBParameterGroupNotFound",
            f"DB cluster parameter group {name} not found.", 404)
    members = _parameter_group_parameters_xml(pg, source_filter)
    return _xml(200, "DescribeDBClusterParametersResponse",
        f"<DescribeDBClusterParametersResult><Parameters>{members}</Parameters></DescribeDBClusterParametersResult>")


def _modify_db_cluster_param_group(p):
    name = _p(p, "DBClusterParameterGroupName")
    pg = _db_cluster_param_groups.get(name)
    if not pg:
        return _error("DBParameterGroupNotFound",
            f"DB cluster parameter group {name} not found.", 404)

    params = pg.setdefault("Parameters", {})
    prefix = _parameter_member_prefix(p)
    idx = 1
    while _p(p, f"{prefix}.{idx}.ParameterName"):
        pname = _p(p, f"{prefix}.{idx}.ParameterName")
        pvalue = _p(p, f"{prefix}.{idx}.ParameterValue")
        apply_method = _p(p, f"{prefix}.{idx}.ApplyMethod") or "immediate"
        params[pname] = {"ParameterValue": pvalue, "ApplyMethod": apply_method}
        idx += 1

    return _xml(200, "ModifyDBClusterParameterGroupResponse",
        f"<ModifyDBClusterParameterGroupResult><DBClusterParameterGroupName>{name}</DBClusterParameterGroupName></ModifyDBClusterParameterGroupResult>")


def _reset_db_cluster_param_group(p):
    name = _p(p, "DBClusterParameterGroupName")
    pg = _db_cluster_param_groups.get(name)
    if not pg:
        return _error("DBParameterGroupNotFound",
            f"DB cluster parameter group {name} not found.", 404)

    params = pg.setdefault("Parameters", {})
    prefix = _parameter_member_prefix(p)
    has_explicit_parameters = bool(_p(p, f"{prefix}.1.ParameterName"))
    reset_all = _p(p, "ResetAllParameters", "").lower() == "true"
    if reset_all and has_explicit_parameters:
        return _error(
            "InvalidParameterCombination",
            "You can't specify both ResetAllParameters and Parameters.",
            400,
        )

    if reset_all or not has_explicit_parameters:
        params.clear()
    else:
        idx = 1
        while _p(p, f"{prefix}.{idx}.ParameterName"):
            params.pop(_p(p, f"{prefix}.{idx}.ParameterName"), None)
            idx += 1

    return _xml(200, "ResetDBClusterParameterGroupResponse",
        f"<ResetDBClusterParameterGroupResult><DBClusterParameterGroupName>{name}</DBClusterParameterGroupName></ResetDBClusterParameterGroupResult>")


# ---------------------------------------------------------------------------
# DB Cluster Snapshots
# ---------------------------------------------------------------------------

def _create_db_cluster_snapshot(p):
    snap_id = _p(p, "DBClusterSnapshotIdentifier")
    cluster_id = _p(p, "DBClusterIdentifier")
    if not snap_id:
        return _error("MissingParameter", "DBClusterSnapshotIdentifier is required", 400)
    if snap_id in _db_cluster_snapshots:
        return _error("DBClusterSnapshotAlreadyExistsFault",
            f"DB cluster snapshot {snap_id} already exists.", 400)

    cluster = _resolve_cluster_in_request_region(cluster_id)
    if not cluster:
        wrong_region = _invalid_region_arn_error(cluster_id, "DBClusterIdentifier")
        if wrong_region:
            return wrong_region
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    cluster_id = cluster["DBClusterIdentifier"]

    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:cluster-snapshot:{snap_id}"
    now_ts = time.time()
    snap = {
        "DBClusterSnapshotIdentifier": snap_id,
        "DBClusterIdentifier": cluster_id,
        "DBClusterSnapshotArn": arn,
        "Engine": cluster["Engine"],
        "EngineVersion": cluster["EngineVersion"],
        "SnapshotCreateTime": _format_time(now_ts),
        "ClusterCreateTime": cluster.get("ClusterCreateTime", _format_time(now_ts)),
        "Status": "available",
        "Port": cluster.get("Port", 5432),
        "VpcId": "vpc-00000000",
        "MasterUsername": cluster.get("MasterUsername", "admin"),
        "SnapshotType": "manual",
        "PercentProgress": 100,
        "StorageEncrypted": cluster.get("StorageEncrypted", False),
        "KmsKeyId": cluster.get("KmsKeyId", ""),
        "AvailabilityZones": cluster.get("AvailabilityZones", []),
        "LicenseModel": _license_model(cluster.get("Engine", "aurora-postgresql")),
        "TagList": list(_tags.get(cluster.get("DBClusterArn", ""), [])),
        "DbClusterResourceId": cluster.get("DbClusterResourceId", ""),
        "IAMDatabaseAuthenticationEnabled": cluster.get("IAMDatabaseAuthenticationEnabled", False),
        "AllocatedStorage": cluster.get("AllocatedStorage", 1),
    }
    _db_cluster_snapshots[snap_id] = snap

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[arn] = req_tags
        snap["TagList"] = req_tags

    return _xml(200, "CreateDBClusterSnapshotResponse",
        f"<CreateDBClusterSnapshotResult><DBClusterSnapshot>{_cluster_snapshot_xml(snap)}</DBClusterSnapshot></CreateDBClusterSnapshotResult>")


def _describe_db_cluster_snapshots(p):
    snap_id = _p(p, "DBClusterSnapshotIdentifier")
    cluster_id = _p(p, "DBClusterIdentifier")
    snap_type = _p(p, "SnapshotType")

    if snap_id:
        snap = _db_cluster_snapshots.get(snap_id)
        if not snap:
            return _error("DBClusterSnapshotNotFoundFault",
                f"DB cluster snapshot {snap_id} not found.", 404)
        snaps = [snap]
    else:
        snaps = list(_db_cluster_snapshots.values())
        if cluster_id:
            wrong_region = _invalid_region_arn_error(cluster_id, "DBClusterIdentifier")
            if wrong_region:
                return wrong_region
            filter_by_arn = _parse_rds_arn(cluster_id) is not None
            cluster_id = _request_region_resource_identifier(cluster_id, "cluster")
            snaps = [s for s in snaps if s["DBClusterIdentifier"] == cluster_id]
            if filter_by_arn:
                snaps = [
                    s for s in snaps
                    if _record_arn_in_request_scope(s, "DBClusterSnapshotArn")
                ]
        if snap_type:
            snaps = [s for s in snaps if s["SnapshotType"] == snap_type]

    members = "".join(
        f"<DBClusterSnapshot>{_cluster_snapshot_xml(s)}</DBClusterSnapshot>" for s in snaps)
    return _xml(200, "DescribeDBClusterSnapshotsResponse",
        f"<DescribeDBClusterSnapshotsResult><DBClusterSnapshots>{members}</DBClusterSnapshots></DescribeDBClusterSnapshotsResult>")


def _delete_db_cluster_snapshot(p):
    snap_id = _p(p, "DBClusterSnapshotIdentifier")
    snap = _db_cluster_snapshots.pop(snap_id, None)
    if not snap:
        return _error("DBClusterSnapshotNotFoundFault",
            f"DB cluster snapshot {snap_id} not found.", 404)
    _tags.pop(snap.get("DBClusterSnapshotArn", ""), None)
    snap["Status"] = "deleted"
    return _xml(200, "DeleteDBClusterSnapshotResponse",
        f"<DeleteDBClusterSnapshotResult><DBClusterSnapshot>{_cluster_snapshot_xml(snap)}</DBClusterSnapshot></DeleteDBClusterSnapshotResult>")


# ---------------------------------------------------------------------------
# ModifyDBSubnetGroup
# ---------------------------------------------------------------------------

def _modify_subnet_group(p):
    name = _p(p, "DBSubnetGroupName")
    sg = _subnet_groups.get(name)
    if not sg:
        return _error("DBSubnetGroupNotFoundFault", f"Subnet group {name} not found.", 404)

    subnet_ids = _parse_member_list(p, "SubnetIds")
    resolved_subnets = None
    if subnet_ids:
        resolved_subnets = _resolve_subnet_group_members(subnet_ids)
        if resolved_subnets is None:
            return _error("InvalidSubnet", _INVALID_SUBNET_MESSAGE, 400)
        stored_vpc_id = sg.get("VpcId")
        if (
            stored_vpc_id not in (None, "", "vpc-00000000")
            and resolved_subnets[1] != stored_vpc_id
        ):
            return _error("InvalidSubnet", _INVALID_SUBNET_MESSAGE, 400)

    if _p(p, "DBSubnetGroupDescription"):
        sg["DBSubnetGroupDescription"] = _p(p, "DBSubnetGroupDescription")
    if resolved_subnets is not None:
        sg["Subnets"], sg["VpcId"] = resolved_subnets

    return _xml(200, "ModifyDBSubnetGroupResponse",
        f"<ModifyDBSubnetGroupResult><DBSubnetGroup>{_subnet_group_xml(sg)}</DBSubnetGroup></ModifyDBSubnetGroupResult>")


# ---------------------------------------------------------------------------
# StartDBCluster / StopDBCluster
# ---------------------------------------------------------------------------

def _cluster_member_instances(cluster):
    """Resolve a cluster's member records to their instance dicts."""
    return [
        inst
        for inst in (
            _instances.get(member.get("DBInstanceIdentifier"))
            for member in cluster.get("DBClusterMembers", [])
        )
        if inst is not None
    ]


def _start_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    cluster = _resolve_cluster_in_request_region(cluster_id)
    if not cluster:
        wrong_region = _invalid_cluster_identifier_error(cluster_id)
        if wrong_region:
            return wrong_region
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    # The request may identify the cluster by ARN; every downstream consumer
    # (the readiness worker's re-lookup, Docker container naming) needs the
    # canonical identifier.
    cluster_id = cluster["DBClusterIdentifier"]
    status = cluster.get("Status")
    if status != "stopped":
        # Real AWS message shape, verified against live transcripts:
        # "DbCluster <id> is in <state> state but expected it to be one of
        # stopped,inaccessible-encryption-credentials-recoverable."
        return _error(
            "InvalidDBClusterStateFault",
            f"DbCluster {cluster_id} is "
            f"in {status} state but expected it to be one of "
            "stopped,inaccessible-encryption-credentials-recoverable.",
            400,
        )
    global_cluster, _member = _global_cluster_member_for_cluster(cluster)
    if (
        global_cluster
        and len(global_cluster.get("GlobalClusterMembers", [])) > 1
    ):
        # Message text is verbatim from the AWS Aurora global-database
        # limitations documentation, not from a captured wire transcript.
        return _error(
            "InvalidDBClusterStateFault",
            "You can only stop and start a cluster that's part of an Aurora "
            "global database if it's the only cluster in the global database.",
            400,
        )

    member_instances = _cluster_member_instances(cluster)
    docker_client = _get_docker()
    had_compute = bool(cluster.get("_shared_container_id"))
    has_replicating_standbys = _pg_cluster_replication_enabled(cluster) and any(
        inst.get("_pg_standby") for inst in member_instances
    )
    if not docker_client or not (
        had_compute or cluster.get("_shared_storage_initialized")
    ):
        if has_replicating_standbys and (
            had_compute or cluster.get("_shared_storage_initialized")
        ):
            # The cluster has real compute (a standby only exists because a
            # reader container launched) but Docker is unreachable right
            # now — often transiently, e.g. Docker Desktop restarting.
            # Demoting here would irreversibly discard the standby identity
            # on a condition that may clear in seconds; fail the start and
            # keep everything stopped so a retry can revive the reader.
            return _error(
                "InternalFailure",
                f"Failed to start compute for DB cluster {cluster_id}.",
                500,
            )
        # Control-plane only, or the cluster never had real compute:
        # flipping the statuses is the entire start.
        cluster["_shared_container_ready"] = True
        cluster["Status"] = "available"
        for inst in member_instances:
            if inst.get("_pg_standby"):
                # No compute is coming back — replication is switched off,
                # or the cluster never had real compute — so no standby
                # container will serve this member's endpoint (#1325).
                _demote_pg_standby_to_alias(
                    inst, cluster, "no cluster compute on StartDBCluster",
                )
            inst["DBInstanceStatus"] = "available"
        # Demotion changes what the ReaderEndpoint should resolve to;
        # republish it so it does not keep advertising the former standby.
        _sync_cluster_endpoints(cluster)
        return _xml(200, "StartDBClusterResponse",
            f"<StartDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StartDBClusterResult>")

    compute_recreated = False
    if had_compute:
        start_result = _restart_cluster_shared_container(cluster_id, cluster)
        if start_result.get("failed"):
            # A preserved container can become unrestartable (for example,
            # its old host port may have been claimed). Recreate only the
            # compute layer; the cluster-owned named volume retains data.
            start_result = _start_cluster_shared_container(
                cluster_id, cluster, remove_stale=True,
            )
            compute_recreated = start_result.get("started", False)
    else:
        # Initialized storage without a container id happens after a host
        # restart: persistence drops the container id but the stable-named
        # container and volume survive. Recreate compute from them.
        start_result = _start_cluster_shared_container(
            cluster_id, cluster, remove_stale=True,
        )
        compute_recreated = start_result.get("started", False)

    if not start_result.get("started"):
        if start_result.get("failed"):
            # Both the restart and the recreate fallback genuinely failed
            # (Docker error, missing image, port exhaustion). Compute did
            # not come back: keep the cluster stopped so StartDBCluster can
            # be retried, and surface the failure instead of reporting a
            # dead endpoint as available. The cluster-owned volume is
            # untouched, so data survives for the retry.
            cluster["Status"] = "stopped"
            for inst in member_instances:
                inst["DBInstanceStatus"] = "stopped"
            return _error(
                "InternalFailure",
                f"Failed to start compute for DB cluster {cluster_id}.",
                500,
            )
        if has_replicating_standbys:
            # Docker went away between the check above and the start
            # attempt. Same reasoning as the pre-check: a transient Docker
            # miss must not irreversibly demote a standby. Keep the cluster
            # stopped and let a retry revive the reader.
            cluster["Status"] = "stopped"
            for inst in member_instances:
                inst["DBInstanceStatus"] = "stopped"
            return _error(
                "InternalFailure",
                f"Failed to start compute for DB cluster {cluster_id}.",
                500,
            )
        # Benign control-plane-only outcome: no Docker in this environment.
        cluster["_shared_container_ready"] = True
        cluster["Status"] = "available"
        for inst in member_instances:
            if inst.get("_pg_standby"):
                _demote_pg_standby_to_alias(
                    inst, cluster, "no cluster compute on StartDBCluster",
                )
            inst["DBInstanceStatus"] = "available"
        _sync_cluster_endpoints(cluster)
        return _xml(200, "StartDBClusterResponse",
            f"<StartDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StartDBClusterResult>")

    cluster["Status"] = "starting"
    for inst in member_instances:
        inst["DBInstanceStatus"] = "starting"
    _sync_cluster_endpoints(cluster)

    # Real AWS StartDBCluster returns immediately with a transitional status
    # and the caller polls until the cluster is available. Finalize readiness
    # on a daemon thread, exactly like CreateDBInstance does for first-member
    # compute. contextvars.copy_context() carries the request's account and
    # region into the daemon.
    ctx = contextvars.copy_context()

    def _bg_start_ready(
        cluster_id=cluster_id,
        container_id=cluster.get("_shared_container_id"),
        container_epoch=start_result.get("container_epoch"),
        ready_host=start_result.get("readiness_host"),
        ready_port=start_result.get("readiness_port"),
        compute_recreated=compute_recreated,
    ):
        cluster = _clusters.get(cluster_id)
        if not cluster:
            return
        standbys_to_revive = []

        def _container_alive():
            client = _get_docker()
            if not client or not container_id:
                return True
            try:
                container = client.containers.get(container_id)
                container.reload()
                return container.status not in ("exited", "dead", "removing")
            except Exception:
                return False

        engine = cluster.get("Engine", "aurora-postgresql")
        master_user = cluster.get("MasterUsername", "admin")
        master_pass = cluster.get("_MasterUserPassword", "password")
        readiness_user = master_user
        readiness_pass = master_pass
        readiness_db = cluster.get("DatabaseName") or "mydb"
        pending_rotation = cluster.get("_pending_master_password_rotation")
        if pending_rotation:
            readiness_pass = pending_rotation["old_password"]
        root_password = readiness_pass
        if _mysql_replication_secondary(cluster):
            readiness_user = "root"
            if cluster.get("_mysql_control_user_ready"):
                readiness_user = _MYSQL_CONTROL_USER
                readiness_pass = _MYSQL_CONTROL_PASSWORD
            readiness_db = None
        database_ready = _wait_for_database_ready(
            ready_host, ready_port, engine, readiness_user,
            readiness_pass, readiness_db, _container_alive,
        )
        if database_ready and _is_mysql_engine(engine):
            _ensure_mysql_compatibility(
                container_id,
                ready_host,
                ready_port,
                root_password,
                cluster.get("EngineVersion")
                or _default_engine_version(engine),
                cluster_id,
                engine=engine,
            )
        with _shared_container_lock:
            if (
                cluster.get("_shared_container_epoch") != container_epoch
                or cluster.get("_shared_container_id") != container_id
            ):
                logger.info(
                    "RDS: ignoring stale start readiness result for cluster "
                    "%s epoch %s container %s",
                    cluster_id, container_epoch, container_id,
                )
                return

            # Real AWS lands a failed start back on ``stopped`` — never on a
            # transitional status — so StartDBCluster can simply be retried.
            def _fail_start():
                # Stop the failed container first (a failed password rotation
                # leaves it running and reachable; for an already-exited
                # container this is a no-op) so the ``stopped`` status is
                # honest the moment it becomes visible and a retried
                # StartDBCluster begins from cleanly stopped compute. Holding
                # _shared_container_lock across the Docker stop matches
                # _stop_cluster_shared_container and keeps a concurrent retry
                # from racing the stop.
                client = _get_docker()
                if client and container_id:
                    try:
                        failed = client.containers.get(container_id)
                        failed.reload()
                        if failed.status not in (
                            "created", "exited", "dead", "removing",
                        ):
                            failed.stop(timeout=5)
                    except Exception:
                        pass
                cluster["_shared_container_ready"] = False
                cluster["Status"] = "stopped"
                for inst in _cluster_member_instances(cluster):
                    inst["DBInstanceStatus"] = "stopped"

            start_failed = False
            pending_rotation = cluster.get("_pending_master_password_rotation")
            if not database_ready:
                logger.warning(
                    "RDS: cluster %s container exited before becoming "
                    "reachable after StartDBCluster", cluster_id,
                )
                _fail_start()
                start_failed = True
            elif pending_rotation and not _rotate_real_password(
                cluster,
                pending_rotation["old_password"],
                pending_rotation["new_password"],
            ):
                _fail_start()
                start_failed = True
            if not start_failed:
                if pending_rotation:
                    cluster.pop("_pending_master_password_rotation", None)
                    _sync_global_mysql_credentials(cluster)
                if _is_mysql_engine(engine) and not _mysql_replication_secondary(
                    cluster,
                ):
                    _grant_mysql_master_user_privileges(
                        ready_host, ready_port, master_user,
                        cluster.get("_MasterUserPassword", master_pass),
                        cluster_id,
                    )
                cluster["_shared_container_ready"] = True
                if _aurora_mysql_8_replication_enabled(cluster):
                    if compute_recreated:
                        # A new container has a new hostname, while the
                        # persisted replica repository and relay-log names
                        # belong to the old container. Reset them before
                        # changing the replication source.
                        cluster["_mysql_replication_reset_pending"] = True
                    _configure_or_defer_mysql_replication(cluster_id, cluster)
                if not cluster.get("_shared_container_ready"):
                    _sync_cluster_endpoints(cluster)
                    _refresh_cluster_status(cluster_id)
                    return
                for inst in _cluster_member_instances(cluster):
                    if inst.get("_pg_standby"):
                        # A replicating reader's compute is revived outside
                        # the lock below (#1325); the writer becoming ready
                        # does not make a reader whose container is still
                        # stopped available.
                        standbys_to_revive.append(inst)
                        continue
                    _attach_instance_to_shared_cluster(inst, cluster)
                    inst["DBInstanceStatus"] = "available"
                _sync_cluster_endpoints(cluster)
                _refresh_cluster_status(cluster_id)
        if start_failed:
            logger.warning(
                "RDS: cluster %s failed to start; returned to stopped",
                cluster_id,
            )
            return
        for inst in standbys_to_revive:
            if not _revive_pg_reader(
                inst["DBInstanceIdentifier"], inst, cluster,
            ):
                # The failed revival landed the cluster back on stopped;
                # reviving further standbys would race that transition.
                return
        logger.info("RDS: cluster %s started and ready", cluster_id)

    threading.Thread(target=ctx.run, args=(_bg_start_ready,), daemon=True).start()

    return _xml(200, "StartDBClusterResponse",
        f"<StartDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StartDBClusterResult>")


def _stop_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    cluster = _resolve_cluster_in_request_region(cluster_id)
    if not cluster:
        wrong_region = _invalid_cluster_identifier_error(cluster_id)
        if wrong_region:
            return wrong_region
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    status = cluster.get("Status")
    if status != "available":
        # Real AWS message shape, verified against live transcripts:
        # "DbCluster <id> is in <state> state but expected it to be one of
        # available."
        return _error(
            "InvalidDBClusterStateFault",
            f"DbCluster {cluster.get('DBClusterIdentifier', cluster_id)} is "
            f"in {status} state but expected it to be one of available.",
            400,
        )

    global_cluster, _member = _global_cluster_member_for_cluster(cluster)
    if (
        global_cluster
        and len(global_cluster.get("GlobalClusterMembers", [])) > 1
    ):
        # Message text is verbatim from the AWS Aurora global-database
        # limitations documentation, not from a captured wire transcript.
        return _error(
            "InvalidDBClusterStateFault",
            "You can only stop and start a cluster that's part of an Aurora "
            "global database if it's the only cluster in the global database.",
            400,
        )
    # Publish the transitional status before touching compute so concurrent
    # reader bootstrap workers can tell a stop-in-progress from a genuinely
    # dead container and park instead of destroying preserved compute.
    cluster["Status"] = "stopping"
    if not _stop_cluster_shared_container(
        cluster.get("DBClusterIdentifier", cluster_id), cluster,
    ):
        # Compute is still running (a failed stop rolls back any container
        # it already stopped); publishing ``stopped`` here would be the
        # reachable-endpoint lie this state machine exists to prevent.
        # Restore readiness (the container is still serving) and surface the
        # failure so StopDBCluster can be retried.
        cluster["_shared_container_ready"] = True
        cluster["Status"] = "available"
        return _error(
            "InternalFailure",
            "Failed to stop compute for DB cluster "
            f"{cluster.get('DBClusterIdentifier', cluster_id)}.",
            500,
        )
    cluster["Status"] = "stopped"
    for inst in _cluster_member_instances(cluster):
        inst["DBInstanceStatus"] = "stopped"
    return _xml(200, "StopDBClusterResponse",
        f"<StopDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StopDBClusterResult>")


def _failover_db_cluster(p):
    """Force an intra-cluster failover: promote a reader member to writer.

    Metadata-only for now, mirroring ``_failover_global_cluster``: the member
    ``IsClusterWriter`` flags flip and the response reports the transitional
    ``failing-over`` cluster status (the stored status stays ``available``,
    as a follow-up DescribeDBClusters observes on AWS once the failover
    completes). Data-plane promotion of a replicating reader container is a
    follow-up (#1325); with today's shared-container clusters every member
    already points at the same process, so there is nothing to re-point.

    Error fidelity notes (wire codes from the RDS service model):
    - unknown cluster: ``DBClusterNotFoundFault`` (404)
    - cluster not ``available`` / no reader to promote:
      ``InvalidDBClusterStateFault`` (400)
    - target problems: ``InvalidDBInstanceState`` (400) — the instance-level
      code omits the ``Fault`` suffix, like ``DBInstanceAlreadyExists``
      (#1297); a target that exists nowhere is ``DBInstanceNotFound`` (404).
    """
    cluster_id = _p(p, "DBClusterIdentifier")
    cluster = _resolve_cluster_in_request_region(cluster_id)
    if not cluster:
        wrong_region = _invalid_cluster_identifier_error(cluster_id)
        if wrong_region:
            return wrong_region
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    cluster_id = cluster.get("DBClusterIdentifier", cluster_id)
    status = cluster.get("Status")
    if status != "available":
        return _error(
            "InvalidDBClusterStateFault",
            f"DbCluster {cluster_id} is in {status} state but expected it to "
            "be one of available.",
            400,
        )
    if cluster.get("_shared_legacy_migration_in_progress") or cluster.get(
        "_shared_legacy_migration_blocked",
    ):
        # restore_state's one-time legacy-storage migration reads
        # IsClusterWriter to pick which member's volume becomes the
        # cluster's adopted shared state; flipping the flag mid-migration
        # could make it adopt a reader's volume. Same gate as
        # CreateDBInstance's cluster-member path.
        return _error(
            "InvalidDBClusterStateFault",
            f"Cannot failover DB cluster {cluster_id} while legacy shared-"
            "storage migration is in progress.",
            400,
        )

    members = cluster.get("DBClusterMembers", [])
    readers = [m for m in members if not m.get("IsClusterWriter")]

    target_id = _p(p, "TargetDBInstanceIdentifier")
    if target_id:
        target = next(
            (m for m in members if m.get("DBInstanceIdentifier") == target_id),
            None,
        )
        if not target:
            if not _resolve_instance(target_id):
                return _error(
                    "DBInstanceNotFound", f"DBInstance {target_id} not found.", 404)
            return _error(
                "InvalidDBInstanceState",
                f"DBInstance {target_id} is not a member of DB cluster "
                f"{cluster_id}.",
                400,
            )
        if target.get("IsClusterWriter"):
            return _error(
                "InvalidDBInstanceState",
                f"DBInstance {target_id} is already the writer of DB cluster "
                f"{cluster_id}; specify a reader instance to promote.",
                400,
            )
        target_instance = _resolve_instance(target_id)
        if target_instance is None:
            # A member with no backing instance record cannot happen today
            # (deletion unregisters the member synchronously), but promoting
            # a phantom would strand the cluster; refuse defensively.
            return _error(
                "DBInstanceNotFound", f"DBInstance {target_id} not found.", 404)
        if target_instance.get("DBInstanceStatus") != "available":
            return _error(
                "InvalidDBInstanceState",
                f"DBInstance {target_id} is in "
                f"{target_instance.get('DBInstanceStatus')} state but expected "
                "it to be one of available.",
                400,
            )
    else:
        # AWS promotes the best candidate from the lowest promotion tier;
        # within a tier Ministack keeps member order (no instance sizing).
        def _promotable(member):
            inst = _resolve_instance(member.get("DBInstanceIdentifier", ""))
            return inst is not None and inst.get("DBInstanceStatus") == "available"

        candidates = sorted(
            (m for m in readers if _promotable(m)),
            key=lambda m: int(m.get("PromotionTier", 1)),
        )
        if not candidates:
            return _error(
                "InvalidDBClusterStateFault",
                f"Cannot failover DB cluster {cluster_id} because it has no "
                "available reader instance to promote.",
                400,
            )
        target = candidates[0]
        target_id = target["DBInstanceIdentifier"]

    for member in members:
        member["IsClusterWriter"] = (
            member.get("DBInstanceIdentifier") == target_id
        )

    response_cluster = copy.deepcopy(cluster)
    response_cluster["Status"] = "failing-over"
    return _xml(200, "FailoverDBClusterResponse",
        f"<FailoverDBClusterResult><DBCluster>{_cluster_xml(response_cluster)}</DBCluster></FailoverDBClusterResult>")


# ---------------------------------------------------------------------------
# Option Groups
# ---------------------------------------------------------------------------

def _create_option_group(p):
    name = _p(p, "OptionGroupName")
    if not name:
        return _error("MissingParameter", "OptionGroupName is required", 400)
    if name in _option_groups:
        return _error("OptionGroupAlreadyExistsFault",
            f"Option group {name} already exists.", 400)

    engine = _p(p, "EngineName") or "postgres"
    major_version = _p(p, "MajorEngineVersion") or "15"
    desc = _p(p, "OptionGroupDescription") or name
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:og:{name}"

    _option_groups[name] = {
        "OptionGroupName": name,
        "OptionGroupDescription": desc,
        "EngineName": engine,
        "MajorEngineVersion": major_version,
        "Options": [],
        "AllowsVpcAndNonVpcInstanceMemberships": True,
        "VpcId": "",
        "OptionGroupArn": arn,
        "SourceAccountId": "",
        "SourceOptionGroup": "",
    }

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[arn] = req_tags

    og = _option_groups[name]
    return _xml(200, "CreateOptionGroupResponse",
        f"<CreateOptionGroupResult><OptionGroup>{_option_group_xml(og)}</OptionGroup></CreateOptionGroupResult>")


def _delete_option_group(p):
    name = _p(p, "OptionGroupName")
    og = _option_groups.pop(name, None)
    if not og:
        return _error("OptionGroupNotFoundFault", f"Option group {name} not found.", 404)
    _tags.pop(og.get("OptionGroupArn", ""), None)
    return _xml(200, "DeleteOptionGroupResponse", "")


def _describe_option_groups(p):
    name = _p(p, "OptionGroupName")
    engine = _p(p, "EngineName")
    major_version = _p(p, "MajorEngineVersion")

    if name:
        og = _option_groups.get(name)
        if not og:
            return _error("OptionGroupNotFoundFault", f"Option group {name} not found.", 404)
        groups = [og]
    else:
        groups = list(_option_groups.values())
        if engine:
            groups = [g for g in groups if g["EngineName"] == engine]
        if major_version:
            groups = [g for g in groups if g["MajorEngineVersion"] == major_version]

    members = "".join(
        f"<OptionGroup>{_option_group_xml(g)}</OptionGroup>" for g in groups)
    return _xml(200, "DescribeOptionGroupsResponse",
        f"<DescribeOptionGroupsResult><OptionGroupsList>{members}</OptionGroupsList></DescribeOptionGroupsResult>")


def _describe_option_group_options(p):
    return _xml(200, "DescribeOptionGroupOptionsResponse",
        "<DescribeOptionGroupOptionsResult><OptionGroupOptions/></DescribeOptionGroupOptionsResult>")


# ---------------------------------------------------------------------------
# Maintenance actions
# ---------------------------------------------------------------------------

def _describe_pending_maintenance_actions(p):
    return _xml(200, "DescribePendingMaintenanceActionsResponse",
        "<DescribePendingMaintenanceActionsResult><PendingMaintenanceActions/></DescribePendingMaintenanceActionsResult>")


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource_scope_error(arn):
    parsed = _parse_rds_arn(arn)
    if not parsed:
        return None
    spec, resource_type, _ = parsed
    if spec.account_id != get_account_id():
        return _error(
            "InvalidParameterValue",
            "The specified resource name does not match an RDS resource in this region.",
            400,
        )
    if resource_type != "global-cluster" and spec.region and spec.region != get_region():
        return _error(
            "InvalidParameterValue",
            "The specified resource name does not match an RDS resource in this region.",
            400,
        )
    return None


def _add_tags(p):
    arn = _p(p, "ResourceName")
    new_tags = _parse_tags(p)
    if not arn:
        return _error("MissingParameter", "ResourceName is required", 400)
    scope_error = _tag_resource_scope_error(arn)
    if scope_error:
        return scope_error

    existing = _tags.get(arn, [])
    existing_keys = {t["Key"]: i for i, t in enumerate(existing)}
    for tag in new_tags:
        k = tag["Key"]
        if k in existing_keys:
            existing[existing_keys[k]] = tag
        else:
            existing.append(tag)
            existing_keys[k] = len(existing) - 1
    _tags[arn] = existing

    _sync_tag_list_to_resource(arn)
    return _xml(200, "AddTagsToResourceResponse", "")


def _remove_tags(p):
    arn = _p(p, "ResourceName")
    keys_to_remove = set(_parse_member_list(p, "TagKeys"))
    if not arn:
        return _error("MissingParameter", "ResourceName is required", 400)
    scope_error = _tag_resource_scope_error(arn)
    if scope_error:
        return scope_error

    existing = _tags.get(arn, [])
    _tags[arn] = [t for t in existing if t["Key"] not in keys_to_remove]

    _sync_tag_list_to_resource(arn)
    return _xml(200, "RemoveTagsFromResourceResponse", "")


def _list_tags(p):
    arn = _p(p, "ResourceName")
    if not arn:
        return _xml(200, "ListTagsForResourceResponse",
            "<ListTagsForResourceResult><TagList/></ListTagsForResourceResult>")
    scope_error = _tag_resource_scope_error(arn)
    if scope_error:
        return scope_error

    tag_list = _tags.get(arn, [])
    members = "".join(f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>" for t in tag_list)
    return _xml(200, "ListTagsForResourceResponse",
        f"<ListTagsForResourceResult><TagList>{members}</TagList></ListTagsForResourceResult>")


def _sync_tag_list_to_resource(arn):
    """Keep the embedded TagList on instances/clusters in sync with _tags."""
    tag_list = _tags.get(arn, [])
    for inst in _instances.values():
        if inst.get("DBInstanceArn") == arn:
            inst["TagList"] = list(tag_list)
            return
    for cl in _clusters.values():
        if cl.get("DBClusterArn") == arn:
            cl["TagList"] = list(tag_list)
            return
    for snap in _snapshots.values():
        if snap.get("DBSnapshotArn") == arn:
            snap["TagList"] = list(tag_list)
            return


def _invalid_global_cluster_identifier_error(global_id):
    parsed = _parse_rds_arn(global_id)
    if not parsed:
        return None
    _, resource_type, _ = parsed
    if resource_type != "global-cluster":
        return None
    return _error("InvalidParameterValue", f"Invalid global cluster identifier:  {global_id}", 400)


# ---------------------------------------------------------------------------
# Global Clusters
#
# Emulation scope: Aurora Global Database membership plus native MySQL 8 GTID
# replication between provisioned members. Failover and switchover remain
# metadata-only; their data-plane promotion behavior is a separate phase.
# ---------------------------------------------------------------------------

def _create_global_cluster(p):
    gc_id = _p(p, "GlobalClusterIdentifier")
    if not gc_id:
        return _error("MissingParameter", "GlobalClusterIdentifier is required", 400)
    invalid_id = _invalid_global_cluster_identifier_error(gc_id)
    if invalid_id:
        return invalid_id
    if gc_id in _global_clusters:
        return _error("GlobalClusterAlreadyExistsFault",
            f"Global cluster {gc_id} already exists.", 400)

    engine = _p(p, "Engine") or "aurora-postgresql"
    engine_version = _p(p, "EngineVersion") or _default_engine_version(engine)
    source_cluster_id = _p(p, "SourceDBClusterIdentifier")
    storage_encrypted = _p(p, "StorageEncrypted") == "true"
    deletion_protection = _p(p, "DeletionProtection") == "true"

    arn = f"arn:aws:rds::{get_account_id()}:global-cluster:{gc_id}"
    resource_id = f"cluster-{new_uuid().replace('-', '')[:20].lower()}"

    source_cluster = None
    if source_cluster_id:
        source_cluster = _resolve_cluster_in_request_region(source_cluster_id)
        if not source_cluster:
            wrong_region = _invalid_region_arn_error(
                source_cluster_id,
                "SourceDBClusterIdentifier",
            )
            if wrong_region:
                return wrong_region
            return _error("DBClusterNotFoundFault",
                f"DBCluster {source_cluster_id} not found.", 404)
        existing_global_id = source_cluster.get("GlobalClusterIdentifier")
        if existing_global_id:
            return _error(
                "InvalidDBClusterStateFault",
                f"DBCluster {source_cluster_id} is already a member of "
                f"global cluster {existing_global_id}.",
                400,
            )
        engine = source_cluster["Engine"]
        engine_version = source_cluster["EngineVersion"]

    # Validate before _prepare_mysql_gtid_history runs: a rejected version
    # must not leave GTID markers on the source cluster when no global
    # cluster is created.
    engine_version_error = _unsupported_aurora_engine_version_error(engine, engine_version)
    if engine_version_error:
        return engine_version_error

    if source_cluster is not None and (
        _aurora_mysql_8_replication_enabled(source_cluster)
        and not _prepare_mysql_gtid_history(source_cluster)
    ):
        return _error(
            "InvalidDBClusterStateFault",
            "The source cluster volume predates GTID-at-creation tracking "
            "and cannot safely become a global writer.",
            400,
        )

    gc = {
        "GlobalClusterIdentifier": gc_id,
        "GlobalClusterArn": arn,
        "GlobalClusterResourceId": resource_id,
        "Engine": engine,
        "EngineVersion": engine_version,
        "Status": "available",
        "StorageEncrypted": storage_encrypted,
        "DeletionProtection": deletion_protection,
        "GlobalClusterMembers": [],
        "DatabaseName": _p(p, "DatabaseName") or "",
    }
    if source_cluster:
        _attach_cluster_to_global(gc, source_cluster, is_writer=True)
    _global_clusters[gc_id] = gc
    return _xml(200, "CreateGlobalClusterResponse",
        f"<CreateGlobalClusterResult><GlobalCluster>{_global_cluster_xml(gc)}</GlobalCluster></CreateGlobalClusterResult>")


def _describe_global_clusters(p):
    gc_id = _p(p, "GlobalClusterIdentifier")
    if gc_id:
        invalid_id = _invalid_global_cluster_identifier_error(gc_id)
        if invalid_id:
            return invalid_id
        gc = _resolve_global_cluster(gc_id)
        if not gc:
            return _error("GlobalClusterNotFoundFault",
                f"Global cluster {gc_id} not found.", 404)
        gcs = [gc]
    else:
        gcs = list(_global_clusters.values())

    members_xml = "".join(
        f"<GlobalCluster>{_global_cluster_xml(gc)}</GlobalCluster>" for gc in gcs
    )
    return _xml(200, "DescribeGlobalClustersResponse",
        f"<DescribeGlobalClustersResult><GlobalClusters>{members_xml}</GlobalClusters></DescribeGlobalClustersResult>")


def _delete_global_cluster(p):
    gc_id = _p(p, "GlobalClusterIdentifier")
    invalid_id = _invalid_global_cluster_identifier_error(gc_id)
    if invalid_id:
        return invalid_id
    gc = _resolve_global_cluster(gc_id)
    if not gc:
        return _error("GlobalClusterNotFoundFault",
            f"Global cluster {gc_id} not found.", 404)

    if gc.get("DeletionProtection"):
        return _error("InvalidParameterCombination",
            "Cannot delete a global cluster when DeletionProtection is enabled.", 400)

    if gc.get("GlobalClusterMembers"):
        return _error("InvalidGlobalClusterStateFault",
            "Global cluster still has member clusters. Remove them before deleting.", 400)

    gc["Status"] = "deleting"
    del _global_clusters[gc["GlobalClusterIdentifier"]]
    return _xml(200, "DeleteGlobalClusterResponse",
        f"<DeleteGlobalClusterResult><GlobalCluster>{_global_cluster_xml(gc)}</GlobalCluster></DeleteGlobalClusterResult>")


def _remove_from_global_cluster(p):
    gc_id = _p(p, "GlobalClusterIdentifier")
    db_cluster_id = _p(p, "DbClusterIdentifier")
    invalid_id = _invalid_global_cluster_identifier_error(gc_id)
    if invalid_id:
        return invalid_id
    gc = _resolve_global_cluster(gc_id)
    if not gc:
        return _error("GlobalClusterNotFoundFault",
            f"Global cluster {gc_id} not found.", 404)

    with _shared_container_lock:
        members = gc.get("GlobalClusterMembers", [])
        cluster = _resolve_cluster(db_cluster_id)
        db_cluster_arn = cluster["DBClusterArn"] if cluster else db_cluster_id
        member = next(
            (m for m in members if m["DBClusterArn"] == db_cluster_arn),
            None,
        )
        if not member:
            return _error("DBClusterNotFoundFault",
                f"DBCluster {db_cluster_id} is not a member of global cluster {gc_id}.", 404)
        if member.get("IsWriter") and len(members) > 1:
            return _error("InvalidGlobalClusterStateFault",
                "Cannot remove the writer DB cluster while reader members remain.", 400)

        if not cluster:
            cluster = _resolve_cluster(db_cluster_arn)
        if (
            cluster
            and not member.get("IsWriter")
            and not _detach_mysql_replication(
                cluster.get("DBClusterIdentifier", db_cluster_id),
                cluster,
            )
        ):
            return _error(
                "InvalidDBClusterStateFault",
                "Cannot detach the secondary until its replication channel "
                "is stopped and reset.",
                400,
            )

        gc["GlobalClusterMembers"] = [
            m for m in members if m["DBClusterArn"] != db_cluster_arn
        ]
        _refresh_global_cluster_readers(gc)
        if cluster:
            cluster.pop("GlobalClusterIdentifier", None)
            cluster.pop("GlobalWriteForwardingStatus", None)
            if not member.get("IsWriter"):
                _clear_mysql_replication_metadata(cluster)
                if not cluster.get("DBClusterMembers"):
                    _stop_cluster_shared_container(
                        cluster.get("DBClusterIdentifier", db_cluster_id),
                        cluster,
                    )
    return _xml(200, "RemoveFromGlobalClusterResponse",
        f"<RemoveFromGlobalClusterResult><GlobalCluster>{_global_cluster_xml(gc)}</GlobalCluster></RemoveFromGlobalClusterResult>")


def _modify_global_cluster(p):
    gc_id = _p(p, "GlobalClusterIdentifier")
    invalid_id = _invalid_global_cluster_identifier_error(gc_id)
    if invalid_id:
        return invalid_id
    gc = _resolve_global_cluster(gc_id)
    if not gc:
        return _error("GlobalClusterNotFoundFault",
            f"Global cluster {gc_id} not found.", 404)

    engine_version = _p(p, "EngineVersion")
    engine_version_error = _unsupported_aurora_engine_version_error(
        gc.get("Engine"), engine_version,
        current_version=gc.get("EngineVersion"),
    )
    if engine_version_error:
        return engine_version_error

    new_id = _p(p, "NewGlobalClusterIdentifier")
    if new_id and new_id != gc_id:
        invalid_new_id = _invalid_global_cluster_identifier_error(new_id)
        if invalid_new_id:
            return invalid_new_id
        if new_id in _global_clusters:
            return _error("GlobalClusterAlreadyExistsFault",
                f"Global cluster {new_id} already exists.", 400)
        old_id = gc["GlobalClusterIdentifier"]
        gc["GlobalClusterIdentifier"] = new_id
        gc["GlobalClusterArn"] = f"arn:aws:rds::{get_account_id()}:global-cluster:{new_id}"
        _global_clusters[new_id] = gc
        del _global_clusters[old_id]
        for member in gc.get("GlobalClusterMembers", []):
            cluster = _resolve_cluster(member["DBClusterArn"])
            if cluster:
                cluster["GlobalClusterIdentifier"] = new_id

    if _p(p, "DeletionProtection"):
        gc["DeletionProtection"] = _p(p, "DeletionProtection") == "true"
    if engine_version:
        # Real AWS applies a global-database version change through
        # ModifyGlobalCluster and propagates it to every member cluster.
        gc["EngineVersion"] = engine_version
        for member in gc.get("GlobalClusterMembers", []):
            member_cluster = _resolve_cluster(member["DBClusterArn"])
            if member_cluster:
                member_cluster["EngineVersion"] = engine_version

    return _xml(200, "ModifyGlobalClusterResponse",
        f"<ModifyGlobalClusterResult><GlobalCluster>{_global_cluster_xml(gc)}</GlobalCluster></ModifyGlobalClusterResult>")


def _find_global_cluster_target_member(gc, target_cluster_id):
    cluster = _resolve_cluster(target_cluster_id)
    db_cluster_arn = cluster["DBClusterArn"] if cluster else target_cluster_id
    members = gc.get("GlobalClusterMembers", [])
    member = next((m for m in members if m["DBClusterArn"] == db_cluster_arn), None)
    return cluster, member


def _switch_global_cluster_writer(p, *, allow_data_loss=False):
    gc_id = _p(p, "GlobalClusterIdentifier")
    target_cluster_id = _p(p, "TargetDbClusterIdentifier")
    invalid_id = _invalid_global_cluster_identifier_error(gc_id)
    if invalid_id:
        return invalid_id
    gc = _resolve_global_cluster(gc_id)
    if not gc:
        return _error("GlobalClusterNotFoundFault",
            f"Global cluster {gc_id} not found.", 404)
    if not target_cluster_id:
        return _error("MissingParameter", "TargetDbClusterIdentifier is required", 400)

    _target_cluster, target_member = _find_global_cluster_target_member(gc, target_cluster_id)
    if not target_member:
        cluster = _resolve_cluster(target_cluster_id)
        if not cluster:
            return _error("DBClusterNotFoundFault",
                f"DBCluster {target_cluster_id} not found.", 404)
        return _error("InvalidGlobalClusterStateFault",
            f"DBCluster {target_cluster_id} is not a secondary member of global cluster {gc_id}.", 400)

    members = gc.get("GlobalClusterMembers", [])
    current_writer = next((m for m in members if m.get("IsWriter")), None)
    if not current_writer or target_member.get("IsWriter") or len(members) < 2:
        return _error("InvalidGlobalClusterStateFault",
            f"Global cluster {gc_id} does not have a secondary target to promote.", 400)

    _set_global_cluster_writer(gc, target_member)
    gc["Status"] = "available"

    response_gc = copy.deepcopy(gc)
    response_gc["Status"] = "switching-over" if not allow_data_loss else "failing-over"
    response_gc["FailoverState"] = {
        "Status": "pending",
        "FromDbClusterArn": current_writer["DBClusterArn"],
        "ToDbClusterArn": target_member["DBClusterArn"],
        "IsDataLossAllowed": bool(allow_data_loss),
    }
    return response_gc


def _switchover_global_cluster(p):
    result = _switch_global_cluster_writer(p, allow_data_loss=False)
    if isinstance(result, tuple):
        return result
    return _xml(200, "SwitchoverGlobalClusterResponse",
        f"<SwitchoverGlobalClusterResult><GlobalCluster>{_global_cluster_xml(result)}</GlobalCluster></SwitchoverGlobalClusterResult>")


def _failover_global_cluster(p):
    gc_id = _p(p, "GlobalClusterIdentifier")
    invalid_id = _invalid_global_cluster_identifier_error(gc_id)
    if invalid_id:
        return invalid_id
    if not _resolve_global_cluster(gc_id):
        return _error("GlobalClusterNotFoundFault",
            f"Global cluster {gc_id} not found.", 404)

    both_failover_modes_specified = "AllowDataLoss" in p and "Switchover" in p
    allow_data_loss = str(_p(p, "AllowDataLoss")).lower() == "true"
    if both_failover_modes_specified:
        return _error(
            "InvalidParameterCombination",
            "AllowDataLoss and Switchover cannot both be specified.",
            400,
        )
    result = _switch_global_cluster_writer(p, allow_data_loss=allow_data_loss)
    if isinstance(result, tuple):
        return result
    return _xml(200, "FailoverGlobalClusterResponse",
        f"<FailoverGlobalClusterResult><GlobalCluster>{_global_cluster_xml(result)}</GlobalCluster></FailoverGlobalClusterResult>")


def _enable_http_endpoint(p):
    arn = _p(p, "ResourceArn")
    wrong_region = _resource_not_found_error_for_arn(arn)
    if wrong_region:
        return wrong_region
    for cluster in _clusters.values():
        if cluster.get("DBClusterArn") == arn:
            cluster["HttpEndpointEnabled"] = True
            return _xml(200, "EnableHttpEndpointResponse",
                f"<EnableHttpEndpointResult>"
                f"<ResourceArn>{arn}</ResourceArn>"
                f"<HttpEndpointEnabled>true</HttpEndpointEnabled>"
                f"</EnableHttpEndpointResult>")
    return _error("DBClusterNotFoundFault", f"Cluster with ARN {arn} not found.", 404)


def _disable_http_endpoint(p):
    arn = _p(p, "ResourceArn")
    wrong_region = _resource_not_found_error_for_arn(arn)
    if wrong_region:
        return wrong_region
    for cluster in _clusters.values():
        if cluster.get("DBClusterArn") == arn:
            cluster["HttpEndpointEnabled"] = False
            return _xml(200, "DisableHttpEndpointResponse",
                f"<DisableHttpEndpointResult>"
                f"<ResourceArn>{arn}</ResourceArn>"
                f"<HttpEndpointEnabled>false</HttpEndpointEnabled>"
                f"</DisableHttpEndpointResult>")
    return _error("DBClusterNotFoundFault", f"Cluster with ARN {arn} not found.", 404)


def _global_cluster_xml(gc):
    _refresh_global_cluster_readers(gc)
    member_xml = ""
    for m in gc.get("GlobalClusterMembers", []):
        readers_xml = "".join(f"<member>{_esc(reader)}</member>" for reader in m.get("Readers", []))
        member_xml += f"""<GlobalClusterMember>
            <DBClusterArn>{m['DBClusterArn']}</DBClusterArn>
            <Readers>{readers_xml}</Readers>
            <IsWriter>{str(m.get('IsWriter', False)).lower()}</IsWriter>
            <GlobalWriteForwardingStatus>{m.get('GlobalWriteForwardingStatus', 'disabled')}</GlobalWriteForwardingStatus>
            <SynchronizationStatus>{m.get('SynchronizationStatus', 'connected')}</SynchronizationStatus>
        </GlobalClusterMember>"""
    failover_state = gc.get("FailoverState") or {}
    failover_state_xml = ""
    if failover_state:
        failover_state_xml = f"""<FailoverState>
            <Status>{failover_state.get('Status', '')}</Status>
            <FromDbClusterArn>{_esc(failover_state.get('FromDbClusterArn', ''))}</FromDbClusterArn>
            <ToDbClusterArn>{_esc(failover_state.get('ToDbClusterArn', ''))}</ToDbClusterArn>
            <IsDataLossAllowed>{str(failover_state.get('IsDataLossAllowed', False)).lower()}</IsDataLossAllowed>
        </FailoverState>"""
    return f"""<GlobalClusterIdentifier>{gc['GlobalClusterIdentifier']}</GlobalClusterIdentifier>
        <GlobalClusterArn>{gc['GlobalClusterArn']}</GlobalClusterArn>
        <GlobalClusterResourceId>{gc['GlobalClusterResourceId']}</GlobalClusterResourceId>
        <Engine>{gc['Engine']}</Engine>
        <EngineVersion>{gc['EngineVersion']}</EngineVersion>
        <Status>{gc['Status']}</Status>
        <DatabaseName>{gc.get('DatabaseName', '')}</DatabaseName>
        <StorageEncrypted>{str(gc.get('StorageEncrypted', False)).lower()}</StorageEncrypted>
        <DeletionProtection>{str(gc.get('DeletionProtection', False)).lower()}</DeletionProtection>
        {failover_state_xml}
        <GlobalClusterMembers>{member_xml}</GlobalClusterMembers>"""


# ---------------------------------------------------------------------------
# Engine Versions & Orderable Options
# ---------------------------------------------------------------------------

def _describe_engine_versions(p):
    engine = _p(p, "Engine") or "postgres"
    version_filter = _p(p, "EngineVersion")
    default_only = _p(p, "DefaultOnly") == "true"
    major_version_filter = (
        version_filter
        if engine == "aurora-postgresql" and version_filter.isdigit()
        else ""
    )
    default_version = (
        AURORA_POSTGRESQL_DEFAULT_ENGINE_VERSIONS.get(major_version_filter)
        if major_version_filter
        else _default_engine_version(engine)
    )
    versions_map = {
        "postgres": [
            ("18.3", "18"), ("17.5", "17"), ("16.4", "16"),
            ("15.3", "15"), ("14.8", "14"), ("13.11", "13"), ("12.15", "12"),
        ],
        "mysql": [
            ("8.0.33", "8.0"), ("8.0.28", "8.0"), ("5.7.43", "5.7"),
        ],
        "mariadb": [
            ("10.6.14", "10.6"), ("10.5.21", "10.5"),
        ],
        "aurora-postgresql": AURORA_POSTGRESQL_ENGINE_VERSIONS,
        "aurora-mysql": AURORA_MYSQL_ENGINE_VERSIONS,
    }
    versions = versions_map.get(engine, [("15.3", "15")])
    members = ""
    supports_global = engine in ("aurora-mysql", "aurora-postgresql")
    for ver, family in versions:
        if major_version_filter:
            if not ver.startswith(major_version_filter + "."):
                continue
        elif version_filter and ver != version_filter:
            continue
        if default_only and ver != default_version:
            continue
        members += f"""<DBEngineVersion>
            <Engine>{engine}</Engine>
            <EngineVersion>{ver}</EngineVersion>
            <DBParameterGroupFamily>{family}</DBParameterGroupFamily>
            <DBEngineDescription>{engine.replace('-', ' ').title()}</DBEngineDescription>
            <DBEngineVersionDescription>{engine} {ver}</DBEngineVersionDescription>
            <ValidUpgradeTarget/>
            <ExportableLogTypes/>
            <SupportsLogExportsToCloudwatchLogs>false</SupportsLogExportsToCloudwatchLogs>
            <SupportsReadReplica>true</SupportsReadReplica>
            <SupportedFeatureNames/>
            <Status>available</Status>
            <SupportsParallelQuery>false</SupportsParallelQuery>
            <SupportsGlobalDatabases>{str(supports_global).lower()}</SupportsGlobalDatabases>
            <SupportsBabelfish>false</SupportsBabelfish>
            <SupportsCertificateRotationWithoutRestart>true</SupportsCertificateRotationWithoutRestart>
        </DBEngineVersion>"""
    return _xml(200, "DescribeDBEngineVersionsResponse",
        f"<DescribeDBEngineVersionsResult><DBEngineVersions>{members}</DBEngineVersions></DescribeDBEngineVersionsResult>")


def _describe_orderable_options(p):
    engine = _p(p, "Engine") or "postgres"
    engine_version = _p(p, "EngineVersion")
    db_class = _p(p, "DBInstanceClass")
    engine_version_error = _unsupported_aurora_engine_version_error(engine, engine_version)
    if engine_version_error:
        return engine_version_error

    instance_classes = [
        "db.t3.micro", "db.t3.small", "db.t3.medium", "db.t3.large",
        "db.r5.large", "db.r5.xlarge", "db.r5.2xlarge",
        "db.m5.large", "db.m5.xlarge", "db.m5.2xlarge",
    ]
    version = engine_version or _default_engine_version(engine)

    members = ""
    for cls in instance_classes:
        if db_class and cls != db_class:
            continue
        members += f"""<OrderableDBInstanceOption>
            <Engine>{engine}</Engine>
            <EngineVersion>{version}</EngineVersion>
            <DBInstanceClass>{cls}</DBInstanceClass>
            <LicenseModel>{_license_model(engine)}</LicenseModel>
            <AvailabilityZones>
                <AvailabilityZone><Name>{get_region()}a</Name></AvailabilityZone>
                <AvailabilityZone><Name>{get_region()}b</Name></AvailabilityZone>
            </AvailabilityZones>
            <MultiAZCapable>true</MultiAZCapable>
            <ReadReplicaCapable>true</ReadReplicaCapable>
            <Vpc>true</Vpc>
            <SupportsStorageEncryption>true</SupportsStorageEncryption>
            <StorageType>gp2</StorageType>
            <SupportsIops>false</SupportsIops>
            <SupportsEnhancedMonitoring>true</SupportsEnhancedMonitoring>
            <SupportsIAMDatabaseAuthentication>true</SupportsIAMDatabaseAuthentication>
            <SupportsPerformanceInsights>true</SupportsPerformanceInsights>
            <AvailableProcessorFeatures/>
            <SupportedEngineModes><member>provisioned</member></SupportedEngineModes>
            <SupportsStorageAutoscaling>true</SupportsStorageAutoscaling>
            <SupportsKerberosAuthentication>false</SupportsKerberosAuthentication>
            <OutpostCapable>false</OutpostCapable>
            <SupportedNetworkTypes><member>IPV4</member></SupportedNetworkTypes>
            <SupportsGlobalDatabases>false</SupportsGlobalDatabases>
            <SupportsClusters>false</SupportsClusters>
            <SupportedActivityStreamModes/>
        </OrderableDBInstanceOption>"""
    return _xml(200, "DescribeOrderableDBInstanceOptionsResponse",
        f"<DescribeOrderableDBInstanceOptionsResult><OrderableDBInstanceOptions>{members}</OrderableDBInstanceOptions></DescribeOrderableDBInstanceOptionsResult>")


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def _instance_xml(i):
    """Render an instance dict to XML fields — no wrapping element."""
    ep = i.get("Endpoint", {})
    subnet = i.get("DBSubnetGroup", {})

    vpc_sg_xml = ""
    for sg in i.get("VpcSecurityGroups", []):
        vpc_sg_xml += f"""<VpcSecurityGroupMembership>
            <VpcSecurityGroupId>{sg.get('VpcSecurityGroupId','')}</VpcSecurityGroupId>
            <Status>{sg.get('Status','active')}</Status>
        </VpcSecurityGroupMembership>"""

    db_sg_xml = ""
    for sg in i.get("DBSecurityGroups", []):
        db_sg_xml += f"""<DBSecurityGroup>
            <DBSecurityGroupName>{sg}</DBSecurityGroupName>
            <Status>active</Status>
        </DBSecurityGroup>"""

    param_xml = ""
    for pg in i.get("DBParameterGroups", []):
        param_xml += f"""<DBParameterGroup>
            <DBParameterGroupName>{pg.get('DBParameterGroupName','')}</DBParameterGroupName>
            <ParameterApplyStatus>{pg.get('ParameterApplyStatus','in-sync')}</ParameterApplyStatus>
        </DBParameterGroup>"""

    option_xml = ""
    for og in i.get("OptionGroupMemberships", []):
        option_xml += f"""<OptionGroupMembership>
            <OptionGroupName>{og.get('OptionGroupName','')}</OptionGroupName>
            <Status>{og.get('Status','in-sync')}</Status>
        </OptionGroupMembership>"""

    tag_xml = ""
    for t in i.get("TagList", []):
        tag_xml += f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>"

    read_replica_xml = ""
    for rr in i.get("ReadReplicaDBInstanceIdentifiers", []):
        read_replica_xml += f"<ReadReplicaDBInstanceIdentifier>{rr}</ReadReplicaDBInstanceIdentifier>"

    subnet_xml = ""
    for s in subnet.get("Subnets", []):
        az = s.get("SubnetAvailabilityZone", {}).get("Name", f"{get_region()}a") if isinstance(s.get("SubnetAvailabilityZone"), dict) else f"{get_region()}a"
        subnet_xml += f"""<Subnet>
            <SubnetIdentifier>{s.get('SubnetIdentifier','')}</SubnetIdentifier>
            <SubnetAvailabilityZone><Name>{az}</Name></SubnetAvailabilityZone>
            <SubnetOutpost/>
            <SubnetStatus>Active</SubnetStatus>
        </Subnet>"""

    pending_xml = ""
    for pk, pv in i.get("PendingModifiedValues", {}).items():
        pending_xml += f"<{pk}>{pv}</{pk}>"

    iops_xml = ""
    if i.get("Iops") is not None:
        iops_xml = f"<Iops>{i['Iops']}</Iops>"

    cert_xml = ""
    cert = i.get("CertificateDetails")
    if cert:
        cert_xml = f"""<CertificateDetails>
            <CAIdentifier>{cert.get('CAIdentifier','')}</CAIdentifier>
            <ValidTill>{cert.get('ValidTill','')}</ValidTill>
        </CertificateDetails>"""

    return f"""<DBInstanceIdentifier>{i['DBInstanceIdentifier']}</DBInstanceIdentifier>
        <DBInstanceClass>{i['DBInstanceClass']}</DBInstanceClass>
        <Engine>{i['Engine']}</Engine>
        <EngineVersion>{i['EngineVersion']}</EngineVersion>
        <DBInstanceStatus>{i['DBInstanceStatus']}</DBInstanceStatus>
        <MasterUsername>{i['MasterUsername']}</MasterUsername>
        <DBName>{i.get('DBName','')}</DBName>
        <Endpoint>
            <Address>{ep.get('Address','localhost')}</Address>
            <Port>{ep.get('Port',5432)}</Port>
            <HostedZoneId>{ep.get('HostedZoneId','Z2R2ITUGPM61AM')}</HostedZoneId>
        </Endpoint>
        <AllocatedStorage>{i['AllocatedStorage']}</AllocatedStorage>
        <InstanceCreateTime>{i.get('InstanceCreateTime','')}</InstanceCreateTime>
        <PreferredBackupWindow>{i.get('PreferredBackupWindow','03:00-04:00')}</PreferredBackupWindow>
        <BackupRetentionPeriod>{i.get('BackupRetentionPeriod',1)}</BackupRetentionPeriod>
        <DBSecurityGroups>{db_sg_xml}</DBSecurityGroups>
        <VpcSecurityGroups>{vpc_sg_xml}</VpcSecurityGroups>
        <DBParameterGroups>{param_xml}</DBParameterGroups>
        <AvailabilityZone>{i.get('AvailabilityZone',f'{get_region()}a')}</AvailabilityZone>
        <DBSubnetGroup>
            <DBSubnetGroupName>{subnet.get('DBSubnetGroupName','default')}</DBSubnetGroupName>
            <DBSubnetGroupDescription>{subnet.get('DBSubnetGroupDescription','')}</DBSubnetGroupDescription>
            <VpcId>{subnet.get('VpcId','vpc-00000000')}</VpcId>
            <SubnetGroupStatus>{subnet.get('SubnetGroupStatus','Complete')}</SubnetGroupStatus>
            <Subnets>{subnet_xml}</Subnets>
            <DBSubnetGroupArn>{subnet.get('DBSubnetGroupArn','')}</DBSubnetGroupArn>
        </DBSubnetGroup>
        <PreferredMaintenanceWindow>{i.get('PreferredMaintenanceWindow','sun:05:00-sun:06:00')}</PreferredMaintenanceWindow>
        <PendingModifiedValues>{pending_xml}</PendingModifiedValues>
        <LatestRestorableTime>{i.get('LatestRestorableTime') or _format_time(time.time())}</LatestRestorableTime>
        <MultiAZ>{str(i.get('MultiAZ',False)).lower()}</MultiAZ>
        <AutoMinorVersionUpgrade>{str(i.get('AutoMinorVersionUpgrade',True)).lower()}</AutoMinorVersionUpgrade>
        <ReadReplicaDBInstanceIdentifiers>{read_replica_xml}</ReadReplicaDBInstanceIdentifiers>
        <ReadReplicaSourceDBInstanceIdentifier>{i.get('ReadReplicaSourceDBInstanceIdentifier','')}</ReadReplicaSourceDBInstanceIdentifier>
        <ReadReplicaDBClusterIdentifiers/>
        <ReplicaMode>{i.get('ReplicaMode','')}</ReplicaMode>
        <LicenseModel>{i.get('LicenseModel','general-public-license')}</LicenseModel>
        {iops_xml}
        <OptionGroupMemberships>{option_xml}</OptionGroupMemberships>
        <PubliclyAccessible>{str(i.get('PubliclyAccessible',False)).lower()}</PubliclyAccessible>
        <StatusInfos/>
        <StorageType>{i.get('StorageType','gp2')}</StorageType>
        <DbInstancePort>{i.get('DbInstancePort',0)}</DbInstancePort>
        <DBClusterIdentifier>{i.get('DBClusterIdentifier','')}</DBClusterIdentifier>
        <StorageEncrypted>{str(i.get('StorageEncrypted',False)).lower()}</StorageEncrypted>
        <KmsKeyId>{i.get('KmsKeyId','')}</KmsKeyId>
        <DbiResourceId>{i.get('DbiResourceId','')}</DbiResourceId>
        <CACertificateIdentifier>{i.get('CACertificateIdentifier','rds-ca-rsa2048-g1')}</CACertificateIdentifier>
        <DomainMemberships/>
        <CopyTagsToSnapshot>{str(i.get('CopyTagsToSnapshot',False)).lower()}</CopyTagsToSnapshot>
        <MonitoringInterval>{i.get('MonitoringInterval',0)}</MonitoringInterval>
        <EnhancedMonitoringResourceArn>{i.get('EnhancedMonitoringResourceArn','')}</EnhancedMonitoringResourceArn>
        <MonitoringRoleArn>{i.get('MonitoringRoleArn','')}</MonitoringRoleArn>
        <PromotionTier>{i.get('PromotionTier',1)}</PromotionTier>
        <DBInstanceArn>{i['DBInstanceArn']}</DBInstanceArn>
        <IAMDatabaseAuthenticationEnabled>{str(i.get('IAMDatabaseAuthenticationEnabled',False)).lower()}</IAMDatabaseAuthenticationEnabled>
        <PerformanceInsightsEnabled>{str(i.get('PerformanceInsightsEnabled',False)).lower()}</PerformanceInsightsEnabled>
        <EnabledCloudwatchLogsExports/>
        <ProcessorFeatures/>
        <DeletionProtection>{str(i.get('DeletionProtection',False)).lower()}</DeletionProtection>
        <AssociatedRoles/>
        <MaxAllocatedStorage>{i.get('MaxAllocatedStorage',i.get('AllocatedStorage',20))}</MaxAllocatedStorage>
        <TagList>{tag_xml}</TagList>
        {cert_xml}
        <CustomerOwnedIpEnabled>{str(i.get('CustomerOwnedIpEnabled',False)).lower()}</CustomerOwnedIpEnabled>
        <BackupTarget>{i.get('BackupTarget','region')}</BackupTarget>
        <NetworkType>{i.get('NetworkType','IPV4')}</NetworkType>
        <StorageThroughput>{i.get('StorageThroughput',0)}</StorageThroughput>
        <IsStorageConfigUpgradeAvailable>{str(i.get('IsStorageConfigUpgradeAvailable',False)).lower()}</IsStorageConfigUpgradeAvailable>"""


def _cluster_xml(c):
    """Render a cluster dict to XML fields."""
    vpc_sg_xml = ""
    for sg in c.get("VpcSecurityGroups", []):
        vpc_sg_xml += f"""<VpcSecurityGroupMembership>
            <VpcSecurityGroupId>{sg.get('VpcSecurityGroupId','')}</VpcSecurityGroupId>
            <Status>{sg.get('Status','active')}</Status>
        </VpcSecurityGroupMembership>"""

    member_xml = ""
    for m in c.get("DBClusterMembers", []):
        member_xml += f"""<DBClusterMember>
            <DBInstanceIdentifier>{m.get('DBInstanceIdentifier','')}</DBInstanceIdentifier>
            <IsClusterWriter>{str(m.get('IsClusterWriter',True)).lower()}</IsClusterWriter>
            <DBClusterParameterGroupStatus>in-sync</DBClusterParameterGroupStatus>
            <PromotionTier>{m.get('PromotionTier',1)}</PromotionTier>
        </DBClusterMember>"""

    az_xml = ""
    for az in c.get("AvailabilityZones", []):
        az_xml += f"<AvailabilityZone>{az}</AvailabilityZone>"

    tag_xml = ""
    for t in c.get("TagList", []):
        tag_xml += f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>"

    # AWS omits <DatabaseName> entirely when no initial database was specified;
    # emitting an empty element would surface as "" instead of None to clients.
    db_name = c.get("DatabaseName")
    db_name_xml = f"<DatabaseName>{db_name}</DatabaseName>" if db_name else ""
    global_cluster_id = c.get("GlobalClusterIdentifier")
    global_cluster_xml = (
        f"<GlobalClusterIdentifier>{global_cluster_id}</GlobalClusterIdentifier>"
        if global_cluster_id else ""
    )
    global_write_forwarding = c.get("GlobalWriteForwardingStatus")
    global_write_forwarding_xml = (
        f"<GlobalWriteForwardingStatus>{global_write_forwarding}</GlobalWriteForwardingStatus>"
        if global_write_forwarding else ""
    )

    # AWS emits <MasterUserSecret> only for clusters with an RDS-managed
    # master user password; KmsKeyId appears only when a key was specified.
    master_user_secret = c.get("MasterUserSecret")
    master_user_secret_xml = ""
    if master_user_secret:
        kms_key_id = master_user_secret.get("KmsKeyId")
        kms_key_xml = f"<KmsKeyId>{kms_key_id}</KmsKeyId>" if kms_key_id else ""
        master_user_secret_xml = (
            "<MasterUserSecret>"
            f"<SecretArn>{master_user_secret.get('SecretArn', '')}</SecretArn>"
            f"<SecretStatus>{master_user_secret.get('SecretStatus', 'active')}</SecretStatus>"
            f"{kms_key_xml}"
            "</MasterUserSecret>"
        )

    return f"""<DBClusterIdentifier>{c['DBClusterIdentifier']}</DBClusterIdentifier>
        <DBClusterArn>{c['DBClusterArn']}</DBClusterArn>
        <Engine>{c['Engine']}</Engine>
        <EngineVersion>{c['EngineVersion']}</EngineVersion>
        <EngineMode>{c.get('EngineMode','provisioned')}</EngineMode>
        <Status>{c['Status']}</Status>
        <MasterUsername>{c.get('MasterUsername','admin')}</MasterUsername>
        {db_name_xml}
        <Endpoint>{c.get('Endpoint','')}</Endpoint>
        <ReaderEndpoint>{c.get('ReaderEndpoint','')}</ReaderEndpoint>
        <Port>{c['Port']}</Port>
        <MultiAZ>{str(c.get('MultiAZ',False)).lower()}</MultiAZ>
        <AvailabilityZones>{az_xml}</AvailabilityZones>
        <DBClusterMembers>{member_xml}</DBClusterMembers>
        <VpcSecurityGroups>{vpc_sg_xml}</VpcSecurityGroups>
        <DBSubnetGroup>{c.get('DBSubnetGroup','default')}</DBSubnetGroup>
        <DBClusterParameterGroup>{c.get('DBClusterParameterGroup','')}</DBClusterParameterGroup>
        <BackupRetentionPeriod>{c.get('BackupRetentionPeriod',1)}</BackupRetentionPeriod>
        <PreferredBackupWindow>{c.get('PreferredBackupWindow','03:00-04:00')}</PreferredBackupWindow>
        <PreferredMaintenanceWindow>{c.get('PreferredMaintenanceWindow','sun:05:00-sun:06:00')}</PreferredMaintenanceWindow>
        <ClusterCreateTime>{c.get('ClusterCreateTime','')}</ClusterCreateTime>
        <EarliestRestorableTime>{c.get('EarliestRestorableTime','')}</EarliestRestorableTime>
        <LatestRestorableTime>{c.get('LatestRestorableTime','')}</LatestRestorableTime>
        <StorageEncrypted>{str(c.get('StorageEncrypted',False)).lower()}</StorageEncrypted>
        <KmsKeyId>{c.get('KmsKeyId','')}</KmsKeyId>
        <DeletionProtection>{str(c.get('DeletionProtection',False)).lower()}</DeletionProtection>
        <IAMDatabaseAuthenticationEnabled>{str(c.get('IAMDatabaseAuthenticationEnabled',False)).lower()}</IAMDatabaseAuthenticationEnabled>
        <HttpEndpointEnabled>{str(c.get('HttpEndpointEnabled',False)).lower()}</HttpEndpointEnabled>
        <CopyTagsToSnapshot>{str(c.get('CopyTagsToSnapshot',False)).lower()}</CopyTagsToSnapshot>
        <CrossAccountClone>{str(c.get('CrossAccountClone',False)).lower()}</CrossAccountClone>
        <DbClusterResourceId>{c.get('DbClusterResourceId','')}</DbClusterResourceId>
        <HostedZoneId>{c.get('HostedZoneId','Z2R2ITUGPM61AM')}</HostedZoneId>
        <AssociatedRoles/>
        <TagList>{tag_xml}</TagList>
        <AllocatedStorage>{c.get('AllocatedStorage',1)}</AllocatedStorage>
        <ActivityStreamStatus>{c.get('ActivityStreamStatus','stopped')}</ActivityStreamStatus>
        <NetworkType>{c.get('NetworkType','IPV4')}</NetworkType>
        {global_cluster_xml}
        {global_write_forwarding_xml}
        {master_user_secret_xml}
        <EngineLifecycleSupport>{c.get('EngineLifecycleSupport','open-source-rds-extended-support')}</EngineLifecycleSupport>"""


def _snapshot_xml(s):
    tag_xml = ""
    for t in s.get("TagList", []):
        tag_xml += f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>"
    return f"""<DBSnapshotIdentifier>{s['DBSnapshotIdentifier']}</DBSnapshotIdentifier>
        <DBInstanceIdentifier>{s['DBInstanceIdentifier']}</DBInstanceIdentifier>
        <DBSnapshotArn>{s.get('DBSnapshotArn','')}</DBSnapshotArn>
        <Engine>{s['Engine']}</Engine>
        <EngineVersion>{s['EngineVersion']}</EngineVersion>
        <SnapshotCreateTime>{s.get('SnapshotCreateTime','')}</SnapshotCreateTime>
        <InstanceCreateTime>{s.get('InstanceCreateTime','')}</InstanceCreateTime>
        <Status>{s['Status']}</Status>
        <AllocatedStorage>{s.get('AllocatedStorage',20)}</AllocatedStorage>
        <AvailabilityZone>{s.get('AvailabilityZone',f'{get_region()}a')}</AvailabilityZone>
        <VpcId>{s.get('VpcId','vpc-00000000')}</VpcId>
        <Port>{s.get('Port',5432)}</Port>
        <MasterUsername>{s.get('MasterUsername','admin')}</MasterUsername>
        <DBName>{s.get('DBName','')}</DBName>
        <SnapshotType>{s.get('SnapshotType','manual')}</SnapshotType>
        <LicenseModel>{s.get('LicenseModel','general-public-license')}</LicenseModel>
        <StorageType>{s.get('StorageType','gp2')}</StorageType>
        <DBInstanceClass>{s.get('DBInstanceClass','db.t3.micro')}</DBInstanceClass>
        <StorageEncrypted>{str(s.get('StorageEncrypted',False)).lower()}</StorageEncrypted>
        <KmsKeyId>{s.get('KmsKeyId','')}</KmsKeyId>
        <Encrypted>{str(s.get('Encrypted',False)).lower()}</Encrypted>
        <IAMDatabaseAuthenticationEnabled>{str(s.get('IAMDatabaseAuthenticationEnabled',False)).lower()}</IAMDatabaseAuthenticationEnabled>
        <PercentProgress>{s.get('PercentProgress',100)}</PercentProgress>
        <DbiResourceId>{s.get('DbiResourceId','')}</DbiResourceId>
        <TagList>{tag_xml}</TagList>
        <OriginalSnapshotCreateTime>{s.get('OriginalSnapshotCreateTime','')}</OriginalSnapshotCreateTime>
        <SnapshotDatabaseTime>{s.get('SnapshotDatabaseTime','')}</SnapshotDatabaseTime>
        <SnapshotTarget>{s.get('SnapshotTarget','region')}</SnapshotTarget>"""


def _subnet_group_xml(sg):
    subnets_xml = ""
    for s in sg.get("Subnets", []):
        az = s.get("SubnetAvailabilityZone", {}).get("Name", f"{get_region()}a") if isinstance(s.get("SubnetAvailabilityZone"), dict) else f"{get_region()}a"
        subnets_xml += f"""<Subnet>
            <SubnetIdentifier>{s.get('SubnetIdentifier','')}</SubnetIdentifier>
            <SubnetAvailabilityZone><Name>{az}</Name></SubnetAvailabilityZone>
            <SubnetOutpost/>
            <SubnetStatus>Active</SubnetStatus>
        </Subnet>"""
    return f"""<DBSubnetGroupName>{sg['DBSubnetGroupName']}</DBSubnetGroupName>
        <DBSubnetGroupDescription>{sg.get('DBSubnetGroupDescription','')}</DBSubnetGroupDescription>
        <VpcId>{sg.get('VpcId','vpc-00000000')}</VpcId>
        <SubnetGroupStatus>{sg.get('SubnetGroupStatus','Complete')}</SubnetGroupStatus>
        <Subnets>{subnets_xml}</Subnets>
        <DBSubnetGroupArn>{sg.get('DBSubnetGroupArn','')}</DBSubnetGroupArn>
        <SupportedNetworkTypes><member>IPV4</member></SupportedNetworkTypes>"""


def _cluster_snapshot_xml(s):
    tag_xml = ""
    for t in s.get("TagList", []):
        tag_xml += f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>"
    az_xml = ""
    for az in s.get("AvailabilityZones", []):
        az_xml += f"<AvailabilityZone>{az}</AvailabilityZone>"
    return f"""<DBClusterSnapshotIdentifier>{s['DBClusterSnapshotIdentifier']}</DBClusterSnapshotIdentifier>
        <DBClusterIdentifier>{s['DBClusterIdentifier']}</DBClusterIdentifier>
        <DBClusterSnapshotArn>{s.get('DBClusterSnapshotArn','')}</DBClusterSnapshotArn>
        <Engine>{s['Engine']}</Engine>
        <EngineVersion>{s['EngineVersion']}</EngineVersion>
        <SnapshotCreateTime>{s.get('SnapshotCreateTime','')}</SnapshotCreateTime>
        <ClusterCreateTime>{s.get('ClusterCreateTime','')}</ClusterCreateTime>
        <Status>{s['Status']}</Status>
        <Port>{s.get('Port',5432)}</Port>
        <VpcId>{s.get('VpcId','vpc-00000000')}</VpcId>
        <MasterUsername>{s.get('MasterUsername','admin')}</MasterUsername>
        <SnapshotType>{s.get('SnapshotType','manual')}</SnapshotType>
        <PercentProgress>{s.get('PercentProgress',100)}</PercentProgress>
        <StorageEncrypted>{str(s.get('StorageEncrypted',False)).lower()}</StorageEncrypted>
        <KmsKeyId>{s.get('KmsKeyId','')}</KmsKeyId>
        <AvailabilityZones>{az_xml}</AvailabilityZones>
        <LicenseModel>{s.get('LicenseModel','postgresql-license')}</LicenseModel>
        <DbClusterResourceId>{s.get('DbClusterResourceId','')}</DbClusterResourceId>
        <IAMDatabaseAuthenticationEnabled>{str(s.get('IAMDatabaseAuthenticationEnabled',False)).lower()}</IAMDatabaseAuthenticationEnabled>
        <AllocatedStorage>{s.get('AllocatedStorage',1)}</AllocatedStorage>
        <TagList>{tag_xml}</TagList>"""


def _option_group_xml(og):
    options_xml = ""
    for opt in og.get("Options", []):
        options_xml += f"<Option><OptionName>{opt.get('OptionName','')}</OptionName></Option>"
    return f"""<OptionGroupName>{og['OptionGroupName']}</OptionGroupName>
        <OptionGroupDescription>{og.get('OptionGroupDescription','')}</OptionGroupDescription>
        <EngineName>{og.get('EngineName','')}</EngineName>
        <MajorEngineVersion>{og.get('MajorEngineVersion','')}</MajorEngineVersion>
        <Options>{options_xml}</Options>
        <AllowsVpcAndNonVpcInstanceMemberships>{str(og.get('AllowsVpcAndNonVpcInstanceMemberships',True)).lower()}</AllowsVpcAndNonVpcInstanceMemberships>
        <VpcId>{og.get('VpcId','')}</VpcId>
        <OptionGroupArn>{og.get('OptionGroupArn','')}</OptionGroupArn>"""


def _single_instance_response(root_tag, result_tag, instance):
    return _xml(200, root_tag,
        f"<{result_tag}><DBInstance>{_instance_xml(instance)}</DBInstance></{result_tag}>")


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _p(params, key, default=""):
    val = params.get(key, [default])
    if isinstance(val, list):
        return val[0] if val else default
    return val


def _parse_tags(params):
    """Parse Tags.member.N.Key / Tags.member.N.Value or Tags.Tag.N.Key / Tags.Tag.N.Value."""
    tags = []
    prefix = "Tags.member"
    if not _p(params, "Tags.member.1.Key"):
        prefix = "Tags.Tag"
    i = 1
    while True:
        key = _p(params, f"{prefix}.{i}.Key")
        if not key:
            break
        value = _p(params, f"{prefix}.{i}.Value", "")
        tags.append({"Key": key, "Value": value})
        i += 1
    return tags


def _parse_member_list(params, prefix):
    """Parse list params in either Prefix.member.N or Prefix.<MemberName>.N format.

    The member.N format is used by direct AWS CLI/SDK calls. The <MemberName>.N
    format is produced by botocore's serializer when dispatched via Step Functions
    aws-sdk integrations (e.g. SubnetIds.SubnetIdentifier.N).
    """
    items = []
    i = 1
    while True:
        val = _p(params, f"{prefix}.member.{i}")
        if not val:
            break
        items.append(val)
        i += 1
    if items:
        return items
    # Fall back to Prefix.<AnyMemberName>.N (botocore serializer format)
    import re
    pattern = re.compile(rf"^{re.escape(prefix)}\.([^.]+)\.(\d+)$")
    numbered = {}
    for key in params:
        m = pattern.match(key)
        if m:
            idx = int(m.group(2))
            numbered[idx] = _p(params, key)
    return [numbered[k] for k in sorted(numbered)] if numbered else []


def _parameter_member_prefix(params, prefix="Parameters"):
    """Handle both Query API and botocore/SFN parameter list serialization."""
    query_prefix = f"{prefix}.member"
    if _p(params, f"{query_prefix}.1.ParameterName"):
        return query_prefix
    return f"{prefix}.Parameter"


def _parameter_xml(
    name,
    value,
    source,
    apply_method,
    description="",
    apply_type="dynamic",
    data_type="string",
    modifiable=True,
):
    return f"""<Parameter>
            <ParameterName>{name}</ParameterName>
            <ParameterValue>{value}</ParameterValue>
            <Description>{_esc(description)}</Description>
            <Source>{source}</Source>
            <ApplyType>{apply_type}</ApplyType>
            <DataType>{data_type}</DataType>
            <IsModifiable>{str(modifiable).lower()}</IsModifiable>
            <ApplyMethod>{apply_method}</ApplyMethod>
        </Parameter>"""


def _parameter_group_parameters_xml(pg, source_filter):
    family = pg.get("DBParameterGroupFamily", "")
    default_params = _default_parameters_for_family(family)
    custom = pg.get("Parameters", {})
    default_names = {p["name"] for p in default_params}
    params_xml = ""

    for param in default_params:
        pname = param["name"]
        cval = custom.get(pname)
        if isinstance(cval, dict):
            value = cval.get("ParameterValue", param.get("default", ""))
            apply_method = cval.get("ApplyMethod", "pending-reboot")
        else:
            value = cval if cval is not None else param.get("default", "")
            apply_method = "pending-reboot"
        source = "user" if pname in custom else "engine-default"
        if source_filter and source != source_filter:
            continue
        params_xml += _parameter_xml(
            pname,
            value,
            source,
            apply_method,
            param.get("description", ""),
            param.get("apply_type", "dynamic"),
            param.get("data_type", "string"),
            param.get("modifiable", True),
        )

    for pname, cval in custom.items():
        if pname in default_names:
            continue
        if source_filter and source_filter != "user":
            continue
        if isinstance(cval, dict):
            value = cval.get("ParameterValue", "")
            apply_method = cval.get("ApplyMethod", "immediate")
        else:
            value = cval if cval is not None else ""
            apply_method = "immediate"
        params_xml += _parameter_xml(pname, value, "user", apply_method)

    return params_xml


def _parse_filters(params):
    """Parse AWS Query filters and the internal JSON-flattened member form."""
    prefix = "Filters.Filter"
    value_member = "Value"
    if not _p(params, f"{prefix}.1.Name"):
        prefix = "Filters.member"
        value_member = "member"

    filters = {}
    i = 1
    while True:
        name = _p(params, f"{prefix}.{i}.Name")
        if not name:
            break
        values = []
        j = 1
        while True:
            v = _p(params, f"{prefix}.{i}.Values.{value_member}.{j}")
            if not v:
                break
            values.append(v)
            j += 1
        filters[name] = values
        i += 1
    return filters


def _apply_instance_filters(instances, filters):
    result = []
    for inst in instances:
        match = True
        for fname, fvals in filters.items():
            if fname == "db-instance-id":
                if inst["DBInstanceIdentifier"] not in fvals:
                    match = False
            elif fname == "engine":
                if inst["Engine"] not in fvals:
                    match = False
            elif fname == "db-cluster-id":
                if inst.get("DBClusterIdentifier", "") not in fvals:
                    match = False
        if match:
            result.append(inst)
    return result


def _apply_cluster_filters(clusters, filters):
    result = []
    for cl in clusters:
        match = True
        for fname, fvals in filters.items():
            if fname == "db-cluster-id":
                if cl["DBClusterIdentifier"] not in fvals:
                    match = False
            elif fname == "engine":
                if cl["Engine"] not in fvals:
                    match = False
        if match:
            result.append(cl)
    return result


def _format_time(ts):
    dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _default_engine_version(engine):
    defaults = {
        "postgres": "15.3", "mysql": "8.0.33", "mariadb": "10.6.14",
        "aurora-postgresql": "17.7", "aurora-mysql": "8.0.mysql_aurora.3.10.3",
    }
    return defaults.get(engine, "15.3")


def _unsupported_aurora_engine_version_error(engine, engine_version, *,
                                             current_version=None):
    supported = {
        "aurora-mysql": AURORA_MYSQL_ENGINE_VERSION_SET,
        "aurora-postgresql": AURORA_POSTGRESQL_ENGINE_VERSION_SET,
    }.get(engine)
    if supported is None or not engine_version or engine_version in supported:
        return None
    # Real AWS also accepts a dot-boundary prefix of a creatable version and
    # resolves it to a concrete minor server-side: aurora-postgresql "16" and
    # aurora-mysql "8.0" (or "8.0.mysql_aurora.3.04") are all valid inputs.
    # Ministack stores the prefix as given; the Docker image is derived from
    # the major, so the running database matches what the prefix requested.
    if any(version.startswith(engine_version + ".") for version in supported):
        return None
    if current_version:
        # Modify paths report the upgrade-target shape real AWS uses when the
        # requested version is not a reachable target from the stored one.
        return _error(
            "InvalidParameterCombination",
            f"Cannot find upgrade target from {current_version} with "
            f"requested version {engine_version}.",
            400,
        )
    return _error(
        "InvalidParameterCombination",
        f"Cannot find version {engine_version} for {engine}",
        400,
    )


def _aurora_major_series(engine, engine_version):
    """Comparable major series for an Aurora engine version.

    aurora-postgresql majors are the first dotted segment ("16.8" -> "16").
    aurora-mysql majors are the Aurora series after the community head
    ("8.0.mysql_aurora.3.10.3" -> "3"); a bare community head such as the
    accepted "8.0" prefix carries no series, so it returns None.
    """
    if not engine_version:
        return None
    if engine == "aurora-mysql":
        if ".mysql_aurora." not in engine_version:
            return None
        return engine_version.split(".mysql_aurora.", 1)[1].split(".")[0] or None
    return engine_version.split(".")[0] or None


def _global_member_engine_version_conflict_error(cluster, engine_version):
    """Reject a member version change that diverges from its global cluster.

    Real AWS applies major version changes of a global database through
    ModifyGlobalCluster, which propagates to every member; per-member minor
    upgrades are allowed. Without this check a member could be moved to a
    different major than its global, a combination CreateDBCluster already
    refuses to produce.
    """
    gc_id = cluster.get("GlobalClusterIdentifier")
    if not engine_version or not gc_id:
        return None
    gc = _resolve_global_cluster(gc_id)
    gc_version = (gc or {}).get("EngineVersion")
    engine = cluster.get("Engine")
    requested_major = _aurora_major_series(engine, engine_version)
    global_major = _aurora_major_series(engine, gc_version)
    if not requested_major or not global_major or requested_major == global_major:
        return None
    return _error(
        "InvalidParameterCombination",
        "Major Version Upgrade isn't supported in a single member of a "
        "global cluster. Use ModifyGlobalCluster to upgrade all the members.",
        400,
    )


def _mysql_community_major_minor(engine_version):
    version = engine_version or ""
    head = version.split(".mysql_aurora.")[0] if ".mysql_aurora." in version else version
    parts = head.split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else head


def _mysql_image_for_version(engine_version):
    return _mysql_runtime_for_version(engine_version)[0]


def _mysql_runtime_for_engine(engine, engine_version):
    """Return the actual MySQL-family image and plugin-compatible series."""
    if engine == "mariadb":
        return "mariadb:latest", None
    return _mysql_runtime_for_version(engine_version)


def _mysql_runtime_for_version(engine_version):
    """Return the selected image and the known series encoded in its tag."""
    major_minor = _mysql_community_major_minor(engine_version)
    image = AURORA_MYSQL_IMAGE_MAP.get(
        major_minor,
        DEFAULT_AURORA_MYSQL_IMAGE,
    )
    _repository, separator, tag = image.rpartition(":")
    if not separator or tag not in AURORA_MYSQL_IMAGE_MAP:
        logger.warning(
            "RDS: cannot derive a supported MySQL series from selected image "
            "%r; IAM auth plugin artifacts will remain disabled",
            image,
        )
        return image, None
    return image, tag


def _default_port(engine):
    if "mysql" in engine or "mariadb" in engine or "aurora-mysql" in engine:
        return "3306"
    return "5432"


def _license_model(engine):
    if "postgres" in engine or "aurora" in engine:
        return "postgresql-license"
    return "general-public-license"


def _docker_image_for_engine(engine, engine_version, user, password, db_name):
    """Return (image, env_dict, container_port, data_path) or all-None.

    data_path is the in-container path where the engine's data volume should
    be mounted. Postgres 18+ reorganised its on-disk layout so that data
    lives under a major-version-specific subdirectory; the official
    postgres:18+ image refuses to start with a volume mounted at
    /var/lib/postgresql/data (the pre-18 path) and points operators at
    /var/lib/postgresql instead. We pick the right path per major so both
    `postgres:17-alpine` and `postgres:18-alpine` start cleanly.
    See https://github.com/docker-library/postgres/pull/1259 for context.
    """
    if "postgres" in engine or "aurora-postgresql" in engine:
        major = engine_version.split(".")[0]
        try:
            major_int = int(major)
        except ValueError:
            major_int = 0
        data_path = "/var/lib/postgresql" if major_int >= 18 else "/var/lib/postgresql/data"
        return (
            apply_image_prefix(f"postgres:{major}-alpine"),
            {"POSTGRES_USER": user, "POSTGRES_PASSWORD": password, "POSTGRES_DB": db_name},
            5432,
            data_path,
        )
    if _is_mysql_engine(engine):
        image, _series = _mysql_runtime_for_engine(engine, engine_version)
        return (
            apply_image_prefix(image),
            {"MYSQL_ROOT_PASSWORD": password, "MYSQL_ROOT_HOST": "%",
             "MYSQL_DATABASE": db_name,
             "MYSQL_USER": user, "MYSQL_PASSWORD": password},
            3306,
            "/var/lib/mysql",
        )
    return None, None, None, None


def _default_parameters_for_family(family):
    """Return a minimal set of parameter definitions for DescribeDBParameters."""
    base = [
        {"name": "max_connections", "default": "100", "description": "Max number of connections",
         "apply_type": "dynamic", "data_type": "integer", "modifiable": True},
        {"name": "shared_buffers", "default": "128MB", "description": "Shared memory buffers",
         "apply_type": "static", "data_type": "string", "modifiable": True},
        {"name": "work_mem", "default": "4MB", "description": "Memory for internal sort ops",
         "apply_type": "dynamic", "data_type": "string", "modifiable": True},
        {"name": "maintenance_work_mem", "default": "64MB", "description": "Memory for maintenance ops",
         "apply_type": "dynamic", "data_type": "string", "modifiable": True},
        {"name": "effective_cache_size", "default": "4GB", "description": "Planner effective cache size",
         "apply_type": "dynamic", "data_type": "string", "modifiable": True},
        {"name": "log_statement", "default": "none", "description": "Type of statements logged",
         "apply_type": "dynamic", "data_type": "string", "modifiable": True},
        {"name": "log_min_duration_statement", "default": "-1", "description": "Min duration before logging",
         "apply_type": "dynamic", "data_type": "integer", "modifiable": True},
    ]
    if "mysql" in family.lower():
        base = [
            {"name": "max_connections", "default": "151", "description": "Max number of connections",
             "apply_type": "dynamic", "data_type": "integer", "modifiable": True},
            {"name": "innodb_buffer_pool_size", "default": "134217728", "description": "InnoDB buffer pool size",
             "apply_type": "static", "data_type": "integer", "modifiable": True},
            {"name": "character_set_server", "default": "utf8mb4", "description": "Server character set",
             "apply_type": "dynamic", "data_type": "string", "modifiable": True},
            {"name": "slow_query_log", "default": "0", "description": "Enable slow query log",
             "apply_type": "dynamic", "data_type": "boolean", "modifiable": True},
            {"name": "long_query_time", "default": "10", "description": "Slow query threshold",
             "apply_type": "dynamic", "data_type": "float", "modifiable": True},
        ]
        if not family.lower().endswith("8.4"):
            base.append(
                {"name": "skip-character-set-client-handshake", "default": "1",
                 "description": "Skip character set client handshake",
                 "apply_type": "static", "data_type": "boolean", "modifiable": True}
            )
    return base


# ---------------------------------------------------------------------------
# RDS Proxy (control plane)
#
# Emulation scope: metadata only. Proxies, proxy endpoints, the default target
# group and its registered targets are modelled with the documented AWS shapes,
# defaults, constraints and error codes, but no connection pooling happens —
# the endpoint hostnames are synthetic and nothing listens on them. Clients
# keep connecting to the instance/cluster endpoints. This is what Terraform,
# CloudFormation and Crossplane exercise (#1488).
# ---------------------------------------------------------------------------

# Names for proxies, proxy endpoints and target groups share one constraint:
# "must begin with a letter and must contain only ASCII letters, digits, and
# hyphens; it can't end with a hyphen or contain two consecutive hyphens".
_DB_PROXY_NAME_RE = re.compile(r"^[a-zA-Z](?:-?[a-zA-Z0-9]+)*$")
_DB_PROXY_ENGINE_FAMILIES = ("MYSQL", "POSTGRESQL", "SQLSERVER")
_DB_PROXY_DEFAULT_TARGET_GROUP = "default"
# "Each AWS account ID is limited to 20 proxies."
_DB_PROXY_QUOTA = 20
# "You can add up to 20 additional proxy endpoints for each proxy."
_DB_PROXY_MAX_USER_ENDPOINTS = 20
# "Array Members: Minimum number of 0 items. Maximum number of 200 items."
_DB_PROXY_MAX_AUTH_ENTRIES = 200
# DescribeDBProxies* MaxRecords: "Default: 100  Constraints: Minimum 20,
# maximum 100."
_DB_PROXY_DEFAULT_MAX_RECORDS = 100
_DB_PROXY_MIN_MAX_RECORDS = 20
# EngineFamily -> the engines that family "supports", per the DBProxy docs:
# "MYSQL supports Aurora MySQL, RDS for MariaDB, and RDS for MySQL databases.
# POSTGRESQL supports Aurora PostgreSQL and RDS for PostgreSQL databases.
# SQLSERVER supports RDS for Microsoft SQL Server databases."
_DB_PROXY_ENGINE_FAMILY_ENGINES = {
    "MYSQL": ("mysql", "mariadb", "aurora-mysql", "aurora"),
    "POSTGRESQL": ("postgres", "aurora-postgresql"),
    "SQLSERVER": (
        "sqlserver-se", "sqlserver-ee", "sqlserver-ex", "sqlserver-web",
    ),
}


def _new_db_proxy_id(prefix):
    return f"{prefix}-{stdlib_secrets.token_hex(9)[:17]}"


def _invalid_parameter_value(message):
    return _error("InvalidParameterValue", message, 400)


def _invalid_parameter_combination(message):
    return _error("InvalidParameterCombination", message, 400)


def _db_proxy_name_error(name, label):
    if not name:
        return _error("MissingParameter", f"{label} is required", 400)
    if len(name) > 63 or not _DB_PROXY_NAME_RE.match(name):
        return _invalid_parameter_value(
            f"The parameter {label} is not a valid identifier. "
            "Identifiers must begin with a letter; must contain only ASCII "
            "letters, digits, and hyphens; and must not end with a hyphen or "
            "contain two consecutive hyphens."
        )
    return None


def _db_proxy_enum_error(params, label, allowed):
    value = _p(params, label)
    if value and value not in allowed:
        return _invalid_parameter_value(
            f"Invalid value {value} for {label}. "
            f"Valid values are {' | '.join(allowed)}."
        )
    return None


def _db_proxy_int_error(params, label, minimum, maximum):
    """Return (value, error). value is None when the parameter is absent."""
    raw = _p(params, label)
    if not raw:
        return None, None
    try:
        value = int(raw)
    except ValueError:
        return None, _invalid_parameter_value(
            f"Invalid value {raw} for {label}. Must be an integer."
        )
    if value < minimum or value > maximum:
        return None, _invalid_parameter_value(
            f"Invalid value {value} for {label}. "
            f"Must be between {minimum} and {maximum}."
        )
    return value, None


def _db_proxy_not_found(name):
    return _error(
        "DBProxyNotFoundFault",
        f"The specified proxy name {name} doesn't correspond to a proxy owned "
        "by your Amazon Web Services account in the specified Amazon Web "
        "Services Region.",
        404,
    )


def _db_proxy_endpoint_not_found(name):
    return _error(
        "DBProxyEndpointNotFoundFault",
        f"The DB proxy endpoint {name} doesn't exist.",
        404,
    )


def _db_proxy_target_group_not_found(name):
    return _error(
        "DBProxyTargetGroupNotFoundFault",
        f"The specified target group {name} isn't available for a proxy owned "
        "by your Amazon Web Services account in the specified Amazon Web "
        "Services Region.",
        404,
    )


def _get_db_proxy(name):
    return _request_region_get(_db_proxies, name, "db-proxy")


def _db_proxy_page(params, records, key_fn, label):
    """Apply the RDS `Marker` / `MaxRecords` contract.

    Returns ``(page, next_marker, error)``. ``next_marker`` is None when the
    page is the last one, in which case no `Marker` is emitted.
    """
    max_records, error = _db_proxy_int_error(
        params, "MaxRecords",
        _DB_PROXY_MIN_MAX_RECORDS, _DB_PROXY_DEFAULT_MAX_RECORDS,
    )
    if error:
        return None, None, error
    if max_records is None:
        max_records = _DB_PROXY_DEFAULT_MAX_RECORDS

    marker = _p(params, "Marker")
    if marker:
        keys = [key_fn(record) for record in records]
        if marker not in keys:
            return None, None, _invalid_parameter_value(
                f"Invalid value {marker} for Marker. "
                f"It does not identify a {label} in this request's results."
            )
        records = records[keys.index(marker) + 1:]

    page = records[:max_records]
    next_marker = key_fn(page[-1]) if len(records) > max_records else None
    return page, next_marker, None


def _marker_xml(next_marker):
    return f"<Marker>{_esc(next_marker)}</Marker>" if next_marker else ""


def _parse_user_auth_configs(params, prefix="Auth"):
    """Parse Auth.member.N.<field> (Query API) into UserAuthConfig dicts."""
    fields = (
        "Description",
        "UserName",
        "AuthScheme",
        "SecretArn",
        "IAMAuth",
        "ClientPasswordAuthType",
    )
    configs = []
    i = 1
    while True:
        base = f"{prefix}.member.{i}"
        if not any(_p(params, f"{base}.{field}") for field in fields):
            break
        entry = {}
        for field in fields:
            value = _p(params, f"{base}.{field}")
            if value:
                entry[field] = value
        configs.append(entry)
        i += 1
    return configs


def _db_proxy_auth_error(auth):
    if len(auth) > _DB_PROXY_MAX_AUTH_ENTRIES:
        return _invalid_parameter_value(
            f"Invalid number of Auth entries: {len(auth)}. "
            f"A proxy supports at most {_DB_PROXY_MAX_AUTH_ENTRIES}."
        )
    for entry in auth:
        scheme = entry.get("AuthScheme")
        if scheme and scheme != "SECRETS":
            return _invalid_parameter_value(
                f"Invalid value {scheme} for AuthScheme. "
                "Valid values are SECRETS."
            )
        iam_auth = entry.get("IAMAuth")
        if iam_auth and iam_auth not in ("DISABLED", "REQUIRED", "ENABLED"):
            return _invalid_parameter_value(
                f"Invalid value {iam_auth} for IAMAuth. "
                "Valid values are DISABLED | REQUIRED | ENABLED."
            )
    return None


def _default_connection_pool_config(engine_family):
    # "Default: 10 for RDS for Microsoft SQL Server, and 100 for all other
    # engines" / "If the value of MaxConnectionsPercent isn't specified, then
    # for SQL Server, MaxIdleConnectionsPercent is 5, and for all other
    # engines, the default is 50" / "Default: 120".
    sqlserver = engine_family == "SQLSERVER"
    return {
        "MaxConnectionsPercent": 10 if sqlserver else 100,
        "MaxIdleConnectionsPercent": 5 if sqlserver else 50,
        "ConnectionBorrowTimeout": 120,
        "SessionPinningFilters": [],
    }


def _create_db_proxy(p):
    name = _p(p, "DBProxyName")
    name_error = _db_proxy_name_error(name, "DBProxyName")
    if name_error:
        return name_error

    engine_family = _p(p, "EngineFamily")
    if not engine_family:
        return _error("MissingParameter", "EngineFamily is required", 400)
    if engine_family not in _DB_PROXY_ENGINE_FAMILIES:
        return _invalid_parameter_value(
            f"Invalid value {engine_family} for EngineFamily. "
            f"Valid values are {' | '.join(_DB_PROXY_ENGINE_FAMILIES)}."
        )

    role_arn = _p(p, "RoleArn")
    if not role_arn:
        return _error("MissingParameter", "RoleArn is required", 400)
    if not 20 <= len(role_arn) <= 2048:
        return _invalid_parameter_value(
            "Invalid value for RoleArn. Must be between 20 and 2048 "
            "characters."
        )

    subnet_ids = _parse_member_list(p, "VpcSubnetIds")
    if not subnet_ids:
        return _error("MissingParameter", "VpcSubnetIds is required", 400)

    for label, allowed in (
        ("DefaultAuthScheme", ("IAM_AUTH", "NONE")),
        ("EndpointNetworkType", ("IPV4", "IPV6", "DUAL")),
        ("TargetConnectionNetworkType", ("IPV4", "IPV6")),
    ):
        enum_error = _db_proxy_enum_error(p, label, allowed)
        if enum_error:
            return enum_error

    idle_timeout, error = _db_proxy_int_error(p, "IdleClientTimeout", 1, 28800)
    if error:
        return error

    auth = _parse_user_auth_configs(p)
    auth_error = _db_proxy_auth_error(auth)
    if auth_error:
        return auth_error
    default_auth_scheme = _p(p, "DefaultAuthScheme")
    # "If you don't specify DefaultAuthScheme or specify this parameter as
    # NONE, you must specify the Auth option."
    if default_auth_scheme in ("", "NONE") and not auth:
        return _invalid_parameter_combination(
            "Auth is required when DefaultAuthScheme is not specified or is "
            "NONE."
        )

    if _db_proxies.get(name):
        return _error(
            "DBProxyAlreadyExistsFault",
            f"The specified proxy name {name} must be unique for all proxies "
            "owned by your Amazon Web Services account in the specified "
            "Amazon Web Services Region.",
            400,
        )
    if len(_db_proxies.values()) >= _DB_PROXY_QUOTA:
        return _error(
            "DBProxyQuotaExceededFault",
            "Your Amazon Web Services account already has the maximum number "
            "of proxies in the specified Amazon Web Services Region.",
            400,
        )

    resolved = _resolve_subnet_group_members(subnet_ids)
    if resolved is None:
        return _error("InvalidSubnet", _INVALID_SUBNET_MESSAGE, 400)
    _subnets, vpc_id = resolved

    region = get_region()
    account_id = get_account_id()
    proxy_id = _new_db_proxy_id("prx")
    arn = f"arn:aws:rds:{region}:{account_id}:db-proxy:{proxy_id}"
    # Real endpoints read "the-proxy.proxy-demo.us-east-1.rds.amazonaws.com" —
    # the middle label is an account/region-scoped hash we can only synthesize.
    endpoint_suffix = new_uuid()[:8]
    now = time.time()

    proxy = {
        "DBProxyName": name,
        "DBProxyArn": arn,
        "Status": "available",
        "EngineFamily": engine_family,
        "VpcId": vpc_id,
        "VpcSecurityGroupIds": _parse_member_list(p, "VpcSecurityGroupIds"),
        "VpcSubnetIds": subnet_ids,
        "Auth": auth,
        "RoleArn": role_arn,
        "Endpoint": f"{name}.proxy-{endpoint_suffix}.{region}.rds.amazonaws.com",
        "RequireTLS": _p(p, "RequireTLS") == "true",
        # "Default: 1800 (30 minutes)  Constraints: 1 to 28,800"
        "IdleClientTimeout": idle_timeout if idle_timeout is not None else 1800,
        "DebugLogging": _p(p, "DebugLogging") == "true",
        "CreatedDate": now,
        "UpdatedDate": now,
        # "Default: IPV4" for both network-type parameters.
        "EndpointNetworkType": _p(p, "EndpointNetworkType") or "IPV4",
        "TargetConnectionNetworkType": _p(p, "TargetConnectionNetworkType") or "IPV4",
        "_EndpointSuffix": endpoint_suffix,
        "TargetGroup": {
            "DBProxyName": name,
            "TargetGroupName": _DB_PROXY_DEFAULT_TARGET_GROUP,
            "TargetGroupArn": (
                f"arn:aws:rds:{region}:{account_id}:target-group:"
                f"{_new_db_proxy_id('prx-tg')}"
            ),
            "IsDefault": True,
            "Status": "available",
            "ConnectionPoolConfig": _default_connection_pool_config(engine_family),
            "CreatedDate": now,
            "UpdatedDate": now,
        },
        "Targets": [],
    }
    if default_auth_scheme:
        proxy["DefaultAuthScheme"] = default_auth_scheme
    _db_proxies[name] = proxy

    # "RDS automatically creates one endpoint for each DB proxy." Default
    # endpoints always have read/write capability.
    _db_proxy_endpoints[name] = {
        "DBProxyEndpointName": name,
        "DBProxyEndpointArn": (
            f"arn:aws:rds:{region}:{account_id}:db-proxy-endpoint:"
            f"{_new_db_proxy_id('prx-endpoint')}"
        ),
        "DBProxyName": name,
        "Status": "available",
        "VpcId": vpc_id,
        "VpcSecurityGroupIds": list(proxy["VpcSecurityGroupIds"]),
        "VpcSubnetIds": list(subnet_ids),
        "Endpoint": proxy["Endpoint"],
        "CreatedDate": now,
        "TargetRole": "READ_WRITE",
        "IsDefault": True,
        "EndpointNetworkType": proxy["EndpointNetworkType"],
    }

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[arn] = req_tags

    return _xml(200, "CreateDBProxyResponse",
        f"<CreateDBProxyResult><DBProxy>{_db_proxy_xml(proxy)}</DBProxy></CreateDBProxyResult>")


def _describe_db_proxies(p):
    name = _p(p, "DBProxyName")
    if name:
        proxy = _get_db_proxy(name)
        if not proxy:
            return _db_proxy_not_found(name)
        proxies = [proxy]
    else:
        proxies = sorted(
            _db_proxies.values(), key=lambda px: px["DBProxyName"],
        )

    page, next_marker, error = _db_proxy_page(
        p, proxies, lambda px: px["DBProxyName"], "DB proxy",
    )
    if error:
        return error

    members = "".join(f"<member>{_db_proxy_xml(px)}</member>" for px in page)
    return _xml(200, "DescribeDBProxiesResponse",
        f"<DescribeDBProxiesResult><DBProxies>{members}</DBProxies>"
        f"{_marker_xml(next_marker)}</DescribeDBProxiesResult>")


def _modify_db_proxy(p):
    name = _p(p, "DBProxyName")
    proxy = _get_db_proxy(name)
    if not proxy:
        return _db_proxy_not_found(name)

    new_name = _p(p, "NewDBProxyName")
    if new_name and new_name != name:
        name_error = _db_proxy_name_error(new_name, "NewDBProxyName")
        if name_error:
            return name_error
        if _db_proxies.get(new_name):
            return _error(
                "DBProxyAlreadyExistsFault",
                f"The specified proxy name {new_name} must be unique for all "
                "proxies owned by your Amazon Web Services account in the "
                "specified Amazon Web Services Region.",
                400,
            )

    enum_error = _db_proxy_enum_error(
        p, "DefaultAuthScheme", ("IAM_AUTH", "NONE"),
    )
    if enum_error:
        return enum_error

    idle_timeout, error = _db_proxy_int_error(p, "IdleClientTimeout", 1, 28800)
    if error:
        return error

    role_arn = _p(p, "RoleArn")
    if role_arn and not 20 <= len(role_arn) <= 2048:
        return _invalid_parameter_value(
            "Invalid value for RoleArn. Must be between 20 and 2048 "
            "characters."
        )

    auth = _parse_user_auth_configs(p)
    auth_error = _db_proxy_auth_error(auth)
    if auth_error:
        return auth_error
    default_auth_scheme = _p(p, "DefaultAuthScheme")
    effective_auth = auth or proxy["Auth"]
    effective_scheme = default_auth_scheme or proxy.get("DefaultAuthScheme", "")
    if effective_scheme in ("", "NONE") and not effective_auth:
        return _invalid_parameter_combination(
            "Auth is required when DefaultAuthScheme is not specified or is "
            "NONE."
        )

    if "RequireTLS" in p:
        proxy["RequireTLS"] = _p(p, "RequireTLS") == "true"
    if "DebugLogging" in p:
        proxy["DebugLogging"] = _p(p, "DebugLogging") == "true"
    if idle_timeout is not None:
        proxy["IdleClientTimeout"] = idle_timeout
    if role_arn:
        proxy["RoleArn"] = role_arn
    if default_auth_scheme:
        proxy["DefaultAuthScheme"] = default_auth_scheme
    if auth:
        proxy["Auth"] = auth
    security_groups = _parse_member_list(p, "SecurityGroups")
    if security_groups:
        proxy["VpcSecurityGroupIds"] = security_groups

    if new_name and new_name != name:
        region = get_region()
        proxy["DBProxyName"] = new_name
        proxy["Endpoint"] = (
            f"{new_name}.proxy-{proxy['_EndpointSuffix']}.{region}.rds.amazonaws.com"
        )
        proxy["TargetGroup"]["DBProxyName"] = new_name
        for endpoint in list(_db_proxy_endpoints.values()):
            if endpoint["DBProxyName"] != name:
                continue
            endpoint["DBProxyName"] = new_name
            if endpoint.get("IsDefault"):
                endpoint["DBProxyEndpointName"] = new_name
                endpoint["Endpoint"] = proxy["Endpoint"]
                _db_proxy_endpoints.pop(name, None)
                _db_proxy_endpoints[new_name] = endpoint
        _db_proxies.pop(name, None)
        _db_proxies[new_name] = proxy

    proxy["UpdatedDate"] = time.time()
    return _xml(200, "ModifyDBProxyResponse",
        f"<ModifyDBProxyResult><DBProxy>{_db_proxy_xml(proxy)}</DBProxy></ModifyDBProxyResult>")


def _delete_db_proxy(p):
    name = _p(p, "DBProxyName")
    proxy = _get_db_proxy(name)
    if not proxy:
        return _db_proxy_not_found(name)

    proxy_name = proxy["DBProxyName"]
    for endpoint_name, endpoint in list(_db_proxy_endpoints.items()):
        if endpoint["DBProxyName"] == proxy_name:
            _tags.pop(endpoint.get("DBProxyEndpointArn", ""), None)
            _db_proxy_endpoints.pop(endpoint_name, None)
    _db_proxies.pop(proxy_name, None)
    _tags.pop(proxy.get("DBProxyArn", ""), None)

    proxy["Status"] = "deleting"
    return _xml(200, "DeleteDBProxyResponse",
        f"<DeleteDBProxyResult><DBProxy>{_db_proxy_xml(proxy)}</DBProxy></DeleteDBProxyResult>")


def _create_db_proxy_endpoint(p):
    proxy_name = _p(p, "DBProxyName")
    proxy = _get_db_proxy(proxy_name)
    if not proxy:
        return _db_proxy_not_found(proxy_name)

    endpoint_name = _p(p, "DBProxyEndpointName")
    name_error = _db_proxy_name_error(endpoint_name, "DBProxyEndpointName")
    if name_error:
        return name_error

    for label, allowed in (
        ("TargetRole", ("READ_WRITE", "READ_ONLY")),
        ("EndpointNetworkType", ("IPV4", "IPV6", "DUAL")),
    ):
        enum_error = _db_proxy_enum_error(p, label, allowed)
        if enum_error:
            return enum_error

    target_role = _p(p, "TargetRole") or "READ_WRITE"
    # "The only role that proxies for RDS for Microsoft SQL Server support is
    # READ_WRITE."
    if proxy["EngineFamily"] == "SQLSERVER" and target_role != "READ_WRITE":
        return _invalid_parameter_value(
            f"Invalid value {target_role} for TargetRole. The only role that "
            "proxies for RDS for Microsoft SQL Server support is READ_WRITE."
        )

    if _db_proxy_endpoints.get(endpoint_name):
        return _error(
            "DBProxyEndpointAlreadyExistsFault",
            f"The specified DB proxy endpoint name {endpoint_name} must be "
            "unique for all DB proxy endpoints owned by your Amazon Web "
            "Services account in the specified Amazon Web Services Region.",
            400,
        )

    user_defined = [
        endpoint
        for endpoint in _db_proxy_endpoints.values()
        if endpoint["DBProxyName"] == proxy["DBProxyName"]
        and not endpoint.get("IsDefault")
    ]
    if len(user_defined) >= _DB_PROXY_MAX_USER_ENDPOINTS:
        return _error(
            "DBProxyEndpointQuotaExceededFault",
            "The DB proxy already has the maximum number of endpoints.",
            400,
        )

    subnet_ids = _parse_member_list(p, "VpcSubnetIds")
    if not subnet_ids:
        return _error("MissingParameter", "VpcSubnetIds is required", 400)
    resolved = _resolve_subnet_group_members(subnet_ids)
    if resolved is None:
        return _error("InvalidSubnet", _INVALID_SUBNET_MESSAGE, 400)
    _subnets, vpc_id = resolved

    region = get_region()
    security_groups = _parse_member_list(p, "VpcSecurityGroupIds")
    endpoint = {
        "DBProxyEndpointName": endpoint_name,
        "DBProxyEndpointArn": (
            f"arn:aws:rds:{region}:{get_account_id()}:db-proxy-endpoint:"
            f"{_new_db_proxy_id('prx-endpoint')}"
        ),
        "DBProxyName": proxy["DBProxyName"],
        "Status": "available",
        "VpcId": vpc_id,
        "VpcSecurityGroupIds": security_groups or list(proxy["VpcSecurityGroupIds"]),
        "VpcSubnetIds": subnet_ids,
        "Endpoint": (
            f"{endpoint_name}.endpoint.proxy-{proxy['_EndpointSuffix']}"
            f".{region}.rds.amazonaws.com"
        ),
        "CreatedDate": time.time(),
        # "The default is READ_WRITE."
        "TargetRole": target_role,
        "IsDefault": False,
        "EndpointNetworkType": _p(p, "EndpointNetworkType") or "IPV4",
    }
    _db_proxy_endpoints[endpoint_name] = endpoint

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[endpoint["DBProxyEndpointArn"]] = req_tags

    return _xml(200, "CreateDBProxyEndpointResponse",
        "<CreateDBProxyEndpointResult><DBProxyEndpoint>"
        f"{_db_proxy_endpoint_xml(endpoint)}"
        "</DBProxyEndpoint></CreateDBProxyEndpointResult>")


def _describe_db_proxy_endpoints(p):
    proxy_name = _p(p, "DBProxyName")
    if proxy_name:
        proxy = _get_db_proxy(proxy_name)
        if not proxy:
            return _db_proxy_not_found(proxy_name)
        proxy_name = proxy["DBProxyName"]

    endpoint_name = _p(p, "DBProxyEndpointName")
    if endpoint_name:
        endpoint = _request_region_get(
            _db_proxy_endpoints, endpoint_name, "db-proxy-endpoint",
        )
        if not endpoint or (proxy_name and endpoint["DBProxyName"] != proxy_name):
            return _db_proxy_endpoint_not_found(endpoint_name)
        endpoints = [endpoint]
    else:
        endpoints = sorted(
            (
                endpoint
                for endpoint in _db_proxy_endpoints.values()
                if not proxy_name or endpoint["DBProxyName"] == proxy_name
            ),
            key=lambda e: e["DBProxyEndpointName"],
        )

    page, next_marker, error = _db_proxy_page(
        p, endpoints, lambda e: e["DBProxyEndpointName"], "DB proxy endpoint",
    )
    if error:
        return error

    members = "".join(
        f"<member>{_db_proxy_endpoint_xml(e)}</member>" for e in page
    )
    return _xml(200, "DescribeDBProxyEndpointsResponse",
        "<DescribeDBProxyEndpointsResult><DBProxyEndpoints>"
        f"{members}</DBProxyEndpoints>{_marker_xml(next_marker)}"
        "</DescribeDBProxyEndpointsResult>")


def _modify_db_proxy_endpoint(p):
    endpoint_name = _p(p, "DBProxyEndpointName")
    endpoint = _request_region_get(
        _db_proxy_endpoints, endpoint_name, "db-proxy-endpoint",
    )
    if not endpoint:
        return _db_proxy_endpoint_not_found(endpoint_name)
    if endpoint.get("IsDefault"):
        # "The RDS proxy default endpoint cannot be modified."
        return _error(
            "InvalidDBProxyEndpointStateFault",
            f"The DB proxy endpoint {endpoint_name} is the default endpoint "
            "for its DB proxy and can't be modified.",
            400,
        )

    endpoint_name = endpoint["DBProxyEndpointName"]
    new_name = _p(p, "NewDBProxyEndpointName")
    if new_name and new_name != endpoint_name:
        name_error = _db_proxy_name_error(new_name, "NewDBProxyEndpointName")
        if name_error:
            return name_error
        if _db_proxy_endpoints.get(new_name):
            return _error(
                "DBProxyEndpointAlreadyExistsFault",
                f"The specified DB proxy endpoint name {new_name} must be "
                "unique for all DB proxy endpoints owned by your Amazon Web "
                "Services account in the specified Amazon Web Services Region.",
                400,
            )

    security_groups = _parse_member_list(p, "VpcSecurityGroupIds")
    if security_groups:
        endpoint["VpcSecurityGroupIds"] = security_groups

    if new_name and new_name != endpoint_name:
        proxy = _db_proxies.get(endpoint["DBProxyName"])
        suffix = proxy["_EndpointSuffix"] if proxy else new_uuid()[:8]
        endpoint["DBProxyEndpointName"] = new_name
        endpoint["Endpoint"] = (
            f"{new_name}.endpoint.proxy-{suffix}.{get_region()}.rds.amazonaws.com"
        )
        _db_proxy_endpoints.pop(endpoint_name, None)
        _db_proxy_endpoints[new_name] = endpoint

    return _xml(200, "ModifyDBProxyEndpointResponse",
        "<ModifyDBProxyEndpointResult><DBProxyEndpoint>"
        f"{_db_proxy_endpoint_xml(endpoint)}"
        "</DBProxyEndpoint></ModifyDBProxyEndpointResult>")


def _delete_db_proxy_endpoint(p):
    endpoint_name = _p(p, "DBProxyEndpointName")
    endpoint = _request_region_get(
        _db_proxy_endpoints, endpoint_name, "db-proxy-endpoint",
    )
    if not endpoint:
        return _db_proxy_endpoint_not_found(endpoint_name)
    if endpoint.get("IsDefault"):
        return _error(
            "InvalidDBProxyEndpointStateFault",
            f"The DB proxy endpoint {endpoint_name} is the default endpoint "
            "for its DB proxy and can't be deleted.",
            400,
        )

    _db_proxy_endpoints.pop(endpoint["DBProxyEndpointName"], None)
    _tags.pop(endpoint.get("DBProxyEndpointArn", ""), None)
    endpoint["Status"] = "deleting"
    return _xml(200, "DeleteDBProxyEndpointResponse",
        "<DeleteDBProxyEndpointResult><DBProxyEndpoint>"
        f"{_db_proxy_endpoint_xml(endpoint)}"
        "</DBProxyEndpoint></DeleteDBProxyEndpointResult>")


def _resolve_db_proxy_target_group(p):
    """Return (proxy, error). The only target group AWS exposes is 'default'."""
    proxy_name = _p(p, "DBProxyName")
    proxy = _get_db_proxy(proxy_name)
    if not proxy:
        return None, _db_proxy_not_found(proxy_name)
    group_name = _p(p, "TargetGroupName")
    if group_name and group_name != _DB_PROXY_DEFAULT_TARGET_GROUP:
        return None, _db_proxy_target_group_not_found(group_name)
    return proxy, None


def _describe_db_proxy_target_groups(p):
    proxy, error = _resolve_db_proxy_target_group(p)
    if error:
        return error

    page, next_marker, error = _db_proxy_page(
        p, [proxy["TargetGroup"]], lambda tg: tg["TargetGroupName"],
        "target group",
    )
    if error:
        return error

    members = "".join(
        f"<member>{_db_proxy_target_group_xml(tg)}</member>" for tg in page
    )
    return _xml(200, "DescribeDBProxyTargetGroupsResponse",
        f"<DescribeDBProxyTargetGroupsResult><TargetGroups>{members}"
        f"</TargetGroups>{_marker_xml(next_marker)}"
        "</DescribeDBProxyTargetGroupsResult>")


def _modify_db_proxy_target_group(p):
    proxy, error = _resolve_db_proxy_target_group(p)
    if error:
        return error
    if not _p(p, "TargetGroupName"):
        return _error("MissingParameter", "TargetGroupName is required", 400)

    new_name = _p(p, "NewName")
    if new_name and new_name != _DB_PROXY_DEFAULT_TARGET_GROUP:
        # "You can't rename the default target group."
        return _invalid_parameter_value(
            "You can't rename the default target group."
        )

    group = proxy["TargetGroup"]
    supplied = [
        key for key in p if key.startswith("ConnectionPoolConfig.")
    ]
    if supplied:
        config, config_error = _build_connection_pool_config(
            p, proxy["EngineFamily"],
        )
        if config_error:
            return config_error
        group["ConnectionPoolConfig"] = config
        group["UpdatedDate"] = time.time()

    return _xml(200, "ModifyDBProxyTargetGroupResponse",
        "<ModifyDBProxyTargetGroupResult><DBProxyTargetGroup>"
        f"{_db_proxy_target_group_xml(group)}"
        "</DBProxyTargetGroup></ModifyDBProxyTargetGroupResult>")


def _build_connection_pool_config(p, engine_family):
    """Build a ConnectionPoolConfig from a request, applying AWS's defaults.

    AWS documents each field's default in terms of what the *call* specifies
    ("If the value of MaxConnectionsPercent isn't specified, then ... the
    default is 50"), so a supplied ConnectionPoolConfig replaces the stored
    one rather than merging into it.
    """
    max_connections, error = _db_proxy_int_error(
        p, "ConnectionPoolConfig.MaxConnectionsPercent", 1, 100,
    )
    if error:
        return None, error
    max_idle, error = _db_proxy_int_error(
        p, "ConnectionPoolConfig.MaxIdleConnectionsPercent", 0, 100,
    )
    if error:
        return None, error
    borrow_timeout, error = _db_proxy_int_error(
        p, "ConnectionPoolConfig.ConnectionBorrowTimeout", 0, 300,
    )
    if error:
        return None, error

    # "If you specify MaxIdleConnectionsPercent, then you must also include a
    # value for MaxConnectionsPercent."
    if max_idle is not None and max_connections is None:
        return None, _invalid_parameter_combination(
            "MaxConnectionsPercent must be specified when "
            "MaxIdleConnectionsPercent is specified."
        )

    defaults = _default_connection_pool_config(engine_family)
    if max_connections is None:
        max_connections = defaults["MaxConnectionsPercent"]
        if max_idle is None:
            max_idle = defaults["MaxIdleConnectionsPercent"]
    elif max_idle is None:
        # "The default value is half of the value of MaxConnectionsPercent."
        max_idle = max_connections // 2

    # "Must be between 0 and the value of MaxConnectionsPercent."
    if max_idle > max_connections:
        return None, _invalid_parameter_value(
            f"Invalid value {max_idle} for MaxIdleConnectionsPercent. "
            f"Must be between 0 and the value of MaxConnectionsPercent "
            f"({max_connections})."
        )

    config = {
        "MaxConnectionsPercent": max_connections,
        "MaxIdleConnectionsPercent": max_idle,
        "ConnectionBorrowTimeout": (
            borrow_timeout if borrow_timeout is not None
            else defaults["ConnectionBorrowTimeout"]
        ),
        "SessionPinningFilters": _parse_member_list(
            p, "ConnectionPoolConfig.SessionPinningFilters",
        ),
    }
    init_query = _p(p, "ConnectionPoolConfig.InitQuery")
    if init_query:
        config["InitQuery"] = init_query
    return config, None


def _db_proxy_engine_family_error(proxy, engine, identifier):
    """Refuse a target whose engine the proxy's EngineFamily doesn't support."""
    family = proxy["EngineFamily"]
    supported = _DB_PROXY_ENGINE_FAMILY_ENGINES.get(family, ())
    if engine and engine not in supported:
        return _invalid_parameter_value(
            f"The engine {engine} of {identifier} is not supported by a proxy "
            f"with an EngineFamily of {family}."
        )
    return None


def _register_db_proxy_targets(p):
    proxy, error = _resolve_db_proxy_target_group(p)
    if error:
        return error

    instance_ids = _parse_member_list(p, "DBInstanceIdentifiers")
    cluster_ids = _parse_member_list(p, "DBClusterIdentifiers")
    registered = {
        (target["Type"], target["RdsResourceId"]) for target in proxy["Targets"]
    }

    new_targets = []
    for instance_id in instance_ids:
        instance = _instances.get(instance_id)
        if not instance:
            return _error(
                "DBInstanceNotFound",
                f"DBInstance {instance_id} not found.",
                404,
            )
        if ("RDS_INSTANCE", instance_id) in registered:
            return _error(
                "DBProxyTargetAlreadyRegisteredFault",
                "The proxy is already associated with the specified RDS DB "
                f"instance {instance_id}.",
                400,
            )
        family_error = _db_proxy_engine_family_error(
            proxy, instance.get("Engine", ""), instance_id,
        )
        if family_error:
            return family_error
        new_targets.append({
            "RdsResourceId": instance_id,
            "Type": "RDS_INSTANCE",
            "Role": "READ_WRITE",
        })

    for cluster_id in cluster_ids:
        cluster = _clusters.get(cluster_id)
        if not cluster:
            return _error(
                "DBClusterNotFoundFault",
                f"DBCluster {cluster_id} not found.",
                404,
            )
        if ("TRACKED_CLUSTER", cluster_id) in registered:
            return _error(
                "DBProxyTargetAlreadyRegisteredFault",
                "The proxy is already associated with the specified Aurora DB "
                f"cluster {cluster_id}.",
                400,
            )
        family_error = _db_proxy_engine_family_error(
            proxy, cluster.get("Engine", ""), cluster_id,
        )
        if family_error:
            return family_error
        new_targets.append({
            "TrackedClusterId": cluster_id,
            "RdsResourceId": cluster_id,
            "Type": "TRACKED_CLUSTER",
            "Role": "READ_WRITE",
        })

    # "Each proxy can be associated with a single target DB instance. However,
    # you can associate multiple proxies with the same DB instance."
    if len(proxy["Targets"]) + len(new_targets) > 1:
        return _invalid_parameter_value(
            f"The proxy {proxy['DBProxyName']} can be associated with a "
            "single target. Deregister the current target before registering "
            "another one."
        )

    proxy["Targets"].extend(new_targets)
    members = "".join(
        f"<member>{_db_proxy_target_xml(_db_proxy_target_view(t))}</member>"
        for t in new_targets
    )
    return _xml(200, "RegisterDBProxyTargetsResponse",
        "<RegisterDBProxyTargetsResult><DBProxyTargets>"
        f"{members}"
        "</DBProxyTargets></RegisterDBProxyTargetsResult>")


def _db_proxy_target_view(target):
    """Resolve a registration against the live instance/cluster record.

    Endpoint, port and health are read at describe time, not frozen at
    registration: a target whose backing database was deleted or stopped must
    not keep reporting itself AVAILABLE at the address it used to have.
    """
    view = dict(target)
    identifier = target["RdsResourceId"]
    if target["Type"] == "RDS_INSTANCE":
        record = _instances.get(identifier)
        arn_key, status_key = "DBInstanceArn", "DBInstanceStatus"
        endpoint = (record or {}).get("Endpoint", {})
        address, port = endpoint.get("Address", ""), endpoint.get("Port", 0)
    else:
        record = _clusters.get(identifier)
        arn_key, status_key = "DBClusterArn", "Status"
        address = (record or {}).get("Endpoint", "")
        port = (record or {}).get("Port", 0)

    if record is None:
        view["TargetArn"] = ""
        view["Endpoint"] = ""
        view["Port"] = 0
        view["TargetHealth"] = {"State": "UNAVAILABLE"}
        return view

    view["TargetArn"] = record.get(arn_key, "")
    view["Endpoint"] = address
    view["Port"] = port
    stopped = record.get(status_key) == "stopped"
    view["TargetHealth"] = {"State": "UNAVAILABLE" if stopped else "AVAILABLE"}
    return view


def _deregister_db_proxy_targets(p):
    proxy, error = _resolve_db_proxy_target_group(p)
    if error:
        return error

    wanted = [
        ("RDS_INSTANCE", identifier)
        for identifier in _parse_member_list(p, "DBInstanceIdentifiers")
    ] + [
        ("TRACKED_CLUSTER", identifier)
        for identifier in _parse_member_list(p, "DBClusterIdentifiers")
    ]
    registered = {
        (target["Type"], target["RdsResourceId"]) for target in proxy["Targets"]
    }
    for target_type, identifier in wanted:
        if (target_type, identifier) not in registered:
            return _error(
                "DBProxyTargetNotFoundFault",
                "The specified RDS DB instance or Aurora DB cluster "
                f"{identifier} isn't available for a proxy owned by your "
                "Amazon Web Services account in the specified Amazon Web "
                "Services Region.",
                404,
            )

    drop = set(wanted)
    proxy["Targets"] = [
        target
        for target in proxy["Targets"]
        if (target["Type"], target["RdsResourceId"]) not in drop
    ]
    # DeregisterDBProxyTargets has an empty output shape, so botocore still
    # looks for the wrapping result element.
    return _xml(200, "DeregisterDBProxyTargetsResponse",
        "<DeregisterDBProxyTargetsResult/>")


def _describe_db_proxy_targets(p):
    proxy, error = _resolve_db_proxy_target_group(p)
    if error:
        return error

    page, next_marker, error = _db_proxy_page(
        p, proxy["Targets"],
        lambda t: f"{t['Type']}:{t['RdsResourceId']}", "target",
    )
    if error:
        return error

    members = "".join(
        f"<member>{_db_proxy_target_xml(_db_proxy_target_view(t))}</member>"
        for t in page
    )
    return _xml(200, "DescribeDBProxyTargetsResponse",
        f"<DescribeDBProxyTargetsResult><Targets>{members}</Targets>"
        f"{_marker_xml(next_marker)}</DescribeDBProxyTargetsResult>")


def _string_list_xml(tag, values):
    members = "".join(f"<member>{_esc(v)}</member>" for v in values)
    return f"<{tag}>{members}</{tag}>"


def _db_proxy_xml(px):
    auth_xml = ""
    for entry in px.get("Auth", []):
        fields = "".join(
            f"<{field}>{_esc(entry[field])}</{field}>"
            for field in (
                "Description",
                "UserName",
                "AuthScheme",
                "SecretArn",
                "IAMAuth",
                "ClientPasswordAuthType",
            )
            if entry.get(field)
        )
        auth_xml += f"<member>{fields}</member>"
    default_auth_scheme = px.get("DefaultAuthScheme")
    default_auth_xml = (
        f"<DefaultAuthScheme>{_esc(default_auth_scheme)}</DefaultAuthScheme>"
        if default_auth_scheme
        else ""
    )
    return f"""<DBProxyName>{_esc(px['DBProxyName'])}</DBProxyName>
        <DBProxyArn>{px['DBProxyArn']}</DBProxyArn>
        <Status>{px['Status']}</Status>
        <EngineFamily>{px['EngineFamily']}</EngineFamily>
        <VpcId>{px.get('VpcId','')}</VpcId>
        {_string_list_xml('VpcSecurityGroupIds', px.get('VpcSecurityGroupIds', []))}
        {_string_list_xml('VpcSubnetIds', px.get('VpcSubnetIds', []))}
        {default_auth_xml}
        <Auth>{auth_xml}</Auth>
        <RoleArn>{_esc(px.get('RoleArn',''))}</RoleArn>
        <Endpoint>{px.get('Endpoint','')}</Endpoint>
        <RequireTLS>{str(px.get('RequireTLS', False)).lower()}</RequireTLS>
        <IdleClientTimeout>{px.get('IdleClientTimeout', 1800)}</IdleClientTimeout>
        <DebugLogging>{str(px.get('DebugLogging', False)).lower()}</DebugLogging>
        <CreatedDate>{_format_time(px['CreatedDate'])}</CreatedDate>
        <UpdatedDate>{_format_time(px['UpdatedDate'])}</UpdatedDate>
        <EndpointNetworkType>{px.get('EndpointNetworkType','IPV4')}</EndpointNetworkType>
        <TargetConnectionNetworkType>{px.get('TargetConnectionNetworkType','IPV4')}</TargetConnectionNetworkType>"""


def _db_proxy_endpoint_xml(ep):
    return f"""<DBProxyEndpointName>{_esc(ep['DBProxyEndpointName'])}</DBProxyEndpointName>
        <DBProxyEndpointArn>{ep['DBProxyEndpointArn']}</DBProxyEndpointArn>
        <DBProxyName>{_esc(ep['DBProxyName'])}</DBProxyName>
        <Status>{ep['Status']}</Status>
        <VpcId>{ep.get('VpcId','')}</VpcId>
        {_string_list_xml('VpcSecurityGroupIds', ep.get('VpcSecurityGroupIds', []))}
        {_string_list_xml('VpcSubnetIds', ep.get('VpcSubnetIds', []))}
        <Endpoint>{ep.get('Endpoint','')}</Endpoint>
        <CreatedDate>{_format_time(ep['CreatedDate'])}</CreatedDate>
        <TargetRole>{ep.get('TargetRole','READ_WRITE')}</TargetRole>
        <IsDefault>{str(ep.get('IsDefault', False)).lower()}</IsDefault>
        <EndpointNetworkType>{ep.get('EndpointNetworkType','IPV4')}</EndpointNetworkType>"""


def _db_proxy_target_group_xml(tg):
    config = tg.get("ConnectionPoolConfig", {})
    init_query = config.get("InitQuery")
    init_query_xml = (
        f"<InitQuery>{_esc(init_query)}</InitQuery>" if init_query else ""
    )
    return f"""<DBProxyName>{_esc(tg['DBProxyName'])}</DBProxyName>
        <TargetGroupName>{_esc(tg['TargetGroupName'])}</TargetGroupName>
        <TargetGroupArn>{tg['TargetGroupArn']}</TargetGroupArn>
        <IsDefault>{str(tg.get('IsDefault', True)).lower()}</IsDefault>
        <Status>{tg.get('Status','available')}</Status>
        <ConnectionPoolConfig>
            <MaxConnectionsPercent>{config.get('MaxConnectionsPercent', 100)}</MaxConnectionsPercent>
            <MaxIdleConnectionsPercent>{config.get('MaxIdleConnectionsPercent', 50)}</MaxIdleConnectionsPercent>
            <ConnectionBorrowTimeout>{config.get('ConnectionBorrowTimeout', 120)}</ConnectionBorrowTimeout>
            {_string_list_xml('SessionPinningFilters', config.get('SessionPinningFilters', []))}
            {init_query_xml}
        </ConnectionPoolConfig>
        <CreatedDate>{_format_time(tg['CreatedDate'])}</CreatedDate>
        <UpdatedDate>{_format_time(tg['UpdatedDate'])}</UpdatedDate>"""


def _db_proxy_target_xml(target):
    health = target.get("TargetHealth", {})
    tracked_cluster = target.get("TrackedClusterId")
    tracked_xml = (
        f"<TrackedClusterId>{_esc(tracked_cluster)}</TrackedClusterId>"
        if tracked_cluster
        else ""
    )
    reason = health.get("Reason")
    description = health.get("Description")
    return f"""<TargetArn>{target.get('TargetArn','')}</TargetArn>
        <Endpoint>{target.get('Endpoint','')}</Endpoint>
        {tracked_xml}
        <RdsResourceId>{_esc(target.get('RdsResourceId',''))}</RdsResourceId>
        <Port>{target.get('Port', 0)}</Port>
        <Type>{target.get('Type','RDS_INSTANCE')}</Type>
        <Role>{target.get('Role','READ_WRITE')}</Role>
        <TargetHealth>
            <State>{health.get('State','AVAILABLE')}</State>
            {f'<Reason>{reason}</Reason>' if reason else ''}
            {f'<Description>{_esc(description)}</Description>' if description else ''}
        </TargetHealth>"""



def _xml(status, root_tag, inner):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    # Real AWS Query-protocol responses include <Type>Sender|Receiver</Type>
    # — Sender for 4xx (caller's fault), Receiver for 5xx. Most SDKs ignore
    # this field but it's part of the documented AWS shape.
    fault_type = "Sender" if 400 <= status < 500 else "Receiver"
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
    <Error><Type>{fault_type}</Type><Code>{code}</Code><Message>{message}</Message></Error>
    <RequestId>{new_uuid()}</RequestId>
</ErrorResponse>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


# ---------------------------------------------------------------------------
# Action map
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    "CreateDBInstance": _create_db_instance,
    "DeleteDBInstance": _delete_db_instance,
    "DescribeDBInstances": _describe_db_instances,
    "ModifyDBInstance": _modify_db_instance,
    "StartDBInstance": _start_db_instance,
    "StopDBInstance": _stop_db_instance,
    "RebootDBInstance": _reboot_db_instance,
    "CreateDBInstanceReadReplica": _create_read_replica,
    "RestoreDBInstanceFromDBSnapshot": _restore_from_snapshot,
    "CreateDBCluster": _create_db_cluster,
    "DeleteDBCluster": _delete_db_cluster,
    "DescribeDBClusters": _describe_db_clusters,
    "ModifyDBCluster": _modify_db_cluster,
    "StartDBCluster": _start_db_cluster,
    "StopDBCluster": _stop_db_cluster,
    "FailoverDBCluster": _failover_db_cluster,
    "CreateDBSnapshot": _create_db_snapshot,
    "DeleteDBSnapshot": _delete_db_snapshot,
    "DescribeDBSnapshots": _describe_db_snapshots,
    "CreateDBClusterSnapshot": _create_db_cluster_snapshot,
    "DescribeDBClusterSnapshots": _describe_db_cluster_snapshots,
    "DeleteDBClusterSnapshot": _delete_db_cluster_snapshot,
    "CreateDBSubnetGroup": _create_subnet_group,
    "DeleteDBSubnetGroup": _delete_subnet_group,
    "DescribeDBSubnetGroups": _describe_subnet_groups,
    "ModifyDBSubnetGroup": _modify_subnet_group,
    "CreateDBParameterGroup": _create_param_group,
    "DeleteDBParameterGroup": _delete_param_group,
    "DescribeDBParameterGroups": _describe_param_groups,
    "DescribeDBParameters": _describe_db_parameters,
    "ModifyDBParameterGroup": _modify_param_group,
    "ResetDBParameterGroup": _reset_param_group,
    "CreateDBClusterParameterGroup": _create_db_cluster_param_group,
    "DescribeDBClusterParameterGroups": _describe_db_cluster_param_groups,
    "DeleteDBClusterParameterGroup": _delete_db_cluster_param_group,
    "DescribeDBClusterParameters": _describe_db_cluster_parameters,
    "ModifyDBClusterParameterGroup": _modify_db_cluster_param_group,
    "ResetDBClusterParameterGroup": _reset_db_cluster_param_group,
    "CreateOptionGroup": _create_option_group,
    "DeleteOptionGroup": _delete_option_group,
    "DescribeOptionGroups": _describe_option_groups,
    "DescribeOptionGroupOptions": _describe_option_group_options,
    "ListTagsForResource": _list_tags,
    "AddTagsToResource": _add_tags,
    "RemoveTagsFromResource": _remove_tags,
    "DescribeDBEngineVersions": _describe_engine_versions,
    "DescribeOrderableDBInstanceOptions": _describe_orderable_options,
    "DescribePendingMaintenanceActions": _describe_pending_maintenance_actions,
    "CreateGlobalCluster": _create_global_cluster,
    "DescribeGlobalClusters": _describe_global_clusters,
    "DeleteGlobalCluster": _delete_global_cluster,
    "RemoveFromGlobalCluster": _remove_from_global_cluster,
    "ModifyGlobalCluster": _modify_global_cluster,
    "SwitchoverGlobalCluster": _switchover_global_cluster,
    "FailoverGlobalCluster": _failover_global_cluster,
    "EnableHttpEndpoint": _enable_http_endpoint,
    "DisableHttpEndpoint": _disable_http_endpoint,
    "CreateDBProxy": _create_db_proxy,
    "DescribeDBProxies": _describe_db_proxies,
    "ModifyDBProxy": _modify_db_proxy,
    "DeleteDBProxy": _delete_db_proxy,
    "CreateDBProxyEndpoint": _create_db_proxy_endpoint,
    "DescribeDBProxyEndpoints": _describe_db_proxy_endpoints,
    "ModifyDBProxyEndpoint": _modify_db_proxy_endpoint,
    "DeleteDBProxyEndpoint": _delete_db_proxy_endpoint,
    "DescribeDBProxyTargetGroups": _describe_db_proxy_target_groups,
    "ModifyDBProxyTargetGroup": _modify_db_proxy_target_group,
    "RegisterDBProxyTargets": _register_db_proxy_targets,
    "DeregisterDBProxyTargets": _deregister_db_proxy_targets,
    "DescribeDBProxyTargets": _describe_db_proxy_targets,
}


def reset():
    # Serialize teardown with warm-boot shared-container startup. Otherwise a
    # restore worker can pass its membership check after reset has enumerated
    # resources, then create a container after the stores are cleared.
    with _shared_container_lock:
        docker_client = _get_docker()
        if docker_client:
            # Shared containers are cluster-owned. Reap them once from the cluster
            # records before considering standalone instances; otherwise every
            # member would try to remove the same container and an empty cluster
            # would leak its backing database.
            shared_container_ids = {
                cluster.get("_shared_container_id")
                for cluster in _clusters.all_values()
                if cluster.get("_shared_container_id")
            }
            for (account_id, region, cluster_id), cluster in _clusters.all_items():
                if any(
                    cluster.get(field)
                    for field in (
                        "_shared_container_id",
                        "_shared_endpoint",
                        "_shared_volume_name",
                    )
                ):
                    _remove_cluster_shared_resources(
                        cluster_id,
                        cluster,
                        timeout=2,
                        account_id=account_id,
                        region=region,
                    )
            for instance in _instances.all_values():
                cid = instance.get("_docker_container_id")
                if (
                    cid
                    and cid not in shared_container_ids
                    and _instance_owns_container(instance)
                ):
                    try:
                        c = docker_client.containers.get(cid)
                        c.stop(timeout=2)
                        c.remove(v=True)
                    except Exception as e:
                        logger.warning(
                            "reset: failed to stop/remove container %s: %s",
                            cid,
                            e,
                        )
        _instances.clear()
        _clusters.clear()
        _subnet_groups.clear()
        _param_groups.clear()
        _snapshots.clear()
        _db_cluster_param_groups.clear()
        _db_cluster_snapshots.clear()
        _option_groups.clear()
        _db_proxies.clear()
        _db_proxy_endpoints.clear()
        _global_clusters.clear()
        _tags.clear()
        _port_counter[0] = BASE_PORT


# Load persisted state at module import. Must run AFTER every helper this
# code path may touch (notably `_get_docker`, `_docker_image_for_engine`,
# `_get_ministack_network`) is defined — `restore_state` spawns daemon threads
# that race against the rest of module parsing, and a thread reaching an
# undefined name raises NameError mid-restore (issue #692 follow-up).
try:
    _restored = load_state("rds")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


async def handle_request(method, path, headers, body, query_params):
    """Dispatch off the event loop.

    Request paths here reach the Docker daemon (container create/start/stop/
    inspect), which blocks for as long as the daemon takes. Measured on ECS: a
    cached-image container start held the loop for 7.3s, during which the health
    endpoint — the cheapest request in the process — could not be served.

    Uses the shared pool: the containers started here (database / cache / search
    engines) never call back into MiniStack, so this cannot re-enter.
    """
    return await run_offloop(
        _handle_request_sync, method, path, headers, body, query_params)


def _live_container_ids():
    """Container ids still owned by a live DB instance or cluster.

    A stopped instance still owns its (exited) container — StartDBInstance must
    be able to restart it — so it is reported here and never reaped.
    """
    ids = set()
    for _key, inst in _instances.all_items():
        cid = inst.get("_docker_container_id")
        if cid:
            ids.add(cid)
    for _key, cl in _clusters.all_items():
        cid = cl.get("_shared_container_id")
        if cid:
            ids.add(cid)
    return ids


container_reaper.register_live_ids("rds", _live_container_ids)
