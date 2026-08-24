"""Amazon DocumentDB (DocDB) service emulator.

Control plane emulated in-process (Query API ``Action=`` form bodies plus
``application/x-amz-json-1.*`` JSON bodies for the same actions); data plane is
real MongoDB — when Docker is available, cluster compute runs in a real
``mongo`` container and every response endpoint is wire-connectable with
pymongo or any MongoDB driver. There is no in-process emulation of Mongo
commands.

Engine versions (single source of truth: ``DOCDB_ENGINE_VERSIONS``):
  - 5.0.0 → family ``docdb5.0``, backed by ``mongo:5.0``
  - 8.0.0 → family ``docdb8.0``, backed by ``mongo:8.0``
5.0.0 is the default, matching the AWS SDK default. Images honor
``MINISTACK_IMAGE_PREFIX``.

Cluster model mirrors RDS/Aurora: one shared mongo container per DB cluster.
The first member created on a cluster starts it; later members are control-
plane records aliasing the same endpoint, so data written through one member
is visible through all members and through both cluster endpoints. Deleting
the last member removes its container but keeps the named volume (under
``DOCDB_PERSIST=1``), so a later member restarts onto preserved data; only
``DeleteDBCluster`` removes container and storage. Standalone instances
(created without ``DBClusterIdentifier`` over raw HTTP) keep their own
per-instance container.

Supported actions (aligned with ``_ACTION_MAP``):
  CreateDBInstance, DeleteDBInstance, DescribeDBInstances, ModifyDBInstance,
  StartDBInstance, StopDBInstance, RebootDBInstance,
  CreateDBCluster, DeleteDBCluster, DescribeDBClusters, ModifyDBCluster,
  StartDBCluster, StopDBCluster, FailoverDBCluster, RestoreDBClusterFromSnapshot,
  CreateDBSubnetGroup, DeleteDBSubnetGroup, DescribeDBSubnetGroups,
  CreateDBSnapshot*, DeleteDBSnapshot*, DescribeDBSnapshots*,
  CreateDBClusterSnapshot, DescribeDBClusterSnapshots, DeleteDBClusterSnapshot,
  ModifyDBClusterSnapshotAttribute, DescribeDBClusterSnapshotAttributes,
  CreateDBClusterParameterGroup, DescribeDBClusterParameterGroups,
  DeleteDBClusterParameterGroup, ModifyDBClusterParameterGroup,
  ResetDBClusterParameterGroup, DescribeDBClusterParameters,
  ListTagsForResource, AddTagsToResource, RemoveTagsFromResource,
  DescribeDBEngineVersions, DescribeOrderableDBInstanceOptions,
  ApplyPendingMaintenanceAction, DescribePendingMaintenanceActions,
  DescribeCertificates, DescribeEvents.

  * Create/Delete/DescribeDBSnapshots do not exist in the real DocumentDB API
    (DocDB has only cluster snapshots). They are served for legacy direct-HTTP
    callers but cannot be reached through the boto3 ``docdb`` client.

Snapshots are metadata-only: they record the cluster/instance configuration
and tags but contain no database dump, so ``RestoreDBClusterFromSnapshot``
yields an empty database.

Env vars: DOCDB_BASE_PORT (default 27117), DOCDB_PERSIST, DOCDB_TMPFS_SIZE,
DOCKER_NETWORK.

References:
- AWS DocDB API: https://docs.aws.amazon.com/documentdb/latest/APIReference/API_Operations_Amazon_DocumentDB_with_MongoDB_compatibility.html
"""

import copy
import datetime
import json
import logging
import os
import socket
import threading
import time
from urllib.parse import parse_qs
from xml.sax.saxutils import escape as _esc

from ministack.core import container_reaper
from ministack.core.concurrency import run_offloop
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    apply_image_prefix,
    get_account_id,
    get_region,
    new_uuid,
)

logger = logging.getLogger("documentdb")

ACCOUNT_ID = "000000000000"
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
BASE_PORT = int(os.environ.get("DOCDB_BASE_PORT", "27117"))
DOCDB_TMPFS_SIZE = os.environ.get("DOCDB_TMPFS_SIZE", "256m")
DOCDB_PERSIST = os.environ.get("DOCDB_PERSIST", "0").lower() in ("1", "true", "yes")
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")

# Single source of truth for DocumentDB engine versions: (version, parameter
# group family). 5.0.0 stays the default per the AWS SDK default.
DOCDB_ENGINE_VERSIONS = [("5.0.0", "docdb5.0"), ("8.0.0", "docdb8.0")]
_DOCDB_ENGINE_VERSION_SET = {version for version, _ in DOCDB_ENGINE_VERSIONS}
DEFAULT_ENGINE_VERSION = "5.0.0"

_instances = AccountRegionScopedDict()
_clusters = AccountRegionScopedDict()
_subnet_groups = AccountRegionScopedDict()
_snapshots = AccountRegionScopedDict()
_db_cluster_snapshots = AccountRegionScopedDict()
_db_cluster_param_groups = AccountRegionScopedDict()
_tags = AccountScopedDict()
_port_counter = [BASE_PORT]

# Recorded by ApplyPendingMaintenanceAction, returned by
# DescribePendingMaintenanceActions. In-memory only (not persisted).
_pending_maintenance_actions: list = []

_docker = None
_ministack_network = None

_shared_container_lock = threading.RLock()
_port_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def get_state():
    """Return a persistable snapshot of all DocumentDB state.

    Returns:
        dict: Store name → record mapping, plus the port counter. Non-
            restorable fields (Docker container ids) are stripped; clusters
            keep their endpoint/port/volume metadata so warm boot can reattach
            to surviving containers and volumes.
    """
    with _shared_container_lock:
        instances = copy.deepcopy(_instances)
        clusters = copy.deepcopy(_clusters)
        state = {
            "instances": instances,
            "clusters": clusters,
            "subnet_groups": copy.deepcopy(_subnet_groups),
            "snapshots": copy.deepcopy(_snapshots),
            "db_cluster_snapshots": copy.deepcopy(_db_cluster_snapshots),
            "db_cluster_param_groups": copy.deepcopy(_db_cluster_param_groups),
            "tags": copy.deepcopy(_tags),
            "port_counter": _port_counter[0],
        }
    for key in list(instances._data):
        instances._data[key].pop("_docker_container_id", None)
    for key in list(clusters._data):
        clusters._data[key].pop("_shared_container_id", None)
    return state


def restore_state(data):
    """Load persisted state and respawn backing containers.

    Instances and clusters come back marked ``creating`` while daemon threads
    restart their mongo containers (reusing persisted host ports when still
    free), then flip to ``available`` once TCP-ready. Stopped resources stay
    stopped. Accepts the current ``(account, region, key)``-keyed layout plus
    older account-scoped and plain-dict layouts defensively.

    Args:
        data: Persisted payload produced by :func:`get_state`; may be empty.
    """
    if not data:
        return
    _clusters.update(data.get("clusters", {}))
    for key, cluster in list(getattr(_clusters, "_data", {}).items()):
        account_id, region, cluster_id = key
        if not isinstance(cluster, dict):
            continue
        cluster["_shared_container_id"] = None
        cluster["_shared_container_ready"] = False
        if DOCDB_PERSIST:
            cluster.setdefault("_shared_volume_name", _cluster_volume_name(cluster_id))
        _clusters._data[key] = cluster

    _subnet_groups.update(data.get("subnet_groups", {}))
    _snapshots.update(data.get("snapshots", {}))
    _db_cluster_snapshots.update(data.get("db_cluster_snapshots", {}))
    _db_cluster_param_groups.update(data.get("db_cluster_param_groups", {}))
    _tags.update(data.get("tags", {}))
    if "port_counter" in data:
        _port_counter[0] = data["port_counter"]

    instances_data = data.get("instances", {})
    to_respawn = []
    if hasattr(instances_data, "all_items"):
        # Current layout: (account_id, region, instance_id) keyed records.
        for key, inst in instances_data.all_items():
            account_id, region, db_id = key
            inst["_docker_container_id"] = None
            inst["DBInstanceStatus"] = "creating"
            if DOCDB_PERSIST:
                inst.setdefault("_docker_volume_name", _instance_volume_name(db_id))
            _instances._data[(account_id, region, db_id)] = inst
            to_respawn.append((account_id, region, db_id, inst))
    elif hasattr(instances_data, "_data"):
        # Legacy account-scoped layout: (account_id, instance_id) keys.
        for key, inst in instances_data._data.items():
            account_id, db_id = key
            region = _record_region(inst)
            inst["_docker_container_id"] = None
            inst["DBInstanceStatus"] = "creating"
            _instances._data[(account_id, region, db_id)] = inst
            to_respawn.append((account_id, region, db_id, inst))
    else:
        # Legacy plain-dict layout: name → record.
        from ministack.core.responses import get_account_id as _acct

        for name, inst in instances_data.items():
            account_id = (_record_account(inst) or _acct())
            region = _record_region(inst)
            inst["_docker_container_id"] = None
            inst["DBInstanceStatus"] = "creating"
            _instances.set_scoped(account_id, region, name, inst)
            to_respawn.append((account_id, region, name, inst))

    # Group member records per cluster so each shared container is respawned
    # exactly once; standalone instances respawn individually.
    member_groups: dict = {}
    standalone = []
    for account_id, region, db_id, inst in to_respawn:
        cluster_id = inst.get("_shared_cluster_id") or inst.get("DBClusterIdentifier")
        if cluster_id:
            member_groups.setdefault((account_id, region, cluster_id), []).append(inst)
        else:
            standalone.append((account_id, region, db_id, inst))

    for (account_id, region, cluster_id), members in member_groups.items():
        thread = threading.Thread(
            target=_respawn_cluster_members,
            args=(account_id, region, cluster_id, members),
            daemon=True,
            name=f"ministack-docdb-respawn-{cluster_id}",
        )
        thread.start()
    for account_id, region, db_id, inst in standalone:
        thread = threading.Thread(
            target=_respawn_standalone_instance,
            args=(account_id, region, db_id, inst),
            daemon=True,
            name=f"ministack-docdb-respawn-{db_id}",
        )
        thread.start()


def _record_region(record):
    """Best-effort region from an ``arn:aws:rds:<region>:...`` record field."""
    for field in ("DBInstanceArn", "DBClusterArn"):
        parts = (record.get(field) or "").split(":")
        if len(parts) > 3 and parts[3]:
            return parts[3]
    return REGION


def _record_account(record):
    """Best-effort account id from an ARN-shaped record field."""
    for field in ("DBInstanceArn", "DBClusterArn"):
        parts = (record.get(field) or "").split(":")
        if len(parts) > 4 and parts[4]:
            return parts[4]
    return None


def _respawn_cluster_members(account_id, region, cluster_id, members):
    """Restart one cluster's shared container after a warm boot.

    Reuses the persisted host port when it is free, waits for TCP readiness on
    the chosen address, then publishes every member ``available``.
    """
    from ministack.core.responses import _request_account_id, _request_region

    if account_id:
        _request_account_id.set(account_id)
    if region:
        _request_region.set(region)
    cluster = _clusters.get_scoped(account_id, region, cluster_id)
    if not cluster:
        for member in members:
            member["DBInstanceStatus"] = "failed"
        return
    if cluster.get("Status") == "stopped":
        cluster["_shared_container_ready"] = False
        for member in members:
            member["DBInstanceStatus"] = "stopped"
        return
    restore_epoch = int(cluster.get("_shared_container_epoch", 0))
    docker_client = _get_docker()

    result = {"started": False, "failed": False}
    with _shared_container_lock:
        current = _clusters.get_scoped(account_id, region, cluster_id)
        if current is not cluster or int(cluster.get("_shared_container_epoch", 0)) != restore_epoch:
            return
        if docker_client:
            if cluster.get("_shared_container_id"):
                result = _restart_cluster_shared_container(cluster_id, cluster)
            else:
                result = _start_cluster_shared_container(cluster_id, cluster, remove_stale=True)

    readiness_host, readiness_port = _readiness_target(cluster, result)
    if result.get("started"):
        ok = _wait_for_port(readiness_host, readiness_port) if readiness_port else True
        status = "available" if ok else "failed"
        if ok:
            logger.info(
                "docdb: restored cluster %s ready at %s:%s",
                cluster_id, readiness_host, readiness_port,
            )
        else:
            logger.warning(
                "docdb: restored cluster %s at %s:%s not ready after timeout",
                cluster_id, readiness_host, readiness_port,
            )
    elif result.get("failed"):
        status = "failed"
    else:
        # No Docker available: compute is virtual, so publish availability.
        status = "available"
    with _shared_container_lock:
        live = _clusters.get_scoped(account_id, region, cluster_id)
        if live is not cluster:
            return
        for member in members:
            member["DBInstanceStatus"] = status


def _respawn_standalone_instance(account_id, region, db_id, instance):
    """Restart one standalone instance's own container after a warm boot."""
    from ministack.core.responses import _request_account_id, _request_region

    if account_id:
        _request_account_id.set(account_id)
    if region:
        _request_region.set(region)
    if instance.get("DBInstanceStatus") == "stopped":
        return
    docker_client = _get_docker()
    if not docker_client:
        instance["DBInstanceStatus"] = "available"
        return
    engine_version = instance.get("EngineVersion") or DEFAULT_ENGINE_VERSION
    master_user = instance.get("MasterUsername", "root")
    master_pass = instance.get("_MasterUserPassword", "password")
    image, env, container_port, data_path = _docker_image_for_docdb(
        engine_version, master_user, master_pass, instance.get("DBName") or "admin",
    )
    host_port = instance.get("_host_port") or _next_port()
    if not _is_host_port_free(host_port):
        host_port = _next_port()
    volume_name = instance.get("_docker_volume_name") if DOCDB_PERSIST else None
    started = _launch_mongo_container(
        f"ministack-docdb-{db_id}",
        image, env, host_port, container_port, data_path,
        labels={
            **container_reaper.own_labels("documentdb"),
            "db_id": db_id,
            "account_id": account_id or get_account_id(),
            "region": region or get_region(),
        },
        volume_name=volume_name,
    ) if image else None
    if not started:
        instance["DBInstanceStatus"] = "failed"
        return
    container_id, internal_addr, internal_port, ep_addr, ep_port = started
    instance.update({
        "_docker_container_id": container_id,
        "_internal_address": internal_addr,
        "_internal_port": internal_port,
        "_host_port": host_port,
        "Endpoint": {"Address": ep_addr, "Port": ep_port, "HostedZoneId": "Z2R2ITUGPM61AM"},
    })
    ok = _wait_for_port(ep_addr, ep_port)
    instance["DBInstanceStatus"] = "available" if ok else "failed"


def _readiness_target(cluster, start_result):
    """Pick the host:port to probe for a freshly (re)started shared container."""
    if start_result.get("readiness_port"):
        return start_result.get("readiness_host"), start_result.get("readiness_port")
    endpoint = cluster.get("_shared_endpoint") or {}
    address = cluster.get("_shared_internal_address") or endpoint.get("Address") or "127.0.0.1"
    port = int(endpoint.get("Port") or 27017)
    return ("127.0.0.1" if address in ("localhost", "") else address), port


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

def _get_docker():
    """Return a cached Docker client, or None when Docker is unavailable."""
    global _docker
    if _docker is None:
        try:
            import docker
            _docker = docker.from_env()
        except Exception:
            pass
    return _docker


def _get_ministack_network(docker_client):
    """Detect the Docker network MiniStack itself runs on (if containerised)."""
    global _ministack_network
    if _ministack_network is not None:
        return _ministack_network or None
    if DOCKER_NETWORK:
        _ministack_network = DOCKER_NETWORK
        logger.debug("DocDB: using DOCKER_NETWORK=%s", DOCKER_NETWORK)
        return DOCKER_NETWORK
    try:
        self_container = docker_client.containers.get(os.environ.get("HOSTNAME", ""))
        nets = list(self_container.attrs["NetworkSettings"]["Networks"].keys())
        if nets:
            _ministack_network = nets[0]
            logger.debug("DocDB: detected MiniStack network: %s", _ministack_network)
            return _ministack_network
    except Exception:
        logger.debug("DocDB: could not detect MiniStack network, using localhost")
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


def _is_host_port_free(port):
    """True when no listener currently holds the given localhost port."""
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=0.5):
            return False
    except OSError:
        return True


def _next_port():
    """Allocate the next host port for published containers."""
    with _port_lock:
        port = _port_counter[0]
        _port_counter[0] += 1
        return port


def _cluster_docker_name(cluster_id):
    """Stable Docker container name for a cluster's shared mongo container."""
    return f"ministack-docdb-cluster-{cluster_id}"


def _cluster_volume_name(cluster_id):
    """Stable Docker volume name for a cluster's persistent storage."""
    return f"ministack-docdb-cluster-{cluster_id}-data"


def _instance_volume_name(db_id):
    """Stable Docker volume name for a standalone instance's storage."""
    return f"ministack-docdb-{db_id}-data"


def _launch_mongo_container(name, image, env, host_port, container_port, data_path,
                            labels, volume_name=None):
    """Run one mongo container and derive its endpoint addresses.

    Returns:
        tuple: ``(container_id, internal_address, internal_port,
        endpoint_address, endpoint_port)``, or None when the run failed. On the
        MiniStack Docker network the endpoint resolves to the container IP and
        native port; otherwise to localhost and the published host port.
    """
    docker_client = _get_docker()
    if not docker_client:
        return None
    ms_network = _get_ministack_network(docker_client)
    kwargs = dict(
        image=image,
        detach=True,
        environment=env,
        ports={f"{container_port}/tcp": host_port},
        name=name,
        labels=labels,
    )
    if ms_network:
        kwargs["network"] = ms_network
    if volume_name:
        kwargs["volumes"] = {volume_name: {"bind": data_path, "mode": "rw"}}
    else:
        kwargs["tmpfs"] = {data_path: f"rw,noexec,nosuid,size={DOCDB_TMPFS_SIZE}"}
    try:
        container = docker_client.containers.run(**kwargs)
    except Exception as e:
        logger.warning("docdb: failed to start container %s: %s", name, e)
        return None
    endpoint_addr, endpoint_port = "localhost", host_port
    internal_addr, internal_port = None, None
    if ms_network:
        try:
            container.reload()
            networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
            ip = networks.get(ms_network, {}).get("IPAddress", "")
            if ip:
                endpoint_addr, endpoint_port = ip, container_port
                internal_addr, internal_port = ip, container_port
        except Exception:
            pass
    return container.id, internal_addr, internal_port, endpoint_addr, endpoint_port


def _remove_stale_owned_container(docker_client, name):
    """Remove a leftover same-name container only when our labels prove ownership."""
    try:
        stale = docker_client.containers.get(name)
    except Exception:
        return
    labels = getattr(stale, "labels", None) or {}
    if labels.get("ministack") == "documentdb":
        try:
            stale.remove(force=True)
        except Exception as e:
            logger.warning("docdb: failed to remove stale container %s: %s", name, e)


# ---------------------------------------------------------------------------
# Cluster shared container lifecycle
# ---------------------------------------------------------------------------

def _start_cluster_shared_container(cluster_id, cluster, remove_stale=False):
    """Start (or recreate) the single mongo container owned by a DocDB cluster.

    Cluster members are control-plane records that all point at this
    container's endpoint. Shared by first-member creation and warm boot.

    Args:
        cluster_id: Cluster identifier; also names the container and volume.
        cluster: Cluster record; updated in place with ``_shared_*`` fields.
        remove_stale: Remove an existing same-name container first.

    Returns:
        dict: ``started`` / ``failed`` flags plus readiness host/port.
    """
    engine_version = cluster.get("EngineVersion") or DEFAULT_ENGINE_VERSION
    master_user = cluster.get("MasterUsername", "root")
    master_pass = cluster.get("_MasterUserPassword", "password")
    db_name = cluster.get("DatabaseName") or "admin"

    def _fallback_endpoint():
        return {
            "Address": "localhost",
            "Port": int(cluster.get("Port") or 27017),
            "HostedZoneId": cluster.get("HostedZoneId", "Z2R2ITUGPM61AM"),
        }

    cluster.update({
        "_shared_container_id": None,
        "_shared_endpoint": _fallback_endpoint(),
        "_shared_internal_address": None,
        "_shared_internal_port": None,
        "_shared_container_ready": True,
    })

    docker_client = _get_docker()
    if not docker_client:
        return {"started": False, "failed": False, "readiness_host": None, "readiness_port": None}

    image, env, container_port, data_path = _docker_image_for_docdb(
        engine_version, master_user, master_pass, db_name,
    )
    container_name = _cluster_docker_name(cluster_id)
    if remove_stale:
        _remove_stale_owned_container(docker_client, container_name)

    host_port = cluster.get("_shared_host_port") or _next_port()
    if not _is_host_port_free(host_port):
        logger.info(
            "docdb: persisted shared host port %d for cluster %s is in use; allocating a fresh port",
            host_port, cluster_id,
        )
        host_port = _next_port()

    volume_name = None
    if DOCDB_PERSIST:
        volume_name = cluster.get("_shared_volume_name") or _cluster_volume_name(cluster_id)
        cluster["_shared_volume_name"] = volume_name
        cluster["_shared_storage_initialized"] = True

    started = _launch_mongo_container(
        container_name,
        image, env, host_port, container_port, data_path,
        labels={
            **container_reaper.own_labels("documentdb"),
            "cluster_id": cluster_id,
            "account_id": get_account_id(),
            "region": get_region(),
        },
        volume_name=volume_name,
    )
    if not started:
        cluster["_shared_container_ready"] = False
        logger.warning("docdb: failed to start shared container for cluster %s", cluster_id)
        return {"started": False, "failed": True, "readiness_host": None, "readiness_port": None}

    container_id, internal_addr, internal_port, ep_addr, ep_port = started
    epoch = int(cluster.get("_shared_container_epoch", 0)) + 1
    cluster.update({
        "_shared_container_id": container_id,
        "_shared_host_port": host_port,
        "_shared_endpoint": {
            "Address": ep_addr,
            "Port": ep_port,
            "HostedZoneId": cluster.get("HostedZoneId", "Z2R2ITUGPM61AM"),
        },
        "_shared_internal_address": internal_addr,
        "_shared_internal_port": internal_port,
        "_shared_container_ready": False,
        "_shared_container_epoch": epoch,
    })
    _sync_cluster_endpoints(cluster)
    readiness_host = internal_addr or "127.0.0.1"
    readiness_port = internal_port or host_port
    return {
        "started": True,
        "failed": False,
        "readiness_host": readiness_host,
        "readiness_port": readiness_port,
    }


def _restart_cluster_shared_container(cluster_id, cluster):
    """Start a preserved (stopped) shared container without recreating it."""
    docker_client = _get_docker()
    container_id = cluster.get("_shared_container_id")
    if not docker_client or not container_id:
        return {"started": False, "failed": False, "readiness_host": None, "readiness_port": None}
    try:
        container = docker_client.containers.get(container_id)
        container.start()
        container.reload()
    except Exception as e:
        cluster["_shared_container_ready"] = False
        logger.warning("docdb: failed to restart shared container for cluster %s: %s", cluster_id, e)
        return {"started": False, "failed": True, "readiness_host": None, "readiness_port": None}

    ms_network = _get_ministack_network(docker_client)
    container_port = int(cluster.get("_shared_internal_port") or 27017)
    host_port = int(
        cluster.get("_shared_host_port")
        or (cluster.get("_shared_endpoint") or {}).get("Port")
        or container_port
    )
    endpoint_addr, endpoint_port = "localhost", host_port
    internal_addr, internal_port = None, None
    if ms_network:
        networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
        ip = networks.get(ms_network, {}).get("IPAddress", "")
        if ip:
            endpoint_addr, endpoint_port = ip, container_port
            internal_addr, internal_port = ip, container_port
    epoch = int(cluster.get("_shared_container_epoch", 0)) + 1
    cluster.update({
        "_shared_endpoint": {
            "Address": endpoint_addr,
            "Port": endpoint_port,
            "HostedZoneId": cluster.get("HostedZoneId", "Z2R2ITUGPM61AM"),
        },
        "_shared_internal_address": internal_addr,
        "_shared_internal_port": internal_port,
        "_shared_container_ready": False,
        "_shared_container_epoch": epoch,
    })
    _sync_cluster_endpoints(cluster)
    logger.info("docdb: restarted shared container for cluster %s", cluster_id)
    return {
        "started": True,
        "failed": False,
        "readiness_host": internal_addr or "127.0.0.1",
        "readiness_port": internal_port or host_port,
    }


def _stop_cluster_shared_container(cluster_id, cluster):
    """Stop a cluster's shared mongo container, preserving it and its volume."""
    docker_client = _get_docker()
    container_id = cluster.get("_shared_container_id")
    if not docker_client or not container_id:
        return True
    try:
        container = docker_client.containers.get(container_id)
        container.reload()
        if container.status not in ("created", "exited", "dead", "removing"):
            container.stop(timeout=5)
            logger.info("docdb: stopped container for cluster %s", cluster_id)
        return True
    except Exception as e:
        logger.warning("docdb: failed to stop container for cluster %s: %s", cluster_id, e)
        return False


def _remove_cluster_shared_resources(cluster_id, cluster, timeout=5):
    """Stop and remove a cluster's shared container and its named volume.

    Called by ``DeleteDBCluster`` and ``reset``; the last place cluster-owned
    storage can be reclaimed.
    """
    docker_client = _get_docker()
    if not docker_client:
        return
    for identifier in [cluster.get("_shared_container_id"), _cluster_docker_name(cluster_id)]:
        if not identifier:
            continue
        try:
            container = docker_client.containers.get(identifier)
            container.stop(timeout=timeout)
            container.remove(v=True)
            logger.info("docdb: removed shared container for cluster %s", cluster_id)
            break
        except Exception:
            continue
    volume_name = cluster.get("_shared_volume_name") or _cluster_volume_name(cluster_id)
    try:
        docker_client.volumes.get(volume_name).remove()
    except Exception as e:
        logger.debug("docdb: no volume to remove for cluster %s: %s", cluster_id, e)


def _attach_instance_to_shared_cluster(instance, cluster):
    """Point a member instance's endpoint at the cluster's shared container."""
    endpoint = cluster.get("_shared_endpoint")
    if not endpoint:
        return
    instance["Endpoint"] = copy.deepcopy(endpoint)
    instance["_host_port"] = cluster.get("_shared_host_port")
    instance["_internal_address"] = cluster.get("_shared_internal_address")
    instance["_internal_port"] = cluster.get("_shared_internal_port")
    instance["_shared_cluster_id"] = cluster["DBClusterIdentifier"]
    instance["MasterUsername"] = cluster.get("MasterUsername", instance.get("MasterUsername", "root"))
    instance["_MasterUserPassword"] = cluster.get(
        "_MasterUserPassword", instance.get("_MasterUserPassword", "password"),
    )


def _sync_cluster_endpoints(cluster):
    """Publish the shared container's endpoint as the cluster Endpoint/Port."""
    endpoint = cluster.get("_shared_endpoint")
    if not endpoint:
        return
    cluster["Endpoint"] = endpoint.get("Address", cluster.get("Endpoint", ""))
    cluster["Port"] = int(endpoint.get("Port", cluster.get("Port", 0)))


def _register_instance_in_cluster(instance):
    """Append the instance to its parent cluster's ``DBClusterMembers``.

    The first member becomes the writer; subsequent members register as
    readers with their ``PromotionTier``.
    """
    cid = instance.get("DBClusterIdentifier")
    if not cid:
        return
    cluster = _clusters.get(cid)
    if not cluster:
        return
    members = cluster.setdefault("DBClusterMembers", [])
    db_id = instance["DBInstanceIdentifier"]
    members[:] = [m for m in members if m.get("DBInstanceIdentifier") != db_id]
    any_writer = any(m.get("IsClusterWriter") for m in members)
    members.append({
        "DBInstanceIdentifier": db_id,
        "IsClusterWriter": not any_writer,
        "PromotionTier": int(instance.get("PromotionTier", 1)),
    })


def _unregister_instance_from_clusters(db_id):
    """Remove an instance from any cluster member list it belongs to."""
    for cluster in _clusters.values():
        mem = cluster.get("DBClusterMembers") or []
        remaining = [m for m in mem if m.get("DBInstanceIdentifier") != db_id]
        if len(remaining) == len(mem):
            continue
        cluster["DBClusterMembers"] = remaining
        if not remaining:
            # Last member removed: take the shared container down but keep its
            # named volume, so a future member restarts onto preserved data.
            # Only DeleteDBCluster removes the storage entirely.
            cluster["_shared_storage_initialized"] = bool(
                cluster.get("_shared_volume_name") or cluster.get("_shared_storage_initialized")
            )
            if _stop_cluster_shared_container(cluster["DBClusterIdentifier"], cluster):
                _teardown_cluster_compute(cluster)
        writer_gone = not any(m.get("IsClusterWriter") for m in remaining)
        if writer_gone and remaining:
            promoted = sorted(remaining, key=lambda m: int(m.get("PromotionTier", 1)))[0]
            promoted["IsClusterWriter"] = True


def _teardown_cluster_compute(cluster):
    """Clear a cluster's live-compute fields after its container went away."""
    cluster["_shared_container_id"] = None
    cluster["_shared_container_ready"] = True
    cluster["_shared_endpoint"] = None
    cluster["_shared_internal_address"] = None
    cluster["_shared_internal_port"] = None


# ---------------------------------------------------------------------------
# Engine versions & Docker images
# ---------------------------------------------------------------------------

def _default_engine_version(engine):
    """Return the default DocumentDB engine version (5.0.0, per AWS SDK)."""
    return DEFAULT_ENGINE_VERSION


def _engine_version_error(engine_version):
    """Build an InvalidParameterCombination error for unsupported versions.

    Returns:
        tuple | None: Error response, or None when the version is cataloged.
    """
    if engine_version in _DOCDB_ENGINE_VERSION_SET:
        return None
    supported = ", ".join(sorted(_DOCDB_ENGINE_VERSION_SET))
    return _error(
        "InvalidParameterCombination",
        f"The engine version {engine_version} is not supported for docdb. "
        f"Supported engine versions: {supported}.",
        400,
    )


def _docker_image_for_docdb(engine_version, user, password, db_name=""):
    """Map a DocumentDB engine version to its wire-compatible mongo image.

    DocDB 5.0 ↔ mongo:5.0 and DocDB 8.0 ↔ mongo:8.0 are wire-compatible with
    their Mongo majors; unknown majors fall back to mongo:5.0 with a warning.
    Honors MINISTACK_IMAGE_PREFIX via :func:`apply_image_prefix`.

    Args:
        engine_version: Requested DocumentDB engine version string.
        user: Root username injected via MONGO_INITDB_ROOT_USERNAME.
        password: Root password injected via MONGO_INITDB_ROOT_PASSWORD.
        db_name: Ignored; clients ``use <db>`` after connecting.

    Returns:
        tuple: ``(image, env_dict, container_port, data_path)``.
    """
    major = str(engine_version or "").split(".")[0]
    images = {"5": "mongo:5.0", "8": "mongo:8.0"}
    image = images.get(major)
    if image is None:
        logger.warning(
            "docdb: unsupported engine version %s; falling back to mongo:5.0",
            engine_version,
        )
        image = "mongo:5.0"
    env = {
        "MONGO_INITDB_ROOT_USERNAME": user,
        "MONGO_INITDB_ROOT_PASSWORD": password,
    }
    return apply_image_prefix(image), env, 27017, "/data/db"


# ---------------------------------------------------------------------------
# Request routing
# ---------------------------------------------------------------------------

def _json_key_to_query_param_name(key):
    """Map JSON / Smithy body keys to Query-API parameter names."""
    lk = key.lower()
    if lk == "dbinstanceidentifier":
        return "DBInstanceIdentifier"
    if lk == "dbclusteridentifier":
        return "DBClusterIdentifier"
    if lk == "filters":
        return "Filters"
    return key


def _flatten_json_request_params(params, data):
    """Merge SigV4 JSON (``application/x-amz-json-1.*``) bodies into query-style params."""
    if not isinstance(data, dict):
        return
    for key, val in data.items():
        if val is None:
            continue
        qkey = _json_key_to_query_param_name(key)
        if isinstance(val, bool):
            params[qkey] = ["true" if val else "false"]
        elif isinstance(val, (int, float)):
            params[qkey] = [str(val)]
        elif isinstance(val, str):
            params[qkey] = [val]
        elif isinstance(val, list) and qkey == "Filters":
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


def _parse_request_params(body, headers, query_params):
    """Merge query-string, form-encoded, and JSON-body parameters into one map."""
    params = dict(query_params)
    if not body:
        return params
    raw = body if isinstance(body, str) else body.decode("utf-8-sig", errors="replace")
    stripped = raw.lstrip()
    ct = (headers.get("content-type") or headers.get("Content-Type") or "").lower()
    merged_json = False
    if stripped.startswith("{") or ("json" in ct and stripped):
        try:
            payload = json.loads(stripped)
            if isinstance(payload, dict):
                _flatten_json_request_params(params, payload)
                merged_json = True
        except json.JSONDecodeError:
            pass
    if not merged_json:
        for k, v in parse_qs(raw).items():
            params[k] = v
    return params


async def handle_request(method, path, headers, body, query_params):
    """Dispatch a DocumentDB request off the event loop.

    Handler paths reach the Docker daemon (container create/start/stop), which
    blocks for as long as the daemon takes; like rds.py this must not hold the
    event loop. The containers started here never call back into MiniStack, so
    the shared non-reentrant pool is safe.

    Args:
        method: HTTP method.
        path: Request path (ignored; Query API lives on POST /).
        headers: Lower-cased request headers.
        body: Raw request body bytes.
        query_params: Parsed query string ({name: [values]}).

    Returns:
        tuple: ``(status, headers, body)`` XML response.
    """
    params = _parse_request_params(body, headers, query_params)
    return await run_offloop(_handle_request_sync, headers, params)


def _handle_request_sync(headers, params):
    """Resolve the requested action and invoke its handler synchronously."""
    target = headers.get("x-amz-target", "") or headers.get("X-Amz-Target", "")
    if target:
        action = target.split(".")[-1]
    else:
        action = _evaluate_params(params, "Action")
    handler = _ACTION_MAP.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown DocumentDB action: {action}", 400)
    return handler(params)


# ---------------------------------------------------------------------------
# DB Instances
# ---------------------------------------------------------------------------

def _create_db_instance(params):
    """Create a DB instance, optionally as a member of a DB cluster.

    Members alias the parent cluster's shared mongo container; the first
    member starts it. Instances created without ``DBClusterIdentifier`` (raw
    HTTP callers only — the boto3 client requires a cluster) run their own
    per-instance container.

    Args:
        params: Merged query/form/JSON request parameters.

    Returns:
        tuple: XML CreateDBInstanceResponse with the instance record.

    Raises:
        DBInstanceAlreadyExistsFault: Same-name instance exists (400).
        DBClusterNotFoundFault: Named parent cluster missing (404).
        InvalidParameterCombination: Unsupported EngineVersion (400).
    """
    db_id = _evaluate_params(params, "DBInstanceIdentifier")
    if not db_id:
        return _error("MissingParameter", "DBInstanceIdentifier is required", 400)
    if db_id in _instances:
        return _error("DBInstanceAlreadyExistsFault", f"DB instance {db_id} already exists", 400)

    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id) if cluster_id else None
    if cluster_id and not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    engine = "docdb"
    if cluster:
        # Members inherit their parent cluster's engine version, as on AWS.
        engine_version = _evaluate_params(params, "EngineVersion") or cluster.get(
            "EngineVersion"
        ) or _default_engine_version(engine)
    else:
        engine_version = _evaluate_params(params, "EngineVersion") or _default_engine_version(engine)
    version_error = _engine_version_error(engine_version)
    if version_error:
        return version_error

    db_class = _evaluate_params(params, "DBInstanceClass") or "db.t3.medium"
    master_user = _evaluate_params(params, "MasterUsername") or "root"
    master_pass = _evaluate_params(params, "MasterUserPassword") or "password"
    db_name = _evaluate_params(params, "DBName") or "admin"
    port = int(_evaluate_params(params, "Port") or "27017")

    if cluster:
        if not _evaluate_params(params, "MasterUsername"):
            master_user = cluster.get("MasterUsername", master_user)
        if not _evaluate_params(params, "MasterUserPassword"):
            master_pass = cluster.get("_MasterUserPassword", master_pass)
    allocated_storage = int(_evaluate_params(params, "AllocatedStorage") or "20")
    storage_type = _evaluate_params(params, "StorageType") or "gp2"
    subnet_group_name = _evaluate_params(params, "DBSubnetGroupName") or "default"
    now_ts = time.time()
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:db:{db_id}"
    dbi_resource_id = f"db-{new_uuid().replace('-', '')[:20].upper()}"

    # Deletion protection is cluster-level on real DocumentDB; instances
    # inherit it and cannot opt out individually.
    deletion_protection = _evaluate_params(params, "DeletionProtection") == "true"
    if cluster:
        deletion_protection = deletion_protection or bool(cluster.get("DeletionProtection"))

    instance = {
        "DBInstanceIdentifier": db_id,
        "DBInstanceClass": db_class,
        "Engine": engine,
        "EngineVersion": engine_version,
        "DBInstanceStatus": "available",
        "MasterUsername": master_user,
        "DBName": db_name,
        "Endpoint": {
            "Address": "localhost",
            "Port": port,
            "HostedZoneId": "Z2R2ITUGPM61AM",
        },
        "AllocatedStorage": allocated_storage,
        "InstanceCreateTime": _format_time(now_ts),
        "PreferredBackupWindow": "03:00-04:00",
        "BackupRetentionPeriod": int(_evaluate_params(params, "BackupRetentionPeriod") or "1"),
        "DBSecurityGroups": [],
        "VpcSecurityGroups": [
            {"VpcSecurityGroupId": sg, "Status": "active"}
            for sg in _parse_member_list(params, "VpcSecurityGroupIds")
        ],
        "DBParameterGroups": [{
            "DBParameterGroupName": f"default.docdb{str(engine_version).split('.')[0]}",
            "ParameterApplyStatus": "in-sync",
        }],
        "AvailabilityZone": _evaluate_params(params, "AvailabilityZone") or f"{get_region()}a",
        "DBSubnetGroup": _subnet_groups.get(subnet_group_name, {
            "DBSubnetGroupName": subnet_group_name,
            "DBSubnetGroupDescription": "default",
            "SubnetGroupStatus": "Complete",
            "Subnets": [],
            "VpcId": "vpc-00000000",
            "DBSubnetGroupArn": f"arn:aws:rds:{get_region()}:{get_account_id()}:subgrp:{subnet_group_name}",
        }),
        "PreferredMaintenanceWindow": _evaluate_params(params, "PreferredMaintenanceWindow") or "sun:05:00-sun:06:00",
        "PendingModifiedValues": {},
        "LatestRestorableTime": _format_time(now_ts),
        "MultiAZ": _evaluate_params(params, "MultiAZ") == "true",
        "AutoMinorVersionUpgrade": _evaluate_params(params, "AutoMinorVersionUpgrade") != "false",
        "ReadReplicaDBInstanceIdentifiers": [],
        "ReadReplicaSourceDBInstanceIdentifier": "",
        "ReadReplicaDBClusterIdentifiers": [],
        "ReplicaMode": "",
        "LicenseModel": "docdb",
        "Iops": int(_evaluate_params(params, "Iops") or "0") if _evaluate_params(params, "Iops") else None,
        "OptionGroupMemberships": [],
        "CharacterSetName": "",
        "NcharCharacterSetName": "",
        "SecondaryAvailabilityZone": "",
        "PubliclyAccessible": _evaluate_params(params, "PubliclyAccessible") == "true",
        "StatusInfos": [],
        "StorageType": storage_type,
        "TdeCredentialArn": "",
        "DbInstancePort": 0,
        "DBClusterIdentifier": cluster_id,
        "StorageEncrypted": _evaluate_params(params, "StorageEncrypted") == "true",
        "KmsKeyId": _evaluate_params(params, "KmsKeyId") or "",
        "DbiResourceId": dbi_resource_id,
        "CACertificateIdentifier": "rds-ca-rsa2048-g1",
        "DomainMemberships": [],
        "CopyTagsToSnapshot": _evaluate_params(params, "CopyTagsToSnapshot") == "true",
        "MonitoringInterval": int(_evaluate_params(params, "MonitoringInterval") or "0"),
        "EnhancedMonitoringResourceArn": "",
        "MonitoringRoleArn": _evaluate_params(params, "MonitoringRoleArn") or "",
        "PromotionTier": int(_evaluate_params(params, "PromotionTier") or "1"),
        "DBInstanceArn": arn,
        "Timezone": "",
        "IAMDatabaseAuthenticationEnabled": _evaluate_params(params, "EnableIAMDatabaseAuthentication") == "true",
        "PerformanceInsightsEnabled": False,
        "PerformanceInsightsKMSKeyId": "",
        "PerformanceInsightsRetentionPeriod": 7,
        "EnabledCloudwatchLogsExports": [],
        "ProcessorFeatures": [],
        "DeletionProtection": deletion_protection,
        "AssociatedRoles": [],
        "MaxAllocatedStorage": int(_evaluate_params(params, "MaxAllocatedStorage") or str(allocated_storage)),
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
        "_docker_container_id": None,
        "_internal_address": None,
        "_internal_port": None,
        "_host_port": None,
        "_MasterUserPassword": master_pass,
    }
    _instances[db_id] = instance

    req_tags = _parse_tags(params)
    if req_tags:
        _tags[arn] = req_tags
        instance["TagList"] = req_tags

    if cluster:
        _ensure_cluster_compute(cluster)
        _attach_instance_to_shared_cluster(instance, cluster)
        _register_instance_in_cluster(instance)
        _log_readiness_async(
            cluster.get("_shared_internal_address") or "127.0.0.1",
            (cluster.get("_shared_endpoint") or {}).get("Port") or port,
            f"cluster {cluster['DBClusterIdentifier']}",
        )
    else:
        _start_instance_container(db_id, instance)

    return _single_instance_response("CreateDBInstanceResponse", "CreateDBInstanceResult", instance)


def _ensure_cluster_compute(cluster):
    """Guarantee a running shared container behind a cluster before use.

    Restarts the preserved container when present, otherwise starts a fresh
    one (removing any stale same-name leftover we own).
    """
    with _shared_container_lock:
        if cluster.get("_shared_container_ready") and cluster.get("_shared_container_id"):
            return
        if cluster.get("_shared_container_id"):
            result = _restart_cluster_shared_container(cluster["DBClusterIdentifier"], cluster)
            if result.get("started"):
                return
        _start_cluster_shared_container(cluster["DBClusterIdentifier"], cluster, remove_stale=True)


def _start_instance_container(db_id, instance):
    """Start the per-instance mongo container backing a standalone instance.

    No-op without Docker: the record keeps its placeholder localhost endpoint.
    """
    docker_client = _get_docker()
    if not docker_client:
        return
    engine_version = instance.get("EngineVersion") or DEFAULT_ENGINE_VERSION
    master_user = instance.get("MasterUsername", "root")
    master_pass = instance.get("_MasterUserPassword", "password")
    image, env, container_port, data_path = _docker_image_for_docdb(
        engine_version, master_user, master_pass, instance.get("DBName") or "admin",
    )
    host_port = _next_port()
    volume_name = _instance_volume_name(db_id) if DOCDB_PERSIST else None
    started = _launch_mongo_container(
        f"ministack-docdb-{db_id}",
        image, env, host_port, container_port, data_path,
        labels={
            **container_reaper.own_labels("documentdb"),
            "db_id": db_id,
            "account_id": get_account_id(),
            "region": get_region(),
        },
        volume_name=volume_name,
    )
    if not started:
        logger.warning("docdb: no container for instance %s; endpoint is a placeholder", db_id)
        return
    container_id, internal_addr, internal_port, ep_addr, ep_port = started
    instance.update({
        "_docker_container_id": container_id,
        "_internal_address": internal_addr,
        "_internal_port": internal_port,
        "_host_port": host_port,
        "Endpoint": {"Address": ep_addr, "Port": ep_port, "HostedZoneId": "Z2R2ITUGPM61AM"},
    })
    _log_readiness_async(internal_addr or "127.0.0.1", internal_port or host_port, f"instance {db_id}")


def _log_readiness_async(host, port, label):
    """Spawn a daemon thread that waits for the port and logs readiness."""
    def _bg_wait(h=host, p=int(port or 0), lbl=label):
        if not p:
            return
        if _wait_for_port(h, p):
            logger.info("docdb: mongo container for %s ready at %s:%s", lbl, h, p)
        else:
            logger.warning("docdb: mongo container for %s at %s:%s not ready after timeout", lbl, h, p)

    threading.Thread(target=_bg_wait, daemon=True).start()


def _delete_db_instance(params):
    """Delete a DB instance and release its backing compute.

    Member deletion unregisters the instance from its cluster; removing the
    final member stops the shared container but keeps its volume. Deletion
    protection is honored before any state changes.

    Args:
        params: Request parameters (DBInstanceIdentifier required).

    Returns:
        tuple: XML DeleteDBInstanceResponse with the deleted instance record.

    Raises:
        DBInstanceNotFound: Unknown identifier (404).
        InvalidParameterCombination: Deletion protection enabled (400).
    """
    db_id = _evaluate_params(params, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)

    # Check protection BEFORE mutating membership/state. Deletion protection is
    # cluster-level on real DocumentDB: members evaluate the parent cluster's
    # live flag (so disabling it frees existing instances); standalone
    # instances fall back to their own flag.
    cluster = _clusters.get(instance.get("DBClusterIdentifier"))
    if cluster is not None:
        protected = bool(cluster.get("DeletionProtection"))
    else:
        protected = bool(instance.get("DeletionProtection"))
    if protected:
        return _error(
            "InvalidParameterCombination",
            "Cannot delete protected DB instance. Deletion Protection is enabled.",
            400,
        )

    cluster_id = instance.get("DBClusterIdentifier")
    if cluster_id:
        was_last_member = _will_be_last_member(cluster_id, db_id)
        _unregister_instance_from_clusters(db_id)
        if was_last_member:
            _log_readiness_teardown(cluster_id)
    else:
        _remove_instance_container(db_id, instance)

    skip_snapshot = _evaluate_params(params, "SkipFinalSnapshot") == "true"
    final_snap_id = _evaluate_params(params, "FinalDBSnapshotIdentifier")
    if not skip_snapshot and final_snap_id:
        _create_snapshot_internal(final_snap_id, instance)

    arn = instance["DBInstanceArn"]
    _tags.pop(arn, None)
    del _instances[db_id]
    return _single_instance_response("DeleteDBInstanceResponse", "DeleteDBInstanceResult", instance)


def _will_be_last_member(cluster_id, db_id):
    """True when deleting db_id would leave its parent cluster with no members."""
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return False
    members = [m for m in cluster.get("DBClusterMembers", [])
               if m.get("DBInstanceIdentifier") != db_id]
    return not members


def _remove_instance_container(db_id, instance):
    """Stop and remove the container owned by a standalone instance."""
    container_id = instance.get("_docker_container_id")
    if not container_id:
        return
    docker_client = _get_docker()
    if not docker_client:
        return
    try:
        c = docker_client.containers.get(container_id)
        c.stop(timeout=5)
        c.remove(v=True)
        logger.info("docdb: removed container for %s", db_id)
    except Exception as e:
        logger.warning("docdb: failed to remove container for %s: %s", db_id, e)


def _log_readiness_teardown(cluster_id):
    """Log that the last member of a cluster went away (compute taken down)."""
    logger.info(
        "docdb: last member of cluster %s deleted; shared container stopped "
        "(volume retained until DeleteDBCluster)", cluster_id,
    )


def _describe_db_instances(params):
    """Describe DB instances, optionally filtered by identifier or Filters."""
    db_id = _evaluate_params(params, "DBInstanceIdentifier")
    if db_id:
        instance = _resolve_instance(db_id)
        if not instance:
            return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
        instances = [instance]
    else:
        instances = list(_instances.values())
        filters = _parse_filters(params)
        if filters:
            instances = _apply_instance_filters(instances, filters)

    members = "".join(f"<DBInstance>{_instance_xml(i)}</DBInstance>" for i in instances)
    return _xml(200, "DescribeDBInstancesResponse",
                f"<DescribeDBInstancesResult><DBInstances>{members}</DBInstances></DescribeDBInstancesResult>")


def _modify_db_instance(params):
    """Apply modifyable fields to a DB instance directly (no pending staging).

    Accepted: DBInstanceClass, AllocatedStorage, EngineVersion, MasterUserPassword,
    DeletionProtection, BackupRetentionPeriod, PreferredMaintenanceWindow,
    MultiAZ, AutoMinorVersionUpgrade, CopyTagsToSnapshot. ApplyImmediately is
    accepted for SDK compatibility; changes always apply immediately.

    Args:
        params: Request parameters (DBInstanceIdentifier required).

    Returns:
        tuple: XML ModifyDBInstanceResponse with the updated record.

    Raises:
        DBInstanceNotFound: Unknown identifier (404).
        InvalidParameterCombination: Unsupported EngineVersion (400).
    """
    db_id = _evaluate_params(params, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)

    new_version = _evaluate_params(params, "EngineVersion")
    if new_version:
        version_error = _engine_version_error(new_version)
        if version_error:
            return version_error
        instance["EngineVersion"] = new_version
        instance["DBParameterGroups"] = [{
            "DBParameterGroupName": f"default.docdb{str(new_version).split('.')[0]}",
            "ParameterApplyStatus": "in-sync",
        }]
    simple_fields = (
        "DBInstanceClass", "AllocatedStorage", "BackupRetentionPeriod",
        "PreferredMaintenanceWindow", "MultiAZ", "AutoMinorVersionUpgrade",
        "CopyTagsToSnapshot",
    )
    for field in simple_fields:
        value = _evaluate_params(params, field)
        if value:
            instance[field] = _coerce_scalar(field, value, instance.get(field))
    if _evaluate_params(params, "MasterUserPassword"):
        instance["_MasterUserPassword"] = _evaluate_params(params, "MasterUserPassword")
    if _evaluate_params(params, "DeletionProtection"):
        instance["DeletionProtection"] = _evaluate_params(params, "DeletionProtection") == "true"

    return _single_instance_response("ModifyDBInstanceResponse", "ModifyDBInstanceResult", instance)


def _coerce_scalar(field, value, current):
    """Cast a request string to the stored field's existing type when known."""
    if isinstance(current, bool):
        return str(value).lower() == "true"
    if isinstance(current, int) and not isinstance(current, bool):
        try:
            return int(value)
        except ValueError:
            return current
    return value


def _start_db_instance(params):
    """Mark a DB instance available (metadata-only; compute is always up)."""
    db_id = _evaluate_params(params, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "available"
    return _single_instance_response("StartDBInstanceResponse", "StartDBInstanceResult", instance)


def _stop_db_instance(params):
    """Mark a DB instance stopped (control-plane status only)."""
    db_id = _evaluate_params(params, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "stopped"
    return _single_instance_response("StopDBInstanceResponse", "StopDBInstanceResult", instance)


def _reboot_db_instance(params):
    """Reboot a DB instance (status returns to available immediately)."""
    db_id = _evaluate_params(params, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "available"
    return _single_instance_response("RebootDBInstanceResponse", "RebootDBInstanceResult", instance)


# ---------------------------------------------------------------------------
# DB Clusters
# ---------------------------------------------------------------------------

def _create_db_cluster(params):
    """Create a DB cluster record; compute starts with its first member.

    Args:
        params: Request parameters (DBClusterIdentifier required).

    Returns:
        tuple: XML CreateDBClusterResponse with the cluster record.

    Raises:
        DBClusterAlreadyExistsFault: Same-name cluster exists (400).
        InvalidParameterCombination: Unsupported EngineVersion (400).
    """
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    if not cluster_id:
        return _error("MissingParameter", "DBClusterIdentifier is required", 400)
    if cluster_id in _clusters:
        return _error("DBClusterAlreadyExistsFault", f"DB cluster {cluster_id} already exists.", 400)

    engine = "docdb"
    engine_version = _evaluate_params(params, "EngineVersion") or _default_engine_version(engine)
    version_error = _engine_version_error(engine_version)
    if version_error:
        return version_error

    port = int(_evaluate_params(params, "Port") or "27017")
    master_user = _evaluate_params(params, "MasterUsername") or "root"
    master_pass = _evaluate_params(params, "MasterUserPassword") or "password"
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:cluster:{cluster_id}"
    unique_suffix = new_uuid()[:8]
    now_ts = time.time()

    az_list = _parse_member_list(params, "AvailabilityZones")
    if not az_list:
        az_list = [f"{get_region()}a", f"{get_region()}b", f"{get_region()}c"]

    cluster = {
        "DBClusterIdentifier": cluster_id,
        "DBClusterArn": arn,
        "Engine": engine,
        "EngineVersion": engine_version,
        "EngineMode": _evaluate_params(params, "EngineMode") or "provisioned",
        "Status": "available",
        "MasterUsername": master_user,
        "_MasterUserPassword": master_pass,
        "DatabaseName": _evaluate_params(params, "DatabaseName") or None,
        "NetworkType": _evaluate_params(params, "NetworkType") or "IPV4",
        "EngineLifecycleSupport": _evaluate_params(params, "EngineLifecycleSupport") or "open-source-rds-extended-support",
        "Endpoint": f"{cluster_id}.cluster-{unique_suffix}.{get_region()}.docdb.amazonaws.com",
        "ReaderEndpoint": f"{cluster_id}.cluster-ro-{unique_suffix}.{get_region()}.docdb.amazonaws.com",
        "Port": port,
        "MultiAZ": _evaluate_params(params, "MultiAZ") == "true",
        "AvailabilityZones": az_list,
        "DBClusterMembers": [],
        "VpcSecurityGroups": [
            {"VpcSecurityGroupId": sg, "Status": "active"}
            for sg in _parse_member_list(params, "VpcSecurityGroupIds")
        ],
        "DBSubnetGroup": _evaluate_params(params, "DBSubnetGroupName") or "default",
        "DBClusterParameterGroup": _evaluate_params(params, "DBClusterParameterGroupName") or "default.docdb",
        "BackupRetentionPeriod": int(_evaluate_params(params, "BackupRetentionPeriod") or "1"),
        "PreferredBackupWindow": _evaluate_params(params, "PreferredBackupWindow") or "03:00-04:00",
        "PreferredMaintenanceWindow": _evaluate_params(params, "PreferredMaintenanceWindow") or "sun:05:00-sun:06:00",
        "ClusterCreateTime": _format_time(now_ts),
        "EarliestRestorableTime": _format_time(now_ts),
        "LatestRestorableTime": _format_time(now_ts),
        "StorageEncrypted": _evaluate_params(params, "StorageEncrypted") == "true",
        "KmsKeyId": _evaluate_params(params, "KmsKeyId") or "",
        "DeletionProtection": _evaluate_params(params, "DeletionProtection") == "true",
        "IAMDatabaseAuthenticationEnabled": _evaluate_params(params, "EnableIAMDatabaseAuthentication") == "true",
        "EnabledCloudwatchLogsExports": [],
        "HttpEndpointEnabled": _evaluate_params(params, "EnableHttpEndpoint") == "true",
        "CopyTagsToSnapshot": _evaluate_params(params, "CopyTagsToSnapshot") == "true",
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
        "_shared_endpoint": None,
        "_shared_host_port": None,
        "_shared_volume_name": _cluster_volume_name(cluster_id) if DOCDB_PERSIST else None,
        "_shared_storage_initialized": False,
        "_shared_container_epoch": 0,
    }
    _clusters[cluster_id] = cluster

    req_tags = _parse_tags(params)
    if req_tags:
        _tags[arn] = req_tags
        cluster["TagList"] = req_tags

    return _xml(200, "CreateDBClusterResponse",
                f"<CreateDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></CreateDBClusterResult>")


def _delete_db_cluster(params):
    """Delete a DB cluster along with its shared container and volume.

    Refuses while instances remain attached, mirroring AWS ordering (delete
    the members first); an empty cluster's compute and storage are always
    removed.

    Args:
        params: Request parameters (DBClusterIdentifier required).

    Returns:
        tuple: XML DeleteDBClusterResponse with the deleted cluster record.

    Raises:
        DBClusterNotFoundFault: Unknown cluster (404).
        InvalidDBClusterStateFault: Cluster still has members (400).
        InvalidParameterCombination: Deletion protection enabled (400).
    """
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    if cluster.get("DBClusterMembers"):
        return _error(
            "InvalidDBClusterStateFault",
            f"Cannot delete DB cluster {cluster_id} because it still contains DB instances. "
            "Delete the instances first.",
            400,
        )
    if cluster.get("DeletionProtection"):
        return _error(
            "InvalidParameterCombination",
            "Cannot delete a DB cluster when DeletionProtection is enabled.",
            400,
        )

    cluster["Status"] = "deleting"
    _remove_cluster_shared_resources(cluster_id, cluster)
    _tags.pop(cluster["DBClusterArn"], None)
    del _clusters[cluster_id]
    return _xml(200, "DeleteDBClusterResponse",
                f"<DeleteDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></DeleteDBClusterResult>")


def _describe_db_clusters(params):
    """Describe DB clusters, optionally filtered by identifier or Filters."""
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    if cluster_id:
        cluster = _clusters.get(cluster_id)
        if not cluster:
            return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
        clusters = [cluster]
    else:
        clusters = list(_clusters.values())
        filters = _parse_filters(params)
        if filters:
            clusters = _apply_cluster_filters(clusters, filters)

    members = "".join(f"<DBCluster>{_cluster_xml(c)}</DBCluster>" for c in clusters)
    return _xml(200, "DescribeDBClustersResponse",
                f"<DescribeDBClustersResult><DBClusters>{members}</DBClusters></DescribeDBClustersResult>")


def _modify_db_cluster(params):
    """Modify cluster settings; MasterUserPassword also rotates container creds."""
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    if _evaluate_params(params, "EngineVersion"):
        cluster["EngineVersion"] = _evaluate_params(params, "EngineVersion")
    if _evaluate_params(params, "MasterUserPassword"):
        cluster["_MasterUserPassword"] = _evaluate_params(params, "MasterUserPassword")
    if _evaluate_params(params, "Port"):
        cluster["Port"] = int(_evaluate_params(params, "Port"))
    if _evaluate_params(params, "BackupRetentionPeriod"):
        cluster["BackupRetentionPeriod"] = int(_evaluate_params(params, "BackupRetentionPeriod"))
    if _evaluate_params(params, "PreferredBackupWindow"):
        cluster["PreferredBackupWindow"] = _evaluate_params(params, "PreferredBackupWindow")
    if _evaluate_params(params, "PreferredMaintenanceWindow"):
        cluster["PreferredMaintenanceWindow"] = _evaluate_params(params, "PreferredMaintenanceWindow")
    if _evaluate_params(params, "DeletionProtection"):
        cluster["DeletionProtection"] = _evaluate_params(params, "DeletionProtection") == "true"
    if _evaluate_params(params, "EnableIAMDatabaseAuthentication"):
        cluster["IAMDatabaseAuthenticationEnabled"] = _evaluate_params(params, "EnableIAMDatabaseAuthentication") == "true"
    if _evaluate_params(params, "EnableHttpEndpoint"):
        cluster["HttpEndpointEnabled"] = _evaluate_params(params, "EnableHttpEndpoint") == "true"
    if _evaluate_params(params, "CopyTagsToSnapshot"):
        cluster["CopyTagsToSnapshot"] = _evaluate_params(params, "CopyTagsToSnapshot") == "true"

    vpc_sgs = _parse_member_list(params, "VpcSecurityGroupIds")
    if vpc_sgs:
        cluster["VpcSecurityGroups"] = [
            {"VpcSecurityGroupId": sg, "Status": "active"} for sg in vpc_sgs
        ]

    return _xml(200, "ModifyDBClusterResponse",
                f"<ModifyDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></ModifyDBClusterResult>")


def _start_db_cluster(params):
    """Start a stopped cluster's compute (restart/recreate the container)."""
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    if cluster.get("Status") != "stopped":
        return _error(
            "InvalidDBClusterStateFault",
            f"DbCluster {cluster_id} is in {cluster.get('Status')} state but expected it to be one of stopped.",
            400,
        )

    members = _cluster_member_instances(cluster)
    had_compute = bool(cluster.get("_shared_container_id"))
    has_storage = bool(cluster.get("_shared_storage_initialized") or cluster.get("_shared_volume_name"))
    docker_client = _get_docker()

    if not docker_client or not (had_compute or has_storage):
        # Control-plane-only, or the cluster never had real compute.
        cluster["_shared_container_ready"] = True
        cluster["Status"] = "available"
        for member in members:
            member["DBInstanceStatus"] = "available"
        return _xml(200, "StartDBClusterResponse",
                    f"<StartDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StartDBClusterResult>")

    if had_compute:
        result = _restart_cluster_shared_container(cluster_id, cluster)
        if result.get("failed"):
            result = _start_cluster_shared_container(cluster_id, cluster, remove_stale=True)
    else:
        result = _start_cluster_shared_container(cluster_id, cluster, remove_stale=True)

    if not result.get("started") and result.get("failed"):
        # Compute did not come back; keep everything stopped so Start can retry.
        return _error("InternalFailure", f"Failed to start compute for DB cluster {cluster_id}.", 500)

    if result.get("started"):
        readiness_host = result.get("readiness_host") or "127.0.0.1"
        readiness_port = result.get("readiness_port")
        ok = _wait_for_port(readiness_host, readiness_port) if readiness_port else True
        status = "available" if ok else "failed"
    else:
        status = "available"
    cluster["_shared_container_ready"] = status == "available"
    cluster["Status"] = status
    for member in members:
        member["DBInstanceStatus"] = status
    return _xml(200, "StartDBClusterResponse",
                f"<StartDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StartDBClusterResult>")


def _stop_db_cluster(params):
    """Stop a cluster's shared mongo container, preserving it and its volume."""
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    if cluster.get("Status") != "available":
        return _error(
            "InvalidDBClusterStateFault",
            f"DbCluster {cluster_id} is in {cluster.get('Status')} state but expected it to be one of available.",
            400,
        )
    _stop_cluster_shared_container(cluster_id, cluster)
    cluster["_shared_container_ready"] = False
    cluster["Status"] = "stopped"
    for member in _cluster_member_instances(cluster):
        member["DBInstanceStatus"] = "stopped"
    return _xml(200, "StopDBClusterResponse",
                f"<StopDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StopDBClusterResult>")


def _failover_db_cluster(params):
    """Rotate IsClusterWriter to the next member (lowest PromotionTier).

    Endpoints stay unchanged, matching real DocDB where cluster endpoints are
    stable across failover. Zero-member clusters succeed as a no-op.

    Args:
        params: Request parameters; TargetDBInstanceIdentifier optional.

    Returns:
        tuple: XML FailoverDBClusterResponse reporting transitional status
        ``failing-over`` (the stored status stays ``available``).

    Raises:
        DBClusterNotFoundFault: Unknown cluster (404).
        InvalidDBInstanceStateFault: Named target is not a reader member (400).
    """
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    members = cluster.get("DBClusterMembers", [])
    readers = [m for m in members if not m.get("IsClusterWriter")]

    target_id = _evaluate_params(params, "TargetDBInstanceIdentifier")
    if target_id:
        target = next((m for m in members if m.get("DBInstanceIdentifier") == target_id), None)
        if not target or target.get("IsClusterWriter"):
            return _error(
                "InvalidDBInstanceStateFault",
                f"DBInstance {target_id} is not a reader member of DB cluster {cluster_id}.",
                400,
            )
    elif readers:
        target = sorted(readers, key=lambda m: int(m.get("PromotionTier", 1)))[0]
        target_id = target["DBInstanceIdentifier"]
    else:
        # Zero-member (or single-writer-only) cluster: nothing to promote.
        target_id = None

    if target_id is not None:
        for member in members:
            member["IsClusterWriter"] = member.get("DBInstanceIdentifier") == target_id

    response_cluster = copy.deepcopy(cluster)
    response_cluster["Status"] = "failing-over"
    return _xml(200, "FailoverDBClusterResponse",
                f"<FailoverDBClusterResult><DBCluster>{_cluster_xml(response_cluster)}</DBCluster></FailoverDBClusterResult>")


def _restore_db_cluster_from_snapshot(params):
    """Restore a cluster from a cluster snapshot's recorded metadata.

    Snapshots are metadata-only (configuration + tags, no data dump), so the
    restored cluster comes up empty against fresh storage.

    Args:
        params: Request parameters (DBClusterIdentifier, Engine,
            SnapshotIdentifier required).

    Returns:
        tuple: XML RestoreDBClusterFromSnapshotResponse with the new cluster.

    Raises:
        DBClusterAlreadyExistsFault: Target name already used (400).
        DBClusterSnapshotNotFoundFault: Snapshot missing (404).
        InvalidParameterCombination: Unsupported EngineVersion (400).
    """
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    if not cluster_id:
        return _error("MissingParameter", "DBClusterIdentifier is required", 400)
    if cluster_id in _clusters:
        return _error("DBClusterAlreadyExistsFault", f"DB cluster {cluster_id} already exists.", 400)

    snap_id = _evaluate_params(params, "SnapshotIdentifier")
    snapshot = _db_cluster_snapshots.get(snap_id) if snap_id else None
    if not snapshot:
        return _error(
            "DBClusterSnapshotNotFoundFault",
            f"DBClusterSnapshot {snap_id} not found." if snap_id else "SnapshotIdentifier is required",
            404 if snap_id else 400,
        )

    engine_version = _evaluate_params(params, "EngineVersion") or snapshot.get("EngineVersion")
    version_error = _engine_version_error(engine_version)
    if version_error:
        return version_error

    source_cluster = _clusters.get(snapshot.get("DBClusterIdentifier"))
    master_pass = (
        source_cluster.get("_MasterUserPassword", "password")
        if source_cluster else "password"
    )
    now_ts = time.time()
    unique_suffix = new_uuid()[:8]
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:cluster:{cluster_id}"

    cluster = {
        "DBClusterIdentifier": cluster_id,
        "DBClusterArn": arn,
        "Engine": "docdb",
        "EngineVersion": engine_version,
        "EngineMode": "provisioned",
        "Status": "available",
        "MasterUsername": snapshot.get("MasterUsername", "root"),
        "_MasterUserPassword": master_pass,
        "DatabaseName": None,
        "NetworkType": _evaluate_params(params, "NetworkType") or "IPV4",
        "EngineLifecycleSupport": "open-source-rds-extended-support",
        "Endpoint": f"{cluster_id}.cluster-{unique_suffix}.{get_region()}.docdb.amazonaws.com",
        "ReaderEndpoint": f"{cluster_id}.cluster-ro-{unique_suffix}.{get_region()}.docdb.amazonaws.com",
        "Port": int(_evaluate_params(params, "Port") or snapshot.get("Port") or "27017"),
        "MultiAZ": False,
        "AvailabilityZones": _parse_member_list(params, "AvailabilityZones") or snapshot.get("AvailabilityZones", []),
        "DBClusterMembers": [],
        "VpcSecurityGroups": [
            {"VpcSecurityGroupId": sg, "Status": "active"}
            for sg in _parse_member_list(params, "VpcSecurityGroupIds")
        ],
        "DBSubnetGroup": _evaluate_params(params, "DBSubnetGroupName") or "default",
        "DBClusterParameterGroup": _evaluate_params(params, "DBClusterParameterGroupName") or "default.docdb",
        "BackupRetentionPeriod": 1,
        "PreferredBackupWindow": "03:00-04:00",
        "PreferredMaintenanceWindow": "sun:05:00-sun:06:00",
        "ClusterCreateTime": _format_time(now_ts),
        "EarliestRestorableTime": _format_time(now_ts),
        "LatestRestorableTime": _format_time(now_ts),
        "StorageEncrypted": snapshot.get("StorageEncrypted", False),
        "KmsKeyId": _evaluate_params(params, "KmsKeyId") or snapshot.get("KmsKeyId", ""),
        "DeletionProtection": _evaluate_params(params, "DeletionProtection") == "true",
        "IAMDatabaseAuthenticationEnabled": False,
        "EnabledCloudwatchLogsExports": [],
        "HttpEndpointEnabled": False,
        "CopyTagsToSnapshot": False,
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
        "_shared_endpoint": None,
        "_shared_host_port": None,
        "_shared_volume_name": _cluster_volume_name(cluster_id) if DOCDB_PERSIST else None,
        "_shared_storage_initialized": False,
        "_shared_container_epoch": 0,
    }
    _clusters[cluster_id] = cluster

    req_tags = _parse_tags(params)
    if req_tags:
        _tags[arn] = req_tags
        cluster["TagList"] = req_tags

    return _xml(200, "RestoreDBClusterFromSnapshotResponse",
                f"<RestoreDBClusterFromSnapshotResult><DBCluster>{_cluster_xml(cluster)}</DBCluster>"
                f"</RestoreDBClusterFromSnapshotResult>")


# ---------------------------------------------------------------------------
# Instance snapshots (legacy direct-HTTP surface; not in the real DocDB API)
# ---------------------------------------------------------------------------

def _create_snapshot_internal(snap_id, instance):
    """Record an instance snapshot copying the instance's configuration."""
    if snap_id in _snapshots:
        return None
    now_ts = time.time()
    snap = {
        "DBSnapshotIdentifier": snap_id,
        "DBInstanceIdentifier": instance["DBInstanceIdentifier"],
        "DBSnapshotArn": f"arn:aws:rds:{get_region()}:{get_account_id()}:snapshot:{snap_id}",
        "Engine": instance.get("Engine", "docdb"),
        "EngineVersion": instance.get("EngineVersion", ""),
        "SnapshotCreateTime": _format_time(now_ts),
        "InstanceCreateTime": instance.get("InstanceCreateTime", ""),
        "Status": "available",
        "AllocatedStorage": instance.get("AllocatedStorage", 20),
        "AvailabilityZone": instance.get("AvailabilityZone", f"{get_region()}a"),
        "VpcId": "vpc-00000000",
        "Port": (instance.get("Endpoint") or {}).get("Port", 27017),
        "MasterUsername": instance.get("MasterUsername", "root"),
        "DBName": instance.get("DBName", ""),
        "SnapshotType": "manual",
        "LicenseModel": "docdb",
        "StorageType": instance.get("StorageType", "gp2"),
        "DBInstanceClass": instance.get("DBInstanceClass", ""),
        "StorageEncrypted": instance.get("StorageEncrypted", False),
        "KmsKeyId": instance.get("KmsKeyId", ""),
        "Encrypted": instance.get("StorageEncrypted", False),
        "IAMDatabaseAuthenticationEnabled": instance.get("IAMDatabaseAuthenticationEnabled", False),
        "PercentProgress": 100,
        "DbiResourceId": instance.get("DbiResourceId", ""),
        "TagList": list(instance.get("TagList", [])) if instance.get("CopyTagsToSnapshot") else [],
        "SnapshotTarget": "region",
        "_master_password": instance.get("_MasterUserPassword", "password"),
    }
    _snapshots[snap_id] = snap
    return snap


def _create_db_snapshot(params):
    """Create a manual snapshot of a DB instance (metadata-only).

    Args:
        params: Request parameters (DBSnapshotIdentifier, DBInstanceIdentifier).

    Returns:
        tuple: XML CreateDBSnapshotResponse with the snapshot record.

    Raises:
        DBSnapshotAlreadyExistsFault: Same-name snapshot exists (400).
        DBInstanceNotFound: Source instance missing (404).
    """
    snap_id = _evaluate_params(params, "DBSnapshotIdentifier")
    if not snap_id:
        return _error("MissingParameter", "DBSnapshotIdentifier is required", 400)
    if snap_id in _snapshots:
        return _error(
            "DBSnapshotAlreadyExistsFault",
            f"Snapshot {snap_id} already exists.",
            400,
        )
    db_id = _evaluate_params(params, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id) if db_id else None
    if not instance:
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)

    snap = _create_snapshot_internal(snap_id, instance)
    arn = snap["DBSnapshotArn"]
    req_tags = _parse_tags(params)
    if req_tags:
        _tags[arn] = req_tags
        snap["TagList"] = req_tags
    return _xml(200, "CreateDBSnapshotResponse",
                f"<CreateDBSnapshotResult><DBSnapshot>{_snapshot_xml(snap)}</DBSnapshot></CreateDBSnapshotResult>")


def _describe_db_snapshots(params):
    """Describe instance snapshots, optionally filtered or listed by source."""
    snap_id = _evaluate_params(params, "DBSnapshotIdentifier")
    if snap_id:
        snap = _snapshots.get(snap_id)
        if not snap:
            return _error("DBSnapshotNotFound", f"DBSnapshot {snap_id} not found.", 404)
        snaps = [snap]
    else:
        snaps = list(_snapshots.values())
        db_id = _evaluate_params(params, "DBInstanceIdentifier")
        if db_id:
            snaps = [s for s in snaps if s.get("DBInstanceIdentifier") == db_id]

    members = "".join(f"<DBSnapshot>{_snapshot_xml(s)}</DBSnapshot>" for s in snaps)
    return _xml(200, "DescribeDBSnapshotsResponse",
                f"<DescribeDBSnapshotsResult><DBSnapshots>{members}</DBSnapshots></DescribeDBSnapshotsResult>")


def _delete_db_snapshot(params):
    """Delete an instance snapshot and its tags.

    Raises:
        DBSnapshotNotFound: Unknown snapshot identifier (404).
    """
    snap_id = _evaluate_params(params, "DBSnapshotIdentifier")
    snap = _snapshots.pop(snap_id, None)
    if not snap:
        return _error("DBSnapshotNotFound", f"DBSnapshot {snap_id} not found.", 404)
    _tags.pop(snap.get("DBSnapshotArn", ""), None)
    snap["Status"] = "deleted"
    return _xml(200, "DeleteDBSnapshotResponse",
                f"<DeleteDBSnapshotResult><DBSnapshot>{_snapshot_xml(snap)}</DBSnapshot></DeleteDBSnapshotResult>")


# ---------------------------------------------------------------------------
# DB Cluster Snapshots (metadata-only: config + tags, no data dump)
# ---------------------------------------------------------------------------

def _create_db_cluster_snapshot(params):
    """Create a manual snapshot of a DB cluster (metadata-only).

    Records the cluster configuration, tags (honoring CopyTagsToSnapshot),
    and the restore attribute defaults; contains no database dump.

    Args:
        params: Request parameters (DBClusterSnapshotIdentifier,
            DBClusterIdentifier required).

    Returns:
        tuple: XML CreateDBClusterSnapshotResponse with the snapshot record.

    Raises:
        DBClusterNotFoundFault: Source cluster missing (404).
        DBClusterSnapshotAlreadyExistsFault: Same-name snapshot exists (400).
    """
    snap_id = _evaluate_params(params, "DBClusterSnapshotIdentifier")
    if not snap_id:
        return _error("MissingParameter", "DBClusterSnapshotIdentifier is required", 400)
    if snap_id in _db_cluster_snapshots:
        return _error(
            "DBClusterSnapshotAlreadyExistsFault",
            f"DB cluster snapshot {snap_id} already exists.",
            400,
        )

    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id) if cluster_id else None
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:cluster-snapshot:{snap_id}"
    snap = {
        "DBClusterSnapshotIdentifier": snap_id,
        "DBClusterIdentifier": cluster["DBClusterIdentifier"],
        "DBClusterSnapshotArn": arn,
        "Engine": cluster["Engine"],
        "EngineVersion": cluster["EngineVersion"],
        "SnapshotCreateTime": _format_time(time.time()),
        "ClusterCreateTime": cluster.get("ClusterCreateTime", ""),
        "Status": "available",
        "Port": cluster.get("Port", 27017),
        "VpcId": "vpc-00000000",
        "MasterUsername": cluster.get("MasterUsername", "root"),
        "SnapshotType": "manual",
        "PercentProgress": 100,
        "StorageEncrypted": cluster.get("StorageEncrypted", False),
        "KmsKeyId": cluster.get("KmsKeyId", ""),
        "AvailabilityZones": cluster.get("AvailabilityZones", []),
        "LicenseModel": "docdb",
        "StorageType": "gp2",
        "DbClusterResourceId": cluster.get("DbClusterResourceId", ""),
        "IAMDatabaseAuthenticationEnabled": cluster.get("IAMDatabaseAuthenticationEnabled", False),
        "AllocatedStorage": cluster.get("AllocatedStorage", 1),
        "SourceDBClusterSnapshotArn": "",
        "TagList": list(_tags.get(cluster.get("DBClusterArn", ""), []))
        if cluster.get("CopyTagsToSnapshot") else [],
        "_attributes": {"restore": [get_account_id()]},
        "_MasterUserPassword": cluster.get("_MasterUserPassword", "password"),
        "_DBSubnetGroup": cluster.get("DBSubnetGroup", "default"),
    }
    _db_cluster_snapshots[snap_id] = snap

    req_tags = _parse_tags(params)
    if req_tags:
        _tags[arn] = req_tags
        snap["TagList"] = req_tags

    return _xml(200, "CreateDBClusterSnapshotResponse",
                "<CreateDBClusterSnapshotResult><DBClusterSnapshot>"
                f"{_cluster_snapshot_xml(snap)}</DBClusterSnapshot></CreateDBClusterSnapshotResult>")


def _describe_db_cluster_snapshots(params):
    """Describe cluster snapshots, optionally filtered by snapshot or cluster."""
    snap_id = _evaluate_params(params, "DBClusterSnapshotIdentifier")
    if snap_id:
        snap = _db_cluster_snapshots.get(snap_id)
        if not snap:
            return _error(
                "DBClusterSnapshotNotFoundFault",
                f"DB cluster snapshot {snap_id} not found.",
                404,
            )
        snaps = [snap]
    else:
        snaps = list(_db_cluster_snapshots.values())
        cluster_id = _evaluate_params(params, "DBClusterIdentifier")
        if cluster_id:
            snaps = [s for s in snaps if s.get("DBClusterIdentifier") == cluster_id]
        snap_type = _evaluate_params(params, "SnapshotType")
        if snap_type:
            snaps = [s for s in snaps if s.get("SnapshotType") == snap_type]

    members = "".join(f"<DBClusterSnapshot>{_cluster_snapshot_xml(s)}</DBClusterSnapshot>" for s in snaps)
    return _xml(200, "DescribeDBClusterSnapshotsResponse",
                "<DescribeDBClusterSnapshotsResult><DBClusterSnapshots>"
                f"{members}</DBClusterSnapshots></DescribeDBClusterSnapshotsResult>")


def _delete_db_cluster_snapshot(params):
    """Delete a cluster snapshot and its tags.

    Raises:
        DBClusterSnapshotNotFoundFault: Unknown snapshot identifier (404).
    """
    snap_id = _evaluate_params(params, "DBClusterSnapshotIdentifier")
    snap = _db_cluster_snapshots.pop(snap_id, None)
    if not snap:
        return _error(
            "DBClusterSnapshotNotFoundFault",
            f"DB cluster snapshot {snap_id} not found.",
            404,
        )
    _tags.pop(snap.get("DBClusterSnapshotArn", ""), None)
    snap["Status"] = "deleted"
    return _xml(200, "DeleteDBClusterSnapshotResponse",
                "<DeleteDBClusterSnapshotResult><DBClusterSnapshot>"
                f"{_cluster_snapshot_xml(snap)}</DBClusterSnapshot></DeleteDBClusterSnapshotResult>")


def _describe_db_cluster_snapshot_attributes(params):
    """Return a cluster snapshot's share/restore attribute values."""
    snap = _resolve_cluster_snapshot(params)
    if isinstance(snap, tuple):
        return snap
    return _snapshot_attributes_response(
        "DescribeDBClusterSnapshotAttributesResponse",
        "DescribeDBClusterSnapshotAttributesResult", snap,
    )


def _modify_db_cluster_snapshot_attribute(params):
    """Add or remove values for a cluster snapshot attribute (share/restore).

    Args:
        params: Request parameters; AttributeName (default ``restore``),
            ValuesToAdd / ValuesToRemove lists of account ids.

    Returns:
        tuple: XML ModifyDBClusterSnapshotAttributeResponse with the updated
        attributes.

    Raises:
        DBClusterSnapshotNotFoundFault: Unknown snapshot identifier (404).
    """
    snap = _resolve_cluster_snapshot(params)
    if isinstance(snap, tuple):
        return snap
    attr_name = _evaluate_params(params, "AttributeName") or "restore"
    attributes = snap.setdefault("_attributes", {})
    values = list(attributes.get(attr_name, []))
    for val in _parse_member_list(params, "ValuesToAdd"):
        if val not in values:
            values.append(val)
    remove = set(_parse_member_list(params, "ValuesToRemove"))
    attributes[attr_name] = [v for v in values if v not in remove]
    return _snapshot_attributes_response(
        "ModifyDBClusterSnapshotAttributeResponse",
        "ModifyDBClusterSnapshotAttributeResult", snap,
    )


def _resolve_cluster_snapshot(params):
    """Fetch the snapshot named by DBClusterSnapshotIdentifier or an error tuple."""
    snap_id = _evaluate_params(params, "DBClusterSnapshotIdentifier")
    snap = _db_cluster_snapshots.get(snap_id)
    if not snap:
        return _error(
            "DBClusterSnapshotNotFoundFault",
            f"DB cluster snapshot {snap_id} not found.",
            404,
        )
    return snap


def _snapshot_attributes_response(root_tag, result_tag, snap):
    """Render a snapshot-attribute response document.

    The service model nests a ``DBClusterSnapshotAttributesResult`` element
    inside the action's ``*Result`` wrapper.
    """
    attrs_xml = ""
    for attr_name, values in (snap.get("_attributes") or {"restore": []}).items():
        vals_xml = "".join(f"<AttributeValue>{_esc(v)}</AttributeValue>" for v in values)
        attrs_xml += f"""<DBClusterSnapshotAttribute>
            <AttributeName>{attr_name}</AttributeName>
            <AttributeValues>{vals_xml}</AttributeValues>
        </DBClusterSnapshotAttribute>"""
    return _xml(200, root_tag,
                f"<{result_tag}><DBClusterSnapshotAttributesResult>"
                f"<DBClusterSnapshotIdentifier>{snap['DBClusterSnapshotIdentifier']}"
                f"</DBClusterSnapshotIdentifier><DBClusterSnapshotAttributes>{attrs_xml}"
                f"</DBClusterSnapshotAttributes></DBClusterSnapshotAttributesResult></{result_tag}>")


# ---------------------------------------------------------------------------
# DB Cluster Parameter Groups
# ---------------------------------------------------------------------------

# Minimal plausible DocumentDB cluster-parameter catalog, applied to every
# family (docdb5.0 / docdb8.0): (name, default, description, type, apply type).
_CLUSTER_PARAMETER_DEFAULTS = [
    ("audit_logs", "disabled", "Which audit logs to export to CloudWatch Logs.", "string", "dynamic"),
    ("change_stream_log_retention_duration", "10800",
     "How long change stream events are retained, in seconds.", "string", "dynamic"),
    ("profiler", "disabled", "Database profiler mode (off, slow_ops, all).", "string", "dynamic"),
    ("profiler_rate_threshold_ns", "100000000",
     "Slow-operation threshold for the profiler, in nanoseconds.", "integer", "dynamic"),
    ("profiler_sampling_rate", "500",
     "Fraction of slow operations sampled, in milliseconds.", "integer", "dynamic"),
    ("tls", "enabled", "Whether TLS is required for connections.", "string", "static"),
    ("ttl_monitor_enabled", "true", "Whether the TTL monitor deletes expired documents.", "boolean", "dynamic"),
]


def _default_parameters_for_family(family):
    """Build default parameter records for a docdb parameter-group family."""
    return [
        {
            "name": name,
            "value": default,
            "description": description,
            "data_type": data_type,
            "apply_type": apply_type,
        }
        for name, default, description, data_type, apply_type in _CLUSTER_PARAMETER_DEFAULTS
    ]


def _create_db_cluster_parameter_group(params):
    """Create an empty DB cluster parameter group for a given family.

    Raises:
        DBParameterGroupAlreadyExistsFault: Same-name group exists (400).
    """
    name = _evaluate_params(params, "DBClusterParameterGroupName")
    if not name:
        return _error("MissingParameter", "DBClusterParameterGroupName is required", 400)
    if name in _db_cluster_param_groups:
        return _error(
            "DBParameterGroupAlreadyExistsFault",
            f"Parameter group {name} already exists.",
            400,
        )
    family = _evaluate_params(params, "DBParameterGroupFamily") or "docdb5.0"
    desc = _evaluate_params(params, "Description") or name
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:cluster-pg:{name}"

    _db_cluster_param_groups[name] = {
        "DBClusterParameterGroupName": name,
        "DBParameterGroupFamily": family,
        "Description": desc,
        "DBClusterParameterGroupArn": arn,
        "Parameters": {},
    }

    req_tags = _parse_tags(params)
    if req_tags:
        _tags[arn] = req_tags

    return _xml(200, "CreateDBClusterParameterGroupResponse",
                "<CreateDBClusterParameterGroupResult><DBClusterParameterGroup>"
                f"<DBClusterParameterGroupName>{name}</DBClusterParameterGroupName>"
                f"<DBParameterGroupFamily>{family}</DBParameterGroupFamily>"
                f"<Description>{_esc(desc)}</Description>"
                f"<DBClusterParameterGroupArn>{arn}</DBClusterParameterGroupArn>"
                "</DBClusterParameterGroup></CreateDBClusterParameterGroupResult>")


def _describe_db_cluster_parameter_groups(params):
    """Describe cluster parameter groups, optionally filtered by name."""
    name = _evaluate_params(params, "DBClusterParameterGroupName")
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
        <DBClusterParameterGroupArn>{g.get('DBClusterParameterGroupArn', '')}</DBClusterParameterGroupArn>
    </DBClusterParameterGroup>""" for g in groups)
    return _xml(200, "DescribeDBClusterParameterGroupsResponse",
                "<DescribeDBClusterParameterGroupsResult><DBClusterParameterGroups>"
                f"{members}</DBClusterParameterGroups></DescribeDBClusterParameterGroupsResult>")


def _delete_db_cluster_parameter_group(params):
    """Delete a cluster parameter group and its tags.

    Raises:
        DBParameterGroupNotFound: Unknown group name (404).
    """
    name = _evaluate_params(params, "DBClusterParameterGroupName")
    pg = _db_cluster_param_groups.pop(name, None)
    if not pg:
        return _error("DBParameterGroupNotFound",
                      f"DB cluster parameter group {name} not found.", 404)
    _tags.pop(pg.get("DBClusterParameterGroupArn", ""), None)
    return _xml(200, "DeleteDBClusterParameterGroupResponse", "")


def _modify_db_cluster_parameter_group(params):
    """Store submitted parameters on a group; status reports ``in-sync``."""
    pg = _resolve_cluster_param_group(params)
    if isinstance(pg, tuple):
        return pg
    store = pg.setdefault("Parameters", {})
    prefix = _parameter_member_prefix(params)
    idx = 1
    while _evaluate_params(params, f"{prefix}.{idx}.ParameterName"):
        pname = _evaluate_params(params, f"{prefix}.{idx}.ParameterName")
        pvalue = _evaluate_params(params, f"{prefix}.{idx}.ParameterValue")
        apply_method = _evaluate_params(params, f"{prefix}.{idx}.ApplyMethod") or "immediate"
        store[pname] = {"ParameterValue": pvalue, "ApplyMethod": apply_method}
        idx += 1

    return _xml(200, "ModifyDBClusterParameterGroupResponse",
                "<ModifyDBClusterParameterGroupResult>"
                f"<DBClusterParameterGroupName>{pg['DBClusterParameterGroupName']}"
                f"</DBClusterParameterGroupName></ModifyDBClusterParameterGroupResult>")


def _reset_db_cluster_parameter_group(params):
    """Reset some or all parameters of a group back to engine defaults."""
    pg = _resolve_cluster_param_group(params)
    if isinstance(pg, tuple):
        return pg
    store = pg.setdefault("Parameters", {})
    prefix = _parameter_member_prefix(params)
    has_explicit_parameters = bool(_evaluate_params(params, f"{prefix}.1.ParameterName"))
    reset_all = _evaluate_params(params, "ResetAllParameters", "").lower() == "true"
    if reset_all and has_explicit_parameters:
        return _error(
            "InvalidParameterCombination",
            "You can't specify both ResetAllParameters and Parameters.",
            400,
        )

    if reset_all or not has_explicit_parameters:
        store.clear()
    else:
        idx = 1
        while _evaluate_params(params, f"{prefix}.{idx}.ParameterName"):
            store.pop(_evaluate_params(params, f"{prefix}.{idx}.ParameterName"), None)
            idx += 1

    return _xml(200, "ResetDBClusterParameterGroupResponse",
                "<ResetDBClusterParameterGroupResult>"
                f"<DBClusterParameterGroupName>{pg['DBClusterParameterGroupName']}"
                f"</DBClusterParameterGroupName></ResetDBClusterParameterGroupResult>")


def _describe_db_cluster_parameters(params):
    """Describe a group's parameters: engine defaults overlaid with overrides.

    Supports the Source filter (``engine-default`` / ``user``).

    Raises:
        DBParameterGroupNotFound: Unknown group name (404).
    """
    pg = _resolve_cluster_param_group(params)
    if isinstance(pg, tuple):
        return pg
    source_filter = _evaluate_params(params, "Source")
    members = _cluster_parameters_xml(pg, source_filter)
    return _xml(200, "DescribeDBClusterParametersResponse",
                "<DescribeDBClusterParametersResult>"
                f"<Parameters>{members}</Parameters></DescribeDBClusterParametersResult>")


def _resolve_cluster_param_group(params):
    """Fetch the parameter group named in the request, or an error tuple."""
    name = _evaluate_params(params, "DBClusterParameterGroupName")
    pg = _db_cluster_param_groups.get(name)
    if not pg:
        return _error("DBParameterGroupNotFound",
                      f"DB cluster parameter group {name} not found.", 404)
    return pg


def _parameter_member_prefix(params, prefix="Parameters"):
    """Handle both Query API and botocore/SFN parameter-list serialization."""
    query_prefix = f"{prefix}.member"
    if _evaluate_params(params, f"{query_prefix}.1.ParameterName"):
        return query_prefix
    return f"{prefix}.Parameter"


def _cluster_parameters_xml(pg, source_filter):
    """Render a group's parameter records (defaults merged with overrides)."""
    defaults = _default_parameters_for_family(pg.get("DBParameterGroupFamily", ""))
    custom = pg.get("Parameters", {})
    default_names = {p["name"] for p in defaults}
    params_xml = ""

    for param in defaults:
        pname = param["name"]
        override = custom.get(pname)
        if isinstance(override, dict):
            value = override.get("ParameterValue", param["value"])
            apply_method = override.get("ApplyMethod", "immediate")
        else:
            value = param["value"]
            apply_method = "immediate"
        source = "user" if pname in custom else "engine-default"
        if source_filter and source != source_filter:
            continue
        params_xml += _cluster_parameter_xml(pname, value, source, apply_method, param)

    if not source_filter or source_filter == "user":
        for pname, override in custom.items():
            if pname in default_names:
                continue
            if isinstance(override, dict):
                value = override.get("ParameterValue", "")
                apply_method = override.get("ApplyMethod", "immediate")
            else:
                value, apply_method = override, "immediate"
            params_xml += _cluster_parameter_xml(pname, value, "user", apply_method, None)
    return params_xml


def _cluster_parameter_xml(name, value, source, apply_method, spec):
    """Render one Parameter record; ``spec`` carries default metadata or None."""
    description = spec["description"] if spec else ""
    data_type = spec["data_type"] if spec else "string"
    apply_type = spec["apply_type"] if spec else "dynamic"
    return f"""<Parameter>
            <ParameterName>{_esc(str(name))}</ParameterName>
            <ParameterValue>{_esc(str(value))}</ParameterValue>
            <Description>{_esc(description)}</Description>
            <Source>{source}</Source>
            <ApplyType>{apply_type}</ApplyType>
            <DataType>{data_type}</DataType>
            <IsModifiable>true</IsModifiable>
            <ApplyMethod>{apply_method}</ApplyMethod>
        </Parameter>"""


# ---------------------------------------------------------------------------
# Subnet Groups (minimal)
# ---------------------------------------------------------------------------

def _create_subnet_group(params):
    """Create a DB subnet group listing VPC subnets for cluster placement."""
    name = _evaluate_params(params, "DBSubnetGroupName")
    if not name:
        return _error("MissingParameter", "DBSubnetGroupName is required", 400)
    desc = _evaluate_params(params, "DBSubnetGroupDescription") or name
    subnet_ids = _parse_member_list(params, "SubnetIds")
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:subgrp:{name}"

    subnets = [{"SubnetIdentifier": sid, "SubnetAvailabilityZone": {"Name": f"{get_region()}a"},
                "SubnetOutpost": {}, "SubnetStatus": "Active"} for sid in subnet_ids]

    _subnet_groups[name] = {
        "DBSubnetGroupName": name,
        "DBSubnetGroupDescription": desc,
        "VpcId": "vpc-00000000",
        "SubnetGroupStatus": "Complete",
        "Subnets": subnets,
        "DBSubnetGroupArn": arn,
        "SupportedNetworkTypes": ["IPV4"],
    }

    req_tags = _parse_tags(params)
    if req_tags:
        _tags[arn] = req_tags

    sg = _subnet_groups[name]
    return _xml(200, "CreateDBSubnetGroupResponse",
                "<CreateDBSubnetGroupResult><DBSubnetGroup>"
                f"{_subnet_group_xml(sg)}</DBSubnetGroup></CreateDBSubnetGroupResult>")


def _delete_subnet_group(params):
    """Delete a DB subnet group and its tags.

    Raises:
        DBSubnetGroupNotFoundFault: Unknown group name (404).
    """
    name = _evaluate_params(params, "DBSubnetGroupName")
    sg = _subnet_groups.pop(name, None)
    if not sg:
        return _error("DBSubnetGroupNotFoundFault", f"Subnet group {name} not found.", 404)
    _tags.pop(sg.get("DBSubnetGroupArn", ""), None)
    return _xml(200, "DeleteDBSubnetGroupResponse", "")


def _describe_subnet_groups(params):
    """Describe DB subnet groups, optionally filtered by name."""
    name = _evaluate_params(params, "DBSubnetGroupName")
    if name:
        sg = _subnet_groups.get(name)
        if not sg:
            return _error("DBSubnetGroupNotFoundFault", f"Subnet group {name} not found.", 404)
        groups = [sg]
    else:
        groups = list(_subnet_groups.values())

    members = "".join(f"<DBSubnetGroup>{_subnet_group_xml(g)}</DBSubnetGroup>" for g in groups)
    return _xml(200, "DescribeDBSubnetGroupsResponse",
                "<DescribeDBSubnetGroupsResult>"
                f"<DBSubnetGroups>{members}</DBSubnetGroups></DescribeDBSubnetGroupsResult>")


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _add_tags(params):
    """Add or overwrite tags on a resource identified by ARN."""
    arn = _evaluate_params(params, "ResourceName")
    new_tags = _parse_tags(params)
    if not arn:
        return _error("MissingParameter", "ResourceName is required", 400)

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


def _remove_tags(params):
    """Remove tag keys from a resource identified by ARN."""
    arn = _evaluate_params(params, "ResourceName")
    keys_to_remove = set(_parse_member_list(params, "TagKeys"))
    if not arn:
        return _error("MissingParameter", "ResourceName is required", 400)

    existing = _tags.get(arn, [])
    _tags[arn] = [t for t in existing if t["Key"] not in keys_to_remove]

    _sync_tag_list_to_resource(arn)
    return _xml(200, "RemoveTagsFromResourceResponse", "")


def _list_tags(params):
    """List tags on a resource identified by ARN."""
    arn = _evaluate_params(params, "ResourceName")
    if not arn:
        return _xml(200, "ListTagsForResourceResponse",
                    "<ListTagsForResourceResult><TagList/></ListTagsForResourceResult>")

    tag_list = _tags.get(arn, [])
    members = "".join(f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>" for t in tag_list)
    return _xml(200, "ListTagsForResourceResponse",
                f"<ListTagsForResourceResult><TagList>{members}</TagList></ListTagsForResourceResult>")


def _sync_tag_list_to_resource(arn):
    """Keep embedded TagList copies in sync with the canonical _tags store."""
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
    for csnap in _db_cluster_snapshots.values():
        if csnap.get("DBClusterSnapshotArn") == arn:
            csnap["TagList"] = list(tag_list)
            return


# ---------------------------------------------------------------------------
# Engine Versions & Orderable Options (docdb)
# ---------------------------------------------------------------------------

def _describe_db_engine_versions(params):
    """Emit the cataloged docdb engine versions with their families."""
    version_filter = _evaluate_params(params, "EngineVersion")
    members = ""
    for ver, family in DOCDB_ENGINE_VERSIONS:
        if version_filter and ver != version_filter:
            continue
        upgrade_targets = ""
        seen_higher = False
        for higher_ver, higher_family in DOCDB_ENGINE_VERSIONS:
            if seen_higher:
                upgrade_targets += f"""<ValidUpgradeTarget>
                        <Engine>docdb</Engine>
                        <EngineVersion>{higher_ver}</EngineVersion>
                        <Description>DocumentDB {higher_ver}</Description>
                        <AutoUpgrade>false</AutoUpgrade>
                        <IsMajorVersionUpgrade>true</IsMajorVersionUpgrade>
                        <SupportedEngineModes>
                            <member>provisioned</member>
                        </SupportedEngineModes>
                        <SupportsParallelQuery>false</SupportsParallelQuery>
                        <SupportsGlobalDatabases>false</SupportsGlobalDatabases>
                        <SupportsBabelfish>false</SupportsBabelfish>
                    </ValidUpgradeTarget>"""
            if higher_ver == ver:
                seen_higher = True
        members += f"""<DBEngineVersion>
            <Engine>docdb</Engine>
            <EngineVersion>{ver}</EngineVersion>
            <DBParameterGroupFamily>{family}</DBParameterGroupFamily>
            <DBEngineDescription>Amazon DocumentDB (with MongoDB compatibility)</DBEngineDescription>
            <DBEngineVersionDescription>DocumentDB {ver}</DBEngineVersionDescription>
            <ValidUpgradeTarget>{upgrade_targets}</ValidUpgradeTarget>
            <ExportableLogTypes/>
            <SupportsLogExportsToCloudwatchLogs>false</SupportsLogExportsToCloudwatchLogs>
            <SupportsReadReplica>true</SupportsReadReplica>
            <SupportedFeatureNames/>
            <Status>available</Status>
            <SupportsParallelQuery>false</SupportsParallelQuery>
            <SupportsGlobalDatabases>false</SupportsGlobalDatabases>
            <SupportsBabelfish>false</SupportsBabelfish>
            <SupportedCACertificateIdentifiers>
                <member>rds-ca-rsa2048-g1</member>
            </SupportedCACertificateIdentifiers>
            <SupportsCertificateRotationWithoutRestart>true</SupportsCertificateRotationWithoutRestart>
        </DBEngineVersion>"""
    return _xml(200, "DescribeDBEngineVersionsResponse",
                "<DescribeDBEngineVersionsResult>"
                f"<DBEngineVersions>{members}</DBEngineVersions></DescribeDBEngineVersionsResult>")


def _describe_orderable_options(params):
    """List orderable instance classes for a cataloged engine version."""
    engine_version = _evaluate_params(params, "EngineVersion") or DEFAULT_ENGINE_VERSION
    if engine_version not in _DOCDB_ENGINE_VERSION_SET:
        engine_version = DEFAULT_ENGINE_VERSION
    db_class = _evaluate_params(params, "DBInstanceClass")

    instance_classes = [
        "db.t3.medium", "db.t3.large", "db.r5.large", "db.r5.xlarge",
        "db.m5.large", "db.m5.xlarge",
    ]

    members = ""
    for cls in instance_classes:
        if db_class and cls != db_class:
            continue
        members += f"""<OrderableDBInstanceOption>
            <Engine>docdb</Engine>
            <EngineVersion>{engine_version}</EngineVersion>
            <DBInstanceClass>{cls}</DBInstanceClass>
            <LicenseModel>docdb</LicenseModel>
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
            <SupportsPerformanceInsights>false</SupportsPerformanceInsights>
            <AvailableProcessorFeatures/>
            <SupportedEngineModes><member>provisioned</member></SupportedEngineModes>
            <SupportsStorageAutoscaling>true</SupportsStorageAutoscaling>
            <SupportsKerberosAuthentication>false</SupportsKerberosAuthentication>
            <OutpostCapable>false</OutpostCapable>
            <SupportedNetworkTypes><member>IPV4</member></SupportedNetworkTypes>
            <SupportsGlobalDatabases>false</SupportsGlobalDatabases>
            <SupportsClusters>true</SupportsClusters>
            <SupportedActivityStreamModes/>
        </OrderableDBInstanceOption>"""
    return _xml(200, "DescribeOrderableDBInstanceOptionsResponse",
                "<DescribeOrderableDBInstanceOptionsResult><OrderableDBInstanceOptions>"
                f"{members}</OrderableDBInstanceOptions></DescribeOrderableDBInstanceOptionsResult>")


# ---------------------------------------------------------------------------
# Certificates, Events, Pending Maintenance Actions
# ---------------------------------------------------------------------------

_STATIC_CERTIFICATE = {
    "CertificateIdentifier": "rds-ca-rsa2048-g1",
    "CertificateType": "CA",
    "Thumbprint": "3c9a5e1f7b2d46a89e0c1d3f5a7b9c2e4d6f8a1b3c5d7e9f",
    "ValidFrom": "2021-01-01T00:00:00Z",
    "ValidTill": "2061-01-01T00:00:00Z",
}


def _describe_certificates(params):
    """Return the static CA certificate matching CACertificateIdentifier."""
    ident = _evaluate_params(params, "CertificateIdentifier")
    cert_arn = f"arn:aws:rds:{get_region()}::cert:{_STATIC_CERTIFICATE['CertificateIdentifier']}"
    cert = dict(_STATIC_CERTIFICATE, CertificateArn=cert_arn)
    certs = [cert] if not ident or ident == cert["CertificateIdentifier"] else []
    members = "".join(f"""<Certificate>
        <CertificateArn>{c['CertificateArn']}</CertificateArn>
        <CertificateIdentifier>{c['CertificateIdentifier']}</CertificateIdentifier>
        <CertificateType>{c['CertificateType']}</CertificateType>
        <Thumbprint>{c['Thumbprint']}</Thumbprint>
        <ValidFrom>{c['ValidFrom']}</ValidFrom>
        <ValidTill>{c['ValidTill']}</ValidTill>
    </Certificate>""" for c in certs)
    return _xml(200, "DescribeCertificatesResponse",
                f"<DescribeCertificatesResult><Certificates>{members}</Certificates></DescribeCertificatesResult>")


def _describe_events(params):
    """Return recorded events; none are recorded today, so an empty list."""
    return _xml(200, "DescribeEventsResponse",
                "<DescribeEventsResult><Events/></DescribeEventsResult>")


def _apply_pending_maintenance_action(params):
    """Record an opt-in for a pending maintenance action on a resource.

    Args:
        params: Request parameters (ResourceIdentifier, ApplyAction,
            OptInType required).

    Returns:
        tuple: XML ApplyPendingMaintenanceActionResponse echoing the action.

    Raises:
        ResourceNotFoundFault: Missing required parameter (400).
    """
    resource_identifier = _evaluate_params(params, "ResourceIdentifier")
    apply_action = _evaluate_params(params, "ApplyAction")
    opt_in_type = _evaluate_params(params, "OptInType") or "immediately"
    if not resource_identifier or not apply_action:
        return _error(
            "ResourceNotFoundFault",
            "ResourceIdentifier and ApplyAction are required.",
            400,
        )
    entry = {
        "ResourceIdentifier": resource_identifier,
        "ApplyAction": apply_action,
        "OptInStatus": opt_in_type,
        "Date": _format_time(time.time()),
    }
    _pending_maintenance_actions[:] = [
        e for e in _pending_maintenance_actions
        if not (e["ResourceIdentifier"] == resource_identifier and e["ApplyAction"] == apply_action)
    ]
    _pending_maintenance_actions.append(entry)
    details = "".join(f"""<PendingMaintenanceAction>
            <Action>{entry['ApplyAction']}</Action>
            <OptInStatus>{entry['OptInStatus']}</OptInStatus>
            <Date>{entry['Date']}</Date>
        </PendingMaintenanceAction>""")
    return _xml(200, "ApplyPendingMaintenanceActionResponse",
                "<ApplyPendingMaintenanceActionResult><ResourcePendingMaintenanceActions>"
                f"<ResourceIdentifier>{_esc(resource_identifier)}</ResourceIdentifier>"
                f"<PendingMaintenanceActionDetails>{details}</PendingMaintenanceActionDetails>"
                "</ResourcePendingMaintenanceActions></ApplyPendingMaintenanceActionResult>")


def _describe_pending_maintenance_actions(params):
    """Return recorded pending maintenance actions, optionally per resource."""
    resource_identifier = _evaluate_params(params, "ResourceIdentifier")
    entries = [
        e for e in _pending_maintenance_actions
        if not resource_identifier or e["ResourceIdentifier"] == resource_identifier
    ]
    grouped: dict = {}
    for entry in entries:
        grouped.setdefault(entry["ResourceIdentifier"], []).append(entry)
    members = ""
    for rid, actions in grouped.items():
        details = "".join(f"""<PendingMaintenanceAction>
                <Action>{a['ApplyAction']}</Action>
                <OptInStatus>{a['OptInStatus']}</OptInStatus>
                <Date>{a['Date']}</Date>
            </PendingMaintenanceAction>""" for a in actions)
        members += f"""<ResourcePendingMaintenanceActions>
            <ResourceIdentifier>{_esc(rid)}</ResourceIdentifier>
            <PendingMaintenanceActionDetails>{details}</PendingMaintenanceActionDetails>
        </ResourcePendingMaintenanceActions>"""
    return _xml(200, "DescribePendingMaintenanceActionsResponse",
                "<DescribePendingMaintenanceActionsResult><PendingMaintenanceActions>"
                f"{members}</PendingMaintenanceActions></DescribePendingMaintenanceActionsResult>")


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def _xml(status, root_tag, inner):
    """Wrap rendered inner fields in a Query-API response document.

    Returns:
        tuple: ``(status, headers, body)`` with the RDS-style XML namespace
        the docdb service model declares.
    """
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    """Build an RDS-style XML error response.

    Args:
        code: AWS wire error code (e.g. ``DBClusterNotFoundFault``).
        message: Human-readable explanation surfaced to the client.
        status: HTTP status code; 4xx marks the fault type ``Sender``.
    """
    fault_type = "Sender" if 400 <= status < 500 else "Receiver"
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
    <Error><Type>{fault_type}</Type><Code>{code}</Code><Message>{message}</Message></Error>
    <RequestId>{new_uuid()}</RequestId>
</ErrorResponse>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _single_instance_response(root_tag, result_tag, instance):
    """Wrap one instance record in a create/delete/modify/start/stop envelope."""
    return _xml(200, root_tag,
                f"<{result_tag}><DBInstance>{_instance_xml(instance)}</DBInstance></{result_tag}>")


def _instance_xml(i):
    """Render an instance dict to XML fields — no wrapping element."""
    ep = i.get("Endpoint", {})
    subnet = i.get("DBSubnetGroup", {})
    if isinstance(subnet, str):
        subnet = {"DBSubnetGroupName": subnet}

    vpc_sg_xml = ""
    for sg in i.get("VpcSecurityGroups", []):
        vpc_sg_xml += f"""<VpcSecurityGroupMembership>
            <VpcSecurityGroupId>{sg.get('VpcSecurityGroupId', '')}</VpcSecurityGroupId>
            <Status>{sg.get('Status', 'active')}</Status>
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
            <DBParameterGroupName>{pg.get('DBParameterGroupName', '')}</DBParameterGroupName>
            <ParameterApplyStatus>{pg.get('ParameterApplyStatus', 'in-sync')}</ParameterApplyStatus>
        </DBParameterGroup>"""

    option_xml = ""
    for og in i.get("OptionGroupMemberships", []):
        option_xml += f"""<OptionGroupMembership>
            <OptionGroupName>{og.get('OptionGroupName', '')}</OptionGroupName>
            <Status>{og.get('Status', 'in-sync')}</Status>
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
            <SubnetIdentifier>{s.get('SubnetIdentifier', '')}</SubnetIdentifier>
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
            <CAIdentifier>{cert.get('CAIdentifier', '')}</CAIdentifier>
            <ValidTill>{cert.get('ValidTill', '')}</ValidTill>
        </CertificateDetails>"""

    return f"""<DBInstanceIdentifier>{i['DBInstanceIdentifier']}</DBInstanceIdentifier>
        <DBInstanceClass>{i['DBInstanceClass']}</DBInstanceClass>
        <Engine>{i['Engine']}</Engine>
        <EngineVersion>{i['EngineVersion']}</EngineVersion>
        <DBInstanceStatus>{i['DBInstanceStatus']}</DBInstanceStatus>
        <MasterUsername>{i['MasterUsername']}</MasterUsername>
        <DBName>{i.get('DBName', '')}</DBName>
        <Endpoint>
            <Address>{ep.get('Address', 'localhost')}</Address>
            <Port>{ep.get('Port', 27017)}</Port>
            <HostedZoneId>{ep.get('HostedZoneId', 'Z2R2ITUGPM61AM')}</HostedZoneId>
        </Endpoint>
        <AllocatedStorage>{i['AllocatedStorage']}</AllocatedStorage>
        <InstanceCreateTime>{i.get('InstanceCreateTime', '')}</InstanceCreateTime>
        <PreferredBackupWindow>{i.get('PreferredBackupWindow', '03:00-04:00')}</PreferredBackupWindow>
        <BackupRetentionPeriod>{i.get('BackupRetentionPeriod', 1)}</BackupRetentionPeriod>
        <DBSecurityGroups>{db_sg_xml}</DBSecurityGroups>
        <VpcSecurityGroups>{vpc_sg_xml}</VpcSecurityGroups>
        <DBParameterGroups>{param_xml}</DBParameterGroups>
        <AvailabilityZone>{i.get('AvailabilityZone', f'{get_region()}a')}</AvailabilityZone>
        <DBSubnetGroup>
            <DBSubnetGroupName>{subnet.get('DBSubnetGroupName', 'default')}</DBSubnetGroupName>
            <DBSubnetGroupDescription>{subnet.get('DBSubnetGroupDescription', '')}</DBSubnetGroupDescription>
            <VpcId>{subnet.get('VpcId', 'vpc-00000000')}</VpcId>
            <SubnetGroupStatus>{subnet.get('SubnetGroupStatus', 'Complete')}</SubnetGroupStatus>
            <Subnets>{subnet_xml}</Subnets>
            <DBSubnetGroupArn>{subnet.get('DBSubnetGroupArn', '')}</DBSubnetGroupArn>
        </DBSubnetGroup>
        <PreferredMaintenanceWindow>{i.get('PreferredMaintenanceWindow', 'sun:05:00-sun:06:00')}</PreferredMaintenanceWindow>
        <PendingModifiedValues>{pending_xml}</PendingModifiedValues>
        <LatestRestorableTime>{i.get('LatestRestorableTime') or _format_time(time.time())}</LatestRestorableTime>
        <MultiAZ>{str(i.get('MultiAZ', False)).lower()}</MultiAZ>
        <AutoMinorVersionUpgrade>{str(i.get('AutoMinorVersionUpgrade', True)).lower()}</AutoMinorVersionUpgrade>
        <ReadReplicaDBInstanceIdentifiers>{read_replica_xml}</ReadReplicaDBInstanceIdentifiers>
        <ReadReplicaSourceDBInstanceIdentifier>{i.get('ReadReplicaSourceDBInstanceIdentifier', '')}</ReadReplicaSourceDBInstanceIdentifier>
        <ReadReplicaDBClusterIdentifiers/>
        <ReplicaMode>{i.get('ReplicaMode', '')}</ReplicaMode>
        <LicenseModel>{i.get('LicenseModel', 'general-public-license')}</LicenseModel>
        {iops_xml}
        <OptionGroupMemberships>{option_xml}</OptionGroupMemberships>
        <PubliclyAccessible>{str(i.get('PubliclyAccessible', False)).lower()}</PubliclyAccessible>
        <StatusInfos/>
        <StorageType>{i.get('StorageType', 'gp2')}</StorageType>
        <DbInstancePort>{i.get('DbInstancePort', 0)}</DbInstancePort>
        <DBClusterIdentifier>{i.get('DBClusterIdentifier', '')}</DBClusterIdentifier>
        <StorageEncrypted>{str(i.get('StorageEncrypted', False)).lower()}</StorageEncrypted>
        <KmsKeyId>{i.get('KmsKeyId', '')}</KmsKeyId>
        <DbiResourceId>{i.get('DbiResourceId', '')}</DbiResourceId>
        <CACertificateIdentifier>{i.get('CACertificateIdentifier', 'rds-ca-rsa2048-g1')}</CACertificateIdentifier>
        <DomainMemberships/>
        <CopyTagsToSnapshot>{str(i.get('CopyTagsToSnapshot', False)).lower()}</CopyTagsToSnapshot>
        <MonitoringInterval>{i.get('MonitoringInterval', 0)}</MonitoringInterval>
        <EnhancedMonitoringResourceArn>{i.get('EnhancedMonitoringResourceArn', '')}</EnhancedMonitoringResourceArn>
        <MonitoringRoleArn>{i.get('MonitoringRoleArn', '')}</MonitoringRoleArn>
        <PromotionTier>{i.get('PromotionTier', 1)}</PromotionTier>
        <DBInstanceArn>{i['DBInstanceArn']}</DBInstanceArn>
        <IAMDatabaseAuthenticationEnabled>{str(i.get('IAMDatabaseAuthenticationEnabled', False)).lower()}</IAMDatabaseAuthenticationEnabled>
        <PerformanceInsightsEnabled>{str(i.get('PerformanceInsightsEnabled', False)).lower()}</PerformanceInsightsEnabled>
        <EnabledCloudwatchLogsExports/>
        <ProcessorFeatures/>
        <DeletionProtection>{str(i.get('DeletionProtection', False)).lower()}</DeletionProtection>
        <AssociatedRoles/>
        <MaxAllocatedStorage>{i.get('MaxAllocatedStorage', i.get('AllocatedStorage', 20))}</MaxAllocatedStorage>
        <TagList>{tag_xml}</TagList>
        {cert_xml}
        <CustomerOwnedIpEnabled>{str(i.get('CustomerOwnedIpEnabled', False)).lower()}</CustomerOwnedIpEnabled>
        <BackupTarget>{i.get('BackupTarget', 'region')}</BackupTarget>
        <NetworkType>{i.get('NetworkType', 'IPV4')}</NetworkType>
        <StorageThroughput>{i.get('StorageThroughput', 0)}</StorageThroughput>
        <IsStorageConfigUpgradeAvailable>{str(i.get('IsStorageConfigUpgradeAvailable', False)).lower()}</IsStorageConfigUpgradeAvailable>"""


def _cluster_xml(c):
    """Render a cluster dict to XML fields — no wrapping element."""
    vpc_sg_xml = ""
    for sg in c.get("VpcSecurityGroups", []):
        vpc_sg_xml += f"""<VpcSecurityGroupMembership>
            <VpcSecurityGroupId>{sg.get('VpcSecurityGroupId', '')}</VpcSecurityGroupId>
            <Status>{sg.get('Status', 'active')}</Status>
        </VpcSecurityGroupMembership>"""

    member_xml = ""
    for m in c.get("DBClusterMembers", []):
        member_xml += f"""<DBClusterMember>
            <DBInstanceIdentifier>{m.get('DBInstanceIdentifier', '')}</DBInstanceIdentifier>
            <IsClusterWriter>{str(m.get('IsClusterWriter', True)).lower()}</IsClusterWriter>
            <DBClusterParameterGroupStatus>in-sync</DBClusterParameterGroupStatus>
            <PromotionTier>{m.get('PromotionTier', 1)}</PromotionTier>
        </DBClusterMember>"""

    az_xml = ""
    for az in c.get("AvailabilityZones", []):
        az_xml += f"<AvailabilityZone>{az}</AvailabilityZone>"

    tag_xml = ""
    for t in c.get("TagList", []):
        tag_xml += f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>"

    db_name = c.get("DatabaseName")
    db_name_xml = f"<DatabaseName>{db_name}</DatabaseName>" if db_name else ""

    return f"""<DBClusterIdentifier>{c['DBClusterIdentifier']}</DBClusterIdentifier>
        <DBClusterArn>{c['DBClusterArn']}</DBClusterArn>
        <Engine>{c['Engine']}</Engine>
        <EngineVersion>{c['EngineVersion']}</EngineVersion>
        <EngineMode>{c.get('EngineMode', 'provisioned')}</EngineMode>
        <Status>{c['Status']}</Status>
        <MasterUsername>{c.get('MasterUsername', 'root')}</MasterUsername>
        {db_name_xml}
        <Endpoint>{c.get('Endpoint', '')}</Endpoint>
        <ReaderEndpoint>{c.get('ReaderEndpoint', '')}</ReaderEndpoint>
        <Port>{c['Port']}</Port>
        <MultiAZ>{str(c.get('MultiAZ', False)).lower()}</MultiAZ>
        <AvailabilityZones>{az_xml}</AvailabilityZones>
        <DBClusterMembers>{member_xml}</DBClusterMembers>
        <VpcSecurityGroups>{vpc_sg_xml}</VpcSecurityGroups>
        <DBSubnetGroup>{c.get('DBSubnetGroup', 'default')}</DBSubnetGroup>
        <DBClusterParameterGroup>{c.get('DBClusterParameterGroup', '')}</DBClusterParameterGroup>
        <BackupRetentionPeriod>{c.get('BackupRetentionPeriod', 1)}</BackupRetentionPeriod>
        <PreferredBackupWindow>{c.get('PreferredBackupWindow', '03:00-04:00')}</PreferredBackupWindow>
        <PreferredMaintenanceWindow>{c.get('PreferredMaintenanceWindow', 'sun:05:00-sun:06:00')}</PreferredMaintenanceWindow>
        <ClusterCreateTime>{c.get('ClusterCreateTime', '')}</ClusterCreateTime>
        <EarliestRestorableTime>{c.get('EarliestRestorableTime', '')}</EarliestRestorableTime>
        <LatestRestorableTime>{c.get('LatestRestorableTime', '')}</LatestRestorableTime>
        <StorageEncrypted>{str(c.get('StorageEncrypted', False)).lower()}</StorageEncrypted>
        <KmsKeyId>{c.get('KmsKeyId', '')}</KmsKeyId>
        <DeletionProtection>{str(c.get('DeletionProtection', False)).lower()}</DeletionProtection>
        <IAMDatabaseAuthenticationEnabled>{str(c.get('IAMDatabaseAuthenticationEnabled', False)).lower()}</IAMDatabaseAuthenticationEnabled>
        <HttpEndpointEnabled>{str(c.get('HttpEndpointEnabled', False)).lower()}</HttpEndpointEnabled>
        <CopyTagsToSnapshot>{str(c.get('CopyTagsToSnapshot', False)).lower()}</CopyTagsToSnapshot>
        <CrossAccountClone>{str(c.get('CrossAccountClone', False)).lower()}</CrossAccountClone>
        <DbClusterResourceId>{c.get('DbClusterResourceId', '')}</DbClusterResourceId>
        <HostedZoneId>{c.get('HostedZoneId', 'Z2R2ITUGPM61AM')}</HostedZoneId>
        <AssociatedRoles/>
        <TagList>{tag_xml}</TagList>
        <AllocatedStorage>{c.get('AllocatedStorage', 1)}</AllocatedStorage>
        <ActivityStreamStatus>{c.get('ActivityStreamStatus', 'stopped')}</ActivityStreamStatus>
        <NetworkType>{c.get('NetworkType', 'IPV4')}</NetworkType>
        <EngineLifecycleSupport>{c.get('EngineLifecycleSupport', 'open-source-rds-extended-support')}</EngineLifecycleSupport>"""


def _snapshot_xml(s):
    """Render an (legacy) instance-snapshot dict to XML fields."""
    tag_xml = ""
    for t in s.get("TagList", []):
        tag_xml += f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>"
    return f"""<DBSnapshotIdentifier>{s['DBSnapshotIdentifier']}</DBSnapshotIdentifier>
        <DBInstanceIdentifier>{s['DBInstanceIdentifier']}</DBInstanceIdentifier>
        <DBSnapshotArn>{s.get('DBSnapshotArn', '')}</DBSnapshotArn>
        <Engine>{s['Engine']}</Engine>
        <EngineVersion>{s['EngineVersion']}</EngineVersion>
        <SnapshotCreateTime>{s.get('SnapshotCreateTime', '')}</SnapshotCreateTime>
        <InstanceCreateTime>{s.get('InstanceCreateTime', '')}</InstanceCreateTime>
        <Status>{s['Status']}</Status>
        <AllocatedStorage>{s.get('AllocatedStorage', 20)}</AllocatedStorage>
        <AvailabilityZone>{s.get('AvailabilityZone', f'{get_region()}a')}</AvailabilityZone>
        <VpcId>{s.get('VpcId', 'vpc-00000000')}</VpcId>
        <Port>{s.get('Port', 27017)}</Port>
        <MasterUsername>{s.get('MasterUsername', 'root')}</MasterUsername>
        <DBName>{s.get('DBName', '')}</DBName>
        <SnapshotType>{s.get('SnapshotType', 'manual')}</SnapshotType>
        <LicenseModel>{s.get('LicenseModel', 'docdb')}</LicenseModel>
        <StorageType>{s.get('StorageType', 'gp2')}</StorageType>
        <DBInstanceClass>{s.get('DBInstanceClass', 'db.t3.medium')}</DBInstanceClass>
        <StorageEncrypted>{str(s.get('StorageEncrypted', False)).lower()}</StorageEncrypted>
        <KmsKeyId>{s.get('KmsKeyId', '')}</KmsKeyId>
        <Encrypted>{str(s.get('Encrypted', False)).lower()}</Encrypted>
        <IAMDatabaseAuthenticationEnabled>{str(s.get('IAMDatabaseAuthenticationEnabled', False)).lower()}</IAMDatabaseAuthenticationEnabled>
        <PercentProgress>{s.get('PercentProgress', 100)}</PercentProgress>
        <DbiResourceId>{s.get('DbiResourceId', '')}</DbiResourceId>
        <TagList>{tag_xml}</TagList>
        <OriginalSnapshotCreateTime>{s.get('OriginalSnapshotCreateTime', '')}</OriginalSnapshotCreateTime>
        <SnapshotDatabaseTime>{s.get('SnapshotDatabaseTime', '')}</SnapshotDatabaseTime>
        <SnapshotTarget>{s.get('SnapshotTarget', 'region')}</SnapshotTarget>"""


def _cluster_snapshot_xml(s):
    """Render a cluster snapshot dict to XML fields — no wrapping element."""
    tag_xml = ""
    for t in s.get("TagList", []):
        tag_xml += f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>"
    az_xml = ""
    for az in s.get("AvailabilityZones", []):
        az_xml += f"<AvailabilityZone>{az}</AvailabilityZone>"
    return f"""<DBClusterSnapshotIdentifier>{s['DBClusterSnapshotIdentifier']}</DBClusterSnapshotIdentifier>
        <DBClusterIdentifier>{s['DBClusterIdentifier']}</DBClusterIdentifier>
        <DBClusterSnapshotArn>{s.get('DBClusterSnapshotArn', '')}</DBClusterSnapshotArn>
        <Engine>{s['Engine']}</Engine>
        <EngineVersion>{s['EngineVersion']}</EngineVersion>
        <SnapshotCreateTime>{s.get('SnapshotCreateTime', '')}</SnapshotCreateTime>
        <ClusterCreateTime>{s.get('ClusterCreateTime', '')}</ClusterCreateTime>
        <Status>{s['Status']}</Status>
        <Port>{s.get('Port', 27017)}</Port>
        <VpcId>{s.get('VpcId', 'vpc-00000000')}</VpcId>
        <MasterUsername>{s.get('MasterUsername', 'root')}</MasterUsername>
        <SnapshotType>{s.get('SnapshotType', 'manual')}</SnapshotType>
        <PercentProgress>{s.get('PercentProgress', 100)}</PercentProgress>
        <StorageEncrypted>{str(s.get('StorageEncrypted', False)).lower()}</StorageEncrypted>
        <KmsKeyId>{s.get('KmsKeyId', '')}</KmsKeyId>
        <AvailabilityZones>{az_xml}</AvailabilityZones>
        <LicenseModel>{s.get('LicenseModel', 'docdb')}</LicenseModel>
        <StorageType>{s.get('StorageType', 'gp2')}</StorageType>
        <DbClusterResourceId>{s.get('DbClusterResourceId', '')}</DbClusterResourceId>
        <SourceDBClusterSnapshotArn>{s.get('SourceDBClusterSnapshotArn', '')}</SourceDBClusterSnapshotArn>
        <IAMDatabaseAuthenticationEnabled>{str(s.get('IAMDatabaseAuthenticationEnabled', False)).lower()}</IAMDatabaseAuthenticationEnabled>
        <AllocatedStorage>{s.get('AllocatedStorage', 1)}</AllocatedStorage>
        <TagList>{tag_xml}</TagList>"""


def _subnet_group_xml(sg):
    """Render a subnet-group dict to XML fields — no wrapping element."""
    subnets_xml = ""
    for s in sg.get("Subnets", []):
        az = s.get("SubnetAvailabilityZone", {}).get("Name", f"{get_region()}a") if isinstance(s.get("SubnetAvailabilityZone"), dict) else f"{get_region()}a"
        subnets_xml += f"""<Subnet>
            <SubnetIdentifier>{s.get('SubnetIdentifier', '')}</SubnetIdentifier>
            <SubnetAvailabilityZone><Name>{az}</Name></SubnetAvailabilityZone>
            <SubnetOutpost/>
            <SubnetStatus>Active</SubnetStatus>
        </Subnet>"""
    return f"""<DBSubnetGroupName>{sg['DBSubnetGroupName']}</DBSubnetGroupName>
        <DBSubnetGroupDescription>{sg.get('DBSubnetGroupDescription', '')}</DBSubnetGroupDescription>
        <VpcId>{sg.get('VpcId', 'vpc-00000000')}</VpcId>
        <SubnetGroupStatus>{sg.get('SubnetGroupStatus', 'Complete')}</SubnetGroupStatus>
        <Subnets>{subnets_xml}</Subnets>
        <DBSubnetGroupArn>{sg.get('DBSubnetGroupArn', '')}</DBSubnetGroupArn>
        <SupportedNetworkTypes><member>IPV4</member></SupportedNetworkTypes>"""


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _format_time(ts):
    """Format a unix timestamp as DocDB-style UTC with millisecond precision."""
    dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _evaluate_params(params, key, default=""):
    """Read the first value for a request parameter, or the default."""
    val = params.get(key, [default])
    if isinstance(val, list):
        return val[0] if val else default
    return val


def _parse_tags(params):
    """Parse Tags.member.N.Key/Value or Tags.Tag.N.Key/Value into records."""
    tags = []
    prefix = "Tags.member"
    if not _evaluate_params(params, "Tags.member.1.Key"):
        prefix = "Tags.Tag"
    i = 1
    while True:
        key = _evaluate_params(params, f"{prefix}.{i}.Key")
        if not key:
            break
        value = _evaluate_params(params, f"{prefix}.{i}.Value", "")
        tags.append({"Key": key, "Value": value})
        i += 1
    return tags


def _parse_member_list(params, prefix):
    """Parse list params in either Prefix.member.N or Prefix.<MemberName>.N form.

    The member.N format is used by direct AWS CLI/SDK calls. The
    <MemberName>.N format is produced by botocore's serializer when dispatched
    via Step Functions aws-sdk integrations (e.g. SubnetIds.SubnetIdentifier.N).
    """
    items = []
    i = 1
    while True:
        val = _evaluate_params(params, f"{prefix}.member.{i}")
        if not val:
            break
        items.append(val)
        i += 1
    if items:
        return items
    import re
    pattern = re.compile(rf"^{re.escape(prefix)}\.([^.]+)\.(\d+)$")
    numbered = {}
    for key in params:
        m = pattern.match(key)
        if m:
            idx = int(m.group(2))
            numbered[idx] = _evaluate_params(params, key)
    return [numbered[k] for k in sorted(numbered)] if numbered else []


def _parse_filters(params):
    """Parse request filters in either ``Filters.Filter.N`` or
    ``Filters.member.N`` wire form (botocore emits one or the other
    depending on the model's locationName)."""
    filters = {}
    i = 1
    while True:
        name = _evaluate_params(params, f"Filters.Filter.{i}.Name")
        value_prefix = f"Filters.Filter.{i}.Values.Value"
        if not name:
            name = _evaluate_params(params, f"Filters.member.{i}.Name")
            value_prefix = f"Filters.member.{i}.Values.member"
        if not name:
            break
        values = []
        j = 1
        while True:
            v = _evaluate_params(params, f"{value_prefix}.{j}")
            if not v:
                break
            values.append(v)
            j += 1
        filters[name] = values
        i += 1
    return filters


# ---------------------------------------------------------------------------
# Record resolution & filtering
# ---------------------------------------------------------------------------

def _resolve_instance(db_id):
    """Look up an instance by DBInstanceIdentifier or DbiResourceId.

    AWS accepts either value for the DBInstanceIdentifier parameter in
    DescribeDBInstances and related APIs.
    """
    inst = _instances.get(db_id)
    if inst:
        return inst
    if db_id.startswith("db-"):
        for inst in _instances.values():
            if inst.get("DbiResourceId") == db_id:
                return inst
    return None


def _cluster_member_instances(cluster):
    """Resolve a cluster's member records to their live instance dicts."""
    return [
        inst
        for inst in (
            _instances.get(member.get("DBInstanceIdentifier"))
            for member in cluster.get("DBClusterMembers", [])
        )
        if inst is not None
    ]


def _apply_instance_filters(instances, filters):
    """Filter instance records by db-instance-id, engine, or db-cluster-id."""
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
    """Filter cluster records by db-cluster-id or engine."""
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


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

def reset():
    """Stop/remove every docdb container (cluster-owned and standalone), then
    clear all state.

    Cluster-owned containers are reaped once from the cluster records before
    standalone instances are considered, so members never race to remove the
    same shared container.
    """
    with _shared_container_lock:
        docker_client = _get_docker()
        shared_container_ids = set()
        if docker_client:
            for (_acct, _reg, cluster_id), cluster in list(_clusters.all_items()):
                if any(cluster.get(f) for f in (
                        "_shared_container_id", "_shared_endpoint", "_shared_volume_name",
                )):
                    if cluster.get("_shared_container_id"):
                        shared_container_ids.add(cluster["_shared_container_id"])
                    _remove_cluster_shared_resources(cluster_id, cluster, timeout=2)
            for instance in _instances.all_values():
                cid = instance.get("_docker_container_id")
                if cid and cid not in shared_container_ids:
                    try:
                        c = docker_client.containers.get(cid)
                        c.stop(timeout=2)
                        c.remove(v=True)
                    except Exception as e:
                        logger.warning("reset: failed to stop/remove docdb container %s: %s", cid, e)
        _instances.clear()
        _clusters.clear()
        _subnet_groups.clear()
        _snapshots.clear()
        _db_cluster_snapshots.clear()
        _db_cluster_param_groups.clear()
        _tags.clear()
        _pending_maintenance_actions.clear()
        _port_counter[0] = BASE_PORT


# ---------------------------------------------------------------------------
# Action map
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    # Instances
    "CreateDBInstance": _create_db_instance,
    "DeleteDBInstance": _delete_db_instance,
    "DescribeDBInstances": _describe_db_instances,
    "ModifyDBInstance": _modify_db_instance,
    "StartDBInstance": _start_db_instance,
    "StopDBInstance": _stop_db_instance,
    "RebootDBInstance": _reboot_db_instance,
    # Clusters
    "CreateDBCluster": _create_db_cluster,
    "DeleteDBCluster": _delete_db_cluster,
    "DescribeDBClusters": _describe_db_clusters,
    "ModifyDBCluster": _modify_db_cluster,
    "StartDBCluster": _start_db_cluster,
    "StopDBCluster": _stop_db_cluster,
    "FailoverDBCluster": _failover_db_cluster,
    "RestoreDBClusterFromSnapshot": _restore_db_cluster_from_snapshot,
    # Subnet groups
    "CreateDBSubnetGroup": _create_subnet_group,
    "DeleteDBSubnetGroup": _delete_subnet_group,
    "DescribeDBSubnetGroups": _describe_subnet_groups,
    # Instance snapshots (legacy direct-HTTP surface; not in the real API)
    "CreateDBSnapshot": _create_db_snapshot,
    "DeleteDBSnapshot": _delete_db_snapshot,
    "DescribeDBSnapshots": _describe_db_snapshots,
    # Cluster snapshots
    "CreateDBClusterSnapshot": _create_db_cluster_snapshot,
    "DeleteDBClusterSnapshot": _delete_db_cluster_snapshot,
    "DescribeDBClusterSnapshots": _describe_db_cluster_snapshots,
    "ModifyDBClusterSnapshotAttribute": _modify_db_cluster_snapshot_attribute,
    "DescribeDBClusterSnapshotAttributes": _describe_db_cluster_snapshot_attributes,
    # Cluster parameter groups
    "CreateDBClusterParameterGroup": _create_db_cluster_parameter_group,
    "DescribeDBClusterParameterGroups": _describe_db_cluster_parameter_groups,
    "DeleteDBClusterParameterGroup": _delete_db_cluster_parameter_group,
    "ModifyDBClusterParameterGroup": _modify_db_cluster_parameter_group,
    "ResetDBClusterParameterGroup": _reset_db_cluster_parameter_group,
    "DescribeDBClusterParameters": _describe_db_cluster_parameters,
    # Tags
    "ListTagsForResource": _list_tags,
    "AddTagsToResource": _add_tags,
    "RemoveTagsFromResource": _remove_tags,
    # Catalog & maintenance
    "DescribeDBEngineVersions": _describe_db_engine_versions,
    "DescribeOrderableDBInstanceOptions": _describe_orderable_options,
    "ApplyPendingMaintenanceAction": _apply_pending_maintenance_action,
    "DescribePendingMaintenanceActions": _describe_pending_maintenance_actions,
    "DescribeCertificates": _describe_certificates,
    "DescribeEvents": _describe_events,
}


# Load persisted state at module import. Must run AFTER every helper this code
# path may touch is defined — restore_state spawns daemon threads that race
# against the rest of module parsing, and a thread reaching an undefined name
# raises NameError mid-restore (mirrors rds.py issue #692).
try:
    _restored = load_state("documentdb")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


def _live_container_ids():
    """Container ids still owned by a live instance or cluster.

    A stopped cluster still owns its (exited) container — StartDBCluster must
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


container_reaper.register_live_ids("documentdb", _live_container_ids)
