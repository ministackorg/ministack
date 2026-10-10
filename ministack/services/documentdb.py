# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Amazon DocumentDB: DB clusters and instances, each cluster backed by a documentdb-local container.

DocumentDB requests arrive signed for ``rds``; the router hands this module the ones that create or
name a DocumentDB cluster or instance. Subnet and parameter groups are shared with RDS.
"""

import copy
import datetime
import json
import logging
import os
import re
import socket
import threading
import time
from urllib.parse import parse_qs
from xml.sax.saxutils import escape as _esc

from ministack.core import container_reaper
from ministack.core.concurrency import run_offloop
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    _request_account_id,
    _request_region,
    apply_image_prefix,
    get_account_id,
    get_region,
    new_uuid,
)
from ministack.services import rds as _rds

logger = logging.getLogger("documentdb")

ACCOUNT_ID = "000000000000"
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
BASE_PORT = int(os.environ.get("DOCDB_BASE_PORT", "27117"))
DOCDB_TMPFS_SIZE = os.environ.get("DOCDB_TMPFS_SIZE", "256m")
DOCDB_PERSIST = os.environ.get("DOCDB_PERSIST", "0").lower() in ("1", "true", "yes")
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")

# Creatable versions; minor versions exist from 5.0 on. The default is the latest major.
DOCDB_ENGINE_VERSIONS = ["3.6.0", "4.0.0", "5.0.0", "5.0.1", "5.0.2", "8.0.0", "8.0.1", "8.0.2"]
_DOCDB_ENGINE_VERSION_SET = set(DOCDB_ENGINE_VERSIONS)
DEFAULT_ENGINE_VERSION = "8.0.0"

_instances = AccountRegionScopedDict()
_clusters = AccountRegionScopedDict()
_tags = AccountScopedDict()
_port_counter = [BASE_PORT]

_docker = None
_ministack_network = None

_shared_container_lock = threading.RLock()
_port_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def get_state():
    """Return a persistable snapshot of all DocumentDB state."""
    with _shared_container_lock:
        instances = copy.deepcopy(_instances)
        clusters = copy.deepcopy(_clusters)
        state = {
            "instances": instances,
            "clusters": clusters,
            "tags": copy.deepcopy(_tags),
            "port_counter": _port_counter[0],
        }
    for key in list(instances._data):
        instances._data[key].pop("_docker_container_id", None)
    for key in list(clusters._data):
        clusters._data[key].pop("_shared_container_id", None)
    return state


def load_persisted_state(data):
    """Load persisted state and respawn backing containers."""
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
        for name, inst in instances_data.items():
            account_id = (_record_account(inst) or get_account_id())
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
    """Store region from an ``arn:aws:rds:<region>:...`` record field."""
    for field in ("DBInstanceArn", "DBClusterArn"):
        parts = (record.get(field) or "").split(":")
        if len(parts) > 3 and parts[3]:
            return parts[3]
    return REGION


def _record_account(record):
    """Store account id from an ARN-shaped record field."""
    for field in ("DBInstanceArn", "DBClusterArn"):
        parts = (record.get(field) or "").split(":")
        if len(parts) > 4 and parts[4]:
            return parts[4]
    return None


def _respawn_cluster_members(account_id, region, cluster_id, members):
    """Restart one cluster's shared container after a warm boot."""
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
        epoch_now = int(cluster.get("_shared_container_epoch", 0))
        if current is not cluster or epoch_now != restore_epoch:
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
    started = _launch_documentdb_container(
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
    """Detect the Docker network MiniStack itself runs on (if containerized)."""
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


def _wait_for_port(host, port, timeout=300):
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
    """Get s the Docker container name for a cluster's shared DocumentDB contain er."""
    return f"ministack-docdb-cluster-{cluster_id}"


def _cluster_volume_name(cluster_id):
    """Gets the Docker volume name for a cluster's persistent storage."""
    return f"ministack-docdb-cluster-{cluster_id}-data"


def _instance_volume_name(db_id):
    """Gets the Docker volume name for a standalone instance's storage."""
    return f"ministack-docdb-{db_id}-data"


def _launch_documentdb_container(name,
                                 image,
                                 env,
                                 host_port,
                                 container_port,
                                 data_path,
                                 labels,
                                 volume_name=None):
    """Run one DocumentDB container and derive its endpoint addresses."""
    docker_client = _get_docker()
    if not docker_client:
        return None
    ms_network = _get_ministack_network(docker_client)
    kwargs = {
        "image": image,
        "detach": True,
        "environment": env,
        "ports": {f"{container_port}/tcp": host_port},
        "name": name,
        "labels": labels,
    }
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
    """Start (or recreate) the single DocumentDB container owned by a DocDB cluster."""
    engine_version = cluster.get("EngineVersion") or DEFAULT_ENGINE_VERSION
    master_user = cluster.get("MasterUsername", "root")
    master_pass = cluster.get("_MasterUserPassword", "password")
    db_name = cluster.get("DatabaseName") or "admin"

    def _fallback_endpoint():
        """Returns default endpoint if needed."""
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
            "docdb: persisted shared host port %d for cluster %s is in use; "
            "allocating a fresh port",
            host_port, cluster_id,
        )
        host_port = _next_port()

    volume_name = None
    if DOCDB_PERSIST:
        volume_name = cluster.get("_shared_volume_name") or _cluster_volume_name(cluster_id)
        cluster["_shared_volume_name"] = volume_name
        cluster["_shared_storage_initialized"] = True

    started = _launch_documentdb_container(
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
        logger.warning(
            "docdb: failed to restart shared container for cluster %s: %s", cluster_id, e)
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
    """Stop a cluster's shared DocumentDB container, preserving it and its volume."""
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
    """Stop and remove a cluster's shared container and its named volume."""
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
    instance["MasterUsername"] = cluster.get(
        "MasterUsername", instance.get("MasterUsername", "root"))
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
    """Append the instance to its parent cluster's ``DBClusterMembers``."""
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
    """Return the default DocumentDB engine version (the latest major)."""
    return DEFAULT_ENGINE_VERSION


def _default_parameter_group(engine_version):
    return "default.docdb" + ".".join(str(engine_version).split(".")[:2])


def _engine_version_error(engine_version):
    """Build an InvalidParameterCombination error for unsupported versions."""
    if engine_version in _DOCDB_ENGINE_VERSION_SET:
        return None
    return _error("InvalidParameterCombination", f"Cannot find version {engine_version} for docdb", 400)


def _docker_image_for_docdb(engine_version, user, password, db_name=""):
    """Return the DocumentDB container configuration for any engine version."""
    env = {
        "USERNAME": user,
        "PASSWORD": password,
        "DOCUMENTDB_PORT": "27017",
    }
    return (
        apply_image_prefix("ghcr.io/documentdb/documentdb/documentdb-local:latest"),
        env,
        27017,
        "/data",
    )


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
    """Dispatch a DocumentDB request off the event loop."""
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


def claims_request(params):
    """Whether an rds-signed request creates or names a DocumentDB cluster or instance."""
    if _evaluate_params(params, "Action") not in _ACTION_MAP:
        return False
    if _evaluate_params(params, "Engine") == "docdb":
        return True
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    if cluster_id and cluster_id in _clusters:
        return True
    instance_id = _evaluate_params(params, "DBInstanceIdentifier")
    if instance_id and _resolve_instance(instance_id):
        return True
    arn = _evaluate_params(params, "ResourceName")
    return bool(arn) and (any(c.get("DBClusterArn") == arn for c in _clusters.values())
                          or any(i.get("DBInstanceArn") == arn for i in _instances.values()))


def cluster_members_xml(filters):
    """``<DBCluster>`` elements for RDS's unfiltered DescribeDBClusters, which lists every engine."""
    clusters = _apply_cluster_filters(list(_clusters.values()), filters) if filters else _clusters.values()
    return "".join(f"<DBCluster>{_cluster_xml(c)}</DBCluster>" for c in clusters)


def instance_members_xml(filters):
    """``<DBInstance>`` elements for RDS's unfiltered DescribeDBInstances, which lists every engine."""
    instances = _apply_instance_filters(list(_instances.values()), filters) if filters else _instances.values()
    return "".join(f"<DBInstance>{_instance_xml(i)}</DBInstance>" for i in instances)


# ---------------------------------------------------------------------------
# DB Instances
# ---------------------------------------------------------------------------

def _create_db_instance(params):
    """Create a DB instance, optionally as a member of a DB cluster."""
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
        engine_version = _evaluate_params(params, "EngineVersion") \
            or _default_engine_version(engine)
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
            "DBParameterGroupName": _default_parameter_group(engine_version),
            "ParameterApplyStatus": "in-sync",
        }],
        "AvailabilityZone": _evaluate_params(params, "AvailabilityZone") or f"{get_region()}a",
        "DBSubnetGroup": _rds._subnet_groups.get(subnet_group_name, {
            "DBSubnetGroupName": subnet_group_name,
            "DBSubnetGroupDescription": "default",
            "SubnetGroupStatus": "Complete",
            "Subnets": [],
            "VpcId": "vpc-00000000",
            "DBSubnetGroupArn": (
            f"arn:aws:rds:{get_region()}:{get_account_id()}:subgrp:{subnet_group_name}"),
        }),
        "PreferredMaintenanceWindow": (
            _evaluate_params(params, "PreferredMaintenanceWindow")
            or "sun:05:00-sun:06:00"),
        "PendingModifiedValues": {},
        "LatestRestorableTime": _format_time(now_ts),
        "MultiAZ": _evaluate_params(params, "MultiAZ") == "true",
        "AutoMinorVersionUpgrade": _evaluate_params(params, "AutoMinorVersionUpgrade") != "false",
        "ReadReplicaDBInstanceIdentifiers": [],
        "ReadReplicaSourceDBInstanceIdentifier": "",
        "ReadReplicaDBClusterIdentifiers": [],
        "ReplicaMode": "",
        "LicenseModel": "docdb",
        "Iops": (
            int(_evaluate_params(params, "Iops") or "0")
            if _evaluate_params(params, "Iops") else None),
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
        "IAMDatabaseAuthenticationEnabled": (
            _evaluate_params(params, "EnableIAMDatabaseAuthentication") == "true"),
        "PerformanceInsightsEnabled": False,
        "PerformanceInsightsKMSKeyId": "",
        "PerformanceInsightsRetentionPeriod": 7,
        "EnabledCloudwatchLogsExports": [],
        "ProcessorFeatures": [],
        "DeletionProtection": deletion_protection,
        "AssociatedRoles": [],
        "MaxAllocatedStorage": int(
            _evaluate_params(params, "MaxAllocatedStorage") or str(allocated_storage)),
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
    """Guarantee a running shared container behind a cluster before use."""
    with _shared_container_lock:
        if cluster.get("_shared_container_ready") and cluster.get("_shared_container_id"):
            return
        if cluster.get("_shared_container_id"):
            result = _restart_cluster_shared_container(cluster["DBClusterIdentifier"], cluster)
            if result.get("started"):
                return
        _start_cluster_shared_container(cluster["DBClusterIdentifier"], cluster, remove_stale=True)


def _start_instance_container(db_id, instance):
    """Start the per-instance DocumentDB container backing a standalone instance."""
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
    started = _launch_documentdb_container(
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
    _log_readiness_async(
        internal_addr or "127.0.0.1", internal_port or host_port, f"instance {db_id}")


def _log_readiness_async(host, port, label):
    """Spawn a daemon thread that waits for the port and logs readiness."""
    def _bg_wait(h=host, p=int(port or 0), lbl=label):
        if not p:
            return
        if _wait_for_port(h, p):
            logger.info("docdb: DocumentDB container for %s ready at %s:%s", lbl, h, p)
        else:
            logger.warning(
                "docdb: DocumentDB container for %s at %s:%s not ready after timeout",
                lbl, h, p)

    threading.Thread(target=_bg_wait, daemon=True).start()


def _delete_db_instance(params):
    """elete a DB instance and release its backing compute."""
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
                f"<DescribeDBInstancesResult><DBInstances>{members}"
                f"</DBInstances></DescribeDBInstancesResult>")


def _modify_db_instance(params):
    """Apply modifyable fields to a DB instance directly (no pending staging)."""
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
            "DBParameterGroupName": _default_parameter_group(new_version),
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
    """Create a DB cluster record; compute starts with its first member."""
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    if not cluster_id:
        return _error("MissingParameter", "DBClusterIdentifier is required", 400)
    if cluster_id in _clusters:
        return _error(
            "DBClusterAlreadyExistsFault", f"DB cluster {cluster_id} already exists.", 400)

    engine = "docdb"
    engine_version = _evaluate_params(params, "EngineVersion") \
        or _default_engine_version(engine)
    version_error = _engine_version_error(engine_version)
    if version_error:
        return version_error
    parameter_group = _evaluate_params(params, "DBClusterParameterGroupName")
    if (parameter_group and not parameter_group.startswith("default.")
            and parameter_group not in _rds._db_cluster_param_groups):
        return _error("DBClusterParameterGroupNotFound",
                      f"DBClusterParameterGroup {parameter_group} not found.", 404)

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
        "EngineLifecycleSupport": (
            _evaluate_params(params, "EngineLifecycleSupport")
            or "open-source-rds-extended-support"),
        "Endpoint": (
            f"{cluster_id}.cluster-{unique_suffix}.{get_region()}.docdb.amazonaws.com"),
        "ReaderEndpoint": (
            f"{cluster_id}.cluster-ro-{unique_suffix}.{get_region()}.docdb.amazonaws.com"),
        "Port": port,
        "MultiAZ": _evaluate_params(params, "MultiAZ") == "true",
        "AvailabilityZones": az_list,
        "DBClusterMembers": [],
        "VpcSecurityGroups": [
            {"VpcSecurityGroupId": sg, "Status": "active"}
            for sg in _parse_member_list(params, "VpcSecurityGroupIds")
        ],
        "DBSubnetGroup": _evaluate_params(params, "DBSubnetGroupName") or "default",
        "DBClusterParameterGroup": (
            _evaluate_params(params, "DBClusterParameterGroupName") or _default_parameter_group(engine_version)),
        "BackupRetentionPeriod": int(_evaluate_params(params, "BackupRetentionPeriod") or "1"),
        "PreferredBackupWindow": _evaluate_params(params, "PreferredBackupWindow") or "03:00-04:00",
        "PreferredMaintenanceWindow": (
            _evaluate_params(params, "PreferredMaintenanceWindow")
            or "sun:05:00-sun:06:00"),
        "ClusterCreateTime": _format_time(now_ts),
        "EarliestRestorableTime": _format_time(now_ts),
        "LatestRestorableTime": _format_time(now_ts),
        "StorageEncrypted": _evaluate_params(params, "StorageEncrypted") == "true",
        "KmsKeyId": _evaluate_params(params, "KmsKeyId") or "",
        "DeletionProtection": _evaluate_params(params, "DeletionProtection") == "true",
        "IAMDatabaseAuthenticationEnabled": (
            _evaluate_params(params, "EnableIAMDatabaseAuthentication") == "true"),
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
                f"<CreateDBClusterResult><DBCluster>{_cluster_xml(cluster)}"
                f"</DBCluster></CreateDBClusterResult>")


def _delete_db_cluster(params):
    """Delete a DB cluster along with its shared container and volume."""
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
                f"<DeleteDBClusterResult><DBCluster>{_cluster_xml(cluster)}"
                f"</DBCluster></DeleteDBClusterResult>")


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
                f"<DescribeDBClustersResult><DBClusters>{members}"
                f"</DBClusters></DescribeDBClustersResult>")


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
        cluster["PreferredMaintenanceWindow"] = _evaluate_params(
            params, "PreferredMaintenanceWindow")
    if _evaluate_params(params, "DeletionProtection"):
        cluster["DeletionProtection"] = _evaluate_params(params, "DeletionProtection") == "true"
    if _evaluate_params(params, "EnableIAMDatabaseAuthentication"):
        cluster["IAMDatabaseAuthenticationEnabled"] = (
            _evaluate_params(params, "EnableIAMDatabaseAuthentication") == "true")
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
                f"<ModifyDBClusterResult><DBCluster>{_cluster_xml(cluster)}"
                f"</DBCluster></ModifyDBClusterResult>")


def _start_db_cluster(params):
    """Start a stopped cluster's compute (restart/recreate the container)."""
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    if cluster.get("Status") != "stopped":
        return _error(
            "InvalidDBClusterStateFault",
            f"DbCluster {cluster_id} is in {cluster.get('Status')} state but "
            "expected it to be one of stopped.",
            400,
        )

    members = _cluster_member_instances(cluster)
    had_compute = bool(cluster.get("_shared_container_id"))
    has_storage = bool(
        cluster.get("_shared_storage_initialized") or cluster.get("_shared_volume_name"))
    docker_client = _get_docker()

    if not docker_client or not (had_compute or has_storage):
        # Control-plane-only, or the cluster never had real compute.
        cluster["_shared_container_ready"] = True
        cluster["Status"] = "available"
        for member in members:
            member["DBInstanceStatus"] = "available"
        return _xml(200, "StartDBClusterResponse",
                    f"<StartDBClusterResult><DBCluster>{_cluster_xml(cluster)}"
                    f"</DBCluster></StartDBClusterResult>")

    if had_compute:
        result = _restart_cluster_shared_container(cluster_id, cluster)
        if result.get("failed"):
            result = _start_cluster_shared_container(cluster_id, cluster, remove_stale=True)
    else:
        result = _start_cluster_shared_container(cluster_id, cluster, remove_stale=True)

    if not result.get("started") and result.get("failed"):
        # Compute did not come back; keep everything stopped so Start can retry.
        return _error(
            "InternalFailure", f"Failed to start compute for DB cluster {cluster_id}.", 500)

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
                f"<StartDBClusterResult><DBCluster>{_cluster_xml(cluster)}"
                f"</DBCluster></StartDBClusterResult>")


def _stop_db_cluster(params):
    """Stop a cluster's shared DocumentDB container, preserving it and its volume."""
    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    if cluster.get("Status") != "available":
        return _error(
            "InvalidDBClusterStateFault",
            f"DbCluster {cluster_id} is in {cluster.get('Status')} state but "
            "expected it to be one of available.",
            400,
        )
    _stop_cluster_shared_container(cluster_id, cluster)
    cluster["_shared_container_ready"] = False
    cluster["Status"] = "stopped"
    for member in _cluster_member_instances(cluster):
        member["DBInstanceStatus"] = "stopped"
    return _xml(200, "StopDBClusterResponse",
                f"<StopDBClusterResult><DBCluster>{_cluster_xml(cluster)}"
                f"</DBCluster></StopDBClusterResult>")


def _failover_db_cluster(params):
    """Rotate IsClusterWriter to the next member (lowest PromotionTier)."""
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
                f"<FailoverDBClusterResult><DBCluster>{_cluster_xml(response_cluster)}"
                f"</DBCluster></FailoverDBClusterResult>")


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tags_resource_not_found(arn):
    """Build the documented not-found fault for an unknown tags target ARN."""
    for cl in _clusters.values():
        if cl.get("DBClusterArn") == arn:
            return None
    for inst in _instances.values():
        if inst.get("DBInstanceArn") == arn:
            return None
    parts = arn.split(":")
    kind = parts[5] if len(parts) >= 7 and parts[0] == "arn" else ""
    if kind == "db":
        return _error("DBInstanceNotFound", f"DBInstance {arn} not found.", 404)
    if kind in ("snapshot", "cluster-snapshot"):
        return _error("DBSnapshotNotFound", f"DBSnapshot {arn} not found.", 404)
    if kind == "subgrp":
        return _error(
            "DBSubnetGroupNotFoundFault", f"Subnet group {arn} not found.", 404)
    if kind in ("cluster-pg", "pg"):
        return _error(
            "DBParameterGroupNotFound",
            f"DB cluster parameter group {arn} not found.",
            404,
        )
    return _error("DBClusterNotFoundFault", f"DBCluster {arn} not found.", 404)


def _add_tags(params):
    """Add or overwrite tags on a resource identified by ARN."""
    arn = _evaluate_params(params, "ResourceName")
    if not arn:
        return _error("MissingParameter", "ResourceName is required", 400)
    not_found = _tags_resource_not_found(arn)
    if not_found:
        return not_found

    new_tags = _parse_tags(params)
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
    if not arn:
        return _error("MissingParameter", "ResourceName is required", 400)
    not_found = _tags_resource_not_found(arn)
    if not_found:
        return not_found

    keys_to_remove = set(_parse_member_list(params, "TagKeys"))
    existing = _tags.get(arn, [])
    _tags[arn] = [t for t in existing if t["Key"] not in keys_to_remove]

    _sync_tag_list_to_resource(arn)
    return _xml(200, "RemoveTagsFromResourceResponse", "")


def _list_tags(params):
    """List tags on a resource identified by ARN."""
    arn = _evaluate_params(params, "ResourceName")
    if not arn:
        return _error("MissingParameter", "ResourceName is required", 400)
    not_found = _tags_resource_not_found(arn)
    if not_found:
        return not_found

    tag_list = _tags.get(arn, [])
    members = "".join(
        f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>"
        for t in tag_list)
    return _xml(200, "ListTagsForResourceResponse",
                f"<ListTagsForResourceResult><TagList>{members}"
                f"</TagList></ListTagsForResourceResult>")


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


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def _xml(status, root_tag, inner):
    """Wrap rendered inner fields in a Query-API response document."""
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    """Build an RDS-style XML error response."""
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


def _subnet_az_name(subnet_entry):
    """AZ name for a subnet record, defaulting to the region."""
    zone = subnet_entry.get("SubnetAvailabilityZone")
    if isinstance(zone, dict) and zone.get("Name"):
        return zone["Name"]
    return f"{get_region()}a"


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

    db_sg_xml = "".join(f"""<DBSecurityGroup>
            <DBSecurityGroupName>{sg}</DBSecurityGroupName>
            <Status>active</Status>
        </DBSecurityGroup>""" for sg in i.get("DBSecurityGroups", []))

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

    read_replica_xml = "".join(
        f"<ReadReplicaDBInstanceIdentifier>{rr}</ReadReplicaDBInstanceIdentifier>"
        for rr in i.get("ReadReplicaDBInstanceIdentifiers", []))

    subnet_xml = ""
    for s in subnet.get("Subnets", []):
        az = _subnet_az_name(s)
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

    backup_window = i.get("PreferredBackupWindow", "03:00-04:00")
    maintenance_window = i.get("PreferredMaintenanceWindow", "sun:05:00-sun:06:00")
    latest_restore = i.get("LatestRestorableTime") or _format_time(time.time())
    read_replica_source = i.get("ReadReplicaSourceDBInstanceIdentifier", "")
    ca_certificate = i.get("CACertificateIdentifier", "rds-ca-rsa2048-g1")
    iam_auth = str(i.get("IAMDatabaseAuthenticationEnabled", False)).lower()

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
        <PreferredBackupWindow>{backup_window}</PreferredBackupWindow>
        <BackupRetentionPeriod>{i.get('BackupRetentionPeriod', 1)}</BackupRetentionPeriod>
        <DBSecurityGroups>{db_sg_xml}</DBSecurityGroups>
        <VpcSecurityGroups>{vpc_sg_xml}</VpcSecurityGroups>
        <DBParameterGroups>{param_xml}</DBParameterGroups>
        <AvailabilityZone>{i.get('AvailabilityZone', f'{get_region()}a')}</AvailabilityZone>
        <DBSubnetGroup>
            <DBSubnetGroupName>{subnet.get('DBSubnetGroupName', 'default')}</DBSubnetGroupName>
            <DBSubnetGroupDescription>{subnet.get('DBSubnetGroupDescription', '')}
            </DBSubnetGroupDescription>
            <VpcId>{subnet.get('VpcId', 'vpc-00000000')}</VpcId>
            <SubnetGroupStatus>{subnet.get('SubnetGroupStatus', 'Complete')}</SubnetGroupStatus>
            <Subnets>{subnet_xml}</Subnets>
            <DBSubnetGroupArn>{subnet.get('DBSubnetGroupArn', '')}</DBSubnetGroupArn>
        </DBSubnetGroup>
        <PreferredMaintenanceWindow>{maintenance_window}</PreferredMaintenanceWindow>
        <PendingModifiedValues>{pending_xml}</PendingModifiedValues>
        <LatestRestorableTime>{latest_restore}</LatestRestorableTime>
        <MultiAZ>{str(i.get('MultiAZ', False)).lower()}</MultiAZ>
        <AutoMinorVersionUpgrade>
            {str(i.get('AutoMinorVersionUpgrade', True)).lower()}
        </AutoMinorVersionUpgrade>
        <ReadReplicaDBInstanceIdentifiers>{read_replica_xml}</ReadReplicaDBInstanceIdentifiers>
        <ReadReplicaSourceDBInstanceIdentifier>{read_replica_source}
        </ReadReplicaSourceDBInstanceIdentifier>
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
        <CACertificateIdentifier>{ca_certificate}</CACertificateIdentifier>
        <DomainMemberships/>
        <CopyTagsToSnapshot>{str(i.get('CopyTagsToSnapshot', False)).lower()}</CopyTagsToSnapshot>
        <MonitoringInterval>{i.get('MonitoringInterval', 0)}</MonitoringInterval>
        <EnhancedMonitoringResourceArn>
            {i.get('EnhancedMonitoringResourceArn', '')}
        </EnhancedMonitoringResourceArn>
        <MonitoringRoleArn>{i.get('MonitoringRoleArn', '')}</MonitoringRoleArn>
        <PromotionTier>{i.get('PromotionTier', 1)}</PromotionTier>
        <DBInstanceArn>{i['DBInstanceArn']}</DBInstanceArn>
        <IAMDatabaseAuthenticationEnabled>{iam_auth}
        </IAMDatabaseAuthenticationEnabled>
        <PerformanceInsightsEnabled>
            {str(i.get('PerformanceInsightsEnabled', False)).lower()}
        </PerformanceInsightsEnabled>
        <EnabledCloudwatchLogsExports/>
        <ProcessorFeatures/>
        <DeletionProtection>{str(i.get('DeletionProtection', False)).lower()}</DeletionProtection>
        <AssociatedRoles/>
        <MaxAllocatedStorage>
            {i.get('MaxAllocatedStorage', i.get('AllocatedStorage', 20))}
        </MaxAllocatedStorage>
        <TagList>{tag_xml}</TagList>
        {cert_xml}
        <CustomerOwnedIpEnabled>
            {str(i.get('CustomerOwnedIpEnabled', False)).lower()}
        </CustomerOwnedIpEnabled>
        <BackupTarget>{i.get('BackupTarget', 'region')}</BackupTarget>
        <NetworkType>{i.get('NetworkType', 'IPV4')}</NetworkType>
        <StorageThroughput>{i.get('StorageThroughput', 0)}</StorageThroughput>
        <IsStorageConfigUpgradeAvailable>
            {str(i.get('IsStorageConfigUpgradeAvailable', False)).lower()}
        </IsStorageConfigUpgradeAvailable>"""


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

    az_xml = "".join(f"<AvailabilityZone>{az}</AvailabilityZone>"
                     for az in c.get("AvailabilityZones", []))

    tag_xml = ""
    for t in c.get("TagList", []):
        tag_xml += f"<Tag><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></Tag>"

    db_name = c.get("DatabaseName")
    db_name_xml = f"<DatabaseName>{db_name}</DatabaseName>" if db_name else ""

    # AWS emits <MasterUserSecret> only for clusters with a managed master
    # user password (CDK's ManageMasterUserPassword / MasterUserSecretArn).
    master_user_secret = c.get("MasterUserSecret")
    master_user_secret_xml = ""
    if master_user_secret:
        master_user_secret_xml = (
            "<MasterUserSecret>"
            f"<SecretArn>{master_user_secret.get('SecretArn', '')}</SecretArn>"
            f"<SecretStatus>{master_user_secret.get('SecretStatus', 'active')}</SecretStatus>"
            "</MasterUserSecret>"
        )

    backup_window = c.get("PreferredBackupWindow", "03:00-04:00")
    maintenance_window = c.get("PreferredMaintenanceWindow", "sun:05:00-sun:06:00")
    iam_auth = str(c.get("IAMDatabaseAuthenticationEnabled", False)).lower()
    lifecycle = c.get("EngineLifecycleSupport", "open-source-rds-extended-support")

    return f"""<DBClusterIdentifier>{c['DBClusterIdentifier']}</DBClusterIdentifier>
        <DBClusterArn>{c['DBClusterArn']}</DBClusterArn>
        <Engine>{c['Engine']}</Engine>
        <EngineVersion>{c['EngineVersion']}</EngineVersion>
        <EngineMode>{c.get('EngineMode', 'provisioned')}</EngineMode>
        <Status>{c['Status']}</Status>
        <MasterUsername>{c.get('MasterUsername', 'root')}</MasterUsername>
        {master_user_secret_xml}
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
        <PreferredBackupWindow>{backup_window}</PreferredBackupWindow>
        <PreferredMaintenanceWindow>{maintenance_window}</PreferredMaintenanceWindow>
        <ClusterCreateTime>{c.get('ClusterCreateTime', '')}</ClusterCreateTime>
        <EarliestRestorableTime>{c.get('EarliestRestorableTime', '')}</EarliestRestorableTime>
        <LatestRestorableTime>{c.get('LatestRestorableTime', '')}</LatestRestorableTime>
        <StorageEncrypted>{str(c.get('StorageEncrypted', False)).lower()}</StorageEncrypted>
        <KmsKeyId>{c.get('KmsKeyId', '')}</KmsKeyId>
        <DeletionProtection>{str(c.get('DeletionProtection', False)).lower()}</DeletionProtection>
        <IAMDatabaseAuthenticationEnabled>{iam_auth}
        </IAMDatabaseAuthenticationEnabled>
        <HttpEndpointEnabled>
            {str(c.get('HttpEndpointEnabled', False)).lower()}
        </HttpEndpointEnabled>
        <CopyTagsToSnapshot>{str(c.get('CopyTagsToSnapshot', False)).lower()}</CopyTagsToSnapshot>
        <CrossAccountClone>{str(c.get('CrossAccountClone', False)).lower()}</CrossAccountClone>
        <DbClusterResourceId>{c.get('DbClusterResourceId', '')}</DbClusterResourceId>
        <HostedZoneId>{c.get('HostedZoneId', 'Z2R2ITUGPM61AM')}</HostedZoneId>
        <AssociatedRoles/>
        <TagList>{tag_xml}</TagList>
        <AllocatedStorage>{c.get('AllocatedStorage', 1)}</AllocatedStorage>
        <ActivityStreamStatus>{c.get('ActivityStreamStatus', 'stopped')}</ActivityStreamStatus>
        <NetworkType>{c.get('NetworkType', 'IPV4')}</NetworkType>
        <EngineLifecycleSupport>{lifecycle}</EngineLifecycleSupport>"""


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
    """Parse list params in either Prefix.member.N or Prefix.<MemberName>.N form."""
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
    pattern = re.compile(rf"^{re.escape(prefix)}\.([^.]+)\.(\d+)$")
    numbered = {}
    for key in params:
        m = pattern.match(key)
        if m:
            idx = int(m.group(2))
            numbered[idx] = _evaluate_params(params, key)
    return [numbered[k] for k in sorted(numbered)] if numbered else []


def _parse_filters(params):
    """
    Parse request filters in either ``Filters.Filter.N`` or ``Filters.member.N`` wire form (botocore emits one
    or the other depending on the model's locationName).
    """
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
    """Look up an instance by DBInstanceIdentifier or DbiResourceId."""
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
    """Stop/remove every docdb container (cluster-owned and standalone), then clear all state."""
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
                        logger.warning(
                            "reset: failed to stop/remove docdb container %s: %s", cid, e)
        _instances.clear()
        _clusters.clear()
        _tags.clear()
        _port_counter[0] = BASE_PORT


# ---------------------------------------------------------------------------
# Action map
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    "CreateDBInstance": _create_db_instance,
    "DeleteDBInstance": _delete_db_instance,
    "DescribeDBInstances": _describe_db_instances,
    "ModifyDBInstance": _modify_db_instance,
    "RebootDBInstance": _reboot_db_instance,
    "CreateDBCluster": _create_db_cluster,
    "DeleteDBCluster": _delete_db_cluster,
    "DescribeDBClusters": _describe_db_clusters,
    "ModifyDBCluster": _modify_db_cluster,
    "StartDBCluster": _start_db_cluster,
    "StopDBCluster": _stop_db_cluster,
    "FailoverDBCluster": _failover_db_cluster,
    "AddTagsToResource": _add_tags,
    "RemoveTagsFromResource": _remove_tags,
    "ListTagsForResource": _list_tags,
}


def _live_container_ids():
    """Container ids still owned by a live instance or cluster."""
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
