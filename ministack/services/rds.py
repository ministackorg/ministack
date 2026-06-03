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
          RemoveFromGlobalCluster, ModifyGlobalCluster.

When Docker is available, CreateDBInstance spins up a real Postgres/MySQL container
and returns the actual host:port as the endpoint.

JSON request bodies (``application/x-amz-json-1.*``, SigV4 JSON) are accepted for the
same actions as the legacy Query API form body, so Terraform / current botocore
clients can call DescribeDBInstances and other operations without ``Action=`` query
parameters.
"""

import copy
import datetime
import contextvars
import json
import logging
import os
import socket
import threading
import time
from urllib.parse import parse_qs
from xml.sax.saxutils import escape as _esc

from ministack.core.persistence import load_state
from ministack.core.responses import AccountScopedDict, apply_image_prefix, get_account_id, get_region, new_uuid

logger = logging.getLogger("rds")

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

_instances = AccountScopedDict()
_clusters = AccountScopedDict()
_subnet_groups = AccountScopedDict()
_param_groups = AccountScopedDict()
_snapshots = AccountScopedDict()
_db_cluster_param_groups = AccountScopedDict()
_db_cluster_snapshots = AccountScopedDict()
_option_groups = AccountScopedDict()
_global_clusters = AccountScopedDict()
_tags = AccountScopedDict()
_port_counter = [BASE_PORT]

_docker = None
_ministack_network = None


# ── Persistence ────────────────────────────────────────────

def get_state():
    instances = copy.deepcopy(_instances)
    # Strip Docker container IDs (not restorable across restarts)
    for key in list(instances._data):
        instances._data[key].pop("_docker_container_id", None)
    state = {
        "instances": instances,
        "clusters": copy.deepcopy(_clusters),
        "subnet_groups": copy.deepcopy(_subnet_groups),
        "param_groups": copy.deepcopy(_param_groups),
        "snapshots": copy.deepcopy(_snapshots),
        "db_cluster_param_groups": copy.deepcopy(_db_cluster_param_groups),
        "db_cluster_snapshots": copy.deepcopy(_db_cluster_snapshots),
        "option_groups": copy.deepcopy(_option_groups),
        "global_clusters": copy.deepcopy(_global_clusters),
        "tags": copy.deepcopy(_tags),
        "port_counter": _port_counter[0],
    }
    return state


def restore_state(data):
    if not data:
        return
    _clusters.update(data.get("clusters", {}))
    _subnet_groups.update(data.get("subnet_groups", {}))
    _param_groups.update(data.get("param_groups", {}))
    _snapshots.update(data.get("snapshots", {}))
    _db_cluster_param_groups.update(data.get("db_cluster_param_groups", {}))
    _db_cluster_snapshots.update(data.get("db_cluster_snapshots", {}))
    _option_groups.update(data.get("option_groups", {}))
    _global_clusters.update(data.get("global_clusters", {}))
    _tags.update(data.get("tags", {}))
    if "port_counter" in data:
        _port_counter[0] = data["port_counter"]
    instances_data = data.get("instances", {})
    to_respawn = []
    if isinstance(instances_data, AccountScopedDict):
        # New format: AccountScopedDict with full multi-account data
        for key, inst in list(instances_data._data.items()):
            inst["_docker_container_id"] = None
            inst["DBInstanceStatus"] = "creating"
            _instances._data[key] = inst
            to_respawn.append((key[0], inst.get("DBInstanceIdentifier") or key[1], inst))
    else:
        # Legacy format: plain dict keyed by instance name
        for name, inst in instances_data.items():
            inst["_docker_container_id"] = None
            inst["DBInstanceStatus"] = "creating"
            _instances[name] = inst
            to_respawn.append((None, name, inst))

    # Re-spin backing containers for persisted instances. Mirrors the MWAA
    # restore pattern: persistence saves the instance metadata but the Docker
    # container itself is killed by the host restart, so the restore path has
    # to bring it back. Without this, restored instances stay marked
    # "available" with no running container, and StartDBInstance is
    # metadata-only so it can't recover them either.
    from ministack.core.responses import _request_account_id
    for account_id, db_id, inst in to_respawn:
        ctx = contextvars.copy_context()

        def _runner(account_id=account_id, db_id=db_id, inst=inst):
            if account_id is not None:
                _request_account_id.set(account_id)
            _start_rds_container_for_instance(db_id, inst)

        threading.Thread(target=ctx.run, args=(_runner,), daemon=True).start()


def _start_rds_container_for_instance(db_id, instance):
    """Re-spin (or re-attach to) the Docker container for a restored instance.

    Reads engine, credentials, and endpoint info from the persisted instance
    dict instead of CreateDBInstance request params. If a container with the
    deterministic name ``ministack-rds-{db_id}`` already exists (e.g. host
    rebooted but Docker preserved stopped containers), it is removed first so
    a clean run can attach to the persistent named volume. Sets
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

    container_name = f"ministack-rds-{db_id}"
    try:
        existing = docker_client.containers.get(container_name)
        # `force=True` stops AND removes in one shot, including
        # half-spawned "Created" containers that didn't fully start
        # — those still hold port mappings and would collide with
        # the next `containers.run` (#692 follow-up: doodaz saw
        # a `Created` container blocking the bind).
        try:
            existing.remove(force=True, v=False)
        except Exception as e:
            logger.warning("RDS: failed to remove stale container %s: %s",
                           container_name, e)
        # Verify the name is actually free now; if removal silently
        # failed, abort respawn rather than crash inside `containers.run`
        # with a confusing name-conflict error.
        try:
            docker_client.containers.get(container_name)
            logger.warning("RDS: stale container %s still present after "
                           "force-remove — aborting respawn", container_name)
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
        labels={"ministack": "rds", "db_id": db_id},
    )
    if ms_network:
        container_kwargs["network"] = ms_network
    if RDS_PERSIST:
        container_kwargs["volumes"] = {
            f"ministack-rds-{db_id}-data": {"bind": data_path, "mode": "rw"},
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

    instance["_docker_container_id"] = container.id

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
    instance["DBInstanceStatus"] = "available"
    logger.info("RDS: respawned container %s for instance %s",
                container_name, db_id)


def _get_docker():
    global _docker
    if _docker is None:
        try:
            import docker
            _docker = docker.from_env()
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
    """Single auth-probe attempt. Returns True on success, False on a
    transient failure. TCP readiness alone is not enough for MySQL/Postgres
    images — they accept sockets before bootstrap creates users/databases —
    so we open and close an authenticated connection. When the DB driver
    isn't installed (lightweight image) we fall back to a one-shot TCP check.
    """
    try:
        if _is_mysql_engine(engine):
            try:
                import pymysql
            except ImportError:
                return _wait_for_port(host, port, timeout=1)
            conn = pymysql.connect(
                host=host, port=int(port), user="root",
                password=password, database=db_name or None,
                connect_timeout=2, read_timeout=2, write_timeout=2,
                autocommit=True)
        elif _is_postgres_engine(engine):
            try:
                import psycopg2
            except ImportError:
                return _wait_for_port(host, port, timeout=1)
            conn = psycopg2.connect(
                host=host, port=int(port), user=user,
                password=password, dbname=db_name or "postgres",
                connect_timeout=2)
        else:
            return _wait_for_port(host, port, timeout=1)
        conn.close()
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
    if cluster.get("Status") in ("stopped", "deleting"):
        return
    member_ids = {
        m.get("DBInstanceIdentifier")
        for m in cluster.get("DBClusterMembers", [])
        if m.get("DBInstanceIdentifier")
    }
    if any(
        inst.get("DBInstanceIdentifier") in member_ids
        and inst.get("DBInstanceStatus") != "available"
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
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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


async def handle_request(method, path, headers, body, query_params):
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


def _sync_cluster_endpoints(cluster):
    """Point Aurora cluster endpoints at reachable local DB instance endpoints."""
    members = cluster.get("DBClusterMembers") or []
    if not members:
        return

    writer = next((m for m in members if m.get("IsClusterWriter")), members[0])
    writer_inst = _instances.get(writer.get("DBInstanceIdentifier"))
    reader_member = next((m for m in members if not m.get("IsClusterWriter")), writer)
    reader_inst = _instances.get(reader_member.get("DBInstanceIdentifier")) or writer_inst

    if writer_inst and writer_inst.get("Endpoint"):
        writer_ep = writer_inst["Endpoint"]
        cluster["Endpoint"] = writer_ep.get("Address", cluster.get("Endpoint", ""))
        cluster["Port"] = int(writer_ep.get("Port", cluster.get("Port", 0)))
    if reader_inst and reader_inst.get("Endpoint"):
        cluster["ReaderEndpoint"] = reader_inst["Endpoint"].get(
            "Address", cluster.get("ReaderEndpoint", ""))


def _register_instance_in_cluster(instance):
    """Append instance to parent cluster ``DBClusterMembers`` (Aurora parity)."""
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
    is_writer = not any_writer
    members.append({
        "DBInstanceIdentifier": db_id,
        "IsClusterWriter": is_writer,
        "PromotionTier": int(instance.get("PromotionTier", 1)),
    })
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
    db_id = _p(p, "DBInstanceIdentifier")
    if not db_id:
        return _error("MissingParameter", "DBInstanceIdentifier is required", 400)
    if db_id in _instances:
        return _error("DBInstanceAlreadyExistsFault", f"DB instance {db_id} already exists", 400)

    engine = _p(p, "Engine") or "postgres"
    engine_version = _p(p, "EngineVersion") or _default_engine_version(engine)
    db_class = _p(p, "DBInstanceClass") or "db.t3.micro"
    master_user = _p(p, "MasterUsername") or "admin"
    master_pass = _p(p, "MasterUserPassword") or "password"
    db_name = _p(p, "DBName") or ""
    port = int(_p(p, "Port") or _default_port(engine))

    # Inherit credentials from cluster when instance is a cluster member.
    cluster_id_param = _p(p, "DBClusterIdentifier")
    if cluster_id_param and cluster_id_param in _clusters:
        parent = _clusters[cluster_id_param]
        if not _p(p, "MasterUsername"):
            master_user = parent.get("MasterUsername", master_user)
        if not _p(p, "MasterUserPassword"):
            master_pass = parent.get("_MasterUserPassword", master_pass)
        if not db_name:
            db_name = parent.get("DatabaseName", "")
    if not db_name:
        db_name = "mydb"
    allocated_storage = int(_p(p, "AllocatedStorage") or "20")
    storage_type = _p(p, "StorageType") or "gp2"
    subnet_group_name = _p(p, "DBSubnetGroupName") or "default"

    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:db:{db_id}"
    dbi_resource_id = f"db-{new_uuid().replace('-', '')[:20].upper()}"
    endpoint_host = _MINISTACK_HOST
    endpoint_port = port
    host_port = None
    docker_container_id = None
    internal_host = None
    internal_port = None
    real_container_started = False
    readiness_host = None
    readiness_port = None

    docker_client = _get_docker()
    if docker_client:
        ms_network = _get_ministack_network(docker_client)
        image, env, container_port, data_path = _docker_image_for_engine(
            engine, engine_version, master_user, master_pass, db_name
        )
        if image:
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
                    name=f"ministack-rds-{db_id}",
                    labels={"ministack": "rds", "db_id": db_id},
                )
                if ms_network:
                    container_kwargs["network"] = ms_network
                # Mount only the engine-appropriate data path. Previously both
                # postgres and mysql paths were mounted unconditionally, which
                # is harmless but wasteful and complicates the Postgres 18+
                # layout change (where the path differs from earlier majors).
                if RDS_PERSIST:
                    container_kwargs["volumes"] = {
                        f"ministack-rds-{db_id}-data": {"bind": data_path, "mode": "rw"},
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

    cluster_id = _p(p, "DBClusterIdentifier")
    param_group_name = _p(p, "DBParameterGroupName") or f"default.{engine}{engine_version.split('.')[0]}"
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

    instance = {
        "DBInstanceIdentifier": db_id,
        "DBInstanceClass": db_class,
        "Engine": engine,
        "EngineVersion": engine_version,
        "DBInstanceStatus": "creating" if real_container_started else "available",
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
        "_internal_address": internal_host,
        "_internal_port": internal_port,
        "_MasterUserPassword": master_pass,
    }
    _instances[db_id] = instance
    _register_instance_in_cluster(instance)

    if real_container_started:
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
            master_user=master_user, master_pass=master_pass,
            db_name=db_name, ready_host=ready_host, ready_port=ready_port,
            ms_network=ms_network, internal_host=internal_host,
            internal_port=internal_port, endpoint_port=endpoint_port,
            container_id=docker_container_id,
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
            if not _wait_for_database_ready(
                ready_host, ready_port, engine, master_user,
                master_pass, db_name, _container_alive,
            ):
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
                    ready_host, ready_port, master_user, master_pass, db_id,
                )
            inst = _instances.get(db_id)
            if inst is not None:
                inst["DBInstanceStatus"] = "available"
            if cluster_id:
                cluster = _clusters.get(cluster_id)
                if cluster:
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
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)

    _unregister_instance_from_clusters(db_id)

    if instance.get("DeletionProtection"):
        return _error("InvalidParameterCombination",
            "Cannot delete a DB instance when DeletionProtection is enabled.", 400)

    docker_client = _get_docker()
    if docker_client and instance.get("_docker_container_id"):
        try:
            c = docker_client.containers.get(instance["_docker_container_id"])
            c.stop(timeout=5)
            c.remove(v=True)
            logger.info("RDS: removed container for %s", db_id)
        except Exception as e:
            logger.warning("RDS: failed to remove container for %s: %s", db_id, e)

    skip_snapshot = _p(p, "SkipFinalSnapshot") == "true"
    final_snap_id = _p(p, "FinalDBSnapshotIdentifier")
    if not skip_snapshot and final_snap_id:
        _create_snapshot_internal(final_snap_id, instance)

    instance["DBInstanceStatus"] = "deleting"
    arn = instance["DBInstanceArn"]
    _tags.pop(arn, None)
    del _instances[db_id]
    return _single_instance_response("DeleteDBInstanceResponse", "DeleteDBInstanceResult", instance)


def _describe_db_instances(p):
    db_id = _p(p, "DBInstanceIdentifier")
    if db_id:
        instance = _resolve_instance(db_id)
        if not instance:
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
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)

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

    new_pass = _p(p, "MasterUserPassword")
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
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "available"
    return _single_instance_response("StartDBInstanceResponse", "StartDBInstanceResult", instance)


def _stop_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "stopped"
    return _single_instance_response("StopDBInstanceResponse", "StopDBInstanceResult", instance)


def _reboot_db_instance(p):
    db_id = _p(p, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
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
        return _error("DBInstanceNotFound", f"DBInstance {source_id} not found.", 404)
    if replica_id in _instances:
        return _error("DBInstanceAlreadyExistsFault", f"DBInstance {replica_id} already exists.", 400)

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
        return _error("DBInstanceAlreadyExistsFault", f"DBInstance {db_id} already exists.", 400)

    snap = _snapshots.get(snap_id)
    if not snap:
        return _error("DBSnapshotNotFound", f"DBSnapshot {snap_id} not found.", 404)

    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:db:{db_id}"
    instance = {
        "DBInstanceIdentifier": db_id,
        "DBInstanceClass": _p(p, "DBInstanceClass") or snap.get("DBInstanceClass", "db.t3.micro"),
        "Engine": snap.get("Engine", "postgres"),
        "EngineVersion": snap.get("EngineVersion", "15.3"),
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

    engine = _p(p, "Engine") or "aurora-postgresql"
    engine_version = _p(p, "EngineVersion") or _default_engine_version(engine)
    port = int(_p(p, "Port") or _default_port(engine))
    master_user = _p(p, "MasterUsername") or "admin"
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:cluster:{cluster_id}"
    unique_suffix = new_uuid()[:8]
    now_ts = time.time()

    vpc_sgs = _parse_member_list(p, "VpcSecurityGroupIds")
    vpc_sg_list = [{"VpcSecurityGroupId": sg, "Status": "active"} for sg in vpc_sgs] if vpc_sgs else []
    az_list = _parse_member_list(p, "AvailabilityZones")
    if not az_list:
        az_list = [f"{get_region()}a", f"{get_region()}b", f"{get_region()}c"]

    master_pass = _p(p, "MasterUserPassword") or "password"

    cluster = {
        "DBClusterIdentifier": cluster_id,
        "DBClusterArn": arn,
        "Engine": engine,
        "EngineVersion": engine_version,
        "EngineMode": _p(p, "EngineMode") or "provisioned",
        "Status": "available",
        "MasterUsername": master_user,
        "_MasterUserPassword": master_pass,
        "DatabaseName": _p(p, "DatabaseName") or None,
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
    }
    _clusters[cluster_id] = cluster

    req_tags = _parse_tags(p)
    if req_tags:
        _tags[arn] = req_tags
        cluster["TagList"] = req_tags

    return _xml(200, "CreateDBClusterResponse",
        f"<CreateDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></CreateDBClusterResult>")


def _delete_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    if cluster.get("DeletionProtection"):
        return _error("InvalidParameterCombination",
            "Cannot delete a DB cluster when DeletionProtection is enabled.", 400)

    skip_snapshot = _p(p, "SkipFinalSnapshot") == "true"
    final_snap_id = _p(p, "FinalDBSnapshotIdentifier")
    if not skip_snapshot and final_snap_id:
        pass

    cluster["Status"] = "deleting"
    _tags.pop(cluster["DBClusterArn"], None)
    del _clusters[cluster_id]
    return _xml(200, "DeleteDBClusterResponse",
        f"<DeleteDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></DeleteDBClusterResult>")


def _describe_db_clusters(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    if cluster_id:
        cluster = _clusters.get(cluster_id)
        if not cluster:
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


def _rotate_real_password(cluster, old_pass, new_pass):
    """Alter the root password on the real MySQL/MariaDB container."""
    cluster_id = cluster.get("DBClusterIdentifier", "")
    for inst in _instances.values():
        if inst.get("DBClusterIdentifier") != cluster_id:
            continue
        engine = inst.get("Engine", "")
        if not any(e in engine for e in ("mysql", "aurora-mysql", "mariadb")):
            continue
        host = inst.get("_internal_address")
        port = inst.get("_internal_port")
        if not host or not port:
            endpoint = inst.get("Endpoint", {})
            if not isinstance(endpoint, dict) or not endpoint.get("Port"):
                continue
            host = endpoint.get("Address", "localhost")
            port = int(endpoint.get("Port", 3306))
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
            logger.info("RDS: rotated root password on %s", cluster_id)
        except Exception as e:
            logger.warning("RDS: password rotation failed on %s: %s",
                           cluster_id, e)
        break


def _modify_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    if _p(p, "EngineVersion"):
        cluster["EngineVersion"] = _p(p, "EngineVersion")
    if _p(p, "MasterUserPassword"):
        new_pass = _p(p, "MasterUserPassword")
        old_pass = cluster.get("_MasterUserPassword", "password")
        cluster["_MasterUserPassword"] = new_pass
        _rotate_real_password(cluster, old_pass, new_pass)
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
            snaps = [s for s in snaps if s["DBInstanceIdentifier"] == db_id]
        if snap_type:
            snaps = [s for s in snaps if s["SnapshotType"] == snap_type]

    members = "".join(f"<DBSnapshot>{_snapshot_xml(s)}</DBSnapshot>" for s in snaps)
    return _xml(200, "DescribeDBSnapshotsResponse",
        f"<DescribeDBSnapshotsResult><DBSnapshots>{members}</DBSnapshots></DescribeDBSnapshotsResult>")


# ---------------------------------------------------------------------------
# Subnet Groups
# ---------------------------------------------------------------------------

def _create_subnet_group(p):
    name = _p(p, "DBSubnetGroupName")
    if not name:
        return _error("MissingParameter", "DBSubnetGroupName is required", 400)
    desc = _p(p, "DBSubnetGroupDescription") or name
    subnet_ids = _parse_member_list(p, "SubnetIds")
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
        params_xml += f"""<Parameter>
            <ParameterName>{pname}</ParameterName>
            <ParameterValue>{value}</ParameterValue>
            <Description>{_esc(param.get('description', ''))}</Description>
            <Source>{source}</Source>
            <ApplyType>{param.get('apply_type', 'dynamic')}</ApplyType>
            <DataType>{param.get('data_type', 'string')}</DataType>
            <IsModifiable>{str(param.get('modifiable', True)).lower()}</IsModifiable>
            <ApplyMethod>{apply_method}</ApplyMethod>
        </Parameter>"""
    # Include custom parameters not in the defaults
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
        params_xml += f"""<Parameter>
            <ParameterName>{pname}</ParameterName>
            <ParameterValue>{value}</ParameterValue>
            <Description></Description>
            <Source>user</Source>
            <ApplyType>dynamic</ApplyType>
            <DataType>string</DataType>
            <IsModifiable>true</IsModifiable>
            <ApplyMethod>{apply_method}</ApplyMethod>
        </Parameter>"""

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
    params = pg.get("Parameters", {})
    # When filtering by source, treat all stored params as "user" source.
    # If filter is "engine-default" and we have no defaults list, return empty.
    if source_filter and source_filter != "user":
        params = {}
    if not params:
        return _xml(200, "DescribeDBClusterParametersResponse",
            "<DescribeDBClusterParametersResult><Parameters/></DescribeDBClusterParametersResult>")
    members = []
    for pname, pinfo in params.items():
        pvalue = pinfo.get("ParameterValue", "")
        apply_method = pinfo.get("ApplyMethod", "immediate")
        members.append(
            f"<Parameter>"
            f"<ParameterName>{pname}</ParameterName>"
            f"<ParameterValue>{pvalue}</ParameterValue>"
            f"<Source>user</Source>"
            f"<ApplyMethod>{apply_method}</ApplyMethod>"
            f"<IsModifiable>true</IsModifiable>"
            f"<ApplyType>dynamic</ApplyType>"
            f"</Parameter>"
        )
    return _xml(200, "DescribeDBClusterParametersResponse",
        f"<DescribeDBClusterParametersResult><Parameters>{''.join(members)}</Parameters></DescribeDBClusterParametersResult>")


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

    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

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
            snaps = [s for s in snaps if s["DBClusterIdentifier"] == cluster_id]
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

    if _p(p, "DBSubnetGroupDescription"):
        sg["DBSubnetGroupDescription"] = _p(p, "DBSubnetGroupDescription")

    subnet_ids = _parse_member_list(p, "SubnetIds")
    if subnet_ids:
        sg["Subnets"] = [
            {"SubnetIdentifier": sid, "SubnetAvailabilityZone": {"Name": f"{get_region()}a"},
             "SubnetOutpost": {}, "SubnetStatus": "Active"} for sid in subnet_ids
        ]

    return _xml(200, "ModifyDBSubnetGroupResponse",
        f"<ModifyDBSubnetGroupResult><DBSubnetGroup>{_subnet_group_xml(sg)}</DBSubnetGroup></ModifyDBSubnetGroupResult>")


# ---------------------------------------------------------------------------
# StartDBCluster / StopDBCluster
# ---------------------------------------------------------------------------

def _start_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    cluster["Status"] = "available"
    return _xml(200, "StartDBClusterResponse",
        f"<StartDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StartDBClusterResult>")


def _stop_db_cluster(p):
    cluster_id = _p(p, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    cluster["Status"] = "stopped"
    return _xml(200, "StopDBClusterResponse",
        f"<StopDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StopDBClusterResult>")


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

def _add_tags(p):
    arn = _p(p, "ResourceName")
    new_tags = _parse_tags(p)
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


def _remove_tags(p):
    arn = _p(p, "ResourceName")
    keys_to_remove = set(_parse_member_list(p, "TagKeys"))
    if not arn:
        return _error("MissingParameter", "ResourceName is required", 400)

    existing = _tags.get(arn, [])
    _tags[arn] = [t for t in existing if t["Key"] not in keys_to_remove]

    _sync_tag_list_to_resource(arn)
    return _xml(200, "RemoveTagsFromResourceResponse", "")


def _list_tags(p):
    arn = _p(p, "ResourceName")
    if not arn:
        return _xml(200, "ListTagsForResourceResponse",
            "<ListTagsForResourceResult><TagList/></ListTagsForResourceResult>")

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


# ---------------------------------------------------------------------------
# Global Clusters
#
# Emulation scope: single-member global clusters only.  A global cluster can
# be created standalone or attached to one existing DB cluster via
# SourceDBClusterIdentifier.  Multi-member membership (secondary clusters
# in other regions), FailoverGlobalCluster, and SwitchoverGlobalCluster are
# not supported — MiniStack is a single-region emulator.
# ---------------------------------------------------------------------------

def _create_global_cluster(p):
    gc_id = _p(p, "GlobalClusterIdentifier")
    if not gc_id:
        return _error("MissingParameter", "GlobalClusterIdentifier is required", 400)
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

    members = []
    if source_cluster_id:
        source_arn = source_cluster_id
        for cl in _clusters.values():
            if cl["DBClusterIdentifier"] == source_cluster_id or cl["DBClusterArn"] == source_cluster_id:
                source_arn = cl["DBClusterArn"]
                engine = cl["Engine"]
                engine_version = cl["EngineVersion"]
                break
        members.append({
            "DBClusterArn": source_arn,
            "IsWriter": True,
            "GlobalWriteForwardingStatus": "disabled",
        })

    gc = {
        "GlobalClusterIdentifier": gc_id,
        "GlobalClusterArn": arn,
        "GlobalClusterResourceId": resource_id,
        "Engine": engine,
        "EngineVersion": engine_version,
        "Status": "available",
        "StorageEncrypted": storage_encrypted,
        "DeletionProtection": deletion_protection,
        "GlobalClusterMembers": members,
        "DatabaseName": _p(p, "DatabaseName") or "",
    }
    _global_clusters[gc_id] = gc
    return _xml(200, "CreateGlobalClusterResponse",
        f"<CreateGlobalClusterResult><GlobalCluster>{_global_cluster_xml(gc)}</GlobalCluster></CreateGlobalClusterResult>")


def _describe_global_clusters(p):
    gc_id = _p(p, "GlobalClusterIdentifier")
    if gc_id:
        gc = _global_clusters.get(gc_id)
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
    gc = _global_clusters.get(gc_id)
    if not gc:
        return _error("GlobalClusterNotFoundFault",
            f"Global cluster {gc_id} not found.", 404)

    if gc.get("DeletionProtection"):
        return _error("InvalidParameterCombination",
            "Cannot delete a global cluster when DeletionProtection is enabled.", 400)

    writer_members = [m for m in gc.get("GlobalClusterMembers", []) if m.get("IsWriter")]
    if writer_members:
        return _error("InvalidGlobalClusterStateFault",
            "Global cluster still has member clusters. Remove them before deleting.", 400)

    gc["Status"] = "deleting"
    del _global_clusters[gc_id]
    return _xml(200, "DeleteGlobalClusterResponse",
        f"<DeleteGlobalClusterResult><GlobalCluster>{_global_cluster_xml(gc)}</GlobalCluster></DeleteGlobalClusterResult>")


def _remove_from_global_cluster(p):
    gc_id = _p(p, "GlobalClusterIdentifier")
    db_cluster_id = _p(p, "DbClusterIdentifier")
    gc = _global_clusters.get(gc_id)
    if not gc:
        return _error("GlobalClusterNotFoundFault",
            f"Global cluster {gc_id} not found.", 404)

    members = gc.get("GlobalClusterMembers", [])
    new_members = [m for m in members if m["DBClusterArn"] != db_cluster_id]
    if len(new_members) == len(members):
        for cl in _clusters.values():
            if cl["DBClusterIdentifier"] == db_cluster_id:
                db_cluster_id = cl["DBClusterArn"]
                break
        new_members = [m for m in members if m["DBClusterArn"] != db_cluster_id]

    gc["GlobalClusterMembers"] = new_members
    return _xml(200, "RemoveFromGlobalClusterResponse",
        f"<RemoveFromGlobalClusterResult><GlobalCluster>{_global_cluster_xml(gc)}</GlobalCluster></RemoveFromGlobalClusterResult>")


def _modify_global_cluster(p):
    gc_id = _p(p, "GlobalClusterIdentifier")
    gc = _global_clusters.get(gc_id)
    if not gc:
        return _error("GlobalClusterNotFoundFault",
            f"Global cluster {gc_id} not found.", 404)

    new_id = _p(p, "NewGlobalClusterIdentifier")
    if new_id and new_id != gc_id:
        if new_id in _global_clusters:
            return _error("GlobalClusterAlreadyExistsFault",
                f"Global cluster {new_id} already exists.", 400)
        gc["GlobalClusterIdentifier"] = new_id
        gc["GlobalClusterArn"] = f"arn:aws:rds::{get_account_id()}:global-cluster:{new_id}"
        _global_clusters[new_id] = gc
        del _global_clusters[gc_id]

    if _p(p, "DeletionProtection"):
        gc["DeletionProtection"] = _p(p, "DeletionProtection") == "true"
    if _p(p, "EngineVersion"):
        gc["EngineVersion"] = _p(p, "EngineVersion")

    return _xml(200, "ModifyGlobalClusterResponse",
        f"<ModifyGlobalClusterResult><GlobalCluster>{_global_cluster_xml(gc)}</GlobalCluster></ModifyGlobalClusterResult>")


def _enable_http_endpoint(p):
    arn = _p(p, "ResourceArn")
    for cluster in _clusters.values():
        if cluster.get("DBClusterArn") == arn:
            cluster["HttpEndpointEnabled"] = True
            return _xml(200, "EnableHttpEndpointResponse",
                f"<EnableHttpEndpointResult>"
                f"<ResourceArn>{arn}</ResourceArn>"
                f"<HttpEndpointEnabled>true</HttpEndpointEnabled>"
                f"</EnableHttpEndpointResult>")
    return _error("DBClusterNotFoundFault", f"Cluster with ARN {arn} not found.", 404)


def _global_cluster_xml(gc):
    member_xml = ""
    for m in gc.get("GlobalClusterMembers", []):
        member_xml += f"""<GlobalClusterMember>
            <DBClusterArn>{m['DBClusterArn']}</DBClusterArn>
            <IsWriter>{str(m.get('IsWriter', False)).lower()}</IsWriter>
            <GlobalWriteForwardingStatus>{m.get('GlobalWriteForwardingStatus', 'disabled')}</GlobalWriteForwardingStatus>
        </GlobalClusterMember>"""
    return f"""<GlobalClusterIdentifier>{gc['GlobalClusterIdentifier']}</GlobalClusterIdentifier>
        <GlobalClusterArn>{gc['GlobalClusterArn']}</GlobalClusterArn>
        <GlobalClusterResourceId>{gc['GlobalClusterResourceId']}</GlobalClusterResourceId>
        <Engine>{gc['Engine']}</Engine>
        <EngineVersion>{gc['EngineVersion']}</EngineVersion>
        <Status>{gc['Status']}</Status>
        <DatabaseName>{gc.get('DatabaseName', '')}</DatabaseName>
        <StorageEncrypted>{str(gc.get('StorageEncrypted', False)).lower()}</StorageEncrypted>
        <DeletionProtection>{str(gc.get('DeletionProtection', False)).lower()}</DeletionProtection>
        <GlobalClusterMembers>{member_xml}</GlobalClusterMembers>"""


# ---------------------------------------------------------------------------
# Engine Versions & Orderable Options
# ---------------------------------------------------------------------------

def _describe_engine_versions(p):
    engine = _p(p, "Engine") or "postgres"
    version_filter = _p(p, "EngineVersion")
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
        "aurora-postgresql": [
            ("18.3", "aurora-postgresql18"), ("17.5", "aurora-postgresql17"),
            ("16.4", "aurora-postgresql16"),
            ("15.3", "aurora-postgresql15"), ("14.8", "aurora-postgresql14"),
        ],
        "aurora-mysql": [
            ("8.0.mysql_aurora.3.03.0", "aurora-mysql8.0"),
        ],
    }
    versions = versions_map.get(engine, [("15.3", "15")])
    members = ""
    for ver, family in versions:
        if version_filter and ver != version_filter:
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
            <SupportsGlobalDatabases>false</SupportsGlobalDatabases>
            <SupportsBabelfish>false</SupportsBabelfish>
            <SupportsCertificateRotationWithoutRestart>true</SupportsCertificateRotationWithoutRestart>
        </DBEngineVersion>"""
    return _xml(200, "DescribeDBEngineVersionsResponse",
        f"<DescribeDBEngineVersionsResult><DBEngineVersions>{members}</DBEngineVersions></DescribeDBEngineVersionsResult>")


def _describe_orderable_options(p):
    engine = _p(p, "Engine") or "postgres"
    engine_version = _p(p, "EngineVersion")
    db_class = _p(p, "DBInstanceClass")

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


def _parse_filters(params):
    """Parse Filters.member.N.Name / Filters.member.N.Values.member.M."""
    filters = {}
    i = 1
    while True:
        name = _p(params, f"Filters.member.{i}.Name")
        if not name:
            break
        values = []
        j = 1
        while True:
            v = _p(params, f"Filters.member.{i}.Values.member.{j}")
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
        "aurora-postgresql": "15.3", "aurora-mysql": "8.0.mysql_aurora.3.03.0",
    }
    return defaults.get(engine, "15.3")


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
    if "mysql" in engine or "aurora-mysql" in engine:
        return (
            apply_image_prefix("mysql:8"),
            {"MYSQL_ROOT_PASSWORD": password, "MYSQL_ROOT_HOST": "%",
             "MYSQL_DATABASE": db_name,
             "MYSQL_USER": user, "MYSQL_PASSWORD": password},
            3306,
            "/var/lib/mysql",
        )
    if "mariadb" in engine:
        return (
            apply_image_prefix("mariadb:latest"),
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
    return base


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
    "EnableHttpEndpoint": _enable_http_endpoint,
}


def reset():
    docker_client = _get_docker()
    if docker_client:
        for instance in _instances.values():
            cid = instance.get("_docker_container_id")
            if cid:
                try:
                    c = docker_client.containers.get(cid)
                    c.stop(timeout=2)
                    c.remove(v=True)
                except Exception as e:
                    logger.warning("reset: failed to stop/remove container %s: %s", cid, e)
    _instances.clear()
    _clusters.clear()
    _subnet_groups.clear()
    _param_groups.clear()
    _snapshots.clear()
    _db_cluster_param_groups.clear()
    _db_cluster_snapshots.clear()
    _option_groups.clear()
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
