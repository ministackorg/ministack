"""

THIS IS STILL A WORK IN PROGRESS.


DocumentDB Service Emulator (control plane).

Query API (Action=...) + JSON bodies for CreateDBInstance etc.
When Docker is available, spins up real mongo:X containers (mongo:5 by default)
and returns usable Endpoint. Address/Port for direct wire-protocol connections
(pymongo, etc.). The data plane is real MongoDB — no in-process emulation of
Mongo commands.

Supported (core surface for aws docdb / boto3 docdb client + IaC):
  CreateDBInstance, DeleteDBInstance, DescribeDBInstances, ModifyDBInstance,
  StartDBInstance, StopDBInstance, RebootDBInstance,
  CreateDBCluster, DeleteDBCluster, DescribeDBClusters, ModifyDBCluster,
  StartDBCluster, StopDBCluster,
  CreateDBSubnetGroup, DeleteDBSubnetGroup, DescribeDBSubnetGroups,
  CreateDBSnapshot, DeleteDBSnapshot, DescribeDBSnapshots,
  ListTagsForResource, AddTagsToResource, RemoveTagsFromResource,
  DescribeDBEngineVersions, DescribeOrderableDBInstanceOptions,
  DescribePendingMaintenanceActions.

Engine is forced to "docdb". Default container port 27017.
Env vars: DOCDB_BASE_PORT (default 27117), DOCDB_PERSIST, DOCDB_TMPFS_SIZE,
DOCKER_NETWORK.

Resources:
- AWS Mongo API: https://docs.aws.amazon.com/documentdb/latest/APIReference/API_Operations_Amazon_DocumentDB_with_MongoDB_compatibility.html
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

from ministack.core.persistence import load_state
from ministack.core.responses import AccountScopedDict, apply_image_prefix, get_account_id, get_region, new_uuid

logger = logging.getLogger("documentdb")

ACCOUNT_ID = "000000000000"
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
BASE_PORT = int(os.environ.get("DOCDB_BASE_PORT", "27117"))
DOCDB_TMPFS_SIZE = os.environ.get("DOCDB_TMPFS_SIZE", "256m")
DOCDB_PERSIST = os.environ.get("DOCDB_PERSIST", "0").lower() in ("1", "true", "yes")
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")

_instances = AccountScopedDict()
_clusters = AccountScopedDict()
_subnet_groups = AccountScopedDict()
_snapshots = AccountScopedDict()
_tags = AccountScopedDict()
_port_counter = [BASE_PORT]

_docker = None
_ministack_network = None
_state: dict = {}  # in-memory storage


# ── Persistence ────────────────────────────────────────────

def get_state():
    instances = copy.deepcopy(_instances)
    for key in list(instances._data):
        instances._data[key].pop("_docker_container_id", None)
    state = {
        "instances": instances,
        "clusters": copy.deepcopy(_clusters),
        "subnet_groups": copy.deepcopy(_subnet_groups),
        "snapshots": copy.deepcopy(_snapshots),
        "tags": copy.deepcopy(_tags),
        "port_counter": _port_counter[0],
    }
    return state


def restore_state(data):
    if not data:
        return
    _clusters.update(data.get("clusters", {}))
    _subnet_groups.update(data.get("subnet_groups", {}))
    _snapshots.update(data.get("snapshots", {}))
    _tags.update(data.get("tags", {}))
    if "port_counter" in data:
        _port_counter[0] = data["port_counter"]
    instances_data = data.get("instances", {})
    if isinstance(instances_data, AccountScopedDict):
        for key, inst in list(instances_data._data.items()):
            inst["_docker_container_id"] = None
            inst["DBInstanceStatus"] = "available"
            _instances._data[key] = inst
    else:
        for name, inst in instances_data.items():
            inst["_docker_container_id"] = None
            inst["DBInstanceStatus"] = "available"
            _instances[name] = inst


try:
    _restored = load_state("documentdb")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


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
    """Detect the Docker network MiniStack is running on (if containerised)."""
    global _ministack_network
    if _ministack_network is not None:
        return _ministack_network or None
    if DOCKER_NETWORK:
        _ministack_network = DOCKER_NETWORK
        logger.debug("DocDB: using DOCKER_NETWORK=%s", DOCKER_NETWORK)
        return DOCKER_NETWORK
    try:
        self_container = docker_client.containers.get(
            os.environ.get("HOSTNAME", ""))
        nets = list(
            self_container.attrs["NetworkSettings"]["Networks"].keys())
        if nets:
            _ministack_network = nets[0]
            logger.debug("DocDB: detected MiniStack network: %s",
                         _ministack_network)
            return _ministack_network
    except Exception:
        logger.debug("DocDB: could not detect MiniStack network, "
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


_port_lock = threading.Lock()


def _next_port():
    with _port_lock:
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


async def handle_request(method, path, headers, body, query_params):
    params = dict(query_params)
    if method == "POST" and body:
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
            form_params = parse_qs(raw)
            for k, v in form_params.items():
                params[k] = v

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
    """
    Parameters and syntax (<Required or not> | <Type of var> | <Options>):
    - DBClusterIdentifier: (Required | str)
    - DBInstanceClass: (Required | str | See note 1 below)
    - DBInstanceIdentifier: (Required | str | See note 2 below)
    - Engine: (Required | str)
    - AutoMinorVersionUpgrade: (Not Required | bool | Default=False)
    - AvailabilityZone: (Not Required | str | Default:Randomly selected from AZ URL)
    - CACertificateIdentifier: (Not Required | str)
    - CopyTagsToSnapshot: (Not Required | bool | Default=False)
    - EnablePerformanceInsights: (Not Required | bool)
    - PerformanceInsightsKMSKeyId: (Not Required | str)
    - PreferredMaintenanceWindow: (Not Required | str)
    - PromotionTier: (Not Required | int | 0-15 | Default=1)
    - Tags_Tag_N: (Not Required | Array of Tags)

    Instance note
    1) This value "DBInstanceClass" can be anything from the table here
       https://docs.aws.amazon.com/documentdb/latest/devguide/db-instance-classes.html#db-instance-class-specs.
       Honestly, since this will be running locally, the code will not do any resource management
       in this regard. The limit you will have will be the PC running Ministack.
    2) Needs from 1-63 letters, numbers, or hyphens. The first character must be a letter.
       Cannot end with a hyphen or contain two consecutive hyphens.
       https://docs.aws.amazon.com/documentdb/latest/APIReference/API_CreateDBInstance.html#:~:text=DBInstanceIdentifier

    Returns: DBInstance
    """
    db_id = _evaluate_params(params, "DBInstanceIdentifier")
    if not db_id:
        return _error("MissingParameter", "DBInstanceIdentifier is required", 400)
    if db_id in _instances:
        return _error("DBInstanceAlreadyExistsFault", f"DB instance {db_id} already exists", 400)

    engine = "docdb"
    engine_version = _evaluate_params(params, "EngineVersion") or _default_engine_version(engine)
    db_class = _evaluate_params(params, "DBInstanceClass") or "db.t3.medium"
    master_user = _evaluate_params(params, "MasterUsername") or "root"
    master_pass = _evaluate_params(params, "MasterUserPassword") or "password"
    db_name = _evaluate_params(params, "DBName") or "admin"
    port = int(_evaluate_params(params, "Port") or "27017")

    cluster_id_param = _evaluate_params(params, "DBClusterIdentifier")
    if cluster_id_param and cluster_id_param in _clusters:
        parent = _clusters[cluster_id_param]
        if not _evaluate_params(params, "MasterUsername"):
            master_user = parent.get("MasterUsername", master_user)
        if not _evaluate_params(params, "MasterUserPassword"):
            master_pass = parent.get("_MasterUserPassword", master_pass)

    allocated_storage = int(_evaluate_params(params, "AllocatedStorage") or "20")
    storage_type = _evaluate_params(params, "StorageType") or "gp2"
    subnet_group_name = _evaluate_params(params, "DBSubnetGroupName") or "default"

    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:db:{db_id}"
    dbi_resource_id = f"db-{new_uuid().replace('-', '')[:20].upper()}"
    endpoint_host = "localhost"
    endpoint_port = port
    docker_container_id = None
    internal_host = None
    internal_port = None

    docker_client = _get_docker()
    if docker_client:
        host_port = _next_port()
        endpoint_port = host_port
        ms_network = _get_ministack_network(docker_client)
        image, env, container_port, data_path = _docker_image_for_docdb(
            engine_version, master_user, master_pass, db_name
        )
        if image:
            try:
                container_kwargs = dict(
                    image=image, detach=True,
                    environment=env,
                    ports={f"{container_port}/tcp": host_port},
                    name=f"ministack-docdb-{db_id}",
                    labels={"ministack": "documentdb", "db_id": db_id},
                )
                if ms_network:
                    container_kwargs["network"] = ms_network
                if DOCDB_PERSIST:
                    container_kwargs["volumes"] = {
                        f"ministack-docdb-{db_id}-data": {"bind": data_path, "mode": "rw"},
                    }
                else:
                    container_kwargs["tmpfs"] = {
                        data_path: f"rw,noexec,nosuid,size={DOCDB_TMPFS_SIZE}",
                    }
                container = docker_client.containers.run(**container_kwargs)
                docker_container_id = container.id
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
                        def _bg_wait(cip=container_ip, cport=container_port,
                                     did=db_id, net=ms_network):
                            if _wait_for_port(cip, cport):
                                logger.info(
                                    "docdb: mongo container for %s ready at "
                                    "%s:%s (network %s)", did, cip, cport, net)
                            else:
                                logger.warning(
                                    "docdb: mongo container for %s at %s:%s "
                                    "not ready after timeout", did, cip, cport)
                        threading.Thread(target=_bg_wait, daemon=True).start()
                    else:
                        logger.info(
                            "docdb: started mongo container for %s on port %s",
                            db_id, host_port)
                else:
                    def _bg_wait_port(hp=host_port, did=db_id):
                        if _wait_for_port("127.0.0.1", hp):
                            logger.info("docdb: mongo container for %s ready on port %s", did, hp)
                        else:
                            logger.warning("docdb: mongo container for %s on port %s not ready after timeout", did, hp)
                    threading.Thread(target=_bg_wait_port, daemon=True).start()
            except Exception as e:
                logger.warning("docdb: Docker failed for %s: %s", db_id, e)

    cluster_id = _evaluate_params(params, "DBClusterIdentifier")
    now_ts = time.time()

    vpc_sgs = _parse_member_list(params, "VpcSecurityGroupIds")
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
        "DBInstanceStatus": "available",
        "MasterUsername": master_user,
        "DBName": db_name,
        "Endpoint": {
            "Address": endpoint_host,
            "Port": endpoint_port,
            "HostedZoneId": "Z2R2ITUGPM61AM",
        },
        "AllocatedStorage": allocated_storage,
        "InstanceCreateTime": _format_time(now_ts),
        "PreferredBackupWindow": "03:00-04:00",
        "BackupRetentionPeriod": int(_evaluate_params(params, "BackupRetentionPeriod") or "1"),
        "DBSecurityGroups": [],
        "VpcSecurityGroups": vpc_sg_list,
        "DBParameterGroups": [{
            "DBParameterGroupName": f"default.docdb{engine_version.split('.')[0]}",
            "ParameterApplyStatus": "in-sync",
        }],
        "AvailabilityZone": _evaluate_params(params, "AvailabilityZone") or f"{get_region()}a",
        "DBSubnetGroup": subnet_group,
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
        "DeletionProtection": _evaluate_params(params, "DeletionProtection") == "true",
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
        "_docker_container_id": docker_container_id,
        "_internal_address": internal_host,
        "_internal_port": internal_port,
        "_MasterUserPassword": master_pass,
    }
    _instances[db_id] = instance
    _register_instance_in_cluster(instance)

    req_tags = _parse_tags(params)
    if req_tags:
        _tags[arn] = req_tags
        instance["TagList"] = req_tags

    return _single_instance_response("CreateDBInstanceResponse", "CreateDBInstanceResult", instance)


def _delete_db_instance(p):
    db_id = _evaluate_params(p, "DBInstanceIdentifier")
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
            logger.info("docdb: removed container for %s", db_id)
        except Exception as e:
            logger.warning("docdb: failed to remove container for %s: %s", db_id, e)

    skip_snapshot = _evaluate_params(p, "SkipFinalSnapshot") == "true"
    final_snap_id = _evaluate_params(p, "FinalDBSnapshotIdentifier")
    if not skip_snapshot and final_snap_id:
        _create_snapshot_internal(final_snap_id, instance)

    instance["DBInstanceStatus"] = "deleting"
    arn = instance["DBInstanceArn"]
    _tags.pop(arn, None)
    del _instances[db_id]
    return _single_instance_response("DeleteDBInstanceResponse", "DeleteDBInstanceResult", instance)


def _describe_db_instances(p):
    db_id = _evaluate_params(p, "DBInstanceIdentifier")
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


def _start_db_instance(p):
    db_id = _evaluate_params(p, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "available"
    return _single_instance_response("StartDBInstanceResponse", "StartDBInstanceResult", instance)


def _stop_db_instance(p):
    db_id = _evaluate_params(p, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "stopped"
    return _single_instance_response("StopDBInstanceResponse", "StopDBInstanceResult", instance)


def _reboot_db_instance(p):
    db_id = _evaluate_params(p, "DBInstanceIdentifier")
    instance = _resolve_instance(db_id)
    if not instance:
        return _error("DBInstanceNotFound", f"DBInstance {db_id} not found.", 404)
    instance["DBInstanceStatus"] = "available"
    return _single_instance_response("RebootDBInstanceResponse", "RebootDBInstanceResult", instance)


# ---------------------------------------------------------------------------
# DB Clusters
# ---------------------------------------------------------------------------

def _create_db_cluster(p):
    """
    IMPORTANT!!!
    This function might need to be rethought because there is a a difference
    between a DocDB cluster and a DocDB instance. Clusters hosts multiple
    instances, so how can Docker replicate this?
    My current idea is to handle this with container naming.
    For example:
        - cluster-1-instance_name
        - cluster-2-instance_name
    The only thing with the cluster is that it won't have any data. Maybe I'll
    use an Alpine image not to take too much resource.

    """
    cluster_id = _evaluate_params(p, "DBClusterIdentifier")
    if not cluster_id:
        return _error("MissingParameter", "DBClusterIdentifier is required", 400)
    if cluster_id in _clusters:
        return _error("DBClusterAlreadyExistsFault",
            f"DB cluster {cluster_id} already exists.", 400)

    engine = "docdb"
    engine_version = _evaluate_params(p, "EngineVersion") or _default_engine_version(engine)
    port = int(_evaluate_params(p, "Port") or "27017")
    master_user = _evaluate_params(p, "MasterUsername") or "root"
    master_pass = _evaluate_params(p, "MasterUserPassword") or "password"
    arn = f"arn:aws:rds:{get_region()}:{get_account_id()}:cluster:{cluster_id}"
    unique_suffix = new_uuid()[:8]
    now_ts = time.time()

    vpc_sgs = _parse_member_list(p, "VpcSecurityGroupIds")
    vpc_sg_list = [{"VpcSecurityGroupId": sg, "Status": "active"} for sg in vpc_sgs] if vpc_sgs else []
    az_list = _parse_member_list(p, "AvailabilityZones")
    if not az_list:
        az_list = [f"{get_region()}a", f"{get_region()}b", f"{get_region()}c"]

    cluster = {
        "DBClusterIdentifier": cluster_id,
        "DBClusterArn": arn,
        "Engine": engine,
        "EngineVersion": engine_version,
        "EngineMode": _evaluate_params(p, "EngineMode") or "provisioned",
        "Status": "available",
        "MasterUsername": master_user,
        "_MasterUserPassword": master_pass,
        "DatabaseName": _evaluate_params(p, "DatabaseName") or None,
        "NetworkType": _evaluate_params(p, "NetworkType") or "IPV4",
        "EngineLifecycleSupport": _evaluate_params(p, "EngineLifecycleSupport") or "open-source-rds-extended-support",
        "Endpoint": f"{cluster_id}.cluster-{unique_suffix}.{get_region()}.docdb.amazonaws.com",
        "ReaderEndpoint": f"{cluster_id}.cluster-ro-{unique_suffix}.{get_region()}.docdb.amazonaws.com",
        "Port": port,
        "MultiAZ": _evaluate_params(p, "MultiAZ") == "true",
        "AvailabilityZones": az_list,
        "DBClusterMembers": [],
        "VpcSecurityGroups": vpc_sg_list,
        "DBSubnetGroup": _evaluate_params(p, "DBSubnetGroupName") or "default",
        "DBClusterParameterGroup": _evaluate_params(p, "DBClusterParameterGroupName") or "default.docdb",
        "BackupRetentionPeriod": int(_evaluate_params(p, "BackupRetentionPeriod") or "1"),
        "PreferredBackupWindow": _evaluate_params(p, "PreferredBackupWindow") or "03:00-04:00",
        "PreferredMaintenanceWindow": _evaluate_params(p, "PreferredMaintenanceWindow") or "sun:05:00-sun:06:00",
        "ClusterCreateTime": _format_time(now_ts),
        "EarliestRestorableTime": _format_time(now_ts),
        "LatestRestorableTime": _format_time(now_ts),
        "StorageEncrypted": _evaluate_params(p, "StorageEncrypted") == "true",
        "KmsKeyId": _evaluate_params(p, "KmsKeyId") or "",
        "DeletionProtection": _evaluate_params(p, "DeletionProtection") == "true",
        "IAMDatabaseAuthenticationEnabled": _evaluate_params(p, "EnableIAMDatabaseAuthentication") == "true",
        "EnabledCloudwatchLogsExports": [],
        "HttpEndpointEnabled": _evaluate_params(p, "EnableHttpEndpoint") == "true",
        "CopyTagsToSnapshot": _evaluate_params(p, "CopyTagsToSnapshot") == "true",
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
    cluster_id = _evaluate_params(p, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    if cluster.get("DeletionProtection"):
        return _error("InvalidParameterCombination",
            "Cannot delete a DB cluster when DeletionProtection is enabled.", 400)

    cluster["Status"] = "deleting"
    _tags.pop(cluster["DBClusterArn"], None)
    del _clusters[cluster_id]
    return _xml(200, "DeleteDBClusterResponse",
        f"<DeleteDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></DeleteDBClusterResult>")


def _describe_db_clusters(p):
    cluster_id = _evaluate_params(p, "DBClusterIdentifier")
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


def _modify_db_cluster(p):
    cluster_id = _evaluate_params(p, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)

    if _evaluate_params(p, "EngineVersion"):
        cluster["EngineVersion"] = _evaluate_params(p, "EngineVersion")
    if _evaluate_params(p, "MasterUserPassword"):
        new_pass = _evaluate_params(p, "MasterUserPassword")
        old_pass = cluster.get("_MasterUserPassword", "password")
        cluster["_MasterUserPassword"] = new_pass
    if _evaluate_params(p, "Port"):
        cluster["Port"] = int(_evaluate_params(p, "Port"))
    if _evaluate_params(p, "BackupRetentionPeriod"):
        cluster["BackupRetentionPeriod"] = int(_evaluate_params(p, "BackupRetentionPeriod"))
    if _evaluate_params(p, "PreferredBackupWindow"):
        cluster["PreferredBackupWindow"] = _evaluate_params(p, "PreferredBackupWindow")
    if _evaluate_params(p, "PreferredMaintenanceWindow"):
        cluster["PreferredMaintenanceWindow"] = _evaluate_params(p, "PreferredMaintenanceWindow")
    if _evaluate_params(p, "DeletionProtection"):
        cluster["DeletionProtection"] = _evaluate_params(p, "DeletionProtection") == "true"
    if _evaluate_params(p, "EnableIAMDatabaseAuthentication"):
        cluster["IAMDatabaseAuthenticationEnabled"] = _evaluate_params(p, "EnableIAMDatabaseAuthentication") == "true"
    if _evaluate_params(p, "EnableHttpEndpoint"):
        cluster["HttpEndpointEnabled"] = _evaluate_params(p, "EnableHttpEndpoint") == "true"
    if _evaluate_params(p, "CopyTagsToSnapshot"):
        cluster["CopyTagsToSnapshot"] = _evaluate_params(p, "CopyTagsToSnapshot") == "true"

    vpc_sgs = _parse_member_list(p, "VpcSecurityGroupIds")
    if vpc_sgs:
        cluster["VpcSecurityGroups"] = [
            {"VpcSecurityGroupId": sg, "Status": "active"} for sg in vpc_sgs
        ]

    return _xml(200, "ModifyDBClusterResponse",
        f"<ModifyDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></ModifyDBClusterResult>")


def _start_db_cluster(p):
    cluster_id = _evaluate_params(p, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    cluster["Status"] = "available"
    return _xml(200, "StartDBClusterResponse",
        f"<StartDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StartDBClusterResult>")


def _stop_db_cluster(p):
    cluster_id = _evaluate_params(p, "DBClusterIdentifier")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("DBClusterNotFoundFault", f"DBCluster {cluster_id} not found.", 404)
    cluster["Status"] = "stopped"
    return _xml(200, "StopDBClusterResponse",
        f"<StopDBClusterResult><DBCluster>{_cluster_xml(cluster)}</DBCluster></StopDBClusterResult>")

# ---------------------------------------------------------------------------
# Subnet Groups (minimal)
# ---------------------------------------------------------------------------

def _create_subnet_group(p):
    name = _evaluate_params(p, "DBSubnetGroupName")
    if not name:
        return _error("MissingParameter", "DBSubnetGroupName is required", 400)
    desc = _evaluate_params(p, "DBSubnetGroupDescription") or name
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
    name = _evaluate_params(p, "DBSubnetGroupName")
    sg = _subnet_groups.pop(name, None)
    if not sg:
        return _error("DBSubnetGroupNotFoundFault", f"Subnet group {name} not found.", 404)
    _tags.pop(sg.get("DBSubnetGroupArn", ""), None)
    return _xml(200, "DeleteDBSubnetGroupResponse", "")


def _describe_subnet_groups(p):
    name = _evaluate_params(p, "DBSubnetGroupName")
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
# Tags
# ---------------------------------------------------------------------------

def _add_tags(p):
    arn = _evaluate_params(p, "ResourceName")
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
    arn = _evaluate_params(p, "ResourceName")
    keys_to_remove = set(_parse_member_list(p, "TagKeys"))
    if not arn:
        return _error("MissingParameter", "ResourceName is required", 400)

    existing = _tags.get(arn, [])
    _tags[arn] = [t for t in existing if t["Key"] not in keys_to_remove]

    _sync_tag_list_to_resource(arn)
    return _xml(200, "RemoveTagsFromResourceResponse", "")


def _list_tags(p):
    arn = _evaluate_params(p, "ResourceName")
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
# Engine Versions & Orderable Options (docdb)
# ---------------------------------------------------------------------------

def _describe_db_engine_versions(p):
    engine = _evaluate_params(p, "Engine") or "docdb"
    version_filter = _evaluate_params(p, "EngineVersion")
    # DocDB-focused versions (wire compatible with mongo 5.x/6.x/7.x)
    versions = [
        ("5.0.0", "docdb5.0"),
        ("4.0.0", "docdb4.0"),
    ]
    members = ""
    for ver, family in versions:
        if version_filter and ver != version_filter:
            continue
        members += f"""<DBEngineVersion>
            <Engine>docdb</Engine>
            <EngineVersion>{ver}</EngineVersion>
            <DBParameterGroupFamily>{family}</DBParameterGroupFamily>
            <DBEngineDescription>Amazon DocumentDB (with MongoDB compatibility)</DBEngineDescription>
            <DBEngineVersionDescription>DocumentDB {ver}</DBEngineVersionDescription>
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
    engine = "docdb"
    engine_version = _evaluate_params(p, "EngineVersion") or "5.0.0"
    db_class = _evaluate_params(p, "DBInstanceClass")

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
        f"<DescribeOrderableDBInstanceOptionsResult><OrderableDBInstanceOptions>{members}</OrderableDBInstanceOptions></DescribeOrderableDBInstanceOptionsResult>")


def _describe_pending_maintenance_actions(p):
    return _xml(200, "DescribePendingMaintenanceActionsResponse",
        "<DescribePendingMaintenanceActionsResult><PendingMaintenanceActions/></DescribePendingMaintenanceActionsResult>")

def _cluster_xml(c):
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

    db_name = c.get("DatabaseName")
    db_name_xml = f"<DatabaseName>{db_name}</DatabaseName>" if db_name else ""

    return f"""<DBClusterIdentifier>{c['DBClusterIdentifier']}</DBClusterIdentifier>
        <DBClusterArn>{c['DBClusterArn']}</DBClusterArn>
        <Engine>{c['Engine']}</Engine>
        <EngineVersion>{c['EngineVersion']}</EngineVersion>
        <EngineMode>{c.get('EngineMode','provisioned')}</EngineMode>
        <Status>{c['Status']}</Status>
        <MasterUsername>{c.get('MasterUsername','root')}</MasterUsername>
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
        <Port>{s.get('Port',27017)}</Port>
        <MasterUsername>{s.get('MasterUsername','root')}</MasterUsername>
        <DBName>{s.get('DBName','')}</DBName>
        <SnapshotType>{s.get('SnapshotType','manual')}</SnapshotType>
        <LicenseModel>{s.get('LicenseModel','docdb')}</LicenseModel>
        <StorageType>{s.get('StorageType','gp2')}</StorageType>
        <DBInstanceClass>{s.get('DBInstanceClass','db.t3.medium')}</DBInstanceClass>
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


def _single_instance_response(root_tag, result_tag, instance):
    return _xml(200, root_tag,
        f"<{result_tag}><DBInstance>{_instance_xml(instance)}</DBInstance></{result_tag}>")


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _evaluate_params(params, key, default=""):
    val = params.get(key, [default])
    if isinstance(val, list):
        return val[0] if val else default
    return val


def _parse_tags(params):
    """Parse Tags.member.N.Key / Tags.member.N.Value or Tags.Tag.N.Key / Tags.Tag.N.Value."""
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
    filters = {}
    i = 1
    while True:
        name = _evaluate_params(params, f"Filters.member.{i}.Name")
        if not name:
            break
        values = []
        j = 1
        while True:
            v = _evaluate_params(params, f"Filters.member.{i}.Values.member.{j}")
            if not v:
                break
            values.append(v)
            j += 1
        filters[name] = values
        i += 1
    return filters


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


def _format_time(ts):
    dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _default_engine_version(engine):
    if engine == "docdb":
        return "5.0.0"
    return "5.0.0"


def _docker_image_for_docdb(engine_version, user, password, db_name=""):
    """Return (image, env_dict, container_port, data_path) for Mongo."""
    # Recent mongo:7 provides good DocDB 5.0 wire/protocol compatibility.
    # Users can override via MINISTACK_IMAGE_PREFIX or future DOCDB_IMAGE env.
    image = apply_image_prefix("mongo:7")
    env = {
        "MONGO_INITDB_ROOT_USERNAME": user,
        "MONGO_INITDB_ROOT_PASSWORD": password,
    }
    # db_name is not auto-created by the official image init for root; clients
    # can `use <db>` after connecting with the returned endpoint + credentials.
    return image, env, 27017, "/data/db"


def _xml(status, root_tag, inner):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://rds.amazonaws.com/doc/2014-10-31/">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
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
    "CreateDBCluster": _create_db_cluster,
    "DeleteDBCluster": _delete_db_cluster,
    "DescribeDBClusters": _describe_db_clusters,
    "ModifyDBCluster": _modify_db_cluster,
    "StartDBCluster": _start_db_cluster,
    "StopDBCluster": _stop_db_cluster,
    "CreateDBSnapshot": _create_db_snapshot,
    "DeleteDBSnapshot": _delete_db_snapshot,
    "DescribeDBSnapshots": _describe_db_snapshots,
    "CreateDBSubnetGroup": _create_subnet_group,
    "DeleteDBSubnetGroup": _delete_subnet_group,
    "DescribeDBSubnetGroups": _describe_subnet_groups,
    "ListTagsForResource": _list_tags,
    "AddTagsToResource": _add_tags,
    "RemoveTagsFromResource": _remove_tags,
    "DescribeDBEngineVersions": _describe_db_engine_versions,
    "DescribeOrderableDBInstanceOptions": _describe_orderable_options,
    "DescribePendingMaintenanceActions": _describe_pending_maintenance_actions,
}


def reset():
    """Stop/remove any running docdb containers, then clear all state."""
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
                    logger.warning("reset: failed to stop/remove docdb container %s: %s", cid, e)
    _instances.clear()
    _clusters.clear()
    _subnet_groups.clear()
    _snapshots.clear()
    _tags.clear()
    _port_counter[0] = BASE_PORT
