"""
ElastiCache Service Emulator.
Query API (Action=...) for control plane.
Supports: CreateCacheCluster, DeleteCacheCluster, DescribeCacheClusters,
          ModifyCacheCluster, RebootCacheCluster,
          CreateReplicationGroup, DeleteReplicationGroup, DescribeReplicationGroups,
          ModifyReplicationGroup, IncreaseReplicaCount, DecreaseReplicaCount,
          CreateCacheSubnetGroup, DescribeCacheSubnetGroups, DeleteCacheSubnetGroup,
          ModifyCacheSubnetGroup,
          CreateCacheParameterGroup, DescribeCacheParameterGroups, DeleteCacheParameterGroup,
          DescribeCacheParameters, ModifyCacheParameterGroup, ResetCacheParameterGroup,
          CreateUser, DescribeUsers, DeleteUser, ModifyUser,
          CreateUserGroup, DescribeUserGroups, DeleteUserGroup, ModifyUserGroup,
          DescribeCacheEngineVersions,
          ListTagsForResource, AddTagsToResource, RemoveTagsFromResource,
          CreateSnapshot, DeleteSnapshot, DescribeSnapshots,
          DescribeEvents.

When Docker is available, CreateCacheCluster spins up a real Redis/Memcached container.
Otherwise returns localhost:6379 (assumes Redis sidecar in docker-compose).
"""

import copy
import logging
import os
import time
from urllib.parse import parse_qs

from ministack.core.persistence import load_state
from ministack.core.responses import AccountScopedDict, get_account_id, new_uuid, get_region

logger = logging.getLogger("elasticache")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
REDIS_DEFAULT_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_DEFAULT_PORT = int(os.environ.get("REDIS_PORT", "6379"))
BASE_PORT = int(os.environ.get("ELASTICACHE_BASE_PORT", "16379"))
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")

_clusters = AccountScopedDict()
_replication_groups = AccountScopedDict()
_subnet_groups = AccountScopedDict()
_param_groups = AccountScopedDict()
_param_group_params = AccountScopedDict()  # group_name -> {param_name -> param_dict}
_tags = AccountScopedDict()  # arn -> [{"Key": ..., "Value": ...}, ...]
_snapshots = AccountScopedDict()
_users = AccountScopedDict()
_user_groups = AccountScopedDict()
# Per-account event log. AccountScopedDict under key "entries" so the list
# manipulation stays simple and DescribeEvents never leaks cross-tenant rows.
_events = AccountScopedDict()


def _events_list() -> list:
    lst = _events.get("entries")
    if lst is None:
        lst = []
        _events["entries"] = lst
    return lst


_port_counter = [BASE_PORT]

_docker = None


# ── Persistence ────────────────────────────────────────────

def get_state():
    state = {
        "replication_groups": copy.deepcopy(_replication_groups),
        "subnet_groups": copy.deepcopy(_subnet_groups),
        "param_groups": copy.deepcopy(_param_groups),
        "param_group_params": copy.deepcopy(_param_group_params),
        "tags": copy.deepcopy(_tags),
        "snapshots": copy.deepcopy(_snapshots),
        "users": copy.deepcopy(_users),
        "user_groups": copy.deepcopy(_user_groups),
        "port_counter": _port_counter[0],
    }
    clusters = {}
    for name, cl in _clusters.items():
        c = copy.deepcopy(cl)
        c.pop("_docker_container_id", None)
        clusters[name] = c
    state["clusters"] = clusters
    return state


def restore_state(data):
    if not data:
        return
    _replication_groups.update(data.get("replication_groups", {}))
    _subnet_groups.update(data.get("subnet_groups", {}))
    _param_groups.update(data.get("param_groups", {}))
    _param_group_params.update(data.get("param_group_params", {}))
    _tags.update(data.get("tags", {}))
    _snapshots.update(data.get("snapshots", {}))
    _users.update(data.get("users", {}))
    _user_groups.update(data.get("user_groups", {}))
    if "port_counter" in data:
        _port_counter[0] = data["port_counter"]
    for name, cl in data.get("clusters", {}).items():
        cl["_docker_container_id"] = None
        cl["CacheClusterStatus"] = "available"
        _clusters[name] = cl


try:
    _restored = load_state("elasticache")
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


def _arn_cluster(cluster_id):
    return f"arn:aws:elasticache:{get_region()}:{get_account_id()}:cluster:{cluster_id}"


def _arn_replication_group(rg_id):
    return f"arn:aws:elasticache:{get_region()}:{get_account_id()}:replicationgroup:{rg_id}"


def _arn_subnet_group(name):
    return f"arn:aws:elasticache:{get_region()}:{get_account_id()}:subnetgroup:{name}"


def _arn_param_group(name):
    return f"arn:aws:elasticache:{get_region()}:{get_account_id()}:parametergroup:{name}"


def _arn_snapshot(name):
    return f"arn:aws:elasticache:{get_region()}:{get_account_id()}:snapshot:{name}"


def _record_event(source_id, source_type, message):
    lst = _events_list()
    lst.append({
        "SourceIdentifier": source_id,
        "SourceType": source_type,
        "Message": message,
        "Date": time.time(),
    })
    if len(lst) > 500:
        lst[:] = lst[-500:]


async def handle_request(method, path, headers, body, query_params):
    params = dict(query_params)
    if method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")

    handlers = {
        "CreateCacheCluster": _create_cache_cluster,
        "DeleteCacheCluster": _delete_cache_cluster,
        "DescribeCacheClusters": _describe_cache_clusters,
        "ModifyCacheCluster": _modify_cache_cluster,
        "RebootCacheCluster": _reboot_cache_cluster,
        "CreateReplicationGroup": _create_replication_group,
        "DeleteReplicationGroup": _delete_replication_group,
        "DescribeReplicationGroups": _describe_replication_groups,
        "ModifyReplicationGroup": _modify_replication_group,
        "IncreaseReplicaCount": _increase_replica_count,
        "DecreaseReplicaCount": _decrease_replica_count,
        "CreateCacheSubnetGroup": _create_subnet_group,
        "DescribeCacheSubnetGroups": _describe_subnet_groups,
        "DeleteCacheSubnetGroup": _delete_subnet_group,
        "ModifyCacheSubnetGroup": _modify_subnet_group,
        "CreateCacheParameterGroup": _create_param_group,
        "DescribeCacheParameterGroups": _describe_param_groups,
        "DeleteCacheParameterGroup": _delete_param_group,
        "DescribeCacheParameters": _describe_cache_parameters,
        "ModifyCacheParameterGroup": _modify_cache_parameter_group,
        "ResetCacheParameterGroup": _reset_cache_parameter_group,
        "DescribeCacheEngineVersions": _describe_engine_versions,
        "CreateUser": _create_user,
        "DescribeUsers": _describe_users,
        "DeleteUser": _delete_user,
        "ModifyUser": _modify_user,
        "CreateUserGroup": _create_user_group,
        "DescribeUserGroups": _describe_user_groups,
        "DeleteUserGroup": _delete_user_group,
        "ModifyUserGroup": _modify_user_group,
        "ListTagsForResource": _list_tags,
        "AddTagsToResource": _add_tags,
        "RemoveTagsFromResource": _remove_tags,
        "CreateSnapshot": _create_snapshot,
        "DeleteSnapshot": _delete_snapshot,
        "DescribeSnapshots": _describe_snapshots,
        "DescribeEvents": _describe_events,
    }

    handler = handlers.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown ElastiCache action: {action}", 400)
    return handler(params)


# ---- Cache Clusters ----

def _create_cache_cluster(p):
    cluster_id = _p(p, "CacheClusterId")
    engine = _p(p, "Engine") or "redis"
    engine_version = _p(p, "EngineVersion") or ("7.0.12" if engine == "redis" else "1.6.17")
    node_type = _p(p, "CacheNodeType") or "cache.t3.micro"
    num_nodes = int(_p(p, "NumCacheNodes") or "1")

    if cluster_id in _clusters:
        return _error("CacheClusterAlreadyExists", f"Cluster {cluster_id} already exists", 400)

    arn = _arn_cluster(cluster_id)
    endpoint_host = REDIS_DEFAULT_HOST
    endpoint_port = REDIS_DEFAULT_PORT if engine == "redis" else 11211
    docker_container_id = None

    docker_client = _get_docker()
    if docker_client:
        host_port = _port_counter[0]
        _port_counter[0] += 1
        endpoint_host = "localhost"
        endpoint_port = host_port

        if engine == "redis":
            image = f"redis:{engine_version.split('.')[0]}-alpine"
            container_port = 6379
        else:
            image = f"memcached:{engine_version}-alpine"
            container_port = 11211

        try:
            run_kwargs = dict(
                image=image, detach=True,
                ports={f"{container_port}/tcp": host_port},
                name=f"ministack-elasticache-{cluster_id}",
                labels={"ministack": "elasticache", "cluster_id": cluster_id},
                volumes={},
            )
            if DOCKER_NETWORK:
                run_kwargs["network"] = DOCKER_NETWORK
            container = docker_client.containers.run(**run_kwargs)
            docker_container_id = container.id
            if DOCKER_NETWORK:
                container.reload()
                networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
                container_ip = networks.get(DOCKER_NETWORK, {}).get("IPAddress", "")
                if container_ip:
                    endpoint_host = container_ip
                    endpoint_port = container_port
                    logger.info("ElastiCache: started %s container for %s at %s:%s (network %s)",
                                engine, cluster_id, container_ip, container_port, DOCKER_NETWORK)
                else:
                    logger.info("ElastiCache: started %s container for %s on port %s", engine, cluster_id, host_port)
            else:
                logger.info("ElastiCache: started %s container for %s on port %s", engine, cluster_id, host_port)
        except Exception as e:
            logger.warning("ElastiCache: Docker failed for %s: %s", cluster_id, e)
            endpoint_host = REDIS_DEFAULT_HOST
            endpoint_port = REDIS_DEFAULT_PORT

    subnet_group = _p(p, "CacheSubnetGroupName") or "default"
    param_group_name = _p(p, "CacheParameterGroupName") or f"default.{engine}{engine_version[:3]}"

    _clusters[cluster_id] = {
        "CacheClusterId": cluster_id,
        "CacheClusterArn": arn,
        "CacheClusterStatus": "available",
        "Engine": engine,
        "EngineVersion": engine_version,
        "CacheNodeType": node_type,
        "NumCacheNodes": num_nodes,
        "CacheClusterCreateTime": time.time(),
        "PreferredAvailabilityZone": f"{get_region()}a",
        "CacheParameterGroup": {
            "CacheParameterGroupName": param_group_name,
            "ParameterApplyStatus": "in-sync",
        },
        "CacheSubnetGroupName": subnet_group,
        "AutoMinorVersionUpgrade": True,
        "SecurityGroups": [],
        "ReplicationGroupId": _p(p, "ReplicationGroupId") or "",
        "SnapshotRetentionLimit": int(_p(p, "SnapshotRetentionLimit") or "0"),
        "SnapshotWindow": _p(p, "SnapshotWindow") or "05:00-06:00",
        "PreferredMaintenanceWindow": _p(p, "PreferredMaintenanceWindow") or "sun:05:00-sun:06:00",
        "CacheNodes": [
            {
                "CacheNodeId": f"{i:04d}",
                "CacheNodeStatus": "available",
                "CacheNodeCreateTime": time.time(),
                "Endpoint": {"Address": endpoint_host, "Port": endpoint_port},
                "ParameterGroupStatus": "in-sync",
                "SourceCacheNodeId": "",
            }
            for i in range(1, num_nodes + 1)
        ],
        "_docker_container_id": docker_container_id,
        "_endpoint": {"Address": endpoint_host, "Port": endpoint_port},
    }

    tags = _extract_tags(p)
    if tags:
        _tags[arn] = tags

    _record_event(cluster_id, "cache-cluster", "Cache cluster created")
    return _xml_cluster_response("CreateCacheClusterResponse", "CreateCacheClusterResult", _clusters[cluster_id])


def _delete_cache_cluster(p):
    cluster_id = _p(p, "CacheClusterId")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("CacheClusterNotFound", f"Cluster {cluster_id} not found", 404)

    docker_client = _get_docker()
    if docker_client and cluster.get("_docker_container_id"):
        try:
            container = docker_client.containers.get(cluster["_docker_container_id"])
            container.stop(timeout=5)
            container.remove()
        except Exception as e:
            logger.warning("ElastiCache: failed to remove container for %s: %s", cluster_id, e)

    cluster["CacheClusterStatus"] = "deleting"
    del _clusters[cluster_id]
    _tags.pop(cluster.get("CacheClusterArn", ""), None)
    _record_event(cluster_id, "cache-cluster", "Cache cluster deleted")
    return _xml_cluster_response("DeleteCacheClusterResponse", "DeleteCacheClusterResult", cluster)


def _describe_cache_clusters(p):
    cluster_id = _p(p, "CacheClusterId")
    if cluster_id:
        cluster = _clusters.get(cluster_id)
        if not cluster:
            return _error("CacheClusterNotFound", f"Cluster {cluster_id} not found", 404)
        clusters = [cluster]
    else:
        clusters = list(_clusters.values())
    members = "".join(_cluster_xml(c) for c in clusters)
    return _xml(200, "DescribeCacheClustersResponse",
        f"<DescribeCacheClustersResult><CacheClusters>{members}</CacheClusters></DescribeCacheClustersResult>")


def _modify_cache_cluster(p):
    cluster_id = _p(p, "CacheClusterId")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("CacheClusterNotFound", f"Cluster {cluster_id} not found", 404)

    if _p(p, "NumCacheNodes"):
        new_count = int(_p(p, "NumCacheNodes"))
        old_count = cluster["NumCacheNodes"]
        cluster["NumCacheNodes"] = new_count
        ep = cluster.get("_endpoint", {})
        if new_count > old_count:
            for i in range(old_count + 1, new_count + 1):
                cluster["CacheNodes"].append({
                    "CacheNodeId": f"{i:04d}",
                    "CacheNodeStatus": "available",
                    "CacheNodeCreateTime": time.time(),
                    "Endpoint": {"Address": ep.get("Address", "localhost"), "Port": ep.get("Port", 6379)},
                    "ParameterGroupStatus": "in-sync",
                    "SourceCacheNodeId": "",
                })
        elif new_count < old_count:
            cluster["CacheNodes"] = cluster["CacheNodes"][:new_count]
    if _p(p, "CacheNodeType"):
        cluster["CacheNodeType"] = _p(p, "CacheNodeType")
    if _p(p, "EngineVersion"):
        cluster["EngineVersion"] = _p(p, "EngineVersion")
    if _p(p, "SnapshotRetentionLimit"):
        cluster["SnapshotRetentionLimit"] = int(_p(p, "SnapshotRetentionLimit"))
    if _p(p, "SnapshotWindow"):
        cluster["SnapshotWindow"] = _p(p, "SnapshotWindow")
    if _p(p, "PreferredMaintenanceWindow"):
        cluster["PreferredMaintenanceWindow"] = _p(p, "PreferredMaintenanceWindow")
    if _p(p, "CacheParameterGroupName"):
        cluster["CacheParameterGroup"]["CacheParameterGroupName"] = _p(p, "CacheParameterGroupName")

    _record_event(cluster_id, "cache-cluster", "Cache cluster modified")
    return _xml_cluster_response("ModifyCacheClusterResponse", "ModifyCacheClusterResult", cluster)


def _reboot_cache_cluster(p):
    cluster_id = _p(p, "CacheClusterId")
    cluster = _clusters.get(cluster_id)
    if not cluster:
        return _error("CacheClusterNotFound", f"Cluster {cluster_id} not found", 404)
    _record_event(cluster_id, "cache-cluster", "Cache cluster rebooted")
    return _xml_cluster_response("RebootCacheClusterResponse", "RebootCacheClusterResult", cluster)


# ---- Replication Groups ----

def _create_replication_group(p):
    rg_id = _p(p, "ReplicationGroupId")
    desc = _p(p, "ReplicationGroupDescription") or ""
    node_type = _p(p, "CacheNodeType") or "cache.t3.micro"
    num_node_groups = int(_p(p, "NumNodeGroups") or "1")
    replicas_per_node_group = int(_p(p, "ReplicasPerNodeGroup") or "1")
    arn = _arn_replication_group(rg_id)
    endpoint_host = REDIS_DEFAULT_HOST
    endpoint_port = REDIS_DEFAULT_PORT

    if rg_id in _replication_groups:
        return _error("ReplicationGroupAlreadyExistsFault",
                       f"Replication group {rg_id} already exists", 400)

    node_groups = []
    for ng_idx in range(1, num_node_groups + 1):
        ng_id = f"{ng_idx:04d}"
        members = []
        for r in range(replicas_per_node_group + 1):
            role = "primary" if r == 0 else "replica"
            members.append({
                "CacheClusterId": f"{rg_id}-{ng_id}-{r + 1:03d}",
                "CacheNodeId": "0001",
                "CurrentRole": role,
                "PreferredAvailabilityZone": f"{get_region()}{'abcdef'[r % 6]}",
                "ReadEndpoint": {"Address": endpoint_host, "Port": endpoint_port},
            })
        node_groups.append({
            "NodeGroupId": ng_id,
            "Status": "available",
            "PrimaryEndpoint": {"Address": endpoint_host, "Port": endpoint_port},
            "ReaderEndpoint": {"Address": endpoint_host, "Port": endpoint_port},
            "NodeGroupMembers": members,
        })

    _replication_groups[rg_id] = {
        "ReplicationGroupId": rg_id,
        "Description": desc,
        "Status": "available",
        "MemberClusters": [],
        "NodeGroups": node_groups,
        "SnapshottingClusterId": "",
        "SnapshotRetentionLimit": int(_p(p, "SnapshotRetentionLimit") or "0"),
        "SnapshotWindow": _p(p, "SnapshotWindow") or "05:00-06:00",
        "ClusterEnabled": num_node_groups > 1,
        "CacheNodeType": node_type,
        "AuthTokenEnabled": _p(p, "AuthToken") != "",
        "TransitEncryptionEnabled": _p(p, "TransitEncryptionEnabled", "false").lower() == "true",
        "AtRestEncryptionEnabled": _p(p, "AtRestEncryptionEnabled", "false").lower() == "true",
        "AutomaticFailover": "enabled" if _p(p, "AutomaticFailoverEnabled", "false").lower() == "true" else "disabled",
        "MultiAZ": "enabled" if _p(p, "MultiAZEnabled", "false").lower() == "true" else "disabled",
        "ConfigurationEndpoint": {"Address": endpoint_host, "Port": endpoint_port} if num_node_groups > 1 else None,
        "ARN": arn,
        "_num_node_groups": num_node_groups,
        "_replicas_per_node_group": replicas_per_node_group,
    }

    tags = _extract_tags(p)
    if tags:
        _tags[arn] = tags

    _record_event(rg_id, "replication-group", "Replication group created")
    return _xml(200, "CreateReplicationGroupResponse",
        f"<CreateReplicationGroupResult><ReplicationGroup>{_rg_xml(_replication_groups[rg_id])}</ReplicationGroup></CreateReplicationGroupResult>")


def _delete_replication_group(p):
    rg_id = _p(p, "ReplicationGroupId")
    rg = _replication_groups.pop(rg_id, None)
    if not rg:
        return _error("ReplicationGroupNotFoundFault", f"Replication group {rg_id} not found", 404)
    _tags.pop(rg.get("ARN", ""), None)
    _record_event(rg_id, "replication-group", "Replication group deleted")
    return _xml(200, "DeleteReplicationGroupResponse",
        f"<DeleteReplicationGroupResult><ReplicationGroup>{_rg_xml(rg)}</ReplicationGroup></DeleteReplicationGroupResult>")


def _describe_replication_groups(p):
    rg_id = _p(p, "ReplicationGroupId")
    if rg_id:
        rg = _replication_groups.get(rg_id)
        if not rg:
            return _error("ReplicationGroupNotFoundFault", f"Replication group {rg_id} not found", 404)
        groups = [rg]
    else:
        groups = list(_replication_groups.values())
    members = "".join(f"<member>{_rg_xml(g)}</member>" for g in groups)
    return _xml(200, "DescribeReplicationGroupsResponse",
        f"<DescribeReplicationGroupsResult><ReplicationGroups>{members}</ReplicationGroups></DescribeReplicationGroupsResult>")


def _modify_replication_group(p):
    rg_id = _p(p, "ReplicationGroupId")
    rg = _replication_groups.get(rg_id)
    if not rg:
        return _error("ReplicationGroupNotFoundFault", f"Replication group {rg_id} not found", 404)

    if _p(p, "ReplicationGroupDescription"):
        rg["Description"] = _p(p, "ReplicationGroupDescription")
    if _p(p, "CacheNodeType"):
        rg["CacheNodeType"] = _p(p, "CacheNodeType")
    if _p(p, "SnapshotRetentionLimit"):
        rg["SnapshotRetentionLimit"] = int(_p(p, "SnapshotRetentionLimit"))
    if _p(p, "SnapshotWindow"):
        rg["SnapshotWindow"] = _p(p, "SnapshotWindow")
    if _p(p, "AutomaticFailoverEnabled"):
        rg["AutomaticFailover"] = "enabled" if _p(p, "AutomaticFailoverEnabled").lower() == "true" else "disabled"
    if _p(p, "MultiAZEnabled"):
        rg["MultiAZ"] = "enabled" if _p(p, "MultiAZEnabled").lower() == "true" else "disabled"
    if _p(p, "EngineVersion"):
        rg["EngineVersion"] = _p(p, "EngineVersion")
    if _p(p, "CacheParameterGroupName"):
        rg["CacheParameterGroupName"] = _p(p, "CacheParameterGroupName")

    _record_event(rg_id, "replication-group", "Replication group modified")
    return _xml(200, "ModifyReplicationGroupResponse",
        f"<ModifyReplicationGroupResult><ReplicationGroup>{_rg_xml(rg)}</ReplicationGroup></ModifyReplicationGroupResult>")


def _increase_replica_count(p):
    rg_id = _p(p, "ReplicationGroupId")
    rg = _replication_groups.get(rg_id)
    if not rg:
        return _error("ReplicationGroupNotFoundFault", f"Replication group {rg_id} not found", 404)

    new_count = int(_p(p, "NewReplicaCount") or "0")
    if new_count <= 0:
        return _error("InvalidParameterValue", "NewReplicaCount must be positive", 400)

    endpoint_host = REDIS_DEFAULT_HOST
    endpoint_port = REDIS_DEFAULT_PORT
    for ng in rg["NodeGroups"]:
        current = len(ng.get("NodeGroupMembers", []))
        target = new_count + 1  # +1 for primary
        while current < target:
            current += 1
            ng["NodeGroupMembers"].append({
                "CacheClusterId": f"{rg_id}-{ng['NodeGroupId']}-{current:03d}",
                "CacheNodeId": "0001",
                "CurrentRole": "replica",
                "PreferredAvailabilityZone": f"{get_region()}a",
                "ReadEndpoint": {"Address": endpoint_host, "Port": endpoint_port},
            })
    rg["_replicas_per_node_group"] = new_count

    _record_event(rg_id, "replication-group", "Replica count increased")
    return _xml(200, "IncreaseReplicaCountResponse",
        f"<IncreaseReplicaCountResult><ReplicationGroup>{_rg_xml(rg)}</ReplicationGroup></IncreaseReplicaCountResult>")


def _decrease_replica_count(p):
    rg_id = _p(p, "ReplicationGroupId")
    rg = _replication_groups.get(rg_id)
    if not rg:
        return _error("ReplicationGroupNotFoundFault", f"Replication group {rg_id} not found", 404)

    new_count = int(_p(p, "NewReplicaCount") or "0")
    if new_count < 0:
        return _error("InvalidParameterValue", "NewReplicaCount must be non-negative", 400)

    for ng in rg["NodeGroups"]:
        target = new_count + 1
        members = ng.get("NodeGroupMembers", [])
        if len(members) > target:
            ng["NodeGroupMembers"] = members[:target]
    rg["_replicas_per_node_group"] = new_count

    _record_event(rg_id, "replication-group", "Replica count decreased")
    return _xml(200, "DecreaseReplicaCountResponse",
        f"<DecreaseReplicaCountResult><ReplicationGroup>{_rg_xml(rg)}</ReplicationGroup></DecreaseReplicaCountResult>")


# ---- Subnet Groups ----

def _create_subnet_group(p):
    name = _p(p, "CacheSubnetGroupName")
    desc = _p(p, "CacheSubnetGroupDescription") or ""
    arn = _arn_subnet_group(name)

    subnets = []
    idx = 1
    while _p(p, f"SubnetIds.member.{idx}"):
        subnets.append({
            "SubnetIdentifier": _p(p, f"SubnetIds.member.{idx}"),
            "SubnetAvailabilityZone": {"Name": f"{get_region()}{'abcdef'[(idx - 1) % 6]}"},
        })
        idx += 1

    _subnet_groups[name] = {
        "CacheSubnetGroupName": name,
        "CacheSubnetGroupDescription": desc,
        "VpcId": "vpc-00000000",
        "Subnets": subnets,
        "ARN": arn,
    }
    subnets_xml = "".join(
        f"<Subnet><SubnetIdentifier>{s['SubnetIdentifier']}</SubnetIdentifier>"
        f"<SubnetAvailabilityZone><Name>{s['SubnetAvailabilityZone']['Name']}</Name></SubnetAvailabilityZone>"
        f"</Subnet>" for s in subnets
    )
    return _xml(200, "CreateCacheSubnetGroupResponse",
        f"<CreateCacheSubnetGroupResult><CacheSubnetGroup>"
        f"<CacheSubnetGroupName>{name}</CacheSubnetGroupName>"
        f"<CacheSubnetGroupDescription>{desc}</CacheSubnetGroupDescription>"
        f"<Subnets>{subnets_xml}</Subnets>"
        f"<ARN>{arn}</ARN>"
        f"</CacheSubnetGroup></CreateCacheSubnetGroupResult>")


def _describe_subnet_groups(p):
    name = _p(p, "CacheSubnetGroupName")
    if name and name not in _subnet_groups:
        return _error("CacheSubnetGroupNotFoundFault", f"Cache subnet group {name} not found.", 404)
    groups = [_subnet_groups[name]] if name and name in _subnet_groups else list(_subnet_groups.values())
    members = ""
    for g in groups:
        subnets_xml = "".join(
            f"<member><SubnetIdentifier>{s['SubnetIdentifier']}</SubnetIdentifier>"
            f"<SubnetAvailabilityZone><Name>{s['SubnetAvailabilityZone']['Name']}</Name></SubnetAvailabilityZone></member>"
            for s in g.get("Subnets", [])
        )
        members += (
            f"<member><CacheSubnetGroupName>{g['CacheSubnetGroupName']}</CacheSubnetGroupName>"
            f"<CacheSubnetGroupDescription>{g.get('CacheSubnetGroupDescription', '')}</CacheSubnetGroupDescription>"
            f"<VpcId>{g.get('VpcId', '')}</VpcId>"
            f"<Subnets>{subnets_xml}</Subnets>"
            f"<ARN>{g.get('ARN', '')}</ARN></member>"
        )
    return _xml(200, "DescribeCacheSubnetGroupsResponse",
        f"<DescribeCacheSubnetGroupsResult><CacheSubnetGroups>{members}</CacheSubnetGroups></DescribeCacheSubnetGroupsResult>")


def _delete_subnet_group(p):
    name = _p(p, "CacheSubnetGroupName")
    if name not in _subnet_groups:
        return _error("CacheSubnetGroupNotFoundFault", f"Cache subnet group {name} not found.", 404)
    sg = _subnet_groups.pop(name, None)
    if sg:
        _tags.pop(sg.get("ARN", ""), None)
    return _xml(200, "DeleteCacheSubnetGroupResponse", "")


def _modify_subnet_group(p):
    name = _p(p, "CacheSubnetGroupName")
    sg = _subnet_groups.get(name)
    if not sg:
        return _error("CacheSubnetGroupNotFoundFault", f"Subnet group {name} not found", 404)

    if _p(p, "CacheSubnetGroupDescription"):
        sg["CacheSubnetGroupDescription"] = _p(p, "CacheSubnetGroupDescription")

    subnets = []
    idx = 1
    while _p(p, f"SubnetIds.member.{idx}"):
        subnets.append({
            "SubnetIdentifier": _p(p, f"SubnetIds.member.{idx}"),
            "SubnetAvailabilityZone": {"Name": f"{get_region()}{'abcdef'[(idx - 1) % 6]}"},
        })
        idx += 1
    if subnets:
        sg["Subnets"] = subnets

    arn = sg.get("ARN", _arn_subnet_group(name))
    return _xml(200, "ModifyCacheSubnetGroupResponse",
        f"<ModifyCacheSubnetGroupResult><CacheSubnetGroup>"
        f"<CacheSubnetGroupName>{name}</CacheSubnetGroupName>"
        f"<CacheSubnetGroupDescription>{sg.get('CacheSubnetGroupDescription', '')}</CacheSubnetGroupDescription>"
        f"<ARN>{arn}</ARN>"
        f"</CacheSubnetGroup></ModifyCacheSubnetGroupResult>")


# ---- Parameter Groups ----

def _create_param_group(p):
    name = _p(p, "CacheParameterGroupName")
    family = _p(p, "CacheParameterGroupFamily") or "redis7.0"
    desc = _p(p, "Description") or ""
    arn = _arn_param_group(name)
    _param_groups[name] = {
        "CacheParameterGroupName": name,
        "CacheParameterGroupFamily": family,
        "Description": desc,
        "IsGlobal": False,
        "ARN": arn,
    }
    _param_group_params[name] = _default_params_for_family(family)
    return _xml(200, "CreateCacheParameterGroupResponse",
        f"<CreateCacheParameterGroupResult><CacheParameterGroup>"
        f"<CacheParameterGroupName>{name}</CacheParameterGroupName>"
        f"<CacheParameterGroupFamily>{family}</CacheParameterGroupFamily>"
        f"<Description>{desc}</Description>"
        f"<ARN>{arn}</ARN>"
        f"</CacheParameterGroup></CreateCacheParameterGroupResult>")


def _describe_param_groups(p):
    name = _p(p, "CacheParameterGroupName")
    if name and name not in _param_groups:
        return _error("CacheParameterGroupNotFound", f"Cache parameter group {name} not found.", 404)
    groups = [_param_groups[name]] if name and name in _param_groups else list(_param_groups.values())
    members = "".join(
        f"<member><CacheParameterGroupName>{g['CacheParameterGroupName']}</CacheParameterGroupName>"
        f"<CacheParameterGroupFamily>{g.get('CacheParameterGroupFamily', '')}</CacheParameterGroupFamily>"
        f"<Description>{g.get('Description', '')}</Description>"
        f"<ARN>{g.get('ARN', '')}</ARN></member>"
        for g in groups
    )
    return _xml(200, "DescribeCacheParameterGroupsResponse",
        f"<DescribeCacheParameterGroupsResult><CacheParameterGroups>{members}</CacheParameterGroups></DescribeCacheParameterGroupsResult>")


def _delete_param_group(p):
    name = _p(p, "CacheParameterGroupName")
    if name not in _param_groups:
        return _error("CacheParameterGroupNotFound", f"Cache parameter group {name} not found.", 404)
    pg = _param_groups.pop(name, None)
    _param_group_params.pop(name, None)
    if pg:
        _tags.pop(pg.get("ARN", ""), None)
    return _xml(200, "DeleteCacheParameterGroupResponse", "")


def _describe_cache_parameters(p):
    name = _p(p, "CacheParameterGroupName")
    if name not in _param_groups:
        return _error("CacheParameterGroupNotFound",
                       f"Parameter group {name} not found", 404)
    params = _param_group_params.get(name, {})
    members = ""
    for pname, pval in params.items():
        members += (
            f"<member>"
            f"<ParameterName>{pname}</ParameterName>"
            f"<ParameterValue>{pval.get('Value', '')}</ParameterValue>"
            f"<Description>{pval.get('Description', '')}</Description>"
            f"<Source>{pval.get('Source', 'system')}</Source>"
            f"<DataType>{pval.get('DataType', 'string')}</DataType>"
            f"<AllowedValues>{pval.get('AllowedValues', '')}</AllowedValues>"
            f"<IsModifiable>{str(pval.get('IsModifiable', True)).lower()}</IsModifiable>"
            f"<MinimumEngineVersion>{pval.get('MinimumEngineVersion', '5.0.0')}</MinimumEngineVersion>"
            f"</member>"
        )
    return _xml(200, "DescribeCacheParametersResponse",
        f"<DescribeCacheParametersResult><Parameters>{members}</Parameters></DescribeCacheParametersResult>")


def _modify_cache_parameter_group(p):
    name = _p(p, "CacheParameterGroupName")
    if name not in _param_groups:
        return _error("CacheParameterGroupNotFound",
                       f"Parameter group {name} not found", 404)
    params = _param_group_params.setdefault(name, {})

    idx = 1
    while _p(p, f"ParameterNameValues.ParameterNameValue.{idx}.ParameterName"):
        pname = _p(p, f"ParameterNameValues.ParameterNameValue.{idx}.ParameterName")
        pvalue = _p(p, f"ParameterNameValues.ParameterNameValue.{idx}.ParameterValue")
        if pname in params:
            params[pname]["Value"] = pvalue
            params[pname]["Source"] = "user"
        else:
            params[pname] = {"Value": pvalue, "Source": "user", "DataType": "string",
                             "Description": "", "IsModifiable": True}
        idx += 1

    return _xml(200, "ModifyCacheParameterGroupResponse",
        f"<ModifyCacheParameterGroupResult>"
        f"<CacheParameterGroupName>{name}</CacheParameterGroupName>"
        f"</ModifyCacheParameterGroupResult>")


def _reset_cache_parameter_group(p):
    name = _p(p, "CacheParameterGroupName")
    if name not in _param_groups:
        return _error("CacheParameterGroupNotFound",
                       f"Parameter group {name} not found", 404)

    reset_all = _p(p, "ResetAllParameters", "false").lower() == "true"
    family = _param_groups[name].get("CacheParameterGroupFamily", "redis7.0")

    if reset_all:
        _param_group_params[name] = _default_params_for_family(family)
    else:
        defaults = _default_params_for_family(family)
        params = _param_group_params.get(name, {})
        idx = 1
        while _p(p, f"ParameterNameValues.ParameterNameValue.{idx}.ParameterName"):
            pname = _p(p, f"ParameterNameValues.ParameterNameValue.{idx}.ParameterName")
            if pname in defaults:
                params[pname] = dict(defaults[pname])
            idx += 1

    return _xml(200, "ResetCacheParameterGroupResponse",
        f"<ResetCacheParameterGroupResult>"
        f"<CacheParameterGroupName>{name}</CacheParameterGroupName>"
        f"</ResetCacheParameterGroupResult>")


def _default_params_for_family(family):
    """Seed with commonly queried Redis/Memcached default parameters."""
    if family.startswith("redis"):
        return {
            "maxmemory-policy": {"Value": "volatile-lru", "Description": "Eviction policy",
                                 "Source": "system", "DataType": "string",
                                 "AllowedValues": "volatile-lru,allkeys-lru,volatile-random,allkeys-random,volatile-ttl,noeviction",
                                 "IsModifiable": True, "MinimumEngineVersion": "2.8.6"},
            "maxmemory-samples": {"Value": "5", "Description": "Number of keys to sample",
                                  "Source": "system", "DataType": "integer",
                                  "AllowedValues": "1-", "IsModifiable": True, "MinimumEngineVersion": "2.8.6"},
            "timeout": {"Value": "0", "Description": "Close connection after N seconds idle",
                        "Source": "system", "DataType": "integer",
                        "AllowedValues": "0-", "IsModifiable": True, "MinimumEngineVersion": "2.6.13"},
            "tcp-keepalive": {"Value": "300", "Description": "TCP keepalive",
                              "Source": "system", "DataType": "integer",
                              "AllowedValues": "0-", "IsModifiable": True, "MinimumEngineVersion": "2.6.13"},
            "databases": {"Value": "16", "Description": "Number of databases",
                          "Source": "system", "DataType": "integer",
                          "AllowedValues": "1-1200000", "IsModifiable": True, "MinimumEngineVersion": "2.6.13"},
        }
    return {
        "max_simultaneous_connections_per_server": {"Value": "8", "Source": "system",
            "DataType": "integer", "Description": "Max connections", "IsModifiable": True},
    }


# ---- Engine Versions ----

def _describe_engine_versions(p):
    engine = _p(p, "Engine") or "redis"
    versions = {"redis": ["7.1.0", "7.0.12", "6.2.14", "5.0.6"], "memcached": ["1.6.22", "1.6.17", "1.6.12"]}
    members = "".join(
        f"<member><Engine>{engine}</Engine><EngineVersion>{v}</EngineVersion>"
        f"<CacheParameterGroupFamily>{engine}{v[:3]}</CacheParameterGroupFamily></member>"
        for v in versions.get(engine, ["7.0.12"])
    )
    return _xml(200, "DescribeCacheEngineVersionsResponse",
        f"<DescribeCacheEngineVersionsResult><CacheEngineVersions>{members}</CacheEngineVersions></DescribeCacheEngineVersionsResult>")


# ---- Tags ----

def _extract_tags(p):
    """Extract Tags.member.N or Tags.Tag.N format from query params."""
    tags = []
    for prefix in ("Tags.member", "Tags.Tag"):
        idx = 1
        while _p(p, f"{prefix}.{idx}.Key"):
            tags.append({
                "Key": _p(p, f"{prefix}.{idx}.Key"),
                "Value": _p(p, f"{prefix}.{idx}.Value") or "",
            })
            idx += 1
        if tags:
            break
    return tags


def _list_tags(p):
    arn = _p(p, "ResourceName")
    tags = _tags.get(arn, [])
    tag_xml = "".join(f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>" for t in tags)
    return _xml(200, "ListTagsForResourceResponse",
        f"<ListTagsForResourceResult><TagList>{tag_xml}</TagList></ListTagsForResourceResult>")


def _add_tags(p):
    arn = _p(p, "ResourceName")
    new_tags = _extract_tags(p)
    existing = _tags.setdefault(arn, [])
    existing_keys = {t["Key"] for t in existing}
    for t in new_tags:
        if t["Key"] in existing_keys:
            for e in existing:
                if e["Key"] == t["Key"]:
                    e["Value"] = t["Value"]
                    break
        else:
            existing.append(t)
            existing_keys.add(t["Key"])

    tag_xml = "".join(f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>" for t in existing)
    return _xml(200, "AddTagsToResourceResponse",
        f"<AddTagsToResourceResult><TagList>{tag_xml}</TagList></AddTagsToResourceResult>")


def _remove_tags(p):
    arn = _p(p, "ResourceName")
    keys_to_remove = set()
    idx = 1
    while _p(p, f"TagKeys.member.{idx}"):
        keys_to_remove.add(_p(p, f"TagKeys.member.{idx}"))
        idx += 1
    if arn in _tags:
        _tags[arn] = [t for t in _tags[arn] if t["Key"] not in keys_to_remove]

    tags = _tags.get(arn, [])
    tag_xml = "".join(f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>" for t in tags)
    return _xml(200, "RemoveTagsFromResourceResponse",
        f"<RemoveTagsFromResourceResult><TagList>{tag_xml}</TagList></RemoveTagsFromResourceResult>")


# ---- Snapshots ----

def _create_snapshot(p):
    snapshot_name = _p(p, "SnapshotName")
    cluster_id = _p(p, "CacheClusterId")
    rg_id = _p(p, "ReplicationGroupId")

    if snapshot_name in _snapshots:
        return _error("SnapshotAlreadyExistsFault", f"Snapshot {snapshot_name} already exists", 400)

    source_id = cluster_id or rg_id
    arn = _arn_snapshot(snapshot_name)
    _snapshots[snapshot_name] = {
        "SnapshotName": snapshot_name,
        "SnapshotStatus": "available",
        "SnapshotSource": "manual",
        "CacheClusterId": cluster_id,
        "ReplicationGroupId": rg_id,
        "CacheNodeType": "cache.t3.micro",
        "Engine": "redis",
        "EngineVersion": "7.0.12",
        "SnapshotRetentionLimit": 0,
        "SnapshotWindow": "05:00-06:00",
        "NodeSnapshots": [{"CacheNodeId": "0001", "SnapshotCreateTime": time.time(),
                           "CacheSize": "0 MB"}],
        "ARN": arn,
        "CreateTime": time.time(),
    }

    if source_id:
        cluster = _clusters.get(source_id) or {}
        rg = _replication_groups.get(source_id) or {}
        src = cluster or rg
        if src:
            _snapshots[snapshot_name]["CacheNodeType"] = src.get("CacheNodeType", "cache.t3.micro")
            _snapshots[snapshot_name]["Engine"] = src.get("Engine", "redis")
            _snapshots[snapshot_name]["EngineVersion"] = src.get("EngineVersion", "7.0.12")

    _record_event(snapshot_name, "snapshot", "Snapshot created")
    return _xml(200, "CreateSnapshotResponse",
        f"<CreateSnapshotResult><Snapshot>{_snapshot_xml(_snapshots[snapshot_name])}</Snapshot></CreateSnapshotResult>")


def _delete_snapshot(p):
    snapshot_name = _p(p, "SnapshotName")
    snap = _snapshots.pop(snapshot_name, None)
    if not snap:
        return _error("SnapshotNotFoundFault", f"Snapshot {snapshot_name} not found", 404)
    _tags.pop(snap.get("ARN", ""), None)
    snap["SnapshotStatus"] = "deleting"
    _record_event(snapshot_name, "snapshot", "Snapshot deleted")
    return _xml(200, "DeleteSnapshotResponse",
        f"<DeleteSnapshotResult><Snapshot>{_snapshot_xml(snap)}</Snapshot></DeleteSnapshotResult>")


def _describe_snapshots(p):
    snapshot_name = _p(p, "SnapshotName")
    cluster_id = _p(p, "CacheClusterId")
    rg_id = _p(p, "ReplicationGroupId")

    snaps = list(_snapshots.values())
    if snapshot_name:
        snaps = [s for s in snaps if s["SnapshotName"] == snapshot_name]
    if cluster_id:
        snaps = [s for s in snaps if s.get("CacheClusterId") == cluster_id]
    if rg_id:
        snaps = [s for s in snaps if s.get("ReplicationGroupId") == rg_id]

    members = "".join(f"<member>{_snapshot_xml(s)}</member>" for s in snaps)
    return _xml(200, "DescribeSnapshotsResponse",
        f"<DescribeSnapshotsResult><Snapshots>{members}</Snapshots></DescribeSnapshotsResult>")


# ---- Events ----

def _describe_events(p):
    source_id = _p(p, "SourceIdentifier")
    source_type = _p(p, "SourceType")
    max_records = int(_p(p, "MaxRecords") or "100")

    filtered = _events_list()
    if source_id:
        filtered = [e for e in filtered if e["SourceIdentifier"] == source_id]
    if source_type:
        filtered = [e for e in filtered if e["SourceType"] == source_type]

    filtered = filtered[-max_records:]
    members = "".join(
        f"<member>"
        f"<SourceIdentifier>{e['SourceIdentifier']}</SourceIdentifier>"
        f"<SourceType>{e['SourceType']}</SourceType>"
        f"<Message>{e['Message']}</Message>"
        f"<Date>{e['Date']}</Date>"
        f"</member>"
        for e in filtered
    )
    return _xml(200, "DescribeEventsResponse",
        f"<DescribeEventsResult><Events>{members}</Events></DescribeEventsResult>")


# ---- Users (Redis ACL) ----

def _arn_user(user_id):
    return f"arn:aws:elasticache:{get_region()}:{get_account_id()}:user:{user_id}"


def _arn_user_group(group_id):
    return f"arn:aws:elasticache:{get_region()}:{get_account_id()}:usergroup:{group_id}"


def _create_user(p):
    user_id = _p(p, "UserId")
    if not user_id:
        return _error("InvalidParameterValue", "UserId is required", 400)
    if user_id in _users:
        return _error("UserAlreadyExistsFault", f"User {user_id} already exists", 400)

    arn = _arn_user(user_id)
    user = {
        "UserId": user_id,
        "UserName": _p(p, "UserName") or user_id,
        "Engine": _p(p, "Engine") or "redis",
        "Status": "active",
        "AccessString": _p(p, "AccessString") or "on ~* +@all",
        "UserGroupIds": [],
        "Authentication": {"Type": "password", "PasswordCount": 1} if _p(p, "Passwords.member.1") else {"Type": "no-password", "PasswordCount": 0},
        "ARN": arn,
    }
    _users[user_id] = user

    tags = _extract_tags(p)
    if tags:
        _tags[arn] = tags

    return _xml(200, "CreateUserResponse", f"<CreateUserResult>{_user_xml(user)}</CreateUserResult>")


def _describe_users(p):
    user_id = _p(p, "UserId")
    engine = _p(p, "Engine")

    if user_id:
        user = _users.get(user_id)
        if not user:
            return _error("UserNotFoundFault", f"User {user_id} not found", 404)
        users = [user]
    else:
        users = list(_users.values())
        if engine:
            users = [u for u in users if u.get("Engine") == engine]

    members = "".join(f"<member>{_user_xml(u)}</member>" for u in users)
    return _xml(200, "DescribeUsersResponse",
        f"<DescribeUsersResult><Users>{members}</Users></DescribeUsersResult>")


def _delete_user(p):
    user_id = _p(p, "UserId")
    user = _users.pop(user_id, None)
    if not user:
        return _error("UserNotFoundFault", f"User {user_id} not found", 404)
    _tags.pop(user.get("ARN", ""), None)
    user["Status"] = "deleting"
    return _xml(200, "DeleteUserResponse", f"<DeleteUserResult>{_user_xml(user)}</DeleteUserResult>")


def _modify_user(p):
    user_id = _p(p, "UserId")
    user = _users.get(user_id)
    if not user:
        return _error("UserNotFoundFault", f"User {user_id} not found", 404)

    if _p(p, "AccessString"):
        user["AccessString"] = _p(p, "AccessString")
    if _p(p, "Passwords.member.1"):
        user["Authentication"] = {"Type": "password", "PasswordCount": 1}

    return _xml(200, "ModifyUserResponse", f"<ModifyUserResult>{_user_xml(user)}</ModifyUserResult>")


def _create_user_group(p):
    group_id = _p(p, "UserGroupId")
    if not group_id:
        return _error("InvalidParameterValue", "UserGroupId is required", 400)
    if group_id in _user_groups:
        return _error("UserGroupAlreadyExistsFault", f"User group {group_id} already exists", 400)

    arn = _arn_user_group(group_id)
    user_ids = []
    idx = 1
    while _p(p, f"UserIds.member.{idx}"):
        user_ids.append(_p(p, f"UserIds.member.{idx}"))
        idx += 1

    group = {
        "UserGroupId": group_id,
        "Status": "active",
        "Engine": _p(p, "Engine") or "redis",
        "UserIds": user_ids,
        "PendingChanges": {},
        "ReplicationGroups": [],
        "ARN": arn,
    }
    _user_groups[group_id] = group

    for uid in user_ids:
        if uid in _users:
            _users[uid].setdefault("UserGroupIds", []).append(group_id)

    tags = _extract_tags(p)
    if tags:
        _tags[arn] = tags

    return _xml(200, "CreateUserGroupResponse", f"<CreateUserGroupResult>{_user_group_xml(group)}</CreateUserGroupResult>")


def _describe_user_groups(p):
    group_id = _p(p, "UserGroupId")

    if group_id:
        group = _user_groups.get(group_id)
        if not group:
            return _error("UserGroupNotFoundFault", f"User group {group_id} not found", 404)
        groups = [group]
    else:
        groups = list(_user_groups.values())

    members = "".join(f"<member>{_user_group_xml(g)}</member>" for g in groups)
    return _xml(200, "DescribeUserGroupsResponse",
        f"<DescribeUserGroupsResult><UserGroups>{members}</UserGroups></DescribeUserGroupsResult>")


def _delete_user_group(p):
    group_id = _p(p, "UserGroupId")
    group = _user_groups.pop(group_id, None)
    if not group:
        return _error("UserGroupNotFoundFault", f"User group {group_id} not found", 404)
    _tags.pop(group.get("ARN", ""), None)

    for uid in group.get("UserIds", []):
        if uid in _users:
            gids = _users[uid].get("UserGroupIds", [])
            if group_id in gids:
                gids.remove(group_id)

    group["Status"] = "deleting"
    return _xml(200, "DeleteUserGroupResponse", f"<DeleteUserGroupResult>{_user_group_xml(group)}</DeleteUserGroupResult>")


def _modify_user_group(p):
    group_id = _p(p, "UserGroupId")
    group = _user_groups.get(group_id)
    if not group:
        return _error("UserGroupNotFoundFault", f"User group {group_id} not found", 404)

    to_add = []
    idx = 1
    while _p(p, f"UserIdsToAdd.member.{idx}"):
        to_add.append(_p(p, f"UserIdsToAdd.member.{idx}"))
        idx += 1

    to_remove = []
    idx = 1
    while _p(p, f"UserIdsToRemove.member.{idx}"):
        to_remove.append(_p(p, f"UserIdsToRemove.member.{idx}"))
        idx += 1

    for uid in to_add:
        if uid not in group["UserIds"]:
            group["UserIds"].append(uid)
        if uid in _users:
            _users[uid].setdefault("UserGroupIds", []).append(group_id)

    for uid in to_remove:
        if uid in group["UserIds"]:
            group["UserIds"].remove(uid)
        if uid in _users:
            gids = _users[uid].get("UserGroupIds", [])
            if group_id in gids:
                gids.remove(group_id)

    return _xml(200, "ModifyUserGroupResponse", f"<ModifyUserGroupResult>{_user_group_xml(group)}</ModifyUserGroupResult>")


def _user_xml(u):
    group_ids_xml = "".join(f"<member>{gid}</member>" for gid in u.get("UserGroupIds", []))
    auth = u.get("Authentication", {})
    return (
        f"<UserId>{u['UserId']}</UserId>"
        f"<UserName>{u.get('UserName', '')}</UserName>"
        f"<Engine>{u.get('Engine', 'redis')}</Engine>"
        f"<Status>{u.get('Status', 'active')}</Status>"
        f"<AccessString>{u.get('AccessString', '')}</AccessString>"
        f"<UserGroupIds>{group_ids_xml}</UserGroupIds>"
        f"<Authentication><Type>{auth.get('Type', 'no-password')}</Type>"
        f"<PasswordCount>{auth.get('PasswordCount', 0)}</PasswordCount></Authentication>"
        f"<ARN>{u.get('ARN', '')}</ARN>"
    )


def _user_group_xml(g):
    user_ids_xml = "".join(f"<member>{uid}</member>" for uid in g.get("UserIds", []))
    rg_xml = "".join(f"<member>{rg}</member>" for rg in g.get("ReplicationGroups", []))
    return (
        f"<UserGroupId>{g['UserGroupId']}</UserGroupId>"
        f"<Status>{g.get('Status', 'active')}</Status>"
        f"<Engine>{g.get('Engine', 'redis')}</Engine>"
        f"<UserIds>{user_ids_xml}</UserIds>"
        f"<ReplicationGroups>{rg_xml}</ReplicationGroups>"
        f"<ARN>{g.get('ARN', '')}</ARN>"
    )


# ---- XML helpers ----

def _cluster_xml_inner(c):
    """Render cluster fields — no wrapping element."""
    ep = c.get("_endpoint", {})
    nodes_xml = ""
    for node in c.get("CacheNodes", []):
        nep = node.get("Endpoint", {})
        nodes_xml += (
            f"<member>"
            f"<CacheNodeId>{node['CacheNodeId']}</CacheNodeId>"
            f"<CacheNodeStatus>{node['CacheNodeStatus']}</CacheNodeStatus>"
            f"<Endpoint><Address>{nep.get('Address', 'localhost')}</Address>"
            f"<Port>{nep.get('Port', 6379)}</Port></Endpoint>"
            f"</member>"
        )
    return (
        f"<CacheClusterId>{c['CacheClusterId']}</CacheClusterId>"
        f"<CacheClusterStatus>{c['CacheClusterStatus']}</CacheClusterStatus>"
        f"<Engine>{c['Engine']}</Engine>"
        f"<EngineVersion>{c['EngineVersion']}</EngineVersion>"
        f"<CacheNodeType>{c['CacheNodeType']}</CacheNodeType>"
        f"<NumCacheNodes>{c['NumCacheNodes']}</NumCacheNodes>"
        f"<CacheClusterArn>{c['CacheClusterArn']}</CacheClusterArn>"
        f"<ARN>{c.get('CacheClusterArn', '')}</ARN>"
        f"<PreferredAvailabilityZone>{c.get('PreferredAvailabilityZone', '')}</PreferredAvailabilityZone>"
        f"<CacheSubnetGroupName>{c.get('CacheSubnetGroupName', '')}</CacheSubnetGroupName>"
        f"<ReplicationGroupId>{c.get('ReplicationGroupId', '')}</ReplicationGroupId>"
        f"<SnapshotRetentionLimit>{c.get('SnapshotRetentionLimit', 0)}</SnapshotRetentionLimit>"
        f"<SnapshotWindow>{c.get('SnapshotWindow', '')}</SnapshotWindow>"
        f"<CacheNodes>{nodes_xml}</CacheNodes>"
    )


def _cluster_xml(c):
    """For list contexts (DescribeCacheClusters), wrap in <member>."""
    return f"<member>{_cluster_xml_inner(c)}</member>"


def _rg_xml(rg):
    node_groups_xml = ""
    for ng in rg.get("NodeGroups", []):
        members_xml = ""
        for m in ng.get("NodeGroupMembers", []):
            rep = m.get("ReadEndpoint", {})
            members_xml += (
                f"<member>"
                f"<CacheClusterId>{m.get('CacheClusterId', '')}</CacheClusterId>"
                f"<CacheNodeId>{m.get('CacheNodeId', '0001')}</CacheNodeId>"
                f"<CurrentRole>{m.get('CurrentRole', 'primary')}</CurrentRole>"
                f"<PreferredAvailabilityZone>{m.get('PreferredAvailabilityZone', '')}</PreferredAvailabilityZone>"
                f"<ReadEndpoint><Address>{rep.get('Address', 'localhost')}</Address>"
                f"<Port>{rep.get('Port', 6379)}</Port></ReadEndpoint>"
                f"</member>"
            )
        pep = ng.get("PrimaryEndpoint", {})
        rdr = ng.get("ReaderEndpoint", {})
        node_groups_xml += (
            f"<member>"
            f"<NodeGroupId>{ng['NodeGroupId']}</NodeGroupId>"
            f"<Status>{ng['Status']}</Status>"
            f"<PrimaryEndpoint><Address>{pep.get('Address', 'localhost')}</Address>"
            f"<Port>{pep.get('Port', 6379)}</Port></PrimaryEndpoint>"
            f"<ReaderEndpoint><Address>{rdr.get('Address', 'localhost')}</Address>"
            f"<Port>{rdr.get('Port', 6379)}</Port></ReaderEndpoint>"
            f"<NodeGroupMembers>{members_xml}</NodeGroupMembers>"
            f"</member>"
        )

    config_ep_xml = ""
    cep = rg.get("ConfigurationEndpoint")
    if cep:
        config_ep_xml = (
            f"<ConfigurationEndpoint><Address>{cep['Address']}</Address>"
            f"<Port>{cep['Port']}</Port></ConfigurationEndpoint>"
        )

    return (
        f"<ReplicationGroupId>{rg['ReplicationGroupId']}</ReplicationGroupId>"
        f"<Description>{rg.get('Description', '')}</Description>"
        f"<Status>{rg['Status']}</Status>"
        f"<CacheNodeType>{rg.get('CacheNodeType', 'cache.t3.micro')}</CacheNodeType>"
        f"<AutomaticFailover>{rg.get('AutomaticFailover', 'disabled')}</AutomaticFailover>"
        f"<MultiAZ>{rg.get('MultiAZ', 'disabled')}</MultiAZ>"
        f"<ClusterEnabled>{str(rg.get('ClusterEnabled', False)).lower()}</ClusterEnabled>"
        f"<AuthTokenEnabled>{str(rg.get('AuthTokenEnabled', False)).lower()}</AuthTokenEnabled>"
        f"<TransitEncryptionEnabled>{str(rg.get('TransitEncryptionEnabled', False)).lower()}</TransitEncryptionEnabled>"
        f"<AtRestEncryptionEnabled>{str(rg.get('AtRestEncryptionEnabled', False)).lower()}</AtRestEncryptionEnabled>"
        f"<SnapshotRetentionLimit>{rg.get('SnapshotRetentionLimit', 0)}</SnapshotRetentionLimit>"
        f"<SnapshotWindow>{rg.get('SnapshotWindow', '')}</SnapshotWindow>"
        f"{config_ep_xml}"
        f"<NodeGroups>{node_groups_xml}</NodeGroups>"
        f"<ARN>{rg['ARN']}</ARN>"
    )


def _snapshot_xml(snap):
    nodes_xml = ""
    for ns in snap.get("NodeSnapshots", []):
        nodes_xml += (
            f"<member>"
            f"<CacheNodeId>{ns.get('CacheNodeId', '0001')}</CacheNodeId>"
            f"<SnapshotCreateTime>{ns.get('SnapshotCreateTime', 0)}</SnapshotCreateTime>"
            f"<CacheSize>{ns.get('CacheSize', '0 MB')}</CacheSize>"
            f"</member>"
        )
    return (
        f"<SnapshotName>{snap['SnapshotName']}</SnapshotName>"
        f"<SnapshotStatus>{snap['SnapshotStatus']}</SnapshotStatus>"
        f"<SnapshotSource>{snap.get('SnapshotSource', 'manual')}</SnapshotSource>"
        f"<CacheClusterId>{snap.get('CacheClusterId', '')}</CacheClusterId>"
        f"<ReplicationGroupId>{snap.get('ReplicationGroupId', '')}</ReplicationGroupId>"
        f"<CacheNodeType>{snap.get('CacheNodeType', 'cache.t3.micro')}</CacheNodeType>"
        f"<Engine>{snap.get('Engine', 'redis')}</Engine>"
        f"<EngineVersion>{snap.get('EngineVersion', '7.0.12')}</EngineVersion>"
        f"<NodeSnapshots>{nodes_xml}</NodeSnapshots>"
        f"<ARN>{snap.get('ARN', '')}</ARN>"
    )


def _xml_cluster_response(root_tag, result_tag, cluster):
    return _xml(200, root_tag, f"<{result_tag}><CacheCluster>{_cluster_xml_inner(cluster)}</CacheCluster></{result_tag}>")


def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _xml(status, root_tag, inner):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://elasticache.amazonaws.com/doc/2015-02-02/">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://elasticache.amazonaws.com/doc/2015-02-02/">
    <Error><Code>{code}</Code><Message>{message}</Message></Error>
    <RequestId>{new_uuid()}</RequestId>
</ErrorResponse>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def reset():
    docker_client = _get_docker()
    if docker_client:
        for cluster in _clusters.values():
            cid = cluster.get("_docker_container_id")
            if cid:
                try:
                    c = docker_client.containers.get(cid)
                    c.stop(timeout=2)
                    c.remove(v=True)
                except Exception as e:
                    logger.warning("reset: failed to stop/remove container %s: %s", cid, e)
    _clusters.clear()
    _replication_groups.clear()
    _subnet_groups.clear()
    _param_groups.clear()
    _param_group_params.clear()
    _snapshots.clear()
    _users.clear()
    _user_groups.clear()
    _events.clear()
    _tags.clear()   # was missing from reset() — HIGH-severity gap from audit
    _port_counter[0] = BASE_PORT
