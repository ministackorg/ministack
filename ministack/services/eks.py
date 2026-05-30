"""
EKS Service Emulator.
REST/JSON protocol — /clusters/* and /clusters/*/node-groups/* paths.

CreateCluster spawns a k3s Docker container providing a real Kubernetes
API server. DeleteCluster stops and removes it.

Supports:
  Clusters:   CreateCluster, DescribeCluster, ListClusters, DeleteCluster
  Nodegroups: CreateNodegroup, DescribeNodegroup, ListNodegroups, DeleteNodegroup
  Tags:       TagResource, UntagResource, ListTagsForResource
"""

import base64
import copy
import importlib
import json
import logging
import os
import re
import threading
import time

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    apply_image_prefix,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("eks")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
EKS_K3S_IMAGE = os.environ.get("EKS_K3S_IMAGE", "rancher/k3s:v1.31.4-k3s1")
EKS_BASE_PORT = int(os.environ.get("EKS_BASE_PORT", "16443"))
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")

try:
    docker_lib = importlib.import_module("docker")
    _docker_available = True
except ImportError:
    docker_lib = None
    _docker_available = False

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_clusters = AccountScopedDict()       # name -> cluster record
_nodegroups = AccountScopedDict()     # "cluster/nodegroup" -> nodegroup record
_addons = AccountScopedDict()         # "cluster/addonName" -> addon record
_tags = AccountScopedDict()           # arn -> {key: value}
_port_counter_lock = threading.Lock()
_port_counter = [EKS_BASE_PORT]


def reset():
    _clusters.clear()
    _nodegroups.clear()
    _addons.clear()
    _tags.clear()
    _port_counter[0] = EKS_BASE_PORT
    _stop_all_k3s()


def get_state():
    clusters = copy.deepcopy(_clusters)
    # Strip Docker container IDs (not restorable across restarts)
    if isinstance(clusters, AccountScopedDict):
        for key in list(clusters._data):
            clusters._data[key].pop("_docker_id", None)
    else:
        for c in clusters.values():
            c.pop("_docker_id", None)
    return {
        "clusters": clusters,
        "nodegroups": copy.deepcopy(_nodegroups),
        "addons": copy.deepcopy(_addons),
        "tags": copy.deepcopy(_tags),
        "port_counter": _port_counter[0],
    }


def restore_state(data):
    _clusters.update(data.get("clusters", {}))
    _nodegroups.update(data.get("nodegroups", {}))
    _addons.update(data.get("addons", {}))
    _tags.update(data.get("tags", {}))
    if "port_counter" in data:
        _port_counter[0] = data["port_counter"]
    # Restored clusters have no running k3s container — keep ACTIVE with mock endpoint
    if isinstance(_clusters, AccountScopedDict):
        for key in list(_clusters._data):
            c = _clusters._data[key]
            c["_docker_id"] = None
    else:
        for c in _clusters.values():
            c["_docker_id"] = None


try:
    _restored = load_state("eks")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted eks state; continuing fresh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _next_port():
    with _port_counter_lock:
        port = _port_counter[0]
        _port_counter[0] += 1
        return port


def _cluster_arn(name):
    return f"arn:aws:eks:{get_region()}:{get_account_id()}:cluster/{name}"


def _nodegroup_arn(cluster_name, ng_name):
    return f"arn:aws:eks:{get_region()}:{get_account_id()}:nodegroup/{cluster_name}/{ng_name}/{new_uuid()[:8]}"


def _addon_arn(cluster_name, addon_name):
    # AWS uses arn:aws:eks:{region}:{account}:addon/{cluster}/{addonName}/{uuid}.
    return f"arn:aws:eks:{get_region()}:{get_account_id()}:addon/{cluster_name}/{addon_name}/{new_uuid()[:8]}"


def _now():
    return int(time.time())


def _json_resp(status, body):
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _error(status, code, message):
    return status, {"Content-Type": "application/json", "x-amzn-errortype": code}, json.dumps({"__type": code, "message": message}).encode()


def _get_docker():
    if not _docker_available:
        return None
    try:
        return docker_lib.from_env()
    except Exception:
        return None


def _get_ministack_network(client):
    """Detect the Docker network MiniStack is running on."""
    if DOCKER_NETWORK:
        return DOCKER_NETWORK
    try:
        hostname = os.environ.get("HOSTNAME", "")
        if not hostname:
            return None
        self_container = client.containers.get(hostname)
        nets = list(self_container.attrs["NetworkSettings"]["Networks"].keys())
        return nets[0] if nets else None
    except Exception:
        return None


def _wait_for_port(host, port, timeout=30):
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except (OSError, ConnectionRefusedError):
            time.sleep(0.5)
    return False


def _k3s_run_kwargs(name: str, port: int, ms_network: str | None = None) -> dict:
    """Build the docker run kwargs for a k3s server container.

    `privileged=True` is required: k3s server mode remounts `/sys/fs/cgroup`,
    which the granular `cap_add` list below cannot grant. Without it k3s
    fails on boot with "failed to evacuate root cgroup: mkdir
    /sys/fs/cgroup/init: read-only file system" (issue #611). The cap_add
    list and unconfined security_opt are kept as defence-in-depth so that
    hardened Docker setups still get the right capability set.
    """
    run_kwargs = dict(
        image=apply_image_prefix(EKS_K3S_IMAGE),
        command=["server",
                 "--disable=traefik,metrics-server,servicelb",
                 "--tls-san=0.0.0.0",
                 "--https-listen-port=6443"],
        detach=True,
        privileged=True,
        cap_add=[
            "SYS_ADMIN", "NET_ADMIN", "NET_RAW", "NET_BIND_SERVICE",
            "SYS_PTRACE", "SYS_RESOURCE", "SYS_CHROOT",
            "DAC_OVERRIDE", "DAC_READ_SEARCH",
            "FOWNER", "FSETID", "CHOWN", "MKNOD",
            "KILL", "SETGID", "SETUID", "SETPCAP", "SETFCAP",
            "AUDIT_WRITE",
        ],
        security_opt=["seccomp=unconfined", "apparmor=unconfined"],
        devices=["/dev/fuse"],
        ports={"6443/tcp": port},
        name=f"ministack-eks-{name}",
        labels={"ministack": "eks", "cluster_name": name},
        environment={"K3S_KUBECONFIG_MODE": "644"},
        volumes={"/lib/modules": {"bind": "/lib/modules", "mode": "ro"}},
        tmpfs={"/run": "", "/var/run": "", "/tmp": ""},
    )
    if ms_network:
        run_kwargs["network"] = ms_network
    return run_kwargs


def _stop_all_k3s():
    """Stop all k3s containers managed by MiniStack."""
    client = _get_docker()
    if not client:
        return
    try:
        for c in client.containers.list(filters={"label": "ministack=eks"}):
            try:
                c.stop(timeout=5)
                c.remove(v=True, force=True)
            except Exception:
                pass
    except Exception:
        pass


def _extract_ca_cert(container, timeout=30):
    """Extract the CA certificate from a running k3s container."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            _, output = container.exec_run("cat /var/lib/rancher/k3s/server/tls/server-ca.crt")
            cert = output.decode("utf-8", errors="replace").strip()
            if cert.startswith("-----BEGIN CERTIFICATE-----"):
                return base64.b64encode(cert.encode()).decode()
        except Exception:
            pass
        time.sleep(1)
    return ""


# ---------------------------------------------------------------------------
# Clusters
# ---------------------------------------------------------------------------

def _create_cluster(body):
    name = body.get("name", "")
    if not name:
        return _error(400, "InvalidParameterException", "Cluster name is required.")
    if name in _clusters:
        return _error(409, "ResourceInUseException", f"Cluster already exists with name: {name}")

    arn = _cluster_arn(name)
    now = _now()
    version = body.get("version", "1.30")
    role_arn = body.get("roleArn", f"arn:aws:iam::{get_account_id()}:role/eks-role")
    vpc_config = body.get("resourcesVpcConfig", {})

    # Spawn k3s container
    endpoint = ""
    ca_data = ""
    container_id = None
    port = _next_port()

    # Build cluster record immediately (status CREATING) and return fast.
    # k3s startup happens in background thread to avoid blocking the event loop.
    endpoint = f"https://localhost:{port}"
    cluster = {
        "name": name,
        "arn": arn,
        "createdAt": now,
        "version": version,
        "endpoint": endpoint,
        "roleArn": role_arn,
        "resourcesVpcConfig": {
            "subnetIds": vpc_config.get("subnetIds", []),
            "securityGroupIds": vpc_config.get("securityGroupIds", []),
            "clusterSecurityGroupId": f"sg-{new_uuid()[:17].replace('-', '')}",
            "vpcId": vpc_config.get("vpcId", "vpc-00000000"),
            "endpointPublicAccess": vpc_config.get("endpointPublicAccess", True),
            "endpointPrivateAccess": vpc_config.get("endpointPrivateAccess", False),
            "publicAccessCidrs": vpc_config.get("publicAccessCidrs", ["0.0.0.0/0"]),
        },
        "kubernetesNetworkConfig": {
            "serviceIpv4Cidr": body.get("kubernetesNetworkConfig", {}).get("serviceIpv4Cidr", "10.100.0.0/16"),
            "ipFamily": "ipv4",
        },
        "logging": body.get("logging", {"clusterLogging": []}),
        "identity": {
            "oidc": {"issuer": f"https://oidc.eks.{get_region()}.amazonaws.com/id/{new_uuid()[:32].replace('-', '').upper()}"}
        },
        "status": "CREATING",
        "certificateAuthority": {"data": ""},
        "platformVersion": f"eks.{int(time.time()) % 100}",
        "tags": body.get("tags", {}),
        "encryptionConfig": body.get("encryptionConfig", []),
        "accessConfig": body.get("accessConfig", {}),
        "_docker_id": None,
        "_port": port,
    }

    _clusters[name] = cluster
    if cluster["tags"]:
        _tags[arn] = dict(cluster["tags"])

    def _bg_start():
        client = _get_docker()
        if not client:
            cluster["status"] = "ACTIVE"
            logger.info("EKS: Docker unavailable — cluster %s created without k3s backend", name)
            return
        try:
            ms_network = _get_ministack_network(client)
            run_kwargs = _k3s_run_kwargs(name=name, port=port, ms_network=ms_network)

            container = client.containers.run(**run_kwargs)
            cluster["_docker_id"] = container.id

            ep = ""
            if ms_network:
                container.reload()
                networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
                container_ip = networks.get(ms_network, {}).get("IPAddress", "")
                if container_ip and _wait_for_port(container_ip, 6443):
                    ep = f"https://{container_ip}:6443"
                    logger.info("EKS: k3s for %s ready at %s (network %s)", name, ep, ms_network)
            if not ep:
                if _wait_for_port("127.0.0.1", port):
                    ep = f"https://localhost:{port}"
                    logger.info("EKS: k3s for %s ready at %s", name, ep)
                else:
                    logger.warning("EKS: k3s for %s did not become ready on port %d", name, port)
                    ep = f"https://localhost:{port}"

            cluster["endpoint"] = ep
            cluster["certificateAuthority"]["data"] = _extract_ca_cert(container)
            cluster["status"] = "ACTIVE"
        except Exception as e:
            logger.warning("EKS: failed to start k3s for %s — falling back to mock: %s", name, e)
            cluster["status"] = "ACTIVE"
            cluster["certificateAuthority"]["data"] = base64.b64encode(b"MOCK-CA-CERTIFICATE").decode()
            cluster["endpoint"] = f"https://localhost:{port}"

    threading.Thread(target=_bg_start, daemon=True, name=f"eks-{name}").start()
    return _json_resp(200, {"cluster": _sanitize(cluster)})


def _describe_cluster(name):
    cluster = _clusters.get(name)
    if not cluster:
        return _error(404, "ResourceNotFoundException", f"No cluster found for name: {name}.")
    return _json_resp(200, {"cluster": _sanitize(cluster)})


def _list_clusters(query):
    max_results = int(query.get("maxResults", 100))
    names = list(_clusters.keys())[:max_results]
    return _json_resp(200, {"clusters": names})


def _delete_cluster(name):
    cluster = _clusters.get(name)
    if not cluster:
        return _error(404, "ResourceNotFoundException", f"No cluster found for name: {name}.")

    # Stop k3s container
    container_id = cluster.get("_docker_id")
    if container_id:
        client = _get_docker()
        if client:
            try:
                c = client.containers.get(container_id)
                c.stop(timeout=5)
                c.remove(v=True, force=True)
                logger.info("EKS: stopped k3s container for %s", name)
            except Exception as e:
                logger.warning("EKS: failed to stop k3s for %s: %s", name, e)

    # Delete all nodegroups in this cluster
    ng_keys = [k for k in _nodegroups if k.startswith(f"{name}/")]
    for k in ng_keys:
        ng = _nodegroups.pop(k, None)
        if ng:
            _tags.pop(ng.get("nodegroupArn", ""), None)

    arn = cluster["arn"]
    cluster["status"] = "DELETING"
    result = _sanitize(cluster)
    _clusters.pop(name, None)
    _tags.pop(arn, None)

    return _json_resp(200, {"cluster": result})


# ---------------------------------------------------------------------------
# Nodegroups
# ---------------------------------------------------------------------------

def _create_nodegroup(cluster_name, body):
    if cluster_name not in _clusters:
        return _error(404, "ResourceNotFoundException", f"No cluster found for name: {cluster_name}.")

    ng_name = body.get("nodegroupName", "")
    if not ng_name:
        return _error(400, "InvalidParameterException", "Nodegroup name is required.")

    key = f"{cluster_name}/{ng_name}"
    if key in _nodegroups:
        return _error(409, "ResourceInUseException", f"Nodegroup already exists with name: {ng_name}")

    arn = _nodegroup_arn(cluster_name, ng_name)
    now = _now()
    scaling = body.get("scalingConfig", {"minSize": 1, "maxSize": 2, "desiredSize": 1})

    nodegroup = {
        "nodegroupName": ng_name,
        "nodegroupArn": arn,
        "clusterName": cluster_name,
        "version": body.get("version", _clusters[cluster_name].get("version", "1.30")),
        "releaseVersion": body.get("releaseVersion", ""),
        "createdAt": now,
        "modifiedAt": now,
        "status": "ACTIVE",
        "capacityType": body.get("capacityType", "ON_DEMAND"),
        "scalingConfig": scaling,
        "instanceTypes": body.get("instanceTypes", ["t3.medium"]),
        "subnets": body.get("subnets", []),
        "amiType": body.get("amiType", "AL2_x86_64"),
        "nodeRole": body.get("nodeRole", f"arn:aws:iam::{get_account_id()}:role/eks-node-role"),
        "labels": body.get("labels", {}),
        "taints": body.get("taints", []),
        "diskSize": body.get("diskSize", 20),
        "health": {"issues": []},
        "resources": {
            "autoScalingGroups": [{"name": f"eks-{ng_name}-{new_uuid()[:8]}"}],
            "remoteAccessSecurityGroup": f"sg-{new_uuid()[:17].replace('-', '')}",
        },
        "tags": body.get("tags", {}),
    }

    _nodegroups[key] = nodegroup
    if nodegroup["tags"]:
        _tags[arn] = dict(nodegroup["tags"])

    return _json_resp(200, {"nodegroup": nodegroup})


def _describe_nodegroup(cluster_name, ng_name):
    key = f"{cluster_name}/{ng_name}"
    ng = _nodegroups.get(key)
    if not ng:
        return _error(404, "ResourceNotFoundException",
                      f"No node group found for name: {ng_name}.")
    return _json_resp(200, {"nodegroup": ng})


def _list_nodegroups(cluster_name, query):
    if cluster_name not in _clusters:
        return _error(404, "ResourceNotFoundException", f"No cluster found for name: {cluster_name}.")
    max_results = int(query.get("maxResults", 100))
    names = [ng["nodegroupName"] for k, ng in _nodegroups.items()
             if k.startswith(f"{cluster_name}/")][:max_results]
    return _json_resp(200, {"nodegroups": names})


def _delete_nodegroup(cluster_name, ng_name):
    key = f"{cluster_name}/{ng_name}"
    ng = _nodegroups.get(key)
    if not ng:
        return _error(404, "ResourceNotFoundException",
                      f"No node group found for name: {ng_name}.")
    ng["status"] = "DELETING"
    result = dict(ng)
    _nodegroups.pop(key, None)
    _tags.pop(ng.get("nodegroupArn", ""), None)
    return _json_resp(200, {"nodegroup": result})


# ---------------------------------------------------------------------------
# Addons
# ---------------------------------------------------------------------------
# CreateAddon / DescribeAddon / DeleteAddon / ListAddons / UpdateAddon.
# Status flips ACTIVE on Create / Update (same shortcut as nodegroups —
# Terraform polls until ACTIVE so a slow-roll status would only stall tests).
# Delete returns the record with status=DELETING and drops the entry.

def _create_addon(cluster_name, body):
    if cluster_name not in _clusters:
        return _error(404, "ResourceNotFoundException",
                      f"No cluster found for name: {cluster_name}.")
    addon_name = body.get("addonName", "")
    if not addon_name:
        return _error(400, "InvalidParameterException", "Addon name is required.")

    key = f"{cluster_name}/{addon_name}"
    if key in _addons:
        return _error(409, "ResourceInUseException",
                      f"Addon already exists with name: {addon_name}")

    arn = _addon_arn(cluster_name, addon_name)
    now = _now()
    addon = {
        "addonName": addon_name,
        "clusterName": cluster_name,
        "status": "ACTIVE",
        "addonVersion": body.get("addonVersion", ""),
        "addonArn": arn,
        "createdAt": now,
        "modifiedAt": now,
        "serviceAccountRoleArn": body.get("serviceAccountRoleArn", ""),
        "tags": body.get("tags", {}),
        "configurationValues": body.get("configurationValues", ""),
        "podIdentityAssociations": body.get("podIdentityAssociations", []),
        "health": {"issues": []},
        "owner": "aws",
        "publisher": "eks",
    }
    _addons[key] = addon
    if addon["tags"]:
        _tags[arn] = dict(addon["tags"])
    return _json_resp(200, {"addon": addon})


def _describe_addon(cluster_name, addon_name):
    addon = _addons.get(f"{cluster_name}/{addon_name}")
    if not addon:
        return _error(404, "ResourceNotFoundException",
                      f"No addon found for cluster {cluster_name} addon {addon_name}")
    return _json_resp(200, {"addon": addon})


def _list_addons(cluster_name, query):
    if cluster_name not in _clusters:
        return _error(404, "ResourceNotFoundException",
                      f"No cluster found for name: {cluster_name}.")
    max_results = int(query.get("maxResults", 100))
    names = [a["addonName"] for k, a in _addons.items()
             if k.startswith(f"{cluster_name}/")][:max_results]
    return _json_resp(200, {"addons": names})


def _delete_addon(cluster_name, addon_name):
    key = f"{cluster_name}/{addon_name}"
    addon = _addons.get(key)
    if not addon:
        return _error(404, "ResourceNotFoundException",
                      f"No addon found for cluster {cluster_name} addon {addon_name}")
    addon["status"] = "DELETING"
    result = dict(addon)
    _addons.pop(key, None)
    _tags.pop(addon.get("addonArn", ""), None)
    return _json_resp(200, {"addon": result})


def _update_addon(cluster_name, addon_name, body):
    key = f"{cluster_name}/{addon_name}"
    addon = _addons.get(key)
    if not addon:
        return _error(404, "ResourceNotFoundException",
                      f"No addon found for cluster {cluster_name} addon {addon_name}")
    for field in ("addonVersion", "serviceAccountRoleArn",
                  "configurationValues", "podIdentityAssociations"):
        if field in body:
            addon[field] = body[field]
    addon["modifiedAt"] = _now()
    addon["status"] = "ACTIVE"
    update = {
        "id": new_uuid(),
        "status": "Successful",
        "type": "AddonUpdate",
        "createdAt": _now(),
    }
    return _json_resp(200, {"update": update})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(arn, body):
    tags = body.get("tags", {})
    existing = _tags.get(arn, {})
    existing.update(tags)
    _tags[arn] = existing
    return _json_resp(200, {})


def _untag_resource(arn, query):
    keys = query.get("tagKeys", [])
    if isinstance(keys, str):
        keys = [keys]
    existing = _tags.get(arn, {})
    for k in keys:
        existing.pop(k, None)
    if existing:
        _tags[arn] = existing
    else:
        _tags.pop(arn, None)
    return _json_resp(200, {})


def _list_tags(arn):
    return _json_resp(200, {"tags": _tags.get(arn, {})})


# ---------------------------------------------------------------------------
# Sanitize (remove internal fields)
# ---------------------------------------------------------------------------

def _sanitize(cluster):
    return {k: v for k, v in cluster.items() if not k.startswith("_")}


# ---------------------------------------------------------------------------
# Request Router
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body_bytes, query_params):
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {}

    query = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}

    # POST /clusters
    if path == "/clusters" and method == "POST":
        return _create_cluster(body)

    # GET /clusters
    if path == "/clusters" and method == "GET":
        return _list_clusters(query)

    # /clusters/{name}
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)", path)
    if m:
        name = m.group(1)
        if method == "GET":
            return _describe_cluster(name)
        if method == "DELETE":
            return _delete_cluster(name)

    # POST /clusters/{name}/node-groups
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/node-groups", path)
    if m:
        cluster_name = m.group(1)
        if method == "POST":
            return _create_nodegroup(cluster_name, body)
        if method == "GET":
            return _list_nodegroups(cluster_name, query)

    # /clusters/{name}/node-groups/{ngName}
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/node-groups/([A-Za-z0-9_-]+)", path)
    if m:
        cluster_name, ng_name = m.group(1), m.group(2)
        if method == "GET":
            return _describe_nodegroup(cluster_name, ng_name)
        if method == "DELETE":
            return _delete_nodegroup(cluster_name, ng_name)

    # POST/GET /clusters/{name}/addons
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/addons", path)
    if m:
        cluster_name = m.group(1)
        if method == "POST":
            return _create_addon(cluster_name, body)
        if method == "GET":
            return _list_addons(cluster_name, query)

    # POST /clusters/{name}/addons/{addonName}/update — UpdateAddon.
    # Must come BEFORE the generic /addons/{addonName} pattern so the
    # `/update` suffix isn't swallowed by the wider regex.
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/addons/([A-Za-z0-9_.-]+)/update", path)
    if m:
        cluster_name, addon_name = m.group(1), m.group(2)
        if method == "POST":
            return _update_addon(cluster_name, addon_name, body)

    # GET/DELETE /clusters/{name}/addons/{addonName}
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/addons/([A-Za-z0-9_.-]+)", path)
    if m:
        cluster_name, addon_name = m.group(1), m.group(2)
        if method == "GET":
            return _describe_addon(cluster_name, addon_name)
        if method == "DELETE":
            return _delete_addon(cluster_name, addon_name)

    # Tags: /tags/{arn+}
    if path.startswith("/tags/"):
        arn = path[6:]
        if method == "GET":
            return _list_tags(arn)
        if method == "POST":
            return _tag_resource(arn, body)
        if method == "DELETE":
            return _untag_resource(arn, query)

    return _error(400, "InvalidRequestException", f"No route for {method} {path}")
