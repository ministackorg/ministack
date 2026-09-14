# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
EKS Service Emulator.
REST/JSON protocol — /clusters/* and /clusters/*/node-groups/* paths.

CreateCluster spawns a k3s Docker container providing a real Kubernetes
API server. DeleteCluster stops and removes it.

Supports:
  Clusters:   CreateCluster, DescribeCluster, ListClusters, DeleteCluster
  Nodegroups: CreateNodegroup, DescribeNodegroup, ListNodegroups, DeleteNodegroup
  IdP configs: AssociateIdentityProviderConfig, DescribeIdentityProviderConfig,
              DisassociateIdentityProviderConfig, ListIdentityProviderConfigs
  Authentication: AWS IAM exec tokens through a k3s TokenReview webhook
  Tags:       TagResource, UntagResource, ListTagsForResource
"""

import base64
import copy
import datetime as dt
import fnmatch
import hashlib
import importlib
import json
import logging
import os
import re
import threading
import time
import urllib.parse

from ministack.core import container_reaper
from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import run_reentrant
from ministack.core.iam_evaluator import (
    AmbiguousAccessKeyError,
    CredentialResolutionError,
    find_iam_access_key_account,
    resolve_caller_identity,
    resolve_credential,
)
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    _account_from_sts_session,
    apply_image_prefix,
    get_account_id,
    get_region,
    new_uuid,
)
from ministack.core.router import extract_access_key_id
from ministack.core.sigv4 import (
    build_canonical_request,
    build_string_to_sign,
    calculate_signature,
    signatures_match,
)

logger = logging.getLogger("eks")

# Cap any single Docker daemon call. docker-py defaults to 60s, which turns a
# slow or wedged daemon into a minutes-long stall on a request path.
_DOCKER_TIMEOUT = float(os.environ.get("MINISTACK_DOCKER_TIMEOUT", "10"))


REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")
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

_clusters = AccountRegionScopedDict()       # name -> cluster record
_nodegroups = AccountRegionScopedDict()     # "cluster/nodegroup" -> nodegroup record
_addons = AccountRegionScopedDict()         # "cluster/addonName" -> addon record
_access_entries = AccountRegionScopedDict() # "cluster\x00principalArn" -> access entry record
_access_policies = AccountRegionScopedDict()# "cluster\x00principalArn\x00policyArn" -> associated policy
_tags = AccountScopedDict()           # arn -> {key: value}
_idp_configs = AccountRegionScopedDict()     # "cluster\x00idp_name" -> idp record
_port_counter_lock = threading.Lock()
_port_counter = [EKS_BASE_PORT]
_oidc_keypair_lock = threading.Lock()
_oidc_keypair = None                  # (private_key, jwk_dict, kid)
_rbac_reconcilers_lock = threading.Lock()
_rbac_reconcilers = {}                 # (account, region, cluster) -> (stop, wake)
_rbac_reconcile_locks = {}             # (account, region, cluster) -> Lock
_EKS_RBAC_RECONCILE_INTERVAL = float(
    os.environ.get("EKS_RBAC_RECONCILE_INTERVAL", "2")
)


def _cluster_endpoint(port):
    """The endpoint DescribeCluster advertises for the kube-apiserver — the
    host-published port ``https://{MINISTACK_HOST}:{port}``. The k3s container
    publishes 6443 to this host port (``ports={"6443/tcp": port}``), so it is
    reachable from the host (``aws eks update-kubeconfig`` + kubectl) and from
    containers that can route to ``MINISTACK_HOST``. The same value is used on
    every path (create, restart, restore).
    """
    return f"https://{_MINISTACK_HOST}:{port}"


def _ministack_issuer_base():
    """Base URL ministack advertises as the cluster's OIDC issuer.

    The scheme tracks the gateway's actual protocol: the discovery/JWKS
    documents are served by ministack's own gateway, so the issuer must say
    https only when the gateway is serving TLS (``USE_SSL=1``) and http
    otherwise. Advertising https on a plain-http gateway would make any client
    that fetches the discovery document fail. Real EKS issuers are always
    https, so run with ``USE_SSL=1`` for IRSA terraform, whose
    ``aws_iam_openid_connect_provider`` client-side rejects non-https urls.
    """
    from ministack.core import tls as _tls
    scheme = "https" if _tls.use_ssl_enabled() else "http"
    port = os.environ.get("GATEWAY_PORT", "4566")
    return f"{scheme}://{_MINISTACK_HOST}:{port}/oidc"


def _new_oidc_id():
    return new_uuid()[:32].replace("-", "").upper()


def _issuer_url(oidc_id):
    return f"{_ministack_issuer_base()}/id/{oidc_id}"


def _get_oidc_keypair():
    """Lazily generate a single RSA keypair for OIDC discovery / JWKS.

    Shared across all clusters — ministack does not issue real IRSA tokens, so
    a single advertised key is sufficient for Terraform's
    aws_iam_openid_connect_provider to fetch + thumbprint the issuer.
    """
    global _oidc_keypair
    if _oidc_keypair is not None:
        return _oidc_keypair
    with _oidc_keypair_lock:
        if _oidc_keypair is not None:
            return _oidc_keypair
        from cryptography.hazmat.primitives.asymmetric import rsa
        priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        nums = priv.public_key().public_numbers()
        n_bytes = nums.n.to_bytes((nums.n.bit_length() + 7) // 8, "big")
        e_bytes = nums.e.to_bytes((nums.e.bit_length() + 7) // 8, "big")
        kid = new_uuid()[:16]
        jwk = {
            "kty": "RSA",
            "alg": "RS256",
            "use": "sig",
            "kid": kid,
            "n": base64.urlsafe_b64encode(n_bytes).rstrip(b"=").decode(),
            "e": base64.urlsafe_b64encode(e_bytes).rstrip(b"=").decode(),
        }
        _oidc_keypair = (priv, jwk, kid)
        return _oidc_keypair


def reset():
    _stop_all_rbac_reconcilers()
    _clusters.clear()
    _nodegroups.clear()
    _addons.clear()
    _access_entries.clear()
    _access_policies.clear()
    _tags.clear()
    _idp_configs.clear()
    _port_counter[0] = EKS_BASE_PORT
    _stop_all_k3s()


def get_state():
    clusters = copy.deepcopy(_clusters)
    # Strip Docker container IDs (not restorable across restarts)
    if isinstance(clusters, AccountRegionScopedDict):
        for key in list(clusters._data):
            clusters._data[key].pop("_docker_id", None)
    else:
        for c in clusters.values():
            c.pop("_docker_id", None)
    return {
        "clusters": clusters,
        "nodegroups": copy.deepcopy(_nodegroups),
        "addons": copy.deepcopy(_addons),
        "access_entries": copy.deepcopy(_access_entries),
        "access_policies": copy.deepcopy(_access_policies),
        "tags": copy.deepcopy(_tags),
        "idp_configs": copy.deepcopy(_idp_configs),
        "port_counter": _port_counter[0],
    }


def _restore_cluster_child_store(store, restored, cluster_regions, separator):
    """Adopt legacy child records into their parent cluster's region.

    Legacy account-scoped EKS state allowed a request in one region to create
    a child for a cluster created in another. Preserve that reachable parent /
    child relationship instead of splitting the records by their individual
    ARNs. Orphans retain the generic ARN-derived (or boot-region) fallback.
    """
    if isinstance(restored, AccountRegionScopedDict):
        store.update(restored)
        return

    if not restored:
        return

    if isinstance(restored, AccountScopedDict):
        items = restored._data.items()
    else:
        account_id = get_account_id()
        items = (((account_id, key), value) for key, value in restored.items())

    for (account_id, key), value in items:
        cluster_name = key.split(separator, 1)[0] if isinstance(key, str) else None
        region = cluster_regions.get((account_id, cluster_name))
        if region is None:
            region = store._region_for_legacy_value(key, value)
        store.set_scoped(account_id, region, key, value)


def load_persisted_state(data):
    return restore_state(data)


def restore_state(data):
    _clusters.update(data.get("clusters", {}))
    cluster_regions = {
        (account_id, cluster_name): region
        for (account_id, region, cluster_name), _cluster in _clusters.all_items()
    }
    for store, state_key, separator in (
        (_nodegroups, "nodegroups", "/"),
        (_addons, "addons", "/"),
        (_access_entries, "access_entries", "\x00"),
        (_access_policies, "access_policies", "\x00"),
        (_idp_configs, "idp_configs", "\x00"),
    ):
        _restore_cluster_child_store(
            store,
            data.get(state_key, {}),
            cluster_regions,
            separator,
        )
    _tags.update(data.get("tags", {}))
    if "port_counter" in data:
        _port_counter[0] = data["port_counter"]
    # Restored clusters have no running k3s container. Drop the stale docker id and
    # normalize the endpoint to the stable host form (https://{MINISTACK_HOST}:{port},
    # default localhost). The cluster is still reported ACTIVE, so the endpoint must
    # stay non-empty: an ACTIVE cluster with an empty endpoint is a contradictory
    # shape that breaks `aws eks update-kubeconfig` and Terraform drift detection.
    # Any container IP persisted from the previous run is now dead, so the configured
    # ministack host is the only address worth reporting after a restart.
    # to_dict() yields every account's live record dict so the rewrite is applied
    # across all tenants — the account-scoped views (values()/items()) would see
    # only the default account because no request scope is set at import time.
    clusters = (
        _clusters.to_dict().values()
        if isinstance(_clusters, AccountRegionScopedDict)
        else _clusters.values()
    )
    for c in clusters:
        c["_docker_id"] = None
        port = c.get("_port")
        if port:
            c["endpoint"] = _cluster_endpoint(port)



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


def _access_entry_arn(cluster_name, principal_arn):
    # AWS: arn:aws:eks:{region}:{account}:access-entry/{cluster}/{principalArnId}/{uuid}.
    return (
        f"arn:aws:eks:{get_region()}:{get_account_id()}:"
        f"access-entry/{cluster_name}/{new_uuid()[:8]}"
    )


def _ae_key(cluster_name: str, principal_arn: str) -> str:
    return f"{cluster_name}\x00{principal_arn}"


def _ap_key(cluster_name: str, principal_arn: str, policy_arn: str) -> str:
    return f"{cluster_name}\x00{principal_arn}\x00{policy_arn}"


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
        return docker_lib.from_env(timeout=_DOCKER_TIMEOUT)
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


def _collect_oidc_state(cluster_name: str):
    """Return (apiserver_args, cfg_refs) for OIDC configs on a cluster.

    MUST be called from a request context (uses contextvars via AccountScopedDict).
    Returned cfg_refs are live dict references — background threads can mutate
    their "status" field without re-entering the request scope.
    """
    args: list[str] = []
    cfg_refs: list[dict] = []
    for key, cfg in list(_idp_configs.items()):
        cn, _, _ = key.partition("\x00")
        if cn != cluster_name:
            continue
        cfg_refs.append(cfg)
        oidc = cfg.get("oidc", {})
        if not oidc:
            continue
        args.append(f"--kube-apiserver-arg=oidc-issuer-url={oidc.get('issuerUrl')}")
        args.append(f"--kube-apiserver-arg=oidc-client-id={oidc.get('clientId')}")
        if oidc.get("usernameClaim"):
            args.append(f"--kube-apiserver-arg=oidc-username-claim={oidc.get('usernameClaim')}")
        if oidc.get("groupsClaim"):
            args.append(f"--kube-apiserver-arg=oidc-groups-claim={oidc.get('groupsClaim')}")
    return args, cfg_refs


def _collect_node_labels(cluster: dict) -> list[str]:
    """Return the AWS-default topology `--node-label` k3s args for a cluster.

    Real EKS nodes carry `topology.kubernetes.io/region` and
    `topology.kubernetes.io/zone` (set by the AWS cloud-controller-manager) so
    Karpenter / `topologySpreadConstraints` / Cluster Autoscaler can schedule.
    Per-node-group label overrides belong on `CreateNodegroup.labels`, which is
    the AWS-shape-correct surface — not a ministack-specific tag convention.

    MUST be called from a request context (uses `get_region()`).
    """
    region = get_region()
    labels = {
        "topology.kubernetes.io/zone": f"{region}a",
        "topology.kubernetes.io/region": region,
    }
    return [f"--node-label={k}={v}" for k, v in labels.items()]


def _k3s_run_kwargs(
    name: str,
    region: str,
    port: int,
    ms_network: str | None = None,
    oidc_args: list[str] | None = None,
    node_labels: list[str] | None = None,
) -> dict:
    """Build the docker run kwargs for a k3s server container.

    `privileged=True` is required: k3s server mode remounts `/sys/fs/cgroup`,
    which the granular `cap_add` list below cannot grant. Without it k3s
    fails on boot with "failed to evacuate root cgroup: mkdir
    /sys/fs/cgroup/init: read-only file system" (issue #611). The cap_add
    list and unconfined security_opt are kept as defence-in-depth so that
    hardened Docker setups still get the right capability set.
    """
    command = [
        "server",
        "--disable=traefik,metrics-server,servicelb",
        "--tls-san=0.0.0.0",
        "--https-listen-port=6443",
        "--kube-apiserver-arg=authentication-token-webhook-config-file=/etc/rancher/k3s/eks-auth-webhook.yaml",
        "--kube-apiserver-arg=authentication-token-webhook-cache-ttl=5m",
    ]
    if oidc_args:
        command.extend(oidc_args)
    if node_labels:
        command.extend(node_labels)

    run_kwargs = dict(
        image=apply_image_prefix(EKS_K3S_IMAGE),
        command=command,
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
        name=f"ministack-eks-{region}-{name}",
        labels=container_reaper.own_labels("eks", cluster_name=name, region=region),
        environment={"K3S_KUBECONFIG_MODE": "644"},
        volumes={"/lib/modules": {"bind": "/lib/modules", "mode": "ro"}},
        tmpfs={"/run": "", "/var/run": "", "/tmp": ""},
    )
    if ms_network:
        run_kwargs["network"] = ms_network
    # host-gateway lets the k3s node reach a host-run MiniStack (the ECR
    # registry mirror, #1054) even on Linux Docker where the name doesn't
    # resolve natively. Harmless when a shared network is used instead.
    run_kwargs["extra_hosts"] = {"host.docker.internal": "host-gateway"}

    return run_kwargs


def _k3s_gateway_host(client, ms_network):
    """Return the address a k3s container can use to call MiniStack."""
    if ms_network and client is not None:
        try:
            self_container = client.containers.get(os.environ.get("HOSTNAME", ""))
            nets = self_container.attrs["NetworkSettings"]["Networks"]
            host = (nets.get(ms_network) or {}).get("IPAddress")
            if host:
                return host
        except Exception:
            pass
    return "host.docker.internal"


def _k3s_auth_webhook_config(client, ms_network, cluster_name, account_id=None, region=None):
    """Build the kubeconfig consumed by the k3s token webhook authenticator.

    The webhook endpoint is deliberately addressed through MiniStack's
    container-network address rather than the host-published EKS port. This
    works for both a shared Docker network and a host-run gateway.
    """
    from ministack.core import tls as _tls

    host = _k3s_gateway_host(client, ms_network)
    port = os.environ.get("GATEWAY_PORT") or os.environ.get("EDGE_PORT") or "4566"
    scheme = "https" if _tls.use_ssl_enabled() else "http"
    account_id = account_id or get_account_id()
    region = region or get_region()
    server = f"{scheme}://{host}:{port}/eks-auth/{account_id}/{region}/{cluster_name}"
    return (
        "apiVersion: v1\n"
        "kind: Config\n"
        "clusters:\n"
        "- name: ministack-eks-auth\n"
        "  cluster:\n"
        f"    server: {server}\n"
        "    insecure-skip-tls-verify: true\n"
        "users:\n"
        "- name: ministack-eks-auth\n"
        "  user: {}\n"
        "contexts:\n"
        "- name: ministack-eks-auth\n"
        "  context:\n"
        "    cluster: ministack-eks-auth\n"
        "    user: ministack-eks-auth\n"
        "current-context: ministack-eks-auth\n"
    ).encode()


def _ecr_registry_hosts(cluster: dict) -> list[str]:
    """The local ECR registry hostnames this cluster's nodes should resolve
    to MiniStack — derived from the cluster ARN (not contextvars) so the
    restore/restart paths, which run outside a request context, get the
    same answer as create."""
    try:
        spec = parse_arn(cluster.get("arn", ""))
        return [f"{spec.account_id}.dkr.ecr.{spec.region}.amazonaws.com"]
    except ArnParseError:
        return []


def _k3s_registries_yaml(client, ms_network: str | None, ecr_hosts: list[str]) -> bytes | None:
    """Build /etc/rancher/k3s/registries.yaml mapping this cluster's local
    ECR registry hostnames to the MiniStack gateway, so pods with images
    like ``<account>.dkr.ecr.<region>.amazonaws.com/repo:tag`` pull from
    local ECR (#1054). ECR's Docker Registry V2 endpoints serve anonymously,
    so no auth config is needed.

    The mirror endpoint must be reachable from INSIDE the k3s container:
    - shared user-defined network: MiniStack's own IP on that network
      (``host-gateway`` resolves to docker0, which iptables typically
      blocks from a sibling bridge — same reasoning as the ECS metadata
      server wiring)
    - otherwise: ``host.docker.internal``, resolvable through the
      host-gateway extra_host added in ``_k3s_run_kwargs``
    """
    if not ecr_hosts:
        return None
    port = os.environ.get("GATEWAY_PORT") or os.environ.get("EDGE_PORT") or "4566"
    host = None
    if ms_network and client is not None:
        try:
            self_container = client.containers.get(os.environ.get("HOSTNAME", ""))
            nets = self_container.attrs["NetworkSettings"]["Networks"]
            host = (nets.get(ms_network) or {}).get("IPAddress") or None
        except Exception:
            host = None
    if not host:
        host = "host.docker.internal"
    endpoint = f"http://{host}:{port}"
    lines = ["mirrors:"]
    for reg in ecr_hosts:
        lines.append(f'  "{reg}":')
        lines.append("    endpoint:")
        lines.append(f'      - "{endpoint}"')
    return ("\n".join(lines) + "\n").encode()


def _start_k3s_container(
    client, run_kwargs: dict, registries_yaml: bytes | None,
    auth_webhook_config: bytes | None = None,
):
    """Start the k3s container, injecting registries.yaml before boot.

    k3s reads /etc/rancher/k3s/registries.yaml once at startup, so the file
    must exist before the entrypoint runs: create the container stopped,
    upload the file with put_archive, then start it."""
    if not registries_yaml and not auth_webhook_config:
        return client.containers.run(**run_kwargs)
    import io
    import tarfile

    create_kwargs = dict(run_kwargs)
    create_kwargs.pop("detach", None)  # run()-only kwarg
    try:
        container = client.containers.create(**create_kwargs)
    except Exception:
        # create() does not auto-pull missing images the way run() does.
        client.images.pull(create_kwargs["image"])
        container = client.containers.create(**create_kwargs)
    try:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for filename, content in (
                ("etc/rancher/k3s/registries.yaml", registries_yaml),
                ("etc/rancher/k3s/eks-auth-webhook.yaml", auth_webhook_config),
            ):
                if not content:
                    continue
                info = tarfile.TarInfo(filename)
                info.size = len(content)
                info.mode = 0o644
                tar.addfile(info, io.BytesIO(content))
        container.put_archive("/", buf.getvalue())
    except Exception as e:
        logger.warning("EKS: could not inject k3s startup configuration: %s", e)
    container.start()
    return container


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

def _create_cluster(body, creator_arn=None):
    name = body.get("name", "")
    if not name:
        return _error(400, "InvalidParameterException", "Cluster name is required.")
    if name in _clusters:
        return _error(409, "ResourceInUseException", f"Cluster already exists with name: {name}")

    arn = _cluster_arn(name)
    now = _now()
    version = body.get("version", "1.30")
    role_arn = body.get("roleArn", f"arn:aws:iam::{get_account_id()}:role/eks-role")
    from ministack.core.iam_evaluator import validate_role_arn
    role_err = validate_role_arn(role_arn)
    if role_err:
        return _error(400, "InvalidParameterException", role_err)
    vpc_config = body.get("resourcesVpcConfig", {})

    # Spawn k3s container
    endpoint = ""
    ca_data = ""
    container_id = None
    port = _next_port()

    # Build cluster record immediately (status CREATING) and return fast.
    # k3s startup happens in background thread to avoid blocking the event loop.
    endpoint = _cluster_endpoint(port)
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
            "oidc": {"issuer": _issuer_url(_new_oidc_id())}
        },
        "status": "CREATING",
        "certificateAuthority": {"data": ""},
        "platformVersion": f"eks.{int(time.time()) % 100}",
        "tags": body.get("tags", {}),
        "encryptionConfig": body.get("encryptionConfig", []),
        "accessConfig": body.get("accessConfig", {}),
        "_creator_arn": creator_arn,
        "_docker_id": None,
        "_port": port,
    }

    _clusters[name] = cluster
    if cluster["tags"]:
        _tags[arn] = dict(cluster["tags"])

    account_id, region = get_account_id(), get_region()
    oidc_args, _idp_cfg_refs = _collect_oidc_state(name)
    node_labels = _collect_node_labels(cluster)

    def _bg_start():
        client = _get_docker()
        if not client:
            cluster["status"] = "ACTIVE"
            logger.info("EKS: Docker unavailable — cluster %s created without k3s backend", name)
            return
        ms_network = None
        try:
            ms_network = _get_ministack_network(client)
            run_kwargs = _k3s_run_kwargs(
                name=name,
                region=region,
                port=port,
                ms_network=ms_network,
                oidc_args=oidc_args,
                node_labels=node_labels,
            )

            registries_yaml = _k3s_registries_yaml(client, ms_network, _ecr_registry_hosts(cluster))
            auth_webhook_config = _k3s_auth_webhook_config(
                client, ms_network, name, account_id, region
            )
            container = _start_k3s_container(
                client, run_kwargs, registries_yaml, auth_webhook_config
            )
            cluster["_docker_id"] = container.id

            cluster["endpoint"] = _cluster_endpoint(port)
            cluster["certificateAuthority"]["data"] = _extract_ca_cert(container)
            cluster["status"] = "ACTIVE"
            _schedule_access_policy_reconcile(name, account_id, region)
        except Exception as e:
            logger.warning("EKS: failed to start k3s for %s — falling back to mock: %s", name, e)
            cluster["status"] = "ACTIVE"
            cluster["certificateAuthority"]["data"] = base64.b64encode(b"MOCK-CA-CERTIFICATE").decode()
            # No container came up — advertise the host-published endpoint.
            cluster["endpoint"] = _cluster_endpoint(port)

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

    _stop_access_policy_reconciler(name, get_account_id(), get_region())

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
# Access Entries (modern EKS IAM bindings — replace aws-auth ConfigMap)
# ---------------------------------------------------------------------------

_VALID_ACCESS_ENTRY_TYPES = (
    "STANDARD", "EC2_LINUX", "EC2_WINDOWS", "FARGATE_LINUX",
)


def _build_access_entry(cluster_name, principal_arn, body):
    now = _now()
    return {
        "clusterName": cluster_name,
        "principalArn": principal_arn,
        "kubernetesGroups": body.get("kubernetesGroups", []),
        "accessEntryArn": _access_entry_arn(cluster_name, principal_arn),
        "createdAt": now,
        "modifiedAt": now,
        "tags": body.get("tags", {}),
        "username": body.get("username", ""),
        "type": body.get("type", "STANDARD"),
    }


def _create_access_entry(cluster_name, body):
    if cluster_name not in _clusters:
        return _error(404, "ResourceNotFoundException",
                      f"No cluster found for name: {cluster_name}.")
    principal_arn = body.get("principalArn", "")
    if not principal_arn:
        return _error(400, "InvalidParameterException",
                      "principalArn is required.")
    ae_type = body.get("type", "STANDARD")
    if ae_type not in _VALID_ACCESS_ENTRY_TYPES:
        return _error(400, "InvalidParameterException",
                      f"Invalid type {ae_type}. Must be one of "
                      f"{list(_VALID_ACCESS_ENTRY_TYPES)}.")
    key = _ae_key(cluster_name, principal_arn)
    if key in _access_entries:
        return _error(409, "ResourceInUseException",
                      f"Access entry already exists for principal {principal_arn}.")
    entry = _build_access_entry(cluster_name, principal_arn, body)
    _access_entries[key] = entry
    if entry["tags"]:
        _tags[entry["accessEntryArn"]] = dict(entry["tags"])
    return _json_resp(200, {"accessEntry": entry})


def _describe_access_entry(cluster_name, principal_arn):
    entry = _access_entries.get(_ae_key(cluster_name, principal_arn))
    if not entry:
        return _error(404, "ResourceNotFoundException",
                      f"No access entry for principal {principal_arn}.")
    return _json_resp(200, {"accessEntry": entry})


def _list_access_entries(cluster_name, query):
    if cluster_name not in _clusters:
        return _error(404, "ResourceNotFoundException",
                      f"No cluster found for name: {cluster_name}.")
    prefix = f"{cluster_name}\x00"
    associated = query.get("associatedPolicyArn")
    arns = []
    for k, e in _access_entries.items():
        if not k.startswith(prefix):
            continue
        if associated:
            # Only include entries that have this policy associated.
            if not any(
                ak.startswith(f"{cluster_name}\x00{e['principalArn']}\x00")
                and ak.endswith(f"\x00{associated}")
                for ak in _access_policies
            ):
                continue
        arns.append(e["principalArn"])
    max_results = int(query.get("maxResults", 100))
    return _json_resp(200, {"accessEntries": arns[:max_results]})


def _delete_access_entry(cluster_name, principal_arn):
    key = _ae_key(cluster_name, principal_arn)
    entry = _access_entries.get(key)
    if not entry:
        return _error(404, "ResourceNotFoundException",
                      f"No access entry for principal {principal_arn}.")
    # Cascading: drop associated access policies for this entry.
    ap_prefix = f"{cluster_name}\x00{principal_arn}\x00"
    for ak in [k for k in _access_policies if k.startswith(ap_prefix)]:
        _access_policies.pop(ak, None)
    _tags.pop(entry.get("accessEntryArn", ""), None)
    _access_entries.pop(key, None)
    _schedule_access_policy_reconcile(cluster_name)
    return _json_resp(200, {})


def _update_access_entry(cluster_name, principal_arn, body):
    key = _ae_key(cluster_name, principal_arn)
    entry = _access_entries.get(key)
    if not entry:
        return _error(404, "ResourceNotFoundException",
                      f"No access entry for principal {principal_arn}.")
    # AWS-allowed update fields only (per botocore model).
    for field in ("kubernetesGroups", "username"):
        if field in body:
            entry[field] = body[field]
    entry["modifiedAt"] = _now()
    return _json_resp(200, {"accessEntry": entry})


def _associate_access_policy(cluster_name, principal_arn, body):
    entry = _access_entries.get(_ae_key(cluster_name, principal_arn))
    if not entry:
        return _error(404, "ResourceNotFoundException",
                      f"No access entry for principal {principal_arn}.")
    if entry["type"] != "STANDARD":
        return _error(400, "InvalidRequestException",
                      "Access policies can only be associated with STANDARD access entries.")
    policy_arn = body.get("policyArn", "")
    if not policy_arn:
        return _error(400, "InvalidParameterException",
                      "policyArn is required.")
    access_scope = body.get("accessScope") or {}
    scope_type = access_scope.get("type")
    if scope_type not in ("cluster", "namespace"):
        return _error(400, "InvalidParameterException",
                      "accessScope.type must be 'cluster' or 'namespace'.")
    namespaces = access_scope.get("namespaces", [])
    if scope_type == "namespace":
        if (
            not isinstance(namespaces, list)
            or not namespaces
            or not all(isinstance(namespace, str) and namespace for namespace in namespaces)
        ):
            return _error(400, "InvalidParameterException",
                          "namespaces must be a nonempty list when accessScope.type is 'namespace'.")
    else:
        namespaces = []
    now = _now()
    key = _ap_key(cluster_name, principal_arn, policy_arn)
    existing = _access_policies.get(key) or {}
    associated = {
        "policyArn": policy_arn,
        "accessScope": {
            "type": scope_type,
            "namespaces": namespaces,
        },
        "associatedAt": existing.get("associatedAt", now),
        "modifiedAt": now,
    }
    _access_policies[key] = associated
    _schedule_access_policy_reconcile(cluster_name)
    return _json_resp(200, {
        "clusterName": cluster_name,
        "principalArn": principal_arn,
        "associatedAccessPolicy": associated,
    })


def _disassociate_access_policy(cluster_name, principal_arn, policy_arn):
    if _ae_key(cluster_name, principal_arn) not in _access_entries:
        return _error(404, "ResourceNotFoundException",
                      f"No access entry for principal {principal_arn}.")
    key = _ap_key(cluster_name, principal_arn, policy_arn)
    if key not in _access_policies:
        return _error(404, "ResourceNotFoundException",
                      f"Policy {policy_arn} is not associated with {principal_arn}.")
    _access_policies.pop(key, None)
    _schedule_access_policy_reconcile(cluster_name)
    return _json_resp(200, {})


def _list_associated_access_policies(cluster_name, principal_arn, query):
    if _ae_key(cluster_name, principal_arn) not in _access_entries:
        return _error(404, "ResourceNotFoundException",
                      f"No access entry for principal {principal_arn}.")
    prefix = f"{cluster_name}\x00{principal_arn}\x00"
    policies = [p for k, p in _access_policies.items() if k.startswith(prefix)]
    max_results = int(query.get("maxResults", 100))
    return _json_resp(200, {
        "clusterName": cluster_name,
        "principalArn": principal_arn,
        "associatedAccessPolicies": policies[:max_results],
    })


# ---------------------------------------------------------------------------
# Access policy RBAC materialization
# ---------------------------------------------------------------------------

_ACCESS_POLICY_ARN_PREFIX = "cluster-access-policy/"
_ACCESS_POLICY_BINDING_LABEL = "ministack.org/eks-access-policy"
_ACCESS_POLICY_MANAGED_LABEL = "ministack.org/managed"

# These are the published Kubernetes rules for the five general-purpose EKS
# access policies. EKS normally evaluates them in its proprietary authorizer;
# k3s has only Kubernetes RBAC, so MiniStack materializes equivalent roles.
# Keep this table explicit rather than referring to the built-in user-facing
# roles: their rules are only similar to, rather than identical to, EKS policy.
_ACCESS_POLICY_RULES = {
    "AmazonEKSClusterAdminPolicy": [
        {"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]},
        {"nonResourceURLs": ["*"], "verbs": ["*"]},
    ],
    "AmazonEKSAdminPolicy": [
        {"apiGroups": ["apps"], "resources": ["daemonsets", "deployments", "deployments/rollback", "deployments/scale", "replicasets", "replicasets/scale", "statefulsets", "statefulsets/scale"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["apps"], "resources": ["controllerrevisions", "daemonsets", "daemonsets/status", "deployments", "deployments/scale", "deployments/status", "replicasets", "replicasets/scale", "replicasets/status", "statefulsets", "statefulsets/scale", "statefulsets/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["authorization.k8s.io"], "resources": ["localsubjectaccessreviews"], "verbs": ["create"]},
        {"apiGroups": ["autoscaling"], "resources": ["horizontalpodautoscalers"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["autoscaling"], "resources": ["horizontalpodautoscalers", "horizontalpodautoscalers/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["batch"], "resources": ["cronjobs", "jobs"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["batch"], "resources": ["cronjobs", "cronjobs/status", "jobs", "jobs/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["discovery.k8s.io"], "resources": ["endpointslices"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["extensions"], "resources": ["daemonsets", "deployments", "deployments/rollback", "deployments/scale", "ingresses", "networkpolicies", "replicasets", "replicasets/scale", "replicationcontrollers/scale"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["extensions"], "resources": ["daemonsets", "daemonsets/status", "deployments", "deployments/scale", "deployments/status", "ingresses", "ingresses/status", "networkpolicies", "replicasets", "replicasets/scale", "replicasets/status", "replicationcontrollers/scale"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["networking.k8s.io"], "resources": ["ingresses", "ingresses/status", "networkpolicies"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["networking.k8s.io"], "resources": ["ingresses", "networkpolicies"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["policy"], "resources": ["poddisruptionbudgets"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["policy"], "resources": ["poddisruptionbudgets", "poddisruptionbudgets/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["rolebindings", "roles"], "verbs": ["create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"]},
        {"apiGroups": [""], "resources": ["configmaps", "endpoints", "persistentvolumeclaims", "persistentvolumeclaims/status", "pods", "replicationcontrollers", "replicationcontrollers/scale", "serviceaccounts", "services", "services/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["pods/attach", "pods/exec", "pods/portforward", "pods/proxy", "secrets", "services/proxy"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["configmaps", "events", "persistentvolumeclaims", "replicationcontrollers", "replicationcontrollers/scale", "secrets", "serviceaccounts", "services", "services/proxy"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": [""], "resources": ["pods", "pods/attach", "pods/exec", "pods/portforward", "pods/proxy"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": [""], "resources": ["serviceaccounts"], "verbs": ["impersonate"]},
        {"apiGroups": [""], "resources": ["bindings", "events", "limitranges", "namespaces/status", "pods/log", "pods/status", "replicationcontrollers/status", "resourcequotas", "resourcequotas/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get", "list", "watch"]},
    ],
    "AmazonEKSEditPolicy": [
        {"apiGroups": ["apps"], "resources": ["daemonsets", "deployments", "deployments/rollback", "deployments/scale", "replicasets", "replicasets/scale", "statefulsets", "statefulsets/scale"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["apps"], "resources": ["controllerrevisions", "daemonsets", "daemonsets/status", "deployments", "deployments/scale", "deployments/status", "replicasets", "replicasets/scale", "replicasets/status", "statefulsets", "statefulsets/scale", "statefulsets/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["autoscaling"], "resources": ["horizontalpodautoscalers", "horizontalpodautoscalers/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["autoscaling"], "resources": ["horizontalpodautoscalers"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["batch"], "resources": ["cronjobs", "jobs"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["batch"], "resources": ["cronjobs", "cronjobs/status", "jobs", "jobs/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["discovery.k8s.io"], "resources": ["endpointslices"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["extensions"], "resources": ["daemonsets", "deployments", "deployments/rollback", "deployments/scale", "ingresses", "networkpolicies", "replicasets", "replicasets/scale", "replicationcontrollers/scale"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["extensions"], "resources": ["daemonsets", "daemonsets/status", "deployments", "deployments/scale", "deployments/status", "ingresses", "ingresses/status", "networkpolicies", "replicasets", "replicasets/scale", "replicasets/status", "replicationcontrollers/scale"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["networking.k8s.io"], "resources": ["ingresses", "networkpolicies"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["networking.k8s.io"], "resources": ["ingresses", "ingresses/status", "networkpolicies"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["policy"], "resources": ["poddisruptionbudgets"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": ["policy"], "resources": ["poddisruptionbudgets", "poddisruptionbudgets/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["pods/attach", "pods/exec", "pods/portforward", "pods/proxy", "secrets", "services/proxy"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["serviceaccounts"], "verbs": ["impersonate"]},
        {"apiGroups": [""], "resources": ["pods", "pods/attach", "pods/exec", "pods/portforward", "pods/proxy"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": [""], "resources": ["configmaps", "events", "persistentvolumeclaims", "replicationcontrollers", "replicationcontrollers/scale", "secrets", "serviceaccounts", "services", "services/proxy"], "verbs": ["create", "delete", "deletecollection", "patch", "update"]},
        {"apiGroups": [""], "resources": ["configmaps", "endpoints", "persistentvolumeclaims", "persistentvolumeclaims/status", "pods", "replicationcontrollers", "replicationcontrollers/scale", "serviceaccounts", "services", "services/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["bindings", "events", "limitranges", "namespaces/status", "pods/log", "pods/status", "replicationcontrollers/status", "resourcequotas", "resourcequotas/status"], "verbs": ["get", "list", "watch"]},
    ],
    "AmazonEKSViewPolicy": [
        {"apiGroups": ["apps"], "resources": ["controllerrevisions", "daemonsets", "daemonsets/status", "deployments", "deployments/scale", "deployments/status", "replicasets", "replicasets/scale", "replicasets/status", "statefulsets", "statefulsets/scale", "statefulsets/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["autoscaling"], "resources": ["horizontalpodautoscalers", "horizontalpodautoscalers/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["batch"], "resources": ["cronjobs", "cronjobs/status", "jobs", "jobs/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["discovery.k8s.io"], "resources": ["endpointslices"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["extensions"], "resources": ["daemonsets", "daemonsets/status", "deployments", "deployments/scale", "deployments/status", "ingresses", "ingresses/status", "networkpolicies", "replicasets", "replicasets/scale", "replicasets/status", "replicationcontrollers/scale"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["networking.k8s.io"], "resources": ["ingresses", "ingresses/status", "networkpolicies"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": ["policy"], "resources": ["poddisruptionbudgets", "poddisruptionbudgets/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["configmaps", "endpoints", "persistentvolumeclaims", "persistentvolumeclaims/status", "pods", "replicationcontrollers", "replicationcontrollers/scale", "serviceaccounts", "services", "services/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["bindings", "events", "limitranges", "namespaces/status", "pods/log", "pods/status", "replicationcontrollers/status", "resourcequotas", "resourcequotas/status"], "verbs": ["get", "list", "watch"]},
        {"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get", "list", "watch"]},
    ],
    "AmazonEKSAdminViewPolicy": [
        {"apiGroups": ["*"], "resources": ["*"], "verbs": ["get", "list", "watch"]},
    ],
}


def _access_policy_name(policy_arn):
    """Return the supported EKS policy name for an AWS partition ARN."""
    if not isinstance(policy_arn, str):
        return None
    match = re.fullmatch(
        rf"arn:[^:]+:eks::aws:{_ACCESS_POLICY_ARN_PREFIX}([^/]+)",
        policy_arn,
    )
    if not match or match.group(1) not in _ACCESS_POLICY_RULES:
        return None
    return match.group(1)


def _access_policy_role_name(policy_arn):
    name = _access_policy_name(policy_arn)
    return f"ministack-eks-{name.lower()}" if name else None


def _access_policy_binding_id(cluster_name, principal_arn, policy_arn):
    value = "\x00".join((cluster_name, principal_arn, policy_arn)).encode()
    return hashlib.sha256(value).hexdigest()[:24]


def _access_policy_group(cluster_name, principal_arn, policy_arn):
    return "ministack:eks:access:" + _access_policy_binding_id(
        cluster_name, principal_arn, policy_arn
    )


def _access_policy_binding_name(cluster_name, principal_arn, policy_arn):
    return "ministack-eks-" + _access_policy_binding_id(
        cluster_name, principal_arn, policy_arn
    )


def _access_policy_associations(cluster_name, account_id, region):
    """Yield supported policy associations for one cluster and tenant."""
    prefix = f"{cluster_name}\x00"
    for (account, policy_region, key), association in _access_policies.all_items():
        if account != account_id or policy_region != region or not str(key).startswith(prefix):
            continue
        _name, principal_arn, policy_arn = str(key).split("\x00", 2)
        if _access_policy_name(policy_arn):
            yield principal_arn, policy_arn, association


def _access_policy_rbac_objects(cluster_name, account_id, region, namespaces):
    """Build MiniStack-managed RBAC objects for the cluster's associations."""
    objects = [
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {
                "name": _access_policy_role_name(
                    f"arn:aws:eks::aws:{_ACCESS_POLICY_ARN_PREFIX}{name}"
                ),
                "labels": {_ACCESS_POLICY_MANAGED_LABEL: "true"},
            },
            "rules": rules,
        }
        for name, rules in _ACCESS_POLICY_RULES.items()
    ]
    for principal_arn, policy_arn, association in _access_policy_associations(
        cluster_name, account_id, region
    ):
        scope = association.get("accessScope", {})
        role_name = _access_policy_role_name(policy_arn)
        binding_name = _access_policy_binding_name(
            cluster_name, principal_arn, policy_arn
        )
        subject = {
            "kind": "Group",
            "apiGroup": "rbac.authorization.k8s.io",
            "name": _access_policy_group(cluster_name, principal_arn, policy_arn),
        }
        metadata = {
            "name": binding_name,
            "labels": {_ACCESS_POLICY_BINDING_LABEL: "true"},
        }
        role_ref = {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": role_name,
        }
        if scope.get("type") == "cluster":
            objects.append({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRoleBinding",
                "metadata": metadata,
                "subjects": [subject],
                "roleRef": role_ref,
            })
            continue
        patterns = scope.get("namespaces") or []
        for namespace in sorted({n for n in namespaces if any(
            fnmatch.fnmatchcase(n, pattern) for pattern in patterns
        )}):
            objects.append({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {**metadata, "namespace": namespace},
                "subjects": [subject],
                "roleRef": role_ref,
            })
    return objects


def _k3s_exec(container, command):
    result = container.exec_run(command)
    if hasattr(result, "exit_code"):
        exit_code, output = result.exit_code, result.output
    elif isinstance(result, tuple):
        exit_code = result[0]
        output = result[1] if len(result) > 1 else b""
    else:
        exit_code, output = 0, b""
    return exit_code, output.decode(errors="replace") if isinstance(output, bytes) else output


def _apply_access_policy_rbac(container, objects):
    """Apply a JSON Kubernetes List without relying on a container temp file."""
    content = json.dumps({"apiVersion": "v1", "kind": "List", "items": objects}).encode()
    payload = base64.b64encode(content).decode("ascii")
    # docker-py's exec_run does not expose a simple stdin path with an exit
    # status. The payload is base64, so it is safe to pass as one shell token;
    # decoding it in the container avoids Docker archive writes to k3s's tmpfs.
    command = ["sh", "-c", f"printf '%s' '{payload}' | base64 -d | kubectl apply -f -"]
    code, output = _k3s_exec(container, command)
    if code:
        raise RuntimeError(output)


def _managed_access_policy_bindings(container, kind):
    command = ["kubectl", "get", kind.lower(), "-l", f"{_ACCESS_POLICY_BINDING_LABEL}=true", "-o", "json"]
    if kind == "RoleBinding":
        command.append("--all-namespaces")
    code, output = _k3s_exec(container, command)
    if code:
        raise RuntimeError(output)
    return json.loads(output).get("items", [])


def _binding_key(obj):
    return (
        obj.get("kind"), obj.get("metadata", {}).get("namespace", ""),
        obj.get("metadata", {}).get("name"),
    )


def _reconcile_access_policy_rbac(cluster_name, account_id, region):
    """Reconcile managed bindings after access-policy or namespace changes."""
    key = (account_id, region, cluster_name)
    with _rbac_reconcilers_lock:
        lock = _rbac_reconcile_locks.setdefault(key, threading.Lock())
    with lock:
        cluster = _clusters.get_scoped(account_id, region, cluster_name)
        if not cluster or not cluster.get("_docker_id"):
            return False
        client = _get_docker()
        if not client:
            return False
        try:
            container = client.containers.get(cluster["_docker_id"])
            code, output = _k3s_exec(container, ["kubectl", "get", "namespaces", "-o", "json"])
            if code:
                raise RuntimeError(output)
            namespaces = [item["metadata"]["name"] for item in json.loads(output).get("items", [])]
            objects = _access_policy_rbac_objects(cluster_name, account_id, region, namespaces)
            desired = {
                _binding_key(obj) for obj in objects
                if obj["kind"] in ("ClusterRoleBinding", "RoleBinding")
            }
            _apply_access_policy_rbac(container, objects)
            for kind in ("ClusterRoleBinding", "RoleBinding"):
                for existing in _managed_access_policy_bindings(container, kind):
                    binding_key = _binding_key(existing)
                    if binding_key in desired:
                        continue
                    command = ["kubectl", "delete", kind.lower(), binding_key[2]]
                    if binding_key[1]:
                        command.extend(["--namespace", binding_key[1]])
                    code, output = _k3s_exec(container, command)
                    if code:
                        raise RuntimeError(output)
            return True
        except Exception as e:
            logger.debug("EKS: RBAC reconciliation for %s failed: %s", cluster_name, e)
            return False


def _has_namespace_access_policy(cluster_name, account_id, region):
    return any(
        association.get("accessScope", {}).get("type") == "namespace"
        for _principal, _policy, association in _access_policy_associations(
            cluster_name, account_id, region
        )
    )


def _schedule_access_policy_reconcile(cluster_name, account_id=None, region=None):
    account_id = account_id or get_account_id()
    region = region or get_region()
    key = (account_id, region, cluster_name)
    with _rbac_reconcilers_lock:
        running = _rbac_reconcilers.get(key)
        if running:
            running[1].set()
            return
        stop = threading.Event()
        wake = threading.Event()
        state = (stop, wake)
        _rbac_reconcilers[key] = state

    def reconcile_loop():
        try:
            while not stop.is_set():
                wake.clear()
                reconciled = _reconcile_access_policy_rbac(
                    cluster_name, account_id, region
                )
                if not reconciled:
                    cluster = _clusters.get_scoped(account_id, region, cluster_name)
                    if not cluster or (
                        cluster.get("status") == "ACTIVE"
                        and not cluster.get("_docker_id")
                    ):
                        return
                    wake.wait(_EKS_RBAC_RECONCILE_INTERVAL)
                    continue
                watches_namespaces = _has_namespace_access_policy(
                    cluster_name, account_id, region
                )
                with _rbac_reconcilers_lock:
                    if _rbac_reconcilers.get(key) is not state:
                        return
                    if wake.is_set():
                        continue
                    if not watches_namespaces:
                        _rbac_reconcilers.pop(key, None)
                        return
                wake.wait(_EKS_RBAC_RECONCILE_INTERVAL)
        finally:
            with _rbac_reconcilers_lock:
                if _rbac_reconcilers.get(key) is state:
                    _rbac_reconcilers.pop(key, None)

    threading.Thread(
        target=reconcile_loop, daemon=True,
        name=f"eks-rbac-{cluster_name}",
    ).start()


def _stop_access_policy_reconciler(cluster_name, account_id, region):
    with _rbac_reconcilers_lock:
        state = _rbac_reconcilers.pop((account_id, region, cluster_name), None)
    if state:
        stop, wake = state
        stop.set()
        wake.set()


def _stop_all_rbac_reconcilers():
    with _rbac_reconcilers_lock:
        states = list(_rbac_reconcilers.values())
        _rbac_reconcilers.clear()
    for stop, wake in states:
        stop.set()
        wake.set()


# ---------------------------------------------------------------------------
# Encryption config (AssociateEncryptionConfig)
# ---------------------------------------------------------------------------

def _associate_encryption_config(cluster_name, body):
    cluster = _clusters.get(cluster_name)
    if not cluster:
        return _error(404, "ResourceNotFoundException",
                      f"No cluster found for name: {cluster_name}.")
    new_cfg = body.get("encryptionConfig") or []
    if not new_cfg:
        return _error(400, "InvalidParameterException",
                      "encryptionConfig is required.")
    if len(new_cfg) > 1:
        return _error(400, "InvalidParameterException",
                      "encryptionConfig array can have at most 1 item.")
    if cluster.get("encryptionConfig"):
        return _error(400, "InvalidRequestException",
                      f"Cluster {cluster_name} already has encryption configuration associated.")
    cluster["encryptionConfig"] = new_cfg
    update = {
        "id": new_uuid(),
        "status": "Successful",
        "type": "AssociateEncryptionConfig",
        "params": [{"type": "EncryptionConfig", "value": json.dumps(new_cfg)}],
        "createdAt": _now(),
        "errors": [],
    }
    return _json_resp(200, {"update": update})


# ---------------------------------------------------------------------------
# OIDC Identity Provider Config (AssociateIdentityProviderConfig)
# ---------------------------------------------------------------------------

def _restart_k3s(cluster_name, oidc_args=None, idp_cfg_refs=None):
    """Restart the cluster's k3s container with the supplied OIDC args.

    Both ``oidc_args`` and ``idp_cfg_refs`` must be captured by the CALLER
    inside the request context (where AccountRegionScopedDict can resolve the
    account and region). The background thread closes over them so it never
    needs the request's contextvars.
    """
    cluster = _clusters.get(cluster_name)
    if not cluster:
        return
    client = _get_docker()
    if not client:
        return

    # Real AWS keeps cluster status ACTIVE during IdP changes — the work is
    # carried in the Update record, not on the cluster itself. Mutate cfg state
    # only.
    oidc_args = oidc_args or []
    idp_cfg_refs = idp_cfg_refs or []
    region = get_region()
    node_labels = _collect_node_labels(cluster)

    def _mark_idp_active():
        for cfg in idp_cfg_refs:
            cfg["status"] = "ACTIVE"

    def _bg_restart():
        ms_network = None
        try:
            docker_id = cluster.get("_docker_id")
            if docker_id:
                try:
                    container = client.containers.get(docker_id)
                    container.stop(timeout=5)
                    container.remove(v=True, force=True)
                except Exception:
                    pass
                cluster["_docker_id"] = None

            ms_network = _get_ministack_network(client)
            run_kwargs = _k3s_run_kwargs(
                name=cluster_name,
                region=region,
                port=cluster["_port"],
                ms_network=ms_network,
                oidc_args=oidc_args,
                node_labels=node_labels,
            )

            registries_yaml = _k3s_registries_yaml(client, ms_network, _ecr_registry_hosts(cluster))
            cluster_spec = parse_arn(cluster.get("arn", ""))
            auth_webhook_config = _k3s_auth_webhook_config(
                client, ms_network, cluster_name, cluster_spec.account_id, cluster_spec.region
            )
            container = _start_k3s_container(
                client, run_kwargs, registries_yaml, auth_webhook_config
            )
            cluster["_docker_id"] = container.id

            cluster["endpoint"] = _cluster_endpoint(cluster["_port"])
            cluster["certificateAuthority"]["data"] = _extract_ca_cert(container)
            _mark_idp_active()
            _schedule_access_policy_reconcile(
                cluster_name, cluster_spec.account_id, cluster_spec.region
            )
        except Exception as e:
            logger.warning("EKS: failed to restart k3s for %s — falling back to mock: %s", cluster_name, e)
            cluster["certificateAuthority"]["data"] = base64.b64encode(b"MOCK-CA-CERTIFICATE").decode()
            # No container came up — advertise the host-published endpoint.
            cluster["endpoint"] = _cluster_endpoint(cluster["_port"])
            _mark_idp_active()

    threading.Thread(target=_bg_restart, daemon=True, name=f"eks-restart-{cluster_name}").start()


def _associate_identity_provider_config(cluster_name, body):
    cluster = _clusters.get(cluster_name)
    if not cluster:
        return _error(404, "ResourceNotFoundException", f"No cluster found for name: {cluster_name}.")

    oidc = body.get("oidc")
    if not oidc:
        return _error(400, "InvalidParameterException", "oidc configuration is required.")

    idp_name = oidc.get("identityProviderConfigName")
    if not idp_name:
        return _error(400, "InvalidParameterException", "identityProviderConfigName is required inside oidc config.")

    if not oidc.get("issuerUrl") or not oidc.get("clientId"):
        return _error(400, "InvalidParameterException", "issuerUrl and clientId are required inside oidc config.")

    # AWS allows only one OIDC IdP config per cluster regardless of name —
    # this covers same-name and different-name duplicates in one check.
    for existing_key in _idp_configs.keys():
        if existing_key.startswith(f"{cluster_name}\x00"):
            return _error(409, "ResourceInUseException", f"Cluster '{cluster_name}' already has an OIDC identity provider configuration.")
    key = f"{cluster_name}\x00{idp_name}"

    arn = (
        f"arn:aws:eks:{get_region()}:{get_account_id()}"
        f":identityproviderconfig/{cluster_name}/oidc/{idp_name}/{new_uuid()}"
    )
    tags = body.get("tags") or {}
    _idp_configs[key] = {
        "oidc": oidc,
        "status": "CREATING",
        "tags": tags,
        "arn": arn,
    }
    if tags:
        _tags[arn] = dict(tags)

    logger.warning(
        "EKS: AssociateIdentityProviderConfig on cluster %s triggers a k3s restart "
        "which wipes in-cluster workloads (Pods/Deployments/Services). "
        "Local-emulator limitation — real AWS rolls config without affecting the data plane.",
        cluster_name,
    )

    oidc_args, idp_cfg_refs = _collect_oidc_state(cluster_name)
    _restart_k3s(cluster_name, oidc_args=oidc_args, idp_cfg_refs=idp_cfg_refs)

    update = {
        "id": new_uuid(),
        "status": "InProgress",
        "type": "IdentityProviderConfigUpdate",
        "params": [{"type": "IdentityProviderConfig", "value": idp_name}],
        "createdAt": _now(),
        "errors": [],
    }
    return _json_resp(200, {"update": update, "tags": body.get("tags") or {}})


def _describe_identity_provider_config(cluster_name, body):
    idp_cfg = body.get("identityProviderConfig") or {}
    name = idp_cfg.get("name")
    if not name:
        return _error(400, "InvalidParameterException", "name is required in identityProviderConfig.")

    key = f"{cluster_name}\x00{name}"
    cfg = _idp_configs.get(key)
    if not cfg:
        return _error(404, "ResourceNotFoundException", f"OIDC provider configuration '{name}' not found on cluster '{cluster_name}'.")

    oidc = cfg["oidc"]
    response = {
        "identityProviderConfig": {
            "oidc": {
                "clientId": oidc.get("clientId"),
                "clusterName": cluster_name,
                "groupsClaim": oidc.get("groupsClaim"),
                "groupsPrefix": oidc.get("groupsPrefix"),
                "identityProviderConfigArn": cfg.get("arn", ""),
                "identityProviderConfigName": name,
                "issuerUrl": oidc.get("issuerUrl"),
                "requiredClaims": oidc.get("requiredClaims") or {},
                "status": cfg.get("status", "ACTIVE"),
                "tags": cfg.get("tags") or {},
                "usernameClaim": oidc.get("usernameClaim"),
                "usernamePrefix": oidc.get("usernamePrefix"),
            }
        }
    }
    return _json_resp(200, response)


def _disassociate_identity_provider_config(cluster_name, body):
    cluster = _clusters.get(cluster_name)
    if not cluster:
        return _error(404, "ResourceNotFoundException", f"No cluster found for name: {cluster_name}.")

    idp_cfg = body.get("identityProviderConfig") or {}
    name = idp_cfg.get("name")
    if not name:
        return _error(400, "InvalidParameterException", "name is required in identityProviderConfig.")

    key = f"{cluster_name}\x00{name}"
    if key not in _idp_configs:
        return _error(404, "ResourceNotFoundException", f"OIDC provider configuration '{name}' not found on cluster '{cluster_name}'.")

    removed = _idp_configs.pop(key, None)
    if removed and removed.get("arn"):
        _tags.pop(removed["arn"], None)

    logger.warning(
        "EKS: DisassociateIdentityProviderConfig on cluster %s triggers a k3s restart "
        "which wipes in-cluster workloads (Pods/Deployments/Services). "
        "Local-emulator limitation — real AWS rolls config without affecting the data plane.",
        cluster_name,
    )

    oidc_args, idp_cfg_refs = _collect_oidc_state(cluster_name)
    _restart_k3s(cluster_name, oidc_args=oidc_args, idp_cfg_refs=idp_cfg_refs)

    update = {
        "id": new_uuid(),
        "status": "InProgress",
        "type": "IdentityProviderConfigUpdate",
        "params": [{"type": "IdentityProviderConfig", "value": name}],
        "createdAt": _now(),
        "errors": [],
    }
    return _json_resp(200, {"update": update})


def _list_identity_provider_configs(cluster_name):
    cluster = _clusters.get(cluster_name)
    if not cluster:
        return _error(404, "ResourceNotFoundException", f"No cluster found for name: {cluster_name}.")

    # AWS caps OIDC IdP configs at one per cluster (enforced in
    # _associate_identity_provider_config), so this list is always <=1 entry
    # and never needs maxResults / nextToken pagination.
    configs = []
    for key in _idp_configs.keys():
        cn, _, name = key.partition("\x00")
        if cn == cluster_name:
            configs.append({"name": name, "type": "oidc"})

    return _json_resp(200, {"identityProviderConfigs": configs})


# ---------------------------------------------------------------------------
# OIDC discovery / JWKS (IRSA support)
# ---------------------------------------------------------------------------


def _oidc_discovery(oidc_id):
    issuer = _issuer_url(oidc_id)
    return _json_resp(200, {
        "issuer": issuer,
        "jwks_uri": f"{issuer}/keys",
        # Real AWS EKS publishes this exact sentinel — IRSA never uses an
        # interactive authorization flow, it just validates signed tokens.
        "authorization_endpoint": "urn:kubernetes:programmatic_authorization",
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "claims_supported": ["sub", "iss"],
    })


def _oidc_jwks():
    try:
        _, jwk, _ = _get_oidc_keypair()
    except ImportError:
        return _error(500, "ServiceUnavailable", "cryptography library unavailable")
    return _json_resp(200, {"keys": [jwk]})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _invalid_tag_resource_arn(arn):
    return _error(400, "InvalidParameterException", f"Invalid resourceArn: {arn}")


def _tag_resource_not_found(arn):
    return _error(404, "ResourceNotFoundException", f"No resource found for ARN: {arn}.")


def _resolve_tag_resource_arn(arn):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None, _invalid_tag_resource_arn(arn)

    if (
        spec.partition != "aws"
        or spec.service != "eks"
        or spec.account_id != get_account_id()
    ):
        return None, _invalid_tag_resource_arn(arn)

    request_region = get_region()
    parts = spec.resource.split("/")
    resource_type = parts[0] if parts else ""

    # Legacy account-scoped state could create a child in a different region
    # from its parent. Restore co-locates those children with the parent while
    # preserving their ARN (and the ARN-keyed persisted tags). Accept that
    # exact ARN only when the child exists in the current scoped store.
    if resource_type == "cluster":
        if len(parts) != 2 or not parts[1] or spec.region != request_region:
            return None, _invalid_tag_resource_arn(arn)
        cluster = _clusters.get(parts[1])
        if not cluster or cluster.get("arn") != arn:
            return None, _tag_resource_not_found(arn)
        return cluster["arn"], None

    if resource_type == "nodegroup":
        if len(parts) != 4 or not all(parts[1:]):
            return None, _invalid_tag_resource_arn(arn)
        nodegroup = _nodegroups.get(f"{parts[1]}/{parts[2]}")
        if not nodegroup or nodegroup.get("nodegroupArn") != arn:
            if spec.region != request_region:
                return None, _invalid_tag_resource_arn(arn)
            return None, _tag_resource_not_found(arn)
        return nodegroup["nodegroupArn"], None

    if resource_type == "addon":
        if len(parts) != 4 or not all(parts[1:]):
            return None, _invalid_tag_resource_arn(arn)
        addon = _addons.get(f"{parts[1]}/{parts[2]}")
        if not addon or addon.get("addonArn") != arn:
            if spec.region != request_region:
                return None, _invalid_tag_resource_arn(arn)
            return None, _tag_resource_not_found(arn)
        return addon["addonArn"], None

    if resource_type == "access-entry":
        if len(parts) != 3 or not all(parts[1:]):
            return None, _invalid_tag_resource_arn(arn)
        for entry in _access_entries.values():
            if entry.get("clusterName") == parts[1] and entry.get("accessEntryArn") == arn:
                return entry["accessEntryArn"], None
        if spec.region != request_region:
            return None, _invalid_tag_resource_arn(arn)
        return None, _tag_resource_not_found(arn)

    if resource_type == "identityproviderconfig":
        if len(parts) != 5 or parts[2] != "oidc" or not all(parts[1:]):
            return None, _invalid_tag_resource_arn(arn)
        cfg = _idp_configs.get(f"{parts[1]}\x00{parts[3]}")
        if not cfg or cfg.get("arn") != arn:
            if spec.region != request_region:
                return None, _invalid_tag_resource_arn(arn)
            return None, _tag_resource_not_found(arn)
        return cfg["arn"], None

    return None, _invalid_tag_resource_arn(arn)


def _tag_resource(arn, body):
    arn, err = _resolve_tag_resource_arn(arn)
    if err:
        return err
    tags = body.get("tags", {})
    existing = _tags.get(arn, {})
    existing.update(tags)
    _tags[arn] = existing
    return _json_resp(200, {})


def _untag_resource(arn, query):
    arn, err = _resolve_tag_resource_arn(arn)
    if err:
        return err
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
    arn, err = _resolve_tag_resource_arn(arn)
    if err:
        return err
    return _json_resp(200, {"tags": _tags.get(arn, {})})


# ---------------------------------------------------------------------------
# AWS IAM authenticator compatible TokenReview webhook
# ---------------------------------------------------------------------------

_EKS_TOKEN_PREFIX = "k8s-aws-v1."
_EKS_TOKEN_VALIDITY_SECONDS = 15 * 60


def _token_review_response(review, *, authenticated=False, username="", uid="", groups=None,
                           audiences=None):
    status = {"authenticated": authenticated}
    if authenticated:
        status["user"] = {
            "username": username,
            "uid": uid,
            "groups": groups or [],
        }
        if audiences:
            status["audiences"] = audiences
    return _json_resp(200, {
        "apiVersion": review.get("apiVersion", "authentication.k8s.io/v1"),
        "kind": "TokenReview",
        "status": status,
    })


def _decode_aws_iam_token(token):
    if not isinstance(token, str) or len(token) > 16384 or not token.startswith(_EKS_TOKEN_PREFIX):
        return None
    encoded = token[len(_EKS_TOKEN_PREFIX):]
    try:
        encoded += "=" * (-len(encoded) % 4)
        raw = base64.urlsafe_b64decode(encoded.encode("ascii"))
        url = raw.decode("utf-8")
        parsed = urllib.parse.urlsplit(url)
    except (ValueError, UnicodeDecodeError, UnicodeEncodeError):
        return None
    if parsed.scheme != "https" or not parsed.netloc or parsed.path not in ("", "/"):
        return None
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if any(len(values) != 1 for values in query.values()):
        return None
    return parsed, query


def _qp(query, name):
    values = query.get(name) or query.get(name.lower())
    if isinstance(values, (list, tuple)):
        return values[0] if values else ""
    return values or ""


def _verify_eks_token(token, cluster_name):
    """Verify an AWS CLI EKS exec token without calling the real STS service.

    ``aws eks get-token`` signs a GET to regional STS with the cluster name in
    the ``x-k8s-aws-id`` signed header. The URL's advertised 60-second
    presign lifetime is intentionally not used here: kubectl caches the
    resulting ExecCredential for roughly 14 minutes and the upstream
    authenticator accepts a 15-minute token window.
    """
    decoded = _decode_aws_iam_token(token)
    if not decoded:
        return None
    parsed, query = decoded
    if (
        _qp(query, "X-Amz-Algorithm") != "AWS4-HMAC-SHA256"
        or _qp(query, "Action") != "GetCallerIdentity"
        or _qp(query, "Version") != "2011-06-15"
    ):
        return None
    credential_scope = _qp(query, "X-Amz-Credential").split("/")
    amz_date = _qp(query, "X-Amz-Date")
    signed_headers = _qp(query, "X-Amz-SignedHeaders").lower()
    signature = _qp(query, "X-Amz-Signature")
    if (
        len(credential_scope) != 5
        or not credential_scope[0]
        or not amz_date
        or not re.fullmatch(r"[0-9a-f]{64}", signature)
        or credential_scope[3] != "sts"
        or credential_scope[4] != "aws4_request"
        or "host" not in signed_headers.split(";")
        or "x-k8s-aws-id" not in signed_headers.split(";")
    ):
        return None

    host = parsed.netloc
    hostname = parsed.hostname or ""
    if not re.fullmatch(r"sts(?:[.-][a-z0-9-]+)?\.amazonaws\.com", hostname):
        return None
    if hostname != f"sts.{credential_scope[2]}.amazonaws.com":
        return None
    try:
        if not 0 <= int(_qp(query, "X-Amz-Expires")) <= _EKS_TOKEN_VALIDITY_SECONDS:
            return None
        signed_at = dt.datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None
    age = (dt.datetime.now(dt.timezone.utc) - signed_at).total_seconds()
    if age < -300 or age > _EKS_TOKEN_VALIDITY_SECONDS or credential_scope[1] != amz_date[:8]:
        return None

    # The cluster ID is not present in the presigned query string. It is the
    # value of a signed header on the original STS request, so inject the
    # cluster being authenticated before rebuilding the canonical request.
    signed_request_headers = {
        "host": host,
        "x-k8s-aws-id": cluster_name,
    }
    canonical_request = build_canonical_request(
        "GET",
        parsed.path or "/",
        signed_request_headers,
        query,
        signed_headers,
        # botocore's STS presigner signs an empty GET payload. Accepting only
        # this hash also keeps the verifier compatible with the authenticator.
        payload_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    )
    string_to_sign = build_string_to_sign(
        amz_date,
        credential_scope[1],
        credential_scope[2],
        credential_scope[3],
        canonical_request,
    )
    access_key_id = credential_scope[0]
    try:
        account_id = find_iam_access_key_account(access_key_id)
    except AmbiguousAccessKeyError:
        return None
    # TokenReview calls have no AWS Authorization header. Resolve every kind
    # of key independently of the gateway request's account context.
    account_id = account_id or _account_from_sts_session(access_key_id)
    if re.fullmatch(r"\d{12}", access_key_id):
        account_id = access_key_id
    credential = resolve_credential(
        access_key_id,
        account_id or os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000"),
        _qp(query, "X-Amz-Security-Token") or None,
    )
    if isinstance(credential, CredentialResolutionError):
        return None
    expected = calculate_signature(
        credential.secret_access_key,
        credential_scope[1],
        credential_scope[2],
        credential_scope[3],
        string_to_sign,
    )
    if not signatures_match(expected, signature):
        return None
    return credential


def _cluster_for_auth(cluster_name, account_id=None, region=None):
    """Find a cluster across scoped stores without leaking tenant state."""
    matches = [
        (account, cluster_region, cluster)
        for (account, cluster_region, name), cluster in _clusters.all_items()
        if name == cluster_name
        and (account_id is None or account == account_id)
        and (region is None or cluster_region == region)
    ]
    return matches[0] if len(matches) == 1 else None


def _role_arn_from_assumed_role(arn):
    match = re.fullmatch(r"arn:([^:]+):sts::([^:]+):assumed-role/([^/]+)/[^/]+", arn or "")
    if not match:
        return ""
    partition, account, role = match.groups()
    # STS session ARNs omit the IAM role's path. Recover it from IAM so that
    # access entries for role/team/developer match assumed-role/developer/...
    from ministack.services import iam
    record = iam._roles.get_scoped(account, None, role)
    if record:
        return record.get("Arn", "")
    return f"arn:{partition}:iam::{account}:role/{role}"


def _access_entry_for_principal(cluster_name, account_id, region, principal_arn):
    candidates = [principal_arn]
    role_arn = _role_arn_from_assumed_role(principal_arn)
    if role_arn:
        candidates.append(role_arn)
    for candidate in candidates:
        entry = _access_entries.get_scoped(
            account_id, region, _ae_key(cluster_name, candidate)
        )
        if entry:
            return entry
    return None


def _authenticate_token_review(cluster_name, account_id, region, review):
    from ministack.app import AUTH

    spec = review.get("spec")
    if not isinstance(spec, dict) or not isinstance(spec.get("token"), str) or not spec["token"]:
        return _token_review_response(review)

    cluster_info = _cluster_for_auth(cluster_name, account_id, region)
    if not cluster_info:
        return _token_review_response(review)
    account_id, region, cluster = cluster_info

    # Permissive mode accepts local bearer credentials without requiring an
    # IAM identity, a matching secret, or an access entry, just like the AWS
    # control plane skips IAM enforcement when AUTH=false.
    if not AUTH:
        return _token_review_response(
            review, authenticated=True, username="ministack-local", uid="ministack-local",
            groups=["system:authenticated", "system:masters"],
        )

    credential = _verify_eks_token(spec["token"], cluster_name)
    if not credential:
        return _token_review_response(review)

    # IAM authorizes AWS EKS API operations at the gateway. Kubernetes access
    # additionally requires the bootstrap creator grant or an access entry;
    # an IAM Allow for eks:* alone does not confer Kubernetes permissions.
    entry = _access_entry_for_principal(
        cluster_name, account_id, region, credential.principal_arn
    )
    username = credential.principal_arn
    groups = ["system:authenticated"]
    if entry:
        username = entry.get("username") or username
        groups.extend(entry.get("kubernetesGroups") or [])
        # EKS evaluates managed access policies in a separate authorizer. k3s
        # has only RBAC, so each policy becomes an internal RBAC group bound
        # to a MiniStack-managed ClusterRole or RoleBinding.
        groups.extend(
            _access_policy_group(cluster_name, entry["principalArn"], policy_arn)
            for principal_arn, policy_arn, _association in _access_policy_associations(
                cluster_name, account_id, region
            )
            if principal_arn == entry["principalArn"]
        )
    elif (
        credential.account_id == account_id
        and cluster.get("accessConfig", {}).get("bootstrapClusterCreatorAdminPermissions", True)
        and cluster.get("_creator_arn") == (
            _role_arn_from_assumed_role(credential.principal_arn) or credential.principal_arn
        )
    ):
        groups.append("system:masters")
    else:
        return _token_review_response(review)

    # Deduplicate while preserving the caller's configured order.
    groups = list(dict.fromkeys(groups))
    return _token_review_response(
        review,
        authenticated=True,
        username=username,
        uid=credential.principal_id or credential.principal_arn,
        groups=groups,
    )


def _handle_auth_webhook(cluster_name, account_id, region, body_bytes):
    try:
        review = json.loads(body_bytes) if body_bytes else {}
    except (TypeError, json.JSONDecodeError):
        review = {}
    if not isinstance(review, dict):
        review = {}
    return _authenticate_token_review(cluster_name, account_id, region, review)


# ---------------------------------------------------------------------------
# Sanitize (remove internal fields)
# ---------------------------------------------------------------------------

def _sanitize(cluster):
    return {k: v for k, v in cluster.items() if not k.startswith("_")}


# ---------------------------------------------------------------------------
# Request Router
# ---------------------------------------------------------------------------

def _handle_request_sync(method, path, headers, body_bytes, query_params):
    # This private route is called by the k3s apiserver's authentication
    # webhook, not by an AWS EKS client. Keep it outside the EKS JSON API
    # namespace so the normal AWS action router never sees TokenReview data.
    auth_match = re.fullmatch(
        r"/eks-auth/(\d{12})/([A-Za-z0-9-]+)/([A-Za-z0-9_.-]+)", path
    )
    if auth_match and method == "POST":
        return _handle_auth_webhook(
            urllib.parse.unquote(auth_match.group(3)),
            auth_match.group(1),
            auth_match.group(2),
            body_bytes,
        )

    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {}

    query = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}

    # POST /clusters
    if path == "/clusters" and method == "POST":
        identity = resolve_caller_identity(extract_access_key_id(headers, query_params))
        creator_arn = identity["userArn"] if identity else None
        return _create_cluster(body, _role_arn_from_assumed_role(creator_arn) or creator_arn)

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

    # POST /clusters/{name}/encryption-config/associate — AssociateEncryptionConfig
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/encryption-config/associate", path)
    if m:
        cluster_name = m.group(1)
        if method == "POST":
            return _associate_encryption_config(cluster_name, body)

    # POST /clusters/{name}/identity-provider-configs/associate
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/identity-provider-configs/associate", path)
    if m:
        cluster_name = m.group(1)
        if method == "POST":
            return _associate_identity_provider_config(cluster_name, body)

    # POST /clusters/{name}/identity-provider-configs/disassociate
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/identity-provider-configs/disassociate", path)
    if m:
        cluster_name = m.group(1)
        if method == "POST":
            return _disassociate_identity_provider_config(cluster_name, body)

    # POST /clusters/{name}/identity-provider-configs/describe
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/identity-provider-configs/describe", path)
    if m:
        cluster_name = m.group(1)
        if method == "POST":
            return _describe_identity_provider_config(cluster_name, body)

    # GET /clusters/{name}/identity-provider-configs
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/identity-provider-configs", path)
    if m:
        cluster_name = m.group(1)
        if method == "GET":
            return _list_identity_provider_configs(cluster_name)

    # OIDC discovery + JWKS (IRSA). Path matches AWS shape under the ministack
    # /oidc prefix because we can't own oidc.eks.{region}.amazonaws.com.
    m = re.fullmatch(r"/oidc/id/([A-Z0-9]+)/\.well-known/openid-configuration", path)
    if m and method == "GET":
        return _oidc_discovery(m.group(1))
    if re.fullmatch(r"/oidc/id/[A-Z0-9]+/keys", path) and method == "GET":
        return _oidc_jwks()

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

    # Access Entries. botocore sends principalArn raw in the path (includes
    # colons and forward slashes from the ARN, e.g.
    # ``arn:aws:iam::000000000000:role/foo``), so the regex must accept
    # slashes. Most-specific routes first; non-greedy `.+?` against the
    # ``/access-policies`` suffix prevents the principalArn capture from
    # swallowing the policy segment.
    # DELETE /clusters/{name}/access-entries/{principalArn}/access-policies/{policyArn}
    m = re.fullmatch(
        r"/clusters/([A-Za-z0-9_-]+)/access-entries/(.+?)/access-policies/(.+)", path)
    if m:
        cluster_name = m.group(1)
        principal_arn = urllib.parse.unquote(m.group(2))
        policy_arn = urllib.parse.unquote(m.group(3))
        if method == "DELETE":
            return _disassociate_access_policy(cluster_name, principal_arn, policy_arn)

    # POST/GET /clusters/{name}/access-entries/{principalArn}/access-policies
    m = re.fullmatch(
        r"/clusters/([A-Za-z0-9_-]+)/access-entries/(.+?)/access-policies", path)
    if m:
        cluster_name = m.group(1)
        principal_arn = urllib.parse.unquote(m.group(2))
        if method == "POST":
            return _associate_access_policy(cluster_name, principal_arn, body)
        if method == "GET":
            return _list_associated_access_policies(cluster_name, principal_arn, query)

    # POST/GET /clusters/{name}/access-entries — CreateAccessEntry / ListAccessEntries
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/access-entries", path)
    if m:
        cluster_name = m.group(1)
        if method == "POST":
            return _create_access_entry(cluster_name, body)
        if method == "GET":
            return _list_access_entries(cluster_name, query)

    # /clusters/{name}/access-entries/{principalArn} — Describe / Update / Delete.
    # Greedy `.+` is safe here only because the more-specific
    # `/access-policies` routes above already matched and returned.
    m = re.fullmatch(r"/clusters/([A-Za-z0-9_-]+)/access-entries/(.+)", path)
    if m:
        cluster_name = m.group(1)
        principal_arn = urllib.parse.unquote(m.group(2))
        if method == "GET":
            return _describe_access_entry(cluster_name, principal_arn)
        if method == "POST":
            return _update_access_entry(cluster_name, principal_arn, body)
        if method == "DELETE":
            return _delete_access_entry(cluster_name, principal_arn)

    # Tags: /tags/{arn+}
    if path.startswith("/tags/"):
        arn = urllib.parse.unquote(path[6:])
        if method == "GET":
            return _list_tags(arn)
        if method == "POST":
            return _tag_resource(arn, body)
        if method == "DELETE":
            return _untag_resource(arn, query)

    return _error(400, "InvalidRequestException", f"No route for {method} {path}")


async def handle_request(method, path, headers, body, query_params):
    """Dispatch off the event loop.

    Request paths here reach the Docker daemon (container create/start/stop/
    inspect), which blocks for as long as the daemon takes. Measured on ECS: a
    cached-image container start held the loop for 7.3s, during which the health
    endpoint — the cheapest request in the process — could not be served.

    Uses run_reentrant, not the shared pool: containers started here are handed
    an endpoint pointing back at MiniStack, so one calls in while this
    dispatch is still running. A bounded pool would queue that nested request
    behind the call waiting on it.
    """
    return await run_reentrant(
        _handle_request_sync, method, path, headers, body, query_params, thread_name="ministack-eks-dispatch")


def _live_container_ids():
    """Container ids still owned by a live EKS cluster.

    EKS records its k3s container under ``_docker_id`` (not the
    ``_docker_container_id`` the other services use), which is why this was
    missed first time round and its exited containers were never reclaimed.
    """
    ids = set()
    for _key, cluster in _clusters.all_items():
        cid = cluster.get("_docker_id")
        if cid:
            ids.add(cid)
    return ids


container_reaper.register_live_ids("eks", _live_container_ids)
