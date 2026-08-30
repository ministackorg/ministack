"""
Amazon Aurora DSQL Emulator (control plane + data plane wiring).

REST-JSON API (no X-Amz-Target; routed on method + path). Covers cluster
lifecycle, tags, cluster policies, and CDC streams (metadata only — no change
records are delivered to the Kinesis target).

Data plane: with ``DSQL_STRICT=1`` and a Docker daemon available, each
cluster gets a real Postgres container (RDS idiom, capped by a fixed port
window off ``DSQL_BASE_PORT``, default 30 ports) fronted by an in-process wire-protocol
proxy (`ministack.core.pgproxy`) that enforces DSQL's SQL subset. Otherwise
the cluster goes ACTIVE metadata-only — nothing listens on its port.
"""

import asyncio
import copy
import hashlib
import json
import logging
import os
import secrets
import string
import threading
import time

from ministack.core import container_reaper, pgproxy
from ministack.core.concurrency import run_offloop
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    get_account_id,
    get_region,
    json_response,
)

logger = logging.getLogger("dsql")

BASE_PORT = int(os.environ.get("DSQL_BASE_PORT", "25432"))
# Real Postgres backend containers are capped by a fixed port window off
# DSQL_BASE_PORT (one port per cluster), not a separate knob: a cluster whose
# allocated port falls outside [BASE_PORT, BASE_PORT + _BACKEND_PORT_WINDOW) is
# served metadata-only.
_BACKEND_PORT_WINDOW = 30
PG_IMAGE = os.environ.get("DSQL_PG_IMAGE", "postgres:16-alpine")
DSQL_PERSIST = os.environ.get("DSQL_PERSIST", "0").lower() in ("1", "true", "yes")
# Backend containers are opt-in: default is control-plane-only (metadata stubs).
DSQL_STRICT = os.environ.get("DSQL_STRICT", "0").lower() in ("1", "true", "yes")

_clusters = AccountScopedDict()  # identifier -> cluster dict
_client_tokens = AccountScopedDict()  # clientToken -> identifier (create idempotency)
_tags = AccountScopedDict()  # cluster/stream arn -> {key: value}
_streams = AccountScopedDict()  # "{clusterId}/{streamId}" -> stream dict
_stream_tokens = AccountScopedDict()  # clientToken -> stream key (create idempotency)

_port_counter = [BASE_PORT]
_port_lock = threading.Lock()

_main_loop = None  # captured app event loop for proxy startup from threads

_ID_ALPHABET = string.ascii_lowercase + string.digits


def _next_port():
    """Allocate a unique localhost port for a cluster endpoint.

    The Postgres wire proxy binds this port per cluster.
    """
    with _port_lock:
        port = _port_counter[0]
        _port_counter[0] += 1
        return port


def _new_identifier():
    return "".join(secrets.choice(_ID_ALPHABET) for _ in range(26))


def _cluster_arn(identifier, account_id=None, region=None):
    account_id = account_id or get_account_id()
    region = region or get_region()
    return f"arn:aws:dsql:{region}:{account_id}:cluster/{identifier}"


def _identifier_from_arn(arn):
    # arn:aws:dsql:{region}:{account}:cluster/{identifier}
    if ":cluster/" in arn:
        return arn.rsplit(":cluster/", 1)[1]
    return ""


def _kms_key_error(kms_key):
    """ValidationException if a customer-managed key isn't in the KMS store.

    Accepts key id, key ARN, alias name, or alias ARN (same as KMS).
    AWS-managed aliases (``alias/aws/...``) always exist, so they pass.
    """
    if not kms_key:
        return None
    alias = kms_key.split(":alias/")[-1] if ":alias/" in kms_key else kms_key
    if alias.startswith("alias/aws/"):
        return None
    from ministack.services import kms  # lazy: only needed when a key is given

    if kms._resolve_key(kms_key) is None:
        return _validation(
            f"KMS key '{kms_key}' does not exist",
            reason="fieldValidationFailed", field="kmsEncryptionKey",
        )
    return None


def _encryption_details(kms_key, account_id, region):
    if kms_key:
        key_arn = kms_key if kms_key.startswith("arn:") else (
            f"arn:aws:kms:{region}:{account_id}:key/{kms_key}"
        )
        return {
            "encryptionType": "CUSTOMER_MANAGED_KMS_KEY",
            "kmsKeyArn": key_arn,
            "encryptionStatus": "ENABLED",
        }
    return {
        "encryptionType": "AWS_OWNED_KMS_KEY",
        "kmsKeyArn": f"arn:aws:kms:{region}:{account_id}:key/aws/dsql",
        "encryptionStatus": "ENABLED",
    }


def _error(code, message, status, **extra):
    """AWS-style JSON error with the model's structured members included."""
    data = {"__type": code, "message": message, **extra}
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    return status, {
        "Content-Type": "application/x-amz-json-1.0",
        "x-amzn-errortype": code,
    }, body


def _not_found(resource_id, resource_type="cluster"):
    return _error(
        "ResourceNotFoundException",
        f"The {resource_type} with id '{resource_id}' does not exist",
        404,
        resourceId=resource_id,
        resourceType=resource_type,
    )


def _validation(message, reason="other", field=None):
    extra = {"reason": reason}
    if field:
        extra["fieldList"] = [{"name": field, "message": message}]
    return _error("ValidationException", message, 400, **extra)


def _qp(query_params, *names):
    """Return the first value of the first matching query parameter."""
    for name in names:
        values = query_params.get(name)
        if values:
            return values[0]
    return None


def _cluster_response(cluster, include_tags=False):
    resp = {
        "identifier": cluster["identifier"],
        "arn": cluster["arn"],
        "status": cluster["status"],
        "creationTime": cluster["creationTime"],
        "deletionProtectionEnabled": cluster["deletionProtectionEnabled"],
    }
    if cluster.get("multiRegionProperties"):
        resp["multiRegionProperties"] = cluster["multiRegionProperties"]
    if cluster.get("encryptionDetails"):
        resp["encryptionDetails"] = cluster["encryptionDetails"]
    if cluster.get("endpoint"):
        resp["endpoint"] = cluster["endpoint"]
    if include_tags:
        resp["tags"] = _tags.get(cluster["arn"], {})
    return resp


# ---------------------------------------------------------------------------
# Backend container + wire proxy lifecycle (data plane)
# ---------------------------------------------------------------------------


def _docker_available():
    """Cheap pre-flight check so we don't import docker-py for nothing."""
    sock = os.environ.get("DOCKER_HOST") or "unix:///var/run/docker.sock"
    if sock.startswith("unix://"):
        return os.path.exists(sock[len("unix://"):])
    return True


def _backends_enabled():
    """Whether real Postgres backend containers should run for clusters.

    Opt-in via ``DSQL_STRICT=1``; default is control-plane-only stubs.
    """
    return DSQL_STRICT and _docker_available()


def _container_name(identifier):
    return f"ministack-dsql-{identifier}"


def _published_port(container):
    ports = container.attrs.get("NetworkSettings", {}).get("Ports", {})
    return int(ports["5432/tcp"][0]["HostPort"])


def _run_backend_container(identifier):
    """Blocking: (re)start the Postgres container and wait for readiness.

    Returns ``(backend_host, backend_port)`` for the proxy to dial. Raises on
    failure; the caller degrades to metadata-only. ``rds`` is imported lazily
    so the heavy module (and its import-time restore) only loads when a
    Docker socket actually exists.
    """
    from ministack.services import rds

    docker_client = rds._get_docker()
    if not docker_client:
        raise RuntimeError("Docker daemon not available")

    name = _container_name(identifier)
    try:
        existing = docker_client.containers.get(name)
        try:
            existing.stop(timeout=2)
        except Exception:
            pass
        try:
            existing.remove(v=False)
        except Exception as e:
            logger.warning("DSQL: failed to remove stale container %s: %s", name, e)
    except Exception:
        pass  # No existing container with that name — fine

    ms_network = rds._get_ministack_network(docker_client)
    container_kwargs = dict(
        image=PG_IMAGE,
        detach=True,
        environment={
            "POSTGRES_USER": "postgres",
            "POSTGRES_DB": "postgres",
            "POSTGRES_HOST_AUTH_METHOD": "trust",
        },
        ports={"5432/tcp": None},  # docker-assigned host port
        name=name,
        labels=container_reaper.own_labels("dsql", cluster_id=identifier),
    )
    if ms_network:
        container_kwargs["network"] = ms_network
    if DSQL_PERSIST:
        container_kwargs["volumes"] = {
            f"ministack-dsql-{identifier}-data": {
                "bind": "/var/lib/postgresql/data",
                "mode": "rw",
            },
        }
    else:
        container_kwargs["tmpfs"] = {
            "/var/lib/postgresql/data": "rw,noexec,nosuid,size=256m",
        }

    container = docker_client.containers.run(**container_kwargs)
    container.reload()

    host = port = None
    if ms_network:
        networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
        container_ip = networks.get(ms_network, {}).get("IPAddress", "")
        if container_ip:
            host, port = container_ip, 5432
    if host is None:
        host, port = "localhost", _published_port(container)

    def _container_alive():
        try:
            container.reload()
            return container.status == "running"
        except Exception:
            return False

    # TCP readiness is not enough: docker-proxy accepts connections while the
    # postgres entrypoint is still running initdb, then drops them. Poll a
    # real authenticated connection (trust auth, so no password) like RDS does.
    if not rds._wait_for_database_ready(
        host, port, "postgres", "postgres", "", "postgres", _container_alive
    ):
        raise RuntimeError(f"backend at {host}:{port} did not become ready")
    return host, port


def _remove_container(identifier):
    """Best-effort blocking removal of a cluster's backend container."""
    if not _docker_available():
        return
    try:
        from ministack.services import rds

        docker_client = rds._get_docker()
        if not docker_client:
            return
        container = docker_client.containers.get(_container_name(identifier))
        container.remove(v=True, force=True)
    except Exception as e:
        logger.debug("DSQL: container removal for %s: %s", identifier, e)


def _serving_loop():
    """The loop serving HTTP, or None outside a running server.

    ``_main_loop`` is only populated by the ``start_restored_proxies`` lifespan
    hook, which is skipped when the dsql module was not already loaded at boot —
    i.e. on every fresh server whose first dsql call is CreateCluster. The app
    captures the loop unconditionally, so fall back to that.
    """
    try:
        from ministack import app

        return app._MAIN_LOOP
    except Exception:
        return None


async def _start_backend(identifier, cluster):
    """Background task: container + proxy, then flip the cluster ACTIVE.

    Any failure degrades to metadata-only (mirroring rds' resilience): the
    cluster still goes ACTIVE, just with nothing listening on its port.
    """
    global _main_loop
    try:
        _main_loop = asyncio.get_running_loop()
    except RuntimeError:
        pass
    try:
        host, port = await asyncio.to_thread(_run_backend_container, identifier)
        await pgproxy.start_proxy(identifier, cluster["port"], host, port)
        cluster["_backend_host"] = host
        cluster["_backend_port"] = port
        cluster["status"] = "ACTIVE"
        logger.info("DSQL: cluster %s backend up at %s:%d", identifier, host, port)
    except Exception as e:
        logger.warning(
            "DSQL: backend startup failed for cluster %s: %s — metadata-only",
            identifier,
            e,
        )
        cluster["status"] = "ACTIVE"
        cluster["_has_backend"] = False


async def _teardown_backend(identifier):
    try:
        await pgproxy.stop_proxy(identifier)
    except Exception as e:
        logger.debug("DSQL: proxy stop for %s: %s", identifier, e)
    try:
        await asyncio.to_thread(_remove_container, identifier)
    except Exception as e:
        logger.warning("DSQL: backend teardown failed for %s: %s", identifier, e)


def _respawn_backend(identifier, cluster):
    """Blocking restore path (runs in a daemon thread at import time)."""
    try:
        host, port = _run_backend_container(identifier)
    except Exception as e:
        logger.warning(
            "DSQL: backend respawn failed for cluster %s: %s — metadata-only",
            identifier,
            e,
        )
        cluster["status"] = "ACTIVE"
        cluster["_has_backend"] = False
        return
    cluster["_backend_host"] = host
    cluster["_backend_port"] = port
    cluster["status"] = "ACTIVE"
    # Proxies need the app event loop; it may not exist yet at import time.
    # `start_restored_proxies` (lifespan hook) picks up anything missed here.
    loop = _main_loop
    if loop is not None and loop.is_running():
        asyncio.run_coroutine_threadsafe(
            pgproxy.start_proxy(identifier, cluster["port"], host, port), loop
        )


async def start_restored_proxies():
    """Lifespan hook: capture the running loop and start wire proxies for
    restored clusters whose backends are already up."""
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    for cluster in list(_clusters._data.values()):
        host = cluster.get("_backend_host")
        port = cluster.get("_backend_port")
        if host and port and cluster.get("status") == "ACTIVE":
            try:
                await pgproxy.start_proxy(
                    cluster["identifier"], cluster["port"], host, int(port)
                )
            except Exception as e:
                logger.warning(
                    "DSQL: proxy restore failed for %s: %s",
                    cluster["identifier"],
                    e,
                )


# ---------------------------------------------------------------------------
# Cluster lifecycle
# ---------------------------------------------------------------------------


def _create_cluster(data):
    account_id = get_account_id()
    region = get_region()

    client_token = data.get("clientToken")
    if client_token:
        existing_id = _client_tokens.get(client_token)
        if isinstance(existing_id, str) and existing_id in _clusters:
            return json_response(_cluster_response(_clusters[existing_id]))

    key_err = _kms_key_error(data.get("kmsEncryptionKey"))
    if key_err:
        return key_err

    identifier = _new_identifier()
    arn = _cluster_arn(identifier, account_id, region)
    port = _next_port()
    has_backend = _backends_enabled()
    if has_backend:
        # Cap concurrent backends to the port window off DSQL_BASE_PORT (one
        # port per cluster). Counting live backends (not the raw port counter)
        # so a deleted cluster frees capacity.
        backend_count = sum(
            1 for c in _clusters._data.values() if c.get("_has_backend")
        )
        if backend_count >= _BACKEND_PORT_WINDOW:
            logger.warning(
                "DSQL: backend port window off DSQL_BASE_PORT (%d ports) full "
                "— cluster %s is metadata-only. Widen the range to allow more.",
                _BACKEND_PORT_WINDOW,
                identifier,
            )
            has_backend = False

    cluster = {
        "identifier": identifier,
        "arn": arn,
        # With a backend the Postgres container + wire proxy spin up in the
        # background; without one the cluster is metadata-only ACTIVE.
        "status": "CREATING" if has_backend else "ACTIVE",
        "creationTime": int(time.time()),
        # AWS creates clusters with deletion protection ON by default.
        "deletionProtectionEnabled": bool(data.get("deletionProtectionEnabled", True)),
        "encryptionDetails": _encryption_details(data.get("kmsEncryptionKey"), account_id, region),
        "port": port,
        "endpoint": f"localhost:{port}",
        "_has_backend": has_backend,
    }
    if data.get("multiRegionProperties"):
        cluster["multiRegionProperties"] = data["multiRegionProperties"]
    if data.get("policy"):
        cluster["policy"] = data["policy"]
        cluster["_policy_version"] = 1

    _clusters[identifier] = cluster
    if client_token:
        _client_tokens[client_token] = identifier
    if data.get("tags"):
        _tags[arn] = dict(data["tags"])

    if has_backend:
        # This handler runs on a worker thread (dispatch is off the event loop so
        # Docker work cannot freeze the server), so there is no *running* loop
        # here to create_task on. Schedule onto the serving loop instead — the
        # same pattern _start_backend's respawn path already uses. Falling back
        # to get_running_loop covers direct/in-process callers that do have one.
        loop = _main_loop or _serving_loop()
        try:
            if loop is not None and loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    _start_backend(identifier, cluster), loop)
            else:
                asyncio.get_running_loop().create_task(
                    _start_backend(identifier, cluster))
        except RuntimeError:
            # No event loop at all (module-level call) — metadata-only.
            cluster["status"] = "ACTIVE"
            cluster["_has_backend"] = False

    return json_response(_cluster_response(cluster))


def _get_cluster(identifier):
    cluster = _clusters.get(identifier)
    if not cluster:
        return _not_found(identifier)
    return json_response(_cluster_response(cluster, include_tags=True))


def _lifecycle_response(cluster):
    """UpdateCluster/DeleteCluster return only these four members per the model."""
    return json_response({
        "identifier": cluster["identifier"],
        "arn": cluster["arn"],
        "status": cluster["status"],
        "creationTime": cluster["creationTime"],
    })


def _update_cluster(identifier, data):
    cluster = _clusters.get(identifier)
    if not cluster:
        return _not_found(identifier)

    if (
        "deletionProtectionEnabled" not in data
        and not data.get("kmsEncryptionKey")
        and not data.get("multiRegionProperties")
    ):
        return _validation(
            "UpdateCluster requires at least one of deletionProtectionEnabled, "
            "kmsEncryptionKey, multiRegionProperties",
            reason="fieldValidationFailed",
        )

    if "deletionProtectionEnabled" in data:
        cluster["deletionProtectionEnabled"] = bool(data["deletionProtectionEnabled"])
    if data.get("kmsEncryptionKey"):
        key_err = _kms_key_error(data["kmsEncryptionKey"])
        if key_err:
            return key_err
        cluster["encryptionDetails"] = _encryption_details(
            data["kmsEncryptionKey"], get_account_id(), get_region()
        )
    if data.get("multiRegionProperties"):
        cluster["multiRegionProperties"] = data["multiRegionProperties"]
    # Don't clobber CREATING while the backend container is still spinning up.
    if cluster["status"] != "CREATING":
        cluster["status"] = "ACTIVE"

    return _lifecycle_response(cluster)


def _delete_cluster(identifier, query_params):
    client_token = _qp(query_params, "client-token", "clientToken")
    if client_token:
        cached = _client_tokens.get(client_token)
        # Idempotent retry after a successful delete returns the cached
        # response (the cluster dict is already gone at that point).
        if isinstance(cached, dict) and cached.get("_deleted"):
            return json_response({
                k: v for k, v in cached.items() if k != "_deleted"
            })

    cluster = _clusters.get(identifier)
    if not cluster:
        return _not_found(identifier)

    if client_token:
        seen_arn = _client_tokens.get(client_token)
        # An existing (string) entry is a create token bound to a different
        # cluster. If the seen token is for a different cluster, reject.
        if isinstance(seen_arn, str) and seen_arn != cluster["arn"]:
            return _not_found(identifier)

    if cluster.get("deletionProtectionEnabled"):
        return _validation(
            "Cannot delete cluster: deletion protection is enabled. "
            "Disable deletion protection before deleting the cluster.",
            reason="deletionProtectionEnabled",
        )

    # Stop the wire proxy and remove the backend container (best effort).
    try:
        asyncio.get_running_loop().create_task(_teardown_backend(identifier))
    except RuntimeError:
        _remove_container(identifier)
    cluster["status"] = "DELETING"
    resp = _lifecycle_response(cluster)
    _tags.pop(cluster["arn"], None)
    for key, stream in list(_streams.items()):
        if stream["clusterIdentifier"] == identifier:
            _tags.pop(stream["arn"], None)
            del _streams[key]
    del _clusters[identifier]
    if client_token:
        # Cache the entire response so an idempotent retry succeeds.
        body = json.loads(resp[2].decode("utf-8"))
        body["_deleted"] = True
        _client_tokens[client_token] = body
    return resp


def _list_clusters(query_params):
    raw_max = _qp(query_params, "max-results", "maxResults")
    next_token = _qp(query_params, "next-token", "nextToken")

    try:
        max_results = int(raw_max) if raw_max else 100
    except (TypeError, ValueError):
        return _validation(
            f"Invalid maxResults: {raw_max}",
            reason="fieldValidationFailed", field="maxResults",
        )
    if max_results < 1 or max_results > 100:
        return _validation(
            "maxResults must be between 1 and 100",
            reason="fieldValidationFailed", field="maxResults",
        )

    start = 0
    if next_token:
        try:
            start = int(next_token)
            if start < 0:
                raise ValueError
        except (TypeError, ValueError):
            return _validation(
                f"Invalid nextToken: {next_token}",
                reason="fieldValidationFailed", field="nextToken",
            )

    summaries = [
        {"identifier": c["identifier"], "arn": c["arn"]}
        for c in _clusters.values()
    ]
    page = summaries[start : start + max_results]

    resp = {"clusters": page}
    if start + max_results < len(summaries):
        resp["nextToken"] = str(start + max_results)
    return json_response(resp)


# ---------------------------------------------------------------------------
# Cluster policy
# ---------------------------------------------------------------------------


def _policy_not_found(identifier):
    return _error(
        "ResourceNotFoundException",
        f"No policy found for cluster '{identifier}'",
        404,
        resourceId=identifier,
        resourceType="policy",
    )


def _check_expected_version(cluster, data=None, query_params=None):
    """ConflictException if expectedPolicyVersion is given and stale.

    ``expectedPolicyVersion`` arrives in the JSON body for
    PutClusterPolicy, or as a URI query param for DeleteClusterPolicy.
    """
    expected = None
    if data:
        expected = data.get("expectedPolicyVersion")
    if expected is None and query_params is not None:
        expected = _qp(query_params, "expected-policy-version", "expectedPolicyVersion")
    if expected is not None and expected != str(cluster.get("_policy_version", 0)):
        return _error(
            "ConflictException",
            f"Policy version mismatch: expected {expected}, current is "
            f"{cluster.get('_policy_version', 0)}",
            409,
            resourceId=cluster["identifier"],
            resourceType="cluster",
        )
    return None


def _put_cluster_policy(identifier, data):
    cluster = _clusters.get(identifier)
    if not cluster:
        return _not_found(identifier)
    policy = data.get("policy")
    if not isinstance(policy, str) or not policy:
        return _validation(
            "policy is required and must be a non-empty string",
            reason="fieldValidationFailed", field="policy",
        )
    conflict = _check_expected_version(cluster, data)
    if conflict:
        return conflict
    version = cluster.get("_policy_version", 0) + 1
    cluster["policy"] = policy
    cluster["_policy_version"] = version
    return json_response({"policyVersion": str(version)})


def _get_cluster_policy(identifier):
    cluster = _clusters.get(identifier)
    if not cluster:
        return _not_found(identifier)
    if "policy" not in cluster:
        return _policy_not_found(identifier)
    return json_response({
        "policy": cluster["policy"],
        "policyVersion": str(cluster["_policy_version"]),
    })


def _delete_cluster_policy(identifier, query_params):
    cluster = _clusters.get(identifier)
    if not cluster:
        return _not_found(identifier)
    if "policy" not in cluster:
        return _policy_not_found(identifier)
    conflict = _check_expected_version(cluster, query_params=query_params)
    if conflict:
        return conflict
    deleted_version = str(cluster["_policy_version"])
    del cluster["policy"]
    cluster["_policy_version"] += 1
    return json_response({"policyVersion": deleted_version})


# ---------------------------------------------------------------------------
# CDC streams
# ---------------------------------------------------------------------------

# A cluster allows 5 CDC streams (AWS cluster quotas). Only the Kinesis target,
# UNORDERED ordering and the JSON format exist in the API today.
_MAX_STREAMS_PER_CLUSTER = 5
_STREAM_ORDERINGS = ("UNORDERED",)
_STREAM_FORMATS = ("JSON",)


def _stream_arn(cluster_identifier, stream_identifier, account_id=None, region=None):
    cluster = _cluster_arn(cluster_identifier, account_id, region)
    return f"{cluster}/stream/{stream_identifier}"


def _stream_key(cluster_identifier, stream_identifier):
    return f"{cluster_identifier}/{stream_identifier}"


def _stream_not_found(cluster_identifier, stream_identifier):
    return _not_found(
        _stream_key(cluster_identifier, stream_identifier), resource_type="stream"
    )


def _stream_summary(stream):
    """The five members ListStreams and DeleteStream report."""
    return {
        "clusterIdentifier": stream["clusterIdentifier"],
        "streamIdentifier": stream["streamIdentifier"],
        "arn": stream["arn"],
        "status": stream["status"],
        "creationTime": stream["creationTime"],
    }


def _stream_response(stream, include_target=False):
    resp = _stream_summary(stream)
    resp["ordering"] = stream["ordering"]
    resp["format"] = stream["format"]
    if include_target:
        resp["targetDefinition"] = stream["targetDefinition"]
        if stream.get("statusReason"):
            resp["statusReason"] = stream["statusReason"]
        resp["tags"] = _tags.get(stream["arn"], {})
    return resp


def _check_target_definition(target):
    """ValidationException for a target the API's union shape can't hold."""
    if not isinstance(target, dict) or len(target) != 1 or "kinesis" not in target:
        return _validation(
            "targetDefinition must set exactly one member: kinesis",
            reason="fieldValidationFailed", field="targetDefinition",
        )
    kinesis_target = target["kinesis"]
    if not isinstance(kinesis_target, dict):
        return _validation(
            "targetDefinition.kinesis must be a structure",
            reason="fieldValidationFailed", field="targetDefinition.kinesis",
        )
    for member in ("streamArn", "roleArn"):
        if not kinesis_target.get(member):
            return _validation(
                f"targetDefinition.kinesis.{member} is required",
                reason="fieldValidationFailed",
                field=f"targetDefinition.kinesis.{member}",
            )
    return None


def _target_failure(target):
    """The failure real DSQL reports through statusReason, or None.

    Delivery is not emulated, so the target is judged once at create time:
    a role that cannot be assumed (AUTH=true only, as everywhere else) and a
    Kinesis stream that isn't there are the two cases MiniStack can see.
    """
    kinesis_target = target["kinesis"]
    from ministack.core.iam_evaluator import validate_role_arn

    if validate_role_arn(kinesis_target["roleArn"]):
        return "ROLE_ACCESS_DENIED"
    from ministack.services import kinesis

    if kinesis._resolve_stream_by_arn(kinesis_target["streamArn"]) is None:
        return "KINESIS_STREAM_NOT_FOUND"
    return None


def _create_stream(cluster_identifier, data):
    cluster = _clusters.get(cluster_identifier)
    if not cluster:
        return _not_found(cluster_identifier)

    client_token = data.get("clientToken")
    if client_token:
        existing_key = _stream_tokens.get(client_token)
        if isinstance(existing_key, str) and existing_key in _streams:
            return json_response(_stream_response(_streams[existing_key]))

    err = _check_target_definition(data.get("targetDefinition"))
    if err:
        return err
    ordering = data.get("ordering")
    if ordering not in _STREAM_ORDERINGS:
        return _validation(
            f"ordering must be one of: {', '.join(_STREAM_ORDERINGS)}",
            reason="fieldValidationFailed", field="ordering",
        )
    fmt = data.get("format")
    if fmt not in _STREAM_FORMATS:
        return _validation(
            f"format must be one of: {', '.join(_STREAM_FORMATS)}",
            reason="fieldValidationFailed", field="format",
        )

    existing = [
        st for st in _streams.values()
        if st["clusterIdentifier"] == cluster_identifier
    ]
    if len(existing) >= _MAX_STREAMS_PER_CLUSTER:
        return _error(
            "ServiceQuotaExceededException", "You have reached the stream limit.",
            402, resourceId=cluster_identifier, resourceType="stream",
            serviceCode="dsql", quotaCode="StreamsPerCluster",
        )

    identifier = _new_identifier()
    arn = _stream_arn(cluster_identifier, identifier)
    failure = _target_failure(data["targetDefinition"])
    stream = {
        "clusterIdentifier": cluster_identifier,
        "streamIdentifier": identifier,
        "arn": arn,
        # Nothing is delivered, so a stream that has a target to deliver to
        # is ACTIVE straight away rather than going through CREATING.
        "status": "FAILED" if failure else "ACTIVE",
        "creationTime": int(time.time()),
        "ordering": ordering,
        "format": fmt,
        "targetDefinition": copy.deepcopy(data["targetDefinition"]),
    }
    if failure:
        stream["statusReason"] = {"error": failure, "updatedAt": stream["creationTime"]}

    _streams[_stream_key(cluster_identifier, identifier)] = stream
    if client_token:
        _stream_tokens[client_token] = _stream_key(cluster_identifier, identifier)
    if data.get("tags"):
        _tags[arn] = dict(data["tags"])

    return json_response(_stream_response(stream))


def _get_stream(cluster_identifier, stream_identifier):
    if cluster_identifier not in _clusters:
        return _not_found(cluster_identifier)
    stream = _streams.get(_stream_key(cluster_identifier, stream_identifier))
    if not stream:
        return _stream_not_found(cluster_identifier, stream_identifier)
    return json_response(_stream_response(stream, include_target=True))


def _list_streams(cluster_identifier, query_params):
    if cluster_identifier not in _clusters:
        return _not_found(cluster_identifier)

    raw_max = _qp(query_params, "max-results", "maxResults")
    next_token = _qp(query_params, "next-token", "nextToken")
    try:
        max_results = int(raw_max) if raw_max else 100
    except (TypeError, ValueError):
        return _validation(
            f"Invalid maxResults: {raw_max}",
            reason="fieldValidationFailed", field="maxResults",
        )
    if max_results < 1 or max_results > 100:
        return _validation(
            "maxResults must be between 1 and 100",
            reason="fieldValidationFailed", field="maxResults",
        )
    start = 0
    if next_token:
        try:
            start = int(next_token)
            if start < 0:
                raise ValueError
        except (TypeError, ValueError):
            return _validation(
                f"Invalid nextToken: {next_token}",
                reason="fieldValidationFailed", field="nextToken",
            )

    summaries = [
        _stream_summary(st) for st in _streams.values()
        if st["clusterIdentifier"] == cluster_identifier
    ]
    resp = {"streams": summaries[start : start + max_results]}
    if start + max_results < len(summaries):
        resp["nextToken"] = str(start + max_results)
    return json_response(resp)


def _delete_stream(cluster_identifier, stream_identifier, query_params):
    if cluster_identifier not in _clusters:
        return _not_found(cluster_identifier)
    key = _stream_key(cluster_identifier, stream_identifier)
    stream = _streams.get(key)
    if not stream:
        return _stream_not_found(cluster_identifier, stream_identifier)
    stream["status"] = "DELETING"
    resp = json_response(_stream_summary(stream))
    _tags.pop(stream["arn"], None)
    del _streams[key]
    return resp


def _get_vpc_endpoint_service_name(identifier):
    cluster = _clusters.get(identifier)
    if not cluster:
        return _not_found(identifier)
    # The service name's suffix is per-cluster and stable, as on AWS.
    suffix = hashlib.sha256(identifier.encode()).hexdigest()[:6]
    return json_response({
        "serviceName": f"com.amazonaws.{get_region()}.dsql-{suffix}",
    })


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def _tags_for_arn(arn):
    """Return tags for a known cluster or stream ARN, or a 404 response."""
    identifier = _identifier_from_arn(arn)
    if "/stream/" in identifier:
        cluster_identifier, _, stream_identifier = identifier.partition("/stream/")
        if _stream_key(cluster_identifier, stream_identifier) not in _streams:
            return None, _not_found(arn, resource_type="stream")
        return _tags.get(arn, {}), None
    if not identifier or identifier not in _clusters:
        return None, _not_found(arn, resource_type="cluster")
    return _tags.get(arn, {}), None


def _tag_resource(arn, data):
    _, err = _tags_for_arn(arn)
    if err:
        return err
    new_tags = data.get("tags")
    if not isinstance(new_tags, dict):
        return _validation(
            "tags must be a map of key/value pairs",
            reason="fieldValidationFailed", field="tags",
        )
    existing = _tags.get(arn, {})
    existing.update(new_tags)
    _tags[arn] = existing
    return json_response({})


def _untag_resource(arn, query_params):
    _, err = _tags_for_arn(arn)
    if err:
        return err
    tag_keys = query_params.get("tagKeys", [])
    existing = _tags.get(arn, {})
    for key in tag_keys:
        existing.pop(key, None)
    _tags[arn] = existing
    return json_response({})


def _list_tags_for_resource(arn):
    tags, err = _tags_for_arn(arn)
    if err:
        return err
    return json_response({"tags": tags})


# ---------------------------------------------------------------------------
# Request routing
# ---------------------------------------------------------------------------


def _handle_request_sync(method, path, headers, body, query_params):
    try:
        data = json.loads(body) if body else {}
        if not isinstance(data, dict):
            return _validation("Request body must be a JSON object", reason="cannotParse")
    except json.JSONDecodeError:
        return _validation("Invalid JSON", reason="cannotParse")

    try:
        # Tag routes embed the ARN in the path: /tags/{resourceArn}
        if path.startswith("/tags/"):
            arn = path[len("/tags/") :]
            if method == "POST":
                return _tag_resource(arn, data)
            if method == "DELETE":
                return _untag_resource(arn, query_params)
            if method == "GET":
                return _list_tags_for_resource(arn)
            return _validation(f"Unknown method for /tags: {method}", reason="unknownOperation")

        if path.startswith("/stream/"):
            parts = path[len("/stream/") :].split("/")
            if len(parts) == 1 and parts[0]:
                if method == "POST":
                    return _create_stream(parts[0], data)
                if method == "GET":
                    return _list_streams(parts[0], query_params)
                return _validation(
                    f"Unknown method for /stream/{{id}}: {method}",
                    reason="unknownOperation",
                )
            if len(parts) == 2 and all(parts):
                if method == "GET":
                    return _get_stream(parts[0], parts[1])
                if method == "DELETE":
                    return _delete_stream(parts[0], parts[1], query_params)
                return _validation(
                    f"Unknown method for /stream/{{id}}/{{streamId}}: {method}",
                    reason="unknownOperation",
                )
            return _validation(f"Unknown path: {method} {path}", reason="unknownOperation")

        if path.startswith("/clusters/") and path.endswith(
            "/vpc-endpoint-service-name"
        ):
            identifier = path[len("/clusters/") : -len("/vpc-endpoint-service-name")]
            if method == "GET":
                return _get_vpc_endpoint_service_name(identifier)
            return _validation(
                f"Unknown method for {path}: {method}", reason="unknownOperation"
            )

        if path == "/cluster":
            if method == "POST":
                return _create_cluster(data)
            if method == "GET":
                return _list_clusters(query_params)
            return _validation(f"Unknown method for /cluster: {method}", reason="unknownOperation")

        if path.startswith("/cluster/"):
            identifier = path[len("/cluster/") :]
            if identifier.endswith("/policy"):
                identifier = identifier[: -len("/policy")]
                if method == "POST":
                    return _put_cluster_policy(identifier, data)
                if method == "GET":
                    return _get_cluster_policy(identifier)
                if method == "DELETE":
                    return _delete_cluster_policy(identifier, query_params)
                return _validation(
                    f"Unknown method for /cluster/{{id}}/policy: {method}",
                    reason="unknownOperation",
                )
            if "/" in identifier:
                return _validation(f"Unknown path: {method} {path}", reason="unknownOperation")
            if method == "GET":
                return _get_cluster(identifier)
            if method == "POST":
                return _update_cluster(identifier, data)
            if method == "DELETE":
                return _delete_cluster(identifier, query_params)
            return _validation(f"Unknown method for /cluster/{{id}}: {method}", reason="unknownOperation")

        return _validation(f"Unknown path: {method} {path}", reason="unknownOperation")
    except Exception as e:
        logger.exception("Error handling %s %s: %s", method, path, e)
        return _error("InternalServerException", str(e), 500)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def get_state():
    clusters = copy.deepcopy(dict(_clusters._data))
    # Backend addresses are runtime-only (recomputed on restore).
    for cluster in clusters.values():
        if isinstance(cluster, dict):
            cluster.pop("_backend_host", None)
            cluster.pop("_backend_port", None)
    return {
        "clusters": clusters,
        "client_tokens": copy.deepcopy(dict(_client_tokens._data)),
        "tags": copy.deepcopy(dict(_tags._data)),
        "streams": copy.deepcopy(dict(_streams._data)),
        "stream_tokens": copy.deepcopy(dict(_stream_tokens._data)),
        "port_counter": _port_counter[0],
    }


def restore_state(data):
    if not data:
        return
    clusters = data.get("clusters", {})
    if clusters:
        _clusters._data.update(clusters)
    tokens = data.get("client_tokens", {})
    if tokens:
        _client_tokens._data.update(tokens)
    tags = data.get("tags", {})
    if tags:
        _tags._data.update(tags)
    streams = data.get("streams", {})
    if streams:
        _streams._data.update(streams)
    stream_tokens = data.get("stream_tokens", {})
    if stream_tokens:
        _stream_tokens._data.update(stream_tokens)
    saved_counter = data.get("port_counter")
    if isinstance(saved_counter, int) and saved_counter > _port_counter[0]:
        _port_counter[0] = saved_counter
    # Never reissue a port still owned by a restored cluster.
    for cluster in _clusters._data.values():
        port = cluster.get("port")
        if isinstance(port, int) and port >= _port_counter[0]:
            _port_counter[0] = port + 1
    # Capture the loop when the import fires during request handling, so
    # respawn threads can schedule proxy startup on it.
    global _main_loop
    try:
        _main_loop = asyncio.get_running_loop()
    except RuntimeError:
        pass
    # Respawn backend containers for restored clusters (blocking Docker work
    # in daemon threads, mirroring rds restore). Proxies start via
    # `_respawn_backend` once the loop is known, or via the lifespan hook
    # `start_restored_proxies`. The cap is deliberately not re-enforced here:
    # clusters that had a backend get it back (best effort), so a restart can
    # transiently exceed the backend port window.
    for cluster in _clusters._data.values():
        if not isinstance(cluster, dict) or "identifier" not in cluster:
            continue
        # Clusters persisted as stubs stay stubs. State saved before
        # `_has_backend` existed defaults to respawning, matching the old
        # always-container behavior.
        if not _backends_enabled() or not cluster.get("_has_backend", True):
            cluster["status"] = "ACTIVE"
            cluster["_has_backend"] = False
            continue
        cluster["status"] = "CREATING"
        threading.Thread(
            target=_respawn_backend,
            args=(cluster["identifier"], cluster),
            daemon=True,
        ).start()


def reset():
    # `/_ministack/reset` runs resets via asyncio.to_thread, so there is no
    # running loop here — schedule proxy shutdown on the captured app loop.
    loop = _main_loop
    if loop is not None and loop.is_running():
        asyncio.run_coroutine_threadsafe(pgproxy.stop_all_proxies(), loop)
    pgproxy.clear_jobs()
    if _docker_available():
        try:
            from ministack.services import rds

            docker_client = rds._get_docker()
            if docker_client:
                for c in docker_client.containers.list(
                    all=True, filters={"label": "ministack=dsql"}
                ):
                    try:
                        c.stop(timeout=2)
                        c.remove(v=True)
                    except Exception as e:
                        logger.warning("reset: failed to remove container %s: %s", c.name, e)
        except Exception as e:
            logger.warning("reset: DSQL container sweep failed: %s", e)
    _clusters.clear()
    _client_tokens.clear()
    _tags.clear()
    _streams.clear()
    _stream_tokens.clear()
    _port_counter[0] = BASE_PORT


try:
    _restored = load_state("dsql")
    if _restored:
        restore_state(_restored)
except Exception:
    logging.getLogger(__name__).exception("Failed to restore persisted state; continuing with fresh store")


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
