"""OpenSearch Serverless (AOSS) tests.

Control plane: collections, encryption/network security policies, data access
policies, tags. Data plane: the collection endpoint is served through the
gateway and proxied to a real OpenSearch container when the server runs with
``OPENSEARCH_DATAPLANE=1`` (``data_plane`` lane).
"""

import base64
import hashlib
import http.client
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
import pytest
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.exceptions import ClientError
from conftest import ENDPOINT, GATEWAY_PORT

from ministack.services import opensearchserverless as aoss_module

REGION = "us-east-1"


def _uid():
    return uuid.uuid4().hex[:8]


def _enc_policy(*resources):
    return json.dumps({
        "Rules": [{"ResourceType": "collection", "Resource": [f"collection/{r}" for r in resources]}],
        "AWSOwnedKey": True,
    })


def _net_policy(name):
    return json.dumps([{
        "Rules": [{"ResourceType": "collection", "Resource": [f"collection/{name}"]}],
        "AllowFromPublic": True,
    }])


def _data_policy(name, principal="arn:aws:iam::000000000000:role/reader"):
    return json.dumps([{
        "Rules": [
            {"ResourceType": "collection", "Resource": [f"collection/{name}"], "Permission": ["aoss:*"]},
            {"ResourceType": "index", "Resource": [f"index/{name}/*"],
             "Permission": ["aoss:DescribeIndex", "aoss:ReadDocument"]},
        ],
        "Principal": [principal],
    }])


def _wait_status(aoss, cid, timeout=240):
    deadline = time.time() + timeout
    while True:
        detail = aoss.batch_get_collection(ids=[cid])["collectionDetails"][0]
        if detail["status"] != "CREATING" or time.time() > deadline:
            return detail
        time.sleep(1)


@pytest.fixture
def collection(aoss):
    """A collection with its encryption policy; deleted afterwards."""
    name = f"c-{_uid()}"
    aoss.create_security_policy(name=f"{name}-enc", type="encryption", policy=_enc_policy(name))
    created = aoss.create_collection(name=name, type="SEARCH")["createCollectionDetail"]
    yield _wait_status(aoss, created["id"])
    try:
        aoss.delete_collection(id=created["id"])
    except ClientError:
        pass
    aoss.delete_security_policy(name=f"{name}-enc", type="encryption")


# ---------------------------------------------------------------------------
# Security policies
# ---------------------------------------------------------------------------

def test_aoss_security_policy_lifecycle(aoss):
    name = f"p-{_uid()}"
    created = aoss.create_security_policy(
        name=name, type="encryption", policy=_enc_policy(name), description="enc"
    )["securityPolicyDetail"]
    try:
        assert created["type"] == "encryption"
        assert created["policy"] == json.loads(_enc_policy(name))
        # AWS docs example: policyVersion is base64("<lastModifiedDate>_<n>").
        assert base64.b64decode(created["policyVersion"]).decode() == f"{created['lastModifiedDate']}_1"

        got = aoss.get_security_policy(name=name, type="encryption")["securityPolicyDetail"]
        assert got == created

        with pytest.raises(ClientError) as exc:
            aoss.update_security_policy(
                name=name, type="encryption", policyVersion="MTY2MzY5MzIxNzgyNl8x",
                policy=_enc_policy(name, f"{name}x"),
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

        time.sleep(0.01)
        updated = aoss.update_security_policy(
            name=name, type="encryption", policyVersion=created["policyVersion"],
            policy=_enc_policy(name, f"{name}x"),
        )["securityPolicyDetail"]
        assert updated["policyVersion"] != created["policyVersion"]
        assert updated["policy"]["Rules"][0]["Resource"] == [f"collection/{name}", f"collection/{name}x"]

        summaries = aoss.list_security_policies(type="encryption")["securityPolicySummaries"]
        summary = next(s for s in summaries if s["name"] == name)
        assert "policy" not in summary
        assert summary["policyVersion"] == updated["policyVersion"]
        assert not any(s["name"] == name for s in aoss.list_security_policies(type="network")["securityPolicySummaries"])
    finally:
        aoss.delete_security_policy(name=name, type="encryption")
    with pytest.raises(ClientError) as exc:
        aoss.get_security_policy(name=name, type="encryption")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_aoss_security_policy_duplicate_conflict(aoss):
    name = f"p-{_uid()}"
    aoss.create_security_policy(name=name, type="network", policy=_net_policy(name))
    try:
        with pytest.raises(ClientError) as exc:
            aoss.create_security_policy(name=name, type="network", policy=_net_policy(name))
        assert exc.value.response["Error"]["Code"] == "ConflictException"
        # Names are per type: an encryption policy may reuse it.
        aoss.create_security_policy(name=name, type="encryption", policy=_enc_policy(f"{name}-other"))
        aoss.delete_security_policy(name=name, type="encryption")
    finally:
        aoss.delete_security_policy(name=name, type="network")


@pytest.mark.parametrize("policy", [
    "not json",
    json.dumps({"Rules": [], "AWSOwnedKey": True}),
    json.dumps({"Rules": [{"ResourceType": "index", "Resource": ["index/x/*"]}], "AWSOwnedKey": True}),
    json.dumps({"Rules": [{"ResourceType": "collection", "Resource": ["collection/x"]}]}),
])
def test_aoss_encryption_policy_validation(aoss, policy):
    with pytest.raises(ClientError) as exc:
        aoss.create_security_policy(name=f"p-{_uid()}", type="encryption", policy=policy)
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_aoss_encryption_policy_rejects_resource_in_another_policy(aoss):
    name = f"p-{_uid()}"
    aoss.create_security_policy(name=name, type="encryption", policy=_enc_policy(name))
    try:
        with pytest.raises(ClientError) as exc:
            aoss.create_security_policy(name=f"{name}-2", type="encryption", policy=_enc_policy(name))
        assert exc.value.response["Error"]["Code"] == "ValidationException"
    finally:
        aoss.delete_security_policy(name=name, type="encryption")


# ---------------------------------------------------------------------------
# Access policies
# ---------------------------------------------------------------------------

def test_aoss_access_policy_lifecycle(aoss):
    name = f"a-{_uid()}"
    created = aoss.create_access_policy(name=name, type="data", policy=_data_policy(name))["accessPolicyDetail"]
    try:
        assert created["policy"] == json.loads(_data_policy(name))
        got = aoss.get_access_policy(name=name, type="data")["accessPolicyDetail"]
        assert got["policyVersion"] == created["policyVersion"]

        time.sleep(0.01)
        updated = aoss.update_access_policy(
            name=name, type="data", policyVersion=created["policyVersion"],
            policy=_data_policy(name, "arn:aws:iam::000000000000:role/writer"),
        )["accessPolicyDetail"]
        assert updated["policy"][0]["Principal"] == ["arn:aws:iam::000000000000:role/writer"]
        # The superseded version can no longer update.
        with pytest.raises(ClientError) as exc:
            aoss.update_access_policy(
                name=name, type="data", policyVersion=created["policyVersion"], description="x",
            )
        assert exc.value.response["Error"]["Code"] == "ValidationException"

        listed = aoss.list_access_policies(type="data", resource=[f"collection/{name}"])["accessPolicySummaries"]
        assert [p["name"] for p in listed] == [name]
        assert aoss.list_access_policies(type="data", resource=["collection/nothing-here"])["accessPolicySummaries"] == []

        stats = aoss.get_policies_stats()
        assert stats["AccessPolicyStats"]["DataPolicyCount"] >= 1
    finally:
        aoss.delete_access_policy(name=name, type="data")
    with pytest.raises(ClientError) as exc:
        aoss.delete_access_policy(name=name, type="data")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


@pytest.mark.parametrize("statement", [
    {"Rules": [{"ResourceType": "index", "Resource": ["index/x/*"], "Permission": ["aoss:ReadDocument"]}],
     "Principal": []},
    {"Rules": [{"ResourceType": "index", "Resource": ["index/x/*"], "Permission": ["aoss:ReadDocument"]}],
     "Principal": ["arn:aws:iam::000000000000:role/*"]},
    {"Rules": [{"ResourceType": "index", "Resource": ["index/x/*"], "Permission": ["aoss:CreateCollectionItems"]}],
     "Principal": ["arn:aws:iam::000000000000:role/r"]},
])
def test_aoss_data_policy_validation(aoss, statement):
    with pytest.raises(ClientError) as exc:
        aoss.create_access_policy(name=f"a-{_uid()}", type="data", policy=json.dumps([statement]))
    assert exc.value.response["Error"]["Code"] == "ValidationException"


# ---------------------------------------------------------------------------
# Collections
# ---------------------------------------------------------------------------

def test_aoss_collection_requires_matching_encryption_policy(aoss):
    name = f"c-{_uid()}"
    with pytest.raises(ClientError) as exc:
        aoss.create_collection(name=name, type="SEARCH")
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert "No matching security policy of encryption type" in exc.value.response["Error"]["Message"]


def test_aoss_collection_prefix_encryption_policy_matches(aoss):
    prefix = f"pre{_uid()}"
    aoss.create_security_policy(name=f"{prefix}-enc", type="encryption", policy=_enc_policy(f"{prefix}*"))
    try:
        created = aoss.create_collection(name=f"{prefix}-logs", type="TIMESERIES")["createCollectionDetail"]
        assert created["kmsKeyArn"] == "auto"
        _wait_status(aoss, created["id"])
        aoss.delete_collection(id=created["id"])
    finally:
        aoss.delete_security_policy(name=f"{prefix}-enc", type="encryption")


def test_aoss_collection_lifecycle(aoss, collection):
    cid, name = collection["id"], collection["name"]
    assert collection["status"] == "ACTIVE"
    assert collection["arn"] == f"arn:aws:aoss:{REGION}:{collection['arn'].split(':')[4]}:collection/{cid}"
    assert len(cid) == 20 and cid.isalnum() and cid.islower()
    assert collection["collectionEndpoint"].endswith(f"://{cid}.{REGION}.aoss.localhost:{GATEWAY_PORT}")
    assert collection["dashboardEndpoint"] == f"{collection['collectionEndpoint']}/_dashboards"
    assert collection["kmsKeyArn"] == "auto"

    by_name = aoss.batch_get_collection(names=[name, "missing-coll"])
    assert [d["id"] for d in by_name["collectionDetails"]] == [cid]
    assert by_name["collectionErrorDetails"] == [
        {"name": "missing-coll", "errorCode": "NOT_FOUND", "errorMessage": "The specified Collection is not found."}
    ]

    summaries = aoss.list_collections(collectionFilters={"name": name})["collectionSummaries"]
    assert summaries == [{"id": cid, "name": name, "status": "ACTIVE", "arn": collection["arn"], "kmsKeyArn": "auto"}]

    updated = aoss.update_collection(id=cid, description="new description")["updateCollectionDetail"]
    assert updated["description"] == "new description"

    with pytest.raises(ClientError) as exc:
        aoss.create_collection(name=name, type="SEARCH")
    assert exc.value.response["Error"]["Code"] == "ConflictException"

    deleted = aoss.delete_collection(id=cid)["deleteCollectionDetail"]
    assert deleted["status"] == "DELETING"
    assert aoss.batch_get_collection(ids=[cid])["collectionDetails"] == []
    with pytest.raises(ClientError) as exc:
        aoss.delete_collection(id=cid)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_aoss_collection_deletion_protection(aoss, collection):
    aoss.update_collection(id=collection["id"], deletionProtection="ENABLED")
    with pytest.raises(ClientError) as exc:
        aoss.delete_collection(id=collection["id"])
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    aoss.update_collection(id=collection["id"], deletionProtection="DISABLED")


def test_aoss_tags(aoss, collection):
    arn = collection["arn"]
    aoss.tag_resource(resourceArn=arn, tags=[{"key": "a", "value": "1"}, {"key": "b", "value": "2"}])
    aoss.tag_resource(resourceArn=arn, tags=[{"key": "a", "value": "3"}])
    aoss.untag_resource(resourceArn=arn, tagKeys=["b"])
    assert aoss.list_tags_for_resource(resourceArn=arn)["tags"] == [{"key": "a", "value": "3"}]
    with pytest.raises(ClientError) as exc:
        aoss.list_tags_for_resource(resourceArn=arn.replace(collection["id"], "zzzzzzzzzzzzzzzzzzzz"))
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# PERSIST_STATE volume persistence
# ---------------------------------------------------------------------------

def _fake_docker_with_volumes():
    """Minimal docker double recording `containers.run()` kwargs and `volumes.get(name).remove()` calls."""
    runs = []
    containers = {}
    removed_volumes = []

    class FakeContainer:
        def __init__(self, name):
            self.id = f"cid-{name}"
            self.name = name
            self.attrs = {"NetworkSettings": {"Networks": {}, "Ports": {"9200/tcp": [{"HostPort": "19200"}]}}}

        def reload(self):
            pass

        def stop(self, timeout=None):
            pass

        def remove(self, force=False, v=False):
            pass

    class FakeContainers:
        def run(self, **kwargs):
            runs.append(kwargs)
            container = FakeContainer(kwargs["name"])
            containers[container.id] = container
            containers[container.name] = container
            return container

        def get(self, identifier):
            return containers[identifier]

        def list(self, all=False, filters=None):
            return []

    class FakeVolume:
        def __init__(self, name):
            self.name = name

        def remove(self):
            removed_volumes.append(self.name)

    class FakeVolumes:
        def get(self, name):
            return FakeVolume(name)

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()
            self.volumes = FakeVolumes()

    return FakeDocker(), runs, removed_volumes


def test_aoss_persist_state_volume_lifecycle(monkeypatch):
    """PERSIST_STATE=1: the collection engine's data dir binds to a named Docker volume,
    recorded on the collection, reused by a warm-boot re-provision (`load_persisted_state`),
    and removed by DeleteCollection."""
    from ministack.core import persistence
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import opensearchserverless as m

    fake_docker, runs, removed_volumes = _fake_docker_with_volumes()

    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    monkeypatch.setattr(m, "_get_docker", lambda: fake_docker)
    monkeypatch.setattr(m, "_dataplane_enabled", lambda: True)
    monkeypatch.setattr(m, "_wait_for_engine", lambda *a, **k: True)
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    # Run provisioning synchronously so assertions can follow create/restore
    # without a join/poll loop.
    monkeypatch.setattr(m, "spawn_background", lambda fn, *a, **kw: fn(*a))

    m.reset()
    try:
        created = json.loads(m._create_collection({
            "name": "vol-test", "type": "SEARCH", "encryptionConfig": {"aWSOwnedKey": True},
        })[2])["createCollectionDetail"]
        cid = created["id"]

        # (1) create requests a named volume mount.
        assert len(runs) == 1
        volume_name = m._volume_name(cid)
        assert runs[0]["volumes"] == {volume_name: {"bind": m._ENGINE_DATA_DIR, "mode": "rw"}}

        # (2) the persisted record carries the volume.
        rec = m._collections.get(cid)
        assert rec["status"] == "ACTIVE", rec
        assert rec["_VolumeName"] == volume_name
        state = m.get_state()

        # (3) restore (warm boot) reattaches the same volume.
        m.load_persisted_state(state)
        assert len(runs) == 2
        assert runs[1]["name"] == runs[0]["name"]
        assert runs[1]["volumes"] == {volume_name: {"bind": m._ENGINE_DATA_DIR, "mode": "rw"}}
        assert m._collections.get(cid)["_VolumeName"] == volume_name

        # (4) delete removes it.
        m._delete_collection({"id": cid})
        assert removed_volumes == [volume_name]
    finally:
        m.reset()


def test_aoss_provision_no_volume_when_not_persisting(monkeypatch):
    """PERSIST_STATE=0 keeps today's behaviour: no volume is mounted."""
    from ministack.core import persistence
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import opensearchserverless as m

    fake_docker, runs, removed_volumes = _fake_docker_with_volumes()

    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    monkeypatch.setattr(m, "_get_docker", lambda: fake_docker)
    monkeypatch.setattr(m, "_dataplane_enabled", lambda: True)
    monkeypatch.setattr(m, "_wait_for_engine", lambda *a, **k: True)
    monkeypatch.setattr(persistence, "PERSIST_STATE", False)
    monkeypatch.setattr(m, "spawn_background", lambda fn, *a, **kw: fn(*a))

    m.reset()
    try:
        created = json.loads(m._create_collection({
            "name": "vol-off-test", "type": "SEARCH", "encryptionConfig": {"aWSOwnedKey": True},
        })[2])["createCollectionDetail"]
        cid = created["id"]
        assert len(runs) == 1
        assert "volumes" not in runs[0]
        assert "_VolumeName" not in m._collections.get(cid)

        m._delete_collection({"id": cid})
        assert removed_volumes == []
    finally:
        m.reset()


def test_aoss_account_isolation():
    def client(account):
        return boto3.client("opensearchserverless", endpoint_url=ENDPOINT, region_name=REGION,
                            aws_access_key_id=account, aws_secret_access_key="test")
    a, b = client("111111111111"), client("222222222222")
    name = f"iso-{_uid()}"
    a.create_security_policy(name=name, type="encryption", policy=_enc_policy(name))
    try:
        assert not any(p["name"] == name for p in b.list_security_policies(type="encryption")["securityPolicySummaries"])
        with pytest.raises(ClientError):
            b.create_collection(name=name, type="SEARCH")
    finally:
        a.delete_security_policy(name=name, type="encryption")


# ---------------------------------------------------------------------------
# Data-plane rules (no server needed)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("method,path,query,ctype,supported", [
    # AWS docs "Supported operations and plugins in Amazon OpenSearch Serverless": denied, answer 404.
    ("GET", "/", {}, "SEARCH", False),
    ("HEAD", "/", {}, "SEARCH", False),
    ("GET", "/_refresh", {}, "SEARCH", False),
    ("POST", "/_cluster/health", {}, "SEARCH", False),
    ("GET", "/_nodes", {}, "SEARCH", False),
    ("POST", "/_reindex", {}, "SEARCH", False),
    ("PUT", "/_snapshot/repo", {}, "SEARCH", False),
    ("GET", "/_cat/nodes", {}, "SEARCH", False),
    ("POST", "/events/_refresh", {}, "SEARCH", False),
    ("POST", "/_search/scroll", {}, "SEARCH", False),
    ("GET", "/_search/scroll", {}, "SEARCH", False),
    ("GET", "/events/_search", {"scroll": ["1m"]}, "SEARCH", False),
    # Proxied.
    ("POST", "/_msearch", {}, "SEARCH", True),
    ("POST", "/events/_msearch", {}, "SEARCH", True),
    ("POST", "/_mget", {}, "SEARCH", True),
    ("POST", "/events/_mget", {}, "SEARCH", True),
    ("POST", "/events/_search", {}, "SEARCH", True),
    ("PUT", "/events", {}, "SEARCH", True),
    ("DELETE", "/events", {}, "SEARCH", True),
    # TIMESERIES: no custom document ids or upserts.
    ("PUT", "/events/_doc/abc", {}, "SEARCH", True),
    ("PUT", "/events/_doc/abc", {}, "TIMESERIES", False),
    ("POST", "/events/_doc/abc", {}, "TIMESERIES", False),
    ("POST", "/events/_doc", {}, "TIMESERIES", True),
    ("POST", "/events/_update/abc", {}, "TIMESERIES", False),
])
def test_aoss_data_plane_denylist(method, path, query, ctype, supported):
    assert aoss_module._operation_supported(method, path, ctype, query) is supported


# ---------------------------------------------------------------------------
# Data plane through the gateway (no Docker)
# ---------------------------------------------------------------------------

def _gateway_request(host_header, method, path, headers=None, body=b""):
    """Connect to the gateway's own address, sending ``host_header`` as Host —
    the way test_cloudfront_dataplane.py's ``_get`` reaches a virtual host
    that doesn't resolve locally."""
    conn = http.client.HTTPConnection(urllib.parse.urlparse(ENDPOINT).hostname, int(GATEWAY_PORT), timeout=10)
    try:
        hdrs = {"Host": host_header}
        hdrs.update(headers or {})
        conn.request(method, path, body=body or None, headers=hdrs)
        resp = conn.getresponse()
        return resp.status, {k.lower(): v for k, v in resp.getheaders()}, resp.read()
    finally:
        conn.close()


def _aoss_signed_headers(body=b""):
    """Satisfies `_signed_for_aoss`'s credential-scope check and the
    x-amz-content-sha256 requirement without a virtual host DNS can resolve."""
    return {
        "authorization": f"AWS4-HMAC-SHA256 Credential=test/20260101/{REGION}/aoss/aws4_request, "
                          "SignedHeaders=host, Signature=" + "0" * 64,
        "x-amz-content-sha256": hashlib.sha256(body).hexdigest(),
    }


def test_aoss_dataplane_routed_through_gateway(collection):
    """The collection endpoint's host is routed through the gateway to
    handle_dataplane (app.py's `_AOSS_COLLECTION_HOST_RE` branch of
    `_handle_special_data_plane_request`), without Docker or a running engine.
    """
    cid = collection["id"]
    known_host = f"{cid}.{REGION}.aoss.localhost:{GATEWAY_PORT}"
    unknown_host = f"{'z' * 20}.{REGION}.aoss.localhost:{GATEWAY_PORT}"

    # Unsigned request to a real collection: 403.
    assert _gateway_request(known_host, "GET", "/events/_search")[0] == 403

    # Unknown collection id host: 404.
    assert _gateway_request(unknown_host, "GET", "/events/_search", _aoss_signed_headers())[0] == 404

    # Denylisted path, correctly signed for aoss: 404.
    assert _gateway_request(known_host, "GET", "/_cat/nodes", _aoss_signed_headers())[0] == 404

    # No engine behind it: a supported, signed request answers 503.
    if os.environ.get("OPENSEARCH_DATAPLANE") != "1":
        assert _gateway_request(known_host, "GET", "/events/_search", _aoss_signed_headers())[0] == 503


# ---------------------------------------------------------------------------
# Data plane (real engine container)
# ---------------------------------------------------------------------------

def _signed(endpoint, method, path, body=None, headers=None, service="aoss", content_sha=True):
    data = json.dumps(body).encode() if body is not None else b""
    hdrs = {"content-type": "application/json"}
    if content_sha:
        hdrs["x-amz-content-sha256"] = hashlib.sha256(data).hexdigest()
    hdrs.update(headers or {})
    req = AWSRequest(method=method, url=endpoint + path, data=data, headers=hdrs)
    SigV4Auth(Credentials("test", "test"), service, REGION).add_auth(req)
    request = urllib.request.Request(endpoint + path, data=data or None, method=method, headers=dict(req.headers))
    try:
        resp = urllib.request.urlopen(request, timeout=60)
    except urllib.error.HTTPError as e:
        resp = e
    return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()


@pytest.mark.skipif(
    os.environ.get("OPENSEARCH_DATAPLANE") != "1",
    reason="set OPENSEARCH_DATAPLANE=1 to run the collection engine smoke",
)
@pytest.mark.data_plane
def test_aoss_dataplane_index_search_and_aoss_behaviour(aoss, collection):
    if collection["status"] != "ACTIVE":
        pytest.skip(f"collection engine unavailable: {collection.get('failureMessage')}")
    ep = collection["collectionEndpoint"]

    status, _, body = _signed(ep, "PUT", "/docs", {"mappings": {"properties": {"title": {"type": "text"}}}})
    assert status == 200, body
    status, _, body = _signed(ep, "PUT", "/docs/_doc/1", {"title": "front door unlocked"})
    assert status == 201, body

    hits = []
    for _ in range(30):
        status, headers, body = _signed(ep, "POST", "/docs/_search", {"query": {"match": {"title": "unlocked"}}})
        assert status == 200 and "content-encoding" not in headers
        hits = json.loads(body)["hits"]["hits"]
        if hits:
            break
        time.sleep(0.5)
    assert [h["_id"] for h in hits] == ["1"]

    # Unsupported APIs answer 404; unsigned / wrongly scoped requests 403.
    assert _signed(ep, "POST", "/docs/_refresh")[0] == 404
    assert _signed(ep, "GET", "/_cluster/health")[0] == 404
    assert _signed(ep, "GET", "/docs", service="es")[0] == 403
    assert _signed(ep, "GET", "/docs", content_sha=False)[0] == 403
    req = urllib.request.Request(ep + "/docs", method="GET")
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(req, timeout=30)
    assert exc.value.code == 403
