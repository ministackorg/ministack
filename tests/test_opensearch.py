"""OpenSearch Service tests.

Covers the full management plane (13 ops) plus account isolation. The data
plane (real ``opensearchproject/opensearch`` container per domain) is gated
by ``OPENSEARCH_DATAPLANE=1`` and exercised separately when that env var
is set.
"""

import os
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError


ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


def _client(account="test"):
    return boto3.client(
        "opensearch",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=account,
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="module")
def os_client():
    return _client()


def _uid():
    return uuid.uuid4().hex[:6]


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def test_opensearch_create_describe_delete(os_client):
    name = f"d-{_uid()}"
    rec = os_client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.13")["DomainStatus"]
    assert rec["DomainName"] == name
    assert rec["EngineVersion"] == "OpenSearch_2.13"
    assert rec["ARN"].startswith("arn:aws:es:")
    assert rec["DomainEndpointOptions"]["EnforceHTTPS"] is True

    desc = os_client.describe_domain(DomainName=name)["DomainStatus"]
    assert desc["DomainName"] == name

    deleted = os_client.delete_domain(DomainName=name)["DomainStatus"]
    assert deleted["Deleted"] is True


def test_opensearch_describe_missing(os_client):
    with pytest.raises(ClientError) as exc:
        os_client.describe_domain(DomainName="nope-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_opensearch_create_duplicate(os_client):
    name = f"dup-{_uid()}"
    os_client.create_domain(DomainName=name)
    try:
        with pytest.raises(ClientError) as exc:
            os_client.create_domain(DomainName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
    finally:
        os_client.delete_domain(DomainName=name)


def test_opensearch_invalid_name_rejected(os_client):
    """AWS DomainName rules: lowercase, 3-28 chars, alphanumeric + hyphens,
    first char must be a lowercase letter."""
    with pytest.raises(ClientError) as exc:
        os_client.create_domain(DomainName="UpperCase")
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_opensearch_describe_domains_filters_unknown(os_client):
    a = f"d-{_uid()}"
    os_client.create_domain(DomainName=a)
    try:
        out = os_client.describe_domains(DomainNames=[a, "ghost-domain-xyz"])
        names = [d["DomainName"] for d in out["DomainStatusList"]]
        assert a in names
        assert "ghost-domain-xyz" not in names
    finally:
        os_client.delete_domain(DomainName=a)


# ---------------------------------------------------------------------------
# ListDomainNames
# ---------------------------------------------------------------------------

def test_opensearch_list_domain_names_engine_filter(os_client):
    es_name = f"es-{_uid()}"
    os_name = f"os-{_uid()}"
    os_client.create_domain(DomainName=es_name, EngineVersion="Elasticsearch_7.10")
    os_client.create_domain(DomainName=os_name, EngineVersion="OpenSearch_2.11")
    try:
        es_only = {d["DomainName"]: d["EngineType"]
                   for d in os_client.list_domain_names(EngineType="Elasticsearch")["DomainNames"]}
        assert es_only.get(es_name) == "Elasticsearch"
        assert os_name not in es_only
    finally:
        os_client.delete_domain(DomainName=es_name)
        os_client.delete_domain(DomainName=os_name)


# ---------------------------------------------------------------------------
# DomainConfig (Update/Describe/Progress)
# ---------------------------------------------------------------------------

def test_opensearch_describe_domain_config_wraps_options_status(os_client):
    name = f"cfg-{_uid()}"
    os_client.create_domain(DomainName=name)
    try:
        cfg = os_client.describe_domain_config(DomainName=name)["DomainConfig"]
        for key in ("EngineVersion", "ClusterConfig", "EBSOptions",
                    "AccessPolicies", "DomainEndpointOptions"):
            assert key in cfg, f"missing {key}"
            entry = cfg[key]
            assert "Options" in entry, f"{key} missing Options"
            assert "Status" in entry, f"{key} missing Status"
            assert entry["Status"]["State"] == "Active"
    finally:
        os_client.delete_domain(DomainName=name)


def test_opensearch_describe_omits_empty_vpc_options(os_client):
    name = f"novpc-{_uid()}"
    os_client.create_domain(DomainName=name)
    try:
        desc = os_client.describe_domain(DomainName=name)["DomainStatus"]
        assert "VPCOptions" not in desc
        assert desc["Endpoint"]
        assert "Endpoints" not in desc

        cfg = os_client.describe_domain_config(DomainName=name)["DomainConfig"]
        assert "VPCOptions" not in cfg
    finally:
        os_client.delete_domain(DomainName=name)


def test_opensearch_describe_domain_config_skips_unset_vpc_options(os_client):
    name = f"dcfg-{_uid()}"
    os_client.create_domain(DomainName=name)
    try:
        cfg = os_client.describe_domain_config(DomainName=name)["DomainConfig"]
        assert "VPCOptions" not in cfg
    finally:
        os_client.delete_domain(DomainName=name)


def test_opensearch_describe_with_vpc_returns_endpoints_map(os_client):
    name = f"vpc-{_uid()}"
    vpc_options = {
        "SubnetIds": ["subnet-aaa", "subnet-bbb"],
        "SecurityGroupIds": ["sg-1"],
    }
    rec = os_client.create_domain(DomainName=name, VPCOptions=vpc_options)["DomainStatus"]
    try:
        assert "Endpoint" not in rec
        assert rec["Endpoints"]["vpc"]
        assert rec["VPCOptions"]["SubnetIds"] == vpc_options["SubnetIds"]
        assert rec["VPCOptions"]["SecurityGroupIds"] == vpc_options["SecurityGroupIds"]
        assert rec["VPCOptions"]["VPCId"].startswith("vpc-")
        assert rec["VPCOptions"]["AvailabilityZones"]

        desc = os_client.describe_domain(DomainName=name)["DomainStatus"]
        assert "Endpoint" not in desc
        assert desc["Endpoints"]["vpc"] == rec["Endpoints"]["vpc"]
    finally:
        os_client.delete_domain(DomainName=name)


def test_opensearch_update_domain_to_vpc_swaps_endpoint_shape(os_client):
    name = f"uvpc-{_uid()}"
    os_client.create_domain(DomainName=name)
    try:
        before = os_client.describe_domain(DomainName=name)["DomainStatus"]
        assert before["Endpoint"]
        assert "Endpoints" not in before

        vpc_options = {
            "SubnetIds": ["subnet-upd"],
            "SecurityGroupIds": ["sg-upd"],
        }
        os_client.update_domain_config(DomainName=name, VPCOptions=vpc_options)

        after = os_client.describe_domain(DomainName=name)["DomainStatus"]
        assert "Endpoint" not in after
        assert after["Endpoints"]["vpc"] == before["Endpoint"]
        assert after["VPCOptions"]["SubnetIds"] == vpc_options["SubnetIds"]
    finally:
        os_client.delete_domain(DomainName=name)


def test_opensearch_update_domain_config_persists(os_client):
    name = f"upd-{_uid()}"
    os_client.create_domain(DomainName=name)
    try:
        new_cluster = {
            "InstanceType": "r5.large.search",
            "InstanceCount": 3,
            "DedicatedMasterEnabled": True,
            "DedicatedMasterCount": 3,
            "DedicatedMasterType": "m5.large.search",
            "ZoneAwarenessEnabled": True,
            "ZoneAwarenessConfig": {"AvailabilityZoneCount": 3},
            "WarmEnabled": False,
            "ColdStorageOptions": {"Enabled": False},
        }
        out = os_client.update_domain_config(DomainName=name, ClusterConfig=new_cluster)
        assert out["DomainConfig"]["ClusterConfig"]["Options"]["InstanceCount"] == 3
        assert out["DomainConfig"]["ClusterConfig"]["Options"]["InstanceType"] == "r5.large.search"

        cfg = os_client.describe_domain_config(DomainName=name)["DomainConfig"]
        assert cfg["ClusterConfig"]["Options"]["InstanceCount"] == 3
    finally:
        os_client.delete_domain(DomainName=name)


def test_opensearch_describe_domain_change_progress(os_client):
    name = f"prog-{_uid()}"
    os_client.create_domain(DomainName=name)
    try:
        p0 = os_client.describe_domain_change_progress(DomainName=name)["ChangeProgressStatus"]
        assert p0["Status"] == "COMPLETED"

        os_client.update_domain_config(DomainName=name, AccessPolicies="{}")
        p1 = os_client.describe_domain_change_progress(DomainName=name)["ChangeProgressStatus"]
        assert p1["Status"] == "COMPLETED"
        assert "AccessPolicies" in p1["CompletedProperties"]
        assert p1["ChangeId"]
    finally:
        os_client.delete_domain(DomainName=name)


# ---------------------------------------------------------------------------
# Versions
# ---------------------------------------------------------------------------

def test_opensearch_list_versions_includes_recent(os_client):
    versions = os_client.list_versions()["Versions"]
    assert "OpenSearch_2.15" in versions
    assert "Elasticsearch_7.10" in versions


def test_opensearch_get_compatible_versions_for_domain(os_client):
    name = f"compat-{_uid()}"
    os_client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.11")
    try:
        compat = os_client.get_compatible_versions(DomainName=name)["CompatibleVersions"]
        assert len(compat) == 1
        assert compat[0]["SourceVersion"] == "OpenSearch_2.11"
        assert "OpenSearch_2.11" not in compat[0]["TargetVersions"]
        assert all(v.startswith("OpenSearch") for v in compat[0]["TargetVersions"])
    finally:
        os_client.delete_domain(DomainName=name)


def test_opensearch_get_compatible_versions_no_domain_returns_matrix(os_client):
    out = os_client.get_compatible_versions()["CompatibleVersions"]
    sources = {entry["SourceVersion"] for entry in out}
    assert "OpenSearch_2.15" in sources
    assert "Elasticsearch_7.10" in sources


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def test_opensearch_tag_lifecycle(os_client):
    name = f"tag-{_uid()}"
    rec = os_client.create_domain(DomainName=name)["DomainStatus"]
    arn = rec["ARN"]
    try:
        os_client.add_tags(ARN=arn, TagList=[
            {"Key": "Env", "Value": "Test"},
            {"Key": "Owner", "Value": "ministack"},
        ])
        tags = {t["Key"]: t["Value"] for t in os_client.list_tags(ARN=arn)["TagList"]}
        assert tags == {"Env": "Test", "Owner": "ministack"}

        os_client.add_tags(ARN=arn, TagList=[{"Key": "Env", "Value": "Prod"}])
        tags = {t["Key"]: t["Value"] for t in os_client.list_tags(ARN=arn)["TagList"]}
        assert tags["Env"] == "Prod"
        assert tags["Owner"] == "ministack"

        os_client.remove_tags(ARN=arn, TagKeys=["Env"])
        tags = {t["Key"]: t["Value"] for t in os_client.list_tags(ARN=arn)["TagList"]}
        assert "Env" not in tags
        assert tags["Owner"] == "ministack"
    finally:
        os_client.delete_domain(DomainName=name)


def test_opensearch_create_with_tag_list(os_client):
    name = f"ctag-{_uid()}"
    rec = os_client.create_domain(
        DomainName=name,
        TagList=[{"Key": "ProjectId", "Value": "X"}],
    )["DomainStatus"]
    try:
        tags = {t["Key"]: t["Value"] for t in os_client.list_tags(ARN=rec["ARN"])["TagList"]}
        assert tags == {"ProjectId": "X"}
    finally:
        os_client.delete_domain(DomainName=name)


# ---------------------------------------------------------------------------
# Multi-tenant
# ---------------------------------------------------------------------------

def test_opensearch_account_isolation():
    a = _client("111111111111")
    b = _client("222222222222")
    name = f"acct-{_uid()}"
    a.create_domain(DomainName=name)
    try:
        a_listed = [d["DomainName"] for d in a.list_domain_names()["DomainNames"]]
        b_listed = [d["DomainName"] for d in b.list_domain_names()["DomainNames"]]
        assert name in a_listed
        assert name not in b_listed
        with pytest.raises(ClientError) as exc:
            b.describe_domain(DomainName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        a.delete_domain(DomainName=name)


# ---------------------------------------------------------------------------
# Data plane (gated)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    os.environ.get("OPENSEARCH_DATAPLANE") != "1",
    reason="set OPENSEARCH_DATAPLANE=1 to run the real-cluster smoke",
)
def test_opensearch_dataplane_cluster_health():
    """When OPENSEARCH_DATAPLANE=1 is set on the ministack server, CreateDomain
    spawns a real opensearchproject/opensearch container and DescribeDomain
    returns its endpoint. Verify cluster health responds."""
    import urllib.request
    import json as _json

    o = _client()
    name = f"dp-{_uid()}"
    rec = o.create_domain(DomainName=name)["DomainStatus"]
    try:
        endpoint = rec["Endpoint"]
        if "ministack.local" in endpoint:
            pytest.skip("dataplane returned stub endpoint — Docker not available on test host")
        url = f"http://{endpoint}/_cluster/health"
        last_err = None
        for _ in range(60):
            try:
                body = _json.loads(urllib.request.urlopen(url, timeout=1).read())
                assert body["status"] in ("green", "yellow")
                return
            except Exception as e:
                last_err = e
                time.sleep(1)
        pytest.fail(f"cluster never became healthy: {last_err!r}")
    finally:
        o.delete_domain(DomainName=name)
