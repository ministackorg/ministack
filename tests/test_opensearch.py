import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


def _client(service, account="test"):
    return boto3.client(service, endpoint_url=ENDPOINT, region_name=REGION,
                        aws_access_key_id=account, aws_secret_access_key="test")


def _client_for(account):
    return _client("opensearch", account=account)


@pytest.fixture(scope="module")
def os_client():
    return _client("opensearch")


def _uid():
    import uuid
    return uuid.uuid4().hex[:8]


def test_opensearch_list_create_describe_delete(os_client):
    name = f"d-{_uid()}"
    # Create
    rec = os_client.create_domain(DomainName=name, EngineVersion="OpenSearch_2.13")["DomainStatus"]
    assert rec["DomainName"] == name
    assert rec["EngineVersion"] == "OpenSearch_2.13"
    assert rec["ARN"].startswith("arn:aws:es:")
    assert rec["Endpoint"].endswith("es.amazonaws.com")

    # ListDomainNames includes it with correct EngineType
    listed = {d["DomainName"]: d["EngineType"] for d in os_client.list_domain_names()["DomainNames"]}
    assert listed.get(name) == "OpenSearch"

    # DescribeDomain
    desc = os_client.describe_domain(DomainName=name)["DomainStatus"]
    assert desc["DomainName"] == name
    assert desc["ClusterConfig"]["InstanceType"] == "m5.large.search"

    # DescribeDomains
    statuses = os_client.describe_domains(DomainNames=[name])["DomainStatusList"]
    assert any(s["DomainName"] == name for s in statuses)

    # DeleteDomain
    deleted = os_client.delete_domain(DomainName=name)["DomainStatus"]
    assert deleted["Deleted"] is True

    # No longer listed
    listed2 = [d["DomainName"] for d in os_client.list_domain_names()["DomainNames"]]
    assert name not in listed2


def test_opensearch_describe_missing_domain_raises(os_client):
    with pytest.raises(ClientError) as exc:
        os_client.describe_domain(DomainName="never-existed-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_opensearch_create_duplicate_raises(os_client):
    name = f"dup-{_uid()}"
    os_client.create_domain(DomainName=name)
    try:
        with pytest.raises(ClientError) as exc:
            os_client.create_domain(DomainName=name)
        assert exc.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
    finally:
        os_client.delete_domain(DomainName=name)


def test_opensearch_engine_type_filter(os_client):
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


def test_opensearch_account_isolation():
    """Domains created under one account ID must not surface under another."""
    a = _client_for("111111111111")
    b = _client_for("222222222222")
    name_a = f"acct-a-{_uid()}"
    a.create_domain(DomainName=name_a)
    try:
        a_listed = [d["DomainName"] for d in a.list_domain_names()["DomainNames"]]
        b_listed = [d["DomainName"] for d in b.list_domain_names()["DomainNames"]]
        assert name_a in a_listed
        assert name_a not in b_listed
    finally:
        a.delete_domain(DomainName=name_a)
