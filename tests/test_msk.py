"""
MSK (Managed Streaming for Kafka) integration tests.

boto3 round-trip against shapes verified from botocore kafka-2018-11-14.
Covers cluster CRUD, configuration CRUD with revisions, SCRAM, tags, and
GetBootstrapBrokers including the MINISTACK_MSK_BOOTSTRAP proxy passthrough.
"""

import base64
import os

import botocore.exceptions
import pytest
from conftest import make_client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _kafka():
    return make_client("kafka")


def _broker_node_group():
    return {
        "InstanceType": "kafka.m5.large",
        "ClientSubnets": ["subnet-aaaaaaaa", "subnet-bbbbbbbb", "subnet-cccccccc"],
        "SecurityGroups": ["sg-aaaaaaaa"],
    }


def _create_basic_cluster(name: str, tags=None):
    kw = {
        "ClusterName": name,
        "BrokerNodeGroupInfo": _broker_node_group(),
        "KafkaVersion": "3.6.0",
        "NumberOfBrokerNodes": 3,
    }
    if tags:
        kw["Tags"] = tags
    return _kafka().create_cluster(**kw)


# ---------------------------------------------------------------------------
# Cluster CRUD
# ---------------------------------------------------------------------------


def test_msk_create_cluster_returns_required_fields():
    resp = _create_basic_cluster("cluster-a")
    assert resp["ClusterArn"].startswith("arn:aws:kafka:us-east-1:")
    assert ":cluster/cluster-a/" in resp["ClusterArn"]
    assert resp["ClusterName"] == "cluster-a"
    assert resp["State"] == "ACTIVE"


def test_msk_describe_cluster_round_trips():
    create = _create_basic_cluster("cluster-b")
    desc = _kafka().describe_cluster(ClusterArn=create["ClusterArn"])
    info = desc["ClusterInfo"]
    assert info["ClusterArn"] == create["ClusterArn"]
    assert info["ClusterName"] == "cluster-b"
    assert info["State"] == "ACTIVE"
    assert info["NumberOfBrokerNodes"] == 3
    assert info["CurrentBrokerSoftwareInfo"]["KafkaVersion"] == "3.6.0"


def test_msk_list_clusters_returns_created_one():
    _create_basic_cluster("cluster-l1")
    _create_basic_cluster("cluster-l2")
    resp = _kafka().list_clusters()
    names = {c["ClusterName"] for c in resp["ClusterInfoList"]}
    assert {"cluster-l1", "cluster-l2"}.issubset(names)


def test_msk_list_clusters_with_name_filter():
    _create_basic_cluster("filter-x1")
    _create_basic_cluster("filter-x2")
    _create_basic_cluster("other-y1")
    resp = _kafka().list_clusters(ClusterNameFilter="filter-")
    names = {c["ClusterName"] for c in resp["ClusterInfoList"]}
    assert {"filter-x1", "filter-x2"}.issubset(names)
    assert "other-y1" not in names


def test_msk_delete_cluster_marks_deleting_and_removes_it():
    create = _create_basic_cluster("cluster-d")
    arn = create["ClusterArn"]
    delete = _kafka().delete_cluster(ClusterArn=arn)
    assert delete["State"] == "DELETING"
    try:
        _kafka().describe_cluster(ClusterArn=arn)
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "NotFoundException"
    else:
        raise AssertionError("expected NotFoundException after delete")


def test_msk_create_cluster_duplicate_name_returns_conflict():
    _create_basic_cluster("dup-cluster")
    try:
        _create_basic_cluster("dup-cluster")
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ConflictException"
    else:
        raise AssertionError("expected ConflictException")


def test_msk_create_cluster_invalid_name_returns_bad_request():
    try:
        _kafka().create_cluster(
            ClusterName="1-starts-with-digit",
            BrokerNodeGroupInfo=_broker_node_group(),
            KafkaVersion="3.6.0",
            NumberOfBrokerNodes=3,
        )
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "BadRequestException"
    else:
        raise AssertionError("expected BadRequestException")


# ---------------------------------------------------------------------------
# Bootstrap brokers
# ---------------------------------------------------------------------------


def test_msk_get_bootstrap_brokers_returns_all_endpoint_strings():
    create = _create_basic_cluster("cluster-boot")
    resp = _kafka().get_bootstrap_brokers(ClusterArn=create["ClusterArn"])
    # All five required strings are present in shape (some may be empty when
    # the matching auth mode isn't configured — wire-shape parity)
    for k in (
        "BootstrapBrokerString",
        "BootstrapBrokerStringTls",
        "BootstrapBrokerStringSaslScram",
        "BootstrapBrokerStringSaslIam",
    ):
        assert k in resp
    assert ":9092" in resp["BootstrapBrokerString"]
    assert ":9094" in resp["BootstrapBrokerStringTls"]
    assert ":9098" in resp["BootstrapBrokerStringSaslIam"]


def test_msk_get_bootstrap_brokers_honors_env_passthrough(monkeypatch):
    """When MINISTACK_MSK_BOOTSTRAP is set, GetBootstrapBrokers must surface
    it so Kafka clients route to the user-supplied broker (Glue pattern)."""
    # Directly exercise the helper since the server already loaded with empty
    # env. The wiring is the same; env values flow through _bootstrap_strings.
    monkeypatch.setattr("ministack.services.msk._BOOTSTRAP_PLAIN",
                         "redpanda.local:19092")
    monkeypatch.setattr("ministack.services.msk._BOOTSTRAP_SASL_IAM",
                         "redpanda.local:19098")
    from ministack.services.msk import _bootstrap_strings
    out = _bootstrap_strings("my-cluster")
    assert out["BootstrapBrokerString"] == "redpanda.local:19092"
    assert out["BootstrapBrokerStringSaslIam"] == "redpanda.local:19098"


def test_msk_get_bootstrap_brokers_unknown_cluster_returns_404():
    arn = "arn:aws:kafka:us-east-1:000000000000:cluster/missing/00000000"
    try:
        _kafka().get_bootstrap_brokers(ClusterArn=arn)
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "NotFoundException"
    else:
        raise AssertionError("expected NotFoundException")


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------


def test_msk_list_nodes_returns_one_per_broker():
    create = _create_basic_cluster("cluster-nodes")
    resp = _kafka().list_nodes(ClusterArn=create["ClusterArn"])
    assert len(resp["NodeInfoList"]) == 3
    for node in resp["NodeInfoList"]:
        assert node["NodeType"] == "BROKER"
        assert "BrokerNodeInfo" in node
        assert "BrokerId" in node["BrokerNodeInfo"]


# ---------------------------------------------------------------------------
# Configurations
# ---------------------------------------------------------------------------


def test_msk_create_configuration_returns_revision_1():
    resp = _kafka().create_configuration(
        Name="cfg-a",
        KafkaVersions=["3.6.0"],
        Description="initial",
        ServerProperties=b"auto.create.topics.enable=true",
    )
    assert resp["Name"] == "cfg-a"
    assert resp["State"] == "ACTIVE"
    assert resp["LatestRevision"]["Revision"] == 1
    assert resp["Arn"].startswith("arn:aws:kafka:us-east-1:")
    assert ":configuration/" in resp["Arn"]


def test_msk_describe_configuration_round_trips():
    create = _kafka().create_configuration(
        Name="cfg-b",
        KafkaVersions=["3.6.0"],
        ServerProperties=b"x=1",
    )
    desc = _kafka().describe_configuration(Arn=create["Arn"])
    assert desc["Name"] == "cfg-b"
    assert desc["KafkaVersions"] == ["3.6.0"]


def test_msk_list_configurations_returns_created_ones():
    _kafka().create_configuration(Name="cfg-l1", KafkaVersions=["3.6.0"],
                                    ServerProperties=b"x=1")
    _kafka().create_configuration(Name="cfg-l2", KafkaVersions=["3.6.0"],
                                    ServerProperties=b"x=2")
    resp = _kafka().list_configurations()
    names = {c["Name"] for c in resp["Configurations"]}
    assert {"cfg-l1", "cfg-l2"}.issubset(names)


def test_msk_describe_configuration_revision_returns_server_properties():
    create = _kafka().create_configuration(
        Name="cfg-rev",
        KafkaVersions=["3.6.0"],
        ServerProperties=b"foo=bar",
    )
    resp = _kafka().describe_configuration_revision(Arn=create["Arn"], Revision=1)
    assert resp["Revision"] == 1
    # ServerProperties round-trips as bytes through botocore's blob handling
    assert resp["ServerProperties"] == b"foo=bar"


def test_msk_list_configuration_revisions():
    create = _kafka().create_configuration(
        Name="cfg-revs",
        KafkaVersions=["3.6.0"],
        ServerProperties=b"y=2",
    )
    resp = _kafka().list_configuration_revisions(Arn=create["Arn"])
    assert len(resp["Revisions"]) == 1
    assert resp["Revisions"][0]["Revision"] == 1


def test_msk_create_configuration_duplicate_name_returns_conflict():
    _kafka().create_configuration(Name="dup-cfg", KafkaVersions=["3.6.0"],
                                    ServerProperties=b"x=1")
    try:
        _kafka().create_configuration(Name="dup-cfg", KafkaVersions=["3.6.0"],
                                        ServerProperties=b"x=2")
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ConflictException"
    else:
        raise AssertionError("expected ConflictException")


# ---------------------------------------------------------------------------
# SCRAM secrets
# ---------------------------------------------------------------------------


def test_msk_scram_associate_list_disassociate_round_trip():
    cluster = _create_basic_cluster("cluster-scram")
    secret_a = "arn:aws:secretsmanager:us-east-1:000000000000:secret:AmazonMSK_a"
    secret_b = "arn:aws:secretsmanager:us-east-1:000000000000:secret:AmazonMSK_b"
    assoc = _kafka().batch_associate_scram_secret(
        ClusterArn=cluster["ClusterArn"],
        SecretArnList=[secret_a, secret_b],
    )
    assert assoc["UnprocessedScramSecrets"] == []
    listed = _kafka().list_scram_secrets(ClusterArn=cluster["ClusterArn"])
    assert set(listed["SecretArnList"]) == {secret_a, secret_b}
    disassoc = _kafka().batch_disassociate_scram_secret(
        ClusterArn=cluster["ClusterArn"],
        SecretArnList=[secret_a],
    )
    assert disassoc["UnprocessedScramSecrets"] == []
    listed_after = _kafka().list_scram_secrets(ClusterArn=cluster["ClusterArn"])
    assert listed_after["SecretArnList"] == [secret_b]


def test_msk_scram_associate_duplicate_reports_unprocessed():
    cluster = _create_basic_cluster("cluster-scram-dup")
    secret = "arn:aws:secretsmanager:us-east-1:000000000000:secret:AmazonMSK_d"
    _kafka().batch_associate_scram_secret(
        ClusterArn=cluster["ClusterArn"],
        SecretArnList=[secret],
    )
    resp = _kafka().batch_associate_scram_secret(
        ClusterArn=cluster["ClusterArn"],
        SecretArnList=[secret],
    )
    assert len(resp["UnprocessedScramSecrets"]) == 1
    assert resp["UnprocessedScramSecrets"][0]["SecretArn"] == secret


def test_msk_scram_disassociate_unknown_reports_unprocessed():
    cluster = _create_basic_cluster("cluster-scram-miss")
    secret = "arn:aws:secretsmanager:us-east-1:000000000000:secret:AmazonMSK_m"
    resp = _kafka().batch_disassociate_scram_secret(
        ClusterArn=cluster["ClusterArn"],
        SecretArnList=[secret],
    )
    assert len(resp["UnprocessedScramSecrets"]) == 1


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def test_msk_create_cluster_with_tags_then_list_tags():
    create = _create_basic_cluster("cluster-tag", tags={"env": "prod", "team": "platform"})
    resp = _kafka().list_tags_for_resource(ResourceArn=create["ClusterArn"])
    assert resp["Tags"]["env"] == "prod"
    assert resp["Tags"]["team"] == "platform"


def test_msk_tag_then_untag_resource():
    create = _create_basic_cluster("cluster-untag")
    arn = create["ClusterArn"]
    _kafka().tag_resource(ResourceArn=arn, Tags={"a": "1", "b": "2"})
    listed = _kafka().list_tags_for_resource(ResourceArn=arn)
    assert listed["Tags"] == {"a": "1", "b": "2"}
    _kafka().untag_resource(ResourceArn=arn, TagKeys=["a"])
    listed = _kafka().list_tags_for_resource(ResourceArn=arn)
    assert listed["Tags"] == {"b": "2"}


# ---------------------------------------------------------------------------
# Region isolation (AccountRegionScopedDict)
# ---------------------------------------------------------------------------


def test_msk_clusters_isolated_by_region():
    import boto3
    from botocore.config import Config
    from conftest import ENDPOINT

    def _client(region):
        return boto3.client(
            "kafka", endpoint_url=ENDPOINT,
            aws_access_key_id="test", aws_secret_access_key="test",
            region_name=region,
            config=Config(region_name=region, retries={"mode": "standard"}),
        )

    east = _client("us-east-1")
    west = _client("us-west-2")
    east.create_cluster(
        ClusterName="east-only",
        BrokerNodeGroupInfo=_broker_node_group(),
        KafkaVersion="3.6.0", NumberOfBrokerNodes=3,
    )
    west.create_cluster(
        ClusterName="west-only",
        BrokerNodeGroupInfo=_broker_node_group(),
        KafkaVersion="3.6.0", NumberOfBrokerNodes=3,
    )
    east_names = {c["ClusterName"] for c in east.list_clusters()["ClusterInfoList"]}
    west_names = {c["ClusterName"] for c in west.list_clusters()["ClusterInfoList"]}
    assert "east-only" in east_names
    assert "east-only" not in west_names
    assert "west-only" in west_names
    assert "west-only" not in east_names
