"""Configuration/readback regressions; synthetic instances, no DB or AWS calls."""

import asyncio
import io
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

import boto3
import pytest
from botocore.awsrequest import AWSResponse
from botocore.config import Config
from botocore.exceptions import ClientError

from ministack.core.responses import AccountRegionScopedDict, request_scope
from ministack.services import rds

FLAGS = ("DeletionProtection", "CopyTagsToSnapshot")
ACCOUNT = "111111111111"
REGION = "us-east-1"


@pytest.fixture(autouse=True)
def isolated_state(monkeypatch):
    monkeypatch.setattr(rds, "_instances", AccountRegionScopedDict())
    monkeypatch.setattr(rds, "_clusters", AccountRegionScopedDict())
    monkeypatch.setattr(rds, "_tags", AccountRegionScopedDict())
    with request_scope(ACCOUNT, REGION):
        yield


def _instance(**values):
    instance = {
        "DBInstanceIdentifier": "immediate-settings",
        "DBInstanceArn": f"arn:aws:rds:{REGION}:{ACCOUNT}:db:immediate-settings",
        "DBInstanceStatus": "available", "MasterUsername": "admin",
        "InstanceCreateTime": "2026-01-01T00:00:00Z",
        "Engine": "postgres", "EngineVersion": "16.3",
        "AllocatedStorage": 20, "DBInstanceClass": "db.t4g.small",
        "PendingModifiedValues": {},
        "DeletionProtection": False, "CopyTagsToSnapshot": False,
    }
    instance.update(values)
    rds._instances[instance["DBInstanceIdentifier"]] = instance
    return instance


def _query(action, **params):
    body = urlencode({"Action": action, "Version": "2014-10-31", **params}).encode()
    status, headers, body = asyncio.run(rds.handle_request(
        "POST", "/", {"content-type": "application/x-www-form-urlencoded"}, body, {},
    ))
    assert status == 200, body
    return status, headers, body.encode() if isinstance(body, str) else body


class _RawResponse(io.BytesIO):
    def stream(self, amt=None, decode_content=False):
        yield self.read()


def _sdk():
    # The before-send transport uses production Query dispatch and XML. An
    # accidentally unhandled request goes to a refusal endpoint, never AWS.
    client = boto3.client(
        "rds", endpoint_url="http://127.0.0.1:9", region_name=REGION,
        aws_access_key_id=ACCOUNT, aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 0}),
    )

    def send(request, **kwargs):
        status, headers, body = asyncio.run(rds.handle_request(
            "POST", "/", {"content-type": "application/x-www-form-urlencoded"},
            request.body, {},
        ))
        body = body.encode() if isinstance(body, str) else body
        return AWSResponse(request.url, status, headers, _RawResponse(body))

    client.meta.events.register("before-send.rds", send)
    return client


def _modify_readback(mode, **params):
    if mode == "sdk":
        client = _sdk()
        changed = client.modify_db_instance(
            DBInstanceIdentifier="immediate-settings", **params,
        )["DBInstance"]
        described = client.describe_db_instances(
            DBInstanceIdentifier="immediate-settings",
        )["DBInstances"][0]
        return changed, described

    query_params = {key: str(value).lower() if isinstance(value, bool) else value
                    for key, value in params.items()}
    outputs = []
    for action in ("ModifyDBInstance", "DescribeDBInstances"):
        _, _, body = _query(
            action, DBInstanceIdentifier="immediate-settings",
            **(query_params if action == "ModifyDBInstance" else {}),
        )
        instance = ET.fromstring(body).find(".//{*}DBInstance")
        outputs.append({
            **{flag: instance.findtext(f"{{*}}{flag}") == "true" for flag in FLAGS},
            "PendingModifiedValues": {
                child.tag.split("}")[-1]: child.text
                for child in instance.find("{*}PendingModifiedValues")
            },
        })
    return outputs


@pytest.mark.parametrize("mode", ["sdk", "query"])
@pytest.mark.parametrize("apply", [None, False, True])
@pytest.mark.parametrize("desired", [False, True])
@pytest.mark.parametrize("flag", FLAGS)
def test_standalone_flags_apply_immediately(mode, apply, desired, flag):
    instance = _instance(**{flag: not desired})
    params = {flag: desired}
    if apply is not None:
        params["ApplyImmediately"] = apply
    for output in _modify_readback(mode, **params):
        assert output[flag] is desired
        assert not set(FLAGS).intersection(output["PendingModifiedValues"])
    assert instance[flag] is desired
    assert instance["PendingModifiedValues"] == {}


@pytest.mark.parametrize("mode", ["sdk", "query"])
@pytest.mark.parametrize("apply", [None, False, True])
def test_both_flags_repeated_updates_preserve_omitted_values(mode, apply):
    _instance()
    params = {} if apply is None else {"ApplyImmediately": apply}
    for desired in (True, False, True):
        for output in _modify_readback(mode, **params, **dict.fromkeys(FLAGS, desired)):
            assert all(output[flag] is desired for flag in FLAGS)
            assert output["PendingModifiedValues"] == {}
    for output in _modify_readback(mode, **params, DeletionProtection=False):
        assert output["DeletionProtection"] is False
        assert output["CopyTagsToSnapshot"] is True
    for output in _modify_readback(mode, **params, CopyTagsToSnapshot=False):
        assert all(output[flag] is False for flag in FLAGS)


@pytest.mark.parametrize("flag", FLAGS)
def test_flag_only_request_preserves_unrelated_pending_changes(flag):
    pending = {"DBInstanceClass": "db.t4g.medium", "AllocatedStorage": 30}
    instance = _instance(PendingModifiedValues=pending.copy())
    for output in _modify_readback("sdk", ApplyImmediately=False, **{flag: True}):
        assert output[flag] is True
        assert output["PendingModifiedValues"] == pending
    assert instance["PendingModifiedValues"] == pending


@pytest.mark.parametrize("apply", [None, False, True])
def test_immediate_protection_blocks_delete_and_disabling_allows_it(monkeypatch, apply):
    monkeypatch.setattr(rds, "_get_docker", lambda: None)
    _instance()
    client = _sdk()
    params = {} if apply is None else {"ApplyImmediately": apply}
    client.modify_db_instance(
        DBInstanceIdentifier="immediate-settings", DeletionProtection=True, **params,
    )
    with pytest.raises(ClientError) as error:
        client.delete_db_instance(
            DBInstanceIdentifier="immediate-settings", SkipFinalSnapshot=True,
        )
    assert error.value.response["Error"]["Code"] == "InvalidParameterCombination"
    assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert client.describe_db_instances(
        DBInstanceIdentifier="immediate-settings",
    )["DBInstances"][0]["DeletionProtection"] is True
    client.modify_db_instance(
        DBInstanceIdentifier="immediate-settings", DeletionProtection=False, **params,
    )
    assert client.delete_db_instance(
        DBInstanceIdentifier="immediate-settings", SkipFinalSnapshot=True,
    )["DBInstance"]["DBInstanceStatus"] == "deleting"
    with pytest.raises(ClientError) as error:
        client.describe_db_instances(DBInstanceIdentifier="immediate-settings")
    assert error.value.response["Error"]["Code"] == "DBInstanceNotFound"
    assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_flags_do_not_create_a_missing_instance():
    with pytest.raises(ClientError) as error:
        _sdk().modify_db_instance(
            DBInstanceIdentifier="missing-settings", DeletionProtection=True,
            CopyTagsToSnapshot=True,
        )
    assert error.value.response["Error"]["Code"] == "DBInstanceNotFound"
    assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert not rds._instances


def test_immediate_flags_are_isolated_by_account_and_region():
    tenants = [(ACCOUNT, REGION), ("222222222222", REGION), (ACCOUNT, "eu-west-1")]
    for account, region in tenants:
        with request_scope(account, region):
            _instance()
    _modify_readback("sdk", ApplyImmediately=False, **dict.fromkeys(FLAGS, True))
    for account, region in tenants:
        with request_scope(account, region):
            output = _sdk().describe_db_instances(
                DBInstanceIdentifier="immediate-settings",
            )["DBInstances"][0]
            assert all(output[flag] is ((account, region) == tenants[0]) for flag in FLAGS)


@pytest.mark.parametrize("engine,membership", [
    ("aurora-postgresql", "DBClusterIdentifier"),
    ("aurora-postgresql", "_shared_cluster_id"),
    ("postgres", "DBClusterIdentifier"),
    ("postgres", "_shared_cluster_id"),
    ("aurora-postgresql", None),
])
@pytest.mark.parametrize("apply", [None, False, True])
def test_cluster_member_and_aurora_paths_are_unchanged(engine, membership, apply):
    # These are existing behavior guards, not claims of cluster-member parity.
    instance = _instance(Engine=engine, **({membership: "cluster"} if membership else {}))
    cluster = {
        "DBClusterIdentifier": "cluster", "DBClusterArn": f"arn:aws:rds:{REGION}:{ACCOUNT}:cluster:cluster",
        "Engine": "aurora-postgresql", "EngineVersion": "17.7",
        "Status": "available", "Port": 5432, **dict.fromkeys(FLAGS, False),
        "ClusterCreateTime": "2026-01-01T00:00:00Z",
        "EarliestRestorableTime": "2026-01-01T00:00:00Z",
        "LatestRestorableTime": "2026-01-01T00:00:00Z",
    }
    rds._clusters["cluster"] = cluster
    params = {} if apply is None else {"ApplyImmediately": apply}
    _modify_readback("sdk", **params, **dict.fromkeys(FLAGS, True))
    assert all(instance[flag] is (apply is True) for flag in FLAGS)
    assert instance["PendingModifiedValues"] == (
        {} if apply is True else dict.fromkeys(FLAGS, True)
    )
    assert all(cluster[flag] is False for flag in FLAGS)
    client = _sdk()
    changed = client.modify_db_cluster(
        DBClusterIdentifier="cluster", **params, **dict.fromkeys(FLAGS, True),
    )["DBCluster"]
    described = client.describe_db_clusters(DBClusterIdentifier="cluster")["DBClusters"][0]
    assert all(output[flag] is True for output in (changed, described) for flag in FLAGS)
