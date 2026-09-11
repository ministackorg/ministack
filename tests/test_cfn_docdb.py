"""CloudFormation + CDK support for AWS::DocDB::* resources.

Covers the two layers CDK deployments exercise:
- ``engine._resolve_dynamic_refs`` — CDK L2 assembles
  ``{{resolve:secretsmanager:...:SecretString:<key>::}}`` placeholders inside
  Fn::Join, so they resolve in a second pass after intrinsics.
- The DocDB provisioners — DBCluster/DBInstance/SubnetGroup/ParameterGroup and
  the SecretTargetAttachment linkage resource, deployed end-to-end with a
  CDK-shaped template (generated secret feeding MasterUserPassword).
"""

import json
import os
import socket
import time
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from ministack.services.cloudformation.engine import _resolve_dynamic_refs


def _endpoint():
    return os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")


def _client(service):
    return boto3.client(
        service,
        endpoint_url=_endpoint(),
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"mode": "standard"}),
    )


@pytest.fixture(scope="module")
def cfn():
    return _client("cloudformation")


@pytest.fixture(scope="module")
def docdb():
    """DocDB client routed via a ``docdb.``-prefixed hostname.

    DocumentDB shares RDS's SigV4 scope, so MiniStack dispatches on the Host
    header; the prefixed endpoint plus a getaddrinfo patch (mirroring
    conftest's docdb fixture) makes plain-localhost deployments reachable.
    """
    endpoint = _endpoint()
    host = urlparse(endpoint).hostname or "localhost"
    real_getaddrinfo = socket.getaddrinfo

    def _patched(h, port, *args, **kwargs):
        if isinstance(h, str) and h.endswith(f".{host}"):
            h = host
        return real_getaddrinfo(h, port, *args, **kwargs)

    socket.getaddrinfo = _patched
    try:
        yield boto3.client(
            "docdb",
            endpoint_url=endpoint.replace(f"//{host}", f"//docdb.{host}", 1),
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            config=Config(retries={"mode": "standard"}),
        )
    finally:
        socket.getaddrinfo = real_getaddrinfo


def _delete_stack_if_exists(cfn, name, timeout=60):
    try:
        cfn.describe_stacks(StackName=name)
    except ClientError as exc:
        if "does not exist" not in str(exc):
            raise
        return
    cfn.delete_stack(StackName=name)
    _wait_stack(cfn, name, timeout=timeout)

# ---------------------------------------------------------------------------
# Pure-function tests for the secretsmanager dynamic-reference pass
# ---------------------------------------------------------------------------

def test_dynref_bare_form_returns_whole_secret():
    from ministack.services import secretsmanager as sm
    sm._secrets["plain-secret"] = {
        "ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:plain-secret",
        "Name": "plain-secret", "DeletedDate": None,
        "Versions": {"v1": {"SecretString": "the-whole-string",
                            "Stages": ["AWSCURRENT"]}},
    }
    try:
        assert _resolve_dynamic_refs("{{resolve:secretsmanager:plain-secret}}") == "the-whole-string"
    finally:
        sm._secrets.pop("plain-secret", None)


def test_dynref_secretstring_key_with_arn():
    """CDK emits ARN-based refs whose colons must not break parsing."""
    from ministack.services import secretsmanager as sm
    arn = ("arn:aws:secretsmanager:us-east-1:000000000000:"
           "secret:docdb-master-AbCdEf")
    sm._secrets["docdb-master-AbCdEf"] = {
        "ARN": arn, "Name": "docdb-master-AbCdEf", "DeletedDate": None,
        "Versions": {"v1": {
            "SecretString": json.dumps({"username": "docdbadmin",
                                        "password": "s3cret"}),
            "Stages": ["AWSCURRENT"],
        }},
    }
    try:
        ref = f"{{{{resolve:secretsmanager:{arn}:SecretString:password::}}}}"
        assert _resolve_dynamic_refs(ref) == "s3cret"
        ref2 = f"{{{{resolve:secretsmanager:{arn}:SecretString:username::}}}}"
        assert _resolve_dynamic_refs(ref2) == "docdbadmin"
    finally:
        sm._secrets.pop("docdb-master-AbCdEf", None)


def test_dynref_inside_structure_and_caching():
    """Refs resolve inside nested structures and one secret is read once."""
    from ministack.services import secretsmanager as sm
    sm._secrets["struct-secret"] = {
        "ARN": "arn:test", "Name": "struct-secret", "DeletedDate": None,
        "Versions": {"v1": {"SecretString": json.dumps({"user": "admin", "pw": "x"}),
                            "Stages": ["AWSCURRENT"]}},
    }
    calls = []
    real = sm.resolve_secret_string

    def counting(sid, **kw):
        calls.append(sid)
        return real(sid, **kw)

    try:
        sm.resolve_secret_string = counting
        cache = {}
        props = {
            "MasterUsername": "{{resolve:secretsmanager:struct-secret:SecretString:user::}}",
            "Nested": [{"Pw": "{{resolve:secretsmanager:struct-secret:SecretString:pw::}}"}],
            "Untouched": "{{resolve:ssm:/some/param}}",
        }
        out = _resolve_dynamic_refs(props, cache)
        assert out["MasterUsername"] == "admin"
        assert out["Nested"][0]["Pw"] == "x"
        assert out["Untouched"] == "{{resolve:ssm:/some/param}}"
        # Two refs to the same secret → exactly one store read.
        assert calls == ["struct-secret"]
    finally:
        sm.resolve_secret_string = real
        sm._secrets.pop("struct-secret", None)


def test_dynref_missing_key_raises():
    from ministack.services import secretsmanager as sm
    sm._secrets["tiny-secret"] = {
        "ARN": "arn:test", "Name": "tiny-secret", "DeletedDate": None,
        "Versions": {"v1": {"SecretString": "{}", "Stages": ["AWSCURRENT"]}},
    }
    try:
        with pytest.raises(ValueError, match="not.*found.*in secret"):
            _resolve_dynamic_refs(
                "{{resolve:secretsmanager:tiny-secret:SecretString:nope::}}")
    finally:
        sm._secrets.pop("tiny-secret", None)


def test_dynref_missing_secret_raises():
    with pytest.raises(ValueError, match="could not resolve secret"):
        _resolve_dynamic_refs("{{resolve:secretsmanager:no-such-secret-xyz}}")


def test_dynref_unknown_service_untouched():
    val = {"A": "{{resolve:ssm:my-param}}"}
    assert _resolve_dynamic_refs(val)["A"] == "{{resolve:ssm:my-param}}"


# ---------------------------------------------------------------------------
# End-to-end deploy of a CDK-shaped DocumentDB stack
# ---------------------------------------------------------------------------

def _wait_stack(cfn, name, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            stacks = cfn.describe_stacks(StackName=name)["Stacks"]
        except ClientError as exc:
            if "does not exist" in str(exc):
                return {"StackStatus": "DELETE_COMPLETE", "StackName": name}
            raise
        status = stacks[0]["StackStatus"]
        if not status.endswith("_IN_PROGRESS"):
            return stacks[0]
        time.sleep(0.5)
    raise TimeoutError(f"Stack {name} stuck at {status}")


CDK_SHAPE_TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Resources": {
        "DocDBSubnets": {
            "Type": "AWS::DocDB::DBSubnetGroup",
            "Properties": {
                "DBSubnetGroupDescription": "demo subnets",
                "SubnetIds": ["subnet-aaaa", "subnet-bbbb"],
            },
        },
        # CDK L2 generates the master secret with GenerateSecretString +
        # SecretStringTemplate, then Fn::Joins dynamic references to it into
        # the cluster's MasterUsername / MasterUserPassword.
        "DocDBMasterSecret": {
            "Type": "AWS::SecretsManager::Secret",
            "Properties": {
                "Description": "Master user credentials",
                "GenerateSecretString": {
                    "SecretStringTemplate": "{\"username\": \"docdbadmin\"}",
                    "GenerateStringKey": "password",
                    "ExcludeCharacters": "\"@/\\",
                },
            },
        },
        "DocDBCluster": {
            "Type": "AWS::DocDB::DBCluster",
            "Properties": {
                "EngineVersion": "5.0.0",
                "DBSubnetGroupName": {"Ref": "DocDBSubnets"},
                "ManageMasterUserPassword": True,
                "MasterUsername": {
                    "Fn::Join": ["", [
                        "{{resolve:secretsmanager:",
                        {"Ref": "DocDBMasterSecret"},
                        ":SecretString:username::}}",
                    ]],
                },
                "MasterUserPassword": {
                    "Fn::Join": ["", [
                        "{{resolve:secretsmanager:",
                        {"Ref": "DocDBMasterSecret"},
                        ":SecretString:password::}}",
                    ]],
                },
            },
        },
        "DocDBInstance": {
            "Type": "AWS::DocDB::DBInstance",
            "Properties": {
                "DBClusterIdentifier": {"Ref": "DocDBCluster"},
                "DBInstanceClass": "db.t3.medium",
                "Engine": "docdb",
            },
        },
        "SecretAttachment": {
            "Type": "AWS::SecretsManager::SecretTargetAttachment",
            "Properties": {
                "SecretId": {"Ref": "DocDBMasterSecret"},
                "TargetId": {"Ref": "DocDBCluster"},
                "TargetType": "AWS::DocDB::DBCluster",
            },
        },
    },
    "Outputs": {
        "ClusterEndpointPort": {"Value": {"Fn::GetAtt": "DocDBCluster.Endpoint.Port"}},
        "InstanceEndpointAddress": {
            "Value": {"Fn::GetAtt": "DocDBInstance.Endpoint.Address"}},
    },
}


def test_cfn_docdb_cdk_shape_stack_lifecycle(cfn, docdb):
    stack_name = "cfn-docdb-cdk-shape"
    _delete_stack_if_exists(cfn, stack_name)
    cfn.create_stack(StackName=stack_name,
                     TemplateBody=json.dumps(CDK_SHAPE_TEMPLATE))
    result = _wait_stack(cfn, stack_name)
    assert result["StackStatus"] == "CREATE_COMPLETE", result.get("StackStatusReason")

    outputs = {o["OutputKey"]: o["OutputValue"] for o in result.get("Outputs", [])}
    assert outputs["ClusterEndpointPort"] in ("27017",) or int(outputs["ClusterEndpointPort"]) >= 27117
    assert outputs["InstanceEndpointAddress"]

    mine = [c for c in docdb.describe_db_clusters()["DBClusters"]
            if c["DBClusterIdentifier"].startswith(stack_name)]
    assert len(mine) == 1
    assert mine[0]["Status"] == "available"
    assert mine[0]["MasterUsername"] == "docdbadmin"

    members = [i for i in docdb.describe_db_instances()["DBInstances"]
               if i["DBInstanceIdentifier"].startswith(stack_name)]
    assert len(members) == 1
    port = members[0]["Endpoint"]["Port"]
    assert port == 27017 or port >= 27117

    # Stack delete removes instance -> cluster -> containers.
    cfn.delete_stack(StackName=stack_name)
    done = _wait_stack(cfn, stack_name)
    assert done["StackStatus"] == "DELETE_COMPLETE"
    remaining = [c for c in docdb.describe_db_clusters()["DBClusters"]
                 if c["DBClusterIdentifier"].startswith(stack_name)]
    assert not remaining


def test_cfn_docdb_cluster_master_user_secret_passthrough(cfn, docdb):
    """Explicit MasterUserSecretArn surfaces as DescribeDBClusters.MasterUserSecret."""
    template = {
        "Resources": {
            "Cluster": {
                "Type": "AWS::DocDB::DBCluster",
                "Properties": {
                    "EngineVersion": "8.0.0",
                    "MasterUsername": "root",
                    "MasterUserPassword": "password123",
                    "MasterUserSecretArn":
                        "arn:aws:secretsmanager:us-east-1:000000000000:secret:linked",
                },
            },
        },
    }
    stack_name = "cfn-docdb-musecret"
    _delete_stack_if_exists(cfn, stack_name)
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    result = _wait_stack(cfn, stack_name)
    assert result["StackStatus"] == "CREATE_COMPLETE", result.get("StackStatusReason")

    clusters = docdb.describe_db_clusters()["DBClusters"]
    mine = next(c for c in clusters if c["DBClusterIdentifier"].startswith(stack_name))
    resp = docdb.describe_db_clusters(DBClusterIdentifier=mine["DBClusterIdentifier"])
    secret = resp["DBClusters"][0].get("MasterUserSecret")
    assert secret and secret["SecretArn"].endswith(":linked")
    assert secret["SecretStatus"] == "active"


def test_cfn_docdb_bad_engine_version_fails_readably(cfn):
    template = {
        "Resources": {
            "Cluster": {
                "Type": "AWS::DocDB::DBCluster",
                "Properties": {
                    "EngineVersion": "6.0.0",
                    "MasterUsername": "root",
                    "MasterUserPassword": "password123",
                },
            },
        },
    }
    stack_name = "cfn-docdb-badversion"
    _delete_stack_if_exists(cfn, stack_name)
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    result = _wait_stack(cfn, stack_name)
    assert result["StackStatus"] == "ROLLBACK_COMPLETE"
    reason = result.get("StackStatusReason") or ""
    events = cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
    failed = [e for e in events if e.get("ResourceStatusReason")]
    reasons = " ".join(e["ResourceStatusReason"] for e in failed)
    assert "InvalidParameterCombination" in reasons or "InvalidParameterCombination" in reason
