"""
Integration tests for EKS service emulator.
Tests cluster CRUD, nodegroup CRUD, tags, and CloudFormation provisioning.
k3s Docker container tests require Docker socket access.
"""
import json
import time
import uuid
import pytest
import boto3
from botocore.exceptions import ClientError


ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


@pytest.fixture(scope="module")
def eks():
    return boto3.client("eks", endpoint_url=ENDPOINT,
                        aws_access_key_id="test", aws_secret_access_key="test",
                        region_name=REGION)


@pytest.fixture(scope="module")
def cfn():
    return boto3.client("cloudformation", endpoint_url=ENDPOINT,
                        aws_access_key_id="test", aws_secret_access_key="test",
                        region_name=REGION)


def _uid():
    return uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# Cluster CRUD
# ---------------------------------------------------------------------------

def test_eks_create_describe_delete_cluster(eks):
    """Test EKS API contract: create → describe → delete → gone."""
    name = f"test-cluster-{_uid()}"
    resp = eks.create_cluster(
        name=name,
        version="1.30",
        roleArn="arn:aws:iam::000000000000:role/eks-role",
        resourcesVpcConfig={"subnetIds": ["subnet-1", "subnet-2"]},
    )
    cluster = resp["cluster"]
    assert cluster["name"] == name
    assert cluster["status"] in ("CREATING", "ACTIVE")
    assert cluster["version"] == "1.30"
    assert "arn" in cluster
    assert f"cluster/{name}" in cluster["arn"]
    assert "endpoint" in cluster
    assert "certificateAuthority" in cluster
    assert "identity" in cluster
    assert "oidc" in cluster["identity"]

    # Describe — wait for background thread to finish.
    # In CI the first describe can transiently fail; retry with backoff.
    resp = None
    for attempt in range(60):
        try:
            resp = eks.describe_cluster(name=name)
            if resp["cluster"]["status"] == "ACTIVE":
                break
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        time.sleep(0.5)
    assert resp is not None, f"Cluster {name} never became describable after 30s"
    assert resp["cluster"]["name"] == name
    assert resp["cluster"]["status"] in ("ACTIVE", "CREATING")

    # Delete
    resp = eks.delete_cluster(name=name)
    assert resp["cluster"]["name"] == name

    # Verify gone
    with pytest.raises(ClientError) as exc:
        eks.describe_cluster(name=name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_eks_create_duplicate_cluster(eks):
    name = f"dup-cluster-{_uid()}"
    eks.create_cluster(name=name, roleArn="arn:aws:iam::000000000000:role/r",
                       resourcesVpcConfig={})
    with pytest.raises(ClientError) as exc:
        eks.create_cluster(name=name, roleArn="arn:aws:iam::000000000000:role/r",
                           resourcesVpcConfig={})
    assert exc.value.response["Error"]["Code"] == "ResourceInUseException"
    eks.delete_cluster(name=name)


def test_eks_list_clusters(eks):
    name = f"list-cluster-{_uid()}"
    eks.create_cluster(name=name, roleArn="arn:aws:iam::000000000000:role/r",
                       resourcesVpcConfig={})
    resp = eks.list_clusters()
    assert name in resp["clusters"]
    eks.delete_cluster(name=name)


def test_eks_delete_nonexistent_cluster(eks):
    with pytest.raises(ClientError) as exc:
        eks.delete_cluster(name="nonexistent-cluster-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Nodegroup CRUD
# ---------------------------------------------------------------------------

def test_eks_create_describe_delete_nodegroup(eks):
    cluster = f"ng-cluster-{_uid()}"
    eks.create_cluster(name=cluster, roleArn="arn:aws:iam::000000000000:role/r",
                       resourcesVpcConfig={})
    ng_name = f"ng-{_uid()}"
    resp = eks.create_nodegroup(
        clusterName=cluster,
        nodegroupName=ng_name,
        scalingConfig={"minSize": 1, "maxSize": 3, "desiredSize": 2},
        instanceTypes=["t3.large"],
        nodeRole="arn:aws:iam::000000000000:role/node-role",
        subnets=["subnet-1"],
        diskSize=50,
    )
    ng = resp["nodegroup"]
    assert ng["nodegroupName"] == ng_name
    assert ng["clusterName"] == cluster
    assert ng["status"] == "ACTIVE"
    assert ng["scalingConfig"]["desiredSize"] == 2
    assert ng["instanceTypes"] == ["t3.large"]
    assert ng["diskSize"] == 50
    assert "nodegroupArn" in ng

    # Describe
    resp = eks.describe_nodegroup(clusterName=cluster, nodegroupName=ng_name)
    assert resp["nodegroup"]["nodegroupName"] == ng_name

    # List
    resp = eks.list_nodegroups(clusterName=cluster)
    assert ng_name in resp["nodegroups"]

    # Delete
    resp = eks.delete_nodegroup(clusterName=cluster, nodegroupName=ng_name)
    assert resp["nodegroup"]["status"] == "DELETING"

    # Verify gone
    with pytest.raises(ClientError) as exc:
        eks.describe_nodegroup(clusterName=cluster, nodegroupName=ng_name)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    eks.delete_cluster(name=cluster)


def test_eks_nodegroup_nonexistent_cluster(eks):
    with pytest.raises(ClientError) as exc:
        eks.create_nodegroup(clusterName="no-such-cluster", nodegroupName="ng1",
                             nodeRole="arn:aws:iam::000000000000:role/r",
                             subnets=["subnet-1"])
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_eks_delete_cluster_cascades_nodegroups(eks):
    cluster = f"cascade-{_uid()}"
    eks.create_cluster(name=cluster, roleArn="arn:aws:iam::000000000000:role/r",
                       resourcesVpcConfig={})
    for i in range(3):
        eks.create_nodegroup(clusterName=cluster, nodegroupName=f"ng-{i}",
                             nodeRole="arn:aws:iam::000000000000:role/r",
                             subnets=["subnet-1"])
    resp = eks.list_nodegroups(clusterName=cluster)
    assert len(resp["nodegroups"]) == 3

    eks.delete_cluster(name=cluster)

    with pytest.raises(ClientError):
        eks.list_nodegroups(clusterName=cluster)


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def test_eks_tag_cluster(eks):
    name = f"tag-cluster-{_uid()}"
    eks.create_cluster(name=name, roleArn="arn:aws:iam::000000000000:role/r",
                       resourcesVpcConfig={}, tags={"env": "test"})
    arn = eks.describe_cluster(name=name)["cluster"]["arn"]

    resp = eks.list_tags_for_resource(resourceArn=arn)
    assert resp["tags"]["env"] == "test"

    eks.tag_resource(resourceArn=arn, tags={"team": "platform"})
    resp = eks.list_tags_for_resource(resourceArn=arn)
    assert resp["tags"]["team"] == "platform"
    assert resp["tags"]["env"] == "test"

    eks.untag_resource(resourceArn=arn, tagKeys=["env"])
    resp = eks.list_tags_for_resource(resourceArn=arn)
    assert "env" not in resp["tags"]
    assert resp["tags"]["team"] == "platform"

    eks.delete_cluster(name=name)


# ---------------------------------------------------------------------------
# CloudFormation
# ---------------------------------------------------------------------------

def test_eks_cfn_cluster(cfn, eks):
    uid = _uid()
    cluster_name = f"cfn-eks-{uid}"
    template = json.dumps({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Cluster": {
                "Type": "AWS::EKS::Cluster",
                "Properties": {
                    "Name": cluster_name,
                    "Version": "1.30",
                    "RoleArn": "arn:aws:iam::000000000000:role/eks-role",
                    "ResourcesVpcConfig": {
                        "subnetIds": ["subnet-1", "subnet-2"],
                    },
                },
            },
        },
    })
    stack_name = f"eks-stack-{uid}"
    cfn.create_stack(StackName=stack_name, TemplateBody=template)

    # Poll for stack — deploy runs as an async task
    stack = None
    for _ in range(30):
        try:
            stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
            if stack["StackStatus"] not in ("CREATE_IN_PROGRESS",):
                break
        except Exception:
            pass
        time.sleep(1)
    assert stack is not None, f"Stack {stack_name} never appeared"
    assert stack["StackStatus"] == "CREATE_COMPLETE"

    resp = eks.describe_cluster(name=cluster_name)
    assert resp["cluster"]["name"] == cluster_name

    cfn.delete_stack(StackName=stack_name)
    time.sleep(2)
