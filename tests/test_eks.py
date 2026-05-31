"""
Integration tests for EKS service emulator.
Tests cluster CRUD, nodegroup CRUD, tags, and CloudFormation provisioning.
k3s Docker container tests require Docker socket access.
"""
import json
import time
import uuid

import boto3
import pytest
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


# -- k3s container run kwargs ----------------------------------------------
#
# Issue #611: k3s requires `--privileged` to remount /sys/fs/cgroup; without
# it the container exits on boot with "failed to evacuate root cgroup". The
# kwargs builder is unit-tested in isolation so this doesn't depend on Docker
# being available in CI.


def test_eks_k3s_run_kwargs_includes_privileged():
    """Regression for #611: k3s server mode needs privileged=True."""
    from ministack.services.eks import _k3s_run_kwargs

    kwargs = _k3s_run_kwargs(name="test-cluster", port=16443)

    assert kwargs["privileged"] is True, (
        "k3s requires privileged=True — without it the cgroup remount fails "
        "with 'failed to evacuate root cgroup' (issue #611)"
    )


def test_eks_k3s_run_kwargs_port_mapping():
    """The 6443 port mapping must be present (the issue report flagged this
    as missing — it wasn't, but lock it in so it stays present)."""
    from ministack.services.eks import _k3s_run_kwargs

    kwargs = _k3s_run_kwargs(name="test-cluster", port=16443)
    assert kwargs["ports"] == {"6443/tcp": 16443}


def test_eks_k3s_run_kwargs_network_optional():
    """`network` is set only when ms_network is provided."""
    from ministack.services.eks import _k3s_run_kwargs

    no_net = _k3s_run_kwargs(name="c1", port=16443)
    assert "network" not in no_net

    with_net = _k3s_run_kwargs(name="c1", port=16443, ms_network="ministack-net")
    assert with_net["network"] == "ministack-net"


def test_eks_k3s_run_kwargs_container_name_and_labels():
    """Each cluster's k3s container is named and labelled so `_stop_all_k3s`
    can find it. Lock the shape used by that lookup."""
    from ministack.services.eks import _k3s_run_kwargs

    kwargs = _k3s_run_kwargs(name="my-cluster", port=16443)
    assert kwargs["name"] == "ministack-eks-my-cluster"
    assert kwargs["labels"] == {"ministack": "eks", "cluster_name": "my-cluster"}


def test_eks_addon_lifecycle(eks):
    """CreateAddon / DescribeAddon / ListAddons / UpdateAddon / DeleteAddon.
    Issue #752: terraform aws_eks_addon fails on missing POST /clusters/{name}/addons."""
    import uuid as _uuid
    cn = f"addons-{_uuid.uuid4().hex[:8]}"
    eks.create_cluster(
        name=cn, roleArn="arn:aws:iam::000000000000:role/eks",
        resourcesVpcConfig={"subnetIds": ["subnet-1", "subnet-2"]},
    )
    try:
        # Create the 4 standard addons in one go.
        for name in ("vpc-cni", "coredns", "kube-proxy", "aws-ebs-csi-driver"):
            r = eks.create_addon(clusterName=cn, addonName=name)
            assert r["addon"]["addonName"] == name
            assert r["addon"]["status"] == "ACTIVE"
            assert f":addon/{cn}/{name}/" in r["addon"]["addonArn"]

        # Describe one.
        r = eks.describe_addon(clusterName=cn, addonName="coredns")
        assert r["addon"]["status"] == "ACTIVE"
        assert r["addon"]["addonName"] == "coredns"

        # List all.
        lst = eks.list_addons(clusterName=cn)["addons"]
        assert set(lst) == {"vpc-cni", "coredns", "kube-proxy", "aws-ebs-csi-driver"}

        # Update changes the version and surfaces a successful update record.
        upd = eks.update_addon(
            clusterName=cn, addonName="coredns",
            addonVersion="v1.11.4-eksbuild.1",
        )
        assert upd["update"]["status"] == "Successful"
        r = eks.describe_addon(clusterName=cn, addonName="coredns")
        assert r["addon"]["addonVersion"] == "v1.11.4-eksbuild.1"

        # Delete returns DELETING and the addon is gone afterwards.
        d = eks.delete_addon(clusterName=cn, addonName="vpc-cni")
        assert d["addon"]["status"] == "DELETING"
        with pytest.raises(ClientError) as e:
            eks.describe_addon(clusterName=cn, addonName="vpc-cni")
        assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        try: eks.delete_cluster(name=cn)
        except Exception: pass


def test_eks_addon_create_on_missing_cluster_404(eks):
    import uuid as _uuid
    missing = f"no-such-cluster-{_uuid.uuid4().hex[:6]}"
    with pytest.raises(ClientError) as e:
        eks.create_addon(clusterName=missing, addonName="vpc-cni")
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_eks_addon_create_duplicate_returns_resource_in_use(eks):
    import uuid as _uuid
    cn = f"addons-dup-{_uuid.uuid4().hex[:8]}"
    eks.create_cluster(
        name=cn, roleArn="arn:aws:iam::000000000000:role/eks",
        resourcesVpcConfig={"subnetIds": ["subnet-1"]},
    )
    try:
        eks.create_addon(clusterName=cn, addonName="vpc-cni")
        with pytest.raises(ClientError) as e:
            eks.create_addon(clusterName=cn, addonName="vpc-cni")
        assert e.value.response["Error"]["Code"] == "ResourceInUseException"
    finally:
        try: eks.delete_cluster(name=cn)
        except Exception: pass
