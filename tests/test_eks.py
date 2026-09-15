"""
Integration tests for EKS service emulator.
Tests cluster CRUD, nodegroup CRUD, tags, and CloudFormation provisioning.
k3s Docker container tests require Docker socket access.
"""
import asyncio
import base64
import datetime as dt
import json
import os
import threading
import time
import uuid
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import quote

import boto3
import pytest
from botocore.auth import SigV4Auth, SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.exceptions import ClientError

import ministack.app as app
from ministack.core.responses import AccountRegionScopedDict, AccountScopedDict, request_scope
from ministack.services import eks as eks_service
from ministack.services import iam, sts

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
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


@pytest.fixture
def eks_mod(monkeypatch):
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import eks as eks_service

    monkeypatch.setattr(eks_service, "_get_docker", lambda: None)
    set_request_account_id("000000000000")
    set_request_region(REGION)
    eks_service.reset()
    yield eks_service
    eks_service.reset()


def _eks_direct(eks_service, method, path, body=None, query=None):
    payload = json.dumps(body or {}).encode("utf-8") if body is not None else b""
    status, headers, raw_body = asyncio.run(
        eks_service.handle_request(method, path, {}, payload, query or {})
    )
    if raw_body:
        parsed_body = json.loads(raw_body.decode("utf-8"))
    else:
        parsed_body = {}
    return status, headers, parsed_body


def _eks_direct_create_cluster(eks_service, name):
    status, _headers, body = _eks_direct(
        eks_service,
        "POST",
        "/clusters",
        {
            "name": name,
            "roleArn": "arn:aws:iam::000000000000:role/eks-role",
            "resourcesVpcConfig": {},
        },
    )
    assert status == 200
    return body["cluster"]["arn"]


def _eks_exec_token(cluster_name, access_key="test", secret_key="test"):
    request = AWSRequest(
        method="GET",
        url="https://sts.us-east-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
        headers={"x-k8s-aws-id": cluster_name},
    )
    SigV4QueryAuth(
        Credentials(access_key, secret_key), "sts", "us-east-1", expires=60
    ).add_auth(request)
    encoded = base64.urlsafe_b64encode(request.url.encode()).decode().rstrip("=")
    return "k8s-aws-v1." + encoded


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


def test_eks_resources_are_region_scoped_direct(eks_mod):
    """Same-name clusters and children remain independent across regions."""
    from ministack.core.responses import set_request_region

    cluster_name = f"regional-{_uid()}"
    nodegroup_name = "workers"
    addon_name = "vpc-cni"
    idp_name = "regional-idp"
    principal_arn = "arn:aws:iam::000000000000:role/regional-access"
    policy_arn = (
        "arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy"
    )
    regions = {
        "us-east-1": "east",
        "us-west-2": "west",
    }
    cluster_arns = {}

    for region, marker in regions.items():
        set_request_region(region)
        cluster_arn = _eks_direct_create_cluster(eks_mod, cluster_name)
        cluster_arns[region] = cluster_arn
        assert f":{region}:" in cluster_arn

        status, _headers, _body = _eks_direct(
            eks_mod,
            "POST",
            f"/clusters/{cluster_name}/node-groups",
            {
                "nodegroupName": nodegroup_name,
                "nodeRole": "arn:aws:iam::000000000000:role/node-role",
                "subnets": ["subnet-1"],
            },
        )
        assert status == 200

        status, _headers, _body = _eks_direct(
            eks_mod,
            "POST",
            f"/clusters/{cluster_name}/addons",
            {"addonName": addon_name},
        )
        assert status == 200

        status, _headers, _body = _eks_direct(
            eks_mod,
            "POST",
            f"/clusters/{cluster_name}/access-entries",
            {"principalArn": principal_arn},
        )
        assert status == 200

        encoded_principal = quote(principal_arn, safe="")
        status, _headers, _body = _eks_direct(
            eks_mod,
            "POST",
            (
                f"/clusters/{cluster_name}/access-entries/"
                f"{encoded_principal}/access-policies"
            ),
            {
                "policyArn": policy_arn,
                "accessScope": {"type": "cluster", "namespaces": []},
            },
        )
        assert status == 200

        status, _headers, _body = _eks_direct(
            eks_mod,
            "POST",
            f"/clusters/{cluster_name}/identity-provider-configs/associate",
            {
                "oidc": {
                    "identityProviderConfigName": idp_name,
                    "issuerUrl": f"https://{marker}.example/issuer",
                    "clientId": f"{marker}-client",
                },
            },
        )
        assert status == 200

        status, _headers, _body = _eks_direct(
            eks_mod,
            "POST",
            f"/tags/{quote(cluster_arn, safe='')}",
            {"tags": {"region": marker}},
        )
        assert status == 200

    for region, marker in regions.items():
        set_request_region(region)
        status, _headers, body = _eks_direct(
            eks_mod, "GET", f"/clusters/{cluster_name}"
        )
        assert status == 200
        assert body["cluster"]["arn"] == cluster_arns[region]

        status, _headers, body = _eks_direct(
            eks_mod, "GET", f"/clusters/{cluster_name}/node-groups"
        )
        assert status == 200
        assert body["nodegroups"] == [nodegroup_name]

        status, _headers, body = _eks_direct(
            eks_mod, "GET", f"/clusters/{cluster_name}/addons"
        )
        assert status == 200
        assert body["addons"] == [addon_name]

        status, _headers, body = _eks_direct(
            eks_mod, "GET", f"/clusters/{cluster_name}/access-entries"
        )
        assert status == 200
        assert body["accessEntries"] == [principal_arn]

        status, _headers, body = _eks_direct(
            eks_mod,
            "GET",
            (
                f"/clusters/{cluster_name}/access-entries/"
                f"{quote(principal_arn, safe='')}/access-policies"
            ),
        )
        assert status == 200
        assert [policy["policyArn"] for policy in body["associatedAccessPolicies"]] == [
            policy_arn
        ]

        status, _headers, body = _eks_direct(
            eks_mod,
            "POST",
            f"/clusters/{cluster_name}/identity-provider-configs/describe",
            {"identityProviderConfig": {"type": "oidc", "name": idp_name}},
        )
        assert status == 200
        oidc = body["identityProviderConfig"]["oidc"]
        assert oidc["clientId"] == f"{marker}-client"

        status, _headers, body = _eks_direct(
            eks_mod,
            "GET",
            f"/tags/{quote(cluster_arns[region], safe='')}",
        )
        assert status == 200
        assert body["tags"] == {"region": marker}

    set_request_region("us-east-1")
    status, _headers, _body = _eks_direct(
        eks_mod, "DELETE", f"/clusters/{cluster_name}"
    )
    assert status == 200

    set_request_region("us-west-2")
    status, _headers, body = _eks_direct(
        eks_mod, "GET", f"/clusters/{cluster_name}"
    )
    assert status == 200
    assert body["cluster"]["arn"] == cluster_arns["us-west-2"]
    status, _headers, body = _eks_direct(
        eks_mod, "GET", f"/clusters/{cluster_name}/node-groups"
    )
    assert status == 200
    assert body["nodegroups"] == [nodegroup_name]


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


def test_eks_tag_resource_accepts_supported_local_arn_shapes_direct(eks_mod):
    cluster = f"tag-shapes-{_uid()}"
    cluster_arn = _eks_direct_create_cluster(eks_mod, cluster)

    status, _headers, body = _eks_direct(
        eks_mod,
        "POST",
        f"/clusters/{cluster}/node-groups",
        {
            "nodegroupName": "workers",
            "nodeRole": "arn:aws:iam::000000000000:role/node-role",
            "subnets": ["subnet-1"],
        },
    )
    assert status == 200
    nodegroup_arn = body["nodegroup"]["nodegroupArn"]

    status, _headers, body = _eks_direct(
        eks_mod,
        "POST",
        f"/clusters/{cluster}/addons",
        {"addonName": "vpc-cni"},
    )
    assert status == 200
    addon_arn = body["addon"]["addonArn"]

    principal = "arn:aws:iam::000000000000:role/eks-access"
    status, _headers, body = _eks_direct(
        eks_mod,
        "POST",
        f"/clusters/{cluster}/access-entries",
        {"principalArn": principal},
    )
    assert status == 200
    access_entry_arn = body["accessEntry"]["accessEntryArn"]

    status, _headers, _body = _eks_direct(
        eks_mod,
        "POST",
        f"/clusters/{cluster}/identity-provider-configs/associate",
        {
            "oidc": {
                "identityProviderConfigName": "tag-idp",
                "issuerUrl": "https://example/issuer",
                "clientId": "client-1",
            },
        },
    )
    assert status == 200
    status, _headers, body = _eks_direct(
        eks_mod,
        "POST",
        f"/clusters/{cluster}/identity-provider-configs/describe",
        {"identityProviderConfig": {"type": "oidc", "name": "tag-idp"}},
    )
    assert status == 200
    idp_arn = body["identityProviderConfig"]["oidc"]["identityProviderConfigArn"]

    for arn in (cluster_arn, nodegroup_arn, addon_arn, access_entry_arn, idp_arn):
        path_arn = quote(arn, safe="") if arn == cluster_arn else arn
        status, _headers, body = _eks_direct(
            eks_mod,
            "POST",
            f"/tags/{path_arn}",
            {"tags": {"scope": "local"}},
        )
        assert status == 200
        assert body == {}

        status, _headers, body = _eks_direct(eks_mod, "GET", f"/tags/{path_arn}")
        assert status == 200
        assert body["tags"]["scope"] == "local"
        assert eks_mod._tags.get(arn) == {"scope": "local"}


def test_eks_tag_apis_reject_invalid_resource_arns_before_tags_direct(eks_mod):
    cluster = f"tag-invalid-{_uid()}"
    cluster_arn = _eks_direct_create_cluster(eks_mod, cluster)
    _eks_direct(eks_mod, "POST", f"/tags/{cluster_arn}", {"tags": {"existing": "tag"}})
    existing_tags = dict(eks_mod._tags.items())

    invalid_arns = [
        "not-an-arn",
        cluster_arn.replace("arn:aws:", "arn:aws-cn:"),
        cluster_arn.replace(":eks:", ":sqs:"),
        cluster_arn.replace(":000000000000:", ":111111111111:"),
        cluster_arn.replace(f":{REGION}:", ":us-west-2:"),
        f"{cluster_arn}/extra",
        f"arn:aws:eks:{REGION}:000000000000:fargateprofile/{cluster}/fp/abc123",
    ]

    for arn in invalid_arns:
        for method, request_body, query in (
            ("GET", None, None),
            ("POST", {"tags": {"bad": "tag"}}, None),
            ("DELETE", None, {"tagKeys": "existing"}),
        ):
            status, _headers, body = _eks_direct(
                eks_mod,
                method,
                f"/tags/{arn}",
                request_body,
                query,
            )
            assert status == 400
            assert body["__type"] == "InvalidParameterException"
            assert dict(eks_mod._tags.items()) == existing_tags


def test_eks_tag_apis_reject_missing_local_resources_before_tags_direct(eks_mod):
    cluster = f"tag-missing-{_uid()}"
    cluster_arn = _eks_direct_create_cluster(eks_mod, cluster)
    _eks_direct(eks_mod, "POST", f"/tags/{cluster_arn}", {"tags": {"existing": "tag"}})
    existing_tags = dict(eks_mod._tags.items())
    missing_arn = f"arn:aws:eks:{REGION}:000000000000:cluster/no-such-cluster"

    for method, request_body, query in (
        ("GET", None, None),
        ("POST", {"tags": {"bad": "tag"}}, None),
        ("DELETE", None, {"tagKeys": "existing"}),
    ):
        status, _headers, body = _eks_direct(
            eks_mod,
            method,
            f"/tags/{missing_arn}",
            request_body,
            query,
        )
        assert status == 404
        assert body["__type"] == "ResourceNotFoundException"
        assert dict(eks_mod._tags.items()) == existing_tags


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

    kwargs = _k3s_run_kwargs(name="test-cluster", region=REGION, port=16443)

    assert kwargs["privileged"] is True, (
        "k3s requires privileged=True — without it the cgroup remount fails "
        "with 'failed to evacuate root cgroup' (issue #611)"
    )


def test_eks_k3s_run_kwargs_port_mapping():
    """The 6443 port mapping must be present (the issue report flagged this
    as missing — it wasn't, but lock it in so it stays present)."""
    from ministack.services.eks import _k3s_run_kwargs

    kwargs = _k3s_run_kwargs(name="test-cluster", region=REGION, port=16443)
    assert kwargs["ports"] == {"6443/tcp": 16443}


def test_eks_k3s_run_kwargs_network_optional():
    """`network` is set only when ms_network is provided."""
    from ministack.services.eks import _k3s_run_kwargs

    no_net = _k3s_run_kwargs(name="c1", region=REGION, port=16443)
    assert "network" not in no_net

    with_net = _k3s_run_kwargs(
        name="c1", region=REGION, port=16443, ms_network="ministack-net"
    )
    assert with_net["network"] == "ministack-net"


def test_eks_k3s_run_kwargs_container_name_and_labels_are_region_scoped():
    """Same-name clusters in different regions need distinct containers."""
    from ministack.services.eks import _k3s_run_kwargs

    east = _k3s_run_kwargs(name="my-cluster", region="us-east-1", port=16443)
    west = _k3s_run_kwargs(name="my-cluster", region="us-west-2", port=16444)

    assert east["name"] == "ministack-eks-us-east-1-my-cluster"
    assert west["name"] == "ministack-eks-us-west-2-my-cluster"
    # Subset, not equality: containers also carry the ownership labels
    # (`ministack.instance` / `ministack.boot`) that scope reaping to this
    # MiniStack. This test is about region scoping, so it asserts only that.
    assert {
        "ministack": "eks",
        "cluster_name": "my-cluster",
        "region": "us-east-1",
    }.items() <= east["labels"].items()
    assert west["labels"]["region"] == "us-west-2"


def test_eks_k3s_run_kwargs_host_gateway_extra_host():
    """The k3s node must be able to reach a host-run MiniStack for ECR
    registry mirroring (#1054) — host.docker.internal via host-gateway."""
    from ministack.services.eks import _k3s_run_kwargs

    kwargs = _k3s_run_kwargs(name="c1", region=REGION, port=16443)
    assert kwargs["extra_hosts"] == {"host.docker.internal": "host-gateway"}


def test_eks_ecr_registry_hosts_from_cluster_arn():
    """ECR mirror hostnames derive from the cluster ARN, not contextvars,
    so restore/restart paths (no request context) behave like create (#1054)."""
    from ministack.services.eks import _ecr_registry_hosts

    cluster = {"arn": "arn:aws:eks:eu-west-1:123456789012:cluster/my-cluster"}
    assert _ecr_registry_hosts(cluster) == ["123456789012.dkr.ecr.eu-west-1.amazonaws.com"]
    assert _ecr_registry_hosts({"arn": "not-an-arn"}) == []
    assert _ecr_registry_hosts({}) == []


def test_eks_k3s_registries_yaml_shape(monkeypatch):
    """registries.yaml maps the cluster's ECR hostname to the gateway; with
    no shared network the endpoint goes through host.docker.internal (#1054)."""
    from ministack.services.eks import _k3s_registries_yaml

    monkeypatch.delenv("GATEWAY_PORT", raising=False)
    monkeypatch.delenv("EDGE_PORT", raising=False)
    yaml_bytes = _k3s_registries_yaml(
        None, None, ["123456789012.dkr.ecr.eu-west-1.amazonaws.com"])
    text = yaml_bytes.decode()
    assert text == (
        "mirrors:\n"
        '  "123456789012.dkr.ecr.eu-west-1.amazonaws.com":\n'
        "    endpoint:\n"
        '      - "http://host.docker.internal:4566"\n'
    )
    assert _k3s_registries_yaml(None, None, []) is None


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
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


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
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# AssociateEncryptionConfig
# ---------------------------------------------------------------------------

def test_eks_associate_encryption_config(eks):
    cn = f"enc-{_uid()}"
    key_arn = f"arn:aws:kms:{REGION}:000000000000:key/{uuid.uuid4()}"
    eks.create_cluster(
        name=cn, roleArn="arn:aws:iam::000000000000:role/eks",
        resourcesVpcConfig={"subnetIds": ["subnet-1"]},
    )
    try:
        resp = eks.associate_encryption_config(
            clusterName=cn,
            encryptionConfig=[{"resources": ["secrets"], "provider": {"keyArn": key_arn}}],
        )
        upd = resp["update"]
        assert upd["type"] == "AssociateEncryptionConfig"
        assert upd["status"] == "Successful"
        assert upd["id"]
        desc = eks.describe_cluster(name=cn)["cluster"]
        assert desc["encryptionConfig"][0]["provider"]["keyArn"] == key_arn
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_associate_encryption_config_missing_cluster(eks):
    with pytest.raises(ClientError) as e:
        eks.associate_encryption_config(
            clusterName=f"nope-{_uid()}",
            encryptionConfig=[{"resources": ["secrets"],
                               "provider": {"keyArn": "arn:aws:kms:us-east-1:000000000000:key/x"}}],
        )
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_eks_associate_encryption_config_already_set(eks):
    cn = f"enc-dup-{_uid()}"
    cfg = [{"resources": ["secrets"],
            "provider": {"keyArn": f"arn:aws:kms:{REGION}:000000000000:key/{uuid.uuid4()}"}}]
    eks.create_cluster(
        name=cn, roleArn="arn:aws:iam::000000000000:role/eks",
        resourcesVpcConfig={"subnetIds": ["subnet-1"]},
        encryptionConfig=cfg,
    )
    try:
        with pytest.raises(ClientError) as e:
            eks.associate_encryption_config(clusterName=cn, encryptionConfig=cfg)
        assert e.value.response["Error"]["Code"] == "InvalidRequestException"
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# OIDC discovery / JWKS (IRSA)
# ---------------------------------------------------------------------------

def test_eks_oidc_issuer_is_ministack_hosted(eks):
    cn = f"oidc-{_uid()}"
    eks.create_cluster(
        name=cn, roleArn="arn:aws:iam::000000000000:role/eks",
        resourcesVpcConfig={"subnetIds": ["subnet-1"]},
    )
    try:
        issuer = eks.describe_cluster(name=cn)["cluster"]["identity"]["oidc"]["issuer"]
        # Must be reachable from clients — points at ministack, not real AWS.
        assert issuer.startswith("http://"), issuer
        assert "/oidc/id/" in issuer, issuer
        assert "amazonaws.com" not in issuer, issuer
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_oidc_discovery_document(eks):
    import urllib.request
    cn = f"oidc-disc-{_uid()}"
    eks.create_cluster(
        name=cn, roleArn="arn:aws:iam::000000000000:role/eks",
        resourcesVpcConfig={"subnetIds": ["subnet-1"]},
    )
    try:
        issuer = eks.describe_cluster(name=cn)["cluster"]["identity"]["oidc"]["issuer"]
        with urllib.request.urlopen(f"{issuer}/.well-known/openid-configuration") as r:
            doc = json.loads(r.read())
        assert doc["issuer"] == issuer
        assert doc["jwks_uri"] == f"{issuer}/keys"
        assert "RS256" in doc["id_token_signing_alg_values_supported"]
        # JWKS must also be reachable and contain at least one RSA signing key.
        with urllib.request.urlopen(doc["jwks_uri"]) as r:
            jwks = json.loads(r.read())
        assert jwks["keys"]
        assert jwks["keys"][0]["kty"] == "RSA"
        assert jwks["keys"][0]["use"] == "sig"
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_oidc_issuer_scheme_https_when_tls(monkeypatch):
    """With USE_SSL=1 the gateway serves TLS, so the advertised OIDC issuer and
    the discovery document both report https (terraform's
    aws_iam_openid_connect_provider rejects non-https urls). Called in-process."""
    from ministack.services import eks as eks_svc

    monkeypatch.setenv("USE_SSL", "1")
    oidc_id = eks_svc._new_oidc_id()
    issuer = eks_svc._issuer_url(oidc_id)
    assert issuer.startswith("https://"), issuer
    assert "/oidc/id/" in issuer, issuer

    status, _headers, body = eks_svc._oidc_discovery(oidc_id)
    assert status == 200
    doc = json.loads(body)
    assert doc["issuer"] == issuer
    assert doc["jwks_uri"] == f"{issuer}/keys"


def test_eks_oidc_issuer_scheme_http_without_tls(monkeypatch):
    """Default (no TLS) keeps http, matching what the plain-http gateway serves."""
    from ministack.services import eks as eks_svc

    monkeypatch.delenv("USE_SSL", raising=False)
    assert eks_svc._ministack_issuer_base().startswith("http://")


# ---------------------------------------------------------------------------
# Access Entries — modern EKS IAM bindings (replace aws-auth ConfigMap).
# Crossplane / Terraform `aws_eks_access_entry` + `aws_eks_access_policy_association`
# both flow through these APIs.
# ---------------------------------------------------------------------------


def _create_basic_cluster(eks):
    cn = f"ae-{_uid()}"
    eks.create_cluster(
        name=cn, roleArn="arn:aws:iam::000000000000:role/eks",
        resourcesVpcConfig={"subnetIds": ["subnet-1"]},
    )
    return cn


def test_eks_pod_identity_association_lifecycle(eks):
    """EKS Pod Identity: the five operations a controller reading pod identity
    needs. Create returns the full association, List the six-field summary,
    Describe the record, Update the mutable members, and Delete answers with
    the association it removed."""
    cn = _create_basic_cluster(eks)
    role = f"arn:aws:iam::000000000000:role/lbc-{_uid()}"
    try:
        created = eks.create_pod_identity_association(
            clusterName=cn, namespace="kube-system",
            serviceAccount="aws-load-balancer-controller",
            roleArn=role, tags={"team": "platform"},
        )["association"]
        assoc_id = created["associationId"]
        assert created["clusterName"] == cn
        assert created["namespace"] == "kube-system"
        assert created["serviceAccount"] == "aws-load-balancer-controller"
        assert created["roleArn"] == role
        assert created["tags"] == {"team": "platform"}
        assert created["associationArn"] == (
            f"arn:aws:eks:{REGION}:000000000000:podidentityassociation/{cn}/{assoc_id}")
        assert created["createdAt"] == created["modifiedAt"]

        described = eks.describe_pod_identity_association(
            clusterName=cn, associationId=assoc_id)["association"]
        assert described["roleArn"] == role

        # The list shape is the summary, not the whole association.
        listed = eks.list_pod_identity_associations(clusterName=cn)["associations"]
        assert len(listed) == 1
        assert set(listed[0]) == {
            "clusterName", "namespace", "serviceAccount",
            "associationArn", "associationId", "ownerArn",
        }

        updated = eks.update_pod_identity_association(
            clusterName=cn, associationId=assoc_id, roleArn=role + "-v2")["association"]
        assert updated["roleArn"] == role + "-v2"
        # namespace and serviceAccount are not members of the update request.
        assert updated["namespace"] == "kube-system"
        assert updated["serviceAccount"] == "aws-load-balancer-controller"

        deleted = eks.delete_pod_identity_association(
            clusterName=cn, associationId=assoc_id)["association"]
        assert deleted["associationId"] == assoc_id
        assert eks.list_pod_identity_associations(clusterName=cn)["associations"] == []
        with pytest.raises(ClientError) as e:
            eks.describe_pod_identity_association(clusterName=cn, associationId=assoc_id)
        assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_pod_identity_associations_go_with_the_cluster(eks):
    """A recreated cluster must not inherit the previous one's associations."""
    cn = _create_basic_cluster(eks)
    eks.create_pod_identity_association(
        clusterName=cn, namespace="default", serviceAccount="app",
        roleArn=f"arn:aws:iam::000000000000:role/pod-{_uid()}",
    )
    assert eks.list_pod_identity_associations(clusterName=cn)["associations"]

    eks.delete_cluster(name=cn)
    eks.create_cluster(name=cn, roleArn="arn:aws:iam::000000000000:role/eks",
                       resourcesVpcConfig={"subnetIds": ["subnet-1"]})
    try:
        assert eks.list_pod_identity_associations(clusterName=cn)["associations"] == []
    finally:
        eks.delete_cluster(name=cn)


def test_eks_pod_identity_association_list_filters(eks):
    """ListPodIdentityAssociations filters on namespace and serviceAccount,
    the two query parameters the operation takes."""
    cn = _create_basic_cluster(eks)
    role = "arn:aws:iam::000000000000:role/r"
    try:
        eks.create_pod_identity_association(
            clusterName=cn, namespace="kube-system", serviceAccount="lbc", roleArn=role)
        eks.create_pod_identity_association(
            clusterName=cn, namespace="apps", serviceAccount="web", roleArn=role)

        by_ns = eks.list_pod_identity_associations(
            clusterName=cn, namespace="apps")["associations"]
        assert [a["serviceAccount"] for a in by_ns] == ["web"]
        by_sa = eks.list_pod_identity_associations(
            clusterName=cn, serviceAccount="lbc")["associations"]
        assert [a["namespace"] for a in by_sa] == ["kube-system"]
        assert len(eks.list_pod_identity_associations(clusterName=cn)["associations"]) == 2
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_pod_identity_association_duplicate_and_missing(eks):
    """One association per namespace and service account, and an unknown
    cluster or association id is ResourceNotFoundException."""
    cn = _create_basic_cluster(eks)
    role = "arn:aws:iam::000000000000:role/r"
    try:
        eks.create_pod_identity_association(
            clusterName=cn, namespace="kube-system", serviceAccount="dup", roleArn=role)
        with pytest.raises(ClientError) as e:
            eks.create_pod_identity_association(
                clusterName=cn, namespace="kube-system", serviceAccount="dup", roleArn=role)
        assert e.value.response["Error"]["Code"] == "ResourceInUseException"

        with pytest.raises(ClientError) as e:
            eks.describe_pod_identity_association(clusterName=cn, associationId="a-nope")
        assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"

        with pytest.raises(ClientError) as e:
            eks.list_pod_identity_associations(clusterName=f"no-such-{_uid()}")
        assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"

        with pytest.raises(ClientError) as e:
            eks.create_pod_identity_association(
                clusterName=f"no-such-{_uid()}", namespace="n",
                serviceAccount="s", roleArn=role)
        assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_pod_identity_association_target_role_gets_an_external_id(eks):
    """A target role makes the association role-chaining, and AWS mints the
    externalId its trust policy matches on."""
    cn = _create_basic_cluster(eks)
    try:
        assoc = eks.create_pod_identity_association(
            clusterName=cn, namespace="kube-system", serviceAccount="chained",
            roleArn="arn:aws:iam::000000000000:role/source",
            targetRoleArn="arn:aws:iam::000000000000:role/target",
            disableSessionTags=True,
        )["association"]
        assert assoc["targetRoleArn"] == "arn:aws:iam::000000000000:role/target"
        assert assoc["externalId"]
        assert assoc["disableSessionTags"] is True

        plain = eks.create_pod_identity_association(
            clusterName=cn, namespace="kube-system", serviceAccount="plain",
            roleArn="arn:aws:iam::000000000000:role/source")["association"]
        assert "externalId" not in plain
        assert plain["disableSessionTags"] is False
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_access_entry_create_describe_delete(eks):
    cn = _create_basic_cluster(eks)
    principal = f"arn:aws:iam::000000000000:user/test-{_uid()}"
    try:
        resp = eks.create_access_entry(
            clusterName=cn, principalArn=principal,
            kubernetesGroups=["admins"], username="admin",
            type="STANDARD",
        )
        ae = resp["accessEntry"]
        assert ae["clusterName"] == cn
        assert ae["principalArn"] == principal
        assert ae["kubernetesGroups"] == ["admins"]
        assert ae["username"] == "admin"
        assert ae["type"] == "STANDARD"
        assert ae["accessEntryArn"].startswith(
            f"arn:aws:eks:{REGION}:")

        desc = eks.describe_access_entry(
            clusterName=cn, principalArn=principal)["accessEntry"]
        assert desc["principalArn"] == principal

        # Delete returns empty body.
        eks.delete_access_entry(clusterName=cn, principalArn=principal)
        with pytest.raises(ClientError) as e:
            eks.describe_access_entry(
                clusterName=cn, principalArn=principal)
        assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_access_entry_create_duplicate_rejected(eks):
    cn = _create_basic_cluster(eks)
    principal = f"arn:aws:iam::000000000000:role/dup-{_uid()}"
    try:
        eks.create_access_entry(clusterName=cn, principalArn=principal)
        with pytest.raises(ClientError) as e:
            eks.create_access_entry(clusterName=cn, principalArn=principal)
        assert e.value.response["Error"]["Code"] == "ResourceInUseException"
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_access_entry_create_missing_cluster(eks):
    bogus = f"no-such-{_uid()}"
    with pytest.raises(ClientError) as e:
        eks.create_access_entry(
            clusterName=bogus,
            principalArn="arn:aws:iam::000000000000:role/r")
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_eks_access_entry_list_returns_principal_arns(eks):
    cn = _create_basic_cluster(eks)
    p1 = f"arn:aws:iam::000000000000:role/list-1-{_uid()}"
    p2 = f"arn:aws:iam::000000000000:role/list-2-{_uid()}"
    try:
        eks.create_access_entry(clusterName=cn, principalArn=p1)
        eks.create_access_entry(clusterName=cn, principalArn=p2)
        listed = eks.list_access_entries(clusterName=cn)["accessEntries"]
        # The creator's entry is there too: AWS sets the cluster creator as a
        # cluster admin access entry at creation time, which is what makes it
        # listable and revocable.
        assert {p1, p2} <= set(listed)
        assert "arn:aws:iam::000000000000:root" in listed
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_access_entry_update_patches_allowed_fields(eks):
    cn = _create_basic_cluster(eks)
    principal = f"arn:aws:iam::000000000000:role/upd-{_uid()}"
    try:
        eks.create_access_entry(
            clusterName=cn, principalArn=principal,
            kubernetesGroups=["before"], username="old",
        )
        updated = eks.update_access_entry(
            clusterName=cn, principalArn=principal,
            kubernetesGroups=["after"], username="new",
        )["accessEntry"]
        assert updated["kubernetesGroups"] == ["after"]
        assert updated["username"] == "new"
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_associate_access_policy_full_cycle(eks):
    cn = _create_basic_cluster(eks)
    principal = f"arn:aws:iam::000000000000:role/policy-{_uid()}"
    policy = ("arn:aws:eks::aws:cluster-access-policy/"
              "AmazonEKSClusterAdminPolicy")
    try:
        eks.create_access_entry(clusterName=cn, principalArn=principal)

        resp = eks.associate_access_policy(
            clusterName=cn, principalArn=principal,
            policyArn=policy,
            accessScope={"type": "cluster", "namespaces": []},
        )
        ap = resp["associatedAccessPolicy"]
        assert ap["policyArn"] == policy
        assert ap["accessScope"]["type"] == "cluster"

        listed = eks.list_associated_access_policies(
            clusterName=cn, principalArn=principal,
        )["associatedAccessPolicies"]
        assert len(listed) == 1
        assert listed[0]["policyArn"] == policy

        eks.disassociate_access_policy(
            clusterName=cn, principalArn=principal, policyArn=policy)
        listed_after = eks.list_associated_access_policies(
            clusterName=cn, principalArn=principal,
        )["associatedAccessPolicies"]
        assert listed_after == []
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_associate_access_policy_namespace_scope_requires_namespaces(eks):
    cn = _create_basic_cluster(eks)
    principal = f"arn:aws:iam::000000000000:role/ns-{_uid()}"
    policy = ("arn:aws:eks::aws:cluster-access-policy/"
              "AmazonEKSEditPolicy")
    try:
        eks.create_access_entry(clusterName=cn, principalArn=principal)
        with pytest.raises(ClientError) as e:
            eks.associate_access_policy(
                clusterName=cn, principalArn=principal,
                policyArn=policy,
                accessScope={"type": "namespace"},  # missing namespaces
            )
        assert e.value.response["Error"]["Code"] == "InvalidParameterException"
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_eks_delete_access_entry_cascades_associated_policies(eks):
    cn = _create_basic_cluster(eks)
    principal = f"arn:aws:iam::000000000000:role/casc-{_uid()}"
    policy = ("arn:aws:eks::aws:cluster-access-policy/"
              "AmazonEKSViewPolicy")
    try:
        eks.create_access_entry(clusterName=cn, principalArn=principal)
        eks.associate_access_policy(
            clusterName=cn, principalArn=principal,
            policyArn=policy,
            accessScope={"type": "cluster", "namespaces": []},
        )
        eks.delete_access_entry(clusterName=cn, principalArn=principal)
        # Recreate to verify the policy was cascaded out (not lingering).
        eks.create_access_entry(clusterName=cn, principalArn=principal)
        listed = eks.list_associated_access_policies(
            clusterName=cn, principalArn=principal,
        )["associatedAccessPolicies"]
        assert listed == []
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# AssociateIdentityProviderConfig
# ---------------------------------------------------------------------------

def test_eks_identity_provider_config(eks):
    cn = f"idp-{_uid()}"
    eks.create_cluster(
        name=cn, roleArn="arn:aws:iam::000000000000:role/eks",
        resourcesVpcConfig={"subnetIds": ["subnet-1"]},
    )
    try:
        # 1. Associate OIDC config
        resp = eks.associate_identity_provider_config(
            clusterName=cn,
            oidc={
                "identityProviderConfigName": "cognito-idp",
                "issuerUrl": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_000000000",
                "clientId": "client-12345",
                "usernameClaim": "sub",
                "groupsClaim": "cognito:groups",
            },
            tags={"env": "test"}
        )
        upd = resp["update"]
        assert upd["type"] == "IdentityProviderConfigUpdate"
        assert upd["status"] in ("InProgress", "Successful")

        # 2. Describe OIDC config
        desc = eks.describe_identity_provider_config(
            clusterName=cn,
            identityProviderConfig={"type": "oidc", "name": "cognito-idp"}
        )
        oidc_desc = desc["identityProviderConfig"]["oidc"]
        assert oidc_desc["identityProviderConfigName"] == "cognito-idp"
        assert oidc_desc["clientId"] == "client-12345"
        assert oidc_desc["issuerUrl"] == "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_000000000"
        assert oidc_desc["status"] in ("CREATING", "ACTIVE")

        # 2b. List OIDC configs
        list_resp = eks.list_identity_provider_configs(clusterName=cn)
        assert "identityProviderConfigs" in list_resp
        configs = list_resp["identityProviderConfigs"]
        assert len(configs) == 1
        assert configs[0]["name"] == "cognito-idp"
        assert configs[0]["type"] == "oidc"

        # 3. Disassociate OIDC config
        dis_resp = eks.disassociate_identity_provider_config(
            clusterName=cn,
            identityProviderConfig={"type": "oidc", "name": "cognito-idp"}
        )
        dis_upd = dis_resp["update"]
        assert dis_upd["type"] == "IdentityProviderConfigUpdate"

        # 4. List after disassociate -> empty
        empty_resp = eks.list_identity_provider_configs(clusterName=cn)
        assert empty_resp["identityProviderConfigs"] == []

        # 5. List on unknown cluster -> ResourceNotFoundException
        with pytest.raises(ClientError) as exc:
            eks.list_identity_provider_configs(clusterName=f"ghost-{_uid()}")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# IdP parity: cluster status, one-per-cluster, tag wiring
# ---------------------------------------------------------------------------

def _create_cluster_for_idp(eks, name):
    eks.create_cluster(
        name=name,
        roleArn="arn:aws:iam::000000000000:role/eks",
        resourcesVpcConfig={"subnetIds": ["subnet-1"]},
    )


def test_associate_idp_keeps_cluster_active(eks):
    """AssociateIdentityProviderConfigResponse is {update, tags} — cluster
    status must stay ACTIVE; UPDATING is never observable on the cluster."""
    cn = f"idp-status-{_uid()}"
    _create_cluster_for_idp(eks, cn)
    try:
        eks.associate_identity_provider_config(
            clusterName=cn,
            oidc={
                "identityProviderConfigName": "idp-1",
                "issuerUrl": "https://example/issuer",
                "clientId": "client-1",
            },
        )
        observed = set()
        for _ in range(5):
            observed.add(eks.describe_cluster(name=cn)["cluster"]["status"])
        assert "UPDATING" not in observed
        assert observed.issubset({"CREATING", "ACTIVE"})
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_only_one_oidc_idp_per_cluster(eks):
    """Real AWS rejects a second OIDC IdP regardless of the new name."""
    cn = f"idp-unique-{_uid()}"
    _create_cluster_for_idp(eks, cn)
    try:
        eks.associate_identity_provider_config(
            clusterName=cn,
            oidc={
                "identityProviderConfigName": "primary",
                "issuerUrl": "https://example/issuer",
                "clientId": "client-1",
            },
        )
        with pytest.raises(ClientError) as exc:
            eks.associate_identity_provider_config(
                clusterName=cn,
                oidc={
                    "identityProviderConfigName": "secondary",
                    "issuerUrl": "https://example/issuer2",
                    "clientId": "client-2",
                },
            )
        assert exc.value.response["Error"]["Code"] == "ResourceInUseException"
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


def test_idp_tags_returned_by_list_tags_for_resource(eks):
    """Tags set at associate time must be reachable via list_tags_for_resource
    on the identityProviderConfigArn, and the ARN must stop resolving after
    disassociate removes the local IdP config."""
    cn = f"idp-tags-{_uid()}"
    _create_cluster_for_idp(eks, cn)
    try:
        eks.associate_identity_provider_config(
            clusterName=cn,
            oidc={
                "identityProviderConfigName": "tag-idp",
                "issuerUrl": "https://example/issuer",
                "clientId": "client-1",
            },
            tags={"env": "test", "owner": "platform"},
        )
        desc = eks.describe_identity_provider_config(
            clusterName=cn,
            identityProviderConfig={"type": "oidc", "name": "tag-idp"},
        )
        arn = desc["identityProviderConfig"]["oidc"]["identityProviderConfigArn"]
        assert arn

        tags = eks.list_tags_for_resource(resourceArn=arn)["tags"]
        assert tags == {"env": "test", "owner": "platform"}

        eks.disassociate_identity_provider_config(
            clusterName=cn,
            identityProviderConfig={"type": "oidc", "name": "tag-idp"},
        )
        with pytest.raises(ClientError) as exc:
            eks.list_tags_for_resource(resourceArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        try:
            eks.delete_cluster(name=cn)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Default node labels (Karpenter / topology-aware controllers)
# ---------------------------------------------------------------------------

def test_eks_collect_node_labels_emits_aws_topology_defaults():
    """AWS-default topology labels must be on every cluster, no opt-in needed."""
    from ministack.services import eks as eks_mod

    cluster = {"tags": {}}
    args = eks_mod._collect_node_labels(cluster)
    keyed = dict(arg.removeprefix("--node-label=").split("=", 1) for arg in args)

    assert "topology.kubernetes.io/region" in keyed
    assert "topology.kubernetes.io/zone" in keyed
    region = keyed["topology.kubernetes.io/region"]
    assert keyed["topology.kubernetes.io/zone"] == f"{region}a"


def test_eks_k3s_run_kwargs_appends_node_labels():
    """node_labels list flows into the k3s server command verbatim."""
    from ministack.services.eks import _k3s_run_kwargs

    run_kwargs = _k3s_run_kwargs(
        name="t",
        region=REGION,
        port=16443,
        node_labels=["--node-label=topology.kubernetes.io/zone=us-east-1a"],
    )
    assert "--node-label=topology.kubernetes.io/zone=us-east-1a" in run_kwargs["command"]
    # Existing server flags must still be present — refactor must not regress them.
    assert "server" in run_kwargs["command"]
    assert "--https-listen-port=6443" in run_kwargs["command"]


# ---------------------------------------------------------------------------
# DescribeCluster endpoint (host-published port)
# ---------------------------------------------------------------------------

def test_eks_cluster_endpoint_defaults_to_host_form():
    """Advertises the host-published port — reachable from the host
    (aws eks update-kubeconfig + kubectl), not a docker-internal IP."""
    from ministack.services import eks as eks_mod

    assert eks_mod._cluster_endpoint(16443) == "https://localhost:16443"


def test_eks_cluster_endpoint_honours_ministack_host(monkeypatch):
    """Host form uses MINISTACK_HOST so a remote-host deployment is reachable."""
    from ministack.services import eks as eks_mod

    monkeypatch.setattr(eks_mod, "_MINISTACK_HOST", "10.0.0.5")
    assert eks_mod._cluster_endpoint(16443) == "https://10.0.0.5:16443"


def test_eks_restore_state_normalizes_endpoint_to_localhost():
    """A persisted cluster restores with no running container, so its endpoint is
    normalized to the stable https://localhost:{port} form (not a dead container
    IP, and never empty — it is still reported ACTIVE)."""
    from ministack.services import eks as eks_mod

    eks_mod.reset()
    try:
        eks_mod._clusters["c-restore"] = {
            "name": "c-restore",
            "status": "ACTIVE",
            "_port": 16443,
            "endpoint": "https://172.18.0.9:6443",  # stale container IP from prev run
            "_docker_id": "deadbeef",
        }
        state = eks_mod.get_state()
        eks_mod.reset()

        eks_mod.load_persisted_state(state)

        restored = eks_mod._clusters.get("c-restore")
        assert restored["endpoint"] == "https://localhost:16443"
        assert restored["_docker_id"] is None
        assert restored["status"] == "ACTIVE"  # endpoint stays non-empty for ACTIVE
    finally:
        eks_mod.reset()


# ---------------------------------------------------------------------------
# IAM exec-token authentication and AUTH mode enforcement (no Docker required)
# ---------------------------------------------------------------------------

_AUTH_ACCOUNT = "000000000000"
_AUTH_OTHER_ACCOUNT = "111111111111"
_AUTH_REGION = "eu-central-1"
_AUTH_CLUSTER = "auth-cluster"
_AUTH_USER_KEY = "AKIAEKSREVIEW"
_AUTH_USER_ARN = f"arn:aws:iam::{_AUTH_ACCOUNT}:user/developer"
_AUTH_ROLE_ARN = f"arn:aws:iam::{_AUTH_ACCOUNT}:role/control-plane"
_AUTH_POLICY_PREFIX = "arn:aws:eks::aws:cluster-access-policy/"
_AUTH_ADMIN_POLICY = _AUTH_POLICY_PREFIX + "AmazonEKSClusterAdminPolicy"


@pytest.fixture
def eks_auth_env(monkeypatch):
    monkeypatch.setattr(app, "AUTH", True)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("MINISTACK_ACCOUNT_ID", _AUTH_ACCOUNT)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    for name in ("_clusters", "_access_entries", "_access_policies", "_idp_configs", "_tags"):
        monkeypatch.setattr(eks_service, name, AccountRegionScopedDict())
    for name in ("_users", "_access_keys", "_roles", "_user_inline_policies"):
        monkeypatch.setattr(iam, name, AccountScopedDict())
    monkeypatch.setattr(sts, "_sessions", {})
    monkeypatch.setattr(eks_service, "_get_docker", lambda: None)
    with request_scope(_AUTH_ACCOUNT, _AUTH_REGION):
        iam._users["developer"] = {"Arn": _AUTH_USER_ARN, "UserId": "developer-id"}
        iam._access_keys[_AUTH_USER_KEY] = {
            "UserName": "developer", "SecretAccessKey": "test", "Status": "Active",
        }
        iam._roles["control-plane"] = {"Arn": _AUTH_ROLE_ARN}
        yield


def _auth_token(key="test", secret="test", session=None, name=_AUTH_CLUSTER, signed_at=None):
    request = AWSRequest(
        method="GET",
        url=f"https://sts.{_AUTH_REGION}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
        headers={"x-k8s-aws-id": name},
    )
    signer = SigV4QueryAuth(Credentials(key, secret, session), "sts", _AUTH_REGION, expires=60)
    if signed_at is None:
        signer.add_auth(request)
    else:
        with patch("botocore.auth.get_current_datetime", return_value=signed_at):
            signer.add_auth(request)
    return "k8s-aws-v1." + base64.urlsafe_b64encode(request.url.encode()).decode().rstrip("=")


# The webhook URL's shared secret; only the kubeconfig written into the k3s
# container carries it, so a caller who merely reaches the gateway cannot use
# the endpoint to probe whether a token is valid.
_AUTH_WEBHOOK_SECRET = "test-webhook-secret-000000000000"


def _auth_cluster(account=_AUTH_ACCOUNT, creator=None, bootstrap=True):
    cluster = {
        "arn": f"arn:aws:eks:{_AUTH_REGION}:{account}:cluster/{_AUTH_CLUSTER}",
        "roleArn": f"arn:aws:iam::{account}:role/control-plane",
        "_creator_arn": creator,
        "accessConfig": {"bootstrapClusterCreatorAdminPermissions": bootstrap},
        "_auth_token": _AUTH_WEBHOOK_SECRET,
    }
    eks_service._clusters.set_scoped(account, _AUTH_REGION, _AUTH_CLUSTER, cluster)
    # CreateCluster mints the creator's access entry; these records are built
    # straight into the store, so do what that path does.
    with request_scope(account, _AUTH_REGION):
        eks_service._bootstrap_creator_access_entry(_AUTH_CLUSTER, cluster, creator)


def _auth_token_for(account, region, name):
    """The cluster's webhook secret, the way k3s reads it from its kubeconfig."""
    cluster = eks_service._clusters.get_scoped(account, region, name) or {}
    # A cluster that does not exist has no secret; keep the URL well formed so
    # the request still reaches the handler and is refused on the lookup.
    return cluster.get("_auth_token") or _AUTH_WEBHOOK_SECRET


def _auth_review(bearer, account=_AUTH_ACCOUNT, name=_AUTH_CLUSTER):
    # Real k3s requests carry no AWS credentials; deliberately use the default
    # request account even when authenticating to a different tenant's cluster.
    secret = _auth_token_for(account, _AUTH_REGION, name)
    with request_scope(_AUTH_ACCOUNT, "us-east-1"):
        status, _, body = asyncio.run(app._dispatch_service_request(
            "POST", f"/_ministack/eks-auth/{account}/{_AUTH_REGION}/{name}/{secret}", {},
            json.dumps({"apiVersion": "authentication.k8s.io/v1", "spec": {"token": bearer}}).encode(),
            {}, "review-test",
        ))
    assert status == 200
    return json.loads(body)["status"]


def _auth_api(method, path, body=None, key="test"):
    payload = json.dumps(body or {}).encode()
    request = AWSRequest(method=method, url=f"http://localhost:4566{path}", data=payload)
    SigV4Auth(Credentials(key, "test"), "eks", _AUTH_REGION).add_auth(request)
    status, _, result = asyncio.run(app._dispatch_service_request(
        method, path, {k.lower(): v for k, v in request.headers.items()}, payload, {}, "api-test",
    ))
    return status, json.loads(result) if result else {}


def _auth_session(account=_AUTH_ACCOUNT, role="developer", key="ASIAEKSREVIEW"):
    sts._sessions[key] = {
        "Arn": f"arn:aws:sts::{account}:assumed-role/{role}/session",
        "AccountId": account, "UserId": "role-id:session", "SecretAccessKey": "test",
        "SessionToken": "local-session", "Expiration": time.time() + 3600,
    }
    return key


def test_eks_token_review_authenticates_root_exec_token(eks_mod, monkeypatch):
    import ministack.app as app
    monkeypatch.setattr(app, "AUTH", True)
    name = f"auth-{_uid()}"
    creator = "arn:aws:iam::000000000000:root"
    cluster = {
        "arn": f"arn:aws:eks:{REGION}:000000000000:cluster/{name}",
        "roleArn": "arn:aws:iam::000000000000:role/eks-role",
        "_creator_arn": creator,
        "_auth_token": _AUTH_WEBHOOK_SECRET,
    }
    eks_mod._clusters[name] = cluster
    eks_mod._bootstrap_creator_access_entry(name, cluster, creator)
    token = _eks_exec_token(name)
    status, _headers, response = _eks_direct(
        eks_mod,
        "POST",
        f"/_ministack/eks-auth/000000000000/{REGION}/{name}/"
        f"{_auth_token_for('000000000000', REGION, name)}",
        {
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "spec": {"token": token},
        },
    )
    assert status == 200
    assert response["status"]["authenticated"] is True
    assert response["status"]["user"]["username"] == "arn:aws:iam::000000000000:root"
    groups = response["status"]["user"]["groups"]
    assert "system:masters" not in groups
    assert any(g.startswith("ministack:eks:access:") for g in groups)


def test_eks_token_review_rejects_wrong_cluster_and_tampering(eks_mod, monkeypatch):
    import ministack.app as app
    monkeypatch.setattr(app, "AUTH", True)
    name = f"auth-{_uid()}"
    eks_mod._clusters[name] = {
        "arn": f"arn:aws:eks:{REGION}:000000000000:cluster/{name}",
        "roleArn": "arn:aws:iam::000000000000:role/eks-role",
    }
    token = _eks_exec_token(name)
    wrong_cluster = _eks_exec_token("another-cluster")
    for candidate in (wrong_cluster, token[:-1] + ("A" if token[-1] != "A" else "B")):
        status, _headers, response = _eks_direct(
            eks_mod,
            "POST",
            f"/_ministack/eks-auth/000000000000/{REGION}/{name}/"
        f"{_auth_token_for('000000000000', REGION, name)}",
            {"spec": {"token": candidate}},
        )
        assert status == 200
        assert response["status"]["authenticated"] is False


@pytest.mark.parametrize("auth", [False, True])
@pytest.mark.parametrize("credential", ["unknown-key", "wrong-secret", "unmapped-user", "opaque-token"])
def test_eks_auth_modes_enforce_credentials_and_access_grants(eks_auth_env, monkeypatch, auth, credential):
    monkeypatch.setattr(app, "AUTH", auth)
    _auth_cluster()
    bearer = {
        "unknown-key": lambda: _auth_token("unknown"),
        "wrong-secret": lambda: _auth_token(secret="wrong"),
        "unmapped-user": lambda: _auth_token(_AUTH_USER_KEY),
        "opaque-token": lambda: "local-bearer-token",
    }[credential]()
    result = _auth_review(bearer)
    assert result["authenticated"] is (not auth)
    if not auth:
        assert "system:masters" in result["user"]["groups"]


@pytest.mark.parametrize("auth", [False, True])
def test_eks_auth_empty_tokens_and_missing_clusters_are_rejected(eks_auth_env, monkeypatch, auth):
    monkeypatch.setattr(app, "AUTH", auth)
    _auth_cluster()
    assert _auth_review("")["authenticated"] is False
    assert _auth_review(_auth_token(), name="missing")["authenticated"] is False


@pytest.mark.parametrize("body", [b"null", b"[]", b"{", b'{"spec": []}'])
def test_eks_auth_malformed_reviews_fail_closed(eks_auth_env, body):
    assert json.loads(eks_service._handle_auth_webhook(_AUTH_CLUSTER, _AUTH_ACCOUNT, _AUTH_REGION, body)[2])["status"] == {
        "authenticated": False,
    }


@pytest.mark.parametrize("auth", [False, True])
def test_eks_auth_iam_enforcement_is_separate_from_kubernetes_access(eks_auth_env, monkeypatch, auth):
    monkeypatch.setattr(app, "AUTH", auth)
    _auth_cluster()
    assert _auth_api("GET", f"/clusters/{_AUTH_CLUSTER}", key=_AUTH_USER_KEY)[0] == (403 if auth else 200)
    iam._user_inline_policies["developer"] = {
        "eks": {"Statement": [{"Effect": "Allow", "Action": "eks:*", "Resource": "*"}]},
    }
    assert _auth_api("GET", f"/clusters/{_AUTH_CLUSTER}", key=_AUTH_USER_KEY)[0] == 200
    assert _auth_review(_auth_token(_AUTH_USER_KEY))["authenticated"] is (not auth)
    eks_service._create_access_entry(_AUTH_CLUSTER, {"principalArn": _AUTH_USER_ARN, "kubernetesGroups": ["developers"]})
    assert _auth_review(_auth_token(_AUTH_USER_KEY))["authenticated"] is True
    if auth:
        assert _auth_review(_auth_token(_AUTH_USER_KEY))["user"]["groups"] == ["system:authenticated", "developers"]


@pytest.mark.parametrize("key,creator", [("test", f"arn:aws:iam::{_AUTH_ACCOUNT}:root"), (_AUTH_USER_KEY, _AUTH_USER_ARN)])
@pytest.mark.parametrize("bootstrap", [False, True])
def test_eks_auth_create_records_actual_caller_and_honors_bootstrap(eks_auth_env, key, creator, bootstrap):
    iam._user_inline_policies["developer"] = {
        "eks": {"Statement": [{"Effect": "Allow", "Action": "eks:*", "Resource": "*"}]},
    }
    status, body = _auth_api("POST", "/clusters", {
        "name": _AUTH_CLUSTER, "roleArn": _AUTH_ROLE_ARN,
        "accessConfig": {"bootstrapClusterCreatorAdminPermissions": bootstrap},
    }, key=key)
    assert status == 200
    assert "_creator_arn" not in body["cluster"]
    assert eks_service._clusters[_AUTH_CLUSTER]["_creator_arn"] == creator
    assert _auth_review(_auth_token(key))["authenticated"] is bootstrap
    # The service role never inherits the caller's bootstrap privilege.
    assert _auth_review(_auth_token(_auth_session(role="control-plane"), session="local-session"))["authenticated"] is False


def test_eks_auth_root_does_not_get_other_accounts_bootstrap_access(eks_auth_env):
    _auth_cluster(_AUTH_OTHER_ACCOUNT, creator=f"arn:aws:iam::{_AUTH_OTHER_ACCOUNT}:root")
    assert _auth_review(_auth_token(), _AUTH_OTHER_ACCOUNT)["authenticated"] is False
    # The creator authorizes through its own access entry and the materialized
    # AmazonEKSClusterAdminPolicy group, not through system:masters.
    groups = _auth_review(_auth_token(_AUTH_OTHER_ACCOUNT), _AUTH_OTHER_ACCOUNT)["user"]["groups"]
    assert groups[0] == "system:authenticated"
    assert "system:masters" not in groups
    assert any(g.startswith("ministack:eks:access:") for g in groups)


def test_eks_delete_cluster_takes_its_access_entries_with_it(eks_auth_env):
    """A recreated cluster must not inherit the previous one's grants."""
    _auth_cluster(creator=f"arn:aws:iam::{_AUTH_ACCOUNT}:root")
    with request_scope(_AUTH_ACCOUNT, _AUTH_REGION):
        prefix = f"{_AUTH_CLUSTER}\x00"
        assert [k for k in eks_service._access_entries if str(k).startswith(prefix)]
        assert [k for k in eks_service._access_policies if str(k).startswith(prefix)]
        eks_service._delete_cluster(_AUTH_CLUSTER)
        assert not [k for k in eks_service._access_entries if str(k).startswith(prefix)]
        assert not [k for k in eks_service._access_policies if str(k).startswith(prefix)]


def test_eks_auth_nondefault_sts_session_and_role_path_access_entry(eks_auth_env):
    _auth_cluster(_AUTH_OTHER_ACCOUNT)
    key = _auth_session(_AUTH_OTHER_ACCOUNT)
    role_arn = f"arn:aws:iam::{_AUTH_OTHER_ACCOUNT}:role/team/developer"
    iam._roles.set_scoped(_AUTH_OTHER_ACCOUNT, None, "developer", {"Arn": role_arn})
    with request_scope(_AUTH_OTHER_ACCOUNT, _AUTH_REGION):
        eks_service._create_access_entry(_AUTH_CLUSTER, {"principalArn": role_arn, "kubernetesGroups": ["developers"]})
    assert _auth_review(_auth_token(key, session="local-session"), _AUTH_OTHER_ACCOUNT)["user"]["groups"] == [
        "system:authenticated", "developers",
    ]
    assert _auth_review(_auth_token(key, session="wrong"), _AUTH_OTHER_ACCOUNT)["authenticated"] is False
    sts._sessions[key]["Expiration"] = time.time() - 1
    assert _auth_review(_auth_token(key, session="local-session"), _AUTH_OTHER_ACCOUNT)["authenticated"] is False


@pytest.mark.parametrize("scope,policy,supported", [
    ({"type": "cluster"}, _AUTH_ADMIN_POLICY, True),
    ({"type": "namespace", "namespaces": ["dev"]}, _AUTH_ADMIN_POLICY, True),
    ({"type": "cluster"}, _AUTH_POLICY_PREFIX + "AmazonEKSViewPolicy", True),
    ({"type": "cluster"}, "fake-AmazonEKSClusterAdminPolicy", False),
])
def test_eks_auth_access_policies_map_to_internal_rbac_groups(eks_auth_env, monkeypatch, scope, policy, supported):
    monkeypatch.setattr(eks_service, "_schedule_access_policy_reconcile", lambda *args: None)
    _auth_cluster()
    eks_service._create_access_entry(_AUTH_CLUSTER, {"principalArn": _AUTH_USER_ARN})
    eks_service._associate_access_policy(_AUTH_CLUSTER, _AUTH_USER_ARN, {"policyArn": policy, "accessScope": scope})
    result = _auth_review(_auth_token(_AUTH_USER_KEY))
    assert result["authenticated"] is True
    group = eks_service._access_policy_group(_AUTH_CLUSTER, _AUTH_USER_ARN, policy)
    assert (group in result["user"]["groups"]) is supported
    assert "system:masters" not in result["user"]["groups"]


def _policy_allows(policy_name, verb, api_group, resource):
    for rule in eks_service._ACCESS_POLICY_RULES[policy_name]:
        groups = rule.get("apiGroups", [])
        resources = rule.get("resources", [])
        verbs = rule["verbs"]
        if (
            (api_group in groups or "*" in groups)
            and (resource in resources or "*" in resources)
            and (verb in verbs or "*" in verbs)
        ):
            return True
    return False


@pytest.mark.parametrize("policy,allowed,denied", [
    ("AmazonEKSClusterAdminPolicy", ("delete", "rbac.authorization.k8s.io", "clusterroles"), ()),
    ("AmazonEKSAdminPolicy", ("create", "rbac.authorization.k8s.io", "rolebindings"), ("delete", "", "nodes")),
    ("AmazonEKSEditPolicy", ("create", "", "secrets"), ("create", "rbac.authorization.k8s.io", "roles")),
    ("AmazonEKSViewPolicy", ("get", "", "pods"), ("get", "", "secrets")),
    ("AmazonEKSAdminViewPolicy", ("get", "", "secrets"), ("create", "", "secrets")),
])
def test_eks_access_policy_rules_allow_and_deny_documented_operations(policy, allowed, denied):
    assert _policy_allows(policy, *allowed)
    if denied:
        assert not _policy_allows(policy, *denied)


def test_eks_access_policy_rbac_reconciles_scopes_namespaces_and_revocation(eks_auth_env, monkeypatch):
    """The materialized k3s RBAC follows association updates and new matches."""
    monkeypatch.setattr(eks_service, "_schedule_access_policy_reconcile", lambda *args: None)
    _auth_cluster()
    eks_service._clusters.get_scoped(_AUTH_ACCOUNT, _AUTH_REGION, _AUTH_CLUSTER)["_docker_id"] = "k3s"
    policy = _AUTH_POLICY_PREFIX + "AmazonEKSViewPolicy"
    with request_scope(_AUTH_ACCOUNT, _AUTH_REGION):
        eks_service._create_access_entry(_AUTH_CLUSTER, {"principalArn": _AUTH_USER_ARN})
        eks_service._associate_access_policy(_AUTH_CLUSTER, _AUTH_USER_ARN, {
            "policyArn": policy,
            "accessScope": {"type": "namespace", "namespaces": ["dev-*"]},
        })

    namespaces = ["default", "dev-api", "prod"]
    captured = []
    managed = {"ClusterRoleBinding": [], "RoleBinding": []}
    deletes = []
    container = SimpleNamespace()
    client = SimpleNamespace(containers=SimpleNamespace(get=lambda _id: container))

    def kubectl(_container, command):
        if command[1:3] == ["get", "namespaces"]:
            return 0, json.dumps({"items": [{"metadata": {"name": name}} for name in namespaces]})
        if "delete" in command:
            deletes.append(command)
        return 0, ""

    monkeypatch.setattr(eks_service, "_get_docker", lambda: client)
    monkeypatch.setattr(eks_service, "_k3s_exec", kubectl)
    monkeypatch.setattr(
        eks_service, "_apply_access_policy_rbac",
        lambda _container, objects: captured.append(objects),
    )
    monkeypatch.setattr(
        eks_service, "_managed_access_policy_bindings",
        lambda _container, kind: managed[kind],
    )

    assert eks_service._reconcile_access_policy_rbac(_AUTH_CLUSTER, _AUTH_ACCOUNT, _AUTH_REGION)
    bindings = [obj for obj in captured[-1] if obj["kind"] == "RoleBinding"]
    assert [binding["metadata"]["namespace"] for binding in bindings] == ["dev-api"]
    assert bindings[0]["roleRef"]["name"] == "ministack-eks-amazoneksviewpolicy"
    managed["RoleBinding"] = bindings

    namespaces.append("dev-worker")
    assert eks_service._reconcile_access_policy_rbac(_AUTH_CLUSTER, _AUTH_ACCOUNT, _AUTH_REGION)
    bindings = [obj for obj in captured[-1] if obj["kind"] == "RoleBinding"]
    assert {binding["metadata"]["namespace"] for binding in bindings} == {"dev-api", "dev-worker"}

    with request_scope(_AUTH_ACCOUNT, _AUTH_REGION):
        eks_service._associate_access_policy(_AUTH_CLUSTER, _AUTH_USER_ARN, {
            "policyArn": policy, "accessScope": {"type": "cluster"},
        })
    managed["RoleBinding"] = bindings
    assert eks_service._reconcile_access_policy_rbac(_AUTH_CLUSTER, _AUTH_ACCOUNT, _AUTH_REGION)
    cluster_bindings = [obj for obj in captured[-1] if obj["kind"] == "ClusterRoleBinding"]
    assert len(cluster_bindings) == 1
    assert {command[2] for command in deletes} == {"rolebinding"}

    managed["ClusterRoleBinding"] = cluster_bindings
    with request_scope(_AUTH_ACCOUNT, _AUTH_REGION):
        eks_service._disassociate_access_policy(_AUTH_CLUSTER, _AUTH_USER_ARN, policy)
    assert eks_service._reconcile_access_policy_rbac(_AUTH_CLUSTER, _AUTH_ACCOUNT, _AUTH_REGION)
    assert not [obj for obj in captured[-1] if obj["kind"].endswith("Binding")]
    assert {command[2] for command in deletes} == {"rolebinding", "clusterrolebinding"}


def test_eks_access_policy_changes_schedule_rbac_reconciliation(eks_auth_env, monkeypatch):
    scheduled = []
    policy = _AUTH_POLICY_PREFIX + "AmazonEKSEditPolicy"
    monkeypatch.setattr(
        eks_service, "_schedule_access_policy_reconcile",
        lambda cluster_name, *args: scheduled.append(cluster_name),
    )
    _auth_cluster()
    eks_service._create_access_entry(_AUTH_CLUSTER, {"principalArn": _AUTH_USER_ARN})
    eks_service._associate_access_policy(_AUTH_CLUSTER, _AUTH_USER_ARN, {
        "policyArn": policy, "accessScope": {"type": "cluster"},
    })
    eks_service._disassociate_access_policy(_AUTH_CLUSTER, _AUTH_USER_ARN, policy)
    eks_service._delete_access_entry(_AUTH_CLUSTER, _AUTH_USER_ARN)
    assert scheduled == [_AUTH_CLUSTER, _AUTH_CLUSTER, _AUTH_CLUSTER]


@pytest.mark.parametrize("minutes,accepted", [(2, True), (14, True), (16, False), (-6, False)])
def test_eks_auth_exec_token_lifetime_matches_kubectl_cache(eks_auth_env, minutes, accepted):
    _auth_cluster(creator=f"arn:aws:iam::{_AUTH_ACCOUNT}:root")
    signed_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes)
    assert _auth_review(_auth_token(signed_at=signed_at))["authenticated"] is accepted


def test_eks_auth_background_webhook_preserves_account_and_region(eks_auth_env, monkeypatch):
    finished = threading.Event()
    captured = {}

    def start(client, kwargs, registries, webhook):
        captured["webhook"] = webhook.decode()
        finished.set()
        return SimpleNamespace(id="fake-container")

    monkeypatch.setattr(eks_service, "_get_docker", lambda: object())
    monkeypatch.setattr(eks_service, "_get_ministack_network", lambda client: None)
    monkeypatch.setattr(eks_service, "_k3s_registries_yaml", lambda *args: None)
    monkeypatch.setattr(eks_service, "_start_k3s_container", start)
    monkeypatch.setattr(eks_service, "_extract_ca_cert", lambda *args: "fake-ca")
    with request_scope(_AUTH_OTHER_ACCOUNT, _AUTH_REGION):
        iam._roles["control-plane"] = {"Arn": f"arn:aws:iam::{_AUTH_OTHER_ACCOUNT}:role/control-plane"}
        status, _, _ = eks_service._create_cluster({"name": _AUTH_CLUSTER, "roleArn": iam._roles["control-plane"]["Arn"]})
    assert status == 200
    assert finished.wait(3)
    assert f"/_ministack/eks-auth/{_AUTH_OTHER_ACCOUNT}/{_AUTH_REGION}/{_AUTH_CLUSTER}/" in captured["webhook"]
    # The secret is in the URL only k3s receives, and it is not guessable.
    secret = captured["webhook"].split(f"/{_AUTH_CLUSTER}/", 1)[1].split("\n", 1)[0].strip()
    assert len(secret) >= 16
