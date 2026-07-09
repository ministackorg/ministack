"""
Integration tests for EKS service emulator.
Tests cluster CRUD, nodegroup CRUD, tags, and CloudFormation provisioning.
k3s Docker container tests require Docker socket access.
"""
import asyncio
import json
import time
import uuid
from urllib.parse import quote

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


def test_eks_k3s_run_kwargs_host_gateway_extra_host():
    """The k3s node must be able to reach a host-run MiniStack for ECR
    registry mirroring (#1054) — host.docker.internal via host-gateway."""
    from ministack.services.eks import _k3s_run_kwargs

    kwargs = _k3s_run_kwargs(name="c1", port=16443)
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
        assert set(listed) == {p1, p2}
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

        # 3. Disassociate OIDC config
        dis_resp = eks.disassociate_identity_provider_config(
            clusterName=cn,
            identityProviderConfig={"type": "oidc", "name": "cognito-idp"}
        )
        dis_upd = dis_resp["update"]
        assert dis_upd["type"] == "IdentityProviderConfigUpdate"

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

        eks_mod.restore_state(state)

        restored = eks_mod._clusters.get("c-restore")
        assert restored["endpoint"] == "https://localhost:16443"
        assert restored["_docker_id"] is None
        assert restored["status"] == "ACTIVE"  # endpoint stays non-empty for ACTIVE
    finally:
        eks_mod.reset()
