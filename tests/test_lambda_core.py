# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
"""AWS Lambda Core network connectors (botocore ``lambda-core`` 2026-04-30).

Driven through a real boto3 ``lambda-core`` client, so every case also covers
the routing the issue reported: the service signs with credential scope
``lambda``, so a request that is not disambiguated by path reaches the function
router and comes back as "Function not found: /2026-04-04/network-connectors".
"""

import uuid as _uuid_mod

import pytest
from botocore.exceptions import ClientError
from conftest import make_client


@pytest.fixture(scope="module")
def lambda_core():
    return make_client("lambda-core")


def _name(prefix="nc"):
    return f"{prefix}-{_uuid_mod.uuid4().hex[:8]}"


def _config(subnets=("subnet-0123456789abcdef0",), groups=("sg-0123456789abcdef0",),
            protocol="IPv4"):
    return {
        "VpcEgressConfiguration": {
            "SubnetIds": list(subnets),
            "SecurityGroupIds": list(groups),
            "NetworkProtocol": protocol,
            "AssociatedComputeResourceTypes": ["MicroVm"],
        }
    }


def test_lambda_core_create_returns_the_modelled_shape(lambda_core):
    """CreateNetworkConnector answers Arn/Name/Id, the three members the model
    marks required on the response, plus the echoed Configuration."""
    name = _name()
    resp = lambda_core.create_network_connector(Name=name, Configuration=_config())
    assert resp["Name"] == name
    assert resp["Id"]
    assert resp["Arn"].endswith(f":network-connector:{name}")
    assert resp["Arn"].startswith("arn:aws:lambda:")
    # NetworkConnectorState enum.
    assert resp["State"] in ("PENDING", "ACTIVE")
    vpc = resp["Configuration"]["VpcEgressConfiguration"]
    assert vpc["SubnetIds"] == ["subnet-0123456789abcdef0"]
    assert vpc["NetworkProtocol"] == "IPv4"
    assert vpc["AssociatedComputeResourceTypes"] == ["MicroVm"]
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 202


def test_lambda_core_get_reports_active_and_iso_last_modified(lambda_core):
    """A connector a client polls after the 202 must leave PENDING, and
    CoreTimestamp is `timestampFormat: iso8601`, so boto3 parses LastModified
    into a datetime rather than handing back a number."""
    import datetime as _dt

    name = _name()
    lambda_core.create_network_connector(Name=name, Configuration=_config())
    got = lambda_core.get_network_connector(Identifier=name)
    assert got["State"] == "ACTIVE"
    assert got["Name"] == name
    assert isinstance(got["LastModified"], _dt.datetime)


def test_lambda_core_get_accepts_the_arn_as_identifier(lambda_core):
    """`Identifier` is documented as the name or the ARN."""
    name = _name()
    created = lambda_core.create_network_connector(Name=name, Configuration=_config())
    got = lambda_core.get_network_connector(Identifier=created["Arn"])
    assert got["Name"] == name


def test_lambda_core_update_changes_config_and_keeps_identity(lambda_core):
    """UpdateNetworkConnector is a 202 and keeps Arn/Name/Id."""
    name = _name()
    created = lambda_core.create_network_connector(Name=name, Configuration=_config())
    updated = lambda_core.update_network_connector(
        Identifier=name,
        Configuration=_config(subnets=["subnet-aaaaaaaaaaaaaaaaa"], protocol="DualStack"),
    )
    assert updated["ResponseMetadata"]["HTTPStatusCode"] == 202
    assert updated["Arn"] == created["Arn"]
    assert updated["Id"] == created["Id"]
    vpc = updated["Configuration"]["VpcEgressConfiguration"]
    assert vpc["SubnetIds"] == ["subnet-aaaaaaaaaaaaaaaaa"]
    assert vpc["NetworkProtocol"] == "DualStack"
    assert lambda_core.get_network_connector(
        Identifier=name)["Configuration"]["VpcEgressConfiguration"][
            "NetworkProtocol"] == "DualStack"


def test_lambda_core_delete_removes_it(lambda_core):
    name = _name()
    lambda_core.create_network_connector(Name=name, Configuration=_config())
    deleted = lambda_core.delete_network_connector(Identifier=name)
    assert deleted["ResponseMetadata"]["HTTPStatusCode"] == 202
    assert deleted["Name"] == name
    with pytest.raises(ClientError) as excinfo:
        lambda_core.get_network_connector(Identifier=name)
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert excinfo.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_lambda_core_list_filters_by_state_and_pages(lambda_core):
    """ListNetworkConnectors takes State/Marker/MaxItems on the query string and
    answers NetworkConnectors + NextMarker."""
    prefix = _name("page")
    made = sorted(f"{prefix}-{i}" for i in range(3))
    for n in made:
        lambda_core.create_network_connector(Name=n, Configuration=_config())

    seen, marker = [], None
    while True:
        kwargs = {"MaxItems": 1}
        if marker:
            kwargs["Marker"] = marker
        page = lambda_core.list_network_connectors(**kwargs)
        seen.extend(c["Name"] for c in page["NetworkConnectors"]
                    if c["Name"].startswith(prefix))
        marker = page.get("NextMarker")
        if not marker:
            break
    assert seen == made

    summary = [c for c in lambda_core.list_network_connectors(
        State="ACTIVE")["NetworkConnectors"] if c["Name"] == made[0]][0]
    # NetworkConnectorSummary.Type is required, and VPC_EGRESS is the one value
    # the model's NetworkConnectorType enum carries.
    assert summary["Type"] == "VPC_EGRESS"
    assert summary["State"] == "ACTIVE"


def test_lambda_core_client_token_replays_the_first_response(lambda_core):
    """ClientToken is an idempotency token: the second call answers the first
    call's response and creates nothing new."""
    token = _uuid_mod.uuid4().hex
    first = lambda_core.create_network_connector(
        Name=_name(), Configuration=_config(), ClientToken=token)
    second = lambda_core.create_network_connector(
        Name=_name(), Configuration=_config(), ClientToken=token)
    assert second["Id"] == first["Id"]
    assert second["Name"] == first["Name"]


def test_lambda_core_duplicate_name_conflicts(lambda_core):
    name = _name()
    lambda_core.create_network_connector(Name=name, Configuration=_config())
    with pytest.raises(ClientError) as excinfo:
        lambda_core.create_network_connector(Name=name, Configuration=_config())
    assert excinfo.value.response["Error"]["Code"] == "ResourceConflictException"
    assert excinfo.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409


def test_lambda_core_unknown_identifier_is_not_found(lambda_core):
    for call in ("get_network_connector", "delete_network_connector"):
        with pytest.raises(ClientError) as excinfo:
            getattr(lambda_core, call)(Identifier="no-such-connector")
        assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"


@pytest.mark.parametrize("config,reason", [
    ({"VpcEgressConfiguration": {"SubnetIds": ["not-a-subnet"]}}, "subnet id"),
    ({"VpcEgressConfiguration": {"SecurityGroupIds": ["nope"]}}, "security group"),
    ({"VpcEgressConfiguration": {"NetworkProtocol": "IPv6"}}, "NetworkProtocol enum"),
    ({"VpcEgressConfiguration": {"AssociatedComputeResourceTypes": ["Nope"]}},
     "ComputeResourceType enum"),
])
def test_lambda_core_configuration_constraints_are_enforced(lambda_core, config, reason):
    """The model's own constraints: subnet/sg id patterns, the NetworkProtocol
    and ComputeResourceType enums, and AssociatedComputeResourceTypesList min 1.

    Only the error code is asserted. No message string is documented for this
    service, so the wordings are MiniStack's own and pinning one here would
    read as if it had been measured."""
    with pytest.raises(ClientError) as excinfo:
        lambda_core.create_network_connector(Name=_name(), Configuration=config)
    err = excinfo.value.response
    assert err["Error"]["Code"] == "InvalidParameterValueException", reason
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_lambda_core_empty_compute_resource_types_is_refused_on_the_wire():
    """AssociatedComputeResourceTypesList is min 1, so botocore refuses an
    empty list client-side and it never reaches the server. Send it raw to
    prove the service refuses it too, rather than accepting a shape the model
    forbids."""
    raw = make_client("lambda-core", {"parameter_validation": False})
    with pytest.raises(ClientError) as excinfo:
        raw.create_network_connector(
            Name=_name(),
            Configuration={"VpcEgressConfiguration":
                           {"AssociatedComputeResourceTypes": []}},
        )
    err = excinfo.value.response
    assert err["Error"]["Code"] == "InvalidParameterValueException"
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_lambda_core_does_not_shadow_the_lambda_function_api(lambda_core):
    """The whole point of the routing fix: lambda-core signs with credential
    scope `lambda`, so adding it must not divert the function API."""
    lam = make_client("lambda")
    with pytest.raises(ClientError) as excinfo:
        lam.get_function(FunctionName="definitely-not-a-function")
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"
    # And the connector surface still answers on the same endpoint.
    assert "NetworkConnectors" in lambda_core.list_network_connectors()
