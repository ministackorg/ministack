"""
Integration tests for the AmazonMQ service (RabbitMQ CRUD).
"""

import uuid

import pytest
from botocore.exceptions import ClientError

# ###########################################################################
# Helpers
# ###########################################################################

def _name(suffix: str = "") -> str:
    """Generate a unique broker name for a test run."""
    return f"intg-mq-{suffix}-{uuid.uuid4().hex[:8]}"


def _create(mq, **kwargs) -> dict:
    params = dict(
        BrokerName=_name("base"),
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
    )
    params.update(kwargs)
    return mq.create_broker(**params)


############################################################################
# CreateBroker
############################################################################

def test_mq_create_broker_with_required_options(mq):
    name = _name("create")
    resp = mq.create_broker(
        BrokerName=name,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "BrokerId" in resp
    assert "BrokerArn" in resp
    assert resp["BrokerArn"].startswith("arn:aws:mq:")

def test_mq_create_broker_with_duplicated_name(mq):
    name = _name("dup")
    _create(mq, BrokerName=name)

    with pytest.raises(ClientError) as exc:
        _create(mq, BrokerName=name)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
    assert exc.value.response["Error"]["Code"] == "ConflictException"

@pytest.mark.parametrize(
    "create_params",
    [
        {"BrokerName": _name("invalid-engine"), "EngineType": "INVALID_ENGINE", "EngineVersion": "1.0", "HostInstanceType": "mq.m5.large", "DeploymentMode": "SINGLE_INSTANCE", "PubliclyAccessible": False},
        {"BrokerName": _name("invalid-engine-version"), "EngineType": "RABBITMQ", "EngineVersion": "INVALID_VERSION", "HostInstanceType": "mq.m5.large", "DeploymentMode": "SINGLE_INSTANCE", "PubliclyAccessible": False},
        {"BrokerName": _name("invalid-deployment-mode"), "EngineType": "RABBITMQ", "EngineVersion": "3.13", "HostInstanceType": "mq.m5.large", "DeploymentMode": "INVALID_MODE", "PubliclyAccessible": False},
        {"BrokerName": _name("invalid-instance-type"), "EngineType": "RABBITMQ", "EngineVersion": "3.13", "HostInstanceType": "INVALID_INSTANCE", "DeploymentMode": "SINGLE_INSTANCE", "PubliclyAccessible": False},
        {"BrokerName": _name("invalid-storage-type"), "EngineType": "RABBITMQ", "EngineVersion": "3.13", "HostInstanceType": "mq.m5.large", "DeploymentMode": "SINGLE_INSTANCE", "StorageType": "INVALID_STORAGE", "PubliclyAccessible": False}
    ],
)
def test_mq_create_broker_with_invalid_parameters(
    mq, create_params
):
    """Test that invalid parameters return BadRequestException."""
    with pytest.raises(ClientError) as exc:
        mq.create_broker(**create_params)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

############################################################################
# ListBrokers
############################################################################

def test_mq_list_brokers(mq):
    names = [_name("list") for _ in range(2)]
    ids = set()
    for n in names:
        r = mq.create_broker(
            BrokerName=n,
            EngineType="RABBITMQ",
            EngineVersion="3.13",
            HostInstanceType="mq.m5.large",
            PubliclyAccessible=False,
            DeploymentMode="SINGLE_INSTANCE",
        )
        ids.add(r["BrokerId"])

    resp = mq.list_brokers()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    listed_ids = {b["BrokerId"] for b in resp.get("BrokerSummaries", [])}
    assert ids.issubset(listed_ids), f"Expected {ids} in {listed_ids}"

def test_mq_list_brokers_with_max_results(mq):
    # Create 10 brokers to ensure we have more than 5 to list
    for _ in range(10):
        _create(mq, BrokerName=_name("list-max"))

    resp = mq.list_brokers(MaxResults=5)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert len(resp.get("BrokerSummaries", [])) <= 5

@pytest.mark.parametrize("invalid_max", [4, 101])
def test_mq_list_brokers_with_invalid_max_results(mq, invalid_max):
    with pytest.raises(ClientError) as exc:
        mq.list_brokers(MaxResults=invalid_max)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

def test_mq_list_brokers_pagination(mq):
    # Create 10 brokers to ensure we have more than 5 to list
    created_ids = []
    for _ in range(10):
        resp = _create(mq, BrokerName=_name("list-page"))
        created_ids.append(resp["BrokerId"])

    # First page with MaxResults=5
    resp1 = mq.list_brokers(MaxResults=5)
    assert resp1["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert len(resp1.get("BrokerSummaries", [])) == 5
    assert "NextToken" in resp1

    # Second page using NextToken
    resp2 = mq.list_brokers(NextToken=resp1["NextToken"])
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert len(resp2.get("BrokerSummaries", [])) == 5

    # Verify all created brokers are listed across pages
    listed_ids = {b["BrokerId"] for b in resp1.get("BrokerSummaries", []) + resp2.get("BrokerSummaries", [])}
    assert set(created_ids).issubset(listed_ids), f"Expected {created_ids} in {listed_ids}"

############################################################################
# DescribeBrokers
############################################################################

def test_mq_describe_broker(mq):
    name = _name("describe")
    create_resp = _create(mq, BrokerName=name)
    broker_id = create_resp["BrokerId"]

    desc = mq.describe_broker(BrokerId=broker_id)
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert desc["BrokerId"] == broker_id
    assert desc["BrokerName"] == name
    assert desc["EngineType"] == "RABBITMQ"
    assert desc["BrokerState"] == "RUNNING"
    assert "BrokerInstances" in desc

def test_mq_describe_broker_with_non_existent_id(mq):
    with pytest.raises(ClientError) as exc:
        mq.describe_broker(BrokerId="invalid-id")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

############################################################################
# DeleteBrokers
############################################################################

def test_mq_delete_broker(mq):
    name = _name("delete")
    broker_id = _create(mq, BrokerName=name)["BrokerId"]

    del_resp = mq.delete_broker(BrokerId=broker_id)
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert del_resp["BrokerId"] == broker_id

def test_mq_delete_broker_with_non_existent_id(mq):
    with pytest.raises(ClientError) as exc:
        mq.delete_broker(BrokerId="invalid-id")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

############################################################################
# UpdateBroker
############################################################################

def test_mq_update_broker_with_required_options(mq):
    broker_id = _create(mq, BrokerName=_name("update"))["BrokerId"]

    resp = mq.update_broker(
        BrokerId=broker_id,
        HostInstanceType="mq.m5.xlarge",
        AutoMinorVersionUpgrade=True,
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["BrokerId"] == broker_id
    assert resp["HostInstanceType"] == "mq.m5.xlarge"

def test_mq_update_broker_with_non_existent_id(mq):
    with pytest.raises(ClientError) as exc:
        mq.update_broker(
            BrokerId="invalid-id",
            HostInstanceType="mq.m5.xlarge",
            AutoMinorVersionUpgrade=True,
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

@pytest.mark.parametrize("update_params", [
    {"HostInstanceType": "INVALID_INSTANCE", "AutoMinorVersionUpgrade": True},
    {"EngineVersion": "INVALID_VERSION", "AutoMinorVersionUpgrade": True},
])
def test_mq_update_broker_with_invalid_options(mq, update_params):
    broker_id = _create(mq, BrokerName=_name("update-invalid"))["BrokerId"]

    with pytest.raises(ClientError) as exc:
        mq.update_broker(BrokerId=broker_id, **update_params)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

############################################################################
# RebootBroker
############################################################################

def test_mq_reboot_broker(mq):
    broker_id = _create(mq, BrokerName=_name("reboot"))["BrokerId"]

    resp = mq.reboot_broker(BrokerId=broker_id)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_mq_reboot_broker_with_non_existent_id(mq):
    with pytest.raises(ClientError) as exc:
        mq.reboot_broker(BrokerId="invalid-id")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

############################################################################
# DescribeBrokerEngineTypes
############################################################################

def test_mq_describe_broker_engine_types_with_no_params(mq):
    resp = mq.describe_broker_engine_types()
    assert len(resp["BrokerEngineTypes"]) > 0
    assert resp["MaxResults"] == 20

def test_mq_describe_broker_engine_types_with_engine_type(mq):
    resp = mq.describe_broker_engine_types(EngineType="RABBITMQ")
    assert len(resp["BrokerEngineTypes"]) > 0
    assert all(e["EngineType"] == "RABBITMQ" for e in resp["BrokerEngineTypes"])

def test_mq_describe_broker_engine_types_with_invalid_engine_type(mq):
    with pytest.raises(ClientError) as exc:
        mq.describe_broker_engine_types(EngineType="INVALID_ENGINE")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

@pytest.mark.parametrize("invalid_max", [4, 101])
def test_mq_describe_broker_engine_types_with_invalid_max_results(mq, invalid_max):
    with pytest.raises(ClientError) as exc:
        mq.describe_broker_engine_types(MaxResults=invalid_max)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

########################################################################
# DescribeBrokerInstanceOptions
########################################################################

def test_mq_describe_broker_instance_options(mq):
    resp = mq.describe_broker_instance_options()

    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert len(resp.get("BrokerInstanceOptions", [])) > 0
    for option in resp["BrokerInstanceOptions"]:
        assert "AvailabilityZones" in option
        assert "EngineType" in option
        assert "HostInstanceType" in option
        assert "StorageType" in option
        assert "SupportedEngineVersions" in option
        assert "SupportedDeploymentModes" in option

@pytest.mark.parametrize(
    "kwargs,assertions",
    [
        (
            {"EngineType": "RABBITMQ"},
            lambda o: o["EngineType"] == "RABBITMQ",
        ),
        (
            {"HostInstanceType": "mq.m5.large"},
            lambda o: o["HostInstanceType"] == "mq.m5.large",
        ),
        (
            {"StorageType": "EBS"},
            lambda o: o["StorageType"] == "EBS",
        ),
        (
            {"EngineType": "RABBITMQ", "HostInstanceType": "mq.m5.large"},
            lambda o: (
                o["EngineType"] == "RABBITMQ"
                and o["HostInstanceType"] == "mq.m5.large"
            ),
        ),
        (
            {"EngineType": "RABBITMQ", "StorageType": "EBS"},
            lambda o: (
                o["EngineType"] == "RABBITMQ"
                and o["StorageType"] == "EBS"
            ),
        ),
        (
            {"HostInstanceType": "mq.m5.large", "StorageType": "EBS"},
            lambda o: (
                o["HostInstanceType"] == "mq.m5.large"
                and o["StorageType"] == "EBS"
            ),
        ),
        (
            {
                "EngineType": "RABBITMQ",
                "HostInstanceType": "mq.m5.large",
                "StorageType": "EBS",
            },
            lambda o: (
                o["EngineType"] == "RABBITMQ"
                and o["HostInstanceType"] == "mq.m5.large"
                and o["StorageType"] == "EBS"
            ),
        ),
    ],
)
def test_mq_broker_instance_options_filtered(mq, kwargs, assertions):
    resp = mq.describe_broker_instance_options(**kwargs)

    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert len(resp.get("BrokerInstanceOptions", [])) > 0
    assert all(assertions(o) for o in resp["BrokerInstanceOptions"])

@pytest.mark.parametrize(
    "kwargs",
    [
        {"EngineType": "INVALID_ENGINE"},
        {"HostInstanceType": "INVALID_INSTANCE"},
        {"StorageType": "INVALID_STORAGE"},
    ],
)
def test_mq_describe_broker_instance_options_with_invalid_parameters(mq, kwargs):
    with pytest.raises(ClientError) as exc:
        mq.describe_broker_instance_options(**kwargs)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

def test_mq_describe_broker_instance_options_with_max_results(mq):
    resp = mq.describe_broker_instance_options(MaxResults=1)

    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["MaxResults"] == 1
    assert len(resp.get("BrokerInstanceOptions", [])) == 1

@pytest.mark.parametrize("invalid_max", [4, 101])
def test_mq_describe_broker_instance_options_with_invalid_max_results(mq, invalid_max):
    with pytest.raises(ClientError) as exc:
        mq.describe_broker_instance_options(MaxResults=invalid_max)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

def test_mq_describe_broker_instance_options_pagination(mq):
    # Create enough options to ensure pagination is needed
    resp1 = mq.describe_broker_instance_options(MaxResults=2)
    assert resp1["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp1["MaxResults"] == 2
    assert len(resp1.get("BrokerInstanceOptions", [])) == 2
    assert "NextToken" in resp1

    resp2 = mq.describe_broker_instance_options(MaxResults=2, NextToken=resp1["NextToken"])
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp2["MaxResults"] == 2
    assert len(resp2.get("BrokerInstanceOptions", [])) >= 1
