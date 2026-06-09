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
    "broker_name_suffix,engine_type,engine_version,host_instance_type,deployment_mode",
    [
        ("invalid-engine", "INVALID_ENGINE", "1.0", "mq.m5.large", "SINGLE_INSTANCE"),
        ("invalid-engine-version", "RABBITMQ", "INVALID_VERSION", "mq.m5.large", "SINGLE_INSTANCE"),
        ("invalid-deployment-mode", "RABBITMQ", "3.13", "mq.m5.large", "INVALID_MODE"),
        ("invalid-instance-type", "RABBITMQ", "3.13", "INVALID_INSTANCE", "SINGLE_INSTANCE"),
    ],
)
def test_mq_create_broker_with_invalid_parameters(
    mq, broker_name_suffix, engine_type, engine_version, host_instance_type, deployment_mode
):
    """Test that invalid parameters return BadRequestException."""
    with pytest.raises(ClientError) as exc:
        mq.create_broker(
            BrokerName=_name(broker_name_suffix),
            EngineType=engine_type,
            EngineVersion=engine_version,
            HostInstanceType=host_instance_type,
            PubliclyAccessible=False,
            DeploymentMode=deployment_mode,
        )
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
