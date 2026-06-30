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

def test_mq_create_broker_initializes_empty_tags(mq):
    """Test that _tags[broker_arn] = {} when no tags provided (Line 222)."""
    name = _name("tags-init-empty")
    resp = mq.create_broker(
        BrokerName=name,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
    )
    broker_arn = resp["BrokerArn"]

    # Verify tags dict is initialized and empty
    list_resp = mq.list_tags(ResourceArn=broker_arn)
    assert list_resp["Tags"] == {}

def test_mq_create_broker_initializes_empty_users(mq):
    """Test that _users[broker_id] = {} on creation (Line 223)."""
    broker_id = _create(mq, BrokerName=_name("users-init"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    # Verify users dict is initialized and empty
    list_resp = mq.list_users(BrokerId=broker_id)
    assert list_resp.get("Users", []) == []
    assert list_resp["MaxResults"] == 20

def test_mq_create_broker_with_tags_initializes_tags(mq):
    """Test that _tags[broker_arn] stores provided tags (Line 222)."""
    name = _name("tags-init-with")
    resp = mq.create_broker(
        BrokerName=name,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
        Tags={"environment": "test", "team": "platform"},
    )
    broker_arn = resp["BrokerArn"]

    # Verify tags are persisted
    list_resp = mq.list_tags(ResourceArn=broker_arn)
    assert list_resp["Tags"]["environment"] == "test"
    assert list_resp["Tags"]["team"] == "platform"

def test_mq_create_broker_with_empty_tags_dict(mq):
    """Test that empty tags dict is handled correctly (Line 222)."""
    name = _name("tags-init-empty-dict")
    resp = mq.create_broker(
        BrokerName=name,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
        Tags={},
    )
    broker_arn = resp["BrokerArn"]

    list_resp = mq.list_tags(ResourceArn=broker_arn)
    assert list_resp["Tags"] == {}

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

    # Paginate with a stable page size and gather IDs until we find all new brokers.
    listed_ids = set()
    token = None
    pages = 0
    while True:
        pages += 1
        kwargs = {"MaxResults": 5}
        if token:
            kwargs["NextToken"] = token

        resp = mq.list_brokers(**kwargs)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert len(resp.get("BrokerSummaries", [])) <= 5

        listed_ids.update({b["BrokerId"] for b in resp.get("BrokerSummaries", [])})
        if set(created_ids).issubset(listed_ids):
            break

        token = resp.get("NextToken")
        if not token:
            break

        # Safety guard in case pagination regresses.
        assert pages <= 100

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

def test_mq_describe_broker_includes_tags(mq):
    """Test that DescribeBroker returns tags from _tags dict (Line 271)."""
    name = _name("describe-tags")
    create_resp = mq.create_broker(
        BrokerName=name,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
        Tags={"env": "staging", "owner": "devops"},
    )
    broker_id = create_resp["BrokerId"]

    desc = mq.describe_broker(BrokerId=broker_id)
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Tags" in desc
    assert desc["Tags"]["env"] == "staging"
    assert desc["Tags"]["owner"] == "devops"

def test_mq_describe_broker_includes_empty_tags(mq):
    """Test that DescribeBroker returns empty tags object when none exist (Line 271)."""
    name = _name("describe-tags-empty")
    create_resp = mq.create_broker(
        BrokerName=name,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
    )
    broker_id = create_resp["BrokerId"]

    desc = mq.describe_broker(BrokerId=broker_id)
    assert "Tags" in desc
    assert desc["Tags"] == {}
    assert isinstance(desc["Tags"], dict)

def test_mq_describe_broker_tags_reflect_create_tags_additions(mq):
    """Test that DescribeBroker reflects tags added via CreateTags (Line 271)."""
    name = _name("describe-tags-added")
    create_resp = _create(mq, BrokerName=name)
    broker_id = create_resp["BrokerId"]
    arn = create_resp["BrokerArn"]

    # Initially no tags
    desc1 = mq.describe_broker(BrokerId=broker_id)
    assert desc1["Tags"] == {}

    # Add tags
    mq.create_tags(ResourceArn=arn, Tags={"added": "after"})

    # Verify DescribeBroker now shows the tags
    desc2 = mq.describe_broker(BrokerId=broker_id)
    assert desc2["Tags"]["added"] == "after"

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

def test_mq_delete_broker_removes_tags(mq):
    """Test that DeleteBroker removes entry from _tags dict (Line 281)."""
    name = _name("delete-tags-cleanup")
    create_resp = mq.create_broker(
        BrokerName=name,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
        Tags={"cleanup": "test"},
    )
    broker_id = create_resp["BrokerId"]
    broker_arn = create_resp["BrokerArn"]

    # Verify tags exist before delete
    list_resp = mq.list_tags(ResourceArn=broker_arn)
    assert list_resp["Tags"]["cleanup"] == "test"

    # Delete broker
    mq.delete_broker(BrokerId=broker_id)

    # Verify tags are cleaned up - ListTags should fail with 404
    with pytest.raises(ClientError) as exc:
        mq.list_tags(ResourceArn=broker_arn)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_mq_delete_broker_removes_users(mq):
    """Test that DeleteBroker removes entry from _users dict (Line 282)."""
    broker_id = _create(mq, BrokerName=_name("delete-users-cleanup"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    # Create a user
    mq.create_user(
        BrokerId=broker_id,
        Username="cleanup_user",
        Password="CleanupPassw0rd!",
        ConsoleAccess=False,
    )

    # Verify user exists
    list_resp = mq.list_users(BrokerId=broker_id)
    usernames = {u["Username"] for u in list_resp.get("Users", [])}
    assert "cleanup_user" in usernames

    # Delete broker
    mq.delete_broker(BrokerId=broker_id)

    # Verify users are cleaned up - ListUsers should fail with 404
    with pytest.raises(ClientError) as exc:
        mq.list_users(BrokerId=broker_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_mq_delete_broker_clears_name_index(mq):
    """Test that DeleteBroker cleans up _name_index secondary index."""
    name = _name("delete-name-index")
    broker_id = _create(mq, BrokerName=name)["BrokerId"]

    # Delete broker
    mq.delete_broker(BrokerId=broker_id)

    # Verify name index is cleared - creating broker with same name should succeed
    resp = mq.create_broker(
        BrokerName=name,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["BrokerId"] != broker_id

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
    resp = mq.describe_broker_instance_options(MaxResults=5)

    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["MaxResults"] == 5
    assert len(resp.get("BrokerInstanceOptions", [])) == 5

@pytest.mark.parametrize("invalid_max", [4, 101])
def test_mq_describe_broker_instance_options_with_invalid_max_results(mq, invalid_max):
    with pytest.raises(ClientError) as exc:
        mq.describe_broker_instance_options(MaxResults=invalid_max)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

def test_mq_describe_broker_instance_options_pagination(mq):
    # Walk all pages with a stable page size and verify pagination progress.
    seen = set()
    token = None
    pages = 0
    while True:
        pages += 1
        kwargs = {"MaxResults": 5}
        if token:
            kwargs["NextToken"] = token

        resp = mq.describe_broker_instance_options(**kwargs)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["MaxResults"] == 5

        items = resp.get("BrokerInstanceOptions", [])
        assert len(items) <= 5

        page_keys = {
            (o["EngineType"], o["HostInstanceType"], o["StorageType"])
            for o in items
        }
        assert page_keys.isdisjoint(seen)
        seen.update(page_keys)

        token = resp.get("NextToken")
        if not token:
            break

        # Safety guard in case pagination regresses.
        assert pages <= 100

    # Ensure pagination actually covered more than one page worth of results.
    assert len(seen) > 5

#########################################################################
# CreateTags
#########################################################################

def test_mq_create_tags(mq):
    broker = _create(mq, BrokerName=_name("tags"))
    arn = broker["BrokerArn"]

    create_resp = mq.create_tags(ResourceArn=arn, Tags={"env": "dev", "team": "core"})
    assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 204

def test_mq_create_tags_with_non_existent_arn(mq):
    with pytest.raises(ClientError) as exc:
        mq.create_tags(ResourceArn="arn:aws:mq:invalid-arn", Tags={"env": "dev"})
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

##########################################################################
# ListTags
##########################################################################

def test_mq_list_tags(mq):
    broker = _create(mq, BrokerName=_name("tags"))
    arn = broker["BrokerArn"]

    mq.create_tags(ResourceArn=arn, Tags={"env": "dev", "team": "core"})

    list_resp = mq.list_tags(ResourceArn=arn)
    assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert list_resp["Tags"]["env"] == "dev"
    assert list_resp["Tags"]["team"] == "core"

def test_mq_list_tags_with_no_tags(mq):
    broker = _create(mq, BrokerName=_name("tags"))
    arn = broker["BrokerArn"]

    list_resp = mq.list_tags(ResourceArn=arn)
    assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert list_resp["Tags"] == {}

def test_mq_list_tags_with_non_existent_arn(mq):
    with pytest.raises(ClientError) as exc:
        mq.list_tags(ResourceArn="arn:aws:mq:invalid-arn")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


@pytest.mark.parametrize(
    "arn_template",
    [
        "arn:aws:sqs:us-east-1:000000000000:broker:{broker_id}",
        "arn:aws:mq:us-west-2:000000000000:broker:{broker_id}",
        "arn:aws:mq:us-east-1:111111111111:broker:{broker_id}",
    ],
)
def test_mq_tag_arns_must_parse_to_local_broker(mq, arn_template):
    broker = _create(mq, BrokerName=_name("tag-arn"))
    arn = arn_template.format(broker_id=broker["BrokerId"])

    with pytest.raises(ClientError) as exc:
        mq.create_tags(ResourceArn=arn, Tags={"env": "dev"})

    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_mq_list_tags_multiple_times_consistent(mq):
    """Test that ListTags returns consistent results across multiple calls."""
    broker = _create(mq, BrokerName=_name("tags-consistency"))
    arn = broker["BrokerArn"]

    mq.create_tags(ResourceArn=arn, Tags={"a": "1", "b": "2", "c": "3"})

    # Call ListTags multiple times and verify consistency
    resp1 = mq.list_tags(ResourceArn=arn)
    resp2 = mq.list_tags(ResourceArn=arn)
    resp3 = mq.list_tags(ResourceArn=arn)

    assert resp1["Tags"] == resp2["Tags"] == resp3["Tags"]
    assert len(resp1["Tags"]) == 3

###########################################################################
# DeleteTags
###########################################################################

def test_mq_delete_tags(mq):
    broker = _create(mq, BrokerName=_name("tags"))
    arn = broker["BrokerArn"]

    mq.create_tags(ResourceArn=arn, Tags={"env": "dev", "team": "core"})

    del_resp = mq.delete_tags(ResourceArn=arn, TagKeys=["env"])
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 204

    list_resp = mq.list_tags(ResourceArn=arn)
    assert "env" not in list_resp["Tags"]
    assert list_resp["Tags"]["team"] == "core"

def test_mq_delete_tags_with_non_existent_arn(mq):
    with pytest.raises(ClientError) as exc:
        mq.delete_tags(ResourceArn="arn:aws:mq:invalid-arn", TagKeys=["env"])
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_mq_delete_tags_idempotent(mq):
    """Test that DeleteTags is idempotent (deleting non-existent key succeeds)."""
    broker = _create(mq, BrokerName=_name("tags-idempotent"))
    arn = broker["BrokerArn"]

    mq.create_tags(ResourceArn=arn, Tags={"key1": "value1"})

    # Delete non-existent key - should succeed
    del_resp = mq.delete_tags(ResourceArn=arn, TagKeys=["nonexistent"])
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 204

    # Verify original tag still exists
    list_resp = mq.list_tags(ResourceArn=arn)
    assert list_resp["Tags"]["key1"] == "value1"

############################################################################
# CreateUser
############################################################################

def test_mq_create_user_with_required_options(mq):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    resp = mq.create_user(
        BrokerId=broker_id,
        Username="testuser",
        Password="TestPassw0rd!",
        ConsoleAccess=False,
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_mq_create_user_with_duplicated_username(mq):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    mq.create_user(
        BrokerId=broker_id,
        Username="testuser",
        Password="TestPassw0rd!",
        ConsoleAccess=False,
    )

    with pytest.raises(ClientError) as exc:
        mq.create_user(
            BrokerId=broker_id,
            Username="testuser",
            Password="AnotherPassw0rd!",
            ConsoleAccess=False,
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
    assert exc.value.response["Error"]["Code"] == "ConflictException"

@pytest.mark.parametrize("invalid_password", [
    "pas",            # less than 4 characters
    "password,",  # with commas
    "password:",  # with colons
    "password="  # with equals sign
])
def test_mq_create_user_with_invalid_password(mq, invalid_password):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]
    with pytest.raises(ClientError) as exc:
        mq.create_user(
            BrokerId=broker_id,
            Username="testuser",
            Password=invalid_password,
            ConsoleAccess=False,
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

def test_mq_create_user_with_non_existent_broker_id(mq):
    with pytest.raises(ClientError) as exc:
        mq.create_user(
            BrokerId="invalid-id",
            Username="testuser",
            Password="TestPassw0rd!",
            ConsoleAccess=False,
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_mq_create_user_with_not_supported_engine_type(mq):
    broker_id = _create(mq, BrokerName=_name("user-invalid"), EngineType="RABBITMQ", EngineVersion="4.2")["BrokerId"]

    with pytest.raises(ClientError) as exc:
        mq.create_user(
            BrokerId=broker_id,
            Username="testuser",
            Password="TestPassw0rd!",
            ConsoleAccess=False,
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

##########################################################################
# DeleteUser
##########################################################################

def test_mq_delete_user(mq):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    mq.create_user(
        BrokerId=broker_id,
        Username="testuser",
        Password="TestPassw0rd!",
        ConsoleAccess=False,
    )

    del_resp = mq.delete_user(BrokerId=broker_id, Username="testuser")
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_mq_delete_user_with_non_existent_username(mq):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    with pytest.raises(ClientError) as exc:
        mq.delete_user(BrokerId=broker_id, Username="nonexistentuser")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_mq_delete_user_with_non_existent_broker_id(mq):
    with pytest.raises(ClientError) as exc:
        mq.delete_user(BrokerId="invalid-id", Username="testuser")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_mq_delete_user_with_not_supported_engine_type(mq):
    broker_id = _create(mq, BrokerName=_name("user-invalid"), EngineType="RABBITMQ", EngineVersion="4.2")["BrokerId"]

    with pytest.raises(ClientError) as exc:
        mq.delete_user(BrokerId=broker_id, Username="testuser")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

###########################################################################
# ListUsers
###########################################################################

def test_mq_list_users(mq):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    mq.create_user(
        BrokerId=broker_id,
        Username="testuser1",
        Password="TestPassw0rd!",
        ConsoleAccess=False,
    )

    list_resp = mq.list_users(BrokerId=broker_id)
    assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert list_resp["MaxResults"] == 20
    usernames = {u["Username"] for u in list_resp.get("Users", [])}
    assert "testuser1" in usernames

def test_mq_list_users_with_no_users(mq):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    list_resp = mq.list_users(BrokerId=broker_id)
    assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert list_resp.get("Users", []) == []
    assert isinstance(list_resp.get("Users", []), list)

def test_mq_list_users_with_non_existent_broker_id(mq):
    with pytest.raises(ClientError) as exc:
        mq.list_users(BrokerId="invalid-id")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_mq_list_users_with_not_supported_engine_type(mq):
    broker_id = _create(mq, BrokerName=_name("user-invalid"), EngineType="RABBITMQ")["BrokerId"]

    with pytest.raises(ClientError) as exc:
        mq.list_users(BrokerId=broker_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

def test_mq_list_users_with_pagination(mq):
    broker_id = _create(mq, BrokerName=_name("user-page"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    # Create enough users to require multiple pages.
    expected_usernames = set()
    for i in range(9):
        username = f"testuser{i}"
        expected_usernames.add(username)
        mq.create_user(
            BrokerId=broker_id,
            Username=username,
            Password="TestPassw0rd!",
            ConsoleAccess=False,
        )

    # Paginate with stable MaxResults and gather until all created users are observed.
    listed_usernames = set()
    token = None
    pages = 0
    while True:
        pages += 1
        kwargs = {"BrokerId": broker_id, "MaxResults": 5}
        if token:
            kwargs["NextToken"] = token

        resp = mq.list_users(**kwargs)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["MaxResults"] == 5
        assert len(resp.get("Users", [])) <= 5

        listed_usernames.update({u["Username"] for u in resp.get("Users", [])})
        if expected_usernames.issubset(listed_usernames):
            break

        token = resp.get("NextToken")
        if not token:
            break

        # Safety guard in case pagination regresses.
        assert pages <= 100

    assert expected_usernames.issubset(listed_usernames), f"Expected {expected_usernames} in {listed_usernames}"

@pytest.mark.parametrize("invalid_max", [4, 101])
def test_mq_list_users_with_invalid_max_results(mq, invalid_max):
    broker_id = _create(mq, BrokerName=_name("user-invalid-max"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    with pytest.raises(ClientError) as exc:
        mq.list_users(BrokerId=broker_id, MaxResults=invalid_max)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

############################################################################
# UpdateUser
############################################################################

def test_mq_update_user(mq):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    mq.create_user(
        BrokerId=broker_id,
        Username="full_update_user",
        Password="InitialPassw0rd!",
        ConsoleAccess=False,
        Groups=["users"],
        ReplicationUser=False,
    )

    # Update all fields
    mq.update_user(
        BrokerId=broker_id,
        Username="full_update_user",
        Password="UpdatedPassw0rd!",
        ConsoleAccess=True,
        Groups=["admins", "ops"],
        ReplicationUser=True,
    )

    # Verify updates
    user = mq.describe_user(BrokerId=broker_id, Username="full_update_user")
    assert user["ConsoleAccess"] == True
    assert set(user["Groups"]) == {"admins", "ops"}
    assert user["ReplicationUser"] == True

def test_mq_update_user_with_non_existent_username(mq):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    with pytest.raises(ClientError) as exc:
        mq.update_user(
            BrokerId=broker_id,
            Username="nonexistentuser",
            Password="NewPassw0rd!",
            ConsoleAccess=True,
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_mq_update_user_with_non_existent_broker_id(mq):
    with pytest.raises(ClientError) as exc:
        mq.update_user(
            BrokerId="invalid-id",
            Username="testuser",
            Password="NewPassw0rd!",
            ConsoleAccess=True,
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_mq_update_user_with_not_supported_engine_type(mq):
    broker_id = _create(mq, BrokerName=_name("user-invalid-update"), EngineType="RABBITMQ")["BrokerId"]

    with pytest.raises(ClientError) as exc:
        mq.update_user(
            BrokerId=broker_id,
            Username="testuser",
            Password="NewPassw0rd!",
            ConsoleAccess=True,
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

@pytest.mark.parametrize("invalid_password", [
    "pas",            # less than 4 characters
    "password,",  # with commas
    "password:",  # with colons
    "password="  # with equals sign
])
def test_mq_update_user_with_invalid_password(mq, invalid_password):
    broker_id = _create(mq, BrokerName=_name("user-invalid-update"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    mq.create_user(
        BrokerId=broker_id,
        Username="testuser",
        Password="TestPassw0rd!",
        ConsoleAccess=False,
    )

    with pytest.raises(ClientError) as exc:
        mq.update_user(
            BrokerId=broker_id,
            Username="testuser",
            Password=invalid_password,
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"

############################################################################
# DescribeUser
############################################################################

def test_mq_describe_user(mq):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    mq.create_user(
        BrokerId=broker_id,
        Username="testuser",
        Password="TestPassw0rd!",
        ConsoleAccess=False,
        Groups=["group1", "group2"],
        ReplicationUser = True
    )

    desc_resp = mq.describe_user(BrokerId=broker_id, Username="testuser")
    assert desc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert desc_resp["Username"] == "testuser"
    assert desc_resp["ConsoleAccess"] is False
    assert set(desc_resp["Groups"]) == {"group1", "group2"}
    assert desc_resp["ReplicationUser"] is True

def test_mq_describe_user_with_non_existent_username(mq):
    broker_id = _create(mq, BrokerName=_name("user"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    with pytest.raises(ClientError) as exc:
        mq.describe_user(BrokerId=broker_id, Username="nonexistentuser")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_mq_describe_user_with_non_existent_broker_id(mq):
    with pytest.raises(ClientError) as exc:
        mq.describe_user(BrokerId="invalid-id", Username="testuser")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_mq_describe_user_with_not_supported_engine_type(mq):
    broker_id = _create(mq, BrokerName=_name("user-invalid-describe"), EngineType="RABBITMQ")["BrokerId"]

    with pytest.raises(ClientError) as exc:
        mq.describe_user(BrokerId=broker_id, Username="testuser")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "BadRequestException"


############################################################################
# Lifecycle tests
############################################################################

def test_mq_create_describe_delete_broker_tags_lifecycle(mq):
    """Test full lifecycle: create with tags -> describe shows tags -> delete removes tags."""
    name = _name("lifecycle-tags")

    # 1. Create broker with tags
    create_resp = mq.create_broker(
        BrokerName=name,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
        Tags={"lifecycle": "test", "phase": "all"},
    )
    broker_id = create_resp["BrokerId"]
    broker_arn = create_resp["BrokerArn"]

    # 2. Describe broker and verify tags included
    desc = mq.describe_broker(BrokerId=broker_id)
    assert desc["Tags"]["lifecycle"] == "test"
    assert desc["Tags"]["phase"] == "all"

    # 3. Add more tags
    mq.create_tags(ResourceArn=broker_arn, Tags={"added": "later"})

    # 4. Verify describe shows all tags
    desc2 = mq.describe_broker(BrokerId=broker_id)
    assert desc2["Tags"]["lifecycle"] == "test"
    assert desc2["Tags"]["phase"] == "all"
    assert desc2["Tags"]["added"] == "later"

    # 5. Delete broker
    mq.delete_broker(BrokerId=broker_id)

    # 6. Verify tags are cleaned up
    with pytest.raises(ClientError) as exc:
        mq.list_tags(ResourceArn=broker_arn)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_mq_multiple_brokers_isolated_tags(mq):
    """Test that tags from different brokers don't interfere with each other."""
    # Create two brokers with different tags
    name1 = _name("isolated-1")
    resp1 = mq.create_broker(
        BrokerName=name1,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
        Tags={"broker": "first", "env": "prod"},
    )
    arn1 = resp1["BrokerArn"]

    name2 = _name("isolated-2")
    resp2 = mq.create_broker(
        BrokerName=name2,
        EngineType="RABBITMQ",
        EngineVersion="3.13",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
        Tags={"broker": "second", "env": "dev"},
    )
    arn2 = resp2["BrokerArn"]

    # Verify each broker has its own isolated tags
    tags1 = mq.list_tags(ResourceArn=arn1)["Tags"]
    tags2 = mq.list_tags(ResourceArn=arn2)["Tags"]

    assert tags1["broker"] == "first"
    assert tags1["env"] == "prod"
    assert tags2["broker"] == "second"
    assert tags2["env"] == "dev"


def test_mq_multiple_brokers_isolated_users(mq):
    """Test that users from different brokers don't interfere with each other."""
    # Create two ActiveMQ brokers
    broker_id1 = _create(mq, BrokerName=_name("users-isolated-1"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]
    broker_id2 = _create(mq, BrokerName=_name("users-isolated-2"), EngineType="ACTIVEMQ", EngineVersion="5.19")["BrokerId"]

    # Create users with same username in different brokers
    mq.create_user(
        BrokerId=broker_id1,
        Username="shared_name",
        Password="Broker1Passw0rd!",
        ConsoleAccess=True,
    )

    mq.create_user(
        BrokerId=broker_id2,
        Username="shared_name",
        Password="Broker2Passw0rd!",
        ConsoleAccess=False,
    )

    # Verify users are isolated
    user1 = mq.describe_user(BrokerId=broker_id1, Username="shared_name")
    user2 = mq.describe_user(BrokerId=broker_id2, Username="shared_name")

    assert user1 is not None
    assert user2 is not None
    assert user1["ConsoleAccess"] == True
    assert user2["ConsoleAccess"] == False


def test_mq_recreate_broker_after_delete_has_fresh_tags_and_users(mq):
    """Test that recreating a broker after deletion starts with fresh tag/user dicts."""
    name = _name("recreate-fresh")

    # Create, add tags/users, and delete
    resp1 = mq.create_broker(
        BrokerName=name,
        EngineType="ACTIVEMQ",
        EngineVersion="5.19",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
        Tags={"first": "creation"},
    )
    broker_id1 = resp1["BrokerId"]
    broker_arn1 = resp1["BrokerArn"]

    # Add a user
    mq.create_user(
        BrokerId=broker_id1,
        Username="first_user",
        Password="FirstPassw0rd!",
        ConsoleAccess=True,
    )

    # Delete the broker
    mq.delete_broker(BrokerId=broker_id1)

    # Recreate broker with same name
    resp2 = mq.create_broker(
        BrokerName=name,
        EngineType="ACTIVEMQ",
        EngineVersion="5.19",
        HostInstanceType="mq.m5.large",
        PubliclyAccessible=False,
        DeploymentMode="SINGLE_INSTANCE",
    )
    broker_id2 = resp2["BrokerId"]
    broker_arn2 = resp2["BrokerArn"]

    # Verify fresh state
    tags = mq.list_tags(ResourceArn=broker_arn2)["Tags"]
    users = mq.list_users(BrokerId=broker_id2)["Users"]

    assert tags == {}
    assert users == []

    # Verify old ARN is different from new ARN
    assert broker_arn1 != broker_arn2
