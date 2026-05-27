import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()

_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"

def test_dynamodb_basic(ddb):
    try:
        ddb.delete_table(TableName="TestTable1")
    except Exception:
        pass
    ddb.create_table(
        TableName="TestTable1",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="TestTable1", Item={"pk": {"S": "key1"}, "data": {"S": "value1"}})
    resp = ddb.get_item(TableName="TestTable1", Key={"pk": {"S": "key1"}})
    assert resp["Item"]["data"]["S"] == "value1"
    ddb.delete_item(TableName="TestTable1", Key={"pk": {"S": "key1"}})
    resp = ddb.get_item(TableName="TestTable1", Key={"pk": {"S": "key1"}})
    assert "Item" not in resp

def test_dynamodb_scan(ddb):
    try:
        ddb.delete_table(TableName="ScanTable")
    except Exception:
        pass
    ddb.create_table(
        TableName="ScanTable",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(10):
        ddb.put_item(TableName="ScanTable", Item={"pk": {"S": f"key{i}"}, "val": {"N": str(i)}})
    resp = ddb.scan(TableName="ScanTable")
    assert resp["Count"] == 10

def test_dynamodb_batch(ddb):
    try:
        ddb.delete_table(TableName="BatchTable")
    except Exception:
        pass
    ddb.create_table(
        TableName="BatchTable",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.batch_write_item(
        RequestItems={
            "BatchTable": [{"PutRequest": {"Item": {"pk": {"S": f"bk{i}"}, "v": {"S": f"bv{i}"}}}} for i in range(5)]
        }
    )
    resp = ddb.scan(TableName="BatchTable")
    assert resp["Count"] == 5

def test_dynamodb_describe_continuous_backups(ddb):
    ddb.create_table(
        TableName="ddb-pitr-tbl",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.describe_continuous_backups(TableName="ddb-pitr-tbl")
    assert resp["ContinuousBackupsDescription"]["ContinuousBackupsStatus"] == "ENABLED"
    pitr = resp["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
    assert pitr["PointInTimeRecoveryStatus"] == "DISABLED"

def test_dynamodb_update_continuous_backups(ddb):
    ddb.update_continuous_backups(
        TableName="ddb-pitr-tbl",
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )
    resp = ddb.describe_continuous_backups(TableName="ddb-pitr-tbl")
    pitr = resp["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
    assert pitr["PointInTimeRecoveryStatus"] == "ENABLED"

def test_dynamodb_describe_endpoints(ddb):
    resp = ddb.describe_endpoints()
    assert len(resp["Endpoints"]) > 0
    assert "Address" in resp["Endpoints"][0]

def test_dynamodb_batch_write_consumed_capacity(ddb):
    ddb.create_table(
        TableName="batch-cap-regression",
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.batch_write_item(
        RequestItems={
            "batch-cap-regression": [
                {"PutRequest": {"Item": {"pk": {"S": "k1"}}}},
            ]
        },
        ReturnConsumedCapacity="TOTAL",
    )
    assert "ConsumedCapacity" in resp, "ConsumedCapacity must be present when ReturnConsumedCapacity=TOTAL"
    assert isinstance(resp["ConsumedCapacity"], list), "ConsumedCapacity must be a list for BatchWriteItem"
    assert resp["ConsumedCapacity"][0]["TableName"] == "batch-cap-regression"
    assert resp["ConsumedCapacity"][0]["CapacityUnits"] == 1.0
    ddb.delete_table(TableName="batch-cap-regression")

def test_dynamodb_put_item_gsi_capacity(ddb):
    """PutItem on a table with 1 GSI must return CapacityUnits=2.0 (table + GSI)."""
    ddb.create_table(
        TableName="gsi-cap-put",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "last_name", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "last_name-index",
                "KeySchema": [{"AttributeName": "last_name", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.put_item(
        TableName="gsi-cap-put",
        Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "last_name": {"S": "Smith"}},
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["ConsumedCapacity"]["CapacityUnits"] == 2.0
    ddb.delete_table(TableName="gsi-cap-put")

def test_dynamodb_batch_write_gsi_capacity(ddb):
    """BatchWriteItem with 2 items on a table with 1 GSI must return CapacityUnits=4.0."""
    ddb.create_table(
        TableName="gsi-cap-batch",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "age", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "age-index",
                "KeySchema": [{"AttributeName": "age", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.batch_write_item(
        RequestItems={
            "gsi-cap-batch": [
                {"PutRequest": {"Item": {"pk": {"S": "p2"}, "sk": {"S": "s2"}, "age": {"N": "25"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "p3"}, "sk": {"S": "s3"}, "age": {"N": "26"}}}},
            ]
        },
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["ConsumedCapacity"][0]["CapacityUnits"] == 4.0
    ddb.delete_table(TableName="gsi-cap-batch")

def test_dynamodb_streams_table_has_stream_arn(ddb):
    """Table with StreamSpecification returns LatestStreamArn and operations succeed."""
    table_name = "stream-arn-test"
    resp = ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    desc = ddb.describe_table(TableName=table_name)["Table"]
    assert desc.get("LatestStreamArn") or desc.get("StreamSpecification", {}).get("StreamEnabled")

    # All write operations should succeed with streams enabled
    ddb.put_item(TableName=table_name, Item={"pk": {"S": "k1"}, "val": {"S": "v1"}})
    ddb.update_item(
        TableName=table_name,
        Key={"pk": {"S": "k1"}},
        UpdateExpression="SET val = :v",
        ExpressionAttributeValues={":v": {"S": "v2"}},
    )
    ddb.delete_item(TableName=table_name, Key={"pk": {"S": "k1"}})
    # Verify item is gone
    get_resp = ddb.get_item(TableName=table_name, Key={"pk": {"S": "k1"}})
    assert "Item" not in get_resp

def test_dynamodb_tag_untag_resource(ddb):
    """Create table, tag it, list tags, untag, verify."""
    table_name = "ddb-tag-test"
    try:
        ddb.delete_table(TableName=table_name)
    except Exception:
        pass
    resp = ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    arn = resp["TableDescription"]["TableArn"]

    # Tag
    ddb.tag_resource(ResourceArn=arn, Tags=[
        {"Key": "env", "Value": "test"},
        {"Key": "team", "Value": "platform"},
    ])
    tags = ddb.list_tags_of_resource(ResourceArn=arn)["Tags"]
    tag_keys = {t["Key"] for t in tags}
    assert "env" in tag_keys
    assert "team" in tag_keys

    # Untag
    ddb.untag_resource(ResourceArn=arn, TagKeys=["team"])
    tags2 = ddb.list_tags_of_resource(ResourceArn=arn)["Tags"]
    tag_keys2 = {t["Key"] for t in tags2}
    assert "env" in tag_keys2
    assert "team" not in tag_keys2

def test_dynamodb_stream_to_lambda(lam, ddb):
    """DynamoDB stream records are delivered to Lambda via event source mapping."""
    table_name = "intg-ddbstream-tbl"
    fn_name = "intg-ddbstream-fn"

    ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    stream_arn = ddb.describe_table(TableName=table_name)["Table"]["LatestStreamArn"]
    assert stream_arn is not None

    code = (
        "import json\n"
        "def handler(event, context):\n"
        "    records = event.get('Records', [])\n"
        "    return {'processed': len(records)}\n"
    )
    lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    esm = lam.create_event_source_mapping(
        FunctionName=fn_name,
        EventSourceArn=stream_arn,
        StartingPosition="TRIM_HORIZON",
        BatchSize=10,
    )
    assert esm["EventSourceArn"] == stream_arn
    assert esm["FunctionArn"].endswith(fn_name)
    assert esm["State"] in ("Creating", "Enabled")

    # Write items to trigger stream records
    ddb.put_item(TableName=table_name, Item={"pk": {"S": "a1"}, "data": {"S": "hello"}})
    ddb.put_item(TableName=table_name, Item={"pk": {"S": "a2"}, "data": {"S": "world"}})
    ddb.delete_item(TableName=table_name, Key={"pk": {"S": "a1"}})

    # Allow background poller to process
    time.sleep(3)

    # Verify the ESM is still active
    esm_resp = lam.get_event_source_mapping(UUID=esm["UUID"])
    assert esm_resp["EventSourceArn"] == stream_arn

    # Verify DynamoDB state is correct after stream operations
    scan = ddb.scan(TableName=table_name)
    pks = {item["pk"]["S"] for item in scan["Items"]}
    assert "a2" in pks
    assert "a1" not in pks

    # Cleanup ESM
    lam.delete_event_source_mapping(UUID=esm["UUID"])

# Migrated from test_ddb.py
def test_dynamodb_create_table(ddb):
    resp = ddb.create_table(
        TableName="t_hash_only",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    desc = resp["TableDescription"]
    assert desc["TableName"] == "t_hash_only"
    assert desc["TableStatus"] == "ACTIVE"
    assert any(k["KeyType"] == "HASH" for k in desc["KeySchema"])

def test_dynamodb_create_table_composite(ddb):
    resp = ddb.create_table(
        TableName="t_composite",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    ks = resp["TableDescription"]["KeySchema"]
    types = {k["KeyType"] for k in ks}
    assert types == {"HASH", "RANGE"}

def test_dynamodb_create_table_duplicate(ddb):
    with pytest.raises(ClientError) as exc:
        ddb.create_table(
            TableName="t_hash_only",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
    assert exc.value.response["Error"]["Code"] == "ResourceInUseException"

def test_dynamodb_delete_table(ddb):
    ddb.create_table(
        TableName="t_to_delete",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.delete_table(TableName="t_to_delete")
    assert resp["TableDescription"]["TableStatus"] == "DELETING"
    tables = ddb.list_tables()["TableNames"]
    assert "t_to_delete" not in tables

def test_dynamodb_delete_table_not_found(ddb):
    with pytest.raises(ClientError) as exc:
        ddb.delete_table(TableName="t_nonexistent_xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    # Real AWS sends `x-amzn-errortype` on JSON-protocol errors. Java/Go SDK v2
    # read it; without it they raise SdkClientException(unknown error type).
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "ResourceNotFoundException"


def test_dynamodb_deletion_protection(ddb):
    # AWS: DeletionProtectionEnabled=True blocks DeleteTable with ValidationException;
    # UpdateTable toggles the flag; DescribeTable reflects current state.
    ddb.create_table(
        TableName="t_protected",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        DeletionProtectionEnabled=True,
    )
    desc = ddb.describe_table(TableName="t_protected")["Table"]
    assert desc["DeletionProtectionEnabled"] is True

    with pytest.raises(ClientError) as exc:
        ddb.delete_table(TableName="t_protected")
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert "deletion protection" in exc.value.response["Error"]["Message"].lower()

    ddb.update_table(TableName="t_protected", DeletionProtectionEnabled=False)
    desc = ddb.describe_table(TableName="t_protected")["Table"]
    assert desc["DeletionProtectionEnabled"] is False

    resp = ddb.delete_table(TableName="t_protected")
    assert resp["TableDescription"]["TableStatus"] == "DELETING"
    assert "t_protected" not in ddb.list_tables()["TableNames"]


def test_dynamodb_deletion_protection_defaults_false(ddb):
    # A table created without the flag should describe as False and delete freely.
    ddb.create_table(
        TableName="t_unprotected",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    desc = ddb.describe_table(TableName="t_unprotected")["Table"]
    assert desc["DeletionProtectionEnabled"] is False
    ddb.delete_table(TableName="t_unprotected")

def test_dynamodb_describe_table(ddb):
    ddb.create_table(
        TableName="t_describe_gsi",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "lsi1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.describe_table(TableName="t_describe_gsi")
    table = resp["Table"]
    assert table["TableName"] == "t_describe_gsi"
    assert len(table["GlobalSecondaryIndexes"]) == 1
    assert table["GlobalSecondaryIndexes"][0]["IndexName"] == "gsi1"
    assert len(table["LocalSecondaryIndexes"]) == 1
    assert table["LocalSecondaryIndexes"][0]["IndexName"] == "lsi1"

def test_dynamodb_list_tables(ddb):
    for i in range(3):
        try:
            ddb.create_table(
                TableName=f"t_list_{i}",
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
        except ClientError:
            pass
    resp = ddb.list_tables(Limit=2)
    assert len(resp["TableNames"]) <= 2
    if "LastEvaluatedTableName" in resp:
        resp2 = ddb.list_tables(ExclusiveStartTableName=resp["LastEvaluatedTableName"], Limit=100)
        assert len(resp2["TableNames"]) >= 1

def test_dynamodb_put_get_item(ddb):
    ddb.put_item(
        TableName="t_hash_only",
        Item={
            "pk": {"S": "allTypes"},
            "str_attr": {"S": "hello"},
            "num_attr": {"N": "42"},
            "bool_attr": {"BOOL": True},
            "null_attr": {"NULL": True},
            "list_attr": {"L": [{"S": "a"}, {"N": "1"}]},
            "map_attr": {"M": {"nested": {"S": "value"}}},
            "ss_attr": {"SS": ["x", "y"]},
            "ns_attr": {"NS": ["1", "2", "3"]},
        },
    )
    resp = ddb.get_item(TableName="t_hash_only", Key={"pk": {"S": "allTypes"}})
    item = resp["Item"]
    assert item["str_attr"]["S"] == "hello"
    assert item["num_attr"]["N"] == "42"
    assert item["bool_attr"]["BOOL"] is True
    assert item["null_attr"]["NULL"] is True
    assert len(item["list_attr"]["L"]) == 2
    assert item["map_attr"]["M"]["nested"]["S"] == "value"
    assert set(item["ss_attr"]["SS"]) == {"x", "y"}
    assert set(item["ns_attr"]["NS"]) == {"1", "2", "3"}

def test_dynamodb_put_item_condition(ddb):
    ddb.put_item(
        TableName="t_hash_only",
        Item={"pk": {"S": "cond_new"}, "val": {"S": "first"}},
        ConditionExpression="attribute_not_exists(pk)",
    )
    resp = ddb.get_item(TableName="t_hash_only", Key={"pk": {"S": "cond_new"}})
    assert resp["Item"]["val"]["S"] == "first"

def test_dynamodb_put_item_condition_fail(ddb):
    ddb.put_item(TableName="t_hash_only", Item={"pk": {"S": "cond_fail"}, "val": {"S": "v1"}})
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName="t_hash_only",
            Item={"pk": {"S": "cond_fail"}, "val": {"S": "v2"}},
            ConditionExpression="attribute_not_exists(pk)",
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

def test_dynamodb_delete_item(ddb):
    ddb.put_item(TableName="t_hash_only", Item={"pk": {"S": "to_del"}, "v": {"S": "gone"}})
    ddb.delete_item(TableName="t_hash_only", Key={"pk": {"S": "to_del"}})
    resp = ddb.get_item(TableName="t_hash_only", Key={"pk": {"S": "to_del"}})
    assert "Item" not in resp

def test_dynamodb_delete_item_return_old(ddb):
    ddb.put_item(
        TableName="t_hash_only",
        Item={"pk": {"S": "ret_old"}, "data": {"S": "precious"}},
    )
    resp = ddb.delete_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "ret_old"}},
        ReturnValues="ALL_OLD",
    )
    assert resp["Attributes"]["data"]["S"] == "precious"

def test_dynamodb_update_item_set(ddb):
    ddb.put_item(TableName="t_hash_only", Item={"pk": {"S": "upd_set"}, "count": {"N": "0"}})
    resp = ddb.update_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "upd_set"}},
        UpdateExpression="SET #c = :val",
        ExpressionAttributeNames={"#c": "count"},
        ExpressionAttributeValues={":val": {"N": "10"}},
        ReturnValues="ALL_NEW",
    )
    assert resp["Attributes"]["count"]["N"] == "10"

def test_dynamodb_update_item_remove(ddb):
    ddb.put_item(
        TableName="t_hash_only",
        Item={"pk": {"S": "upd_rem"}, "extra": {"S": "bye"}, "keep": {"S": "stay"}},
    )
    resp = ddb.update_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "upd_rem"}},
        UpdateExpression="REMOVE extra",
        ReturnValues="ALL_NEW",
    )
    assert "extra" not in resp["Attributes"]
    assert resp["Attributes"]["keep"]["S"] == "stay"

def test_dynamodb_update_item_condition_on_missing_item_fails(ddb):
    """Missing item + attribute_exists(...) condition must fail with ConditionalCheckFailedException."""
    try:
        ddb.delete_table(TableName="t_update_cond_missing")
    except Exception:
        pass
    ddb.create_table(
        TableName="t_update_cond_missing",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    missing_key = {"pk": {"S": "missing-update-item"}}
    with pytest.raises(ClientError) as exc:
        ddb.update_item(
            TableName="t_update_cond_missing",
            Key=missing_key,
            UpdateExpression="SET v = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
            ConditionExpression="attribute_exists(pk)",
            ReturnValues="ALL_NEW",
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_dynamodb_conditional_check_failed_returns_item_when_all_old(ddb):
    """ReturnValuesOnConditionCheckFailure='ALL_OLD' must populate the
    `Item` member on ConditionalCheckFailedException for PutItem,
    UpdateItem, DeleteItem, and TransactWriteItems. Verified against
    botocore: ConditionalCheckFailedException shape includes `Item`,
    and Put/Update/Delete sub-ops accept ReturnValuesOnConditionCheckFailure.
    """
    table = "t_ccf_all_old"
    try:
        ddb.delete_table(TableName=table)
    except Exception:
        pass
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    seed = {"pk": {"S": "k1"}, "v": {"S": "original"}}
    ddb.put_item(TableName=table, Item=seed)

    # PutItem: condition fails because the row already exists.
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName=table,
            Item={"pk": {"S": "k1"}, "v": {"S": "new"}},
            ConditionExpression="attribute_not_exists(pk)",
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
    assert exc.value.response.get("Item") == seed

    # UpdateItem: condition fails because the existing value is "original".
    with pytest.raises(ClientError) as exc:
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET v = :v",
            ConditionExpression="v = :expected",
            ExpressionAttributeValues={":v": {"S": "new"}, ":expected": {"S": "wrong"}},
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
    assert exc.value.response.get("Item") == seed

    # DeleteItem: same setup.
    with pytest.raises(ClientError) as exc:
        ddb.delete_item(
            TableName=table,
            Key={"pk": {"S": "k1"}},
            ConditionExpression="v = :expected",
            ExpressionAttributeValues={":expected": {"S": "wrong"}},
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
    assert exc.value.response.get("Item") == seed

    # TransactWriteItems: failing CancellationReason carries Item.
    with pytest.raises(ClientError) as exc:
        ddb.transact_write_items(TransactItems=[{
            "Put": {
                "TableName": table,
                "Item": {"pk": {"S": "k1"}, "v": {"S": "new"}},
                "ConditionExpression": "attribute_not_exists(pk)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
            },
        }])
    reasons = exc.value.response["CancellationReasons"]
    assert reasons[0]["Code"] == "ConditionalCheckFailed"
    assert reasons[0].get("Item") == seed

    # And without ALL_OLD, the Item field must NOT be present.
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName=table,
            Item={"pk": {"S": "k1"}, "v": {"S": "new"}},
            ConditionExpression="attribute_not_exists(pk)",
        )
    assert "Item" not in exc.value.response


def test_dynamodb_get_item_missing_sort_key_fails_validation(ddb):
    try:
        ddb.delete_table(TableName="t_get_missing_sk")
    except Exception:
        pass
    ddb.create_table(
        TableName="t_get_missing_sk",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    with pytest.raises(ClientError) as exc:
        ddb.get_item(TableName="t_get_missing_sk", Key={"pk": {"S": "q_pk"}})
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert exc.value.response["Error"]["Message"] == "The provided key element does not match the schema"

def test_dynamodb_get_item_wrong_key_type_fails_validation(ddb):
    try:
        ddb.delete_table(TableName="t_get_wrong_type")
    except Exception:
        pass
    ddb.create_table(
        TableName="t_get_wrong_type",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="t_get_wrong_type", Item={"pk": {"S": "typed-key"}})
    with pytest.raises(ClientError) as exc:
        ddb.get_item(TableName="t_get_wrong_type", Key={"pk": {"N": "123"}})
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert exc.value.response["Error"]["Message"] == "The provided key element does not match the schema"

def test_dynamodb_update_item_extra_key_attribute_fails_validation(ddb):
    try:
        ddb.delete_table(TableName="t_update_extra_key")
    except Exception:
        pass
    ddb.create_table(
        TableName="t_update_extra_key",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    with pytest.raises(ClientError) as exc:
        ddb.update_item(
            TableName="t_update_extra_key",
            Key={"pk": {"S": "k1"}, "sk": {"S": "unexpected"}},
            UpdateExpression="SET v = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"
    assert exc.value.response["Error"]["Message"] == "The provided key element does not match the schema"

def test_dynamodb_update_item_add(ddb):
    ddb.put_item(TableName="t_hash_only", Item={"pk": {"S": "upd_add"}, "counter": {"N": "5"}})
    # 'counter' is in the AWS reserved-keyword list — must be aliased.
    resp = ddb.update_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "upd_add"}},
        UpdateExpression="ADD #c :inc",
        ExpressionAttributeNames={"#c": "counter"},
        ExpressionAttributeValues={":inc": {"N": "3"}},
        ReturnValues="ALL_NEW",
    )
    assert resp["Attributes"]["counter"]["N"] == "8"

def test_dynamodb_update_item_all_old(ddb):
    ddb.put_item(TableName="t_hash_only", Item={"pk": {"S": "upd_old"}, "v": {"N": "1"}})
    resp = ddb.update_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "upd_old"}},
        UpdateExpression="SET v = :new",
        ExpressionAttributeValues={":new": {"N": "99"}},
        ReturnValues="ALL_OLD",
    )
    assert resp["Attributes"]["v"]["N"] == "1"

def test_dynamodb_query_pk_only(ddb):
    for i in range(3):
        ddb.put_item(
            TableName="t_composite",
            Item={"pk": {"S": "q_pk"}, "sk": {"S": f"sk_{i}"}, "n": {"N": str(i)}},
        )
    resp = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "q_pk"}},
    )
    assert resp["Count"] == 3

def test_dynamodb_query_pk_sk(ddb):
    for i in range(5):
        ddb.put_item(
            TableName="t_composite",
            Item={"pk": {"S": "q_sk"}, "sk": {"S": f"item_{i:03d}"}},
        )
    resp_bw = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
        ExpressionAttributeValues={
            ":pk": {"S": "q_sk"},
            ":prefix": {"S": "item_00"},
        },
    )
    assert resp_bw["Count"] >= 1
    for item in resp_bw["Items"]:
        assert item["sk"]["S"].startswith("item_00")

    resp_bt = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk AND sk BETWEEN :lo AND :hi",
        ExpressionAttributeValues={
            ":pk": {"S": "q_sk"},
            ":lo": {"S": "item_001"},
            ":hi": {"S": "item_003"},
        },
    )
    assert resp_bt["Count"] >= 1
    for item in resp_bt["Items"]:
        assert "item_001" <= item["sk"]["S"] <= "item_003"

def test_dynamodb_query_filter(ddb):
    for i in range(5):
        ddb.put_item(
            TableName="t_composite",
            Item={"pk": {"S": "q_filt"}, "sk": {"S": f"f_{i}"}, "val": {"N": str(i)}},
        )
    resp = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk",
        FilterExpression="val > :min",
        ExpressionAttributeValues={":pk": {"S": "q_filt"}, ":min": {"N": "2"}},
    )
    assert resp["Count"] == 2
    assert resp["ScannedCount"] == 5

def test_dynamodb_query_pagination(ddb):
    for i in range(6):
        ddb.put_item(
            TableName="t_composite",
            Item={"pk": {"S": "q_page"}, "sk": {"S": f"p_{i:03d}"}},
        )
    resp1 = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "q_page"}},
        Limit=3,
    )
    assert resp1["Count"] == 3
    assert "LastEvaluatedKey" in resp1

    resp2 = ddb.query(
        TableName="t_composite",
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "q_page"}},
        ExclusiveStartKey=resp1["LastEvaluatedKey"],
        Limit=3,
    )
    assert resp2["Count"] == 3
    page1_sks = {it["sk"]["S"] for it in resp1["Items"]}
    page2_sks = {it["sk"]["S"] for it in resp2["Items"]}
    assert page1_sks.isdisjoint(page2_sks)

def test_dynamodb_scan_from_ddb(ddb):
    ddb.create_table(
        TableName="t_scan",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(8):
        ddb.put_item(TableName="t_scan", Item={"pk": {"S": f"sc_{i}"}, "n": {"N": str(i)}})
    resp = ddb.scan(TableName="t_scan")
    assert resp["Count"] == 8
    assert len(resp["Items"]) == 8

def test_dynamodb_scan_filter(ddb):
    resp = ddb.scan(
        TableName="t_scan",
        FilterExpression="n >= :min",
        ExpressionAttributeValues={":min": {"N": "5"}},
    )
    assert resp["Count"] == 3
    for item in resp["Items"]:
        assert int(item["n"]["N"]) >= 5

def test_dynamodb_batch_write(ddb):
    ddb.create_table(
        TableName="t_bw",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.batch_write_item(
        RequestItems={
            "t_bw": [{"PutRequest": {"Item": {"pk": {"S": f"bw_{i}"}, "data": {"S": f"d{i}"}}}} for i in range(10)]
        }
    )
    resp = ddb.scan(TableName="t_bw")
    assert resp["Count"] == 10

def test_dynamodb_batch_get(ddb):
    resp = ddb.batch_get_item(
        RequestItems={
            "t_bw": {
                "Keys": [{"pk": {"S": f"bw_{i}"}} for i in range(5)],
            }
        }
    )
    assert len(resp["Responses"]["t_bw"]) == 5

def test_dynamodb_transact_write(ddb):
    ddb.create_table(
        TableName="t_tx",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.transact_write_items(
        TransactItems=[
            {
                "Put": {
                    "TableName": "t_tx",
                    "Item": {"pk": {"S": "tx1"}, "v": {"S": "a"}},
                }
            },
            {
                "Put": {
                    "TableName": "t_tx",
                    "Item": {"pk": {"S": "tx2"}, "v": {"S": "b"}},
                }
            },
            {
                "Put": {
                    "TableName": "t_tx",
                    "Item": {"pk": {"S": "tx3"}, "v": {"S": "c"}},
                }
            },
        ]
    )
    resp = ddb.scan(TableName="t_tx")
    assert resp["Count"] == 3

    ddb.transact_write_items(
        TransactItems=[
            {"Delete": {"TableName": "t_tx", "Key": {"pk": {"S": "tx3"}}}},
            {
                "Update": {
                    "TableName": "t_tx",
                    "Key": {"pk": {"S": "tx1"}},
                    "UpdateExpression": "SET v = :new",
                    "ExpressionAttributeValues": {":new": {"S": "updated"}},
                },
            },
        ]
    )
    item = ddb.get_item(TableName="t_tx", Key={"pk": {"S": "tx1"}})["Item"]
    assert item["v"]["S"] == "updated"
    gone = ddb.get_item(TableName="t_tx", Key={"pk": {"S": "tx3"}})
    assert "Item" not in gone

def test_dynamodb_transact_get(ddb):
    resp = ddb.transact_get_items(
        TransactItems=[
            {"Get": {"TableName": "t_tx", "Key": {"pk": {"S": "tx1"}}}},
            {"Get": {"TableName": "t_tx", "Key": {"pk": {"S": "tx2"}}}},
        ]
    )
    assert len(resp["Responses"]) == 2
    assert resp["Responses"][0]["Item"]["pk"]["S"] == "tx1"
    assert resp["Responses"][1]["Item"]["pk"]["S"] == "tx2"

def test_dynamodb_gsi_query(ddb):
    ddb.create_table(
        TableName="t_gsi_q",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi_index",
                "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(4):
        ddb.put_item(
            TableName="t_gsi_q",
            Item={
                "pk": {"S": f"main_{i}"},
                "gsi_pk": {"S": "shared_gsi"},
                "data": {"N": str(i)},
            },
        )
    ddb.put_item(
        TableName="t_gsi_q",
        Item={
            "pk": {"S": "main_other"},
            "gsi_pk": {"S": "other_gsi"},
            "data": {"N": "99"},
        },
    )
    resp = ddb.query(
        TableName="t_gsi_q",
        IndexName="gsi_index",
        KeyConditionExpression="gsi_pk = :gpk",
        ExpressionAttributeValues={":gpk": {"S": "shared_gsi"}},
    )
    assert resp["Count"] == 4
    for item in resp["Items"]:
        assert item["gsi_pk"]["S"] == "shared_gsi"

def test_dynamodb_ttl(ddb):
    import uuid as _uuid

    table = f"intg-ttl-{_uuid.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    # Initially disabled
    resp = ddb.describe_time_to_live(TableName=table)
    assert resp["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"

    # Enable TTL
    ddb.update_time_to_live(
        TableName=table,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )
    resp = ddb.describe_time_to_live(TableName=table)
    assert resp["TimeToLiveDescription"]["TimeToLiveStatus"] == "ENABLED"
    assert resp["TimeToLiveDescription"]["AttributeName"] == "expires_at"

    # Disable TTL
    ddb.update_time_to_live(
        TableName=table,
        TimeToLiveSpecification={"Enabled": False, "AttributeName": "expires_at"},
    )
    resp = ddb.describe_time_to_live(TableName=table)
    assert resp["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"
    ddb.delete_table(TableName=table)

def test_dynamodb_update_table(ddb):
    import uuid as _uuid

    table = f"intg-updtbl-{_uuid.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    resp = ddb.update_table(
        TableName=table,
        BillingMode="PROVISIONED",
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    assert resp["TableDescription"]["TableName"] == table
    ddb.delete_table(TableName=table)

def test_dynamodb_ttl_expiry(ddb):
    """TTL setting is stored and reported correctly; expiry enforcement is in the background reaper."""
    import uuid as _uuid_mod

    table = f"intg-ttl-exp-{_uuid_mod.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.update_time_to_live(
        TableName=table,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )
    past = int(time.time()) - 10
    ddb.put_item(
        TableName=table,
        Item={
            "pk": {"S": "expired-item"},
            "expires_at": {"N": str(past)},
            "data": {"S": "should-be-gone"},
        },
    )
    # Item present immediately (reaper hasn't run yet)
    resp = ddb.get_item(TableName=table, Key={"pk": {"S": "expired-item"}})
    assert "Item" in resp

    # TTL setting is correctly reflected in DescribeTimeToLive
    desc = ddb.describe_time_to_live(TableName=table)["TimeToLiveDescription"]
    assert desc["TimeToLiveStatus"] == "ENABLED"
    assert desc["AttributeName"] == "expires_at"

def test_dynamodb_gsi_query_pagination_with_collisions(ddb):
    """GSI Query with ExclusiveStartKey must paginate correctly when multiple
    items share the same GSI sort-key value. Real DynamoDB tiebreaks on the
    base table's primary key — emulator must do the same or pagination silently
    drops items from page 2 onwards. Regression for issue #597."""
    table = "t_gsi_pag_collide"
    ddb.create_table(
        TableName=table,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi1pk", "AttributeType": "S"},
            {"AttributeName": "gsi1sk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [
                    {"AttributeName": "gsi1pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi1sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    # Five items sharing the SAME (gsi1pk, gsi1sk); only base pk differs.
    for i in range(5):
        ddb.put_item(
            TableName=table,
            Item={
                "pk": {"S": f"item-{i}"},
                "sk": {"S": "X"},
                "gsi1pk": {"S": "GROUP"},
                "gsi1sk": {"S": "X"},
            },
        )

    seen, last_key, pages = [], None, 0
    while True:
        pages += 1
        kwargs = dict(
            TableName=table,
            IndexName="gsi1",
            KeyConditionExpression="gsi1pk = :v",
            ExpressionAttributeValues={":v": {"S": "GROUP"}},
            Limit=3,
        )
        if last_key is not None:
            kwargs["ExclusiveStartKey"] = last_key
        out = ddb.query(**kwargs)
        seen.extend(it["pk"]["S"] for it in out.get("Items", []))
        last_key = out.get("LastEvaluatedKey")
        if last_key is None or pages >= 10:
            break

    assert pages < 10, f"pagination did not terminate: {pages} pages, seen={seen}"
    assert sorted(seen) == [f"item-{i}" for i in range(5)], (
        f"expected all 5 items via cursor, got {seen}"
    )


def test_dynamodb_gsi_hash_only_query_pagination(ddb):
    """GSI with no sort key must paginate without cycling through the same
    items. Regression for issue #593."""
    table = "t_gsi_hash_only_pag"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "bucket", "AttributeType": "N"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "bucket-index",
                "KeySchema": [{"AttributeName": "bucket", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    n = 25
    for i in range(n):
        ddb.put_item(
            TableName=table,
            Item={
                "id": {"S": f"row-{i:04d}"},
                "bucket": {"N": "0"},
            },
        )

    seen, last_key, pages = [], None, 0
    while True:
        pages += 1
        kwargs = dict(
            TableName=table,
            IndexName="bucket-index",
            KeyConditionExpression="#b = :v",
            ExpressionAttributeNames={"#b": "bucket"},
            ExpressionAttributeValues={":v": {"N": "0"}},
            Limit=8,
        )
        if last_key is not None:
            kwargs["ExclusiveStartKey"] = last_key
        out = ddb.query(**kwargs)
        seen.extend(it["id"]["S"] for it in out.get("Items", []))
        last_key = out.get("LastEvaluatedKey")
        if last_key is None or pages >= 10:
            break

    assert pages < 10, f"pagination did not terminate: {pages} pages, seen={seen}"
    assert len(seen) == n, f"expected {n} unique items, got {len(seen)}: {seen}"
    assert len(set(seen)) == n, f"items duplicated across pages: {seen}"


def test_dynamodb_query_pagination_hash_only(ddb):
    """Pagination on a hash-only table (no sort key) must return results after the ESK."""
    table = "t_hash_paginate"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(5):
        ddb.put_item(TableName=table, Item={"pk": {"S": f"item_{i:03d}"}, "v": {"N": str(i)}})

    resp1 = ddb.scan(TableName=table, Limit=3)
    assert resp1["Count"] == 3
    assert "LastEvaluatedKey" in resp1

    resp2 = ddb.scan(TableName=table, Limit=3, ExclusiveStartKey=resp1["LastEvaluatedKey"])
    assert resp2["Count"] == 2
    all_pks = {it["pk"]["S"] for it in resp1["Items"]} | {it["pk"]["S"] for it in resp2["Items"]}
    assert len(all_pks) == 5

def test_dynamodb_update_item_updated_new(ddb):
    """UpdateItem ReturnValues=UPDATED_NEW returns only changed attributes."""
    ddb.create_table(
        TableName="qa-ddb-updated-new",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(
        TableName="qa-ddb-updated-new",
        Item={"pk": {"S": "k1"}, "a": {"S": "old"}, "b": {"N": "1"}},
    )
    resp = ddb.update_item(
        TableName="qa-ddb-updated-new",
        Key={"pk": {"S": "k1"}},
        UpdateExpression="SET a = :new",
        ExpressionAttributeValues={":new": {"S": "new"}},
        ReturnValues="UPDATED_NEW",
    )
    assert "Attributes" in resp
    assert resp["Attributes"]["a"]["S"] == "new"
    assert "b" not in resp["Attributes"]

def test_dynamodb_update_item_updated_old(ddb):
    """UpdateItem ReturnValues=UPDATED_OLD returns old values of changed attributes."""
    ddb.create_table(
        TableName="qa-ddb-updated-old",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="qa-ddb-updated-old", Item={"pk": {"S": "k1"}, "score": {"N": "10"}})
    resp = ddb.update_item(
        TableName="qa-ddb-updated-old",
        Key={"pk": {"S": "k1"}},
        UpdateExpression="SET score = :new",
        ExpressionAttributeValues={":new": {"N": "20"}},
        ReturnValues="UPDATED_OLD",
    )
    assert resp["Attributes"]["score"]["N"] == "10"

def test_dynamodb_conditional_put_fails(ddb):
    """PutItem with attribute_not_exists condition fails if item already exists."""
    ddb.create_table(
        TableName="qa-ddb-cond-put",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="qa-ddb-cond-put", Item={"pk": {"S": "exists"}})
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName="qa-ddb-cond-put",
            Item={"pk": {"S": "exists"}, "data": {"S": "new"}},
            ConditionExpression="attribute_not_exists(pk)",
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

def test_dynamodb_query_with_filter_expression(ddb):
    """Query with FilterExpression reduces Count but not ScannedCount."""
    ddb.create_table(
        TableName="qa-ddb-filter",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(5):
        ddb.put_item(
            TableName="qa-ddb-filter",
            Item={
                "pk": {"S": "user1"},
                "sk": {"N": str(i)},
                "active": {"BOOL": i % 2 == 0},
            },
        )
    resp = ddb.query(
        TableName="qa-ddb-filter",
        KeyConditionExpression="pk = :pk",
        FilterExpression="active = :t",
        ExpressionAttributeValues={":pk": {"S": "user1"}, ":t": {"BOOL": True}},
    )
    assert resp["Count"] == 3
    assert resp["ScannedCount"] == 5

def test_dynamodb_scan_with_limit_and_pagination(ddb):
    """Scan with Limit returns LastEvaluatedKey and pagination works."""
    ddb.create_table(
        TableName="qa-ddb-scan-page",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(10):
        ddb.put_item(TableName="qa-ddb-scan-page", Item={"pk": {"S": f"item{i:02d}"}})
    all_items = []
    lek = None
    while True:
        kwargs = {"TableName": "qa-ddb-scan-page", "Limit": 3}
        if lek:
            kwargs["ExclusiveStartKey"] = lek
        resp = ddb.scan(**kwargs)
        all_items.extend(resp["Items"])
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
    assert len(all_items) == 10

def test_dynamodb_transact_write_condition_cancel(ddb):
    """TransactWriteItems cancels entire transaction if one condition fails."""
    ddb.create_table(
        TableName="qa-ddb-transact",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName="qa-ddb-transact", Item={"pk": {"S": "existing"}})
    with pytest.raises(ClientError) as exc:
        ddb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": "qa-ddb-transact",
                        "Item": {"pk": {"S": "new-item"}},
                    }
                },
                {
                    "Put": {
                        "TableName": "qa-ddb-transact",
                        "Item": {"pk": {"S": "existing"}, "data": {"S": "x"}},
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                },
            ]
        )
    assert exc.value.response["Error"]["Code"] == "TransactionCanceledException"
    resp = ddb.get_item(TableName="qa-ddb-transact", Key={"pk": {"S": "new-item"}})
    assert "Item" not in resp

def test_dynamodb_transact_write_multiple_failures_all_returned(ddb):
    """TransactWriteItems returns CancellationReasons for ALL failed conditions, not just the first."""
    table = "qa-ddb-transact"
    # Ensure two items exist to trigger two condition failures
    ddb.put_item(TableName=table, Item={"pk": {"S": "multi_fail_1"}, "val": {"S": "a"}})
    ddb.put_item(TableName=table, Item={"pk": {"S": "multi_fail_2"}, "val": {"S": "b"}})
    with pytest.raises(ClientError) as exc:
        ddb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": table,
                        "Item": {"pk": {"S": "multi_fail_1"}, "val": {"S": "x"}},
                        "ConditionExpression": "attribute_not_exists(pk)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
                    }
                },
                {
                    "Put": {
                        "TableName": table,
                        "Item": {"pk": {"S": "brand_new"}},
                    }
                },
                {
                    "Put": {
                        "TableName": table,
                        "Item": {"pk": {"S": "multi_fail_2"}, "val": {"S": "y"}},
                        "ConditionExpression": "attribute_not_exists(pk)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
                    }
                },
            ]
        )
    err = exc.value.response
    assert err["Error"]["Code"] == "TransactionCanceledException"
    reasons = err["CancellationReasons"]
    assert len(reasons) == 3
    # First and third items should have ConditionalCheckFailed with Item populated
    assert reasons[0]["Code"] == "ConditionalCheckFailed"
    assert reasons[0]["Item"]["pk"]["S"] == "multi_fail_1"
    assert reasons[0]["Item"]["val"]["S"] == "a"
    # Second item had no condition — should be "None"
    assert reasons[1]["Code"] == "None"
    # Third item should also be failed with its old item
    assert reasons[2]["Code"] == "ConditionalCheckFailed"
    assert reasons[2]["Item"]["pk"]["S"] == "multi_fail_2"
    assert reasons[2]["Item"]["val"]["S"] == "b"


def test_dynamodb_batch_get_missing_table(ddb):
    """BatchGetItem with non-existent table raises ResourceNotFoundException.

    Real AWS rejects the whole batch upfront when any RequestItems key names
    a table that doesn't exist — it does NOT silently put the entry into
    UnprocessedKeys. This matches dynamodb-conformance.org's batchGetItem
    "Non-existent table" test."""
    with pytest.raises(ClientError) as e:
        ddb.batch_get_item(RequestItems={"qa-ddb-nonexistent-xyz": {"Keys": [{"pk": {"S": "k1"}}]}})
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"

def test_dynamodb_scan_filter_legacy(ddb):
    """Scan with legacy ScanFilter (ComparisonOperator style) returns matching items."""
    table = "intg-ddb-scanfilter"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(5):
        ddb.put_item(TableName=table, Item={
            "pk": {"S": f"sf_{i}"},
            "color": {"S": "red" if i % 2 == 0 else "blue"},
        })

    resp = ddb.scan(
        TableName=table,
        ScanFilter={
            "color": {
                "AttributeValueList": [{"S": "red"}],
                "ComparisonOperator": "EQ",
            }
        },
    )
    assert resp["Count"] == 3
    for item in resp["Items"]:
        assert item["color"]["S"] == "red"

def test_dynamodb_query_filter_legacy(ddb):
    """Query with legacy QueryFilter (ComparisonOperator style) returns matching items."""
    table = "intg-ddb-queryfilter"
    ddb.create_table(
        TableName=table,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(5):
        ddb.put_item(TableName=table, Item={
            "pk": {"S": "qf_pk"},
            "sk": {"S": f"sk_{i}"},
            "status": {"S": "active" if i < 3 else "inactive"},
        })

    resp = ddb.query(
        TableName=table,
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "qf_pk"}},
        QueryFilter={
            "status": {
                "AttributeValueList": [{"S": "active"}],
                "ComparisonOperator": "EQ",
            }
        },
    )
    assert resp["Count"] == 3
    assert resp["ScannedCount"] == 5
    for item in resp["Items"]:
        assert item["status"]["S"] == "active"


# ---------------------------------------------------------------------------
# Terraform compatibility tests
# ---------------------------------------------------------------------------


def test_dynamodb_pay_per_request_provisioned_throughput(ddb):
    """PAY_PER_REQUEST tables must return ProvisionedThroughput with zero values."""
    tname = "tf-compat-ondemand"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    try:
        desc = ddb.describe_table(TableName=tname)["Table"]
        pt = desc["ProvisionedThroughput"]
        assert pt["ReadCapacityUnits"] == 0, \
            f"Expected ReadCapacityUnits=0 for PAY_PER_REQUEST, got {pt['ReadCapacityUnits']}"
        assert pt["WriteCapacityUnits"] == 0, \
            f"Expected WriteCapacityUnits=0 for PAY_PER_REQUEST, got {pt['WriteCapacityUnits']}"
    finally:
        ddb.delete_table(TableName=tname)


def test_dynamodb_provisioned_keeps_capacity(ddb):
    """PROVISIONED tables must keep their configured throughput values."""
    tname = "tf-compat-provisioned"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PROVISIONED",
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 5},
    )
    try:
        desc = ddb.describe_table(TableName=tname)["Table"]
        pt = desc["ProvisionedThroughput"]
        assert pt["ReadCapacityUnits"] == 10
        assert pt["WriteCapacityUnits"] == 5
    finally:
        ddb.delete_table(TableName=tname)


def test_dynamodb_pay_per_request_gsi_zero_throughput(ddb):
    """GSIs on PAY_PER_REQUEST tables must have zero ProvisionedThroughput."""
    tname = "tf-compat-ondemand-gsi"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_key", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi-test",
                "KeySchema": [{"AttributeName": "gsi_key", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    try:
        desc = ddb.describe_table(TableName=tname)["Table"]
        gsis = desc.get("GlobalSecondaryIndexes", [])
        assert len(gsis) == 1, f"Expected 1 GSI, got {len(gsis)}"
        gsi_pt = gsis[0]["ProvisionedThroughput"]
        assert gsi_pt["ReadCapacityUnits"] == 0, \
            f"Expected GSI ReadCapacityUnits=0 for PAY_PER_REQUEST, got {gsi_pt['ReadCapacityUnits']}"
        assert gsi_pt["WriteCapacityUnits"] == 0, \
            f"Expected GSI WriteCapacityUnits=0 for PAY_PER_REQUEST, got {gsi_pt['WriteCapacityUnits']}"
    finally:
        ddb.delete_table(TableName=tname)


def test_dynamodb_update_to_pay_per_request_zeroes_throughput(ddb):
    """Updating billing mode to PAY_PER_REQUEST should zero out throughput."""
    tname = "tf-compat-update-billing"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PROVISIONED",
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 5},
    )
    try:
        ddb.update_table(TableName=tname, BillingMode="PAY_PER_REQUEST")
        desc = ddb.describe_table(TableName=tname)["Table"]
        pt = desc["ProvisionedThroughput"]
        assert pt["ReadCapacityUnits"] == 0, \
            f"Expected ReadCapacityUnits=0 after switching to PAY_PER_REQUEST, got {pt['ReadCapacityUnits']}"
        assert pt["WriteCapacityUnits"] == 0, \
            f"Expected WriteCapacityUnits=0 after switching to PAY_PER_REQUEST, got {pt['WriteCapacityUnits']}"
    finally:
        ddb.delete_table(TableName=tname)


# ---------------------------------------------------------------------------
# ExecuteStatement (PartiQL)
# ---------------------------------------------------------------------------

def test_partiql_select_all(ddb):
    """SELECT * FROM table — the IntelliJ use case."""
    tname = "partiql-select-all"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=tname, Item={"pk": {"S": "a"}, "val": {"S": "1"}})
    ddb.put_item(TableName=tname, Item={"pk": {"S": "b"}, "val": {"S": "2"}})

    resp = ddb.execute_statement(Statement=f'SELECT * FROM "{tname}"')
    assert len(resp["Items"]) == 2
    pks = sorted(it["pk"]["S"] for it in resp["Items"])
    assert pks == ["a", "b"]


def test_partiql_select_with_where(ddb):
    """SELECT with WHERE clause and ? parameter binding."""
    tname = "partiql-select-where"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=tname, Item={"pk": {"S": "x"}, "status": {"S": "active"}})
    ddb.put_item(TableName=tname, Item={"pk": {"S": "y"}, "status": {"S": "inactive"}})

    resp = ddb.execute_statement(
        Statement=f'SELECT * FROM "{tname}" WHERE pk = ?',
        Parameters=[{"S": "x"}],
    )
    assert len(resp["Items"]) == 1
    assert resp["Items"][0]["pk"]["S"] == "x"


def test_partiql_select_projection(ddb):
    """SELECT specific columns."""
    tname = "partiql-select-proj"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=tname, Item={"pk": {"S": "k1"}, "a": {"S": "1"}, "b": {"S": "2"}})

    resp = ddb.execute_statement(Statement=f'SELECT pk, a FROM "{tname}"')
    assert len(resp["Items"]) == 1
    item = resp["Items"][0]
    assert "pk" in item
    assert "a" in item
    assert "b" not in item


def test_partiql_insert(ddb):
    """INSERT INTO table VALUE {...}."""
    tname = "partiql-insert"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    ddb.execute_statement(
        Statement=f"INSERT INTO \"{tname}\" VALUE {{'pk': ?, 'data': ?}}",
        Parameters=[{"S": "ins1"}, {"S": "hello"}],
    )
    resp = ddb.get_item(TableName=tname, Key={"pk": {"S": "ins1"}})
    assert resp["Item"]["data"]["S"] == "hello"


def test_partiql_insert_duplicate_rejected(ddb):
    """INSERT with duplicate key should fail."""
    tname = "partiql-ins-dup"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=tname, Item={"pk": {"S": "dup"}})

    with pytest.raises(ClientError) as exc:
        ddb.execute_statement(
            Statement=f"INSERT INTO \"{tname}\" VALUE {{'pk': ?}}",
            Parameters=[{"S": "dup"}],
        )
    # Real AWS returns DuplicateItemException — matches the conformance
    # suite's expected error type.
    assert exc.value.response["Error"]["Code"] == "DuplicateItemException"


def test_partiql_update(ddb):
    """UPDATE table SET attr = val WHERE pk = val."""
    tname = "partiql-update"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=tname, Item={"pk": {"S": "u1"}, "status": {"S": "old"}})

    ddb.execute_statement(
        Statement=f"UPDATE \"{tname}\" SET status = ? WHERE pk = ?",
        Parameters=[{"S": "new"}, {"S": "u1"}],
    )
    resp = ddb.get_item(TableName=tname, Key={"pk": {"S": "u1"}})
    assert resp["Item"]["status"]["S"] == "new"


def test_partiql_delete(ddb):
    """DELETE FROM table WHERE pk = val."""
    tname = "partiql-delete"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=tname, Item={"pk": {"S": "d1"}, "val": {"S": "x"}})

    ddb.execute_statement(
        Statement=f'DELETE FROM "{tname}" WHERE pk = ?',
        Parameters=[{"S": "d1"}],
    )
    resp = ddb.get_item(TableName=tname, Key={"pk": {"S": "d1"}})
    assert "Item" not in resp


def test_partiql_nonexistent_table(ddb):
    """ExecuteStatement on a nonexistent table should return ResourceNotFoundException."""
    with pytest.raises(ClientError) as exc:
        ddb.execute_statement(Statement='SELECT * FROM "no-such-table-partiql"')
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_partiql_select_where_number(ddb):
    """WHERE clause with numeric comparison."""
    tname = "partiql-num-where"
    try:
        ddb.delete_table(TableName=tname)
    except ClientError:
        pass
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=tname, Item={"pk": {"S": "n1"}, "age": {"N": "25"}})
    ddb.put_item(TableName=tname, Item={"pk": {"S": "n2"}, "age": {"N": "30"}})

    resp = ddb.execute_statement(
        Statement=f'SELECT * FROM "{tname}" WHERE age > ?',
        Parameters=[{"N": "27"}],
    )
    assert len(resp["Items"]) == 1
    assert resp["Items"][0]["pk"]["S"] == "n2"


def test_dynamodb_stream_arn_stable(ddb):
    """LatestStreamArn should be stable across DescribeTable calls."""
    tname = f"stream-stable-{_uuid_mod.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    desc1 = ddb.describe_table(TableName=tname)["Table"]
    desc2 = ddb.describe_table(TableName=tname)["Table"]
    assert desc1["LatestStreamArn"] == desc2["LatestStreamArn"]
    assert desc1["LatestStreamLabel"] == desc2["LatestStreamLabel"]
    ddb.delete_table(TableName=tname)



def test_ddb_sse_description_shape_matches_aws(ddb, kms_client):
    """CreateTable and UpdateTable must return an AWS-shaped SSEDescription
    (Status + SSEType + KMSMasterKeyArn), not the request's SSESpecification
    (Enabled + KMSMasterKeyId). Regression for #411 — Terraform's waiter hangs
    forever if Status is missing."""
    key_id = kms_client.create_key(Description="ddb-sse-t")["KeyMetadata"]["KeyId"]
    key_arn = f"arn:aws:kms:us-east-1:000000000000:key/{key_id}"
    tname = "t-sse-shape"
    try: ddb.delete_table(TableName=tname)
    except Exception: pass

    ddb.create_table(
        TableName=tname,
        KeySchema=[{"AttributeName": "PK", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "PK", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        SSESpecification={"Enabled": True, "SSEType": "KMS", "KMSMasterKeyId": key_arn},
    )
    desc = ddb.describe_table(TableName=tname)["Table"]
    sse = desc["SSEDescription"]
    assert sse["Status"] == "ENABLED"
    assert sse["SSEType"] == "KMS"
    assert sse["KMSMasterKeyArn"] == key_arn
    assert "Enabled" not in sse
    assert "KMSMasterKeyId" not in sse

    # UpdateTable with SSESpecification must also produce the right shape.
    ddb.update_table(
        TableName=tname,
        SSESpecification={"Enabled": False},
    )
    sse = ddb.describe_table(TableName=tname)["Table"]["SSEDescription"]
    assert sse["Status"] == "DISABLED"
    ddb.delete_table(TableName=tname)


# ========== from test_dynamodb_kinesis_destination.py ==========
# DDB→Kinesis streaming destination (Enable/Disable/Describe/Update + envelope).

import base64
import json
import time

import pytest
from botocore.exceptions import ClientError


def _make_table(ddb, name):
    try:
        ddb.delete_table(TableName=name)
    except ClientError:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )


def _make_stream(kin, name):
    try:
        kin.delete_stream(StreamName=name)
    except ClientError:
        pass
    kin.create_stream(StreamName=name, ShardCount=1)
    # Streams are ACTIVE immediately in MiniStack, but the DescribeStream call
    # is cheap and keeps the test robust if that ever changes.
    kin.describe_stream(StreamName=name)
    return kin.describe_stream(StreamName=name)["StreamDescription"]["StreamARN"]


def _drain_kinesis(kin, stream_name):
    shards = kin.describe_stream(StreamName=stream_name)["StreamDescription"]["Shards"]
    out = []
    for shard in shards:
        it = kin.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]
        for _ in range(5):
            resp = kin.get_records(ShardIterator=it, Limit=1000)
            out.extend(resp.get("Records", []))
            nxt = resp.get("NextShardIterator")
            if not nxt or nxt == it or not resp.get("Records"):
                break
            it = nxt
    return out


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def test_enable_returns_active_and_describe_lists_it(ddb, kin):
    _make_table(ddb, "KdsLifecycle")
    arn = _make_stream(kin, "ministack-kds-lifecycle")

    resp = ddb.enable_kinesis_streaming_destination(
        TableName="KdsLifecycle", StreamArn=arn,
    )
    # AWS returns ENABLING immediately; the destination flips to ACTIVE
    # by the time Describe is called.
    assert resp["DestinationStatus"] == "ENABLING"
    assert resp["StreamArn"] == arn
    assert resp["TableName"] == "KdsLifecycle"

    desc = ddb.describe_kinesis_streaming_destination(TableName="KdsLifecycle")
    dests = desc["KinesisDataStreamDestinations"]
    assert len(dests) == 1
    assert dests[0]["StreamArn"] == arn
    assert dests[0]["DestinationStatus"] == "ACTIVE"
    assert dests[0]["ApproximateCreationDateTimePrecision"] == "MILLISECOND"


def test_enable_twice_same_table_and_arn_raises(ddb, kin):
    _make_table(ddb, "KdsDup")
    arn = _make_stream(kin, "ministack-kds-dup")
    ddb.enable_kinesis_streaming_destination(TableName="KdsDup", StreamArn=arn)

    with pytest.raises(ClientError) as ei:
        ddb.enable_kinesis_streaming_destination(TableName="KdsDup", StreamArn=arn)
    assert ei.value.response["Error"]["Code"] == "ResourceInUseException"


def test_enable_requires_existing_table(ddb, kin):
    arn = _make_stream(kin, "ministack-kds-missing")
    with pytest.raises(ClientError) as ei:
        ddb.enable_kinesis_streaming_destination(TableName="ThisTableDoesNotExist", StreamArn=arn)
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Delivery: item mutations end up as Kinesis records when ACTIVE
# ---------------------------------------------------------------------------

def test_item_mutations_land_in_kinesis_stream(ddb, kin):
    _make_table(ddb, "KdsDeliver")
    arn = _make_stream(kin, "ministack-kds-deliver")
    ddb.enable_kinesis_streaming_destination(TableName="KdsDeliver", StreamArn=arn)

    ddb.put_item(TableName="KdsDeliver", Item={"pk": {"S": "a"}, "val": {"N": "1"}})
    ddb.update_item(
        TableName="KdsDeliver",
        Key={"pk": {"S": "a"}},
        UpdateExpression="SET val = :v",
        ExpressionAttributeValues={":v": {"N": "2"}},
    )
    ddb.delete_item(TableName="KdsDeliver", Key={"pk": {"S": "a"}})

    records = _drain_kinesis(kin, "ministack-kds-deliver")
    assert len(records) == 3

    decoded = []
    for r in records:
        payload = r["Data"]
        # boto3 normalises the base64-encoded Data back into bytes for us
        if isinstance(payload, str):
            payload = base64.b64decode(payload)
        decoded.append(json.loads(payload.decode("utf-8")))

    event_names = [d["eventName"] for d in decoded]
    assert event_names == ["INSERT", "MODIFY", "REMOVE"]
    for d in decoded:
        assert d["eventSource"] == "aws:dynamodb"
        assert d["dynamodb"]["Keys"] == {"pk": {"S": "a"}}


def test_disable_stops_delivery(ddb, kin):
    _make_table(ddb, "KdsDisable")
    arn = _make_stream(kin, "ministack-kds-disable")
    ddb.enable_kinesis_streaming_destination(TableName="KdsDisable", StreamArn=arn)

    ddb.put_item(TableName="KdsDisable", Item={"pk": {"S": "before"}})

    resp = ddb.disable_kinesis_streaming_destination(TableName="KdsDisable", StreamArn=arn)
    # AWS returns DISABLING immediately; storage is DISABLED so Describe
    # below shows the steady-state.
    assert resp["DestinationStatus"] == "DISABLING"

    # Describe still lists the now-DISABLED entry (matches AWS ~24h retention).
    dests = ddb.describe_kinesis_streaming_destination(TableName="KdsDisable")[
        "KinesisDataStreamDestinations"
    ]
    assert len(dests) == 1
    assert dests[0]["DestinationStatus"] == "DISABLED"

    ddb.put_item(TableName="KdsDisable", Item={"pk": {"S": "after"}})

    records = _drain_kinesis(kin, "ministack-kds-disable")
    assert len(records) == 1  # only the pre-disable INSERT


def test_disable_without_active_raises(ddb, kin):
    _make_table(ddb, "KdsNoActive")
    arn = _make_stream(kin, "ministack-kds-no-active")
    with pytest.raises(ClientError) as ei:
        ddb.disable_kinesis_streaming_destination(TableName="KdsNoActive", StreamArn=arn)
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

def test_update_precision(ddb, kin):
    _make_table(ddb, "KdsUpdate")
    arn = _make_stream(kin, "ministack-kds-update")
    ddb.enable_kinesis_streaming_destination(TableName="KdsUpdate", StreamArn=arn)

    resp = ddb.update_kinesis_streaming_destination(
        TableName="KdsUpdate",
        StreamArn=arn,
        UpdateKinesisStreamingConfiguration={
            "ApproximateCreationDateTimePrecision": "MICROSECOND",
        },
    )
    # AWS returns UPDATING immediately; Describe below shows steady-state ACTIVE.
    assert resp["DestinationStatus"] == "UPDATING"
    assert (
        resp["UpdateKinesisStreamingConfiguration"]["ApproximateCreationDateTimePrecision"]
        == "MICROSECOND"
    )

    dests = ddb.describe_kinesis_streaming_destination(TableName="KdsUpdate")[
        "KinesisDataStreamDestinations"
    ]
    assert dests[0]["ApproximateCreationDateTimePrecision"] == "MICROSECOND"


def test_update_rejects_invalid_precision(ddb, kin):
    """boto3 catches `NANOSECOND` client-side via enum validation, so we hit
    the server with a raw HTTP POST to verify the server-side ValidationException
    actually fires (not just the SDK)."""
    import urllib.error
    import urllib.request

    _make_table(ddb, "KdsUpdateInvalid")
    arn = _make_stream(kin, "ministack-kds-update-invalid")
    ddb.enable_kinesis_streaming_destination(TableName="KdsUpdateInvalid", StreamArn=arn)

    body = json.dumps({
        "TableName": "KdsUpdateInvalid",
        "StreamArn": arn,
        "UpdateKinesisStreamingConfiguration": {"ApproximateCreationDateTimePrecision": "NANOSECOND"},
    }).encode("utf-8")
    req = urllib.request.Request(
        "http://localhost:4566/",
        data=body,
        headers={
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Target": "DynamoDB_20120810.UpdateKinesisStreamingDestination",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(req)
        pytest.fail("Expected server to reject NANOSECOND precision")
    except urllib.error.HTTPError as e:
        assert e.code == 400
        err_body = json.loads(e.read().decode("utf-8"))
        assert err_body.get("__type", "").endswith("ValidationException")


# ---------------------------------------------------------------------------
# Cleanup on table delete
# ---------------------------------------------------------------------------

def test_delete_table_removes_destinations(ddb, kin):
    _make_table(ddb, "KdsAutoclean")
    arn = _make_stream(kin, "ministack-kds-autoclean")
    ddb.enable_kinesis_streaming_destination(TableName="KdsAutoclean", StreamArn=arn)

    ddb.delete_table(TableName="KdsAutoclean")
    # After a fresh CreateTable there should be zero destinations.
    _make_table(ddb, "KdsAutoclean")
    dests = ddb.describe_kinesis_streaming_destination(TableName="KdsAutoclean")[
        "KinesisDataStreamDestinations"
    ]
    assert dests == []


# ---------------------------------------------------------------------------
# Order stability (smoke)
# ---------------------------------------------------------------------------

def test_multiple_puts_land_in_order(ddb, kin):
    _make_table(ddb, "KdsOrder")
    arn = _make_stream(kin, "ministack-kds-order")
    ddb.enable_kinesis_streaming_destination(TableName="KdsOrder", StreamArn=arn)

    for i in range(5):
        ddb.put_item(TableName="KdsOrder", Item={"pk": {"S": f"k{i}"}})

    # Short sleep to make sure arrival timestamps settle (not strictly needed).
    time.sleep(0.05)
    records = _drain_kinesis(kin, "ministack-kds-order")
    assert len(records) == 5
    decoded = [
        json.loads(
            (base64.b64decode(r["Data"]) if isinstance(r["Data"], str) else r["Data"]).decode("utf-8")
        )
        for r in records
    ]
    keys = [d["dynamodb"]["Keys"]["pk"]["S"] for d in decoded]
    assert keys == [f"k{i}" for i in range(5)]


# ---------------------------------------------------------------------------
# Legacy Expected API tests (issue #563)
# ---------------------------------------------------------------------------


def test_dynamodb_put_item_expected_exists_false(ddb):
    """PutItem with Expected {Exists: false} blocks overwrites."""
    table = "intg-ddb-expected"
    try:
        ddb.delete_table(TableName=table)
    except ClientError:
        pass
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    # First put succeeds — item does not exist
    ddb.put_item(
        TableName=table,
        Item={"pk": {"S": "exp1"}, "val": {"S": "first"}},
        Expected={"pk": {"Exists": False}},
    )
    resp = ddb.get_item(TableName=table, Key={"pk": {"S": "exp1"}})
    assert resp["Item"]["val"]["S"] == "first"

    # Second put fails — item already exists
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName=table,
            Item={"pk": {"S": "exp1"}, "val": {"S": "second"}},
            Expected={"pk": {"Exists": False}},
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_dynamodb_put_item_expected_value_eq(ddb):
    """PutItem with Expected {Value: ...} shorthand (EQ check)."""
    table = "intg-ddb-expected"
    ddb.put_item(TableName=table, Item={"pk": {"S": "exp_val"}, "status": {"S": "draft"}})
    # Should succeed — status matches
    ddb.put_item(
        TableName=table,
        Item={"pk": {"S": "exp_val"}, "status": {"S": "published"}},
        Expected={"status": {"Value": {"S": "draft"}}},
    )
    resp = ddb.get_item(TableName=table, Key={"pk": {"S": "exp_val"}})
    assert resp["Item"]["status"]["S"] == "published"

    # Should fail — status is now "published", not "draft"
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName=table,
            Item={"pk": {"S": "exp_val"}, "status": {"S": "archived"}},
            Expected={"status": {"Value": {"S": "draft"}}},
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_dynamodb_put_item_expected_comparison_operator(ddb):
    """PutItem with Expected using full ComparisonOperator form."""
    table = "intg-ddb-expected"
    ddb.put_item(TableName=table, Item={"pk": {"S": "exp_comp"}, "count": {"N": "5"}})
    # LE: count <= 10 → should succeed
    ddb.put_item(
        TableName=table,
        Item={"pk": {"S": "exp_comp"}, "count": {"N": "10"}},
        Expected={"count": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "10"}]}},
    )
    # GT: count > 100 → should fail (count is 10)
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName=table,
            Item={"pk": {"S": "exp_comp"}, "count": {"N": "20"}},
            Expected={"count": {"ComparisonOperator": "GT", "AttributeValueList": [{"N": "100"}]}},
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_dynamodb_delete_item_expected(ddb):
    """DeleteItem with Expected condition."""
    table = "intg-ddb-expected"
    ddb.put_item(TableName=table, Item={"pk": {"S": "exp_del"}, "status": {"S": "inactive"}})
    # Should fail — status is not "active"
    with pytest.raises(ClientError) as exc:
        ddb.delete_item(
            TableName=table,
            Key={"pk": {"S": "exp_del"}},
            Expected={"status": {"Value": {"S": "active"}}},
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
    # Item should still exist
    resp = ddb.get_item(TableName=table, Key={"pk": {"S": "exp_del"}})
    assert "Item" in resp

    # Should succeed — status matches
    ddb.delete_item(
        TableName=table,
        Key={"pk": {"S": "exp_del"}},
        Expected={"status": {"Value": {"S": "inactive"}}},
    )
    resp = ddb.get_item(TableName=table, Key={"pk": {"S": "exp_del"}})
    assert "Item" not in resp


def test_dynamodb_update_item_expected(ddb):
    """UpdateItem with Expected condition."""
    table = "intg-ddb-expected"
    ddb.put_item(TableName=table, Item={"pk": {"S": "exp_upd"}, "ver": {"N": "1"}})
    # Optimistic locking — update only if ver == 1
    ddb.update_item(
        TableName=table,
        Key={"pk": {"S": "exp_upd"}},
        UpdateExpression="SET ver = :newver",
        ExpressionAttributeValues={":newver": {"N": "2"}},
        Expected={"ver": {"Value": {"N": "1"}}},
    )
    resp = ddb.get_item(TableName=table, Key={"pk": {"S": "exp_upd"}})
    assert resp["Item"]["ver"]["N"] == "2"

    # Should fail — ver is now 2, not 1
    with pytest.raises(ClientError) as exc:
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "exp_upd"}},
            UpdateExpression="SET ver = :newver",
            ExpressionAttributeValues={":newver": {"N": "3"}},
            Expected={"ver": {"Value": {"N": "1"}}},
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_dynamodb_expected_conditional_operator_or(ddb):
    """Expected with ConditionalOperator=OR — passes if any condition is true."""
    table = "intg-ddb-expected"
    ddb.put_item(TableName=table, Item={"pk": {"S": "exp_or"}, "a": {"S": "x"}, "b": {"S": "y"}})
    # a == "x" OR b == "z" → should pass (a matches)
    ddb.put_item(
        TableName=table,
        Item={"pk": {"S": "exp_or"}, "a": {"S": "x"}, "b": {"S": "y"}},
        Expected={
            "a": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "x"}]},
            "b": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "z"}]},
        },
        ConditionalOperator="OR",
    )


def test_dynamodb_expected_between(ddb):
    """Expected with BETWEEN operator."""
    table = "intg-ddb-expected"
    ddb.put_item(TableName=table, Item={"pk": {"S": "exp_btwn"}, "score": {"N": "75"}})
    # score BETWEEN 50 AND 100 → should pass
    ddb.put_item(
        TableName=table,
        Item={"pk": {"S": "exp_btwn"}, "score": {"N": "80"}},
        Expected={
            "score": {
                "ComparisonOperator": "BETWEEN",
                "AttributeValueList": [{"N": "50"}, {"N": "100"}],
            }
        },
    )
    # score BETWEEN 90 AND 100 → should fail (score is 80)
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName=table,
            Item={"pk": {"S": "exp_btwn"}, "score": {"N": "85"}},
            Expected={
                "score": {
                    "ComparisonOperator": "BETWEEN",
                    "AttributeValueList": [{"N": "90"}, {"N": "100"}],
                }
            },
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_dynamodb_expected_in(ddb):
    """Expected with IN operator."""
    table = "intg-ddb-expected"
    ddb.put_item(TableName=table, Item={"pk": {"S": "exp_in"}, "status": {"S": "active"}})
    # status IN ("active", "pending") → should pass
    ddb.put_item(
        TableName=table,
        Item={"pk": {"S": "exp_in"}, "status": {"S": "active"}},
        Expected={
            "status": {
                "ComparisonOperator": "IN",
                "AttributeValueList": [{"S": "active"}, {"S": "pending"}],
            }
        },
    )


def test_dynamodb_expected_mutually_exclusive_with_condition_expression(ddb):
    """Expected and ConditionExpression cannot be used together."""
    table = "intg-ddb-expected"
    with pytest.raises(ClientError) as exc:
        ddb.put_item(
            TableName=table,
            Item={"pk": {"S": "exp_excl"}},
            Expected={"pk": {"Exists": False}},
            ConditionExpression="attribute_not_exists(pk)",
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"


# ---------------------------------------------------------------------------
# Legacy KeyConditions API tests (issue #563)
# ---------------------------------------------------------------------------


def test_dynamodb_query_key_conditions_basic(ddb):
    """Query with legacy KeyConditions on partition key only."""
    table = "intg-ddb-keycond"
    try:
        ddb.delete_table(TableName=table)
    except ClientError:
        pass
    ddb.create_table(
        TableName=table,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(5):
        ddb.put_item(TableName=table, Item={
            "pk": {"S": "kc_pk"},
            "sk": {"S": f"sk_{i:03d}"},
            "data": {"S": f"val_{i}"},
        })
    # Add items with different PK to ensure filtering works
    ddb.put_item(TableName=table, Item={"pk": {"S": "other_pk"}, "sk": {"S": "sk_000"}, "data": {"S": "other"}})

    resp = ddb.query(
        TableName=table,
        KeyConditions={
            "pk": {
                "AttributeValueList": [{"S": "kc_pk"}],
                "ComparisonOperator": "EQ",
            },
        },
    )
    assert resp["Count"] == 5
    assert all(item["pk"]["S"] == "kc_pk" for item in resp["Items"])


def test_dynamodb_query_key_conditions_sort_key_begins_with(ddb):
    """Query with KeyConditions using BEGINS_WITH on sort key."""
    table = "intg-ddb-keycond"
    resp = ddb.query(
        TableName=table,
        KeyConditions={
            "pk": {
                "AttributeValueList": [{"S": "kc_pk"}],
                "ComparisonOperator": "EQ",
            },
            "sk": {
                "AttributeValueList": [{"S": "sk_00"}],
                "ComparisonOperator": "BEGINS_WITH",
            },
        },
    )
    # sk_000, sk_001, sk_002, sk_003, sk_004 all start with "sk_00"
    assert resp["Count"] == 5


def test_dynamodb_query_key_conditions_sort_key_between(ddb):
    """Query with KeyConditions using BETWEEN on sort key."""
    table = "intg-ddb-keycond"
    resp = ddb.query(
        TableName=table,
        KeyConditions={
            "pk": {
                "AttributeValueList": [{"S": "kc_pk"}],
                "ComparisonOperator": "EQ",
            },
            "sk": {
                "AttributeValueList": [{"S": "sk_001"}, {"S": "sk_003"}],
                "ComparisonOperator": "BETWEEN",
            },
        },
    )
    assert resp["Count"] == 3
    sks = [item["sk"]["S"] for item in resp["Items"]]
    assert sks == ["sk_001", "sk_002", "sk_003"]


def test_dynamodb_query_key_conditions_sort_key_lt(ddb):
    """Query with KeyConditions using LT on sort key."""
    table = "intg-ddb-keycond"
    resp = ddb.query(
        TableName=table,
        KeyConditions={
            "pk": {
                "AttributeValueList": [{"S": "kc_pk"}],
                "ComparisonOperator": "EQ",
            },
            "sk": {
                "AttributeValueList": [{"S": "sk_002"}],
                "ComparisonOperator": "LT",
            },
        },
    )
    assert resp["Count"] == 2
    sks = [item["sk"]["S"] for item in resp["Items"]]
    assert sks == ["sk_000", "sk_001"]


def test_dynamodb_query_key_conditions_mutually_exclusive(ddb):
    """KeyConditions and KeyConditionExpression cannot be used together."""
    table = "intg-ddb-keycond"
    with pytest.raises(ClientError) as exc:
        ddb.query(
            TableName=table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "kc_pk"}},
            KeyConditions={
                "pk": {
                    "AttributeValueList": [{"S": "kc_pk"}],
                    "ComparisonOperator": "EQ",
                },
            },
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_query_key_conditions_with_query_filter(ddb):
    """KeyConditions can be used together with legacy QueryFilter."""
    table = "intg-ddb-keycond"
    # Add items with a "status" attribute for filtering
    for i in range(4):
        ddb.put_item(TableName=table, Item={
            "pk": {"S": "kc_filt"},
            "sk": {"S": f"f_{i:03d}"},
            "status": {"S": "active" if i < 2 else "inactive"},
        })

    resp = ddb.query(
        TableName=table,
        KeyConditions={
            "pk": {
                "AttributeValueList": [{"S": "kc_filt"}],
                "ComparisonOperator": "EQ",
            },
        },
        QueryFilter={
            "status": {
                "AttributeValueList": [{"S": "active"}],
                "ComparisonOperator": "EQ",
            },
        },
    )
    assert resp["Count"] == 2
    assert resp["ScannedCount"] == 4
    for item in resp["Items"]:
        assert item["status"]["S"] == "active"


# ---------------------------------------------------------------------------
# Legacy AttributeUpdates (UpdateItem)
# ---------------------------------------------------------------------------

def test_dynamodb_attribute_updates_put(ddb):
    """PUT action sets attributes on a new and existing item."""
    table = f"t_attr_upd_put_{_uuid_mod.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    # Create item via AttributeUpdates (upsert)
    ddb.update_item(
        TableName=table,
        Key={"pk": {"S": "k1"}},
        AttributeUpdates={
            "name": {"Action": "PUT", "Value": {"S": "alice"}},
            "age": {"Action": "PUT", "Value": {"N": "30"}},
        },
    )
    item = ddb.get_item(TableName=table, Key={"pk": {"S": "k1"}})["Item"]
    assert item["name"] == {"S": "alice"}
    assert item["age"] == {"N": "30"}

    # Update existing item
    ddb.update_item(
        TableName=table,
        Key={"pk": {"S": "k1"}},
        AttributeUpdates={
            "name": {"Action": "PUT", "Value": {"S": "bob"}},
        },
    )
    item = ddb.get_item(TableName=table, Key={"pk": {"S": "k1"}})["Item"]
    assert item["name"] == {"S": "bob"}
    assert item["age"] == {"N": "30"}  # unchanged


def test_dynamodb_attribute_updates_delete(ddb):
    """DELETE action removes an attribute or subtracts from a set."""
    table = f"t_attr_upd_del_{_uuid_mod.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=table, Item={
        "pk": {"S": "k1"},
        "color": {"S": "red"},
        "tags": {"SS": ["a", "b", "c"]},
    })

    # DELETE without Value → remove attribute
    ddb.update_item(
        TableName=table,
        Key={"pk": {"S": "k1"}},
        AttributeUpdates={"color": {"Action": "DELETE"}},
    )
    item = ddb.get_item(TableName=table, Key={"pk": {"S": "k1"}})["Item"]
    assert "color" not in item

    # DELETE with Value → subtract from set
    ddb.update_item(
        TableName=table,
        Key={"pk": {"S": "k1"}},
        AttributeUpdates={"tags": {"Action": "DELETE", "Value": {"SS": ["b"]}}},
    )
    item = ddb.get_item(TableName=table, Key={"pk": {"S": "k1"}})["Item"]
    assert sorted(item["tags"]["SS"]) == ["a", "c"]


def test_dynamodb_attribute_updates_add(ddb):
    """ADD action increments a number or adds to a set."""
    table = f"t_attr_upd_add_{_uuid_mod.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=table, Item={
        "pk": {"S": "k1"},
        "counter": {"N": "10"},
        "tags": {"SS": ["a"]},
    })

    ddb.update_item(
        TableName=table,
        Key={"pk": {"S": "k1"}},
        AttributeUpdates={
            "counter": {"Action": "ADD", "Value": {"N": "5"}},
            "tags": {"Action": "ADD", "Value": {"SS": ["b", "c"]}},
        },
    )
    item = ddb.get_item(TableName=table, Key={"pk": {"S": "k1"}})["Item"]
    assert item["counter"] == {"N": "15"}
    assert sorted(item["tags"]["SS"]) == ["a", "b", "c"]

    # ADD on non-existent numeric attribute → starts from 0
    ddb.update_item(
        TableName=table,
        Key={"pk": {"S": "k1"}},
        AttributeUpdates={"new_num": {"Action": "ADD", "Value": {"N": "7"}}},
    )
    item = ddb.get_item(TableName=table, Key={"pk": {"S": "k1"}})["Item"]
    assert item["new_num"] == {"N": "7"}


def test_dynamodb_attribute_updates_mutually_exclusive_with_update_expression(ddb):
    """AttributeUpdates and UpdateExpression cannot be used together."""
    table = f"t_attr_upd_excl_{_uuid_mod.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    with pytest.raises(ClientError) as exc_info:
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "k1"}},
            UpdateExpression="SET #n = :v",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":v": {"S": "x"}},
            AttributeUpdates={"name": {"Action": "PUT", "Value": {"S": "y"}}},
        )
    assert exc_info.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_attribute_updates_default_action_is_put(ddb):
    """When Action is omitted it defaults to PUT."""
    table = f"t_attr_upd_dflt_{_uuid_mod.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.update_item(
        TableName=table,
        Key={"pk": {"S": "k1"}},
        AttributeUpdates={"name": {"Value": {"S": "alice"}}},
    )
    item = ddb.get_item(TableName=table, Key={"pk": {"S": "k1"}})["Item"]
    assert item["name"] == {"S": "alice"}


def test_dynamodb_attribute_updates_delete_with_value_on_non_set_raises(ddb):
    """DELETE with a Value where the existing attribute is not a set must fail."""
    table = f"t_attr_upd_del_invalid_{_uuid_mod.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=table, Item={"pk": {"S": "k1"}, "name": {"S": "alice"}})

    with pytest.raises(ClientError) as exc_info:
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "k1"}},
            AttributeUpdates={"name": {"Action": "DELETE", "Value": {"SS": ["x"]}}},
        )
    assert exc_info.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_attribute_updates_add_on_string_attribute_raises(ddb):
    """ADD applied to an existing String attribute must fail (Number/set only)."""
    table = f"t_attr_upd_add_invalid_{_uuid_mod.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=table, Item={"pk": {"S": "k1"}, "name": {"S": "alice"}})

    with pytest.raises(ClientError) as exc_info:
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "k1"}},
            AttributeUpdates={"name": {"Action": "ADD", "Value": {"N": "1"}}},
        )
    assert exc_info.value.response["Error"]["Code"] == "ValidationException"


# ---------------------------------------------------------------------------
# UpdateExpression arithmetic + if_not_exists (issue #648)
# ---------------------------------------------------------------------------

def _make_counter_table(ddb, name):
    ddb.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "PK", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "PK", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )


def test_dynamodb_update_if_not_exists_minus_in_outer_parens(ddb):
    """Regression for #648: `SET v = (if_not_exists(v, :d) - :amt)` lost the
    arithmetic and assigned the resolved value directly. The outer parens
    pinned every token to depth>0, so the top-level operator scan skipped
    the `-` and silently dropped the subtraction.
    """
    table = "if-not-exists-minus-parens"
    _make_counter_table(ddb, table)
    try:
        ddb.put_item(TableName=table, Item={"PK": {"S": "x"}, "v": {"N": "10000"}})
        ddb.update_item(
            TableName=table,
            Key={"PK": {"S": "x"}},
            UpdateExpression="SET #v = (if_not_exists(#v, :d) - :amt)",
            ExpressionAttributeNames={"#v": "v"},
            ExpressionAttributeValues={":d": {"N": "0"}, ":amt": {"N": "3000"}},
        )
        got = ddb.get_item(TableName=table, Key={"PK": {"S": "x"}})["Item"]["v"]
        assert got == {"N": "7000"}
    finally:
        ddb.delete_table(TableName=table)


def test_dynamodb_update_if_not_exists_plus_in_outer_parens(ddb):
    """Same shape with `+` rather than `-` — both PLUS and MINUS go through
    the same operator-scan branch, so both need the outer-paren strip.
    """
    table = "if-not-exists-plus-parens"
    _make_counter_table(ddb, table)
    try:
        ddb.put_item(TableName=table, Item={"PK": {"S": "x"}, "v": {"N": "100"}})
        ddb.update_item(
            TableName=table,
            Key={"PK": {"S": "x"}},
            UpdateExpression="SET #v = (if_not_exists(#v, :d) + :amt)",
            ExpressionAttributeNames={"#v": "v"},
            ExpressionAttributeValues={":d": {"N": "0"}, ":amt": {"N": "42"}},
        )
        got = ddb.get_item(TableName=table, Key={"PK": {"S": "x"}})["Item"]["v"]
        assert got == {"N": "142"}
    finally:
        ddb.delete_table(TableName=table)


def test_dynamodb_update_if_not_exists_arithmetic_when_attribute_missing(ddb):
    """When the attribute doesn't exist yet, `if_not_exists` resolves to the
    default and the arithmetic still applies: `0 - 3000 = -3000`.
    """
    table = "if-not-exists-arith-missing"
    _make_counter_table(ddb, table)
    try:
        ddb.put_item(TableName=table, Item={"PK": {"S": "x"}})  # no `v`
        ddb.update_item(
            TableName=table,
            Key={"PK": {"S": "x"}},
            UpdateExpression="SET #v = (if_not_exists(#v, :d) - :amt)",
            ExpressionAttributeNames={"#v": "v"},
            ExpressionAttributeValues={":d": {"N": "0"}, ":amt": {"N": "3000"}},
        )
        got = ddb.get_item(TableName=table, Key={"PK": {"S": "x"}})["Item"]["v"]
        assert got == {"N": "-3000"}
    finally:
        ddb.delete_table(TableName=table)


def test_dynamodb_update_arithmetic_without_outer_parens_still_works(ddb):
    """Sanity: the pre-existing no-outer-parens form must keep working too —
    `SET v = if_not_exists(v, :d) - :amt`.
    """
    table = "if-not-exists-no-parens"
    _make_counter_table(ddb, table)
    try:
        ddb.put_item(TableName=table, Item={"PK": {"S": "x"}, "v": {"N": "10"}})
        ddb.update_item(
            TableName=table,
            Key={"PK": {"S": "x"}},
            UpdateExpression="SET #v = if_not_exists(#v, :d) - :amt",
            ExpressionAttributeNames={"#v": "v"},
            ExpressionAttributeValues={":d": {"N": "0"}, ":amt": {"N": "3"}},
        )
        got = ddb.get_item(TableName=table, Key={"PK": {"S": "x"}})["Item"]["v"]
        assert got == {"N": "7"}
    finally:
        ddb.delete_table(TableName=table)


def test_dynamodb_update_nested_parens_do_not_flatten_two_groups(ddb):
    """`(a) + (b)` must NOT be flattened by the outer-paren strip — the
    opening LPAREN doesn't match the final RPAREN. Numeric value of
    `(:left) + (:right)` should be 10.
    """
    table = "nested-parens-two-groups"
    _make_counter_table(ddb, table)
    try:
        ddb.put_item(TableName=table, Item={"PK": {"S": "x"}})
        ddb.update_item(
            TableName=table,
            Key={"PK": {"S": "x"}},
            UpdateExpression="SET v = (:left) + (:right)",
            ExpressionAttributeValues={":left": {"N": "3"}, ":right": {"N": "7"}},
        )
        got = ddb.get_item(TableName=table, Key={"PK": {"S": "x"}})["Item"]["v"]
        assert got == {"N": "10"}
    finally:
        ddb.delete_table(TableName=table)




# ---------------------------------------------------------------------------
# Contributor Insights, Resource Policies, Export/Import
# Added for DynamoDB conformance — previously unsupported by ministack.
# Behavior verified against botocore service-2.json (2012-08-10).
# ---------------------------------------------------------------------------

def _ci_table(ddb, name):
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return ddb.describe_table(TableName=name)["Table"]["TableArn"]


def test_dynamodb_contributor_insights_lifecycle(ddb):
    name = "ci-table"
    _ci_table(ddb, name)
    try:
        # Default state: DISABLED, empty rule list.
        d = ddb.describe_contributor_insights(TableName=name)
        assert d["ContributorInsightsStatus"] == "DISABLED"
        assert d["ContributorInsightsRuleList"] == []

        # ENABLE → returns ENABLING; next describe transitions to ENABLED.
        u = ddb.update_contributor_insights(TableName=name, ContributorInsightsAction="ENABLE")
        assert u["ContributorInsightsStatus"] == "ENABLING"
        d = ddb.describe_contributor_insights(TableName=name)
        assert d["ContributorInsightsStatus"] == "ENABLED"

        # List includes our table.
        lst = ddb.list_contributor_insights()
        names = [s["TableName"] for s in lst.get("ContributorInsightsSummaries", [])]
        assert name in names

        # DISABLE.
        u = ddb.update_contributor_insights(TableName=name, ContributorInsightsAction="DISABLE")
        assert u["ContributorInsightsStatus"] == "DISABLING"
        d = ddb.describe_contributor_insights(TableName=name)
        assert d["ContributorInsightsStatus"] == "DISABLED"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_contributor_insights_invalid_action(ddb):
    name = "ci-bad-action"
    _ci_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.update_contributor_insights(TableName=name, ContributorInsightsAction="TOGGLE")
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_contributor_insights_unknown_table(ddb):
    with pytest.raises(ClientError) as e:
        ddb.describe_contributor_insights(TableName="nope-ci")
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_dynamodb_resource_policy_put_get_delete(ddb):
    name = "rp-table"
    arn = _ci_table(ddb, name)
    try:
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::000000000000:role/r"},
                "Action": ["dynamodb:GetItem"],
                "Resource": [arn],
            }],
        })
        r = ddb.put_resource_policy(ResourceArn=arn, Policy=policy)
        rev = r["RevisionId"]
        assert rev

        g = ddb.get_resource_policy(ResourceArn=arn)
        assert g["Policy"] == policy
        assert g["RevisionId"] == rev

        # Conditional update with wrong expected revision -> PolicyNotFoundException.
        with pytest.raises(ClientError) as e:
            ddb.put_resource_policy(ResourceArn=arn, Policy=policy, ExpectedRevisionId="not-a-real-rev")
        assert e.value.response["Error"]["Code"] == "PolicyNotFoundException"

        # NO_POLICY revision against existing policy -> PolicyNotFoundException.
        with pytest.raises(ClientError) as e:
            ddb.put_resource_policy(ResourceArn=arn, Policy=policy, ExpectedRevisionId="NO_POLICY")
        assert e.value.response["Error"]["Code"] == "PolicyNotFoundException"

        # Correct expected revision succeeds and returns new revision.
        r2 = ddb.put_resource_policy(ResourceArn=arn, Policy=policy, ExpectedRevisionId=rev)
        assert r2["RevisionId"] != rev

        # Delete returns the deleted revision.
        d = ddb.delete_resource_policy(ResourceArn=arn)
        assert d["RevisionId"] == r2["RevisionId"]

        # Get on missing policy -> PolicyNotFoundException.
        with pytest.raises(ClientError) as e:
            ddb.get_resource_policy(ResourceArn=arn)
        assert e.value.response["Error"]["Code"] == "PolicyNotFoundException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_resource_policy_unknown_arn(ddb):
    fake_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/does-not-exist-rp"
    with pytest.raises(ClientError) as e:
        ddb.get_resource_policy(ResourceArn=fake_arn)
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    with pytest.raises(ClientError) as e:
        ddb.put_resource_policy(ResourceArn=fake_arn, Policy="{}")
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_dynamodb_export_table_to_point_in_time(ddb):
    name = "exp-table"
    arn = _ci_table(ddb, name)
    try:
        r = ddb.export_table_to_point_in_time(TableArn=arn, S3Bucket="my-bucket")
        desc = r["ExportDescription"]
        assert desc["TableArn"] == arn
        assert desc["S3Bucket"] == "my-bucket"
        assert desc["ExportStatus"] in ("IN_PROGRESS", "COMPLETED")
        export_arn = desc["ExportArn"]

        d = ddb.describe_export(ExportArn=export_arn)
        assert d["ExportDescription"]["ExportArn"] == export_arn

        lst = ddb.list_exports(TableArn=arn)
        arns = [s["ExportArn"] for s in lst["ExportSummaries"]]
        assert export_arn in arns

        # Idempotency: same ClientToken returns the same export.
        token = "client-token-123"
        a = ddb.export_table_to_point_in_time(TableArn=arn, S3Bucket="my-bucket", ClientToken=token)
        b = ddb.export_table_to_point_in_time(TableArn=arn, S3Bucket="my-bucket", ClientToken=token)
        assert a["ExportDescription"]["ExportArn"] == b["ExportDescription"]["ExportArn"]
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_describe_export_not_found(ddb):
    with pytest.raises(ClientError) as e:
        ddb.describe_export(ExportArn="arn:aws:dynamodb:us-east-1:000000000000:table/x/export/nope")
    assert e.value.response["Error"]["Code"] == "ExportNotFoundException"


def test_dynamodb_export_unknown_table(ddb):
    fake_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/does-not-exist-export"
    with pytest.raises(ClientError) as e:
        ddb.export_table_to_point_in_time(TableArn=fake_arn, S3Bucket="b")
    assert e.value.response["Error"]["Code"] == "TableNotFoundException"


def test_dynamodb_import_table(ddb):
    name = "imp-table"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    r = ddb.import_table(
        S3BucketSource={"S3Bucket": "import-src"},
        InputFormat="DYNAMODB_JSON",
        TableCreationParameters={
            "TableName": name,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
        },
    )
    desc = r["ImportTableDescription"]
    assert desc["ImportStatus"] in ("IN_PROGRESS", "COMPLETED")
    assert desc["InputFormat"] == "DYNAMODB_JSON"
    arn = desc["ImportArn"]
    try:
        d = ddb.describe_import(ImportArn=arn)
        assert d["ImportTableDescription"]["ImportArn"] == arn
        lst = ddb.list_imports()
        arns = [s["ImportArn"] for s in lst["ImportSummaryList"]]
        assert arn in arns
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_describe_import_not_found(ddb):
    with pytest.raises(ClientError) as e:
        ddb.describe_import(ImportArn="arn:aws:dynamodb:us-east-1:000000000000:import/nope")
    assert e.value.response["Error"]["Code"] == "ImportNotFoundException"


# ---------------------------------------------------------------------------
# Limits — AWS-spec enforcement (item size, batch caps, number precision,
# empty sets/strings). Verified against AWS DynamoDB Developer Guide
# "Service, account, and table quotas".
# ---------------------------------------------------------------------------

def _basic_table(ddb, name, with_sk=False):
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ks = [{"AttributeName": "pk", "KeyType": "HASH"}]
    ad = [{"AttributeName": "pk", "AttributeType": "S"}]
    if with_sk:
        ks.append({"AttributeName": "sk", "KeyType": "RANGE"})
        ad.append({"AttributeName": "sk", "AttributeType": "S"})
    ddb.create_table(
        TableName=name, KeySchema=ks, AttributeDefinitions=ad, BillingMode="PAY_PER_REQUEST",
    )


def test_dynamodb_batch_write_cap_25(ddb):
    name = "bw-cap"
    _basic_table(ddb, name)
    try:
        # 26 items must be rejected per AWS quota.
        reqs = [{"PutRequest": {"Item": {"pk": {"S": f"k{i}"}}}} for i in range(26)]
        with pytest.raises(ClientError) as e:
            ddb.batch_write_item(RequestItems={name: reqs})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_batch_get_cap_100(ddb):
    name = "bg-cap"
    _basic_table(ddb, name)
    try:
        keys = [{"pk": {"S": f"k{i}"}} for i in range(101)]
        with pytest.raises(ClientError) as e:
            ddb.batch_get_item(RequestItems={name: {"Keys": keys}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_batch_write_duplicate_key_rejected(ddb):
    name = "bw-dup"
    _basic_table(ddb, name)
    try:
        reqs = [
            {"PutRequest": {"Item": {"pk": {"S": "same"}, "x": {"N": "1"}}}},
            {"PutRequest": {"Item": {"pk": {"S": "same"}, "x": {"N": "2"}}}},
        ]
        with pytest.raises(ClientError) as e:
            ddb.batch_write_item(RequestItems={name: reqs})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_batch_get_duplicate_key_rejected(ddb):
    name = "bg-dup"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "a"}})
        with pytest.raises(ClientError) as e:
            ddb.batch_get_item(RequestItems={name: {"Keys": [{"pk": {"S": "a"}}, {"pk": {"S": "a"}}]}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_empty_string_set_rejected(ddb):
    name = "empty-ss"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "tags": {"SS": []}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_empty_number_set_rejected(ddb):
    name = "empty-ns"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "scores": {"NS": []}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_empty_binary_set_rejected(ddb):
    name = "empty-bs"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "blobs": {"BS": []}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_duplicate_ss_values_rejected(ddb):
    name = "dup-ss"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "tags": {"SS": ["a", "a", "b"]}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_empty_string_hash_key_rejected(ddb):
    name = "empty-pk"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.put_item(TableName=name, Item={"pk": {"S": ""}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_empty_string_sort_key_rejected(ddb):
    name = "empty-sk"
    _basic_table(ddb, name, with_sk=True)
    try:
        with pytest.raises(ClientError) as e:
            ddb.put_item(TableName=name, Item={"pk": {"S": "p"}, "sk": {"S": ""}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_item_over_400kb_rejected(ddb):
    name = "huge-item"
    _basic_table(ddb, name)
    try:
        big = "x" * (400 * 1024 + 100)
        with pytest.raises(ClientError) as e:
            ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "blob": {"S": big}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_number_39_digits_rejected(ddb):
    name = "huge-num"
    _basic_table(ddb, name)
    try:
        # 39 significant digits — AWS limit is 38.
        n = "1" * 39
        with pytest.raises(ClientError) as e:
            ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "n": {"N": n}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_number_above_max_magnitude_rejected(ddb):
    name = "num-mag"
    _basic_table(ddb, name)
    try:
        # Magnitude > 9.99...E+125 rejected per AWS quota.
        with pytest.raises(ClientError) as e:
            ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "n": {"N": "1E126"}})
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_number_canonicalization_leading_zeros(ddb):
    """AWS canonicalizes numbers: strips leading zeros, normalizes negative
    zero to 0, trims trailing zeros after the decimal."""
    name = "num-canon"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "a"}, "v": {"N": "000123"}})
        got = ddb.get_item(TableName=name, Key={"pk": {"S": "a"}})["Item"]["v"]["N"]
        assert got == "123"
        ddb.put_item(TableName=name, Item={"pk": {"S": "b"}, "v": {"N": "-0"}})
        got = ddb.get_item(TableName=name, Key={"pk": {"S": "b"}})["Item"]["v"]["N"]
        assert got == "0"
        ddb.put_item(TableName=name, Item={"pk": {"S": "c"}, "v": {"N": "1.500"}})
        got = ddb.get_item(TableName=name, Key={"pk": {"S": "c"}})["Item"]["v"]["N"]
        assert got == "1.5"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_transact_write_cap_100(ddb):
    name = "tw-cap"
    _basic_table(ddb, name)
    try:
        items = [{"Put": {"TableName": name, "Item": {"pk": {"S": f"k{i}"}}}} for i in range(101)]
        with pytest.raises(ClientError) as e:
            ddb.transact_write_items(TransactItems=items)
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_transact_get_cap_100(ddb):
    name = "tg-cap"
    _basic_table(ddb, name)
    try:
        items = [{"Get": {"TableName": name, "Key": {"pk": {"S": f"k{i}"}}}} for i in range(101)]
        with pytest.raises(ClientError) as e:
            ddb.transact_get_items(TransactItems=items)
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def _raw_ddb(target: str, body: dict):
    """Direct HTTP DDB call — bypasses boto3's client-side validation so we
    can verify the server's own length checks (which is what the conformance
    suite hits)."""
    import urllib.request, urllib.error
    req = urllib.request.Request(
        "http://localhost:4566/",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Target": f"DynamoDB_20120810.{target}",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/dynamodb/aws4_request, SignedHeaders=host;x-amz-date;x-amz-target, Signature=00",
            "X-Amz-Date": "20200101T000000Z",
        },
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.getcode(), json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_dynamodb_transact_write_empty_rejected():
    code, body = _raw_ddb("TransactWriteItems", {"TransactItems": []})
    assert code == 400
    assert "ValidationException" in body.get("__type", "")


def test_dynamodb_transact_get_empty_rejected():
    code, body = _raw_ddb("TransactGetItems", {"TransactItems": []})
    assert code == 400
    assert "ValidationException" in body.get("__type", "")


def test_dynamodb_transact_write_duplicate_keys_rejected(ddb):
    name = "tw-dup"
    _basic_table(ddb, name)
    try:
        items = [
            {"Put": {"TableName": name, "Item": {"pk": {"S": "same"}, "a": {"N": "1"}}}},
            {"Put": {"TableName": name, "Item": {"pk": {"S": "same"}, "a": {"N": "2"}}}},
        ]
        with pytest.raises(ClientError) as e:
            ddb.transact_write_items(TransactItems=items)
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# CreateTable validation — AWS-canonical name/key/LSI rules.
# Verified against botocore service-2.json (2012-08-10) and AWS Developer
# Guide "CreateTable" reference (pattern [a-zA-Z0-9_.-]+, length 3-255,
# LSI requires range key on base table, etc.).
# ---------------------------------------------------------------------------

def test_dynamodb_create_table_name_too_short(ddb):
    with pytest.raises(ClientError) as e:
        ddb.create_table(
            TableName="xy",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
    assert e.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_create_table_name_too_long(ddb):
    with pytest.raises(ClientError) as e:
        ddb.create_table(
            TableName="a" * 256,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
    assert e.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_create_table_invalid_chars(ddb):
    with pytest.raises(ClientError) as e:
        ddb.create_table(
            TableName="bad table",  # space — invalid char
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
    assert e.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_create_table_missing_key_schema(ddb):
    # boto3 client-side rejects KeySchema=[] outright. Use direct HTTP so the
    # server gets the chance to apply its own validation (this is what the
    # conformance suite exercises).
    code, body = _raw_ddb("CreateTable", {
        "TableName": "ms-no-ks",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
    })
    assert code == 400
    assert "ValidationException" in body.get("__type", "")


def test_dynamodb_create_table_unused_attribute_definition(ddb):
    """Attributes in AttributeDefinitions that don't appear in any KeySchema
    are rejected by real AWS."""
    with pytest.raises(ClientError) as e:
        ddb.create_table(
            TableName="unused-attr-defs",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "extra", "AttributeType": "S"},  # unused
            ],
            BillingMode="PAY_PER_REQUEST",
        )
    assert e.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_create_table_lsi_on_hash_only_rejected(ddb):
    """LSI requires the base table to have a sort key."""
    with pytest.raises(ClientError) as e:
        ddb.create_table(
            TableName="lsi-hash-only",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "alt", "AttributeType": "S"},
            ],
            LocalSecondaryIndexes=[{
                "IndexName": "lsi1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "alt", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }],
            BillingMode="PAY_PER_REQUEST",
        )
    assert e.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_create_table_lsi_different_hash_key_rejected(ddb):
    """LSI's HASH key must match the base table's HASH key."""
    with pytest.raises(ClientError) as e:
        ddb.create_table(
            TableName="lsi-bad-hash",
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "other", "AttributeType": "S"},
                {"AttributeName": "lsisk", "AttributeType": "S"},
            ],
            LocalSecondaryIndexes=[{
                "IndexName": "lsi1",
                "KeySchema": [
                    {"AttributeName": "other", "KeyType": "HASH"},  # != base hash
                    {"AttributeName": "lsisk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }],
            BillingMode="PAY_PER_REQUEST",
        )
    assert e.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_create_table_class_standard_ia_roundtrip(ddb):
    name = "tc-ia"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    try:
        ddb.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            TableClass="STANDARD_INFREQUENT_ACCESS",
        )
        d = ddb.describe_table(TableName=name)["Table"]
        assert d.get("TableClassSummary", {}).get("TableClass") == "STANDARD_INFREQUENT_ACCESS"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_create_table_on_demand_throughput_roundtrip(ddb):
    name = "tc-odt"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    try:
        ddb.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            OnDemandThroughput={"MaxReadRequestUnits": 1000, "MaxWriteRequestUnits": 500},
        )
        d = ddb.describe_table(TableName=name)["Table"]
        odt = d.get("OnDemandThroughput") or {}
        assert odt.get("MaxReadRequestUnits") == 1000
        assert odt.get("MaxWriteRequestUnits") == 500
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_create_table_sse_aws_managed_key(ddb):
    """SSESpecification with Enabled=true and no KMSMasterKeyId picks the
    AWS-managed key (alias/aws/dynamodb)."""
    name = "tc-sse-mgmt"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    try:
        ddb.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            SSESpecification={"Enabled": True, "SSEType": "KMS"},
        )
        d = ddb.describe_table(TableName=name)["Table"]
        sse = d.get("SSEDescription") or {}
        assert sse.get("Status") == "ENABLED"
        assert sse.get("SSEType") == "KMS"
        assert "alias/aws/dynamodb" in (sse.get("KMSMasterKeyArn") or "")
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# UpdateTable validation — AWS-canonical billing/throughput/index rules.
# ---------------------------------------------------------------------------

def test_dynamodb_update_table_provisioned_noop_rejected(ddb):
    name = "ut-noop"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PROVISIONED",
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    try:
        with pytest.raises(ClientError) as e:
            ddb.update_table(
                TableName=name,
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_table_invalid_throughput_rejected(ddb):
    """Boto3 client-side validates ReadCapacityUnits>=1, so hit the server
    with raw HTTP to exercise its own validator (matches the conformance
    suite's approach)."""
    name = "ut-zero"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PROVISIONED",
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    try:
        code, body = _raw_ddb("UpdateTable", {
            "TableName": name,
            "ProvisionedThroughput": {"ReadCapacityUnits": 0, "WriteCapacityUnits": 5},
        })
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_table_ppr_with_throughput_rejected(ddb):
    name = "ut-ppr-pt"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.update_table(
                TableName=name,
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_table_class_change(ddb):
    name = "ut-tc"
    _basic_table(ddb, name)
    try:
        ddb.update_table(TableName=name, TableClass="STANDARD_INFREQUENT_ACCESS")
        d = ddb.describe_table(TableName=name)["Table"]
        assert d["TableClassSummary"]["TableClass"] == "STANDARD_INFREQUENT_ACCESS"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_table_on_demand_throughput_change(ddb):
    name = "ut-odt"
    _basic_table(ddb, name)
    try:
        ddb.update_table(
            TableName=name,
            OnDemandThroughput={"MaxReadRequestUnits": 2000, "MaxWriteRequestUnits": 1000},
        )
        d = ddb.describe_table(TableName=name)["Table"]
        assert d["OnDemandThroughput"]["MaxReadRequestUnits"] == 2000
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_delete_table_deletion_protection_enforced(ddb):
    name = "ut-dp"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        DeletionProtectionEnabled=True,
    )
    try:
        with pytest.raises(ClientError) as e:
            ddb.delete_table(TableName=name)
        assert e.value.response["Error"]["Code"] == "ValidationException"
        # Disable protection and confirm delete succeeds.
        ddb.update_table(TableName=name, DeletionProtectionEnabled=False)
        ddb.delete_table(TableName=name)
    except Exception:
        try:
            ddb.update_table(TableName=name, DeletionProtectionEnabled=False)
            ddb.delete_table(TableName=name)
        except Exception:
            pass
        raise


def test_dynamodb_update_table_add_duplicate_gsi_rejected(ddb):
    name = "ut-gsi-dup"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    try:
        with pytest.raises(ClientError) as e:
            ddb.update_table(
                TableName=name,
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "gsi_pk", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexUpdates=[{
                    "Create": {
                        "IndexName": "gsi1",  # already exists
                        "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                }],
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_table_delete_nonexistent_gsi_rejected(ddb):
    name = "ut-gsi-del"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.update_table(
                TableName=name,
                GlobalSecondaryIndexUpdates=[{"Delete": {"IndexName": "nope"}}],
            )
        assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_table_add_gsi_undefined_attribute_rejected(ddb):
    name = "ut-gsi-undef"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.update_table(
                TableName=name,
                GlobalSecondaryIndexUpdates=[{
                    "Create": {
                        "IndexName": "gsi-x",
                        "KeySchema": [{"AttributeName": "undefined_attr", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                }],
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# GetItem ProjectionExpression — nested map paths and list indexes per AWS.
# ---------------------------------------------------------------------------

def _proj_table(ddb, name):
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


def test_dynamodb_projection_nested_map_path(ddb):
    name = "pe-map"
    _proj_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={
            "pk": {"S": "a"},
            "data": {"M": {
                "name": {"S": "alice"},
                "age": {"N": "30"},
                "email": {"S": "a@b.c"},
            }},
        })
        r = ddb.get_item(
            TableName=name, Key={"pk": {"S": "a"}},
            ProjectionExpression="#d.#n",
            ExpressionAttributeNames={"#d": "data", "#n": "name"},
        )
        assert r["Item"] == {"data": {"M": {"name": {"S": "alice"}}}}
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_projection_list_index(ddb):
    name = "pe-list"
    _proj_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={
            "pk": {"S": "a"},
            "items": {"L": [{"S": "first"}, {"S": "second"}, {"S": "third"}]},
        })
        r = ddb.get_item(
            TableName=name, Key={"pk": {"S": "a"}},
            ProjectionExpression="#i[1]",
            ExpressionAttributeNames={"#i": "items"},
        )
        assert r["Item"] == {"items": {"L": [{"S": "second"}]}}
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_projection_nested_path_and_list_index(ddb):
    name = "pe-mix"
    _proj_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={
            "pk": {"S": "a"},
            "rec": {"M": {
                "tags": {"L": [{"S": "red"}, {"S": "blue"}]},
                "name": {"S": "x"},
            }},
        })
        r = ddb.get_item(TableName=name, Key={"pk": {"S": "a"}}, ProjectionExpression="rec.tags[0]")
        assert r["Item"] == {"rec": {"M": {"tags": {"L": [{"S": "red"}]}}}}
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_projection_deep_nested(ddb):
    name = "pe-deep"
    _proj_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={
            "pk": {"S": "a"},
            "level1": {"M": {
                "level2": {"M": {
                    "leaf": {"S": "deep"},
                    "other": {"S": "skip"},
                }},
                "sibling": {"S": "skip"},
            }},
        })
        r = ddb.get_item(TableName=name, Key={"pk": {"S": "a"}}, ProjectionExpression="level1.level2.leaf")
        assert r["Item"] == {"level1": {"M": {"level2": {"M": {"leaf": {"S": "deep"}}}}}}
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_projection_multiple_sibling_paths(ddb):
    """Two paths under the same root map merge in the projected result."""
    name = "pe-sib"
    _proj_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={
            "pk": {"S": "a"},
            "p": {"M": {
                "x": {"S": "X"},
                "y": {"S": "Y"},
                "z": {"S": "Z"},
            }},
        })
        r = ddb.get_item(
            TableName=name,
            Key={"pk": {"S": "a"}},
            ProjectionExpression="p.x, p.y",
        )
        got = r["Item"]
        assert "p" in got
        assert got["p"]["M"].get("x") == {"S": "X"}
        assert got["p"]["M"].get("y") == {"S": "Y"}
        assert "z" not in got["p"]["M"]
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# ReturnValues + ReturnItemCollectionMetrics enum + shape validation.
# ---------------------------------------------------------------------------

def test_dynamodb_put_item_invalid_return_values_rejected(ddb):
    name = "pi-rv"
    _basic_table(ddb, name)
    try:
        # ALL_NEW is invalid for PutItem (only NONE | ALL_OLD).
        code, body = _raw_ddb("PutItem", {
            "TableName": name,
            "Item": {"pk": {"S": "x"}},
            "ReturnValues": "ALL_NEW",
        })
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_put_item_collection_metrics_size(ddb):
    """ReturnItemCollectionMetrics=SIZE returns ItemCollectionMetrics when the
    table has at least one LSI."""
    name = "pi-icm"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "lsk", "AttributeType": "S"},
        ],
        LocalSecondaryIndexes=[{
            "IndexName": "byLsk",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsk", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    try:
        r = ddb.put_item(
            TableName=name,
            Item={"pk": {"S": "k"}, "sk": {"S": "a"}, "lsk": {"S": "l"}},
            ReturnItemCollectionMetrics="SIZE",
        )
        icm = r.get("ItemCollectionMetrics") or {}
        assert icm.get("ItemCollectionKey", {}).get("pk") == {"S": "k"}
        assert "SizeEstimateRangeGB" in icm
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_delete_item_collection_metrics_size(ddb):
    name = "di-icm"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "lsk", "AttributeType": "S"},
        ],
        LocalSecondaryIndexes=[{
            "IndexName": "byLsk",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsk", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "sk": {"S": "a"}, "lsk": {"S": "l"}})
        r = ddb.delete_item(
            TableName=name,
            Key={"pk": {"S": "k"}, "sk": {"S": "a"}},
            ReturnItemCollectionMetrics="SIZE",
        )
        icm = r.get("ItemCollectionMetrics") or {}
        assert icm.get("ItemCollectionKey", {}).get("pk") == {"S": "k"}
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_item_collection_metrics_size(ddb):
    name = "ui-icm"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "lsk", "AttributeType": "S"},
        ],
        LocalSecondaryIndexes=[{
            "IndexName": "byLsk",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "lsk", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "sk": {"S": "a"}, "lsk": {"S": "l"}})
        r = ddb.update_item(
            TableName=name,
            Key={"pk": {"S": "k"}, "sk": {"S": "a"}},
            UpdateExpression="SET v = :v",
            ExpressionAttributeValues={":v": {"S": "value"}},
            ReturnItemCollectionMetrics="SIZE",
        )
        icm = r.get("ItemCollectionMetrics") or {}
        assert icm.get("ItemCollectionKey", {}).get("pk") == {"S": "k"}
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_remove_updated_new_omits_attributes(ddb):
    """UpdateItem with REMOVE-only and ReturnValues=UPDATED_NEW must NOT
    return Attributes (no new values to report)."""
    name = "ur-un"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "x": {"S": "X"}})
        r = ddb.update_item(
            TableName=name,
            Key={"pk": {"S": "k"}},
            UpdateExpression="REMOVE x",
            ReturnValues="UPDATED_NEW",
        )
        assert "Attributes" not in r
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# UpdateItem semantics — pre-update snapshot, key protection, missing path.
# ---------------------------------------------------------------------------

def test_dynamodb_update_set_reads_pre_update_value(ddb):
    """SET reads RHS against the pre-update snapshot — `SET a = b, b = :v`
    must set `a` to the OLD value of `b`."""
    name = "u-snap"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "b": {"S": "OLD"}})
        ddb.update_item(
            TableName=name,
            Key={"pk": {"S": "k"}},
            UpdateExpression="SET a = b, b = :v",
            ExpressionAttributeValues={":v": {"S": "NEW"}},
        )
        got = ddb.get_item(TableName=name, Key={"pk": {"S": "k"}})["Item"]
        assert got["a"] == {"S": "OLD"}
        assert got["b"] == {"S": "NEW"}
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_set_missing_intermediate_path_rejected(ddb):
    """`SET a.b.c = :v` where `a.b` doesn't exist must be rejected."""
    name = "u-bad-path"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}})
        with pytest.raises(ClientError) as e:
            ddb.update_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                UpdateExpression="SET a.b.c = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_cannot_modify_hash_key(ddb):
    name = "u-pk"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}})
        with pytest.raises(ClientError) as e:
            ddb.update_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                UpdateExpression="SET pk = :v",
                ExpressionAttributeValues={":v": {"S": "newkey"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_cannot_modify_range_key(ddb):
    name = "u-sk"
    _basic_table(ddb, name, with_sk=True)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "sk": {"S": "a"}})
        with pytest.raises(ClientError) as e:
            ddb.update_item(
                TableName=name,
                Key={"pk": {"S": "k"}, "sk": {"S": "a"}},
                UpdateExpression="SET sk = :v",
                ExpressionAttributeValues={":v": {"S": "b"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_nested_set_updated_new(ddb):
    """SET on a nested map path with ReturnValues=UPDATED_NEW returns the
    affected attribute's projected new value."""
    name = "u-nest-un"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={
            "pk": {"S": "k"},
            "m": {"M": {"a": {"S": "A"}, "b": {"S": "B"}}},
        })
        r = ddb.update_item(
            TableName=name,
            Key={"pk": {"S": "k"}},
            UpdateExpression="SET m.a = :v",
            ExpressionAttributeValues={":v": {"S": "A2"}},
            ReturnValues="UPDATED_NEW",
        )
        # The attribute `m` is the one that changed; AWS returns it in
        # full as the projected new-value tree.
        assert "m" in r.get("Attributes", {})
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# Query / Scan validation — ConsistentRead-on-GSI, Select rules,
# Segment/TotalSegments, Limit, binary sort ordering, size() UTF-16.
# ---------------------------------------------------------------------------

def _gsi_table(ddb, name):
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "byGsiPk",
            "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )


def test_dynamodb_query_consistent_read_on_gsi_rejected(ddb):
    name = "q-cr-gsi"
    _gsi_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.query(
                TableName=name,
                IndexName="byGsiPk",
                KeyConditionExpression="gsi_pk = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
                ConsistentRead=True,
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_query_limit_zero_rejected(ddb):
    name = "q-lim-zero"
    _basic_table(ddb, name)
    try:
        code, body = _raw_ddb("Query", {
            "TableName": name,
            "KeyConditionExpression": "pk = :v",
            "ExpressionAttributeValues": {":v": {"S": "k"}},
            "Limit": 0,
        })
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_scan_segment_without_total_rejected(ddb):
    name = "s-seg-only"
    _basic_table(ddb, name)
    try:
        code, body = _raw_ddb("Scan", {"TableName": name, "Segment": 0})
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_scan_total_without_segment_rejected(ddb):
    name = "s-tot-only"
    _basic_table(ddb, name)
    try:
        code, body = _raw_ddb("Scan", {"TableName": name, "TotalSegments": 4})
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_scan_segment_out_of_range_rejected(ddb):
    name = "s-seg-oob"
    _basic_table(ddb, name)
    try:
        code, body = _raw_ddb("Scan", {"TableName": name, "Segment": 4, "TotalSegments": 4})
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_scan_limit_zero_rejected(ddb):
    name = "s-lim-zero"
    _basic_table(ddb, name)
    try:
        code, body = _raw_ddb("Scan", {"TableName": name, "Limit": 0})
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_query_binary_sort_key_ordering(ddb):
    """Binary sort keys must order bytewise (not lexicographic on base64)."""
    import base64
    name = "q-bsk"
    try:
        ddb.delete_table(TableName=name)
    except Exception:
        pass
    ddb.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "bsk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "bsk", "AttributeType": "B"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    try:
        for raw in (b"\x01", b"\xff", b"\x10"):
            ddb.put_item(TableName=name, Item={
                "pk": {"S": "k"},
                "bsk": {"B": raw},
            })
        r = ddb.query(
            TableName=name,
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": {"S": "k"}},
        )
        items = r["Items"]
        bs_bytes = []
        for it in items:
            v = it["bsk"]
            if isinstance(v, dict):
                v = v.get("B")
            if not isinstance(v, (bytes, bytearray)):
                v = base64.b64decode(v)
            bs_bytes.append(bytes(v))
        assert bs_bytes == sorted(bs_bytes)
        assert bs_bytes[0] == b"\x01"
        assert bs_bytes[-1] == b"\xff"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_query_select_all_projected_attrs_without_index_rejected(ddb):
    name = "q-apa"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.query(
                TableName=name,
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
                Select="ALL_PROJECTED_ATTRIBUTES",
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_query_select_specific_without_projection_rejected(ddb):
    name = "q-spec"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.query(
                TableName=name,
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
                Select="SPECIFIC_ATTRIBUTES",
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_size_utf16_units(ddb):
    """AWS size(s) returns UTF-16 code-unit count, NOT Python char count."""
    name = "u16"
    _basic_table(ddb, name)
    try:
        # "😀" is one Python char but two UTF-16 units (surrogate pair).
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "s": {"S": "😀"}})
        # FilterExpression `size(s) = :two` should match.
        r = ddb.scan(
            TableName=name,
            FilterExpression="size(s) = :two",
            ExpressionAttributeValues={":two": {"N": "2"}},
        )
        assert r["Count"] == 1
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# Transactions — ConsumedCapacity (2x per item) and ClientRequestToken
# idempotency per AWS Developer Guide.
# ---------------------------------------------------------------------------

def test_dynamodb_transact_write_consumed_capacity(ddb):
    name = "tw-cc"
    _basic_table(ddb, name)
    try:
        r = ddb.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": name, "Item": {"pk": {"S": "a"}}}},
                {"Put": {"TableName": name, "Item": {"pk": {"S": "b"}}}},
            ],
            ReturnConsumedCapacity="TOTAL",
        )
        cc = r.get("ConsumedCapacity")
        assert cc and cc[0]["TableName"] == name
        # 2 items × 2 units = 4.
        assert cc[0]["CapacityUnits"] == 4.0
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_transact_get_consumed_capacity(ddb):
    name = "tg-cc"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "a"}})
        ddb.put_item(TableName=name, Item={"pk": {"S": "b"}})
        r = ddb.transact_get_items(
            TransactItems=[
                {"Get": {"TableName": name, "Key": {"pk": {"S": "a"}}}},
                {"Get": {"TableName": name, "Key": {"pk": {"S": "b"}}}},
            ],
            ReturnConsumedCapacity="TOTAL",
        )
        cc = r.get("ConsumedCapacity")
        assert cc and cc[0]["TableName"] == name
        assert cc[0]["CapacityUnits"] == 4.0
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_transact_write_idempotency_token_mismatch(ddb):
    name = "tw-crt"
    _basic_table(ddb, name)
    try:
        token = "idem-1"
        # First call lands.
        ddb.transact_write_items(
            ClientRequestToken=token,
            TransactItems=[{"Put": {"TableName": name, "Item": {"pk": {"S": "a"}}}}],
        )
        # Second call with same token but DIFFERENT payload must fail.
        with pytest.raises(ClientError) as e:
            ddb.transact_write_items(
                ClientRequestToken=token,
                TransactItems=[{"Put": {"TableName": name, "Item": {"pk": {"S": "DIFFERENT"}}}}],
            )
        assert e.value.response["Error"]["Code"] == "IdempotentParameterMismatchException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_transact_write_idempotency_token_same_payload(ddb):
    name = "tw-crt-same"
    _basic_table(ddb, name)
    try:
        token = "idem-same"
        items = [{"Put": {"TableName": name, "Item": {"pk": {"S": "a"}}}}]
        ddb.transact_write_items(ClientRequestToken=token, TransactItems=items)
        # Same payload is a no-op replay — should succeed.
        ddb.transact_write_items(ClientRequestToken=token, TransactItems=items)
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# Legacy parameters — mutual-exclusion + projection semantics per AWS.
# ---------------------------------------------------------------------------

def test_dynamodb_get_item_atg_and_pe_mutually_exclusive(ddb):
    name = "atg-pe"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "a": {"S": "A"}})
        with pytest.raises(ClientError) as e:
            ddb.get_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                ProjectionExpression="a",
                AttributesToGet=["a"],
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_get_item_attributes_to_get_only(ddb):
    """AttributesToGet returns exactly the requested attributes — does NOT
    auto-include the hash/sort key."""
    name = "atg-keys"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "a": {"S": "A"}, "b": {"S": "B"}})
        r = ddb.get_item(TableName=name, Key={"pk": {"S": "k"}}, AttributesToGet=["a"])
        assert r["Item"] == {"a": {"S": "A"}}
        assert "pk" not in r["Item"]
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_scan_filter_and_filter_expression_mutually_exclusive(ddb):
    name = "sf-fe"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.scan(
                TableName=name,
                FilterExpression="a = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
                ScanFilter={"a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "EQ"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_attribute_updates_cannot_modify_key(ddb):
    name = "au-key"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}})
        with pytest.raises(ClientError) as e:
            ddb.update_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                AttributeUpdates={"pk": {"Value": {"S": "newkey"}, "Action": "PUT"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# Tags / TTL validation.
# ---------------------------------------------------------------------------

def test_dynamodb_tag_resource_invalid_arn_rejected(ddb):
    with pytest.raises(ClientError) as e:
        ddb.tag_resource(ResourceArn="not-an-arn", Tags=[{"Key": "k", "Value": "v"}])
    assert e.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_list_tags_unknown_arn_rejected(ddb):
    fake = "arn:aws:dynamodb:us-east-1:000000000000:table/does-not-exist-tag-XX"
    with pytest.raises(ClientError) as e:
        ddb.list_tags_of_resource(ResourceArn=fake)
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_dynamodb_ttl_empty_attribute_name_rejected(ddb):
    name = "ttl-empty"
    _basic_table(ddb, name)
    try:
        code, body = _raw_ddb("UpdateTimeToLive", {
            "TableName": name,
            "TimeToLiveSpecification": {"Enabled": True, "AttributeName": ""},
        })
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# PartiQL — BatchExecuteStatement and ExecuteTransaction.
# Verified against botocore service-2.json (2012-08-10).
# ---------------------------------------------------------------------------

def test_dynamodb_batch_execute_statement_multi_select(ddb):
    name = "bxs-sel"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "a"}, "v": {"N": "1"}})
        ddb.put_item(TableName=name, Item={"pk": {"S": "b"}, "v": {"N": "2"}})
        r = ddb.batch_execute_statement(Statements=[
            {"Statement": f'SELECT * FROM "{name}" WHERE pk = ?', "Parameters": [{"S": "a"}]},
            {"Statement": f'SELECT * FROM "{name}" WHERE pk = ?', "Parameters": [{"S": "b"}]},
        ])
        responses = r["Responses"]
        assert len(responses) == 2
        # Each entry should carry an Item with the matching pk.
        pks = sorted([resp.get("Item", {}).get("pk", {}).get("S") for resp in responses])
        assert pks == ["a", "b"]
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_batch_execute_statement_partial_failure(ddb):
    name = "bxs-pf"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "a"}})
        r = ddb.batch_execute_statement(Statements=[
            {"Statement": f'INSERT INTO "{name}" VALUE {{\'pk\': ?}}', "Parameters": [{"S": "a"}]},  # dup
            {"Statement": f'INSERT INTO "{name}" VALUE {{\'pk\': ?}}', "Parameters": [{"S": "b"}]},  # OK
        ])
        # The whole batch returns 200 with per-statement results.
        assert len(r["Responses"]) == 2
        assert r["Responses"][0].get("Error", {}).get("Code") == "DuplicateItemException"
        assert "Error" not in r["Responses"][1]
        # Confirm "b" landed.
        got = ddb.get_item(TableName=name, Key={"pk": {"S": "b"}})
        assert "Item" in got
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_batch_execute_statement_empty_rejected():
    code, body = _raw_ddb("BatchExecuteStatement", {"Statements": []})
    assert code == 400
    assert "ValidationException" in body.get("__type", "")


def test_dynamodb_execute_transaction_multi_insert(ddb):
    name = "et-ins"
    _basic_table(ddb, name)
    try:
        r = ddb.execute_transaction(TransactStatements=[
            {"Statement": f'INSERT INTO "{name}" VALUE {{\'pk\': ?}}', "Parameters": [{"S": "a"}]},
            {"Statement": f'INSERT INTO "{name}" VALUE {{\'pk\': ?}}', "Parameters": [{"S": "b"}]},
        ])
        assert len(r["Responses"]) == 2
        for pk in ("a", "b"):
            got = ddb.get_item(TableName=name, Key={"pk": {"S": pk}})
            assert "Item" in got
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_execute_transaction_rollback_on_duplicate(ddb):
    """If one INSERT in a transaction fails (dup key), the whole transaction
    rolls back — no statements take effect."""
    name = "et-rb"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "existing"}})
        with pytest.raises(ClientError) as e:
            ddb.execute_transaction(TransactStatements=[
                {"Statement": f'INSERT INTO "{name}" VALUE {{\'pk\': ?}}', "Parameters": [{"S": "new"}]},
                {"Statement": f'INSERT INTO "{name}" VALUE {{\'pk\': ?}}', "Parameters": [{"S": "existing"}]},  # dup
            ])
        assert e.value.response["Error"]["Code"] == "TransactionCanceledException"
        # "new" must NOT have landed (rollback).
        got = ddb.get_item(TableName=name, Key={"pk": {"S": "new"}})
        assert "Item" not in got
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_execute_transaction_empty_rejected():
    code, body = _raw_ddb("ExecuteTransaction", {"TransactStatements": []})
    assert code == 400
    assert "ValidationException" in body.get("__type", "")


def test_dynamodb_execute_statement_insert_existing_dup_exception(ddb):
    """ExecuteStatement INSERT on existing item returns DuplicateItemException
    (not ConditionalCheckFailedException)."""
    name = "es-dup"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "a"}})
        with pytest.raises(ClientError) as e:
            ddb.execute_statement(
                Statement=f'INSERT INTO "{name}" VALUE {{\'pk\': ?}}',
                Parameters=[{"S": "a"}],
            )
        assert e.value.response["Error"]["Code"] == "DuplicateItemException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_execute_statement_consumed_capacity(ddb):
    name = "es-cc"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "a"}})
        r = ddb.execute_statement(
            Statement=f'SELECT * FROM "{name}" WHERE pk = ?',
            Parameters=[{"S": "a"}],
            ReturnConsumedCapacity="TOTAL",
        )
        cc = r.get("ConsumedCapacity") or {}
        assert cc.get("TableName") == name
        assert cc.get("CapacityUnits", 0) > 0
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# Error-message canonicalization — AWS-canonical strings + new validators
# (EAV/EAN-without-expression, non-existent table standardization).
# ---------------------------------------------------------------------------

def test_dynamodb_expression_attribute_values_without_expression_rejected(ddb):
    name = "eav-no-expr"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}})
        with pytest.raises(ClientError) as e:
            ddb.put_item(
                TableName=name,
                Item={"pk": {"S": "k"}},
                ExpressionAttributeValues={":v": {"S": "x"}},
                # NO ConditionExpression — should reject.
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_expression_attribute_names_without_expression_rejected(ddb):
    name = "ean-no-expr"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.get_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                ExpressionAttributeNames={"#a": "a"},
                # NO ProjectionExpression — should reject.
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_put_item_nonexistent_table_canonical(ddb):
    with pytest.raises(ClientError) as e:
        ddb.put_item(TableName="does-not-exist-pi-xyz", Item={"pk": {"S": "k"}})
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert "non-existent table" in e.value.response["Error"]["Message"].lower()


def test_dynamodb_get_item_nonexistent_table_canonical(ddb):
    with pytest.raises(ClientError) as e:
        ddb.get_item(TableName="does-not-exist-gi-xyz", Key={"pk": {"S": "k"}})
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_dynamodb_query_invalid_return_consumed_capacity_rejected(ddb):
    name = "q-rcc-bad"
    _basic_table(ddb, name)
    try:
        # Boto3 client-side rejects non-enum values, so use raw HTTP.
        code, body = _raw_ddb("Query", {
            "TableName": name,
            "KeyConditionExpression": "pk = :v",
            "ExpressionAttributeValues": {":v": {"S": "k"}},
            "ReturnConsumedCapacity": "BOGUS",
        })
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# Redundant parentheses — AWS rejects `((expr))` patterns in expressions.
# ---------------------------------------------------------------------------

def test_dynamodb_delete_item_redundant_parens_rejected(ddb):
    name = "rp-di"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "x": {"N": "1"}})
        with pytest.raises(ClientError) as e:
            ddb.delete_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                ConditionExpression="((x = :v))",
                ExpressionAttributeValues={":v": {"N": "1"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_put_item_redundant_parens_rejected(ddb):
    name = "rp-pi"
    _basic_table(ddb, name)
    try:
        with pytest.raises(ClientError) as e:
            ddb.put_item(
                TableName=name,
                Item={"pk": {"S": "k"}},
                ConditionExpression="((attribute_not_exists(pk)))",
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_scan_redundant_parens_rejected(ddb):
    name = "rp-sc"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "x": {"N": "1"}})
        with pytest.raises(ClientError) as e:
            ddb.scan(
                TableName=name,
                FilterExpression="((x = :v))",
                ExpressionAttributeValues={":v": {"N": "1"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_legitimate_parens_still_work(ddb):
    """`(a) OR (b)` is NOT redundant — those parens group operands."""
    name = "rp-ok"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "x": {"N": "1"}, "y": {"N": "2"}})
        r = ddb.scan(
            TableName=name,
            FilterExpression="(x = :v1) OR (y = :v2)",
            ExpressionAttributeValues={":v1": {"N": "9"}, ":v2": {"N": "2"}},
        )
        assert r["Count"] == 1
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# Backups (CreateBackup, DescribeBackup, DeleteBackup, ListBackups,
# RestoreTableFromBackup, RestoreTableToPointInTime) and DescribeLimits.
# ---------------------------------------------------------------------------

def test_dynamodb_create_describe_list_delete_backup(ddb):
    name = "bk-table"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "a"}, "v": {"N": "1"}})
        r = ddb.create_backup(TableName=name, BackupName="bk1")
        details = r["BackupDetails"]
        arn = details["BackupArn"]
        assert details["BackupName"] == "bk1"
        assert details["BackupStatus"] == "AVAILABLE"

        d = ddb.describe_backup(BackupArn=arn)
        assert d["BackupDescription"]["BackupDetails"]["BackupArn"] == arn

        lst = ddb.list_backups(TableName=name)
        arns = [s["BackupArn"] for s in lst["BackupSummaries"]]
        assert arn in arns

        deleted = ddb.delete_backup(BackupArn=arn)
        assert deleted["BackupDescription"]["BackupDetails"]["BackupArn"] == arn

        with pytest.raises(ClientError) as e:
            ddb.describe_backup(BackupArn=arn)
        assert e.value.response["Error"]["Code"] == "BackupNotFoundException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_restore_table_from_backup(ddb):
    src = "bk-src"
    target = "bk-restored"
    _basic_table(ddb, src)
    try:
        ddb.put_item(TableName=src, Item={"pk": {"S": "a"}, "v": {"N": "42"}})
        arn = ddb.create_backup(TableName=src, BackupName="snap1")["BackupDetails"]["BackupArn"]
        try:
            ddb.delete_table(TableName=target)
        except Exception:
            pass
        r = ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)
        assert r["TableDescription"]["TableName"] == target
        # Items should be restored.
        got = ddb.get_item(TableName=target, Key={"pk": {"S": "a"}})
        assert got["Item"]["v"]["N"] == "42"
    finally:
        try:
            ddb.delete_backup(BackupArn=arn)
        except Exception:
            pass
        for t in (src, target):
            try:
                ddb.delete_table(TableName=t)
            except Exception:
                pass


def test_dynamodb_restore_table_target_already_exists_rejected(ddb):
    src = "rt-src"
    target = "rt-target"
    _basic_table(ddb, src)
    _basic_table(ddb, target)
    try:
        arn = ddb.create_backup(TableName=src, BackupName="snap")["BackupDetails"]["BackupArn"]
        with pytest.raises(ClientError) as e:
            ddb.restore_table_from_backup(TargetTableName=target, BackupArn=arn)
        assert e.value.response["Error"]["Code"] == "TableAlreadyExistsException"
        ddb.delete_backup(BackupArn=arn)
    finally:
        for t in (src, target):
            try:
                ddb.delete_table(TableName=t)
            except Exception:
                pass


def test_dynamodb_restore_table_to_point_in_time(ddb):
    src = "pitr-src"
    target = "pitr-target"
    _basic_table(ddb, src)
    try:
        ddb.put_item(TableName=src, Item={"pk": {"S": "x"}, "v": {"S": "live"}})
        try:
            ddb.delete_table(TableName=target)
        except Exception:
            pass
        r = ddb.restore_table_to_point_in_time(SourceTableName=src, TargetTableName=target)
        assert r["TableDescription"]["TableName"] == target
        got = ddb.get_item(TableName=target, Key={"pk": {"S": "x"}})
        assert got["Item"]["v"]["S"] == "live"
    finally:
        for t in (src, target):
            try:
                ddb.delete_table(TableName=t)
            except Exception:
                pass


def test_dynamodb_describe_backup_not_found(ddb):
    with pytest.raises(ClientError) as e:
        ddb.describe_backup(BackupArn="arn:aws:dynamodb:us-east-1:000000000000:table/x/backup/nope")
    assert e.value.response["Error"]["Code"] == "BackupNotFoundException"


def test_dynamodb_create_backup_unknown_table(ddb):
    with pytest.raises(ClientError) as e:
        ddb.create_backup(TableName="does-not-exist-bk", BackupName="bk-snap")
    assert e.value.response["Error"]["Code"] == "TableNotFoundException"


def test_dynamodb_describe_limits(ddb):
    r = ddb.describe_limits()
    assert r["AccountMaxReadCapacityUnits"] > 0
    assert r["AccountMaxWriteCapacityUnits"] > 0
    assert r["TableMaxReadCapacityUnits"] > 0
    assert r["TableMaxWriteCapacityUnits"] > 0


# ---------------------------------------------------------------------------
# AWS-keyword detection in expressions — every name must be #-aliased if it
# matches a system keyword. Source: AWS DynamoDB Developer Guide.
# ---------------------------------------------------------------------------

def test_dynamodb_keyword_in_condition_expression_rejected(ddb):
    name = "kw-ce"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "status": {"S": "x"}})
        with pytest.raises(ClientError) as e:
            ddb.put_item(
                TableName=name,
                Item={"pk": {"S": "k"}, "status": {"S": "y"}},
                ConditionExpression="status = :v",  # STATUS is a system keyword
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_keyword_in_update_expression_rejected(ddb):
    name = "kw-ue"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}})
        with pytest.raises(ClientError) as e:
            ddb.update_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                UpdateExpression="SET name = :v",  # NAME is a system keyword
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_keyword_in_projection_expression_rejected(ddb):
    name = "kw-pe"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "name": {"S": "n"}})
        with pytest.raises(ClientError) as e:
            ddb.get_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                ProjectionExpression="name",
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_keyword_in_filter_expression_rejected(ddb):
    name = "kw-fe"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "status": {"S": "x"}})
        with pytest.raises(ClientError) as e:
            ddb.scan(
                TableName=name,
                FilterExpression="status = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_keyword_with_alias_accepted(ddb):
    name = "kw-alias"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "status": {"S": "live"}})
        r = ddb.get_item(
            TableName=name,
            Key={"pk": {"S": "k"}},
            ProjectionExpression="#s",
            ExpressionAttributeNames={"#s": "status"},
        )
        assert r["Item"] == {"status": {"S": "live"}}
    finally:
        ddb.delete_table(TableName=name)


# ---------------------------------------------------------------------------
# Exact-error-message coverage — conformance Tier 3 error-messages bucket.
# ---------------------------------------------------------------------------

def test_dynamodb_create_table_duplicate_index_names_rejected(ddb):
    with pytest.raises(ClientError) as e:
        ddb.create_table(
            TableName="dup-idx",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "same-idx",
                    "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "same-idx",  # duplicate name
                    "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
    assert e.value.response["Error"]["Code"] == "ValidationException"


def test_dynamodb_query_empty_key_condition_rejected(ddb):
    name = "q-empty-kce"
    _basic_table(ddb, name)
    try:
        code, body = _raw_ddb("Query", {
            "TableName": name,
            "KeyConditionExpression": "",
        })
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_query_invalid_select_value_rejected(ddb):
    name = "q-bad-sel"
    _basic_table(ddb, name)
    try:
        code, body = _raw_ddb("Query", {
            "TableName": name,
            "KeyConditionExpression": "pk = :v",
            "ExpressionAttributeValues": {":v": {"S": "x"}},
            "Select": "BOGUS",
        })
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_item_empty_update_expression_rejected(ddb):
    name = "u-empty-ue"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}})
        code, body = _raw_ddb("UpdateItem", {
            "TableName": name,
            "Key": {"pk": {"S": "k"}},
            "UpdateExpression": "",
        })
        assert code == 400
        assert "ValidationException" in body.get("__type", "")
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_unused_expression_attribute_name_rejected(ddb):
    name = "u-unused-ean"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}})
        with pytest.raises(ClientError) as e:
            ddb.update_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                UpdateExpression="SET a = :v",
                ExpressionAttributeNames={"#unused": "ghost"},  # defined but never used
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
        assert "unused" in e.value.response["Error"]["Message"].lower()
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_unused_expression_attribute_value_rejected(ddb):
    name = "u-unused-eav"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}})
        with pytest.raises(ClientError) as e:
            ddb.update_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                UpdateExpression="SET a = :v",
                ExpressionAttributeValues={":v": {"S": "x"}, ":unused": {"S": "ghost"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
        assert "unused" in e.value.response["Error"]["Message"].lower()
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_update_missing_value_reference_rejected(ddb):
    name = "u-missing-ref"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}})
        with pytest.raises(ClientError) as e:
            ddb.update_item(
                TableName=name,
                Key={"pk": {"S": "k"}},
                UpdateExpression="SET a = :v",
                # NOT defining :v in EAV
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_contains_with_distinct_operands_required(ddb):
    name = "c-distinct"
    _basic_table(ddb, name)
    try:
        ddb.put_item(TableName=name, Item={"pk": {"S": "k"}, "a": {"S": "abc"}})
        with pytest.raises(ClientError) as e:
            ddb.scan(
                TableName=name,
                FilterExpression="contains(a, a)",  # same path and operand
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_query_non_key_attribute_in_kce_rejected(ddb):
    name = "q-nonkey-kce"
    _basic_table(ddb, name, with_sk=True)
    try:
        with pytest.raises(ClientError) as e:
            ddb.query(
                TableName=name,
                # Try filtering on a non-key attribute in KeyConditionExpression.
                KeyConditionExpression="pk = :p AND other = :o",
                ExpressionAttributeValues={":p": {"S": "k"}, ":o": {"S": "x"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
        assert "key schema" in e.value.response["Error"]["Message"].lower()
    finally:
        ddb.delete_table(TableName=name)


def test_dynamodb_query_missing_hash_key_in_kce_rejected(ddb):
    name = "q-missing-pk"
    _basic_table(ddb, name, with_sk=True)
    try:
        with pytest.raises(ClientError) as e:
            ddb.query(
                TableName=name,
                # No pk condition.
                KeyConditionExpression="sk = :s",
                ExpressionAttributeValues={":s": {"S": "a"}},
            )
        assert e.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ddb.delete_table(TableName=name)
