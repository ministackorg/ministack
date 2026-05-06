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
    resp = ddb.update_item(
        TableName="t_hash_only",
        Key={"pk": {"S": "upd_add"}},
        UpdateExpression="ADD counter :inc",
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
    """BatchGetItem with non-existent table returns it in UnprocessedKeys."""
    resp = ddb.batch_get_item(RequestItems={"qa-ddb-nonexistent-xyz": {"Keys": [{"pk": {"S": "k1"}}]}})
    assert "qa-ddb-nonexistent-xyz" in resp["UnprocessedKeys"]

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
    assert "ConditionalCheckFailed" in exc.value.response["Error"]["Code"]


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


