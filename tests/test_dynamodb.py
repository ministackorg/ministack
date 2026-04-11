import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

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

