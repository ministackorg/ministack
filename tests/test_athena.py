import json
import os
import time
import urllib.request
import uuid as _uuid_mod

import boto3
import duckdb
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import make_client


@pytest.fixture
def persisted_parquet_store(s3):
    """Uploads table data through the S3 API, the way a client stores it on AWS."""

    class _Uploader:
        @staticmethod
        def _persist_object(bucket, key, body):
            s3.put_object(Bucket=bucket, Key=key, Body=body)

    return _Uploader()


def test_athena_queries_glue_backed_parquet(
    athena, glue, s3, persisted_parquet_store, tmp_path,
):
    """Resolve a Glue Parquet table and apply a predicate to its rows."""
    suffix = _uuid_mod.uuid4().hex[:10]
    bucket = f"athena-parquet-{suffix}"
    database = f"parquet_{suffix}"
    s3.create_bucket(Bucket=bucket)
    glue.create_database(DatabaseInput={"Name": database})

    parquet = tmp_path / "items.parquet"
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE items (id INTEGER, category VARCHAR)")
        connection.executemany(
            "INSERT INTO items VALUES (?, ?)", [(1, "keep"), (2, "skip"), (3, "keep")],
        )
        connection.execute(f"COPY items TO '{parquet}' (FORMAT PARQUET)")
    persisted_parquet_store._persist_object(
        bucket, "data/items.parquet", parquet.read_bytes(),
    )
    glue.create_table(DatabaseName=database, TableInput={
        "Name": "items",
        "StorageDescriptor": {
            "Location": f"s3://{bucket}/data/",
            "Columns": [
                {"Name": "id", "Type": "int"},
                {"Name": "category", "Type": "string"},
            ],
        },
        "Parameters": {"classification": "parquet"},
    })
    query_id = athena.start_query_execution(
        QueryString=(f"SELECT id FROM {database}.items "
                     "WHERE category = 'keep' ORDER BY id"),
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": f"s3://{bucket}/results/"},
    )["QueryExecutionId"]
    for _ in range(30):
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        if execution["Status"]["State"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(0.1)
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    assert [row["Data"][0]["VarCharValue"] for row in rows] == ["id", "1", "3"]


def _client(region):
    return boto3.client(
        "athena",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"mode": "standard"}),
    )


def test_athena_query(athena):
    resp = athena.start_query_execution(
        QueryString="SELECT 1 AS num, 'hello' AS greeting",
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )
    query_id = resp["QueryExecutionId"]
    state = None
    for _ in range(10):
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.2)
    assert state == "SUCCEEDED", f"Query ended in state: {state}"
    results = athena.get_query_results(QueryExecutionId=query_id)
    assert len(results["ResultSet"]["Rows"]) >= 1

def test_athena_workgroup(athena):
    athena.create_work_group(
        Name="test-wg",
        Description="Test workgroup",
        Configuration={"ResultConfiguration": {"OutputLocation": "s3://athena-results/test/"}},
    )
    wgs = athena.list_work_groups()
    assert any(wg["Name"] == "test-wg" for wg in wgs["WorkGroups"])
    resp = athena.create_named_query(
        Name="my-query",
        Database="default",
        QueryString="SELECT * FROM my_table LIMIT 10",
        WorkGroup="test-wg",
    )
    assert "NamedQueryId" in resp

def test_athena_query_execution_v2(athena):
    resp = athena.start_query_execution(
        QueryString="SELECT 42 AS answer, 'world' AS hello",
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )
    qid = resp["QueryExecutionId"]
    state = None
    for _ in range(50):
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.1)
    assert state == "SUCCEEDED", f"Query ended in state: {state}"

    results = athena.get_query_results(QueryExecutionId=qid)
    rows = results["ResultSet"]["Rows"]
    assert len(rows) >= 2
    assert rows[0]["Data"][0]["VarCharValue"] == "answer"
    assert rows[1]["Data"][0]["VarCharValue"] == "42"

def test_athena_workgroup_v2(athena):
    athena.create_work_group(
        Name="ath-wg-v2",
        Description="V2 workgroup",
        Configuration={"ResultConfiguration": {"OutputLocation": "s3://ath-out/v2/"}},
    )
    resp = athena.get_work_group(WorkGroup="ath-wg-v2")
    assert resp["WorkGroup"]["Name"] == "ath-wg-v2"
    assert resp["WorkGroup"]["Description"] == "V2 workgroup"
    assert resp["WorkGroup"]["State"] == "ENABLED"

    wgs = athena.list_work_groups()
    assert any(wg["Name"] == "ath-wg-v2" for wg in wgs["WorkGroups"])

    athena.update_work_group(
        WorkGroup="ath-wg-v2",
        ConfigurationUpdates={"ResultConfigurationUpdates": {"OutputLocation": "s3://ath-out/v2-new/"}},
    )
    resp2 = athena.get_work_group(WorkGroup="ath-wg-v2")
    assert "v2-new" in resp2["WorkGroup"]["Configuration"]["ResultConfiguration"]["OutputLocation"]

    athena.delete_work_group(WorkGroup="ath-wg-v2", RecursiveDeleteOption=True)
    with pytest.raises(ClientError):
        athena.get_work_group(WorkGroup="ath-wg-v2")


def test_athena_named_resources_are_region_scoped():
    east = _client("us-east-1")
    west = _client("us-west-2")
    suffix = _uuid_mod.uuid4().hex[:8]
    workgroup = f"ath-region-wg-{suffix}"
    catalog = f"ath-region-catalog-{suffix}"
    statement = f"ath-region-statement-{suffix}"

    for client, region in ((east, "east"), (west, "west")):
        client.create_work_group(Name=workgroup, Description=region)
        client.create_data_catalog(Name=catalog, Type="HIVE", Description=region)
        client.create_prepared_statement(
            StatementName=statement,
            WorkGroup=workgroup,
            QueryStatement=f"SELECT '{region}'",
        )

    try:
        assert east.get_work_group(WorkGroup=workgroup)["WorkGroup"][
            "Description"
        ] == "east"
        assert west.get_work_group(WorkGroup=workgroup)["WorkGroup"][
            "Description"
        ] == "west"
        assert east.get_data_catalog(Name=catalog)["DataCatalog"][
            "Description"
        ] == "east"
        assert west.get_data_catalog(Name=catalog)["DataCatalog"][
            "Description"
        ] == "west"
        assert east.get_prepared_statement(
            StatementName=statement, WorkGroup=workgroup
        )["PreparedStatement"]["QueryStatement"] == "SELECT 'east'"
        assert west.get_prepared_statement(
            StatementName=statement, WorkGroup=workgroup
        )["PreparedStatement"]["QueryStatement"] == "SELECT 'west'"
    finally:
        for client in (east, west):
            client.delete_prepared_statement(
                StatementName=statement, WorkGroup=workgroup
            )
            client.delete_data_catalog(Name=catalog)
            client.delete_work_group(
                WorkGroup=workgroup, RecursiveDeleteOption=True
            )


def test_athena_defaults_are_seeded_per_region():
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import athena as service

    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"

    service.reset()
    try:
        set_request_account_id(account_id)
        set_request_region("us-east-1")
        service._ensure_default_workgroup()
        service._ensure_default_data_catalog()
        service._workgroups["primary"]["Description"] = "east primary"

        set_request_region("us-west-2")
        service._ensure_default_workgroup()
        service._ensure_default_data_catalog()

        assert service._workgroups["primary"]["Description"] == "Primary workgroup"
        assert service._data_catalogs["AwsDataCatalog"]["Type"] == "GLUE"
        assert service._workgroups.get_scoped(
            account_id, "us-east-1", "primary"
        )["Description"] == "east primary"
        assert service._data_catalogs.get_scoped(
            account_id, "us-east-1", "AwsDataCatalog"
        )["Type"] == "GLUE"
    finally:
        service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_athena_legacy_state_migrates_to_configured_boot_region(monkeypatch):
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import athena as service

    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    boot_region = "us-west-2"
    first_request_region = "eu-central-1"
    tags = AccountScopedDict()
    stores = {
        "_executions": ("legacy-execution", {"QueryExecutionId": "legacy-execution"}),
        "_workgroups": ("legacy-workgroup", {"Name": "legacy-workgroup"}),
        "_named_queries": ("legacy-query", {"NamedQueryId": "legacy-query"}),
        "_data_catalogs": ("legacy-catalog", {"Name": "legacy-catalog"}),
        "_prepared_statements": (
            "legacy-workgroup/legacy-statement",
            {"StatementName": "legacy-statement", "WorkGroup": "legacy-workgroup"},
        ),
    }

    set_request_account_id(account_id)
    monkeypatch.setattr(service, "REGION", boot_region)
    set_request_region(first_request_region)
    payload = {}
    for store_name, (key, value) in stores.items():
        legacy = AccountScopedDict()
        legacy[key] = value
        payload[store_name] = legacy
    tag_arn = f"arn:aws:athena:{boot_region}:{account_id}:workgroup/legacy-workgroup"
    tags[tag_arn] = {"legacy": "true"}
    payload["_tags"] = tags

    service.reset()
    try:
        service.load_persisted_state(payload)
        for store_name, (key, value) in stores.items():
            restored = getattr(service, store_name).get_scoped(
                account_id, boot_region, key
            )
            assert restored == value
            assert (
                getattr(service, store_name).get_scoped(
                    account_id, first_request_region, key
                )
                is None
            )
        assert service._tags.get(tag_arn) == {"legacy": "true"}
        assert get_region() == first_request_region
    finally:
        service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_athena_legacy_children_follow_workgroup_region(monkeypatch):
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import athena as service

    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    boot_region = "us-east-1"
    workgroup_region = "us-west-2"
    first_request_region = "eu-central-1"
    workgroup_name = "legacy-workgroup"

    def legacy_store(key, value):
        store = AccountScopedDict()
        store._data[(account_id, key)] = value
        return store

    payload = {
        "_workgroups": legacy_store(
            workgroup_name,
            {
                "Name": workgroup_name,
                "Configuration": {
                    "ResultConfiguration": {
                        "EncryptionConfiguration": {
                            "KmsKey": (
                                f"arn:aws:kms:{workgroup_region}:{account_id}:key/key-id"
                            )
                        }
                    }
                },
            },
        ),
        "_executions": legacy_store(
            "legacy-execution",
            {"QueryExecutionId": "legacy-execution", "WorkGroup": workgroup_name},
        ),
        "_named_queries": legacy_store(
            "legacy-query",
            {"NamedQueryId": "legacy-query", "WorkGroup": workgroup_name},
        ),
        "_prepared_statements": legacy_store(
            f"{workgroup_name}/legacy-statement",
            {
                "StatementName": "legacy-statement",
                "WorkGroupName": workgroup_name,
            },
        ),
        "_data_catalogs": legacy_store(
            "legacy-catalog", {"Name": "legacy-catalog"}
        ),
    }

    monkeypatch.setattr(service, "REGION", boot_region)
    set_request_account_id(account_id)
    set_request_region(first_request_region)
    service.reset()
    try:
        assert service.REGION != first_request_region
        service.load_persisted_state(payload)

        assert service._workgroups.get_scoped(
            account_id, workgroup_region, workgroup_name
        )
        for store_name, key in (
            ("_executions", "legacy-execution"),
            ("_named_queries", "legacy-query"),
            ("_prepared_statements", f"{workgroup_name}/legacy-statement"),
        ):
            store = getattr(service, store_name)
            assert store.get_scoped(account_id, workgroup_region, key)
            assert store.get_scoped(account_id, boot_region, key) is None
            assert store.get_scoped(account_id, first_request_region, key) is None
        assert service._data_catalogs.get_scoped(
            account_id, boot_region, "legacy-catalog"
        )
        assert get_region() == first_request_region
    finally:
        service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_reset_clears_athena_state_across_regions():
    from ministack.core.responses import get_region, set_request_region
    from ministack.services import athena as service

    original_region = get_region()
    regional_stores = (
        service._executions,
        service._workgroups,
        service._named_queries,
        service._data_catalogs,
        service._prepared_statements,
    )

    service.reset()
    try:
        for region in ("us-east-1", "us-west-2"):
            set_request_region(region)
            for store in regional_stores:
                store[f"resource-{region}"] = {"region": region}
        service._tags["arn:aws:athena:us-east-1:000000000000:workgroup/tagged"] = {
            "tag": "value"
        }

        service.reset()
        assert all(not store.has_any() for store in regional_stores)
        assert not service._tags._data
    finally:
        service.reset()
        set_request_region(original_region)

def test_athena_named_query_v2(athena):
    resp = athena.create_named_query(
        Name="ath-nq-v2",
        Database="default",
        QueryString="SELECT * FROM t LIMIT 10",
        WorkGroup="primary",
        Description="Named query v2",
    )
    nqid = resp["NamedQueryId"]
    nq = athena.get_named_query(NamedQueryId=nqid)["NamedQuery"]
    assert nq["Name"] == "ath-nq-v2"
    assert nq["Database"] == "default"
    assert nq["QueryString"] == "SELECT * FROM t LIMIT 10"

    listed = athena.list_named_queries()
    assert nqid in listed["NamedQueryIds"]

    athena.delete_named_query(NamedQueryId=nqid)
    with pytest.raises(ClientError):
        athena.get_named_query(NamedQueryId=nqid)

def test_athena_data_catalog_v2(athena):
    athena.create_data_catalog(
        Name="ath-cat-v2",
        Type="HIVE",
        Description="V2 catalog",
        Parameters={"metadata-function": "arn:aws:lambda:us-east-1:000000000000:function:f"},
    )
    resp = athena.get_data_catalog(Name="ath-cat-v2")
    assert resp["DataCatalog"]["Name"] == "ath-cat-v2"
    assert resp["DataCatalog"]["Type"] == "HIVE"

    listed = athena.list_data_catalogs()
    assert any(c["CatalogName"] == "ath-cat-v2" for c in listed["DataCatalogsSummary"])

    athena.update_data_catalog(Name="ath-cat-v2", Type="HIVE", Description="Updated v2")
    resp2 = athena.get_data_catalog(Name="ath-cat-v2")
    assert resp2["DataCatalog"]["Description"] == "Updated v2"

    athena.delete_data_catalog(Name="ath-cat-v2")
    with pytest.raises(ClientError):
        athena.get_data_catalog(Name="ath-cat-v2")

def test_athena_prepared_statement_v2(athena):
    athena.create_work_group(
        Name="ath-ps-v2wg",
        Description="PS WG",
        Configuration={"ResultConfiguration": {"OutputLocation": "s3://out/"}},
    )
    athena.create_prepared_statement(
        StatementName="ath-ps-v2",
        WorkGroup="ath-ps-v2wg",
        QueryStatement="SELECT ? AS val",
        Description="Prepared v2",
    )
    resp = athena.get_prepared_statement(StatementName="ath-ps-v2", WorkGroup="ath-ps-v2wg")
    assert resp["PreparedStatement"]["StatementName"] == "ath-ps-v2"
    assert resp["PreparedStatement"]["QueryStatement"] == "SELECT ? AS val"

    listed = athena.list_prepared_statements(WorkGroup="ath-ps-v2wg")
    assert any(s["StatementName"] == "ath-ps-v2" for s in listed["PreparedStatements"])

    athena.delete_prepared_statement(StatementName="ath-ps-v2", WorkGroup="ath-ps-v2wg")
    with pytest.raises(ClientError):
        athena.get_prepared_statement(StatementName="ath-ps-v2", WorkGroup="ath-ps-v2wg")

def test_athena_tags_v2(athena):
    athena.create_work_group(
        Name="ath-tag-v2wg",
        Description="Tag WG",
        Configuration={"ResultConfiguration": {"OutputLocation": "s3://out/"}},
        Tags=[{"Key": "init", "Value": "yes"}],
    )
    arn = athena.get_work_group(WorkGroup="ath-tag-v2wg")["WorkGroup"]["Configuration"]["ResultConfiguration"][
        "OutputLocation"
    ]
    wg_arn = "arn:aws:athena:us-east-1:000000000000:workgroup/ath-tag-v2wg"

    athena.tag_resource(ResourceARN=wg_arn, Tags=[{"Key": "env", "Value": "dev"}])
    resp = athena.list_tags_for_resource(ResourceARN=wg_arn)
    tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tag_map["env"] == "dev"

    athena.untag_resource(ResourceARN=wg_arn, TagKeys=["env"])
    resp2 = athena.list_tags_for_resource(ResourceARN=wg_arn)
    assert not any(t["Key"] == "env" for t in resp2["Tags"])


def test_athena_tag_resource_arn_parser_accepts_local_resource_shapes():
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import athena as m

    original_account = get_account_id()
    original_region = get_region()
    original_tags = dict(m._tags._data)

    try:
        m._tags.clear()
        set_request_account_id("000000000000")
        set_request_region("us-east-1")

        workgroup_arn = "arn:aws:athena:us-east-1:000000000000:workgroup/parser-wg"
        catalog_arn = "arn:aws:athena:us-east-1:000000000000:datacatalog/parser-catalog"

        assert m._tag_resource({
            "ResourceARN": workgroup_arn,
            "Tags": [{"Key": "env", "Value": "east"}],
        })[0] == 200
        assert m._tag_resource({
            "ResourceARN": catalog_arn,
            "Tags": [{"Key": "team", "Value": "data"}],
        })[0] == 200

        _status, _headers, body = m._list_tags_for_resource({"ResourceARN": workgroup_arn})
        assert json.loads(body)["Tags"] == [{"Key": "env", "Value": "east"}]

        assert m._untag_resource({"ResourceARN": workgroup_arn, "TagKeys": ["env"]})[0] == 200
        _status, _headers, body = m._list_tags_for_resource({"ResourceARN": workgroup_arn})
        assert json.loads(body)["Tags"] == []
        assert m._tags.get(catalog_arn) == {"team": "data"}
    finally:
        m._tags.clear()
        m._tags._data.update(original_tags)
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_athena_tag_resource_arn_parser_rejects_invalid_scope_without_mutation():
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import athena as m

    original_account = get_account_id()
    original_region = get_region()
    original_tags = dict(m._tags._data)

    def assert_invalid(response):
        status, headers, body = response
        assert status == 400
        assert headers["x-amzn-errortype"] == "InvalidRequestException"
        assert json.loads(body)["__type"] == "InvalidRequestException"

    try:
        m._tags.clear()
        set_request_account_id("000000000000")
        set_request_region("us-east-1")

        invalid_arns = [
            "not-an-arn",
            "arn:aws:s3:us-east-1:000000000000:workgroup/parser-wg",
            "arn:aws-cn:athena:us-east-1:000000000000:workgroup/parser-wg",
            "arn:aws:athena:us-west-2:000000000000:workgroup/parser-wg",
            "arn:aws:athena:us-east-1:111111111111:workgroup/parser-wg",
            "arn:aws:athena:us-east-1:000000000000:namedquery/parser-query",
            "arn:aws:athena:us-east-1:000000000000:workgroup/parser-wg/extra",
        ]

        for arn in invalid_arns:
            assert_invalid(m._tag_resource({
                "ResourceARN": arn,
                "Tags": [{"Key": "env", "Value": "bad"}],
            }))
            assert_invalid(m._untag_resource({"ResourceARN": arn, "TagKeys": ["env"]}))
            assert_invalid(m._list_tags_for_resource({"ResourceARN": arn}))

        assert m._tags._data == {}
    finally:
        m._tags.clear()
        m._tags._data.update(original_tags)
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_athena_update_workgroup(athena):
    import uuid as _uuid

    wg = f"intg-wg-update-{_uuid.uuid4().hex[:8]}"
    athena.create_work_group(Name=wg, Description="before")
    athena.update_work_group(WorkGroup=wg, Description="after")
    resp = athena.get_work_group(WorkGroup=wg)
    assert resp["WorkGroup"]["Description"] == "after"
    athena.delete_work_group(WorkGroup=wg, RecursiveDeleteOption=True)

def test_athena_batch_get_named_query(athena):
    import uuid as _uuid

    wg = f"intg-wg-batch-{_uuid.uuid4().hex[:8]}"
    athena.create_work_group(Name=wg)
    nq1 = athena.create_named_query(
        Name="q1",
        Database="default",
        QueryString="SELECT 1",
        WorkGroup=wg,
    )["NamedQueryId"]
    nq2 = athena.create_named_query(
        Name="q2",
        Database="default",
        QueryString="SELECT 2",
        WorkGroup=wg,
    )["NamedQueryId"]
    resp = athena.batch_get_named_query(NamedQueryIds=[nq1, nq2, "nonexistent-id"])
    assert len(resp["NamedQueries"]) == 2
    assert len(resp["UnprocessedNamedQueryIds"]) == 1
    athena.delete_work_group(WorkGroup=wg, RecursiveDeleteOption=True)

def test_athena_batch_get_query_execution(athena):
    qid1 = athena.start_query_execution(
        QueryString="SELECT 42",
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )["QueryExecutionId"]
    qid2 = athena.start_query_execution(
        QueryString="SELECT 99",
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )["QueryExecutionId"]
    time.sleep(1.0)
    resp = athena.batch_get_query_execution(QueryExecutionIds=[qid1, qid2, "nonexistent-id"])
    assert len(resp["QueryExecutions"]) == 2
    assert len(resp["UnprocessedQueryExecutionIds"]) == 1

def test_athena_stop_query(athena):
    """StopQueryExecution cancels a running query."""
    resp = athena.start_query_execution(
        QueryString="SELECT 1",
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )
    qid = resp["QueryExecutionId"]
    athena.stop_query_execution(QueryExecutionId=qid)
    desc = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
    assert desc["Status"]["State"] in ("CANCELLED", "SUCCEEDED")

def test_athena_prepared_statement_crud(athena):
    """CreatePreparedStatement / GetPreparedStatement / DeletePreparedStatement."""
    athena.create_prepared_statement(
        StatementName="qa-athena-stmt",
        WorkGroup="primary",
        QueryStatement="SELECT * FROM tbl WHERE id = ?",
        Description="test stmt",
    )
    stmt = athena.get_prepared_statement(StatementName="qa-athena-stmt", WorkGroup="primary")["PreparedStatement"]
    assert stmt["StatementName"] == "qa-athena-stmt"
    assert "SELECT" in stmt["QueryStatement"]
    stmts = athena.list_prepared_statements(WorkGroup="primary")["PreparedStatements"]
    assert any(s["StatementName"] == "qa-athena-stmt" for s in stmts)
    athena.delete_prepared_statement(StatementName="qa-athena-stmt", WorkGroup="primary")
    stmts2 = athena.list_prepared_statements(WorkGroup="primary")["PreparedStatements"]
    assert not any(s["StatementName"] == "qa-athena-stmt" for s in stmts2)

def test_athena_data_catalog_crud(athena):
    """CreateDataCatalog / GetDataCatalog / ListDataCatalogs / DeleteDataCatalog."""
    athena.create_data_catalog(Name="qa-athena-catalog", Type="HIVE", Description="test catalog")
    catalog = athena.get_data_catalog(Name="qa-athena-catalog")["DataCatalog"]
    assert catalog["Name"] == "qa-athena-catalog"
    assert catalog["Type"] == "HIVE"
    catalogs = athena.list_data_catalogs()["DataCatalogsSummary"]
    assert any(c["CatalogName"] == "qa-athena-catalog" for c in catalogs)
    athena.delete_data_catalog(Name="qa-athena-catalog")
    catalogs2 = athena.list_data_catalogs()["DataCatalogsSummary"]
    assert not any(c["CatalogName"] == "qa-athena-catalog" for c in catalogs2)

def test_athena_engine_mock_via_config(athena):
    """Switching ATHENA_ENGINE to 'mock' via /_ministack/config returns mock results."""
    import json as _json
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        f"{endpoint}/_ministack/config",
        data=_json.dumps({"athena.ATHENA_ENGINE": "mock"}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = _json.loads(urllib.request.urlopen(req, timeout=5).read())
    assert resp["applied"].get("athena.ATHENA_ENGINE") == "mock"

    # Query executes and succeeds in mock mode
    qid = athena.start_query_execution(
        QueryString="SELECT 1",
        ResultConfiguration={"OutputLocation": "s3://athena-results/"},
    )["QueryExecutionId"]
    import time as _time

    for _ in range(10):
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED"):
            break
        _time.sleep(0.2)
    assert state == "SUCCEEDED"

    # Reset back to auto
    req2 = urllib.request.Request(
        f"{endpoint}/_ministack/config",
        data=_json.dumps({"athena.ATHENA_ENGINE": "auto"}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req2, timeout=5)


def test_athena_table_metadata_includes_partition_keys(athena, glue):
    """GetTableMetadata / ListTableMetadata surface the Glue table's real columns
    and partition keys rather than empty stubs (#1423)."""
    glue.create_database(DatabaseInput={"Name": "md_db"})
    glue.create_table(DatabaseName="md_db", TableInput={
        "Name": "events",
        "StorageDescriptor": {"Columns": [{"Name": "id", "Type": "bigint"}]},
        "PartitionKeys": [{"Name": "dt", "Type": "string"}],
    })
    md = athena.get_table_metadata(
        CatalogName="AwsDataCatalog", DatabaseName="md_db",
        TableName="events")["TableMetadata"]
    assert [c["Name"] for c in md["Columns"]] == ["id"]
    assert [p["Name"] for p in md["PartitionKeys"]] == ["dt"]
    lst = athena.list_table_metadata(
        CatalogName="AwsDataCatalog", DatabaseName="md_db")["TableMetadataList"]
    assert any(
        t["Name"] == "events" and [p["Name"] for p in t["PartitionKeys"]] == ["dt"]
        for t in lst)


def test_athena_mixed_glue_and_s3_uri(athena, glue, monkeypatch, tmp_path):
    bucket_name = "athena-results"
    db_name = "test_db_athena_glue_s3"

    from ministack.services import s3 as s3mod
    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)

    import json as _json
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        f"{endpoint}/_ministack/config",
        data=_json.dumps({"athena.ATHENA_DATA_DIR": str(tmp_path)}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=5)

    make_client("s3").put_object(Bucket=bucket_name, Key="tables/users/data.csv",
                                  Body=b"id,name\n1,alice\n2,bob")

    s3mod._persist_object(
        bucket_name,
        "raw/age_info.csv",
        b"id,age\n1,25\n2,30"
    )

    s3mod._persist_object(
        bucket_name,
        "raw/height_info.csv",
        b"id,height\n1,170\n2,180"
    )

    glue.create_database(DatabaseInput={'Name': db_name})
    glue.create_table(
        DatabaseName=db_name,
        TableInput={
            'Name': 'users_table',
            'StorageDescriptor': {
                'Location': f's3://{bucket_name}/tables/users/',
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            },
            'Parameters': {'classification': 'csv'}
        }
    )

    query = f"""
        SELECT u.name, a.age, h.height
        FROM {db_name}.users_table u
        JOIN read_csv('s3://{bucket_name}/raw/age_info.csv') a ON u.id = a.id
        JOIN 's3://{bucket_name}/raw/height_info.csv' h ON u.id = h.id
        ORDER BY u.id
    """

    output_loc = f"s3://{bucket_name}/results/"

    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': db_name},
        ResultConfiguration={'OutputLocation': output_loc}
    )
    query_id = resp['QueryExecutionId']

    for _ in range(10):
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(0.2)

    assert state == "SUCCEEDED", f"Query ended in state: {state}"
    results = athena.get_query_results(QueryExecutionId=query_id)

    rows = results["ResultSet"]["Rows"]
    assert len(rows) >= 2
    assert rows[1]["Data"][0]["VarCharValue"] == "alice"
    assert rows[1]["Data"][1]["VarCharValue"] == "25"
    assert rows[1]["Data"][2]["VarCharValue"] == "170"


@pytest.fixture(scope="module", autouse=True)
def _create_s3_results_bucket(s3):
    s3.create_bucket(Bucket="athena-results")


@pytest.fixture(autouse=True)
def _primary_result_location(athena):
    """AWS's primary workgroup has no result location until the user sets one."""
    athena.update_work_group(WorkGroup="primary", ConfigurationUpdates={
        "ResultConfigurationUpdates": {"OutputLocation": "s3://athena-results/"}})


def test_athena_list_and_get_databases_from_glue(athena, glue):
    """ListDatabases / GetDatabase read the Glue Data Catalog, so a catalog client that
    walks databases before tables sees what Glue holds."""
    glue.create_database(DatabaseInput={"Name": "ath_db_list", "Description": "listed"})
    names = [d["Name"] for d in athena.list_databases(CatalogName="AwsDataCatalog")["DatabaseList"]]
    assert "ath_db_list" in names
    got = athena.get_database(CatalogName="AwsDataCatalog", DatabaseName="ath_db_list")["Database"]
    assert got["Name"] == "ath_db_list" and got["Description"] == "listed"
    with pytest.raises(ClientError) as err:
        athena.get_database(CatalogName="AwsDataCatalog", DatabaseName="ath_db_missing")
    assert err.value.response["Error"]["Code"] == "MetadataException"
    with pytest.raises(ClientError):
        athena.list_databases(CatalogName="no_such_catalog")
    page = athena.list_databases(CatalogName="AwsDataCatalog", MaxResults=1)
    assert len(page["DatabaseList"]) == 1 and "NextToken" in page
    glue.create_database(DatabaseInput={"Name": "ath_db_nodesc"})
    got = athena.get_database(CatalogName="AwsDataCatalog", DatabaseName="ath_db_nodesc")["Database"]
    assert "Description" not in got
    with pytest.raises(ClientError) as err:
        athena.list_databases(CatalogName="AwsDataCatalog", NextToken="not-a-token")
    assert err.value.response["Error"]["Code"] == "InvalidRequestException"


def test_athena_workgroup_reports_engine_version(athena):
    version = athena.get_work_group(WorkGroup="primary")["WorkGroup"]["Configuration"]["EngineVersion"]
    assert version["EffectiveEngineVersion"] == "Athena engine version 3"


def _run_to_completion(athena, query, database):
    query_id = athena.start_query_execution(
        QueryString=query, QueryExecutionContext={"Database": database},
    )["QueryExecutionId"]
    for _ in range(20):
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        if execution["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.2)
    return query_id, execution


def _seed_users(monkeypatch, tmp_path, glue, db_name):
    from ministack.services import s3 as s3mod
    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        f"{endpoint}/_ministack/config",
        data=json.dumps({"athena.ATHENA_DATA_DIR": str(tmp_path)}).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    urllib.request.urlopen(req, timeout=5)
    make_client("s3").put_object(Bucket="athena-results", Key=f"{db_name}/users/data.csv",
                                  Body=b"id,email\n1,a@example.com\n2,b@example.com")
    glue.create_database(DatabaseInput={"Name": db_name})
    glue.create_table(DatabaseName=db_name, TableInput={
        "Name": "users",
        "StorageDescriptor": {
            "Columns": [{"Name": "id", "Type": "bigint"}, {"Name": "email", "Type": "string"}],
            "Location": f"s3://athena-results/{db_name}/users/",
        },
        "Parameters": {"classification": "csv", "skip.header.line.count": "1"},
    })


def test_athena_resolves_quoted_and_catalog_qualified_tables(athena, glue, monkeypatch, tmp_path):
    """Trino clients emit "catalog"."db"."table" AS "alias"; the reference resolves to the
    table's data and a reference without an alias keeps resolving qualified columns."""
    db_name = "ath_quoted_db"
    _seed_users(monkeypatch, tmp_path, glue, db_name)
    query = (f'SELECT "users"."id" AS "id", "users"."email" AS "email" '
             f'FROM "awsdatacatalog"."{db_name}"."users" AS "users" ORDER BY "users"."id"')
    query_id, execution = _run_to_completion(athena, query, db_name)
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    assert [d["VarCharValue"] for d in rows[0]["Data"]] == ["id", "email"]
    assert [d["VarCharValue"] for d in rows[1]["Data"]] == ["1", "a@example.com"]

    query_id, execution = _run_to_completion(athena, "SELECT users.email FROM users WHERE users.id = 2", db_name)
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    assert [d["VarCharValue"] for d in rows[1]["Data"]] == ["b@example.com"]

    # A CTE named like the Glue table shadows it, and a leading comment does not hide the header row.
    query = "-- note\nWITH users AS (SELECT 99 AS id) SELECT users.id FROM users"
    query_id, execution = _run_to_completion(athena, query, db_name)
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    assert execution["StatementType"] == "DML"
    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    assert [[d["VarCharValue"] for d in r["Data"]] for r in rows] == [["id"], ["99"]]


def test_athena_header_row_only_on_first_page(athena, glue, monkeypatch, tmp_path):
    db_name = "ath_paged_db"
    _seed_users(monkeypatch, tmp_path, glue, db_name)
    query_id, execution = _run_to_completion(athena, "SELECT id FROM users ORDER BY id", db_name)
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    first = athena.get_query_results(QueryExecutionId=query_id, MaxResults=1)
    assert [r["Data"][0]["VarCharValue"] for r in first["ResultSet"]["Rows"]] == ["id", "1"]
    second = athena.get_query_results(QueryExecutionId=query_id, MaxResults=1, NextToken=first["NextToken"])
    assert [r["Data"][0]["VarCharValue"] for r in second["ResultSet"]["Rows"]] == ["2"]


def test_athena_create_and_drop_table_apply_to_glue(athena, glue, monkeypatch, tmp_path):
    """CREATE EXTERNAL TABLE registers the table in Glue (so metadata calls see it), an empty
    location reads as zero rows, and DROP TABLE removes it."""
    db_name = "ath_ddl_db"
    _seed_users(monkeypatch, tmp_path, glue, db_name)
    ddl = "CREATE EXTERNAL TABLE events (id INT, note STRING COMMENT 'free text') LOCATION 's3://athena-results/ath_ddl_db/events/'"
    _, execution = _run_to_completion(athena, ddl, db_name)
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    assert execution["StatementType"] == "DDL"
    md = athena.get_table_metadata(CatalogName="AwsDataCatalog", DatabaseName=db_name, TableName="events")["TableMetadata"]
    assert [(c["Name"], c["Type"]) for c in md["Columns"]] == [("id", "int"), ("note", "string")]
    assert md["Columns"][1]["Comment"] == "free text"

    query_id, execution = _run_to_completion(athena, "SELECT id, note FROM events", db_name)
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    result = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]
    assert [c["Name"] for c in result["ResultSetMetadata"]["ColumnInfo"]] == ["id", "note"]
    assert len(result["Rows"]) == 1  # header only

    _, execution = _run_to_completion(athena, "DROP TABLE events", db_name)
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    listed = athena.list_table_metadata(CatalogName="AwsDataCatalog", DatabaseName=db_name)["TableMetadataList"]
    assert [t["Name"] for t in listed] == ["users"]
    with pytest.raises(ClientError):
        glue.get_table(DatabaseName=db_name, Name="events")
    _, execution = _run_to_completion(athena, "DROP TABLE events", db_name)
    assert execution["Status"]["State"] == "FAILED"


def test_athena_create_table_requires_external(athena, glue):
    """Athena rejects a Hive CREATE TABLE without EXTERNAL at submission, not as a failed execution."""
    glue.create_database(DatabaseInput={"Name": "ath_managed_db"})
    with pytest.raises(ClientError) as err:
        athena.start_query_execution(
            QueryString="CREATE TABLE t (a int) LOCATION 's3://athena-results/t/'",
            QueryExecutionContext={"Database": "ath_managed_db"},
        )
    assert err.value.response["Error"]["Code"] == "InvalidRequestException"
    assert err.value.response["Error"]["Message"] == "External keyword required for table type HIVE"
    assert err.value.response["AthenaErrorCode"] == "MALFORMED_QUERY"
    with pytest.raises(ClientError):
        glue.get_table(DatabaseName="ath_managed_db", Name="t")


def test_athena_create_iceberg_table_is_not_supported(athena, glue):
    glue.create_database(DatabaseInput={"Name": "ath_iceberg_db"})
    _, execution = _run_to_completion(
        athena,
        "CREATE TABLE t (a int) LOCATION 's3://athena-results/ice/' TBLPROPERTIES ('table_type'='ICEBERG')",
        "ath_iceberg_db",
    )
    assert execution["Status"]["State"] == "FAILED"
    assert "NOT_SUPPORTED" in execution["Status"]["StateChangeReason"]


def test_athena_create_table_writes_the_glue_table_athena_does(athena, glue):
    glue.create_database(DatabaseInput={"Name": "ath_shape_db"})
    ddl = (
        "CREATE EXTERNAL TABLE p (a int COMMENT 'ca', b string) COMMENT 'table comment' "
        "PARTITIONED BY (dt string) STORED AS PARQUET LOCATION 's3://athena-results/parquet/' "
        "TBLPROPERTIES ('classification'='parquet', 'x'='y')"
    )
    _, execution = _run_to_completion(athena, ddl, "ath_shape_db")
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    table = glue.get_table(DatabaseName="ath_shape_db", Name="p")["Table"]
    assert table["Owner"] == "hadoop" and table["TableType"] == "EXTERNAL_TABLE"
    assert "Description" not in table or not table["Description"]
    parameters = table["Parameters"]
    assert parameters.pop("transient_lastDdlTime").isdigit()
    assert parameters == {"EXTERNAL": "TRUE", "classification": "parquet", "x": "y", "comment": "table comment"}
    assert table["PartitionKeys"] == [{"Name": "dt", "Type": "string"}]
    storage = table["StorageDescriptor"]
    assert storage["Columns"] == [{"Name": "a", "Type": "int", "Comment": "ca"}, {"Name": "b", "Type": "string"}]
    assert storage["Location"] == "s3://athena-results/parquet"
    assert storage["InputFormat"] == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    assert storage["OutputFormat"] == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    assert storage["SerdeInfo"] == {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        "Parameters": {"serialization.format": "1"},
    }
    assert storage["NumberOfBuckets"] == -1 and storage["Compressed"] is False

    _, execution = _run_to_completion(
        athena,
        "CREATE EXTERNAL TABLE d (a int, b string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
        "LOCATION 's3://athena-results/delim/'",
        "ath_shape_db",
    )
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    storage = glue.get_table(DatabaseName="ath_shape_db", Name="d")["Table"]["StorageDescriptor"]
    assert storage["InputFormat"] == "org.apache.hadoop.mapred.TextInputFormat"
    assert storage["OutputFormat"] == "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    assert storage["SerdeInfo"] == {
        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "Parameters": {"serialization.format": ",", "field.delim": ","},
    }


def test_athena_reads_a_parquet_table_created_without_classification(
    athena, glue, s3, persisted_parquet_store, tmp_path,
):
    """A table's format comes from its InputFormat when no classification property names it."""
    suffix = _uuid_mod.uuid4().hex[:10]
    bucket, database = f"athena-ddl-parquet-{suffix}", f"ddl_parquet_{suffix}"
    s3.create_bucket(Bucket=bucket)
    glue.create_database(DatabaseInput={"Name": database})
    parquet = tmp_path / "items.parquet"
    with duckdb.connect() as connection:
        connection.execute("COPY (SELECT * FROM (VALUES (1), (2)) v(id)) TO '%s' (FORMAT PARQUET)" % parquet)
    persisted_parquet_store._persist_object(bucket, "data/items.parquet", parquet.read_bytes())
    _, execution = _run_to_completion(
        athena, f"CREATE EXTERNAL TABLE items (id int) STORED AS PARQUET LOCATION 's3://{bucket}/data/'", database,
    )
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    assert "classification" not in glue.get_table(DatabaseName=database, Name="items")["Table"]["Parameters"]
    query_id, execution = _run_to_completion(athena, "SELECT id FROM items ORDER BY id", database)
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    assert [row["Data"][0]["VarCharValue"] for row in rows] == ["id", "1", "2"]


def test_athena_partitioned_table_types_partitions_and_resolves_qualified_columns(
    athena, glue, s3, persisted_parquet_store, tmp_path,
):
    """Partition keys take their Glue types, and ``db.table.column`` resolves against the table."""
    suffix = _uuid_mod.uuid4().hex[:10]
    bucket, database = f"athena-partitions-{suffix}", f"partitions_{suffix}"
    s3.create_bucket(Bucket=bucket)
    glue.create_database(DatabaseInput={"Name": database})
    parquet = tmp_path / "event.parquet"
    with duckdb.connect() as connection:
        connection.execute("COPY (SELECT 'a' AS note, 1 AS action) TO '%s' (FORMAT PARQUET)" % parquet)
    persisted_parquet_store._persist_object(
        bucket, "usage/year=2026/month=04/day=23/event.parquet", parquet.read_bytes(),
    )
    glue.create_table(DatabaseName=database, TableInput={
        "Name": "usage",
        "StorageDescriptor": {
            "Location": f"s3://{bucket}/usage/",
            "Columns": [{"Name": "note", "Type": "string"}, {"Name": "action", "Type": "int"}],
        },
        "PartitionKeys": [
            {"Name": "year", "Type": "string"},
            {"Name": "month", "Type": "string"},
            {"Name": "day", "Type": "string"},
        ],
        "Parameters": {"classification": "parquet"},
    })

    query_id, execution = _run_to_completion(
        athena, f"SELECT {database}.usage.note, year, month, day FROM {database}.usage", database,
    )
    assert execution["Status"]["State"] == "SUCCEEDED", execution["Status"]
    result = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]
    assert [cell["VarCharValue"] for cell in result["Rows"][1]["Data"]] == ["a", "2026", "04", "23"]
    assert [column["Type"] for column in result["ResultSetMetadata"]["ColumnInfo"]] == ["varchar"] * 4


# ---- DDL and table-reference parsing (in-process, no server) ----


def test_parse_create_external_table_with_location_and_comment():
    from ministack.services.athena import _CreateTable, _parse_ddl

    ddl = _parse_ddl(
        "CREATE EXTERNAL TABLE events (id INT, note STRING COMMENT 'free text') "
        "LOCATION 's3://bucket/db/events/'"
    )
    assert ddl == _CreateTable(
        None, "events", external=True,
        columns=[{"Name": "id", "Type": "int"}, {"Name": "note", "Type": "string", "Comment": "free text"}],
        location="s3://bucket/db/events/",
    )


def test_parse_create_table_if_not_exists_qualified_and_quoted():
    from ministack.services.athena import _parse_ddl

    ddl = _parse_ddl('CREATE TABLE IF NOT EXISTS `my db`."Users" ("Id" bigint)')
    assert ddl.if_not_exists is True and ddl.external is False
    assert (ddl.database, ddl.table) == ("my db", "Users")
    assert ddl.columns == [{"Name": "Id", "Type": "bigint"}]


@pytest.mark.parametrize("clause, input_format, output_format, serde", [
    ("", "org.apache.hadoop.mapred.TextInputFormat", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
     "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
    ("STORED AS orc", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
     "org.apache.hadoop.hive.ql.io.orc.OrcSerde"),
    ("STORED AS INPUTFORMAT 'in.Format' OUTPUTFORMAT 'out.Format'", "in.Format", "out.Format",
     "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
    ("ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'", "org.apache.hadoop.mapred.TextInputFormat",
     "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat", "org.openx.data.jsonserde.JsonSerDe"),
])
def test_parse_storage_clauses_map_to_athena_formats(clause, input_format, output_format, serde):
    from ministack.services.athena import _parse_ddl

    ddl = _parse_ddl(f"CREATE EXTERNAL TABLE t (a int) {clause} LOCATION 's3://b/t/'")
    assert (ddl.input_format, ddl.output_format, ddl.serde) == (input_format, output_format, serde)


def test_parse_serde_properties_delimiters_and_buckets():
    from ministack.services.athena import _parse_ddl

    ddl = _parse_ddl(
        "CREATE EXTERNAL TABLE t (a int) CLUSTERED BY (a) INTO 4 BUCKETS "
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' "
        "TBLPROPERTIES ('skip.header.line.count'='1')"
    )
    assert ddl.serde_parameters == {"serialization.format": "\t", "field.delim": "\t", "line.delim": "\n"}
    assert (ddl.bucket_columns, ddl.number_of_buckets) == (["a"], 4)
    assert ddl.table_properties == {"skip.header.line.count": "1"}
    ddl = _parse_ddl(
        "CREATE EXTERNAL TABLE t (a int) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' "
        "WITH SERDEPROPERTIES ('ignore.malformed.json'='true')"
    )
    assert ddl.serde_parameters == {"serialization.format": "1", "ignore.malformed.json": "true"}


def test_parse_iceberg_table_properties():
    from ministack.services.athena import _parse_ddl

    assert _parse_ddl("CREATE TABLE t (a int) TBLPROPERTIES ('table_type'='ICEBERG')").iceberg is True
    assert _parse_ddl("CREATE EXTERNAL TABLE t (a int)").iceberg is False


@pytest.mark.parametrize("query", [
    "SELECT 1",
    "CREATE TABLE t ()",                                                   # no columns
    "CREATE TABLE t (id)",                                                 # column without a type
    "CREATE TABLE t (id int",                                              # unclosed
    "CREATE TABLE t AS SELECT 1",                                          # CTAS
    "CREATE TABLE t (a int); DROP TABLE victim",                           # two statements
    "CREATE TABLE t (a int) STORED AS SEQUENCEFILE",                       # format not modeled
    "CREATE TABLE t (a int) CLUSTERED BY (a) SORTED BY (a) INTO 2 BUCKETS",
    "CREATE TABLE t (a int) LOCATION 's3://b/t/' SOMETHING ELSE",
    "CREATE DATABASE db",
    "ALTER TABLE t ADD COLUMNS (x int)",
    "DROP TABLE",
    "DROP TABLE a b",
    "DROP TABLE a; DROP TABLE b",
])
def test_parse_unmodeled_statements_return_none(query):
    from ministack.services.athena import _parse_ddl

    assert _parse_ddl(query) is None


def test_parse_column_types_nested_and_precise():
    from ministack.services.athena import _parse_ddl

    ddl = _parse_ddl(
        "CREATE TABLE t (id int, tags array<string>, geo struct<lat:double,lng:double>, "
        "m map<string,int>, amount DECIMAL(10, 2))"
    )
    assert [c["Type"] for c in ddl.columns] == [
        "int", "array<string>", "struct<lat:double,lng:double>", "map<string,int>", "decimal(10, 2)",
    ]


def test_parse_literals_and_comments_are_not_structure():
    from ministack.services.athena import _parse_ddl

    ddl = _parse_ddl(
        "create external table t (a int COMMENT 'has, a comma and a )', b string) -- trailing (comment\n"
        "LOCATION 's3://b/it''s/'"
    )
    assert [c["Name"] for c in ddl.columns] == ["a", "b"]
    assert ddl.columns[0]["Comment"] == "has, a comma and a )"
    assert ddl.location == "s3://b/it's/"


def test_parse_drop_table():
    from ministack.services.athena import _DropTable, _parse_ddl

    assert _parse_ddl("DROP TABLE events") == _DropTable(None, "events")
    assert _parse_ddl("drop table if exists db.events;") == _DropTable("db", "events", if_exists=True)


def test_table_references_qualified_quoted_and_aliased():
    from ministack.services.athena import _table_references, _TableReference

    query = 'SELECT u.id FROM "awsdatacatalog"."acme"."users" u JOIN orders ON u.id = orders.uid'
    refs = _table_references(query)
    assert refs == [
        _TableReference("acme", "users", query.index('"awsdatacatalog"'), query.index(" u JOIN"), aliased=True,
                        catalog="awsdatacatalog"),
        _TableReference(None, "orders", query.index("orders ON"), query.index(" ON u.id"), aliased=False),
    ]


def test_table_references_skip_ctes_and_keep_clause_keywords_unaliased():
    from ministack.services.athena import _table_references

    refs = _table_references("WITH users AS (SELECT 1 AS id) SELECT * FROM users JOIN acme.events WHERE 1 = 1")
    assert [(r.database, r.table, r.aliased) for r in refs] == [("acme", "events", False)]
    assert _table_references("SELECT 1") == []
    assert _table_references("SELECT 1 FROM a.b.c.d") == []
    assert [r.aliased for r in _table_references('SELECT * FROM t AS "x" LIMIT 1')] == [True]


def _s3tables_writer(bucket_arn, key_id="test"):
    """A client-side DuckDB attached to the bucket's Iceberg REST catalog, the way
    Spark or PyIceberg would write an S3 table; skips when the extensions are unavailable."""
    from urllib.parse import urlparse

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    parsed = urlparse(endpoint)
    con = duckdb.connect()
    try:
        con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
        con.execute(f"CREATE SECRET s (TYPE S3, KEY_ID '{key_id}', SECRET 'test', "
                    f"ENDPOINT '{parsed.hostname}:{parsed.port or 4566}', URL_STYLE 'path', "
                    f"USE_SSL false, REGION 'us-east-1')")
        # SigV4 clients carry their account in the Credential scope; DuckDB only signs AWS hosts.
        credential = f"AWS4-HMAC-SHA256 Credential={key_id}/19700101/us-east-1/s3tables/aws4_request"
        con.execute(f"CREATE SECRET h (TYPE HTTP, SCOPE '{endpoint}/iceberg', "
                    f"EXTRA_HTTP_HEADERS MAP {{'Authorization': '{credential}'}})")
        con.execute(f"ATTACH '{bucket_arn}' AS cat (TYPE ICEBERG, ENDPOINT '{endpoint}/iceberg', "
                    f"AUTHORIZATION_TYPE 'none')")
    except Exception as exc:  # pragma: no cover - environment dependent
        con.close()
        pytest.skip(f"DuckDB Iceberg REST writes unavailable: {exc}")
    return con


def _athena_rows(athena, query, context, output=None):
    extra = {"ResultConfiguration": {"OutputLocation": output}} if output else {}
    query_id = athena.start_query_execution(QueryString=query, QueryExecutionContext=context,
                                            **extra)["QueryExecutionId"]
    for _ in range(100):
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        if execution["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.2)
    if execution["Status"]["State"] != "SUCCEEDED":
        return execution["Status"]["State"], None
    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    return "SUCCEEDED", [[d.get("VarCharValue") for d in r["Data"]] for r in rows[1:]]


def test_athena_queries_s3_tables_through_s3tablescatalog(athena):
    """Both documented forms: "s3tablescatalog/<bucket>".ns.table, and the context
    Catalog s3tablescatalog/<bucket> with an unqualified table. Each bucket reads its own rows."""
    s3t = boto3.client("s3tables", endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
                       region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
    suffix = _uuid_mod.uuid4().hex[:8]
    names = {}
    for label, rows in (("one", "(1, 'apple'), (2, 'pear')"), ("two", "(7, 'plum')")):
        name = f"ath-s3t-{label}-{suffix}"
        arn = s3t.create_table_bucket(name=name)["arn"]
        s3t.create_namespace(tableBucketARN=arn, namespace=["sales"])
        con = _s3tables_writer(arn)
        con.execute("CREATE TABLE cat.sales.orders (id INTEGER, item VARCHAR)")
        con.execute(f"INSERT INTO cat.sales.orders VALUES {rows}")
        con.close()
        names[label] = name

    state, rows = _athena_rows(
        athena, f'SELECT id, item FROM "s3tablescatalog/{names["one"]}"."sales"."orders" ORDER BY id',
        {"Database": "default"})
    assert (state, rows) == ("SUCCEEDED", [["1", "apple"], ["2", "pear"]])

    state, rows = _athena_rows(
        athena, "SELECT o.id, o.item FROM orders o",
        {"Catalog": f"s3tablescatalog/{names['two']}", "Database": "sales"})
    assert (state, rows) == ("SUCCEEDED", [["7", "plum"]])

    state, _ = _athena_rows(
        athena, f'SELECT * FROM "s3tablescatalog/{names["one"]}"."sales"."missing"', {"Database": "default"})
    assert state == "FAILED"


def test_athena_reads_glue_csv_by_position_from_every_visible_object(athena, glue, s3):
    """S3 objects are read whatever their names, without S3_PERSIST; hidden "_"/"." files and
    folders are skipped; CSV fields map to the Glue columns by position, a value that does not
    cast reads as NULL and a missing trailing field as NULL."""
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket, database = f"ath-csv-{suffix}", f"csv_{suffix}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="t/part-00000", Body=b"1,alice\n2,bob\n")
    s3.put_object(Bucket=bucket, Key="t/more/part-00001.csv", Body=b"x,carol\n4\n")
    s3.put_object(Bucket=bucket, Key="t/_SUCCESS", Body=b"")
    s3.put_object(Bucket=bucket, Key="t/.part-00000.crc", Body=b"garbage,garbage\n")
    s3.put_object(Bucket=bucket, Key="t/_temporary/0/part-9", Body=b"9,ghost\n")
    s3.put_object(Bucket=bucket, Key="other/part-00000", Body=b"8,elsewhere\n")
    glue.create_database(DatabaseInput={"Name": database})
    glue.create_table(DatabaseName=database, TableInput={
        "Name": "people",
        "StorageDescriptor": {
            "Columns": [{"Name": "id", "Type": "int"}, {"Name": "name", "Type": "string"}],
            "Location": f"s3://{bucket}/t/",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                          "Parameters": {"field.delim": ","}},
        },
        "Parameters": {"classification": "csv"},
    })
    state, rows = _athena_rows(athena, "SELECT id, name FROM people ORDER BY name NULLS LAST",
                               {"Database": database})
    assert (state, rows) == ("SUCCEEDED", [["1", "alice"], ["2", "bob"], [None, "carol"], ["4", None]])


def test_athena_csv_skip_header_line_count(athena, glue, s3):
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket, database = f"ath-hdr-{suffix}", f"hdr_{suffix}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="t/a.csv", Body=b"id,name\n1,alice\n")
    glue.create_database(DatabaseInput={"Name": database})
    for name, params in (("skipped", {"skip.header.line.count": "1"}), ("kept", {})):
        glue.create_table(DatabaseName=database, TableInput={
            "Name": name,
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "int"}, {"Name": "name", "Type": "string"}],
                "Location": f"s3://{bucket}/t/",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                              "Parameters": {"field.delim": ","}},
            },
            "Parameters": {"classification": "csv", **params},
        })
    assert _athena_rows(athena, "SELECT id, name FROM skipped", {"Database": database}) == (
        "SUCCEEDED", [["1", "alice"]])
    assert _athena_rows(athena, "SELECT id, name FROM kept ORDER BY name", {"Database": database}) == (
        "SUCCEEDED", [["1", "alice"], [None, "name"]])


def test_athena_s3_tables_query_in_a_non_default_account():
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    kw = dict(endpoint_url=endpoint, region_name="us-east-1",
              aws_access_key_id="111111111111", aws_secret_access_key="test")
    s3t, athena = boto3.client("s3tables", **kw), boto3.client("athena", **kw)
    name = f"ath-s3t-acct-{_uuid_mod.uuid4().hex[:8]}"
    boto3.client("s3", **kw).create_bucket(Bucket=f"{name}-results")
    arn = s3t.create_table_bucket(name=name)["arn"]
    assert ":111111111111:" in arn
    s3t.create_namespace(tableBucketARN=arn, namespace=["sales"])
    con = _s3tables_writer(arn, key_id="111111111111")
    con.execute("CREATE TABLE cat.sales.orders (id INTEGER)")
    con.execute("INSERT INTO cat.sales.orders VALUES (5)")
    con.close()
    assert _athena_rows(athena, "SELECT id FROM orders",
                        {"Catalog": f"s3tablescatalog/{name}", "Database": "sales"},
                        output=f"s3://{name}-results/") == ("SUCCEEDED", [["5"]])



def test_athena_start_query_without_output_location_is_refused():
    athena = _client("us-east-1")
    workgroup = f"ath-noout-{_uuid_mod.uuid4().hex[:8]}"
    athena.create_work_group(Name=workgroup)
    with pytest.raises(ClientError) as exc:
        athena.start_query_execution(QueryString="SELECT 1", WorkGroup=workgroup)
    err = exc.value.response
    assert err["Error"]["Code"] == "InvalidRequestException"
    assert err["Error"]["Message"] == ("No output location provided. An output location is required either "
                                       "through the Workgroup result configuration setting or as an API input.")
    assert err["ResponseMetadata"]["HTTPStatusCode"] == 400
    query_id = athena.start_query_execution(
        QueryString="SELECT 1", WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": "s3://athena-results/"})["QueryExecutionId"]
    assert query_id


def test_athena_reads_avro_tables(athena, glue, s3, tmp_path):
    path = tmp_path / "rows.avro"
    con = duckdb.connect()
    try:
        con.execute("INSTALL avro; LOAD avro;")
        con.execute(f"COPY (SELECT 1::INT AS id, 'a' AS name UNION ALL SELECT 2, 'b') TO '{path}' (FORMAT avro)")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"DuckDB Avro unavailable: {exc}")
    finally:
        con.close()
    suffix = _uuid_mod.uuid4().hex[:8]
    bucket, database = f"ath-avro-{suffix}", f"avro_{suffix}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="t/part-00000", Body=path.read_bytes())
    glue.create_database(DatabaseInput={"Name": database})
    glue.create_table(DatabaseName=database, TableInput={
        "Name": "rows",
        "StorageDescriptor": {
            "Columns": [{"Name": "id", "Type": "int"}, {"Name": "name", "Type": "string"}],
            "Location": f"s3://{bucket}/t/",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.avro.AvroSerDe"},
        },
        "Parameters": {"classification": "avro"},
    })
    assert _athena_rows(athena, "SELECT id, name FROM rows ORDER BY id", {"Database": database}) == (
        "SUCCEEDED", [["1", "a"], ["2", "b"]])
