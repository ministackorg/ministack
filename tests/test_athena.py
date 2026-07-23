import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


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
        service.restore_state(payload)
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
        service.restore_state(payload)

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

    s3mod._persist_object(
        bucket_name,
        "tables/users/data.csv",
        b"id,name\n1,alice\n2,bob"
    )

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
