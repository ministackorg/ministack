import asyncio
import importlib
import io
import json
import os
import threading
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

import ministack.core.responses as _responses
from ministack.core.router import detect_service


def test_glue_catalog(glue):
    glue.create_database(DatabaseInput={"Name": "test_db", "Description": "Test database"})
    glue.create_table(
        DatabaseName="test_db",
        TableInput={
            "Name": "test_table",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "int"},
                    {"Name": "name", "Type": "string"},
                ],
                "Location": "s3://my-bucket/data/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
            },
            "TableType": "EXTERNAL_TABLE",
        },
    )
    resp = glue.get_table(DatabaseName="test_db", Name="test_table")
    assert resp["Table"]["Name"] == "test_table"

def test_glue_list(glue):
    dbs = glue.get_databases()
    assert any(d["Name"] == "test_db" for d in dbs["DatabaseList"])
    tables = glue.get_tables(DatabaseName="test_db")
    assert any(t["Name"] == "test_table" for t in tables["TableList"])

def test_glue_job(glue):
    glue.create_job(
        Name="test-job",
        Role="arn:aws:iam::000000000000:role/GlueRole",
        Command={"Name": "glueetl", "ScriptLocation": "s3://my-bucket/scripts/etl.py"},
        GlueVersion="3.0",
    )
    resp = glue.start_job_run(JobName="test-job")
    assert "JobRunId" in resp
    runs = glue.get_job_runs(JobName="test-job")
    assert len(runs["JobRuns"]) == 1

def test_glue_crawler(glue):
    glue.create_crawler(
        Name="test-crawler",
        Role="arn:aws:iam::000000000000:role/GlueRole",
        DatabaseName="test_db",
        Targets={"S3Targets": [{"Path": "s3://my-bucket/data/"}]},
    )
    resp = glue.get_crawler(Name="test-crawler")
    assert resp["Crawler"]["Name"] == "test-crawler"
    glue.start_crawler(Name="test-crawler")

def test_glue_database_location_uri(glue):
    glue.create_database(DatabaseInput={"Name": "db_no_location"})
    resp = glue.get_database(Name="db_no_location")
    assert resp["Database"].get("LocationUri") is None

    glue.create_database(DatabaseInput={"Name": "db_with_location", "LocationUri": "s3://my-bucket/warehouse/"})
    resp = glue.get_database(Name="db_with_location")
    assert resp["Database"]["LocationUri"] == "s3://my-bucket/warehouse/"

def test_glue_database_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_db_v2", "Description": "v2 DB"})
    resp = glue.get_database(Name="glue_db_v2")
    assert resp["Database"]["Name"] == "glue_db_v2"
    assert resp["Database"]["Description"] == "v2 DB"

    glue.update_database(
        Name="glue_db_v2",
        DatabaseInput={"Name": "glue_db_v2", "Description": "updated"},
    )
    resp2 = glue.get_database(Name="glue_db_v2")
    assert resp2["Database"]["Description"] == "updated"

    glue.delete_database(Name="glue_db_v2")
    with pytest.raises(ClientError) as exc:
        glue.get_database(Name="glue_db_v2")
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

def test_glue_table_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_tbl_v2db"})
    glue.create_table(
        DatabaseName="glue_tbl_v2db",
        TableInput={
            "Name": "tbl_v2",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "id", "Type": "int"},
                    {"Name": "name", "Type": "string"},
                ],
                "Location": "s3://bucket/tbl_v2/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
            },
            "TableType": "EXTERNAL_TABLE",
        },
    )
    resp = glue.get_table(DatabaseName="glue_tbl_v2db", Name="tbl_v2")
    assert resp["Table"]["Name"] == "tbl_v2"
    assert len(resp["Table"]["StorageDescriptor"]["Columns"]) == 2

    glue.update_table(
        DatabaseName="glue_tbl_v2db",
        TableInput={"Name": "tbl_v2", "Description": "updated table"},
    )
    resp2 = glue.get_table(DatabaseName="glue_tbl_v2db", Name="tbl_v2")
    assert resp2["Table"]["Description"] == "updated table"

    glue.delete_table(DatabaseName="glue_tbl_v2db", Name="tbl_v2")
    with pytest.raises(ClientError) as exc:
        glue.get_table(DatabaseName="glue_tbl_v2db", Name="tbl_v2")
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

def test_glue_view_original_text_roundtrip(glue):
    glue.create_database(DatabaseInput={"Name": "glue_view_db"})
    original = "/* Presto View: eyJjYXRhbG9nIjoiaWNlYmVyZyJ9 */"
    expanded = "/* Presto View */"
    glue.create_table(
        DatabaseName="glue_view_db",
        TableInput={
            "Name": "vw_x",
            "TableType": "VIRTUAL_VIEW",
            "ViewOriginalText": original,
            "ViewExpandedText": expanded,
        },
    )
    resp = glue.get_table(DatabaseName="glue_view_db", Name="vw_x")
    assert resp["Table"]["ViewOriginalText"] == original
    assert resp["Table"]["ViewExpandedText"] == expanded

    glue.update_table(
        DatabaseName="glue_view_db",
        TableInput={
            "Name": "vw_x",
            "TableType": "VIRTUAL_VIEW",
            "ViewOriginalText": original + " v2",
            "ViewExpandedText": expanded + " v2",
        },
    )
    resp2 = glue.get_table(DatabaseName="glue_view_db", Name="vw_x")
    assert resp2["Table"]["ViewOriginalText"] == original + " v2"
    assert resp2["Table"]["ViewExpandedText"] == expanded + " v2"

def test_glue_list_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_lst_v2db"})
    glue.create_table(
        DatabaseName="glue_lst_v2db",
        TableInput={
            "Name": "lt_a",
            "StorageDescriptor": {
                "Columns": [{"Name": "c", "Type": "string"}],
                "Location": "s3://b/lt_a/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
        },
    )
    glue.create_table(
        DatabaseName="glue_lst_v2db",
        TableInput={
            "Name": "lt_b",
            "StorageDescriptor": {
                "Columns": [{"Name": "c", "Type": "string"}],
                "Location": "s3://b/lt_b/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
        },
    )
    dbs = glue.get_databases()
    assert any(d["Name"] == "glue_lst_v2db" for d in dbs["DatabaseList"])
    tables = glue.get_tables(DatabaseName="glue_lst_v2db")
    names = [t["Name"] for t in tables["TableList"]]
    assert "lt_a" in names
    assert "lt_b" in names

def test_glue_job_v2(glue):
    glue.create_job(
        Name="glue-job-v2",
        Role="arn:aws:iam::000000000000:role/R",
        Command={"Name": "glueetl", "ScriptLocation": "s3://b/s.py"},
        GlueVersion="3.0",
    )
    job = glue.get_job(JobName="glue-job-v2")["Job"]
    assert job["Name"] == "glue-job-v2"

    run_resp = glue.start_job_run(JobName="glue-job-v2", Arguments={"--key": "val"})
    run_id = run_resp["JobRunId"]
    assert run_id

    run = glue.get_job_run(JobName="glue-job-v2", RunId=run_id)["JobRun"]
    assert run["Id"] == run_id
    assert run["JobName"] == "glue-job-v2"

    runs = glue.get_job_runs(JobName="glue-job-v2")["JobRuns"]
    assert any(r["Id"] == run_id for r in runs)

def test_glue_crawler_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_cr_v2db"})
    glue.create_crawler(
        Name="glue-cr-v2",
        Role="arn:aws:iam::000000000000:role/R",
        DatabaseName="glue_cr_v2db",
        Targets={"S3Targets": [{"Path": "s3://b/data/"}]},
    )
    cr = glue.get_crawler(Name="glue-cr-v2")["Crawler"]
    assert cr["Name"] == "glue-cr-v2"
    assert cr["State"] == "READY"

    glue.start_crawler(Name="glue-cr-v2")
    cr2 = glue.get_crawler(Name="glue-cr-v2")["Crawler"]
    assert cr2["State"] == "RUNNING"

def test_glue_tags_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_tag_v2db"})
    arn = "arn:aws:glue:us-east-1:000000000000:database/glue_tag_v2db"
    glue.tag_resource(ResourceArn=arn, TagsToAdd={"env": "test", "team": "data"})
    resp = glue.get_tags(ResourceArn=arn)
    assert resp["Tags"]["env"] == "test"
    assert resp["Tags"]["team"] == "data"

    glue.untag_resource(ResourceArn=arn, TagsToRemove=["team"])
    resp2 = glue.get_tags(ResourceArn=arn)
    assert resp2["Tags"] == {"env": "test"}


def _glue_json(response):
    status, _headers, body = response
    return status, json.loads(body)


@pytest.mark.parametrize(
    "resource",
    [
        "catalog",
        "catalog/s3tablescatalog",
        "blueprint/glue-tag-parser-blueprint",
        "crawler/glue-tag-parser-crawler",
        "customEntityType/glue-tag-parser-custom-entity",
        "dataQualityRuleset/glue-tag-parser-ruleset",
        "database/glue_tag_parser_db",
        "database/glue_tag_parser_catalog/glue_tag_parser_db",
        "table/glue_tag_parser_db/glue_tag_parser_table",
        "table/glue_tag_parser_catalog/glue_tag_parser_db/glue_tag_parser_table",
        "job/glue-tag-parser-job",
        "connection/glue-tag-parser-connection",
        "connectionType:glue-tag-parser-connection-type",
        "devEndpoint/glue-tag-parser-endpoint",
        "integration:glue-tag-parser-integration",
        "integrationresourceproperty/glue_tag_parser_resource_type/glue_tag_parser_resource",
        "mlTransform/glue-tag-parser-transform",
        "registry/glue-tag-parser-registry",
        "schema/glue-tag-parser-registry/glue-tag-parser-schema",
        "security-configuration/glue-tag-parser-security",
        "securityConfiguration/glue-tag-parser-security",
        "session/glue-tag-parser-session",
        "trigger/glue-tag-parser-trigger",
        "usageProfile/glue-tag-parser-profile",
        "userDefinedFunction/glue_tag_parser_db/glue_tag_parser_function",
        "workflow/glue-tag-parser-workflow",
    ],
)
def test_glue_tag_resource_accepts_supported_arn_shapes(resource):
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import glue as _glue

    original_account = get_account_id()
    original_region = get_region()
    arn = f"arn:aws:glue:us-east-1:000000000000:{resource}"

    try:
        _glue._tags.clear()
        set_request_account_id("000000000000")
        set_request_region("us-east-1")

        status, body = _glue_json(
            _glue._tag_resource({"ResourceArn": arn, "TagsToAdd": {"env": "test"}})
        )
        assert status == 200
        assert body == {}

        status, body = _glue_json(_glue._get_tags({"ResourceArn": arn}))
        assert status == 200
        assert body["Tags"] == {"env": "test"}
    finally:
        _glue._tags.clear()
        set_request_account_id(original_account)
        set_request_region(original_region)


@pytest.mark.parametrize(
    "arn",
    [
        "not-an-arn",
        "arn:notaws:glue:us-east-1:000000000000:database/glue_tag_parser_db",
        "arn:aws:sns:us-east-1:000000000000:topic/glue-tag-parser",
        "arn:aws:glue:us-west-2:000000000000:database/glue_tag_parser_db",
        "arn:aws:glue:us-east-1:111111111111:database/glue_tag_parser_db",
        "arn:aws:glue:us-east-1:000000000000:",
        "arn:aws:glue:us-east-1:000000000000:database/",
        "arn:aws:glue:us-east-1:000000000000:catalog/",
        "arn:aws:glue:us-east-1:000000000000:schema/glue_tag_parser_schema",
        "arn:aws:glue:us-east-1:000000000000:database//glue_tag_parser_db",
        "arn:aws:glue:us-east-1:000000000000:database:glue_tag_parser_db",
        "arn:aws:glue:us-east-1:000000000000:notAResource/glue_tag_parser_resource",
    ],
)
def test_glue_tag_apis_reject_invalid_resource_arns_before_touching_tags(arn):
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import glue as _glue

    original_account = get_account_id()
    original_region = get_region()
    valid_arn = "arn:aws:glue:us-east-1:000000000000:database/glue_tag_parser_db"

    try:
        _glue._tags.clear()
        set_request_account_id("000000000000")
        set_request_region("us-east-1")
        _glue._tag_resource({"ResourceArn": valid_arn, "TagsToAdd": {"env": "test"}})
        before = dict(_glue._tags.items())

        calls = [
            _glue._tag_resource({"ResourceArn": arn, "TagsToAdd": {"bad": "tag"}}),
            _glue._untag_resource({"ResourceArn": arn, "TagsToRemove": ["env"]}),
            _glue._get_tags({"ResourceArn": arn}),
        ]
        for response in calls:
            status, body = _glue_json(response)
            assert status == 400
            assert body["__type"] == "InvalidInputException"
            assert "Invalid Glue resource ARN" in body["message"]
            assert dict(_glue._tags.items()) == before
    finally:
        _glue._tags.clear()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_glue_create_database_persists_tags(glue):
    """CreateDatabase top-level Tags must be stored (issue #1130 — real AWS shape)."""
    glue.create_database(
        DatabaseInput={"Name": "db_with_tags"},
        Tags={"env": "prod", "owner": "data-platform"},
    )
    arn = "arn:aws:glue:us-east-1:000000000000:database/db_with_tags"
    tags = glue.get_tags(ResourceArn=arn)["Tags"]
    assert tags == {"env": "prod", "owner": "data-platform"}

def test_glue_partition_v2(glue):
    glue.create_database(DatabaseInput={"Name": "glue_part_v2db"})
    glue.create_table(
        DatabaseName="glue_part_v2db",
        TableInput={
            "Name": "ptbl_v2",
            "StorageDescriptor": {
                "Columns": [{"Name": "data", "Type": "string"}],
                "Location": "s3://b/pt/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
            "PartitionKeys": [
                {"Name": "year", "Type": "string"},
                {"Name": "month", "Type": "string"},
            ],
        },
    )
    glue.create_partition(
        DatabaseName="glue_part_v2db",
        TableName="ptbl_v2",
        PartitionInput={
            "Values": ["2024", "01"],
            "StorageDescriptor": {
                "Columns": [{"Name": "data", "Type": "string"}],
                "Location": "s3://b/pt/year=2024/month=01/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
        },
    )
    glue.create_partition(
        DatabaseName="glue_part_v2db",
        TableName="ptbl_v2",
        PartitionInput={
            "Values": ["2024", "02"],
            "StorageDescriptor": {
                "Columns": [{"Name": "data", "Type": "string"}],
                "Location": "s3://b/pt/year=2024/month=02/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
        },
    )
    resp = glue.get_partition(
        DatabaseName="glue_part_v2db",
        TableName="ptbl_v2",
        PartitionValues=["2024", "01"],
    )
    assert resp["Partition"]["Values"] == ["2024", "01"]

    parts = glue.get_partitions(DatabaseName="glue_part_v2db", TableName="ptbl_v2")
    assert len(parts["Partitions"]) == 2

def test_glue_connection_v2(glue):
    glue.create_connection(
        ConnectionInput={
            "Name": "glue-conn-v2",
            "ConnectionType": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": "jdbc:postgresql://host/db",
                "USERNAME": "user",
                "PASSWORD": "pass",
            },
        }
    )
    resp = glue.get_connection(Name="glue-conn-v2")
    assert resp["Connection"]["Name"] == "glue-conn-v2"
    assert resp["Connection"]["ConnectionType"] == "JDBC"

    conns = glue.get_connections()
    assert any(c["Name"] == "glue-conn-v2" for c in conns["ConnectionList"])

    glue.delete_connection(ConnectionName="glue-conn-v2")
    with pytest.raises(ClientError) as exc:
        glue.get_connection(Name="glue-conn-v2")
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

def test_glue_trigger(glue):
    glue.create_trigger(Name="test-trig", Type="ON_DEMAND", Actions=[{"JobName": "nonexistent-job"}])
    resp = glue.get_trigger(Name="test-trig")
    assert resp["Trigger"]["Name"] == "test-trig"
    assert resp["Trigger"]["State"] == "CREATED"
    glue.start_trigger(Name="test-trig")
    resp2 = glue.get_trigger(Name="test-trig")
    assert resp2["Trigger"]["State"] == "ACTIVATED"
    glue.stop_trigger(Name="test-trig")
    resp3 = glue.get_trigger(Name="test-trig")
    assert resp3["Trigger"]["State"] == "DEACTIVATED"
    glue.delete_trigger(Name="test-trig")

def test_glue_workflow(glue):
    glue.create_workflow(Name="test-wf", Description="Test workflow")
    resp = glue.get_workflow(Name="test-wf")
    assert resp["Workflow"]["Name"] == "test-wf"
    run = glue.start_workflow_run(Name="test-wf")
    assert "RunId" in run
    glue.delete_workflow(Name="test-wf")

def test_glue_partition_crud(glue):
    """CreatePartition / GetPartition / GetPartitions / DeletePartition."""
    glue.create_database(DatabaseInput={"Name": "qa-glue-partdb"})
    glue.create_table(
        DatabaseName="qa-glue-partdb",
        TableInput={
            "Name": "qa-glue-parttbl",
            "StorageDescriptor": {
                "Columns": [],
                "Location": "s3://bucket/key",
                "InputFormat": "",
                "OutputFormat": "",
                "SerdeInfo": {},
            },
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        },
    )
    glue.create_partition(
        DatabaseName="qa-glue-partdb",
        TableName="qa-glue-parttbl",
        PartitionInput={
            "Values": ["2024-01-01"],
            "StorageDescriptor": {
                "Columns": [],
                "Location": "s3://bucket/key/dt=2024-01-01",
                "InputFormat": "",
                "OutputFormat": "",
                "SerdeInfo": {},
            },
        },
    )
    part = glue.get_partition(
        DatabaseName="qa-glue-partdb",
        TableName="qa-glue-parttbl",
        PartitionValues=["2024-01-01"],
    )["Partition"]
    assert part["Values"] == ["2024-01-01"]
    parts = glue.get_partitions(DatabaseName="qa-glue-partdb", TableName="qa-glue-parttbl")["Partitions"]
    assert len(parts) == 1
    glue.delete_partition(
        DatabaseName="qa-glue-partdb",
        TableName="qa-glue-parttbl",
        PartitionValues=["2024-01-01"],
    )
    parts2 = glue.get_partitions(DatabaseName="qa-glue-partdb", TableName="qa-glue-parttbl")["Partitions"]
    assert len(parts2) == 0

def test_glue_duplicate_partition_error(glue):
    """CreatePartition with duplicate values raises AlreadyExistsException."""
    glue.create_database(DatabaseInput={"Name": "qa-glue-duppartdb"})
    glue.create_table(
        DatabaseName="qa-glue-duppartdb",
        TableInput={
            "Name": "qa-glue-dupparttbl",
            "StorageDescriptor": {
                "Columns": [],
                "Location": "s3://b/k",
                "InputFormat": "",
                "OutputFormat": "",
                "SerdeInfo": {},
            },
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        },
    )
    part_input = {
        "Values": ["2024-01-01"],
        "StorageDescriptor": {
            "Columns": [],
            "Location": "s3://b/k/dt=2024-01-01",
            "InputFormat": "",
            "OutputFormat": "",
            "SerdeInfo": {},
        },
    }
    glue.create_partition(
        DatabaseName="qa-glue-duppartdb",
        TableName="qa-glue-dupparttbl",
        PartitionInput=part_input,
    )
    with pytest.raises(ClientError) as exc:
        glue.create_partition(
            DatabaseName="qa-glue-duppartdb",
            TableName="qa-glue-dupparttbl",
            PartitionInput=part_input,
        )
    assert exc.value.response["Error"]["Code"] == "AlreadyExistsException"


# ---------------------------------------------------------------------------
# BatchDeleteTable
# ---------------------------------------------------------------------------

def test_glue_batch_delete_table(glue):
    db = "qa-bdt-db"
    glue.create_database(DatabaseInput={"Name": db})
    for t in ("tbl_a", "tbl_b", "tbl_c"):
        glue.create_table(
            DatabaseName=db,
            TableInput={
                "Name": t,
                "StorageDescriptor": {
                    "Columns": [{"Name": "c", "Type": "string"}],
                    "Location": f"s3://b/{t}/",
                    "InputFormat": "TIF",
                    "OutputFormat": "TOF",
                    "SerdeInfo": {"SerializationLibrary": "SL"},
                },
            },
        )
    resp = glue.batch_delete_table(DatabaseName=db, TablesToDelete=["tbl_a", "tbl_b", "no_such"])
    errors = resp.get("Errors", [])
    assert len(errors) == 1
    assert errors[0]["TableName"] == "no_such"
    tables = glue.get_tables(DatabaseName=db)
    names = [t["Name"] for t in tables["TableList"]]
    assert "tbl_a" not in names
    assert "tbl_b" not in names
    assert "tbl_c" in names
    # cleanup
    glue.delete_table(DatabaseName=db, Name="tbl_c")
    glue.delete_database(Name=db)


# ---------------------------------------------------------------------------
# BatchGetPartition
# ---------------------------------------------------------------------------

def test_glue_batch_get_partition(glue):
    db = "qa-bgp-db"
    tbl = "qa-bgp-tbl"
    glue.create_database(DatabaseInput={"Name": db})
    glue.create_table(
        DatabaseName=db,
        TableInput={
            "Name": tbl,
            "StorageDescriptor": {
                "Columns": [],
                "Location": "s3://b/k",
                "InputFormat": "",
                "OutputFormat": "",
                "SerdeInfo": {},
            },
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        },
    )
    for val in ("2024-01", "2024-02"):
        glue.create_partition(
            DatabaseName=db,
            TableName=tbl,
            PartitionInput={
                "Values": [val],
                "StorageDescriptor": {
                    "Columns": [],
                    "Location": f"s3://b/k/dt={val}",
                    "InputFormat": "",
                    "OutputFormat": "",
                    "SerdeInfo": {},
                },
            },
        )
    resp = glue.batch_get_partition(
        DatabaseName=db,
        TableName=tbl,
        PartitionsToGet=[
            {"Values": ["2024-01"]},
            {"Values": ["2024-02"]},
            {"Values": ["no-such"]},
        ],
    )
    assert len(resp["Partitions"]) == 2
    assert len(resp["UnprocessedKeys"]) == 1
    assert resp["UnprocessedKeys"][0]["Values"] == ["no-such"]
    # cleanup
    glue.delete_table(DatabaseName=db, Name=tbl)
    glue.delete_database(Name=db)


# ---------------------------------------------------------------------------
# BatchCreatePartition
# ---------------------------------------------------------------------------

def test_glue_batch_create_partition(glue):
    db = "qa-bcp-db"
    tbl = "qa-bcp-tbl"
    glue.create_database(DatabaseInput={"Name": db})
    glue.create_table(
        DatabaseName=db,
        TableInput={
            "Name": tbl,
            "StorageDescriptor": {
                "Columns": [],
                "Location": "s3://b/k",
                "InputFormat": "",
                "OutputFormat": "",
                "SerdeInfo": {},
            },
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        },
    )
    resp = glue.batch_create_partition(
        DatabaseName=db,
        TableName=tbl,
        PartitionInputList=[
            {
                "Values": ["2024-03"],
                "StorageDescriptor": {
                    "Columns": [],
                    "Location": "s3://b/k/dt=2024-03",
                    "InputFormat": "",
                    "OutputFormat": "",
                    "SerdeInfo": {},
                },
            },
            {
                "Values": ["2024-04"],
                "StorageDescriptor": {
                    "Columns": [],
                    "Location": "s3://b/k/dt=2024-04",
                    "InputFormat": "",
                    "OutputFormat": "",
                    "SerdeInfo": {},
                },
            },
        ],
    )
    assert resp.get("Errors", []) == []
    parts = glue.get_partitions(DatabaseName=db, TableName=tbl)["Partitions"]
    assert len(parts) == 2
    # duplicate insert returns error
    resp2 = glue.batch_create_partition(
        DatabaseName=db,
        TableName=tbl,
        PartitionInputList=[
            {
                "Values": ["2024-03"],
                "StorageDescriptor": {
                    "Columns": [],
                    "Location": "s3://b/k/dt=2024-03",
                    "InputFormat": "",
                    "OutputFormat": "",
                    "SerdeInfo": {},
                },
            },
        ],
    )
    assert len(resp2["Errors"]) == 1
    assert resp2["Errors"][0]["ErrorDetail"]["ErrorCode"] == "AlreadyExistsException"
    # cleanup
    glue.delete_table(DatabaseName=db, Name=tbl)
    glue.delete_database(Name=db)


# ---------------------------------------------------------------------------
# BatchUpdatePartition
# ---------------------------------------------------------------------------

def test_glue_batch_update_partition(glue):
    db = "qa-bup-db"
    tbl = "qa-bup-tbl"
    glue.create_database(DatabaseInput={"Name": db})
    glue.create_table(
        DatabaseName=db,
        TableInput={
            "Name": tbl,
            "StorageDescriptor": {
                "Columns": [],
                "Location": "s3://b/k",
                "InputFormat": "",
                "OutputFormat": "",
                "SerdeInfo": {},
            },
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        },
    )
    glue.batch_create_partition(
        DatabaseName=db,
        TableName=tbl,
        PartitionInputList=[
            {
                "Values": ["2024-05"],
                "StorageDescriptor": {
                    "Columns": [],
                    "Location": "s3://b/k/dt=2024-05",
                    "InputFormat": "",
                    "OutputFormat": "",
                    "SerdeInfo": {},
                },
            },
        ],
    )
    before = glue.get_partition(
        DatabaseName=db,
        TableName=tbl,
        PartitionValues=["2024-05"],
    )["Partition"]
    creation_time = before["CreationTime"]
    resp = glue.batch_update_partition(
        DatabaseName=db,
        TableName=tbl,
        Entries=[
            {
                "PartitionValueList": ["2024-05"],
                "PartitionInput": {
                    "Values": ["2024-05"],
                    "StorageDescriptor": {
                        "Columns": [],
                        "Location": "s3://b/k/dt=2024-05-updated",
                        "InputFormat": "",
                        "OutputFormat": "",
                        "SerdeInfo": {},
                    },
                },
            },
        ],
    )
    assert resp.get("Errors", []) == []
    part = glue.get_partition(
        DatabaseName=db,
        TableName=tbl,
        PartitionValues=["2024-05"],
    )["Partition"]
    assert part["StorageDescriptor"]["Location"] == "s3://b/k/dt=2024-05-updated"
    # CreationTime is preserved across an update (LastAccessTime is refreshed)
    assert part["CreationTime"] == creation_time
    # updating a partition that does not exist returns an error entry
    resp2 = glue.batch_update_partition(
        DatabaseName=db,
        TableName=tbl,
        Entries=[
            {
                "PartitionValueList": ["no-such"],
                "PartitionInput": {
                    "Values": ["no-such"],
                    "StorageDescriptor": {
                        "Columns": [],
                        "Location": "s3://b/k/dt=no-such",
                        "InputFormat": "",
                        "OutputFormat": "",
                        "SerdeInfo": {},
                    },
                },
            },
        ],
    )
    assert len(resp2["Errors"]) == 1
    assert resp2["Errors"][0]["PartitionValueList"] == ["no-such"]
    assert resp2["Errors"][0]["ErrorDetail"]["ErrorCode"] == "EntityNotFoundException"
    # updating against a nonexistent table fails at the request level
    with pytest.raises(ClientError) as exc:
        glue.batch_update_partition(
            DatabaseName=db,
            TableName="no-such-tbl",
            Entries=[{"PartitionValueList": ["x"], "PartitionInput": {"Values": ["x"]}}],
        )
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
    # cleanup
    glue.delete_table(DatabaseName=db, Name=tbl)
    glue.delete_database(Name=db)


# ---------------------------------------------------------------------------
# GetCrawlerMetrics
# ---------------------------------------------------------------------------

def test_glue_get_crawler_metrics(glue):
    name = "qa-metrics-cr"
    glue.create_crawler(
        Name=name,
        Role="arn:aws:iam::000000000000:role/R",
        DatabaseName="test_db",
        Targets={"S3Targets": [{"Path": "s3://b/d/"}]},
    )
    resp = glue.get_crawler_metrics(CrawlerNameList=[name])
    assert len(resp["CrawlerMetricsList"]) == 1
    m = resp["CrawlerMetricsList"][0]
    assert m["CrawlerName"] == name
    assert "TablesCreated" in m
    # cleanup
    glue.delete_crawler(Name=name)


# ---------------------------------------------------------------------------
# UpdateCrawler
# ---------------------------------------------------------------------------

def test_glue_update_crawler(glue):
    name = "qa-upd-cr"
    glue.create_crawler(
        Name=name,
        Role="arn:aws:iam::000000000000:role/R",
        DatabaseName="test_db",
        Targets={"S3Targets": [{"Path": "s3://b/d/"}]},
    )
    glue.update_crawler(Name=name, Description="updated desc", Role="arn:aws:iam::000000000000:role/New")
    cr = glue.get_crawler(Name=name)["Crawler"]
    assert cr["Description"] == "updated desc"
    assert cr["Role"] == "arn:aws:iam::000000000000:role/New"
    assert cr["Version"] == 2
    # cleanup
    glue.delete_crawler(Name=name)


# ---------------------------------------------------------------------------
# StopCrawler
# ---------------------------------------------------------------------------

def test_glue_stop_crawler(glue):
    name = "qa-stop-cr"
    glue.create_crawler(
        Name=name,
        Role="arn:aws:iam::000000000000:role/R",
        DatabaseName="test_db",
        Targets={"S3Targets": [{"Path": "s3://b/d/"}]},
    )
    glue.start_crawler(Name=name)
    cr = glue.get_crawler(Name=name)["Crawler"]
    assert cr["State"] == "RUNNING"
    glue.stop_crawler(Name=name)
    cr2 = glue.get_crawler(Name=name)["Crawler"]
    assert cr2["State"] == "READY"
    # stopping a non-running crawler raises
    with pytest.raises(ClientError) as exc:
        glue.stop_crawler(Name=name)
    assert exc.value.response["Error"]["Code"] == "CrawlerNotRunningException"
    # cleanup
    glue.delete_crawler(Name=name)


# ---------------------------------------------------------------------------
# CreateJob / DeleteJob / GetJobs / UpdateJob
# ---------------------------------------------------------------------------

def test_glue_create_delete_job(glue):
    name = "qa-cd-job"
    resp = glue.create_job(
        Name=name,
        Role="arn:aws:iam::000000000000:role/R",
        Command={"Name": "glueetl", "ScriptLocation": "s3://b/s.py"},
        GlueVersion="3.0",
    )
    assert resp["Name"] == name
    job = glue.get_job(JobName=name)["Job"]
    assert job["Name"] == name
    # delete returns JobName
    resp2 = glue.delete_job(JobName=name)
    assert resp2["JobName"] == name
    with pytest.raises(ClientError) as exc:
        glue.get_job(JobName=name)
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


def test_glue_get_jobs(glue):
    names = ["qa-gj-a", "qa-gj-b"]
    for n in names:
        glue.create_job(
            Name=n,
            Role="arn:aws:iam::000000000000:role/R",
            Command={"Name": "glueetl", "ScriptLocation": "s3://b/s.py"},
        )
    resp = glue.get_jobs()
    found = [j["Name"] for j in resp["Jobs"]]
    for n in names:
        assert n in found
    # cleanup
    for n in names:
        glue.delete_job(JobName=n)


def test_glue_update_job(glue):
    name = "qa-uj-job"
    glue.create_job(
        Name=name,
        Role="arn:aws:iam::000000000000:role/R",
        Command={"Name": "glueetl", "ScriptLocation": "s3://b/s.py"},
        Description="orig",
    )
    resp = glue.update_job(
        JobName=name,
        JobUpdate={"Description": "updated", "MaxRetries": 3},
    )
    assert resp["JobName"] == name
    job = glue.get_job(JobName=name)["Job"]
    assert job["Description"] == "updated"
    assert job["MaxRetries"] == 3
    # cleanup
    glue.delete_job(JobName=name)


# ---------------------------------------------------------------------------
# BatchStopJobRun
# ---------------------------------------------------------------------------

def test_glue_batch_stop_job_run(glue):
    name = "qa-bsjr-job"
    glue.create_job(
        Name=name,
        Role="arn:aws:iam::000000000000:role/R",
        Command={"Name": "glueetl", "ScriptLocation": "s3://b/s.py"},
    )
    run1 = glue.start_job_run(JobName=name)["JobRunId"]
    run2 = glue.start_job_run(JobName=name)["JobRunId"]
    # Ministack auto-completes runs (SUCCEEDED), so batch stop returns errors
    # for completed runs + not-found run
    resp = glue.batch_stop_job_run(JobName=name, JobRunIds=[run1, run2, "no-such-run"])
    assert "SuccessfulSubmissions" in resp
    assert "Errors" in resp
    # All 3 should be errors: 2 already completed + 1 not found
    assert len(resp["Errors"]) == 3
    # cleanup
    glue.delete_job(JobName=name)


# ---------------------------------------------------------------------------
# SecurityConfigurations (Create / Delete / Get / GetAll)
# ---------------------------------------------------------------------------

def test_glue_security_configuration_crud(glue):
    name = "qa-sec-cfg"
    resp = glue.create_security_configuration(
        Name=name,
        EncryptionConfiguration={
            "S3Encryption": [{"S3EncryptionMode": "SSE-S3"}],
        },
    )
    assert resp["Name"] == name
    assert "CreatedTimestamp" in resp

    cfg = glue.get_security_configuration(Name=name)["SecurityConfiguration"]
    assert cfg["Name"] == name
    assert cfg["EncryptionConfiguration"]["S3Encryption"] == [{"S3EncryptionMode": "SSE-S3"}]

    all_cfgs = glue.get_security_configurations()["SecurityConfigurations"]
    assert any(c["Name"] == name for c in all_cfgs)

    glue.delete_security_configuration(Name=name)
    with pytest.raises(ClientError) as exc:
        glue.get_security_configuration(Name=name)
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


def test_glue_security_configuration_duplicate(glue):
    name = "qa-sec-dup"
    glue.create_security_configuration(Name=name, EncryptionConfiguration={})
    with pytest.raises(ClientError) as exc:
        glue.create_security_configuration(Name=name, EncryptionConfiguration={})
    assert exc.value.response["Error"]["Code"] == "AlreadyExistsException"
    # cleanup
    glue.delete_security_configuration(Name=name)


# ---------------------------------------------------------------------------
# Classifiers (Create / Get / GetAll / Delete)
# ---------------------------------------------------------------------------

def test_glue_classifier_crud(glue):
    name = "qa-cls-grok"
    glue.create_classifier(
        GrokClassifier={
            "Name": name,
            "Classification": "test",
            "GrokPattern": "%{WORD:field}",
        },
    )
    cls = glue.get_classifier(Name=name)["Classifier"]
    assert "GrokClassifier" in cls
    assert cls["GrokClassifier"]["Name"] == name
    assert cls["GrokClassifier"]["GrokPattern"] == "%{WORD:field}"

    all_cls = glue.get_classifiers()["Classifiers"]
    assert any("GrokClassifier" in c and c["GrokClassifier"]["Name"] == name for c in all_cls)

    glue.delete_classifier(Name=name)
    with pytest.raises(ClientError) as exc:
        glue.get_classifier(Name=name)
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


def test_glue_classifier_json(glue):
    name = "qa-cls-json"
    glue.create_classifier(
        JsonClassifier={"Name": name, "JsonPath": "$.records[*]"},
    )
    cls = glue.get_classifier(Name=name)["Classifier"]
    assert "JsonClassifier" in cls
    assert cls["JsonClassifier"]["JsonPath"] == "$.records[*]"
    # cleanup
    glue.delete_classifier(Name=name)


def test_glue_classifier_duplicate(glue):
    name = "qa-cls-dup"
    glue.create_classifier(
        GrokClassifier={"Name": name, "Classification": "t", "GrokPattern": "%{WORD:f}"},
    )
    with pytest.raises(ClientError) as exc:
        glue.create_classifier(
            GrokClassifier={"Name": name, "Classification": "t", "GrokPattern": "%{WORD:f}"},
        )
    assert exc.value.response["Error"]["Code"] == "AlreadyExistsException"
    # cleanup
    glue.delete_classifier(Name=name)


# ---------------------------------------------------------------------------
# BatchGetTriggers
# ---------------------------------------------------------------------------

def test_glue_batch_get_triggers(glue):
    names = ["qa-bgt-a", "qa-bgt-b"]
    for n in names:
        glue.create_trigger(Name=n, Type="ON_DEMAND", Actions=[{"JobName": "dummy"}])
    resp = glue.batch_get_triggers(TriggerNames=["qa-bgt-a", "qa-bgt-b", "no-such-trig"])
    found = [t["Name"] for t in resp["Triggers"]]
    assert "qa-bgt-a" in found
    assert "qa-bgt-b" in found
    assert "no-such-trig" in resp["TriggersNotFound"]
    # cleanup
    for n in names:
        glue.delete_trigger(Name=n)


# ---------------------------------------------------------------------------
# GetTriggers
# ---------------------------------------------------------------------------

def test_glue_get_triggers(glue):
    names = ["qa-gt-x", "qa-gt-y"]
    for n in names:
        glue.create_trigger(Name=n, Type="ON_DEMAND", Actions=[{"JobName": "target-job"}])
    resp = glue.get_triggers(DependentJobName="target-job")
    found = [t["Name"] for t in resp["Triggers"]]
    for n in names:
        assert n in found
    # without filter, should also include them
    resp2 = glue.get_triggers()
    found2 = [t["Name"] for t in resp2["Triggers"]]
    for n in names:
        assert n in found2
    # cleanup
    for n in names:
        glue.delete_trigger(Name=n)


# ---------------------------------------------------------------------------
# UpdateWorkflow
# ---------------------------------------------------------------------------

def test_glue_update_workflow(glue):
    name = "qa-upd-wf"
    glue.create_workflow(Name=name, Description="orig")
    resp = glue.update_workflow(Name=name, Description="updated", MaxConcurrentRuns=5)
    assert resp["Name"] == name
    wf = glue.get_workflow(Name=name)["Workflow"]
    assert wf["Description"] == "updated"
    assert wf["MaxConcurrentRuns"] == 5
    # not found
    with pytest.raises(ClientError) as exc:
        glue.update_workflow(Name="no-such-wf", Description="x")
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
    # cleanup
    glue.delete_workflow(Name=name)


# ---------------------------------------------------------------------------
# CreatePartitionIndex / GetPartitionIndexes
# ---------------------------------------------------------------------------

def test_glue_partition_indexes(glue):
    db = "qa-pidx-db"
    tbl = "qa-pidx-tbl"
    glue.create_database(DatabaseInput={"Name": db})
    glue.create_table(
        DatabaseName=db,
        TableInput={
            "Name": tbl,
            "StorageDescriptor": {
                "Columns": [{"Name": "data", "Type": "string"}],
                "Location": "s3://b/pidx/",
                "InputFormat": "TIF",
                "OutputFormat": "TOF",
                "SerdeInfo": {"SerializationLibrary": "SL"},
            },
            "PartitionKeys": [
                {"Name": "year", "Type": "string"},
                {"Name": "month", "Type": "string"},
            ],
        },
    )
    glue.create_partition_index(
        DatabaseName=db,
        TableName=tbl,
        PartitionIndex={"IndexName": "idx_year", "Keys": ["year"]},
    )
    glue.create_partition_index(
        DatabaseName=db,
        TableName=tbl,
        PartitionIndex={"IndexName": "idx_month", "Keys": ["month"]},
    )
    resp = glue.get_partition_indexes(DatabaseName=db, TableName=tbl)
    indexes = resp["PartitionIndexDescriptorList"]
    assert len(indexes) == 2
    idx_names = [i["IndexName"] for i in indexes]
    assert "idx_year" in idx_names
    assert "idx_month" in idx_names
    assert all(i["IndexStatus"] == "ACTIVE" for i in indexes)
    # cleanup
    glue.delete_table(DatabaseName=db, Name=tbl)
    glue.delete_database(Name=db)


# ── Spark job image selection (1.3.50) ─────────────────────

def test_glue_spark_skips_docker_when_image_missing(glue):
    """glueetl job falls back to subprocess when the Spark Docker image is not pulled.
    The job should not crash MiniStack — it either runs via subprocess (and fails
    on pyspark import) or stubs as SUCCEEDED if the script can't be resolved."""
    import time
    job_name = "test-spark-no-image"
    try:
        glue.delete_job(JobName=job_name)
    except Exception:
        pass

    glue.create_job(
        Name=job_name,
        Role="arn:aws:iam::000000000000:role/GlueRole",
        Command={"Name": "glueetl", "ScriptLocation": "s3://nonexistent/script.py"},
        GlueVersion="4.0",
    )
    resp = glue.start_job_run(JobName=job_name)
    run_id = resp["JobRunId"]

    # Poll until terminal state
    for _ in range(20):
        run = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]
        if run["JobRunState"] in ("SUCCEEDED", "FAILED", "TIMEOUT"):
            break
        time.sleep(0.5)

    # Script can't be resolved (nonexistent S3 path) so it should stub as SUCCEEDED
    # The key assertion: it does NOT hang or crash — it reaches a terminal state
    assert run["JobRunState"] in ("SUCCEEDED", "FAILED"), (
        f"Job should reach terminal state without Docker image. Got: {run['JobRunState']}"
    )


def test_glue_spark_image_for_version_maps_to_official_aws_image():
    """`GlueVersion: 4.0` and `3.0` map to the canonical `amazon/aws-glue-libs`
    images real AWS Glue uses for Spark. Override via `GLUE_DOCKER_IMAGE`."""
    from ministack.services import glue as _glue

    # Default mapping for supported Spark Glue versions
    assert _glue._glue_image_for_version("4.0") == "amazon/aws-glue-libs:glue_libs_4.0.0_image_01"
    assert _glue._glue_image_for_version("3.0") == "amazon/aws-glue-libs:glue_libs_3.0.0_image_01"

    # Unknown GlueVersion falls back to 4.0 (latest supported)
    assert _glue._glue_image_for_version("99.0") == "amazon/aws-glue-libs:glue_libs_4.0.0_image_01"


def test_glue_spark_image_env_override(monkeypatch):
    """Setting GLUE_DOCKER_IMAGE bypasses the per-version map."""
    from ministack.services import glue as _glue

    monkeypatch.setattr(_glue, "GLUE_DOCKER_IMAGE_OVERRIDE", "my-org/custom-glue:latest")
    assert _glue._glue_image_for_version("4.0") == "my-org/custom-glue:latest"
    assert _glue._glue_image_for_version("3.0") == "my-org/custom-glue:latest"


def test_glue_is_spark_job_classifies_by_command_name():
    """`glueetl` and `gluestreaming` are Spark; `pythonshell` is not."""
    from ministack.services import glue as _glue

    assert _glue._is_spark_job({"Command": {"Name": "glueetl"}}) is True
    assert _glue._is_spark_job({"Command": {"Name": "gluestreaming"}}) is True
    assert _glue._is_spark_job({"Command": {"Name": "pythonshell"}}) is False
    assert _glue._is_spark_job({"Command": {}}) is False
    assert _glue._is_spark_job({}) is False


def test_glue_update_table_version_id_optimistic_concurrency(glue):
    """UpdateTable with a stale VersionId must fail (#1183 — real AWS shape)."""
    glue.create_database(DatabaseInput={"Name": "ver_db"})
    glue.create_table(DatabaseName="ver_db", TableInput={"Name": "t1", "Description": "v1"})

    # Read initial version.
    t = glue.get_table(DatabaseName="ver_db", Name="t1")["Table"]
    assert t["VersionId"] == "1"

    # Update with matching VersionId — succeeds and bumps version.
    glue.update_table(
        DatabaseName="ver_db",
        TableInput={"Name": "t1", "Description": "v2"},
        VersionId="1",
    )
    t2 = glue.get_table(DatabaseName="ver_db", Name="t1")["Table"]
    assert t2["VersionId"] == "2"
    assert t2["Description"] == "v2"

    # Update with stale VersionId — rejected.
    with pytest.raises(ClientError) as exc:
        glue.update_table(
            DatabaseName="ver_db",
            TableInput={"Name": "t1", "Description": "v3"},
            VersionId="1",  # stale
        )
    assert exc.value.response["Error"]["Code"] == "ConcurrentModificationException"

    # No VersionId passed — current ministack/AWS shape still allows (back-compat).
    glue.update_table(DatabaseName="ver_db", TableInput={"Name": "t1", "Description": "v4"})
    t4 = glue.get_table(DatabaseName="ver_db", Name="t1")["Table"]
    assert t4["VersionId"] == "3"


def test_glue_user_defined_function_crud(glue):
    """CRUD lifecycle for Glue UserDefinedFunction APIs (#1186)."""
    glue.create_database(DatabaseInput={"Name": "udf_db"})

    glue.create_user_defined_function(
        DatabaseName="udf_db",
        FunctionInput={
            "FunctionName": "upper_clean",
            "ClassName": "com.example.UpperClean",
            "OwnerName": "data-platform",
            "OwnerType": "USER",
            "ResourceUris": [{"ResourceType": "JAR", "Uri": "s3://my-bucket/udfs/upper.jar"}],
        },
    )

    got = glue.get_user_defined_function(DatabaseName="udf_db", FunctionName="upper_clean")["UserDefinedFunction"]
    assert got["FunctionName"] == "upper_clean"
    assert got["ClassName"] == "com.example.UpperClean"
    assert got["DatabaseName"] == "udf_db"
    assert got["ResourceUris"][0]["Uri"] == "s3://my-bucket/udfs/upper.jar"

    # Duplicate create rejected.
    with pytest.raises(ClientError) as exc:
        glue.create_user_defined_function(
            DatabaseName="udf_db",
            FunctionInput={"FunctionName": "upper_clean", "ClassName": "x"},
        )
    assert exc.value.response["Error"]["Code"] == "AlreadyExistsException"

    glue.update_user_defined_function(
        DatabaseName="udf_db",
        FunctionName="upper_clean",
        FunctionInput={"FunctionName": "upper_clean", "ClassName": "com.example.UpperClean2"},
    )
    got2 = glue.get_user_defined_function(DatabaseName="udf_db", FunctionName="upper_clean")["UserDefinedFunction"]
    assert got2["ClassName"] == "com.example.UpperClean2"

    # Pattern is a regex (RE2) over the function name in real Glue — ".*" matches all.
    listed = glue.get_user_defined_functions(DatabaseName="udf_db", Pattern=".*")["UserDefinedFunctions"]
    names = [u["FunctionName"] for u in listed]
    assert "upper_clean" in names

    glue.delete_user_defined_function(DatabaseName="udf_db", FunctionName="upper_clean")
    with pytest.raises(ClientError) as exc2:
        glue.get_user_defined_function(DatabaseName="udf_db", FunctionName="upper_clean")
    assert exc2.value.response["Error"]["Code"] == "EntityNotFoundException"


def test_glue_get_user_defined_functions_pattern_is_regex(glue):
    """GetUserDefinedFunctions treats Pattern as an RE2 regex over the function
    name, not a glob — matches real AWS Glue (the semantics Trino's Glue connector
    relies on when resolving a UDF via `trino__<name>__.*`)."""
    glue.create_database(DatabaseInput={"Name": "regex_db"})
    func_name = "trino__dw_clean_text__abc123"
    glue.create_user_defined_function(
        DatabaseName="regex_db",
        FunctionInput={"FunctionName": func_name, "ClassName": "com.example.Clean"},
    )

    def names(**kwargs):
        return [u["FunctionName"] for u in glue.get_user_defined_functions(**kwargs)["UserDefinedFunctions"]]

    # `.*` matches everything (real Glue: 1; old glob behaviour: 0).
    assert func_name in names(DatabaseName="regex_db", Pattern=".*")
    # A Trino-style anchored regex matches the function (real Glue: 1; old glob: 0).
    assert func_name in names(DatabaseName="regex_db", Pattern="trino__dw_clean_text__.*")
    # A regex that cannot match returns nothing.
    assert names(DatabaseName="regex_db", Pattern="nope__.*") == []

    # A bare `*` is an INVALID regex and must raise InvalidInputException
    # (real Glue: matched 1 under the old glob behaviour — now an error).
    with pytest.raises(ClientError) as exc:
        glue.get_user_defined_functions(DatabaseName="regex_db", Pattern="*")
    assert exc.value.response["Error"]["Code"] == "InvalidInputException"
    assert "Invalid pattern syntax" in exc.value.response["Error"]["Message"]

    # DatabaseName omitted searches across all databases in the catalog.
    assert func_name in names(Pattern="trino__.*")


def test_glue_get_user_defined_functions_pattern_java_quote(glue):
    r"""GetUserDefinedFunctions accepts java.util.regex `\Q...\E` literal-quote
    spans, matching real AWS Glue (which compiles Pattern with java.util.regex).
    Trino's Hive/Glue connector resolves a UDF with a pattern like
    `trino__\Qdw_clean_text\E__.*`; Python `re` / Go RE2 reject `\Q` outright, so
    ministack must translate the span before compiling."""
    glue.create_database(DatabaseInput={"Name": "pq"})
    func_name = "trino__dw_clean_text__abc"
    glue.create_user_defined_function(
        DatabaseName="pq",
        FunctionInput={"FunctionName": func_name, "ClassName": "TrinoFunction"},
    )

    def names(**kwargs):
        return [u["FunctionName"] for u in glue.get_user_defined_functions(**kwargs)["UserDefinedFunctions"]]

    # The exact pattern Trino's Glue connector emits — must resolve the function,
    # not raise `bad escape \Q`.
    assert func_name in names(DatabaseName="pq", Pattern=r"trino__\Qdw_clean_text\E__.*")
    # `.*` still matches everything (regex semantics unaffected).
    assert func_name in names(DatabaseName="pq", Pattern=".*")
    # The literal between \Q...\E is matched literally: regex metachars inside it
    # do not apply, so a `.` in the quoted span only matches a literal dot.
    glue.create_user_defined_function(
        DatabaseName="pq",
        FunctionInput={"FunctionName": "a.b", "ClassName": "TrinoFunction"},
    )
    assert names(DatabaseName="pq", Pattern=r"^\Qa.b\E$") == ["a.b"]
    assert names(DatabaseName="pq", Pattern=r"^\Qaxb\E$") == []
    # A trailing \Q with no closing \E quotes to end-of-string (Java semantics).
    assert func_name in names(DatabaseName="pq", Pattern=r"trino__\Qdw_clean_text__abc")


def test_glue_resolve_script_account_scoped(tmp_path, monkeypatch):
    """_resolve_script finds an s3:// script persisted under the account-scoped
    on-disk layout DATA_DIR/<account>/<bucket>/<key> (#827).

    Regression: the on-disk lookup omitted the account id, so it could never
    match an object written by the canonical account-scoped writer.
    """
    from ministack.core import responses as respmod
    from ministack.services import glue as gluemod
    from ministack.services import s3 as s3mod

    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(gluemod, "S3_DATA_DIR", str(tmp_path))

    # A non-default account also pins that the account segment is honoured,
    # not just the hardcoded default.
    token = respmod._request_account_id.set("111111111111")
    try:
        s3mod._persist_object("scripts-bucket", "jobs/etl.py", b"print('ok')\n")
        persisted = s3mod._object_disk_path("scripts-bucket", "jobs/etl.py")
        assert os.path.isfile(persisted)

        resolved = gluemod._resolve_script("s3://scripts-bucket/jobs/etl.py")
        assert resolved is not None, "script not resolved — on-disk path is unscoped"
        assert "111111111111" in resolved
        assert os.path.samefile(resolved, persisted)
    finally:
        respmod._request_account_id.reset(token)


def test_glue_start_job_run_resolves_script_in_worker_thread(tmp_path, monkeypatch):
    """StartJobRun's worker thread resolves the account-scoped script under the
    caller's account, not the default (#827, cf. #639).

    Regression: _execute runs on a bare threading.Thread, which does not copy
    contextvars, so get_account_id() inside _resolve_script reverted to the
    default account and a non-default account's script was never found.
    """
    from ministack.core import responses as respmod
    from ministack.services import glue as gluemod
    from ministack.services import s3 as s3mod

    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(gluemod, "S3_DATA_DIR", str(tmp_path))

    account = "210987654321"
    captured = {}
    done = threading.Event()

    real_resolve = gluemod._resolve_script

    def spy_resolve(location):
        # Capture the account the worker thread actually runs under, then defer
        # to the real resolver so the on-disk lookup is genuinely exercised.
        captured["account"] = respmod.get_account_id()
        captured["resolved"] = real_resolve(location)
        done.set()
        return captured["resolved"]

    def noop_subprocess(run, job, args, resolved):
        run["JobRunState"] = "SUCCEEDED"

    monkeypatch.setattr(gluemod, "_resolve_script", spy_resolve)
    monkeypatch.setattr(gluemod, "_execute_subprocess", noop_subprocess)

    token = respmod._request_account_id.set(account)
    try:
        s3mod._persist_object("glue-scripts", "etl/main.py", b"print('ok')\n")
        persisted = s3mod._object_disk_path("glue-scripts", "etl/main.py")

        gluemod._create_job({
            "Name": "ctx-job",
            "Command": {"Name": "pythonshell", "ScriptLocation": "s3://glue-scripts/etl/main.py"},
        })
        gluemod._start_job_run({"JobName": "ctx-job"})

        assert done.wait(5), "worker thread never invoked _resolve_script"
        assert captured["account"] == account, (
            f"worker ran under {captured.get('account')!r}, not caller account {account!r}"
        )
        assert captured["resolved"] is not None
        assert os.path.samefile(captured["resolved"], persisted)
    finally:
        respmod._request_account_id.reset(token)
        gluemod._jobs._data.pop((account, "ctx-job"), None)
        gluemod._job_runs._data.pop((account, "ctx-job"), None)


def test_glue_crawler_completes_for_non_default_account(monkeypatch):
    """StartCrawler's finish timer returns the crawler to READY under the
    caller's account, not the default (#827, cf. #639).

    Regression: _finish_crawl runs on a bare threading.Timer, which does not
    copy contextvars, so for a non-default account the `name in _crawlers`
    guard (evaluated under the default account) was False and the crawler hung
    in RUNNING forever with LastCrawl never recorded.
    """
    from ministack.core import responses as respmod
    from ministack.services import glue as gluemod

    # Shrink the 5s finish timer so the test is fast.
    monkeypatch.setattr(gluemod, "CRAWLER_RUN_SECONDS", 0.2)

    account = "555555555555"
    name = "ctx-crawler"
    token = respmod._request_account_id.set(account)
    try:
        gluemod._crawlers._data.pop((account, name), None)
        gluemod._create_crawler({
            "Name": name,
            "Role": "arn:aws:iam::555555555555:role/GlueRole",
            "Targets": {"S3Targets": [{"Path": "s3://b/data/"}]},
        })
        gluemod._start_crawler({"Name": name})
        assert gluemod._crawlers[name]["State"] == "RUNNING"

        # Wait for the finish timer (0.2s) to fire.
        deadline = time.time() + 3
        while time.time() < deadline and gluemod._crawlers[name]["State"] != "READY":
            time.sleep(0.05)

        assert gluemod._crawlers[name]["State"] == "READY", (
            "crawler stuck in RUNNING — finish timer ran under the wrong account"
        )
        last_crawl = gluemod._crawlers[name]["LastCrawl"]
        assert last_crawl is not None and last_crawl["Status"] == "SUCCEEDED"
    finally:
        respmod._request_account_id.reset(token)
        gluemod._crawlers._data.pop((account, name), None)


def test_glue_resolve_script_isolated_per_account(tmp_path, monkeypatch):
    """A script persisted under one account is not resolvable from another (#827).

    Guards the core multi-tenancy property: account-scoped resolution must not
    leak one tenant's on-disk objects to another.
    """
    from ministack.core import responses as respmod
    from ministack.services import glue as gluemod
    from ministack.services import s3 as s3mod

    monkeypatch.setattr(s3mod, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(s3mod, "S3_PERSIST", True)
    monkeypatch.setattr(gluemod, "S3_DATA_DIR", str(tmp_path))

    owner = "111111111111"
    other = "222222222222"
    uri = "s3://scripts-bucket/jobs/etl.py"

    tok_owner = respmod._request_account_id.set(owner)
    try:
        s3mod._persist_object("scripts-bucket", "jobs/etl.py", b"print('ok')\n")
        owner_path = gluemod._resolve_script(uri)
        assert owner_path is not None and owner in owner_path
    finally:
        respmod._request_account_id.reset(tok_owner)

    tok_other = respmod._request_account_id.set(other)
    try:
        # 'other' has no such object on disk (scoped under its own account) or
        # in memory, so the lookup must not surface 'owner's script.
        assert gluemod._resolve_script(uri) is None
    finally:
        respmod._request_account_id.reset(tok_other)


# ---- Column Statistics ----

def _make_int_column_stats(column_name):
    return {
        "ColumnName": column_name,
        "ColumnType": "int",
        "AnalyzedTime": 1700000000,
        "StatisticsData": {
            "Type": "LONG",
            "LongColumnStatisticsData": {
                "MinimumValue": 1,
                "MaximumValue": 100,
                "NumberOfNulls": 0,
                "NumberOfDistinctValues": 50,
            },
        },
    }


def _setup_stats_table(glue, db, table, partitioned=False):
    glue.create_database(DatabaseInput={"Name": db})
    table_input = {
        "Name": table,
        "StorageDescriptor": {
            "Columns": [
                {"Name": "id", "Type": "int"},
                {"Name": "amount", "Type": "int"},
            ],
            "Location": f"s3://bucket/{db}/{table}/",
            "InputFormat": "",
            "OutputFormat": "",
            "SerdeInfo": {},
        },
    }
    if partitioned:
        table_input["PartitionKeys"] = [{"Name": "dt", "Type": "string"}]
    glue.create_table(DatabaseName=db, TableInput=table_input)


def test_glue_column_statistics_for_table_crud(glue):
    db, table = "qa-glue-colstats-tbl-db", "qa-glue-colstats-tbl"
    _setup_stats_table(glue, db, table)

    stats_id = _make_int_column_stats("id")
    stats_amount = _make_int_column_stats("amount")
    upd = glue.update_column_statistics_for_table(
        DatabaseName=db, TableName=table,
        ColumnStatisticsList=[stats_id, stats_amount],
    )
    assert upd.get("Errors", []) == []

    got = glue.get_column_statistics_for_table(
        DatabaseName=db, TableName=table, ColumnNames=["id", "amount"],
    )
    assert {s["ColumnName"] for s in got["ColumnStatisticsList"]} == {"id", "amount"}
    assert got.get("Errors", []) == []

    missing = glue.get_column_statistics_for_table(
        DatabaseName=db, TableName=table, ColumnNames=["id", "missing"],
    )
    assert [s["ColumnName"] for s in missing["ColumnStatisticsList"]] == ["id"]
    assert missing["Errors"][0]["ColumnName"] == "missing"

    glue.delete_column_statistics_for_table(
        DatabaseName=db, TableName=table, ColumnName="id",
    )
    after = glue.get_column_statistics_for_table(
        DatabaseName=db, TableName=table, ColumnNames=["id", "amount"],
    )
    assert [s["ColumnName"] for s in after["ColumnStatisticsList"]] == ["amount"]


def test_glue_column_statistics_for_table_missing_table(glue):
    with pytest.raises(ClientError) as exc:
        glue.get_column_statistics_for_table(
            DatabaseName="nope-db", TableName="nope-tbl", ColumnNames=["id"],
        )
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


def test_glue_delete_column_statistics_for_table_unknown_column_is_idempotent(glue):
    db, table = "qa-glue-colstats-tbl-del-db", "qa-glue-colstats-tbl-del"
    _setup_stats_table(glue, db, table)
    # Real AWS Glue returns 200 / empty body when deleting stats for a column
    # that never had any — Delete* operations are idempotent.
    glue.delete_column_statistics_for_table(
        DatabaseName=db, TableName=table, ColumnName="never-set",
    )


def test_glue_column_statistics_cleared_on_table_delete(glue):
    db, table = "qa-glue-colstats-cleanup-db", "qa-glue-colstats-cleanup"
    _setup_stats_table(glue, db, table)
    glue.update_column_statistics_for_table(
        DatabaseName=db, TableName=table,
        ColumnStatisticsList=[_make_int_column_stats("id")],
    )
    glue.delete_table(DatabaseName=db, Name=table)
    # Re-create the table with the same name; stats from the previous
    # incarnation must not leak through.
    glue.create_table(
        DatabaseName=db,
        TableInput={
            "Name": table,
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "int"}],
                "Location": f"s3://bucket/{db}/{table}/",
                "InputFormat": "", "OutputFormat": "", "SerdeInfo": {},
            },
        },
    )
    got = glue.get_column_statistics_for_table(
        DatabaseName=db, TableName=table, ColumnNames=["id"],
    )
    assert got["ColumnStatisticsList"] == []
    assert got["Errors"][0]["ColumnName"] == "id"


def test_glue_column_statistics_for_partition_crud(glue):
    db, table = "qa-glue-colstats-part-db", "qa-glue-colstats-part"
    _setup_stats_table(glue, db, table, partitioned=True)
    glue.create_partition(
        DatabaseName=db, TableName=table,
        PartitionInput={
            "Values": ["2024-01-01"],
            "StorageDescriptor": {
                "Columns": [], "Location": f"s3://bucket/{db}/{table}/dt=2024-01-01",
                "InputFormat": "", "OutputFormat": "", "SerdeInfo": {},
            },
        },
    )

    stats_id = _make_int_column_stats("id")
    stats_amount = _make_int_column_stats("amount")
    upd = glue.update_column_statistics_for_partition(
        DatabaseName=db, TableName=table,
        PartitionValues=["2024-01-01"],
        ColumnStatisticsList=[stats_id, stats_amount],
    )
    assert upd.get("Errors", []) == []

    got = glue.get_column_statistics_for_partition(
        DatabaseName=db, TableName=table,
        PartitionValues=["2024-01-01"], ColumnNames=["id", "amount"],
    )
    assert {s["ColumnName"] for s in got["ColumnStatisticsList"]} == {"id", "amount"}

    glue.delete_column_statistics_for_partition(
        DatabaseName=db, TableName=table,
        PartitionValues=["2024-01-01"], ColumnName="amount",
    )
    after = glue.get_column_statistics_for_partition(
        DatabaseName=db, TableName=table,
        PartitionValues=["2024-01-01"], ColumnNames=["id", "amount"],
    )
    assert [s["ColumnName"] for s in after["ColumnStatisticsList"]] == ["id"]
    assert after["Errors"][0]["ColumnName"] == "amount"


def test_glue_column_statistics_for_partition_missing_partition(glue):
    db, table = "qa-glue-colstats-nopart-db", "qa-glue-colstats-nopart"
    _setup_stats_table(glue, db, table, partitioned=True)
    with pytest.raises(ClientError) as exc:
        glue.update_column_statistics_for_partition(
            DatabaseName=db, TableName=table,
            PartitionValues=["never"],
            ColumnStatisticsList=[_make_int_column_stats("id")],
        )
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


# ---------------------------------------------------------------------------
# Iceberg REST catalog surface (in-process routing/config/load tests).
# Folded from test_glue_iceberg_rest.py.
# ---------------------------------------------------------------------------


def _svc(name):
    """Resolve a service module through sys.modules at call time.

    NOT a module-level `from ministack.services import glue, s3` — the
    persistence regression tests cold-reimport service modules
    (`sys.modules.pop` + fresh import), so an import-time binding here can
    end up pointing at a stale module object while glue.py's own lazy
    `from ministack.services import s3` resolves the fresh one. State
    seeded on the stale object is then invisible to production code (the
    exact failure mode this replaced: 404 where 200 was expected, only
    when test_persistence.py ran first in the same worker)."""
    return importlib.import_module(f"ministack.services.{name}")


def _call(method, path, query_params=None):
    status, headers, body = asyncio.run(
        _svc("glue").handle_request(method, path, {}, b"", query_params or {})
    )
    payload = json.loads(body) if body else None
    return status, headers, payload


@pytest.fixture(autouse=True)
def _reset():
    # Pin the per-request account so AccountScopedDict lookups land in the
    # same bucket whether the caller is the test or production code.
    _responses._request_account_id.set("000000000000")
    _responses._request_region.set("us-east-1")
    _svc("glue").reset()
    _svc("s3")._buckets.clear()
    yield
    _svc("glue").reset()
    _svc("s3")._buckets.clear()


# ── Routing ──────────────────────────────────────────────────


def test_glue_signed_iceberg_request_routes_to_glue_service():
    """DuckDB signs Iceberg REST requests with the `glue` credential scope.
    The router's existing scope dispatch must land them on the glue module —
    no path-based special case required."""
    headers = {
        "authorization": (
            "AWS4-HMAC-SHA256 Credential=test/20260610/us-east-1/glue/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        )
    }
    assert detect_service("GET", "/iceberg/v1/config", headers, {}) == "glue"


def test_s3tables_signed_iceberg_request_still_routes_to_s3tables():
    """The S3 Tables Iceberg REST surface is a distinct AWS service; signing
    as `s3tables` must keep landing on services/s3tables.py."""
    headers = {
        "authorization": (
            "AWS4-HMAC-SHA256 Credential=test/20260610/us-east-1/s3tables/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        )
    }
    assert detect_service("GET", "/iceberg/v1/config", headers, {}) == "s3tables"


# ── /iceberg/v1/config ───────────────────────────────────────


def test_config_returns_glue_catalogs_prefix_and_s3_overrides():
    status, headers, payload = _call(
        "GET", "/iceberg/v1/config", {"warehouse": ["000000000000"]}
    )
    assert status == 200
    assert headers["Content-Type"] == "application/json"
    # Glue's prefix shape — subsequent client URLs become
    # /iceberg/v1/catalogs/000000000000/namespaces/...
    assert payload["defaults"]["prefix"] == "catalogs/000000000000"
    overrides = payload["overrides"]
    assert overrides["s3.endpoint"].startswith("http://")
    assert overrides["s3.path-style-access"] == "true"
    assert "s3.region" in overrides
    # Fixed creds, never echoed from the host env — emitting ambient
    # AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY into a response body would
    # leak real credentials from a developer's shell.
    assert overrides["s3.access-key-id"] == "test"
    assert overrides["s3.secret-access-key"] == "test"


def test_config_without_warehouse_returns_empty_defaults():
    _, _, payload = _call("GET", "/iceberg/v1/config")
    assert payload["defaults"] == {}
    assert "overrides" in payload


# ── Namespaces ───────────────────────────────────────────────


def test_list_namespaces_returns_glue_databases():
    _svc("glue")._databases["db_a"] = {"Name": "db_a"}
    _svc("glue")._databases["db_b"] = {"Name": "db_b"}
    _, _, payload = _call("GET", "/iceberg/v1/catalogs/000000000000/namespaces")
    assert payload["namespaces"] == [["db_a"], ["db_b"]]


def test_get_namespace_404s_when_database_missing():
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/nope"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchNamespaceException"


def test_get_namespace_returns_shape_when_database_exists():
    _svc("glue")._databases["db"] = {"Name": "db"}
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db"
    )
    assert status == 200
    assert payload == {"namespace": ["db"], "properties": {}}


# ── ListTables ───────────────────────────────────────────────


def test_list_tables_404s_when_namespace_missing():
    """Real Iceberg REST returns NoSuchNamespaceException for ListTables on
    an unknown namespace — not an empty 200 list."""
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/nope/tables"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchNamespaceException"


def test_list_tables_hides_non_iceberg_glue_tables():
    """A Glue table without `Parameters['metadata_location']` is a plain
    Hive/CSV/Parquet table and must not appear on the Iceberg surface."""
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/iceberg_table"] = {
        "Name": "iceberg_table",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": "s3://lake/t/metadata/v0.metadata.json"},
    }
    _svc("glue")._tables["db/csv_table"] = {
        "Name": "csv_table", "DatabaseName": "db", "Parameters": {},
    }
    _, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables"
    )
    assert [t["name"] for t in payload["identifiers"]] == ["iceberg_table"]
    assert payload["identifiers"][0]["namespace"] == ["db"]


# ── LoadTable hot path ───────────────────────────────────────


_META_JSON = {
    "format-version": 2,
    "table-uuid": "11111111-2222-3333-4444-555555555555",
    "location": "s3://lake/dim_application",
    "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
    "current-schema-id": 0,
    "current-snapshot-id": 42,
    "properties": {},
}


def _seed_glue_table_with_metadata():
    key = "dim_application/metadata/00003-deadbeef.metadata.json"
    _svc("s3")._buckets["lake"] = {"objects": {key: {"body": json.dumps(_META_JSON).encode()}}}
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/dim_application"] = {
        "Name": "dim_application",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": f"s3://lake/{key}"},
    }


def test_load_table_inlines_metadata_json_verbatim():
    _seed_glue_table_with_metadata()
    status, _, payload = _call(
        "GET",
        "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/dim_application",
    )
    assert status == 200
    assert payload["metadata-location"].endswith("/00003-deadbeef.metadata.json")
    assert payload["metadata"] == _META_JSON  # passthrough — no transformation
    assert "s3.endpoint" in payload["config"]


def test_load_table_404s_when_metadata_object_missing():
    """Glue table exists and points at an S3 URI, but the metadata.json
    object isn't there. 200-with-empty-metadata would make DuckDB treat it
    as a real-but-empty table and silently return wrong results."""
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/orphan"] = {
        "Name": "orphan",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": "s3://lake/orphan/metadata/v0.metadata.json"},
    }
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/orphan"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchTableException"
    assert payload["error"]["code"] == 404


def test_load_table_404s_when_metadata_json_unparseable():
    key = "broken/metadata/v0.metadata.json"
    _svc("s3")._buckets["lake"] = {"objects": {key: {"body": b"<<not json>>"}}}
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/broken"] = {
        "Name": "broken",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": f"s3://lake/{key}"},
    }
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/broken"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchTableException"


def test_load_table_404s_on_unknown_table():
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/missing"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchTableException"


def test_load_table_404s_on_non_iceberg_table():
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/csv"] = {"Name": "csv", "DatabaseName": "db", "Parameters": {}}
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/csv"
    )
    assert status == 404


# ── HEAD / TableExists ───────────────────────────────────────


def test_head_returns_200_for_iceberg_table():
    _seed_glue_table_with_metadata()
    status, _, _ = _call(
        "HEAD",
        "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/dim_application",
    )
    assert status == 200


def test_head_returns_404_for_non_iceberg_table():
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/csv"] = {"Name": "csv", "DatabaseName": "db", "Parameters": {}}
    status, _, _ = _call(
        "HEAD", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/csv"
    )
    assert status == 404


# ── Fall-throughs ────────────────────────────────────────────


def test_post_to_table_returns_501_unsupported():
    """Read-only surface: writes get an explicit 501 envelope instead of a
    silent success that never persisted anything."""
    status, _, payload = _call(
        "POST", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/t"
    )
    assert status == 501
    assert payload["error"]["type"] == "UnsupportedOperationException"


def test_non_catalogs_prefix_returns_501():
    """Bare-warehouse prefixes (`/v1/{warehouse}/namespaces`) are the
    S3 Tables shape, not Glue's — reject rather than guess."""
    status, _, payload = _call(
        "GET", "/iceberg/v1/000000000000/namespaces"
    )
    assert status == 501


def test_unknown_version_prefix_404s():
    status, _, _ = _call("GET", "/iceberg/v2/config")
    assert status == 404


def test_glue_json_rpc_surface_unaffected():
    """The X-Amz-Target JSON RPC surface must keep working alongside the
    Iceberg branch — same module, two protocols."""
    status, _, body = asyncio.run(
        _svc("glue").handle_request(
            "POST", "/",
            {"x-amz-target": "AWSGlue.CreateDatabase"},
            json.dumps({"DatabaseInput": {"Name": "rpc_db"}}).encode(),
            {},
        )
    )
    assert status == 200
    assert "rpc_db" in _svc("glue")._databases
