import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


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
