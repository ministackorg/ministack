"""CloudFormation AWS::Glue::* resource types (#1875)."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from ministack.services.cloudformation.stacks import _diff_resources

_ROLE = "arn:aws:iam::000000000000:role/glue-cfn-role"
_WAIT = {"Delay": 1, "MaxAttempts": 30}


def _suffix():
    return uuid.uuid4().hex[:8]


def _create(cfn, name, template):
    cfn.create_stack(StackName=name, TemplateBody=json.dumps(template))
    cfn.get_waiter("stack_create_complete").wait(StackName=name, WaiterConfig=_WAIT)


def _update(cfn, name, template):
    cfn.update_stack(StackName=name, TemplateBody=json.dumps(template))
    cfn.get_waiter("stack_update_complete").wait(StackName=name, WaiterConfig=_WAIT)


def _delete(cfn, name):
    cfn.delete_stack(StackName=name)
    cfn.get_waiter("stack_delete_complete").wait(StackName=name, WaiterConfig=_WAIT)


def _outputs(cfn, name):
    stack = cfn.describe_stacks(StackName=name)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def _not_found(call):
    with pytest.raises(ClientError) as exc:
        call()
    assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


def _glue_arn(sts, kind, name):
    account = sts.get_caller_identity()["Account"]
    return f"arn:aws:glue:us-east-1:{account}:{kind}/{name}"


def _data_lake(s, *, table_description="raw events", crawler_schedule=True,
               job_tags=True, trigger_description="nightly", partition_location="p1"):
    crawler = {
        "Name": f"crawler-{s}",
        "Role": _ROLE,
        "DatabaseName": {"Ref": "Db"},
        "Targets": {"S3Targets": [{"Path": f"s3://lake-{s}/events/"}]},
        "Tags": {"team": "data"},
    }
    if crawler_schedule:
        crawler["Schedule"] = {"ScheduleExpression": "cron(0 2 * * ? *)"}
    job = {
        "Name": f"job-{s}",
        "Role": _ROLE,
        "Command": {"Name": "pythonshell", "ScriptLocation": f"s3://lake-{s}/etl.py"},
        "DefaultArguments": {"--stage": "dev"},
        "MaxRetries": 1,
    }
    if job_tags:
        job["Tags"] = [{"Key": "owner", "Value": "etl"}]
    return {
        "Resources": {
            "Db": {
                "Type": "AWS::Glue::Database",
                "Properties": {
                    "CatalogId": {"Ref": "AWS::AccountId"},
                    "DatabaseInput": {"Name": f"lake_{s}", "Description": "data lake"},
                },
            },
            "Events": {
                "Type": "AWS::Glue::Table",
                "Properties": {
                    "CatalogId": {"Ref": "AWS::AccountId"},
                    "DatabaseName": {"Ref": "Db"},
                    "TableInput": {
                        "Name": "events",
                        "Description": table_description,
                        "TableType": "EXTERNAL_TABLE",
                        "PartitionKeys": [{"Name": "dt", "Type": "string"}],
                        "StorageDescriptor": {
                            "Columns": [{"Name": "id", "Type": "string"}],
                            "Location": f"s3://lake-{s}/events/",
                        },
                    },
                },
            },
            "Day": {
                "Type": "AWS::Glue::Partition",
                "Properties": {
                    "CatalogId": {"Ref": "AWS::AccountId"},
                    "DatabaseName": {"Ref": "Db"},
                    "TableName": {"Ref": "Events"},
                    "PartitionInput": {
                        "Values": ["2026-10-01"],
                        "StorageDescriptor": {"Location": f"s3://lake-{s}/events/{partition_location}/"},
                    },
                },
            },
            "Jdbc": {
                "Type": "AWS::Glue::Connection",
                "Properties": {
                    "CatalogId": {"Ref": "AWS::AccountId"},
                    "ConnectionInput": {
                        "Name": f"conn-{s}",
                        "ConnectionType": "JDBC",
                        "ConnectionProperties": {"JDBC_CONNECTION_URL": "jdbc:postgresql://db:5432/app"},
                    },
                },
            },
            "Crawler": {"Type": "AWS::Glue::Crawler", "Properties": crawler},
            "Job": {"Type": "AWS::Glue::Job", "Properties": job},
            "Trigger": {
                "Type": "AWS::Glue::Trigger",
                "Properties": {
                    "Name": f"trigger-{s}",
                    "Type": "SCHEDULED",
                    "Schedule": "cron(0 3 * * ? *)",
                    "Description": trigger_description,
                    "Actions": [{"JobName": {"Ref": "Job"}}],
                },
            },
        },
        "Outputs": {
            key: {"Value": {"Ref": key}}
            for key in ("Db", "Events", "Day", "Jdbc", "Crawler", "Job", "Trigger")
        } | {
            "JdbcName": {"Value": {"Fn::GetAtt": ["Jdbc", "Name"]}},
            "DayValues": {"Value": {"Fn::GetAtt": ["Day", "IdentifierPartitionInputValues"]}},
        },
    }


def test_cfn_glue_data_lake_stack_create_and_delete(cfn, glue, sts):
    s = _suffix()
    stack = f"glue-lake-{s}"
    _create(cfn, stack, _data_lake(s))
    db, conn, crawler, job, trigger = f"lake_{s}", f"conn-{s}", f"crawler-{s}", f"job-{s}", f"trigger-{s}"

    out = _outputs(cfn, stack)
    assert out["Db"] == db
    assert out["Events"] == "events"
    assert out["Jdbc"] == out["JdbcName"] == conn
    assert out["Crawler"] == crawler
    assert out["Job"] == job
    assert out["Trigger"] == trigger
    # The partition's Ref is its compound primary identifier joined with "|".
    account = sts.get_caller_identity()["Account"]
    assert out["Day"] == f"{account}|{db}|events|{out['DayValues']}"

    assert glue.get_database(Name=db)["Database"]["Description"] == "data lake"
    table = glue.get_table(DatabaseName=db, Name="events")["Table"]
    assert table["Description"] == "raw events"
    assert table["PartitionKeys"] == [{"Name": "dt", "Type": "string"}]
    partition = glue.get_partition(DatabaseName=db, TableName="events",
                                   PartitionValues=["2026-10-01"])["Partition"]
    assert partition["StorageDescriptor"]["Location"] == f"s3://lake-{s}/events/p1/"
    assert glue.get_connection(Name=conn)["Connection"]["ConnectionType"] == "JDBC"
    got_crawler = glue.get_crawler(Name=crawler)["Crawler"]
    assert got_crawler["DatabaseName"] == db
    assert got_crawler["Schedule"]["ScheduleExpression"] == "cron(0 2 * * ? *)"
    assert glue.get_job(JobName=job)["Job"]["Command"]["Name"] == "pythonshell"
    assert glue.get_trigger(Name=trigger)["Trigger"]["Actions"] == [{"JobName": job}]
    # Tags given as a map (crawler) and as a Tag list (job) both land.
    assert glue.get_tags(ResourceArn=_glue_arn(sts, "crawler", crawler))["Tags"] == {"team": "data"}
    assert glue.get_tags(ResourceArn=_glue_arn(sts, "job", job))["Tags"] == {"owner": "etl"}

    _delete(cfn, stack)
    _not_found(lambda: glue.get_database(Name=db))
    _not_found(lambda: glue.get_table(DatabaseName=db, Name="events"))
    _not_found(lambda: glue.get_connection(Name=conn))
    _not_found(lambda: glue.get_crawler(Name=crawler))
    _not_found(lambda: glue.get_job(JobName=job))
    _not_found(lambda: glue.get_trigger(Name=trigger))


def test_cfn_glue_update_in_place(cfn, glue, sts):
    s = _suffix()
    stack = f"glue-upd-{s}"
    _create(cfn, stack, _data_lake(s))
    db, crawler, job, trigger = f"lake_{s}", f"crawler-{s}", f"job-{s}", f"trigger-{s}"
    created = glue.get_table(DatabaseName=db, Name="events")["Table"]["CreateTime"]
    try:
        _update(cfn, stack, _data_lake(
            s, table_description="cleaned events", crawler_schedule=False, job_tags=False,
            trigger_description="hourly", partition_location="p2"))

        table = glue.get_table(DatabaseName=db, Name="events")["Table"]
        assert table["Description"] == "cleaned events"
        assert table["CreateTime"] == created
        assert table["VersionId"] == "2"
        partition = glue.get_partition(DatabaseName=db, TableName="events",
                                       PartitionValues=["2026-10-01"])["Partition"]
        assert partition["StorageDescriptor"]["Location"] == f"s3://lake-{s}/events/p2/"
        # A property the template dropped is cleared, not left at its old value.
        assert not glue.get_crawler(Name=crawler)["Crawler"].get("Schedule")
        assert glue.get_tags(ResourceArn=_glue_arn(sts, "job", job))["Tags"] == {}
        assert glue.get_trigger(Name=trigger)["Trigger"]["Description"] == "hourly"
    finally:
        _delete(cfn, stack)


def test_cfn_glue_table_rename_replaces(cfn, glue):
    s = _suffix()
    stack = f"glue-ren-{s}"
    template = _data_lake(s)
    _create(cfn, stack, template)
    db = f"lake_{s}"
    try:
        template["Resources"]["Events"]["Properties"]["TableInput"]["Name"] = "events_v2"
        # The partition follows its table through Ref, which replaces it too.
        _update(cfn, stack, template)
        assert _outputs(cfn, stack)["Events"] == "events_v2"
        assert glue.get_table(DatabaseName=db, Name="events_v2")["Table"]["Name"] == "events_v2"
        _not_found(lambda: glue.get_table(DatabaseName=db, Name="events"))
        glue.get_partition(DatabaseName=db, TableName="events_v2", PartitionValues=["2026-10-01"])
    finally:
        _delete(cfn, stack)


def test_cfn_glue_partition_values_change_replaces(cfn, glue):
    s = _suffix()
    stack = f"glue-pv-{s}"
    template = _data_lake(s)
    _create(cfn, stack, template)
    db = f"lake_{s}"
    before = _outputs(cfn, stack)["Day"]
    try:
        template["Resources"]["Day"]["Properties"]["PartitionInput"]["Values"] = ["2026-10-02"]
        _update(cfn, stack, template)
        assert _outputs(cfn, stack)["Day"] != before
        glue.get_partition(DatabaseName=db, TableName="events", PartitionValues=["2026-10-02"])
        _not_found(lambda: glue.get_partition(
            DatabaseName=db, TableName="events", PartitionValues=["2026-10-01"]))
    finally:
        _delete(cfn, stack)


def test_cfn_glue_database_input_name_change_updates_in_place(cfn, glue):
    s = _suffix()
    stack = f"glue-dbn-{s}"

    def template(name, description):
        return {
            "Resources": {"Db": {"Type": "AWS::Glue::Database", "Properties": {
                "CatalogId": {"Ref": "AWS::AccountId"},
                "DatabaseInput": {"Name": name, "Description": description}}}},
            "Outputs": {"Db": {"Value": {"Ref": "Db"}}},
        }

    _create(cfn, stack, template(f"first_{s}", "one"))
    try:
        # DatabaseName is the type's only create-only property, so the
        # database is updated, not replaced, and keeps its Ref.
        _update(cfn, stack, template(f"second_{s}", "two"))
        assert _outputs(cfn, stack)["Db"] == f"first_{s}"
        assert glue.get_database(Name=f"first_{s}")["Database"]["Description"] == "two"
        _not_found(lambda: glue.get_database(Name=f"second_{s}"))
    finally:
        _delete(cfn, stack)


def test_cfn_glue_table_moves_database(cfn, glue):
    s = _suffix()
    stack = f"glue-mv-{s}"

    def template(db_ref):
        return {"Resources": {
            "A": {"Type": "AWS::Glue::Database", "Properties": {
                "CatalogId": {"Ref": "AWS::AccountId"}, "DatabaseInput": {"Name": f"a_{s}"}}},
            "B": {"Type": "AWS::Glue::Database", "Properties": {
                "CatalogId": {"Ref": "AWS::AccountId"}, "DatabaseInput": {"Name": f"b_{s}"}}},
            "T": {"Type": "AWS::Glue::Table", "Properties": {
                "CatalogId": {"Ref": "AWS::AccountId"}, "DatabaseName": {"Ref": db_ref},
                "TableInput": {"Name": "t"}}},
        }}

    _create(cfn, stack, template("A"))
    try:
        _update(cfn, stack, template("B"))
        glue.get_table(DatabaseName=f"b_{s}", Name="t")
        _not_found(lambda: glue.get_table(DatabaseName=f"a_{s}", Name="t"))
    finally:
        _delete(cfn, stack)


def test_cfn_glue_named_trigger_type_change_is_refused(cfn, glue):
    s = _suffix()
    stack = f"glue-trg-{s}"

    def template(trigger_type):
        props = {"Name": f"trigger-{s}", "Type": trigger_type,
                 "Actions": [{"JobName": f"job-{s}"}]}
        if trigger_type == "SCHEDULED":
            props["Schedule"] = "cron(0 3 * * ? *)"
        return {"Resources": {"T": {"Type": "AWS::Glue::Trigger", "Properties": props}}}

    _create(cfn, stack, template("ON_DEMAND"))
    try:
        cfn.update_stack(StackName=stack, TemplateBody=json.dumps(template("SCHEDULED")))
        with pytest.raises(Exception):
            cfn.get_waiter("stack_update_complete").wait(StackName=stack, WaiterConfig=_WAIT)
        events = cfn.describe_stack_events(StackName=stack)["StackEvents"]
        reasons = [e.get("ResourceStatusReason", "") for e in events if e["LogicalResourceId"] == "T"]
        assert any("custom-named resource requires replacing" in r for r in reasons)
        assert glue.get_trigger(Name=f"trigger-{s}")["Trigger"]["Type"] == "ON_DEMAND"
    finally:
        _delete(cfn, stack)


def test_cfn_glue_generated_names(cfn, glue):
    s = _suffix()
    stack = f"Glue-Gen-{s}"
    template = {
        "Resources": {
            "Db": {"Type": "AWS::Glue::Database", "Properties": {
                "CatalogId": {"Ref": "AWS::AccountId"}, "DatabaseInput": {}}},
            "Tbl": {"Type": "AWS::Glue::Table", "Properties": {
                "CatalogId": {"Ref": "AWS::AccountId"}, "DatabaseName": {"Ref": "Db"},
                "TableInput": {"TableType": "EXTERNAL_TABLE"}}},
            "Job": {"Type": "AWS::Glue::Job", "Properties": {
                "Role": _ROLE, "Command": {"Name": "pythonshell", "ScriptLocation": "s3://b/k.py"}}},
        },
        "Outputs": {k: {"Value": {"Ref": k}} for k in ("Db", "Tbl", "Job")},
    }
    _create(cfn, stack, template)
    try:
        out = _outputs(cfn, stack)
        # Glue folds catalog names to lowercase, so generated ones start that way.
        assert out["Db"] == out["Db"].lower() and out["Db"].startswith(f"glue-gen-{s}-db-")
        assert out["Tbl"] == out["Tbl"].lower()
        assert out["Job"].startswith(f"{stack}-Job-")
        glue.get_table(DatabaseName=out["Db"], Name=out["Tbl"])
        glue.get_job(JobName=out["Job"])
        # An update under a generated name keeps the same resource.
        template["Resources"]["Job"]["Properties"]["MaxRetries"] = 2
        _update(cfn, stack, template)
        assert _outputs(cfn, stack)["Job"] == out["Job"]
        assert glue.get_job(JobName=out["Job"])["Job"]["MaxRetries"] == 2
    finally:
        _delete(cfn, stack)


# Expected values follow the createOnlyProperties of the published registry
# schemas; they were not observed on an AWS change set.
@pytest.mark.parametrize("rtype,old,new,expected", [
    ("AWS::Glue::Database", {"DatabaseName": "a"}, {"DatabaseName": "b"}, "Always"),
    ("AWS::Glue::Database", {"DatabaseInput": {"Name": "a"}}, {"DatabaseInput": {"Name": "b"}}, "Never"),
    ("AWS::Glue::Job", {"Name": "a"}, {"Name": "b"}, "Always"),
    ("AWS::Glue::Job", {"MaxRetries": 0}, {"MaxRetries": 1}, "Never"),
    ("AWS::Glue::Trigger", {"Type": "ON_DEMAND"}, {"Type": "SCHEDULED"}, "Always"),
    ("AWS::Glue::Crawler", {"Description": "a"}, {"Description": "b"}, "Never"),
    ("AWS::Glue::Partition", {"TableName": "a"}, {"TableName": "b"}, "Conditionally"),
])
def test_cfn_glue_change_set_recreation(rtype, old, new, expected):
    def tmpl(props):
        return {"Resources": {"R": {"Type": rtype, "Properties": props}}}

    change = _diff_resources(tmpl(old), tmpl(new))[0]["ResourceChange"]
    targets = {d["Target"]["Name"]: d["Target"]["RequiresRecreation"]
               for d in change.get("Details", []) if d["Target"].get("Name")}
    assert set(targets.values()) == {expected}
