import uuid

import pytest
from botocore.exceptions import ClientError, ParamValidationError

name_prefix = "cur-test"
test_tags = [
    { "Key": "kids", "Value": "zalkpz" },
    { "Key": "dates", "Value": "091203060606" },
]

def _report_definition(name: str) -> dict:
    return {
        "ReportName": name,
        "TimeUnit": "DAILY",
        "Format": "textORcsv",
        "Compression": "GZIP",
        "AdditionalSchemaElements": ["RESOURCES"],
        "S3Bucket": "cur-test-bucket",
        "S3Prefix": "reports",
        "S3Region": "us-east-1",
        "AdditionalArtifacts": [],
        "RefreshClosedReports": True,
        "ReportVersioning": "OVERWRITE_REPORT",
    }


def _uid() -> str:
    return uuid.uuid4().hex[:8]


# -- DeleteReportDefinition ----------------------------------------------------


def test_cur_delete_report_definition(cur):
    name = f"{name_prefix}-{_uid()}"

    # First create one and then we'll immediately delete it.
    cur.put_report_definition(ReportDefinition=_report_definition(name))

    deleted = cur.delete_report_definition(ReportName=name)
    assert deleted["ResponseMetadata"]["HTTPStatusCode"] == 200

    reports = cur.describe_report_definitions()["ReportDefinitions"]
    assert all(r["ReportName"] != name for r in reports)


def test_cur_delete_report_definition_not_found(cur):
    name = f"not-found-{_uid()}"

    with pytest.raises(ClientError) as e:
      cur.delete_report_definition(ReportName=name)

    assert e.value.response["Error"]["Code"] =="ValidationException"
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


# -- DescribeReportDefinitions -------------------------------------------------


def test_cur_describe_report_definitions(cur):
    max = 4
    name = f"{name_prefix}-{_uid()}"

    # Create {max} report definitions.
    for i in range(0, max):
        cur.put_report_definition(
            ReportDefinition=_report_definition(f"{name}-{i:02d}")
        )

    # Retrieve all report definitions. This will include the {max} just created,
    # plus, potentially, any leftovers in the MiniStack state.
    resp = cur.describe_report_definitions()
    list = resp["ReportDefinitions"]

    # Verify that each of the test reports exists.
    for i in range(0, max):
        assert any(r["ReportName"] == f"{name}-{i:02d}" for r in list)


def test_cur_describe_report_definitions_empty(cur):

    # Get all the report definitions and delete them so we're empty.
    resp = cur.describe_report_definitions()
    list = resp["ReportDefinitions"]

    for definition in list:
        cur.delete_report_definition(ReportName=definition["ReportName"])


    # Get all the definitins again, which should be empty.
    resp = cur.describe_report_definitions()
    list = resp["ReportDefinitions"]

    assert list == []


# -- ModifyReportDefinition ----------------------------------------------------

    # base = _report_definition(f"{name_prefix}-{_uid()}")
    # modified = {**base, "S3Prefix": "xxx"}

    # assert modified["S3Prefix"] == "xxx"
    # assert all(modified[k] == v for k, v in base.items() if k != "S3Prefix")

def test_cur_modify_report_definition(cur):
    original_name = f"{name_prefix}-{_uid()}"
    original = _report_definition(original_name)

    # The second definition will have these items changed.
    modified_name = f"{original_name}-modified"
    modified = {
        **original,
        "ReportName": modified_name,
        "Compression": "ZIP",
        "S3Prefix": "new-prefix",
    }

    # Put both the original and modified definitions.
    for r in [original, modified]:
      cur.put_report_definition(ReportDefinition=r)

    # Retrieve all the definitions and make sure the two that we just put
    # actually exist.
    resp = cur.describe_report_definitions()
    created = [
        r for r in resp["ReportDefinitions"]
        if r["ReportName"].startswith(original_name)
    ]

    assert len(created) == 2
    assert {r["ReportName"] for r in created} == {original_name, modified_name}

    # Make sure the only the three test attributes were changed. Retrieve the
    # two definitions from the created list and then compare them attribute-by-
    # attribute.
    by_name = {r["ReportName"]: r for r in created}
    created_original = by_name[original_name]
    created_modified = by_name[modified_name]

    changed_keys = {"ReportName", "Compression", "S3Prefix"}

    # Confirm changed attributes match expected values.
    assert created_modified["ReportName"] == modified_name
    assert created_modified["Compression"] == "ZIP"
    assert created_modified["S3Prefix"] == "new-prefix"

    # Confirm every other attribute is unchanged.
    for k, v in created_original.items():
        if k in changed_keys:
            continue

        assert created_modified[k] == v


# -- PutReportDefinition -------------------------------------------------------


def test_cur_put_report_definition(cur):
    name = f"{name_prefix}-{_uid()}"

    resp = cur.put_report_definition(
        ReportDefinition=_report_definition(name)
    )

    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    list = cur.describe_report_definitions()["ReportDefinitions"]
    assert any(r["ReportName"] == name for r in list)


def test_cur_put_report_definition_duplicate_name(cur):
    name = f"{name_prefix}-{_uid()}"
    cur.put_report_definition(ReportDefinition=_report_definition(name))

    with pytest.raises(ClientError) as e:
        cur.put_report_definition(ReportDefinition=_report_definition(name))

    assert e.value.response["Error"]["Code"] == "DuplicateReportNameException"


def test_cur_put_report_definition_missing_report_name(cur):
    bad = _report_definition(f"{name_prefix}-{_uid()}")

    # This is an invalid report definition, but it is not caught by Boto3 like
    # the missing test below
    bad["ReportName"] = "   "

    with pytest.raises(ClientError) as e:
        cur.put_report_definition(ReportDefinition=bad)

    assert e.value.response["Error"]["Code"] == "ValidationException"


def test_cur_put_report_definition_missing_definition_body(cur):
    # Note that we're checking ParamValidationError as opposed to the more
    # typical ClientError. Boto3 is throwing this before the MiniStack client is
    # hit.
    with pytest.raises(ParamValidationError) as e:
        cur.put_report_definition(ReportDefinition={})

    assert "Missing required parameter in ReportDefinition" in str(e.value)


# -- TagResource ---------------------------------------------------------------


def test_cur_tag_resource(cur):
    name = f"{name_prefix}-{_uid()}"
    cur.put_report_definition(ReportDefinition=_report_definition(name))

    resp = cur.tag_resource(
        ReportName=name,
        Tags=test_tags,
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    tags = cur.list_tags_for_resource(ReportName=name)["Tags"]
    assert len(tags) == 2
    assert all(tag in tags for tag in test_tags)


def test_cur_tag_resource_not_found(cur):
    name = f"not-found-{_uid()}"

    with pytest.raises(ClientError) as e:
        cur.tag_resource(
            ReportName=name,
            Tags=[{"Key": "not", "Value": "found"}],
        )

    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_cur_tag_resource_overwrites(cur):
    name = f"{name_prefix}-{_uid()}"
    cur.put_report_definition(ReportDefinition=_report_definition(name))

    # Add initial tags.
    cur.tag_resource(
        ReportName=name,
        Tags=test_tags,
    )

    # Overwrite one tag, add another.
    cur.tag_resource(
        ReportName=name,
        Tags=[
            {"Key": "kids", "Value": "kpzzal"},
            {"Key": "size", "Value": "6"},
        ],
    )

    tags = cur.list_tags_for_resource(ReportName=name)["Tags"]
    assert {"Key": "kids", "Value": "kpzzal"} in tags
    assert {"Key": "dates", "Value": "091203060606"} in tags
    assert {"Key": "size", "Value": "6"} in tags
    # The original key/value should not exist.
    assert {"Key": "kids", "Value": "zalkpz"} not in tags


# -- UntagResource -------------------------------------------------------------


def test_cur_untag_resource(cur):
    name = f"{name_prefix}-{_uid()}"
    cur.put_report_definition(ReportDefinition=_report_definition(name))
    cur.tag_resource(ReportName=name, Tags=test_tags)

    resp = cur.untag_resource(ReportName=name, TagKeys=["kids"])
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    tags = cur.list_tags_for_resource(ReportName=name)["Tags"]
    assert {"Key": "kids", "Value": "zalkpz"} not in tags
    assert {"Key": "dates", "Value": "091203060606"} in tags


def test_cur_untag_resource_not_found(cur):
    name = f"not-found-{_uid()}"

    with pytest.raises(ClientError) as e:
        cur.untag_resource(ReportName=name, TagKeys=["kids"])

    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_cur_untag_resource_nonexistent_keys(cur):
    name = f"{name_prefix}-{_uid()}"
    cur.put_report_definition(ReportDefinition=_report_definition(name))
    cur.tag_resource(ReportName=name, Tags=test_tags)

    # Untag keys that don't exist - should succeed gracefully.
    resp = cur.untag_resource(ReportName=name, TagKeys=["nonexistent", "also-no"])
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    # Original tags should still be there.
    tags = cur.list_tags_for_resource(ReportName=name)["Tags"]
    assert len(tags) == 2
    assert all(tag in tags for tag in test_tags)

# -- Persistence ----------------------------------------------------------
#
# Regression for 1.3.36: the CUR module declared `get_state()` and
# `restore_state()` but the load_state() call at import time was missing,
# so MS wrote state on shutdown but never read it back on warm-boot.


def test_cur_state_round_trips_through_get_and_restore():
    from ministack.services import cur

    cur.reset()
    cur._report_definitions["r1"] = {
        "ReportName": "r1",
        "TimeUnit": "HOURLY",
        "Format": "Parquet",
        "S3Bucket": "billing",
    }
    cur._report_tags["r1"] = {"team": "finops"}

    snapshot = cur.get_state()
    cur.reset()
    assert "r1" not in cur._report_definitions

    cur.restore_state(snapshot)
    assert cur._report_definitions["r1"]["S3Bucket"] == "billing"
    assert cur._report_tags["r1"] == {"team": "finops"}


def test_cur_module_calls_load_state_on_import():
    """The bug we fixed: load_state was never invoked at import time, so
    every warm-boot lost CUR state. Verify the module exposes _restored
    (set by the import-time block whether or not anything was found)."""
    from ministack.services import cur

    assert hasattr(cur, "_restored")
