import uuid

import pytest
from botocore.exceptions import ClientError, ParamValidationError


name_prefix = "cur-test"

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


# -- DeleteReportDefinition -------------------------------------------------------


def test_current_delete_report_definition(cur):
    name = f"{name_prefix}-{_uid()}"

    # First create one and then we'll immediately delete it.
    cur.put_report_definition(ReportDefinition=_report_definition(name))

    deleted = cur.delete_report_definition(ReportName=name)
    assert deleted["ResponseMetadata"]["HTTPStatusCode"] == 200

    reports = cur.describe_report_definitions()["ReportDefinitions"]
    assert all(r["ReportName"] != name for r in reports)


def test_current_delete_report_definition_not_found(cur):
    name = f"not-found-{_uid()}"

    with pytest.raises(ClientError) as e:
      cur.delete_report_definition(ReportName=name)

    assert e.value.response["Error"]["Code"] =="ValidationException"
    assert e.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


# -- DescribeReportDefinition -------------------------------------------------------


def test_cur_describe_report_definition(cur):
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