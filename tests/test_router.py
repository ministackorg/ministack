"""Unit tests for ministack.core.router.detect_service.

Pure routing-layer tests — no boto3, no live server. Covers the path-based
fallback (i.e. when neither X-Amz-Target nor a SigV4 credential scope is
available to disambiguate the service).
"""
import pytest

from ministack.core.router import detect_service

_HEADERS = {"host": "localhost:4566"}


@pytest.mark.parametrize("path", [
    # 2015-03-31 — Functions, ESM, Layers, Tags
    "/2015-03-31/functions/foo",
    "/2015-03-31/functions/foo/invocations",
    "/2015-03-31/functions/foo/aliases",
    "/2015-03-31/event-source-mappings",
    "/2015-03-31/event-source-mappings/abc-123",
    "/2015-03-31/layers/my-layer/versions",
    "/2015-03-31/tags/arn:aws:lambda:us-east-1:000000000000:function:foo",
    # 2016-08-19 — account-settings
    "/2016-08-19/account-settings",
    "/2016-08-19/account-settings/",
    # 2018-06-01 — runtime API (called unsigned by Lambda containers)
    "/2018-06-01/runtime/invocation/next",
    "/2018-06-01/runtime/invocation/abc/response",
    "/2018-06-01/runtime/invocation/abc/error",
    # 2018-10-31 — layers (alternate version)
    "/2018-10-31/layers/foo",
    # 2019-09-25 — EventInvokeConfig
    "/2019-09-25/functions/foo/event-invoke-config",
    "/2019-09-25/functions/foo/event-invoke-config/list",
    # 2019-09-30 — ProvisionedConcurrency
    "/2019-09-30/functions/foo/provisioned-concurrency",
    # 2020-04-22 — CodeSigningConfig
    "/2020-04-22/code-signing-configs/csc-abc",
    # 2021-10-31 — FunctionUrl
    "/2021-10-31/functions/foo/url",
])
def test_lambda_paths_route_to_lambda_unsigned(path):
    """Lambda API paths route to lambda even without a SigV4 Authorization header.

    boto3 always signs and the credential-scope check picks up `lambda`,
    but unsigned clients (raw HTTP, curl, the Lambda Runtime API itself)
    must still resolve via path.
    """
    assert detect_service("GET", path, _HEADERS, {}) == "lambda"


@pytest.mark.parametrize("path", [
    "/",
    "/mybucket/key",
    "/foo.txt",
    "/some-bucket/path/to/object",
])
def test_non_api_paths_fall_back_to_s3(path):
    """Plain object-style paths still default to S3 — fix doesn't widen Lambda routing."""
    assert detect_service("GET", path, _HEADERS, {}) == "s3"


@pytest.mark.parametrize("path", [
    "/2019-09-25/",                        # bare date prefix, no resource
    "/2019-09-25/something-else",          # unknown resource under valid date
    "/2013-04-01/restapis",                # apigateway date — should not be lambda
    "/abcd-ef-gh/functions",               # not a date
    "/functions/foo",                      # no date prefix
])
def test_non_lambda_dated_paths_dont_route_to_lambda(path):
    assert detect_service("GET", path, _HEADERS, {}) != "lambda"


def test_lambda_credential_scope_still_routes_when_path_unknown():
    """SigV4 with `lambda` scope wins regardless of path shape."""
    headers = {
        "host": "localhost:4566",
        "authorization": (
            "AWS4-HMAC-SHA256 "
            "Credential=test/20260428/us-east-1/lambda/aws4_request, "
            "SignedHeaders=host, Signature=fake"
        ),
    }
    assert detect_service("GET", "/2099-01-01/something-new", headers, {}) == "lambda"


@pytest.mark.parametrize(("method", "path"), [
    ("POST", "/2021-01-01/opensearch/domain/example/config"),
    ("GET", "/2021-01-01/opensearch/domain/example"),
    ("POST", "/2021-01-01/opensearch/domain-info"),
    ("GET", "/2021-01-01/domain/example"),
    ("GET", "/2021-01-01/versions"),
    ("GET", "/2021-01-01/compatibleVersions"),
    ("POST", "/2021-01-01/tags"),
    ("POST", "/2021-01-01/tags-removal"),
])
def test_opensearch_management_paths_route_without_sigv4(method, path):
    """OpenSearch custom-resource calls must not fall through to S3."""
    assert detect_service(method, path, _HEADERS, {}) == "opensearch"


def test_unknown_opensearch_version_path_still_falls_back_to_s3():
    assert detect_service(
        "POST", "/2021-01-01/not-opensearch/domain/example/config", _HEADERS, {}
    ) == "s3"


def _sigv4_headers(service):
    return {
        "host": "localhost:4566",
        "authorization": (
            "AWS4-HMAC-SHA256 "
            f"Credential=test/20260811/us-east-1/{service}/aws4_request, "
            "SignedHeaders=host, Signature=fake"
        ),
    }


def test_iot_jobs_data_credential_scope_routes():
    """The SDK signs iot-jobs-data requests with the `iot-jobs-data` scope
    (botocore signingName); the same path signed with `iot` must stay on the
    control plane — GET /things/{t}/jobs is GetPendingJobExecutions on one
    client and ListJobExecutionsForThing on the other."""
    assert detect_service(
        "GET", "/things/t1/jobs", _sigv4_headers("iot-jobs-data"), {}
    ) == "iot-jobs-data"
    assert detect_service("GET", "/things/t1/jobs", _sigv4_headers("iot"), {}) == "iot"


@pytest.mark.parametrize(
    "host",
    [
        # What DescribeEndpoint(endpointType="iot:Jobs") actually hands out —
        # a device following the documented flow signs against exactly this.
        "a1b2c3.jobs.iot.us-east-1.localhost:4566",
        # The spelling the AWS Device SDK's jobs documentation uses.
        "a1b2c3.data.jobs.iot.us-east-1.localhost:4566",
    ],
)
def test_iot_jobs_data_host_routes_before_iot(host):
    """Both jobs-endpoint spellings also match the `iot\\.` regex — the
    iot-jobs-data entry must win via pattern ordering, or the request lands on
    the control plane where GET /things/{t}/jobs is a different operation."""
    assert detect_service(
        "GET", "/things/t1/jobs/$next", {"host": host}, {}
    ) == "iot-jobs-data"
