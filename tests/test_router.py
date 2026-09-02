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


# --- Step 5: host-header routing ------------------------------------------
#
# The ``host_patterns`` regexes are service tokens (``iot\.``, ``logs\.``,
# ``email\.`` ...). They are consulted only for hosts the stack actually
# serves, and each token has to sit at a label boundary. The expectations below
# were captured from the router *before* the guard existed, so they pin that
# AWS-shaped hosts route exactly as they always did.

# ``<token>.<suffix>`` -> service, for every ``host_patterns`` entry whose
# token can stand alone as the first label.
_TOKEN_ROUTES = {
    "account": "account", "acm": "acm", "airflow": "airflow", "aos": "opensearch",
    "apigateway": "apigateway", "appconfig": "appconfig",
    "appconfigdata": "appconfigdata", "appsync": "appsync",
    "appsync-api": "appsync-events", "appsync-realtime-api": "appsync-events",
    "athena": "athena", "autoscaling": "autoscaling", "backup": "backup",
    "batch": "batch", "bedrock": "bedrock", "bedrock-runtime": "bedrock-runtime",
    "cloudcontrolapi": "cloudcontrol", "cloudformation": "cloudformation",
    "cloudfront": "cloudfront", "cloudfront-kvs": "cloudfront-keyvaluestore",
    "cloudtrail": "cloudtrail", "codebuild": "codebuild",
    "cognito-identity": "cognito-identity", "cognito-idp": "cognito-idp",
    "config": "config", "cur": "cur", "dsql": "dsql", "dynamodb": "dynamodb",
    "ec2": "ec2", "ecr": "ecr", "ecs": "ecs", "eks": "eks",
    "elasticache": "elasticache", "elasticfilesystem": "elasticfilesystem",
    "elasticloadbalancing": "elasticloadbalancing",
    "elasticmapreduce": "elasticmapreduce", "email": "ses", "es": "opensearch",
    "events": "events", "execute-api": "apigateway", "firehose": "firehose",
    "glue": "glue", "iam": "iam", "inspector2": "inspector2", "iot": "iot",
    "kafka": "kafka", "kinesis": "kinesis", "kinesis-firehose": "firehose",
    "kms": "kms", "lambda": "lambda", "lambda-microvms": "lambda-microvms",
    "logs": "logs", "mediaconnect": "mediaconnect", "monitoring": "monitoring",
    "mq": "mq", "opensearch": "opensearch", "organizations": "organizations",
    "pipes": "pipes", "rds": "rds", "rds-data": "rds-data",
    "resource-groups": "resource-groups", "route53": "route53", "s3": "s3",
    "s3files": "s3files", "s3tables": "s3tables", "scheduler": "scheduler",
    "secretsmanager": "secretsmanager", "servicediscovery": "servicediscovery",
    "sns": "sns", "sqs": "sqs", "ssm": "ssm", "states": "states", "sts": "sts",
    "tagging": "tagging", "transfer": "transfer", "waf": "waf", "wafv2": "wafv2",
    "waf-regional": "waf-regional",
    # multi-label tokens
    "streams.dynamodb": "dynamodbstreams", "api.ecr": "ecr",
    "jobs.iot": "iot-jobs-data", "data-ats.iot": "iot-data", "data.iot": "iot-data",
    # tokens that are *not* a service by themselves — stay on the default
    "email-smtp": "s3",
}

_SERVED_SUFFIXES = (
    "eu-central-1.amazonaws.com",
    "eu-central-1.localhost.localstack.cloud",
    "localhost.localstack.cloud:4566",
    "us-east-1.localhost:4566",
    "localhost:4566",           # the short form: sts.localhost:4566
)

# A two-label alias (``s3.dev``, the LocalStack-era shape) is served as well;
# only the single-label tokens fit it — ``streams.dynamodb.dev`` has three
# labels and is a customer domain like any other.
_ALIAS_SUFFIX = "dev"

# Shapes that carry a resource id or a legacy/dualstack spelling in front of
# the token, plus a few AWS hosts no pattern claims (must stay on the default).
_EXPLICIT_HOST_ROUTES = [
    ("mybucket.s3.eu-central-1.amazonaws.com", "s3"),
    ("mybucket.s3.localhost.localstack.cloud:4566", "s3"),
    ("mybucket.s3-eu-west-1.amazonaws.com", "s3"),
    ("s3-eu-west-1.amazonaws.com", "s3"),
    ("mybucket.s3-website-us-east-1.amazonaws.com", "s3"),
    ("s3-fips.us-east-1.amazonaws.com", "s3"),
    ("s3.cn-north-1.amazonaws.com.cn", "s3"),
    ("sqs.cn-north-1.amazonaws.com.cn", "sqs"),
    ("abcd1234.execute-api.eu-central-1.amazonaws.com", "apigateway"),
    ("abcd1234.execute-api.localhost:4566", "apigateway"),
    ("abcd1234.execute-api.us-east-1.localhost.localstack.cloud:4566", "apigateway"),
    ("a1b2c3-ats.iot.eu-central-1.amazonaws.com", "iot"),
    ("a1b2c3-ats.iot.us-east-1.localhost:4566", "iot"),
    ("a1b2c3.credentials.iot.eu-central-1.amazonaws.com", "iot"),
    ("a1b2c3.data-ats.iot.eu-central-1.amazonaws.com", "iot-data"),
    ("a1b2c3.data.iot.eu-central-1.amazonaws.com", "iot-data"),
    ("a1b2c3.jobs.iot.eu-central-1.amazonaws.com", "iot-jobs-data"),
    ("a1b2c3.data.jobs.iot.eu-central-1.amazonaws.com", "iot-jobs-data"),
    ("lambda-microvms.localhost:4566", "lambda-microvms"),
    ("myfn.lambda-microvms.localhost.localstack.cloud:4566", "lambda-microvms"),
    ("123456789012.dkr.ecr.eu-central-1.amazonaws.com", "ecr"),
    ("abcd1234.appsync-api.eu-central-1.amazonaws.com", "appsync-events"),
    ("abcd1234.appsync-realtime-api.eu-central-1.amazonaws.com", "appsync-events"),
    ("waf.amazonaws.com", "waf"),
    ("sts.amazonaws.com", "sts"),
    ("iam.amazonaws.com", "iam"),
    ("route53.amazonaws.com", "route53"),
    ("cloudfront.amazonaws.com", "cloudfront"),
    ("search-mydomain-abc.eu-central-1.es.amazonaws.com", "s3"),
    ("vpc-mydomain.eu-central-1.es.amazonaws.com", "s3"),
    ("mydomain.auth.eu-central-1.amazoncognito.com", "s3"),
    ("queue.amazonaws.com", "s3"),
    ("localhost:4566", "s3"),
    ("localhost", "s3"),
    ("127.0.0.1:4566", "s3"),
    ("ministack:4566", "s3"),
    ("ministack-core:4566", "s3"),
    ("", "s3"),
]


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        (f"{token}.{suffix}", svc)
        for token, svc in _TOKEN_ROUTES.items()
        for suffix in _SERVED_SUFFIXES
    ]
    + [
        (f"{token}.{_ALIAS_SUFFIX}", svc)
        for token, svc in _TOKEN_ROUTES.items()
        if "." not in token
    ]
    + _EXPLICIT_HOST_ROUTES,
)
def test_aws_shaped_hosts_route_as_before(host, expected):
    assert detect_service("GET", "/status", {"host": host}, {}) == expected


@pytest.mark.parametrize("host", [
    "probe.iot.example.com",
    "logs.example.com",
    "email.corp.example",
    "status.lambda.example.com",
    "console.example.com",
    "s3.iot-example.local",              # not served until the operator says so
    "iot.example.com:4566",
])
def test_foreign_hosts_are_not_routed_by_service_token(host):
    """A customer domain is never an AWS endpoint: a Host the stack does not
    serve carries no routing information, however its labels are spelled."""
    assert detect_service("GET", "/status", {"host": host}, {}) == "s3"


@pytest.mark.parametrize(("host", "expected"), [
    ("s3.ministack:4566", "s3"),
    ("iot.localhost:4566", "iot"),
    ("sqs.dev", "sqs"),                # two labels: an alias, not a customer domain
    ("dynamodb.dev:4566", "dynamodb"),
    ("iot.127.0.0.1.nip.io", "s3"),    # a dotted name, not an IP literal
    ("logs.ministack.internal", "s3"),  # three labels, not served
])
def test_guard_boundaries(host, expected):
    assert detect_service("GET", "/status", {"host": host}, {}) == expected


@pytest.mark.parametrize("host", [
    "probe-iot.localhost:4566",         # token after a hyphen
    "notlogs.localhost:4566",           # token inside a label
    "myemail.localhost.localstack.cloud",
])
def test_served_host_tokens_match_only_at_label_start(host):
    assert detect_service("GET", "/status", {"host": host}, {}) == "s3"


def test_served_host_prefers_the_multi_label_token():
    # ``appconfig.`` used to also match ``config\.`` by substring; ordering
    # saved it. With anchored tokens the second pattern no longer fires.
    assert detect_service(
        "GET", "/status", {"host": "appconfig.us-east-1.localhost:4566"}, {}
    ) == "appconfig"


@pytest.mark.parametrize("host", [
    "ministack",
    "ministack-core:4566",
    "127.0.0.1:4566",
    "[::1]:4566",
])
def test_stack_hosts_without_service_labels_fall_to_default(host):
    assert detect_service("GET", "/status", {"host": host}, {}) == "s3"


def test_extra_host_suffixes_env_opens_pattern_routing(monkeypatch):
    monkeypatch.delenv("MINISTACK_EXTRA_HOST_SUFFIXES", raising=False)
    assert detect_service("GET", "/status", {"host": "s3.iot-example.local"}, {}) == "s3"
    assert detect_service("GET", "/status", {"host": "iot.iot-example.local"}, {}) == "s3"

    monkeypatch.setenv("MINISTACK_EXTRA_HOST_SUFFIXES", "corp.example, iot-example.local")
    assert detect_service("GET", "/status", {"host": "iot.iot-example.local"}, {}) == "iot"
    assert detect_service(
        "GET", "/status", {"host": "a1-ats.iot.eu-central-1.iot-example.local:4566"}, {}
    ) == "iot"
    assert detect_service("GET", "/status", {"host": "logs.corp.example"}, {}) == "logs"
    # label boundary: ``notiot-example.local`` is not under ``iot-example.local``
    assert detect_service("GET", "/status", {"host": "iot.notiot-example.local"}, {}) == "s3"


def test_ministack_host_env_is_a_served_suffix(monkeypatch):
    monkeypatch.setenv("MINISTACK_HOST", "aws.dev.example")
    assert detect_service("GET", "/status", {"host": "sqs.aws.dev.example:4566"}, {}) == "sqs"
    assert detect_service(
        "GET", "/status", {"host": "abcd1234.execute-api.aws.dev.example"}, {}
    ) == "apigateway"
    assert detect_service("GET", "/status", {"host": "sqs.other.example"}, {}) == "s3"


def test_container_hostname_is_a_served_suffix(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "core-7f3a")
    assert detect_service("GET", "/status", {"host": "sns.core-7f3a:4566"}, {}) == "sns"
