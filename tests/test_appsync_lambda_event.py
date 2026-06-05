"""Tests for AppSync Lambda resolver event shape — verifies full AppSyncResolverEvent is built."""

import io
import json
import urllib.request
import zipfile

import pytest

ENDPOINT = "http://localhost:4566"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_zip(handler_code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", handler_code)
    return buf.getvalue()


def _create_lambda(lam, fn_name, handler_code):
    try:
        lam.delete_function(FunctionName=fn_name)
    except Exception:
        pass
    lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_zip(handler_code)},
    )
    return lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]


def _graphql_post(api_url, query, variables=None, headers=None):
    """Send a GraphQL POST request to the AppSync endpoint."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    req = urllib.request.Request(
        api_url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _setup_api_with_lambda_resolver(appsync, lam, fn_name, handler_code):
    """Create an AppSync API with API_KEY auth and a Lambda resolver for 'testField'."""
    fn_arn = _create_lambda(lam, fn_name, handler_code)

    api = appsync.create_graphql_api(
        name=f"test-api-{fn_name}",
        authenticationType="API_KEY",
    )
    api_id = api["graphqlApi"]["apiId"]
    api_key = appsync.create_api_key(apiId=api_id)["apiKey"]["id"]
    # Path-based endpoint: the central router maps only /v1/apis* to AppSync.
    # A bare /graphql POST from this client is routed to S3.
    graphql_url = f"{ENDPOINT}/v1/apis/{api_id}/graphql"

    appsync.create_data_source(
        apiId=api_id,
        name="LambdaDS",
        type="AWS_LAMBDA",
        lambdaConfig={"lambdaFunctionArn": fn_arn},
    )
    appsync.create_resolver(
        apiId=api_id,
        typeName="Query",
        fieldName="testField",
        dataSourceName="LambdaDS",
        kind="UNIT",
    )
    return api_id, api_key, graphql_url


# Resolver that simply reports whether event.identity was populated — used by the
# AWS_LAMBDA authorizer degradation tests below.
_IDENTITY_PROBE_RESOLVER = (
    "def handler(event, ctx):\n"
    "    return {'hasIdentity': event.get('identity') is not None}\n"
)


def _setup_lambda_auth_api(appsync, lam, name, authorizer_arn, resolver_code):
    """Create an AWS_LAMBDA-auth API with the given authorizer ARN and a Lambda resolver."""
    resolver_arn = _create_lambda(lam, f"{name}-resolver", resolver_code)

    api = appsync.create_graphql_api(
        name=name,
        authenticationType="AWS_LAMBDA",
        lambdaAuthorizerConfig={"authorizerUri": authorizer_arn},
    )
    api_id = api["graphqlApi"]["apiId"]
    graphql_url = f"{ENDPOINT}/v1/apis/{api_id}/graphql"

    appsync.create_data_source(
        apiId=api_id, name="LambdaDS", type="AWS_LAMBDA",
        lambdaConfig={"lambdaFunctionArn": resolver_arn},
    )
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="testField",
        dataSourceName="LambdaDS", kind="UNIT",
    )
    return api_id, graphql_url


# ── Test 1: info.fieldName is present in Lambda event ─────────────────────────

def test_appsync_lambda_event_field_name(appsync, lam):
    """Lambda event must contain info.fieldName matching the queried GraphQL field (AWS-standard shape)."""
    handler = (
        "def handler(event, ctx):\n"
        "    # Return the field-value dict directly: ministack places the handler's\n"
        "    # return value verbatim under data.<fieldName> (no body/data unwrap).\n"
        "    return {'fieldName': event.get('info', {}).get('fieldName')}\n"
    )
    api_id, api_key, url = _setup_api_with_lambda_resolver(appsync, lam, "field-name-test", handler)

    resp = _graphql_post(url, "{ testField { fieldName } }", headers={"x-api-key": api_key})

    assert "errors" not in resp
    assert resp["data"]["testField"]["fieldName"] == "testField"


# ── Test 2: arguments are passed correctly ────────────────────────────────────

def test_appsync_lambda_event_arguments(appsync, lam):
    """Lambda event must contain parsed arguments from the GraphQL query."""
    handler = (
        "def handler(event, ctx):\n"
        "    args = event.get('arguments', {})\n"
        "    return {'receivedId': args.get('params', {}).get('id', 'missing')}\n"
    )
    api_id, api_key, url = _setup_api_with_lambda_resolver(appsync, lam, "args-test", handler)

    resp = _graphql_post(
        url,
        'query { testField(params: {id: "issuer-abc"}) { receivedId } }',
        headers={"x-api-key": api_key},
    )

    assert "errors" not in resp
    assert resp["data"]["testField"]["receivedId"] == "issuer-abc"


# ── Test 3: request.headers.x-api-key is forwarded ───────────────────────────

def test_appsync_lambda_event_api_key_header(appsync, lam):
    """The x-api-key header must be in event.request.headers so isApiKeyAuthenticated() works."""
    handler = (
        "def handler(event, ctx):\n"
        "    headers = event.get('request', {}).get('headers', {})\n"
        "    return {'hasApiKey': 'x-api-key' in headers}\n"
    )
    api_id, api_key, url = _setup_api_with_lambda_resolver(appsync, lam, "api-key-header-test", handler)

    resp = _graphql_post(url, "{ testField { hasApiKey } }", headers={"x-api-key": api_key})

    assert "errors" not in resp
    assert resp["data"]["testField"]["hasApiKey"] is True


# ── Test 4: all custom headers forwarded ─────────────────────────────────────

def test_appsync_lambda_event_custom_headers_forwarded(appsync, lam):
    """x-request-id, x-session-id, x-user-id, x-workflow, x-process must be in event.request.headers."""
    handler = (
        "def handler(event, ctx):\n"
        "    headers = event.get('request', {}).get('headers', {})\n"
        "    return {\n"
        "        'requestId': headers.get('x-request-id', ''),\n"
        "        'sessionId': headers.get('x-session-id', ''),\n"
        "        'userId': headers.get('x-user-id', ''),\n"
        "        'workflow': headers.get('x-workflow', ''),\n"
        "        'process': headers.get('x-process', ''),\n"
        "    }\n"
    )
    api_id, api_key, url = _setup_api_with_lambda_resolver(appsync, lam, "custom-headers-test", handler)

    resp = _graphql_post(url, "{ testField { requestId sessionId userId workflow process } }", headers={
        "x-api-key": api_key,
        "x-request-id": "req-abc-123",
        "x-session-id": "sess-xyz-456",
        "x-user-id": "user-789",
        "x-workflow": "LOGIN",
        "x-process": "OTP_VERIFY",
    })

    assert "errors" not in resp
    data = resp["data"]["testField"]
    assert data["requestId"] == "req-abc-123"
    assert data["sessionId"] == "sess-xyz-456"
    assert data["userId"] == "user-789"
    assert data["workflow"] == "LOGIN"
    assert data["process"] == "OTP_VERIFY"


# ── Test 5: identity is absent in API_KEY mode ───────────────────────────────

def test_appsync_lambda_event_no_identity_in_api_key_mode(appsync, lam):
    """In API_KEY auth mode, event.identity must be absent or null."""
    handler = (
        "def handler(event, ctx):\n"
        "    return {'hasIdentity': event.get('identity') is not None}\n"
    )
    api_id, api_key, url = _setup_api_with_lambda_resolver(appsync, lam, "no-identity-test", handler)

    resp = _graphql_post(url, "{ testField { hasIdentity } }", headers={"x-api-key": api_key})

    assert "errors" not in resp
    assert resp["data"]["testField"]["hasIdentity"] is False


# ── Test 6: Lambda authorizer sets identity.resolverContext ──────────────────

def test_appsync_lambda_event_identity_from_authorizer(appsync, lam):
    """AWS_LAMBDA auth mode — identity.resolverContext from authorizer is in Lambda event."""
    authorizer_code = (
        "def handler(event, ctx):\n"
        "    return {\n"
        "        'isAuthorized': True,\n"
        "        'resolverContext': {\n"
        "            'customId': 'test-user-id',\n"
        "            'email': 'user@example.com',\n"
        "            'cognitoGroups': 'admin',\n"
        "        }\n"
        "    }\n"
    )
    resolver_code = (
        "def handler(event, ctx):\n"
        "    identity = event.get('identity') or {}\n"
        "    rc = identity.get('resolverContext') or {}\n"
        "    return {\n"
        "        'customId': rc.get('customId', ''),\n"
        "        'email': rc.get('email', ''),\n"
        "    }\n"
    )
    auth_arn = _create_lambda(lam, "lambda-authorizer-test", authorizer_code)
    resolver_arn = _create_lambda(lam, "lambda-resolver-test", resolver_code)

    api = appsync.create_graphql_api(
        name="lambda-auth-api",
        authenticationType="AWS_LAMBDA",
        lambdaAuthorizerConfig={"authorizerUri": auth_arn},
    )
    api_id = api["graphqlApi"]["apiId"]
    url = f"{ENDPOINT}/v1/apis/{api_id}/graphql"

    appsync.create_data_source(
        apiId=api_id, name="LambdaDS", type="AWS_LAMBDA",
        lambdaConfig={"lambdaFunctionArn": resolver_arn},
    )
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="testField",
        dataSourceName="LambdaDS", kind="UNIT",
    )

    resp = _graphql_post(url, "{ testField { customId email } }", headers={
        "Authorization": "Bearer fake-jwt-token",
    })

    assert "errors" not in resp
    assert resp["data"]["testField"]["customId"] == "test-user-id"
    assert resp["data"]["testField"]["email"] == "user@example.com"


# ── Test 7: Lambda not found — graceful fallback, no HTTP 500 ────────────────

def test_appsync_lambda_not_found_no_crash(appsync):
    """If Lambda function doesn't exist in ministack, AppSync returns a response (no crash)."""
    api = appsync.create_graphql_api(name="lambda-missing-api", authenticationType="API_KEY")
    api_id = api["graphqlApi"]["apiId"]
    api_key = appsync.create_api_key(apiId=api_id)["apiKey"]["id"]

    appsync.create_data_source(
        apiId=api_id, name="MissingLambdaDS", type="AWS_LAMBDA",
        lambdaConfig={"lambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:does-not-exist"},
    )
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="testField",
        dataSourceName="MissingLambdaDS", kind="UNIT",
    )

    # Should not raise an exception — returns a response (possibly with null data)
    resp = _graphql_post(f"{ENDPOINT}/v1/apis/{api_id}/graphql", "{ testField }", headers={"x-api-key": api_key})
    assert "data" in resp or "errors" in resp  # any response is OK — just no crash


# ── Test 8: Lambda returns errors array ──────────────────────────────────────

def test_appsync_lambda_returns_errors(appsync, lam):
    """Lambda returning {errors: ['INTERNAL_SERVER_ERROR']} is passed through correctly."""
    handler = (
        "def handler(event, ctx):\n"
        "    # The resolver's OWN error envelope, returned directly. ministack passes\n"
        "    # it through under data.testField. This is NOT a Lambda execution\n"
        "    # failure (see Test 11 for that path).\n"
        "    return {'errors': ['INTERNAL_SERVER_ERROR'], 'data': None}\n"
    )
    api_id, api_key, url = _setup_api_with_lambda_resolver(appsync, lam, "error-response-test", handler)

    resp = _graphql_post(url, "{ testField { id } }", headers={"x-api-key": api_key})

    # Errors should be surfaced, not cause HTTP 500
    assert resp["data"]["testField"]["errors"] == ["INTERNAL_SERVER_ERROR"]


# ── Test 9: source is empty dict for root fields ──────────────────────────────

def test_appsync_lambda_event_source_empty_for_root(appsync, lam):
    """event.source must be {} (empty) for top-level Query fields."""
    handler = (
        "def handler(event, ctx):\n"
        "    source = event.get('source')\n"
        "    return {'sourceIsEmpty': source == {} or source is None}\n"
    )
    api_id, api_key, url = _setup_api_with_lambda_resolver(appsync, lam, "source-test", handler)

    resp = _graphql_post(url, "{ testField { sourceIsEmpty } }", headers={"x-api-key": api_key})

    assert "errors" not in resp
    assert resp["data"]["testField"]["sourceIsEmpty"] is True


# ── Test 10: GraphQL variables are substituted before Lambda invocation ───────

def test_appsync_lambda_event_variables_substituted(appsync, lam):
    """Variables in the query must be resolved to values before the event is built."""
    handler = (
        "def handler(event, ctx):\n"
        "    args = event.get('arguments', {})\n"
        "    params = args.get('params', {})\n"
        "    # Should be 'issuer-from-var', not '$id'\n"
        "    return {'id': params.get('id', 'missing')}\n"
    )
    api_id, api_key, url = _setup_api_with_lambda_resolver(appsync, lam, "variables-test", handler)

    resp = _graphql_post(
        url,
        "query GetTest($id: ID!) { testField(params: {id: $id}) { id } }",
        variables={"id": "issuer-from-var"},
        headers={"x-api-key": api_key},
    )

    assert "errors" not in resp
    assert resp["data"]["testField"]["id"] == "issuer-from-var"


# ── Test 11: unhandled Lambda exception surfaces as a GraphQL error ───────────

def test_appsync_lambda_unhandled_exception_becomes_error(appsync, lam):
    """A Lambda that raises must yield a GraphQL `errors` entry, not fake `data`."""
    handler = (
        "def handler(event, ctx):\n"
        "    raise Exception('boom')\n"
    )
    api_id, api_key, url = _setup_api_with_lambda_resolver(appsync, lam, "raise-test", handler)

    resp = _graphql_post(url, "{ testField { id } }", headers={"x-api-key": api_key})

    # The RIE error payload (errorMessage/errorType) must NOT leak through as data.
    field = resp.get("data", {}).get("testField")
    assert field is None or "errorMessage" not in field
    # The failure is surfaced as an errors entry (top-level or under the field).
    assert "errors" in resp or (isinstance(field, dict) and "errors" in field)


# ── Test 12: authorizer rejection (isAuthorized:false) → identity null ────────

def test_appsync_lambda_authorizer_rejection_identity_null(appsync, lam):
    """AWS_LAMBDA auth — an authorizer returning isAuthorized:false must NOT block the
    request: it proceeds (HTTP 200) with event.identity null, and the rejecting
    authorizer's resolverContext must NOT leak into the resolver."""
    authorizer = (
        "def handler(event, ctx):\n"
        "    return {'isAuthorized': False, 'resolverContext': {'should': 'not-leak'}}\n"
    )
    auth_arn = _create_lambda(lam, "authz-reject-test", authorizer)
    _, url = _setup_lambda_auth_api(appsync, lam, "authz-reject-api", auth_arn, _IDENTITY_PROBE_RESOLVER)

    resp = _graphql_post(url, "{ testField { hasIdentity } }", headers={"Authorization": "Bearer fake-jwt"})

    assert "errors" not in resp
    assert resp["data"]["testField"]["hasIdentity"] is False


# ── Test 13: missing authorizer Lambda → graceful fallback ────────────────────

def test_appsync_lambda_missing_authorizer_degrades(appsync, lam):
    """AWS_LAMBDA auth — a lambdaAuthorizerConfig.authorizerUri pointing at a function
    that does not exist must degrade gracefully (HTTP 200, identity null), not crash."""
    _, url = _setup_lambda_auth_api(
        appsync, lam, "authz-missing-api",
        "arn:aws:lambda:us-east-1:000000000000:function:authorizer-does-not-exist",
        _IDENTITY_PROBE_RESOLVER,
    )

    resp = _graphql_post(url, "{ testField { hasIdentity } }", headers={"Authorization": "Bearer fake-jwt"})

    assert "errors" not in resp
    assert resp["data"]["testField"]["hasIdentity"] is False


# ── Test 14: failing (raising) authorizer Lambda → graceful fallback ──────────

def test_appsync_lambda_failing_authorizer_degrades(appsync, lam):
    """AWS_LAMBDA auth — an authorizer that raises must be caught (HTTP 200, identity
    null), not surface as an HTTP 500 / GraphQL error."""
    authorizer = (
        "def handler(event, ctx):\n"
        "    raise Exception('authorizer boom')\n"
    )
    auth_arn = _create_lambda(lam, "authz-raise-test", authorizer)
    _, url = _setup_lambda_auth_api(appsync, lam, "authz-raise-api", auth_arn, _IDENTITY_PROBE_RESOLVER)

    resp = _graphql_post(url, "{ testField { hasIdentity } }", headers={"Authorization": "Bearer fake-jwt"})

    assert "errors" not in resp
    assert resp["data"]["testField"]["hasIdentity"] is False
