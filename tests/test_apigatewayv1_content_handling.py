"""
Regression tests for API Gateway v1 (REST API) ContentHandling fidelity.

Background
----------
The AWS REST API Gateway v1 spec defines a `contentHandling` field on
both Integration (`PutIntegration`) and IntegrationResponse
(`PutIntegrationResponse`), with valid values `CONVERT_TO_BINARY` /
`CONVERT_TO_TEXT`. Terraform's `aws_api_gateway_integration` and
`aws_api_gateway_integration_response` resources both expose this
field; without it the AWS provider cannot mark the resource as
matched and plans to re-set it on every apply.

Bugs (per the project audit):

  H-8  PutIntegration silently dropped `contentHandling` — same family
       as #439 which fixed it for v2 but never backported to v1.
  M-6  PutIntegrationResponse historically dropped `contentHandling`;
       turns out this was already added in `_put_integration_response`
       (commit 0ef45048). The regression tests below pin both paths.

Uses the session-scoped `apigw_v1` fixture from tests/conftest.py.
"""
import pytest


@pytest.fixture
def method_setup(apigw_v1):
    """Create a fresh REST API + resource + method as a foundation for
    integration tests. Yields (api_id, resource_id, http_method) and
    deletes the REST API in teardown so the session-scoped client
    doesn't leak state across tests."""
    api = apigw_v1.create_rest_api(name="ch-test-api")
    api_id = api["id"]
    root_id = apigw_v1.get_resources(restApiId=api_id)["items"][0]["id"]
    res = apigw_v1.create_resource(
        restApiId=api_id, parentId=root_id, pathPart="ch",
    )
    resource_id = res["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="POST",
        authorizationType="NONE",
    )
    apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="POST",
        statusCode="200",
    )
    try:
        yield api_id, resource_id, "POST"
    finally:
        try:
            apigw_v1.delete_rest_api(restApiId=api_id)
        except Exception:
            pass


# ── H-8: PutIntegration / GetIntegration round-trip ───────────────────

@pytest.mark.parametrize("ch_value", ["CONVERT_TO_TEXT", "CONVERT_TO_BINARY"])
def test_put_integration_persists_content_handling(apigw_v1, method_setup, ch_value):
    """PutIntegration accepting `contentHandling` must store the value
    so subsequent GetIntegration returns it. Without the fix, the field
    was silently dropped — breaking Terraform's
    `aws_api_gateway_integration.content_handling` round-trip."""
    api_id, resource_id, method = method_setup
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        type="HTTP",
        integrationHttpMethod="POST",
        uri="https://httpbin.org/anything",
        contentHandling=ch_value,
    )

    got = apigw_v1.get_integration(
        restApiId=api_id, resourceId=resource_id, httpMethod=method,
    )
    assert got.get("contentHandling") == ch_value, (
        f"PutIntegration silently dropped contentHandling={ch_value!r}; "
        "GetIntegration returned: " + repr(got.get("contentHandling"))
    )


def test_put_integration_omits_content_handling_when_not_set(apigw_v1, method_setup):
    """When the caller does NOT pass contentHandling, the response must
    not invent one. Real AWS omits the field; some boto3-driven
    Terraform plans diff against an emulator that returns an empty
    string or other default."""
    api_id, resource_id, method = method_setup
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        type="HTTP",
        integrationHttpMethod="POST",
        uri="https://httpbin.org/anything",
    )

    got = apigw_v1.get_integration(
        restApiId=api_id, resourceId=resource_id, httpMethod=method,
    )
    # Either the key is absent or its value is None/null (boto3 strips
    # null fields). Anything else (empty string, "NONE") would be a
    # fabricated value that misleads consumers.
    assert got.get("contentHandling") in (None, ), (
        "GetIntegration returned a fabricated contentHandling value "
        f"{got.get('contentHandling')!r} when none was set."
    )


def test_update_integration_can_patch_content_handling(apigw_v1, method_setup):
    """Terraform's apply path uses UpdateIntegration with a JSON Patch
    op (`replace /contentHandling`). The updated contentHandling value
    must persist and be returned by GetIntegration."""
    api_id, resource_id, method = method_setup
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        type="HTTP",
        integrationHttpMethod="POST",
        uri="https://httpbin.org/anything",
        contentHandling="CONVERT_TO_TEXT",
    )
    apigw_v1.update_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        patchOperations=[
            {"op": "replace", "path": "/contentHandling", "value": "CONVERT_TO_BINARY"},
        ],
    )

    got = apigw_v1.get_integration(
        restApiId=api_id, resourceId=resource_id, httpMethod=method,
    )
    assert got.get("contentHandling") == "CONVERT_TO_BINARY"


# ── M-6 regression lock: PutIntegrationResponse still works ───────────

@pytest.mark.parametrize("ch_value", ["CONVERT_TO_TEXT", "CONVERT_TO_BINARY"])
def test_put_integration_response_persists_content_handling(apigw_v1, method_setup, ch_value):
    """PutIntegrationResponse persisting `contentHandling` was already
    implemented in `_put_integration_response` (commit 0ef45048).
    This test pins that behaviour so a future refactor can't silently
    regress it (the audit's M-6 listed it as missing, which was wrong —
    keep it covered to make sure it stays right)."""
    api_id, resource_id, method = method_setup
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        type="HTTP",
        integrationHttpMethod="POST",
        uri="https://httpbin.org/anything",
    )
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        statusCode="200",
        contentHandling=ch_value,
    )

    got = apigw_v1.get_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod=method,
        statusCode="200",
    )
    assert got.get("contentHandling") == ch_value
