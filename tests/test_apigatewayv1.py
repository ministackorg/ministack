import asyncio
import io
import json
import os
import shutil
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
import yaml
from botocore.exceptions import ClientError

from ministack.core.responses import set_request_region
from ministack.services import apigateway_v1

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

_EXECUTE_PORT = urlparse(_endpoint).port or 4566

def test_apigwv1_create_rest_api(apigw_v1):
    """CreateRestApi returns id, name, and createdDate as datetime."""
    import datetime

    resp = apigw_v1.create_rest_api(name="v1-create-test")
    assert "id" in resp
    assert resp["name"] == "v1-create-test"
    assert "createdDate" in resp
    assert isinstance(resp["createdDate"], datetime.datetime), "createdDate must be a datetime, not a float"
    apigw_v1.delete_rest_api(restApiId=resp["id"])

def test_apigwv1_get_rest_api(apigw_v1):
    """GetRestApi returns the created API."""
    api_id = apigw_v1.create_rest_api(name="v1-get-test")["id"]
    resp = apigw_v1.get_rest_api(restApiId=api_id)
    assert resp["id"] == api_id
    assert resp["name"] == "v1-get-test"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_get_rest_apis(apigw_v1):
    """GetRestApis returns item list containing created APIs."""
    id1 = apigw_v1.create_rest_api(name="v1-list-a")["id"]
    id2 = apigw_v1.create_rest_api(name="v1-list-b")["id"]
    resp = apigw_v1.get_rest_apis()
    ids = [a["id"] for a in resp["items"]]
    assert id1 in ids
    assert id2 in ids
    apigw_v1.delete_rest_api(restApiId=id1)
    apigw_v1.delete_rest_api(restApiId=id2)

def test_apigwv1_get_account_defaults_and_update_roundtrip(apigw_v1):
    """GetAccount returns AWS-shaped defaults; UpdateAccount with /cloudwatchRoleArn
    patch op round-trips. Single test to avoid cross-test state bleed on the
    singleton per-account /account settings."""
    # Clear any prior state on the singleton (singleton per account, shared across tests)
    apigw_v1.update_account(patchOperations=[
        {"op": "replace", "path": "/cloudwatchRoleArn", "value": ""}
    ])

    resp = apigw_v1.get_account()
    assert resp["throttleSettings"] == {"burstLimit": 5000, "rateLimit": 10000}
    assert resp["features"] == ["UsagePlans"]
    assert resp["apiKeyVersion"] == "4"

    # Mirrors Terraform aws_api_gateway_account: single cloudwatchRoleArn replace.
    role_arn = "arn:aws:iam::000000000000:role/apigw-cloudwatch-test"
    apigw_v1.update_account(patchOperations=[
        {"op": "replace", "path": "/cloudwatchRoleArn", "value": role_arn}
    ])
    resp = apigw_v1.get_account()
    assert resp["cloudwatchRoleArn"] == role_arn
    # Defaults still present after the patch
    assert resp["throttleSettings"] == {"burstLimit": 5000, "rateLimit": 10000}


def test_apigwv1_account_settings_are_region_isolated():
    west_role = "arn:aws:iam::000000000000:role/apigw-cloudwatch-west"
    east_role = "arn:aws:iam::000000000000:role/apigw-cloudwatch-east"

    set_request_region("us-west-2")
    status, _headers, _body = apigateway_v1._update_account(
        {"patchOperations": [{"op": "replace", "path": "/cloudwatchRoleArn", "value": west_role}]}
    )
    assert status == 200

    set_request_region("us-east-1")
    status, _headers, body = apigateway_v1._get_account()
    assert status == 200
    assert json.loads(body)["cloudwatchRoleArn"] is None

    status, _headers, _body = apigateway_v1._update_account(
        {"patchOperations": [{"op": "replace", "path": "/cloudwatchRoleArn", "value": east_role}]}
    )
    assert status == 200

    set_request_region("us-west-2")
    status, _headers, body = apigateway_v1._get_account()
    assert status == 200
    assert json.loads(body)["cloudwatchRoleArn"] == west_role


def test_apigwv1_rest_api_policy_terraform_roundtrip(apigw_v1):
    """GetRestApi must return `policy` JSON-string-escape-encoded, matching
    real AWS. Terraform-provider-aws's flattenAPIPolicy wraps the SDK-decoded
    policy in outer quotes and re-parses as JSON; if ministack returns the
    raw policy (unescaped) the provider fails with
    ``invalid character 'S' after top-level value``. Regression for #430."""
    import urllib.request
    raw_policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"execute-api:Invoke","Resource":"*"}]}'
    api_id = apigw_v1.create_rest_api(name="v1-policy-roundtrip", policy=raw_policy)["id"]
    try:
        # What the AWS SDK v2 deserializer hands to the Terraform provider
        # is the outer-JSON-decoded string. Fetch it raw (bypass botocore
        # which may further manipulate the field).
        endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
        with urllib.request.urlopen(f"{endpoint}/restapis/{api_id}") as r:
            body = json.loads(r.read().decode())
        sdk_decoded_policy = body["policy"]

        # The Terraform provider does:
        #   NormalizeJsonString(`"` + *out.Policy + `"`) -> strconv.Unquote
        # which is equivalent to: json.loads('"' + policy + '"')
        recovered = json.loads('"' + sdk_decoded_policy + '"')
        assert recovered == raw_policy, f"provider roundtrip lost fidelity: {recovered!r} vs {raw_policy!r}"
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_update_rest_api(apigw_v1):
    """UpdateRestApi (PATCH) modifies the API name."""
    api_id = apigw_v1.create_rest_api(name="v1-update-before")["id"]
    apigw_v1.update_rest_api(
        restApiId=api_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-update-after"}],
    )
    resp = apigw_v1.get_rest_api(restApiId=api_id)
    assert resp["name"] == "v1-update-after"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_rest_api(apigw_v1):
    """DeleteRestApi removes the API; subsequent GetRestApi raises."""
    api_id = apigw_v1.create_rest_api(name="v1-delete-test")["id"]
    apigw_v1.delete_rest_api(restApiId=api_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_rest_api(restApiId=api_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_apigwv1_gateway_response_crud_resets_to_default(apigw_v1):
    """GatewayResponse customizations round-trip and deletion restores AWS defaults."""
    api_id = apigw_v1.create_rest_api(name="v1-gateway-response-test")["id"]
    response_type = "BAD_REQUEST_BODY"
    response_parameters = {
        "gatewayresponse.header.Access-Control-Allow-Origin": "'*'",
    }
    response_templates = {
        "application/json": '{"error":"$context.error.messageString"}',
    }

    try:
        default = apigw_v1.get_gateway_response(
            restApiId=api_id,
            responseType=response_type,
        )
        assert default["defaultResponse"] is True
        assert default["statusCode"] == "400"

        created = apigw_v1.put_gateway_response(
            restApiId=api_id,
            responseType=response_type,
            statusCode="422",
            responseParameters=response_parameters,
            responseTemplates=response_templates,
        )
        assert created["defaultResponse"] is False
        assert created["statusCode"] == "422"

        fetched = apigw_v1.get_gateway_response(
            restApiId=api_id,
            responseType=response_type,
        )
        assert fetched["responseParameters"] == response_parameters
        assert fetched["responseTemplates"] == response_templates

        listed = apigw_v1.get_gateway_responses(restApiId=api_id)["items"]
        listed_response = next(item for item in listed if item["responseType"] == response_type)
        assert listed_response["defaultResponse"] is False
        assert listed_response["statusCode"] == "422"

        apigw_v1.delete_gateway_response(
            restApiId=api_id,
            responseType=response_type,
        )
        reset_response = apigw_v1.get_gateway_response(
            restApiId=api_id,
            responseType=response_type,
        )
        assert reset_response["defaultResponse"] is True
        assert reset_response["statusCode"] == "400"
        assert reset_response["responseParameters"] == {}
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_gateway_response_state_survives_persistence_roundtrip():
    """Customized gateway responses participate in API Gateway v1 persistence."""
    from ministack.services import apigateway_v1 as service

    service.reset()
    try:
        _status, _headers, body = service._create_rest_api({"name": "persist-gateway-response"})
        api_id = json.loads(body)["id"]
        service._put_gateway_response(
            api_id,
            "BAD_REQUEST_BODY",
            {
                "statusCode": "422",
                "responseParameters": {"gatewayresponse.header.X-Test": "'yes'"},
                "responseTemplates": {"application/json": '{"persisted":true}'},
            },
        )

        snapshot = service.get_state()
        service.reset()
        service.load_persisted_state(snapshot)

        restored = service._gateway_responses[api_id]["BAD_REQUEST_BODY"]
        assert restored["statusCode"] == "422"
        assert restored["responseTemplates"] == {"application/json": '{"persisted":true}'}
    finally:
        service.reset()


def test_apigwv1_control_plane_resources_are_region_isolated():
    set_request_region("us-west-2")
    status, _headers, body = apigateway_v1._create_rest_api({"name": "regional-v1-api"})
    assert status == 201
    api_id = json.loads(body)["id"]
    root_id = next(
        resource["id"]
        for resource in apigateway_v1._resources[api_id].values()
        if resource["path"] == "/"
    )
    apigateway_v1._create_resource(api_id, root_id, {"pathPart": "west"})
    apigateway_v1._create_api_key({"name": "west-key"})
    apigateway_v1._create_usage_plan({"name": "west-plan"})
    apigateway_v1._create_domain_name({"domainName": "shared.example.com"})

    set_request_region("us-east-1")
    status, _headers, _body = apigateway_v1._get_rest_api(api_id)
    assert status == 404
    status, _headers, body = apigateway_v1._get_rest_apis({})
    assert status == 200
    assert api_id not in {api["id"] for api in json.loads(body)["item"]}
    status, _headers, _body = apigateway_v1._get_resources(api_id, {})
    assert status == 404
    assert json.loads(apigateway_v1._get_api_keys({})[2])["item"] == []
    assert json.loads(apigateway_v1._get_usage_plans({})[2])["item"] == []
    assert json.loads(apigateway_v1._get_domain_names({})[2])["item"] == []

    status, _headers, body = apigateway_v1._create_domain_name(
        {"domainName": "shared.example.com"}
    )
    assert status == 201
    assert json.loads(body)["regionalDomainName"].endswith(
        ".execute-api.us-east-1.amazonaws.com"
    )

    set_request_region("us-west-2")
    status, _headers, body = apigateway_v1._get_domain_name("shared.example.com")
    assert status == 200
    assert json.loads(body)["regionalDomainName"].endswith(
        ".execute-api.us-west-2.amazonaws.com"
    )


def test_apigwv1_execute_api_resolves_api_owner_region():
    set_request_region("us-west-2")
    status, _headers, body = apigateway_v1._create_rest_api({"name": "west-execute-api"})
    assert status == 201
    api_id = json.loads(body)["id"]
    root_id = next(
        resource["id"]
        for resource in apigateway_v1._resources[api_id].values()
        if resource["path"] == "/"
    )
    status, _headers, body = apigateway_v1._create_resource(
        api_id, root_id, {"pathPart": "mock"}
    )
    assert status == 201
    resource_id = json.loads(body)["id"]
    apigateway_v1._put_method(
        api_id, resource_id, "GET", {"authorizationType": "NONE"}
    )
    apigateway_v1._put_integration(
        api_id,
        resource_id,
        "GET",
        {
            "type": "MOCK",
            "requestTemplates": {"application/json": '{"statusCode": 200}'},
        },
    )
    apigateway_v1._put_method_response(api_id, resource_id, "GET", "200", {})
    apigateway_v1._put_integration_response(
        api_id,
        resource_id,
        "GET",
        "200",
        {"responseTemplates": {"application/json": '{"region":"west"}'}},
    )
    status, _headers, body = apigateway_v1._create_deployment(api_id, {})
    assert status == 201
    deployment_id = json.loads(body)["id"]
    apigateway_v1._create_stage(
        api_id, {"stageName": "prod", "deploymentId": deployment_id}
    )

    set_request_region("us-east-1")
    status, _headers, body = asyncio.run(
        apigateway_v1.handle_execute(api_id, "prod", "GET", "/mock", {}, b"", {})
    )
    assert status == 200
    assert json.loads(body)["region"] == "west"

    status, _headers, _body = asyncio.run(
        apigateway_v1.handle_execute("missing", "prod", "GET", "/mock", {}, b"", {})
    )
    assert status == 404


def test_apigwv1_documentation_part_crud(apigw_v1):
    """Documentation parts create, list, update, and delete through the SDK."""
    api_id = apigw_v1.create_rest_api(name="v1-documentation-part-test")["id"]
    created = apigw_v1.create_documentation_part(
        restApiId=api_id,
        location={"type": "RESOURCE", "path": "/pets"},
        properties='{"description":"Pet operations"}',
    )
    part_id = created["id"]

    try:
        fetched = apigw_v1.get_documentation_part(
            restApiId=api_id,
            documentationPartId=part_id,
        )
        assert fetched["location"] == {"type": "RESOURCE", "path": "/pets"}
        assert fetched["properties"] == '{"description":"Pet operations"}'

        listed = apigw_v1.get_documentation_parts(
            restApiId=api_id,
            type="RESOURCE",
            path="/pets",
        )["items"]
        assert [part["id"] for part in listed] == [part_id]

        updated = apigw_v1.update_documentation_part(
            restApiId=api_id,
            documentationPartId=part_id,
            patchOperations=[
                {
                    "op": "replace",
                    "path": "/properties",
                    "value": '{"description":"Updated pet operations"}',
                },
            ],
        )
        assert updated["id"] == part_id
        assert updated["properties"] == '{"description":"Updated pet operations"}'

        apigw_v1.delete_documentation_part(
            restApiId=api_id,
            documentationPartId=part_id,
        )
        with pytest.raises(ClientError) as exc:
            apigw_v1.get_documentation_part(
                restApiId=api_id,
                documentationPartId=part_id,
            )
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_documentation_part_state_survives_persistence_roundtrip():
    from ministack.services import apigateway_v1 as service

    service.reset()
    try:
        _status, _headers, body = service._create_rest_api({"name": "persist-doc-part"})
        api_id = json.loads(body)["id"]
        _status, _headers, body = service._create_documentation_part(
            api_id,
            {
                "location": {"type": "API"},
                "properties": '{"description":"Persisted"}',
            },
        )
        part_id = json.loads(body)["id"]

        snapshot = service.get_state()
        service.reset()
        service.load_persisted_state(snapshot)

        restored = service._documentation_parts[api_id][part_id]
        assert restored["location"] == {"type": "API"}
        assert restored["properties"] == '{"description":"Persisted"}'
    finally:
        service.reset()


def test_apigwv1_create_resource(apigw_v1):
    """CreateResource creates a child resource with computed path."""
    api_id = apigw_v1.create_rest_api(name="v1-resource-create")["id"]
    # Get root resource id
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resp = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="users",
    )
    assert resp["pathPart"] == "users"
    assert resp["path"] == "/users"
    assert "id" in resp
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_get_resources(apigw_v1):
    """GetResources returns the root resource plus any created children."""
    api_id = apigw_v1.create_rest_api(name="v1-get-resources")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.create_resource(restApiId=api_id, parentId=root["id"], pathPart="items")
    resources = apigw_v1.get_resources(restApiId=api_id)["items"]
    paths = [r["path"] for r in resources]
    assert "/" in paths
    assert "/items" in paths
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_put_get_method(apigw_v1):
    """PutMethod creates a method; GetMethod returns it."""
    api_id = apigw_v1.create_rest_api(name="v1-method-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="ping",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    resp = apigw_v1.get_method(restApiId=api_id, resourceId=resource_id, httpMethod="GET")
    assert resp["httpMethod"] == "GET"
    assert resp["authorizationType"] == "NONE"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_put_integration(apigw_v1):
    """PutIntegration sets AWS_PROXY integration on a method."""
    api_id = apigw_v1.create_rest_api(name="v1-integration-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="ping",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    resp = apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:myFunc/invocations",
    )
    assert resp["type"] == "AWS_PROXY"
    # Real AWS returns HTTP 201 Created for PutIntegration.
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_put_method_response(apigw_v1):
    """PutMethodResponse sets a 200 method response."""
    api_id = apigw_v1.create_rest_api(name="v1-method-response-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="things",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    resp = apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
    )
    assert resp["statusCode"] == "200"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_put_integration_response(apigw_v1):
    """PutIntegrationResponse sets a 200 integration response."""
    api_id = apigw_v1.create_rest_api(name="v1-int-response-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="things",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="MOCK",
        integrationHttpMethod="POST",
        uri="",
    )
    resp = apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
    )
    assert resp["statusCode"] == "200"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_create_deployment(apigw_v1):
    """CreateDeployment returns a deployment with id and createdDate."""
    api_id = apigw_v1.create_rest_api(name="v1-deployment-test")["id"]
    resp = apigw_v1.create_deployment(restApiId=api_id, description="initial deployment")
    assert "id" in resp
    assert "createdDate" in resp
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_create_stage(apigw_v1):
    """CreateStage creates a named stage linked to a deployment."""
    api_id = apigw_v1.create_rest_api(name="v1-stage-test")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    resp = apigw_v1.create_stage(
        restApiId=api_id,
        stageName="prod",
        deploymentId=dep_id,
    )
    assert resp["stageName"] == "prod"
    assert resp["deploymentId"] == dep_id
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_get_export_oas30_json(apigw_v1):
    """GetExport returns an OpenAPI 3 document instead of matching GetStage."""
    api_id = apigw_v1.create_rest_api(
        name="v1-export-oas30",
        description="Export regression test",
        version="2026-07-22",
    )["id"]
    try:
        apigw_v1.create_model(
            restApiId=api_id,
            name="Thing",
            contentType="application/json",
            schema=json.dumps({
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": ["id"],
            }),
        )
        root = next(
            resource
            for resource in apigw_v1.get_resources(restApiId=api_id)["items"]
            if resource["path"] == "/"
        )
        things_id = apigw_v1.create_resource(
            restApiId=api_id,
            parentId=root["id"],
            pathPart="things",
        )["id"]
        thing_id = apigw_v1.create_resource(
            restApiId=api_id,
            parentId=things_id,
            pathPart="{thingId}",
        )["id"]
        apigw_v1.put_method(
            restApiId=api_id,
            resourceId=thing_id,
            httpMethod="GET",
            authorizationType="NONE",
            operationName="GetThing",
            requestParameters={
                "method.request.path.thingId": True,
                "method.request.querystring.verbose": False,
            },
            requestModels={"application/json": "Thing"},
        )
        apigw_v1.put_method_response(
            restApiId=api_id,
            resourceId=thing_id,
            httpMethod="GET",
            statusCode="200",
            responseModels={"application/json": "Thing"},
            responseParameters={"method.response.header.X-Request-Id": False},
        )
        apigw_v1.put_integration(
            restApiId=api_id,
            resourceId=thing_id,
            httpMethod="GET",
            type="MOCK",
            integrationHttpMethod="POST",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        apigw_v1.put_integration_response(
            restApiId=api_id,
            resourceId=thing_id,
            httpMethod="GET",
            statusCode="200",
        )
        deployment_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
        apigw_v1.create_stage(
            restApiId=api_id,
            stageName="prod",
            deploymentId=deployment_id,
        )

        response = apigw_v1.get_export(
            restApiId=api_id,
            stageName="prod",
            exportType="oas30",
            accepts="application/json",
            parameters={"extensions": "integrations"},
        )
        document = json.loads(response["body"].read())

        assert response["contentType"] == "application/json"
        assert response["contentDisposition"].endswith('v1-export-oas30-prod-oas30.json"')
        assert document["openapi"] == "3.0.1"
        assert document["info"] == {
            "title": "v1-export-oas30",
            "version": "2026-07-22",
            "description": "Export regression test",
        }
        assert document["servers"] == [{
            "url": f"https://{api_id}.execute-api.us-east-1.amazonaws.com/prod"
        }]
        assert document["components"]["schemas"]["Thing"]["required"] == ["id"]

        operation = document["paths"]["/things/{thingId}"]["get"]
        assert operation["operationId"] == "GetThing"
        assert operation["requestBody"]["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/Thing"
        }
        assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/Thing"
        }
        assert operation["responses"]["200"]["headers"]["X-Request-Id"] == {
            "schema": {"type": "string"}
        }
        assert {parameter["name"] for parameter in operation["parameters"]} == {
            "thingId", "verbose"
        }
        assert operation["x-amazon-apigateway-integration"]["type"] == "mock"
        assert operation["x-amazon-apigateway-integration"]["responses"]["default"] == {
            "statusCode": "200"
        }
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigatewayv1_get_export_swagger_yaml(apigw_v1):
    """GetExport supports Swagger 2.0 and YAML response bodies."""
    api_id = apigw_v1.create_rest_api(name="v1-export-swagger")["id"]
    try:
        apigw_v1.create_model(
            restApiId=api_id,
            name="Result",
            contentType="application/json",
            schema='{"type":"object"}',
        )
        root = next(
            resource
            for resource in apigw_v1.get_resources(restApiId=api_id)["items"]
            if resource["path"] == "/"
        )
        apigw_v1.put_method(
            restApiId=api_id,
            resourceId=root["id"],
            httpMethod="ANY",
            authorizationType="NONE",
            apiKeyRequired=True,
            requestModels={"application/json": "Result"},
        )
        apigw_v1.put_method_response(
            restApiId=api_id,
            resourceId=root["id"],
            httpMethod="ANY",
            statusCode="200",
            responseModels={"application/json": "Result"},
        )
        deployment_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
        apigw_v1.create_stage(
            restApiId=api_id,
            stageName="local",
            deploymentId=deployment_id,
        )

        response = apigw_v1.get_export(
            restApiId=api_id,
            stageName="local",
            exportType="swagger",
            accepts="application/yaml",
        )
        document = yaml.safe_load(response["body"].read())

        assert response["contentType"] == "application/yaml"
        assert document["swagger"] == "2.0"
        assert document["host"] == f"{api_id}.execute-api.us-east-1.amazonaws.com"
        assert document["basePath"] == "/local"
        assert document["definitions"]["Result"] == {"type": "object"}
        assert document["securityDefinitions"]["api_key"] == {
            "type": "apiKey",
            "name": "x-api-key",
            "in": "header",
        }
        operation = document["paths"]["/"]["x-amazon-apigateway-any-method"]
        assert operation["security"] == [{"api_key": []}]
        assert operation["parameters"][0]["schema"] == {"$ref": "#/definitions/Result"}
        assert operation["responses"]["200"]["schema"] == {"$ref": "#/definitions/Result"}
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigatewayv1_get_export_validates_stage_and_format(apigw_v1):
    api_id = apigw_v1.create_rest_api(name="v1-export-validation")["id"]
    try:
        with pytest.raises(ClientError) as missing_stage:
            apigw_v1.get_export(
                restApiId=api_id,
                stageName="missing",
                exportType="oas30",
                accepts="application/json",
            )
        assert missing_stage.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

        deployment_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
        apigw_v1.create_stage(
            restApiId=api_id,
            stageName="prod",
            deploymentId=deployment_id,
        )
        empty_export = apigw_v1.get_export(
            restApiId=api_id,
            stageName="prod",
            exportType="oas30",
            accepts="application/json",
        )
        assert json.loads(empty_export["body"].read())["components"]["schemas"] == {}

        with pytest.raises(ClientError) as invalid_export_type:
            apigw_v1.get_export(
                restApiId=api_id,
                stageName="prod",
                exportType="invalid",
                accepts="application/json",
            )
        assert invalid_export_type.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

        with pytest.raises(ClientError) as invalid_accept:
            apigw_v1.get_export(
                restApiId=api_id,
                stageName="prod",
                exportType="oas30",
                accepts="text/plain",
            )
        assert invalid_accept.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_update_stage(apigw_v1):
    """UpdateStage (PATCH) updates stage variables."""
    api_id = apigw_v1.create_rest_api(name="v1-stage-update")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="dev", deploymentId=dep_id)
    apigw_v1.update_stage(
        restApiId=api_id,
        stageName="dev",
        patchOperations=[
            {"op": "replace", "path": "/variables/myVar", "value": "myVal"},
            {"op": "replace", "path": "/tracingEnabled", "value": "true"},
        ],
    )
    resp = apigw_v1.get_stage(restApiId=api_id, stageName="dev")
    assert resp["variables"]["myVar"] == "myVal"
    assert resp["tracingEnabled"] is True
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_update_stage_method_settings_wildcard(apigw_v1):
    """UpdateStage paths like ``/*/*/metrics/enabled`` map to ``methodSettings['*/*']`` (Terraform)."""
    api_id = apigw_v1.create_rest_api(name="v1-method-settings")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="local", deploymentId=dep_id)
    apigw_v1.update_stage(
        restApiId=api_id,
        stageName="local",
        patchOperations=[
            {"op": "replace", "path": "/*/*/metrics/enabled", "value": "true"},
            {"op": "replace", "path": "/*/*/logging/loglevel", "value": "INFO"},
        ],
    )
    stage = apigw_v1.get_stage(restApiId=api_id, stageName="local")
    assert "*/*" in stage.get("methodSettings", {})
    ms = stage["methodSettings"]["*/*"]
    assert ms["metricsEnabled"] is True
    assert ms["loggingLevel"] == "INFO"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_authorizer_crud(apigw_v1):
    """Authorizer full lifecycle: create, get, update (patch), delete."""
    api_id = apigw_v1.create_rest_api(name="v1-auth-crud")["id"]
    auth = apigw_v1.create_authorizer(
        restApiId=api_id,
        name="my-auth",
        type="TOKEN",
        authorizerUri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:auth/invocations",
        identitySource="method.request.header.Authorization",
    )
    auth_id = auth["id"]
    assert auth["name"] == "my-auth"

    got = apigw_v1.get_authorizer(restApiId=api_id, authorizerId=auth_id)
    assert got["id"] == auth_id

    apigw_v1.update_authorizer(
        restApiId=api_id,
        authorizerId=auth_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "renamed-auth"}],
    )
    got2 = apigw_v1.get_authorizer(restApiId=api_id, authorizerId=auth_id)
    assert got2["name"] == "renamed-auth"

    listed = apigw_v1.get_authorizers(restApiId=api_id)["items"]
    assert any(a["id"] == auth_id for a in listed)

    apigw_v1.delete_authorizer(restApiId=api_id, authorizerId=auth_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_authorizer(restApiId=api_id, authorizerId=auth_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_model_crud(apigw_v1):
    """CreateModel, GetModel, DeleteModel lifecycle."""
    api_id = apigw_v1.create_rest_api(name="v1-model-crud")["id"]
    resp = apigw_v1.create_model(
        restApiId=api_id,
        name="MyModel",
        contentType="application/json",
        schema='{"type": "object"}',
    )
    assert resp["name"] == "MyModel"

    got = apigw_v1.get_model(restApiId=api_id, modelName="MyModel")
    assert got["name"] == "MyModel"

    listed = apigw_v1.get_models(restApiId=api_id)["items"]
    assert any(m["name"] == "MyModel" for m in listed)

    apigw_v1.delete_model(restApiId=api_id, modelName="MyModel")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_model(restApiId=api_id, modelName="MyModel")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_update_model(apigw_v1):
    """UpdateModel applies patchOperations. Terraform aws_api_gateway_model
    issues PATCH on description / schema changes; the dispatcher previously
    fell through to 404."""
    api_id = apigw_v1.create_rest_api(name="v1-update-model")["id"]
    apigw_v1.create_model(
        restApiId=api_id,
        name="PatchMe",
        description="initial",
        contentType="application/json",
        schema='{"type": "object"}',
    )
    apigw_v1.update_model(
        restApiId=api_id,
        modelName="PatchMe",
        patchOperations=[{"op": "replace", "path": "/description", "value": "updated"}],
    )
    got = apigw_v1.get_model(restApiId=api_id, modelName="PatchMe")
    assert got["description"] == "updated"
    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_tags(apigw_v1):
    """TagResource, GetTags, UntagResource."""
    api_id = apigw_v1.create_rest_api(name="v1-tags-test")["id"]
    arn = f"arn:aws:apigateway:us-east-1::/restapis/{api_id}"

    apigw_v1.tag_resource(resourceArn=arn, tags={"env": "test", "team": "platform"})
    resp = apigw_v1.get_tags(resourceArn=arn)
    assert resp["tags"]["env"] == "test"
    assert resp["tags"]["team"] == "platform"

    apigw_v1.untag_resource(resourceArn=arn, tagKeys=["env"])
    resp2 = apigw_v1.get_tags(resourceArn=arn)
    assert "env" not in resp2["tags"]
    assert resp2["tags"]["team"] == "platform"

    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_apikey_crud(apigw_v1):
    """ApiKey full lifecycle: create, get, delete."""
    resp = apigw_v1.create_api_key(name="v1-test-key", enabled=True)
    key_id = resp["id"]
    assert resp["name"] == "v1-test-key"
    assert "value" in resp

    got = apigw_v1.get_api_key(apiKey=key_id, includeValue=True)
    assert got["id"] == key_id

    listed = apigw_v1.get_api_keys()["items"]
    assert any(k["id"] == key_id for k in listed)

    apigw_v1.delete_api_key(apiKey=key_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_api_key(apiKey=key_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_apigwv1_usage_plan_crud(apigw_v1):
    """UsagePlan full lifecycle: create, get, delete."""
    resp = apigw_v1.create_usage_plan(
        name="v1-plan",
        throttle={"rateLimit": 100, "burstLimit": 200},
        quota={"limit": 10000, "period": "MONTH"},
    )
    plan_id = resp["id"]
    assert resp["name"] == "v1-plan"

    got = apigw_v1.get_usage_plan(usagePlanId=plan_id)
    assert got["id"] == plan_id

    listed = apigw_v1.get_usage_plans()["items"]
    assert any(p["id"] == plan_id for p in listed)

    apigw_v1.delete_usage_plan(usagePlanId=plan_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_usage_plan(usagePlanId=plan_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_apigwv1_execute_lambda_proxy(apigw_v1, lam):
    """End-to-end: create API + resource + method + integration + deploy + invoke Lambda."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-proxy-{_uuid.uuid4().hex[:8]}"
    code = b"import json\ndef handler(event, context):\n    return {'statusCode': 200, 'body': 'pong'}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-exec-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="ping",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/ping"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    body = resp.read()
    assert body == b"pong"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_token_authorizer(apigw_v1, lam):
    """A CUSTOM TOKEN authorizer is invoked on the data path and enforced. (#1345)

    - No identity-source header  -> 401 without invoking the authorizer.
    - Deny policy                -> 403.
    - Authorizer raises Unauthorized -> 401.
    - Allow policy               -> 200, with the returned context (values
      stringified) plus principalId injected into requestContext.authorizer.
    """
    import urllib.request as _urlreq
    import urllib.error as _urlerr
    import uuid as _uuid

    suffix = _uuid.uuid4().hex[:8]
    auth_fn = f"intg-v1-authfn-{suffix}"
    int_fn = f"intg-v1-intfn-{suffix}"

    auth_code = (
        b"def handler(event, context):\n"
        b"    tok = event.get('authorizationToken')\n"
        b"    arn = event['methodArn']\n"
        b"    if tok == 'deny-me':\n"
        b"        return _p('alice', 'Deny', arn, {})\n"
        b"    if tok == 'unauth':\n"
        b"        raise Exception('Unauthorized')\n"
        b"    return _p('alice', 'Allow', arn, {'user': 'alice', 'admin': True, 'count': 3})\n"
        b"def _p(pid, effect, arn, ctx):\n"
        b"    return {'principalId': pid, 'context': ctx, 'policyDocument': {\n"
        b"        'Version': '2012-10-17', 'Statement': [\n"
        b"            {'Action': 'execute-api:Invoke', 'Effect': effect, 'Resource': arn}]}}\n"
    )
    int_code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    auth = event.get('requestContext', {}).get('authorizer')\n"
        b"    return {'statusCode': 200, 'body': json.dumps(auth)}\n"
    )

    def _mkfn(name, code):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("index.py", code)
        lam.create_function(
            FunctionName=name, Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler", Code={"ZipFile": buf.getvalue()},
        )

    _mkfn(auth_fn, auth_code)
    _mkfn(int_fn, int_code)

    api_id = apigw_v1.create_rest_api(name=f"v1-authz-{suffix}")["id"]
    try:
        root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
        resource_id = apigw_v1.create_resource(
            restApiId=api_id, parentId=root["id"], pathPart="private",
        )["id"]

        auth_uri = (
            f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
            f"arn:aws:lambda:us-east-1:000000000000:function:{auth_fn}/invocations"
        )
        authorizer_id = apigw_v1.create_authorizer(
            restApiId=api_id, name="tok-auth", type="TOKEN",
            authorizerUri=auth_uri,
            identitySource="method.request.header.Authorization",
            authorizerResultTtlInSeconds=0,
        )["id"]

        apigw_v1.put_method(
            restApiId=api_id, resourceId=resource_id, httpMethod="GET",
            authorizationType="CUSTOM", authorizerId=authorizer_id,
        )
        apigw_v1.put_integration(
            restApiId=api_id, resourceId=resource_id, httpMethod="GET",
            type="AWS_PROXY", integrationHttpMethod="POST",
            uri=(
                f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
                f"arn:aws:lambda:us-east-1:000000000000:function:{int_fn}/invocations"
            ),
        )
        dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
        apigw_v1.create_stage(restApiId=api_id, stageName="dev", deploymentId=dep_id)

        host = f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"
        url = f"http://{host}/dev/private"

        def _call(token):
            req = _urlreq.Request(url, method="GET")
            req.add_header("Host", host)
            if token is not None:
                req.add_header("Authorization", token)
            try:
                r = _urlreq.urlopen(req)
                return r.status, r.read()
            except _urlerr.HTTPError as e:
                return e.code, e.read()

        # No Authorization header -> 401, authorizer not invoked.
        status, _ = _call(None)
        assert status == 401

        # Deny policy -> 403.
        status, _ = _call("deny-me")
        assert status == 403

        # Authorizer raises "Unauthorized" -> 401.
        status, _ = _call("unauth")
        assert status == 401

        # Allow policy -> 200; context stringified + principalId injected.
        status, body = _call("allow-me")
        assert status == 200
        auth = json.loads(body)
        assert auth is not None
        assert auth["principalId"] == "alice"
        assert auth["user"] == "alice"
        assert auth["admin"] == "true"
        assert auth["count"] == "3"
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)
        lam.delete_function(FunctionName=auth_fn)
        lam.delete_function(FunctionName=int_fn)


def test_apigwv1_execute_aws_iam_requires_auth_header(apigw_v1, lam):
    """AWS_IAM methods reject requests without an Authorization header (403). (#1345)

    We do not verify the SigV4 signature; only the presence check is enforced.
    """
    import urllib.request as _urlreq
    import urllib.error as _urlerr
    import uuid as _uuid

    fname = f"intg-v1-iam-{_uuid.uuid4().hex[:8]}"
    code = b"def handler(event, context):\n    return {'statusCode': 200, 'body': 'ok'}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler", Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-iam-{fname}")["id"]
    try:
        root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
        resource_id = apigw_v1.create_resource(
            restApiId=api_id, parentId=root["id"], pathPart="secure",
        )["id"]
        apigw_v1.put_method(
            restApiId=api_id, resourceId=resource_id, httpMethod="GET",
            authorizationType="AWS_IAM",
        )
        apigw_v1.put_integration(
            restApiId=api_id, resourceId=resource_id, httpMethod="GET",
            type="AWS_PROXY", integrationHttpMethod="POST",
            uri=(
                f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
                f"arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations"
            ),
        )
        dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
        apigw_v1.create_stage(restApiId=api_id, stageName="dev", deploymentId=dep_id)

        host = f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"
        url = f"http://{host}/dev/secure"

        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", host)
        try:
            r = _urlreq.urlopen(req)
            status = r.status
        except _urlerr.HTTPError as e:
            status = e.code
        assert status == 403

        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", host)
        req.add_header("Authorization", "AWS4-HMAC-SHA256 Credential=...")
        r = _urlreq.urlopen(req)
        assert r.status == 200
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_lambda_proxy_multi_value_headers(apigw_v1, lam):
    """Payload format 1.0 `multiValueHeaders` yields one header line per value.

    Real APIGW v1 carries multi-value headers (notably Set-Cookie) in
    `multiValueHeaders`; each value must reach the wire as a separate header.
    """
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-mvh-{_uuid.uuid4().hex[:8]}"
    code = (
        b"def handler(event, context):\n"
        b"    return {\n"
        b"        'statusCode': 200,\n"
        b"        'multiValueHeaders': {'Set-Cookie': ['a=1; Path=/', 'b=2; Path=/']},\n"
        b"        'body': 'ok',\n"
        b"    }\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-mvh-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="cookie",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/cookie"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert (resp.headers.get_all("Set-Cookie") or []) == ["a=1; Path=/", "b=2; Path=/"]

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_lambda_proxy_mvh_wins_case_insensitive(apigw_v1, lam):
    """Case-insensitive collision between `headers` and `multiValueHeaders`.

    Per AWS docs: "If the same key-value pair is specified in both, only the
    values from `multiValueHeaders` will appear in the merged list." HTTP
    headers are case-insensitive, so `Set-Cookie` in `headers` plus
    `set-cookie` in `multiValueHeaders` must NOT both ship — only the MVH
    values win.
    """
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-mvh-case-{_uuid.uuid4().hex[:8]}"
    code = (
        b"def handler(event, context):\n"
        b"    return {\n"
        b"        'statusCode': 200,\n"
        b"        'headers': {'Set-Cookie': 'should-be-dropped=1; Path=/'},\n"
        b"        'multiValueHeaders': {'set-cookie': ['a=1; Path=/', 'b=2; Path=/']},\n"
        b"        'body': 'ok',\n"
        b"    }\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-mvh-case-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="cookie",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/cookie"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    set_cookies = resp.headers.get_all("Set-Cookie") or []
    # Only MVH values ship; the scalar `Set-Cookie` from `headers` is dropped.
    assert set_cookies == ["a=1; Path=/", "b=2; Path=/"]
    # And the scalar value is not somewhere else in the response.
    assert all("should-be-dropped" not in c for c in set_cookies)

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_lambda_proxy_header_case_override(apigw_v1, lam):
    """A lowercase `content-type` overrides the default, not duplicates it.

    Follow-up to #750: the multiValueHeaders merge there case-folds collisions,
    but a plain-`headers` `content-type` vs the seeded default `Content-Type`
    was still emitted twice. HTTP field names are case-insensitive (RFC 9110
    §5.1), so the function's header must win as a single header.
    """
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-ctcase-{_uuid.uuid4().hex[:8]}"
    code = (
        b"def handler(event, context):\n"
        b"    return {\n"
        b"        'statusCode': 200,\n"
        b"        'headers': {'content-type': 'text/plain'},\n"
        b"        'body': 'ok',\n"
        b"    }\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-ctcase-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="ct",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/ct"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    # Exactly one Content-Type, and it's the Lambda's value (not the default).
    assert (resp.headers.get_all("Content-Type") or []) == ["text/plain"]

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


def _deploy_custom_integration_api(
    apigw_v1, lam, fname, code, path_part,
    *, integration_type="AWS", with_integration_response=True,
):
    """Deploy a REST API whose GET method integrates a freshly created Lambda.

    Defaults to the non-proxy `AWS` integration with the default (empty
    selectionPattern) integration response. `integration_type="AWS_PROXY"` and
    `with_integration_response=False` cover the two contrasting cases.
    Returns (api_id, url)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-custom-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart=path_part,
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type=integration_type,
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    if with_integration_response:
        apigw_v1.put_integration_response(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            statusCode="200",
            selectionPattern="",
        )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/{path_part}"
    return api_id, url


def test_apigwv1_execute_lambda_custom_returns_raw_output(apigw_v1, lam):
    """A non-proxy (custom) `AWS` integration returns the handler output verbatim.

    Unlike AWS_PROXY there is no `{statusCode, headers, body}` envelope to
    interpret: the return value IS the response body, serialized as JSON, and
    the status comes from the integration response (200). A handler that
    happens to return a `statusCode` key ships that key to the client inside
    the body — it must NOT be promoted to the HTTP status.
    """
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-custom-raw-{_uuid.uuid4().hex[:8]}"
    code = (
        b"def handler(event, context):\n"
        b"    return {'statusCode': 418, 'body': 'x', 'nested': {'ok': True}}\n"
    )
    api_id, url = _deploy_custom_integration_api(apigw_v1, lam, fname, code, "custom")
    try:
        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        resp = _urlreq.urlopen(req)
        # The handler's `statusCode` is data, not the HTTP status.
        assert resp.status == 200
        assert resp.headers.get("Content-Type") == "application/json"
        assert json.loads(resp.read()) == {"statusCode": 418, "body": "x", "nested": {"ok": True}}
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_lambda_custom_function_error_passes_through_200(apigw_v1, lam):
    """A standard Lambda error on a non-proxy integration is returned through the
    default (200) integration response with the raw error document as the body.

    AWS only maps a Lambda error to a 5xx when an integration-response
    selectionPattern matches it; with none configured — the only shape modeled —
    the error is "returned as 200 OK by default"
    (https://docs.aws.amazon.com/apigateway/latest/developerguide/handle-errors-in-lambda-integration.html)."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-custom-err-{_uuid.uuid4().hex[:8]}"
    code = (
        b"def handler(event, context):\n"
        b"    raise RuntimeError('kaboom')\n"
    )
    api_id, url = _deploy_custom_integration_api(apigw_v1, lam, fname, code, "boom")
    try:
        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        resp = _urlreq.urlopen(req)
        assert resp.status == 200
        body = json.loads(resp.read())
        # The standard Lambda error document is passed through as the body,
        # rather than being hidden behind a generic gateway message.
        assert body.get("errorMessage") == "kaboom"
        assert "errorType" in body
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_lambda_custom_without_integration_responses_is_200(
    apigw_v1, lam
):
    """A method with no integrationResponses at all still answers 200.

    put_integration_response is optional, and a method that never called it has
    nothing to select a status from. AWS answers 200; the status lookup must
    fall back rather than fail or invent one from the payload.
    """
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-custom-noresp-{_uuid.uuid4().hex[:8]}"
    code = (
        b"def handler(event, context):\n"
        b"    return {'statusCode': 418, 'body': 'x'}\n"
    )
    api_id, url = _deploy_custom_integration_api(
        apigw_v1, lam, fname, code, "noresp", with_integration_response=False,
    )
    try:
        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        resp = _urlreq.urlopen(req)
        assert resp.status == 200
        assert json.loads(resp.read()) == {"statusCode": 418, "body": "x"}
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_lambda_proxy_envelope_still_interpreted(apigw_v1, lam):
    """The AWS_PROXY contract is untouched by the non-proxy split.

    The same handler payload that a custom integration must return verbatim is
    a response envelope here: `statusCode` becomes the HTTP status and `body`
    becomes the body. Pinned against the identical handler so a future change
    to the shared event builder or the type dispatch cannot quietly swap the
    two contracts.
    """
    import urllib.error as _urlerr
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-proxy-pin-{_uuid.uuid4().hex[:8]}"
    code = (
        b"def handler(event, context):\n"
        b"    return {'statusCode': 418, 'body': 'x'}\n"
    )
    api_id, url = _deploy_custom_integration_api(
        apigw_v1, lam, fname, code, "proxypin", integration_type="AWS_PROXY",
    )
    try:
        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        with pytest.raises(_urlerr.HTTPError) as exc:
            _urlreq.urlopen(req)
        assert exc.value.code == 418
        assert exc.value.read() == b"x"
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_lambda_proxy_binary_media_types(apigw_v1, lam):
    """REST API (v1) binary support keyed off `binaryMediaTypes`.

    Verified against real AWS: a request body whose Content-Type matches a
    configured binaryMediaType is delivered base64 (isBase64Encoded=true); a
    base64 response body is decoded only when the request Accept also matches.
    """
    import base64 as _b64
    import hashlib
    import json as _json
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-binmedia-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json, base64, hashlib\n"
        b"def handler(event, context):\n"
        b"    qs = event.get('queryStringParameters') or {}\n"
        b"    if qs.get('mode') == 'resp':\n"
        b"        return {'statusCode': 200, 'isBase64Encoded': True,\n"
        b"                'headers': {'Content-Type': 'application/octet-stream'},\n"
        b"                'body': base64.b64encode(bytes(range(8))).decode('ascii')}\n"
        b"    b = event.get('body'); isb = bool(event.get('isBase64Encoded'))\n"
        b"    if b is None: raw = b''\n"
        b"    elif isb: raw = base64.b64decode(b)\n"
        b"    else: raw = b.encode('utf-8', 'surrogateescape')\n"
        b"    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},\n"
        b"            'body': json.dumps({'isB64': isb, 'sha': hashlib.sha256(raw).hexdigest()})}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler", Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(
        name=f"v1-binmedia-{fname}", binaryMediaTypes=["application/octet-stream"],
    )["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id, parentId=root["id"], pathPart="echo",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id, resourceId=resource_id, httpMethod="ANY", authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id, resourceId=resource_id, httpMethod="ANY",
        type="AWS_PROXY", integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    base = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/echo"
    host = f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"

    # Request: Content-Type matches binaryMediaTypes -> base64-encoded body.
    payload = bytes(range(256))
    req = _urlreq.Request(base, data=payload, method="POST")
    req.add_header("Host", host)
    req.add_header("Content-Type", "application/octet-stream")
    got = _json.loads(_urlreq.urlopen(req).read())
    assert got["isB64"] is True
    assert got["sha"] == hashlib.sha256(payload).hexdigest()

    # Request: Content-Type does NOT match -> UTF-8 string.
    req2 = _urlreq.Request(base, data=b'{"x":1}', method="POST")
    req2.add_header("Host", host)
    req2.add_header("Content-Type", "application/json")
    assert _json.loads(_urlreq.urlopen(req2).read())["isB64"] is False

    # Response: Accept matches binaryMediaTypes -> base64 decoded to raw bytes.
    req3 = _urlreq.Request(base + "?mode=resp", method="GET")
    req3.add_header("Host", host)
    req3.add_header("Accept", "application/octet-stream")
    assert _urlreq.urlopen(req3).read() == bytes(range(8))

    # Response: Accept does NOT match -> literal base64 string passed through.
    req4 = _urlreq.Request(base + "?mode=resp", method="GET")
    req4.add_header("Host", host)
    req4.add_header("Accept", "application/json")
    assert _urlreq.urlopen(req4).read() == _b64.b64encode(bytes(range(8)))

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


@pytest.mark.skipif(not shutil.which("curl"), reason="provided bootstrap uses curl for Runtime API")
def test_apigwv1_execute_lambda_proxy_provided_runtime(apigw_v1, lam):
    """execute-api AWS_PROXY must run provided.* zips via lambda_svc (Go/terraform parity)."""
    import urllib.request as _urlreq
    import uuid as _uuid

    bootstrap_script = (
        "#!/bin/sh\n"
        'RUNTIME_API="${AWS_LAMBDA_RUNTIME_API}"\n'
        "while true; do\n"
        '  RESP=$(curl -s -D /tmp/hdr '
        '"http://${RUNTIME_API}/2018-06-01/runtime/invocation/next")\n'
        '  REQUEST_ID=$(grep -i "Lambda-Runtime-Aws-Request-Id" /tmp/hdr '
        '| tr -d "\\r" | cut -d" " -f2)\n'
        '  curl -s -X POST '
        '"http://${RUNTIME_API}/2018-06-01/runtime/invocation/${REQUEST_ID}/response" '
        "-d '{\"statusCode\":200,\"body\":\"from-provided-bootstrap\"}'\n"
        "done\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        info = zipfile.ZipInfo("bootstrap")
        info.external_attr = 0o755 << 16
        zf.writestr(info, bootstrap_script)

    fname = f"intg-v1-provided-{_uuid.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=fname,
        Runtime="provided.al2023",
        Handler="bootstrap",
        Code={"ZipFile": buf.getvalue()},
        Role="arn:aws:iam::000000000000:role/test-role",
        Timeout=30,
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-provided-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="hit",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=(
            "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
            f"arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations"
        ),
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/hit"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req, timeout=60)
    assert resp.status == 200
    assert resp.read() == b"from-provided-bootstrap"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigwv1_execute_path_params(apigw_v1, lam):
    """Path parameter {userId} is passed correctly in event['pathParameters']."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-params-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    uid = (event.get('pathParameters') or {}).get('userId', 'missing')\n"
        b"    return {'statusCode': 200, 'body': uid}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-params-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    users_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="users",
    )["id"]
    user_id_res = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=users_id,
        pathPart="{userId}",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=user_id_res,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=user_id_res,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="v1", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/v1/users/alice123"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert resp.read() == b"alice123"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigwv1_literal_sibling_wins_over_param_registered_first(apigw_v1, lam):
    """A literal segment must resolve to its own method even when a {param}
    sibling under the same parent was created first (#970). AWS routes by
    specificity (literal > {param}), not by resource-creation order, so the
    literal path must not be shadowed into a 405."""
    import urllib.request as _urlreq
    import uuid as _uuid

    def _make_fn(label):
        fname = f"intg-v1-{label}-{_uuid.uuid4().hex[:8]}"
        code = (
            b"def handler(event, context):\n"
            b"    return {'statusCode': 200, 'body': '" + label.encode() + b"'}\n"
        )
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("index.py", code)
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler",
            Code={"ZipFile": buf.getvalue()},
        )
        return fname

    get_fn = _make_fn("getuser")
    verify_fn = _make_fn("verify")

    api_id = apigw_v1.create_rest_api(name=f"v1-precedence-{_uuid.uuid4().hex[:8]}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    users_id = apigw_v1.create_resource(restApiId=api_id, parentId=root["id"], pathPart="users")["id"]

    def _wire(parent, path_part, http_method, fname):
        rid = apigw_v1.create_resource(restApiId=api_id, parentId=parent, pathPart=path_part)["id"]
        apigw_v1.put_method(restApiId=api_id, resourceId=rid, httpMethod=http_method, authorizationType="NONE")
        apigw_v1.put_integration(
            restApiId=api_id, resourceId=rid, httpMethod=http_method,
            type="AWS_PROXY", integrationHttpMethod="POST",
            uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
        )

    # {id} registered BEFORE the literal sibling — the order that triggered #970.
    _wire(users_id, "{id}", "GET", get_fn)
    _wire(users_id, "verifyUserEmail", "POST", verify_fn)

    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="v1", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/v1/users/verifyUserEmail"
    req = _urlreq.Request(url, method="POST", data=b"{}")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert resp.read() == b"verify"

    # The {param} sibling still resolves for its own path.
    url2 = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/v1/users/alice123"
    req2 = _urlreq.Request(url2, method="GET")
    req2.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp2 = _urlreq.urlopen(req2)
    assert resp2.status == 200
    assert resp2.read() == b"getuser"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=get_fn)
    lam.delete_function(FunctionName=verify_fn)

def test_apigwv1_execute_mock_integration(apigw_v1):
    """MOCK integration returns fixed JSON from integration response template."""
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-mock-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="mock",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="MOCK",
        integrationHttpMethod="GET",
        uri="",
        requestTemplates={"application/json": '{"statusCode": 200}'},
    )
    apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
    )
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
        responseTemplates={"application/json": '{"mocked": true}'},
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/mock"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    body = json.loads(resp.read())
    assert body["mocked"] is True

    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_execute_missing_resource_403(apigw_v1):
    """Request to a non-existent path returns 403 Missing Authentication Token —
    real API Gateway (REST) treats an unsupported resource as
    MISSING_AUTHENTICATION_TOKEN (403), not 404."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-missing-resource")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/nonexistent"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    try:
        _urlreq.urlopen(req)
        assert False, "Expected 403"
    except _urlerr.HTTPError as e:
        assert e.code == 403

    apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_http_proxy_does_not_block_parallel_ddb(monkeypatch):
    import asyncio

    from ministack.services import apigateway as apigw_mod
    from ministack.services import apigateway_v1 as apigw_v1_mod
    from ministack.services import dynamodb as ddb_mod

    def _slow_urlopen(_request_or_url, _timeout_seconds):
        time.sleep(0.4)
        return 200, {"Content-Type": "application/json"}, b"{}"

    # _urlopen_async lives on apigateway and is reused by v1; patch the sync
    # helper there so both v1 and v2 tests target the same offload point.
    monkeypatch.setattr(apigw_mod, "_urlopen_sync", _slow_urlopen)

    async def _run():
        slow_call = asyncio.create_task(
            apigw_v1_mod._invoke_http_proxy_v1(
                {"uri": "http://example.test"},
                "/slow",
                "GET",
                {},
                None,
                {},
            )
        )
        await asyncio.sleep(0.05)
        started = time.perf_counter()
        status, _, _ = await ddb_mod.handle_request(
            "POST",
            "/",
            {"x-amz-target": "DynamoDB_20120810.ListTables"},
            b"{}",
            {},
        )
        elapsed = time.perf_counter() - started
        await slow_call
        return status, elapsed

    status, elapsed = asyncio.run(_run())
    assert status == 200
    assert elapsed < 0.2, f"Parallel DDB request was delayed for {elapsed:.2f}s"


def test_apigwv1_http_proxy_substitutes_path_params_and_forwards_query(monkeypatch):
    """HTTP_PROXY uses the substituted integration URI as the upstream URL."""
    import asyncio

    from ministack.services import apigateway as apigw_mod
    from ministack.services import apigateway_v1 as apigw_v1_mod

    captured = {}

    def _capture(req, _timeout_seconds):
        captured["url"] = req.full_url
        return 200, {"Content-Type": "application/json"}, b'{"ok": true}'

    monkeypatch.setattr(apigw_mod, "_urlopen_sync", _capture)

    status, _headers, body = apigw_v1_mod._create_rest_api({"name": "qa-v1-httpproxy-subst"})
    assert status == 201
    api_id = json.loads(body)["id"]

    try:
        status, _headers, body = apigw_v1_mod._get_resources(api_id, {})
        assert status == 200
        root = next(r for r in json.loads(body)["item"] if r["path"] == "/")

        status, _headers, body = apigw_v1_mod._create_resource(
            api_id,
            root["id"],
            {"pathPart": "things"},
        )
        assert status == 201
        things = json.loads(body)

        status, _headers, body = apigw_v1_mod._create_resource(
            api_id,
            things["id"],
            {"pathPart": "{thingId}"},
        )
        assert status == 201
        thing = json.loads(body)

        status, _headers, _body = apigw_v1_mod._put_method(
            api_id,
            thing["id"],
            "GET",
            {
                "authorizationType": "NONE",
                "requestParameters": {"method.request.path.thingId": True},
            },
        )
        assert status == 201

        status, _headers, _body = apigw_v1_mod._put_integration(
            api_id,
            thing["id"],
            "GET",
            {
                "type": "HTTP_PROXY",
                "httpMethod": "GET",
                "uri": "http://upstream.test/items/{thingId}",
                "requestParameters": {"integration.request.path.thingId": "method.request.path.thingId"},
            },
        )
        assert status == 201

        status, _headers, _body = apigw_v1_mod._create_deployment(api_id, {"stageName": "test"})
        assert status == 201

        status, _headers, _body = asyncio.run(
            apigw_v1_mod.handle_execute(
                api_id,
                "test",
                "GET",
                "/things/abc-123",
                {"host": "test"},
                b"",
                {"limit": ["10"]},
            )
        )

        assert status == 200
        assert captured["url"] == "http://upstream.test/items/abc-123?limit=10"

    finally:
        apigw_v1_mod._delete_rest_api(api_id)


def test_apigwv1_http_proxy_timeout_is_configurable(monkeypatch):
    """`_timeout_from_env` honours the env var and falls back on bad input.
    Tested directly instead of via importlib.reload so the suite-wide
    apigateway_v1 module state is not rebuilt mid-run."""
    from ministack.services.apigateway import _timeout_from_env

    monkeypatch.setenv("MINISTACK_APIGW_PROXY_TIMEOUT_SECONDS", "55")
    assert _timeout_from_env("MINISTACK_APIGW_PROXY_TIMEOUT_SECONDS", 30.0) == 55.0
    monkeypatch.setenv("MINISTACK_APIGW_PROXY_TIMEOUT_SECONDS", "not-a-number")
    assert _timeout_from_env("MINISTACK_APIGW_PROXY_TIMEOUT_SECONDS", 30.0) == 30.0
    monkeypatch.setenv("MINISTACK_APIGW_PROXY_TIMEOUT_SECONDS", "0")
    assert _timeout_from_env("MINISTACK_APIGW_PROXY_TIMEOUT_SECONDS", 30.0) == 30.0


def test_apigwv1_no_conflict_with_v2(apigw_v1, apigw, lam):
    """v1 and v2 APIs can coexist; execute-api routes them independently."""
    import urllib.request as _urlreq
    import uuid as _uuid

    # Create v1 Lambda
    fname_v1 = f"intg-coexist-v1-{_uuid.uuid4().hex[:8]}"
    code_v1 = b"def handler(event, context):\n    return {'statusCode': 200, 'body': 'v1-response'}\n"
    buf_v1 = io.BytesIO()
    with zipfile.ZipFile(buf_v1, "w") as zf:
        zf.writestr("index.py", code_v1)
    lam.create_function(
        FunctionName=fname_v1,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf_v1.getvalue()},
    )

    # Create v2 Lambda
    fname_v2 = f"intg-coexist-v2-{_uuid.uuid4().hex[:8]}"
    code_v2 = b"def handler(event, context):\n    return {'statusCode': 200, 'body': 'v2-response'}\n"
    buf_v2 = io.BytesIO()
    with zipfile.ZipFile(buf_v2, "w") as zf:
        zf.writestr("index.py", code_v2)
    lam.create_function(
        FunctionName=fname_v2,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf_v2.getvalue()},
    )

    # Set up v1 API
    v1_api_id = apigw_v1.create_rest_api(name="coexist-v1")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=v1_api_id)["items"] if r["path"] == "/")
    res_id = apigw_v1.create_resource(restApiId=v1_api_id, parentId=root["id"], pathPart="hit")["id"]
    apigw_v1.put_method(
        restApiId=v1_api_id,
        resourceId=res_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=v1_api_id,
        resourceId=res_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname_v1}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=v1_api_id)["id"]
    apigw_v1.create_stage(restApiId=v1_api_id, stageName="s", deploymentId=dep_id)

    # Set up v2 API
    v2_api_id = apigw.create_api(Name="coexist-v2", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=v2_api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname_v2}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=v2_api_id, RouteKey="GET /hit", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=v2_api_id, StageName="$default")

    # Invoke v1
    url_v1 = f"http://{v1_api_id}.execute-api.localhost:{_EXECUTE_PORT}/s/hit"
    req_v1 = _urlreq.Request(url_v1, method="GET")
    req_v1.add_header("Host", f"{v1_api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp_v1 = _urlreq.urlopen(req_v1)
    assert resp_v1.status == 200
    assert resp_v1.read() == b"v1-response"

    # Invoke v2
    url_v2 = f"http://{v2_api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/hit"
    req_v2 = _urlreq.Request(url_v2, method="GET")
    req_v2.add_header("Host", f"{v2_api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp_v2 = _urlreq.urlopen(req_v2)
    assert resp_v2.status == 200
    assert resp_v2.read() == b"v2-response"

    # Cleanup
    apigw_v1.delete_rest_api(restApiId=v1_api_id)
    apigw.delete_api(ApiId=v2_api_id)
    lam.delete_function(FunctionName=fname_v1)
    lam.delete_function(FunctionName=fname_v2)

def test_apigwv1_update_rest_api_name(apigw_v1):
    """UpdateRestApi renames the API via patchOperations."""
    api_id = apigw_v1.create_rest_api(name="v1-update-name-before")["id"]
    apigw_v1.update_rest_api(
        restApiId=api_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-update-name-after"}],
    )
    assert apigw_v1.get_rest_api(restApiId=api_id)["name"] == "v1-update-name-after"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_resource(apigw_v1):
    """DeleteResource removes a resource; subsequent GetResource raises 404."""
    api_id = apigw_v1.create_rest_api(name="v1-del-resource")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    child_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="todel")["id"]
    apigw_v1.delete_resource(restApiId=api_id, resourceId=child_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_resource(restApiId=api_id, resourceId=child_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_method(apigw_v1):
    """DeleteMethod removes method; GetMethod raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-method")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.delete_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_integration(apigw_v1):
    """DeleteIntegration removes integration; GetIntegration raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-integration")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    apigw_v1.delete_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_method_response(apigw_v1):
    """DeleteMethodResponse removes the method response entry."""
    api_id = apigw_v1.create_rest_api(name="v1-del-mresp")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_method_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    apigw_v1.delete_method_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_method_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_integration_response(apigw_v1):
    """DeleteIntegrationResponse removes the integration response entry."""
    api_id = apigw_v1.create_rest_api(name="v1-del-iresp")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
    )
    apigw_v1.delete_integration_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_integration_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_deployment(apigw_v1):
    """DeleteDeployment removes deployment; GetDeployment raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-deploy")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.delete_deployment(restApiId=api_id, deploymentId=dep_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_deployment(restApiId=api_id, deploymentId=dep_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_stage(apigw_v1):
    """DeleteStage removes stage; GetStage raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-stage")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="todel", deploymentId=dep_id)
    apigw_v1.delete_stage(restApiId=api_id, stageName="todel")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_stage(restApiId=api_id, stageName="todel")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_update_api_key(apigw_v1):
    """UpdateApiKey updates name and sets lastUpdatedDate."""
    import datetime

    key_id = apigw_v1.create_api_key(name="v1-key-update-before")["id"]
    resp = apigw_v1.update_api_key(
        apiKey=key_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-key-update-after"}],
    )
    assert resp["name"] == "v1-key-update-after"
    assert isinstance(resp["lastUpdatedDate"], datetime.datetime)
    apigw_v1.delete_api_key(apiKey=key_id)

def test_apigwv1_update_usage_plan(apigw_v1):
    """UpdateUsagePlan updates name via patchOperations."""
    plan_id = apigw_v1.create_usage_plan(name="v1-plan-update-before")["id"]
    resp = apigw_v1.update_usage_plan(
        usagePlanId=plan_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-plan-update-after"}],
    )
    assert resp["name"] == "v1-plan-update-after"
    apigw_v1.delete_usage_plan(usagePlanId=plan_id)

def test_apigwv1_deployment_api_summary(apigw_v1):
    """CreateDeployment apiSummary reflects methods configured on resources."""
    api_id = apigw_v1.create_rest_api(name="v1-api-summary")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    dep = apigw_v1.create_deployment(restApiId=api_id)
    assert "/" in dep.get("apiSummary", {}), "apiSummary must include root resource path"
    assert "GET" in dep["apiSummary"]["/"], "apiSummary must include configured HTTP method"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_domain_name_crud(apigw_v1):
    """DomainName create, get, list, delete lifecycle."""
    resp = apigw_v1.create_domain_name(
        domainName="api.example.com",
        endpointConfiguration={"types": ["REGIONAL"]},
    )
    assert resp["domainName"] == "api.example.com"
    got = apigw_v1.get_domain_name(domainName="api.example.com")
    assert got["domainName"] == "api.example.com"
    listed = apigw_v1.get_domain_names()["items"]
    assert any(d["domainName"] == "api.example.com" for d in listed)
    apigw_v1.delete_domain_name(domainName="api.example.com")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_domain_name(domainName="api.example.com")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_apigwv1_base_path_mapping_crud(apigw_v1):
    """BasePathMapping create, get, list, delete lifecycle."""
    apigw_v1.create_domain_name(domainName="bpm.example.com")
    api_id = apigw_v1.create_rest_api(name="v1-bpm-api")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="prod", deploymentId=dep_id)

    mapping = apigw_v1.create_base_path_mapping(
        domainName="bpm.example.com",
        basePath="v1",
        restApiId=api_id,
        stage="prod",
    )
    assert mapping["basePath"] == "v1"
    assert mapping["restApiId"] == api_id

    got = apigw_v1.get_base_path_mapping(domainName="bpm.example.com", basePath="v1")
    assert got["basePath"] == "v1"

    listed = apigw_v1.get_base_path_mappings(domainName="bpm.example.com")["items"]
    assert any(m["basePath"] == "v1" for m in listed)

    apigw_v1.delete_base_path_mapping(domainName="bpm.example.com", basePath="v1")
    apigw_v1.delete_rest_api(restApiId=api_id)
    apigw_v1.delete_domain_name(domainName="bpm.example.com")

def test_apigwv1_execute_missing_stage_404(apigw_v1):
    """execute-api returns 404 when stage does not exist."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-no-stage")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    apigw_v1.create_deployment(restApiId=api_id)
    # Do NOT create a stage — request to a nonexistent stage should 404

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/nonexistent/"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    with pytest.raises(_urlerr.HTTPError) as exc:
        _urlreq.urlopen(req)
    assert exc.value.code == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_execute_missing_method_403(apigw_v1):
    """execute-api returns 403 Missing Authentication Token when the resource
    exists but the method is not configured — real API Gateway treats an
    unsupported method as MISSING_AUTHENTICATION_TOKEN (403), not 405."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-no-method")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="noop")["id"]
    # PUT method for POST only — GET not configured
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="POST",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(restApiId=api_id, resourceId=resource_id, httpMethod="POST", type="MOCK")
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/noop"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    with pytest.raises(_urlerr.HTTPError) as exc:
        _urlreq.urlopen(req)
    assert exc.value.code == 403
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_execute_lambda_arn_uri(apigw_v1, lam):
    """execute-api Lambda proxy works with plain arn:aws:lambda ARN as integration URI."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"v1-arn-uri-{_uuid.uuid4().hex[:8]}"
    code = b"import json\ndef handler(event, context):\n    return {'statusCode': 200, 'body': 'arn-ok'}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-arn-{fname}")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="hit")["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    # Use plain arn:aws:lambda ARN (not apigateway URI form)
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/hit"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert resp.read() == b"arn-ok"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigwv1_execute_lambda_requestcontext(apigw_v1, lam):
    """execute-api Lambda event includes required requestContext fields."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"v1-reqctx-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    ctx = event.get('requestContext', {})\n"
        b"    body = json.dumps({\n"
        b"        'stage': ctx.get('stage'),\n"
        b"        'httpMethod': ctx.get('httpMethod'),\n"
        b"        'apiId': ctx.get('apiId'),\n"
        b"        'has_requestTime': 'requestTime' in ctx,\n"
        b"        'has_requestTimeEpoch': 'requestTimeEpoch' in ctx,\n"
        b"        'has_protocol': 'protocol' in ctx,\n"
        b"        'has_path': 'path' in ctx,\n"
        b"        'has_mvh': 'multiValueHeaders' in event,\n"
        b"    })\n"
        b"    return {'statusCode': 200, 'body': body}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-ctx-{fname}")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="ctx")["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="prod", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/ctx"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    data = json.loads(resp.read())
    assert data["stage"] == "prod"
    assert data["httpMethod"] == "GET"
    assert data["apiId"] == api_id
    assert data["has_requestTime"] is True
    assert data["has_requestTimeEpoch"] is True
    assert data["has_protocol"] is True
    assert data["has_path"] is True
    assert data["has_mvh"] is True

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigwv1_execute_mock_response_parameters(apigw_v1):
    """MOCK integration responseParameters are applied as HTTP response headers."""
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-mock-params")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="rp")["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        responseParameters={"method.response.header.X-Custom-Header": False},
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="MOCK",
        requestTemplates={"application/json": '{"statusCode": 200}'},
    )
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
        responseTemplates={"application/json": '{"ok": true}'},
        responseParameters={"method.response.header.X-Custom-Header": "'myvalue'"},
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/rp"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.headers.get("X-Custom-Header") == "myvalue"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_usage_plan_key_crud(apigw_v1):
    """CreateUsagePlanKey / GetUsagePlanKeys / DeleteUsagePlanKey."""
    api_key = apigw_v1.create_api_key(name="qa-v1-key", enabled=True)
    key_id = api_key["id"]
    plan = apigw_v1.create_usage_plan(
        name="qa-v1-plan",
        throttle={"rateLimit": 100, "burstLimit": 200},
    )
    plan_id = plan["id"]
    apigw_v1.create_usage_plan_key(usagePlanId=plan_id, keyId=key_id, keyType="API_KEY")
    keys = apigw_v1.get_usage_plan_keys(usagePlanId=plan_id)["items"]
    assert any(k["id"] == key_id for k in keys)
    apigw_v1.delete_usage_plan_key(usagePlanId=plan_id, keyId=key_id)
    keys2 = apigw_v1.get_usage_plan_keys(usagePlanId=plan_id)["items"]
    assert not any(k["id"] == key_id for k in keys2)

def test_apigwv1_get_usage_plan_key(apigw_v1):
    """GetUsagePlanKey returns the per-key entry. The Terraform AWS provider
    issues this call immediately after CreateUsagePlanKey to verify the
    resource exists; before the handler was added the request fell through
    to a 404 and aws_api_gateway_usage_plan_key applies aborted with
    'couldn't find resource'."""
    api_key = apigw_v1.create_api_key(name="qa-v1-gupk-key", enabled=True)
    key_id = api_key["id"]
    plan_id = apigw_v1.create_usage_plan(
        name="qa-v1-gupk-plan",
        throttle={"rateLimit": 100, "burstLimit": 200},
    )["id"]
    apigw_v1.create_usage_plan_key(usagePlanId=plan_id, keyId=key_id, keyType="API_KEY")

    got = apigw_v1.get_usage_plan_key(usagePlanId=plan_id, keyId=key_id)
    assert got["id"] == key_id
    assert got["type"] == "API_KEY"

    with pytest.raises(ClientError) as exc:
        apigw_v1.get_usage_plan_key(usagePlanId=plan_id, keyId="missing-key-id")
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

    with pytest.raises(ClientError) as exc:
        apigw_v1.get_usage_plan_key(usagePlanId="missing-plan-id", keyId=key_id)
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

    apigw_v1.delete_usage_plan_key(usagePlanId=plan_id, keyId=key_id)

def test_apigwv1_created_date_is_unix_timestamp(apigw_v1):
    resp = apigw_v1.create_rest_api(name="tf-date-test")
    created = resp["createdDate"]
    # boto3 parses numeric timestamps as datetime.datetime — if it were a string
    # botocore would raise a deserialization error before we even get here.
    import datetime

    assert isinstance(created, datetime.datetime), (
        f"createdDate should be datetime (parsed from Unix int), got {type(created)}"
    )
    apigw_v1.delete_rest_api(restApiId=resp["id"])


# ========== Custom/predictable REST API IDs via tags (issue #400) ==========

def test_apigwv1_custom_id_via_ms_custom_id_tag(apigw_v1):
    resp = apigw_v1.create_rest_api(
        name="ms-custom-v1", tags={"ms-custom-id": "v1pinned"},
    )
    assert resp["id"] == "v1pinned"


def test_apigwv1_custom_id_rejects_ls_custom_id(apigw_v1):
    """ls-custom-id is not supported; caller must use ms-custom-id."""
    with pytest.raises(ClientError) as exc_info:
        apigw_v1.create_rest_api(
            name="ls-reject-v1", tags={"ls-custom-id": "should-fail"},
        )
    assert exc_info.value.response["Error"]["Code"] == "BadRequestException"
    assert "ms-custom-id" in exc_info.value.response["Error"]["Message"]


def test_apigwv1_custom_id_duplicate_rejected(apigw_v1):
    apigw_v1.create_rest_api(
        name="v1-dup-1", tags={"ms-custom-id": "v1dup"},
    )
    with pytest.raises(ClientError) as exc_info:
        apigw_v1.create_rest_api(
            name="v1-dup-2", tags={"ms-custom-id": "v1dup"},
        )
    assert exc_info.value.response["Error"]["Code"] == "ConflictException"


def test_apigwv1_custom_id_duplicate_rejected_across_regions():
    set_request_region("us-west-2")
    status, _headers, body = apigateway_v1._create_rest_api(
        {"name": "v1-dup-west", "tags": {"ms-custom-id": "v1regionaldup"}}
    )
    assert status == 201
    assert json.loads(body)["id"] == "v1regionaldup"

    set_request_region("us-east-1")
    status, _headers, body = apigateway_v1._create_rest_api(
        {"name": "v1-dup-east", "tags": {"ms-custom-id": "v1regionaldup"}}
    )
    assert status == 409
    assert json.loads(body)["__type"] == "ConflictException"
    assert apigateway_v1.find_api_scope("v1regionaldup") == (
        "000000000000",
        "us-west-2",
    )


def test_apigwv1_custom_id_absent_uses_random(apigw_v1):
    resp = apigw_v1.create_rest_api(name="v1-random")
    # _new_id() returns up to 10 hex chars; trimmed to [:8] in _create_rest_api.
    assert 8 <= len(resp["id"]) <= 10


def test_apigwv1_lambda_proxy_emits_cloudwatch_logs(apigw_v1, lam, logs):
    """Lambda invoked via API Gateway v1 REST proxy must emit CloudWatch Logs."""
    import urllib.request as _urlreq

    fname = f"intg-v1-cwl-{_uuid_mod.uuid4().hex[:8]}"
    marker = f"MARKER-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        f"import sys\n"
        f"def handler(event, context):\n"
        f"    print('{marker}')\n"
        f"    return {{'statusCode': 200, 'body': 'ok'}}\n"
    ).encode()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-cwl-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="cwltest",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/cwltest"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200

    # Verify CloudWatch Logs contain the marker text
    log_group = f"/aws/lambda/{fname}"
    streams = logs.describe_log_streams(logGroupName=log_group)["logStreams"]
    assert len(streams) >= 1, f"Expected at least one log stream in {log_group}"

    all_messages = []
    for stream in streams:
        events = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream["logStreamName"],
        )["events"]
        all_messages.extend(e["message"] for e in events)

    assert any(marker in msg for msg in all_messages), (
        f"Marker '{marker}' not found in CloudWatch Logs. Messages: {all_messages}"
    )
    assert any(msg.startswith("START RequestId:") for msg in all_messages)
    assert any(msg.startswith("END RequestId:") for msg in all_messages)
    assert any(msg.startswith("REPORT RequestId:") for msg in all_messages)

    # Cleanup
    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigwv1_lambda_proxy_emits_cloudwatch_logs_nodejs(apigw_v1, lam, logs):
    """Node.js Lambda invoked via API Gateway v1 REST proxy must emit CloudWatch Logs."""
    import urllib.request as _urlreq

    fname = f"intg-v1-cwl-js-{_uuid_mod.uuid4().hex[:8]}"
    marker = f"JSMARKER-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        "exports.handler = async (event) => {\n"
        f"  console.log('{marker}');\n"
        "  return { statusCode: 200, body: 'ok' };\n"
        "};\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.js", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="nodejs20.x",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-cwl-js-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="cwljs",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/cwljs"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200

    log_group = f"/aws/lambda/{fname}"
    streams = logs.describe_log_streams(logGroupName=log_group)["logStreams"]
    assert len(streams) >= 1

    all_messages = []
    for stream in streams:
        events = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream["logStreamName"],
        )["events"]
        all_messages.extend(e["message"] for e in events)

    assert any(marker in msg for msg in all_messages), (
        f"Marker '{marker}' not found in CloudWatch Logs. Messages: {all_messages}"
    )
    assert any(msg.startswith("START RequestId:") for msg in all_messages)
    assert any(msg.startswith("END RequestId:") for msg in all_messages)
    assert any(msg.startswith("REPORT RequestId:") for msg in all_messages)

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)


# ========== from test_apigatewayv1_content_handling.py ==========
# Regression tests for API Gateway v1 (REST API) ContentHandling fidelity.
# H-8: PutIntegration was dropping contentHandling (same family as #439 for v2).
# M-6: PutIntegrationResponse pin (already fixed; pinned here so it stays fixed).


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


def test_apigateway_v1_create_domain_name_accepts_new_tls_policy(apigw_v1):
    """securityPolicy must accept the 2026-03 added enum value
    SecurityPolicy-TLS13-1-2-FIPS-PFS-PQ-2025-09 and any future opaque values."""
    domain = f"api-tls-{_uuid_mod.uuid4().hex[:8]}.example.com"
    new_policy = "SecurityPolicy-TLS13-1-2-FIPS-PFS-PQ-2025-09"
    r = apigw_v1.create_domain_name(
        domainName=domain,
        certificateName="c1",
        certificateArn=f"arn:aws:acm:us-east-1:000000000000:certificate/{_uuid_mod.uuid4().hex[:8]}",
        securityPolicy=new_policy,
    )
    assert r["securityPolicy"] == new_policy
    got = apigw_v1.get_domain_name(domainName=domain)
    assert got["securityPolicy"] == new_policy


def test_apigateway_v1_create_domain_name_default_tls_policy(apigw_v1):
    """When securityPolicy is omitted, AWS defaults to TLS_1_2."""
    domain = f"api-tls-default-{_uuid_mod.uuid4().hex[:8]}.example.com"
    r = apigw_v1.create_domain_name(
        domainName=domain,
        certificateName="c2",
    )
    assert r["securityPolicy"] == "TLS_1_2"


# ---------------------------------------------------------------------------
# ARN-parser / tag-scope in-process unit tests. Folded from
# test_apigatewayv1_arn_parser.py.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_apigateway_v1():
    set_request_region("us-east-1")
    apigateway_v1.reset()
    yield
    apigateway_v1.reset()
    set_request_region("us-east-1")


def _payload(response):
    status, _headers, body = response
    return status, json.loads(body.decode("utf-8")) if body else {}


def _create_rest_api():
    status, body = _payload(
        apigateway_v1._create_rest_api(
            {"name": "arn-parser-test", "tags": {"created": "true"}}
        )
    )
    assert status == 201
    api_id = body["id"]
    return api_id, f"arn:aws:apigateway:us-east-1::/restapis/{api_id}"


def _create_stage_arn():
    api_id, _resource_arn = _create_rest_api()
    status, body = _payload(
        apigateway_v1._create_stage(
            api_id,
            {"stageName": "prod", "tags": {"created": "stage"}},
        )
    )
    assert status == 201
    return f"arn:aws:apigateway:us-east-1::/restapis/{api_id}/stages/{body['stageName']}"


def _create_api_key_arn():
    status, body = _payload(
        apigateway_v1._create_api_key(
            {"name": "arn-parser-key", "tags": {"created": "api-key"}}
        )
    )
    assert status == 201
    return f"arn:aws:apigateway:us-east-1::/apikeys/{body['id']}"


def _create_usage_plan_arn():
    status, body = _payload(
        apigateway_v1._create_usage_plan(
            {"name": "arn-parser-plan", "tags": {"created": "usage-plan"}}
        )
    )
    assert status == 201
    return f"arn:aws:apigateway:us-east-1::/usageplans/{body['id']}"


def _create_domain_name_arn():
    status, body = _payload(
        apigateway_v1._create_domain_name(
            {"domainName": "arn-parser.example.com", "tags": {"created": "domain"}}
        )
    )
    assert status == 201
    return f"arn:aws:apigateway:us-east-1::/domainnames/{body['domainName']}"


def _tag_store():
    return dict(apigateway_v1._v1_tags.items())


@pytest.mark.parametrize(
    "resource_arn_factory",
    [
        lambda: _create_rest_api()[1],
        _create_stage_arn,
        _create_api_key_arn,
        _create_usage_plan_arn,
        _create_domain_name_arn,
    ],
)
def test_apigatewayv1_tag_resource_accepts_existing_local_resource_arn(
    resource_arn_factory,
):
    resource_arn = resource_arn_factory()

    status, body = _payload(
        apigateway_v1._tag_v1_resource(
            resource_arn,
            {"tags": {"env": "test", "team": "platform"}},
        )
    )
    assert status == 204
    assert body == {}

    status, body = _payload(apigateway_v1._get_v1_tags(resource_arn))
    assert status == 200
    assert body["tags"]["env"] == "test"
    assert body["tags"]["team"] == "platform"

    status, body = _payload(apigateway_v1._untag_v1_resource(resource_arn, ["env"]))
    assert status == 204
    assert body == {}

    status, body = _payload(apigateway_v1._get_v1_tags(resource_arn))
    assert status == 200
    assert "env" not in body["tags"]
    assert body["tags"]["team"] == "platform"


@pytest.mark.parametrize(
    ("resource_arn_factory", "expected_status", "expected_code"),
    [
        (lambda api_id: "not-an-arn", 400, "BadRequestException"),
        (lambda api_id: "arn:aws:apigateway:us-east-1::", 400, "BadRequestException"),
        (
            lambda api_id: f"arn:aws-us-gov:apigateway:us-east-1::/restapis/{api_id}",
            400,
            "BadRequestException",
        ),
        (
            lambda api_id: f"arn:aws:lambda:us-east-1::/restapis/{api_id}",
            400,
            "BadRequestException",
        ),
        (
            lambda api_id: f"arn:aws:apigateway:us-west-2::/restapis/{api_id}",
            400,
            "BadRequestException",
        ),
        (
            lambda api_id: f"arn:aws:apigateway:us-east-1:000000000000:/restapis/{api_id}",
            400,
            "BadRequestException",
        ),
        (
            lambda api_id: f"arn:aws:apigateway:us-east-1::/domainnames/{api_id}.example.com",
            404,
            "NotFoundException",
        ),
        (
            lambda api_id: f"arn:aws:apigateway:us-east-1::/restapis/{api_id}/resources/root",
            400,
            "BadRequestException",
        ),
        (
            lambda api_id: "arn:aws:apigateway:us-east-1::/restapis/missing-api",
            404,
            "NotFoundException",
        ),
        (
            lambda api_id: f"arn:aws:apigateway:us-east-1::/restapis/{api_id}/stages/missing-stage",
            404,
            "NotFoundException",
        ),
        (
            lambda api_id: "arn:aws:apigateway:us-east-1::/apikeys/missing-key",
            404,
            "NotFoundException",
        ),
        (
            lambda api_id: "arn:aws:apigateway:us-east-1::/usageplans/missing-plan",
            404,
            "NotFoundException",
        ),
        (
            lambda api_id: "arn:aws:apigateway:us-east-1::/domainnames/missing.example.com",
            404,
            "NotFoundException",
        ),
    ],
)
def test_apigatewayv1_tag_resource_rejects_invalid_or_missing_arns_before_tags_change(
    resource_arn_factory,
    expected_status,
    expected_code,
):
    api_id, _resource_arn = _create_rest_api()
    bad_arn = resource_arn_factory(api_id)
    before = _tag_store()

    for response in (
        apigateway_v1._get_v1_tags(bad_arn),
        apigateway_v1._tag_v1_resource(bad_arn, {"tags": {"mutated": "true"}}),
        apigateway_v1._untag_v1_resource(bad_arn, ["created"]),
    ):
        status, body = _payload(response)
        assert status == expected_status
        assert body["__type"] == expected_code
        assert _tag_store() == before


@pytest.mark.parametrize(
    "resource_arn_factory",
    [
        lambda: _create_rest_api()[1],
        _create_stage_arn,
        _create_api_key_arn,
        _create_usage_plan_arn,
        _create_domain_name_arn,
    ],
)
def test_apigatewayv1_tag_resource_rejects_cross_region_local_resource_arns(
    resource_arn_factory,
):
    resource_arn = resource_arn_factory()
    cross_region_arn = resource_arn.replace(":us-east-1:", ":us-west-2:")
    before = _tag_store()

    set_request_region("us-west-2")
    for response in (
        apigateway_v1._get_v1_tags(cross_region_arn),
        apigateway_v1._tag_v1_resource(cross_region_arn, {"tags": {"mutated": "true"}}),
        apigateway_v1._untag_v1_resource(cross_region_arn, ["created"]),
    ):
        status, body = _payload(response)
        assert status == 404
        assert body["__type"] == "NotFoundException"
        assert _tag_store() == before


def test_apigatewayv1_load_persisted_state_backfills_taggable_resource_regions():
    apigateway_v1.load_persisted_state(
        {
            "rest_apis": {
                "legacy-api": {"id": "legacy-api", "name": "legacy api"},
            },
            "rest_api_regions": {
                "legacy-api": "us-west-2",
            },
            "stages_v1": {
                "legacy-api": {"prod": {"stageName": "prod", "tags": {}}},
            },
            "resources": {
                "legacy-api": {"root": {"id": "root", "path": "/"}},
            },
            "api_keys": {
                "legacy-key": {"id": "legacy-key", "name": "legacy key", "tags": {}},
                "legacy-key-west": {"id": "legacy-key-west", "name": "legacy key west", "tags": {}},
            },
            "api_key_regions": {
                "legacy-key-west": "us-west-2",
            },
            "usage_plans": {
                "legacy-plan": {"id": "legacy-plan", "name": "legacy plan", "tags": {}},
                "legacy-plan-west": {"id": "legacy-plan-west", "name": "legacy plan west", "tags": {}},
            },
            "usage_plan_regions": {
                "legacy-plan-west": "us-west-2",
            },
            "usage_plan_keys": {
                "legacy-plan-west": {
                    "legacy-key-west": {"id": "legacy-key-west", "type": "API_KEY"}
                },
            },
            "domain_names": {
                "legacy.example.com": {
                    "domainName": "legacy.example.com",
                    "regionalDomainName": "legacy.example.com.execute-api.us-west-2.amazonaws.com",
                    "tags": {},
                },
            },
            "base_path_mappings": {
                "legacy.example.com": {
                    "v1": {"basePath": "v1", "restApiId": "legacy-api", "stage": "prod"}
                },
            },
            "v1_tags": {
                "arn:aws:apigateway:us-west-2::/restapis/legacy-api": {"legacy": "true"},
                "arn:aws:apigateway:us-west-2::/apikeys/legacy-key-west": {"legacy": "true"},
                "arn:aws:apigateway:us-west-2::/usageplans/legacy-plan-west": {"legacy": "true"},
            },
        }
    )

    account_id = "000000000000"
    assert apigateway_v1._rest_apis.get_scoped(account_id, "us-west-2", "legacy-api")
    assert apigateway_v1._resources.get_scoped(
        account_id, "us-west-2", "legacy-api"
    )["root"]["path"] == "/"
    assert (
        apigateway_v1._usage_plan_keys.get_scoped(
            account_id, "us-west-2", "legacy-plan-west"
        )["legacy-key-west"]["type"]
        == "API_KEY"
    )
    assert apigateway_v1._base_path_mappings.get_scoped(
        account_id, "us-west-2", "legacy.example.com"
    )["v1"]["stage"] == "prod"
    assert apigateway_v1._rest_apis.get_scoped(account_id, "us-east-1", "legacy-api") is None

    for resource_arn in (
        "arn:aws:apigateway:us-east-1::/apikeys/legacy-key",
        "arn:aws:apigateway:us-east-1::/usageplans/legacy-plan",
    ):
        status, body = _payload(
            apigateway_v1._tag_v1_resource(resource_arn, {"tags": {"env": "test"}})
        )
        assert status == 204
        assert body == {}

    set_request_region("us-west-2")
    for resource_arn in (
        "arn:aws:apigateway:us-west-2::/apikeys/legacy-key-west",
        "arn:aws:apigateway:us-west-2::/usageplans/legacy-plan-west",
    ):
        status, body = _payload(
            apigateway_v1._tag_v1_resource(resource_arn, {"tags": {"env": "test"}})
        )
        assert status == 204
        assert body == {}

    status, body = _payload(
        apigateway_v1._get_v1_tags(
            "arn:aws:apigateway:us-west-2::/restapis/legacy-api"
        )
    )
    assert status == 200
    assert body["tags"] == {"legacy": "true"}

    status, body = _payload(
        apigateway_v1._tag_v1_resource(
            "arn:aws:apigateway:us-west-2::/restapis/legacy-api/stages/prod",
            {"tags": {"env": "test"}},
        )
    )
    assert status == 204
    assert body == {}

    status, body = _payload(
        apigateway_v1._tag_v1_resource(
            "arn:aws:apigateway:us-west-2::/domainnames/legacy.example.com",
            {"tags": {"env": "test"}},
        )
    )
    assert status == 204
    assert body == {}


# ---- Data plane: Lambda-authorizer result caching ----

_AUTH_ECHO_BACKEND = (
    "import json\n"
    "def handler(event, context):\n"
    "    return {'statusCode': 200,\n"
    "            'body': json.dumps(event['requestContext'].get('authorizer'))}\n"
)

# Prelude for authorizer sources: counts invocations by pushing one SQS message
# per call. The counter has to live INSIDE the emulator — writing to a
# ``tmp_path`` file only works while the server runs in-process, and silently
# breaks (FileNotFoundError on every counted test) under the documented
# ``docker compose up`` workflow where the Lambda runs in its own container.
_AUTH_MARK_PRELUDE = (
    "import os, boto3\n"
    "def _mark(qname):\n"
    "    sqs = boto3.client('sqs', endpoint_url=os.environ['AWS_ENDPOINT_URL'])\n"
    "    url = sqs.get_queue_url(QueueName=qname)['QueueUrl']\n"
    "    sqs.send_message(QueueUrl=url, MessageBody='1')\n"
)


def _auth_counter_queue(sqs):
    """Create the SQS queue an authorizer marks on every invocation."""
    qname = f"v1-auth-count-{_uuid_mod.uuid4().hex[:8]}"
    sqs.create_queue(QueueName=qname)
    return qname


def _auth_count(sqs, qname):
    """How many times the authorizer behind ``qname`` has run."""
    url = sqs.get_queue_url(QueueName=qname)["QueueUrl"]
    attrs = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"])


def _auth_delete_queue(sqs, qname):
    try:
        sqs.delete_queue(QueueUrl=sqs.get_queue_url(QueueName=qname)["QueueUrl"])
    except ClientError:
        pass


def _auth_make_lambda(lam, label, code):
    fname = f"v1-auth-{label}-{_uuid_mod.uuid4().hex[:8]}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Timeout=30,
        Code={"ZipFile": buf.getvalue()},
    )
    return fname


# ---- Data plane: {proxy+} fallthrough from a methodless intermediate node ----


def _proxy_fb_make_lambda(lam, label, code):
    fname = f"v1-proxyfb-{label}-{_uuid_mod.uuid4().hex[:8]}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Timeout=30,
        Code={"ZipFile": buf.getvalue()},
    )
    return fname


def _auth_drop_lambda(lam, fname):
    try:
        lam.delete_function(FunctionName=fname)
    except ClientError:
        pass


def _auth_drop_api(apigw_v1, api_id):
    try:
        apigw_v1.delete_rest_api(restApiId=api_id)
    except ClientError:
        pass


def _auth_lambda_uri(fname):
    return (
        "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
        f"arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations"
    )


def _proxy_fb_uri(fname):
    return (
        "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
        f"arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations"
    )


def _auth_token_authorizer_code(qname):
    """TOKEN authorizer: Allow for `allow*` tokens, Deny otherwise.

    The policy's ``Resource`` is the canonical ``event['methodArn']`` — the
    shape every AWS sample emits, and the one whose per-request evaluation
    keeps a cached policy from granting another method or stage. The methodArn
    is echoed through ``context`` so a test can see which one was issued.
    """
    return (
        _AUTH_MARK_PRELUDE
        + "def handler(event, context):\n"
        f"    _mark({qname!r})\n"
        "    token = event.get('authorizationToken', '')\n"
        "    effect = 'Allow' if token.startswith('allow') else 'Deny'\n"
        "    return {\n"
        "        'principalId': 'user|' + token,\n"
        "        'policyDocument': {'Version': '2012-10-17', 'Statement': [\n"
        "            {'Action': 'execute-api:Invoke', 'Effect': effect,\n"
        "             'Resource': event['methodArn']}]},\n"
        "        'context': {'arn': event['methodArn']},\n"
        "    }\n"
    )


def _auth_http(url, method="GET", headers=None, timeout=30):
    """(status, body_bytes) without raising on 4xx/5xx.

    An explicit timeout keeps a wedged request from hanging the whole session.
    """
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    req = _urlreq.Request(url, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        resp = _urlreq.urlopen(req, timeout=timeout)
        return resp.status, resp.read()
    except _urlerr.HTTPError as e:
        return e.code, e.read()
def _proxy_fb_http(url, method="GET", headers=None, timeout=30):
    """(status, body_bytes) without raising on 4xx/5xx."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    req = _urlreq.Request(url, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        resp = _urlreq.urlopen(req, timeout=timeout)
        return resp.status, resp.read()
    except _urlerr.HTTPError as e:
        return e.code, e.read()


def _auth_root_id(apigw_v1, api_id):
    return next(
        r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/"
    )


def _auth_execute_url(api_id, stage, path):
    # Path-based execute form: no *.localhost DNS needed.
    return f"http://localhost:{_EXECUTE_PORT}/_aws/execute-api/{api_id}/{stage}/{path}"


def _auth_wire_lambda_method(apigw_v1, api_id, resource_id, backend_fname, authorizer_id):
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="CUSTOM",
        authorizerId=authorizer_id,
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=_auth_lambda_uri(backend_fname),
    )


def _auth_build_api(apigw_v1, backend_fname, authorizer_kwargs, path_parts=("secure",)):
    """API with a CUSTOM-guarded `GET /{part}` per entry in ``path_parts``."""
    api_id = apigw_v1.create_rest_api(name=f"v1-auth-{_uuid_mod.uuid4().hex[:6]}")["id"]
    root_id = _auth_root_id(apigw_v1, api_id)
    authorizer_id = apigw_v1.create_authorizer(restApiId=api_id, **authorizer_kwargs)["id"]
    for part in path_parts:
        resource_id = apigw_v1.create_resource(
            restApiId=api_id, parentId=root_id, pathPart=part
        )["id"]
        _auth_wire_lambda_method(apigw_v1, api_id, resource_id, backend_fname, authorizer_id)
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)
    return api_id, authorizer_id


def test_apigwv1_authorizer_cache_is_scoped_per_method_arn(apigw_v1, lam, sqs):
    """A cached Allow grants only the method it was issued for.

    The canonical `Resource: event['methodArn']` policy covers exactly one
    method. Caching the allow/deny *decision* under a key without the method ARN
    replays that Allow on every other method the same token touches; caching the
    *policy* and re-evaluating it per request does not.
    """
    qname = _auth_counter_queue(sqs)
    backend = _auth_make_lambda(lam, "be", _AUTH_ECHO_BACKEND)
    authz = _auth_make_lambda(lam, "tok", _auth_token_authorizer_code(qname))
    api_id, _ = _auth_build_api(
        apigw_v1, backend,
        dict(name="tok", type="TOKEN", authorizerUri=_auth_lambda_uri(authz),
             authorizerResultTtlInSeconds=300),
        path_parts=("alpha", "beta"),
    )
    try:
        status, _ = _auth_http(
            _auth_execute_url(api_id, "test", "alpha"), headers={"Authorization": "allow-abc"}
        )
        assert status == 200
        assert _auth_count(sqs, qname) == 1

        status, body = _auth_http(
            _auth_execute_url(api_id, "test", "beta"), headers={"Authorization": "allow-abc"}
        )
        assert status == 403, "an Allow issued for /alpha must not carry over to /beta"
        assert json.loads(body) == {"Message": "User is not authorized to access this resource"}
        assert _auth_count(sqs, qname) == 1, "the cached policy is what gets re-evaluated"
    finally:
        _auth_drop_api(apigw_v1, api_id)
        _auth_drop_lambda(lam, backend)
        _auth_drop_lambda(lam, authz)
        _auth_delete_queue(sqs, qname)


def test_apigwv1_authorizer_cache_is_scoped_per_stage(apigw_v1, lam, sqs):
    """API Gateway caches authorizer results per stage: a verdict computed on
    one stage never answers the same token on another."""
    qname = _auth_counter_queue(sqs)
    backend = _auth_make_lambda(lam, "be", _AUTH_ECHO_BACKEND)
    authz = _auth_make_lambda(lam, "tok", _auth_token_authorizer_code(qname))
    api_id, _ = _auth_build_api(
        apigw_v1, backend,
        dict(name="tok", type="TOKEN", authorizerUri=_auth_lambda_uri(authz),
             authorizerResultTtlInSeconds=300),
    )
    try:
        dep_id = apigw_v1.get_deployments(restApiId=api_id)["items"][0]["id"]
        apigw_v1.create_stage(restApiId=api_id, stageName="prod", deploymentId=dep_id)

        status, body = _auth_http(
            _auth_execute_url(api_id, "test", "secure"), headers={"Authorization": "allow-abc"}
        )
        assert status == 200
        assert "/test/GET/secure" in json.loads(body)["arn"]
        assert _auth_count(sqs, qname) == 1

        status, body = _auth_http(
            _auth_execute_url(api_id, "prod", "secure"), headers={"Authorization": "allow-abc"}
        )
        assert status == 200, body
        assert _auth_count(sqs, qname) == 2, "the test-stage entry must not serve prod"
        assert "/prod/GET/secure" in json.loads(body)["arn"]

        # The per-stage entries are independent: both now answer from cache.
        for stage in ("test", "prod"):
            status, _ = _auth_http(
                _auth_execute_url(api_id, stage, "secure"), headers={"Authorization": "allow-abc"}
            )
            assert status == 200
        assert _auth_count(sqs, qname) == 2
    finally:
        _auth_drop_api(apigw_v1, api_id)
        _auth_drop_lambda(lam, backend)
        _auth_drop_lambda(lam, authz)
        _auth_delete_queue(sqs, qname)


def test_apigwv1_authorizer_invalid_validation_expression_is_500(apigw_v1, lam):
    """CreateAuthorizer stores identityValidationExpression verbatim, so an
    uncompilable expression only fails on the data path — as a 500
    AUTHORIZER_CONFIGURATION_ERROR, not an exception out of the handler."""
    backend = _auth_make_lambda(lam, "be", _AUTH_ECHO_BACKEND)
    authz = _auth_make_lambda(
        lam, "tok",
        "def handler(event, context):\n"
        "    return {'principalId': 'p',\n"
        "            'policyDocument': {'Version': '2012-10-17', 'Statement': [\n"
        "                {'Action': 'execute-api:Invoke', 'Effect': 'Allow',\n"
        "                 'Resource': event['methodArn']}]}}\n",
    )
    api_id, _ = _auth_build_api(
        apigw_v1, backend,
        dict(name="tok", type="TOKEN", authorizerUri=_auth_lambda_uri(authz),
             identityValidationExpression="allow-[a-z",
             authorizerResultTtlInSeconds=0),
    )
    try:
        status, body = _auth_http(
            _auth_execute_url(api_id, "test", "secure"), headers={"Authorization": "allow-abc"}
        )
        assert status == 500
        assert json.loads(body) == {"message": "Internal server error"}
    finally:
        _auth_drop_api(apigw_v1, api_id)
        _auth_drop_lambda(lam, backend)
        _auth_drop_lambda(lam, authz)


def test_apigwv1_authorizer_without_principal_id_is_500_and_not_cached(apigw_v1, lam, sqs):
    """AWS requires a principal. An Allow policy with no principalId is an
    AUTHORIZER_CONFIGURATION_ERROR, not an allow that reaches the backend with
    an empty principalId — and a configuration error is never cached."""
    qname = _auth_counter_queue(sqs)
    backend = _auth_make_lambda(lam, "be", _AUTH_ECHO_BACKEND)
    authz = _auth_make_lambda(
        lam, "noprincipal",
        _AUTH_MARK_PRELUDE
        + "def handler(event, context):\n"
        f"    _mark({qname!r})\n"
        "    return {'policyDocument': {'Version': '2012-10-17', 'Statement': [\n"
        "        {'Action': 'execute-api:Invoke', 'Effect': 'Allow',\n"
        "         'Resource': event['methodArn']}]}}\n",
    )
    api_id, _ = _auth_build_api(
        apigw_v1, backend,
        dict(name="noprincipal", type="TOKEN", authorizerUri=_auth_lambda_uri(authz),
             authorizerResultTtlInSeconds=300),
    )
    try:
        url = _auth_execute_url(api_id, "test", "secure")
        for _ in range(2):
            status, body = _auth_http(url, headers={"Authorization": "allow-abc"})
            assert status == 500
            assert json.loads(body) == {"message": "Internal server error"}
        assert _auth_count(sqs, qname) == 2, "a configuration error must not be cached"
    finally:
        _auth_drop_api(apigw_v1, api_id)
        _auth_drop_lambda(lam, backend)
        _auth_drop_lambda(lam, authz)
        _auth_delete_queue(sqs, qname)


def test_apigwv1_authorizer_without_policy_document_is_500_and_not_cached(apigw_v1, lam, sqs):
    """A response with no policyDocument is a misconfigured authorizer: AWS
    answers 500 AuthorizerConfigurationException, not an implicit deny — and
    the malformed response is never cached."""
    qname = _auth_counter_queue(sqs)
    backend = _auth_make_lambda(lam, "be", _AUTH_ECHO_BACKEND)
    authz = _auth_make_lambda(
        lam, "nopolicy",
        _AUTH_MARK_PRELUDE
        + "def handler(event, context):\n"
        f"    _mark({qname!r})\n"
        "    return {'principalId': 'user|abc'}\n",
    )
    api_id, _ = _auth_build_api(
        apigw_v1, backend,
        dict(name="nopolicy", type="TOKEN", authorizerUri=_auth_lambda_uri(authz),
             authorizerResultTtlInSeconds=300),
    )
    try:
        url = _auth_execute_url(api_id, "test", "secure")
        for _ in range(2):
            status, body = _auth_http(url, headers={"Authorization": "allow-abc"})
            assert status == 500
            assert json.loads(body) == {"message": "Internal server error"}
        assert _auth_count(sqs, qname) == 2, "a configuration error must not be cached"
    finally:
        _auth_drop_api(apigw_v1, api_id)
        _auth_drop_lambda(lam, backend)
        _auth_drop_lambda(lam, authz)
        _auth_delete_queue(sqs, qname)


def test_apigwv1_authorizer_unparsable_ttl_falls_back_to_the_default(apigw_v1, lam, sqs):
    """UpdateAuthorizer applies JSON Patch without validating, so the TTL can be
    any string by the time the data plane reads it. An unparsable value falls
    back to the same default an absent one does (and does not raise).

    The PATCH goes over raw HTTP: botocore models the field as an integer and
    would raise while parsing the *response*, after the server already stored
    the string — which is exactly how an authorizer ends up in this state.
    """
    import urllib.request as _urlreq

    qname = _auth_counter_queue(sqs)
    backend = _auth_make_lambda(lam, "be", _AUTH_ECHO_BACKEND)
    authz = _auth_make_lambda(lam, "tok", _auth_token_authorizer_code(qname))
    api_id, authorizer_id = _auth_build_api(
        apigw_v1, backend,
        dict(name="tok", type="TOKEN", authorizerUri=_auth_lambda_uri(authz),
             authorizerResultTtlInSeconds=300),
    )
    try:
        patch = _urlreq.Request(
            f"{_endpoint}/restapis/{api_id}/authorizers/{authorizer_id}",
            data=json.dumps({"patchOperations": [{
                "op": "replace",
                "path": "/authorizerResultTtlInSeconds",
                "value": "not-a-number",
            }]}).encode(),
            headers={"Content-Type": "application/json"},
            method="PATCH",
        )
        patched = json.loads(_urlreq.urlopen(patch, timeout=30).read())
        assert patched["authorizerResultTtlInSeconds"] == "not-a-number"

        url = _auth_execute_url(api_id, "test", "secure")
        for _ in range(2):
            status, _ = _auth_http(url, headers={"Authorization": "allow-abc"})
            assert status == 200
        assert _auth_count(sqs, qname) == 1, "caching stays on at the default TTL"
    finally:
        _auth_drop_api(apigw_v1, api_id)
        _auth_drop_lambda(lam, backend)
        _auth_drop_lambda(lam, authz)
        _auth_delete_queue(sqs, qname)


def test_apigwv1_authorizer_cache_is_bounded():
    """The result cache is keyed on caller-supplied tokens, so it is capped
    rather than left to grow for the life of the process. Expired entries go
    first; beyond that the oldest insertions are evicted."""
    cache = apigateway_v1._authorizer_cache
    cap = apigateway_v1._AUTHORIZER_CACHE_MAX
    saved = dict(cache)
    cache.clear()
    try:
        expired_at = time.time() - 1
        for i in range(cap):
            apigateway_v1._cache_authorizer_result(("stale", i), expired_at, {}, {})
        assert len(cache) == cap

        # The next write is over the cap and prunes the expired entries first.
        apigateway_v1._cache_authorizer_result(("live", 0), time.time() + 300, {}, {})
        assert len(cache) == 1
        assert ("live", 0) in cache

        # With nothing expired to reclaim, the oldest insertions are evicted.
        for i in range(cap + 50):
            apigateway_v1._cache_authorizer_result(("live", i), time.time() + 300, {}, {})
        assert len(cache) <= cap
        assert ("live", cap + 49) in cache
    finally:
        cache.clear()
        cache.update(saved)
def _proxy_fb_wire(apigw_v1, api_id, resource_id, http_method, backend, **method_kwargs):
    apigw_v1.put_method(
        restApiId=api_id, resourceId=resource_id, httpMethod=http_method,
        **method_kwargs,
    )
    apigw_v1.put_integration(
        restApiId=api_id, resourceId=resource_id, httpMethod=http_method,
        type="AWS_PROXY", integrationHttpMethod="POST", uri=_proxy_fb_uri(backend),
    )


def _proxy_fb_drop_lambda(lam, fname):
    try:
        lam.delete_function(FunctionName=fname)
    except ClientError:
        pass


def _proxy_fb_drop_api(apigw_v1, api_id):
    try:
        apigw_v1.delete_rest_api(restApiId=api_id)
    except ClientError:
        pass


def test_apigwv1_methodless_resource_falls_through_to_proxy(apigw_v1, lam):
    """A path-matched resource that does not serve the requested verb falls
    through to a `{proxy+}` elsewhere in the tree instead of failing the
    request. That covers an implicit intermediate node like `/jobs`, which
    exists only as the parent of `/jobs/{operation_type}`.

    Measured against real API Gateway (eu-central-1, MOCK integrations): only an
    exact resource+method match keeps a request on the specific resource."""
    proxy_backend = _proxy_fb_make_lambda(
        lam, "prx",
        "def handler(event, context):\n"
        "    return {'statusCode': 200,\n"
        "            'body': 'via-proxy:' + event['pathParameters']['proxy']}\n",
    )
    jobs_backend = _proxy_fb_make_lambda(
        lam, "job",
        "def handler(event, context):\n"
        "    return {'statusCode': 200,\n"
        "            'body': 'job:' + event['pathParameters']['operation_type']}\n",
    )
    api_id = apigw_v1.create_rest_api(name=f"v1-proxyfb-{_uuid_mod.uuid4().hex[:6]}")["id"]
    try:
        root_id = next(
            r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"]
            if r["path"] == "/"
        )
        proxy_id = apigw_v1.create_resource(
            restApiId=api_id, parentId=root_id, pathPart="{proxy+}"
        )["id"]
        _proxy_fb_wire(apigw_v1, api_id, proxy_id, "ANY", proxy_backend,
                       authorizationType="NONE")
        jobs_id = apigw_v1.create_resource(
            restApiId=api_id, parentId=root_id, pathPart="jobs"
        )["id"]
        op_id = apigw_v1.create_resource(
            restApiId=api_id, parentId=jobs_id, pathPart="{operation_type}"
        )["id"]
        _proxy_fb_wire(apigw_v1, api_id, op_id, "GET", jobs_backend,
                       authorizationType="NONE")
        dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
        apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

        base = f"http://localhost:{_EXECUTE_PORT}/_aws/execute-api/{api_id}/test"

        # The specific resource still wins where it serves the verb.
        assert _proxy_fb_http(f"{base}/jobs/reboot") == (200, b"job:reboot")

        # /jobs itself declares no methods -> the sibling {proxy+} serves it.
        assert _proxy_fb_http(f"{base}/jobs", method="POST") == (200, b"via-proxy:jobs")
        assert _proxy_fb_http(f"{base}/jobs") == (200, b"via-proxy:jobs")

        # /jobs/{operation_type} declares GET only, so POST does not match it
        # either and lands on the proxy as well. Measured on real AWS: routing
        # is on the resource+method pair, not on the resource alone.
        assert _proxy_fb_http(f"{base}/jobs/reboot", method="POST") == (
            200, b"via-proxy:jobs/reboot")
    finally:
        _proxy_fb_drop_api(apigw_v1, api_id)
        _proxy_fb_drop_lambda(lam, proxy_backend)
        _proxy_fb_drop_lambda(lam, jobs_backend)


def test_apigwv1_proxy_fallthrough_reaches_past_a_guarded_sibling(apigw_v1, lam):
    """A guarded sibling method does not stop the fallthrough, because routing
    happens before authorization: an open root `{proxy+} ANY` alongside a
    protected `/admin POST` serves `GET /admin` from the unauthenticated proxy.

    This reads like an auth bypass, so it was measured rather than assumed. On
    real API Gateway (eu-central-1), with `/admin POST` at `AWS_IAM` and an open
    `{proxy+} ANY`, an unsigned `GET /admin` answered 200 from the proxy. The
    guarded verb itself stays guarded; it is the verbs the resource does not
    declare that leave it. Protecting a path therefore means protecting every
    method on it, or not putting an open `{proxy+}` above it."""
    proxy_backend = _proxy_fb_make_lambda(
        lam, "openprx",
        "def handler(event, context):\n"
        "    return {'statusCode': 200,\n"
        "            'body': 'via-proxy:' + event['pathParameters']['proxy']}\n",
    )
    admin_backend = _proxy_fb_make_lambda(
        lam, "admin",
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'body': 'admin-secret'}\n",
    )
    authz = _proxy_fb_make_lambda(
        lam, "denyall",
        "def handler(event, context):\n"
        "    return {'principalId': 'p',\n"
        "            'policyDocument': {'Version': '2012-10-17', 'Statement': [\n"
        "                {'Action': 'execute-api:Invoke', 'Effect': 'Deny',\n"
        "                 'Resource': event['methodArn']}]}}\n",
    )
    api_id = apigw_v1.create_rest_api(name=f"v1-proxyfb-{_uuid_mod.uuid4().hex[:6]}")["id"]
    try:
        root_id = next(
            r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"]
            if r["path"] == "/"
        )
        proxy_id = apigw_v1.create_resource(
            restApiId=api_id, parentId=root_id, pathPart="{proxy+}"
        )["id"]
        _proxy_fb_wire(apigw_v1, api_id, proxy_id, "ANY", proxy_backend,
                       authorizationType="NONE")
        admin_id = apigw_v1.create_resource(
            restApiId=api_id, parentId=root_id, pathPart="admin"
        )["id"]
        auth_id = apigw_v1.create_authorizer(
            restApiId=api_id, name="denyall", type="TOKEN",
            authorizerUri=_proxy_fb_uri(authz), authorizerResultTtlInSeconds=0,
        )["id"]
        _proxy_fb_wire(apigw_v1, api_id, admin_id, "POST", admin_backend,
                       authorizationType="CUSTOM", authorizerId=auth_id)
        dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
        apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

        base = f"http://localhost:{_EXECUTE_PORT}/_aws/execute-api/{api_id}/test"

        # The guarded verb is enforced by the authorizer.
        status, body = _proxy_fb_http(f"{base}/admin", method="POST",
                                      headers={"Authorization": "t"})
        assert status == 403
        assert b"admin-secret" not in body

        # /admin declares POST only, so every other verb leaves the resource and
        # lands on the open proxy — authorization on the sibling method does not
        # hold the path. Verified against real AWS before pinning it here.
        for verb in ("GET", "DELETE"):
            assert _proxy_fb_http(f"{base}/admin", method=verb) == (
                200, b"via-proxy:admin"), f"{verb} /admin did not reach the proxy"

        # The proxy still serves paths that are genuinely its own.
        assert _proxy_fb_http(f"{base}/public/thing") == (200, b"via-proxy:public/thing")
    finally:
        _proxy_fb_drop_api(apigw_v1, api_id)
        _proxy_fb_drop_lambda(lam, proxy_backend)
        _proxy_fb_drop_lambda(lam, admin_backend)
        _proxy_fb_drop_lambda(lam, authz)


def test_apigwv1_cors_preflight_only_resource_falls_through(apigw_v1, lam):
    """The shape real deployments actually have: an intermediate that carries a
    CORS `OPTIONS` preflight and nothing else.

    CDK and Serverless both add that preflight, so the intermediate is never
    method-less in practice — it simply does not serve the verb being asked
    for. Real AWS falls through here too (measured, eu-central-1), which is why
    the fallthrough triggers on "does not serve this verb" rather than on "has
    no methods": the narrower rule would miss every API we deploy."""
    proxy_backend = _proxy_fb_make_lambda(
        lam, "corsprx",
        "def handler(event, context):\n"
        "    return {'statusCode': 200,\n"
        "            'body': 'via-proxy:' + event['pathParameters']['proxy']}\n",
    )
    preflight_backend = _proxy_fb_make_lambda(
        lam, "corspre",
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'body': 'preflight'}\n",
    )
    api_id = apigw_v1.create_rest_api(name=f"v1-proxyfb-{_uuid_mod.uuid4().hex[:6]}")["id"]
    try:
        root_id = next(
            r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"]
            if r["path"] == "/"
        )
        proxy_id = apigw_v1.create_resource(
            restApiId=api_id, parentId=root_id, pathPart="{proxy+}"
        )["id"]
        _proxy_fb_wire(apigw_v1, api_id, proxy_id, "ANY", proxy_backend,
                       authorizationType="NONE")
        cors_id = apigw_v1.create_resource(
            restApiId=api_id, parentId=root_id, pathPart="remote_connection"
        )["id"]
        _proxy_fb_wire(apigw_v1, api_id, cors_id, "OPTIONS", preflight_backend,
                       authorizationType="NONE")
        dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
        apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

        base = f"http://localhost:{_EXECUTE_PORT}/_aws/execute-api/{api_id}/test"

        # OPTIONS is declared, so it stays on the resource.
        assert _proxy_fb_http(f"{base}/remote_connection", method="OPTIONS") == (
            200, b"preflight")

        # GET is not, so it falls through to the sibling proxy.
        assert _proxy_fb_http(f"{base}/remote_connection") == (
            200, b"via-proxy:remote_connection")
    finally:
        _proxy_fb_drop_api(apigw_v1, api_id)
        _proxy_fb_drop_lambda(lam, proxy_backend)
        _proxy_fb_drop_lambda(lam, preflight_backend)
