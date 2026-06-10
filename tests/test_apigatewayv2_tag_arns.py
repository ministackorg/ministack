import asyncio
import json
import urllib.parse

import pytest

from ministack.core.responses import get_account_id, get_region, set_request_account_id, set_request_region
from ministack.services import apigateway as _apigw


@pytest.fixture(autouse=True)
def _reset_apigateway_state():
    original_account = get_account_id()
    original_region = get_region()
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    _apigw.reset()
    try:
        yield
    finally:
        _apigw.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def _payload(response):
    status, _headers, body = response
    return status, json.loads(body or b"{}")


def _post_tags_path(resource_arn, tags):
    encoded_arn = urllib.parse.quote(resource_arn, safe="")
    body = json.dumps({"tags": tags}).encode("utf-8")
    return asyncio.run(_apigw.handle_request("POST", f"/v2/tags/{encoded_arn}", {}, body, {}))


def _create_api(name="tag-arn-api"):
    status, body = _payload(_apigw._create_api({"name": name, "protocolType": "HTTP"}))
    assert status == 201
    return body["apiId"]


def test_apigwv2_tag_api_arn_decodes_and_uses_canonical_key():
    api_id = _create_api()
    api_arn = _apigw._api_arn(api_id)

    status, _headers, _body = _post_tags_path(api_arn, {"env": "test"})

    assert status == 201
    assert _apigw._api_tags.get(api_arn) == {"env": "test"}

    status, body = _payload(_apigw._get_tags(api_arn))
    assert status == 200
    assert body["tags"] == {"env": "test"}


def test_apigwv2_tag_arns_accept_existing_local_api_and_stage_resources():
    api_id = _create_api("tag-stage-api")

    stage_status, _stage_body = _payload(
        _apigw._create_stage(api_id, {"stageName": "$default", "tags": {"created": "stage"}})
    )
    assert stage_status == 201

    stage_arn = _apigw._api_resource_arn(api_id, "stages", "$default")
    status, _headers, _body = _apigw._tag_resource(stage_arn, {"tags": {"owner": "team-a"}})
    assert status == 201

    stage_status, stage_body = _payload(_apigw._get_tags(stage_arn))
    assert stage_status == 200
    assert stage_body["tags"] == {"created": "stage", "owner": "team-a"}


def test_apigwv2_tag_arns_reject_invalid_or_nonlocal_resources_before_touching_tags():
    api_id = _create_api("tag-reject-api")
    route_status, route = _payload(_apigw._create_route(api_id, {"routeKey": "GET /items"}))
    assert route_status == 201
    integration_status, integration = _payload(
        _apigw._create_integration(
            api_id,
            {
                "integrationType": "HTTP_PROXY",
                "integrationUri": "https://example.com",
                "integrationMethod": "GET",
            },
        )
    )
    assert integration_status == 201
    deployment_status, deployment = _payload(_apigw._create_deployment(api_id, {}))
    assert deployment_status == 201
    authorizer_status, authorizer = _payload(
        _apigw._create_authorizer(
            api_id,
            {
                "name": "request-auth",
                "authorizerType": "REQUEST",
                "authorizerUri": "not-an-arn",
                "authorizerCredentialsArn": "also-not-an-arn",
            },
        )
    )
    assert authorizer_status == 201

    bad_arns = [
        ("not-an-arn", "BadRequestException"),
        (f"arn:aws-us-gov:apigateway:us-east-1::/apis/{api_id}", "BadRequestException"),
        (f"arn:aws:execute-api:us-east-1::/apis/{api_id}", "BadRequestException"),
        (f"arn:aws:apigateway:us-west-2::/apis/{api_id}", "BadRequestException"),
        (f"arn:aws:apigateway:us-east-1:000000000000:/apis/{api_id}", "BadRequestException"),
        ("arn:aws:apigateway:us-east-1::/domainnames/example.com", "BadRequestException"),
        (f"arn:aws:apigateway:us-east-1::/apis/{api_id}/routes", "BadRequestException"),
        (f"arn:aws:apigateway:us-east-1::/apis/{api_id}/routes/{route['routeId']}", "BadRequestException"),
        (
            f"arn:aws:apigateway:us-east-1::/apis/{api_id}/integrations/{integration['integrationId']}",
            "BadRequestException",
        ),
        (
            f"arn:aws:apigateway:us-east-1::/apis/{api_id}/deployments/{deployment['deploymentId']}",
            "BadRequestException",
        ),
        (
            f"arn:aws:apigateway:us-east-1::/apis/{api_id}/authorizers/{authorizer['authorizerId']}",
            "BadRequestException",
        ),
        ("arn:aws:apigateway:us-east-1::/apis/missing", "NotFoundException"),
        (f"arn:aws:apigateway:us-east-1::/apis/{api_id}/stages/missing", "NotFoundException"),
    ]

    for resource_arn, expected_code in bad_arns:
        before = dict(_apigw._api_tags.items())
        status, body = _payload(_apigw._tag_resource(resource_arn, {"tags": {"bad": "tag"}}))
        assert status in (400, 404)
        assert body["__type"] == expected_code
        assert dict(_apigw._api_tags.items()) == before
        assert _apigw._api_tags.get(resource_arn) is None

    stage_status, _stage = _payload(_apigw._create_stage(api_id, {"stageName": "prod"}))
    assert stage_status == 201
    stage_arn = _apigw._api_resource_arn(api_id, "stages", "prod")
    assert _apigw._tag_resource(stage_arn, {"tags": {"ok": "yes"}})[0] == 201
    assert _apigw._api_tags.get(stage_arn) == {"ok": "yes"}
    assert _apigw._delete_stage(api_id, "prod")[0] == 204
    status, body = _payload(_apigw._get_tags(stage_arn))
    assert status == 404
    assert body["__type"] == "NotFoundException"
    assert _apigw._api_tags.get(stage_arn) is None
