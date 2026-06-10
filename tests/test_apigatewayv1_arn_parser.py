import json

import pytest

from ministack.core.responses import set_request_region
from ministack.services import apigateway_v1


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
            "stages_v1": {
                "legacy-api": {"prod": {"stageName": "prod", "tags": {}}},
            },
            "api_keys": {
                "legacy-key": {"id": "legacy-key", "name": "legacy key", "tags": {}},
                "legacy-key-west": {"id": "legacy-key-west", "name": "legacy key west", "tags": {}},
            },
            "usage_plans": {
                "legacy-plan": {"id": "legacy-plan", "name": "legacy plan", "tags": {}},
                "legacy-plan-west": {"id": "legacy-plan-west", "name": "legacy plan west", "tags": {}},
            },
            "domain_names": {
                "legacy.example.com": {
                    "domainName": "legacy.example.com",
                    "regionalDomainName": "legacy.example.com.execute-api.us-west-2.amazonaws.com",
                    "tags": {},
                },
            },
            "v1_tags": {
                "arn:aws:apigateway:us-west-2::/restapis/legacy-api": {"legacy": "true"},
                "arn:aws:apigateway:us-west-2::/apikeys/legacy-key-west": {"legacy": "true"},
                "arn:aws:apigateway:us-west-2::/usageplans/legacy-plan-west": {"legacy": "true"},
            },
        }
    )

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
