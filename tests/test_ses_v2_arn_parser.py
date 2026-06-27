import asyncio
import json

import pytest

from ministack.core.responses import set_request_account_id, set_request_region

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"


@pytest.fixture()
def ses_v2():
    from ministack.services import ses_v2 as service

    set_request_account_id(ACCOUNT_ID)
    set_request_region(REGION)
    service.reset()
    yield service
    service.reset()


def _arn(kind, name, *, partition="aws", service="ses", region=REGION, account=ACCOUNT_ID):
    return f"arn:{partition}:{service}:{region}:{account}:{kind}/{name}"


def _call(service, method, path="/v2/email/tags", *, body=None, query=None):
    raw_body = json.dumps(body).encode("utf-8") if body is not None else b""
    status, _headers, raw = asyncio.run(
        service.handle_request(method, path, {}, raw_body, query or {})
    )
    return status, json.loads(raw.decode("utf-8")) if raw else {}


def test_ses_v2_identity_tag_resource_uses_parser_backed_resource_arn(ses_v2):
    identity = "parser.example.com"
    resource_arn = _arn("identity", identity)

    status, _body = _call(
        ses_v2,
        "POST",
        "/v2/email/identities",
        body={
            "EmailIdentity": identity,
            "Tags": [{"Key": "created", "Value": "yes"}],
        },
    )
    assert status == 200

    status, body = _call(
        ses_v2,
        "GET",
        query={"ResourceArn": [resource_arn]},
    )
    assert status == 200
    assert body["Tags"] == [{"Key": "created", "Value": "yes"}]

    status, _body = _call(
        ses_v2,
        "POST",
        body={
            "ResourceArn": resource_arn,
            "Tags": [
                {"Key": "created", "Value": "updated"},
                {"Key": "team", "Value": "platform"},
            ],
        },
    )
    assert status == 200

    status, body = _call(ses_v2, "GET", query={"ResourceArn": [resource_arn]})
    assert status == 200
    assert body["Tags"] == [
        {"Key": "created", "Value": "updated"},
        {"Key": "team", "Value": "platform"},
    ]

    status, _body = _call(
        ses_v2,
        "DELETE",
        query={"ResourceArn": [resource_arn], "TagKeys": ["created"]},
    )
    assert status == 200

    status, body = _call(ses_v2, "GET", query={"ResourceArn": [resource_arn]})
    assert status == 200
    assert body["Tags"] == [{"Key": "team", "Value": "platform"}]


def test_ses_v2_configuration_set_tag_resource_uses_parser_backed_resource_arn(ses_v2):
    config_set_name = "parser-config"
    resource_arn = _arn("configuration-set", config_set_name)

    status, _body = _call(
        ses_v2,
        "POST",
        "/v2/email/configuration-sets",
        body={
            "ConfigurationSetName": config_set_name,
            "Tags": [{"Key": "created", "Value": "yes"}],
        },
    )
    assert status == 200

    status, _body = _call(
        ses_v2,
        "POST",
        body={
            "ResourceArn": resource_arn,
            "Tags": [{"Key": "team", "Value": "email"}],
        },
    )
    assert status == 200

    status, body = _call(ses_v2, "GET", query={"ResourceArn": [resource_arn]})
    assert status == 200
    assert body["Tags"] == [
        {"Key": "created", "Value": "yes"},
        {"Key": "team", "Value": "email"},
    ]


@pytest.mark.parametrize(
    "bad_arn",
    [
        "not-an-arn",
        f"arn:aws:ses:{REGION}:{ACCOUNT_ID}",
        _arn("identity", "parser.example.com", partition="aws-cn"),
        _arn("identity", "parser.example.com", service="sesv2"),
        _arn("identity", "parser.example.com", region="us-west-2"),
        _arn("identity", "parser.example.com", account="111111111111"),
        _arn("template", "parser-template"),
        f"arn:aws:ses:{REGION}:{ACCOUNT_ID}:identity/parser.example.com/extra",
    ],
)
@pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
def test_ses_v2_tag_apis_reject_invalid_resource_arns_before_touching_tags(ses_v2, bad_arn, method):
    identity = "parser.example.com"
    valid_arn = _arn("identity", identity)
    _call(ses_v2, "POST", "/v2/email/identities", body={"EmailIdentity": identity})
    _call(
        ses_v2,
        "POST",
        body={"ResourceArn": valid_arn, "Tags": [{"Key": "keep", "Value": "yes"}]},
    )

    if method == "POST":
        status, body = _call(
            ses_v2,
            method,
            body={"ResourceArn": bad_arn, "Tags": [{"Key": "bad", "Value": "no"}]},
        )
    elif method == "DELETE":
        status, body = _call(
            ses_v2,
            method,
            query={"ResourceArn": [bad_arn], "TagKeys": ["keep"]},
        )
    else:
        status, body = _call(ses_v2, method, query={"ResourceArn": [bad_arn]})

    assert status == 400
    assert body["name"] == "BadRequestException"
    assert ses_v2._ses_tags.get(bad_arn) is None

    status, body = _call(ses_v2, "GET", query={"ResourceArn": [valid_arn]})
    assert status == 200
    assert body["Tags"] == [{"Key": "keep", "Value": "yes"}]


@pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
def test_ses_v2_tag_apis_reject_missing_local_resources_before_touching_tags(ses_v2, method):
    missing_arn = _arn("identity", "missing.example.com")

    if method == "POST":
        status, body = _call(
            ses_v2,
            method,
            body={"ResourceArn": missing_arn, "Tags": [{"Key": "bad", "Value": "no"}]},
        )
    elif method == "DELETE":
        status, body = _call(
            ses_v2,
            method,
            query={"ResourceArn": [missing_arn], "TagKeys": ["bad"]},
        )
    else:
        status, body = _call(ses_v2, method, query={"ResourceArn": [missing_arn]})

    assert status == 404
    assert body["name"] == "NotFoundException"
    assert ses_v2._ses_tags.get(missing_arn) is None
