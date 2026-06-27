import json

import pytest

import ministack.services.servicediscovery as sd_svc


@pytest.fixture(autouse=True)
def reset_servicediscovery_state():
    sd_svc.reset()
    yield
    sd_svc.reset()


def _json_result(response):
    status, headers, body = response
    return status, headers, json.loads(body.decode("utf-8"))


def _seed_namespace(ns_id="ns-direct"):
    arn = sd_svc._namespace_arn(ns_id)
    sd_svc._namespaces[ns_id] = {
        "Id": ns_id,
        "Arn": arn,
        "Name": "direct.local",
        "Type": "HTTP",
    }
    return arn


def _seed_service(svc_id="srv-direct"):
    ns_id = "ns-for-service"
    sd_svc._namespaces[ns_id] = {
        "Id": ns_id,
        "Arn": sd_svc._namespace_arn(ns_id),
        "Name": "service.local",
        "Type": "HTTP",
    }
    arn = sd_svc._service_arn(svc_id)
    sd_svc._services[svc_id] = {
        "Id": svc_id,
        "Arn": arn,
        "Name": "direct-service",
        "NamespaceId": ns_id,
    }
    return arn


@pytest.mark.parametrize("seed_resource", [_seed_namespace, _seed_service])
def test_servicediscovery_tag_apis_accept_local_namespace_and_service_arns(seed_resource):
    arn = seed_resource()

    status, _, body = _json_result(
        sd_svc._tag_resource(
            {
                "ResourceARN": arn,
                "Tags": [
                    {"Key": "env", "Value": "test"},
                    {"Key": "owner", "Value": "platform"},
                ],
            }
        )
    )
    assert status == 200
    assert body == {}

    status, _, body = _json_result(sd_svc._list_tags_for_resource({"ResourceARN": arn}))
    assert status == 200
    assert body["Tags"] == [
        {"Key": "env", "Value": "test"},
        {"Key": "owner", "Value": "platform"},
    ]

    status, _, body = _json_result(sd_svc._untag_resource({"ResourceARN": arn, "TagKeys": ["env"]}))
    assert status == 200
    assert body == {}

    status, _, body = _json_result(sd_svc._list_tags_for_resource({"ResourceARN": arn}))
    assert status == 200
    assert body["Tags"] == [{"Key": "owner", "Value": "platform"}]


@pytest.mark.parametrize(
    "bad_arn",
    [
        "not-an-arn",
        "arn:aws-cn:servicediscovery:us-east-1:000000000000:namespace/ns-direct",
        "arn:aws:s3:us-east-1:000000000000:namespace/ns-direct",
        "arn:aws:servicediscovery:us-east-1:111122223333:namespace/ns-direct",
        "arn:aws:servicediscovery:us-west-2:000000000000:namespace/ns-direct",
        "arn:aws:servicediscovery:us-east-1:000000000000:instance/ns-direct/inst-1",
        "arn:aws:servicediscovery:us-east-1:000000000000:namespace/ns-direct/child",
        "arn:aws:servicediscovery:us-east-1:000000000000:service",
    ],
)
def test_servicediscovery_tag_apis_reject_invalid_resource_arns_before_touching_tags(bad_arn):
    valid_arn = _seed_namespace()
    sd_svc._resource_tags[valid_arn] = [{"Key": "keep", "Value": "yes"}]

    status, _, body = _json_result(
        sd_svc._tag_resource({"ResourceARN": bad_arn, "Tags": [{"Key": "new", "Value": "tag"}]})
    )
    assert status == 400
    assert body["__type"] == "InvalidInput"
    assert sd_svc._resource_tags.get(bad_arn) is None

    status, _, body = _json_result(sd_svc._untag_resource({"ResourceARN": bad_arn, "TagKeys": ["keep"]}))
    assert status == 400
    assert body["__type"] == "InvalidInput"

    status, _, body = _json_result(sd_svc._list_tags_for_resource({"ResourceARN": bad_arn}))
    assert status == 400
    assert body["__type"] == "InvalidInput"

    assert sd_svc._resource_tags[valid_arn] == [{"Key": "keep", "Value": "yes"}]


@pytest.mark.parametrize(
    ("resource_arn", "expected_error"),
    [
        ("arn:aws:servicediscovery:us-east-1:000000000000:namespace/ns-missing", "NamespaceNotFound"),
        ("arn:aws:servicediscovery:us-east-1:000000000000:service/srv-missing", "ServiceNotFound"),
    ],
)
def test_servicediscovery_tag_apis_reject_missing_local_resources(resource_arn, expected_error):
    for call in (
        lambda: sd_svc._tag_resource({"ResourceARN": resource_arn, "Tags": [{"Key": "new", "Value": "tag"}]}),
        lambda: sd_svc._untag_resource({"ResourceARN": resource_arn, "TagKeys": ["old"]}),
        lambda: sd_svc._list_tags_for_resource({"ResourceARN": resource_arn}),
    ):
        status, _, body = _json_result(call())
        assert status == 404
        assert body["__type"] == expected_error

    assert sd_svc._resource_tags.get(resource_arn) is None
