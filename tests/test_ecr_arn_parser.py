import json

import pytest

from ministack.core.responses import get_account_id, get_region, set_request_account_id, set_request_region
from ministack.services import ecr as ecr_svc


def _body(response):
    return json.loads(response[2].decode("utf-8"))


def _reset_ecr_state():
    for store in (
        ecr_svc._repositories,
        ecr_svc._images,
        ecr_svc._lifecycle_policies,
        ecr_svc._repo_policies,
        ecr_svc._layer_blobs,
        ecr_svc._manifest_blobs,
        ecr_svc._uploads,
    ):
        store.clear()


@pytest.fixture(autouse=True)
def ecr_module_state():
    original_account = get_account_id()
    original_region = get_region()
    _reset_ecr_state()
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    yield
    _reset_ecr_state()
    set_request_account_id(original_account)
    set_request_region(original_region)


def _create_repository(name):
    response = ecr_svc._create_repository({"repositoryName": name})
    assert response[0] == 200, _body(response)
    return _body(response)["repository"]


def _assert_repository_not_found(response):
    assert response[0] == 400
    assert _body(response)["__type"] == "RepositoryNotFoundException"


def test_ecr_tag_apis_require_request_scope_and_stored_repository_arn_match():
    repo_name = "arn-parser-region-scope"
    stored = _create_repository(repo_name)
    stored_arn = stored["repositoryArn"]
    assert stored_arn == f"arn:aws:ecr:us-east-1:000000000000:repository/{repo_name}"

    seed_response = ecr_svc._tag_resource({
        "resourceArn": stored_arn,
        "tags": [{"Key": "env", "Value": "east"}],
    })
    assert seed_response[0] == 200, _body(seed_response)

    set_request_region("us-west-2")
    fabricated_request_region_arn = f"arn:aws:ecr:us-west-2:000000000000:repository/{repo_name}"
    for resource_arn in (stored_arn, fabricated_request_region_arn):
        _assert_repository_not_found(ecr_svc._tag_resource({
            "resourceArn": resource_arn,
            "tags": [{"Key": "bad", "Value": "tag"}],
        }))
        _assert_repository_not_found(ecr_svc._list_tags_for_resource({"resourceArn": resource_arn}))
        _assert_repository_not_found(ecr_svc._untag_resource({
            "resourceArn": resource_arn,
            "tagKeys": ["env"],
        }))

    set_request_region("us-east-1")
    assert _body(ecr_svc._list_tags_for_resource({"resourceArn": stored_arn}))["tags"] == [
        {"Key": "env", "Value": "east"}
    ]


@pytest.mark.parametrize(
    "resource_arn",
    [
        "not-an-arn",
        "arn:aws-cn:ecr:us-east-1:000000000000:repository/arn-parser-scope",
        "arn:aws:ecr:us-west-2:000000000000:repository/arn-parser-scope",
        "arn:aws:ecr:us-east-1:111111111111:repository/arn-parser-scope",
        "arn:aws:sns:us-east-1:000000000000:repository/arn-parser-scope",
        "arn:aws:ecr:us-east-1:000000000000:image/arn-parser-scope",
        "arn:aws:ecr:us-east-1:000000000000:repository/",
    ],
)
def test_ecr_tag_apis_reject_invalid_or_out_of_scope_repository_arns(resource_arn):
    repo_name = "arn-parser-scope"
    valid_arn = _create_repository(repo_name)["repositoryArn"]

    seed_response = ecr_svc._tag_resource({
        "resourceArn": valid_arn,
        "tags": [{"Key": "existing", "Value": "tag"}],
    })
    assert seed_response[0] == 200, _body(seed_response)

    _assert_repository_not_found(ecr_svc._tag_resource({
        "resourceArn": resource_arn,
        "tags": [{"Key": "bad", "Value": "tag"}],
    }))
    _assert_repository_not_found(ecr_svc._list_tags_for_resource({"resourceArn": resource_arn}))
    _assert_repository_not_found(ecr_svc._untag_resource({
        "resourceArn": resource_arn,
        "tagKeys": ["existing"],
    }))

    tags = _body(ecr_svc._list_tags_for_resource({"resourceArn": valid_arn}))["tags"]
    assert tags == [{"Key": "existing", "Value": "tag"}]
