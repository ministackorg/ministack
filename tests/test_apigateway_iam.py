# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""API Gateway management authorization through request dispatch and IAM policies."""

import asyncio
import json

import pytest

from ministack.core.iam_actions import extract_iam_action, extract_resource_arn
from ministack.core.iam_evaluator import EvalContext, evaluate, parse_policy_document


@pytest.mark.parametrize("path", ["/restapis/api/stages/prod", "/v2/apis/api/stages/prod"])
@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE"])
def test_management_actions_use_http_verbs(path, method):
    # Neither query-protocol parameters nor JSON-protocol headers may override
    # the action of this REST-only service.
    assert extract_iam_action(
        "apigateway", method, path,
        {"x-amz-target": "ApiGateway.GetStage"}, b"", {"Action": "GetStage"},
    ) == f"apigateway:{method}"


@pytest.mark.parametrize("path, resource", [
    ("/restapis", "/restapis"),
    ("/restapis/api", "/restapis/api"),
    ("/restapis/api/stages", "/restapis/api/stages"),
    ("/restapis/api/stages/prod", "/restapis/api/stages/prod"),
    ("/restapis/api/stages/prod/", "/restapis/api/stages/prod"),
    ("/restapis/api/stages/", "/restapis/api/stages"),
    ("/restapis/api/resources/root/methods/GET/integration", "/restapis/api/resources/root/methods/GET/integration"),
    ("/v2/apis", "/apis"),
    ("/v2/apis/api", "/apis/api"),
    ("/v2/apis/api/stages", "/apis/api/stages"),
    ("/v2/apis/api/stages/", "/apis/api/stages"),
    ("/v2/apis/api/stages/prod/", "/apis/api/stages/prod"),
    ("/v2/apis/api/stages/%24default", "/apis/api/stages/$default"),
    ("/v2/apis/api/stages/%24default/", "/apis/api/stages/$default"),
    ("/v2/apis/api/routes/route", "/apis/api/routes/route"),
    ("/domainnames/example.com/basepathmappings", "/domainnames/example.com/basepathmappings"),
    ("/v2/domainnames/example.com/apimappings", "/domainnames/example.com/apimappings"),
    ("/account", "/account"),
])
def test_management_resource_preserves_path(path, resource):
    assert extract_resource_arn(
        "apigateway", "GET", path, {}, b"", {}, "us-east-2", "000000000000",
    ) == f"arn:aws:apigateway:us-east-2::{resource}"


@pytest.mark.parametrize("prefix", ["/restapis/api", "/v2/apis/api"])
@pytest.mark.parametrize("suffix", ["", "/"])
@pytest.mark.parametrize("case, method, stage, expected", [
    ("stage-grant", "GET", "prod", 200),
    ("stage-grant", "PATCH", "prod", 200),
    ("stage-grant", "GET", "dev", 403),
    ("parent-grant", "GET", "prod", 403),
    ("operation-grant", "GET", "prod", 403),
    ("stage-deny", "GET", "prod", 403),
    ("stage-deny", "GET", "dev", 200),
])
def test_stage_policy_is_enforced_on_dispatch(monkeypatch, prefix, suffix, case, method, stage, expected):
    import ministack.app as app
    from ministack.core import iam_evaluator

    parent = f"arn:aws:apigateway:us-east-2::{prefix.removeprefix('/v2')}"
    grants = {
        "stage-grant": [{"Effect": "Allow", "Action": ["apigateway:GET", "apigateway:PATCH"],
                         "Resource": f"{parent}/stages/prod"}],
        "parent-grant": [{"Effect": "Allow", "Action": "apigateway:GET", "Resource": parent}],
        "operation-grant": [{"Effect": "Allow", "Action": "apigateway:GetStage", "Resource": "*"}],
        "stage-deny": [
            {"Effect": "Allow", "Action": "apigateway:*", "Resource": f"{parent}/*"},
            {"Effect": "Deny", "Action": "apigateway:GET", "Resource": f"{parent}/stages/prod"},
        ],
    }
    statements = parse_policy_document({"Statement": grants[case]})
    dispatched = []

    def enforce_policy(access_key, action, service, region, resource_arn="*", service_context=None):
        result = evaluate(EvalContext(
            principal_arn="arn:aws:iam::000000000000:user/caller",
            principal_type="User", principal_account="000000000000",
            action=action, resource_arn=resource_arn, region=region,
        ), [statements])
        return None if result.decision == "Allow" else result

    async def handler(*args):
        dispatched.append(args)
        return 200, {}, b"{}"

    monkeypatch.setattr(app, "AUTH", True)
    monkeypatch.setattr(iam_evaluator, "enforce", enforce_policy)
    monkeypatch.setattr(iam_evaluator, "pin_request_caller", lambda *args: None)
    monkeypatch.setitem(app.SERVICE_HANDLERS, "apigateway", handler)
    headers = {"authorization": "AWS4-HMAC-SHA256 Credential=test/20261006/us-east-2/apigateway/aws4_request"}
    status, _, body = asyncio.run(app._dispatch_service_request(
        method, f"{prefix}/stages/{stage}{suffix}", headers, b"", {}, "request-stage",
    ))
    assert status == expected
    assert bool(dispatched) == (expected == 200)
    if expected == 403:
        assert f"apigateway:{method}" in json.loads(body)["message"]
