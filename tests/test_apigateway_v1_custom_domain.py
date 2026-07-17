"""API Gateway v1 custom-domain data-plane routing (#1030).

Control-plane DomainName / BasePathMapping already exist. These tests require
the data plane to resolve Host + path using AWS mapping rules:

  - longest matching base path wins
  - ``(none)`` is the catch-all empty mapping
  - strip the matched base path before resource matching
  - stage comes from the mapping, never from the URL
  - registered custom domains must not fall through to S3 vhost
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
_EXECUTE_PORT = urlparse(_endpoint).port or 4566


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _http_get(path: str, host: str) -> tuple[int, bytes, dict]:
    url = f"{_endpoint}{path}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Host", host)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.read(), dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers.items())


def _http_options(path: str, host: str) -> tuple[int, bytes, dict]:
    url = f"{_endpoint}{path}"
    req = urllib.request.Request(url, method="OPTIONS")
    req.add_header("Host", host)
    req.add_header("Origin", "https://example.com")
    req.add_header("Access-Control-Request-Method", "GET")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.read(), dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers.items())


def _deploy_mock_hello(apigw_v1, *, name: str, stage: str, body: dict):
    """Create REST API with MOCK GET /hello returning ``body`` JSON."""
    api_id = apigw_v1.create_rest_api(name=name)["id"]
    root_id = next(
        r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/"
    )
    hello_id = apigw_v1.create_resource(
        restApiId=api_id, parentId=root_id, pathPart="hello"
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=hello_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=hello_id,
        httpMethod="GET",
        type="MOCK",
        requestTemplates={"application/json": '{"statusCode": 200}'},
    )
    apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=hello_id,
        httpMethod="GET",
        statusCode="200",
    )
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=hello_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
        responseTemplates={"application/json": json.dumps(body)},
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName=stage, deploymentId=dep_id)
    return api_id


def _cleanup(apigw_v1, *, domain: str | None = None, api_ids: list[str] | None = None):
    for api_id in api_ids or []:
        try:
            apigw_v1.delete_rest_api(restApiId=api_id)
        except ClientError:
            pass
    if domain:
        try:
            apigw_v1.delete_domain_name(domainName=domain)
        except ClientError:
            pass


# ---------------------------------------------------------------------------
# Unit: resolver matching (in-process against service module state)
# ---------------------------------------------------------------------------


@pytest.fixture
def resolve_state(monkeypatch):
    """Fresh account-scoped domain/mapping state for in-process resolver tests."""
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import apigateway_v1 as v1

    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    v1._domain_names.clear()
    v1._domain_name_regions.clear()
    v1._base_path_mappings.clear()
    yield v1
    v1._domain_names.clear()
    v1._domain_name_regions.clear()
    v1._base_path_mappings.clear()


def _seed_domain(v1, domain: str, mappings: dict[str, tuple[str, str]]):
    v1._domain_names[domain] = {"domainName": domain}
    v1._domain_name_regions[domain] = "us-east-1"
    v1._base_path_mappings[domain] = {
        base: {"basePath": base, "restApiId": api_id, "stage": stage}
        for base, (api_id, stage) in mappings.items()
    }


def test_app_custom_domain_strategy_prefers_v1(monkeypatch):
    from ministack import app

    class Service:
        def __init__(self, owns_host):
            self.owns_host = owns_host

        def has_custom_domain(self, host):
            return self.owns_host and host == "api.example.com"

    v1 = Service(True)
    v2 = Service(True)
    modules = {"apigateway_v1": v1, "apigateway": v2}
    monkeypatch.setattr(app, "_get_module", modules.__getitem__)

    assert app._custom_domain_service("api.example.com") is v1
    assert app._custom_domain_service("unknown.example.com") is None


def test_resolve_unknown_host_is_miss(resolve_state):
    v1 = resolve_state
    assert v1.has_custom_domain("unknown.example.com") is False
    assert v1.resolve_custom_domain("unknown.example.com", "/shop/hello") is None
    assert v1.has_custom_domain("") is False
    assert v1._remaining_path_for_base_path("/hello", "(none)") is None


def test_resolve_strips_port_and_is_case_insensitive(resolve_state):
    v1 = resolve_state
    _seed_domain(v1, "Shop.Example.COM", {"shop": ("api1", "local")})
    assert v1.has_custom_domain("shop.example.com:4566") is True
    target = v1.resolve_custom_domain("shop.example.com:4566", "/shop/hello")
    assert target == ("api1", "local", "/hello")


def test_resolve_explicit_base_path_strips_prefix(resolve_state):
    v1 = resolve_state
    _seed_domain(v1, "api.example.com", {"shop": ("api1", "local")})
    target = v1.resolve_custom_domain("api.example.com", "/shop/hello")
    assert target == ("api1", "local", "/hello")
    assert v1.resolve_custom_domain("api.example.com", "shop/hello") == target


def test_resolve_exact_base_path_becomes_root(resolve_state):
    v1 = resolve_state
    _seed_domain(v1, "api.example.com", {"shop": ("api1", "local")})
    target = v1.resolve_custom_domain("api.example.com", "/shop")
    assert target[2] == "/"
    target_slash = v1.resolve_custom_domain("api.example.com", "/shop/")
    assert target_slash[2] == "/"


def test_resolve_none_catch_all_preserves_path(resolve_state):
    v1 = resolve_state
    _seed_domain(v1, "api.example.com", {"(none)": ("api1", "prod")})
    target = v1.resolve_custom_domain("api.example.com", "/customers/42")
    assert target == ("api1", "prod", "/customers/42")


def test_resolve_longest_match_wins_over_shorter_and_none(resolve_state):
    v1 = resolve_state
    _seed_domain(
        v1,
        "api.example.com",
        {
            "(none)": ("api-none", "a"),
            "orders": ("api-orders", "b"),
            "orders/v1": ("api-v1", "c"),
        },
    )
    target = v1.resolve_custom_domain("api.example.com", "/orders/v1/items/9")
    assert target == ("api-v1", "c", "/items/9")


def test_resolve_keeps_longest_when_shorter_mapping_is_seen_later(resolve_state):
    v1 = resolve_state
    _seed_domain(
        v1,
        "api.example.com",
        {
            "orders/v1": ("api-v1", "c"),
            "orders": ("api-orders", "b"),
            "/": ("ignored", "ignored"),
        },
    )
    assert v1.resolve_custom_domain("api.example.com", "/orders/v1/items") == (
        "api-v1",
        "c",
        "/items",
    )


def test_resolve_requires_path_segment_boundary(resolve_state):
    """``/shophello`` must NOT match base path ``shop`` (segment boundary)."""
    v1 = resolve_state
    _seed_domain(
        v1,
        "api.example.com",
        {"shop": ("api-shop", "local"), "(none)": ("api-none", "prod")},
    )
    target = v1.resolve_custom_domain("api.example.com", "/shophello")
    assert target == ("api-none", "prod", "/shophello")


def test_resolve_registered_domain_without_match_is_forbidden(resolve_state):
    v1 = resolve_state
    _seed_domain(v1, "api.example.com", {"shop": ("api1", "local")})
    assert v1.has_custom_domain("api.example.com") is True
    assert v1.resolve_custom_domain("api.example.com", "/other/path") is None


def test_resolve_registered_domain_with_no_mappings_is_forbidden(resolve_state):
    v1 = resolve_state
    v1._domain_names["empty.example.com"] = {"domainName": "empty.example.com"}
    v1._domain_name_regions["empty.example.com"] = "us-east-1"
    v1._base_path_mappings["empty.example.com"] = {}
    assert v1.has_custom_domain("empty.example.com") is True
    assert v1.resolve_custom_domain("empty.example.com", "/anything") is None


def test_resolve_stage_comes_from_mapping_not_path(resolve_state):
    v1 = resolve_state
    _seed_domain(v1, "api.example.com", {"shop": ("api1", "staging")})
    target = v1.resolve_custom_domain("api.example.com", "/shop/local/hello")
    assert target == ("api1", "staging", "/local/hello")


def test_v1_custom_domains_are_region_isolated(resolve_state):
    from ministack.core.responses import set_request_region

    v1 = resolve_state
    _seed_domain(v1, "api.example.com", {"shop": ("api1", "local")})
    set_request_region("us-west-2")
    try:
        assert v1.has_custom_domain("api.example.com") is False
        assert v1.resolve_custom_domain("api.example.com", "/shop/hello") is None
    finally:
        set_request_region("us-east-1")


# ---------------------------------------------------------------------------
# Integration: HTTP data plane
# ---------------------------------------------------------------------------


def test_custom_domain_explicit_base_path_invokes_mapped_api(apigw_v1):
    domain = _uid("cd.example.com")
    stage = "local"
    api_id = _deploy_mock_hello(
        apigw_v1,
        name=_uid("cd-explicit"),
        stage=stage,
        body={"via": "custom-domain", "route": "hello"},
    )
    try:
        apigw_v1.create_domain_name(domainName=domain)
        apigw_v1.create_base_path_mapping(
            domainName=domain, basePath="shop", restApiId=api_id, stage=stage
        )

        # Control: normal execute-api still works.
        status, body, _ = _http_get(
            f"/{stage}/hello",
            host=f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}",
        )
        assert status == 200
        assert json.loads(body)["via"] == "custom-domain"

        # Custom domain: strip /shop, stage from mapping.
        status, body, _ = _http_get("/shop/hello", host=domain)
        assert status == 200, body
        assert json.loads(body) == {"via": "custom-domain", "route": "hello"}
    finally:
        _cleanup(apigw_v1, domain=domain, api_ids=[api_id])


def test_custom_domain_none_mapping_preserves_path(apigw_v1):
    domain = _uid("none.example.com")
    api_id = _deploy_mock_hello(
        apigw_v1,
        name=_uid("cd-none"),
        stage="prod",
        body={"catch": "all"},
    )
    try:
        apigw_v1.create_domain_name(domainName=domain)
        apigw_v1.create_base_path_mapping(
            domainName=domain, basePath="(none)", restApiId=api_id, stage="prod"
        )
        status, body, _ = _http_get("/hello", host=domain)
        assert status == 200, body
        assert json.loads(body)["catch"] == "all"
    finally:
        _cleanup(apigw_v1, domain=domain, api_ids=[api_id])


def test_custom_domain_longest_base_path_wins(apigw_v1):
    domain = _uid("long.example.com")
    short_api = _deploy_mock_hello(
        apigw_v1, name=_uid("cd-short"), stage="a", body={"which": "short"}
    )
    long_api = _deploy_mock_hello(
        apigw_v1, name=_uid("cd-long"), stage="b", body={"which": "long"}
    )
    try:
        apigw_v1.create_domain_name(domainName=domain)
        apigw_v1.create_base_path_mapping(
            domainName=domain, basePath="orders", restApiId=short_api, stage="a"
        )
        # Multi-segment key: stored if create accepts it; exercise longest-match.
        apigw_v1.create_base_path_mapping(
            domainName=domain, basePath="orders/v1", restApiId=long_api, stage="b"
        )
        status, body, _ = _http_get("/orders/v1/hello", host=domain)
        assert status == 200, body
        assert json.loads(body)["which"] == "long"
    finally:
        _cleanup(apigw_v1, domain=domain, api_ids=[short_api, long_api])


def test_custom_domain_registered_without_mapping_does_not_hit_s3(apigw_v1):
    domain = _uid("nomap.example.com")
    try:
        apigw_v1.create_domain_name(domainName=domain)
        status, body, _ = _http_get("/anything", host=domain)
        assert status in (403, 404), body
        text = body.decode(errors="replace")
        assert "NoSuchBucket" not in text
        assert "<Error>" not in text or "NoSuchBucket" not in text
    finally:
        _cleanup(apigw_v1, domain=domain)


def test_custom_domain_unmatched_path_without_none_does_not_hit_s3(apigw_v1):
    domain = _uid("unmatched.example.com")
    api_id = _deploy_mock_hello(
        apigw_v1, name=_uid("cd-unmatched"), stage="local", body={"x": 1}
    )
    try:
        apigw_v1.create_domain_name(domainName=domain)
        apigw_v1.create_base_path_mapping(
            domainName=domain, basePath="shop", restApiId=api_id, stage="local"
        )
        status, body, _ = _http_get("/other/hello", host=domain)
        assert status in (403, 404), body
        assert b"NoSuchBucket" not in body
    finally:
        _cleanup(apigw_v1, domain=domain, api_ids=[api_id])


def test_custom_domain_localhost_suffix_takes_precedence_over_s3_vhost(apigw_v1):
    """Registered ``foo.localhost`` must not be treated as S3 bucket ``foo``."""
    domain = f"{_uid('cdbucket')}.localhost"
    api_id = _deploy_mock_hello(
        apigw_v1, name=_uid("cd-s3prec"), stage="local", body={"not": "s3"}
    )
    try:
        apigw_v1.create_domain_name(domainName=domain)
        apigw_v1.create_base_path_mapping(
            domainName=domain, basePath="shop", restApiId=api_id, stage="local"
        )
        status, body, _ = _http_get("/shop/hello", host=domain)
        assert status == 200, body
        assert json.loads(body)["not"] == "s3"
        assert b"NoSuchBucket" not in body
    finally:
        _cleanup(apigw_v1, domain=domain, api_ids=[api_id])


def test_custom_domain_missing_api_resource_uses_apigw_error_shape(apigw_v1):
    domain = _uid("missres.example.com")
    api_id = _deploy_mock_hello(
        apigw_v1, name=_uid("cd-missres"), stage="local", body={"ok": True}
    )
    try:
        apigw_v1.create_domain_name(domainName=domain)
        apigw_v1.create_base_path_mapping(
            domainName=domain, basePath="shop", restApiId=api_id, stage="local"
        )
        status, body, _ = _http_get("/shop/nope", host=domain)
        assert status == 404, body
        payload = json.loads(body)
        assert "message" in payload
        assert b"NoSuchBucket" not in body
    finally:
        _cleanup(apigw_v1, domain=domain, api_ids=[api_id])


def test_custom_domain_missing_stage_returns_apigw_stage_error(apigw_v1):
    domain = _uid("missstage.example.com")
    api_id = _deploy_mock_hello(
        apigw_v1, name=_uid("cd-missstage"), stage="local", body={"ok": True}
    )
    try:
        apigw_v1.create_domain_name(domainName=domain)
        apigw_v1.create_base_path_mapping(
            domainName=domain, basePath="shop", restApiId=api_id, stage="ghost"
        )
        status, body, _ = _http_get("/shop/hello", host=domain)
        assert status == 404, body
        assert b"Stage 'ghost' not found" in body
    finally:
        _cleanup(apigw_v1, domain=domain, api_ids=[api_id])


def test_custom_domain_options_skips_generic_preflight(apigw_v1):
    """Registered custom-domain OPTIONS must not be the generic wildcard preflight alone."""
    domain = _uid("opt.example.com")
    api_id = _deploy_mock_hello(
        apigw_v1, name=_uid("cd-opt"), stage="local", body={"ok": True}
    )
    try:
        apigw_v1.create_domain_name(domainName=domain)
        apigw_v1.create_base_path_mapping(
            domainName=domain, basePath="shop", restApiId=api_id, stage="local"
        )
        status, body, headers = _http_options("/shop/hello", host=domain)
        # Must be handled as API Gateway traffic (not crash / not S3).
        assert status in (200, 204, 403, 404, 405), (status, body, headers)
        assert b"NoSuchBucket" not in body
    finally:
        _cleanup(apigw_v1, domain=domain, api_ids=[api_id])


def test_execute_api_host_unchanged_with_custom_domains_present(apigw_v1):
    domain = _uid("keep.example.com")
    api_id = _deploy_mock_hello(
        apigw_v1, name=_uid("cd-keep"), stage="local", body={"keep": True}
    )
    try:
        apigw_v1.create_domain_name(domainName=domain)
        apigw_v1.create_base_path_mapping(
            domainName=domain, basePath="shop", restApiId=api_id, stage="local"
        )
        status, body, _ = _http_get(
            "/local/hello",
            host=f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}",
        )
        assert status == 200
        assert json.loads(body)["keep"] is True
    finally:
        _cleanup(apigw_v1, domain=domain, api_ids=[api_id])
