"""API Gateway v2 DomainName / ApiMapping + custom-domain data plane (#1030).

v2 uses ApiMappingKey (empty string for root), not v1's ``(none)``.
"""

from __future__ import annotations

import io
import json
import os
import urllib.error
import urllib.request
import uuid
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
_EXECUTE_PORT = urlparse(_endpoint).port or 4566


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


def _cleanup(apigw, *, domain: str | None = None, api_ids: list[str] | None = None):
    if domain:
        try:
            apigw.delete_domain_name(DomainName=domain)
        except ClientError:
            pass
    for api_id in api_ids or []:
        try:
            apigw.delete_api(ApiId=api_id)
        except ClientError:
            pass


def _deploy_http_hello(
    apigw, lam, *, body: dict, stage: str = "$default", echo_domain: bool = False
) -> str:
    fname = _uid("v2cd-fn")
    response_expr = (
        "{'domainName': event['requestContext']['domainName'], "
        "'domainPrefix': event['requestContext']['domainPrefix']}"
        if echo_domain
        else json.dumps(body)
    )
    code = (
        "import json\n"
        "def handler(event, context):\n"
        f"    return {{'statusCode': 200, 'headers': {{'Content-Type': 'application/json'}},"
        f" 'body': json.dumps({response_expr})}}\n"
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
    api_id = apigw.create_api(Name=_uid("v2cd-api"), ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /hello", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName=stage)
    return api_id


# ---------------------------------------------------------------------------
# Control plane: DomainName + ApiMapping CRUD
# ---------------------------------------------------------------------------


def test_v2_create_domain_name_input_contract(apigw):
    model = apigw.meta.service_model
    create = model.operation_model("CreateDomainName").input_shape.members
    output = model.operation_model("CreateDomainName").output_shape.members
    assert "CertificateArn" not in create
    assert "ApiMappingSelectionExpression" not in create
    assert "DomainNameConfigurations" in create
    assert "ApiMappingSelectionExpression" in output

    from ministack.services import apigateway as apigw_svc

    domain = _uid("v2contract.example.com")
    try:
        status, _, body = apigw_svc._create_domain_name({
            "domainName": domain,
            "certificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/smuggled",
            "apiMappingSelectionExpression": "$request.path",
        })
        assert status == 201
        dn = json.loads(body)
        assert dn["apiMappingSelectionExpression"] == "$request.basepath"
        cfg = dn["domainNameConfigurations"][0]
        assert cfg.get("certificateArn", "") == ""
    finally:
        apigw_svc._domain_names.pop(domain, None)
        apigw_svc._domain_name_regions.pop(domain, None)
        apigw_svc._api_mappings.pop(domain, None)


def test_v2_create_get_list_delete_domain_name(apigw):
    domain = _uid("v2.example.com")
    try:
        created = apigw.create_domain_name(DomainName=domain)
        assert created["DomainName"] == domain
        config = created["DomainNameConfigurations"][0]
        assert config["EndpointType"] == "REGIONAL"
        assert config["SecurityPolicy"] == "TLS_1_2"
        assert config["ApiGatewayDomainName"]
        assert config["HostedZoneId"]
        assert config["IpAddressType"] == "ipv4"

        got = apigw.get_domain_name(DomainName=domain)
        assert got["DomainName"] == domain

        listed = apigw.get_domain_names()
        names = {item["DomainName"] for item in listed.get("Items", [])}
        assert domain in names

        apigw.delete_domain_name(DomainName=domain)
        with pytest.raises(ClientError) as exc:
            apigw.get_domain_name(DomainName=domain)
        assert exc.value.response["Error"]["Code"] in (
            "NotFoundException",
            "NotFound",
        )
    finally:
        _cleanup(apigw, domain=domain)


def test_v2_update_domain_name(apigw):
    domain = _uid("v2upd.example.com")
    try:
        created = apigw.create_domain_name(
            DomainName=domain,
            MutualTlsAuthentication={"TruststoreUri": "s3://bucket/original.pem"},
            RoutingMode="ROUTING_RULE_THEN_API_MAPPING",
        )
        assert created["MutualTlsAuthentication"]["TruststoreUri"].endswith(
            "original.pem"
        )
        assert created["RoutingMode"] == "ROUTING_RULE_THEN_API_MAPPING"
        updated = apigw.update_domain_name(
            DomainName=domain,
            DomainNameConfigurations=[
                {
                    "CertificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/fake",
                    "EndpointType": "REGIONAL",
                    "SecurityPolicy": "TLS_1_2",
                }
            ],
            MutualTlsAuthentication={"TruststoreUri": "s3://bucket/truststore.pem"},
            RoutingMode="API_MAPPING_ONLY",
        )
        assert updated["DomainName"] == domain
        configs = updated.get("DomainNameConfigurations") or []
        assert configs
        assert configs[0].get("CertificateArn", "").endswith("/fake")
        assert updated["MutualTlsAuthentication"]["TruststoreUri"].endswith(
            "truststore.pem"
        )
        assert updated["RoutingMode"] == "API_MAPPING_ONLY"
    finally:
        _cleanup(apigw, domain=domain)


def test_v2_create_get_list_update_delete_api_mapping(apigw, lam):
    domain = _uid("v2map.example.com")
    api_id = _deploy_http_hello(apigw, lam, body={"ok": True})
    try:
        apigw.create_domain_name(DomainName=domain)
        mapping = apigw.create_api_mapping(
            DomainName=domain,
            ApiId=api_id,
            Stage="$default",
            ApiMappingKey="shop",
        )
        assert mapping["ApiId"] == api_id
        assert mapping["Stage"] == "$default"
        assert mapping["ApiMappingKey"] == "shop"
        mapping_id = mapping["ApiMappingId"]

        got = apigw.get_api_mapping(DomainName=domain, ApiMappingId=mapping_id)
        assert got["ApiMappingId"] == mapping_id

        listed = apigw.get_api_mappings(DomainName=domain)
        assert any(m["ApiMappingId"] == mapping_id for m in listed.get("Items", []))

        updated = apigw.update_api_mapping(
            DomainName=domain,
            ApiMappingId=mapping_id,
            ApiId=api_id,
            ApiMappingKey="store",
        )
        assert updated["ApiMappingKey"] == "store"

        apigw.delete_api_mapping(DomainName=domain, ApiMappingId=mapping_id)
        with pytest.raises(ClientError):
            apigw.get_api_mapping(DomainName=domain, ApiMappingId=mapping_id)
    finally:
        _cleanup(apigw, domain=domain, api_ids=[api_id])


def test_v2_duplicate_api_mapping_key_conflicts(apigw, lam):
    domain = _uid("v2dup.example.com")
    api_id = _deploy_http_hello(apigw, lam, body={"ok": True})
    try:
        apigw.create_domain_name(DomainName=domain)
        apigw.create_api_mapping(
            DomainName=domain, ApiId=api_id, Stage="$default", ApiMappingKey="shop"
        )
        with pytest.raises(ClientError) as exc:
            apigw.create_api_mapping(
                DomainName=domain, ApiId=api_id, Stage="$default", ApiMappingKey="shop"
            )
        assert exc.value.response["Error"]["Code"] in ("ConflictException", "Conflict")
    finally:
        _cleanup(apigw, domain=domain, api_ids=[api_id])


def test_v2_empty_api_mapping_key_is_root(apigw, lam):
    domain = _uid("v2root.example.com")
    api_id = _deploy_http_hello(apigw, lam, body={"root": True})
    try:
        apigw.create_domain_name(DomainName=domain)
        mapping = apigw.create_api_mapping(
            DomainName=domain, ApiId=api_id, Stage="$default"
        )
        assert mapping.get("ApiMappingKey", "") == ""
    finally:
        _cleanup(apigw, domain=domain, api_ids=[api_id])


def test_v2_domain_and_mapping_errors_are_aws_shaped(resolve_state):
    v2 = resolve_state

    status, _, _ = v2._create_domain_name({})
    assert status == 400
    status, _, configured_body = v2._create_domain_name(
        {
            "domainName": "configured.example.com",
            "mutualTlsAuthentication": {"truststoreUri": "s3://bucket/store.pem"},
            "routingMode": "API_MAPPING_ONLY",
        }
    )
    assert status == 201
    configured = json.loads(configured_body)
    assert configured["mutualTlsAuthentication"]["truststoreUri"].endswith(
        "store.pem"
    )
    assert configured["routingMode"] == "API_MAPPING_ONLY"
    status, _, _ = v2._get_domain_name("missing.example.com")
    assert status == 404
    status, _, _ = v2._update_domain_name("missing.example.com", {})
    assert status == 404
    status, _, _ = v2._delete_domain_name("missing.example.com")
    assert status == 404

    v2._create_domain_name({"domainName": "api.example.com"})
    status, _, _ = v2._create_domain_name({"domainName": "api.example.com"})
    assert status == 409
    status, _, _ = v2._create_api_mapping("missing.example.com", {})
    assert status == 404
    status, _, _ = v2._create_api_mapping("api.example.com", {})
    assert status == 400
    status, _, _ = v2._create_api_mapping(
        "api.example.com", {"apiId": "missing", "stage": "$default"}
    )
    assert status == 404
    _, _, api_body = v2._create_api({"name": "one", "protocolType": "HTTP"})
    api_id = json.loads(api_body)["apiId"]
    status, _, _ = v2._create_api_mapping(
        "api.example.com", {"apiId": api_id, "stage": "missing"}
    )
    assert status == 404

    status, _, _ = v2._get_api_mappings("missing.example.com")
    assert status == 404
    status, _, _ = v2._get_api_mapping("missing.example.com", "missing")
    assert status == 404
    status, _, _ = v2._get_api_mapping("api.example.com", "missing")
    assert status == 404
    status, _, _ = v2._update_api_mapping("missing.example.com", "missing", {})
    assert status == 404
    status, _, _ = v2._update_api_mapping("api.example.com", "missing", {})
    assert status == 404
    status, _, _ = v2._delete_api_mapping("missing.example.com", "missing")
    assert status == 404
    status, _, _ = v2._delete_api_mapping("api.example.com", "missing")
    assert status == 404


def test_v2_update_api_mapping_is_atomic_on_invalid_target(resolve_state):
    v2 = resolve_state
    v2._create_domain_name({"domainName": "api.example.com"})
    _, _, api_body = v2._create_api({"name": "one", "protocolType": "HTTP"})
    api_id = json.loads(api_body)["apiId"]
    v2._create_stage(api_id, {"stageName": "$default"})
    _, _, mapping_body = v2._create_api_mapping(
        "api.example.com",
        {"apiId": api_id, "stage": "$default", "apiMappingKey": "shop"},
    )
    mapping = json.loads(mapping_body)

    status, _, _ = v2._update_api_mapping(
        "api.example.com",
        mapping["apiMappingId"],
        {"apiMappingKey": "changed", "apiId": "missing"},
    )
    assert status == 404
    stored = v2._api_mappings["api.example.com"][mapping["apiMappingId"]]
    assert stored["apiMappingKey"] == "shop"
    assert stored["apiId"] == api_id

    status, _, _ = v2._update_api_mapping(
        "api.example.com",
        mapping["apiMappingId"],
        {"apiId": api_id, "stage": "missing"},
    )
    assert status == 404
    assert stored["stage"] == "$default"

    _, _, other_body = v2._create_api_mapping(
        "api.example.com",
        {"apiId": api_id, "stage": "$default", "apiMappingKey": "other"},
    )
    other = json.loads(other_body)
    status, _, _ = v2._update_api_mapping(
        "api.example.com",
        other["apiMappingId"],
        {"apiId": api_id, "apiMappingKey": "shop"},
    )
    assert status == 409
    assert v2._api_mappings["api.example.com"][other["apiMappingId"]][
        "apiMappingKey"
    ] == "other"


def test_v2_custom_domain_state_persists_and_is_deep_copied(resolve_state):
    v2 = resolve_state
    v2._create_domain_name({"domainName": "api.example.com"})
    snapshot = v2.get_state()
    snapshot["domain_names"]["api.example.com"]["tags"]["mutated"] = "outside"
    assert "mutated" not in v2._domain_names["api.example.com"]["tags"]

    v2.reset()
    v2.load_persisted_state(snapshot)
    assert v2.has_custom_domain("api.example.com")
    assert v2._domain_names["api.example.com"]["tags"]["mutated"] == "outside"


def test_v2_custom_domains_are_region_isolated(resolve_state):
    from ministack.core.responses import set_request_region

    v2 = resolve_state
    v2._create_domain_name({"domainName": "api.example.com"})
    set_request_region("us-west-2")
    try:
        assert v2.has_custom_domain("api.example.com") is False
        assert v2.resolve_custom_domain("api.example.com", "/hello") is None
        status, _, _ = v2._get_domain_name("api.example.com")
        assert status == 404
        assert json.loads(v2._get_domain_names()[2])["items"] == []
    finally:
        set_request_region("us-east-1")


def test_v2_custom_domains_are_account_isolated(resolve_state):
    from ministack.core.responses import set_request_account_id

    v2 = resolve_state
    v2._create_domain_name({"domainName": "api.example.com"})
    set_request_account_id("111111111111")
    try:
        assert v2.has_custom_domain("api.example.com") is False
        status, _, _ = v2._get_domain_name("api.example.com")
        assert status == 404
        status, _, _ = v2._create_domain_name({"domainName": "api.example.com"})
        assert status == 201
    finally:
        set_request_account_id("000000000000")
    assert v2.has_custom_domain("api.example.com") is True


def test_v2_mapped_api_and_stage_cannot_be_deleted(resolve_state):
    v2 = resolve_state
    v2._create_domain_name({"domainName": "api.example.com"})
    _, _, api_body = v2._create_api({"name": "one", "protocolType": "HTTP"})
    api_id = json.loads(api_body)["apiId"]
    v2._create_stage(api_id, {"stageName": "$default"})
    v2._create_api_mapping(
        "api.example.com",
        {"apiId": api_id, "stage": "$default", "apiMappingKey": ""},
    )

    status, _, _ = v2._delete_stage(api_id, "$default")
    assert status == 409
    assert "$default" in v2._stages[api_id]

    status, _, _ = v2._delete_api(api_id)
    assert status == 409
    assert api_id in v2._apis

    _, _, other_body = v2._create_api({"name": "other", "protocolType": "HTTP"})
    other_api_id = json.loads(other_body)["apiId"]
    assert v2._find_api_mapping(other_api_id) is None


# ---------------------------------------------------------------------------
# Unit: resolver
# ---------------------------------------------------------------------------


@pytest.fixture
def resolve_state(monkeypatch):
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import apigateway as v2

    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    v2.reset()
    yield v2
    v2.reset()


def _seed_v2_domain(v2, domain: str, mappings: dict[str, tuple[str, str]]):
    """mappings: apiMappingKey -> (api_id, stage). Empty key is root catch-all."""
    v2._domain_names[domain] = {"domainName": domain}
    v2._domain_name_regions[domain] = "us-east-1"
    stored = {}
    for key, (api_id, stage) in mappings.items():
        mid = f"map-{key or 'root'}"
        stored[mid] = {
            "apiMappingId": mid,
            "apiMappingKey": key,
            "apiId": api_id,
            "stage": stage,
        }
    v2._api_mappings[domain] = stored


def test_v2_resolve_unknown_host_is_miss(resolve_state):
    v2 = resolve_state
    assert v2.has_custom_domain("unknown.example.com") is False
    assert v2.has_custom_domain("") is False
    assert v2.resolve_custom_domain("unknown.example.com", "/shop/hello") is None
    assert v2._normalize_api_mapping_key(None) == ""
    assert v2._remaining_path_for_mapping_key("/hello", "") is None


def test_v2_resolve_empty_key_preserves_path(resolve_state):
    v2 = resolve_state
    _seed_v2_domain(v2, "api.example.com", {"": ("api1", "$default")})
    assert v2.has_custom_domain("API.EXAMPLE.COM:4566") is True
    target = v2.resolve_custom_domain("api.example.com", "/hello")
    assert target == ("api1", "$default", "/hello")


def test_v2_resolve_explicit_key_strips_prefix(resolve_state):
    v2 = resolve_state
    _seed_v2_domain(v2, "api.example.com", {"shop": ("api1", "$default")})
    target = v2.resolve_custom_domain("api.example.com:4566", "/shop/hello")
    assert target == ("api1", "$default", "/hello")


def test_v2_resolve_exact_key_becomes_root_and_normalizes_path(resolve_state):
    v2 = resolve_state
    _seed_v2_domain(v2, "api.example.com", {"shop": ("api1", "$default")})
    assert v2.resolve_custom_domain("api.example.com", "/shop") == (
        "api1",
        "$default",
        "/",
    )
    assert v2.resolve_custom_domain("api.example.com", "/shop/") == (
        "api1",
        "$default",
        "/",
    )
    assert v2.resolve_custom_domain("api.example.com", "shop/hello") == (
        "api1",
        "$default",
        "/hello",
    )


def test_v2_resolve_requires_path_segment_boundary(resolve_state):
    v2 = resolve_state
    _seed_v2_domain(
        v2,
        "api.example.com",
        {"shop": ("api-shop", "prod"), "": ("api-root", "$default")},
    )
    assert v2.resolve_custom_domain("api.example.com", "/shophello") == (
        "api-root",
        "$default",
        "/shophello",
    )


def test_v2_resolve_longest_key_wins(resolve_state):
    v2 = resolve_state
    _seed_v2_domain(
        v2,
        "api.example.com",
        {
            "": ("api-root", "$default"),
            "orders": ("api-orders", "prod"),
            "orders/v1": ("api-v1", "prod"),
        },
    )
    target = v2.resolve_custom_domain("api.example.com", "/orders/v1/items/9")
    assert target == ("api-v1", "prod", "/items/9")


def test_v2_resolve_registered_without_match_is_forbidden(resolve_state):
    v2 = resolve_state
    _seed_v2_domain(v2, "api.example.com", {"shop": ("api1", "$default")})
    assert v2.has_custom_domain("api.example.com") is True
    assert v2.resolve_custom_domain("api.example.com", "/other") is None


# ---------------------------------------------------------------------------
# Integration: HTTP data plane
# ---------------------------------------------------------------------------


def test_v2_custom_domain_explicit_key_invokes_mapped_api(apigw, lam):
    domain = _uid("v2cd.example.com")
    api_id = _deploy_http_hello(apigw, lam, body={"via": "v2-custom-domain"})
    try:
        apigw.create_domain_name(DomainName=domain)
        apigw.create_api_mapping(
            DomainName=domain, ApiId=api_id, Stage="$default", ApiMappingKey="shop"
        )

        status, body, _ = _http_get(
            "/$default/hello",
            host=f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}",
        )
        assert status == 200
        assert json.loads(body)["via"] == "v2-custom-domain"

        status, body, _ = _http_get("/shop/hello", host=domain)
        assert status == 200, body
        assert json.loads(body)["via"] == "v2-custom-domain"
    finally:
        _cleanup(apigw, domain=domain, api_ids=[api_id])


def test_v2_custom_domain_empty_key_preserves_path(apigw, lam):
    domain = _uid("v2none.example.com")
    api_id = _deploy_http_hello(apigw, lam, body={"catch": "all"})
    try:
        apigw.create_domain_name(DomainName=domain)
        apigw.create_api_mapping(DomainName=domain, ApiId=api_id, Stage="$default")
        status, body, _ = _http_get("/hello", host=domain)
        assert status == 200, body
        assert json.loads(body)["catch"] == "all"
    finally:
        _cleanup(apigw, domain=domain, api_ids=[api_id])


def test_v2_custom_domain_registered_without_mapping_does_not_hit_s3(apigw):
    domain = _uid("v2nomap.example.com")
    try:
        apigw.create_domain_name(DomainName=domain)
        status, body, _ = _http_get("/anything", host=domain)
        assert status in (403, 404), body
        assert b"NoSuchBucket" not in body
    finally:
        _cleanup(apigw, domain=domain)


def test_v2_custom_domain_unmatched_path_does_not_hit_s3(apigw, lam):
    domain = _uid("v2unmatched.example.com")
    api_id = _deploy_http_hello(apigw, lam, body={"x": 1})
    try:
        apigw.create_domain_name(DomainName=domain)
        apigw.create_api_mapping(
            DomainName=domain, ApiId=api_id, Stage="$default", ApiMappingKey="shop"
        )
        status, body, _ = _http_get("/other/hello", host=domain)
        assert status in (403, 404), body
        assert b"NoSuchBucket" not in body
    finally:
        _cleanup(apigw, domain=domain, api_ids=[api_id])


def test_v2_custom_domain_options_skips_generic_preflight(apigw):
    domain = _uid("v2options.example.com")
    try:
        apigw.create_domain_name(DomainName=domain)
        url = f"{_endpoint}/anything"
        req = urllib.request.Request(url, method="OPTIONS")
        req.add_header("Host", domain)
        req.add_header("Origin", "https://example.com")
        req.add_header("Access-Control-Request-Method", "GET")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                status, body = resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            status, body = exc.code, exc.read()
        assert status in (403, 404)
        assert b"NoSuchBucket" not in body
    finally:
        _cleanup(apigw, domain=domain)


def test_v2_custom_domain_is_reported_in_lambda_request_context(apigw, lam):
    domain = _uid("v2context.example.com")
    api_id = _deploy_http_hello(
        apigw,
        lam,
        body={},
        echo_domain=True,
    )
    try:
        apigw.create_domain_name(DomainName=domain)
        apigw.create_api_mapping(
            DomainName=domain,
            ApiId=api_id,
            Stage="$default",
            ApiMappingKey="shop",
        )
        status, body, _ = _http_get("/shop/hello", host=domain)
        assert status == 200, body
        payload = json.loads(body)
        assert payload["domainName"] == domain
        assert payload["domainPrefix"] == domain.split(".", 1)[0]
    finally:
        _cleanup(apigw, domain=domain, api_ids=[api_id])


def test_v2_path_based_execute_api_keeps_execute_api_domain_name(apigw, lam):
    """Path-based /_aws/execute-api must not report Host=localhost as domainName."""
    api_id = _deploy_http_hello(apigw, lam, body={}, echo_domain=True)
    try:
        url = f"{_endpoint}/_aws/execute-api/{api_id}/$default/hello"
        req = urllib.request.Request(url, method="GET")
        # Intentionally leave Host as the gateway host (localhost).
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                status, body = resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            status, body = exc.code, exc.read()
        assert status == 200, body
        payload = json.loads(body)
        assert payload["domainName"] == f"{api_id}.execute-api.localhost"
        assert payload["domainPrefix"] == api_id
    finally:
        _cleanup(apigw, api_ids=[api_id])
