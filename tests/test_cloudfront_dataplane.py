# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
CloudFront data plane tests — a distribution actually serving traffic.

A CloudFront distribution's DomainName is always AWS-shaped
(``<label>.cloudfront.net``); routing matches that host, or the resolvable
local form built on MINISTACK_HOST (``<label>.cloudfront.localhost``), which
this file uses to reach a distribution over the real wire.

One distribution (``cf_stack``) is built once per module and exercised by
most tests; it fronts a custom API Gateway HTTP API backed by a Lambda that
echoes the inbound path, headers, query string, and cookies, with several
cache behaviors covering the request-forwarding, protocol-policy, allowed-
methods, and origin-path/custom-header surfaces. A handful of tests need
their own distribution (S3 origin, a raw path-echo origin, a dead origin).
"""

import asyncio
import http.client
import http.server
import io
import json
import socket
import ssl
import threading
import uuid as _uuid_mod
import zipfile
from unittest import mock

import pytest
from conftest import GATEWAY_PORT

from ministack.core import cloudfront_js
from ministack.services import cloudfront_dataplane

# Managed policy ids (CloudFront Developer Guide, "Managed origin request
# policies" / "Managed cache policies" / "Managed response headers
# policies").
_ALL_VIEWER = "216adef6-5c7f-47e4-b989-5492eafa07d3"
_ALL_VIEWER_EXCEPT_HOST = "b689b0a8-53d0-40ab-baf2-68738e2966ac"
_CACHING_DISABLED = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
_MANAGED_CORS_WITH_PREFLIGHT = "5cc3b908-e619-4b99-88e5-2cf7f45965bd"

_ORIGIN_LAMBDA_CODE = (
    b"import json\n"
    b"def handler(event, context):\n"
    b"    path = event.get('rawPath', '')\n"
    b"    headers = event.get('headers', {})\n"
    b"    # MiniStack's apigatewayv2 (payload format 2.0) does not split an\n"
    b"    # incoming Cookie header into the top-level `cookies` array real AWS\n"
    b"    # uses; it stays in `headers['cookie']` like any other header, which\n"
    b"    # is also what cloudfront_dataplane.py actually forwards.\n"
    b"    cookie_header = headers.get('cookie', '')\n"
    b"    cookies = [c.strip() for c in cookie_header.split(';') if c.strip()]\n"
    b"    resp = {\n"
    b"        'statusCode': 200,\n"
    b"        'headers': {'Content-Type': 'application/json', 'X-Origin-Header': 'origin-value'},\n"
    b"        'body': json.dumps({\n"
    b"            'path': path,\n"
    b"            'headers': headers,\n"
    b"            'query': event.get('queryStringParameters') or {},\n"
    b"            'cookies': cookies,\n"
    b"        }),\n"
    b"    }\n"
    b"    if path == '/cookies':\n"
    b"        resp['cookies'] = ['resp1=a; Path=/', 'resp2=b; Path=/']\n"
    b"    if path == '/status/500':\n"
    b"        resp['statusCode'] = 500\n"
    b"    return resp\n"
)


def _zip_lambda(code: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


def _none_cache_policy_config(name: str) -> dict:
    return {
        "Name": name,
        "MinTTL": 0,
        "ParametersInCacheKeyAndForwardedToOrigin": {
            "HeadersConfig": {"HeaderBehavior": "none"},
            "CookiesConfig": {"CookieBehavior": "none"},
            "QueryStringsConfig": {"QueryStringBehavior": "none"},
            "EnableAcceptEncodingGzip": False,
        },
    }


def _response_headers_policy(cloudfront, name: str, custom_headers: list) -> str:
    resp = cloudfront.create_response_headers_policy(
        ResponseHeadersPolicyConfig={
            "Name": name,
            "CustomHeadersConfig": {
                "Quantity": len(custom_headers),
                "Items": [{"Header": h, "Value": v, "Override": o} for (h, v, o) in custom_headers],
            },
        }
    )
    return resp["ResponseHeadersPolicy"]["Id"]


def _create_function(cloudfront, name: str, code: bytes, runtime: str = "cloudfront-js-2.0") -> str:
    created = cloudfront.create_function(
        Name=name, FunctionConfig={"Comment": name, "Runtime": runtime}, FunctionCode=code,
    )
    published = cloudfront.publish_function(Name=name, IfMatch=created["ETag"])
    return published["FunctionSummary"]["FunctionMetadata"]["FunctionARN"]


@pytest.fixture(scope="module")
def cf_backend(apigw, lam):
    """A custom API Gateway HTTP API, backed by a Lambda that echoes the
    inbound request, addressed like a real CloudFront custom origin."""
    suffix = _uuid_mod.uuid4().hex[:10]
    lam.create_function(
        FunctionName=f"cf-dp-origin-{suffix}",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _zip_lambda(_ORIGIN_LAMBDA_CODE)},
    )
    api_id = apigw.create_api(Name=f"cf-dp-api-{suffix}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:cf-dp-origin-{suffix}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="ANY /{proxy+}", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")
    # "*.execute-api.localhost" resolves to 127.0.0.1 (RFC 6761); the origin
    # is dialed there at the gateway's own listening port, exactly as any
    # custom origin is dialed at its configured DomainName/HTTPPort.
    return {"origin_domain": f"{api_id}.execute-api.localhost"}


@pytest.fixture(scope="module")
def cf_stack(cloudfront, cf_backend):
    """One distribution wired up per the module docstring."""
    suffix = _uuid_mod.uuid4().hex[:10]
    origin_domain = cf_backend["origin_domain"]

    cp_none_id = cloudfront.create_cache_policy(
        CachePolicyConfig=_none_cache_policy_config(f"cf-dp-cp-none-{suffix}")
    )["CachePolicy"]["Id"]

    default_policy_id = _response_headers_policy(
        cloudfront, f"cf-dp-default-policy-{suffix}",
        [("X-Origin-Header", "policy-value", True), ("X-Default-Marker", "yes", False)],
    )
    csv_policy_id = _response_headers_policy(
        cloudfront, f"cf-dp-csv-policy-{suffix}", [("X-Csv-Marker", "yes", False)],
    )

    request_fn_arn = _create_function(
        cloudfront, f"cf-dp-viewer-request-{suffix}",
        b"function handler(event) {\n"
        b"  var request = event.request;\n"
        b"  if (request.uri === '/fn/blocked') {\n"
        b"    return { statusCode: 403, statusDescription: 'Forbidden',\n"
        b"             headers: { 'content-type': { value: 'text/plain' } },\n"
        b"             body: { encoding: 'text', data: 'blocked by viewer-request' } };\n"
        b"  }\n"
        b"  request.uri = request.uri.replace('/fn/', '/');\n"
        b"  return request;\n"
        b"}\n",
    )
    response_fn_arn = _create_function(
        cloudfront, f"cf-dp-viewer-response-{suffix}",
        b"function handler(event) {\n"
        b"  var response = event.response;\n"
        b"  response.headers['x-viewer-response-added'] = { value: 'yes' };\n"
        b"  return response;\n"
        b"}\n",
    )
    async_fn_arn = _create_function(
        cloudfront, f"cf-dp-async-{suffix}",
        b"async function handler(event) {\n"
        b"  var v = await Promise.resolve('yes');\n"
        b"  var request = event.request;\n"
        b"  request.headers['x-async-ran'] = { value: v };\n"
        b"  request.uri = request.uri.replace('/async/', '/');\n"
        b"  return request;\n"
        b"}\n",
    )
    error_fn_arn = _create_function(
        cloudfront, f"cf-dp-error-{suffix}",
        b"function handler(event) { throw new Error('boom'); }\n",
    )
    response_delete_multi_fn_arn = _create_function(
        cloudfront, f"cf-dp-response-delete-multi-{suffix}",
        b"function handler(event) {\n"
        b"  var response = event.response;\n"
        b"  delete response.headers['x-origin-header'];\n"
        b"  response.headers['x-multi-resp'] = { value: 'a', multiValue: [{ value: 'a' }, { value: 'b' }] };\n"
        b"  return response;\n"
        b"}\n",
    )
    viewer_ip_fn_arn = _create_function(
        cloudfront, f"cf-dp-viewer-ip-{suffix}",
        b"function handler(event) {\n"
        b"  var request = event.request;\n"
        b"  request.headers['x-viewer-ip'] = { value: event.viewer.ip };\n"
        b"  return request;\n"
        b"}\n",
    )

    def behavior(path_pattern, target_origin_id="origin-apigw", **overrides):
        b = {
            "PathPattern": path_pattern,
            "TargetOriginId": target_origin_id,
            "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": cp_none_id,
            "OriginRequestPolicyId": _ALL_VIEWER_EXCEPT_HOST,
            "ResponseHeadersPolicyId": default_policy_id,
        }
        b.update(overrides)
        return b

    dist_config = {
        "CallerReference": f"cf-dp-{suffix}",
        "Comment": "cloudfront_dataplane test distribution",
        "Enabled": True,
        "DefaultRootObject": "index.html",
        "Origins": {
            "Quantity": 2,
            "Items": [
                {
                    "Id": "origin-apigw",
                    "DomainName": origin_domain,
                    "CustomOriginConfig": {
                        "HTTPPort": int(GATEWAY_PORT), "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                        "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                        "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                    },
                },
                {
                    "Id": "origin-apigw-prefixed",
                    "DomainName": origin_domain,
                    "OriginPath": "/extra",
                    "CustomHeaders": {
                        "Quantity": 1,
                        "Items": [{"HeaderName": "X-Added-By-Origin", "HeaderValue": "yes"}],
                    },
                    "CustomOriginConfig": {
                        "HTTPPort": int(GATEWAY_PORT), "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                        "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                        "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                    },
                },
            ],
        },
        "DefaultCacheBehavior": {
            k: v for k, v in behavior("*").items() if k != "PathPattern"
        },
        "CacheBehaviors": {
            "Quantity": 8,
            "Items": [
                {**behavior("*.csv"), "ResponseHeadersPolicyId": csv_policy_id},
                {
                    **behavior("/fn/*"),
                    "FunctionAssociations": {
                        "Quantity": 2,
                        "Items": [
                            {"FunctionARN": request_fn_arn, "EventType": "viewer-request"},
                            {"FunctionARN": response_fn_arn, "EventType": "viewer-response"},
                        ],
                    },
                },
                {
                    **behavior("/async/*"),
                    "FunctionAssociations": {
                        "Quantity": 1,
                        "Items": [{"FunctionARN": async_fn_arn, "EventType": "viewer-request"}],
                    },
                },
                {
                    **behavior("/err/*"),
                    "FunctionAssociations": {
                        "Quantity": 1,
                        "Items": [{"FunctionARN": error_fn_arn, "EventType": "viewer-request"}],
                    },
                },
                {**behavior("/cd/*"), "CachePolicyId": _CACHING_DISABLED, "OriginRequestPolicyId": ""},
                {
                    **{k: v for k, v in behavior("/legacy/*").items()
                       if k not in ("CachePolicyId", "OriginRequestPolicyId")},
                    "MinTTL": 0, "DefaultTTL": 0, "MaxTTL": 0,
                    "ForwardedValues": {
                        "QueryString": True,
                        "Cookies": {"Forward": "whitelist", "WhitelistedNames": {"Quantity": 1, "Items": ["keep"]}},
                        "Headers": {"Quantity": 1, "Items": ["X-Legacy"]},
                    },
                },
                {**behavior("/restricted/*"),
                 "AllowedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]}},
                # No leading "/" — CloudFront treats it the same as "/api/*".
                behavior("api/*"),
                {
                    **behavior("/cors/*"), "ResponseHeadersPolicyId": _MANAGED_CORS_WITH_PREFLIGHT,
                    "AllowedMethods": {"Quantity": 3, "Items": ["GET", "HEAD", "OPTIONS"]},
                },
                {
                    **behavior("/respmulti/*"),
                    "FunctionAssociations": {
                        "Quantity": 1,
                        "Items": [{"FunctionARN": response_delete_multi_fn_arn, "EventType": "viewer-response"}],
                    },
                },
                {
                    **behavior("/viewerip/*"),
                    "FunctionAssociations": {
                        "Quantity": 1,
                        "Items": [{"FunctionARN": viewer_ip_fn_arn, "EventType": "viewer-request"}],
                    },
                },
            ],
        },
    }
    # "/cd/*" carries no OriginRequestPolicyId at all; the empty-string
    # placeholder above exists only so `behavior()`'s dict-merge has a key to
    # drop — CloudFront's own XML omits the element entirely for "no ORP".
    for item in dist_config["CacheBehaviors"]["Items"]:
        if item.get("OriginRequestPolicyId") == "":
            del item["OriginRequestPolicyId"]
    dist_config["CacheBehaviors"]["Items"].append(
        {**behavior("/redirect/*"), "ViewerProtocolPolicy": "redirect-to-https"}
    )
    dist_config["CacheBehaviors"]["Items"].append(
        {**behavior("/prefixed/*", target_origin_id="origin-apigw-prefixed")}
    )
    dist_config["CacheBehaviors"]["Quantity"] = len(dist_config["CacheBehaviors"]["Items"])

    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    return {"dist_host": dist["DomainName"], "origin_domain": origin_domain}


def _get(host: str, path: str, method: str = "GET", extra_headers: dict | None = None):
    """``host`` is a distribution's real AWS-shaped DomainName
    (``<label>.cloudfront.net``), sent verbatim as the Host header — exactly
    what a real viewer sends. The connection itself targets the gateway's own
    address directly, since ``*.cloudfront.net`` doesn't resolve locally (the
    same way ``_raw_get`` below reaches an execute-api/ALB host)."""
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        headers = {"Host": f"{host}:{GATEWAY_PORT}"}
        headers.update(extra_headers or {})
        conn.request(method, path, headers=headers)
        resp = conn.getresponse()
        return resp.status, dict(resp.getheaders()), resp.read()
    finally:
        conn.close()


def _skip_without_node():
    if not cloudfront_js.available():
        pytest.skip("node is not available")


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def test_routes_by_distribution_domain(cf_stack):
    status, _headers, body = _get(cf_stack["dist_host"], "/hello")
    assert status == 200
    assert json.loads(body)["path"] == "/hello"


def test_path_pattern_without_leading_slash_matches_as_if_it_had_one(cf_stack):
    # PathPattern "api/*" (no leading /) — CloudFront Developer Guide,
    # "Path pattern": "CloudFront treats the path the same with or without
    # the leading /".
    status, _headers, body = _get(cf_stack["dist_host"], "/api/hello")
    assert status == 200
    assert json.loads(body)["path"] == "/api/hello"


def test_unknown_distribution_label_is_not_served_by_cloudfront():
    # Not a 200, and not CloudFront's own data plane (no X-Cache): falls
    # through to whatever the gateway's generic router answers for an
    # otherwise-unrouted host — CloudFront's own control-plane 404, since no
    # other special-case host pattern matches either.
    status, headers, body = _get("d0000000000000.cloudfront.localhost", "/anything")
    assert "X-Cache" not in headers
    assert status == 404
    assert b"<Code>NoSuchResource</Code>" in body


# ---------------------------------------------------------------------------
# CloudFront Functions
# ---------------------------------------------------------------------------


def test_viewer_request_function_rewrites_uri(cf_stack):
    _skip_without_node()
    status, _headers, body = _get(cf_stack["dist_host"], "/fn/hello")
    assert status == 200
    assert json.loads(body)["path"] == "/hello"


def test_function_generated_response_skips_the_origin(cf_stack):
    _skip_without_node()
    status, headers, body = _get(cf_stack["dist_host"], "/fn/blocked")
    assert status == 403
    assert body == b"blocked by viewer-request"
    assert headers.get("X-Cache") == "FunctionGeneratedResponse from cloudfront"
    assert "X-Origin-Header" not in headers  # never reached the origin


def test_viewer_response_function_adds_a_header(cf_stack):
    _skip_without_node()
    status, headers, _body = _get(cf_stack["dist_host"], "/fn/hello")
    assert status == 200
    assert headers.get("X-Viewer-Response-Added") == "yes"


def test_async_2_0_handler_runs_and_awaits(cf_stack):
    _skip_without_node()
    status, _headers, body = _get(cf_stack["dist_host"], "/async/hello")
    assert status == 200
    payload = json.loads(body)
    assert payload["path"] == "/hello"
    assert payload["headers"].get("x-async-ran") == "yes"


def test_function_error_answers_503(cf_stack):
    _skip_without_node()
    status, headers, _body = _get(cf_stack["dist_host"], "/err/hello")
    assert status == 503
    assert headers.get("X-Cache") == "Error from cloudfront"


def test_viewer_response_function_skipped_when_origin_status_is_400_or_above(cf_stack):
    # functions-event-structure.html, "Status code and body" note: CloudFront
    # does not invoke a viewer-response function when the origin answers 400+.
    _skip_without_node()
    status, headers, _body = _get(cf_stack["dist_host"], "/fn/status/500")
    assert status == 500
    assert "X-Viewer-Response-Added" not in headers


def test_viewer_response_function_deletes_header_and_expands_multivalue(cf_stack):
    # Replace, not merge (a deleted header stays gone), and a multiValue
    # response header becomes repeated Set-Cookie-style lines.
    _skip_without_node()
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", "/respmulti/hello", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_stack['dist_host']}:{GATEWAY_PORT}")
        conn.endheaders()
        resp = conn.getresponse()
        resp.read()
        multi_values = resp.msg.get_all("X-Multi-Resp") or []
        has_origin_header = "X-Origin-Header" in resp.msg
    finally:
        conn.close()
    assert sorted(multi_values) == ["a", "b"]
    assert not has_origin_header


def test_function_event_viewer_ip_is_the_tcp_peer_not_the_xff_header(cf_stack):
    # functions-event-structure.html: viewer.ip is the TCP peer, regardless
    # of what X-Forwarded-For the viewer sent.
    _skip_without_node()
    status, _headers, body = _get(
        cf_stack["dist_host"], "/viewerip/hello", extra_headers={"X-Forwarded-For": "203.0.113.9"},
    )
    assert status == 200
    assert json.loads(body)["headers"].get("x-viewer-ip") == "127.0.0.1"


# ---------------------------------------------------------------------------
# Cache behaviors and response-headers policy
# ---------------------------------------------------------------------------


def test_ordered_behavior_matches_csv_glob_with_its_own_policy(cf_stack):
    status, headers, _body = _get(cf_stack["dist_host"], "/report.csv")
    assert status == 200
    assert headers.get("X-Csv-Marker") == "yes"
    assert "X-Default-Marker" not in headers


def test_response_headers_policy_overrides_origin_header_and_adds_marker(cf_stack):
    # The origin always emits X-Origin-Header: origin-value; the default
    # behavior's response-headers policy names the same header with
    # Override=True, and adds X-Default-Marker unconditionally.
    status, headers, _body = _get(cf_stack["dist_host"], "/hello")
    assert status == 200
    assert headers.get("X-Origin-Header") == "policy-value"
    assert headers.get("X-Default-Marker") == "yes"


def test_cors_headers_only_applied_when_origin_header_present(cf_stack):
    status, headers, _body = _get(cf_stack["dist_host"], "/cors/hello")
    assert status == 200
    assert "Access-Control-Allow-Origin" not in headers

    status, headers, _body = _get(
        cf_stack["dist_host"], "/cors/hello", extra_headers={"Origin": "https://example.com"},
    )
    assert status == 200
    assert headers.get("Access-Control-Allow-Origin") == "*"
    assert headers.get("Access-Control-Expose-Headers") == "*"
    # Preflight-only headers (Allow-Methods/-Headers/-Max-Age) never appear
    # on a plain GET.
    assert "Access-Control-Allow-Methods" not in headers


def test_cors_preflight_headers_only_on_options(cf_stack):
    # The response-headers policy applies to whatever status the request
    # produces (here, the backing API Gateway HTTP API's own unconfigured-
    # CORS OPTIONS rejection — orthogonal to the response-headers policy
    # under test), so only the header is asserted, not the status.
    _status, headers, _body = _get(
        cf_stack["dist_host"], "/cors/hello", method="OPTIONS",
        extra_headers={"Origin": "https://example.com"},
    )
    assert headers.get("Access-Control-Allow-Methods") == "DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT"


def test_cloudfront_added_headers_present(cf_stack):
    status, headers, _body = _get(cf_stack["dist_host"], "/hello")
    assert status == 200
    assert headers.get("X-Cache") == "Miss from cloudfront"
    assert headers.get("Via", "").endswith(".cloudfront.net (CloudFront)")
    assert "X-Amz-Cf-Id" in headers
    assert "X-Amz-Cf-Pop" in headers


# ---------------------------------------------------------------------------
# Cache-policy / origin-request-policy forwarding
# ---------------------------------------------------------------------------


def test_all_viewer_except_host_header_forwards_viewer_header_not_host(cf_stack):
    status, _headers, body = _get(
        cf_stack["dist_host"], "/hello", extra_headers={"X-Custom-Test": "present"},
    )
    assert status == 200
    payload = json.loads(body)
    assert payload["headers"].get("x-custom-test") == "present"
    assert payload["headers"].get("host") == cf_stack["origin_domain"]


def test_all_viewer_forwards_the_distributions_own_host(cf_raw_origin_stack):
    # AllViewer forwards every viewer header including Host, so the origin
    # sees the *distribution's* Host, not a default derived from its own
    # DomainName. A real network-distinct origin doesn't care; this uses the
    # raw http.server origin (not the execute-api one) because MiniStack's
    # own execute-api addressing is itself Host-routed, so forwarding the
    # viewer's Host there would recurse into the gateway's own routing
    # instead of reaching the intended origin — an artifact of this
    # emulator's single-port addressing, not of CloudFront's own behavior.
    status, _headers, body = _get(
        cf_raw_origin_stack, "/av/hello", extra_headers={"X-Custom-Test": "present"},
    )
    assert status == 200
    payload = json.loads(body)
    assert payload["headers"].get("x-custom-test") == "present"
    assert payload["headers"].get("host") == f"{cf_raw_origin_stack}:{GATEWAY_PORT}"


def test_caching_disabled_with_no_origin_request_policy_forwards_only_defaults(cf_stack):
    # CachingDisabled's ParametersInCacheKeyAndForwardedToOrigin is
    # none/none/none, and the behavior carries no origin request policy at
    # all: nothing beyond the always-included defaults (Host, User-Agent)
    # reaches the origin.
    status, _headers, body = _get(
        cf_stack["dist_host"], "/cd/hello", extra_headers={"X-Custom-Test": "present"},
    )
    assert status == 200
    payload = json.loads(body)
    assert "x-custom-test" not in payload["headers"]
    assert payload["headers"].get("host") == cf_stack["origin_domain"]


def test_x_forwarded_for_always_appends_regardless_of_policy(cf_stack):
    # CloudFront Developer Guide, "Client IP addresses": X-Forwarded-For is
    # always the viewer's own chain plus the client IP, even under
    # CachingDisabled with no origin request policy forwarding it.
    status, _headers, body = _get(
        cf_stack["dist_host"], "/cd/hello", extra_headers={"X-Forwarded-For": "203.0.113.9"},
    )
    assert status == 200
    payload = json.loads(body)
    assert payload["headers"].get("x-forwarded-for") == "203.0.113.9, 127.0.0.1"


def test_user_agent_defaults_to_amazon_cloudfront_when_not_forwarded(cf_stack):
    # CloudFront Developer Guide, "User-Agent header": replaced unconditionally
    # unless the forwarding policy carries the viewer's own.
    status, _headers, body = _get(
        cf_stack["dist_host"], "/cd/hello", extra_headers={"User-Agent": "custom-client/1.0"},
    )
    assert status == 200
    assert json.loads(body)["headers"].get("user-agent") == "Amazon CloudFront"


def test_legacy_forwarded_values_forwards_only_its_own_selections(cf_stack):
    status, _headers, body = _get(
        cf_stack["dist_host"], "/legacy/hello?a=1&b=2",
        extra_headers={"X-Legacy": "yes", "X-Not-Forwarded": "no", "Cookie": "keep=1; drop=2"},
    )
    assert status == 200
    payload = json.loads(body)
    assert payload["query"] == {"a": "1", "b": "2"}  # ForwardedValues.QueryString=true
    assert payload["headers"].get("x-legacy") == "yes"
    assert "x-not-forwarded" not in payload["headers"]
    assert payload["cookies"] == ["keep=1"]  # only the whitelisted cookie name


# ---------------------------------------------------------------------------
# _combine_forward — the cache-policy / origin-request-policy combinator
# (same function serves headers, cookies and query strings). Exercised
# end-to-end above for "all"/"none"; "whitelist" and "allExcept" are cheaper
# to prove directly against the pure combinator than via more distributions.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cache_kind_set,orp_kind_set,name,expected",
    [
        (("whitelist", {"a"}), None, "a", True),
        (("whitelist", {"a"}), None, "b", False),
        (("whitelist", {"a"}), ("whitelist", {"b"}), "b", True),  # ORP allow-list adds to cache's
        (("allExcept", {"a"}), None, "a", False),  # blocked by the cache policy
        (("allExcept", {"a"}), None, "b", True),
        (("allExcept", {"a"}), ("allExcept", {"b"}), "a", False),  # ORP allExcept doesn't rescue it
        (("allExcept", {"a"}), ("whitelist", {"a"}), "a", True),  # ORP allow-list rescues a blocked name
        (("none", None), ("allExcept", {"a"}), "a", False),
        (("none", None), ("allExcept", {"a"}), "b", True),
        (("none", None), None, "a", False),
    ],
)
def test_combine_forward_whitelist_and_allexcept_branches(cache_kind_set, orp_kind_set, name, expected):
    assert cloudfront_dataplane._combine_forward(cache_kind_set, orp_kind_set, name) == expected


# ---------------------------------------------------------------------------
# Behavior-level settings
# ---------------------------------------------------------------------------


def test_redirect_to_https_sends_301_with_https_location(cf_stack):
    status, headers, _body = _get(cf_stack["dist_host"], "/redirect/hello")
    assert status == 301
    assert headers.get("Location") == f"https://{cf_stack['dist_host']}:{GATEWAY_PORT}/redirect/hello"


def test_allowed_methods_rejects_a_disallowed_method(cf_stack):
    status, _headers, _body = _get(cf_stack["dist_host"], "/restricted/hello", method="POST")
    assert status == 403


def test_default_root_object_is_requested_for_the_root_url(cf_stack):
    status, _headers, body = _get(cf_stack["dist_host"], "/")
    assert status == 200
    assert json.loads(body)["path"] == "/index.html"


def test_origin_path_and_custom_headers_reach_the_origin(cf_stack):
    status, _headers, body = _get(cf_stack["dist_host"], "/prefixed/hello")
    assert status == 200
    payload = json.loads(body)
    assert payload["path"] == "/extra/prefixed/hello"
    assert payload["headers"].get("x-added-by-origin") == "yes"


def test_multiple_set_cookie_headers_all_carried_through(cf_stack):
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", "/cookies", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_stack['dist_host']}:{GATEWAY_PORT}")
        conn.endheaders()
        resp = conn.getresponse()
        resp.read()
        cookies = resp.msg.get_all("Set-Cookie") or []
    finally:
        conn.close()
    assert sorted(cookies) == ["resp1=a; Path=/", "resp2=b; Path=/"]


# ---------------------------------------------------------------------------
# AWS-owned origin hosts — refused before a socket opens; no server or
# distribution needed, calls the pure function. An emulator must never open
# a socket to a real AWS host.
# ---------------------------------------------------------------------------


def test_aws_owned_origin_host_refused_before_any_socket():
    origin = {
        "domain_name": "abcd1234.execute-api.us-east-1.amazonaws.com.",  # trailing dot
        "origin_path": "", "custom_headers": [], "s3_bucket": None,
        "http_port": 80, "https_port": 443, "protocol_policy": "https-only",
    }
    with mock.patch("http.client.HTTPSConnection") as https_conn, \
            mock.patch("http.client.HTTPConnection") as http_conn:
        with pytest.raises(cloudfront_dataplane.OriginUnreachable):
            cloudfront_dataplane._forward_to_origin(
                origin, "GET", "/anything", "", {}, b"", viewer_is_https=True,
            )
    https_conn.assert_not_called()
    http_conn.assert_not_called()


def test_origin_ssl_context_trusts_gateway_cert_under_use_ssl(tmp_path):
    # Precedent: lambda_svc.py's TLS trust for Lambda containers — system
    # roots plus the gateway's own cert, not a replacement of the store.
    from ministack.core.x509_utils import generate_ca

    ca_pem, _ca_key_pem = generate_ca(common_name="cf-dp-origin-ssl-test")
    cert_path = tmp_path / "ca.pem"
    cert_path.write_text(ca_pem)
    with mock.patch("ministack.core.tls.use_ssl_enabled", return_value=True), \
            mock.patch("ministack.core.tls.resolve_tls_material", return_value=(str(cert_path), "key")):
        ctx = cloudfront_dataplane._origin_ssl_context()
    assert ctx.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN
    subjects = [dict(rdn for pair in cert["subject"] for rdn in pair) for cert in ctx.get_ca_certs()]
    assert any(s.get("commonName") == "cf-dp-origin-ssl-test" for s in subjects)


def test_stacked_distribution_via_header_answers_403(cloudfront):
    # CloudFront Developer Guide, http-403-permission-denied.html, "Stacked
    # distributions cause a 403 error": an origin pointed back at this same
    # distribution (via the resolvable `<label>.cloudfront.localhost` form)
    # must answer 403 on the second hop rather than recurse into a hang.
    suffix = _uuid_mod.uuid4().hex[:10]
    dist_config = {
        "CallerReference": f"cf-dp-stacked-{suffix}",
        "Comment": "cloudfront_dataplane stacked-distribution test",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "self-origin",
                "DomainName": "placeholder.cloudfront.localhost",
                "CustomOriginConfig": {
                    "HTTPPort": int(GATEWAY_PORT), "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "self-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
    }
    created = cloudfront.create_distribution(DistributionConfig=dist_config)
    dist = created["Distribution"]
    label = dist["DomainName"].removesuffix(".cloudfront.net")
    dist_config["Origins"]["Items"][0]["DomainName"] = f"{label}.cloudfront.localhost"
    cloudfront.update_distribution(DistributionConfig=dist_config, Id=dist["Id"], IfMatch=created["ETag"])

    status, _headers, _body = _get(dist["DomainName"], "/anything")
    assert status == 403


def test_unreachable_origin_answers_502(cloudfront):
    suffix = _uuid_mod.uuid4().hex[:10]
    dist_config = {
        "CallerReference": f"cf-dp-dead-{suffix}",
        "Comment": "cloudfront_dataplane dead-origin test",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "dead-origin",
                "DomainName": "127.0.0.1",
                "CustomOriginConfig": {
                    "HTTPPort": 1, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",  # nothing listens on port 1
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "dead-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
    }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    status, headers, _body = _get(dist["DomainName"], "/anything")
    assert status == 502
    assert headers.get("X-Cache") == "Error from cloudfront"


def test_forward_to_origin_timeout_raises_origin_timeout():
    # CloudFront Developer Guide, http-504-gateway-timeout.html: "The origin
    # didn't respond before the request expired." OriginReadTimeout default
    # 30s (DownloadDistValuesOrigin.md, "Response timeout").
    origin = {
        "domain_name": "127.0.0.1", "origin_path": "", "custom_headers": [], "s3_bucket": None,
        "http_port": 1, "https_port": 443, "protocol_policy": "http-only", "read_timeout": 30,
    }
    with mock.patch("http.client.HTTPConnection") as mock_conn_cls:
        mock_conn_cls.return_value.request.side_effect = TimeoutError("timed out")
        with pytest.raises(cloudfront_dataplane.OriginTimeout):
            cloudfront_dataplane._forward_to_origin(
                origin, "GET", "/anything", "", {}, b"", viewer_is_https=False,
            )


def test_handle_request_maps_origin_timeout_to_504():
    parsed = {
        "default_root_object": "",
        "origins": {"o1": {
            "domain_name": "127.0.0.1", "origin_path": "", "custom_headers": [],
            "s3_bucket": None, "http_port": 1, "https_port": 443,
            "protocol_policy": "http-only", "read_timeout": 30,
        }},
        "default_behavior": {
            "path_pattern": None, "target_origin_id": "o1", "viewer_protocol_policy": "allow-all",
            "allowed_methods": None, "cache_policy_id": _CACHING_DISABLED,
            "origin_request_policy_id": None, "response_headers_policy_id": None,
            "forwarded_values": None, "functions": {},
        },
        "ordered_behaviors": [],
    }
    dist = {"Id": "EDUMMY00000000", "DomainName": "ddummy0000000.cloudfront.net"}
    with mock.patch("ministack.services.cloudfront.parse_distribution_dataplane_config", return_value=parsed), \
            mock.patch("ministack.services.cloudfront_dataplane._forward_to_origin",
                       side_effect=cloudfront_dataplane.OriginTimeout("slow")):
        status, headers, _body = asyncio.run(
            cloudfront_dataplane.handle_request(dist, "GET", "/anything", "/anything", "", {}, b"", {})
        )
    assert status == 504
    assert headers.get("X-Cache") == "Error from cloudfront"


# ---------------------------------------------------------------------------
# S3 origin — served from MiniStack's own S3 store in-process.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def cf_s3_stack(cloudfront, s3):
    suffix = _uuid_mod.uuid4().hex[:10]
    bucket = f"cf-dp-s3-origin-{suffix}"
    s3.create_bucket(Bucket=bucket)
    body = b"hello from the bucket\n"
    s3.put_object(Bucket=bucket, Key="hello.txt", Body=body, ContentType="text/plain")

    dist_config = {
        "CallerReference": f"cf-dp-s3-{suffix}",
        "Comment": "cloudfront_dataplane S3-origin test distribution",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "s3-origin",
                "DomainName": f"{bucket}.s3.amazonaws.com",
                "S3OriginConfig": {"OriginAccessIdentity": ""},
                "OriginAccessControlId": "test-oac-id",
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "s3-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
    }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps({"Statement": [{
        "Effect": "Allow", "Principal": {"Service": "cloudfront.amazonaws.com"},
        "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{bucket}/*",
        "Condition": {"StringEquals": {"AWS:SourceArn": dist["ARN"]}},
    }]}))
    return {"dist_host": dist["DomainName"], "body": body}


def test_s3_origin_without_a_bucket_policy_grant_is_denied(cloudfront, s3):
    """An OAC origin whose bucket policy does not grant the distribution answers 403."""
    suffix = _uuid_mod.uuid4().hex[:10]
    bucket = f"cf-dp-s3-nogrant-{suffix}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="hello.txt", Body=b"x")
    dist = cloudfront.create_distribution(DistributionConfig={
        "CallerReference": f"cf-dp-s3-nogrant-{suffix}", "Comment": "", "Enabled": True,
        "Origins": {"Quantity": 1, "Items": [{
            "Id": "s3-origin", "DomainName": f"{bucket}.s3.amazonaws.com",
            "S3OriginConfig": {"OriginAccessIdentity": ""}, "OriginAccessControlId": "test-oac-id",
        }]},
        "DefaultCacheBehavior": {"TargetOriginId": "s3-origin", "ViewerProtocolPolicy": "allow-all",
                                 "CachePolicyId": _CACHING_DISABLED},
    })["Distribution"]
    status, _headers, body = _get(dist["DomainName"], "/hello.txt")
    assert status == 403
    assert b"<Code>AccessDenied</Code>" in body


def test_s3_origin_serves_bucket_object(cf_s3_stack):
    status, headers, body = _get(cf_s3_stack["dist_host"], "/hello.txt")
    assert status == 200
    assert body == cf_s3_stack["body"]
    assert headers.get("Content-Type") == "text/plain"


def test_s3_origin_range_get_returns_partial_content(cf_s3_stack):
    # Range is forwarded regardless of cache/origin-request-policy header
    # configuration (CloudFront Developer Guide, "HTTP request headers and
    # CloudFront behavior"), even under CachingDisabled with no ORP here.
    status, headers, body = _get(
        cf_s3_stack["dist_host"], "/hello.txt", extra_headers={"Range": "bytes=0-4"},
    )
    assert status == 206
    assert body == cf_s3_stack["body"][:5]
    assert headers.get("Content-Range", "").startswith("bytes 0-4/")


def test_s3_origin_missing_key_returns_access_denied(cf_s3_stack):
    # The OAC bucket policy CloudFront's console generates grants only
    # s3:GetObject, never s3:ListBucket — without ListBucket, S3 answers a
    # missing key with 403 AccessDenied rather than 404 NoSuchKey.
    status, _headers, body = _get(cf_s3_stack["dist_host"], "/does-not-exist.txt")
    assert status == 403
    assert b"<Code>AccessDenied</Code>" in body


# ---------------------------------------------------------------------------
# Percent-encoding fidelity (CloudFront Developer Guide, "Restrictions on all
# edge functions" > "URI, query string, and headers encoding").
# ---------------------------------------------------------------------------


class _PathEchoHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({
            "path": self.path,
            "headers": {k.lower(): v for k, v in self.headers.items()},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if self.path.endswith("/hopbyhop"):
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Should-Survive", "yes")
        if self.path.endswith("/respdup"):
            self.send_header("X-Resp-Dup", "first")
            self.send_header("X-Resp-Dup", "second")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        posted_body = self.rfile.read(length) if length else b""
        body = json.dumps({
            "path": self.path,
            "body": posted_body.decode("utf-8", errors="replace"),
            "content_length_header": self.headers.get("Content-Length"),
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="module")
def path_echo_origin_port():
    server = http.server.HTTPServer(("127.0.0.1", 0), _PathEchoHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.fixture(scope="module")
def cf_raw_origin_stack(cloudfront, path_echo_origin_port):
    suffix = _uuid_mod.uuid4().hex[:10]
    dist_config = {
        "CallerReference": f"cf-dp-raw-{suffix}",
        "Comment": "cloudfront_dataplane raw-path-fidelity test distribution",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "raw-origin",
                "DomainName": "127.0.0.1",
                "CustomOriginConfig": {
                    "HTTPPort": path_echo_origin_port, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "raw-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
        "CacheBehaviors": {
            "Quantity": 1,
            "Items": [{
                "PathPattern": "/av/*",
                "TargetOriginId": "raw-origin", "ViewerProtocolPolicy": "allow-all",
                "CachePolicyId": _CACHING_DISABLED, "OriginRequestPolicyId": _ALL_VIEWER,
            }],
        },
    }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    return dist["DomainName"]


def _raw_get(dist_host: str, raw_path: str):
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", raw_path, skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{dist_host}:{GATEWAY_PORT}")
        conn.endheaders()
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def test_percent_encoded_path_forwarded_byte_exact(cf_raw_origin_stack):
    for raw_path in (
        "/echo/%21%24%26%27%28%29%2A%2B%2C%3B%3D%40",
        "/ready_pin_guests/123%2Ejson",
    ):
        status, body = _raw_get(cf_raw_origin_stack, raw_path)
        assert status == 200
        assert json.loads(body)["path"] == raw_path


def test_query_string_forwarded_byte_exact_when_unmodified_and_fully_allowed(cf_raw_origin_stack):
    # "/av/*" carries AllViewer (forwards every query-string parameter) and
    # no function: nothing should re-encode the viewer's own percent-encoding.
    raw_path = "/av/hello?a=%68%65%6c%6c%6f&b=1"
    status, body = _raw_get(cf_raw_origin_stack, raw_path)
    assert status == 200
    assert json.loads(body)["path"] == raw_path


def test_post_body_round_trips_with_content_length(cf_raw_origin_stack):
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        payload = b'{"hello": "world"}'
        conn.putrequest("POST", "/av/echo", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_raw_origin_stack}:{GATEWAY_PORT}")
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Content-Length", str(len(payload)))
        conn.endheaders(payload)
        resp = conn.getresponse()
        body = resp.read()
    finally:
        conn.close()
    echoed = json.loads(body)
    assert echoed["body"] == payload.decode()
    assert echoed["content_length_header"] == str(len(payload))


def test_hop_by_hop_response_headers_stripped(cf_raw_origin_stack):
    status, headers, _body = _get(cf_raw_origin_stack, "/av/hopbyhop")
    assert status == 200
    assert "Connection" not in headers
    assert headers.get("X-Should-Survive") == "yes"


# ---------------------------------------------------------------------------
# Raw request-header fidelity toward the origin — a plain http.server or
# http.client doesn't distinguish "one folded line" from "two lines with the
# same name", so this reads the exact bytes off the socket.
# ---------------------------------------------------------------------------


class _RawHeaderCaptureServer:
    """Accepts one connection at a time, records the raw request-header
    block exactly as received, and answers a trivial 200."""

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(5)
        self.port = self.sock.getsockname()[1]
        self.last_header_lines: list = []
        self._stop = False
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        self.sock.settimeout(0.5)
        while not self._stop:
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with conn:
                conn.settimeout(5)
                data = b""
                while b"\r\n\r\n" not in data:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                head, _, _rest = data.partition(b"\r\n\r\n")
                lines = head.decode("latin-1").split("\r\n")
                self.last_header_lines = lines[1:]  # drop the request line
                body = b"ok"
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: "
                    + str(len(body)).encode() + b"\r\n\r\n" + body
                )

    def stop(self):
        self._stop = True
        self.sock.close()
        self.thread.join(timeout=5)


@pytest.fixture(scope="module")
def raw_header_server():
    server = _RawHeaderCaptureServer()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def cf_raw_header_stack(cloudfront, raw_header_server):
    suffix = _uuid_mod.uuid4().hex[:10]
    dup_edit_fn_arn = None
    if cloudfront_js.available():
        # Viewer headers are already comma-joined before this module ever
        # sees them (app.py, RFC 9110 §5.2), so a function is the only way to
        # produce a genuine multiValue request header here.
        dup_edit_fn_arn = _create_function(
            cloudfront, f"cf-dp-dup-header-edit-{suffix}",
            b"function handler(event) {\n"
            b"  var request = event.request;\n"
            b"  request.headers['x-dup'] = { value: 'a', multiValue: [{ value: 'a' }, { value: 'b' }] };\n"
            b"  return request;\n"
            b"}\n",
        )
    dist_config = {
        "CallerReference": f"cf-dp-rawheaders-{suffix}",
        "Comment": "cloudfront_dataplane raw request-header test distribution",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "raw-header-origin",
                "DomainName": "127.0.0.1",
                "CustomOriginConfig": {
                    "HTTPPort": raw_header_server.port, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
                # Lowercase, viewer sends Title-Case — proves the overwrite is
                # case-insensitive, not merely a literal-name match.
                "CustomHeaders": {"Quantity": 1, "Items": [{"HeaderName": "x-custom-test", "HeaderValue": "from-origin"}]},
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "raw-header-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED, "OriginRequestPolicyId": _ALL_VIEWER,
        },
    }
    if dup_edit_fn_arn:
        dist_config["CacheBehaviors"] = {
            "Quantity": 1,
            "Items": [{
                "PathPattern": "/dupheader/*",
                "TargetOriginId": "raw-header-origin", "ViewerProtocolPolicy": "allow-all",
                "CachePolicyId": _CACHING_DISABLED, "OriginRequestPolicyId": _ALL_VIEWER,
                "FunctionAssociations": {
                    "Quantity": 1,
                    "Items": [{"FunctionARN": dup_edit_fn_arn, "EventType": "viewer-request"}],
                },
            }],
        }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    return dist["DomainName"]


def test_origin_request_headers_no_duplicates_case_insensitive_overwrite_and_xff(
    cf_raw_header_stack, raw_header_server,
):
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", "/anything", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_raw_header_stack}:{GATEWAY_PORT}")
        conn.putheader("X-Custom-Test", "viewer-value")
        conn.putheader("X-Forwarded-For", "203.0.113.5")
        conn.endheaders()
        resp = conn.getresponse()
        resp.read()
    finally:
        conn.close()

    lines = raw_header_server.last_header_lines
    names_lower = [line.split(":", 1)[0].lower() for line in lines if line]
    # AllViewer forwards Host too: exactly one line, not a lowercase
    # viewer-forwarded one plus a Title-Cased one this module also adds.
    assert names_lower.count("host") == 1

    custom_test_lines = [line for line in lines if line.lower().startswith("x-custom-test:")]
    assert len(custom_test_lines) == 1
    assert custom_test_lines[0].split(":", 1)[1].strip() == "from-origin"

    xff_lines = [line for line in lines if line.lower().startswith("x-forwarded-for:")]
    assert len(xff_lines) == 1
    assert xff_lines[0].split(":", 1)[1].strip() == "203.0.113.5, 127.0.0.1"


def test_function_set_multivalue_header_forwarded_as_one_comma_joined_line(
    cf_raw_header_stack, raw_header_server,
):
    # RFC 9110 §5.3: a repeated field value is one comma-joined line when
    # forwarded to the origin, not several lines of the same name.
    _skip_without_node()
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", "/dupheader/hello", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_raw_header_stack}:{GATEWAY_PORT}")
        conn.endheaders()
        resp = conn.getresponse()
        resp.read()
    finally:
        conn.close()

    dup_lines = [line for line in raw_header_server.last_header_lines if line.lower().startswith("x-dup:")]
    assert len(dup_lines) == 1
    assert dup_lines[0].split(":", 1)[1].strip() == "a, b"


@pytest.fixture(scope="module")
def cf_response_dup_stack(cloudfront, path_echo_origin_port):
    """A distribution whose origin (the raw path-echo server) sends a
    genuinely repeated response header, for the response-side multiValue
    edit-only merge rule below."""
    suffix = _uuid_mod.uuid4().hex[:10]
    fn_arn = None
    if cloudfront_js.available():
        fn_arn = _create_function(
            cloudfront, f"cf-dp-response-dup-edit-{suffix}",
            b"function handler(event) {\n"
            b"  var response = event.response;\n"
            b"  if (response.headers['x-resp-dup']) {\n"
            b"    response.headers['x-resp-dup'].value = 'edited';\n"
            b"  }\n"
            b"  return response;\n"
            b"}\n",
        )
    behavior = {
        "TargetOriginId": "respdup-origin", "ViewerProtocolPolicy": "allow-all",
        "CachePolicyId": _CACHING_DISABLED,
    }
    if fn_arn:
        behavior["FunctionAssociations"] = {
            "Quantity": 1, "Items": [{"FunctionARN": fn_arn, "EventType": "viewer-response"}],
        }
    dist_config = {
        "CallerReference": f"cf-dp-respdup-{suffix}",
        "Comment": "cloudfront_dataplane response multiValue edit-only test",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "respdup-origin",
                "DomainName": "127.0.0.1",
                "CustomOriginConfig": {
                    "HTTPPort": path_echo_origin_port, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
            }],
        },
        "DefaultCacheBehavior": behavior,
    }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    return dist["DomainName"]


def test_viewer_response_function_edits_only_value_of_repeated_header_keeps_remaining_values(
    cf_response_dup_stack,
):
    # functions-event-structure.html: a header whose multiValue is unchanged
    # from the event is read only for its primary value; the rest of its
    # original values survive.
    _skip_without_node()
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", "/respdup", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_response_dup_stack}:{GATEWAY_PORT}")
        conn.endheaders()
        resp = conn.getresponse()
        resp.read()
        dup_values = resp.msg.get_all("X-Resp-Dup") or []
    finally:
        conn.close()
    assert dup_values == ["edited", "second"]


# ---------------------------------------------------------------------------
# TLS to a custom https origin.
# ---------------------------------------------------------------------------


@pytest.fixture
def untrusted_https_origin_port(tmp_path):
    pytest.importorskip("cryptography")
    from ministack.core.x509_utils import generate_ca, sign_leaf_certificate

    ca_pem, ca_key_pem = generate_ca(common_name="cf-dp-untrusted-ca")
    leaf_pem, leaf_key_pem, _public = sign_leaf_certificate(
        ca_pem, ca_key_pem, common_name="127.0.0.1", san_ips=["127.0.0.1"],
    )
    cert_path = tmp_path / "leaf.pem"
    cert_path.write_text(leaf_pem + leaf_key_pem)

    server = http.server.HTTPServer(("127.0.0.1", 0), _PathEchoHandler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(cert_path))
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_untrusted_origin_certificate_answers_502(cloudfront, untrusted_https_origin_port):
    # http-502-bad-gateway.html's self-signed/untrusted-chain case: the
    # origin's own CA isn't in the system trust store (and USE_SSL is off in
    # this test harness, so the gateway cert isn't trusted either).
    suffix = _uuid_mod.uuid4().hex[:10]
    dist_config = {
        "CallerReference": f"cf-dp-untrusted-{suffix}",
        "Comment": "cloudfront_dataplane untrusted-origin-cert test",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "untrusted-origin",
                "DomainName": "127.0.0.1",
                "CustomOriginConfig": {
                    "HTTPPort": 80, "HTTPSPort": untrusted_https_origin_port,
                    "OriginProtocolPolicy": "https-only",
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "untrusted-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
    }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    status, headers, _body = _get(dist["DomainName"], "/anything")
    assert status == 502
    assert headers.get("X-Cache") == "Error from cloudfront"
