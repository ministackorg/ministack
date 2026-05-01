"""Unit tests for AppSync Events ``dns`` defaults (no live MiniStack)."""

from __future__ import annotations

from ministack.core.responses import set_request_region


def test_default_dns_localhost_with_region(monkeypatch):
    monkeypatch.delenv("APPSYNC_EVENTS_HTTP_HOST_TEMPLATE", raising=False)
    monkeypatch.delenv("APPSYNC_EVENTS_REALTIME_HOST_TEMPLATE", raising=False)
    monkeypatch.setenv("MINISTACK_HOST", "localhost")
    monkeypatch.setenv("GATEWAY_PORT", "4566")
    set_request_region("us-east-1")

    from ministack.services.appsync_events import _default_dns_for_api

    d = _default_dns_for_api("abc123")
    assert d["HTTP"] == "abc123.appsync-api.us-east-1.localhost:4566"
    assert d["REALTIME"] == "abc123.appsync-realtime-api.us-east-1.localhost:4566"


def test_dns_templates_override_when_both_set(monkeypatch):
    monkeypatch.setenv(
        "APPSYNC_EVENTS_HTTP_HOST_TEMPLATE",
        "{api_id}.appsync-api.custom:{port}",
    )
    monkeypatch.setenv(
        "APPSYNC_EVENTS_REALTIME_HOST_TEMPLATE",
        "{api_id}.appsync-realtime-api.custom:{port}",
    )
    monkeypatch.setenv("GATEWAY_PORT", "8080")
    set_request_region("eu-west-2")

    from ministack.services.appsync_events import _default_dns_for_api

    d = _default_dns_for_api("x")
    assert d["HTTP"] == "x.appsync-api.custom:8080"
    assert d["REALTIME"] == "x.appsync-realtime-api.custom:8080"
