# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""CloudFront data plane: serves viewer requests to ``<label>.cloudfront.net`` / ``<label>.cloudfront.<MINISTACK_HOST>``.

Not modelled: caching, WAF, logging, geo restrictions, signed URLs and cookies,
Lambda@Edge, CORS preflight, synthetic ``CloudFront-Viewer-*`` headers, origins at AWS hostnames (502).
"""

import base64
import fnmatch
import http.client
import logging
import os
import random
import ssl
import string
from re import compile as _re_compile
from re import escape as _re_escape
from urllib.parse import quote, unquote, urlencode

from ministack.core import cloudfront_js
from ministack.core.concurrency import run_offloop, run_reentrant
from ministack.core.responses import new_uuid
from ministack.services import cloudfront, s3

logger = logging.getLogger("cloudfront_dataplane")

_HOP_BY_HOP_REQUEST_HEADERS = {"host", "cookie", "content-length", "connection", "transfer-encoding"}
# Hop-by-hop headers dropped at the socket boundary (_forward_to_origin).
_CONNECTION_MANAGEMENT_HEADERS = {"content-length", "connection", "transfer-encoding"}

# Always forwarded, whatever the policies say (Developer Guide, request headers table).
_ALWAYS_FORWARD_REQUEST_HEADERS = {
    "range", "if-match", "if-modified-since", "if-none-match", "if-range", "if-unmodified-since",
}

# Dropped from an origin's response before it reaches the viewer — connection
# management is this module's concern, not the origin's.
_HOP_BY_HOP_RESPONSE_HEADERS = {"connection", "keep-alive", "transfer-encoding", "upgrade", "trailer"}


class CloudFrontFunctionError(Exception):
    """A CloudFront Function threw, timed out, or its code didn't define a
    handler — CloudFront answers every one of these with a 503."""


# ---------------------------------------------------------------------------
# CloudFront Functions event <-> HTTP conversion (functions-event-structure.html)
# ---------------------------------------------------------------------------


def _title_case_header_name(name: str) -> str:
    """"example-header-name" -> "Example-Header-Name", as CloudFront Functions title-case header names."""
    return "-".join(part[:1].upper() + part[1:] if part[:1].isascii() else part for part in name.split("-"))


def _event_items(values: dict) -> dict:
    """A querystring/headers object: one field per name, carrying a
    ``multiValue`` array when the same name repeated (functions-event-
    structure.html, "Duplicate query strings, headers, and cookies")."""
    out = {}
    for name, value in values.items():
        items = value if isinstance(value, list) else [value]
        out[name] = {"value": items[0]}
        if len(items) > 1:
            out[name]["multiValue"] = [{"value": v} for v in items]
    return out


def _event_cookies_from_header(cookie_header: str) -> dict:
    cookies = {}
    for part in (cookie_header or "").split(";"):
        name, sep, value = part.strip().partition("=")
        if sep and name:
            cookies[name] = {"value": value.strip()}
    return cookies


def _event_cookies_from_set_cookie(set_cookie) -> dict:
    """The response.cookies event object built from raw Set-Cookie header values."""
    values = set_cookie if isinstance(set_cookie, list) else ([set_cookie] if set_cookie else [])
    cookies = {}
    for raw in values:
        name_value, _, attrs = raw.partition(";")
        name, _, value = name_value.partition("=")
        name = name.strip()
        if not name:
            continue
        entry = {"value": value.strip()}
        if attrs.strip():
            entry["attributes"] = attrs.strip()
        if name in cookies:
            existing = cookies[name]
            existing.setdefault("multiValue", [{k: v for k, v in existing.items() if k != "multiValue"}])
            existing["multiValue"].append(entry)
        else:
            cookies[name] = entry
    return cookies


def _set_cookie_values_from_event(cookies: dict) -> list:
    out = []
    for name, field in (cookies or {}).items():
        if not isinstance(field, dict):
            continue
        entries = field.get("multiValue") or [field]
        for entry in entries:
            line = f"{name}={entry.get('value', '')}"
            if entry.get("attributes"):
                line += f"; {entry['attributes']}"
            out.append(line)
    return out


def _event_querystring(query_params: dict) -> dict:
    return _event_items({name: values for name, values in (query_params or {}).items()})


def _merge_function_headers(original_headers_obj: dict, returned_headers_obj: dict) -> dict:
    """A name(lowercase)->value dict built from a function's returned headers object."""
    original = original_headers_obj or {}
    out = {}
    for name, field in (returned_headers_obj or {}).items():
        if not isinstance(field, dict):
            continue
        orig_field = original.get(name)
        orig_multi = orig_field.get("multiValue") if isinstance(orig_field, dict) else None
        new_multi = field.get("multiValue")
        if new_multi and new_multi != orig_multi:
            out[name.lower()] = [str(v.get("value", "")) for v in new_multi]
        elif orig_multi:
            out[name.lower()] = [str(field.get("value", ""))] + [str(v.get("value", "")) for v in orig_multi[1:]]
        elif "value" in field:
            out[name.lower()] = str(field["value"])
    return out


def _build_function_event(event_type: str, dist_id: str, dist_domain: str, request_id: str,
                           method: str, uri: str, query_params: dict, headers: dict, client_ip: str) -> dict:
    request_headers = {k: v for k, v in headers.items() if k != "cookie"}
    return {
        "version": "1.0",
        "context": {
            "distributionDomainName": dist_domain,
            "distributionId": dist_id,
            "eventType": event_type,
            "requestId": request_id,
        },
        # functions-event-structure.html: the TCP peer, not a header value.
        "viewer": {"ip": client_ip},
        "request": {
            "method": method,
            "uri": uri,
            "querystring": _event_querystring(query_params),
            "headers": _event_items(request_headers),
            "cookies": _event_cookies_from_header(headers.get("cookie", "")),
        },
    }


def _response_event_fields(status: int, reason: str, headers: dict) -> dict:
    response_headers = {k: v for k, v in headers.items() if k != "set-cookie"}
    return {
        "statusCode": status,
        "statusDescription": reason or "",
        "headers": _event_items(response_headers),
        "cookies": _event_cookies_from_set_cookie(headers.get("set-cookie")),
    }


def _body_from_response_event(response: dict, fallback: bytes) -> bytes:
    body = response.get("body")
    if body is None:
        return fallback
    if isinstance(body, str):
        return body.encode("utf-8")
    if isinstance(body, dict):
        data = body.get("data", "")
        if body.get("encoding") == "base64":
            return base64.b64decode(data)
        return str(data).encode("utf-8")
    return fallback


async def _run_function(function_arn: str, event: dict) -> dict:
    code = cloudfront.live_function_code(function_arn)
    if code is None:
        raise CloudFrontFunctionError(f"{function_arn} has no published (LIVE) code")
    try:
        # A function cannot call back into MiniStack, so this is run_offloop work.
        return await run_offloop(cloudfront_js.evaluate, code, event)
    except cloudfront_js.CloudFrontFunctionError as exc:
        raise CloudFrontFunctionError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Cache behavior matching — CloudFront path-pattern glob, first match wins.
# ---------------------------------------------------------------------------


def _glob_to_regex(pattern: str):
    # A leading / in a path pattern is optional (Developer Guide, "Path pattern").
    if not pattern.startswith("/"):
        pattern = "/" + pattern
    out = [".*" if ch == "*" else "." if ch == "?" else _re_escape(ch) for ch in pattern]
    return _re_compile("^" + "".join(out) + "$")


def _match_behavior(parsed: dict, path: str):
    for behavior in parsed["ordered_behaviors"]:
        if _glob_to_regex(behavior["path_pattern"]).match(path):
            return behavior
    return parsed["default_behavior"]


# ---------------------------------------------------------------------------
# Origin-request forwarding: the cache policy / origin request policy combination table (Developer Guide).
# ---------------------------------------------------------------------------


def _behavior_kind(behavior: str, names, lowercase: bool = False):
    """A cache or origin request policy behavior value as a (kind, set) pair."""
    names = [n.lower() for n in (names or [])] if lowercase else list(names or [])
    if behavior in ("all", "allViewer", "allViewerAndWhitelistCloudFront"):
        return ("all", None)
    if behavior == "whitelist":
        return ("whitelist", set(names))
    if behavior == "allExcept":
        return ("allExcept", set(names))
    return ("none", None)


def _combine_forward(cache_kind_set, orp_kind_set, name: str) -> bool:
    """Whether ``name`` reaches the origin, per the cache-policy / origin-request-policy combination table."""
    cache_kind, cache_set = cache_kind_set
    if cache_kind == "all":
        return True  # a cache policy's "all" always wins, even over an ORP block list
    orp_kind, orp_set = orp_kind_set if orp_kind_set is not None else ("none", None)
    if orp_kind == "all":
        return True
    if cache_kind == "none":
        if orp_kind == "whitelist":
            return name in orp_set
        if orp_kind == "allExcept":
            return name not in orp_set
        return False
    if cache_kind == "whitelist":
        if name in cache_set:
            return True
        if orp_kind == "whitelist":
            return name in orp_set
        if orp_kind == "allExcept":
            return name not in orp_set
        return False
    if cache_kind == "allExcept":
        if name not in cache_set:
            if orp_kind == "allExcept":
                return name not in orp_set
            return True  # cache policy already allows it through unless ORP also blocks it
        return orp_kind == "whitelist" and name in orp_set  # ORP allow-list rescues a cache-blocked name
    return False


def _legacy_predicates(fv: dict):
    """Forwarding predicates for a legacy ``ForwardedValues`` behavior (no CachePolicyId)."""
    header_names = {n.lower() for n in (fv.get("headers") or [])}
    forward_all_headers = "*" in (fv.get("headers") or [])
    cookies_forward = fv.get("cookies_forward", "none")
    cookies_whitelist = fv.get("cookies_whitelist") or []
    forward_qs = bool(fv.get("query_string"))

    def header_allowed(name):
        return forward_all_headers or name.lower() in header_names

    def cookie_allowed(name):
        if cookies_forward == "all":
            return True
        if cookies_forward == "whitelist":
            return any(fnmatch.fnmatchcase(name, pattern) for pattern in cookies_whitelist)
        return False

    def qs_allowed(_name):
        return forward_qs

    return header_allowed, cookie_allowed, qs_allowed


def _policy_predicates(behavior: dict):
    cache_params = cloudfront.cache_policy_params(behavior.get("cache_policy_id")) or {}
    orp_cfg = (
        cloudfront.origin_request_policy_config(behavior["origin_request_policy_id"])
        if behavior.get("origin_request_policy_id") else None
    )

    cache_headers = _behavior_kind(cache_params.get("HeaderBehavior", "none"), cache_params.get("Headers"), lowercase=True)
    cache_cookies = _behavior_kind(cache_params.get("CookieBehavior", "none"), cache_params.get("Cookies"))
    cache_qs = _behavior_kind(cache_params.get("QueryStringBehavior", "none"), cache_params.get("QueryStrings"))

    if orp_cfg is not None:
        orp_headers = _behavior_kind(orp_cfg["HeaderBehavior"], orp_cfg["Headers"], lowercase=True)
        orp_cookies = _behavior_kind(orp_cfg["CookieBehavior"], orp_cfg["Cookies"])
        orp_qs = _behavior_kind(orp_cfg["QueryStringBehavior"], orp_cfg["QueryStrings"])
    else:
        orp_headers = orp_cookies = orp_qs = None

    def header_allowed(name):
        return _combine_forward(cache_headers, orp_headers, name.lower())

    def cookie_allowed(name):
        return _combine_forward(cache_cookies, orp_cookies, name)

    def qs_allowed(name):
        return _combine_forward(cache_qs, orp_qs, name)

    return header_allowed, cookie_allowed, qs_allowed


def _forwarding_predicates(behavior: dict):
    if behavior.get("cache_policy_id"):
        return _policy_predicates(behavior)
    if behavior.get("forwarded_values") is not None:
        return _legacy_predicates(behavior["forwarded_values"])
    # Neither a cache policy nor legacy ForwardedValues: nothing beyond the
    # defaults every origin request always carries (Host, User-Agent).
    return (lambda _n: False), (lambda _n: False), (lambda _n: False)


# ---------------------------------------------------------------------------
# Response-headers policy
# ---------------------------------------------------------------------------


def _cors_origin_allowed(allow_origins: list, viewer_origin: str) -> bool:
    """Whether ``viewer_origin`` matches an AccessControlAllowOrigins entry ("*" or a glob)."""
    return any(p == "*" or fnmatch.fnmatchcase(viewer_origin, p) for p in allow_origins or [])


def _apply_cors_headers(headers: dict, cors_cfg: dict, viewer_origin: str, method: str) -> dict:
    """Apply a response headers policy's CORS headers (Developer Guide, "CORS headers")."""
    if not cors_cfg or not viewer_origin or not _cors_origin_allowed(cors_cfg.get("AllowOrigins"), viewer_origin):
        return headers
    result = dict(headers)
    override = cors_cfg.get("OriginOverride", False)

    def _set(name, value):
        key = name.lower()
        if key in result and not override:
            return
        result[key] = value

    _set("access-control-allow-origin", "*" if "*" in (cors_cfg.get("AllowOrigins") or []) else viewer_origin)
    if cors_cfg.get("AllowCredentials"):
        _set("access-control-allow-credentials", "true")
    if cors_cfg.get("ExposeHeaders"):
        _set("access-control-expose-headers", ", ".join(cors_cfg["ExposeHeaders"]))
    if method == "OPTIONS":
        if cors_cfg.get("AllowHeaders"):
            _set("access-control-allow-headers", ", ".join(cors_cfg["AllowHeaders"]))
        if cors_cfg.get("AllowMethods"):
            _set("access-control-allow-methods", ", ".join(cors_cfg["AllowMethods"]))
        if cors_cfg.get("MaxAgeSec") is not None:
            _set("access-control-max-age", str(cors_cfg["MaxAgeSec"]))
    return result


def _apply_response_headers_policy(headers: dict, policy_cfg: dict, viewer_origin: str, method: str) -> dict:
    if not policy_cfg:
        return headers
    result = _apply_cors_headers(headers, policy_cfg.get("Cors"), viewer_origin, method)

    def _set(name, value, override):
        key = name.lower()
        if key in result and not override:
            return
        result[key] = value

    # CustomHeaders/RemoveHeaders are None when the policy never supplied that block.
    for item in policy_cfg.get("CustomHeaders") or []:
        _set(item["Header"], item["Value"], item["Override"])

    sec = policy_cfg.get("Security") or {}
    if "ContentTypeOptions" in sec:
        _set("x-content-type-options", "nosniff", sec["ContentTypeOptions"]["Override"])
    if "FrameOptions" in sec:
        _set("x-frame-options", sec["FrameOptions"]["FrameOption"], sec["FrameOptions"]["Override"])
    if "XSSProtection" in sec:
        xp = sec["XSSProtection"]
        value = "1" if xp["Protection"] else "0"
        if xp["Protection"] and xp.get("ModeBlock"):
            value += "; mode=block"
        if xp.get("ReportUri"):
            value += f"; report={xp['ReportUri']}"
        _set("x-xss-protection", value, xp["Override"])
    if "ReferrerPolicy" in sec:
        _set("referrer-policy", sec["ReferrerPolicy"]["ReferrerPolicy"], sec["ReferrerPolicy"]["Override"])
    if "ContentSecurityPolicy" in sec:
        _set("content-security-policy", sec["ContentSecurityPolicy"]["ContentSecurityPolicy"],
             sec["ContentSecurityPolicy"]["Override"])
    if "StrictTransportSecurity" in sec:
        hsts = sec["StrictTransportSecurity"]
        value = f"max-age={hsts['AccessControlMaxAgeSec']}"
        if hsts.get("IncludeSubdomains"):
            value += "; includeSubDomains"
        if hsts.get("Preload"):
            value += "; preload"
        _set("strict-transport-security", value, hsts["Override"])

    for item in policy_cfg.get("RemoveHeaders") or []:
        result.pop(item["Header"].lower(), None)

    return result


# ---------------------------------------------------------------------------
# CloudFront's own added headers and synthesized error pages
# ---------------------------------------------------------------------------


def _cf_request_id() -> str:
    return base64.b64encode(os.urandom(42)).decode().rstrip("=")


def _cf_via_hash() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=13))


def _add_cloudfront_headers(headers: dict, x_cache: str, request_id: str | None = None) -> dict:
    result = dict(headers)
    result["via"] = f"1.1 {_cf_via_hash()}.cloudfront.net (CloudFront)"
    result["x-cache"] = x_cache
    result["x-amz-cf-pop"] = "LOC50-C1"
    result["x-amz-cf-id"] = request_id or _cf_request_id()
    return result


def _render_headers(headers: dict, title_case: bool) -> dict:
    if not title_case:
        return dict(headers)
    return {_title_case_header_name(name): value for name, value in headers.items()}


# The fixed body of CloudFront's own error pages ("The request could not be satisfied").
_CF_ERROR_TEMPLATE = """<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<HTML><HEAD><META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=iso-8859-1">
<TITLE>ERROR: The request could not be satisfied</TITLE>
</HEAD><BODY>
<H1>{status} ERROR</H1>
<H2>The request could not be satisfied.</H2>
<HR noshade size="1px">
{explanation}
<BR clear="all">
<HR noshade size="1px">
<PRE>
Generated by cloudfront (CloudFront)
Request ID: {request_id}
</PRE>
</BODY></HTML>"""


def _cf_error_response(status: int, explanation: str, x_cache: str) -> tuple:
    request_id = _cf_request_id()
    body = _CF_ERROR_TEMPLATE.format(status=status, explanation=explanation,
                                      request_id=request_id).encode("utf-8")
    headers = _add_cloudfront_headers({"content-type": "text/html"}, x_cache, request_id=request_id)
    return status, _render_headers(headers, title_case=True), body


# A failed origin TLS handshake is a 502 (Developer Guide, http-502-bad-gateway).
_ERROR_X_CACHE = "Error from cloudfront"


# ---------------------------------------------------------------------------
# Origin forwarding
# ---------------------------------------------------------------------------


class OriginUnreachable(Exception):
    pass


class OriginTimeout(Exception):
    """The origin didn't respond within OriginReadTimeout."""


# An origin addressed by an AWS-owned hostname is refused with 502, so no traffic reaches real AWS.
_AWS_OWNED_HOST_SUFFIXES = (".amazonaws.com", ".amazonaws.com.cn", ".on.aws", ".api.aws", ".cloudfront.net")


def _normalize_origin_host(host: str) -> str:
    h = (host or "").strip().lower()
    return h[:-1] if h.endswith(".") else h


def _is_aws_owned_host(host: str) -> bool:
    return any(host == suffix.removeprefix(".") or host.endswith(suffix) for suffix in _AWS_OWNED_HOST_SUFFIXES)


def _origin_connect_target(origin: dict, viewer_is_https: bool) -> tuple:
    """(connect_host, connect_port, use_https) — the origin's own
    DomainName and HTTPPort/HTTPSPort, dialed exactly as configured."""
    policy = origin["protocol_policy"]
    use_https = policy == "https-only" or (policy == "match-viewer" and viewer_is_https)
    port = origin["https_port"] if use_https else origin["http_port"]
    return origin["domain_name"], port, use_https


def _origin_ssl_context() -> ssl.SSLContext:
    """System roots, plus the gateway's own TLS cert under USE_SSL
    (lambda_svc.py's container CA-trust precedent) — a custom https origin
    may itself be a MiniStack-served endpoint sharing that cert."""
    from ministack.core import tls

    ctx = ssl.create_default_context()
    if tls.use_ssl_enabled():
        ctx.load_verify_locations(tls.resolve_tls_material()[0])
        ctx.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
    return ctx


def _set_header_ci(headers: dict, name: str, value) -> None:
    """Set ``name: value``, replacing any case-insensitive match."""
    for existing in [k for k in headers if k.lower() == name.lower()]:
        del headers[existing]
    headers[name] = value


def _origin_x_forwarded_for(viewer_xff: str, client_ip: str) -> str:
    """CloudFront Developer Guide, "Client IP addresses": appends the TCP
    peer's address to an existing viewer-sent X-Forwarded-For, or adds a
    fresh one carrying just that address."""
    return f"{viewer_xff}, {client_ip}" if viewer_xff else client_ip


def _forward_to_origin(origin: dict, method: str, uri: str, query_string: str,
                        headers: dict, body: bytes, viewer_is_https: bool, client_ip: str = "127.0.0.1",
                        viewer_x_forwarded_for: str = "") -> tuple:
    """Blocking: run under core.concurrency.run_reentrant (the origin may be
    MiniStack itself). Returns (status, reason, headers, body)."""
    conn_host, conn_port, use_https = _origin_connect_target(origin, viewer_is_https)
    if _is_aws_owned_host(_normalize_origin_host(conn_host)):
        raise OriginUnreachable(f"refusing to dial AWS-owned origin host {conn_host!r}")
    target_path = uri + (f"?{query_string}" if query_string else "")

    fwd_headers = {k: v for k, v in headers.items() if k not in _CONNECTION_MANAGEMENT_HEADERS}
    for name, value in origin.get("custom_headers") or []:
        _set_header_ci(fwd_headers, name, value)
    # Host is the origin's DomainName unless the policies forward the viewer's Host.
    _set_header_ci(fwd_headers, "Host", headers.get("host", origin["domain_name"]))
    _set_header_ci(fwd_headers, "Via", f"1.1 {_cf_via_hash()}.cloudfront.net (CloudFront)")
    _set_header_ci(fwd_headers, "X-Amz-Cf-Id", _cf_request_id())
    # Always appended (Developer Guide, "Client IP addresses").
    _set_header_ci(fwd_headers, "X-Forwarded-For",
                    _origin_x_forwarded_for(viewer_x_forwarded_for, client_ip))
    # RFC 9110 §5.3: a repeated field value is one comma-joined line.
    send_headers = {
        name: ", ".join(str(v) for v in value) if isinstance(value, list) else value
        for name, value in fwd_headers.items()
    }

    conn_cls = http.client.HTTPSConnection if use_https else http.client.HTTPConnection
    timeout = origin.get("read_timeout", 30)
    conn = (
        conn_cls(conn_host, conn_port, timeout=timeout, context=_origin_ssl_context())
        if use_https else conn_cls(conn_host, conn_port, timeout=timeout)
    )
    try:
        conn.request(method, target_path, body=body if body else None, headers=send_headers)
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, resp.reason, resp.getheaders(), data
    except TimeoutError as e:
        raise OriginTimeout(str(e)) from e
    except Exception as e:
        # An unreachable origin, or an untrusted or self-signed origin certificate.
        raise OriginUnreachable(str(e)) from e
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# S3 origin — served from MiniStack's own S3 store in-process, never a socket.
# ---------------------------------------------------------------------------


def _s3_object_key(origin: dict, uri: str) -> str:
    """OriginPath prefixed to the request URI, as CloudFront builds the origin path."""
    return unquote((origin.get("origin_path") or "") + uri).lstrip("/")


def _serve_s3_origin(origin: dict, method: str, uri: str, headers: dict, dist_arn: str = "") -> tuple:
    """Fetch from an S3 origin in-process (blocking; run off the event loop)."""
    return s3.serve_cloudfront_origin_fetch(
        origin["s3_bucket"], _s3_object_key(origin, uri), method, headers,
        source_arn=dist_arn if origin.get("oac_id") else "", oai_id=origin.get("oai_id", ""))


# ---------------------------------------------------------------------------
# Multi-value header handling — a name -> value map where a repeated name
# (duplicate response headers, multiple Set-Cookie) becomes a name -> list.
# ---------------------------------------------------------------------------


async def _fetch_from_origin(origin: dict, method: str, uri: str, query_string: str, headers: dict, body: bytes,
                             viewer_is_https: bool, client_ip: str, viewer_xff: str, dist_arn: str) -> tuple:
    """(status, reason, headers, body) from an S3 or custom origin; raises OriginTimeout / OriginUnreachable."""
    if origin.get("s3_bucket"):
        status, header_pairs, data = await run_offloop(_serve_s3_origin, origin, method, uri, headers, dist_arn)
        pairs = header_pairs.items() if isinstance(header_pairs, dict) else header_pairs
        return status, http.client.responses.get(status, ""), _multidict_from_pairs(pairs), data
    # The origin may be MiniStack's own gateway, so the call must be reentrant.
    status, reason, header_pairs, data = await run_reentrant(
        _forward_to_origin, origin, method, (origin.get("origin_path") or "") + uri, query_string, headers, body,
        viewer_is_https, client_ip, viewer_xff,
    )
    return status, reason, _multidict_from_pairs(header_pairs), data


def _multidict_from_pairs(pairs) -> dict:
    out: dict = {}
    for name, value in pairs:
        key = name.lower()
        if key in out:
            existing = out[key]
            if isinstance(existing, list):
                existing.append(value)
            else:
                out[key] = [existing, value]
        else:
            out[key] = value
    return out


def _strip_hop_by_hop_response_headers(headers: dict) -> dict:
    """Connection management is this module's concern, not the origin's."""
    return {k: v for k, v in headers.items() if k not in _HOP_BY_HOP_RESPONSE_HEADERS
            and not k.startswith("proxy-")}


# ---------------------------------------------------------------------------
# Behavior-level settings: ViewerProtocolPolicy, AllowedMethods, DefaultRootObject
# ---------------------------------------------------------------------------


def _check_viewer_protocol_policy(behavior: dict, method: str, viewer_is_https: bool, request_uri: str,
                                   query_string: str, headers: dict):
    """A redirect/forbid response, or None to continue."""
    policy = behavior.get("viewer_protocol_policy", "allow-all")
    if viewer_is_https or policy == "allow-all":
        return None
    if policy == "redirect-to-https":
        host = headers.get("host", "")
        location = f"https://{host}{request_uri}" + (f"?{query_string}" if query_string else "")
        redirect_status = 301 if method in ("GET", "HEAD") else 307
        status, resp_headers, body = _cf_error_response(redirect_status, "The document has moved.", _ERROR_X_CACHE)
        resp_headers["Location"] = location
        return status, resp_headers, body
    if policy == "https-only":
        return _cf_error_response(403, "Viewers must use HTTPS to request this object.", _ERROR_X_CACHE)
    return None


def _check_allowed_methods(behavior: dict, method: str):
    allowed = behavior.get("allowed_methods")
    if allowed is not None and method not in allowed:
        # Community-observed CloudFront 403 page text for this exact case
        # (see _CF_ERROR_TEMPLATE's citation note).
        return _cf_error_response(
            403,
            "This distribution is not configured to allow the HTTP request method that was used for this "
            "request. The distribution supports only cachable requests.",
            _ERROR_X_CACHE,
        )
    return None


def _apply_default_root_object(default_root_object: str, request_uri: str) -> str:
    """CloudFront Developer Guide, "Default root object": the configured
    object is requested from the origin when a viewer requests the
    distribution's root URL."""
    if default_root_object and request_uri == "/":
        return "/" + default_root_object
    return request_uri


def _check_stacked_distribution(headers: dict):
    """403 for a request that already passed through this hop (Developer Guide, stacked distributions)."""
    if "(cloudfront)" in headers.get("via", "").lower():
        return _cf_error_response(
            403, "This distribution is configured to serve another CloudFront distribution as its origin.",
            _ERROR_X_CACHE,
        )
    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def handle_request(dist: dict, method: str, path: str, raw_uri: str, raw_query_string: str,
                          headers: dict, body: bytes, query_params: dict, client_ip: str = "127.0.0.1") -> tuple:
    """Serve one viewer request against ``dist`` (a cloudfront.py distribution record)."""
    stacked_response = _check_stacked_distribution(headers)
    if stacked_response is not None:
        return stacked_response

    dist_id = dist["Id"]
    dist_domain = dist["DomainName"]
    request_id = new_uuid()
    viewer_is_https = headers.get("x-forwarded-proto", "http") == "https"

    parsed = cloudfront.parse_distribution_dataplane_config(dist)
    behavior = _match_behavior(parsed, path)
    if behavior is None:
        return _cf_error_response(502, "The request could not be satisfied.", _ERROR_X_CACHE)

    protocol_response = _check_viewer_protocol_policy(
        behavior, method, viewer_is_https, raw_uri, raw_query_string, headers,
    )
    if protocol_response is not None:
        return protocol_response
    methods_response = _check_allowed_methods(behavior, method)
    if methods_response is not None:
        return methods_response

    request_headers = dict(headers)
    request_uri = _apply_default_root_object(parsed.get("default_root_object", ""), raw_uri)
    request_query = dict(query_params or {})
    literal_query_string = None  # set when a function rearranges querystring into a literal string
    function_touched_querystring = False  # set when a function's returned request names a querystring object

    request_function_arn = behavior["functions"].get("viewer-request")
    if request_function_arn:
        event = _build_function_event(
            "viewer-request", dist_id, dist_domain, request_id,
            method, request_uri, request_query, request_headers, client_ip,
        )
        try:
            result = await _run_function(request_function_arn, event)
        except CloudFrontFunctionError:
            logger.exception("viewer-request function error (distribution=%s)", dist_id)
            return _cf_error_response(503, "The Lambda function failed to execute.", _ERROR_X_CACHE)

        if isinstance(result, dict) and "statusCode" in result:
            # A function-generated response goes straight to the viewer, skipping the origin.
            resp_headers = _merge_function_headers(None, result.get("headers"))
            resp_headers = _add_cloudfront_headers(resp_headers, "FunctionGeneratedResponse from cloudfront")
            set_cookie_values = _set_cookie_values_from_event(result.get("cookies"))
            if set_cookie_values:
                resp_headers["set-cookie"] = (
                    set_cookie_values if len(set_cookie_values) > 1 else set_cookie_values[0]
                )
            # statusDescription is dropped: ASGI carries no reason phrase.
            resp_body = _body_from_response_event(result, b"")
            return result["statusCode"], _render_headers(resp_headers, title_case=True), resp_body

        # Request headers stay lowercase internally; responses are Title-Cased once, on return.
        req = result if isinstance(result, dict) else event["request"]
        request_uri = req.get("uri", request_uri)
        request_headers = _merge_function_headers(event["request"]["headers"], req.get("headers"))
        cookie_pairs = [f"{n}={(f.get('value') if isinstance(f, dict) else f)}"
                        for n, f in (req.get("cookies") or {}).items()]
        if cookie_pairs:
            request_headers["cookie"] = "; ".join(cookie_pairs)
        qs_field = req.get("querystring")
        if isinstance(qs_field, str):
            # A function rewrote the querystring as a literal string (functions-event-structure).
            literal_query_string = qs_field
            request_query = {}
        elif qs_field is not None:
            function_touched_querystring = True
            request_query = {}
            for name, field in qs_field.items():
                if not isinstance(field, dict):
                    continue
                values = [field.get("value", "")]
                values += [v.get("value", "") for v in field.get("multiValue", [])[1:]]
                request_query[name] = values

    origin = parsed["origins"].get(behavior["target_origin_id"])
    if origin is None:
        return _cf_error_response(502, "The request could not be satisfied.", _ERROR_X_CACHE)

    header_allowed, cookie_allowed, qs_allowed = _forwarding_predicates(behavior)
    request_cookies = _event_cookies_from_header(request_headers.get("cookie", ""))
    forward_host = header_allowed("host")

    fwd_headers = {
        name: value for name, value in request_headers.items()
        if name not in _HOP_BY_HOP_REQUEST_HEADERS
        and (header_allowed(name) or name in _ALWAYS_FORWARD_REQUEST_HEADERS)
    }
    if header_allowed("user-agent") and "user-agent" in request_headers:
        fwd_headers["user-agent"] = request_headers["user-agent"]
    else:
        # User-Agent is "Amazon CloudFront" unless forwarded (Developer Guide, "User-Agent header").
        fwd_headers["user-agent"] = "Amazon CloudFront"
    if forward_host:
        fwd_headers["host"] = request_headers.get("host", "")
    cookie_pairs = [f"{name}={field['value']}" for name, field in request_cookies.items() if cookie_allowed(name)]
    if cookie_pairs:
        fwd_headers["cookie"] = "; ".join(cookie_pairs)
    if literal_query_string is not None:
        forward_qs = literal_query_string
    elif not function_touched_querystring and all(qs_allowed(name) for name in request_query):
        # Byte-exact: nothing rewrote the querystring and the policy forwards
        # every parameter, so there is no reason to re-encode it.
        forward_qs = raw_query_string
    else:
        forward_qs = urlencode(
            [(name, v) for name, values in request_query.items() if qs_allowed(name) for v in values],
            quote_via=quote,
        )

    try:
        status, reason, origin_headers, origin_body = await _fetch_from_origin(
            origin, method, request_uri, forward_qs, fwd_headers, body, viewer_is_https,
            client_ip, request_headers.get("x-forwarded-for", ""), dist.get("ARN", ""),
        )
        origin_status = status
        error_response = parsed["custom_error_responses"].get(status)
        if error_response:
            # The page is requested through the cache behavior its path matches (Developer Guide,
            # "Store objects and custom error pages in different locations").
            origin = parsed["origins"].get(_match_behavior(parsed, error_response["page"])["target_origin_id"])
            if origin is None:
                return _cf_error_response(502, "The request could not be satisfied.", _ERROR_X_CACHE)
            status, reason, origin_headers, origin_body = await _fetch_from_origin(
                origin, "GET", error_response["page"], "", {"user-agent": "Amazon CloudFront"}, b"",
                viewer_is_https, client_ip, "", dist.get("ARN", ""),
            )
            # An unavailable page is answered with the status its origin returned (Developer Guide,
            # "Generate custom error responses").
            if status < 400:
                status = error_response["response_code"]
    except OriginTimeout:
        logger.warning("Origin timed out for distribution=%s origin=%s", dist_id, origin["domain_name"])
        return _cf_error_response(504, "The request could not be satisfied.", _ERROR_X_CACHE)
    except OriginUnreachable:
        logger.warning("Origin unreachable for distribution=%s origin=%s", dist_id, origin["domain_name"])
        return _cf_error_response(502, "The request could not be satisfied.", _ERROR_X_CACHE)

    origin_headers = _strip_hop_by_hop_response_headers(origin_headers)
    policy_cfg = cloudfront.response_headers_policy_config(behavior.get("response_headers_policy_id"))
    response_headers = _apply_response_headers_policy(
        origin_headers, policy_cfg, request_headers.get("origin", ""), method,
    )
    response_body = origin_body

    response_function_arn = behavior["functions"].get("viewer-response")
    # No viewer-response function runs when the origin answers 400 or above.
    if response_function_arn and origin_status < 400:
        event = _build_function_event(
            "viewer-response", dist_id, dist_domain, request_id,
            method, request_uri, request_query, headers, client_ip,
        )
        event["response"] = _response_event_fields(status, reason, response_headers)
        try:
            result = await _run_function(response_function_arn, event)
        except CloudFrontFunctionError:
            logger.exception("viewer-response function error (distribution=%s)", dist_id)
            return _cf_error_response(503, "The Lambda function failed to execute.", _ERROR_X_CACHE)
        response = result if isinstance(result, dict) else event["response"]
        status = response.get("statusCode", status)
        # Replace, not merge: a header the function deleted stays gone.
        response_headers = _merge_function_headers(event["response"]["headers"], response.get("headers"))
        set_cookie_values = _set_cookie_values_from_event(response.get("cookies"))
        if set_cookie_values:
            response_headers["set-cookie"] = set_cookie_values if len(set_cookie_values) > 1 else set_cookie_values[0]
        response_body = _body_from_response_event(response, origin_body)

    final_headers = _add_cloudfront_headers(response_headers, "Miss from cloudfront")
    return status, _render_headers(final_headers, title_case=True), response_body
