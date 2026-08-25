"""ALB authenticate-oidc listener action.

Covers the round-trip of AuthenticateOidcConfig through the control plane, the
cookie sharding rules a browser imposes, and the data-plane behaviour a client
observes: redirect to the identity provider, a session written on callback,
identity headers injected for the target, and the OnUnauthenticatedRequest
choices.
"""

import json
import os
import time
import urllib.request as _req
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

import pytest

# The ALB fixtures live alongside the rest of the ALB suite.
from tests.test_alb import _alb_setup, _alb_teardown  # noqa: F401

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
_EXECUTE_PORT = urlparse(_endpoint).port or 4566


def _oidc_action(order=1, **overrides):
    cfg = {
        "Issuer": "https://idp.example.com",
        "AuthorizationEndpoint": "https://idp.example.com/authorize",
        "TokenEndpoint": "https://idp.example.com/token",
        "UserInfoEndpoint": "https://idp.example.com/userinfo",
        "ClientId": "client-abc",
        "ClientSecret": "shhh",
        "Scope": "openid email",
        "SessionTimeout": 3600,
    }
    cfg.update(overrides)
    return {"Type": "authenticate-oidc", "Order": order, "AuthenticateOidcConfig": cfg}


# --------------------------------------------------------------- control plane

def test_authenticate_oidc_config_round_trips(elbv2):
    """The rule survives Create -> Describe with its configuration intact."""
    lb = elbv2.create_load_balancer(Name="oidc-rt")["LoadBalancers"][0]
    tg = elbv2.create_target_group(Name="oidc-rt-tg", TargetType="lambda")["TargetGroups"][0]
    try:
        listener = elbv2.create_listener(
            LoadBalancerArn=lb["LoadBalancerArn"], Protocol="HTTP", Port=80,
            DefaultActions=[
                _oidc_action(order=1),
                {"Type": "forward", "Order": 2, "TargetGroupArn": tg["TargetGroupArn"]},
            ],
        )["Listeners"][0]

        described = elbv2.describe_listeners(ListenerArns=[listener["ListenerArn"]])["Listeners"][0]
        actions = sorted(described["DefaultActions"], key=lambda a: a.get("Order", 1))
        assert [a["Type"] for a in actions] == ["authenticate-oidc", "forward"]

        cfg = actions[0]["AuthenticateOidcConfig"]
        assert cfg["Issuer"] == "https://idp.example.com"
        assert cfg["AuthorizationEndpoint"] == "https://idp.example.com/authorize"
        assert cfg["TokenEndpoint"] == "https://idp.example.com/token"
        assert cfg["ClientId"] == "client-abc"
        assert cfg["Scope"] == "openid email"
        assert cfg["SessionTimeout"] == 3600
        # Defaults AWS fills in when the caller omits them.
        assert cfg["SessionCookieName"] == "AWSELBAuthSessionCookie"
        assert cfg["OnUnauthenticatedRequest"] == "authenticate"
        # AWS never echoes the secret back, and neither do we.
        assert "ClientSecret" not in cfg
    finally:
        elbv2.delete_load_balancer(LoadBalancerArn=lb["LoadBalancerArn"])
        elbv2.delete_target_group(TargetGroupArn=tg["TargetGroupArn"])


def test_authenticate_oidc_config_round_trips_on_a_rule(elbv2):
    """A path-based rule carries the action just as a listener default does."""
    lb = elbv2.create_load_balancer(Name="oidc-rule")["LoadBalancers"][0]
    tg = elbv2.create_target_group(Name="oidc-rule-tg", TargetType="lambda")["TargetGroups"][0]
    try:
        listener = elbv2.create_listener(
            LoadBalancerArn=lb["LoadBalancerArn"], Protocol="HTTP", Port=80,
            DefaultActions=[{"Type": "forward", "TargetGroupArn": tg["TargetGroupArn"]}],
        )["Listeners"][0]
        rule = elbv2.create_rule(
            ListenerArn=listener["ListenerArn"], Priority=10,
            Conditions=[{"Field": "path-pattern", "Values": ["/private/*"]}],
            Actions=[
                _oidc_action(order=1, SessionCookieName="MySession"),
                {"Type": "forward", "Order": 2, "TargetGroupArn": tg["TargetGroupArn"]},
            ],
        )["Rules"][0]

        described = elbv2.describe_rules(RuleArns=[rule["RuleArn"]])["Rules"][0]
        actions = sorted(described["Actions"], key=lambda a: a.get("Order", 1))
        assert actions[0]["AuthenticateOidcConfig"]["SessionCookieName"] == "MySession"
    finally:
        elbv2.delete_load_balancer(LoadBalancerArn=lb["LoadBalancerArn"])
        elbv2.delete_target_group(TargetGroupArn=tg["TargetGroupArn"])


# ------------------------------------------------------------------- sharding

def test_session_cookie_fits_the_browser_ceiling():
    """Every shard must fit in 4096 bytes including its name and attributes.

    A shard sized to 4096 bytes of value alone yields a Set-Cookie the browser
    silently discards, which is indistinguishable from having no session.
    """
    from ministack.services import alb

    session = {"id_token": "x" * 12000, "access_token": "y" * 4000, "exp": 99999999999}
    cookies = alb._oidc_session_cookies("AWSELBAuthSessionCookie", session, 3600, secure=True)

    assert len(cookies) > 1, "a session this size must shard"
    for cookie in cookies:
        assert len(cookie) <= alb.OIDC_COOKIE_MAX_BYTES, (
            f"shard is {len(cookie)} bytes, over the {alb.OIDC_COOKIE_MAX_BYTES} ceiling"
        )
        assert "; Path=/" in cookie and "; HttpOnly" in cookie
        assert "; Max-Age=3600" in cookie
        assert "; Secure" in cookie


def test_session_cookie_shards_are_named_in_sequence():
    from ministack.services import alb

    cookies = alb._oidc_session_cookies("AWSELBAuthSessionCookie",
                                        {"id_token": "x" * 12000}, 60, secure=False)
    for index, cookie in enumerate(cookies):
        assert cookie.startswith(f"AWSELBAuthSessionCookie-{index}=")
    assert "; Secure" not in cookies[0], "plain HTTP must not mark the cookie Secure"


def test_session_survives_a_shard_round_trip():
    from ministack.services import alb

    original = {"sub": "user-1", "email": "a@example.com",
                "id_token": "j" * 9000, "exp": 123}
    cookies = alb._oidc_session_cookies("AWSELBAuthSessionCookie", original, 3600, False)

    jar = {}
    for cookie in cookies:
        name, _, rest = cookie.partition("=")
        jar[name] = rest.split(";", 1)[0]

    assert alb._oidc_read_session(jar, "AWSELBAuthSessionCookie") == original


def test_a_missing_shard_invalidates_the_session():
    """A gap in the sequence means the client lost part of the session."""
    from ministack.services import alb

    cookies = alb._oidc_session_cookies("AWSELBAuthSessionCookie",
                                        {"id_token": "j" * 9000}, 3600, False)
    assert len(cookies) > 2
    jar = {}
    for cookie in cookies:
        name, _, rest = cookie.partition("=")
        jar[name] = rest.split(";", 1)[0]
    del jar["AWSELBAuthSessionCookie-1"]

    assert alb._oidc_read_session(jar, "AWSELBAuthSessionCookie") is None


def test_no_session_cookie_reads_as_no_session():
    from ministack.services import alb
    assert alb._oidc_read_session({}, "AWSELBAuthSessionCookie") is None
    assert alb._oidc_read_session({"AWSELBAuthSessionCookie-0": "not-base64!!"},
                                  "AWSELBAuthSessionCookie") is None


def test_cookie_header_parsing():
    from ministack.services import alb
    jar = alb._oidc_parse_cookies({"cookie": "a=1; b = 2 ;c=3; malformed"})
    assert jar == {"a": "1", "b": "2", "c": "3"}
    assert alb._oidc_parse_cookies({}) == {}


def test_claims_are_read_without_verification():
    """The ID token arrives over the back channel, so there is nothing to verify."""
    import base64

    from ministack.services import alb

    payload = base64.urlsafe_b64encode(
        json.dumps({"sub": "abc", "email": "a@example.com"}).encode()
    ).decode().rstrip("=")
    assert alb._oidc_claims(f"header.{payload}.signature")["sub"] == "abc"
    assert alb._oidc_claims("not-a-jwt") == {}


# ------------------------------------------------------------------ data plane

def _alb_with_oidc(elbv2, lam, name, **cfg_overrides):
    """Build an ALB whose only rule authenticates, then forwards to a Lambda."""
    fn_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    h = {k.lower(): v for k, v in (event.get('headers') or {}).items()}\n"
        "    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},\n"
        "            'body': json.dumps({'identity': h.get('x-amzn-oidc-identity'),\n"
        "                                'data': bool(h.get('x-amzn-oidc-data'))})}\n"
    )
    lb_arn, tg_arn, l_arn, fn_arn = _alb_setup(elbv2, lam, name, f"{name}-fn", fn_code)
    elbv2.modify_listener(
        ListenerArn=l_arn,
        DefaultActions=[
            _oidc_action(order=1, **cfg_overrides),
            {"Type": "forward", "Order": 2, "TargetGroupArn": tg_arn},
        ],
    )
    return lb_arn, tg_arn, l_arn


class _NoRedirect(_req.HTTPRedirectHandler):
    """Keep redirects visible.

    The point of most of these tests is the 302 itself, and its Location points
    at an identity provider that does not exist. Following it would turn the
    assertion into a DNS failure.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_opener = _req.build_opener(_NoRedirect)


def _get(path, host, headers=None):
    req = _req.Request(f"{_endpoint}{path}", method="GET")
    req.add_header("Host", f"{host}.alb.localhost:{_EXECUTE_PORT}")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        resp = _opener.open(req)
        return resp.status, dict(resp.headers), resp.read()
    except HTTPError as e:
        return e.code, dict(e.headers), e.read()


def test_unauthenticated_request_redirects_to_the_identity_provider(elbv2, lam):
    name = "oidc-dp-redir"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name)
    try:
        status, headers, _ = _get("/private/thing", name)
        assert status == 302

        location = urlparse(headers["Location"])
        assert location.netloc == "idp.example.com"
        assert location.path == "/authorize"

        q = parse_qs(location.query)
        assert q["client_id"] == ["client-abc"]
        assert q["response_type"] == ["code"]
        assert q["scope"] == ["openid email"]
        assert q["redirect_uri"][0].endswith("/oauth2/idpresponse")
        assert q["state"], "the provider must be given a state to return"
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_on_unauthenticated_request_allow_lets_the_request_through(elbv2, lam):
    name = "oidc-dp-allow"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name, OnUnauthenticatedRequest="allow")
    try:
        status, _, body = _get("/public", name)
        assert status == 200
        # The target runs, but with no identity attached.
        assert json.loads(body)["identity"] is None
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_on_unauthenticated_request_deny_refuses(elbv2, lam):
    name = "oidc-dp-deny"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name, OnUnauthenticatedRequest="deny")
    try:
        status, _, _ = _get("/private", name)
        assert status == 401
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_a_valid_session_reaches_the_target_with_identity_attached(elbv2, lam):
    name = "oidc-dp-session"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name)
    try:
        from ministack.services import alb

        session = {"sub": "user-42", "email": "u@example.com",
                   "access_token": "at", "id_token": "it",
                   "exp": int(time.time()) + 600}
        cookies = alb._oidc_session_cookies("AWSELBAuthSessionCookie", session, 600, False)
        jar = "; ".join(c.split(";", 1)[0] for c in cookies)

        status, _, body = _get("/private", name, {"Cookie": jar})
        assert status == 200
        payload = json.loads(body)
        assert payload["identity"] == "user-42"
        assert payload["data"] is True
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_an_expired_session_redirects_again(elbv2, lam):
    name = "oidc-dp-expired"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name)
    try:
        from ministack.services import alb

        session = {"sub": "user-42", "exp": int(time.time()) - 1}
        cookies = alb._oidc_session_cookies("AWSELBAuthSessionCookie", session, 600, False)
        jar = "; ".join(c.split(";", 1)[0] for c in cookies)

        status, headers, _ = _get("/private", name, {"Cookie": jar})
        assert status == 302
        assert "idp.example.com" in headers["Location"]
        # The dead shards are cleared so they cannot be replayed.
        assert "Set-Cookie" in headers
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_a_callback_without_a_matching_request_is_refused(elbv2, lam):
    """A replayed or forged callback has no pending state to match."""
    name = "oidc-dp-badcb"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name)
    try:
        status, _, _ = _get("/oauth2/idpresponse?code=abc&state=never-issued", name)
        assert status == 400
        status, _, _ = _get("/oauth2/idpresponse", name)
        assert status == 400
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_a_custom_session_cookie_name_is_honoured(elbv2, lam):
    name = "oidc-dp-cookiename"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name, SessionCookieName="MySession")
    try:
        from ministack.services import alb

        session = {"sub": "user-9", "exp": int(time.time()) + 600}
        cookies = alb._oidc_session_cookies("MySession", session, 600, False)
        jar = "; ".join(c.split(";", 1)[0] for c in cookies)

        status, _, body = _get("/private", name, {"Cookie": jar})
        assert status == 200
        assert json.loads(body)["identity"] == "user-9"

        # The default name must not be accepted when another was configured.
        wrong = alb._oidc_session_cookies("AWSELBAuthSessionCookie", session, 600, False)
        wrong_jar = "; ".join(c.split(";", 1)[0] for c in wrong)
        status, _, _ = _get("/private", name, {"Cookie": wrong_jar})
        assert status == 302
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_authenticate_cognito_is_reported_as_unimplemented(elbv2, lam):
    """Better a loud 501 than forwarding a request nobody authenticated."""
    name = "oidc-dp-cognito"
    fn_code = ("def handler(event, context):\n"
               "    return {'statusCode': 200, 'body': 'reached'}\n")
    lb_arn, tg_arn, l_arn, _ = _alb_setup(elbv2, lam, name, f"{name}-fn", fn_code)
    try:
        elbv2.modify_listener(
            ListenerArn=l_arn,
            DefaultActions=[
                {"Type": "authenticate-cognito", "Order": 1},
                {"Type": "forward", "Order": 2, "TargetGroupArn": tg_arn},
            ],
        )
        status, _, body = _get("/private", name)
        assert status == 501
        assert b"not implemented" in body
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_modify_listener_updates_the_serving_default_rule(elbv2, lam):
    """ModifyListener must change what the data plane actually serves.

    CreateListener copies the default actions into an auto-created default
    rule, and dispatch reads that rule. A ModifyListener that updates only the
    listener record reports success while traffic keeps hitting the old
    actions — silently, which is the worst way for it to be wrong.
    """
    name = "oidc-modify"
    fn_code = ("def handler(event, context):\n"
               "    return {'statusCode': 200, 'body': 'target'}\n")
    lb_arn, tg_arn, l_arn, _ = _alb_setup(elbv2, lam, name, f"{name}-fn", fn_code)
    try:
        status, _, body = _get("/anything", name)
        assert status == 200 and body == b"target"

        elbv2.modify_listener(ListenerArn=l_arn, DefaultActions=[{
            "Type": "fixed-response",
            "FixedResponseConfig": {"StatusCode": "418", "ContentType": "text/plain",
                                    "MessageBody": "changed"},
        }])

        status, _, body = _get("/anything", name)
        assert status == 418, "ModifyListener did not reach the data plane"
        assert body == b"changed"
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")
