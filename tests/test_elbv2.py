import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
_EXECUTE_PORT = urlparse(_endpoint).port or 4566

def test_elbv2_create_describe_delete_lb(elbv2):
    resp = elbv2.create_load_balancer(Name="qa-alb", Type="application", Scheme="internet-facing")
    lb = resp["LoadBalancers"][0]
    lb_arn = lb["LoadBalancerArn"]
    assert lb_arn.startswith("arn:aws:elasticloadbalancing")
    assert lb["LoadBalancerName"] == "qa-alb"
    assert lb["Type"] == "application"
    assert lb["Scheme"] == "internet-facing"
    assert "DNSName" in lb
    assert lb["State"]["Code"] == "active"

    desc = elbv2.describe_load_balancers(LoadBalancerArns=[lb_arn])
    assert desc["LoadBalancers"][0]["LoadBalancerArn"] == lb_arn

    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)
    desc2 = elbv2.describe_load_balancers()
    assert not any(l["LoadBalancerArn"] == lb_arn for l in desc2["LoadBalancers"])

def test_elbv2_describe_lb_by_name(elbv2):
    elbv2.create_load_balancer(Name="qa-alb-named")
    resp = elbv2.describe_load_balancers(Names=["qa-alb-named"])
    assert len(resp["LoadBalancers"]) == 1
    assert resp["LoadBalancers"][0]["LoadBalancerName"] == "qa-alb-named"
    elbv2.delete_load_balancer(LoadBalancerArn=resp["LoadBalancers"][0]["LoadBalancerArn"])

def test_elbv2_duplicate_lb_name(elbv2):
    elbv2.create_load_balancer(Name="qa-alb-dup")
    import botocore.exceptions

    try:
        elbv2.create_load_balancer(Name="qa-alb-dup")
        assert False, "should have raised"
    except botocore.exceptions.ClientError as e:
        assert "DuplicateLoadBalancerName" in str(e)
    finally:
        lbs = elbv2.describe_load_balancers(Names=["qa-alb-dup"])["LoadBalancers"]
        if lbs:
            elbv2.delete_load_balancer(LoadBalancerArn=lbs[0]["LoadBalancerArn"])

def test_elbv2_lb_attributes(elbv2):
    lb_arn = elbv2.create_load_balancer(Name="qa-alb-attrs")["LoadBalancers"][0]["LoadBalancerArn"]
    attrs = elbv2.describe_load_balancer_attributes(LoadBalancerArn=lb_arn)["Attributes"]
    keys = {a["Key"] for a in attrs}
    assert "idle_timeout.timeout_seconds" in keys

    elbv2.modify_load_balancer_attributes(
        LoadBalancerArn=lb_arn,
        Attributes=[{"Key": "idle_timeout.timeout_seconds", "Value": "120"}],
    )
    updated = elbv2.describe_load_balancer_attributes(LoadBalancerArn=lb_arn)["Attributes"]
    val = next(a["Value"] for a in updated if a["Key"] == "idle_timeout.timeout_seconds")
    assert val == "120"
    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

def test_elbv2_create_describe_delete_tg(elbv2):
    resp = elbv2.create_target_group(
        Name="qa-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        HealthCheckPath="/health",
    )
    tg = resp["TargetGroups"][0]
    tg_arn = tg["TargetGroupArn"]
    assert tg_arn.startswith("arn:aws:elasticloadbalancing")
    assert tg["TargetGroupName"] == "qa-tg"
    assert tg["HealthCheckPath"] == "/health"

    desc = elbv2.describe_target_groups(TargetGroupArns=[tg_arn])
    assert desc["TargetGroups"][0]["TargetGroupArn"] == tg_arn

    elbv2.delete_target_group(TargetGroupArn=tg_arn)
    desc2 = elbv2.describe_target_groups()
    assert not any(t["TargetGroupArn"] == tg_arn for t in desc2["TargetGroups"])

def test_elbv2_tg_attributes(elbv2):
    tg_arn = elbv2.create_target_group(
        Name="qa-tg-attrs",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
    )["TargetGroups"][0]["TargetGroupArn"]
    attrs = elbv2.describe_target_group_attributes(TargetGroupArn=tg_arn)["Attributes"]
    keys = {a["Key"] for a in attrs}
    assert "deregistration_delay.timeout_seconds" in keys

    elbv2.modify_target_group_attributes(
        TargetGroupArn=tg_arn,
        Attributes=[{"Key": "deregistration_delay.timeout_seconds", "Value": "60"}],
    )
    updated = elbv2.describe_target_group_attributes(TargetGroupArn=tg_arn)["Attributes"]
    val = next(a["Value"] for a in updated if a["Key"] == "deregistration_delay.timeout_seconds")
    assert val == "60"
    elbv2.delete_target_group(TargetGroupArn=tg_arn)

def test_elbv2_listener_crud(elbv2):
    lb_arn = elbv2.create_load_balancer(Name="qa-alb-listener")["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name="qa-tg-l",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
    )["TargetGroups"][0]["TargetGroupArn"]

    l_resp = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )
    listener = l_resp["Listeners"][0]
    l_arn = listener["ListenerArn"]
    assert l_arn.startswith("arn:aws:elasticloadbalancing")
    assert listener["Port"] == 80
    assert listener["Protocol"] == "HTTP"

    desc = elbv2.describe_listeners(LoadBalancerArn=lb_arn)
    assert any(l["ListenerArn"] == l_arn for l in desc["Listeners"])

    # TG should now reference LB
    tg_desc = elbv2.describe_target_groups(TargetGroupArns=[tg_arn])["TargetGroups"][0]
    assert lb_arn in tg_desc["LoadBalancerArns"]

    elbv2.modify_listener(ListenerArn=l_arn, Port=8080)
    updated = elbv2.describe_listeners(ListenerArns=[l_arn])["Listeners"][0]
    assert updated["Port"] == 8080

    elbv2.delete_listener(ListenerArn=l_arn)
    desc2 = elbv2.describe_listeners(LoadBalancerArn=lb_arn)
    assert not any(l["ListenerArn"] == l_arn for l in desc2["Listeners"])

    elbv2.delete_target_group(TargetGroupArn=tg_arn)
    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)


def test_elbv2_describe_listener_attributes(elbv2):
    lb_arn = elbv2.create_load_balancer(Name="qa-alb-listener-attrs")["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name="qa-tg-la",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
    )["TargetGroups"][0]["TargetGroupArn"]
    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )["Listeners"][0]["ListenerArn"]

    resp = elbv2.describe_listener_attributes(ListenerArn=l_arn)
    attrs = {a["Key"]: a["Value"] for a in resp["Attributes"]}
    assert attrs.get("routing.http.response.server.enabled") == "true"

    elbv2.delete_listener(ListenerArn=l_arn)
    elbv2.delete_target_group(TargetGroupArn=tg_arn)
    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)


def test_elbv2_describe_listener_attributes_not_found(elbv2):
    with pytest.raises(ClientError) as exc:
        elbv2.describe_listener_attributes(ListenerArn="arn:aws:elasticloadbalancing:us-east-1:000000000000:listener/app/missing/abc/def")
    assert exc.value.response["Error"]["Code"] == "ListenerNotFound"


def test_elbv2_modify_listener_attributes(elbv2):
    lb_arn = elbv2.create_load_balancer(Name="qa-alb-mod-listener-attrs")["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name="qa-tg-mla",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
    )["TargetGroups"][0]["TargetGroupArn"]
    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )["Listeners"][0]["ListenerArn"]

    resp = elbv2.modify_listener_attributes(
        ListenerArn=l_arn,
        Attributes=[
            {"Key": "routing.http.response.server.enabled", "Value": "false"},
            {"Key": "routing.http.response.strict_transport_security.header_value", "Value": "max-age=31536000"},
        ],
    )
    attrs = {a["Key"]: a["Value"] for a in resp["Attributes"]}
    assert attrs["routing.http.response.server.enabled"] == "false"
    assert attrs["routing.http.response.strict_transport_security.header_value"] == "max-age=31536000"

    desc = elbv2.describe_listener_attributes(ListenerArn=l_arn)
    desc_attrs = {a["Key"]: a["Value"] for a in desc["Attributes"]}
    assert desc_attrs["routing.http.response.server.enabled"] == "false"
    assert desc_attrs["routing.http.response.strict_transport_security.header_value"] == "max-age=31536000"

    elbv2.delete_listener(ListenerArn=l_arn)
    elbv2.delete_target_group(TargetGroupArn=tg_arn)
    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)


def test_elbv2_modify_listener_attributes_not_found(elbv2):
    with pytest.raises(ClientError) as exc:
        elbv2.modify_listener_attributes(
            ListenerArn="arn:aws:elasticloadbalancing:us-east-1:000000000000:listener/app/missing/abc/def",
            Attributes=[{"Key": "routing.http.response.server.enabled", "Value": "false"}],
        )
    assert exc.value.response["Error"]["Code"] == "ListenerNotFound"

def test_elbv2_rule_crud(elbv2):
    lb_arn = elbv2.create_load_balancer(Name="qa-alb-rules")["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name="qa-tg-r",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
    )["TargetGroups"][0]["TargetGroupArn"]
    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )["Listeners"][0]["ListenerArn"]

    # describe should include default rule
    rules = elbv2.describe_rules(ListenerArn=l_arn)["Rules"]
    assert any(r["IsDefault"] for r in rules)

    # create a custom rule
    rule_resp = elbv2.create_rule(
        ListenerArn=l_arn,
        Priority=10,
        Conditions=[{"Field": "path-pattern", "Values": ["/api/*"]}],
        Actions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )
    rule = rule_resp["Rules"][0]
    r_arn = rule["RuleArn"]
    assert not rule["IsDefault"]
    assert rule["Priority"] == "10"

    rules2 = elbv2.describe_rules(ListenerArn=l_arn)["Rules"]
    assert any(r["RuleArn"] == r_arn for r in rules2)

    elbv2.delete_rule(RuleArn=r_arn)
    rules3 = elbv2.describe_rules(ListenerArn=l_arn)["Rules"]
    assert not any(r["RuleArn"] == r_arn for r in rules3)

    elbv2.delete_listener(ListenerArn=l_arn)
    elbv2.delete_target_group(TargetGroupArn=tg_arn)
    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

def test_elbv2_register_deregister_targets(elbv2):
    tg_arn = elbv2.create_target_group(
        Name="qa-tg-targets",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
    )["TargetGroups"][0]["TargetGroupArn"]

    elbv2.register_targets(
        TargetGroupArn=tg_arn,
        Targets=[{"Id": "i-0001", "Port": 80}, {"Id": "i-0002", "Port": 80}],
    )
    health = elbv2.describe_target_health(TargetGroupArn=tg_arn)
    assert len(health["TargetHealthDescriptions"]) == 2
    ids = {d["Target"]["Id"] for d in health["TargetHealthDescriptions"]}
    assert ids == {"i-0001", "i-0002"}
    for d in health["TargetHealthDescriptions"]:
        assert d["TargetHealth"]["State"] == "healthy"

    elbv2.deregister_targets(TargetGroupArn=tg_arn, Targets=[{"Id": "i-0001"}])
    health2 = elbv2.describe_target_health(TargetGroupArn=tg_arn)
    assert len(health2["TargetHealthDescriptions"]) == 1
    assert health2["TargetHealthDescriptions"][0]["Target"]["Id"] == "i-0002"

    elbv2.delete_target_group(TargetGroupArn=tg_arn)

def test_elbv2_tags(elbv2):
    lb_arn = elbv2.create_load_balancer(
        Name="qa-alb-tags",
        Tags=[{"Key": "env", "Value": "test"}],
    )["LoadBalancers"][0]["LoadBalancerArn"]

    elbv2.add_tags(
        ResourceArns=[lb_arn],
        Tags=[{"Key": "team", "Value": "infra"}],
    )
    desc = elbv2.describe_tags(ResourceArns=[lb_arn])
    tag_map = {t["Key"]: t["Value"] for t in desc["TagDescriptions"][0]["Tags"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "infra"

    elbv2.remove_tags(ResourceArns=[lb_arn], TagKeys=["env"])
    desc2 = elbv2.describe_tags(ResourceArns=[lb_arn])
    tag_map2 = {t["Key"]: t["Value"] for t in desc2["TagDescriptions"][0]["Tags"]}
    assert "env" not in tag_map2
    assert tag_map2["team"] == "infra"

    elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

# Migrated from test_alb.py
def _alb_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()

def _alb_setup(elbv2, lam, lb_name, fn_name, fn_code, listener_port=80, extra_rules=None):
    """Create LB + Lambda TG + listener + register Lambda as target.
    Returns (lb_arn, tg_arn, l_arn, fn_arn).
    """
    # Lambda
    lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _alb_zip(fn_code)},
    )
    fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]

    # ALB infra
    lb_arn = elbv2.create_load_balancer(Name=lb_name)["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name=f"{lb_name}-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        TargetType="lambda",
    )["TargetGroups"][0]["TargetGroupArn"]
    elbv2.register_targets(TargetGroupArn=tg_arn, Targets=[{"Id": fn_arn}])

    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=listener_port,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )["Listeners"][0]["ListenerArn"]

    for rule_kwargs in extra_rules or []:
        elbv2.create_rule(ListenerArn=l_arn, **rule_kwargs)

    return lb_arn, tg_arn, l_arn, fn_arn

def _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, fn_name):
    try:
        elbv2.delete_listener(ListenerArn=l_arn)
    except Exception:
        pass
    try:
        elbv2.delete_target_group(TargetGroupArn=tg_arn)
    except Exception:
        pass
    try:
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)
    except Exception:
        pass
    try:
        lam.delete_function(FunctionName=fn_name)
    except Exception:
        pass

def test_elbv2_dataplane_forward_lambda(elbv2, lam):
    """ALB forwards request to Lambda via /_alb/{lb-name}/ path prefix."""
    import urllib.request as _req

    fn_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {\n"
        "        'statusCode': 200,\n"
        "        'headers': {'Content-Type': 'application/json'},\n"
        "        'body': json.dumps({'method': event['httpMethod'], 'path': event['path']}),\n"
        "    }\n"
    )
    lb_arn, tg_arn, l_arn, fn_arn = _alb_setup(elbv2, lam, "dp-alb-fwd", "dp-alb-fwd-fn", fn_code)
    try:
        url = f"{_endpoint}/_alb/dp-alb-fwd/api/hello"
        resp = _req.urlopen(_req.Request(url, method="GET"))
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body["method"] == "GET"
        assert body["path"] == "/api/hello"
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, "dp-alb-fwd-fn")

def test_elbv2_dataplane_event_shape(elbv2, lam):
    """ALB event passed to Lambda contains all required fields."""
    import urllib.parse as _parse
    import urllib.request as _req

    fn_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {\n"
        "        'statusCode': 200,\n"
        "        'headers': {'Content-Type': 'application/json'},\n"
        "        'body': json.dumps(event),\n"
        "    }\n"
    )
    lb_arn, tg_arn, l_arn, fn_arn = _alb_setup(elbv2, lam, "dp-alb-evt", "dp-alb-evt-fn", fn_code)
    try:
        url = f"{_endpoint}/_alb/dp-alb-evt/check?foo=bar"
        resp = _req.urlopen(_req.Request(url, method="GET"))
        body = json.loads(resp.read())
        assert "requestContext" in body
        assert "elb" in body["requestContext"]
        assert body["httpMethod"] == "GET"
        assert body["path"] == "/check"
        assert body["queryStringParameters"].get("foo") == "bar"
        assert "headers" in body
        assert body["isBase64Encoded"] is False
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, "dp-alb-evt-fn")

def test_elbv2_dataplane_fixed_response(elbv2, lam):
    """ALB fixed-response action returns configured status/body without invoking Lambda."""
    import urllib.error as _err
    import urllib.request as _req

    fn_code = "def handler(event, context):\n    return {'statusCode': 200, 'body': 'should-not-reach'}\n"
    lb_arn = elbv2.create_load_balancer(Name="dp-alb-fixed")["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name="dp-alb-fixed-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        TargetType="lambda",
    )["TargetGroups"][0]["TargetGroupArn"]
    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[
            {
                "Type": "fixed-response",
                "FixedResponseConfig": {
                    "StatusCode": "200",
                    "ContentType": "text/plain",
                    "MessageBody": "maintenance",
                },
            }
        ],
    )["Listeners"][0]["ListenerArn"]
    try:
        url = f"{_endpoint}/_alb/dp-alb-fixed/any/path"
        resp = _req.urlopen(_req.Request(url, method="GET"))
        assert resp.status == 200
        assert resp.read() == b"maintenance"
    finally:
        elbv2.delete_listener(ListenerArn=l_arn)
        elbv2.delete_target_group(TargetGroupArn=tg_arn)
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)
        try:
            lam.delete_function(FunctionName="dp-alb-fixed-fn")
        except Exception:
            pass

def test_elbv2_dataplane_redirect(elbv2):
    """ALB redirect action returns 301 with a Location header."""
    import http.client as _http
    from urllib.parse import urlparse as _urlparse

    lb_arn = elbv2.create_load_balancer(Name="dp-alb-redir")["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name="dp-alb-redir-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        TargetType="lambda",
    )["TargetGroups"][0]["TargetGroupArn"]
    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[
            {
                "Type": "redirect",
                "RedirectConfig": {
                    "Protocol": "https",
                    "Host": "example.com",
                    "Path": "/new",
                    "StatusCode": "HTTP_301",
                },
            }
        ],
    )["Listeners"][0]["ListenerArn"]
    try:
        # Use http.client directly — it never auto-follows redirects
        parsed = _urlparse(_endpoint)
        conn = _http.HTTPConnection(parsed.hostname, parsed.port or 4566)
        conn.request("GET", "/_alb/dp-alb-redir/old")
        resp = conn.getresponse()
        assert resp.status == 301
        location = resp.getheader("Location", "")
        assert "example.com" in location
        conn.close()
    finally:
        elbv2.delete_listener(ListenerArn=l_arn)
        elbv2.delete_target_group(TargetGroupArn=tg_arn)
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

def test_elbv2_dataplane_path_pattern_rule(elbv2, lam):
    """Path-pattern rule routes /api/* to one Lambda; default routes to another."""
    import urllib.request as _req

    api_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},\n"
        "            'body': json.dumps({'target': 'api'})}\n"
    )
    default_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},\n"
        "            'body': json.dumps({'target': 'default'})}\n"
    )
    for fn_name, fn_code in [("dp-alb-api-fn", api_code), ("dp-alb-def-fn", default_code)]:
        lam.create_function(
            FunctionName=fn_name,
            Runtime="python3.9",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler",
            Code={"ZipFile": _alb_zip(fn_code)},
        )

    lb_arn = elbv2.create_load_balancer(Name="dp-alb-rules")["LoadBalancers"][0]["LoadBalancerArn"]
    api_tg_arn = elbv2.create_target_group(
        Name="dp-alb-api-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        TargetType="lambda",
    )["TargetGroups"][0]["TargetGroupArn"]
    def_tg_arn = elbv2.create_target_group(
        Name="dp-alb-def-tg",
        Protocol="HTTP",
        Port=80,
        VpcId="vpc-00000001",
        TargetType="lambda",
    )["TargetGroups"][0]["TargetGroupArn"]

    api_fn_arn = lam.get_function(FunctionName="dp-alb-api-fn")["Configuration"]["FunctionArn"]
    def_fn_arn = lam.get_function(FunctionName="dp-alb-def-fn")["Configuration"]["FunctionArn"]
    elbv2.register_targets(TargetGroupArn=api_tg_arn, Targets=[{"Id": api_fn_arn}])
    elbv2.register_targets(TargetGroupArn=def_tg_arn, Targets=[{"Id": def_fn_arn}])

    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": def_tg_arn}],
    )["Listeners"][0]["ListenerArn"]
    elbv2.create_rule(
        ListenerArn=l_arn,
        Priority=10,
        Conditions=[{"Field": "path-pattern", "Values": ["/api/*"]}],
        Actions=[{"Type": "forward", "TargetGroupArn": api_tg_arn}],
    )

    try:
        # /api/* hits the api Lambda
        resp_api = _req.urlopen(_req.Request(f"{_endpoint}/_alb/dp-alb-rules/api/users", method="GET"))
        body_api = json.loads(resp_api.read())
        assert body_api["target"] == "api"

        # /other hits the default Lambda
        resp_def = _req.urlopen(_req.Request(f"{_endpoint}/_alb/dp-alb-rules/other", method="GET"))
        body_def = json.loads(resp_def.read())
        assert body_def["target"] == "default"
    finally:
        elbv2.delete_listener(ListenerArn=l_arn)
        for tg in (api_tg_arn, def_tg_arn):
            elbv2.delete_target_group(TargetGroupArn=tg)
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)
        for fn_name in ("dp-alb-api-fn", "dp-alb-def-fn"):
            try:
                lam.delete_function(FunctionName=fn_name)
            except Exception:
                pass

def test_elbv2_dataplane_no_listener_returns_503(elbv2):
    """Request to an ALB with no listeners returns 503."""
    import urllib.error as _err
    import urllib.request as _req

    lb_arn = elbv2.create_load_balancer(Name="dp-alb-empty")["LoadBalancers"][0]["LoadBalancerArn"]
    try:
        req = _req.Request(f"{_endpoint}/_alb/dp-alb-empty/anything", method="GET")
        try:
            _req.urlopen(req)
            assert False, "Expected 503"
        except _err.HTTPError as e:
            assert e.code == 503
    finally:
        elbv2.delete_load_balancer(LoadBalancerArn=lb_arn)

def test_elbv2_dataplane_host_header_routing(elbv2, lam):
    """ALB matches requests by {lb-name}.alb.localhost Host header."""
    import urllib.request as _req

    fn_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},\n"
        "            'body': json.dumps({'routed': True})}\n"
    )
    lb_arn, tg_arn, l_arn, fn_arn = _alb_setup(elbv2, lam, "dp-alb-host", "dp-alb-host-fn", fn_code)
    try:
        # Send to the plain ministack port but with the ALB host header
        req = _req.Request(f"{_endpoint}/hello", method="GET")
        req.add_header("Host", f"dp-alb-host.alb.localhost:{_EXECUTE_PORT}")
        resp = _req.urlopen(req)
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body["routed"] is True
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, "dp-alb-host-fn")
