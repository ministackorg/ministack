import io
import json
import os
import time
import urllib.request as _req
import uuid as _uuid_mod
import zipfile
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

import pytest
from botocore.exceptions import ClientError

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
_EXECUTE_PORT = urlparse(_endpoint).port or 4566
# Predict the server-side loopback port. Under the matched-port protocol,
# MINISTACK_ENDPOINT alone implies a direct connection (endpoint port == server port);
# split-port topologies must export GATEWAY_PORT/EDGE_PORT into the test env.
_SERVER_GATEWAY_PORT = int(
    os.environ.get("GATEWAY_PORT") or os.environ.get("EDGE_PORT") or _EXECUTE_PORT
)


def test_elbv2_arn_tail_helpers_require_elbv2_resource_scope():
    from ministack.services import alb

    assert alb._load_balancer_id_from_arn(
        "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/lb-id"
    ) == "lb-id"
    assert alb._listener_id_from_arn(
        "arn:aws:elasticloadbalancing:us-east-1:000000000000:listener/app/my-lb/lb-id/listener-id"
    ) == "listener-id"
    assert alb._target_group_full_name_from_arn(
        "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/my-tg/tg-id"
    ) == "my-tg/tg-id"
    assert alb._load_balancer_id_from_arn(
        "arn:aws:sqs:us-east-1:000000000000:loadbalancer/app/my-lb/lb-id"
    ) == ""
    assert alb._listener_id_from_arn(
        "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/my-tg/tg-id"
    ) == ""


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


@pytest.mark.parametrize(
    ("arn", "code"),
    [
        ("not-an-arn", "ValidationError"),
        (
            "arn:aws:sqs:us-east-1:000000000000:loadbalancer/app/qa-alb-tags/missing",
            "ValidationError",
        ),
        (
            "arn:aws:elasticloadbalancing:us-west-2:000000000000:loadbalancer/app/qa-alb-tags/missing",
            "ValidationError",
        ),
        (
            "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/qa-alb-tags/missing",
            "LoadBalancerNotFound",
        ),
    ],
)
def test_elbv2_tag_arns_must_parse_to_local_resources(elbv2, arn, code):
    with pytest.raises(ClientError) as exc:
        elbv2.add_tags(ResourceArns=[arn], Tags=[{"Key": "env", "Value": "test"}])

    assert exc.value.response["Error"]["Code"] == code


def test_elbv2_add_tags_validates_all_arns_before_mutating(elbv2):
    lb_arn = elbv2.create_load_balancer(Name="qa-alb-tags-atomic")["LoadBalancers"][0]["LoadBalancerArn"]
    missing_arn = (
        "arn:aws:elasticloadbalancing:us-east-1:000000000000:"
        "loadbalancer/app/qa-alb-tags-atomic/missing"
    )

    with pytest.raises(ClientError) as exc:
        elbv2.add_tags(
            ResourceArns=[lb_arn, missing_arn],
            Tags=[{"Key": "team", "Value": "infra"}],
        )

    assert exc.value.response["Error"]["Code"] == "LoadBalancerNotFound"
    desc = elbv2.describe_tags(ResourceArns=[lb_arn])
    assert desc["TagDescriptions"][0]["Tags"] == []

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
        Runtime="python3.12",
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

@pytest.mark.serial
def test_elbv2_dataplane_forward_lambda(elbv2, lam, cw):
    """ALB forwards request to Lambda via /_alb/{lb-name}/ path prefix."""
    import urllib.request as _req

    fn_name = "dp-alb-fwd-fn"
    fn_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {\n"
        "        'statusCode': 200,\n"
        "        'headers': {'Content-Type': 'application/json'},\n"
        "        'body': json.dumps({'method': event['httpMethod'], 'path': event['path']}),\n"
        "    }\n"
    )
    lb_arn, tg_arn, l_arn, fn_arn = _alb_setup(elbv2, lam, "dp-alb-fwd", fn_name, fn_code)
    try:
        url = f"{_endpoint}/_alb/dp-alb-fwd/api/hello"
        resp = _req.urlopen(_req.Request(url, method="GET"))
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body["method"] == "GET"
        assert body["path"] == "/api/hello"

        end = time.time() + 60
        start = end - 600
        invocations = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start,
            EndTime=end,
            Period=60,
            Statistics=["Sum"],
        )
        total = sum(p["Sum"] for p in invocations["Datapoints"])
        assert total >= 1, f"expected ALB Lambda target to emit metrics, got {total}"
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, fn_name)


@pytest.mark.serial
def test_elbv2_dataplane_lambda_target_emits_metrics(elbv2, lam, cw):
    import urllib.request as _req

    suffix = _uuid_mod.uuid4().hex[:8]
    lb_name = f"alb-met-{suffix}"
    fn_name = f"alb-met-fn-{suffix}"
    fn_code = (
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'body': 'ok'}\n"
    )
    lb_arn, tg_arn, l_arn, _fn_arn = _alb_setup(elbv2, lam, lb_name, fn_name, fn_code)
    try:
        url = f"{_endpoint}/_alb/{lb_name}/metrics"
        resp = _req.urlopen(_req.Request(url, method="GET"))
        assert resp.status == 200

        end = time.time() + 1
        start = end - 600
        invocations = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start, EndTime=end,
            Period=60, Statistics=["Sum"],
        )
        total = sum(p["Sum"] for p in invocations["Datapoints"])
        assert total >= 1, f"expected >=1 invocation, got {total}"

        duration = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Duration",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start, EndTime=end,
            Period=60, Statistics=["Average"],
        )
        assert duration["Datapoints"], "no Duration datapoints recorded"
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, fn_name)


def test_elbv2_dataplane_event_shape(elbv2, lam):
    """ALB event passed to Lambda contains all required fields."""
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
            Runtime="python3.12",
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


def test_alb_set_subnets_updates_lb(elbv2):
    """SetSubnets replaces the LB's Subnets and returns AvailabilityZones."""
    arn = elbv2.create_load_balancer(
        Name="qa-alb-setsub",
        Subnets=["subnet-aaa"],
    )["LoadBalancers"][0]["LoadBalancerArn"]
    resp = elbv2.set_subnets(LoadBalancerArn=arn, Subnets=["subnet-bbb", "subnet-ccc"])
    assert resp["IpAddressType"] in ("ipv4", "dualstack", "dualstack-without-public-ipv4")
    zone_subnets = {z["SubnetId"] for z in resp["AvailabilityZones"]}
    assert zone_subnets == {"subnet-bbb", "subnet-ccc"}


def test_alb_set_ip_address_type(elbv2):
    arn = elbv2.create_load_balancer(Name="qa-alb-setip")["LoadBalancers"][0]["LoadBalancerArn"]
    resp = elbv2.set_ip_address_type(LoadBalancerArn=arn, IpAddressType="dualstack")
    assert resp["IpAddressType"] == "dualstack"
    desc = elbv2.describe_load_balancers(LoadBalancerArns=[arn])["LoadBalancers"][0]
    assert desc["IpAddressType"] == "dualstack"


def test_alb_set_security_groups(elbv2):
    """SetSecurityGroups returns SecurityGroupIds per botocore output shape."""
    arn = elbv2.create_load_balancer(
        Name="qa-alb-setsg",
        SecurityGroups=["sg-aaa"],
    )["LoadBalancers"][0]["LoadBalancerArn"]
    resp = elbv2.set_security_groups(LoadBalancerArn=arn, SecurityGroups=["sg-bbb", "sg-ccc"])
    assert resp["SecurityGroupIds"] == ["sg-bbb", "sg-ccc"]


def _alb_http_target_setup(elbv2, lb_name, target_id, target_port, target_type="ip"):
    """Create LB + instance/ip TG + listener + register an HTTP target.
    Returns (lb_arn, tg_arn, l_arn).
    """
    lb_arn = elbv2.create_load_balancer(Name=lb_name)["LoadBalancers"][0]["LoadBalancerArn"]
    tg_arn = elbv2.create_target_group(
        Name=f"{lb_name}-tg",
        Protocol="HTTP",
        Port=target_port,
        VpcId="vpc-00000001",
        TargetType=target_type,
    )["TargetGroups"][0]["TargetGroupArn"]
    elbv2.register_targets(TargetGroupArn=tg_arn, Targets=[{"Id": target_id, "Port": target_port}])
    l_arn = elbv2.create_listener(
        LoadBalancerArn=lb_arn,
        Protocol="HTTP",
        Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
    )["Listeners"][0]["ListenerArn"]
    return lb_arn, tg_arn, l_arn


def _alb_http_target_teardown(elbv2, lb_arn, tg_arn, l_arn):
    for fn, kwargs in (
        (elbv2.delete_listener, {"ListenerArn": l_arn}),
        (elbv2.delete_target_group, {"TargetGroupArn": tg_arn}),
        (elbv2.delete_load_balancer, {"LoadBalancerArn": lb_arn}),
    ):
        try:
            fn(**kwargs)
        except Exception:
            pass


@pytest.mark.serial
def test_elbv2_dataplane_forward_ip_target(elbv2):
    """ALB data plane proxies instance/ip targets over HTTP.

    The emulator's own health endpoint (127.0.0.1 at the configured gateway port from inside the
    server process) doubles as the backend, so the test needs no external
    HTTP server and works both in-container and locally.
    """
    import urllib.request as _req

    lb_name = "dp-alb-ip"
    lb_arn, tg_arn, l_arn = _alb_http_target_setup(
        elbv2, lb_name, "127.0.0.1", _SERVER_GATEWAY_PORT
    )
    try:
        url = f"{_endpoint}/_alb/{lb_name}/_ministack/health"
        resp = _req.urlopen(_req.Request(url, method="GET"))
        assert resp.status == 200
        body = json.loads(resp.read())
        assert "services" in body
    finally:
        _alb_http_target_teardown(elbv2, lb_arn, tg_arn, l_arn)


def _start_chunked_server(chunks, delay):
    """Serve `chunks` with `delay` seconds between them and no Content-Length."""
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for i, chunk in enumerate(chunks):
                if i:
                    time.sleep(delay)
                self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
                self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _start_fixed_body_server(body):
    """Serve `body` in one shot with a Content-Length."""
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _set_alb_config(**values):
    """Set alb module config on the server via the runtime config endpoint."""
    import urllib.request as _req

    payload = json.dumps({f"alb.{k}": v for k, v in values.items()}).encode()
    _req.urlopen(_req.Request(f"{_endpoint}/_ministack/config", data=payload, method="POST"))


def _start_dying_chunked_server(chunk):
    """Chunked target that sends one chunk then dies without the 0-length end.

    Chunked is the framing where a premature close is invisible: there is no
    declared length for the client to check, so a proxy that terminates the
    body cleanly makes a truncated stream look finished.
    """
    import socket
    import threading

    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)

    def _serve():
        try:
            conn, _ = sock.accept()
            conn.recv(4096)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                b"Transfer-Encoding: chunked\r\n\r\n"
                b"%x\r\n%s\r\n" % (len(chunk), chunk)
            )
            conn.close()
        except OSError:
            pass

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()
    return sock, thread


@pytest.mark.serial
def test_elbv2_dataplane_truncated_target_body_is_not_passed_off_as_complete(elbv2):
    """A target that dies mid-body must not read as a short but valid response.

    The proxy must withhold the terminating frame, otherwise a truncated
    chunked stream reaches the client as a well-formed one and there is nothing
    left for it to detect.
    """
    import urllib.request as _req

    sock, thread = _start_dying_chunked_server(b"partial")
    port = sock.getsockname()[1]

    lb_name = "dp-alb-dying"
    lb_arn, tg_arn, l_arn = _alb_http_target_setup(elbv2, lb_name, "127.0.0.1", port)
    try:
        resp = _req.urlopen(_req.Request(f"{_endpoint}/_alb/{lb_name}/", method="GET"))
        with pytest.raises(Exception) as excinfo:
            resp.read()
        assert "IncompleteRead" in type(excinfo.value).__name__ or "Incomplete" in str(excinfo.value)
    finally:
        _alb_http_target_teardown(elbv2, lb_arn, tg_arn, l_arn)
        sock.close()
        thread.join(timeout=5)


def _start_raw_server(script):
    """Serve one connection by running `script(conn)` on a raw socket."""
    import socket as _socket
    import threading

    sock = _socket.socket()
    sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)

    def _serve():
        try:
            conn, _ = sock.accept()
            conn.recv(4096)
            script(conn)
            conn.close()
        except OSError:
            pass

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()
    return sock, thread


@pytest.mark.serial
def test_elbv2_dataplane_connection_close_target_gets_the_idle_timeout(elbv2):
    """A `Connection: close` target must be bounded by the idle timeout too.

    http.client drops conn.sock for any will_close response, so a timeout set
    after getresponse() silently never lands and the connect deadline governs
    the whole stream.
    """
    import urllib.request as _req

    def _script(conn):
        conn.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
            b"Connection: close\r\n\r\ndata: a\n\n"
        )
        time.sleep(3.0)
        conn.sendall(b"data: b\n\n")

    sock, thread = _start_raw_server(_script)
    port = sock.getsockname()[1]

    lb_name = "dp-alb-close"
    lb_arn, tg_arn, l_arn = _alb_http_target_setup(elbv2, lb_name, "127.0.0.1", port)
    try:
        _set_alb_config(TARGET_CONNECT_TIMEOUT=1.0, TARGET_IDLE_TIMEOUT=30.0)
        body = _req.urlopen(_req.Request(f"{_endpoint}/_alb/{lb_name}/", method="GET")).read()
        assert body.count(b"data:") == 2, f"stream cut short at the connect deadline: {body!r}"
    finally:
        _set_alb_config(TARGET_CONNECT_TIMEOUT=10.0, TARGET_IDLE_TIMEOUT=60.0)
        _alb_http_target_teardown(elbv2, lb_arn, tg_arn, l_arn)
        sock.close()
        thread.join(timeout=5)


@pytest.mark.serial
def test_elbv2_dataplane_slow_target_headers_are_not_a_connect_failure(elbv2):
    """A target slow to send headers is idle, not unreachable.

    The connect deadline covers establishing the connection; waiting on the
    response belongs to the idle budget.
    """
    import urllib.request as _req

    def _script(conn):
        time.sleep(3.0)
        conn.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello"
        )

    sock, thread = _start_raw_server(_script)
    port = sock.getsockname()[1]

    lb_name = "dp-alb-slowhdr"
    lb_arn, tg_arn, l_arn = _alb_http_target_setup(elbv2, lb_name, "127.0.0.1", port)
    try:
        _set_alb_config(TARGET_CONNECT_TIMEOUT=1.0, TARGET_IDLE_TIMEOUT=30.0)
        resp = _req.urlopen(_req.Request(f"{_endpoint}/_alb/{lb_name}/", method="GET"))
        assert resp.status == 200
        assert resp.read() == b"hello"
    finally:
        _set_alb_config(TARGET_CONNECT_TIMEOUT=10.0, TARGET_IDLE_TIMEOUT=60.0)
        _alb_http_target_teardown(elbv2, lb_arn, tg_arn, l_arn)
        sock.close()
        thread.join(timeout=5)


@pytest.mark.serial
def test_elbv2_dataplane_streams_target_response(elbv2):
    """A streaming target reaches the client as it produces, not at the end.

    Four chunks 0.25s apart: buffering makes the first byte land with the last
    one, so first-byte time would equal total time.
    """
    import urllib.request as _req

    chunks = [b"chunk-%d" % i for i in range(4)]
    delay = 0.25
    server, thread = _start_chunked_server(chunks, delay)
    port = server.server_address[1]

    lb_name = "dp-alb-stream"
    lb_arn, tg_arn, l_arn = _alb_http_target_setup(elbv2, lb_name, "127.0.0.1", port)
    try:
        started = time.monotonic()
        resp = _req.urlopen(_req.Request(f"{_endpoint}/_alb/{lb_name}/", method="GET"))
        first = resp.read(len(chunks[0]))
        first_byte_at = time.monotonic() - started
        rest = resp.read()
        total_at = time.monotonic() - started

        assert resp.status == 200
        assert first + rest == b"".join(chunks)
        # Three inter-chunk gaps, so the body spans ~0.75s. Half of that is a
        # wide margin against scheduling noise while still failing outright if
        # the response is buffered.
        assert first_byte_at < total_at - (delay * 3) / 2, (
            f"first byte at {first_byte_at:.3f}s, complete at {total_at:.3f}s — response was buffered"
        )
    finally:
        _alb_http_target_teardown(elbv2, lb_arn, tg_arn, l_arn)
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.serial
def test_elbv2_dataplane_non_streaming_body_is_intact(elbv2):
    """A plain Content-Length response survives the read loop unchanged.

    The body is larger than one read so a truncating or mis-ordered loop shows
    up as a length or content mismatch. The framing is asserted too: AWS ALB
    passes the target's Content-Length through rather than re-chunking, so a
    fixed-length target must not arrive chunked.
    """
    import urllib.request as _req

    body = bytes(range(256)) * 1024  # 256 KiB, spans several reads
    server, thread = _start_fixed_body_server(body)
    port = server.server_address[1]

    lb_name = "dp-alb-fixed"
    lb_arn, tg_arn, l_arn = _alb_http_target_setup(elbv2, lb_name, "127.0.0.1", port)
    try:
        resp = _req.urlopen(_req.Request(f"{_endpoint}/_alb/{lb_name}/", method="GET"))
        received = resp.read()
        assert resp.status == 200
        assert resp.headers.get("Content-Type") == "application/octet-stream"
        assert resp.headers.get("Content-Length") == str(len(body))
        assert resp.headers.get("Transfer-Encoding") is None
        assert len(received) == len(body)
        assert received == body
    finally:
        _alb_http_target_teardown(elbv2, lb_arn, tg_arn, l_arn)
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.serial
def test_elbv2_dataplane_forward_instance_target_hostname(elbv2):
    """Instance targets resolve the Id as a hostname (no EC2 metadata in an
    emulator), so localhost works as an instance Id too."""
    import urllib.request as _req

    lb_name = "dp-alb-inst"
    lb_arn, tg_arn, l_arn = _alb_http_target_setup(
        elbv2, lb_name, "localhost", _SERVER_GATEWAY_PORT, target_type="instance"
    )
    try:
        url = f"{_endpoint}/_alb/{lb_name}/_ministack/health"
        resp = _req.urlopen(_req.Request(url, method="GET"))
        assert resp.status == 200
    finally:
        _alb_http_target_teardown(elbv2, lb_arn, tg_arn, l_arn)


@pytest.mark.serial
def test_elbv2_dataplane_ip_target_unreachable_returns_502(elbv2):
    """Connection failures surface as 502 Bad Gateway, like a real ALB."""
    import urllib.error as _err
    import urllib.request as _req

    lb_name = "dp-alb-dead"
    lb_arn, tg_arn, l_arn = _alb_http_target_setup(elbv2, lb_name, "127.0.0.1", 59999)
    try:
        url = f"{_endpoint}/_alb/{lb_name}/anything"
        with pytest.raises(_err.HTTPError) as exc_info:
            _req.urlopen(_req.Request(url, method="GET"))
        assert exc_info.value.code == 502
        body = json.loads(exc_info.value.read())
        assert "connect error" in body["message"]
    finally:
        _alb_http_target_teardown(elbv2, lb_arn, tg_arn, l_arn)


def test_alb_load_balancers_are_region_isolated(elbv2):
    """A load balancer created in us-east-1 must not be visible from us-west-2."""
    import boto3

    elbv2_west = boto3.client(
        "elbv2", endpoint_url=_endpoint,
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name="us-west-2",
    )
    name = f"iso-{_uuid_mod.uuid4().hex[:8]}"
    arn = elbv2.create_load_balancer(
        Name=name, Type="application", Scheme="internet-facing",
    )["LoadBalancers"][0]["LoadBalancerArn"]
    assert ":us-east-1:" in arn
    try:
        east = [lb["LoadBalancerArn"] for lb in elbv2.describe_load_balancers()["LoadBalancers"]]
        west = [lb["LoadBalancerArn"] for lb in elbv2_west.describe_load_balancers()["LoadBalancers"]]
        assert arn in east
        assert arn not in west
    finally:
        elbv2.delete_load_balancer(LoadBalancerArn=arn)


def test_alb_rule_condition_accepts_typed_config_shape(elbv2):
    """A rule condition's values may arrive as PathPatternConfig, not a flat Values list.

    The Terraform AWS provider sends the typed form. Parsing only the legacy flat
    list records an empty condition, so the rule matches nothing and every request
    falls through to the listener's default action.
    """
    vpc = "vpc-typedcond"
    lb = elbv2.create_load_balancer(Name="typedcond-lb", Subnets=["subnet-1"])["LoadBalancers"][0]
    tg = elbv2.create_target_group(Name="typedcond-tg", Port=80, Protocol="HTTP", VpcId=vpc)["TargetGroups"][0]
    listener = elbv2.create_listener(
        LoadBalancerArn=lb["LoadBalancerArn"], Protocol="HTTP", Port=80,
        DefaultActions=[{
            "Type": "fixed-response",
            "FixedResponseConfig": {"StatusCode": "200", "ContentType": "text/plain", "MessageBody": "OK"},
        }],
    )["Listeners"][0]

    # botocore serialises this as Conditions.member.1.PathPatternConfig.Values.member.1
    elbv2.create_rule(
        ListenerArn=listener["ListenerArn"],
        Priority=1,
        Conditions=[{"Field": "path-pattern", "PathPatternConfig": {"Values": ["/q"]}}],
        Actions=[{"Type": "forward", "TargetGroupArn": tg["TargetGroupArn"]}],
    )

    rules = elbv2.describe_rules(ListenerArn=listener["ListenerArn"])["Rules"]
    rule = next(r for r in rules if r.get("Priority") == "1")
    cond = rule["Conditions"][0]
    assert cond["Field"] == "path-pattern"
    assert cond["Values"] == ["/q"], f"condition values were dropped: {cond}"
    # AWS answers with both shapes. A client reading only the typed config — the
    # Terraform provider does — sees an empty condition without this, and plans a
    # change on every run to put the values back.
    assert cond["PathPatternConfig"]["Values"] == ["/q"], f"typed config not returned: {cond}"


def test_alb_rule_condition_still_accepts_flat_values(elbv2):
    """The legacy flat Values list must keep working."""
    vpc = "vpc-flatcond"
    lb = elbv2.create_load_balancer(Name="flatcond-lb", Subnets=["subnet-1"])["LoadBalancers"][0]
    tg = elbv2.create_target_group(Name="flatcond-tg", Port=80, Protocol="HTTP", VpcId=vpc)["TargetGroups"][0]
    listener = elbv2.create_listener(
        LoadBalancerArn=lb["LoadBalancerArn"], Protocol="HTTP", Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg["TargetGroupArn"]}],
    )["Listeners"][0]

    elbv2.create_rule(
        ListenerArn=listener["ListenerArn"],
        Priority=5,
        Conditions=[{"Field": "path-pattern", "Values": ["/legacy"]}],
        Actions=[{"Type": "forward", "TargetGroupArn": tg["TargetGroupArn"]}],
    )

    rules = elbv2.describe_rules(ListenerArn=listener["ListenerArn"])["Rules"]
    rule = next(r for r in rules if r.get("Priority") == "5")
    assert rule["Conditions"][0]["Values"] == ["/legacy"]


def test_alb_rule_conditions_round_trip_in_both_shapes(elbv2):
    """host-header and http-request-method must also come back in their typed config."""
    vpc = "vpc-bothshapes"
    lb = elbv2.create_load_balancer(Name="bothshapes-lb", Subnets=["subnet-1"])["LoadBalancers"][0]
    tg = elbv2.create_target_group(Name="bothshapes-tg", Port=80, Protocol="HTTP", VpcId=vpc)["TargetGroups"][0]
    listener = elbv2.create_listener(
        LoadBalancerArn=lb["LoadBalancerArn"], Protocol="HTTP", Port=80,
        DefaultActions=[{"Type": "forward", "TargetGroupArn": tg["TargetGroupArn"]}],
    )["Listeners"][0]

    cases = [
        (20, {"Field": "host-header", "HostHeaderConfig": {"Values": ["api.example.com"]}},
         "HostHeaderConfig", ["api.example.com"]),
        (21, {"Field": "http-request-method", "HttpRequestMethodConfig": {"Values": ["POST"]}},
         "HttpRequestMethodConfig", ["POST"]),
    ]
    for priority, condition, config_key, expected in cases:
        elbv2.create_rule(
            ListenerArn=listener["ListenerArn"], Priority=priority, Conditions=[condition],
            Actions=[{"Type": "forward", "TargetGroupArn": tg["TargetGroupArn"]}],
        )

    rules = {r["Priority"]: r for r in elbv2.describe_rules(ListenerArn=listener["ListenerArn"])["Rules"]}
    for priority, _cond, config_key, expected in cases:
        cond = rules[str(priority)]["Conditions"][0]
        assert cond["Values"] == expected
        assert cond[config_key]["Values"] == expected, f"{config_key} not returned: {cond}"


# ===========================================================================
# authenticate-oidc listener action
# ===========================================================================


class _NoRedirect(_req.HTTPRedirectHandler):
    """Keep redirects visible: the 302 is the assertion, and its Location
    points at an identity provider that does not exist."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_oidc_opener = _req.build_opener(_NoRedirect)


def _oidc_get(path, host, headers=None):
    req = _req.Request(f"{_endpoint}{path}", method="GET")
    req.add_header("Host", f"{host}.alb.localhost:{_EXECUTE_PORT}")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        resp = _oidc_opener.open(req)
        return resp.status, dict(resp.headers), resp.read()
    except HTTPError as e:
        return e.code, dict(e.headers), e.read()



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




def test_unauthenticated_request_redirects_to_the_identity_provider(elbv2, lam):
    name = "oidc-dp-redir"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name)
    try:
        status, headers, _ = _oidc_get("/private/thing", name)
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
        status, _, body = _oidc_get("/public", name)
        assert status == 200
        # The target runs, but with no identity attached.
        assert json.loads(body)["identity"] is None
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_on_unauthenticated_request_deny_refuses(elbv2, lam):
    name = "oidc-dp-deny"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name, OnUnauthenticatedRequest="deny")
    try:
        status, _, _ = _oidc_get("/private", name)
        assert status == 401
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_a_valid_session_reaches_the_target_with_identity_attached(elbv2, lam):
    name = "oidc-dp-session"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name)
    try:
        from ministack.services import alb

        session = {"claims": {"sub": "user-42", "email": "u@example.com"},
                   "access_token": "at",
                   "exp": int(time.time()) + 600}
        cookies = alb._oidc_session_cookies("AWSELBAuthSessionCookie", session, 600, False)
        jar = "; ".join(c.split(";", 1)[0] for c in cookies)

        status, _, body = _oidc_get("/private", name, {"Cookie": jar})
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

        status, headers, _ = _oidc_get("/private", name, {"Cookie": jar})
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
        status, _, _ = _oidc_get("/oauth2/idpresponse?code=abc&state=never-issued", name)
        assert status == 401
        status, _, _ = _oidc_get("/oauth2/idpresponse", name)
        assert status == 401
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_a_custom_session_cookie_name_is_honoured(elbv2, lam):
    name = "oidc-dp-cookiename"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name, SessionCookieName="MySession")
    try:
        from ministack.services import alb

        session = {"claims": {"sub": "user-9"}, "exp": int(time.time()) + 600}
        cookies = alb._oidc_session_cookies("MySession", session, 600, False)
        jar = "; ".join(c.split(";", 1)[0] for c in cookies)

        status, _, body = _oidc_get("/private", name, {"Cookie": jar})
        assert status == 200
        assert json.loads(body)["identity"] == "user-9"

        # The default name must not be accepted when another was configured.
        wrong = alb._oidc_session_cookies("AWSELBAuthSessionCookie", session, 600, False)
        wrong_jar = "; ".join(c.split(";", 1)[0] for c in wrong)
        status, _, _ = _oidc_get("/private", name, {"Cookie": wrong_jar})
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
        status, _, body = _oidc_get("/private", name)
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
        status, _, body = _oidc_get("/anything", name)
        assert status == 200 and body == b"target"

        elbv2.modify_listener(ListenerArn=l_arn, DefaultActions=[{
            "Type": "fixed-response",
            "FixedResponseConfig": {"StatusCode": "418", "ContentType": "text/plain",
                                    "MessageBody": "changed"},
        }])

        status, _, body = _oidc_get("/anything", name)
        assert status == 418, "ModifyListener did not reach the data plane"
        assert body == b"changed"
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_client_supplied_identity_headers_are_stripped_in_allow_mode(elbv2, lam):
    """AWS owns the X-Amzn-Oidc-* headers: a client cannot forge an identity by
    sending them directly, even when OnUnauthenticatedRequest=allow forwards
    the request without authentication."""
    name = "oidc-dp-forge"
    lb_arn, tg_arn, l_arn = _alb_with_oidc(elbv2, lam, name, OnUnauthenticatedRequest="allow")
    try:
        status, _, body = _oidc_get("/public", name, {
            "x-amzn-oidc-identity": "forged-admin",
            "x-amzn-oidc-data": "forged.jwt.blob",
        })
        assert status == 200
        payload = json.loads(body)
        assert payload["identity"] is None
        assert payload["data"] is False
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


def test_oidc_data_is_an_alb_format_claims_jwt(elbv2, lam):
    """x-amzn-oidc-data carries the user claims in AWS's JWT shape — kid,
    signer (the load balancer ARN), iss, client and exp in the header — never
    the provider's ID token, which AWS does not pass to the backend."""
    import base64

    name = "oidc-dp-jwt"
    fn_code = (
        "import json\n"
        "def handler(event, context):\n"
        "    h = {k.lower(): v for k, v in (event.get('headers') or {}).items()}\n"
        "    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},\n"
        "            'body': json.dumps({'data': h.get('x-amzn-oidc-data')})}\n"
    )
    lb_arn, tg_arn, l_arn, _ = _alb_setup(elbv2, lam, name, f"{name}-fn", fn_code)
    try:
        elbv2.modify_listener(
            ListenerArn=l_arn,
            DefaultActions=[
                _oidc_action(order=1),
                {"Type": "forward", "Order": 2, "TargetGroupArn": tg_arn},
            ],
        )
        from ministack.services import alb

        session = {"claims": {"sub": "user-77", "email": "x@example.com"},
                   "access_token": "at", "exp": int(time.time()) + 600}
        cookies = alb._oidc_session_cookies("AWSELBAuthSessionCookie", session, 600, False)
        jar = "; ".join(c.split(";", 1)[0] for c in cookies)

        status, _, body = _oidc_get("/private", name, {"Cookie": jar})
        assert status == 200
        token = json.loads(body)["data"]
        head, payload, sig = token.split(".")
        # ALB's documented deviation from standard JWT: every segment keeps
        # its base64 padding. Strict JWT parsers fail on real ALB tokens
        # because of it, so the emulator must reproduce it.
        for segment in (head, payload, sig):
            assert len(segment) % 4 == 0
        header = json.loads(base64.urlsafe_b64decode(head))
        claims = json.loads(base64.urlsafe_b64decode(payload))
        assert header["alg"] == "ES256"
        assert header["signer"] == lb_arn
        assert header["kid"]
        assert header["iss"] == "https://idp.example.com"
        assert header["client"] == "client-abc"
        assert claims == {"sub": "user-77", "email": "x@example.com"}
    finally:
        _alb_teardown(elbv2, lam, lb_arn, tg_arn, l_arn, f"{name}-fn")


# ------------------------------------------------------- the fixed behaviours

def _run_auth(action_cfg, path, query=None, cookie=None):
    """Drive _authenticate_oidc in-process, as dispatch would."""
    import asyncio

    from ministack.services import alb

    headers = {"host": "lb.alb.localhost:4566"}
    if cookie:
        headers["cookie"] = cookie
    action = {"Type": "authenticate-oidc", "AuthenticateOidcConfig": action_cfg}
    return asyncio.run(alb._authenticate_oidc(
        action, "GET", path, headers, b"", query or {}, 80,
        lb_arn="arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/lb/abc",
    )), headers


def test_callback_claims_come_from_the_userinfo_endpoint(monkeypatch):
    """The session stores the UserInfo claims, as AWS documents — the ID token
    (whose own claims differ here on purpose) is never stored or forwarded."""
    from ministack.services import alb

    id_payload = json.dumps({"sub": "from-id-token"}).encode()
    import base64 as _b64
    id_token = "h." + _b64.urlsafe_b64encode(id_payload).decode().rstrip("=") + ".s"
    monkeypatch.setattr(alb, "_oidc_post_form", lambda url, form, timeout=10.0: (
        200, {"access_token": "at-1", "id_token": id_token}))
    monkeypatch.setattr(alb, "_oidc_get_userinfo", lambda url, token, timeout=10.0: (
        {"sub": "from-userinfo", "email": "u@example.com"}))

    alb._oidc_pending["st-1"] = {"url": "http://lb/orig", "redirect_uri": "http://lb/oauth2/idpresponse",
                                 "created": time.time()}
    (status, out_headers, _), _ = _run_auth(
        {"TokenEndpoint": "https://idp/t", "UserInfoEndpoint": "https://idp/u",
         "ClientId": "c", "ClientSecret": "s"},
        "/oauth2/idpresponse", {"code": ["abc"], "state": ["st-1"]})
    assert status == 302

    jar = {}
    for cookie in out_headers["Set-Cookie"]:
        name, _, rest = cookie.partition("=")
        jar[name] = rest.split(";", 1)[0]
    session = alb._oidc_read_session(jar, "AWSELBAuthSessionCookie")
    assert session["claims"] == {"sub": "from-userinfo", "email": "u@example.com"}
    assert "id_token" not in session, "AWS never passes the ID token to the backend"


def test_callback_falls_back_to_id_token_claims_when_userinfo_fails(monkeypatch):
    import base64 as _b64

    from ministack.services import alb
    payload = _b64.urlsafe_b64encode(json.dumps({"sub": "fallback-sub"}).encode()).decode().rstrip("=")
    monkeypatch.setattr(alb, "_oidc_post_form", lambda url, form, timeout=10.0: (
        200, {"access_token": "at-2", "id_token": f"h.{payload}.s"}))
    monkeypatch.setattr(alb, "_oidc_get_userinfo", lambda url, token, timeout=10.0: None)

    alb._oidc_pending["st-2"] = {"url": "http://lb/orig", "redirect_uri": "http://lb/oauth2/idpresponse",
                                 "created": time.time()}
    (status, out_headers, _), _ = _run_auth(
        {"TokenEndpoint": "https://idp/t", "UserInfoEndpoint": "https://idp/u", "ClientId": "c"},
        "/oauth2/idpresponse", {"code": ["abc"], "state": ["st-2"]})
    assert status == 302
    jar = {c.partition("=")[0]: c.partition("=")[2].split(";", 1)[0]
           for c in out_headers["Set-Cookie"]}
    assert alb._oidc_read_session(jar, "AWSELBAuthSessionCookie")["claims"]["sub"] == "fallback-sub"


def test_abandoned_login_attempts_are_pruned_after_the_15_minute_bound():
    from ministack.services import alb

    alb._oidc_pending["stale-1"] = {"url": "u", "redirect_uri": "r", "created": time.time() - 901}
    alb._oidc_pending["fresh-1"] = {"url": "u", "redirect_uri": "r", "created": time.time()}
    (status, _, _), _ = _run_auth(
        {"AuthorizationEndpoint": "https://idp/a", "ClientId": "c"}, "/private")
    assert status == 302
    assert "stale-1" not in alb._oidc_pending, "abandoned attempt survived the prune"
    assert "fresh-1" in alb._oidc_pending, "a live attempt must not be pruned"
