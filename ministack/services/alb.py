"""
ALB / ELBv2 (Elastic Load Balancing v2) Service Emulator.
Query API (Action=...) with XML responses. In-memory only.

Supports:
  Load Balancers:       CreateLoadBalancer, DescribeLoadBalancers, DeleteLoadBalancer,
                        ModifyLoadBalancerAttributes, DescribeLoadBalancerAttributes
  Target Groups:        CreateTargetGroup, DescribeTargetGroups, ModifyTargetGroup,
                        DeleteTargetGroup, DescribeTargetGroupAttributes,
                        ModifyTargetGroupAttributes
  Listeners:            CreateListener, DescribeListeners, ModifyListener, DeleteListener,
                        DescribeListenerAttributes, ModifyListenerAttributes
  Rules:                CreateRule, DescribeRules, ModifyRule, DeleteRule,
                        SetRulePriorities
  Target Registration:  RegisterTargets, DeregisterTargets, DescribeTargetHealth
  Tags:                 AddTags, RemoveTags, DescribeTags
"""

import base64
import copy
import fnmatch
import json
import logging
import os
import random
import string
import time
from urllib.parse import parse_qs

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import AccountScopedDict, get_account_id, new_uuid

logger = logging.getLogger("alb")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
NS = "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/"

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
_lbs = AccountScopedDict()        # lb_arn   -> LB record
_tgs = AccountScopedDict()        # tg_arn   -> TG record
_listeners = AccountScopedDict()  # l_arn    -> Listener record
_rules = AccountScopedDict()      # r_arn    -> Rule record
_targets = AccountScopedDict()    # tg_arn   -> [target dict]
_tags = AccountScopedDict()       # res_arn  -> [{Key, Value}]
_lb_attrs = AccountScopedDict()   # lb_arn   -> [{Key, Value}]
_tg_attrs = AccountScopedDict()   # tg_arn   -> [{Key, Value}]
_listener_attrs = AccountScopedDict()  # l_arn -> [{Key, Value}]


def get_state():
    return copy.deepcopy({
        "_lbs": _lbs,
        "_tgs": _tgs,
        "_listeners": _listeners,
        "_rules": _rules,
        "_targets": _targets,
        "_tags": _tags,
        "_lb_attrs": _lb_attrs,
        "_tg_attrs": _tg_attrs,
        "_listener_attrs": _listener_attrs,
    })


def restore_state(data):
    _lbs.update(data.get("_lbs", {}))
    _tgs.update(data.get("_tgs", {}))
    _listeners.update(data.get("_listeners", {}))
    _rules.update(data.get("_rules", {}))
    _targets.update(data.get("_targets", {}))
    _tags.update(data.get("_tags", {}))
    _lb_attrs.update(data.get("_lb_attrs", {}))
    _tg_attrs.update(data.get("_tg_attrs", {}))
    _listener_attrs.update(data.get("_listener_attrs", {}))


_restored = load_state("alb")
if _restored:
    restore_state(_restored)

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _p(params, key, default=""):
    val = params.get(key, [default])
    return (val[0] if val else default) if isinstance(val, list) else val


def _parse_member_list(params, prefix):
    items, i = [], 1
    while True:
        v = _p(params, f"{prefix}.member.{i}")
        if not v:
            break
        items.append(v)
        i += 1
    return items


def _parse_tags(params):
    tags, i = [], 1
    while True:
        k = _p(params, f"Tags.member.{i}.Key")
        if not k:
            break
        tags.append({"Key": k, "Value": _p(params, f"Tags.member.{i}.Value")})
        i += 1
    return tags


def _parse_actions(params, prefix="DefaultActions"):
    actions, i = [], 1
    while True:
        t = _p(params, f"{prefix}.member.{i}.Type")
        if not t:
            break
        action = {"Type": t, "Order": int(_p(params, f"{prefix}.member.{i}.Order", str(i)))}
        tg = _p(params, f"{prefix}.member.{i}.TargetGroupArn")
        if tg:
            action["TargetGroupArn"] = tg
        rc_code = _p(params, f"{prefix}.member.{i}.RedirectConfig.StatusCode")
        if rc_code:
            action["RedirectConfig"] = {
                "Protocol": _p(params, f"{prefix}.member.{i}.RedirectConfig.Protocol", "#{protocol}"),
                "Port": _p(params, f"{prefix}.member.{i}.RedirectConfig.Port", "#{port}"),
                "Host": _p(params, f"{prefix}.member.{i}.RedirectConfig.Host", "#{host}"),
                "Path": _p(params, f"{prefix}.member.{i}.RedirectConfig.Path", "/#{path}"),
                "StatusCode": rc_code,
            }
        fr_code = _p(params, f"{prefix}.member.{i}.FixedResponseConfig.StatusCode")
        if fr_code:
            action["FixedResponseConfig"] = {
                "StatusCode": fr_code,
                "ContentType": _p(params, f"{prefix}.member.{i}.FixedResponseConfig.ContentType", "text/plain"),
                "MessageBody": _p(params, f"{prefix}.member.{i}.FixedResponseConfig.MessageBody", ""),
            }
        actions.append(action)
        i += 1
    return actions


def _parse_conditions(params, prefix="Conditions"):
    conditions, i = [], 1
    while True:
        field = _p(params, f"{prefix}.member.{i}.Field")
        if not field:
            break
        values, j = [], 1
        while True:
            v = _p(params, f"{prefix}.member.{i}.Values.member.{j}")
            if not v:
                break
            values.append(v)
            j += 1
        conditions.append({"Field": field, "Values": values})
        i += 1
    return conditions


def _parse_targets_param(params, prefix="Targets"):
    targets, i = [], 1
    while True:
        tid = _p(params, f"{prefix}.member.{i}.Id")
        if not tid:
            break
        t = {"Id": tid}
        port = _p(params, f"{prefix}.member.{i}.Port")
        if port:
            t["Port"] = int(port)
        targets.append(t)
        i += 1
    return targets


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def _short_id():
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=16))

# ---------------------------------------------------------------------------
# XML builders
# ---------------------------------------------------------------------------

def _xml(status, action, inner):
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<{action}Response xmlns="{NS}">'
        f'<{action}Result>{inner}</{action}Result>'
        f'<ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>'
        f'</{action}Response>'
    ).encode("utf-8")
    return status, {"Content-Type": "text/xml"}, body


def _empty(action):
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<{action}Response xmlns="{NS}">'
        f'<{action}Result/>'
        f'<ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>'
        f'</{action}Response>'
    ).encode("utf-8")
    return 200, {"Content-Type": "text/xml"}, body


def _error(code, message, status=400):
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<ErrorResponse xmlns="{NS}">'
        f'<Error><Code>{code}</Code><Message>{message}</Message></Error>'
        f'<RequestId>{new_uuid()}</RequestId>'
        f'</ErrorResponse>'
    ).encode("utf-8")
    return status, {"Content-Type": "text/xml"}, body


def _attrs_xml(attrs):
    return "".join(
        f"<member><Key>{a['Key']}</Key><Value>{a['Value']}</Value></member>"
        for a in attrs
    )

# ---------------------------------------------------------------------------
# XML serialisers for each resource type
# ---------------------------------------------------------------------------

def _lb_xml(lb):
    azs = "".join(
        f"<member><ZoneName>{REGION}a</ZoneName><SubnetId>{s}</SubnetId>"
        f"<LoadBalancerAddresses/></member>"
        for s in lb.get("Subnets", [])
    )
    sgs = "".join(f"<member>{sg}</member>" for sg in lb.get("SecurityGroups", []))
    return (
        f"<member>"
        f"<LoadBalancerArn>{lb['LoadBalancerArn']}</LoadBalancerArn>"
        f"<LoadBalancerName>{lb['LoadBalancerName']}</LoadBalancerName>"
        f"<DNSName>{lb['DNSName']}</DNSName>"
        f"<CanonicalHostedZoneId>Z35SXDOTRQ7X7K</CanonicalHostedZoneId>"
        f"<CreatedTime>{lb['CreatedTime']}</CreatedTime>"
        f"<Scheme>{lb['Scheme']}</Scheme>"
        f"<VpcId>{lb.get('VpcId','')}</VpcId>"
        f"<State><Code>{lb['State']}</Code></State>"
        f"<Type>{lb['Type']}</Type>"
        f"<AvailabilityZones>{azs}</AvailabilityZones>"
        f"<SecurityGroups>{sgs}</SecurityGroups>"
        f"<IpAddressType>{lb.get('IpAddressType','ipv4')}</IpAddressType>"
        f"</member>"
    )


def _tg_xml(tg):
    lb_arns = "".join(f"<member>{a}</member>" for a in tg.get("LoadBalancerArns", []))
    return (
        f"<member>"
        f"<TargetGroupArn>{tg['TargetGroupArn']}</TargetGroupArn>"
        f"<TargetGroupName>{tg['TargetGroupName']}</TargetGroupName>"
        f"<Protocol>{tg.get('Protocol','HTTP')}</Protocol>"
        f"<Port>{tg.get('Port',80)}</Port>"
        f"<VpcId>{tg.get('VpcId','')}</VpcId>"
        f"<HealthCheckProtocol>{tg.get('HealthCheckProtocol','HTTP')}</HealthCheckProtocol>"
        f"<HealthCheckPort>{tg.get('HealthCheckPort','traffic-port')}</HealthCheckPort>"
        f"<HealthCheckEnabled>{str(tg.get('HealthCheckEnabled',True)).lower()}</HealthCheckEnabled>"
        f"<HealthCheckPath>{tg.get('HealthCheckPath','/')}</HealthCheckPath>"
        f"<HealthCheckIntervalSeconds>{tg.get('HealthCheckIntervalSeconds',30)}</HealthCheckIntervalSeconds>"
        f"<HealthCheckTimeoutSeconds>{tg.get('HealthCheckTimeoutSeconds',5)}</HealthCheckTimeoutSeconds>"
        f"<HealthyThresholdCount>{tg.get('HealthyThresholdCount',5)}</HealthyThresholdCount>"
        f"<UnhealthyThresholdCount>{tg.get('UnhealthyThresholdCount',2)}</UnhealthyThresholdCount>"
        f"<Matcher><HttpCode>{tg.get('Matcher',{}).get('HttpCode','200')}</HttpCode></Matcher>"
        f"<LoadBalancerArns>{lb_arns}</LoadBalancerArns>"
        f"<TargetType>{tg.get('TargetType','instance')}</TargetType>"
        f"</member>"
    )


def _action_xml(a):
    inner = f"<Type>{a['Type']}</Type><Order>{a.get('Order',1)}</Order>"
    if "TargetGroupArn" in a:
        inner += f"<TargetGroupArn>{a['TargetGroupArn']}</TargetGroupArn>"
    if "RedirectConfig" in a:
        rc = a["RedirectConfig"]
        inner += (
            f"<RedirectConfig>"
            f"<Protocol>{rc.get('Protocol','#{protocol}')}</Protocol>"
            f"<Port>{rc.get('Port','#{port}')}</Port>"
            f"<Host>{rc.get('Host','#{host}')}</Host>"
            f"<Path>{rc.get('Path','/#{path}')}</Path>"
            f"<StatusCode>{rc.get('StatusCode','HTTP_301')}</StatusCode>"
            f"</RedirectConfig>"
        )
    if "FixedResponseConfig" in a:
        frc = a["FixedResponseConfig"]
        inner += (
            f"<FixedResponseConfig>"
            f"<StatusCode>{frc.get('StatusCode','200')}</StatusCode>"
            f"<ContentType>{frc.get('ContentType','text/plain')}</ContentType>"
            f"<MessageBody>{frc.get('MessageBody','')}</MessageBody>"
            f"</FixedResponseConfig>"
        )
    return f"<member>{inner}</member>"


def _listener_xml(l):
    acts = "".join(_action_xml(a) for a in l.get("DefaultActions", []))
    return (
        f"<member>"
        f"<ListenerArn>{l['ListenerArn']}</ListenerArn>"
        f"<LoadBalancerArn>{l['LoadBalancerArn']}</LoadBalancerArn>"
        f"<Port>{l.get('Port',80)}</Port>"
        f"<Protocol>{l.get('Protocol','HTTP')}</Protocol>"
        f"<DefaultActions>{acts}</DefaultActions>"
        f"</member>"
    )


def _rule_xml(r):
    conds = "".join(
        f"<member><Field>{c['Field']}</Field>"
        f"<Values>{''.join(f'<member>{v}</member>' for v in c.get('Values',[]))}</Values>"
        f"</member>"
        for c in r.get("Conditions", [])
    )
    acts = "".join(_action_xml(a) for a in r.get("Actions", []))
    return (
        f"<member>"
        f"<RuleArn>{r['RuleArn']}</RuleArn>"
        f"<Priority>{r['Priority']}</Priority>"
        f"<Conditions>{conds}</Conditions>"
        f"<Actions>{acts}</Actions>"
        f"<IsDefault>{str(r.get('IsDefault',False)).lower()}</IsDefault>"
        f"</member>"
    )

# ---------------------------------------------------------------------------
# Load Balancer handlers
# ---------------------------------------------------------------------------

def _create_lb(params):
    name = _p(params, "Name")
    if not name:
        return _error("ValidationError", "Name is required")
    for lb in _lbs.values():
        if lb["LoadBalancerName"] == name:
            return _error("DuplicateLoadBalancerName",
                          f"A load balancer with name '{name}' already exists.")
    lid = _short_id()
    arn = f"arn:aws:elasticloadbalancing:{REGION}:{get_account_id()}:loadbalancer/app/{name}/{lid}"
    lb = {
        "LoadBalancerArn": arn,
        "LoadBalancerName": name,
        "DNSName": f"{name}-{lid[:8]}.{REGION}.elb.amazonaws.com",
        "Scheme": _p(params, "Scheme", "internet-facing"),
        "VpcId": _p(params, "VpcId", "vpc-00000001"),
        "State": "active",
        "Type": _p(params, "Type", "application"),
        "Subnets": _parse_member_list(params, "Subnets"),
        "SecurityGroups": _parse_member_list(params, "SecurityGroups"),
        "IpAddressType": _p(params, "IpAddressType", "ipv4"),
        "CreatedTime": _now_iso(),
    }
    _lbs[arn] = lb
    _tags[arn] = _parse_tags(params)
    _lb_attrs[arn] = [
        {"Key": "access_logs.s3.enabled", "Value": "false"},
        {"Key": "deletion_protection.enabled", "Value": "false"},
        {"Key": "idle_timeout.timeout_seconds", "Value": "60"},
    ]
    return _xml(200, "CreateLoadBalancer", f"<LoadBalancers>{_lb_xml(lb)}</LoadBalancers>")


def _describe_lbs(params):
    arn_filter = _parse_member_list(params, "LoadBalancerArns")
    name_filter = _parse_member_list(params, "Names")
    results = list(_lbs.values())
    if arn_filter:
        results = [lb for lb in results if lb["LoadBalancerArn"] in arn_filter]
        if not results:
            return _error("LoadBalancerNotFound", "One or more load balancers not found", 400)
    if name_filter:
        results = [lb for lb in results if lb["LoadBalancerName"] in name_filter]
        if not results:
            return _error("LoadBalancerNotFound", "One or more load balancers not found", 400)
    return _xml(200, "DescribeLoadBalancers",
                f"<LoadBalancers>{''.join(_lb_xml(lb) for lb in results)}</LoadBalancers>")


def _delete_lb(params):
    arn = _p(params, "LoadBalancerArn")
    _lbs.pop(arn, None)
    _lb_attrs.pop(arn, None)
    _tags.pop(arn, None)
    return _empty("DeleteLoadBalancer")


def _describe_lb_attrs(params):
    arn = _p(params, "LoadBalancerArn")
    if arn not in _lbs:
        return _error("LoadBalancerNotFound", f"Load balancer '{arn}' not found.")
    return _xml(200, "DescribeLoadBalancerAttributes",
                f"<Attributes>{_attrs_xml(_lb_attrs.get(arn,[]))}</Attributes>")


def _modify_lb_attrs(params):
    arn = _p(params, "LoadBalancerArn")
    if arn not in _lbs:
        return _error("LoadBalancerNotFound", f"Load balancer '{arn}' not found.")
    attrs = _lb_attrs.setdefault(arn, [])
    idx = {a["Key"]: i for i, a in enumerate(attrs)}
    i = 1
    while True:
        key = _p(params, f"Attributes.member.{i}.Key")
        if not key:
            break
        val = _p(params, f"Attributes.member.{i}.Value")
        if key in idx:
            attrs[idx[key]]["Value"] = val
        else:
            attrs.append({"Key": key, "Value": val})
            idx[key] = len(attrs) - 1
        i += 1
    return _xml(200, "ModifyLoadBalancerAttributes",
                f"<Attributes>{_attrs_xml(attrs)}</Attributes>")



# ---------------------------------------------------------------------------
# Target Group handlers
# ---------------------------------------------------------------------------

def _create_tg(params):
    name = _p(params, "Name")
    if not name:
        return _error("ValidationError", "Name is required")
    for tg in _tgs.values():
        if tg["TargetGroupName"] == name:
            return _error("DuplicateTargetGroupName",
                          f"A target group with name '{name}' already exists.")
    tid = _short_id()
    arn = f"arn:aws:elasticloadbalancing:{REGION}:{get_account_id()}:targetgroup/{name}/{tid}"
    tg = {
        "TargetGroupArn": arn,
        "TargetGroupName": name,
        "Protocol": _p(params, "Protocol", "HTTP"),
        "Port": int(_p(params, "Port", "80") or 80),
        "VpcId": _p(params, "VpcId", ""),
        "HealthCheckProtocol": _p(params, "HealthCheckProtocol", "HTTP"),
        "HealthCheckPort": _p(params, "HealthCheckPort", "traffic-port"),
        "HealthCheckEnabled": _p(params, "HealthCheckEnabled", "true").lower() == "true",
        "HealthCheckPath": _p(params, "HealthCheckPath", "/"),
        "HealthCheckIntervalSeconds": int(_p(params, "HealthCheckIntervalSeconds", "30") or 30),
        "HealthCheckTimeoutSeconds": int(_p(params, "HealthCheckTimeoutSeconds", "5") or 5),
        "HealthyThresholdCount": int(_p(params, "HealthyThresholdCount", "5") or 5),
        "UnhealthyThresholdCount": int(_p(params, "UnhealthyThresholdCount", "2") or 2),
        "Matcher": {"HttpCode": _p(params, "Matcher.HttpCode", "200")},
        "LoadBalancerArns": [],
        "TargetType": _p(params, "TargetType", "instance"),
    }
    _tgs[arn] = tg
    _targets[arn] = []
    _tags[arn] = _parse_tags(params)
    _tg_attrs[arn] = [
        {"Key": "deregistration_delay.timeout_seconds", "Value": "300"},
        {"Key": "stickiness.enabled", "Value": "false"},
        {"Key": "stickiness.type", "Value": "lb_cookie"},
    ]
    return _xml(200, "CreateTargetGroup", f"<TargetGroups>{_tg_xml(tg)}</TargetGroups>")


def _describe_tgs(params):
    arn_filter = _parse_member_list(params, "TargetGroupArns")
    name_filter = _parse_member_list(params, "Names")
    lb_arn = _p(params, "LoadBalancerArn")
    results = list(_tgs.values())
    if arn_filter:
        results = [tg for tg in results if tg["TargetGroupArn"] in arn_filter]
        if not results:
            return _error("TargetGroupNotFound", "One or more target groups not found", 400)
    if name_filter:
        results = [tg for tg in results if tg["TargetGroupName"] in name_filter]
    if lb_arn:
        results = [tg for tg in results if lb_arn in tg.get("LoadBalancerArns", [])]
    return _xml(200, "DescribeTargetGroups",
                f"<TargetGroups>{''.join(_tg_xml(tg) for tg in results)}</TargetGroups>")


def _modify_tg(params):
    arn = _p(params, "TargetGroupArn")
    tg = _tgs.get(arn)
    if not tg:
        return _error("TargetGroupNotFound", f"Target group '{arn}' not found.")
    for field, param in [("HealthCheckProtocol", "HealthCheckProtocol"),
                         ("HealthCheckPort", "HealthCheckPort"),
                         ("HealthCheckPath", "HealthCheckPath")]:
        v = _p(params, param)
        if v:
            tg[field] = v
    for field, param, cast in [
        ("HealthCheckEnabled", "HealthCheckEnabled", lambda v: v.lower() == "true"),
        ("HealthCheckIntervalSeconds", "HealthCheckIntervalSeconds", int),
        ("HealthCheckTimeoutSeconds", "HealthCheckTimeoutSeconds", int),
        ("HealthyThresholdCount", "HealthyThresholdCount", int),
        ("UnhealthyThresholdCount", "UnhealthyThresholdCount", int),
    ]:
        v = _p(params, param)
        if v:
            tg[field] = cast(v)
    http_code = _p(params, "Matcher.HttpCode")
    if http_code:
        tg["Matcher"]["HttpCode"] = http_code
    return _xml(200, "ModifyTargetGroup", f"<TargetGroups>{_tg_xml(tg)}</TargetGroups>")


def _delete_tg(params):
    arn = _p(params, "TargetGroupArn")
    if arn not in _tgs:
        return _error("TargetGroupNotFound", f"Target group '{arn}' not found", 400)
    _tgs.pop(arn, None)
    _targets.pop(arn, None)
    _tg_attrs.pop(arn, None)
    _tags.pop(arn, None)
    return _empty("DeleteTargetGroup")


def _describe_tg_attrs(params):
    arn = _p(params, "TargetGroupArn")
    if arn not in _tgs:
        return _error("TargetGroupNotFound", f"Target group '{arn}' not found.")
    return _xml(200, "DescribeTargetGroupAttributes",
                f"<Attributes>{_attrs_xml(_tg_attrs.get(arn,[]))}</Attributes>")


def _modify_tg_attrs(params):
    arn = _p(params, "TargetGroupArn")
    if arn not in _tgs:
        return _error("TargetGroupNotFound", f"Target group '{arn}' not found.")
    attrs = _tg_attrs.setdefault(arn, [])
    idx = {a["Key"]: i for i, a in enumerate(attrs)}
    i = 1
    while True:
        key = _p(params, f"Attributes.member.{i}.Key")
        if not key:
            break
        val = _p(params, f"Attributes.member.{i}.Value")
        if key in idx:
            attrs[idx[key]]["Value"] = val
        else:
            attrs.append({"Key": key, "Value": val})
            idx[key] = len(attrs) - 1
        i += 1
    return _xml(200, "ModifyTargetGroupAttributes",
                f"<Attributes>{_attrs_xml(attrs)}</Attributes>")


# ---------------------------------------------------------------------------
# Listener handlers
# ---------------------------------------------------------------------------

def _create_listener(params):
    lb_arn = _p(params, "LoadBalancerArn")
    if lb_arn not in _lbs:
        return _error("LoadBalancerNotFound", f"Load balancer '{lb_arn}' not found.")
    lid = _short_id()
    lb = _lbs[lb_arn]
    lb_name = lb["LoadBalancerName"]
    lb_id = lb_arn.split("/")[-1]
    l_arn = (f"arn:aws:elasticloadbalancing:{REGION}:{get_account_id()}"
             f":listener/app/{lb_name}/{lb_id}/{lid}")
    actions = _parse_actions(params, "DefaultActions")
    for action in actions:
        tg_arn = action.get("TargetGroupArn")
        if tg_arn and tg_arn in _tgs and lb_arn not in _tgs[tg_arn]["LoadBalancerArns"]:
            _tgs[tg_arn]["LoadBalancerArns"].append(lb_arn)
    listener = {
        "ListenerArn": l_arn,
        "LoadBalancerArn": lb_arn,
        "Port": int(_p(params, "Port", "80") or 80),
        "Protocol": _p(params, "Protocol", "HTTP"),
        "DefaultActions": actions,
    }
    _listeners[l_arn] = listener
    _listener_attrs[l_arn] = [
        {"Key": "routing.http.response.server.enabled", "Value": "true"},
    ]
    _tags[l_arn] = _parse_tags(params)
    # auto-create default rule
    rule_id = _short_id()
    rule_arn = (f"arn:aws:elasticloadbalancing:{REGION}:{get_account_id()}"
                f":listener-rule/app/{lb_name}/{lb_id}/{lid}/{rule_id}")
    _rules[rule_arn] = {
        "RuleArn": rule_arn, "ListenerArn": l_arn,
        "Priority": "default", "Conditions": [],
        "Actions": actions, "IsDefault": True,
    }
    return _xml(200, "CreateListener", f"<Listeners>{_listener_xml(listener)}</Listeners>")


def _describe_listeners(params):
    lb_arn = _p(params, "LoadBalancerArn")
    arn_filter = _parse_member_list(params, "ListenerArns")
    results = list(_listeners.values())
    if lb_arn:
        results = [l for l in results if l["LoadBalancerArn"] == lb_arn]
    if arn_filter:
        results = [l for l in results if l["ListenerArn"] in arn_filter]
    return _xml(200, "DescribeListeners",
                f"<Listeners>{''.join(_listener_xml(l) for l in results)}</Listeners>")


def _modify_listener(params):
    arn = _p(params, "ListenerArn")
    listener = _listeners.get(arn)
    if not listener:
        return _error("ListenerNotFound", f"Listener '{arn}' not found.")
    port = _p(params, "Port")
    if port:
        listener["Port"] = int(port)
    protocol = _p(params, "Protocol")
    if protocol:
        listener["Protocol"] = protocol
    actions = _parse_actions(params, "DefaultActions")
    if actions:
        listener["DefaultActions"] = actions
    return _xml(200, "ModifyListener", f"<Listeners>{_listener_xml(listener)}</Listeners>")


def _delete_listener(params):
    arn = _p(params, "ListenerArn")
    if arn not in _listeners:
        return _error("ListenerNotFound", f"Listener '{arn}' not found", 400)
    _listeners.pop(arn, None)
    _listener_attrs.pop(arn, None)
    _tags.pop(arn, None)
    for rarn in [k for k, v in list(_rules.items()) if v.get("ListenerArn") == arn]:
        _rules.pop(rarn, None)
    return _empty("DeleteListener")


def _describe_listener_attrs(params):
    arn = _p(params, "ListenerArn")
    if arn not in _listeners:
        return _error("ListenerNotFound", f"Listener '{arn}' not found.")
    attrs = _listener_attrs.get(arn, [])
    return _xml(200, "DescribeListenerAttributes",
                f"<Attributes>{_attrs_xml(attrs)}</Attributes>")


def _modify_listener_attrs(params):
    arn = _p(params, "ListenerArn")
    if arn not in _listeners:
        return _error("ListenerNotFound", f"Listener '{arn}' not found.")
    attrs = _listener_attrs.setdefault(arn, [])
    idx = {a["Key"]: i for i, a in enumerate(attrs)}
    i = 1
    while True:
        key = _p(params, f"Attributes.member.{i}.Key")
        if not key:
            break
        val = _p(params, f"Attributes.member.{i}.Value")
        if key in idx:
            attrs[idx[key]]["Value"] = val
        else:
            attrs.append({"Key": key, "Value": val})
            idx[key] = len(attrs) - 1
        i += 1
    return _xml(200, "ModifyListenerAttributes",
                f"<Attributes>{_attrs_xml(attrs)}</Attributes>")


# ---------------------------------------------------------------------------
# Rule handlers
# ---------------------------------------------------------------------------

def _create_rule(params):
    l_arn = _p(params, "ListenerArn")
    if l_arn not in _listeners:
        return _error("ListenerNotFound", f"Listener '{l_arn}' not found.")
    listener = _listeners[l_arn]
    lb_arn = listener["LoadBalancerArn"]
    lb_name = _lbs[lb_arn]["LoadBalancerName"]
    lb_id = lb_arn.split("/")[-1]
    l_id = l_arn.split("/")[-1]
    rule_id = _short_id()
    rule_arn = (f"arn:aws:elasticloadbalancing:{REGION}:{get_account_id()}"
                f":listener-rule/app/{lb_name}/{lb_id}/{l_id}/{rule_id}")
    rule = {
        "RuleArn": rule_arn, "ListenerArn": l_arn,
        "Priority": _p(params, "Priority", "1"),
        "Conditions": _parse_conditions(params),
        "Actions": _parse_actions(params, "Actions"),
        "IsDefault": False,
    }
    _rules[rule_arn] = rule
    _tags[rule_arn] = _parse_tags(params)
    return _xml(200, "CreateRule", f"<Rules>{_rule_xml(rule)}</Rules>")


def _describe_rules(params):
    l_arn = _p(params, "ListenerArn")
    arn_filter = _parse_member_list(params, "RuleArns")
    results = list(_rules.values())
    if l_arn:
        results = [r for r in results if r.get("ListenerArn") == l_arn]
    if arn_filter:
        results = [r for r in results if r["RuleArn"] in arn_filter]
    return _xml(200, "DescribeRules", f"<Rules>{''.join(_rule_xml(r) for r in results)}</Rules>")


def _modify_rule(params):
    arn = _p(params, "RuleArn")
    rule = _rules.get(arn)
    if not rule:
        return _error("RuleNotFound", f"Rule '{arn}' not found.")
    conds = _parse_conditions(params)
    if conds:
        rule["Conditions"] = conds
    acts = _parse_actions(params, "Actions")
    if acts:
        rule["Actions"] = acts
    return _xml(200, "ModifyRule", f"<Rules>{_rule_xml(rule)}</Rules>")


def _delete_rule(params):
    arn = _p(params, "RuleArn")
    if _rules.get(arn, {}).get("IsDefault"):
        return _error("OperationNotPermitted", "Cannot delete a default rule.")
    _rules.pop(arn, None)
    _tags.pop(arn, None)
    return _empty("DeleteRule")


def _set_rule_priorities(params):
    updated, i = [], 1
    while True:
        arn = _p(params, f"RulePriorities.member.{i}.RuleArn")
        if not arn:
            break
        priority = _p(params, f"RulePriorities.member.{i}.Priority")
        if arn in _rules:
            _rules[arn]["Priority"] = priority
            updated.append(_rules[arn])
        i += 1
    return _xml(200, "SetRulePriorities",
                f"<Rules>{''.join(_rule_xml(r) for r in updated)}</Rules>")


# ---------------------------------------------------------------------------
# Target registration handlers
# ---------------------------------------------------------------------------

def _register_targets(params):
    tg_arn = _p(params, "TargetGroupArn")
    if tg_arn not in _tgs:
        return _error("TargetGroupNotFound", f"Target group '{tg_arn}' not found.")
    new_tgts = _parse_targets_param(params)
    existing = _targets.setdefault(tg_arn, [])
    existing_ids = {t["Id"] for t in existing}
    for t in new_tgts:
        if t["Id"] not in existing_ids:
            existing.append(t)
            existing_ids.add(t["Id"])
    return _empty("RegisterTargets")


def _deregister_targets(params):
    tg_arn = _p(params, "TargetGroupArn")
    if tg_arn not in _tgs:
        return _error("TargetGroupNotFound", f"Target group '{tg_arn}' not found.")
    to_remove = {t["Id"] for t in _parse_targets_param(params)}
    _targets[tg_arn] = [t for t in _targets.get(tg_arn, []) if t["Id"] not in to_remove]
    return _empty("DeregisterTargets")


def _describe_target_health(params):
    tg_arn = _p(params, "TargetGroupArn")
    if tg_arn not in _tgs:
        return _error("TargetGroupNotFound", f"Target group '{tg_arn}' not found.")
    registered = _targets.get(tg_arn, [])
    target_filter = {t["Id"] for t in _parse_targets_param(params)}
    if target_filter:
        registered = [t for t in registered if t["Id"] in target_filter]
    default_port = _tgs[tg_arn].get("Port", 80)
    descs = "".join(
        f"<member>"
        f"<Target><Id>{t['Id']}</Id><Port>{t.get('Port', default_port)}</Port></Target>"
        f"<HealthStatus>healthy</HealthStatus>"
        f"<TargetHealth><State>healthy</State></TargetHealth>"
        f"</member>"
        for t in registered
    )
    return _xml(200, "DescribeTargetHealth",
                f"<TargetHealthDescriptions>{descs}</TargetHealthDescriptions>")


# ---------------------------------------------------------------------------
# Tag handlers
# ---------------------------------------------------------------------------

def _add_tags(params):
    arns = _parse_member_list(params, "ResourceArns")
    new_tags = _parse_tags(params)
    for arn in arns:
        existing = _tags.setdefault(arn, [])
        idx = {t["Key"]: i for i, t in enumerate(existing)}
        for tag in new_tags:
            if tag["Key"] in idx:
                existing[idx[tag["Key"]]]["Value"] = tag["Value"]
            else:
                existing.append(tag)
                idx[tag["Key"]] = len(existing) - 1
    return _empty("AddTags")


def _remove_tags(params):
    arns = _parse_member_list(params, "ResourceArns")
    key_set = set(_parse_member_list(params, "TagKeys"))
    for arn in arns:
        if arn in _tags:
            _tags[arn] = [t for t in _tags[arn] if t["Key"] not in key_set]
    return _empty("RemoveTags")


def _describe_tags(params):
    arns = _parse_member_list(params, "ResourceArns")
    descs = ""
    for arn in arns:
        tags_xml = "".join(
            f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
            for t in _tags.get(arn, [])
        )
        descs += f"<member><ResourceArn>{arn}</ResourceArn><Tags>{tags_xml}</Tags></member>"
    return _xml(200, "DescribeTags", f"<TagDescriptions>{descs}</TagDescriptions>")


# ---------------------------------------------------------------------------
# Action map, request routing, and reset
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    "CreateLoadBalancer": _create_lb,
    "DescribeLoadBalancers": _describe_lbs,
    "DeleteLoadBalancer": _delete_lb,
    "DescribeLoadBalancerAttributes": _describe_lb_attrs,
    "ModifyLoadBalancerAttributes": _modify_lb_attrs,
    "CreateTargetGroup": _create_tg,
    "DescribeTargetGroups": _describe_tgs,
    "ModifyTargetGroup": _modify_tg,
    "DeleteTargetGroup": _delete_tg,
    "DescribeTargetGroupAttributes": _describe_tg_attrs,
    "ModifyTargetGroupAttributes": _modify_tg_attrs,
    "CreateListener": _create_listener,
    "DescribeListeners": _describe_listeners,
    "DescribeListenerAttributes": _describe_listener_attrs,
    "ModifyListenerAttributes": _modify_listener_attrs,
    "ModifyListener": _modify_listener,
    "DeleteListener": _delete_listener,
    "CreateRule": _create_rule,
    "DescribeRules": _describe_rules,
    "ModifyRule": _modify_rule,
    "DeleteRule": _delete_rule,
    "SetRulePriorities": _set_rule_priorities,
    "RegisterTargets": _register_targets,
    "DeregisterTargets": _deregister_targets,
    "DescribeTargetHealth": _describe_target_health,
    "AddTags": _add_tags,
    "RemoveTags": _remove_tags,
    "DescribeTags": _describe_tags,
}


async def handle_request(method, path, headers, body, query_params):
    params = dict(query_params)
    if method == "POST" and body:
        raw = body if isinstance(body, str) else body.decode("utf-8", errors="replace")
        for k, v in parse_qs(raw).items():
            params[k] = v
    action = _p(params, "Action")
    handler = _ACTION_MAP.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown ELBv2 action: {action}", 400)
    return handler(params)


# ---------------------------------------------------------------------------
# Data-plane: host/name lookup
# ---------------------------------------------------------------------------

def find_lb_for_host(host):
    hostname = host.split(":")[0].lower()
    for lb in _lbs.values():
        if lb["DNSName"].lower() == hostname:
            return lb
        if hostname == f"{lb['LoadBalancerName'].lower()}.alb.localhost":
            return lb
    return None


def _find_lb_by_name(name):
    name_lc = name.lower()
    for lb in _lbs.values():
        if lb["LoadBalancerName"].lower() == name_lc:
            return lb
    return None


# ---------------------------------------------------------------------------
# Data-plane: rule matching
# ---------------------------------------------------------------------------

def _match_condition(cond, method, path, headers, query_params):
    field = cond.get("Field", "")
    values = cond.get("Values", [])

    if field == "path-pattern":
        return any(fnmatch.fnmatch(path, v) for v in values)

    if field == "host-header":
        hostname = headers.get("host", "").split(":")[0]
        return any(fnmatch.fnmatch(hostname, v) for v in values)

    if field == "http-method":
        return method.upper() in [v.upper() for v in values]

    if field == "query-string":
        # Values stored as "key=value" strings
        for v in values:
            if "=" in v:
                k, expected = v.split("=", 1)
                actual = query_params.get(k, [""])[0] if isinstance(query_params.get(k), list) else query_params.get(k, "")
                if actual != expected:
                    return False
            else:
                if v not in query_params:
                    return False
        return True

    if field == "http-header":
        cfg = cond.get("HttpHeaderConfig", {})
        hname = cfg.get("HttpHeaderName", "").lower()
        hvals = cfg.get("Values", values)
        actual = headers.get(hname, "")
        return any(fnmatch.fnmatch(actual, v) for v in hvals)

    # source-ip is not implemented (no real network in emulator) — always matches.
    # Unknown condition types also always match to avoid silently dropping traffic.
    return True


def _rule_sort_key(rule):
    p = rule.get("Priority", "default")
    if p == "default":
        return (1, 0)
    try:
        return (0, int(p))
    except (ValueError, TypeError):
        return (0, 9999)


# ---------------------------------------------------------------------------
# Data-plane: action execution
# ---------------------------------------------------------------------------

async def _execute_action(action, method, path, headers, body, query_params):
    atype = action.get("Type", "").lower()

    if atype == "fixed-response":
        frc = action.get("FixedResponseConfig", {})
        code = int(frc.get("StatusCode", "200"))
        ct = frc.get("ContentType", "text/plain")
        msg = frc.get("MessageBody", "")
        return code, {"Content-Type": ct}, msg.encode("utf-8")

    if atype == "redirect":
        rc = action.get("RedirectConfig", {})
        code = int(rc.get("StatusCode", "HTTP_301").replace("HTTP_", ""))
        src_host = headers.get("host", "localhost").split(":")[0]
        proto = rc.get("Protocol", "#{protocol}").replace("#{protocol}", "http")
        rhost = rc.get("Host", "#{host}").replace("#{host}", src_host)
        rport = rc.get("Port", "#{port}").replace("#{port}", "")
        rpath = rc.get("Path", "/#{path}").replace("#{path}", path.lstrip("/"))
        location = f"{proto}://{rhost}"
        if rport and rport not in ("80", "443", ""):
            location += f":{rport}"
        location += rpath
        return code, {"Location": location, "Content-Type": "text/plain"}, b""

    if atype == "forward":
        tg_arn = action.get("TargetGroupArn", "")
        return await _forward_to_tg(tg_arn, method, path, headers, body, query_params)

    return (502, {"Content-Type": "application/json"},
            json.dumps({"message": f"Unsupported action type: {atype}"}).encode())


async def _forward_to_tg(tg_arn, method, path, headers, body, query_params):
    tg = _tgs.get(tg_arn)
    if not tg:
        return (502, {"Content-Type": "application/json"},
                json.dumps({"message": f"Target group '{tg_arn}' not found"}).encode())

    registered = _targets.get(tg_arn, [])
    if not registered:
        return (503, {"Content-Type": "application/json"},
                json.dumps({"message": "No registered targets in target group"}).encode())

    target_type = tg.get("TargetType", "instance")

    if target_type == "lambda":
        func_id = registered[0]["Id"]
        func_name = func_id.split(":function:")[-1].split(":")[0] if ":function:" in func_id else func_id
        return await _invoke_lambda_target(func_name, tg_arn, method, path,
                                           headers, body, query_params)

    return (502, {"Content-Type": "application/json"},
            json.dumps({"message": f"Target type '{target_type}' not supported."}).encode())


async def _invoke_lambda_target(func_name, tg_arn, method, path, headers, body, query_params):
    try:
        from ministack.services import lambda_svc
    except ImportError:
        return (502, {"Content-Type": "application/json"},
                json.dumps({"message": "Lambda service unavailable"}).encode())

    if func_name not in lambda_svc._functions:
        return (502, {"Content-Type": "application/json"},
                json.dumps({"message": f"Lambda function '{func_name}' not found"}).encode())

    body_str = None
    is_b64 = False
    if body:
        try:
            body_str = body.decode("utf-8")
        except UnicodeDecodeError:
            body_str = base64.b64encode(body).decode("ascii")
            is_b64 = True

    qs_single = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}
    qs_multi = {k: (v if isinstance(v, list) else [v]) for k, v in query_params.items()}

    event = {
        "requestContext": {"elb": {"targetGroupArn": tg_arn}},
        "httpMethod": method.upper(),
        "path": path,
        "queryStringParameters": qs_single,
        "multiValueQueryStringParameters": qs_multi,
        "headers": {k.lower(): v for k, v in headers.items()},
        "multiValueHeaders": {k.lower(): [v] for k, v in headers.items()},
        "body": body_str,
        "isBase64Encoded": is_b64,
    }

    _, resp_headers, resp_body = await lambda_svc._invoke(func_name, event, {})

    if resp_headers.get("X-Amz-Function-Error"):
        raw = resp_body.decode("utf-8", errors="replace") if isinstance(resp_body, bytes) else str(resp_body)
        return (502, {"Content-Type": "application/json"},
                json.dumps({"message": f"Lambda error: {raw}"}).encode())

    try:
        result = json.loads(resp_body) if isinstance(resp_body, bytes) else resp_body
        if not isinstance(result, dict):
            return (200, {"Content-Type": "text/plain"},
                    str(result).encode("utf-8"))

        resp_code = int(result.get("statusCode", 200))
        out_headers = dict(result.get("headers") or {})
        for k, vals in (result.get("multiValueHeaders") or {}).items():
            out_headers[k] = vals[-1]

        out_body = result.get("body", "")
        if result.get("isBase64Encoded"):
            out_body = base64.b64decode(out_body)
        elif isinstance(out_body, str):
            out_body = out_body.encode("utf-8")
        elif not isinstance(out_body, bytes):
            out_body = json.dumps(out_body).encode("utf-8")

        return resp_code, out_headers, out_body

    except Exception:
        raw = resp_body if isinstance(resp_body, bytes) else str(resp_body).encode()
        return 200, {"Content-Type": "text/plain"}, raw


# ---------------------------------------------------------------------------
# Data-plane: main dispatcher
# ---------------------------------------------------------------------------

async def dispatch_request(lb, method, path, headers, body, query_params, port=80):
    lb_arn = lb["LoadBalancerArn"]

    candidates = [l for l in _listeners.values() if l["LoadBalancerArn"] == lb_arn]
    matching = [l for l in candidates if l.get("Port", 80) == port] or candidates

    if not matching:
        return (503, {"Content-Type": "application/json"},
                json.dumps({"message": f"No listeners configured for '{lb['LoadBalancerName']}'"}).encode())

    listener = matching[0]
    l_arn = listener["ListenerArn"]

    listener_rules = sorted(
        (r for r in _rules.values() if r.get("ListenerArn") == l_arn),
        key=_rule_sort_key,
    )

    for rule in listener_rules:
        conditions = rule.get("Conditions", [])
        is_default = rule.get("IsDefault", False)
        matched = is_default or all(
            _match_condition(c, method, path, headers, query_params)
            for c in conditions
        )
        if matched:
            actions = rule.get("Actions") or listener.get("DefaultActions", [])
            if actions:
                return await _execute_action(actions[0], method, path,
                                             headers, body, query_params)

    return (502, {"Content-Type": "application/json"},
            json.dumps({"message": "No matching ALB rule found"}).encode())


def reset():
    _lbs.clear()
    _tgs.clear()
    _listeners.clear()
    _rules.clear()
    _targets.clear()
    _tags.clear()
    _lb_attrs.clear()
    _tg_attrs.clear()
    _listener_attrs.clear()
