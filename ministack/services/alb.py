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

import asyncio
import base64
import contextlib
import copy
import fnmatch
import json
import logging
import os
import random
import socket
import string
import time
from urllib.parse import parse_qs, urlencode

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import run_reentrant
from ministack.core.persistence import load_state
from ministack.core.responses import AccountRegionScopedDict, AccountScopedDict, get_account_id, get_region, new_uuid

logger = logging.getLogger("alb")

# ALB uses two deadlines: connecting to a target, then idling on an established
# connection.
# https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-troubleshooting.html#http-504-issues
TARGET_CONNECT_TIMEOUT = float(os.environ.get("ALB_TARGET_CONNECT_TIMEOUT_SECONDS", "10"))
TARGET_IDLE_TIMEOUT = float(os.environ.get("ALB_TARGET_IDLE_TIMEOUT_SECONDS", "60"))

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# authenticate-oidc defaults, matching the AWS API.
OIDC_DEFAULT_COOKIE_NAME = "AWSELBAuthSessionCookie"
OIDC_DEFAULT_SESSION_TIMEOUT = 604800  # 7 days, as AWS documents
OIDC_CALLBACK_PATH = "/oauth2/idpresponse"
# AWS splits the session cookie into shards named -0, -1, -2, … A client that
# reads only the first shard silently loses the tail of a large session, so the
# split is reproduced here rather than smoothed over.
#
# 4096 is the per-cookie ceiling browsers enforce (RFC 6265 §6.1 asks for at
# least 4096 bytes per cookie, and the major engines treat it as the maximum),
# and it covers the *whole* Set-Cookie pair — name, value and attributes. A
# shard sized to 4096 bytes of value alone produces a cookie the browser
# silently discards, which is the same symptom as no session at all, so the
# name and attributes are subtracted from the budget below.
OIDC_COOKIE_MAX_BYTES = 4096
NS = "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/"

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
# Resource stores are region-scoped (ELBv2 resources are regional and List/
# Describe are region-scoped). The ARN-keyed satellite stores below stay
# account-scoped — their ARN key already embeds the region.
_lbs = AccountRegionScopedDict()        # lb_arn   -> LB record
_tgs = AccountRegionScopedDict()        # tg_arn   -> TG record
_listeners = AccountRegionScopedDict()  # l_arn    -> Listener record
_rules = AccountRegionScopedDict()      # r_arn    -> Rule record
_targets = AccountScopedDict()    # tg_arn   -> [target dict] (ARN key embeds region)
_tags = AccountScopedDict()       # res_arn  -> [{Key, Value}] (ARN key embeds region)
_lb_attrs = AccountScopedDict()   # lb_arn   -> [{Key, Value}] (ARN key embeds region)
_tg_attrs = AccountScopedDict()   # tg_arn   -> [{Key, Value}] (ARN key embeds region)
_listener_attrs = AccountScopedDict()  # l_arn -> [{Key, Value}] (ARN key embeds region)

# authenticate-oidc: in-flight authorization requests, keyed by the opaque
# `state` value handed to the identity provider. Holds only the URL to return
# the browser to once the callback lands — the session itself lives in the
# client's cookie, exactly as it does on a real load balancer, so nothing here
# is needed to validate a request.
_oidc_pending = AccountScopedDict()  # state -> {"url": str, "created": float}


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
        "_oidc_pending": _oidc_pending,
    })


def _region_from_arn(value, fallback):
    if not isinstance(value, str) or not value.startswith("arn:"):
        return fallback
    try:
        return parse_arn(value).region or fallback
    except ArnParseError:
        return fallback


def _restore_region_scoped(store, restored):
    """Re-home legacy account-scoped ALB records into their ARN's region.
    New state already arrives region-scoped."""
    if isinstance(restored, AccountRegionScopedDict):
        store.update(restored)
        return
    boot_region = get_region()
    if isinstance(restored, AccountScopedDict):
        entries = restored._data.items()
    elif isinstance(restored, dict):
        account_id = get_account_id()
        entries = (((account_id, arn), rec) for arn, rec in restored.items())
    else:
        store.update(restored)
        return
    for (account_id, arn), rec in entries:
        store.set_scoped(account_id, _region_from_arn(arn, boot_region), arn, rec)


def restore_state(data):
    _restore_region_scoped(_lbs, data.get("_lbs", {}))
    _restore_region_scoped(_tgs, data.get("_tgs", {}))
    _restore_region_scoped(_listeners, data.get("_listeners", {}))
    _restore_region_scoped(_rules, data.get("_rules", {}))
    _targets.update(data.get("_targets", {}))
    _tags.update(data.get("_tags", {}))
    _lb_attrs.update(data.get("_lb_attrs", {}))
    _tg_attrs.update(data.get("_tg_attrs", {}))
    _listener_attrs.update(data.get("_listener_attrs", {}))
    _oidc_pending.update(data.get("_oidc_pending", {}))


try:
    _restored = load_state("alb")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )

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


def _elbv2_resource_tail(arn: str, prefix: str) -> str:
    """Return a stored ELBv2 ARN tail for ID generation, or empty string."""
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return ""
    if spec.service != "elasticloadbalancing" or not spec.resource.startswith(prefix):
        return ""
    return spec.resource[len(prefix):]


def _load_balancer_id_from_arn(arn: str) -> str:
    tail = _elbv2_resource_tail(arn, "loadbalancer/")
    return tail.rpartition("/")[2] if tail else ""


def _listener_id_from_arn(arn: str) -> str:
    tail = _elbv2_resource_tail(arn, "listener/")
    return tail.rpartition("/")[2] if tail else ""


def _target_group_full_name_from_arn(arn: str) -> str:
    return _elbv2_resource_tail(arn, "targetgroup/")


def _resolve_taggable_elbv2_arn(arn: str):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None, _error("ValidationError", f"Invalid resource ARN: {arn}")

    if (
        spec.partition != "aws"
        or spec.service != "elasticloadbalancing"
        or spec.region != get_region()
        or spec.account_id != get_account_id()
    ):
        return None, _error("ValidationError", f"Invalid resource ARN: {arn}")

    resources = (
        ("loadbalancer/", _lbs, "LoadBalancerNotFound", "Load balancer"),
        ("targetgroup/", _tgs, "TargetGroupNotFound", "Target group"),
        ("listener/", _listeners, "ListenerNotFound", "Listener"),
        ("listener-rule/", _rules, "RuleNotFound", "Rule"),
    )
    for prefix, store, code, label in resources:
        if spec.resource.startswith(prefix):
            if arn not in store:
                return None, _error(code, f"{label} '{arn}' not found.")
            return arn, None

    return None, _error("ValidationError", f"Invalid resource ARN: {arn}")


def _resolve_taggable_elbv2_arns(arns):
    resolved = []
    for arn in arns:
        arn, err = _resolve_taggable_elbv2_arn(arn)
        if err:
            return [], err
        resolved.append(arn)
    return resolved, None


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
        oidc_issuer = _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.Issuer")
        if oidc_issuer:
            oidc = {
                "Issuer": oidc_issuer,
                "AuthorizationEndpoint": _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.AuthorizationEndpoint"),
                "TokenEndpoint": _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.TokenEndpoint"),
                "UserInfoEndpoint": _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.UserInfoEndpoint"),
                "ClientId": _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.ClientId"),
                "Scope": _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.Scope", "openid"),
                "SessionCookieName": _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.SessionCookieName",
                                        OIDC_DEFAULT_COOKIE_NAME),
                "SessionTimeout": _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.SessionTimeout",
                                     str(OIDC_DEFAULT_SESSION_TIMEOUT)),
                "OnUnauthenticatedRequest": _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.OnUnauthenticatedRequest",
                                               "authenticate"),
            }
            secret = _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.ClientSecret")
            if secret:
                # Held for the back-channel token exchange only. AWS never
                # returns it from Describe*, and neither does _action_xml.
                oidc["ClientSecret"] = secret
            reuse = _p(params, f"{prefix}.member.{i}.AuthenticateOidcConfig.UseExistingClientSecret")
            if reuse:
                oidc["UseExistingClientSecret"] = reuse.lower() == "true"
            action["AuthenticateOidcConfig"] = oidc

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


# A rule condition carries its values either in the flat legacy ``Values`` list or
# in a per-field typed config. The Terraform AWS provider sends the typed form, so a
# parser that reads only ``Values`` records an empty condition, and the rule then
# matches nothing and the listener falls through to its default action.
_CONDITION_VALUE_KEYS = {
    "path-pattern": "PathPatternConfig",
    "host-header": "HostHeaderConfig",
    "http-request-method": "HttpRequestMethodConfig",
    "source-ip": "SourceIpConfig",
}


def _parse_condition_values(params, base):
    """Read a condition's values from whichever shape the caller used."""
    def _values_at(prefix):
        out, j = [], 1
        while True:
            v = _p(params, f"{prefix}.member.{j}")
            if not v:
                break
            out.append(v)
            j += 1
        return out

    values = _values_at(f"{base}.Values")
    if values:
        return values

    for config_key in _CONDITION_VALUE_KEYS.values():
        values = _values_at(f"{base}.{config_key}.Values")
        if values:
            return values
    return []


def _parse_conditions(params, prefix="Conditions"):
    conditions, i = [], 1
    while True:
        base = f"{prefix}.member.{i}"
        field = _p(params, f"{base}.Field")
        if not field:
            break
        conditions.append({"Field": field, "Values": _parse_condition_values(params, base)})
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
        f"<member><ZoneName>{get_region()}a</ZoneName><SubnetId>{s}</SubnetId>"
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
    if "AuthenticateOidcConfig" in a:
        oc = a["AuthenticateOidcConfig"]
        # ClientSecret is deliberately absent: AWS never echoes it back.
        inner += (
            f"<AuthenticateOidcConfig>"
            f"<Issuer>{oc.get('Issuer','')}</Issuer>"
            f"<AuthorizationEndpoint>{oc.get('AuthorizationEndpoint','')}</AuthorizationEndpoint>"
            f"<TokenEndpoint>{oc.get('TokenEndpoint','')}</TokenEndpoint>"
            f"<UserInfoEndpoint>{oc.get('UserInfoEndpoint','')}</UserInfoEndpoint>"
            f"<ClientId>{oc.get('ClientId','')}</ClientId>"
            f"<Scope>{oc.get('Scope','openid')}</Scope>"
            f"<SessionCookieName>{oc.get('SessionCookieName', OIDC_DEFAULT_COOKIE_NAME)}</SessionCookieName>"
            f"<SessionTimeout>{oc.get('SessionTimeout', OIDC_DEFAULT_SESSION_TIMEOUT)}</SessionTimeout>"
            f"<OnUnauthenticatedRequest>{oc.get('OnUnauthenticatedRequest','authenticate')}</OnUnauthenticatedRequest>"
            f"</AuthenticateOidcConfig>"
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


def _condition_xml(c):
    """Render a condition the way AWS does: the flat Values list *and*, for the
    fields that have one, the typed config carrying the same values.

    A client that reads only the typed config — the Terraform provider does, for
    path_pattern and host_header — sees an empty condition otherwise, and plans a
    change on every run to put the values back.
    """
    values = c.get("Values", [])
    members = "".join(f"<member>{v}</member>" for v in values)
    xml = f"<member><Field>{c['Field']}</Field><Values>{members}</Values>"
    config_key = _CONDITION_VALUE_KEYS.get(c.get("Field"))
    if config_key:
        xml += f"<{config_key}><Values>{members}</Values></{config_key}>"
    return xml + "</member>"


def _rule_xml(r):
    conds = "".join(_condition_xml(c) for c in r.get("Conditions", []))
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
    arn = f"arn:aws:elasticloadbalancing:{get_region()}:{get_account_id()}:loadbalancer/app/{name}/{lid}"
    lb = {
        "LoadBalancerArn": arn,
        "LoadBalancerName": name,
        "DNSName": f"{name}-{lid[:8]}.{get_region()}.elb.amazonaws.com",
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
    arn = f"arn:aws:elasticloadbalancing:{get_region()}:{get_account_id()}:targetgroup/{name}/{tid}"
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
    lb_id = _load_balancer_id_from_arn(lb_arn)
    l_arn = (f"arn:aws:elasticloadbalancing:{get_region()}:{get_account_id()}"
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
    rule_arn = (f"arn:aws:elasticloadbalancing:{get_region()}:{get_account_id()}"
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
        # CreateListener snapshots the default actions into an auto-created
        # default rule, and the data plane reads the rule, not the listener.
        # Updating only the listener leaves the old actions serving traffic —
        # ModifyListener would appear to succeed and change nothing.
        for rule in _rules.values():
            if rule.get("ListenerArn") == arn and rule.get("IsDefault"):
                rule["Actions"] = actions
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
    lb_id = _load_balancer_id_from_arn(lb_arn)
    l_id = _listener_id_from_arn(l_arn)
    rule_id = _short_id()
    rule_arn = (f"arn:aws:elasticloadbalancing:{get_region()}:{get_account_id()}"
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

def set_targets_for_group(tg_arn, targets, previous_ids=()):
    """Replace the caller's own registrations in a target group.

    Used by ECS, whose services reconcile their tasks' addresses as they come
    and go. Real ECS deregisters only its own tasks, so registrations made by
    anyone else — RegisterTargets calls, or a second service sharing the
    group — survive: only ``previous_ids`` (the caller's last publication) are
    withdrawn before ``targets`` (as ``(id, port)`` pairs) are added.
    Unknown target groups are ignored rather than raising: a service can
    outlive the group it referenced.
    """
    if tg_arn not in _tgs:
        return False
    withdrawn = set(previous_ids)
    fresh = [{"Id": tid, "Port": port} for tid, port in targets]
    fresh_ids = {t["Id"] for t in fresh}
    _targets[tg_arn] = [
        t for t in _targets.get(tg_arn, [])
        if t["Id"] not in withdrawn and t["Id"] not in fresh_ids
    ] + fresh
    return True


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
    arns, err = _resolve_taggable_elbv2_arns(_parse_member_list(params, "ResourceArns"))
    if err:
        return err
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
    arns, err = _resolve_taggable_elbv2_arns(_parse_member_list(params, "ResourceArns"))
    if err:
        return err
    key_set = set(_parse_member_list(params, "TagKeys"))
    for arn in arns:
        if arn in _tags:
            _tags[arn] = [t for t in _tags[arn] if t["Key"] not in key_set]
    return _empty("RemoveTags")


def _describe_tags(params):
    arns = _parse_member_list(params, "ResourceArns")
    descs = ""
    for arn in arns:
        arn, err = _resolve_taggable_elbv2_arn(arn)
        if err:
            return err
        tags_xml = "".join(
            f"<member><Key>{t['Key']}</Key><Value>{t['Value']}</Value></member>"
            for t in _tags.get(arn, [])
        )
        descs += f"<member><ResourceArn>{arn}</ResourceArn><Tags>{tags_xml}</Tags></member>"
    return _xml(200, "DescribeTags", f"<TagDescriptions>{descs}</TagDescriptions>")


# ---------------------------------------------------------------------------
# SetSubnets / SetIpAddressType / SetSecurityGroups
# ---------------------------------------------------------------------------

def _set_subnets(params):
    arn = _p(params, "LoadBalancerArn")
    lb = _lbs.get(arn)
    if not lb:
        return _error("LoadBalancerNotFound", "Load balancer not found", 400)
    subnets = _parse_member_list(params, "Subnets")
    if not subnets:
        # SubnetMappings.member.N.SubnetId form
        mappings = []
        idx = 1
        while True:
            sid = _p(params, f"SubnetMappings.member.{idx}.SubnetId")
            if not sid:
                break
            mappings.append(sid)
            idx += 1
        subnets = mappings
    if subnets:
        lb["Subnets"] = subnets
    ip_type = _p(params, "IpAddressType")
    if ip_type:
        lb["IpAddressType"] = ip_type
    azs = "".join(
        f"<member><ZoneName>{get_region()}a</ZoneName><SubnetId>{s}</SubnetId>"
        f"<LoadBalancerAddresses/></member>"
        for s in lb.get("Subnets", [])
    )
    return _xml(200, "SetSubnets",
                f"<AvailabilityZones>{azs}</AvailabilityZones>"
                f"<IpAddressType>{lb.get('IpAddressType','ipv4')}</IpAddressType>")


def _set_ip_address_type(params):
    arn = _p(params, "LoadBalancerArn")
    lb = _lbs.get(arn)
    if not lb:
        return _error("LoadBalancerNotFound", "Load balancer not found", 400)
    ip_type = _p(params, "IpAddressType")
    if ip_type not in ("ipv4", "dualstack", "dualstack-without-public-ipv4"):
        return _error("ValidationError",
                      f"Invalid IpAddressType: {ip_type}", 400)
    lb["IpAddressType"] = ip_type
    return _xml(200, "SetIpAddressType",
                f"<IpAddressType>{ip_type}</IpAddressType>")


def _set_security_groups(params):
    arn = _p(params, "LoadBalancerArn")
    lb = _lbs.get(arn)
    if not lb:
        return _error("LoadBalancerNotFound", "Load balancer not found", 400)
    sgs = _parse_member_list(params, "SecurityGroups")
    lb["SecurityGroups"] = sgs
    members = "".join(f"<member>{s}</member>" for s in sgs)
    return _xml(200, "SetSecurityGroups",
                f"<SecurityGroupIds>{members}</SecurityGroupIds>")


# ---------------------------------------------------------------------------
# Action map, request routing, and reset
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    "CreateLoadBalancer": _create_lb,
    "DescribeLoadBalancers": _describe_lbs,
    "DeleteLoadBalancer": _delete_lb,
    "DescribeLoadBalancerAttributes": _describe_lb_attrs,
    "ModifyLoadBalancerAttributes": _modify_lb_attrs,
    "SetSubnets": _set_subnets,
    "SetIpAddressType": _set_ip_address_type,
    "SetSecurityGroups": _set_security_groups,
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

    if field in ("http-request-method", "http-method"):
        # AWS's field name is http-request-method; the old local-only spelling
        # keeps matching for rules stored before the rename.
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
# Data-plane: authenticate-oidc
# ---------------------------------------------------------------------------
#
# The listener rule action that makes a load balancer an OIDC relying party.
# An unauthenticated request is sent to the identity provider; the provider
# returns the browser to OIDC_CALLBACK_PATH with a code; the load balancer
# exchanges that code for tokens on the back channel and writes the session
# into the client's cookie. Only then does the request reach the target, with
# the caller's identity attached in X-Amzn-Oidc-* headers.
#
# The session lives entirely in the cookie, as it does on AWS, so no
# server-side lookup is needed to validate a request. AWS encrypts that cookie
# with a key only the load balancer holds; here it is base64-encoded JSON,
# because the point is to reproduce the protocol a client observes, not to
# keep a secret from the developer running the emulator.


def _oidc_parse_cookies(headers):
    """Parse a Cookie header into a plain dict."""
    jar = {}
    for part in (headers.get("cookie") or "").split(";"):
        name, sep, value = part.partition("=")
        if sep:
            jar[name.strip()] = value.strip()
    return jar


def _oidc_read_session(jar, cookie_name):
    """Reassemble a session from its cookie shards, or return None.

    Shards must be contiguous from -0. A gap means the client dropped part of
    the session, which is indistinguishable from having no session at all.
    """
    shards, index = [], 0
    while f"{cookie_name}-{index}" in jar:
        shards.append(jar[f"{cookie_name}-{index}"])
        index += 1
    if not shards:
        return None
    try:
        blob = "".join(shards)
        blob += "=" * (-len(blob) % 4)  # restore stripped base64 padding
        return json.loads(base64.urlsafe_b64decode(blob.encode()))
    except Exception:
        return None


def _oidc_session_cookies(cookie_name, session, max_age, secure):
    """Render a session as Set-Cookie values, sharded the way AWS shards them.

    Each shard is sized so the complete Set-Cookie pair fits inside the
    browser's per-cookie ceiling — see OIDC_COOKIE_MAX_BYTES.
    """
    blob = base64.urlsafe_b64encode(json.dumps(session).encode()).decode().rstrip("=")
    attrs = f"; Path=/; HttpOnly; Max-Age={max_age}"
    if secure:
        attrs += "; Secure"

    # Budget for the value alone. The index is allowed three digits, which is
    # far more shards than a session can plausibly need, so the reservation
    # stays correct as the count grows.
    overhead = len(f"{cookie_name}-000=") + len(attrs)
    budget = max(OIDC_COOKIE_MAX_BYTES - overhead, 1)

    shards = [blob[i:i + budget] for i in range(0, len(blob), budget)] or [""]
    return [f"{cookie_name}-{i}={shard}{attrs}" for i, shard in enumerate(shards)]


def _oidc_expiry_cookies(cookie_name, count):
    """Expire previously-set shards so a stale session cannot linger."""
    return [f"{cookie_name}-{i}=; Path=/; HttpOnly; Max-Age=0" for i in range(max(count, 1))]


def _oidc_request_url(headers, path, query_params, listener_port):
    """Rebuild the URL the client asked for, to return to after authenticating."""
    host = headers.get("host") or f"localhost:{listener_port}"
    proto = headers.get("x-forwarded-proto") or "http"
    qs = urlencode([(k, v) for k, vs in (query_params or {}).items()
                    for v in (vs if isinstance(vs, list) else [vs])])
    return f"{proto}://{host}{path}" + (f"?{qs}" if qs else "")


def _oidc_post_form(url, form, timeout=10.0):
    """POST a form to the identity provider's token endpoint and read JSON back.

    Blocking on purpose: run it through run_reentrant, the same way the target
    proxy does, rather than adding an async HTTP dependency to the emulator.
    """
    import http.client
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    conn_cls = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(parts.hostname, parts.port or (443 if parts.scheme == "https" else 80),
                    timeout=timeout)
    try:
        body = urlencode(form)
        conn.request("POST", parts.path or "/", body=body, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Content-Length": str(len(body)),
            "Accept": "application/json",
        })
        resp = conn.getresponse()
        raw = resp.read()
        try:
            return resp.status, json.loads(raw or b"{}")
        except ValueError:
            return resp.status, {"raw": raw.decode("utf-8", errors="replace")}
    finally:
        with contextlib.suppress(Exception):
            conn.close()


def _oidc_get_userinfo(url, access_token, timeout=10.0):
    """GET the IdP's user info endpoint with the access token.

    This is where the claims the target sees come from on AWS: the load
    balancer exchanges the access token at the user info endpoint and passes
    those claims on — it never passes the ID token to the backend.
    """
    import http.client
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    conn_cls = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(parts.hostname, parts.port or (443 if parts.scheme == "https" else 80),
                    timeout=timeout)
    try:
        conn.request("GET", parts.path or "/", headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        })
        resp = conn.getresponse()
        raw = resp.read()
        if resp.status >= 400:
            return None
        try:
            claims = json.loads(raw or b"{}")
        except ValueError:
            return None
        return claims if isinstance(claims, dict) else None
    except Exception:
        return None
    finally:
        with contextlib.suppress(Exception):
            conn.close()


# The key the emulator signs x-amzn-oidc-data with. AWS signs with a regional
# key served from public-keys.auth.elb.<region>.amazonaws.com/<kid>; there is
# no such endpoint locally, so the signature is real ES256 but verifiable only
# in principle. Apps that decode the payload (the common pattern) work; apps
# that fetch the AWS key endpoint fail loudly, exactly as they would against
# any non-AWS host.
_OIDC_SIGNING_KEY = None
_OIDC_KID = None


def _oidc_signing_material():
    global _OIDC_SIGNING_KEY, _OIDC_KID
    if _OIDC_KID is None:
        _OIDC_KID = new_uuid()
        try:
            from cryptography.hazmat.primitives.asymmetric import ec
            _OIDC_SIGNING_KEY = ec.generate_private_key(ec.SECP256R1())
        except Exception:
            _OIDC_SIGNING_KEY = None
    return _OIDC_SIGNING_KEY, _OIDC_KID


def _oidc_b64url(raw: bytes) -> str:
    # ALB's documented quirk: unlike standard JWTs, the x-amzn-oidc-data
    # segments are base64url WITH the trailing padding characters ("includes
    # padding characters at the end" per the user-claims-encoding docs) —
    # strict JWT libraries choke on real ALB tokens for exactly this reason,
    # so an emulator that strips the padding hides that failure mode.
    return base64.urlsafe_b64encode(raw).decode()


def _oidc_data_jwt(claims, cfg, lb_arn, exp):
    """Build x-amzn-oidc-data the way AWS documents it: the user claims as an
    ES256 JWT whose header carries ``kid``, ``signer`` (the load balancer ARN),
    ``iss``, ``client`` and ``exp``. The payload is the claims from the user
    info endpoint — never the provider's ID token, which AWS does not pass to
    the backend."""
    key, kid = _oidc_signing_material()
    header = {
        "typ": "JWT",
        "alg": "ES256",
        "kid": kid,
        "signer": lb_arn,
        "iss": cfg.get("Issuer", ""),
        "client": cfg.get("ClientId", ""),
        "exp": exp,
    }
    signing_input = (
        _oidc_b64url(json.dumps(header, separators=(",", ":")).encode())
        + "."
        + _oidc_b64url(json.dumps(claims, separators=(",", ":")).encode())
    )
    signature = b""
    if key is not None:
        try:
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.asymmetric import ec, utils
            der = key.sign(signing_input.encode(), ec.ECDSA(hashes.SHA256()))
            r, s = utils.decode_dss_signature(der)
            signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")
        except Exception:
            signature = b""
    return f"{signing_input}.{_oidc_b64url(signature)}"


def _oidc_claims(id_token):
    """Read the claims out of an ID token without verifying it.

    The token came straight from the provider over the back channel, so there
    is no third party to have tampered with it. Verification would mean
    fetching JWKS, which buys an emulator nothing.
    """
    try:
        payload = id_token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload.encode()))
    except Exception:
        return {}


async def _authenticate_oidc(action, method, path, headers, body, query_params, listener_port,
                             lb_arn=""):
    """Run one authenticate-oidc action.

    Returns None when the caller is authenticated and the request should carry
    on to the next action in the rule; otherwise returns the (status, headers,
    body) triple to send back. On success the caller's identity is injected
    into `headers` for the target to read.
    """
    cfg = action.get("AuthenticateOidcConfig") or {}
    cookie_name = cfg.get("SessionCookieName") or OIDC_DEFAULT_COOKIE_NAME
    try:
        session_timeout = int(cfg.get("SessionTimeout") or OIDC_DEFAULT_SESSION_TIMEOUT)
    except (TypeError, ValueError):
        session_timeout = OIDC_DEFAULT_SESSION_TIMEOUT
    secure = (headers.get("x-forwarded-proto") or "").lower() == "https"
    jar = _oidc_parse_cookies(headers)

    # The X-Amzn-Oidc-* headers are the load balancer's to set. Strip any the
    # client supplied before anything else runs — otherwise `allow` mode would
    # forward a forged identity straight to the target.
    for forged in ("x-amzn-oidc-identity", "x-amzn-oidc-accesstoken", "x-amzn-oidc-data"):
        headers.pop(forged, None)

    # --- the callback leg -------------------------------------------------
    if path == OIDC_CALLBACK_PATH:
        code = _oidc_first(query_params.get("code"))
        state = _oidc_first(query_params.get("state"))
        pending = _oidc_pending.pop(state, None) if state else None
        if not code or pending is None:
            # A replayed, forged, or timed-out callback. AWS bounds the whole
            # login at 15 minutes and answers HTTP 401 when it is exceeded.
            return (401, {"Content-Type": "text/plain"},
                    b"Invalid or expired authentication response")

        status, tokens = await run_reentrant(
            lambda: _oidc_post_form(cfg.get("TokenEndpoint", ""), {
                "grant_type": "authorization_code",
                "code": code,
                "client_id": cfg.get("ClientId", ""),
                "client_secret": cfg.get("ClientSecret", ""),
                "redirect_uri": pending["redirect_uri"],
            }),
            thread_name="ministack-alb-oidc-token",
        )
        if status >= 400 or "access_token" not in tokens:
            logger.warning("ALB authenticate-oidc: token exchange failed (%s): %s", status, tokens)
            return (561, {"Content-Type": "text/plain"},
                    b"Authentication failed: the identity provider rejected the code exchange")

        # The claims the target sees come from the user info endpoint, as on
        # AWS; the ID token only serves as a fallback when the IdP has no
        # usable user info response. AWS never passes the ID token itself to
        # the backend, and neither does this.
        userinfo = cfg.get("UserInfoEndpoint", "")
        claims = None
        if userinfo:
            claims = await run_reentrant(
                lambda: _oidc_get_userinfo(userinfo, tokens.get("access_token", "")),
                thread_name="ministack-alb-oidc-userinfo",
            )
        if not claims:
            claims = _oidc_claims(tokens.get("id_token", ""))
        session = {
            "claims": claims,
            "access_token": tokens.get("access_token", ""),
            "exp": int(time.time()) + session_timeout,
        }
        return (302, {
            "Location": pending["url"],
            "Set-Cookie": _oidc_session_cookies(cookie_name, session, session_timeout, secure),
            "Content-Type": "text/plain",
        }, b"")

    # --- an established session -------------------------------------------
    session = _oidc_read_session(jar, cookie_name)
    if session and int(session.get("exp", 0)) > time.time():
        claims = session.get("claims") or {}
        headers["x-amzn-oidc-identity"] = str(claims.get("sub", ""))
        headers["x-amzn-oidc-accesstoken"] = session.get("access_token", "")
        headers["x-amzn-oidc-data"] = _oidc_data_jwt(
            claims, cfg, lb_arn, int(session.get("exp", 0)))
        return None

    # --- no usable session -------------------------------------------------
    on_unauth = (cfg.get("OnUnauthenticatedRequest") or "authenticate").lower()
    if on_unauth == "allow":
        return None
    if on_unauth == "deny":
        return (401, {"Content-Type": "text/plain"}, b"Unauthorized")

    host = headers.get("host") or f"localhost:{listener_port}"
    proto = headers.get("x-forwarded-proto") or "http"
    redirect_uri = f"{proto}://{host}{OIDC_CALLBACK_PATH}"
    state = new_uuid()
    # AWS bounds a login attempt at 15 minutes; anything older is dead weight
    # an abandoned redirect left behind.
    cutoff = time.time() - 900
    for stale in [k for k, v in _oidc_pending.items() if v.get("created", 0) < cutoff]:
        _oidc_pending.pop(stale, None)
    _oidc_pending[state] = {
        "url": _oidc_request_url(headers, path, query_params, listener_port),
        "redirect_uri": redirect_uri,
        "created": time.time(),
    }
    location = cfg.get("AuthorizationEndpoint", "") + "?" + urlencode({
        "client_id": cfg.get("ClientId", ""),
        "response_type": "code",
        "scope": cfg.get("Scope") or "openid",
        "redirect_uri": redirect_uri,
        "state": state,
    })
    expired = _oidc_expiry_cookies(cookie_name, len(jar)) if session else []
    out_headers = {"Location": location, "Content-Type": "text/plain"}
    if expired:
        out_headers["Set-Cookie"] = expired
    return (302, out_headers, b"")


def _oidc_first(value):
    """Query parameters arrive as either a scalar or a list."""
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return value


# ---------------------------------------------------------------------------
# Data-plane: action execution
# ---------------------------------------------------------------------------

async def _execute_actions(actions, method, path, headers, body, query_params, listener_port=80,
                           lb_arn=""):
    """Run a rule's actions in Order until one produces a response.

    Authentication actions are gates: they either short-circuit with a redirect
    or a refusal, or they pass the request on to the action behind them, having
    attached the caller's identity. Every other action type is terminal, which
    is why the loop returns on the first one it meets.
    """
    ordered = sorted(actions, key=lambda a: int(a.get("Order", 1) or 1))
    forwarded = dict(headers)

    for action in ordered:
        atype = (action.get("Type") or "").lower()
        if atype == "authenticate-oidc":
            response = await _authenticate_oidc(action, method, path, forwarded,
                                                body, query_params, listener_port,
                                                lb_arn=lb_arn)
            if response is not None:
                return response
            continue
        if atype == "authenticate-cognito":
            # Cognito authentication is the same protocol with the endpoints
            # derived from a user pool rather than given explicitly. Not
            # implemented; failing loudly beats forwarding an unauthenticated
            # request to a target that assumes the load balancer checked.
            return (501, {"Content-Type": "application/json"},
                    json.dumps({"message": "authenticate-cognito is not implemented"}).encode())
        return await _execute_action(action, method, path, forwarded, body, query_params)

    # Every action was a gate that passed and none of them was terminal.
    return (502, {"Content-Type": "application/json"},
            json.dumps({"message": "Rule has no terminal action"}).encode())


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
        return await _invoke_lambda_target(func_id, tg_arn, method, path,
                                           headers, body, query_params)

    if target_type in ("instance", "ip"):
        target = random.choice(registered)
        return await _proxy_http_target(target, tg, method, path,
                                        headers, body, query_params)

    return (502, {"Content-Type": "application/json"},
            json.dumps({"message": f"Target type '{target_type}' not supported."}).encode())


async def _invoke_lambda_target(function_ref, tg_arn, method, path, headers, body, query_params):
    try:
        from ministack.services import lambda_svc
    except ImportError:
        return (502, {"Content-Type": "application/json"},
                json.dumps({"message": "Lambda service unavailable"}).encode())

    try:
        tg_spec = parse_arn(tg_arn)
    except ArnParseError:
        tg_spec = None
    owner_account_id = tg_spec.account_id if tg_spec else get_account_id()
    owner_region = tg_spec.region if tg_spec else get_region()

    func, config, func_name = lambda_svc._get_func_record_for_ref_in_scope(
        function_ref,
        account_id=owner_account_id,
        region=owner_region,
    )
    if not func or not config:
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

    exec_record = lambda_svc._execution_record_for_config(func, config)

    def _invoke_and_record_metrics():
        started = time.time()
        invocation_result = lambda_svc._execute_function_with_config_scope(exec_record, event)
        duration_ms = (time.time() - started) * 1000.0
        lambda_svc._run_with_function_config_scope(
            config,
            lambda_svc._emit_lambda_metrics,
            func_name,
            duration_ms=duration_ms,
            error=bool(invocation_result.get("error")),
            throttle=bool(invocation_result.get("throttle")),
        )
        return invocation_result

    exec_result = await run_reentrant(_invoke_and_record_metrics,
                                      thread_name="ministack-alb-invoke")
    lambda_response, _err = lambda_svc.lambda_execute_result_to_api_proxy_response(exec_result)

    if exec_result.get("error"):
        raw = lambda_response.get("body", "") if lambda_response else exec_result.get("body", "")
        raw = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        return (502, {"Content-Type": "application/json"},
                json.dumps({"message": f"Lambda error: {raw}"}).encode())

    try:
        result = lambda_response or {}
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
        raw = json.dumps(lambda_response).encode()
        return 200, {"Content-Type": "text/plain"}, raw


async def _await_http_disconnect(receive) -> None:
    """Resolve when the ASGI client goes away."""
    while True:
        message = await receive()
        if message.get("type") == "http.disconnect":
            return


async def _proxy_http_target(target, tg, method, path, headers, body, query_params):
    """Forward a data-plane request to an instance/ip target over HTTP.

    The target Id is used as the host to connect to — an IP address for
    ip target groups, or any resolvable hostname for instance target
    groups (an emulator has no EC2 metadata to resolve instance ids
    against, so a hostname is the useful interpretation). Mirrors ALB
    behavior: hop-by-hop headers are stripped, X-Forwarded-* and
    X-Amzn-Trace-Id are injected, target HTTP errors pass through, and
    connection failures surface as 502.
    """
    import http.client
    from urllib.parse import urlencode

    host = str(target.get("Id", ""))
    port = int(target.get("Port") or tg.get("Port") or 80)
    qs = {k: (v[0] if isinstance(v, list) else v) for k, v in (query_params or {}).items()}
    target_path = path + (f"?{urlencode(qs)}" if qs else "")

    hop_by_hop = {"host", "content-length", "connection", "transfer-encoding",
                  "accept-encoding"}
    fwd = {k: v for k, v in (headers or {}).items() if k.lower() not in hop_by_hop}
    fwd["X-Forwarded-For"] = (headers or {}).get("x-forwarded-for") or "127.0.0.1"
    fwd.setdefault("X-Forwarded-Proto", "http")
    fwd.setdefault("X-Forwarded-Port", "80")
    fwd["X-Amzn-Trace-Id"] = "Root=1-%08x-%s" % (
        int(time.time()),
        "".join(random.choices("0123456789abcdef", k=24)),
    )

    data = body if isinstance(body, (bytes, bytearray)) else (body.encode() if body else None)

    # Relay the body as it arrives and keep the target's framing, as AWS does.
    # https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-troubleshooting.html#http-504-issues
    from ministack.core.responses import StreamingResponse

    def _open():
        conn = http.client.HTTPConnection(host, port, timeout=TARGET_CONNECT_TIMEOUT)
        try:
            conn.connect()
            sock = conn.sock
            sock.settimeout(TARGET_IDLE_TIMEOUT)
            conn.request(method.upper(), target_path, body=data, headers=fwd)
            resp = conn.getresponse()
        except Exception as e:
            with contextlib.suppress(Exception):
                conn.close()
            return None, None, None, e
        return conn, resp, sock, None

    conn, resp, sock, err = await asyncio.to_thread(_open)
    if resp is None:
        return (502, {"Content-Type": "application/json"},
                json.dumps({"message": f"Target {host}:{port} connect error: {err}"}).encode())

    status, out_headers = resp.status, dict(resp.headers)

    async def _stream(send, receive):
        disconnected = asyncio.create_task(_await_http_disconnect(receive))
        complete = False
        try:
            while True:
                reader = asyncio.create_task(asyncio.to_thread(resp.read1, 65536))
                done, _ = await asyncio.wait(
                    {reader, disconnected}, return_when=asyncio.FIRST_COMPLETED
                )
                if disconnected in done:
                    reader.cancel()
                    break
                chunk = reader.result()
                if not chunk:
                    # read1 returns b"" where read() would raise IncompleteRead.
                    complete = not resp.length
                    break
                await send({"type": "http.response.body", "body": chunk, "more_body": True})
        except Exception:
            logger.exception("ALB target %s:%s stream failed", host, port)
        finally:
            disconnected.cancel()

            with contextlib.suppress(Exception):
                sock.shutdown(socket.SHUT_RDWR)

            def _close():
                with contextlib.suppress(Exception):
                    resp.close()
                with contextlib.suppress(Exception):
                    conn.close()

            with contextlib.suppress(RuntimeError):
                asyncio.get_running_loop().run_in_executor(None, _close)
            if complete:
                # Withholding the terminator is what marks a truncated body.
                with contextlib.suppress(Exception):
                    await send({"type": "http.response.body", "body": b"", "more_body": False})

    return status, out_headers, StreamingResponse(_stream)


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
                return await _execute_actions(actions, method, path,
                                              headers, body, query_params, port,
                                              lb_arn=lb.get("LoadBalancerArn", ""))

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
