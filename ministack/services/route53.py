"""
Amazon Route 53 Emulator.
REST/XML API — service credential scope: route53.

Supports:
  Hosted Zones:   CreateHostedZone, GetHostedZone, DeleteHostedZone,
                  ListHostedZones, ListHostedZonesByName,
                  UpdateHostedZoneComment
  Record Sets:    ChangeResourceRecordSets, ListResourceRecordSets
  Changes:        GetChange
  Health Checks:  CreateHealthCheck, GetHealthCheck, DeleteHealthCheck,
                  ListHealthChecks, UpdateHealthCheck
  Tags:           ChangeTagsForResource, ListTagsForResource

Wire protocol:
  All requests/responses use XML with namespace
  https://route53.amazonaws.com/doc/2013-04-01/
  Paths are under /2013-04-01/
"""

import copy
import logging
import random
import re
import string
import threading
from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement, tostring

from defusedxml.ElementTree import fromstring

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import AccountScopedDict, new_uuid

logger = logging.getLogger("route53")

NS = "https://route53.amazonaws.com/doc/2013-04-01/"
API_VERSION = "2013-04-01"

# ─── in-memory state ──────────────────────────────────────────────────────────

_zones = AccountScopedDict()           # zone_id -> zone dict
_records = AccountScopedDict()         # zone_id -> list of record-set dicts
_changes = AccountScopedDict()         # change_id -> change dict
_health_checks = AccountScopedDict()   # hc_id -> health check dict
_tags = AccountScopedDict()            # (resource_type, resource_id) -> {key: value}
_caller_refs = AccountScopedDict()     # caller_reference -> zone_id (idempotency)
_hc_caller_refs = AccountScopedDict()  # caller_reference -> hc_id
_lock = threading.Lock()


# ── Persistence ────────────────────────────────────────────

def get_state():
    with _lock:
        return {
            "zones": copy.deepcopy(_zones),
            "records": copy.deepcopy(_records),
            "health_checks": copy.deepcopy(_health_checks),
            "tags": {f"{k[0]}|{k[1]}": v for k, v in copy.deepcopy(_tags).items()},
            "caller_refs": copy.deepcopy(_caller_refs),
            "hc_caller_refs": copy.deepcopy(_hc_caller_refs),
            "changes": copy.deepcopy(_changes),
        }


def restore_state(data):
    if data:
        with _lock:
            _zones.update(data.get("zones", {}))
            _records.update(data.get("records", {}))
            _health_checks.update(data.get("health_checks", {}))
            raw_tags = data.get("tags", {})
            for k, v in raw_tags.items():
                parts = k.split("|", 1)
                if len(parts) == 2:
                    _tags[(parts[0], parts[1])] = v
            _caller_refs.update(data.get("caller_refs", {}))
            _hc_caller_refs.update(data.get("hc_caller_refs", {}))
            _changes.update(data.get("changes", {}))


try:
    _restored = load_state("route53")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


def reset():
    with _lock:
        _zones.clear()
        _records.clear()
        _changes.clear()
        _health_checks.clear()
        _tags.clear()
        _caller_refs.clear()
        _hc_caller_refs.clear()


# ─── ID generators ────────────────────────────────────────────────────────────

_ID_CHARS = string.ascii_uppercase + string.digits


def _zone_id() -> str:
    return "Z" + "".join(random.choices(_ID_CHARS, k=13))


def _change_id() -> str:
    return "C" + "".join(random.choices(_ID_CHARS, k=13))


def _hc_id() -> str:
    return new_uuid()


# ─── XML helpers ─────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _xml_response(root_tag: str, builder_fn, status: int = 200) -> tuple:
    root = Element(root_tag, xmlns=NS)
    builder_fn(root)
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    return status, {"Content-Type": "text/xml"}, body


def _error_response(code: str, message: str, status: int = 400) -> tuple:
    root = Element("ErrorResponse", xmlns=NS)
    err = SubElement(root, "Error")
    SubElement(err, "Type").text = "Sender"
    SubElement(err, "Code").text = code
    SubElement(err, "Message").text = message
    SubElement(root, "RequestId").text = new_uuid()
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    return status, {"Content-Type": "text/xml"}, body


def _find(el, tag):
    """Find child by local tag name ignoring namespace."""
    for child in el:
        local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if local == tag:
            return child
    return None


def _findall(el, tag):
    return [c for c in el if (c.tag.split("}")[-1] if "}" in c.tag else c.tag) == tag]


def _text(el, tag, default=""):
    child = _find(el, tag)
    return child.text or default if child is not None else default


def _parse_body(body: bytes):
    if not body:
        return None
    try:
        return fromstring(body.decode("utf-8"))
    except Exception:
        return None


# ─── domain name helpers ──────────────────────────────────────────────────────

def _normalise_name(name: str) -> str:
    """Ensure domain name ends with a dot."""
    if not name:
        return name
    return name if name.endswith(".") else name + "."


def _name_sort_key(name: str) -> tuple[str, ...]:
    """Route53 sorts names by labels reversed (e.g. com.example.www.)."""
    labels = _normalise_name(name).rstrip(".").split(".")
    return tuple(reversed(labels))


# ─── default records (SOA + NS) ───────────────────────────────────────────────

_DEFAULT_NS = [
    "ns-1.awsdns-1.com.",
    "ns-2.awsdns-2.net.",
    "ns-3.awsdns-3.org.",
    "ns-4.awsdns-4.co.uk.",
]


def _default_records(zone_name: str) -> list:
    return [
        {
            "Name": zone_name,
            "Type": "SOA",
            "TTL": "900",
            "ResourceRecords": [
                f"{_DEFAULT_NS[0]} awsdns-hostmaster.amazon.com. 1 7200 900 1209600 86400"
            ],
        },
        {
            "Name": zone_name,
            "Type": "NS",
            "TTL": "172800",
            "ResourceRecords": list(_DEFAULT_NS),
        },
    ]


# ─── record set key (for uniqueness) ─────────────────────────────────────────

def _rs_key(rs: dict) -> tuple:
    return (rs["Name"], rs["Type"], rs.get("SetIdentifier", ""))


# ─── XML builders for common structures ───────────────────────────────────────

def _build_hosted_zone_el(parent: Element, zone: dict):
    hz = SubElement(parent, "HostedZone")
    SubElement(hz, "Id").text = f"/hostedzone/{zone['id']}"
    SubElement(hz, "Name").text = zone["name"]
    SubElement(hz, "CallerReference").text = zone["caller_reference"]
    cfg = SubElement(hz, "Config")
    SubElement(cfg, "Comment").text = zone.get("comment", "")
    SubElement(cfg, "PrivateZone").text = "true" if zone.get("private") else "false"
    SubElement(hz, "ResourceRecordSetCount").text = str(
        len(_records.get(zone["id"], []))
    )


def _build_delegation_set_el(parent: Element, zone_name: str):
    ds = SubElement(parent, "DelegationSet")
    SubElement(ds, "NameServers")
    ns_list = _find(ds, "NameServers")
    for ns in _DEFAULT_NS:
        SubElement(ns_list, "NameServer").text = ns


def _build_change_info_el(parent: Element, change: dict):
    ci = SubElement(parent, "ChangeInfo")
    SubElement(ci, "Id").text = f"/change/{change['id']}"
    SubElement(ci, "Status").text = change["status"]
    SubElement(ci, "SubmittedAt").text = change["submitted_at"]
    if change.get("comment"):
        SubElement(ci, "Comment").text = change["comment"]


def _build_record_set_el(parent: Element, rs: dict):
    rrs = SubElement(parent, "ResourceRecordSet")
    SubElement(rrs, "Name").text = rs["Name"]
    SubElement(rrs, "Type").text = rs["Type"]
    if rs.get("SetIdentifier"):
        SubElement(rrs, "SetIdentifier").text = rs["SetIdentifier"]
    if rs.get("Weight") is not None:
        SubElement(rrs, "Weight").text = str(rs["Weight"])
    if rs.get("Region"):
        SubElement(rrs, "Region").text = rs["Region"]
    if rs.get("Failover"):
        SubElement(rrs, "Failover").text = rs["Failover"]
    if rs.get("MultiValueAnswer") is not None:
        SubElement(rrs, "MultiValueAnswer").text = str(rs["MultiValueAnswer"]).lower()
    if rs.get("TTL") is not None:
        SubElement(rrs, "TTL").text = str(rs["TTL"])
    if rs.get("AliasTarget"):
        at = SubElement(rrs, "AliasTarget")
        SubElement(at, "HostedZoneId").text = rs["AliasTarget"].get("HostedZoneId", "")
        SubElement(at, "DNSName").text = rs["AliasTarget"].get("DNSName", "")
        SubElement(at, "EvaluateTargetHealth").text = str(
            rs["AliasTarget"].get("EvaluateTargetHealth", False)
        ).lower()
    if rs.get("ResourceRecords"):
        rr_list = SubElement(rrs, "ResourceRecords")
        for val in rs["ResourceRecords"]:
            rr = SubElement(rr_list, "ResourceRecord")
            SubElement(rr, "Value").text = val
    if rs.get("HealthCheckId"):
        SubElement(rrs, "HealthCheckId").text = rs["HealthCheckId"]
    if rs.get("GeoLocation"):
        geo = SubElement(rrs, "GeoLocation")
        gl = rs["GeoLocation"]
        if gl.get("ContinentCode"):
            SubElement(geo, "ContinentCode").text = gl["ContinentCode"]
        if gl.get("CountryCode"):
            SubElement(geo, "CountryCode").text = gl["CountryCode"]
        if gl.get("SubdivisionCode"):
            SubElement(geo, "SubdivisionCode").text = gl["SubdivisionCode"]
    if rs.get("CidrRoutingConfig"):
        crc = SubElement(rrs, "CidrRoutingConfig")
        SubElement(crc, "CollectionId").text = rs["CidrRoutingConfig"].get("CollectionId", "")
        SubElement(crc, "LocationName").text = rs["CidrRoutingConfig"].get("LocationName", "")


# ─── record set XML parser ────────────────────────────────────────────────────

def _parse_record_set(el) -> dict:
    rs = {}
    rs["Name"] = _normalise_name(_text(el, "Name"))
    rs["Type"] = _text(el, "Type")
    if _find(el, "SetIdentifier") is not None:
        rs["SetIdentifier"] = _text(el, "SetIdentifier")
    if _find(el, "Weight") is not None:
        rs["Weight"] = int(_text(el, "Weight", "0"))
    if _find(el, "Region") is not None:
        rs["Region"] = _text(el, "Region")
    if _find(el, "Failover") is not None:
        rs["Failover"] = _text(el, "Failover")
    if _find(el, "MultiValueAnswer") is not None:
        rs["MultiValueAnswer"] = _text(el, "MultiValueAnswer").lower() == "true"
    if _find(el, "TTL") is not None:
        rs["TTL"] = _text(el, "TTL")
    at_el = _find(el, "AliasTarget")
    if at_el is not None:
        dns_name = _text(at_el, "DNSName")
        if dns_name and not dns_name.endswith("."):
            dns_name += "."
        rs["AliasTarget"] = {
            "HostedZoneId": _text(at_el, "HostedZoneId"),
            "DNSName": dns_name,
            "EvaluateTargetHealth": _text(at_el, "EvaluateTargetHealth", "false").lower() == "true",
        }
    rr_container = _find(el, "ResourceRecords")
    if rr_container is not None:
        rs["ResourceRecords"] = [
            _text(rr, "Value") for rr in _findall(rr_container, "ResourceRecord")
        ]
    if _find(el, "HealthCheckId") is not None:
        rs["HealthCheckId"] = _text(el, "HealthCheckId")
    geo_el = _find(el, "GeoLocation")
    if geo_el is not None:
        gl = {}
        for field in ("ContinentCode", "CountryCode", "SubdivisionCode"):
            if _find(geo_el, field) is not None:
                gl[field] = _text(geo_el, field)
        rs["GeoLocation"] = gl
    crc_el = _find(el, "CidrRoutingConfig")
    if crc_el is not None:
        rs["CidrRoutingConfig"] = {
            "CollectionId": _text(crc_el, "CollectionId"),
            "LocationName": _text(crc_el, "LocationName"),
        }
    return rs


# ─── health check XML builder ─────────────────────────────────────────────────

def _build_health_check_el(parent: Element, hc: dict):
    h = SubElement(parent, "HealthCheck")
    SubElement(h, "Id").text = hc["id"]
    SubElement(h, "CallerReference").text = hc["caller_reference"]
    SubElement(h, "HealthCheckVersion").text = str(hc.get("version", 1))
    cfg_el = SubElement(h, "HealthCheckConfig")
    cfg = hc.get("config", {})
    for field in (
        "Type", "IPAddress", "Port", "FullyQualifiedDomainName", "ResourcePath",
        "SearchString", "RequestInterval", "FailureThreshold", "MeasureLatency",
        "EnableSNI", "Inverted", "Disabled", "HealthThreshold", "RoutingControlArn",
        "InsufficientDataHealthStatus",
    ):
        if field in cfg:
            SubElement(cfg_el, field).text = str(cfg[field])
    if "ChildHealthChecks" in cfg:
        chc = SubElement(cfg_el, "ChildHealthChecks")
        for c in cfg["ChildHealthChecks"]:
            SubElement(chc, "ChildHealthCheck").text = c
    if "Regions" in cfg:
        reg_el = SubElement(cfg_el, "Regions")
        for r in cfg["Regions"]:
            SubElement(reg_el, "Region").text = r
    if "AlarmIdentifier" in cfg:
        ai = SubElement(cfg_el, "AlarmIdentifier")
        SubElement(ai, "Name").text = cfg["AlarmIdentifier"].get("Name", "")
        SubElement(ai, "Region").text = cfg["AlarmIdentifier"].get("Region", "")


def _parse_health_check_config(el) -> dict:
    cfg = {}
    for field in (
        "Type", "IPAddress", "FullyQualifiedDomainName", "ResourcePath",
        "SearchString", "InsufficientDataHealthStatus", "RoutingControlArn",
    ):
        if _find(el, field) is not None:
            cfg[field] = _text(el, field)
    for int_field in ("Port", "RequestInterval", "FailureThreshold", "HealthThreshold"):
        if _find(el, int_field) is not None:
            cfg[int_field] = int(_text(el, int_field, "0"))
    for bool_field in ("MeasureLatency", "EnableSNI", "Inverted", "Disabled"):
        if _find(el, bool_field) is not None:
            cfg[bool_field] = _text(el, bool_field).lower() == "true"
    chc_el = _find(el, "ChildHealthChecks")
    if chc_el is not None:
        cfg["ChildHealthChecks"] = [_text(c, "ChildHealthCheck") for c in _findall(chc_el, "ChildHealthCheck")]
    reg_el = _find(el, "Regions")
    if reg_el is not None:
        cfg["Regions"] = [_text(r, "Region") for r in _findall(reg_el, "Region")]
    ai_el = _find(el, "AlarmIdentifier")
    if ai_el is not None:
        cfg["AlarmIdentifier"] = {
            "Name": _text(ai_el, "Name"),
            "Region": _text(ai_el, "Region"),
        }
    return cfg


# ─── operations ──────────────────────────────────────────────────────────────

def _create_hosted_zone(body: bytes, query_params: dict):
    root = _parse_body(body)
    if root is None:
        return _error_response("InvalidInput", "Missing or invalid request body.")

    caller_ref = _text(root, "CallerReference")
    name = _normalise_name(_text(root, "Name"))
    if not name or not caller_ref:
        return _error_response("InvalidInput", "Name and CallerReference are required.")

    cfg_el = _find(root, "HostedZoneConfig")
    comment = _text(cfg_el, "Comment") if cfg_el is not None else ""
    private = _text(cfg_el, "PrivateZone", "false").lower() == "true" if cfg_el is not None else False

    with _lock:
        if caller_ref in _caller_refs:
            existing_id = _caller_refs[caller_ref]
            zone = _zones[existing_id]
            change = {"id": _change_id(), "status": "INSYNC", "submitted_at": _now_iso(), "comment": ""}
            def build(root):
                _build_hosted_zone_el(root, zone)
                _build_change_info_el(root, change)
                _build_delegation_set_el(root, zone["name"])
            return _xml_response("CreateHostedZoneResponse", build, 201)

        zone_id = _zone_id()
        zone = {
            "id": zone_id,
            "name": name,
            "caller_reference": caller_ref,
            "comment": comment,
            "private": private,
        }
        _zones[zone_id] = zone
        _records[zone_id] = _default_records(name)
        _caller_refs[caller_ref] = zone_id

        change_id = _change_id()
        change = {"id": change_id, "status": "INSYNC", "submitted_at": _now_iso(), "comment": ""}
        _changes[change_id] = change

    def build(root):
        _build_hosted_zone_el(root, zone)
        _build_change_info_el(root, change)
        _build_delegation_set_el(root, name)

    return _xml_response("CreateHostedZoneResponse", build, 201)


def _get_hosted_zone(zone_id: str):
    with _lock:
        zone = _zones.get(zone_id)
    if not zone:
        return _error_response("NoSuchHostedZone", f"No hosted zone found with ID: {zone_id}", 404)

    def build(root):
        _build_hosted_zone_el(root, zone)
        _build_delegation_set_el(root, zone["name"])

    return _xml_response("GetHostedZoneResponse", build)


def _delete_hosted_zone(zone_id: str):
    with _lock:
        zone = _zones.get(zone_id)
        if not zone:
            return _error_response("NoSuchHostedZone", f"No hosted zone found with ID: {zone_id}", 404)
        recs = _records.get(zone_id, [])
        non_default = [r for r in recs if not (r["Type"] in ("SOA", "NS") and r["Name"] == zone["name"])]
        if non_default:
            return _error_response(
                "HostedZoneNotEmpty",
                "The hosted zone contains resource record sets other than the default SOA and NS records.",
            )
        del _zones[zone_id]
        del _records[zone_id]
        _caller_refs.pop(zone.get("caller_reference", ""), None)
        change_id = _change_id()
        change = {"id": change_id, "status": "INSYNC", "submitted_at": _now_iso(), "comment": ""}
        _changes[change_id] = change

    def build(root):
        _build_change_info_el(root, change)

    return _xml_response("DeleteHostedZoneResponse", build)


def _list_hosted_zones(query_params: dict):
    marker = (query_params.get("marker") or [""])[0] if isinstance(query_params.get("marker"), list) else query_params.get("marker", "")
    max_items = int((query_params.get("maxitems") or ["100"])[0] if isinstance(query_params.get("maxitems"), list) else query_params.get("maxitems", 100))
    max_items = min(max_items, 100)

    with _lock:
        zones = sorted(_zones.values(), key=lambda z: z["id"])

    if marker:
        zones = [z for z in zones if z["id"] > marker]

    is_truncated = len(zones) > max_items
    page = zones[:max_items]
    next_marker = page[-1]["id"] if is_truncated else None

    def build(root):
        hz_list = SubElement(root, "HostedZones")
        for zone in page:
            _build_hosted_zone_el(hz_list, zone)
        SubElement(root, "IsTruncated").text = str(is_truncated).lower()
        SubElement(root, "Marker").text = marker or ""
        SubElement(root, "MaxItems").text = str(max_items)
        if next_marker:
            SubElement(root, "NextMarker").text = next_marker

    return _xml_response("ListHostedZonesResponse", build)


def _list_hosted_zones_by_name(query_params: dict):
    def _qp(key):
        v = query_params.get(key, "")
        return (v[0] if isinstance(v, list) else v) or ""

    dns_name = _normalise_name(_qp("dnsname")) if _qp("dnsname") else ""
    hz_id = _qp("hostedzoneid")
    max_items = min(int(_qp("maxitems") or 100), 100)

    with _lock:
        zones = sorted(_zones.values(), key=lambda z: z["name"])

    if dns_name:
        zones = [z for z in zones if z["name"] >= dns_name]
    if hz_id:
        zones = [z for z in zones if z["id"] >= hz_id]

    is_truncated = len(zones) > max_items
    page = zones[:max_items]
    next_dns = page[-1]["name"] if is_truncated else None
    next_hz = page[-1]["id"] if is_truncated else None

    def build(root):
        SubElement(root, "DNSName").text = dns_name
        SubElement(root, "HostedZoneId").text = hz_id
        hz_list = SubElement(root, "HostedZones")
        for zone in page:
            _build_hosted_zone_el(hz_list, zone)
        SubElement(root, "IsTruncated").text = str(is_truncated).lower()
        SubElement(root, "MaxItems").text = str(max_items)
        if next_dns:
            SubElement(root, "NextDNSName").text = next_dns
        if next_hz:
            SubElement(root, "NextHostedZoneId").text = next_hz

    return _xml_response("ListHostedZonesByNameResponse", build)


def _update_hosted_zone_comment(zone_id: str, body: bytes):
    root = _parse_body(body)
    if root is None:
        return _error_response("InvalidInput", "Missing or invalid request body.")
    with _lock:
        zone = _zones.get(zone_id)
        if not zone:
            return _error_response("NoSuchHostedZone", f"No hosted zone found with ID: {zone_id}", 404)
        cfg_el = _find(root, "Comment")
        if cfg_el is not None:
            zone["comment"] = cfg_el.text or ""

    def build(root):
        _build_hosted_zone_el(root, zone)

    return _xml_response("UpdateHostedZoneCommentResponse", build)


def _change_resource_record_sets(zone_id: str, body: bytes):
    root = _parse_body(body)
    if root is None:
        return _error_response("InvalidInput", "Missing or invalid request body.")

    with _lock:
        zone = _zones.get(zone_id)
        if not zone:
            return _error_response("NoSuchHostedZone", f"No hosted zone found with ID: {zone_id}", 404)

        batch_el = _find(root, "ChangeBatch")
        if batch_el is None:
            return _error_response("InvalidInput", "Missing ChangeBatch element.")
        comment = _text(batch_el, "Comment")
        changes_el = _find(batch_el, "Changes")
        if changes_el is None:
            return _error_response("InvalidInput", "Missing Changes element.")

        ops = []
        for change_el in _findall(changes_el, "Change"):
            action = _text(change_el, "Action")
            rs_el = _find(change_el, "ResourceRecordSet")
            if rs_el is None:
                return _error_response("InvalidInput", "Missing ResourceRecordSet element.")
            rs = _parse_record_set(rs_el)
            if not rs.get("Name") or not rs.get("Type"):
                return _error_response("InvalidInput", "Name and Type are required in ResourceRecordSet.")
            ops.append((action, rs))

        current = list(_records[zone_id])

        for action, rs in ops:
            key = _rs_key(rs)
            existing = next((r for r in current if _rs_key(r) == key), None)

            if action == "CREATE":
                if existing:
                    return _error_response(
                        "InvalidChangeBatch",
                        f"Tried to create resource record set {rs['Name']} type {rs['Type']} but it already exists.",
                    )
                current.append(rs)

            elif action == "DELETE":
                if not existing:
                    return _error_response(
                        "InvalidChangeBatch",
                        f"Tried to delete resource record set {rs['Name']} type {rs['Type']} but it does not exist.",
                    )
                current = [r for r in current if _rs_key(r) != key]

            elif action == "UPSERT":
                if existing:
                    current = [rs if _rs_key(r) == key else r for r in current]
                else:
                    current.append(rs)
            else:
                return _error_response("InvalidInput", f"Unknown action: {action}")

        _records[zone_id] = current
        change_id = _change_id()
        change = {"id": change_id, "status": "INSYNC", "submitted_at": _now_iso(), "comment": comment}
        _changes[change_id] = change

    def build(root):
        _build_change_info_el(root, change)

    return _xml_response("ChangeResourceRecordSetsResponse", build)


def _list_resource_record_sets(zone_id: str, query_params: dict):
    def _qp(key):
        v = query_params.get(key, "")
        return (v[0] if isinstance(v, list) else v) or ""

    start_name = _normalise_name(_qp("name")) if _qp("name") else ""
    start_type = _qp("type")
    start_id = _qp("identifier")
    max_items = min(int(_qp("maxitems") or 300), 300)

    with _lock:
        zone = _zones.get(zone_id)
        if not zone:
            return _error_response("NoSuchHostedZone", f"No hosted zone found with ID: {zone_id}", 404)
        records = list(_records.get(zone_id, []))

    records.sort(
        key=lambda r: (
            _name_sort_key(r["Name"]),
            r["Type"],
            r.get("SetIdentifier", ""),
        )
    )

    if start_name:
        start_key = (
            _name_sort_key(start_name),
            start_type,
            start_id,
        )
        records = [
            r
            for r in records
            if (
                _name_sort_key(r["Name"]),
                r["Type"],
                r.get("SetIdentifier", ""),
            )
            >= start_key
        ]

    is_truncated = len(records) > max_items
    page = records[:max_items]
    next_name = records[max_items]["Name"] if is_truncated else None
    next_type = records[max_items]["Type"] if is_truncated else None
    next_id = records[max_items].get("SetIdentifier", "") if is_truncated else None

    def build(root):
        rrs_list = SubElement(root, "ResourceRecordSets")
        for rs in page:
            _build_record_set_el(rrs_list, rs)
        SubElement(root, "IsTruncated").text = str(is_truncated).lower()
        SubElement(root, "MaxItems").text = str(max_items)
        if next_name:
            SubElement(root, "NextRecordName").text = next_name
        if next_type:
            SubElement(root, "NextRecordType").text = next_type
        if next_id:
            SubElement(root, "NextRecordIdentifier").text = next_id

    return _xml_response("ListResourceRecordSetsResponse", build)


def _get_change(change_id: str):
    with _lock:
        change = _changes.get(change_id)
    if not change:
        return _error_response("NoSuchChange", f"A change with the ID {change_id} does not exist.", 404)

    def build(root):
        _build_change_info_el(root, change)

    return _xml_response("GetChangeResponse", build)


# ─── health checks ────────────────────────────────────────────────────────────

def _create_health_check(body: bytes):
    root = _parse_body(body)
    if root is None:
        return _error_response("InvalidInput", "Missing or invalid request body.")

    caller_ref = _text(root, "CallerReference")
    if not caller_ref:
        return _error_response("InvalidInput", "CallerReference is required.")

    cfg_el = _find(root, "HealthCheckConfig")
    cfg = _parse_health_check_config(cfg_el) if cfg_el is not None else {}

    with _lock:
        if caller_ref in _hc_caller_refs:
            hc = _health_checks[_hc_caller_refs[caller_ref]]
            def build(root):
                _build_health_check_el(root, hc)
            return _xml_response("CreateHealthCheckResponse", build, 201)

        hc_id = _hc_id()
        hc = {"id": hc_id, "caller_reference": caller_ref, "config": cfg, "version": 1}
        _health_checks[hc_id] = hc
        _hc_caller_refs[caller_ref] = hc_id

    def build(root):
        _build_health_check_el(root, hc)

    return _xml_response("CreateHealthCheckResponse", build, 201)


def _get_health_check(hc_id: str):
    with _lock:
        hc = _health_checks.get(hc_id)
    if not hc:
        return _error_response("NoSuchHealthCheck", f"No health check exists with the specified ID {hc_id}.", 404)

    def build(root):
        _build_health_check_el(root, hc)

    return _xml_response("GetHealthCheckResponse", build)


def _delete_health_check(hc_id: str):
    with _lock:
        if hc_id not in _health_checks:
            return _error_response("NoSuchHealthCheck", f"No health check exists with the specified ID {hc_id}.", 404)
        hc = _health_checks.pop(hc_id)
        _hc_caller_refs.pop(hc.get("caller_reference", ""), None)

    return _xml_response("DeleteHealthCheckResponse", lambda root: None)


def _list_health_checks(query_params: dict):
    def _qp(key):
        v = query_params.get(key, "")
        return (v[0] if isinstance(v, list) else v) or ""

    marker = _qp("marker")
    max_items = min(int(_qp("maxitems") or 100), 1000)

    with _lock:
        checks = sorted(_health_checks.values(), key=lambda h: h["id"])

    if marker:
        checks = [h for h in checks if h["id"] > marker]

    is_truncated = len(checks) > max_items
    page = checks[:max_items]
    next_marker = page[-1]["id"] if is_truncated else None

    def build(root):
        hc_list = SubElement(root, "HealthChecks")
        for hc in page:
            _build_health_check_el(hc_list, hc)
        SubElement(root, "IsTruncated").text = str(is_truncated).lower()
        SubElement(root, "Marker").text = marker
        SubElement(root, "MaxItems").text = str(max_items)
        if next_marker:
            SubElement(root, "NextMarker").text = next_marker

    return _xml_response("ListHealthChecksResponse", build)


def _update_health_check(hc_id: str, body: bytes):
    root = _parse_body(body)
    if root is None:
        return _error_response("InvalidInput", "Missing or invalid request body.")
    with _lock:
        hc = _health_checks.get(hc_id)
        if not hc:
            return _error_response("NoSuchHealthCheck", f"No health check exists with the specified ID {hc_id}.", 404)
        updates = _parse_health_check_config(root)
        hc["config"] = {**hc["config"], **updates}
        hc["version"] = hc.get("version", 1) + 1

    def build(root):
        _build_health_check_el(root, hc)

    return _xml_response("UpdateHealthCheckResponse", build)


# ─── tags ─────────────────────────────────────────────────────────────────────

def _change_tags_for_resource(resource_type: str, resource_id: str, body: bytes):
    root = _parse_body(body)
    if root is None:
        return _error_response("InvalidInput", "Missing or invalid request body.")

    with _lock:
        if resource_type == "hostedzone" and resource_id not in _zones:
            return _error_response("NoSuchHostedZone", f"No hosted zone found with ID: {resource_id}", 404)
        if resource_type == "healthcheck" and resource_id not in _health_checks:
            return _error_response("NoSuchHealthCheck", f"No health check exists with the specified ID {resource_id}.", 404)

        key = (resource_type, resource_id)
        if key not in _tags:
            _tags[key] = {}

        add_el = _find(root, "AddTags")
        if add_el is not None:
            for tag_el in _findall(add_el, "Tag"):
                k = _text(tag_el, "Key")
                v = _text(tag_el, "Value")
                if k:
                    _tags[key][k] = v

        remove_el = _find(root, "RemoveTagKeys")
        if remove_el is not None:
            for key_el in _findall(remove_el, "Key"):
                _tags[key].pop(key_el.text or "", None)

    return _xml_response("ChangeTagsForResourceResponse", lambda root: None)


def _list_tags_for_resource(resource_type: str, resource_id: str):
    with _lock:
        if resource_type == "hostedzone" and resource_id not in _zones:
            return _error_response("NoSuchHostedZone", f"No hosted zone found with ID: {resource_id}", 404)
        if resource_type == "healthcheck" and resource_id not in _health_checks:
            return _error_response("NoSuchHealthCheck", f"No health check exists with the specified ID {resource_id}.", 404)
        tag_map = dict(_tags.get((resource_type, resource_id), {}))

    def build(root):
        rts = SubElement(root, "ResourceTagSet")
        SubElement(rts, "ResourceType").text = resource_type
        SubElement(rts, "ResourceId").text = resource_id
        tags_el = SubElement(rts, "Tags")
        for k, v in tag_map.items():
            tag_el = SubElement(tags_el, "Tag")
            SubElement(tag_el, "Key").text = k
            SubElement(tag_el, "Value").text = v

    return _xml_response("ListTagsForResourceResponse", build)


# ─── request router ───────────────────────────────────────────────────────────

async def handle_request(method, path, headers, body, query_params):
    # Strip /2013-04-01 prefix
    p = path
    if p.startswith(f"/{API_VERSION}"):
        p = p[len(f"/{API_VERSION}"):]

    # POST /hostedzone
    if method == "POST" and p == "/hostedzone":
        return _create_hosted_zone(body, query_params)

    # GET /hostedzone
    if method == "GET" and p == "/hostedzone":
        return _list_hosted_zones(query_params)

    # GET /hostedzonesbyname
    if method == "GET" and p == "/hostedzonesbyname":
        return _list_hosted_zones_by_name(query_params)

    # GET|DELETE|POST /hostedzone/{id}
    m = re.match(r"^/hostedzone/([^/]+)$", p)
    if m:
        zone_id = m.group(1)
        if method == "GET":
            return _get_hosted_zone(zone_id)
        if method == "DELETE":
            return _delete_hosted_zone(zone_id)
        if method == "POST":
            return _update_hosted_zone_comment(zone_id, body)

    # POST /hostedzone/{id}/rrset/
    m = re.match(r"^/hostedzone/([^/]+)/rrset/?$", p)
    if m:
        zone_id = m.group(1)
        if method == "POST":
            return _change_resource_record_sets(zone_id, body)
        if method == "GET":
            return _list_resource_record_sets(zone_id, query_params)

    # GET /change/{id}
    m = re.match(r"^/change/([^/]+)$", p)
    if m and method == "GET":
        return _get_change(m.group(1))

    # POST /healthcheck
    if method == "POST" and p == "/healthcheck":
        return _create_health_check(body)

    # GET /healthcheck
    if method == "GET" and p == "/healthcheck":
        return _list_health_checks(query_params)

    # GET|DELETE|POST /healthcheck/{id}
    m = re.match(r"^/healthcheck/([^/]+)$", p)
    if m:
        hc_id = m.group(1)
        if method == "GET":
            return _get_health_check(hc_id)
        if method == "DELETE":
            return _delete_health_check(hc_id)
        if method == "POST":
            return _update_health_check(hc_id, body)

    # POST /tags/{resourceType}/{resourceId}
    m = re.match(r"^/tags/([^/]+)/([^/]+)$", p)
    if m:
        resource_type, resource_id = m.group(1), m.group(2)
        if method == "POST":
            return _change_tags_for_resource(resource_type, resource_id, body)
        if method == "GET":
            return _list_tags_for_resource(resource_type, resource_id)

    return _error_response("InvalidInput", f"Unknown Route53 endpoint: {method} {path}", 400)
