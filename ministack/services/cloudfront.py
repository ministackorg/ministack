"""
CloudFront Service Emulator.
REST/XML API — service credential scope: cloudfront.
Paths are under /2020-05-31/

Supports:
  Distributions: CreateDistribution, GetDistribution, GetDistributionConfig,
                 ListDistributions, UpdateDistribution, DeleteDistribution
  Invalidations: CreateInvalidation, ListInvalidations, GetInvalidation
  Origin Access Control (OAC): CreateOriginAccessControl, GetOriginAccessControl,
                 GetOriginAccessControlConfig, ListOriginAccessControls,
                 UpdateOriginAccessControl, DeleteOriginAccessControl
  Tags: TagResource, UntagResource, ListTagsForResource
"""

import copy
import logging
import os
import random
import re
import string
from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement, tostring

from defusedxml.ElementTree import fromstring

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import AccountScopedDict, get_account_id, new_uuid

logger = logging.getLogger("cloudfront")

NS = "http://cloudfront.amazonaws.com/doc/2020-05-31/"

# ---------------------------------------------------------------------------
# Path regexes — note: _DIST_CFG_RE must be matched before _DIST_ID_RE
# ---------------------------------------------------------------------------
_DIST_RE     = re.compile(r"^/2020-05-31/distribution/?$")
_DIST_CFG_RE = re.compile(r"^/2020-05-31/distribution/([^/]+)/config$")
_DIST_ID_RE  = re.compile(r"^/2020-05-31/distribution/([^/]+)/?$")
_INV_RE      = re.compile(r"^/2020-05-31/distribution/([^/]+)/invalidation/?$")
_INV_ID_RE   = re.compile(r"^/2020-05-31/distribution/([^/]+)/invalidation/([^/]+)$")
_TAG_RE      = re.compile(r"^/2020-05-31/tagging/?$")

# OAC path regexes — note: _OAC_CFG_RE must be matched before _OAC_ID_RE
_OAC_RE      = re.compile(r"^/2020-05-31/origin-access-control/?$")
_OAC_CFG_RE  = re.compile(r"^/2020-05-31/origin-access-control/([^/]+)/config$")
_OAC_ID_RE   = re.compile(r"^/2020-05-31/origin-access-control/([^/]+)/?$")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
_distributions = AccountScopedDict()   # Id -> distribution record
_invalidations = AccountScopedDict()   # distribution_id -> [invalidation record, ...]
_tags = AccountScopedDict()            # arn -> [{"Key": ..., "Value": ...}]
_oacs = AccountScopedDict()            # Id -> OAC record


def reset():
    _distributions.clear()
    _invalidations.clear()
    _tags.clear()
    _oacs.clear()


def get_state():
    return copy.deepcopy({
        "distributions": _distributions,
        "invalidations": _invalidations,
        "tags": _tags,
        "oacs": _oacs,
    })


def restore_state(data):
    _distributions.update(data.get("distributions", {}))
    _invalidations.update(data.get("invalidations", {}))
    _tags.update(data.get("tags", {}))
    _oacs.update(data.get("oacs", {}))


_restored = load_state("cloudfront")
if _restored:
    restore_state(_restored)


# ---------------------------------------------------------------------------
# ID generators — real CloudFront uses 14-char uppercase alphanumeric IDs
# ---------------------------------------------------------------------------
_ID_CHARS = string.ascii_uppercase + string.digits


def _dist_id() -> str:
    return "E" + "".join(random.choices(_ID_CHARS, k=13))


def _inv_id() -> str:
    return "I" + "".join(random.choices(_ID_CHARS, k=13))


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def _xml_response(root_tag: str, builder_fn, status: int = 200, extra_headers: dict = None) -> tuple:
    root = Element(root_tag, xmlns=NS)
    builder_fn(root)
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    headers = {"Content-Type": "text/xml"}
    if extra_headers:
        headers.update(extra_headers)
    return status, headers, body


def _error(code: str, message: str, status: int) -> tuple:
    root = Element("ErrorResponse", xmlns=NS)
    err = SubElement(root, "Error")
    SubElement(err, "Code").text = code
    SubElement(err, "Message").text = message
    SubElement(root, "RequestId").text = new_uuid()
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="unicode").encode("utf-8")
    return status, {"Content-Type": "text/xml"}, body


def _find(el, tag):
    """Find direct child by local tag name, ignoring namespace prefix."""
    for child in el:
        local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if local == tag:
            return child
    return None


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


def _get_enabled(config_el) -> bool:
    """Extract Enabled boolean from a DistributionConfig XML element."""
    val = _text(config_el, "Enabled", "true")
    return val.strip().lower() != "false"


def _build_distribution_xml(parent, dist):
    """Append Distribution child elements to parent."""
    SubElement(parent, "Id").text = dist["Id"]
    SubElement(parent, "ARN").text = dist["ARN"]
    SubElement(parent, "Status").text = dist["Status"]
    SubElement(parent, "LastModifiedTime").text = dist["LastModifiedTime"]
    SubElement(parent, "InProgressInvalidationBatches").text = "0"
    SubElement(parent, "DomainName").text = dist["DomainName"]
    # Re-parse and embed the stored config XML
    config_el = fromstring(dist["config_xml"])
    config_el.tag = "DistributionConfig"
    parent.append(config_el)


_VALID_ORIGIN_TYPES = {"s3", "mediastore", "mediapackagev2", "lambda"}
_VALID_SIGNING_BEHAVIORS = {"always", "never", "no-override"}
_VALID_SIGNING_PROTOCOLS = {"sigv4"}


def _validate_oac_config(el):
    """Validate OAC config fields from a parsed XML element.

    Returns an error tuple (via _error()) on validation failure, or None on success.
    """
    name = _text(el, "Name")
    if not name:
        return _error("InvalidArgument", "Name is required.", 400)

    origin_type = _text(el, "OriginAccessControlOriginType")
    if origin_type not in _VALID_ORIGIN_TYPES:
        return _error("InvalidArgument", "Invalid OriginAccessControlOriginType value.", 400)

    signing_behavior = _text(el, "SigningBehavior")
    if signing_behavior not in _VALID_SIGNING_BEHAVIORS:
        return _error("InvalidArgument", "Invalid SigningBehavior value.", 400)

    signing_protocol = _text(el, "SigningProtocol")
    if signing_protocol not in _VALID_SIGNING_PROTOCOLS:
        return _error("InvalidArgument", "Invalid SigningProtocol value.", 400)

    return None


def _build_oac_xml(parent, oac):
    """Append OriginAccessControl child elements (Id + config) to parent."""
    SubElement(parent, "Id").text = oac["Id"]
    config_el = SubElement(parent, "OriginAccessControlConfig")
    SubElement(config_el, "Name").text = oac["Name"]
    SubElement(config_el, "Description").text = oac.get("Description", "")
    SubElement(config_el, "OriginAccessControlOriginType").text = oac["OriginAccessControlOriginType"]
    SubElement(config_el, "SigningBehavior").text = oac["SigningBehavior"]
    SubElement(config_el, "SigningProtocol").text = oac["SigningProtocol"]


def _build_oac_config_xml(parent, oac):
    """Append only OAC config fields directly to parent element."""
    SubElement(parent, "Name").text = oac["Name"]
    SubElement(parent, "Description").text = oac.get("Description", "")
    SubElement(parent, "OriginAccessControlOriginType").text = oac["OriginAccessControlOriginType"]
    SubElement(parent, "SigningBehavior").text = oac["SigningBehavior"]
    SubElement(parent, "SigningProtocol").text = oac["SigningProtocol"]


def _build_invalidation_xml(parent, inv):
    """Append Invalidation child elements to parent."""
    SubElement(parent, "Id").text = inv["Id"]
    SubElement(parent, "Status").text = inv["Status"]
    SubElement(parent, "CreateTime").text = inv["CreateTime"]
    batch = SubElement(parent, "InvalidationBatch")
    paths_el = SubElement(batch, "Paths")
    items = inv["InvalidationBatch"]["Paths"]["Items"]
    SubElement(paths_el, "Quantity").text = str(len(items))
    items_el = SubElement(paths_el, "Items")
    for p in items:
        SubElement(items_el, "Path").text = p
    SubElement(batch, "CallerReference").text = inv["InvalidationBatch"]["CallerReference"]


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    logger.debug("%s %s", method, path)

    m = _DIST_RE.match(path)
    if m:
        if method == "POST":
            return _create_distribution(headers, body)
        if method == "GET":
            return _list_distributions()

    m = _DIST_CFG_RE.match(path)
    if m:
        dist_id = m.group(1)
        if method == "GET":
            return _get_distribution_config(dist_id)
        if method == "PUT":
            return _update_distribution(dist_id, headers, body)

    m = _DIST_ID_RE.match(path)
    if m:
        dist_id = m.group(1)
        if method == "GET":
            return _get_distribution(dist_id)
        if method == "DELETE":
            return _delete_distribution(dist_id, headers)

    m = _INV_RE.match(path)
    if m:
        dist_id = m.group(1)
        if method == "POST":
            return _create_invalidation(dist_id, body)
        if method == "GET":
            return _list_invalidations(dist_id)

    m = _INV_ID_RE.match(path)
    if m:
        dist_id = m.group(1)
        inv_id = m.group(2)
        if method == "GET":
            return _get_invalidation(dist_id, inv_id)

    m = _TAG_RE.match(path)
    if m:
        resource = query_params.get("Resource", [""])[0] if isinstance(query_params.get("Resource"), list) else query_params.get("Resource", "")
        operation = query_params.get("Operation", [""])[0] if isinstance(query_params.get("Operation"), list) else query_params.get("Operation", "")
        if method == "GET":
            return _list_tags(resource)
        if method == "POST" and operation == "Tag":
            return _tag_resource(resource, body)
        if method == "POST" and operation == "Untag":
            return _untag_resource(resource, body)

    # OAC routes
    m = _OAC_RE.match(path)
    if m:
        if method == "POST":
            return _create_oac(headers, body)
        if method == "GET":
            return _list_oacs()

    m = _OAC_CFG_RE.match(path)
    if m:
        oac_id = m.group(1)
        if method == "GET":
            return _get_oac_config(oac_id)
        if method == "PUT":
            return _update_oac(oac_id, headers, body)

    m = _OAC_ID_RE.match(path)
    if m:
        oac_id = m.group(1)
        if method == "GET":
            return _get_oac(oac_id)
        if method == "DELETE":
            return _delete_oac(oac_id, headers)

    return _error("NoSuchResource", f"No route for {method} {path}", 404)


# ---------------------------------------------------------------------------
# Distribution handlers
# ---------------------------------------------------------------------------

def _create_distribution(headers, body):
    config_el = _parse_body(body)
    if config_el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)

    if not _text(config_el, "CallerReference"):
        return _error("InvalidArgument", "CallerReference is required.", 400)
    if _find(config_el, "Origins") is None:
        return _error("InvalidArgument", "Origins is required.", 400)
    if _find(config_el, "DefaultCacheBehavior") is None:
        return _error("InvalidArgument", "DefaultCacheBehavior is required.", 400)

    dist_id = _dist_id()
    etag = new_uuid()
    now = _now_iso()

    dist = {
        "Id": dist_id,
        "ARN": f"arn:aws:cloudfront::{get_account_id()}:distribution/{dist_id}",
        "Status": "Deployed",
        "DomainName": f"{dist_id}.cloudfront.net",
        "LastModifiedTime": now,
        "ETag": etag,
        "config_xml": tostring(config_el, encoding="unicode"),
        "enabled": _get_enabled(config_el),
    }
    _distributions[dist_id] = dist
    _invalidations[dist_id] = []

    logger.info("CreateDistribution id=%s", dist_id)

    def build(root):
        _build_distribution_xml(root, dist)

    return _xml_response("Distribution", build, status=201, extra_headers={
        "ETag": etag,
        "Location": f"/2020-05-31/distribution/{dist_id}",
    })


def _get_distribution(dist_id):
    dist = _distributions.get(dist_id)
    if not dist:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)

    def build(root):
        _build_distribution_xml(root, dist)

    return _xml_response("Distribution", build, extra_headers={"ETag": dist["ETag"]})


def _get_distribution_config(dist_id):
    dist = _distributions.get(dist_id)
    if not dist:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)

    config_el = fromstring(dist["config_xml"])
    config_el.tag = "DistributionConfig"
    config_el.set("xmlns", NS)
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(config_el, encoding="unicode").encode("utf-8")
    return 200, {"Content-Type": "text/xml", "ETag": dist["ETag"]}, body


def _list_distributions():
    items = list(_distributions.values())

    def build(root):
        SubElement(root, "Marker").text = ""
        SubElement(root, "MaxItems").text = "100"
        SubElement(root, "IsTruncated").text = "false"
        SubElement(root, "Quantity").text = str(len(items))
        if items:
            items_el = SubElement(root, "Items")
            for dist in items:
                ds = SubElement(items_el, "DistributionSummary")
                SubElement(ds, "Id").text = dist["Id"]
                SubElement(ds, "ARN").text = dist["ARN"]
                SubElement(ds, "Status").text = dist["Status"]
                SubElement(ds, "LastModifiedTime").text = dist["LastModifiedTime"]
                SubElement(ds, "DomainName").text = dist["DomainName"]
                SubElement(ds, "Enabled").text = str(dist["enabled"]).lower()
                SubElement(ds, "Comment").text = _text(fromstring(dist["config_xml"]), "Comment")

    return _xml_response("DistributionList", build)


def _update_distribution(dist_id, headers, body):
    dist = _distributions.get(dist_id)
    if not dist:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)

    if_match = headers.get("if-match", "")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != dist["ETag"]:
        return _error("PreconditionFailed", "The precondition given in one or more of the request-header fields evaluated to false.", 412)

    config_el = _parse_body(body)
    if config_el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)

    new_etag = new_uuid()
    dist["config_xml"] = tostring(config_el, encoding="unicode")
    dist["enabled"] = _get_enabled(config_el)
    dist["ETag"] = new_etag
    dist["LastModifiedTime"] = _now_iso()

    logger.info("UpdateDistribution id=%s", dist_id)

    def build(root):
        _build_distribution_xml(root, dist)

    return _xml_response("Distribution", build, extra_headers={"ETag": new_etag})


def _delete_distribution(dist_id, headers):
    dist = _distributions.get(dist_id)
    if not dist:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)

    if_match = headers.get("if-match", "")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != dist["ETag"]:
        return _error("PreconditionFailed", "The precondition given in one or more of the request-header fields evaluated to false.", 412)

    if dist["enabled"]:
        return _error("DistributionNotDisabled", "The distribution you are trying to delete has not been disabled.", 409)

    del _distributions[dist_id]
    _invalidations.pop(dist_id, None)

    logger.info("DeleteDistribution id=%s", dist_id)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Invalidation handlers
# ---------------------------------------------------------------------------

def _create_invalidation(dist_id, body):
    if dist_id not in _distributions:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)

    batch_el = _parse_body(body)
    if batch_el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)

    paths_el = _find(batch_el, "Paths")
    caller_ref = _text(batch_el, "CallerReference")

    path_items = []
    if paths_el is not None:
        items_el = _find(paths_el, "Items")
        if items_el is not None:
            for child in items_el:
                if child.text:
                    path_items.append(child.text)

    inv_id = _inv_id()
    now = _now_iso()
    inv = {
        "Id": inv_id,
        "Status": "Completed",
        "CreateTime": now,
        "InvalidationBatch": {
            "Paths": {"Quantity": len(path_items), "Items": path_items},
            "CallerReference": caller_ref,
        },
    }
    _invalidations[dist_id].append(inv)

    logger.info("CreateInvalidation dist=%s inv=%s paths=%d", dist_id, inv_id, len(path_items))

    def build(root):
        _build_invalidation_xml(root, inv)

    return _xml_response("Invalidation", build, status=201, extra_headers={
        "Location": f"/2020-05-31/distribution/{dist_id}/invalidation/{inv_id}",
    })


def _list_invalidations(dist_id):
    if dist_id not in _distributions:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)

    invs = _invalidations.get(dist_id, [])

    def build(root):
        SubElement(root, "Marker").text = ""
        SubElement(root, "MaxItems").text = "100"
        SubElement(root, "IsTruncated").text = "false"
        SubElement(root, "Quantity").text = str(len(invs))
        if invs:
            items_el = SubElement(root, "Items")
            for inv in invs:
                summary = SubElement(items_el, "InvalidationSummary")
                SubElement(summary, "Id").text = inv["Id"]
                SubElement(summary, "Status").text = inv["Status"]
                SubElement(summary, "CreateTime").text = inv["CreateTime"]

    return _xml_response("InvalidationList", build)


def _get_invalidation(dist_id, inv_id):
    if dist_id not in _distributions:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)

    invs = _invalidations.get(dist_id, [])
    inv = next((i for i in invs if i["Id"] == inv_id), None)
    if not inv:
        return _error("NoSuchInvalidation", "The specified invalidation does not exist.", 404)

    def build(root):
        _build_invalidation_xml(root, inv)

    return _xml_response("Invalidation", build)


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------

def _list_tags(resource_arn):
    tags = _tags.get(resource_arn, [])
    root = Element("Tags", xmlns=NS)
    items = SubElement(root, "Items")
    for t in tags:
        tag_el = SubElement(items, "Tag")
        SubElement(tag_el, "Key").text = t["Key"]
        SubElement(tag_el, "Value").text = t["Value"]
    body = tostring(root, encoding="unicode")
    return 200, {"Content-Type": "application/xml"}, f'<?xml version="1.0" encoding="UTF-8"?>\n{body}'.encode()


def _tag_resource(resource_arn, body):
    el = _parse_body(body)
    items_el = _find(el, "Items") or _find(el, "Tags")
    if items_el is None:
        items_el = el
    existing = {t["Key"]: t for t in _tags.get(resource_arn, [])}
    for tag_el in items_el:
        local = tag_el.tag.split("}")[-1] if "}" in tag_el.tag else tag_el.tag
        if local == "Tag":
            key = _text(tag_el, "Key")
            val = _text(tag_el, "Value")
            if key:
                existing[key] = {"Key": key, "Value": val}
    _tags[resource_arn] = list(existing.values())
    return 204, {}, b""


def _untag_resource(resource_arn, body):
    el = _parse_body(body)
    items_el = _find(el, "Items") or _find(el, "Keys")
    if items_el is None:
        items_el = el
    remove_keys = set()
    for child in items_el:
        local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if local == "Key":
            remove_keys.add(child.text or "")
    _tags[resource_arn] = [t for t in _tags.get(resource_arn, []) if t["Key"] not in remove_keys]
    return 204, {}, b""


# ---------------------------------------------------------------------------
# OAC handlers
# ---------------------------------------------------------------------------

def _create_oac(headers, body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)

    validation_err = _validate_oac_config(el)
    if validation_err is not None:
        return validation_err

    name = _text(el, "Name")

    # Check name uniqueness across existing OACs in the account
    for existing in _oacs.values():
        if existing["Name"] == name:
            return _error(
                "OriginAccessControlAlreadyExists",
                "An origin access control with this name already exists.",
                409,
            )

    oac_id = _dist_id()
    etag = new_uuid()

    oac = {
        "Id": oac_id,
        "Name": name,
        "Description": _text(el, "Description"),
        "OriginAccessControlOriginType": _text(el, "OriginAccessControlOriginType"),
        "SigningBehavior": _text(el, "SigningBehavior"),
        "SigningProtocol": _text(el, "SigningProtocol"),
        "ETag": etag,
    }
    _oacs[oac_id] = oac

    logger.info("CreateOriginAccessControl id=%s name=%s", oac_id, name)

    def build(root):
        _build_oac_xml(root, oac)

    return _xml_response("OriginAccessControl", build, status=201, extra_headers={
        "ETag": etag,
        "Location": f"/2020-05-31/origin-access-control/{oac_id}",
    })


def _get_oac(oac_id):
    oac = _oacs.get(oac_id)
    if not oac:
        return _error("NoSuchOriginAccessControl", "The specified origin access control does not exist.", 404)

    def build(root):
        _build_oac_xml(root, oac)

    return _xml_response("OriginAccessControl", build, extra_headers={"ETag": oac["ETag"]})


def _get_oac_config(oac_id):
    oac = _oacs.get(oac_id)
    if not oac:
        return _error("NoSuchOriginAccessControl", "The specified origin access control does not exist.", 404)

    def build(root):
        _build_oac_config_xml(root, oac)

    return _xml_response("OriginAccessControlConfig", build, extra_headers={"ETag": oac["ETag"]})


def _list_oacs():
    items = list(_oacs.values())

    def build(root):
        SubElement(root, "Marker").text = ""
        SubElement(root, "MaxItems").text = "100"
        SubElement(root, "IsTruncated").text = "false"
        SubElement(root, "Quantity").text = str(len(items))
        if items:
            items_el = SubElement(root, "Items")
            for oac in items:
                summary = SubElement(items_el, "OriginAccessControlSummary")
                SubElement(summary, "Id").text = oac["Id"]
                SubElement(summary, "Name").text = oac["Name"]
                SubElement(summary, "Description").text = oac.get("Description", "")
                SubElement(summary, "OriginAccessControlOriginType").text = oac["OriginAccessControlOriginType"]
                SubElement(summary, "SigningBehavior").text = oac["SigningBehavior"]
                SubElement(summary, "SigningProtocol").text = oac["SigningProtocol"]

    return _xml_response("OriginAccessControlList", build)


def _update_oac(oac_id, headers, body):
    oac = _oacs.get(oac_id)
    if not oac:
        return _error("NoSuchOriginAccessControl", "The specified origin access control does not exist.", 404)

    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != oac["ETag"]:
        return _error("PreconditionFailed", "The precondition given in one or more of the request-header fields evaluated to false.", 412)

    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)

    validation_err = _validate_oac_config(el)
    if validation_err is not None:
        return validation_err

    name = _text(el, "Name")

    # Check name uniqueness, excluding the OAC being updated
    for existing in _oacs.values():
        if existing["Id"] != oac_id and existing["Name"] == name:
            return _error(
                "OriginAccessControlAlreadyExists",
                "An origin access control with this name already exists.",
                409,
            )

    new_etag = new_uuid()
    oac["Name"] = name
    oac["Description"] = _text(el, "Description")
    oac["OriginAccessControlOriginType"] = _text(el, "OriginAccessControlOriginType")
    oac["SigningBehavior"] = _text(el, "SigningBehavior")
    oac["SigningProtocol"] = _text(el, "SigningProtocol")
    oac["ETag"] = new_etag

    logger.info("UpdateOriginAccessControl id=%s name=%s", oac_id, name)

    def build(root):
        _build_oac_xml(root, oac)

    return _xml_response("OriginAccessControl", build, extra_headers={"ETag": new_etag})


def _delete_oac(oac_id, headers):
    oac = _oacs.get(oac_id)
    if not oac:
        return _error("NoSuchOriginAccessControl", "The specified origin access control does not exist.", 404)

    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != oac["ETag"]:
        return _error("PreconditionFailed", "The precondition given in one or more of the request-header fields evaluated to false.", 412)

    del _oacs[oac_id]

    logger.info("DeleteOriginAccessControl id=%s", oac_id)
    return 204, {}, b""
