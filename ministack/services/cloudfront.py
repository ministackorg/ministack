"""
CloudFront Service Emulator.
REST/XML API — service credential scope: cloudfront.
Paths are under /2020-05-31/

Supports:
  Distributions: CreateDistribution, CreateDistributionWithTags (DistributionConfigWithTags),
                 GetDistribution, GetDistributionConfig,
                 ListDistributions, UpdateDistribution, DeleteDistribution
  Invalidations: CreateInvalidation, ListInvalidations, GetInvalidation
  Origin Access Control (OAC): CreateOriginAccessControl, GetOriginAccessControl,
                 GetOriginAccessControlConfig, ListOriginAccessControls,
                 UpdateOriginAccessControl, DeleteOriginAccessControl
  Functions (stub): CreateFunction, DeleteFunction, DescribeFunction, GetFunction,
                 ListFunctions, PublishFunction, UpdateFunction
  KeyValueStore: CreateKeyValueStore, DescribeKeyValueStore, ListKeyValueStores,
                 UpdateKeyValueStore, DeleteKeyValueStore
  Cache policies: CreateCachePolicy, GetCachePolicy, GetCachePolicyConfig,
                 UpdateCachePolicy, DeleteCachePolicy, ListDistributionsByCachePolicyId
  Origin request policies: CreateOriginRequestPolicy, GetOriginRequestPolicy,
                 GetOriginRequestPolicyConfig, UpdateOriginRequestPolicy,
                 DeleteOriginRequestPolicy, ListDistributionsByOriginRequestPolicyId
  Response headers policies: CreateResponseHeadersPolicy, GetResponseHeadersPolicy,
                 GetResponseHeadersPolicyConfig, UpdateResponseHeadersPolicy,
                 DeleteResponseHeadersPolicy, ListDistributionsByResponseHeadersPolicyId
  Tags: TagResource, UntagResource, ListTagsForResource
  SaaS Manager (multi-tenant distributions):
                 CreateConnectionGroup, GetConnectionGroup,
                 GetConnectionGroupByRoutingEndpoint, UpdateConnectionGroup,
                 DeleteConnectionGroup, ListConnectionGroups,
                 CreateDistributionTenant, GetDistributionTenant,
                 GetDistributionTenantByDomain, UpdateDistributionTenant,
                 DeleteDistributionTenant, ListDistributionTenants,
                 ListDistributionTenantsByCustomization,
                 AssociateDistributionTenantWebACL, DisassociateDistributionTenantWebACL,
                 CreateInvalidationForDistributionTenant,
                 GetInvalidationForDistributionTenant, ListInvalidationsForDistributionTenant,
                 VerifyDnsConfiguration, GetManagedCertificateDetails,
                 ListDomainConflicts, UpdateDomainAssociation,
                 ListDistributionsByConnectionMode
"""

import base64
import copy
import logging
import os
import random
import re
import string
from datetime import datetime, timezone
from urllib.parse import unquote
from xml.etree.ElementTree import Element, SubElement, tostring

from defusedxml.ElementTree import fromstring

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import AccountScopedDict, get_account_id, new_uuid

logger = logging.getLogger("cloudfront")

NS = "http://cloudfront.amazonaws.com/doc/2020-05-31/"

# ---------------------------------------------------------------------------
# Path regexes — note: _DIST_CFG_RE must be matched before _DIST_ID_RE
# ---------------------------------------------------------------------------
_DIST_RE = re.compile(r"^/2020-05-31/distribution/?$")
_DIST_CFG_RE = re.compile(r"^/2020-05-31/distribution/([^/]+)/config$")
_DIST_ID_RE = re.compile(r"^/2020-05-31/distribution/([^/]+)/?$")
_INV_RE = re.compile(r"^/2020-05-31/distribution/([^/]+)/invalidation/?$")
_INV_ID_RE = re.compile(r"^/2020-05-31/distribution/([^/]+)/invalidation/([^/]+)$")
_TAG_RE = re.compile(r"^/2020-05-31/tagging/?$")

# OAC path regexes — note: _OAC_CFG_RE must be matched before _OAC_ID_RE
_OAC_RE = re.compile(r"^/2020-05-31/origin-access-control/?$")
_OAC_CFG_RE = re.compile(r"^/2020-05-31/origin-access-control/([^/]+)/config$")
_OAC_ID_RE = re.compile(r"^/2020-05-31/origin-access-control/([^/]+)/?$")

_FUN_LIST_RE = re.compile(r"^/2020-05-31/function/?$")
_FUN_DESCRIBE_RE = re.compile(r"^/2020-05-31/function/([^/]+)/describe/?$")
_FUN_PUBLISH_RE = re.compile(r"^/2020-05-31/function/([^/]+)/publish/?$")
_FUN_NAME_RE = re.compile(r"^/2020-05-31/function/([^/]+)/?$")

_KVS_LIST_RE = re.compile(r"^/2020-05-31/key-value-store/?$")
_KVS_NAME_RE = re.compile(r"^/2020-05-31/key-value-store/([^/]+)/?$")

_CACHE_POLICY_RE = re.compile(r"^/2020-05-31/cache-policy/?$")
_CACHE_POLICY_CFG_RE = re.compile(r"^/2020-05-31/cache-policy/([^/]+)/config$")
_CACHE_POLICY_ID_RE = re.compile(r"^/2020-05-31/cache-policy/([^/]+)/?$")
_DIST_BY_CACHE_POLICY_RE = re.compile(r"^/2020-05-31/distributionsByCachePolicyId/([^/]+)/?$")

_ORP_RE = re.compile(r"^/2020-05-31/origin-request-policy/?$")
_ORP_CFG_RE = re.compile(r"^/2020-05-31/origin-request-policy/([^/]+)/config$")
_ORP_ID_RE = re.compile(r"^/2020-05-31/origin-request-policy/([^/]+)/?$")
_DIST_BY_ORP_RE = re.compile(r"^/2020-05-31/distributionsByOriginRequestPolicyId/([^/]+)/?$")

_RHP_RE = re.compile(r"^/2020-05-31/response-headers-policy/?$")
_RHP_CFG_RE = re.compile(r"^/2020-05-31/response-headers-policy/([^/]+)/config$")
_RHP_ID_RE = re.compile(r"^/2020-05-31/response-headers-policy/([^/]+)/?$")
_DIST_BY_RHP_RE = re.compile(r"^/2020-05-31/distributionsByResponseHeadersPolicyId/([^/]+)/?$")

# SaaS Manager path regexes. Get* identifiers may be an ARN — the ASGI layer
# hands us the percent-decoded path, so an ARN's embedded "/" lands in the
# identifier segment. The identifier regexes are greedy and MUST be matched
# after the tenant sub-resource regexes (web-acl, invalidation).
_CONN_GROUP_RE = re.compile(r"^/2020-05-31/connection-group/?$")
_CONN_GROUP_ID_RE = re.compile(r"^/2020-05-31/connection-group/(.+?)/?$")
_CONN_GROUPS_LIST_RE = re.compile(r"^/2020-05-31/connection-groups/?$")
_TENANT_RE = re.compile(r"^/2020-05-31/distribution-tenant/?$")
_TENANT_WEBACL_ASSOC_RE = re.compile(r"^/2020-05-31/distribution-tenant/([^/]+)/associate-web-acl/?$")
_TENANT_WEBACL_DISASSOC_RE = re.compile(r"^/2020-05-31/distribution-tenant/([^/]+)/disassociate-web-acl/?$")
_TENANT_INV_RE = re.compile(r"^/2020-05-31/distribution-tenant/([^/]+)/invalidation/?$")
_TENANT_INV_ID_RE = re.compile(r"^/2020-05-31/distribution-tenant/([^/]+)/invalidation/([^/]+)$")
_TENANT_ID_RE = re.compile(r"^/2020-05-31/distribution-tenant/(.+?)/?$")
_TENANTS_LIST_RE = re.compile(r"^/2020-05-31/distribution-tenants/?$")
_TENANTS_BY_CUSTOMIZATION_RE = re.compile(r"^/2020-05-31/distribution-tenants-by-customization/?$")
_MANAGED_CERT_RE = re.compile(r"^/2020-05-31/managed-certificate/(.+?)/?$")
_VERIFY_DNS_RE = re.compile(r"^/2020-05-31/verify-dns-configuration/?$")
_DOMAIN_CONFLICTS_RE = re.compile(r"^/2020-05-31/domain-conflicts/?$")
_DOMAIN_ASSOCIATION_RE = re.compile(r"^/2020-05-31/domain-association/?$")
_DIST_BY_CONN_MODE_RE = re.compile(r"^/2020-05-31/distributionsByConnectionMode/([^/]+)/?$")

# ---------------------------------------------------------------------------
# Read-only surface for resource families MiniStack does not yet persist.
# These return AWS-shaped empty collections so the SDK gets a valid response
# instead of a routing fall-through. Shapes verified against botocore
# cloudfront service-2.json (2020-05-31): each list container's XML root is
# the payload member's locationName, MaxItems defaults to 100, an empty list
# omits Items, and NextMarker is omitted when there is no next page.
# ---------------------------------------------------------------------------
_KEY_GROUP_LIST_RE = re.compile(r"^/2020-05-31/key-group/?$")
_PUBLIC_KEY_LIST_RE = re.compile(r"^/2020-05-31/public-key/?$")
_FLE_LIST_RE = re.compile(r"^/2020-05-31/field-level-encryption/?$")
_FLE_PROFILE_LIST_RE = re.compile(r"^/2020-05-31/field-level-encryption-profile/?$")
_CDP_LIST_RE = re.compile(r"^/2020-05-31/continuous-deployment-policy/?$")
_OAI_LIST_RE = re.compile(r"^/2020-05-31/origin-access-identity/cloudfront/?$")
_STREAMING_DIST_LIST_RE = re.compile(r"^/2020-05-31/streaming-distribution/?$")
_VPC_ORIGIN_LIST_RE = re.compile(r"^/2020-05-31/vpc-origin/?$")
_REALTIME_LOG_LIST_RE = re.compile(r"^/2020-05-31/realtime-log-config/?$")
_ANYCAST_IP_LIST_RE = re.compile(r"^/2020-05-31/anycast-ip-list/?$")
_MONITORING_SUB_RE = re.compile(r"^/2020-05-31/distributions/([^/]+)/monitoring-subscription/?$")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
_distributions = AccountScopedDict()  # Id -> distribution record
_invalidations = AccountScopedDict()  # distribution_id -> [invalidation record, ...]
_tags = AccountScopedDict()  # arn -> [{"Key": ..., "Value": ...}]
_oacs = AccountScopedDict()  # Id -> OAC record
_functions = AccountScopedDict()  # Name -> function record (CloudFront Functions API)
_kvstores = AccountScopedDict()  # Name -> KVS record
_cache_policies = AccountScopedDict()  # Id -> cache policy record
_origin_request_policies = AccountScopedDict()  # Id -> origin request policy record
_response_headers_policies = AccountScopedDict()  # Id -> response headers policy record
_connection_groups = AccountScopedDict()  # Id -> connection group record (SaaS Manager)
_distribution_tenants = AccountScopedDict()  # Id -> distribution tenant record (SaaS Manager)
_tenant_invalidations = AccountScopedDict()  # tenant_id -> [invalidation record, ...]


def reset():
    _distributions.clear()
    _invalidations.clear()
    _tags.clear()
    _oacs.clear()
    _functions.clear()
    _kvstores.clear()
    _cache_policies.clear()
    _origin_request_policies.clear()
    _response_headers_policies.clear()
    _connection_groups.clear()
    _distribution_tenants.clear()
    _tenant_invalidations.clear()


def get_state():
    return copy.deepcopy(
        {
            "distributions": _distributions,
            "invalidations": _invalidations,
            "tags": _tags,
            "oacs": _oacs,
            "functions": _functions,
            "kvstores": _kvstores,
            "cache_policies": _cache_policies,
            "origin_request_policies": _origin_request_policies,
            "response_headers_policies": _response_headers_policies,
            "connection_groups": _connection_groups,
            "distribution_tenants": _distribution_tenants,
            "tenant_invalidations": _tenant_invalidations,
        }
    )


def restore_state(data):
    _distributions.update(data.get("distributions", {}))
    _invalidations.update(data.get("invalidations", {}))
    _tags.update(data.get("tags", {}))
    _oacs.update(data.get("oacs", {}))
    _functions.update(data.get("functions", {}))
    _kvstores.update(data.get("kvstores", {}))
    _cache_policies.update(data.get("cache_policies", {}))
    _origin_request_policies.update(data.get("origin_request_policies", {}))
    _response_headers_policies.update(data.get("response_headers_policies", {}))
    _connection_groups.update(data.get("connection_groups", {}))
    _distribution_tenants.update(data.get("distribution_tenants", {}))
    _tenant_invalidations.update(data.get("tenant_invalidations", {}))


try:
    _restored = load_state("cloudfront")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging

    logging.getLogger(__name__).exception("Failed to restore persisted state; continuing with fresh store")


# ---------------------------------------------------------------------------
# ID generators — real CloudFront uses 14-char uppercase alphanumeric IDs
# ---------------------------------------------------------------------------
_ID_CHARS = string.ascii_uppercase + string.digits


def _dist_id() -> str:
    return "E" + "".join(random.choices(_ID_CHARS, k=13))


def _inv_id() -> str:
    return "I" + "".join(random.choices(_ID_CHARS, k=13))


# Real SaaS Manager resource IDs are dt_/cg_-prefixed KSUIDs (27 base62 chars).
_KSUID_CHARS = string.ascii_letters + string.digits


def _tenant_id() -> str:
    return "dt_" + "".join(random.choices(_KSUID_CHARS, k=27))


def _conn_group_id() -> str:
    return "cg_" + "".join(random.choices(_KSUID_CHARS, k=27))


def _routing_endpoint() -> str:
    return "d" + "".join(random.choices(string.ascii_lowercase + string.digits, k=13)) + ".cloudfront.net"


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


def _local_tag_name(el) -> str:
    t = el.tag
    return t.split("}")[-1] if "}" in t else t


def _add_xml_block(parent, source_el):
    block = SubElement(parent, _local_tag_name(source_el))
    block.text = source_el.text
    block.attrib.update(source_el.attrib)
    for child in source_el:
        _add_xml_block(block, child)
    return block


def _add_config_block(parent, config_el, tag):
    child = _find(config_el, tag)
    if child is not None:
        _add_xml_block(parent, child)


# Minimal empty XML for each REQUIRED-block field on DistributionSummary.
# Real AWS emits these even when the distribution was created with nothing
# in them; SDKs that strict-parse (Go v2, Java v2) reject responses that
# omit required members.
_EMPTY_SUMMARY_BLOCKS = {
    "Aliases": "<Aliases><Quantity>0</Quantity></Aliases>",
    "Origins": "<Origins><Quantity>0</Quantity></Origins>",
    "CacheBehaviors": "<CacheBehaviors><Quantity>0</Quantity></CacheBehaviors>",
    "CustomErrorResponses": "<CustomErrorResponses><Quantity>0</Quantity></CustomErrorResponses>",
    "ViewerCertificate": "<ViewerCertificate><CloudFrontDefaultCertificate>true</CloudFrontDefaultCertificate><MinimumProtocolVersion>TLSv1</MinimumProtocolVersion><CertificateSource>cloudfront</CertificateSource></ViewerCertificate>",
    "Restrictions": "<Restrictions><GeoRestriction><RestrictionType>none</RestrictionType><Quantity>0</Quantity></GeoRestriction></Restrictions>",
    "DefaultCacheBehavior": "<DefaultCacheBehavior><TargetOriginId></TargetOriginId><ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy></DefaultCacheBehavior>",
}


def _add_config_block_with_default(parent, config_el, tag):
    """Like `_add_config_block` but emits a minimal-but-valid empty block
    when the source config doesn't contain `tag` — keeps DistributionSummary
    schema-complete for strict-parsing SDKs."""
    child = _find(config_el, tag)
    if child is not None:
        _add_xml_block(parent, child)
    elif tag in _EMPTY_SUMMARY_BLOCKS:
        _add_xml_block(parent, fromstring(_EMPTY_SUMMARY_BLOCKS[tag]))


def _unwrap_distribution_create_xml(root_el):
    """Return ``(DistributionConfig element, Tags element or None)``.

    Terraform / boto3 ``CreateDistributionWithTags`` posts a
    ``DistributionConfigWithTags`` root; ``CreateDistribution`` uses
    ``DistributionConfig`` directly.
    """
    if root_el is None:
        return None, None
    if _local_tag_name(root_el) == "DistributionConfigWithTags":
        cfg = _find(root_el, "DistributionConfig")
        tags_el = _find(root_el, "Tags")
        return cfg, tags_el
    return root_el, None


def _ingest_distribution_tags_from_xml(dist_arn: str, tags_el):
    """Apply tag Items from CreateDistributionWithTags onto ``_tags``."""
    if tags_el is None:
        return
    items_el = _find(tags_el, "Items") or tags_el
    existing = {t["Key"]: t for t in _tags.get(dist_arn, [])}
    for tag_el in items_el:
        local = _local_tag_name(tag_el)
        if local == "Tag":
            key = _text(tag_el, "Key")
            val = _text(tag_el, "Value")
            if key:
                existing[key] = {"Key": key, "Value": val}
    _tags[dist_arn] = list(existing.values())


def _get_enabled(config_el) -> bool:
    """Extract Enabled boolean from a DistributionConfig XML element."""
    val = _text(config_el, "Enabled", "true")
    return val.strip().lower() != "false"


def _ensure_distribution_config_sdk_compat(config_el):
    """Patch DistributionConfig XML so hashicorp/aws CloudFront flatten does not nil-deref.

    terraform-provider-aws (e.g. v6.42) does ``OriginGroups.Quantity`` without checking
    ``OriginGroups``; real AWS returns ``<OriginGroups><Quantity>0</Quantity></OriginGroups>``
    even when empty. Requests often omit that block.
    """
    if config_el is None:
        return
    if _find(config_el, "OriginGroups") is None:
        og = SubElement(config_el, "OriginGroups")
        SubElement(og, "Quantity").text = "0"


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
    _ensure_distribution_config_sdk_compat(config_el)
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
# CloudFront Functions (Terraform aws_cloudfront_function / distribution associations)
# ---------------------------------------------------------------------------


def _qval(query_params, key, default=""):
    v = query_params.get(key, default)
    if isinstance(v, list):
        return v[0] if v else default
    return v if v is not None else default


def _func_arn(name: str) -> str:
    return f"arn:aws:cloudfront::{get_account_id()}:function/{name}"


def _kvs_arn(name: str) -> str:
    return f"arn:aws:cloudfront::{get_account_id()}:key-value-store/{name}"


def _resolve_taggable_cloudfront_arn(arn: str):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None, _error("InvalidArgument", f"Invalid resource ARN: {arn}", 400)

    if (
        spec.partition != "aws"
        or spec.service != "cloudfront"
        or spec.region
        or spec.account_id != get_account_id()
    ):
        return None, _error("InvalidArgument", f"Invalid resource ARN: {arn}", 400)

    resource_type, sep, name = spec.resource.partition("/")
    if not sep or not name:
        return None, _error("InvalidArgument", f"Invalid resource ARN: {arn}", 400)

    resources = {
        "distribution": (_distributions, "NoSuchDistribution", "The specified distribution does not exist.", "ARN"),
        "function": (_functions, "NoSuchFunctionExists", "The specified function does not exist.", "arn"),
        "key-value-store": (_kvstores, "EntityNotFound", f"The key value store {name} was not found.", "ARN"),
        "distribution-tenant": (_distribution_tenants, "EntityNotFound", "The distribution tenant was not found.", "Arn"),
        "connection-group": (_connection_groups, "EntityNotFound", "The connection group was not found.", "Arn"),
    }
    entry = resources.get(resource_type)
    if not entry:
        return None, _error("InvalidArgument", f"Invalid resource ARN: {arn}", 400)

    store, code, message, arn_key = entry
    record = store.get(name)
    if not record or record.get(arn_key) != arn:
        return None, _error(code, message, 404)
    return arn, None


def _function_summary_builder(fn: dict, stage: str, status: str, last_modified: str):
    def build(root):
        fc = SubElement(root, "FunctionConfig")
        SubElement(fc, "Comment").text = fn.get("comment", "")
        kvs_arns = fn.get("kvs_arns", [])
        kvs = SubElement(fc, "KeyValueStoreAssociations")
        SubElement(kvs, "Quantity").text = str(len(kvs_arns))
        items_el = SubElement(kvs, "Items")
        for arn in kvs_arns:
            assoc = SubElement(items_el, "KeyValueStoreAssociation")
            SubElement(assoc, "KeyValueStoreARN").text = arn
        SubElement(fc, "Runtime").text = fn["runtime"]
        md = SubElement(root, "FunctionMetadata")
        SubElement(md, "CreatedTime").text = fn["created"]
        SubElement(md, "FunctionARN").text = fn["arn"]
        SubElement(md, "LastModifiedTime").text = last_modified
        SubElement(md, "Stage").text = stage
        SubElement(root, "Name").text = fn["name"]
        SubElement(root, "Status").text = status

    return build


def _cf_parse_function_config(cfg_el):
    if cfg_el is None:
        return None, _error("InvalidArgument", "FunctionConfig is required.", 400)
    comment = _text(cfg_el, "Comment")
    runtime = _text(cfg_el, "Runtime")
    if not runtime:
        return None, _error("InvalidArgument", "Runtime is required.", 400)
    kvs_arns = []
    kvs_el = _find(cfg_el, "KeyValueStoreAssociations")
    if kvs_el is not None:
        items_el = _find(kvs_el, "Items")
        if items_el is not None:
            for child in items_el:
                if _local_tag_name(child) == "KeyValueStoreAssociation":
                    arn = _text(child, "KeyValueStoreARN")
                    if arn:
                        kvs_arns.append(arn)
    return {"comment": comment, "runtime": runtime, "kvs_arns": kvs_arns}, None


def _cf_decode_function_code(code_b64: str):
    if not code_b64:
        return None, _error("InvalidArgument", "FunctionCode is required.", 400)
    try:
        return base64.b64decode(code_b64.encode("ascii"), validate=True), None
    except Exception:
        return None, _error("InvalidArgument", "FunctionCode is not valid base64.", 400)


def _cf_create_function(headers, body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    name = _text(el, "Name")
    if not name:
        return _error("InvalidArgument", "Name is required.", 400)
    if name in _functions:
        return _error("FunctionAlreadyExists", "A function with the same name already exists in this account.", 409)

    cfg_el = _find(el, "FunctionConfig")
    cfg, err = _cf_parse_function_config(cfg_el)
    if err is not None:
        return err
    code, err = _cf_decode_function_code(_text(el, "FunctionCode"))
    if err is not None:
        return err

    now = _now_iso()
    dev_etag = new_uuid()
    fn = {
        "name": name,
        "arn": _func_arn(name),
        "comment": cfg["comment"],
        "runtime": cfg["runtime"],
        "kvs_arns": cfg["kvs_arns"],
        "code": code,
        "created": now,
        "last_modified_dev": now,
        "last_modified_live": None,
        "dev_etag": dev_etag,
        "live_etag": None,
    }
    _functions[name] = fn
    logger.info("CreateFunction name=%s", name)

    return _xml_response(
        "FunctionSummary",
        _function_summary_builder(fn, "DEVELOPMENT", "UNPUBLISHED", fn["last_modified_dev"]),
        status=201,
        extra_headers={
            "ETag": dev_etag,
            "Location": f"/2020-05-31/function/{name}",
        },
    )


def _cf_list_functions(query_params):
    stage_filter = _qval(query_params, "Stage", "")
    summaries = []
    for fn in _functions.values():
        if stage_filter in ("", "DEVELOPMENT"):
            summaries.append((fn, "DEVELOPMENT", "UNPUBLISHED", fn["last_modified_dev"]))
        if stage_filter in ("", "LIVE") and fn["live_etag"]:
            summaries.append((fn, "LIVE", "DEPLOYED", fn["last_modified_live"] or fn["last_modified_dev"]))

    def build(root):
        SubElement(root, "MaxItems").text = "100"
        SubElement(root, "NextMarker").text = ""
        SubElement(root, "Quantity").text = str(len(summaries))
        if not summaries:
            return
        items_el = SubElement(root, "Items")
        for fn, stage, status, lm in summaries:
            fs = SubElement(items_el, "FunctionSummary")
            _function_summary_builder(fn, stage, status, lm)(fs)

    return _xml_response("FunctionList", build)


def _cf_describe_function(name: str, stage: str):
    fn = _functions.get(name)
    if not fn:
        return _error("NoSuchFunctionExists", "The specified function does not exist.", 404)
    if stage == "LIVE":
        if not fn["live_etag"]:
            return _error("NoSuchFunctionExists", "The specified function does not exist.", 404)
        etag = fn["live_etag"]
        lm = fn["last_modified_live"] or fn["last_modified_dev"]
        st = "DEPLOYED"
    elif stage == "DEVELOPMENT":
        etag = fn["dev_etag"]
        lm = fn["last_modified_dev"]
        st = "UNPUBLISHED"
    else:
        return _error("InvalidArgument", "Invalid Stage value.", 400)

    return _xml_response(
        "FunctionSummary",
        _function_summary_builder(fn, stage, st, lm),
        extra_headers={"ETag": etag},
    )


def _cf_get_function(name: str, stage: str):
    fn = _functions.get(name)
    if not fn:
        return _error("NoSuchFunctionExists", "The specified function does not exist.", 404)
    if stage == "LIVE":
        if not fn["live_etag"]:
            return _error("NoSuchFunctionExists", "The specified function does not exist.", 404)
        etag = fn["live_etag"]
        code = fn["code"]
    elif stage == "DEVELOPMENT":
        etag = fn["dev_etag"]
        code = fn["code"]
    else:
        return _error("InvalidArgument", "Invalid Stage value.", 400)

    return 200, {"Content-Type": "application/javascript", "ETag": etag}, code


def _cf_publish_function(name: str, headers):
    fn = _functions.get(name)
    if not fn:
        return _error("NoSuchFunctionExists", "The specified function does not exist.", 404)
    if_match = headers.get("if-match", "")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != fn["dev_etag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )

    now = _now_iso()
    fn["live_etag"] = new_uuid()
    fn["last_modified_live"] = now
    logger.info("PublishFunction name=%s", name)

    lm = fn["last_modified_live"]
    return _xml_response(
        "FunctionSummary",
        _function_summary_builder(fn, "LIVE", "DEPLOYED", lm),
        extra_headers={"ETag": fn["live_etag"]},
    )


def _cf_update_function(name: str, headers, body):
    fn = _functions.get(name)
    if not fn:
        return _error("NoSuchFunctionExists", "The specified function does not exist.", 404)
    if_match = headers.get("if-match", "")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != fn["dev_etag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )

    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    cfg_el = _find(el, "FunctionConfig")
    cfg, err = _cf_parse_function_config(cfg_el)
    if err is not None:
        return err
    code, err = _cf_decode_function_code(_text(el, "FunctionCode"))
    if err is not None:
        return err

    now = _now_iso()
    fn["comment"] = cfg["comment"]
    fn["runtime"] = cfg["runtime"]
    fn["kvs_arns"] = cfg["kvs_arns"]
    fn["code"] = code
    fn["last_modified_dev"] = now
    fn["dev_etag"] = new_uuid()
    fn["live_etag"] = None
    fn["last_modified_live"] = None
    logger.info("UpdateFunction name=%s", name)

    return _xml_response(
        "FunctionSummary",
        _function_summary_builder(fn, "DEVELOPMENT", "UNPUBLISHED", fn["last_modified_dev"]),
        extra_headers={"ETag": fn["dev_etag"]},
    )


def _cf_delete_function(name: str, headers):
    fn = _functions.get(name)
    if not fn:
        return _error("NoSuchFunctionExists", "The specified function does not exist.", 404)
    if_match = headers.get("if-match", "")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != fn["dev_etag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )

    del _functions[name]
    logger.info("DeleteFunction name=%s", name)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Cache policies (Terraform aws_cloudfront_cache_policy)
# Shapes verified against botocore cloudfront service-2.json (2020-05-31).
# ---------------------------------------------------------------------------

_CACHE_HEADER_BEHAVIORS = {"none", "whitelist"}
_CACHE_COOKIE_BEHAVIORS = {"none", "whitelist", "allExcept", "all"}
_CACHE_QUERYSTRING_BEHAVIORS = {"none", "whitelist", "allExcept", "all"}


def _parse_name_items(block_el, names_tag):
    """Pull <Items><Name>..</Name></Items> out of a Headers/Cookies/QueryStrings block."""
    names = _find(block_el, names_tag) if block_el is not None else None
    items = []
    if names is not None:
        items_el = _find(names, "Items")
        if items_el is not None:
            for child in items_el:
                local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if local == "Name":
                    items.append(child.text or "")
    return items


def _parse_cache_policy_config(el):
    """Parse a <CachePolicyConfig> element into a stored dict, or return an _error tuple."""
    name = _text(el, "Name")
    if not name:
        return None, _error("InvalidArgument", "The cache policy name is required.", 400)

    min_ttl_el = _find(el, "MinTTL")
    if min_ttl_el is None or not (min_ttl_el.text or "").strip():
        return None, _error("InvalidArgument", "The MinTTL value is required.", 400)
    try:
        min_ttl = int(min_ttl_el.text)
    except (TypeError, ValueError):
        return None, _error("InvalidArgument", "The MinTTL value is not valid.", 400)

    cfg = {"Name": name, "Comment": _text(el, "Comment"), "MinTTL": min_ttl}
    for opt in ("DefaultTTL", "MaxTTL"):
        opt_el = _find(el, opt)
        if opt_el is not None and (opt_el.text or "").strip():
            try:
                cfg[opt] = int(opt_el.text)
            except (TypeError, ValueError):
                return None, _error("InvalidArgument", f"The {opt} value is not valid.", 400)

    params_el = _find(el, "ParametersInCacheKeyAndForwardedToOrigin")
    if params_el is not None:
        headers_cfg = _find(params_el, "HeadersConfig")
        cookies_cfg = _find(params_el, "CookiesConfig")
        qs_cfg = _find(params_el, "QueryStringsConfig")
        if headers_cfg is None or cookies_cfg is None or qs_cfg is None:
            return None, _error(
                "InvalidArgument",
                "HeadersConfig, CookiesConfig, and QueryStringsConfig are required.",
                400,
            )
        header_behavior = _text(headers_cfg, "HeaderBehavior")
        cookie_behavior = _text(cookies_cfg, "CookieBehavior")
        qs_behavior = _text(qs_cfg, "QueryStringBehavior")
        if header_behavior not in _CACHE_HEADER_BEHAVIORS:
            return None, _error("InvalidArgument", "Invalid HeaderBehavior value.", 400)
        if cookie_behavior not in _CACHE_COOKIE_BEHAVIORS:
            return None, _error("InvalidArgument", "Invalid CookieBehavior value.", 400)
        if qs_behavior not in _CACHE_QUERYSTRING_BEHAVIORS:
            return None, _error("InvalidArgument", "Invalid QueryStringBehavior value.", 400)
        gzip_el = _find(params_el, "EnableAcceptEncodingGzip")
        if gzip_el is None:
            return None, _error("InvalidArgument", "EnableAcceptEncodingGzip is required.", 400)
        cfg["Parameters"] = {
            "EnableAcceptEncodingGzip": (gzip_el.text or "").strip().lower() == "true",
            "EnableAcceptEncodingBrotli": _text(params_el, "EnableAcceptEncodingBrotli").strip().lower() == "true",
            "HeaderBehavior": header_behavior,
            "Headers": _parse_name_items(headers_cfg, "Headers"),
            "CookieBehavior": cookie_behavior,
            "Cookies": _parse_name_items(cookies_cfg, "Cookies"),
            "QueryStringBehavior": qs_behavior,
            "QueryStrings": _parse_name_items(qs_cfg, "QueryStrings"),
        }
    return cfg, None


def _build_names_block(parent, names_tag, items):
    block = SubElement(parent, names_tag)
    SubElement(block, "Quantity").text = str(len(items))
    if items:
        items_el = SubElement(block, "Items")
        for it in items:
            SubElement(items_el, "Name").text = it


def _build_cache_policy_config_xml(parent, cfg):
    SubElement(parent, "Comment").text = cfg.get("Comment", "")
    SubElement(parent, "Name").text = cfg["Name"]
    # AWS fills the documented defaults when the caller omits these.
    SubElement(parent, "DefaultTTL").text = str(cfg.get("DefaultTTL", 86400))
    SubElement(parent, "MaxTTL").text = str(cfg.get("MaxTTL", 31536000))
    SubElement(parent, "MinTTL").text = str(cfg["MinTTL"])
    params = cfg.get("Parameters")
    if params is not None:
        p_el = SubElement(parent, "ParametersInCacheKeyAndForwardedToOrigin")
        SubElement(p_el, "EnableAcceptEncodingGzip").text = "true" if params["EnableAcceptEncodingGzip"] else "false"
        SubElement(p_el, "EnableAcceptEncodingBrotli").text = "true" if params["EnableAcceptEncodingBrotli"] else "false"
        hc = SubElement(p_el, "HeadersConfig")
        SubElement(hc, "HeaderBehavior").text = params["HeaderBehavior"]
        _build_names_block(hc, "Headers", params["Headers"])
        cc = SubElement(p_el, "CookiesConfig")
        SubElement(cc, "CookieBehavior").text = params["CookieBehavior"]
        _build_names_block(cc, "Cookies", params["Cookies"])
        qc = SubElement(p_el, "QueryStringsConfig")
        SubElement(qc, "QueryStringBehavior").text = params["QueryStringBehavior"]
        _build_names_block(qc, "QueryStrings", params["QueryStrings"])


def _build_cache_policy_xml(parent, policy):
    SubElement(parent, "Id").text = policy["Id"]
    SubElement(parent, "LastModifiedTime").text = policy["LastModifiedTime"]
    cfg_el = SubElement(parent, "CachePolicyConfig")
    _build_cache_policy_config_xml(cfg_el, policy["Config"])


def _value_contains(obj, target):
    """Best-effort recursive search for a policy Id anywhere in a distribution record."""
    if isinstance(obj, str):
        return obj == target
    if isinstance(obj, dict):
        return any(_value_contains(v, target) for v in obj.values())
    if isinstance(obj, (list, tuple)):
        return any(_value_contains(v, target) for v in obj)
    return False


def _distributions_using_cache_policy(policy_id):
    return [d.get("Id", "") for d in _distributions.values() if _value_contains(d, policy_id)]


def _create_cache_policy(body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    cfg, err = _parse_cache_policy_config(el)
    if err is not None:
        return err
    for existing in _cache_policies.values():
        if existing["Config"]["Name"] == cfg["Name"]:
            return _error("CachePolicyAlreadyExists", "A cache policy with the same name already exists.", 409)
    policy_id = new_uuid()
    etag = new_uuid()
    policy = {"Id": policy_id, "ETag": etag, "LastModifiedTime": _now_iso(), "Config": cfg}
    _cache_policies[policy_id] = policy
    logger.info("CreateCachePolicy id=%s name=%s", policy_id, cfg["Name"])

    def build(root):
        _build_cache_policy_xml(root, policy)

    return _xml_response(
        "CachePolicy", build, status=201,
        extra_headers={"ETag": etag, "Location": f"/2020-05-31/cache-policy/{policy_id}"},
    )


def _get_cache_policy(policy_id):
    policy = _cache_policies.get(policy_id)
    if not policy:
        return _error("NoSuchCachePolicy", "The cache policy does not exist.", 404)

    def build(root):
        _build_cache_policy_xml(root, policy)

    return _xml_response("CachePolicy", build, extra_headers={"ETag": policy["ETag"]})


def _get_cache_policy_config(policy_id):
    policy = _cache_policies.get(policy_id)
    if not policy:
        return _error("NoSuchCachePolicy", "The cache policy does not exist.", 404)

    def build(root):
        _build_cache_policy_config_xml(root, policy["Config"])

    return _xml_response("CachePolicyConfig", build, extra_headers={"ETag": policy["ETag"]})


def _update_cache_policy(policy_id, headers, body):
    policy = _cache_policies.get(policy_id)
    if not policy:
        return _error("NoSuchCachePolicy", "The cache policy does not exist.", 404)
    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != policy["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    cfg, err = _parse_cache_policy_config(el)
    if err is not None:
        return err
    for existing in _cache_policies.values():
        if existing["Id"] != policy_id and existing["Config"]["Name"] == cfg["Name"]:
            return _error("CachePolicyAlreadyExists", "A cache policy with the same name already exists.", 409)
    new_etag = new_uuid()
    policy["Config"] = cfg
    policy["ETag"] = new_etag
    policy["LastModifiedTime"] = _now_iso()
    logger.info("UpdateCachePolicy id=%s name=%s", policy_id, cfg["Name"])

    def build(root):
        _build_cache_policy_xml(root, policy)

    return _xml_response("CachePolicy", build, extra_headers={"ETag": new_etag})


def _delete_cache_policy(policy_id, headers):
    policy = _cache_policies.get(policy_id)
    if not policy:
        return _error("NoSuchCachePolicy", "The cache policy does not exist.", 404)
    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != policy["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )
    if _distributions_using_cache_policy(policy_id):
        return _error(
            "CachePolicyInUse",
            "The cache policy cannot be deleted because it is attached to one or more cache behaviors.",
            409,
        )
    del _cache_policies[policy_id]
    logger.info("DeleteCachePolicy id=%s", policy_id)
    return 204, {}, b""


def _list_distributions_by_cache_policy(policy_id):
    if not _cache_policies.get(policy_id):
        return _error("NoSuchCachePolicy", "The cache policy does not exist.", 404)
    dist_ids = _distributions_using_cache_policy(policy_id)

    def build(root):
        SubElement(root, "Marker").text = ""
        SubElement(root, "MaxItems").text = "100"
        SubElement(root, "IsTruncated").text = "false"
        SubElement(root, "Quantity").text = str(len(dist_ids))
        if dist_ids:
            items_el = SubElement(root, "Items")
            for did in dist_ids:
                SubElement(items_el, "DistributionId").text = did

    return _xml_response("DistributionIdList", build)


# ---------------------------------------------------------------------------
# Origin request policies (aws_cloudfront_origin_request_policy) and response
# headers policies (aws_cloudfront_response_headers_policy) — #1249.
# Shapes verified against botocore cloudfront service-2.json (2020-05-31).
# ---------------------------------------------------------------------------


def _xbool(el, tag, default=None):
    """Parse a boolean child element; return ``default`` when it is absent."""
    child = _find(el, tag)
    if child is None:
        return default
    return (child.text or "").strip().lower() == "true"


def _opt_text(el, tag):
    """Return a child element's text, or None when the element is absent."""
    child = _find(el, tag)
    return (child.text or "") if child is not None else None


def _bstr(value):
    return "true" if value else "false"


def _fmt_rate(x):
    return "%g" % x


def _parse_str_list_block(cfg_el, block_tag, item_tag):
    """Parse ``<block_tag><Items><item_tag>..</item_tag></Items></block_tag>`` to a list."""
    block = _find(cfg_el, block_tag)
    items = []
    if block is not None:
        items_el = _find(block, "Items")
        if items_el is not None:
            for child in items_el:
                local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if local == item_tag:
                    items.append(child.text or "")
    return items


def _build_str_list_block(parent, block_tag, item_tag, items):
    block = SubElement(parent, block_tag)
    SubElement(block, "Quantity").text = str(len(items))
    if items:
        items_el = SubElement(block, "Items")
        for it in items:
            SubElement(items_el, item_tag).text = it


def _distributions_using_policy(policy_id):
    return [d.get("Id", "") for d in _distributions.values() if _value_contains(d, policy_id)]


# ---- generic policy CRUD, shared by ORP and RHP ----


def _policy_precheck_if_match(headers, policy):
    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != policy["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )
    return None


def _policy_create(store, spec, body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    cfg, err = spec["parse"](el)
    if err is not None:
        return err
    for existing in store.values():
        if existing["Config"]["Name"] == cfg["Name"]:
            return _error(spec["dup"], f"A {spec['label']} with the same name already exists.", 409)
    pid = new_uuid()
    etag = new_uuid()
    policy = {"Id": pid, "ETag": etag, "LastModifiedTime": _now_iso(), "Config": cfg}
    store[pid] = policy
    logger.info("Create %s id=%s name=%s", spec["label"], pid, cfg["Name"])
    return _xml_response(
        spec["resource_tag"], lambda r: spec["build_resource"](r, policy),
        status=201, extra_headers={"ETag": etag, "Location": f"{spec['path']}/{pid}"},
    )


def _policy_get(store, spec, pid):
    policy = store.get(pid)
    if not policy:
        return _error(spec["missing"], f"The {spec['label']} does not exist.", 404)
    return _xml_response(spec["resource_tag"], lambda r: spec["build_resource"](r, policy),
                         extra_headers={"ETag": policy["ETag"]})


def _policy_get_config(store, spec, pid):
    policy = store.get(pid)
    if not policy:
        return _error(spec["missing"], f"The {spec['label']} does not exist.", 404)
    return _xml_response(spec["config_tag"], lambda r: spec["build_config"](r, policy["Config"]),
                         extra_headers={"ETag": policy["ETag"]})


def _policy_update(store, spec, pid, headers, body):
    policy = store.get(pid)
    if not policy:
        return _error(spec["missing"], f"The {spec['label']} does not exist.", 404)
    pc = _policy_precheck_if_match(headers, policy)
    if pc is not None:
        return pc
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    cfg, err = spec["parse"](el)
    if err is not None:
        return err
    for existing in store.values():
        if existing["Id"] != pid and existing["Config"]["Name"] == cfg["Name"]:
            return _error(spec["dup"], f"A {spec['label']} with the same name already exists.", 409)
    new_etag = new_uuid()
    policy["Config"] = cfg
    policy["ETag"] = new_etag
    policy["LastModifiedTime"] = _now_iso()
    return _xml_response(spec["resource_tag"], lambda r: spec["build_resource"](r, policy),
                         extra_headers={"ETag": new_etag})


def _policy_delete(store, spec, pid, headers):
    policy = store.get(pid)
    if not policy:
        return _error(spec["missing"], f"The {spec['label']} does not exist.", 404)
    pc = _policy_precheck_if_match(headers, policy)
    if pc is not None:
        return pc
    if _distributions_using_policy(pid):
        return _error(spec["in_use"],
                      f"The {spec['label']} cannot be deleted because it is attached to one or more cache behaviors.",
                      409)
    del store[pid]
    logger.info("Delete %s id=%s", spec["label"], pid)
    return 204, {}, b""


def _policy_list_distributions(store, spec, pid):
    if not store.get(pid):
        return _error(spec["missing"], f"The {spec['label']} does not exist.", 404)
    dist_ids = _distributions_using_policy(pid)

    def build(root):
        SubElement(root, "Marker").text = ""
        SubElement(root, "MaxItems").text = "100"
        SubElement(root, "IsTruncated").text = "false"
        SubElement(root, "Quantity").text = str(len(dist_ids))
        if dist_ids:
            items_el = SubElement(root, "Items")
            for did in dist_ids:
                SubElement(items_el, "DistributionId").text = did

    return _xml_response("DistributionIdList", build)


# ---- OriginRequestPolicy ----

_ORP_HEADER_BEHAVIORS = {"none", "whitelist", "allViewer", "allViewerAndWhitelistCloudFront", "allExcept"}
_ORP_COOKIE_BEHAVIORS = {"none", "whitelist", "all", "allExcept"}
_ORP_QUERYSTRING_BEHAVIORS = {"none", "whitelist", "all", "allExcept"}


def _parse_orp_config(el):
    name = _text(el, "Name")
    if not name:
        return None, _error("InvalidArgument", "The origin request policy name is required.", 400)
    headers_cfg = _find(el, "HeadersConfig")
    cookies_cfg = _find(el, "CookiesConfig")
    qs_cfg = _find(el, "QueryStringsConfig")
    if headers_cfg is None or cookies_cfg is None or qs_cfg is None:
        return None, _error("InvalidArgument",
                            "HeadersConfig, CookiesConfig, and QueryStringsConfig are required.", 400)
    hb = _text(headers_cfg, "HeaderBehavior")
    cb = _text(cookies_cfg, "CookieBehavior")
    qb = _text(qs_cfg, "QueryStringBehavior")
    if hb not in _ORP_HEADER_BEHAVIORS:
        return None, _error("InvalidArgument", "Invalid HeaderBehavior value.", 400)
    if cb not in _ORP_COOKIE_BEHAVIORS:
        return None, _error("InvalidArgument", "Invalid CookieBehavior value.", 400)
    if qb not in _ORP_QUERYSTRING_BEHAVIORS:
        return None, _error("InvalidArgument", "Invalid QueryStringBehavior value.", 400)
    return {
        "Name": name, "Comment": _text(el, "Comment"),
        "HeaderBehavior": hb, "Headers": _parse_name_items(headers_cfg, "Headers"),
        "CookieBehavior": cb, "Cookies": _parse_name_items(cookies_cfg, "Cookies"),
        "QueryStringBehavior": qb, "QueryStrings": _parse_name_items(qs_cfg, "QueryStrings"),
    }, None


def _build_orp_config_xml(parent, cfg):
    SubElement(parent, "Comment").text = cfg.get("Comment", "")
    SubElement(parent, "Name").text = cfg["Name"]
    hc = SubElement(parent, "HeadersConfig")
    SubElement(hc, "HeaderBehavior").text = cfg["HeaderBehavior"]
    _build_names_block(hc, "Headers", cfg["Headers"])
    cc = SubElement(parent, "CookiesConfig")
    SubElement(cc, "CookieBehavior").text = cfg["CookieBehavior"]
    _build_names_block(cc, "Cookies", cfg["Cookies"])
    qc = SubElement(parent, "QueryStringsConfig")
    SubElement(qc, "QueryStringBehavior").text = cfg["QueryStringBehavior"]
    _build_names_block(qc, "QueryStrings", cfg["QueryStrings"])


def _build_orp_xml(parent, policy):
    SubElement(parent, "Id").text = policy["Id"]
    SubElement(parent, "LastModifiedTime").text = policy["LastModifiedTime"]
    cfg_el = SubElement(parent, "OriginRequestPolicyConfig")
    _build_orp_config_xml(cfg_el, policy["Config"])


_ORP_SPEC = {
    "label": "origin request policy", "resource_tag": "OriginRequestPolicy",
    "config_tag": "OriginRequestPolicyConfig", "path": "/2020-05-31/origin-request-policy",
    "list_tag": "OriginRequestPolicyList", "summary_tag": "OriginRequestPolicySummary",
    "missing": "NoSuchOriginRequestPolicy", "dup": "OriginRequestPolicyAlreadyExists",
    "in_use": "OriginRequestPolicyInUse", "parse": _parse_orp_config,
    "build_resource": _build_orp_xml, "build_config": _build_orp_config_xml,
}


# ---- ResponseHeadersPolicy ----

_RHP_FRAME_OPTIONS = {"DENY", "SAMEORIGIN"}
_RHP_REFERRER = {
    "no-referrer", "no-referrer-when-downgrade", "origin", "origin-when-cross-origin",
    "same-origin", "strict-origin", "strict-origin-when-cross-origin", "unsafe-url",
}


def _parse_rhp_config(el):
    name = _text(el, "Name")
    if not name:
        return None, _error("InvalidArgument", "The response headers policy name is required.", 400)
    cfg = {"Name": name, "Comment": _text(el, "Comment"), "Cors": None, "Security": None,
           "ServerTiming": None, "CustomHeaders": [], "RemoveHeaders": []}

    cors_el = _find(el, "CorsConfig")
    if cors_el is not None:
        cors = {
            "AllowOrigins": _parse_str_list_block(cors_el, "AccessControlAllowOrigins", "Origin"),
            "AllowHeaders": _parse_str_list_block(cors_el, "AccessControlAllowHeaders", "Header"),
            "AllowMethods": _parse_str_list_block(cors_el, "AccessControlAllowMethods", "Method"),
            "AllowCredentials": _xbool(cors_el, "AccessControlAllowCredentials", False),
            "OriginOverride": _xbool(cors_el, "OriginOverride", False),
            "ExposeHeaders": None, "MaxAgeSec": None,
        }
        if _find(cors_el, "AccessControlExposeHeaders") is not None:
            cors["ExposeHeaders"] = _parse_str_list_block(cors_el, "AccessControlExposeHeaders", "Header")
        maxage = _find(cors_el, "AccessControlMaxAgeSec")
        if maxage is not None and (maxage.text or "").strip():
            cors["MaxAgeSec"] = int(maxage.text)
        cfg["Cors"] = cors

    sec_el = _find(el, "SecurityHeadersConfig")
    if sec_el is not None:
        sec = {}
        xss = _find(sec_el, "XSSProtection")
        if xss is not None:
            sec["XSSProtection"] = {
                "Override": _xbool(xss, "Override", False),
                "Protection": _xbool(xss, "Protection", False),
                "ModeBlock": _xbool(xss, "ModeBlock"),
                "ReportUri": _opt_text(xss, "ReportUri"),
            }
        fo = _find(sec_el, "FrameOptions")
        if fo is not None:
            fov = _text(fo, "FrameOption")
            if fov not in _RHP_FRAME_OPTIONS:
                return None, _error("InvalidArgument", "Invalid FrameOption value.", 400)
            sec["FrameOptions"] = {"Override": _xbool(fo, "Override", False), "FrameOption": fov}
        rp = _find(sec_el, "ReferrerPolicy")
        if rp is not None:
            rpv = _text(rp, "ReferrerPolicy")
            if rpv not in _RHP_REFERRER:
                return None, _error("InvalidArgument", "Invalid ReferrerPolicy value.", 400)
            sec["ReferrerPolicy"] = {"Override": _xbool(rp, "Override", False), "ReferrerPolicy": rpv}
        csp = _find(sec_el, "ContentSecurityPolicy")
        if csp is not None:
            sec["ContentSecurityPolicy"] = {"Override": _xbool(csp, "Override", False),
                                            "ContentSecurityPolicy": _text(csp, "ContentSecurityPolicy")}
        cto = _find(sec_el, "ContentTypeOptions")
        if cto is not None:
            sec["ContentTypeOptions"] = {"Override": _xbool(cto, "Override", False)}
        hsts = _find(sec_el, "StrictTransportSecurity")
        if hsts is not None:
            sec["StrictTransportSecurity"] = {
                "Override": _xbool(hsts, "Override", False),
                "IncludeSubdomains": _xbool(hsts, "IncludeSubdomains"),
                "Preload": _xbool(hsts, "Preload"),
                "AccessControlMaxAgeSec": int(_text(hsts, "AccessControlMaxAgeSec") or "0"),
            }
        cfg["Security"] = sec

    st_el = _find(el, "ServerTimingHeadersConfig")
    if st_el is not None:
        st = {"Enabled": _xbool(st_el, "Enabled", False), "SamplingRate": None}
        sr = _find(st_el, "SamplingRate")
        if sr is not None and (sr.text or "").strip():
            st["SamplingRate"] = float(sr.text)
        cfg["ServerTiming"] = st

    ch_el = _find(el, "CustomHeadersConfig")
    if ch_el is not None:
        items_el = _find(ch_el, "Items")
        if items_el is not None:
            for it in items_el:
                local = it.tag.split("}")[-1] if "}" in it.tag else it.tag
                if local == "ResponseHeadersPolicyCustomHeader":
                    cfg["CustomHeaders"].append({
                        "Header": _text(it, "Header"), "Value": _text(it, "Value"),
                        "Override": _xbool(it, "Override", False),
                    })

    rh_el = _find(el, "RemoveHeadersConfig")
    if rh_el is not None:
        items_el = _find(rh_el, "Items")
        if items_el is not None:
            for it in items_el:
                local = it.tag.split("}")[-1] if "}" in it.tag else it.tag
                if local == "ResponseHeadersPolicyRemoveHeader":
                    cfg["RemoveHeaders"].append({"Header": _text(it, "Header")})

    return cfg, None


def _build_rhp_config_xml(parent, cfg):
    SubElement(parent, "Comment").text = cfg.get("Comment", "")
    SubElement(parent, "Name").text = cfg["Name"]

    cors = cfg.get("Cors")
    if cors is not None:
        c = SubElement(parent, "CorsConfig")
        _build_str_list_block(c, "AccessControlAllowOrigins", "Origin", cors["AllowOrigins"])
        _build_str_list_block(c, "AccessControlAllowHeaders", "Header", cors["AllowHeaders"])
        _build_str_list_block(c, "AccessControlAllowMethods", "Method", cors["AllowMethods"])
        SubElement(c, "AccessControlAllowCredentials").text = _bstr(cors["AllowCredentials"])
        if cors.get("ExposeHeaders") is not None:
            _build_str_list_block(c, "AccessControlExposeHeaders", "Header", cors["ExposeHeaders"])
        if cors.get("MaxAgeSec") is not None:
            SubElement(c, "AccessControlMaxAgeSec").text = str(cors["MaxAgeSec"])
        SubElement(c, "OriginOverride").text = _bstr(cors["OriginOverride"])

    sec = cfg.get("Security")
    if sec is not None:
        s = SubElement(parent, "SecurityHeadersConfig")
        if "XSSProtection" in sec:
            x = SubElement(s, "XSSProtection")
            SubElement(x, "Override").text = _bstr(sec["XSSProtection"]["Override"])
            SubElement(x, "Protection").text = _bstr(sec["XSSProtection"]["Protection"])
            if sec["XSSProtection"].get("ModeBlock") is not None:
                SubElement(x, "ModeBlock").text = _bstr(sec["XSSProtection"]["ModeBlock"])
            if sec["XSSProtection"].get("ReportUri") is not None:
                SubElement(x, "ReportUri").text = sec["XSSProtection"]["ReportUri"]
        if "FrameOptions" in sec:
            f = SubElement(s, "FrameOptions")
            SubElement(f, "Override").text = _bstr(sec["FrameOptions"]["Override"])
            SubElement(f, "FrameOption").text = sec["FrameOptions"]["FrameOption"]
        if "ReferrerPolicy" in sec:
            r = SubElement(s, "ReferrerPolicy")
            SubElement(r, "Override").text = _bstr(sec["ReferrerPolicy"]["Override"])
            SubElement(r, "ReferrerPolicy").text = sec["ReferrerPolicy"]["ReferrerPolicy"]
        if "ContentSecurityPolicy" in sec:
            cs = SubElement(s, "ContentSecurityPolicy")
            SubElement(cs, "Override").text = _bstr(sec["ContentSecurityPolicy"]["Override"])
            SubElement(cs, "ContentSecurityPolicy").text = sec["ContentSecurityPolicy"]["ContentSecurityPolicy"]
        if "ContentTypeOptions" in sec:
            ct = SubElement(s, "ContentTypeOptions")
            SubElement(ct, "Override").text = _bstr(sec["ContentTypeOptions"]["Override"])
        if "StrictTransportSecurity" in sec:
            h = SubElement(s, "StrictTransportSecurity")
            SubElement(h, "Override").text = _bstr(sec["StrictTransportSecurity"]["Override"])
            if sec["StrictTransportSecurity"].get("IncludeSubdomains") is not None:
                SubElement(h, "IncludeSubdomains").text = _bstr(sec["StrictTransportSecurity"]["IncludeSubdomains"])
            if sec["StrictTransportSecurity"].get("Preload") is not None:
                SubElement(h, "Preload").text = _bstr(sec["StrictTransportSecurity"]["Preload"])
            SubElement(h, "AccessControlMaxAgeSec").text = str(sec["StrictTransportSecurity"]["AccessControlMaxAgeSec"])

    st = cfg.get("ServerTiming")
    if st is not None:
        stel = SubElement(parent, "ServerTimingHeadersConfig")
        SubElement(stel, "Enabled").text = _bstr(st["Enabled"])
        if st.get("SamplingRate") is not None:
            SubElement(stel, "SamplingRate").text = _fmt_rate(st["SamplingRate"])

    ch = SubElement(parent, "CustomHeadersConfig")
    SubElement(ch, "Quantity").text = str(len(cfg["CustomHeaders"]))
    if cfg["CustomHeaders"]:
        items = SubElement(ch, "Items")
        for hdr in cfg["CustomHeaders"]:
            it = SubElement(items, "ResponseHeadersPolicyCustomHeader")
            SubElement(it, "Header").text = hdr["Header"]
            SubElement(it, "Value").text = hdr["Value"]
            SubElement(it, "Override").text = _bstr(hdr["Override"])

    rh = SubElement(parent, "RemoveHeadersConfig")
    SubElement(rh, "Quantity").text = str(len(cfg["RemoveHeaders"]))
    if cfg["RemoveHeaders"]:
        items = SubElement(rh, "Items")
        for hdr in cfg["RemoveHeaders"]:
            it = SubElement(items, "ResponseHeadersPolicyRemoveHeader")
            SubElement(it, "Header").text = hdr["Header"]


def _build_rhp_xml(parent, policy):
    SubElement(parent, "Id").text = policy["Id"]
    SubElement(parent, "LastModifiedTime").text = policy["LastModifiedTime"]
    cfg_el = SubElement(parent, "ResponseHeadersPolicyConfig")
    _build_rhp_config_xml(cfg_el, policy["Config"])


_RHP_SPEC = {
    "label": "response headers policy", "resource_tag": "ResponseHeadersPolicy",
    "config_tag": "ResponseHeadersPolicyConfig", "path": "/2020-05-31/response-headers-policy",
    "list_tag": "ResponseHeadersPolicyList", "summary_tag": "ResponseHeadersPolicySummary",
    "missing": "NoSuchResponseHeadersPolicy", "dup": "ResponseHeadersPolicyAlreadyExists",
    "in_use": "ResponseHeadersPolicyInUse", "parse": _parse_rhp_config,
    "build_resource": _build_rhp_xml, "build_config": _build_rhp_config_xml,
}


# ---------------------------------------------------------------------------
# Read-only list handlers for resource families with no backing store.
# Shapes verified against botocore cloudfront service-2.json (2020-05-31).
# ---------------------------------------------------------------------------
_DEFAULT_MAX_ITEMS = "100"


def _empty_marker_list(query_params, root_tag, with_marker):
    """Build an AWS-shaped empty collection.

    ``with_marker=False`` matches the ``KeyGroupList`` family
    (NextMarker/MaxItems/Quantity); ``with_marker=True`` matches the
    ``CloudFrontOriginAccessIdentityList`` family, which additionally carries
    Marker + IsTruncated. Both omit ``Items`` when empty and omit
    ``NextMarker`` when there is no next page.
    """
    max_items = _qval(query_params, "MaxItems", _DEFAULT_MAX_ITEMS) or _DEFAULT_MAX_ITEMS
    marker = _qval(query_params, "Marker", "")

    def build(root):
        if with_marker:
            SubElement(root, "Marker").text = marker
            SubElement(root, "MaxItems").text = max_items
            SubElement(root, "IsTruncated").text = "false"
            SubElement(root, "Quantity").text = "0"
        else:
            SubElement(root, "MaxItems").text = max_items
            SubElement(root, "Quantity").text = "0"

    return _xml_response(root_tag, build)


def _list_realtime_log_configs(query_params):
    """RealtimeLogConfigs has no Quantity; carries MaxItems/IsTruncated/Marker."""
    max_items = _qval(query_params, "MaxItems", _DEFAULT_MAX_ITEMS) or _DEFAULT_MAX_ITEMS
    marker = _qval(query_params, "Marker", "")

    def build(root):
        SubElement(root, "MaxItems").text = max_items
        SubElement(root, "IsTruncated").text = "false"
        SubElement(root, "Marker").text = marker

    return _xml_response("RealtimeLogConfigs", build)


def _list_anycast_ip_lists(query_params):
    """AnycastIpListCollection: Marker/MaxItems/IsTruncated/Quantity, Items omitted when empty."""
    max_items = _qval(query_params, "MaxItems", _DEFAULT_MAX_ITEMS) or _DEFAULT_MAX_ITEMS
    marker = _qval(query_params, "Marker", "")

    def build(root):
        SubElement(root, "Marker").text = marker
        SubElement(root, "MaxItems").text = max_items
        SubElement(root, "IsTruncated").text = "false"
        SubElement(root, "Quantity").text = "0"

    return _xml_response("AnycastIpListCollection", build)


def _list_cache_policies(query_params):
    """CachePolicyList wrapping stored custom cache policies (Type=custom)."""
    max_items = _qval(query_params, "MaxItems", _DEFAULT_MAX_ITEMS) or _DEFAULT_MAX_ITEMS
    policies = list(_cache_policies.values())

    def build(root):
        SubElement(root, "MaxItems").text = max_items
        SubElement(root, "Quantity").text = str(len(policies))
        if policies:
            items_el = SubElement(root, "Items")
            for policy in policies:
                summary = SubElement(items_el, "CachePolicySummary")
                SubElement(summary, "Type").text = "custom"
                cp = SubElement(summary, "CachePolicy")
                _build_cache_policy_xml(cp, policy)

    return _xml_response("CachePolicyList", build)


def _list_policies(store, spec, query_params):
    """Generic ``*PolicyList`` wrapping stored custom policies (Type=custom).

    Shared by origin request policies and response headers policies; the
    summary member and resource tag come from ``spec``.
    """
    max_items = _qval(query_params, "MaxItems", _DEFAULT_MAX_ITEMS) or _DEFAULT_MAX_ITEMS
    policies = list(store.values())

    def build(root):
        SubElement(root, "MaxItems").text = max_items
        SubElement(root, "Quantity").text = str(len(policies))
        if policies:
            items_el = SubElement(root, "Items")
            for policy in policies:
                summary = SubElement(items_el, spec["summary_tag"])
                SubElement(summary, "Type").text = "custom"
                res = SubElement(summary, spec["resource_tag"])
                spec["build_resource"](res, policy)

    return _xml_response(spec["list_tag"], build)


def _get_monitoring_subscription(dist_id):
    """MiniStack does not persist monitoring subscriptions. Real AWS returns
    NoSuchDistribution (404) for an unknown distribution and
    NoSuchMonitoringSubscription (404) when none is configured."""
    if dist_id not in _distributions:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)
    return _error(
        "NoSuchMonitoringSubscription",
        "A monitoring subscription does not exist for the specified distribution.",
        404,
    )


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
        resource = (
            query_params.get("Resource", [""])[0]
            if isinstance(query_params.get("Resource"), list)
            else query_params.get("Resource", "")
        )
        operation = (
            query_params.get("Operation", [""])[0]
            if isinstance(query_params.get("Operation"), list)
            else query_params.get("Operation", "")
        )
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

    # Cache policy routes
    m = _CACHE_POLICY_CFG_RE.match(path)
    if m:
        if method == "GET":
            return _get_cache_policy_config(m.group(1))

    m = _CACHE_POLICY_RE.match(path)
    if m:
        if method == "POST":
            return _create_cache_policy(body)
        if method == "GET":
            return _list_cache_policies(query_params)

    m = _CACHE_POLICY_ID_RE.match(path)
    if m:
        policy_id = m.group(1)
        if method == "GET":
            return _get_cache_policy(policy_id)
        if method == "PUT":
            return _update_cache_policy(policy_id, headers, body)
        if method == "DELETE":
            return _delete_cache_policy(policy_id, headers)

    m = _DIST_BY_CACHE_POLICY_RE.match(path)
    if m:
        if method == "GET":
            return _list_distributions_by_cache_policy(m.group(1))

    # Origin request policy routes
    m = _ORP_CFG_RE.match(path)
    if m:
        if method == "GET":
            return _policy_get_config(_origin_request_policies, _ORP_SPEC, m.group(1))

    m = _ORP_RE.match(path)
    if m:
        if method == "POST":
            return _policy_create(_origin_request_policies, _ORP_SPEC, body)
        if method == "GET":
            return _list_policies(_origin_request_policies, _ORP_SPEC, query_params)

    m = _ORP_ID_RE.match(path)
    if m:
        pid = m.group(1)
        if method == "GET":
            return _policy_get(_origin_request_policies, _ORP_SPEC, pid)
        if method == "PUT":
            return _policy_update(_origin_request_policies, _ORP_SPEC, pid, headers, body)
        if method == "DELETE":
            return _policy_delete(_origin_request_policies, _ORP_SPEC, pid, headers)

    m = _DIST_BY_ORP_RE.match(path)
    if m:
        if method == "GET":
            return _policy_list_distributions(_origin_request_policies, _ORP_SPEC, m.group(1))

    # Response headers policy routes
    m = _RHP_CFG_RE.match(path)
    if m:
        if method == "GET":
            return _policy_get_config(_response_headers_policies, _RHP_SPEC, m.group(1))

    m = _RHP_RE.match(path)
    if m:
        if method == "POST":
            return _policy_create(_response_headers_policies, _RHP_SPEC, body)
        if method == "GET":
            return _list_policies(_response_headers_policies, _RHP_SPEC, query_params)

    m = _RHP_ID_RE.match(path)
    if m:
        pid = m.group(1)
        if method == "GET":
            return _policy_get(_response_headers_policies, _RHP_SPEC, pid)
        if method == "PUT":
            return _policy_update(_response_headers_policies, _RHP_SPEC, pid, headers, body)
        if method == "DELETE":
            return _policy_delete(_response_headers_policies, _RHP_SPEC, pid, headers)

    m = _DIST_BY_RHP_RE.match(path)
    if m:
        if method == "GET":
            return _policy_list_distributions(_response_headers_policies, _RHP_SPEC, m.group(1))

    # CloudFront Functions API (used by Terraform aws_cloudfront_function)
    m = _FUN_DESCRIBE_RE.match(path)
    if m:
        name = m.group(1)
        if method == "GET":
            stage = _qval(query_params, "Stage", "")
            if not stage:
                return _error("InvalidArgument", "The Stage query string parameter is required.", 400)
            return _cf_describe_function(name, stage)

    m = _FUN_PUBLISH_RE.match(path)
    if m:
        name = m.group(1)
        if method == "POST":
            return _cf_publish_function(name, headers)

    m = _FUN_NAME_RE.match(path)
    if m:
        name = m.group(1)
        if method == "GET":
            stage = _qval(query_params, "Stage", "")
            if not stage:
                return _error("InvalidArgument", "Stage is required.", 400)
            return _cf_get_function(name, stage)
        if method == "PUT":
            return _cf_update_function(name, headers, body)
        if method == "DELETE":
            return _cf_delete_function(name, headers)

    m = _FUN_LIST_RE.match(path)
    if m:
        if method == "POST":
            return _cf_create_function(headers, body)
        if method == "GET":
            return _cf_list_functions(query_params)

    # KeyValueStore routes
    m = _KVS_NAME_RE.match(path)
    if m:
        kvs_name = m.group(1)
        if method == "GET":
            return _describe_kvs(kvs_name)
        if method == "PUT":
            return _update_kvs(kvs_name, headers, body)
        if method == "DELETE":
            return _delete_kvs(kvs_name, headers)

    m = _KVS_LIST_RE.match(path)
    if m:
        if method == "POST":
            return _create_kvs(headers, body)
        if method == "GET":
            return _list_kvstores(query_params)

    # SaaS Manager routes. Tenant sub-resource routes must be matched before
    # the greedy _TENANT_ID_RE / _CONN_GROUP_ID_RE identifier routes.
    m = _TENANT_WEBACL_ASSOC_RE.match(path)
    if m:
        if method == "PUT":
            return _associate_tenant_webacl(m.group(1), headers, body)

    m = _TENANT_WEBACL_DISASSOC_RE.match(path)
    if m:
        if method == "PUT":
            return _disassociate_tenant_webacl(m.group(1), headers)

    m = _TENANT_INV_ID_RE.match(path)
    if m:
        if method == "GET":
            return _get_tenant_invalidation(m.group(1), m.group(2))

    m = _TENANT_INV_RE.match(path)
    if m:
        if method == "POST":
            return _create_tenant_invalidation(m.group(1), body)
        if method == "GET":
            return _list_tenant_invalidations(m.group(1))

    m = _TENANT_RE.match(path)
    if m:
        if method == "POST":
            return _create_distribution_tenant(body)
        if method == "GET":
            return _get_distribution_tenant_by_domain(query_params)

    m = _TENANTS_BY_CUSTOMIZATION_RE.match(path)
    if m:
        if method == "POST":
            return _list_distribution_tenants_by_customization(body)

    m = _TENANTS_LIST_RE.match(path)
    if m:
        if method == "POST":
            return _list_distribution_tenants(body)

    m = _TENANT_ID_RE.match(path)
    if m:
        identifier = m.group(1)
        if method == "GET":
            return _get_distribution_tenant(identifier)
        if method == "PUT":
            return _update_distribution_tenant(identifier, headers, body)
        if method == "DELETE":
            return _delete_distribution_tenant(identifier, headers)

    m = _CONN_GROUP_RE.match(path)
    if m:
        if method == "POST":
            return _create_connection_group(body)
        if method == "GET":
            return _get_connection_group_by_routing_endpoint(query_params)

    m = _CONN_GROUPS_LIST_RE.match(path)
    if m:
        if method == "POST":
            return _list_connection_groups(body)

    m = _CONN_GROUP_ID_RE.match(path)
    if m:
        identifier = m.group(1)
        if method == "GET":
            return _get_connection_group(identifier)
        if method == "PUT":
            return _update_connection_group(identifier, headers, body)
        if method == "DELETE":
            return _delete_connection_group(identifier, headers)

    m = _MANAGED_CERT_RE.match(path)
    if m:
        if method == "GET":
            return _get_managed_certificate_details(m.group(1))

    m = _VERIFY_DNS_RE.match(path)
    if m:
        if method == "POST":
            return _verify_dns_configuration(body)

    m = _DOMAIN_CONFLICTS_RE.match(path)
    if m:
        if method == "POST":
            return _list_domain_conflicts(body)

    m = _DOMAIN_ASSOCIATION_RE.match(path)
    if m:
        if method == "POST":
            return _update_domain_association(headers, body)

    m = _DIST_BY_CONN_MODE_RE.match(path)
    if m:
        if method == "GET":
            return _list_distributions_by_connection_mode(m.group(1))

    # Read-only list surface for resource families with no backing store.
    # Family split matches botocore: KeyGroupList-style carry no Marker/
    # IsTruncated; the OAI/StreamingDistribution/VpcOrigin family does.
    m = _KEY_GROUP_LIST_RE.match(path)
    if m and method == "GET":
        return _empty_marker_list(query_params, "KeyGroupList", with_marker=False)

    m = _PUBLIC_KEY_LIST_RE.match(path)
    if m and method == "GET":
        return _empty_marker_list(query_params, "PublicKeyList", with_marker=False)

    m = _FLE_LIST_RE.match(path)
    if m and method == "GET":
        return _empty_marker_list(query_params, "FieldLevelEncryptionList", with_marker=False)

    m = _FLE_PROFILE_LIST_RE.match(path)
    if m and method == "GET":
        return _empty_marker_list(query_params, "FieldLevelEncryptionProfileList", with_marker=False)

    m = _CDP_LIST_RE.match(path)
    if m and method == "GET":
        return _empty_marker_list(query_params, "ContinuousDeploymentPolicyList", with_marker=False)

    m = _OAI_LIST_RE.match(path)
    if m and method == "GET":
        return _empty_marker_list(query_params, "CloudFrontOriginAccessIdentityList", with_marker=True)

    m = _STREAMING_DIST_LIST_RE.match(path)
    if m and method == "GET":
        return _empty_marker_list(query_params, "StreamingDistributionList", with_marker=True)

    m = _VPC_ORIGIN_LIST_RE.match(path)
    if m and method == "GET":
        return _empty_marker_list(query_params, "VpcOriginList", with_marker=True)

    m = _REALTIME_LOG_LIST_RE.match(path)
    if m and method == "GET":
        return _list_realtime_log_configs(query_params)

    m = _ANYCAST_IP_LIST_RE.match(path)
    if m and method == "GET":
        return _list_anycast_ip_lists(query_params)

    m = _MONITORING_SUB_RE.match(path)
    if m and method == "GET":
        return _get_monitoring_subscription(m.group(1))

    return _error("NoSuchResource", f"No route for {method} {path}", 404)


# ---------------------------------------------------------------------------
# Distribution handlers
# ---------------------------------------------------------------------------


def _create_distribution(headers, body):
    root_el = _parse_body(body)
    config_el, tags_el = _unwrap_distribution_create_xml(root_el)
    if config_el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)

    caller_ref = _text(config_el, "CallerReference")
    if not caller_ref:
        return _error("InvalidArgument", "CallerReference is required.", 400)
    # CallerReference idempotency — return existing distribution if CallerReference matches
    for existing in _distributions.values():
        if existing.get("CallerReference") == caller_ref:

            def build(root, _dist=existing):
                _build_distribution_xml(root, _dist)

            return _xml_response("Distribution", build, status=200, extra_headers={"ETag": existing["ETag"]})
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
        "CallerReference": caller_ref,
        "config_xml": tostring(config_el, encoding="unicode"),
        "enabled": _get_enabled(config_el),
    }
    _distributions[dist_id] = dist
    _invalidations[dist_id] = []

    _ingest_distribution_tags_from_xml(dist["ARN"], tags_el)

    logger.info("CreateDistribution id=%s", dist_id)

    def build(root):
        _build_distribution_xml(root, dist)

    return _xml_response(
        "Distribution",
        build,
        status=201,
        extra_headers={
            "ETag": etag,
            "Location": f"/2020-05-31/distribution/{dist_id}",
        },
    )


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
    _ensure_distribution_config_sdk_compat(config_el)
    config_el.tag = "DistributionConfig"
    config_el.set("xmlns", NS)
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(config_el, encoding="unicode").encode("utf-8")
    return 200, {"Content-Type": "text/xml", "ETag": dist["ETag"]}, body


def _dist_config_el(dist):
    """Parsed DistributionConfig element for a stored distribution.

    CloudFormation-provisioned records carry an empty ``config_xml``; parse
    defensively so account-wide scans never fail on them."""
    xml = dist.get("config_xml")
    if xml:
        try:
            return fromstring(xml)
        except Exception:
            pass
    return Element("DistributionConfig")


def _dist_connection_mode(dist) -> str:
    """A distribution's ConnectionMode; absent in the stored config means direct."""
    return _text(_dist_config_el(dist), "ConnectionMode") or "direct"


def _build_distribution_list_xml(root, items):
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
            config_el = _dist_config_el(dist)
            # Field order matches real AWS DistributionSummary shape so
            # SDKs that strict-parse (Go v2, Java v2) don't reject it.
            # All 19 fields below are REQUIRED per botocore service-2.json.
            _add_config_block_with_default(ds, config_el, "Aliases")
            _add_config_block_with_default(ds, config_el, "Origins")
            _add_config_block_with_default(ds, config_el, "DefaultCacheBehavior")
            _add_config_block_with_default(ds, config_el, "CacheBehaviors")
            _add_config_block_with_default(ds, config_el, "CustomErrorResponses")
            SubElement(ds, "Comment").text = _text(config_el, "Comment") or ""
            SubElement(ds, "PriceClass").text = _text(config_el, "PriceClass") or "PriceClass_All"
            SubElement(ds, "Enabled").text = str(dist["enabled"]).lower()
            _add_config_block_with_default(ds, config_el, "ViewerCertificate")
            _add_config_block_with_default(ds, config_el, "Restrictions")
            SubElement(ds, "WebACLId").text = _text(config_el, "WebACLId") or ""
            SubElement(ds, "HttpVersion").text = _text(config_el, "HttpVersion") or "http2"
            SubElement(ds, "IsIPV6Enabled").text = (_text(config_el, "IsIPV6Enabled") or "true").lower()
            SubElement(ds, "Staging").text = str(dist.get("Staging", False)).lower()
            SubElement(ds, "ConnectionMode").text = _text(config_el, "ConnectionMode") or "direct"


def _list_distributions():
    items = list(_distributions.values())
    return _xml_response("DistributionList", lambda root: _build_distribution_list_xml(root, items))


def _update_distribution(dist_id, headers, body):
    dist = _distributions.get(dist_id)
    if not dist:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)

    if_match = headers.get("if-match", "")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != dist["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )

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
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )

    if dist["enabled"]:
        return _error(
            "DistributionNotDisabled", "The distribution you are trying to delete has not been disabled.", 409
        )

    if any(t["DistributionId"] == dist_id for t in _distribution_tenants.values()):
        return _error(
            "ResourceInUse",
            "The distribution has distribution tenants associated with it and cannot be deleted.",
            409,
        )

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

    invs = _invalidations[dist_id]
    for existing in invs:
        if existing["InvalidationBatch"]["CallerReference"] == caller_ref:
            existing_paths = existing["InvalidationBatch"]["Paths"]["Items"]
            if set(existing_paths) != set(path_items):
                return _error(
                    "InvalidationBatchAlreadyExists",
                    "An invalidation batch with this CallerReference already exists.",
                    400,
                )

            def build(root, _inv=existing):
                _build_invalidation_xml(root, _inv)

            return _xml_response(
                "Invalidation",
                build,
                status=201,
                extra_headers={
                    "Location": f"/2020-05-31/distribution/{dist_id}/invalidation/{existing['Id']}",
                },
            )

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

    return _xml_response(
        "Invalidation",
        build,
        status=201,
        extra_headers={
            "Location": f"/2020-05-31/distribution/{dist_id}/invalidation/{inv_id}",
        },
    )


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
    resource_arn, err = _resolve_taggable_cloudfront_arn(resource_arn)
    if err:
        return err
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
    resource_arn, err = _resolve_taggable_cloudfront_arn(resource_arn)
    if err:
        return err
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
    resource_arn, err = _resolve_taggable_cloudfront_arn(resource_arn)
    if err:
        return err
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

    return _xml_response(
        "OriginAccessControl",
        build,
        status=201,
        extra_headers={
            "ETag": etag,
            "Location": f"/2020-05-31/origin-access-control/{oac_id}",
        },
    )


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
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )

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
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )

    del _oacs[oac_id]

    logger.info("DeleteOriginAccessControl id=%s", oac_id)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# KeyValueStore handlers
# ---------------------------------------------------------------------------

_KVS_NAME_RE_VALIDATE = re.compile(r"^[a-zA-Z0-9\-_]{1,64}$")


def _build_kvs_xml(parent, kvs):
    SubElement(parent, "ARN").text = kvs["ARN"]
    SubElement(parent, "Comment").text = kvs.get("Comment", "")
    SubElement(parent, "Id").text = kvs["Id"]
    SubElement(parent, "LastModifiedTime").text = kvs["LastModifiedTime"]
    SubElement(parent, "Name").text = kvs["Name"]
    SubElement(parent, "Status").text = kvs.get("Status", "READY")


def _create_kvs(headers, body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)

    name = _text(el, "Name")
    if not name:
        return _error("InvalidArgument", "Name is required.", 400)
    if not _KVS_NAME_RE_VALIDATE.match(name):
        return _error("InvalidArgument", "Name must match pattern [a-zA-Z0-9-_]{1,64}.", 400)
    if name in _kvstores:
        return _error("EntityAlreadyExists", f"A key value store with name {name} already exists.", 409)

    comment = _text(el, "Comment")
    kvs_id = new_uuid()
    etag = new_uuid()
    now = _now_iso()
    arn = _kvs_arn(name)

    # Optional ImportSource (create-only) — AWS spec: structure with required
    # SourceType + SourceARN. We accept and round-trip the values; data import
    # itself is not performed (no S3 fetch). Recorded so callers that
    # describe the store can see what was requested.
    import_source = None
    imp_el = _find(el, "ImportSource")
    if imp_el is not None:
        src_type = _text(imp_el, "SourceType") or ""
        src_arn = _text(imp_el, "SourceARN") or ""
        if not src_type or not src_arn:
            return _error("InvalidArgument", "ImportSource requires SourceType and SourceARN.", 400)
        import_source = {"SourceType": src_type, "SourceARN": src_arn}

    kvs = {
        "Id": kvs_id,
        "Name": name,
        "Comment": comment,
        "ARN": arn,
        "Status": "READY",
        "LastModifiedTime": now,
        "ETag": etag,
        "ImportSource": import_source,
    }
    _kvstores[name] = kvs

    tags_el = _find(el, "Tags")
    if tags_el is not None:
        _ingest_distribution_tags_from_xml(arn, tags_el)

    logger.info("CreateKeyValueStore name=%s id=%s", name, kvs_id)

    def build(root):
        _build_kvs_xml(root, kvs)

    return _xml_response(
        "KeyValueStore",
        build,
        status=201,
        extra_headers={
            "ETag": etag,
            "Location": f"/2020-05-31/key-value-store/{name}",
        },
    )


def _describe_kvs(name):
    kvs = _kvstores.get(name)
    if not kvs:
        return _error("EntityNotFound", f"The key value store {name} was not found.", 404)

    def build(root):
        _build_kvs_xml(root, kvs)

    return _xml_response("KeyValueStore", build, extra_headers={"ETag": kvs["ETag"]})


def _list_kvstores(query_params):
    max_items = int(_qval(query_params, "MaxItems", "100") or "100")
    items = list(_kvstores.values())[:max_items]

    def build(root):
        items_el = SubElement(root, "Items")
        for kvs in items:
            kvs_el = SubElement(items_el, "KeyValueStore")
            _build_kvs_xml(kvs_el, kvs)
        SubElement(root, "MaxItems").text = str(max_items)
        SubElement(root, "Quantity").text = str(len(items))

    return _xml_response("KeyValueStoreList", build)


def _update_kvs(name, headers, body):
    kvs = _kvstores.get(name)
    if not kvs:
        return _error("EntityNotFound", f"The key value store {name} was not found.", 404)

    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != kvs["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )

    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)

    comment = _text(el, "Comment")
    new_etag = new_uuid()
    kvs["Comment"] = comment
    kvs["ETag"] = new_etag
    kvs["LastModifiedTime"] = _now_iso()

    logger.info("UpdateKeyValueStore name=%s", name)

    def build(root):
        _build_kvs_xml(root, kvs)

    return _xml_response("KeyValueStore", build, extra_headers={"ETag": new_etag})


def _delete_kvs(name, headers):
    kvs = _kvstores.get(name)
    if not kvs:
        return _error("EntityNotFound", f"The key value store {name} was not found.", 404)

    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != kvs["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )

    arn = kvs["ARN"]
    for fn in _functions.values():
        if arn in fn.get("kvs_arns", []):
            return _error(
                "CannotDeleteEntityWhileInUse",
                "The key value store is associated with a function and cannot be deleted.",
                409,
            )

    del _kvstores[name]

    logger.info("DeleteKeyValueStore name=%s", name)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# SaaS Manager — connection groups and distribution tenants.
# Shapes verified against botocore cloudfront service-2.json (2020-05-31):
# result shapes without a payload member use an <OperationNameResult> root;
# payload results use the payload shape name as root. Non-flattened lists
# without a member locationName (Domains, Parameters, ValidationTokenDetails)
# serialize items as <member>; named ones use their locationName.
# MiniStack collapses async workflows: resources deploy, domains activate,
# and managed certificates issue immediately.
# ---------------------------------------------------------------------------

_CONNECTION_MODES = {"direct", "tenant-only"}
_WEBACL_CUSTOMIZATION_ACTIONS = {"override", "disable"}
_VALIDATION_TOKEN_HOSTS = {"cloudfront", "self-hosted"}


def _tenant_arn(tenant_id: str) -> str:
    return f"arn:aws:cloudfront::{get_account_id()}:distribution-tenant/{tenant_id}"


def _conn_group_arn(cg_id: str) -> str:
    return f"arn:aws:cloudfront::{get_account_id()}:connection-group/{cg_id}"


def _find_connection_group(identifier: str):
    """Resolve a connection group by ID, name, or ARN (per AWS docs)."""
    identifier = unquote(identifier or "")
    cg = _connection_groups.get(identifier)
    if cg:
        return cg
    for cg in _connection_groups.values():
        if cg["Name"] == identifier or cg["Arn"] == identifier:
            return cg
    return None


def _find_distribution_tenant(identifier: str):
    """Resolve a distribution tenant by ID, name, or ARN (per AWS docs)."""
    identifier = unquote(identifier or "")
    tenant = _distribution_tenants.get(identifier)
    if tenant:
        return tenant
    for tenant in _distribution_tenants.values():
        if tenant["Name"] == identifier or tenant["Arn"] == identifier:
            return tenant
    return None


def _domains_overlap(a: str, b: str) -> bool:
    """Case-insensitive match; a leading ``*.`` wildcard covers exactly one label."""
    a, b = (a or "").lower(), (b or "").lower()
    if a == b:
        return True

    def covers(wild, host):
        if not wild.startswith("*."):
            return False
        suffix = wild[2:]
        return host.endswith("." + suffix) and "." not in host[: -len(suffix) - 1]

    return covers(a, b) or covers(b, a)


def _domain_owners(domain: str, wildcard_overlap: bool = False):
    """All (resource_type, resource_id) pairs currently claiming ``domain``.

    Scans tenant domains and distribution alias CNAMEs, case-insensitively —
    CloudFront treats CNAMEs as globally unique across both families.
    ``wildcard_overlap`` additionally counts single-level wildcard overlaps
    (ListDomainConflicts semantics).
    """
    d = (domain or "").lower()

    def claims(owned):
        return _domains_overlap(d, owned) if wildcard_overlap else owned.lower() == d

    owners = []
    for tenant in _distribution_tenants.values():
        if any(claims(x) for x in tenant["Domains"]):
            owners.append(("distribution-tenant", tenant["Id"]))
    for dist in _distributions.values():
        if any(claims(x) for x in _get_distribution_aliases(dist)):
            owners.append(("distribution", dist["Id"]))
    return owners


def _get_distribution_aliases(dist):
    config_el = _dist_config_el(dist)
    aliases = _find(config_el, "Aliases")
    items = []
    if aliases is not None:
        items_el = _find(aliases, "Items")
        if items_el is not None:
            items = [c.text or "" for c in items_el]
    return items


def _set_distribution_aliases(dist, aliases):
    config_el = _dist_config_el(dist)
    block = _find(config_el, "Aliases")
    if block is not None:
        config_el.remove(block)
    block = SubElement(config_el, "Aliases")
    SubElement(block, "Quantity").text = str(len(aliases))
    if aliases:
        items_el = SubElement(block, "Items")
        for alias in aliases:
            SubElement(items_el, "CNAME").text = alias
    dist["config_xml"] = tostring(config_el, encoding="unicode")


# ---- request XML parsers ----


def _parse_tenant_domains(el):
    """Parse <Domains><member><Domain>..</Domain></member></Domains>; None when absent."""
    block = _find(el, "Domains")
    if block is None:
        return None
    domains = []
    for child in block:
        d = _text(child, "Domain")
        if d:
            domains.append(d)
    return domains


def _parse_tenant_parameters(el):
    block = _find(el, "Parameters")
    if block is None:
        return None
    params = []
    for child in block:
        name = _text(child, "Name")
        if name:
            params.append({"Name": name, "Value": _text(child, "Value")})
    return params


def _parse_customizations(el):
    """Parse a <Customizations> block into a dict, or return an _error tuple.

    Returns ``(customizations_or_None, error_or_None)``.
    """
    block = _find(el, "Customizations")
    if block is None:
        return None, None
    cust = {}
    webacl = _find(block, "WebAcl")
    if webacl is not None:
        action = _text(webacl, "Action")
        if action not in _WEBACL_CUSTOMIZATION_ACTIONS:
            return None, _error("InvalidArgument", "Invalid WebAcl customization Action value.", 400)
        entry = {"Action": action}
        arn = _opt_text(webacl, "Arn")
        if arn:
            entry["Arn"] = arn
        cust["WebAcl"] = entry
    cert = _find(block, "Certificate")
    if cert is not None:
        arn = _text(cert, "Arn")
        if not arn:
            return None, _error("InvalidArgument", "Certificate customization requires Arn.", 400)
        cust["Certificate"] = {"Arn": arn}
    geo = _find(block, "GeoRestrictions")
    if geo is not None:
        rtype = _text(geo, "RestrictionType")
        if rtype not in {"blacklist", "whitelist", "none"}:
            return None, _error("InvalidArgument", "Invalid GeoRestrictions RestrictionType value.", 400)
        locations_el = _find(geo, "Locations")
        locations = [c.text or "" for c in locations_el] if locations_el is not None else []
        cust["GeoRestrictions"] = {"RestrictionType": rtype, "Locations": locations}
    return cust, None


def _parse_managed_certificate_request(el, domains):
    """Mint a managed-certificate record from <ManagedCertificateRequest>.

    Returns ``(record_or_None, error_or_None)``. MiniStack issues immediately.
    """
    mcr = _find(el, "ManagedCertificateRequest")
    if mcr is None:
        return None, None
    host = _text(mcr, "ValidationTokenHost")
    if host not in _VALIDATION_TOKEN_HOSTS:
        return None, _error("InvalidArgument", "Invalid ValidationTokenHost value.", 400)
    return {
        "CertificateArn": f"arn:aws:acm:us-east-1:{get_account_id()}:certificate/{new_uuid()}",
        "CertificateStatus": "issued",
        "ValidationTokenHost": host,
        "PrimaryDomainName": _opt_text(mcr, "PrimaryDomainName"),
        "Domains": list(domains),
    }, None


# ---- response XML builders ----


def _build_tags_block_xml(parent, arn):
    tags_el = SubElement(parent, "Tags")
    items = SubElement(tags_el, "Items")
    for t in _tags.get(arn, []):
        tag_el = SubElement(items, "Tag")
        SubElement(tag_el, "Key").text = t["Key"]
        SubElement(tag_el, "Value").text = t["Value"]


def _build_customizations_xml(parent, cust):
    if not cust:
        return
    block = SubElement(parent, "Customizations")
    webacl = cust.get("WebAcl")
    if webacl:
        w = SubElement(block, "WebAcl")
        SubElement(w, "Action").text = webacl["Action"]
        if webacl.get("Arn"):
            SubElement(w, "Arn").text = webacl["Arn"]
    cert = cust.get("Certificate")
    if cert:
        c = SubElement(block, "Certificate")
        SubElement(c, "Arn").text = cert["Arn"]
    geo = cust.get("GeoRestrictions")
    if geo:
        g = SubElement(block, "GeoRestrictions")
        SubElement(g, "RestrictionType").text = geo["RestrictionType"]
        if geo.get("Locations"):
            locs = SubElement(g, "Locations")
            for loc in geo["Locations"]:
                SubElement(locs, "Location").text = loc


def _build_connection_group_xml(parent, cg):
    SubElement(parent, "Id").text = cg["Id"]
    SubElement(parent, "Name").text = cg["Name"]
    SubElement(parent, "Arn").text = cg["Arn"]
    SubElement(parent, "CreatedTime").text = cg["CreatedTime"]
    SubElement(parent, "LastModifiedTime").text = cg["LastModifiedTime"]
    _build_tags_block_xml(parent, cg["Arn"])
    SubElement(parent, "Ipv6Enabled").text = _bstr(cg["Ipv6Enabled"])
    SubElement(parent, "RoutingEndpoint").text = cg["RoutingEndpoint"]
    if cg.get("AnycastIpListId"):
        SubElement(parent, "AnycastIpListId").text = cg["AnycastIpListId"]
    SubElement(parent, "Status").text = "Deployed"
    SubElement(parent, "Enabled").text = _bstr(cg["Enabled"])
    SubElement(parent, "IsDefault").text = _bstr(cg["IsDefault"])


def _build_connection_group_summary_xml(parent, cg):
    SubElement(parent, "Id").text = cg["Id"]
    SubElement(parent, "Name").text = cg["Name"]
    SubElement(parent, "Arn").text = cg["Arn"]
    SubElement(parent, "RoutingEndpoint").text = cg["RoutingEndpoint"]
    SubElement(parent, "CreatedTime").text = cg["CreatedTime"]
    SubElement(parent, "LastModifiedTime").text = cg["LastModifiedTime"]
    SubElement(parent, "ETag").text = cg["ETag"]
    if cg.get("AnycastIpListId"):
        SubElement(parent, "AnycastIpListId").text = cg["AnycastIpListId"]
    SubElement(parent, "Enabled").text = _bstr(cg["Enabled"])
    SubElement(parent, "Status").text = "Deployed"
    SubElement(parent, "IsDefault").text = _bstr(cg["IsDefault"])


def _build_domain_results_xml(parent, domains):
    block = SubElement(parent, "Domains")
    for d in domains:
        item = SubElement(block, "member")
        SubElement(item, "Domain").text = d
        SubElement(item, "Status").text = "active"


def _build_tenant_parameters_xml(parent, params):
    if not params:
        return
    block = SubElement(parent, "Parameters")
    for p in params:
        item = SubElement(block, "member")
        SubElement(item, "Name").text = p["Name"]
        SubElement(item, "Value").text = p["Value"]


def _build_distribution_tenant_xml(parent, tenant):
    SubElement(parent, "Id").text = tenant["Id"]
    SubElement(parent, "DistributionId").text = tenant["DistributionId"]
    SubElement(parent, "Name").text = tenant["Name"]
    SubElement(parent, "Arn").text = tenant["Arn"]
    _build_domain_results_xml(parent, tenant["Domains"])
    _build_tags_block_xml(parent, tenant["Arn"])
    _build_customizations_xml(parent, tenant.get("Customizations"))
    _build_tenant_parameters_xml(parent, tenant.get("Parameters"))
    SubElement(parent, "ConnectionGroupId").text = tenant["ConnectionGroupId"]
    SubElement(parent, "CreatedTime").text = tenant["CreatedTime"]
    SubElement(parent, "LastModifiedTime").text = tenant["LastModifiedTime"]
    SubElement(parent, "Enabled").text = _bstr(tenant["Enabled"])
    SubElement(parent, "Status").text = "Deployed"


def _build_distribution_tenant_summary_xml(parent, tenant):
    SubElement(parent, "Id").text = tenant["Id"]
    SubElement(parent, "DistributionId").text = tenant["DistributionId"]
    SubElement(parent, "Name").text = tenant["Name"]
    SubElement(parent, "Arn").text = tenant["Arn"]
    _build_domain_results_xml(parent, tenant["Domains"])
    SubElement(parent, "ConnectionGroupId").text = tenant["ConnectionGroupId"]
    _build_customizations_xml(parent, tenant.get("Customizations"))
    SubElement(parent, "CreatedTime").text = tenant["CreatedTime"]
    SubElement(parent, "LastModifiedTime").text = tenant["LastModifiedTime"]
    SubElement(parent, "ETag").text = tenant["ETag"]
    SubElement(parent, "Enabled").text = _bstr(tenant["Enabled"])
    SubElement(parent, "Status").text = "Deployed"


def _tenant_response(tenant, status=200):
    def build(root):
        _build_distribution_tenant_xml(root, tenant)

    return _xml_response("DistributionTenant", build, status=status, extra_headers={"ETag": tenant["ETag"]})


def _conn_group_response(cg, status=200):
    def build(root):
        _build_connection_group_xml(root, cg)

    return _xml_response("ConnectionGroup", build, status=status, extra_headers={"ETag": cg["ETag"]})


# ---- connection group handlers ----


def _create_connection_group(body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    name = _text(el, "Name")
    if not name:
        return _error("InvalidArgument", "Name is required.", 400)
    for existing in _connection_groups.values():
        if existing["Name"] == name:
            return _error("EntityAlreadyExists", "A connection group with this name already exists.", 409)

    now = _now_iso()
    cg = {
        "Id": _conn_group_id(),
        "Name": name,
        "CreatedTime": now,
        "LastModifiedTime": now,
        "Ipv6Enabled": _xbool(el, "Ipv6Enabled", True),
        "RoutingEndpoint": _routing_endpoint(),
        "AnycastIpListId": _opt_text(el, "AnycastIpListId"),
        "Enabled": _xbool(el, "Enabled", True),
        "IsDefault": False,
        "ETag": new_uuid(),
    }
    cg["Arn"] = _conn_group_arn(cg["Id"])
    _connection_groups[cg["Id"]] = cg
    _ingest_distribution_tags_from_xml(cg["Arn"], _find(el, "Tags"))
    logger.info("CreateConnectionGroup id=%s name=%s", cg["Id"], name)
    return _conn_group_response(cg, status=201)


def _default_connection_group():
    """The account's default connection group, created lazily like real AWS
    does when a tenant is created without an explicit ConnectionGroupId."""
    for cg in _connection_groups.values():
        if cg["IsDefault"]:
            return cg
    now = _now_iso()
    cg = {
        "Id": _conn_group_id(),
        "CreatedTime": now,
        "LastModifiedTime": now,
        "Ipv6Enabled": True,
        "RoutingEndpoint": _routing_endpoint(),
        "AnycastIpListId": None,
        "Enabled": True,
        "IsDefault": True,
        "ETag": new_uuid(),
    }
    cg["Name"] = f"CreatedByCloudFront-{cg['Id']}"
    cg["Arn"] = _conn_group_arn(cg["Id"])
    _connection_groups[cg["Id"]] = cg
    logger.info("Created default connection group id=%s", cg["Id"])
    return cg


def _get_connection_group(identifier):
    cg = _find_connection_group(identifier)
    if not cg:
        return _error("EntityNotFound", "The specified connection group does not exist.", 404)
    return _conn_group_response(cg)


def _get_connection_group_by_routing_endpoint(query_params):
    endpoint = _qval(query_params, "RoutingEndpoint", "")
    if not endpoint:
        return _error("InvalidArgument", "The RoutingEndpoint query string parameter is required.", 400)
    for cg in _connection_groups.values():
        if cg["RoutingEndpoint"] == endpoint:
            return _conn_group_response(cg)
    return _error("EntityNotFound", "The specified connection group does not exist.", 404)


def _update_connection_group(identifier, headers, body):
    cg = _find_connection_group(identifier)
    if not cg:
        return _error("EntityNotFound", "The specified connection group does not exist.", 404)
    pc = _policy_precheck_if_match(headers, cg)
    if pc is not None:
        return pc
    # boto3 serializes a request with only Id + IfMatch as an empty body;
    # real AWS accepts it as a members-unchanged update.
    el = _parse_body(body)
    if el is None and body:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    if el is not None:
        ipv6 = _xbool(el, "Ipv6Enabled")
        if ipv6 is not None:
            cg["Ipv6Enabled"] = ipv6
        enabled = _xbool(el, "Enabled")
        if enabled is not None:
            cg["Enabled"] = enabled
        anycast = _opt_text(el, "AnycastIpListId")
        if anycast is not None:
            cg["AnycastIpListId"] = anycast
    cg["ETag"] = new_uuid()
    cg["LastModifiedTime"] = _now_iso()
    logger.info("UpdateConnectionGroup id=%s", cg["Id"])
    return _conn_group_response(cg)


def _delete_connection_group(identifier, headers):
    cg = _find_connection_group(identifier)
    if not cg:
        return _error("EntityNotFound", "The specified connection group does not exist.", 404)
    pc = _policy_precheck_if_match(headers, cg)
    if pc is not None:
        return pc
    if any(t["ConnectionGroupId"] == cg["Id"] for t in _distribution_tenants.values()):
        return _error(
            "CannotDeleteEntityWhileInUse",
            "The connection group has distribution tenants associated with it and cannot be deleted.",
            409,
        )
    if cg["Enabled"]:
        return _error("ResourceNotDisabled", "The connection group you are trying to delete has not been disabled.", 409)
    del _connection_groups[cg["Id"]]
    logger.info("DeleteConnectionGroup id=%s", cg["Id"])
    return 204, {}, b""


def _list_connection_groups(body):
    el = _parse_body(body)
    anycast_filter = None
    if el is not None:
        assoc = _find(el, "AssociationFilter")
        if assoc is not None:
            anycast_filter = _opt_text(assoc, "AnycastIpListId")
    groups = [
        cg for cg in _connection_groups.values()
        if not anycast_filter or cg.get("AnycastIpListId") == anycast_filter
    ]

    def build(root):
        items_el = SubElement(root, "ConnectionGroups")
        for cg in groups:
            summary = SubElement(items_el, "ConnectionGroupSummary")
            _build_connection_group_summary_xml(summary, cg)

    return _xml_response("ListConnectionGroupsResult", build)


# ---- distribution tenant handlers ----


def _check_tenant_distribution(dist_id):
    """Validate the distribution a tenant attaches to. Returns an error tuple or None."""
    dist = _distributions.get(dist_id)
    if not dist:
        return _error("EntityNotFound", "The specified distribution does not exist.", 404)
    if _dist_connection_mode(dist) != "tenant-only":
        return _error(
            "InvalidAssociation",
            "Distribution tenants can only be associated with multi-tenant (tenant-only) distributions.",
            409,
        )
    return None


def _check_tenant_domain_conflicts(domains, exclude_tenant_id=None):
    lowered = [d.lower() for d in domains]
    if len(set(lowered)) != len(lowered):
        return _error("InvalidArgument", "Duplicate domains are not allowed.", 400)
    for domain in domains:
        for _rtype, rid in _domain_owners(domain):
            if rid != exclude_tenant_id:
                return _error("CNAMEAlreadyExists", f"The CNAME {domain} is already in use.", 409)
    return None


def _create_distribution_tenant(body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    dist_id = _text(el, "DistributionId")
    if not dist_id:
        return _error("InvalidArgument", "DistributionId is required.", 400)
    name = _text(el, "Name")
    if not name:
        return _error("InvalidArgument", "Name is required.", 400)
    domains = _parse_tenant_domains(el)
    if not domains:
        return _error("InvalidArgument", "Domains is required.", 400)

    err = _check_tenant_distribution(dist_id)
    if err is not None:
        return err
    for existing in _distribution_tenants.values():
        if existing["Name"] == name:
            return _error("EntityAlreadyExists", "A distribution tenant with this name already exists.", 409)
    err = _check_tenant_domain_conflicts(domains)
    if err is not None:
        return err

    cg_id_text = _text(el, "ConnectionGroupId")
    if cg_id_text:
        cg = _find_connection_group(cg_id_text)
        if not cg:
            return _error("EntityNotFound", "The specified connection group does not exist.", 404)
    else:
        cg = _default_connection_group()

    cust, err = _parse_customizations(el)
    if err is not None:
        return err
    managed_cert, err = _parse_managed_certificate_request(el, domains)
    if err is not None:
        return err

    now = _now_iso()
    tenant = {
        "Id": _tenant_id(),
        "DistributionId": dist_id,
        "Name": name,
        "Domains": domains,
        "Customizations": cust,
        "Parameters": _parse_tenant_parameters(el) or [],
        "ConnectionGroupId": cg["Id"],
        "CreatedTime": now,
        "LastModifiedTime": now,
        "Enabled": _xbool(el, "Enabled", True),
        "ETag": new_uuid(),
        "ManagedCertificate": managed_cert,
    }
    tenant["Arn"] = _tenant_arn(tenant["Id"])
    _distribution_tenants[tenant["Id"]] = tenant
    _tenant_invalidations[tenant["Id"]] = []
    _ingest_distribution_tags_from_xml(tenant["Arn"], _find(el, "Tags"))
    logger.info("CreateDistributionTenant id=%s name=%s dist=%s", tenant["Id"], name, dist_id)
    return _tenant_response(tenant, status=201)


def _get_distribution_tenant(identifier):
    tenant = _find_distribution_tenant(identifier)
    if not tenant:
        return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
    return _tenant_response(tenant)


def _get_distribution_tenant_by_domain(query_params):
    domain = _qval(query_params, "domain", "")
    if not domain:
        return _error("InvalidArgument", "The domain query string parameter is required.", 400)
    d = domain.lower()
    for tenant in _distribution_tenants.values():
        if any(x.lower() == d for x in tenant["Domains"]):
            return _tenant_response(tenant)
    return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)


def _update_distribution_tenant(identifier, headers, body):
    tenant = _find_distribution_tenant(identifier)
    if not tenant:
        return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
    pc = _policy_precheck_if_match(headers, tenant)
    if pc is not None:
        return pc
    # boto3 serializes a request with only Id + IfMatch as an empty body;
    # real AWS accepts it as a members-unchanged update.
    el = _parse_body(body)
    if el is None and body:
        return _error("MalformedXML", "The XML document is malformed.", 400)

    # Validate the whole request before committing anything — a rejected
    # update must leave the tenant untouched, like real AWS.
    updates = {}
    if el is not None:
        dist_id = _text(el, "DistributionId")
        if dist_id and dist_id != tenant["DistributionId"]:
            err = _check_tenant_distribution(dist_id)
            if err is not None:
                return err
        if dist_id:
            updates["DistributionId"] = dist_id
        domains = _parse_tenant_domains(el)
        if domains is not None:
            if not domains:
                return _error("InvalidArgument", "Domains must not be empty.", 400)
            err = _check_tenant_domain_conflicts(domains, exclude_tenant_id=tenant["Id"])
            if err is not None:
                return err
            updates["Domains"] = domains
        cg_id_text = _text(el, "ConnectionGroupId")
        if cg_id_text:
            cg = _find_connection_group(cg_id_text)
            if not cg:
                return _error("EntityNotFound", "The specified connection group does not exist.", 404)
            updates["ConnectionGroupId"] = cg["Id"]
        cust, err = _parse_customizations(el)
        if err is not None:
            return err
        if cust is not None:
            updates["Customizations"] = cust
        params = _parse_tenant_parameters(el)
        if params is not None:
            updates["Parameters"] = params
        enabled = _xbool(el, "Enabled")
        if enabled is not None:
            updates["Enabled"] = enabled
        managed_cert, err = _parse_managed_certificate_request(el, updates.get("Domains", tenant["Domains"]))
        if err is not None:
            return err
        if managed_cert is not None:
            updates["ManagedCertificate"] = managed_cert

    tenant.update(updates)
    tenant["ETag"] = new_uuid()
    tenant["LastModifiedTime"] = _now_iso()
    logger.info("UpdateDistributionTenant id=%s", tenant["Id"])
    return _tenant_response(tenant)


def _delete_distribution_tenant(identifier, headers):
    tenant = _find_distribution_tenant(identifier)
    if not tenant:
        return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
    pc = _policy_precheck_if_match(headers, tenant)
    if pc is not None:
        return pc
    if tenant["Enabled"]:
        return _error(
            "ResourceNotDisabled", "The distribution tenant you are trying to delete has not been disabled.", 409
        )
    del _distribution_tenants[tenant["Id"]]
    _tenant_invalidations.pop(tenant["Id"], None)
    logger.info("DeleteDistributionTenant id=%s", tenant["Id"])
    return 204, {}, b""


def _list_distribution_tenants(body):
    el = _parse_body(body)
    dist_filter = cg_filter = None
    if el is not None:
        assoc = _find(el, "AssociationFilter")
        if assoc is not None:
            dist_filter = _opt_text(assoc, "DistributionId")
            cg_filter = _opt_text(assoc, "ConnectionGroupId")
    tenants = [
        t for t in _distribution_tenants.values()
        if (not dist_filter or t["DistributionId"] == dist_filter)
        and (not cg_filter or t["ConnectionGroupId"] == cg_filter)
    ]
    return _tenant_list_response(tenants, "ListDistributionTenantsResult")


def _list_distribution_tenants_by_customization(body):
    el = _parse_body(body)
    webacl_filter = cert_filter = None
    if el is not None:
        webacl_filter = _opt_text(el, "WebACLArn")
        cert_filter = _opt_text(el, "CertificateArn")

    def matches(tenant):
        cust = tenant.get("Customizations") or {}
        if webacl_filter and (cust.get("WebAcl") or {}).get("Arn") != webacl_filter:
            return False
        if cert_filter and (cust.get("Certificate") or {}).get("Arn") != cert_filter:
            return False
        return True

    return _tenant_list_response(
        [t for t in _distribution_tenants.values() if matches(t)],
        "ListDistributionTenantsByCustomizationResult",
    )


def _tenant_list_response(tenants, root_tag):
    def build(root):
        items_el = SubElement(root, "DistributionTenantList")
        for tenant in tenants:
            summary = SubElement(items_el, "DistributionTenantSummary")
            _build_distribution_tenant_summary_xml(summary, tenant)

    return _xml_response(root_tag, build)


def _associate_tenant_webacl(tenant_id, headers, body):
    tenant = _find_distribution_tenant(tenant_id)
    if not tenant:
        return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
    if_match = headers.get("if-match")
    if if_match and if_match != tenant["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )
    el = _parse_body(body)
    webacl_arn = _text(el, "WebACLArn") if el is not None else ""
    if not webacl_arn:
        return _error("InvalidArgument", "WebACLArn is required.", 400)
    cust = tenant.get("Customizations") or {}
    cust["WebAcl"] = {"Action": "override", "Arn": webacl_arn}
    tenant["Customizations"] = cust
    tenant["ETag"] = new_uuid()
    tenant["LastModifiedTime"] = _now_iso()
    logger.info("AssociateDistributionTenantWebACL id=%s", tenant["Id"])

    def build(root):
        SubElement(root, "Id").text = tenant["Id"]
        SubElement(root, "WebACLArn").text = webacl_arn

    return _xml_response(
        "AssociateDistributionTenantWebACLResult", build, extra_headers={"ETag": tenant["ETag"]}
    )


def _disassociate_tenant_webacl(tenant_id, headers):
    tenant = _find_distribution_tenant(tenant_id)
    if not tenant:
        return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
    if_match = headers.get("if-match")
    if if_match and if_match != tenant["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )
    cust = tenant.get("Customizations") or {}
    cust.pop("WebAcl", None)
    tenant["Customizations"] = cust
    tenant["ETag"] = new_uuid()
    tenant["LastModifiedTime"] = _now_iso()
    logger.info("DisassociateDistributionTenantWebACL id=%s", tenant["Id"])

    def build(root):
        SubElement(root, "Id").text = tenant["Id"]

    return _xml_response(
        "DisassociateDistributionTenantWebACLResult", build, extra_headers={"ETag": tenant["ETag"]}
    )


# ---- tenant invalidation handlers (mirror the distribution ones) ----


def _create_tenant_invalidation(tenant_id, body):
    tenant = _find_distribution_tenant(tenant_id)
    if not tenant:
        return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
    batch_el = _parse_body(body)
    if batch_el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)

    paths_el = _find(batch_el, "Paths")
    caller_ref = _text(batch_el, "CallerReference")
    path_items = []
    if paths_el is not None:
        items_el = _find(paths_el, "Items")
        if items_el is not None:
            path_items = [child.text for child in items_el if child.text]

    invs = _tenant_invalidations.setdefault(tenant["Id"], [])
    for existing in invs:
        if existing["InvalidationBatch"]["CallerReference"] == caller_ref:
            if set(existing["InvalidationBatch"]["Paths"]["Items"]) != set(path_items):
                return _error(
                    "InvalidationBatchAlreadyExists",
                    "An invalidation batch with this CallerReference already exists.",
                    400,
                )

            def build(root, _inv=existing):
                _build_invalidation_xml(root, _inv)

            return _xml_response(
                "Invalidation", build, status=201,
                extra_headers={
                    "Location": f"/2020-05-31/distribution-tenant/{tenant['Id']}/invalidation/{existing['Id']}",
                },
            )

    inv = {
        "Id": _inv_id(),
        "Status": "Completed",
        "CreateTime": _now_iso(),
        "InvalidationBatch": {
            "Paths": {"Quantity": len(path_items), "Items": path_items},
            "CallerReference": caller_ref,
        },
    }
    invs.append(inv)
    logger.info("CreateInvalidationForDistributionTenant tenant=%s inv=%s", tenant["Id"], inv["Id"])

    def build(root):
        _build_invalidation_xml(root, inv)

    return _xml_response(
        "Invalidation", build, status=201,
        extra_headers={
            "Location": f"/2020-05-31/distribution-tenant/{tenant['Id']}/invalidation/{inv['Id']}",
        },
    )


def _list_tenant_invalidations(tenant_id):
    tenant = _find_distribution_tenant(tenant_id)
    if not tenant:
        return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
    invs = _tenant_invalidations.get(tenant["Id"], [])

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
                SubElement(summary, "CreateTime").text = inv["CreateTime"]
                SubElement(summary, "Status").text = inv["Status"]

    return _xml_response("InvalidationList", build)


def _get_tenant_invalidation(tenant_id, inv_id):
    tenant = _find_distribution_tenant(tenant_id)
    if not tenant:
        return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
    inv = next((i for i in _tenant_invalidations.get(tenant["Id"], []) if i["Id"] == inv_id), None)
    if not inv:
        return _error("NoSuchInvalidation", "The specified invalidation does not exist.", 404)

    def build(root):
        _build_invalidation_xml(root, inv)

    return _xml_response("Invalidation", build)


# ---- domain and certificate handlers ----


def _verify_dns_configuration(body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    identifier = _text(el, "Identifier")
    if not identifier:
        return _error("InvalidArgument", "Identifier is required.", 400)
    tenant = _find_distribution_tenant(identifier)
    if not tenant:
        return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
    domain = _text(el, "Domain")
    domains = [domain] if domain else tenant["Domains"]

    def build(root):
        list_el = SubElement(root, "DnsConfigurationList")
        for d in domains:
            item = SubElement(list_el, "DnsConfiguration")
            SubElement(item, "Domain").text = d
            SubElement(item, "Status").text = "valid-configuration"

    return _xml_response("VerifyDnsConfigurationResult", build)


def _get_managed_certificate_details(identifier):
    tenant = _find_distribution_tenant(identifier)
    if not tenant:
        return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
    mc = tenant.get("ManagedCertificate")

    def build(root):
        if not mc:
            return
        SubElement(root, "CertificateArn").text = mc["CertificateArn"]
        SubElement(root, "CertificateStatus").text = mc["CertificateStatus"]
        SubElement(root, "ValidationTokenHost").text = mc["ValidationTokenHost"]
        details = SubElement(root, "ValidationTokenDetails")
        for d in mc["Domains"]:
            item = SubElement(details, "member")
            SubElement(item, "Domain").text = d

    return _xml_response("ManagedCertificateDetails", build)


def _list_domain_conflicts(body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    domain = _text(el, "Domain")
    resource_el = _find(el, "DomainControlValidationResource")
    if not domain or resource_el is None:
        return _error("InvalidArgument", "Domain and DomainControlValidationResource are required.", 400)
    tenant_ref = _text(resource_el, "DistributionTenantId")
    dist_ref = _text(resource_el, "DistributionId")
    exclude_ids = set()
    if tenant_ref:
        tenant = _find_distribution_tenant(tenant_ref)
        if not tenant:
            return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
        exclude_ids.add(tenant["Id"])
    elif dist_ref:
        if dist_ref not in _distributions:
            return _error("EntityNotFound", "The specified distribution does not exist.", 404)
        exclude_ids.add(dist_ref)
    else:
        return _error("InvalidArgument", "DomainControlValidationResource requires a resource id.", 400)

    conflicts = [
        (rtype, rid)
        for rtype, rid in _domain_owners(domain, wildcard_overlap=True)
        if rid not in exclude_ids
    ]

    def build(root):
        list_el = SubElement(root, "DomainConflicts")
        for rtype, rid in conflicts:
            # The list member's XML name is DomainConflicts per the model.
            item = SubElement(list_el, "DomainConflicts")
            SubElement(item, "Domain").text = domain
            SubElement(item, "ResourceType").text = rtype
            SubElement(item, "ResourceId").text = rid
            SubElement(item, "AccountId").text = get_account_id()

    return _xml_response("ListDomainConflictsResult", build)


def _update_domain_association(headers, body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    domain = _text(el, "Domain")
    target_el = _find(el, "TargetResource")
    if not domain or target_el is None:
        return _error("InvalidArgument", "Domain and TargetResource are required.", 400)

    tenant_ref = _text(target_el, "DistributionTenantId")
    dist_ref = _text(target_el, "DistributionId")
    target_tenant = target_dist = None
    if tenant_ref:
        target_tenant = _find_distribution_tenant(tenant_ref)
        if not target_tenant:
            return _error("EntityNotFound", "The specified distribution tenant does not exist.", 404)
        target = target_tenant
    elif dist_ref:
        target_dist = _distributions.get(dist_ref)
        if not target_dist:
            return _error("EntityNotFound", "The specified distribution does not exist.", 404)
        target = target_dist
    else:
        return _error("InvalidArgument", "TargetResource requires a resource id.", 400)

    if_match = headers.get("if-match")
    if if_match and if_match != target["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )

    # Detach the domain from whichever resource currently claims it.
    d = domain.lower()
    for tenant in _distribution_tenants.values():
        if tenant is target_tenant:
            continue
        if any(x.lower() == d for x in tenant["Domains"]):
            tenant["Domains"] = [x for x in tenant["Domains"] if x.lower() != d]
            tenant["ETag"] = new_uuid()
            tenant["LastModifiedTime"] = _now_iso()
    for dist in _distributions.values():
        if dist is target_dist:
            continue
        aliases = _get_distribution_aliases(dist)
        if any(x.lower() == d for x in aliases):
            _set_distribution_aliases(dist, [x for x in aliases if x.lower() != d])
            dist["ETag"] = new_uuid()
            dist["LastModifiedTime"] = _now_iso()

    # Attach it to the target.
    if target_tenant is not None:
        if not any(x.lower() == d for x in target_tenant["Domains"]):
            target_tenant["Domains"] = [*target_tenant["Domains"], domain]
        resource_id = target_tenant["Id"]
    else:
        aliases = _get_distribution_aliases(target_dist)
        if not any(x.lower() == d for x in aliases):
            _set_distribution_aliases(target_dist, [*aliases, domain])
        resource_id = target_dist["Id"]
    target["ETag"] = new_uuid()
    target["LastModifiedTime"] = _now_iso()
    logger.info("UpdateDomainAssociation domain=%s target=%s", domain, resource_id)

    def build(root):
        SubElement(root, "Domain").text = domain
        SubElement(root, "ResourceId").text = resource_id

    return _xml_response("UpdateDomainAssociationResult", build, extra_headers={"ETag": target["ETag"]})


def _list_distributions_by_connection_mode(mode):
    if mode not in _CONNECTION_MODES:
        return _error("InvalidArgument", "Invalid ConnectionMode value.", 400)
    items = [d for d in _distributions.values() if _dist_connection_mode(d) == mode]
    return _xml_response("DistributionList", lambda root: _build_distribution_list_xml(root, items))
