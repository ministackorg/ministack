# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
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
                 UpdateCachePolicy, DeleteCachePolicy, ListCachePolicies,
                 ListDistributionsByCachePolicyId — includes the fixed catalog
                 of AWS-managed policies (Type=managed), which are immutable
  Origin request policies: CreateOriginRequestPolicy, GetOriginRequestPolicy,
                 GetOriginRequestPolicyConfig, UpdateOriginRequestPolicy,
                 DeleteOriginRequestPolicy, ListOriginRequestPolicies,
                 ListDistributionsByOriginRequestPolicyId — includes the
                 AWS-managed policy catalog (Type=managed), immutable
  Response headers policies: CreateResponseHeadersPolicy, GetResponseHeadersPolicy,
                 GetResponseHeadersPolicyConfig, UpdateResponseHeadersPolicy,
                 DeleteResponseHeadersPolicy, ListResponseHeadersPolicies,
                 ListDistributionsByResponseHeadersPolicyId — includes the
                 AWS-managed policy catalog (Type=managed), immutable
  Monitoring subscriptions: CreateMonitoringSubscription, GetMonitoringSubscription,
                 DeleteMonitoringSubscription
  Public keys: CreatePublicKey, GetPublicKey, GetPublicKeyConfig,
                 UpdatePublicKey, DeletePublicKey, ListPublicKeys
  Key groups: CreateKeyGroup, GetKeyGroup, GetKeyGroupConfig,
                 UpdateKeyGroup, DeleteKeyGroup, ListKeyGroups
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
import fnmatch
import http.client
import logging
import os
import random
import re
import ssl
import string
import uuid
from datetime import datetime, timezone
from re import compile as _re_compile
from re import escape as _re_escape
from urllib.parse import quote, unquote, urlencode
from xml.etree.ElementTree import Element, SubElement, tostring

from defusedxml.ElementTree import fromstring

from ministack.core import node_pool
from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import run_offloop, run_reentrant
from ministack.core.responses import AccountScopedDict, get_account_id, new_uuid
from ministack.services import s3

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

_PUBLIC_KEY_RE = re.compile(r"^/2020-05-31/public-key/?$")
_PUBLIC_KEY_CFG_RE = re.compile(r"^/2020-05-31/public-key/([^/]+)/config$")
_PUBLIC_KEY_ID_RE = re.compile(r"^/2020-05-31/public-key/([^/]+)/?$")

_KEY_GROUP_RE = re.compile(r"^/2020-05-31/key-group/?$")
_KEY_GROUP_CFG_RE = re.compile(r"^/2020-05-31/key-group/([^/]+)/config$")
_KEY_GROUP_ID_RE = re.compile(r"^/2020-05-31/key-group/([^/]+)/?$")

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
_public_keys = AccountScopedDict()  # Id -> public key record
_key_groups = AccountScopedDict()  # Id -> key group record
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
    _public_keys.clear()
    _key_groups.clear()
    _connection_groups.clear()
    _distribution_tenants.clear()
    _tenant_invalidations.clear()
    _cf_functions_reset()


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
            "public_keys": _public_keys,
            "key_groups": _key_groups,
            "connection_groups": _connection_groups,
            "distribution_tenants": _distribution_tenants,
            "tenant_invalidations": _tenant_invalidations,
        }
    )


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    _distributions.update(data.get("distributions", {}))
    _invalidations.update(data.get("invalidations", {}))
    _tags.update(data.get("tags", {}))
    _oacs.update(data.get("oacs", {}))
    _functions.update(data.get("functions", {}))
    _kvstores.update(data.get("kvstores", {}))
    _cache_policies.update(data.get("cache_policies", {}))
    _origin_request_policies.update(data.get("origin_request_policies", {}))
    _response_headers_policies.update(data.get("response_headers_policies", {}))
    _public_keys.update(data.get("public_keys", {}))
    _key_groups.update(data.get("key_groups", {}))
    _connection_groups.update(data.get("connection_groups", {}))
    _distribution_tenants.update(data.get("distribution_tenants", {}))
    _tenant_invalidations.update(data.get("tenant_invalidations", {}))




# ---------------------------------------------------------------------------
# ID generators — real CloudFront uses 14-char uppercase alphanumeric IDs
# ---------------------------------------------------------------------------
_ID_CHARS = string.ascii_uppercase + string.digits


def _dist_id() -> str:
    return "E" + "".join(random.choices(_ID_CHARS, k=13))


def _inv_id() -> str:
    return "I" + "".join(random.choices(_ID_CHARS, k=13))


def _pk_id() -> str:
    return "K" + "".join(random.choices(_ID_CHARS, k=13))


def _kg_id() -> str:
    return "K" + "".join(random.choices(_ID_CHARS, k=13))


# Real SaaS Manager resource IDs are dt_/cg_-prefixed KSUIDs (27 base62 chars).
_KSUID_CHARS = string.ascii_letters + string.digits


def _tenant_id() -> str:
    return "dt_" + "".join(random.choices(_KSUID_CHARS, k=27))


def _conn_group_id() -> str:
    return "cg_" + "".join(random.choices(_KSUID_CHARS, k=27))


def _cloudfront_domain() -> str:
    """AWS's edge domain shape: 'd' + 13 lowercase alphanumerics +
    '.cloudfront.net' (e.g. d111111abcdef8.cloudfront.net) — assigned to both
    a distribution's DomainName and a connection group's RoutingEndpoint."""
    return "d" + "".join(random.choices(string.ascii_lowercase + string.digits, k=13)) + ".cloudfront.net"


def _new_distribution_domain() -> str:
    """A `_cloudfront_domain()` value not already used by a distribution in
    this account."""
    existing = {d["DomainName"] for d in _distributions.values()}
    domain = _cloudfront_domain()
    while domain in existing:
        domain = _cloudfront_domain()
    return domain


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


def _strip_namespace(el):
    """Rewrite an element tree's tags to their local names, in place.

    A stored ``config_xml`` is the client's own request body, which declares
    ``xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"`` on
    ``DistributionConfig`` — so ``fromstring`` qualifies every tag as
    ``{ns}Tag``. Re-serialising that inside a response whose root carries
    ``xmlns`` as a plain attribute makes ElementTree invent an ``ns0:`` prefix
    for all of them, and AWS SDKs parse REST-XML without namespace awareness:
    ``ns0:Origins`` does not match ``Origins``, so the whole config reads as
    absent. Only the root tag was being corrected, which is why
    ``GetDistribution`` returned a ``DistributionConfig`` containing nothing but
    the handful of elements built with unqualified names.
    """
    el.tag = _local_tag_name(el)
    for child in el:
        _strip_namespace(child)
    return el


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


# CloudFormation's DistributionConfig is the API's, in JSON, with a few
# differences: a {Quantity, Items} block is a plain list (Aliases, Origins,
# CacheBehaviors, AllowedMethods, ...; OriginGroups and GeoRestriction keep
# the block), CachedMethods sits next to AllowedMethods instead of inside it,
# and these member names are spelled differently. Keyed by the API structure
# the member sits in.
_CFN_DISTRIBUTION_CONFIG_RENAMES = {
    ("DistributionConfig", "IPV6Enabled"): "IsIPV6Enabled",
    ("Origin", "OriginCustomHeaders"): "CustomHeaders",
    ("CustomOriginConfig", "OriginSSLProtocols"): "OriginSslProtocols",
    ("ViewerCertificate", "AcmCertificateArn"): "ACMCertificateArn",
    ("ViewerCertificate", "IamCertificateId"): "IAMCertificateId",
    ("ViewerCertificate", "SslSupportMethod"): "SSLSupportMethod",
    ("GeoRestriction", "Locations"): "Items",
}

_api_model = None


def _api_shape(name):
    """A shape of the CloudFront API model botocore ships, loaded once."""
    global _api_model
    if _api_model is None:
        import botocore.session

        _api_model = botocore.session.get_session().get_service_model("cloudfront")
    return _api_model.shape_for(name)


def _distribution_config_xml(config: dict):
    """The ``DistributionConfig`` element for a configuration given in
    CloudFormation's JSON shape: what ``CreateDistribution`` would have
    stored had the same configuration arrived on the wire, so every read
    path parses a CloudFormation-provisioned distribution like any other.
    Walks the API model, so a member the API defines renders and one it
    does not (the legacy ``CNAMEs``, ``CustomOrigin``, ``S3Origin``) is
    dropped; an element that would come out empty is omitted, as the API
    omits ``Items`` at ``Quantity`` 0."""
    root = Element("DistributionConfig")
    _render_config_member(root, _api_shape("DistributionConfig"), config)
    return root


def _render_config_member(el, shape, value):
    if shape.type_name == "structure":
        if not isinstance(value, dict):
            return
        members = shape.members
        value = {_CFN_DISTRIBUTION_CONFIG_RENAMES.get((shape.name, k), k): v
                 for k, v in value.items()}
        if "AllowedMethods" in members and (
                isinstance(value.get("AllowedMethods"), list) or "CachedMethods" in value):
            # The API nests CachedMethods inside AllowedMethods, so a
            # CachedMethods on its own needs one: GET and HEAD, the first of
            # the three choices the reference lists (it states no default).
            value["AllowedMethods"] = {"Items": value.get("AllowedMethods") or ["GET", "HEAD"],
                                       "CachedMethods": value.pop("CachedMethods", None)}
        if "Quantity" in members and "Items" in members:
            value.setdefault("Quantity", len(value.get("Items") or []))
        if "Enabled" in members and "Enabled" not in value:
            # Six structures carry an Enabled the template may omit:
            # TrustedSigners and TrustedKeyGroups are on when they list
            # anything; Logging, OriginShield and GrpcConfig are on when the
            # block is present; DistributionConfig defaults to on, as
            # _get_enabled reads it.
            value["Enabled"] = bool(value.get("Items")) if "Items" in members else True
        for name, member in members.items():
            item = value.get(name)
            if item is None:
                continue
            if member.type_name == "structure" and "Items" in member.members and isinstance(item, list):
                item = {"Items": item}
            _append_config_member(el, member.serialization.get("name", name), member, item)
    elif shape.type_name == "list":
        if not isinstance(value, list):
            return
        tag = shape.member.serialization.get("name", "member")
        for item in value:
            _append_config_member(el, tag, shape.member, item)
    elif isinstance(value, bool):
        el.text = "true" if value else "false"
    elif not isinstance(value, (dict, list)):
        el.text = str(value)


def _append_config_member(parent, tag, shape, value):
    """Render ``value`` under ``tag`` and attach it only when something came
    out: an empty list, or a structure none of whose keys the API knows,
    leaves no element behind."""
    child = Element(tag)
    _render_config_member(child, shape, value)
    if len(child) or child.text is not None:
        parent.append(child)


def _build_distribution_xml(parent, dist):
    """Append Distribution child elements to parent."""
    SubElement(parent, "Id").text = dist["Id"]
    SubElement(parent, "ARN").text = dist["ARN"]
    SubElement(parent, "Status").text = dist["Status"]
    SubElement(parent, "LastModifiedTime").text = dist["LastModifiedTime"]
    SubElement(parent, "InProgressInvalidationBatches").text = "0"
    SubElement(parent, "DomainName").text = dist["DomainName"]
    # Re-parse and embed the stored config XML
    config_el = _strip_namespace(fromstring(dist["config_xml"]))
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


def _function_view(fn: dict, stage: str) -> dict:
    """The function body a stage serves.

    An update lands in DEVELOPMENT only — "The changes are made only to the
    version of the function that is in the DEVELOPMENT stage. To copy the
    updates from the DEVELOPMENT stage to LIVE, you must publish the function"
    — so LIVE keeps serving the body captured at the last publish. A record
    from before this snapshot existed falls back to the live body.
    """
    if stage == "LIVE":
        return fn.get("live_body") or fn
    return fn


def _function_summary_builder(fn: dict, stage: str, status: str, last_modified: str):
    view = _function_view(fn, stage)

    def build(root):
        fc = SubElement(root, "FunctionConfig")
        SubElement(fc, "Comment").text = view.get("comment", "")
        kvs_arns = view.get("kvs_arns", [])
        kvs = SubElement(fc, "KeyValueStoreAssociations")
        SubElement(kvs, "Quantity").text = str(len(kvs_arns))
        items_el = SubElement(kvs, "Items")
        for arn in kvs_arns:
            assoc = SubElement(items_el, "KeyValueStoreAssociation")
            SubElement(assoc, "KeyValueStoreARN").text = arn
        SubElement(fc, "Runtime").text = view.get("runtime", fn["runtime"])
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
        # The body PublishFunction froze; LIVE serves this, not the working copy.
        "live_body": None,
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
        code = _function_view(fn, "LIVE").get("code", fn["code"])
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
    # Publishing copies DEVELOPMENT to LIVE; later updates to the development
    # body must not reach the stage that is serving traffic.
    fn["live_body"] = {
        "comment": fn["comment"],
        "runtime": fn["runtime"],
        "kvs_arns": list(fn.get("kvs_arns", [])),
        "code": fn["code"],
    }
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
    # LIVE is untouched: an update changes the DEVELOPMENT stage only, and the
    # published version keeps serving until PublishFunction copies this one.
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


# ---------------------------------------------------------------------------
# AWS-managed policies (cache / origin request / response headers).
#
# Real CloudFront ships a fixed catalog of these under every account; Terraform
# modules reference them by name (e.g. "Managed-CachingDisabled") via the
# aws_cloudfront_cache_policy/aws_cloudfront_origin_request_policy/
# aws_cloudfront_response_headers_policy data sources, and ListCachePolicies
# et al. must report them with Type=managed. They are immutable (Update/Delete
# error) and identical for every account, so they are seeded once as module
# constants rather than per-account state — nothing resets or persists them
# because nothing ever mutates them.
#
# Evidence: AWS docs — "Use managed cache policies", "Use managed origin
# request policies", "Use managed response headers policies" (CloudFront
# Developer Guide) for names/ids/configs. The literal "Managed-" Name prefix
# (docs show only the short console name) is confirmed by ops-v2's
# modules/cloudfront-api, which already looks policies up by e.g.
# "Managed-CachingDisabled". LastModifiedTime below is a stable placeholder,
# not an AWS-observed value.
# ---------------------------------------------------------------------------

_MANAGED_POLICY_LAST_MODIFIED = "2020-05-20T04:29:32.290Z"


def _managed_etag(policy_id: str) -> str:
    """A stable, unique ETag for a managed policy (real AWS ETags are opaque)."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ministack-cloudfront-managed-policy/{policy_id}"))


def _managed_policy(policy_id: str, config: dict) -> dict:
    return {
        "Id": policy_id,
        "ETag": _managed_etag(policy_id),
        "LastModifiedTime": _MANAGED_POLICY_LAST_MODIFIED,
        "Config": config,
    }


def _cache_params(gzip, brotli, header_behavior, headers, cookie_behavior, cookies, qs_behavior, qs):
    return {
        "EnableAcceptEncodingGzip": gzip,
        "EnableAcceptEncodingBrotli": brotli,
        "HeaderBehavior": header_behavior, "Headers": list(headers),
        "CookieBehavior": cookie_behavior, "Cookies": list(cookies),
        "QueryStringBehavior": qs_behavior, "QueryStrings": list(qs),
    }


_MANAGED_CACHE_POLICIES = {
    pid: _managed_policy(pid, cfg)
    for pid, cfg in {
        "2e54312d-136d-493c-8eb9-b001f22f67d2": {
            "Name": "Managed-Amplify", "Comment": "", "MinTTL": 2, "DefaultTTL": 2, "MaxTTL": 600,
            "Parameters": _cache_params(
                True, True, "whitelist", ["Authorization", "CloudFront-Viewer-Country", "Host"],
                "all", [], "all", [],
            ),
        },
        "4135ea2d-6df8-44a3-9df3-4b5a84be39ad": {
            "Name": "Managed-CachingDisabled", "Comment": "", "MinTTL": 0, "DefaultTTL": 0, "MaxTTL": 0,
            "Parameters": _cache_params(False, False, "none", [], "none", [], "none", []),
        },
        "658327ea-f89d-4fab-a63d-7e88639e58f6": {
            "Name": "Managed-CachingOptimized", "Comment": "", "MinTTL": 1, "DefaultTTL": 86400, "MaxTTL": 31536000,
            "Parameters": _cache_params(True, True, "none", [], "none", [], "none", []),
        },
        "b2884449-e4de-46a7-ac36-70bc7f1ddd6d": {
            "Name": "Managed-CachingOptimizedForUncompressedObjects", "Comment": "",
            "MinTTL": 1, "DefaultTTL": 86400, "MaxTTL": 31536000,
            "Parameters": _cache_params(False, False, "none", [], "none", [], "none", []),
        },
        "08627262-05a9-4f76-9ded-b50ca2e3a84f": {
            "Name": "Managed-Elemental-MediaPackage", "Comment": "",
            "MinTTL": 0, "DefaultTTL": 86400, "MaxTTL": 31536000,
            "Parameters": _cache_params(
                True, False, "whitelist", ["Origin"], "none", [],
                "whitelist", ["aws.manifestfilter", "start", "end", "m"],
            ),
        },
        "83da9c7e-98b4-4e11-a168-04f0df8e2c65": {
            "Name": "Managed-UseOriginCacheControlHeaders", "Comment": "",
            "MinTTL": 0, "DefaultTTL": 0, "MaxTTL": 31536000,
            "Parameters": _cache_params(
                True, True, "whitelist",
                ["Host", "Origin", "X-HTTP-Method-Override", "X-HTTP-Method", "X-Method-Override"],
                "all", [], "none", [],
            ),
        },
        "4cc15a8a-d715-48a4-82b8-cc0b614638fe": {
            "Name": "Managed-UseOriginCacheControlHeaders-QueryStrings", "Comment": "",
            "MinTTL": 0, "DefaultTTL": 0, "MaxTTL": 31536000,
            "Parameters": _cache_params(
                True, True, "whitelist",
                ["Host", "Origin", "X-HTTP-Method-Override", "X-HTTP-Method", "X-Method-Override"],
                "all", [], "all", [],
            ),
        },
    }.items()
}

_ILLEGAL_UPDATE_MSG = "The specified CloudFront managed policy cannot be updated."
_ILLEGAL_DELETE_MSG = "The specified CloudFront managed policy cannot be deleted."


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
    for existing in (*_cache_policies.values(), *_MANAGED_CACHE_POLICIES.values()):
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
    policy = _cache_policies.get(policy_id) or _MANAGED_CACHE_POLICIES.get(policy_id)
    if not policy:
        return _error("NoSuchCachePolicy", "The cache policy does not exist.", 404)

    def build(root):
        _build_cache_policy_xml(root, policy)

    return _xml_response("CachePolicy", build, extra_headers={"ETag": policy["ETag"]})


def _get_cache_policy_config(policy_id):
    policy = _cache_policies.get(policy_id) or _MANAGED_CACHE_POLICIES.get(policy_id)
    if not policy:
        return _error("NoSuchCachePolicy", "The cache policy does not exist.", 404)

    def build(root):
        _build_cache_policy_config_xml(root, policy["Config"])

    return _xml_response("CachePolicyConfig", build, extra_headers={"ETag": policy["ETag"]})


def _update_cache_policy(policy_id, headers, body):
    if policy_id in _MANAGED_CACHE_POLICIES:
        return _error("IllegalUpdate", _ILLEGAL_UPDATE_MSG, 400)
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
    for existing in (*_cache_policies.values(), *_MANAGED_CACHE_POLICIES.values()):
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
    if policy_id in _MANAGED_CACHE_POLICIES:
        return _error("IllegalDelete", _ILLEGAL_DELETE_MSG, 400)
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
    if not _cache_policies.get(policy_id) and policy_id not in _MANAGED_CACHE_POLICIES:
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


def _policy_lookup(store, spec, pid):
    """Custom store first, then the type's AWS-managed catalog."""
    return store.get(pid) or spec["managed"].get(pid)


def _policy_create(store, spec, body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    cfg, err = spec["parse"](el)
    if err is not None:
        return err
    for existing in (*store.values(), *spec["managed"].values()):
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
    policy = _policy_lookup(store, spec, pid)
    if not policy:
        return _error(spec["missing"], f"The {spec['label']} does not exist.", 404)
    return _xml_response(spec["resource_tag"], lambda r: spec["build_resource"](r, policy),
                         extra_headers={"ETag": policy["ETag"]})


def _policy_get_config(store, spec, pid):
    policy = _policy_lookup(store, spec, pid)
    if not policy:
        return _error(spec["missing"], f"The {spec['label']} does not exist.", 404)
    return _xml_response(spec["config_tag"], lambda r: spec["build_config"](r, policy["Config"]),
                         extra_headers={"ETag": policy["ETag"]})


def _policy_update(store, spec, pid, headers, body):
    if pid in spec["managed"]:
        return _error("IllegalUpdate", _ILLEGAL_UPDATE_MSG, 400)
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
    for existing in (*store.values(), *spec["managed"].values()):
        if existing["Id"] != pid and existing["Config"]["Name"] == cfg["Name"]:
            return _error(spec["dup"], f"A {spec['label']} with the same name already exists.", 409)
    new_etag = new_uuid()
    policy["Config"] = cfg
    policy["ETag"] = new_etag
    policy["LastModifiedTime"] = _now_iso()
    return _xml_response(spec["resource_tag"], lambda r: spec["build_resource"](r, policy),
                         extra_headers={"ETag": new_etag})


def _policy_delete(store, spec, pid, headers):
    if pid in spec["managed"]:
        return _error("IllegalDelete", _ILLEGAL_DELETE_MSG, 400)
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
    if not _policy_lookup(store, spec, pid):
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


def _orp_cfg(name, header_behavior, headers=(), cookie_behavior="none", qs_behavior="none", qs=()):
    return {
        "Name": name, "Comment": "",
        "HeaderBehavior": header_behavior, "Headers": list(headers),
        "CookieBehavior": cookie_behavior, "Cookies": [],
        "QueryStringBehavior": qs_behavior, "QueryStrings": list(qs),
    }


# Evidence: AWS docs "Use managed origin request policies" (CloudFront
# Developer Guide) for names/ids/behaviors; see the catalog note above
# _MANAGED_CACHE_POLICIES for the "Managed-" Name-prefix evidence.
_MANAGED_ORIGIN_REQUEST_POLICIES = {
    pid: _managed_policy(pid, cfg)
    for pid, cfg in {
        "216adef6-5c7f-47e4-b989-5492eafa07d3": _orp_cfg(
            "Managed-AllViewer", "allViewer", cookie_behavior="all", qs_behavior="all",
        ),
        "33f36d7e-f396-46d9-90e0-52428a34d9dc": _orp_cfg(
            "Managed-AllViewerAndCloudFrontHeaders-2022-06", "allViewerAndWhitelistCloudFront",
            headers=[
                "CloudFront-Forwarded-Proto", "CloudFront-Is-Android-Viewer", "CloudFront-Is-Desktop-Viewer",
                "CloudFront-Is-IOS-Viewer", "CloudFront-Is-Mobile-Viewer", "CloudFront-Is-SmartTV-Viewer",
                "CloudFront-Is-Tablet-Viewer", "CloudFront-Viewer-Address", "CloudFront-Viewer-ASN",
                "CloudFront-Viewer-City", "CloudFront-Viewer-Country", "CloudFront-Viewer-Country-Name",
                "CloudFront-Viewer-Country-Region", "CloudFront-Viewer-Country-Region-Name",
                "CloudFront-Viewer-Http-Version", "CloudFront-Viewer-Latitude", "CloudFront-Viewer-Longitude",
                "CloudFront-Viewer-Metro-Code", "CloudFront-Viewer-Postal-Code", "CloudFront-Viewer-Time-Zone",
                "CloudFront-Viewer-TLS",
            ],
            cookie_behavior="all", qs_behavior="all",
        ),
        "b689b0a8-53d0-40ab-baf2-68738e2966ac": _orp_cfg(
            "Managed-AllViewerExceptHostHeader", "allExcept", headers=["Host"],
            cookie_behavior="all", qs_behavior="all",
        ),
        "59781a5b-3903-41f3-afcb-af62929ccde1": _orp_cfg(
            "Managed-CORS-CustomOrigin", "whitelist", headers=["Origin"],
        ),
        "88a5eaf4-2fd4-4709-b370-b4c650ea3fcf": _orp_cfg(
            "Managed-CORS-S3Origin", "whitelist",
            headers=["Origin", "Access-Control-Request-Headers", "Access-Control-Request-Method"],
        ),
        "775133bc-15f2-49f9-abea-afb2e0bf67d2": _orp_cfg(
            "Managed-Elemental-MediaTailor-PersonalizedManifests", "whitelist",
            headers=["Origin", "Access-Control-Request-Headers", "Access-Control-Request-Method",
                     "User-Agent", "X-Forwarded-For"],
            qs_behavior="all",
        ),
        "bf0718e1-ba1e-49d1-88b1-f726733018ae": _orp_cfg(
            "Managed-HostHeaderOnly", "whitelist", headers=["Host"],
        ),
        "acba4595-bd28-49b8-b9fe-13317c0390fa": _orp_cfg(
            "Managed-UserAgentRefererHeaders", "whitelist", headers=["User-Agent", "Referer"],
        ),
    }.items()
}


_ORP_SPEC = {
    "label": "origin request policy", "resource_tag": "OriginRequestPolicy",
    "config_tag": "OriginRequestPolicyConfig", "path": "/2020-05-31/origin-request-policy",
    "list_tag": "OriginRequestPolicyList", "summary_tag": "OriginRequestPolicySummary",
    "missing": "NoSuchOriginRequestPolicy", "dup": "OriginRequestPolicyAlreadyExists",
    "in_use": "OriginRequestPolicyInUse", "parse": _parse_orp_config,
    "build_resource": _build_orp_xml, "build_config": _build_orp_config_xml,
    "managed": _MANAGED_ORIGIN_REQUEST_POLICIES,
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


def _rhp_managed_cors(allow_methods=(), expose_headers=None):
    return {
        "AllowOrigins": ["*"], "AllowHeaders": [], "AllowMethods": list(allow_methods),
        "AllowCredentials": False, "OriginOverride": False,
        "ExposeHeaders": expose_headers, "MaxAgeSec": None,
    }


# The full security-headers set shared by SecurityHeadersPolicy and both
# combined CORS+security managed policies (identical settings per AWS docs).
_RHP_MANAGED_SECURITY = {
    "XSSProtection": {"Override": False, "Protection": True, "ModeBlock": True, "ReportUri": None},
    "FrameOptions": {"Override": False, "FrameOption": "SAMEORIGIN"},
    "ReferrerPolicy": {"Override": False, "ReferrerPolicy": "strict-origin-when-cross-origin"},
    "ContentTypeOptions": {"Override": True},
    "StrictTransportSecurity": {
        "Override": False, "IncludeSubdomains": None, "Preload": None, "AccessControlMaxAgeSec": 31536000,
    },
}


def _rhp_cfg(name, cors=None, security=None):
    return {
        "Name": name, "Comment": "", "Cors": cors, "Security": security,
        "ServerTiming": None, "CustomHeaders": [], "RemoveHeaders": [],
    }


# Evidence: AWS docs "Use managed response headers policies" (CloudFront
# Developer Guide) for names/ids/CORS+security settings; see the catalog note
# above _MANAGED_CACHE_POLICIES for the "Managed-" Name-prefix evidence.
_MANAGED_RESPONSE_HEADERS_POLICIES = {
    pid: _managed_policy(pid, cfg)
    for pid, cfg in {
        "e61eb60c-9c35-4d20-a928-2b84e02af89c": _rhp_cfg(
            "Managed-CORS-and-SecurityHeadersPolicy",
            cors=_rhp_managed_cors(), security=dict(_RHP_MANAGED_SECURITY),
        ),
        "5cc3b908-e619-4b99-88e5-2cf7f45965bd": _rhp_cfg(
            "Managed-CORS-With-Preflight",
            cors=_rhp_managed_cors(
                allow_methods=["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
                expose_headers=["*"],
            ),
        ),
        "eaab4381-ed33-4a86-88ca-d9558dc6cd63": _rhp_cfg(
            "Managed-CORS-with-preflight-and-SecurityHeadersPolicy",
            cors=_rhp_managed_cors(
                allow_methods=["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
                expose_headers=["*"],
            ),
            security=dict(_RHP_MANAGED_SECURITY),
        ),
        "67f7725c-6f97-4210-82d7-5512b31e9d03": _rhp_cfg(
            "Managed-SecurityHeadersPolicy", security=dict(_RHP_MANAGED_SECURITY),
        ),
        "60669652-455b-4ae9-85a4-c4c02393f86c": _rhp_cfg(
            "Managed-SimpleCORS", cors=_rhp_managed_cors(),
        ),
    }.items()
}


_RHP_SPEC = {
    "label": "response headers policy", "resource_tag": "ResponseHeadersPolicy",
    "config_tag": "ResponseHeadersPolicyConfig", "path": "/2020-05-31/response-headers-policy",
    "list_tag": "ResponseHeadersPolicyList", "summary_tag": "ResponseHeadersPolicySummary",
    "missing": "NoSuchResponseHeadersPolicy", "dup": "ResponseHeadersPolicyAlreadyExists",
    "in_use": "ResponseHeadersPolicyInUse", "parse": _parse_rhp_config,
    "build_resource": _build_rhp_xml, "build_config": _build_rhp_config_xml,
    "managed": _MANAGED_RESPONSE_HEADERS_POLICIES,
}


# ---------------------------------------------------------------------------
# Public keys
# ---------------------------------------------------------------------------


def _parse_public_key_config(el):
    caller_reference = _text(el, "CallerReference")
    name = _text(el, "Name")
    encoded_key = _text(el, "EncodedKey")
    if not caller_reference or not name or not encoded_key:
        return None, _error(
            "InvalidArgument", "CallerReference, Name, and EncodedKey are required.", 400
        )
    return {
        "CallerReference": caller_reference, "Name": name,
        "EncodedKey": encoded_key, "Comment": _text(el, "Comment"),
    }, None


def _build_public_key_config_xml(parent, cfg):
    SubElement(parent, "CallerReference").text = cfg["CallerReference"]
    SubElement(parent, "Name").text = cfg["Name"]
    SubElement(parent, "EncodedKey").text = cfg["EncodedKey"]
    if cfg.get("Comment"):
        SubElement(parent, "Comment").text = cfg["Comment"]


def _build_public_key_xml(parent, pk):
    SubElement(parent, "Id").text = pk["Id"]
    SubElement(parent, "CreatedTime").text = pk["CreatedTime"]
    cfg_el = SubElement(parent, "PublicKeyConfig")
    _build_public_key_config_xml(cfg_el, pk["Config"])


def _public_keys_using(pk_id):
    return [kg.get("Id", "") for kg in _key_groups.values() if pk_id in kg["Config"].get("Items", [])]


def _create_public_key(body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    cfg, err = _parse_public_key_config(el)
    if err is not None:
        return err
    pk_id = _pk_id()
    etag = new_uuid()
    pk = {"Id": pk_id, "ETag": etag, "CreatedTime": _now_iso(), "Config": cfg}
    _public_keys[pk_id] = pk
    logger.info("CreatePublicKey id=%s name=%s", pk_id, cfg["Name"])
    return _xml_response(
        "PublicKey", lambda r: _build_public_key_xml(r, pk), status=201,
        extra_headers={"ETag": etag, "Location": f"/2020-05-31/public-key/{pk_id}"},
    )


def _get_public_key(pk_id):
    pk = _public_keys.get(pk_id)
    if not pk:
        return _error("NoSuchPublicKey", "The public key does not exist.", 404)
    return _xml_response("PublicKey", lambda r: _build_public_key_xml(r, pk), extra_headers={"ETag": pk["ETag"]})


def _get_public_key_config(pk_id):
    pk = _public_keys.get(pk_id)
    if not pk:
        return _error("NoSuchPublicKey", "The public key does not exist.", 404)
    return _xml_response(
        "PublicKeyConfig", lambda r: _build_public_key_config_xml(r, pk["Config"]),
        extra_headers={"ETag": pk["ETag"]},
    )


def _update_public_key(pk_id, headers, body):
    pk = _public_keys.get(pk_id)
    if not pk:
        return _error("NoSuchPublicKey", "The public key does not exist.", 404)
    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != pk["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    cfg, err = _parse_public_key_config(el)
    if err is not None:
        return err
    # Only the comment can change.
    if any(cfg[k] != pk["Config"][k] for k in ("CallerReference", "Name", "EncodedKey")):
        return _error("CannotChangeImmutablePublicKeyFields", "Only the comment can be changed.", 400)
    new_etag = new_uuid()
    pk["Config"] = cfg
    pk["ETag"] = new_etag
    _public_keys[pk_id] = pk
    logger.info("UpdatePublicKey id=%s", pk_id)
    return _xml_response("PublicKey", lambda r: _build_public_key_xml(r, pk), extra_headers={"ETag": new_etag})


def _delete_public_key(pk_id, headers):
    pk = _public_keys.get(pk_id)
    if not pk:
        return _error("NoSuchPublicKey", "The public key does not exist.", 404)
    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != pk["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )
    if _public_keys_using(pk_id):
        return _error("PublicKeyInUse", "The public key is attached to one or more key groups.", 409)
    del _public_keys[pk_id]
    logger.info("DeletePublicKey id=%s", pk_id)
    return 204, {}, b""


def _list_public_keys(query_params):
    max_items = _qval(query_params, "MaxItems", _DEFAULT_MAX_ITEMS) or _DEFAULT_MAX_ITEMS
    keys = list(_public_keys.values())

    def build(root):
        SubElement(root, "MaxItems").text = max_items
        SubElement(root, "Quantity").text = str(len(keys))
        if keys:
            items_el = SubElement(root, "Items")
            for pk in keys:
                summary = SubElement(items_el, "PublicKeySummary")
                SubElement(summary, "Id").text = pk["Id"]
                SubElement(summary, "Name").text = pk["Config"]["Name"]
                SubElement(summary, "CreatedTime").text = pk["CreatedTime"]
                SubElement(summary, "EncodedKey").text = pk["Config"]["EncodedKey"]
                if pk["Config"].get("Comment"):
                    SubElement(summary, "Comment").text = pk["Config"]["Comment"]

    return _xml_response("PublicKeyList", build)


# ---------------------------------------------------------------------------
# Key groups
# ---------------------------------------------------------------------------


def _parse_key_group_config(el):
    name = _text(el, "Name")
    if not name:
        return None, _error("InvalidArgument", "The key group name is required.", 400)
    items_el = _find(el, "Items")
    items = []
    if items_el is not None:
        for child in items_el:
            local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if local == "PublicKey":
                items.append(child.text or "")
    if not items:
        return None, _error("InvalidArgument", "A key group must contain at least one public key.", 400)
    for pk_id in items:
        if pk_id not in _public_keys:
            return None, _error("InvalidArgument", f"Public key {pk_id} does not exist.", 400)
    return {"Name": name, "Items": items, "Comment": _text(el, "Comment")}, None


def _build_key_group_config_xml(parent, cfg):
    SubElement(parent, "Name").text = cfg["Name"]
    items_el = SubElement(parent, "Items")
    for pk_id in cfg["Items"]:
        SubElement(items_el, "PublicKey").text = pk_id
    if cfg.get("Comment"):
        SubElement(parent, "Comment").text = cfg["Comment"]


def _build_key_group_xml(parent, kg):
    SubElement(parent, "Id").text = kg["Id"]
    SubElement(parent, "LastModifiedTime").text = kg["LastModifiedTime"]
    cfg_el = SubElement(parent, "KeyGroupConfig")
    _build_key_group_config_xml(cfg_el, kg["Config"])


def _trusted_key_group_ids(dist):
    try:
        root = fromstring(dist.get("config_xml") or "")
    except Exception:
        return set()
    return {
        el.text for tkg in root.iter() if tkg.tag.split("}")[-1] == "TrustedKeyGroups"
        for el in tkg.iter() if el.tag.split("}")[-1] == "KeyGroup" and el.text
    }


def _key_groups_using(kg_id):
    return [d.get("Id", "") for d in _distributions.values() if kg_id in _trusted_key_group_ids(d)]


def _create_key_group(body):
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    cfg, err = _parse_key_group_config(el)
    if err is not None:
        return err
    for existing in _key_groups.values():
        if existing["Config"]["Name"] == cfg["Name"]:
            return _error("KeyGroupAlreadyExists", "A key group with the same name already exists.", 409)
    kg_id = _kg_id()
    etag = new_uuid()
    kg = {"Id": kg_id, "ETag": etag, "LastModifiedTime": _now_iso(), "Config": cfg}
    _key_groups[kg_id] = kg
    logger.info("CreateKeyGroup id=%s name=%s", kg_id, cfg["Name"])
    return _xml_response(
        "KeyGroup", lambda r: _build_key_group_xml(r, kg), status=201,
        extra_headers={"ETag": etag, "Location": f"/2020-05-31/key-group/{kg_id}"},
    )


def _get_key_group(kg_id):
    kg = _key_groups.get(kg_id)
    if not kg:
        return _error("NoSuchResource", "The key group does not exist.", 404)
    return _xml_response("KeyGroup", lambda r: _build_key_group_xml(r, kg), extra_headers={"ETag": kg["ETag"]})


def _get_key_group_config(kg_id):
    kg = _key_groups.get(kg_id)
    if not kg:
        return _error("NoSuchResource", "The key group does not exist.", 404)
    return _xml_response(
        "KeyGroupConfig", lambda r: _build_key_group_config_xml(r, kg["Config"]),
        extra_headers={"ETag": kg["ETag"]},
    )


def _update_key_group(kg_id, headers, body):
    kg = _key_groups.get(kg_id)
    if not kg:
        return _error("NoSuchResource", "The key group does not exist.", 404)
    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != kg["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )
    el = _parse_body(body)
    if el is None:
        return _error("MalformedXML", "The XML document is malformed.", 400)
    cfg, err = _parse_key_group_config(el)
    if err is not None:
        return err
    for existing in _key_groups.values():
        if existing["Id"] != kg_id and existing["Config"]["Name"] == cfg["Name"]:
            return _error("KeyGroupAlreadyExists", "A key group with the same name already exists.", 409)
    new_etag = new_uuid()
    kg["Config"] = cfg
    kg["ETag"] = new_etag
    kg["LastModifiedTime"] = _now_iso()
    logger.info("UpdateKeyGroup id=%s", kg_id)
    return _xml_response("KeyGroup", lambda r: _build_key_group_xml(r, kg), extra_headers={"ETag": new_etag})


def _delete_key_group(kg_id, headers):
    kg = _key_groups.get(kg_id)
    if not kg:
        return _error("NoSuchResource", "The key group does not exist.", 404)
    if_match = headers.get("if-match")
    if not if_match:
        return _error("InvalidIfMatchVersion", "The If-Match version is missing or not valid for the resource.", 400)
    if if_match != kg["ETag"]:
        return _error(
            "PreconditionFailed",
            "The precondition given in one or more of the request-header fields evaluated to false.",
            412,
        )
    if _key_groups_using(kg_id):
        return _error("ResourceInUse", "The key group is attached to one or more distributions.", 409)
    del _key_groups[kg_id]
    logger.info("DeleteKeyGroup id=%s", kg_id)
    return 204, {}, b""


def _list_key_groups(query_params):
    max_items = _qval(query_params, "MaxItems", _DEFAULT_MAX_ITEMS) or _DEFAULT_MAX_ITEMS
    groups = list(_key_groups.values())

    def build(root):
        SubElement(root, "MaxItems").text = max_items
        SubElement(root, "Quantity").text = str(len(groups))
        if groups:
            items_el = SubElement(root, "Items")
            for kg in groups:
                summary = SubElement(items_el, "KeyGroupSummary")
                kg_el = SubElement(summary, "KeyGroup")
                _build_key_group_xml(kg_el, kg)

    return _xml_response("KeyGroupList", build)


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
    """CachePolicyList: managed policies plus stored custom ones.

    ``Type`` optionally filters to ``managed`` or ``custom``; omitted, AWS
    returns both (ListCachePolicies API reference).
    """
    max_items = _qval(query_params, "MaxItems", _DEFAULT_MAX_ITEMS) or _DEFAULT_MAX_ITEMS
    type_filter = _qval(query_params, "Type", "")
    entries = []
    if type_filter in ("", "managed"):
        entries.extend(("managed", p) for p in _MANAGED_CACHE_POLICIES.values())
    if type_filter in ("", "custom"):
        entries.extend(("custom", p) for p in _cache_policies.values())

    def build(root):
        SubElement(root, "MaxItems").text = max_items
        SubElement(root, "Quantity").text = str(len(entries))
        if entries:
            items_el = SubElement(root, "Items")
            for kind, policy in entries:
                summary = SubElement(items_el, "CachePolicySummary")
                SubElement(summary, "Type").text = kind
                cp = SubElement(summary, "CachePolicy")
                _build_cache_policy_xml(cp, policy)

    return _xml_response("CachePolicyList", build)


def _list_policies(store, spec, query_params):
    """Generic ``*PolicyList``: managed policies plus stored custom ones.

    Shared by origin request policies and response headers policies; the
    summary member and resource tag come from ``spec``. ``Type`` optionally
    filters to ``managed`` or ``custom``; omitted, AWS returns both.
    """
    max_items = _qval(query_params, "MaxItems", _DEFAULT_MAX_ITEMS) or _DEFAULT_MAX_ITEMS
    type_filter = _qval(query_params, "Type", "")
    entries = []
    if type_filter in ("", "managed"):
        entries.extend(("managed", p) for p in spec["managed"].values())
    if type_filter in ("", "custom"):
        entries.extend(("custom", p) for p in store.values())

    def build(root):
        SubElement(root, "MaxItems").text = max_items
        SubElement(root, "Quantity").text = str(len(entries))
        if entries:
            items_el = SubElement(root, "Items")
            for kind, policy in entries:
                summary = SubElement(items_el, spec["summary_tag"])
                SubElement(summary, "Type").text = kind
                res = SubElement(summary, spec["resource_tag"])
                spec["build_resource"](res, policy)

    return _xml_response(spec["list_tag"], build)


def _build_monitoring_subscription_xml(parent, sub):
    cfg = SubElement(parent, "RealtimeMetricsSubscriptionConfig")
    SubElement(cfg, "RealtimeMetricsSubscriptionStatus").text = sub["RealtimeMetricsSubscriptionStatus"]


_MONITORING_SUB_STATUSES = {"Enabled", "Disabled"}


def _create_monitoring_subscription(dist_id, body):
    """CreateMonitoringSubscription (POST, 200): NoSuchDistribution when the
    distribution is unknown. terraform-provider-aws's
    aws_cloudfront_monitoring_subscription resource wires its Update to this
    same Create operation (UpdateWithoutTimeout: resourceMonitoringSubscriptionCreate)
    with no AlreadyExists handling, so Create on a distribution that already
    has a subscription must overwrite its status rather than error."""
    dist = _distributions.get(dist_id)
    if not dist:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)
    el = _parse_body(body)
    cfg_el = _find(el, "RealtimeMetricsSubscriptionConfig") if el is not None else None
    status = _text(cfg_el, "RealtimeMetricsSubscriptionStatus") if cfg_el is not None else ""
    if status not in _MONITORING_SUB_STATUSES:
        return _error(
            "InvalidArgument", "RealtimeMetricsSubscriptionStatus must be Enabled or Disabled.", 400
        )
    sub = {"RealtimeMetricsSubscriptionStatus": status}
    dist["MonitoringSubscription"] = sub
    logger.info("CreateMonitoringSubscription dist=%s status=%s", dist_id, status)
    return _xml_response("MonitoringSubscription", lambda r: _build_monitoring_subscription_xml(r, sub))


def _get_monitoring_subscription(dist_id):
    """NoSuchDistribution (404) for an unknown distribution,
    NoSuchMonitoringSubscription (404) when none is configured."""
    dist = _distributions.get(dist_id)
    if not dist:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)
    sub = dist.get("MonitoringSubscription")
    if not sub:
        return _error(
            "NoSuchMonitoringSubscription",
            "A monitoring subscription does not exist for the specified distribution.",
            404,
        )
    return _xml_response("MonitoringSubscription", lambda r: _build_monitoring_subscription_xml(r, sub))


def _delete_monitoring_subscription(dist_id):
    """DeleteMonitoringSubscription: 200 with an empty body per the API model
    (unlike the 204-with-empty-body shape most other CloudFront deletes use)."""
    dist = _distributions.get(dist_id)
    if not dist:
        return _error("NoSuchDistribution", "The specified distribution does not exist.", 404)
    if not dist.get("MonitoringSubscription"):
        return _error(
            "NoSuchMonitoringSubscription",
            "A monitoring subscription does not exist for the specified distribution.",
            404,
        )
    dist["MonitoringSubscription"] = None
    logger.info("DeleteMonitoringSubscription dist=%s", dist_id)
    return 200, {}, b""


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

    # UpdatePublicKey is PUT .../config, unlike the other families.
    m = _PUBLIC_KEY_CFG_RE.match(path)
    if m:
        pk_id = m.group(1)
        if method == "GET":
            return _get_public_key_config(pk_id)
        if method == "PUT":
            return _update_public_key(pk_id, headers, body)

    m = _PUBLIC_KEY_RE.match(path)
    if m:
        if method == "POST":
            return _create_public_key(body)
        if method == "GET":
            return _list_public_keys(query_params)

    m = _PUBLIC_KEY_ID_RE.match(path)
    if m:
        pk_id = m.group(1)
        if method == "GET":
            return _get_public_key(pk_id)
        if method == "DELETE":
            return _delete_public_key(pk_id, headers)

    # Key group routes
    m = _KEY_GROUP_CFG_RE.match(path)
    if m:
        if method == "GET":
            return _get_key_group_config(m.group(1))

    m = _KEY_GROUP_RE.match(path)
    if m:
        if method == "POST":
            return _create_key_group(body)
        if method == "GET":
            return _list_key_groups(query_params)

    m = _KEY_GROUP_ID_RE.match(path)
    if m:
        kg_id = m.group(1)
        if method == "GET":
            return _get_key_group(kg_id)
        if method == "PUT":
            return _update_key_group(kg_id, headers, body)
        if method == "DELETE":
            return _delete_key_group(kg_id, headers)

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
    if m:
        dist_id = m.group(1)
        if method == "GET":
            return _get_monitoring_subscription(dist_id)
        if method == "POST":
            return _create_monitoring_subscription(dist_id, body)
        if method == "DELETE":
            return _delete_monitoring_subscription(dist_id)

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
        "DomainName": _new_distribution_domain(),
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

    config_el = _strip_namespace(fromstring(dist["config_xml"]))
    _ensure_distribution_config_sdk_compat(config_el)
    config_el.tag = "DistributionConfig"
    config_el.set("xmlns", NS)
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(config_el, encoding="unicode").encode("utf-8")
    return 200, {"Content-Type": "text/xml", "ETag": dist["ETag"]}, body


def _dist_config_el(dist):
    """Parsed DistributionConfig element for a stored distribution.

    A CloudFormation-provisioned record persisted before the provisioner
    rendered its configuration carries an empty ``config_xml``; parse
    defensively so account-wide scans never fail on one."""
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
        "RoutingEndpoint": _cloudfront_domain(),
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
        "RoutingEndpoint": _cloudfront_domain(),
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


# ---------------------------------------------------------------------------
# Data-plane accessors over the distribution and policy stores.
# ---------------------------------------------------------------------------


def find_distribution_for_label(label: str):
    """The distribution whose DomainName is ``<label>.cloudfront.net``.

    A viewer request carries no credentials, so this scans the ambient
    account's distributions the same way ``alb.find_lb_for_host`` scans
    ``_lbs.values()`` for ALB's own host-routed data plane — not a
    cross-account search, just the same ambient-scoped lookup every other
    host-routed service uses here.
    """
    domain = f"{label}.cloudfront.net"
    for dist in _distributions.values():
        if dist.get("DomainName", "").lower() == domain:
            return dist
    return None


# The regional/legacy REST-endpoint domain shapes an Origin's DomainName
# takes when the origin is an S3 bucket (CloudFront Developer Guide, "Amazon
# S3 origin"). A website-endpoint domain (s3-website-*) isn't matched here:
# AWS itself requires that shape to be configured as a CustomOriginConfig,
# never S3OriginConfig, so it is correctly left to the custom-origin path.
_S3_ORIGIN_DOMAIN_RE = re.compile(
    r"^(?P<bucket>[a-z0-9][a-z0-9.-]*[a-z0-9])\.s3(?:\.[a-z0-9-]+|-[a-z0-9-]+)?\.amazonaws\.com$"
)


def _s3_origin_bucket(domain_name: str):
    m = _S3_ORIGIN_DOMAIN_RE.match((domain_name or "").strip().lower())
    return m.group("bucket") if m else None


def _dataplane_custom_headers(origin_el):
    """Origin.CustomHeaders — wire name "CustomHeaders" (CloudFormation's
    OriginCustomHeaders is a JSON-only rename, see _CFN_DISTRIBUTION_CONFIG_RENAMES
    above); Items/OriginCustomHeader/{HeaderName,HeaderValue} per the botocore
    cloudfront service-2.json (2020-05-31) shape."""
    headers = []
    ch_el = _find(origin_el, "CustomHeaders")
    items_el = _find(ch_el, "Items") if ch_el is not None else None
    if items_el is not None:
        for it in items_el:
            if _local_tag_name(it) == "OriginCustomHeader":
                name = _text(it, "HeaderName")
                if name:
                    headers.append((name, _text(it, "HeaderValue")))
    return headers


def _dataplane_origin(origin_el):
    custom = _find(origin_el, "CustomOriginConfig")
    s3cfg = _find(origin_el, "S3OriginConfig")
    domain_name = _text(origin_el, "DomainName")
    return {
        "id": _text(origin_el, "Id"),
        "domain_name": domain_name,
        "origin_path": _opt_text(origin_el, "OriginPath") or "",
        "custom_headers": _dataplane_custom_headers(origin_el),
        # Origin carries exactly one of S3OriginConfig or CustomOriginConfig
        # (AWS's schema is mutually exclusive); the bucket name itself is
        # only ever recoverable from DomainName, never a separate field.
        "s3_bucket": _s3_origin_bucket(domain_name) if s3cfg is not None else None,
        "http_port": int(_text(custom, "HTTPPort") or "80") if custom is not None else 80,
        "https_port": int(_text(custom, "HTTPSPort") or "443") if custom is not None else 443,
        "protocol_policy": _text(custom, "OriginProtocolPolicy") if custom is not None else "match-viewer",
        # Response timeout doc ("Origin response timeout"): default 30s.
        "read_timeout": int(_text(custom, "OriginReadTimeout") or "30") if custom is not None else 30,
    }


def _dataplane_function_associations(behavior_el):
    associations = {}
    fa_el = _find(behavior_el, "FunctionAssociations")
    items_el = _find(fa_el, "Items") if fa_el is not None else None
    if items_el is None:
        return associations
    for it in items_el:
        if _local_tag_name(it) == "FunctionAssociation":
            associations[_text(it, "EventType")] = _text(it, "FunctionARN")
    return associations


def _dataplane_forwarded_values(behavior_el):
    """Legacy ``ForwardedValues`` (botocore cloudfront service-2.json), used
    by a behavior that has no CachePolicyId. Returns None when the behavior
    carries no ForwardedValues block either (nothing legacy to forward)."""
    fv_el = _find(behavior_el, "ForwardedValues")
    if fv_el is None:
        return None
    cookies_el = _find(fv_el, "Cookies")
    return {
        "query_string": _xbool(fv_el, "QueryString", False),
        "headers": _parse_name_items(fv_el, "Headers"),
        "cookies_forward": _text(cookies_el, "Forward", "none") if cookies_el is not None else "none",
        "cookies_whitelist": _parse_name_items(cookies_el, "WhitelistedNames") if cookies_el is not None else [],
    }


def _dataplane_allowed_methods(behavior_el):
    """The behavior's AllowedMethods.Items, or None when the block is absent."""
    am_el = _find(behavior_el, "AllowedMethods")
    if am_el is None:
        return None
    items_el = _find(am_el, "Items")
    methods = []
    if items_el is not None:
        for it in items_el:
            if _local_tag_name(it) == "Method":
                methods.append(it.text or "")
    return methods


def _dataplane_behavior(behavior_el, path_pattern=None):
    return {
        "path_pattern": path_pattern,
        "target_origin_id": _text(behavior_el, "TargetOriginId"),
        "viewer_protocol_policy": _text(behavior_el, "ViewerProtocolPolicy", "allow-all"),
        "allowed_methods": _dataplane_allowed_methods(behavior_el),
        "cache_policy_id": _opt_text(behavior_el, "CachePolicyId"),
        "origin_request_policy_id": _opt_text(behavior_el, "OriginRequestPolicyId"),
        "response_headers_policy_id": _opt_text(behavior_el, "ResponseHeadersPolicyId"),
        "forwarded_values": _dataplane_forwarded_values(behavior_el),
        "functions": _dataplane_function_associations(behavior_el),
    }


def parse_distribution_dataplane_config(dist: dict) -> dict:
    """A distribution's stored config XML, reduced to what the data plane
    dispatches against: the default root object, origins by id, the default
    behavior, and ordered behaviors in declaration order (first
    ``path_pattern`` match wins)."""
    config_el = _dist_config_el(dist)

    origins = {}
    origins_el = _find(config_el, "Origins")
    items_el = _find(origins_el, "Items") if origins_el is not None else None
    if items_el is not None:
        for it in items_el:
            if _local_tag_name(it) == "Origin":
                origin = _dataplane_origin(it)
                origins[origin["id"]] = origin

    default_el = _find(config_el, "DefaultCacheBehavior")
    default_behavior = _dataplane_behavior(default_el) if default_el is not None else None

    ordered_behaviors = []
    behaviors_el = _find(config_el, "CacheBehaviors")
    items_el = _find(behaviors_el, "Items") if behaviors_el is not None else None
    if items_el is not None:
        for it in items_el:
            if _local_tag_name(it) == "CacheBehavior":
                ordered_behaviors.append(_dataplane_behavior(it, _text(it, "PathPattern")))

    return {
        "default_root_object": _opt_text(config_el, "DefaultRootObject") or "",
        "origins": origins,
        "default_behavior": default_behavior,
        "ordered_behaviors": ordered_behaviors,
    }


def cache_policy_params(policy_id):
    """A cache policy's Parameters dict (managed or custom), or None when the
    policy has no ParametersInCacheKeyAndForwardedToOrigin block (nothing
    beyond the defaults CloudFront always includes) or doesn't exist."""
    policy = _cache_policies.get(policy_id) or _MANAGED_CACHE_POLICIES.get(policy_id)
    return policy["Config"].get("Parameters") if policy else None


def origin_request_policy_config(policy_id):
    """A custom or AWS-managed origin request policy's Config dict, or None."""
    policy = _origin_request_policies.get(policy_id) or _MANAGED_ORIGIN_REQUEST_POLICIES.get(policy_id)
    return policy["Config"] if policy else None


def response_headers_policy_config(policy_id):
    """A custom or AWS-managed response-headers policy's Config dict, or None."""
    policy = _response_headers_policies.get(policy_id) or _MANAGED_RESPONSE_HEADERS_POLICIES.get(policy_id)
    return policy["Config"] if policy else None


def live_function_code(function_arn: str):
    """The published (LIVE) JS body for a FunctionAssociation's FunctionARN,
    or None when the ARN doesn't resolve to a published function."""
    name = function_arn.rsplit("/", 1)[-1] if function_arn else ""
    fn = _functions.get(name)
    if not fn or not fn.get("live_etag"):
        return None
    return _function_view(fn, "LIVE").get("code", fn["code"])


# ---- CloudFront Functions (Node worker pool) ----

# CloudFront Functions run in ~1ms; this timeout only stops a runaway function (Lambda@Edge 5s figure).
_EVAL_TIMEOUT = 5.0
_MAX_OLD_SPACE_MB = 256
_RECYCLE_AFTER = 1000
_MAX_IDLE = 2

# stdout carries the protocol, so anything the function logs goes to stderr.
_WORKER_SCRIPT = r"""
const readline = require("readline");
const vm = require("vm");
const crypto = require("crypto");
const querystring = require("querystring");
const bufferModule = require("buffer");

const _stderrWrite = process.stderr.write.bind(process.stderr);
const _stdoutWrite = process.stdout.write.bind(process.stdout);
process.stdout.write = (chunk, enc, cb) => _stderrWrite(chunk, enc, cb);

// CloudFront Functions documents a fixed require()/import surface (CloudFront
// Developer Guide, "JavaScript runtime 2.0 features for CloudFront
// Functions"): crypto, querystring, and the Buffer module. Node exposes a
// much larger module graph; refusing everything else here makes a function
// that imports e.g. "fs" or "http" fail the way it would on the real edge
// runtime, instead of silently succeeding against Node's surface.
const _ALLOWED_MODULES = { crypto, querystring, buffer: bufferModule };
function sandboxRequire(name) {
  if (Object.prototype.hasOwnProperty.call(_ALLOWED_MODULES, name)) return _ALLOWED_MODULES[name];
  throw new Error("require('" + name + "') is not supported by CloudFront Functions");
}

const compiled = new Map();

function compile(code) {
  const key = crypto.createHash("sha256").update(code).digest("hex");
  const hit = compiled.get(key);
  if (hit) return hit;

  // Runtime 1.0 code calls require() directly; runtime 2.0 code uses ES
  // module `import` statements for the same three modules (CloudFront
  // Developer Guide, "JavaScript runtime 2.0 features for CloudFront
  // Functions"). Rather than a full ESM loader, a default or namespace
  // import is rewritten to the equivalent `require()` call — sandboxRequire
  // still enforces the allowed module set either way, and a named import
  // (e.g. `import { createHash } from 'crypto'`) is rewritten the same way.
  const stripped = code.replace(
    /^\s*import\s+(?:(\*\s*as\s*(\w+))|\{([^}]*)\}|(\w+))\s+from\s+['"]([^'"]+)['"]\s*;?\s*$/gm,
    (_m, _star, starName, named, deflt, source) => {
      if (named) {
        return named.split(",").map((part) => {
          const [orig, alias] = part.split(/\s+as\s+/).map((x) => x.trim());
          return orig ? `const ${alias || orig} = require(${JSON.stringify(source)}).${orig};` : "";
        }).join("\n");
      }
      const name = starName || deflt;
      return name ? `const ${name} = require(${JSON.stringify(source)});` : "";
    },
  );

  const src = `${stripped}\n;globalThis.__handler = typeof handler === "function" ? handler : null;`;
  const sandbox = {
    require: sandboxRequire, console, JSON, Math, Date, Object, Array, String,
    Number, Boolean, RegExp, Map, Set, Promise, Error, TypeError, RangeError,
    TextEncoder, TextDecoder, atob, btoa, Buffer: bufferModule.Buffer,
  };
  vm.createContext(sandbox);
  new vm.Script(src, { filename: "function.js" }).runInContext(sandbox);
  const entry = { handler: sandbox.__handler };
  compiled.set(key, entry);
  return entry;
}

async function run(req) {
  const { code, event } = req;
  const entry = compile(code);
  if (typeof entry.handler !== "function") {
    return { status: "error", message: "function code does not define a top-level handler(event) function" };
  }
  try {
    let value = entry.handler(event);
    // cloudfront-js-2.0 allows async handlers (CloudFront Developer Guide,
    // "JavaScript runtime 2.0 features"); 1.0 handlers return a plain value.
    if (value && typeof value.then === "function") {
      value = await value;
    }
    return { status: "ok", value: value === undefined ? null : value };
  } catch (err) {
    return { status: "error", message: String((err && err.stack) || err) };
  }
}

const rl = readline.createInterface({ input: process.stdin });
rl.on("line", async (line) => {
  if (!line.trim()) return;
  let out;
  try {
    out = await run(JSON.parse(line));
  } catch (e) {
    out = { status: "error", message: String((e && e.stack) || e) };
  }
  _stdoutWrite(JSON.stringify(out) + "\n");
});
"""


_pool = node_pool.NodeWorkerPool(
    _WORKER_SCRIPT, log_prefix="cloudfront-js", logger=logger, timeout=_EVAL_TIMEOUT,
    max_old_space_mb=_MAX_OLD_SPACE_MB, recycle_after=_RECYCLE_AFTER, max_idle=_MAX_IDLE,
)


def _cf_function_evaluate(code: bytes, event: dict):
    """Run a published function's code against `event`; return its returned
    request/response object (a dict), or raise CloudFrontFunctionError."""
    try:
        out = _pool.call({"code": code.decode("utf-8"), "event": event}, timeout=_EVAL_TIMEOUT)
    except node_pool.NodeWorkerTimeout as exc:
        raise CloudFrontFunctionError(
            f"function evaluation exceeded {int(_EVAL_TIMEOUT)} seconds and was cancelled"
        ) from exc
    except node_pool.NodeWorkerError as exc:
        if exc.reason == "missing_node":
            raise CloudFrontFunctionError("CloudFront Functions need Node, which was not found") from exc
        if exc.reason == "closed":
            raise CloudFrontFunctionError("CloudFront Functions worker closed unexpectedly") from exc
        raise CloudFrontFunctionError(f"CloudFront Functions worker failed: {exc.cause_text}") from exc

    if out["status"] == "error":
        raise CloudFrontFunctionError(out.get("message", "function execution error"))
    return out.get("value")


def _cf_functions_reset():
    """Drop every worker, so a reset leaves no compiled-function cache behind."""
    _pool.reset()


def _cf_functions_available():
    """Whether CloudFront Functions can be evaluated at all in this environment."""
    return node_pool.available()


# ---- Data plane: serves a distribution's viewer traffic ----

_HOP_BY_HOP_REQUEST_HEADERS = {"host", "cookie", "content-length", "connection", "transfer-encoding"}
# A narrower set used only at the actual socket boundary (_forward_to_origin).
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
    """"example-header-name" -> "Example-Header-Name", as CloudFront Functions title-cases names."""
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
    code = live_function_code(function_arn)
    if code is None:
        raise CloudFrontFunctionError(f"{function_arn} has no published (LIVE) code")
    # The function cannot call back into MiniStack, so this is run_offloop work.
    return await run_offloop(_cf_function_evaluate, code, event)


# ---------------------------------------------------------------------------
# Cache behavior matching — CloudFront path-pattern glob, first match wins.
# ---------------------------------------------------------------------------


def _glob_to_regex(pattern: str):
    # "You can optionally include a slash (/) at the beginning of the path pattern...
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
    cache_params = cache_policy_params(behavior.get("cache_policy_id")) or {}
    orp_cfg = (
        origin_request_policy_config(behavior["origin_request_policy_id"])
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
    """Whether ``viewer_origin`` matches one of the AccessControlAllowOrigins entries."""
    return any(p == "*" or fnmatch.fnmatchcase(viewer_origin, p) for p in allow_origins or [])


def _apply_cors_headers(headers: dict, cors_cfg: dict, viewer_origin: str, method: str) -> dict:
    """Apply a response headers policy's CORS config."""
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


# CloudFront Developer Guide, http-502-bad-gateway.html.
_ERROR_X_CACHE = "Error from cloudfront"


# ---------------------------------------------------------------------------
# Origin forwarding
# ---------------------------------------------------------------------------


class OriginUnreachable(Exception):
    pass


class OriginTimeout(Exception):
    """The origin didn't respond within OriginReadTimeout."""


# An origin addressed by an AWS-owned hostname is refused with 502 so no traffic reaches real AWS.
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
    # MiniStack's own endpoints carry the gateway port, so an explicit :port in DomainName is honoured.
    host, sep, port = origin["domain_name"].rpartition(":")
    if sep and port.isdigit():
        return host, int(port), use_https
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
    """Set ``name`` case-insensitively, replacing any existing spelling."""
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
        # Covers an unreachable origin and an untrusted or self-signed origin certificate.
        raise OriginUnreachable(str(e)) from e
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# S3 origin — served from MiniStack's own S3 store in-process, never a socket.
# ---------------------------------------------------------------------------


def _s3_object_key(origin: dict, uri: str) -> str:
    """CloudFront concatenates OriginPath directly in front of the request URI to build what reaches the origin."""
    return unquote((origin.get("origin_path") or "") + uri).lstrip("/")


def _serve_s3_origin(origin: dict, method: str, uri: str, headers: dict) -> tuple:
    """Fetch from an S3 origin in-process (blocking)."""
    return s3.serve_cloudfront_origin_fetch(origin["s3_bucket"], _s3_object_key(origin, uri), method, headers)


# ---------------------------------------------------------------------------
# Multi-value headers: a repeated name maps to a list of values.


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
    """CloudFront Developer Guide, http-403-permission-denied.html, "Stacked distributions cause a 403 error"."""
    if "(cloudfront)" in headers.get("via", "").lower():
        return _cf_error_response(
            403, "This distribution is configured to serve another CloudFront distribution as its origin.",
            _ERROR_X_CACHE,
        )
    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def handle_viewer_request(dist: dict, method: str, path: str, raw_uri: str, raw_query_string: str,
                          headers: dict, body: bytes, query_params: dict, client_ip: str = "127.0.0.1") -> tuple:
    """Serve one viewer request against ``dist`` (a cloudfront.py distribution record)."""
    stacked_response = _check_stacked_distribution(headers)
    if stacked_response is not None:
        return stacked_response

    dist_id = dist["Id"]
    dist_domain = dist["DomainName"]
    request_id = new_uuid()
    viewer_is_https = headers.get("x-forwarded-proto", "http") == "https"

    parsed = parse_distribution_dataplane_config(dist)
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
            # Function-generated response.
            resp_headers = _merge_function_headers(None, result.get("headers"))
            resp_headers = _add_cloudfront_headers(resp_headers, "FunctionGeneratedResponse from cloudfront")
            set_cookie_values = _set_cookie_values_from_event(result.get("cookies"))
            if set_cookie_values:
                resp_headers["set-cookie"] = (
                    set_cookie_values if len(set_cookie_values) > 1 else set_cookie_values[0]
                )
            # statusDescription has no outlet.
            resp_body = _body_from_response_event(result, b"")
            return result["statusCode"], _render_headers(resp_headers, title_case=True), resp_body

        # Request-side headers stay lowercase-keyed internally.
        req = result if isinstance(result, dict) else event["request"]
        request_uri = req.get("uri", request_uri)
        request_headers = _merge_function_headers(event["request"]["headers"], req.get("headers"))
        cookie_pairs = [f"{n}={(f.get('value') if isinstance(f, dict) else f)}"
                        for n, f in (req.get("cookies") or {}).items()]
        if cookie_pairs:
            request_headers["cookie"] = "; ".join(cookie_pairs)
        qs_field = req.get("querystring")
        if isinstance(qs_field, str):
            # A function rewrote the querystring as a literal string (functions-event-structure, "Query string").
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
        # CloudFront Developer Guide, "User-Agent header".
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

    if origin.get("s3_bucket"):
        status, origin_headers_list, origin_body = await run_offloop(
            _serve_s3_origin, origin, method, request_uri, fwd_headers,
        )
        reason = http.client.responses.get(status, "")
        origin_headers = _multidict_from_pairs(origin_headers_list.items() if isinstance(origin_headers_list, dict)
                                                else origin_headers_list)
    else:
        full_uri = (origin.get("origin_path") or "") + request_uri
        try:
            # The origin may be MiniStack's own gateway, so the call must be reentrant.
            status, reason, origin_headers_list, origin_body = await run_reentrant(
                _forward_to_origin, origin, method, full_uri, forward_qs, fwd_headers, body, viewer_is_https,
                client_ip, request_headers.get("x-forwarded-for", ""),
            )
        except OriginTimeout:
            logger.warning("Origin timed out for distribution=%s origin=%s", dist_id, origin["domain_name"])
            return _cf_error_response(504, "The request could not be satisfied.", _ERROR_X_CACHE)
        except OriginUnreachable:
            logger.warning("Origin unreachable for distribution=%s origin=%s", dist_id, origin["domain_name"])
            return _cf_error_response(502, "The request could not be satisfied.", _ERROR_X_CACHE)
        origin_headers = _multidict_from_pairs(origin_headers_list)

    origin_headers = _strip_hop_by_hop_response_headers(origin_headers)
    policy_cfg = response_headers_policy_config(behavior.get("response_headers_policy_id"))
    response_headers = _apply_response_headers_policy(
        origin_headers, policy_cfg, request_headers.get("origin", ""), method,
    )
    response_body = origin_body

    response_function_arn = behavior["functions"].get("viewer-response")
    # CloudFront does not run a viewer-response function when the origin answers 400 or above.
    if response_function_arn and status < 400:
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
        # Replace, not merge.
        response_headers = _merge_function_headers(event["response"]["headers"], response.get("headers"))
        set_cookie_values = _set_cookie_values_from_event(response.get("cookies"))
        if set_cookie_values:
            response_headers["set-cookie"] = set_cookie_values if len(set_cookie_values) > 1 else set_cookie_values[0]
        response_body = _body_from_response_event(response, origin_body)

    final_headers = _add_cloudfront_headers(response_headers, "Miss from cloudfront")
    return status, _render_headers(final_headers, title_case=True), response_body
