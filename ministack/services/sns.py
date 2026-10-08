# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
SNS Service Emulator — AWS-compatible.
Supports: CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes,
          Subscribe, Unsubscribe, ConfirmSubscription,
          ListSubscriptions, ListSubscriptionsByTopic,
          GetSubscriptionAttributes, SetSubscriptionAttributes,
          Publish, PublishBatch,
          ListTagsForResource, TagResource, UntagResource,
          CreatePlatformApplication, ListPlatformApplications,
          GetPlatformApplicationAttributes, SetPlatformApplicationAttributes,
          DeletePlatformApplication,
          CreatePlatformEndpoint, GetEndpointAttributes, SetEndpointAttributes,
          DeleteEndpoint.
SNS → Lambda fanout dispatches via _execute_function (synchronous).
FIFO topics: .fifo naming validation, MessageGroupId/MessageDeduplicationId enforcement,
             5-minute deduplication window, sequence numbers, content-based deduplication,
             PublishBatch FIFO support.
"""

import asyncio
import contextvars
import copy
import hashlib
import json
import logging
import os
import threading as _threading
import time
from urllib.parse import parse_qs

_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_PORT = os.environ.get("GATEWAY_PORT", "4566")

import ministack.services.lambda_svc as _lambda_svc
from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import esm_wake
from ministack.core.responses import AccountRegionScopedDict, get_account_id, get_region, new_uuid, request_scope
from ministack.services import sqs as _sqs

logger = logging.getLogger("sns")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

import re as _re


def _normalize_arn(arn: str) -> str:
    """Normalize an SNS ARN that has an empty account ID.
    Some SDKs (Go v2 with skipRequestingAccountId) construct ARNs with empty
    account like arn:aws:sns:us-east-1::topic-name. Replace the empty account
    with the current request's account ID so the lookup succeeds.
    """
    if arn and _re.match(r"arn:aws:sns:[^:]+::[^:]+", arn):
        return _re.sub(r"(arn:aws:sns:[^:]+)::", rf"\1:{get_account_id()}:", arn)
    return arn


def _topic_by_arn_any_scope(topic_arn: str) -> dict | None:
    """Fetch a topic by ARN, reaching into its owner's scope when foreign.

    Only the account may differ: like real SNS, a topic in another region is
    invisible no matter who asks.
    """
    topic = _topics.get(topic_arn)
    if topic is not None:
        return topic
    try:
        spec = parse_arn(topic_arn)
    except ArnParseError:
        return None
    if not spec.account_id or not spec.region or spec.service != "sns":
        return None
    if spec.region != get_region() or spec.account_id == get_account_id():
        return None
    return _topics.get_scoped(spec.account_id, spec.region, topic_arn)


def _topic_owner(topic: dict) -> str:
    try:
        return parse_arn(topic.get("arn", "")).account_id or get_account_id()
    except ArnParseError:
        return get_account_id()


def _enforce_topic_policy(topic: dict, iam_action: str):
    """Gate one topic API call on the topic's resource policy.

    Returns an ``AuthorizationError`` (403) response tuple when denied, else
    ``None``. Only under AUTH=true is anything denied; with auth disabled
    every call passes, like the rest of the identity layer. Same-account
    callers then pass unless the Policy carries an explicit Deny for them;
    cross-account callers need an explicit Allow, like real SNS.

    Known boundary under AUTH=true: a caller with no identity Allow whose
    access comes from this Policy alone is denied by the app-level identity
    check before this resource check runs, where real SNS would allow the
    union (same caveat as the queue check).
    """
    from ministack.app import AUTH
    if not AUTH:
        return None
    from ministack.core.iam_evaluator import (
        EvalContext,
        caller_arn,
        resource_policy_allows,
    )

    topic_arn = topic.get("arn", "")
    ctx = EvalContext(
        principal_arn=caller_arn(),
        principal_type="Root",
        principal_account=get_account_id(),
        action=iam_action,
        resource_arn=topic_arn or "*",
        region=get_region(),
    )
    if resource_policy_allows(topic.get("attributes", {}).get("Policy") or "", ctx,
                              _topic_owner(topic) == get_account_id()):
        return None
    return _error("AuthorizationError",
                  f"User: {ctx.principal_arn} is not authorized to perform: "
                  f"{iam_action} on resource: {topic_arn}", 403)


def _get_topic(topic_arn: str, action: str | None = None):
    """Resolve a topic by ARN, enforcing its Policy when *action* is given.

    Returns ``(topic, None)`` on success or ``(None, error_tuple)`` with a
    ``NotFound`` (404) or ``AuthorizationError`` (403) response.
    """
    arn = _normalize_arn(topic_arn)
    topic = _topic_by_arn_any_scope(arn)
    if topic is None:
        return None, _error("NotFound", f"Topic does not exist: {arn}", 404)
    if action is not None:
        denied = _enforce_topic_policy(topic, action)
        if denied is not None:
            return None, denied
    return topic, None


def topic_policy_allows(topic_arn: str, service: str,
                        source_arn: str, source_account: str) -> bool:
    """Whether the topic's Policy lets an AWS service publish to it.

    Used by S3 notifications, which publish as the ``s3.amazonaws.com``
    service principal with the bucket as ``aws:SourceArn``/``aws:SourceAccount``
    (plus ``aws:SourceOwner`` for older policy samples). Only under AUTH=true
    is the Policy consulted; with auth disabled every delivery passes. Every
    topic carries the default policy, whose ``AWS:SourceOwner`` condition then
    admits same-account service deliveries; a custom Policy without an S3
    statement — or no Policy at all — blocks them, like real SNS.
    """
    from ministack.app import AUTH
    if not AUTH:
        return True
    from ministack.core.iam_evaluator import EvalContext, evaluate_resource_policy

    topic = _topic_by_arn_any_scope(topic_arn)
    if topic is None:
        return False
    raw_policy = topic.get("attributes", {}).get("Policy") or ""
    if not raw_policy:
        return False
    # The caller is the service itself, not an IAM principal in the source
    # account: an account-ID grant must not authorize it (real AWS keeps the
    # service principal distinct), while Principal "*" legitimately matches.
    ctx = EvalContext(
        principal_arn="*",
        principal_type="Service",
        principal_account="",
        action="sns:Publish",
        resource_arn=topic_arn,
        region=get_region(),
        service_context={
            "aws:sourcearn": source_arn,
            "aws:sourceaccount": source_account,
            "aws:sourceowner": source_account,
        },
    )
    return evaluate_resource_policy(raw_policy, ctx, service=service).decision == "Allow"


def _sqs_queue_name_from_arn_spec(spec) -> str | None:
    if spec.service != "sqs" or not spec.resource or ":" in spec.resource or "/" in spec.resource:
        return None
    return spec.resource


def _lambda_function_name_from_arn_spec(spec) -> str | None:
    if spec.service != "lambda":
        return None
    parts = spec.resource.split(":", 2)
    if len(parts) < 2 or parts[0] != "function" or not parts[1]:
        return None
    return parts[1]


def _invalid_subscription_endpoint(protocol: str, endpoint: str):
    return _error(
        "InvalidParameterException",
        f"Invalid parameter: Endpoint {endpoint} is not a valid {protocol.upper()} ARN",
        400,
    )


def _validate_subscription_endpoint(protocol: str, endpoint: str):
    if protocol not in {"sqs", "lambda"}:
        return None
    try:
        spec = parse_arn(endpoint)
    except ArnParseError:
        return _invalid_subscription_endpoint(protocol, endpoint)

    if not spec.region or not spec.account_id:
        return _invalid_subscription_endpoint(protocol, endpoint)
    if protocol == "sqs" and not _sqs_queue_name_from_arn_spec(spec):
        return _invalid_subscription_endpoint(protocol, endpoint)
    if protocol == "lambda" and not _lambda_function_name_from_arn_spec(spec):
        return _invalid_subscription_endpoint(protocol, endpoint)
    return None


def _resolve_topic_tag_arn(arn: str, action: str):
    arn = _normalize_arn(arn)
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return arn, None, _error("InvalidParameterException", f"Invalid SNS topic ARN: {arn}", 400)

    if (
        spec.partition != "aws"
        or spec.service != "sns"
        or not spec.region
        or not spec.account_id
        or not spec.resource
        or ":" in spec.resource
        or "/" in spec.resource
    ):
        return arn, None, _error("InvalidParameterException", f"Invalid SNS topic ARN: {arn}", 400)

    if spec.region != get_region():
        return arn, None, _error("ResourceNotFoundException", "Resource not found", 404)

    topic = _topic_by_arn_any_scope(arn)
    if not topic:
        return arn, None, _error("ResourceNotFoundException", "Resource not found", 404)
    denied = _enforce_topic_policy(topic, action)
    if denied is not None:
        return arn, None, denied
    return arn, topic, None


_topics = AccountRegionScopedDict()
_sub_arn_to_topic = AccountRegionScopedDict()
_platform_applications = AccountRegionScopedDict()
_platform_endpoints = AccountRegionScopedDict()

# Direct-to-phone publishes by recipient, served at /_ministack/sns/sms-messages.
_sms_messages = AccountRegionScopedDict()


def _sms_log_for(phone_number: str) -> list:
    """The record list for one phone number in the current account and region."""
    records = _sms_messages.get(phone_number)
    if records is None:
        records = []
        _sms_messages[phone_number] = records
    return records


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "topics": copy.deepcopy(_topics),
        "sub_arn_to_topic": copy.deepcopy(_sub_arn_to_topic),
        "sms_messages": copy.deepcopy(_sms_messages),
        "platform_applications": copy.deepcopy(_platform_applications),
        "platform_endpoints": copy.deepcopy(_platform_endpoints),
    }


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if data:
        _topics.update(data.get("topics", {}))
        for topic in _topics.all_values():
            topic.pop("messages", None)  # publish history is no longer kept
        _sub_arn_to_topic.update(data.get("sub_arn_to_topic", {}))
        _sms_messages.update(data.get("sms_messages", {}))
        _platform_applications.update(data.get("platform_applications", {}))
        _platform_endpoints.update(data.get("platform_endpoints", {}))




async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    params = dict(query_params)
    if method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    from ministack.core.iam_evaluator import pin_request_caller
    pin_request_caller(headers, query_params)

    action = _p(params, "Action")
    handlers = {
        "CreateTopic": _create_topic,
        "DeleteTopic": _delete_topic,
        "ListTopics": _list_topics,
        "GetTopicAttributes": _get_topic_attributes,
        "SetTopicAttributes": _set_topic_attributes,
        "AddPermission": _add_permission,
        "RemovePermission": _remove_permission,
        "Subscribe": _subscribe,
        "ConfirmSubscription": _confirm_subscription,
        "Unsubscribe": _unsubscribe,
        "ListSubscriptions": _list_subscriptions,
        "ListSubscriptionsByTopic": _list_subscriptions_by_topic,
        "GetSubscriptionAttributes": _get_subscription_attributes,
        "SetSubscriptionAttributes": _set_subscription_attributes,
        "Publish": _publish,
        "PublishBatch": _publish_batch,
        "ListTagsForResource": _list_tags_for_resource,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "CreatePlatformApplication": _create_platform_application,
        "ListPlatformApplications": _list_platform_applications,
        "GetPlatformApplicationAttributes": _get_platform_application_attributes,
        "SetPlatformApplicationAttributes": _set_platform_application_attributes,
        "CreatePlatformEndpoint": _create_platform_endpoint,
        "DeletePlatformApplication": _delete_platform_application,
        "GetEndpointAttributes": _get_endpoint_attributes,
        "SetEndpointAttributes": _set_endpoint_attributes,
        "DeleteEndpoint": _delete_endpoint,
    }

    handler = handlers.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown action: {action}", 400)
    return handler(params)


# ---------------------------------------------------------------------------
# Topic management
# ---------------------------------------------------------------------------

def _create_topic(params):
    name = _p(params, "Name")
    if not name:
        return _error("InvalidParameterException", "Name is required", 400)

    # ── Collect explicit attributes from the request ──
    explicit_attrs = {}
    i = 1
    while _p(params, f"Attributes.entry.{i}.key"):
        key = _p(params, f"Attributes.entry.{i}.key")
        val = _p(params, f"Attributes.entry.{i}.value")
        explicit_attrs[key] = val
        i += 1

    fifo_attr = explicit_attrs.get("FifoTopic", "")
    is_fifo_name = name.endswith(".fifo")

    # FIFO naming validation: FifoTopic=true requires .fifo suffix
    if fifo_attr == "true" and not is_fifo_name:
        return _error(
            "InvalidParameterException",
            "Invalid parameter: Topic names with FIFO attribute must end with .fifo suffix",
            400,
        )

    # Auto-detect FIFO when name ends with .fifo but attribute not explicitly set
    if is_fifo_name and fifo_attr != "true":
        explicit_attrs["FifoTopic"] = "true"

    is_fifo = explicit_attrs.get("FifoTopic") == "true"

    # Default ContentBasedDeduplication to "false" for FIFO topics
    if is_fifo and "ContentBasedDeduplication" not in explicit_attrs:
        explicit_attrs["ContentBasedDeduplication"] = "false"

    arn = f"arn:aws:sns:{get_region()}:{get_account_id()}:{name}"
    if arn not in _topics:
        default_policy = json.dumps({
            "Version": "2008-10-17",
            "Id": "__default_policy_ID",
            "Statement": [{
                "Sid": "__default_statement_ID",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": ["SNS:Publish", "SNS:Subscribe", "SNS:Receive"],
                "Resource": arn,
                "Condition": {"StringEquals": {"AWS:SourceOwner": get_account_id()}},
            }],
        })
        topic = {
            "name": name,
            "arn": arn,
            "attributes": {
                "TopicArn": arn,
                "DisplayName": "",
                "Owner": get_account_id(),
                "Policy": default_policy,
                "SubscriptionsConfirmed": "0",
                "SubscriptionsPending": "0",
                "SubscriptionsDeleted": "0",
                "EffectiveDeliveryPolicy": json.dumps({
                    "http": {
                        "defaultHealthyRetryPolicy": {
                            "minDelayTarget": 20,
                            "maxDelayTarget": 20,
                            "numRetries": 3,
                        }
                    }
                }),
            },
            "subscriptions": [],
            "tags": {},
        }

        # Apply explicit attributes (including auto-set FIFO attrs)
        topic["attributes"].update(explicit_attrs)

        # Initialize FIFO-specific state
        if is_fifo:
            topic["dedup_cache"] = {}
            topic["fifo_seq"] = 0

        topic["tags"].update(_param_tags(params))

        _topics[arn] = topic
        logger.info("SNS topic created: %s%s", name, " (FIFO)" if is_fifo else "")

    return _xml(200, "CreateTopicResponse",
                f"<CreateTopicResult><TopicArn>{arn}</TopicArn></CreateTopicResult>")


def _delete_topic(params):
    arn = _normalize_arn(_p(params, "TopicArn"))
    topic = _topic_by_arn_any_scope(arn)
    if topic is None:
        # Idempotent like real SNS: deleting a missing topic still answers 200.
        return _xml(200, "DeleteTopicResponse", "")
    denied = _enforce_topic_policy(topic, "sns:DeleteTopic")
    if denied is not None:
        return denied
    owner = _topic_owner(topic)
    try:
        region = parse_arn(arn).region or get_region()
    except ArnParseError:
        region = get_region()
    _topics.pop_scoped(owner, region, arn, None)
    for sub in topic.get("subscriptions", []):
        _sub_arn_to_topic.pop(sub["arn"], None)
    return _xml(200, "DeleteTopicResponse", "")


def _list_topics(params):
    all_arns = list(_topics.keys())
    next_token = _p(params, "NextToken")
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = all_arns[start:start + 100]
    members = "".join(
        f"<member><TopicArn>{arn}</TopicArn></member>" for arn in page
    )
    next_token_xml = ""
    if start + 100 < len(all_arns):
        next_token_xml = f"<NextToken>{start + 100}</NextToken>"
    return _xml(200, "ListTopicsResponse",
                f"<ListTopicsResult><Topics>{members}</Topics>{next_token_xml}</ListTopicsResult>")


def _get_topic_attributes(params):
    topic, err = _get_topic(_p(params, "TopicArn"), "sns:GetTopicAttributes")
    if err is not None:
        return err
    _refresh_subscription_counts(topic)
    attrs = "".join(
        f"<entry><key>{k}</key><value>{_xml_escape(v)}</value></entry>"
        for k, v in topic["attributes"].items()
    )
    return _xml(200, "GetTopicAttributesResponse",
                f"<GetTopicAttributesResult><Attributes>{attrs}</Attributes></GetTopicAttributesResult>")


def _set_topic_attributes(params):
    topic, err = _get_topic(_p(params, "TopicArn"), "sns:SetTopicAttributes")
    if err is not None:
        return err
    attr_name = _p(params, "AttributeName")
    attr_val = _p(params, "AttributeValue")
    if attr_name:
        topic["attributes"][attr_name] = attr_val
    return _xml(200, "SetTopicAttributesResponse", "")


def _member_list(params, key: str) -> list:
    """Collect a query-protocol ``<Key>.member.N`` list."""
    out = []
    i = 1
    while True:
        v = _p(params, f"{key}.member.{i}")
        if not v:
            break
        out.append(v)
        i += 1
    return out


def _add_permission(params):
    topic, err = _get_topic(_p(params, "TopicArn"), "sns:AddPermission")
    if err is not None:
        return err
    topic_arn = topic["arn"]
    label = _p(params, "Label")
    if not label:
        return _error("InvalidParameter",
                      "Label is required", 400)
    account_ids = _member_list(params, "AWSAccountId")
    actions = _member_list(params, "ActionName")
    if not account_ids:
        return _error("InvalidParameter",
                      "AWSAccountId is required", 400)
    if not actions:
        return _error("InvalidParameter",
                      "ActionName is required", 400)

    raw = topic["attributes"].get("Policy") or ""
    try:
        policy = json.loads(raw) if raw else {}
    except (TypeError, json.JSONDecodeError):
        policy = {}
    policy.setdefault("Version", "2012-10-17")
    policy.setdefault("Id", f"{topic_arn}/SNSDefaultPolicy")
    statements = policy.setdefault("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
        policy["Statement"] = statements

    if any(s.get("Sid") == label for s in statements):
        return _error("InvalidParameter",
                      f"Value {label} for parameter Label is invalid. "
                      f"Reason: Already exists.", 400)

    statements.append({
        "Sid": label,
        "Effect": "Allow",
        "Principal": {"AWS": list(account_ids)},
        "Action": [a if a.startswith("sns:") else f"sns:{a[4:]}" if a.startswith("SNS:") else f"sns:{a}" for a in actions],
        "Resource": topic_arn,
    })
    topic["attributes"]["Policy"] = json.dumps(policy)
    return _xml(200, "AddPermissionResponse", "")


def _remove_permission(params):
    topic, err = _get_topic(_p(params, "TopicArn"), "sns:RemovePermission")
    if err is not None:
        return err
    label = _p(params, "Label")
    if not label:
        return _error("InvalidParameter",
                      "Label is required", 400)

    raw = topic["attributes"].get("Policy") or ""
    if raw:
        try:
            policy = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            policy = {}
        statements = policy.get("Statement") or []
        if isinstance(statements, dict):
            statements = [statements]
        remaining = [s for s in statements if s.get("Sid") != label]
        if remaining:
            policy["Statement"] = remaining
            topic["attributes"]["Policy"] = json.dumps(policy)
        else:
            topic["attributes"].pop("Policy", None)
    return _xml(200, "RemovePermissionResponse", "")


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------

def _subscribe(params):
    protocol = _p(params, "Protocol")
    endpoint = _p(params, "Endpoint")

    topic, err = _get_topic(_p(params, "TopicArn"), "sns:Subscribe")
    if err is not None:
        return err
    topic_arn = topic["arn"]

    if not protocol:
        return _error("InvalidParameterException", "Protocol is required", 400)
    endpoint_error = _validate_subscription_endpoint(protocol, endpoint)
    if endpoint_error:
        return endpoint_error

    for existing in topic["subscriptions"]:
        if existing["protocol"] == protocol and existing["endpoint"] == endpoint:
            return _xml(200, "SubscribeResponse",
                        f"<SubscribeResult><SubscriptionArn>{existing['arn']}</SubscriptionArn></SubscribeResult>")

    sub_arn = f"{topic_arn}:{new_uuid()}"
    needs_confirmation = protocol in ("http", "https")

    sub = {
        "arn": sub_arn,
        "protocol": protocol,
        "endpoint": endpoint,
        "confirmed": not needs_confirmation,
        "topic_arn": topic_arn,
        "owner": get_account_id(),
        "token": new_uuid() if needs_confirmation else None,
        "attributes": {
            "SubscriptionArn": sub_arn,
            "TopicArn": topic_arn,
            "Protocol": protocol,
            "Endpoint": endpoint,
            "Owner": get_account_id(),
            "ConfirmationWasAuthenticated": "true" if not needs_confirmation else "false",
            "PendingConfirmation": "true" if needs_confirmation else "false",
            "RawMessageDelivery": "false",
        },
    }

    allowed_attrs = {"DeliveryPolicy", "FilterPolicy", "FilterPolicyScope",
                     "RawMessageDelivery", "RedrivePolicy", "SubscriptionRoleArn"}
    i = 1
    while _p(params, f"Attributes.entry.{i}.key"):
        key = _p(params, f"Attributes.entry.{i}.key")
        val = _p(params, f"Attributes.entry.{i}.value")
        if key in allowed_attrs:
            sub["attributes"][key] = val or ""
        i += 1

    topic["subscriptions"].append(sub)
    _sub_arn_to_topic[sub_arn] = topic_arn
    _refresh_subscription_counts(topic)

    if needs_confirmation:
        asyncio.ensure_future(_send_subscription_confirmation(topic_arn, sub))

    # Real AWS returns the literal lowercase string "pending confirmation"
    # (with a space) as the SubscriptionArn until the subscriber confirms.
    result_arn = "pending confirmation" if needs_confirmation else sub_arn
    return _xml(200, "SubscribeResponse",
                f"<SubscribeResult><SubscriptionArn>{result_arn}</SubscriptionArn></SubscribeResult>")


def _confirm_subscription(params):
    token = _p(params, "Token")

    # Gated on the confirmation secret, not the topic policy: the
    # SubscribeURL GET carries no SigV4 identity, so the topic must resolve
    # outside the caller's scope.
    topic, err = _get_topic(_p(params, "TopicArn"))
    if err is not None:
        return err
    topic_arn = topic["arn"]

    if not token:
        return _error("InvalidParameterException", "Token is required", 400)

    for sub in topic["subscriptions"]:
        if sub.get("token") == token:
            sub["confirmed"] = True
            sub["token"] = None
            sub["attributes"]["PendingConfirmation"] = "false"
            sub["attributes"]["ConfirmationWasAuthenticated"] = "true"
            _refresh_subscription_counts(topic)
            return _xml(200, "ConfirmSubscriptionResponse",
                        f"<ConfirmSubscriptionResult><SubscriptionArn>{sub['arn']}</SubscriptionArn></ConfirmSubscriptionResult>")

    return _error("InvalidParameterException", "Invalid token", 400)


def _unsubscribe(params):
    sub_arn = _p(params, "SubscriptionArn")
    topic_arn = _sub_arn_to_topic.get(sub_arn)
    topic = _topic_by_arn_any_scope(topic_arn) if topic_arn else None
    if topic is not None:
        topic["subscriptions"] = [s for s in topic["subscriptions"] if s["arn"] != sub_arn]
        _refresh_subscription_counts(topic)
    _sub_arn_to_topic.pop(sub_arn, None)
    return _xml(200, "UnsubscribeResponse", "")


def _list_subscriptions(params):
    all_subs = []
    for topic in _topics.values():
        for sub in topic["subscriptions"]:
            all_subs.append(sub)
    next_token = _p(params, "NextToken")
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = all_subs[start:start + 100]
    members = ""
    for sub in page:
        members += (
            "<member>"
            f"<SubscriptionArn>{sub['arn']}</SubscriptionArn>"
            f"<Owner>{sub.get('owner', get_account_id())}</Owner>"
            f"<TopicArn>{sub['topic_arn']}</TopicArn>"
            f"<Protocol>{sub['protocol']}</Protocol>"
            f"<Endpoint>{_xml_escape(sub['endpoint'])}</Endpoint>"
            "</member>"
        )
    next_token_xml = ""
    if start + 100 < len(all_subs):
        next_token_xml = f"<NextToken>{start + 100}</NextToken>"
    return _xml(200, "ListSubscriptionsResponse",
                f"<ListSubscriptionsResult><Subscriptions>{members}</Subscriptions>{next_token_xml}</ListSubscriptionsResult>")


def _list_subscriptions_by_topic(params):
    topic, err = _get_topic(_p(params, "TopicArn"), "sns:ListSubscriptionsByTopic")
    if err is not None:
        return err
    topic_arn = topic["arn"]
    members = ""
    for sub in topic["subscriptions"]:
        members += (
            "<member>"
            f"<SubscriptionArn>{sub['arn']}</SubscriptionArn>"
            f"<Owner>{sub.get('owner', get_account_id())}</Owner>"
            f"<TopicArn>{topic_arn}</TopicArn>"
            f"<Protocol>{sub['protocol']}</Protocol>"
            f"<Endpoint>{_xml_escape(sub['endpoint'])}</Endpoint>"
            "</member>"
        )
    return _xml(200, "ListSubscriptionsByTopicResponse",
                f"<ListSubscriptionsByTopicResult><Subscriptions>{members}</Subscriptions></ListSubscriptionsByTopicResult>")


def _get_subscription_attributes(params):
    sub_arn = _p(params, "SubscriptionArn")
    topic_arn = _sub_arn_to_topic.get(sub_arn)
    if not topic_arn or _topic_by_arn_any_scope(topic_arn) is None:
        return _error("NotFound", f"Subscription does not exist: {sub_arn}", 404)

    sub = _find_subscription(topic_arn, sub_arn)
    if not sub:
        return _error("NotFound", f"Subscription does not exist: {sub_arn}", 404)

    attrs = "".join(
        f"<entry><key>{k}</key><value>{_xml_escape(v)}</value></entry>"
        for k, v in sub["attributes"].items()
    )
    return _xml(200, "GetSubscriptionAttributesResponse",
                f"<GetSubscriptionAttributesResult><Attributes>{attrs}</Attributes></GetSubscriptionAttributesResult>")


def _set_subscription_attributes(params):
    sub_arn = _p(params, "SubscriptionArn")
    topic_arn = _sub_arn_to_topic.get(sub_arn)
    if not topic_arn or _topic_by_arn_any_scope(topic_arn) is None:
        return _error("NotFound", f"Subscription does not exist: {sub_arn}", 404)

    sub = _find_subscription(topic_arn, sub_arn)
    if not sub:
        return _error("NotFound", f"Subscription does not exist: {sub_arn}", 404)

    attr_name = _p(params, "AttributeName")
    attr_val = _p(params, "AttributeValue")

    allowed = {"DeliveryPolicy", "FilterPolicy", "FilterPolicyScope",
               "RawMessageDelivery", "RedrivePolicy", "SubscriptionRoleArn"}
    if attr_name not in allowed:
        return _error("InvalidParameterException",
                      f"Invalid attribute name: {attr_name}", 400)

    if attr_name == "FilterPolicy" and attr_val:
        try:
            json.loads(attr_val)
        except json.JSONDecodeError:
            return _error("InvalidParameterException", "Invalid JSON in FilterPolicy", 400)

    sub["attributes"][attr_name] = attr_val
    return _xml(200, "SetSubscriptionAttributesResponse", "")


# ---------------------------------------------------------------------------
# FIFO helpers
# ---------------------------------------------------------------------------

# AWS SNS FIFO topics deduplicate messages for exactly 5 minutes (300 s).
# Publishing the same MessageDeduplicationId within this window returns the
# original MessageId/SequenceNumber without re-delivering to subscribers.
# Reference: https://docs.aws.amazon.com/sns/latest/dg/fifo-message-dedup.html
_DEDUP_WINDOW_S = 300
_fifo_lock = _threading.Lock()


def _is_fifo_topic(topic: dict) -> bool:
    """Return True if the topic is a FIFO topic."""
    return topic.get("attributes", {}).get("FifoTopic") == "true"


def _prune_sns_dedup(topic: dict) -> None:
    """Remove expired entries (older than 300s) from the topic's dedup_cache."""
    now = time.time()
    topic["dedup_cache"] = {
        k: v for k, v in topic.get("dedup_cache", {}).items()
        if v["expire"] > now
    }


def _resolve_dedup_id(topic: dict, params: dict, message: str) -> str:
    """Resolve the effective MessageDeduplicationId.

    Priority:
      1. Explicit param value
      2. SHA-256 of body when ContentBasedDeduplication is enabled
      3. Raise ValueError when neither is available
    """
    explicit = _p(params, "MessageDeduplicationId") or ""
    if explicit:
        return explicit

    cbd = topic.get("attributes", {}).get("ContentBasedDeduplication", "false")
    if cbd == "true":
        return hashlib.sha256(message.encode()).hexdigest()

    raise ValueError(
        "Invalid parameter: The MessageDeduplicationId parameter is required "
        "for FIFO topics when ContentBasedDeduplication is not enabled"
    )


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------

class SnsPublishError(ValueError):
    """A publish AWS rejects, carrying the error code the API answers with."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def publish_internal(
    topic_arn: str,
    message,
    subject: str = "",
    *,
    message_structure: str = "",
    message_attributes: dict | None = None,
    message_group_id: str = "",
    message_deduplication_id: str = "",
) -> dict | None:
    """Publish to a topic from inside MiniStack, bypassing the HTTP action.

    Cross-service producers (an IoT topic rule, an EventBridge target, a Pipes
    enrichment) reach SNS in-process, and every site that hand-rolls the message
    append plus fan-out loses whatever the Publish path does around them — the
    payload size limit, FIFO grouping and deduplication, and the message_structure
    / message_attributes fields a subscriber filter policy reads. This is that
    path, and `_publish` itself is the HTTP wrapper over it.

    Returns ``{"message_id", "sequence_number", "duplicate"}``, or ``None`` when
    the topic does not exist. Raises `SnsPublishError` for a publish AWS would
    reject — notably a FIFO topic addressed without a MessageGroupId.
    """
    topic = _topic_by_arn_any_scope(topic_arn)
    if topic is None:
        return None

    if isinstance(message, (dict, list)):
        message = json.dumps(message)
    msg_attrs = message_attributes or {}

    # AWS rejects Publish requests whose Message + MessageAttributes exceed
    # 256 KiB. Real-AWS error code is InvalidParameter (400).
    if _message_payload_size(message, msg_attrs) > _SNS_MAX_PAYLOAD_BYTES:
        raise SnsPublishError(
            "InvalidParameter",
            f"Invalid parameter: Message too long. Maximum size is {_SNS_MAX_PAYLOAD_BYTES} bytes.",
        )

    seq_number = None
    dedup_id = message_deduplication_id

    # -- FIFO validation, deduplication, and sequencing --
    if _is_fifo_topic(topic):
        if not message_group_id:
            raise SnsPublishError(
                "InvalidParameterException",
                "Invalid parameter: The MessageGroupId parameter is required for FIFO topics",
            )
        # Resolve dedup ID: explicit > CBD SHA-256 > error
        try:
            dedup_id = _resolve_dedup_id(
                topic, {"MessageDeduplicationId": message_deduplication_id}, message
            )
        except ValueError as exc:
            raise SnsPublishError("InvalidParameterException", str(exc)) from exc

        # Prune expired cache entries, then check for duplicate
        with _fifo_lock:
            _prune_sns_dedup(topic)
            cached = topic.get("dedup_cache", {}).get(dedup_id)
            if cached:
                # Duplicate within the 5-minute window — replay the original
                # result without re-delivering to subscribers.
                return {
                    "message_id": cached["message_id"],
                    "sequence_number": cached["sequence_number"],
                    "duplicate": True,
                }

            # New message: increment sequence counter
            topic["fifo_seq"] = topic.get("fifo_seq", 0) + 1
            seq_number = str(topic["fifo_seq"]).zfill(20)
            msg_id = new_uuid()

            # Cache the entry for deduplication (300s window)
            topic.setdefault("dedup_cache", {})[dedup_id] = {
                "expire": time.time() + _DEDUP_WINDOW_S,
                "message_id": msg_id,
                "sequence_number": seq_number,
            }
    else:
        msg_id = new_uuid()

    _fanout(topic_arn, msg_id, message, subject, message_structure, msg_attrs,
            message_group_id=message_group_id, message_dedup_id=dedup_id)
    logger.info(
        "SNS%s publish to %s: %s", " FIFO" if seq_number else "", topic_arn, message[:100]
    )
    return {"message_id": msg_id, "sequence_number": seq_number, "duplicate": False}


def _publish(params):
    topic_arn = _normalize_arn(_p(params, "TopicArn") or _p(params, "TargetArn"))
    phone_number = _p(params, "PhoneNumber")
    message = _p(params, "Message")
    subject = _p(params, "Subject")
    message_structure = _p(params, "MessageStructure")

    if isinstance(message, (dict, list)):
        message = json.dumps(message)

    if phone_number and not topic_arn:
        msg_id = new_uuid()
        _sms_log_for(phone_number).append({
            "PhoneNumber": phone_number,
            "TopicArn": None,
            "SubscriptionArn": None,
            "MessageId": msg_id,
            "Message": message,
            "MessageAttributes": _parse_message_attributes(params),
            "MessageStructure": message_structure or None,
            "Subject": subject or None,
        })
        logger.info("SNS SMS to %s: %s", phone_number, message[:80])
        return _xml(200, "PublishResponse",
                    f"<PublishResult><MessageId>{msg_id}</MessageId></PublishResult>")

    if not topic_arn:
        return _error("InvalidParameterException",
                      "TopicArn, TargetArn, or PhoneNumber is required", 400)

    topic, err = _get_topic(topic_arn, "sns:Publish")
    if err is not None:
        # Publishing directly to a mobile-push platform endpoint (TargetArn) is
        # valid in AWS — https://docs.aws.amazon.com/sns/latest/api/API_Publish.html
        # We don't deliver anything, but the call must succeed.
        if topic_arn in _platform_endpoints:
            msg_id = new_uuid()
            logger.info("SNS platform-endpoint publish stub to %s", topic_arn)
            return _xml(200, "PublishResponse",
                        f"<PublishResult><MessageId>{msg_id}</MessageId></PublishResult>")
        return err
    topic_arn = topic["arn"]

    try:
        result = publish_internal(
            topic_arn,
            message,
            subject,
            message_structure=message_structure,
            message_attributes=_parse_message_attributes(params),
            message_group_id=_p(params, "MessageGroupId") or "",
            message_deduplication_id=_p(params, "MessageDeduplicationId") or "",
        )
    except SnsPublishError as exc:
        return _error(exc.code, str(exc), 400)

    inner = f"<MessageId>{result['message_id']}</MessageId>"
    if result["sequence_number"] is not None:
        inner += f"<SequenceNumber>{result['sequence_number']}</SequenceNumber>"
    return _xml(200, "PublishResponse", f"<PublishResult>{inner}</PublishResult>")


def _publish_batch(params):
    topic_arn = _normalize_arn(_p(params, "TopicArn"))
    if not topic_arn:
        return _error("InvalidParameterException", "TopicArn is required", 400)
    topic, err = _get_topic(topic_arn, "sns:Publish")
    if err is not None:
        return err
    topic_arn = topic["arn"]

    entries = _parse_batch_entries(params)
    if not entries:
        return _error("InvalidParameterException",
                      "PublishBatchRequestEntries is required", 400)
    if len(entries) > 10:
        return _error("TooManyEntriesInBatchRequest",
                      "The batch request contains more entries than permissible", 400)

    ids_seen = set()
    for entry in entries:
        eid = entry.get("id", "")
        if eid in ids_seen:
            return _error("BatchEntryIdsNotDistinct",
                          "Batch entry ids must be distinct", 400)
        ids_seen.add(eid)

    fifo = _is_fifo_topic(topic)

    successful = ""
    failed = ""
    for entry in entries:
        eid = entry["id"]
        message = entry.get("message", "")
        subject = entry.get("subject", "")
        message_structure = entry.get("message_structure", "")
        msg_attrs = entry.get("message_attributes", {})
        group_id = entry.get("message_group_id", "")
        entry_dedup_id = entry.get("message_dedup_id", "")

        # Per-entry payload size check: real AWS surfaces each oversized entry
        # as a per-entry failure rather than failing the whole batch.
        if _message_payload_size(message, msg_attrs) > _SNS_MAX_PAYLOAD_BYTES:
            failed += (
                "<member>"
                f"<Id>{_xml_escape(eid)}</Id>"
                f"<Code>InvalidParameter</Code>"
                f"<Message>Invalid parameter: Message too long. Maximum size is {_SNS_MAX_PAYLOAD_BYTES} bytes.</Message>"
                f"<SenderFault>true</SenderFault>"
                "</member>"
            )
            continue

        # ── FIFO per-entry validation ──
        if fifo:
            if not group_id:
                failed += (
                    "<member>"
                    f"<Id>{_xml_escape(eid)}</Id>"
                    f"<Code>InvalidParameterException</Code>"
                    f"<Message>Invalid parameter: The MessageGroupId parameter is required for FIFO topics</Message>"
                    f"<SenderFault>true</SenderFault>"
                    "</member>"
                )
                continue

            # Build a mini params dict so _resolve_dedup_id can read the explicit value
            entry_params = {}
            if entry_dedup_id:
                entry_params["MessageDeduplicationId"] = [entry_dedup_id]
            try:
                dedup_id = _resolve_dedup_id(topic, entry_params, message)
            except ValueError as exc:
                failed += (
                    "<member>"
                    f"<Id>{_xml_escape(eid)}</Id>"
                    f"<Code>InvalidParameterException</Code>"
                    f"<Message>{_xml_escape(str(exc))}</Message>"
                    f"<SenderFault>true</SenderFault>"
                    "</member>"
                )
                continue

            # Dedup check
            with _fifo_lock:
                _prune_sns_dedup(topic)
                cached = topic.get("dedup_cache", {}).get(dedup_id)
                if cached:
                    successful += (
                        "<member>"
                        f"<Id>{_xml_escape(eid)}</Id>"
                        f"<MessageId>{cached['message_id']}</MessageId>"
                        f"<SequenceNumber>{cached['sequence_number']}</SequenceNumber>"
                        "</member>"
                    )
                    continue

                # New FIFO message: increment sequence counter
                topic["fifo_seq"] = topic.get("fifo_seq", 0) + 1
                seq_number = str(topic["fifo_seq"]).zfill(20)
                msg_id = new_uuid()

                # Cache for deduplication
                topic.setdefault("dedup_cache", {})[dedup_id] = {
                    "expire": time.time() + _DEDUP_WINDOW_S,
                    "message_id": msg_id,
                    "sequence_number": seq_number,
                }

            _fanout(topic_arn, msg_id, message, subject, message_structure, msg_attrs,
                    message_group_id=group_id, message_dedup_id=dedup_id)

            successful += (
                "<member>"
                f"<Id>{_xml_escape(eid)}</Id>"
                f"<MessageId>{msg_id}</MessageId>"
                f"<SequenceNumber>{seq_number}</SequenceNumber>"
                "</member>"
            )
        else:
            # ── Standard (non-FIFO) batch entry ──
            msg_id = new_uuid()
            _fanout(topic_arn, msg_id, message, subject, message_structure, msg_attrs)

            successful += (
                "<member>"
                f"<Id>{_xml_escape(eid)}</Id>"
                f"<MessageId>{msg_id}</MessageId>"
                "</member>"
            )

    return _xml(200, "PublishBatchResponse",
                f"<PublishBatchResult>"
                f"<Successful>{successful}</Successful>"
                f"<Failed>{failed}</Failed>"
                f"</PublishBatchResult>")


# ---------------------------------------------------------------------------
# Fanout
# ---------------------------------------------------------------------------

def _fanout(topic_arn: str, msg_id: str, message: str, subject: str,
            message_structure: str = "", message_attributes: dict | None = None,
            message_group_id: str = "", message_dedup_id: str = ""):
    topic = _topic_by_arn_any_scope(topic_arn)
    if not topic:
        return
    try:
        _spec = parse_arn(topic_arn)
        _owner, _region = _spec.account_id, _spec.region
    except ArnParseError:
        _owner, _region = get_account_id(), get_region()

    for sub in topic["subscriptions"]:
        if not sub.get("confirmed"):
            continue

        protocol = sub.get("protocol", "")
        endpoint = sub.get("endpoint", "")

        effective_message = _resolve_message_for_protocol(
            message, message_structure, protocol
        )
        if not _matches_filter_policy(sub, message_attributes or {}, effective_message):
            continue

        raw = sub.get("attributes", {}).get("RawMessageDelivery", "false") == "true"
        envelope = _build_envelope(
            topic_arn, msg_id, effective_message, subject,
            message_attributes or {}, raw
        )

        if protocol == "sqs":
            _deliver_to_sqs(endpoint, envelope, raw, effective_message,
                           message_group_id=message_group_id, message_dedup_id=message_dedup_id,
                           message_attributes=message_attributes or {},
                           topic_arn=topic_arn)
        elif protocol in ("http", "https"):
            _threading.Thread(
                target=asyncio.run,
                args=(_deliver_to_http(endpoint, envelope),),
                daemon=True,
            ).start()
        elif protocol == "lambda":
            # SNS delivers to Lambda asynchronously: Publish returns as soon as
            # the notification is accepted and must not block on the
            # subscriber's execution. Deliver on a background thread, mirroring
            # the http(s) path above; a slow or failing subscriber Lambda no
            # longer stalls the Publish call (or its upstream caller).
            # The topic owner's account/region contextvars must travel into the
            # thread: _get_func_record_for_ref rejects an ARN whose account
            # differs from get_account_id(), and a fresh thread's empty context
            # reads back the default account — dropping every delivery for a
            # non-default tenant. After a cross-account publish the publisher's
            # scope would hide the owner's function, so the owner's is pinned.
            with request_scope(_owner or get_account_id(), _region or get_region()):
                _sns_ctx = contextvars.copy_context()
            _threading.Thread(
                target=_sns_ctx.run,
                args=(_deliver_to_lambda, endpoint, envelope, topic_arn, sub["arn"], msg_id, effective_message, message_attributes or {}),
                daemon=True,
            ).start()
        elif protocol == "email" or protocol == "email-json":
            logger.info("SNS fanout → email %s (stub)", endpoint)
        elif protocol == "sms":
            logger.info("SNS fanout → SMS %s (stub)", endpoint)
        elif protocol == "application":
            logger.info("SNS fanout → application %s (stub)", endpoint)


def _deliver_to_sqs(endpoint: str, envelope: str, raw: bool, raw_message: str,
                    message_group_id: str = "", message_dedup_id: str = "",
                    message_attributes: dict | None = None,
                    topic_arn: str = ""):
    try:
        spec = parse_arn(endpoint)
    except ArnParseError:
        logger.warning("SNS fanout: invalid SQS endpoint ARN %s", endpoint)
        return
    queue_name = _sqs_queue_name_from_arn_spec(spec)
    if not queue_name:
        logger.warning("SNS fanout: invalid SQS endpoint ARN %s", endpoint)
        return
    queue = _sqs._queue_by_arn(str(spec))
    if not queue:
        logger.warning("SNS fanout: SQS queue %s not found", queue_name)
        return
    try:
        source_account = parse_arn(topic_arn).account_id if topic_arn else ""
    except ArnParseError:
        source_account = ""
    if not _sqs.queue_policy_allows(str(spec), "sns.amazonaws.com", topic_arn,
                                    source_account or get_account_id()):
        logger.warning("SNS fanout: queue policy denies delivery from %s to %s",
                       topic_arn, queue_name)
        return

    body = raw_message if raw else envelope
    sqs_attrs = dict(message_attributes) if raw and message_attributes else {}
    now = time.time()
    msg = {
        "id": new_uuid(),
        "body": body,
        "md5": hashlib.md5(body.encode()).hexdigest(),
        "message_attributes": sqs_attrs,
        # Real SQS emits MD5OfMessageAttributes alongside MD5OfBody on
        # ReceiveMessage; the field reads from msg["md5_attrs"]. Without
        # this, raw SNS→SQS deliveries diverge from real AWS for
        # consumers that verify the attribute MD5 (Java/Go SDKs do).
        "md5_attrs": _sqs._md5_msg_attrs(sqs_attrs),
        "receipt_handle": None,
        "sent_at": now,
        "visible_at": now + _sqs.queue_delay(queue),
        "receive_count": 0,
    }
    if message_group_id:
        msg["group_id"] = message_group_id
    if message_dedup_id:
        msg["dedup_id"] = message_dedup_id
    _sqs._ensure_msg_fields(msg)
    queue["messages"].append(msg)
    esm_wake.set()
    logger.info("SNS fanout → SQS %s", queue_name)


def _deliver_to_lambda(endpoint: str, envelope: str, topic_arn: str, sub_arn: str,
                       msg_id: str, raw_message: str, message_attributes: dict):
    """Invoke a Lambda function with the SNS Records envelope (AWS format)."""
    # endpoint is a Lambda ARN: arn:aws:lambda:region:account:function:name
    func, config, func_name = _lambda_svc._get_func_record_for_ref(endpoint)
    if not func or not config:
        logger.warning("SNS fanout: Lambda function %s not found", func_name)
        return
    event = {
        "Records": [
            {
                "EventVersion": "1.0",
                "EventSubscriptionArn": sub_arn,
                "EventSource": "aws:sns",
                "Sns": json.loads(envelope),
            }
        ]
    }
    try:
        exec_record = _lambda_svc._execution_record_for_config(func, config)
        _lambda_svc._execute_function_with_config_scope(exec_record, event)
        logger.info("SNS fanout → Lambda %s", func_name)
    except Exception as exc:
        logger.error("SNS fanout → Lambda %s failed: %s", func_name, exc)


def _http_post_sync(endpoint: str, payload: str, sns_message_type: str) -> int:
    """Blocking HTTP POST for SNS delivery. Runs on a worker thread so the
    event loop stays unblocked. Uses stdlib only — aiohttp was dropped because
    it isn't a declared dependency and wasn't shipped in the Docker image,
    which silently skipped every HTTP subscription confirmation (#460).

    Handles `http://user:pass@host/path` userinfo by stripping it from the URL
    and promoting it to the HTTP auth header, matching real AWS SNS
    behaviour for HTTP(S) endpoints with embedded credentials. urllib leaves
    userinfo in the URL by default, which would break the Host header."""
    import base64 as _b64
    import urllib.parse
    import urllib.request
    parsed = urllib.parse.urlsplit(endpoint)
    headers = {
        "Content-Type": "text/plain; charset=UTF-8",
        "x-amz-sns-message-type": sns_message_type,
    }
    if parsed.username is not None:
        user = urllib.parse.unquote(parsed.username)
        pwd = urllib.parse.unquote(parsed.password or "")
        token = _b64.b64encode(f"{user}:{pwd}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {token}"
        netloc = parsed.hostname or ""
        if parsed.port is not None:
            netloc = f"{netloc}:{parsed.port}"
        endpoint = urllib.parse.urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))
    req = urllib.request.Request(
        endpoint,
        data=payload.encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status


async def _deliver_to_http(endpoint: str, payload: str):
    try:
        status = await asyncio.to_thread(_http_post_sync, endpoint, payload, "Notification")
        logger.info("SNS HTTP delivery to %s: %s", endpoint, status)
    except Exception as exc:
        logger.warning("SNS HTTP delivery to %s failed: %s", endpoint, exc)


async def _send_subscription_confirmation(topic_arn: str, sub: dict):
    endpoint = sub.get("endpoint", "")
    token = sub.get("token", "")
    payload = json.dumps({
        "Type": "SubscriptionConfirmation",
        "MessageId": new_uuid(),
        "TopicArn": topic_arn,
        "Token": token,
        "Message": f"You have chosen to subscribe to the topic {topic_arn}. "
                   f"To confirm the subscription, visit the SubscribeURL included in this message.",
        "SubscribeURL": f"http://{_HOST}:{_PORT}/?Action=ConfirmSubscription&TopicArn={topic_arn}&Token={token}",
        "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "SignatureVersion": "1",
        "Signature": "FAKE",
        "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-fake.pem",
    })
    try:
        status = await asyncio.to_thread(_http_post_sync, endpoint, payload, "SubscriptionConfirmation")
        logger.info("SNS SubscriptionConfirmation sent to %s: %s", endpoint, status)
    except Exception as exc:
        logger.warning("SNS SubscriptionConfirmation to %s failed: %s", endpoint, exc)


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _list_tags_for_resource(params):
    arn, topic, err = _resolve_topic_tag_arn(_p(params, "ResourceArn"),
                                             "sns:ListTagsForResource")
    if err:
        return err
    tags_xml = ""
    for k, v in topic.get("tags", {}).items():
        tags_xml += f"<member><Key>{k}</Key><Value>{v}</Value></member>"
    return _xml(200, "ListTagsForResourceResponse",
                f"<ListTagsForResourceResult><Tags>{tags_xml}</Tags></ListTagsForResourceResult>")


def _param_tags(params) -> dict:
    """The Tags.member.N pairs of a CreateTopic or TagResource request."""
    tags = {}
    i = 1
    while _p(params, f"Tags.member.{i}.Key"):
        tags[_p(params, f"Tags.member.{i}.Key")] = _p(params, f"Tags.member.{i}.Value")
        i += 1
    return tags


def _tag_resource(params):
    _arn, topic, err = _resolve_topic_tag_arn(_p(params, "ResourceArn"),
                                              "sns:TagResource")
    if err:
        return err
    topic["tags"].update(_param_tags(params))
    return _xml(200, "TagResourceResponse", "<TagResourceResult/>")


def _untag_resource(params):
    _arn, topic, err = _resolve_topic_tag_arn(_p(params, "ResourceArn"),
                                              "sns:UntagResource")
    if err:
        return err
    i = 1
    while _p(params, f"TagKeys.member.{i}"):
        topic.get("tags", {}).pop(_p(params, f"TagKeys.member.{i}"), None)
        i += 1
    return _xml(200, "UntagResourceResponse", "<UntagResourceResult/>")


# ---------------------------------------------------------------------------
# Platform application stubs
# ---------------------------------------------------------------------------

def _create_platform_application(params):
    name = _p(params, "Name")
    platform = _p(params, "Platform")
    if not name or not platform:
        return _error("InvalidParameterException", "Name and Platform are required", 400)

    arn = f"arn:aws:sns:{get_region()}:{get_account_id()}:app/{platform}/{name}"
    attrs = {}
    i = 1
    while _p(params, f"Attributes.entry.{i}.key"):
        key = _p(params, f"Attributes.entry.{i}.key")
        val = _p(params, f"Attributes.entry.{i}.value")
        attrs[key] = val
        i += 1

    attrs.setdefault("Enabled", "true")

    _platform_applications[arn] = {
        "arn": arn,
        "name": name,
        "platform": platform,
        "attributes": attrs,
    }
    return _xml(200, "CreatePlatformApplicationResponse",
                f"<CreatePlatformApplicationResult>"
                f"<PlatformApplicationArn>{arn}</PlatformApplicationArn>"
                f"</CreatePlatformApplicationResult>")


def _list_platform_applications(params):
    # https://docs.aws.amazon.com/sns/latest/api/API_ListPlatformApplications.html
    # Returns up to 100 per page; NextToken is the numeric offset of the next
    # page. The store is account+region scoped, so .values() already filters to
    # the caller's region, matching AWS's per-region platform applications.
    all_apps = list(_platform_applications.values())
    next_token = _p(params, "NextToken")
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = all_apps[start:start + 100]
    members = "".join(
        "<member>"
        f"<PlatformApplicationArn>{_xml_escape(app['arn'])}</PlatformApplicationArn>"
        "<Attributes>"
        + "".join(
            f"<entry><key>{_xml_escape(k)}</key><value>{_xml_escape(v)}</value></entry>"
            for k, v in app.get("attributes", {}).items()
        )
        + "</Attributes>"
        "</member>"
        for app in page
    )
    next_token_xml = ""
    if start + 100 < len(all_apps):
        next_token_xml = f"<NextToken>{start + 100}</NextToken>"
    return _xml(200, "ListPlatformApplicationsResponse",
                f"<ListPlatformApplicationsResult>"
                f"<PlatformApplications>{members}</PlatformApplications>"
                f"{next_token_xml}</ListPlatformApplicationsResult>")


def _get_platform_application_attributes(params):
    # https://docs.aws.amazon.com/sns/latest/api/API_GetPlatformApplicationAttributes.html
    # PlatformApplicationArn is required (InvalidParameter/400); an unknown ARN
    # is NotFound/404. Attributes are returned as entry key/value pairs, e.g.
    # Enabled, AllowEndpointPolicies, AuthenticationMethod and the Event* topic
    # ARNs. Unlike the endpoint APIs there is no separate "Enabled" default in
    # the request, so CreatePlatformApplication seeds it.
    arn = _p(params, "PlatformApplicationArn")
    if not arn:
        return _error("InvalidParameter",
                      "Invalid parameter: PlatformApplicationArn Reason: no value for required parameter",
                      400)
    app = _platform_applications.get(arn)
    if app is None:
        return _error("NotFound",
                      f"PlatformApplication does not exist: {arn}", 404)
    entries = "".join(
        f"<entry><key>{_xml_escape(k)}</key><value>{_xml_escape(v)}</value></entry>"
        for k, v in app.get("attributes", {}).items()
    )
    return _xml(200, "GetPlatformApplicationAttributesResponse",
                f"<GetPlatformApplicationAttributesResult>"
                f"<Attributes>{entries}</Attributes>"
                f"</GetPlatformApplicationAttributesResult>")


def _set_platform_application_attributes(params):
    # https://docs.aws.amazon.com/sns/latest/api/API_SetPlatformApplicationAttributes.html
    # Both PlatformApplicationArn and Attributes are required; the call merges
    # the supplied entries into the existing attribute map (PlatformCredential,
    # PlatformPrincipal, Event* topic ARNs, *FeedbackRoleArn, Enabled, ...).
    # The response body carries only ResponseMetadata.
    arn = _p(params, "PlatformApplicationArn")
    if not arn:
        return _error("InvalidParameter",
                      "Invalid parameter: PlatformApplicationArn Reason: no value for required parameter",
                      400)
    updates = {}
    i = 1
    while _p(params, f"Attributes.entry.{i}.key"):
        updates[_p(params, f"Attributes.entry.{i}.key")] = \
            _p(params, f"Attributes.entry.{i}.value")
        i += 1
    if not updates:
        return _error("InvalidParameter",
                      "Invalid parameter: Attributes Reason: no value for required parameter",
                      400)
    app = _platform_applications.get(arn)
    if app is None:
        return _error("NotFound",
                      f"PlatformApplication does not exist: {arn}", 404)
    app["attributes"].update(updates)
    return _xml(200, "SetPlatformApplicationAttributesResponse", "")


def _create_platform_endpoint(params):
    app_arn = _p(params, "PlatformApplicationArn")
    token = _p(params, "Token")

    if app_arn not in _platform_applications:
        return _error("NotFound", f"PlatformApplication does not exist: {app_arn}", 404)
    if not token:
        return _error("InvalidParameterException", "Token is required", 400)

    # CustomUserData is a top-level request param (not an Attributes entry).
    custom_user_data = _p(params, "CustomUserData")

    attrs = {"Enabled": "true", "Token": token}
    i = 1
    while _p(params, f"Attributes.entry.{i}.key"):
        key = _p(params, f"Attributes.entry.{i}.key")
        val = _p(params, f"Attributes.entry.{i}.value")
        attrs[key] = val
        i += 1
    if custom_user_data:
        attrs["CustomUserData"] = custom_user_data

    # AWS dedups by Token within a platform application: re-requesting the same
    # Token returns the existing endpoint when CustomUserData matches, but
    # raises if it differs — so callers know to Get/Set the existing endpoint.
    # Idempotency per https://docs.aws.amazon.com/sns/latest/api/API_CreatePlatformEndpoint.html
    # ("if the requester already owns an endpoint with the same device token and
    # attributes, that endpoint's ARN is returned"); duplicate-token error string
    # + parse-the-ARN guidance per
    # https://aws.amazon.com/blogs/mobile/mobile-token-management-with-amazon-sns
    for existing in _platform_endpoints.values():
        if (existing["application_arn"] == app_arn
                and existing["attributes"].get("Token") == token):
            if (existing["attributes"].get("CustomUserData", "")
                    == attrs.get("CustomUserData", "")):
                return _xml(200, "CreatePlatformEndpointResponse",
                            f"<CreatePlatformEndpointResult>"
                            f"<EndpointArn>{existing['arn']}</EndpointArn>"
                            f"</CreatePlatformEndpointResult>")
            return _error(
                "InvalidParameter",
                f"Endpoint {existing['arn']} already exists with the same Token, "
                f"but different attributes.",
                400,
            )

    endpoint_arn = f"{app_arn}/{new_uuid()}"
    _platform_endpoints[endpoint_arn] = {
        "arn": endpoint_arn,
        "application_arn": app_arn,
        "attributes": attrs,
    }
    return _xml(200, "CreatePlatformEndpointResponse",
                f"<CreatePlatformEndpointResult>"
                f"<EndpointArn>{endpoint_arn}</EndpointArn>"
                f"</CreatePlatformEndpointResult>")


def _delete_platform_application(params):
    # Idempotent in AWS. Also drop the application's endpoints.
    # https://docs.aws.amazon.com/sns/latest/api/API_DeletePlatformApplication.html
    arn = _p(params, "PlatformApplicationArn")
    _platform_applications.pop(arn, None)
    stale = [e["arn"] for e in _platform_endpoints.values()
             if e["application_arn"] == arn]
    for ep_arn in stale:
        _platform_endpoints.pop(ep_arn, None)
    return _xml(200, "DeletePlatformApplicationResponse", "")


def _get_endpoint_attributes(params):
    # https://docs.aws.amazon.com/sns/latest/api/API_GetEndpointAttributes.html
    arn = _p(params, "EndpointArn")
    endpoint = _platform_endpoints.get(arn)
    if endpoint is None:
        return _error("NotFound", f"Endpoint does not exist: {arn}", 404)
    entries = "".join(
        f"<entry><key>{_xml_escape(k)}</key><value>{_xml_escape(v)}</value></entry>"
        for k, v in endpoint["attributes"].items()
    )
    return _xml(200, "GetEndpointAttributesResponse",
                f"<GetEndpointAttributesResult><Attributes>{entries}</Attributes>"
                f"</GetEndpointAttributesResult>")


def _set_endpoint_attributes(params):
    # https://docs.aws.amazon.com/sns/latest/api/API_SetEndpointAttributes.html
    arn = _p(params, "EndpointArn")
    endpoint = _platform_endpoints.get(arn)
    if endpoint is None:
        return _error("NotFound", f"Endpoint does not exist: {arn}", 404)
    i = 1
    while _p(params, f"Attributes.entry.{i}.key"):
        key = _p(params, f"Attributes.entry.{i}.key")
        val = _p(params, f"Attributes.entry.{i}.value")
        endpoint["attributes"][key] = val
        i += 1
    return _xml(200, "SetEndpointAttributesResponse", "")


def _delete_endpoint(params):
    # AWS DeleteEndpoint is idempotent — succeeds even if already gone.
    # https://docs.aws.amazon.com/sns/latest/api/API_DeleteEndpoint.html
    arn = _p(params, "EndpointArn")
    _platform_endpoints.pop(arn, None)
    return _xml(200, "DeleteEndpointResponse", "")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _xml(status, root_tag, inner):
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<{root_tag} xmlns="http://sns.amazonaws.com/doc/2010-03-31/">'
        f'{inner}'
        f'<ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>'
        f'</{root_tag}>'
    ).encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    error_type = "Sender" if status < 500 else "Receiver"
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">'
        f'<Error><Type>{error_type}</Type><Code>{code}</Code><Message>{_xml_escape(message)}</Message></Error>'
        f'<RequestId>{new_uuid()}</RequestId>'
        f'</ErrorResponse>'
    ).encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _xml_escape(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;"))


def _find_subscription(topic_arn: str, sub_arn: str) -> dict | None:
    topic = _topic_by_arn_any_scope(topic_arn)
    if not topic:
        return None
    for sub in topic["subscriptions"]:
        if sub["arn"] == sub_arn:
            return sub
    return None


def _refresh_subscription_counts(topic: dict):
    subs = topic.get("subscriptions", [])
    confirmed = sum(1 for s in subs if s.get("confirmed"))
    pending = sum(1 for s in subs if not s.get("confirmed"))
    topic["attributes"]["SubscriptionsConfirmed"] = str(confirmed)
    topic["attributes"]["SubscriptionsPending"] = str(pending)


_SNS_MAX_PAYLOAD_BYTES = 262144  # 256 KiB, per AWS SNS Publish docs


def _message_payload_size(message: str, attrs: dict) -> int:
    """Return the byte size of a Publish payload (Message + MessageAttributes).

    Subject is intentionally excluded — AWS docs limit it to 100 chars but
    don't count it toward the 256 KiB Publish size limit.
    """
    total = len((message or "").encode("utf-8"))
    for name, attr in (attrs or {}).items():
        total += len(name.encode("utf-8"))
        total += len((attr.get("DataType") or "").encode("utf-8"))
        sv = attr.get("StringValue")
        if sv:
            total += len(sv.encode("utf-8"))
        bv = attr.get("BinaryValue")
        if bv:
            total += len(bv) if isinstance(bv, (bytes, bytearray)) else len(bv.encode("utf-8"))
    return total


def _parse_message_attributes(params) -> dict:
    """Parse MessageAttributes.entry.N.Name / .Value.DataType / .Value.StringValue"""
    attrs = {}
    i = 1
    while True:
        name = _p(params, f"MessageAttributes.entry.{i}.Name")
        if not name:
            break
        data_type = _p(params, f"MessageAttributes.entry.{i}.Value.DataType")
        string_val = _p(params, f"MessageAttributes.entry.{i}.Value.StringValue")
        binary_val = _p(params, f"MessageAttributes.entry.{i}.Value.BinaryValue")
        attr = {"DataType": data_type}
        if string_val:
            attr["StringValue"] = string_val
        if binary_val:
            attr["BinaryValue"] = binary_val
        attrs[name] = attr
        i += 1
    return attrs


def _parse_batch_entries(params) -> list[dict]:
    entries = []
    i = 1
    while True:
        eid = _p(params, f"PublishBatchRequestEntries.member.{i}.Id")
        if not eid:
            break
        entry = {
            "id": eid,
            "message": _p(params, f"PublishBatchRequestEntries.member.{i}.Message"),
            "subject": _p(params, f"PublishBatchRequestEntries.member.{i}.Subject"),
            "message_structure": _p(params, f"PublishBatchRequestEntries.member.{i}.MessageStructure"),
            "message_attributes": {},
            "message_group_id": _p(params, f"PublishBatchRequestEntries.member.{i}.MessageGroupId"),
            "message_dedup_id": _p(params, f"PublishBatchRequestEntries.member.{i}.MessageDeduplicationId"),
        }
        j = 1
        while True:
            attr_name = _p(params, f"PublishBatchRequestEntries.member.{i}.MessageAttributes.entry.{j}.Name")
            if not attr_name:
                break
            data_type = _p(params, f"PublishBatchRequestEntries.member.{i}.MessageAttributes.entry.{j}.Value.DataType")
            string_val = _p(params, f"PublishBatchRequestEntries.member.{i}.MessageAttributes.entry.{j}.Value.StringValue")
            entry["message_attributes"][attr_name] = {
                "DataType": data_type,
                "StringValue": string_val,
            }
            j += 1
        entries.append(entry)
        i += 1
    return entries


def _resolve_message_for_protocol(message: str, message_structure: str,
                                   protocol: str) -> str:
    if message_structure != "json":
        return message
    try:
        parsed = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        return message
    if not isinstance(parsed, dict):
        return message
    return parsed.get(protocol, parsed.get("default", message))


def _matches_filter_policy(sub: dict, message_attributes: dict, message: str = "") -> bool:
    policy_json = sub.get("attributes", {}).get("FilterPolicy", "")
    if not policy_json:
        return True
    try:
        policy = json.loads(policy_json)
    except (json.JSONDecodeError, TypeError):
        return True
    if not isinstance(policy, dict):
        return True

    scope = sub.get("attributes", {}).get("FilterPolicyScope", "MessageAttributes")
    if scope == "MessageBody":
        # "Filter policies for the message body assume that the message payload
        # is a well-formed JSON object"; anything else is filtered out.
        try:
            body = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            return False
        return isinstance(body, dict) and _policy_matches(policy, _body_lookup(body), nested=True)
    return _policy_matches(policy, _attribute_lookup(message_attributes))


_OR_RESERVED_MEMBER_KEYS = frozenset({
    "anything-but", "prefix", "suffix", "equals-ignore-case",
    "numeric", "exists", "cidr", "wildcard",
})


def _is_or_operator(value) -> bool:
    """True when a ``$or`` value is SNS's OR operator rather than a literal
    attribute name. AWS recognizes ``$or`` only when it holds an array of at
    least two objects, none of whose field names are reserved rule keywords;
    otherwise ``$or`` is treated as an ordinary attribute name."""
    if not isinstance(value, list) or len(value) < 2:
        return False
    for member in value:
        if not isinstance(member, dict):
            return False
        if any(key in _OR_RESERVED_MEMBER_KEYS for key in member):
            return False
    return True


def _attribute_lookup(message_attributes: dict):
    """Lookup over message attributes: key -> (present, values, nonempty)."""
    def lookup(key):
        attr = message_attributes.get(key)
        if attr is None:
            return False, [], bool(message_attributes)
        return True, _attr_candidate_values(attr), True
    lookup.child = None
    return lookup


def _body_lookup(body: dict):
    """Lookup over a JSON body; an array value matches if any element does."""
    def lookup(key):
        if key not in body or body[key] is None:
            return False, [], bool(body)
        value = body[key]
        return True, (value if isinstance(value, list) else [value]), True
    lookup.child = lambda key: _body_lookup(body[key]) if isinstance(body.get(key), dict) else None
    return lookup


def _policy_matches(policy: dict, lookup, nested: bool = False) -> bool:
    """Match one filter-policy object. Sibling keys are AND-ed; a recognized
    ``$or`` matches when any member policy matches; on a message body a nested
    policy object matches the nested property."""
    for key, rules in policy.items():
        if key == "$or" and _is_or_operator(rules):
            if not any(_policy_matches(member, lookup, nested) for member in rules):
                return False
            continue
        if nested and isinstance(rules, dict):
            child = lookup.child(key)
            if child is None or not _policy_matches(rules, child, nested):
                return False
            continue
        if not isinstance(rules, list):
            rules = [rules]
        present, values, nonempty = lookup(key)
        if not _key_matches(rules, present, values, nonempty):
            return False
    return True


def _key_matches(rules: list, present: bool, values: list, nonempty: bool) -> bool:
    for rule in rules:
        if isinstance(rule, dict) and "exists" in rule:
            # "exists": false "only matches if at least one attribute is present".
            if rule["exists"] is True and present and any(v not in ("", None) for v in values):
                return True
            if rule["exists"] is False and not present and nonempty:
                return True
            continue
        if present and any(_value_matches(value, rule) for value in values):
            return True
    return False


def _attr_candidate_values(attr: dict) -> list:
    """Values to match a message attribute against a filter policy. A scalar
    attribute yields its single StringValue; a String.Array yields each element
    (AWS matches an array attribute when any element matches)."""
    raw = attr.get("StringValue", "")
    if (attr.get("DataType") or "").strip() != "String.Array":
        return [raw]
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return [raw]
    if not isinstance(parsed, list):
        return [raw]
    values = []
    for element in parsed:
        if isinstance(element, bool):
            values.append("true" if element else "false")
        elif isinstance(element, str):
            values.append(element)
        elif isinstance(element, (int, float)):
            values.append(str(element))
    return values or [raw]


def _as_number(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _wildcard_matches(value: str, pattern: str) -> bool:
    """`*` matches any run of characters; everything else is literal."""
    return _re.fullmatch(".*".join(_re.escape(part) for part in pattern.split("*")), value, _re.S) is not None


def _cidr_matches(value: str, cidr: str) -> bool:
    import ipaddress
    try:
        return ipaddress.ip_address(value) in ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        return False


def _string_rule_matches(value, rule: dict) -> bool:
    if not isinstance(value, str):
        return False
    if "wildcard" in rule:
        return isinstance(rule["wildcard"], str) and _wildcard_matches(value, rule["wildcard"])
    if "cidr" in rule:
        return isinstance(rule["cidr"], str) and _cidr_matches(value, rule["cidr"])
    if "prefix" in rule:
        return isinstance(rule["prefix"], str) and value.startswith(rule["prefix"])
    if "suffix" in rule:
        return isinstance(rule["suffix"], str) and value.endswith(rule["suffix"])
    if "equals-ignore-case" in rule:
        return isinstance(rule["equals-ignore-case"], str) and value.lower() == rule["equals-ignore-case"].lower()
    return False


def _value_matches(value, rule) -> bool:
    if isinstance(rule, bool) or rule is None:
        return value is rule or (isinstance(value, str) and value == json.dumps(rule))
    if isinstance(rule, str):
        return isinstance(value, str) and value == rule
    if isinstance(rule, (int, float)):
        num = _as_number(value)
        return num is not None and num == float(rule)
    if not isinstance(rule, dict):
        return False
    if any(k in rule for k in ("prefix", "suffix", "equals-ignore-case", "wildcard", "cidr")):
        return _string_rule_matches(value, rule)
    if "anything-but" in rule:
        excluded = rule["anything-but"]
        if isinstance(excluded, dict):
            return isinstance(value, str) and not _string_rule_matches(value, excluded)
        excluded = excluded if isinstance(excluded, list) else [excluded]
        return not any(_value_matches(value, e) for e in excluded)
    if "numeric" in rule:
        num = _as_number(value)
        return num is not None and _check_numeric(num, rule["numeric"])
    return False


def _check_numeric(value: float, conditions: list) -> bool:
    i = 0
    while i < len(conditions) - 1:
        op = conditions[i]
        threshold = float(conditions[i + 1])
        if op == "=" and value != threshold:
            return False
        if op == ">" and not (value > threshold):
            return False
        if op == ">=" and not (value >= threshold):
            return False
        if op == "<" and not (value < threshold):
            return False
        if op == "<=" and not (value <= threshold):
            return False
        i += 2
    return True


def _build_envelope(topic_arn: str, msg_id: str, message: str, subject: str,
                    message_attributes: dict, raw: bool) -> str:
    if raw:
        return message

    envelope = {
        "Type": "Notification",
        "MessageId": msg_id,
        "TopicArn": topic_arn,
        "Subject": subject or None,
        "Message": message,
        "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "SignatureVersion": "1",
        "Signature": "FAKE",
        "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-fake.pem",
        "UnsubscribeURL": f"http://{_HOST}:{_PORT}/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:{get_region()}:{get_account_id()}:example",
    }

    if message_attributes:
        formatted = {}
        for name, attr in message_attributes.items():
            formatted[name] = {"Type": attr.get("DataType", "String"),
                               "Value": attr.get("StringValue", "")}
        envelope["MessageAttributes"] = formatted

    return json.dumps({k: v for k, v in envelope.items() if v is not None})


def reset():
    _topics.clear()
    _sub_arn_to_topic.clear()
    _sms_messages.clear()
    _platform_applications.clear()
    _platform_endpoints.clear()
