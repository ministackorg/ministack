"""
SNS Service Emulator — AWS-compatible.
Supports: CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes,
          Subscribe, Unsubscribe, ConfirmSubscription,
          ListSubscriptions, ListSubscriptionsByTopic,
          GetSubscriptionAttributes, SetSubscriptionAttributes,
          Publish, PublishBatch,
          ListTagsForResource, TagResource, UntagResource,
          CreatePlatformApplication, CreatePlatformEndpoint.
SNS → Lambda fanout dispatches via _execute_function (synchronous).
FIFO topics: .fifo naming validation, MessageGroupId/MessageDeduplicationId enforcement,
             5-minute deduplication window, sequence numbers, content-based deduplication,
             FIFO SQS subscription validation, PublishBatch FIFO support.
"""

import asyncio
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
from ministack.core.responses import AccountScopedDict, get_account_id, new_uuid, get_region
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

from ministack.core.persistence import PERSIST_STATE, load_state

_topics = AccountScopedDict()
_sub_arn_to_topic = AccountScopedDict()
_platform_applications = AccountScopedDict()
_platform_endpoints = AccountScopedDict()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "topics": copy.deepcopy(_topics),
        "sub_arn_to_topic": copy.deepcopy(_sub_arn_to_topic),
        "platform_applications": copy.deepcopy(_platform_applications),
        "platform_endpoints": copy.deepcopy(_platform_endpoints),
    }


def restore_state(data):
    if data:
        _topics.update(data.get("topics", {}))
        _sub_arn_to_topic.update(data.get("sub_arn_to_topic", {}))
        _platform_applications.update(data.get("platform_applications", {}))
        _platform_endpoints.update(data.get("platform_endpoints", {}))


try:
    _restored = load_state("sns")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    params = dict(query_params)
    if method == "POST" and body:
        form_params = parse_qs(body.decode("utf-8", errors="replace"))
        for k, v in form_params.items():
            params[k] = v

    action = _p(params, "Action")
    handlers = {
        "CreateTopic": _create_topic,
        "DeleteTopic": _delete_topic,
        "ListTopics": _list_topics,
        "GetTopicAttributes": _get_topic_attributes,
        "SetTopicAttributes": _set_topic_attributes,
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
        "CreatePlatformEndpoint": _create_platform_endpoint,
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
            "messages": [],
            "tags": {},
        }

        # Apply explicit attributes (including auto-set FIFO attrs)
        topic["attributes"].update(explicit_attrs)

        # Initialize FIFO-specific state
        if is_fifo:
            topic["dedup_cache"] = {}
            topic["fifo_seq"] = 0

        # Store tags from CreateTopic
        i = 1
        while _p(params, f"Tag.member.{i}.Key"):
            key = _p(params, f"Tag.member.{i}.Key")
            val = _p(params, f"Tag.member.{i}.Value")
            topic["tags"][key] = val
            i += 1

        _topics[arn] = topic

    return _xml(200, "CreateTopicResponse",
                f"<CreateTopicResult><TopicArn>{arn}</TopicArn></CreateTopicResult>")


def _delete_topic(params):
    arn = _normalize_arn(_p(params, "TopicArn"))
    topic = _topics.pop(arn, None)
    if topic:
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
    arn = _normalize_arn(_p(params, "TopicArn"))
    topic = _topics.get(arn)
    if not topic:
        return _error("NotFound", f"Topic does not exist: {arn}", 404)
    _refresh_subscription_counts(topic)
    attrs = "".join(
        f"<entry><key>{k}</key><value>{_xml_escape(v)}</value></entry>"
        for k, v in topic["attributes"].items()
    )
    return _xml(200, "GetTopicAttributesResponse",
                f"<GetTopicAttributesResult><Attributes>{attrs}</Attributes></GetTopicAttributesResult>")


def _set_topic_attributes(params):
    arn = _normalize_arn(_p(params, "TopicArn"))
    topic = _topics.get(arn)
    if not topic:
        return _error("NotFound", f"Topic does not exist: {arn}", 404)
    attr_name = _p(params, "AttributeName")
    attr_val = _p(params, "AttributeValue")
    if attr_name:
        topic["attributes"][attr_name] = attr_val
    return _xml(200, "SetTopicAttributesResponse", "")


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------

def _subscribe(params):
    topic_arn = _normalize_arn(_p(params, "TopicArn"))
    protocol = _p(params, "Protocol")
    endpoint = _p(params, "Endpoint")

    topic = _topics.get(topic_arn)
    if not topic:
        return _error("NotFound", f"Topic does not exist: {topic_arn}", 404)

    if not protocol:
        return _error("InvalidParameterException", "Protocol is required", 400)

    # FIFO subscription validation: SQS endpoints must be FIFO queues
    if _is_fifo_topic(topic) and protocol == "sqs":
        queue_name = (endpoint or "").split(":")[-1]
        if not queue_name.endswith(".fifo"):
            return _error(
                "InvalidParameterException",
                "Invalid parameter: Invalid parameter: Topic with FIFO requires a subscription to a FIFO SQS Queue",
                400,
            )

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
                     "RawMessageDelivery", "RedrivePolicy"}
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

    result_arn = "PendingConfirmation" if needs_confirmation else sub_arn
    return _xml(200, "SubscribeResponse",
                f"<SubscribeResult><SubscriptionArn>{result_arn}</SubscriptionArn></SubscribeResult>")


def _confirm_subscription(params):
    topic_arn = _normalize_arn(_p(params, "TopicArn"))
    token = _p(params, "Token")

    topic = _topics.get(topic_arn)
    if not topic:
        return _error("NotFound", f"Topic does not exist: {topic_arn}", 404)

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
    if topic_arn and topic_arn in _topics:
        topic = _topics[topic_arn]
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
    topic_arn = _normalize_arn(_p(params, "TopicArn"))
    topic = _topics.get(topic_arn)
    if not topic:
        return _error("NotFound", f"Topic does not exist: {topic_arn}", 404)
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
    if not topic_arn or topic_arn not in _topics:
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
    if not topic_arn or topic_arn not in _topics:
        return _error("NotFound", f"Subscription does not exist: {sub_arn}", 404)

    sub = _find_subscription(topic_arn, sub_arn)
    if not sub:
        return _error("NotFound", f"Subscription does not exist: {sub_arn}", 404)

    attr_name = _p(params, "AttributeName")
    attr_val = _p(params, "AttributeValue")

    allowed = {"DeliveryPolicy", "FilterPolicy", "FilterPolicyScope",
               "RawMessageDelivery", "RedrivePolicy"}
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

def _publish(params):
    topic_arn = _normalize_arn(_p(params, "TopicArn") or _p(params, "TargetArn"))
    phone_number = _p(params, "PhoneNumber")
    message = _p(params, "Message")
    subject = _p(params, "Subject")
    message_structure = _p(params, "MessageStructure")

    if phone_number and not topic_arn:
        msg_id = new_uuid()
        logger.info("SNS SMS stub to %s: %s", phone_number, message[:80])
        return _xml(200, "PublishResponse",
                    f"<PublishResult><MessageId>{msg_id}</MessageId></PublishResult>")

    if not topic_arn:
        return _error("InvalidParameterException",
                      "TopicArn, TargetArn, or PhoneNumber is required", 400)

    if topic_arn not in _topics:
        return _error("NotFound", f"Topic does not exist: {topic_arn}", 404)

    topic = _topics[topic_arn]
    msg_attrs = _parse_message_attributes(params)
    fifo = _is_fifo_topic(topic)

    # ── FIFO validation, deduplication, and sequencing ──
    if fifo:
        group_id = _p(params, "MessageGroupId") or ""
        if not group_id:
            return _error(
                "InvalidParameterException",
                "Invalid parameter: The MessageGroupId parameter is required for FIFO topics",
                400,
            )

        # Resolve dedup ID: explicit > CBD SHA-256 > error
        try:
            dedup_id = _resolve_dedup_id(topic, params, message)
        except ValueError as exc:
            return _error("InvalidParameterException", str(exc), 400)

        # Prune expired cache entries, then check for duplicate
        with _fifo_lock:
            _prune_sns_dedup(topic)
            cached = topic.get("dedup_cache", {}).get(dedup_id)
            if cached:
                # Duplicate within the 5-minute window — return cached result
                return _xml(
                    200,
                    "PublishResponse",
                    f"<PublishResult>"
                    f"<MessageId>{cached['message_id']}</MessageId>"
                    f"<SequenceNumber>{cached['sequence_number']}</SequenceNumber>"
                    f"</PublishResult>",
                )

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

        topic["messages"].append({
            "id": msg_id,
            "message": message,
            "subject": subject,
            "message_structure": message_structure,
            "message_attributes": msg_attrs,
            "timestamp": int(time.time()),
        })

        _fanout(topic_arn, msg_id, message, subject, message_structure, msg_attrs,
                message_group_id=group_id, message_dedup_id=dedup_id)

        logger.info("SNS FIFO publish to %s: %s", topic_arn, message[:100])
        return _xml(
            200,
            "PublishResponse",
            f"<PublishResult>"
            f"<MessageId>{msg_id}</MessageId>"
            f"<SequenceNumber>{seq_number}</SequenceNumber>"
            f"</PublishResult>",
        )

    # ── Standard (non-FIFO) publish path ──
    msg_id = new_uuid()
    topic["messages"].append({
        "id": msg_id,
        "message": message,
        "subject": subject,
        "message_structure": message_structure,
        "message_attributes": msg_attrs,
        "timestamp": int(time.time()),
    })

    group_id = _p(params, "MessageGroupId") or ""
    dedup_id = _p(params, "MessageDeduplicationId") or ""
    _fanout(topic_arn, msg_id, message, subject, message_structure, msg_attrs,
            message_group_id=group_id, message_dedup_id=dedup_id)
    logger.info("SNS publish to %s: %s", topic_arn, message[:100])

    return _xml(200, "PublishResponse",
                f"<PublishResult><MessageId>{msg_id}</MessageId></PublishResult>")


def _publish_batch(params):
    topic_arn = _normalize_arn(_p(params, "TopicArn"))
    if not topic_arn:
        return _error("InvalidParameterException", "TopicArn is required", 400)
    if topic_arn not in _topics:
        return _error("NotFound", f"Topic does not exist: {topic_arn}", 404)

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

    topic = _topics[topic_arn]
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

            topic["messages"].append({
                "id": msg_id,
                "message": message,
                "subject": subject,
                "message_structure": message_structure,
                "message_attributes": msg_attrs,
                "timestamp": int(time.time()),
            })

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
            topic["messages"].append({
                "id": msg_id,
                "message": message,
                "subject": subject,
                "message_structure": message_structure,
                "message_attributes": msg_attrs,
                "timestamp": int(time.time()),
            })
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
    topic = _topics.get(topic_arn)
    if not topic:
        return

    for sub in topic["subscriptions"]:
        if not sub.get("confirmed"):
            continue

        protocol = sub.get("protocol", "")
        endpoint = sub.get("endpoint", "")

        if not _matches_filter_policy(sub, message_attributes or {}):
            continue

        effective_message = _resolve_message_for_protocol(
            message, message_structure, protocol
        )

        raw = sub.get("attributes", {}).get("RawMessageDelivery", "false") == "true"
        envelope = _build_envelope(
            topic_arn, msg_id, effective_message, subject,
            message_attributes or {}, raw
        )

        if protocol == "sqs":
            _deliver_to_sqs(endpoint, envelope, raw, effective_message,
                           message_group_id=message_group_id, message_dedup_id=message_dedup_id,
                           message_attributes=message_attributes or {})
        elif protocol in ("http", "https"):
            _threading.Thread(
                target=asyncio.run,
                args=(_deliver_to_http(endpoint, envelope),),
                daemon=True,
            ).start()
        elif protocol == "lambda":
            _deliver_to_lambda(endpoint, envelope, topic_arn, sub["arn"], msg_id, effective_message, message_attributes or {})
        elif protocol == "email" or protocol == "email-json":
            logger.info("SNS fanout → email %s (stub)", endpoint)
        elif protocol == "sms":
            logger.info("SNS fanout → SMS %s (stub)", endpoint)
        elif protocol == "application":
            logger.info("SNS fanout → application %s (stub)", endpoint)


def _deliver_to_sqs(endpoint: str, envelope: str, raw: bool, raw_message: str,
                    message_group_id: str = "", message_dedup_id: str = "",
                    message_attributes: dict | None = None):
    queue_name = endpoint.split(":")[-1]
    queue_url = _sqs._queue_url(queue_name)
    queue = _sqs._queues.get(queue_url)
    if not queue:
        logger.warning("SNS fanout: SQS queue %s not found", queue_name)
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
        "visible_at": now,
        "receive_count": 0,
    }
    if message_group_id:
        msg["group_id"] = message_group_id
    if message_dedup_id:
        msg["dedup_id"] = message_dedup_id
    _sqs._ensure_msg_fields(msg)
    queue["messages"].append(msg)
    logger.info("SNS fanout → SQS %s", queue_name)


def _deliver_to_lambda(endpoint: str, envelope: str, topic_arn: str, sub_arn: str,
                       msg_id: str, raw_message: str, message_attributes: dict):
    """Invoke a Lambda function with the SNS Records envelope (AWS format)."""
    # endpoint is a Lambda ARN: arn:aws:lambda:region:account:function:name
    func_name = endpoint.split(":")[-1]
    func = _lambda_svc._functions.get(func_name)
    if not func:
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
        _lambda_svc._execute_function(func, event)
        logger.info("SNS fanout → Lambda %s", func_name)
    except Exception as exc:
        logger.error("SNS fanout → Lambda %s failed: %s", func_name, exc)


def _http_post_sync(endpoint: str, payload: str, sns_message_type: str) -> int:
    """Blocking HTTP POST for SNS delivery. Runs on a worker thread so the
    event loop stays unblocked. Uses stdlib only — aiohttp was dropped because
    it isn't a declared dependency and wasn't shipped in the Docker image,
    which silently skipped every HTTP subscription confirmation (#460).

    Handles `http://user:pass@host/path` userinfo by stripping it from the URL
    and promoting to an Authorization: Basic header, matching real AWS SNS
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
    arn = _normalize_arn(_p(params, "ResourceArn"))
    topic = _topics.get(arn)
    tags_xml = ""
    if topic:
        for k, v in topic.get("tags", {}).items():
            tags_xml += f"<member><Key>{k}</Key><Value>{v}</Value></member>"
    return _xml(200, "ListTagsForResourceResponse",
                f"<ListTagsForResourceResult><Tags>{tags_xml}</Tags></ListTagsForResourceResult>")


def _tag_resource(params):
    arn = _normalize_arn(_p(params, "ResourceArn"))
    topic = _topics.get(arn)
    if not topic:
        return _error("ResourceNotFoundException", "Resource not found", 404)
    i = 1
    while _p(params, f"Tags.member.{i}.Key"):
        key = _p(params, f"Tags.member.{i}.Key")
        val = _p(params, f"Tags.member.{i}.Value")
        topic["tags"][key] = val
        i += 1
    return _xml(200, "TagResourceResponse", "<TagResourceResult/>")


def _untag_resource(params):
    arn = _normalize_arn(_p(params, "ResourceArn"))
    topic = _topics.get(arn)
    if topic:
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


def _create_platform_endpoint(params):
    app_arn = _p(params, "PlatformApplicationArn")
    token = _p(params, "Token")

    if app_arn not in _platform_applications:
        return _error("NotFound", f"PlatformApplication does not exist: {app_arn}", 404)
    if not token:
        return _error("InvalidParameterException", "Token is required", 400)

    endpoint_arn = f"{app_arn}/{new_uuid()}"

    attrs = {"Enabled": "true", "Token": token}
    i = 1
    while _p(params, f"Attributes.entry.{i}.key"):
        key = _p(params, f"Attributes.entry.{i}.key")
        val = _p(params, f"Attributes.entry.{i}.value")
        attrs[key] = val
        i += 1

    _platform_endpoints[endpoint_arn] = {
        "arn": endpoint_arn,
        "application_arn": app_arn,
        "attributes": attrs,
    }
    return _xml(200, "CreatePlatformEndpointResponse",
                f"<CreatePlatformEndpointResult>"
                f"<EndpointArn>{endpoint_arn}</EndpointArn>"
                f"</CreatePlatformEndpointResult>")


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
    topic = _topics.get(topic_arn)
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


def _matches_filter_policy(sub: dict, message_attributes: dict) -> bool:
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
        return True

    for key, allowed_values in policy.items():
        attr = message_attributes.get(key)
        if attr is None:
            return False
        attr_value = attr.get("StringValue", "")
        if not isinstance(allowed_values, list):
            allowed_values = [allowed_values]
        if not _attr_matches_any(attr_value, allowed_values):
            return False
    return True


def _attr_matches_any(attr_value: str, rules: list) -> bool:
    for rule in rules:
        if isinstance(rule, str):
            if attr_value == rule:
                return True
        elif isinstance(rule, (int, float)):
            try:
                if float(attr_value) == float(rule):
                    return True
            except (ValueError, TypeError):
                pass
        elif isinstance(rule, dict):
            if "exists" in rule:
                if rule["exists"] is True:
                    return True
                continue
            if "prefix" in rule:
                if attr_value.startswith(rule["prefix"]):
                    return True
            if "anything-but" in rule:
                excluded = rule["anything-but"]
                if isinstance(excluded, list):
                    if attr_value not in excluded:
                        return True
                elif attr_value != str(excluded):
                    return True
            if "numeric" in rule:
                try:
                    num = float(attr_value)
                    conditions = rule["numeric"]
                    if _check_numeric(num, conditions):
                        return True
                except (ValueError, TypeError):
                    pass
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
    _platform_applications.clear()
    _platform_endpoints.clear()
