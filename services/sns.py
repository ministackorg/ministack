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
"""

import asyncio
import hashlib
import json
import logging
import time
from urllib.parse import parse_qs

from core.responses import new_uuid
from services import sqs as _sqs
import services.lambda_svc as _lambda_svc

logger = logging.getLogger("sns")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_topics: dict = {}
_sub_arn_to_topic: dict = {}
_platform_applications: dict = {}
_platform_endpoints: dict = {}


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
        return _error("InvalidParameter", "Name is required", 400)

    arn = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:{name}"
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
                "Condition": {"StringEquals": {"AWS:SourceOwner": ACCOUNT_ID}},
            }],
        })
        _topics[arn] = {
            "name": name,
            "arn": arn,
            "attributes": {
                "TopicArn": arn,
                "DisplayName": name,
                "Owner": ACCOUNT_ID,
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

        i = 1
        while _p(params, f"Attributes.entry.{i}.key"):
            key = _p(params, f"Attributes.entry.{i}.key")
            val = _p(params, f"Attributes.entry.{i}.value")
            _topics[arn]["attributes"][key] = val
            i += 1

    return _xml(200, "CreateTopicResponse",
                f"<CreateTopicResult><TopicArn>{arn}</TopicArn></CreateTopicResult>")


def _delete_topic(params):
    arn = _p(params, "TopicArn")
    topic = _topics.pop(arn, None)
    if topic:
        for sub in topic.get("subscriptions", []):
            _sub_arn_to_topic.pop(sub["arn"], None)
    return _xml(200, "DeleteTopicResponse", "")


def _list_topics(params):
    members = "".join(
        f"<member><TopicArn>{arn}</TopicArn></member>" for arn in _topics
    )
    return _xml(200, "ListTopicsResponse",
                f"<ListTopicsResult><Topics>{members}</Topics></ListTopicsResult>")


def _get_topic_attributes(params):
    arn = _p(params, "TopicArn")
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
    arn = _p(params, "TopicArn")
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
    topic_arn = _p(params, "TopicArn")
    protocol = _p(params, "Protocol")
    endpoint = _p(params, "Endpoint")

    topic = _topics.get(topic_arn)
    if not topic:
        return _error("NotFound", f"Topic does not exist: {topic_arn}", 404)

    if not protocol:
        return _error("InvalidParameter", "Protocol is required", 400)

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
        "owner": ACCOUNT_ID,
        "token": new_uuid() if needs_confirmation else None,
        "attributes": {
            "SubscriptionArn": sub_arn,
            "TopicArn": topic_arn,
            "Protocol": protocol,
            "Endpoint": endpoint,
            "Owner": ACCOUNT_ID,
            "ConfirmationWasAuthenticated": "true" if not needs_confirmation else "false",
            "PendingConfirmation": "true" if needs_confirmation else "false",
            "RawMessageDelivery": "false",
        },
    }

    topic["subscriptions"].append(sub)
    _sub_arn_to_topic[sub_arn] = topic_arn
    _refresh_subscription_counts(topic)

    if needs_confirmation:
        asyncio.ensure_future(_send_subscription_confirmation(topic_arn, sub))

    result_arn = "PendingConfirmation" if needs_confirmation else sub_arn
    return _xml(200, "SubscribeResponse",
                f"<SubscribeResult><SubscriptionArn>{result_arn}</SubscriptionArn></SubscribeResult>")


def _confirm_subscription(params):
    topic_arn = _p(params, "TopicArn")
    token = _p(params, "Token")

    topic = _topics.get(topic_arn)
    if not topic:
        return _error("NotFound", f"Topic does not exist: {topic_arn}", 404)

    if not token:
        return _error("InvalidParameter", "Token is required", 400)

    for sub in topic["subscriptions"]:
        if sub.get("token") == token:
            sub["confirmed"] = True
            sub["token"] = None
            sub["attributes"]["PendingConfirmation"] = "false"
            sub["attributes"]["ConfirmationWasAuthenticated"] = "true"
            _refresh_subscription_counts(topic)
            return _xml(200, "ConfirmSubscriptionResponse",
                        f"<ConfirmSubscriptionResult><SubscriptionArn>{sub['arn']}</SubscriptionArn></ConfirmSubscriptionResult>")

    return _error("InvalidParameter", "Invalid token", 400)


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
    members = ""
    for topic in _topics.values():
        for sub in topic["subscriptions"]:
            members += (
                "<member>"
                f"<SubscriptionArn>{sub['arn']}</SubscriptionArn>"
                f"<Owner>{sub.get('owner', ACCOUNT_ID)}</Owner>"
                f"<TopicArn>{sub['topic_arn']}</TopicArn>"
                f"<Protocol>{sub['protocol']}</Protocol>"
                f"<Endpoint>{sub['endpoint']}</Endpoint>"
                "</member>"
            )
    return _xml(200, "ListSubscriptionsResponse",
                f"<ListSubscriptionsResult><Subscriptions>{members}</Subscriptions></ListSubscriptionsResult>")


def _list_subscriptions_by_topic(params):
    topic_arn = _p(params, "TopicArn")
    topic = _topics.get(topic_arn)
    if not topic:
        return _error("NotFound", f"Topic does not exist: {topic_arn}", 404)
    members = ""
    for sub in topic["subscriptions"]:
        members += (
            "<member>"
            f"<SubscriptionArn>{sub['arn']}</SubscriptionArn>"
            f"<Owner>{sub.get('owner', ACCOUNT_ID)}</Owner>"
            f"<TopicArn>{topic_arn}</TopicArn>"
            f"<Protocol>{sub['protocol']}</Protocol>"
            f"<Endpoint>{sub['endpoint']}</Endpoint>"
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
        return _error("InvalidParameter",
                      f"Invalid attribute name: {attr_name}", 400)

    if attr_name == "FilterPolicy" and attr_val:
        try:
            json.loads(attr_val)
        except json.JSONDecodeError:
            return _error("InvalidParameter", "Invalid JSON in FilterPolicy", 400)

    sub["attributes"][attr_name] = attr_val
    return _xml(200, "SetSubscriptionAttributesResponse", "")


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------

def _publish(params):
    topic_arn = _p(params, "TopicArn") or _p(params, "TargetArn")
    phone_number = _p(params, "PhoneNumber")
    message = _p(params, "Message")
    subject = _p(params, "Subject")
    message_structure = _p(params, "MessageStructure")

    if phone_number and not topic_arn:
        msg_id = new_uuid()
        logger.info(f"SNS SMS stub to {phone_number}: {message[:80]}")
        return _xml(200, "PublishResponse",
                    f"<PublishResult><MessageId>{msg_id}</MessageId></PublishResult>")

    if not topic_arn:
        return _error("InvalidParameter",
                      "TopicArn, TargetArn, or PhoneNumber is required", 400)

    if topic_arn not in _topics:
        return _error("NotFound", f"Topic does not exist: {topic_arn}", 404)

    msg_attrs = _parse_message_attributes(params)
    msg_id = new_uuid()

    _topics[topic_arn]["messages"].append({
        "id": msg_id,
        "message": message,
        "subject": subject,
        "message_structure": message_structure,
        "message_attributes": msg_attrs,
        "timestamp": time.time(),
    })

    _fanout(topic_arn, msg_id, message, subject, message_structure, msg_attrs)
    logger.info(f"SNS publish to {topic_arn}: {message[:100]}")

    return _xml(200, "PublishResponse",
                f"<PublishResult><MessageId>{msg_id}</MessageId></PublishResult>")


def _publish_batch(params):
    topic_arn = _p(params, "TopicArn")
    if not topic_arn:
        return _error("InvalidParameter", "TopicArn is required", 400)
    if topic_arn not in _topics:
        return _error("NotFound", f"Topic does not exist: {topic_arn}", 404)

    entries = _parse_batch_entries(params)
    if not entries:
        return _error("InvalidParameter",
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

    successful = ""
    failed = ""
    for entry in entries:
        eid = entry["id"]
        message = entry.get("message", "")
        subject = entry.get("subject", "")
        message_structure = entry.get("message_structure", "")
        msg_attrs = entry.get("message_attributes", {})

        msg_id = new_uuid()
        _topics[topic_arn]["messages"].append({
            "id": msg_id,
            "message": message,
            "subject": subject,
            "message_structure": message_structure,
            "message_attributes": msg_attrs,
            "timestamp": time.time(),
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
            message_structure: str = "", message_attributes: dict | None = None):
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
            _deliver_to_sqs(endpoint, envelope, raw, effective_message)
        elif protocol in ("http", "https"):
            asyncio.ensure_future(
                _deliver_to_http(endpoint, envelope)
            )
        elif protocol == "lambda":
            _deliver_to_lambda(endpoint, envelope, topic_arn, sub["arn"], msg_id, effective_message, message_attributes or {})
        elif protocol == "email" or protocol == "email-json":
            logger.info(f"SNS fanout → email {endpoint} (stub)")
        elif protocol == "sms":
            logger.info(f"SNS fanout → SMS {endpoint} (stub)")
        elif protocol == "application":
            logger.info(f"SNS fanout → application {endpoint} (stub)")


def _deliver_to_sqs(endpoint: str, envelope: str, raw: bool, raw_message: str):
    queue_name = endpoint.split(":")[-1]
    queue_url = _sqs._queue_url(queue_name)
    queue = _sqs._queues.get(queue_url)
    if not queue:
        logger.warning(f"SNS fanout: SQS queue {queue_name} not found")
        return

    body = raw_message if raw else envelope
    now = time.time()
    msg = {
        "id": new_uuid(),
        "body": body,
        "md5": hashlib.md5(body.encode()).hexdigest(),
        "receipt_handle": None,
        "sent_at": now,
        "visible_at": now,
        "receive_count": 0,
    }
    _sqs._ensure_msg_fields(msg)
    queue["messages"].append(msg)
    logger.info(f"SNS fanout → SQS {queue_name}")


def _deliver_to_lambda(endpoint: str, envelope: str, topic_arn: str, sub_arn: str,
                       msg_id: str, raw_message: str, message_attributes: dict):
    """Invoke a Lambda function with the SNS Records envelope (AWS format)."""
    # endpoint is a Lambda ARN: arn:aws:lambda:region:account:function:name
    func_name = endpoint.split(":")[-1]
    func = _lambda_svc._functions.get(func_name)
    if not func:
        logger.warning(f"SNS fanout: Lambda function {func_name} not found")
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
        logger.info(f"SNS fanout → Lambda {func_name}")
    except Exception as exc:
        logger.error(f"SNS fanout → Lambda {func_name} failed: {exc}")


async def _deliver_to_http(endpoint: str, payload: str):
    try:
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                endpoint,
                data=payload,
                headers={
                    "Content-Type": "text/plain; charset=UTF-8",
                    "x-amz-sns-message-type": "Notification",
                },
            ) as resp:
                logger.info(f"SNS HTTP delivery to {endpoint}: {resp.status}")
    except ImportError:
        logger.warning("aiohttp not installed — HTTP delivery skipped")
    except Exception as exc:
        logger.warning(f"SNS HTTP delivery to {endpoint} failed: {exc}")


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
        "SubscribeURL": f"http://localhost:4566/?Action=ConfirmSubscription&TopicArn={topic_arn}&Token={token}",
        "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "SignatureVersion": "1",
        "Signature": "FAKE",
        "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-fake.pem",
    })
    try:
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                endpoint,
                data=payload,
                headers={
                    "Content-Type": "text/plain; charset=UTF-8",
                    "x-amz-sns-message-type": "SubscriptionConfirmation",
                },
            ) as resp:
                logger.info(f"SNS SubscriptionConfirmation sent to {endpoint}: {resp.status}")
    except ImportError:
        logger.info(f"aiohttp not installed — subscription confirmation for {endpoint} skipped")
    except Exception as exc:
        logger.warning(f"SNS SubscriptionConfirmation to {endpoint} failed: {exc}")


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _list_tags_for_resource(params):
    arn = _p(params, "ResourceArn")
    topic = _topics.get(arn)
    tags_xml = ""
    if topic:
        for k, v in topic.get("tags", {}).items():
            tags_xml += f"<member><Key>{k}</Key><Value>{v}</Value></member>"
    return _xml(200, "ListTagsForResourceResponse",
                f"<ListTagsForResourceResult><Tags>{tags_xml}</Tags></ListTagsForResourceResult>")


def _tag_resource(params):
    arn = _p(params, "ResourceArn")
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
    arn = _p(params, "ResourceArn")
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
        return _error("InvalidParameter", "Name and Platform are required", 400)

    arn = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:app/{platform}/{name}"
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
        return _error("InvalidParameter", "Token is required", 400)

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
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">'
        f'<Error><Code>{code}</Code><Message>{_xml_escape(message)}</Message></Error>'
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
        "UnsubscribeURL": f"http://localhost:4566/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:{REGION}:{ACCOUNT_ID}:example",
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
