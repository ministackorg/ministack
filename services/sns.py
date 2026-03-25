"""
SNS Service Emulator.
Supports: CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes,
          Subscribe, Unsubscribe, ListSubscriptions, ListSubscriptionsByTopic, Publish.
"""

import time
import hashlib
import logging
from urllib.parse import parse_qs

from core.responses import new_uuid

logger = logging.getLogger("sns")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_topics: dict = {}  # arn -> {name, attributes, subscriptions: [{arn, protocol, endpoint, confirmed}]}
_sub_arn_to_topic: dict = {}


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
        "Unsubscribe": _unsubscribe,
        "ListSubscriptions": _list_subscriptions,
        "ListSubscriptionsByTopic": _list_subscriptions_by_topic,
        "Publish": _publish,
    }

    handler = handlers.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown action: {action}", 400)
    return handler(params)


def _create_topic(params):
    name = _p(params, "Name")
    if not name:
        return _error("InvalidParameter", "Name is required", 400)
    arn = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:{name}"
    if arn not in _topics:
        _topics[arn] = {
            "name": name, "arn": arn,
            "attributes": {
                "TopicArn": arn,
                "DisplayName": name,
                "SubscriptionsConfirmed": "0",
                "SubscriptionsPending": "0",
                "SubscriptionsDeleted": "0",
            },
            "subscriptions": [],
            "messages": [],
        }
    return _xml(200, "CreateTopicResponse", f"<CreateTopicResult><TopicArn>{arn}</TopicArn></CreateTopicResult>")


def _delete_topic(params):
    arn = _p(params, "TopicArn")
    _topics.pop(arn, None)
    return _xml(200, "DeleteTopicResponse", "")


def _list_topics(params):
    members = "".join(f"<member><TopicArn>{arn}</TopicArn></member>" for arn in _topics)
    return _xml(200, "ListTopicsResponse", f"<ListTopicsResult><Topics>{members}</Topics></ListTopicsResult>")


def _get_topic_attributes(params):
    arn = _p(params, "TopicArn")
    topic = _topics.get(arn)
    if not topic:
        return _error("NotFound", "Topic not found", 404)
    attrs = "".join(f"<entry><key>{k}</key><value>{v}</value></entry>" for k, v in topic["attributes"].items())
    return _xml(200, "GetTopicAttributesResponse", f"<GetTopicAttributesResult><Attributes>{attrs}</Attributes></GetTopicAttributesResult>")


def _set_topic_attributes(params):
    arn = _p(params, "TopicArn")
    topic = _topics.get(arn)
    if not topic:
        return _error("NotFound", "Topic not found", 404)
    attr_name = _p(params, "AttributeName")
    attr_val = _p(params, "AttributeValue")
    topic["attributes"][attr_name] = attr_val
    return _xml(200, "SetTopicAttributesResponse", "")


def _subscribe(params):
    topic_arn = _p(params, "TopicArn")
    protocol = _p(params, "Protocol")
    endpoint = _p(params, "Endpoint")

    topic = _topics.get(topic_arn)
    if not topic:
        return _error("NotFound", "Topic not found", 404)

    sub_arn = f"{topic_arn}:{new_uuid()}"
    sub = {"arn": sub_arn, "protocol": protocol, "endpoint": endpoint, "confirmed": True, "topic_arn": topic_arn}
    topic["subscriptions"].append(sub)
    _sub_arn_to_topic[sub_arn] = topic_arn
    topic["attributes"]["SubscriptionsConfirmed"] = str(len([s for s in topic["subscriptions"] if s["confirmed"]]))

    return _xml(200, "SubscribeResponse", f"<SubscribeResult><SubscriptionArn>{sub_arn}</SubscriptionArn></SubscribeResult>")


def _unsubscribe(params):
    sub_arn = _p(params, "SubscriptionArn")
    topic_arn = _sub_arn_to_topic.get(sub_arn)
    if topic_arn and topic_arn in _topics:
        _topics[topic_arn]["subscriptions"] = [s for s in _topics[topic_arn]["subscriptions"] if s["arn"] != sub_arn]
    _sub_arn_to_topic.pop(sub_arn, None)
    return _xml(200, "UnsubscribeResponse", "")


def _list_subscriptions(params):
    members = ""
    for topic in _topics.values():
        for sub in topic["subscriptions"]:
            members += f"""<member>
                <SubscriptionArn>{sub['arn']}</SubscriptionArn>
                <TopicArn>{sub['topic_arn']}</TopicArn>
                <Protocol>{sub['protocol']}</Protocol>
                <Endpoint>{sub['endpoint']}</Endpoint>
            </member>"""
    return _xml(200, "ListSubscriptionsResponse", f"<ListSubscriptionsResult><Subscriptions>{members}</Subscriptions></ListSubscriptionsResult>")


def _list_subscriptions_by_topic(params):
    topic_arn = _p(params, "TopicArn")
    topic = _topics.get(topic_arn)
    if not topic:
        return _error("NotFound", "Topic not found", 404)
    members = ""
    for sub in topic["subscriptions"]:
        members += f"""<member>
            <SubscriptionArn>{sub['arn']}</SubscriptionArn>
            <TopicArn>{topic_arn}</TopicArn>
            <Protocol>{sub['protocol']}</Protocol>
            <Endpoint>{sub['endpoint']}</Endpoint>
        </member>"""
    return _xml(200, "ListSubscriptionsByTopicResponse", f"<ListSubscriptionsByTopicResult><Subscriptions>{members}</Subscriptions></ListSubscriptionsByTopicResult>")


def _publish(params):
    topic_arn = _p(params, "TopicArn") or _p(params, "TargetArn")
    message = _p(params, "Message")
    subject = _p(params, "Subject")

    msg_id = new_uuid()

    if topic_arn and topic_arn in _topics:
        _topics[topic_arn]["messages"].append({
            "id": msg_id, "message": message, "subject": subject,
            "timestamp": time.time(),
        })
        _fanout(topic_arn, msg_id, message, subject)
        logger.info(f"SNS publish to {topic_arn}: {message[:100]}")

    return _xml(200, "PublishResponse", f"<PublishResult><MessageId>{msg_id}</MessageId></PublishResult>")


def _fanout(topic_arn: str, msg_id: str, message: str, subject: str):
    """Deliver SNS message to all confirmed subscribers."""
    import json as _json
    from services import sqs as _sqs

    topic = _topics.get(topic_arn)
    if not topic:
        return

    # SNS wraps the message in an envelope when delivering to SQS
    envelope = _json.dumps({
        "Type": "Notification",
        "MessageId": msg_id,
        "TopicArn": topic_arn,
        "Subject": subject or "",
        "Message": message,
        "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "SignatureVersion": "1",
        "Signature": "FAKE",
        "SigningCertURL": "",
        "UnsubscribeURL": "",
    })

    for sub in topic["subscriptions"]:
        if not sub.get("confirmed"):
            continue
        protocol = sub.get("protocol", "")
        endpoint = sub.get("endpoint", "")

        if protocol == "sqs":
            # endpoint is the SQS queue ARN: arn:aws:sqs:region:account:queue-name
            queue_name = endpoint.split(":")[-1]
            queue_url = _sqs._queue_url(queue_name)
            queue = _sqs._queues.get(queue_url)
            if queue:
                import hashlib as _hashlib
                queue["messages"].append({
                    "id": new_uuid(),
                    "body": envelope,
                    "md5": _hashlib.md5(envelope.encode()).hexdigest(),
                    "receipt_handle": None,
                    "sent_at": time.time(),
                    "visible_at": time.time(),
                    "receive_count": 0,
                    "attributes": {},
                })
                logger.info(f"SNS fanout → SQS {queue_name}")
            else:
                logger.warning(f"SNS fanout: SQS queue {queue_name} not found")


def _p(params, key, default=""):
    val = params.get(key, [default])
    return val[0] if isinstance(val, list) else val


def _xml(status, root_tag, inner):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
    {inner}
    <ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
    <Error><Code>{code}</Code><Message>{message}</Message></Error>
    <RequestId>{new_uuid()}</RequestId>
</ErrorResponse>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body
