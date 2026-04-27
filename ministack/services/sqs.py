"""
SQS Service Emulator — Full AWS-compatible implementation.

Supports both legacy Query API and modern JSON API (X-Amz-Target: AmazonSQS.*).

Features:
  - Standard and FIFO queues (.fifo suffix, MessageGroupId, MessageDeduplicationId)
  - Dead-letter queues (RedrivePolicy with maxReceiveCount & deadLetterTargetArn)
  - Long polling (WaitTimeSeconds)
  - Message user attributes and system attributes
  - ChangeMessageVisibilityBatch
  - Proper FIFO deduplication (5-minute window)
  - FIFO message-group ordering (one in-flight message per group)
  - ApproximateNumberOfMessagesDelayed tracking
  - QueueArn in GetQueueAttributes

Actions:
  CreateQueue, DeleteQueue, ListQueues, GetQueueUrl, GetQueueAttributes,
  SetQueueAttributes, PurgeQueue, SendMessage, ReceiveMessage, DeleteMessage,
  ChangeMessageVisibility, ChangeMessageVisibilityBatch, SendMessageBatch,
  DeleteMessageBatch, ListQueueTags, TagQueue, UntagQueue.
"""

import asyncio
import base64
import copy
import hashlib
import json
import logging
import os
import struct
import threading
import time
from urllib.parse import parse_qs
from xml.sax.saxutils import escape as _esc

from ministack.core.persistence import load_state, PERSIST_STATE
from ministack.core.responses import AccountScopedDict, get_account_id, md5_hash, new_uuid, now_iso, get_region

logger = logging.getLogger("sqs")

# ── Module-level state ──────────────────────────────────────

_queues = AccountScopedDict()
_queue_name_to_url = AccountScopedDict()
_queues_lock = threading.Lock()


# ── Persistence ────────────────────────────────────────────

def get_state():
    # Both must be deepcopy(asd): dict(asd) iterates only the current
    # request's tenant via AccountScopedDict.__iter__, so other tenants'
    # name→url mappings would silently disappear at shutdown
    # serialisation. Same bug family as #492.
    return {
        "queues": copy.deepcopy(_queues),
        "queue_name_to_url": copy.deepcopy(_queue_name_to_url),
    }


def restore_state(data):
    if data:
        _queues.update(data.get("queues", {}))
        _queue_name_to_url.update(data.get("queue_name_to_url", {}))


try:
    _restored = load_state("sqs")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
DEFAULT_HOST = os.environ.get("MINISTACK_HOST", "localhost")
DEFAULT_PORT = os.environ.get("GATEWAY_PORT", "4566")
_DEDUP_WINDOW_S = 300  # 5 minutes


# ── Exceptions ──────────────────────────────────────────────

class _Err(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status
        super().__init__(message)


# ── Queue URL ───────────────────────────────────────────────

def _queue_url(name: str) -> str:
    return f"http://{DEFAULT_HOST}:{DEFAULT_PORT}/{get_account_id()}/{name}"


# ────────────────────────────────────────────────────────────
#  ENTRY POINT
# ────────────────────────────────────────────────────────────

async def handle_request(method: str, path: str, headers: dict,
                         body: bytes, query_params: dict) -> tuple:
    """Handle SQS requests — supports both legacy Query API and modern JSON API."""
    target = headers.get("x-amz-target", "")

    # JSON protocol  (X-Amz-Target: AmazonSQS.*)
    if target.startswith("AmazonSQS."):
        action = target.split(".")[-1]
        try:
            data = json.loads(body) if body else {}
        except Exception:
            data = {}
        return await _handle_json(action, data, path)

    # Legacy Query / form-encoded protocol
    params = dict(query_params)
    if method == "POST" and body:
        for k, v in parse_qs(body.decode("utf-8", errors="replace")).items():
            params[k] = v

    action = _p(params, "Action")
    if not action:
        return _xml_err_resp("MissingAction", "Missing Action parameter", 400)
    return await _handle_query(action, params, path)


# ── JSON protocol layer ────────────────────────────────────

async def _handle_json(action: str, data: dict, path: str) -> tuple:
    try:
        qurl = data.get("QueueUrl", "") or _url_from_path(path)
        result = await _dispatch(action, data, qurl)
        return _json_ok(result)
    except _Err as e:
        return _json_err_resp(e.code, e.message, e.status)


# ── Query / XML protocol layer ─────────────────────────────

async def _handle_query(action: str, params: dict, path: str) -> tuple:
    try:
        data = _normalise(action, params)
        qurl = data.get("QueueUrl", "") or _url_from_path(path)
        data["QueueUrl"] = qurl
        result = await _dispatch(action, data, qurl)
        return _to_xml(action, result)
    except _Err as e:
        return _xml_err_resp(e.code, e.message, e.status)


# ── Dispatcher ──────────────────────────────────────────────

_HANDLERS: dict = {}

async def _dispatch(action: str, data: dict, qurl: str) -> dict:
    fn = _HANDLERS.get(action)
    if fn is None:
        raise _Err("InvalidAction",
                    f"The action {action} is not valid for this endpoint.")
    result = fn(data, qurl)
    if asyncio.iscoroutine(result):
        result = await result
    return result


# ────────────────────────────────────────────────────────────
#  CORE ACTIONS
# ────────────────────────────────────────────────────────────

def _act_create_queue(data: dict, _u: str) -> dict:
    name = data.get("QueueName", "")
    if not name:
        raise _Err("MissingParameter",
                    "The request must contain the parameter QueueName.")

    attrs = data.get("Attributes") or {}
    is_fifo = name.endswith(".fifo") or attrs.get("FifoQueue") == "true"

    if is_fifo and not name.endswith(".fifo"):
        raise _Err("InvalidParameterValue",
                    "The name of a FIFO queue can only include alphanumeric "
                    "characters, hyphens, or underscores, must end with .fifo suffix.")
    if name.endswith(".fifo"):
        is_fifo = True

    url = _queue_url(name)
    if url in _queues:
        # If attrs differ from existing queue, return error
        if attrs:
            existing = _queues[url]["attributes"]
            for k, v in attrs.items():
                if k in existing and existing[k] != v:
                    raise _Err("QueueNameExists",
                               "A queue already exists with the same name and a different value for attribute " + k)
        return {"QueueUrl": url}

    ts = str(int(time.time()))
    q: dict = {
        "name": name, "url": url, "is_fifo": is_fifo,
        "attributes": {
            "QueueArn": f"arn:aws:sqs:{get_region()}:{get_account_id()}:{name}",
            "CreatedTimestamp": ts,
            "LastModifiedTimestamp": ts,
            "VisibilityTimeout": "30",
            "MaximumMessageSize": "262144",
            "MessageRetentionPeriod": "345600",
            "DelaySeconds": "0",
            "ReceiveMessageWaitTimeSeconds": "0",
        },
        "messages": [],
        "tags": {},
        "dedup_cache": {},
        "fifo_seq": 0,
    }
    if is_fifo:
        q["attributes"]["FifoQueue"] = "true"
        q["attributes"]["ContentBasedDeduplication"] = \
            attrs.get("ContentBasedDeduplication", "false")

    for k, v in attrs.items():
        q["attributes"][k] = str(v)

    # Apply tags passed at creation time
    create_tags = data.get("Tags") or data.get("tags") or {}
    if create_tags:
        q["tags"].update(create_tags)

    _queues[url] = q
    _queue_name_to_url[name] = url
    return {"QueueUrl": url}


def _act_delete_queue(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    if url in _queues:
        _queue_name_to_url.pop(_queues[url]["name"], None)
        del _queues[url]
    return {}


def _act_list_queues(data: dict, _u: str) -> dict:
    pfx = data.get("QueueNamePrefix", "")
    mx = int(data.get("MaxResults", 1000))
    urls = [u for u, q in _queues.items()
            if not pfx or q["name"].startswith(pfx)]
    return {"QueueUrls": urls[:mx]}


def _act_get_queue_url(data: dict, _u: str) -> dict:
    name = data.get("QueueName", "")
    url = _queue_name_to_url.get(name)
    if not url:
        raise _Err("QueueDoesNotExist",
                    "The specified queue does not exist.")
    return {"QueueUrl": url}


# ── SendMessage ─────────────────────────────────────────────

def _act_send_message(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)

    body_text = data.get("MessageBody", "")
    if not body_text:
        raise _Err("MissingParameter",
                    "The request must contain the parameter MessageBody.")

    delay = int(data.get("DelaySeconds")
                or q["attributes"].get("DelaySeconds", "0"))
    msg_attrs = data.get("MessageAttributes") or {}
    sys_attrs = data.get("MessageSystemAttributes") or {}
    group_id = data.get("MessageGroupId")
    dedup_id = data.get("MessageDeduplicationId")
    dedup_cache_key = None
    seq = None

    # FIFO-specific validation & dedup
    if q["is_fifo"]:
        if not group_id:
            raise _Err("MissingParameter",
                        "The request must contain the parameter MessageGroupId.")
        if not dedup_id:
            if q["attributes"].get("ContentBasedDeduplication") == "true":
                dedup_id = hashlib.sha256(body_text.encode()).hexdigest()
            else:
                raise _Err("InvalidParameterValue",
                            "The queue should either have ContentBasedDeduplication "
                            "enabled or MessageDeduplicationId provided explicitly.")
        # When DeduplicationScope=messageGroup the dedup window is per message
        # group; two messages with the same body but different group IDs are
        # distinct and must both be enqueued.  Scope the cache key accordingly.
        dedup_scope = q["attributes"].get("DeduplicationScope", "queue")
        dedup_cache_key = (
            f"{group_id}:{dedup_id}" if dedup_scope == "messageGroup" else dedup_id
        )
        _prune_dedup(q)
        cached = q["dedup_cache"].get(dedup_cache_key)
        if cached:
            r: dict = {"MessageId": cached["id"],
                       "MD5OfMessageBody": cached["md5"]}
            if cached.get("md5a"):
                r["MD5OfMessageAttributes"] = cached["md5a"]
            if cached.get("seq"):
                r["SequenceNumber"] = cached["seq"]
            return r
        q["fifo_seq"] += 1
        seq = str(q["fifo_seq"]).zfill(20)
        delay = 0

    now = time.time()
    mid = new_uuid()
    md5b = hashlib.md5(body_text.encode()).hexdigest()
    md5a = _md5_msg_attrs(msg_attrs)

    msg: dict = {
        "id": mid,
        "body": body_text,
        "md5_body": md5b,
        "md5_attrs": md5a,
        "receipt_handle": None,
        "sent_at": now,
        "visible_at": now + delay,
        "receive_count": 0,
        "first_receive_at": None,
        "message_attributes": msg_attrs,
        "sys": {
            "SenderId": get_account_id(),
            "SentTimestamp": str(int(now * 1000)),
        },
        "group_id": group_id,
        "dedup_id": dedup_id,
        "dedup_cache_key": dedup_cache_key if q["is_fifo"] else None,
        "seq": seq,
    }
    q["messages"].append(msg)

    if q["is_fifo"] and dedup_id:
        q["dedup_cache"][dedup_cache_key] = {
            "expire": now + _DEDUP_WINDOW_S,
            "id": mid, "md5": md5b, "md5a": md5a, "seq": seq,
        }

    result: dict = {"MessageId": mid, "MD5OfMessageBody": md5b}
    if md5a:
        result["MD5OfMessageAttributes"] = md5a
    if seq:
        result["SequenceNumber"] = seq
    return result


# ── ReceiveMessage (async — long polling) ───────────────────

async def _act_receive_message(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)

    max_n = min(int(data.get("MaxNumberOfMessages", 1)), 10)
    vis = int(data.get("VisibilityTimeout")
              or q["attributes"].get("VisibilityTimeout", "30"))
    wait = int(data.get("WaitTimeSeconds")
               or q["attributes"].get("ReceiveMessageWaitTimeSeconds", "0"))

    attr_names = (data.get("AttributeNames")
                  or data.get("SystemAttributeNames") or [])
    msg_attr_names = data.get("MessageAttributeNames") or []

    deadline = time.time() + wait
    msgs: list = []

    while True:
        _dlq_sweep(q)
        msgs = _collect_msgs(q, max_n, vis)
        if msgs or time.time() >= deadline:
            break
        await asyncio.sleep(min(0.1, max(0.01, deadline - time.time())))

    out: list = []
    for m in msgs:
        entry: dict = {
            "MessageId": m["id"],
            "ReceiptHandle": m["receipt_handle"],
            "MD5OfBody": m["md5_body"],
            "Body": m["body"],
        }
        sa = _build_sys_attrs(m, attr_names)
        if sa:
            entry["Attributes"] = sa
        fa = _filter_msg_attrs(m["message_attributes"], msg_attr_names)
        if fa:
            entry["MessageAttributes"] = fa
            if m["md5_attrs"]:
                entry["MD5OfMessageAttributes"] = m["md5_attrs"]
        out.append(entry)

    return {"Messages": out} if out else {}


# ── DeleteMessage ───────────────────────────────────────────

def _act_delete_message(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)
    rh = data.get("ReceiptHandle", "")
    if not rh:
        raise _Err("MissingParameter",
                    "The request must contain the parameter ReceiptHandle.")
    # Only remove messages whose receipt_handle is set and matches.
    # Messages that have never been received (receipt_handle is None) are never
    # accidentally removed by an empty or unrelated receipt handle.
    found = False
    kept = []
    for m in q["messages"]:
        if m["receipt_handle"] is not None and m["receipt_handle"] == rh:
            found = True
            # Clear the FIFO dedup cache entry so the same dedup ID can be
            # reused immediately after deletion.  Real AWS keeps a strict
            # 5-minute window, but clearing on delete is more practical for
            # local development where tests re-run with fixed dedup IDs.
            if q["is_fifo"] and m.get("dedup_id"):
                # Use the stored cache key (which may be group-scoped) to
                # clear the correct dedup entry.
                cache_key = m.get("dedup_cache_key") or m["dedup_id"]
                q["dedup_cache"].pop(cache_key, None)
        else:
            kept.append(m)
    if not found:
        raise _Err("ReceiptHandleIsInvalid", "The input receipt handle is invalid.")
    q["messages"] = kept
    return {}


# ── ChangeMessageVisibility ────────────────────────────────

def _act_change_visibility(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)
    rh = data.get("ReceiptHandle", "")
    vt = int(data.get("VisibilityTimeout", 30))
    found = False
    for m in q["messages"]:
        if m["receipt_handle"] is not None and m["receipt_handle"] == rh:
            m["visible_at"] = time.time() + vt
            found = True
            break
    if not found:
        raise _Err("ReceiptHandleIsInvalid", "The input receipt handle is invalid.")
    return {}


# ── ChangeMessageVisibilityBatch ───────────────────────────

def _act_change_visibility_batch(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)
    ok: list = []
    fail: list = []
    for e in data.get("Entries", []):
        eid = e.get("Id", "")
        rh = e.get("ReceiptHandle", "")
        vt = int(e.get("VisibilityTimeout", 30))
        found = False
        for m in q["messages"]:
            if m["receipt_handle"] is not None and m["receipt_handle"] == rh:
                m["visible_at"] = time.time() + vt
                found = True
                break
        if found:
            ok.append({"Id": eid})
        else:
            fail.append({
                "Id": eid,
                "Code": "ReceiptHandleIsInvalid",
                "Message": "The input receipt handle is invalid.",
                "SenderFault": True,
            })
    return {"Successful": ok, "Failed": fail}


# ── GetQueueAttributes ─────────────────────────────────────

def _act_get_queue_attributes(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)
    _refresh_counts(q)
    names = data.get("AttributeNames") or ["All"]
    want_all = "All" in names
    out: dict = {}
    for k, v in q["attributes"].items():
        if want_all or k in names:
            out[k] = v
    return {"Attributes": out}


# ── SetQueueAttributes ─────────────────────────────────────

def _act_set_queue_attributes(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)
    for k, v in (data.get("Attributes") or {}).items():
        q["attributes"][k] = str(v)
    q["attributes"]["LastModifiedTimestamp"] = str(int(time.time()))
    return {}


# ── PurgeQueue ──────────────────────────────────────────────

def _act_purge_queue(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)
    q["messages"].clear()
    return {}


# ── SendMessageBatch ───────────────────────────────────────

def _act_send_message_batch(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    _get_q(url)
    entries = data.get("Entries", [])
    if len(entries) > 10:
        raise _Err("TooManyEntriesInBatchRequest",
                   "Too many messages in a batch request. A maximum of 10 messages are allowed.")
    ok: list = []
    fail: list = []
    for e in entries:
        try:
            sub: dict = {
                "QueueUrl": url,
                "MessageBody": e.get("MessageBody", ""),
                "DelaySeconds": e.get("DelaySeconds"),
                "MessageAttributes": e.get("MessageAttributes"),
                "MessageGroupId": e.get("MessageGroupId"),
                "MessageDeduplicationId": e.get("MessageDeduplicationId"),
            }
            r = _act_send_message(sub, url)
            r["Id"] = e.get("Id", "")
            ok.append(r)
        except _Err as ex:
            fail.append({"Id": e.get("Id", ""), "Code": ex.code,
                         "Message": ex.message, "SenderFault": True})
    return {"Successful": ok, "Failed": fail}


# ── DeleteMessageBatch ─────────────────────────────────────

def _act_delete_message_batch(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)
    ok: list = []
    fail: list = []
    for e in data.get("Entries", []):
        eid = e.get("Id", "")
        rh = e.get("ReceiptHandle", "")
        before = len(q["messages"])
        kept = []
        for m in q["messages"]:
            if m["receipt_handle"] is not None and m["receipt_handle"] == rh:
                if q["is_fifo"] and m.get("dedup_id"):
                    q["dedup_cache"].pop(m["dedup_id"], None)
            else:
                kept.append(m)
        q["messages"] = kept
        if len(q["messages"]) < before:
            ok.append({"Id": eid})
        else:
            fail.append({
                "Id": eid,
                "Code": "ReceiptHandleIsInvalid",
                "Message": "The input receipt handle is invalid.",
                "SenderFault": True,
            })
    return {"Successful": ok, "Failed": fail}


# ── Tags ────────────────────────────────────────────────────

def _act_list_queue_tags(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)
    return {"Tags": dict(q.get("tags", {}))}


def _act_tag_queue(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)
    q.setdefault("tags", {}).update(data.get("Tags") or {})
    return {}


def _act_untag_queue(data: dict, qurl: str) -> dict:
    url = data.get("QueueUrl", qurl)
    q = _get_q(url)
    for k in data.get("TagKeys", []):
        q.get("tags", {}).pop(k, None)
    return {}


# ── Register handlers ──────────────────────────────────────

_HANDLERS.update({
    "CreateQueue":                  _act_create_queue,
    "DeleteQueue":                  _act_delete_queue,
    "ListQueues":                   _act_list_queues,
    "GetQueueUrl":                  _act_get_queue_url,
    "SendMessage":                  _act_send_message,
    "ReceiveMessage":               _act_receive_message,
    "DeleteMessage":                _act_delete_message,
    "ChangeMessageVisibility":      _act_change_visibility,
    "ChangeMessageVisibilityBatch": _act_change_visibility_batch,
    "GetQueueAttributes":           _act_get_queue_attributes,
    "SetQueueAttributes":           _act_set_queue_attributes,
    "PurgeQueue":                   _act_purge_queue,
    "SendMessageBatch":             _act_send_message_batch,
    "DeleteMessageBatch":           _act_delete_message_batch,
    "ListQueueTags":                _act_list_queue_tags,
    "TagQueue":                     _act_tag_queue,
    "UntagQueue":                   _act_untag_queue,
})


# ────────────────────────────────────────────────────────────
#  QUEUE / MESSAGE HELPERS
# ────────────────────────────────────────────────────────────

def _get_q(url: str) -> dict:
    q = _queues.get(url)
    if q is None:
        # Fallback: extract queue name from URL and look up by name.
        # This handles cases where the hostname differs (e.g. docker-compose
        # service name "ministack" vs "localhost"), or when a bare queue name
        # is passed instead of a full URL (supported by AWS and some SDKs).
        parts = url.rstrip("/").split("/")
        name = parts[-1] if len(parts) >= 2 else url
        canonical_url = _queue_name_to_url.get(name)
        if canonical_url:
            q = _queues.get(canonical_url)
    if q is None:
        raise _Err("QueueDoesNotExist",
                    "The specified queue does not exist for this wsdl version.")
    return q


def _ensure_msg_fields(m: dict) -> None:
    """Ensure internal message shape for ReceiveMessage (SNS fan-out etc.)."""
    if "md5_body" not in m and m.get("md5"):
        m["md5_body"] = m["md5"]
    if "md5_body" not in m:
        body = m.get("body") or ""
        if not isinstance(body, str):
            body = str(body)
        m["md5_body"] = hashlib.md5(body.encode()).hexdigest()
    m.setdefault("message_attributes", {})
    m.setdefault("md5_attrs", None)
    m.setdefault("first_receive_at", None)
    m.setdefault("receive_count", 0)
    m.setdefault("receipt_handle", None)
    m.setdefault("sent_at", time.time())
    m.setdefault("visible_at", m["sent_at"])
    if "sys" not in m:
        sent = m["sent_at"]
        m["sys"] = {
            "SenderId": get_account_id(),
            "SentTimestamp": str(int(sent * 1000)),
        }
    m.setdefault("group_id", None)
    m.setdefault("dedup_id", None)
    m.setdefault("seq", None)


def _refresh_counts(q: dict) -> None:
    """Recompute approximate message counters."""
    now = time.time()
    visible = delayed = inflight = 0
    for m in q["messages"]:
        if m["visible_at"] <= now:
            visible += 1
        elif m["receive_count"] == 0:
            delayed += 1
        else:
            inflight += 1
    q["attributes"]["ApproximateNumberOfMessages"] = str(visible)
    q["attributes"]["ApproximateNumberOfMessagesNotVisible"] = str(inflight)
    q["attributes"]["ApproximateNumberOfMessagesDelayed"] = str(delayed)


# ── Collect visible messages for ReceiveMessage ────────────

def _collect_msgs(q: dict, max_n: int, vis_timeout: int) -> list:
    now = time.time()
    if q["is_fifo"]:
        return _collect_fifo(q, max_n, vis_timeout, now)
    for m in q["messages"]:
        _ensure_msg_fields(m)
    visible = [m for m in q["messages"] if m["visible_at"] <= now]
    result = visible[:max_n]
    for m in result:
        m["receipt_handle"] = new_uuid()
        m["visible_at"] = now + vis_timeout
        m["receive_count"] += 1
        if m.get("first_receive_at") is None:
            m["first_receive_at"] = now
    return result


def _collect_fifo(q: dict, max_n: int, vis_timeout: int,
                  now: float) -> list:
    """FIFO queues: respect message-group ordering.

    Only one message per group can be in-flight at a time.  Messages within
    a group are delivered in send order.
    """
    for m in q["messages"]:
        _ensure_msg_fields(m)
    inflight_groups: set = {
        m["group_id"] for m in q["messages"]
        if m["visible_at"] > now
        and m["receive_count"] > 0
        and m["group_id"]
    }
    result: list = []
    for m in q["messages"]:
        if len(result) >= max_n:
            break
        if m["visible_at"] > now:
            continue
        g = m["group_id"]
        if g in inflight_groups:
            continue
        m["receipt_handle"] = new_uuid()
        m["visible_at"] = now + vis_timeout
        m["receive_count"] += 1
        if m.get("first_receive_at") is None:
            m["first_receive_at"] = now
        result.append(m)
    return result


# ── Dead-letter queue sweep ────────────────────────────────

def _dlq_sweep(q: dict) -> None:
    """Move messages that have reached maxReceiveCount to the DLQ."""
    rp_raw = q["attributes"].get("RedrivePolicy")
    if not rp_raw:
        return
    try:
        rp = json.loads(rp_raw)
    except Exception:
        return
    max_rc = int(rp.get("maxReceiveCount", 0))
    arn = rp.get("deadLetterTargetArn", "")
    if not max_rc or not arn:
        return

    dlq = next((qq for qq in _queues.values()
                if qq["attributes"].get("QueueArn") == arn), None)
    if dlq is None:
        return

    now = time.time()
    keep: list = []
    for m in q["messages"]:
        if m["receive_count"] >= max_rc and m["visible_at"] <= now:
            moved = dict(m)
            moved["receipt_handle"] = None
            moved["visible_at"] = now
            dlq["messages"].append(moved)
        else:
            keep.append(m)
    q["messages"] = keep


# ── FIFO deduplication cache maintenance ───────────────────

def _prune_dedup(q: dict) -> None:
    now = time.time()
    q["dedup_cache"] = {
        k: v for k, v in q["dedup_cache"].items()
        if v["expire"] > now
    }


# ── Build system attributes for a received message ────────

def _build_sys_attrs(msg: dict, names: list) -> dict:
    if not names:
        return {}
    want_all = "All" in names
    r: dict = {}
    if want_all or "SenderId" in names:
        r["SenderId"] = msg["sys"].get("SenderId", get_account_id())
    if want_all or "SentTimestamp" in names:
        r["SentTimestamp"] = msg["sys"].get("SentTimestamp", "0")
    if want_all or "ApproximateReceiveCount" in names:
        r["ApproximateReceiveCount"] = str(msg["receive_count"])
    if want_all or "ApproximateFirstReceiveTimestamp" in names:
        ts = msg.get("first_receive_at")
        r["ApproximateFirstReceiveTimestamp"] = \
            str(int(ts * 1000)) if ts else "0"
    if msg.get("seq") and (want_all or "SequenceNumber" in names):
        r["SequenceNumber"] = msg["seq"]
    if msg.get("dedup_id") and (want_all or "MessageDeduplicationId" in names):
        r["MessageDeduplicationId"] = msg["dedup_id"]
    if msg.get("group_id") and (want_all or "MessageGroupId" in names):
        r["MessageGroupId"] = msg["group_id"]
    return r


# ── Filter user message attributes by requested names ─────

def _filter_msg_attrs(attrs: dict, names: list) -> dict:
    if not attrs or not names:
        return {}
    if "All" in names or ".*" in names:
        return dict(attrs)
    out: dict = {}
    for n in names:
        if n.endswith(".*"):
            pfx = n[:-2]
            for k, v in attrs.items():
                if k.startswith(pfx):
                    out[k] = v
        elif n in attrs:
            out[n] = attrs[n]
    return out


# ── MD5 of message attributes (AWS wire-format) ───────────

def _md5_msg_attrs(attrs: dict | None) -> str | None:
    """Compute the MD5 digest of message attributes following the
    exact binary encoding that the real SQS service uses."""
    if not attrs:
        return None
    buf = bytearray()
    for name in sorted(attrs):
        a = attrs[name]
        dt = (a.get("DataType") or "String").encode("utf-8")
        nb = name.encode("utf-8")
        buf += struct.pack("!I", len(nb)) + nb
        buf += struct.pack("!I", len(dt)) + dt
        if dt.startswith(b"Binary"):
            buf += b"\x02"
            val = a.get("BinaryValue", b"")
            if isinstance(val, str):
                val = base64.b64decode(val)
            if isinstance(val, bytearray):
                val = bytes(val)
            buf += struct.pack("!I", len(val)) + val
        else:
            buf += b"\x01"
            val = (a.get("StringValue") or "").encode("utf-8")
            buf += struct.pack("!I", len(val)) + val
    return hashlib.md5(bytes(buf)).hexdigest()


# ────────────────────────────────────────────────────────────
#  RESPONSE FORMATTERS
# ────────────────────────────────────────────────────────────

# ── JSON ────────────────────────────────────────────────────

def _json_ok(data: dict, status: int = 200) -> tuple:
    return (status,
            {"Content-Type": "application/x-amz-json-1.0"},
            json.dumps(data).encode("utf-8"))


# Mapping from JSON protocol shape names to legacy Query-protocol error codes.
# SQS has the awsQueryCompatible trait: botocore reads x-amzn-query-error and
# overrides Error.Code with the legacy code so SDK callers using the old
# namespaced strings (e.g. "AWS.SimpleQueueService.NonExistentQueue") still work.
_QUERY_COMPAT_CODES: dict[str, str] = {
    # Source: aws-sdk-go service/sqs/errors.go ErrCode* constants (authoritative)
    "QueueDoesNotExist":              "AWS.SimpleQueueService.NonExistentQueue",
    "QueueNameExists":                "QueueAlreadyExists",
    "TooManyEntriesInBatchRequest":   "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
    "EmptyBatchRequest":              "AWS.SimpleQueueService.EmptyBatchRequest",
    "BatchEntryIdsNotDistinct":       "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
    "BatchRequestTooLong":            "AWS.SimpleQueueService.BatchRequestTooLong",
    "InvalidBatchEntryId":            "AWS.SimpleQueueService.InvalidBatchEntryId",
    "MessageNotInflight":             "AWS.SimpleQueueService.MessageNotInflight",
    "PurgeQueueInProgress":           "AWS.SimpleQueueService.PurgeQueueInProgress",
    "QueueDeletedRecently":           "AWS.SimpleQueueService.QueueDeletedRecently",
    "UnsupportedOperation":           "AWS.SimpleQueueService.UnsupportedOperation",
    "OverLimit":                      "OverLimit",
    "InvalidIdFormat":                "InvalidIdFormat",
    "InvalidMessageContents":         "InvalidMessageContents",
    "ReceiptHandleIsInvalid":         "ReceiptHandleIsInvalid",
    "InvalidAttributeName":           "InvalidAttributeName",
    "InvalidAttributeValue":          "InvalidAttributeValue",
    "InvalidSecurity":                "InvalidSecurity",
    "InvalidAddress":                 "InvalidAddress",
    "RequestThrottled":               "RequestThrottled",
    "ResourceNotFoundException":      "ResourceNotFoundException",
    # KMS errors — no namespace prefix
    "KmsAccessDenied":                "KmsAccessDenied",
    "KmsDisabled":                    "KmsDisabled",
    "KmsInvalidKeyUsage":             "KmsInvalidKeyUsage",
    "KmsInvalidState":                "KmsInvalidState",
    "KmsNotFound":                    "KmsNotFound",
    "KmsOptInRequired":               "KmsOptInRequired",
    "KmsThrottled":                   "KmsThrottled",
}


def _json_err_resp(code: str, msg: str, status: int = 400) -> tuple:
    fault = "Sender" if status < 500 else "Receiver"
    legacy = _QUERY_COMPAT_CODES.get(code, code)
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "x-amzn-query-error": f"{legacy};{fault}",
    }
    return (status, headers, json.dumps({"__type": code, "message": msg}).encode("utf-8"))


# ── XML ─────────────────────────────────────────────────────

def _xml_resp(status: int, root: str, inner: str) -> tuple:
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<{root} xmlns="http://queue.amazonaws.com/doc/2012-11-05/">'
        f'{inner}'
        f'<ResponseMetadata><RequestId>{new_uuid()}</RequestId></ResponseMetadata>'
        f'</{root}>'
    ).encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _xml_err_resp(code: str, msg: str, status: int = 400) -> tuple:
    sender_type = "Sender" if status < 500 else "Receiver"
    body = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">'
        f'<Error><Type>{sender_type}</Type><Code>{_esc(code)}</Code><Message>{_esc(msg)}</Message></Error>'
        f'<RequestId>{new_uuid()}</RequestId>'
        f'</ErrorResponse>'
    ).encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _sender_fault_str(val) -> str:
    if isinstance(val, bool):
        return "true" if val else "false"
    return str(val)


def _to_xml(action: str, result: dict) -> tuple:
    """Convert a core-action result dict into a legacy XML response."""

    if action == "CreateQueue":
        return _xml_resp(200, "CreateQueueResponse",
            f'<CreateQueueResult>'
            f'<QueueUrl>{_esc(result["QueueUrl"])}</QueueUrl>'
            f'</CreateQueueResult>')

    if action == "DeleteQueue":
        return _xml_resp(200, "DeleteQueueResponse", "")

    if action == "ListQueues":
        members = "".join(
            f"<QueueUrl>{_esc(u)}</QueueUrl>"
            for u in result.get("QueueUrls", []))
        return _xml_resp(200, "ListQueuesResponse",
                         f"<ListQueuesResult>{members}</ListQueuesResult>")

    if action == "GetQueueUrl":
        return _xml_resp(200, "GetQueueUrlResponse",
            f'<GetQueueUrlResult>'
            f'<QueueUrl>{_esc(result["QueueUrl"])}</QueueUrl>'
            f'</GetQueueUrlResult>')

    if action == "SendMessage":
        inner = (f'<SendMessageResult>'
                 f'<MessageId>{result["MessageId"]}</MessageId>'
                 f'<MD5OfMessageBody>{result["MD5OfMessageBody"]}</MD5OfMessageBody>')
        if "MD5OfMessageAttributes" in result:
            inner += (f'<MD5OfMessageAttributes>'
                      f'{result["MD5OfMessageAttributes"]}'
                      f'</MD5OfMessageAttributes>')
        if "SequenceNumber" in result:
            inner += f'<SequenceNumber>{result["SequenceNumber"]}</SequenceNumber>'
        inner += '</SendMessageResult>'
        return _xml_resp(200, "SendMessageResponse", inner)

    if action == "ReceiveMessage":
        return _xml_resp(200, "ReceiveMessageResponse",
                         f'<ReceiveMessageResult>'
                         f'{_msgs_to_xml(result.get("Messages", []))}'
                         f'</ReceiveMessageResult>')

    if action == "DeleteMessage":
        return _xml_resp(200, "DeleteMessageResponse", "")

    if action == "ChangeMessageVisibility":
        return _xml_resp(200, "ChangeMessageVisibilityResponse", "")

    if action == "ChangeMessageVisibilityBatch":
        inner = _batch_result_xml(
            result, "ChangeMessageVisibilityBatchResultEntry")
        return _xml_resp(200, "ChangeMessageVisibilityBatchResponse",
                         f'<ChangeMessageVisibilityBatchResult>'
                         f'{inner}'
                         f'</ChangeMessageVisibilityBatchResult>')

    if action == "GetQueueAttributes":
        ax = "".join(
            f'<Attribute><Name>{_esc(k)}</Name>'
            f'<Value>{_esc(str(v))}</Value></Attribute>'
            for k, v in result.get("Attributes", {}).items())
        return _xml_resp(200, "GetQueueAttributesResponse",
                         f'<GetQueueAttributesResult>{ax}</GetQueueAttributesResult>')

    if action == "SetQueueAttributes":
        return _xml_resp(200, "SetQueueAttributesResponse", "")

    if action == "PurgeQueue":
        return _xml_resp(200, "PurgeQueueResponse", "")

    if action == "SendMessageBatch":
        inner = ""
        for e in result.get("Successful", []):
            inner += (f'<SendMessageBatchResultEntry>'
                      f'<Id>{_esc(e.get("Id",""))}</Id>'
                      f'<MessageId>{e["MessageId"]}</MessageId>'
                      f'<MD5OfMessageBody>{e["MD5OfMessageBody"]}'
                      f'</MD5OfMessageBody>')
            if "MD5OfMessageAttributes" in e:
                inner += (f'<MD5OfMessageAttributes>'
                          f'{e["MD5OfMessageAttributes"]}'
                          f'</MD5OfMessageAttributes>')
            if "SequenceNumber" in e:
                inner += (f'<SequenceNumber>{e["SequenceNumber"]}'
                          f'</SequenceNumber>')
            inner += '</SendMessageBatchResultEntry>'
        inner += _batch_errors_xml(result.get("Failed", []))
        return _xml_resp(200, "SendMessageBatchResponse",
                         f'<SendMessageBatchResult>{inner}</SendMessageBatchResult>')

    if action == "DeleteMessageBatch":
        inner = ""
        for e in result.get("Successful", []):
            inner += (f'<DeleteMessageBatchResultEntry>'
                      f'<Id>{_esc(e["Id"])}</Id>'
                      f'</DeleteMessageBatchResultEntry>')
        inner += _batch_errors_xml(result.get("Failed", []))
        return _xml_resp(200, "DeleteMessageBatchResponse",
                         f'<DeleteMessageBatchResult>{inner}</DeleteMessageBatchResult>')

    if action == "ListQueueTags":
        tx = "".join(
            f'<Tag><Key>{_esc(k)}</Key><Value>{_esc(v)}</Value></Tag>'
            for k, v in result.get("Tags", {}).items())
        return _xml_resp(200, "ListQueueTagsResponse",
                         f'<ListQueueTagsResult>{tx}</ListQueueTagsResult>')

    if action == "TagQueue":
        return _xml_resp(200, "TagQueueResponse", "")

    if action == "UntagQueue":
        return _xml_resp(200, "UntagQueueResponse", "")

    return _xml_resp(200, f"{action}Response", "")


# ── XML sub-helpers ─────────────────────────────────────────

def _msgs_to_xml(msgs: list) -> str:
    """Render a list of received messages to XML."""
    parts: list = []
    for m in msgs:
        x = (f'<Message>'
             f'<MessageId>{m["MessageId"]}</MessageId>'
             f'<ReceiptHandle>{_esc(m["ReceiptHandle"])}</ReceiptHandle>'
             f'<MD5OfBody>{m["MD5OfBody"]}</MD5OfBody>'
             f'<Body>{_esc(m["Body"])}</Body>')

        # System attributes → <Attribute>
        for ak, av in m.get("Attributes", {}).items():
            x += (f'<Attribute>'
                  f'<Name>{_esc(ak)}</Name>'
                  f'<Value>{_esc(str(av))}</Value>'
                  f'</Attribute>')

        # User message attributes → <MessageAttribute>
        for ak, av in m.get("MessageAttributes", {}).items():
            x += (f'<MessageAttribute>'
                  f'<Name>{_esc(ak)}</Name><Value>'
                  f'<DataType>{_esc(av.get("DataType","String"))}</DataType>')
            if "StringValue" in av:
                x += f'<StringValue>{_esc(av["StringValue"])}</StringValue>'
            if "BinaryValue" in av:
                x += (f'<BinaryValue>'
                      f'{_esc(str(av["BinaryValue"]))}'
                      f'</BinaryValue>')
            x += '</Value></MessageAttribute>'

        if "MD5OfMessageAttributes" in m:
            x += (f'<MD5OfMessageAttributes>'
                  f'{m["MD5OfMessageAttributes"]}'
                  f'</MD5OfMessageAttributes>')

        x += '</Message>'
        parts.append(x)
    return "".join(parts)


def _batch_result_xml(result: dict, entry_tag: str) -> str:
    inner = ""
    for e in result.get("Successful", []):
        inner += f'<{entry_tag}><Id>{_esc(e["Id"])}</Id></{entry_tag}>'
    inner += _batch_errors_xml(result.get("Failed", []))
    return inner


def _batch_errors_xml(failed: list) -> str:
    x = ""
    for e in failed:
        x += (f'<BatchResultErrorEntry>'
              f'<Id>{_esc(e.get("Id",""))}</Id>'
              f'<Code>{_esc(e.get("Code",""))}</Code>'
              f'<Message>{_esc(e.get("Message",""))}</Message>'
              f'<SenderFault>{_sender_fault_str(e.get("SenderFault", True))}</SenderFault>'
              f'</BatchResultErrorEntry>')
    return x


# ────────────────────────────────────────────────────────────
#  QUERY-PARAM NORMALISATION  (indexed form → flat dict)
# ────────────────────────────────────────────────────────────

def _normalise(action: str, params: dict) -> dict:
    """Convert indexed query params to the same dict shape the JSON API uses."""
    d: dict = {}

    # Scalar params
    for key in ("QueueName", "QueueUrl", "MessageBody", "ReceiptHandle",
                "VisibilityTimeout", "DelaySeconds", "WaitTimeSeconds",
                "MaxNumberOfMessages", "MaxResults", "QueueNamePrefix",
                "MessageGroupId", "MessageDeduplicationId",
                "ReceiveRequestAttemptId"):
        v = _p(params, key)
        if v:
            d[key] = v

    # Attribute.N.Name / .Value  →  Attributes dict
    attrs: dict = {}
    i = 1
    while _p(params, f"Attribute.{i}.Name"):
        attrs[_p(params, f"Attribute.{i}.Name")] = \
            _p(params, f"Attribute.{i}.Value")
        i += 1
    if attrs:
        d["Attributes"] = attrs

    # AttributeName.N  →  AttributeNames list
    an: list = []
    i = 1
    while _p(params, f"AttributeName.{i}"):
        an.append(_p(params, f"AttributeName.{i}"))
        i += 1
    if an:
        d["AttributeNames"] = an

    # MessageAttributeName.N
    man: list = []
    i = 1
    while _p(params, f"MessageAttributeName.{i}"):
        man.append(_p(params, f"MessageAttributeName.{i}"))
        i += 1
    if man:
        d["MessageAttributeNames"] = man

    # MessageAttribute.N.Name / .Value.*
    ma: dict = {}
    i = 1
    while _p(params, f"MessageAttribute.{i}.Name"):
        nm = _p(params, f"MessageAttribute.{i}.Name")
        a: dict = {
            "DataType": _p(params,
                           f"MessageAttribute.{i}.Value.DataType") or "String",
        }
        sv = _p(params, f"MessageAttribute.{i}.Value.StringValue")
        bv = _p(params, f"MessageAttribute.{i}.Value.BinaryValue")
        if sv:
            a["StringValue"] = sv
        if bv:
            a["BinaryValue"] = bv
        ma[nm] = a
        i += 1
    if ma:
        d["MessageAttributes"] = ma

    # Tag.N.Key / .Value
    tags: dict = {}
    i = 1
    while _p(params, f"Tag.{i}.Key"):
        tags[_p(params, f"Tag.{i}.Key")] = _p(params, f"Tag.{i}.Value")
        i += 1
    if tags:
        d["Tags"] = tags

    # TagKey.N
    tk: list = []
    i = 1
    while _p(params, f"TagKey.{i}"):
        tk.append(_p(params, f"TagKey.{i}"))
        i += 1
    if tk:
        d["TagKeys"] = tk

    # ── Batch entries ───────────────────────────────────────

    if action == "SendMessageBatch":
        d["Entries"] = _parse_send_batch_entries(params)

    if action == "DeleteMessageBatch":
        entries: list = []
        i = 1
        pfx = "DeleteMessageBatchRequestEntry"
        while _p(params, f"{pfx}.{i}.Id"):
            entries.append({
                "Id": _p(params, f"{pfx}.{i}.Id"),
                "ReceiptHandle": _p(params, f"{pfx}.{i}.ReceiptHandle"),
            })
            i += 1
        d["Entries"] = entries

    if action == "ChangeMessageVisibilityBatch":
        entries = []
        i = 1
        pfx = "ChangeMessageVisibilityBatchRequestEntry"
        while _p(params, f"{pfx}.{i}.Id"):
            entries.append({
                "Id": _p(params, f"{pfx}.{i}.Id"),
                "ReceiptHandle": _p(params, f"{pfx}.{i}.ReceiptHandle"),
                "VisibilityTimeout":
                    _p(params, f"{pfx}.{i}.VisibilityTimeout"),
            })
            i += 1
        d["Entries"] = entries

    return d


def _parse_send_batch_entries(params: dict) -> list:
    entries: list = []
    i = 1
    pfx = "SendMessageBatchRequestEntry"
    while _p(params, f"{pfx}.{i}.Id"):
        e: dict = {
            "Id": _p(params, f"{pfx}.{i}.Id"),
            "MessageBody": _p(params, f"{pfx}.{i}.MessageBody"),
        }
        ds = _p(params, f"{pfx}.{i}.DelaySeconds")
        if ds:
            e["DelaySeconds"] = ds
        gid = _p(params, f"{pfx}.{i}.MessageGroupId")
        if gid:
            e["MessageGroupId"] = gid
        did = _p(params, f"{pfx}.{i}.MessageDeduplicationId")
        if did:
            e["MessageDeduplicationId"] = did

        # Per-entry message attributes
        ema: dict = {}
        j = 1
        while _p(params, f"{pfx}.{i}.MessageAttribute.{j}.Name"):
            anm = _p(params, f"{pfx}.{i}.MessageAttribute.{j}.Name")
            a: dict = {
                "DataType": _p(
                    params,
                    f"{pfx}.{i}.MessageAttribute.{j}.Value.DataType"
                ) or "String",
            }
            sv = _p(params,
                    f"{pfx}.{i}.MessageAttribute.{j}.Value.StringValue")
            bv = _p(params,
                    f"{pfx}.{i}.MessageAttribute.{j}.Value.BinaryValue")
            if sv:
                a["StringValue"] = sv
            if bv:
                a["BinaryValue"] = bv
            ema[anm] = a
            j += 1
        if ema:
            e["MessageAttributes"] = ema

        entries.append(e)
        i += 1
    return entries


# ────────────────────────────────────────────────────────────
#  LOW-LEVEL HELPERS
# ────────────────────────────────────────────────────────────

def _p(params: dict, key: str, default: str = "") -> str:
    """Extract a scalar value from *params* which may hold strings or lists
    (``parse_qs`` returns lists)."""
    val = params.get(key, [default])
    if isinstance(val, list):
        return val[0] if val else default
    return val


def _url_from_path(path: str) -> str:
    """Derive a queue URL from a request path like ``/000000000000/my-queue``."""
    parts = path.strip("/").split("/")
    if len(parts) >= 2:
        return _queue_url(parts[-1])
    return ""


def reset():
    _queues.clear()
    _queue_name_to_url.clear()


# ────────────────────────────────────────────────────────────
#  ESM helpers (internal)
# ────────────────────────────────────────────────────────────
#
# Lambda Event Source Mapping (SQS → Lambda) should behave like a real client:
# - "receive" makes messages invisible for the queue VisibilityTimeout and assigns ReceiptHandle
# - "delete" removes by ReceiptHandle
#
# The ESM poller lives in `services/lambda_svc.py` and runs in a background thread
# (non-async), so we provide sync helpers that reuse the same core SQS logic as
# ReceiveMessage/DeleteMessageBatch.


def _receive_messages_for_esm(queue_url: str, max_number: int) -> list[dict]:
    """Receive up to max_number messages for ESM consumption (thread-safe)."""
    with _queues_lock:
        q = _get_q(queue_url)
        max_n = min(int(max_number or 1), 10)
        vis = int(q["attributes"].get("VisibilityTimeout", "30"))
        _dlq_sweep(q)
        return _collect_msgs(q, max_n, vis)


def _delete_messages_for_esm(queue_url: str, receipt_handles: set[str]) -> None:
    """Best-effort delete of messages received by ESM (thread-safe)."""
    if not receipt_handles:
        return
    with _queues_lock:
        q = _get_q(queue_url)
        kept = []
        for m in q["messages"]:
            if m.get("receipt_handle") is not None and m.get("receipt_handle") in receipt_handles:
                if q["is_fifo"] and m.get("dedup_id"):
                    q["dedup_cache"].pop(m["dedup_id"], None)
            else:
                kept.append(m)
        q["messages"] = kept
