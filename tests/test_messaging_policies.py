"""SQS queue-policy and SNS topic-policy enforcement.

The Policy attributes were stored (CRUD) but never evaluated. Under AUTH=true
they are now enforced:

- Same-account API calls pass unless the policy carries an explicit Deny.
- Cross-account API calls need an explicit Allow (AccessDenied /
  AuthorizationError otherwise).
- S3→SQS, S3→SNS and SNS→SQS deliveries need an explicit Allow for the
  calling service in the destination policy (with its SourceArn/SourceAccount
  conditions); without one the delivery is dropped, like real AWS.
- S3 refuses a notification config whose SQS/SNS destination denies it
  (InvalidArgument), unless SkipDestinationValidation skips the probe.

With AUTH=false every path stays permissive, like the rest of the identity
layer. The shared CI server runs with auth disabled, so these tests drive the
service modules in-process with AUTH monkeypatched (the test_cfn capabilities
pattern) instead of going over HTTP.
"""

import json
import re
import uuid

import pytest
from conftest import (
    REGION,
    sqs_policy_allow_s3,
    sqs_policy_allow_sns,
)

from ministack.core.iam_evaluator import (
    EvalContext,
    evaluate_resource_policy,
)
from ministack.core.responses import request_scope
from ministack.services import s3 as s3_svc
from ministack.services import sns as sns_svc
from ministack.services import sqs as sqs_svc

ACCT_A = "111111111111"
ACCT_B = "222222222222"


def _reset_messaging_state():
    from ministack.core.iam_evaluator import _request_caller_arn

    sqs_svc.reset()
    sns_svc.reset()
    s3_svc.reset()
    _request_caller_arn.set("")


@pytest.fixture
def auth(monkeypatch):
    """AUTH=true with clean in-process SQS/SNS/S3 state."""
    import ministack.app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", True)
    _reset_messaging_state()
    yield
    _reset_messaging_state()


@pytest.fixture
def noauth(monkeypatch):
    """AUTH=false with clean in-process SQS/SNS/S3 state."""
    import ministack.app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", False)
    _reset_messaging_state()
    yield
    _reset_messaging_state()


def _uniq(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _as(account, fn, *args, **kwargs):
    """Call an in-process service function as *account* in the test region."""
    with request_scope(account, REGION):
        return fn(*args, **kwargs)


def _resp_detail(resp):
    """``(status, code, message)`` of an in-process SNS/S3 response tuple."""
    status, _headers, body = resp
    text = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else str(body)
    code = re.search(r"<Code>([^<]*)</Code>", text)
    msg = re.search(r"<Message>([^<]*)</Message>", text)
    return (status, code.group(1) if code else "",
            msg.group(1) if msg else "")


def _resp_code(resp):
    """``(status, code)`` of an in-process SNS/S3 response tuple."""
    status, code, _ = _resp_detail(resp)
    return status, code


def _qarn(account, name):
    return f"arn:aws:sqs:{REGION}:{account}:{name}"


def _tarn(account, name):
    return f"arn:aws:sns:{REGION}:{account}:{name}"


def _make_queue(account, name, policy=None):
    url = _as(account, sqs_svc._act_create_queue,
              {"QueueName": name}, "")["QueueUrl"]
    if policy is not None:
        _as(account, sqs_svc._act_set_queue_attributes,
            {"QueueUrl": url,
             "Attributes": {"Policy": json.dumps(policy)}}, url)
    return url


def _queue_bodies(arn):
    q = sqs_svc._queue_by_arn(arn)
    assert q is not None
    return [m["body"] for m in q["messages"]]


def _deny_all(arn, action):
    return {"Version": "2012-10-17", "Statement": [{
        "Sid": "deny", "Effect": "Deny", "Principal": {"AWS": "*"},
        "Action": action, "Resource": arn}]}


def _allow_account(arn, account, action):
    return {"Version": "2012-10-17", "Statement": [{
        "Sid": "x", "Effect": "Allow", "Principal": {"AWS": account},
        "Action": action, "Resource": arn}]}


def _make_topic(account, name):
    assert _resp_code(_as(account, sns_svc._create_topic,
                           {"Name": name})) == (200, "")
    return _tarn(account, name)


def _set_topic_policy(account, arn, policy):
    assert _resp_code(_as(account, sns_svc._set_topic_attributes, {
        "TopicArn": arn, "AttributeName": "Policy",
        "AttributeValue": json.dumps(policy)})) == (200, "")


def _topic_policy(arn):
    topic = sns_svc._topic_by_arn_any_scope(arn)
    assert topic is not None
    return json.loads(topic["attributes"]["Policy"])


def _make_bucket(account, name):
    assert _resp_code(_as(account, s3_svc._create_bucket,
                           name, b"", {})) == (200, "")


def _notif_xml(kind, arn):
    tag = "Queue" if kind == "sqs" else "Topic"
    return (
        "<NotificationConfiguration>"
        f"<{tag}Configuration><{tag}>{arn}</{tag}>"
        "<Event>s3:ObjectCreated:*</Event>"
        f"</{tag}Configuration>"
        "</NotificationConfiguration>"
    ).encode()


def _put_notif(account, bucket, kind, arn, skip=False):
    headers = {"x-amz-skip-destination-validation": "true"} if skip else {}
    return _as(account, s3_svc._put_bucket_notification,
               bucket, _notif_xml(kind, arn), headers)


# ---------------------------------------------------------------------------
# Unit: evaluate_resource_policy Service principals
# ---------------------------------------------------------------------------

def _ctx(action="sqs:SendMessage", resource="arn:aws:sqs:us-east-1:1:q",
         source_arn="", source_account="", principal="arn:aws:iam::1:root"):
    return EvalContext(
        principal_arn=principal,
        principal_type="Service",
        principal_account="1",
        action=action,
        resource_arn=resource,
        region="us-east-1",
        service_context={"aws:sourcearn": source_arn,
                         "aws:sourceaccount": source_account,
                         "aws:sourceowner": source_account},
    )


def _svc_policy():
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:us-east-1:1:q",
            "Condition": {"ArnLike": {"aws:SourceArn": "arn:aws:s3:*:*:bkt"}},
        }],
    }


class TestEvaluateResourcePolicyService:
    def test_service_principal_allows_matching_service(self):
        assert evaluate_resource_policy(
            _svc_policy(), _ctx(source_arn="arn:aws:s3:::bkt"),
            service="s3.amazonaws.com").decision == "Allow"

    def test_service_principal_ignores_other_service(self):
        assert evaluate_resource_policy(
            _svc_policy(), _ctx(source_arn="arn:aws:s3:::bkt"),
            service="sns.amazonaws.com").decision == "ImplicitDeny"

    def test_service_principal_ignored_without_service_arg(self):
        # Legacy callers (Lambda layer policies, S3 bucket policies) keep the
        # old behaviour: Service entries never match.
        assert evaluate_resource_policy(
            _svc_policy(), _ctx(source_arn="arn:aws:s3:::bkt")).decision == "ImplicitDeny"

    def test_wrong_source_arn_denies(self):
        assert evaluate_resource_policy(
            _svc_policy(), _ctx(source_arn="arn:aws:s3:::other"),
            service="s3.amazonaws.com").decision == "ImplicitDeny"

    def test_aws_principal_still_matches(self):
        doc = {"Statement": [{
            "Effect": "Allow", "Principal": {"AWS": "222222222222"},
            "Action": "sqs:SendMessage", "Resource": "*"}]}
        ctx = _ctx(principal="arn:aws:iam::222222222222:root")
        assert evaluate_resource_policy(doc, ctx).decision == "Allow"

    def test_star_principal_matches(self):
        doc = {"Statement": [{
            "Effect": "Allow", "Principal": "*",
            "Action": "sqs:SendMessage", "Resource": "*"}]}
        assert evaluate_resource_policy(doc, _ctx()).decision == "Allow"


# ---------------------------------------------------------------------------
# Cross-account SQS
# ---------------------------------------------------------------------------

class TestCrossAccountSqs:
    def test_send_denied_without_policy(self, auth):
        url = _make_queue(ACCT_A, _uniq("xpol-q"))
        with pytest.raises(sqs_svc._Err) as exc:
            _as(ACCT_B, sqs_svc._act_send_message,
                {"QueueUrl": url, "MessageBody": "no-policy"}, url)
        assert exc.value.code == "AccessDenied"
        assert exc.value.status == 403

    def test_send_allowed_with_policy(self, auth):
        name = _uniq("xpol-q")
        url = _make_queue(ACCT_A, name, _allow_account(
            _qarn(ACCT_A, name), ACCT_B, "sqs:SendMessage"))
        _as(ACCT_B, sqs_svc._act_send_message,
            {"QueueUrl": url, "MessageBody": "with-policy"}, url)
        assert _queue_bodies(_qarn(ACCT_A, name)) == ["with-policy"]

    def test_send_allowed_via_add_permission(self, auth):
        name = _uniq("xpol-q")
        url = _make_queue(ACCT_A, name)
        _as(ACCT_A, sqs_svc._act_add_permission,
            {"QueueUrl": url, "Label": "xacct",
             "AWSAccountIds": [ACCT_B], "Actions": ["SendMessage"]}, url)
        _as(ACCT_B, sqs_svc._act_send_message,
            {"QueueUrl": url, "MessageBody": "via-add-permission"}, url)
        assert _queue_bodies(_qarn(ACCT_A, name)) == ["via-add-permission"]

    def test_explicit_deny_blocks_same_account(self, auth):
        name = _uniq("xpol-q")
        url = _make_queue(ACCT_A, name, _deny_all(
            _qarn(ACCT_A, name), "sqs:SendMessage"))
        with pytest.raises(sqs_svc._Err) as exc:
            _as(ACCT_A, sqs_svc._act_send_message,
                {"QueueUrl": url, "MessageBody": "deny-me"}, url)
        assert exc.value.code == "AccessDenied"

    def test_get_queue_url_with_owner_account(self, auth):
        name = _uniq("xpol-q")
        url = _make_queue(ACCT_A, name, _allow_account(
            _qarn(ACCT_A, name), ACCT_B, "sqs:GetQueueUrl"))
        found = _as(ACCT_B, sqs_svc._act_get_queue_url,
                    {"QueueName": name,
                     "QueueOwnerAWSAccountId": ACCT_A}, "")["QueueUrl"]
        assert found == url

    def test_get_queue_url_cross_account_denied_without_policy(self, auth):
        name = _uniq("xpol-q")
        _make_queue(ACCT_A, name)
        with pytest.raises(sqs_svc._Err) as exc:
            _as(ACCT_B, sqs_svc._act_get_queue_url,
                {"QueueName": name,
                 "QueueOwnerAWSAccountId": ACCT_A}, "")
        assert exc.value.code == "AccessDenied"
        assert exc.value.status == 403

    def test_get_queue_url_same_account_denied_by_explicit_deny(self, auth):
        name = _uniq("xpol-q")
        _make_queue(ACCT_A, name, _deny_all(
            _qarn(ACCT_A, name), "sqs:GetQueueUrl"))
        with pytest.raises(sqs_svc._Err) as exc:
            _as(ACCT_A, sqs_svc._act_get_queue_url,
                {"QueueName": name}, "")
        assert exc.value.code == "AccessDenied"
        assert exc.value.status == 403

    def test_get_queue_url_same_account_passes_without_policy(self, auth):
        name = _uniq("xpol-q")
        url = _make_queue(ACCT_A, name)
        assert _as(ACCT_A, sqs_svc._act_get_queue_url,
                   {"QueueName": name}, "") == {"QueueUrl": url}


# ---------------------------------------------------------------------------
# Cross-account SNS (+ AddPermission/RemovePermission)
# ---------------------------------------------------------------------------

class TestCrossAccountSns:
    def test_publish_denied_by_default(self, auth):
        arn = _make_topic(ACCT_A, _uniq("xpol-t"))
        assert _resp_code(_as(ACCT_B, sns_svc._publish, {
            "TopicArn": arn, "Message": "no-grant"})) == (403, "AuthorizationError")

    def test_publish_allowed_with_policy(self, auth):
        name = _uniq("xpol-t")
        arn = _make_topic(ACCT_A, name)
        _set_topic_policy(ACCT_A, arn, _allow_account(
            arn, ACCT_B, "sns:Publish"))
        assert _resp_code(_as(ACCT_B, sns_svc._publish, {
            "TopicArn": arn, "Message": "granted"})) == (200, "")
        topic = sns_svc._topic_by_arn_any_scope(arn)
        assert [m["message"] for m in topic["messages"]] == ["granted"]

    def test_explicit_deny_blocks_same_account(self, auth):
        name = _uniq("xpol-t")
        arn = _make_topic(ACCT_A, name)
        _set_topic_policy(ACCT_A, arn, _deny_all(arn, "sns:Publish"))
        assert _resp_code(_as(ACCT_A, sns_svc._publish, {
            "TopicArn": arn, "Message": "deny-me"})) == (403, "AuthorizationError")

    def test_add_permission_grants_cross_account_publish(self, auth):
        arn = _make_topic(ACCT_A, _uniq("xpol-t"))
        assert _resp_code(_as(ACCT_A, sns_svc._add_permission, {
            "TopicArn": arn, "Label": "perm-1",
            "AWSAccountId.member.1": ACCT_B,
            "ActionName.member.1": "Publish"})) == (200, "")
        stmt = next(s for s in _topic_policy(arn)["Statement"]
                    if s.get("Sid") == "perm-1")
        assert ACCT_B in stmt["Principal"]["AWS"]
        assert "sns:Publish" in stmt["Action"]
        assert _resp_code(_as(ACCT_B, sns_svc._publish, {
            "TopicArn": arn,
            "Message": "via-add-permission"})) == (200, "")

    def test_add_permission_rejects_duplicate_label(self, auth):
        arn = _make_topic(ACCT_A, _uniq("xpol-t"))
        params = {"TopicArn": arn, "Label": "dup",
                  "AWSAccountId.member.1": ACCT_B,
                  "ActionName.member.1": "Publish"}
        assert _resp_code(_as(ACCT_A, sns_svc._add_permission, params)) == (200, "")
        assert _resp_code(
            _as(ACCT_A, sns_svc._add_permission, params)) == (
                400, "InvalidParameterException")

    def test_cross_account_tag_access_needs_grant(self, auth):
        name = _uniq("xpol-t")
        arn = _make_topic(ACCT_A, name)
        assert _resp_code(_as(ACCT_B, sns_svc._list_tags_for_resource, {
            "ResourceArn": arn})) == (403, "AuthorizationError")
        _set_topic_policy(ACCT_A, arn, _allow_account(
            arn, ACCT_B, ["sns:TagResource", "sns:ListTagsForResource"]))
        assert _resp_code(_as(ACCT_B, sns_svc._tag_resource, {
            "ResourceArn": arn, "Tags.member.1.Key": "k",
            "Tags.member.1.Value": "v"})) == (200, "")
        assert _resp_code(_as(ACCT_B, sns_svc._list_tags_for_resource, {
            "ResourceArn": arn})) == (200, "")
        assert sns_svc._topic_by_arn_any_scope(arn)["tags"] == {"k": "v"}

    def test_remove_permission_drops_statement(self, auth):
        arn = _make_topic(ACCT_A, _uniq("xpol-t"))
        _as(ACCT_A, sns_svc._add_permission, {
            "TopicArn": arn, "Label": "drop",
            "AWSAccountId.member.1": ACCT_B,
            "ActionName.member.1": "Publish"})
        assert _resp_code(_as(ACCT_B, sns_svc._publish, {
            "TopicArn": arn, "Message": "before"})) == (200, "")
        assert _resp_code(_as(ACCT_A, sns_svc._remove_permission, {
            "TopicArn": arn, "Label": "drop"})) == (200, "")
        assert all(s.get("Sid") != "drop"
                   for s in _topic_policy(arn)["Statement"])
        assert _resp_code(_as(ACCT_B, sns_svc._publish, {
            "TopicArn": arn, "Message": "after"})) == (403, "AuthorizationError")


# ---------------------------------------------------------------------------
# S3 notifications gated on the destination policy
# ---------------------------------------------------------------------------

def _bucket_and_queue(tag):
    bucket, qname = _uniq(f"{tag}-bkt"), _uniq(f"{tag}-q")
    _make_bucket(ACCT_A, bucket)
    url = _make_queue(ACCT_A, qname)
    return bucket, url, _qarn(ACCT_A, qname)


def _assert_put_rejected(bucket, kind, arn):
    status, code, msg = _resp_detail(_put_notif(ACCT_A, bucket, kind, arn))
    assert status == 400
    assert code == "InvalidArgument"
    assert "Unable to validate the following destination configurations" in msg


def _bucket_topic_queue(tag):
    bucket = _uniq(f"{tag}-bkt")
    _make_bucket(ACCT_A, bucket)
    topic = _make_topic(ACCT_A, _uniq(f"{tag}-t"))
    qname = _uniq(f"{tag}-q")
    qurl = _make_queue(ACCT_A, qname,
                       sqs_policy_allow_sns(_qarn(ACCT_A, qname), topic))
    assert _resp_code(_as(ACCT_A, sns_svc._subscribe, {
        "TopicArn": topic, "Protocol": "sqs",
        "Endpoint": _qarn(ACCT_A, qname)})) == (200, "")
    return bucket, topic, qurl, _qarn(ACCT_A, qname)


class TestS3NotificationPolicies:
    def test_s3_put_rejected_without_queue_policy(self, auth):
        bucket, _url, arn = _bucket_and_queue("xpol")
        _assert_put_rejected(bucket, "sqs", arn)

    def test_s3_put_rejected_by_unrelated_policy(self, auth):
        bucket, url, arn = _bucket_and_queue("xpol")
        _as(ACCT_A, sqs_svc._act_set_queue_attributes,
            {"QueueUrl": url, "Attributes": {"Policy": json.dumps(
                _allow_account(arn, ACCT_B, "sqs:SendMessage"))}}, url)
        _assert_put_rejected(bucket, "sqs", arn)

    def test_s3_put_rejected_by_wrong_source_arn(self, auth):
        bucket, url, arn = _bucket_and_queue("xpol")
        _as(ACCT_A, sqs_svc._act_set_queue_attributes,
            {"QueueUrl": url, "Attributes": {"Policy": json.dumps(
                sqs_policy_allow_s3(arn, "no-such-bucket", ACCT_A))}}, url)
        _assert_put_rejected(bucket, "sqs", arn)

    def test_s3_to_sqs_delivered_with_policy(self, auth):
        bucket, url, arn = _bucket_and_queue("xpol")
        _as(ACCT_A, sqs_svc._act_set_queue_attributes,
            {"QueueUrl": url, "Attributes": {"Policy": json.dumps(
                sqs_policy_allow_s3(arn, bucket, ACCT_A))}}, url)
        assert _resp_code(_put_notif(ACCT_A, bucket, "sqs", arn)) == (200, "")
        bodies = _queue_bodies(arn)
        assert len(bodies) == 1
        assert "s3:TestEvent" in bodies[0]

    def test_s3_skip_validation_stores_but_delivery_blocked(self, auth):
        bucket, _url, arn = _bucket_and_queue("xpol")
        assert _resp_code(
            _put_notif(ACCT_A, bucket, "sqs", arn, skip=True)) == (200, "")
        assert _queue_bodies(arn) == []
        _as(ACCT_A, s3_svc._deliver_event_to_sqs,
            arn, {"Records": []}, REGION, bucket_name=bucket)
        assert _queue_bodies(arn) == []

    def test_s3_to_sns_default_policy_delivers(self, auth):
        bucket, topic, _qurl, qarn = _bucket_topic_queue("xpol")
        assert _resp_code(_put_notif(ACCT_A, bucket, "sns", topic)) == (200, "")
        bodies = _queue_bodies(qarn)
        assert len(bodies) == 1
        assert "s3:TestEvent" in bodies[0]

    def test_s3_put_rejected_by_topic_policy_without_s3(self, auth):
        bucket, topic, _qurl, _qarn = _bucket_topic_queue("xpol")
        _set_topic_policy(ACCT_A, topic, _allow_account(
            topic, ACCT_A, "sns:Publish"))
        _assert_put_rejected(bucket, "sns", topic)

    def test_s3_to_sns_skip_validation_delivery_blocked(self, auth):
        bucket, topic, _qurl, qarn = _bucket_topic_queue("xpol")
        _set_topic_policy(ACCT_A, topic, _allow_account(
            topic, ACCT_A, "sns:Publish"))
        assert _resp_code(
            _put_notif(ACCT_A, bucket, "sns", topic, skip=True)) == (200, "")
        _as(ACCT_A, s3_svc._deliver_event_to_sns,
            topic, {"Records": []}, REGION, bucket_name=bucket)
        assert _queue_bodies(qarn) == []

    def test_s3_to_cross_account_sns_topic_delivered_with_policy(self, auth):
        bucket = _uniq("xpol-xbkt")
        _make_bucket(ACCT_A, bucket)
        topic = _make_topic(ACCT_B, _uniq("xpol-xt"))
        _set_topic_policy(ACCT_B, topic, {
            "Version": "2012-10-17", "Statement": [{
                "Sid": "s3-xacct", "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "sns:Publish", "Resource": topic,
                "Condition": {
                    "ArnLike": {"aws:SourceArn": f"arn:aws:s3:::{bucket}"},
                    "StringEquals": {"aws:SourceAccount": ACCT_A}}}]})
        qname = _uniq("xpol-xq")
        _make_queue(ACCT_B, qname, sqs_policy_allow_sns(
            _qarn(ACCT_B, qname), topic))
        assert _resp_code(_as(ACCT_B, sns_svc._subscribe, {
            "TopicArn": topic, "Protocol": "sqs",
            "Endpoint": _qarn(ACCT_B, qname)})) == (200, "")
        # The topic lives in another account, but its policy allows this
        # bucket's deliveries, so the PUT validates instead of failing.
        assert _resp_code(_put_notif(ACCT_A, bucket, "sns", topic)) == (200, "")
        bodies = _queue_bodies(_qarn(ACCT_B, qname))
        assert len(bodies) == 1
        assert "s3:TestEvent" in bodies[0]

    def test_s3_to_cross_account_sqs_queue_delivered_with_policy(self, auth):
        bucket = _uniq("xpol-xbkt")
        _make_bucket(ACCT_A, bucket)
        qname = _uniq("xpol-xq")
        _make_queue(ACCT_B, qname, sqs_policy_allow_s3(
            _qarn(ACCT_B, qname), bucket, ACCT_A))
        assert _resp_code(
            _put_notif(ACCT_A, bucket, "sqs", _qarn(ACCT_B, qname))) == (200, "")
        bodies = _queue_bodies(_qarn(ACCT_B, qname))
        assert len(bodies) == 1
        assert "s3:TestEvent" in bodies[0]

    def test_s3_to_cross_account_topic_rejected_without_policy(self, auth):
        bucket = _uniq("xpol-xbkt")
        _make_bucket(ACCT_A, bucket)
        topic = _make_topic(ACCT_B, _uniq("xpol-xt"))
        _assert_put_rejected(bucket, "sns", topic)


# ---------------------------------------------------------------------------
# SNS→SQS fanout gated on the queue policy
# ---------------------------------------------------------------------------

def _topic_and_queue(tag):
    topic = _make_topic(ACCT_A, _uniq(f"{tag}-t"))
    qname = _uniq(f"{tag}-q")
    qurl = _make_queue(ACCT_A, qname)
    assert _resp_code(_as(ACCT_A, sns_svc._subscribe, {
        "TopicArn": topic, "Protocol": "sqs",
        "Endpoint": _qarn(ACCT_A, qname)})) == (200, "")
    return topic, qurl, _qarn(ACCT_A, qname)


class TestSnsFanoutPolicies:
    def test_fanout_blocked_without_policy(self, auth):
        topic, _qurl, qarn = _topic_and_queue("xpol-fan")
        assert _resp_code(_as(ACCT_A, sns_svc._publish, {
            "TopicArn": topic, "Message": "no-policy"})) == (200, "")
        assert _queue_bodies(qarn) == []

    def test_fanout_blocked_by_wrong_source_arn(self, auth):
        topic, qurl, qarn = _topic_and_queue("xpol-fan")
        _as(ACCT_A, sqs_svc._act_set_queue_attributes,
            {"QueueUrl": qurl, "Attributes": {"Policy": json.dumps(
                sqs_policy_allow_sns(
                    qarn, f"arn:aws:sns:{REGION}:{ACCT_A}:wrong-topic"))}},
            qurl)
        assert _resp_code(_as(ACCT_A, sns_svc._publish, {
            "TopicArn": topic, "Message": "wrong-source"})) == (200, "")
        assert _queue_bodies(qarn) == []

    def test_fanout_delivered_with_policy(self, auth):
        topic, qurl, qarn = _topic_and_queue("xpol-fan")
        _as(ACCT_A, sqs_svc._act_set_queue_attributes,
            {"QueueUrl": qurl, "Attributes": {"Policy": json.dumps(
                sqs_policy_allow_sns(qarn, topic))}}, qurl)
        assert _resp_code(_as(ACCT_A, sns_svc._publish, {
            "TopicArn": topic, "Message": "with-policy"})) == (200, "")
        bodies = _queue_bodies(qarn)
        assert len(bodies) == 1
        assert "with-policy" in bodies[0]

    def test_cross_account_fanout_needs_queue_policy(self, auth):
        topic = _make_topic(ACCT_A, _uniq("xpol-xfan"))
        _set_topic_policy(ACCT_A, topic, _allow_account(
            topic, ACCT_B, "sns:Subscribe"))
        qname = _uniq("xpol-xfanq")
        qurl = _make_queue(ACCT_B, qname)
        assert _resp_code(_as(ACCT_B, sns_svc._subscribe, {
            "TopicArn": topic, "Protocol": "sqs",
            "Endpoint": _qarn(ACCT_B, qname)})) == (200, "")
        assert _resp_code(_as(ACCT_A, sns_svc._publish, {
            "TopicArn": topic, "Message": "no-queue-policy"})) == (200, "")
        assert _queue_bodies(_qarn(ACCT_B, qname)) == []

        _as(ACCT_B, sqs_svc._act_set_queue_attributes,
            {"QueueUrl": qurl, "Attributes": {"Policy": json.dumps(
                sqs_policy_allow_sns(_qarn(ACCT_B, qname), topic))}}, qurl)
        assert _resp_code(_as(ACCT_A, sns_svc._publish, {
            "TopicArn": topic, "Message": "with-queue-policy"})) == (200, "")
        bodies = _queue_bodies(_qarn(ACCT_B, qname))
        assert len(bodies) == 1
        assert "with-queue-policy" in bodies[0]

    def test_cross_account_subscribe_denied_without_policy(self, auth):
        topic = _make_topic(ACCT_A, _uniq("xpol-xfan"))
        assert _resp_code(_as(ACCT_B, sns_svc._subscribe, {
            "TopicArn": topic, "Protocol": "email",
            "Endpoint": "x@example.com"})) == (403, "AuthorizationError")


# ---------------------------------------------------------------------------
# AUTH=false stays permissive
# ---------------------------------------------------------------------------

class TestAuthDisabledIsPermissive:
    def test_cross_account_send_passes_without_policy(self, noauth):
        name = _uniq("xpol-q")
        url = _make_queue(ACCT_A, name)
        _as(ACCT_B, sqs_svc._act_send_message,
            {"QueueUrl": url, "MessageBody": "open"}, url)
        assert _queue_bodies(_qarn(ACCT_A, name)) == ["open"]

    def test_cross_account_publish_passes_without_policy(self, noauth):
        arn = _make_topic(ACCT_A, _uniq("xpol-t"))
        assert _resp_code(_as(ACCT_B, sns_svc._publish, {
            "TopicArn": arn, "Message": "open"})) == (200, "")

    def test_explicit_deny_ignored(self, noauth):
        name = _uniq("xpol-q")
        url = _make_queue(ACCT_A, name, _deny_all(
            _qarn(ACCT_A, name), "sqs:SendMessage"))
        _as(ACCT_A, sqs_svc._act_send_message,
            {"QueueUrl": url, "MessageBody": "deny-ignored"}, url)
        assert _queue_bodies(_qarn(ACCT_A, name)) == ["deny-ignored"]

    def test_get_queue_url_ignores_deny(self, noauth):
        name = _uniq("xpol-q")
        url = _make_queue(ACCT_A, name, _deny_all(
            _qarn(ACCT_A, name), "sqs:GetQueueUrl"))
        assert _as(ACCT_A, sqs_svc._act_get_queue_url,
                   {"QueueName": name}, "") == {"QueueUrl": url}

    def test_s3_put_and_delivery_without_policy(self, noauth):
        bucket, _url, arn = _bucket_and_queue("xpol")
        assert _resp_code(_put_notif(ACCT_A, bucket, "sqs", arn)) == (200, "")
        bodies = _queue_bodies(arn)
        assert len(bodies) == 1
        assert "s3:TestEvent" in bodies[0]

    def test_fanout_without_policy(self, noauth):
        topic, _qurl, qarn = _topic_and_queue("xpol-fan")
        assert _resp_code(_as(ACCT_A, sns_svc._publish, {
            "TopicArn": topic, "Message": "open"})) == (200, "")
        assert len(_queue_bodies(qarn)) == 1
