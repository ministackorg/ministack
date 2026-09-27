"""SQS queue-policy and SNS topic-policy enforcement.

The Policy attributes were stored (CRUD) but never evaluated: S3→SQS and
SNS→SQS deliveries landed regardless of the destination policy, and
cross-account send/publish failed (or passed) regardless of it. These tests
pin the enforced behaviour:

- Same-account API calls pass unless the policy carries an explicit Deny.
- Cross-account API calls need an explicit Allow (AccessDenied /
  AuthorizationError otherwise).
- S3→SQS, S3→SNS and SNS→SQS deliveries need an explicit Allow for the
  calling service in the destination policy (with its SourceArn/SourceAccount
  conditions); without one the delivery is dropped, like real AWS.
- S3 refuses a notification config whose SQS/SNS destination denies it
  (InvalidArgument), unless SkipDestinationValidation skips the probe.
"""

import json
import time
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from ministack.core.iam_evaluator import (
    EvalContext,
    evaluate_resource_policy,
)
from tests.conftest import (
    ENDPOINT,
    REGION,
    sqs_policy_allow_s3,
    sqs_policy_allow_sns,
)

ACCT_A = "111111111111"
ACCT_B = "222222222222"


def _client(service, access_key):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key="test",
        region_name=REGION,
        config=Config(region_name=REGION, retries={"max_attempts": 0}),
    )


def _uniq(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _recv_all(sqs, qurl):
    out = []
    for _ in range(3):
        msgs = sqs.receive_message(QueueUrl=qurl, MaxNumberOfMessages=10).get("Messages", [])
        out.extend(msgs)
        if not msgs:
            break
    return out


def _wait_for(sqs, qurl, predicate, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        msgs = _recv_all(sqs, qurl)
        if predicate(msgs):
            return msgs
        time.sleep(0.3)
    return []


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

def _queue_arn(sqs, qurl):
    return sqs.get_queue_attributes(
        QueueUrl=qurl, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]


class TestCrossAccountSqs:
    def test_send_denied_without_policy(self):
        sqs_a = _client("sqs", ACCT_A)
        sqs_b = _client("sqs", ACCT_B)
        qurl = sqs_a.create_queue(QueueName=_uniq("xpol-q"))["QueueUrl"]
        with pytest.raises(ClientError) as exc:
            sqs_b.send_message(QueueUrl=qurl, MessageBody="no-policy")
        assert exc.value.response["Error"]["Code"] == "AccessDenied"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403

    def test_send_allowed_with_policy(self):
        sqs_a = _client("sqs", ACCT_A)
        sqs_b = _client("sqs", ACCT_B)
        qurl = sqs_a.create_queue(QueueName=_uniq("xpol-q"))["QueueUrl"]
        arn = _queue_arn(sqs_a, qurl)
        sqs_a.set_queue_attributes(QueueUrl=qurl, Attributes={"Policy": json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Sid": "x", "Effect": "Allow",
                           "Principal": {"AWS": ACCT_B},
                           "Action": "sqs:SendMessage", "Resource": arn}]})})
        sqs_b.send_message(QueueUrl=qurl, MessageBody="with-policy")
        msgs = _recv_all(sqs_a, qurl)
        assert [m["Body"] for m in msgs] == ["with-policy"]

    def test_send_allowed_via_add_permission(self):
        sqs_a = _client("sqs", ACCT_A)
        sqs_b = _client("sqs", ACCT_B)
        qurl = sqs_a.create_queue(QueueName=_uniq("xpol-q"))["QueueUrl"]
        sqs_a.add_permission(QueueUrl=qurl, Label="xacct",
                             AWSAccountIds=[ACCT_B], Actions=["SendMessage"])
        sqs_b.send_message(QueueUrl=qurl, MessageBody="via-add-permission")
        assert _recv_all(sqs_a, qurl)[0]["Body"] == "via-add-permission"

    def test_explicit_deny_blocks_same_account(self):
        sqs_a = _client("sqs", ACCT_A)
        qurl = sqs_a.create_queue(QueueName=_uniq("xpol-q"))["QueueUrl"]
        arn = _queue_arn(sqs_a, qurl)
        sqs_a.set_queue_attributes(QueueUrl=qurl, Attributes={"Policy": json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Sid": "deny", "Effect": "Deny",
                           "Principal": {"AWS": "*"},
                           "Action": "sqs:SendMessage", "Resource": arn}]})})
        with pytest.raises(ClientError) as exc:
            sqs_a.send_message(QueueUrl=qurl, MessageBody="deny-me")
        assert exc.value.response["Error"]["Code"] == "AccessDenied"

    def test_get_queue_url_with_owner_account(self):
        sqs_a = _client("sqs", ACCT_A)
        sqs_b = _client("sqs", ACCT_B)
        name = _uniq("xpol-q")
        qurl = sqs_a.create_queue(QueueName=name)["QueueUrl"]
        arn = _queue_arn(sqs_a, qurl)
        sqs_a.set_queue_attributes(QueueUrl=qurl, Attributes={"Policy": json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Sid": "x", "Effect": "Allow",
                           "Principal": {"AWS": ACCT_B},
                           "Action": "sqs:GetQueueUrl", "Resource": arn}]})})
        found = sqs_b.get_queue_url(QueueName=name, QueueOwnerAWSAccountId=ACCT_A)["QueueUrl"]
        assert found.rstrip("/").split("/")[-1] == name


# ---------------------------------------------------------------------------
# Cross-account SNS (+ AddPermission/RemovePermission)
# ---------------------------------------------------------------------------

class TestCrossAccountSns:
    def test_publish_denied_by_default(self):
        sns_a = _client("sns", ACCT_A)
        sns_b = _client("sns", ACCT_B)
        arn = sns_a.create_topic(Name=_uniq("xpol-t"))["TopicArn"]
        with pytest.raises(ClientError) as exc:
            sns_b.publish(TopicArn=arn, Message="no-grant")
        assert exc.value.response["Error"]["Code"] == "AuthorizationError"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403

    def test_publish_allowed_with_policy(self):
        sns_a = _client("sns", ACCT_A)
        sns_b = _client("sns", ACCT_B)
        arn = sns_a.create_topic(Name=_uniq("xpol-t"))["TopicArn"]
        sns_a.set_topic_attributes(TopicArn=arn, AttributeName="Policy",
                                   AttributeValue=json.dumps({
                                       "Version": "2012-10-17",
                                       "Statement": [{
                                           "Sid": "x", "Effect": "Allow",
                                           "Principal": {"AWS": ACCT_B},
                                           "Action": "sns:Publish",
                                           "Resource": arn}]}))
        assert sns_b.publish(TopicArn=arn, Message="granted")["MessageId"]

    def test_explicit_deny_blocks_same_account(self):
        sns_a = _client("sns", ACCT_A)
        arn = sns_a.create_topic(Name=_uniq("xpol-t"))["TopicArn"]
        sns_a.set_topic_attributes(TopicArn=arn, AttributeName="Policy",
                                   AttributeValue=json.dumps({
                                       "Version": "2012-10-17",
                                       "Statement": [{
                                           "Sid": "deny", "Effect": "Deny",
                                           "Principal": {"AWS": "*"},
                                           "Action": "sns:Publish",
                                           "Resource": arn}]}))
        with pytest.raises(ClientError) as exc:
            sns_a.publish(TopicArn=arn, Message="deny-me")
        assert exc.value.response["Error"]["Code"] == "AuthorizationError"

    def test_add_permission_grants_cross_account_publish(self):
        sns_a = _client("sns", ACCT_A)
        sns_b = _client("sns", ACCT_B)
        arn = sns_a.create_topic(Name=_uniq("xpol-t"))["TopicArn"]
        sns_a.add_permission(TopicArn=arn, Label="perm-1",
                             AWSAccountId=[ACCT_B], ActionName=["Publish"])
        policy = json.loads(sns_a.get_topic_attributes(
            TopicArn=arn)["Attributes"]["Policy"])
        stmt = next(s for s in policy["Statement"] if s.get("Sid") == "perm-1")
        assert ACCT_B in stmt["Principal"]["AWS"]
        assert "sns:Publish" in stmt["Action"]
        assert sns_b.publish(TopicArn=arn, Message="via-add-permission")["MessageId"]

    def test_add_permission_rejects_duplicate_label(self):
        sns_a = _client("sns", ACCT_A)
        arn = sns_a.create_topic(Name=_uniq("xpol-t"))["TopicArn"]
        sns_a.add_permission(TopicArn=arn, Label="dup",
                             AWSAccountId=[ACCT_B], ActionName=["Publish"])
        with pytest.raises(ClientError) as exc:
            sns_a.add_permission(TopicArn=arn, Label="dup",
                                 AWSAccountId=[ACCT_B], ActionName=["Publish"])
        assert exc.value.response["Error"]["Code"] == "InvalidParameterException"

    def test_cross_account_tag_access_needs_grant(self):
        sns_a = _client("sns", ACCT_A)
        sns_b = _client("sns", ACCT_B)
        arn = sns_a.create_topic(Name=_uniq("xpol-t"))["TopicArn"]
        with pytest.raises(ClientError) as exc:
            sns_b.list_tags_for_resource(ResourceArn=arn)
        assert exc.value.response["Error"]["Code"] == "AuthorizationError"
        sns_a.set_topic_attributes(TopicArn=arn, AttributeName="Policy",
                                   AttributeValue=json.dumps({
                                       "Version": "2012-10-17",
                                       "Statement": [{
                                           "Sid": "tags",
                                           "Effect": "Allow",
                                           "Principal": {"AWS": ACCT_B},
                                           "Action": ["sns:TagResource",
                                                      "sns:ListTagsForResource"],
                                           "Resource": arn}]}))
        sns_b.tag_resource(ResourceArn=arn, Tags=[{"Key": "k", "Value": "v"}])
        tags = sns_b.list_tags_for_resource(ResourceArn=arn)["Tags"]
        assert {"Key": "k", "Value": "v"} in tags

    def test_remove_permission_drops_statement(self):
        sns_a = _client("sns", ACCT_A)
        sns_b = _client("sns", ACCT_B)
        arn = sns_a.create_topic(Name=_uniq("xpol-t"))["TopicArn"]
        sns_a.add_permission(TopicArn=arn, Label="drop",
                             AWSAccountId=[ACCT_B], ActionName=["Publish"])
        assert sns_b.publish(TopicArn=arn, Message="before")["MessageId"]
        sns_a.remove_permission(TopicArn=arn, Label="drop")
        policy = json.loads(sns_a.get_topic_attributes(
            TopicArn=arn)["Attributes"]["Policy"])
        assert all(s.get("Sid") != "drop" for s in policy["Statement"])
        with pytest.raises(ClientError) as exc:
            sns_b.publish(TopicArn=arn, Message="after")
        assert exc.value.response["Error"]["Code"] == "AuthorizationError"


# ---------------------------------------------------------------------------
# S3 notifications gated on the destination policy
# ---------------------------------------------------------------------------

class TestS3NotificationPolicies:
    def _bucket_and_queue(self):
        s3_a = _client("s3", ACCT_A)
        sqs_a = _client("sqs", ACCT_A)
        bucket = _uniq("xpol-bkt")
        s3_a.create_bucket(Bucket=bucket)
        qurl = sqs_a.create_queue(QueueName=_uniq("xpol-s3q"))["QueueUrl"]
        arn = _queue_arn(sqs_a, qurl)
        return s3_a, sqs_a, bucket, qurl, arn

    def _put_queue_config(self, s3_a, bucket, arn, **kwargs):
        return s3_a.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={"QueueConfigurations": [
                {"QueueArn": arn, "Events": ["s3:ObjectCreated:*"]}]},
            **kwargs)

    def _assert_rejected(self, s3_a, bucket, arn):
        with pytest.raises(ClientError) as exc:
            self._put_queue_config(s3_a, bucket, arn)
        assert exc.value.response["Error"]["Code"] == "InvalidArgument"
        assert "Unable to validate the following destination configurations" in \
            exc.value.response["Error"]["Message"]

    def test_s3_put_rejected_without_queue_policy(self):
        s3_a, _sqs_a, bucket, _qurl, arn = self._bucket_and_queue()
        self._assert_rejected(s3_a, bucket, arn)

    def test_s3_put_rejected_by_unrelated_policy(self):
        s3_a, sqs_a, bucket, qurl, arn = self._bucket_and_queue()
        sqs_a.set_queue_attributes(QueueUrl=qurl, Attributes={"Policy": json.dumps({
            "Version": "2012-10-17", "Statement": [{
                "Sid": "other", "Effect": "Allow",
                "Principal": {"AWS": ACCT_B},
                "Action": "sqs:SendMessage", "Resource": arn}]})})
        self._assert_rejected(s3_a, bucket, arn)

    def test_s3_put_rejected_by_wrong_source_arn(self):
        s3_a, sqs_a, bucket, qurl, arn = self._bucket_and_queue()
        sqs_a.set_queue_attributes(
            QueueUrl=qurl,
            Attributes={"Policy": json.dumps(
                sqs_policy_allow_s3(arn, "no-such-bucket", ACCT_A))})
        self._assert_rejected(s3_a, bucket, arn)

    def test_s3_to_sqs_delivered_with_policy(self):
        s3_a, sqs_a, bucket, qurl, arn = self._bucket_and_queue()
        sqs_a.set_queue_attributes(
            QueueUrl=qurl,
            Attributes={"Policy": json.dumps(
                sqs_policy_allow_s3(arn, bucket, ACCT_A))})
        self._put_queue_config(s3_a, bucket, arn)
        sqs_a.purge_queue(QueueUrl=qurl)
        time.sleep(0.5)
        s3_a.put_object(Bucket=bucket, Key="k", Body=b"v")
        msgs = _wait_for(sqs_a, qurl,
                         lambda m: any("ObjectCreated" in x["Body"] for x in m))
        assert any("ObjectCreated" in x["Body"] for x in msgs)

    def test_s3_skip_validation_stores_but_delivery_blocked(self):
        s3_a, sqs_a, bucket, qurl, arn = self._bucket_and_queue()
        self._put_queue_config(s3_a, bucket, arn, SkipDestinationValidation=True)
        s3_a.put_object(Bucket=bucket, Key="k", Body=b"v")
        assert _wait_for(sqs_a, qurl, lambda m: len(m) >= 1, timeout=3) == []

    def _bucket_topic_queue(self):
        s3_a = _client("s3", ACCT_A)
        sns_a = _client("sns", ACCT_A)
        sqs_a = _client("sqs", ACCT_A)
        bucket = _uniq("xpol-bkt")
        s3_a.create_bucket(Bucket=bucket)
        topic = sns_a.create_topic(Name=_uniq("xpol-t"))["TopicArn"]
        qurl = sqs_a.create_queue(QueueName=_uniq("xpol-s3q"))["QueueUrl"]
        qarn = _queue_arn(sqs_a, qurl)
        sqs_a.set_queue_attributes(
            QueueUrl=qurl,
            Attributes={"Policy": json.dumps(sqs_policy_allow_sns(qarn, topic))})
        sns_a.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)
        return s3_a, sns_a, sqs_a, bucket, topic, qurl

    def _put_topic_config(self, s3_a, bucket, topic, **kwargs):
        return s3_a.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={"TopicConfigurations": [
                {"TopicArn": topic, "Events": ["s3:ObjectCreated:*"]}]},
            **kwargs)

    def test_s3_to_sns_default_policy_delivers(self):
        s3_a, _sns_a, sqs_a, bucket, topic, qurl = self._bucket_topic_queue()
        self._put_topic_config(s3_a, bucket, topic)
        msgs = _wait_for(sqs_a, qurl, lambda m: len(m) >= 1)
        assert len(msgs) >= 1

    def test_s3_put_rejected_by_topic_policy_without_s3(self):
        s3_a, sns_a, _sqs_a, bucket, topic, _qurl = self._bucket_topic_queue()
        sns_a.set_topic_attributes(TopicArn=topic, AttributeName="Policy",
                                   AttributeValue=json.dumps({
                                       "Version": "2012-10-17",
                                       "Statement": [{
                                           "Sid": "users-only",
                                           "Effect": "Allow",
                                           "Principal": {"AWS": ACCT_A},
                                           "Action": "sns:Publish",
                                           "Resource": topic}]}))
        with pytest.raises(ClientError) as exc:
            self._put_topic_config(s3_a, bucket, topic)
        assert exc.value.response["Error"]["Code"] == "InvalidArgument"
        assert "Unable to validate the following destination configurations" in \
            exc.value.response["Error"]["Message"]

    def test_s3_to_sns_skip_validation_delivery_blocked(self):
        s3_a, sns_a, sqs_a, bucket, topic, qurl = self._bucket_topic_queue()
        sns_a.set_topic_attributes(TopicArn=topic, AttributeName="Policy",
                                   AttributeValue=json.dumps({
                                       "Version": "2012-10-17",
                                       "Statement": [{
                                           "Sid": "users-only",
                                           "Effect": "Allow",
                                           "Principal": {"AWS": ACCT_A},
                                           "Action": "sns:Publish",
                                           "Resource": topic}]}))
        self._put_topic_config(s3_a, bucket, topic, SkipDestinationValidation=True)
        s3_a.put_object(Bucket=bucket, Key="k", Body=b"v")
        assert _wait_for(sqs_a, qurl, lambda m: len(m) >= 1, timeout=3) == []


# ---------------------------------------------------------------------------
# SNS→SQS fanout gated on the queue policy
# ---------------------------------------------------------------------------

class TestSnsFanoutPolicies:
    def _topic_and_queue(self):
        sns_a = _client("sns", ACCT_A)
        sqs_a = _client("sqs", ACCT_A)
        topic = sns_a.create_topic(Name=_uniq("xpol-fan"))["TopicArn"]
        qurl = sqs_a.create_queue(QueueName=_uniq("xpol-fanq"))["QueueUrl"]
        qarn = _queue_arn(sqs_a, qurl)
        sns_a.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)
        return sns_a, sqs_a, topic, qurl, qarn

    def test_fanout_blocked_without_policy(self):
        sns_a, sqs_a, topic, qurl, _qarn = self._topic_and_queue()
        sns_a.publish(TopicArn=topic, Message="no-policy")
        time.sleep(1.0)
        assert _recv_all(sqs_a, qurl) == []

    def test_fanout_blocked_by_wrong_source_arn(self):
        sns_a, sqs_a, topic, qurl, qarn = self._topic_and_queue()
        policy = sqs_policy_allow_sns(
            qarn, f"arn:aws:sns:{REGION}:{ACCT_A}:wrong-topic")
        sqs_a.set_queue_attributes(QueueUrl=qurl,
                                   Attributes={"Policy": json.dumps(policy)})
        sns_a.publish(TopicArn=topic, Message="wrong-source")
        time.sleep(1.0)
        assert _recv_all(sqs_a, qurl) == []

    def test_fanout_delivered_with_policy(self):
        sns_a, sqs_a, topic, qurl, qarn = self._topic_and_queue()
        sqs_a.set_queue_attributes(
            QueueUrl=qurl,
            Attributes={"Policy": json.dumps(sqs_policy_allow_sns(qarn, topic))})
        sns_a.publish(TopicArn=topic, Message="with-policy")
        msgs = _wait_for(sqs_a, qurl, lambda m: len(m) == 1)
        assert len(msgs) == 1

    def test_cross_account_fanout_needs_queue_policy(self):
        sns_a = _client("sns", ACCT_A)
        sns_b = _client("sns", ACCT_B)
        sqs_b = _client("sqs", ACCT_B)
        topic = sns_a.create_topic(Name=_uniq("xpol-xfan"))["TopicArn"]
        sns_a.set_topic_attributes(TopicArn=topic, AttributeName="Policy",
                                   AttributeValue=json.dumps({
                                       "Version": "2012-10-17",
                                       "Statement": [{
                                           "Sid": "sub", "Effect": "Allow",
                                           "Principal": {"AWS": ACCT_B},
                                           "Action": "sns:Subscribe",
                                           "Resource": topic}]}))
        qurl = sqs_b.create_queue(QueueName=_uniq("xpol-xfanq"))["QueueUrl"]
        qarn = _queue_arn(sqs_b, qurl)
        sns_b.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=qarn)
        sns_a.publish(TopicArn=topic, Message="no-queue-policy")
        time.sleep(1.0)
        assert _recv_all(sqs_b, qurl) == []

        sqs_b.set_queue_attributes(
            QueueUrl=qurl,
            Attributes={"Policy": json.dumps(sqs_policy_allow_sns(qarn, topic))})
        sns_a.publish(TopicArn=topic, Message="with-queue-policy")
        msgs = _wait_for(sqs_b, qurl, lambda m: len(m) == 1)
        assert len(msgs) == 1

    def test_cross_account_subscribe_denied_without_policy(self):
        sns_a = _client("sns", ACCT_A)
        sns_b = _client("sns", ACCT_B)
        topic = sns_a.create_topic(Name=_uniq("xpol-xfan"))["TopicArn"]
        with pytest.raises(ClientError) as exc:
            sns_b.subscribe(TopicArn=topic, Protocol="email",
                            Endpoint="x@example.com")
        assert exc.value.response["Error"]["Code"] == "AuthorizationError"
