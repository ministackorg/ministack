"""Wire-fidelity assertions on response envelopes (issue #2128).

Expected values are real AWS observations, not model guesses:

- Error envelope shape comes from a 17-region capture sweep: the query
  family (sns/iam/sts/rds/elbv2/cloudformation/...) carries ``<Type>``
  inside ``<Error>`` and no ``<?xml?>`` declaration; ec2 carries the
  declaration, no ``<Type>``, and ``text/xml;charset=UTF-8``; rest-xml
  carries both the declaration and ``<Type>``.
- Success Content-Types were verified across
  us-east-1/us-west-2/eu-west-1/ap-southeast-2: query -> ``text/xml``,
  ec2 -> ``text/xml;charset=UTF-8``, json -> ``application/x-amz-json-
  1.{jsonVersion}``, rest-json -> ``application/json``, rest-xml ->
  ``application/xml`` (s3) or ``text/xml`` (route53).

These tests run the enduser surface: boto3 clients against the live
server, asserting exactly what an SDK caller parses back.
"""

import os
import urllib.error
import urllib.request

import pytest
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _ct(resp):
    """Response Content-Type as botocore exposes it (HTTPHeaderDict is
    case-insensitive)."""
    return resp["ResponseMetadata"]["HTTPHeaders"]["content-type"]


def _status(err):
    return err["ResponseMetadata"]["HTTPStatusCode"]


# ---------------------------------------------------------------------------
# Query-protocol error envelopes: <Type> inside <Error>, text/xml
# ---------------------------------------------------------------------------


def test_iam_query_error_envelope(iam):
    with pytest.raises(ClientError) as exc:
        iam.get_user(UserName="wire-missing-user")
    err = exc.value.response
    assert err["Error"]["Code"] == "NoSuchEntity"
    assert err["Error"]["Type"] == "Sender"
    assert _status(err) == 404
    assert _ct(err) == "text/xml"


def test_sns_query_error_envelope(sns):
    # set_topic_attributes was not part of the captured auth-ordering
    # change, so it keeps its NotFound/404 on a missing topic — but the
    # envelope still carries the query family's <Type>Sender</Type>.
    with pytest.raises(ClientError) as exc:
        sns.set_topic_attributes(
            TopicArn="arn:aws:sns:us-east-1:000000000000:wire-missing",
            AttributeName="DisplayName",
            AttributeValue="x",
        )
    err = exc.value.response
    assert err["Error"]["Code"] == "NotFound"
    assert err["Error"]["Type"] == "Sender"
    assert _ct(err) == "text/xml"


def test_elbv2_query_error_envelope(elbv2):
    with pytest.raises(ClientError) as exc:
        elbv2.describe_load_balancers(Names=["wire-missing"])
    err = exc.value.response
    assert err["Error"]["Code"] == "LoadBalancerNotFound"
    assert err["Error"]["Type"] == "Sender"
    assert _ct(err) == "text/xml"


def test_autoscaling_query_error_envelope(autoscaling):
    with pytest.raises(ClientError) as exc:
        autoscaling.update_auto_scaling_group(
            AutoScalingGroupName="wire-missing")
    err = exc.value.response
    assert err["Error"]["Code"] == "ValidationError"
    assert err["Error"]["Type"] == "Sender"
    assert _ct(err) == "text/xml"


def test_elasticache_query_error_envelope(ec):
    with pytest.raises(ClientError) as exc:
        ec.describe_cache_clusters(CacheClusterId="wire-missing")
    err = exc.value.response
    assert err["Error"]["Code"] == "CacheClusterNotFound"
    assert err["Error"]["Type"] == "Sender"
    assert _ct(err) == "text/xml"


def test_rds_query_error_envelope(rds):
    with pytest.raises(ClientError) as exc:
        rds.describe_db_instances(DBInstanceIdentifier="wire-missing")
    err = exc.value.response
    assert err["Error"]["Code"] == "DBInstanceNotFound"
    assert err["Error"]["Type"] == "Sender"
    assert _status(err) == 404
    assert _ct(err) == "text/xml"


def test_docdb_query_error_envelope(docdb):
    with pytest.raises(ClientError) as exc:
        docdb.describe_db_instances(DBInstanceIdentifier="wire-missing")
    err = exc.value.response
    assert err["Error"]["Type"] == "Sender"
    assert _ct(err) == "text/xml"


def test_cloudwatch_query_error_envelope():
    # boto3 1.42+ speaks smithy-rpc-v2-cbor to CloudWatch, so the legacy
    # Query API path is exercised with a raw form-encoded request — the
    # shape an XML-mode client still parses.
    req = urllib.request.Request(
        f"{ENDPOINT}/",
        data=b"Action=GetDashboard&Version=2010-08-01&DashboardName=wire-missing",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/"
                             "us-east-1/monitoring/aws4_request, "
                             "SignedHeaders=host, Signature=x",
        },
    )
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(req)
    http_err = exc.value
    body = http_err.read().decode()
    assert http_err.headers["content-type"] == "text/xml"
    # Query family: <Type> inside <Error>, no <?xml?> declaration.
    assert not body.startswith("<?xml")
    assert "<Type>Sender</Type>" in body
    assert "ResourceNotFound" in body


def test_ses_query_error_envelope(ses):
    with pytest.raises(ClientError) as exc:
        ses.describe_receipt_rule_set(RuleSetName="wire-missing")
    err = exc.value.response
    assert err["Error"]["Code"] == "RuleSetDoesNotExist"
    assert err["Error"]["Type"] == "Sender"
    assert _ct(err) == "text/xml"


def test_cfn_query_error_envelope(cfn):
    with pytest.raises(ClientError) as exc:
        cfn.describe_stacks(StackName="wire-missing")
    err = exc.value.response
    assert err["Error"]["Code"] == "ValidationError"
    assert err["Error"]["Type"] == "Sender"
    assert _ct(err) == "text/xml"


# ---------------------------------------------------------------------------
# Non-query envelopes: ec2 keeps its shape, rest-xml keeps <Type>
# ---------------------------------------------------------------------------


def test_ec2_error_envelope_has_no_type_and_charset(ec2):
    with pytest.raises(ClientError) as exc:
        ec2.describe_instances(InstanceIds=["i-0123456789abcdef0"])
    err = exc.value.response
    assert err["Error"]["Code"] == "InvalidInstanceID.NotFound"
    # ec2 errors carry no <Type> on the real wire (0/17 captures).
    assert err["Error"].get("Type") is None
    assert _ct(err) == "text/xml;charset=UTF-8"


def test_route53_restxml_error_envelope(r53):
    with pytest.raises(ClientError) as exc:
        r53.get_hosted_zone(Id="/hostedzone/ZWIREMISSING000")
    err = exc.value.response
    assert err["Error"]["Type"] == "Sender"
    assert _ct(err) == "text/xml"


# ---------------------------------------------------------------------------
# Success Content-Types per protocol family
# ---------------------------------------------------------------------------


def test_query_success_content_types(iam, sns, sts, ec, autoscaling,
                                     elbv2, ses, rds, docdb, cfn):
    calls = [
        (iam.list_users, {}),
        (sns.list_topics, {}),
        (sts.get_caller_identity, {}),
        (ec.describe_cache_clusters, {}),
        (autoscaling.describe_auto_scaling_groups, {}),
        (elbv2.describe_load_balancers, {}),
        (ses.list_identities, {}),
        (rds.describe_db_instances, {}),
        (docdb.describe_db_instances, {}),
        (cfn.list_stacks, {}),
    ]
    for call, kwargs in calls:
        assert _ct(call(**kwargs)) == "text/xml", call.__name__


def test_cloudwatch_query_success_content_type():
    # Raw Query API request — see test_cloudwatch_query_error_envelope.
    req = urllib.request.Request(
        f"{ENDPOINT}/",
        data=b"Action=ListMetrics&Version=2010-08-01",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/"
                             "us-east-1/monitoring/aws4_request, "
                             "SignedHeaders=host, Signature=x",
        },
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.headers["content-type"] == "text/xml"


def test_json_success_content_types(ddb, sqs, kin):
    # jsonVersion comes from the botocore service model (dynamodb/sqs are
    # 1.0, kinesis is 1.1) — matching what real AWS puts on the wire.
    assert _ct(ddb.list_tables()) == "application/x-amz-json-1.0"
    assert _ct(sqs.list_queues()) == "application/x-amz-json-1.0"
    assert _ct(kin.list_streams()) == "application/x-amz-json-1.1"


def test_restxml_success_content_types(s3, r53):
    assert _ct(s3.list_buckets()) == "application/xml"
    assert _ct(r53.list_hosted_zones()) == "text/xml"


def test_restjson_success_content_type(lam):
    assert _ct(lam.list_functions()) == "application/json"


def test_ec2_success_content_type(ec2):
    assert _ct(ec2.describe_regions()) == "text/xml;charset=UTF-8"


def test_s3_object_payload_content_type_not_rewritten(s3):
    # A stored object's Content-Type is payload, not an envelope — the
    # success fixer must not rewrite text/xml into the s3 envelope value.
    bucket = "wire-ct-payload"
    s3.create_bucket(Bucket=bucket)
    try:
        s3.put_object(Bucket=bucket, Key="doc.xml", Body=b"<a/>",
                      ContentType="text/xml")
        resp = s3.get_object(Bucket=bucket, Key="doc.xml")
        assert _ct(resp) == "text/xml"
    finally:
        s3.delete_object(Bucket=bucket, Key="doc.xml")
        s3.delete_bucket(Bucket=bucket)


# ---------------------------------------------------------------------------
# Per-service error-code ordering (real AWS captures)
# ---------------------------------------------------------------------------


def test_sfn_describe_state_machine_authz_before_existence(sfn):
    # Real AWS evaluates authorization before existence: a nonexistent
    # arn answers AccessDeniedException, not StateMachineDoesNotExist.
    with pytest.raises(ClientError) as exc:
        sfn.describe_state_machine(
            stateMachineArn="arn:aws:states:us-east-1:000000000000:"
                            "stateMachine:wire-missing")
    err = exc.value.response
    assert err["Error"]["Code"] == "AccessDeniedException"
    assert _status(err) == 400


def test_sns_get_topic_attributes_auth_before_existence(sns):
    # Real AWS answers InvalidClientTokenId 403 on a nonexistent topic —
    # auth is evaluated before existence (real-AWS capture).
    with pytest.raises(ClientError) as exc:
        sns.get_topic_attributes(
            TopicArn="arn:aws:sns:us-east-1:000000000000:wire-missing")
    err = exc.value.response
    assert err["Error"]["Code"] == "InvalidClientTokenId"
    assert err["Error"]["Type"] == "Sender"
    assert _status(err) == 403
    assert _ct(err) == "text/xml"
