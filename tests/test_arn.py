import pytest

from ministack.core.arn import Arn, ArnParseError, is_arn, parse_arn


def test_parse_arn_rejects_invalid_prefix():
    with pytest.raises(ArnParseError, match="arn: invalid prefix"):
        parse_arn("invalid")


def test_parse_arn_rejects_too_few_sections():
    with pytest.raises(ArnParseError, match="arn: not enough sections"):
        parse_arn("arn:nope")


@pytest.mark.parametrize(
    "value, expected",
    [
        (
            "arn:aws:ecr:us-west-2:123456789012:repository/foo/bar",
            Arn(
                partition="aws",
                service="ecr",
                region="us-west-2",
                account_id="123456789012",
                resource="repository/foo/bar",
            ),
        ),
        (
            "arn:aws:elasticbeanstalk:us-east-1:123456789012:environment/My App/MyEnvironment",
            Arn(
                partition="aws",
                service="elasticbeanstalk",
                region="us-east-1",
                account_id="123456789012",
                resource="environment/My App/MyEnvironment",
            ),
        ),
        (
            "arn:aws:iam::123456789012:user/David",
            Arn(
                partition="aws",
                service="iam",
                region="",
                account_id="123456789012",
                resource="user/David",
            ),
        ),
        (
            "arn:aws:rds:eu-west-1:123456789012:db:mysql-db",
            Arn(
                partition="aws",
                service="rds",
                region="eu-west-1",
                account_id="123456789012",
                resource="db:mysql-db",
            ),
        ),
        (
            "arn:aws:s3:::my_corporate_bucket/exampleobject.png",
            Arn(
                partition="aws",
                service="s3",
                region="",
                account_id="",
                resource="my_corporate_bucket/exampleobject.png",
            ),
        ),
    ],
)
def test_parse_arn_matches_aws_sdk_go_v2_examples(value, expected):
    assert parse_arn(value) == expected


def test_parse_arn_preserves_resource_tail_colons():
    spec = parse_arn("arn:aws:lambda:us-east-1:123456789012:function:my-func:live")

    assert spec.region == "us-east-1"
    assert spec.account_id == "123456789012"
    assert spec.resource == "function:my-func:live"
    assert str(spec) == "arn:aws:lambda:us-east-1:123456789012:function:my-func:live"


@pytest.mark.parametrize(
    "value, expected",
    [
        ("arn:aws:service:us-west-2:123456789012:restype/resvalue", True),
        ("arn:aws:service:us-west-2:123456789012:restype:resvalue", True),
        ("arn:aws:service:us-west-2:123456789012:*", True),
        ("arn:::::", True),
        ("some random string", False),
        ("arn:aws:service:us-west-2:123456789012", False),
        (None, False),
    ],
)
def test_is_arn_matches_aws_sdk_go_v2_shape_check(value, expected):
    assert is_arn(value) is expected
