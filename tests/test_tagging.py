import pytest
from botocore.exceptions import ClientError

# ========== Resource Groups Tagging API ==========

# Unique tag key scopes all resources to this test file — avoids collisions with other tests
_TAG_KEY = "tagging-test"


# ========== S3 ==========

def test_tagging_get_resources_s3_basic(tagging, s3):
    s3.create_bucket(Bucket="tg-s3-basic")
    s3.put_bucket_tagging(Bucket="tg-s3-basic", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "s3-basic"}]
    })

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["s3-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert "arn:aws:s3:::tg-s3-basic" in arns


def test_tagging_get_resources_s3_tags_returned(tagging, s3):
    s3.create_bucket(Bucket="tg-s3-tags")
    s3.put_bucket_tagging(Bucket="tg-s3-tags", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "s3-tags"}, {"Key": "team", "Value": "platform"}]
    })

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["s3-tags"]}])
    matched = [r for r in resp["ResourceTagMappingList"] if r["ResourceARN"] == "arn:aws:s3:::tg-s3-tags"]
    assert len(matched) == 1
    tag_map = {t["Key"]: t["Value"] for t in matched[0]["Tags"]}
    assert tag_map[_TAG_KEY] == "s3-tags"
    assert tag_map["team"] == "platform"


# ========== SQS ==========

def test_tagging_get_resources_sqs(tagging, sqs):
    url = sqs.create_queue(QueueName="tg-sqs-basic")["QueueUrl"]
    sqs.tag_queue(QueueUrl=url, Tags={_TAG_KEY: "sqs-basic"})

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["sqs-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any("tg-sqs-basic" in a for a in arns)


# ========== SNS ==========

def test_tagging_get_resources_sns(tagging, sns):
    topic_arn = sns.create_topic(Name="tg-sns-basic")["TopicArn"]
    sns.tag_resource(ResourceArn=topic_arn, Tags=[{"Key": _TAG_KEY, "Value": "sns-basic"}])

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["sns-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert topic_arn in arns


# ========== DynamoDB ==========

def test_tagging_get_resources_dynamodb(tagging, ddb):
    ddb.create_table(
        TableName="tg-ddb-basic",
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table_arn = ddb.describe_table(TableName="tg-ddb-basic")["Table"]["TableArn"]
    ddb.tag_resource(ResourceArn=table_arn, Tags=[{"Key": _TAG_KEY, "Value": "ddb-basic"}])

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["ddb-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert table_arn in arns


# ========== Cross-service fan-out ==========

def test_tagging_get_resources_cross_service(tagging, s3, sqs):
    s3.create_bucket(Bucket="tg-cross-s3")
    s3.put_bucket_tagging(Bucket="tg-cross-s3", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "cross-svc"}]
    })
    url = sqs.create_queue(QueueName="tg-cross-sqs")["QueueUrl"]
    sqs.tag_queue(QueueUrl=url, Tags={_TAG_KEY: "cross-svc"})

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["cross-svc"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert "arn:aws:s3:::tg-cross-s3" in arns
    assert any("tg-cross-sqs" in a for a in arns)


# ========== Tag filter semantics ==========

def test_tagging_get_resources_tag_filter_or_values(tagging, s3):
    """Values list within a TagFilter uses OR — either value matches."""
    s3.create_bucket(Bucket="tg-or-prod")
    s3.put_bucket_tagging(Bucket="tg-or-prod", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "or-prod"}]
    })
    s3.create_bucket(Bucket="tg-or-staging")
    s3.put_bucket_tagging(Bucket="tg-or-staging", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "or-staging"}]
    })
    s3.create_bucket(Bucket="tg-or-other")
    s3.put_bucket_tagging(Bucket="tg-or-other", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "or-other"}]
    })

    resp = tagging.get_resources(
        TagFilters=[{"Key": _TAG_KEY, "Values": ["or-prod", "or-staging"]}]
    )
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert "arn:aws:s3:::tg-or-prod" in arns
    assert "arn:aws:s3:::tg-or-staging" in arns
    assert "arn:aws:s3:::tg-or-other" not in arns


def test_tagging_get_resources_tag_filter_and_keys(tagging, s3):
    """Multiple TagFilters use AND — resource must match all keys."""
    s3.create_bucket(Bucket="tg-and-both")
    s3.put_bucket_tagging(Bucket="tg-and-both", Tagging={
        "TagSet": [
            {"Key": _TAG_KEY, "Value": "and-match"},
            {"Key": "and-extra-key", "Value": "and-extra-val"},
        ]
    })
    s3.create_bucket(Bucket="tg-and-one")
    s3.put_bucket_tagging(Bucket="tg-and-one", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "and-match"}]
    })

    resp = tagging.get_resources(TagFilters=[
        {"Key": _TAG_KEY, "Values": ["and-match"]},
        {"Key": "and-extra-key", "Values": ["and-extra-val"]},
    ])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert "arn:aws:s3:::tg-and-both" in arns
    assert "arn:aws:s3:::tg-and-one" not in arns


# ========== ResourceTypeFilters ==========

def test_tagging_get_resources_resource_type_filter_s3_only(tagging, s3, sqs):
    s3.create_bucket(Bucket="tg-type-s3")
    s3.put_bucket_tagging(Bucket="tg-type-s3", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "type-filter"}]
    })
    url = sqs.create_queue(QueueName="tg-type-sqs")["QueueUrl"]
    sqs.tag_queue(QueueUrl=url, Tags={_TAG_KEY: "type-filter"})

    resp = tagging.get_resources(
        TagFilters=[{"Key": _TAG_KEY, "Values": ["type-filter"]}],
        ResourceTypeFilters=["s3"],
    )
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert "arn:aws:s3:::tg-type-s3" in arns
    assert not any("tg-type-sqs" in a for a in arns)


# ========== Edge cases ==========

def test_tagging_get_resources_no_match(tagging):
    resp = tagging.get_resources(
        TagFilters=[{"Key": _TAG_KEY, "Values": ["__nonexistent__"]}]
    )
    assert resp["ResourceTagMappingList"] == []


def test_tagging_get_resources_pagination_token_empty(tagging):
    resp = tagging.get_resources()
    assert resp.get("PaginationToken", "") == ""
