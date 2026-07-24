import io
import os
import uuid
import uuid as _uuid_mod
import zipfile

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

# ========== Resource Groups Tagging API ==========

# Unique tag key scopes all resources to this test file — avoids collisions with other tests
_TAG_KEY = "tagging-test"


def _regional_client(service, region):
    return boto3.client(
        service,
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


def _account_client(service, account, region="us-east-1"):
    return boto3.client(
        service,
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        region_name=region,
        aws_access_key_id=account,
        aws_secret_access_key="test",
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


def _lambda_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", "def handler(event, context):\n    return 'ok'\n")
    return buf.getvalue()


def _uid():
    return _uuid_mod.uuid4().hex[:8]


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


def test_tagging_get_resources_resource_arn_list_filters_to_requested_resources(tagging, s3):
    s3.create_bucket(Bucket="tg-get-arn-list-a")
    s3.create_bucket(Bucket="tg-get-arn-list-b")
    s3.put_bucket_tagging(Bucket="tg-get-arn-list-a", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "arn-list"}]
    })
    s3.put_bucket_tagging(Bucket="tg-get-arn-list-b", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "arn-list"}]
    })

    arn = "arn:aws:s3:::tg-get-arn-list-a"
    resp = tagging.get_resources(ResourceARNList=[arn])

    assert [r["ResourceARN"] for r in resp["ResourceTagMappingList"]] == [arn]


def test_tagging_get_resources_resource_arn_list_malformed_arn_fails_request(tagging):
    with pytest.raises(ClientError) as exc:
        tagging.get_resources(ResourceARNList=["not-an-arn"])

    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_tagging_get_resources_resource_arn_list_rejects_filters(tagging, s3):
    s3.create_bucket(Bucket="tg-get-arn-list-filter")
    arn = "arn:aws:s3:::tg-get-arn-list-filter"

    with pytest.raises(ClientError) as exc:
        tagging.get_resources(
            ResourceARNList=[arn],
            TagFilters=[{"Key": _TAG_KEY, "Values": ["invalid-combination"]}],
        )

    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_tagging_get_resources_resource_arn_list_omits_unresolved_well_formed_arns(tagging):
    resp = tagging.get_resources(ResourceARNList=[
        "arn:aws:sns:us-west-2:000000000000:foreign-region-topic",
        "arn:aws:sns:us-east-1:999999999999:bogus-account-topic",
        "arn:aws:notaservice:us-east-1:000000000000:thing/missing",
        "arn:aws:sns:us-east-1:000000000000:missing-topic",
    ])

    assert resp["ResourceTagMappingList"] == []


# ========== Phase 2: New service collectors ==========

def test_tagging_get_resources_kms(tagging, kms_client):
    key_id = kms_client.create_key(Description="tg-kms-basic")["KeyMetadata"]["KeyId"]
    kms_client.tag_resource(KeyId=key_id, Tags=[{"TagKey": _TAG_KEY, "TagValue": "kms-basic"}])

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["kms-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any(key_id in a for a in arns)


def test_tagging_get_resources_kms_tags_returned(tagging, kms_client):
    """KMS stores tags as TagKey/TagValue — verify normalised to Key/Value in response."""
    key_id = kms_client.create_key(Description="tg-kms-tags")["KeyMetadata"]["KeyId"]
    kms_client.tag_resource(KeyId=key_id, Tags=[
        {"TagKey": _TAG_KEY, "TagValue": "kms-tags"},
        {"TagKey": "team", "TagValue": "platform"},
    ])

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["kms-tags"]}])
    matched = [r for r in resp["ResourceTagMappingList"] if key_id in r["ResourceARN"]]
    assert len(matched) == 1
    tag_map = {t["Key"]: t["Value"] for t in matched[0]["Tags"]}
    assert tag_map[_TAG_KEY] == "kms-tags"
    assert tag_map["team"] == "platform"


def test_tagging_get_resources_ecr(tagging, ecr):
    ecr.create_repository(
        repositoryName="tg-ecr-basic",
        tags=[{"Key": _TAG_KEY, "Value": "ecr-basic"}],
    )

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["ecr-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any("tg-ecr-basic" in a for a in arns)


def test_tagging_get_resources_ecs(tagging, ecs):
    ecs.create_cluster(
        clusterName="tg-ecs-basic",
        tags=[{"key": _TAG_KEY, "value": "ecs-basic"}],
    )

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["ecs-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any("tg-ecs-basic" in a for a in arns)


def test_tagging_get_resources_ecs_tags_returned(tagging, ecs):
    """ECS stores tags as lowercase key/value — verify normalised to Key/Value in response."""
    ecs.create_cluster(clusterName="tg-ecs-tags", tags=[
        {"key": _TAG_KEY, "value": "ecs-tags"},
        {"key": "team", "value": "infra"},
    ])

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["ecs-tags"]}])
    matched = [r for r in resp["ResourceTagMappingList"] if "tg-ecs-tags" in r["ResourceARN"]]
    assert len(matched) == 1
    tag_map = {t["Key"]: t["Value"] for t in matched[0]["Tags"]}
    assert tag_map[_TAG_KEY] == "ecs-tags"
    assert tag_map["team"] == "infra"


def test_tagging_get_resources_glue(tagging, glue):
    glue.create_database(DatabaseInput={"Name": "tg-glue-db"})
    db_arn = glue.get_database(Name="tg-glue-db")["Database"].get(
        "DatabaseArn",
        "arn:aws:glue:us-east-1:000000000000:database/tg-glue-db",
    )
    glue.tag_resource(ResourceArn=db_arn, TagsToAdd={_TAG_KEY: "glue-basic"})

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["glue-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert db_arn in arns


def test_tagging_get_resources_cognito_idp(tagging, cognito_idp):
    pool_id = cognito_idp.create_user_pool(PoolName="tg-cognito-pool")["UserPool"]["Id"]
    pool_arn = cognito_idp.describe_user_pool(UserPoolId=pool_id)["UserPool"]["Arn"]
    cognito_idp.tag_resource(ResourceArn=pool_arn, Tags={_TAG_KEY: "cognito-idp-basic"})

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["cognito-idp-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any(pool_id in a for a in arns)


def test_tagging_get_resources_cognito_identity(tagging, cognito_identity):
    pool_id = cognito_identity.create_identity_pool(
        IdentityPoolName="tg-cognito-identity",
        AllowUnauthenticatedIdentities=False,
    )["IdentityPoolId"]
    cognito_identity.tag_resource(
        ResourceArn=f"arn:aws:cognito-identity:us-east-1:000000000000:identitypool/{pool_id}",
        Tags={_TAG_KEY: "cognito-identity-basic"},
    )

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["cognito-identity-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any(pool_id in a for a in arns)


def test_tagging_get_resources_appsync(tagging, appsync):
    api_id = appsync.create_graphql_api(
        name="tg-appsync-api",
        authenticationType="API_KEY",
        tags={_TAG_KEY: "appsync-basic"},
    )["graphqlApi"]["apiId"]
    api_arn = appsync.get_graphql_api(apiId=api_id)["graphqlApi"]["arn"]

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["appsync-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert api_arn in arns


def test_tagging_get_resources_appsync_is_region_scoped():
    east_appsync = _regional_client("appsync", "us-east-1")
    west_tagging = _regional_client("resourcegroupstaggingapi", "us-west-2")
    api = east_appsync.create_graphql_api(
        name="tg-appsync-regional",
        authenticationType="API_KEY",
        tags={_TAG_KEY: "appsync-east"},
    )["graphqlApi"]

    try:
        resp = west_tagging.get_resources(
            TagFilters=[{"Key": _TAG_KEY, "Values": ["appsync-east"]}]
        )
        arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
        assert api["arn"] not in arns
    finally:
        east_appsync.delete_graphql_api(apiId=api["apiId"])


def test_tagging_get_resources_scheduler(tagging, scheduler):
    scheduler.create_schedule(
        Name="tg-scheduler-sched",
        GroupName="default",
        ScheduleExpression="rate(1 hour)",
        Target={
            "Arn": "arn:aws:sqs:us-east-1:000000000000:dummy",
            "RoleArn": "arn:aws:iam::000000000000:role/dummy",
        },
        FlexibleTimeWindow={"Mode": "OFF"},
    )
    sched_arn = "arn:aws:scheduler:us-east-1:000000000000:schedule/default/tg-scheduler-sched"
    scheduler.tag_resource(ResourceArn=sched_arn, Tags=[{"Key": _TAG_KEY, "Value": "scheduler-basic"}])

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["scheduler-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any("tg-scheduler-sched" in a for a in arns)


def test_tagging_get_resources_cloudfront(tagging, cloudfront):
    dist_id = cloudfront.create_distribution(DistributionConfig={
        "CallerReference": "tg-cf-dist",
        "Origins": {"Quantity": 1, "Items": [{
            "Id": "o1",
            "DomainName": "example.com",
            "S3OriginConfig": {"OriginAccessIdentity": ""},
        }]},
        "DefaultCacheBehavior": {
            "TargetOriginId": "o1",
            "ViewerProtocolPolicy": "allow-all",
            "ForwardedValues": {"QueryString": False, "Cookies": {"Forward": "none"}},
            "MinTTL": 0,
        },
        "Comment": "",
        "Enabled": True,
    })["Distribution"]["Id"]
    dist_arn = cloudfront.get_distribution(Id=dist_id)["Distribution"]["ARN"]
    cloudfront.tag_resource(
        Resource=dist_arn,
        Tags={"Items": [{"Key": _TAG_KEY, "Value": "cf-basic"}]},
    )

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["cf-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert dist_arn in arns


def test_tagging_get_resources_efs(tagging, efs):
    fs_id = efs.create_file_system(
        Tags=[{"Key": _TAG_KEY, "Value": "efs-basic"}],
    )["FileSystemId"]

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["efs-basic"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any(fs_id in a for a in arns)


def test_tagging_get_resources_efs_access_point(tagging, efs):
    fs_id = efs.create_file_system()["FileSystemId"]
    ap_id = efs.create_access_point(
        FileSystemId=fs_id,
        Tags=[{"Key": _TAG_KEY, "Value": "efs-ap"}],
    )["AccessPointId"]

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["efs-ap"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any(ap_id in a for a in arns)


def test_tagging_get_resources_resource_type_filter_kms(tagging, kms_client, s3):
    """ResourceTypeFilters=["kms"] returns KMS resources and excludes S3."""
    key_id = kms_client.create_key(Description="tg-type-kms")["KeyMetadata"]["KeyId"]
    kms_client.tag_resource(KeyId=key_id, Tags=[{"TagKey": _TAG_KEY, "TagValue": "type-kms"}])
    s3.create_bucket(Bucket="tg-type-kms-s3")
    s3.put_bucket_tagging(Bucket="tg-type-kms-s3", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "type-kms"}]
    })

    resp = tagging.get_resources(
        TagFilters=[{"Key": _TAG_KEY, "Values": ["type-kms"]}],
        ResourceTypeFilters=["kms"],
    )
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any(key_id in a for a in arns)
    assert not any("tg-type-kms-s3" in a for a in arns)


def test_tagging_get_resources_cross_service_phase2(tagging, kms_client, ecr):
    """GetResources fan-out includes Phase 2 collectors (KMS + ECR)."""
    key_id = kms_client.create_key(Description="tg-cross2-kms")["KeyMetadata"]["KeyId"]
    kms_client.tag_resource(KeyId=key_id, Tags=[{"TagKey": _TAG_KEY, "TagValue": "cross-phase2"}])
    ecr.create_repository(
        repositoryName="tg-cross2-ecr",
        tags=[{"Key": _TAG_KEY, "Value": "cross-phase2"}],
    )

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["cross-phase2"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert any(key_id in a for a in arns)
    assert any("tg-cross2-ecr" in a for a in arns)


# ========== GetTagKeys ==========

def test_tagging_get_tag_keys_returns_known_key(tagging, s3):
    s3.create_bucket(Bucket="tg-keys-s3")
    s3.put_bucket_tagging(Bucket="tg-keys-s3", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "keys-test"}]
    })
    resp = tagging.get_tag_keys()
    assert _TAG_KEY in resp["TagKeys"]


def test_tagging_get_tag_keys_no_duplicates(tagging, s3):
    """Same key on multiple resources appears once."""
    s3.create_bucket(Bucket="tg-keys-dup-a")
    s3.put_bucket_tagging(Bucket="tg-keys-dup-a", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "v1"}]
    })
    s3.create_bucket(Bucket="tg-keys-dup-b")
    s3.put_bucket_tagging(Bucket="tg-keys-dup-b", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "v2"}]
    })
    resp = tagging.get_tag_keys()
    assert resp["TagKeys"].count(_TAG_KEY) == 1


def test_tagging_get_tag_keys_cross_service_phase2(tagging, kms_client):
    """GetTagKeys aggregates keys from Phase 2 collectors, not just Phase 1."""
    key_id = kms_client.create_key(Description="tg-keys-kms")["KeyMetadata"]["KeyId"]
    kms_client.tag_resource(KeyId=key_id, Tags=[{"TagKey": _TAG_KEY, "TagValue": "keys-kms"}])

    resp = tagging.get_tag_keys()
    assert _TAG_KEY in resp["TagKeys"]


def test_tagging_get_tag_keys_pagination_token_empty(tagging):
    resp = tagging.get_tag_keys()
    assert resp.get("PaginationToken", "") == ""


# ========== GetTagValues ==========

def test_tagging_get_tag_values_returns_values(tagging, s3):
    s3.create_bucket(Bucket="tg-vals-a")
    s3.put_bucket_tagging(Bucket="tg-vals-a", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "vals-v1"}]
    })
    s3.create_bucket(Bucket="tg-vals-b")
    s3.put_bucket_tagging(Bucket="tg-vals-b", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "vals-v2"}]
    })
    resp = tagging.get_tag_values(Key=_TAG_KEY)
    assert "vals-v1" in resp["TagValues"]
    assert "vals-v2" in resp["TagValues"]


def test_tagging_get_tag_values_excludes_other_keys(tagging, s3):
    s3.create_bucket(Bucket="tg-vals-other")
    s3.put_bucket_tagging(Bucket="tg-vals-other", Tagging={
        "TagSet": [{"Key": "other-key", "Value": "should-not-appear"}]
    })
    resp = tagging.get_tag_values(Key=_TAG_KEY)
    assert "should-not-appear" not in resp["TagValues"]


def test_tagging_get_tag_values_cross_service_phase2(tagging, s3, kms_client):
    """GetTagValues returns values sourced from both Phase 1 and Phase 2 collectors."""
    s3.create_bucket(Bucket="tg-vals-phase2-s3")
    s3.put_bucket_tagging(Bucket="tg-vals-phase2-s3", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "vals-from-s3"}]
    })
    key_id = kms_client.create_key(Description="tg-vals-kms")["KeyMetadata"]["KeyId"]
    kms_client.tag_resource(KeyId=key_id, Tags=[{"TagKey": _TAG_KEY, "TagValue": "vals-from-kms"}])

    resp = tagging.get_tag_values(Key=_TAG_KEY)
    assert "vals-from-s3" in resp["TagValues"]
    assert "vals-from-kms" in resp["TagValues"]


def test_tagging_get_tag_values_empty_for_unknown_key(tagging):
    resp = tagging.get_tag_values(Key="__nonexistent_key__")
    assert resp["TagValues"] == []


def test_tagging_get_tag_values_pagination_token_empty(tagging):
    resp = tagging.get_tag_values(Key=_TAG_KEY)
    assert resp.get("PaginationToken", "") == ""


# ========== TagResources ==========

def test_tagging_tag_resources_s3(tagging, s3):
    s3.create_bucket(Bucket="tg-tr-s3")
    arn = "arn:aws:s3:::tg-tr-s3"
    resp = tagging.tag_resources(ResourceARNList=[arn], Tags={_TAG_KEY: "tr-s3"})
    assert resp["FailedResourcesMap"] == {}
    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["tr-s3"]}])
    assert any(r["ResourceARN"] == arn for r in check["ResourceTagMappingList"])


def test_tagging_tag_resources_kms(tagging, kms_client):
    key_id = kms_client.create_key(Description="tg-tr-kms")["KeyMetadata"]["KeyId"]
    arn = f"arn:aws:kms:us-east-1:000000000000:key/{key_id}"
    resp = tagging.tag_resources(ResourceARNList=[arn], Tags={_TAG_KEY: "tr-kms"})
    assert resp["FailedResourcesMap"] == {}
    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["tr-kms"]}])
    assert any(r["ResourceARN"] == arn for r in check["ResourceTagMappingList"])


def test_tagging_tag_resources_merges_existing(tagging, s3):
    """TagResources merges new tags, preserving keys not in the request."""
    s3.create_bucket(Bucket="tg-tr-merge")
    s3.put_bucket_tagging(Bucket="tg-tr-merge", Tagging={
        "TagSet": [{"Key": "existing", "Value": "keep-me"}]
    })
    arn = "arn:aws:s3:::tg-tr-merge"
    tagging.tag_resources(ResourceARNList=[arn], Tags={_TAG_KEY: "tr-merge"})
    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["tr-merge"]}])
    matched = next(r for r in check["ResourceTagMappingList"] if r["ResourceARN"] == arn)
    tag_map = {t["Key"]: t["Value"] for t in matched["Tags"]}
    assert tag_map["existing"] == "keep-me"
    assert tag_map[_TAG_KEY] == "tr-merge"


def test_tagging_tag_resources_overwrites_existing_key(tagging, s3):
    """TagResources overwrites the value when the same key already exists."""
    s3.create_bucket(Bucket="tg-tr-overwrite")
    s3.put_bucket_tagging(Bucket="tg-tr-overwrite", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "old-value"}]
    })
    arn = "arn:aws:s3:::tg-tr-overwrite"
    tagging.tag_resources(ResourceARNList=[arn], Tags={_TAG_KEY: "new-value"})
    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["new-value"]}])
    assert any(r["ResourceARN"] == arn for r in check["ResourceTagMappingList"])


def test_tagging_tag_resources_multiple_arns(tagging, s3):
    """TagResources applies the same tags to multiple ARNs in one call."""
    s3.create_bucket(Bucket="tg-tr-multi-a")
    s3.create_bucket(Bucket="tg-tr-multi-b")
    arns = ["arn:aws:s3:::tg-tr-multi-a", "arn:aws:s3:::tg-tr-multi-b"]
    resp = tagging.tag_resources(ResourceARNList=arns, Tags={_TAG_KEY: "tr-multi"})
    assert resp["FailedResourcesMap"] == {}
    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["tr-multi"]}])
    result_arns = [r["ResourceARN"] for r in check["ResourceTagMappingList"]]
    assert "arn:aws:s3:::tg-tr-multi-a" in result_arns
    assert "arn:aws:s3:::tg-tr-multi-b" in result_arns


def test_tagging_tag_resources_unknown_service(tagging):
    """Unknown service segment appears in FailedResourcesMap, not an exception."""
    resp = tagging.tag_resources(
        ResourceARNList=["arn:aws:unknownsvc:us-east-1:000000000000:no-such-resource"],
        Tags={_TAG_KEY: "fail"},
    )
    assert "arn:aws:unknownsvc:us-east-1:000000000000:no-such-resource" in resp["FailedResourcesMap"]


# ========== UntagResources ==========

def test_tagging_untag_resources_s3(tagging, s3):
    s3.create_bucket(Bucket="tg-utr-s3")
    s3.put_bucket_tagging(Bucket="tg-utr-s3", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "utr-s3"}]
    })
    arn = "arn:aws:s3:::tg-utr-s3"
    resp = tagging.untag_resources(ResourceARNList=[arn], TagKeys=[_TAG_KEY])
    assert resp["FailedResourcesMap"] == {}
    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["utr-s3"]}])
    assert not any(r["ResourceARN"] == arn for r in check["ResourceTagMappingList"])


def test_tagging_untag_resources_kms(tagging, kms_client):
    key_id = kms_client.create_key(Description="tg-utr-kms")["KeyMetadata"]["KeyId"]
    kms_client.tag_resource(KeyId=key_id, Tags=[{"TagKey": _TAG_KEY, "TagValue": "utr-kms"}])
    arn = f"arn:aws:kms:us-east-1:000000000000:key/{key_id}"
    resp = tagging.untag_resources(ResourceARNList=[arn], TagKeys=[_TAG_KEY])
    assert resp["FailedResourcesMap"] == {}
    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["utr-kms"]}])
    assert not any(r["ResourceARN"] == arn for r in check["ResourceTagMappingList"])


def test_tagging_untag_resources_preserves_other_keys(tagging, s3):
    """UntagResources removes only the specified keys, leaving others intact."""
    s3.create_bucket(Bucket="tg-utr-preserve")
    s3.put_bucket_tagging(Bucket="tg-utr-preserve", Tagging={
        "TagSet": [
            {"Key": _TAG_KEY, "Value": "remove-me"},
            {"Key": "stay", "Value": "here"},
        ]
    })
    arn = "arn:aws:s3:::tg-utr-preserve"
    tagging.untag_resources(ResourceARNList=[arn], TagKeys=[_TAG_KEY])
    check = tagging.get_resources(TagFilters=[{"Key": "stay", "Values": ["here"]}])
    assert any(r["ResourceARN"] == arn for r in check["ResourceTagMappingList"])


def test_tagging_untag_resources_nonexistent_key_is_noop(tagging, s3):
    """Removing a key that does not exist is a no-op, not an error."""
    s3.create_bucket(Bucket="tg-utr-noop")
    arn = "arn:aws:s3:::tg-utr-noop"
    resp = tagging.untag_resources(ResourceARNList=[arn], TagKeys=["__nonexistent__"])
    assert resp["FailedResourcesMap"] == {}


def test_tagging_untag_resources_unknown_service(tagging):
    """Unknown service segment appears in FailedResourcesMap."""
    resp = tagging.untag_resources(
        ResourceARNList=["arn:aws:unknownsvc:us-east-1:000000000000:no-such-resource"],
        TagKeys=[_TAG_KEY],
    )
    assert "arn:aws:unknownsvc:us-east-1:000000000000:no-such-resource" in resp["FailedResourcesMap"]


# ========== Cross-operation roundtrips ==========

def test_tagging_tag_then_get_roundtrip(tagging, s3):
    """Tags applied via TagResources are visible in GetResources."""
    s3.create_bucket(Bucket="tg-roundtrip")
    arn = "arn:aws:s3:::tg-roundtrip"
    tagging.tag_resources(ResourceARNList=[arn], Tags={_TAG_KEY: "roundtrip"})
    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["roundtrip"]}])
    assert any(r["ResourceARN"] == arn for r in resp["ResourceTagMappingList"])


def test_tagging_tag_untag_get_roundtrip(tagging, s3):
    """Tags removed via UntagResources no longer appear in GetResources."""
    s3.create_bucket(Bucket="tg-full-roundtrip")
    arn = "arn:aws:s3:::tg-full-roundtrip"
    tagging.tag_resources(ResourceARNList=[arn], Tags={_TAG_KEY: "full-roundtrip"})
    tagging.untag_resources(ResourceARNList=[arn], TagKeys=[_TAG_KEY])
    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["full-roundtrip"]}])
    assert not any(r["ResourceARN"] == arn for r in resp["ResourceTagMappingList"])


def test_tagging_tag_resources_visible_in_get_tag_keys(tagging, s3):
    """Tags applied via TagResources appear in GetTagKeys."""
    s3.create_bucket(Bucket="tg-tr-keys")
    arn = "arn:aws:s3:::tg-tr-keys"
    tagging.tag_resources(ResourceARNList=[arn], Tags={"phase3-key": "phase3-val"})
    resp = tagging.get_tag_keys()
    assert "phase3-key" in resp["TagKeys"]


# ========== Error shape (1.3.5) ==========

def test_tagging_tag_resources_unknown_service_returns_invalid_parameter(tagging):
    """Unknown ARN service segment returns InvalidParameterException/400, not
    InternalServiceException/501 — matches real AWS."""
    resp = tagging.tag_resources(
        ResourceARNList=["arn:aws:unknownsvc:us-east-1:000000000000:no-such"],
        Tags={"k": "v"},
    )
    entry = resp["FailedResourcesMap"]["arn:aws:unknownsvc:us-east-1:000000000000:no-such"]
    assert entry["ErrorCode"] == "InvalidParameterException"
    assert entry["StatusCode"] == 400


def test_tagging_tag_resources_malformed_arn_returns_invalid_parameter(tagging):
    with pytest.raises(ClientError) as exc:
        tagging.tag_resources(
            ResourceARNList=["arn:nope"],
            Tags={"k": "v"},
        )

    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


def test_tagging_tag_resources_foreign_region_arn_fails_request_without_mutating_local_resource(tagging, kms_client):
    key_id = kms_client.create_key(Description="tg-tr-kms-foreign-region")["KeyMetadata"]["KeyId"]
    foreign_region_arn = f"arn:aws:kms:us-west-2:000000000000:key/{key_id}"

    with pytest.raises(ClientError) as exc:
        tagging.tag_resources(
            ResourceARNList=[foreign_region_arn],
            Tags={_TAG_KEY: "wrong-region"},
        )

    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"
    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["wrong-region"]}])
    assert not any(key_id in r["ResourceARN"] for r in check["ResourceTagMappingList"])


def test_tagging_tag_resources_kms_forged_region_arn_does_not_mutate_key(kms_client):
    key_id = kms_client.create_key(Description="tg-tr-kms-forged-region")["KeyMetadata"]["KeyId"]
    forged_region_arn = f"arn:aws:kms:us-west-2:000000000000:key/{key_id}"
    west_tagging = _regional_client("resourcegroupstaggingapi", "us-west-2")

    resp = west_tagging.tag_resources(
        ResourceARNList=[forged_region_arn],
        Tags={_TAG_KEY: "forged-region"},
    )

    entry = resp["FailedResourcesMap"][forged_region_arn]
    assert entry["ErrorCode"] == "InvalidParameterException"
    assert entry["StatusCode"] == 400
    assert _TAG_KEY not in {t["TagKey"] for t in kms_client.list_resource_tags(KeyId=key_id)["Tags"]}


def test_tagging_tag_resources_malformed_arn_in_mixed_list_fails_request_before_mutation(tagging, s3):
    s3.create_bucket(Bucket="tg-tr-mixed-malformed")
    arn = "arn:aws:s3:::tg-tr-mixed-malformed"

    with pytest.raises(ClientError) as exc:
        tagging.tag_resources(
            ResourceARNList=[arn, "not-an-arn"],
            Tags={_TAG_KEY: "should-not-apply"},
        )

    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"
    check = tagging.get_resources(ResourceARNList=[arn])
    assert check["ResourceTagMappingList"] == []


def test_tagging_tag_resources_unsupported_global_arn_does_not_block_valid_resources(tagging, s3):
    s3.create_bucket(Bucket="tg-tr-mixed-unsupported-global")
    s3_arn = "arn:aws:s3:::tg-tr-mixed-unsupported-global"
    iam_arn = "arn:aws:iam::000000000000:role/tg-unsupported-role"

    resp = tagging.tag_resources(ResourceARNList=[iam_arn, s3_arn], Tags={_TAG_KEY: "mixed-global"})

    entry = resp["FailedResourcesMap"][iam_arn]
    assert entry["ErrorCode"] == "InvalidParameterException"
    assert entry["StatusCode"] == 400
    check = tagging.get_resources(ResourceARNList=[s3_arn])
    matched = next(r for r in check["ResourceTagMappingList"] if r["ResourceARN"] == s3_arn)
    assert {t["Key"]: t["Value"] for t in matched["Tags"]}[_TAG_KEY] == "mixed-global"


def test_tagging_tag_resources_unsupported_foreign_region_arn_does_not_block_valid_resources(tagging, s3):
    s3.create_bucket(Bucket="tg-tr-mixed-unsupported-foreign")
    s3_arn = "arn:aws:s3:::tg-tr-mixed-unsupported-foreign"
    unknown_arn = "arn:aws:unknownsvc:us-west-2:000000000000:no-such-resource"

    resp = tagging.tag_resources(ResourceARNList=[unknown_arn, s3_arn], Tags={_TAG_KEY: "mixed-foreign"})

    entry = resp["FailedResourcesMap"][unknown_arn]
    assert entry["ErrorCode"] == "InvalidParameterException"
    assert entry["StatusCode"] == 400
    check = tagging.get_resources(ResourceARNList=[s3_arn])
    matched = next(r for r in check["ResourceTagMappingList"] if r["ResourceARN"] == s3_arn)
    assert {t["Key"]: t["Value"] for t in matched["Tags"]}[_TAG_KEY] == "mixed-foreign"


def test_tagging_tag_resources_missing_region_arn_fails_request_before_mutation(lam, tagging):
    fname = f"tg-lambda-missing-region-{uuid.uuid4().hex[:8]}"
    base_arn = None
    try:
        base_arn = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler",
            Code={"ZipFile": _lambda_zip()},
        )["FunctionArn"]
        malformed_arn = base_arn.replace(":us-east-1:", "::")

        with pytest.raises(ClientError) as exc:
            tagging.tag_resources(ResourceARNList=[malformed_arn], Tags={"missing-region": "bad"})

        assert exc.value.response["Error"]["Code"] == "InvalidParameterException"
        assert "missing-region" not in lam.list_tags(Resource=base_arn)["Tags"]
    finally:
        if base_arn:
            lam.delete_function(FunctionName=fname)


def test_tagging_tag_resources_missing_account_arn_fails_request_before_mutation(lam, tagging, s3):
    fname = f"tg-lambda-missing-account-{uuid.uuid4().hex[:8]}"
    bucket = "tg-tr-missing-account"
    base_arn = None
    try:
        s3.create_bucket(Bucket=bucket)
        base_arn = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler",
            Code={"ZipFile": _lambda_zip()},
        )["FunctionArn"]
        malformed_arn = base_arn.replace(":000000000000:", "::")
        s3_arn = f"arn:aws:s3:::{bucket}"

        with pytest.raises(ClientError) as exc:
            tagging.tag_resources(ResourceARNList=[s3_arn, malformed_arn], Tags={"missing-account": "bad"})

        assert exc.value.response["Error"]["Code"] == "InvalidParameterException"
        assert tagging.get_resources(ResourceARNList=[s3_arn])["ResourceTagMappingList"] == []
        assert "missing-account" not in lam.list_tags(Resource=base_arn)["Tags"]
    finally:
        if base_arn:
            lam.delete_function(FunctionName=fname)


def test_tagging_tag_resources_missing_lambda_surfaces_in_failed_map(tagging):
    """Tagging a Lambda that does not exist in the caller's account surfaces
    a failure in FailedResourcesMap instead of silently no-op'ing."""
    arn = "arn:aws:lambda:us-east-1:000000000000:function:no-such-fn-tag-missing"
    resp = tagging.tag_resources(ResourceARNList=[arn], Tags={"k": "v"})
    assert arn in resp["FailedResourcesMap"]
    entry = resp["FailedResourcesMap"][arn]
    assert entry["ErrorCode"] == "InvalidParameterException"
    assert entry["StatusCode"] == 400


def test_tagging_tag_resources_foreign_region_lambda_arn_fails_request_without_mutating_functions(lam, tagging):
    west_lam = _regional_client("lambda", "us-west-2")
    fname = f"tg-lambda-region-{uuid.uuid4().hex[:8]}"
    code = _lambda_zip()

    east_arn = None
    west_arn = None
    try:
        east_arn = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler",
            Code={"ZipFile": code},
        )["FunctionArn"]
        west_arn = west_lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler",
            Code={"ZipFile": code},
        )["FunctionArn"]

        lam.tag_resource(Resource=east_arn, Tags={"region": "east"})

        with pytest.raises(ClientError) as exc:
            tagging.tag_resources(ResourceARNList=[west_arn], Tags={"region": "west"})

        assert exc.value.response["Error"]["Code"] == "InvalidParameterException"
        assert lam.list_tags(Resource=east_arn)["Tags"]["region"] == "east"
        assert "region" not in west_lam.list_tags(Resource=west_arn)["Tags"]
    finally:
        if east_arn:
            lam.delete_function(FunctionName=fname)
        if west_arn:
            west_lam.delete_function(FunctionName=fname)


def test_tagging_tag_resources_qualified_lambda_arn_tags_base_function(lam, tagging):
    fname = f"tg-lambda-qualified-{uuid.uuid4().hex[:8]}"
    base_arn = None
    try:
        base_arn = lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler",
            Code={"ZipFile": _lambda_zip()},
        )["FunctionArn"]
        version_arn = lam.publish_version(FunctionName=fname)["FunctionArn"]

        tagging.tag_resources(ResourceARNList=[version_arn], Tags={"qualified": "yes"})
        assert lam.list_tags(Resource=base_arn)["Tags"]["qualified"] == "yes"

        tagging.untag_resources(ResourceARNList=[version_arn], TagKeys=["qualified"])
        assert "qualified" not in lam.list_tags(Resource=base_arn)["Tags"]
    finally:
        if base_arn:
            lam.delete_function(FunctionName=fname)


def test_tagging_tag_resources_glue_connection_roundtrip(tagging, glue):
    name = f"tg-glue-conn-{uuid.uuid4().hex[:8]}"
    arn = f"arn:aws:glue:us-east-1:000000000000:connection/{name}"
    glue.create_connection(
        ConnectionInput={
            "Name": name,
            "ConnectionType": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": "jdbc:postgresql://host/db",
                "USERNAME": "user",
                "PASSWORD": "pass",
            },
        }
    )

    resp = tagging.tag_resources(ResourceARNList=[arn], Tags={_TAG_KEY: "glue-conn"})
    assert resp["FailedResourcesMap"] == {}
    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["glue-conn"]}])
    assert any(r["ResourceARN"] == arn for r in check["ResourceTagMappingList"])

    resp = tagging.untag_resources(ResourceARNList=[arn], TagKeys=[_TAG_KEY])
    assert resp["FailedResourcesMap"] == {}
    check = tagging.get_resources(ResourceARNList=[arn])
    matched = [r for r in check["ResourceTagMappingList"] if r["ResourceARN"] == arn]
    assert matched
    assert _TAG_KEY not in {t["Key"] for t in matched[0]["Tags"]}


def test_tagging_tag_resources_default_eventbridge_and_scheduler_resources_exist_before_service_touch():
    account = "111122223333"
    scoped_tagging = _account_client("resourcegroupstaggingapi", account)
    arns = [
        f"arn:aws:events:us-east-1:{account}:event-bus/default",
        f"arn:aws:scheduler:us-east-1:{account}:schedule-group/default",
    ]

    resp = scoped_tagging.tag_resources(ResourceARNList=arns, Tags={_TAG_KEY: "default-resource"})
    assert resp["FailedResourcesMap"] == {}
    check = scoped_tagging.get_resources(ResourceARNList=arns)
    result_arns = [r["ResourceARN"] for r in check["ResourceTagMappingList"]]
    assert result_arns == arns


def test_tagging_tag_resources_eventbridge_rule_arn_with_slash_bus_name(tagging, eb):
    bus_name = f"aws.partner/example.com/tg-{uuid.uuid4().hex[:8]}"
    eb.create_event_bus(Name=bus_name)
    rule_arn = eb.put_rule(
        Name=f"tg-rule-{uuid.uuid4().hex[:8]}",
        EventBusName=bus_name,
        EventPattern='{"source":["unit"]}',
    )["RuleArn"]

    resp = tagging.tag_resources(ResourceARNList=[rule_arn], Tags={_TAG_KEY: "event-rule"})
    assert resp["FailedResourcesMap"] == {}
    check = tagging.get_resources(ResourceARNList=[rule_arn])
    matched = next(r for r in check["ResourceTagMappingList"] if r["ResourceARN"] == rule_arn)
    assert {t["Key"]: t["Value"] for t in matched["Tags"]}[_TAG_KEY] == "event-rule"


def test_tagging_untag_missing_sns_surfaces_in_failed_map(tagging):
    arn = "arn:aws:sns:us-east-1:000000000000:no-such-topic-untag-missing"
    resp = tagging.untag_resources(ResourceARNList=[arn], TagKeys=["k"])
    assert arn in resp["FailedResourcesMap"]
    entry = resp["FailedResourcesMap"][arn]
    assert entry["ErrorCode"] == "InvalidParameterException"
    assert entry["StatusCode"] == 400


def test_tagging_untag_missing_backup_resources_surface_in_failed_map(tagging):
    arns = [
        "arn:aws:backup:us-east-1:000000000000:backup-vault:no-such-vault",
        "arn:aws:backup:us-east-1:000000000000:backup-plan:no-such-plan",
    ]

    resp = tagging.untag_resources(ResourceARNList=arns, TagKeys=["k"])

    for arn in arns:
        entry = resp["FailedResourcesMap"][arn]
        assert entry["ErrorCode"] == "InvalidParameterException"
        assert entry["StatusCode"] == 400


def test_tagging_tag_resources_cross_account_isolation(s3):
    """Tags applied in account A must not be visible in account B. Guard for
    the multi-tenant property guaranteed by AccountScopedDict under the hood."""
    import boto3
    from botocore.config import Config

    def _client(svc: str, account: str):
        return boto3.client(
            svc, endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
            region_name="us-east-1",
            aws_access_key_id=account, aws_secret_access_key="test",
            config=Config(retries={"mode": "standard"}),
        )

    s3_a = _client("s3", "111111111111")
    s3_b = _client("s3", "222222222222")
    tag_a = _client("resourcegroupstaggingapi", "111111111111")
    tag_b = _client("resourcegroupstaggingapi", "222222222222")

    s3_a.create_bucket(Bucket="tg-iso-acct-a")
    s3_b.create_bucket(Bucket="tg-iso-acct-b")
    arn_a = "arn:aws:s3:::tg-iso-acct-a"
    tag_a.tag_resources(ResourceARNList=[arn_a], Tags={"tenant": "A"})

    # Account B must not see A's tag.
    resp_b = tag_b.get_resources(TagFilters=[{"Key": "tenant", "Values": ["A"]}])
    arns_b = [r["ResourceARN"] for r in resp_b["ResourceTagMappingList"]]
    assert arn_a not in arns_b

    # Account A still sees its own tag.
    resp_a = tag_a.get_resources(TagFilters=[{"Key": "tenant", "Values": ["A"]}])
    arns_a = [r["ResourceARN"] for r in resp_a["ResourceTagMappingList"]]
    assert arn_a in arns_a


# ========== ElastiCache ==========

def test_tagging_get_resources_elasticache_tagged(tagging, ec):
    """GetResources returns ElastiCache resources that have been tagged."""
    ec.create_cache_cluster(
        CacheClusterId="tg-ec-tagged",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    arn = "arn:aws:elasticache:us-east-1:000000000000:cluster:tg-ec-tagged"
    ec.add_tags_to_resource(
        ResourceName=arn,
        Tags=[{"Key": _TAG_KEY, "Value": "ec-tagged"}],
    )

    resp = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["ec-tagged"]}])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert arn in arns


def test_tagging_get_resources_elasticache_untagged(tagging, ec):
    """GetResources with ResourceTypeFilters returns untagged ElastiCache resources."""
    ec.create_cache_cluster(
        CacheClusterId="tg-ec-untagged",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    arn = "arn:aws:elasticache:us-east-1:000000000000:cluster:tg-ec-untagged"

    resp = tagging.get_resources(ResourceTypeFilters=["elasticache"])
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert arn in arns


def test_tagging_get_resources_elasticache_type_filter(tagging, ec, s3):
    """ResourceTypeFilters=["elasticache"] excludes non-elasticache resources."""
    ec.create_cache_cluster(
        CacheClusterId="tg-ec-filter",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    ec_arn = "arn:aws:elasticache:us-east-1:000000000000:cluster:tg-ec-filter"
    ec.add_tags_to_resource(
        ResourceName=ec_arn,
        Tags=[{"Key": _TAG_KEY, "Value": "ec-filter"}],
    )
    s3.create_bucket(Bucket="tg-ec-filter-s3")
    s3.put_bucket_tagging(Bucket="tg-ec-filter-s3", Tagging={
        "TagSet": [{"Key": _TAG_KEY, "Value": "ec-filter"}]
    })

    resp = tagging.get_resources(
        TagFilters=[{"Key": _TAG_KEY, "Values": ["ec-filter"]}],
        ResourceTypeFilters=["elasticache"],
    )
    arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert ec_arn in arns
    assert "arn:aws:s3:::tg-ec-filter-s3" not in arns


def test_tagging_tag_resources_elasticache(tagging, ec):
    """TagResources on a valid ElastiCache ARN succeeds."""
    ec.create_cache_cluster(
        CacheClusterId="tg-ec-tr",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    arn = "arn:aws:elasticache:us-east-1:000000000000:cluster:tg-ec-tr"
    resp = tagging.tag_resources(ResourceARNList=[arn], Tags={_TAG_KEY: "ec-tr"})
    assert resp["FailedResourcesMap"] == {}

    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["ec-tr"]}])
    assert any(r["ResourceARN"] == arn for r in check["ResourceTagMappingList"])


def test_tagging_tag_resources_elasticache_replication_group_propagates_to_members(tagging, ec):
    """TagResources/UntagResources on a replication group fan out to member clusters."""
    rg_id = f"tg-ec-rg-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Tagging API fanout",
        CacheNodeType="cache.t3.micro",
        NumCacheClusters=2,
    )
    rg = ec.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]
    rg_arn = rg["ARN"]
    member_id = rg["MemberClusters"][0]
    member = ec.describe_cache_clusters(CacheClusterId=member_id)["CacheClusters"][0]
    member_arn = member["ARN"]

    resp = tagging.tag_resources(ResourceARNList=[rg_arn], Tags={_TAG_KEY: "ec-rg-tr"})
    assert resp["FailedResourcesMap"] == {}
    member_tags = ec.list_tags_for_resource(ResourceName=member_arn)["TagList"]
    assert {t["Key"]: t["Value"] for t in member_tags}[_TAG_KEY] == "ec-rg-tr"

    resp = tagging.untag_resources(ResourceARNList=[rg_arn], TagKeys=[_TAG_KEY])
    assert resp["FailedResourcesMap"] == {}
    member_tags = ec.list_tags_for_resource(ResourceName=member_arn)["TagList"]
    assert not any(t["Key"] == _TAG_KEY for t in member_tags)

    ec.delete_replication_group(ReplicationGroupId=rg_id)


def test_tagging_tag_resources_elasticache_nonexistent(tagging):
    """TagResources on a non-existent ElastiCache ARN returns InvalidParameterException."""
    arn = "arn:aws:elasticache:us-east-1:000000000000:cluster:no-such-cluster"
    resp = tagging.tag_resources(ResourceARNList=[arn], Tags={_TAG_KEY: "fail"})
    assert arn in resp["FailedResourcesMap"]
    entry = resp["FailedResourcesMap"][arn]
    assert entry["ErrorCode"] == "InvalidParameterException"
    assert entry["StatusCode"] == 400


def test_tagging_untag_resources_elasticache_nonexistent(tagging):
    """UntagResources on a non-existent ElastiCache ARN returns InvalidParameterException."""
    arn = "arn:aws:elasticache:us-east-1:000000000000:replicationgroup:no-such-rg"
    resp = tagging.untag_resources(ResourceARNList=[arn], TagKeys=[_TAG_KEY])
    assert arn in resp["FailedResourcesMap"]
    entry = resp["FailedResourcesMap"][arn]
    assert entry["ErrorCode"] == "InvalidParameterException"
    assert entry["StatusCode"] == 400


def test_tagging_untag_resources_elasticache(tagging, ec):
    """UntagResources removes tags from a valid ElastiCache resource."""
    ec.create_cache_cluster(
        CacheClusterId="tg-ec-utr",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    arn = "arn:aws:elasticache:us-east-1:000000000000:cluster:tg-ec-utr"
    tagging.tag_resources(ResourceARNList=[arn], Tags={_TAG_KEY: "ec-utr"})
    resp = tagging.untag_resources(ResourceARNList=[arn], TagKeys=[_TAG_KEY])
    assert resp["FailedResourcesMap"] == {}

    check = tagging.get_resources(TagFilters=[{"Key": _TAG_KEY, "Values": ["ec-utr"]}])
    assert not any(r["ResourceARN"] == arn for r in check["ResourceTagMappingList"])


# ========== Module reset ==========

def test_tagging_module_exposes_no_op_reset():
    """Tag dispatcher is stateless (delegates to per-service tag stores), but
    must expose `reset()` so the central `/_ministack/reset` doesn't log a
    warning."""
    from ministack.services import tagging
    assert callable(getattr(tagging, "reset", None))
    tagging.reset()
