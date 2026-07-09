import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid as _uuid_mod

import pytest
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _describe_store_raw(kvs_arn):
    url = f"{ENDPOINT}/key-value-stores/{urllib.parse.quote(kvs_arn, safe='')}"
    req = urllib.request.Request(url, method="GET")
    return urllib.request.urlopen(req, timeout=10)


def test_kvs_dataplane_describe(cloudfront, cloudfront_kvs):
    name = f"dp-desc-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="describe test")
    arn = create_resp["KeyValueStore"]["ARN"]

    resp = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    assert resp["KvsARN"] == arn
    assert resp["ItemCount"] == 0
    assert resp["TotalSizeInBytes"] == 0
    assert resp["Status"] == "READY"
    assert "etag" in resp["ResponseMetadata"]["HTTPHeaders"]


def test_kvs_dataplane_put_and_get_key(cloudfront, cloudfront_kvs):
    name = f"dp-put-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="put/get test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="route/home", Value="/index.html", IfMatch=etag)
    assert put_resp["ItemCount"] == 1
    assert put_resp["TotalSizeInBytes"] > 0
    new_etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]
    assert new_etag != etag

    get_resp = cloudfront_kvs.get_key(KvsARN=arn, Key="route/home")
    assert get_resp["Key"] == "route/home"
    assert get_resp["Value"] == "/index.html"
    assert get_resp["ItemCount"] == 1


def test_kvs_dataplane_delete_key(cloudfront, cloudfront_kvs):
    name = f"dp-del-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="delete test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="to-delete", Value="val", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]

    del_resp = cloudfront_kvs.delete_key(KvsARN=arn, Key="to-delete", IfMatch=etag)
    assert del_resp["ItemCount"] == 0

    with pytest.raises(ClientError) as exc:
        cloudfront_kvs.get_key(KvsARN=arn, Key="to-delete")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_kvs_dataplane_list_keys(cloudfront, cloudfront_kvs):
    name = f"dp-list-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="list test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="key-a", Value="val-a", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]
    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="key-b", Value="val-b", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]
    cloudfront_kvs.put_key(KvsARN=arn, Key="key-c", Value="val-c", IfMatch=etag)

    resp = cloudfront_kvs.list_keys(KvsARN=arn)
    keys = [item["Key"] for item in resp["Items"]]
    assert "key-a" in keys
    assert "key-b" in keys
    assert "key-c" in keys


def test_kvs_dataplane_update_keys(cloudfront, cloudfront_kvs):
    name = f"dp-upd-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="update keys test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="existing", Value="old", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]

    resp = cloudfront_kvs.update_keys(
        KvsARN=arn,
        IfMatch=etag,
        Puts=[
            {"Key": "new-key", "Value": "new-val"},
            {"Key": "existing", "Value": "updated"},
        ],
        Deletes=[],
    )
    assert resp["ItemCount"] == 2

    get_resp = cloudfront_kvs.get_key(KvsARN=arn, Key="existing")
    assert get_resp["Value"] == "updated"

    get_resp = cloudfront_kvs.get_key(KvsARN=arn, Key="new-key")
    assert get_resp["Value"] == "new-val"


def test_kvs_dataplane_update_keys_with_deletes(cloudfront, cloudfront_kvs):
    name = f"dp-upddel-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="update+delete test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="keep", Value="yes", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]
    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="remove", Value="bye", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]

    resp = cloudfront_kvs.update_keys(
        KvsARN=arn,
        IfMatch=etag,
        Puts=[{"Key": "added", "Value": "hello"}],
        Deletes=[{"Key": "remove"}],
    )
    assert resp["ItemCount"] == 2

    with pytest.raises(ClientError) as exc:
        cloudfront_kvs.get_key(KvsARN=arn, Key="remove")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    get_resp = cloudfront_kvs.get_key(KvsARN=arn, Key="added")
    assert get_resp["Value"] == "hello"


def test_kvs_dataplane_etag_conflict(cloudfront, cloudfront_kvs):
    name = f"dp-conflict-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="conflict test")
    arn = create_resp["KeyValueStore"]["ARN"]

    with pytest.raises(ClientError) as exc:
        cloudfront_kvs.put_key(KvsARN=arn, Key="x", Value="y", IfMatch="wrong-etag")
    assert exc.value.response["Error"]["Code"] == "ConflictException"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409


def test_kvs_dataplane_not_found(cloudfront_kvs):
    fake_arn = "arn:aws:cloudfront::000000000000:key-value-store/nonexistent"
    with pytest.raises(ClientError) as exc:
        cloudfront_kvs.describe_key_value_store(KvsARN=fake_arn)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_kvs_dataplane_rejects_invalid_kvs_arns(cloudfront):
    name = f"dp-invalid-arn-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="invalid arn test")
    arn = create_resp["KeyValueStore"]["ARN"]
    invalid_cases = [
        "arn:aws:cloudfront::000000000000:distribution/example",
        arn.replace(":cloudfront:", ":sqs:"),
        arn.replace(":000000000000:", ":111111111111:"),
        arn.replace("cloudfront::", "cloudfront:us-east-1:"),
        f"{arn}/extra",
    ]

    for bad_arn in invalid_cases:
        with pytest.raises(urllib.error.HTTPError) as exc:
            _describe_store_raw(bad_arn)
        assert exc.value.code == 400
        body = json.loads(exc.value.read().decode("utf-8"))
        assert body["__type"] == "ValidationException"


def test_kvs_dataplane_list_keys_pagination(cloudfront, cloudfront_kvs):
    name = f"dp-page-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="pagination test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    for i in range(5):
        put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key=f"k{i:02d}", Value=f"v{i}", IfMatch=etag)
        etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]

    resp = cloudfront_kvs.list_keys(KvsARN=arn, MaxResults=2)
    assert len(resp["Items"]) == 2
    assert "NextToken" in resp

    resp2 = cloudfront_kvs.list_keys(KvsARN=arn, MaxResults=2, NextToken=resp["NextToken"])
    assert len(resp2["Items"]) == 2

    all_keys = [item["Key"] for item in resp["Items"] + resp2["Items"]]
    assert len(set(all_keys)) == 4
