"""S3 Access Points (#2159): the S3 Control API and the data plane through an
access point ARN or alias."""
import json
import os
import re
import urllib.error
import urllib.request
import uuid

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import make_client, patch_endpoint_dns

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
ACCOUNT = "000000000000"


def _client(service, region, account_key="test"):
    return boto3.client(
        service, endpoint_url=ENDPOINT, region_name=region,
        aws_access_key_id=account_key, aws_secret_access_key="test",
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


@pytest.fixture(autouse=True)
def _s3control_dns():
    """boto3 prefixes S3 Control and access point hosts with the account id."""
    with patch_endpoint_dns():
        yield


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def bucket():
    """A bucket in us-east-1 holding k.txt."""
    s3 = _client("s3", "us-east-1")
    name = f"ap-data-{_uid()}"
    s3.create_bucket(Bucket=name)
    s3.put_object(Bucket=name, Key="k.txt", Body=b"hello")
    yield name
    for obj in s3.list_objects_v2(Bucket=name).get("Contents", []):
        s3.delete_object(Bucket=name, Key=obj["Key"])
    try:
        s3.delete_bucket(Bucket=name)
    except ClientError:
        pass


@pytest.fixture
def access_point(bucket):
    ctl = _client("s3control", "us-east-1")
    name = f"ap-{_uid()}"
    created = ctl.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=bucket)
    yield name, created
    try:
        ctl.delete_access_point(AccountId=ACCOUNT, Name=name)
    except ClientError:
        pass


# --- Control plane ---------------------------------------------------------

def test_access_point_control_create_get_list_delete(bucket):
    ctl = _client("s3control", "us-east-1")
    name = f"ap-{_uid()}"
    created = ctl.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=bucket)
    arn = f"arn:aws:s3:us-east-1:{ACCOUNT}:accesspoint/{name}"
    assert created["AccessPointArn"] == arn
    assert re.fullmatch(rf"{name[:20]}-[a-z0-9]{{29}}use1b-s3alias", created["Alias"])

    ap = ctl.get_access_point(AccountId=ACCOUNT, Name=name)
    assert (ap["Name"], ap["Bucket"], ap["BucketAccountId"], ap["NetworkOrigin"]) == (
        name, bucket, ACCOUNT, "Internet")
    assert ap["AccessPointArn"] == arn and ap["Alias"] == created["Alias"]
    assert ap["PublicAccessBlockConfiguration"] == {
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True}
    assert ap["Endpoints"]["ipv4"] == "s3-accesspoint.us-east-1.amazonaws.com"
    assert set(ap["Endpoints"]) == {"ipv4", "fips", "dualstack", "fips_dualstack"}
    assert "CreationDate" in ap

    second = f"ap-{_uid()}"
    ctl.create_access_point(AccountId=ACCOUNT, Name=second, Bucket=bucket,
                            VpcConfiguration={"VpcId": "vpc-0123456789abcdef0"})
    try:
        listed = ctl.list_access_points(AccountId=ACCOUNT, Bucket=bucket)["AccessPointList"]
        assert sorted(a["Name"] for a in listed) == sorted([name, second])
        assert {a["Name"]: a["NetworkOrigin"] for a in listed}[second] == "VPC"
        page = ctl.list_access_points(AccountId=ACCOUNT, Bucket=bucket, MaxResults=1)
        assert len(page["AccessPointList"]) == 1 and page["NextToken"]
        rest = ctl.list_access_points(AccountId=ACCOUNT, Bucket=bucket, NextToken=page["NextToken"])
        assert len(rest["AccessPointList"]) == 1
    finally:
        ctl.delete_access_point(AccountId=ACCOUNT, Name=second)

    with pytest.raises(ClientError) as exc:
        ctl.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=bucket)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
    assert exc.value.response["Error"]["Code"] == "AccessPointAlreadyOwnedByYou"
    assert exc.value.response["Error"]["Message"] == (
        "Your previous request to create the named accesspoint succeeded and you already own it.")

    with pytest.raises(ClientError) as exc:
        _client("s3", "us-east-1").delete_bucket(Bucket=bucket)
    assert exc.value.response["Error"]["Code"] == "BucketHasAccessPointsAttached"

    ctl.delete_access_point(AccountId=ACCOUNT, Name=name)
    with pytest.raises(ClientError) as exc:
        ctl.get_access_point(AccountId=ACCOUNT, Name=name)
    err = exc.value.response["Error"]
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert (err["Code"], err["Message"], err["AccessPointName"]) == (
        "NoSuchAccessPoint", "The specified accesspoint does not exist", name)


def test_access_point_control_bucket_must_be_in_request_region(bucket):
    with pytest.raises(ClientError) as exc:
        _client("s3control", "eu-west-1").create_access_point(AccountId=ACCOUNT, Name=f"ap-{_uid()}", Bucket=bucket)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "InvalidRequest"
    assert exc.value.response["Error"]["Message"] == (
        "Amazon S3 AccessPoint can only be created for bucket in the same region")


def test_access_point_control_same_name_in_another_region(bucket):
    """Access point names are unique per account and region (observed)."""
    euw1_s3 = _client("s3", "eu-west-1")
    euw1_bucket = f"ap-euw1-{_uid()}"
    euw1_s3.create_bucket(Bucket=euw1_bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-1"})
    name = f"ap-{_uid()}"
    use1, euw1 = _client("s3control", "us-east-1"), _client("s3control", "eu-west-1")
    use1.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=bucket)
    try:
        alias = euw1.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=euw1_bucket)["Alias"]
        assert alias.endswith("euw1b-s3alias")
        assert euw1.get_access_point(AccountId=ACCOUNT, Name=name)["Bucket"] == euw1_bucket
        assert use1.get_access_point(AccountId=ACCOUNT, Name=name)["Bucket"] == bucket
        euw1.delete_access_point(AccountId=ACCOUNT, Name=name)
    finally:
        use1.delete_access_point(AccountId=ACCOUNT, Name=name)
        euw1_s3.delete_bucket(Bucket=euw1_bucket)


def test_access_point_control_policy_round_trip(access_point):
    name, _ = access_point
    ctl = _client("s3control", "us-east-1")
    policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"}, "Action": "s3:GetObject",
        "Resource": f"arn:aws:s3:us-east-1:{ACCOUNT}:accesspoint/{name}/object/*"}]})
    ctl.put_access_point_policy(AccountId=ACCOUNT, Name=name, Policy=policy)
    assert ctl.get_access_point_policy(AccountId=ACCOUNT, Name=name)["Policy"] == policy
    ctl.delete_access_point_policy(AccountId=ACCOUNT, Name=name)
    with pytest.raises(ClientError) as exc:
        ctl.get_access_point_policy(AccountId=ACCOUNT, Name=name)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


@pytest.mark.parametrize("method,path,status,fields", [
    ("POST", "/{ap}/policy", 405, {"Method": "POST", "ResourceType": "ACCESSPOINTPOLICY"}),
    ("PATCH", "/{ap}/policy", 405, {"Method": "PATCH", "ResourceType": "ACCESSPOINTPOLICY"}),
    ("POST", "/{ap}", 405, {"Method": "POST", "ResourceType": "ACCESSPOINT"}),
    ("POST", "", 405, {"Method": "POST", "ResourceType": "ACCESSPOINTS"}),
    ("GET", "/{ap}/x", 400, {"URI": "accesspoint/{ap}/x"}),
    ("DELETE", "/{ap}/x", 400, {}),
    ("DELETE", "/{ap}/policy/x", 400, {}),
    ("GET", "/{ap}/", 400, {}),
    ("GET", "//{ap}", 400, {}),
    ("PUT", "//{ap}/policy", 400, {}),
    ("GET", "/", 400, {}),    ("GET", "/{ap}/policyStatus", 404, {"AccessPointName": "{ap}/policyStatus"}),
    ("GET", "/{ap}/scope", 404, {}),
])
def test_access_point_control_unserved_requests(access_point, method, path, status, fields):
    """Unserved methods answer 405, unparsed paths 400, unemulated routes 404; none changes the access point."""
    name, _ = access_point
    ctl = _client("s3control", "us-east-1")
    policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"}, "Action": "s3:GetObject",
        "Resource": f"arn:aws:s3:us-east-1:{ACCOUNT}:accesspoint/{name}/object/*"}]})
    ctl.put_access_point_policy(AccountId=ACCOUNT, Name=name, Policy=policy)
    body = b"<PutAccessPointPolicyRequest><Policy>{}</Policy></PutAccessPointPolicyRequest>"
    got, _, data = _raw(method, ENDPOINT.split("://", 1)[1], "/v20180820/accesspoint" + path.format(ap=name),
                        "us-east-1", body if method == "PUT" else b"", headers={"x-amz-account-id": ACCOUNT})
    code = {405: "MethodNotAllowed", 400: "InvalidURI", 404: "NoSuchAccessPoint"}[status]
    assert (got, re.search(rb"<Code>(\w+)</Code>", data).group(1).decode()) == (status, code)
    for tag, value in fields.items():
        assert f"<{tag}>{value.format(ap=name)}</{tag}>".encode() in data
    assert ctl.get_access_point_policy(AccountId=ACCOUNT, Name=name)["Policy"] == policy


@pytest.mark.auth_contract
def test_access_point_named_policy_needs_the_access_point_action(auth_enabled, iam_user, bucket):
    """/v20180820/accesspoint/policy names an access point called "policy"."""
    ctl = _client("s3control", "us-east-1")
    ctl.create_access_point(AccountId=ACCOUNT, Name="policy", Bucket=bucket)
    try:
        user = iam_user(policy={"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": ["s3:GetAccessPointPolicy", "s3:DeleteAccessPointPolicy"],
            "Resource": "*"}]}).client("s3control", region="us-east-1")
        user.delete_access_point_policy(AccountId=ACCOUNT, Name="policy")
        if auth_enabled:
            for call in (user.get_access_point, user.delete_access_point):
                with pytest.raises(ClientError) as exc:
                    call(AccountId=ACCOUNT, Name="policy")
                assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
                assert exc.value.response["Error"]["Code"] == "AccessDenied"
            assert ctl.get_access_point(AccountId=ACCOUNT, Name="policy")["Bucket"] == bucket
        else:
            assert user.get_access_point(AccountId=ACCOUNT, Name="policy")["Bucket"] == bucket
            user.delete_access_point(AccountId=ACCOUNT, Name="policy")
    finally:
        try:
            ctl.delete_access_point(AccountId=ACCOUNT, Name="policy")
        except ClientError:
            pass


@pytest.mark.auth_contract
def test_access_point_policy_unserved_method_is_refused_before_authorization(auth_enabled, iam_user, access_point):
    """POST on an access point policy answers 405 to any caller and returns no policy."""
    name, _ = access_point
    _client("s3control", "us-east-1").put_access_point_policy(AccountId=ACCOUNT, Name=name, Policy=json.dumps({
        "Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"}, "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:us-east-1:{ACCOUNT}:accesspoint/{name}/object/*"}]}))
    user = iam_user()
    iam = make_client("iam")
    key = iam.create_access_key(UserName=user.name)["AccessKey"]
    try:
        signed = {"key": (key["AccessKeyId"], key["SecretAccessKey"]), "headers": {"x-amz-account-id": ACCOUNT}}
        host, path = ENDPOINT.split("://", 1)[1], f"/v20180820/accesspoint/{name}/policy"
        status, _, data = _raw("POST", host, path, "us-east-1", **signed)
        assert status == 405 and b"<ResourceType>ACCESSPOINTPOLICY</ResourceType>" in data
        assert b"<Policy>" not in data
        assert _raw("GET", host, path, "us-east-1", **signed)[0] == (403 if auth_enabled else 200)
    finally:
        iam.delete_access_key(UserName=user.name, AccessKeyId=key["AccessKeyId"])


# --- Data plane --------------------------------------------------------------

def _raw(method, host, path, region, body=b"", key=("test", "test"), headers=None):
    """A SigV4-signed request with an explicit Host, no redirect handling."""
    import hashlib
    import http.client
    from urllib.parse import urlparse

    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    req = AWSRequest(method=method, url=f"http://{host}{path}", data=body, headers=headers)
    req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    S3SigV4Auth(Credentials(*key), "s3", region).add_auth(req)
    parsed = urlparse(ENDPOINT)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=30)
    conn.request(method, path, body=body or None, headers={**dict(req.headers.items()), "Host": host})
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, {k.lower(): v for k, v in resp.getheaders()}, data


def test_access_point_arn_reaches_the_bucket(bucket, access_point):
    name, created = access_point
    s3 = _client("s3", "us-east-1")
    arn = created["AccessPointArn"]
    assert s3.get_object(Bucket=arn, Key="k.txt")["Body"].read() == b"hello"
    s3.put_object(Bucket=arn, Key="through-ap.txt", Body=b"via ap")
    assert s3.get_object(Bucket=bucket, Key="through-ap.txt")["Body"].read() == b"via ap"
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=arn)["Contents"]]
    assert keys == ["k.txt", "through-ap.txt"]
    # A client configured for another region still signs for the ARN's region.
    assert _client("s3", "eu-west-1").get_object(Bucket=arn, Key="k.txt")["Body"].read() == b"hello"


def test_access_point_arn_wrong_region_signing_gets_the_redirect(access_point):
    name, _ = access_point
    host = f"{name}-{ACCOUNT}.{ENDPOINT.split('://', 1)[1]}"
    status, headers, _ = _raw("GET", host, "/k.txt", "eu-west-1")
    assert status == 301
    assert headers["x-amz-bucket-region"] == "us-east-1"
    assert _raw("GET", host, "/k.txt", "us-east-1")[0] == 200


def test_access_point_arn_other_account_is_denied(access_point):
    _, created = access_point
    other = _client("s3", "us-east-1", account_key="111122223333")
    with pytest.raises(ClientError) as exc:
        other.get_object(Bucket=created["AccessPointArn"], Key="k.txt")
    assert exc.value.response["Error"]["Code"] in ("AccessDenied", "NoSuchAccessPoint")


def test_access_point_arn_shaped_bucket_without_access_point_is_a_bucket():
    """`<name>-<12 digits>` is a valid bucket name; with no access point of
    that name it is served as the bucket."""
    s3 = _client("s3", "us-east-1")
    name = f"plain-{_uid()}-123456789012"
    s3.create_bucket(Bucket=name)
    try:
        s3.put_object(Bucket=name, Key="b.txt", Body=b"bucket")
        host = f"{name}.{ENDPOINT.split('://', 1)[1]}"
        assert _raw("GET", host, "/b.txt", "us-east-1")[2] == b"bucket"
    finally:
        s3.delete_object(Bucket=name, Key="b.txt")
        s3.delete_bucket(Bucket=name)


def test_access_point_alias_works_as_a_bucket_name(bucket, access_point):
    _, created = access_point
    s3 = _client("s3", "us-east-1")
    alias = created["Alias"]
    assert s3.get_object(Bucket=alias, Key="k.txt")["Body"].read() == b"hello"
    s3.put_object(Bucket=alias, Key="alias.txt", Body=b"via alias")
    assert s3.get_object(Bucket=bucket, Key="alias.txt")["Body"].read() == b"via alias"
    s3.copy_object(Bucket=bucket, Key="copied.txt", CopySource={"Bucket": alias, "Key": "k.txt"})
    assert s3.get_object(Bucket=bucket, Key="copied.txt")["Body"].read() == b"hello"


@pytest.mark.auth_contract
@pytest.mark.parametrize("addressing_style", ["path", "virtual"])
@pytest.mark.parametrize("allow_upload", [False, True], ids=["no-permission", "allow"])
def test_access_point_alias_upload_permission_contract(
    bucket, access_point, auth_enabled, iam_user, addressing_style, allow_upload,
):
    _, created = access_point
    policy = {"Statement": [{"Effect": "Allow", "Action": "s3:PutObject", "Resource": "*"}]}
    client = iam_user(policy=policy if allow_upload else None).client(
        "s3", config=Config(s3={"addressing_style": addressing_style}),
    )
    root = _client("s3", "us-east-1")
    if auth_enabled and not allow_upload:
        with pytest.raises(ClientError) as exc:
            client.put_object(Bucket=created["Alias"], Key="contract-upload", Body=b"via alias")
        assert exc.value.response["Error"]["Code"] == "AccessDenied"
        assert root.list_objects_v2(Bucket=bucket, Prefix="contract-upload")["KeyCount"] == 0
    else:
        assert client.put_object(
            Bucket=created["Alias"], Key="contract-upload", Body=b"via alias",
        )["ResponseMetadata"]["HTTPStatusCode"] == 200
        with root.get_object(Bucket=bucket, Key="contract-upload")["Body"] as body:
            assert body.read() == b"via alias"


def _status(call):
    try:
        return call()["ResponseMetadata"]["HTTPStatusCode"]
    except ClientError as exc:
        return exc.response["ResponseMetadata"]["HTTPStatusCode"], exc.response["Error"]["Code"]


def test_access_point_block_public_acls_refuses_public_acl_writes():
    """An access point's BlockPublicAcls refuses public ACLs sent through it, whatever the bucket's settings."""
    s3, ctl = _client("s3", "us-east-1"), _client("s3control", "us-east-1")
    name = f"ap-acl-{_uid()}"
    all_off = {k: False for k in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")}
    s3.create_bucket(Bucket=name, ObjectOwnership="BucketOwnerPreferred")
    s3.put_public_access_block(Bucket=name, PublicAccessBlockConfiguration=all_off)
    s3.put_object(Bucket=name, Key="k.txt", Body=b"hello")
    blocking = ctl.create_access_point(AccountId=ACCOUNT, Name=f"ap-on-{_uid()}", Bucket=name)
    open_ap = ctl.create_access_point(
        AccountId=ACCOUNT, Name=f"ap-off-{_uid()}", Bucket=name, PublicAccessBlockConfiguration=all_off)
    try:
        denied = (403, "AccessDenied")
        for target in (blocking["Alias"], blocking["AccessPointArn"]):
            assert _status(lambda: s3.put_object(Bucket=target, Key="w", Body=b"x", ACL="public-read")) == denied
            assert _status(lambda: s3.put_object_acl(Bucket=target, Key="k.txt", ACL="public-read")) == denied
            assert _status(lambda: s3.put_object(Bucket=target, Key="w", Body=b"x", ACL="private")) == 200
        for target in (open_ap["Alias"], open_ap["AccessPointArn"], name):
            assert _status(lambda: s3.put_object(Bucket=target, Key="w", Body=b"x", ACL="public-read")) == 200
    finally:
        for ap in (blocking, open_ap):
            ctl.delete_access_point(AccountId=ACCOUNT, Name=ap["AccessPointArn"].rsplit("/", 1)[1])
        for obj in s3.list_objects_v2(Bucket=name).get("Contents", []):
            s3.delete_object(Bucket=name, Key=obj["Key"])
        s3.delete_bucket(Bucket=name)


def test_access_point_alias_from_another_account_is_denied(access_point):
    _name, created = access_point
    other = _client("s3", "us-east-1", account_key="111122223333")
    with pytest.raises(ClientError) as exc:
        other.get_object(Bucket=created["Alias"], Key="k.txt")
    assert exc.value.response["Error"]["Code"] == "AccessDenied"


def _access_point_policy(bucket, access_point_arn, grant, deny=None, condition=None):
    """An identity policy allowing reads and copies on ``grant`` ("bucket", "access point" or "both")."""
    arns = {"bucket": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
            "access point": [access_point_arn, f"{access_point_arn}/object/*"]}
    resources = arns["bucket"] + arns["access point"] if grant == "both" else arns[grant]
    statements = [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                   "Resource": resources}]
    if deny or condition:
        statements.append({"Effect": "Deny", "Action": ["s3:GetObject", "s3:ListBucket"],
                           "Resource": arns[deny] if deny else "*", **({"Condition": condition} if condition else {})})
    return {"Version": "2012-10-17", "Statement": statements}


def _assert_access(auth_enabled, allowed, call):
    """Under AUTH a refused call answers 403 AccessDenied; anything else succeeds."""
    if auth_enabled and not allowed:
        with pytest.raises(ClientError) as exc:
            call()
        error = exc.value.response
        assert (error["Error"]["Code"], error["ResponseMetadata"]["HTTPStatusCode"]) == ("AccessDenied", 403)
    else:
        call()


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
@pytest.mark.parametrize("grant, deny, allowed", [
    ("both", None, True),
    ("bucket", None, False),
    ("access point", None, False),
    ("both", "access point", False),
    ("both", "bucket", False),
], ids=["both", "bucket-only", "access-point-only", "deny-access-point", "deny-bucket"])
def test_access_point_request_needs_access_point_and_bucket_contract(auth_enabled, iam_user, bucket, access_point,
                                                                     via, grant, deny, allowed):
    """Through an access point a read and a listing are checked on the access point and on its bucket."""
    _name, created = access_point
    client = iam_user(policy=_access_point_policy(bucket, created["AccessPointArn"], grant, deny)).client("s3")
    target = created[via]
    _assert_access(auth_enabled, allowed, lambda: client.get_object(Bucket=target, Key="k.txt"))
    _assert_access(auth_enabled, allowed, lambda: client.list_objects_v2(Bucket=target))
    _assert_access(auth_enabled, grant != "access point" and deny != "bucket",
                   lambda: client.get_object(Bucket=bucket, Key="k.txt"))


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
@pytest.mark.parametrize("key, condition_value, through_access_point, through_bucket", [
    ("s3:DataAccessPointArn", "arn", False, True),
    ("s3:DataAccessPointAccount", ACCOUNT, False, True),
    ("s3:AccessPointNetworkOrigin", "Internet", False, True),
    ("s3:ExistingObjectTag/class", "secret", False, False),
    ("s3:DataAccessPointArn", None, True, False),
], ids=["arn", "account", "network-origin", "object-tag", "arn-absent"])
def test_access_point_condition_keys_contract(auth_enabled, iam_user, bucket, access_point, via, key,
                                              condition_value, through_access_point, through_bucket):
    """A request through an access point carries its ARN, account and network origin, and the object's tags."""
    _name, created = access_point
    _client("s3", "us-east-1").put_object_tagging(Bucket=bucket, Key="k.txt",
                                                  Tagging={"TagSet": [{"Key": "class", "Value": "secret"}]})
    arn = created["AccessPointArn"]
    condition = ({"Null": {key: "true"}} if condition_value is None
                 else {"StringEquals": {key: arn if condition_value == "arn" else condition_value}})
    client = iam_user(policy=_access_point_policy(bucket, arn, "both", condition=condition)).client("s3")
    _assert_access(auth_enabled, through_access_point, lambda: client.get_object(Bucket=created[via], Key="k.txt"))
    _assert_access(auth_enabled, through_bucket, lambda: client.get_object(Bucket=bucket, Key="k.txt"))


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
@pytest.mark.parametrize("condition, through_bucket", [(False, False), (True, True)], ids=["deny", "deny-on-arn-key"])
def test_access_point_request_meets_the_bucket_policy_contract(auth_enabled, iam_user, bucket, access_point, via,
                                                               condition, through_bucket):
    """The bucket policy applies through an access point and sees its ARN."""
    _name, created = access_point
    arn = created["AccessPointArn"]
    user = iam_user(policy=_access_point_policy(bucket, arn, "both"))
    statement = {"Effect": "Deny", "Principal": {"AWS": user.arn}, "Action": "s3:GetObject",
                 "Resource": f"arn:aws:s3:::{bucket}/*"}
    if condition:
        statement["Condition"] = {"StringEquals": {"s3:DataAccessPointArn": arn}}
    _client("s3", "us-east-1").put_bucket_policy(
        Bucket=bucket, Policy=json.dumps({"Version": "2012-10-17", "Statement": [statement]}))
    client = user.client("s3")
    _assert_access(auth_enabled, False, lambda: client.get_object(Bucket=created[via], Key="k.txt"))
    _assert_access(auth_enabled, through_bucket, lambda: client.get_object(Bucket=bucket, Key="k.txt"))


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
@pytest.mark.parametrize("grant, effect, delegate, allowed", [
    ("bucket", "Allow", False, True),
    (None, "Allow", True, True),
    ("access point", None, True, True),
    ("access point", "Allow", False, False),
    ("both", "Deny", True, False),
], ids=["allow-identity-on-bucket", "allow-delegated", "delegated-identity-on-access-point",
        "allow-not-delegated", "deny"])
def test_access_point_policy_contract(auth_enabled, iam_user, bucket, access_point, via, grant, effect, delegate,
                                      allowed):
    """The access point policy answers for the access point, the bucket policy or identity for the bucket."""
    name, created = access_point
    arn = created["AccessPointArn"]
    user = iam_user(policy=_access_point_policy(bucket, arn, grant) if grant else None)
    if delegate:
        _client("s3", "us-east-1").put_bucket_policy(Bucket=bucket, Policy=json.dumps({
            "Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "*",
                "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
                "Condition": {"StringEquals": {"s3:DataAccessPointAccount": ACCOUNT}}}]}))
    if effect:
        _client("s3control", "us-east-1").put_access_point_policy(AccountId=ACCOUNT, Name=name, Policy=json.dumps({
            "Version": "2012-10-17", "Statement": [{
                "Effect": effect, "Principal": {"AWS": user.arn}, "Action": ["s3:GetObject", "s3:ListBucket"],
                "Resource": [arn, f"{arn}/object/*"]}]}))
    client = user.client("s3")
    _assert_access(auth_enabled, allowed, lambda: client.get_object(Bucket=created[via], Key="k.txt"))
    _assert_access(auth_enabled, allowed, lambda: client.list_objects_v2(Bucket=created[via]))
    _assert_access(auth_enabled, grant in ("bucket", "both"), lambda: client.get_object(Bucket=bucket, Key="k.txt"))


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
def test_access_point_refuses_bucket_policy_writes_contract(auth_enabled, iam_user, bucket, access_point, via):
    """Under AUTH an access point refuses PutBucketPolicy and DeleteBucketPolicy and serves GetBucketPolicy."""
    _name, created = access_point
    arn = created["AccessPointArn"]
    client = iam_user(policy={"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Action": ["s3:PutBucketPolicy", "s3:GetBucketPolicy", "s3:DeleteBucketPolicy"],
        "Resource": [f"arn:aws:s3:::{bucket}", arn]}]}).client("s3", region="us-east-1")
    policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"}, "Action": "s3:GetObject",
        "Resource": f"arn:aws:s3:::{bucket}/*"}]})
    client.put_bucket_policy(Bucket=bucket, Policy=policy)
    assert client.get_bucket_policy(Bucket=created[via])["Policy"] == policy
    for write in (lambda: client.delete_bucket_policy(Bucket=created[via]),
                  lambda: client.put_bucket_policy(Bucket=created[via], Policy=policy)):
        if auth_enabled:
            with pytest.raises(ClientError) as exc:
                write()
            error = exc.value.response["Error"]
            assert error["Code"] == "AccessDenied"
            assert error["Message"].endswith(f'on resource: "{arn}" with an explicit deny in a resource-based policy')
            assert client.get_bucket_policy(Bucket=bucket)["Policy"] == policy
        else:
            write()
    client.delete_bucket_policy(Bucket=bucket)


@pytest.mark.auth_contract
def test_access_point_refuses_unsigned_requests_contract(auth_enabled, bucket, access_point):
    """Under AUTH an unsigned request through an access point is refused; the bucket still serves it."""
    _name, created = access_point
    s3 = _client("s3", "us-east-1")
    s3.put_bucket_ownership_controls(Bucket=bucket, OwnershipControls={
        "Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]})
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={k: False for k in (
        "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")})
    s3.put_object(Bucket=bucket, Key="pub.txt", Body=b"public", ACL="public-read")
    anon = boto3.client("s3", endpoint_url=ENDPOINT, region_name="us-east-1",
                        config=Config(signature_version=UNSIGNED, retries={"mode": "standard"}))
    assert anon.get_object(Bucket=bucket, Key="pub.txt")["Body"].read() == b"public"
    for target in (created["Alias"], created["AccessPointArn"]):
        if auth_enabled:
            with pytest.raises(ClientError) as exc:
                anon.get_object(Bucket=target, Key="pub.txt")
            error = exc.value.response
            assert (error["Error"]["Code"], error["ResponseMetadata"]["HTTPStatusCode"]) == ("InvalidRequest", 400)
        else:
            assert anon.get_object(Bucket=target, Key="pub.txt")["Body"].read() == b"public"


@pytest.mark.auth_contract
@pytest.mark.parametrize("via", ["AccessPointArn", "Alias"])
def test_access_point_sigv2_requests_contract(auth_enabled, iam_user, bucket, access_point, via):
    """Through an access point a SigV2 presigned URL is served, a SigV2 Authorization header is refused under AUTH."""
    _name, created = access_point
    user = iam_user(policy=_access_point_policy(bucket, created["AccessPointArn"], "both"))
    client = user.client("s3", region="us-east-1", config=Config(signature_version="s3"))
    url = client.generate_presigned_url("get_object", Params={"Bucket": created[via], "Key": "k.txt"})
    assert "AWSAccessKeyId=" in url
    with urllib.request.urlopen(url, timeout=10) as resp:
        assert resp.read() == b"hello"
    tampered = re.sub(r"Signature=[^&]+", "Signature=AAAA", url)
    if auth_enabled:
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(tampered, timeout=10)
        assert exc.value.code == 403
        assert b"<Code>SignatureDoesNotMatch</Code>" in exc.value.read()
        with pytest.raises(ClientError) as exc:
            client.get_object(Bucket=created[via], Key="k.txt")
        error = exc.value.response
        assert (error["Error"]["Code"], error["ResponseMetadata"]["HTTPStatusCode"]) == ("InvalidRequest", 400)
    else:
        with urllib.request.urlopen(tampered, timeout=10) as resp:
            assert resp.read() == b"hello"
        assert client.get_object(Bucket=created[via], Key="k.txt")["Body"].read() == b"hello"
    assert client.get_object(Bucket=bucket, Key="k.txt")["Body"].read() == b"hello"


@pytest.mark.auth_contract
@pytest.mark.parametrize("grant, deny, condition, allowed", [
    ("both", None, False, True),
    ("bucket", None, False, False),
    ("both", "access point", False, False),
    ("both", None, True, False),
], ids=["both", "bucket-only", "deny-access-point", "deny-on-arn-key"])
def test_access_point_alias_copy_source_contract(auth_enabled, iam_user, bucket, access_point, grant, deny,
                                                 condition, allowed):
    """A copy source named by an alias is read through its access point."""
    _name, created = access_point
    arn = created["AccessPointArn"]
    policy = _access_point_policy(bucket, arn, grant, deny,
                                  {"StringEquals": {"s3:DataAccessPointArn": arn}} if condition else None)
    client = iam_user(policy=policy).client("s3")
    _assert_access(auth_enabled, allowed, lambda: client.copy_object(
        Bucket=bucket, Key="copy.txt", CopySource={"Bucket": created["Alias"], "Key": "k.txt"}))
    _assert_access(auth_enabled, True, lambda: client.copy_object(
        Bucket=bucket, Key="copy.txt", CopySource={"Bucket": bucket, "Key": "k.txt"}))


def test_access_point_control_invalid_name(bucket):
    ctl = _client("s3control", "us-east-1")
    with pytest.raises(ClientError) as exc:
        ctl.create_access_point(AccountId=ACCOUNT, Name="Bad_Name", Bucket=bucket)
    err = exc.value.response
    assert (err["Error"]["Code"], err["ResponseMetadata"]["HTTPStatusCode"]) == ("InvalidAccessPoint", 400)
