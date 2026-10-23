"""S3 Object Lambda (#2159): Object Lambda Access Points in S3 Control, and
GetObject / HeadObject / List transformations through a Lambda that answers
with WriteGetObjectResponse.

The data plane follows the S3 user guide; AWS no longer onboards new Object
Lambda customers, so it could not be observed (see the plan's evidence table).
"""
import io
import json
import os
import re
import uuid
import zipfile

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import make_client, patch_endpoint_dns

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
ACCOUNT = "000000000000"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-role"


def _client(service, region="us-east-1", **config):
    return boto3.client(
        service, endpoint_url=ENDPOINT, region_name=region,
        aws_access_key_id="test", aws_secret_access_key="test",
        config=Config(region_name=region, retries={"mode": "standard"}, **config),
    )


@pytest.fixture(autouse=True)
def _host_dns():
    """boto3 prefixes S3 Control and access point hosts."""
    with patch_endpoint_dns():
        yield


def _uid():
    return uuid.uuid4().hex[:8]


def _zip(source: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", source)
    return buf.getvalue()


# Fetches the original through the presigned inputS3Url and writes it back
# upper-cased, with a status, a header and metadata of its own.
_UPPER_HANDLER = """
import urllib.request
import boto3

def handler(event, context):
    ctx = event["getObjectContext"]
    original = urllib.request.urlopen(ctx["inputS3Url"]).read()
    boto3.client("s3").write_get_object_response(
        RequestRoute=ctx["outputRoute"], RequestToken=ctx["outputToken"],
        Body=original.upper(), StatusCode=200, ContentType="text/x-upper",
        Metadata={"transformed": "yes"})
    return {"status_code": 200}
"""


@pytest.fixture
def supporting_access_point():
    """A bucket holding k.txt and an access point on it, both in us-east-1."""
    s3, ctl = _client("s3"), _client("s3control")
    bucket, name = f"olap-data-{_uid()}", f"olap-sup-{_uid()}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="k.txt", Body=b"hello object lambda")
    arn = ctl.create_access_point(AccountId=ACCOUNT, Name=name, Bucket=bucket)["AccessPointArn"]
    yield bucket, arn
    ctl.delete_access_point(AccountId=ACCOUNT, Name=name)
    for obj in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
        s3.delete_object(Bucket=bucket, Key=obj["Key"])
    s3.delete_bucket(Bucket=bucket)


def _function(source, region="us-east-1"):
    lam = _client("lambda", region)
    name = f"olap-fn-{_uid()}"
    arn = lam.create_function(FunctionName=name, Runtime="python3.12", Role=ROLE, Handler="index.handler",
                              Code={"ZipFile": _zip(source)}, Timeout=30)["FunctionArn"]
    return lam, name, arn


@pytest.fixture
def upper_function():
    lam, name, arn = _function(_UPPER_HANDLER)
    yield arn
    lam.delete_function(FunctionName=name)


def _configuration(supporting_arn, function_arn, actions=("GetObject",), allowed=()):
    return {
        "SupportingAccessPoint": supporting_arn,
        "AllowedFeatures": list(allowed),
        "TransformationConfigurations": [{
            "Actions": list(actions),
            "ContentTransformation": {"AwsLambda": {"FunctionArn": function_arn, "FunctionPayload": "p"}},
        }],
    }


@pytest.fixture
def olap(supporting_access_point, upper_function):
    bucket, supporting_arn = supporting_access_point
    ctl = _client("s3control")
    name = f"olap-{_uid()}"
    created = ctl.create_access_point_for_object_lambda(
        AccountId=ACCOUNT, Name=name, Configuration=_configuration(supporting_arn, upper_function))
    yield name, created, bucket
    try:
        ctl.delete_access_point_for_object_lambda(AccountId=ACCOUNT, Name=name)
    except ClientError:
        pass


# --- Control plane ---------------------------------------------------------

def test_object_lambda_control_create_get_list_delete(supporting_access_point, upper_function):
    _, supporting_arn = supporting_access_point
    ctl = _client("s3control")
    name = f"olap-{_uid()}"
    config = _configuration(supporting_arn, upper_function, actions=("GetObject", "HeadObject"),
                            allowed=("GetObject-Range",))
    created = ctl.create_access_point_for_object_lambda(AccountId=ACCOUNT, Name=name, Configuration=config)
    assert created["ObjectLambdaAccessPointArn"] == f"arn:aws:s3-object-lambda:us-east-1:{ACCOUNT}:accesspoint/{name}"
    assert re.fullmatch(rf"{name[:20]}-[a-z0-9]{{24}}use1a--ol-s3", created["Alias"]["Value"])
    assert created["Alias"]["Status"] == "READY"

    got = ctl.get_access_point_for_object_lambda(AccountId=ACCOUNT, Name=name)
    assert got["Name"] == name and got["Alias"] == created["Alias"] and "CreationDate" in got
    assert all(got["PublicAccessBlockConfiguration"].values())

    stored = ctl.get_access_point_configuration_for_object_lambda(AccountId=ACCOUNT, Name=name)["Configuration"]
    assert stored["SupportingAccessPoint"] == supporting_arn
    assert stored["AllowedFeatures"] == ["GetObject-Range"]
    assert stored["TransformationConfigurations"][0]["Actions"] == ["GetObject", "HeadObject"]
    assert stored["TransformationConfigurations"][0]["ContentTransformation"]["AwsLambda"] == {
        "FunctionArn": upper_function, "FunctionPayload": "p"}

    ctl.put_access_point_configuration_for_object_lambda(
        AccountId=ACCOUNT, Name=name, Configuration=_configuration(supporting_arn, upper_function))
    stored = ctl.get_access_point_configuration_for_object_lambda(AccountId=ACCOUNT, Name=name)["Configuration"]
    assert stored["TransformationConfigurations"][0]["Actions"] == ["GetObject"]

    listed = ctl.list_access_points_for_object_lambda(AccountId=ACCOUNT)["ObjectLambdaAccessPointList"]
    assert name in [a["Name"] for a in listed]

    policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"},
        "Action": "s3-object-lambda:GetObject", "Resource": created["ObjectLambdaAccessPointArn"]}]})
    ctl.put_access_point_policy_for_object_lambda(AccountId=ACCOUNT, Name=name, Policy=policy)
    assert ctl.get_access_point_policy_for_object_lambda(AccountId=ACCOUNT, Name=name)["Policy"] == policy
    ctl.delete_access_point_policy_for_object_lambda(AccountId=ACCOUNT, Name=name)

    ctl.delete_access_point_for_object_lambda(AccountId=ACCOUNT, Name=name)
    with pytest.raises(ClientError) as exc:
        ctl.get_access_point_for_object_lambda(AccountId=ACCOUNT, Name=name)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


@pytest.mark.parametrize("change", ["supporting-region", "supporting-missing", "lambda-region", "feature", "action"])
def test_object_lambda_control_validation(supporting_access_point, upper_function, change):
    """The supporting access point and Lambda share the access point's account
    and region; actions and allowed features come from the documented sets."""
    _, supporting_arn = supporting_access_point
    config = _configuration(supporting_arn, upper_function)
    if change == "supporting-region":
        config["SupportingAccessPoint"] = supporting_arn.replace(":us-east-1:", ":eu-west-1:")
    elif change == "supporting-missing":
        config["SupportingAccessPoint"] = supporting_arn + "-missing"
    elif change == "lambda-region":
        config["TransformationConfigurations"][0]["ContentTransformation"]["AwsLambda"]["FunctionArn"] = (
            upper_function.replace(":us-east-1:", ":eu-west-1:"))
    elif change == "feature":
        config["AllowedFeatures"] = ["GetObject-Everything"]
    else:
        config["TransformationConfigurations"][0]["Actions"] = ["PutObject"]
    with pytest.raises(ClientError) as exc:
        _client("s3control").create_access_point_for_object_lambda(
            AccountId=ACCOUNT, Name=f"olap-{_uid()}", Configuration=config)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "InvalidRequest"


@pytest.fixture
def control_olap(supporting_access_point):
    """An Object Lambda Access Point with a policy, for control-plane requests only."""
    _, supporting_arn = supporting_access_point
    ctl = _client("s3control")
    name = f"olap-{_uid()}"
    function_arn = f"arn:aws:lambda:us-east-1:{ACCOUNT}:function:olap-unused"
    arn = ctl.create_access_point_for_object_lambda(
        AccountId=ACCOUNT, Name=name, Configuration=_configuration(supporting_arn, function_arn),
    )["ObjectLambdaAccessPointArn"]
    policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"},
        "Action": "s3-object-lambda:GetObject", "Resource": arn}]})
    ctl.put_access_point_policy_for_object_lambda(AccountId=ACCOUNT, Name=name, Policy=policy)
    yield name, policy
    try:
        ctl.delete_access_point_for_object_lambda(AccountId=ACCOUNT, Name=name)
    except ClientError:
        pass


_OLAP_POLICY_BODY = (b"<PutAccessPointPolicyForObjectLambdaRequest><Policy>{}</Policy>"
                     b"</PutAccessPointPolicyForObjectLambdaRequest>")


def _control_raw(method, path, body=b"", key=("test", "test")):
    """A SigV4-signed S3 Control request, the path sent as given: status and body."""
    import hashlib
    import http.client
    from urllib.parse import urlparse

    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    req = AWSRequest(method=method, url=f"{ENDPOINT}{path}", data=body, headers={"x-amz-account-id": ACCOUNT})
    req.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()
    S3SigV4Auth(Credentials(*key), "s3", "us-east-1").add_auth(req)
    parsed = urlparse(ENDPOINT)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=30)
    conn.request(method, path, body=body or None, headers=dict(req.headers.items()))
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


@pytest.mark.parametrize("method,path,status,fields", [
    ("POST", "/{ol}/policy", 405, {"Method": "POST", "ResourceType": "OBJECT_LAMBDA_ACCESS_POINT_POLICY"}),
    ("POST", "/{ol}", 405, {"Method": "POST", "ResourceType": "OBJECT_LAMBDA_ACCESS_POINT"}),
    ("POST", "/{ol}/configuration", 405,
     {"Method": "POST", "ResourceType": "OBJECT_LAMBDA_ACCESS_POINT_CONFIGURATION"}),
    ("DELETE", "/{ol}/configuration", 405,
     {"Method": "DELETE", "ResourceType": "OBJECT_LAMBDA_ACCESS_POINT_CONFIGURATION"}),
    ("POST", "", 405, {"Method": "POST", "ResourceType": "OBJECT_LAMBDA_ACCESS_POINTS"}),
    ("DELETE", "/{ol}/x", 400, {"URI": "accesspointforobjectlambda/{ol}/x"}),
    ("GET", "/{ol}/x", 400, {}),
    ("PUT", "//{ol}/policy", 400, {}),
    ("GET", "//{ol}/policy", 400, {}),
    ("GET", "/", 400, {}),    ("GET", "/{ol}/policyStatus", 404, {"AccessPointName": "{ol}/policyStatus"}),
])
def test_object_lambda_control_unserved_requests(control_olap, method, path, status, fields):
    """Unserved methods answer 405, unparsed paths 400, unemulated routes 404; none changes the access point."""
    name, policy = control_olap
    got, data = _control_raw(method, "/v20180820/accesspointforobjectlambda" + path.format(ol=name),
                             _OLAP_POLICY_BODY if method == "PUT" else b"")
    code = {405: "MethodNotAllowed", 400: "InvalidURI", 404: "NoSuchAccessPoint"}[status]
    assert (got, re.search(rb"<Code>(\w+)</Code>", data).group(1).decode()) == (status, code)
    for tag, value in fields.items():
        assert f"<{tag}>{value.format(ol=name)}</{tag}>".encode() in data
    ctl = _client("s3control")
    assert ctl.get_access_point_policy_for_object_lambda(AccountId=ACCOUNT, Name=name)["Policy"] == policy
    assert ctl.get_access_point_configuration_for_object_lambda(AccountId=ACCOUNT, Name=name)["Configuration"]


@pytest.mark.auth_contract
def test_object_lambda_control_unparsed_paths_change_nothing(auth_enabled, iam_user, control_olap):
    """An extra segment or a double slash neither deletes the access point nor sets its policy."""
    name, policy = control_olap
    user = iam_user()
    iam = make_client("iam")
    key = iam.create_access_key(UserName=user.name)["AccessKey"]
    creds = (key["AccessKeyId"], key["SecretAccessKey"])
    prefix = "/v20180820/accesspointforobjectlambda"
    try:
        assert _control_raw("DELETE", f"{prefix}/{name}/x", key=creds)[0] == 400
        assert _control_raw("PUT", f"{prefix}//{name}/policy", _OLAP_POLICY_BODY, key=creds)[0] == 400
        assert _control_raw("POST", f"{prefix}/{name}/policy", key=creds)[0] == 405
        assert _control_raw("GET", f"{prefix}/{name}", key=creds)[0] == (403 if auth_enabled else 200)
    finally:
        iam.delete_access_key(UserName=user.name, AccessKeyId=key["AccessKeyId"])
    ctl = _client("s3control")
    assert ctl.get_access_point_for_object_lambda(AccountId=ACCOUNT, Name=name)["Name"] == name
    assert ctl.get_access_point_policy_for_object_lambda(AccountId=ACCOUNT, Name=name)["Policy"] == policy


# --- Routing and WriteGetObjectResponse ------------------------------------

def test_object_lambda_routing_unknown_name():
    arn = f"arn:aws:s3-object-lambda:us-east-1:{ACCOUNT}:accesspoint/nope-{_uid()}"
    with pytest.raises(ClientError) as exc:
        _client("s3").get_object(Bucket=arn, Key="k.txt")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NoSuchAccessPoint"


@pytest.mark.parametrize("inject_host_prefix", [False, True])
def test_object_lambda_write_get_object_response_unknown_route(inject_host_prefix):
    """An unknown route is refused as AWS does (observed: 400 InvalidHostHeader),
    with or without the modeled host prefix."""
    s3 = _client("s3", inject_host_prefix=inject_host_prefix)
    with pytest.raises(ClientError) as exc:
        s3.write_get_object_response(RequestRoute=f"io-use1-{_uid()}", RequestToken="t", Body=b"x")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "InvalidHostHeader"
    assert exc.value.response["Error"]["Message"] == "Invalid Host header."


# --- GetObject transformation ----------------------------------------------

def test_object_lambda_get_object_through_arn_and_alias(olap):
    name, created, _ = olap
    s3 = _client("s3")
    for target in (created["ObjectLambdaAccessPointArn"], created["Alias"]["Value"]):
        obj = s3.get_object(Bucket=target, Key="k.txt")
        assert obj["Body"].read() == b"HELLO OBJECT LAMBDA"
        assert obj["ContentType"] == "text/x-upper"
        assert obj["Metadata"] == {"transformed": "yes"}


def _olap_with(supporting_arn, source, actions=("GetObject",), allowed=()):
    lam, fn_name, fn_arn = _function(source)
    ctl = _client("s3control")
    name = f"olap-{_uid()}"
    created = ctl.create_access_point_for_object_lambda(
        AccountId=ACCOUNT, Name=name, Configuration=_configuration(supporting_arn, fn_arn, actions, allowed))

    def cleanup():
        ctl.delete_access_point_for_object_lambda(AccountId=ACCOUNT, Name=name)
        lam.delete_function(FunctionName=fn_name)

    return created["ObjectLambdaAccessPointArn"], cleanup


@pytest.mark.parametrize("source,code,status", [
    ("def handler(e, c):\n    raise RuntimeError('boom')\n", "LambdaRuntimeError", 500),
    ("def handler(e, c):\n    return {'status_code': 200}\n", "LambdaResponseNotReceived", 500),
    ("import boto3\ndef handler(e, c):\n    ctx = e['getObjectContext']\n"
     "    boto3.client('s3').write_get_object_response(RequestRoute=ctx['outputRoute'], "
     "RequestToken=ctx['outputToken'], StatusCode=403, ErrorCode='NotAllowed', ErrorMessage='nope')\n",
     "NotAllowed", 403),
], ids=["raises", "no-write", "forwarded-error"])
def test_object_lambda_get_object_errors(supporting_access_point, source, code, status):
    _, supporting_arn = supporting_access_point
    arn, cleanup = _olap_with(supporting_arn, source)
    try:
        with pytest.raises(ClientError) as exc:
            _client("s3").get_object(Bucket=arn, Key="k.txt")
        assert exc.value.response["Error"]["Code"] == code
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == status
    finally:
        cleanup()


def test_object_lambda_get_object_missing_function(supporting_access_point, upper_function):
    _, supporting_arn = supporting_access_point
    lam, fn_name, fn_arn = _function("def handler(e, c):\n    return 1\n")
    ctl = _client("s3control")
    name = f"olap-{_uid()}"
    created = ctl.create_access_point_for_object_lambda(
        AccountId=ACCOUNT, Name=name, Configuration=_configuration(supporting_arn, fn_arn))
    lam.delete_function(FunctionName=fn_name)
    try:
        with pytest.raises(ClientError) as exc:
            _client("s3").get_object(Bucket=created["ObjectLambdaAccessPointArn"], Key="k.txt")
        assert exc.value.response["Error"]["Code"] == "LambdaNotFound"
    finally:
        ctl.delete_access_point_for_object_lambda(AccountId=ACCOUNT, Name=name)


def test_object_lambda_wrong_region_signing(olap):
    """Signed for another region than the Object Lambda Access Point's: the
    AC1 redirect with the access point's region."""
    import http.client

    from botocore.auth import S3SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    name, _, _ = olap
    netloc = ENDPOINT.split("://", 1)[1]
    host = f"{name}-{ACCOUNT}.{netloc}"
    req = AWSRequest(method="GET", url=f"http://{host}/k.txt")
    req.headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
    S3SigV4Auth(Credentials("test", "test"), "s3-object-lambda", "eu-west-1").add_auth(req)
    hostname, port = netloc.rsplit(":", 1)
    conn = http.client.HTTPConnection(hostname, int(port), timeout=30)
    conn.request("GET", "/k.txt", headers={**dict(req.headers.items()), "Host": host})
    resp = conn.getresponse()
    resp.read()
    assert resp.status == 301
    assert resp.getheader("x-amz-bucket-region") == "us-east-1"


# --- AllowedFeatures and pass-through --------------------------------------

_RANGE_ECHO = """
import boto3

def handler(event, context):
    ctx = event["getObjectContext"]
    boto3.client("s3").write_get_object_response(
        RequestRoute=ctx["outputRoute"], RequestToken=ctx["outputToken"],
        Body=(event["userRequest"]["headers"].get("Range") or "none").encode())
    return {"status_code": 200}
"""


def test_object_lambda_allowed_features(supporting_access_point):
    _, supporting_arn = supporting_access_point
    refused_arn, refused_cleanup = _olap_with(supporting_arn, _RANGE_ECHO)
    allowed_arn, allowed_cleanup = _olap_with(supporting_arn, _RANGE_ECHO, allowed=("GetObject-Range",))
    s3 = _client("s3")
    try:
        with pytest.raises(ClientError) as exc:
            s3.get_object(Bucket=refused_arn, Key="k.txt", Range="bytes=0-4")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501
        assert s3.get_object(Bucket=allowed_arn, Key="k.txt", Range="bytes=0-4")["Body"].read() == b"bytes=0-4"
    finally:
        refused_cleanup()
        allowed_cleanup()


def test_object_lambda_pass_through(olap):
    """Operations without a transformation go to the supporting access point."""
    _, created, bucket = olap
    s3 = _client("s3")
    arn = created["ObjectLambdaAccessPointArn"]
    s3.put_object(Bucket=arn, Key="put.txt", Body=b"plain")
    assert s3.get_object(Bucket=bucket, Key="put.txt")["Body"].read() == b"plain"
    assert s3.head_object(Bucket=arn, Key="put.txt")["ContentLength"] == 5
    s3.delete_object(Bucket=arn, Key="put.txt")
    assert "Contents" not in s3.list_objects_v2(Bucket=bucket, Prefix="put.txt")


# --- HeadObject and list transformations -----------------------------------

_HEAD_AND_LIST = """
def handler(event, context):
    if "headObjectContext" in event:
        return {"statusCode": 200, "headers": {"Content-Type": "text/x-head", "Content-Length": "7"},
                "metadata": {"seen": "head"}}
    if "listObjectsV2Context" in event:
        return {"statusCode": 200, "listResultXml":
                "<?xml version='1.0' encoding='UTF-8'?><ListBucketResult><Name>b</Name><KeyCount>1</KeyCount>"
                "<IsTruncated>false</IsTruncated><Contents><Key>only-v2.txt</Key><Size>1</Size></Contents>"
                "</ListBucketResult>"}
    return {"statusCode": 200, "listBucketResult": {
        "name": "b", "isTruncated": False, "contents": [{"key": "only-v1.txt", "size": 1}]}}
"""


def test_object_lambda_head_object_and_list_objects(supporting_access_point):
    _, supporting_arn = supporting_access_point
    arn, cleanup = _olap_with(supporting_arn, _HEAD_AND_LIST, actions=("HeadObject", "ListObjects", "ListObjectsV2"))
    s3 = _client("s3")
    try:
        head = s3.head_object(Bucket=arn, Key="k.txt")
        assert (head["ContentType"], head["Metadata"]) == ("text/x-head", {"seen": "head"})
        assert [o["Key"] for o in s3.list_objects_v2(Bucket=arn)["Contents"]] == ["only-v2.txt"]
        # A transformation without GetObject passes GetObject through.
        assert s3.get_object(Bucket=arn, Key="k.txt")["Body"].read() == b"hello object lambda"
    finally:
        cleanup()


def test_object_lambda_list_objects_json_result(supporting_access_point):
    _, supporting_arn = supporting_access_point
    arn, cleanup = _olap_with(supporting_arn, _HEAD_AND_LIST, actions=("ListObjects",))
    try:
        listed = _client("s3").list_objects(Bucket=arn)
        assert [o["Key"] for o in listed["Contents"]] == ["only-v1.txt"]
    finally:
        cleanup()


# --- Executors ---------------------------------------------------------------

@pytest.mark.skipif(os.environ.get("LAMBDA_EXECUTOR", "").lower() != "docker",
                    reason="requires LAMBDA_EXECUTOR=docker and Docker daemon")
def test_object_lambda_get_object_docker_executor(olap):
    """The same transformation from the Docker executor: boto3 in the Lambda
    image must honour AWS_DISABLE_HOST_PREFIX_INJECTION, and the inputS3Url
    must use a host the container reaches."""
    _, created, _ = olap
    obj = _client("s3").get_object(Bucket=created["ObjectLambdaAccessPointArn"], Key="k.txt")
    assert obj["Body"].read() == b"HELLO OBJECT LAMBDA"


def test_object_lambda_input_url_host_follows_executor(monkeypatch):
    """In-process: a container executor gets host.docker.internal, the
    subprocess executors 127.0.0.1."""
    from ministack.services import lambda_svc, s3_object_lambda

    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.setattr(lambda_svc, "LAMBDA_EXECUTOR", "local")
    monkeypatch.setattr(lambda_svc, "LAMBDA_STRICT", False)
    config = {"FunctionName": "f", "Runtime": "python3.12"}
    assert s3_object_lambda._endpoint_for(config).startswith(("http://127.0.0.1:", "https://127.0.0.1:"))
    monkeypatch.setattr(lambda_svc, "LAMBDA_EXECUTOR", "docker")
    assert "://host.docker.internal:" in s3_object_lambda._endpoint_for(config)
