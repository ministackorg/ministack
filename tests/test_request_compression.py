"""Gzip-compressed request bodies (smithy @requestCompression).

An operation that carries smithy's ``@requestCompression`` trait makes the SDK
gzip its request body once the body passes REQUEST_MIN_COMPRESSION_SIZE_BYTES
(default 10240) and send ``Content-Encoding: gzip``. CloudWatch PutMetricData
carries the trait in botocore's and aws-sdk-go-v2's bundled models, so boto3
compresses it with no client configuration at all.

The body must be inflated before the service handler parses it. S3 is the
exception: there Content-Encoding is object metadata, so the body must stay
exactly as sent.
"""

import gzip
import http.client
import json
import os
import uuid as _uuid_mod
from urllib.parse import urlparse

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")


def _post_gzip(target: str, payload: dict, content_type: str):
    """POST a gzipped JSON body with Content-Encoding: gzip."""
    parsed = urlparse(ENDPOINT)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 4566, timeout=10)
    body = gzip.compress(json.dumps(payload).encode())
    conn.request("POST", "/", body=body, headers={
        "Content-Type": content_type,
        "Content-Encoding": "gzip",
        "Content-Length": str(len(body)),
        "X-Amz-Target": target,
        "Authorization":
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/logs/aws4_request,"
            " SignedHeaders=host, Signature=x",
    })
    resp = conn.getresponse()
    resp.read()
    conn.close()
    return resp.status


def test_gzip_json_request_body_is_inflated(logs):
    """A gzipped JSON body reaches the handler as plain JSON. Without the
    inflate the handler read compressed bytes and failed on invalid JSON.
    The body is built by hand: this covers the wire behavior for the JSON
    protocol, whatever the client that compressed it."""
    name = f"/gzip-req-{_uuid_mod.uuid4().hex[:8]}"

    status = _post_gzip(
        "Logs_20140328.CreateLogGroup",
        {"logGroupName": name},
        "application/x-amz-json-1.1",
    )
    assert status == 200

    groups = logs.describe_log_groups(logGroupNamePrefix=name)["logGroups"]
    assert [g["logGroupName"] for g in groups] == [name]

    logs.delete_log_group(logGroupName=name)


def test_gzip_form_request_body_is_inflated():
    """Same for the Query protocol, where a compressed body also hides the
    Action parameter the request routes on. Body built by hand."""
    parsed = urlparse(ENDPOINT)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 4566, timeout=10)
    body = gzip.compress(b"Action=DescribeRegions&Version=2016-11-15")
    conn.request("POST", "/", body=body, headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Encoding": "gzip",
        "Content-Length": str(len(body)),
        "Authorization":
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/ec2/aws4_request,"
            " SignedHeaders=host, Signature=x",
    })
    resp = conn.getresponse()
    payload = resp.read().decode()
    conn.close()

    assert resp.status == 200
    assert "<DescribeRegionsResponse" in payload


def test_gzip_put_metric_data_stores_every_datapoint(cw):
    """PutMetricData carries @requestCompression, so boto3 gzips the body on
    its own once it passes 10240 bytes. No client configuration is involved.

    This is the operation a real SDK compresses today, and the failure was
    silent: MiniStack answered 200 and stored none of the batch, so a caller
    lost every datapoint with no error to see."""
    namespace = f"GzipMetrics-{_uuid_mod.uuid4().hex[:8]}"
    sent = {}
    cw.meta.events.register(
        "before-send.cloudwatch.PutMetricData",
        lambda request, **kw: sent.update(
            encoding=request.headers.get("Content-Encoding")),
    )

    # ~600 datapoints serialize well past the compression threshold.
    cw.put_metric_data(Namespace=namespace, MetricData=[
        {
            "MetricName": f"metric-{i}",
            "Value": float(i),
            "Dimensions": [
                {"Name": "PaddingDimensionName", "Value": f"padding-value-{i:05d}"},
            ],
        }
        for i in range(600)
    ])

    # Guard: if botocore stops compressing, this test no longer covers the bug.
    assert sent.get("encoding") in (b"gzip", "gzip"), sent
    assert len(cw.list_metrics(Namespace=namespace)["Metrics"]) == 600


def test_s3_put_object_keeps_gzip_body_and_metadata(s3):
    """S3 must not inflate. Content-Encoding is object metadata there, so the
    stored bytes stay compressed and the header comes back on GET."""
    bucket = "intg-gzip-passthrough"
    s3.create_bucket(Bucket=bucket)
    compressed = gzip.compress(b"stored-compressed" * 64)

    s3.put_object(
        Bucket=bucket, Key="payload.gz", Body=compressed, ContentEncoding="gzip",
    )

    resp = s3.get_object(Bucket=bucket, Key="payload.gz")
    assert resp["Body"].read() == compressed
    assert resp["ContentEncoding"] == "gzip"
