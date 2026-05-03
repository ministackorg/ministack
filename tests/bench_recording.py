"""
Benchmark: CloudTrail recording hook overhead.

Measures req/s and per-request latency across three service protocols
(S3 REST, DynamoDB JSON-target, SQS query-string) with recording off
and on, so the delta can be included in the PR description.

Usage:
    # Start ministack first, then run:
    python tests/bench_recording.py                   # recording off
    CLOUDTRAIL_RECORDING=1 python tests/bench_recording.py   # recording on

    # Or compare both in one run (server must already have recording toggled):
    python tests/bench_recording.py --compare

Options:
    --endpoint  Ministack URL (default: http://localhost:4566)
    --n         Requests per scenario (default: 300)
    --compare   Run off then on by toggling via /_ministack/config (no restart needed)
"""

import argparse
import statistics
import time
import uuid

import boto3
import requests

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


def _client(service, endpoint=ENDPOINT):
    return boto3.client(
        service,
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
    )


def _uid():
    return uuid.uuid4().hex[:8]


def _set_recording(enabled: bool, endpoint=ENDPOINT):
    resp = requests.post(
        f"{endpoint}/_ministack/config",
        json={"cloudtrail._recording_enabled": "true" if enabled else "false"},
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to toggle recording: {resp.text}")


def _reset(endpoint=ENDPOINT):
    requests.post(f"{endpoint}/_ministack/reset")


def _percentile(data, pct):
    return statistics.quantiles(data, n=100)[pct - 1]


def bench_s3(n, endpoint=ENDPOINT):
    """GET on a known bucket — exercises the S3 REST path."""
    s3 = _client("s3", endpoint)
    bucket = f"bench-{_uid()}"
    s3.create_bucket(Bucket=bucket)

    latencies = []
    for _ in range(n):
        t0 = time.perf_counter()
        s3.head_bucket(Bucket=bucket)
        latencies.append((time.perf_counter() - t0) * 1000)

    s3.delete_bucket(Bucket=bucket)
    return latencies


def bench_dynamodb(n, endpoint=ENDPOINT):
    """GetItem on a known table — exercises the DynamoDB JSON-target path."""
    ddb = _client("dynamodb", endpoint)
    table = f"bench-{_uid()}"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    latencies = []
    for _ in range(n):
        t0 = time.perf_counter()
        ddb.get_item(TableName=table, Key={"pk": {"S": "bench-key"}})
        latencies.append((time.perf_counter() - t0) * 1000)

    ddb.delete_table(TableName=table)
    return latencies


def bench_sqs(n, endpoint=ENDPOINT):
    """ReceiveMessage on an empty queue — exercises the SQS query-string path."""
    sqs = _client("sqs", endpoint)
    url = sqs.create_queue(QueueName=f"bench-{_uid()}")["QueueUrl"]

    latencies = []
    for _ in range(n):
        t0 = time.perf_counter()
        sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
        latencies.append((time.perf_counter() - t0) * 1000)

    sqs.delete_queue(QueueUrl=url)
    return latencies


def _report(label, latencies):
    n = len(latencies)
    total_s = sum(latencies) / 1000
    req_s = n / total_s
    p50 = statistics.median(latencies)
    p95 = _percentile(latencies, 95)
    p99 = _percentile(latencies, 99)
    print(
        f"  {label:<10}  {req_s:>7.0f} req/s   "
        f"p50={p50:.1f}ms  p95={p95:.1f}ms  p99={p99:.1f}ms"
    )
    return req_s


def _run_all(label, n, endpoint):
    print(f"\n{'─' * 60}")
    print(f"  {label}")
    print(f"{'─' * 60}")
    warmup = max(10, n // 10)
    print(f"  Warming up ({warmup} req per scenario)...")
    bench_s3(warmup, endpoint)
    bench_dynamodb(warmup, endpoint)
    bench_sqs(warmup, endpoint)
    print(f"  Measuring ({n} req per scenario)...")
    results = {
        "s3": _report("S3 HeadBucket", bench_s3(n, endpoint)),
        "dynamodb": _report("DDB GetItem", bench_dynamodb(n, endpoint)),
        "sqs": _report("SQS Recv", bench_sqs(n, endpoint)),
    }
    return results


def _delta_row(scenario, off, on):
    delta = (on - off) / off * 100
    sign = "+" if delta > 0 else ""
    flag = "  ⚠️  overhead >2%" if delta < -2 else ""
    print(f"  {scenario:<12}  off={off:>7.0f}  on={on:>7.0f}  delta={sign}{delta:.1f}%{flag}")


def main():
    parser = argparse.ArgumentParser(description="Bench CloudTrail recording overhead")
    parser.add_argument("--endpoint", default=ENDPOINT)
    parser.add_argument("--n", type=int, default=300, help="Requests per scenario")
    parser.add_argument("--compare", action="store_true", help="Run off then on in one pass")
    args = parser.parse_args()

    if args.compare:
        print("Running with recording OFF...")
        _set_recording(False, args.endpoint)
        off = _run_all("CLOUDTRAIL_RECORDING=0 (off)", args.n, args.endpoint)

        _reset(args.endpoint)

        print("\nRunning with recording ON...")
        _set_recording(True, args.endpoint)
        on = _run_all("CLOUDTRAIL_RECORDING=1 (on)", args.n, args.endpoint)

        _set_recording(False, args.endpoint)

        print(f"\n{'─' * 60}")
        print("  Delta summary (req/s, higher is better)")
        print(f"{'─' * 60}")
        for scenario in ("s3", "dynamodb", "sqs"):
            labels = {"s3": "S3", "dynamodb": "DynamoDB", "sqs": "SQS"}
            _delta_row(labels[scenario], off[scenario], on[scenario])
        print()
    else:
        recording = __import__("os").environ.get("CLOUDTRAIL_RECORDING", "0")
        label = f"CLOUDTRAIL_RECORDING={recording}"
        _run_all(label, args.n, args.endpoint)
        print()


if __name__ == "__main__":
    main()
