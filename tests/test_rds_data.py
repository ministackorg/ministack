"""
Tests for RDS Data API service emulator.
Since no real DB containers are available in CI, these tests focus on:
- API routing (requests reach the handler, not 404)
- Parameter validation (missing resourceArn, missing sql, etc.)
- Transaction lifecycle error paths
- Invalid resource ARN handling
"""

import json
import urllib.request
import os

import pytest
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
REGION = "us-east-1"
ACCOUNT_ID = "000000000000"

FAKE_CLUSTER_ARN = f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:cluster:nonexistent-cluster"
FAKE_SECRET_ARN = f"arn:aws:secretsmanager:{REGION}:{ACCOUNT_ID}:secret:nonexistent-secret"


def _raw_post(path, body):
    """Send a raw POST to the MiniStack endpoint (bypassing boto3 since
    rds-data uses REST paths like /Execute)."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{ENDPOINT}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


# ── Routing tests ──────────────────────────────────────────

def test_execute_route_exists():
    """POST /Execute reaches the rds-data handler (not a 404)."""
    status, body = _raw_post("/Execute", {})
    # Should get a 400 (missing params), not 404
    assert status == 400
    assert "BadRequestException" in str(body) or "resourceArn" in str(body)


def test_begin_transaction_route_exists():
    """POST /BeginTransaction reaches the rds-data handler."""
    status, body = _raw_post("/BeginTransaction", {})
    assert status == 400


def test_commit_transaction_route_exists():
    """POST /CommitTransaction reaches the rds-data handler."""
    status, body = _raw_post("/CommitTransaction", {})
    assert status == 400


def test_rollback_transaction_route_exists():
    """POST /RollbackTransaction reaches the rds-data handler."""
    status, body = _raw_post("/RollbackTransaction", {})
    assert status == 400


def test_batch_execute_route_exists():
    """POST /BatchExecute reaches the rds-data handler."""
    status, body = _raw_post("/BatchExecute", {})
    assert status == 400


# ── Parameter validation ───────────────────────────────────

def test_execute_missing_resource_arn():
    status, body = _raw_post("/Execute", {
        "secretArn": FAKE_SECRET_ARN,
        "sql": "SELECT 1",
    })
    assert status == 400
    assert "resourceArn" in body.get("message", body.get("Message", ""))


def test_execute_missing_secret_arn():
    status, body = _raw_post("/Execute", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "sql": "SELECT 1",
    })
    assert status == 400
    assert "secretArn" in body.get("message", body.get("Message", ""))


def test_execute_missing_sql():
    status, body = _raw_post("/Execute", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "secretArn": FAKE_SECRET_ARN,
    })
    assert status == 400
    assert "sql" in body.get("message", body.get("Message", ""))


def test_batch_execute_missing_sql():
    status, body = _raw_post("/BatchExecute", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "secretArn": FAKE_SECRET_ARN,
    })
    assert status == 400
    assert "sql" in body.get("message", body.get("Message", ""))


# ── Invalid ARN ────────────────────────────────────────────

def test_execute_nonexistent_cluster():
    """ExecuteStatement with a non-existent cluster ARN returns an error."""
    status, body = _raw_post("/Execute", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "secretArn": FAKE_SECRET_ARN,
        "sql": "SELECT 1",
    })
    assert status == 400
    assert "not found" in body.get("message", body.get("Message", "")).lower()


def test_begin_transaction_nonexistent_cluster():
    """BeginTransaction with a non-existent cluster ARN returns an error."""
    status, body = _raw_post("/BeginTransaction", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "secretArn": FAKE_SECRET_ARN,
    })
    assert status == 400
    assert "not found" in body.get("message", body.get("Message", "")).lower()


def test_batch_execute_nonexistent_cluster():
    status, body = _raw_post("/BatchExecute", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "secretArn": FAKE_SECRET_ARN,
        "sql": "INSERT INTO t VALUES (1)",
    })
    assert status == 400
    assert "not found" in body.get("message", body.get("Message", "")).lower()


# ── Transaction lifecycle (error paths) ────────────────────

def test_commit_missing_transaction_id():
    status, body = _raw_post("/CommitTransaction", {})
    assert status == 400
    assert "transactionId" in body.get("message", body.get("Message", ""))


def test_rollback_missing_transaction_id():
    status, body = _raw_post("/RollbackTransaction", {})
    assert status == 400
    assert "transactionId" in body.get("message", body.get("Message", ""))


def test_commit_nonexistent_transaction():
    status, body = _raw_post("/CommitTransaction", {
        "transactionId": "nonexistent-txn-id",
    })
    assert status == 404
    assert "not found" in body.get("message", body.get("Message", "")).lower()


def test_rollback_nonexistent_transaction():
    status, body = _raw_post("/RollbackTransaction", {
        "transactionId": "nonexistent-txn-id",
    })
    assert status == 404
    assert "not found" in body.get("message", body.get("Message", "")).lower()


# ── Invalid JSON ───────────────────────────────────────────

def test_execute_invalid_json():
    """Malformed JSON body returns BadRequestException."""
    req = urllib.request.Request(
        f"{ENDPOINT}/Execute",
        data=b"not-json{{{",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        status = resp.status
        body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        status = e.code
        body = json.loads(e.read())
    assert status == 400
    assert "Invalid JSON" in body.get("message", body.get("Message", ""))
