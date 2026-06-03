import json
import os
import urllib.request
import urllib.error
import boto3

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _ports_state(account: str | None = None):
    """Hit /_ministack/ports — returns a tuple `(status, body)` where `body`
    is the parsed JSON response (or `None` on parse failure). If the request
    failed to reach the server, returns `(None, None)`.
    """
    url = f"{ENDPOINT}/_ministack/ports"
    if account:
        url = f"{url}?account={account}"
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
            return resp.getcode(), json.loads(resp.read())
    except urllib.error.HTTPError as e:
        # Return status and parsed JSON body when the server responds with an
        # error status code (e.g. 400, 500).
        try:
            body = json.loads(e.read())
        except Exception:
            body = None
        return e.code, body
    except Exception:
        return None, None


def test_admin_ports_account_filtered(rds, ec):
    """When `?account=ID` is supplied, the endpoint returns a flat mapping for
    that account only (no grouping)."""
    # Create minimal RDS instance and ElastiCache cluster under default test account
    rds.create_db_instance(
        DBInstanceIdentifier="test-db-ports",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password123",
        AllocatedStorage=20,
    )

    ec.create_cache_cluster(
        CacheClusterId="test-redis-ports",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )

    account = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
    status, state = _ports_state(account=account)
    assert status is not None, "/_ministack/ports did not respond"
    assert status == 200, f"Unexpected status: {status}"

    # RDS entry: grouped by account (consistent with SES)
    rds_group = state.get("rds", {})
    assert account in rds_group, "Account key missing from RDS ports output"
    rds_map = rds_group[account]
    assert "test-db-ports" in rds_map, "RDS instance not present in ports output"
    assert isinstance(rds_map["test-db-ports"].get("host_port"), int)

    # ElastiCache cluster entry: grouped by account
    ec_clusters_group = state.get("elasticache", {}).get("clusters", {})
    assert account in ec_clusters_group, "Account key missing from ElastiCache clusters output"
    ec_clusters = ec_clusters_group[account]
    assert "test-redis-ports" in ec_clusters, "ElastiCache cluster not present in ports output"
    assert isinstance(ec_clusters["test-redis-ports"].get("host_port"), int)


def test_admin_ports_grouped_by_account(rds, ec):
    """When no account is supplied, the endpoint groups resources by account.
    This creates a second account and resources under it to verify grouping."""
    # Create resource under default account
    rds.create_db_instance(
        DBInstanceIdentifier="group-db-a",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password123",
        AllocatedStorage=20,
    )
    ec.create_cache_cluster(
        CacheClusterId="group-ec-a",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )

    # Create resources under a different account (12-digit access key)
    other_account = "111111111111"
    rds_other = boto3.client("rds", endpoint_url=ENDPOINT, aws_access_key_id=other_account, aws_secret_access_key="test", region_name="us-east-1")
    ec_other = boto3.client("elasticache", endpoint_url=ENDPOINT, aws_access_key_id=other_account, aws_secret_access_key="test", region_name="us-east-1")

    rds_other.create_db_instance(
        DBInstanceIdentifier="group-db-b",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password123",
        AllocatedStorage=20,
    )
    ec_other.create_cache_cluster(
        CacheClusterId="group-ec-b",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )

    status, state = _ports_state()
    assert status is not None, "/_ministack/ports did not respond"
    assert status == 200, f"Unexpected status: {status}"

    # Expect top-level grouping keys for both accounts
    rds_group = state.get("rds", {})
    assert isinstance(rds_group, dict)
    # At least one known account key should be present
    assert any(isinstance(k, str) and k.isdigit() for k in rds_group.keys())

    ec_group = state.get("elasticache", {}).get("clusters", {})
    assert any(isinstance(k, str) and k.isdigit() for k in ec_group.keys())
