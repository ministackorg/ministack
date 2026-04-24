"""
E2E test: Lambda containers can reach ElastiCache containers over DOCKER_NETWORK.

Skipped when DOCKER_NETWORK is not set (requires Docker networking).
"""
import io
import json
import os
import time
import zipfile

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("DOCKER_NETWORK"),
    reason="DOCKER_NETWORK not set — skipping network connectivity test",
)

_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


def _make_zip_js(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.js", code)
    return buf.getvalue()


def test_elasticache_lambda_network_connectivity(ec, lam):
    """Prove that Lambda containers can TCP-connect to an ElastiCache container."""
    cluster_id = "net-test-redis"
    fn_py = "ec-net-test-py"
    fn_js = "ec-net-test-js"

    # 1. Create ElastiCache Redis cluster
    ec.create_cache_cluster(
        CacheClusterId=cluster_id,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )

    try:
        resp = ec.describe_cache_clusters(CacheClusterId=cluster_id)
        cluster = resp["CacheClusters"][0]
        node = cluster["CacheNodes"][0]
        host = node["Endpoint"]["Address"]
        port = int(node["Endpoint"]["Port"])

        # 2. Endpoint.Address must NOT be localhost when DOCKER_NETWORK is set
        assert host not in ("localhost", "redis"), (
            f"Expected container IP, got '{host}' — DOCKER_NETWORK not working"
        )

        # 3. Wait for Redis container to accept connections
        import socket
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                with socket.create_connection((host, port), timeout=2):
                    break
            except OSError:
                time.sleep(1)
        else:
            pytest.fail(f"ElastiCache container at {host}:{port} not reachable after 60s")

        # 4. Python Lambda — TCP connect to ElastiCache endpoint
        py_code = f"""\
import socket, json
def handler(event, context):
    try:
        s = socket.create_connection(("{host}", {port}), timeout=5)
        s.close()
        return {{"connected": True}}
    except Exception as e:
        return {{"connected": False, "error": str(e)}}
"""
        lam.create_function(
            FunctionName=fn_py,
            Runtime="python3.12",
            Role=_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _make_zip(py_code)},
            Timeout=15,
        )

        resp = lam.invoke(FunctionName=fn_py, Payload=json.dumps({}))
        result = json.loads(resp["Payload"].read())
        assert result.get("connected") is True, f"Python Lambda failed: {result}"

        # 5. JS Lambda — TCP connect to ElastiCache endpoint
        js_code = f"""\
const net = require("net");
exports.handler = async (event) => {{
    return new Promise((resolve) => {{
        const sock = new net.Socket();
        sock.setTimeout(5000);
        sock.connect({port}, "{host}", () => {{
            sock.destroy();
            resolve({{ connected: true }});
        }});
        sock.on("error", (err) => {{
            sock.destroy();
            resolve({{ connected: false, error: err.message }});
        }});
        sock.on("timeout", () => {{
            sock.destroy();
            resolve({{ connected: false, error: "timeout" }});
        }});
    }});
}};
"""
        lam.create_function(
            FunctionName=fn_js,
            Runtime="nodejs20.x",
            Role=_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _make_zip_js(js_code)},
            Timeout=15,
        )

        resp = lam.invoke(FunctionName=fn_js, Payload=json.dumps({}))
        result = json.loads(resp["Payload"].read())
        assert result.get("connected") is True, f"JS Lambda failed: {result}"

    finally:
        # 6. Cleanup
        for fn in (fn_py, fn_js):
            try:
                lam.delete_function(FunctionName=fn)
            except Exception:
                pass
        try:
            ec.delete_cache_cluster(CacheClusterId=cluster_id)
        except Exception:
            pass
