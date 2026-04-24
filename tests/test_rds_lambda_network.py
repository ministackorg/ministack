"""
E2E test: Lambda containers can reach RDS containers over DOCKER_NETWORK.

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


def _wait_for_rds(rds_client, db_id, timeout=120):
    """Poll DescribeDBInstances until the instance is available."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = rds_client.describe_db_instances(DBInstanceIdentifier=db_id)
        inst = resp["DBInstances"][0]
        if inst["DBInstanceStatus"] == "available":
            return inst
        time.sleep(2)
    raise TimeoutError(f"RDS instance {db_id} not available after {timeout}s")


def test_rds_lambda_network_connectivity(rds, lam):
    """Prove that Lambda containers can TCP-connect to an RDS container."""
    db_id = "net-test-pg"
    fn_py = "rds-net-test-py"
    fn_js = "rds-net-test-js"

    # 1. Create RDS Postgres instance
    rds.create_db_instance(
        DBInstanceIdentifier=db_id,
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )

    try:
        inst = _wait_for_rds(rds, db_id)
        endpoint = inst["Endpoint"]
        host = endpoint["Address"]
        port = int(endpoint["Port"])

        # 2. Endpoint.Address must NOT be localhost when DOCKER_NETWORK is set
        assert host != "localhost", (
            f"Expected container IP, got 'localhost' — DOCKER_NETWORK not working"
        )

        # 3. Wait for the Postgres container to accept connections
        import socket
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                with socket.create_connection((host, port), timeout=2):
                    break
            except OSError:
                time.sleep(1)
        else:
            pytest.fail(f"RDS container at {host}:{port} not reachable after 60s")

        # 4. Python Lambda — TCP connect to RDS endpoint
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

        # 5. JS Lambda — TCP connect to RDS endpoint
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
            rds.delete_db_instance(
                DBInstanceIdentifier=db_id, SkipFinalSnapshot=True
            )
        except Exception:
            pass
