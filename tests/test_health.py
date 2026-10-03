import json
import os
import pathlib
import re
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")


def test_health_endpoint():
    import urllib.request

    resp = urllib.request.urlopen(f"{ENDPOINT}/_ministack/health")
    assert resp.status == 200
    data = json.loads(resp.read())
    assert "services" in data
    assert "s3" in data["services"]

def test_health_endpoint_ministack():
    import urllib.request

    resp = urllib.request.urlopen(f"{ENDPOINT}/_ministack/health")
    assert resp.status == 200
    data = json.loads(resp.read())
    assert data["edition"] == "light"


_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _healthcheck_code(dockerfile):
    match = re.search(r'^\s*CMD python -c "(.*)" \|\| exit 1$', (_ROOT / dockerfile).read_text(), re.M)
    assert match, f"no HEALTHCHECK command in {dockerfile}"
    return match.group(1)


@pytest.mark.parametrize("dockerfile", ["Dockerfile", "Dockerfile.full"])
@pytest.mark.parametrize("port_var", ["GATEWAY_PORT", "EDGE_PORT"])
def test_image_healthcheck_probes_the_configured_port(dockerfile, port_var):
    paths = []

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            paths.append(self.path)
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        env = {port_var: str(server.server_address[1])}
        result = subprocess.run(
            [sys.executable, "-c", _healthcheck_code(dockerfile)], env=env, capture_output=True, timeout=10
        )
    finally:
        server.shutdown()
        server.server_close()
    assert result.returncode == 0, result.stderr.decode()
    assert paths == ["/_ministack/health"]
