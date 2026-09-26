"""Native callback tests against a local fake broker; no database/Docker needed."""

import contextlib
import http.server
import json
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parents[1] / "contrib/mysql-iam-plugin"
CAPABILITY = "a" * 64


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    compiler = shutil.which("c++")
    if not compiler:
        pytest.skip("Native broker client tests require a C++ compiler and libcurl development files")
    root = tmp_path_factory.mktemp("iam-plugin-client")
    source = root / "client.cc"
    source.write_text('''
#include "broker_client.h"
#include <iostream>
#include <iterator>
int main(int argc, char **argv) {
  if (argc != 2 || curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) return 2;
  const std::string packet((std::istreambuf_iterator<char>(std::cin)), {});
  bool allowed = false;
  try {
    allowed = ministack_iam::authorize(argv[1], std::strlen(argv[1]),
        reinterpret_cast<const unsigned char *>(packet.data()), packet.size());
  } catch (...) {}
  curl_global_cleanup();
  return allowed ? 0 : 1;
}
''')
    binary = root / "client"
    compiled = subprocess.run([compiler, "-std=c++11", "-Wall", "-Wextra", "-Werror",
                               "-I", str(SOURCE), str(source), "-lcurl", "-o", str(binary)],
                              capture_output=True, text=True)
    if compiled.returncode:
        pytest.fail(f"Native broker client compilation failed:\n{compiled.stdout}\n{compiled.stderr}", pytrace=False)
    return binary


@contextlib.contextmanager
def broker(*, status=200, body=b'{"allowed":true}', delay=0, redirect=None):
    requests = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append((self.path, dict(self.headers),
                             json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
            time.sleep(delay)
            try:
                self.send_response(status)
                if redirect:
                    self.send_header("Location", redirect)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *_args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port, requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def invoke(client, port, *, user="AppUser", token=b"sdk-token\0", env=None):
    config = dict(os.environ, MINISTACK_RDS_IAM_BROKER_HOST="127.0.0.1",
                  MINISTACK_RDS_IAM_BROKER_PORT=str(port), MINISTACK_RDS_IAM_CAPABILITY=CAPABILITY)
    config.update(env or {})
    result = subprocess.run([str(client), user], input=token, env=config, capture_output=True, timeout=6)
    assert result.stdout == result.stderr == b""
    assert result.returncode in (0, 1)
    return result.returncode == 0


def test_native_client_forwards_username_and_token_without_truncation(client):
    token = b'host:3306/?DBUser=AppUser&session=' + b'x' * 8192 + b'"\\\n'
    with broker() as (port, requests):
        assert invoke(client, port, user='App"User', token=token + b"\0")
        assert requests == [("/_ministack/rds/iam-auth", requests[0][1],
                             {"username": 'App"User', "token": token.decode()})]
        assert requests[0][1]["X-Ministack-RDS-Capability"] == CAPABILITY
        assert requests[0][1]["Content-Type"] == "application/json"


@pytest.mark.parametrize("status,body", [
    (403, b'{"allowed":false}'), (500, b'{"allowed":true}'), (204, b""),
    (200, b""), (200, b"true"), (200, b'{"allowed":"true"}'),
    (200, b'{"allowed":true,"allowed":false}'), (200, b'{"allowed":true}junk'),
    (200, b'{"allowed":true,"extra":1}'), (200, b"x" * 1025),
])
def test_native_client_denies_noncanonical_responses(client, status, body):
    with broker(status=status, body=body) as (port, _):
        assert not invoke(client, port)


@pytest.mark.parametrize("env", [
    {"MINISTACK_RDS_IAM_BROKER_HOST": ""}, {"MINISTACK_RDS_IAM_BROKER_HOST": "localhost"},
    {"MINISTACK_RDS_IAM_BROKER_PORT": "0"}, {"MINISTACK_RDS_IAM_BROKER_PORT": "65536"},
    {"MINISTACK_RDS_IAM_BROKER_PORT": "80/path"}, {"MINISTACK_RDS_IAM_CAPABILITY": ""},
    {"MINISTACK_RDS_IAM_CAPABILITY": "a" * 63 + "\n"},
])
def test_native_client_denies_invalid_configuration_without_callback(client, env):
    with broker() as (port, requests):
        assert not invoke(client, port, env=env)
        assert requests == []


@pytest.mark.parametrize("packet", [b"", b"missing-nul", b"embedded\0nul\0", b"x" * 65537 + b"\0"])
def test_native_client_denies_invalid_packets_without_callback(client, packet):
    with broker() as (port, requests):
        assert not invoke(client, port, token=packet)
        assert requests == []


def test_native_client_allows_empty_token_only_on_explicit_broker_allow(client):
    # AUTH=false belongs to the broker, not to the C++ adapter.
    with broker() as (port, requests):
        assert invoke(client, port, token=b"\0")
        assert requests[0][2]["token"] == ""


def test_native_client_never_follows_redirects(client):
    with broker() as (other_port, other_requests):
        with broker(status=307, redirect=f"http://127.0.0.1:{other_port}/") as (port, _):
            assert not invoke(client, port)
        assert other_requests == []


def test_native_client_ignores_proxy_environment(client):
    with broker() as (proxy_port, proxy_requests):
        with broker() as (port, _):
            assert invoke(client, port, env={"http_proxy": f"http://127.0.0.1:{proxy_port}",
                                            "ALL_PROXY": f"http://127.0.0.1:{proxy_port}", "NO_PROXY": ""})
        assert proxy_requests == []


def test_native_client_bounds_callback_time(client):
    with broker(delay=4) as (port, _):
        start = time.monotonic()
        assert not invoke(client, port)
        assert time.monotonic() - start < 5


def test_native_client_connection_failure_denies(client):
    with broker() as (port, _):
        pass
    assert not invoke(client, port)
