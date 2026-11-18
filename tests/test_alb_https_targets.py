"""HTTPS target forwarding used by a policy proxy in front of a shared ALB."""
import json
import os
import ssl
import subprocess
import threading
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")


@pytest.mark.parametrize("protocol", ["HTTP", "HTTPS"])
def test_header_selected_target_preserves_original_request(elbv2, tmp_path, protocol):
    certificate = tmp_path / "server.crt"
    key = tmp_path / "server.key"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=untrusted.target", "-keyout", str(key), "-out", str(certificate),
    ], capture_output=True, check=True)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.dumps({
                "path": self.path,
                "host": self.headers.get("Host"),
                "authorization": self.headers.get("Authorization"),
                "cookie": self.headers.get("Cookie"),
                "schemes": self.headers.get_all("X-Forwarded-Proto"),
                "body": self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode(),
            }).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    backend = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    if protocol == "HTTPS":
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(certificate, key)
        backend.socket = tls.wrap_socket(backend.socket, server_side=True)
    thread = threading.Thread(target=backend.serve_forever, daemon=True)
    thread.start()
    name = "https-target-" + uuid.uuid4().hex[:8]
    lb = elbv2.create_load_balancer(Name=name, Type="application")["LoadBalancers"][0]
    elbv2.modify_load_balancer_attributes(LoadBalancerArn=lb["LoadBalancerArn"], Attributes=[{
        "Key": "routing.http.preserve_host_header.enabled", "Value": "true",
    }])
    target = elbv2.create_target_group(
        Name=name, Port=backend.server_port, Protocol=protocol, TargetType="ip", VpcId="vpc-test",
    )["TargetGroups"][0]
    elbv2.register_targets(TargetGroupArn=target["TargetGroupArn"], Targets=[{
        "Id": "127.0.0.1", "Port": backend.server_port,
    }])
    listener = elbv2.create_listener(
        LoadBalancerArn=lb["LoadBalancerArn"], Port=80, Protocol="HTTP", DefaultActions=[{
            "Type": "fixed-response", "FixedResponseConfig": {
                "StatusCode": "404", "ContentType": "text/plain", "MessageBody": "Unknown backend",
            },
        }],
    )["Listeners"][0]
    elbv2.create_rule(ListenerArn=listener["ListenerArn"], Priority=1, Conditions=[{
        "Field": "http-header", "HttpHeaderConfig": {
            "HttpHeaderName": "X-Backend", "Values": ["clientgateway"],
        },
    }], Actions=[{"Type": "forward", "TargetGroupArn": target["TargetGroupArn"]}])
    try:
        query = "SAMLResponse=a%2Bb%2Fc&RelayState=x+y&Signature=%2F%2b%3D&q=x+y&q=x%20y"
        path = "/idp/realms/gca/Consumer/metaAlias/sp?" + query
        request = urllib.request.Request(f"{ENDPOINT}/_alb/{name}{path}", data=b"payload", headers={
            "Host": "accounts.example.test", "X-Backend": "clientgateway",
            "Authorization": "Bearer example", "Cookie": "session=example", "X-Forwarded-Proto": "https",
        })
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 200
            actual = json.load(response)
        assert actual == {
            "path": path, "host": "accounts.example.test", "authorization": "Bearer example",
            "cookie": "session=example", "schemes": ["http"], "body": "payload",
        }
    finally:
        elbv2.delete_listener(ListenerArn=listener["ListenerArn"])
        elbv2.delete_load_balancer(LoadBalancerArn=lb["LoadBalancerArn"])
        elbv2.delete_target_group(TargetGroupArn=target["TargetGroupArn"])
        backend.shutdown()
        backend.server_close()
        thread.join(timeout=5)
