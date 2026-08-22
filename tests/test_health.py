import json
import os


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
