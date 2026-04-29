import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


def test_health_endpoint():
    import urllib.request

    resp = urllib.request.urlopen("http://localhost:4566/_ministack/health")
    assert resp.status == 200
    data = json.loads(resp.read())
    assert "services" in data
    assert "s3" in data["services"]

def test_health_endpoint_ministack():
    import urllib.request

    resp = urllib.request.urlopen("http://localhost:4566/_ministack/health")
    assert resp.status == 200
    data = json.loads(resp.read())
    assert data["edition"] == "light"
