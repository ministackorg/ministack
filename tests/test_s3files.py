"""S3 Files (s3files-2025-05-05) tests.

Driven via raw HTTP since botocore doesn't ship `s3files` in the version
pinned for the rest of the suite. The wire format here is exactly what
the AWS CLI / SDKs send: REST/JSON with camelCase fields and the routes
documented at docs.aws.amazon.com/AmazonS3/.../API_Operations_Amazon_S3_Files.html.
"""

import json
import urllib.error
import urllib.parse
import urllib.request
import uuid


ENDPOINT = "http://localhost:4566"
BUCKET_ARN = "arn:aws:s3:::test-bucket"
ROLE_ARN = "arn:aws:iam::000000000000:role/s3files-role"


def _auth(account="000000000000"):
    return (
        f"AWS4-HMAC-SHA256 "
        f"Credential={account}/20260504/us-east-1/s3files/aws4_request, "
        f"SignedHeaders=host, Signature=00"
    )


_AUTH = _auth()


def _req(method, path, body=None, query=None, account=None):
    url = ENDPOINT + path
    if query:
        url += "?" + urllib.parse.urlencode(query, doseq=True)
    data = None
    headers = {"Authorization": _auth(account) if account else _AUTH}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        raw = resp.read()
        return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw.decode("latin-1", "replace")}


def _uid():
    return uuid.uuid4().hex[:6]


# ---------------------------------------------------------------------------
# File systems
# ---------------------------------------------------------------------------

def test_create_file_system_uses_put_and_camel_case():
    status, body = _req("PUT", "/file-systems", {
        "bucket": BUCKET_ARN,
        "roleArn": ROLE_ARN,
        "tags": [{"key": "Name", "value": f"fs-{_uid()}"}],
    })
    assert status == 201, body
    assert body["fileSystemId"].startswith("fs-")
    assert body["fileSystemArn"].startswith("arn:aws:s3files:")
    assert body["bucket"] == BUCKET_ARN
    assert body["roleArn"] == ROLE_ARN
    assert body["status"] == "available"
    assert isinstance(body["creationTime"], int)
    assert body["ownerId"] == "000000000000"
    fs_id = body["fileSystemId"]
    _req("DELETE", f"/file-systems/{fs_id}")


def test_create_file_system_post_is_not_supported():
    status, body = _req("POST", "/file-systems", {
        "bucket": BUCKET_ARN,
        "roleArn": ROLE_ARN,
    })
    assert status == 400
    assert body.get("__type") == "ValidationException"


def test_create_file_system_missing_required_fields():
    status, body = _req("PUT", "/file-systems", {})
    assert status == 400
    assert body.get("__type") == "ValidationException"


def test_get_and_list_file_systems_camel_case():
    s, body = _req("PUT", "/file-systems", {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN})
    fs_id = body["fileSystemId"]
    try:
        s, got = _req("GET", f"/file-systems/{fs_id}")
        assert s == 200
        assert got["fileSystemId"] == fs_id
        assert got["bucket"] == BUCKET_ARN

        s, listed = _req("GET", "/file-systems")
        assert s == 200
        assert any(fs["fileSystemId"] == fs_id for fs in listed["fileSystems"])

        s, filtered = _req("GET", "/file-systems", query={"bucket": BUCKET_ARN})
        assert s == 200
        assert all(fs["bucket"] == BUCKET_ARN for fs in filtered["fileSystems"])
    finally:
        _req("DELETE", f"/file-systems/{fs_id}")


def test_get_file_system_not_found():
    s, body = _req("GET", "/file-systems/fs-deadbeefdeadbeefdead")
    assert s == 404
    assert body.get("__type") == "ResourceNotFoundException"


def test_delete_file_system_returns_204():
    s, body = _req("PUT", "/file-systems", {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN})
    fs_id = body["fileSystemId"]
    s, _ = _req("DELETE", f"/file-systems/{fs_id}")
    assert s == 204


# ---------------------------------------------------------------------------
# Mount targets
# ---------------------------------------------------------------------------

def test_mount_target_lifecycle():
    s, fs = _req("PUT", "/file-systems", {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN})
    fs_id = fs["fileSystemId"]
    try:
        s, mt = _req("PUT", "/mount-targets", {
            "fileSystemId": fs_id,
            "subnetId": "subnet-12345678",
            "securityGroups": ["sg-12345678"],
        })
        assert s == 200
        assert mt["mountTargetId"].startswith("fsmt-")
        assert mt["fileSystemId"] == fs_id
        assert mt["securityGroups"] == ["sg-12345678"]
        mt_id = mt["mountTargetId"]

        s, got = _req("GET", f"/mount-targets/{mt_id}")
        assert s == 200 and got["mountTargetId"] == mt_id

        s, listed = _req("GET", "/mount-targets", query={"fileSystemId": fs_id})
        assert s == 200
        assert any(m["mountTargetId"] == mt_id for m in listed["mountTargets"])

        s, updated = _req("PUT", f"/mount-targets/{mt_id}", {
            "securityGroups": ["sg-aaaaaaaa", "sg-bbbbbbbb"],
        })
        assert s == 200
        assert updated["securityGroups"] == ["sg-aaaaaaaa", "sg-bbbbbbbb"]

        s, _ = _req("DELETE", f"/mount-targets/{mt_id}")
        assert s == 204
    finally:
        _req("DELETE", f"/file-systems/{fs_id}")


def test_create_mount_target_requires_filesystem():
    s, body = _req("PUT", "/mount-targets", {"subnetId": "subnet-12345678"})
    assert s == 400
    assert body.get("__type") == "ValidationException"


def test_update_mount_target_requires_security_groups():
    s, fs = _req("PUT", "/file-systems", {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN})
    fs_id = fs["fileSystemId"]
    try:
        s, mt = _req("PUT", "/mount-targets", {
            "fileSystemId": fs_id,
            "subnetId": "subnet-12345678",
        })
        s, body = _req("PUT", f"/mount-targets/{mt['mountTargetId']}", {})
        assert s == 400
        assert body.get("__type") == "ValidationException"
    finally:
        _req("DELETE", f"/file-systems/{fs_id}")


# ---------------------------------------------------------------------------
# Access points
# ---------------------------------------------------------------------------

def test_access_point_lifecycle():
    s, fs = _req("PUT", "/file-systems", {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN})
    fs_id = fs["fileSystemId"]
    try:
        s, ap = _req("PUT", "/access-points", {
            "fileSystemId": fs_id,
            "posixUser": {"uid": 1000, "gid": 1000},
            "rootDirectory": {"path": "/data"},
            "tags": [{"key": "Name", "value": "ap-test"}],
        })
        assert s == 200
        assert ap["accessPointId"].startswith("fsap-")
        assert ap["fileSystemId"] == fs_id
        assert ap["accessPointArn"].startswith(f"arn:aws:s3files:")
        assert "/access-point/" in ap["accessPointArn"]
        assert ap["name"] == "ap-test"

        s, listed = _req("GET", "/access-points", query={"fileSystemId": fs_id})
        assert s == 200
        assert any(a["accessPointId"] == ap["accessPointId"] for a in listed["accessPoints"])

        s, _ = _req("DELETE", f"/access-points/{ap['accessPointId']}")
        assert s == 204
    finally:
        _req("DELETE", f"/file-systems/{fs_id}")


def test_list_access_points_requires_filesystem_id():
    s, body = _req("GET", "/access-points")
    assert s == 400
    assert body.get("__type") == "ValidationException"


# ---------------------------------------------------------------------------
# File-system policy
# ---------------------------------------------------------------------------

def test_file_system_policy_lifecycle():
    s, fs = _req("PUT", "/file-systems", {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN})
    fs_id = fs["fileSystemId"]
    try:
        policy_doc = json.dumps({"Version": "2012-10-17", "Statement": []})
        s, _ = _req("PUT", f"/file-systems/{fs_id}/policy", {"policy": policy_doc})
        assert s == 200

        s, got = _req("GET", f"/file-systems/{fs_id}/policy")
        assert s == 200
        assert got["fileSystemId"] == fs_id
        assert got["policy"] == policy_doc

        s, _ = _req("DELETE", f"/file-systems/{fs_id}/policy")
        assert s == 204

        s, body = _req("GET", f"/file-systems/{fs_id}/policy")
        assert s == 404
    finally:
        _req("DELETE", f"/file-systems/{fs_id}")


# ---------------------------------------------------------------------------
# Synchronization configuration
# ---------------------------------------------------------------------------

def test_synchronization_configuration_lifecycle_and_optimistic_locking():
    s, fs = _req("PUT", "/file-systems", {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN})
    fs_id = fs["fileSystemId"]
    try:
        s, _ = _req("PUT", f"/file-systems/{fs_id}/synchronization-configuration", {
            "expirationDataRules": [{"daysAfterLastAccess": 7}],
            "importDataRules": [{"prefix": "", "trigger": "NEW_OBJECTS"}],
        })
        assert s == 200

        s, got = _req("GET", f"/file-systems/{fs_id}/synchronization-configuration")
        assert s == 200
        assert got["latestVersionNumber"] == 1
        assert got["expirationDataRules"][0]["daysAfterLastAccess"] == 7

        # stale version → 409
        s, body = _req("PUT", f"/file-systems/{fs_id}/synchronization-configuration", {
            "expirationDataRules": [{"daysAfterLastAccess": 14}],
            "importDataRules": [{"prefix": "", "trigger": "NEW_OBJECTS"}],
            "latestVersionNumber": 99,
        })
        assert s == 409
        assert body.get("__type") == "ConflictException"

        # correct version → 200, bump
        s, _ = _req("PUT", f"/file-systems/{fs_id}/synchronization-configuration", {
            "expirationDataRules": [{"daysAfterLastAccess": 14}],
            "importDataRules": [{"prefix": "", "trigger": "NEW_OBJECTS"}],
            "latestVersionNumber": 1,
        })
        assert s == 200
        s, got = _req("GET", f"/file-systems/{fs_id}/synchronization-configuration")
        assert got["latestVersionNumber"] == 2
    finally:
        _req("DELETE", f"/file-systems/{fs_id}")


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------

def test_resource_tags_lifecycle_uses_resource_tags_path():
    s, fs = _req("PUT", "/file-systems", {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN})
    fs_id = fs["fileSystemId"]
    try:
        s, _ = _req("POST", f"/resource-tags/{fs_id}", {
            "tags": [{"key": "Env", "value": "test"}, {"key": "Owner", "value": "ministack"}],
        })
        assert s == 200

        s, listed = _req("GET", f"/resource-tags/{fs_id}")
        assert s == 200
        tags = {t["key"]: t["value"] for t in listed["tags"]}
        assert tags == {"Env": "test", "Owner": "ministack"}

        s, _ = _req("DELETE", f"/resource-tags/{fs_id}", query={"tagKeys": ["Env"]})
        assert s == 200

        s, listed = _req("GET", f"/resource-tags/{fs_id}")
        assert {t["key"] for t in listed["tags"]} == {"Owner"}
    finally:
        _req("DELETE", f"/file-systems/{fs_id}")


def test_tag_unknown_resource_returns_404():
    s, body = _req("POST", "/resource-tags/fs-deadbeefdeadbeefdead", {
        "tags": [{"key": "k", "value": "v"}],
    })
    assert s == 404
    assert body.get("__type") == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Multi-tenancy
# ---------------------------------------------------------------------------

def test_account_isolation_for_file_systems():
    a, b = "111111111111", "222222222222"
    s, fs = _req("PUT", "/file-systems",
                 {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN}, account=a)
    fs_id = fs["fileSystemId"]
    try:
        s, listed_a = _req("GET", "/file-systems", account=a)
        s, listed_b = _req("GET", "/file-systems", account=b)
        ids_a = {f["fileSystemId"] for f in listed_a["fileSystems"]}
        ids_b = {f["fileSystemId"] for f in listed_b["fileSystems"]}
        assert fs_id in ids_a
        assert fs_id not in ids_b

        s, body = _req("GET", f"/file-systems/{fs_id}", account=b)
        assert s == 404
        assert body.get("__type") == "ResourceNotFoundException"

        s, body = _req("DELETE", f"/file-systems/{fs_id}", account=b)
        assert s == 404
    finally:
        _req("DELETE", f"/file-systems/{fs_id}", account=a)


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

def test_list_file_systems_pagination_walks_all_items():
    created = []
    try:
        for _ in range(7):
            _, fs = _req("PUT", "/file-systems",
                         {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN})
            created.append(fs["fileSystemId"])

        seen = []
        token = None
        for _ in range(10):  # safety bound; real loop terminates by None
            q = {"maxResults": 3}
            if token:
                q["nextToken"] = token
            s, page = _req("GET", "/file-systems", query=q)
            assert s == 200
            assert len(page["fileSystems"]) <= 3
            seen.extend(f["fileSystemId"] for f in page["fileSystems"])
            token = page.get("nextToken")
            if not token:
                break
        else:
            raise AssertionError("pagination did not terminate")

        assert set(created).issubset(set(seen))
        # No duplicates across pages.
        assert len(seen) == len(set(seen))
    finally:
        for fs_id in created:
            _req("DELETE", f"/file-systems/{fs_id}")


# ---------------------------------------------------------------------------
# ARN-form resourceId acceptance
# ---------------------------------------------------------------------------

def test_body_parameter_accepts_full_arn_form():
    """AWS shapes accept either a bare ID (`fs-xxx`) or a full ARN for
    `fileSystemId` request fields. Verify the ARN form is normalized so
    downstream lookups match the bare-ID storage key.

    Path-parameter ARNs are not exercised: ASGI delivers the path already
    URL-decoded, which collides with the `/` inside the ARN. Real SDKs
    typically pass the bare ID in the URI and only use the ARN form in the
    JSON body, which is the case covered here.
    """
    s, fs = _req("PUT", "/file-systems", {"bucket": BUCKET_ARN, "roleArn": ROLE_ARN})
    fs_id = fs["fileSystemId"]
    fs_arn = fs["fileSystemArn"]
    try:
        # CreateMountTarget with the ARN form of fileSystemId.
        s, mt = _req("PUT", "/mount-targets", {
            "fileSystemId": fs_arn,
            "subnetId": "subnet-12345678",
        })
        assert s == 200, mt
        assert mt["fileSystemId"] == fs_id  # normalized back to bare ID
        _req("DELETE", f"/mount-targets/{mt['mountTargetId']}")

        # CreateAccessPoint with the ARN form too.
        s, ap = _req("PUT", "/access-points", {"fileSystemId": fs_arn})
        assert s == 200, ap
        assert ap["fileSystemId"] == fs_id
        _req("DELETE", f"/access-points/{ap['accessPointId']}")
    finally:
        _req("DELETE", f"/file-systems/{fs_id}")
