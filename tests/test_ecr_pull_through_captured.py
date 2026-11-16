"""ECR pull-through cache, registry policy and the answers around them: every captured AWS error (issue 2617).

`tests/fixtures/ecr_pull_through/captured_errors.json` holds one row per non-2xx AWS record of the actions this
change adds or changes: the capture key, action, request body, status, `__type` and message as captured. Names are
normalised to this test's own, accounts to 000000000000 (account 2 to 000000000002), and the signing principal in
an AccessDenied text to `{caller}`. Each row names the setup it needs; the test builds it with unique names,
sends the body through the HTTP endpoint and asserts status, `__type` and the whole message.
"""

import json
import os
import re
import urllib.parse

import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

pytestmark = pytest.mark.serial  # shares the registry-wide rule quota and registry policy

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
_FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "ecr_pull_through", "captured_errors.json")
_ROWS = [dict(row, index=i) for i, row in enumerate(json.load(open(_FIXTURE)))]
_TOKEN = re.compile(r"\{(?:P\d+|S\d+n?|caller)\}")
_TARGETS = {
    "ecr": "AmazonEC2ContainerRegistry_V20150921",
    "secretsmanager": "secretsmanager",
    "kms": "TrentService",
}
_ACCOUNT = {1: "000000000000", 2: "000000000002"}


def _send(account, service, action, body):
    """Sign with the account's own key (account 1 is the default `test` root) and post a JSON-protocol request."""
    key = "test" if account == 1 else _ACCOUNT[account]
    data = body if isinstance(body, str) else json.dumps(body)
    url = ENDPOINT + "/"
    req = AWSRequest(
        method="POST",
        url=url,
        data=data,
        headers={
            "X-Amz-Target": f"{_TARGETS[service]}.{action}",
            "Content-Type": "application/x-amz-json-1.1",
            "Host": urllib.parse.urlparse(ENDPOINT).netloc,
        },
    )
    SigV4Auth(Credentials(key, "test"), service, "us-east-1").add_auth(req)
    return requests.post(url, data=data.encode(), headers=dict(req.headers.items()))


class _Row:
    """Builds a row's setup with unique names, sends its body and cleans up what it created."""

    def __init__(self, row):
        self.row = row
        self.account = row["account"]
        self.uid = f"ptc{row['index']:04d}"
        self.tokens = {"{caller}": f"arn:aws:iam::{_ACCOUNT[self.account]}:root"}
        self.secret_names = []
        self.key_ids = []
        self.repos = []
        self.root = False

    def sub(self, value):
        if isinstance(value, str):
            return _TOKEN.sub(lambda m: self.tokens.get(m.group(0), m.group(0)), value)
        if isinstance(value, list):
            return [self.sub(v) for v in value]
        if isinstance(value, dict):
            return {k: self.sub(v) for k, v in value.items()}
        return value

    def call(self, service, action, body):
        return _send(self.account, service, action, body)

    def must(self, service, action, body):
        r = self.call(service, action, body)
        assert r.status_code == 200, f"setup {action} {body}: {r.status_code} {r.text}"
        return r.json() if r.text else {}

    def build(self):
        row, setup = self.row, self.row["setup"]
        for token in sorted({t for t in _TOKEN.findall(json.dumps(row))}):
            if token.startswith("{P"):
                self.tokens[token] = f"{self.uid}p{token[2:-1]}"
        for i, (token, spec) in enumerate(setup.get("secrets", {}).items()):
            name = f"ecr-pullthroughcache/{self.uid}-s{i}"
            self.secret_names.append(name)
            region, acct = "us-east-1", _ACCOUNT[self.account]
            if spec["state"] == "missing":
                self.tokens[token] = f"arn:aws:secretsmanager:{region}:{acct}:secret:{name}-AbCdEf"
                continue
            create = {"Name": name}
            if spec.get("value") == "binary":
                create["SecretBinary"] = "AQID"
            elif spec.get("value") != "none":
                create["SecretString"] = json.dumps({"username": "u", "accessToken": "t"})
            if spec.get("kms"):
                key_id = self.must("kms", "CreateKey", {})["KeyMetadata"]["KeyId"]
                self.key_ids.append(key_id)
                create["KmsKeyId"] = key_id
            self.tokens[token] = self.must("secretsmanager", "CreateSecret", create)["ARN"]
        for token in [t for t in self.tokens if t.startswith("{S")]:
            self.tokens[token[:-1] + "n}"] = self.tokens[token].rsplit("-", 1)[0]  # the ARN without its random suffix
        if setup.get("clear_registry_policy"):
            self.call("ecr", "DeleteRegistryPolicy", {})
        for rule in setup.get("rules", []):
            body = self.sub(rule)
            if body["ecrRepositoryPrefix"] == "ROOT":
                self.root = True
            self.must("ecr", "CreatePullThroughCacheRule", body)
        for repo in [setup.get("repo"), setup.get("repo_with_image")]:
            if not repo:
                continue
            name = self.sub(repo["name"])
            self.repos.append(name)
            self.must("ecr", "CreateRepository", {"repositoryName": name})
            if repo.get("manifest") is not None or "repo_with_image" in setup:
                manifest = self.sub(repo["manifest"]) if repo.get("manifest") else json.dumps(_MANIFEST)
                self.must(
                    "ecr", "PutImage", {"repositoryName": name, "imageManifest": manifest, "imageTag": repo["tag"]}
                )
        if setup.get("fill_quota"):
            have = len(self.must("ecr", "DescribePullThroughCacheRules", {"maxResults": 1000})["pullThroughCacheRules"])
            for n in range(50 - have):
                self.must(
                    "ecr",
                    "CreatePullThroughCacheRule",
                    {"ecrRepositoryPrefix": f"{self.uid}q{n}", "upstreamRegistryUrl": "public.ecr.aws"},
                )
        for token, spec in setup.get("secrets", {}).items():
            arn = self.tokens[token]
            if spec["state"] == "deleted":
                self.must("secretsmanager", "DeleteSecret", {"SecretId": arn, "RecoveryWindowInDays": 7})
            if spec.get("kms"):
                key_id = create_key = self.key_ids[0]
                if spec["kms"] == "disabled":
                    self.must("kms", "DisableKey", {"KeyId": create_key})
                else:
                    self.must("kms", "ScheduleKeyDeletion", {"KeyId": key_id, "PendingWindowInDays": 7})
            if spec.get("policy"):
                self.must(
                    "secretsmanager", "PutResourcePolicy", {"SecretId": arn, "ResourcePolicy": self.sub(spec["policy"])}
                )

    def send(self):
        row = self.row
        if "body" in row:
            body = json.dumps(self.sub(row["body"]))
        else:
            body = self.sub(row["body_raw"])
        return self.call("ecr" if row["action"] not in _OTHER else _OTHER[row["action"]], row["action"], body)

    def cleanup(self):
        listed = self.call("ecr", "DescribePullThroughCacheRules", {"maxResults": 1000})
        if listed.status_code == 200:
            for rule in listed.json()["pullThroughCacheRules"]:
                if rule["ecrRepositoryPrefix"].startswith(self.uid) or (
                    self.root and rule["ecrRepositoryPrefix"] == "ROOT"
                ):
                    self.call("ecr", "DeletePullThroughCacheRule", {"ecrRepositoryPrefix": rule["ecrRepositoryPrefix"]})
        for name in self.repos:
            self.call("ecr", "DeleteRepository", {"repositoryName": name, "force": True})
        for name in self.secret_names:
            self.call("secretsmanager", "DeleteSecret", {"SecretId": name, "ForceDeleteWithoutRecovery": True})
        for key_id in self.key_ids:
            self.call("kms", "ScheduleKeyDeletion", {"KeyId": key_id, "PendingWindowInDays": 7})


_OTHER = {}
_MANIFEST = {
    "schemaVersion": 2,
    "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
    "config": {
        "mediaType": "application/vnd.docker.container.image.v1+json",
        "size": 2,
        "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
    },
    "layers": [],
}


def _run(row):
    ctx = _Row(row)
    try:
        ctx.build()
        r = ctx.send()
        assert r.status_code == row["status"], (row["key"], r.text)
        body = r.json() if r.text else {}
        got_type = (body.get("__type") or "").split("#")[-1]
        assert got_type == row["type"], (row["key"], r.text)
        got = body.get("message", body.get("Message"))
        want = ctx.sub(row["message"]) if row["message"] is not None else None
        assert got == want, (row["key"], got, want)
    finally:
        ctx.cleanup()


@pytest.mark.parametrize("row", [r for r in _ROWS if not r.get("auth")], ids=lambda r: r["key"])
def test_captured_error(row):
    _run(row)


@pytest.mark.auth_contract
@pytest.mark.parametrize("row", [r for r in _ROWS if r.get("auth")], ids=lambda r: r["key"])
def test_captured_error_needing_auth(row, auth_enabled):
    if not auth_enabled:
        pytest.skip("this answer needs the caller identity: run with AUTH=true")
    _run(row)


def test_named_keys_are_in_the_table():
    keys = {r["key"] for r in _ROWS}
    for named in (
        "r07-cache-delete-repo-no-force",
        "r12-create-ecr-upstream-no-role",
        "r21-upd-existing-role-len-2048",
        "r01-create-quay-with-cred",
        "r37-create-bad-prefix",
        "r37-create-dh-nosuch-secret",
        "r08-put-om-version+action",
        "r08-put-len-16384",
        "r08-put-len-20480",
        "r08-put-len-sid-10241",
    ):
        assert named in keys, named
    assert any(k.startswith("r40-") and k.endswith("-c-ecr-no-role") for k in keys)
