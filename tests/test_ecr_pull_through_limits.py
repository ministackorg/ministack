"""ECR pull-through cache rules: quota order, key state, role length, trailing-slash prefixes (issue 2617).

Bodies and messages are taken from the AWS captures named in each case.
"""

import json
import os

import boto3
import pytest
import requests

pytestmark = pytest.mark.serial  # shares the registry-wide rule quota, registry policy and service-linked role

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
_ACCT = "000000000000"
_SM = "arn:aws:secretsmanager"
_UNDECRYPTABLE = (
    "The secret is accessible but is unable to be decrypted. Verify the resource permisisons and try again."
)
_UNAUTH_RULE = (
    "Invalid input: Please create an authenticated pull through cache rule to update the credentials"
)
_LIMIT = f"Registry id '{_ACCT}' has the maximum number of pull-through cache rules allowed, which is 50."
_ROLE_LEN = "Invalid parameter at 'customRoleArn' failed to satisfy constraint: 'Member must have length less than or equal to 2048'"
_PATTERN = (
    "^([a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*(\\/[a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*)*\\/?|ROOT)$"
)


def _call(action, body):
    return requests.post(
        ENDPOINT + "/",
        data=json.dumps(body),
        headers={
            "X-Amz-Target": f"AmazonEC2ContainerRegistry_V20150921.{action}",
            "Content-Type": "application/x-amz-json-1.1",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/ecr/aws4_request, SignedHeaders=host, Signature=x",
        },
    )


def _expect(r, status, code, message):
    assert r.status_code == status, r.text
    assert r.json()["__type"] == code, r.text
    assert r.headers["x-amzn-errortype"] == code, r.text
    assert r.json()["message"] == message, r.text


def _clean_rules():
    for r in _call("DescribePullThroughCacheRules", {"maxResults": 1000}).json().get("pullThroughCacheRules", []):
        _call("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": r["ecrRepositoryPrefix"]})


@pytest.fixture(autouse=True)
def _clean():
    _clean_rules()
    yield
    _clean_rules()


def _client(service):
    return boto3.client(service, endpoint_url=ENDPOINT, region_name="us-east-1",
                        aws_access_key_id="test", aws_secret_access_key="test")


def _secret(name, **kw):
    sm = _client("secretsmanager")
    try:
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
    except Exception:
        pass
    return sm.create_secret(Name=name, **kw)["ARN"]


def _dh(prefix, arn, **extra):
    return _call("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "registry-1.docker.io", "credentialArn": arn, **extra})


# ── r27: the quota slot ───────────────────────────────────

def test_quota_slot_is_after_every_other_create_check_and_before_the_duplicate():
    # r27-quota-create-dh, r27-quota-create-01, r27-full+url-bad, r27-full+dup+dh-no-cred, r27-full+dup, r27-full+dup-dh
    arn = _secret("ecr-pullthroughcache/msc-2617-r27-ok", SecretString="x")
    assert _dh("msc2617-r27-dh", arn).status_code == 200
    for i in range(1, 50):
        r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": f"msc2617-r27-q{i:02d}", "upstreamRegistryUrl": "public.ecr.aws"})
        assert r.status_code == 200, (i, r.text)
    # Other create errors answer first.
    _expect(
        _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r27-new", "upstreamRegistryUrl": "registry.example.com"}),
        400, "UnsupportedUpstreamRegistryException",
        "The upstream registry URL registry.example.com is invalid. Specify a valid upstream registry URL and try again",
    )
    _expect(
        _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r27-q01", "upstreamRegistryUrl": "registry-1.docker.io"}),
        400, "UnsupportedUpstreamRegistryException",
        "The specified upstream registry requires authentication. Specify a valid Secrets Manager ARN containing "
        "the upstream registry credentials and try again.",
    )
    # A valid create and a duplicate prefix both hit the quota.
    for prefix, upstream in (("msc2617-r27-new", "public.ecr.aws"), ("msc2617-r27-q01", "public.ecr.aws"), ("msc2617-r27-q01/", "public.ecr.aws")):
        _expect(_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": upstream}),
                400, "LimitExceededException", _LIMIT)
    _expect(_dh("msc2617-r27-dh", arn), 400, "LimitExceededException", _LIMIT)
    # Refused creates store nothing.
    assert len(_call("DescribePullThroughCacheRules", {"maxResults": 1000}).json()["pullThroughCacheRules"]) == 50
    # r27 other actions are not limited.
    _expect(
        _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r27-q-new", "credentialArn": arn}),
        400, "PullThroughCacheRuleNotFoundException",
        f"A pull-through cache rule with repository prefix 'msc2617-r27-q-new' does not exist in the registry with id '{_ACCT}'",
    )
    _expect(_call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r27-q01", "credentialArn": arn}),
            400, "InvalidParameterException", _UNAUTH_RULE)
    assert _call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r27-q01"}).status_code == 200
    # A delete frees a slot at once; the next create fills it again.
    assert _call("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r27-q01"}).status_code == 200
    assert _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r27-q01", "upstreamRegistryUrl": "public.ecr.aws"}).status_code == 200
    _expect(_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r27-new", "upstreamRegistryUrl": "public.ecr.aws"}),
            400, "LimitExceededException", _LIMIT)


# ── r28: customer managed key state ───────────────────────

def test_secret_under_disabled_or_pending_key_is_unable_to_decrypt():
    # r28-kms-create-key, r28-secret-kms, r28-create-kms-key-enabled, r28-create-kms-key-disabled,
    # r28-create-om-bad-prefix+kms-disabled, r28-update-to-kms-key-disabled, r28-update-om-both+kms-disabled,
    # r28-validate-kms-key-disabled, r28-create-kms-key-pending, r28-update-to-kms-key-reenabled
    kms = _client("kms")
    key_id = kms.create_key()["KeyMetadata"]["KeyId"]
    arn = _secret("ecr-pullthroughcache/msc-2617-r28-kms", SecretString="x", KmsKeyId=key_id)
    plain = _secret("ecr-pullthroughcache/msc-2617-r28-ok", SecretString="x")
    assert _dh("msc2617-r28-dh-ok", plain).status_code == 200
    assert _dh("msc2617-r28-dh-kms", arn).status_code == 200
    kms.disable_key(KeyId=key_id)
    for state in ("disabled", "pending"):
        if state == "pending":
            kms.enable_key(KeyId=key_id)
            kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
        _expect(_dh("msc2617-r28-dh-kms2", arn), 400, "UnableToDecryptSecretValueException", _UNDECRYPTABLE)
        _expect(_call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r28-dh-ok", "credentialArn": arn}),
                400, "UnableToDecryptSecretValueException", _UNDECRYPTABLE)
        # Before the both-fields check, the rule lookup and rule-type compatibility.
        _expect(_call("UpdatePullThroughCacheRule", {
            "ecrRepositoryPrefix": "msc2617-r28-dh-ok", "credentialArn": arn,
            "customRoleArn": f"arn:aws:iam::{_ACCT}:role/nosuch"}), 400, "UnableToDecryptSecretValueException", _UNDECRYPTABLE)
        _expect(_call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r28-missing", "credentialArn": arn}),
                400, "UnableToDecryptSecretValueException", _UNDECRYPTABLE)
        v = _call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r28-dh-kms"}).json()
        assert v["isValid"] is False
        assert json.loads(v["failure"]) == {
            "failureCode": "UNABLE_TO_ACCESS_SECRET",
            "failureReason": "The secret is unable to be accessed. Verify the resource permissions for the secret and try again."}
    # The shape error comes first, and the rule is unchanged.
    r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "BAD", "upstreamRegistryUrl": "registry-1.docker.io", "credentialArn": arn})
    _expect(r, 400, "InvalidParameterException",
            "Invalid parameter at 'ecrRepositoryPrefix' failed to satisfy constraint: 'Member must satisfy regular "
            "expression pattern: ^([a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*(\\/[a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*)*\\/?|ROOT)$'")
    assert _call("DescribePullThroughCacheRules", {"ecrRepositoryPrefixes": ["msc2617-r28-dh-ok"]}).json()[
        "pullThroughCacheRules"][0]["credentialArn"] == plain
    kms.cancel_key_deletion(KeyId=key_id)
    kms.enable_key(KeyId=key_id)
    r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r28-dh-ok", "credentialArn": arn})
    assert r.status_code == 200, r.text


# ── r29: Update of an ECR rule with a credentialArn ───────

def test_update_ecr_rule_with_credential_is_refused():
    # r29-secret-s1, r29-ecr-rule, r29-upd-ecr-cred-readable, r29-upd-ecr-cred-readable+role, r29-upd-dh-role-only
    arn = _secret("ecr-pullthroughcache/msc-2617-r29-s1", SecretString="x")
    role = f"arn:aws:iam::{_ACCT}:role/msc2617-r29-nosuch-role"
    r = _call("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": "msc2617-r29-ecr", "upstreamRegistryUrl": "111122223333.dkr.ecr.us-east-1.amazonaws.com",
        "customRoleArn": role})
    assert r.status_code == 200, r.text
    _expect(_call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r29-ecr", "credentialArn": arn}),
            400, "InvalidParameterException",
            "Invalid input: The specified upstream registry requires authentication. Specify a valid IAM role ARN and try again.")
    _expect(_call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r29-ecr", "credentialArn": arn, "customRoleArn": role}),
            400, "InvalidParameterException", "Invalid input: Please set either credential arn or role arn to update the credentials")
    rule = _call("DescribePullThroughCacheRules", {"ecrRepositoryPrefixes": ["msc2617-r29-ecr"]}).json()["pullThroughCacheRules"][0]
    assert rule["customRoleArn"] == role and "credentialArn" not in rule


# ── r31: customRoleArn length first in the joined message ─

def test_role_length_is_first_in_the_joined_message():
    # r31-secret-s1, r31-dh-rule, r31-c-2049+alone-role-first, r31-c-2049+prefix-bad-role-first
    role = f"arn:aws:iam::{_ACCT}:role/" + "r" * (2049 - len(f"arn:aws:iam::{_ACCT}:role/"))
    assert len(role) == 2049
    bad_prefix = f"Invalid parameter at 'ecrRepositoryPrefix' failed to satisfy constraint: 'Member must satisfy regular expression pattern: {_PATTERN}'"
    r = _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "msc2617-r31-a", "customRoleArn": role})
    _expect(r, 400, "InvalidParameterException", _ROLE_LEN)
    r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "BAD", "upstreamRegistryUrl": "public.ecr.aws", "customRoleArn": role})
    _expect(r, 400, "InvalidParameterException", f"{_ROLE_LEN}; {bad_prefix}")
    arn = _secret("ecr-pullthroughcache/msc-2617-r31-s1", SecretString="x")
    assert _dh("msc2617-r31-dh", arn).status_code == 200
    r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "BAD", "customRoleArn": role})
    _expect(r, 400, "InvalidParameterException", f"{_ROLE_LEN}; {bad_prefix}")
    # Both fields and a missing rule report only the length entry.
    r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r31-dh", "credentialArn": arn, "customRoleArn": role})
    _expect(r, 400, "InvalidParameterException", _ROLE_LEN)
    r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r31-none", "customRoleArn": role})
    _expect(r, 400, "InvalidParameterException", _ROLE_LEN)


# ── r33: trailing '/' on upstreamRepositoryPrefix ─────────

@pytest.mark.parametrize("upstream_prefix", ["docker/library/", "docker/"])
def test_trailing_slash_upstream_prefix_is_kept_verbatim(upstream_prefix):
    # r33-create-trailing-slash, r33-create-one-level-trailing
    r = _call("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": "msc2617-r33-t2/", "upstreamRegistryUrl": "public.ecr.aws", "upstreamRepositoryPrefix": upstream_prefix})
    assert r.status_code == 200, r.text
    assert r.json()["ecrRepositoryPrefix"] == "msc2617-r33-t2"
    assert r.json()["upstreamRepositoryPrefix"] == upstream_prefix
    rule = _call("DescribePullThroughCacheRules", {"ecrRepositoryPrefixes": ["msc2617-r33-t2"]}).json()["pullThroughCacheRules"][0]
    assert rule["upstreamRepositoryPrefix"] == upstream_prefix
    v = _call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r33-t2"}).json()
    assert v["isValid"] is True and v["failure"] == ""
    # Nothing is created for a pull under it.
    pull = _call("BatchGetImage", {"repositoryName": "msc2617-r33-t2/alpine", "imageIds": [{"imageTag": "3.20"}]})
    _expect(pull, 400, "RepositoryNotFoundException",
            f"The repository with name 'msc2617-r33-t2/alpine' does not exist in the registry with id '{_ACCT}'")


@pytest.mark.parametrize("bad,tail", [
    ("/docker/library", ""), ("docker//library", ""), ("docker/library//", ""),
    ("/", "; Invalid parameter at 'upstreamRepositoryPrefix' failed to satisfy constraint: "
          "'Member must have length greater than or equal to 2'"),
])
def test_upstream_prefix_pattern_errors(bad, tail):
    # r33-create-leading-slash, r33-create-double-slash, r33-create-two-trailing, r33-create-slash-only
    r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r33-t3", "upstreamRegistryUrl": "public.ecr.aws", "upstreamRepositoryPrefix": bad})
    _expect(r, 400, "InvalidParameterException",
            f"Invalid parameter at 'upstreamRepositoryPrefix' failed to satisfy constraint: 'Member must satisfy regular expression pattern: {_PATTERN}'" + tail)


# ── r34: a created or deleted rule applies at once ────────

def test_created_rule_is_described_and_valid_at_once_and_delete_applies_at_once():
    # r34-c-bgi-t0-rule, r34-c-*-describe, r34-c-*-validate, r34-d-bgi-t0-rule, r34-d-*-validate
    r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r34-a", "upstreamRegistryUrl": "public.ecr.aws"})
    assert r.status_code == 200, r.text
    assert _call("DescribePullThroughCacheRules", {"ecrRepositoryPrefixes": ["msc2617-r34-a"]}).status_code == 200
    assert _call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r34-a"}).json()["isValid"] is True
    assert _call("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r34-a"}).status_code == 200
    _expect(_call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r34-a"}),
            400, "PullThroughCacheRuleNotFoundException",
            f"A pull-through cache rule with repository prefix 'msc2617-r34-a' does not exist in the registry with id '{_ACCT}'")
    _expect(_call("BatchGetImage", {"repositoryName": "msc2617-r34-a/docker/library/alpine", "imageIds": [{"imageTag": "3.20"}]}),
            400, "RepositoryNotFoundException",
            f"The repository with name 'msc2617-r34-a/docker/library/alpine' does not exist in the registry with id '{_ACCT}'")
