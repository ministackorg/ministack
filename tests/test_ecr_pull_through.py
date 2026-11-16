"""ECR pull-through cache rules: Create validation and response shape (issue 2617).

Bodies and messages are taken from the AWS captures named in each case id.
"""

import json
import os

import pytest
import requests

pytestmark = pytest.mark.serial  # shares the registry-wide rule quota, registry policy and service-linked role

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

_PREFIX_PATTERN = (
    "^([a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*(\\/[a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*)*\\/?|ROOT)$"
)
_ENUM_MSG = (
    "Invalid parameter at 'upstreamRegistry' failed to satisfy constraint: 'Member must satisfy enum value set: "
    "[ecr-public, chainguard, k8s, azure-container-registry, docker-hub, ecr-public-premium, "
    "github-container-registry, ecr, "
    "gitlab-container-registry, quay]'"
)


def _prefix_msg(name, tail=None):
    pattern = f"Invalid parameter at '{name}' failed to satisfy constraint: 'Member must satisfy regular expression pattern: {_PREFIX_PATTERN}'"
    return pattern


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


@pytest.fixture(autouse=True)
def _clean():
    for r in _call("DescribePullThroughCacheRules", {}).json().get("pullThroughCacheRules", []):
        _call("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": r["ecrRepositoryPrefix"]})
    yield
    for r in _call("DescribePullThroughCacheRules", {}).json().get("pullThroughCacheRules", []):
        _call("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": r["ecrRepositoryPrefix"]})


@pytest.mark.parametrize(
    "key,url,prefix",
    [
        ("r01-create-ecr-public-inferred", "public.ecr.aws", "msc2617-r01-pub"),
        ("r01-create-quay", "quay.io", "msc2617-r01-quay"),
    ],
)
def test_create_unauthenticated_response_shape(key, url, prefix):
    r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": url})
    assert r.status_code == 200, key
    body = r.json()
    assert set(body) == {"ecrRepositoryPrefix", "upstreamRegistryUrl", "createdAt", "registryId"}
    assert body["ecrRepositoryPrefix"] == prefix
    assert body["upstreamRegistryUrl"] == url


def test_create_explicit_upstream_registry_not_echoed_for_unauthenticated():
    # r01-create-ecr-public-explicit
    r = _call(
        "CreatePullThroughCacheRule",
        {"ecrRepositoryPrefix": "msc2617-r01-pub2", "upstreamRegistryUrl": "public.ecr.aws", "upstreamRegistry": "ecr-public"},
    )
    assert r.status_code == 200
    assert "upstreamRegistry" not in r.json()


def test_create_trailing_slash_stripped():
    # r01-create-trailing-slash
    r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r01-slash/", "upstreamRegistryUrl": "public.ecr.aws"})
    assert r.status_code == 200
    assert r.json()["ecrRepositoryPrefix"] == "msc2617-r01-slash"
    twin = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r01-slash", "upstreamRegistryUrl": "public.ecr.aws"})
    assert twin.status_code == 400
    assert twin.json()["__type"] == "PullThroughCacheRuleAlreadyExistsException"
    # r01-create-trailing-slash-twin
    assert twin.json()["message"] == (
        "A pull-through cache rule with repository prefix 'msc2617-r01-slash' already exists "
        f"in the registry with id '{r.json()['registryId']}'"
    )


@pytest.mark.parametrize(
    "key,url",
    [
        ("r02-url-unsupported", "registry.example.com"),
        ("r02-url-https", "https://public.ecr.aws"),
        ("r02-url-trailing-slash", "public.ecr.aws/"),
    ],
)
def test_create_unsupported_url(key, url):
    r = _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": url, "ecrRepositoryPrefix": "msc2617-r02-url"})
    assert r.status_code == 400, key
    body = r.json()
    assert body["__type"] == "UnsupportedUpstreamRegistryException"
    assert body["message"] == f"The upstream registry URL {url} is invalid. Specify a valid upstream registry URL and try again"


@pytest.mark.parametrize("url", ["", "a" * 2048])
def test_create_blank_or_2048_url_is_internal_failure(url):
    # r02-blank-url, r02-url-long-2048
    r = _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": url, "ecrRepositoryPrefix": "msc2617-r02-blankurl"})
    assert r.status_code == 500
    assert r.json() == {"__type": "InternalFailure"}
    assert _call("DescribePullThroughCacheRules", {}).json()["pullThroughCacheRules"] == []


def test_create_missing_prefix():
    # r02-missing-prefix
    r = _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": "public.ecr.aws"})
    assert r.status_code == 400
    assert r.json()["__type"] == "InvalidParameterException"
    assert r.json()["message"] == "Invalid parameter at 'ecrRepositoryPrefix' failed to satisfy constraint: 'Member must not be null'"


def test_create_blank_prefix():
    # r02-blank-prefix
    r = _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": ""})
    assert r.status_code == 400
    assert r.json()["__type"] == "InvalidParameterException"
    assert r.json()["message"] == (
        _prefix_msg("ecrRepositoryPrefix")
        + "; Invalid parameter at 'ecrRepositoryPrefix' failed to satisfy constraint: 'Member must have length greater than or equal to 2'"
    )


def test_create_space_prefix():
    # r02-space-prefix
    r = _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "  "})
    assert r.status_code == 400
    assert r.json()["__type"] == "InvalidParameterException"
    assert r.json()["message"] == _prefix_msg("ecrRepositoryPrefix")


def test_create_upstream_prefix_accepted_for_ecr_public():
    # r02-upstream-prefix-ecr-public
    r = _call(
        "CreatePullThroughCacheRule",
        {"upstreamRepositoryPrefix": "docker", "upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "msc2617-r02-upp"},
    )
    assert r.status_code == 200


def test_create_upstream_prefix_upper():
    # r02-upstream-prefix-upper
    r = _call(
        "CreatePullThroughCacheRule",
        {"upstreamRepositoryPrefix": "DOCKER", "upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "msc2617-r02-upp2"},
    )
    assert r.status_code == 400
    assert r.json()["__type"] == "InvalidParameterException"
    assert r.json()["message"] == _prefix_msg("upstreamRepositoryPrefix")


def test_create_upstream_prefix_len_1():
    # r02-upstream-prefix-len-1
    r = _call(
        "CreatePullThroughCacheRule",
        {"upstreamRepositoryPrefix": "d", "upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "msc2617-r02-upp3"},
    )
    assert r.status_code == 400
    assert r.json()["__type"] == "InvalidParameterException"
    assert r.json()["message"] == (
        "Invalid parameter at 'upstreamRepositoryPrefix' failed to satisfy constraint: "
        "'Member must have length greater than or equal to 2'"
    )


@pytest.mark.parametrize("key,value", [("r02-enum-unknown", "foo"), ("r02-enum-case", "ECR-PUBLIC"), ("r02-enum-blank", "")])
def test_create_enum_rejected(key, value):
    r = _call(
        "CreatePullThroughCacheRule",
        {"upstreamRegistry": value, "upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "msc2617-r02-enum1"},
    )
    assert r.status_code == 400, key
    assert r.json()["__type"] == "InvalidParameterException"
    assert r.json()["message"] == _ENUM_MSG


def test_create_regid_before_prefix():
    # r02-om-regid-bad+prefix-bad
    r = _call(
        "CreatePullThroughCacheRule",
        {"registryId": "abc", "upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "BAD"},
    )
    assert r.status_code == 400
    assert r.json()["__type"] == "InvalidParameterException"
    assert r.json()["message"] == "Invalid parameter at 'registryId' failed to satisfy constraint: 'must satisfy regular expression [0-9]{12}'"


def test_create_enum_and_upstream_prefix_joined_enum_first():
    # r23-upp-bad+enum-bad-upp-first
    r = _call(
        "CreatePullThroughCacheRule",
        {"upstreamRepositoryPrefix": "BAD", "upstreamRegistry": "foo", "upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "msc2617-r23-a1"},
    )
    assert r.status_code == 400
    assert r.json()["__type"] == "InvalidParameterException"
    assert r.json()["message"] == _ENUM_MSG + "; " + _prefix_msg("upstreamRepositoryPrefix")


@pytest.mark.parametrize(
    "key,url,prefix,upstream",
    [
        ("r01-create-docker-hub", "registry-1.docker.io", "msc2617-r01-dh", "docker-hub"),
        ("r01-create-ghcr", "ghcr.io", "msc2617-r01-ghcr", "github-container-registry"),
        ("r01-create-gitlab", "registry.gitlab.com", "msc2617-r01-gitlab", "gitlab-container-registry"),
    ],
)
def test_create_upstream_registry_derived_from_url(key, url, prefix, upstream):
    import boto3

    sm = boto3.client(
        "secretsmanager", endpoint_url=ENDPOINT, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )
    name = f"ecr-pullthroughcache/{prefix}"
    try:
        arn = sm.create_secret(Name=name, SecretString=json.dumps({"username": "u", "accessToken": "t"}))["ARN"]
    except sm.exceptions.ResourceExistsException:
        arn = sm.describe_secret(SecretId=name)["ARN"]
    r = _call(
        "CreatePullThroughCacheRule",
        {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": url, "credentialArn": arn, "upstreamRegistry": "quay"},
    )
    assert r.status_code == 200, (key, r.text)
    body = r.json()
    assert body["upstreamRegistry"] == upstream
    assert body["credentialArn"] == arn


# ---- part 2: credentialArn, customRoleArn, registryId, joined errors, secrets, upstream auth, duplicates ----

_ACCT = "000000000000"
_CRED_PATTERN = "^arn:aws(-\\w+)*:secretsmanager:[a-zA-Z0-9-:]+:secret:ecr\\-pullthroughcache\\/[a-zA-Z0-9\\/_+=.@-]+$"
_CRED_MSG = (
    "Invalid parameter at 'credentialArn' failed to satisfy constraint: "
    f"'Member must satisfy regular expression pattern: {_CRED_PATTERN}'"
)
_REGID_MSG = "Invalid parameter at 'registryId' failed to satisfy constraint: 'must satisfy regular expression [0-9]{12}'"
_NULL_URL = "Invalid parameter at 'upstreamRegistryUrl' failed to satisfy constraint: 'Member must not be null'"
_NULL_PREFIX = "Invalid parameter at 'ecrRepositoryPrefix' failed to satisfy constraint: 'Member must not be null'"
_NEEDS_SECRET = (
    "The specified upstream registry requires authentication. Specify a valid Secrets Manager ARN containing "
    "the upstream registry credentials and try again."
)
_SAME_REGISTRY = "Upstream registry URL cannot be the same as the cache registry."
_SECRET_REGION = (
    "Please verify the secret exists in the same region and account that the pullthrough cache rule will be "
    "created in, and try again."
)
_SECRET_NOT_FOUND = (
    "The ARN of the secret specified in the pull through cache rule was not found. Update the pull through "
    "cache rule with a valid secret ARN and try again."
)
_SECRET_UNREADABLE = "The secret is unable to be accessed. Verify the resource permissions for the secret and try again."
_SM = "arn:aws:secretsmanager"


def _sm():
    import boto3

    return boto3.client(
        "secretsmanager", endpoint_url=ENDPOINT, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )


def _secret(name, **kw):
    sm = _sm()
    try:
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
    except Exception:
        pass
    return sm.create_secret(Name=name, **kw)["ARN"]


def _expect(r, status, code, message):
    assert r.status_code == status, r.text
    assert r.json()["__type"] == code, r.text
    # The captures spell the key "Message" for AccessDeniedException and SerializationException, "message" otherwise.
    key = "Message" if code in ("AccessDeniedException", "SerializationException") else "message"
    assert r.json()[key] == message, r.text


def _dh(extra, prefix="msc2617-r02-dh"):
    return _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": "registry-1.docker.io", "ecrRepositoryPrefix": prefix, **extra})


@pytest.mark.parametrize(
    "key,cred",
    [
        ("r02-cred-no-prefix", f"{_SM}:us-east-1:{_ACCT}:secret:msc-2617-r02-create-validation-plain-AbCdEf"),
        ("r02-cred-wrong-service", f"arn:aws:ssm:us-east-1:{_ACCT}:parameter/ecr-pullthroughcache/msc-2617-x"),
        ("r02-cred-name-only", "ecr-pullthroughcache/msc-2617-r02-create-validation-name-onlyxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
    ],
)
def test_create_credential_arn_pattern(key, cred):
    _expect(_dh({"credentialArn": cred}), 400, "InvalidParameterException", _CRED_MSG)


def test_create_credential_arn_cross_region_account_partition():
    base = "secret:ecr-pullthroughcache/msc-2617-r02-create-validation-x-AbCdEf"
    # r02-cred-other-region, r02-cred-other-account
    for key, arn in (
        ("r02-cred-other-region", f"{_SM}:us-west-1:{_ACCT}:{base}"),
        ("r02-cred-other-account", f"{_SM}:us-east-1:123456789012:{base}"),
    ):
        _expect(_dh({"credentialArn": arn}), 400, "InvalidParameterException", _SECRET_REGION)
    # r02-cred-other-partition
    _expect(_dh({"credentialArn": f"arn:aws-cn:secretsmanager:us-east-1:{_ACCT}:{base}"}), 400,
            "SecretNotFoundException", _SECRET_NOT_FOUND)


def test_create_credential_arn_len_612_unable_to_access():
    # r02-cred-len-612
    head = f"{_SM}:us-east-1:{_ACCT}:secret:ecr-pullthroughcache/"
    arn = head + "c" * (612 - len(head))
    _expect(_dh({"credentialArn": arn}), 400, "UnableToAccessSecretException", _SECRET_UNREADABLE)
    too_long = _dh({"credentialArn": arn + "c"})
    # r02-cred-len-613
    _expect(too_long, 400, "InvalidParameterException",
            "Invalid parameter at 'credentialArn' failed to satisfy constraint: 'Member must have length less than or equal to 612'")


@pytest.mark.parametrize("name_len,code,message", [(519, "SecretNotFoundException", _SECRET_NOT_FOUND),
                                                    (520, "UnableToAccessSecretException", _SECRET_UNREADABLE)])
def test_create_credential_secret_name_cutoff_is_the_longest_real_arn_resource(name_len, code, message):
    """MiniStack contract, uncaptured: ledger 227 (a real ARN resource is at most 519 characters, so a missing
    secret up to that length is not found and a longer one is unreadable; r02-cred-len-612 is the captured 612)."""
    resource = "secret:ecr-pullthroughcache/"
    arn = f"{_SM}:us-east-1:{_ACCT}:" + resource + "c" * (name_len - len(resource) + len("secret:"))
    _expect(_dh({"credentialArn": arn}), 400, code, message)


def test_create_role_not_arn_blank_and_len_2048():
    ecr_url = f"{_ACCT}.dkr.ecr.us-east-1.amazonaws.com"
    body = {"upstreamRegistryUrl": ecr_url, "ecrRepositoryPrefix": "msc2617-r02-role"}
    # r02-role-not-arn
    _expect(_call("CreatePullThroughCacheRule", {**body, "customRoleArn": "not-an-arn"}), 400,
            "InvalidParameterException", "Invalid role ARN supplied in request.")
    # r02-role-blank: '' is treated as absent
    _expect(_call("CreatePullThroughCacheRule", {**body, "customRoleArn": ""}), 400,
            "UnsupportedUpstreamRegistryException", _SAME_REGISTRY)
    # r02-role-len-2048
    role = f"arn:aws:iam::{_ACCT}:role/" + "r" * (2048 - len(f"arn:aws:iam::{_ACCT}:role/"))
    _expect(_call("CreatePullThroughCacheRule", {**body, "customRoleArn": role}), 400,
            "UnsupportedUpstreamRegistryException", _SAME_REGISTRY)


def test_create_ecr_upstream_own_registry_refused():
    # r02-ecr-no-role, r02-ecr-no-role-explicit, r02-ecr-nosuch-role
    url = f"{_ACCT}.dkr.ecr.us-east-1.amazonaws.com"
    for extra in (
        {},
        {"upstreamRegistry": "ecr"},
        {"customRoleArn": f"arn:aws:iam::{_ACCT}:role/msc-2617-r02-nosuch-role"},
    ):
        r = _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": url, "ecrRepositoryPrefix": "msc2617-r02-ecr", **extra})
        _expect(r, 400, "UnsupportedUpstreamRegistryException", _SAME_REGISTRY)


@pytest.mark.parametrize("key,regid", [("r02-regid-11", "12345678901"), ("r02-regid-13", "1234567890123"), ("r02-regid-letters", "abcdefghijkl")])
def test_create_registry_id_pattern(key, regid):
    r = _call("CreatePullThroughCacheRule", {"registryId": regid, "upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "msc2617-r02-rg"})
    _expect(r, 400, "InvalidParameterException", _REGID_MSG)


def test_create_registry_id_own_account_accepted():
    r = _call("CreatePullThroughCacheRule", {"registryId": _ACCT, "upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "msc2617-r02-rgok"})
    assert r.status_code == 200, r.text
    assert r.json()["registryId"] == _ACCT


def test_create_shape_errors_joined():
    # r02-missing-both
    r = _call("CreatePullThroughCacheRule", {})
    _expect(r, 400, "InvalidParameterException", f"{_NULL_URL}; {_NULL_PREFIX}")
    # r02-om-pattern+length
    r = _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "A" * 31})
    _expect(r, 400, "InvalidParameterException",
            _prefix_msg("ecrRepositoryPrefix") + "; Invalid parameter at 'ecrRepositoryPrefix' failed to satisfy "
            "constraint: 'Member must have length less than or equal to 30'")
    # r02-om-prefix-bad+url-missing
    r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "BAD"})
    _expect(r, 400, "InvalidParameterException", f"{_NULL_URL}; {_prefix_msg('ecrRepositoryPrefix')}")


@pytest.mark.parametrize(
    "key,url,prefix",
    [
        ("r01-create-chainguard-nocred", "cgr.dev", "msc2617-r01-cgr"),
        ("r02-cred-missing-docker-hub", "registry-1.docker.io", "msc2617-r02-nc-docker"),
        ("r02-cred-missing-github-container-registry", "ghcr.io", "msc2617-r02-nc-ghcr"),
        ("gitlab", "registry.gitlab.com", "msc2617-r02-nc-gitlab"),
    ],
)
def test_create_authenticated_upstream_needs_credential(key, url, prefix):
    r = _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": url, "ecrRepositoryPrefix": prefix})
    _expect(r, 400, "UnsupportedUpstreamRegistryException", _NEEDS_SECRET)


def test_create_duplicate_prefix():
    # r01-create-dup-identical, r01-create-dup-other-upstream, r01-create-trailing-slash-twin
    ok = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r01-pub", "upstreamRegistryUrl": "public.ecr.aws"})
    assert ok.status_code == 200
    msg = f"A pull-through cache rule with repository prefix 'msc2617-r01-pub' already exists in the registry with id '{_ACCT}'"
    for url, prefix in (("public.ecr.aws", "msc2617-r01-pub"), ("quay.io", "msc2617-r01-pub"), ("quay.io", "msc2617-r01-pub/")):
        r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": url})
        _expect(r, 400, "PullThroughCacheRuleAlreadyExistsException", msg)


def test_create_secret_lookup_same_account_and_region():
    # r03-create-cred-ok, r03-create-cred-partial-arn, r03-create-cred-wrong-suffix
    arn = _secret("ecr-pullthroughcache/msc-2617-r03-secrets-ok", SecretString="x")
    r = _dh({"credentialArn": arn}, "msc2617-r03-s-ok")
    assert r.status_code == 200, r.text
    assert r.json()["credentialArn"] == arn
    partial = arn.rsplit("-", 1)[0]
    r = _dh({"credentialArn": partial}, "msc2617-r03-s-partial")
    assert r.status_code == 200, r.text
    assert r.json()["credentialArn"] == partial
    r = _dh({"credentialArn": partial + "-ZZZZZZ"}, "msc2617-r03-s-suffix")
    _expect(r, 400, "SecretNotFoundException", _SECRET_NOT_FOUND)


def test_create_secret_unreadable():
    # r03-create-cred-binary, r03-create-cred-scheduled
    arn = _secret("ecr-pullthroughcache/msc-2617-r03-secrets-binary", SecretBinary=b"\x00\x01")
    _expect(_dh({"credentialArn": arn}, "msc2617-r03-s-binary"), 400, "UnableToAccessSecretException", _SECRET_UNREADABLE)
    arn = _secret("ecr-pullthroughcache/msc-2617-r03-secrets-sched", SecretString="x")
    _sm().delete_secret(SecretId=arn, RecoveryWindowInDays=7)
    _expect(_dh({"credentialArn": arn}, "msc2617-r03-s-sched"), 400, "UnableToAccessSecretException", _SECRET_UNREADABLE)


@pytest.mark.parametrize("key,value", [("r03-create-cred-plain", "not json"), ("r03-create-cred-nokeys", '{"a": "b"}'), ("r03-create-cred-emptyjson", "{}")])
def test_create_secret_content_not_parsed(key, value):
    arn = _secret(f"ecr-pullthroughcache/msc-2617-{key}", SecretString=value)
    r = _dh({"credentialArn": arn}, "msc2617-r03-content")
    assert r.status_code == 200, r.text
    assert r.json()["credentialArn"] == arn


def test_create_rule_over_existing_repositories():
    # r07-pre-create-repo, r07-pre-rule-over-repo, r07-pre-create-repo-same-as-prefix

    def ecr_call(action, body):
        return requests.post(
            ENDPOINT + "/",
            data=json.dumps(body),
            headers={
                "X-Amz-Target": f"AmazonEC2ContainerRegistry_V20150921.{action}",
                "Content-Type": "application/x-amz-json-1.1",
                "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/ecr/aws4_request, SignedHeaders=host, Signature=x",
            },
        )

    prefix = "msc-2617-r07-first-pull-pre"
    for name in (f"{prefix}/docker/library/busybox", prefix):
        ecr_call("DeleteRepository", {"repositoryName": name, "force": True})
    try:
        assert ecr_call("CreateRepository", {"repositoryName": f"{prefix}/docker/library/busybox"}).status_code == 200
        r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "public.ecr.aws"})
        assert r.status_code == 200, r.text
        assert set(r.json()) == {"createdAt", "ecrRepositoryPrefix", "registryId", "upstreamRegistryUrl"}
        assert ecr_call("CreateRepository", {"repositoryName": prefix}).status_code == 200
    finally:
        for name in (f"{prefix}/docker/library/busybox", prefix):
            ecr_call("DeleteRepository", {"repositoryName": name, "force": True})


# ── Quota, create check order, Update (ledger part 3) ──────────────────────

_ONE_CRED = "Invalid input: Please set either credential arn or role arn to update the credentials"
_UNAUTH_RULE = "Invalid input: Please create an authenticated pull through cache rule to update the credentials"
_SECRET_REGION_UPD = (
    "Please verify the secret exists in the same region and account that the pullthrough cache rule will be created in, and try again."
)
_UPD_NOT_FOUND = "A pull-through cache rule with repository prefix '{}' does not exist in the registry with id '" + _ACCT + "'"


def _describe_prefix(prefix):
    return _call("DescribePullThroughCacheRules", {"ecrRepositoryPrefixes": [prefix]}).json()["pullThroughCacheRules"][0]


def test_create_quota_50_rules():
    # r10-limit-create-46, r10-limit-create-47 (AWS's account held other rules, MiniStack's limit is the 50 in the message)
    for i in range(50):
        r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": f"msc-2617-r10-lim{i:02d}", "upstreamRegistryUrl": "public.ecr.aws"})
        assert r.status_code == 200, (i, r.text)
    r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc-2617-r10-lim50", "upstreamRegistryUrl": "public.ecr.aws"})
    _expect(r, 400, "LimitExceededException", f"Registry id '{_ACCT}' has the maximum number of pull-through cache rules allowed, which is 50.")
    # r27: update and delete are not limited.
    assert _call("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": "msc-2617-r10-lim00"}).status_code == 200
    r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc-2617-r10-lim50", "upstreamRegistryUrl": "public.ecr.aws"})
    assert r.status_code == 200, r.text


def test_create_check_order_and_refusals_leave_no_rule():
    # r02-om-regid-bad+prefix-bad, r02-om-regid-other+secret-notfound, r02-om-dup+regid-other, r02-missing-prefix--after
    _expect(
        _call("CreatePullThroughCacheRule", {"registryId": "abc", "upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "BAD"}),
        400, "InvalidParameterException", _REGID_MSG,
    )
    denied = f"User: arn:aws:iam::{_ACCT}:root is not authorized to perform: ecr:CreatePullThroughCacheRule on resource: * because no resource-based policy allows the ecr:CreatePullThroughCacheRule action"
    for body in (
        {"registryId": "111122223333", "upstreamRegistryUrl": "public.ecr.aws", "ecrRepositoryPrefix": "msc2617-r02-om"},
        {
            "registryId": "111122223333",
            "upstreamRegistryUrl": "registry-1.docker.io",
            "credentialArn": f"{_SM}:us-east-1:{_ACCT}:secret:ecr-pullthroughcache/msc-2617-r02-create-validation-nosuch-AbCdEf",
            "ecrRepositoryPrefix": "msc2617-r02-om3",
        },
    ):
        r = _call("CreatePullThroughCacheRule", body)
        assert r.status_code == 400, r.text
        assert r.json()["__type"] == "AccessDeniedException"
        assert r.json()["Message"] == denied, r.text
    _call("CreatePullThroughCacheRule", {"upstreamRegistryUrl": "public.ecr.aws"})
    # Other tests share the server under xdist, so check only what this test could have made.
    rules = _call("DescribePullThroughCacheRules", {"maxResults": 1000}).json()["pullThroughCacheRules"]
    assert not {r["ecrRepositoryPrefix"] for r in rules} & {"BAD", "msc2617-r02-om", "msc2617-r02-om3"}
    assert all(r.get("ecrRepositoryPrefix") for r in rules)


def _dh_rule(prefix, secret_name):
    arn = _secret(secret_name, SecretString="x")
    r = _dh({"credentialArn": arn}, prefix)
    assert r.status_code == 200, r.text
    return arn


def test_update_replaces_credential_and_keeps_created_at():
    # r04-upd-dh-cred, r04-upd-dh-cred--after, r04-upd-dh-same-cred
    _dh_rule("msc2617-r04-dh", "ecr-pullthroughcache/msc-2617-r04-s1")
    created = _describe_prefix("msc2617-r04-dh")
    arn2 = _secret("ecr-pullthroughcache/msc-2617-r04-s2", SecretString="x")
    r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r04-dh", "credentialArn": arn2})
    assert r.status_code == 200, r.text
    assert set(r.json()) == {"ecrRepositoryPrefix", "registryId", "updatedAt", "credentialArn"}
    assert r.json()["credentialArn"] == arn2 and r.json()["registryId"] == _ACCT
    after = _describe_prefix("msc2617-r04-dh")
    assert after["credentialArn"] == arn2
    assert after["createdAt"] == created["createdAt"]
    assert after["upstreamRegistryUrl"] == "registry-1.docker.io"
    r2 = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r04-dh", "credentialArn": arn2})
    assert r2.status_code == 200, r2.text
    assert r2.json()["updatedAt"] >= r.json()["updatedAt"]
    assert _describe_prefix("msc2617-r04-dh")["updatedAt"] == r2.json()["updatedAt"]


def test_update_ignores_upstream_and_unknown_members():
    # r04-upd-body-upstream-url-existing, r04-upd-body-upstream-registry-existing
    arn = _dh_rule("msc2617-r04-dh", "ecr-pullthroughcache/msc-2617-r04-s1")
    r = _call(
        "UpdatePullThroughCacheRule",
        {
            "ecrRepositoryPrefix": "msc2617-r04-dh",
            "credentialArn": arn,
            "upstreamRegistryUrl": "quay.io",
            "upstreamRegistry": "quay",
            "upstreamRepositoryPrefix": "BAD PREFIX",
            "unknownMember": 1,
        },
    )
    assert r.status_code == 200, r.text
    assert "upstreamRepositoryPrefix" not in r.json()
    after = _describe_prefix("msc2617-r04-dh")
    assert after["upstreamRegistryUrl"] == "registry-1.docker.io"
    assert after["credentialArn"] == arn


def test_update_needs_a_credential():
    # r04-upd-dh-no-fields, r04-upd-missing-no-fields, r04-upd-existing-role-blank
    _dh_rule("msc2617-r04-dh", "ecr-pullthroughcache/msc-2617-r04-s1")
    for body in (
        {"ecrRepositoryPrefix": "msc2617-r04-dh"},
        {"ecrRepositoryPrefix": "msc2617-r04-missing"},
        {"ecrRepositoryPrefix": "msc2617-r04-dh", "customRoleArn": ""},
    ):
        _expect(_call("UpdatePullThroughCacheRule", body), 400, "InvalidParameterException", _ONE_CRED)


def test_update_shape_rules():
    # r04-upd-missing-prefix-member, r04-upd-blank-prefix, r04-upd-prefix-upper
    arn = f"{_SM}:us-east-1:{_ACCT}:secret:ecr-pullthroughcache/msc-2617-r04-update-s1-5ItlEM"
    _expect(_call("UpdatePullThroughCacheRule", {"credentialArn": arn}), 400, "InvalidParameterException", _NULL_PREFIX)
    _expect(
        _call("UpdatePullThroughCacheRule", {"credentialArn": arn, "ecrRepositoryPrefix": ""}),
        400, "InvalidParameterException",
        _prefix_msg("ecrRepositoryPrefix") + "; Invalid parameter at 'ecrRepositoryPrefix' failed to satisfy constraint: 'Member must have length greater than or equal to 2'",
    )
    _expect(
        _call("UpdatePullThroughCacheRule", {"credentialArn": arn, "ecrRepositoryPrefix": "MSC-2617-R04-UPDATE-MISSING"}),
        400, "InvalidParameterException", _prefix_msg("ecrRepositoryPrefix"),
    )


def test_update_secret_checks_are_the_create_ones():
    # r04-upd-existing-cred-nosuch-secret, -other-region, -other-account
    _dh_rule("msc2617-r04-dh", "ecr-pullthroughcache/msc-2617-r04-s1")
    base = "secret:ecr-pullthroughcache/msc-2617-r04-update-nosuch-AbCdEf"
    r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r04-dh", "credentialArn": f"{_SM}:us-east-1:{_ACCT}:{base}"})
    _expect(
        r, 400, "SecretNotFoundException",
        "The ARN of the secret specified in the pull through cache rule was not found. Update the pull through cache rule with a valid secret ARN and try again.",
    )
    for arn in (f"{_SM}:us-west-1:{_ACCT}:{base}", f"{_SM}:us-east-1:111122223333:{base}"):
        r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r04-dh", "credentialArn": arn})
        _expect(r, 400, "InvalidParameterException", _SECRET_REGION_UPD)


def test_update_secret_content_not_parsed_and_partial_arn_verbatim():
    # r04-upd-existing-cred-plain, -nokeys, -partial-arn
    _dh_rule("msc2617-r04-dh", "ecr-pullthroughcache/msc-2617-r04-s1")
    for name, value in (("plain", "not json"), ("nokeys", '{"a": "b"}')):
        arn = _secret(f"ecr-pullthroughcache/msc-2617-r04-upd-{name}-content", SecretString=value)
        r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r04-dh", "credentialArn": arn})
        assert r.status_code == 200, r.text
        assert r.json()["credentialArn"] == arn
    partial = arn.rsplit("-", 1)[0]
    r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r04-dh", "credentialArn": partial})
    assert r.status_code == 200, r.text
    assert r.json()["credentialArn"] == partial
    assert _describe_prefix("msc2617-r04-dh")["credentialArn"] == partial


def test_update_unknown_prefix_after_other_checks():
    # r04-upd-missing-valid-cred, r04-upd-missing-cred-nokeys, r04-upd-missing-role-len-2048
    arn = _secret("ecr-pullthroughcache/msc-2617-r04-s1", SecretString="x")
    role = f"arn:aws:iam::{_ACCT}:role/" + "r" * (2048 - len(f"arn:aws:iam::{_ACCT}:role/"))
    msg = _UPD_NOT_FOUND.format("msc2617-r04-missing")
    for body in ({"credentialArn": arn}, {"customRoleArn": role}):
        r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r04-missing", **body})
        _expect(r, 400, "PullThroughCacheRuleNotFoundException", msg)


def test_update_rule_type_compatibility():
    # r04-upd-quay-add-cred, r04-upd-pub-add-cred, r04-upd-pub-add-role
    arn = _secret("ecr-pullthroughcache/msc-2617-r04-s1", SecretString="x")
    for prefix, url in (("msc2617-r04-quay", "quay.io"), ("msc2617-r04-pub", "public.ecr.aws")):
        assert _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": url}).status_code == 200
    for prefix, body in (
        ("msc2617-r04-quay", {"credentialArn": arn}),
        ("msc2617-r04-pub", {"credentialArn": arn}),
        ("msc2617-r04-pub", {"customRoleArn": f"arn:aws:iam::{_ACCT}:role/msc-2617-r04-update-nosuch-role"}),
    ):
        r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, **body})
        _expect(r, 400, "InvalidParameterException", _UNAUTH_RULE)


# ── ValidatePullThroughCacheRule ──────────────────────────

_PARSE_FAIL = json.dumps(
    {"failureCode": "UNABLE_TO_PARSE_SECRET",
     "failureReason": "Unable to parse secret value. Secret must contain 'username' and 'accessToken' keys."},
    separators=(",", ":"),
)


def test_validate_unauthenticated_rule_is_valid_and_unknown_is_not_found():
    # r05-validate-a, r05-validate-b, r05-validate-regid-own, r05-validate-prefix-trailing-slash-of-existing
    assert _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r05-b", "upstreamRegistryUrl": "quay.io"}).status_code == 200
    for body in ({"ecrRepositoryPrefix": "msc2617-r05-b"}, {"ecrRepositoryPrefix": "msc2617-r05-b/", "registryId": _ACCT}):
        r = _call("ValidatePullThroughCacheRule", body)
        assert r.status_code == 200, r.text
        assert r.json() == {"ecrRepositoryPrefix": "msc2617-r05-b", "registryId": _ACCT,
                            "upstreamRegistryUrl": "quay.io", "isValid": True, "failure": ""}
    r = _call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "z9"})
    _expect(r, 400, "PullThroughCacheRuleNotFoundException",
            f"A pull-through cache rule with repository prefix 'z9' does not exist in the registry with id '{_ACCT}'")
    r = _call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "MSC2617-R05-B"})
    _expect(r, 400, "InvalidParameterException", _prefix_msg("ecrRepositoryPrefix"))


@pytest.mark.parametrize(
    "key,value",
    [("r03-validate-cred-plain", "plain"), ("r03-validate-cred-nokeys", '{"a":"b"}'),
     ("r03-validate-cred-emptyjson", "{}"), ("r03-validate-cred-onlyuser", '{"username":"u"}')],
)
def test_validate_unparseable_secret(key, value):
    arn = _secret("ecr-pullthroughcache/msc-2617-r03-vparse", SecretString=value)
    assert _dh({"credentialArn": arn}, "msc2617-r03-v").status_code == 200
    r = _call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r03-v"})
    assert r.status_code == 200, r.text
    assert r.json() == {"ecrRepositoryPrefix": "msc2617-r03-v", "registryId": _ACCT,
                        "upstreamRegistryUrl": "registry-1.docker.io", "credentialArn": arn,
                        "isValid": False, "failure": _PARSE_FAIL}


def test_validate_deleted_secret_unable_to_access_omits_members():
    # ledger line 44 (AWS captured an unreadable secret; deleted is MiniStack's choice, stated in the ledger)
    arn = _secret("ecr-pullthroughcache/msc-2617-r03-vgone", SecretString="{}")
    assert _dh({"credentialArn": arn}, "msc2617-r03-g").status_code == 200
    _sm().delete_secret(SecretId=arn, ForceDeleteWithoutRecovery=True)
    r = _call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r03-g"})
    assert r.status_code == 200, r.text
    assert r.json() == {"ecrRepositoryPrefix": "msc2617-r03-g", "registryId": _ACCT, "isValid": False,
                        "failure": json.dumps({"failureCode": "UNABLE_TO_ACCESS_SECRET",
                                               "failureReason": "The secret is unable to be accessed. Verify the resource permissions for the secret and try again."},
                                              separators=(",", ":"))}


def test_validate_azure_rule_is_unable_to_validate_with_the_captured_text():
    # r03-validate-cred-ok-azure-container-registry, r19-azure-validate (the endpoint is unreachable from the emulator)
    arn = _secret("ecr-pullthroughcache/msc-2617-r19-az", SecretString='{"username":"u","accessToken":"t"}')
    r = _call("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": "msc2617-r19-az", "upstreamRegistryUrl": "msc2617example.azurecr.io", "credentialArn": arn})
    assert r.status_code == 200, r.text
    r = _call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r19-az"})
    assert r.status_code == 200, r.text
    assert r.json() == {
        "ecrRepositoryPrefix": "msc2617-r19-az", "registryId": _ACCT, "upstreamRegistryUrl": "msc2617example.azurecr.io",
        "credentialArn": arn, "isValid": False,
        "failure": json.dumps({"failureCode": "UNABLE_TO_VALIDATE",
                               "failureReason": "Failed to get response from upstream registry authorization endpoint. "
                                                "The upstream registry may be unavailable. Try again."},
                              separators=(",", ":"))}


# ---- part 10: joined order, ROOT + upstreamRepositoryPrefix, Update with both fields ----

_UPP_MSG_PATTERN = _prefix_msg("upstreamRepositoryPrefix")
_URL_NULL = "Invalid parameter at 'upstreamRegistryUrl' failed to satisfy constraint: 'Member must not be null'"
_CROSS_ACCOUNT_ROLE = "arn:aws:iam::111122223333:role/msc-2617-r23-nosuch-role"
_OWN_ROLE = f"arn:aws:iam::{_ACCT}:role/msc-2617-r23-nosuch-role"
_NOSUCH_SECRET = f"arn:aws:secretsmanager:us-east-1:{_ACCT}:secret:ecr-pullthroughcache/msc-2617-r23-nosuch-AbCdEf"


@pytest.mark.parametrize(
    "key,body",
    [
        ("r23-upp-bad+url-missing-upp-first", {"upstreamRepositoryPrefix": "BAD", "ecrRepositoryPrefix": "msc2617-r23-a2"}),
        ("r23-upp-bad+url-missing-upp-last", {"ecrRepositoryPrefix": "msc2617-r23-a2", "upstreamRepositoryPrefix": "BAD"}),
    ],
)
def test_create_url_missing_precedes_upstream_prefix(key, body):
    r = _call("CreatePullThroughCacheRule", body)
    _expect(r, 400, "InvalidParameterException", _URL_NULL + "; " + _UPP_MSG_PATTERN)
    assert _call("DescribePullThroughCacheRules", {}).json()["pullThroughCacheRules"] == []


@pytest.mark.parametrize(
    "key,url,upp,message",
    [
        ("r23-root-upp-alone", "public.ecr.aws", "docker",
         "Invalid input: Must specify ecr-repository-prefix when specify upstream-repository-prefix."),
        ("r23-root-upp-quay", "quay.io", "docker",
         "Invalid input: Must specify ecr-repository-prefix when specify upstream-repository-prefix."),
        ("r23-root-upp-bad", "public.ecr.aws", "BAD", _UPP_MSG_PATTERN),
    ],
)
def test_create_root_with_upstream_prefix(key, url, upp, message):
    r = _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": "ROOT", "upstreamRegistryUrl": url, "upstreamRepositoryPrefix": upp})
    _expect(r, 400, "InvalidParameterException", message)
    assert _call("DescribePullThroughCacheRules", {"ecrRepositoryPrefixes": ["ROOT"]}).status_code == 400


def test_update_both_fields_with_readable_secret_is_refused_on_any_rule():
    # r23-both-dh+readable, r23-both-missing+readable, r23-dh-rule, r23-secret-s1
    arn = _dh_rule("msc2617-r23-dh", "ecr-pullthroughcache/msc-2617-r23-s1")
    before = _describe_prefix("msc2617-r23-dh")
    for prefix in ("msc2617-r23-dh", "msc2617-r23-missing"):
        r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "credentialArn": arn, "customRoleArn": _OWN_ROLE})
        _expect(r, 400, "InvalidParameterException", _ONE_CRED)
    assert _describe_prefix("msc2617-r23-dh") == before


def test_update_cross_account_role_precedes_secret_and_rule_lookup():
    # r23-both-dh+role-other-account, r23-both-missing+role-other-account, --after
    _dh_rule("msc2617-r23-dh", "ecr-pullthroughcache/msc-2617-r23-s1")
    before = _describe_prefix("msc2617-r23-dh")
    for prefix in ("msc2617-r23-dh", "msc2617-r23-missing"):
        r = _call(
            "UpdatePullThroughCacheRule",
            {"ecrRepositoryPrefix": prefix, "credentialArn": _NOSUCH_SECRET, "customRoleArn": _CROSS_ACCOUNT_ROLE},
        )
        assert r.status_code == 400, r.text
        assert r.json() == {"__type": "AccessDeniedException", "Message": "Cross-account pass role is not allowed."}
    assert _describe_prefix("msc2617-r23-dh") == before


def test_update_ignores_invalid_upstream_members_but_looks_up_the_rule():
    # r23-upd-upp-enum-members-existing(+--after), r23-upd-upp-enum-members-missing
    arn = _dh_rule("msc2617-r23-dh", "ecr-pullthroughcache/msc-2617-r23-s1")
    before = _describe_prefix("msc2617-r23-dh")
    body = {"credentialArn": arn, "upstreamRepositoryPrefix": "BAD", "upstreamRegistry": "foo"}
    r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r23-dh", **body})
    assert r.status_code == 200, r.text
    after = _describe_prefix("msc2617-r23-dh")
    assert {k: v for k, v in after.items() if k != "updatedAt"} == {k: v for k, v in before.items() if k != "updatedAt"}
    r = _call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r23-missing", **body})
    _expect(r, 400, "PullThroughCacheRuleNotFoundException", _UPD_NOT_FOUND.format("msc2617-r23-missing"))


def test_delete_and_describe_error_paths():
    # r05-delete-missing, r05-delete-prefix-upper, r05-desc-max+filter, r05-desc-token+filter,
    # r05-desc-token-garbage, r05-desc-max-1001
    _expect(_call("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r05-missing"}), 400,
            "PullThroughCacheRuleNotFoundException",
            f"A pull-through cache rule with repository prefix 'msc2617-r05-missing' does not exist in the registry with id '{_ACCT}'")
    _expect(_call("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": "MSC2617-R05-MISSING"}), 400,
            "InvalidParameterException", _prefix_msg("ecrRepositoryPrefix"))
    _expect(_call("DescribePullThroughCacheRules", {"maxResults": 1, "ecrRepositoryPrefixes": ["msc2617-r05-b"]}), 400,
            "InvalidParameterException",
            "Invalid parameter at 'MaxResults' failed to satisfy constraint: 'Cannot provide MaxResults if providing RepositoryPrefixes'")
    _expect(_call("DescribePullThroughCacheRules", {"nextToken": "abc", "ecrRepositoryPrefixes": ["msc2617-r05-b"]}), 400,
            "InvalidParameterException",
            "Invalid parameter at 'NextToken' failed to satisfy constraint: 'Cannot provide NextToken if providing RepositoryPrefixes'")
    _expect(_call("DescribePullThroughCacheRules", {"nextToken": "not-a-token"}), 400, "InvalidParameterException",
            "Invalid parameter at 'NextToken' failed to satisfy constraint: 'Invalid next token provided'")
    _expect(_call("DescribePullThroughCacheRules", {"maxResults": 1001}), 400, "InvalidParameterException",
            "Invalid parameter at 'maxResults' failed to satisfy constraint: 'Member must have value less than or equal to 1000'")


def test_delete_response_members_and_second_delete_is_not_found():
    # r19-delete-dh-cred-updated, r05-delete-c-again: the credentialed rule's members come back, then it is gone
    sm = requests.post(ENDPOINT + "/", data=json.dumps({"Name": "ecr-pullthroughcache/msc-2617-r19-del",
                                                        "SecretString": '{"username":"u","accessToken":"t"}'}),
                       headers={"X-Amz-Target": "secretsmanager.CreateSecret", "Content-Type": "application/x-amz-json-1.1",
                                "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/secretsmanager/aws4_request, SignedHeaders=host, Signature=x"})
    arn = sm.json()["ARN"]
    assert _dh({"credentialArn": arn}, "msc2617-r19-del").status_code == 200
    r = _call("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r19-del"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body) == {"ecrRepositoryPrefix", "upstreamRegistryUrl", "createdAt", "registryId", "credentialArn"}
    assert body["credentialArn"] == arn and body["registryId"] == _ACCT
    assert _call("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r19-del"}).status_code == 400


_MAX_LOW = "Invalid parameter at 'maxResults' failed to satisfy constraint: 'Member must have value greater than or equal to 1'"
_MAX_HIGH = "Invalid parameter at 'maxResults' failed to satisfy constraint: 'Member must have value less than or equal to 1000'"


@pytest.mark.parametrize("raw,message", [("1e999", _MAX_HIGH), ("-1e999", _MAX_LOW)])
def test_describe_max_results_infinity_answers_as_out_of_range_by_sign(raw, message):
    """MiniStack contract, uncaptured: ledger 211 (infinity takes the out-of-range answer of its sign, as the
    captured r06-describe-max-huge for the high side and r06-describe-max-negative for the low side)."""
    response = requests.post(ENDPOINT + "/", data='{"maxResults": ' + raw + "}", headers={
        "X-Amz-Target": "AmazonEC2ContainerRegistry_V20150921.DescribePullThroughCacheRules",
        "Content-Type": "application/x-amz-json-1.1",
        "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/ecr/aws4_request, SignedHeaders=host, Signature=x"})
    _expect(response, 400, "InvalidParameterException", message)


def test_key_policy_with_principal_star_names_every_principal():
    """MiniStack contract, uncaptured: ledger 216 (a Principal "*" or {"AWS": "*"} statement grants every
    principal, so it counts as naming the service-linked role; an explicit Deny still wins)."""
    from ministack.services import kms

    role = f"arn:aws:iam::{_ACCT}:role/aws-service-role/pullthroughcache.ecr.amazonaws.com/AWSServiceRoleForECRPullThroughCache"

    def key_with(*statements):
        policy = {"Version": "2012-10-17", "Statement": list(statements)}
        _, _, body = kms._create_key({"Policy": json.dumps(policy)})
        return json.loads(body)["KeyMetadata"]["KeyId"]

    root = {"Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{_ACCT}:root"}, "Action": "kms:*", "Resource": "*"}
    allow = {"Effect": "Allow", "Action": "kms:Decrypt", "Resource": "*"}
    deny = {"Effect": "Deny", "Principal": "*", "Action": "kms:Decrypt", "Resource": "*"}
    grants = {}
    for label, statements in {
        "root-only": [root],
        "string-star": [root, {**allow, "Principal": "*"}],
        "aws-star": [root, {**allow, "Principal": {"AWS": "*"}}],
        "aws-star-list": [root, {**allow, "Principal": {"AWS": ["*"]}}],
        "star-then-deny": [root, {**allow, "Principal": "*"}, deny],
    }.items():
        grants[label] = kms.key_policy_grants_named_principal(key_with(*statements), "Decrypt", role)
    kms.reset()
    assert grants == {"root-only": False, "string-star": True, "aws-star": True, "aws-star-list": True,
                      "star-then-deny": False}
