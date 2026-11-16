"""ECR pull-through cache: the caller's and the service-linked role's read of the credential secret (issue 2617).

Bodies and messages are taken from the AWS captures named in each case. Under AUTH=false no read is checked
(every secret reads as readable); under AUTH=true the root key is a caller and an explicit Deny refuses it.
"""

import json
import os

import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
_ACCT = "000000000000"
_UNABLE = "The secret is unable to be accessed. Verify the resource permissions for the secret and try again."
_CANT = "The secret can't be accessed. Verify the resource permissions for the secret and try again."
_SLR = f"arn:aws:iam::{_ACCT}:role/aws-service-role/pullthroughcache.ecr.amazonaws.com/AWSServiceRoleForECRPullThroughCache"
_PREFIX = "msc2617-r28-auth"
# r38-omit-gsv-create-om-bad-prefix
_BAD_PREFIX = ("Invalid parameter at 'ecrRepositoryPrefix' failed to satisfy constraint: 'Member must satisfy regular "
               "expression pattern: ^([a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*(\\/[a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*)*\\/?|ROOT)$'")
# r02-cred-nosuch-secret-docker-hub
_NOT_FOUND = ("The ARN of the secret specified in the pull through cache rule was not found. Update the pull through "
              "cache rule with a valid secret ARN and try again.")

pytestmark = pytest.mark.auth_contract


def _client(service):
    return boto3.client(service, endpoint_url=ENDPOINT, region_name="us-east-1",
                        aws_access_key_id="test", aws_secret_access_key="test")


@pytest.fixture
def ecr():
    client = _client("ecr")
    yield client
    # Only this file's rules: other test files share the registry.
    for rule in client.describe_pull_through_cache_rules(maxResults=1000)["pullThroughCacheRules"]:
        if rule["ecrRepositoryPrefix"].startswith(("msc2617-r28-auth", "msc2617-r36", "msc2617-r38", "msc2617-r40")):
            client.delete_pull_through_cache_rule(ecrRepositoryPrefix=rule["ecrRepositoryPrefix"])


@pytest.fixture
def secrets():
    client = _client("secretsmanager")
    created = []

    def make(suffix, **kw):
        name = f"ecr-pullthroughcache/msc-2617-r28-auth-{suffix}"
        try:
            client.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
        except ClientError:
            pass
        arn = client.create_secret(Name=name, SecretString=json.dumps({"username": "u", "accessToken": "t"}),
                                   **kw)["ARN"]
        created.append(name)
        return arn

    make.client = client
    yield make
    for name in created:
        client.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)


def _deny(arn, **extra):
    return json.dumps({"Version": "2012-10-17", "Statement": [{
        "Sid": "Deny", "Effect": "Deny", "Principal": "*", "Action": "secretsmanager:GetSecretValue",
        "Resource": "*", **extra}]})


_VIA = {"Condition": {"ForAnyValue:StringEquals": {"aws:CalledVia": ["ecr.amazonaws.com"]}}}
_ROLE_ONLY = {"Condition": {"ArnLike": {"aws:PrincipalArn": "arn:aws:iam::*:role/aws-service-role/"
                                                             "pullthroughcache.ecr.amazonaws.com/*"}}}


def _create(ecr, arn, prefix=_PREFIX):
    return ecr.create_pull_through_cache_rule(ecrRepositoryPrefix=prefix, upstreamRegistryUrl="registry-1.docker.io",
                                              credentialArn=arn)


def _refused(exc, code, message):
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == code
    assert exc.value.response["Error"]["Message"] == message


@pytest.mark.parametrize("via", [False, True])
def test_create_and_update_read_the_secret_as_the_caller_through_ecr(ecr, secrets, auth_enabled, via):
    # r28-create-deny-policy, r28-create-deny-via-policy, r28-update-to-deny-policy, r28-deny-policy--after
    arn = secrets("deny")
    ok = secrets("ok")
    _create(ecr, ok)
    secrets.client.put_resource_policy(SecretId=arn, ResourcePolicy=_deny(arn, **(_VIA if via else {})))
    if auth_enabled:
        with pytest.raises(ClientError) as exc:
            _create(ecr, arn, "msc2617-r28-auth-new")
        _refused(exc, "UnableToAccessSecretException", _UNABLE)
        with pytest.raises(ClientError) as exc:
            ecr.update_pull_through_cache_rule(ecrRepositoryPrefix=_PREFIX, credentialArn=arn)
        _refused(exc, "UnableToAccessSecretException", _UNABLE)
        names = [r["ecrRepositoryPrefix"] for r in ecr.describe_pull_through_cache_rules(maxResults=1000)["pullThroughCacheRules"]
                 if r["ecrRepositoryPrefix"].startswith("msc2617-r28-auth")]
        assert names == [_PREFIX]
        # Removing the policy takes effect at once.
        secrets.client.delete_resource_policy(SecretId=arn)
    assert _create(ecr, arn, "msc2617-r28-auth-new")["ecrRepositoryPrefix"] == "msc2617-r28-auth-new"
    ecr.update_pull_through_cache_rule(ecrRepositoryPrefix=_PREFIX, credentialArn=arn)


@pytest.mark.parametrize("deny_via", [True, False])
def test_the_secret_read_refuses_every_create_and_update_shape(ecr, secrets, auth_enabled, deny_via):
    # r28-create-om-bad-url+deny-via, r28-create-om-public+deny-via, r28-create-om-azure+deny-via,
    # r28-update-same-deny-via-policy, r28-update-missing-deny-via-policy, r28-update-public-deny-via-policy,
    # r28-update-om-both+deny-via, r28-update-om-both+deny
    if not auth_enabled:
        pytest.skip("caller identity checks need AUTH=true")
    arn = secrets("shapes")
    ok = secrets("shapes-ok")
    _create(ecr, ok)
    ecr.create_pull_through_cache_rule(ecrRepositoryPrefix=_PREFIX + "-quay", upstreamRegistryUrl="quay.io")
    secrets.client.put_resource_policy(SecretId=arn, ResourcePolicy=_deny(arn, **(_VIA if deny_via else {})))
    role = f"arn:aws:iam::{_ACCT}:role/msc-2617-r28-nosuch-role"
    calls = [
        lambda: ecr.create_pull_through_cache_rule(
            ecrRepositoryPrefix=_PREFIX + "-bad-url", upstreamRegistryUrl="registry.example.com", credentialArn=arn),
        lambda: ecr.create_pull_through_cache_rule(
            ecrRepositoryPrefix=_PREFIX + "-public", upstreamRegistryUrl="quay.io", credentialArn=arn),
        lambda: ecr.create_pull_through_cache_rule(
            ecrRepositoryPrefix=_PREFIX + "-azure", upstreamRegistryUrl="msc2617example.azurecr.io",
            credentialArn=arn),
        lambda: ecr.update_pull_through_cache_rule(ecrRepositoryPrefix=_PREFIX, credentialArn=arn),
        lambda: ecr.update_pull_through_cache_rule(ecrRepositoryPrefix=_PREFIX + "-missing", credentialArn=arn),
        lambda: ecr.update_pull_through_cache_rule(ecrRepositoryPrefix=_PREFIX + "-quay", credentialArn=arn),
        lambda: ecr.update_pull_through_cache_rule(ecrRepositoryPrefix=_PREFIX, credentialArn=arn,
                                                   customRoleArn=role),
    ]
    for call in calls:
        with pytest.raises(ClientError) as exc:
            call()
        _refused(exc, "UnableToAccessSecretException", _UNABLE)
    names = sorted(r["ecrRepositoryPrefix"] for r in
                   ecr.describe_pull_through_cache_rules(maxResults=1000)["pullThroughCacheRules"]
                   if r["ecrRepositoryPrefix"].startswith("msc2617-r28-auth"))
    assert names == [_PREFIX, _PREFIX + "-quay"]


def test_the_callers_own_get_secret_value_is_not_denied_by_the_called_via_statement(ecr, secrets):
    # r28-deny-via-caller-get-secret
    arn = secrets("via-caller")
    secrets.client.put_resource_policy(SecretId=arn, ResourcePolicy=_deny(arn, **_VIA))
    assert secrets.client.get_secret_value(SecretId=arn)["ARN"] == arn


def test_the_service_linked_role_deny_does_not_refuse_create(ecr, secrets):
    # r28-create-deny-slr-policy
    arn = secrets("deny-slr")
    secrets.client.put_resource_policy(SecretId=arn, ResourcePolicy=_deny(arn, **_ROLE_ONLY))
    assert _create(ecr, arn)["credentialArn"] == arn


def test_validate_reads_as_the_caller_then_as_the_service_linked_role(ecr, secrets, auth_enabled):
    # r28-validate-deny-policy, r28-validate-deny-via-policy, r28-validate-deny-slr-policy, r28-deny-via-validate-poll-1
    caller = secrets("v-caller")
    via = secrets("v-via")
    role = secrets("v-role")
    for name, arn, extra in (("c", caller, {}), ("v", via, _VIA), ("r", role, _ROLE_ONLY)):
        _create(ecr, arn, f"msc2617-r28-auth-{name}")
        secrets.client.put_resource_policy(SecretId=arn, ResourcePolicy=_deny(arn, **extra))
    if not auth_enabled:
        return
    for name, reason in (("c", _UNABLE), ("v", _UNABLE)):
        out = ecr.validate_pull_through_cache_rule(ecrRepositoryPrefix=f"msc2617-r28-auth-{name}")
        assert out["isValid"] is False
        assert json.loads(out["failure"]) == {"failureCode": "UNABLE_TO_ACCESS_SECRET", "failureReason": reason}
        assert "credentialArn" not in out
    out = ecr.validate_pull_through_cache_rule(ecrRepositoryPrefix="msc2617-r28-auth-r")
    assert out["isValid"] is False
    assert json.loads(out["failure"]) == {"failureCode": "UNABLE_TO_ACCESS_SECRET", "failureReason": _CANT}
    assert out["credentialArn"] == role
    # Removing the policy takes effect on the next Validate.
    secrets.client.delete_resource_policy(SecretId=via)
    out = ecr.validate_pull_through_cache_rule(ecrRepositoryPrefix="msc2617-r28-auth-v")
    assert "UNABLE_TO_ACCESS_SECRET" not in out["failure"]


def test_validate_customer_managed_key_must_name_the_service_linked_role(ecr, secrets, auth_enabled):
    # r28-validate-kms-key-enabled (default key policy: "can't be accessed")
    kms = _client("kms")
    key_id = kms.create_key()["KeyMetadata"]["KeyId"]
    named = kms.create_key(Policy=json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{_ACCT}:root"}, "Action": "kms:*", "Resource": "*"},
        {"Effect": "Allow", "Principal": {"AWS": _SLR}, "Action": "kms:Decrypt", "Resource": "*"}]}))["KeyMetadata"]["KeyId"]
    default_arn = secrets("kms-default", KmsKeyId=key_id)
    named_arn = secrets("kms-named", KmsKeyId=named)
    _create(ecr, default_arn, "msc2617-r28-auth-kd")
    _create(ecr, named_arn, "msc2617-r28-auth-kn")
    if not auth_enabled:
        return
    out = ecr.validate_pull_through_cache_rule(ecrRepositoryPrefix="msc2617-r28-auth-kd")
    assert json.loads(out["failure"])["failureReason"] == _CANT
    out = ecr.validate_pull_through_cache_rule(ecrRepositoryPrefix="msc2617-r28-auth-kn")
    assert "UNABLE_TO_ACCESS_SECRET" not in out["failure"]


def test_a_key_ministack_does_not_hold_is_readable(ecr, secrets, auth_enabled):
    # user decision kms-key-not-in-ministack
    arn = secrets("kms-unheld", KmsKeyId=f"arn:aws:kms:us-east-1:{_ACCT}:key/11111111-2222-3333-4444-555555555555")
    _create(ecr, arn)
    if auth_enabled:
        out = ecr.validate_pull_through_cache_rule(ecrRepositoryPrefix=_PREFIX)
        assert "UNABLE_TO_ACCESS_SECRET" not in out["failure"]


# ── non-root callers (r38, r40) and the pull with an unreadable secret ───────────────────────────────

def _role_clients(iam_admin, policy, tag):
    """ECR client of an assumed MiniStack role holding *policy* (r38/r40 callers are assumed roles)."""
    name = f"msc2617-r38-{tag}"
    trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"AWS": "*"},
                                                      "Action": "sts:AssumeRole"}]}
    role = iam_admin.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]
    iam_admin.put_role_policy(RoleName=name, PolicyName="p", PolicyDocument=json.dumps(policy))
    creds = _client("sts").assume_role(RoleArn=role["Arn"], RoleSessionName="sess")["Credentials"]
    ecr = boto3.client("ecr", endpoint_url=ENDPOINT, region_name="us-east-1",
                       aws_access_key_id=creds["AccessKeyId"], aws_secret_access_key=creds["SecretAccessKey"],
                       aws_session_token=creds["SessionToken"])
    return name, role["Arn"], ecr


def _policy(*actions):
    return {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": list(actions), "Resource": "*"}]}


@pytest.fixture
def iam_admin():
    if os.environ.get("AUTH", "false").lower() != "true":
        pytest.skip("caller identity checks need AUTH=true")
    client = _client("iam")
    names = []
    yield client, names
    for name in names:
        client.delete_role_policy(RoleName=name, PolicyName="p")
        client.delete_role(RoleName=name)


def test_non_root_caller_without_get_secret_value_is_refused(ecr, secrets, iam_admin, auth_enabled):
    # r38-omit-gsv-*, r38-omit-all-*, r38-allow-gsv-*
    arn = secrets("r38")
    admin, names = iam_admin
    name, _, omit = _role_clients(admin, _policy("ecr:*"), "omit")
    names.append(name)
    with pytest.raises(ClientError) as exc:
        _create(omit, arn, "msc2617-r38-new")
    _refused(exc, "UnableToAccessSecretException", _UNABLE)
    assert [r for r in ecr.describe_pull_through_cache_rules(maxResults=1000)["pullThroughCacheRules"]
            if r["ecrRepositoryPrefix"] == "msc2617-r38-new"] == []
    # A bad ecrRepositoryPrefix is a shape error, which comes before the secret read.
    with pytest.raises(ClientError) as exc:
        _create(omit, arn, "bad prefix!")
    _refused(exc, "InvalidParameterException", _BAD_PREFIX)
    name, _, allow = _role_clients(admin, _policy("ecr:*", "secretsmanager:GetSecretValue"), "allow")
    names.append(name)
    assert _create(allow, arn, "msc2617-r38-new")["ecrRepositoryPrefix"] == "msc2617-r38-new"
    with pytest.raises(ClientError) as exc:
        omit.update_pull_through_cache_rule(ecrRepositoryPrefix="msc2617-r38-new", credentialArn=arn)
    _refused(exc, "UnableToAccessSecretException", _UNABLE)
    result = omit.validate_pull_through_cache_rule(ecrRepositoryPrefix="msc2617-r38-new")
    assert result["isValid"] is False and json.loads(result["failure"]) == {
        "failureCode": "UNABLE_TO_ACCESS_SECRET", "failureReason": _UNABLE}


def test_non_root_caller_scoped_to_one_secret_gets_unable_to_access_for_a_missing_one(ecr, secrets, iam_admin,
                                                                                      auth_enabled):
    # r40-allow-c-cred-missing, r40-allow-u-cred-missing+role, r40-none-u-dh-rule-cred (policy as in r40-iam-role-policy-allow)
    arn = secrets("r40-scoped")
    missing = arn.rsplit("-", 1)[0] + "-nosuch-AbCdEf"
    admin, names = iam_admin
    scoped = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "ecr:*", "Resource": "*"},
        {"Effect": "Allow", "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
         "Resource": arn}]}
    name, _, allow = _role_clients(admin, scoped, "r40-scoped")
    names.append(name)
    with pytest.raises(ClientError) as exc:
        _create(allow, missing, "msc2617-r40-miss")
    _refused(exc, "UnableToAccessSecretException", _UNABLE)
    _create(allow, arn, "msc2617-r40-miss")
    with pytest.raises(ClientError) as exc:
        allow.update_pull_through_cache_rule(ecrRepositoryPrefix="msc2617-r40-miss", credentialArn=missing)
    _refused(exc, "UnableToAccessSecretException", _UNABLE)
    # The root caller still gets the lookup answer (r02-cred-nosuch-secret-docker-hub).
    with pytest.raises(ClientError) as exc:
        _create(ecr, missing, "msc2617-r40-root")
    _refused(exc, "SecretNotFoundException", _NOT_FOUND)


_PASS_NO_ALLOW = "because no identity-based policy allows the iam:PassRole action"
_PASS_DENY = "with an explicit deny in an identity-based policy"


def _pass_role_refusal(tag, role, tail):
    return (f"User: arn:aws:sts::{_ACCT}:assumed-role/msc2617-r38-{tag}/sess is not authorized to perform: "
            f"iam:PassRole on resource: {role} {tail}")


@pytest.mark.parametrize("operation", ["create", "update"])
@pytest.mark.parametrize("kind", ["omit", "deny", "omit-len-2049"])
def test_pass_role_on_the_custom_role_is_checked_before_everything_after_it(ecr, secrets, iam_admin, auth_enabled,
                                                                           operation, kind):
    # r40-omit-c-role-exists, r40-deny-c-role-exists, r40-omit-c-role-len-2049, r40-omit-u-*, r40-deny-u-*
    arn = secrets("r40")
    long = kind == "omit-len-2049"
    role = f"arn:aws:iam::{_ACCT}:role/" + ("a" * 2049 if long else "msc2617-r40-target")
    admin, names = iam_admin
    policy = _policy("ecr:*", "secretsmanager:GetSecretValue")
    if kind == "deny":
        policy["Statement"].append({"Effect": "Deny", "Action": "iam:PassRole", "Resource": "*"})
    tag = f"r40-{kind}-{operation}"
    name, _, caller = _role_clients(admin, policy, tag)
    names.append(name)
    prefix = "msc2617-r40"
    if operation == "create":
        call = caller.create_pull_through_cache_rule
        args = {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "registry-1.docker.io",
                "credentialArn": arn, "customRoleArn": role}
    else:
        ecr.create_pull_through_cache_rule(ecrRepositoryPrefix=prefix, upstreamRegistryUrl="registry-1.docker.io",
                                           credentialArn=arn)
        call = caller.update_pull_through_cache_rule
        args = {"ecrRepositoryPrefix": prefix, "credentialArn": arn, "customRoleArn": role}
    try:
        with pytest.raises(ClientError) as exc:
            call(**args)
        _refused(exc, "AccessDeniedException",
                 _pass_role_refusal(tag, role, _PASS_DENY if kind == "deny" else _PASS_NO_ALLOW))
        held = [r for r in ecr.describe_pull_through_cache_rules(maxResults=1000)["pullThroughCacheRules"]
                if r["ecrRepositoryPrefix"] == prefix]
        assert len(held) == (operation == "update")
        assert not held or "customRoleArn" not in held[0]
    finally:
        try:
            ecr.delete_pull_through_cache_rule(ecrRepositoryPrefix=prefix)
        except ClientError:
            pass


def test_pull_with_an_unreadable_secret_creates_no_repository(ecr, secrets, auth_enabled):
    # ledger line 166 (uncaptured: ANSWERS pull-secret-unreadable default)
    arn = secrets("pull")
    _create(ecr, arn, "msc2617-r36-pull")
    secrets.client.put_resource_policy(SecretId=arn, ResourcePolicy=_deny(arn))
    result = ecr.batch_get_image(repositoryName="msc2617-r36-pull/library/alpine",
                                 imageIds=[{"imageTag": "3.19"}])
    if auth_enabled:
        assert result["failures"][0]["failureCode"] == "UpstreamAccessDenied"
        assert result["failures"][0]["failureReason"] == (
            "The authentication to the upstream registry failed. Verify that the credentials provided in the "
            "pull through cache rule are correct, and try again.")
        with pytest.raises(ClientError):
            ecr.describe_repositories(repositoryNames=["msc2617-r36-pull/library/alpine"])


_OTHER_REGISTRY = {
    # r04-upd-existing-regid-other, r05-desc-regid-other, r05-validate-regid-other, r05-delete-regid-other
    "CreatePullThroughCacheRule": {"ecrRepositoryPrefix": "msc2617-r02-om", "upstreamRegistryUrl": "public.ecr.aws"},
    "UpdatePullThroughCacheRule": {"ecrRepositoryPrefix": "msc-2617-r04-update-dh",
                                   "credentialArn": f"arn:aws:secretsmanager:us-east-1:{_ACCT}:secret:x"},
    "DescribePullThroughCacheRules": {},
    "ValidatePullThroughCacheRule": {"ecrRepositoryPrefix": "msc2617-r05-b"},
    "DeletePullThroughCacheRule": {"ecrRepositoryPrefix": "msc2617-r05-b"},
}


@pytest.mark.parametrize("action", list(_OTHER_REGISTRY))
def test_another_accounts_registry_id_names_the_assumed_role_caller(ecr, iam_admin, auth_enabled, action):
    # r02-om-dup+regid-other, r04-upd-existing-regid-other, r05-desc-regid-other, r05-validate-regid-other,
    # r05-delete-regid-other: AWS names whichever principal signed; r38 shows the assumed-role shape
    admin, names = iam_admin
    tag = f"regid-{action[:6].lower()}"
    name, _, caller = _role_clients(admin, _policy("ecr:*"), tag)
    names.append(name)
    method = getattr(caller, "".join(("_" + c.lower()) if c.isupper() else c for c in action).lstrip("_"))
    with pytest.raises(ClientError) as exc:
        method(registryId="111122223333", **_OTHER_REGISTRY[action])
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
    assert exc.value.response["Error"]["Message"] == (
        f"User: arn:aws:sts::{_ACCT}:assumed-role/msc2617-r38-{tag}/sess is not authorized to perform: "
        f"ecr:{action} on resource: * because no resource-based policy allows the ecr:{action} action")


def test_a_deny_all_secret_policy_on_a_disabled_key_is_unable_to_decrypt(ecr, secrets, auth_enabled):
    """MiniStack contract, uncaptured: the ledger 169 interim order (item 4, the key state, before item 5, the
    secret's resource policy). Uncaptured: v10's open question of a secret with both a disabled key and a
    deny-all resource policy. Create and Update answer UnableToDecryptSecretValueException."""
    if not auth_enabled:
        pytest.skip("the resource policy is only evaluated under AUTH=true")
    kms = _client("kms")
    key_id = kms.create_key()["KeyMetadata"]["KeyId"]
    arn = secrets("deny-and-disabled", KmsKeyId=key_id)
    ok = secrets("deny-and-disabled-ok")
    _create(ecr, ok)
    secrets.client.put_resource_policy(SecretId=arn, ResourcePolicy=_deny(arn, **_VIA))
    kms.disable_key(KeyId=key_id)
    message = "The secret is accessible but is unable to be decrypted. Verify the resource permisisons and try again."
    with pytest.raises(ClientError) as exc:
        _create(ecr, arn, _PREFIX + "-dd")
    _refused(exc, "UnableToDecryptSecretValueException", message)
    with pytest.raises(ClientError) as exc:
        ecr.update_pull_through_cache_rule(ecrRepositoryPrefix=_PREFIX, credentialArn=arn)
    _refused(exc, "UnableToDecryptSecretValueException", message)
