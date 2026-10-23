"""EC2 Verified Access control plane: the calls the Terraform aws_verifiedaccess_* resources make.

Expected codes, messages and orders are AWS's answers in us-east-1; each test names the
capture keys it follows (A/B/C/D/E/F: reads, W: writes, X/Y: load-balancer endpoints,
V: device providers, groups, logging and a network-interface endpoint, Z: check orders).
"""

import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import ENDPOINT, make_client

_POLICY = 'permit(principal, action, resource) when { context.idp.email like "*@example.com" };'
_OIDC = {
    "Issuer": "https://login.example.com",
    "AuthorizationEndpoint": "https://login.example.com/authorize",
    "TokenEndpoint": "https://login.example.com/token",
    "UserInfoEndpoint": "https://login.example.com/userinfo",
    "ClientId": "client",
    "ClientSecret": "secret",
    "Scope": "openid email",
}
_MISSING = {"Instance": "vai-0000000000000000a", "TrustProvider": "vatp-0000000000000000a",
            "Group": "vagr-0000000000000000a", "Endpoint": "vae-0000000000000000a"}


def _client(region="us-east-1", account_id="test", service="ec2", validate=True):
    return boto3.client(
        service, endpoint_url=ENDPOINT, region_name=region,
        aws_access_key_id=account_id, aws_secret_access_key="test",
        config=Config(region_name=region, retries={"total_max_attempts": 1}, parameter_validation=validate),
    )


@pytest.fixture
def raw():
    """An EC2 client that sends requests botocore would refuse to build."""
    return _client(validate=False)


def _oidc_provider(ec2, ref="idp", **extra):
    return ec2.create_verified_access_trust_provider(
        TrustProviderType="user", UserTrustProviderType="oidc", PolicyReferenceName=ref,
        OidcOptions=dict(_OIDC), **extra)["VerifiedAccessTrustProvider"]


def _device_provider(ec2, device_type="jamf", **options):
    return ec2.create_verified_access_trust_provider(
        TrustProviderType="device", DeviceTrustProviderType=device_type, PolicyReferenceName=device_type,
        DeviceOptions={"TenantId": "tenant-1", **options})["VerifiedAccessTrustProvider"]


def _instance(ec2, **extra):
    return ec2.create_verified_access_instance(**extra)["VerifiedAccessInstance"]["VerifiedAccessInstanceId"]


def _attach(ec2, inst_id, tp_id):
    ec2.attach_verified_access_trust_provider(VerifiedAccessInstanceId=inst_id, VerifiedAccessTrustProviderId=tp_id)


def _instance_with_provider(ec2):
    """An instance with an OIDC provider attached: AWS refuses groups on an instance without one."""
    inst_id = _instance(ec2)
    tp_id = _oidc_provider(ec2)["VerifiedAccessTrustProviderId"]
    _attach(ec2, inst_id, tp_id)
    return inst_id, tp_id


def _group(ec2, inst_id, **extra):
    return ec2.create_verified_access_group(
        VerifiedAccessInstanceId=inst_id, **extra)["VerifiedAccessGroup"]["VerifiedAccessGroupId"]


def _error(exc):
    err = exc.value.response["Error"]
    return err["Code"], err["Message"]


def _status(exc):
    return exc.value.response["ResponseMetadata"]["HTTPStatusCode"]


def _provider_ids(ec2, inst_id):
    inst = ec2.describe_verified_access_instances(VerifiedAccessInstanceIds=[inst_id])["VerifiedAccessInstances"][0]
    return [p["VerifiedAccessTrustProviderId"] for p in inst["VerifiedAccessTrustProviders"]]


def _cleanup(ec2, groups=(), instances=(), providers=()):
    for group_id in groups:
        ec2.delete_verified_access_group(VerifiedAccessGroupId=group_id)
    for inst_id in instances:
        for tp_id in _provider_ids(ec2, inst_id):
            ec2.detach_verified_access_trust_provider(VerifiedAccessInstanceId=inst_id, VerifiedAccessTrustProviderId=tp_id)
        ec2.delete_verified_access_instance(VerifiedAccessInstanceId=inst_id)
    for tp_id in providers:
        ec2.delete_verified_access_trust_provider(VerifiedAccessTrustProviderId=tp_id)


# ── Trust providers ────────────────────────────────────────


def test_verified_access_oidc_trust_provider_lifecycle(ec2):
    """W2:create:oidc-idp, W2:describe:oidc-idp, W2:modify:oidc-scope-only, V6:delete:tp, V6:describe:tp-after-delete."""
    tp = _oidc_provider(ec2, Description="corp idp", TagSpecifications=[
        {"ResourceType": "verified-access-trust-provider", "Tags": [{"Key": "team", "Value": "platform"}]}])
    tp_id = tp["VerifiedAccessTrustProviderId"]
    assert tp_id.startswith("vatp-") and len(tp_id) == 22
    assert (tp["TrustProviderType"], tp["UserTrustProviderType"], tp["Description"]) == ("user", "oidc", "corp idp")
    assert tp["OidcOptions"] == {**_OIDC, "ClientSecret": "REDACTED"}
    assert tp["SseSpecification"] == {"CustomerManagedKeyEnabled": False}
    assert tp["Tags"] == [{"Key": "team", "Value": "platform"}]

    described = ec2.describe_verified_access_trust_providers(
        VerifiedAccessTrustProviderIds=[tp_id])["VerifiedAccessTrustProviders"][0]
    assert described["PolicyReferenceName"] == "idp"
    assert described["OidcOptions"]["ClientSecret"] == "REDACTED"

    modified = ec2.modify_verified_access_trust_provider(
        VerifiedAccessTrustProviderId=tp_id, Description="renamed", OidcOptions={"Scope": "openid"},
    )["VerifiedAccessTrustProvider"]
    assert modified["Description"] == "renamed"
    assert modified["OidcOptions"] == {**_OIDC, "Scope": "openid", "ClientSecret": "REDACTED"}
    assert "Tags" not in modified

    deleted = ec2.delete_verified_access_trust_provider(VerifiedAccessTrustProviderId=tp_id)["VerifiedAccessTrustProvider"]
    assert deleted["VerifiedAccessTrustProviderId"] == tp_id
    with pytest.raises(ClientError) as exc:
        ec2.describe_verified_access_trust_providers(VerifiedAccessTrustProviderIds=[tp_id])
    assert _error(exc) == ("InvalidVerifiedAccessTrustProviderId.NotFound", f"VerifiedAccessTrustProvider {tp_id} does not exist")
    with pytest.raises(ClientError) as exc:
        ec2.delete_verified_access_trust_provider(VerifiedAccessTrustProviderId=tp_id)
    assert _error(exc) == ("InvalidVerifiedAccessTrustProviderId.NotFound", f"VerifiedAccessTrustProvider {tp_id} does not exist")


def test_verified_access_device_trust_providers(ec2, raw):
    """V1:create:jamf, V1:create:crowdstrike, W2:create:jumpcloud, W2:create:jumpcloud:tenant-only, W2:modify:device-url-only, V1:modify:url-on-jamf,
    V1:create:crowdstrike-url, W2:create:jamf."""
    jamf = _device_provider(ec2, "jamf")
    assert jamf["DeviceOptions"] == {"TenantId": "tenant-1"}
    assert "UserTrustProviderType" not in jamf and jamf["Description"] == ""
    crowdstrike = _device_provider(ec2, "crowdstrike")
    assert crowdstrike["DeviceOptions"] == {
        "TenantId": "tenant-1", "PublicSigningKeyUrl": "https://assets-public.falcon.crowdstrike.com/zta/jwk.json"}
    # W2:create:jumpcloud:tenant-only: AWS fills jumpcloud's key URL in too.
    default_jc = _device_provider(ec2, "jumpcloud")
    assert default_jc["DeviceOptions"] == {"TenantId": "tenant-1",
                                           "PublicSigningKeyUrl": "https://go.jumpcloud.com/.well-known/jwks.json"}
    ec2.delete_verified_access_trust_provider(VerifiedAccessTrustProviderId=default_jc["VerifiedAccessTrustProviderId"])
    jumpcloud = _device_provider(ec2, "jumpcloud", PublicSigningKeyUrl="https://keys.example.com")
    modified = ec2.modify_verified_access_trust_provider(
        VerifiedAccessTrustProviderId=jumpcloud["VerifiedAccessTrustProviderId"],
        DeviceOptions={"PublicSigningKeyUrl": "https://keys2.example.com"})["VerifiedAccessTrustProvider"]
    assert modified["DeviceOptions"] == {"TenantId": "tenant-1", "PublicSigningKeyUrl": "https://keys2.example.com"}

    with pytest.raises(ClientError) as exc:
        ec2.modify_verified_access_trust_provider(
            VerifiedAccessTrustProviderId=jamf["VerifiedAccessTrustProviderId"],
            DeviceOptions={"PublicSigningKeyUrl": "https://keys2.example.com"})
    assert _error(exc) == ("InvalidParameterValue", "Parameter DeviceOptions is not allowed when DeviceTrustProviderType is jamf")
    for device_type in ("jamf", "crowdstrike"):
        with pytest.raises(ClientError) as exc:
            _device_provider(ec2, device_type, PublicSigningKeyUrl="https://keys.example.com")
        assert _error(exc) == ("InvalidParameterValue",
                               f"Parameter PublicSigningKeyUrl is not allowed when DeviceTrustProviderType is {device_type}")
    _cleanup(ec2, providers=[p["VerifiedAccessTrustProviderId"] for p in (jamf, crowdstrike, jumpcloud)])


_TP_CREATE_ERRORS = [
    # (capture key, request, code, message)
    ("E1:CreateVerifiedAccessTrustProvider:no-params", {}, "MissingParameter",
     "The request must include the following parameters: PolicyReferenceName, TrustProviderType. "
     "Add the required parameters and retry the request."),
    ("W2:invalid:type=robot", {"TrustProviderType": "robot"}, "InvalidParameterValue",
     "The value of trustProviderType you provided is not valid"),
    ("W2:invalid:policy-ref=bad-chars", {"TrustProviderType": "device", "DeviceTrustProviderType": "jamf",
                                         "PolicyReferenceName": "Bad Name!"}, "InvalidParameterValue",
     "Value of parameter PolicyReferenceName must match regular expression [a-zA-Z_][a-zA-Z0-9_]*"),
    ("W2:invalid:user+DeviceTrustProviderType", {"TrustProviderType": "user", "DeviceTrustProviderType": "jamf"},
     "MissingParameter", "Missing UserTrustProviderType parameter required when TrustProviderType is user"),
    ("W2:invalid:user-type=bogus", {"TrustProviderType": "user", "UserTrustProviderType": "bogus"},
     "InvalidParameterValue", "The value of userTrustProviderType you provided is not valid"),
    ("W2:invalid:user+both-types", {"TrustProviderType": "user", "UserTrustProviderType": "oidc",
                                    "DeviceTrustProviderType": "jamf", "OidcOptions": _OIDC},
     "InvalidParameterCombination", "Parameter DeviceTrustProviderType is not allowed when TrustProviderType is user"),
    ("W2:invalid:oidc+DeviceOptions", {"TrustProviderType": "user", "UserTrustProviderType": "oidc", "OidcOptions": _OIDC,
                                       "DeviceOptions": {"TenantId": "t"}},
     "InvalidParameterCombination", "Parameter DeviceOptions is not allowed when TrustProviderType is user"),
    ("W2:invalid:oidc-without-OidcOptions", {"TrustProviderType": "user", "UserTrustProviderType": "oidc"},
     "MissingParameter",
     "Missing OidcOptions or NativeApplicationOidcOptions parameter required when UserTrustProviderType is oidc"),
    ("W2:create:iam-identity-center", {"TrustProviderType": "user", "UserTrustProviderType": "iam-identity-center"},
     "InvalidParameterValue", "IAM Identity Center is not enabled on your organization"),
    ("V1:create:oidc-without-ClientSecret", {"TrustProviderType": "user", "UserTrustProviderType": "oidc",
                                             "OidcOptions": {k: v for k, v in _OIDC.items() if k != "ClientSecret"}},
     "MissingParameter", "Missing required parameter ClientSecret in OidcOptions"),
    ("W2:create:oidc-partial-options", {"TrustProviderType": "user", "UserTrustProviderType": "oidc",
                                        "OidcOptions": {"Issuer": "https://login.example.com"}},
     "MissingParameter", "Missing required parameter AuthorizationEndpoint in OidcOptions"),
    ("W2:create:oidc-http-issuer", {"TrustProviderType": "user", "UserTrustProviderType": "oidc",
                                    "OidcOptions": {**_OIDC, "Issuer": "http://login.example.com"}},
     "InvalidParameterValue", "Parameter Issuer in OidcOptions must be a valid HTTPS URL"),
    ("V1:create:oidc-not-url", {"TrustProviderType": "user", "UserTrustProviderType": "oidc",
                                "OidcOptions": {**_OIDC, "TokenEndpoint": "token"}},
     "InvalidParameterValue", "Parameter TokenEndpoint in OidcOptions must be a valid HTTPS URL"),
    ("W2:invalid:device+UserTrustProviderType", {"TrustProviderType": "device", "UserTrustProviderType": "oidc"},
     "InvalidParameterCombination", "Parameter UserTrustProviderType is not allowed when TrustProviderType is device"),
    ("W2:invalid:device-without-device-type", {"TrustProviderType": "device"}, "MissingParameter",
     "Missing DeviceTrustProviderType parameter required when TrustProviderType is device"),
    ("W2:invalid:device-type=bogus", {"TrustProviderType": "device", "DeviceTrustProviderType": "bogus"},
     "InvalidParameterValue", "The value of deviceTrustProviderType you provided is not valid"),
    ("W2:invalid:device+OidcOptions", {"TrustProviderType": "device", "DeviceTrustProviderType": "jamf", "OidcOptions": _OIDC},
     "InvalidParameterCombination", "Parameter OidcOptions is not allowed when TrustProviderType is device"),
    ("W2:create:jamf:no-options", {"TrustProviderType": "device", "DeviceTrustProviderType": "jamf"}, "MissingParameter",
     "Missing DeviceOptions parameter required when TrustProviderType is device"),
    ("W2:create:jamf:url-only", {"TrustProviderType": "device", "DeviceTrustProviderType": "jamf",
                                 "DeviceOptions": {"PublicSigningKeyUrl": "https://keys.example.com"}},
     "MissingParameter", "Missing required parameter TenantId in DeviceOptions"),
    ("Z1:tp:type-bad+ref-bad", {"TrustProviderType": "robot", "PolicyReferenceName": "Bad Name!"}, "InvalidParameterValue",
     "The value of trustProviderType you provided is not valid"),
    ("Z1:tp:ref-too-long", {"TrustProviderType": "device", "DeviceTrustProviderType": "jamf", "DeviceOptions": {"TenantId": "t"},
                            "PolicyReferenceName": "a" * 33}, "InvalidParameterValue",
     "Length of parameter PolicyReferenceName must be between 1 and 32"),
    ("Z1:tp:oidc-no-options+device-type", {"TrustProviderType": "user", "UserTrustProviderType": "oidc",
                                           "DeviceTrustProviderType": "jamf"}, "MissingParameter",
     "Missing OidcOptions or NativeApplicationOidcOptions parameter required when UserTrustProviderType is oidc"),
    ("Z1:tp:iic+oidc-options", {"TrustProviderType": "user", "UserTrustProviderType": "iam-identity-center",
                                "OidcOptions": _OIDC}, "InvalidParameterCombination",
     "Parameter OidcOptions is not allowed when UserTrustProviderType is iam-identity-center"),
    ("Z1:tp:iic+device-options", {"TrustProviderType": "user", "UserTrustProviderType": "iam-identity-center",
                                  "DeviceOptions": {"TenantId": "t"}}, "InvalidParameterValue",
     "IAM Identity Center is not enabled on your organization"),
    ("Z1:tp:oidc-missing-field+http-issuer", {"TrustProviderType": "user", "UserTrustProviderType": "oidc", "OidcOptions": {
        k: v for k, v in {**_OIDC, "Issuer": "http://a.example.com"}.items() if k != "Scope"}},
     "InvalidParameterValue", "Parameter Issuer in OidcOptions must be a valid HTTPS URL"),
    ("Z1:tp:oidc-http-userinfo", {"TrustProviderType": "user", "UserTrustProviderType": "oidc",
                                  "OidcOptions": {**_OIDC, "UserInfoEndpoint": "http://login.example.com/userinfo"}},
     "InvalidParameterValue", "Parameter UserInfoEndpoint in OidcOptions must be a valid HTTPS URL"),
    ("Z1:tp:oidc-issuer-upper-scheme", {"TrustProviderType": "user", "UserTrustProviderType": "oidc",
                                        "OidcOptions": {**_OIDC, "Issuer": "HTTPS://login.example.com"}},
     "InvalidParameterValue", "Parameter Issuer in OidcOptions must be a valid HTTPS URL"),
    ("Z1:tp:oidc-long-scope", {"TrustProviderType": "user", "UserTrustProviderType": "oidc",
                               "OidcOptions": {**_OIDC, "Scope": "s" * 257}},
     "InvalidParameterValue", "Length of parameter Scope in OidcOptions must be between 1 and 256"),
    ("Z1:tp:device-type-missing+oidc", {"TrustProviderType": "device", "OidcOptions": _OIDC}, "InvalidParameterCombination",
     "Parameter OidcOptions is not allowed when TrustProviderType is device"),
    ("Z1:tp:device-type-bad+oidc", {"TrustProviderType": "device", "DeviceTrustProviderType": "bogus", "OidcOptions": _OIDC},
     "InvalidParameterValue", "The value of deviceTrustProviderType you provided is not valid"),
    ("Z1:tp:jumpcloud-http-url", {"TrustProviderType": "device", "DeviceTrustProviderType": "jumpcloud",
                                  "DeviceOptions": {"TenantId": "t", "PublicSigningKeyUrl": "http://k.example.com"}},
     "InvalidParameterValue", "Parameter PublicSigningKeyUrl must be a valid HTTPS URL"),
    ("Z1:tp:jamf-url+sse-fake-key", {"TrustProviderType": "device", "DeviceTrustProviderType": "jamf",
                                     "DeviceOptions": {"TenantId": "t", "PublicSigningKeyUrl": "https://k.example.com"},
                                     "SseSpecification": {"CustomerManagedKeyEnabled": True, "KmsKeyArn":
                                                          "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000"}},
     "InvalidParameterValue", "Parameter PublicSigningKeyUrl is not allowed when DeviceTrustProviderType is jamf"),
    ("Z1:tp:device-sse-enabled-no-key", {"TrustProviderType": "device", "DeviceTrustProviderType": "jamf",
                                         "DeviceOptions": {"TenantId": "t"}, "SseSpecification": {"CustomerManagedKeyEnabled": True}},
     "MissingParameter", "Invalid value for parameter KmsKeyArn"),
]


@pytest.mark.parametrize("key, request_params, code, message", _TP_CREATE_ERRORS, ids=[c[0] for c in _TP_CREATE_ERRORS])
def test_verified_access_trust_provider_create_errors(raw, key, request_params, code, message):
    params = dict(request_params)
    if key != "E1:CreateVerifiedAccessTrustProvider:no-params":
        params.setdefault("PolicyReferenceName", "x")
    before = len(raw.describe_verified_access_trust_providers()["VerifiedAccessTrustProviders"])
    with pytest.raises(ClientError) as exc:
        raw.create_verified_access_trust_provider(**params)
    assert (_status(exc), *_error(exc)) == (400, code, message)
    assert len(raw.describe_verified_access_trust_providers()["VerifiedAccessTrustProviders"]) == before


def test_verified_access_trust_provider_modify_errors(ec2):
    """W2:modify:device-options-on-oidc, V1:modify:oidc-options-on-device, V1:modify:oidc-http-issuer."""
    oidc = _oidc_provider(ec2)["VerifiedAccessTrustProviderId"]
    device = _device_provider(ec2, "jumpcloud")["VerifiedAccessTrustProviderId"]
    for tp_id, kwargs, message in (
        (oidc, {"DeviceOptions": {"PublicSigningKeyUrl": "https://k.example.com"}},
         "Parameter DeviceOptions is not allowed when TrustProviderType is user"),
        (device, {"OidcOptions": {"Scope": "openid"}}, "Parameter OidcOptions is not allowed when TrustProviderType is device"),
        (oidc, {"OidcOptions": {"Issuer": "http://x.example.com"}}, "Parameter Issuer in OidcOptions must be a valid HTTPS URL"),
    ):
        with pytest.raises(ClientError) as exc:
            ec2.modify_verified_access_trust_provider(VerifiedAccessTrustProviderId=tp_id, **kwargs)
        assert _error(exc) == ("InvalidParameterValue", message)
    assert ec2.describe_verified_access_trust_providers(
        VerifiedAccessTrustProviderIds=[oidc])["VerifiedAccessTrustProviders"][0]["OidcOptions"]["Issuer"] == _OIDC["Issuer"]
    _cleanup(ec2, providers=[oidc, device])


# ── Instances and attachments ──────────────────────────────


def test_verified_access_instance_attach_detach(ec2):
    """V1:attach:jamf-to-second-instance and W3:attach:oidc-to-second-instance name the requested instance; W3:create:instance, V1:attach:*, V1:describe:after-attaches, W3:attach:oidc (response), V1:detach:crowdstrike,
    V1:detach:crowdstrike-again, V1:attach:jamf-to-second-instance, W3:attach:second-oidc, V1:delete-tp:attached-device,
    V1:delete:instance-with-provider-no-group, W8:delete:instance."""
    inst_id = _instance(ec2, Description="platform", TagSpecifications=[
        {"ResourceType": "verified-access-instance", "Tags": [{"Key": "Name", "Value": "va"}]}])
    created = ec2.describe_verified_access_instances(VerifiedAccessInstanceIds=[inst_id])["VerifiedAccessInstances"][0]
    assert inst_id.startswith("vai-") and len(inst_id) == 21
    assert (created["FipsEnabled"], created["VerifiedAccessTrustProviders"]) == (False, [])
    jamf = _device_provider(ec2, "jamf")["VerifiedAccessTrustProviderId"]
    oidc = _oidc_provider(ec2, Description="corp")["VerifiedAccessTrustProviderId"]
    crowdstrike = _device_provider(ec2, "crowdstrike")["VerifiedAccessTrustProviderId"]
    second_oidc = _oidc_provider(ec2, ref="idp2")["VerifiedAccessTrustProviderId"]
    for tp_id in (jamf, oidc, crowdstrike):
        attached = ec2.attach_verified_access_trust_provider(
            VerifiedAccessInstanceId=inst_id, VerifiedAccessTrustProviderId=tp_id)
    assert attached["VerifiedAccessTrustProvider"]["VerifiedAccessTrustProviderId"] == crowdstrike
    # The attach response lists the providers without their descriptions; Describe lists them.
    assert [p.get("Description") for p in attached["VerifiedAccessInstance"]["VerifiedAccessTrustProviders"]] == [None] * 3
    described = ec2.describe_verified_access_instances(VerifiedAccessInstanceIds=[inst_id])["VerifiedAccessInstances"][0]
    assert [(p["VerifiedAccessTrustProviderId"], p.get("Description")) for p in described["VerifiedAccessTrustProviders"]] == [
        (jamf, None), (oidc, "corp"), (crowdstrike, None)]

    other = _instance(ec2)
    with pytest.raises(ClientError) as exc:
        _attach(ec2, other, jamf)
    assert _error(exc) == ("VerifiedAccessTrustProviderAttachmentLimitExceeded",
                           f"VerifiedAccessTrustProvider {jamf} is already attached to VerifiedAccessInstance {other}")
    with pytest.raises(ClientError) as exc:
        _attach(ec2, inst_id, second_oidc)
    assert _error(exc) == ("VerifiedAccessTrustProviderAttachmentLimitExceeded",
                           "Cannot have more than 1 VerifiedAccessTrustProvider with TrustProviderType user "
                           "attached to a VerifiedAccessInstance")
    with pytest.raises(ClientError) as exc:
        ec2.delete_verified_access_trust_provider(VerifiedAccessTrustProviderId=jamf)
    assert _error(exc) == ("InvalidParameterValue", "VerifiedAccessTrustProvider has existing attachments. "
                           "Please remove all VerifiedAccessTrustProvider attachments before deleting.")
    with pytest.raises(ClientError) as exc:
        ec2.delete_verified_access_instance(VerifiedAccessInstanceId=inst_id)
    assert _error(exc) == ("InvalidParameterValue", f"{inst_id} has existing attachments. "
                           "Remove all VerifiedAccessTrustProvider attachments before deleting.")

    ec2.detach_verified_access_trust_provider(VerifiedAccessInstanceId=inst_id, VerifiedAccessTrustProviderId=oidc)
    assert _provider_ids(ec2, inst_id) == [jamf, crowdstrike]
    with pytest.raises(ClientError) as exc:
        ec2.detach_verified_access_trust_provider(VerifiedAccessInstanceId=inst_id, VerifiedAccessTrustProviderId=oidc)
    assert _error(exc) == ("InvalidParameterValue", f"Trust provider {oidc} is not attached to instance {inst_id}")

    ec2.modify_verified_access_instance(VerifiedAccessInstanceId=inst_id, Description="changed")
    assert ec2.describe_verified_access_instances(
        VerifiedAccessInstanceIds=[inst_id])["VerifiedAccessInstances"][0]["Description"] == "changed"
    _cleanup(ec2, instances=[inst_id, other], providers=[jamf, oidc, crowdstrike, second_oidc])
    with pytest.raises(ClientError) as exc:
        ec2.delete_verified_access_instance(VerifiedAccessInstanceId=inst_id)
    assert _error(exc) == ("InvalidVerifiedAccessInstanceId.NotFound", f"The verified-access-instance ID '{inst_id}' does not exist")


def test_verified_access_attachment_id_checks(ec2):
    """W1:attach_*, W1:detach_*, V1:attach:malformed-*, V1:detach:malformed-*, W3:attach:*-missing-*, W3:detach:*-missing-*:
    a well-formed id is looked up (instance first); a malformed one is reported only when no lookup failed."""
    inst_id, tp_id = _instance_with_provider(ec2)
    a, d = ec2.attach_verified_access_trust_provider, ec2.detach_verified_access_trust_provider
    for call, inst, tp, code, message in (
        (a, _MISSING["Instance"], _MISSING["TrustProvider"], "InvalidVerifiedAccessInstanceId.NotFound",
         f"The verified-access-instance ID '{_MISSING['Instance']}' does not exist"),
        (a, "abc", _MISSING["TrustProvider"], "InvalidVerifiedAccessTrustProviderId.NotFound",
         f"The verified-access-trust-provider ID '{_MISSING['TrustProvider']}' does not exist"),
        (a, "abc", tp_id, "InvalidParameterValue", "Length of parameter VerifiedAccessInstanceId must be between 5 and 21"),
        (a, inst_id, "abc", "InvalidParameterValue", "Length of parameter VerifiedAccessTrustProviderId must be between 6 and 22"),
        (d, "abc", _MISSING["TrustProvider"], "InvalidVerifiedAccessTrustProviderId.NotFound",
         f"VerifiedAccessTrustProvider {_MISSING['TrustProvider']} does not exist"),
        (d, _MISSING["Instance"], tp_id, "InvalidVerifiedAccessInstanceId.NotFound",
         f"The verified-access-instance ID '{_MISSING['Instance']}' does not exist"),
    ):
        with pytest.raises(ClientError) as exc:
            call(VerifiedAccessInstanceId=inst, VerifiedAccessTrustProviderId=tp)
        assert _error(exc) == (code, message)
    _cleanup(ec2, instances=[inst_id], providers=[tp_id])


def test_verified_access_user_provider_is_locked_while_groups_exist(ec2):
    """V3:detach:with-group, V3:detach:device-with-group, W5:create:group-no-policy, V3:delete-instance:with-groups."""
    inst_id, oidc = _instance_with_provider(ec2)
    jumpcloud = _device_provider(ec2, "jumpcloud")["VerifiedAccessTrustProviderId"]
    _attach(ec2, inst_id, jumpcloud)
    group_id = _group(ec2, inst_id)
    with pytest.raises(ClientError) as exc:
        ec2.detach_verified_access_trust_provider(VerifiedAccessInstanceId=inst_id, VerifiedAccessTrustProviderId=oidc)
    assert _error(exc) == ("VerifiedAccessGroupAttachmentExists",
                           f"VerifiedAccessInstance {inst_id} is not empty. Please remove all VerifiedAccessGroups "
                           "before attaching or detaching a VerifiedAccessTrustProvider with TrustProviderType user")
    ec2.detach_verified_access_trust_provider(VerifiedAccessInstanceId=inst_id, VerifiedAccessTrustProviderId=jumpcloud)
    with pytest.raises(ClientError) as exc:
        ec2.delete_verified_access_instance(VerifiedAccessInstanceId=inst_id)
    assert _error(exc)[1] == (f"{inst_id} has existing attachments. "
                              "Remove all VerifiedAccessTrustProvider attachments before deleting.")

    bare = _instance(ec2)
    with pytest.raises(ClientError) as exc:
        _group(ec2, bare)
    assert _error(exc) == ("InvalidParameterValue", f"Must attach a TrustProvider to Instance {bare} before you can create a Group")
    _cleanup(ec2, groups=[group_id], instances=[inst_id, bare], providers=[oidc, jumpcloud])


def test_verified_access_round6_check_orders(ec2):
    """Z2:create:group, Z2:attach:tp-inner-upper, Z2:attach:tp-upper-prefix, Z2:detach:tp-inner-upper, Z2:attach:both-malformed,
    Z2:modify-group:group-abc+instance-fake, -group-fake+instance-abc, -group-abc+instance-real, Z2:modify-tp:oidc-device-options+bad-issuer,
    Z2:create-group:no-provider+sse-fake."""
    fake_key = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000"
    device_only = _instance(ec2)
    jamf = _device_provider(ec2, "jamf")["VerifiedAccessTrustProviderId"]
    _attach(ec2, device_only, jamf)
    with pytest.raises(ClientError) as exc:
        _group(ec2, device_only)
    assert _error(exc) == ("InvalidParameterValue",
                           f"Must attach a 'user' TrustProvider to Instance {device_only} before you can create a Group")
    bare = _instance(ec2)
    with pytest.raises(ClientError) as exc:
        _group(ec2, bare, SseSpecification={"CustomerManagedKeyEnabled": True, "KmsKeyArn": fake_key})
    assert _error(exc) == ("InvalidParameterValue", f"Could not find the following key: {fake_key}")

    inst_id, oidc = _instance_with_provider(ec2)
    group_id = _group(ec2, inst_id)
    a, d = ec2.attach_verified_access_trust_provider, ec2.detach_verified_access_trust_provider
    for call, tp, error in (
        (a, "vatp-0000000A000000000", ("InvalidVerifiedAccessTrustProviderId.NotFound",
                                       "The verified-access-trust-provider ID 'vatp-0000000A000000000' does not exist")),
        (a, "VATP-00000000000000000", ("InvalidParameterValue",
                                       "Value of parameter VerifiedAccessTrustProviderId must match regular expression [a-z0-9-]+")),
        (d, "vatp-0000000A000000000", ("InvalidParameterValue",
                                       "Value of parameter VerifiedAccessTrustProviderId must match regular expression [a-z0-9-]+")),
    ):
        with pytest.raises(ClientError) as exc:
            call(VerifiedAccessInstanceId=inst_id, VerifiedAccessTrustProviderId=tp)
        assert _error(exc) == error
    with pytest.raises(ClientError) as exc:
        a(VerifiedAccessInstanceId="abc", VerifiedAccessTrustProviderId="abc")
    assert _error(exc)[1] == "Length of parameter VerifiedAccessTrustProviderId must be between 6 and 22"
    other_oidc = _oidc_provider(ec2, ref="idp2")["VerifiedAccessTrustProviderId"]

    mg = ec2.modify_verified_access_group
    for group, inst, error in (
        ("abc", _MISSING["Instance"], ("InvalidVerifiedAccessInstanceId.NotFound",
                                       f"The verified-access-instance ID '{_MISSING['Instance']}' does not exist")),
        (_MISSING["Group"], "abc", ("InvalidVerifiedAccessGroupId.NotFound",
                                    f"The verified-access-group ID '{_MISSING['Group']}' does not exist")),
        ("abc", inst_id, ("InvalidParameterValue", "Length of parameter VerifiedAccessGroupId must be between 6 and 22")),
    ):
        with pytest.raises(ClientError) as exc:
            mg(VerifiedAccessGroupId=group, VerifiedAccessInstanceId=inst)
        assert _error(exc) == error
    with pytest.raises(ClientError) as exc:
        ec2.modify_verified_access_trust_provider(VerifiedAccessTrustProviderId=oidc, OidcOptions={"Issuer": "http://x.example.com"},
                                                  DeviceOptions={"PublicSigningKeyUrl": "https://k.example.com"})
    assert _error(exc) == ("InvalidParameterValue", "Parameter Issuer in OidcOptions must be a valid HTTPS URL")
    _cleanup(ec2, groups=[group_id], instances=[device_only, bare, inst_id], providers=[jamf, oidc, other_oidc])


def test_verified_access_round8_orders_and_orphans(ec2, raw):
    """R1:*, R2:*, R3:* (round 8)."""
    inst_id, oidc = _instance_with_provider(ec2)
    group_id = _group(ec2, inst_id)
    other, other_oidc = _instance_with_provider(ec2)
    spare = _oidc_provider(ec2, ref="idp3")["VerifiedAccessTrustProviderId"]
    locked = ("VerifiedAccessGroupAttachmentExists",
              f"VerifiedAccessInstance {inst_id} is not empty. Please remove all VerifiedAccessGroups before attaching "
              "or detaching a VerifiedAccessTrustProvider with TrustProviderType user")
    for call, tp in ((ec2.attach_verified_access_trust_provider, other_oidc),
                     (ec2.attach_verified_access_trust_provider, spare),
                     (ec2.detach_verified_access_trust_provider, spare)):
        with pytest.raises(ClientError) as exc:
            call(VerifiedAccessInstanceId=inst_id, VerifiedAccessTrustProviderId=tp)
        assert _error(exc) == locked
    with pytest.raises(ClientError) as exc:
        ec2.detach_verified_access_trust_provider(VerifiedAccessInstanceId="abc", VerifiedAccessTrustProviderId="abc")
    assert _error(exc)[1] == "Length of parameter VerifiedAccessTrustProviderId must be between 6 and 22"
    for group, inst, message in (
        (group_id, "abc", "Length of parameter VerifiedAccessInstanceId must be between 5 and 21"),
        (group_id, "VAI-00000000000000000", "Value of parameter VerifiedAccessInstanceId must match regular expression [a-z0-9-]+"),
        ("abc", "abc", "Length of parameter VerifiedAccessGroupId must be between 6 and 22"),
    ):
        with pytest.raises(ClientError) as exc:
            ec2.modify_verified_access_group(VerifiedAccessGroupId=group, VerifiedAccessInstanceId=inst)
        assert _error(exc) == ("InvalidParameterValue", message)
    for op, member, value in (("describe_verified_access_groups", "VerifiedAccessInstanceId", "vai-0000000A000000000"),
                              ("describe_verified_access_endpoints", "VerifiedAccessGroupId", "vagr-0000000A000000000")):
        with pytest.raises(ClientError) as exc:
            getattr(ec2, op)(**{member: value})
        assert _error(exc)[1] == f"Value of parameter {member} must match regular expression [a-z0-9-]+"

    # An instance holding a moved-in group and no provider can be deleted; the group stays.
    bare = _instance(ec2)
    ec2.modify_verified_access_group(VerifiedAccessGroupId=group_id, VerifiedAccessInstanceId=bare)
    ec2.delete_verified_access_instance(VerifiedAccessInstanceId=bare)
    orphan = ec2.describe_verified_access_groups(VerifiedAccessGroupIds=[group_id])["VerifiedAccessGroups"][0]
    assert orphan["VerifiedAccessInstanceId"] == bare

    with pytest.raises(ClientError) as exc:
        raw.create_verified_access_trust_provider(TrustProviderType="device", UserTrustProviderType="oidc",
                                                  DeviceTrustProviderType="bogus", PolicyReferenceName="x")
    assert _error(exc)[1] == "The value of deviceTrustProviderType you provided is not valid"
    jumpcloud = _device_provider(ec2, "jumpcloud", PublicSigningKeyUrl="https://k.example.com")["VerifiedAccessTrustProviderId"]
    with pytest.raises(ClientError) as exc:
        ec2.modify_verified_access_trust_provider(VerifiedAccessTrustProviderId=jumpcloud, DeviceOptions={"PublicSigningKeyUrl": "keys"})
    assert _error(exc) == ("InvalidParameterValue", "Parameter PublicSigningKeyUrl must be a valid HTTPS URL")

    key = make_client("kms").create_key()["KeyMetadata"]
    make_client("kms").create_alias(AliasName=f"alias/va-{uuid.uuid4().hex[:8]}", TargetKeyId=key["KeyId"])
    sse = {"CustomerManagedKeyEnabled": True, "KmsKeyArn": key["Arn"]}
    keyed = ec2.modify_verified_access_trust_provider(VerifiedAccessTrustProviderId=jumpcloud, SseSpecification=sse)
    assert keyed["VerifiedAccessTrustProvider"]["SseSpecification"] == sse
    keyed_group = _group(ec2, other, SseSpecification=sse)
    for arn, error in (
        ("arn:aws:kms:us-east-1:000000000000:alias/x", ("InvalidParameterValue", "Invalid value for parameter KmsKeyArn")),
        (key["Arn"].replace("000000000000", "123456789012"),
         ("UnauthorizedOperation", "User does not have permission to invoke required action: kms:DescribeKey")),
        (key["Arn"].replace("us-east-1", "us-west-2"),
         ("InvalidParameterValue", f"Could not find the following key: {key['Arn'].replace('us-east-1', 'us-west-2')}")),
    ):
        with pytest.raises(ClientError) as exc:
            ec2.modify_verified_access_group_policy(VerifiedAccessGroupId=keyed_group,
                                                    SseSpecification={"CustomerManagedKeyEnabled": True, "KmsKeyArn": arn})
        assert _error(exc) == error

    before = ec2.describe_verified_access_instances(VerifiedAccessInstanceIds=[other])["VerifiedAccessInstances"][0]["LastUpdatedTime"]
    import time
    time.sleep(1.1)
    ec2.modify_verified_access_instance_logging_configuration(VerifiedAccessInstanceId=other, AccessLogs={"IncludeTrustContext": True})
    after = ec2.describe_verified_access_instances(VerifiedAccessInstanceIds=[other])["VerifiedAccessInstances"][0]["LastUpdatedTime"]
    assert after > before
    _cleanup(ec2, groups=[group_id, keyed_group], instances=[inst_id, other],
             providers=[oidc, other_oidc, spare, jumpcloud])


def test_verified_access_response_times_and_ids(ec2):
    """V3:move:to-instance-without-provider, V3:describe:after-move, W5:modify:description, W2:modify:oidc-scope-only,
    Q3:create:wild-one-level and Q5:create:same-domain-while-deleting (ids start with 0)."""
    import time

    inst_id, tp_id = _instance_with_provider(ec2)
    assert inst_id[4] == "0" and tp_id[5] == "0"
    group_id = _group(ec2, inst_id)
    created = ec2.describe_verified_access_groups(VerifiedAccessGroupIds=[group_id])["VerifiedAccessGroups"][0]["LastUpdatedTime"]
    time.sleep(1.1)
    target = _instance(ec2)
    moved = ec2.modify_verified_access_group(VerifiedAccessGroupId=group_id, VerifiedAccessInstanceId=target)["VerifiedAccessGroup"]
    assert moved["LastUpdatedTime"] == created
    after = ec2.describe_verified_access_groups(VerifiedAccessGroupIds=[group_id])["VerifiedAccessGroups"][0]["LastUpdatedTime"]
    assert after > created
    time.sleep(1.1)
    renamed = ec2.modify_verified_access_group(VerifiedAccessGroupId=group_id, Description="x")["VerifiedAccessGroup"]
    assert renamed["LastUpdatedTime"] > after
    tp_before = ec2.describe_verified_access_trust_providers(
        VerifiedAccessTrustProviderIds=[tp_id])["VerifiedAccessTrustProviders"][0]["LastUpdatedTime"]
    time.sleep(1.1)
    modified = ec2.modify_verified_access_trust_provider(VerifiedAccessTrustProviderId=tp_id, Description="y")
    assert modified["VerifiedAccessTrustProvider"]["LastUpdatedTime"] == tp_before
    _cleanup(ec2, groups=[group_id], instances=[inst_id, target], providers=[tp_id])


def test_verified_access_round9_kms_and_orders(ec2):
    """S1:*, S2:* (round 9): KMS key state and region, SSE first on a provider modify."""
    kms, kms_west = make_client("kms"), _client("us-west-2", service="kms")
    inst_id, oidc = _instance_with_provider(ec2)
    group_id = _group(ec2, inst_id)
    disabled = kms.create_key()["KeyMetadata"]["Arn"]
    kms.disable_key(KeyId=disabled)
    pending = kms.create_key()["KeyMetadata"]["Arn"]
    kms.schedule_key_deletion(KeyId=pending, PendingWindowInDays=7)
    west = kms_west.create_key()["KeyMetadata"]["Arn"]
    for arn, error in ((disabled, ("InvalidParameterValue", f"The provided KMS key {disabled} is disabled")),
                       (pending, ("InvalidParameterValue", f"The provided KMS key {pending} is disabled")),
                       (west, ("InvalidParameterValue", f"Could not find the following key: {west}")),
                       ("arn:aws:kms:us-east-1:123456789012:alias/x",
                        ("UnauthorizedOperation", "User does not have permission to invoke required action: kms:DescribeKey"))):
        with pytest.raises(ClientError) as exc:
            ec2.modify_verified_access_group_policy(VerifiedAccessGroupId=group_id,
                                                    SseSpecification={"CustomerManagedKeyEnabled": True, "KmsKeyArn": arn})
        assert _error(exc) == error
    fake_key = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000"
    jumpcloud = _device_provider(ec2, "jumpcloud", PublicSigningKeyUrl="https://k.example.com")["VerifiedAccessTrustProviderId"]
    for tp_id, kwargs in ((jumpcloud, {"DeviceOptions": {"PublicSigningKeyUrl": "http://k.example.com"}}),
                          (oidc, {"OidcOptions": {"Issuer": "http://x.example.com"}})):
        with pytest.raises(ClientError) as exc:
            ec2.modify_verified_access_trust_provider(VerifiedAccessTrustProviderId=tp_id, **kwargs,
                                                      SseSpecification={"CustomerManagedKeyEnabled": True, "KmsKeyArn": fake_key})
        assert _error(exc) == ("InvalidParameterValue", f"Could not find the following key: {fake_key}")
    _cleanup(ec2, groups=[group_id], instances=[inst_id], providers=[oidc, jumpcloud])


def test_verified_access_instance_refuses_cidr_subdomain(ec2):
    """MiniStack scope: cidr endpoints are not emulated, so the custom subdomain is refused and nothing is stored."""
    before = len(ec2.describe_verified_access_instances()["VerifiedAccessInstances"])
    with pytest.raises(ClientError) as exc:
        ec2.create_verified_access_instance(CidrEndpointsCustomSubDomain="corp.example.com")
    assert _error(exc)[0] == "UnsupportedOperation"
    assert len(ec2.describe_verified_access_instances()["VerifiedAccessInstances"]) == before


# ── Logging ────────────────────────────────────────────────


def test_verified_access_logging_describe_lists_next_token_first(ec2):
    """V4:logging:all-MaxResults-1 (round 4): the response lists nextToken before loggingConfigurationSet."""
    import urllib.request

    instances = [_instance(ec2), _instance(ec2)]
    req = urllib.request.Request(
        ENDPOINT, data=b"Action=DescribeVerifiedAccessInstanceLoggingConfigurations&Version=2016-11-15&MaxResults=1",
        method="POST", headers={"Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
                                "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/ec2/aws4_request"})
    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode()
    assert -1 < body.index("<nextToken>") < body.index("<loggingConfigurationSet>")
    _cleanup(ec2, instances=instances)


def test_verified_access_logging_configuration(ec2):
    """W4:describe:default, W4:log-version:*, W4:include-trust-context, V2:log:*, W4:s3+cw-enabled, W4:s3-only-in-request,
    W4:s3-missing-bucket, W4:cw-missing-group, W4:firehose-missing, R3:logging:*."""
    inst_id = _instance(ec2)
    modify = ec2.modify_verified_access_instance_logging_configuration
    default = ec2.describe_verified_access_instance_logging_configurations(
        VerifiedAccessInstanceIds=[inst_id])["LoggingConfigurations"][0]
    assert default == {"VerifiedAccessInstanceId": inst_id, "AccessLogs": {
        "S3": {"Enabled": False}, "CloudWatchLogs": {"Enabled": False}, "KinesisDataFirehose": {"Enabled": False},
        "LogVersion": "ocsf-1.0.0-rc.2", "IncludeTrustContext": False}}

    logs = modify(VerifiedAccessInstanceId=inst_id, AccessLogs={"LogVersion": "OCSF-0.1"})["LoggingConfiguration"]["AccessLogs"]
    assert logs["LogVersion"] == "ocsf-0.1"
    # A request without LogVersion sets it back to the default.
    logs = modify(VerifiedAccessInstanceId=inst_id, AccessLogs={"IncludeTrustContext": True})["LoggingConfiguration"]["AccessLogs"]
    assert (logs["LogVersion"], logs["IncludeTrustContext"]) == ("ocsf-1.0.0-rc.2", True)
    for access_logs, code, message in (
        ({"LogVersion": "ocsf-1.0.0"}, "InvalidParameterValue", "Invalid value for parameter LogVersion"),
        ({"LogVersion": "ocsf-0.1", "IncludeTrustContext": True}, "InvalidParameterCombination",
         "The LogVersion ocsf-0.1 is not compatible with IncludeTrustContext true"),
        ({"S3": {"Enabled": False, "BucketName": "b"}}, "InvalidParameterCombination",
         "The parameter AccessLogs.S3.BucketName cannot be used when AccessLogs.S3.Enabled is false"),
        ({"CloudWatchLogs": {"Enabled": False, "LogGroup": "/x"}}, "InvalidParameterCombination",
         "The parameter AccessLogs.CloudWatchLogs.LogGroup cannot be used when AccessLogs.CloudWatchLogs.Enabled is false"),
        ({"S3": {"Enabled": True}, "LogVersion": "ocsf-9"}, "MissingParameter", "Missing required parameter AccessLogs.S3.BucketName"),
        ({"KinesisDataFirehose": {"Enabled": True}}, "MissingParameter",
         "Missing required parameter AccessLogs.KinesisDataFirehose.DeliveryStream"),
        ({"S3": {"Enabled": True, "BucketName": f"missing-{uuid.uuid4().hex[:8]}"},
          "CloudWatchLogs": {"Enabled": True, "LogGroup": "/missing"}},
         "LogDestinationNotFound", "Log destination resource identified by AccessLogs.S3.BucketName was not found"),
        ({"CloudWatchLogs": {"Enabled": True, "LogGroup": "/missing"}}, "LogDestinationNotFound",
         "Log destination resource identified by AccessLogs.CloudWatchLogs.LogGroup was not found"),
    ):
        with pytest.raises(ClientError) as exc:
            modify(VerifiedAccessInstanceId=inst_id, AccessLogs=access_logs)
        assert _error(exc) == (code, message), access_logs

    bucket, group = f"va-logs-{uuid.uuid4().hex[:8]}", f"/va/{uuid.uuid4().hex[:8]}"
    make_client("s3").create_bucket(Bucket=bucket)
    make_client("logs").create_log_group(logGroupName=group)
    logs = modify(VerifiedAccessInstanceId=inst_id, AccessLogs={
        "S3": {"Enabled": True, "BucketName": bucket, "Prefix": "va"},
        "CloudWatchLogs": {"Enabled": True, "LogGroup": group}})["LoggingConfiguration"]["AccessLogs"]
    assert logs["S3"] == {"Enabled": True, "BucketName": bucket, "Prefix": "va", "BucketOwner": "000000000000",
                          "DeliveryStatus": {"Code": "success"}}
    assert logs["CloudWatchLogs"] == {"Enabled": True, "LogGroup": group, "DeliveryStatus": {"Code": "success"}}
    # Destinations a request leaves out are kept.
    logs = modify(VerifiedAccessInstanceId=inst_id, AccessLogs={"S3": {"Enabled": False}})["LoggingConfiguration"]["AccessLogs"]
    assert logs["S3"] == {"Enabled": False}
    assert logs["CloudWatchLogs"]["Enabled"] is True
    modify(VerifiedAccessInstanceId=inst_id, AccessLogs={"CloudWatchLogs": {"Enabled": False}})
    # R3:logging:firehose, R3:logging:describe-firehose.
    stream = f"va-{uuid.uuid4().hex[:8]}"
    make_client("firehose").create_delivery_stream(
        DeliveryStreamName=stream, DeliveryStreamType="DirectPut",
        S3DestinationConfiguration={"RoleARN": "arn:aws:iam::000000000000:role/fh", "BucketARN": f"arn:aws:s3:::{bucket}"})
    logs = modify(VerifiedAccessInstanceId=inst_id, AccessLogs={
        "KinesisDataFirehose": {"Enabled": True, "DeliveryStream": stream}})["LoggingConfiguration"]["AccessLogs"]
    assert logs["KinesisDataFirehose"] == {"Enabled": True, "DeliveryStream": stream, "DeliveryStatus": {"Code": "success"}}
    modify(VerifiedAccessInstanceId=inst_id, AccessLogs={"KinesisDataFirehose": {"Enabled": False}})
    ec2.delete_verified_access_instance(VerifiedAccessInstanceId=inst_id)


# ── Groups and policies ────────────────────────────────────


def test_verified_access_group_lifecycle(ec2):
    """W5:create:group, W5:describe:by-instance, W5:get-policy:new, V3:get-policy:none, W5:modify:description,
    V3:move:to-instance-without-provider, V6:delete:group, V6:describe:group-after-delete, V6:delete:group-again."""
    inst_id, tp_id = _instance_with_provider(ec2)
    group = ec2.create_verified_access_group(VerifiedAccessInstanceId=inst_id, Description="apps", PolicyDocument=_POLICY,
                                             TagSpecifications=[{"ResourceType": "verified-access-group",
                                                                 "Tags": [{"Key": "team", "Value": "a"}]}])["VerifiedAccessGroup"]
    group_id = group["VerifiedAccessGroupId"]
    assert group_id.startswith("vagr-") and len(group_id) == 22
    assert group["VerifiedAccessGroupArn"] == f"arn:aws:ec2:us-east-1:000000000000:verified-access-group/{group_id}"
    assert (group["Owner"], group["SseSpecification"]) == ("000000000000", {"CustomerManagedKeyEnabled": False})
    assert ec2.get_verified_access_group_policy(VerifiedAccessGroupId=group_id) | {"ResponseMetadata": None} == {
        "PolicyEnabled": True, "PolicyDocument": _POLICY, "ResponseMetadata": None}
    listed = ec2.describe_verified_access_groups(VerifiedAccessInstanceId=inst_id)["VerifiedAccessGroups"]
    assert [(g["VerifiedAccessGroupId"], g["Tags"]) for g in listed] == [(group_id, [{"Key": "team", "Value": "a"}])]

    bare_group = _group(ec2, inst_id)
    policy = ec2.get_verified_access_group_policy(VerifiedAccessGroupId=bare_group)
    assert (policy["PolicyEnabled"], "PolicyDocument" in policy) == (False, False)
    modified = ec2.modify_verified_access_group(VerifiedAccessGroupId=bare_group, Description="renamed")["VerifiedAccessGroup"]
    assert modified["Description"] == "renamed" and "Tags" not in modified
    # A group can move to an instance without a trust provider.
    target = _instance(ec2)
    moved = ec2.modify_verified_access_group(VerifiedAccessGroupId=bare_group, VerifiedAccessInstanceId=target)
    assert moved["VerifiedAccessGroup"]["VerifiedAccessInstanceId"] == target

    deleted = ec2.delete_verified_access_group(VerifiedAccessGroupId=group_id)["VerifiedAccessGroup"]
    assert deleted["DeletionTime"] and "Tags" not in deleted
    with pytest.raises(ClientError) as exc:
        ec2.describe_verified_access_groups(VerifiedAccessGroupIds=[group_id])
    assert _error(exc) == ("InvalidVerifiedAccessGroupId.NotFound", f"VerifiedAccessGroup {group_id} does not exist")
    with pytest.raises(ClientError) as exc:
        ec2.delete_verified_access_group(VerifiedAccessGroupId=group_id)
    assert _error(exc) == ("InvalidVerifiedAccessGroupId.NotFound", f"The verified-access-group ID '{group_id}' does not exist")
    _cleanup(ec2, groups=[bare_group], instances=[inst_id, target], providers=[tp_id])


def test_verified_access_group_policy_rules(ec2):
    """W5:policy:*, V3:policy:*, W5:policy:sse-fake-key, W5:policy:sse-real-key, V3:policy:sse-key-not-enabled,
    V3:create:group-sse-enabled-no-key, V3:create:group-sse-fake-key."""
    inst_id, tp_id = _instance_with_provider(ec2)
    group_id = _group(ec2, inst_id, PolicyDocument=_POLICY)
    modify = ec2.modify_verified_access_group_policy
    for kwargs, code, message in (
        ({"PolicyEnabled": True}, "MissingParameter", "Policy Document must be provided when Policy Enabled is true"),
        ({"PolicyDocument": _POLICY}, "InvalidParameterValue", "Policy Document cannot be provided when Policy Enabled is false or missing"),
        ({"PolicyEnabled": False, "PolicyDocument": _POLICY}, "InvalidParameterValue",
         "Policy Document cannot be provided when Policy Enabled is false or missing"),
    ):
        with pytest.raises(ClientError) as exc:
            modify(VerifiedAccessGroupId=group_id, **kwargs)
        assert _error(exc) == (code, message)
    unchanged = modify(VerifiedAccessGroupId=group_id)
    assert (unchanged["PolicyEnabled"], unchanged["PolicyDocument"]) == (True, _POLICY)

    fake_key = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000"
    with pytest.raises(ClientError) as exc:
        modify(VerifiedAccessGroupId=group_id, SseSpecification={"CustomerManagedKeyEnabled": True, "KmsKeyArn": fake_key})
    assert _error(exc) == ("InvalidParameterValue", f"Could not find the following key: {fake_key}")
    with pytest.raises(ClientError) as exc:
        _group(ec2, inst_id, SseSpecification={"CustomerManagedKeyEnabled": True})
    assert _error(exc) == ("MissingParameter", "Invalid value for parameter KmsKeyArn")
    key_arn = make_client("kms").create_key()["KeyMetadata"]["Arn"]
    with_key = modify(VerifiedAccessGroupId=group_id, SseSpecification={"CustomerManagedKeyEnabled": True, "KmsKeyArn": key_arn})
    assert with_key["SseSpecification"] == {"CustomerManagedKeyEnabled": True, "KmsKeyArn": key_arn}
    assert (with_key["PolicyEnabled"], with_key["PolicyDocument"]) == (True, _POLICY)
    off = modify(VerifiedAccessGroupId=group_id, SseSpecification={"CustomerManagedKeyEnabled": False, "KmsKeyArn": key_arn})
    assert off["SseSpecification"] == {"CustomerManagedKeyEnabled": False}

    disabled = modify(VerifiedAccessGroupId=group_id, PolicyEnabled=False)
    assert (disabled["PolicyEnabled"], "PolicyDocument" in disabled) == (False, False)
    policy = ec2.get_verified_access_group_policy(VerifiedAccessGroupId=group_id)
    assert (policy["PolicyEnabled"], "PolicyDocument" in policy) == (False, False)
    _cleanup(ec2, groups=[group_id], instances=[inst_id], providers=[tp_id])


# ── Describe and id checks ─────────────────────────────────


_DESCRIBES = {
    "instances": ("describe_verified_access_instances", "VerifiedAccessInstanceIds", "Instance", 5, 200),
    "trust_providers": ("describe_verified_access_trust_providers", "VerifiedAccessTrustProviderIds", "TrustProvider", 5, 1000),
    "groups": ("describe_verified_access_groups", "VerifiedAccessGroupIds", "Group", 5, 200),
    "endpoints": ("describe_verified_access_endpoints", "VerifiedAccessEndpointIds", "Endpoint", 5, 200),
    "logging": ("describe_verified_access_instance_logging_configurations", "VerifiedAccessInstanceIds", "Instance", 1, 10),
}


@pytest.mark.parametrize("name", list(_DESCRIBES))
def test_verified_access_describe_checks(raw, name):
    """A1:*, A12:*, B1:*, B12:*, C1:*, D1:* for each Describe call."""
    op, member, kind, low, high = _DESCRIBES[name]
    call = getattr(raw, op)
    singular = member[:-1]
    lengths = {"Instance": (5, 21), "TrustProvider": (6, 22), "Group": (6, 22), "Endpoint": (5, 21)}[kind]
    missing = _MISSING[kind]
    cases = [
        ({member: [missing]}, f"InvalidVerifiedAccess{kind}Id.NotFound", f"VerifiedAccess{kind} {missing} does not exist"),
        ({member: ["abc"]}, "InvalidParameterValue", f"Length of parameter {singular} must be between {lengths[0]} and {lengths[1]}"),
        ({member: [missing[:4] + "_" + missing[5:]]}, "InvalidParameterValue",
         f"Value of parameter {singular} must match regular expression [a-z0-9-]+"),
        ({member: [missing, "abc"]}, "InvalidParameterValue",
         f"Length of parameter {singular} must be between {lengths[0]} and {lengths[1]}"),
        ({member: ["abc"], "MaxResults": low}, "InvalidParameterCombination",
         f"The parameters {member} and MaxResults cannot be used together"),
        ({member: [missing], "MaxResults": low, "NextToken": "x"}, "InvalidParameterCombination",
         f"The parameters {member} and NextToken cannot be used together"),
        ({"MaxResults": low - 1}, "InvalidParameterValue", f"The parameter MaxResults must be between {low} and {high}"),
        ({"MaxResults": high + 1}, "InvalidParameterValue", f"The parameter MaxResults must be between {low} and {high}"),
        ({"MaxResults": low - 1, "NextToken": "x"}, "InvalidParameterValue", f"The parameter MaxResults must be between {low} and {high}"),
        ({"MaxResults": "abc"}, "InvalidParameterValue", "Invalid integer value abc"),
        ({"NextToken": "not-a-token"}, "InvalidParameterValue", "Next token is invalid"),
    ]
    for params, code, message in cases:
        with pytest.raises(ClientError) as exc:
            call(**params)
        assert (_status(exc), *_error(exc)) == (400, code, message), params
    call(MaxResults=low)
    call(MaxResults=high)
    # Filter names AWS does not know are ignored.
    call(Filters=[{"Name": "bogus", "Values": ["x"]}])


def test_verified_access_describe_member_checks(raw):
    """B1:groups:instance-missing+group-missing, C1:groups:*, C1:endpoints:*, D1:endpoints:*, D1:groups:*."""
    vai, vagr, vae = _MISSING["Instance"], _MISSING["Group"], _MISSING["Endpoint"]
    for op, params, code, message in (
        ("describe_verified_access_groups", {"VerifiedAccessInstanceId": vai, "VerifiedAccessGroupIds": [vagr]},
         "InvalidVerifiedAccessGroupId.NotFound", f"VerifiedAccessGroup {vagr} does not exist"),
        ("describe_verified_access_groups", {"VerifiedAccessInstanceId": "abc", "VerifiedAccessGroupIds": [vagr]},
         "InvalidParameterValue", "Length of parameter VerifiedAccessInstanceId must be between 5 and 21"),
        ("describe_verified_access_groups", {"VerifiedAccessInstanceId": vai, "MaxResults": 0},
         "InvalidParameterValue", "The parameter MaxResults must be between 5 and 200"),
        ("describe_verified_access_groups", {"VerifiedAccessInstanceId": vai, "NextToken": "x"},
         "InvalidParameterValue", "Next token is invalid"),
        ("describe_verified_access_groups", {"VerifiedAccessInstanceId": vai},
         "InvalidVerifiedAccessInstanceId.NotFound", f"VerifiedAccessInstance {vai} does not exist"),
        ("describe_verified_access_endpoints", {"VerifiedAccessInstanceId": vai, "VerifiedAccessGroupId": vagr},
         "InvalidParameterCombination", "The parameters VerifiedAccessInstanceId and VerifiedAccessGroupId cannot be used together"),
        ("describe_verified_access_endpoints", {"VerifiedAccessInstanceId": "abc", "VerifiedAccessGroupId": "abc"},
         "InvalidParameterValue", "Length of parameter VerifiedAccessInstanceId must be between 5 and 21"),
        ("describe_verified_access_endpoints", {"VerifiedAccessInstanceId": vai, "VerifiedAccessGroupId": vagr, "MaxResults": 0},
         "InvalidParameterValue", "The parameter MaxResults must be between 5 and 200"),
        ("describe_verified_access_endpoints", {"VerifiedAccessInstanceId": vai, "VerifiedAccessEndpointIds": [vae]},
         "InvalidVerifiedAccessEndpointId.NotFound", f"VerifiedAccessEndpoint {vae} does not exist"),
        ("describe_verified_access_endpoints", {"VerifiedAccessGroupId": vagr},
         "InvalidVerifiedAccessGroupId.NotFound", f"VerifiedAccessGroup {vagr} does not exist"),
    ):
        with pytest.raises(ClientError) as exc:
            getattr(raw, op)(**params)
        assert _error(exc) == (code, message), params


_REQUIRED = [
    ("delete_verified_access_instance", "VerifiedAccessInstanceId"),
    ("modify_verified_access_instance", "VerifiedAccessInstanceId"),
    ("delete_verified_access_trust_provider", "VerifiedAccessTrustProviderId"),
    ("modify_verified_access_trust_provider", "VerifiedAccessTrustProviderId"),
    ("create_verified_access_group", "VerifiedAccessInstanceId"),
    ("modify_verified_access_group", "VerifiedAccessGroupId"),
    ("delete_verified_access_group", "VerifiedAccessGroupId"),
    ("get_verified_access_group_policy", "VerifiedAccessGroupId"),
    ("modify_verified_access_group_policy", "VerifiedAccessGroupId"),
    ("modify_verified_access_endpoint", "VerifiedAccessEndpointId"),
    ("delete_verified_access_endpoint", "VerifiedAccessEndpointId"),
    ("get_verified_access_endpoint_policy", "VerifiedAccessEndpointId"),
    ("modify_verified_access_endpoint_policy", "VerifiedAccessEndpointId"),
]


@pytest.mark.parametrize("op, member", _REQUIRED)
def test_verified_access_missing_parameter(raw, op, member):
    """E1:<Operation>:no-params."""
    with pytest.raises(ClientError) as exc:
        getattr(raw, op)()
    assert _error(exc) == ("MissingParameter", f"The request must include the {member} parameter. "
                           "Add the required parameter and retry the request.")


def test_verified_access_missing_parameters_listed(raw):
    """E1:AttachVerifiedAccessTrustProvider:*, E1:DetachVerifiedAccessTrustProvider:*, E1:ModifyVerifiedAccessInstanceLoggingConfiguration:*,
    W1:create_verified_access_endpoint:no-params."""
    for call in (raw.attach_verified_access_trust_provider, raw.detach_verified_access_trust_provider):
        with pytest.raises(ClientError) as exc:
            call()
        assert _error(exc)[1] == ("The request must include the following parameters: VerifiedAccessInstanceId, "
                                  "VerifiedAccessTrustProviderId. Add the required parameters and retry the request.")
        with pytest.raises(ClientError) as exc:
            call(VerifiedAccessInstanceId=_MISSING["Instance"])
        assert _error(exc)[1] == ("The request must include the VerifiedAccessTrustProviderId parameter. "
                                  "Add the required parameter and retry the request.")
    with pytest.raises(ClientError) as exc:
        raw.modify_verified_access_instance_logging_configuration()
    assert _error(exc)[1] == ("The request must include the following parameters: AccessLogs, VerifiedAccessInstanceId. "
                              "Add the required parameters and retry the request.")
    with pytest.raises(ClientError) as exc:
        raw.create_verified_access_endpoint(EndpointType="load-balancer")
    assert _error(exc) == ("MissingParameter", "Missing required parameter VerifiedAccessGroupId")


_WRITE_ID_CHECKS = [
    # (operation, member, kind, extra request members, NotFound wording)
    ("modify_verified_access_instance", "VerifiedAccessInstanceId", "Instance", {}, "single"),
    ("delete_verified_access_instance", "VerifiedAccessInstanceId", "Instance", {}, "single"),
    ("modify_verified_access_instance_logging_configuration", "VerifiedAccessInstanceId", "Instance",
     {"AccessLogs": {"IncludeTrustContext": False}}, "single"),
    ("create_verified_access_group", "VerifiedAccessInstanceId", "Instance", {}, "single"),
    ("modify_verified_access_trust_provider", "VerifiedAccessTrustProviderId", "TrustProvider", {}, "listed"),
    ("delete_verified_access_trust_provider", "VerifiedAccessTrustProviderId", "TrustProvider", {}, "listed"),
    ("modify_verified_access_group", "VerifiedAccessGroupId", "Group", {}, "single"),
    ("delete_verified_access_group", "VerifiedAccessGroupId", "Group", {}, "single"),
    ("modify_verified_access_group_policy", "VerifiedAccessGroupId", "Group", {}, "single"),
    ("get_verified_access_group_policy", "VerifiedAccessGroupId", "Group", {}, "single"),
    ("create_verified_access_endpoint", "VerifiedAccessGroupId", "Group",
     {"EndpointType": "network-interface", "AttachmentType": "vpc"}, "single"),
    ("modify_verified_access_endpoint", "VerifiedAccessEndpointId", "Endpoint", {}, "single"),
    ("delete_verified_access_endpoint", "VerifiedAccessEndpointId", "Endpoint", {}, "single"),
    ("modify_verified_access_endpoint_policy", "VerifiedAccessEndpointId", "Endpoint", {}, "single"),
    ("get_verified_access_endpoint_policy", "VerifiedAccessEndpointId", "Endpoint", {}, "single"),
]


@pytest.mark.parametrize("op, member, kind, extra, wording", _WRITE_ID_CHECKS, ids=[c[0] for c in _WRITE_ID_CHECKS])
def test_verified_access_id_checks_on_single_resource_calls(raw, op, member, kind, extra, wording):
    """W1:<op>:missing/abc/upper/inner-upper/underscore, F1:get_verified_access_*_policy:*: trust provider calls
    hold the whole id to the pattern; the other calls only the part before the first hyphen."""
    missing = _MISSING[kind]
    prefix, _, rest = missing.partition("-")
    inner_upper = f"{prefix}-{rest[:7]}A{rest[8:]}"
    pattern_error = ("InvalidParameterValue", f"Value of parameter {member} must match regular expression [a-z0-9-]+")
    rtype = {"Instance": "instance", "TrustProvider": "trust-provider", "Group": "group", "Endpoint": "endpoint"}[kind]

    def not_found(value):
        if wording == "listed":
            return f"InvalidVerifiedAccess{kind}Id.NotFound", f"VerifiedAccess{kind} {value} does not exist"
        return f"InvalidVerifiedAccess{kind}Id.NotFound", f"The verified-access-{rtype} ID '{value}' does not exist"

    lengths = {"Instance": (5, 21), "TrustProvider": (6, 22), "Group": (6, 22), "Endpoint": (5, 21)}[kind]
    expected = {
        missing: not_found(missing),
        "abc": ("InvalidParameterValue", f"Length of parameter {member} must be between {lengths[0]} and {lengths[1]}"),
        f"{prefix.upper()}-{rest}": pattern_error,
        f"{prefix}_{rest}": pattern_error,
        inner_upper: pattern_error if kind == "TrustProvider" else not_found(inner_upper),
    }
    for value, error in expected.items():
        with pytest.raises(ClientError) as exc:
            getattr(raw, op)(**{member: value}, **extra)
        assert _error(exc) == error, value


def test_verified_access_resources_are_regional_and_per_account(ec2):
    """W6:region:instance, W6:region:tp, V3:region:group, V3:region:group-policy, W6:region:modify-instance."""
    west, other = _client("us-west-2"), _client(account_id="123456789012")
    inst_id, tp_id = _instance_with_provider(ec2)
    group_id = _group(ec2, inst_id)
    for client in (west, other):
        for call, kwargs, error in (
            (client.describe_verified_access_instances, {"VerifiedAccessInstanceIds": [inst_id]},
             ("InvalidVerifiedAccessInstanceId.NotFound", f"VerifiedAccessInstance {inst_id} does not exist")),
            (client.describe_verified_access_trust_providers, {"VerifiedAccessTrustProviderIds": [tp_id]},
             ("InvalidVerifiedAccessTrustProviderId.NotFound", f"VerifiedAccessTrustProvider {tp_id} does not exist")),
            (client.describe_verified_access_groups, {"VerifiedAccessGroupIds": [group_id]},
             ("InvalidVerifiedAccessGroupId.NotFound", f"VerifiedAccessGroup {group_id} does not exist")),
            (client.get_verified_access_group_policy, {"VerifiedAccessGroupId": group_id},
             ("InvalidVerifiedAccessGroupId.NotFound", f"The verified-access-group ID '{group_id}' does not exist")),
            (client.modify_verified_access_instance, {"VerifiedAccessInstanceId": inst_id, "Description": "x"},
             ("InvalidVerifiedAccessInstanceId.NotFound", f"The verified-access-instance ID '{inst_id}' does not exist")),
        ):
            with pytest.raises(ClientError) as exc:
                call(**kwargs)
            assert _error(exc) == error
    _cleanup(ec2, groups=[group_id], instances=[inst_id], providers=[tp_id])


def test_verified_access_tags_and_filters(ec2):
    """V4:tags:describe-tags, V4:tags:delete-all, V4:tags:describe-untagged-group, W6:filter:*, V4:filter:*, V4:page:*."""
    marker = uuid.uuid4().hex
    tagged = [ec2.create_verified_access_trust_provider(
        TrustProviderType="device", DeviceTrustProviderType="jumpcloud", PolicyReferenceName="j",
        DeviceOptions={"TenantId": "t"}, TagSpecifications=[{"ResourceType": "verified-access-trust-provider",
                                                             "Tags": [{"Key": "run", "Value": marker}]}],
    )["VerifiedAccessTrustProvider"]["VerifiedAccessTrustProviderId"] for _ in range(7)]
    describe = ec2.describe_verified_access_trust_providers
    by_tag = {tp["VerifiedAccessTrustProviderId"] for tp in describe(Filters=[{"Name": "tag:run", "Values": [marker]}])["VerifiedAccessTrustProviders"]}
    assert by_tag == set(tagged)
    assert describe(Filters=[{"Name": "tag:run", "Values": [marker]}, {"Name": "tag:Owner", "Values": ["nobody"]}])[
        "VerifiedAccessTrustProviders"] == []
    ignored = describe(Filters=[{"Name": "tag:run", "Values": [marker]}, {"Name": "bogus", "Values": ["x"]}])
    assert len(ignored["VerifiedAccessTrustProviders"]) == 7
    paged = [tp["VerifiedAccessTrustProviderId"]
             for page in ec2.get_paginator("describe_verified_access_trust_providers").paginate(
                 Filters=[{"Name": "tag:run", "Values": [marker]}], PaginationConfig={"PageSize": 5})
             for tp in page["VerifiedAccessTrustProviders"]]
    assert sorted(paged) == sorted(tagged)

    inst_id = _instance(ec2)
    ec2.create_tags(Resources=[inst_id, tagged[0]], Tags=[{"Key": "env", "Value": "dev"}])
    tags = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": [inst_id, tagged[0]]}, {"Name": "key", "Values": ["env"]}])["Tags"]
    assert {(t["ResourceId"], t["ResourceType"]) for t in tags} == {
        (inst_id, "verified-access-instance"), (tagged[0], "verified-access-trust-provider")}
    ec2.delete_tags(Resources=[inst_id], Tags=[{"Key": "env"}])
    assert ec2.describe_verified_access_instances(VerifiedAccessInstanceIds=[inst_id])["VerifiedAccessInstances"][0]["Tags"] == []
    _cleanup(ec2, instances=[inst_id], providers=tagged)


# ── Endpoints ──────────────────────────────────────────────


@pytest.fixture
def endpoint_fixture(ec2):
    """An instance with a provider, a group, an internal ALB, an ENI and an issued certificate."""
    subnets = [s["SubnetId"] for s in ec2.describe_subnets()["Subnets"]][:2]
    lb_arn = make_client("elbv2").create_load_balancer(
        Name=f"va-{uuid.uuid4().hex[:10]}", Subnets=subnets, Scheme="internal", Type="application",
    )["LoadBalancers"][0]["LoadBalancerArn"]
    eni = ec2.create_network_interface(SubnetId=subnets[0])["NetworkInterface"]["NetworkInterfaceId"]
    vpc_id = ec2.describe_subnets(SubnetIds=subnets[:1])["Subnets"][0]["VpcId"]
    sg = ec2.create_security_group(GroupName=f"va-{uuid.uuid4().hex[:8]}", Description="va", VpcId=vpc_id)["GroupId"]
    cert = make_client("acm").request_certificate(
        DomainName="app.example.com", SubjectAlternativeNames=["app.example.com", "eni.example.com"])["CertificateArn"]
    inst_id, tp_id = _instance_with_provider(ec2)
    group_id = _group(ec2, inst_id)
    fixture = {"lb": lb_arn, "eni": eni, "sg": sg, "cert": cert, "subnets": subnets, "instance": inst_id, "provider": tp_id,
               "group": group_id}
    yield fixture
    for ep in ec2.describe_verified_access_endpoints(VerifiedAccessInstanceId=inst_id)["VerifiedAccessEndpoints"]:
        if ep["Status"]["Code"] != "deleting":
            ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ep["VerifiedAccessEndpointId"])
    groups = [g["VerifiedAccessGroupId"] for g in ec2.describe_verified_access_groups()["VerifiedAccessGroups"]
              if g["VerifiedAccessInstanceId"] == inst_id]
    _cleanup(ec2, groups=groups, instances=[inst_id], providers=[tp_id])


def _lb_endpoint_params(fx, **over):
    params = dict(VerifiedAccessGroupId=fx["group"], EndpointType="load-balancer", AttachmentType="vpc",
                  DomainCertificateArn=fx["cert"], ApplicationDomain="app.example.com", EndpointDomainPrefix="app",
                  SecurityGroupIds=[fx["sg"]],
                  LoadBalancerOptions={"Protocol": "https", "Port": 443, "LoadBalancerArn": fx["lb"], "SubnetIds": fx["subnets"]})
    params.update(over)
    return {k: v for k, v in params.items() if v is not None}


def test_verified_access_endpoint_create_errors(raw, endpoint_fixture):
    """X1:invalid:*, V5:invalid:*, Y1:invalid:*, W1:create_verified_access_endpoint:*."""
    fx = endpoint_fixture
    lb = {"Protocol": "https", "Port": 443, "LoadBalancerArn": fx["lb"], "SubnetIds": fx["subnets"]}
    eni = {"NetworkInterfaceId": fx["eni"], "Protocol": "https", "Port": 443}
    cases = [
        ({"EndpointType": None}, "MissingParameter", "Required parameter EndpointType was empty or null in the request"),
        ({"EndpointType": None, "AttachmentType": None}, "MissingParameter",
         "Required parameter EndpointType was empty or null in the request"),
        ({"AttachmentType": None}, "MissingParameter", "Required parameter AttachmentType was empty or null in the request"),
        ({"SecurityGroupIds": ["sg-00000000000000000"]}, "InvalidParameterValue", "The Security Group Ids are invalid"),
        ({"LoadBalancerOptions": {**lb, "SubnetIds": ["subnet-00000000000000000"]}}, "InvalidParameterValue",
         "Invalid value for parameter SubnetIds"),
        ({"VerifiedAccessGroupId": _MISSING["Group"], "EndpointType": "bogus"}, "InvalidVerifiedAccessGroupId.NotFound",
         f"The verified-access-group ID '{_MISSING['Group']}' does not exist"),
        ({"EndpointType": "bogus"}, "InvalidParameterValue", "The value of endpointType you provided is not valid"),
        ({"AttachmentType": "bogus"}, "InvalidParameterValue", "The value of attachmentType you provided is not valid"),
        ({"EndpointType": "rds", "LoadBalancerOptions": None}, "MissingParameter", "Missing required parameter RdsOptions"),
        ({"EndpointType": "cidr", "LoadBalancerOptions": None}, "MissingParameter", "Missing required parameter CidrOptions"),
        ({"LoadBalancerOptions": None, "DomainCertificateArn": "arn:c"}, "MissingParameter",
         "LoadBalancerOptions must be provided if endpoint type load-balancer is specified"),
        ({"LoadBalancerOptions": None, "NetworkInterfaceOptions": eni}, "MissingParameter",
         "LoadBalancerOptions must be provided if endpoint type load-balancer is specified"),
        ({"LoadBalancerOptions": {k: v for k, v in lb.items() if k != "LoadBalancerArn"}}, "MissingParameter",
         "LoadBalancerArn must be provided in LoadBalancerOptions"),
        ({"NetworkInterfaceOptions": eni}, "InvalidParameterCombination",
         "NetworkInterfaceOptions cannot be provided if endpoint type load-balancer is specified"),
        ({"LoadBalancerOptions": {**lb, "PortRanges": [{"FromPort": 443, "ToPort": 443}]}}, "InvalidParameterValue",
         "Parameter PortRanges is not currently supported for the operation"),
        ({"DomainCertificateArn": None}, "MissingParameter", "Missing required parameter DomainCertificateArn"),
        ({"ApplicationDomain": None}, "MissingParameter", "Missing required parameter ApplicationDomain"),
        ({"EndpointDomainPrefix": None}, "MissingParameter", "Missing required parameter EndpointDomainPrefix"),
        ({"LoadBalancerOptions": {**lb, "LoadBalancerArn": fx["lb"][:-16] + "0" * 16}}, "InvalidParameterValue",
         "The LoadBalancerArn is invalid"),
        ({"DomainCertificateArn": fx["cert"][:-12] + "0" * 12}, "InvalidParameterValue", "The CertificateArn is invalid"),
        ({"ApplicationDomain": "other.example.com"}, "InvalidParameterValue",
         "The ApplicationDomain provided is not present in the DomainCertificateArn provided in the request."),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None}, "MissingParameter",
         "NetworkInterfaceOptions must be provided if endpoint type network-interface is specified"),
        ({"EndpointType": "network-interface", "NetworkInterfaceOptions": eni}, "InvalidParameterCombination",
         "LoadBalancerOptions cannot be provided if endpoint type network-interface is specified"),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None,
          "NetworkInterfaceOptions": {"Protocol": "https", "Port": 443}}, "MissingParameter",
         "Missing required parameter NetworkInterfaceId"),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None,
          "NetworkInterfaceOptions": {**eni, "NetworkInterfaceId": "eni-00000000000000000"}}, "InvalidParameterValue",
         "NetworkInterfaceId is invalid"),
    ]
    for over, code, message in cases:
        with pytest.raises(ClientError) as exc:
            raw.create_verified_access_endpoint(**_lb_endpoint_params(fx, **over))
        assert (_status(exc), *_error(exc)) == (400, code, message), over
    assert raw.describe_verified_access_endpoints(VerifiedAccessGroupId=fx["group"])["VerifiedAccessEndpoints"] == []


def test_verified_access_endpoint_create_check_order(raw, endpoint_fixture):
    """Z4:invalid:*, Q3:invalid:*, R4:invalid:*, R5:*, S4:invalid:* (two faults per request: the one AWS reports)."""
    fx = endpoint_fixture
    lb = {"Protocol": "https", "Port": 443, "LoadBalancerArn": fx["lb"], "SubnetIds": fx["subnets"]}
    bad_lb = {**lb, "LoadBalancerArn": fx["lb"][:-16] + "0" * 16}
    bad_cert = fx["cert"][:-12] + "0" * 12
    cases = [
        ({"EndpointType": None, "AttachmentType": "bogus"}, "InvalidParameterValue",
         "The value of attachmentType you provided is not valid"),
        ({"EndpointType": "bogus", "AttachmentType": "bogus"}, "InvalidParameterValue",
         "The value of endpointType you provided is not valid; The value of attachmentType you provided is not valid"),
        ({"EndpointType": "bogus", "AttachmentType": None}, "InvalidParameterValue", "The value of endpointType you provided is not valid"),
        ({"AttachmentType": None, "LoadBalancerOptions": None}, "MissingParameter",
         "Required parameter AttachmentType was empty or null in the request"),
        ({"LoadBalancerOptions": {k: v for k, v in lb.items() if k != "Protocol"}}, "MissingParameter",
         "Missing required parameter protocol in EniOptions"),
        ({"LoadBalancerOptions": {"Port": 443, "PortRanges": [{"FromPort": 1, "ToPort": 2}]}}, "MissingParameter",
         "Missing required parameter protocol in EniOptions"),
        ({"LoadBalancerOptions": {k: v for k, v in lb.items() if k != "Port"}}, "MissingParameter",
         "Port must be provided in LoadBalancerOptions"),
        ({"LoadBalancerOptions": {k: v for k, v in lb.items() if k != "SubnetIds"}}, "MissingParameter",
         "SubnetIds must be provided in LoadBalancerOptions"),
        ({"LoadBalancerOptions": {**lb, "Protocol": "ftp"}}, "InvalidParameterValue",
         "The value of loadBalancerOptions.protocol you provided is not valid"),
        ({"LoadBalancerOptions": {**lb, "Port": 0}}, "InvalidParameterValue", "The value of loadBalancerOptions.port you provided is not valid"),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None,
          "NetworkInterfaceOptions": {"NetworkInterfaceId": fx["eni"], "Port": 443}}, "MissingParameter",
         "Missing required parameter protocol in EniOptions"),
        ({"SecurityGroupIds": None, "LoadBalancerOptions": {**lb, "SubnetIds": ["subnet-00000000000000000"]}},
         "InvalidParameterValue", "Invalid value for parameter SubnetIds"),
        ({"LoadBalancerOptions": {**lb, "SubnetIds": ["subnet-00000000000000000"]}, "DomainCertificateArn": bad_cert},
         "InvalidParameterValue", "Invalid value for parameter SubnetIds"),
        ({"SecurityGroupIds": ["sg-00000000000000000"], "LoadBalancerOptions": bad_lb}, "InvalidParameterValue",
         "The LoadBalancerArn is invalid"),
        ({"ApplicationDomain": None, "LoadBalancerOptions": bad_lb}, "InvalidParameterValue", "The LoadBalancerArn is invalid"),
        ({"EndpointDomainPrefix": "a.b", "LoadBalancerOptions": bad_lb}, "InvalidParameterValue", "The LoadBalancerArn is invalid"),
        ({"DomainCertificateArn": None, "ApplicationDomain": None, "EndpointDomainPrefix": None}, "MissingParameter",
         "Missing required parameter DomainCertificateArn"),
        ({"ApplicationDomain": None, "EndpointDomainPrefix": None}, "MissingParameter", "Missing required parameter ApplicationDomain"),
        ({"EndpointDomainPrefix": None, "DomainCertificateArn": bad_cert}, "MissingParameter",
         "Missing required parameter EndpointDomainPrefix"),
        ({"SecurityGroupIds": None, "DomainCertificateArn": None}, "MissingParameter", "Missing required parameter DomainCertificateArn"),
        ({"ApplicationDomain": None, "EndpointDomainPrefix": "a.b"}, "MissingParameter", "Missing required parameter ApplicationDomain"),
        ({"SecurityGroupIds": ["sg-00000000000000000"], "EndpointDomainPrefix": "a.b"}, "InvalidParameterValue",
         "The Security Group Ids are invalid"),
        ({"SecurityGroupIds": ["sg-00000000000000000"], "ApplicationDomain": "x.other.com"}, "InvalidParameterValue",
         "The Security Group Ids are invalid"),
        ({"SecurityGroupIds": None, "EndpointDomainPrefix": "a.b"}, "InvalidParameterValue",
         "The EndpointDomainPrefix provided in the request contains unsupported characters"),
        ({"EndpointDomainPrefix": "a.b", "DomainCertificateArn": bad_cert}, "InvalidParameterValue",
         "The EndpointDomainPrefix provided in the request contains unsupported characters"),
        ({"EndpointDomainPrefix": "-ab"}, "InvalidParameterValue", "The EndpointDomainPrefix provided in the request contains unsupported characters"),
        ({"EndpointDomainPrefix": "ab-"}, "InvalidParameterValue", "The EndpointDomainPrefix provided in the request contains unsupported characters"),
        ({"EndpointDomainPrefix": "a.bbbbbbbbbbbbbbbbbbb"}, "InvalidParameterValue",
         "The EndpointDomainPrefix provided in the request contains unsupported characters"),
        ({"EndpointDomainPrefix": "a" * 21}, "InvalidParameterValue", "Provided EndpointDomainPrefix exceeds max character limit of 20 characters"),
        ({"SecurityGroupIds": None}, "MissingParameter", "Security Group Ids must be provided if attachment type vpc is selected"),
        ({"SecurityGroupIds": None, "DomainCertificateArn": bad_cert}, "InvalidParameterValue", "The CertificateArn is invalid"),
        ({"LoadBalancerOptions": {**lb, "Protocol": "tcp"}}, "InvalidParameterValue",
         "Parameter port in LoadBalancerOptions is not supported with 'tcp' protocol"),
        ({"LoadBalancerOptions": {k: v for k, v in {**lb, "Protocol": "tcp"}.items() if k != "Port"}}, "MissingParameter",
         "PortRanges must be provided in LoadBalancerOptions"),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None,
          "NetworkInterfaceOptions": {"NetworkInterfaceId": fx["eni"], "Protocol": "tcp", "Port": 443}}, "InvalidParameterValue",
         "Parameter port in EniOptions is not supported with 'tcp' protocol"),
        ({"LoadBalancerOptions": {k: v for k, v in {**lb, "Port": 0}.items() if k != "SubnetIds"}}, "InvalidParameterValue",
         "The value of loadBalancerOptions.port you provided is not valid"),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None,
          "NetworkInterfaceOptions": {"Protocol": "https", "Port": 0}}, "InvalidParameterValue",
         "The value of networkInterfaceOptions.port you provided is not valid"),
        ({"DomainCertificateArn": None, "SecurityGroupIds": ["sg-00000000000000000"]}, "InvalidParameterValue",
         "The Security Group Ids are invalid"),
        ({"LoadBalancerOptions": {**lb, "Port": 65536}}, "InvalidParameterValue", "The value of loadBalancerOptions.port you provided is not valid"),
        ({"ApplicationDomain": "APP.example.com"}, "InvalidParameterValue",
         "The ApplicationDomain provided is not present in the DomainCertificateArn provided in the request."),
        ({"EndpointDomainPrefix": "_a-bcdefghijklmnopqr", "DomainCertificateArn": bad_cert}, "InvalidParameterValue",
         "The CertificateArn is invalid"),
        ({"LoadBalancerOptions": {"Protocol": "ftp", "SubnetIds": fx["subnets"], "LoadBalancerArn": fx["lb"]}},
         "InvalidParameterValue", "The value of loadBalancerOptions.protocol you provided is not valid"),
        ({"LoadBalancerOptions": {"Protocol": "https", "LoadBalancerArn": fx["lb"]}}, "MissingParameter",
         "Port must be provided in LoadBalancerOptions"),
        ({"LoadBalancerOptions": {"Protocol": "https", "Port": 443}}, "MissingParameter", "SubnetIds must be provided in LoadBalancerOptions"),
        ({"LoadBalancerOptions": {**lb, "Port": 0, "PortRanges": [{"FromPort": 1, "ToPort": 2}]}}, "InvalidParameterValue",
         "The value of loadBalancerOptions.port you provided is not valid"),
        ({"LoadBalancerOptions": {**bad_lb, "SubnetIds": ["subnet-00000000000000000"]}}, "InvalidParameterValue",
         "Invalid value for parameter SubnetIds"),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None, "NetworkInterfaceOptions": {"Port": 443}},
         "MissingParameter", "Missing required parameter protocol in EniOptions"),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None,
          "NetworkInterfaceOptions": {"NetworkInterfaceId": fx["eni"], "Protocol": "https", "Port": 0}},
         "InvalidParameterValue", "The value of networkInterfaceOptions.port you provided is not valid"),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None,
          "NetworkInterfaceOptions": {"NetworkInterfaceId": fx["eni"], "Protocol": "ftp", "Port": 443}},
         "InvalidParameterValue", "The value of networkInterfaceOptions.protocol you provided is not valid"),
        ({"DomainCertificateArn": bad_cert, "ApplicationDomain": "x.other.com"}, "InvalidParameterValue", "The CertificateArn is invalid"),
    ]
    for over, code, message in cases:
        with pytest.raises(ClientError) as exc:
            raw.create_verified_access_endpoint(**_lb_endpoint_params(fx, **over))
        assert _error(exc) == (code, message), over
    assert raw.describe_verified_access_endpoints(VerifiedAccessGroupId=fx["group"])["VerifiedAccessEndpoints"] == []


def test_verified_access_endpoint_prefix_wildcard_and_device_domain(ec2, endpoint_fixture):
    """Z4:invalid:prefix-upper, Z4:invalid:prefix-underscore, Z4:invalid:wild-cert-*, Q3:create:wild-one-level, Q3:invalid:wild-apex,
    X2:create:lb (no device provider), Q3:create:wild-one-level (with one), Q5:create:same-domain-while-deleting."""
    fx = endpoint_fixture
    wild = make_client("acm").request_certificate(DomainName="*.example.com")["CertificateArn"]
    for domain in ("example.com", "a.b.example.com"):
        with pytest.raises(ClientError) as exc:
            ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx, DomainCertificateArn=wild, ApplicationDomain=domain))
        assert _error(exc) == ("InvalidParameterValue", "The ApplicationDomain provided is not present in the "
                               "DomainCertificateArn provided in the request.")
    plain = ec2.create_verified_access_endpoint(**_lb_endpoint_params(
        fx, DomainCertificateArn=wild, ApplicationDomain="web.example.com", EndpointDomainPrefix="App_1"))["VerifiedAccessEndpoint"]
    assert plain["EndpointDomain"].startswith("App_1.edge-")
    assert "DeviceValidationDomain" not in plain
    ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=plain["VerifiedAccessEndpointId"])
    jamf = _device_provider(ec2, "jamf")["VerifiedAccessTrustProviderId"]
    _attach(ec2, fx["instance"], jamf)
    # R5:describe:after-device: the domain shows once the instance has a device provider.
    assert ec2.describe_verified_access_endpoints(
        VerifiedAccessEndpointIds=[plain["VerifiedAccessEndpointId"]])["VerifiedAccessEndpoints"][0]["DeviceValidationDomain"]
    # The domain of an endpoint that is being deleted is free again.
    with_device = ec2.create_verified_access_endpoint(**_lb_endpoint_params(
        fx, DomainCertificateArn=wild, ApplicationDomain="web.example.com", EndpointDomainPrefix="web"))["VerifiedAccessEndpoint"]
    edge = with_device["EndpointDomain"].split(".")[1]
    validation = with_device["DeviceValidationDomain"]
    assert validation.startswith(f"{edge}.vld-") and validation.endswith(".vld.prod.verified-access.us-east-1.amazonaws.com")
    # One vld id per instance (Q3:create:wild-one-level and Q5:create:same-domain-while-deleting share it).
    second = ec2.create_verified_access_endpoint(**_lb_endpoint_params(
        fx, DomainCertificateArn=wild, ApplicationDomain="web2.example.com", EndpointDomainPrefix="web2"))["VerifiedAccessEndpoint"]
    assert second["DeviceValidationDomain"].split(".")[1] == validation.split(".")[1]
    assert second["EndpointDomain"].split(".")[1] != edge
    ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=second["VerifiedAccessEndpointId"])
    ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=with_device["VerifiedAccessEndpointId"])
    ec2.describe_verified_access_endpoints(VerifiedAccessInstanceId=fx["instance"])
    ec2.detach_verified_access_trust_provider(VerifiedAccessInstanceId=fx["instance"], VerifiedAccessTrustProviderId=jamf)
    ec2.delete_verified_access_trust_provider(VerifiedAccessTrustProviderId=jamf)


def test_verified_access_round10_endpoints(ec2, raw, endpoint_fixture):
    """T1:*, T3:*, T4:*, T5:* (round 10): tcp, network-interface values and protocol changes on modify,
    the group checked first on modify, the device validation domain after a detach."""
    fx = endpoint_fixture
    lb = {"Protocol": "https", "Port": 443, "LoadBalancerArn": fx["lb"], "SubnetIds": fx["subnets"]}
    native = ("InvalidParameterValue", "TCP based endpoints require a TrustProvider with NativeApplicationOidcOptions to be "
              "attached to the instance.")
    for over, error in (
        ({"ApplicationDomain": "x.other.com", "SecurityGroupIds": None},
         ("InvalidParameterValue", "The ApplicationDomain provided is not present in the DomainCertificateArn provided in the request.")),
        ({"LoadBalancerOptions": {**{k: v for k, v in lb.items() if k != "Port"}, "Protocol": "tcp",
                                  "PortRanges": [{"FromPort": 443, "ToPort": 443}]}}, native),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None,
          "NetworkInterfaceOptions": {"NetworkInterfaceId": fx["eni"], "Protocol": "tcp"}},
         ("MissingParameter", "Missing required parameter PortRanges")),
        ({"EndpointType": "network-interface", "LoadBalancerOptions": None,
          "NetworkInterfaceOptions": {"NetworkInterfaceId": fx["eni"], "Protocol": "tcp", "PortRanges": [{"FromPort": 443, "ToPort": 443}]}},
         native),
    ):
        with pytest.raises(ClientError) as exc:
            raw.create_verified_access_endpoint(**_lb_endpoint_params(fx, **over))
        assert _error(exc) == error, over

    jamf = _device_provider(ec2, "jamf")["VerifiedAccessTrustProviderId"]
    _attach(ec2, fx["instance"], jamf)
    lb_ep = ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx))["VerifiedAccessEndpoint"]
    with pytest.raises(ClientError) as exc:
        ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx, EndpointDomainPrefix="w2", SecurityGroupIds=None))
    assert _error(exc) == ("MissingParameter", "Security Group Ids must be provided if attachment type vpc is selected")
    eni_ep = ec2.create_verified_access_endpoint(**_lb_endpoint_params(
        fx, EndpointType="network-interface", LoadBalancerOptions=None, ApplicationDomain="eni.example.com",
        EndpointDomainPrefix="eni", NetworkInterfaceOptions={"NetworkInterfaceId": fx["eni"], "Protocol": "https", "Port": 443},
    ))["VerifiedAccessEndpoint"]
    me = raw.modify_verified_access_endpoint
    for ep_id, kwargs, error in (
        (eni_ep["VerifiedAccessEndpointId"], {"NetworkInterfaceOptions": {"Port": 0}},
         ("InvalidParameterValue", "The value of networkInterfaceOptions.port you provided is not valid")),
        (eni_ep["VerifiedAccessEndpointId"], {"NetworkInterfaceOptions": {"Protocol": "ftp"}},
         ("InvalidParameterValue", "The value of networkInterfaceOptions.protocol you provided is not valid")),
        (eni_ep["VerifiedAccessEndpointId"], {"NetworkInterfaceOptions": {"Protocol": "tcp", "Port": 443}},
         ("InvalidParameterValue", "Protocol cannot be modified between HTTP/HTTPS and TCP")),
        (lb_ep["VerifiedAccessEndpointId"], {"LoadBalancerOptions": {"Protocol": "tcp"}},
         ("InvalidParameterValue", "Protocol cannot be modified between HTTP/HTTPS and TCP")),
        (lb_ep["VerifiedAccessEndpointId"], {"VerifiedAccessGroupId": _MISSING["Group"], "NetworkInterfaceOptions": {"Port": 80}},
         ("InvalidVerifiedAccessGroupId.NotFound", f"The verified-access-group ID '{_MISSING['Group']}' does not exist")),
        (lb_ep["VerifiedAccessEndpointId"], {"VerifiedAccessGroupId": _MISSING["Group"], "LoadBalancerOptions": {"Port": 0}},
         ("InvalidVerifiedAccessGroupId.NotFound", f"The verified-access-group ID '{_MISSING['Group']}' does not exist")),
    ):
        with pytest.raises(ClientError) as exc:
            me(VerifiedAccessEndpointId=ep_id, **kwargs)
        assert _error(exc) == error, kwargs
    ec2.detach_verified_access_trust_provider(VerifiedAccessInstanceId=fx["instance"], VerifiedAccessTrustProviderId=jamf)
    after = ec2.describe_verified_access_endpoints(VerifiedAccessEndpointIds=[lb_ep["VerifiedAccessEndpointId"]])
    assert after["VerifiedAccessEndpoints"][0]["DeviceValidationDomain"] == lb_ep["DeviceValidationDomain"]
    for ep in (lb_ep, eni_ep):
        ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ep["VerifiedAccessEndpointId"])
    ec2.delete_verified_access_trust_provider(VerifiedAccessTrustProviderId=jamf)


def test_verified_access_round11_endpoints(ec2, raw, endpoint_fixture):
    """U1:*, U3:logging:*, U4:*, U5:*, U7:* (round 11): the tcp check order on create and modify, another
    account as the log bucket owner, the device domain without a read, and an endpoint in an orphaned group."""
    fx = endpoint_fixture
    lb = {"Protocol": "https", "Port": 443, "LoadBalancerArn": fx["lb"], "SubnetIds": fx["subnets"]}
    ranges = [{"FromPort": 443, "ToPort": 443}]
    tcp_lb = {"Protocol": "tcp", "LoadBalancerArn": fx["lb"], "SubnetIds": fx["subnets"], "PortRanges": ranges}
    native = ("InvalidParameterValue", "TCP based endpoints require a TrustProvider with NativeApplicationOidcOptions to be "
              "attached to the instance.")
    eni = {"EndpointType": "network-interface", "LoadBalancerOptions": None}
    for over, error in (
        ({"LoadBalancerOptions": {**tcp_lb, "Port": 443}},
         ("InvalidParameterValue", "Parameter port in LoadBalancerOptions is not supported with 'tcp' protocol")),
        ({"LoadBalancerOptions": {**lb, "Protocol": "tcp", "Port": 0}},
         ("InvalidParameterValue", "The value of loadBalancerOptions.port you provided is not valid")),
        ({"LoadBalancerOptions": {k: v for k, v in tcp_lb.items() if k != "LoadBalancerArn"}},
         ("MissingParameter", "LoadBalancerArn must be provided in LoadBalancerOptions")),
        ({"LoadBalancerOptions": {k: v for k, v in tcp_lb.items() if k != "SubnetIds"}}, native),
        ({"LoadBalancerOptions": {**tcp_lb, "SubnetIds": ["subnet-00000000000000000"]}},
         ("InvalidParameterValue", "Invalid value for parameter SubnetIds")),
        ({"LoadBalancerOptions": {**tcp_lb, "LoadBalancerArn": fx["lb"][:-16] + "0" * 16}},
         ("InvalidParameterValue", "The LoadBalancerArn is invalid")),
        ({"LoadBalancerOptions": tcp_lb, "SecurityGroupIds": None},
         ("MissingParameter", "Security Group Ids must be provided if attachment type vpc is selected")),
        ({"LoadBalancerOptions": tcp_lb, "DomainCertificateArn": fx["cert"][:-12] + "0" * 12}, native),
        ({**eni, "NetworkInterfaceOptions": {"NetworkInterfaceId": "eni-00000000000000000", "Protocol": "tcp", "PortRanges": ranges}},
         ("InvalidParameterValue", "NetworkInterfaceId is invalid")),
        ({**eni, "NetworkInterfaceOptions": {"Protocol": "tcp", "PortRanges": ranges}},
         ("MissingParameter", "Missing required parameter NetworkInterfaceId")),
    ):
        with pytest.raises(ClientError) as exc:
            raw.create_verified_access_endpoint(**_lb_endpoint_params(fx, **over))
        assert _error(exc) == error, over

    bucket = f"va-owner-{uuid.uuid4().hex[:8]}"
    make_client("s3").create_bucket(Bucket=bucket)
    lm = ec2.modify_verified_access_instance_logging_configuration
    with pytest.raises(ClientError) as exc:
        lm(VerifiedAccessInstanceId=fx["instance"],
           AccessLogs={"S3": {"Enabled": True, "BucketName": bucket, "BucketOwner": "123456789012"}})
    assert _error(exc) == ("LogDestinationPermissionIssue", "User does not have permission to configure log destination "
                           "resource identified by AccessLogs.S3.BucketName for log delivery")
    assert _status(exc) == 400
    account = make_client("sts").get_caller_identity()["Account"]
    logs = lm(VerifiedAccessInstanceId=fx["instance"],
              AccessLogs={"S3": {"Enabled": True, "BucketName": bucket, "BucketOwner": account}})["LoggingConfiguration"]
    assert logs["AccessLogs"]["S3"]["BucketOwner"] == account
    lm(VerifiedAccessInstanceId=fx["instance"], AccessLogs={"S3": {"Enabled": False}})

    # An attach and a detach with no read between them still leave the endpoint a device validation domain.
    jamf = _device_provider(ec2, "jamf")["VerifiedAccessTrustProviderId"]
    _attach(ec2, fx["instance"], jamf)
    ec2.detach_verified_access_trust_provider(VerifiedAccessInstanceId=fx["instance"], VerifiedAccessTrustProviderId=jamf)
    ep = ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx))["VerifiedAccessEndpoint"]
    assert ep["DeviceValidationDomain"]
    ep_id = ep["VerifiedAccessEndpointId"]
    for opts, error in (
        ({"Port": 0, "PortRanges": [{"FromPort": 1, "ToPort": 2}]},
         "The value of loadBalancerOptions.port you provided is not valid"),
        ({"Protocol": "tcp", "PortRanges": [{"FromPort": 1, "ToPort": 2}]}, "Protocol cannot be modified between HTTP/HTTPS and TCP"),
        ({"Protocol": "tcp", "SubnetIds": ["subnet-00000000000000000"]}, "Protocol cannot be modified between HTTP/HTTPS and TCP"),
        ({"Protocol": "tcp", "Port": 0}, "The value of loadBalancerOptions.port you provided is not valid"),
        ({"Protocol": "ftp", "PortRanges": [{"FromPort": 1, "ToPort": 2}]},
         "The value of loadBalancerOptions.protocol you provided is not valid"),
    ):
        with pytest.raises(ClientError) as exc:
            raw.modify_verified_access_endpoint(VerifiedAccessEndpointId=ep_id, LoadBalancerOptions=opts)
        assert _error(exc) == ("InvalidParameterValue", error), opts
    ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ep_id)
    ec2.delete_verified_access_trust_provider(VerifiedAccessTrustProviderId=jamf)

    # A group moved to a bare instance that is then deleted takes no HTTP/HTTPS endpoint.
    orphan = _group(ec2, fx["instance"])
    bare = _instance(ec2)
    ec2.modify_verified_access_group(VerifiedAccessGroupId=orphan, VerifiedAccessInstanceId=bare)
    ec2.delete_verified_access_instance(VerifiedAccessInstanceId=bare)
    with pytest.raises(ClientError) as exc:
        ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx, VerifiedAccessGroupId=orphan))
    assert _error(exc) == ("InvalidParameterValue",
                           "HTTP/HTTPS based endpoints require a TrustProvider with OidcOptions to be attached to the instance.")
    ec2.delete_verified_access_group(VerifiedAccessGroupId=orphan)


def test_verified_access_round12_check_orders(ec2, raw, endpoint_fixture):
    """V1:invalid:*, V3:logging:*, V4:modify:*, V5:invalid:* (round 12), W1:invalid:* (round 13): the remaining pairs of two faults."""
    fx = endpoint_fixture
    ranges = [{"FromPort": 443, "ToPort": 443}]
    tcp_lb = {"Protocol": "tcp", "LoadBalancerArn": fx["lb"], "SubnetIds": fx["subnets"], "PortRanges": ranges}
    native = ("InvalidParameterValue", "TCP based endpoints require a TrustProvider with NativeApplicationOidcOptions to be "
              "attached to the instance.")
    oidc = ("InvalidParameterValue", "HTTP/HTTPS based endpoints require a TrustProvider with OidcOptions to be attached to the instance.")
    fake_sg, fake_eni = "sg-00000000000000000", "eni-00000000000000000"
    eni = {"EndpointType": "network-interface", "LoadBalancerOptions": None}
    for over, error in (
        ({"LoadBalancerOptions": {"Protocol": "tcp", "SubnetIds": fx["subnets"]}},
         ("MissingParameter", "PortRanges must be provided in LoadBalancerOptions")),
        ({**eni, "NetworkInterfaceOptions": {"Protocol": "tcp"}}, ("MissingParameter", "Missing required parameter NetworkInterfaceId")),
        ({"LoadBalancerOptions": {"Protocol": "tcp", "Port": 443, "SubnetIds": fx["subnets"], "PortRanges": ranges}},
         ("InvalidParameterValue", "Parameter port in LoadBalancerOptions is not supported with 'tcp' protocol")),
        ({**eni, "NetworkInterfaceOptions": {"Protocol": "tcp", "Port": 443, "PortRanges": ranges}},
         ("InvalidParameterValue", "Parameter port in EniOptions is not supported with 'tcp' protocol")),
        ({"LoadBalancerOptions": tcp_lb, "SecurityGroupIds": [fake_sg]}, ("InvalidParameterValue", "The Security Group Ids are invalid")),
        ({"LoadBalancerOptions": tcp_lb, "DomainCertificateArn": None}, native),
        ({"LoadBalancerOptions": tcp_lb, "ApplicationDomain": None}, native),
        ({"LoadBalancerOptions": tcp_lb, "EndpointDomainPrefix": None}, native),
        ({**eni, "SecurityGroupIds": [fake_sg], "NetworkInterfaceOptions": {
            "NetworkInterfaceId": fake_eni, "Protocol": "tcp", "PortRanges": ranges}}, ("InvalidParameterValue", "NetworkInterfaceId is invalid")),
        ({**eni, "SecurityGroupIds": [fake_sg], "NetworkInterfaceOptions": {
            "NetworkInterfaceId": fake_eni, "Protocol": "https", "Port": 443}}, ("InvalidParameterValue", "NetworkInterfaceId is invalid")),
    ):
        with pytest.raises(ClientError) as exc:
            raw.create_verified_access_endpoint(**_lb_endpoint_params(fx, **over))
        assert _error(exc) == error, over

    bucket = f"va-pairs-{uuid.uuid4().hex[:8]}"
    make_client("s3").create_bucket(Bucket=bucket)
    other = {"Enabled": True, "BucketName": bucket, "BucketOwner": "123456789012"}
    for missing in ({"CloudWatchLogs": {"Enabled": True, "LogGroup": "/va/missing"}},
                    {"KinesisDataFirehose": {"Enabled": True, "DeliveryStream": "va-missing"}}):
        with pytest.raises(ClientError) as exc:
            ec2.modify_verified_access_instance_logging_configuration(
                VerifiedAccessInstanceId=fx["instance"], AccessLogs={"S3": other, **missing})
        assert _error(exc)[0] == "LogDestinationPermissionIssue", missing

    ep_id = ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx))["VerifiedAccessEndpoint"]["VerifiedAccessEndpointId"]
    for kwargs, error in (
        ({"NetworkInterfaceOptions": {"Port": 80}, "LoadBalancerOptions": {"Port": 0}},
         ("InvalidParameterValue", "The value of loadBalancerOptions.port you provided is not valid")),
        ({"NetworkInterfaceOptions": {"Port": 80}, "LoadBalancerOptions": {"Protocol": "ftp"}},
         ("InvalidParameterValue", "The value of loadBalancerOptions.protocol you provided is not valid")),
        ({"LoadBalancerOptions": {"PortRanges": ranges, "SubnetIds": ["subnet-00000000000000000"]}},
         ("InvalidParameterValue", "Parameter PortRanges is not currently supported for the operation")),
    ):
        with pytest.raises(ClientError) as exc:
            raw.modify_verified_access_endpoint(VerifiedAccessEndpointId=ep_id, **kwargs)
        assert _error(exc) == error, kwargs
    ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ep_id)

    # A group on an instance with no provider, then on one that was deleted, answers alike.
    group = _group(ec2, fx["instance"])
    bare = _instance(ec2)
    ec2.modify_verified_access_group(VerifiedAccessGroupId=group, VerifiedAccessInstanceId=bare)
    fake_key = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000"
    pairs = (
        ({}, oidc),
        ({"SecurityGroupIds": None}, ("MissingParameter", "Security Group Ids must be provided if attachment type vpc is selected")),
        ({"SecurityGroupIds": [fake_sg]}, ("InvalidParameterValue", "The Security Group Ids are invalid")),
        ({"DomainCertificateArn": None}, ("MissingParameter", "Missing required parameter DomainCertificateArn")),
        ({"ApplicationDomain": "x.other.com"},
         ("InvalidParameterValue", "The ApplicationDomain provided is not present in the DomainCertificateArn provided in the request.")),
        ({"SseSpecification": {"CustomerManagedKeyEnabled": True, "KmsKeyArn": fake_key}}, oidc),
    )
    for deleted in (False, True):
        if deleted:
            ec2.delete_verified_access_instance(VerifiedAccessInstanceId=bare)
        for over, error in pairs:
            with pytest.raises(ClientError) as exc:
                raw.create_verified_access_endpoint(**_lb_endpoint_params(
                    fx, VerifiedAccessGroupId=group, EndpointDomainPrefix="orphan", **over))
            assert _error(exc) == error, (deleted, over)
    ec2.delete_verified_access_group(VerifiedAccessGroupId=group)


def test_verified_access_round14_check_orders(ec2, raw, endpoint_fixture):
    """X1:tp:*, X2:invalid:*, X3:*, X4:logging:*, X6:modify:*, X7:* (round 14): the pairs the final audit found uncaptured."""
    oidc = {"Issuer": "https://idp.example.com", "AuthorizationEndpoint": "https://idp.example.com/auth",
            "TokenEndpoint": "https://idp.example.com/token", "UserInfoEndpoint": "https://idp.example.com/userinfo",
            "ClientId": "client", "ClientSecret": "secret", "Scope": "openid"}
    user = dict(TrustProviderType="user", UserTrustProviderType="oidc", PolicyReferenceName="idp")
    for kwargs, error in (
        ({"OidcOptions": {k: v for k, v in oidc.items() if k not in ("Issuer", "AuthorizationEndpoint")}},
         ("MissingParameter", "Missing required parameter Issuer in OidcOptions")),
        ({"OidcOptions": {k: v for k, v in oidc.items() if k not in ("TokenEndpoint", "UserInfoEndpoint")}},
         ("MissingParameter", "Missing required parameter TokenEndpoint in OidcOptions")),
        ({"OidcOptions": {k: v for k, v in oidc.items() if k not in ("UserInfoEndpoint", "ClientId")}},
         ("MissingParameter", "Missing required parameter UserInfoEndpoint in OidcOptions")),
        ({"OidcOptions": {k: v for k, v in oidc.items() if k not in ("ClientId", "ClientSecret")}},
         ("MissingParameter", "Missing required parameter ClientId in OidcOptions")),
        ({"OidcOptions": {k: v for k, v in oidc.items() if k not in ("ClientSecret", "Scope")}},
         ("MissingParameter", "Missing required parameter ClientSecret in OidcOptions")),
        ({"OidcOptions": {k: v for k, v in oidc.items() if k != "Issuer"}, "DeviceOptions": {"TenantId": "t"}},
         ("MissingParameter", "Missing required parameter Issuer in OidcOptions")),
        ({"OidcOptions": {**oidc, "Issuer": "http://idp.example.com"}, "DeviceTrustProviderType": "jamf"},
         ("InvalidParameterValue", "Parameter Issuer in OidcOptions must be a valid HTTPS URL")),
        ({"OidcOptions": oidc, "PolicyReferenceName": "a!" * 20},
         ("InvalidParameterValue", "Length of parameter PolicyReferenceName must be between 1 and 32")),
    ):
        with pytest.raises(ClientError) as exc:
            raw.create_verified_access_trust_provider(**{**user, **kwargs})
        assert _error(exc) == error, kwargs

    fx = endpoint_fixture
    lb = {"Protocol": "https", "Port": 443, "LoadBalancerArn": fx["lb"], "SubnetIds": fx["subnets"]}
    ranges = [{"FromPort": 443, "ToPort": 443}]
    refused = ("InvalidParameterValue", "Parameter PortRanges is not currently supported for the operation")
    eni = {"EndpointType": "network-interface", "LoadBalancerOptions": None}
    for over, error in (
        ({"LoadBalancerOptions": {**lb, "Protocol": "ftp", "Port": 0}},
         ("InvalidParameterValue", "The value of loadBalancerOptions.protocol you provided is not valid; "
                                   "The value of loadBalancerOptions.port you provided is not valid")),
        ({"LoadBalancerOptions": {k: v for k, v in {**lb, "PortRanges": ranges}.items() if k != "Port"}}, refused),
        ({"LoadBalancerOptions": {k: v for k, v in {**lb, "PortRanges": ranges}.items() if k != "SubnetIds"}}, refused),
        ({"LoadBalancerOptions": {k: v for k, v in {**lb, "PortRanges": ranges}.items() if k != "LoadBalancerArn"}}, refused),
        ({"LoadBalancerOptions": {**lb, "SubnetIds": ["subnet-00000000000000000"], "PortRanges": ranges}}, refused),
        ({"SecurityGroupIds": ["sg-00000000000000000"], "LoadBalancerOptions": {**lb, "PortRanges": ranges}}, refused),
        ({**eni, "NetworkInterfaceOptions": {"Protocol": "https", "Port": 443, "PortRanges": ranges}}, refused),
        ({**eni, "NetworkInterfaceOptions": {"NetworkInterfaceId": "eni-00000000000000000", "Protocol": "https", "Port": 443,
                                             "PortRanges": ranges}}, refused),
    ):
        with pytest.raises(ClientError) as exc:
            raw.create_verified_access_endpoint(**_lb_endpoint_params(fx, **over))
        assert _error(exc) == error, over

    for call, kwargs, error in (
        (raw.describe_verified_access_groups, {"VerifiedAccessInstanceId": "abc", "MaxResults": 1},
         ("InvalidParameterValue", "The parameter MaxResults must be between 5 and 200")),
        (raw.describe_verified_access_groups, {"VerifiedAccessInstanceId": "abc", "NextToken": "bad"},
         ("InvalidParameterValue", "Length of parameter VerifiedAccessInstanceId must be between 5 and 21")),
        (raw.describe_verified_access_endpoints, {"VerifiedAccessInstanceId": fx["instance"], "VerifiedAccessGroupId": fx["group"],
                                                  "NextToken": "bad"},
         ("InvalidParameterCombination", "The parameters VerifiedAccessInstanceId and VerifiedAccessGroupId cannot be used together")),
        (raw.describe_verified_access_instances, {"VerifiedAccessInstanceIds": [fx["instance"]], "MaxResults": "abc", "NextToken": "bad"},
         ("InvalidParameterValue", "Invalid integer value abc")),
    ):
        with pytest.raises(ClientError) as exc:
            call(**kwargs)
        assert _error(exc) == error, kwargs

    for logs in ({"CloudWatchLogs": {"Enabled": True}, "KinesisDataFirehose": {"Enabled": True}},
                 {"CloudWatchLogs": {"Enabled": True}, "KinesisDataFirehose": {"Enabled": False, "DeliveryStream": "x"}}):
        with pytest.raises(ClientError) as exc:
            ec2.modify_verified_access_instance_logging_configuration(VerifiedAccessInstanceId=fx["instance"], AccessLogs=logs)
        assert _error(exc) == ("MissingParameter", "Missing required parameter AccessLogs.CloudWatchLogs.LogGroup"), logs

    ep_id = ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx))["VerifiedAccessEndpoint"]["VerifiedAccessEndpointId"]
    combination = ("InvalidParameterCombination",
                   "NetworkInterfaceOptions may only be specified for Verified Access Endpoints with network-interface type")
    for opts in ({"Protocol": "tcp"}, {"PortRanges": ranges}, {"SubnetIds": ["subnet-00000000000000000"]}):
        with pytest.raises(ClientError) as exc:
            raw.modify_verified_access_endpoint(VerifiedAccessEndpointId=ep_id, NetworkInterfaceOptions={"Port": 80},
                                                LoadBalancerOptions=opts)
        assert _error(exc) == combination, opts
    # A deleting endpoint doesn't block the instance delete; the attached provider does.
    ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ep_id)
    with pytest.raises(ClientError) as exc:
        ec2.delete_verified_access_instance(VerifiedAccessInstanceId=fx["instance"])
    assert _error(exc)[1].endswith("has existing attachments. Remove all VerifiedAccessTrustProvider attachments before deleting.")


def test_verified_access_round15_pages_and_pairs(ec2, raw, endpoint_fixture):
    """Y1:tp:long-scope+device-options, Y3:endpoints:page-5, Y4:modify:* (round 15)."""
    import urllib.request

    with pytest.raises(ClientError) as exc:
        raw.create_verified_access_trust_provider(
            TrustProviderType="user", UserTrustProviderType="oidc", PolicyReferenceName="idp", DeviceOptions={"TenantId": "t"},
            OidcOptions={"Issuer": "https://idp.example.com", "AuthorizationEndpoint": "https://idp.example.com/auth",
                         "TokenEndpoint": "https://idp.example.com/token", "UserInfoEndpoint": "https://idp.example.com/userinfo",
                         "ClientId": "client", "ClientSecret": "secret", "Scope": "s" * 300})
    assert _error(exc) == ("InvalidParameterValue", "Length of parameter Scope in OidcOptions must be between 1 and 256")

    fx = endpoint_fixture
    names = [f"app{n}.example.com" for n in range(1, 7)]
    cert = make_client("acm").request_certificate(DomainName=names[0], SubjectAlternativeNames=names)["CertificateArn"]
    eps = [ec2.create_verified_access_endpoint(**_lb_endpoint_params(
        fx, DomainCertificateArn=cert, ApplicationDomain=name, EndpointDomainPrefix=name.split(".")[0],
    ))["VerifiedAccessEndpoint"]["VerifiedAccessEndpointId"] for name in names]
    body = f"Action=DescribeVerifiedAccessEndpoints&Version=2016-11-15&VerifiedAccessInstanceId={fx['instance']}&MaxResults=5"
    req = urllib.request.Request(ENDPOINT, data=body.encode(), method="POST", headers={
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/ec2/aws4_request"})
    with urllib.request.urlopen(req) as resp:
        text = resp.read().decode()
    assert -1 < text.index("<verifiedAccessEndpointSet>") < text.index("<nextToken>")

    for port in (0, 70000):
        with pytest.raises(ClientError) as exc:
            raw.modify_verified_access_endpoint(VerifiedAccessEndpointId=eps[0],
                                                LoadBalancerOptions={"Protocol": "ftp", "Port": port})
        assert _error(exc) == ("InvalidParameterValue", "The value of loadBalancerOptions.protocol you provided is not valid; "
                                                        "The value of loadBalancerOptions.port you provided is not valid"), port
    for ep_id in eps:
        ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ep_id)


def test_verified_access_round16_check_orders(ec2, raw, endpoint_fixture):
    """Z1:tp:*, Z2:*, Z3:invalid:* (round 16): the pairs the audit of 08b05ab5 found uncaptured."""
    oidc = {"Issuer": "https://idp.example.com", "AuthorizationEndpoint": "https://idp.example.com/auth",
            "TokenEndpoint": "https://idp.example.com/token", "UserInfoEndpoint": "https://idp.example.com/userinfo",
            "ClientId": "client", "ClientSecret": "secret", "Scope": "openid"}
    https = ("InvalidParameterValue", "Parameter Issuer in OidcOptions must be a valid HTTPS URL")
    for opts, extra, error in (
        ({k: v for k, v in oidc.items() if k != "ClientId"}, {"DeviceTrustProviderType": "jamf"},
         ("MissingParameter", "Missing required parameter ClientId in OidcOptions")),
        ({**oidc, "Scope": "s" * 300}, {"DeviceTrustProviderType": "jamf"},
         ("InvalidParameterValue", "Length of parameter Scope in OidcOptions must be between 1 and 256")),
        ({**oidc, "Issuer": "http://idp.example.com"}, {"DeviceOptions": {"TenantId": "t"}}, https),
        ({**{k: v for k, v in oidc.items() if k != "Issuer"}, "TokenEndpoint": "http://idp.example.com/token"}, {},
         ("MissingParameter", "Missing required parameter Issuer in OidcOptions")),
        ({**oidc, "Issuer": "http://idp.example.com", "Scope": "s" * 300}, {}, https),
    ):
        with pytest.raises(ClientError) as exc:
            raw.create_verified_access_trust_provider(TrustProviderType="user", UserTrustProviderType="oidc",
                                                      PolicyReferenceName="idp", OidcOptions=opts, **extra)
        assert _error(exc) == error, (opts, extra)

    fake_key = "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000"
    sse = {"CustomerManagedKeyEnabled": True, "KmsKeyArn": fake_key}
    for call, kwargs, code in (
        (raw.modify_verified_access_trust_provider, {"VerifiedAccessTrustProviderId": _MISSING["TrustProvider"],
                                                     "OidcOptions": {"Issuer": "http://idp.example.com"}}, "TrustProvider"),
        (raw.modify_verified_access_trust_provider, {"VerifiedAccessTrustProviderId": _MISSING["TrustProvider"],
                                                     "SseSpecification": sse}, "TrustProvider"),
        (raw.modify_verified_access_instance_logging_configuration, {"VerifiedAccessInstanceId": _MISSING["Instance"],
                                                                     "AccessLogs": {"S3": {"Enabled": True}}}, "Instance"),
        (raw.create_verified_access_group, {"VerifiedAccessInstanceId": _MISSING["Instance"], "SseSpecification": sse}, "Instance"),
        (raw.modify_verified_access_group, {"VerifiedAccessGroupId": _MISSING["Group"], "VerifiedAccessInstanceId": "abc"}, "Group"),
        (raw.modify_verified_access_group_policy, {"VerifiedAccessGroupId": _MISSING["Group"], "PolicyEnabled": True}, "Group"),
        (raw.modify_verified_access_endpoint_policy, {"VerifiedAccessEndpointId": _MISSING["Endpoint"], "PolicyEnabled": True}, "Endpoint"),
    ):
        with pytest.raises(ClientError) as exc:
            call(**kwargs)
        assert _error(exc)[0] == f"InvalidVerifiedAccess{code}Id.NotFound", kwargs

    fx = endpoint_fixture
    refused = ("InvalidParameterValue", "Parameter PortRanges is not currently supported for the operation")
    ranges = [{"FromPort": 443, "ToPort": 443}]
    eni = {"EndpointType": "network-interface", "LoadBalancerOptions": None}
    for over, error in (
        ({**eni, "NetworkInterfaceOptions": {"NetworkInterfaceId": "eni-00000000000000000", "Protocol": "ftp", "Port": 0}},
         ("InvalidParameterValue", "The value of networkInterfaceOptions.protocol you provided is not valid; "
                                   "The value of networkInterfaceOptions.port you provided is not valid")),
        ({"LoadBalancerOptions": {"Protocol": "http", "LoadBalancerArn": fx["lb"], "SubnetIds": fx["subnets"], "PortRanges": ranges}},
         refused),
        ({**eni, "NetworkInterfaceOptions": {"Protocol": "http", "Port": 80, "PortRanges": ranges}}, refused),
    ):
        with pytest.raises(ClientError) as exc:
            raw.create_verified_access_endpoint(**_lb_endpoint_params(fx, **over))
        assert _error(exc) == error, over


def test_verified_access_deleting_endpoint_keeps_page_offsets(ec2, endpoint_fixture):
    """MiniStack: a deleting endpoint is listed once more; pages of that listing keep their offsets."""
    fx = endpoint_fixture
    wild = make_client("acm").request_certificate(DomainName="*.example.com")["CertificateArn"]
    ids = [ec2.create_verified_access_endpoint(**_lb_endpoint_params(
        fx, DomainCertificateArn=wild, ApplicationDomain=f"e{n}.example.com", EndpointDomainPrefix=f"e{n}",
    ))["VerifiedAccessEndpoint"]["VerifiedAccessEndpointId"] for n in range(6)]
    ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ids[0])
    first = ec2.describe_verified_access_endpoints(VerifiedAccessGroupId=fx["group"], MaxResults=5)
    second = ec2.describe_verified_access_endpoints(VerifiedAccessGroupId=fx["group"], MaxResults=5,
                                                    NextToken=first["NextToken"])
    listed = [e["VerifiedAccessEndpointId"] for e in first["VerifiedAccessEndpoints"] + second["VerifiedAccessEndpoints"]]
    assert sorted(listed) == sorted(ids)
    fresh = ec2.describe_verified_access_endpoints(VerifiedAccessGroupId=fx["group"])["VerifiedAccessEndpoints"]
    assert sorted(e["VerifiedAccessEndpointId"] for e in fresh) == sorted(ids[1:])


def test_verified_access_endpoint_modify_check_order(ec2, endpoint_fixture):
    """Q4:modify:*, Q4:move-group:*."""
    fx = endpoint_fixture
    ep_id = ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx))["VerifiedAccessEndpoint"]["VerifiedAccessEndpointId"]
    other_inst, other_tp = _instance_with_provider(ec2)
    other = _group(ec2, other_inst)
    move = ec2.modify_verified_access_endpoint
    for kwargs, code, message in (
        ({"VerifiedAccessGroupId": _MISSING["Group"], "LoadBalancerOptions": {"SubnetIds": ["subnet-00000000000000000"]}},
         "InvalidVerifiedAccessGroupId.NotFound", f"The verified-access-group ID '{_MISSING['Group']}' does not exist"),
        ({"VerifiedAccessGroupId": other, "LoadBalancerOptions": {"PortRanges": [{"FromPort": 1, "ToPort": 2}]}},
         "InvalidParameterValue", "Parameter PortRanges is not currently supported for the operation"),
        ({"VerifiedAccessGroupId": other, "NetworkInterfaceOptions": {"Port": 80}}, "InvalidParameterCombination",
         "NetworkInterfaceOptions may only be specified for Verified Access Endpoints with network-interface type"),
        ({"VerifiedAccessGroupId": other, "LoadBalancerOptions": {"SubnetIds": ["subnet-00000000000000000"]}},
         "InvalidParameterValue", "Invalid value for parameter SubnetIds"),
        ({"LoadBalancerOptions": {"Protocol": "ftp"}}, "InvalidParameterValue",
         "The value of loadBalancerOptions.protocol you provided is not valid"),
    ):
        with pytest.raises(ClientError) as exc:
            move(VerifiedAccessEndpointId=ep_id, **kwargs)
        assert _error(exc) == (code, message), kwargs
    same = ec2.modify_verified_access_group(VerifiedAccessGroupId=fx["group"], VerifiedAccessInstanceId=fx["instance"])
    assert same["VerifiedAccessGroup"]["VerifiedAccessInstanceId"] == fx["instance"]
    with pytest.raises(ClientError) as exc:
        ec2.modify_verified_access_group(VerifiedAccessGroupId=fx["group"], VerifiedAccessInstanceId=_MISSING["Instance"])
    assert _error(exc)[0] == "InvalidVerifiedAccessInstanceId.NotFound"
    ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ep_id)
    _cleanup(ec2, groups=[other], instances=[other_inst], providers=[other_tp])


def test_verified_access_user_provider_attach_is_locked_by_groups(ec2):
    """Q2:move-group:to-bare-instance, Q2:attach:user-while-group, Q2:attach:device-while-group, Q2:create-group:device-only."""
    inst_id, tp_id = _instance_with_provider(ec2)
    group_id = _group(ec2, inst_id)
    bare = _instance(ec2)
    ec2.modify_verified_access_group(VerifiedAccessGroupId=group_id, VerifiedAccessInstanceId=bare)
    user = _oidc_provider(ec2, ref="idp3")["VerifiedAccessTrustProviderId"]
    with pytest.raises(ClientError) as exc:
        _attach(ec2, bare, user)
    assert _error(exc) == ("VerifiedAccessGroupAttachmentExists",
                           f"VerifiedAccessInstance {bare} is not empty. Please remove all VerifiedAccessGroups before "
                           "attaching or detaching a VerifiedAccessTrustProvider with TrustProviderType user")
    jamf = _device_provider(ec2, "jamf")["VerifiedAccessTrustProviderId"]
    _attach(ec2, bare, jamf)
    _cleanup(ec2, groups=[group_id], instances=[inst_id, bare], providers=[tp_id, user, jamf])


def test_verified_access_load_balancer_endpoint_lifecycle(ec2, endpoint_fixture):
    """X2:create:lb, X2:describe:active, X2:modify:while-pending, X2:delete-group:endpoint-pending,
    X2:delete-instance:with-groups-and-endpoints, X3:*, X4:modify:*-options-on-lb, V5:delete:eni-endpoint, V5:poll:delete."""
    fx = endpoint_fixture
    ep = ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx, Description="web", PolicyDocument=_POLICY, TagSpecifications=[
        {"ResourceType": "verified-access-endpoint", "Tags": [{"Key": "site", "Value": "app"}]}]))["VerifiedAccessEndpoint"]
    ep_id = ep["VerifiedAccessEndpointId"]
    assert ep_id.startswith("vae-") and len(ep_id) == 21
    assert ep["Status"] == {"Code": "pending"}
    domain = ep["EndpointDomain"]
    assert domain.startswith("app.edge-") and domain.endswith(f".{fx['instance']}.prod.verified-access.us-east-1.amazonaws.com")
    assert ep["LoadBalancerOptions"] == {"LoadBalancerArn": fx["lb"], "Port": 443, "Protocol": "https", "SubnetIds": fx["subnets"]}
    assert "NetworkInterfaceOptions" not in ep
    assert (ep["Tags"], ep["SseSpecification"]) == ([{"Key": "site", "Value": "app"}], {"CustomerManagedKeyEnabled": False})
    described = ec2.describe_verified_access_endpoints(VerifiedAccessEndpointIds=[ep_id])["VerifiedAccessEndpoints"][0]
    assert described["Status"] == {"Code": "active"}

    modified = ec2.modify_verified_access_endpoint(
        VerifiedAccessEndpointId=ep_id, Description="renamed", LoadBalancerOptions={"Port": 8443})["VerifiedAccessEndpoint"]
    assert (modified["Status"], modified["Description"]) == ({"Code": "updating"}, "renamed")
    assert modified["LoadBalancerOptions"]["Port"] == 8443 and modified["LoadBalancerOptions"]["LoadBalancerArn"] == fx["lb"]
    assert "Tags" not in modified
    for kwargs, message in (
        ({"NetworkInterfaceOptions": {"Port": 80}}, "NetworkInterfaceOptions may only be specified for Verified Access "
                                                    "Endpoints with network-interface type"),
        ({"RdsOptions": {"Port": 5432}}, "RdsOptions may only be specified for Verified Access Endpoints with rds type"),
    ):
        with pytest.raises(ClientError) as exc:
            ec2.modify_verified_access_endpoint(VerifiedAccessEndpointId=ep_id, **kwargs)
        assert _error(exc) == ("InvalidParameterCombination", message)

    with pytest.raises(ClientError) as exc:
        ec2.delete_verified_access_group(VerifiedAccessGroupId=fx["group"])
    assert _error(exc) == ("InvalidParameterValue", f"VerifiedAccessGroup {fx['group']} is not empty. "
                           "Please remove all VerifiedAccessEndpoints before deleting.")
    with pytest.raises(ClientError) as exc:
        ec2.delete_verified_access_instance(VerifiedAccessInstanceId=fx["instance"])
    assert _error(exc) == ("InvalidParameterValue", f"VerifiedAccessInstance {fx['instance']} is not empty. "
                           "Please remove all VerifiedAccessEndpoints before deleting.")
    with pytest.raises(ClientError) as exc:
        ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx))
    assert _error(exc) == ("InvalidParameterValue", f"ApplicationDomain provided is already in use by another endpoint in "
                           f"VerifiedAccessInstance {fx['instance']}. ApplicationDomain must be unique within a Verified Access Instance.")

    policy = ec2.get_verified_access_endpoint_policy(VerifiedAccessEndpointId=ep_id)
    assert (policy["PolicyEnabled"], policy["PolicyDocument"]) == (True, _POLICY)
    ec2.modify_verified_access_endpoint_policy(VerifiedAccessEndpointId=ep_id, PolicyEnabled=False)
    policy = ec2.get_verified_access_endpoint_policy(VerifiedAccessEndpointId=ep_id)
    assert (policy["PolicyEnabled"], "PolicyDocument" in policy) == (False, False)

    deleted = ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ep_id)["VerifiedAccessEndpoint"]
    assert deleted["Status"] == {"Code": "deleting"} and "Tags" not in deleted
    # Y5:*: a deleting endpoint answers NotFound to every call but Describe, and its group can go.
    for call in (lambda: ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ep_id),
                 lambda: ec2.modify_verified_access_endpoint(VerifiedAccessEndpointId=ep_id, Description="late"),
                 lambda: ec2.get_verified_access_endpoint_policy(VerifiedAccessEndpointId=ep_id)):
        with pytest.raises(ClientError) as exc:
            call()
        assert _error(exc) == ("InvalidVerifiedAccessEndpointId.NotFound",
                               f"The verified-access-endpoint ID '{ep_id}' does not exist")
    assert ec2.describe_verified_access_endpoints(
        VerifiedAccessEndpointIds=[ep_id])["VerifiedAccessEndpoints"][0]["Status"] == {"Code": "deleting"}
    with pytest.raises(ClientError) as exc:
        ec2.describe_verified_access_endpoints(VerifiedAccessEndpointIds=[ep_id])
    assert _error(exc) == ("InvalidVerifiedAccessEndpointId.NotFound", f"VerifiedAccessEndpoint {ep_id} does not exist")


def test_verified_access_endpoint_and_group_moves(ec2, endpoint_fixture):
    """Y4:move:missing-group, Y4:move:same-instance, Y4:move:other-instance, Y4:move:group-with-endpoint,
    Y4:detach:provider-with-endpoints, Y5:delete-group:*-while-endpoint-deleting."""
    fx = endpoint_fixture
    ep_id = ec2.create_verified_access_endpoint(**_lb_endpoint_params(fx))["VerifiedAccessEndpoint"]["VerifiedAccessEndpointId"]
    same = _group(ec2, fx["instance"])
    other_inst, other_tp = _instance_with_provider(ec2)
    other = _group(ec2, other_inst)
    move = ec2.modify_verified_access_endpoint
    with pytest.raises(ClientError) as exc:
        move(VerifiedAccessEndpointId=ep_id, VerifiedAccessGroupId=_MISSING["Group"])
    assert _error(exc) == ("InvalidVerifiedAccessGroupId.NotFound", f"The verified-access-group ID '{_MISSING['Group']}' does not exist")
    with pytest.raises(ClientError) as exc:
        move(VerifiedAccessEndpointId=ep_id, VerifiedAccessGroupId=other)
    assert _error(exc) == ("InvalidParameterValue", "Cannot move a VerifiedAccessEndpoint to a VerifiedAccessGroup that "
                           "belongs to a different VerifiedAccessInstance.")
    moved = move(VerifiedAccessEndpointId=ep_id, VerifiedAccessGroupId=same)["VerifiedAccessEndpoint"]
    assert (moved["VerifiedAccessGroupId"], moved["Status"]) == (same, {"Code": "updating"})
    with pytest.raises(ClientError) as exc:
        ec2.modify_verified_access_group(VerifiedAccessGroupId=same, VerifiedAccessInstanceId=other_inst)
    assert _error(exc) == ("InvalidParameterValue", f"VerifiedAccessGroup {same} is not empty. "
                           "Please remove all VerifiedAccessEndpoints before changing VerifiedAccessInstanceId.")
    with pytest.raises(ClientError) as exc:
        ec2.detach_verified_access_trust_provider(VerifiedAccessInstanceId=fx["instance"],
                                                  VerifiedAccessTrustProviderId=fx["provider"])
    assert _error(exc)[0] == "VerifiedAccessGroupAttachmentExists"
    ec2.delete_verified_access_endpoint(VerifiedAccessEndpointId=ep_id)
    ec2.delete_verified_access_group(VerifiedAccessGroupId=same)
    _cleanup(ec2, groups=[other], instances=[other_inst], providers=[other_tp])


def test_verified_access_network_interface_endpoint(ec2, endpoint_fixture):
    """V5:create:eni, V5:modify:port, V5:modify:protocol-http, V5:modify:lb-options-on-eni, V5:modify:port-ranges, V5:get-policy:none."""
    fx = endpoint_fixture
    ep = ec2.create_verified_access_endpoint(**_lb_endpoint_params(
        fx, EndpointType="network-interface", LoadBalancerOptions=None, ApplicationDomain="eni.example.com",
        EndpointDomainPrefix="eni", NetworkInterfaceOptions={"NetworkInterfaceId": fx["eni"], "Protocol": "https", "Port": 443},
    ))["VerifiedAccessEndpoint"]
    ep_id = ep["VerifiedAccessEndpointId"]
    assert ep["NetworkInterfaceOptions"] == {"NetworkInterfaceId": fx["eni"], "Port": 443, "Protocol": "https"}
    assert "LoadBalancerOptions" not in ep
    policy = ec2.get_verified_access_endpoint_policy(VerifiedAccessEndpointId=ep_id)
    assert (policy["PolicyEnabled"], "PolicyDocument" in policy) == (False, False)
    modified = ec2.modify_verified_access_endpoint(
        VerifiedAccessEndpointId=ep_id, NetworkInterfaceOptions={"Port": 8443, "Protocol": "http"})["VerifiedAccessEndpoint"]
    assert modified["NetworkInterfaceOptions"] == {"NetworkInterfaceId": fx["eni"], "Port": 8443, "Protocol": "http"}
    with pytest.raises(ClientError) as exc:
        ec2.modify_verified_access_endpoint(VerifiedAccessEndpointId=ep_id, LoadBalancerOptions={"Port": 443})
    assert _error(exc) == ("InvalidParameterCombination",
                           "LoadBalancerOptions may only be specified for Verified Access Endpoints with load-balancer type")
    with pytest.raises(ClientError) as exc:
        ec2.modify_verified_access_endpoint(VerifiedAccessEndpointId=ep_id,
                                            NetworkInterfaceOptions={"PortRanges": [{"FromPort": 443, "ToPort": 443}]})
    assert _error(exc) == ("InvalidParameterValue", "Parameter PortRanges is not currently supported for the operation")


# ── IAM ────────────────────────────────────────────────────


def test_verified_access_iam_resource_arns():
    """A14:*, E1:*, W7:deny:*, V3:deny:*, Z5:deny:*: the resource AWS names when it denies each call, the
    id's ARN for a resource that exists and <type>/* for one that does not."""
    from ministack.core.iam_actions import extract_resource_arn
    from ministack.services import ec2 as mod

    account, region = "123456789012", "us-east-1"
    for store, key in ((mod._va_instances, "vai-00000000000000001"), (mod._va_trust_providers, "vatp-00000000000000001"),
                       (mod._va_groups, "vagr-00000000000000001"), (mod._va_endpoints, "vae-00000000000000001"),
                       (mod._subnets, "subnet-00000001"), (mod._security_groups, "sg-00000001"),
                       (mod._network_interfaces, "eni-00000001")):
        store.set_scoped(account, region, key, {})
    mod._va_endpoints.set_scoped(account, region, "vae-00000000000000002", {"Status": "deleting"})

    def arn(params):
        query = {k: [v] for k, v in params.items()}
        return extract_resource_arn("ec2", "POST", "/", {}, b"", query, region, account)

    prefix = f"arn:aws:ec2:{region}:{account}:"
    vai, vatp, vagr, vae = "vai-00000000000000001", "vatp-00000000000000001", "vagr-00000000000000001", "vae-00000000000000001"
    try:
        for params, expected in (
            ({"Action": "CreateVerifiedAccessInstance"}, "verified-access-instance/*"),
            ({"Action": "CreateVerifiedAccessTrustProvider", "TrustProviderType": "user"}, "verified-access-trust-provider/*"),
            ({"Action": "DeleteVerifiedAccessInstance", "VerifiedAccessInstanceId": vai}, f"verified-access-instance/{vai}"),
            ({"Action": "DeleteVerifiedAccessInstance", "VerifiedAccessInstanceId": "vai-0000000000000000f"},
             "verified-access-instance/*"),
            ({"Action": "ModifyVerifiedAccessInstance", "VerifiedAccessInstanceId": "abc"}, "verified-access-instance/*"),
            ({"Action": "ModifyVerifiedAccessTrustProvider", "VerifiedAccessTrustProviderId": vatp},
             f"verified-access-trust-provider/{vatp}"),
            ({"Action": "AttachVerifiedAccessTrustProvider", "VerifiedAccessInstanceId": vai,
              "VerifiedAccessTrustProviderId": vatp}, f"verified-access-instance/{vai}"),
            ({"Action": "AttachVerifiedAccessTrustProvider", "VerifiedAccessInstanceId": "abc",
              "VerifiedAccessTrustProviderId": "vatp-0000000000000000f"}, "verified-access-trust-provider/*"),
            ({"Action": "CreateVerifiedAccessGroup", "VerifiedAccessInstanceId": vai}, f"verified-access-instance/{vai}"),
            ({"Action": "ModifyVerifiedAccessGroup", "VerifiedAccessGroupId": vagr}, f"verified-access-group/{vagr}"),
            ({"Action": "ModifyVerifiedAccessGroup", "VerifiedAccessGroupId": vagr, "VerifiedAccessInstanceId": vai},
             f"verified-access-instance/{vai}"),
            ({"Action": "GetVerifiedAccessGroupPolicy", "VerifiedAccessGroupId": vagr}, f"verified-access-group/{vagr}"),
            ({"Action": "ModifyVerifiedAccessEndpoint", "VerifiedAccessEndpointId": "vae-0000000000000000f",
              "VerifiedAccessGroupId": vagr}, "verified-access-endpoint/*"),
            ({"Action": "GetVerifiedAccessEndpointPolicy", "VerifiedAccessEndpointId": vae}, f"verified-access-endpoint/{vae}"),
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": vagr, "SecurityGroupId.1": "sg-1",
              "LoadBalancerOptions.SubnetId.1": "subnet-00000001"}, "subnet/subnet-00000001"),
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": vagr,
              "LoadBalancerOptions.SubnetId.1": "subnet-0000000f"}, f"verified-access-group/{vagr}"),
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": "vagr-0000000000000000f"},
             "verified-access-group/*"),
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": vagr, "SecurityGroupId.1": "sg-00000001"},
             "security-group/sg-00000001"),
            ({"Action": "ModifyVerifiedAccessGroup", "VerifiedAccessGroupId": vagr, "VerifiedAccessInstanceId": "abc"},
             f"verified-access-group/{vagr}"),
            ({"Action": "CreateVerifiedAccessEndpoint"}, "verified-access-endpoint/*"),
            # U2:deny:* (round 11), V2:deny:* (round 12): every subnet is scanned, then every security group.
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": vagr, "SecurityGroupId.1": "sg-00000001",
              "LoadBalancerOptions.SubnetId.1": "subnet-0000000f", "LoadBalancerOptions.SubnetId.2": "subnet-00000001"},
             "subnet/subnet-00000001"),
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": vagr, "SecurityGroupId.1": "sg-0000000f",
              "SecurityGroupId.2": "sg-00000001", "LoadBalancerOptions.SubnetId.1": "subnet-0000000f"},
             "security-group/sg-00000001"),
            # V2:deny:* (round 12): the first known security group at any index; with none known, the group.
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": vagr, "SecurityGroupId.1": "sg-00000001",
              "SecurityGroupId.2": "sg-0000000f", "LoadBalancerOptions.SubnetId.1": "subnet-0000000f"},
             "security-group/sg-00000001"),
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": vagr, "SecurityGroupId.1": "sg-0000000f",
              "LoadBalancerOptions.SubnetId.1": "subnet-0000000f"}, f"verified-access-group/{vagr}"),
            # X5:deny:eni-real+fake-sg-only (round 14): a known ENI with no known security group.
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": vagr, "SecurityGroupId.1": "sg-0000000f",
              "NetworkInterfaceOptions.NetworkInterfaceId": "eni-00000001"}, "network-interface/eni-00000001"),
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": vagr, "SecurityGroupId.1": "sg-00000001",
              "NetworkInterfaceOptions.NetworkInterfaceId": "eni-00000001"}, "security-group/sg-00000001"),
            # Z4:deny:lb-subnet+known-eni (round 16): a known subnet before a known ENI.
            ({"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": vagr, "LoadBalancerOptions.SubnetId.1": "subnet-00000001",
              "NetworkInterfaceOptions.NetworkInterfaceId": "eni-00000001"}, "subnet/subnet-00000001"),
            # U6:deny:* (round 11): a deleting endpoint is named like a missing one.
            ({"Action": "ModifyVerifiedAccessEndpoint", "VerifiedAccessEndpointId": "vae-00000000000000002"},
             "verified-access-endpoint/*"),
            ({"Action": "DeleteVerifiedAccessEndpoint", "VerifiedAccessEndpointId": "vae-00000000000000002"},
             "verified-access-endpoint/*"),
        ):
            assert arn(params) == prefix + expected, params
        assert arn({"Action": "DescribeVerifiedAccessGroups", "VerifiedAccessInstanceId": vai}) == "*"
    finally:
        for store, key in ((mod._va_instances, vai), (mod._va_trust_providers, vatp), (mod._va_groups, vagr),
                           (mod._va_endpoints, vae), (mod._va_endpoints, "vae-00000000000000002"),
                           (mod._subnets, "subnet-00000001"), (mod._security_groups, "sg-00000001"),
                           (mod._network_interfaces, "eni-00000001")):
            store.pop_scoped(account, region, key, None)


@pytest.mark.auth_contract
@pytest.mark.parametrize("permission", ["none", "allow", "other-resource", "explicit-deny"])
def test_verified_access_modify_permission_contract(auth_enabled, iam_user, permission):
    """W7:deny:modify-instance: ModifyVerifiedAccessInstance is authorized on the instance ARN."""
    admin = make_client("ec2")
    inst_id = _instance(admin, Description="before")
    resource = f"arn:aws:ec2:us-east-1:000000000000:verified-access-instance/{inst_id}"
    statements = []
    if permission != "none":
        target = resource.replace(inst_id, "vai-00000000000000000") if permission == "other-resource" else resource
        statements.append({"Effect": "Allow", "Action": "ec2:ModifyVerifiedAccessInstance", "Resource": target})
    if permission == "explicit-deny":
        statements.append({"Effect": "Deny", "Action": "ec2:ModifyVerifiedAccessInstance", "Resource": resource})
    policy = {"Version": "2012-10-17", "Statement": statements} if statements else None
    client = iam_user(policy=policy).client("ec2")
    try:
        if auth_enabled and permission != "allow":
            with pytest.raises(ClientError) as exc:
                client.modify_verified_access_instance(VerifiedAccessInstanceId=inst_id, Description="after")
            assert _error(exc)[0] == "UnauthorizedOperation"
            assert _status(exc) == 403
            expected = "before"
        else:
            client.modify_verified_access_instance(VerifiedAccessInstanceId=inst_id, Description="after")
            expected = "after"
        assert admin.describe_verified_access_instances(
            VerifiedAccessInstanceIds=[inst_id])["VerifiedAccessInstances"][0]["Description"] == expected
    finally:
        admin.delete_verified_access_instance(VerifiedAccessInstanceId=inst_id)


@pytest.mark.auth_contract
def test_verified_access_create_permission_contract(auth_enabled, iam_user):
    """V3:deny:create-instance: CreateVerifiedAccessInstance is authorized on verified-access-instance/*."""
    resource = "arn:aws:ec2:us-east-1:000000000000:verified-access-instance/*"
    allowed = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "ec2:CreateVerifiedAccessInstance", "Resource": resource},
        {"Effect": "Allow", "Action": "ec2:DeleteVerifiedAccessInstance", "Resource": "*"}]}).client("ec2")
    denied = iam_user().client("ec2")
    inst_id = _instance(allowed)
    allowed.delete_verified_access_instance(VerifiedAccessInstanceId=inst_id)
    if auth_enabled:
        with pytest.raises(ClientError) as exc:
            denied.create_verified_access_instance()
        assert (_status(exc), _error(exc)[0]) == (403, "UnauthorizedOperation")
    else:
        inst_id = _instance(denied)
        denied.delete_verified_access_instance(VerifiedAccessInstanceId=inst_id)


@pytest.mark.auth_contract
def test_verified_access_deny_on_each_authorized_resource_holds(auth_enabled, iam_user):
    """AWS authorizes a group move on the group and the instance, and a detach on the instance and the provider."""
    admin = make_client("ec2")
    inst_id, oidc_id = _instance_with_provider(admin)
    device_id = _device_provider(admin)["VerifiedAccessTrustProviderId"]
    _attach(admin, inst_id, device_id)
    group_id = _group(admin, inst_id, Description="before")
    prefix = "arn:aws:ec2:us-east-1:000000000000:"
    client = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "ec2:*", "Resource": "*"},
        {"Effect": "Deny", "Action": "ec2:ModifyVerifiedAccessGroup", "Resource": f"{prefix}verified-access-group/{group_id}"},
        {"Effect": "Deny", "Action": "ec2:DetachVerifiedAccessTrustProvider",
         "Resource": f"{prefix}verified-access-trust-provider/{device_id}"},
    ]}).client("ec2")
    try:
        calls = (
            lambda: client.modify_verified_access_group(VerifiedAccessGroupId=group_id, VerifiedAccessInstanceId=inst_id,
                                                        Description="after"),
            lambda: client.detach_verified_access_trust_provider(VerifiedAccessInstanceId=inst_id,
                                                                 VerifiedAccessTrustProviderId=device_id),
        )
        for call in calls:
            if auth_enabled:
                with pytest.raises(ClientError) as exc:
                    call()
                assert (_status(exc), _error(exc)[0]) == (403, "UnauthorizedOperation")
            else:
                call()
        group = admin.describe_verified_access_groups(VerifiedAccessGroupIds=[group_id])["VerifiedAccessGroups"][0]
        assert group["Description"] == ("before" if auth_enabled else "after")
        assert (device_id in _provider_ids(admin, inst_id)) is auth_enabled
    finally:
        _cleanup(admin, groups=[group_id], instances=[inst_id], providers=[oidc_id, device_id])


def test_verified_access_resource_arns_cover_every_authorized_resource():
    from ministack.core.iam_actions import verified_access_resource_arns

    prefix = "arn:aws:ec2:us-east-1:000000000000:"
    params = {"Action": "CreateVerifiedAccessEndpoint", "VerifiedAccessGroupId": "vagr-00000000000000009",
              "LoadBalancerOptions.SubnetId.1": "subnet-a", "LoadBalancerOptions.SubnetId.2": "subnet-b",
              "SecurityGroupId.1": "sg-a"}
    arns = verified_access_resource_arns(params, "us-east-1", "000000000000")
    assert set(arns) == {prefix + r for r in ("security-group/sg-a", "subnet/subnet-a", "subnet/subnet-b",
                                                   "verified-access-endpoint/*", "verified-access-group/*")}
    assert verified_access_resource_arns({"Action": "CreateVerifiedAccessGroup", "VerifiedAccessInstanceId": "vai-00000000000000009"},
                                         "us-east-1", "000000000000") == [
        prefix + "verified-access-instance/*", prefix + "verified-access-group/*"]
    assert verified_access_resource_arns({"Action": "DescribeVerifiedAccessGroups"}, "us-east-1", "000000000000") == []
