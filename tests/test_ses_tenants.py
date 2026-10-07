# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
"""SES tenant lifecycle, resource associations, and AWS-observed errors."""

import copy
import json
import re
import uuid

import pytest
from botocore.exceptions import ClientError

from ministack.core.responses import set_request_account_id, set_request_region


def _error(client, operation, code, message, **params):
    with pytest.raises(ClientError) as caught:
        getattr(client, operation)(**params)
    response = caught.value.response
    assert response["Error"] == {"Code": code, "Message": message}
    assert response["ResponseMetadata"]["HTTPStatusCode"] == (404 if code == "NotFoundException" else 400)


def test_tenant_configuration_set_association_lifecycle(sesv2):
    name = "tenant-" + uuid.uuid4().hex[:10]
    tags = [{"Key": "purpose", "Value": "regression"}]
    sesv2.create_configuration_set(ConfigurationSetName=name, Tags=tags)
    tenant = sesv2.create_tenant(TenantName=name, Tags=tags)
    assert re.fullmatch(r"tn-[a-f0-9]{29}", tenant["TenantId"])
    assert tenant["TenantArn"].endswith(f"tenant/{name}/{tenant['TenantId']}")
    assert tenant["SendingStatus"] == "ENABLED"
    assert tenant["Tags"] == tags
    assert sesv2.get_tenant(TenantName=name)["Tenant"] == {k: v for k, v in tenant.items() if k != "ResponseMetadata"}
    _error(
        sesv2,
        "create_tenant",
        "AlreadyExistsException",
        f"Tenant with name {name} already exists in account 000000000000",
        TenantName=name,
    )
    listed = sesv2.list_tenants(Filter={"TENANT_NAME_CONTAINS": name, "SENDING_STATUS": "ENABLED"})["Tenants"]
    assert listed == [{k: v for k, v in tenant.items() if k not in ("ResponseMetadata", "Tags")}]
    assert sesv2.list_tags_for_resource(ResourceArn=tenant["TenantArn"])["Tags"] == tags
    sesv2.tag_resource(ResourceArn=tenant["TenantArn"], Tags=[{"Key": "updated", "Value": "yes"}])
    sesv2.untag_resource(ResourceArn=tenant["TenantArn"], TagKeys=["purpose"])
    assert sesv2.get_tenant(TenantName=name)["Tenant"]["Tags"] == [{"Key": "updated", "Value": "yes"}]
    arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{name}"
    sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=arn)
    _error(
        sesv2,
        "create_tenant_resource_association",
        "AlreadyExistsException",
        f"Resources {arn} has already been associated with tenant {name}",
        TenantName=name,
        ResourceArn=arn,
    )
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == [
        {"ResourceType": "configuration-set", "ResourceArn": arn}
    ]
    inverse = sesv2.list_resource_tenants(ResourceArn=arn)["ResourceTenants"]
    assert inverse[0]["TenantId"] == tenant["TenantId"]
    assert inverse[0]["ResourceArn"] == arn
    _error(
        sesv2,
        "delete_configuration_set",
        "BadRequestException",
        f"Cannot delete <{arn}> because it has tenant associations. Remove all tenant associations and try again.",
        ConfigurationSetName=name,
    )
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=arn)
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=arn)
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == []
    sesv2.delete_tenant(TenantName=name)
    _error(sesv2, "get_tenant", "NotFoundException", f"The requested tenant <{name}> does not exist.", TenantName=name)
    _error(
        sesv2,
        "list_tags_for_resource",
        "NotFoundException",
        f"No Tenant present with name: {name}with tenantId: {tenant['TenantId']}",
        ResourceArn=tenant["TenantArn"],
    )
    for operation, params in [
        ("tag_resource", {"Tags": [{"Key": "purpose", "Value": "regression"}]}),
        ("untag_resource", {"TagKeys": ["purpose"]}),
    ]:
        _error(sesv2, operation, "NotFoundException", f"No Tenant present with name: {name}with tenantId: {tenant['TenantId']}",
               ResourceArn=tenant["TenantArn"], **params)
    sesv2.delete_configuration_set(ConfigurationSetName=name)


def test_tenant_invalid_and_missing_resources(sesv2):
    name = "tenant-errors-" + uuid.uuid4().hex[:10]
    _error(
        sesv2,
        "create_tenant",
        "BadRequestException",
        f"Invalid tenant name <{name} bad>: only alphanumeric ASCII characters, '_', and '-' are allowed.",
        TenantName=name + " bad",
    )
    _error(
        sesv2,
        "create_tenant_resource_association",
        "NotFoundException",
        f"The requested tenant <{name}> does not exist.",
        TenantName=name,
        ResourceArn="invalid-arn",
    )
    sesv2.create_tenant(TenantName=name)
    _error(
        sesv2,
        "create_tenant_resource_association",
        "BadRequestException",
        "Provided resource identifier is not an SES resource",
        TenantName=name,
        ResourceArn="invalid-arn",
    )
    arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{name}"
    _error(
        sesv2,
        "create_tenant_resource_association",
        "NotFoundException",
        f"Configuration set <{name}> does not exist:",
        TenantName=name,
        ResourceArn=arn,
    )
    other = arn.replace("us-east-1", "us-west-2")
    _error(
        sesv2,
        "create_tenant_resource_association",
        "BadRequestException",
        f"Resource <{other}> must be in the same region",
        TenantName=name,
        ResourceArn=other,
    )
    sesv2.delete_tenant(TenantName=name)


@pytest.mark.parametrize("filters,message", [
    ({"INVALID": "template"},
     "1 validation error detected: Value at 'filter' failed to satisfy constraint: Map keys must satisfy constraint: [Member must satisfy enum value set: [RESOURCE_TYPE]]"),
    ({"RESOURCE_TYPE": "bogus"}, "Invalid resource type bogus specified."),
    ({"RESOURCE_TYPE": "template,configuration-set"},
     "Invalid resource type template,configuration-set specified."),
])
def test_tenant_resource_filter_errors(sesv2, filters, message):
    name = "tenant-filter-" + uuid.uuid4().hex[:10]
    sesv2.create_tenant(TenantName=name)
    _error(sesv2, "list_tenant_resources", "BadRequestException", message,
           TenantName=name, Filter=filters)
    sesv2.delete_tenant(TenantName=name)


def test_tenant_sending_status_filter(sesv2):
    name = "tenant-status-" + uuid.uuid4().hex[:10]
    sesv2.create_tenant(TenantName=name)
    _error(sesv2, "list_tenants", "BadRequestException", "Invalid sending status <bogus>.",
           Filter={"TENANT_NAME_CONTAINS": name, "SENDING_STATUS": "bogus"})
    for status in ("REINSTATED", "DISABLED"):
        assert sesv2.list_tenants(Filter={"TENANT_NAME_CONTAINS": name, "SENDING_STATUS": status})["Tenants"] == []
    sesv2.delete_tenant(TenantName=name)


def test_tenant_template_association_and_delete_with_associations(sesv2):
    name = "tenant-template-" + uuid.uuid4().hex[:10]
    sesv2.create_email_template(TemplateName=name, TemplateContent={"Subject": "probe", "Text": "probe"})
    sesv2.create_configuration_set(ConfigurationSetName=name)
    template_arn = f"arn:aws:ses:us-east-1:000000000000:template/{name}"
    config_arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{name}"
    tenants = [name + suffix for suffix in ("-a", "-b")]
    for tenant_name in tenants:
        sesv2.create_tenant(TenantName=tenant_name)
        for arn in (config_arn, template_arn):
            sesv2.create_tenant_resource_association(TenantName=tenant_name, ResourceArn=arn)
        assert sesv2.list_tenant_resources(TenantName=tenant_name, Filter={"RESOURCE_TYPE": "template"})["TenantResources"] == [
            {"ResourceType": "template", "ResourceArn": template_arn}
        ]
        first = sesv2.list_tenant_resources(TenantName=tenant_name, PageSize=1)
        second = sesv2.list_tenant_resources(TenantName=tenant_name, PageSize=1, NextToken=first["NextToken"])
        assert first["TenantResources"] + second["TenantResources"] == [
            {"ResourceType": "configuration-set", "ResourceArn": config_arn},
            {"ResourceType": "template", "ResourceArn": template_arn},
        ]
        assert "NextToken" not in second
    first = sesv2.list_resource_tenants(ResourceArn=config_arn, PageSize=1)
    second = sesv2.list_resource_tenants(ResourceArn=config_arn, PageSize=1, NextToken=first["NextToken"])
    assert [item["TenantName"] for item in first["ResourceTenants"] + second["ResourceTenants"]] == tenants
    assert "NextToken" not in second
    sesv2.delete_tenant(TenantName=tenants[0])
    assert [item["TenantName"] for item in sesv2.list_resource_tenants(ResourceArn=config_arn)["ResourceTenants"]] == tenants[1:]
    _error(sesv2, "delete_configuration_set", "BadRequestException",
           f"Cannot delete <{config_arn}> because it has tenant associations. Remove all tenant associations and try again.",
           ConfigurationSetName=name)
    sesv2.delete_tenant(TenantName=tenants[1])
    assert sesv2.list_resource_tenants(ResourceArn=config_arn)["ResourceTenants"] == []
    sesv2.delete_configuration_set(ConfigurationSetName=name)
    sesv2.delete_email_template(TemplateName=name)


def test_tenant_state_roundtrip_and_account_region_isolation():
    from ministack.core.persistence import _json_default, _json_object_hook
    from ministack.services import ses_v2

    snapshot = copy.deepcopy(ses_v2.get_state())
    scopes = [
        ("000000000000", "us-east-1"),
        ("111111111111", "us-east-1"),
        ("000000000000", "us-west-2"),
    ]
    saved_tenants = {}
    try:
        for account, region in scopes:
            set_request_account_id(account)
            set_request_region(region)
            assert "same" not in ses_v2._tenants
            assert "same" not in ses_v2._tenant_resources
            status, _, body = ses_v2._tenant_request(
                "POST", "/tenants", {"TenantName": "same", "Tags": [{"Key": "region", "Value": region}]}
            )
            assert status == 200
            saved_tenants[(account, region)] = json.loads(body)
            arn = f"arn:aws:ses:{region}:{account}:configuration-set/same"
            ses_v2._config_sets["same"] = {"ConfigurationSetName": "same"}
            assert ses_v2._tenant_request(
                "POST", "/tenants/resources", {"TenantName": "same", "ResourceArn": arn}
            )[0] == 200

        encoded = json.dumps(ses_v2.get_state(), default=_json_default, sort_keys=True)
        saved = json.loads(encoded, object_hook=_json_object_hook)
        ses_v2.reset()
        for account, region in scopes:
            set_request_account_id(account)
            set_request_region(region)
            assert not ses_v2._tenants
            assert not ses_v2._tenant_resources
        ses_v2.load_persisted_state(saved)
        assert json.dumps(ses_v2.get_state(), default=_json_default, sort_keys=True) == encoded
        for account, region in scopes:
            set_request_account_id(account)
            set_request_region(region)
            tenant = saved_tenants[(account, region)]
            assert ses_v2._tenants["same"] == tenant
            assert ses_v2._ses_tags[tenant["TenantArn"]] == [{"Key": "region", "Value": region}]
            arn = f"arn:aws:ses:{region}:{account}:configuration-set/same"
            assert list(ses_v2._tenant_resources["same"]) == [arn]
            inverse = json.loads(ses_v2._tenant_request(
                "POST", "/resources/tenants/list", {"ResourceArn": arn}
            )[2])["ResourceTenants"]
            assert inverse == [{
                "TenantName": "same",
                "TenantId": tenant["TenantId"],
                "ResourceArn": arn,
                "AssociatedTimestamp": ses_v2._tenant_resources["same"][arn],
            }]
    finally:
        ses_v2.reset()
        ses_v2.load_persisted_state(snapshot)


def test_tenant_suppression_lifecycle(sesv2):
    name = "tenant-suppr-" + uuid.uuid4().hex[:10]
    attrs = {"SuppressedReasons": ["BOUNCE"], "SuppressionScope": "TENANT"}
    created = sesv2.create_tenant(TenantName=name, SuppressionAttributes=attrs)
    assert created["SuppressionAttributes"] == attrs
    assert sesv2.get_tenant(TenantName=name)["Tenant"]["SuppressionAttributes"] == attrs
    listed = sesv2.list_tenants(Filter={"TENANT_NAME_CONTAINS": name})["Tenants"]
    assert listed and "SuppressionAttributes" not in listed[0]
    updated = {"SuppressedReasons": ["BOUNCE", "COMPLAINT"], "SuppressionScope": "ACCOUNT"}
    sesv2.put_tenant_suppression_attributes(TenantName=name, **updated)
    assert sesv2.get_tenant(TenantName=name)["Tenant"]["SuppressionAttributes"] == updated
    sesv2.put_tenant_suppression_attributes(TenantName=name)
    assert "SuppressionAttributes" not in sesv2.get_tenant(TenantName=name)["Tenant"]
    sesv2.put_tenant_suppression_attributes(
        TenantName=name, SuppressedReasons=[], SuppressionScope="TENANT"
    )
    assert sesv2.get_tenant(TenantName=name)["Tenant"]["SuppressionAttributes"] == {
        "SuppressedReasons": [],
        "SuppressionScope": "TENANT",
    }
    sesv2.delete_tenant(TenantName=name)


def test_tenant_suppression_validation(sesv2):
    name = "tenant-supval-" + uuid.uuid4().hex[:10]
    create_reasons = (
        "1 validation error detected: Value at 'suppressionAttributes.suppressedReasons'"
        " failed to satisfy constraint: Member must satisfy constraint:"
        " [Member must satisfy enum value set: [BOUNCE, COMPLAINT]]"
    )
    create_scope = (
        "1 validation error detected: Value at 'suppressionAttributes.suppressionScope'"
        " failed to satisfy constraint: Member must satisfy enum value set: [TENANT, ACCOUNT]"
    )
    put_reasons = create_reasons.replace("suppressionAttributes.suppressedReasons", "suppressedReasons")
    put_scope = create_scope.replace("suppressionAttributes.suppressionScope", "suppressionScope")
    null_reasons = (
        "1 validation error detected: Value null at 'suppressionAttributes.suppressedReasons'"
        " failed to satisfy constraint: Member must not be null"
    )
    null_scope = (
        "1 validation error detected: Value null at 'suppressionAttributes.suppressionScope'"
        " failed to satisfy constraint: Member must not be null"
    )
    both_null = "2 validation errors detected: " + "; ".join(
        [null_reasons.split(": ", 1)[1], null_scope.split(": ", 1)[1]]
    )
    both_create = "2 validation errors detected: " + "; ".join(
        [create_reasons.split(": ", 1)[1], create_scope.split(": ", 1)[1]]
    )
    both_put = "2 validation errors detected: " + "; ".join(
        [put_reasons.split(": ", 1)[1], put_scope.split(": ", 1)[1]]
    )
    bad_attrs = {"SuppressedReasons": ["NOPE"], "SuppressionScope": "TENANT"}
    _error(sesv2, "create_tenant", "BadRequestException", create_reasons,
           TenantName=name, SuppressionAttributes=bad_attrs)
    _error(sesv2, "create_tenant", "BadRequestException", create_scope,
           TenantName=name,
           SuppressionAttributes={"SuppressedReasons": ["BOUNCE"], "SuppressionScope": "NOPE"})
    _error(sesv2, "create_tenant", "BadRequestException", both_create,
           TenantName=name,
           SuppressionAttributes={"SuppressedReasons": ["NOPE"], "SuppressionScope": "NOPE"})
    _error(sesv2, "create_tenant", "BadRequestException",
           "SuppressedReasons cannot be specified without SuppressionScope.",
           TenantName=name, SuppressionAttributes={"SuppressedReasons": ["BOUNCE"]})
    _error(sesv2, "create_tenant", "BadRequestException",
           "SuppressionScope cannot be specified without SuppressedReasons.",
           TenantName=name, SuppressionAttributes={"SuppressionScope": "TENANT"})
    _error(sesv2, "create_tenant", "BadRequestException", both_null,
           TenantName=name, SuppressionAttributes={})
    _error(sesv2, "create_tenant", "BadRequestException", null_scope,
           TenantName=name, SuppressionAttributes={"SuppressedReasons": []})
    # Precedence: model enum errors beat the name check, which beats pairing rules,
    # which beat the duplicate check.
    _error(sesv2, "create_tenant", "BadRequestException", create_reasons,
           TenantName="bad name", SuppressionAttributes=bad_attrs)
    _error(sesv2, "create_tenant", "BadRequestException",
           "Invalid tenant name <bad name>: only alphanumeric ASCII characters, '_', and '-' are allowed.",
           TenantName="bad name", SuppressionAttributes={"SuppressedReasons": ["BOUNCE"]})
    sesv2.create_tenant(TenantName=name)
    _error(sesv2, "create_tenant", "BadRequestException", create_reasons,
           TenantName=name, SuppressionAttributes=bad_attrs)
    _error(sesv2, "create_tenant", "BadRequestException",
           "SuppressedReasons cannot be specified without SuppressionScope.",
           TenantName=name, SuppressionAttributes={"SuppressedReasons": ["BOUNCE"]})
    _error(sesv2, "put_tenant_suppression_attributes", "BadRequestException", put_reasons,
           TenantName=name, SuppressedReasons=["NOPE"], SuppressionScope="TENANT")
    _error(sesv2, "put_tenant_suppression_attributes", "BadRequestException", put_scope,
           TenantName=name, SuppressedReasons=["BOUNCE"], SuppressionScope="NOPE")
    _error(sesv2, "put_tenant_suppression_attributes", "BadRequestException", both_put,
           TenantName=name, SuppressedReasons=["NOPE"], SuppressionScope="NOPE")
    _error(sesv2, "put_tenant_suppression_attributes", "BadRequestException",
           "SuppressedReasons cannot be specified without SuppressionScope.",
           TenantName=name, SuppressedReasons=["BOUNCE"])
    _error(sesv2, "put_tenant_suppression_attributes", "BadRequestException",
           "SuppressionScope is required when SuppressedReasons are provided. Valid values are: TENANT, ACCOUNT",
           TenantName=name, SuppressedReasons=[])
    _error(sesv2, "put_tenant_suppression_attributes", "BadRequestException",
           "SuppressionScope cannot be specified without SuppressedReasons.",
           TenantName=name, SuppressionScope="TENANT")
    missing = name + "-missing"
    _error(sesv2, "put_tenant_suppression_attributes", "BadRequestException", put_reasons,
           TenantName=missing, SuppressedReasons=["NOPE"], SuppressionScope="TENANT")
    _error(sesv2, "put_tenant_suppression_attributes", "BadRequestException",
           "SuppressionScope cannot be specified without SuppressedReasons.",
           TenantName=missing, SuppressionScope="TENANT")
    _error(sesv2, "put_tenant_suppression_attributes", "NotFoundException",
           f"The requested tenant <{missing}> does not exist.", TenantName=missing)
    sesv2.delete_tenant(TenantName=name)


def test_tenant_list_ordering(sesv2):
    prefix = "tenant-ord-" + uuid.uuid4().hex[:10]
    first, second = prefix + "-zz", prefix + "-aa"
    sesv2.create_tenant(TenantName=first)
    sesv2.create_tenant(TenantName=second)
    names = [
        t["TenantName"]
        for t in sesv2.list_tenants(Filter={"TENANT_NAME_CONTAINS": prefix})["Tenants"]
    ]
    assert names == [second, first]
    cs_b, cs_a = prefix + "-cs-b", prefix + "-cs-a"
    sesv2.create_configuration_set(ConfigurationSetName=cs_b)
    sesv2.create_configuration_set(ConfigurationSetName=cs_a)
    dom = prefix + ".example.com"
    sesv2.create_email_identity(EmailIdentity=dom)
    arns = [
        f"arn:aws:ses:us-east-1:000000000000:{kind}/{leaf}"
        for kind, leaf in [
            ("configuration-set", cs_b),
            ("identity", dom),
            ("configuration-set", cs_a),
        ]
    ]
    for arn in arns:
        sesv2.create_tenant_resource_association(TenantName=first, ResourceArn=arn)
    listed = sesv2.list_tenant_resources(TenantName=first)["TenantResources"]
    assert [item["ResourceArn"] for item in listed] == sorted(arns)
    for arn in arns:
        sesv2.delete_tenant_resource_association(TenantName=first, ResourceArn=arn)
    sesv2.delete_tenant(TenantName=first)
    sesv2.delete_tenant(TenantName=second)
    sesv2.delete_configuration_set(ConfigurationSetName=cs_b)
    sesv2.delete_configuration_set(ConfigurationSetName=cs_a)
    sesv2.delete_email_identity(EmailIdentity=dom)


def test_tenant_resource_tenants_keep_creation_order(sesv2):
    prefix = "tenant-rto-" + uuid.uuid4().hex[:10]
    cs = prefix + "-cs"
    sesv2.create_configuration_set(ConfigurationSetName=cs)
    arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{cs}"
    early, late = prefix + "-early", prefix + "-late"
    sesv2.create_tenant(TenantName=early)
    sesv2.create_tenant(TenantName=late)
    sesv2.create_tenant_resource_association(TenantName=late, ResourceArn=arn)
    sesv2.create_tenant_resource_association(TenantName=early, ResourceArn=arn)
    names = [
        item["TenantName"]
        for item in sesv2.list_resource_tenants(ResourceArn=arn)["ResourceTenants"]
    ]
    assert names == [early, late]
    sesv2.delete_tenant(TenantName=early)
    sesv2.delete_tenant(TenantName=late)
    sesv2.delete_configuration_set(ConfigurationSetName=cs)


def test_tenant_resource_delete_guards(sesv2, ses):
    prefix = "tenant-guard-" + uuid.uuid4().hex[:10]
    cs, tpl, dom = prefix + "-cs", prefix + "-tpl", prefix + ".example.com"
    sesv2.create_configuration_set(ConfigurationSetName=cs)
    sesv2.create_email_template(TemplateName=tpl, TemplateContent={"Subject": "s", "Text": "t"})
    sesv2.create_email_identity(EmailIdentity=dom)
    name = prefix + "-t"
    sesv2.create_tenant(TenantName=name)
    arns = {
        kind: f"arn:aws:ses:us-east-1:000000000000:{kind}/{leaf}"
        for kind, leaf in [("configuration-set", cs), ("template", tpl), ("identity", dom)]
    }
    for arn in arns.values():
        sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=arn)
    for operation, params in [
        ("delete_configuration_set", {"ConfigurationSetName": cs}),
        ("delete_email_template", {"TemplateName": tpl}),
        ("delete_email_identity", {"EmailIdentity": dom}),
    ]:
        kind = {"delete_configuration_set": "configuration-set",
                "delete_email_template": "template",
                "delete_email_identity": "identity"}[operation]
        _error(sesv2, operation, "BadRequestException",
               f"Cannot delete <{arns[kind]}> because it has tenant associations."
               " Remove all tenant associations and try again.",
               **params)
    for operation, params in [
        ("delete_configuration_set", {"ConfigurationSetName": cs}),
        ("delete_template", {"TemplateName": tpl}),
        ("delete_identity", {"Identity": dom}),
    ]:
        kind = {"delete_configuration_set": "configuration-set",
                "delete_template": "template",
                "delete_identity": "identity"}[operation]
        _error(ses, operation, "InvalidParameterValue",
               f"Cannot delete <{arns[kind]}> because it has tenant associations."
               " Remove all tenant associations and try again.",
               **params)
    for arn in arns.values():
        sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=arn)
    sesv2.delete_tenant(TenantName=name)
    sesv2.delete_configuration_set(ConfigurationSetName=cs)
    sesv2.delete_email_template(TemplateName=tpl)
    sesv2.delete_email_identity(EmailIdentity=dom)


def test_tenant_association_rejects_non_ses_arn(sesv2):
    name = "tenant-nonses-" + uuid.uuid4().hex[:10]
    sesv2.create_tenant(TenantName=name)
    _error(sesv2, "create_tenant_resource_association", "BadRequestException",
           "Provided ARN is not in SES resource ARN format",
           TenantName=name, ResourceArn=f"arn:aws:s3:::{name}-bucket")
    _error(sesv2, "list_tags_for_resource", "NotFoundException",
           f"No Tenant present with name: nullwith tenantId: {name}",
           ResourceArn=f"arn:aws:ses:us-east-1:000000000000:tenant/{name}")
    sesv2.delete_tenant(TenantName=name)


def test_tenant_list_pagesize_bounds(sesv2):
    name = "tenant-pages-" + uuid.uuid4().hex[:10]
    sesv2.create_tenant(TenantName=name)
    _error(sesv2, "list_tenants", "BadRequestException",
           "1 validation error detected: Value '0' at 'pageSize' failed to satisfy"
           " constraint: Member must have value greater than or equal to 1",
           PageSize=0)
    over = ("1 validation error detected: Value '101' at 'pageSize' failed to satisfy"
            " constraint: Member must have value less than or equal to 100")
    _error(sesv2, "list_tenants", "BadRequestException", over, PageSize=101)
    _error(sesv2, "list_tenant_resources", "BadRequestException", over,
           TenantName=name, PageSize=101)
    cs = name + "-cs"
    sesv2.create_configuration_set(ConfigurationSetName=cs)
    arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{cs}"
    _error(sesv2, "list_resource_tenants", "BadRequestException", over,
           ResourceArn=arn, PageSize=101)
    sesv2.delete_tenant(TenantName=name)
    sesv2.delete_configuration_set(ConfigurationSetName=cs)


def test_tenant_v1_resources_can_be_associated(sesv2, ses):
    prefix = "tenant-v1res-" + uuid.uuid4().hex[:10]
    cs, dom = prefix + "-cs", prefix + ".example.com"
    ses.create_configuration_set(ConfigurationSet={"Name": cs})
    ses.verify_domain_identity(Domain=dom)
    name = prefix + "-t"
    sesv2.create_tenant(TenantName=name)
    cs_arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{cs}"
    idn_arn = f"arn:aws:ses:us-east-1:000000000000:identity/{dom}"
    sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=cs_arn)
    sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=idn_arn)
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == [
        {"ResourceType": "configuration-set", "ResourceArn": cs_arn},
        {"ResourceType": "identity", "ResourceArn": idn_arn},
    ]
    blocked = "because it has tenant associations. Remove all tenant associations and try again."
    _error(sesv2, "delete_configuration_set", "BadRequestException",
           f"Cannot delete <{cs_arn}> {blocked}", ConfigurationSetName=cs)
    _error(sesv2, "delete_email_identity", "BadRequestException",
           f"Cannot delete <{idn_arn}> {blocked}", EmailIdentity=dom)
    _error(ses, "delete_configuration_set", "InvalidParameterValue",
           f"Cannot delete <{cs_arn}> {blocked}", ConfigurationSetName=cs)
    _error(ses, "delete_identity", "InvalidParameterValue",
           f"Cannot delete <{idn_arn}> {blocked}", Identity=dom)
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=cs_arn)
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=idn_arn)
    sesv2.delete_tenant(TenantName=name)
    ses.delete_configuration_set(ConfigurationSetName=cs)
    ses.delete_identity(Identity=dom)


def test_tenant_association_normalizes_foreign_partitions(sesv2):
    prefix = "tenant-part-" + uuid.uuid4().hex[:10]
    cs = prefix + "-cs"
    sesv2.create_configuration_set(ConfigurationSetName=cs)
    name = prefix + "-t"
    tenant = sesv2.create_tenant(TenantName=name)
    aws_arn = f"arn:aws:ses:us-east-1:000000000000:configuration-set/{cs}"
    cn_arn = f"arn:aws-cn:ses:us-east-1:000000000000:configuration-set/{cs}"
    sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=cn_arn)
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == [
        {"ResourceType": "configuration-set", "ResourceArn": aws_arn}
    ]
    _error(sesv2, "create_tenant_resource_association", "AlreadyExistsException",
           f"Resources {aws_arn} has already been associated with tenant {name}",
           TenantName=name, ResourceArn=aws_arn)
    inverse = sesv2.list_resource_tenants(ResourceArn=cn_arn)["ResourceTenants"]
    assert [(item["TenantName"], item["ResourceArn"]) for item in inverse] == [(name, aws_arn)]
    _error(sesv2, "delete_configuration_set", "BadRequestException",
           f"Cannot delete <{aws_arn}> because it has tenant associations."
           " Remove all tenant associations and try again.",
           ConfigurationSetName=cs)
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=aws_arn)
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == []
    sesv2.create_tenant_resource_association(TenantName=name, ResourceArn=aws_arn)
    sesv2.delete_tenant_resource_association(TenantName=name, ResourceArn=cn_arn)
    assert sesv2.list_tenant_resources(TenantName=name)["TenantResources"] == []
    xacct = f"arn:aws-cn:ses:us-east-1:111122223333:configuration-set/{cs}"
    _error(sesv2, "create_tenant_resource_association", "BadRequestException",
           f"Resource <{xacct}> must be in the same account",
           TenantName=name, ResourceArn=xacct)
    cn_tenant = tenant["TenantArn"].replace("arn:aws:", "arn:aws-cn:", 1)
    sesv2.tag_resource(ResourceArn=cn_tenant, Tags=[{"Key": "pk", "Value": "v"}])
    assert sesv2.list_tags_for_resource(ResourceArn=tenant["TenantArn"])["Tags"] == [
        {"Key": "pk", "Value": "v"}
    ]
    sesv2.tag_resource(ResourceArn=cn_arn, Tags=[{"Key": "ck", "Value": "v"}])
    assert sesv2.list_tags_for_resource(ResourceArn=aws_arn)["Tags"] == [
        {"Key": "ck", "Value": "v"}
    ]
    sesv2.delete_tenant(TenantName=name)
    sesv2.delete_configuration_set(ConfigurationSetName=cs)
