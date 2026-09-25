"""CloudFormation updates of AppSync GraphQL APIs and API keys, in place and rolled back."""

import json
import time
import uuid as _uuid_mod

from test_cfn import (
    _FAILING_RESOURCE,
    _cfn_appsync_members_template,
    _cfn_output,
    _delete_cfn_test_stack,
    _wait_stack,
)


def test_cfn_appsync_api_update_keeps_id_and_children(cfn, appsync):
    """Every property of AWS::AppSync::GraphQLApi is No interruption, so the
    API keeps its id. The create-fallback minted a new one and re-seeded the
    api-key, data-source, resolver and type stores under it, so the renamed
    API came back empty and the real one was orphaned."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-upd-{suffix}"

    def template(name):
        return json.dumps({
            "Resources": {
                "Api": {
                    "Type": "AWS::AppSync::GraphQLApi",
                    "Properties": {"Name": name, "AuthenticationType": "API_KEY"},
                },
                "DS": {
                    "Type": "AWS::AppSync::DataSource",
                    "Properties": {
                        "ApiId": {"Fn::GetAtt": ["Api", "ApiId"]},
                        "Name": "probe_source",
                        "Type": "NONE",
                    },
                },
            },
            "Outputs": {"ApiId": {"Value": {"Fn::GetAtt": ["Api", "ApiId"]}}},
        })

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template(f"api_{suffix}_a"))
        _wait_stack(cfn, stack_name)
        api_id = next(
            o["OutputValue"]
            for o in cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
            if o["OutputKey"] == "ApiId")
        assert appsync.get_data_source(
            apiId=api_id, name="probe_source")["dataSource"]["name"] == "probe_source"

        cfn.update_stack(StackName=stack_name,
                         TemplateBody=template(f"api_{suffix}_b"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        api = appsync.get_graphql_api(apiId=api_id)["graphqlApi"]
        assert api["name"] == f"api_{suffix}_b"
        # The data source is the evidence the children survived: the create
        # re-seeded _data_sources for the new api id.
        assert appsync.get_data_source(
            apiId=api_id, name="probe_source")["dataSource"]["name"] == "probe_source"
        assert len([a for a in appsync.list_graphql_apis()["graphqlApis"]
                    if a["name"].startswith(f"api_{suffix}")]) == 1
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_appsync_api_key_updates_in_place(cfn, appsync):
    """Description and Expires are No interruption on AWS::AppSync::ApiKey.
    The create mints a fresh key id, so under the fallback every expiry change
    handed out a new key and left the previous one valid on the API."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-key-{suffix}"
    expiry = (int(time.time()) + 7 * 86400) // 3600 * 3600

    def template(description, expires):
        return json.dumps({
            "Resources": {
                "Api": {
                    "Type": "AWS::AppSync::GraphQLApi",
                    "Properties": {"Name": f"keyapi_{suffix}",
                                   "AuthenticationType": "API_KEY"},
                },
                "Key": {
                    "Type": "AWS::AppSync::ApiKey",
                    "Properties": {
                        "ApiId": {"Fn::GetAtt": ["Api", "ApiId"]},
                        "Description": description,
                        "Expires": expires,
                    },
                },
            },
            "Outputs": {
                "ApiId": {"Value": {"Fn::GetAtt": ["Api", "ApiId"]}},
                "KeyId": {"Value": {"Fn::GetAtt": ["Key", "ApiKeyId"]}},
            },
        })

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("before", expiry))
        _wait_stack(cfn, stack_name)
        outputs = {o["OutputKey"]: o["OutputValue"] for o in
                   cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]}
        api_id, key_id = outputs["ApiId"], outputs["KeyId"]

        cfn.update_stack(StackName=stack_name,
                         TemplateBody=template("after", expiry + 3600))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        keys = appsync.list_api_keys(apiId=api_id)["apiKeys"]
        assert len(keys) == 1, "the update left the previous key on the API"
        assert keys[0]["id"] == key_id
        assert keys[0]["description"] == "after"
        assert keys[0]["expires"] == expiry + 3600
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _cfn_appsync_api_template(name, providers=None, with_bad=False):
    props = {"Name": name, "AuthenticationType": "API_KEY"}
    if providers is not None:
        props["AdditionalAuthenticationProviders"] = providers
    resources = {"Api": {"Type": "AWS::AppSync::GraphQLApi", "Properties": props}}
    if with_bad:
        resources["Bad"] = {**_FAILING_RESOURCE, "DependsOn": "Api"}
    return json.dumps({
        "Resources": resources,
        "Outputs": {"ApiId": {"Value": {"Fn::GetAtt": ["Api", "ApiId"]}}},
    })


def test_cfn_appsync_additional_auth_provider_added_by_update_reads_back(cfn, appsync):
    """AdditionalAuthenticationProviders is No interruption. AppSync is a
    rest-json API, so the stored list has to carry the API's camelCase: kept
    in the template's PascalCase, botocore read back [{}] and the API's auth
    check never saw AWS_IAM. Measured on AWS 2026-09-21: the added provider
    reads back as [{"authenticationType": "AWS_IAM"}]."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-auth-upd-{suffix}"
    cfn.create_stack(StackName=stack_name,
                     TemplateBody=_cfn_appsync_api_template(f"auth_{suffix}"))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        api_id = _cfn_output(cfn, stack_name, "ApiId")

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_appsync_api_template(
            f"auth_{suffix}", [{"AuthenticationType": "AWS_IAM"}]))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        api = appsync.get_graphql_api(apiId=api_id)["graphqlApi"]
        assert api["additionalAuthenticationProviders"] == [
            {"authenticationType": "AWS_IAM"}]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_appsync_additional_auth_providers_at_create_read_back(cfn, appsync):
    """The create stores the list in the API's shape too, nested configuration
    members included."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-auth-new-{suffix}"
    providers = [
        {"AuthenticationType": "AWS_IAM"},
        {"AuthenticationType": "OPENID_CONNECT",
         "OpenIDConnectConfig": {"Issuer": "https://issuer.example.com",
                                 "ClientId": "client-1", "IatTTL": 60}},
    ]
    cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_appsync_api_template(
        f"auth_{suffix}", providers))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        api = appsync.get_graphql_api(
            apiId=_cfn_output(cfn, stack_name, "ApiId"))["graphqlApi"]
        assert api["additionalAuthenticationProviders"] == [
            {"authenticationType": "AWS_IAM"},
            {"authenticationType": "OPENID_CONNECT",
             "openIDConnectConfig": {"issuer": "https://issuer.example.com",
                                     "clientId": "client-1", "iatTTL": 60}},
        ]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_appsync_api_rename_is_rolled_back(cfn, appsync):
    """A rename is applied in place, so a later failure in the same update has
    to rename the API back. Measured on AWS 2026-09-21: the API reads its old
    name after UPDATE_ROLLBACK_COMPLETE, under the same id."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-rb-{suffix}"
    cfn.create_stack(StackName=stack_name,
                     TemplateBody=_cfn_appsync_api_template(f"rb_{suffix}_a"))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        api_id = _cfn_output(cfn, stack_name, "ApiId")

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_appsync_api_template(
            f"rb_{suffix}_b", with_bad=True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_output(cfn, stack_name, "ApiId") == api_id
        assert appsync.get_graphql_api(apiId=api_id)["graphqlApi"]["name"] == f"rb_{suffix}_a"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_appsync_additional_auth_provider_removed_reads_absent(cfn, appsync):
    """A template that stops declaring AdditionalAuthenticationProviders
    leaves the API without the member: AWS answers it absent, not as an empty
    list (measured 2026-09-21, same api id)."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-auth-rm-{suffix}"
    cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_appsync_api_template(
        f"auth_{suffix}", [{"AuthenticationType": "AWS_IAM"}]))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        cfn.update_stack(StackName=stack_name,
                         TemplateBody=_cfn_appsync_api_template(f"auth_{suffix}"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        api = appsync.get_graphql_api(
            apiId=_cfn_output(cfn, stack_name, "ApiId"))["graphqlApi"]
        assert "additionalAuthenticationProviders" not in api
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_appsync_api_members_apply_change_and_reset(cfn, appsync):
    """The GraphQLApi members a template sets reach the API at create, change
    in place on update and go back to AppSync's defaults when removed, as
    measured on AWS 2026-09-21. The provisioner stored only the name, the
    authentication type and the extra providers, so XrayEnabled,
    QueryDepthLimit, ResolverCountLimit, IntrospectionConfig,
    EnvironmentVariables, OwnerContact and OpenIDConnectConfig were accepted
    and never readable; a ClientId or Issuer change is in place too."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-members-{suffix}"
    name = f"members_{suffix}"
    oidc = {"Issuer": "https://issuer.example.com", "ClientId": "c1"}

    def members(api_id):
        api = appsync.get_graphql_api(apiId=api_id)["graphqlApi"]
        return {key: api.get(key) for key in (
            "xrayEnabled", "queryDepthLimit", "resolverCountLimit",
            "introspectionConfig", "ownerContact", "visibility", "apiType")}

    def env(api_id):
        return appsync.get_graphql_api_environment_variables(
            apiId=api_id)["environmentVariables"]

    cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_appsync_members_template(name, {
        "XrayEnabled": True, "QueryDepthLimit": 5, "ResolverCountLimit": 10,
        "IntrospectionConfig": "DISABLED", "EnvironmentVariables": {"K1": "v1"},
        "OwnerContact": "owner-a"}, oidc))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        api_id = _cfn_output(cfn, stack_name, "ApiId")
        oidc_id = _cfn_output(cfn, stack_name, "OidcId")
        assert members(api_id) == {
            "xrayEnabled": True, "queryDepthLimit": 5, "resolverCountLimit": 10,
            "introspectionConfig": "DISABLED", "ownerContact": "owner-a",
            "visibility": "GLOBAL", "apiType": "GRAPHQL"}
        assert env(api_id) == {"K1": "v1"}
        assert appsync.get_graphql_api(apiId=oidc_id)["graphqlApi"]["openIDConnectConfig"] == {
            "issuer": "https://issuer.example.com", "clientId": "c1", "authTTL": 0, "iatTTL": 0}

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_appsync_members_template(name, {
            "XrayEnabled": False, "QueryDepthLimit": 6, "ResolverCountLimit": 11,
            "IntrospectionConfig": "ENABLED", "EnvironmentVariables": {"K1": "v2", "K2": "x"},
            "OwnerContact": "owner-b"}, {**oidc, "ClientId": "c2"}))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert _cfn_output(cfn, stack_name, "ApiId") == api_id
        assert _cfn_output(cfn, stack_name, "OidcId") == oidc_id
        assert members(api_id) == {
            "xrayEnabled": False, "queryDepthLimit": 6, "resolverCountLimit": 11,
            "introspectionConfig": "ENABLED", "ownerContact": "owner-b",
            "visibility": "GLOBAL", "apiType": "GRAPHQL"}
        assert env(api_id) == {"K1": "v2", "K2": "x"}
        assert appsync.get_graphql_api(
            apiId=oidc_id)["graphqlApi"]["openIDConnectConfig"]["clientId"] == "c2"

        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_appsync_members_template(
            name, {}, {**oidc, "ClientId": "c2"}))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert members(api_id) == {
            "xrayEnabled": False, "queryDepthLimit": 0, "resolverCountLimit": 0,
            "introspectionConfig": "ENABLED", "ownerContact": None,
            "visibility": "GLOBAL", "apiType": "GRAPHQL"}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_appsync_api_visibility_change_fails_the_update(cfn, appsync):
    """Visibility can only be set when the API is created. AWS fails the
    update with the handler's message and rolls it back (measured 2026-09-21),
    where the provisioner used to accept the change and keep GLOBAL."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-vis-{suffix}"
    oidc = {"Issuer": "https://issuer.example.com", "ClientId": "c1"}
    cfn.create_stack(StackName=stack_name, TemplateBody=_cfn_appsync_members_template(
        f"vis_{suffix}", {}, oidc))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        api_id = _cfn_output(cfn, stack_name, "ApiId")
        cfn.update_stack(StackName=stack_name, TemplateBody=_cfn_appsync_members_template(
            f"vis_{suffix}", {"Visibility": "PRIVATE"}, oidc))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
        reasons = [e.get("ResourceStatusReason", "")
                   for e in cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
                   if e["LogicalResourceId"] == "Api" and e["ResourceStatus"] == "UPDATE_FAILED"]
        assert reasons and "Property Visibility can only be set when creating a GraphQL API" in reasons[0]
        assert _cfn_output(cfn, stack_name, "ApiId") == api_id
        assert appsync.get_graphql_api(apiId=api_id)["graphqlApi"]["visibility"] == "GLOBAL"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_appsync_api_tags_take_the_stack_tags(cfn, appsync):
    """A GraphQL API carries its template Tags and the stack-level tags, and a
    stack-tag change reaches it; AppSync lists no aws:cloudformation:* tag on
    it (measured on AWS 2026-09-21). The provisioner stored no tag at all,
    and its eight-character api id made an ARN botocore refuses as too short
    for ListTagsForResource."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-tags-{suffix}"
    template = json.dumps({
        "Resources": {"Api": {"Type": "AWS::AppSync::GraphQLApi", "Properties": {
            "Name": f"tags_{suffix}", "AuthenticationType": "API_KEY",
            "Tags": [{"Key": "own", "Value": "a"}]}}},
        "Outputs": {"Arn": {"Value": {"Fn::GetAtt": ["Api", "Arn"]}}},
    })
    cfn.create_stack(StackName=stack_name, TemplateBody=template,
                     Tags=[{"Key": "stage", "Value": "one"}])
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        arn = _cfn_output(cfn, stack_name, "Arn")
        assert appsync.list_tags_for_resource(resourceArn=arn)["tags"] == {
            "own": "a", "stage": "one"}
        cfn.update_stack(StackName=stack_name, UsePreviousTemplate=True,
                         Tags=[{"Key": "stage", "Value": "two"}])
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert appsync.list_tags_for_resource(resourceArn=arn)["tags"] == {
            "own": "a", "stage": "two"}
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
