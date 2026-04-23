import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

_EXECUTE_PORT = urlparse(_endpoint).port or 4566

def test_apigwv1_create_rest_api(apigw_v1):
    """CreateRestApi returns id, name, and createdDate as datetime."""
    import datetime

    resp = apigw_v1.create_rest_api(name="v1-create-test")
    assert "id" in resp
    assert resp["name"] == "v1-create-test"
    assert "createdDate" in resp
    assert isinstance(resp["createdDate"], datetime.datetime), "createdDate must be a datetime, not a float"
    apigw_v1.delete_rest_api(restApiId=resp["id"])

def test_apigwv1_get_rest_api(apigw_v1):
    """GetRestApi returns the created API."""
    api_id = apigw_v1.create_rest_api(name="v1-get-test")["id"]
    resp = apigw_v1.get_rest_api(restApiId=api_id)
    assert resp["id"] == api_id
    assert resp["name"] == "v1-get-test"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_get_rest_apis(apigw_v1):
    """GetRestApis returns item list containing created APIs."""
    id1 = apigw_v1.create_rest_api(name="v1-list-a")["id"]
    id2 = apigw_v1.create_rest_api(name="v1-list-b")["id"]
    resp = apigw_v1.get_rest_apis()
    ids = [a["id"] for a in resp["items"]]
    assert id1 in ids
    assert id2 in ids
    apigw_v1.delete_rest_api(restApiId=id1)
    apigw_v1.delete_rest_api(restApiId=id2)

def test_apigwv1_get_account_defaults_and_update_roundtrip(apigw_v1):
    """GetAccount returns AWS-shaped defaults; UpdateAccount with /cloudwatchRoleArn
    patch op round-trips. Single test to avoid cross-test state bleed on the
    singleton per-account /account settings."""
    # Clear any prior state on the singleton (singleton per account, shared across tests)
    apigw_v1.update_account(patchOperations=[
        {"op": "replace", "path": "/cloudwatchRoleArn", "value": ""}
    ])

    resp = apigw_v1.get_account()
    assert resp["throttleSettings"] == {"burstLimit": 5000, "rateLimit": 10000}
    assert resp["features"] == ["UsagePlans"]
    assert resp["apiKeyVersion"] == "4"

    # Mirrors Terraform aws_api_gateway_account: single cloudwatchRoleArn replace.
    role_arn = "arn:aws:iam::000000000000:role/apigw-cloudwatch-test"
    apigw_v1.update_account(patchOperations=[
        {"op": "replace", "path": "/cloudwatchRoleArn", "value": role_arn}
    ])
    resp = apigw_v1.get_account()
    assert resp["cloudwatchRoleArn"] == role_arn
    # Defaults still present after the patch
    assert resp["throttleSettings"] == {"burstLimit": 5000, "rateLimit": 10000}


def test_apigwv1_rest_api_policy_terraform_roundtrip(apigw_v1):
    """GetRestApi must return `policy` JSON-string-escape-encoded, matching
    real AWS. Terraform-provider-aws's flattenAPIPolicy wraps the SDK-decoded
    policy in outer quotes and re-parses as JSON; if ministack returns the
    raw policy (unescaped) the provider fails with
    ``invalid character 'S' after top-level value``. Regression for #430."""
    import urllib.request
    raw_policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"execute-api:Invoke","Resource":"*"}]}'
    api_id = apigw_v1.create_rest_api(name="v1-policy-roundtrip", policy=raw_policy)["id"]
    try:
        # What the AWS SDK v2 deserializer hands to the Terraform provider
        # is the outer-JSON-decoded string. Fetch it raw (bypass botocore
        # which may further manipulate the field).
        endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
        with urllib.request.urlopen(f"{endpoint}/restapis/{api_id}") as r:
            body = json.loads(r.read().decode())
        sdk_decoded_policy = body["policy"]

        # The Terraform provider does:
        #   NormalizeJsonString(`"` + *out.Policy + `"`) -> strconv.Unquote
        # which is equivalent to: json.loads('"' + policy + '"')
        recovered = json.loads('"' + sdk_decoded_policy + '"')
        assert recovered == raw_policy, f"provider roundtrip lost fidelity: {recovered!r} vs {raw_policy!r}"
    finally:
        apigw_v1.delete_rest_api(restApiId=api_id)


def test_apigwv1_update_rest_api(apigw_v1):
    """UpdateRestApi (PATCH) modifies the API name."""
    api_id = apigw_v1.create_rest_api(name="v1-update-before")["id"]
    apigw_v1.update_rest_api(
        restApiId=api_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-update-after"}],
    )
    resp = apigw_v1.get_rest_api(restApiId=api_id)
    assert resp["name"] == "v1-update-after"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_rest_api(apigw_v1):
    """DeleteRestApi removes the API; subsequent GetRestApi raises."""
    api_id = apigw_v1.create_rest_api(name="v1-delete-test")["id"]
    apigw_v1.delete_rest_api(restApiId=api_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_rest_api(restApiId=api_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_apigwv1_create_resource(apigw_v1):
    """CreateResource creates a child resource with computed path."""
    api_id = apigw_v1.create_rest_api(name="v1-resource-create")["id"]
    # Get root resource id
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resp = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="users",
    )
    assert resp["pathPart"] == "users"
    assert resp["path"] == "/users"
    assert "id" in resp
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_get_resources(apigw_v1):
    """GetResources returns the root resource plus any created children."""
    api_id = apigw_v1.create_rest_api(name="v1-get-resources")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.create_resource(restApiId=api_id, parentId=root["id"], pathPart="items")
    resources = apigw_v1.get_resources(restApiId=api_id)["items"]
    paths = [r["path"] for r in resources]
    assert "/" in paths
    assert "/items" in paths
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_put_get_method(apigw_v1):
    """PutMethod creates a method; GetMethod returns it."""
    api_id = apigw_v1.create_rest_api(name="v1-method-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="ping",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    resp = apigw_v1.get_method(restApiId=api_id, resourceId=resource_id, httpMethod="GET")
    assert resp["httpMethod"] == "GET"
    assert resp["authorizationType"] == "NONE"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_put_integration(apigw_v1):
    """PutIntegration sets AWS_PROXY integration on a method."""
    api_id = apigw_v1.create_rest_api(name="v1-integration-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="ping",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    resp = apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:myFunc/invocations",
    )
    assert resp["type"] == "AWS_PROXY"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_put_method_response(apigw_v1):
    """PutMethodResponse sets a 200 method response."""
    api_id = apigw_v1.create_rest_api(name="v1-method-response-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="things",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    resp = apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
    )
    assert resp["statusCode"] == "200"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_put_integration_response(apigw_v1):
    """PutIntegrationResponse sets a 200 integration response."""
    api_id = apigw_v1.create_rest_api(name="v1-int-response-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="things",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="MOCK",
        integrationHttpMethod="POST",
        uri="",
    )
    resp = apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
    )
    assert resp["statusCode"] == "200"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_create_deployment(apigw_v1):
    """CreateDeployment returns a deployment with id and createdDate."""
    api_id = apigw_v1.create_rest_api(name="v1-deployment-test")["id"]
    resp = apigw_v1.create_deployment(restApiId=api_id, description="initial deployment")
    assert "id" in resp
    assert "createdDate" in resp
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_create_stage(apigw_v1):
    """CreateStage creates a named stage linked to a deployment."""
    api_id = apigw_v1.create_rest_api(name="v1-stage-test")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    resp = apigw_v1.create_stage(
        restApiId=api_id,
        stageName="prod",
        deploymentId=dep_id,
    )
    assert resp["stageName"] == "prod"
    assert resp["deploymentId"] == dep_id
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_update_stage(apigw_v1):
    """UpdateStage (PATCH) updates stage variables."""
    api_id = apigw_v1.create_rest_api(name="v1-stage-update")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="dev", deploymentId=dep_id)
    apigw_v1.update_stage(
        restApiId=api_id,
        stageName="dev",
        patchOperations=[{"op": "replace", "path": "/variables/myVar", "value": "myVal"}],
    )
    resp = apigw_v1.get_stage(restApiId=api_id, stageName="dev")
    assert resp["variables"]["myVar"] == "myVal"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_authorizer_crud(apigw_v1):
    """Authorizer full lifecycle: create, get, update (patch), delete."""
    api_id = apigw_v1.create_rest_api(name="v1-auth-crud")["id"]
    auth = apigw_v1.create_authorizer(
        restApiId=api_id,
        name="my-auth",
        type="TOKEN",
        authorizerUri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:auth/invocations",
        identitySource="method.request.header.Authorization",
    )
    auth_id = auth["id"]
    assert auth["name"] == "my-auth"

    got = apigw_v1.get_authorizer(restApiId=api_id, authorizerId=auth_id)
    assert got["id"] == auth_id

    apigw_v1.update_authorizer(
        restApiId=api_id,
        authorizerId=auth_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "renamed-auth"}],
    )
    got2 = apigw_v1.get_authorizer(restApiId=api_id, authorizerId=auth_id)
    assert got2["name"] == "renamed-auth"

    listed = apigw_v1.get_authorizers(restApiId=api_id)["items"]
    assert any(a["id"] == auth_id for a in listed)

    apigw_v1.delete_authorizer(restApiId=api_id, authorizerId=auth_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_authorizer(restApiId=api_id, authorizerId=auth_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_model_crud(apigw_v1):
    """CreateModel, GetModel, DeleteModel lifecycle."""
    api_id = apigw_v1.create_rest_api(name="v1-model-crud")["id"]
    resp = apigw_v1.create_model(
        restApiId=api_id,
        name="MyModel",
        contentType="application/json",
        schema='{"type": "object"}',
    )
    assert resp["name"] == "MyModel"

    got = apigw_v1.get_model(restApiId=api_id, modelName="MyModel")
    assert got["name"] == "MyModel"

    listed = apigw_v1.get_models(restApiId=api_id)["items"]
    assert any(m["name"] == "MyModel" for m in listed)

    apigw_v1.delete_model(restApiId=api_id, modelName="MyModel")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_model(restApiId=api_id, modelName="MyModel")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_tags(apigw_v1):
    """TagResource, GetTags, UntagResource."""
    api_id = apigw_v1.create_rest_api(name="v1-tags-test")["id"]
    arn = f"arn:aws:apigateway:us-east-1::/restapis/{api_id}"

    apigw_v1.tag_resource(resourceArn=arn, tags={"env": "test", "team": "platform"})
    resp = apigw_v1.get_tags(resourceArn=arn)
    assert resp["tags"]["env"] == "test"
    assert resp["tags"]["team"] == "platform"

    apigw_v1.untag_resource(resourceArn=arn, tagKeys=["env"])
    resp2 = apigw_v1.get_tags(resourceArn=arn)
    assert "env" not in resp2["tags"]
    assert resp2["tags"]["team"] == "platform"

    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_apikey_crud(apigw_v1):
    """ApiKey full lifecycle: create, get, delete."""
    resp = apigw_v1.create_api_key(name="v1-test-key", enabled=True)
    key_id = resp["id"]
    assert resp["name"] == "v1-test-key"
    assert "value" in resp

    got = apigw_v1.get_api_key(apiKey=key_id, includeValue=True)
    assert got["id"] == key_id

    listed = apigw_v1.get_api_keys()["items"]
    assert any(k["id"] == key_id for k in listed)

    apigw_v1.delete_api_key(apiKey=key_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_api_key(apiKey=key_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_apigwv1_usage_plan_crud(apigw_v1):
    """UsagePlan full lifecycle: create, get, delete."""
    resp = apigw_v1.create_usage_plan(
        name="v1-plan",
        throttle={"rateLimit": 100, "burstLimit": 200},
        quota={"limit": 10000, "period": "MONTH"},
    )
    plan_id = resp["id"]
    assert resp["name"] == "v1-plan"

    got = apigw_v1.get_usage_plan(usagePlanId=plan_id)
    assert got["id"] == plan_id

    listed = apigw_v1.get_usage_plans()["items"]
    assert any(p["id"] == plan_id for p in listed)

    apigw_v1.delete_usage_plan(usagePlanId=plan_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_usage_plan(usagePlanId=plan_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_apigwv1_execute_lambda_proxy(apigw_v1, lam):
    """End-to-end: create API + resource + method + integration + deploy + invoke Lambda."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-proxy-{_uuid.uuid4().hex[:8]}"
    code = b"import json\ndef handler(event, context):\n    return {'statusCode': 200, 'body': 'pong'}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-exec-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="ping",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/ping"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    body = resp.read()
    assert body == b"pong"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigwv1_execute_path_params(apigw_v1, lam):
    """Path parameter {userId} is passed correctly in event['pathParameters']."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-v1-params-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    uid = (event.get('pathParameters') or {}).get('userId', 'missing')\n"
        b"    return {'statusCode': 200, 'body': uid}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-params-{fname}")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    users_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="users",
    )["id"]
    user_id_res = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=users_id,
        pathPart="{userId}",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=user_id_res,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=user_id_res,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="v1", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/v1/users/alice123"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert resp.read() == b"alice123"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigwv1_execute_mock_integration(apigw_v1):
    """MOCK integration returns fixed JSON from integration response template."""
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-mock-test")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(
        restApiId=api_id,
        parentId=root["id"],
        pathPart="mock",
    )["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="MOCK",
        integrationHttpMethod="GET",
        uri="",
        requestTemplates={"application/json": '{"statusCode": 200}'},
    )
    apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
    )
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
        responseTemplates={"application/json": '{"mocked": true}'},
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/mock"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    body = json.loads(resp.read())
    assert body["mocked"] is True

    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_execute_missing_resource_404(apigw_v1):
    """Request to non-existent path returns 404 with AWS-style message."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-missing-resource")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/nonexistent"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    try:
        _urlreq.urlopen(req)
        assert False, "Expected 404"
    except _urlerr.HTTPError as e:
        assert e.code == 404

    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_no_conflict_with_v2(apigw_v1, apigw, lam):
    """v1 and v2 APIs can coexist; execute-api routes them independently."""
    import urllib.request as _urlreq
    import uuid as _uuid

    # Create v1 Lambda
    fname_v1 = f"intg-coexist-v1-{_uuid.uuid4().hex[:8]}"
    code_v1 = b"def handler(event, context):\n    return {'statusCode': 200, 'body': 'v1-response'}\n"
    buf_v1 = io.BytesIO()
    with zipfile.ZipFile(buf_v1, "w") as zf:
        zf.writestr("index.py", code_v1)
    lam.create_function(
        FunctionName=fname_v1,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf_v1.getvalue()},
    )

    # Create v2 Lambda
    fname_v2 = f"intg-coexist-v2-{_uuid.uuid4().hex[:8]}"
    code_v2 = b"def handler(event, context):\n    return {'statusCode': 200, 'body': 'v2-response'}\n"
    buf_v2 = io.BytesIO()
    with zipfile.ZipFile(buf_v2, "w") as zf:
        zf.writestr("index.py", code_v2)
    lam.create_function(
        FunctionName=fname_v2,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf_v2.getvalue()},
    )

    # Set up v1 API
    v1_api_id = apigw_v1.create_rest_api(name="coexist-v1")["id"]
    root = next(r for r in apigw_v1.get_resources(restApiId=v1_api_id)["items"] if r["path"] == "/")
    res_id = apigw_v1.create_resource(restApiId=v1_api_id, parentId=root["id"], pathPart="hit")["id"]
    apigw_v1.put_method(
        restApiId=v1_api_id,
        resourceId=res_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=v1_api_id,
        resourceId=res_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname_v1}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=v1_api_id)["id"]
    apigw_v1.create_stage(restApiId=v1_api_id, stageName="s", deploymentId=dep_id)

    # Set up v2 API
    v2_api_id = apigw.create_api(Name="coexist-v2", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=v2_api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname_v2}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=v2_api_id, RouteKey="GET /hit", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=v2_api_id, StageName="$default")

    # Invoke v1
    url_v1 = f"http://{v1_api_id}.execute-api.localhost:{_EXECUTE_PORT}/s/hit"
    req_v1 = _urlreq.Request(url_v1, method="GET")
    req_v1.add_header("Host", f"{v1_api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp_v1 = _urlreq.urlopen(req_v1)
    assert resp_v1.status == 200
    assert resp_v1.read() == b"v1-response"

    # Invoke v2
    url_v2 = f"http://{v2_api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/hit"
    req_v2 = _urlreq.Request(url_v2, method="GET")
    req_v2.add_header("Host", f"{v2_api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp_v2 = _urlreq.urlopen(req_v2)
    assert resp_v2.status == 200
    assert resp_v2.read() == b"v2-response"

    # Cleanup
    apigw_v1.delete_rest_api(restApiId=v1_api_id)
    apigw.delete_api(ApiId=v2_api_id)
    lam.delete_function(FunctionName=fname_v1)
    lam.delete_function(FunctionName=fname_v2)

def test_apigwv1_update_rest_api_name(apigw_v1):
    """UpdateRestApi renames the API via patchOperations."""
    api_id = apigw_v1.create_rest_api(name="v1-update-name-before")["id"]
    apigw_v1.update_rest_api(
        restApiId=api_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-update-name-after"}],
    )
    assert apigw_v1.get_rest_api(restApiId=api_id)["name"] == "v1-update-name-after"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_resource(apigw_v1):
    """DeleteResource removes a resource; subsequent GetResource raises 404."""
    api_id = apigw_v1.create_rest_api(name="v1-del-resource")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    child_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="todel")["id"]
    apigw_v1.delete_resource(restApiId=api_id, resourceId=child_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_resource(restApiId=api_id, resourceId=child_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_method(apigw_v1):
    """DeleteMethod removes method; GetMethod raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-method")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.delete_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_method(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_integration(apigw_v1):
    """DeleteIntegration removes integration; GetIntegration raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-integration")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    apigw_v1.delete_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_method_response(apigw_v1):
    """DeleteMethodResponse removes the method response entry."""
    api_id = apigw_v1.create_rest_api(name="v1-del-mresp")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_method_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    apigw_v1.delete_method_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_method_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_integration_response(apigw_v1):
    """DeleteIntegrationResponse removes the integration response entry."""
    api_id = apigw_v1.create_rest_api(name="v1-del-iresp")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=root_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
    )
    apigw_v1.delete_integration_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_integration_response(restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_deployment(apigw_v1):
    """DeleteDeployment removes deployment; GetDeployment raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-deploy")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.delete_deployment(restApiId=api_id, deploymentId=dep_id)
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_deployment(restApiId=api_id, deploymentId=dep_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_delete_stage(apigw_v1):
    """DeleteStage removes stage; GetStage raises 404 after."""
    api_id = apigw_v1.create_rest_api(name="v1-del-stage")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="todel", deploymentId=dep_id)
    apigw_v1.delete_stage(restApiId=api_id, stageName="todel")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_stage(restApiId=api_id, stageName="todel")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_update_api_key(apigw_v1):
    """UpdateApiKey updates name and sets lastUpdatedDate."""
    import datetime

    key_id = apigw_v1.create_api_key(name="v1-key-update-before")["id"]
    resp = apigw_v1.update_api_key(
        apiKey=key_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-key-update-after"}],
    )
    assert resp["name"] == "v1-key-update-after"
    assert isinstance(resp["lastUpdatedDate"], datetime.datetime)
    apigw_v1.delete_api_key(apiKey=key_id)

def test_apigwv1_update_usage_plan(apigw_v1):
    """UpdateUsagePlan updates name via patchOperations."""
    plan_id = apigw_v1.create_usage_plan(name="v1-plan-update-before")["id"]
    resp = apigw_v1.update_usage_plan(
        usagePlanId=plan_id,
        patchOperations=[{"op": "replace", "path": "/name", "value": "v1-plan-update-after"}],
    )
    assert resp["name"] == "v1-plan-update-after"
    apigw_v1.delete_usage_plan(usagePlanId=plan_id)

def test_apigwv1_deployment_api_summary(apigw_v1):
    """CreateDeployment apiSummary reflects methods configured on resources."""
    api_id = apigw_v1.create_rest_api(name="v1-api-summary")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    dep = apigw_v1.create_deployment(restApiId=api_id)
    assert "/" in dep.get("apiSummary", {}), "apiSummary must include root resource path"
    assert "GET" in dep["apiSummary"]["/"], "apiSummary must include configured HTTP method"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_domain_name_crud(apigw_v1):
    """DomainName create, get, list, delete lifecycle."""
    resp = apigw_v1.create_domain_name(
        domainName="api.example.com",
        endpointConfiguration={"types": ["REGIONAL"]},
    )
    assert resp["domainName"] == "api.example.com"
    got = apigw_v1.get_domain_name(domainName="api.example.com")
    assert got["domainName"] == "api.example.com"
    listed = apigw_v1.get_domain_names()["items"]
    assert any(d["domainName"] == "api.example.com" for d in listed)
    apigw_v1.delete_domain_name(domainName="api.example.com")
    with pytest.raises(ClientError) as exc:
        apigw_v1.get_domain_name(domainName="api.example.com")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_apigwv1_base_path_mapping_crud(apigw_v1):
    """BasePathMapping create, get, list, delete lifecycle."""
    apigw_v1.create_domain_name(domainName="bpm.example.com")
    api_id = apigw_v1.create_rest_api(name="v1-bpm-api")["id"]
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="prod", deploymentId=dep_id)

    mapping = apigw_v1.create_base_path_mapping(
        domainName="bpm.example.com",
        basePath="v1",
        restApiId=api_id,
        stage="prod",
    )
    assert mapping["basePath"] == "v1"
    assert mapping["restApiId"] == api_id

    got = apigw_v1.get_base_path_mapping(domainName="bpm.example.com", basePath="v1")
    assert got["basePath"] == "v1"

    listed = apigw_v1.get_base_path_mappings(domainName="bpm.example.com")["items"]
    assert any(m["basePath"] == "v1" for m in listed)

    apigw_v1.delete_base_path_mapping(domainName="bpm.example.com", basePath="v1")
    apigw_v1.delete_rest_api(restApiId=api_id)
    apigw_v1.delete_domain_name(domainName="bpm.example.com")

def test_apigwv1_execute_missing_stage_404(apigw_v1):
    """execute-api returns 404 when stage does not exist."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-no-stage")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    apigw_v1.put_method(restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE")
    apigw_v1.put_integration(restApiId=api_id, resourceId=root_id, httpMethod="GET", type="MOCK")
    apigw_v1.create_deployment(restApiId=api_id)
    # Do NOT create a stage — request to a nonexistent stage should 404

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/nonexistent/"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    with pytest.raises(_urlerr.HTTPError) as exc:
        _urlreq.urlopen(req)
    assert exc.value.code == 404
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_execute_missing_method_405(apigw_v1):
    """execute-api returns 405 when resource exists but method is not configured."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-no-method")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="noop")["id"]
    # PUT method for POST only — GET not configured
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="POST",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(restApiId=api_id, resourceId=resource_id, httpMethod="POST", type="MOCK")
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/noop"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    with pytest.raises(_urlerr.HTTPError) as exc:
        _urlreq.urlopen(req)
    assert exc.value.code == 405
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_execute_lambda_arn_uri(apigw_v1, lam):
    """execute-api Lambda proxy works with plain arn:aws:lambda ARN as integration URI."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"v1-arn-uri-{_uuid.uuid4().hex[:8]}"
    code = b"import json\ndef handler(event, context):\n    return {'statusCode': 200, 'body': 'arn-ok'}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-arn-{fname}")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="hit")["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    # Use plain arn:aws:lambda ARN (not apigateway URI form)
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/hit"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert resp.read() == b"arn-ok"

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigwv1_execute_lambda_requestcontext(apigw_v1, lam):
    """execute-api Lambda event includes required requestContext fields."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"v1-reqctx-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    ctx = event.get('requestContext', {})\n"
        b"    body = json.dumps({\n"
        b"        'stage': ctx.get('stage'),\n"
        b"        'httpMethod': ctx.get('httpMethod'),\n"
        b"        'apiId': ctx.get('apiId'),\n"
        b"        'has_requestTime': 'requestTime' in ctx,\n"
        b"        'has_requestTimeEpoch': 'requestTimeEpoch' in ctx,\n"
        b"        'has_protocol': 'protocol' in ctx,\n"
        b"        'has_path': 'path' in ctx,\n"
        b"        'has_mvh': 'multiValueHeaders' in event,\n"
        b"    })\n"
        b"    return {'statusCode': 200, 'body': body}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    api_id = apigw_v1.create_rest_api(name=f"v1-ctx-{fname}")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="ctx")["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="AWS_PROXY",
        integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:{fname}/invocations",
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="prod", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/ctx"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    data = json.loads(resp.read())
    assert data["stage"] == "prod"
    assert data["httpMethod"] == "GET"
    assert data["apiId"] == api_id
    assert data["has_requestTime"] is True
    assert data["has_requestTimeEpoch"] is True
    assert data["has_protocol"] is True
    assert data["has_path"] is True
    assert data["has_mvh"] is True

    apigw_v1.delete_rest_api(restApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigwv1_execute_mock_response_parameters(apigw_v1):
    """MOCK integration responseParameters are applied as HTTP response headers."""
    import urllib.request as _urlreq

    api_id = apigw_v1.create_rest_api(name="v1-mock-params")["id"]
    root_id = next(r["id"] for r in apigw_v1.get_resources(restApiId=api_id)["items"] if r["path"] == "/")
    resource_id = apigw_v1.create_resource(restApiId=api_id, parentId=root_id, pathPart="rp")["id"]
    apigw_v1.put_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        authorizationType="NONE",
    )
    apigw_v1.put_method_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        responseParameters={"method.response.header.X-Custom-Header": False},
    )
    apigw_v1.put_integration(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        type="MOCK",
        requestTemplates={"application/json": '{"statusCode": 200}'},
    )
    apigw_v1.put_integration_response(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="GET",
        statusCode="200",
        selectionPattern="",
        responseTemplates={"application/json": '{"ok": true}'},
        responseParameters={"method.response.header.X-Custom-Header": "'myvalue'"},
    )
    dep_id = apigw_v1.create_deployment(restApiId=api_id)["id"]
    apigw_v1.create_stage(restApiId=api_id, stageName="test", deploymentId=dep_id)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/test/rp"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.headers.get("X-Custom-Header") == "myvalue"
    apigw_v1.delete_rest_api(restApiId=api_id)

def test_apigwv1_usage_plan_key_crud(apigw_v1):
    """CreateUsagePlanKey / GetUsagePlanKeys / DeleteUsagePlanKey."""
    api_key = apigw_v1.create_api_key(name="qa-v1-key", enabled=True)
    key_id = api_key["id"]
    plan = apigw_v1.create_usage_plan(
        name="qa-v1-plan",
        throttle={"rateLimit": 100, "burstLimit": 200},
    )
    plan_id = plan["id"]
    apigw_v1.create_usage_plan_key(usagePlanId=plan_id, keyId=key_id, keyType="API_KEY")
    keys = apigw_v1.get_usage_plan_keys(usagePlanId=plan_id)["items"]
    assert any(k["id"] == key_id for k in keys)
    apigw_v1.delete_usage_plan_key(usagePlanId=plan_id, keyId=key_id)
    keys2 = apigw_v1.get_usage_plan_keys(usagePlanId=plan_id)["items"]
    assert not any(k["id"] == key_id for k in keys2)

def test_apigwv1_created_date_is_unix_timestamp(apigw_v1):
    resp = apigw_v1.create_rest_api(name="tf-date-test")
    created = resp["createdDate"]
    # boto3 parses numeric timestamps as datetime.datetime — if it were a string
    # botocore would raise a deserialization error before we even get here.
    import datetime

    assert isinstance(created, datetime.datetime), (
        f"createdDate should be datetime (parsed from Unix int), got {type(created)}"
    )
    apigw_v1.delete_rest_api(restApiId=resp["id"])


# ========== Custom/predictable REST API IDs via tags (issue #400) ==========

def test_apigwv1_custom_id_via_ms_custom_id_tag(apigw_v1):
    resp = apigw_v1.create_rest_api(
        name="ms-custom-v1", tags={"ms-custom-id": "v1pinned"},
    )
    assert resp["id"] == "v1pinned"


def test_apigwv1_custom_id_rejects_ls_custom_id(apigw_v1):
    """ls-custom-id is not supported; caller must use ms-custom-id."""
    with pytest.raises(ClientError) as exc_info:
        apigw_v1.create_rest_api(
            name="ls-reject-v1", tags={"ls-custom-id": "should-fail"},
        )
    assert exc_info.value.response["Error"]["Code"] == "BadRequestException"
    assert "ms-custom-id" in exc_info.value.response["Error"]["Message"]


def test_apigwv1_custom_id_duplicate_rejected(apigw_v1):
    apigw_v1.create_rest_api(
        name="v1-dup-1", tags={"ms-custom-id": "v1dup"},
    )
    with pytest.raises(ClientError) as exc_info:
        apigw_v1.create_rest_api(
            name="v1-dup-2", tags={"ms-custom-id": "v1dup"},
        )
    assert exc_info.value.response["Error"]["Code"] == "ConflictException"


def test_apigwv1_custom_id_absent_uses_random(apigw_v1):
    resp = apigw_v1.create_rest_api(name="v1-random")
    # _new_id() returns up to 10 hex chars; trimmed to [:8] in _create_rest_api.
    assert 8 <= len(resp["id"]) <= 10
