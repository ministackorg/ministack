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

def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()

_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"

def test_apigw_create_api(apigw):
    resp = apigw.create_api(Name="test-api", ProtocolType="HTTP")
    assert "ApiId" in resp
    assert resp["Name"] == "test-api"
    assert resp["ProtocolType"] == "HTTP"

def test_apigw_get_api(apigw):
    create = apigw.create_api(Name="get-api-test", ProtocolType="HTTP")
    api_id = create["ApiId"]
    resp = apigw.get_api(ApiId=api_id)
    assert resp["ApiId"] == api_id
    assert resp["Name"] == "get-api-test"

def test_apigw_get_apis(apigw):
    apigw.create_api(Name="list-api-a", ProtocolType="HTTP")
    apigw.create_api(Name="list-api-b", ProtocolType="HTTP")
    resp = apigw.get_apis()
    names = [a["Name"] for a in resp["Items"]]
    assert "list-api-a" in names
    assert "list-api-b" in names

def test_apigw_update_api(apigw):
    api_id = apigw.create_api(Name="update-api-before", ProtocolType="HTTP")["ApiId"]
    apigw.update_api(ApiId=api_id, Name="update-api-after")
    resp = apigw.get_api(ApiId=api_id)
    assert resp["Name"] == "update-api-after"

def test_apigw_delete_api(apigw):
    from botocore.exceptions import ClientError

    api_id = apigw.create_api(Name="delete-api-test", ProtocolType="HTTP")["ApiId"]
    apigw.delete_api(ApiId=api_id)
    with pytest.raises(ClientError) as exc:
        apigw.get_api(ApiId=api_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_apigw_create_route(apigw):
    api_id = apigw.create_api(Name="route-api", ProtocolType="HTTP")["ApiId"]
    resp = apigw.create_route(ApiId=api_id, RouteKey="GET /items")
    assert "RouteId" in resp
    assert resp["RouteKey"] == "GET /items"

def test_apigw_get_routes(apigw):
    api_id = apigw.create_api(Name="routes-list-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /a")
    apigw.create_route(ApiId=api_id, RouteKey="POST /b")
    resp = apigw.get_routes(ApiId=api_id)
    keys = [r["RouteKey"] for r in resp["Items"]]
    assert "GET /a" in keys
    assert "POST /b" in keys

def test_apigw_get_route(apigw):
    api_id = apigw.create_api(Name="get-route-api", ProtocolType="HTTP")["ApiId"]
    route_id = apigw.create_route(ApiId=api_id, RouteKey="DELETE /things")["RouteId"]
    resp = apigw.get_route(ApiId=api_id, RouteId=route_id)
    assert resp["RouteId"] == route_id
    assert resp["RouteKey"] == "DELETE /things"

def test_apigw_update_route(apigw):
    api_id = apigw.create_api(Name="update-route-api", ProtocolType="HTTP")["ApiId"]
    route_id = apigw.create_route(ApiId=api_id, RouteKey="GET /old")["RouteId"]
    apigw.update_route(ApiId=api_id, RouteId=route_id, RouteKey="GET /new")
    resp = apigw.get_route(ApiId=api_id, RouteId=route_id)
    assert resp["RouteKey"] == "GET /new"

def test_apigw_delete_route(apigw):
    api_id = apigw.create_api(Name="del-route-api", ProtocolType="HTTP")["ApiId"]
    route_id = apigw.create_route(ApiId=api_id, RouteKey="GET /gone")["RouteId"]
    apigw.delete_route(ApiId=api_id, RouteId=route_id)
    resp = apigw.get_routes(ApiId=api_id)
    assert not any(r["RouteId"] == route_id for r in resp["Items"])

def test_apigw_create_integration(apigw):
    api_id = apigw.create_api(Name="integ-api", ProtocolType="HTTP")["ApiId"]
    resp = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:my-fn",
        PayloadFormatVersion="2.0",
    )
    assert "IntegrationId" in resp
    assert resp["IntegrationType"] == "AWS_PROXY"
    assert resp["PayloadFormatVersion"] == "2.0"

def test_apigw_get_integrations(apigw):
    api_id = apigw.create_api(Name="integ-list-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:fn1",
    )
    resp = apigw.get_integrations(ApiId=api_id)
    assert len(resp["Items"]) >= 1

def test_apigw_get_integration(apigw):
    api_id = apigw.create_api(Name="get-integ-api", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="HTTP_PROXY",
        IntegrationUri="https://example.com",
        IntegrationMethod="GET",
    )["IntegrationId"]
    resp = apigw.get_integration(ApiId=api_id, IntegrationId=int_id)
    assert resp["IntegrationId"] == int_id
    assert resp["IntegrationType"] == "HTTP_PROXY"

def test_apigw_delete_integration(apigw):
    api_id = apigw.create_api(Name="del-integ-api", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:fn2",
    )["IntegrationId"]
    apigw.delete_integration(ApiId=api_id, IntegrationId=int_id)
    resp = apigw.get_integrations(ApiId=api_id)
    assert not any(i["IntegrationId"] == int_id for i in resp["Items"])

def test_apigw_create_stage(apigw):
    api_id = apigw.create_api(Name="stage-api", ProtocolType="HTTP")["ApiId"]
    resp = apigw.create_stage(ApiId=api_id, StageName="prod")
    assert resp["StageName"] == "prod"

def test_apigw_get_stages(apigw):
    api_id = apigw.create_api(Name="stages-list-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(ApiId=api_id, StageName="v1")
    apigw.create_stage(ApiId=api_id, StageName="v2")
    resp = apigw.get_stages(ApiId=api_id)
    names = [s["StageName"] for s in resp["Items"]]
    assert "v1" in names
    assert "v2" in names

def test_apigw_get_stage(apigw):
    api_id = apigw.create_api(Name="get-stage-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(ApiId=api_id, StageName="dev")
    resp = apigw.get_stage(ApiId=api_id, StageName="dev")
    assert resp["StageName"] == "dev"

def test_apigw_update_stage(apigw):
    api_id = apigw.create_api(Name="update-stage-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(ApiId=api_id, StageName="staging")
    apigw.update_stage(ApiId=api_id, StageName="staging", Description="updated")
    resp = apigw.get_stage(ApiId=api_id, StageName="staging")
    assert resp.get("Description") == "updated"

def test_apigw_delete_stage(apigw):
    api_id = apigw.create_api(Name="del-stage-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(ApiId=api_id, StageName="temp")
    apigw.delete_stage(ApiId=api_id, StageName="temp")
    resp = apigw.get_stages(ApiId=api_id)
    assert not any(s["StageName"] == "temp" for s in resp["Items"])

def test_apigw_create_deployment(apigw):
    api_id = apigw.create_api(Name="deploy-api", ProtocolType="HTTP")["ApiId"]
    resp = apigw.create_deployment(ApiId=api_id)
    assert "DeploymentId" in resp
    assert resp["DeploymentStatus"] == "DEPLOYED"

def test_apigw_get_deployments(apigw):
    api_id = apigw.create_api(Name="deployments-list-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_deployment(ApiId=api_id, Description="first")
    apigw.create_deployment(ApiId=api_id, Description="second")
    resp = apigw.get_deployments(ApiId=api_id)
    assert len(resp["Items"]) >= 2

def test_apigw_get_deployment(apigw):
    api_id = apigw.create_api(Name="get-deploy-api", ProtocolType="HTTP")["ApiId"]
    dep_id = apigw.create_deployment(ApiId=api_id, Description="single")["DeploymentId"]
    resp = apigw.get_deployment(ApiId=api_id, DeploymentId=dep_id)
    assert resp["DeploymentId"] == dep_id

def test_apigw_delete_deployment(apigw):
    api_id = apigw.create_api(Name="del-deploy-api", ProtocolType="HTTP")["ApiId"]
    dep_id = apigw.create_deployment(ApiId=api_id)["DeploymentId"]
    apigw.delete_deployment(ApiId=api_id, DeploymentId=dep_id)
    resp = apigw.get_deployments(ApiId=api_id)
    assert not any(d["DeploymentId"] == dep_id for d in resp["Items"])

def test_apigw_tag_resource(apigw):
    api_id = apigw.create_api(Name="tag-api", ProtocolType="HTTP")["ApiId"]
    resource_arn = f"arn:aws:apigateway:us-east-1::/apis/{api_id}"
    apigw.tag_resource(ResourceArn=resource_arn, Tags={"env": "test", "owner": "team-a"})
    resp = apigw.get_tags(ResourceArn=resource_arn)
    assert resp["Tags"].get("env") == "test"
    assert resp["Tags"].get("owner") == "team-a"

def test_apigw_untag_resource(apigw):
    api_id = apigw.create_api(Name="untag-api", ProtocolType="HTTP")["ApiId"]
    resource_arn = f"arn:aws:apigateway:us-east-1::/apis/{api_id}"
    apigw.tag_resource(ResourceArn=resource_arn, Tags={"remove-me": "yes", "keep-me": "yes"})
    apigw.untag_resource(ResourceArn=resource_arn, TagKeys=["remove-me"])
    resp = apigw.get_tags(ResourceArn=resource_arn)
    assert "remove-me" not in resp["Tags"]
    assert resp["Tags"].get("keep-me") == "yes"

def test_apigw_api_not_found(apigw):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        apigw.get_api(ApiId="00000000")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_apigw_route_on_deleted_api(apigw):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        apigw.create_route(ApiId="00000000", RouteKey="GET /x")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_apigw_http_protocol_type(apigw):
    resp = apigw.create_api(Name="http-proto-api", ProtocolType="HTTP")
    assert resp["ProtocolType"] == "HTTP"
    api_id = resp["ApiId"]
    fetched = apigw.get_api(ApiId=api_id)
    assert fetched["ProtocolType"] == "HTTP"

def test_apigw_execute_lambda_proxy(apigw, lam):
    """API Gateway execute-api routes a request through Lambda proxy integration."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-apigw-fn-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    return {\n"
        b"        'statusCode': 200,\n"
        b"        'headers': {'Content-Type': 'application/json'},\n"
        b"        'body': json.dumps({'path': event.get('rawPath', '/')}),\n"
        b"    }\n"
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

    api_id = apigw.create_api(Name=f"exec-api-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    route_id = apigw.create_route(
        ApiId=api_id,
        RouteKey="GET /hello",
        Target=f"integrations/{int_id}",
    )["RouteId"]
    apigw.create_stage(ApiId=api_id, StageName="$default")

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/hello"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    body = json.loads(resp.read())
    assert body["path"] == "/hello"

    # Cleanup
    apigw.delete_route(ApiId=api_id, RouteId=route_id)
    apigw.delete_integration(ApiId=api_id, IntegrationId=int_id)
    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigw_execute_no_route(apigw):
    """execute-api returns 404 when no matching route exists."""
    import urllib.error as _urlerr
    import urllib.request as _urlreq

    api_id = apigw.create_api(Name="no-route-api", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(ApiId=api_id, StageName="$default")
    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/nonexistent"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    try:
        _urlreq.urlopen(req)
        assert False, "Expected 404"
    except _urlerr.HTTPError as e:
        assert e.code == 404
    apigw.delete_api(ApiId=api_id)

def test_apigw_execute_default_route(apigw, lam):
    """$default catch-all route matches any path."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-default-fn-{_uuid.uuid4().hex[:8]}"
    code = b"def handler(event, context):\n    return {'statusCode': 200, 'body': 'ok'}\n"
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
    api_id = apigw.create_api(Name=f"default-route-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="$default", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/any/path/here"
    req = _urlreq.Request(url, method="POST")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200

    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigw_path_param_route(apigw, lam):
    """Route with {id} path parameter matches requests correctly."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-param-fn-{_uuid.uuid4().hex[:8]}"
    code = (
        b"import json\n"
        b"def handler(event, context):\n"
        b"    return {'statusCode': 200, 'body': json.dumps({'rawPath': event.get('rawPath')})}\n"
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
    api_id = apigw.create_api(Name=f"param-api-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /items/{id}", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/items/abc123"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    body = json.loads(resp.read())
    assert body["rawPath"] == "/items/abc123"

    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigw_path_parameters_in_event(apigw, lam):
    """API Gateway v2 should populate pathParameters in the Lambda event."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-pathparam-{_uuid.uuid4().hex[:8]}"
    code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'body': json.dumps(event.get('pathParameters'))}\n"
    )
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    api_id = apigw.create_api(Name=f"pp-api-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /items/{itemId}", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    try:
        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/items/my-item-42"
        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        resp = _urlreq.urlopen(req)
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body == {"itemId": "my-item-42"}
    finally:
        apigw.delete_api(ApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigw_greedy_path_parameters_in_event(apigw, lam):
    """{proxy+} greedy path parameter should be extracted into pathParameters."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-greedy-pp-{_uuid.uuid4().hex[:8]}"
    code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'body': json.dumps(event.get('pathParameters'))}\n"
    )
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    api_id = apigw.create_api(Name=f"greedy-pp-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /files/{proxy+}", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    try:
        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/files/a/b/c.txt"
        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        resp = _urlreq.urlopen(req)
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body == {"proxy": "a/b/c.txt"}
    finally:
        apigw.delete_api(ApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigw_query_params_and_headers_in_event(apigw, lam):
    """API Gateway v2 should pass queryStringParameters, rawQueryString, and headers to Lambda."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-qp-{_uuid.uuid4().hex[:8]}"
    code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'body': json.dumps({\n"
        "        'qs': event.get('queryStringParameters'),\n"
        "        'rawQs': event.get('rawQueryString'),\n"
        "        'customHeader': event.get('headers', {}).get('x-custom-header'),\n"
        "    })}\n"
    )
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    api_id = apigw.create_api(Name=f"qp-api-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /search", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    try:
        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/search?q=hello&tag=a&tag=b"
        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        req.add_header("X-Custom-Header", "test-value")
        resp = _urlreq.urlopen(req)
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body["qs"]["q"] == "hello"
        # Multi-value params should be comma-joined per AWS API Gateway v2 spec
        assert body["qs"]["tag"] == "a,b"
        assert "q=hello" in body["rawQs"]
        assert "tag=a" in body["rawQs"]
        assert "tag=b" in body["rawQs"]
        assert body["customHeader"] == "test-value"
    finally:
        apigw.delete_api(ApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigw_multiple_path_parameters(apigw, lam):
    """Multiple path parameters in one route should all be extracted."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-multi-pp-{_uuid.uuid4().hex[:8]}"
    code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'body': json.dumps(event.get('pathParameters'))}\n"
    )
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    api_id = apigw.create_api(Name=f"multi-pp-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(
        ApiId=api_id,
        RouteKey="GET /projects/{projectKey}/items/{itemId}",
        Target=f"integrations/{int_id}",
    )
    apigw.create_stage(ApiId=api_id, StageName="$default")

    try:
        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/projects/bunya/items/prod-42"
        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        resp = _urlreq.urlopen(req)
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body == {"projectKey": "bunya", "itemId": "prod-42"}
    finally:
        apigw.delete_api(ApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigw_no_path_parameters_returns_null(apigw, lam):
    """Routes without path parameters should have pathParameters as null."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-no-pp-{_uuid.uuid4().hex[:8]}"
    code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'body': json.dumps({'pp': event.get('pathParameters')})}\n"
    )
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    api_id = apigw.create_api(Name=f"no-pp-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /products", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    try:
        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/products"
        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        resp = _urlreq.urlopen(req)
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body["pp"] is None
    finally:
        apigw.delete_api(ApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigw_url_encoded_path_parameter(apigw, lam):
    """URL-encoded characters in path parameters are decoded by the ASGI layer."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-enc-pp-{_uuid.uuid4().hex[:8]}"
    code = (
        "import json\n"
        "def handler(event, context):\n"
        "    return {'statusCode': 200, 'body': json.dumps(event.get('pathParameters'))}\n"
    )
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    api_id = apigw.create_api(Name=f"enc-pp-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /items/{itemId}", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    try:
        # URL-encode a value with special characters
        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/items/hello%20world"
        req = _urlreq.Request(url, method="GET")
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        resp = _urlreq.urlopen(req)
        assert resp.status == 200
        body = json.loads(resp.read())
        # AWS passes the decoded value in pathParameters
        assert body["itemId"] == "hello world"
    finally:
        apigw.delete_api(ApiId=api_id)
        lam.delete_function(FunctionName=fname)


def test_apigw_greedy_path_param(apigw, lam):
    """{proxy+} greedy path parameter matches paths with multiple segments."""
    import urllib.request as _urlreq
    import uuid as _uuid_mod

    fname = f"intg-greedy-{_uuid_mod.uuid4().hex[:8]}"
    code = 'def handler(event, context):\n    return {"statusCode": 200, "body": event["rawPath"]}\n'
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    func_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fname}"
    api_id = apigw.create_api(Name="greedy-test", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=func_arn,
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /files/{proxy+}", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    # Path with multiple segments should match {proxy+}
    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/files/a/b/c"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    # handler returns rawPath as body string
    assert resp.read().decode() == "/files/a/b/c"

    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigw_authorizer_crud(apigw):
    """CreateAuthorizer / GetAuthorizer / GetAuthorizers / UpdateAuthorizer / DeleteAuthorizer."""
    import uuid as _uuid_mod

    api_id = apigw.create_api(Name=f"auth-test-{_uuid_mod.uuid4().hex[:8]}", ProtocolType="HTTP")["ApiId"]

    # Create JWT authorizer
    resp = apigw.create_authorizer(
        ApiId=api_id,
        AuthorizerType="JWT",
        Name="my-jwt-auth",
        IdentitySource=["$request.header.Authorization"],
        JwtConfiguration={
            "Audience": ["https://example.com"],
            "Issuer": "https://idp.example.com",
        },
    )
    assert resp["AuthorizerType"] == "JWT"
    assert resp["Name"] == "my-jwt-auth"
    auth_id = resp["AuthorizerId"]

    # Get single
    got = apigw.get_authorizer(ApiId=api_id, AuthorizerId=auth_id)
    assert got["AuthorizerId"] == auth_id
    assert got["JwtConfiguration"]["Issuer"] == "https://idp.example.com"

    # List
    listed = apigw.get_authorizers(ApiId=api_id)
    assert any(a["AuthorizerId"] == auth_id for a in listed["Items"])

    # Update
    updated = apigw.update_authorizer(ApiId=api_id, AuthorizerId=auth_id, Name="renamed-auth")
    assert updated["Name"] == "renamed-auth"

    # Delete
    apigw.delete_authorizer(ApiId=api_id, AuthorizerId=auth_id)
    listed2 = apigw.get_authorizers(ApiId=api_id)
    assert not any(a["AuthorizerId"] == auth_id for a in listed2["Items"])

    apigw.delete_api(ApiId=api_id)

def test_apigw_routekey_in_lambda_event(apigw, lam):
    """routeKey in Lambda event should reflect the matched route, not hardcoded $default."""
    import urllib.request as _urlreq
    import uuid as _uuid_mod

    fname = f"intg-rk-{_uuid_mod.uuid4().hex[:8]}"
    code = 'def handler(event, context):\n    return {"statusCode": 200, "body": event["routeKey"]}\n'
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    func_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fname}"
    api_id = apigw.create_api(Name="rk-test", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=func_arn,
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /ping", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/ping"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert resp.read().decode() == "GET /ping"

    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_apigw_update_integration(apigw):
    """UpdateIntegration changes integrationUri."""
    api_id = apigw.create_api(Name="qa-apigw-update-integ", ProtocolType="HTTP")["ApiId"]
    integ_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:old-fn",
    )["IntegrationId"]
    apigw.update_integration(
        ApiId=api_id,
        IntegrationId=integ_id,
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:new-fn",
    )
    integ = apigw.get_integration(ApiId=api_id, IntegrationId=integ_id)
    assert "new-fn" in integ["IntegrationUri"]

def test_apigw_delete_route_v2(apigw):
    """DeleteRoute removes the route from GetRoutes."""
    api_id = apigw.create_api(Name="qa-apigw-del-route", ProtocolType="HTTP")["ApiId"]
    route_id = apigw.create_route(ApiId=api_id, RouteKey="GET /qa")["RouteId"]
    apigw.delete_route(ApiId=api_id, RouteId=route_id)
    routes = apigw.get_routes(ApiId=api_id)["Items"]
    assert not any(r["RouteId"] == route_id for r in routes)

def test_apigw_stage_variables(apigw):
    """CreateStage with stageVariables stores and returns them."""
    api_id = apigw.create_api(Name="qa-apigw-stage-vars", ProtocolType="HTTP")["ApiId"]
    apigw.create_stage(
        ApiId=api_id,
        StageName="dev",
        StageVariables={"env": "development", "version": "1"},
    )
    stage = apigw.get_stage(ApiId=api_id, StageName="dev")
    assert stage["StageVariables"]["env"] == "development"
    assert stage["StageVariables"]["version"] == "1"

def test_apigw_v2_stage_timestamps(apigw):
    """API Gateway v2 Stage timestamps should be ISO8601 (datetime)."""
    from datetime import datetime
    api = apigw.create_api(Name="ts-stage-v44", ProtocolType="HTTP")
    api_id = api["ApiId"]
    stage = apigw.create_stage(ApiId=api_id, StageName="test-stage")
    assert isinstance(stage["CreatedDate"], datetime), f"CreatedDate should be datetime, got {type(stage['CreatedDate'])}"
    assert isinstance(stage["LastUpdatedDate"], datetime), f"LastUpdatedDate should be datetime, got {type(stage['LastUpdatedDate'])}"
    apigw.delete_api(ApiId=api_id)
