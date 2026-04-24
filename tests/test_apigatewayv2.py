"""API Gateway v2 tests — HTTP API + WebSocket + common lifecycle."""

import io
import json
import os
import pytest
import time
import uuid as _uuid_mod
import zipfile
from botocore.exceptions import ClientError
from urllib.parse import urlparse


_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

_EXECUTE_PORT = urlparse(_endpoint).port or 4566

def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()

def _make_zip_js(code: str, filename: str = "index.js") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, code)
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


def test_apigw_integration_content_handling_strategy_roundtrip(apigw):
    """Regression for #439: contentHandlingStrategy must survive Create and Update.
    Without this, Terraform replans the field on every apply and the runtime
    silently misses CONVERT_TO_TEXT / CONVERT_TO_BINARY payload translation."""
    api_id = apigw.create_api(Name="qa-apigw-chs", ProtocolType="HTTP")["ApiId"]
    integ_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri="arn:aws:lambda:us-east-1:000000000000:function:chs-fn",
        ContentHandlingStrategy="CONVERT_TO_TEXT",
    )["IntegrationId"]
    integ = apigw.get_integration(ApiId=api_id, IntegrationId=integ_id)
    assert integ.get("ContentHandlingStrategy") == "CONVERT_TO_TEXT"

    apigw.update_integration(
        ApiId=api_id,
        IntegrationId=integ_id,
        ContentHandlingStrategy="CONVERT_TO_BINARY",
    )
    integ = apigw.get_integration(ApiId=api_id, IntegrationId=integ_id)
    assert integ.get("ContentHandlingStrategy") == "CONVERT_TO_BINARY"

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


# ========== from test_apigwv2.py ==========

def test_apigwv2_created_date_is_unix_timestamp(apigw):
    resp = apigw.create_api(Name="tf-date-test-v2", ProtocolType="HTTP")
    created = resp["CreatedDate"]
    import datetime

    assert isinstance(created, datetime.datetime), (
        f"CreatedDate should be datetime (parsed from Unix int), got {type(created)}"
    )
    apigw.delete_api(ApiId=resp["ApiId"])


# ========== from test_apigwv2_websocket.py ==========

"""API Gateway v2 WebSocket — end-to-end tests.

Covers:
  - CreateApi(protocolType=WEBSOCKET) control-plane defaults
  - Route/Integration CRUD for WS API
  - RouteResponse / IntegrationResponse CRUD
  - Live $connect / $default / $disconnect dispatch via a Lambda
  - @connections runtime API: PostToConnection, GetConnection, DeleteConnection
  - Client isolation when two sockets connect to the same API
  - Accept/reject of $connect based on Lambda statusCode

Uses a hand-rolled WebSocket client (stdlib only) to keep the project
dependency-free.
"""

import base64
import hashlib
import io
import json
import os
import socket
import struct
import time
import uuid
import zipfile
from urllib.parse import urlparse

import pytest


_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"




# ── Minimal stdlib WebSocket client ──────────────────────────────────────────
class _WSClient:
    """Blocking WebSocket client — just enough to drive tests."""

    def __init__(self, host: str, port: int, path: str, headers: dict | None = None):
        self._sock = socket.create_connection((host, port), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        request_headers = {
            "Host": f"{host}:{port}",
            "Upgrade": "websocket",
            "Connection": "Upgrade",
            "Sec-WebSocket-Key": key,
            "Sec-WebSocket-Version": "13",
        }
        if headers:
            request_headers.update(headers)
        lines = [f"GET {path} HTTP/1.1"]
        for k, v in request_headers.items():
            lines.append(f"{k}: {v}")
        lines.append("")
        lines.append("")
        self._sock.sendall("\r\n".join(lines).encode())
        self._buf = b""
        self._read_handshake(key)

    def _read_handshake(self, key: str) -> None:
        while b"\r\n\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise RuntimeError(f"handshake closed, got: {self._buf!r}")
            self._buf += chunk
        header_blob, self._buf = self._buf.split(b"\r\n\r\n", 1)
        first_line = header_blob.split(b"\r\n", 1)[0]
        if b"101" not in first_line:
            raise RuntimeError(f"WS handshake failed: {header_blob!r}")
        expected = base64.b64encode(hashlib.sha1((key + _WS_GUID).encode()).digest()).decode()
        if expected.encode() not in header_blob:
            raise RuntimeError("Sec-WebSocket-Accept mismatch")

    def send(self, text: str) -> None:
        payload = text.encode()
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        length = len(payload)
        if length < 126:
            header = struct.pack("!BB", 0x81, 0x80 | length)
        elif length < 65536:
            header = struct.pack("!BBH", 0x81, 0x80 | 126, length)
        else:
            header = struct.pack("!BBQ", 0x81, 0x80 | 127, length)
        self._sock.sendall(header + mask + masked)

    def recv(self, timeout: float = 3.0) -> str | None:
        """Return the next text or binary frame's payload as a string."""
        self._sock.settimeout(timeout)
        try:
            while True:
                frame = self._recv_frame()
                if frame is None:
                    return None
                opcode, payload = frame
                if opcode in (0x1, 0x2):   # text or binary
                    return payload.decode("utf-8", errors="replace")
                if opcode == 0x8:   # close
                    return None
                # ignore ping/pong for test purposes
        except socket.timeout:
            return None

    def _recv_all(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self._sock.recv(max(4096, n - len(self._buf)))
            if not chunk:
                return b""
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _recv_frame(self):
        hdr = self._recv_all(2)
        if len(hdr) < 2:
            return None
        b1, b2 = hdr[0], hdr[1]
        opcode = b1 & 0x0F
        masked = (b2 & 0x80) != 0
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._recv_all(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_all(8))[0]
        mask = self._recv_all(4) if masked else b""
        payload = self._recv_all(length)
        if masked:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return opcode, payload

    def close(self) -> None:
        try:
            # close frame (code 1000)
            self._sock.sendall(b"\x88\x82" + os.urandom(4) + b"\x03\xe8")
        except Exception:
            pass
        try:
            self._sock.close()
        except Exception:
            pass


# ── Fixtures / helpers ───────────────────────────────────────────────────────
_ECHO_CODE = """
import json

def handler(event, context):
    rc = event.get('requestContext', {})
    action = rc.get('routeKey', '$default')
    body_text = event.get('body', '')
    try:
        parsed = json.loads(body_text) if body_text else {}
    except Exception:
        parsed = {}
    # Echo the incoming frame with the connectionId for easy test assertions.
    resp = {
        'connectionId': rc.get('connectionId'),
        'eventType': rc.get('eventType'),
        'action': action,
        'body': parsed,
    }
    return {'statusCode': 200, 'body': json.dumps(resp)}
"""

_CONNECT_REJECT_CODE = """
def handler(event, context):
    # Force $connect rejection.
    return {'statusCode': 401, 'body': 'denied'}
"""


def _make_fn(lam, name: str, code: str) -> str:
    try:
        lam.delete_function(FunctionName=name)
    except Exception:
        pass
    lam.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    return f"arn:aws:lambda:us-east-1:000000000000:function:{name}"


def _wire_ws_api(apigw, lam, *, name_suffix: str,
                 connect_code: str | None = None,
                 default_code: str = _ECHO_CODE,
                 disconnect_code: str | None = None) -> tuple[str, dict]:
    """Create a WS API + routes + integrations and return (apiId, metadata)."""
    api = apigw.create_api(Name=f"ws-{name_suffix}", ProtocolType="WEBSOCKET")
    api_id = api["ApiId"]
    meta = {"created_functions": []}
    assert api.get("RouteSelectionExpression") == "$request.body.action"

    def _route(route_key: str, code: str):
        fn_name = f"ws-{name_suffix}-{route_key.lstrip('$')}-{uuid.uuid4().hex[:6]}"
        arn = _make_fn(lam, fn_name, code)
        meta["created_functions"].append(fn_name)
        integ = apigw.create_integration(
            ApiId=api_id,
            IntegrationType="AWS_PROXY",
            IntegrationUri=arn,
            IntegrationMethod="POST",
        )
        apigw.create_route(
            ApiId=api_id,
            RouteKey=route_key,
            Target=f"integrations/{integ['IntegrationId']}",
        )

    if connect_code is not None:
        _route("$connect", connect_code)
    _route("$default", default_code)
    if disconnect_code is not None:
        _route("$disconnect", disconnect_code)
    apigw.create_stage(ApiId=api_id, StageName="prod")
    return api_id, meta


# ── Control-plane tests ──────────────────────────────────────────────────────
def test_ws_create_api_defaults(apigw):
    """WEBSOCKET APIs default routeSelectionExpression to $request.body.action."""
    resp = apigw.create_api(Name="ws-defaults", ProtocolType="WEBSOCKET")
    assert resp["ProtocolType"] == "WEBSOCKET"
    assert resp["RouteSelectionExpression"] == "$request.body.action"


def test_ws_create_api_custom_rse(apigw):
    resp = apigw.create_api(
        Name="ws-custom-rse", ProtocolType="WEBSOCKET",
        RouteSelectionExpression="$request.body.type",
    )
    assert resp["RouteSelectionExpression"] == "$request.body.type"


def test_ws_route_response_crud(apigw):
    api_id = apigw.create_api(Name="ws-rr", ProtocolType="WEBSOCKET")["ApiId"]
    route = apigw.create_route(ApiId=api_id, RouteKey="sendMessage")
    rr = apigw.create_route_response(
        ApiId=api_id, RouteId=route["RouteId"], RouteResponseKey="$default",
    )
    assert rr["RouteResponseKey"] == "$default"
    assert "RouteResponseId" in rr
    got = apigw.get_route_responses(ApiId=api_id, RouteId=route["RouteId"])
    assert any(i["RouteResponseId"] == rr["RouteResponseId"] for i in got["Items"])
    apigw.delete_route_response(
        ApiId=api_id, RouteId=route["RouteId"], RouteResponseId=rr["RouteResponseId"],
    )


def test_ws_integration_response_crud(apigw):
    api_id = apigw.create_api(Name="ws-ir", ProtocolType="WEBSOCKET")["ApiId"]
    integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="MOCK",
    )
    ir = apigw.create_integration_response(
        ApiId=api_id, IntegrationId=integ["IntegrationId"],
        IntegrationResponseKey="/200/",
    )
    assert ir["IntegrationResponseKey"] == "/200/"
    assert "IntegrationResponseId" in ir
    got = apigw.get_integration_responses(ApiId=api_id, IntegrationId=integ["IntegrationId"])
    assert any(i["IntegrationResponseId"] == ir["IntegrationResponseId"] for i in got["Items"])


# ── Data-plane tests ─────────────────────────────────────────────────────────
def test_ws_connect_and_echo_via_default_route(apigw, lam):
    api_id, meta = _wire_ws_api(
        apigw, lam, name_suffix="echo",
        connect_code=None, default_code=_ECHO_CODE,
    )
    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        ws.send(json.dumps({"action": "sendMessage", "payload": "hi"}))
        resp = ws.recv()
        assert resp is not None, "no reply from Lambda"
        parsed = json.loads(resp)
        assert parsed["eventType"] == "MESSAGE"
        assert parsed["body"]["payload"] == "hi"
        assert parsed["connectionId"]
    finally:
        ws.close()


def test_ws_connect_route_accepts(apigw, lam):
    """$connect Lambda returning 200 accepts the upgrade."""
    api_id, _ = _wire_ws_api(
        apigw, lam, name_suffix="connect-ok",
        connect_code=_ECHO_CODE,
    )
    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        ws.send(json.dumps({"action": "x"}))
        # Should get a normal MESSAGE response — proves the socket is live.
        resp = ws.recv()
        assert resp is not None
    finally:
        ws.close()


def test_ws_connect_route_rejects(apigw, lam):
    """$connect Lambda returning non-2xx rejects the upgrade."""
    api_id, _ = _wire_ws_api(
        apigw, lam, name_suffix="connect-deny",
        connect_code=_CONNECT_REJECT_CODE,
    )
    with pytest.raises(Exception):
        _WSClient("localhost", _EXECUTE_PORT, "/prod",
                  headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})


def test_ws_post_to_connection_from_management_api(apigw, lam):
    """@connections PostToConnection pushes a message to the live socket."""
    import urllib.request

    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="p2c")
    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        # Drive a frame so the Lambda runs and returns the connectionId in its reply.
        ws.send(json.dumps({"action": "sendMessage"}))
        reply = ws.recv()
        conn_id = json.loads(reply)["connectionId"]
        assert conn_id

        # Push a message from a separate HTTP request (simulating server-side push).
        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/@connections/{conn_id}"
        req = urllib.request.Request(
            url, data=b"server-push-payload", method="POST",
            headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
        )
        r = urllib.request.urlopen(req, timeout=5)
        assert r.status == 200

        pushed = ws.recv(timeout=3)
        assert pushed == "server-push-payload"
    finally:
        ws.close()


def test_ws_get_connection_returns_metadata(apigw, lam):
    """@connections GetConnection returns connected-at / identity."""
    import urllib.request

    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="getc")
    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        ws.send(json.dumps({"action": "x"}))
        reply = ws.recv()
        conn_id = json.loads(reply)["connectionId"]

        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/@connections/{conn_id}"
        req = urllib.request.Request(
            url, method="GET",
            headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
        )
        r = urllib.request.urlopen(req, timeout=5)
        meta = json.loads(r.read())
        # Int epoch seconds, per ministack JSON timestamp convention.
        assert isinstance(meta["ConnectedAt"], int)
        assert isinstance(meta["LastActiveAt"], int)
        assert meta["Identity"]["sourceIp"]
    finally:
        ws.close()


def test_ws_delete_connection_closes_socket(apigw, lam):
    """@connections DeleteConnection terminates the WS session."""
    import urllib.request

    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="delc")
    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        ws.send(json.dumps({"action": "x"}))
        reply = ws.recv()
        conn_id = json.loads(reply)["connectionId"]

        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/@connections/{conn_id}"
        req = urllib.request.Request(
            url, method="DELETE",
            headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
        )
        r = urllib.request.urlopen(req, timeout=5)
        assert r.status in (200, 204)

        # Give the server a moment to close, then subsequent recv returns None.
        time.sleep(0.5)
        assert ws.recv(timeout=1.5) is None
    finally:
        ws.close()


def test_ws_post_to_unknown_connection_returns_410(apigw, lam):
    """@connections PostToConnection on an unknown id returns 410 GoneException."""
    import urllib.request
    import urllib.error

    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="gone")
    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/@connections/{uuid.uuid4().hex}"
    req = urllib.request.Request(
        url, data=b"hi", method="POST",
        headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
    )
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req, timeout=5)
    assert exc_info.value.code == 410


_CAPTURE_QS_CODE = """
import json

def handler(event, context):
    # Echo the $connect event's queryStringParameters so the test can assert on them.
    rc = event.get('requestContext', {})
    return {
        'statusCode': 200,
        'body': json.dumps({
            'qs': event.get('queryStringParameters'),
            'mvqs': event.get('multiValueQueryStringParameters'),
            'eventType': rc.get('eventType'),
        }),
    }
"""


def test_ws_connect_receives_query_string_parameters(apigw, lam):
    """$connect Lambda event exposes queryStringParameters + multiValueQueryStringParameters.

    After accepting, we send a frame and rely on the echo Lambda to confirm the
    socket is live; the test's primary assertion is that the $connect Lambda
    did NOT reject us (so QS params didn't break event validation).
    """
    # Two Lambdas: one on $connect that validates the QS param; one on $default that echoes.
    api_id = apigw.create_api(Name="ws-qs-gate", ProtocolType="WEBSOCKET")["ApiId"]

    gate_code = """
def handler(event, context):
    qs = event.get('queryStringParameters') or {}
    mvqs = event.get('multiValueQueryStringParameters') or {}
    # Reject unless the caller passed ?token=abc
    if qs.get('token') != 'abc':
        return {'statusCode': 401, 'body': 'denied'}
    # Also confirm multi-value came through when a key is repeated.
    if mvqs.get('tag') != ['a', 'b']:
        return {'statusCode': 401, 'body': 'mv missing'}
    return {'statusCode': 200}
"""
    gate_arn = _make_fn(lam, f"ws-qs-gate-connect-{uuid.uuid4().hex[:6]}", gate_code)
    echo_arn = _make_fn(lam, f"ws-qs-gate-default-{uuid.uuid4().hex[:6]}", _ECHO_CODE)

    gate_integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=gate_arn, IntegrationMethod="POST",
    )
    apigw.create_route(ApiId=api_id, RouteKey="$connect",
                       Target=f"integrations/{gate_integ['IntegrationId']}")

    echo_integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=echo_arn, IntegrationMethod="POST",
    )
    apigw.create_route(ApiId=api_id, RouteKey="$default",
                       Target=f"integrations/{echo_integ['IntegrationId']}")
    apigw.create_stage(ApiId=api_id, StageName="prod")

    # Without QS params → $connect rejects
    with pytest.raises(Exception):
        _WSClient("localhost", _EXECUTE_PORT, "/prod",
                  headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})

    # With ?token=abc&tag=a&tag=b → accepted, MESSAGE works.
    ws = _WSClient(
        "localhost", _EXECUTE_PORT, "/prod?token=abc&tag=a&tag=b",
        headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
    )
    try:
        ws.send(json.dumps({"action": "ping"}))
        resp = ws.recv()
        assert resp is not None
        assert json.loads(resp)["eventType"] == "MESSAGE"
    finally:
        ws.close()


def test_ws_mock_integration_returns_template_body(apigw):
    """WEBSOCKET routes with MOCK integration + responseTemplates return the template
    body on the socket without any Lambda invocation."""
    api_id = apigw.create_api(Name="ws-mock", ProtocolType="WEBSOCKET")["ApiId"]
    integ = apigw.create_integration(ApiId=api_id, IntegrationType="MOCK")
    apigw.create_route(ApiId=api_id, RouteKey="$default",
                       Target=f"integrations/{integ['IntegrationId']}")
    apigw.create_integration_response(
        ApiId=api_id, IntegrationId=integ["IntegrationId"],
        IntegrationResponseKey="$default",
        ResponseTemplates={"$default": '{"from":"mock"}'},
    )
    apigw.create_stage(ApiId=api_id, StageName="prod")

    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        ws.send(json.dumps({"action": "anything"}))
        resp = ws.recv()
        assert resp == '{"from":"mock"}'
    finally:
        ws.close()


def test_ws_two_clients_stay_isolated(apigw, lam):
    """Two WS sockets on the same API get distinct connectionIds and
    @connections messages don't cross-deliver."""
    import urllib.request

    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="iso")
    a = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                  headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    b = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                  headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        a.send(json.dumps({"action": "x"}))
        b.send(json.dumps({"action": "y"}))
        a_reply = json.loads(a.recv())
        b_reply = json.loads(b.recv())
        assert a_reply["connectionId"] != b_reply["connectionId"]

        # Push to A only — B should not receive it.
        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/@connections/{a_reply['connectionId']}"
        urllib.request.urlopen(urllib.request.Request(
            url, data=b"for-a-only", method="POST",
            headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
        ), timeout=5)

        got_a = a.recv(timeout=3)
        got_b = b.recv(timeout=1)
        assert got_a == "for-a-only"
        assert got_b is None
    finally:
        a.close()
        b.close()


# ========== Path-based data plane (issue #401) ==========

def test_apigwv2_path_based_execute_api_http(apigw, lam):
    """HTTP API reachable via /_aws/execute-api/{apiId}/{stage}/{path} without Host override."""
    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="pb-http")  # creates a WS API — reuse for wiring
    # But we need an HTTP API for this test. Build one explicitly.
    http_api_id = apigw.create_api(Name="pb-http-api", ProtocolType="HTTP")["ApiId"]
    fn_name = f"pb-http-fn-{uuid.uuid4().hex[:6]}"
    arn = _make_fn(lam, fn_name, _ECHO_CODE)
    integ = apigw.create_integration(
        ApiId=http_api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=arn, IntegrationMethod="POST",
    )
    apigw.create_route(
        ApiId=http_api_id, RouteKey="GET /hello",
        Target=f"integrations/{integ['IntegrationId']}",
    )
    apigw.create_stage(ApiId=http_api_id, StageName="prod")

    import urllib.request
    url = f"http://localhost:{_EXECUTE_PORT}/_aws/execute-api/{http_api_id}/prod/hello"
    r = urllib.request.urlopen(url, timeout=5)
    assert r.status == 200
    payload = json.loads(r.read())
    assert payload["eventType"] == "MESSAGE" or payload.get("action") == "GET /hello" or "hello" in str(payload)


def test_apigwv2_path_based_websocket(apigw, lam):
    """WebSocket reachable via ws://localhost/_aws/execute-api/{apiId}/{stage}."""
    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="pb-ws")
    ws = _WSClient(
        "localhost", _EXECUTE_PORT,
        f"/_aws/execute-api/{api_id}/prod",
        headers={"Host": f"localhost:{_EXECUTE_PORT}"},
    )
    try:
        ws.send(json.dumps({"action": "sendMessage", "payload": "path-based"}))
        resp = ws.recv()
        assert resp is not None
        parsed = json.loads(resp)
        assert parsed["body"]["payload"] == "path-based"
    finally:
        ws.close()


def test_apigwv1_path_based_restapi_legacy_user_request(apigw_v1, lam):
    """REST API v1 reachable via /restapis/{apiId}/{stage}/_user_request_/{path} (LocalStack legacy)."""
    import urllib.request

    api_id = apigw_v1.create_rest_api(name="pb-v1-api")["id"]
    root = apigw_v1.get_resources(restApiId=api_id)["items"][0]["id"]
    res_id = apigw_v1.create_resource(restApiId=api_id, parentId=root, pathPart="hello")["id"]
    apigw_v1.put_method(
        restApiId=api_id, resourceId=res_id, httpMethod="GET", authorizationType="NONE",
    )

    fn_name = f"pb-v1-fn-{uuid.uuid4().hex[:6]}"
    arn = _make_fn(lam, fn_name, _ECHO_CODE)
    apigw_v1.put_integration(
        restApiId=api_id, resourceId=res_id, httpMethod="GET",
        type="AWS_PROXY", integrationHttpMethod="POST",
        uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/{arn}/invocations",
    )
    apigw_v1.create_deployment(restApiId=api_id, stageName="prod")

    url = f"http://localhost:{_EXECUTE_PORT}/restapis/{api_id}/prod/_user_request_/hello"
    r = urllib.request.urlopen(url, timeout=5)
    assert r.status == 200


# ========== Custom/predictable API IDs via tags (issue #400) ==========

def test_apigwv2_custom_id_via_ms_custom_id_tag(apigw):
    """ms-custom-id tag pins the apiId to the caller-supplied value."""
    resp = apigw.create_api(
        Name="ms-custom-id-test", ProtocolType="HTTP",
        Tags={"ms-custom-id": "mypinnedid"},
    )
    assert resp["ApiId"] == "mypinnedid"
    assert "mypinnedid.execute-api" in resp["ApiEndpoint"]


def test_apigwv2_custom_id_rejects_ls_custom_id(apigw):
    """ls-custom-id (LocalStack's tag) is not supported. Callers get a clear
    BadRequestException pointing them at the ministack-native 'ms-custom-id'."""
    with pytest.raises(ClientError) as exc_info:
        apigw.create_api(
            Name="ls-reject-test", ProtocolType="HTTP",
            Tags={"ls-custom-id": "should-fail"},
        )
    assert exc_info.value.response["Error"]["Code"] == "BadRequestException"
    assert "ms-custom-id" in exc_info.value.response["Error"]["Message"]


def test_apigwv2_custom_id_duplicate_rejected(apigw):
    """Second CreateApi with the same ms-custom-id in the same account is rejected."""
    apigw.create_api(
        Name="dup-1", ProtocolType="HTTP",
        Tags={"ms-custom-id": "duplicated"},
    )
    with pytest.raises(ClientError) as exc_info:
        apigw.create_api(
            Name="dup-2", ProtocolType="HTTP",
            Tags={"ms-custom-id": "duplicated"},
        )
    assert exc_info.value.response["Error"]["Code"] == "ConflictException"
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409


def test_apigwv2_custom_id_absent_uses_random(apigw):
    """CreateApi without the tag continues to produce a random apiId."""
    resp = apigw.create_api(Name="random-id", ProtocolType="HTTP")
    assert len(resp["ApiId"]) == 8


# ========== Lambda alias qualifier in integrationUri (issue #407) ==========

def test_apigwv2_integration_uri_with_alias_qualifier_resolves_to_alias_target(apigw, lam):
    """HTTP integration pointing at arn:...:function:<name>:<alias> must resolve
    the alias to its target version, not try to invoke a function literally
    named after the alias (#407)."""
    import urllib.request

    # 1) Publish a Lambda that returns a distinctive string.
    code = """
def handler(event, context):
    return {'statusCode': 200, 'body': 'hello-from-alias'}
"""
    fn_name = f"alias-target-fn-{uuid.uuid4().hex[:6]}"
    try:
        lam.delete_function(FunctionName=fn_name)
    except Exception:
        pass
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.12", Role=_LAMBDA_ROLE,
        Handler="index.handler", Code={"ZipFile": _make_zip(code)}, Publish=True,
    )
    # Publishing on create yields version 1; create an alias pointing at it.
    lam.create_alias(FunctionName=fn_name, Name="live", FunctionVersion="1")

    # 2) Wire an HTTP API integration URI to the qualified ARN.
    api_id = apigw.create_api(Name="alias-integ", ProtocolType="HTTP")["ApiId"]
    qualified_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fn_name}:live"
    integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=qualified_arn, IntegrationMethod="POST",
    )
    apigw.create_route(
        ApiId=api_id, RouteKey="GET /hello",
        Target=f"integrations/{integ['IntegrationId']}",
    )
    apigw.create_stage(ApiId=api_id, StageName="live")

    # 3) Hit the route — before the fix this returned 502 "'live' not found".
    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/live/hello"
    req = urllib.request.Request(url)
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    r = urllib.request.urlopen(req, timeout=5)
    assert r.status == 200
    assert r.read() == b"hello-from-alias"


# ========== CORS configuration respected (issue #406) ==========

def _create_cors_api(apigw, *, name: str, cors: dict | None):
    kwargs = {"Name": name, "ProtocolType": "HTTP"}
    if cors is not None:
        kwargs["CorsConfiguration"] = cors
    api_id = apigw.create_api(**kwargs)["ApiId"]
    # Default stage so execute_path parsing works at the root.
    apigw.create_stage(ApiId=api_id, StageName="$default", AutoDeploy=True)
    return api_id


def test_apigwv2_cors_preflight_echoes_configured_origin(apigw):
    """OPTIONS preflight returns allow_origin from cors_configuration, not wildcard (#406)."""
    import urllib.request
    api_id = _create_cors_api(apigw, name="cors-origin", cors={
        "AllowOrigins": ["http://localhost:3000"],
        "AllowMethods": ["GET", "POST", "OPTIONS"],
        "AllowHeaders": ["content-type", "cookie"],
        "AllowCredentials": True,
        "MaxAge": 600,
    })
    req = urllib.request.Request(
        f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/",
        method="OPTIONS",
    )
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    req.add_header("Origin", "http://localhost:3000")
    req.add_header("Access-Control-Request-Method", "GET")
    r = urllib.request.urlopen(req, timeout=5)
    assert r.status == 204
    assert r.headers["Access-Control-Allow-Origin"] == "http://localhost:3000"
    assert r.headers["Access-Control-Allow-Credentials"] == "true"
    assert r.headers["Access-Control-Max-Age"] == "600"
    # methods/headers should reflect config
    assert "GET" in r.headers["Access-Control-Allow-Methods"]
    assert "cookie" in r.headers["Access-Control-Allow-Headers"].lower()


def test_apigwv2_cors_preflight_denies_non_allowlisted_origin(apigw):
    """OPTIONS from an origin not in allow_origins returns 403 with no CORS headers."""
    import urllib.request, urllib.error
    api_id = _create_cors_api(apigw, name="cors-deny", cors={
        "AllowOrigins": ["http://localhost:3000"],
        "AllowMethods": ["GET"],
    })
    req = urllib.request.Request(
        f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/",
        method="OPTIONS",
    )
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    req.add_header("Origin", "http://evil.example.com")
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req, timeout=5)
    assert exc_info.value.code == 403


def test_apigwv2_cors_preflight_403_when_no_configuration(apigw):
    """API without CorsConfiguration returns 403 on OPTIONS (AWS default)."""
    import urllib.request, urllib.error
    api_id = _create_cors_api(apigw, name="no-cors", cors=None)
    req = urllib.request.Request(
        f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/",
        method="OPTIONS",
    )
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    req.add_header("Origin", "http://localhost:3000")
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req, timeout=5)
    assert exc_info.value.code == 403


# ========== $default stage routing (issue #404) ==========

def test_apigwv2_default_stage_serves_from_root(apigw, lam):
    """v2 HTTP API with $default stage must route /api/hello to GET /api/hello —
    the first segment is NOT the stage name (#404)."""
    import urllib.request

    fn_name = f"default-stage-fn-{uuid.uuid4().hex[:6]}"
    arn = _make_fn(lam, fn_name, _ECHO_CODE)

    api_id = apigw.create_api(Name="default-stage-test", ProtocolType="HTTP")["ApiId"]
    integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=arn, IntegrationMethod="POST",
    )
    apigw.create_route(
        ApiId=api_id, RouteKey="GET /api/hello",
        Target=f"integrations/{integ['IntegrationId']}",
    )
    apigw.create_stage(ApiId=api_id, StageName="$default", AutoDeploy=True)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/api/hello"
    req = urllib.request.Request(url)
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    r = urllib.request.urlopen(req, timeout=5)
    assert r.status == 200


def test_apigwv2_named_stage_still_requires_prefix(apigw, lam):
    """APIs with a named stage (not $default) still require the stage in the path."""
    import urllib.request

    fn_name = f"named-stage-fn-{uuid.uuid4().hex[:6]}"
    arn = _make_fn(lam, fn_name, _ECHO_CODE)

    api_id = apigw.create_api(Name="named-stage-test", ProtocolType="HTTP")["ApiId"]
    integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=arn, IntegrationMethod="POST",
    )
    apigw.create_route(
        ApiId=api_id, RouteKey="GET /api/hello",
        Target=f"integrations/{integ['IntegrationId']}",
    )
    apigw.create_stage(ApiId=api_id, StageName="live", AutoDeploy=True)

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/live/api/hello"
    req = urllib.request.Request(url)
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    r = urllib.request.urlopen(req, timeout=5)
    assert r.status == 200


# ========== APIGW wrapper URI unwrap (issue #409) ==========

def _wrapped_uri(fn_arn: str) -> str:
    """Build the APIGW integration URI Terraform/AWS actually send."""
    return f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/{fn_arn}/invocations"


def test_apigwv2_integration_wrapped_function_arn(apigw, lam):
    """Wrapped integrationUri pointing at an unqualified Lambda ARN must invoke
    the function — regression test for 1.3.6's wrapper mis-parse (#409)."""
    import urllib.request

    code = "def handler(e,c): return {'statusCode': 200, 'body': 'wrapped-plain'}\n"
    fn_name = f"wrapped-plain-{uuid.uuid4().hex[:6]}"
    arn = _make_fn(lam, fn_name, code)

    api_id = apigw.create_api(Name="wrapped-plain-api", ProtocolType="HTTP")["ApiId"]
    integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=_wrapped_uri(arn), IntegrationMethod="POST",
    )
    apigw.create_route(ApiId=api_id, RouteKey="GET /hello",
                       Target=f"integrations/{integ['IntegrationId']}")
    apigw.create_stage(ApiId=api_id, StageName="$default", AutoDeploy=True)

    req = urllib.request.Request(f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/hello")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    r = urllib.request.urlopen(req, timeout=5)
    assert r.status == 200
    assert r.read() == b"wrapped-plain"


def test_apigwv2_integration_wrapped_alias_arn(apigw, lam):
    """Wrapped integrationUri pointing at an alias ARN must resolve to the
    alias target (combines #407 + #409)."""
    import urllib.request

    code = "def handler(e,c): return {'statusCode': 200, 'body': 'wrapped-alias'}\n"
    fn_name = f"wrapped-alias-{uuid.uuid4().hex[:6]}"
    lam.create_function(
        FunctionName=fn_name, Runtime="python3.12", Role=_LAMBDA_ROLE,
        Handler="index.handler", Code={"ZipFile": _make_zip(code)}, Publish=True,
    )
    lam.create_alias(FunctionName=fn_name, Name="live", FunctionVersion="1")
    alias_arn = f"arn:aws:lambda:us-east-1:000000000000:function:{fn_name}:live"

    api_id = apigw.create_api(Name="wrapped-alias-api", ProtocolType="HTTP")["ApiId"]
    integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=_wrapped_uri(alias_arn), IntegrationMethod="POST",
    )
    apigw.create_route(ApiId=api_id, RouteKey="GET /hello",
                       Target=f"integrations/{integ['IntegrationId']}")
    apigw.create_stage(ApiId=api_id, StageName="$default", AutoDeploy=True)

    req = urllib.request.Request(f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/hello")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    r = urllib.request.urlopen(req, timeout=5)
    assert r.status == 200
    assert r.read() == b"wrapped-alias"


def test_apigwv2_extract_lambda_ref_matrix():
    """Unit-level table test for every integrationUri shape we've seen in the
    wild. Lock the parser so #409-class bugs can't recur silently (#407, #409)."""
    from ministack.services.apigateway import _extract_lambda_ref_from_integration_uri as unwrap
    from ministack.services.lambda_svc import _resolve_name_and_qualifier as parse

    cases = [
        # wrapped (Terraform invoke_arn)
        ("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:my-function/invocations",
         "my-function", None),
        # wrapped + alias
        ("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:my-function:live/invocations",
         "my-function", "live"),
        # wrapped + version number
        ("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:my-function:3/invocations",
         "my-function", "3"),
        # wrapped + $LATEST explicit
        ("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:my-function:$LATEST/invocations",
         "my-function", "$LATEST"),
        # bare lambda ARN (direct CreateIntegration without wrapper)
        ("arn:aws:lambda:us-east-1:000000000000:function:my-function", "my-function", None),
        # bare alias ARN
        ("arn:aws:lambda:us-east-1:000000000000:function:my-function:live", "my-function", "live"),
        # plain function name
        ("my-function", "my-function", None),
        # plain name + qualifier
        ("my-function:live", "my-function", "live"),
        # empty string (degenerate — should not crash)
        ("", "", None),
        # future API version path — unwrap still works
        ("arn:aws:apigateway:us-east-1:lambda:path/2020-04-16/functions/arn:aws:lambda:us-east-1:000000000000:function:my-function/invocations",
         "my-function", None),
        # cross-account wrapper (parse OK; downstream per-account lookup handles existence)
        ("arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:999999999999:function:cross-acct/invocations",
         "cross-acct", None),
        # malformed: /invocations suffix without a wrapper
        ("my-function/invocations", "my-function", None),
    ]
    for uri, expected_name, expected_q in cases:
        ref = unwrap(uri)
        name, qualifier = parse(ref)
        assert name == expected_name, f"{uri!r} → name={name!r}, expected {expected_name!r}"
        assert qualifier == expected_q, f"{uri!r} → qualifier={qualifier!r}, expected {expected_q!r}"


def test_apigw_lambda_proxy_emits_cloudwatch_logs(apigw, lam, logs):
    """Lambda invoked via API Gateway v2 proxy must emit CloudWatch Logs."""
    import urllib.request as _urlreq

    fname = f"intg-apigw-cwl-{_uuid_mod.uuid4().hex[:8]}"
    marker = f"MARKER-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        "import sys\n"
        "def handler(event, context):\n"
        f"    print('{marker}')\n"
        "    return {'statusCode': 200, 'body': 'ok'}\n"
    )

    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    api_id = apigw.create_api(Name=f"cwl-test-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    route_id = apigw.create_route(
        ApiId=api_id,
        RouteKey="GET /cwltest",
        Target=f"integrations/{int_id}",
    )["RouteId"]
    apigw.create_stage(ApiId=api_id, StageName="$default")

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/cwltest"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200

    # Verify CloudWatch Logs contain the marker text
    log_group = f"/aws/lambda/{fname}"
    streams = logs.describe_log_streams(logGroupName=log_group)["logStreams"]
    assert len(streams) >= 1, f"Expected at least one log stream in {log_group}"

    all_messages = []
    for stream in streams:
        events = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream["logStreamName"],
        )["events"]
        all_messages.extend(e["message"] for e in events)

    assert any(marker in msg for msg in all_messages), (
        f"Marker '{marker}' not found in CloudWatch Logs. Messages: {all_messages}"
    )
    # Verify START/END/REPORT structure
    assert any(msg.startswith("START RequestId:") for msg in all_messages)
    assert any(msg.startswith("END RequestId:") for msg in all_messages)
    assert any(msg.startswith("REPORT RequestId:") for msg in all_messages)

    # Cleanup
    apigw.delete_route(ApiId=api_id, RouteId=route_id)
    apigw.delete_integration(ApiId=api_id, IntegrationId=int_id)
    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)


def test_apigw_lambda_proxy_emits_cloudwatch_logs_nodejs(apigw, lam, logs):
    """Node.js Lambda invoked via API Gateway v2 proxy must emit CloudWatch Logs."""
    import urllib.request as _urlreq

    fname = f"intg-apigw-cwl-js-{_uuid_mod.uuid4().hex[:8]}"
    marker = f"JSMARKER-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        "exports.handler = async (event) => {\n"
        f"  console.log('{marker}');\n"
        "  return { statusCode: 200, body: 'ok' };\n"
        "};\n"
    )

    lam.create_function(
        FunctionName=fname,
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(code)},
    )

    api_id = apigw.create_api(Name=f"cwl-js-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    route_id = apigw.create_route(
        ApiId=api_id,
        RouteKey="GET /cwljs",
        Target=f"integrations/{int_id}",
    )["RouteId"]
    apigw.create_stage(ApiId=api_id, StageName="$default")

    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/cwljs"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200

    log_group = f"/aws/lambda/{fname}"
    streams = logs.describe_log_streams(logGroupName=log_group)["logStreams"]
    assert len(streams) >= 1

    all_messages = []
    for stream in streams:
        events = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream["logStreamName"],
        )["events"]
        all_messages.extend(e["message"] for e in events)

    assert any(marker in msg for msg in all_messages), (
        f"Marker '{marker}' not found in CloudWatch Logs. Messages: {all_messages}"
    )
    assert any(msg.startswith("START RequestId:") for msg in all_messages)
    assert any(msg.startswith("END RequestId:") for msg in all_messages)
    assert any(msg.startswith("REPORT RequestId:") for msg in all_messages)

    apigw.delete_route(ApiId=api_id, RouteId=route_id)
    apigw.delete_integration(ApiId=api_id, IntegrationId=int_id)
    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)
