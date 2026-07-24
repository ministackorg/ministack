import io
import json
import os
import time
import urllib.request
import uuid as _uuid_mod
import zipfile
from urllib.parse import quote, urlparse

import boto3
import pytest
from botocore.exceptions import ClientError

_ENDPOINT = "http://localhost:4566"


def _client(region):
    return boto3.client(
        "appsync",
        endpoint_url=_ENDPOINT,
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def test_appsync_create_and_list_api():
    """Create a GraphQL API and list it."""
    from conftest import make_client
    appsync = make_client("appsync")
    resp = appsync.create_graphql_api(name="test-api", authenticationType="API_KEY")
    api = resp["graphqlApi"]
    assert api["name"] == "test-api"
    assert api["apiId"]
    assert api["authenticationType"] == "API_KEY"

    apis = appsync.list_graphql_apis()["graphqlApis"]
    assert any(a["apiId"] == api["apiId"] for a in apis)


def test_appsync_graphql_apis_are_region_scoped():
    east = _client("us-east-1")
    west = _client("us-west-2")
    east_api = east.create_graphql_api(
        name="regional-api-east", authenticationType="API_KEY"
    )["graphqlApi"]
    west_api = west.create_graphql_api(
        name="regional-api-west", authenticationType="API_KEY"
    )["graphqlApi"]

    try:
        east_ids = {api["apiId"] for api in east.list_graphql_apis()["graphqlApis"]}
        west_ids = {api["apiId"] for api in west.list_graphql_apis()["graphqlApis"]}
        assert east_api["apiId"] in east_ids
        assert east_api["apiId"] not in west_ids
        assert west_api["apiId"] in west_ids
        assert west_api["apiId"] not in east_ids
        assert ":us-east-1:" in east_api["arn"]
        assert ":us-west-2:" in west_api["arn"]

        # Local data-plane URLs do not encode or sign a region. The API ID
        # must select its stored region before resolver execution.
        west_graphql = _appsync_graphql_post(
            f"{_ENDPOINT}/v1/apis/{west_api['apiId']}/graphql",
            "{ __typename }",
        )
        assert "errors" not in west_graphql
    finally:
        east.delete_graphql_api(apiId=east_api["apiId"])
        west.delete_graphql_api(apiId=west_api["apiId"])


@pytest.mark.parametrize("credential_location", ["header", "query"])
def test_appsync_signed_graphql_request_preserves_region(credential_location):
    east = _client("us-east-1")
    api = east.create_graphql_api(
        name=f"signed-region-{credential_location}", authenticationType="API_KEY"
    )["graphqlApi"]
    url = f"{_ENDPOINT}/v1/apis/{api['apiId']}/graphql"
    headers = {}
    credential = "test/20260722/us-west-2/appsync/aws4_request"
    if credential_location == "header":
        headers["Authorization"] = (
            f"AWS4-HMAC-SHA256 Credential={credential}, "
            "SignedHeaders=host;x-amz-date, Signature=fake"
        )
    else:
        url = f"{url}?X-Amz-Credential={quote(credential, safe='')}"

    try:
        with pytest.raises(urllib.error.HTTPError) as exc:
            _appsync_graphql_post(url, "{ __typename }", headers=headers)
        assert exc.value.code == 404
    finally:
        east.delete_graphql_api(apiId=api["apiId"])


@pytest.mark.parametrize("api_selector", ["api_key", "single_api_fallback"])
def test_appsync_signed_root_graphql_request_preserves_region(api_selector):
    east = _client("us-east-1")
    api = east.create_graphql_api(
        name=f"signed-root-region-{api_selector}",
        authenticationType="API_KEY" if api_selector == "api_key" else "AWS_IAM",
    )["graphqlApi"]
    url = f"{_ENDPOINT}/graphql"
    headers = {
        "Host": f"{api['apiId']}.appsync-api.us-east-1.localhost:4566",
        "Authorization": (
            "AWS4-HMAC-SHA256 "
            "Credential=test/20260722/us-west-2/appsync/aws4_request, "
            "SignedHeaders=host;x-amz-date, Signature=fake"
        ),
    }
    if api_selector == "api_key":
        headers["x-api-key"] = east.create_api_key(apiId=api["apiId"])["apiKey"]["id"]

    try:
        with pytest.raises(urllib.error.HTTPError) as exc:
            _appsync_graphql_post(url, "{ __typename }", headers=headers)
        assert exc.value.code == 401
    finally:
        east.delete_graphql_api(apiId=api["apiId"])


def test_appsync_signed_root_graphql_request_uses_current_region_fallback():
    west = _client("us-west-2")
    api = west.create_graphql_api(
        name="signed-root-current-region-fallback",
        authenticationType="AWS_IAM",
    )["graphqlApi"]
    headers = {
        "Host": f"{api['apiId']}.appsync-api.us-west-2.localhost:4566",
        "Authorization": (
            "AWS4-HMAC-SHA256 "
            "Credential=test/20260722/us-west-2/appsync/aws4_request, "
            "SignedHeaders=host;x-amz-date, Signature=fake"
        ),
    }

    try:
        response = _appsync_graphql_post(
            f"{_ENDPOINT}/graphql",
            "{ __typename }",
            headers=headers,
        )
        assert "errors" not in response
    finally:
        west.delete_graphql_api(apiId=api["apiId"])


def test_appsync_graphql_honors_dynamodb_data_source_region():
    east_ddb = boto3.client(
        "dynamodb",
        endpoint_url=_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    west_appsync = _client("us-west-2")
    table_name = f"appsync-cross-region-{_uuid_mod.uuid4().hex[:12]}"
    east_ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    east_ddb.put_item(
        TableName=table_name,
        Item={"id": {"S": "item-1"}, "name": {"S": "East item"}},
    )
    api = west_appsync.create_graphql_api(
        name="cross-region-ddb", authenticationType="API_KEY"
    )["graphqlApi"]
    west_appsync.create_data_source(
        apiId=api["apiId"],
        name="east-table",
        type="AMAZON_DYNAMODB",
        dynamodbConfig={"tableName": table_name, "awsRegion": "us-east-1"},
    )
    west_appsync.create_resolver(
        apiId=api["apiId"],
        typeName="Query",
        fieldName="getItem",
        dataSourceName="east-table",
    )

    try:
        response = _appsync_graphql_post(
            f"{_ENDPOINT}/v1/apis/{api['apiId']}/graphql",
            'query { getItem(id: "item-1") { id name } }',
        )
        assert response["data"]["getItem"] == {
            "id": "item-1",
            "name": "East item",
        }
    finally:
        west_appsync.delete_graphql_api(apiId=api["apiId"])
        east_ddb.delete_table(TableName=table_name)


def test_appsync_legacy_children_restore_beside_parent_api():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import appsync as service

    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    boot_region = "us-east-1"
    api_region = "us-west-2"
    api_id = "legacy-api"
    apis = AccountScopedDict()
    tags = AccountScopedDict()

    set_request_account_id(account_id)
    set_request_region(boot_region)
    api_arn = f"arn:aws:appsync:{api_region}:{account_id}:apis/{api_id}"
    apis[api_id] = {"apiId": api_id, "arn": api_arn}
    tags[api_arn] = {"legacy": "true"}
    payload = {"apis": apis, "tags": tags}
    for key in ("api_keys", "data_sources", "resolvers", "types"):
        children = AccountScopedDict()
        children[api_id] = {"legacy": key}
        payload[key] = children

    service.reset()
    try:
        service.restore_state(payload)
        assert service._apis.get_scoped(account_id, api_region, api_id)["arn"] == api_arn
        for store in (
            service._api_keys,
            service._data_sources,
            service._resolvers,
            service._types,
        ):
            assert store.get_scoped(account_id, api_region, api_id) is not None
            assert store.get_scoped(account_id, boot_region, api_id) is None
        assert service._tags.get(api_arn) == {"legacy": "true"}
    finally:
        service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_appsync_reset_clears_all_regions():
    from ministack.core.responses import get_region, set_request_region
    from ministack.services import appsync as service

    original_region = get_region()
    regional_stores = (
        service._apis,
        service._api_keys,
        service._data_sources,
        service._resolvers,
        service._types,
    )
    service.reset()
    try:
        for region in ("us-east-1", "us-west-2"):
            set_request_region(region)
            for store in regional_stores:
                store[f"resource-{region}"] = {"region": region}
        service._tags["arn:aws:appsync:us-east-1:000000000000:apis/tagged"] = {
            "tag": "value"
        }

        service.reset()
        assert all(not store.has_any() for store in regional_stores)
        assert not service._tags._data
    finally:
        service.reset()
        set_request_region(original_region)

def test_appsync_get_and_delete_api():
    from conftest import make_client
    appsync = make_client("appsync")
    resp = appsync.create_graphql_api(name="del-api", authenticationType="API_KEY")
    api_id = resp["graphqlApi"]["apiId"]
    got = appsync.get_graphql_api(apiId=api_id)
    assert got["graphqlApi"]["name"] == "del-api"
    appsync.delete_graphql_api(apiId=api_id)
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError):
        appsync.get_graphql_api(apiId=api_id)

def test_appsync_api_key_crud():
    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="key-api", authenticationType="API_KEY")["graphqlApi"]
    key = appsync.create_api_key(apiId=api["apiId"])["apiKey"]
    assert key["id"]
    keys = appsync.list_api_keys(apiId=api["apiId"])["apiKeys"]
    assert len(keys) >= 1
    appsync.delete_api_key(apiId=api["apiId"], id=key["id"])


def test_appsync_tags_reject_wrong_region_api_arn(appsync):
    import boto3

    api = appsync.create_graphql_api(name="tag-region-api", authenticationType="API_KEY")["graphqlApi"]
    arn_parts = api["arn"].split(":")
    wrong_region = "us-west-2" if arn_parts[3] != "us-west-2" else "us-east-2"
    arn_parts[3] = wrong_region
    wrong_region_arn = ":".join(arn_parts)
    regional_appsync = boto3.client(
        "appsync",
        endpoint_url=_ENDPOINT,
        region_name=wrong_region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    with pytest.raises(ClientError) as exc:
        regional_appsync.tag_resource(resourceArn=wrong_region_arn, tags={"env": "test"})

    assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_appsync_data_source_crud():
    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="ds-api", authenticationType="API_KEY")["graphqlApi"]
    ds = appsync.create_data_source(
        apiId=api["apiId"], name="myds", type="AMAZON_DYNAMODB",
        dynamodbConfig={"tableName": "test-table", "awsRegion": "us-east-1"},
    )["dataSource"]
    assert ds["name"] == "myds"
    got = appsync.get_data_source(apiId=api["apiId"], name="myds")
    assert got["dataSource"]["name"] == "myds"
    appsync.delete_data_source(apiId=api["apiId"], name="myds")

def test_appsync_graphql_create_and_query(ddb):
    """Full AppSync flow: create API + data source + resolver, then execute GraphQL."""
    from conftest import make_client
    appsync = make_client("appsync")

    # Create DynamoDB table
    ddb.create_table(
        TableName="gql-users",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    # Create API
    api = appsync.create_graphql_api(name="gql-test", authenticationType="API_KEY")["graphqlApi"]
    api_id = api["apiId"]

    # Create API key
    key = appsync.create_api_key(apiId=api_id)["apiKey"]

    # Create data source
    appsync.create_data_source(
        apiId=api_id, name="usersDS", type="AMAZON_DYNAMODB",
        dynamodbConfig={"tableName": "gql-users", "awsRegion": "us-east-1"},
    )

    # Create resolvers
    appsync.create_resolver(
        apiId=api_id, typeName="Mutation", fieldName="createUser",
        dataSourceName="usersDS",
    )
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="getUser",
        dataSourceName="usersDS",
    )
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="listUsers",
        dataSourceName="usersDS",
    )

    # Execute mutation via HTTP
    import json as _json
    import urllib.request
    mutation = _json.dumps({
        "query": 'mutation CreateUser { createUser(input: {id: "u1", name: "Alice", email: "alice@example.com"}) { id name email } }',
    }).encode()
    req = urllib.request.Request(
        f"http://localhost:4566/v1/apis/{api_id}/graphql",
        data=mutation,
        headers={"Content-Type": "application/json", "x-api-key": key["id"]},
    )
    with urllib.request.urlopen(req) as r:
        resp = _json.loads(r.read())
    assert "data" in resp
    assert resp["data"]["createUser"]["name"] == "Alice"

    # Query
    query = _json.dumps({
        "query": 'query GetUser { getUser(id: "u1") { id name email } }',
    }).encode()
    req = urllib.request.Request(
        f"http://localhost:4566/v1/apis/{api_id}/graphql",
        data=query,
        headers={"Content-Type": "application/json", "x-api-key": key["id"]},
    )
    with urllib.request.urlopen(req) as r:
        resp = _json.loads(r.read())
    assert resp["data"]["getUser"]["name"] == "Alice"
    assert resp["data"]["getUser"]["id"] == "u1"

    # List
    list_q = _json.dumps({
        "query": "query ListUsers { listUsers { items { id name } } }",
    }).encode()
    req = urllib.request.Request(
        f"http://localhost:4566/v1/apis/{api_id}/graphql",
        data=list_q,
        headers={"Content-Type": "application/json", "x-api-key": key["id"]},
    )
    with urllib.request.urlopen(req) as r:
        resp = _json.loads(r.read())
    items = resp["data"]["listUsers"]["items"]
    assert len(items) >= 1
    assert any(u["name"] == "Alice" for u in items)

def test_appsync_graphql_update_mutation(ddb):
    """Update an existing item via GraphQL mutation."""
    import json as _json
    import urllib.request

    from conftest import make_client
    appsync = make_client("appsync")

    try:
        ddb.create_table(TableName="gql-update", KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                         AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
    except Exception:
        pass

    api = appsync.create_graphql_api(name="gql-upd", authenticationType="API_KEY")["graphqlApi"]
    key = appsync.create_api_key(apiId=api["apiId"])["apiKey"]
    appsync.create_data_source(apiId=api["apiId"], name="ds", type="AMAZON_DYNAMODB",
                               dynamodbConfig={"tableName": "gql-update", "awsRegion": "us-east-1"})
    appsync.create_resolver(apiId=api["apiId"], typeName="Mutation", fieldName="createItem", dataSourceName="ds")
    appsync.create_resolver(apiId=api["apiId"], typeName="Mutation", fieldName="updateItem", dataSourceName="ds")
    appsync.create_resolver(apiId=api["apiId"], typeName="Query", fieldName="getItem", dataSourceName="ds")

    def gql(query):
        req = urllib.request.Request(f"http://localhost:4566/v1/apis/{api['apiId']}/graphql",
            data=_json.dumps({"query": query}).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return _json.loads(r.read())

    # Create
    gql('mutation { createItem(input: {id: "i1", title: "Original"}) { id title } }')
    # Update
    resp = gql('mutation { updateItem(input: {id: "i1", title: "Updated"}) { id title } }')
    assert resp["data"]["updateItem"]["title"] == "Updated"
    # Verify via get
    resp = gql('query { getItem(id: "i1") { id title } }')
    assert resp["data"]["getItem"]["title"] == "Updated"

def test_appsync_graphql_delete_mutation(ddb):
    """Delete an item via GraphQL mutation."""
    import json as _json
    import urllib.request

    from conftest import make_client
    appsync = make_client("appsync")

    try:
        ddb.create_table(TableName="gql-del", KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                         AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
    except Exception:
        pass

    api = appsync.create_graphql_api(name="gql-del", authenticationType="API_KEY")["graphqlApi"]
    appsync.create_data_source(apiId=api["apiId"], name="ds", type="AMAZON_DYNAMODB",
                               dynamodbConfig={"tableName": "gql-del", "awsRegion": "us-east-1"})
    appsync.create_resolver(apiId=api["apiId"], typeName="Mutation", fieldName="createItem", dataSourceName="ds")
    appsync.create_resolver(apiId=api["apiId"], typeName="Mutation", fieldName="deleteItem", dataSourceName="ds")
    appsync.create_resolver(apiId=api["apiId"], typeName="Query", fieldName="getItem", dataSourceName="ds")

    def gql(query):
        req = urllib.request.Request(f"http://localhost:4566/v1/apis/{api['apiId']}/graphql",
            data=_json.dumps({"query": query}).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return _json.loads(r.read())

    gql('mutation { createItem(input: {id: "d1", title: "Doomed"}) { id } }')
    resp = gql('mutation { deleteItem(input: {id: "d1"}) { id title } }')
    assert resp["data"]["deleteItem"]["id"] == "d1"
    # Verify deleted
    resp = gql('query { getItem(id: "d1") { id } }')
    assert resp["data"]["getItem"] is None

def test_appsync_graphql_with_variables():
    """GraphQL query using $variables."""
    import json as _json
    import urllib.request

    from conftest import make_client
    appsync = make_client("appsync")
    ddb_client = make_client("dynamodb")

    try:
        ddb_client.create_table(TableName="gql-vars", KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                         AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
    except Exception:
        pass

    api = appsync.create_graphql_api(name="gql-vars", authenticationType="API_KEY")["graphqlApi"]
    appsync.create_data_source(apiId=api["apiId"], name="ds", type="AMAZON_DYNAMODB",
                               dynamodbConfig={"tableName": "gql-vars", "awsRegion": "us-east-1"})
    appsync.create_resolver(apiId=api["apiId"], typeName="Mutation", fieldName="createItem", dataSourceName="ds")
    appsync.create_resolver(apiId=api["apiId"], typeName="Query", fieldName="getItem", dataSourceName="ds")

    def gql(query, variables=None):
        body = {"query": query}
        if variables:
            body["variables"] = variables
        req = urllib.request.Request(f"http://localhost:4566/v1/apis/{api['apiId']}/graphql",
            data=_json.dumps(body).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            return _json.loads(r.read())

    gql('mutation { createItem(input: {id: "v1", name: "Var Test"}) { id } }')
    resp = gql('query GetItem($id: ID!) { getItem(id: $id) { id name } }', {"id": "v1"})
    assert resp["data"]["getItem"]["name"] == "Var Test"

def test_appsync_graphql_nonexistent_item():
    """Query for a non-existent item returns null."""
    import json as _json
    import urllib.request

    from conftest import make_client
    appsync = make_client("appsync")
    ddb_client = make_client("dynamodb")

    try:
        ddb_client.create_table(TableName="gql-404", KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                         AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
    except Exception:
        pass

    api = appsync.create_graphql_api(name="gql-404", authenticationType="API_KEY")["graphqlApi"]
    appsync.create_data_source(apiId=api["apiId"], name="ds", type="AMAZON_DYNAMODB",
                               dynamodbConfig={"tableName": "gql-404", "awsRegion": "us-east-1"})
    appsync.create_resolver(apiId=api["apiId"], typeName="Query", fieldName="getItem", dataSourceName="ds")

    req = urllib.request.Request(f"http://localhost:4566/v1/apis/{api['apiId']}/graphql",
        data=_json.dumps({"query": 'query { getItem(id: "ghost") { id } }'}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        resp = _json.loads(r.read())
    assert resp["data"]["getItem"] is None

def test_appsync_graphql_nonexistent_api():
    """Query against a non-existent API returns 404."""
    import json as _json
    import urllib.request
    req = urllib.request.Request("http://localhost:4566/v1/apis/fake-api-id/graphql",
        data=_json.dumps({"query": "{ getItem(id: \"1\") { id } }"}).encode(),
        headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req)
        assert False, "Should have failed"
    except urllib.error.HTTPError as e:
        assert e.code == 404

def test_appsync_graphql_empty_query():
    """Empty query returns 400."""
    import json as _json
    import urllib.request

    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="gql-empty", authenticationType="API_KEY")["graphqlApi"]

    req = urllib.request.Request(f"http://localhost:4566/v1/apis/{api['apiId']}/graphql",
        data=_json.dumps({"query": ""}).encode(),
        headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req)
        assert False, "Should have failed"
    except urllib.error.HTTPError as e:
        assert e.code == 400


# ---------------------------------------------------------------------------
# AppSync Lambda resolver event shape — verifies full AppSyncResolverEvent is built.
# ---------------------------------------------------------------------------

def _appsync_lambda_zip(handler_code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", handler_code)
    return buf.getvalue()


def _appsync_create_lambda(lam, fn_name, handler_code):
    try:
        lam.delete_function(FunctionName=fn_name)
    except Exception:
        pass
    lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _appsync_lambda_zip(handler_code)},
    )
    return lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]


def _appsync_graphql_post(api_url, query, variables=None, headers=None):
    """Send a GraphQL POST request to the AppSync endpoint."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    req = urllib.request.Request(
        api_url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _appsync_setup_api_with_lambda_resolver(appsync, lam, fn_name, handler_code):
    """Create an AppSync API with API_KEY auth and a Lambda resolver for 'testField'."""
    fn_arn = _appsync_create_lambda(lam, fn_name, handler_code)

    api = appsync.create_graphql_api(
        name=f"test-api-{fn_name}",
        authenticationType="API_KEY",
    )
    api_id = api["graphqlApi"]["apiId"]
    api_key = appsync.create_api_key(apiId=api_id)["apiKey"]["id"]
    graphql_url = f"{_ENDPOINT}/v1/apis/{api_id}/graphql"

    appsync.create_data_source(
        apiId=api_id,
        name="LambdaDS",
        type="AWS_LAMBDA",
        lambdaConfig={"lambdaFunctionArn": fn_arn},
    )
    appsync.create_resolver(
        apiId=api_id,
        typeName="Query",
        fieldName="testField",
        dataSourceName="LambdaDS",
        kind="UNIT",
    )
    return api_id, api_key, graphql_url


_APPSYNC_IDENTITY_PROBE_RESOLVER = (
    "def handler(event, ctx):\n"
    "    return {'hasIdentity': event.get('identity') is not None}\n"
)


def _appsync_setup_lambda_auth_api(appsync, lam, name, authorizer_arn, resolver_code):
    """Create an AWS_LAMBDA-auth API with the given authorizer ARN and a Lambda resolver."""
    resolver_arn = _appsync_create_lambda(lam, f"{name}-resolver", resolver_code)

    api = appsync.create_graphql_api(
        name=name,
        authenticationType="AWS_LAMBDA",
        lambdaAuthorizerConfig={"authorizerUri": authorizer_arn},
    )
    api_id = api["graphqlApi"]["apiId"]
    graphql_url = f"{_ENDPOINT}/v1/apis/{api_id}/graphql"

    appsync.create_data_source(
        apiId=api_id, name="LambdaDS", type="AWS_LAMBDA",
        lambdaConfig={"lambdaFunctionArn": resolver_arn},
    )
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="testField",
        dataSourceName="LambdaDS", kind="UNIT",
    )
    return api_id, graphql_url


def _appsync_expect_unauthorized(url, query, headers):
    """A rejected AWS_LAMBDA authorizer must surface as HTTP 401 with an
    `UnauthorizedException` errors envelope, per the AppSync Developer Guide."""
    import urllib.error

    req = urllib.request.Request(
        url,
        data=json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as exc:
        assert exc.code == 401, f"expected 401, got {exc.code}"
        body = json.loads(exc.read())
        assert "errors" in body and body["errors"]
        assert body["errors"][0]["errorType"] == "UnauthorizedException"
        return body
    raise AssertionError("expected HTTP 401 but request succeeded")


def test_appsync_lambda_event_field_name(appsync, lam):
    """Lambda event must contain info.fieldName matching the queried GraphQL field."""
    handler = (
        "def handler(event, ctx):\n"
        "    return {'fieldName': event.get('info', {}).get('fieldName')}\n"
    )
    _, api_key, url = _appsync_setup_api_with_lambda_resolver(appsync, lam, "field-name-test", handler)
    resp = _appsync_graphql_post(url, "{ testField { fieldName } }", headers={"x-api-key": api_key})
    assert "errors" not in resp
    assert resp["data"]["testField"]["fieldName"] == "testField"


def test_appsync_lambda_event_arguments(appsync, lam):
    """Lambda event must contain parsed arguments from the GraphQL query."""
    handler = (
        "def handler(event, ctx):\n"
        "    args = event.get('arguments', {})\n"
        "    return {'receivedId': args.get('params', {}).get('id', 'missing')}\n"
    )
    _, api_key, url = _appsync_setup_api_with_lambda_resolver(appsync, lam, "args-test", handler)
    resp = _appsync_graphql_post(
        url, 'query { testField(params: {id: "issuer-abc"}) { receivedId } }',
        headers={"x-api-key": api_key},
    )
    assert "errors" not in resp
    assert resp["data"]["testField"]["receivedId"] == "issuer-abc"


def test_appsync_lambda_event_api_key_header(appsync, lam):
    """The x-api-key header must be in event.request.headers so isApiKeyAuthenticated() works."""
    handler = (
        "def handler(event, ctx):\n"
        "    headers = event.get('request', {}).get('headers', {})\n"
        "    return {'hasApiKey': 'x-api-key' in headers}\n"
    )
    _, api_key, url = _appsync_setup_api_with_lambda_resolver(appsync, lam, "api-key-header-test", handler)
    resp = _appsync_graphql_post(url, "{ testField { hasApiKey } }", headers={"x-api-key": api_key})
    assert "errors" not in resp
    assert resp["data"]["testField"]["hasApiKey"] is True


def test_appsync_lambda_event_custom_headers_forwarded(appsync, lam):
    """x-request-id, x-session-id, x-user-id, x-workflow, x-process must be in event.request.headers."""
    handler = (
        "def handler(event, ctx):\n"
        "    h = event.get('request', {}).get('headers', {})\n"
        "    return {\n"
        "        'requestId': h.get('x-request-id', ''),\n"
        "        'sessionId': h.get('x-session-id', ''),\n"
        "        'userId': h.get('x-user-id', ''),\n"
        "        'workflow': h.get('x-workflow', ''),\n"
        "        'process': h.get('x-process', ''),\n"
        "    }\n"
    )
    _, api_key, url = _appsync_setup_api_with_lambda_resolver(appsync, lam, "custom-headers-test", handler)
    resp = _appsync_graphql_post(
        url, "{ testField { requestId sessionId userId workflow process } }",
        headers={
            "x-api-key": api_key,
            "x-request-id": "req-abc-123",
            "x-session-id": "sess-xyz-456",
            "x-user-id": "user-789",
            "x-workflow": "LOGIN",
            "x-process": "OTP_VERIFY",
        },
    )
    assert "errors" not in resp
    data = resp["data"]["testField"]
    assert data["requestId"] == "req-abc-123"
    assert data["sessionId"] == "sess-xyz-456"
    assert data["userId"] == "user-789"
    assert data["workflow"] == "LOGIN"
    assert data["process"] == "OTP_VERIFY"


def test_appsync_lambda_event_no_identity_in_api_key_mode(appsync, lam):
    """In API_KEY auth mode, event.identity must be absent or null."""
    handler = (
        "def handler(event, ctx):\n"
        "    return {'hasIdentity': event.get('identity') is not None}\n"
    )
    _, api_key, url = _appsync_setup_api_with_lambda_resolver(appsync, lam, "no-identity-test", handler)
    resp = _appsync_graphql_post(url, "{ testField { hasIdentity } }", headers={"x-api-key": api_key})
    assert "errors" not in resp
    assert resp["data"]["testField"]["hasIdentity"] is False


def test_appsync_lambda_event_identity_from_authorizer(appsync, lam):
    """AWS_LAMBDA auth mode — identity.resolverContext from authorizer is in Lambda event."""
    authorizer_code = (
        "def handler(event, ctx):\n"
        "    return {\n"
        "        'isAuthorized': True,\n"
        "        'resolverContext': {\n"
        "            'customId': 'test-user-id',\n"
        "            'email': 'user@example.com',\n"
        "            'cognitoGroups': 'admin',\n"
        "        }\n"
        "    }\n"
    )
    resolver_code = (
        "def handler(event, ctx):\n"
        "    identity = event.get('identity') or {}\n"
        "    rc = identity.get('resolverContext') or {}\n"
        "    return {\n"
        "        'customId': rc.get('customId', ''),\n"
        "        'email': rc.get('email', ''),\n"
        "    }\n"
    )
    auth_arn = _appsync_create_lambda(lam, "lambda-authorizer-test", authorizer_code)
    resolver_arn = _appsync_create_lambda(lam, "lambda-resolver-test", resolver_code)

    api = appsync.create_graphql_api(
        name="lambda-auth-api",
        authenticationType="AWS_LAMBDA",
        lambdaAuthorizerConfig={"authorizerUri": auth_arn},
    )
    api_id = api["graphqlApi"]["apiId"]
    url = f"{_ENDPOINT}/v1/apis/{api_id}/graphql"
    appsync.create_data_source(
        apiId=api_id, name="LambdaDS", type="AWS_LAMBDA",
        lambdaConfig={"lambdaFunctionArn": resolver_arn},
    )
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="testField",
        dataSourceName="LambdaDS", kind="UNIT",
    )
    resp = _appsync_graphql_post(url, "{ testField { customId email } }",
                                 headers={"Authorization": "Bearer fake-jwt-token"})
    assert "errors" not in resp
    assert resp["data"]["testField"]["customId"] == "test-user-id"
    assert resp["data"]["testField"]["email"] == "user@example.com"


def test_appsync_lambda_not_found_no_crash(appsync):
    """If Lambda function doesn't exist in ministack, AppSync returns a response (no crash)."""
    api = appsync.create_graphql_api(name="lambda-missing-api", authenticationType="API_KEY")
    api_id = api["graphqlApi"]["apiId"]
    api_key = appsync.create_api_key(apiId=api_id)["apiKey"]["id"]
    appsync.create_data_source(
        apiId=api_id, name="MissingLambdaDS", type="AWS_LAMBDA",
        lambdaConfig={"lambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:does-not-exist"},
    )
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="testField",
        dataSourceName="MissingLambdaDS", kind="UNIT",
    )
    resp = _appsync_graphql_post(f"{_ENDPOINT}/v1/apis/{api_id}/graphql", "{ testField }",
                                 headers={"x-api-key": api_key})
    assert "data" in resp or "errors" in resp


def test_appsync_lambda_returns_errors(appsync, lam):
    """Lambda returning {errors: ['INTERNAL_SERVER_ERROR']} is passed through correctly."""
    handler = (
        "def handler(event, ctx):\n"
        "    return {'errors': ['INTERNAL_SERVER_ERROR'], 'data': None}\n"
    )
    _, api_key, url = _appsync_setup_api_with_lambda_resolver(appsync, lam, "error-response-test", handler)
    resp = _appsync_graphql_post(url, "{ testField { id } }", headers={"x-api-key": api_key})
    assert resp["data"]["testField"]["errors"] == ["INTERNAL_SERVER_ERROR"]


def test_appsync_lambda_event_source_empty_for_root(appsync, lam):
    """event.source must be {} (empty) for top-level Query fields."""
    handler = (
        "def handler(event, ctx):\n"
        "    s = event.get('source')\n"
        "    return {'sourceIsEmpty': s == {} or s is None}\n"
    )
    _, api_key, url = _appsync_setup_api_with_lambda_resolver(appsync, lam, "source-test", handler)
    resp = _appsync_graphql_post(url, "{ testField { sourceIsEmpty } }", headers={"x-api-key": api_key})
    assert "errors" not in resp
    assert resp["data"]["testField"]["sourceIsEmpty"] is True


def test_appsync_lambda_event_variables_substituted(appsync, lam):
    """Variables in the query must be resolved to values before the event is built."""
    handler = (
        "def handler(event, ctx):\n"
        "    args = event.get('arguments', {})\n"
        "    return {'id': args.get('params', {}).get('id', 'missing')}\n"
    )
    _, api_key, url = _appsync_setup_api_with_lambda_resolver(appsync, lam, "variables-test", handler)
    resp = _appsync_graphql_post(
        url, "query GetTest($id: ID!) { testField(params: {id: $id}) { id } }",
        variables={"id": "issuer-from-var"},
        headers={"x-api-key": api_key},
    )
    assert "errors" not in resp
    assert resp["data"]["testField"]["id"] == "issuer-from-var"


def test_appsync_lambda_unhandled_exception_becomes_error(appsync, lam):
    """A Lambda that raises must yield a GraphQL `errors` entry, not fake `data`."""
    handler = (
        "def handler(event, ctx):\n"
        "    raise Exception('boom')\n"
    )
    _, api_key, url = _appsync_setup_api_with_lambda_resolver(appsync, lam, "raise-test", handler)
    resp = _appsync_graphql_post(url, "{ testField { id } }", headers={"x-api-key": api_key})
    field = resp.get("data", {}).get("testField")
    assert field is None or "errorMessage" not in field
    assert "errors" in resp or (isinstance(field, dict) and "errors" in field)


def test_appsync_lambda_authorizer_rejection_returns_unauthorized(appsync, lam):
    """AWS_LAMBDA auth — `isAuthorized:false` must reject with `UnauthorizedException` (HTTP 401)."""
    authorizer = (
        "def handler(event, ctx):\n"
        "    return {'isAuthorized': False, 'resolverContext': {'should': 'not-leak'}}\n"
    )
    auth_arn = _appsync_create_lambda(lam, "authz-reject-test", authorizer)
    _, url = _appsync_setup_lambda_auth_api(appsync, lam, "authz-reject-api", auth_arn,
                                            _APPSYNC_IDENTITY_PROBE_RESOLVER)
    _appsync_expect_unauthorized(url, "{ testField { hasIdentity } }",
                                 headers={"Authorization": "Bearer fake-jwt"})


def test_appsync_lambda_authorizer_wrong_region_arn_does_not_fallback(appsync, lam):
    """A wrong-region authorizer ARN must not invoke a same-named local Lambda."""
    authorizer = (
        "def handler(event, ctx):\n"
        "    return {'isAuthorized': True, 'resolverContext': {'region': 'current'}}\n"
    )
    auth_arn = _appsync_create_lambda(lam, "authz-wrong-region-test", authorizer)
    arn_parts = auth_arn.split(":")
    arn_parts[3] = "us-west-2" if arn_parts[3] != "us-west-2" else "us-east-2"
    wrong_region_arn = ":".join(arn_parts)

    _, url = _appsync_setup_lambda_auth_api(
        appsync, lam, "authz-wrong-region-api", wrong_region_arn,
        _APPSYNC_IDENTITY_PROBE_RESOLVER,
    )
    _appsync_expect_unauthorized(url, "{ testField { hasIdentity } }",
                                 headers={"Authorization": "Bearer fake-jwt"})


def test_appsync_lambda_missing_authorizer_returns_unauthorized(appsync, lam):
    """AWS_LAMBDA auth — missing authorizer Lambda must reject with `UnauthorizedException` (HTTP 401)."""
    _, url = _appsync_setup_lambda_auth_api(
        appsync, lam, "authz-missing-api",
        "arn:aws:lambda:us-east-1:000000000000:function:authorizer-does-not-exist",
        _APPSYNC_IDENTITY_PROBE_RESOLVER,
    )
    _appsync_expect_unauthorized(url, "{ testField { hasIdentity } }",
                                 headers={"Authorization": "Bearer fake-jwt"})


def test_appsync_lambda_failing_authorizer_returns_unauthorized(appsync, lam):
    """AWS_LAMBDA auth — raising authorizer must reject with `UnauthorizedException` (HTTP 401)."""
    authorizer = (
        "def handler(event, ctx):\n"
        "    raise Exception('authorizer boom')\n"
    )
    auth_arn = _appsync_create_lambda(lam, "authz-raise-test", authorizer)
    _, url = _appsync_setup_lambda_auth_api(appsync, lam, "authz-raise-api", auth_arn,
                                            _APPSYNC_IDENTITY_PROBE_RESOLVER)
    _appsync_expect_unauthorized(url, "{ testField { hasIdentity } }",
                                 headers={"Authorization": "Bearer fake-jwt"})
