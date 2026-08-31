import io
import json
import os
import urllib.request
import uuid as _uuid_mod
import zipfile
from urllib.parse import quote, urlparse

import boto3
import pytest
from botocore.exceptions import ClientError

_ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
_ENDPOINT_NETLOC = urlparse(_ENDPOINT).netloc


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
        west_key = west.create_api_key(apiId=west_api["apiId"])["apiKey"]["id"]
        west_graphql = _appsync_graphql_post(
            f"{_ENDPOINT}/v1/apis/{west_api['apiId']}/graphql",
            "{ __typename }",
            headers={"x-api-key": west_key},
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
        "Host": f"{api['apiId']}.appsync-api.us-east-1.{_ENDPOINT_NETLOC}",
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
        "Host": f"{api['apiId']}.appsync-api.us-west-2.{_ENDPOINT_NETLOC}",
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

    api_key = west_appsync.create_api_key(apiId=api["apiId"])["apiKey"]["id"]

    try:
        response = _appsync_graphql_post(
            f"{_ENDPOINT}/v1/apis/{api['apiId']}/graphql",
            'query { getItem(id: "item-1") { id name } }',
            headers={"x-api-key": api_key},
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
        f"{_ENDPOINT}/v1/apis/{api_id}/graphql",
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
        f"{_ENDPOINT}/v1/apis/{api_id}/graphql",
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
        f"{_ENDPOINT}/v1/apis/{api_id}/graphql",
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
        req = urllib.request.Request(f"{_ENDPOINT}/v1/apis/{api['apiId']}/graphql",
            data=_json.dumps({"query": query}).encode(),
            headers={"Content-Type": "application/json", "x-api-key": key["id"]})
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
    key = appsync.create_api_key(apiId=api["apiId"])["apiKey"]
    appsync.create_data_source(apiId=api["apiId"], name="ds", type="AMAZON_DYNAMODB",
                               dynamodbConfig={"tableName": "gql-del", "awsRegion": "us-east-1"})
    appsync.create_resolver(apiId=api["apiId"], typeName="Mutation", fieldName="createItem", dataSourceName="ds")
    appsync.create_resolver(apiId=api["apiId"], typeName="Mutation", fieldName="deleteItem", dataSourceName="ds")
    appsync.create_resolver(apiId=api["apiId"], typeName="Query", fieldName="getItem", dataSourceName="ds")

    def gql(query):
        req = urllib.request.Request(f"{_ENDPOINT}/v1/apis/{api['apiId']}/graphql",
            data=_json.dumps({"query": query}).encode(),
            headers={"Content-Type": "application/json", "x-api-key": key["id"]})
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
    key = appsync.create_api_key(apiId=api["apiId"])["apiKey"]
    appsync.create_data_source(apiId=api["apiId"], name="ds", type="AMAZON_DYNAMODB",
                               dynamodbConfig={"tableName": "gql-vars", "awsRegion": "us-east-1"})
    appsync.create_resolver(apiId=api["apiId"], typeName="Mutation", fieldName="createItem", dataSourceName="ds")
    appsync.create_resolver(apiId=api["apiId"], typeName="Query", fieldName="getItem", dataSourceName="ds")

    def gql(query, variables=None):
        body = {"query": query}
        if variables:
            body["variables"] = variables
        req = urllib.request.Request(f"{_ENDPOINT}/v1/apis/{api['apiId']}/graphql",
            data=_json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "x-api-key": key["id"]})
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
    key = appsync.create_api_key(apiId=api["apiId"])["apiKey"]
    appsync.create_data_source(apiId=api["apiId"], name="ds", type="AMAZON_DYNAMODB",
                               dynamodbConfig={"tableName": "gql-404", "awsRegion": "us-east-1"})
    appsync.create_resolver(apiId=api["apiId"], typeName="Query", fieldName="getItem", dataSourceName="ds")

    req = urllib.request.Request(f"{_ENDPOINT}/v1/apis/{api['apiId']}/graphql",
        data=_json.dumps({"query": 'query { getItem(id: "ghost") { id } }'}).encode(),
        headers={"Content-Type": "application/json", "x-api-key": key["id"]})
    with urllib.request.urlopen(req) as r:
        resp = _json.loads(r.read())
    assert resp["data"]["getItem"] is None

def test_appsync_graphql_nonexistent_api():
    """Query against a non-existent API returns 404."""
    import json as _json
    import urllib.request
    req = urllib.request.Request(f"{_ENDPOINT}/v1/apis/fake-api-id/graphql",
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

    req = urllib.request.Request(f"{_ENDPOINT}/v1/apis/{api['apiId']}/graphql",
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


SDL = """type Query {
  hello: String
}
schema { query: Query }
"""


def test_appsync_schema_creation_and_introspection():
    """StartSchemaCreation stores the SDL; the status polls to SUCCESS and it reads back."""
    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="schema-api", authenticationType="API_KEY")["graphqlApi"]

    started = appsync.start_schema_creation(apiId=api["apiId"], definition=SDL.encode())
    assert started["status"] in ("PROCESSING", "SUCCESS")

    status = appsync.get_schema_creation_status(apiId=api["apiId"])
    assert status["status"] == "SUCCESS"

    schema = appsync.get_introspection_schema(apiId=api["apiId"], format="SDL")
    assert schema["schema"].read().decode() == SDL


def test_appsync_schema_creation_status_without_a_schema():
    """An API that has never had a schema reports NOT_APPLICABLE rather than failing."""
    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="noschema-api", authenticationType="API_KEY")["graphqlApi"]
    assert appsync.get_schema_creation_status(apiId=api["apiId"])["status"] == "NOT_APPLICABLE"


def test_appsync_pipeline_function_crud():
    """Create, get, list, update and delete a pipeline function."""
    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="fn-api", authenticationType="API_KEY")["graphqlApi"]
    appsync.create_data_source(apiId=api["apiId"], name="NoneDS", type="NONE")

    created = appsync.create_function(
        apiId=api["apiId"], name="fnOne", dataSourceName="NoneDS",
        functionVersion="2018-05-29",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="export function request(ctx){return {};} export function response(ctx){return ctx.result;}",
    )["functionConfiguration"]
    assert created["name"] == "fnOne"
    assert created["functionId"]
    assert created["functionArn"].endswith(f"/functions/{created['functionId']}")
    assert created["runtime"]["name"] == "APPSYNC_JS"

    got = appsync.get_function(apiId=api["apiId"], functionId=created["functionId"])
    assert got["functionConfiguration"]["name"] == "fnOne"

    listed = appsync.list_functions(apiId=api["apiId"])["functions"]
    assert [f["functionId"] for f in listed] == [created["functionId"]]

    updated = appsync.update_function(
        apiId=api["apiId"], functionId=created["functionId"],
        name="fnOneRenamed", dataSourceName="NoneDS", functionVersion="2018-05-29",
    )["functionConfiguration"]
    assert updated["name"] == "fnOneRenamed"
    assert updated["functionId"] == created["functionId"]

    appsync.delete_function(apiId=api["apiId"], functionId=created["functionId"])
    assert appsync.list_functions(apiId=api["apiId"])["functions"] == []


def test_appsync_pipeline_resolver_referencing_functions():
    """A PIPELINE resolver keeps the function ids it was created with.

    This is the shape Terraform produces: functions first, then a resolver whose
    pipelineConfig lists their ids.
    """
    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="pipe-api", authenticationType="API_KEY")["graphqlApi"]
    appsync.start_schema_creation(apiId=api["apiId"], definition=SDL.encode())
    appsync.create_data_source(apiId=api["apiId"], name="NoneDS", type="NONE")

    ids = [
        appsync.create_function(
            apiId=api["apiId"], name=f"step{i}", dataSourceName="NoneDS",
            functionVersion="2018-05-29",
        )["functionConfiguration"]["functionId"]
        for i in range(2)
    ]

    resolver = appsync.create_resolver(
        apiId=api["apiId"], typeName="Query", fieldName="hello",
        kind="PIPELINE", pipelineConfig={"functions": ids},
    )["resolver"]
    assert resolver["kind"] == "PIPELINE"
    assert resolver["pipelineConfig"]["functions"] == ids


def test_appsync_graphql_api_environment_variables():
    """Environment variables round-trip, and a put replaces the whole map."""
    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="env-api", authenticationType="API_KEY")["graphqlApi"]

    assert appsync.get_graphql_api_environment_variables(apiId=api["apiId"])["environmentVariables"] == {}

    put = appsync.put_graphql_api_environment_variables(
        apiId=api["apiId"], environmentVariables={"FEATURE": "on", "TIER": "local"},
    )["environmentVariables"]
    assert put == {"FEATURE": "on", "TIER": "local"}

    got = appsync.get_graphql_api_environment_variables(apiId=api["apiId"])["environmentVariables"]
    assert got == {"FEATURE": "on", "TIER": "local"}

    # AWS replaces rather than merges
    replaced = appsync.put_graphql_api_environment_variables(
        apiId=api["apiId"], environmentVariables={"ONLY": "this"},
    )["environmentVariables"]
    assert replaced == {"ONLY": "this"}


def test_appsync_update_data_source_and_resolver():
    """Terraform re-applies an existing API by updating in place, not recreating."""
    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="upd-api", authenticationType="API_KEY")["graphqlApi"]
    appsync.start_schema_creation(apiId=api["apiId"], definition=SDL.encode())
    appsync.create_data_source(apiId=api["apiId"], name="DS", type="NONE", description="first")

    updated = appsync.update_data_source(
        apiId=api["apiId"], name="DS", type="NONE", description="second",
    )["dataSource"]
    assert updated["description"] == "second"
    assert appsync.get_data_source(apiId=api["apiId"], name="DS")["dataSource"]["description"] == "second"

    appsync.create_resolver(
        apiId=api["apiId"], typeName="Query", fieldName="hello", dataSourceName="DS",
    )
    appsync.update_resolver(
        apiId=api["apiId"], typeName="Query", fieldName="hello", dataSourceName="DS",
        kind="PIPELINE", pipelineConfig={"functions": []},
    )
    got = appsync.get_resolver(apiId=api["apiId"], typeName="Query", fieldName="hello")["resolver"]
    assert got["kind"] == "PIPELINE"


def test_appsync_update_api_key():
    """UpdateApiKey changes the description without minting a new key."""
    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="key-api", authenticationType="API_KEY")["graphqlApi"]
    key = appsync.create_api_key(apiId=api["apiId"], description="before")["apiKey"]

    updated = appsync.update_api_key(
        apiId=api["apiId"], id=key["id"], description="after",
    )["apiKey"]
    assert updated["id"] == key["id"]
    assert updated["description"] == "after"


def test_appsync_graphql_api_reports_server_side_defaults():
    """apiType and visibility must come back, or a Terraform plan forces replacement.

    Both are ForceNew in the AWS provider, so an API that omits them reads as
    needing to be recreated on every plan — taking every datasource, function and
    resolver with it.
    """
    from conftest import make_client
    appsync = make_client("appsync")
    api = appsync.create_graphql_api(name="defaults-api", authenticationType="API_KEY")["graphqlApi"]
    assert api["apiType"] == "GRAPHQL"
    assert api["visibility"] == "GLOBAL"
    assert api["introspectionConfig"] == "ENABLED"

    got = appsync.get_graphql_api(apiId=api["apiId"])["graphqlApi"]
    assert got["apiType"] == "GRAPHQL"
    assert got["visibility"] == "GLOBAL"


def test_appsync_delete_api_removes_functions_and_schema():
    """Deleting an API must drop its functions and schema from the stores.

    Asserted against the stores directly: every read path checks the API exists
    first, so going through the API would report NotFoundException whether or not
    the child state was actually released.
    """
    import json as _json_mod

    from ministack.services import appsync as _appsync

    resp = _appsync._create_graphql_api({"name": "cascade", "authenticationType": "API_KEY"})
    api_id = _json_mod.loads(resp[2])["graphqlApi"]["apiId"]
    _appsync._start_schema_creation(api_id, {"definition": "type Query { hi: String }"})
    _appsync._create_data_source(api_id, {"name": "DS", "type": "NONE"})
    _appsync._create_function(api_id, {"name": "fn", "dataSourceName": "DS"})

    assert api_id in _appsync._functions
    assert api_id in _appsync._schemas

    _appsync._delete_graphql_api(api_id)

    assert api_id not in _appsync._functions, "functions leaked after DeleteGraphqlApi"
    assert api_id not in _appsync._schemas, "schema leaked after DeleteGraphqlApi"
    assert api_id not in _appsync._data_sources


def test_appsync_update_preserves_creation_time(monkeypatch):
    """An update must not restamp createdAt, as AWS does not.

    _now() has whole-second granularity, so a create and an update in the same
    second are indistinguishable; the clock is advanced between them.
    """
    import json as _json_mod

    from ministack.services import appsync as _appsync

    clock = {"t": 1_700_000_000}
    monkeypatch.setattr(_appsync, "_now", lambda: clock["t"])

    resp = _appsync._create_graphql_api({"name": "ctime", "authenticationType": "API_KEY"})
    api_id = _json_mod.loads(resp[2])["graphqlApi"]["apiId"]
    created = _json_mod.loads(
        _appsync._create_data_source(api_id, {"name": "DS", "type": "NONE", "description": "one"})[2]
    )["dataSource"]

    clock["t"] += 3600
    updated = _json_mod.loads(
        _appsync._update_data_source(api_id, "DS", {"type": "NONE", "description": "two"})[2]
    )["dataSource"]

    assert updated["description"] == "two"
    assert updated["createdAt"] == created["createdAt"], "createdAt was restamped by the update"
    assert updated["lastUpdatedAt"] == clock["t"], "lastUpdatedAt should advance"


def test_appsync_graphql_api_echoes_its_tags(appsync):
    """The GraphqlApi object must carry `tags`.

    AWS returns tags on the API itself, and that is where the Terraform provider
    reads them. CreateGraphqlApi stored them in the ARN-keyed tag store but left
    them off the record, so tags_all refreshed to empty and every plan proposed
    the same tag change.
    """
    api = appsync.create_graphql_api(
        name="qa-api-tags",
        authenticationType="API_KEY",
        tags={"ourco:env": "ministack"},
    )["graphqlApi"]
    assert api.get("tags") == {"ourco:env": "ministack"}

    got = appsync.get_graphql_api(apiId=api["apiId"])["graphqlApi"]
    assert got.get("tags") == {"ourco:env": "ministack"}

    listed = [
        a for a in appsync.list_graphql_apis()["graphqlApis"]
        if a["apiId"] == api["apiId"]
    ]
    assert listed and listed[0].get("tags") == {"ourco:env": "ministack"}

    # A tag added later through TagResource must show up on the API too.
    appsync.tag_resource(resourceArn=api["arn"], tags={"team": "platform"})
    got = appsync.get_graphql_api(apiId=api["apiId"])["graphqlApi"]
    assert got["tags"]["team"] == "platform"
    assert got["tags"]["ourco:env"] == "ministack"


def _cache_api(appsync, name):
    return appsync.create_graphql_api(name=name, authenticationType="API_KEY")[
        "graphqlApi"
    ]["apiId"]


def test_appsync_api_cache_lifecycle(appsync):
    """CreateApiCache / Get / Update / Flush / Delete.

    aws_appsync_api_cache cannot be applied at all without these, so an
    environment that enables caching in the cloud has to disable it locally and
    silently diverges from the thing it is meant to reproduce.
    """
    api_id = _cache_api(appsync, "qa-cache-lifecycle")

    created = appsync.create_api_cache(
        apiId=api_id,
        ttl=60,
        apiCachingBehavior="PER_RESOLVER_CACHING",
        type="SMALL",
        atRestEncryptionEnabled=True,
        transitEncryptionEnabled=True,
    )["apiCache"]
    assert created["ttl"] == 60
    assert created["apiCachingBehavior"] == "PER_RESOLVER_CACHING"
    assert created["type"] == "SMALL"
    assert created["atRestEncryptionEnabled"] is True
    assert created["transitEncryptionEnabled"] is True
    assert created["status"] == "AVAILABLE"

    got = appsync.get_api_cache(apiId=api_id)["apiCache"]
    assert got == created

    updated = appsync.update_api_cache(
        apiId=api_id, ttl=300, apiCachingBehavior="FULL_REQUEST_CACHING", type="MEDIUM"
    )["apiCache"]
    assert updated["ttl"] == 300
    assert updated["apiCachingBehavior"] == "FULL_REQUEST_CACHING"
    assert updated["type"] == "MEDIUM"
    # Encryption is set at create time and cannot be changed by an update; it
    # must survive one rather than silently reverting to false.
    assert updated["atRestEncryptionEnabled"] is True
    assert updated["transitEncryptionEnabled"] is True

    appsync.flush_api_cache(apiId=api_id)
    assert appsync.get_api_cache(apiId=api_id)["apiCache"]["ttl"] == 300

    appsync.delete_api_cache(apiId=api_id)
    with pytest.raises(ClientError) as e:
        appsync.get_api_cache(apiId=api_id)
    assert e.value.response["Error"]["Code"] == "NotFoundException"


def test_appsync_api_cache_rejects_a_second_cache_and_unknown_apis(appsync):
    """One cache per API, and the operations 404 on an API that does not exist."""
    api_id = _cache_api(appsync, "qa-cache-errors")
    appsync.create_api_cache(
        apiId=api_id, ttl=60, apiCachingBehavior="PER_RESOLVER_CACHING", type="SMALL"
    )

    with pytest.raises(ClientError) as e:
        appsync.create_api_cache(
            apiId=api_id, ttl=60, apiCachingBehavior="PER_RESOLVER_CACHING", type="SMALL"
        )
    assert e.value.response["Error"]["Code"] == "BadRequestException"

    for call in (
        lambda: appsync.get_api_cache(apiId="doesnotexist"),
        lambda: appsync.delete_api_cache(apiId="doesnotexist"),
        lambda: appsync.flush_api_cache(apiId="doesnotexist"),
    ):
        with pytest.raises(ClientError) as e:
            call()
        assert e.value.response["Error"]["Code"] == "NotFoundException"


def test_appsync_deleting_an_api_releases_its_cache(appsync):
    """A cache must not outlive its API — a later API reusing the id would
    otherwise inherit a cache nobody created."""
    api_id = _cache_api(appsync, "qa-cache-cascade")
    appsync.create_api_cache(
        apiId=api_id, ttl=60, apiCachingBehavior="PER_RESOLVER_CACHING", type="SMALL"
    )
    appsync.delete_graphql_api(apiId=api_id)

    with pytest.raises(ClientError) as e:
        appsync.get_api_cache(apiId=api_id)
    assert e.value.response["Error"]["Code"] == "NotFoundException"


def _graphql(api_id, api_key, query, variables=None):
    """POST a query to the API's data plane the way an SDK would."""
    import requests as _rq
    payload = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    r = _rq.post(f"{_ENDPOINT}/v1/apis/{api_id}/graphql",
                 headers={"x-api-key": api_key, "content-type": "application/json"},
                 json=payload, timeout=15)
    return r.json()

def _cache_ds_api(appsync, ddb, name):
    """An API with a DynamoDB-backed resolver, which is what ministack executes."""
    api = appsync.create_graphql_api(name=name, authenticationType="API_KEY")["graphqlApi"]
    api_id = api["apiId"]
    key = appsync.create_api_key(apiId=api_id)["apiKey"]["id"]
    table = f"{name}-table"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    appsync.create_data_source(
        apiId=api_id, name="Things", type="AMAZON_DYNAMODB",
        dynamodbConfig={"tableName": table, "awsRegion": "us-east-1"},
    )
    return api_id, key, table


def test_appsync_caches_a_resolver_that_declares_a_ttl(appsync, ddb):
    """A cached resolver must not re-read its datasource within the TTL.

    An API can declare an ApiCache and per-resolver cachingConfig, and both
    round-trip through the control plane, but the data plane ignored them: every
    query re-ran the resolver. A caching bug — a stale read, a key collision —
    could not be reproduced locally at all.
    """
    api_id, key, table = _cache_ds_api(appsync, ddb, "qa-cache-exec")
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="getThing", dataSourceName="Things",
        cachingConfig={"ttl": 300, "cachingKeys": ["$context.arguments.id"]},
    )
    appsync.create_api_cache(
        apiId=api_id, ttl=60, apiCachingBehavior="PER_RESOLVER_CACHING", type="SMALL")

    ddb.put_item(TableName=table, Item={"id": {"S": "t1"}, "v": {"S": "first"}})
    q = '{ getThing(id: "t1") { id v } }'
    first = _graphql(api_id, key, q)
    assert first["data"]["getThing"]["v"] == "first"

    # Change the row underneath. A cached resolver must still answer "first".
    ddb.put_item(TableName=table, Item={"id": {"S": "t1"}, "v": {"S": "second"}})
    cached = _graphql(api_id, key, q)
    assert cached["data"]["getThing"]["v"] == "first", "resolver was not cached"

    # A different argument is a different entry and must see the new value.
    ddb.put_item(TableName=table, Item={"id": {"S": "t2"}, "v": {"S": "other"}})
    other = _graphql(api_id, key, '{ getThing(id: "t2") { id v } }')
    assert other["data"]["getThing"]["v"] == "other", "caching keys must isolate entries"

    # FlushApiCache drops it, so the next read sees the new value.
    appsync.flush_api_cache(apiId=api_id)
    assert _graphql(api_id, key, q)["data"]["getThing"]["v"] == "second"


def test_appsync_does_not_cache_without_an_api_cache_or_a_ttl(appsync, ddb):
    """No ApiCache means no caching, and under PER_RESOLVER_CACHING a resolver
    that declares no ttl is not cached either — otherwise enabling the cache
    would silently start serving stale data from resolvers that never asked."""
    api_id, key, table = _cache_ds_api(appsync, ddb, "qa-cache-off")
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="getThing", dataSourceName="Things")

    ddb.put_item(TableName=table, Item={"id": {"S": "t1"}, "v": {"S": "first"}})
    q = '{ getThing(id: "t1") { id v } }'
    assert _graphql(api_id, key, q)["data"]["getThing"]["v"] == "first"
    ddb.put_item(TableName=table, Item={"id": {"S": "t1"}, "v": {"S": "second"}})
    # No cache on the API at all.
    assert _graphql(api_id, key, q)["data"]["getThing"]["v"] == "second"

    # Now add a cache, but the resolver still declares no ttl.
    appsync.create_api_cache(
        apiId=api_id, ttl=60, apiCachingBehavior="PER_RESOLVER_CACHING", type="SMALL")
    ddb.put_item(TableName=table, Item={"id": {"S": "t1"}, "v": {"S": "third"}})
    assert _graphql(api_id, key, q)["data"]["getThing"]["v"] == "third"


@pytest.fixture(scope="module")
def httpserver_url():
    """A tiny echo server for the HTTP data source test.

    Bound on all interfaces because the ministack under test may be another
    process — or a container — reaching back to the test host.
    """
    import http.server
    import socket
    import threading

    class _Echo(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("content-length", 0)))
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.end_headers()
            self.wfile.write(body or b"{}")

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("0.0.0.0", 0), _Echo)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    host = socket.gethostbyname(socket.gethostname())
    try:
        yield f"http://{host}:{srv.server_address[1]}"
    finally:
        srv.shutdown()

def _js_api(appsync, name, ds_type="NONE", **ds_kwargs):
    api = appsync.create_graphql_api(name=name, authenticationType="API_KEY")["graphqlApi"]
    api_id = api["apiId"]
    key = appsync.create_api_key(apiId=api_id)["apiKey"]["id"]
    appsync.create_data_source(apiId=api_id, name="DS", type=ds_type, **ds_kwargs)
    return api_id, key


def _js_resolver(appsync, api_id, field, code, kind="UNIT", **kw):
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName=field, dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"}, code=code,
        kind=kind, **kw)


def test_appsync_js_unit_resolver_runs_its_code(appsync):
    """A resolver's request() decides what the data source is asked for, and its
    response() shapes the answer. Neither ran before: the executor dispatched on
    data source type and inferred an operation from the field arguments, so the
    resolver's own logic never affected the result."""
    api_id, key = _js_api(appsync, "qa-js-unit")
    _js_resolver(appsync, api_id, "ping", """
        export function request(ctx) {
          return { payload: { pong: true, echoed: ctx.args.name } }
        }
        export function response(ctx) {
          return { ...ctx.result, seen: true }
        }
    """)
    out = _graphql(api_id, key, '{ ping(name: "hi") }')
    assert out["data"]["ping"] == {"pong": True, "echoed": "hi", "seen": True}


def test_appsync_js_resolver_error_becomes_a_graphql_error(appsync):
    """util.error must surface as a GraphQL error with its errorType, not a
    500 and not a silent null."""
    api_id, key = _js_api(appsync, "qa-js-error")
    _js_resolver(appsync, api_id, "ping", """
        import { util } from '@aws-appsync/utils'
        export function request(ctx) { util.error('not allowed', 'Forbidden') }
    """)
    out = _graphql(api_id, key, '{ ping }')
    assert out["data"]["ping"] is None
    assert out["errors"][0]["message"] == "not allowed"
    assert out["errors"][0].get("errorType") == "Forbidden"


def test_appsync_js_early_return_skips_the_data_source(appsync):
    """runtime.earlyReturn ends the resolver before the data source is touched."""
    api_id, key = _js_api(appsync, "qa-js-early")
    _js_resolver(appsync, api_id, "ping", """
        import { runtime } from '@aws-appsync/utils'
        export function request(ctx) { runtime.earlyReturn({ short: true }) }
        export function response(ctx) { return { neverReached: true } }
    """)
    assert _graphql(api_id, key, '{ ping }')["data"]["ping"] == {"short": True}


def test_appsync_js_http_data_source_is_called(appsync, httpserver_url):
    """HTTP is one of the four data source types the control plane accepts and
    the executor ignored — a resolver over one answered the mock."""
    api_id, key = _js_api(appsync, "qa-js-http", ds_type="HTTP",
                          httpConfig={"endpoint": httpserver_url})
    _js_resolver(appsync, api_id, "ping", """
        export function request(ctx) {
          return { method: 'POST', resourcePath: '/echo',
                   params: { headers: { 'content-type': 'application/json' },
                             body: JSON.stringify({ sent: ctx.args.name }) } }
        }
        export function response(ctx) {
          return JSON.parse(ctx.result.body)
        }
    """)
    out = _graphql(api_id, key, '{ ping(name: "over-http") }')
    assert out["data"]["ping"]["sent"] == "over-http"


def test_appsync_js_pipeline_threads_stash_and_prev(appsync):
    """A pipeline runs its functions in order, with ctx.stash carried across and
    ctx.prev.result holding the previous function's value."""
    api_id, key = _js_api(appsync, "qa-js-pipeline")
    fn1 = appsync.create_function(
        apiId=api_id, name="one", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) { ctx.stash.step = 1; return { payload: { a: 1 } } }
            export function response(ctx) { return ctx.result }
        """)["functionConfiguration"]["functionId"]
    fn2 = appsync.create_function(
        apiId=api_id, name="two", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) {
              return { payload: { a: ctx.prev.result.a + 1, step: ctx.stash.step } }
            }
            export function response(ctx) { return ctx.result }
        """)["functionConfiguration"]["functionId"]

    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="chain", kind="PIPELINE",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        pipelineConfig={"functions": [fn1, fn2]},
        code="""
            export function request(ctx) { return {} }
            export function response(ctx) { return ctx.prev.result }
        """)
    out = _graphql(api_id, key, '{ chain }')
    assert out["data"]["chain"] == {"a": 2, "step": 1}


def test_appsync_non_js_resolver_keeps_its_existing_behaviour(appsync, ddb):
    """A resolver with no APPSYNC_JS code still resolves the way it did before,
    so adding execution does not change any API that worked already."""
    api_id, key, table = _cache_ds_api(appsync, ddb, "qa-js-legacy")
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="getThing", dataSourceName="Things")
    ddb.put_item(TableName=table, Item={"id": {"S": "t1"}, "v": {"S": "legacy"}})
    out = _graphql(api_id, key, '{ getThing(id: "t1") { id v } }')
    assert out["data"]["getThing"]["v"] == "legacy"


_NESTED_SDL = """
type Thing { id: ID! label: String owner: String }
type Query { getThing(id: ID!): Thing }
schema { query: Query }
"""


def test_appsync_js_nested_type_resolvers_run(appsync):
    """A resolver on a nested type must run, with ctx.source set to the parent.

    Only top-level Query/Mutation fields resolved before, so a field like
    Thing.label was simply absent from the response. Most of a real API's
    resolvers live on nested types — ctx.source is read 371 times across this
    API's resolvers — so without this the graph barely executes.
    """
    api_id, key = _js_api(appsync, "qa-js-nested")
    appsync.start_schema_creation(apiId=api_id, definition=_NESTED_SDL.encode())
    _js_resolver(appsync, api_id, "getThing", """
        export function request(ctx) { return { payload: { id: ctx.args.id } } }
        export function response(ctx) { return ctx.result }
    """)
    appsync.create_resolver(
        apiId=api_id, typeName="Thing", fieldName="label", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) {
              return { payload: 'label-for-' + ctx.source.id }
            }
            export function response(ctx) { return ctx.result }
        """)

    out = _graphql(api_id, key, '{ getThing(id: "t1") { id label } }')
    assert out["data"]["getThing"]["id"] == "t1"
    assert out["data"]["getThing"]["label"] == "label-for-t1", \
        "the nested resolver must run with ctx.source from its parent"


def test_appsync_js_identity_comes_from_a_cognito_token(appsync, cognito_idp):
    """ctx.identity must be populated from a verified Cognito token.

    identity is read 617 times across this API's resolvers and identity.sub 309
    times; it was only ever populated by a Lambda authorizer, so a Cognito API
    saw null and every permission check failed.
    """
    pool = cognito_idp.create_user_pool(PoolName="qa-js-identity-pool")["UserPool"]
    client = cognito_idp.create_user_pool_client(
        UserPoolId=pool["Id"], ClientName="app", ExplicitAuthFlows=["ADMIN_NO_SRP_AUTH"],
    )["UserPoolClient"]
    username = "identity-probe@example.com"
    cognito_idp.admin_create_user(UserPoolId=pool["Id"], Username=username,
                                  MessageAction="SUPPRESS")
    cognito_idp.admin_set_user_password(UserPoolId=pool["Id"], Username=username,
                                        Password="TestPass123!", Permanent=True)
    token = cognito_idp.admin_initiate_auth(
        UserPoolId=pool["Id"], ClientId=client["ClientId"],
        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": username, "PASSWORD": "TestPass123!"},
    )["AuthenticationResult"]["IdToken"]

    api = appsync.create_graphql_api(
        name="qa-js-identity", authenticationType="AMAZON_COGNITO_USER_POOLS",
        userPoolConfig={"userPoolId": pool["Id"], "awsRegion": "us-east-1",
                        "defaultAction": "ALLOW"},
    )["graphqlApi"]
    api_id = api["apiId"]
    appsync.create_data_source(apiId=api_id, name="DS", type="NONE")
    _js_resolver(appsync, api_id, "whoami", """
        export function request(ctx) { return { payload: { sub: ctx.identity.sub } } }
        export function response(ctx) { return ctx.result }
    """)

    import requests as _rq
    out = _rq.post(f"{_ENDPOINT}/v1/apis/{api_id}/graphql",
                   headers={"Authorization": f"Bearer {token}",
                            "content-type": "application/json"},
                   json={"query": "{ whoami }"}, timeout=15).json()
    assert out["data"]["whoami"]["sub"], "ctx.identity.sub must be populated"


def test_appsync_js_ctx_error_is_visible_to_response(appsync):
    """response() sees a data source failure in ctx.error rather than being
    skipped, which is how a resolver turns an upstream error into its own."""
    api_id, key = _js_api(appsync, "qa-js-ctxerror", ds_type="HTTP",
                          httpConfig={"endpoint": "http://127.0.0.1:1"})
    _js_resolver(appsync, api_id, "ping", """
        export function request(ctx) { return { method: 'GET', resourcePath: '/' } }
        export function response(ctx) {
          if (ctx.error) { return { failed: true, type: ctx.error.type } }
          return ctx.result
        }
    """)
    out = _graphql(api_id, key, '{ ping }')
    assert out["data"]["ping"]["failed"] is True


def test_appsync_js_ctx_env_exposes_api_environment_variables(appsync):
    """ctx.env carries the API's environment variables, which is how a resolver
    reads a secret it must not hold in code."""
    api_id, key = _js_api(appsync, "qa-js-env")
    appsync.put_graphql_api_environment_variables(
        apiId=api_id, environmentVariables={"SOME_SECRET": "s3cr3t"})
    _js_resolver(appsync, api_id, "ping", """
        export function request(ctx) { return { payload: { seen: ctx.env.SOME_SECRET } } }
        export function response(ctx) { return ctx.result }
    """)
    assert _graphql(api_id, key, '{ ping }')["data"]["ping"]["seen"] == "s3cr3t"


def test_appsync_js_dynamodb_helper_module_is_available(appsync, ddb):
    """`@aws-appsync/utils/dynamodb` must resolve, not vanish with the import.

    The worker strips imports because AppSync injects util and runtime natively
    and the npm package is types-only — but that sub-module is not types-only:
    ddb.get/put/update/remove build real request objects. Stripping its import
    left the binding undefined and the resolver died with "ddb is not defined".
    """
    api_id, key, table = _cache_ds_api(appsync, ddb, "qa-js-ddbutils")
    ddb.put_item(TableName=table, Item={"id": {"S": "d1"}, "v": {"S": "found"}})
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="getThing", dataSourceName="Things",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            import * as ddb from '@aws-appsync/utils/dynamodb'
            export function request(ctx) { return ddb.get({ key: { id: ctx.args.id } }) }
            export function response(ctx) { return ctx.result }
        """)
    out = _graphql(api_id, key, '{ getThing(id: "d1") { id v } }')
    assert out["data"]["getThing"]["v"] == "found"


def test_appsync_js_named_dynamodb_helper_import_also_works(appsync, ddb):
    """The named form — import { get } from '...' — must bind too."""
    api_id, key, table = _cache_ds_api(appsync, ddb, "qa-js-ddbnamed")
    ddb.put_item(TableName=table, Item={"id": {"S": "d2"}, "v": {"S": "named"}})
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="getThing", dataSourceName="Things",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            import { get } from '@aws-appsync/utils/dynamodb'
            export function request(ctx) { return get({ key: { id: ctx.args.id } }) }
            export function response(ctx) { return ctx.result }
        """)
    assert _graphql(api_id, key, '{ getThing(id: "d2") { id v } }')["data"]["getThing"]["v"] == "named"


def test_appsync_evaluate_code_runs_a_handler(appsync):
    """EvaluateCode is how AWS lets you test a resolver without deploying it.

    ministack has an evaluator now, so it can answer the same operation — which
    is what makes a resolver testable locally the way it is against AWS.
    """
    out = appsync.evaluate_code(
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) { return { payload: { got: ctx.arguments.n } } }
            export function response(ctx) { return ctx.result }
        """,
        context=json.dumps({"arguments": {"n": 7}}),
        function="request",
    )
    assert json.loads(out["evaluationResult"]) == {"payload": {"got": 7}}


def test_appsync_evaluate_code_reports_a_resolver_error(appsync):
    """A resolver that raises must come back as an error, not a crash."""
    out = appsync.evaluate_code(
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            import { util } from '@aws-appsync/utils'
            export function request(ctx) { util.error('nope', 'Forbidden') }
        """,
        context=json.dumps({"arguments": {}}),
        function="request",
    )
    assert out["error"]["message"] == "nope"


def test_appsync_js_util_surface_matches_what_appsync_provides(appsync):
    """The util namespaces a resolver can reach.

    ministack provided 8 of the 21 members AWS declares, and three of those were
    wrong rather than missing: autoUlid returned a UUID, util.dynamodb had only
    a pass-through toDynamoDB, and util.transform was an empty object, so a call
    failed as "not a function" well away from the cause.
    """
    api_id, key = _js_api(appsync, "qa-js-utils")
    _js_resolver(appsync, api_id, "ping", """
        import { util } from '@aws-appsync/utils'
        export function request(ctx) {
          return { payload: {
            b64: util.base64Decode(util.base64Encode('hello')),
            url: util.urlDecode(util.urlEncode('a b&c')),
            matches: util.matches('^a.*z$', 'abcz'),
            ulid: util.autoUlid().length,
            ksuid: util.autoKsuid().length,
            round: util.math.roundNum(2.6),
            ddb: util.dynamodb.toDynamoDB({ a: 1, b: true }),
            iso: util.time.epochMilliSecondsToISO8601(0),
          } }
        }
        export function response(ctx) { return ctx.result }
    """)
    got = _graphql(api_id, key, '{ ping }')["data"]["ping"]
    assert got["b64"] == "hello"
    assert got["url"] == "a b&c"
    assert got["matches"] is True
    assert got["ulid"] == 26, "a ULID is 26 characters, not a UUID"
    assert got["ksuid"] == 27
    assert got["round"] == 3
    assert got["ddb"] == {"M": {"a": {"N": "1"}, "b": {"BOOL": True}}}
    assert got["iso"] == "1970-01-01T00:00:00.000Z"


def test_appsync_js_unimplemented_util_refuses_by_name(appsync):
    """What is not implemented says so, rather than returning undefined and
    failing somewhere later with no trace of the cause."""
    api_id, key = _js_api(appsync, "qa-js-utils-gap")
    _js_resolver(appsync, api_id, "ping", """
        import { util } from '@aws-appsync/utils'
        export function request(ctx) { return util.xml.toMap('<a/>') }
    """)
    out = _graphql(api_id, key, '{ ping }')
    assert "xml.toMap" in out["errors"][0]["message"]


def test_appsync_js_transform_builds_a_dynamodb_filter_expression(appsync):
    """util.transform.toDynamoDBFilterExpression builds a real expression.

    DynamoDB is the data source most AppSync APIs use, and this is how a
    resolver turns a GraphQL filter argument into one — so it is the transform
    function that actually gets called. It returned nothing before, because
    util.transform was an empty object.
    """
    api_id, key = _js_api(appsync, "qa-js-transform")
    _js_resolver(appsync, api_id, "ping", """
        import { util } from '@aws-appsync/utils'
        export function request(ctx) {
          return { payload: util.transform.toDynamoDBFilterExpression({
            title: { beginsWith: 'foo' },
            views: { gt: 10 },
          }) }
        }
        export function response(ctx) { return ctx.result }
    """)
    got = _graphql(api_id, key, '{ ping }')["data"]["ping"]
    assert "begins_with" in got["expression"]
    assert got["expressionNames"]["#title"] == "title"
    assert got["expressionValues"][":title_beginsWith"] == {"S": "foo"}
    assert got["expressionValues"][":views_gt"] == {"N": "10"}
    # Two conditions on one filter are ANDed, as AppSync does.
    assert " AND " in got["expression"]


def test_appsync_js_transform_handles_or_not_and_the_operator_set(appsync):
    """The operators AppSync documents, and the and/or/not nesting."""
    api_id, key = _js_api(appsync, "qa-js-transform-ops")
    _js_resolver(appsync, api_id, "ping", """
        import { util } from '@aws-appsync/utils'
        export function request(ctx) {
          return { payload: {
            ops: util.transform.toDynamoDBFilterExpression({
              a: { eq: 1 }, b: { ne: 2 }, c: { between: [1, 5] },
              d: { in: ['x', 'y'] }, e: { contains: 'z' },
              f: { attributeExists: true },
            }),
            nested: util.transform.toDynamoDBFilterExpression({
              or: [{ p: { eq: 1 } }, { not: { q: { eq: 2 } } }],
            }),
          } }
        }
        export function response(ctx) { return ctx.result }
    """)
    got = _graphql(api_id, key, '{ ping }')["data"]["ping"]
    e = got["ops"]["expression"]
    assert "#a = :a_eq" in e and "#b <> :b_ne" in e
    assert "#c BETWEEN :c_between_0 AND :c_between_1" in e
    assert "#d IN (:d_in_0,:d_in_1)" in e
    assert "contains(#e, :e_contains)" in e
    assert "attribute_exists(#f)" in e
    n = got["nested"]["expression"]
    assert " OR " in n and "NOT" in n


def test_appsync_anonymous_operation_with_variables_is_parsed(appsync):
    """`mutation($x: T!) { ... }` — no space before the parenthesis.

    This is the form every SDK and generated client sends. The operation regex
    required whitespace after the keyword, so it did not match, and the fallback
    parsed the whole document as a single field literally named "mutation" —
    answering {"data": {"mutation": null}} with no error to say why.
    """
    api_id, key = _js_api(appsync, "qa-anon-op")
    _js_resolver(appsync, api_id, "echo", """
        export function request(ctx) { return { payload: { got: ctx.args.n } } }
        export function response(ctx) { return ctx.result }
    """)

    import requests as _rq
    for query in (
        'query($n: Int) { echo(n: $n) }',        # anonymous, variables, no space
        'query ($n: Int) { echo(n: $n) }',       # anonymous, with a space
        'query Named($n: Int) { echo(n: $n) }',  # named
        # The shape that actually failed: a non-null variable type, which is
        # what an input object argument always is.
        'query($n: Int!) { echo(n: $n) }',
    ):
        r = _rq.post(f"{_ENDPOINT}/v1/apis/{api_id}/graphql",
                     headers={"x-api-key": key, "content-type": "application/json"},
                     json={"query": query, "variables": {"n": 7}}, timeout=15).json()
        assert "echo" in (r.get("data") or {}), f"{query!r} parsed as {r}"
        assert r["data"]["echo"] == {"got": 7}, f"{query!r} gave {r}"


def test_appsync_js_aliased_utils_import_binds(appsync):
    """`import { util as util2 }` — what a bundler emits on a name collision.

    esbuild renames when several bundled modules import util, so 93 of this
    API's 191 resolvers arrive as util2 or util3. Stripping the import left the
    alias undefined and the resolver died with "util2 is not defined".
    """
    api_id, key = _js_api(appsync, "qa-js-alias")
    _js_resolver(appsync, api_id, "ping", """
        import { util as util2 } from '@aws-appsync/utils'
        import { runtime as runtime2 } from '@aws-appsync/utils'
        export function request(ctx) {
          return { payload: { id: util2.autoId().length, hasRuntime: typeof runtime2.earlyReturn } }
        }
        export function response(ctx) { return ctx.result }
    """)
    got = _graphql(api_id, key, '{ ping }')["data"]["ping"]
    assert got["id"] == 36
    assert got["hasRuntime"] == "function"


_GC_SDL = """
type Thing { id: ID! label: String owner: String }
type Query { getThing(id: ID!): Thing echo(n: Int): Int }
type Mutation { makeThing(input: MakeInput!): Thing }
input MakeInput { label: String! }
schema { query: Query mutation: Mutation }
"""


def _gc_api(appsync, name):
    api_id, key = _js_api(appsync, name)
    appsync.start_schema_creation(apiId=api_id, definition=_GC_SDL.encode())
    _js_resolver(appsync, api_id, "getThing", """
        export function request(ctx) { return { payload: { id: ctx.args.id } } }
        export function response(ctx) { return ctx.result }
    """)
    appsync.create_resolver(
        apiId=api_id, typeName="Thing", fieldName="label", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) { return { payload: 'L-' + ctx.source.id } }
            export function response(ctx) { return ctx.result }
        """)
    return api_id, key


def test_appsync_graphql_field_alias(appsync):
    """`a: getThing(...)` — the response is keyed by the alias, not the field."""
    api_id, key = _gc_api(appsync, "qa-gc-alias")
    out = _graphql(api_id, key, '{ a: getThing(id: "t1") { id } }')
    assert out["data"]["a"]["id"] == "t1", out


def test_appsync_graphql_fragments(appsync):
    """A named fragment and an inline fragment both contribute their fields."""
    api_id, key = _gc_api(appsync, "qa-gc-frag")
    out = _graphql(api_id, key, """
        { getThing(id: "t1") { ...F ... on Thing { label } } }
        fragment F on Thing { id }
    """)
    assert out["data"]["getThing"]["id"] == "t1", out
    assert out["data"]["getThing"]["label"] == "L-t1", out


def test_appsync_graphql_skip_and_include_directives(appsync):
    """@skip and @include decide whether a field is in the response at all."""
    api_id, key = _gc_api(appsync, "qa-gc-directives")
    out = _graphql(api_id, key,
                   'query($yes: Boolean!, $no: Boolean!) '
                   '{ getThing(id: "t1") { id label @include(if: $no) owner @skip(if: $yes) } }',
                   {"yes": True, "no": False})
    thing = out["data"]["getThing"]
    assert "label" not in thing, f"@include(if: false) must omit the field: {thing}"
    assert "owner" not in thing, f"@skip(if: true) must omit the field: {thing}"


def test_appsync_graphql_rejects_an_invalid_query(appsync):
    """A query naming a field the schema does not have is refused, not executed."""
    api_id, key = _gc_api(appsync, "qa-gc-validate")
    out = _graphql(api_id, key, '{ getThing(id: "t1") { nope } }')
    assert out.get("errors"), "an invalid selection must be an error"
    assert "nope" in out["errors"][0]["message"]
    assert not (out.get("data") or {}).get("getThing")


def test_appsync_graphql_introspection(appsync):
    """__schema is how every client and codegen tool starts."""
    api_id, key = _gc_api(appsync, "qa-gc-introspect")
    out = _graphql(api_id, key, '{ __schema { queryType { name } } }')
    assert out["data"]["__schema"]["queryType"]["name"] == "Query", out


def test_appsync_graphql_variable_defaults_and_coercion(appsync):
    """A variable default applies, and values coerce to the declared type."""
    api_id, key = _gc_api(appsync, "qa-gc-vars")
    _js_resolver(appsync, api_id, "echo", """
        export function request(ctx) { return { payload: ctx.args.n } }
        export function response(ctx) { return ctx.result }
    """)
    out = _graphql(api_id, key, 'query($n: Int = 42) { echo(n: $n) }')
    assert out["data"]["echo"] == 42, out


def test_appsync_js_function_early_return_continues_the_pipeline(appsync):
    """earlyReturn in a FUNCTION skips its data source and response — and the
    pipeline continues to the next function.

    AWS: "the data source and response handler are skipped, and the next
    function request handler (or the pipeline resolver response handler if this
    was the last AWS AppSync function) is called."

    Ending the whole pipeline instead means every later function is silently
    skipped. A real resolver that guards its first function this way then
    returns null with no error, because the function that would have set the
    result never ran.
    """
    api_id, key = _js_api(appsync, "qa-early-fn")
    first = appsync.create_function(
        apiId=api_id, name="skipper", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            import { runtime } from '@aws-appsync/utils'
            export function request(ctx) { runtime.earlyReturn() }
            export function response(ctx) { return 'RESPONSE_SHOULD_BE_SKIPPED' }
        """)["functionConfiguration"]["functionId"]
    second = appsync.create_function(
        apiId=api_id, name="setter", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) { ctx.stash.result = 'SECOND_RAN'; return { payload: 1 } }
            export function response(ctx) { return ctx.result }
        """)["functionConfiguration"]["functionId"]

    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="chain", kind="PIPELINE",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        pipelineConfig={"functions": [first, second]},
        code="""
            export function request(ctx) { return {} }
            export function response(ctx) { return ctx.stash.result }
        """)
    out = _graphql(api_id, key, '{ chain }')
    assert out["data"]["chain"] == "SECOND_RAN", \
        f"a function's earlyReturn must not end the pipeline: {out}"


def test_appsync_js_resolver_early_return_still_runs_the_response(appsync):
    """earlyReturn in the pipeline RESOLVER's request skips the functions, and
    the resolver's own response handler still runs.

    AWS: "the pipeline execution is skipped, and the pipeline resolver response
    handler is called immediately."
    """
    api_id, key = _js_api(appsync, "qa-early-resolver")
    fn = appsync.create_function(
        apiId=api_id, name="never", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) { ctx.stash.ran = true; return { payload: 1 } }
            export function response(ctx) { return ctx.result }
        """)["functionConfiguration"]["functionId"]
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="chain", kind="PIPELINE",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        pipelineConfig={"functions": [fn]},
        code="""
            import { runtime } from '@aws-appsync/utils'
            export function request(ctx) { runtime.earlyReturn({ short: true }) }
            export function response(ctx) {
              return ctx.stash.ran ? 'FUNCTIONS_RAN' : 'RESPONSE_RAN'
            }
        """)
    out = _graphql(api_id, key, '{ chain }')
    assert out["data"]["chain"] == "RESPONSE_RAN", \
        f"the resolver's response must still run, and the functions must not: {out}"


def test_appsync_js_early_return_skip_to_end(appsync):
    """skipTo: 'END' from a function skips the rest of the pipeline and goes
    straight to the resolver's response handler."""
    api_id, key = _js_api(appsync, "qa-early-skipend")
    first = appsync.create_function(
        apiId=api_id, name="ender", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            import { runtime } from '@aws-appsync/utils'
            export function request(ctx) { runtime.earlyReturn('ENDED', { skipTo: 'END' }) }
            export function response(ctx) { return ctx.result }
        """)["functionConfiguration"]["functionId"]
    second = appsync.create_function(
        apiId=api_id, name="shouldNotRun", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) { ctx.stash.ran = true; return { payload: 1 } }
            export function response(ctx) { return ctx.result }
        """)["functionConfiguration"]["functionId"]
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="chain", kind="PIPELINE",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        pipelineConfig={"functions": [first, second]},
        code="""
            export function request(ctx) { return {} }
            export function response(ctx) {
              return ctx.stash.ran ? 'SECOND_RAN' : String(ctx.prev.result)
            }
        """)
    out = _graphql(api_id, key, '{ chain }')
    assert out["data"]["chain"] == "ENDED", f"skipTo END must stop the pipeline: {out}"


def test_appsync_js_pipeline_response_sees_ctx_result(appsync):
    """A pipeline resolver's response handler gets ctx.result as well as
    ctx.prev.result — AWS documents result as "available only to response
    handlers", and real resolvers return ctx.result from the after step."""
    api_id, key = _js_api(appsync, "qa-pipe-ctxresult")
    fn = appsync.create_function(
        apiId=api_id, name="produce", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) { return { payload: { v: 'FROM_FUNCTION' } } }
            export function response(ctx) { return ctx.result }
        """)["functionConfiguration"]["functionId"]
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="chain", kind="PIPELINE",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        pipelineConfig={"functions": [fn]},
        code="""
            export function request(ctx) { return {} }
            export function response(ctx) { return ctx.result.v }
        """)
    assert _graphql(api_id, key, '{ chain }')["data"]["chain"] == "FROM_FUNCTION"


def test_appsync_js_lambda_datasource_receives_the_payload_verbatim(appsync, lam):
    """A JS resolver returning {operation: 'Invoke', payload} sends exactly that
    payload as the Lambda event.

    It was being wrapped in the standard resolver event instead, so a function
    expecting its own shape — say {text: [...]} — received
    {arguments: {text: [...]}, info: {...}} and returned something the resolver
    could not use. The failure surfaces inside the resolver, far from the cause.
    """
    import io
    import zipfile

    def _zip(src):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("index.py", src)
        return buf.getvalue()

    code = _zip(
        "def handler(e, c):\n"
        "    # Echo the event back so the test can see exactly what arrived.\n"
        "    return e\n"
    )
    fn = "qa-appsync-payload-fn"
    lam.create_function(
        FunctionName=fn, Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/r", Handler="index.handler",
        Code={"ZipFile": code},
    )
    api = appsync.create_graphql_api(name="qa-lambda-payload",
                                     authenticationType="API_KEY")["graphqlApi"]
    api_id = api["apiId"]
    key = appsync.create_api_key(apiId=api_id)["apiKey"]["id"]
    appsync.create_data_source(
        apiId=api_id, name="DS", type="AWS_LAMBDA",
        lambdaConfig={"lambdaFunctionArn":
                      f"arn:aws:lambda:us-east-1:000000000000:function:{fn}"},
    )
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="echo", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) {
              return { operation: 'Invoke', payload: { text: ['a', 'b'] } }
            }
            export function response(ctx) { return ctx.result }
        """,
    )
    got = _graphql(api_id, key, '{ echo }')["data"]["echo"]
    assert got == {"text": ["a", "b"]}, \
        f"the payload must arrive verbatim, not wrapped in a resolver event: {got}"


def test_appsync_cognito_api_refuses_an_unauthenticated_request(appsync, cognito_idp):
    """An API whose auth mode is AMAZON_COGNITO_USER_POOLS refuses a caller with
    no credentials, as AWS does — unconditionally, not behind a switch.

    Credentials are still not verified (ministack issued them itself); what is
    refused is a request satisfying none of the API's configured providers,
    which is the case that made every authorization test pass regardless of
    what the resolvers did.
    """
    pool = cognito_idp.create_user_pool(PoolName="qa-authmode-pool")["UserPool"]
    api = appsync.create_graphql_api(
        name="qa-authmode", authenticationType="AMAZON_COGNITO_USER_POOLS",
        userPoolConfig={"userPoolId": pool["Id"], "awsRegion": "us-east-1",
                        "defaultAction": "ALLOW"},
    )["graphqlApi"]
    api_id = api["apiId"]
    appsync.create_data_source(apiId=api_id, name="DS", type="NONE")
    appsync.create_resolver(
        apiId=api_id, typeName="Query", fieldName="secret", dataSourceName="DS",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) { return { payload: 'LEAKED' } }
            export function response(ctx) { return ctx.result }
        """)

    import requests as _rq
    r = _rq.post(f"{_ENDPOINT}/v1/apis/{api_id}/graphql",
                 headers={"content-type": "application/json"},
                 json={"query": "{ secret }"}, timeout=15)
    assert r.status_code == 401, f"expected 401, got {r.status_code} {r.text[:200]}"
    assert "LEAKED" not in r.text


def test_appsync_schemaless_api_keeps_lenient_execution(ddb):
    """An API with no schema is executed leniently, exactly as before.

    Validation is a per-API property that appears when a schema does: parsing
    always happens, but a query is only checked against a schema once one has
    been uploaded. Every API created before StartSchemaCreation existed is
    schemaless, so validating those would start rejecting queries that work
    today — this pins that it does not.
    """
    import json
    import urllib.request

    from conftest import make_client

    appsync = make_client("appsync")

    ddb.create_table(
        TableName="schemaless-users",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    api = appsync.create_graphql_api(
        name="schemaless-api", authenticationType="API_KEY")["graphqlApi"]
    api_id = api["apiId"]
    key = appsync.create_api_key(apiId=api_id)["apiKey"]

    appsync.create_data_source(
        apiId=api_id, name="usersDS", type="AMAZON_DYNAMODB",
        dynamodbConfig={"tableName": "schemaless-users", "awsRegion": "us-east-1"},
    )
    appsync.create_resolver(
        apiId=api_id, typeName="Mutation", fieldName="createUser",
        dataSourceName="usersDS",
    )

    # No StartSchemaCreation call — this API has no schema at all.
    assert not appsync.get_graphql_api(apiId=api_id)["graphqlApi"].get("_schema")

    def _post(query):
        req = urllib.request.Request(
            f"{_ENDPOINT}/v1/apis/{api_id}/graphql",
            data=json.dumps({"query": query}).encode(),
            headers={"Content-Type": "application/json", "x-api-key": key["id"]},
        )
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    body = _post('mutation { createUser(input: {id: "u1", name: "Ada"}) { id name } }')
    assert "errors" not in body, body
    assert body["data"]["createUser"]["id"] == "u1", body

    # The decisive half: with a schema this field would be a validation error
    # before any resolver ran. Schemaless, it must still reach the resolver.
    body = _post('mutation { createUser(input: {id: "u2", nope: 1}) { id } }')
    assert body.get("data", {}).get("createUser", {}).get("id") == "u2", body


# ---------------------------------------------------------------------------
# Fixes on top of the AppSync execution merge: required members validated,
# ApiCache carries healthMetricsConfig, introspection serves JSON, the JS
# worker is bounded, DynamoDB requests decode and scope correctly, and CFN
# releases every child store.
# ---------------------------------------------------------------------------


def test_appsync_ddb_plain_decodes_numbers_and_sets():
    """AttributeValue N is a string that may be a float or negative, and the
    set types are sets — none of them survive a digits-only int() decode."""
    from ministack.services.appsync import _ddb_plain

    assert _ddb_plain({"N": "42"}) == 42
    assert _ddb_plain({"N": "-2"}) == -2
    assert _ddb_plain({"N": "3.5"}) == 3.5
    assert _ddb_plain({"N": "-0.25"}) == -0.25
    assert _ddb_plain({"SS": ["a", "b"]}) == ["a", "b"]
    assert _ddb_plain({"NS": ["1", "-2", "3.5"]}) == [1, -2, 3.5]
    assert _ddb_plain({"BS": ["AQ==", "Ag=="]}) == ["AQ==", "Ag=="]
    assert _ddb_plain({"M": {"n": {"N": "-7"}, "s": {"SS": ["x"]}}}) == {"n": -7, "s": ["x"]}
    assert _ddb_plain({"L": [{"N": "1.5"}, {"NULL": True}]}) == [1.5, None]


def test_appsync_api_cache_validates_required_members():
    """CreateApiCache requires ttl (1-3600), apiCachingBehavior and type; AWS
    refuses a missing or out-of-range member rather than defaulting it.

    Driven through the handlers directly: boto3 enforces required members
    client-side, so the wire-level refusal is unreachable through it.
    """
    import json as _json_mod

    from ministack.services import appsync as _appsync

    resp = _appsync._create_graphql_api({"name": "cache-validate", "authenticationType": "API_KEY"})
    api_id = _json_mod.loads(resp[2])["graphqlApi"]["apiId"]

    valid = {"ttl": 60, "apiCachingBehavior": "FULL_REQUEST_CACHING", "type": "SMALL"}
    for member in ("ttl", "apiCachingBehavior", "type"):
        body = {k: v for k, v in valid.items() if k != member}
        status, _hdrs, payload = _appsync._create_api_cache(api_id, body)
        assert status == 400, f"missing {member} must be refused, got {status}"
        assert _json_mod.loads(payload)["__type"].endswith("BadRequestException")

    for bad_ttl in (0, 3601):
        status, _hdrs, payload = _appsync._create_api_cache(api_id, {**valid, "ttl": bad_ttl})
        assert status == 400, f"ttl={bad_ttl} must be refused"

    status, _hdrs, payload = _appsync._create_api_cache(api_id, {**valid, "apiCachingBehavior": "SOMETIMES"})
    assert status == 400

    # A valid create reports healthMetricsConfig, defaulted DISABLED when unset.
    status, _hdrs, payload = _appsync._create_api_cache(api_id, valid)
    assert status == 200
    record = _json_mod.loads(payload)["apiCache"]
    assert record["healthMetricsConfig"] == "DISABLED"

    # An update must re-supply the required members too, and carries
    # healthMetricsConfig forward when not restated.
    status, _hdrs, payload = _appsync._update_api_cache(api_id, {"ttl": 120})
    assert status == 400
    status, _hdrs, payload = _appsync._update_api_cache(
        api_id, {**valid, "ttl": 120, "healthMetricsConfig": "ENABLED"})
    assert _json_mod.loads(payload)["apiCache"]["healthMetricsConfig"] == "ENABLED"
    status, _hdrs, payload = _appsync._update_api_cache(api_id, {**valid, "ttl": 180})
    assert _json_mod.loads(payload)["apiCache"]["healthMetricsConfig"] == "ENABLED"

    _appsync._delete_graphql_api(api_id)


def test_appsync_function_requires_data_source_name():
    """CreateFunction and UpdateFunction refuse a missing dataSourceName, which
    the API declares required, instead of storing an empty string."""
    import json as _json_mod

    from ministack.services import appsync as _appsync

    resp = _appsync._create_graphql_api({"name": "fn-validate", "authenticationType": "API_KEY"})
    api_id = _json_mod.loads(resp[2])["graphqlApi"]["apiId"]

    status, _hdrs, payload = _appsync._create_function(api_id, {"name": "orphan"})
    assert status == 400
    assert _json_mod.loads(payload)["__type"].endswith("BadRequestException")

    _appsync._create_data_source(api_id, {"name": "DS", "type": "NONE"})
    resp = _appsync._create_function(api_id, {"name": "fn", "dataSourceName": "DS"})
    fn_id = _json_mod.loads(resp[2])["functionConfiguration"]["functionId"]

    status, _hdrs, _payload = _appsync._update_function(api_id, fn_id, {"name": "fn"})
    assert status == 400, "UpdateFunction without dataSourceName must be refused"

    _appsync._delete_graphql_api(api_id)


def test_appsync_introspection_schema_json_is_a_real_document(appsync):
    """format=JSON answers the introspection query result, not SDL and not a
    400 — it is what `aws appsync get-introspection-schema --format JSON`
    writes for codegen tooling."""
    api = appsync.create_graphql_api(name="introspect-json", authenticationType="API_KEY")["graphqlApi"]
    try:
        appsync.start_schema_creation(
            apiId=api["apiId"],
            definition=b"type Query { hello: String }\nschema { query: Query }\n")

        blob = appsync.get_introspection_schema(apiId=api["apiId"], format="JSON")["schema"].read()
        document = json.loads(blob)
        assert "__schema" in document
        type_names = {t["name"] for t in document["__schema"]["types"]}
        assert "Query" in type_names

        sdl = appsync.get_introspection_schema(apiId=api["apiId"], format="SDL")["schema"].read()
        assert b"type Query" in sdl
    finally:
        appsync.delete_graphql_api(apiId=api["apiId"])


def test_appsync_js_worker_timeout_kills_and_recovers():
    """An infinite loop in a resolver must not wedge evaluation for the whole
    service: the evaluation is bounded, the stuck process is killed, and the
    next evaluation runs on a fresh worker."""
    from ministack.core import appsync_js

    if not appsync_js.available():
        pytest.skip("node is not available")

    code = "export function request(ctx) { return { payload: ctx.args.x } }"
    try:
        with pytest.raises(appsync_js.AppSyncJsTimeout):
            appsync_js.evaluate(
                "export function request(ctx) { while (true) {} }",
                "request", {"args": {}, "stash": {}}, timeout=2)

        status, value, _appended, _stash, _skip = appsync_js.evaluate(
            code, "request", {"args": {"x": 7}, "stash": {}})
        assert status == "ok" and value == {"payload": 7}
    finally:
        appsync_js.reset()


def test_appsync_js_resolver_honors_dynamodb_data_source_region(appsync, ddb):
    """A JS resolver's DynamoDB data source reads the table in the region the
    data source declares, not the region the request came in on."""
    import uuid as _uuid

    east_ddb = ddb  # conftest ddb client is us-east-1
    west = boto3.client("appsync", endpoint_url=_ENDPOINT, region_name="us-west-2",
                        aws_access_key_id="test", aws_secret_access_key="test")
    table = f"js-cross-region-{_uuid.uuid4().hex[:12]}"
    east_ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    east_ddb.put_item(TableName=table, Item={"id": {"S": "k1"}, "v": {"S": "east"}})

    api = west.create_graphql_api(name="js-cross-region", authenticationType="API_KEY")["graphqlApi"]
    api_id = api["apiId"]
    key = west.create_api_key(apiId=api_id)["apiKey"]["id"]
    west.create_data_source(
        apiId=api_id, name="EastTable", type="AMAZON_DYNAMODB",
        dynamodbConfig={"tableName": table, "awsRegion": "us-east-1"},
    )
    west.create_resolver(
        apiId=api_id, typeName="Query", fieldName="getThing", dataSourceName="EastTable",
        runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
        code="""
            export function request(ctx) {
                return { operation: 'GetItem', key: { id: { S: ctx.args.id } } }
            }
            export function response(ctx) { return ctx.result }
        """,
    )
    try:
        out = _appsync_graphql_post(
            f"{_ENDPOINT}/v1/apis/{api_id}/graphql",
            'query { getThing(id: "k1") { id v } }',
            headers={"x-api-key": key},
        )
        assert out["data"]["getThing"] == {"id": "k1", "v": "east"}, out
    finally:
        west.delete_graphql_api(apiId=api_id)
        east_ddb.delete_table(TableName=table)


def test_appsync_cfn_api_delete_releases_every_child_store():
    """The CloudFormation provisioner's delete must release the same stores the
    service's own DeleteGraphqlApi releases — functions, schema, cache and
    cache entries included, which a hand-kept pop list silently missed."""
    import json as _json_mod

    from ministack.services import appsync as _appsync
    from ministack.services.cloudformation import provisioners as _prov

    resp = _appsync._create_graphql_api({"name": "cfn-cascade", "authenticationType": "API_KEY"})
    api_id = _json_mod.loads(resp[2])["graphqlApi"]["apiId"]
    _appsync._start_schema_creation(api_id, {"definition": "type Query { hi: String }"})
    _appsync._create_data_source(api_id, {"name": "DS", "type": "NONE"})
    _appsync._create_function(api_id, {"name": "fn", "dataSourceName": "DS"})
    _appsync._create_api_cache(
        api_id, {"ttl": 60, "apiCachingBehavior": "FULL_REQUEST_CACHING", "type": "SMALL"})
    _appsync._cache_put(api_id, "k", 60, {"cached": True})

    _prov._appsync_api_delete(api_id, {})

    for store, label in (
        (_appsync._apis, "apis"), (_appsync._functions, "functions"),
        (_appsync._schemas, "schemas"), (_appsync._caches, "caches"),
        (_appsync._cache_entries, "cache entries"), (_appsync._data_sources, "data sources"),
    ):
        assert api_id not in store, f"{label} leaked after the CFN delete"


def test_appsync_cfn_schema_lands_in_the_schema_store():
    """A CFN-provisioned AWS::AppSync::GraphQLSchema must be visible where the
    data plane and GetIntrospectionSchema read schemas from."""
    import json as _json_mod

    from ministack.services import appsync as _appsync
    from ministack.services.cloudformation import provisioners as _prov

    resp = _appsync._create_graphql_api({"name": "cfn-schema", "authenticationType": "API_KEY"})
    api_id = _json_mod.loads(resp[2])["graphqlApi"]["apiId"]

    _prov._appsync_schema_create(
        "Schema", {"ApiId": api_id, "Definition": "type Query { hi: String }"}, "stack")
    assert (_appsync._schemas.get(api_id) or {}).get("definition") == "type Query { hi: String }"

    _prov._appsync_schema_delete(f"{api_id}/schema", {"ApiId": api_id})
    assert _appsync._schemas.get(api_id) is None

    _appsync._delete_graphql_api(api_id)
