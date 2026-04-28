import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


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
