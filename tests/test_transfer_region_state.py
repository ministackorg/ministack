import pytest
from botocore.exceptions import ClientError


def test_transfer_servers_and_users_are_region_scoped():
    import os

    import boto3
    from botocore.config import Config

    def _client(region):
        return boto3.client(
            "transfer",
            endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=region,
            config=Config(region_name=region, retries={"mode": "standard"}),
        )

    east = _client("us-east-1")
    west = _client("us-west-2")
    east_server = east.create_server()["ServerId"]
    west_server = west.create_server()["ServerId"]
    user_name = "same-name-regional-user"

    try:
        east.create_user(
            ServerId=east_server,
            UserName=user_name,
            Role="arn:aws:iam::000000000000:role/transfer-role",
        )
        west.create_user(
            ServerId=west_server,
            UserName=user_name,
            Role="arn:aws:iam::000000000000:role/transfer-role",
        )

        east_ids = {server["ServerId"] for server in east.list_servers()["Servers"]}
        west_ids = {server["ServerId"] for server in west.list_servers()["Servers"]}
        assert east_server in east_ids
        assert east_server not in west_ids
        assert west_server in west_ids
        assert west_server not in east_ids
        assert east.list_users(ServerId=east_server)["Users"][0]["UserName"] == user_name
        assert west.list_users(ServerId=west_server)["Users"][0]["UserName"] == user_name
        with pytest.raises(ClientError) as exc:
            west.describe_server(ServerId=east_server)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        east.delete_server(ServerId=east_server)
        west.delete_server(ServerId=west_server)


def test_transfer_legacy_state_restores_resources_to_arn_region():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import transfer as service

    account_id = "111111111111"
    boot_region = "us-east-1"
    resource_region = "us-west-2"
    server_id = "s-legacyregional01"
    user_name = "legacy-user"
    user_key = f"{server_id}/{user_name}"
    original_account = get_account_id()
    original_region = get_region()
    servers = AccountScopedDict()
    users = AccountScopedDict()

    set_request_account_id(account_id)
    set_request_region(boot_region)
    servers[server_id] = {
        "ServerId": server_id,
        "Arn": f"arn:aws:transfer:{resource_region}:{account_id}:server/{server_id}",
        "State": "ONLINE",
    }
    users[user_key] = {
        "ServerId": server_id,
        "UserName": user_name,
        "Arn": (
            f"arn:aws:transfer:{resource_region}:{account_id}:"
            f"user/{server_id}/{user_name}"
        ),
    }

    service.reset()
    try:
        service.restore_state({"servers": servers, "users": users})
        assert service._servers.get_scoped(
            account_id, resource_region, server_id
        )["ServerId"] == server_id
        assert service._users.get_scoped(
            account_id, resource_region, user_key
        )["UserName"] == user_name
        assert service._servers.get_scoped(account_id, boot_region, server_id) is None
        assert service._users.get_scoped(account_id, boot_region, user_key) is None
    finally:
        service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_transfer_reset_clears_state_across_regions():
    from ministack.core.responses import get_region, set_request_region
    from ministack.services import transfer as service

    original_region = get_region()
    service.reset()
    try:
        for region in ("us-east-1", "us-west-2"):
            set_request_region(region)
            service._servers[f"server-{region}"] = {"State": "ONLINE"}
            service._users[f"server-{region}/user"] = {"UserName": "user"}
        service.reset()
        assert not service._servers.has_any()
        assert not service._users.has_any()
    finally:
        service.reset()
        set_request_region(original_region)
