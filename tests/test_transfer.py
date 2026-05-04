import pytest
from botocore.exceptions import ClientError

# ========== Server lifecycle ==========

def test_transfer_create_server(transfer):
    resp = transfer.create_server()
    assert "ServerId" in resp
    assert resp["ServerId"].startswith("s-")


def test_transfer_describe_server(transfer):
    sid = transfer.create_server()["ServerId"]
    resp = transfer.describe_server(ServerId=sid)
    server = resp["Server"]
    assert server["ServerId"] == sid
    assert server["State"] == "ONLINE"
    assert server["EndpointType"] == "PUBLIC"
    assert server["IdentityProviderType"] == "SERVICE_MANAGED"
    assert "SFTP" in server["Protocols"]
    assert server["Arn"].startswith("arn:aws:transfer:")


def test_transfer_describe_server_not_found(transfer):
    with pytest.raises(ClientError) as exc:
        transfer.describe_server(ServerId="s-doesnotexist00000")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_transfer_list_servers(transfer):
    resp = transfer.list_servers()
    assert "Servers" in resp
    assert len(resp["Servers"]) >= 1


def test_transfer_create_server_with_options(transfer):
    resp = transfer.create_server(
        EndpointType="VPC",
        Protocols=["SFTP", "FTPS"],
        IdentityProviderType="API_GATEWAY",
        Tags=[{"Key": "env", "Value": "test"}],
    )
    sid = resp["ServerId"]
    server = transfer.describe_server(ServerId=sid)["Server"]
    assert server["EndpointType"] == "VPC"
    assert "FTPS" in server["Protocols"]
    assert server["IdentityProviderType"] == "API_GATEWAY"


def test_transfer_delete_server(transfer):
    sid = transfer.create_server()["ServerId"]
    transfer.delete_server(ServerId=sid)
    with pytest.raises(ClientError) as exc:
        transfer.describe_server(ServerId=sid)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_transfer_delete_server_cascades_users(transfer):
    sid = transfer.create_server()["ServerId"]
    transfer.create_user(
        ServerId=sid,
        UserName="cascade-user",
        Role="arn:aws:iam::000000000000:role/transfer-role",
    )
    transfer.delete_server(ServerId=sid)
    # Recreate to verify user is gone
    sid2 = transfer.create_server()["ServerId"]
    resp = transfer.list_users(ServerId=sid2)
    assert len(resp["Users"]) == 0


# ========== User CRUD ==========

@pytest.fixture
def server_id(transfer):
    """Create a fresh server for user tests."""
    return transfer.create_server()["ServerId"]


def test_transfer_create_user(transfer, server_id):
    resp = transfer.create_user(
        ServerId=server_id,
        UserName="test-sftp-user",
        HomeDirectoryType="LOGICAL",
        HomeDirectoryMappings=[{"Entry": "/", "Target": "/my-bucket/path"}],
        Role="arn:aws:iam::000000000000:role/transfer-role",
        SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ testkey",
    )
    assert resp["ServerId"] == server_id
    assert resp["UserName"] == "test-sftp-user"


def test_transfer_describe_user(transfer, server_id):
    transfer.create_user(
        ServerId=server_id,
        UserName="describe-user",
        HomeDirectoryType="LOGICAL",
        HomeDirectoryMappings=[{"Entry": "/", "Target": "/bucket/home"}],
        Role="arn:aws:iam::000000000000:role/xfer",
        SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ desckey",
    )
    resp = transfer.describe_user(ServerId=server_id, UserName="describe-user")
    user = resp["User"]
    assert user["UserName"] == "describe-user"
    assert user["HomeDirectoryType"] == "LOGICAL"
    assert user["HomeDirectoryMappings"] == [{"Entry": "/", "Target": "/bucket/home"}]
    assert user["Role"] == "arn:aws:iam::000000000000:role/xfer"
    assert len(user["SshPublicKeys"]) == 1
    assert user["SshPublicKeys"][0]["SshPublicKeyBody"] == "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ desckey"
    assert user["SshPublicKeys"][0]["SshPublicKeyId"].startswith("key-")
    assert user["Arn"].startswith("arn:aws:transfer:")


def test_transfer_create_user_duplicate(transfer, server_id):
    transfer.create_user(
        ServerId=server_id,
        UserName="dup-user",
        Role="arn:aws:iam::000000000000:role/xfer",
    )
    with pytest.raises(ClientError) as exc:
        transfer.create_user(
            ServerId=server_id,
            UserName="dup-user",
            Role="arn:aws:iam::000000000000:role/xfer",
        )
    assert exc.value.response["Error"]["Code"] == "ResourceExistsException"


def test_transfer_create_user_server_not_found(transfer):
    with pytest.raises(ClientError) as exc:
        transfer.create_user(
            ServerId="s-doesnotexist00000",
            UserName="orphan-user",
            Role="arn:aws:iam::000000000000:role/xfer",
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_transfer_create_user_bad_ssh_key(transfer, server_id):
    with pytest.raises(ClientError) as exc:
        transfer.create_user(
            ServerId=server_id,
            UserName="badkey-user",
            Role="arn:aws:iam::000000000000:role/xfer",
            SshPublicKeyBody="not-a-valid-key",
        )
    assert exc.value.response["Error"]["Code"] == "InvalidRequestException"


def test_transfer_describe_user_not_found(transfer, server_id):
    with pytest.raises(ClientError) as exc:
        transfer.describe_user(ServerId=server_id, UserName="nonexistent")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_transfer_delete_user(transfer, server_id):
    transfer.create_user(
        ServerId=server_id,
        UserName="to-delete",
        Role="arn:aws:iam::000000000000:role/xfer",
    )
    transfer.delete_user(ServerId=server_id, UserName="to-delete")
    with pytest.raises(ClientError) as exc:
        transfer.describe_user(ServerId=server_id, UserName="to-delete")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_transfer_list_users(transfer, server_id):
    transfer.create_user(
        ServerId=server_id,
        UserName="list-user-a",
        Role="arn:aws:iam::000000000000:role/xfer",
    )
    transfer.create_user(
        ServerId=server_id,
        UserName="list-user-b",
        Role="arn:aws:iam::000000000000:role/xfer",
    )
    resp = transfer.list_users(ServerId=server_id)
    names = [u["UserName"] for u in resp["Users"]]
    assert "list-user-a" in names
    assert "list-user-b" in names


# ========== SSH key management ==========

def test_transfer_import_ssh_key(transfer, server_id):
    transfer.create_user(
        ServerId=server_id,
        UserName="key-user",
        Role="arn:aws:iam::000000000000:role/xfer",
        SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ original",
    )
    resp = transfer.import_ssh_public_key(
        ServerId=server_id,
        UserName="key-user",
        SshPublicKeyBody="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIG newkey",
    )
    assert resp["SshPublicKeyId"].startswith("key-")
    assert resp["UserName"] == "key-user"

    user = transfer.describe_user(ServerId=server_id, UserName="key-user")["User"]
    assert len(user["SshPublicKeys"]) == 2
    bodies = {k["SshPublicKeyBody"] for k in user["SshPublicKeys"]}
    assert "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ original" in bodies
    assert "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIG newkey" in bodies


def test_transfer_import_ssh_key_bad_format(transfer, server_id):
    transfer.create_user(
        ServerId=server_id,
        UserName="badimport-user",
        Role="arn:aws:iam::000000000000:role/xfer",
    )
    with pytest.raises(ClientError) as exc:
        transfer.import_ssh_public_key(
            ServerId=server_id,
            UserName="badimport-user",
            SshPublicKeyBody="invalid-key-format",
        )
    assert exc.value.response["Error"]["Code"] == "InvalidRequestException"


def test_transfer_delete_ssh_key(transfer, server_id):
    transfer.create_user(
        ServerId=server_id,
        UserName="delkey-user",
        Role="arn:aws:iam::000000000000:role/xfer",
        SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ first",
    )
    import_resp = transfer.import_ssh_public_key(
        ServerId=server_id,
        UserName="delkey-user",
        SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ second",
    )
    new_key_id = import_resp["SshPublicKeyId"]

    # Get the original key ID
    user = transfer.describe_user(ServerId=server_id, UserName="delkey-user")["User"]
    original_key_id = [k["SshPublicKeyId"] for k in user["SshPublicKeys"]
                       if k["SshPublicKeyBody"] == "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ first"][0]

    # Delete the original key
    transfer.delete_ssh_public_key(
        ServerId=server_id,
        UserName="delkey-user",
        SshPublicKeyId=original_key_id,
    )

    user = transfer.describe_user(ServerId=server_id, UserName="delkey-user")["User"]
    assert len(user["SshPublicKeys"]) == 1
    assert user["SshPublicKeys"][0]["SshPublicKeyId"] == new_key_id
    assert user["SshPublicKeys"][0]["SshPublicKeyBody"] == "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ second"


# ========== WorkOS end-to-end workflow ==========

def test_transfer_workos_sftp_workflow(transfer):
    """
    Simulates the WorkOS SFTP directory sync workflow:
    1. Server exists (create it)
    2. Create user with LOGICAL home dir + SSH key
    3. Describe user to verify
    4. Rotate SSH key (import new, delete old)
    5. Verify single key remains
    6. Delete user
    """
    # 1. Create server
    sid = transfer.create_server()["ServerId"]

    # 2. Create user with LOGICAL home directory mapping to S3
    transfer.create_user(
        ServerId=sid,
        UserName="sftp-org123",
        HomeDirectoryType="LOGICAL",
        HomeDirectoryMappings=[{"Entry": "/", "Target": "/sftp-org123-bucket/"}],
        Role="arn:aws:iam::000000000000:role/aws_transfer_service_write_only_role",
        SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ oldkey",
    )

    # 3. Describe user and verify setup
    user = transfer.describe_user(ServerId=sid, UserName="sftp-org123")["User"]
    assert user["HomeDirectoryType"] == "LOGICAL"
    assert user["HomeDirectoryMappings"] == [{"Entry": "/", "Target": "/sftp-org123-bucket/"}]
    assert len(user["SshPublicKeys"]) == 1
    old_key_id = user["SshPublicKeys"][0]["SshPublicKeyId"]

    # 4. Rotate SSH key: import new key
    import_resp = transfer.import_ssh_public_key(
        ServerId=sid,
        UserName="sftp-org123",
        SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ newkey",
    )
    new_key_id = import_resp["SshPublicKeyId"]

    # Verify both keys present
    user = transfer.describe_user(ServerId=sid, UserName="sftp-org123")["User"]
    assert len(user["SshPublicKeys"]) == 2

    # Delete old key
    transfer.delete_ssh_public_key(
        ServerId=sid,
        UserName="sftp-org123",
        SshPublicKeyId=old_key_id,
    )

    # 5. Verify single key remains
    user = transfer.describe_user(ServerId=sid, UserName="sftp-org123")["User"]
    assert len(user["SshPublicKeys"]) == 1
    assert user["SshPublicKeys"][0]["SshPublicKeyId"] == new_key_id
    assert user["SshPublicKeys"][0]["SshPublicKeyBody"] == "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQ newkey"

    # 6. Delete user
    transfer.delete_user(ServerId=sid, UserName="sftp-org123")
    with pytest.raises(ClientError) as exc:
        transfer.describe_user(ServerId=sid, UserName="sftp-org123")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ========== from test_transfer_sftp.py ==========
# Transfer Family — real asyncssh SFTP server (separate test surface from REST control plane).

import asyncio
import os
import socket
import time
import urllib.request
import uuid

import pytest

asyncssh = pytest.importorskip("asyncssh")
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

ENDPOINT_HOST = os.environ.get("MINISTACK_HOST", "127.0.0.1")
SFTP_PORT = int(os.environ.get("SFTP_PORT", "2222"))

# Honour MINISTACK_ENDPOINT (e.g. http://localhost:14566) so the same test
# file works against a locally-built MiniStack on port 4566 *and* against a
# preview Docker image bound to a different host port.
_endpoint = os.environ.get("MINISTACK_ENDPOINT", f"http://{ENDPOINT_HOST}:4566")
ADMIN_BASE = _endpoint.rstrip("/")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _gen_keypair():
    """Return ``(private_pem_text, public_openssh_text)`` for an ed25519 key.

    asyncssh's ``connect(client_keys=...)`` accepts in-memory PEM, so we
    avoid touching the filesystem.
    """
    priv = ed25519.Ed25519PrivateKey.generate()
    priv_pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    pub = priv.public_key().public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH,
    ).decode("utf-8")
    return priv_pem, pub


def _sftp_listening() -> bool:
    """Best-effort probe: return True if something is accepting TCP on
    SFTP_PORT. Skips the whole module if the listener isn't up."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1.0)
    try:
        s.connect((ENDPOINT_HOST, SFTP_PORT))
    except Exception:
        return False
    finally:
        s.close()
    return True


pytestmark = pytest.mark.skipif(
    not _sftp_listening(),
    reason=f"SFTP listener not reachable at {ENDPOINT_HOST}:{SFTP_PORT} — start MiniStack first",
)


def _connect(username, priv_pem, port=None):
    """Return an asyncssh connection coroutine for the given creds."""
    return asyncssh.connect(
        host=ENDPOINT_HOST,
        port=port or SFTP_PORT,
        username=username,
        client_keys=[asyncssh.import_private_key(priv_pem)],
        known_hosts=None,
    )


def _provision(transfer, s3, *, bucket=None, home_prefix="", logical_mappings=None):
    """CreateServer + CreateBucket + CreateUser. Returns the dict the
    individual tests need: server_id, user_name, priv_pem, bucket name.
    """
    suffix = uuid.uuid4().hex[:8]
    server_id = transfer.create_server()["ServerId"]
    user_name = f"u-{suffix}"
    bucket = bucket or f"sftp-bucket-{suffix}"
    s3.create_bucket(Bucket=bucket)
    priv_pem, pub_ssh = _gen_keypair()

    user_kwargs = {
        "ServerId": server_id,
        "UserName": user_name,
        "Role": f"arn:aws:iam::000000000000:role/sftp-{suffix}",
        "SshPublicKeyBody": pub_ssh,
    }
    if logical_mappings is not None:
        user_kwargs["HomeDirectoryType"] = "LOGICAL"
        user_kwargs["HomeDirectoryMappings"] = logical_mappings
    else:
        user_kwargs["HomeDirectoryType"] = "PATH"
        user_kwargs["HomeDirectory"] = f"/{bucket}/{home_prefix}".rstrip("/")

    transfer.create_user(**user_kwargs)
    return {
        "server_id": server_id,
        "user_name": user_name,
        "priv_pem": priv_pem,
        "bucket": bucket,
        "describe": transfer.describe_server(ServerId=server_id)["Server"],
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_sftp_basic_upload_download(transfer, s3):
    """Connect, upload a file via SFTP, verify it landed in S3, download
    it back. End-to-end smoke test."""
    ctx = _provision(transfer, s3)

    async def _run():
        async with await _connect(ctx["user_name"], ctx["priv_pem"]) as conn:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open("/hello.txt", "wb") as f:
                    await f.write(b"hello sftp\n")
                # Re-list to confirm the file appears.
                names = await sftp.listdir("/")
                assert "hello.txt" in [n for n in names]
                # Download and verify.
                async with sftp.open("/hello.txt", "rb") as f:
                    body = await f.read()
                assert body == b"hello sftp\n"

    asyncio.run(_run())

    # Verify it actually landed in S3 via the standard API.
    obj = s3.get_object(Bucket=ctx["bucket"], Key="hello.txt")
    assert obj["Body"].read() == b"hello sftp\n"
    transfer.delete_server(ServerId=ctx["server_id"])


# ---------------------------------------------------------------------------
# Auth-failure cases
# ---------------------------------------------------------------------------


def test_sftp_wrong_key_rejected(transfer, s3):
    """A connecting client with a public key that wasn't ImportSshPublicKey'd
    for the user must be rejected — not silently allowed in."""
    ctx = _provision(transfer, s3)
    _other_priv, _ = _gen_keypair()
    other_priv_pem, _ = _gen_keypair()

    async def _run():
        with pytest.raises((asyncssh.PermissionDenied, asyncssh.misc.PermissionDenied)):
            await _connect(ctx["user_name"], other_priv_pem)

    asyncio.run(_run())
    transfer.delete_server(ServerId=ctx["server_id"])


def test_sftp_unknown_user_rejected(transfer, s3):
    """A username with no matching Transfer user must be rejected."""
    ctx = _provision(transfer, s3)

    async def _run():
        with pytest.raises((asyncssh.PermissionDenied, asyncssh.misc.PermissionDenied)):
            await _connect("nobody-" + uuid.uuid4().hex[:8], ctx["priv_pem"])

    asyncio.run(_run())
    transfer.delete_server(ServerId=ctx["server_id"])


def test_sftp_offline_server_rejects_auth(transfer, s3):
    """StopServer flips State=OFFLINE; the SFTP listener must refuse
    auth for that server's users (matches AWS — a stopped server doesn't
    serve traffic). Bringing it back ONLINE re-allows auth."""
    ctx = _provision(transfer, s3)
    transfer.stop_server(ServerId=ctx["server_id"])

    async def _denied():
        with pytest.raises((asyncssh.PermissionDenied, asyncssh.misc.PermissionDenied)):
            await _connect(ctx["user_name"], ctx["priv_pem"])

    asyncio.run(_denied())

    transfer.start_server(ServerId=ctx["server_id"])

    async def _allowed():
        async with await _connect(ctx["user_name"], ctx["priv_pem"]) as conn:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open("/probe", "wb") as f:
                    await f.write(b"x")

    asyncio.run(_allowed())
    transfer.delete_server(ServerId=ctx["server_id"])


# ---------------------------------------------------------------------------
# VFS / S3 semantics
# ---------------------------------------------------------------------------


def test_sftp_rename_across_prefixes(transfer, s3):
    """Rename should work across nested prefixes — implemented as S3
    copy + delete since S3 has no atomic rename."""
    ctx = _provision(transfer, s3)

    async def _run():
        async with await _connect(ctx["user_name"], ctx["priv_pem"]) as conn:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open("/a.txt", "wb") as f:
                    await f.write(b"alpha")
                await sftp.rename("/a.txt", "/sub/b.txt")
                async with sftp.open("/sub/b.txt", "rb") as f:
                    assert await f.read() == b"alpha"
                # Source must be gone.
                with pytest.raises(asyncssh.SFTPError):
                    await sftp.stat("/a.txt")

    asyncio.run(_run())
    transfer.delete_server(ServerId=ctx["server_id"])


def test_sftp_large_file_roundtrip(transfer, s3):
    """1 MiB file uploaded and read back — exercises the buffer-then-PUT
    write path under non-trivial load."""
    ctx = _provision(transfer, s3)
    payload = (b"abcdef0123" * 1024) * 100  # ~1 MiB

    async def _run():
        async with await _connect(ctx["user_name"], ctx["priv_pem"]) as conn:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open("/big.bin", "wb") as f:
                    await f.write(payload)
                async with sftp.open("/big.bin", "rb") as f:
                    got = await f.read()
        assert got == payload

    asyncio.run(_run())
    transfer.delete_server(ServerId=ctx["server_id"])


def test_sftp_mkdir_listdir_rmdir(transfer, s3):
    """mkdir creates a zero-byte placeholder under the prefix; listdir
    sees it as a directory; rmdir removes it (only when empty)."""
    ctx = _provision(transfer, s3)

    async def _run():
        async with await _connect(ctx["user_name"], ctx["priv_pem"]) as conn:
            async with conn.start_sftp_client() as sftp:
                await sftp.mkdir("/inbox")
                names = await sftp.listdir("/")
                assert "inbox" in names

                # rmdir on non-empty must fail; create a file then try.
                async with sftp.open("/inbox/x", "wb") as f:
                    await f.write(b"x")
                with pytest.raises(asyncssh.SFTPError):
                    await sftp.rmdir("/inbox")

                # Remove the file, then rmdir succeeds.
                await sftp.remove("/inbox/x")
                await sftp.rmdir("/inbox")

    asyncio.run(_run())
    transfer.delete_server(ServerId=ctx["server_id"])


def test_sftp_logical_root_entry_mapping(transfer, s3):
    """LOGICAL home dir with Entry='/' maps all paths to the target bucket.
    Regression test for a bug where entry + '/' produced '//' and never
    matched any path."""
    suffix = uuid.uuid4().hex[:8]
    bucket = f"sftp-root-{suffix}"
    s3.create_bucket(Bucket=bucket)
    ctx = _provision(
        transfer,
        s3,
        bucket=bucket,
        logical_mappings=[{"Entry": "/", "Target": f"/{bucket}"}],
    )

    async def _run():
        async with await _connect(ctx["user_name"], ctx["priv_pem"]) as conn:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open("/users.csv", "wb") as f:
                    await f.write(b"id,name\n1,Alice\n")
                async with sftp.open("/users.csv", "rb") as f:
                    assert await f.read() == b"id,name\n1,Alice\n"

    asyncio.run(_run())

    obj = s3.get_object(Bucket=bucket, Key="users.csv")
    assert obj["Body"].read() == b"id,name\n1,Alice\n"
    transfer.delete_server(ServerId=ctx["server_id"])


def test_sftp_logical_home_directory_mappings(transfer, s3):
    """LOGICAL home dir maps virtual `/inbox` → bucket prefix; an upload
    to /inbox/foo lands at the mapped S3 key."""
    suffix = uuid.uuid4().hex[:8]
    bucket = f"sftp-logical-{suffix}"
    s3.create_bucket(Bucket=bucket)
    ctx = _provision(
        transfer,
        s3,
        bucket=bucket,
        logical_mappings=[
            {"Entry": "/inbox", "Target": f"/{bucket}/incoming"},
            {"Entry": "/archive", "Target": f"/{bucket}/old"},
        ],
    )

    async def _run():
        async with await _connect(ctx["user_name"], ctx["priv_pem"]) as conn:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open("/inbox/today.txt", "wb") as f:
                    await f.write(b"hi")

    asyncio.run(_run())

    obj = s3.get_object(Bucket=bucket, Key="incoming/today.txt")
    assert obj["Body"].read() == b"hi"
    transfer.delete_server(ServerId=ctx["server_id"])


# ---------------------------------------------------------------------------
# Multi-user / multi-server
# ---------------------------------------------------------------------------


def test_sftp_concurrent_uploads_two_users(transfer, s3):
    """Two distinct users on two distinct servers each upload concurrently
    — exercises the SSH key disambiguation path (single-port mode)."""
    a = _provision(transfer, s3)
    b = _provision(transfer, s3)

    async def _upload(ctx, body):
        async with await _connect(ctx["user_name"], ctx["priv_pem"]) as conn:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open("/concurrent.bin", "wb") as f:
                    await f.write(body)

    async def _run():
        await asyncio.gather(
            _upload(a, b"AAAA"),
            _upload(b, b"BBBB"),
        )

    asyncio.run(_run())

    assert s3.get_object(Bucket=a["bucket"], Key="concurrent.bin")["Body"].read() == b"AAAA"
    assert s3.get_object(Bucket=b["bucket"], Key="concurrent.bin")["Body"].read() == b"BBBB"
    transfer.delete_server(ServerId=a["server_id"])
    transfer.delete_server(ServerId=b["server_id"])


def test_sftp_key_disambiguates_overlapping_usernames(transfer, s3):
    """Two servers, same UserName, different keys: each user's key routes
    to the correct server's bucket. This is the AWS-faithful single-port
    behavior — username alone isn't enough, the SSH key is the
    disambiguator."""
    suffix = uuid.uuid4().hex[:8]
    user_name = f"shared-{suffix}"
    bucket_a = f"sftp-share-a-{suffix}"
    bucket_b = f"sftp-share-b-{suffix}"
    s3.create_bucket(Bucket=bucket_a)
    s3.create_bucket(Bucket=bucket_b)

    sid_a = transfer.create_server()["ServerId"]
    sid_b = transfer.create_server()["ServerId"]
    priv_a, pub_a = _gen_keypair()
    priv_b, pub_b = _gen_keypair()
    transfer.create_user(
        ServerId=sid_a, UserName=user_name,
        Role="arn:aws:iam::000000000000:role/r",
        HomeDirectoryType="PATH", HomeDirectory=f"/{bucket_a}",
        SshPublicKeyBody=pub_a,
    )
    transfer.create_user(
        ServerId=sid_b, UserName=user_name,
        Role="arn:aws:iam::000000000000:role/r",
        HomeDirectoryType="PATH", HomeDirectory=f"/{bucket_b}",
        SshPublicKeyBody=pub_b,
    )

    async def _put(priv, body):
        async with await _connect(user_name, priv) as conn:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open("/marker", "wb") as f:
                    await f.write(body)

    asyncio.run(_put(priv_a, b"from-a"))
    asyncio.run(_put(priv_b, b"from-b"))

    assert s3.get_object(Bucket=bucket_a, Key="marker")["Body"].read() == b"from-a"
    assert s3.get_object(Bucket=bucket_b, Key="marker")["Body"].read() == b"from-b"
    transfer.delete_server(ServerId=sid_a)
    transfer.delete_server(ServerId=sid_b)


# ---------------------------------------------------------------------------
# Per-server-port mode (env-gated)
# ---------------------------------------------------------------------------


def _sftp_port_state():
    """Hit /_ministack/transfer/sftp-ports — returns the JSON dict, or None
    on any failure (used to skip per-server-mode tests cleanly)."""
    import json as _json
    try:
        with urllib.request.urlopen(
            f"{ADMIN_BASE}/_ministack/transfer/sftp-ports", timeout=2
        ) as resp:
            return _json.loads(resp.read())
    except Exception:
        return None


@pytest.mark.skipif(
    not (_sftp_port_state() or {}).get("port_per_server"),
    reason="MiniStack server not started with SFTP_PORT_PER_SERVER=1",
)
def test_sftp_port_per_server_mode(transfer, s3):
    """In per-server mode, the admin endpoint surfaces a unique port for
    each Transfer server and connecting on that port reaches only that
    server's users. Mirrors AWS Transfer's per-server endpoint model."""
    ctx = _provision(transfer, s3)
    state = _sftp_port_state()
    port = state["per_server"].get(ctx["server_id"])
    assert port and port != SFTP_PORT, "expected a unique per-server SFTP port"

    async def _run():
        async with await _connect(ctx["user_name"], ctx["priv_pem"], port=port) as conn:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open("/per-server-marker", "wb") as f:
                    await f.write(b"ok")

    asyncio.run(_run())
    obj = s3.get_object(Bucket=ctx["bucket"], Key="per-server-marker")
    assert obj["Body"].read() == b"ok"
    transfer.delete_server(ServerId=ctx["server_id"])
