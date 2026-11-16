"""Wire-level body rules of CreatePullThroughCacheRule (capture keys r06-create-*)."""
import asyncio
import json

import pytest

from ministack.services import ecr

pytestmark = pytest.mark.serial  # shares the registry-wide rule quota, registry policy and service-linked role


@pytest.fixture(autouse=True)
def _clean():
    ecr.reset()
    yield
    ecr.reset()


def _raw(body):
    status, _headers, raw = asyncio.run(ecr.handle_request(
        "POST", "/", {"x-amz-target": "AmazonEC2ContainerRegistry_V20150921.CreatePullThroughCacheRule"},
        body, {}))
    return status, json.loads(raw)


@pytest.mark.parametrize("key,body,expected", [
    ("r06-create-body-empty", b"", {"__type": "SerializationException"}),
    ("r06-create-body-array", b"[]", {"__type": "SerializationException"}),
    ("r06-create-body-string", b'"x"',
     {"__type": "SerializationException", "Message": "Unexpected value type in payload"}),
    ("r06-create-body-not-json", b"not json",
     {"__type": "SerializationException", "Message": "Unexpected Character in input string at byte 1"}),
    ("r06-create-body-truncated", b'{"ecrRepositoryPrefix": "',
     {"__type": "SerializationException", "Message": "Unexpected end of payload"}),
    ("r06-create-body-trailing-comma", b'{"ecrRepositoryPrefix": "msc2617-r06-good",}',
     {"__type": "SerializationException", "Message": "Unexpected Character in input string at byte 43"}),
    ("r06-create-prefix-int", b'{"ecrRepositoryPrefix": 123, "upstreamRegistryUrl": "public.ecr.aws"}',
     {"__type": "SerializationException", "Message": "NUMBER_VALUE cannot be converted to String"}),
    ("r06-create-url-int", b'{"ecrRepositoryPrefix": "msc2617-r06-new", "upstreamRegistryUrl": 123}',
     {"__type": "SerializationException", "Message": "NUMBER_VALUE cannot be converted to String"}),
    ("r06-create-enum-int",
     b'{"ecrRepositoryPrefix": "msc2617-r06-new", "upstreamRegistryUrl": "public.ecr.aws", "upstreamRegistry": 123}',
     {"__type": "SerializationException", "Message": "NUMBER_VALUE cannot be converted to String"}),
    ("r06-create-wrong-case-member",
     b'{"EcrRepositoryPrefix": "msc2617-r06-wcase", "UpstreamRegistryUrl": "public.ecr.aws"}',
     {"__type": "InvalidParameterException",
      "message": "Invalid parameter at 'upstreamRegistryUrl' failed to satisfy constraint: "
                 "'Member must not be null'; Invalid parameter at 'ecrRepositoryPrefix' failed to satisfy "
                 "constraint: 'Member must not be null'"}),
])
def test_body_refused(key, body, expected):
    status, got = _raw(body)
    assert status == 400, key
    assert got == expected, key


@pytest.mark.parametrize("key,body,prefix", [
    ("r06-create-unknown-member",
     b'{"ecrRepositoryPrefix": "msc2617-r06-unk", "upstreamRegistryUrl": "public.ecr.aws", "notAMember": 1}',
     "msc2617-r06-unk"),
    ("r06-create-nested-object",
     b'{"ecrRepositoryPrefix": "msc2617-r06-nest", "upstreamRegistryUrl": "public.ecr.aws", '
     b'"x": {"y": [1, {"z": null}]}}', "msc2617-r06-nest"),
])
def test_unknown_members_ignored(key, body, prefix):
    status, got = _raw(body)
    assert status == 200, key
    assert got["ecrRepositoryPrefix"] == prefix
    assert got["upstreamRegistryUrl"] == "public.ecr.aws"


# ── Registry V2 credentials and the fill workers (ledger lines 220 and 221) ───────────────────────────────

def _action(action, body):
    status, _headers, raw = asyncio.run(ecr.handle_request(
        "POST", "/", {"x-amz-target": f"AmazonEC2ContainerRegistry_V20150921.{action}"}, json.dumps(body).encode(), {}))
    return status, json.loads(raw)


def _rule(prefix):
    assert _action("CreatePullThroughCacheRule",
                   {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "public.ecr.aws"})[0] == 200


def _manifest(path, headers):
    return asyncio.run(ecr.handle_registry_request("GET", path, headers, b"", {}))


@pytest.mark.parametrize("authorization", ["", "Basic", "Basic ", "Bearer   ", "Negotiate abc", "garbage", "Digest x"])
def test_an_authorization_header_without_a_known_scheme_and_credential_is_unsigned(monkeypatch, authorization):
    """r15-noauth-get-rule-path (AUTH=true). MiniStack contract, uncaptured: ledger line 220 (signed means a scheme
    MiniStack issues, Basic or Bearer or SigV4, with a non-empty credential)."""
    monkeypatch.setattr("ministack.app.AUTH", True)
    monkeypatch.setattr(ecr.ecr_pull_through, "fill", lambda *a: pytest.fail("an unsigned request started a fill"))
    _rule("msc2617-wire-auth")
    status, headers, body = _manifest("/v2/msc2617-wire-auth/alpine/manifests/3.20", {"authorization": authorization})
    assert status == 401 and body == b"Not Authorized\n" and "Www-Authenticate" in headers


@pytest.mark.parametrize("authorization", ["Basic QVdTOm1pbmlzdGFjay1hdXRoLXRva2Vu", "bearer t", "AWS4-HMAC-SHA256 Credential=x"])
def test_an_authorization_header_of_a_known_scheme_with_a_credential_is_signed(monkeypatch, authorization):
    """MiniStack contract, uncaptured: ledger line 220 (the signed answer is the fill of r15-v2-first-pull)."""
    monkeypatch.setattr("ministack.app.AUTH", True)
    filled = []
    monkeypatch.setattr(ecr.ecr_pull_through, "fill", lambda name, ids: filled.append(name))
    _rule("msc2617-wire-auth")
    status, _, _ = _manifest("/v2/msc2617-wire-auth/alpine/manifests/3.20", {"authorization": authorization})
    assert status == 404 and filled == ["msc2617-wire-auth/alpine"]


def test_only_requests_that_may_fetch_use_the_bounded_fill_executor(monkeypatch):
    """MiniStack contract, uncaptured: ledger line 221 (4 workers; an ordinary repository runs inline as on
    v1.5.39; a rule path or cache repository goes to the executor)."""
    pool = []

    async def fake_pool(fn, *args):
        pool.append(args)
        return fn(*args)

    monkeypatch.setattr(ecr.ecr_pull_through, "run_in_fill_pool", fake_pool)
    assert ecr.ecr_pull_through._fill_executor._max_workers == 4
    assert _action("CreateRepository", {"repositoryName": "ordinary"})[0] == 200
    _rule("msc2617-wire-pool")
    monkeypatch.setattr(ecr.ecr_pull_through, "fill", lambda *a: None)
    ordinary = {"repositoryName": "ordinary", "imageIds": [{"imageTag": "x"}]}
    assert _action("BatchGetImage", ordinary)[0] == 200
    assert _action("GetDownloadUrlForLayer", {"repositoryName": "ordinary", "layerDigest": "sha256:" + "0" * 64})[0] == 200
    assert pool == []
    ruled = {"repositoryName": "msc2617-wire-pool/alpine", "imageIds": [{"imageTag": "x"}]}
    assert _action("BatchGetImage", ruled)[0] == 400  # the stubbed fill brought no repository
    assert len(pool) == 1


def test_a_daemon_pull_past_its_deadline_is_unavailable(monkeypatch):
    """MiniStack contract, uncaptured: ledger line 221 (an overall deadline of 300 s; past it the outcome is the
    one of an unreachable upstream)."""
    through = ecr.ecr_pull_through
    assert through._PULL_DEADLINE == 300.0

    class Api:
        def pull(self, *args, **kwargs):
            return iter([{"status": "Pulling"}])

        def get_image(self, ref):
            pytest.fail("the deadline passed before the image was saved")

    class Client:
        api = Api()

    monkeypatch.setattr(through, "_docker_client", lambda: Client())
    monkeypatch.setattr(through, "_PULL_DEADLINE", -1.0)
    assert through._daemon_pull("public.ecr.aws", "a/b", "1", None) == ("unavailable", "", b"", {})
