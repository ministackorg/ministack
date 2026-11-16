"""ECR pull-through cache: other-registry refusals, repository delete, HEAD blob, throttle retry, redirects,
and the unsigned V2 fill under AUTH=true (issue 2617). In-process, with the upstream stubbed."""


import pytest

from ministack.core.iam_evaluator import caller_arn
from ministack.core.responses import get_account_id, get_region
from ministack.services import ecr
from tests.test_ecr_pull_through_fill import (  # noqa: F401  (fixture and helpers shared with the fill tests)
    LAYER,
    PREFIX,
    PULL,
    _action,
    _digest,
    _pull,
    _rule,
    _v2,
    upstream,
)

pytestmark = pytest.mark.serial


@pytest.fixture
def pull_through():
    """Bind the module as this file's ``ecr_pull_through`` (imported here, not at the top)."""
    global ecr_pull_through
    from ministack.services import ecr_pull_through

    return ecr_pull_through


@pytest.mark.parametrize("action,body", [
    # r04-upd-existing-regid-other, r05-delete-regid-other, r05-desc-regid-other, r05-validate-regid-other
    ("UpdatePullThroughCacheRule", {"registryId": "111122223333", "ecrRepositoryPrefix": "msc-2617-r04-update-dh",
                                   "credentialArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:x"}),
    ("DeletePullThroughCacheRule", {"registryId": "111122223333", "ecrRepositoryPrefix": "msc2617-r05-b"}),
    ("DescribePullThroughCacheRules", {"registryId": "111122223333"}),
    ("ValidatePullThroughCacheRule", {"registryId": "111122223333", "ecrRepositoryPrefix": "msc2617-r05-b"}),
])
def test_another_accounts_registry_is_access_denied_on_every_rule_action(upstream, action, body):
    status, answer = _action(action, body)
    assert status == 400 and answer == {
        "__type": "AccessDeniedException",
        "Message": f"User: {caller_arn()} is not authorized to perform: ecr:{action} on resource: * because no "
                   f"resource-based policy allows the ecr:{action} action",
    }


def test_delete_repository_drops_origin_and_blobs(upstream):
    # ANSWERS origins-after-repository-delete: held manifest and layer records go with the repository
    _rule(PREFIX, "public.ecr.aws")
    _pull()
    assert ecr_pull_through._origins.get(PULL) and ecr._manifest_blobs.get(PULL)
    ecr._layer_blobs.setdefault(PULL, {})["sha256:x"] = b"x"
    status, _ = _action("DeleteRepository", {"repositoryName": PULL, "force": True})
    assert status == 200
    assert PULL not in ecr_pull_through._origins
    assert PULL not in ecr._layer_blobs and PULL not in ecr._manifest_blobs


def test_ensure_layer_leaves_no_empty_layer_entry(upstream):
    _action("CreateRepository", {"repositoryName": "lay"})
    _action("GetDownloadUrlForLayer", {"repositoryName": "lay", "layerDigest": _digest(LAYER)})
    assert "lay" not in ecr._layer_blobs


def test_head_blob_runs_the_on_demand_fetch_like_get(upstream):
    # uncaptured (ANSWERS head-blob-on-demand): GET and HEAD agree
    _rule(PREFIX, "public.ecr.aws")
    _pull()
    ecr._layer_blobs.get(PULL, {}).pop(_digest(LAYER), None)
    status, headers, body = _v2("HEAD", f"/v2/{PULL}/blobs/{_digest(LAYER)}")
    assert status == 200 and body == b""
    assert headers["Content-Length"] == str(len(LAYER)) and headers["Docker-Content-Digest"] == _digest(LAYER)
    assert ecr._layer_blobs[PULL][_digest(LAYER)] == LAYER


def test_head_blob_of_a_failed_fetch_is_blob_unknown(upstream):
    _rule(PREFIX, "public.ecr.aws")
    _pull()
    ecr._layer_blobs.get(PULL, {}).pop(_digest(LAYER), None)
    upstream.down = True
    status, _, _ = _v2("HEAD", f"/v2/{PULL}/blobs/{_digest(LAYER)}")
    assert status == 404


@pytest.mark.parametrize("retry_after,waits", [(None, [1, 2, 4]), ("3", [3, 3, 3]), ("99", [10, 10, 10])])
def test_429_retry_waits(monkeypatch, retry_after, waits, pull_through):
    # ANSWERS upstream-throttle-retry: 3 retries; min(Retry-After, 10) s, else 1, 2, 4 s
    slept = []
    reply = {"retry-after": retry_after} if retry_after else {}
    monkeypatch.setattr(ecr_pull_through, "_http_get_once", lambda url, headers: (429, reply, b""))
    monkeypatch.setattr(ecr_pull_through.time, "sleep", slept.append)
    assert ecr_pull_through._http_get("https://r.test/v2/", {})[0] == 429
    assert slept == waits


def test_redirect_to_http_is_refused(monkeypatch, pull_through):
    # ANSWERS upstream-redirect-and-size: only https redirects are followed
    class Opener:
        def open(self, request, timeout=None):
            import urllib.error
            raise urllib.error.HTTPError(request.full_url, 302, "Found", {"Location": "http://elsewhere.test/t"}, None)

    monkeypatch.setattr(ecr_pull_through, "_opener", Opener())
    with pytest.raises(OSError):
        ecr_pull_through._http_get_once("https://r.test/token", {})


def test_unsigned_manifest_fill_under_auth_is_401_and_creates_nothing(upstream, monkeypatch):
    # r15-noauth-get-rule-path, r15-noauth-get-rule-path--repo (AUTH=true only)
    monkeypatch.setattr("ministack.app.AUTH", True)
    _rule(PREFIX, "public.ecr.aws")
    realm = f'Basic realm="https://{get_account_id()}.dkr.ecr.{get_region()}.amazonaws.com/",service="ecr.amazonaws.com"'
    for method in ("GET", "HEAD"):
        status, headers, body = _v2(method, f"/v2/{PULL}/manifests/3.20", signed=False)
        assert status == 401
        assert headers["Content-Type"] == "text/plain; charset=utf-8"
        assert headers["Docker-Distribution-API-Version"] == "registry/2.0"
        assert headers["Www-Authenticate"] == realm
        assert body == (b"Not Authorized\n" if method == "GET" else b"")
        assert headers["Content-Length"] == "15"
    assert PULL not in ecr._repositories and not upstream.urls
    status, _, _ = _v2("GET", f"/v2/{PULL}/manifests/3.20", signed=True)
    assert status == 200


class _Body:
    """A response whose body reads like a stream: ``read(n)`` gives at most n bytes."""

    def __init__(self, data):
        self.data, self.status, self.headers = data, 200, {}

    def read(self, size=-1):
        return self.data if size is None or size < 0 else self.data[:size]

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _opener_with(monkeypatch, response=None, error=None):
    import urllib.error

    class Opener:
        def open(self, request, timeout=None):
            if error is not None:
                raise urllib.error.HTTPError(request.full_url, error[0], "x", {}, _Body(error[1]))
            return response

    monkeypatch.setattr(ecr_pull_through, "_opener", Opener())


def test_a_body_of_exactly_1_mib_is_read_whole(monkeypatch, pull_through):
    """MiniStack contract, uncaptured: ledger 214 (ledger 188: read at most 1 MiB; only more is no answer)."""
    data = b"x" * (1 << 20)
    _opener_with(monkeypatch, _Body(data))
    assert ecr_pull_through._http_get_once("https://r.test/v2/", {})[2] == data


@pytest.mark.parametrize("kind", ["probe-or-token", "error-body"])
def test_a_body_over_1_mib_is_no_answer_not_a_truncated_one(monkeypatch, pull_through, kind):
    """MiniStack contract, uncaptured: ledger 214 (ledger 188): over 1 MiB raises OSError, which every caller
    treats as no answer; the body is never cut and parsed. Covers the /v2/ probe, the token answer and the error body."""
    data = b"x" * ((1 << 20) + 1)
    _opener_with(monkeypatch, *((_Body(data), None) if kind == "probe-or-token" else (None, (500, data))))
    with pytest.raises(OSError):
        ecr_pull_through._http_get_once("https://r.test/v2/", {})


@pytest.mark.parametrize("size,outcome", [(1 << 20, ("ok", "t" * 10)), ((1 << 20) + 1, ("unavailable", None))])
def test_a_token_answer_is_unavailable_only_when_over_1_mib(monkeypatch, pull_through, size, outcome):
    """MiniStack contract, uncaptured: ledger 214 (ledger 188): a token answer of more than 1 MiB is no answer
    (UNABLE_TO_VALIDATE), even when its first 1 MiB parses; exactly 1 MiB is read whole."""
    import urllib.error

    head = b'{"token":"tttttttttt","pad":"'
    body = head + b" " * (size - len(head) - 2) + b'"}'
    assert len(body) == size

    class Opener:
        def open(self, request, timeout=None):
            if request.full_url.endswith("/v2/"):
                raise urllib.error.HTTPError(request.full_url, 401, "x", {
                    "WWW-Authenticate": 'Bearer realm="https://auth.test/token"'}, _Body(b""))
            return _Body(body)

    monkeypatch.setattr(ecr_pull_through, "_opener", Opener())
    assert ecr_pull_through._bearer_token("r.test", None) == outcome
