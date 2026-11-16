"""ECR pull-through cache: fixes of the third independent audit (issue 2617).

In-process; the upstream is a fake host-daemon pull. Captured answers are cited by key, the rest is the
MiniStack contract decided in the ledger.
"""
import asyncio
import concurrent.futures
import hashlib
import json
import threading

import pytest

from ministack.services import ecr, ecr_registry_policy

pytestmark = pytest.mark.serial  # shares the registry-wide rule quota, registry policy and service-linked role

PREFIX = "msc2617-aud"
NAME = f"{PREFIX}/docker/library/alpine"
INDEX_TYPE = "application/vnd.oci.image.index.v1+json"
MANIFEST_TYPE = "application/vnd.oci.image.manifest.v1+json"
CHILD = "sha256:" + hashlib.sha256(b"child").hexdigest()
INDEX = json.dumps({"schemaVersion": 2, "mediaType": INDEX_TYPE, "manifests": [
    {"mediaType": MANIFEST_TYPE, "digest": CHILD, "size": 10}]}).encode()


@pytest.fixture
def through(monkeypatch):
    from ministack.services import ecr_pull_through

    ecr.reset()
    pulls = []

    def pull(host, path, reference, credentials):
        pulls.append(reference)
        if reference == "latest":
            return "ok", INDEX_TYPE, INDEX, {}
        return "unavailable", "", b"", {}

    monkeypatch.setattr(ecr_pull_through, "_daemon_pull", pull)
    ecr_pull_through.pulls = pulls
    yield ecr_pull_through
    ecr.reset()


def _call(action, body):
    raw = body if isinstance(body, (bytes, str)) else json.dumps(body)
    status, _headers, out = asyncio.run(ecr.handle_request(
        "POST", "/", {"x-amz-target": f"AmazonEC2ContainerRegistry_V20150921.{action}"},
        raw.encode() if isinstance(raw, str) else raw, {}))
    return status, json.loads(out)


def _rule(prefix=PREFIX):
    assert _call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "public.ecr.aws"})[0] == 200


def _fill():
    _rule()
    status, body = _call("BatchGetImage", {"repositoryName": NAME, "imageIds": [{"imageTag": "latest"}]})
    assert status == 200 and body["failures"] == []


def _v2(method, path):
    return asyncio.run(ecr.handle_registry_request(method, path, {"Authorization": "AWS4-HMAC-SHA256 x"}, b"", {}))


def test_lone_surrogate_in_a_policy_is_refused_and_stores_nothing():
    """r08-put-invalid-policy-text style answer ("Invalid registry policy provided"). MiniStack contract,
    uncaptured: ledger 235 (a lone surrogate never gives a 500)."""
    ecr.reset()
    for field in ("Resource", "Sid"):
        statement = {"Effect": "Allow", "Principal": "*", "Action": "ecr:DescribeImages", "Resource": "*"}
        statement[field] = "arn:\\ud800" if field == "Resource" else "s\\ud800"
        text = json.dumps({"Statement": [statement]}).replace("\\\\ud800", "\\ud800")
        status, body = _call("PutRegistryPolicy", {"policyText": text})
        assert status == 400 and body["message"].endswith("constraint: 'Invalid registry policy provided'")
        assert _call("GetRegistryPolicy", {})[1]["__type"] == "RegistryPolicyNotFoundException"
    ecr.reset()


def test_lone_surrogate_in_a_rule_url_is_a_400_not_a_500():
    """MiniStack contract, uncaptured: ledger 235."""
    ecr.reset()
    status, body = _call("CreatePullThroughCacheRule",
                         '{"ecrRepositoryPrefix": "msc2617-aud", "upstreamRegistryUrl": "a\\ud800"}')
    assert status == 400 and body["__type"] == "UnsupportedUpstreamRegistryException"
    assert body["message"] == "The upstream registry URL a? is invalid. Specify a valid upstream registry URL and try again"
    status, body = _call("BatchGetImage", {"repositoryName": "a\ud800", "imageIds": []})
    assert (status, body["__type"], body["message"]) == (
        400, "RepositoryNotFoundException",
        "The repository with name 'a?' does not exist in the registry with id '000000000000'")
    ecr.reset()


def test_integer_token_beyond_the_int_digit_limit_answers_like_a_too_large_integer():
    """r06-describe-max-int64-over (the same answer for an integer too long for int()). MiniStack contract,
    uncaptured: ledger 235."""
    ecr.reset()
    captured = _call("DescribePullThroughCacheRules", '{"maxResults": 9223372036854775808}')
    assert captured[0] == 400
    assert _call("DescribePullThroughCacheRules", '{"maxResults": ' + "9" * 5000 + "}") == captured
    assert _call("DescribePullThroughCacheRules", '{"maxResults": -' + "9" * 5000 + "}")[0] == 400
    ecr.reset()


@pytest.mark.parametrize("prefix", ["a/", "1/"])
def test_a_one_character_prefix_with_a_trailing_slash_is_accepted(prefix):
    """MiniStack contract, uncaptured: ledger 237 (the length is measured on the raw value, as in
    r33-create-slash-only)."""
    ecr.reset()
    body = {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "public.ecr.aws"}
    assert _call("CreatePullThroughCacheRule", body)[0] == 200
    ecr.reset()


def test_a_child_of_a_stored_index_the_daemon_cannot_fetch_is_a_missing_image(through):
    """MiniStack contract, uncaptured: ledger 235 (maintainer answer child-platform-on-demand)."""
    _fill()
    status, body = _call("BatchGetImage", {"repositoryName": NAME, "imageIds": [{"imageDigest": CHILD}]})
    assert status == 200 and body["images"] == []
    assert body["failures"][0]["failureCode"] == "ImageNotFound"
    for method in ("GET", "HEAD"):
        status, _headers, raw = _v2(method, f"/v2/{NAME}/manifests/{CHILD}")
        assert status == 404
        if method == "GET":
            assert json.loads(raw)["errors"][0]["code"] == "MANIFEST_UNKNOWN"


def test_a_null_or_numeric_repository_name_is_not_found_with_a_rule_present(through):
    """MiniStack contract, uncaptured: ledger 235 (the v1.5.39 answer, never a 500)."""
    _rule()
    for name in (None, 123, [1], {"a": 1}):
        status, body = _call("BatchGetImage", {"repositoryName": name, "imageIds": []})
        assert status == 400 and body["__type"] == "RepositoryNotFoundException"
        assert body["message"] == f"The repository with name '{name}' does not exist"
    assert through.match_rule(None) is None and through.fill(5, []) is None


def test_a_held_image_is_served_while_the_fill_executor_is_busy(through, monkeypatch):
    """MiniStack contract, uncaptured: ledger 235 (only a request that contacts the upstream waits for a
    fill worker)."""
    _fill()
    busy = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    release = threading.Event()
    busy.submit(release.wait)
    monkeypatch.setattr(through, "_fill_executor", busy)
    try:
        async def served():
            held = {"repositoryName": NAME, "imageIds": [{"imageTag": "latest"}]}
            body = json.dumps(held).encode()
            status, _, out = await asyncio.wait_for(ecr.handle_request(
                "POST", "/", {"x-amz-target": "AmazonEC2ContainerRegistry_V20150921.BatchGetImage"}, body, {}), 5)
            registry = await asyncio.wait_for(ecr.handle_registry_request(
                "HEAD", f"/v2/{NAME}/manifests/latest", {"Authorization": "AWS4-HMAC-SHA256 x"}, b"", {}), 5)
            return status, json.loads(out), registry[0]

        status, body, registry_status = asyncio.run(served())
        assert status == 200 and len(body["images"]) == 1 and registry_status == 200
        assert not busy._work_queue.qsize()
    finally:
        release.set()
        busy.shutdown()


@pytest.mark.parametrize("manifest", [
    {"schemaVersion": 2, "config": {"digest": ["x"], "size": 1}, "layers": []},
    {"schemaVersion": 2, "mediaType": INDEX_TYPE, "manifests": [{"digest": {"a": 1}, "size": 1}]},
    {"schemaVersion": 2, "mediaType": 5, "layers": []},
    {"schemaVersion": 2, "config": {"digest": "sha256:a", "size": "1"}, "layers": []},
])
def test_a_malformed_upstream_manifest_is_unavailable_and_leaves_no_repository(through, monkeypatch, manifest):
    """MiniStack contract, uncaptured: ledger 235 (the existing malformed-upstream answer)."""
    monkeypatch.setattr(through, "_daemon_pull", lambda *a: ("ok", MANIFEST_TYPE, json.dumps(manifest).encode(), {}))
    _rule()
    status, body = _call("BatchGetImage", {"repositoryName": NAME, "imageIds": [{"imageTag": "latest"}]})
    assert status == 200 and body["failures"] == [{
        "imageId": {"imageTag": "latest"}, "failureCode": "UpstreamUnavailable",
        "failureReason": "Unable to reach the upstream registry."}]
    assert NAME not in ecr._repositories and NAME not in through._origins


def test_a_deeply_nested_upstream_manifest_is_unavailable_not_a_500(through, monkeypatch):
    """MiniStack contract, uncaptured: ledger 237 (a parse failure of any kind is a malformed manifest)."""
    monkeypatch.setattr(through, "_daemon_pull", lambda *a: ("ok", MANIFEST_TYPE, b"[" * 100000, {}))
    _rule()
    status, body = _call("BatchGetImage", {"repositoryName": NAME, "imageIds": [{"imageTag": "latest"}]})
    assert status == 200 and body["failures"][0]["failureCode"] == "UpstreamUnavailable"
    assert NAME not in ecr._repositories and NAME not in through._origins


@pytest.mark.parametrize("outcome, code", [("unavailable", "UpstreamUnavailable"), ("denied", "UpstreamAccessDenied")])
def test_a_first_fill_still_reports_an_unreachable_or_denying_upstream(through, monkeypatch, outcome, code):
    """MiniStack contract, uncaptured: ledger 237 (the child-platform rule leaves a first fill's outcomes)."""
    monkeypatch.setattr(through, "_daemon_pull", lambda *a: (outcome, "", b"", {}))
    _rule()
    status, body = _call("BatchGetImage", {"repositoryName": NAME, "imageIds": [{"imageTag": "latest"}]})
    assert status == 200 and body["images"] == [] and body["failures"][0]["failureCode"] == code
    assert NAME not in ecr._repositories


def test_a_request_that_must_fetch_runs_on_the_fill_pool_and_a_held_one_does_not(through, monkeypatch):
    """MiniStack contract, uncaptured: ledger 237."""
    calls = []
    real = through.run_in_fill_pool

    async def spy(fn, *args):
        calls.append(fn)
        return await real(fn, *args)

    monkeypatch.setattr(through, "run_in_fill_pool", spy)
    _fill()
    assert len(calls) == 1
    status, body = _call("BatchGetImage", {"repositoryName": NAME, "imageIds": [{"imageTag": "latest"}]})
    assert status == 200 and len(body["images"]) == 1
    assert len(calls) == 1


def test_a_non_string_layer_digest_on_a_cache_repository_is_a_serialization_error(through):
    """MiniStack contract, uncaptured: ledger 235 (SerializationException "Unexpected field type")."""
    _fill()
    for action, body in (("GetDownloadUrlForLayer", {"layerDigest": ["x"]}),
                         ("GetDownloadUrlForLayer", {"layerDigest": 5}),
                         ("BatchCheckLayerAvailability", {"layerDigests": ["sha256:a", 7]}),
                         ("BatchCheckLayerAvailability", {"layerDigests": [["x"]]})):
        status, out = _call(action, {"repositoryName": NAME, **body})
        assert status == 400 and out == {"__type": "SerializationException", "Message": "Unexpected field type"}


def test_the_docker_client_is_closed_when_the_ping_fails(through, monkeypatch):
    """MiniStack contract, uncaptured: ledger 237."""
    import sys
    import types

    closed = []

    class Client:
        def ping(self):
            raise RuntimeError("no daemon")

        def close(self):
            closed.append(1)

    fake = types.SimpleNamespace(from_env=lambda **kw: Client())
    monkeypatch.setitem(sys.modules, "docker", fake)
    assert through._docker_client() is None
    assert closed == [1]


def test_the_docker_client_is_closed_after_each_pull(through, monkeypatch):
    """MiniStack contract, uncaptured: ledger 235."""
    closed = []

    class Api:
        def pull(self, *args, **kwargs):
            raise RuntimeError("daemon failure")

    class Client:
        api = Api()

        def close(self):
            closed.append(1)

    monkeypatch.setattr(through, "_docker_client", lambda: Client())
    assert through._daemon_pull_once("public.ecr.aws", "a/b", "1", None)[0] == "unavailable"
    assert closed == [1]


def test_layer_availability_omits_layer_size_on_cache_repositories_only(through):
    """r07-layer-availability (an UNAVAILABLE layer has no layerSize). An ordinary repository keeps v1.5.39's
    layerSize 0. MiniStack contract, uncaptured: ledger 235."""
    _fill()
    digest = "sha256:" + "0" * 64
    status, body = _call("BatchCheckLayerAvailability", {"repositoryName": NAME, "layerDigests": [digest]})
    assert status == 200 and body == {"failures": [], "layers": [{"layerAvailability": "UNAVAILABLE", "layerDigest": digest}]}
    assert _call("CreateRepository", {"repositoryName": "ordinary"})[0] == 200
    status, body = _call("BatchCheckLayerAvailability", {"repositoryName": "ordinary", "layerDigests": [digest]})
    assert body["layers"] == [{"layerDigest": digest, "layerAvailability": "UNAVAILABLE", "layerSize": 0}]


def _policy(action="ecr:DescribeImages", **extra):
    text = json.dumps({"Statement": [{"Effect": "Allow", "Principal": "*", "Action": action, "Resource": "*", **extra}]})
    return _call("PutRegistryPolicy", {"policyText": text})


def test_registry_policy_refuses_an_infinite_number_and_bracket_wildcards():
    """MiniStack contract, uncaptured: ledger 235 (1e999 is refused like Infinity; only * and ? are wildcards;
    matching stays case-insensitive)."""
    ecr.reset()
    refused = "Invalid registry policy provided"
    assert refused in _call("PutRegistryPolicy", {"policyText": '{"Id": 1e999, "Statement": []}'})[1]["message"]
    text = ('{"Statement":[{"Effect":"Allow","Principal":"*","Action":"ecr:DescribeImages","Resource":"*",'
            '"Condition":{"NumericEquals":{"x":1e999}}}]}')
    assert refused in _call("PutRegistryPolicy", {"policyText": text})[1]["message"]
    assert refused in _policy("ecr:[a-z]*")[1]["message"]
    assert _policy("ecr:batchgetimage")[0] == 200
    assert _policy("ecr:Batch*")[0] == 200
    assert _policy("ecr:Descr?beImages")[0] == 200
    assert ecr_registry_policy._action_known("ecr:[") is False
    ecr.reset()
