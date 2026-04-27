"""
Regression tests for three medium/low correctness bugs:

  - apigateway + apigateway_v1 get_state() returned live
    AccountScopedDict references rather than deep copies. A
    concurrent write during shutdown serialisation could corrupt
    the persisted snapshot.

  - secretsmanager._delete_secret(force=True) deleted the secret
    record but left orphan entries in `_resource_policies` (keyed
    by ARN) — invisible to the API but accumulating in memory and
    surviving warm-boot via the persistence path.

  - acm._list_certificates returned `{"NextToken": null}`
    unconditionally. Real AWS omits the key when there is no next
    page; SDK consumers that paginate via
    `if "NextToken" in response` will loop forever.

Each test bypasses boto3 to inspect a layer it would otherwise hide:
in-process module state for the first two, or the wire-level JSON
before client-side null-stripping for the NextToken case.
"""
import importlib
import json
import os
import urllib.request

import pytest

# Match the project convention from tests/conftest.py — honours
# `MINISTACK_ENDPOINT` so tests run unchanged against a non-default
# host / port (Docker networking, alternate CI bind, etc.).
ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _module(name):
    return importlib.import_module(f"ministack.services.{name}")


# ── apigateway / apigateway_v1 get_state must deep-copy ───────────────

@pytest.mark.parametrize("mod_name", ["apigateway", "apigateway_v1"])
def test_apigateway_get_state_returns_independent_copy(mod_name):
    """`get_state()` must return a snapshot decoupled from live module
    state — i.e. a `copy.deepcopy()` of each dict, not the live
    reference. If it returns the live ref, a concurrent write during
    shutdown serialisation corrupts the persisted JSON.

    Asserted by identity (`is not`) on every dict returned: each value
    in the snapshot must be a different Python object than the
    corresponding module-level state dict, so any subsequent mutation
    on either side cannot affect the other."""
    mod = _module(mod_name)
    if hasattr(mod, "reset"):
        mod.reset()

    snapshot = mod.get_state()

    leaks = []
    for key, snap_value in snapshot.items():
        live = getattr(mod, f"_{key}", None)
        if live is None:
            continue  # snapshot key without a matching `_key` attr
        if snap_value is live:
            leaks.append(key)

    assert not leaks, (
        f"{mod_name}.get_state() returned LIVE references for these keys: "
        f"{leaks}. A concurrent write to one of these dicts during "
        "shutdown serialisation would corrupt the persisted JSON. "
        f"Wrap each value in `copy.deepcopy(...)`."
    )

    if hasattr(mod, "reset"):
        mod.reset()


# ── secretsmanager force-delete must clean up orphan policies ─────────

def test_secretsmanager_force_delete_removes_resource_policy():
    """ForceDeleteWithoutRecovery must remove not just the secret but
    also its associated `_resource_policies[arn]` entry. Otherwise the
    policy lingers as an orphan referenced by an ARN no longer in
    `_secrets` — invisible to APIs but accumulating in memory.

    Tests the in-process module directly (rather than via boto3
    against the live server) so the assertion can observe both the
    `_secrets` and `_resource_policies` dicts together."""
    sm = _module("secretsmanager")
    sm.reset()

    # Stage 1: create a secret with a resource policy via the module's
    # action handlers, mirroring what boto3 -> handle_request would do.
    create_resp = json.loads(_invoke_action(
        sm, "CreateSecret",
        {"Name": "force-delete-canary", "SecretString": "x"},
    ))
    arn = create_resp["ARN"]
    _invoke_action(sm, "PutResourcePolicy", {
        "SecretId": arn,
        "ResourcePolicy": '{"Version":"2012-10-17","Statement":[]}',
    })
    assert arn in sm._resource_policies, "Test setup failed — policy didn't register"

    # Stage 2: force-delete.
    _invoke_action(sm, "DeleteSecret", {
        "SecretId": arn,
        "ForceDeleteWithoutRecovery": True,
    })

    # Stage 3: assert the policy entry is also gone.
    assert arn not in sm._resource_policies, (
        "Force-deleting a secret left an orphan entry in "
        "`_resource_policies` keyed by the now-deleted ARN. The "
        "delete path must pop both `_secrets[name]` AND "
        "`_resource_policies[arn]`."
    )
    sm.reset()


def _invoke_action(mod, action, params):
    """Mini-helper: run a service module's action handler synchronously
    and return the raw JSON body. Bypasses boto3 + HTTP so tests can
    observe in-process module state."""
    import asyncio
    headers = {"x-amz-target": f"secretsmanager.{action}"}
    body = json.dumps(params).encode()
    status, _resp_headers, resp_body = asyncio.run(
        mod.handle_request("POST", "/", headers, body, {})
    )
    if status >= 300:
        raise AssertionError(f"{action} failed: {status} {resp_body!r}")
    return resp_body.decode() if isinstance(resp_body, bytes) else resp_body


# ── ACM ListCertificates must omit NextToken when no more pages ───────

def test_acm_list_certificates_omits_nexttoken_when_no_more_pages():
    """Returning `{"NextToken": null}` is non-standard AWS — real AWS
    omits the key. boto3 strips null fields client-side so a boto3-
    only test can't see this, but other SDKs (Java, Go, raw HTTP) and
    pagination loops checking `if "NextToken" in response` see the
    literal null and loop forever.

    Asserted at the wire level via raw HTTP to bypass boto3's null
    stripping."""
    req = urllib.request.Request(
        ENDPOINT.rstrip("/") + "/",
        method="POST",
        headers={
            "x-amz-target": "CertificateManager.ListCertificates",
            "Content-Type": "application/x-amz-json-1.1",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/x/us-east-1/acm/aws4_request",
        },
        data=b"{}",
    )
    body = json.loads(urllib.request.urlopen(req, timeout=5).read())

    assert "NextToken" not in body, (
        f"ListCertificates wire response contains NextToken when "
        f"there is no next page (got {body.get('NextToken')!r}). "
        "Real AWS omits the key. SDK consumers checking "
        "`if 'NextToken' in response` (Java, Go, raw HTTP — boto3 "
        "strips nulls) loop forever against a literal null."
    )
