"""
Regression tests for "state dict dropped from get_state/restore_state" bugs.

Pattern: a service exposes an API that mutates an `AccountScopedDict`,
but the dict is missing from `get_state()` and/or `restore_state()`. With
PERSIST_STATE=1, every record stored via that API silently disappears on
the next restart.

This file covers five distinct state-dict persistence drops:

  - secretsmanager._resource_policies
  - kinesis._consumers             (enhanced fan-out)
  - ecs._attributes                (PutAttributes / ListAttributes)
  - sns._platform_applications
  - sns._platform_endpoints

Each test populates the dict, snapshots state via the public
`get_state()` / `restore_state()` contract, simulates a restart, and
asserts the record survived.
"""
import importlib

import pytest

from ministack.core import persistence


def _module(mod_name):
    return importlib.import_module(f"ministack.services.{mod_name}")


@pytest.fixture(autouse=True)
def _enable_persistence(monkeypatch, tmp_path):
    """Force PERSIST_STATE on and point STATE_DIR at a tmp dir for the
    duration of each test so save_state / load_state actually write and
    read JSON files instead of short-circuiting."""
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))


def _round_trip(mod, svc_key):
    """Simulate a full warm-boot through the on-disk JSON path.

    Going through `save_state` / `load_state` (rather than calling
    `get_state` / `restore_state` directly in-memory) catches encoder
    / decoder regressions AND import-order bugs (a `restore_state`
    that references a globals-only symbol declared further down the
    module would NameError on real warm-boot but pass an in-memory
    test that already has the symbol bound)."""
    persistence.save_state(svc_key, mod.get_state())
    mod.reset()
    loaded = persistence.load_state(svc_key)
    assert loaded is not None, (
        f"persistence.load_state({svc_key!r}) returned None — state "
        "file was not written by save_state()."
    )
    mod.restore_state(loaded)


# ── secretsmanager._resource_policies ──────────────────────────────────

def test_secretsmanager_resource_policies_survive_warm_boot():
    """`PutResourcePolicy` writes to `_resource_policies`, but if that
    dict is missing from `get_state()` the policy is gone after restart.
    Terraform `aws_secretsmanager_secret_policy` would silently drop."""
    mod = _module("secretsmanager")
    mod.reset()
    arn = "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret-AbCdEf"
    mod._resource_policies[arn] = '{"Version":"2012-10-17","Statement":[]}'

    _round_trip(mod, "secretsmanager")

    assert mod._resource_policies.get(arn) == '{"Version":"2012-10-17","Statement":[]}', (
        "Resource policy lost across get_state → restore_state — "
        "_resource_policies must be in both."
    )
    mod.reset()


# ── kinesis._consumers ─────────────────────────────────────────────────

def test_kinesis_consumers_survive_warm_boot():
    """`RegisterStreamConsumer` writes to `_consumers`. Without
    persistence symmetry, every enhanced fan-out registration is lost on
    restart and `DescribeStreamConsumer` returns ResourceNotFoundException."""
    mod = _module("kinesis")
    mod.reset()
    consumer_arn = (
        "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream/consumer/c1:123"
    )
    mod._consumers[consumer_arn] = {
        "ConsumerARN": consumer_arn,
        "ConsumerName": "c1",
        "ConsumerStatus": "ACTIVE",
        "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
        "ConsumerCreationTimestamp": 1700000000.0,
    }

    _round_trip(mod, "kinesis")

    assert consumer_arn in mod._consumers, (
        "Kinesis consumer lost across get_state → restore_state — "
        "_consumers must be in both."
    )
    mod.reset()


# ── ecs._attributes ────────────────────────────────────────────────────

def test_ecs_attributes_survive_warm_boot():
    """`PutAttributes` writes to `_attributes`. Lost on restart without
    persistence wiring."""
    mod = _module("ecs")
    mod.reset()
    mod._attributes["i-deadbeef:my-attr"] = {
        "name": "my-attr",
        "value": "v1",
        "targetType": "container-instance",
        "targetId": "i-deadbeef",
    }

    _round_trip(mod, "ecs")

    assert "i-deadbeef:my-attr" in mod._attributes, (
        "ECS attribute lost across get_state → restore_state — "
        "_attributes must be in both."
    )
    mod.reset()


# ── sns._platform_applications + sns._platform_endpoints ──────────────

def test_sns_platform_applications_survive_warm_boot():
    """`CreatePlatformApplication` writes to `_platform_applications`.
    Mobile push topology is lost on restart without persistence wiring."""
    mod = _module("sns")
    mod.reset()
    app_arn = "arn:aws:sns:us-east-1:000000000000:app/GCM/MyApp"
    mod._platform_applications[app_arn] = {
        "PlatformApplicationArn": app_arn,
        "Attributes": {"Platform": "GCM"},
    }

    _round_trip(mod, "sns")

    assert app_arn in mod._platform_applications, (
        "SNS platform application lost across get_state → restore_state — "
        "_platform_applications must be in both."
    )
    mod.reset()


def test_sns_platform_endpoints_survive_warm_boot():
    """`CreatePlatformEndpoint` writes to `_platform_endpoints`."""
    mod = _module("sns")
    mod.reset()
    ep_arn = "arn:aws:sns:us-east-1:000000000000:endpoint/GCM/MyApp/abc"
    mod._platform_endpoints[ep_arn] = {
        "EndpointArn": ep_arn,
        "Token": "device-token-xyz",
        "Enabled": "true",
    }

    _round_trip(mod, "sns")

    assert ep_arn in mod._platform_endpoints, (
        "SNS platform endpoint lost across get_state → restore_state — "
        "_platform_endpoints must be in both."
    )
    mod.reset()


# ── Import-order regression for the ECS NameError trap ───────────────

def test_ecs_module_reload_with_persisted_attributes_does_not_namerror():
    """Regression for the import-order trap: `restore_state()` runs at
    module import time (via the `try: load_state("ecs")` block at the
    bottom of services/ecs.py). If `_attributes` is declared AFTER that
    block, the restore call NameErrors and the surrounding try/except
    silently swallows it — wiping all ECS state on warm-boot.

    This test simulates a real warm-boot: write a populated `ecs.json`
    to STATE_DIR, then `importlib.reload()` the module so the load_state
    block runs against the file. If `_attributes` (or any other
    referenced symbol) is declared too late, the restored state will
    be missing because the entire restore_state body crashed."""
    mod = _module("ecs")
    mod.reset()
    arn = "arn:aws:ecs:us-east-1:000000000000:cluster/reload-canary"
    mod._clusters[arn] = {"clusterArn": arn, "status": "ACTIVE"}
    mod._attributes["i-canary:reload-attr"] = {
        "name": "reload-attr",
        "value": "v",
        "targetType": "container-instance",
        "targetId": "i-canary",
    }

    # Persist via the same path save_all uses on shutdown.
    persistence.save_state("ecs", mod.get_state())

    # Force a full reload so the module-level try/load_state/restore_state
    # block at the bottom of ecs.py executes against the on-disk JSON.
    importlib.reload(mod)

    assert arn in mod._clusters, (
        "Cluster lost after reload — likely NameError in restore_state "
        "swallowed by the try/except. Check that every referenced state "
        "dict (_attributes etc.) is declared BEFORE the load_state block."
    )
    assert "i-canary:reload-attr" in mod._attributes, (
        "ECS _attributes lost after reload — same root cause."
    )
    mod.reset()


# ── Generic NameError-at-import regression for ALL persisted services ─

def _persisted_services():
    """Return a sorted list of ``(svc_key, mod_name)`` pairs from
    ``ministack.app._state_map``.

    Evaluated by ``@pytest.mark.parametrize(...)`` at test collection
    time — `_state_map` is therefore imported when pytest collects this
    module, NOT lazily per test case. (Calling it inside the parametrize
    decorator means it runs once, at collection.)"""
    from ministack.app import _state_map
    return sorted(_state_map.items())


@pytest.mark.parametrize("svc_key,mod_name", _persisted_services())
def test_module_cold_import_with_typical_snapshot_does_not_log_restore_failure(
    svc_key, mod_name, caplog,
):
    """Generic regression for the NameError-at-import pattern that hit
    `ecs._attributes` (#492) and `acm._synthetic_pem` (#494).

    The bug shape: `restore_state(data)` references a module-level
    symbol declared further down the file. The import-time `try:
    load_state(...)` block calls `restore_state()` BEFORE Python
    evaluates the later definition, so the lookup NameErrors. The
    surrounding try/except logs `Failed to restore persisted state` and
    swallows the exception, so the module appears to import cleanly
    while ALL its persisted state silently disappears.

    The test:
      1. Captures the module's current `get_state()` snapshot (a
         non-empty dict-of-empty-dicts — important so `restore_state`
         doesn't early-return on truthy emptiness checks).
      2. Persists that to disk via the production `save_state` path.
      3. **Removes the module from `sys.modules` and re-imports it
         fresh** — `importlib.reload()` would NOT catch the bug
         because it merges new definitions into the existing
         namespace, leaving any late-declared symbol bound from the
         previous import.
      4. Asserts no WARNING+ log record mentioning "restore" / "failed"
         / "continuing fresh" was emitted during the cold import.

    Catches: unconditional symbol references in restore_state
    (ECS-style). Does NOT catch: conditional references inside loops
    over restored data when the data is empty (ACM-style needs
    populated state — see the per-service tests above).
    """
    import sys

    # Persistence is already enabled and STATE_DIR is already pointed at
    # a per-test tmp by the autouse `_enable_persistence` fixture.

    # Step 1+2: produce + persist a snapshot using the already-loaded
    # module (so we get a valid get_state() shape).
    mod = _module(mod_name)
    if hasattr(mod, "reset"):
        mod.reset()
    persistence.save_state(svc_key, mod.get_state())

    # Step 3: cold-import — wipe sys.modules and re-import.
    # importlib.reload() won't work because it merges into the
    # existing namespace; the late-declared symbol stays bound from
    # the prior import.
    full_name = f"ministack.services.{mod_name}"
    sys.modules.pop(full_name, None)

    caplog.clear()
    with caplog.at_level("WARNING"):
        mod = importlib.import_module(full_name)

    bad = [
        r for r in caplog.records
        if r.levelno >= 30  # WARNING+
        and any(needle in r.getMessage().lower()
                for needle in ("failed to restore", "restore failed",
                               "continuing fresh", "continuing with fresh"))
    ]
    if hasattr(mod, "reset"):
        mod.reset()

    assert not bad, (
        f"Cold import of `{mod_name}` (state-key `{svc_key}`) emitted "
        f"a restore-failure log:\n  "
        + "\n  ".join(r.getMessage() for r in bad)
        + "\n\nThis usually means `restore_state` references a "
        "module-level symbol that's declared further down the file. "
        "The import-time `try: load_state()` block runs before the "
        "later definition, so the symbol lookup NameErrors and the "
        "surrounding try/except swallows it. Hoist the symbol above "
        "the import-time `load_state` block (see ECS `_attributes` "
        "or ACM `_synthetic_pem` for the canonical fix)."
    )
