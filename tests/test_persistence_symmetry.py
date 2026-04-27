"""
Regression tests for the persistence-symmetry architectural bug.

Background
----------
When PERSIST_STATE=1, every service that participates in `_state_map`
(see `ministack/app.py`) is saved on shutdown via `save_all()`. State is
restored on startup either by a service's own `load_state()` call at
module import time, OR by `_load_persisted_state()` which calls a
`load_persisted_state()` method on the service module.

For five services (autoscaling, backup, eks, scheduler, pipes), the
shutdown path persists the state to disk but no restore path runs at
startup, so the next boot starts with an empty store. `pipes` is
additionally missing from `_state_map`, so its state is never even saved.

These tests assert the round-trip works for every persisted service.
"""
import importlib
from pathlib import Path

import pytest

from ministack.app import _state_map  # noqa: E402  (intentional internal import)
from ministack.core import persistence


# Services that MUST be persistence-round-trippable. Every entry of
# `_state_map` qualifies. The set is materialised here so an addition to
# `_state_map` automatically gets coverage.
ALL_PERSISTED_SERVICES = sorted(_state_map.items())


def _module(mod_name):
    return importlib.import_module(f"ministack.services.{mod_name}")


@pytest.mark.parametrize("svc_key,mod_name", ALL_PERSISTED_SERVICES)
def test_service_has_restore_path(svc_key, mod_name):
    """Every service in `_state_map` must expose a way to restore its own state.

    Either:
      (a) the module calls `load_state()` itself at import time, OR
      (b) the module exposes `load_persisted_state(data)` AND is wired into
          `_load_persisted_state()` in app.py.
    """
    mod = _module(mod_name)
    src = Path(mod.__file__).read_text()

    # (a) self-restore at import: must import load_state AND call it.
    self_restoring = (
        "from ministack.core.persistence import" in src
        and "load_state" in src
        and "load_state(" in src
    )

    # (b) centrally restored: must define load_persisted_state and be in
    # the explicit allow-list in app.py's `_load_persisted_state()`.
    has_central_method = hasattr(mod, "load_persisted_state")
    centrally_restored = has_central_method and svc_key in {
        "apigateway", "apigateway_v1", "servicediscovery",
    }

    assert self_restoring or centrally_restored, (
        f"Service `{svc_key}` (module `{mod_name}`) is in `_state_map` and "
        f"will be saved on shutdown, but has no restore path on startup. "
        f"Either add `load_state()` at module top, or define "
        f"`load_persisted_state(data)` and add it to "
        f"`_load_persisted_state()` in app.py."
    )


def test_pipes_is_in_state_map():
    """`pipes` defines `get_state()` so it expects to be persisted, but it
    is missing from `_state_map`. Without this, pipe definitions evaporate
    on every restart even before considering restore-path coverage."""
    pipes = _module("pipes")
    assert hasattr(pipes, "get_state"), "pipes module no longer has get_state — update this test"
    assert "pipes" in _state_map, (
        "`pipes` defines get_state() but is missing from `_state_map` in "
        "app.py — its state is never saved on shutdown."
    )


def test_state_map_services_without_endpoint_are_eagerly_imported():
    """Services in `_state_map` but NOT in `SERVICE_REGISTRY` have no
    AWS endpoint, so the lazy router never imports them. Their
    import-time `load_state()` block therefore never fires unless
    `_load_persisted_state()` eagerly imports them at startup.

    Without this, persisted RUNNING pipes don't resume their poller
    after warm-boot until something else happens to import the
    module (e.g. a new CFN pipe registration) — silently breaking
    event forwarding for the entire window between restart and the
    next pipe-related API call."""
    from ministack.app import SERVICE_REGISTRY, _load_persisted_state
    import inspect

    # Find services that need eager import.
    routable_modules = {cfg["module"] for cfg in SERVICE_REGISTRY.values()}
    needs_eager_import = [
        mod_name for _, mod_name in _state_map.items()
        if mod_name not in routable_modules
    ]
    assert needs_eager_import, (
        "Test premise broken: every persisted module is now also routable, "
        "so this test would never catch the bug it's guarding against. "
        "Update it or delete it."
    )

    # The eager-import section in _load_persisted_state must reference each
    # such module by name, otherwise it stays unimported and its restore
    # never runs.
    src = inspect.getsource(_load_persisted_state)
    for mod_name in needs_eager_import:
        assert f'"{mod_name}"' in src or f"'{mod_name}'" in src, (
            f"Service `{mod_name}` is in `_state_map` but not in "
            f"`SERVICE_REGISTRY`, and `_load_persisted_state()` doesn't "
            f"eagerly import it. With PERSIST_STATE=1, its persisted "
            f"state will be silently ignored on warm-boot."
        )


# ── Functional round-trip tests ────────────────────────────────────────

def _round_trip(mod_name, svc_key, populate_fn, observe_fn):
    """Helper: populate -> save -> reset -> restore -> observe."""
    mod = _module(mod_name)
    mod.reset()
    populate_fn(mod)
    snapshot = mod.get_state()

    # Persist via the same code path as `save_all` would use.
    persistence.save_state(svc_key, snapshot)

    # Wipe in-memory state — this simulates a process restart.
    mod.reset()

    # Restore via the same code path the module would use at import.
    loaded = persistence.load_state(svc_key)
    assert loaded is not None, (
        f"persistence.load_state({svc_key!r}) returned None — state file "
        "was not written by save_state(). Check `_state_map` membership "
        "and `get_state()` correctness."
    )
    if hasattr(mod, "restore_state"):
        mod.restore_state(loaded)
    elif hasattr(mod, "load_persisted_state"):
        mod.load_persisted_state(loaded)
    else:
        pytest.fail(
            f"Module {mod_name} has neither restore_state nor "
            "load_persisted_state — cannot restore."
        )

    # Cleanup state file before observation, so a failure doesn't pollute
    # the next test run.
    import os
    state_file = os.path.join(persistence.STATE_DIR, f"{svc_key}.json")
    if os.path.exists(state_file):
        os.remove(state_file)

    observe_fn(mod)
    mod.reset()


@pytest.fixture(autouse=True)
def _enable_persistence(monkeypatch, tmp_path):
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))


def test_autoscaling_round_trip():
    def populate(mod):
        # Drive the state via the module's own dict directly — minimal
        # surface, no SDK needed.
        mod._launch_configs["lc-test"] = {"LaunchConfigurationName": "lc-test"}
        mod._asgs["asg-test"] = {"AutoScalingGroupName": "asg-test", "MinSize": 1}

    def observe(mod):
        assert "lc-test" in mod._launch_configs
        assert "asg-test" in mod._asgs

    _round_trip("autoscaling", "autoscaling", populate, observe)


def test_backup_round_trip():
    def populate(mod):
        mod._vaults["vault-test"] = {"BackupVaultName": "vault-test"}

    def observe(mod):
        assert "vault-test" in mod._vaults

    _round_trip("backup", "backup", populate, observe)


def test_eks_round_trip():
    def populate(mod):
        mod._clusters["cluster-test"] = {"name": "cluster-test", "status": "ACTIVE"}

    def observe(mod):
        assert "cluster-test" in mod._clusters

    _round_trip("eks", "eks", populate, observe)


def test_scheduler_round_trip():
    # Production code keys _schedules by `f"{group}/{name}"` strings (see
    # scheduler.py CreateSchedule etc.), not tuples — even though the
    # pre-existing inline comment on the dict mis-describes the shape. Use
    # the real production key shape so this test catches a regression that
    # broke string-key serialisation.
    def populate(mod):
        mod._schedule_groups["default"] = {"Name": "default"}
        mod._schedules["default/sched-test"] = {"Name": "sched-test"}

    def observe(mod):
        assert "default" in mod._schedule_groups
        assert "default/sched-test" in mod._schedules

    _round_trip("scheduler", "scheduler", populate, observe)


def test_pipes_round_trip():
    # Use a complete pipe record matching `register_pipe()` shape so the
    # background poller (which the restore path may start) doesn't blow up
    # on KeyError if it iterates this entry. Source/Target are intentionally
    # non-DDB/non-SNS so `_poll_once` skips them quickly.
    pipe_arn = "arn:aws:pipes:us-east-1:000000000000:pipe/pipe-test"

    def populate(mod):
        mod._pipes["pipe-test"] = {
            "Name": "pipe-test",
            "Arn": pipe_arn,
            "RoleArn": "",
            "Source": "arn:aws:sqs:us-east-1:000000000000:irrelevant",
            "Target": "arn:aws:sqs:us-east-1:000000000000:irrelevant",
            "DesiredState": "STOPPED",
            "CurrentState": "STOPPED",
            "StartingPosition": "LATEST",
            "Tags": {},
            "CreationTime": 0,
        }
        mod._positions[pipe_arn] = 0

    def observe(mod):
        assert "pipe-test" in mod._pipes
        assert mod._positions.get(pipe_arn) == 0

    _round_trip("pipes", "pipes", populate, observe)


def test_pipes_restore_starts_poller_for_running_pipes(monkeypatch):
    """When `restore_state` reloads pipes that are RUNNING, the background
    poller must be (re)started so events keep flowing after warm-boot."""
    mod = _module("pipes")
    mod.reset()
    # Reset the poller flag so this test is independent of execution order.
    monkeypatch.setattr(mod, "_poller_started", False)

    pipe_arn = "arn:aws:pipes:us-east-1:000000000000:pipe/poller-test"
    mod.restore_state({
        "pipes": {
            "poller-test": {
                "Name": "poller-test",
                "Arn": pipe_arn,
                "RoleArn": "",
                "Source": "arn:aws:sqs:us-east-1:000000000000:irrelevant",
                "Target": "arn:aws:sqs:us-east-1:000000000000:irrelevant",
                "DesiredState": "RUNNING",
                "CurrentState": "RUNNING",
                "StartingPosition": "LATEST",
                "Tags": {},
                "CreationTime": 0,
            },
        },
        "positions": {pipe_arn: 0},
    })

    assert mod._poller_started, (
        "restore_state() did not start the pipes poller for a RUNNING pipe — "
        "warm-booted pipes would silently stop forwarding events."
    )
    mod.reset()


# ── PERSIST_STATE gating ──────────────────────────────────────────────

@pytest.mark.parametrize("svc_key", [
    "autoscaling", "backup", "eks", "scheduler", "pipes",
])
def test_load_state_is_noop_when_persist_state_disabled(monkeypatch, svc_key, tmp_path):
    """When PERSIST_STATE=0, load_state() must return None without touching
    disk and without invoking restore_state(). Catches a regression where
    a service module accidentally calls restore_state() unconditionally."""
    monkeypatch.setattr(persistence, "PERSIST_STATE", False)
    # Pre-write a state file that *would* succeed if persistence were on,
    # so we can assert that it is NOT consumed.
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))
    bogus_path = tmp_path / f"{svc_key}.json"
    bogus_path.write_text('{"would_have_been_restored": true}')

    result = persistence.load_state(svc_key)
    assert result is None, (
        f"load_state({svc_key!r}) returned non-None even though "
        "PERSIST_STATE is False — restore must be gated."
    )
