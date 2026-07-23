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


def test_account_region_scoped_dict_isolates_account_and_region():
    """Account+region scoped stores keep same-name resources independent."""
    from ministack.core.responses import (
        AccountRegionScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    original_account = get_account_id()
    original_region = get_region()

    try:
        store = AccountRegionScopedDict()
        set_request_account_id("111111111111")
        set_request_region("us-east-1")
        store["same-name"] = {"scope": "111/east"}

        set_request_region("us-west-2")
        store["same-name"] = {"scope": "111/west"}

        set_request_account_id("222222222222")
        set_request_region("us-east-1")
        store["same-name"] = {"scope": "222/east"}

        assert len(store) == 1
        assert store["same-name"] == {"scope": "222/east"}

        set_request_account_id("111111111111")
        assert store["same-name"] == {"scope": "111/east"}
        set_request_region("us-west-2")
        assert store["same-name"] == {"scope": "111/west"}
    finally:
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_account_region_scoped_dict_persistence_round_trip(monkeypatch, tmp_path):
    """Account+region scoped stores must survive the JSON persistence path."""
    from ministack.core.responses import (
        AccountRegionScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    original_account = get_account_id()
    original_region = get_region()
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))

    try:
        store = AccountRegionScopedDict()
        set_request_account_id("111111111111")
        set_request_region("us-east-1")
        store["same-name"] = {"region": "us-east-1"}
        store[("compound", "key")] = {"region": "us-east-1"}
        set_request_region("us-west-2")
        store["same-name"] = {"region": "us-west-2"}

        persistence.save_state("account-region-scoped", {"store": store})
        loaded = persistence.load_state("account-region-scoped")
        assert loaded is not None

        restored = loaded["store"]
        set_request_region("us-east-1")
        assert restored["same-name"] == {"region": "us-east-1"}
        assert restored[("compound", "key")] == {"region": "us-east-1"}
        set_request_region("us-west-2")
        assert restored["same-name"] == {"region": "us-west-2"}
    finally:
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_account_region_scoped_dict_adopts_legacy_values_to_arn_region():
    """Legacy account-scoped stores should migrate into the region in the value ARN."""
    from ministack.core.responses import (
        AccountRegionScopedDict,
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    original_account = get_account_id()
    original_region = get_region()

    try:
        legacy = AccountScopedDict()
        legacy._data[("111111111111", "west-sm")] = {
            "stateMachineArn": "arn:aws:states:us-west-2:111111111111:stateMachine:west-sm",
        }
        legacy._data[("111111111111", "no-arn")] = {"name": "no-arn"}

        restored = AccountRegionScopedDict()
        set_request_account_id("111111111111")
        set_request_region("us-east-1")
        restored.update(legacy)

        assert restored.get_scoped("111111111111", "us-west-2", "west-sm") == {
            "stateMachineArn": "arn:aws:states:us-west-2:111111111111:stateMachine:west-sm",
        }
        assert restored.get_scoped("111111111111", "us-east-1", "west-sm") is None
        assert restored.get_scoped("111111111111", "us-east-1", "no-arn") == {"name": "no-arn"}
    finally:
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_account_region_scoped_dict_update_preserves_tuple_resource_keys():
    """Plain dict updates should treat tuple keys as normal resource keys."""
    from ministack.core.responses import (
        AccountRegionScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    original_account = get_account_id()
    original_region = get_region()

    try:
        store = AccountRegionScopedDict()
        set_request_account_id("111111111111")
        set_request_region("us-east-1")
        store.update({("bucket", "object-key"): {"value": "east"}})

        assert store[("bucket", "object-key")] == {"value": "east"}
        assert store.get_scoped("111111111111", "us-east-1", ("bucket", "object-key")) == {"value": "east"}
        assert store.get_scoped("bucket", "us-east-1", "object-key") is None
    finally:
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_account_region_scoped_dict_update_adopts_plain_values_to_arn_region():
    """Plain legacy dict restores should use the resource ARN region when present."""
    from ministack.core.responses import (
        AccountRegionScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    original_account = get_account_id()
    original_region = get_region()

    try:
        store = AccountRegionScopedDict()
        set_request_account_id("111111111111")
        set_request_region("us-east-1")
        store.update({
            "west-sm": {
                "stateMachineArn": "arn:aws:states:us-west-2:111111111111:stateMachine:west-sm",
            },
        })

        assert store.get_scoped("111111111111", "us-west-2", "west-sm") == {
            "stateMachineArn": "arn:aws:states:us-west-2:111111111111:stateMachine:west-sm",
        }
        assert store.get_scoped("111111111111", "us-east-1", "west-sm") is None
    finally:
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_account_region_scoped_dict_update_prefers_key_arn_region():
    """Legacy ARN-keyed stores should not be scoped by unrelated value ARNs."""
    from ministack.core.responses import (
        AccountRegionScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    original_account = get_account_id()
    original_region = get_region()
    east_arn = "arn:aws:states:us-east-1:111111111111:stateMachine:east-sm"
    west_role_arn = "arn:aws:iam:us-west-2:111111111111:role/west-role"

    try:
        store = AccountRegionScopedDict()
        set_request_account_id("111111111111")
        set_request_region("us-west-2")
        store.update({east_arn: {"RoleArn": west_role_arn}})

        assert store.get_scoped("111111111111", "us-east-1", east_arn) == {"RoleArn": west_role_arn}
        assert store.get_scoped("111111111111", "us-west-2", east_arn) is None
    finally:
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_account_scoped_dict_helpers_remain_account_only():
    """Scoped helper methods on AccountScopedDict continue to ignore region."""
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        set_request_account_id,
    )

    original_account = get_account_id()

    try:
        store = AccountScopedDict()
        store.set_scoped("111111111111", "us-west-2", "same-name", {"value": "helper"})
        set_request_account_id("111111111111")

        assert store["same-name"] == {"value": "helper"}
        store["normal"] = {"value": "normal"}
        assert store.get_scoped("111111111111", "us-east-1", "normal") == {"value": "normal"}
        assert store.contains_scoped("111111111111", "us-west-2", "normal")
        assert store.pop_scoped("111111111111", "us-west-2", "normal") == {"value": "normal"}
        assert "normal" not in store
    finally:
        set_request_account_id(original_account)


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
    import inspect

    from ministack.app import SERVICE_REGISTRY, _load_persisted_state

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


def test_save_dict_includes_sibling_imported_modules():
    """Regression for #704. `appsync_events` is reached only via sibling
    import from `appsync.py` for REST traffic — the routed handler for
    `appsync-events` never fires (Event API traffic arrives under the
    `appsync` credential scope). Same shape: `apigateway_v1` is reached
    via sibling import from `apigateway.py`. If the shutdown save loop
    only consults `_loaded_modules` (populated by `_get_module`), those
    sibling-imported modules are silently skipped and their state is
    dropped. The fallback through `sys.modules` is the fix."""
    import sys as _sys

    from ministack.app import _build_persistence_save_dict, _loaded_modules

    # Force-import appsync_events the way appsync.py does it — a plain
    # sibling import that bypasses `_get_module` and therefore does NOT
    # populate `_loaded_modules`.
    import ministack.services.appsync_events  # noqa: F401

    # Simulate the bug condition: module is in sys.modules but absent
    # from _loaded_modules.
    saved = _loaded_modules.pop("appsync_events", None)
    try:
        assert "ministack.services.appsync_events" in _sys.modules, (
            "test premise broken — module isn't in sys.modules"
        )
        assert "appsync_events" not in _loaded_modules, (
            "test premise broken — module is still in _loaded_modules"
        )

        save_dict = _build_persistence_save_dict()

        assert "appsync_events" in save_dict, (
            "shutdown save loop dropped appsync_events even though the "
            "module was imported via a sibling import. The sys.modules "
            "fallback in `_build_persistence_save_dict` is missing or broken."
        )
        # The value must be the bound get_state method, not the result.
        assert callable(save_dict["appsync_events"]), (
            "save_dict should map to a callable (get_state method ref) — "
            "save_all invokes it. Got %r" % (save_dict["appsync_events"],)
        )
    finally:
        if saved is not None:
            _loaded_modules["appsync_events"] = saved


def test_save_dict_skips_modules_never_imported():
    """The sys.modules fallback must NOT save state for modules that
    were never imported at all — there's no state to capture and any
    `get_state()` call on a non-imported module would attribute-error.
    Defensive guard: ensure the fallback path's `hasattr` check works."""
    import sys as _sys

    from ministack.app import _build_persistence_save_dict, _loaded_modules, _state_map

    # Pick any persisted module and ensure it's truly absent from both
    # `_loaded_modules` and `sys.modules`. `cur` is an obscure one that
    # most test sessions won't have touched.
    target = "ecs_metadata"  # not in _state_map → guaranteed absent from save_dict
    assert target not in {v for v in _state_map.values()}, (
        "test premise broken — pick a module not in _state_map"
    )

    # Even after the fallback path runs, ecs_metadata must not appear.
    save_dict = _build_persistence_save_dict()
    assert "ecs_metadata" not in save_dict, (
        "save_dict picked up a module that isn't even in _state_map — "
        "the loop's key-membership check is broken."
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
    from ministack.core.responses import get_region, set_request_region

    original_region = get_region()

    def populate(mod):
        set_request_region("us-east-1")
        mod._schedule_groups["default"] = {
            "Arn": "arn:aws:scheduler:us-east-1:000000000000:schedule-group/default",
            "Name": "default",
        }
        mod._schedules["default/sched-test"] = {
            "Arn": "arn:aws:scheduler:us-east-1:000000000000:schedule/default/sched-test",
            "Name": "sched-test",
            "ScheduleExpression": "rate(1 hour)",
        }
        set_request_region("us-west-2")
        mod._schedule_groups["default"] = {
            "Arn": "arn:aws:scheduler:us-west-2:000000000000:schedule-group/default",
            "Name": "default",
        }
        mod._schedules["default/sched-test"] = {
            "Arn": "arn:aws:scheduler:us-west-2:000000000000:schedule/default/sched-test",
            "Name": "sched-test",
            "ScheduleExpression": "rate(2 hours)",
        }

    def observe(mod):
        set_request_region("us-east-1")
        assert mod._schedules["default/sched-test"]["ScheduleExpression"] == "rate(1 hour)"
        set_request_region("us-west-2")
        assert mod._schedules["default/sched-test"]["ScheduleExpression"] == "rate(2 hours)"

    try:
        _round_trip("scheduler", "scheduler", populate, observe)
    finally:
        set_request_region(original_region)


def test_scheduler_legacy_account_scoped_state_uses_resource_arn_region():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import scheduler as mod

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    region = "us-west-2"
    group = "legacy-group"
    schedule_key = f"{group}/legacy-schedule"
    legacy_groups = AccountScopedDict()
    legacy_groups._data[(account_id, group)] = {
        "Arn": f"arn:aws:scheduler:{region}:{account_id}:schedule-group/{group}",
        "Name": group,
    }
    legacy_schedules = AccountScopedDict()
    legacy_schedules._data[(account_id, schedule_key)] = {
        "Arn": f"arn:aws:scheduler:{region}:{account_id}:schedule/{schedule_key}",
        "Name": "legacy-schedule",
        "GroupName": group,
    }

    mod.reset()
    try:
        set_request_account_id(account_id)
        set_request_region("us-east-1")
        mod.restore_state(
            {"schedule_groups": legacy_groups, "schedules": legacy_schedules}
        )

        assert mod._schedule_groups.get_scoped(account_id, "us-east-1", group) is None
        assert mod._schedules.get_scoped(account_id, "us-east-1", schedule_key) is None
        assert mod._schedule_groups.get_scoped(account_id, region, group)["Name"] == group
        assert mod._schedules.get_scoped(account_id, region, schedule_key)["Name"] == (
            "legacy-schedule"
        )
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


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


def test_lambda_esm_eager_loaded_at_boot_when_persisted(monkeypatch):
    """#889: persisted SQS event source mappings must resume polling after a
    warm restart even under pure-SQS traffic. The ESM poller starts from
    lambda_svc's import-time restore (`_ensure_poller`), and lambda_svc is
    otherwise imported lazily only on a Lambda request — so `_load_persisted_state`
    must eager-import it at boot when ESMs are persisted, else the restored
    mapping sits Enabled-but-unpolled and messages pile up. A restore_state-level
    test does NOT catch this: the bug is the module never being imported."""
    import ministack.app as app
    monkeypatch.setattr(app, "load_state",
                        lambda key: {"esms": {"uuid-1": {"Enabled": True}}} if key == "lambda" else None)
    requested = []
    real = app._get_module
    monkeypatch.setattr(app, "_get_module", lambda n: (requested.append(n), real(n))[1])
    app._load_persisted_state()
    assert "lambda_svc" in requested, (
        "#889: persisted ESMs present but lambda_svc was not eager-imported at "
        "boot — the SQS poller never starts under pure-SQS traffic after restart."
    )


def test_lambda_not_eager_loaded_without_persisted_esms(monkeypatch):
    """Narrow: no persisted ESMs → don't pay the lambda_svc cold-start at boot."""
    import ministack.app as app
    monkeypatch.setattr(app, "load_state",
                        lambda key: {"esms": {}} if key == "lambda" else None)
    requested = []
    real = app._get_module
    monkeypatch.setattr(app, "_get_module", lambda n: (requested.append(n), real(n))[1])
    app._load_persisted_state()
    assert "lambda_svc" not in requested


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


# ========== from test_state_dict_persistence.py ==========
# Companion to the symmetry tests above. Symmetry tests check that
# *every service* participates in get_state/restore_state. These tests
# check that within services, *every AccountScopedDict mutated by the
# public API* is captured — the 'dict dropped from get_state' bug pattern.
import importlib

import pytest

from ministack.core import persistence


def _get_module(mod_name):
    return importlib.import_module(f"ministack.services.{mod_name}")


@pytest.fixture(autouse=True)
def _enable_persistence_dict(monkeypatch, tmp_path):
    """Force PERSIST_STATE on and point STATE_DIR at a tmp dir for the
    duration of each test so save_state / load_state actually write and
    read JSON files instead of short-circuiting."""
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))


def _round_trip_dict(mod, svc_key):
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
    mod = _get_module("secretsmanager")
    mod.reset()
    arn = "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret-AbCdEf"
    mod._resource_policies[arn] = '{"Version":"2012-10-17","Statement":[]}'

    _round_trip_dict(mod, "secretsmanager")

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
    mod = _get_module("kinesis")
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

    _round_trip_dict(mod, "kinesis")

    assert consumer_arn in mod._consumers, (
        "Kinesis consumer lost across get_state → restore_state — "
        "_consumers must be in both."
    )
    mod.reset()


def test_kinesis_shard_iterators_survive_warm_boot_in_original_scope():
    """Live shard iterators must round-trip through JSON persistence without
    becoming visible to other accounts or regions."""
    import json
    import time

    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _get_module("kinesis")
    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    region = "us-west-2"
    stream_name = "warm-boot-stream"
    stream_arn = f"arn:aws:kinesis:{region}:{account_id}:stream/{stream_name}"
    shard_id = "shardId-000000000000"
    token = "live-iterator-token"
    now = time.time()

    mod.reset()
    try:
        set_request_account_id(account_id)
        set_request_region(region)
        mod._streams[stream_name] = {
            "StreamName": stream_name,
            "StreamARN": stream_arn,
            "StreamStatus": "ACTIVE",
            "StreamModeDetails": {"StreamMode": "PROVISIONED"},
            "RetentionPeriodHours": 24,
            "shards": {
                shard_id: {
                    "records": [{
                        "SequenceNumber": "1",
                        "ApproximateArrivalTimestamp": now,
                        "Data": b"warm-boot-record",
                        "PartitionKey": "pk1",
                    }],
                    "starting_hash_key": "0",
                    "ending_hash_key": str(2**128 - 1),
                    "starting_sequence_number": "1",
                    "parent_shard_id": None,
                    "adjacent_parent_shard_id": None,
                },
            },
            "tags": {},
            "CreationTimestamp": now,
            "EncryptionType": "NONE",
        }
        mod._shard_iterators[token] = {
            "stream": stream_name,
            "stream_arn": stream_arn,
            "shard_id": shard_id,
            "position": 0,
            "created_at": now,
        }

        _round_trip_dict(mod, "kinesis")

        assert token in mod._shard_iterators
        status, _, body = mod._get_records({"ShardIterator": token, "Limit": 1})
        assert status == 200
        payload = json.loads(body)
        assert payload["Records"][0]["PartitionKey"] == "pk1"
        assert payload["NextShardIterator"] in mod._shard_iterators

        set_request_region("us-east-1")
        assert token not in mod._shard_iterators
        status, _, body = mod._get_records({"ShardIterator": token})
        assert status == 400
        assert json.loads(body)["__type"] == "ExpiredIteratorException"

        set_request_account_id("222222222222")
        set_request_region(region)
        assert token not in mod._shard_iterators
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


# ── ecs._attributes ────────────────────────────────────────────────────

def test_ecs_attributes_survive_warm_boot():
    """`PutAttributes` writes to `_attributes`. Lost on restart without
    persistence wiring."""
    mod = _get_module("ecs")
    mod.reset()
    mod._attributes["i-deadbeef:my-attr"] = {
        "name": "my-attr",
        "value": "v1",
        "targetType": "container-instance",
        "targetId": "i-deadbeef",
    }

    _round_trip_dict(mod, "ecs")

    assert "i-deadbeef:my-attr" in mod._attributes, (
        "ECS attribute lost across get_state → restore_state — "
        "_attributes must be in both."
    )
    mod.reset()


def test_ecs_region_scoped_state_survives_warm_boot(monkeypatch):
    """ECS v2 persistence retains every resource's account and region scope."""
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _get_module("ecs")
    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    cluster_name = "warm-boot-cluster"
    family = "warm-boot-family"

    monkeypatch.setattr(mod, "_get_docker", lambda: None)
    mod.reset()
    try:
        set_request_account_id(account_id)
        for region, revision in (("us-east-1", 2), ("us-west-2", 1)):
            set_request_region(region)
            cluster_arn = f"arn:aws:ecs:{region}:{account_id}:cluster/{cluster_name}"
            td_key = f"{family}:{revision}"
            td_arn = f"arn:aws:ecs:{region}:{account_id}:task-definition/{td_key}"
            service_key = f"{cluster_name}/warm-boot-service"
            service_arn = (
                f"arn:aws:ecs:{region}:{account_id}:service/"
                f"{cluster_name}/warm-boot-service"
            )
            task_arn = f"arn:aws:ecs:{region}:{account_id}:task/{cluster_name}/{region}"
            cp_arn = f"arn:aws:ecs:{region}:{account_id}:capacity-provider/warm-boot-cp"

            mod._clusters[cluster_name] = {
                "clusterArn": cluster_arn,
                "clusterName": cluster_name,
                "status": "ACTIVE",
            }
            mod._task_defs[td_key] = {
                "taskDefinitionArn": td_arn,
                "family": family,
                "revision": revision,
            }
            mod._task_def_latest[family] = revision
            mod._services[service_key] = {
                "serviceArn": service_arn,
                "serviceName": "warm-boot-service",
                "clusterArn": cluster_arn,
            }
            mod._tasks[task_arn] = {
                "taskArn": task_arn,
                "clusterArn": cluster_arn,
                "lastStatus": "RUNNING",
                "_docker_ids": [f"stale-{region}"],
            }
            mod._capacity_providers["warm-boot-cp"] = {
                "capacityProviderArn": cp_arn,
                "name": "warm-boot-cp",
            }
            mod._attributes["i-warm:zone"] = {
                "targetId": "i-warm",
                "name": "zone",
                "value": region,
            }
            mod._account_settings["containerInsights"] = region
            mod._tags[cluster_arn] = [{"key": "region", "value": region}]

        _round_trip_dict(mod, "ecs")

        for region, revision in (("us-east-1", 2), ("us-west-2", 1)):
            cluster_arn = f"arn:aws:ecs:{region}:{account_id}:cluster/{cluster_name}"
            task_arn = f"arn:aws:ecs:{region}:{account_id}:task/{cluster_name}/{region}"
            assert mod._clusters.get_scoped(account_id, region, cluster_name)["clusterArn"] == (
                cluster_arn
            )
            assert mod._task_def_latest.get_scoped(account_id, region, family) == revision
            assert mod._account_settings.get_scoped(
                account_id, region, "containerInsights"
            ) == region
            assert mod._attributes.get_scoped(account_id, region, "i-warm:zone")["value"] == (
                region
            )
            restored_task = mod._tasks.get_scoped(account_id, region, task_arn)
            assert restored_task["lastStatus"] == "STOPPED"
            assert restored_task["_docker_ids"] == []
            assert mod._tags.get_scoped(account_id, region, cluster_arn) == [
                {"key": "region", "value": region}
            ]
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_ecs_legacy_account_scoped_state_migrates_by_arn(monkeypatch, tmp_path):
    """Legacy ECS state adopts ARN regions and boot-scopes ARN-less stores."""
    import json as _json

    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _get_module("ecs")
    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    boot_region = "us-east-1"
    resource_region = "us-west-2"
    cluster_name = "legacy-cluster"
    family = "legacy-family"
    cluster_arn = f"arn:aws:ecs:{resource_region}:{account_id}:cluster/{cluster_name}"
    service_key = f"{cluster_name}/legacy-service"
    service_arn = (
        f"arn:aws:ecs:{resource_region}:{account_id}:service/"
        f"{cluster_name}/legacy-service"
    )
    task_arn = (
        f"arn:aws:ecs:{resource_region}:{account_id}:task/"
        f"{cluster_name}/legacy-task"
    )

    def scoped(key, value):
        store = AccountScopedDict()
        store._data[(account_id, key)] = value
        return store

    legacy_payload = {
        "clusters": scoped(cluster_name, {
            "clusterArn": cluster_arn,
            "clusterName": cluster_name,
            "status": "ACTIVE",
        }),
        "task_defs": scoped(family + ":3", {
            "taskDefinitionArn": (
                f"arn:aws:ecs:{resource_region}:{account_id}:"
                f"task-definition/{family}:3"
            ),
            "family": family,
            "revision": 3,
        }),
        "task_def_latest": scoped(family, 3),
        "services": scoped(service_key, {
            "serviceArn": service_arn,
            "serviceName": "legacy-service",
            "clusterArn": cluster_arn,
        }),
        "tasks": scoped(task_arn, {
            "taskArn": task_arn,
            "clusterArn": cluster_arn,
            "lastStatus": "RUNNING",
            "_docker_ids": ["stale-container"],
        }),
        "account_settings": scoped("containerInsights", "enabled"),
        "attributes": scoped("i-legacy:zone", {
            "targetId": "i-legacy",
            "name": "zone",
            "value": "legacy",
        }),
    }

    monkeypatch.setattr(mod, "_get_docker", lambda: None)
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))
    mod.reset()
    try:
        set_request_account_id(account_id)
        set_request_region(boot_region)
        with open(tmp_path / "ecs.json", "w") as f:
            _json.dump(legacy_payload, f, default=persistence._json_default)

        loaded = persistence.load_state("ecs")
        assert isinstance(loaded["tasks"], AccountScopedDict)
        mod.restore_state(loaded)

        assert mod._clusters.get_scoped(account_id, resource_region, cluster_name)[
            "clusterArn"
        ] == cluster_arn
        assert mod._services.get_scoped(account_id, resource_region, service_key)[
            "serviceArn"
        ] == service_arn
        restored_task = mod._tasks.get_scoped(account_id, resource_region, task_arn)
        assert restored_task["lastStatus"] == "STOPPED"
        assert restored_task["_docker_ids"] == []

        assert mod._task_def_latest.get_scoped(account_id, boot_region, family) is None
        assert mod._task_def_latest.get_scoped(account_id, resource_region, family) == 3

        set_request_region(resource_region)
        described = _json.loads(
            mod._describe_task_definition({"taskDefinition": family})[2]
        )["taskDefinition"]
        assert described["taskDefinitionArn"].endswith(
            f"task-definition/{family}:3"
        )

        registered = _json.loads(mod._register_task_definition({
            "family": family,
            "containerDefinitions": [{"name": "app", "image": "busybox"}],
        })[2])["taskDefinition"]
        assert registered["revision"] == 4
        assert mod._task_defs.get_scoped(
            account_id, resource_region, family + ":3"
        )["taskDefinitionArn"] == described["taskDefinitionArn"]
        assert mod._task_defs.get_scoped(
            account_id, resource_region, family + ":4"
        )["taskDefinitionArn"] == registered["taskDefinitionArn"]

        # Other ARN-less legacy stores intentionally adopt the boot region.
        assert mod._account_settings.get_scoped(
            account_id, boot_region, "containerInsights"
        ) == "enabled"
        assert mod._attributes.get_scoped(account_id, boot_region, "i-legacy:zone")[
            "value"
        ] == "legacy"
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_ecs_legacy_task_revision_migration_reconstructs_each_region(monkeypatch):
    """Legacy shared counters do not reset revisions in other ARN regions."""
    import json as _json

    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _get_module("ecs")
    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    boot_region = "us-east-1"
    other_region = "us-west-2"
    family = "multi-region-legacy-family"

    legacy_task_defs = AccountScopedDict()
    for region, revision, marker in (
        (boot_region, 1, "east-original"),
        (other_region, 2, "west-original"),
    ):
        key = f"{family}:{revision}"
        legacy_task_defs._data[(account_id, key)] = {
            "taskDefinitionArn": (
                f"arn:aws:ecs:{region}:{account_id}:task-definition/{key}"
            ),
            "family": family,
            "revision": revision,
            "status": "ACTIVE",
            "marker": marker,
        }

    legacy_latest = AccountScopedDict()
    legacy_latest._data[(account_id, family)] = 2

    monkeypatch.setattr(mod, "_get_docker", lambda: None)
    mod.reset()
    try:
        set_request_account_id(account_id)
        set_request_region(boot_region)
        mod.restore_state({
            "task_defs": legacy_task_defs,
            "task_def_latest": legacy_latest,
        })

        assert mod._task_def_latest.get_scoped(
            account_id, boot_region, family
        ) == 1
        assert mod._task_def_latest.get_scoped(
            account_id, other_region, family
        ) == 2

        for region, restored_revision, next_revision in (
            (boot_region, 1, 2),
            (other_region, 2, 3),
        ):
            set_request_region(region)
            status, _, body = mod._describe_task_definition({
                "taskDefinition": family,
            })
            assert status == 200
            described = _json.loads(body)["taskDefinition"]
            assert described["revision"] == restored_revision
            assert described["taskDefinitionArn"].startswith(
                f"arn:aws:ecs:{region}:{account_id}:"
            )

            status, _, body = mod._register_task_definition({
                "family": family,
                "containerDefinitions": [{"name": "app", "image": "busybox"}],
            })
            assert status == 200
            assert _json.loads(body)["taskDefinition"]["revision"] == next_revision

        assert mod._task_defs.get_scoped(
            account_id, boot_region, f"{family}:1"
        )["marker"] == "east-original"
        assert mod._task_defs.get_scoped(
            account_id, other_region, f"{family}:2"
        )["marker"] == "west-original"
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_ecs_plain_dict_tasks_migrate_to_arn_region(monkeypatch):
    """Oldest bare-dict task state restores under the task ARN region."""
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _get_module("ecs")
    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    boot_region = "us-east-1"
    resource_region = "us-west-2"
    task_arn = f"arn:aws:ecs:{resource_region}:{account_id}:task/legacy-cluster/legacy-task"

    monkeypatch.setattr(mod, "_get_docker", lambda: None)
    mod.reset()
    try:
        set_request_account_id(account_id)
        set_request_region(boot_region)
        mod.restore_state(
            {
                "tasks": {
                    task_arn: {
                        "taskArn": task_arn,
                        "lastStatus": "RUNNING",
                        "_docker_ids": ["stale-container"],
                    },
                },
            }
        )

        restored_task = mod._tasks.get_scoped(account_id, resource_region, task_arn)
        assert restored_task["lastStatus"] == "STOPPED"
        assert restored_task["_docker_ids"] == []
        assert mod._tasks.get_scoped(account_id, boot_region, task_arn) is None
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


# ── sns._platform_applications + sns._platform_endpoints ──────────────

def test_sns_platform_applications_survive_warm_boot():
    """`CreatePlatformApplication` writes to `_platform_applications`.
    Mobile push topology is lost on restart without persistence wiring."""
    mod = _get_module("sns")
    mod.reset()
    app_arn = "arn:aws:sns:us-east-1:000000000000:app/GCM/MyApp"
    mod._platform_applications[app_arn] = {
        "PlatformApplicationArn": app_arn,
        "Attributes": {"Platform": "GCM"},
    }

    _round_trip_dict(mod, "sns")

    assert app_arn in mod._platform_applications, (
        "SNS platform application lost across get_state → restore_state — "
        "_platform_applications must be in both."
    )
    mod.reset()


def test_sns_platform_endpoints_survive_warm_boot():
    """`CreatePlatformEndpoint` writes to `_platform_endpoints`."""
    mod = _get_module("sns")
    mod.reset()
    ep_arn = "arn:aws:sns:us-east-1:000000000000:endpoint/GCM/MyApp/abc"
    mod._platform_endpoints[ep_arn] = {
        "EndpointArn": ep_arn,
        "Token": "device-token-xyz",
        "Enabled": "true",
    }

    _round_trip_dict(mod, "sns")

    assert ep_arn in mod._platform_endpoints, (
        "SNS platform endpoint lost across get_state → restore_state — "
        "_platform_endpoints must be in both."
    )
    mod.reset()


def test_sns_region_scoped_stores_survive_warm_boot_in_original_scope():
    """SNS topic, subscription, application, and endpoint stores stay scoped
    after the real JSON persistence path."""
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _get_module("sns")
    mod.reset()
    original_account = get_account_id()
    original_region = get_region()
    try:
        set_request_account_id("111111111111")
        set_request_region("us-west-2")

        topic_arn = "arn:aws:sns:us-west-2:111111111111:persisted-topic"
        sub_arn = f"{topic_arn}:sub-1"
        app_arn = "arn:aws:sns:us-west-2:111111111111:app/GCM/PersistedApp"
        endpoint_arn = f"{app_arn}/endpoint-1"
        subscription = {
            "arn": sub_arn,
            "protocol": "email",
            "endpoint": "persisted@example.com",
            "confirmed": True,
            "topic_arn": topic_arn,
            "owner": "111111111111",
            "attributes": {"SubscriptionArn": sub_arn, "TopicArn": topic_arn},
        }
        mod._topics[topic_arn] = {
            "name": "persisted-topic",
            "arn": topic_arn,
            "attributes": {"TopicArn": topic_arn, "Owner": "111111111111"},
            "subscriptions": [subscription],
            "messages": [],
            "tags": {},
        }
        mod._sub_arn_to_topic[sub_arn] = topic_arn
        mod._platform_applications[app_arn] = {
            "arn": app_arn,
            "name": "PersistedApp",
            "platform": "GCM",
            "attributes": {},
        }
        mod._platform_endpoints[endpoint_arn] = {
            "arn": endpoint_arn,
            "application_arn": app_arn,
            "attributes": {"Token": "persisted-token", "Enabled": "true"},
        }

        _round_trip_dict(mod, "sns")

        assert mod._topics[topic_arn]["subscriptions"][0]["arn"] == sub_arn
        assert mod._sub_arn_to_topic[sub_arn] == topic_arn
        assert mod._platform_applications[app_arn]["arn"] == app_arn
        assert mod._platform_endpoints[endpoint_arn]["attributes"]["Token"] == "persisted-token"

        set_request_region("us-east-1")
        assert mod._topics.get(topic_arn) is None
        assert mod._sub_arn_to_topic.get(sub_arn) is None
        assert mod._platform_applications.get(app_arn) is None
        assert mod._platform_endpoints.get(endpoint_arn) is None

        set_request_account_id("222222222222")
        set_request_region("us-west-2")
        assert mod._topics.get(topic_arn) is None
        assert mod._sub_arn_to_topic.get(sub_arn) is None
        assert mod._platform_applications.get(app_arn) is None
        assert mod._platform_endpoints.get(endpoint_arn) is None
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_eventbridge_region_scoped_stores_survive_warm_boot_in_original_scope():
    """EventBridge regional stores remain in their original account/region
    after the real JSON persistence path."""
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _get_module("eventbridge")
    mod.reset()
    original_account = get_account_id()
    original_region = get_region()
    try:
        set_request_account_id("111111111111")
        set_request_region("us-west-2")

        now = mod._now_ts()
        bus_name = "persisted-bus"
        rule_name = "persisted-rule"
        archive_name = "persisted-archive"
        replay_name = "persisted-replay"
        endpoint_name = "persisted-endpoint"
        connection_name = "persisted-connection"
        api_destination_name = "persisted-api"
        partner_name = "persisted-partner"
        bus_arn = f"arn:aws:events:us-west-2:111111111111:event-bus/{bus_name}"
        rule_key = mod._rule_key(rule_name, bus_name)
        rule_arn = f"arn:aws:events:us-west-2:111111111111:rule/{bus_name}/{rule_name}"
        archive_arn = f"arn:aws:events:us-west-2:111111111111:archive/{archive_name}"
        replay_arn = f"arn:aws:events:us-west-2:111111111111:replay/{replay_name}"
        endpoint_arn = f"arn:aws:events:us-west-2:111111111111:endpoint/{endpoint_name}"
        connection_arn = f"arn:aws:events:us-west-2:111111111111:connection/{connection_name}"
        api_destination_arn = (
            f"arn:aws:events:us-west-2:111111111111:api-destination/{api_destination_name}"
        )
        partner_arn = f"arn:aws:events:us-west-2:222222222222:event-source/{partner_name}"

        mod._event_buses[bus_name] = {
            "Name": bus_name,
            "Arn": bus_arn,
            "CreationTime": now,
            "LastModifiedTime": now,
        }
        mod._rules[rule_key] = {
            "Name": rule_name,
            "Arn": rule_arn,
            "EventBusName": bus_name,
            "ScheduleExpression": "rate(5 minutes)",
            "State": "ENABLED",
            "CreationTime": now,
        }
        mod._targets[rule_key] = [
            {
                "Id": "persisted-target",
                "Arn": "arn:aws:sqs:us-west-2:111111111111:persisted-queue",
            }
        ]
        mod._tags[rule_arn] = {"env": "test"}
        mod._archives[archive_name] = {
            "ArchiveName": archive_name,
            "ArchiveArn": archive_arn,
            "EventSourceArn": bus_arn,
            "State": "ENABLED",
            "CreationTime": now,
            "EventCount": 0,
            "Events": [],
        }
        mod._replays[replay_name] = {
            "ReplayName": replay_name,
            "ReplayArn": replay_arn,
            "EventSourceArn": archive_arn,
            "Destination": {"Arn": bus_arn},
            "State": "COMPLETED",
            "ReplayStartTime": now,
            "ReplayEndTime": now,
        }
        mod._endpoints[endpoint_name] = {
            "Name": endpoint_name,
            "Arn": endpoint_arn,
            "EndpointUrl": f"https://{endpoint_name}.global-events.us-west-2.amazonaws.com",
            "State": "ACTIVE",
            "CreationTime": now,
            "LastModifiedTime": now,
        }
        mod._event_bus_policies[bus_name] = {
            "Version": "2012-10-17",
            "Statement": [{"Sid": "Allow", "Resource": bus_arn}],
        }
        mod._connections[connection_name] = {
            "Name": connection_name,
            "ConnectionArn": connection_arn,
            "ConnectionState": "AUTHORIZED",
            "AuthorizationType": "API_KEY",
            "CreationTime": now,
            "LastModifiedTime": now,
        }
        mod._api_destinations[api_destination_name] = {
            "Name": api_destination_name,
            "ApiDestinationArn": api_destination_arn,
            "ApiDestinationState": "ACTIVE",
            "ConnectionArn": connection_arn,
            "InvocationEndpoint": "https://example.com",
            "HttpMethod": "POST",
            "CreationTime": now,
            "LastModifiedTime": now,
        }
        mod._partner_event_sources[mod._partner_key("222222222222", partner_name)] = {
            "Name": partner_name,
            "Account": "222222222222",
            "EventSourceArn": partner_arn,
        }

        _round_trip_dict(mod, "eventbridge")

        assert mod._event_buses[bus_name]["Arn"] == bus_arn
        assert mod._rules[rule_key]["Arn"] == rule_arn
        assert mod._targets[rule_key][0]["Id"] == "persisted-target"
        assert mod._tags[rule_arn]["env"] == "test"
        assert mod._archives[archive_name]["ArchiveArn"] == archive_arn
        assert mod._replays[replay_name]["ReplayArn"] == replay_arn
        assert mod._endpoints[endpoint_name]["Arn"] == endpoint_arn
        assert mod._event_bus_policies[bus_name]["Statement"][0]["Resource"] == bus_arn
        assert mod._connections[connection_name]["ConnectionArn"] == connection_arn
        assert mod._api_destinations[api_destination_name]["ApiDestinationArn"] == api_destination_arn
        partner_key = mod._partner_key("222222222222", partner_name)
        assert mod._partner_event_sources[partner_key]["EventSourceArn"] == partner_arn

        set_request_region("us-east-1")
        assert mod._event_buses.get(bus_name) is None
        assert mod._rules.get(rule_key) is None
        assert mod._targets.get(rule_key) is None
        assert mod._tags.get(rule_arn) is None
        assert mod._archives.get(archive_name) is None
        assert mod._replays.get(replay_name) is None
        assert mod._endpoints.get(endpoint_name) is None
        assert mod._event_bus_policies.get(bus_name) is None
        assert mod._connections.get(connection_name) is None
        assert mod._api_destinations.get(api_destination_name) is None
        assert mod._partner_event_sources.get(partner_key) is None

        set_request_account_id("222222222222")
        set_request_region("us-west-2")
        assert mod._event_buses.get(bus_name) is None
        assert mod._rules.get(rule_key) is None
        assert mod._targets.get(rule_key) is None
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_eventbridge_legacy_account_scoped_targets_restore_to_rule_region():
    """Legacy target stores are scoped to the EventBridge rule region, not the
    target ARN region, when migrating from account-only persistence."""
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )

    mod = _get_module("eventbridge")
    mod.reset()
    original_account = get_account_id()
    original_region = get_region()
    try:
        set_request_account_id("111111111111")
        set_request_region("us-east-1")
        rule_key = mod._rule_key("legacy-rule", "default")
        legacy_rules = AccountScopedDict()
        legacy_targets = AccountScopedDict()
        legacy_rules[rule_key] = {
            "Name": "legacy-rule",
            "Arn": "arn:aws:events:us-west-2:111111111111:rule/legacy-rule",
            "EventBusName": "default",
            "ScheduleExpression": "rate(5 minutes)",
            "State": "ENABLED",
        }
        legacy_targets[rule_key] = [
            {
                "Id": "foreign-sns",
                "Arn": "arn:aws:sns:eu-central-1:111111111111:legacy-topic",
            }
        ]

        mod.restore_state({"rules": legacy_rules, "targets": legacy_targets})

        set_request_region("us-west-2")
        assert mod._rules[rule_key]["Name"] == "legacy-rule"
        assert mod._targets[rule_key][0]["Id"] == "foreign-sns"

        set_request_region("eu-central-1")
        assert mod._targets.get(rule_key) is None
    finally:
        mod.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


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
    mod = _get_module("ecs")
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
    # a per-test tmp by the autouse `_enable_persistence_dict` fixture.

    # Step 1+2: produce + persist a snapshot using the already-loaded
    # module (so we get a valid get_state() shape).
    mod = _get_module(mod_name)
    if hasattr(mod, "reset"):
        mod.reset()
    persistence.save_state(svc_key, mod.get_state())

    # Step 3: cold-import — wipe sys.modules and re-import.
    # importlib.reload() won't work because it merges into the
    # existing namespace; the late-declared symbol stays bound from
    # the prior import.
    import ministack.services as _services_pkg

    full_name = f"ministack.services.{mod_name}"
    # The cold-import swaps a brand-new module object into BOTH sys.modules and
    # the `ministack.services` package attribute. Other already-imported modules
    # that did `from ministack.services import <mod>` keep a reference to the
    # ORIGINAL object, so we must restore it afterwards. Otherwise the fresh
    # module (with empty, reset state) leaks into later tests on the same xdist
    # worker and desyncs cross-module references — e.g. cold-importing `ecs`
    # then `secretsmanager` leaves the fresh `ecs` pointing at a stale
    # `secretsmanager`, so ECS RunTask can no longer resolve Secrets Manager
    # secrets created via the live module. Both the sys.modules entry and the
    # package attribute must be restored: `from ministack.services import <mod>`
    # reads the package attribute, not sys.modules directly.
    original_mod = sys.modules.get(full_name)
    sys.modules.pop(full_name, None)

    try:
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
    finally:
        # Re-register the original module so references bound before this test
        # (e.g. `ecs.secretsmanager`) stay valid for subsequent tests.
        if original_mod is not None:
            sys.modules[full_name] = original_mod
            setattr(_services_pkg, mod_name, original_mod)

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


def test_legacy_unwrapped_state_file_loads_and_migrates_region(monkeypatch, tmp_path):
    """U4: a pre-version-stamp on-disk file (no wrapper, account-scoped) must
    load through the real load_state() disk path and migrate into a region-
    scoped store, recovering region from a value ARN. Prior tests only seeded
    dicts in memory, so this disk->migrate path was never exercised."""
    import json as _json

    from ministack.core.responses import (
        AccountRegionScopedDict,
        AccountScopedDict,
        set_request_region,
    )

    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))

    legacy = AccountScopedDict()
    legacy._data[("000000000000", "res-1")] = {
        "Arn": "arn:aws:appconfig:eu-west-1:000000000000:application/res-1",
    }
    # Write a legacy (unwrapped, implicit-v1) file exactly as old MiniStack did.
    with open(tmp_path / "demo.json", "w") as f:
        _json.dump({"store": legacy}, f, default=persistence._json_default)

    loaded = persistence.load_state("demo")
    assert isinstance(loaded["store"], AccountScopedDict)

    region_store = AccountRegionScopedDict()
    region_store.update(loaded["store"])
    set_request_region("eu-west-1")
    assert region_store["res-1"]["Arn"].startswith("arn:aws:appconfig:eu-west-1")


def test_default_state_format_version_stays_v2_and_refuses_newer(monkeypatch, tmp_path):
    """U4: unchanged services stay on v2 and refuse newer state files."""
    import json as _json

    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))

    persistence.save_state("sqs", {"k": "v"})
    raw = _json.loads((tmp_path / "sqs.json").read_text())
    assert raw["__ministack_format__"] == persistence.STATE_FORMAT_VERSION == 2
    assert raw["payload"] == {"k": "v"}
    assert persistence.load_state("sqs") == {"k": "v"}

    (tmp_path / "future.json").write_text(_json.dumps({
        "__ministack_format__": persistence.STATE_FORMAT_VERSION + 1,
        "payload": {"k": "v"},
    }))
    assert persistence.load_state("future") is None


def test_ecs_region_scoped_state_is_rejected_by_v2_reader(monkeypatch, tmp_path):
    """A rollback binary must reject ECS's regional schema instead of
    accepting it as v2 and silently dropping every regional store."""
    import json as _json

    from ministack.core.responses import AccountRegionScopedDict

    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))

    clusters = AccountRegionScopedDict()
    clusters.set_scoped(
        "000000000000",
        "us-west-2",
        "regional-cluster",
        {"clusterArn": "arn:aws:ecs:us-west-2:000000000000:cluster/regional-cluster"},
    )
    persistence.save_state("ecs", {"clusters": clusters})

    raw = _json.loads((tmp_path / "ecs.json").read_text())
    assert raw["__ministack_format__"] == 3
    loaded_clusters = persistence.load_state("ecs")["clusters"]
    assert loaded_clusters.get_scoped(
        "000000000000", "us-west-2", "regional-cluster"
    )["clusterArn"].endswith("cluster/regional-cluster")

    # Simulate the previous binary, whose highest understood format is v2.
    monkeypatch.setattr(persistence, "SERVICE_STATE_FORMAT_VERSIONS", {})
    assert persistence.load_state("ecs") is None


def test_appsync_region_scoped_state_is_rejected_by_v2_reader(
    monkeypatch, tmp_path
):
    """A rollback binary must reject AppSync's regional schema instead of
    accepting it as v2 and silently dropping every regional store."""
    import json as _json

    from ministack.core.responses import AccountRegionScopedDict

    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))

    apis = AccountRegionScopedDict()
    apis.set_scoped(
        "000000000000",
        "us-west-2",
        "regional-api",
        {
            "apiId": "regional-api",
            "arn": "arn:aws:appsync:us-west-2:000000000000:apis/regional-api",
        },
    )
    persistence.save_state("appsync", {"apis": apis})

    raw = _json.loads((tmp_path / "appsync.json").read_text())
    assert raw["__ministack_format__"] == 3
    loaded_apis = persistence.load_state("appsync")["apis"]
    assert loaded_apis.get_scoped(
        "000000000000", "us-west-2", "regional-api"
    )["apiId"] == "regional-api"

    # Simulate the previous binary, whose highest understood format is v2.
    monkeypatch.setattr(persistence, "SERVICE_STATE_FORMAT_VERSIONS", {})
    assert persistence.load_state("appsync") is None


def test_resource_groups_region_scoped_state_is_rejected_by_v2_reader(
    monkeypatch, tmp_path
):
    """A rollback binary must reject Resource Groups' regional schema instead
    of accepting it as v2 and silently dropping every regional store."""

    import json as _json

    from ministack.core.responses import AccountRegionScopedDict

    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))

    groups = AccountRegionScopedDict()
    groups.set_scoped(
        "000000000000",
        "us-west-2",
        "regional-group",
        {
            "GroupArn": (
                "arn:aws:resource-groups:us-west-2:000000000000:"
                "group/regional-group"
            )
        },
    )
    persistence.save_state("resource_groups", {"groups": groups})

    raw = _json.loads((tmp_path / "resource_groups.json").read_text())
    assert raw["__ministack_format__"] == 3
    loaded_groups = persistence.load_state("resource_groups")["groups"]
    assert loaded_groups.get_scoped(
        "000000000000", "us-west-2", "regional-group"
    )["GroupArn"].endswith("group/regional-group")

    # Simulate the previous binary, whose highest understood format is v2.
    monkeypatch.setattr(persistence, "SERVICE_STATE_FORMAT_VERSIONS", {})
    assert persistence.load_state("resource_groups") is None


def test_codebuild_region_scoped_state_is_rejected_by_v2_reader(
    monkeypatch, tmp_path
):
    """A rollback binary must reject CodeBuild's regional schema instead of
    accepting it as v2 and silently dropping every regional store."""
    import json as _json

    from ministack.core.responses import AccountRegionScopedDict

    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))

    projects = AccountRegionScopedDict()
    projects.set_scoped(
        "000000000000",
        "us-west-2",
        "regional-project",
        {
            "arn": (
                "arn:aws:codebuild:us-west-2:000000000000:"
                "project/regional-project"
            )
        },
    )
    persistence.save_state("codebuild", {"projects": projects})

    raw = _json.loads((tmp_path / "codebuild.json").read_text())
    assert raw["__ministack_format__"] == 3
    loaded_projects = persistence.load_state("codebuild")["projects"]
    assert loaded_projects.get_scoped(
        "000000000000", "us-west-2", "regional-project"
    )["arn"].endswith("project/regional-project")

    # Simulate the previous binary, whose highest understood format is v2.
    monkeypatch.setattr(persistence, "SERVICE_STATE_FORMAT_VERSIONS", {})
    assert persistence.load_state("codebuild") is None


def test_mq_region_scoped_state_is_rejected_by_v2_reader(monkeypatch, tmp_path):
    """A rollback binary must reject MQ's regional schema instead of
    accepting it as v2 and silently dropping every regional store."""
    import json as _json

    from ministack.core.responses import AccountRegionScopedDict

    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))

    brokers = AccountRegionScopedDict()
    brokers.set_scoped(
        "000000000000",
        "us-west-2",
        "regional-broker",
        {
            "brokerArn": (
                "arn:aws:mq:us-west-2:000000000000:broker:regional-broker"
            )
        },
    )
    persistence.save_state("mq", {"brokers": brokers})

    raw = _json.loads((tmp_path / "mq.json").read_text())
    assert raw["__ministack_format__"] == 3
    loaded_brokers = persistence.load_state("mq")["brokers"]
    assert loaded_brokers.get_scoped(
        "000000000000", "us-west-2", "regional-broker"
    )["brokerArn"].endswith("broker:regional-broker")

    monkeypatch.setattr(persistence, "SERVICE_STATE_FORMAT_VERSIONS", {})
    assert persistence.load_state("mq") is None


def test_ses_region_scoped_state_is_rejected_by_v2_reader(monkeypatch, tmp_path):
    """A rollback binary must reject both SES persistence files after their
    stores become regional instead of accepting v2 and restoring them empty."""
    import json as _json

    from ministack.core.responses import AccountRegionScopedDict

    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))

    for service in ("ses", "ses_v2"):
        identities = AccountRegionScopedDict()
        identities.set_scoped(
            "000000000000",
            "us-west-2",
            "regional@example.com",
            {"VerificationStatus": "Success"},
        )
        persistence.save_state(service, {"_identities": identities})

        raw = _json.loads((tmp_path / f"{service}.json").read_text())
        assert raw["__ministack_format__"] == 3
        loaded_identities = persistence.load_state(service)["_identities"]
        assert loaded_identities.get_scoped(
            "000000000000", "us-west-2", "regional@example.com"
        )["VerificationStatus"] == "Success"

    # Simulate the previous binary, whose highest understood format is v2.
    monkeypatch.setattr(persistence, "SERVICE_STATE_FORMAT_VERSIONS", {})
    assert persistence.load_state("ses") is None
    assert persistence.load_state("ses_v2") is None


def test_batch_region_scoped_state_is_rejected_by_v2_reader(monkeypatch, tmp_path):
    """A rollback binary must reject Batch's regional schema instead of
    accepting it as v2 and silently dropping every regional store."""
    import json as _json

    from ministack.core.responses import AccountRegionScopedDict

    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))

    jobs = AccountRegionScopedDict()
    jobs.set_scoped(
        "000000000000",
        "us-west-2",
        "regional-job",
        {"jobArn": "arn:aws:batch:us-west-2:000000000000:job/regional-job"},
    )
    persistence.save_state("batch", {"jobs": jobs})

    raw = _json.loads((tmp_path / "batch.json").read_text())
    assert raw["__ministack_format__"] == 3
    loaded_jobs = persistence.load_state("batch")["jobs"]
    assert loaded_jobs.get_scoped(
        "000000000000", "us-west-2", "regional-job"
    )["jobArn"].endswith("job/regional-job")

    # Simulate the previous binary, whose highest understood format is v2.
    monkeypatch.setattr(persistence, "SERVICE_STATE_FORMAT_VERSIONS", {})
    assert persistence.load_state("batch") is None


def test_batch_persistence_lifecycle_restores_regional_state(monkeypatch, tmp_path):
    """The gateway save map and Batch import-time restore must preserve state
    outside the ambient boot region across a process-shaped reload."""
    import importlib

    from ministack.app import _build_persistence_save_dict, _state_map
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import batch as service

    account_id = "111111111111"
    boot_region = "us-east-1"
    resource_region = "us-west-2"
    job_id = "regional-job"
    job = {
        "jobArn": f"arn:aws:batch:{resource_region}:{account_id}:job/{job_id}",
        "status": "SUCCEEDED",
    }

    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))
    set_request_account_id(account_id)
    set_request_region(boot_region)
    service.reset()
    try:
        service._jobs.set_scoped(account_id, resource_region, job_id, job)

        assert _state_map["batch"] == "batch"
        save_dict = _build_persistence_save_dict()
        assert "batch" in save_dict
        persistence.save_all({"batch": save_dict["batch"]})

        service.reset()
        importlib.reload(service)

        assert service._jobs.get_scoped(
            account_id, resource_region, job_id
        ) == job
        assert service._jobs.get_scoped(account_id, boot_region, job_id) is None
    finally:
        service.reset()
