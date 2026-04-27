"""
Regression tests for "state dict dropped from get_state/restore_state" bugs.

Pattern: a service exposes an API that mutates an `AccountScopedDict`,
but the dict is missing from `get_state()` and/or `restore_state()`. With
PERSIST_STATE=1, every record stored via that API silently disappears on
the next restart.

This file covers five distinct state-dict persistence drops surfaced by
the persistence-symmetry audit:

  H-1  secretsmanager._resource_policies
  H-3  kinesis._consumers             (enhanced fan-out)
  H-4  ecs._attributes                (PutAttributes / ListAttributes)
  H-5  sns._platform_applications
  H-5  sns._platform_endpoints

Each test populates the dict, snapshots state via the public
`get_state()` / `restore_state()` contract, simulates a restart, and
asserts the record survived.
"""
import importlib

import pytest


def _module(mod_name):
    return importlib.import_module(f"ministack.services.{mod_name}")


def _round_trip(mod):
    """Snapshot → reset → restore via the module's public hooks."""
    snapshot = mod.get_state()
    mod.reset()
    mod.restore_state(snapshot)


# ── H-1: secretsmanager._resource_policies ─────────────────────────────

def test_secretsmanager_resource_policies_survive_warm_boot():
    """`PutResourcePolicy` writes to `_resource_policies`, but if that
    dict is missing from `get_state()` the policy is gone after restart.
    Terraform `aws_secretsmanager_secret_policy` would silently drop."""
    mod = _module("secretsmanager")
    mod.reset()
    arn = "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret-AbCdEf"
    mod._resource_policies[arn] = '{"Version":"2012-10-17","Statement":[]}'

    _round_trip(mod)

    assert mod._resource_policies.get(arn) == '{"Version":"2012-10-17","Statement":[]}', (
        "Resource policy lost across get_state → restore_state — "
        "_resource_policies must be in both."
    )
    mod.reset()


# ── H-3: kinesis._consumers ────────────────────────────────────────────

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

    _round_trip(mod)

    assert consumer_arn in mod._consumers, (
        "Kinesis consumer lost across get_state → restore_state — "
        "_consumers must be in both."
    )
    mod.reset()


# ── H-4: ecs._attributes ───────────────────────────────────────────────

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

    _round_trip(mod)

    assert "i-deadbeef:my-attr" in mod._attributes, (
        "ECS attribute lost across get_state → restore_state — "
        "_attributes must be in both."
    )
    mod.reset()


# ── H-5: sns._platform_applications + sns._platform_endpoints ─────────

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

    _round_trip(mod)

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

    _round_trip(mod)

    assert ep_arn in mod._platform_endpoints, (
        "SNS platform endpoint lost across get_state → restore_state — "
        "_platform_endpoints must be in both."
    )
    mod.reset()
