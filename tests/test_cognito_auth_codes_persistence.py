"""
Regression test for cognito hosted-UI / federation `_auth_codes`
persistence.

Background
----------
Two distinct OAuth2 code stores exist in `services/cognito.py`:

  - `_authorization_codes` — managed-login PKCE flow. Was already
    persisted in get_state/restore_state.
  - `_auth_codes` — hosted-UI / SAML-OIDC federation relay flow. Was
    declared with the comment "ephemeral, not persisted" but the codes
    have a 5-minute TTL and any in-flight Cognito hosted-UI sign-in
    that straddles a warm-boot becomes invalid for no good reason
    (the user has to re-auth even though the code itself is still
    well within its TTL).

Why these dicts are intentionally PLAIN dicts (not AccountScopedDict):
the OAuth2 token endpoint (`cognito.py:_oauth2_token`) is a public
endpoint with no AWS authentication context — it identifies the
caller via the OAuth2 `code` + `client_id` + `client_secret` (HTTP
Basic), NOT via a SigV4 access key. If these dicts were AccountScopedDict,
the code lookup would happen under whatever default account the
callback runs in, and codes issued under one tenant would be invisible
to other tenants, breaking the OAuth2 flow entirely. Effective tenant
isolation is provided by the random unguessable token namespace.
"""
import importlib

import pytest

from ministack.core import persistence


def _module():
    return importlib.import_module("ministack.services.cognito")


@pytest.fixture(autouse=True)
def _enable_persistence(monkeypatch, tmp_path):
    """Force PERSIST_STATE on and point STATE_DIR at a tmp dir so
    save_state / load_state actually write and read JSON files."""
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))


def _round_trip(mod, svc_key="cognito"):
    """Simulate a full warm-boot via the on-disk JSON path."""
    persistence.save_state(svc_key, mod.get_state())
    mod.reset()
    loaded = persistence.load_state(svc_key)
    assert loaded is not None, "load_state returned None — get_state may be wrong"
    mod.restore_state(loaded)


def test_auth_codes_survive_warm_boot():
    """`_auth_codes` populated by the hosted-UI / federation flow must
    survive a warm-boot through the on-disk JSON path. Without the fix
    `_auth_codes` was missing from get_state/restore_state, so any
    in-flight hosted-UI sign-in within the 5-minute code TTL was
    silently invalidated by a restart."""
    mod = _module()
    mod.reset()

    # Match the actual production payload shape written by the SAML/OIDC
    # callback handler in `cognito.py` — `type: "code"` entries store
    # `scopes` as a space-separated string (copied from `relay["scope"]`)
    # and have no `state` field (state lives on the sibling `type: "relay"`
    # entry that this code-type one is created from).
    relay_state = "test-relay-12345"
    mod._auth_codes[relay_state] = {
        "type": "code",
        "pool_id": "us-east-1_TestPool",
        "client_id": "client-id-abc",
        "username": "user@example.com",
        "sub": "user-sub-12345",
        "redirect_uri": "https://app.example.com/callback",
        "scopes": "openid email",
        "created_at": 1700000000.0,
    }

    _round_trip(mod)

    assert relay_state in mod._auth_codes, (
        "Hosted-UI relay code lost across warm-boot — _auth_codes must "
        "be in both get_state() and restore_state()."
    )
    assert mod._auth_codes[relay_state]["pool_id"] == "us-east-1_TestPool"
    assert mod._auth_codes[relay_state]["client_id"] == "client-id-abc"
    mod.reset()


def test_auth_codes_dict_types_are_plain_builtin_dict():
    """Belt-and-braces: assert that `_auth_codes` and
    `_authorization_codes` remain plain built-in `dict` instances —
    not AccountScopedDict, not any other dict-like wrapper.

    These dicts are looked up by random unguessable token from a public
    OAuth2 callback with no AWS auth context. Wrapping them in
    AccountScopedDict (or anything else with request-scoped lookup
    semantics) would make the callback lookup happen under a default
    account, invisible to codes issued under any other tenant —
    breaking the entire OAuth2 flow. This test pins the type so a
    well-meaning future refactor doesn't silently break OAuth2.

    `type(x) is dict` (strict identity) catches not just AccountScopedDict
    but also any other Mapping subclass that might quietly slip in."""
    mod = _module()

    assert type(mod._auth_codes) is dict, (
        "_auth_codes must remain a plain built-in dict — see the "
        "docstring for why."
    )
    assert type(mod._authorization_codes) is dict, (
        "_authorization_codes must remain a plain built-in dict — same "
        "reason."
    )
