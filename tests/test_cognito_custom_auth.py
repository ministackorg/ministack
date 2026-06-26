"""Tests for Cognito CUSTOM_AUTH flow — DefineAuthChallenge, CreateAuthChallenge, VerifyAuthChallenge."""

import io
import json
import time
import zipfile

import pytest
from botocore.exceptions import ClientError


# ── Module-level state reset between tests ────────────────────────────────────

@pytest.fixture(autouse=True)
def clear_challenge_sessions():
    """Reset the TEST-PROCESS _challenge_sessions after each in-process unit test.

    This only clears the test process's module instance (used by the in-process
    unit tests below). It does NOT touch the server's session store; server-side
    sessions are keyed by random tokens, so they never collide across API tests.
    """
    import ministack.services.cognito as cognito_mod
    yield
    cognito_mod._challenge_sessions.clear()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_zip(handler_code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", handler_code)
    return buf.getvalue()


def _setup_pool(cognito_idp, pool_name, lambda_config=None):
    """Create pool + client with ALLOW_CUSTOM_AUTH + enabled user. Returns (pool_id, client_id)."""
    kwargs = {"PoolName": pool_name}
    if lambda_config:
        kwargs["LambdaConfig"] = lambda_config
    pid = cognito_idp.create_user_pool(**kwargs)["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid,
        ClientName="app",
        ExplicitAuthFlows=["ALLOW_CUSTOM_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid,
        Username="user@example.com",
        MessageAction="SUPPRESS",
    )
    cognito_idp.admin_set_user_password(
        UserPoolId=pid, Username="user@example.com", Password="Pass1234!", Permanent=True
    )
    return pid, cid


def _create_lambda(lam, fn_name, handler_code):
    """Deploy a Python Lambda function and return its ARN."""
    try:
        lam.delete_function(FunctionName=fn_name)
    except Exception:
        pass
    lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/service-role/lambda-role",
        Handler="index.handler",
        Code={"ZipFile": _make_zip(handler_code)},
    )
    return lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]


# ── Test 1: InitiateAuth CUSTOM_AUTH, no Lambda triggers configured ────────────

def test_custom_auth_initiate_no_trigger(cognito_idp):
    """When no CreateAuthChallenge Lambda is configured, return the default PROVIDE_AUTH_PARAMETERS challenge."""
    pid, cid = _setup_pool(cognito_idp, "NoTriggerPool")

    resp = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )

    assert resp["ChallengeName"] == "CUSTOM_CHALLENGE"
    assert "Session" in resp
    assert len(resp["Session"]) > 10  # non-empty, real-looking token
    assert resp["ChallengeParameters"].get("challenge") == "PROVIDE_AUTH_PARAMETERS"


# ── Test 2: InitiateAuth with CreateAuthChallenge Lambda ─────────────────────

def test_custom_auth_initiate_with_create_trigger(cognito_idp, lam):
    """CreateAuthChallenge Lambda is invoked; its publicChallengeParameters are returned."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'MAGIC_LINK', 'emailIdentifier': 'A1'}\n"
        "    return event\n"
    )
    fn_arn = _create_lambda(lam, "create-auth-basic", create_handler)
    pid, cid = _setup_pool(cognito_idp, "CreateTriggerPool", {"CreateAuthChallenge": fn_arn})

    resp = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )

    assert resp["ChallengeName"] == "CUSTOM_CHALLENGE"
    assert resp["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    assert resp["ChallengeParameters"]["emailIdentifier"] == "A1"


# ── Test 3: InitiateAuth — user not found ────────────────────────────────────

def test_custom_auth_user_not_found(cognito_idp):
    pid = cognito_idp.create_user_pool(PoolName="NfPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="app",
        ExplicitAuthFlows=["ALLOW_CUSTOM_AUTH"],
    )["UserPoolClient"]["ClientId"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.initiate_auth(
            ClientId=cid,
            AuthFlow="CUSTOM_AUTH",
            AuthParameters={"USERNAME": "notfound@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "UserNotFoundException"


# ── Test 4: InitiateAuth — user disabled ─────────────────────────────────────

def test_custom_auth_user_disabled(cognito_idp):
    pid, cid = _setup_pool(cognito_idp, "DisabledPool")
    cognito_idp.admin_disable_user(UserPoolId=pid, Username="user@example.com")

    with pytest.raises(ClientError) as exc:
        cognito_idp.initiate_auth(
            ClientId=cid,
            AuthFlow="CUSTOM_AUTH",
            AuthParameters={"USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "NotAuthorizedException"


# ── Test 5: InitiateAuth — client missing ALLOW_CUSTOM_AUTH ──────────────────

def test_custom_auth_client_missing_explicit_flow(cognito_idp):
    """Client without ALLOW_CUSTOM_AUTH in ExplicitAuthFlows is rejected."""
    pid = cognito_idp.create_user_pool(PoolName="WrongFlowPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="app",
        ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],  # no ALLOW_CUSTOM_AUTH
    )["UserPoolClient"]["ClientId"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.initiate_auth(
            ClientId=cid,
            AuthFlow="CUSTOM_AUTH",
            AuthParameters={"USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 6: RespondToAuthChallenge — missing Session ─────────────────────────

def test_custom_auth_respond_missing_session(cognito_idp):
    pid, cid = _setup_pool(cognito_idp, "MissingSessionPool")

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 7: RespondToAuthChallenge — invalid session token ───────────────────

def test_custom_auth_respond_invalid_session(cognito_idp):
    pid, cid = _setup_pool(cognito_idp, "InvalidSessionPool")

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session="invalid-session-token-1234567890",
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 8: Session expiry — in-process unit test ────────────────────────────
# Cannot be driven over HTTP: the test process can't fast-forward the server's
# session clock, and there's no API to do so. Test the helper directly.

def test_custom_auth_session_expiry_in_process():
    import ministack.services.cognito as cognito_mod

    token, session = cognito_mod._create_challenge_session(
        "us-east-1_pool", "client123", "user@example.com"
    )
    # Live session resolves cleanly.
    got, err = cognito_mod._get_challenge_session(token)
    assert got is session and err is None

    # Expire it, then confirm _get_challenge_session rejects and evicts it.
    session["expires_at"] = time.time() - 1  # in the past
    got, err = cognito_mod._get_challenge_session(token)
    assert got is None
    assert "expired" in err.lower()
    assert cognito_mod._challenge_sessions.get(token) is None


# ── Test 9: Full flow — correct answer, tokens issued ────────────────────────

def test_custom_auth_full_flow_issue_tokens(cognito_idp, lam):
    """InitiateAuth → RespondToAuthChallenge with correct answer → AuthenticationResult."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'MAGIC_LINK'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-full-flow", create_handler)
    verify_arn = _create_lambda(lam, "verify-full-flow", verify_handler)
    define_arn = _create_lambda(lam, "define-full-flow", define_handler)

    pid, cid = _setup_pool(cognito_idp, "FullFlowPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeName"] == "CUSTOM_CHALLENGE"
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "SECRETCODE", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step2
    result = step2["AuthenticationResult"]
    assert "AccessToken" in result
    assert "IdToken" in result
    assert "RefreshToken" in result


# ── Test 10: Wrong answer → failAuthentication, session cleared ───────────────

def test_custom_auth_wrong_answer_fail_auth(cognito_idp, lam):
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'MAGIC_LINK'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = False\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-fail-auth", create_handler)
    verify_arn = _create_lambda(lam, "verify-fail-auth", verify_handler)
    define_arn = _create_lambda(lam, "define-fail-auth", define_handler)

    pid, cid = _setup_pool(cognito_idp, "FailAuthPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "WRONGCODE", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "NotAuthorizedException"

    # Prove the session was cleared server-side: retrying with the same token
    # is now rejected as a non-existent session (verified via the API).
    with pytest.raises(ClientError) as exc2:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "STILLWRONG", "USERNAME": "user@example.com"},
        )
    assert exc2.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 11: Multi-round — magic link then SMS OTP ────────────────────────────

def test_custom_auth_multi_round(cognito_idp, lam):
    """Three steps: InitiateAuth → Respond(magic link) → Respond(SMS OTP) → tokens."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    answered_count = len([c for c in event['request']['session'] if c.get('challengeResult')])\n"
        "    if answered_count == 0:\n"
        "        event['response']['publicChallengeParameters'] = {'round': '1', 'challenge': 'MAGIC_LINK'}\n"
        "    else:\n"
        "        event['response']['publicChallengeParameters'] = {'round': str(answered_count + 1), 'challenge': 'SMS_OTP'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    answered = [s for s in session if s.get('challengeResult') is not None]\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif len(answered) >= 2:\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-multi", create_handler)
    verify_arn = _create_lambda(lam, "verify-multi", verify_handler)
    define_arn = _create_lambda(lam, "define-multi", define_handler)

    pid, cid = _setup_pool(cognito_idp, "MultiRoundPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "magic-link-code", "USERNAME": "user@example.com"},
    )
    assert step2.get("ChallengeName") == "CUSTOM_CHALLENGE"
    assert step2["ChallengeParameters"]["challenge"] == "SMS_OTP"
    assert step2["Session"] == session  # SAME token — never re-generated

    step3 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step3


# ── Test 12: Lambda not found — session preserved for retry ──────────────────

def test_custom_auth_lambda_not_found_session_preserved(cognito_idp):
    # Only VerifyAuthChallengeResponse points at a non-existent Lambda.
    pid, cid = _setup_pool(cognito_idp, "LambdaNotFoundPool", {
        "VerifyAuthChallengeResponse": "arn:aws:lambda:us-east-1:000000000000:function:does-not-exist",
    })

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = resp["Session"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidLambdaResponseException"

    # Session preserved — a retry with the same token reaches the trigger again
    with pytest.raises(ClientError) as exc2:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc2.value.response["Error"]["Code"] == "InvalidLambdaResponseException"


# ── Test 13: Lambda crashes — session preserved ───────────────────────────────

def test_custom_auth_lambda_crash_session_preserved(cognito_idp, lam):
    broken = "def handler(event, ctx):\n    raise RuntimeError('boom')\n"
    verify_arn = _create_lambda(lam, "verify-crash", broken)

    pid, cid = _setup_pool(cognito_idp, "CrashPool", {
        "VerifyAuthChallengeResponse": verify_arn,
    })

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = resp["Session"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidLambdaResponseException"

    # Session preserved — retry reaches the crashing trigger again
    with pytest.raises(ClientError) as exc2:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc2.value.response["Error"]["Code"] == "InvalidLambdaResponseException"


# ── Test 14: AdminInitiateAuth CUSTOM_AUTH ────────────────────────────────────

def test_custom_auth_admin_initiate(cognito_idp):
    pid, cid = _setup_pool(cognito_idp, "AdminInitPool")

    resp = cognito_idp.admin_initiate_auth(
        UserPoolId=pid, ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert resp["ChallengeName"] == "CUSTOM_CHALLENGE"
    assert "Session" in resp


# ── Test 15: AdminRespondToAuthChallenge CUSTOM_CHALLENGE ─────────────────────

def test_custom_auth_admin_respond(cognito_idp, lam):
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    verify_arn = _create_lambda(lam, "verify-admin", verify_handler)
    define_arn = _create_lambda(lam, "define-admin", define_handler)

    pid, cid = _setup_pool(cognito_idp, "AdminRespondPool", {
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid, ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    step2 = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid, ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "code", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step2


# ── Test 16: ClientMetadata is top-level, propagated to Lambda ────────────────

def test_custom_auth_client_metadata_propagated(cognito_idp, lam):
    """ClientMetadata is a top-level InitiateAuth field, not inside AuthParameters."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    meta = event['request'].get('clientMetadata', {})\n"
        "    event['response']['publicChallengeParameters'] = {'signInMethod': meta.get('signInMethod', 'unknown')}\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-meta", create_handler)
    pid, cid = _setup_pool(cognito_idp, "MetaPool", {"CreateAuthChallenge": create_arn})

    resp = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
        ClientMetadata={"signInMethod": "MAGIC_LINK"},
    )
    assert resp["ChallengeParameters"]["signInMethod"] == "MAGIC_LINK"


# ── Test 17: Session persists across get_state/restore_state — in-process ────

def test_custom_auth_session_persistence():
    import ministack.services.cognito as cognito_mod

    token, _session = cognito_mod._create_challenge_session(
        "us-east-1_pool", "client123", "user@example.com"
    )
    assert cognito_mod._challenge_sessions.get(token) is not None

    # Save, clear, restore.
    state = cognito_mod.get_state()
    cognito_mod._challenge_sessions.clear()
    assert cognito_mod._challenge_sessions.get(token) is None
    cognito_mod.restore_state(state)
    assert cognito_mod._challenge_sessions.get(token) is not None


# ── Test 18: Concurrent sessions for same user ───────────────────────────────

def test_custom_auth_concurrent_sessions(cognito_idp, lam):
    """Two parallel auth flows for the same user get independent, usable tokens."""
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-concurrent", define_handler)
    verify_arn = _create_lambda(lam, "verify-concurrent", verify_handler)

    pid, cid = _setup_pool(cognito_idp, "ConcurrentPool", {
        "DefineAuthChallenge": define_arn,
        "VerifyAuthChallengeResponse": verify_arn,
    })

    s1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )["Session"]
    s2 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )["Session"]

    assert s1 != s2

    # Both tokens are live and independent — complete each to tokens
    r1 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid, ChallengeName="CUSTOM_CHALLENGE", Session=s1,
        ChallengeResponses={"ANSWER": "", "USERNAME": "user@example.com"},
    )
    r2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid, ChallengeName="CUSTOM_CHALLENGE", Session=s2,
        ChallengeResponses={"ANSWER": "", "USERNAME": "user@example.com"},
    )
    # Each session should issue tokens independently (DefineAuthChallenge sees a completed challenge)
    assert "AuthenticationResult" in r1
    assert "AuthenticationResult" in r2


# ── Test 19: LambdaConfig keys stored correctly ───────────────────────────────

def test_custom_auth_lambda_config_stored(cognito_idp):
    """Pool LambdaConfig stores DefineAuthChallenge, CreateAuthChallenge, VerifyAuthChallengeResponse."""
    define_arn = "arn:aws:lambda:us-east-1:000000000000:function:define"
    create_arn = "arn:aws:lambda:us-east-1:000000000000:function:create"
    verify_arn = "arn:aws:lambda:us-east-1:000000000000:function:verify"

    pid = cognito_idp.create_user_pool(
        PoolName="LambdaConfigPool",
        LambdaConfig={
            "DefineAuthChallenge": define_arn,
            "CreateAuthChallenge": create_arn,
            "VerifyAuthChallengeResponse": verify_arn,
        },
    )["UserPool"]["Id"]

    desc = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    cfg = desc["LambdaConfig"]
    assert cfg["DefineAuthChallenge"] == define_arn
    assert cfg["CreateAuthChallenge"] == create_arn
    assert cfg["VerifyAuthChallengeResponse"] == verify_arn


# ── Test 20: DefineAuth unexpected response — session cleared ─────────────────

def test_custom_auth_define_unexpected_response_clears_session(cognito_idp, lam):
    """DefineAuthChallenge with all-false response and no challengeName clears session."""
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    else:\n"
        "        pass\n"
        "    return event\n"
    )
    verify_arn = _create_lambda(lam, "verify-unexpected", verify_handler)
    define_arn = _create_lambda(lam, "define-unexpected", define_handler)

    pid, cid = _setup_pool(cognito_idp, "UnexpectedPool", {
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = resp["Session"]

    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidLambdaResponseException"

    # Session cleared
    with pytest.raises(ClientError) as exc2:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc2.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 21: Empty ANSWER is forwarded to Lambda ────────────────────────────

def test_custom_auth_empty_answer_passed_to_lambda(cognito_idp, lam):
    """Empty ANSWER must not be rejected by the emulator — Lambda handles it."""
    verify_handler = (
        "def handler(event, ctx):\n"
        "    answer = event['request'].get('challengeAnswer', '')\n"
        "    event['response']['answerCorrect'] = len(answer) == 0\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    verify_arn = _create_lambda(lam, "verify-empty-answer", verify_handler)
    define_arn = _create_lambda(lam, "define-empty-answer", define_handler)

    pid, cid = _setup_pool(cognito_idp, "EmptyAnswerPool", {
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step2


# ── Test 22: DefineAuth issues tokens at InitiateAuth (zero-round bypass) ────

def test_custom_auth_define_issues_tokens_at_initiate(cognito_idp, lam):
    """DefineAuthChallenge returning issueTokens=True at InitiateAuth bypasses the challenge."""
    define_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['issueTokens'] = True\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-bypass", define_handler)
    pid, cid = _setup_pool(cognito_idp, "BypassPool", {"DefineAuthChallenge": define_arn})

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    # Tokens issued directly from InitiateAuth
    assert "AuthenticationResult" in resp
    assert "AccessToken" in resp["AuthenticationResult"]


# ── Test 23: Session cleared after issueTokens=True ──────────────────────────

def test_custom_auth_session_cleared_after_tokens_issued(cognito_idp, lam):
    """Session is deleted after AuthenticationResult — verified via the API."""
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    verify_arn = _create_lambda(lam, "verify-cleanup", verify_handler)
    define_arn = _create_lambda(lam, "define-cleanup", define_handler)

    pid, cid = _setup_pool(cognito_idp, "CleanupPool", {
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session_token = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session_token,
        ChallengeResponses={"ANSWER": "code", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step2

    # Session cleaned up after tokens issued
    with pytest.raises(ClientError) as exc:
        cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session_token,
            ChallengeResponses={"ANSWER": "test", "USERNAME": "user@example.com"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterException"


# ── Test 24: ClientMetadata propagated to VerifyAuthChallenge ────────────────

def test_custom_auth_client_metadata_propagated_to_verify(cognito_idp, lam):
    """ClientMetadata passed in RespondToAuthChallenge reaches VerifyAuthChallenge Lambda."""
    verify_handler = (
        "def handler(event, ctx):\n"
        "    meta = event['request'].get('clientMetadata', {})\n"
        "    if meta.get('signInMethod') == 'MAGIC_LINK':\n"
        "        event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    verify_arn = _create_lambda(lam, "verify-meta-respond", verify_handler)
    define_arn = _create_lambda(lam, "define-meta-respond", define_handler)

    pid, cid = _setup_pool(cognito_idp, "MetaRespondPool", {
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "code", "USERNAME": "user@example.com"},
        ClientMetadata={"signInMethod": "MAGIC_LINK"},
    )
    assert "AuthenticationResult" in step2


# ── Test 25: UpdateUserPool stores CUSTOM_AUTH LambdaConfig keys ──────────────

def test_custom_auth_lambda_config_stored_via_update(cognito_idp):
    """UpdateUserPool also stores DefineAuthChallenge, CreateAuthChallenge, VerifyAuthChallengeResponse."""
    pid = cognito_idp.create_user_pool(PoolName="UpdateLambdaPool")["UserPool"]["Id"]

    define_arn = "arn:aws:lambda:us-east-1:000000000000:function:define-update"
    create_arn = "arn:aws:lambda:us-east-1:000000000000:function:create-update"
    verify_arn = "arn:aws:lambda:us-east-1:000000000000:function:verify-update"

    cognito_idp.update_user_pool(
        UserPoolId=pid,
        LambdaConfig={
            "DefineAuthChallenge": define_arn,
            "CreateAuthChallenge": create_arn,
            "VerifyAuthChallengeResponse": verify_arn,
        },
    )

    desc = cognito_idp.describe_user_pool(UserPoolId=pid)["UserPool"]
    cfg = desc["LambdaConfig"]
    assert cfg["DefineAuthChallenge"] == define_arn
    assert cfg["CreateAuthChallenge"] == create_arn
    assert cfg["VerifyAuthChallengeResponse"] == verify_arn


# ── Test 26: User with empty Attributes list ─────────────────────────────────

def test_custom_auth_user_no_attributes(cognito_idp):
    """User created with no UserAttributes still completes CUSTOM_AUTH initiate."""
    pid = cognito_idp.create_user_pool(PoolName="NoAttrsPool")["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="app",
        ExplicitAuthFlows=["ALLOW_CUSTOM_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(
        UserPoolId=pid, Username="bare@example.com",
        MessageAction="SUPPRESS",
    )

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "bare@example.com"},
    )
    assert resp["ChallengeName"] == "CUSTOM_CHALLENGE"


# ── Test 27: DefineAuthChallenge present, CreateAuthChallenge absent ──────────

def test_custom_auth_define_present_create_absent(cognito_idp, lam):
    """DefineAuthChallenge configured but no CreateAuthChallenge — default challenge used."""
    define_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-no-create", define_handler)
    pid, cid = _setup_pool(cognito_idp, "DefineOnlyPool", {"DefineAuthChallenge": define_arn})

    resp = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert resp["ChallengeName"] == "CUSTOM_CHALLENGE"
    assert resp["ChallengeParameters"].get("challenge") == "PROVIDE_AUTH_PARAMETERS"


# ── Test 28: Session list grows correctly across rounds ───────────────────────

def test_custom_auth_session_list_grows_across_rounds(cognito_idp, lam):
    """Each round appends one entry to session['challenges']; DefineAuth sees the correct history.
    The first CreateAuthChallenge sees an empty session (AWS parity), so round == "0"."""
    define_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    if not session:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    elif session[-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['failAuthentication'] = True\n"
        "    return event\n"
    )
    create_handler = (
        "def handler(event, ctx):\n"
        "    session = event['request']['session']\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'MAGIC_LINK', 'round': str(len(session))}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-session-growth", define_handler)
    create_arn = _create_lambda(lam, "create-session-growth", create_handler)
    verify_arn = _create_lambda(lam, "verify-session-growth", verify_handler)

    pid, cid = _setup_pool(cognito_idp, "SessionGrowthPool", {
        "DefineAuthChallenge": define_arn,
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["round"] == "0"
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "code", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step2


# ── Test 29: Max challenge attempts exceeded ─────────────────────────────────

def test_custom_auth_max_attempts_exceeded(cognito_idp, lam):
    """Exceeded max attempts terminates with NotAuthorizedException."""
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'TEST'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = False\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-max-attempts", create_handler)
    verify_arn = _create_lambda(lam, "verify-max-attempts", verify_handler)
    define_arn = _create_lambda(lam, "define-max-attempts", define_handler)

    pid, cid = _setup_pool(cognito_idp, "MaxAttemptsPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step1["Session"]

    # Keep answering until max attempts exceeded
    last_exc = None
    for i in range(5):
        try:
            cognito_idp.respond_to_auth_challenge(
                ClientId=cid,
                ChallengeName="CUSTOM_CHALLENGE",
                Session=session,
                ChallengeResponses={"ANSWER": f"attempt{i}", "USERNAME": "user@example.com"},
            )
        except ClientError as e:
            last_exc = e
            if e.response["Error"]["Code"] == "NotAuthorizedException":
                break

    assert last_exc is not None
    assert last_exc.response["Error"]["Code"] == "NotAuthorizedException"


# ── Test: issueTokens on cap-boundary attempt must win over MaxAttempts ──────

def test_custom_auth_issue_tokens_on_third_attempt_boundary(cognito_idp, lam):
    """A correct answer on attempt N == MAX_CHALLENGE_ATTEMPTS must issue
    tokens, not be rejected for hitting the cap.

    Regression for the order-of-checks bug: the cap is meant to prevent a
    NEXT (4th) round, not penalize success on the boundary. Define returns
    issueTokens=True only after the 3rd answer; the prior buggy ordering
    rejected with `Max authentication attempts exceeded` before reaching the
    issueTokens branch.
    """
    create_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'challenge': 'TEST'}\n"
        "    return event\n"
    )
    verify_handler = (
        "def handler(event, ctx):\n"
        "    event['response']['answerCorrect'] = True\n"
        "    return event\n"
    )
    define_handler = (
        "def handler(event, ctx):\n"
        "    answered = sum(1 for c in event['request']['session']"
        " if c.get('challengeResult') is not None)\n"
        # Issue tokens exactly on the 3rd answered attempt (cap boundary).
        "    if answered >= 3:\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-boundary", create_handler)
    verify_arn = _create_lambda(lam, "verify-boundary", verify_handler)
    define_arn = _create_lambda(lam, "define-boundary", define_handler)

    pid, cid = _setup_pool(cognito_idp, "BoundaryPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    session = step["Session"]
    # Answer 3 times — the 3rd must issue tokens, not hit the cap.
    last = None
    for i in range(3):
        last = cognito_idp.respond_to_auth_challenge(
            ClientId=cid,
            ChallengeName="CUSTOM_CHALLENGE",
            Session=session,
            ChallengeResponses={"ANSWER": f"attempt{i}", "USERNAME": "user@example.com"},
        )
        session = last.get("Session", session)
    assert "AuthenticationResult" in last, last
    assert last["AuthenticationResult"].get("AccessToken")


# ── Test 30: Issue #725 reproduction ─────────────────────────────────────────

def test_custom_auth_issue_725_repro(cognito_idp, lam):
    """Exact reproduction from ministackorg/ministack#725 — exercises session[] and private-param carry-through."""
    define = (
        "def handler(event, ctx):\n"
        "    s = event['request']['session']\n"
        "    if not s:\n"
        "        event['response'].update(challengeName='CUSTOM_CHALLENGE', issueTokens=False, failAuthentication=False)\n"
        "    elif s[-1].get('challengeResult'):\n"
        "        event['response'].update(issueTokens=True, failAuthentication=False)\n"
        "    else:\n"
        "        event['response'].update(issueTokens=False, failAuthentication=True)\n"
        "    return event\n"
    )
    create = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'type': 'MAGIC_LINK'}\n"
        "    event['response']['privateChallengeParameters'] = {'answer': 'expected-token'}\n"
        "    event['response']['challengeMetadata'] = 'MAGIC_LINK'\n"
        "    return event\n"
    )
    verify = (
        "def handler(event, ctx):\n"
        "    expected = event['request']['privateChallengeParameters']['answer']\n"
        "    event['response']['answerCorrect'] = (event['request']['challengeAnswer'] == expected)\n"
        "    return event\n"
    )
    define_arn = _create_lambda(lam, "define-725", define)
    create_arn = _create_lambda(lam, "create-725", create)
    verify_arn = _create_lambda(lam, "verify-725", verify)

    pid = cognito_idp.create_user_pool(PoolName="repro-725", LambdaConfig={
        "DefineAuthChallenge": define_arn,
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
    })["UserPool"]["Id"]
    cid = cognito_idp.create_user_pool_client(
        UserPoolId=pid, ClientName="c",
        ExplicitAuthFlows=["ALLOW_CUSTOM_AUTH"],
    )["UserPoolClient"]["ClientId"]
    cognito_idp.admin_create_user(UserPoolId=pid, Username="alice", MessageAction="SUPPRESS")

    r1 = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "alice"},
    )
    assert r1["ChallengeName"] == "CUSTOM_CHALLENGE"
    assert r1["ChallengeParameters"]["type"] == "MAGIC_LINK"

    r2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=r1["Session"],
        ChallengeResponses={"USERNAME": "alice", "ANSWER": "expected-token"},
    )
    assert "AccessToken" in r2["AuthenticationResult"]


def test_custom_auth_issue_725_private_params_carry_through(cognito_idp, lam):
    """Verify that privateChallengeParameters round-trip from CreateAuthChallenge to VerifyAuthChallenge."""
    create = (
        "def handler(event, ctx):\n"
        "    event['response']['publicChallengeParameters'] = {'msg': 'send this to user'}\n"
        "    event['response']['privateChallengeParameters'] = {'secret': 'only-server-knows'}\n"
        "    return event\n"
    )
    verify = (
        "def handler(event, ctx):\n"
        "    # Verify handler can read privateChallengeParameters set by create\n"
        "    secret = event['request']['privateChallengeParameters'].get('secret', '')\n"
        "    answer = event['request']['challengeAnswer']\n"
        "    event['response']['answerCorrect'] = (secret == answer)\n"
        "    return event\n"
    )
    define = (
        "def handler(event, ctx):\n"
        "    if event['request']['session'] and event['request']['session'][-1].get('challengeResult'):\n"
        "        event['response']['issueTokens'] = True\n"
        "    else:\n"
        "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
        "    return event\n"
    )
    create_arn = _create_lambda(lam, "create-private-params", create)
    verify_arn = _create_lambda(lam, "verify-private-params", verify)
    define_arn = _create_lambda(lam, "define-private-params", define)

    pid, cid = _setup_pool(cognito_idp, "PrivateParamsPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    r1 = cognito_idp.initiate_auth(
        ClientId=cid,
        AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert r1["ChallengeParameters"]["msg"] == "send this to user"

    r2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=r1["Session"],
        ChallengeResponses={"ANSWER": "only-server-knows", "USERNAME": "user@example.com"},
    )
    assert "AccessToken" in r2["AuthenticationResult"]


# ── Regression: verify result is merged into the round it belongs to ──────────
#
# AWS records ONE ChallengeResult per CUSTOM_AUTH round, carrying BOTH the
# challengeMetadata (set by CreateAuthChallenge) AND the challengeResult (from
# VerifyAuthChallengeResponse). The triggers below model a real consumer
# (magic-link -> SMS-OTP) that identifies the completed step by reading BOTH
# fields from the SAME session element. With the prior split-entry behaviour the
# verify result landed in a second, metadata-less record, so the magic-link
# round was never recognised as complete and the SMS-OTP step never ran.

# CreateAuthChallenge: round 1 = MAGIC_LINK, round 2+ = SMS_OTP, with the step
# name carried in challengeMetadata (not just publicChallengeParameters).
_MERGE_CREATE_HANDLER = (
    "def handler(event, ctx):\n"
    "    answered = [c for c in event['request']['session']"
    " if c.get('challengeResult') is not None]\n"
    "    step = 'MAGIC_LINK' if len(answered) == 0 else 'SMS_OTP'\n"
    "    event['response']['publicChallengeParameters'] = {'challenge': step}\n"
    "    event['response']['challengeMetadata'] = step\n"
    "    return event\n"
)
_MERGE_VERIFY_HANDLER = (
    "def handler(event, ctx):\n"
    "    event['response']['answerCorrect'] = True\n"
    "    return event\n"
)
# DefineAuthChallenge advances ONLY when a round is a single merged entry:
# challengeResult truthy AND the expected challengeMetadata on the SAME element.
# Under the split-entry bug the MAGIC_LINK round is never 'done', so this falls
# through to failAuthentication and never reaches the SMS_OTP step.
_MERGE_DEFINE_HANDLER = (
    "def handler(event, ctx):\n"
    "    session = event['request']['session']\n"
    "    def done(meta):\n"
    "        return any(c.get('challengeResult') and c.get('challengeMetadata') == meta\n"
    "                   for c in session)\n"
    "    if not session:\n"
    "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
    "    elif done('MAGIC_LINK') and done('SMS_OTP'):\n"
    "        event['response']['issueTokens'] = True\n"
    "    elif done('MAGIC_LINK'):\n"
    "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
    "    else:\n"
    "        event['response']['failAuthentication'] = True\n"
    "    return event\n"
)


def test_custom_auth_merged_result_metadata_advances_steps(cognito_idp, lam):
    """A round's metadata and result live on one session entry, so a multi-step
    magic-link -> SMS-OTP flow completes (RespondToAuthChallenge path)."""
    create_arn = _create_lambda(lam, "create-merge", _MERGE_CREATE_HANDLER)
    verify_arn = _create_lambda(lam, "verify-merge", _MERGE_VERIFY_HANDLER)
    define_arn = _create_lambda(lam, "define-merge", _MERGE_DEFINE_HANDLER)

    pid, cid = _setup_pool(cognito_idp, "MergeResultPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    session = step1["Session"]

    # Round 1 (magic link). On the buggy split-entry shape this raises
    # NotAuthorizedException because the round is never recognised as complete.
    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "magic-link-token", "USERNAME": "user@example.com"},
    )
    assert step2.get("ChallengeName") == "CUSTOM_CHALLENGE"
    assert step2["ChallengeParameters"]["challenge"] == "SMS_OTP"

    # Round 2 (SMS OTP) -> tokens.
    step3 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step3
    assert step3["AuthenticationResult"].get("IdToken")


def test_custom_auth_admin_merged_result_metadata_advances_steps(cognito_idp, lam):
    """Same merged-round contract on the AdminRespondToAuthChallenge path."""
    create_arn = _create_lambda(lam, "create-merge-admin", _MERGE_CREATE_HANDLER)
    verify_arn = _create_lambda(lam, "verify-merge-admin", _MERGE_VERIFY_HANDLER)
    define_arn = _create_lambda(lam, "define-merge-admin", _MERGE_DEFINE_HANDLER)

    pid, cid = _setup_pool(cognito_idp, "MergeResultAdminPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid, ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    session = step1["Session"]

    step2 = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid, ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "magic-link-token", "USERNAME": "user@example.com"},
    )
    assert step2.get("ChallengeName") == "CUSTOM_CHALLENGE"
    assert step2["ChallengeParameters"]["challenge"] == "SMS_OTP"

    step3 = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid, ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step3
    assert step3["AuthenticationResult"].get("IdToken")


def test_update_pending_challenge_result_merge_and_fallback():
    """Unit-pin _update_pending_challenge_result's full contract: merge the
    result into the pending round in place (True or False, without growing the
    history), and fall back to appending only when there is no pending round."""
    import ministack.services.cognito as cognito_mod

    def _pending(metadata):
        return {
            "challengeName": "CUSTOM_CHALLENGE",
            "challengeResult": None,
            "challengeMetadata": metadata,
            "publicChallengeParameters": {},
            "privateChallengeParameters": {},
            "timestamp": 0,
        }

    # Correct answer merges into the pending round — no new entry, metadata kept.
    session = {"challenges": [_pending("MAGIC_LINK")], "last_challenge_metadata": "MAGIC_LINK"}
    cognito_mod._update_pending_challenge_result(session, True)
    assert len(session["challenges"]) == 1
    assert session["challenges"][0]["challengeResult"] is True
    assert session["challenges"][0]["challengeMetadata"] == "MAGIC_LINK"

    # Wrong answer is recorded in place as False — not None, not dropped.
    session = {"challenges": [_pending("SMS_OTP")], "last_challenge_metadata": "SMS_OTP"}
    cognito_mod._update_pending_challenge_result(session, False)
    assert len(session["challenges"]) == 1
    assert session["challenges"][0]["challengeResult"] is False

    # No pending round (empty history) — fall back to appending one entry.
    session = {"challenges": [], "last_challenge_metadata": None}
    cognito_mod._update_pending_challenge_result(session, True)
    assert len(session["challenges"]) == 1
    assert session["challenges"][0]["challengeName"] == "CUSTOM_CHALLENGE"
    assert session["challenges"][0]["challengeResult"] is True
    assert session["challenges"][0]["challengeMetadata"] is None

    # Last round already resolved — append a new entry, leave the prior intact.
    resolved = dict(_pending("MAGIC_LINK"), challengeResult=True)
    session = {"challenges": [resolved], "last_challenge_metadata": "MAGIC_LINK"}
    cognito_mod._update_pending_challenge_result(session, False)
    assert len(session["challenges"]) == 2
    assert session["challenges"][0]["challengeResult"] is True
    assert session["challenges"][0]["challengeMetadata"] == "MAGIC_LINK"
    assert session["challenges"][1]["challengeResult"] is False


# ── Regression: CreateAuthChallenge sees the AWS-faithful session length ───────
#
# AWS passes an EMPTY session array to the FIRST CreateAuthChallenge (the round
# being created is not itself a session entry), then only COMPLETED rounds on
# later invocations. AWS's own trigger examples branch on session.length, so the
# observed length must match. The handler echoes the raw len it sees each round.
_LEN_CREATE_HANDLER = (
    "def handler(event, ctx):\n"
    "    n = len(event['request']['session'])\n"
    "    step = 'MAGIC_LINK' if n == 0 else 'SMS_OTP'\n"
    "    event['response']['publicChallengeParameters'] = {'challenge': step, 'rawlen': str(n)}\n"
    "    return event\n"
)
_LEN_DEFINE_HANDLER = (
    "def handler(event, ctx):\n"
    "    answered = [c for c in event['request']['session'] if c.get('challengeResult') is not None]\n"
    "    if len(answered) >= 2:\n"
    "        event['response']['issueTokens'] = True\n"
    "    else:\n"
    "        event['response']['challengeName'] = 'CUSTOM_CHALLENGE'\n"
    "    return event\n"
)


def test_custom_auth_first_create_receives_empty_session(cognito_idp, lam):
    """Round-1 CreateAuthChallenge sees an empty session (len 0); round-2 sees
    one completed round (len 1) — RespondToAuthChallenge path."""
    create_arn = _create_lambda(lam, "create-len", _LEN_CREATE_HANDLER)
    verify_arn = _create_lambda(lam, "verify-len", _MERGE_VERIFY_HANDLER)
    define_arn = _create_lambda(lam, "define-len", _LEN_DEFINE_HANDLER)

    pid, cid = _setup_pool(cognito_idp, "SessionLenPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.initiate_auth(
        ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["rawlen"] == "0"
    assert step1["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    session = step1["Session"]

    step2 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "magic-link-token", "USERNAME": "user@example.com"},
    )
    assert step2["ChallengeParameters"]["rawlen"] == "1"
    assert step2["ChallengeParameters"]["challenge"] == "SMS_OTP"

    step3 = cognito_idp.respond_to_auth_challenge(
        ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step3


def test_custom_auth_admin_first_create_receives_empty_session(cognito_idp, lam):
    """Same empty-first-session contract on the AdminInitiateAuth path."""
    create_arn = _create_lambda(lam, "create-len-admin", _LEN_CREATE_HANDLER)
    verify_arn = _create_lambda(lam, "verify-len-admin", _MERGE_VERIFY_HANDLER)
    define_arn = _create_lambda(lam, "define-len-admin", _LEN_DEFINE_HANDLER)

    pid, cid = _setup_pool(cognito_idp, "SessionLenAdminPool", {
        "CreateAuthChallenge": create_arn,
        "VerifyAuthChallengeResponse": verify_arn,
        "DefineAuthChallenge": define_arn,
    })

    step1 = cognito_idp.admin_initiate_auth(
        UserPoolId=pid, ClientId=cid, AuthFlow="CUSTOM_AUTH",
        AuthParameters={"USERNAME": "user@example.com"},
    )
    assert step1["ChallengeParameters"]["rawlen"] == "0"
    assert step1["ChallengeParameters"]["challenge"] == "MAGIC_LINK"
    session = step1["Session"]

    step2 = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid, ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "magic-link-token", "USERNAME": "user@example.com"},
    )
    assert step2["ChallengeParameters"]["rawlen"] == "1"
    assert step2["ChallengeParameters"]["challenge"] == "SMS_OTP"

    step3 = cognito_idp.admin_respond_to_auth_challenge(
        UserPoolId=pid, ClientId=cid,
        ChallengeName="CUSTOM_CHALLENGE",
        Session=session,
        ChallengeResponses={"ANSWER": "123456", "USERNAME": "user@example.com"},
    )
    assert "AuthenticationResult" in step3
