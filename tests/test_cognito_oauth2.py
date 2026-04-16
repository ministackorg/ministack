"""
Integration tests for Cognito OAuth2/OIDC IdP endpoints.

Tests the full OAuth2 authorization code flow including:
  /oauth2/authorize, /login, /oauth2/token, /oauth2/userInfo, /logout
"""
import base64
import hashlib
import json
import os
import secrets
import urllib.error
import urllib.parse
import urllib.request

from conftest import ENDPOINT, make_client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_pool_with_user(cognito_idp, generate_secret=True):
    """Create a user pool with a confirmed user and an OAuth-enabled client."""
    pool = cognito_idp.create_user_pool(PoolName='OAuth2TestPool')
    pool_id = pool['UserPool']['Id']

    client_kwargs = {
        'UserPoolId': pool_id,
        'ClientName': 'oauth2-test-client',
        'GenerateSecret': generate_secret,
        'AllowedOAuthFlows': ['code'],
        'AllowedOAuthScopes': ['openid', 'email', 'profile'],
        'AllowedOAuthFlowsUserPoolClient': True,
        'CallbackURLs': ['http://localhost:3000/callback'],
        'LogoutURLs': ['http://localhost:3000/logout'],
        'DefaultRedirectURI': 'http://localhost:3000/callback',
        'ExplicitAuthFlows': ['ALLOW_USER_PASSWORD_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
    }
    client_resp = cognito_idp.create_user_pool_client(**client_kwargs)
    client = client_resp['UserPoolClient']

    cognito_idp.admin_create_user(
        UserPoolId=pool_id,
        Username='testuser',
        TemporaryPassword='TempPass1!',
        UserAttributes=[
            {'Name': 'email', 'Value': 'test@example.com'},
            {'Name': 'email_verified', 'Value': 'true'},
            {'Name': 'name', 'Value': 'Test User'},
        ],
    )
    cognito_idp.admin_set_user_password(
        UserPoolId=pool_id, Username='testuser', Password='TestPass1!', Permanent=True,
    )

    return pool_id, client


def _lower_headers(h):
    """Return a plain dict with all header names lowercased."""
    return {k.lower(): v for k, v in h.items()}


def _get(url, follow_redirects=True):
    """GET request, optionally not following redirects."""
    req = urllib.request.Request(url, method='GET')
    if not follow_redirects:
        opener = urllib.request.build_opener(_NoRedirectHandler)
    else:
        opener = urllib.request.build_opener()
    try:
        resp = opener.open(req, timeout=10)
        return resp.status, _lower_headers(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, _lower_headers(e.headers), e.read()


def _post_form(url, data, headers=None, follow_redirects=True):
    """POST form-encoded data."""
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(url, data=body, method='POST')
    req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    if not follow_redirects:
        opener = urllib.request.build_opener(_NoRedirectHandler)
    else:
        opener = urllib.request.build_opener()
    try:
        resp = opener.open(req, timeout=10)
        return resp.status, _lower_headers(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, _lower_headers(e.headers), e.read()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(newurl, code, msg, headers, fp)


# ---------------------------------------------------------------------------
# Tests — /oauth2/authorize
# ---------------------------------------------------------------------------

def test_oauth2_authorize_shows_login_form():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    url = (f'{ENDPOINT}/oauth2/authorize?response_type=code'
           f'&client_id={client_id}'
           f'&redirect_uri=http://localhost:3000/callback'
           f'&scope=openid+email'
           f'&state=abc123')
    status, headers, body = _get(url)
    html = body.decode('utf-8')

    assert status == 200
    assert 'text/html' in headers.get('content-type', '')
    assert '<form' in html
    assert 'username' in html
    assert 'password' in html
    assert client_id in html


def test_oauth2_authorize_invalid_client():
    url = (f'{ENDPOINT}/oauth2/authorize?response_type=code'
           f'&client_id=nonexistent'
           f'&redirect_uri=http://localhost:3000/callback')
    status, headers, body = _get(url)
    resp = json.loads(body)

    assert status == 400
    assert resp['error'] == 'invalid_client'


def test_oauth2_authorize_invalid_redirect_uri():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    url = (f'{ENDPOINT}/oauth2/authorize?response_type=code'
           f'&client_id={client_id}'
           f'&redirect_uri=http://evil.com/callback')
    status, headers, body = _get(url)
    resp = json.loads(body)

    assert status == 400
    assert resp['error'] == 'invalid_request'


def test_oauth2_authorize_unsupported_response_type():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    url = (f'{ENDPOINT}/oauth2/authorize?response_type=token'
           f'&client_id={client_id}'
           f'&redirect_uri=http://localhost:3000/callback')
    status, headers, body = _get(url)
    resp = json.loads(body)

    assert status == 400
    assert resp['error'] == 'unsupported_response_type'


# ---------------------------------------------------------------------------
# Tests — /login
# ---------------------------------------------------------------------------

def test_oauth2_login_success_redirects_with_code():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'testuser',
            'password': 'TestPass1!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid email',
            'state': 'mystate',
            'response_type': 'code',
        },
        follow_redirects=False,
    )

    assert status == 302
    location = headers.get('location', '')
    assert location.startswith('http://localhost:3000/callback')
    parsed = urllib.parse.urlparse(location)
    qs = urllib.parse.parse_qs(parsed.query)
    assert 'code' in qs
    assert qs['state'] == ['mystate']


def test_oauth2_login_failure_shows_error():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'testuser',
            'password': 'WrongPass!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid',
            'state': 'xyz',
            'response_type': 'code',
        },
    )

    assert status == 200
    html = body.decode('utf-8')
    assert 'Incorrect username or password' in html


# ---------------------------------------------------------------------------
# Tests — /oauth2/token
# ---------------------------------------------------------------------------

def _do_login_and_get_code(cognito_idp, client_id, extra_form=None):
    """Helper: submit login form, return the authorization code."""
    form = {
        'username': 'testuser',
        'password': 'TestPass1!',
        'client_id': client_id,
        'redirect_uri': 'http://localhost:3000/callback',
        'scope': 'openid email',
        'state': 'test',
        'response_type': 'code',
    }
    if extra_form:
        form.update(extra_form)
    status, headers, body = _post_form(f'{ENDPOINT}/login', form, follow_redirects=False)
    assert status == 302
    location = headers.get('location', '')
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
    return qs['code'][0]


def test_oauth2_token_authorization_code():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')
    code = _do_login_and_get_code(cognito_idp, client_id)

    status, headers, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })

    assert status == 200
    resp = json.loads(body)
    assert 'access_token' in resp
    assert 'id_token' in resp
    assert 'refresh_token' in resp
    assert resp['token_type'] == 'Bearer'
    assert resp['expires_in'] == 3600


def test_oauth2_token_authorization_code_with_pkce():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, generate_secret=False)
    client_id = client['ClientId']

    # Generate PKCE pair
    code_verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(code_verifier.encode('ascii')).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b'=').decode('ascii')

    code = _do_login_and_get_code(cognito_idp, client_id, {
        'code_challenge': code_challenge,
        'code_challenge_method': 'S256',
    })

    status, headers, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'code_verifier': code_verifier,
    })

    assert status == 200
    resp = json.loads(body)
    assert 'access_token' in resp
    assert 'id_token' in resp


def test_oauth2_token_invalid_pkce_verifier():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, generate_secret=False)
    client_id = client['ClientId']

    code_verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(code_verifier.encode('ascii')).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b'=').decode('ascii')

    code = _do_login_and_get_code(cognito_idp, client_id, {
        'code_challenge': code_challenge,
        'code_challenge_method': 'S256',
    })

    status, headers, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'code_verifier': 'wrong-verifier',
    })

    assert status == 400
    resp = json.loads(body)
    assert resp['error'] == 'invalid_grant'


def test_oauth2_token_code_reuse():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')
    code = _do_login_and_get_code(cognito_idp, client_id)

    # First use — should succeed
    status1, _, body1 = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status1 == 200

    # Second use — should fail
    status2, _, body2 = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status2 == 400
    resp2 = json.loads(body2)
    assert resp2['error'] == 'invalid_grant'


def test_oauth2_token_refresh_token():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')
    code = _do_login_and_get_code(cognito_idp, client_id)

    # Get initial tokens
    status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status == 200
    tokens = json.loads(body)
    refresh_token = tokens['refresh_token']

    # Refresh
    status2, _, body2 = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status2 == 200
    resp2 = json.loads(body2)
    assert 'access_token' in resp2
    assert 'id_token' in resp2


def test_oauth2_token_client_credentials():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, generate_secret=True)
    client_id = client['ClientId']
    client_secret = client['ClientSecret']

    status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'openid',
    })

    assert status == 200
    resp = json.loads(body)
    assert 'access_token' in resp
    assert resp['token_type'] == 'Bearer'
    # client_credentials should NOT return id_token or refresh_token
    assert 'id_token' not in resp
    assert 'refresh_token' not in resp


def test_oauth2_token_client_auth_basic():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp, generate_secret=True)
    client_id = client['ClientId']
    client_secret = client['ClientSecret']

    basic = base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()

    status, _, body = _post_form(
        f'{ENDPOINT}/oauth2/token',
        {
            'grant_type': 'client_credentials',
            'scope': 'openid',
        },
        headers={'Authorization': f'Basic {basic}'},
    )

    assert status == 200
    resp = json.loads(body)
    assert 'access_token' in resp


# ---------------------------------------------------------------------------
# Tests — /oauth2/userInfo
# ---------------------------------------------------------------------------

def test_oauth2_userinfo():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')
    code = _do_login_and_get_code(cognito_idp, client_id)

    # Get tokens
    _, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    tokens = json.loads(body)
    access_token = tokens['access_token']

    # Call userInfo
    req = urllib.request.Request(
        f'{ENDPOINT}/oauth2/userInfo',
        headers={'Authorization': f'Bearer {access_token}'},
    )
    resp = urllib.request.urlopen(req, timeout=10)
    assert resp.status == 200
    claims = json.loads(resp.read())

    assert 'sub' in claims
    assert claims.get('email') == 'test@example.com'
    assert claims.get('cognito:username') == 'testuser'
    assert claims.get('name') == 'Test User'


def test_oauth2_userinfo_invalid_token():
    req = urllib.request.Request(
        f'{ENDPOINT}/oauth2/userInfo',
        headers={'Authorization': 'Bearer invalid-token'},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        assert False, 'Expected 401'
    except urllib.error.HTTPError as e:
        assert e.code == 401
        resp = json.loads(e.read())
        assert resp['error'] == 'invalid_token'


def test_oauth2_userinfo_missing_token():
    req = urllib.request.Request(f'{ENDPOINT}/oauth2/userInfo')
    try:
        urllib.request.urlopen(req, timeout=10)
        assert False, 'Expected 401'
    except urllib.error.HTTPError as e:
        assert e.code == 401


# ---------------------------------------------------------------------------
# Tests — /logout
# ---------------------------------------------------------------------------

def test_oauth2_logout_redirects():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    url = (f'{ENDPOINT}/logout'
           f'?client_id={client_id}'
           f'&logout_uri=http://localhost:3000/logout')
    status, headers, body = _get(url, follow_redirects=False)

    assert status == 302
    assert headers.get('location', '') == 'http://localhost:3000/logout'


def test_oauth2_logout_invalid_uri():
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']

    url = (f'{ENDPOINT}/logout'
           f'?client_id={client_id}'
           f'&logout_uri=http://evil.com/logout')
    status, headers, body = _get(url, follow_redirects=False)

    assert status == 400
    resp = json.loads(body)
    assert resp['error'] == 'invalid_request'


# ---------------------------------------------------------------------------
# Tests — E2E flow
# ---------------------------------------------------------------------------

def test_oauth2_full_flow():
    """End-to-end: authorize -> login -> token -> userInfo."""
    cognito_idp = make_client('cognito-idp')
    pool_id, client = _setup_pool_with_user(cognito_idp)
    client_id = client['ClientId']
    client_secret = client.get('ClientSecret', '')

    # 1. GET /oauth2/authorize — get login form
    url = (f'{ENDPOINT}/oauth2/authorize?response_type=code'
           f'&client_id={client_id}'
           f'&redirect_uri=http://localhost:3000/callback'
           f'&scope=openid+email'
           f'&state=e2e-state')
    status, headers, body = _get(url)
    assert status == 200
    assert '<form' in body.decode('utf-8')

    # 2. POST /login — submit credentials
    status, headers, body = _post_form(
        f'{ENDPOINT}/login',
        {
            'username': 'testuser',
            'password': 'TestPass1!',
            'client_id': client_id,
            'redirect_uri': 'http://localhost:3000/callback',
            'scope': 'openid email',
            'state': 'e2e-state',
            'response_type': 'code',
        },
        follow_redirects=False,
    )
    assert status == 302
    location = headers.get('location', '')
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
    code = qs['code'][0]
    assert qs['state'] == ['e2e-state']

    # 3. POST /oauth2/token — exchange code for tokens
    status, _, body = _post_form(f'{ENDPOINT}/oauth2/token', {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': 'http://localhost:3000/callback',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    assert status == 200
    tokens = json.loads(body)
    assert 'access_token' in tokens
    assert 'id_token' in tokens
    assert 'refresh_token' in tokens

    # 4. GET /oauth2/userInfo — verify user claims
    req = urllib.request.Request(
        f'{ENDPOINT}/oauth2/userInfo',
        headers={'Authorization': f'Bearer {tokens["access_token"]}'},
    )
    resp = urllib.request.urlopen(req, timeout=10)
    claims = json.loads(resp.read())
    assert claims['email'] == 'test@example.com'
    assert claims['cognito:username'] == 'testuser'

    # 5. GET /logout — redirect to logout URI
    logout_url = (f'{ENDPOINT}/logout'
                  f'?client_id={client_id}'
                  f'&logout_uri=http://localhost:3000/logout')
    status, headers, _ = _get(logout_url, follow_redirects=False)
    assert status == 302
    assert headers.get('location', '') == 'http://localhost:3000/logout'
