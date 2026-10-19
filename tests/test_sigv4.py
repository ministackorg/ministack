import datetime as dt
import hashlib

import pytest

from ministack.core.sigv4 import (
    build_canonical_headers,
    build_canonical_query_string,
    build_canonical_request,
    build_string_to_sign,
    calculate_signature,
    derive_signing_key,
    header_signature_failure,
    parse_header_signature,
    parse_query_signature,
    presigned_request_is_expired,
    request_time_failure,
    signatures_match,
    uri_encode,
)


@pytest.mark.parametrize(
    "value, encode_slash, expected",
    [
        ("abcXYZ012-_.~", True, "abcXYZ012-_.~"),
        ("space + percent%", True, "space%20%2B%20percent%25"),
        ("folder/object", True, "folder%2Fobject"),
        ("folder/object", False, "folder/object"),
        ("caf\N{LATIN SMALL LETTER E WITH ACUTE}", True, "caf%C3%A9"),
    ],
)
def test_uri_encode_uses_sigv4_rules(value, encode_slash, expected):
    assert uri_encode(value, encode_slash=encode_slash) == expected


def test_build_canonical_query_string_encodes_sorts_and_excludes_signature():
    query_params = {
        "space key": ["b/a", "a+b"],
        "Action": "connect",
        "X-Amz-Signature": "not-part-of-the-canonical-query",
    }

    assert build_canonical_query_string(query_params) == (
        "Action=connect&space%20key=a%2Bb&space%20key=b%2Fa"
    )


def test_build_canonical_query_string_accepts_custom_exclusions():
    query_params = {"Action": "connect", "DBUser": "alice", "Nonce": "123"}

    assert build_canonical_query_string(query_params, exclude=("nonce",)) == "Action=connect&DBUser=alice"


def test_build_canonical_headers_normalizes_names_and_values():
    headers = {
        "host": "examplebucket.s3.amazonaws.com",
        "x-custom": "  spaced\t  value ",
    }

    assert build_canonical_headers(headers, "host;x-custom") == (
        "host:examplebucket.s3.amazonaws.com\n"
        "x-custom:spaced value\n"
    )


def test_build_canonical_request_matches_fixed_vector():
    query_params = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": "AKIDEXAMPLE/20130524/us-east-1/s3/aws4_request",
        "X-Amz-Date": "20130524T000000Z",
        "X-Amz-Expires": "86400",
        "X-Amz-SignedHeaders": "host",
        "X-Amz-Signature": "excluded",
    }

    assert build_canonical_request(
        "GET",
        "/test.txt",
        {"host": "examplebucket.s3.amazonaws.com"},
        query_params,
        "host",
    ) == (
        "GET\n"
        "/test.txt\n"
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&"
        "X-Amz-Credential=AKIDEXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&"
        "X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\n"
        "host:examplebucket.s3.amazonaws.com\n"
        "\n"
        "host\n"
        "UNSIGNED-PAYLOAD"
    )


def test_signing_helpers_match_botocore_fixed_vector():
    canonical_request = (
        "GET\n"
        "/test.txt\n"
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&"
        "X-Amz-Credential=AKIDEXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&"
        "X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\n"
        "host:examplebucket.s3.amazonaws.com\n"
        "\n"
        "host\n"
        "UNSIGNED-PAYLOAD"
    )
    string_to_sign = build_string_to_sign(
        "20130524T000000Z",
        "20130524",
        "us-east-1",
        "s3",
        canonical_request,
    )

    assert string_to_sign == (
        "AWS4-HMAC-SHA256\n"
        "20130524T000000Z\n"
        "20130524/us-east-1/s3/aws4_request\n"
        "fe76c9a452b5c779479d88b7efe53bc3935d1a56dd76e83e930f401e91272d73"
    )
    assert derive_signing_key(
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        "20130524",
        "us-east-1",
        "s3",
    ).hex() == "f117494eff5d09da21cbf7f0339559ea04fc9582d31299cb992be70a6b27c97a"
    assert calculate_signature(
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        "20130524",
        "us-east-1",
        "s3",
        string_to_sign,
    ) == "ca6159ff16837c055653a722d9f10b6a529b7c62c84174a2859958324bc78766"


def test_presigned_request_expiry_is_exclusive_at_boundary():
    expires_at = dt.datetime(2026, 9, 4, 12, 15, tzinfo=dt.timezone.utc)

    assert not presigned_request_is_expired("20260904T120000Z", "900", now=expires_at)
    assert presigned_request_is_expired(
        "20260904T120000Z",
        "900",
        now=expires_at + dt.timedelta(microseconds=1),
    )


@pytest.mark.parametrize(
    "amz_date, expires",
    [
        ("not-a-date", "900"),
        ("20260904T120000Z", "not-a-duration"),
    ],
)
def test_presigned_request_expiry_rejects_malformed_values(amz_date, expires):
    with pytest.raises(ValueError):
        presigned_request_is_expired(amz_date, expires)


def test_signatures_match_accepts_only_equal_values():
    assert signatures_match("abc123", "abc123")
    assert not signatures_match("abc123", "abc124")


_SCOPE = "AKIAIOSFODNN7EXAMPLE/20260930/eu-central-1/sts/aws4_request"


@pytest.mark.parametrize(
    "authorization, code, message",
    [
        (
            f"AWS4-HMAC-SHA256 Credential={_SCOPE}, Signature=abc",
            "MissingAuthorizationParameter",
            "Authorization header requires 'SignedHeaders' parameter. (Hashed with SHA-256 and encoded with Base64) "
            "Authorization=NfAZ3F0AUnKOtT3DAo6CWylCcBLuRNI5BHzk96CvGwM=",
        ),
        (
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/eu-central-1/sts/aws4_request, "
            "SignedHeaders=host;x-amz-date, Signature=abc",
            "IncompleteSignature",
            "Credential must have exactly 5 slash-delimited elements, e.g. keyid/date/region/service/term, "
            "got 'AKIAIOSFODNN7EXAMPLE/eu-central-1/sts/aws4_request'",
        ),
        (
            "AWS4-HMAC-SHA256 SignedHeaders=host;x-amz-date, Signature=abc",
            "MissingAuthorizationParameter",
            "Authorization header requires 'Credential' parameter. (Hashed with SHA-256 and encoded with Base64) "
            "Authorization=ahJcBrcKAe4SBJto5I/CIJQro+xeLn5ihFrmUmERRpY=",
        ),
        (
            "AWS4-HMAC-SHA256 Credential=, SignedHeaders=host;x-amz-date, Signature=abc",
            "IncompleteSignature",
            "Authorization header requires 'Credential' parameter. (Hashed with SHA-256 and encoded with Base64) "
            "Authorization=Rso9P5e0/U0V+vZQW+PsjFTI/EYgUn2YbmHes/SFYPA=",
        ),
        (
            "AWS4-HMAC-SHA256",
            "MissingAuthorizationParameter",
            "Authorization header requires 'Credential' parameter. Authorization header requires 'Signature' "
            "parameter. Authorization header requires 'SignedHeaders' parameter. (Hashed with SHA-256 and encoded "
            "with Base64) Authorization=YI2YWea8FSxnksZL2ETHbs5elHm9HQufxTLK0W0ffV0=",
        ),
    ],
)
def test_parse_header_signature_names_the_missing_part_as_aws_does(authorization, code, message):
    headers = {"authorization": authorization, "x-amz-date": "20260930T120000Z"}

    assert parse_header_signature(headers) == (code, message)


def test_parse_header_signature_takes_the_date_header_when_x_amz_date_is_absent():
    headers = {
        "authorization": f"AWS4-HMAC-SHA256 Credential={_SCOPE}, SignedHeaders=date;host, Signature=abc",
        "date": "Wed, 30 Sep 2026 12:00:00 GMT",
    }

    parsed = parse_header_signature(headers)

    assert (parsed.access_key_id, parsed.service, parsed.amz_date) == (
        "AKIAIOSFODNN7EXAMPLE", "sts", "20260930T120000Z")


def test_parse_header_signature_names_a_missing_date_with_the_missing_parameters():
    headers = {"authorization": "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=abc"}

    assert parse_header_signature(headers) == (
        "MissingAuthorizationParameter",
        "Authorization header requires 'Credential' parameter. Authorization header requires existence of either "
        "a 'X-Amz-Date' or a 'Date' header. (Hashed with SHA-256 and encoded with Base64) "
        "Authorization=QtbsE59+Cww86WuNzDL08sE4JUxLk21/H/rRcBdVXrQ=",
    )


@pytest.mark.parametrize("query, code, missing", [
    ({"X-Amz-Algorithm": ["AWS4-HMAC-SHA256"], "X-Amz-Date": ["20260930T120000Z"],
      "X-Amz-SignedHeaders": ["host"], "X-Amz-Signature": ["abc"]},
     "MissingAuthorizationParameter", ["X-Amz-Credential"]),
    ({"X-Amz-Algorithm": ["AWS4-HMAC-SHA256"], "X-Amz-Credential": [""], "X-Amz-Date": ["20260930T120000Z"],
      "X-Amz-SignedHeaders": ["host"], "X-Amz-Signature": ["abc"]},
     "IncompleteSignature", ["X-Amz-Credential"]),
    ({"X-Amz-Algorithm": ["AWS4-HMAC-SHA256"]},
     "MissingAuthorizationParameter", ["X-Amz-Credential", "X-Amz-Signature", "X-Amz-SignedHeaders", "X-Amz-Date"]),
], ids=["no-credential", "empty-credential", "only-algorithm"])
def test_parse_query_signature_names_every_missing_parameter(query, code, missing):
    assert parse_query_signature(query) == (code, " ".join(
        [f"AWS query-string parameters must include '{name}'." for name in missing]
        + ["Re-examine the query-string parameters."]))


def _presigned(secret, token_signed, service="iotdevicegateway"):
    """Query parameters of a GET /mqtt presigned for ``service``, with its session token."""
    amz_date = "20260930T120000Z"
    query = {"X-Amz-Algorithm": ["AWS4-HMAC-SHA256"],
             "X-Amz-Credential": [f"ASIAEXAMPLE/20260930/us-east-1/{service}/aws4_request"],
             "X-Amz-Date": [amz_date], "X-Amz-SignedHeaders": ["host"]}
    if token_signed:
        query["X-Amz-Security-Token"] = ["token/+="]
    canonical = build_canonical_request("GET", "/mqtt", {"host": "iot.example"}, query, "host",
                                        payload_hash=hashlib.sha256(b"").hexdigest())
    string_to_sign = build_string_to_sign(amz_date, amz_date[:8], "us-east-1", service, canonical)
    query["X-Amz-Signature"] = [calculate_signature(secret, amz_date[:8], "us-east-1", service, string_to_sign)]
    query["X-Amz-Security-Token"] = ["token/+="]
    return query


@pytest.mark.parametrize("service, token_signed, expected", [
    ("iotdevicegateway", False, None),
    ("iotdevicegateway", True, "SignatureDoesNotMatch"),
    ("execute-api", True, None),
    ("execute-api", False, "SignatureDoesNotMatch"),
])
def test_presigned_iot_url_leaves_the_session_token_unsigned(service, token_signed, expected):
    query = _presigned("secret", token_signed, service)
    signature = parse_query_signature(query)

    failure = header_signature_failure(signature, "secret", "GET", "/mqtt", query, {"host": "iot.example"}, b"",
                                       presigned=True)

    assert (failure and failure[0]) == expected


def test_signature_failure_can_name_the_strings_it_expected():
    query = _presigned("secret", True, "execute-api")
    signature = parse_query_signature(query)

    code, message = header_signature_failure(signature, "other", "GET", "/mqtt", query, {"host": "iot.example"}, b"",
                                             presigned=True, explain=True)

    canonical = (
        "GET\n/mqtt\nX-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAEXAMPLE%2F20260930%2Fus-east-1"
        "%2Fexecute-api%2Faws4_request&X-Amz-Date=20260930T120000Z&X-Amz-Security-Token=token%2F%2B%3D"
        f"&X-Amz-SignedHeaders=host\nhost:iot.example\n\nhost\n{hashlib.sha256(b'').hexdigest()}"
    )
    string_to_sign = build_string_to_sign("20260930T120000Z", "20260930", "us-east-1", "execute-api", canonical)
    assert code == "SignatureDoesNotMatch"
    assert message == (
        "The request signature we calculated does not match the signature you provided. Check your AWS Secret "
        "Access Key and signing method. Consult the service documentation for details.\n\n"
        f"The Canonical String for this request should have been\n'{canonical}'\n\n"
        f"The String-to-Sign should have been\n'{string_to_sign}'\n"
    )


@pytest.mark.parametrize(
    "service, amz_date, expected",
    [
        ("kms", "20260930T120500Z", None),
        ("kms", "20260930T120501Z", "Signature not yet current: 20260930T120501Z is still later than "
                                    "20260930T120500Z (20260930T120000Z + 5 min.)"),
        ("sts", "20260930T114500Z", None),
        ("sts", "20260930T114459Z", "Signature expired: 20260930T114459Z is now earlier than "
                                    "20260930T114500Z (20260930T120000Z - 15 min.)"),
    ],
)
def test_request_time_failure_uses_the_services_window(service, amz_date, expected):
    headers = {
        "authorization": f"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/{amz_date[:8]}/us-east-1/{service}/aws4_request, "
                         "SignedHeaders=host;x-amz-date, Signature=abc",
        "x-amz-date": amz_date,
    }
    now = dt.datetime(2026, 9, 30, 12, 0, tzinfo=dt.timezone.utc)

    failure = request_time_failure(parse_header_signature(headers), now=now)

    assert failure == (expected and ("RequestTimeTooSkewed", expected))
