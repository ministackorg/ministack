import datetime as dt

import pytest

from ministack.core.sigv4 import (
    build_canonical_headers,
    build_canonical_query_string,
    build_canonical_request,
    build_string_to_sign,
    calculate_signature,
    derive_signing_key,
    presigned_request_is_expired,
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
