import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _regional_kms(region):
    return boto3.client(
        "kms",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


def test_kms_create_symmetric_key(kms_client):
    resp = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT",
        KeyUsage="ENCRYPT_DECRYPT",
        Description="test symmetric key",
        Tags=[{"TagKey": "env", "TagValue": "test"}],
        Policy="{}",
    )
    meta = resp["KeyMetadata"]
    assert meta["KeyId"]
    assert meta["Arn"].startswith("arn:aws:kms:")
    assert meta["KeySpec"] == "SYMMETRIC_DEFAULT"
    assert meta["KeyUsage"] == "ENCRYPT_DECRYPT"
    assert meta["Enabled"] is True
    assert meta["KeyState"] == "Enabled"
    assert meta["Description"] == "test symmetric key"

    tags = kms_client.list_resource_tags(KeyId=meta["KeyId"])["Tags"]
    assert {"TagKey": "env", "TagValue": "test"} in tags

    policy = kms_client.get_key_policy(KeyId=meta["KeyId"], PolicyName="default")["Policy"]
    assert policy == "{}"

def test_kms_create_rsa_2048_sign_key(kms_client):
    resp = kms_client.create_key(
        KeySpec="RSA_2048",
        KeyUsage="SIGN_VERIFY",
        Description="test RSA signing key",
    )
    meta = resp["KeyMetadata"]
    assert meta["KeySpec"] == "RSA_2048"
    assert meta["KeyUsage"] == "SIGN_VERIFY"
    assert "RSASSA_PKCS1_V1_5_SHA_256" in meta["SigningAlgorithms"]

def test_kms_create_rsa_4096_encrypt_key(kms_client):
    resp = kms_client.create_key(
        KeySpec="RSA_4096",
        KeyUsage="ENCRYPT_DECRYPT",
    )
    meta = resp["KeyMetadata"]
    assert meta["KeySpec"] == "RSA_4096"
    assert "RSAES_OAEP_SHA_256" in meta["EncryptionAlgorithms"]

def test_kms_list_keys(kms_client):
    created = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    key_id = created["KeyMetadata"]["KeyId"]
    resp = kms_client.list_keys()
    key_ids = [k["KeyId"] for k in resp["Keys"]]
    assert key_id in key_ids

def test_kms_describe_key(kms_client):
    created = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", Description="describe me"
    )
    key_id = created["KeyMetadata"]["KeyId"]
    resp = kms_client.describe_key(KeyId=key_id)
    assert resp["KeyMetadata"]["Description"] == "describe me"
    assert resp["KeyMetadata"]["KeyId"] == key_id

def test_kms_describe_key_by_arn(kms_client):
    created = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    arn = created["KeyMetadata"]["Arn"]
    resp = kms_client.describe_key(KeyId=arn)
    assert resp["KeyMetadata"]["Arn"] == arn


def test_kms_key_arn_resolution_rejects_wrong_scope(kms_client):
    created = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    arn = created["KeyMetadata"]["Arn"]
    invalid_cases = [
        arn.replace(":000000000000:", ":111111111111:"),
        arn.replace(":us-east-1:", ":us-west-2:"),
        arn.replace(":kms:", ":sqs:"),
        arn.replace(":key/", ":alias/"),
    ]

    for key_id in invalid_cases:
        with pytest.raises(ClientError) as exc:
            kms_client.describe_key(KeyId=key_id)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_kms_key_arn_resolution_rejects_forged_request_region(kms_client):
    created = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    arn = created["KeyMetadata"]["Arn"]
    west_arn = arn.replace(":us-east-1:", ":us-west-2:")
    west_kms = _regional_kms("us-west-2")

    with pytest.raises(ClientError) as exc:
        west_kms.describe_key(KeyId=west_arn)
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_kms_keys_are_region_scoped(kms_client):
    created = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    key_id = created["KeyMetadata"]["KeyId"]
    west_kms = _regional_kms("us-west-2")

    west_key_ids = {k["KeyId"] for k in west_kms.list_keys()["Keys"]}
    assert key_id not in west_key_ids

    with pytest.raises(ClientError) as exc:
        west_kms.describe_key(KeyId=key_id)
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

    assert kms_client.describe_key(KeyId=key_id)["KeyMetadata"]["KeyId"] == key_id


def test_kms_cross_region_key_id_resolves_notfound(kms_client):
    created = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    key_id = created["KeyMetadata"]["KeyId"]
    west_kms = _regional_kms("us-west-2")

    for call in (
        lambda: west_kms.describe_key(KeyId=key_id),
        lambda: west_kms.encrypt(KeyId=key_id, Plaintext=b"cross-region"),
    ):
        with pytest.raises(ClientError) as exc:
            call()
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_kms_describe_nonexistent_key(kms_client):
    with pytest.raises(ClientError) as exc_info:
        kms_client.describe_key(KeyId="nonexistent-key-id")
    assert "NotFoundException" in str(exc_info.value)
    # Real AWS sends `x-amzn-errortype` on JSON-protocol errors. Java/Go SDK v2
    # read it; without it they raise SdkClientException(unknown error type).
    assert exc_info.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "NotFoundException"

def test_kms_sign_and_verify_pkcs1(kms_client):
    key = kms_client.create_key(KeySpec="RSA_2048", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]
    message = b"header.payload"

    sign_resp = kms_client.sign(
        KeyId=key_id,
        Message=message,
        MessageType="RAW",
        SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA_256",
    )
    assert key_id in sign_resp["KeyId"]  # KeyId in response is the full ARN
    assert sign_resp["SigningAlgorithm"] == "RSASSA_PKCS1_V1_5_SHA_256"
    assert len(sign_resp["Signature"]) > 0

    verify_resp = kms_client.verify(
        KeyId=key_id,
        Message=message,
        MessageType="RAW",
        Signature=sign_resp["Signature"],
        SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA_256",
    )
    assert verify_resp["SignatureValid"] is True

def test_kms_sign_and_verify_pss(kms_client):
    key = kms_client.create_key(KeySpec="RSA_2048", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]
    message = b"test-pss-message"

    sign_resp = kms_client.sign(
        KeyId=key_id,
        Message=message,
        MessageType="RAW",
        SigningAlgorithm="RSASSA_PSS_SHA_256",
    )
    verify_resp = kms_client.verify(
        KeyId=key_id,
        Message=message,
        MessageType="RAW",
        Signature=sign_resp["Signature"],
        SigningAlgorithm="RSASSA_PSS_SHA_256",
    )
    assert verify_resp["SignatureValid"] is True

def test_kms_verify_wrong_message(kms_client):
    key = kms_client.create_key(KeySpec="RSA_2048", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    sign_resp = kms_client.sign(
        KeyId=key_id,
        Message=b"original",
        MessageType="RAW",
        SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA_256",
    )
    # Real AWS raises KMSInvalidSignatureException on invalid signature
    import pytest
    with pytest.raises(kms_client.exceptions.KMSInvalidSignatureException):
        kms_client.verify(
            KeyId=key_id,
            Message=b"tampered",
            MessageType="RAW",
            Signature=sign_resp["Signature"],
            SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA_256",
        )

def test_kms_jwt_signing_flow(kms_client):
    """Sign a JWT-style header.payload string and verify the signature."""
    import base64
    key = kms_client.create_key(KeySpec="RSA_2048", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    header = base64.urlsafe_b64encode(
        b'{"alg":"RS256","typ":"JWT"}'
    ).rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(
        b'{"sub":"user-2001","iss":"auth-service"}'
    ).rstrip(b"=").decode()
    signing_input = f"{header}.{payload}"

    sign_resp = kms_client.sign(
        KeyId=key_id,
        Message=signing_input.encode(),
        MessageType="RAW",
        SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA_256",
    )
    assert sign_resp["Signature"]

    verify_resp = kms_client.verify(
        KeyId=key_id,
        Message=signing_input.encode(),
        MessageType="RAW",
        Signature=sign_resp["Signature"],
        SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA_256",
    )
    assert verify_resp["SignatureValid"] is True

def test_kms_encrypt_decrypt_roundtrip(kms_client):
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]
    plaintext = b"sensitive document content"

    enc_resp = kms_client.encrypt(KeyId=key_id, Plaintext=plaintext)
    assert key_id in enc_resp["KeyId"]

    dec_resp = kms_client.decrypt(CiphertextBlob=enc_resp["CiphertextBlob"])
    assert dec_resp["Plaintext"] == plaintext

def test_kms_encrypt_decrypt_with_explicit_key(kms_client):
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]
    plaintext = b"another secret"

    enc_resp = kms_client.encrypt(KeyId=key_id, Plaintext=plaintext)
    dec_resp = kms_client.decrypt(
        KeyId=key_id, CiphertextBlob=enc_resp["CiphertextBlob"]
    )
    assert dec_resp["Plaintext"] == plaintext

def test_kms_generate_data_key_aes_256(kms_client):
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.generate_data_key(KeyId=key_id, KeySpec="AES_256")
    assert key_id in resp["KeyId"]
    assert len(resp["Plaintext"]) == 32
    assert resp["CiphertextBlob"]

def test_kms_generate_data_key_aes_128(kms_client):
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.generate_data_key(KeyId=key_id, KeySpec="AES_128")
    assert len(resp["Plaintext"]) == 16

def test_kms_generate_data_key_decrypt_roundtrip(kms_client):
    """Encrypted data key should be decryptable back to the plaintext."""
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    gen_resp = kms_client.generate_data_key(KeyId=key_id, KeySpec="AES_256")
    dec_resp = kms_client.decrypt(CiphertextBlob=gen_resp["CiphertextBlob"])
    assert dec_resp["Plaintext"] == gen_resp["Plaintext"]

def test_kms_generate_data_key_without_plaintext(kms_client):
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.generate_data_key_without_plaintext(
        KeyId=key_id, KeySpec="AES_256"
    )
    assert key_id in resp["KeyId"]
    assert resp["CiphertextBlob"]
    assert "Plaintext" not in resp

def test_kms_generate_data_key_pair_ed25519(kms_client):
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.generate_data_key_pair(
        KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519"
    )
    assert key_id in resp["KeyId"]
    assert resp["KeyPairSpec"] == "ECC_NIST_EDWARDS25519"
    assert resp["PrivateKeyCiphertextBlob"]
    # Unlike the WithoutPlaintext variant, this one does hand back the private key.
    assert len(resp["PrivateKeyPlaintext"]) == 48
    assert len(resp["PublicKey"]) == 44

def test_kms_generate_data_key_pair_plaintext_matches_ciphertext(kms_client):
    """PrivateKeyPlaintext must be the same key that PrivateKeyCiphertextBlob wraps."""
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.generate_data_key_pair(
        KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519"
    )
    dec_resp = kms_client.decrypt(
        CiphertextBlob=resp["PrivateKeyCiphertextBlob"], KeyId=key_id
    )
    assert dec_resp["Plaintext"] == resp["PrivateKeyPlaintext"]

def test_kms_generate_data_key_pair_rsa(kms_client):
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.generate_data_key_pair(KeyId=key_id, KeyPairSpec="RSA_2048")
    assert resp["KeyPairSpec"] == "RSA_2048"
    assert resp["PrivateKeyPlaintext"]
    assert resp["PublicKey"].startswith(bytes.fromhex("3082012230"))

def test_kms_generate_data_key_pair_variants_agree(kms_client):
    """The two variants differ in exactly one field: PrivateKeyPlaintext."""
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    with_pt = kms_client.generate_data_key_pair(
        KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519"
    )
    without_pt = kms_client.generate_data_key_pair_without_plaintext(
        KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519"
    )

    def shape(resp):
        return {k for k in resp if k != "ResponseMetadata"}

    assert shape(with_pt) - shape(without_pt) == {"PrivateKeyPlaintext"}
    assert shape(without_pt) - shape(with_pt) == set()

def test_kms_generate_data_key_pair_usable_for_signing(kms_client):
    """The returned plaintext private key must actually work with the public key."""
    serialization = pytest.importorskip(
        "cryptography.hazmat.primitives.serialization"
    )
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.generate_data_key_pair(
        KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519"
    )
    private_key = serialization.load_der_private_key(
        resp["PrivateKeyPlaintext"], password=None
    )
    public_key = serialization.load_der_public_key(resp["PublicKey"])
    public_key.verify(private_key.sign(b"message"), b"message")

def test_kms_generate_data_key_pair_requires_symmetric_key(kms_client):
    key = kms_client.create_key(KeySpec="RSA_2048", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    with pytest.raises(ClientError) as exc:
        kms_client.generate_data_key_pair(
            KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519"
        )
    assert exc.value.response["Error"]["Code"] == "InvalidKeyUsageException"
    assert "GenerateDataKeyPair requires" in exc.value.response["Error"]["Message"]

def test_kms_generate_data_key_pair_without_plaintext_ed25519(kms_client):
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.generate_data_key_pair_without_plaintext(
        KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519"
    )
    assert key_id in resp["KeyId"]
    assert resp["KeyPairSpec"] == "ECC_NIST_EDWARDS25519"
    assert resp["PrivateKeyCiphertextBlob"]
    # The entire point of the WithoutPlaintext variant: the private key
    # plaintext is never returned to the caller.
    assert "PrivateKeyPlaintext" not in resp
    # Ed25519 SubjectPublicKeyInfo is a fixed 44 bytes: a 12-byte header
    # (SEQUENCE + AlgorithmIdentifier for OID 1.3.101.112) + the 32-byte key.
    assert resp["PublicKey"].startswith(bytes.fromhex("302a300506032b6570032100"))
    assert len(resp["PublicKey"]) == 44

def test_kms_generate_data_key_pair_without_plaintext_rsa(kms_client):
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.generate_data_key_pair_without_plaintext(
        KeyId=key_id, KeyPairSpec="RSA_2048"
    )
    assert resp["KeyPairSpec"] == "RSA_2048"
    assert resp["PrivateKeyCiphertextBlob"]
    assert "PrivateKeyPlaintext" not in resp
    # SubjectPublicKeyInfo for RSA-2048.
    assert resp["PublicKey"].startswith(bytes.fromhex("3082012230"))

def test_kms_generate_data_key_pair_without_plaintext_decrypt_roundtrip(kms_client):
    """The wrapped private key decrypts back to a PKCS#8 DER private key."""
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    gen_resp = kms_client.generate_data_key_pair_without_plaintext(
        KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519"
    )
    dec_resp = kms_client.decrypt(
        CiphertextBlob=gen_resp["PrivateKeyCiphertextBlob"], KeyId=key_id
    )
    # PKCS#8 DER for an Ed25519 private key is a fixed 48 bytes.
    assert len(dec_resp["Plaintext"]) == 48
    assert dec_resp["Plaintext"].startswith(bytes.fromhex("302e020100300506032b657004220420"))

def test_kms_generate_data_key_pair_without_plaintext_encryption_context(kms_client):
    """EncryptionContext binds the wrapped private key: decrypt must supply the same one."""
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]
    context = {"purpose": "signing", "owner": "svc-a"}

    gen_resp = kms_client.generate_data_key_pair_without_plaintext(
        KeyId=key_id,
        KeyPairSpec="ECC_NIST_EDWARDS25519",
        EncryptionContext=context,
    )
    dec_resp = kms_client.decrypt(
        CiphertextBlob=gen_resp["PrivateKeyCiphertextBlob"],
        KeyId=key_id,
        EncryptionContext=context,
    )
    assert len(dec_resp["Plaintext"]) == 48

    with pytest.raises(ClientError) as exc:
        kms_client.decrypt(
            CiphertextBlob=gen_resp["PrivateKeyCiphertextBlob"],
            KeyId=key_id,
            EncryptionContext={"purpose": "signing", "owner": "svc-b"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidCiphertextException"

def test_kms_generate_data_key_pair_without_plaintext_keys_match(kms_client):
    """The wrapped private key must actually pair with the returned public key.

    Guards against a key pair whose metadata and material disagree — the public
    key is useless if it does not verify what the private key signs.
    """
    serialization = pytest.importorskip(
        "cryptography.hazmat.primitives.serialization"
    )
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    gen_resp = kms_client.generate_data_key_pair_without_plaintext(
        KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519"
    )
    dec_resp = kms_client.decrypt(
        CiphertextBlob=gen_resp["PrivateKeyCiphertextBlob"], KeyId=key_id
    )

    private_key = serialization.load_der_private_key(dec_resp["Plaintext"], password=None)
    derived_public = private_key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    assert derived_public == gen_resp["PublicKey"]

    # And the pair actually works end to end.
    signature = private_key.sign(b"message")
    public_key = serialization.load_der_public_key(gen_resp["PublicKey"])
    public_key.verify(signature, b"message")

def test_kms_generate_data_key_pair_without_plaintext_requires_symmetric_key(kms_client):
    """The CMK wraps the generated private key, so it must be symmetric."""
    key = kms_client.create_key(KeySpec="RSA_2048", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    with pytest.raises(ClientError) as exc:
        kms_client.generate_data_key_pair_without_plaintext(
            KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519"
        )
    assert exc.value.response["Error"]["Code"] == "InvalidKeyUsageException"
    # Names this operation, not the GenerateDataKeyPair variant it shares a helper with.
    assert (
        "GenerateDataKeyPairWithoutPlaintext requires"
        in exc.value.response["Error"]["Message"]
    )

def test_kms_generate_data_key_pair_without_plaintext_rejects_bad_spec(kms_client):
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    with pytest.raises(ClientError) as exc:
        kms_client.generate_data_key_pair_without_plaintext(
            KeyId=key_id, KeyPairSpec="ECC_NIST_P224"
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"

def test_kms_generate_data_key_pair_without_plaintext_nonexistent_key(kms_client):
    with pytest.raises(ClientError) as exc:
        kms_client.generate_data_key_pair_without_plaintext(
            KeyId=str(_uuid_mod.uuid4()), KeyPairSpec="ECC_NIST_EDWARDS25519"
        )
    assert exc.value.response["Error"]["Code"] == "NotFoundException"

def test_kms_ciphertext_uses_per_blob_nonce(kms_client):
    """Same plaintext + CMK + EncryptionContext must not yield identical
    ciphertext. Each blob carries a random nonce mixed into key derivation, so
    the XOR keystream never repeats across blobs. Without it the keystream would
    recur, and a wrapped key pair's private half would be recoverable from the
    public outputs (the DER embeds the public modulus). Both blobs must still
    decrypt back to the same plaintext."""
    key = kms_client.create_key()
    key_id = key["KeyMetadata"]["KeyId"]
    ctx = {"app": "prod"}
    zeros = b"\x00" * 48
    c1 = kms_client.encrypt(KeyId=key_id, Plaintext=zeros, EncryptionContext=ctx)["CiphertextBlob"]
    c2 = kms_client.encrypt(KeyId=key_id, Plaintext=zeros, EncryptionContext=ctx)["CiphertextBlob"]
    assert c1 != c2, "identical ciphertext means the keystream repeats (no nonce)"
    assert kms_client.decrypt(CiphertextBlob=c1, EncryptionContext=ctx)["Plaintext"] == zeros
    assert kms_client.decrypt(CiphertextBlob=c2, EncryptionContext=ctx)["Plaintext"] == zeros

def test_kms_data_key_pair_private_key_resists_keystream_attack(kms_client):
    """A wrapped private key must not fall out of a keystream-reuse attack.
    The attacker grabs a keystream via a chosen-plaintext Encrypt under the same
    CMK + EncryptionContext, then XORs it into the pair blob's data region. The
    per-blob nonce makes the two keystreams differ, so the recovered bytes are
    not a valid private key. Without the nonce this would reconstruct the DER."""
    serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
    key = kms_client.create_key()
    key_id = key["KeyMetadata"]["KeyId"]
    ctx = {"app": "prod"}
    blob = kms_client.generate_data_key_pair_without_plaintext(
        KeyId=key_id, KeyPairSpec="ECC_NIST_EDWARDS25519", EncryptionContext=ctx
    )["PrivateKeyCiphertextBlob"]
    keystream_blob = kms_client.encrypt(
        KeyId=key_id, Plaintext=b"\x00" * len(blob), EncryptionContext=ctx
    )["CiphertextBlob"]
    # data region starts after key_id(36) + ctx_hash(32) + nonce(16) = 84
    guess = bytes(a ^ b for a, b in zip(blob[84:], keystream_blob[84:]))
    with pytest.raises(Exception):
        serialization.load_der_private_key(guess, password=None)

def test_kms_get_public_key(kms_client):
    key = kms_client.create_key(KeySpec="RSA_2048", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.get_public_key(KeyId=key_id)
    assert key_id in resp["KeyId"]
    assert resp["KeySpec"] == "RSA_2048"
    assert resp["PublicKey"]

def test_kms_encrypt_decrypt_with_encryption_context(kms_client):
    """EncryptionContext must match between encrypt and decrypt."""
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]
    plaintext = b"context-sensitive data"
    context = {"service": "storage", "bucket": "documents"}

    enc_resp = kms_client.encrypt(
        KeyId=key_id, Plaintext=plaintext, EncryptionContext=context
    )

    dec_resp = kms_client.decrypt(
        CiphertextBlob=enc_resp["CiphertextBlob"],
        EncryptionContext=context,
    )
    assert dec_resp["Plaintext"] == plaintext

def test_kms_decrypt_wrong_context_fails(kms_client):
    """Decrypt with wrong EncryptionContext should fail."""
    key = kms_client.create_key(
        KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT"
    )
    key_id = key["KeyMetadata"]["KeyId"]

    enc_resp = kms_client.encrypt(
        KeyId=key_id,
        Plaintext=b"secret",
        EncryptionContext={"env": "prod"},
    )

    with pytest.raises(ClientError) as exc_info:
        kms_client.decrypt(
            CiphertextBlob=enc_resp["CiphertextBlob"],
            EncryptionContext={"env": "dev"},
        )
    assert "InvalidCiphertextException" in str(exc_info.value)

def test_kms_create_and_list_alias(kms_client):
    key = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.create_alias(AliasName="alias/test-alias", TargetKeyId=key_id)
    resp = kms_client.list_aliases()
    alias_names = [a["AliasName"] for a in resp["Aliases"]]
    assert "alias/test-alias" in alias_names

def test_kms_use_alias_for_encrypt(kms_client):
    """Encrypt/Decrypt using alias instead of key ID."""
    key = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT")
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.create_alias(AliasName="alias/enc-alias", TargetKeyId=key_id)
    enc = kms_client.encrypt(KeyId="alias/enc-alias", Plaintext=b"via alias")
    dec = kms_client.decrypt(CiphertextBlob=enc["CiphertextBlob"])
    assert dec["Plaintext"] == b"via alias"

def test_kms_describe_key_by_alias(kms_client):
    key = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.create_alias(AliasName="alias/desc-alias", TargetKeyId=key_id)
    resp = kms_client.describe_key(KeyId="alias/desc-alias")
    assert resp["KeyMetadata"]["KeyId"] == key_id


def test_kms_describe_key_by_alias_arn(kms_client):
    key = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.create_alias(AliasName="alias/desc-alias-arn", TargetKeyId=key_id)

    resp = kms_client.describe_key(
        KeyId="arn:aws:kms:us-east-1:000000000000:alias/desc-alias-arn",
    )

    assert resp["KeyMetadata"]["KeyId"] == key_id


def test_kms_alias_arn_resolution_rejects_forged_request_region(kms_client):
    key = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.create_alias(AliasName="alias/forged-region-alias", TargetKeyId=key_id)
    west_kms = _regional_kms("us-west-2")

    with pytest.raises(ClientError) as exc:
        west_kms.describe_key(
            KeyId="arn:aws:kms:us-west-2:000000000000:alias/forged-region-alias",
        )
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_kms_aliases_are_region_scoped(kms_client):
    alias_name = f"alias/region-scope-{_uuid_mod.uuid4().hex[:8]}"
    east_key_id = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")["KeyMetadata"]["KeyId"]
    kms_client.create_alias(AliasName=alias_name, TargetKeyId=east_key_id)

    west_kms = _regional_kms("us-west-2")
    west_key_id = west_kms.create_key(KeySpec="SYMMETRIC_DEFAULT")["KeyMetadata"]["KeyId"]
    west_kms.create_alias(AliasName=alias_name, TargetKeyId=west_key_id)

    east_aliases = {
        alias["AliasArn"]: alias["TargetKeyId"]
        for alias in kms_client.list_aliases()["Aliases"]
        if alias["AliasName"] == alias_name
    }
    west_aliases = {
        alias["AliasArn"]: alias["TargetKeyId"]
        for alias in west_kms.list_aliases()["Aliases"]
        if alias["AliasName"] == alias_name
    }

    assert east_aliases == {
        f"arn:aws:kms:us-east-1:000000000000:{alias_name}": east_key_id,
    }
    assert west_aliases == {
        f"arn:aws:kms:us-west-2:000000000000:{alias_name}": west_key_id,
    }
    assert kms_client.describe_key(KeyId=alias_name)["KeyMetadata"]["KeyId"] == east_key_id
    assert west_kms.describe_key(KeyId=alias_name)["KeyMetadata"]["KeyId"] == west_key_id


def test_kms_restore_legacy_account_scoped_state_adopts_key_arn_region():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import kms as _kms

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    key_id = str(_uuid_mod.uuid4())
    alias_name = "alias/legacy-west"
    alias_arn = f"arn:aws:kms:us-west-2:{account_id}:{alias_name}"
    key_arn = f"arn:aws:kms:us-west-2:{account_id}:key/{key_id}"

    legacy_keys = AccountScopedDict()
    legacy_keys._data[(account_id, key_id)] = {
        "KeyId": key_id,
        "Arn": key_arn,
        "KeyState": "Enabled",
        "Enabled": True,
        "KeySpec": "SYMMETRIC_DEFAULT",
        "KeyUsage": "ENCRYPT_DECRYPT",
        "Description": "legacy west key",
        "CreationDate": 1700000000,
        "Origin": "AWS_KMS",
        "EncryptionAlgorithms": ["SYMMETRIC_DEFAULT"],
        "SigningAlgorithms": [],
        "_symmetric_key_b64": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
    }
    legacy_aliases = AccountScopedDict()
    legacy_aliases._data[(account_id, alias_arn)] = key_id

    _kms.reset()
    try:
        set_request_account_id(account_id)
        set_request_region("us-east-1")
        _kms.restore_state({"keys": legacy_keys, "aliases": legacy_aliases})

        assert _kms._keys.get_scoped(account_id, "us-east-1", key_id) is None
        assert _kms._keys.get_scoped(account_id, "us-west-2", key_id)["Arn"] == key_arn
        assert _kms._aliases.get_scoped(account_id, "us-west-2", alias_arn) == key_id

        assert _kms._resolve_key(alias_name) is None
        set_request_region("us-west-2")
        assert _kms._resolve_key(alias_name)["KeyId"] == key_id
    finally:
        _kms.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_kms_restore_legacy_bare_alias_name_adopts_target_key_region():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import kms as _kms

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    key_id = str(_uuid_mod.uuid4())
    alias_name = "alias/legacy-bare-west"
    alias_arn = f"arn:aws:kms:us-west-2:{account_id}:{alias_name}"
    key_arn = f"arn:aws:kms:us-west-2:{account_id}:key/{key_id}"

    legacy_keys = AccountScopedDict()
    legacy_keys._data[(account_id, key_id)] = {
        "KeyId": key_id,
        "Arn": key_arn,
        "KeyState": "Enabled",
        "Enabled": True,
        "KeySpec": "SYMMETRIC_DEFAULT",
        "KeyUsage": "ENCRYPT_DECRYPT",
        "Description": "legacy west key",
        "CreationDate": 1700000000,
        "Origin": "AWS_KMS",
        "EncryptionAlgorithms": ["SYMMETRIC_DEFAULT"],
        "SigningAlgorithms": [],
        "_symmetric_key_b64": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
    }
    legacy_aliases = AccountScopedDict()
    legacy_aliases._data[(account_id, alias_name)] = key_id

    _kms.reset()
    try:
        set_request_account_id(account_id)
        set_request_region("us-east-1")
        _kms.restore_state({"keys": legacy_keys, "aliases": legacy_aliases})

        assert _kms._aliases.get_scoped(account_id, "us-east-1", alias_arn) is None
        assert _kms._aliases.get_scoped(account_id, "us-west-2", alias_arn) == key_id
        assert _kms._resolve_key(alias_name) is None

        set_request_region("us-west-2")
        assert _kms._resolve_key(alias_name)["KeyId"] == key_id
    finally:
        _kms.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_kms_cloudformation_alias_resolves_by_name_and_arn(cfn, kms_client):
    alias_name = f"alias/cfn-kms-alias-{_uuid_mod.uuid4().hex[:8]}"
    template = {
        "Resources": {
            "Key": {"Type": "AWS::KMS::Key", "Properties": {"Description": "cfn alias key"}},
            "Alias": {
                "Type": "AWS::KMS::Alias",
                "Properties": {"AliasName": alias_name, "TargetKeyId": {"Ref": "Key"}},
            },
        },
    }
    stack_name = f"kms-cfn-alias-{_uuid_mod.uuid4().hex[:8]}"
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))

    by_name = kms_client.describe_key(KeyId=alias_name)["KeyMetadata"]
    alias_arn = f"arn:aws:kms:us-east-1:000000000000:{alias_name}"
    by_arn = kms_client.describe_key(KeyId=alias_arn)["KeyMetadata"]
    assert by_arn["KeyId"] == by_name["KeyId"]


def test_kms_wrong_service_alias_arn_does_not_tail_match(kms_client):
    key = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    kms_client.create_alias(AliasName="alias/wrong-service-tail", TargetKeyId=key["KeyMetadata"]["KeyId"])

    with pytest.raises(ClientError) as exc:
        kms_client.describe_key(
            KeyId="arn:aws:sqs:us-east-1:000000000000:alias/wrong-service-tail",
        )

    assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_kms_update_alias(kms_client):
    key1 = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    key2 = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    kms_client.create_alias(AliasName="alias/upd-alias", TargetKeyId=key1["KeyMetadata"]["KeyId"])
    kms_client.update_alias(AliasName="alias/upd-alias", TargetKeyId=key2["KeyMetadata"]["KeyId"])
    resp = kms_client.describe_key(KeyId="alias/upd-alias")
    assert resp["KeyMetadata"]["KeyId"] == key2["KeyMetadata"]["KeyId"]

def test_kms_delete_alias(kms_client):
    key = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT")
    kms_client.create_alias(AliasName="alias/del-alias", TargetKeyId=key["KeyMetadata"]["KeyId"])
    kms_client.delete_alias(AliasName="alias/del-alias")
    with pytest.raises(ClientError) as exc:
        kms_client.describe_key(KeyId="alias/del-alias")
    assert "NotFoundException" in str(exc.value)

def test_kms_enable_disable_key_rotation(kms_client):
    """EnableKeyRotation / DisableKeyRotation / GetKeyRotationStatus."""
    key = kms_client.create_key(KeyUsage="ENCRYPT_DECRYPT")
    key_id = key["KeyMetadata"]["KeyId"]
    status = kms_client.get_key_rotation_status(KeyId=key_id)
    assert status["KeyRotationEnabled"] is False
    kms_client.enable_key_rotation(KeyId=key_id)
    status = kms_client.get_key_rotation_status(KeyId=key_id)
    assert status["KeyRotationEnabled"] is True
    kms_client.disable_key_rotation(KeyId=key_id)
    status = kms_client.get_key_rotation_status(KeyId=key_id)
    assert status["KeyRotationEnabled"] is False
    kms_client.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

def test_kms_get_put_key_policy(kms_client):
    """GetKeyPolicy / PutKeyPolicy."""
    key = kms_client.create_key()
    key_id = key["KeyMetadata"]["KeyId"]
    policy = kms_client.get_key_policy(KeyId=key_id, PolicyName="default")
    assert "Statement" in policy["Policy"]
    custom = '{"Version":"2012-10-17","Statement":[]}'
    kms_client.put_key_policy(KeyId=key_id, PolicyName="default", Policy=custom)
    got = kms_client.get_key_policy(KeyId=key_id, PolicyName="default")
    assert got["Policy"] == custom
    kms_client.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

def test_kms_tag_untag_list_v2(kms_client):
    """TagResource / UntagResource / ListResourceTags."""
    key = kms_client.create_key()
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.tag_resource(KeyId=key_id, Tags=[
        {"TagKey": "env", "TagValue": "test"},
        {"TagKey": "team", "TagValue": "platform"},
    ])
    tags = kms_client.list_resource_tags(KeyId=key_id)
    tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "platform"
    kms_client.untag_resource(KeyId=key_id, TagKeys=["team"])
    tags = kms_client.list_resource_tags(KeyId=key_id)
    assert len(tags["Tags"]) == 1
    assert tags["Tags"][0]["TagKey"] == "env"
    kms_client.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)


def test_kms_tag_resource_accepts_key_arn(kms_client):
    key = kms_client.create_key()
    arn = key["KeyMetadata"]["Arn"]

    kms_client.tag_resource(KeyId=arn, Tags=[{"TagKey": "env", "TagValue": "test"}])

    tags = kms_client.list_resource_tags(KeyId=arn)
    tag_map = {t["TagKey"]: t["TagValue"] for t in tags["Tags"]}
    assert tag_map["env"] == "test"
    kms_client.schedule_key_deletion(KeyId=arn, PendingWindowInDays=7)

def test_kms_enable_disable_key(kms_client):
    """EnableKey / DisableKey."""
    key = kms_client.create_key()
    key_id = key["KeyMetadata"]["KeyId"]
    assert key["KeyMetadata"]["KeyState"] == "Enabled"
    kms_client.disable_key(KeyId=key_id)
    desc = kms_client.describe_key(KeyId=key_id)
    assert desc["KeyMetadata"]["KeyState"] == "Disabled"
    kms_client.enable_key(KeyId=key_id)
    desc = kms_client.describe_key(KeyId=key_id)
    assert desc["KeyMetadata"]["KeyState"] == "Enabled"
    kms_client.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

def test_kms_schedule_cancel_deletion(kms_client):
    """ScheduleKeyDeletion / CancelKeyDeletion."""
    key = kms_client.create_key()
    key_id = key["KeyMetadata"]["KeyId"]
    resp = kms_client.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
    assert resp["KeyState"] == "PendingDeletion"
    kms_client.cancel_key_deletion(KeyId=key_id)
    desc = kms_client.describe_key(KeyId=key_id)
    assert desc["KeyMetadata"]["KeyState"] == "Disabled"

def test_kms_terraform_full_flow(kms_client):
    """Full Terraform aws_kms_key lifecycle."""
    key = kms_client.create_key(KeySpec="SYMMETRIC_DEFAULT", KeyUsage="ENCRYPT_DECRYPT", Description="RDS key")
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.enable_key_rotation(KeyId=key_id)
    assert kms_client.get_key_rotation_status(KeyId=key_id)["KeyRotationEnabled"] is True
    pol = kms_client.get_key_policy(KeyId=key_id, PolicyName="default")
    assert len(pol["Policy"]) > 0
    kms_client.tag_resource(KeyId=key_id, Tags=[{"TagKey": "Name", "TagValue": "rds-key"}])
    assert kms_client.list_resource_tags(KeyId=key_id)["Tags"][0]["TagValue"] == "rds-key"
    desc = kms_client.describe_key(KeyId=key_id)
    assert desc["KeyMetadata"]["Description"] == "RDS key"
    kms_client.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

def test_kms_list_key_policies(kms_client):
    """ListKeyPolicies returns default policy name."""
    key = kms_client.create_key()
    key_id = key["KeyMetadata"]["KeyId"]
    resp = kms_client.list_key_policies(KeyId=key_id)
    assert "default" in resp["PolicyNames"]
    kms_client.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)

def test_kms_create_ecc_secg_p256k1_key(kms_client):
    resp = kms_client.create_key(
        KeySpec="ECC_SECG_P256K1",
        KeyUsage="SIGN_VERIFY",
        Description="secp256k1 signing key",
    )
    meta = resp["KeyMetadata"]
    assert meta["KeySpec"] == "ECC_SECG_P256K1"
    assert meta["KeyUsage"] == "SIGN_VERIFY"
    assert "ECDSA_SHA_256" in meta["SigningAlgorithms"]
    assert meta["EncryptionAlgorithms"] == []

def test_kms_ecc_sign_and_verify(kms_client):
    key = kms_client.create_key(KeySpec="ECC_SECG_P256K1", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]
    message = b"hello secp256k1"

    sign_resp = kms_client.sign(
        KeyId=key_id,
        Message=message,
        MessageType="RAW",
        SigningAlgorithm="ECDSA_SHA_256",
    )
    assert key_id in sign_resp["KeyId"]  # KeyId in response is the full ARN
    assert sign_resp["SigningAlgorithm"] == "ECDSA_SHA_256"
    assert len(sign_resp["Signature"]) > 0

    verify_resp = kms_client.verify(
        KeyId=key_id,
        Message=message,
        MessageType="RAW",
        Signature=sign_resp["Signature"],
        SigningAlgorithm="ECDSA_SHA_256",
    )
    assert verify_resp["SignatureValid"] is True

def test_kms_ecc_verify_wrong_message(kms_client):
    key = kms_client.create_key(KeySpec="ECC_SECG_P256K1", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    sign_resp = kms_client.sign(
        KeyId=key_id,
        Message=b"original",
        MessageType="RAW",
        SigningAlgorithm="ECDSA_SHA_256",
    )
    import pytest
    with pytest.raises(kms_client.exceptions.KMSInvalidSignatureException):
        kms_client.verify(
            KeyId=key_id,
            Message=b"tampered",
            MessageType="RAW",
            Signature=sign_resp["Signature"],
            SigningAlgorithm="ECDSA_SHA_256",
        )

def test_kms_ecc_get_public_key(kms_client):
    key = kms_client.create_key(KeySpec="ECC_SECG_P256K1", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.get_public_key(KeyId=key_id)
    assert key_id in resp["KeyId"]
    assert resp["KeySpec"] == "ECC_SECG_P256K1"
    assert resp["PublicKey"]
    assert "ECDSA_SHA_256" in resp["SigningAlgorithms"]

def test_kms_create_ecc_nist_edwards25519_key(kms_client):
    resp = kms_client.create_key(
        KeySpec="ECC_NIST_EDWARDS25519",
        KeyUsage="SIGN_VERIFY",
        Description="ed25519 signing key",
    )
    meta = resp["KeyMetadata"]
    assert meta["KeySpec"] == "ECC_NIST_EDWARDS25519"
    assert meta["KeyUsage"] == "SIGN_VERIFY"
    # Real AWS lists both algorithms for this key spec — Developer Guide
    # "Supported signing algorithms for ECC key specs".
    assert meta["SigningAlgorithms"] == ["ED25519_SHA_512", "ED25519_PH_SHA_512"]
    assert meta["EncryptionAlgorithms"] == []

def test_kms_ecc_nist_edwards25519_sign_verify(kms_client):
    key = kms_client.create_key(KeySpec="ECC_NIST_EDWARDS25519", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]
    message = b"hello ed25519"

    sign_resp = kms_client.sign(
        KeyId=key_id,
        Message=message,
        MessageType="RAW",
        SigningAlgorithm="ED25519_SHA_512",
    )
    assert key_id in sign_resp["KeyId"]
    assert sign_resp["SigningAlgorithm"] == "ED25519_SHA_512"
    assert len(sign_resp["Signature"]) > 0

    verify_resp = kms_client.verify(
        KeyId=key_id,
        Message=message,
        MessageType="RAW",
        Signature=sign_resp["Signature"],
        SigningAlgorithm="ED25519_SHA_512",
    )
    assert verify_resp["SignatureValid"] is True

def test_kms_ecc_nist_edwards25519_verify_wrong_message(kms_client):
    key = kms_client.create_key(KeySpec="ECC_NIST_EDWARDS25519", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    sign_resp = kms_client.sign(
        KeyId=key_id,
        Message=b"original ed25519",
        MessageType="RAW",
        SigningAlgorithm="ED25519_SHA_512",
    )
    with pytest.raises(kms_client.exceptions.KMSInvalidSignatureException):
        kms_client.verify(
            KeyId=key_id,
            Message=b"tampered ed25519",
            MessageType="RAW",
            Signature=sign_resp["Signature"],
            SigningAlgorithm="ED25519_SHA_512",
        )

def test_kms_ecc_nist_edwards25519_get_public_key(kms_client):
    key = kms_client.create_key(KeySpec="ECC_NIST_EDWARDS25519", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    resp = kms_client.get_public_key(KeyId=key_id)
    assert key_id in resp["KeyId"]
    assert resp["KeySpec"] == "ECC_NIST_EDWARDS25519"
    assert resp["PublicKey"]
    assert resp["SigningAlgorithms"] == ["ED25519_SHA_512", "ED25519_PH_SHA_512"]

def test_kms_ed25519_sha_512_rejects_non_raw_message_type(kms_client):
    """ED25519_SHA_512 requires MessageType=RAW (AWS Developer Guide / Sign API)."""
    key = kms_client.create_key(KeySpec="ECC_NIST_EDWARDS25519", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]
    with pytest.raises(Exception) as exc:
        kms_client.sign(
            KeyId=key_id,
            Message=b"a" * 64,
            MessageType="DIGEST",
            SigningAlgorithm="ED25519_SHA_512",
        )
    msg = str(exc.value)
    assert "ED25519_SHA_512" in msg and "RAW" in msg


def test_kms_ed25519_ph_sha_512_sign_returns_unsupported(kms_client):
    """ED25519_PH_SHA_512 (Ed25519ph) is listed in metadata but Sign is not yet implemented."""
    key = kms_client.create_key(KeySpec="ECC_NIST_EDWARDS25519", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]
    with pytest.raises(Exception) as exc:
        kms_client.sign(
            KeyId=key_id,
            Message=b"a" * 64,
            MessageType="DIGEST",
            SigningAlgorithm="ED25519_PH_SHA_512",
        )
    assert "ED25519_PH_SHA_512" in str(exc.value)


def test_kms_ed25519_ph_sha_512_verify_returns_unsupported(kms_client):
    """ED25519_PH_SHA_512 Verify is also gated until Ed25519ph lands."""
    key = kms_client.create_key(KeySpec="ECC_NIST_EDWARDS25519", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]
    with pytest.raises(Exception) as exc:
        kms_client.verify(
            KeyId=key_id,
            Message=b"a" * 64,
            MessageType="DIGEST",
            Signature=b"\x00" * 64,
            SigningAlgorithm="ED25519_PH_SHA_512",
        )
    assert "ED25519_PH_SHA_512" in str(exc.value)


def test_kms_ecc_nist_p256_sign_verify(kms_client):
    key = kms_client.create_key(KeySpec="ECC_NIST_P256", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    sign_resp = kms_client.sign(
        KeyId=key_id,
        Message=b"nist p256 message",
        MessageType="RAW",
        SigningAlgorithm="ECDSA_SHA_256",
    )
    verify_resp = kms_client.verify(
        KeyId=key_id,
        Message=b"nist p256 message",
        MessageType="RAW",
        Signature=sign_resp["Signature"],
        SigningAlgorithm="ECDSA_SHA_256",
    )
    assert verify_resp["SignatureValid"] is True

def test_kms_ecc_nist_p384_sign_verify(kms_client):
    key = kms_client.create_key(KeySpec="ECC_NIST_P384", KeyUsage="SIGN_VERIFY")
    meta = key["KeyMetadata"]
    assert "ECDSA_SHA_384" in meta["SigningAlgorithms"]

    sign_resp = kms_client.sign(
        KeyId=meta["KeyId"],
        Message=b"nist p384 message",
        MessageType="RAW",
        SigningAlgorithm="ECDSA_SHA_384",
    )
    verify_resp = kms_client.verify(
        KeyId=meta["KeyId"],
        Message=b"nist p384 message",
        MessageType="RAW",
        Signature=sign_resp["Signature"],
        SigningAlgorithm="ECDSA_SHA_384",
    )
    assert verify_resp["SignatureValid"] is True

def test_kms_ecc_nist_p521_sign_verify(kms_client):
    key = kms_client.create_key(KeySpec="ECC_NIST_P521", KeyUsage="SIGN_VERIFY")
    meta = key["KeyMetadata"]
    assert "ECDSA_SHA_512" in meta["SigningAlgorithms"]

    sign_resp = kms_client.sign(
        KeyId=meta["KeyId"],
        Message=b"nist p521 message",
        MessageType="RAW",
        SigningAlgorithm="ECDSA_SHA_512",
    )
    verify_resp = kms_client.verify(
        KeyId=meta["KeyId"],
        Message=b"nist p521 message",
        MessageType="RAW",
        Signature=sign_resp["Signature"],
        SigningAlgorithm="ECDSA_SHA_512",
    )
    assert verify_resp["SignatureValid"] is True

def test_kms_ecc_sign_verify_digest_mode(kms_client):
    """Sign/Verify with MessageType=DIGEST (pre-hashed message)."""
    import hashlib
    key = kms_client.create_key(KeySpec="ECC_SECG_P256K1", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]

    message_digest = hashlib.sha256(b"original message").digest()

    sign_resp = kms_client.sign(
        KeyId=key_id,
        Message=message_digest,
        MessageType="DIGEST",
        SigningAlgorithm="ECDSA_SHA_256",
    )
    assert sign_resp["SigningAlgorithm"] == "ECDSA_SHA_256"

    verify_resp = kms_client.verify(
        KeyId=key_id,
        Message=message_digest,
        MessageType="DIGEST",
        Signature=sign_resp["Signature"],
        SigningAlgorithm="ECDSA_SHA_256",
    )
    assert verify_resp["SignatureValid"] is True

    # Wrong digest should fail with KMSInvalidSignatureException
    import pytest
    wrong_digest = hashlib.sha256(b"different message").digest()
    with pytest.raises(kms_client.exceptions.KMSInvalidSignatureException):
        kms_client.verify(
            KeyId=key_id,
            Message=wrong_digest,
            MessageType="DIGEST",
            Signature=sign_resp["Signature"],
            SigningAlgorithm="ECDSA_SHA_256",
        )

def test_kms_ecc_sign_via_alias(kms_client):
    """Sign and verify using an alias instead of key ID."""
    key = kms_client.create_key(KeySpec="ECC_SECG_P256K1", KeyUsage="SIGN_VERIFY")
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.create_alias(AliasName="alias/ecc-sign-alias", TargetKeyId=key_id)

    sign_resp = kms_client.sign(
        KeyId="alias/ecc-sign-alias",
        Message=b"alias signing test",
        MessageType="RAW",
        SigningAlgorithm="ECDSA_SHA_256",
    )
    verify_resp = kms_client.verify(
        KeyId="alias/ecc-sign-alias",
        Message=b"alias signing test",
        MessageType="RAW",
        Signature=sign_resp["Signature"],
        SigningAlgorithm="ECDSA_SHA_256",
    )
    assert verify_resp["SignatureValid"] is True

def test_kms_key_rotation_with_period(kms_client):
    """EnableKeyRotation with custom RotationPeriodInDays."""
    key = kms_client.create_key()
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.enable_key_rotation(KeyId=key_id, RotationPeriodInDays=180)
    status = kms_client.get_key_rotation_status(KeyId=key_id)
    assert status["KeyRotationEnabled"] is True
    assert status["RotationPeriodInDays"] == 180
    kms_client.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)


def test_kms_pending_deletion_blocks_encrypt(kms_client):
    """Encrypt on a PendingDeletion key should raise KMSInvalidStateException."""
    import pytest
    key = kms_client.create_key()
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)
    with pytest.raises(kms_client.exceptions.KMSInvalidStateException):
        kms_client.encrypt(KeyId=key_id, Plaintext=b"test")


def test_kms_disabled_key_blocks_encrypt(kms_client):
    """Encrypt on a disabled key should raise DisabledException."""
    import pytest
    key = kms_client.create_key()
    key_id = key["KeyMetadata"]["KeyId"]
    kms_client.disable_key(KeyId=key_id)
    with pytest.raises(kms_client.exceptions.DisabledException):
        kms_client.encrypt(KeyId=key_id, Plaintext=b"test")
