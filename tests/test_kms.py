import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


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
