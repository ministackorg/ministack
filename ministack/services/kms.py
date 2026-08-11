"""
KMS (Key Management Service) Emulator.
JSON-based API via X-Amz-Target (prefix: TrentService).
Supports: CreateKey, ListKeys, DescribeKey, Sign, Verify,
          Encrypt, Decrypt, GenerateDataKey,
          GenerateDataKeyWithoutPlaintext, GenerateDataKeyPair,
          GenerateDataKeyPairWithoutPlaintext, GenerateMac, VerifyMac.
"""

import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("kms")

try:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, ed25519, padding, rsa, utils
    HAS_CRYPTO = True
except ImportError:
    InvalidSignature = Exception
    HAS_CRYPTO = False
    logger.warning(
        "cryptography package not installed; "
        "KMS Sign/Verify will return errors. "
        "Install with: pip install cryptography"
    )

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

from ministack.core.persistence import PERSIST_STATE, load_state

_keys = AccountRegionScopedDict()
# key_id -> {
#     KeyId, Arn, KeyState, KeyUsage, KeySpec, Description,
#     CreationDate, Enabled, Origin,
#     _private_key (asymmetric private key object, RSA/ECC only),
#     _public_key_der (bytes, RSA/ECC only),
#     _symmetric_key (bytes, SYMMETRIC_DEFAULT only),
#     _hmac_key (bytes, HMAC_* only),
# }
_aliases = AccountRegionScopedDict()  # alias ARN -> key_id

_HMAC_KEY_SPECS = {
    "HMAC_224": ("HMAC_SHA_224", 28),
    "HMAC_256": ("HMAC_SHA_256", 32),
    "HMAC_384": ("HMAC_SHA_384", 48),
    "HMAC_512": ("HMAC_SHA_512", 64),
}

_MAC_ALGORITHM_DIGESTS = {
    "HMAC_SHA_224": "sha224",
    "HMAC_SHA_256": "sha256",
    "HMAC_SHA_384": "sha384",
    "HMAC_SHA_512": "sha512",
}

MAC_KEY_USAGE = "GENERATE_VERIFY_MAC"

MAC_MESSAGE_MAX_BYTES = 4096

MAC_MAX_BYTES = 6144

_MAX_MESSAGE_B64_CHARS = 4 * ((MAC_MESSAGE_MAX_BYTES + 2) // 3)


def _alias_arn(alias_name):
    return f"arn:aws:kms:{get_region()}:{get_account_id()}:{alias_name}"


def _alias_arn_from_key_record(alias_name, rec, account_id=None):
    if rec and rec.get("Arn"):
        try:
            spec = parse_arn(rec["Arn"])
            if spec.service == "kms" and spec.region and spec.account_id:
                return f"arn:{spec.partition}:kms:{spec.region}:{spec.account_id}:{alias_name}"
        except ArnParseError:
            pass
    return f"arn:aws:kms:{get_region()}:{account_id or get_account_id()}:{alias_name}"


# ── Persistence ────────────────────────────────────────────

def get_state():
    """Return JSON-serializable state. Symmetric keys are base64-encoded;
    RSA private keys are PEM-encoded if cryptography is available."""
    serializable_keys = AccountRegionScopedDict()
    # Iterate _data directly to capture ALL accounts
    for scoped_key, rec in _keys._data.items():
        entry = {k: v for k, v in rec.items()
                 if k not in ("_private_key", "_public_key_der", "_symmetric_key",
                              "_hmac_key")}
        if "_symmetric_key" in rec:
            entry["_symmetric_key_b64"] = base64.b64encode(rec["_symmetric_key"]).decode()
        if "_hmac_key" in rec:
            entry["_hmac_key_b64"] = base64.b64encode(rec["_hmac_key"]).decode()
        if "_public_key_der" in rec:
            entry["_public_key_der_b64"] = base64.b64encode(rec["_public_key_der"]).decode()
        if "_private_key" in rec and HAS_CRYPTO:
            try:
                pem = rec["_private_key"].private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )
                entry["_private_key_pem"] = base64.b64encode(pem).decode()
            except Exception:
                pass
        serializable_keys._data[scoped_key] = entry
    return {"keys": serializable_keys, "aliases": _aliases}


def restore_state(data):
    if data:
        keys_data = data.get("keys", {})

        def _region_from_key_entry(entry):
            try:
                spec = parse_arn(entry.get("Arn", ""))
            except (ArnParseError, AttributeError):
                return get_region()
            if spec.service != "kms":
                return get_region()
            return spec.region or get_region()

        def _restore_key_entry(entry):
            if "_symmetric_key_b64" in entry:
                entry["_symmetric_key"] = base64.b64decode(entry.pop("_symmetric_key_b64"))
            if "_hmac_key_b64" in entry:
                entry["_hmac_key"] = base64.b64decode(entry.pop("_hmac_key_b64"))
            if "_public_key_der_b64" in entry:
                entry["_public_key_der"] = base64.b64decode(entry.pop("_public_key_der_b64"))
            if "_private_key_pem" in entry and HAS_CRYPTO:
                try:
                    pem_bytes = base64.b64decode(entry.pop("_private_key_pem"))
                    entry["_private_key"] = serialization.load_pem_private_key(pem_bytes, password=None)
                except Exception:
                    pass

        if isinstance(keys_data, AccountRegionScopedDict):
            for scoped_key, entry in keys_data._data.items():
                _restore_key_entry(entry)
                _keys._data[scoped_key] = entry
        elif isinstance(keys_data, AccountScopedDict):
            for (account_id, kid), entry in keys_data._data.items():
                _restore_key_entry(entry)
                _keys.set_scoped(account_id, _region_from_key_entry(entry), kid, entry)
        else:
            for kid, entry in keys_data.items():
                _restore_key_entry(entry)
                _keys.set_scoped(get_account_id(), _region_from_key_entry(entry), kid, entry)

        def _key_record_for_alias_target(account_id, target_id):
            for (stored_account, _region, key_id), rec in _keys._data.items():
                if stored_account == account_id and key_id == target_id:
                    return rec
            return None

        def _store_alias(account_id, alias_key, target_id):
            storage_key = alias_key
            if not str(alias_key).startswith("arn:"):
                storage_key = _alias_arn_from_key_record(
                    alias_key,
                    _key_record_for_alias_target(account_id, target_id),
                    account_id,
                )
            try:
                spec = parse_arn(storage_key)
                region = spec.region if spec.service == "kms" and spec.region else get_region()
            except ArnParseError:
                region = get_region()
            _aliases.set_scoped(account_id, region, storage_key, target_id)

        aliases_data = data.get("aliases", {})
        if isinstance(aliases_data, AccountRegionScopedDict):
            _aliases.update(aliases_data)
        elif isinstance(aliases_data, AccountScopedDict):
            for (account_id, alias_key), target_id in aliases_data._data.items():
                _store_alias(account_id, alias_key, target_id)
        else:
            for alias_key, target_id in aliases_data.items():
                _store_alias(get_account_id(), alias_key, target_id)


try:
    _restored = load_state("kms")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


def _arn(key_id):
    return f"arn:aws:kms:{get_region()}:{get_account_id()}:key/{key_id}"


def _key_metadata(rec):
    metadata = {
        "KeyId": rec["KeyId"],
        "Arn": rec["Arn"],
        "CreationDate": rec["CreationDate"],
        "Enabled": rec["Enabled"],
        "Description": rec.get("Description", ""),
        "KeyUsage": rec["KeyUsage"],
        "KeyState": rec["KeyState"],
        "Origin": rec["Origin"],
        "KeyManager": "CUSTOMER",
        "CustomerMasterKeySpec": rec["KeySpec"],
        "KeySpec": rec["KeySpec"],
    }
    if _is_hmac_key(rec):
        metadata["MacAlgorithms"] = rec.get("MacAlgorithms", [])
    else:
        metadata["EncryptionAlgorithms"] = rec.get("EncryptionAlgorithms", [])
        metadata["SigningAlgorithms"] = rec.get("SigningAlgorithms", [])
    # "This value is present only when the KMS key is scheduled for deletion,
    # that is, when its KeyState is PendingDeletion."
    if "DeletionDate" in rec:
        metadata["DeletionDate"] = rec["DeletionDate"]
    return metadata


def _key_ref_from_arn(key_id_or_arn):
    try:
        spec = parse_arn(key_id_or_arn)
    except ArnParseError:
        return None
    if (
        spec.partition != "aws"
        or spec.service != "kms"
        or spec.region != get_region()
        or spec.account_id != get_account_id()
    ):
        return None
    if spec.resource.startswith("key/"):
        key_id = spec.resource[len("key/"):]
        return ("key", key_id) if key_id else None
    if spec.resource.startswith("alias/"):
        alias_name = spec.resource
        return ("alias", key_id_or_arn) if alias_name != "alias/" else None
    return None


def _resolve_key(key_id_or_arn):
    if not key_id_or_arn:
        return None
    if key_id_or_arn.startswith("arn:"):
        key_ref = _key_ref_from_arn(key_id_or_arn)
        if not key_ref:
            return None
        ref_type, key_ref_value = key_ref
        if ref_type == "key":
            rec = _keys.get(key_ref_value)
            return rec if rec and rec.get("Arn") == key_id_or_arn else None
        key_id_or_arn = key_ref_value
    # Direct key ID lookup
    if key_id_or_arn in _keys:
        return _keys[key_id_or_arn]
    # Alias lookup: "alias/my-key"
    alias_arn = key_id_or_arn if key_id_or_arn.startswith("arn:") else _alias_arn(key_id_or_arn)
    if alias_arn in _aliases:
        return _keys.get(_aliases[alias_arn])
    return None


def _check_key_state(rec):
    """Return an error response if the key is in an unusable state, else None."""
    if rec["KeyState"] == "PendingDeletion":
        return error_response_json(
            "KMSInvalidStateException",
            f"{rec['Arn']} is pending deletion.",
            400,
        )
    if rec["KeyState"] == "Disabled":
        return error_response_json(
            "DisabledException",
            f"{rec['Arn']} is disabled.",
            400,
        )
    return None


def _is_hmac_key(rec):
    return "_hmac_key" in rec or rec.get("KeySpec") in _HMAC_KEY_SPECS


def _reject_hmac_key(rec, operation):
    if _is_hmac_key(rec):
        return error_response_json(
            "InvalidKeyUsageException",
            f"{rec['Arn']} key usage is {rec.get('KeyUsage', MAC_KEY_USAGE)} "
            f"which is not valid for {operation}.",
            400,
        )
    return None


def _require_crypto(operation):
    if not HAS_CRYPTO:
        return error_response_json(
            "KMSInternalException",
            f"{operation} requires the cryptography package. "
            "Install with: pip install cryptography",
            500,
        )
    return None


# ---- Operations ----


def _create_key(data):
    key_id = new_uuid()
    key_spec = data.get("KeySpec", data.get("CustomerMasterKeySpec", "SYMMETRIC_DEFAULT"))
    key_usage = data.get("KeyUsage", "ENCRYPT_DECRYPT")
    if key_spec in _HMAC_KEY_SPECS and key_usage != MAC_KEY_USAGE:
        return error_response_json(
            "ValidationException",
            f"KeyUsage {key_usage} is not compatible with KeySpec {key_spec}. "
            f"HMAC KMS keys require KeyUsage {MAC_KEY_USAGE}.",
            400,
        )
    if key_usage == MAC_KEY_USAGE and key_spec not in _HMAC_KEY_SPECS:
        return error_response_json(
            "ValidationException",
            f"KeyUsage {MAC_KEY_USAGE} is not compatible with KeySpec {key_spec}. "
            "It requires one of: "
            f"[{', '.join(sorted(_HMAC_KEY_SPECS))}]",
            400,
        )
    description = data.get("Description", "")
    tags = data.get("Tags", [])
    policy = data.get("Policy", json.dumps({
        "Version": "2012-10-17",
        "Id": "key-default-1",
        "Statement": [{
            "Sid": "Enable IAM User Permissions",
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{get_account_id()}:root"},
            "Action": "kms:*",
            "Resource": "*",
        }],
    }))

    rec = {
        "KeyId": key_id,
        "Arn": _arn(key_id),
        "KeyState": "Enabled",
        "Enabled": True,
        "KeySpec": key_spec,
        "KeyUsage": key_usage,
        "Description": description,
        "CreationDate": int(time.time()),
        "Origin": "AWS_KMS",
        "Tags": tags,
        "Policy": policy,
    }

    if key_spec == "SYMMETRIC_DEFAULT":
        rec["_symmetric_key"] = os.urandom(32)
        rec["EncryptionAlgorithms"] = ["SYMMETRIC_DEFAULT"]
        rec["SigningAlgorithms"] = []
    elif key_spec in _HMAC_KEY_SPECS:
        mac_algorithm, material_len = _HMAC_KEY_SPECS[key_spec]
        rec["_hmac_key"] = os.urandom(material_len)
        rec["MacAlgorithms"] = [mac_algorithm]
    elif key_spec in ("RSA_2048", "RSA_3072", "RSA_4096"):
        err = _require_crypto("CreateKey")
        if err:
            return err
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=int(key_spec.split("_")[1])
        )
        rec["_private_key"] = private_key
        rec["_public_key_der"] = private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        if key_usage == "SIGN_VERIFY":
            rec["SigningAlgorithms"] = [
                "RSASSA_PKCS1_V1_5_SHA_256",
                "RSASSA_PKCS1_V1_5_SHA_384",
                "RSASSA_PKCS1_V1_5_SHA_512",
                "RSASSA_PSS_SHA_256",
                "RSASSA_PSS_SHA_384",
                "RSASSA_PSS_SHA_512",
            ]
            rec["EncryptionAlgorithms"] = []
        else:
            rec["EncryptionAlgorithms"] = [
                "RSAES_OAEP_SHA_1",
                "RSAES_OAEP_SHA_256",
            ]
            rec["SigningAlgorithms"] = []
    elif key_spec in ("ECC_NIST_P256", "ECC_NIST_P384", "ECC_NIST_P521", "ECC_SECG_P256K1"):
        err = _require_crypto("CreateKey")
        if err:
            return err
        curve_map = {
            "ECC_NIST_P256": ec.SECP256R1(),
            "ECC_NIST_P384": ec.SECP384R1(),
            "ECC_NIST_P521": ec.SECP521R1(),
            "ECC_SECG_P256K1": ec.SECP256K1(),
        }
        private_key = ec.generate_private_key(curve_map[key_spec])
        rec["_private_key"] = private_key
        rec["_public_key_der"] = private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        signing_algo_map = {
            "ECC_NIST_P256": ["ECDSA_SHA_256"],
            "ECC_NIST_P384": ["ECDSA_SHA_384"],
            "ECC_NIST_P521": ["ECDSA_SHA_512"],
            "ECC_SECG_P256K1": ["ECDSA_SHA_256"],
        }
        rec["SigningAlgorithms"] = signing_algo_map[key_spec]
        rec["EncryptionAlgorithms"] = []
    elif key_spec == "ECC_NIST_EDWARDS25519":
        err = _require_crypto("CreateKey")
        if err:
            return err
        private_key = ed25519.Ed25519PrivateKey.generate()
        rec["_private_key"] = private_key
        rec["_public_key_der"] = private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        # Real AWS exposes both for ECC_NIST_EDWARDS25519 — verified against the
        # KMS Developer Guide "Supported signing algorithms for ECC key specs"
        # table. PH variant is listed in metadata even though Sign/Verify return
        # UnsupportedOperationException below until Ed25519ph lands.
        rec["SigningAlgorithms"] = ["ED25519_SHA_512", "ED25519_PH_SHA_512"]
        rec["EncryptionAlgorithms"] = []
    else:
        return error_response_json(
            "UnsupportedOperationException",
            f"KeySpec {key_spec} is not supported in this emulator",
            400,
        )

    _keys[key_id] = rec
    logger.info("Created key %s (%s, %s)", key_id, key_spec, key_usage)
    return json_response({"KeyMetadata": _key_metadata(rec)})


def _list_keys(data):
    limit = data.get("Limit", 1000)
    keys = [{"KeyId": r["KeyId"], "KeyArn": r["Arn"]} for r in _keys.values()]
    return json_response({
        "Keys": keys[:limit],
        "Truncated": len(keys) > limit,
    })


def _describe_key(data):
    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {key_id} not found", 400)
    return json_response({"KeyMetadata": _key_metadata(rec)})


def _get_public_key(data):
    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {key_id} not found", 400)
    err = _reject_hmac_key(rec, "GetPublicKey")
    if err:
        return err
    if "_public_key_der" not in rec:
        return error_response_json(
            "UnsupportedOperationException",
            "GetPublicKey is only valid for asymmetric keys",
            400,
        )
    return json_response({
        "KeyId": rec["Arn"],
        "KeyUsage": rec["KeyUsage"],
        "KeySpec": rec["KeySpec"],
        "PublicKey": base64.b64encode(rec["_public_key_der"]).decode(),
        "SigningAlgorithms": rec.get("SigningAlgorithms", []),
        "EncryptionAlgorithms": rec.get("EncryptionAlgorithms", []),
    })


def _sign(data):
    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {key_id} not found", 400)
    err = _check_key_state(rec) or _reject_hmac_key(rec, "Sign")
    if err:
        return err
    err = _require_crypto("Sign")
    if err:
        return err
    if "_private_key" not in rec:
        return error_response_json(
            "UnsupportedOperationException",
            "Sign is only valid for asymmetric SIGN_VERIFY keys",
            400,
        )

    message_b64 = data.get("Message", "")
    message_type = data.get("MessageType", "RAW")
    algorithm = data.get("SigningAlgorithm", "RSASSA_PKCS1_V1_5_SHA_256")

    if isinstance(message_b64, str):
        message = base64.b64decode(message_b64)
    else:
        message = message_b64

    if algorithm in ("ED25519_SHA_512", "ED25519_PH_SHA_512"):
        if rec["KeySpec"] != "ECC_NIST_EDWARDS25519":
            return error_response_json(
                "UnsupportedOperationException",
                f"Signing algorithm {algorithm} is not supported for this key",
                400,
            )
        # AWS KMS: "ED25519_SHA_512 signing algorithm requires MessageType:RAW,
        # while ED25519_PH_SHA_512 requires MessageType:DIGEST. These message
        # types cannot be used interchangeably." (Developer Guide).
        if algorithm == "ED25519_SHA_512" and message_type != "RAW":
            return error_response_json(
                "UnsupportedOperationException",
                "ED25519_SHA_512 signing algorithm requires MessageType=RAW",
                400,
            )
        if algorithm == "ED25519_PH_SHA_512":
            # HashEdDSA (Ed25519ph) — RFC 8032 §5.1 / FIPS 186-5 §7.8. The
            # cryptography library does not expose the dom2 prefix needed for a
            # spec-correct implementation, so we surface the gap honestly
            # instead of routing through pure Ed25519 (which would produce
            # signatures incompatible with real AWS KMS).
            return error_response_json(
                "UnsupportedOperationException",
                "ED25519_PH_SHA_512 (Ed25519ph) signing is not yet implemented in this emulator",
                400,
            )
        signature = rec["_private_key"].sign(message)
        logger.debug("Signed %d bytes with key %s (%s)", len(message), key_id, algorithm)
        return json_response({
            "KeyId": rec["Arn"],
            "Signature": base64.b64encode(signature).decode(),
            "SigningAlgorithm": algorithm,
        })

    pad, hash_algo = _signing_params(algorithm)
    if hash_algo is None:
        return error_response_json(
            "UnsupportedOperationException",
            f"Signing algorithm {algorithm} is not supported",
            400,
        )

    if pad is None:
        # ECDSA – no padding; pass ec.ECDSA(hash) as the algorithm
        if message_type == "DIGEST":
            signature = rec["_private_key"].sign(
                message, ec.ECDSA(utils.Prehashed(hash_algo))
            )
        else:
            signature = rec["_private_key"].sign(message, ec.ECDSA(hash_algo))
    else:
        # RSA
        if message_type == "DIGEST":
            signature = rec["_private_key"].sign(
                message, pad, utils.Prehashed(hash_algo)
            )
        else:
            signature = rec["_private_key"].sign(message, pad, hash_algo)

    logger.debug("Signed %d bytes with key %s (%s)", len(message), key_id, algorithm)
    return json_response({
        "KeyId": rec["Arn"],
        "Signature": base64.b64encode(signature).decode(),
        "SigningAlgorithm": algorithm,
    })


def _verify(data):
    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {key_id} not found", 400)
    err = _check_key_state(rec) or _reject_hmac_key(rec, "Verify")
    if err:
        return err
    err = _require_crypto("Verify")
    if err:
        return err
    if "_private_key" not in rec:
        return error_response_json(
            "UnsupportedOperationException",
            "Verify is only valid for asymmetric SIGN_VERIFY keys",
            400,
        )

    message_b64 = data.get("Message", "")
    message_type = data.get("MessageType", "RAW")
    signature_b64 = data.get("Signature", "")
    algorithm = data.get("SigningAlgorithm", "RSASSA_PKCS1_V1_5_SHA_256")

    message = base64.b64decode(message_b64) if isinstance(message_b64, str) else message_b64
    signature = base64.b64decode(signature_b64) if isinstance(signature_b64, str) else signature_b64

    public_key = rec["_private_key"].public_key()
    if algorithm in ("ED25519_SHA_512", "ED25519_PH_SHA_512"):
        if rec["KeySpec"] != "ECC_NIST_EDWARDS25519":
            return error_response_json(
                "UnsupportedOperationException",
                f"Signing algorithm {algorithm} is not supported for this key",
                400,
            )
        if algorithm == "ED25519_SHA_512" and message_type != "RAW":
            return error_response_json(
                "UnsupportedOperationException",
                "ED25519_SHA_512 signing algorithm requires MessageType=RAW",
                400,
            )
        if algorithm == "ED25519_PH_SHA_512":
            return error_response_json(
                "UnsupportedOperationException",
                "ED25519_PH_SHA_512 (Ed25519ph) verification is not yet implemented in this emulator",
                400,
            )
        try:
            public_key.verify(signature, message)
        except InvalidSignature:
            return error_response_json(
                "KMSInvalidSignatureException",
                "Signature verification failed",
                400,
            )
        return json_response({
            "KeyId": rec["Arn"],
            "SignatureValid": True,
            "SigningAlgorithm": algorithm,
        })

    pad, hash_algo = _signing_params(algorithm, for_verify=True)
    if hash_algo is None:
        return error_response_json(
            "UnsupportedOperationException",
            f"Signing algorithm {algorithm} is not supported",
            400,
        )

    try:
        if pad is None:
            # ECDSA
            if message_type == "DIGEST":
                public_key.verify(signature, message, ec.ECDSA(utils.Prehashed(hash_algo)))
            else:
                public_key.verify(signature, message, ec.ECDSA(hash_algo))
        else:
            # RSA
            if message_type == "DIGEST":
                public_key.verify(signature, message, pad, utils.Prehashed(hash_algo))
            else:
                public_key.verify(signature, message, pad, hash_algo)
        valid = True
    except InvalidSignature:
        return error_response_json(
            "KMSInvalidSignatureException",
            "Signature verification failed",
            400,
        )

    return json_response({
        "KeyId": rec["Arn"],
        "SignatureValid": True,
        "SigningAlgorithm": algorithm,
    })


def _signing_params(algorithm, for_verify=False):
    """Return (padding, hash_algorithm) for a signing algorithm.

    For RSA algorithms, padding is a padding object.
    For ECDSA algorithms, padding is None (ECDSA uses ec.ECDSA() instead).
    If the algorithm is unknown, returns (None, None).
    """
    if not HAS_CRYPTO:
        return None, None

    # PSS salt_length must be MAX_LENGTH for signing, AUTO for verification
    pss_salt = padding.PSS.AUTO if for_verify else padding.PSS.MAX_LENGTH

    algo_map = {
        "RSASSA_PKCS1_V1_5_SHA_256": (padding.PKCS1v15(), hashes.SHA256()),
        "RSASSA_PKCS1_V1_5_SHA_384": (padding.PKCS1v15(), hashes.SHA384()),
        "RSASSA_PKCS1_V1_5_SHA_512": (padding.PKCS1v15(), hashes.SHA512()),
        "RSASSA_PSS_SHA_256": (
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=pss_salt,
            ),
            hashes.SHA256(),
        ),
        "RSASSA_PSS_SHA_384": (
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA384()),
                salt_length=pss_salt,
            ),
            hashes.SHA384(),
        ),
        "RSASSA_PSS_SHA_512": (
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA512()),
                salt_length=pss_salt,
            ),
            hashes.SHA512(),
        ),
        # ECDSA – padding is None; callers use ec.ECDSA(hash) instead
        "ECDSA_SHA_256": (None, hashes.SHA256()),
        "ECDSA_SHA_384": (None, hashes.SHA384()),
        "ECDSA_SHA_512": (None, hashes.SHA512()),
    }
    return algo_map.get(algorithm, (None, None))


def _blob_too_long_error(field_name, max_bytes):
    return error_response_json(
        "ValidationException",
        f"1 validation error detected: Value at '{field_name}' failed to "
        "satisfy constraint: Member must have length less than or equal to "
        f"{max_bytes}",
        400,
    )


def _decode_blob(value, field_name, max_bytes=None):
    if max_bytes is not None and isinstance(value, str) and len(value) > 4 * ((max_bytes + 2) // 3):
        return None, _blob_too_long_error(field_name, max_bytes)
    if isinstance(value, (bytes, bytearray)):
        decoded = bytes(value)
    elif isinstance(value, str):
        try:
            decoded = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError):
            return None, error_response_json(
                "SerializationException",
                f"Invalid base64 encoding for '{field_name}'",
                400,
            )
    else:
        return None, error_response_json(
            "SerializationException",
            f"Expected a base64-encoded blob for '{field_name}'",
            400,
        )
    if max_bytes is not None and len(decoded) > max_bytes:
        return None, _blob_too_long_error(field_name, max_bytes)
    return decoded, None


def _message_too_long_error():
    return error_response_json(
        "ValidationException",
        "1 validation error detected: Value at 'message' failed to satisfy "
        "constraint: Member must have length less than or equal to "
        f"{MAC_MESSAGE_MAX_BYTES}",
        400,
    )


def _mac_params(data, operation):
    algorithm = data.get("MacAlgorithm", "")
    if algorithm not in _MAC_ALGORITHM_DIGESTS:
        return None, None, None, error_response_json(
            "ValidationException",
            f"1 validation error detected: Value '{algorithm}' at 'macAlgorithm' "
            "failed to satisfy constraint: Member must satisfy enum value set: "
            f"[{', '.join(_MAC_ALGORITHM_DIGESTS)}]",
            400,
        )

    raw_message = data.get("Message", "")
    if isinstance(raw_message, str) and len(raw_message) > _MAX_MESSAGE_B64_CHARS:
        return None, None, None, _message_too_long_error()

    message, err = _decode_blob(raw_message, "message")
    if err:
        return None, None, None, err
    if not message:
        return None, None, None, error_response_json(
            "ValidationException",
            "1 validation error detected: Value at 'message' failed to satisfy "
            "constraint: Member must have length greater than or equal to 1",
            400,
        )
    if len(message) > MAC_MESSAGE_MAX_BYTES:
        return None, None, None, _message_too_long_error()

    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return None, None, None, error_response_json(
            "NotFoundException", f"Key {key_id} not found", 400
        )
    err = _check_key_state(rec)
    if err:
        return None, None, None, err
    if not _is_hmac_key(rec) or "_hmac_key" not in rec:
        return None, None, None, error_response_json(
            "InvalidKeyUsageException",
            f"{rec['Arn']} key usage is {rec.get('KeyUsage', '')} which is not "
            f"valid for {operation}. {operation} requires a key with key usage "
            f"{MAC_KEY_USAGE}.",
            400,
        )
    if algorithm not in rec.get("MacAlgorithms", []):
        return None, None, None, error_response_json(
            "InvalidKeyUsageException",
            f"Algorithm {algorithm} is incompatible with key spec "
            f"{rec.get('KeySpec', '')}.",
            400,
        )
    return rec, algorithm, message, None


def _compute_mac(rec, algorithm, message):
    return hmac.new(
        rec["_hmac_key"], message, _MAC_ALGORITHM_DIGESTS[algorithm]
    ).digest()


def _dry_run_error(operation):
    return error_response_json(
        "DryRunOperationException",
        "The request was rejected because the DryRun parameter was specified.",
        400,
    )


def _generate_mac(data):
    rec, algorithm, message, err = _mac_params(data, "GenerateMac")
    if err:
        return err
    if data.get("DryRun"):
        return _dry_run_error("GenerateMac")

    mac = _compute_mac(rec, algorithm, message)
    logger.debug(
        "Generated MAC over %d bytes with key %s (%s)",
        len(message), rec["KeyId"], algorithm,
    )
    return json_response({
        "KeyId": rec["Arn"],
        "Mac": base64.b64encode(mac).decode(),
        "MacAlgorithm": algorithm,
    })


def _verify_mac(data):
    mac, err = _decode_blob(data.get("Mac", ""), "mac", MAC_MAX_BYTES)
    if err:
        return err
    if not mac:
        return error_response_json(
            "ValidationException",
            "1 validation error detected: Value at 'mac' failed to satisfy "
            "constraint: Member must have length greater than or equal to 1",
            400,
        )

    rec, algorithm, message, err = _mac_params(data, "VerifyMac")
    if err:
        return err
    if not hmac.compare_digest(_compute_mac(rec, algorithm, message), mac):
        return error_response_json(
            "KMSInvalidMacException",
            "The request was rejected because the HMAC verification failed.",
            400,
        )
    if data.get("DryRun"):
        return _dry_run_error("VerifyMac")

    return json_response({
        "KeyId": rec["Arn"],
        "MacValid": True,
        "MacAlgorithm": algorithm,
    })


def _encrypt(data):
    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {key_id} not found", 400)
    err = _reject_hmac_key(rec, "Encrypt") or _check_key_state(rec)
    if err:
        return err

    plaintext_b64 = data.get("Plaintext", "")
    plaintext = base64.b64decode(plaintext_b64) if isinstance(plaintext_b64, str) else plaintext_b64
    enc_context = data.get("EncryptionContext", {})

    if "_symmetric_key" in rec:
        # Fake symmetric encryption: XOR with a key-derived pad.
        # This is NOT real AES, but sufficient for emulation. The
        # ciphertext is: key_id_bytes(36) + context_hash(32) + xor_encrypted_data.
        # EncryptionContext is mixed into key derivation so decrypt
        # must supply the same context or get different plaintext.
        nonce = os.urandom(16)
        key_bytes = _derive_with_context(rec["_symmetric_key"], enc_context, nonce)
        pad_stream = _expand_key(key_bytes, len(plaintext))
        encrypted = bytes(a ^ b for a, b in zip(plaintext, pad_stream))
        ctx_hash = hashlib.sha256(
            json.dumps(enc_context, sort_keys=True).encode()
        ).digest()
        # Layout: key_id(36) + ctx_hash(32) + nonce(16) + xor_encrypted_data.
        ciphertext = rec["KeyId"].encode() + ctx_hash + nonce + encrypted
    elif "_private_key" in rec and rec["KeyUsage"] == "ENCRYPT_DECRYPT":
        if enc_context:
            return error_response_json(
                "UnsupportedOperationException",
                "EncryptionContext is not supported with asymmetric keys",
                400,
            )
        err = _require_crypto("Encrypt")
        if err:
            return err
        public_key = rec["_private_key"].public_key()
        ciphertext = public_key.encrypt(
            plaintext,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )
    else:
        return error_response_json(
            "UnsupportedOperationException",
            "This key cannot be used for encryption",
            400,
        )

    return json_response({
        "KeyId": rec["Arn"],
        "CiphertextBlob": base64.b64encode(ciphertext).decode(),
        "EncryptionAlgorithm": data.get(
            "EncryptionAlgorithm", "SYMMETRIC_DEFAULT"
        ),
    })


def _decrypt(data):
    ciphertext_b64 = data.get("CiphertextBlob", "")
    ciphertext = base64.b64decode(ciphertext_b64) if isinstance(ciphertext_b64, str) else ciphertext_b64
    enc_context = data.get("EncryptionContext", {})

    # For symmetric keys the ciphertext is: key_id(36) + ctx_hash(32) + encrypted_data
    key_id_from_data = data.get("KeyId", "")
    rec = None

    if key_id_from_data:
        rec = _resolve_key(key_id_from_data)

    # Try extracting key ID from ciphertext prefix (symmetric)
    if not rec and len(ciphertext) > 68:
        embedded_id = ciphertext[:36].decode("utf-8", errors="ignore")
        rec = _resolve_key(embedded_id)

    if not rec:
        # AWS rule: NotFoundException only when the caller named a specific
        # KeyId that doesn't exist. A short/garbage ciphertext with no
        # explicit KeyId can't be parsed → InvalidCiphertextException.
        if key_id_from_data:
            return error_response_json(
                "NotFoundException",
                f"Key {key_id_from_data} not found",
                400,
            )
        return error_response_json(
            "InvalidCiphertextException",
            "",
            400,
        )
    err = _check_key_state(rec) or _reject_hmac_key(rec, "Decrypt")
    if err:
        return err

    if "_symmetric_key" in rec:
        stored_ctx_hash = ciphertext[36:68]
        provided_ctx_hash = hashlib.sha256(
            json.dumps(enc_context, sort_keys=True).encode()
        ).digest()
        if stored_ctx_hash != provided_ctx_hash:
            return error_response_json(
                "InvalidCiphertextException",
                "EncryptionContext does not match",
                400,
            )
        # Layout: key_id(36) + ctx_hash(32) + nonce(16) + xor_encrypted_data.
        nonce = ciphertext[68:84]
        encrypted_data = ciphertext[84:]
        key_bytes = _derive_with_context(rec["_symmetric_key"], enc_context, nonce)
        pad_stream = _expand_key(key_bytes, len(encrypted_data))
        plaintext = bytes(a ^ b for a, b in zip(encrypted_data, pad_stream))
    elif "_private_key" in rec:
        if enc_context:
            return error_response_json(
                "UnsupportedOperationException",
                "EncryptionContext is not supported with asymmetric keys",
                400,
            )
        err = _require_crypto("Decrypt")
        if err:
            return err
        try:
            plaintext = rec["_private_key"].decrypt(
                ciphertext,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None,
                ),
            )
        except ValueError:
            # AWS KMS doesn't surface the underlying crypto library's error
            # text; the documented InvalidCiphertextException message format
            # is empty / opaque.
            return error_response_json(
                "InvalidCiphertextException",
                "",
                400,
            )
    else:
        return error_response_json(
            "UnsupportedOperationException",
            "This key cannot be used for decryption",
            400,
        )

    return json_response({
        "KeyId": rec["Arn"],
        "Plaintext": base64.b64encode(plaintext).decode(),
        "EncryptionAlgorithm": data.get(
            "EncryptionAlgorithm", "SYMMETRIC_DEFAULT"
        ),
    })


def _generate_data_key_common(data, action="GenerateDataKey"):
    """Shared logic for GenerateDataKey and GenerateDataKeyWithoutPlaintext."""
    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return None, None, error_response_json(
            "NotFoundException", f"Key {key_id} not found", 400
        )
    err = _check_key_state(rec) or _reject_hmac_key(rec, action)
    if err:
        return None, None, err
    if "_symmetric_key" not in rec:
        return None, None, error_response_json(
            "UnsupportedOperationException",
            f"{action} requires a symmetric key",
            400,
        )

    spec = data.get("KeySpec", "AES_256")
    length = data.get("NumberOfBytes")
    if length:
        data_key = os.urandom(length)
    elif spec == "AES_256":
        data_key = os.urandom(32)
    elif spec == "AES_128":
        data_key = os.urandom(16)
    else:
        data_key = os.urandom(32)

    enc_context = data.get("EncryptionContext", {})
    nonce = os.urandom(16)
    cmk_bytes = _derive_with_context(rec["_symmetric_key"], enc_context, nonce)
    pad_stream = _expand_key(cmk_bytes, len(data_key))
    encrypted = bytes(a ^ b for a, b in zip(data_key, pad_stream))
    ctx_hash = hashlib.sha256(
        json.dumps(enc_context, sort_keys=True).encode()
    ).digest()
    # Layout: key_id(36) + ctx_hash(32) + nonce(16) + xor_encrypted_data.
    ciphertext = rec["KeyId"].encode() + ctx_hash + nonce + encrypted

    return rec, data_key, ciphertext


def _generate_data_key(data):
    rec, data_key, result = _generate_data_key_common(data)
    if rec is None:
        # result is an error response tuple
        return result
    return json_response({
        "KeyId": rec["Arn"],
        "Plaintext": base64.b64encode(data_key).decode(),
        "CiphertextBlob": base64.b64encode(result).decode(),
    })


def _generate_data_key_pair_common(data, action):
    """Shared logic for GenerateDataKeyPair and GenerateDataKeyPairWithoutPlaintext.

    Generates a data key pair and wraps the private key under the CMK. `action`
    is the caller's operation name, so errors name the operation the client
    actually invoked rather than whichever variant this helper was written for.

    Returns (payload, private_key_der, None) on success, or (None, None, error)
    on failure. The two operations differ in exactly one thing: whether
    PrivateKeyPlaintext gets added to the payload -- so the caller decides that
    and nothing else.

    KeyPairSpec follows the real AWS enum, minus SM2 (which _create_key does not
    implement either). The asymmetric specs now match _create_key's exactly.
    """
    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return None, None, error_response_json(
            "NotFoundException", f"Key {key_id} not found", 400
        )
    err = _check_key_state(rec) or _reject_hmac_key(rec, action)
    if err:
        return None, None, err
    # The CMK must be symmetric: it wraps the generated private key. Real AWS
    # rejects an asymmetric CMK here with InvalidKeyUsageException (the KeyUsage
    # is incompatible with generating data keys), not UnsupportedOperationException.
    if "_symmetric_key" not in rec:
        return None, None, error_response_json(
            "InvalidKeyUsageException",
            f"{action} requires a symmetric key",
            400,
        )
    err = _require_crypto(action)
    if err:
        return None, None, err

    spec = data.get("KeyPairSpec", "")
    if spec in ("RSA_2048", "RSA_3072", "RSA_4096"):
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=int(spec.split("_")[1])
        )
    elif spec in ("ECC_NIST_P256", "ECC_NIST_P384", "ECC_NIST_P521", "ECC_SECG_P256K1"):
        curve_map = {
            "ECC_NIST_P256": ec.SECP256R1(),
            "ECC_NIST_P384": ec.SECP384R1(),
            "ECC_NIST_P521": ec.SECP521R1(),
            "ECC_SECG_P256K1": ec.SECP256K1(),
        }
        private_key = ec.generate_private_key(curve_map[spec])
    elif spec == "ECC_NIST_EDWARDS25519":
        private_key = ed25519.Ed25519PrivateKey.generate()
    else:
        return None, None, error_response_json(
            "ValidationException",
            f"1 validation error detected: Value '{spec}' at 'keyPairSpec' "
            "failed to satisfy constraint: Member must satisfy enum value set: "
            "[RSA_2048, RSA_3072, RSA_4096, ECC_NIST_P256, ECC_NIST_P384, "
            "ECC_NIST_P521, ECC_SECG_P256K1, ECC_NIST_EDWARDS25519]",
            400,
        )

    private_der = private_key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    public_der = private_key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    enc_context = data.get("EncryptionContext", {})
    # Ciphertext layout: key_id(36) + ctx_hash(32) + nonce(16) + xor_encrypted_data.
    # The per-blob nonce is what stops the wrapped private key from leaking:
    # without it the keystream repeats across every blob sharing this CMK and
    # EncryptionContext. Must stay in step with _encrypt,
    # _generate_data_key_common and _decrypt, which spell out this layout by hand.
    nonce = os.urandom(16)
    cmk_bytes = _derive_with_context(rec["_symmetric_key"], enc_context, nonce)
    pad_stream = _expand_key(cmk_bytes, len(private_der))
    encrypted = bytes(a ^ b for a, b in zip(private_der, pad_stream))
    ctx_hash = hashlib.sha256(
        json.dumps(enc_context, sort_keys=True).encode()
    ).digest()
    ciphertext = rec["KeyId"].encode() + ctx_hash + nonce + encrypted

    logger.info("Generated data key pair %s under CMK %s", spec, rec["KeyId"])
    payload = {
        "KeyId": rec["Arn"],
        "KeyPairSpec": spec,
        "PrivateKeyCiphertextBlob": base64.b64encode(ciphertext).decode(),
        "PublicKey": base64.b64encode(public_der).decode(),
    }
    return payload, private_der, None


def _generate_data_key_pair(data):
    payload, private_der, err = _generate_data_key_pair_common(
        data, "GenerateDataKeyPair"
    )
    if err:
        return err
    payload["PrivateKeyPlaintext"] = base64.b64encode(private_der).decode()
    return json_response(payload)


def _generate_data_key_pair_without_plaintext(data):
    """Same as GenerateDataKeyPair, minus PrivateKeyPlaintext.

    That omission is the whole point of this variant: the private key exists
    only inside this process for the moment it takes to wrap it, so the caller
    can persist the ciphertext + public key without ever holding the private
    key material.
    """
    payload, _private_der, err = _generate_data_key_pair_common(
        data, "GenerateDataKeyPairWithoutPlaintext"
    )
    if err:
        return err
    return json_response(payload)


def _generate_data_key_without_plaintext(data):
    rec, _data_key, result = _generate_data_key_common(
        data, "GenerateDataKeyWithoutPlaintext"
    )
    if rec is None:
        return result
    return json_response({
        "KeyId": rec["Arn"],
        "CiphertextBlob": base64.b64encode(result).decode(),
    })


def _derive_with_context(key_bytes, enc_context, nonce):
    """Mix EncryptionContext and a per-blob nonce into key material.

    The nonce makes the keystream unique per ciphertext even under the same CMK
    and EncryptionContext, so wrapping a partly-predictable plaintext (e.g. a
    key pair's DER, whose public half is recoverable from the returned
    PublicKey) no longer leaks the keystream. decrypt reads the nonce back out
    of the ciphertext layout.
    """
    ctx_bytes = json.dumps(enc_context, sort_keys=True).encode()
    return hashlib.sha256(key_bytes + ctx_bytes + nonce).digest()


def _expand_key(key_bytes, length):
    """Expand a key to the required length using SHA-256 chaining."""
    result = b""
    counter = 0
    while len(result) < length:
        result += hashlib.sha256(key_bytes + counter.to_bytes(4, "big")).digest()
        counter += 1
    return result[:length]


# ---- Alias operations ----


def _create_alias(data):
    alias_name = data.get("AliasName", "")
    target_key_id = data.get("TargetKeyId", "")
    if not alias_name or not alias_name.startswith("alias/"):
        return error_response_json("ValidationException", "AliasName must start with alias/", 400)
    if not target_key_id:
        return error_response_json("ValidationException", "TargetKeyId is required", 400)
    rec = _resolve_key(target_key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {target_key_id} not found", 400)
    alias_arn = _alias_arn(alias_name)
    if alias_arn in _aliases:
        return error_response_json("AlreadyExistsException", f"Alias {alias_name} already exists", 400)
    _aliases[alias_arn] = rec["KeyId"]
    logger.info("Created alias %s -> %s", alias_name, rec["KeyId"])
    return json_response({})


def _delete_alias(data):
    alias_name = data.get("AliasName", "")
    alias_arn = _alias_arn(alias_name)
    if alias_arn not in _aliases:
        return error_response_json("NotFoundException", f"Alias {alias_name} not found", 400)
    del _aliases[alias_arn]
    return json_response({})


def _list_aliases(data):
    key_id = data.get("KeyId")
    items = []
    for alias_arn, target_id in _aliases.items():
        try:
            spec = parse_arn(alias_arn)
        except ArnParseError:
            continue
        if spec.service != "kms" or spec.region != get_region() or spec.account_id != get_account_id():
            continue
        if key_id and target_id != key_id:
            rec = _resolve_key(key_id)
            if not rec or rec["KeyId"] != target_id:
                continue
        items.append({
            "AliasName": spec.resource,
            "AliasArn": alias_arn,
            "TargetKeyId": target_id,
        })
    return json_response({"Aliases": items, "Truncated": False})


def _update_alias(data):
    alias_name = data.get("AliasName", "")
    target_key_id = data.get("TargetKeyId", "")
    alias_arn = _alias_arn(alias_name)
    if alias_arn not in _aliases:
        return error_response_json("NotFoundException", f"Alias {alias_name} not found", 400)
    rec = _resolve_key(target_key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {target_key_id} not found", 400)
    _aliases[alias_arn] = rec["KeyId"]
    return json_response({})


# ---- Key Rotation ----


def _reject_rotation_for_hmac(rec, operation):
    if _is_hmac_key(rec):
        return error_response_json(
            "UnsupportedOperationException",
            f"{operation} is not supported for HMAC KMS keys ({rec['Arn']}).",
            400,
        )
    return None


def _enable_key_rotation(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    err = _reject_rotation_for_hmac(rec, "EnableKeyRotation")
    if err:
        return err
    rec["KeyRotationEnabled"] = True
    rec["RotationPeriodInDays"] = data.get("RotationPeriodInDays", 365)
    return json_response({})


def _disable_key_rotation(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    err = _reject_rotation_for_hmac(rec, "DisableKeyRotation")
    if err:
        return err
    rec["KeyRotationEnabled"] = False
    return json_response({})


def _get_key_rotation_status(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    return json_response({
        "KeyRotationEnabled": rec.get("KeyRotationEnabled", False),
        "RotationPeriodInDays": rec.get("RotationPeriodInDays", 365),
    })


# ---- Key Policy ----


def _get_key_policy(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    policy = rec.get("Policy")
    return json_response({"Policy": policy, "PolicyName": "default"})


def _put_key_policy(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    rec["Policy"] = data.get("Policy", "")
    return json_response({})


def _list_key_policies(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    return json_response({"PolicyNames": ["default"], "Truncated": False})


# ---- Enable / Disable / Schedule Deletion ----


def _enable_key(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    rec["Enabled"] = True
    rec["KeyState"] = "Enabled"
    return json_response({})


def _disable_key(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    rec["Enabled"] = False
    rec["KeyState"] = "Disabled"
    return json_response({})


def _update_key_description(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    # An explicit empty string clears the description, which is how AWS removes it.
    rec["Description"] = data.get("Description", "")
    return json_response({})


def _schedule_key_deletion(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    days = data.get("PendingWindowInDays", 30)
    rec["KeyState"] = "PendingDeletion"
    rec["Enabled"] = False
    rec["DeletionDate"] = int(time.time() + (days * 86400))
    return json_response({
        "KeyId": rec["Arn"],
        "KeyState": "PendingDeletion",
        "DeletionDate": rec["DeletionDate"],
    })


def _cancel_key_deletion(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    rec["KeyState"] = "Disabled"
    rec.pop("DeletionDate", None)
    return json_response({"KeyId": rec["Arn"]})


# ---- Tags ----


def _tag_resource(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    tags = rec.setdefault("Tags", [])
    for tag in data.get("Tags", []):
        existing = next((t for t in tags if t["TagKey"] == tag["TagKey"]), None)
        if existing:
            existing["TagValue"] = tag["TagValue"]
        else:
            tags.append(tag)
    return json_response({})


def _untag_resource(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    remove_keys = set(data.get("TagKeys", []))
    rec["Tags"] = [t for t in rec.get("Tags", []) if t["TagKey"] not in remove_keys]
    return json_response({})


def _list_resource_tags(data):
    rec = _resolve_key(data.get("KeyId", ""))
    if not rec:
        return error_response_json("NotFoundException", f"Key {data.get('KeyId', '')} not found", 400)
    return json_response({"Tags": rec.get("Tags", []), "Truncated": False})


# ---- Request handler ----

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateKey": _create_key,
        "ListKeys": _list_keys,
        "DescribeKey": _describe_key,
        "GetPublicKey": _get_public_key,
        "Sign": _sign,
        "Verify": _verify,
        "Encrypt": _encrypt,
        "Decrypt": _decrypt,
        "GenerateMac": _generate_mac,
        "VerifyMac": _verify_mac,
        "GenerateDataKey": _generate_data_key,
        "GenerateDataKeyWithoutPlaintext": _generate_data_key_without_plaintext,
        "GenerateDataKeyPair": _generate_data_key_pair,
        "GenerateDataKeyPairWithoutPlaintext": _generate_data_key_pair_without_plaintext,
        "CreateAlias": _create_alias,
        "DeleteAlias": _delete_alias,
        "ListAliases": _list_aliases,
        "UpdateAlias": _update_alias,
        "EnableKeyRotation": _enable_key_rotation,
        "DisableKeyRotation": _disable_key_rotation,
        "GetKeyRotationStatus": _get_key_rotation_status,
        "GetKeyPolicy": _get_key_policy,
        "PutKeyPolicy": _put_key_policy,
        "ListKeyPolicies": _list_key_policies,
        "EnableKey": _enable_key,
        "DisableKey": _disable_key,
        "UpdateKeyDescription": _update_key_description,
        "ScheduleKeyDeletion": _schedule_key_deletion,
        "CancelKeyDeletion": _cancel_key_deletion,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListResourceTags": _list_resource_tags,
    }

    handler = handlers.get(action)
    if not handler:
        logger.warning("Unknown KMS action: %s", action)
        return error_response_json(
            "InvalidAction", f"Unknown action: {action}", 400
        )
    return handler(data)


def reset():
    _keys.clear()
    _aliases.clear()
