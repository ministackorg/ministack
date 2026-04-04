"""
KMS (Key Management Service) Emulator.
JSON-based API via X-Amz-Target (prefix: TrentService).
Supports: CreateKey, ListKeys, DescribeKey, Sign, Verify,
          Encrypt, Decrypt, GenerateDataKey,
          GenerateDataKeyWithoutPlaintext.
"""

import base64
import hashlib
import json
import logging
import os
import time

from ministack.core.responses import error_response_json, json_response, new_uuid

logger = logging.getLogger("kms")

try:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding, rsa, utils
    HAS_CRYPTO = True
except ImportError:
    InvalidSignature = Exception
    HAS_CRYPTO = False
    logger.warning(
        "cryptography package not installed; "
        "KMS Sign/Verify will return errors. "
        "Install with: pip install cryptography"
    )

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

from ministack.core.persistence import load_state, PERSIST_STATE

_keys: dict = {}
# key_id -> {
#     KeyId, Arn, KeyState, KeyUsage, KeySpec, Description,
#     CreationDate, Enabled, Origin,
#     _private_key (rsa private key object, RSA only),
#     _public_key_der (bytes, RSA only),
#     _symmetric_key (bytes, SYMMETRIC_DEFAULT only),
# }
_aliases: dict = {}  # alias_name -> key_id (e.g. "alias/my-key" -> "uuid")


# ── Persistence ────────────────────────────────────────────

def get_state():
    """Return JSON-serializable state. Symmetric keys are base64-encoded;
    RSA private keys are PEM-encoded if cryptography is available."""
    serializable_keys = {}
    for kid, rec in _keys.items():
        entry = {k: v for k, v in rec.items()
                 if k not in ("_private_key", "_public_key_der", "_symmetric_key")}
        if "_symmetric_key" in rec:
            entry["_symmetric_key_b64"] = base64.b64encode(rec["_symmetric_key"]).decode()
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
        serializable_keys[kid] = entry
    return {"keys": serializable_keys, "aliases": _aliases}


def restore_state(data):
    if data:
        for kid, entry in data.get("keys", {}).items():
            if "_symmetric_key_b64" in entry:
                entry["_symmetric_key"] = base64.b64decode(entry.pop("_symmetric_key_b64"))
            if "_public_key_der_b64" in entry:
                entry["_public_key_der"] = base64.b64decode(entry.pop("_public_key_der_b64"))
            if "_private_key_pem" in entry and HAS_CRYPTO:
                try:
                    pem_bytes = base64.b64decode(entry.pop("_private_key_pem"))
                    entry["_private_key"] = serialization.load_pem_private_key(pem_bytes, password=None)
                except Exception:
                    pass
            _keys[kid] = entry
        _aliases.update(data.get("aliases", {}))


_restored = load_state("kms")
if _restored:
    restore_state(_restored)


def _arn(key_id):
    return f"arn:aws:kms:{REGION}:{ACCOUNT_ID}:key/{key_id}"


def _key_metadata(rec):
    return {
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
        "EncryptionAlgorithms": rec.get("EncryptionAlgorithms", []),
        "SigningAlgorithms": rec.get("SigningAlgorithms", []),
    }


def _resolve_key(key_id_or_arn):
    if not key_id_or_arn:
        return None
    # Direct key ID lookup
    if key_id_or_arn in _keys:
        return _keys[key_id_or_arn]
    # ARN lookup
    for rec in _keys.values():
        if rec["Arn"] == key_id_or_arn:
            return rec
    # Alias lookup: "alias/my-key" or "arn:aws:kms:...:alias/my-key"
    alias_name = key_id_or_arn
    if ":alias/" in alias_name:
        alias_name = "alias/" + alias_name.split(":alias/")[-1]
    if alias_name in _aliases:
        return _keys.get(_aliases[alias_name])
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
    description = data.get("Description", "")

    rec = {
        "KeyId": key_id,
        "Arn": _arn(key_id),
        "KeyState": "Enabled",
        "Enabled": True,
        "KeySpec": key_spec,
        "KeyUsage": key_usage,
        "Description": description,
        "CreationDate": time.time(),
        "Origin": "AWS_KMS",
    }

    if key_spec == "SYMMETRIC_DEFAULT":
        rec["_symmetric_key"] = os.urandom(32)
        rec["EncryptionAlgorithms"] = ["SYMMETRIC_DEFAULT"]
        rec["SigningAlgorithms"] = []
    elif key_spec in ("RSA_2048", "RSA_4096"):
        err = _require_crypto("CreateKey")
        if err:
            return err
        bits = 2048 if key_spec == "RSA_2048" else 4096
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=bits
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
    if "_public_key_der" not in rec:
        return error_response_json(
            "UnsupportedOperationException",
            "GetPublicKey is only valid for asymmetric keys",
            400,
        )
    return json_response({
        "KeyId": rec["KeyId"],
        "KeyUsage": rec["KeyUsage"],
        "KeySpec": rec["KeySpec"],
        "PublicKey": base64.b64encode(rec["_public_key_der"]).decode(),
        "SigningAlgorithms": rec.get("SigningAlgorithms", []),
        "EncryptionAlgorithms": rec.get("EncryptionAlgorithms", []),
    })


def _sign(data):
    err = _require_crypto("Sign")
    if err:
        return err

    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {key_id} not found", 400)
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

    pad, hash_algo = _signing_params(algorithm)
    if pad is None:
        return error_response_json(
            "UnsupportedOperationException",
            f"Signing algorithm {algorithm} is not supported",
            400,
        )

    if message_type == "DIGEST":
        signature = rec["_private_key"].sign(
            message, pad, utils.Prehashed(hash_algo)
        )
    else:
        signature = rec["_private_key"].sign(message, pad, hash_algo)

    logger.debug("Signed %d bytes with key %s (%s)", len(message), key_id, algorithm)
    return json_response({
        "KeyId": rec["KeyId"],
        "Signature": base64.b64encode(signature).decode(),
        "SigningAlgorithm": algorithm,
    })


def _verify(data):
    err = _require_crypto("Verify")
    if err:
        return err

    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {key_id} not found", 400)
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

    pad, hash_algo = _signing_params(algorithm, for_verify=True)
    if pad is None:
        return error_response_json(
            "UnsupportedOperationException",
            f"Signing algorithm {algorithm} is not supported",
            400,
        )

    public_key = rec["_private_key"].public_key()
    try:
        if message_type == "DIGEST":
            public_key.verify(signature, message, pad, utils.Prehashed(hash_algo))
        else:
            public_key.verify(signature, message, pad, hash_algo)
        valid = True
    except InvalidSignature:
        valid = False

    return json_response({
        "KeyId": rec["KeyId"],
        "SignatureValid": valid,
        "SigningAlgorithm": algorithm,
    })


def _signing_params(algorithm, for_verify=False):
    """Return (padding, hash_algorithm) for a signing algorithm."""
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
    }
    return algo_map.get(algorithm, (None, None))


def _encrypt(data):
    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {key_id} not found", 400)

    plaintext_b64 = data.get("Plaintext", "")
    plaintext = base64.b64decode(plaintext_b64) if isinstance(plaintext_b64, str) else plaintext_b64
    enc_context = data.get("EncryptionContext", {})

    if "_symmetric_key" in rec:
        # Fake symmetric encryption: XOR with a key-derived pad.
        # This is NOT real AES, but sufficient for emulation. The
        # ciphertext is: key_id_bytes(36) + context_hash(32) + xor_encrypted_data.
        # EncryptionContext is mixed into key derivation so decrypt
        # must supply the same context or get different plaintext.
        key_bytes = _derive_with_context(rec["_symmetric_key"], enc_context)
        pad_stream = _expand_key(key_bytes, len(plaintext))
        encrypted = bytes(a ^ b for a, b in zip(plaintext, pad_stream))
        ctx_hash = hashlib.sha256(
            json.dumps(enc_context, sort_keys=True).encode()
        ).digest()
        ciphertext = rec["KeyId"].encode() + ctx_hash + encrypted
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
        "KeyId": rec["KeyId"],
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
        return error_response_json(
            "NotFoundException",
            "Unable to find the key for decryption",
            400,
        )

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
        encrypted_data = ciphertext[68:]
        key_bytes = _derive_with_context(rec["_symmetric_key"], enc_context)
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
        except ValueError as e:
            return error_response_json(
                "InvalidCiphertextException",
                str(e),
                400,
            )
    else:
        return error_response_json(
            "UnsupportedOperationException",
            "This key cannot be used for decryption",
            400,
        )

    return json_response({
        "KeyId": rec["KeyId"],
        "Plaintext": base64.b64encode(plaintext).decode(),
        "EncryptionAlgorithm": data.get(
            "EncryptionAlgorithm", "SYMMETRIC_DEFAULT"
        ),
    })


def _generate_data_key_common(data):
    """Shared logic for GenerateDataKey and GenerateDataKeyWithoutPlaintext."""
    key_id = data.get("KeyId", "")
    rec = _resolve_key(key_id)
    if not rec:
        return None, None, error_response_json(
            "NotFoundException", f"Key {key_id} not found", 400
        )
    if "_symmetric_key" not in rec:
        return None, None, error_response_json(
            "UnsupportedOperationException",
            "GenerateDataKey requires a symmetric key",
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
    cmk_bytes = _derive_with_context(rec["_symmetric_key"], enc_context)
    pad_stream = _expand_key(cmk_bytes, len(data_key))
    encrypted = bytes(a ^ b for a, b in zip(data_key, pad_stream))
    ctx_hash = hashlib.sha256(
        json.dumps(enc_context, sort_keys=True).encode()
    ).digest()
    ciphertext = rec["KeyId"].encode() + ctx_hash + encrypted

    return rec, data_key, ciphertext


def _generate_data_key(data):
    rec, data_key, result = _generate_data_key_common(data)
    if rec is None:
        # result is an error response tuple
        return result
    return json_response({
        "KeyId": rec["KeyId"],
        "Plaintext": base64.b64encode(data_key).decode(),
        "CiphertextBlob": base64.b64encode(result).decode(),
    })


def _generate_data_key_without_plaintext(data):
    rec, _data_key, result = _generate_data_key_common(data)
    if rec is None:
        return result
    return json_response({
        "KeyId": rec["KeyId"],
        "CiphertextBlob": base64.b64encode(result).decode(),
    })


def _derive_with_context(key_bytes, enc_context):
    """Mix EncryptionContext into key material so decrypt requires the same context."""
    ctx_bytes = json.dumps(enc_context, sort_keys=True).encode()
    return hashlib.sha256(key_bytes + ctx_bytes).digest()


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
    if alias_name in _aliases:
        return error_response_json("AlreadyExistsException", f"Alias {alias_name} already exists", 400)
    _aliases[alias_name] = rec["KeyId"]
    logger.info("Created alias %s -> %s", alias_name, rec["KeyId"])
    return json_response({})


def _delete_alias(data):
    alias_name = data.get("AliasName", "")
    if alias_name not in _aliases:
        return error_response_json("NotFoundException", f"Alias {alias_name} not found", 400)
    del _aliases[alias_name]
    return json_response({})


def _list_aliases(data):
    key_id = data.get("KeyId")
    items = []
    for alias_name, target_id in _aliases.items():
        if key_id and target_id != key_id:
            rec = _resolve_key(key_id)
            if not rec or rec["KeyId"] != target_id:
                continue
        items.append({
            "AliasName": alias_name,
            "AliasArn": f"arn:aws:kms:{REGION}:{ACCOUNT_ID}:{alias_name}",
            "TargetKeyId": target_id,
        })
    return json_response({"Aliases": items, "Truncated": False})


def _update_alias(data):
    alias_name = data.get("AliasName", "")
    target_key_id = data.get("TargetKeyId", "")
    if alias_name not in _aliases:
        return error_response_json("NotFoundException", f"Alias {alias_name} not found", 400)
    rec = _resolve_key(target_key_id)
    if not rec:
        return error_response_json("NotFoundException", f"Key {target_key_id} not found", 400)
    _aliases[alias_name] = rec["KeyId"]
    return json_response({})


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
        "GenerateDataKey": _generate_data_key,
        "GenerateDataKeyWithoutPlaintext": _generate_data_key_without_plaintext,
        "CreateAlias": _create_alias,
        "DeleteAlias": _delete_alias,
        "ListAliases": _list_aliases,
        "UpdateAlias": _update_alias,
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
