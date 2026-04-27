"""
ACM (Certificate Manager) Service Emulator.
JSON-based API via X-Amz-Target.
Supports: RequestCertificate, DescribeCertificate, ListCertificates,
          DeleteCertificate, GetCertificate, ImportCertificate,
          AddTagsToCertificate, RemoveTagsFromCertificate, ListTagsForCertificate,
          UpdateCertificateOptions, RenewCertificate, ResendValidationEmail.
"""

import copy
import json
import os
import logging
import time

from ministack.core.persistence import PERSIST_STATE, load_state

from ministack.core.responses import AccountScopedDict, get_account_id, error_response_json, json_response, new_uuid, now_iso, get_region

logger = logging.getLogger("acm")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_certificates = AccountScopedDict()  # arn -> certificate dict


def get_state():
    # Strip _private_key before persisting — real AWS only exposes the
    # private key via passphrase-protected ExportCertificate, and the
    # GetCertificate path already honours that. Writing it plaintext to
    # ${STATE_DIR}/acm.json would turn warm-boot persistence into a
    # side-channel for material that the wire protocol refuses to leak.
    # The cert body and chain still round-trip; only PrivateKey is lost,
    # which means a re-import is required after restart for IMPORTED
    # certs that need the key.
    # Iterate _data directly (not items()) so the snapshot includes
    # every tenant's certificates — items() is request-scoped to the
    # current account and would silently drop other tenants' certs
    # from the persisted snapshot, breaking multi-tenancy across
    # warm boots.
    scrubbed = copy.deepcopy(_certificates)
    for cert in scrubbed._data.values():
        cert.pop("_private_key", None)
    return {"_certificates": scrubbed}


def _synthetic_pem(domain):
    """A clearly-synthetic but syntactically PEM-decodable placeholder
    for RequestCertificate-issued certs. The emulator does not generate
    real X.509, so anything that actually parses ASN.1 will still fail,
    but the PEM body must remain valid base64 so consumers that pre-
    decode (PyOpenSSL, cryptography) don't error before they get to the
    parser. The requested domain lives in DomainName / SubjectAlternative
    Names metadata, not embedded in the PEM payload.

    Defined above the import-time `restore_state` block (rather than
    next to its other call site in `_request_certificate`) so the
    backfill path doesn't NameError when the load_state try block
    fires at module import."""
    _ = domain  # represented in cert metadata, not the base64 block
    return (
        "-----BEGIN CERTIFICATE-----\n"
        "AQIDBAUGBwgJCgsMDQ4PEA==\n"
        "-----END CERTIFICATE-----\n"
    )


def restore_state(data):
    _certificates.update(data.get("_certificates", {}))
    # Backwards compat: pre-fix snapshots have certificates without
    # `_pem_body` / `_pem_chain` (the old GetCertificate path returned
    # a hard-coded literal regardless of stored data). Without backfill,
    # GetCertificate would return an empty Certificate field for those
    # certs after warm-boot — strictly worse than the old behaviour.
    # Use the synthetic placeholder so consumers that just substring-
    # check 'BEGIN CERTIFICATE' (Terraform / CDK) keep working.
    for cert in _certificates._data.values():
        if "_pem_body" not in cert:
            cert["_pem_body"] = _synthetic_pem(cert.get("DomainName", ""))
        if "_pem_chain" not in cert:
            cert["_pem_chain"] = ""


try:
    _restored = load_state("acm")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


def _future_iso(seconds):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + seconds))


def _epoch(iso_or_epoch):
    """Convert ISO timestamp to epoch float if needed. ACM API returns epoch floats."""
    if isinstance(iso_or_epoch, (int, float)):
        return float(iso_or_epoch)
    try:
        return time.mktime(time.strptime(iso_or_epoch, "%Y-%m-%dT%H:%M:%SZ")) - time.timezone
    except (ValueError, TypeError):
        return time.time()


def _cert_arn():
    return f"arn:aws:acm:{get_region()}:{get_account_id()}:certificate/{new_uuid()}"


def _validation_options(domain, method):
    return {
        "DomainName": domain,
        "ValidationMethod": method,
        "ValidationStatus": "SUCCESS",
        "ResourceRecord": {
            "Name": f"_acme-challenge.{domain}.",
            "Type": "CNAME",
            "Value": f"fake-validation-{new_uuid()[:8]}.acm.amazonaws.com.",
        },
    }


def _cert_shape(cert):
    return {
        "CertificateArn": cert["CertificateArn"],
        "DomainName": cert["DomainName"],
        "SubjectAlternativeNames": cert.get("SubjectAlternativeNames", [cert["DomainName"]]),
        "Status": cert["Status"],
        "Type": cert.get("Type", "AMAZON_ISSUED"),
        "KeyAlgorithm": "RSA_2048",
        "SignatureAlgorithm": "SHA256WITHRSA",
        "InUseBy": cert.get("InUseBy", []),
        "CreatedAt": _epoch(cert["CreatedAt"]),
        "IssuedAt": _epoch(cert.get("IssuedAt", cert["CreatedAt"])),
        "NotBefore": _epoch(cert.get("NotBefore", cert["CreatedAt"])),
        "NotAfter": _epoch(cert.get("NotAfter", _future_iso(365 * 24 * 3600))),
        "DomainValidationOptions": cert.get("DomainValidationOptions", []),
        "Options": cert.get("Options", {}),
        "Tags": cert.get("Tags", []),
    }


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "RequestCertificate": _request_certificate,
        "DescribeCertificate": _describe_certificate,
        "ListCertificates": _list_certificates,
        "DeleteCertificate": _delete_certificate,
        "GetCertificate": _get_certificate,
        "ImportCertificate": _import_certificate,
        "AddTagsToCertificate": _add_tags,
        "RemoveTagsFromCertificate": _remove_tags,
        "ListTagsForCertificate": _list_tags,
        "UpdateCertificateOptions": _update_options,
        "RenewCertificate": _renew_certificate,
        "ResendValidationEmail": _resend_validation_email,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


# (`_synthetic_pem` is defined near `restore_state` above so the
# import-time backfill path doesn't NameError.)


def _request_certificate(data):
    domain = data.get("DomainName", "")
    if not domain:
        return error_response_json("InvalidParameterException", "DomainName is required", 400)
    method = data.get("ValidationMethod", "DNS")
    sans = data.get("SubjectAlternativeNames", [domain])
    if domain not in sans:
        sans = [domain] + sans
    arn = _cert_arn()
    now = now_iso()
    _certificates[arn] = {
        "CertificateArn": arn,
        "DomainName": domain,
        "SubjectAlternativeNames": sans,
        "Status": "ISSUED",
        "Type": "AMAZON_ISSUED",
        "CreatedAt": now,
        "IssuedAt": now,
        "NotBefore": now,
        "NotAfter": _future_iso(365 * 24 * 3600),
        "DomainValidationOptions": [_validation_options(d, method) for d in sans],
        "ValidationMethod": method,
        "Tags": data.get("Tags", []),
        "Options": {},
        "_pem_body": _synthetic_pem(domain),
        "_pem_chain": "",
        "_private_key": "",
    }
    logger.info("RequestCertificate: %s -> %s", domain, arn)
    return json_response({"CertificateArn": arn})


def _describe_certificate(data):
    arn = data.get("CertificateArn", "")
    cert = _certificates.get(arn)
    if not cert:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    return json_response({"Certificate": _cert_shape(cert)})


def _list_certificates(data):
    statuses = data.get("CertificateStatuses", [])
    summaries = []
    for arn, cert in _certificates.items():
        if statuses and cert["Status"] not in statuses:
            continue
        summaries.append({
            "CertificateArn": arn,
            "DomainName": cert["DomainName"],
            "Status": cert["Status"],
        })
    # Real AWS omits NextToken when there's no next page. boto3 strips
    # null fields client-side so it tolerates `"NextToken": null`, but
    # other SDKs (Java, Go, raw HTTP) and pagination loops checking
    # `if "NextToken" in response` see the literal null and loop
    # forever. ACM emulator currently emits a single page, so always
    # omit the key.
    return json_response({"CertificateSummaryList": summaries})


def _delete_certificate(data):
    arn = data.get("CertificateArn", "")
    if arn not in _certificates:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    del _certificates[arn]
    return json_response({})


def _decode_pem_field(value):
    """ImportCertificate accepts PEM bodies as base64-encoded blobs over
    the wire. boto3 base64-encodes the bytes for us; the JSON we receive
    contains the encoded string. We store the decoded UTF-8 PEM so that
    GetCertificate can return it unchanged."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        # Try base64 first (the AWS-JSON wire shape); fall back to the
        # raw string when the body is already a PEM (some SDK paths /
        # tests skip the base64 step).
        if value.lstrip().startswith("-----"):
            return value
        try:
            import base64
            return base64.b64decode(value).decode("utf-8", errors="replace")
        except Exception:
            return value
    return ""


def _get_certificate(data):
    arn = data.get("CertificateArn", "")
    cert = _certificates.get(arn)
    if cert is None:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    # PrivateKey is intentionally never returned — real AWS only exposes
    # it via ExportCertificate (passphrase-protected).
    return json_response({
        "Certificate": cert.get("_pem_body", ""),
        "CertificateChain": cert.get("_pem_chain", ""),
    })


def _import_certificate(data):
    arn = data.get("CertificateArn") or _cert_arn()
    now = now_iso()
    cert_body = _decode_pem_field(data.get("Certificate"))
    cert_chain = _decode_pem_field(data.get("CertificateChain"))
    private_key = _decode_pem_field(data.get("PrivateKey"))
    # Synthetic DomainName: real AWS parses CN/SAN from the cert; we
    # don't ship X.509 parsing, so we emit a clearly-synthetic value
    # that doesn't claim coverage of any specific domain. Re-import
    # preserves the existing DomainName so downstream resources stay
    # stable.
    existing = _certificates.get(arn) or {}
    domain = existing.get("DomainName") or f"imported-cert-{arn.rsplit('/', 1)[-1][:8]}.invalid"
    _certificates[arn] = {
        "CertificateArn": arn,
        "DomainName": domain,
        "SubjectAlternativeNames": existing.get("SubjectAlternativeNames", [domain]),
        "Status": "ISSUED",
        "Type": "IMPORTED",
        "CreatedAt": existing.get("CreatedAt", now),
        "IssuedAt": now,
        "NotBefore": now,
        "NotAfter": _future_iso(365 * 24 * 3600),
        "DomainValidationOptions": [],
        "Tags": data.get("Tags", existing.get("Tags", [])),
        "Options": existing.get("Options", {}),
        "_pem_body": cert_body,
        "_pem_chain": cert_chain,
        "_private_key": private_key,
    }
    return json_response({"CertificateArn": arn})


def _add_tags(data):
    arn = data.get("CertificateArn", "")
    cert = _certificates.get(arn)
    if not cert:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    existing = {t["Key"]: t for t in cert.get("Tags", [])}
    for tag in data.get("Tags", []):
        existing[tag["Key"]] = tag
    cert["Tags"] = list(existing.values())
    return json_response({})


def _remove_tags(data):
    arn = data.get("CertificateArn", "")
    cert = _certificates.get(arn)
    if not cert:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    remove_keys = {t["Key"] for t in data.get("Tags", [])}
    cert["Tags"] = [t for t in cert.get("Tags", []) if t["Key"] not in remove_keys]
    return json_response({})


def _list_tags(data):
    arn = data.get("CertificateArn", "")
    cert = _certificates.get(arn)
    if not cert:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    return json_response({"Tags": cert.get("Tags", [])})


def _update_options(data):
    arn = data.get("CertificateArn", "")
    cert = _certificates.get(arn)
    if not cert:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    cert["Options"] = data.get("Options", {})
    return json_response({})


def _renew_certificate(data):
    arn = data.get("CertificateArn", "")
    if arn not in _certificates:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    return json_response({})


def _resend_validation_email(data):
    arn = data.get("CertificateArn", "")
    if arn not in _certificates:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    return json_response({})


def reset():
    _certificates.clear()
