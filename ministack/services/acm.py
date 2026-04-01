"""
ACM (Certificate Manager) Service Emulator.
JSON-based API via X-Amz-Target.
Supports: RequestCertificate, DescribeCertificate, ListCertificates,
          DeleteCertificate, GetCertificate, ImportCertificate,
          AddTagsToCertificate, RemoveTagsFromCertificate, ListTagsForCertificate,
          UpdateCertificateOptions, RenewCertificate, ResendValidationEmail.
"""

import json
import os
import logging
import time

from ministack.core.responses import error_response_json, json_response, new_uuid, now_iso

logger = logging.getLogger("acm")

ACCOUNT_ID = "000000000000"
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_certificates: dict = {}  # arn -> certificate dict


def _future_iso(seconds):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + seconds))


def _cert_arn():
    return f"arn:aws:acm:{REGION}:{ACCOUNT_ID}:certificate/{new_uuid()}"


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
        "CreatedAt": cert["CreatedAt"],
        "IssuedAt": cert.get("IssuedAt", cert["CreatedAt"]),
        "NotBefore": cert.get("NotBefore", cert["CreatedAt"]),
        "NotAfter": cert.get("NotAfter", _future_iso(365 * 24 * 3600)),
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
    return json_response({"CertificateSummaryList": summaries, "NextToken": None})


def _delete_certificate(data):
    arn = data.get("CertificateArn", "")
    if arn not in _certificates:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    del _certificates[arn]
    return json_response({})


def _get_certificate(data):
    arn = data.get("CertificateArn", "")
    if arn not in _certificates:
        return error_response_json("ResourceNotFoundException", f"Certificate {arn} not found", 400)
    fake_pem = "-----BEGIN CERTIFICATE-----\nMIIFakeCertificateDataHere\n-----END CERTIFICATE-----"
    fake_chain = "-----BEGIN CERTIFICATE-----\nMIIFakeChainDataHere\n-----END CERTIFICATE-----"
    return json_response({"Certificate": fake_pem, "CertificateChain": fake_chain})


def _import_certificate(data):
    arn = data.get("CertificateArn") or _cert_arn()
    now = now_iso()
    _certificates[arn] = {
        "CertificateArn": arn,
        "DomainName": "imported.example.com",
        "SubjectAlternativeNames": ["imported.example.com"],
        "Status": "ISSUED",
        "Type": "IMPORTED",
        "CreatedAt": now,
        "IssuedAt": now,
        "NotBefore": now,
        "NotAfter": _future_iso(365 * 24 * 3600),
        "DomainValidationOptions": [],
        "Tags": data.get("Tags", []),
        "Options": {},
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
