"""
AWS Account service stub (rest-json).

Endpoints (botocore service-2.json):
- POST /getAccountInformation   GetAccountInformation
- POST /getContactInformation   GetContactInformation
- POST /listRegions             ListRegions
- POST /getRegionOptStatus      GetRegionOptStatus

Includes the ``AccountState`` field added by AWS on 2026-04-29 (additive).
Older SDKs silently drop unknown response fields per JSON protocol; newer
SDKs see ``AccountState`` populated.
"""

import json
import logging
import time

from ministack.core.responses import (
    error_response_json,
    get_account_id,
)

logger = logging.getLogger("account")


def _json(status, body):
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


# Single fixed creation timestamp per process — AWS returns the original
# account creation time, which doesn't change. int epoch seconds per memory.
_CREATED_AT = int(time.time()) - (365 * 24 * 3600)


def _get_account_information(_payload):
    return _json(200, {
        "AccountId": get_account_id(),
        "AccountCreatedDate": _CREATED_AT,
        "AccountName": "MiniStack Local Account",
        # AWS additive field added 2026-04-29 — represents lifecycle phase.
        # ACTIVE is the only steady-state value for an emulated account.
        "AccountState": "ACTIVE",
    })


def _get_contact_information(_payload):
    return _json(200, {
        "ContactInformation": {
            "FullName": "MiniStack Local",
            "AddressLine1": "1 Localhost Lane",
            "City": "Local",
            "CountryCode": "US",
            "PhoneNumber": "+10000000000",
            "PostalCode": "00000",
        }
    })


_REGIONS_LIST = [
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "af-south-1", "ap-east-1", "ap-south-1", "ap-south-2",
    "ap-northeast-1", "ap-northeast-2", "ap-northeast-3",
    "ap-southeast-1", "ap-southeast-2", "ap-southeast-3",
    "ap-southeast-4", "ap-southeast-5",
    "ca-central-1", "ca-west-1",
    "eu-central-1", "eu-central-2", "eu-west-1", "eu-west-2",
    "eu-west-3", "eu-north-1", "eu-south-1", "eu-south-2",
    "il-central-1", "me-south-1", "me-central-1",
    "sa-east-1", "mx-central-1",
]
_OPT_IN_REQUIRED = {
    "af-south-1", "ap-east-1", "ap-south-2", "ap-southeast-3",
    "ap-southeast-4", "ap-southeast-5", "ca-west-1", "eu-central-2",
    "eu-south-1", "eu-south-2", "il-central-1", "me-south-1",
    "me-central-1", "mx-central-1",
}


def _list_regions(payload):
    filter_status = payload.get("RegionOptStatusContains") or []
    out = []
    for r in _REGIONS_LIST:
        status = "ENABLED_BY_DEFAULT" if r not in _OPT_IN_REQUIRED else "ENABLED"
        if filter_status and status not in filter_status:
            continue
        out.append({"RegionName": r, "RegionOptStatus": status})
    return _json(200, {"Regions": out})


def _get_region_opt_status(payload):
    region = payload.get("RegionName")
    if not region or region not in _REGIONS_LIST:
        return error_response_json("ResourceNotFoundException",
                                   f"Region not found: {region}", 400)
    status = "ENABLED_BY_DEFAULT" if region not in _OPT_IN_REQUIRED else "ENABLED"
    return _json(200, {"RegionName": region, "RegionOptStatus": status})


_DISPATCH = {
    "/getAccountInformation": _get_account_information,
    "/getContactInformation": _get_contact_information,
    "/listRegions": _list_regions,
    "/getRegionOptStatus": _get_region_opt_status,
}


async def handle_request(method, path, headers, body, query_params):
    if method != "POST":
        return error_response_json("InvalidRequest",
                                   f"Unsupported method {method}", 400)
    fn = _DISPATCH.get(path.rstrip("/"))
    if fn is None:
        return error_response_json("InvalidAction",
                                   f"Unsupported account path: {path}", 400)
    body_text = body.decode("utf-8") if isinstance(body, bytes) else (body or "")
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "invalid JSON body", 400)
    return fn(payload)


def reset():
    pass
