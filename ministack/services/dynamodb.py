"""
DynamoDB Service Emulator.
Supports: CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable,
          PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan,
          BatchWriteItem, BatchGetItem, TransactWriteItems, TransactGetItems,
          DescribeTimeToLive, UpdateTimeToLive,
          DescribeContinuousBackups, UpdateContinuousBackups, DescribeEndpoints,
          TagResource, UntagResource, ListTagsOfResource,
          EnableKinesisStreamingDestination, DisableKinesisStreamingDestination,
          DescribeKinesisStreamingDestination, UpdateKinesisStreamingDestination,
          ExecuteStatement (PartiQL: SELECT, INSERT, UPDATE, DELETE).
Legacy conditional parameters: Expected (PutItem/UpdateItem/DeleteItem),
          KeyConditions (Query), ScanFilter/QueryFilter (Scan/Query).
Uses X-Amz-Target header for action routing (JSON API).
"""

import copy
import json
import logging
import os
import re
import threading
import time
from collections import defaultdict
from decimal import Decimal, InvalidOperation

from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_iso,
)
from ministack.services._dynamodb_keywords import AWS_KEYWORDS

logger = logging.getLogger("dynamodb")


def _conditional_check_failed(data, old_item, message="The conditional request failed"):
    """Standard ConditionalCheckFailedException response, with `Item` populated
    when the caller passed `ReturnValuesOnConditionCheckFailure="ALL_OLD"` and
    we have the prior item. AWS returns the existing item in the error body
    so callers don't have to re-fetch (see CancellationReason / Put / Update /
    Delete shapes in service-2.json)."""
    body = {"__type": "ConditionalCheckFailedException", "message": message}
    if data.get("ReturnValuesOnConditionCheckFailure") == "ALL_OLD" and old_item:
        body["Item"] = old_item
    return 400, {
        "Content-Type": "application/x-amz-json-1.0",
        "x-amzn-errortype": "ConditionalCheckFailedException",
    }, json.dumps(body, ensure_ascii=False).encode("utf-8")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

from ministack.core.persistence import PERSIST_STATE, load_state

_tables = AccountScopedDict()
_tags = AccountScopedDict()
_ttl_settings = AccountScopedDict()
_pitr_settings = AccountScopedDict()
# Kinesis streaming destinations — TableName -> list of
# {"StreamArn": str, "DestinationStatus": "ACTIVE"|"DISABLED",
#  "ApproximateCreationDateTimePrecision": "MILLISECOND"|"MICROSECOND"}.
# ACTIVE entries get each _emit_stream_event record fanned out via
# kinesis.put_record_internal; DISABLED entries stay on the describe
# response (matching the ~24 h AWS retention window for readability).
_kinesis_destinations = AccountScopedDict()
# Contributor Insights — key is "TableName" or "TableName/index/IndexName".
# Value: {"ContributorInsightsStatus": "ENABLED"|"DISABLED",
#         "LastUpdateDateTime": float epoch, "ContributorInsightsRuleList": [str, ...]}.
_backups = AccountScopedDict()  # BackupArn -> BackupDescription dict
_contributor_insights = AccountScopedDict()
# Resource-based policies — ResourceArn -> {"Policy": str, "RevisionId": str}.
_resource_policies = AccountScopedDict()
# Export tasks — ExportArn -> ExportDescription dict.
_exports = AccountScopedDict()
# Import tasks — ImportArn -> ImportTableDescription dict.
_imports = AccountScopedDict()
_lock = threading.Lock()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "tables": copy.deepcopy(_tables),
        "tags": copy.deepcopy(_tags),
        "ttl_settings": copy.deepcopy(_ttl_settings),
        "pitr_settings": copy.deepcopy(_pitr_settings),
        "kinesis_destinations": copy.deepcopy(_kinesis_destinations),
        "contributor_insights": copy.deepcopy(_contributor_insights),
        "backups": copy.deepcopy(_backups),
        "resource_policies": copy.deepcopy(_resource_policies),
        "exports": copy.deepcopy(_exports),
        "imports": copy.deepcopy(_imports),
    }


def restore_state(data):
    if data:
        _tables.update(data.get("tables", {}))
        # Restore items as defaultdict(dict) — JSON deserializes as plain dict
        for tbl in _tables.values():
            if isinstance(tbl.get("items"), dict) and not isinstance(tbl["items"], defaultdict):
                tbl["items"] = defaultdict(dict, tbl["items"])
            # Migrate legacy SSEDescription shape (pre-#411): convert
            # {Enabled, KMSMasterKeyId} → {Status, KMSMasterKeyArn, SSEType}.
            sse = tbl.get("SSEDescription")
            if sse and "Status" not in sse and ("Enabled" in sse or "KMSMasterKeyId" in sse):
                tbl["SSEDescription"] = _sse_description_from_spec(sse)
        _tags.update(data.get("tags", {}))
        _ttl_settings.update(data.get("ttl_settings", {}))
        _pitr_settings.update(data.get("pitr_settings", {}))
        _kinesis_destinations.update(data.get("kinesis_destinations", {}))
        _contributor_insights.update(data.get("contributor_insights", {}))
        _backups.update(data.get("backups", {}))
        _resource_policies.update(data.get("resource_policies", {}))
        _exports.update(data.get("exports", {}))
        _imports.update(data.get("imports", {}))


try:
    _restored = load_state("dynamodb")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


# ---------------------------------------------------------------------------
# Validation helpers — AWS-canonical limit + format enforcement.
#
# Sources:
#   * botocore service-2.json (2012-08-10) — shape ranges and required fields.
#   * AWS DynamoDB Developer Guide:
#     - "Service, account, and table quotas" (400 KB items, 38-digit numbers,
#        1E+126/-1E-130 magnitude bounds, 25 items per BatchWriteItem,
#        100 items per BatchGetItem, 100/4MB per TransactWriteItems).
#     - "Working with items" (empty sets rejected, no duplicate set elements,
#        attribute-name + value bytes count toward size).
#     - "Reserved words" (https://docs.aws.amazon.com/amazondynamodb/latest/
#        developerguide/ReservedWords.html).
# ---------------------------------------------------------------------------

# Numeric bounds per AWS docs.
_DDB_NUM_MAX_DIGITS = 38
_DDB_NUM_POS_MAX_EXP = 126   # |n| <= 9.9999...E+125 (positive 126 inclusive)
_DDB_NUM_NEG_MIN_EXP = -130  # smallest non-zero magnitude is 1E-130

# Item size caps.
_DDB_ITEM_MAX_BYTES = 400 * 1024
# TransactWriteItems / TransactGetItems caps.
_DDB_TXN_WRITE_MAX_ITEMS = 100
_DDB_TXN_GET_MAX_ITEMS = 100
_DDB_TXN_MAX_BYTES = 4 * 1024 * 1024
# Batch caps.
_DDB_BATCH_WRITE_MAX = 25
_DDB_BATCH_GET_MAX = 100


def _ddb_canonicalize_number(s: str) -> str | None:
    """Validate a DynamoDB Number string and return its canonical form, or None
    if invalid. Mirrors AWS behavior: numbers are stored as variable-precision
    decimals (up to 38 significant digits, magnitude between 1E-130 and
    9.9999E+125 inclusive). AWS canonicalizes: strips leading zeros, strips
    trailing zeros after the decimal point, normalizes negative zero to "0".
    Returns the canonical string the caller should persist, or None if the
    value is out of range / malformed.
    """
    if s is None:
        return None
    if not isinstance(s, str) or not s:
        return None
    raw = s.strip()
    # Reject empty / sign-only / non-numeric.
    try:
        d = Decimal(raw)
    except InvalidOperation:
        return None
    if d.is_nan() or d.is_infinite():
        return None
    # Significant-digit count: digits of the coefficient excluding leading zeros.
    sign, digits, exp = d.as_tuple()
    # Strip trailing zeros from significand to get true significant-digit count.
    sig = list(digits)
    while len(sig) > 1 and sig[-1] == 0:
        sig.pop()
        exp += 1
    while len(sig) > 1 and sig[0] == 0:
        sig.pop(0)
    if len(sig) > _DDB_NUM_MAX_DIGITS:
        return None
    if d == 0:
        return "0"
    # Magnitude check: AWS limits magnitude such that the decimal exponent of
    # the leading digit lies in [-130, 125]. Compute as adjusted exponent =
    # exp + (number_of_digits - 1). DynamoDB accepts 1E-130 through 9.9999E+125.
    adjusted = exp + len(sig) - 1
    if adjusted > _DDB_NUM_POS_MAX_EXP - 1:  # > 125
        return None
    if adjusted < _DDB_NUM_NEG_MIN_EXP:  # < -130
        return None
    # Canonical string: use Decimal's normalize but format without trailing zeros
    # and without unnecessary leading zeros.
    norm = d.normalize()
    # Decimal's normalize() returns scientific notation for very large/small;
    # we want the human-readable form when reasonable. Format manually.
    sign_str = "-" if sign else ""
    digits_str = "".join(str(x) for x in sig)
    if exp >= 0:
        text = digits_str + "0" * exp
        return sign_str + text
    # exp < 0
    point = len(digits_str) + exp
    if point <= 0:
        text = "0." + "0" * (-point) + digits_str
    else:
        text = digits_str[:point] + "." + digits_str[point:]
    # Strip trailing dot if any.
    if text.endswith("."):
        text = text[:-1]
    return sign_str + text


def _validate_attribute_value(attr_name: str, value: dict) -> tuple | None:
    """Recursively validate a single attribute value. Returns an error response
    tuple if invalid, or None if OK. May mutate `value` to canonicalize numbers.
    """
    if not isinstance(value, dict) or not value:
        return error_response_json("ValidationException",
            "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes", 400)
    if len(value) != 1:
        return error_response_json("ValidationException",
            "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes", 400)
    (vtype, vval), = value.items()
    if vtype == "S":
        if not isinstance(vval, str):
            return error_response_json("ValidationException",
                f"Supplied AttributeValue is empty, must contain exactly one of the supported datatypes", 400)
    elif vtype == "N":
        canon = _ddb_canonicalize_number(vval)
        if canon is None:
            return error_response_json("ValidationException",
                f"The parameter cannot be converted to a numeric value: {vval}", 400)
        value["N"] = canon
    elif vtype == "B":
        if not isinstance(vval, (str, bytes)) or not vval:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: Binary attribute value is empty", 400)
    elif vtype == "BOOL":
        if not isinstance(vval, bool):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: BOOL value must be true or false", 400)
    elif vtype == "NULL":
        if vval is not True:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: Null attribute value must be true", 400)
    elif vtype == "SS":
        if not isinstance(vval, list) or not vval:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: An string set  may not be empty", 400)
        if len(set(vval)) != len(vval):
            return error_response_json("ValidationException",
                "Input collection contains duplicates", 400)
        for s in vval:
            if not isinstance(s, str) or s == "":
                return error_response_json("ValidationException",
                    "One or more parameter values were invalid: An string set may not be empty", 400)
    elif vtype == "NS":
        if not isinstance(vval, list) or not vval:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: An number set  may not be empty", 400)
        seen = set()
        new_vals = []
        for n in vval:
            canon = _ddb_canonicalize_number(n if isinstance(n, str) else str(n))
            if canon is None:
                return error_response_json("ValidationException",
                    f"The parameter cannot be converted to a numeric value: {n}", 400)
            if canon in seen:
                return error_response_json("ValidationException",
                    "Input collection contains duplicates", 400)
            seen.add(canon)
            new_vals.append(canon)
        value["NS"] = new_vals
    elif vtype == "BS":
        if not isinstance(vval, list) or not vval:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: An binary set  may not be empty", 400)
        seen = set()
        for b in vval:
            key = b if isinstance(b, str) else (b.decode("latin-1") if isinstance(b, bytes) else None)
            if key is None or key == "":
                return error_response_json("ValidationException",
                    "One or more parameter values were invalid: Binary set element may not be empty", 400)
            if key in seen:
                return error_response_json("ValidationException",
                    "Input collection contains duplicates", 400)
            seen.add(key)
    elif vtype == "L":
        if not isinstance(vval, list):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: List must be a list", 400)
        for sub in vval:
            err = _validate_attribute_value(attr_name, sub)
            if err:
                return err
    elif vtype == "M":
        if not isinstance(vval, dict):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: Map must be a map", 400)
        for k, sub in vval.items():
            err = _validate_attribute_value(attr_name, sub)
            if err:
                return err
    else:
        return error_response_json("ValidationException",
            f"Supplied AttributeValue is empty, must contain exactly one of the supported datatypes", 400)
    return None


def _attribute_value_size(value: dict) -> int:
    """Estimated byte size of an AttributeValue per AWS accounting.

    AWS docs: "The size of an item is the sum of the lengths of its attribute
    names and values." Per-type rules approximated from public guidance:
      - String/Binary: byte length of the value.
      - Number: 1 byte + ceil(digits / 2) bytes (max 21 bytes per number).
      - Boolean/Null: 1 byte.
      - List/Map: 3 bytes overhead + 1 byte per element + sum of child sizes.
      - SS/NS/BS: sum of element sizes.
    """
    if not isinstance(value, dict) or len(value) != 1:
        return 0
    (vtype, vval), = value.items()
    if vtype == "S":
        return len(vval.encode("utf-8")) if isinstance(vval, str) else 0
    if vtype == "N":
        # Strip sign, decimal, leading zeros for digit count.
        s = (vval or "").lstrip("-").replace(".", "").lstrip("0") or "0"
        return 1 + (len(s) + 1) // 2
    if vtype == "B":
        if isinstance(vval, bytes):
            return len(vval)
        try:
            import base64
            return len(base64.b64decode(vval))
        except Exception:
            return len(vval) if isinstance(vval, str) else 0
    if vtype in ("BOOL", "NULL"):
        return 1
    if vtype == "SS":
        return sum(len(s.encode("utf-8")) for s in vval if isinstance(s, str))
    if vtype == "NS":
        total = 0
        for n in vval:
            s = (n or "").lstrip("-").replace(".", "").lstrip("0") or "0"
            total += 1 + (len(s) + 1) // 2
        return total
    if vtype == "BS":
        total = 0
        for b in vval:
            if isinstance(b, bytes):
                total += len(b)
            else:
                try:
                    import base64
                    total += len(base64.b64decode(b))
                except Exception:
                    total += len(b) if isinstance(b, str) else 0
        return total
    if vtype == "L":
        return 3 + sum(1 + _attribute_value_size(sub) for sub in vval if isinstance(sub, dict))
    if vtype == "M":
        total = 3
        for k, sub in vval.items():
            total += 1 + len(k.encode("utf-8")) + _attribute_value_size(sub)
        return total
    return 0


def _item_size_bytes(item: dict) -> int:
    total = 0
    if not isinstance(item, dict):
        return 0
    for name, value in item.items():
        total += len(name.encode("utf-8"))
        total += _attribute_value_size(value)
    return total


def _validate_item(item: dict, pk_name: str | None = None, sk_name: str | None = None) -> tuple | None:
    """Validate a full item: each attribute, then total size cap. Mutates
    Number values to canonical form."""
    if not isinstance(item, dict):
        return error_response_json("ValidationException",
            "Item must be a structure", 400)
    for name, value in item.items():
        # Empty string/binary not allowed for hash/sort key attributes
        if name in (pk_name, sk_name):
            if isinstance(value, dict):
                (vtype, vval), = value.items()
                if vtype == "S" and vval == "":
                    return error_response_json("ValidationException",
                        f"One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: {name}", 400)
                if vtype == "B" and (vval == "" or vval == b""):
                    return error_response_json("ValidationException",
                        f"One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: {name}", 400)
        err = _validate_attribute_value(name, value)
        if err:
            return err
    size = _item_size_bytes(item)
    if size > _DDB_ITEM_MAX_BYTES:
        return error_response_json("ValidationException",
            f"Item size has exceeded the maximum allowed size", 400)
    return None

# DynamoDB Streams: table_name -> list of stream records
# Each record follows the DynamoDB Streams event format consumed by Lambda ESMs.
_stream_records = AccountScopedDict()
_stream_seq_counter = 0
_stream_seq_lock = threading.Lock()


def _next_stream_seq():
    global _stream_seq_counter
    with _stream_seq_lock:
        _stream_seq_counter += 1
        return f"{int(time.time() * 1000):020d}{_stream_seq_counter:010d}"


def _build_change_record(table: dict, event_name: str, old_item: dict | None, new_item: dict | None, view_type: str) -> dict:
    """Build a DynamoDB Streams-shaped change record. ``view_type`` controls
    whether OldImage / NewImage are populated."""
    record: dict = {
        "eventID": new_uuid(),
        "eventName": event_name,
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodb",
        "awsRegion": get_region(),
        "dynamodb": {
            "ApproximateCreationDateTime": int(time.time()),
            "Keys": {},
            "SequenceNumber": _next_stream_seq(),
            "SizeBytes": 0,
            "StreamViewType": view_type,
        },
        "eventSourceARN": f"{table['TableArn']}/stream/{now_iso()}",
    }

    ref_item = new_item or old_item or {}
    pk_name = table["pk_name"]
    sk_name = table["sk_name"]
    if pk_name and pk_name in ref_item:
        record["dynamodb"]["Keys"][pk_name] = ref_item[pk_name]
    if sk_name and sk_name in ref_item:
        record["dynamodb"]["Keys"][sk_name] = ref_item[sk_name]

    if view_type in ("NEW_AND_OLD_IMAGES", "OLD_IMAGE") and old_item:
        record["dynamodb"]["OldImage"] = old_item
    if view_type in ("NEW_AND_OLD_IMAGES", "NEW_IMAGE") and new_item:
        record["dynamodb"]["NewImage"] = new_item

    return record


def _emit_stream_event(table_name: str, event_name: str, old_item: dict | None, new_item: dict | None):
    """Emit a change to DynamoDB Streams (if enabled) and to any ACTIVE Kinesis
    streaming destinations registered for this table.

    AWS treats DynamoDB Streams and Kinesis streaming destination as
    independent subscriptions — a table can have either, both, or neither.
    Each path is gated independently here; the function name is kept for
    backwards compatibility with existing call sites."""
    table = _tables.get(table_name)
    if not table:
        return

    spec = table.get("StreamSpecification") or {}
    streams_enabled = bool(spec.get("StreamEnabled"))
    has_kinesis = bool(_kinesis_destinations.get(table_name))
    if not streams_enabled and not has_kinesis:
        return

    if streams_enabled:
        view_type = spec.get("StreamViewType", "NEW_AND_OLD_IMAGES")
        record = _build_change_record(table, event_name, old_item, new_item, view_type)
        if table_name not in _stream_records:
            _stream_records[table_name] = []
        _stream_records[table_name].append(record)

    if has_kinesis:
        # AWS's Kinesis streaming destination always carries the equivalent of
        # NEW_AND_OLD_IMAGES — the StreamViewType setting belongs to Streams,
        # not to the Kinesis fan-out path. Build a fresh record so its
        # SequenceNumber and eventID are independent of the Streams record.
        kinesis_record = _build_change_record(table, event_name, old_item, new_item, "NEW_AND_OLD_IMAGES")
        _fan_out_to_kinesis(table_name, kinesis_record)


def _fan_out_to_kinesis(table_name: str, record: dict) -> None:
    """Deliver a Streams record to every ACTIVE Kinesis streaming destination
    registered for this table. Failures are logged and swallowed so DynamoDB
    writes stay green even if the downstream stream disappeared."""
    dests = _kinesis_destinations.get(table_name, [])
    if not dests:
        return
    try:
        from ministack.services.kinesis import put_record_internal
    except Exception as exc:  # pragma: no cover - kinesis module missing
        logger.warning("Kinesis streaming destination: import failed: %s", exc)
        return
    payload = json.dumps(record, default=str).encode("utf-8")
    pk = record.get("eventID", new_uuid())
    for dest in dests:
        if dest.get("DestinationStatus") != "ACTIVE":
            continue
        try:
            put_record_internal(dest["StreamArn"], pk, payload)
        except Exception as exc:
            logger.warning(
                "Kinesis streaming destination delivery failed for %s -> %s: %s",
                table_name, dest.get("StreamArn"), exc,
            )

# ---------------------------------------------------------------------------
# TTL background reaper
# ---------------------------------------------------------------------------

def _ttl_reaper():
    """Periodically delete items whose TTL attribute has expired."""
    while True:
        time.sleep(60)
        now = time.time()
        try:
            with _lock:
                for table_name, setting in list(_ttl_settings.items()):
                    if setting.get("TimeToLiveStatus") != "ENABLED":
                        continue
                    attr = setting.get("AttributeName", "")
                    if not attr:
                        continue
                    table = _tables.get(table_name)
                    if not table:
                        continue
                    for pk_val, sk_map in list(table["items"].items()):
                        for sk_val, item in list(sk_map.items()):
                            ttl_attr = item.get(attr)
                            if ttl_attr is None:
                                continue
                            ttl_val = _extract_key_val(ttl_attr)
                            try:
                                if float(ttl_val) <= now:
                                    del sk_map[sk_val]
                                    logger.debug("TTL expired item %s/%s from %s", pk_val, sk_val, table_name)
                            except (ValueError, TypeError):
                                pass
                        if not sk_map:
                            del table["items"][pk_val]
                    _update_counts(table)
        except Exception as exc:
            logger.error("TTL reaper error: %s", exc)


threading.Thread(target=_ttl_reaper, daemon=True, name="dynamodb-ttl-reaper").start()


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateTable": _create_table,
        "DeleteTable": _delete_table,
        "DescribeTable": _describe_table,
        "ListTables": _list_tables,
        "UpdateTable": _update_table,
        "PutItem": _put_item,
        "GetItem": _get_item,
        "DeleteItem": _delete_item,
        "UpdateItem": _update_item,
        "Query": _query,
        "Scan": _scan,
        "BatchWriteItem": _batch_write_item,
        "BatchGetItem": _batch_get_item,
        "TransactWriteItems": _transact_write_items,
        "TransactGetItems": _transact_get_items,
        "DescribeTimeToLive": _describe_ttl,
        "UpdateTimeToLive": _update_ttl,
        "DescribeContinuousBackups": _describe_continuous_backups,
        "UpdateContinuousBackups": _update_continuous_backups,
        "DescribeEndpoints": _describe_endpoints,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsOfResource": _list_tags,
        "EnableKinesisStreamingDestination": _enable_kinesis_streaming_destination,
        "DisableKinesisStreamingDestination": _disable_kinesis_streaming_destination,
        "DescribeKinesisStreamingDestination": _describe_kinesis_streaming_destination,
        "UpdateKinesisStreamingDestination": _update_kinesis_streaming_destination,
        "ExecuteStatement": _execute_statement,
        "BatchExecuteStatement": _batch_execute_statement,
        "ExecuteTransaction": _execute_transaction,
        "UpdateContributorInsights": _update_contributor_insights,
        "DescribeContributorInsights": _describe_contributor_insights,
        "ListContributorInsights": _list_contributor_insights,
        "PutResourcePolicy": _put_resource_policy,
        "GetResourcePolicy": _get_resource_policy,
        "DeleteResourcePolicy": _delete_resource_policy,
        "ExportTableToPointInTime": _export_table_to_point_in_time,
        "DescribeExport": _describe_export,
        "ListExports": _list_exports,
        "ImportTable": _import_table,
        "DescribeImport": _describe_import,
        "ListImports": _list_imports,
        "CreateBackup": _create_backup,
        "DescribeBackup": _describe_backup,
        "DeleteBackup": _delete_backup,
        "ListBackups": _list_backups,
        "RestoreTableFromBackup": _restore_table_from_backup,
        "RestoreTableToPointInTime": _restore_table_to_point_in_time,
        "DescribeLimits": _describe_limits,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("UnknownOperationException", f"Unknown operation: {action}", 400)
    status, resp_headers, resp_body = handler(data)
    # Add CRC32 checksum — Go SDK v2 DynamoDB client validates this on Close()
    import zlib
    body_bytes = resp_body if isinstance(resp_body, bytes) else resp_body.encode("utf-8")
    resp_headers["x-amz-crc32"] = str(zlib.crc32(body_bytes) & 0xFFFFFFFF)
    return status, resp_headers, resp_body


# ---------------------------------------------------------------------------
# Table operations
# ---------------------------------------------------------------------------

def _sse_description_from_spec(spec: dict | None) -> dict | None:
    """Convert the request's ``SSESpecification`` into the response-shape
    ``SSEDescription`` AWS actually returns on DescribeTable.

    Request shape (caller):
        {"Enabled": true, "SSEType": "KMS", "KMSMasterKeyId": "<arn|alias|id>"}

    Response shape (SSEDescription per AWS docs):
        {"Status": "ENABLED" | "DISABLED",
         "SSEType": "AES256" | "KMS",
         "KMSMasterKeyArn": "<key-arn>",   # only when SSEType == KMS
         "InaccessibleEncryptionDateTime": <optional>}

    Terraform waiters read ``Status`` and ``KMSMasterKeyArn``; the legacy
    ``Enabled`` / ``KMSMasterKeyId`` names are request-only and Terraform
    v6 will hang forever waiting for a status that never appears (#411).
    """
    if not spec:
        return None
    enabled = bool(spec.get("Enabled", False))
    # AWS default: when SSEType is omitted and Enabled is true, SSE-KMS with
    # the AWS-managed key (alias/aws/dynamodb) is configured. SSEType "AES256"
    # is not a valid CreateTable input — only "KMS" or omission.
    sse_type = spec.get("SSEType") or "KMS"
    desc = {
        "Status": "ENABLED" if enabled else "DISABLED",
        "SSEType": sse_type,
    }
    if sse_type == "KMS":
        kms_key = spec.get("KMSMasterKeyId") or spec.get("KMSMasterKeyArn")
        if not kms_key:
            # AWS-managed key — fabricate a deterministic alias ARN.
            kms_key = f"arn:aws:kms:{get_region()}:{get_account_id()}:alias/aws/dynamodb"
        desc["KMSMasterKeyArn"] = kms_key
    return desc


_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_INDEX_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_VALID_ATTR_TYPES = {"S", "N", "B"}
_VALID_KEY_TYPES = {"HASH", "RANGE"}
_VALID_BILLING_MODES = {"PROVISIONED", "PAY_PER_REQUEST"}
_VALID_TABLE_CLASSES = {"STANDARD", "STANDARD_INFREQUENT_ACCESS"}
_VALID_PROJECTION_TYPES = {"ALL", "KEYS_ONLY", "INCLUDE"}


def _validate_table_name(name: str) -> tuple | None:
    """Per AWS DynamoDB Developer Guide: 3-255 chars, pattern [A-Za-z0-9_.-]."""
    if not isinstance(name, str) or not name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    if len(name) < 3 or len(name) > 255:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{name}' at 'tableName' failed to satisfy constraint: Member must have length between 3 and 255", 400)
    if not _TABLE_NAME_RE.match(name):
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{name}' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", 400)
    return None


def _validate_key_schema(key_schema: list, attr_defs: list, context: str = "tableName") -> tuple | None:
    """Validates KeySchema + AttributeDefinitions per AWS rules:
       - 1 HASH required; 0 or 1 RANGE allowed.
       - Every key attribute must appear in AttributeDefinitions.
       - No duplicate attribute names in KeySchema.
    """
    if not key_schema:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'keySchema' failed to satisfy constraint: Member must not be null", 400)
    if not isinstance(key_schema, list) or len(key_schema) == 0 or len(key_schema) > 2:
        return error_response_json("ValidationException",
            "1 validation error detected: KeySchema must contain 1 or 2 elements", 400)
    hash_count = 0
    range_count = 0
    seen_names = set()
    for ks in key_schema:
        attr = ks.get("AttributeName")
        kt = ks.get("KeyType")
        if not attr or kt not in _VALID_KEY_TYPES:
            return error_response_json("ValidationException",
                f"1 validation error detected: KeySchema element invalid: {ks}", 400)
        if attr in seen_names:
            return error_response_json("ValidationException",
                f"Two keys cannot have the same name: {attr}", 400)
        seen_names.add(attr)
        if kt == "HASH":
            hash_count += 1
        elif kt == "RANGE":
            range_count += 1
    if hash_count != 1:
        return error_response_json("ValidationException",
            "1 validation error detected: KeySchema must contain exactly one HASH key", 400)
    # Every key attribute must be defined in AttributeDefinitions.
    defined = {a.get("AttributeName"): a.get("AttributeType") for a in (attr_defs or [])}
    for ks in key_schema:
        attr = ks["AttributeName"]
        if attr not in defined:
            return error_response_json("ValidationException",
                f"Hash Key not specified in Attribute Definitions. Type unknown: {attr}", 400)
        if defined[attr] not in _VALID_ATTR_TYPES:
            return error_response_json("ValidationException",
                f"Invalid AttributeType for attribute {attr}: {defined[attr]}", 400)
    return None


def _create_table(data):
    name = data.get("TableName")
    err = _validate_table_name(name)
    if err:
        return err
    if name in _tables:
        return error_response_json("ResourceInUseException", f"Table already exists: {name}", 400)

    key_schema = data.get("KeySchema")
    attr_defs = data.get("AttributeDefinitions") or []
    # Validate AttributeDefinitions structure.
    seen_attr_names = set()
    for ad in attr_defs:
        an = ad.get("AttributeName")
        at = ad.get("AttributeType")
        if not an:
            return error_response_json("ValidationException",
                "1 validation error detected: AttributeDefinitions element missing AttributeName", 400)
        if at not in _VALID_ATTR_TYPES:
            return error_response_json("ValidationException",
                f"1 validation error detected: Value '{at}' at 'attributeDefinitions..attributeType' failed to satisfy constraint: Member must satisfy enum value set: [B, N, S]", 400)
        if an in seen_attr_names:
            return error_response_json("ValidationException",
                f"Duplicate AttributeName in AttributeDefinitions: {an}", 400)
        seen_attr_names.add(an)
    err = _validate_key_schema(key_schema, attr_defs)
    if err:
        return err
    pk_name = sk_name = None
    for ks in key_schema:
        if ks["KeyType"] == "HASH":
            pk_name = ks["AttributeName"]
        elif ks["KeyType"] == "RANGE":
            sk_name = ks["AttributeName"]

    # BillingMode validation.
    billing_mode = data.get("BillingMode", "PROVISIONED")
    if billing_mode not in _VALID_BILLING_MODES:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{billing_mode}' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: {sorted(_VALID_BILLING_MODES)}", 400)
    # TableClass validation.
    table_class = data.get("TableClass")
    if table_class is not None and table_class not in _VALID_TABLE_CLASSES:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{table_class}' at 'tableClass' failed to satisfy constraint: Member must satisfy enum value set: {sorted(_VALID_TABLE_CLASSES)}", 400)
    # ProvisionedThroughput required when PROVISIONED.
    pt = data.get("ProvisionedThroughput")
    if billing_mode == "PROVISIONED":
        if not pt or not isinstance(pt, dict):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: ProvisionedThroughput must be specified when BillingMode is PROVISIONED", 400)
        if int(pt.get("ReadCapacityUnits", 0)) <= 0 or int(pt.get("WriteCapacityUnits", 0)) <= 0:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must be greater than zero", 400)
    elif pt is not None:
        return error_response_json("ValidationException",
            "One or more parameter values were invalid: ProvisionedThroughput should not be specified when BillingMode is PAY_PER_REQUEST", 400)

    gsis = copy.deepcopy(data.get("GlobalSecondaryIndexes", []))
    lsis = copy.deepcopy(data.get("LocalSecondaryIndexes", []))

    # LSI validation: requires the base table to have a RANGE key, and each LSI
    # must have the same HASH key as the base table.
    if lsis and not sk_name:
        return error_response_json("ValidationException",
            "Local Secondary indices are not allowed on hash tables, only hash and range tables", 400)
    for lsi in lsis:
        lks = lsi.get("KeySchema") or []
        if not any(k.get("KeyType") == "HASH" and k.get("AttributeName") == pk_name for k in lks):
            return error_response_json("ValidationException",
                f"Local Secondary Index '{lsi.get('IndexName')}' must use the table's hash key", 400)

    # Duplicate-index-name detection across LSI + GSI.
    seen_index_names = set()
    for idx in lsis + gsis:
        iname = idx.get("IndexName")
        if iname and iname in seen_index_names:
            return error_response_json("ValidationException",
                f"Two indexes cannot have the same name: {iname}", 400)
        if iname:
            seen_index_names.add(iname)
    # Every attribute referenced in any index KeySchema must be in AttributeDefinitions.
    referenced_attrs = set()
    for ks in key_schema:
        referenced_attrs.add(ks["AttributeName"])
    for idx_group in (gsis, lsis):
        for idx in idx_group:
            for k in idx.get("KeySchema") or []:
                referenced_attrs.add(k.get("AttributeName"))
    for ad_name in referenced_attrs:
        if ad_name not in seen_attr_names:
            return error_response_json("ValidationException",
                f"Some AttributeDefinitions are not present in KeySchema: {ad_name}", 400)
    # AttributeDefinitions must not include unused attrs (real AWS rejects).
    unused = seen_attr_names - referenced_attrs
    if unused:
        return error_response_json("ValidationException",
            f"One or more parameter values were invalid: Some AttributeDefinitions are not used: {sorted(unused)}", 400)

    gsi_default_throughput = (
        {"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
        if billing_mode == "PAY_PER_REQUEST"
        else {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    )
    for gsi in gsis:
        gsi.setdefault("IndexStatus", "ACTIVE")
        gsi.setdefault("ProvisionedThroughput", gsi_default_throughput)
        gsi["IndexArn"] = f"arn:aws:dynamodb:{get_region()}:{get_account_id()}:table/{name}/index/{gsi['IndexName']}"
        gsi["IndexSizeBytes"] = 0
        gsi["ItemCount"] = 0
    for lsi in lsis:
        lsi["IndexArn"] = f"arn:aws:dynamodb:{get_region()}:{get_account_id()}:table/{name}/index/{lsi['IndexName']}"
        lsi["IndexSizeBytes"] = 0
        lsi["ItemCount"] = 0

    _tables[name] = {
        "TableName": name,
        "KeySchema": key_schema,
        "AttributeDefinitions": attr_defs,
        "pk_name": pk_name,
        "sk_name": sk_name,
        "items": defaultdict(dict),
        "TableStatus": "ACTIVE",
        "CreationDateTime": int(time.time()),
        "ItemCount": 0,
        "TableSizeBytes": 0,
        "TableArn": f"arn:aws:dynamodb:{get_region()}:{get_account_id()}:table/{name}",
        "TableId": new_uuid(),
        "GlobalSecondaryIndexes": gsis,
        "LocalSecondaryIndexes": lsis,
        "ProvisionedThroughput": {"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
            if billing_mode == "PAY_PER_REQUEST"
            else data.get("ProvisionedThroughput", {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}),
        "BillingModeSummary": {"BillingMode": billing_mode},
        "StreamSpecification": data.get("StreamSpecification"),
        "SSEDescription": _sse_description_from_spec(data.get("SSESpecification")),
        "DeletionProtectionEnabled": data.get("DeletionProtectionEnabled", False),
    }
    # TableClass round-trip — DescribeTable echoes the configured class via
    # TableClassSummary (real AWS shape).
    if table_class:
        _tables[name]["TableClassSummary"] = {
            "TableClass": table_class,
            "LastUpdateDateTime": int(time.time()),
        }
    # OnDemandThroughput round-trip — only meaningful for PAY_PER_REQUEST.
    on_demand = data.get("OnDemandThroughput")
    if on_demand is not None:
        _tables[name]["OnDemandThroughput"] = {
            "MaxReadRequestUnits": int(on_demand.get("MaxReadRequestUnits", -1)),
            "MaxWriteRequestUnits": int(on_demand.get("MaxWriteRequestUnits", -1)),
        }
    if data.get("StreamSpecification"):
        stream_label = now_iso()
        _tables[name]["LatestStreamLabel"] = stream_label
        _tables[name]["LatestStreamArn"] = f"{_tables[name]['TableArn']}/stream/{stream_label}"
    if data.get("Tags"):
        _tags[_tables[name]["TableArn"]] = data["Tags"]
    logger.info("DynamoDB table created: %s", name)
    return json_response({"TableDescription": _table_description(name)})


def _delete_table(data):
    name = data.get("TableName")
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)
    if _tables[name].get("DeletionProtectionEnabled"):
        return error_response_json("ValidationException",
            "Table can't be deleted as deletion protection is enabled", 400)
    desc = _table_description(name)
    desc["TableStatus"] = "DELETING"
    del _tables[name]
    _tags.pop(desc.get("TableArn", ""), None)
    _ttl_settings.pop(name, None)
    _pitr_settings.pop(name, None)
    _kinesis_destinations.pop(name, None)
    return json_response({"TableDescription": desc})


def _describe_table(data):
    name = data.get("TableName")
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)
    return json_response({"Table": _table_description(name)})


def _list_tables(data):
    limit = data.get("Limit", 100)
    start = data.get("ExclusiveStartTableName", "")
    names = sorted(_tables.keys())
    if start:
        names = [n for n in names if n > start]
    names = names[:limit]
    result = {"TableNames": names}
    if len(names) == limit and names:
        result["LastEvaluatedTableName"] = names[-1]
    return json_response(result)


def _update_table(data):
    name = data.get("TableName")
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)
    table = _tables[name]

    current_billing = table.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED")
    new_billing = data.get("BillingMode")
    # ProvisionedThroughput validation when supplied.
    pt = data.get("ProvisionedThroughput")
    if pt is not None:
        # PAY_PER_REQUEST + ProvisionedThroughput is invalid.
        if new_billing == "PAY_PER_REQUEST" or (new_billing is None and current_billing == "PAY_PER_REQUEST"):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: ProvisionedThroughput should not be specified when BillingMode is PAY_PER_REQUEST", 400)
        rcu = int(pt.get("ReadCapacityUnits", 0))
        wcu = int(pt.get("WriteCapacityUnits", 0))
        if rcu <= 0 or wcu <= 0:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must be greater than zero", 400)
        existing_pt = table.get("ProvisionedThroughput") or {}
        # AWS rejects an UpdateTable that would result in no change.
        if (
            new_billing is None
            and current_billing == "PROVISIONED"
            and rcu == int(existing_pt.get("ReadCapacityUnits", 0))
            and wcu == int(existing_pt.get("WriteCapacityUnits", 0))
        ):
            return error_response_json("ValidationException",
                "The provisioned throughput for the table will not change. The requested value equals the current value.", 400)
        table["ProvisionedThroughput"] = pt
    if new_billing is not None:
        if new_billing not in _VALID_BILLING_MODES:
            return error_response_json("ValidationException",
                f"1 validation error detected: Value '{new_billing}' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: {sorted(_VALID_BILLING_MODES)}", 400)
        table["BillingModeSummary"] = {"BillingMode": new_billing, "LastUpdateToPayPerRequestDateTime": int(time.time())}
        if new_billing == "PAY_PER_REQUEST":
            table["ProvisionedThroughput"] = {"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
    if "AttributeDefinitions" in data:
        table["AttributeDefinitions"] = data["AttributeDefinitions"]
    if "StreamSpecification" in data:
        table["StreamSpecification"] = data["StreamSpecification"]
    if "SSESpecification" in data:
        # Terraform v6 calls UpdateTable(SSESpecification=...) on warm boots
        # when it sees the legacy shape in state (#411). Convert to the
        # response-shape SSEDescription with a proper Status field so the
        # Terraform waiter can observe ENABLED/DISABLED and return.
        table["SSEDescription"] = _sse_description_from_spec(data["SSESpecification"])
    if "DeletionProtectionEnabled" in data:
        table["DeletionProtectionEnabled"] = data["DeletionProtectionEnabled"]
    # TableClass change round-trip.
    if "TableClass" in data:
        tc = data["TableClass"]
        if tc not in _VALID_TABLE_CLASSES:
            return error_response_json("ValidationException",
                f"1 validation error detected: Value '{tc}' at 'tableClass' failed to satisfy constraint: Member must satisfy enum value set: {sorted(_VALID_TABLE_CLASSES)}", 400)
        table["TableClassSummary"] = {"TableClass": tc, "LastUpdateDateTime": int(time.time())}
    # OnDemandThroughput change round-trip.
    if "OnDemandThroughput" in data:
        odt = data["OnDemandThroughput"] or {}
        table["OnDemandThroughput"] = {
            "MaxReadRequestUnits": int(odt.get("MaxReadRequestUnits", -1)),
            "MaxWriteRequestUnits": int(odt.get("MaxWriteRequestUnits", -1)),
        }

    existing_idx_names = {g["IndexName"] for g in table.get("GlobalSecondaryIndexes", [])}
    defined_attrs = {a["AttributeName"] for a in table.get("AttributeDefinitions", [])}
    for update in data.get("GlobalSecondaryIndexUpdates", []):
        if "Create" in update:
            gsi_def = copy.deepcopy(update["Create"])
            idx_name = gsi_def.get("IndexName")
            if idx_name in existing_idx_names:
                return error_response_json("ValidationException",
                    f"Attempting to create a duplicate index: {idx_name}", 400)
            # Every key attribute of the new GSI must be in AttributeDefinitions.
            for k in gsi_def.get("KeySchema") or []:
                if k.get("AttributeName") not in defined_attrs:
                    return error_response_json("ValidationException",
                        f"One or more parameter values were invalid: AttributeDefinitions does not contain {k.get('AttributeName')} referenced by GSI {idx_name}", 400)
            gsi_def.setdefault("IndexStatus", "ACTIVE")
            gsi_billing = table.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED")
            gsi_def.setdefault(
                "ProvisionedThroughput",
                {"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
                if gsi_billing == "PAY_PER_REQUEST"
                else {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
            gsi_def["IndexArn"] = f"arn:aws:dynamodb:{get_region()}:{get_account_id()}:table/{name}/index/{gsi_def['IndexName']}"
            gsi_def["IndexSizeBytes"] = 0
            gsi_def["ItemCount"] = 0
            table["GlobalSecondaryIndexes"].append(gsi_def)
            existing_idx_names.add(idx_name)
        elif "Delete" in update:
            idx_name = update["Delete"]["IndexName"]
            if idx_name not in existing_idx_names:
                return error_response_json("ResourceNotFoundException",
                    f"Attempt to remove a non-existent index: {idx_name}", 400)
            table["GlobalSecondaryIndexes"] = [g for g in table["GlobalSecondaryIndexes"] if g["IndexName"] != idx_name]
            existing_idx_names.discard(idx_name)
        elif "Update" in update:
            idx_name = update["Update"]["IndexName"]
            if idx_name not in existing_idx_names:
                return error_response_json("ResourceNotFoundException",
                    f"Attempt to update a non-existent index: {idx_name}", 400)
            for gsi in table["GlobalSecondaryIndexes"]:
                if gsi["IndexName"] == idx_name:
                    if "ProvisionedThroughput" in update["Update"]:
                        gsi["ProvisionedThroughput"] = update["Update"]["ProvisionedThroughput"]

    return json_response({"TableDescription": _table_description(name)})


def _table_description(name):
    t = _tables[name]
    desc = {
        "TableName": t["TableName"],
        "KeySchema": t["KeySchema"],
        "AttributeDefinitions": t["AttributeDefinitions"],
        "TableStatus": t["TableStatus"],
        "CreationDateTime": t["CreationDateTime"],
        "ItemCount": t["ItemCount"],
        "TableSizeBytes": t["TableSizeBytes"],
        "TableArn": t["TableArn"],
        "TableId": t.get("TableId", new_uuid()),
        "ProvisionedThroughput": t["ProvisionedThroughput"],
    }
    if t.get("BillingModeSummary"):
        desc["BillingModeSummary"] = t["BillingModeSummary"]
    if t.get("GlobalSecondaryIndexes"):
        desc["GlobalSecondaryIndexes"] = t["GlobalSecondaryIndexes"]
    if t.get("LocalSecondaryIndexes"):
        desc["LocalSecondaryIndexes"] = t["LocalSecondaryIndexes"]
    if t.get("StreamSpecification"):
        desc["StreamSpecification"] = t["StreamSpecification"]
        desc["LatestStreamLabel"] = t.get("LatestStreamLabel", "")
        desc["LatestStreamArn"] = t.get("LatestStreamArn", "")
    if t.get("SSEDescription"):
        desc["SSEDescription"] = t["SSEDescription"]
    if t.get("TableClassSummary"):
        desc["TableClassSummary"] = t["TableClassSummary"]
    if t.get("OnDemandThroughput"):
        desc["OnDemandThroughput"] = t["OnDemandThroughput"]
    desc["DeletionProtectionEnabled"] = t.get("DeletionProtectionEnabled", False)
    desc["WarmThroughput"] = t.get("WarmThroughput", {
        "ReadUnitsPerSecond": 0,
        "WriteUnitsPerSecond": 0,
        "Status": "ACTIVE",
    })
    return desc


# ---------------------------------------------------------------------------
# Item operations
# ---------------------------------------------------------------------------

_PUT_DELETE_RV_VALUES = {"NONE", "ALL_OLD"}
_UPDATE_RV_VALUES = {"NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"}
_RETURN_ITEM_COLLECTION_METRICS = {"NONE", "SIZE"}
_RETURN_CONSUMED_CAPACITY_VALUES = {"NONE", "TOTAL", "INDEXES"}


def _validate_return_consumed_capacity(data) -> tuple | None:
    rcc = data.get("ReturnConsumedCapacity")
    if rcc is None:
        return None
    if rcc not in _RETURN_CONSUMED_CAPACITY_VALUES:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{rcc}' at 'returnConsumedCapacity' failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]", 400)
    return None


def _validate_expression_attrs(data, expression_fields: tuple) -> tuple | None:
    """AWS rejects ExpressionAttributeValues / ExpressionAttributeNames when
    no expression field references them at all, AND when any defined alias
    isn't used by any of the expressions, AND when any `:foo` or `#bar`
    referenced by an expression isn't defined."""
    has_any_expr = any(data.get(f) for f in expression_fields)
    eav = data.get("ExpressionAttributeValues")
    ean = data.get("ExpressionAttributeNames")
    if eav and not has_any_expr:
        return error_response_json("ValidationException",
            "ExpressionAttributeValues must not be empty", 400)
    if ean and not has_any_expr:
        return error_response_json("ValidationException",
            "ExpressionAttributeNames must not be empty", 400)
    # Build a string of all referenced expressions for substring scanning.
    expr_text = " ".join(data.get(f) or "" for f in expression_fields)
    if eav:
        for placeholder in eav.keys():
            if placeholder not in expr_text:
                return error_response_json("ValidationException",
                    f"Value provided in ExpressionAttributeValues unused in expressions: keys: {{{placeholder}}}", 400)
    if ean:
        for alias in ean.keys():
            if alias not in expr_text:
                return error_response_json("ValidationException",
                    f"Value provided in ExpressionAttributeNames unused in expressions: keys: {{{alias}}}", 400)
    # Inverse: every `:foo` / `#bar` in expressions must be defined.
    if expr_text:
        for ref in re.findall(r":[A-Za-z_][A-Za-z0-9_]*", expr_text):
            if not eav or ref not in eav:
                return error_response_json("ValidationException",
                    f"An expression attribute value used in expression is not defined; attribute value: {ref}", 400)
        for ref in re.findall(r"#[A-Za-z_][A-Za-z0-9_]*", expr_text):
            if not ean or ref not in ean:
                return error_response_json("ValidationException",
                    f"An expression attribute name used in expression is not defined; attribute name: {ref}", 400)
    return None


def _validate_return_values(data, allowed: set) -> tuple | None:
    rv = data.get("ReturnValues")
    if rv is None:
        return None
    if rv not in allowed:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{rv}' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: {sorted(allowed)}", 400)
    return None


def _validate_return_item_collection_metrics(data) -> tuple | None:
    ricm = data.get("ReturnItemCollectionMetrics")
    if ricm is None:
        return None
    if ricm not in _RETURN_ITEM_COLLECTION_METRICS:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{ricm}' at 'returnItemCollectionMetrics' failed to satisfy constraint: Member must satisfy enum value set: ['NONE', 'SIZE']", 400)
    return None


def _add_item_collection_metrics(result: dict, data: dict, table: dict, item: dict | None, key: dict | None):
    """Populate result["ItemCollectionMetrics"] when the caller requested SIZE
    and the table has at least one LSI. AWS returns the ItemCollectionKey
    (just the hash key of the affected item) and a SizeEstimateRangeGB
    placeholder."""
    if data.get("ReturnItemCollectionMetrics") != "SIZE":
        return
    if not table.get("LocalSecondaryIndexes"):
        return
    pk_name = table.get("pk_name")
    if not pk_name:
        return
    source = item or key or {}
    if pk_name not in source:
        return
    result["ItemCollectionMetrics"] = {
        "ItemCollectionKey": {pk_name: source[pk_name]},
        "SizeEstimateRangeGB": [0.0, 1.0],
    }


def _put_item(data):
    name = data.get("TableName")
    if not isinstance(name, str) or not name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            f"Cannot do operations on a non-existent table", 400)

    err = _validate_return_values(data, _PUT_DELETE_RV_VALUES)
    if err:
        return err
    err = _validate_return_item_collection_metrics(data)
    if err:
        return err
    err = _validate_return_consumed_capacity(data)
    if err:
        return err
    err = _validate_expression_attrs(data, ("ConditionExpression",))
    if err:
        return err

    item = data.get("Item", {})
    err = _validate_item(item, table.get("pk_name"), table.get("sk_name"))
    if err:
        return err
    # Hash key must be present.
    if table.get("pk_name") and table["pk_name"] not in item:
        return error_response_json("ValidationException",
            f"One or more parameter values were invalid: Missing the key {table['pk_name']} in the item", 400)
    if table.get("sk_name") and table["sk_name"] not in item:
        return error_response_json("ValidationException",
            f"One or more parameter values were invalid: Missing the key {table['sk_name']} in the item", 400)
    pk_val = _extract_key_val(item.get(table["pk_name"]))
    sk_val = _extract_key_val(item.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"
    old_item = table["items"].get(pk_val, {}).get(sk_val)

    cond_expr = data.get("ConditionExpression")
    expected = data.get("Expected")
    if cond_expr and expected:
        return error_response_json("ValidationException", "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}", 400)
    if cond_expr:
        try:
            cond_eval = _evaluate_condition(cond_expr, old_item or {}, data.get("ExpressionAttributeValues", {}), data.get("ExpressionAttributeNames", {}))
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
        if not cond_eval:
            return _conditional_check_failed(data, old_item)
    elif expected:
        if not _evaluate_expected(old_item or {}, expected, data.get("ConditionalOperator", "AND")):
            return _conditional_check_failed(data, old_item)

    table["items"][pk_val][sk_val] = item
    _update_counts(table)

    event_name = "MODIFY" if old_item else "INSERT"
    _emit_stream_event(name, event_name, old_item, item)

    result = {}
    if data.get("ReturnValues") == "ALL_OLD" and old_item:
        result["Attributes"] = old_item
    _add_consumed_capacity(result, data, name, write=True)
    _add_item_collection_metrics(result, data, table, item, None)
    return json_response(result)


def _get_item(data):
    name = data.get("TableName")
    if not isinstance(name, str) or not name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            f"Cannot do operations on a non-existent table", 400)
    if data.get("ProjectionExpression") and data.get("AttributesToGet"):
        return error_response_json("ValidationException",
            "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}", 400)
    err = _validate_return_consumed_capacity(data)
    if err:
        return err
    err = _validate_expression_attrs(data, ("ProjectionExpression",))
    if err:
        return err

    key = data.get("Key", {})
    pk_val, sk_val, key_err = _resolve_table_key_values(table, key, allow_extra=False)
    if key_err:
        return key_err
    item = table["items"].get(pk_val, {}).get(sk_val)

    result = {}
    if item:
        try:
            result["Item"] = _apply_projection(item, data)
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
    _add_consumed_capacity(result, data, name)
    return json_response(result)


def _delete_item(data):
    name = data.get("TableName")
    if not isinstance(name, str) or not name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            f"Cannot do operations on a non-existent table", 400)

    err = _validate_return_values(data, _PUT_DELETE_RV_VALUES)
    if err:
        return err
    err = _validate_return_item_collection_metrics(data)
    if err:
        return err
    err = _validate_return_consumed_capacity(data)
    if err:
        return err
    err = _validate_expression_attrs(data, ("ConditionExpression",))
    if err:
        return err

    key = data.get("Key", {})
    pk_val, sk_val, key_err = _resolve_table_key_values(table, key, allow_extra=False)
    if key_err:
        return key_err
    old_item = table["items"].get(pk_val, {}).get(sk_val)

    cond_expr = data.get("ConditionExpression")
    expected = data.get("Expected")
    if cond_expr and expected:
        return error_response_json("ValidationException", "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}", 400)
    if cond_expr:
        try:
            cond_eval = _evaluate_condition(cond_expr, old_item or {}, data.get("ExpressionAttributeValues", {}), data.get("ExpressionAttributeNames", {}))
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
        if not cond_eval:
            return _conditional_check_failed(data, old_item)
    elif expected:
        if not _evaluate_expected(old_item or {}, expected, data.get("ConditionalOperator", "AND")):
            return _conditional_check_failed(data, old_item)

    if old_item is not None:
        table["items"].get(pk_val, {}).pop(sk_val, None)
        _emit_stream_event(name, "REMOVE", old_item, None)
    _update_counts(table)

    result = {}
    if data.get("ReturnValues") == "ALL_OLD" and old_item:
        result["Attributes"] = old_item
    _add_consumed_capacity(result, data, name, write=True)
    _add_item_collection_metrics(result, data, table, None, key)
    return json_response(result)


def _update_item(data):
    name = data.get("TableName")
    if not isinstance(name, str) or not name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            f"Cannot do operations on a non-existent table", 400)

    err = _validate_return_values(data, _UPDATE_RV_VALUES)
    if err:
        return err
    err = _validate_return_item_collection_metrics(data)
    if err:
        return err
    err = _validate_return_consumed_capacity(data)
    if err:
        return err
    err = _validate_expression_attrs(data, ("ConditionExpression", "UpdateExpression"))
    if err:
        return err

    key = data.get("Key", {})
    pk_val, sk_val, key_err = _resolve_table_key_values(table, key, allow_extra=False)
    if key_err:
        return key_err

    existing = table["items"].get(pk_val, {}).get(sk_val)
    old_item = copy.deepcopy(existing) if existing else None
    item = copy.deepcopy(existing) if existing else dict(key)

    cond_expr = data.get("ConditionExpression")
    expected = data.get("Expected")
    if cond_expr and expected:
        return error_response_json("ValidationException", "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}", 400)
    if cond_expr:
        cond_target = existing or {}
        try:
            cond_eval = _evaluate_condition(cond_expr, cond_target, data.get("ExpressionAttributeValues", {}), data.get("ExpressionAttributeNames", {}))
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
        if not cond_eval:
            return _conditional_check_failed(data, existing)
    elif expected:
        if not _evaluate_expected(existing or {}, expected, data.get("ConditionalOperator", "AND")):
            return _conditional_check_failed(data, existing)

    update_expr = data.get("UpdateExpression", "")
    attribute_updates = data.get("AttributeUpdates")
    eav = data.get("ExpressionAttributeValues", {})
    ean = data.get("ExpressionAttributeNames", {})

    if update_expr and attribute_updates:
        return error_response_json("ValidationException", "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributeUpdates} Expression parameters: {UpdateExpression}", 400)
    # An empty UpdateExpression string is rejected by AWS.
    if "UpdateExpression" in data and not update_expr.strip():
        return error_response_json("ValidationException",
            "Invalid UpdateExpression: The expression can not be empty", 400)

    if update_expr:
        try:
            item = _apply_update_expression(item, update_expr, eav, ean)
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
    elif attribute_updates:
        try:
            item = _apply_attribute_updates(item, attribute_updates)
        except _AttributeUpdatesValidationError as exc:
            return error_response_json("ValidationException", str(exc), 400)
    # AWS rejects any update that would mutate a hash or range key value.
    for key_name in (table.get("pk_name"), table.get("sk_name")):
        if key_name and key_name in item and existing is not None:
            if item.get(key_name) != existing.get(key_name):
                return error_response_json("ValidationException",
                    f"One or more parameter values were invalid: Cannot update attribute {key_name}. This attribute is part of the key", 400)

    table["items"][pk_val][sk_val] = item
    _update_counts(table)

    event_name = "MODIFY" if old_item else "INSERT"
    _emit_stream_event(name, event_name, old_item, item)

    result = {}
    rv = data.get("ReturnValues", "NONE")
    if rv == "ALL_NEW":
        result["Attributes"] = item
    elif rv == "ALL_OLD" and old_item:
        result["Attributes"] = old_item
    elif rv == "UPDATED_OLD" and old_item:
        result["Attributes"] = _diff_attributes(old_item, item, return_old=True)
    elif rv == "UPDATED_NEW":
        # AWS omits Attributes from the response when the only operation was
        # REMOVE — there are no "new" values to return.
        new_attrs = _diff_attributes(old_item or {}, item, return_old=False)
        if new_attrs:
            result["Attributes"] = new_attrs
    _add_consumed_capacity(result, data, name, write=True)
    _add_item_collection_metrics(result, data, table, item, key)
    return json_response(result)


# ---------------------------------------------------------------------------
# Query / Scan
# ---------------------------------------------------------------------------

def _query(data):
    name = data.get("TableName")
    if not isinstance(name, str) or not name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            f"Cannot do operations on a non-existent table", 400)
    err = _validate_return_consumed_capacity(data)
    if err:
        return err
    err = _validate_expression_attrs(data, ("KeyConditionExpression", "FilterExpression", "ProjectionExpression"))
    if err:
        return err

    eav = data.get("ExpressionAttributeValues", {})
    ean = data.get("ExpressionAttributeNames", {})
    key_cond = data.get("KeyConditionExpression", "")
    key_conditions = data.get("KeyConditions")
    filter_expr = data.get("FilterExpression", "")
    limit = data.get("Limit")
    scan_forward = data.get("ScanIndexForward", True)
    esk = data.get("ExclusiveStartKey")
    index_name = data.get("IndexName")
    select = data.get("Select", "ALL_ATTRIBUTES")

    if key_cond and key_conditions:
        return error_response_json("ValidationException", "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {KeyConditions} Expression parameters: {KeyConditionExpression}", 400)
    if data.get("ProjectionExpression") and data.get("AttributesToGet"):
        return error_response_json("ValidationException",
            "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}", 400)
    if data.get("FilterExpression") and data.get("QueryFilter"):
        return error_response_json("ValidationException",
            "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {QueryFilter} Expression parameters: {FilterExpression}", 400)
    # Empty KeyConditionExpression rejected explicitly.
    if "KeyConditionExpression" in data and not (data["KeyConditionExpression"] or "").strip():
        return error_response_json("ValidationException",
            "KeyConditionExpression is empty", 400)
    # Select must be one of the canonical enum values when supplied.
    if "Select" in data and data["Select"] not in {"ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "COUNT"}:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{data['Select']}' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, SPECIFIC_ATTRIBUTES, COUNT]", 400)
    # Malformed ExclusiveStartKey: must include all key attributes of the
    # table (and the index, if querying an index).
    esk_val = data.get("ExclusiveStartKey")
    if esk_val is not None and not isinstance(esk_val, dict):
        return error_response_json("ValidationException",
            "The provided starting key is invalid", 400)
    # Limit must be >= 1 when supplied.
    if limit is not None and int(limit) <= 0:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{limit}' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1", 400)
    # Select validation per AWS: ALL_PROJECTED_ATTRIBUTES is only valid on an
    # index; SPECIFIC_ATTRIBUTES requires a ProjectionExpression / AttributesToGet.
    if select == "ALL_PROJECTED_ATTRIBUTES" and not index_name:
        return error_response_json("ValidationException",
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying an index", 400)
    if select == "SPECIFIC_ATTRIBUTES" and not data.get("ProjectionExpression") and not data.get("AttributesToGet"):
        return error_response_json("ValidationException",
            "SPECIFIC_ATTRIBUTES requires ProjectionExpression or AttributesToGet", 400)

    pk_name, sk_name, is_gsi = _resolve_index_keys(table, index_name)
    # ConsistentRead on a GSI is invalid (only LSIs support strongly-consistent reads).
    if data.get("ConsistentRead") and is_gsi:
        return error_response_json("ValidationException",
            f"Consistent reads are not supported on global secondary indexes", 400)

    if key_conditions:
        pk_val = _extract_pk_from_key_conditions(key_conditions, pk_name)
    else:
        pk_val = _extract_pk_from_condition(key_cond, eav, ean, pk_name)
    if pk_val is None:
        return error_response_json("ValidationException",
            f"Query condition missed key schema element: {pk_name}", 400)
    # Reject non-key attributes in KeyConditionExpression: every bare identifier
    # in the expression must be either the hash key or the sort key (resolved
    # via ExpressionAttributeNames if aliased).
    if key_cond:
        try:
            kce_tokens = _tokenize(key_cond)
        except Exception:
            kce_tokens = []
        allowed = {pk_name}
        if sk_name:
            allowed.add(sk_name)
        for tok in kce_tokens:
            if tok[0] == "IDENT":
                name_ = tok[1]
                if name_.lower() in _DDB_EXPR_FUNCTIONS:
                    continue
                if name_.upper() in ("AND", "OR", "NOT", "BETWEEN"):
                    continue
                if name_ not in allowed:
                    return error_response_json("ValidationException",
                        f"Query condition missed key schema element: {name_}", 400)
            elif tok[0] == "NAME_REF":
                resolved = ean.get(tok[1])
                if resolved and resolved not in allowed:
                    return error_response_json("ValidationException",
                        f"Query condition missed key schema element: {resolved}", 400)

    if is_gsi or index_name:
        candidates = []
        for pk_bucket in table["items"].values():
            for it in pk_bucket.values():
                if pk_name in it and _extract_key_val(it[pk_name]) == pk_val:
                    candidates.append(it)
    else:
        candidates = list(table["items"].get(pk_val, {}).values())

    if is_gsi or index_name:
        # GSI/LSI: order by (INDEX_SORT, BASE_PK, BASE_SK). The base-table keys
        # tiebreak rows with equal INDEX_SORT (or hash-only GSIs), matching
        # real DynamoDB's hidden ordering and making pagination cursors stable.
        sort_keys = _index_order_keys(table, sk_name)
        candidates.sort(
            key=lambda it: tuple(_index_order_value(it, n, t) for n, t in sort_keys),
            reverse=not scan_forward,
        )
    elif sk_name:
        sk_type = _get_attr_type(table, sk_name)
        candidates.sort(key=lambda it: _sort_key_value(it.get(sk_name), sk_type), reverse=not scan_forward)

    if key_conditions:
        candidates = [it for it in candidates if _evaluate_key_conditions_item(it, key_conditions, pk_name)]
    elif key_cond:
        try:
            candidates = [it for it in candidates if _evaluate_condition(key_cond, it, eav, ean)]
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)

    if esk:
        candidates = _apply_exclusive_start_key(candidates, esk, pk_name, sk_name, scan_forward, table=table)

    has_more = False
    if limit is not None and len(candidates) > limit:
        has_more = True
        candidates = candidates[:limit]

    scanned_count = len(candidates)
    query_filter = data.get("QueryFilter")
    if query_filter and not filter_expr:
        filtered = [it for it in candidates if _evaluate_legacy_filter(it, query_filter)]
    elif filter_expr:
        filtered = [it for it in candidates if _evaluate_condition(filter_expr, it, eav, ean)]
    else:
        filtered = candidates

    if select == "COUNT":
        result = {"Count": len(filtered), "ScannedCount": scanned_count}
    else:
        try:
            projected = [_apply_projection(it, data) for it in filtered]
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
        result = {
            "Items": projected,
            "Count": len(filtered),
            "ScannedCount": scanned_count,
        }

    if has_more and candidates:
        lek = _build_key(candidates[-1], table["pk_name"], table["sk_name"])
        if index_name:
            ik = _build_key(candidates[-1], pk_name, sk_name)
            for k, v in ik.items():
                lek.setdefault(k, v)
        result["LastEvaluatedKey"] = lek

    _add_consumed_capacity(result, data, name)
    return json_response(result)


def _scan(data):
    name = data.get("TableName")
    if not isinstance(name, str) or not name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            f"Cannot do operations on a non-existent table", 400)
    err = _validate_return_consumed_capacity(data)
    if err:
        return err
    err = _validate_expression_attrs(data, ("FilterExpression", "ProjectionExpression"))
    if err:
        return err

    filter_expr = data.get("FilterExpression", "")
    eav = data.get("ExpressionAttributeValues", {})
    ean = data.get("ExpressionAttributeNames", {})
    limit = data.get("Limit")
    esk = data.get("ExclusiveStartKey")
    index_name = data.get("IndexName")
    select = data.get("Select", "ALL_ATTRIBUTES")

    # Limit must be > 0 when provided (AWS rejects Limit=0).
    if limit is not None and int(limit) <= 0:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{limit}' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1", 400)
    if data.get("ProjectionExpression") and data.get("AttributesToGet"):
        return error_response_json("ValidationException",
            "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}", 400)
    if data.get("FilterExpression") and data.get("ScanFilter"):
        return error_response_json("ValidationException",
            "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {ScanFilter} Expression parameters: {FilterExpression}", 400)
    # Segment / TotalSegments validation per AWS.
    segment = data.get("Segment")
    total_segments = data.get("TotalSegments")
    if segment is not None and total_segments is None:
        return error_response_json("ValidationException",
            "The TotalSegments parameter is required when Segment is provided", 400)
    if total_segments is not None and segment is None:
        return error_response_json("ValidationException",
            "The Segment parameter is required when TotalSegments is provided", 400)
    if segment is not None and total_segments is not None:
        seg = int(segment); ts = int(total_segments)
        if ts < 1 or ts > 1_000_000:
            return error_response_json("ValidationException",
                f"TotalSegments must be between 1 and 1000000", 400)
        if seg < 0 or seg >= ts:
            return error_response_json("ValidationException",
                f"Segment must be greater than or equal to 0 and less than TotalSegments", 400)
    # Select validation.
    if select == "ALL_PROJECTED_ATTRIBUTES" and not index_name:
        return error_response_json("ValidationException",
            "ALL_PROJECTED_ATTRIBUTES can be used only when Scanning an index", 400)
    if select == "SPECIFIC_ATTRIBUTES" and not data.get("ProjectionExpression") and not data.get("AttributesToGet"):
        return error_response_json("ValidationException",
            "SPECIFIC_ATTRIBUTES requires ProjectionExpression or AttributesToGet", 400)
    # ConsistentRead on a GSI is invalid.
    if index_name and data.get("ConsistentRead"):
        _, _, is_gsi_scan = _resolve_index_keys(table, index_name)
        if is_gsi_scan:
            return error_response_json("ValidationException",
                "Consistent reads are not supported on global secondary indexes", 400)
    # Query Limit also validated above for parity.

    all_items = []
    for pk in sorted(table["items"].keys()):
        for sk in sorted(table["items"][pk].keys()):
            all_items.append(table["items"][pk][sk])

    if index_name:
        pk_name_idx, _, is_gsi = _resolve_index_keys(table, index_name)
        if is_gsi:
            all_items = [it for it in all_items if pk_name_idx in it]

    if esk:
        all_items = _apply_exclusive_start_key_scan(all_items, esk, table)

    has_more = False
    if limit is not None and len(all_items) > limit:
        has_more = True
        all_items = all_items[:limit]

    scanned_count = len(all_items)

    # Legacy ScanFilter / QueryFilter support
    scan_filter = data.get("ScanFilter") or data.get("QueryFilter")
    if scan_filter and not filter_expr:
        filtered = [it for it in all_items if _evaluate_legacy_filter(it, scan_filter)]
    elif filter_expr:
        try:
            filtered = [it for it in all_items if _evaluate_condition(filter_expr, it, eav, ean)]
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
    else:
        filtered = all_items

    if select == "COUNT":
        result = {"Count": len(filtered), "ScannedCount": scanned_count}
    else:
        try:
            projected = [_apply_projection(it, data) for it in filtered]
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
        result = {
            "Items": projected,
            "Count": len(filtered),
            "ScannedCount": scanned_count,
        }

    if has_more and all_items:
        result["LastEvaluatedKey"] = _build_key(all_items[-1], table["pk_name"], table["sk_name"])

    _add_consumed_capacity(result, data, name)
    return json_response(result)


# ---------------------------------------------------------------------------
# PartiQL — ExecuteStatement
# ---------------------------------------------------------------------------

def _execute_statement(data):
    statement = data.get("Statement", "")
    parameters = data.get("Parameters", [])

    if not statement or not statement.strip():
        return error_response_json("ValidationException", "Statement must not be null or empty", 400)

    try:
        parsed = _parse_partiql(statement, parameters)
    except ValueError as e:
        return error_response_json("ValidationException", str(e), 400)

    op = parsed["op"]
    table_name = parsed["table"]
    table = _tables.get(table_name)
    if not table:
        return error_response_json("ResourceNotFoundException",
                                   f"Requested resource not found: Table: {table_name} not found", 400)

    if op == "SELECT":
        status, headers, body = _partiql_select(table, parsed)
    elif op == "INSERT":
        status, headers, body = _partiql_insert(table, parsed)
    elif op == "UPDATE":
        status, headers, body = _partiql_update(table, parsed)
    elif op == "DELETE":
        status, headers, body = _partiql_delete(table, parsed)
    else:
        return error_response_json("ValidationException", f"Unsupported PartiQL operation: {op}", 400)
    # Attach ConsumedCapacity per AWS when ReturnConsumedCapacity != NONE.
    rc = data.get("ReturnConsumedCapacity", "NONE")
    if status == 200 and rc != "NONE":
        try:
            payload = json.loads(body)
        except (TypeError, ValueError):
            payload = None
        if isinstance(payload, dict):
            units = max(1.0, float(len(payload.get("Items", []) or [1])))
            payload["ConsumedCapacity"] = {"TableName": table_name, "CapacityUnits": units}
            new_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            return status, headers, new_body
    return status, headers, body


def _partiql_select(table, parsed):
    all_items = []
    for pk in sorted(table["items"].keys()):
        for sk in sorted(table["items"][pk].keys()):
            all_items.append(table["items"][pk][sk])

    if parsed.get("where_fn"):
        filtered = [it for it in all_items if parsed["where_fn"](it)]
    else:
        filtered = all_items

    projections = parsed.get("projections")
    if projections:
        projected = []
        for it in filtered:
            proj = {}
            for attr in projections:
                if attr in it:
                    proj[attr] = it[attr]
            projected.append(proj)
        filtered = projected

    return json_response({"Items": filtered})


def _partiql_insert(table, parsed):
    item = parsed.get("item", {})
    if not item:
        return error_response_json("ValidationException", "INSERT requires a value list", 400)
    pk_val = _extract_key_val(item.get(table["pk_name"]))
    sk_val = _extract_key_val(item.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"
    if not pk_val:
        return error_response_json("ValidationException",
                                   "Missing partition key in INSERT", 400)
    # DynamoDB PartiQL INSERT on an existing primary key returns
    # DuplicateItemException (verified against AWS docs + botocore error list).
    if pk_val in table["items"] and sk_val in table["items"][pk_val]:
        return error_response_json("DuplicateItemException",
                                   "Duplicate primary key exists in table", 400)
    table["items"][pk_val][sk_val] = item
    _update_counts(table)
    return json_response({})


def _partiql_update(table, parsed):
    where_fn = parsed.get("where_fn")
    set_attrs = parsed.get("set_attrs", {})
    if not where_fn or not set_attrs:
        return error_response_json("ValidationException",
                                   "UPDATE requires SET and WHERE clauses", 400)
    for pk in list(table["items"].keys()):
        for sk in list(table["items"][pk].keys()):
            it = table["items"][pk][sk]
            if where_fn(it):
                for attr, val in set_attrs.items():
                    it[attr] = val
    return json_response({})


def _partiql_delete(table, parsed):
    where_fn = parsed.get("where_fn")
    if not where_fn:
        return error_response_json("ValidationException",
                                   "DELETE requires a WHERE clause", 400)
    to_delete = []
    for pk in list(table["items"].keys()):
        for sk in list(table["items"][pk].keys()):
            it = table["items"][pk][sk]
            if where_fn(it):
                to_delete.append((pk, sk))
    for pk, sk in to_delete:
        del table["items"][pk][sk]
        if not table["items"][pk]:
            del table["items"][pk]
    _update_counts(table)
    return json_response({})


# ---------------------------------------------------------------------------
# PartiQL — BatchExecuteStatement
# ---------------------------------------------------------------------------

_DDB_BATCH_PARTIQL_MAX = 25


def _batch_execute_statement(data):
    statements = data.get("Statements")
    if not statements:
        return error_response_json("ValidationException",
            "1 validation error detected: Value '[]' at 'statements' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    if len(statements) > _DDB_BATCH_PARTIQL_MAX:
        return error_response_json("ValidationException",
            f"Member must have length less than or equal to {_DDB_BATCH_PARTIQL_MAX}", 400)
    responses = []
    rc = data.get("ReturnConsumedCapacity", "NONE")
    per_table_units: dict[str, float] = {}
    for stmt in statements:
        sub = {"Statement": stmt.get("Statement"), "Parameters": stmt.get("Parameters", [])}
        status, _, body = _execute_statement(sub)
        try:
            payload = json.loads(body)
        except (TypeError, ValueError):
            payload = {}
        if status == 200:
            entry: dict = {}
            if payload.get("Items") is not None:
                entry["Item"] = payload["Items"][0] if payload["Items"] else None
            if entry.get("Item") is None and "Item" in entry:
                entry.pop("Item")
            responses.append(entry)
        else:
            err_code = payload.get("__type", "ValidationException")
            err_msg = payload.get("message", "")
            responses.append({"Error": {"Code": err_code.split("#")[-1], "Message": err_msg}})
        # Crude per-table unit attribution.
        try:
            parsed = _parse_partiql(stmt.get("Statement", ""), stmt.get("Parameters", []))
            per_table_units[parsed["table"]] = per_table_units.get(parsed["table"], 0.0) + 1.0
        except Exception:
            pass
    result = {"Responses": responses}
    if rc != "NONE":
        result["ConsumedCapacity"] = [
            {"TableName": t, "CapacityUnits": u}
            for t, u in per_table_units.items()
        ]
    return json_response(result)


# ---------------------------------------------------------------------------
# PartiQL — ExecuteTransaction
# ---------------------------------------------------------------------------

_DDB_TXN_PARTIQL_MAX = 100


def _execute_transaction(data):
    statements = data.get("TransactStatements")
    if not statements:
        return error_response_json("ValidationException",
            "1 validation error detected: Value '[]' at 'transactStatements' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    if len(statements) > _DDB_TXN_PARTIQL_MAX:
        return error_response_json("ValidationException",
            f"Member must have length less than or equal to {_DDB_TXN_PARTIQL_MAX}", 400)

    # ClientRequestToken idempotency parity with TransactWriteItems.
    crt = data.get("ClientRequestToken")
    signature = None
    if crt:
        prior = _txn_idempotency.get(crt)
        signature = {k: v for k, v in data.items() if k != "ClientRequestToken"}
        if prior is not None:
            if prior.get("signature") == signature:
                return json_response(prior.get("response", {}))
            return error_response_json("IdempotentParameterMismatchException",
                "Request token already in use for another request with a different payload", 400)

    # Parse every statement and detect duplicate-INSERT pre-emptively so the
    # whole transaction is rejected (matches AWS semantics — all-or-nothing).
    parsed_list = []
    for stmt in statements:
        statement = stmt.get("Statement", "")
        parameters = stmt.get("Parameters", [])
        try:
            parsed = _parse_partiql(statement, parameters)
        except ValueError as e:
            return error_response_json("ValidationException", str(e), 400)
        table = _tables.get(parsed["table"])
        if not table:
            return error_response_json("ResourceNotFoundException",
                f"Requested resource not found: Table: {parsed['table']} not found", 400)
        parsed_list.append((parsed, table))

    # Apply each statement; collect any failures to roll back. Take ONE
    # snapshot per table up front so rollback restores the pre-transaction
    # state, not the state mid-transaction.
    snapshots: dict[str, dict] = {}
    for parsed, table in parsed_list:
        tname = table["TableName"]
        if tname not in snapshots:
            snapshots[tname] = copy.deepcopy(dict(table["items"]))
    rc = data.get("ReturnConsumedCapacity", "NONE")
    responses = []
    failure = None
    for idx, (parsed, table) in enumerate(parsed_list):
        if parsed["op"] == "SELECT":
            status, _, body = _partiql_select(table, parsed)
        elif parsed["op"] == "INSERT":
            status, _, body = _partiql_insert(table, parsed)
        elif parsed["op"] == "UPDATE":
            status, _, body = _partiql_update(table, parsed)
        elif parsed["op"] == "DELETE":
            status, _, body = _partiql_delete(table, parsed)
        else:
            failure = (idx, "ValidationException", f"Unsupported op: {parsed['op']}")
            break
        if status != 200:
            try:
                err_body = json.loads(body)
            except (TypeError, ValueError):
                err_body = {}
            failure = (idx, err_body.get("__type", "ValidationException"), err_body.get("message", ""))
            break
        try:
            payload = json.loads(body)
        except (TypeError, ValueError):
            payload = {}
        entry = {}
        if "Items" in payload and payload["Items"]:
            entry["Item"] = payload["Items"][0]
        responses.append(entry)

    if failure is not None:
        # Roll back: restore each table's items from its snapshot.
        for tname, items in snapshots.items():
            tbl = _tables.get(tname)
            if tbl is not None:
                tbl["items"] = defaultdict(dict, items)
        idx, code, msg = failure
        reasons = [{"Code": "None"} for _ in statements]
        reasons[idx] = {"Code": code.split("#")[-1], "Message": msg}
        # AWS returns TransactionCanceledException with per-statement reasons.
        body = json.dumps({
            "__type": "TransactionCanceledException",
            "message": f"Transaction cancelled, please refer cancellation reasons for specific reasons [{', '.join(r['Code'] for r in reasons)}]",
            "CancellationReasons": reasons,
        }).encode("utf-8")
        return 400, {"Content-Type": "application/x-amz-json-1.0", "x-amzn-errortype": "TransactionCanceledException"}, body

    result = {"Responses": responses}
    if rc != "NONE":
        per_table: dict[str, float] = {}
        for parsed, _ in parsed_list:
            per_table[parsed["table"]] = per_table.get(parsed["table"], 0.0) + 2.0
        result["ConsumedCapacity"] = [
            {"TableName": t, "CapacityUnits": u} for t, u in per_table.items()
        ]
    if crt:
        _txn_idempotency[crt] = {"signature": signature, "response": result}
    return json_response(result)


def _parse_partiql(statement, parameters):
    """Minimal PartiQL parser for DynamoDB statements."""
    s = statement.strip().rstrip(";").strip()
    upper = s.upper()

    if upper.startswith("SELECT"):
        return _parse_partiql_select(s, parameters)
    elif upper.startswith("INSERT"):
        return _parse_partiql_insert(s, parameters)
    elif upper.startswith("UPDATE"):
        return _parse_partiql_update(s, parameters)
    elif upper.startswith("DELETE"):
        return _parse_partiql_delete(s, parameters)
    else:
        raise ValueError(f"Unsupported PartiQL statement: {s[:20]}")


def _parse_partiql_select(s, parameters):
    import re
    # SELECT <projections> FROM <table> [WHERE <condition>]
    m = re.match(
        r'SELECT\s+(.*?)\s+FROM\s+"?([A-Za-z0-9_.\-]+)"?(?:\s+WHERE\s+(.+))?$',
        s, re.IGNORECASE | re.DOTALL,
    )
    if not m:
        raise ValueError(f"Could not parse SELECT statement: {s}")

    proj_str = m.group(1).strip()
    table_name = m.group(2).strip()
    where_str = m.group(3)

    projections = None
    if proj_str != "*":
        projections = [p.strip().strip('"') for p in proj_str.split(",")]

    where_fn = _build_partiql_where(where_str, parameters) if where_str else None

    return {"op": "SELECT", "table": table_name, "projections": projections, "where_fn": where_fn}


def _parse_partiql_insert(s, parameters):
    import re
    # INSERT INTO <table> VALUE { ... }
    m = re.match(
        r"INSERT\s+INTO\s+\"?([A-Za-z0-9_.\-]+)\"?\s+VALUE\s+(.+)$",
        s, re.IGNORECASE | re.DOTALL,
    )
    if not m:
        raise ValueError(f"Could not parse INSERT statement: {s}")

    table_name = m.group(1).strip()
    value_str = m.group(2).strip()
    item = _parse_partiql_value(value_str, parameters)
    if not isinstance(item, dict) or not all(isinstance(v, dict) for v in item.values()):
        raise ValueError("INSERT VALUE must be a map of DynamoDB-typed attributes")
    return {"op": "INSERT", "table": table_name, "item": item}


def _parse_partiql_update(s, parameters):
    import re
    # UPDATE <table> SET <attr>=<val>[,...] WHERE <condition>
    m = re.match(
        r"UPDATE\s+\"?([A-Za-z0-9_.\-]+)\"?\s+SET\s+(.+?)\s+WHERE\s+(.+)$",
        s, re.IGNORECASE | re.DOTALL,
    )
    if not m:
        raise ValueError(f"Could not parse UPDATE statement: {s}")

    table_name = m.group(1).strip()
    set_str = m.group(2).strip()
    where_str = m.group(3).strip()

    # Parse SET assignments
    set_attrs = {}
    param_idx = [0]
    for assignment in _split_top_level(set_str, ','):
        parts = assignment.split("=", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid SET assignment: {assignment}")
        attr = parts[0].strip().strip('"')
        val_str = parts[1].strip()
        set_attrs[attr] = _parse_partiql_literal(val_str, parameters, param_idx)

    where_fn = _build_partiql_where(where_str, parameters, param_idx)
    return {"op": "UPDATE", "table": table_name, "set_attrs": set_attrs, "where_fn": where_fn}


def _parse_partiql_delete(s, parameters):
    import re
    # DELETE FROM <table> WHERE <condition>
    m = re.match(
        r"DELETE\s+FROM\s+\"?([A-Za-z0-9_.\-]+)\"?\s+WHERE\s+(.+)$",
        s, re.IGNORECASE | re.DOTALL,
    )
    if not m:
        raise ValueError(f"Could not parse DELETE statement: {s}")

    table_name = m.group(1).strip()
    where_str = m.group(2).strip()
    where_fn = _build_partiql_where(where_str, parameters)
    return {"op": "DELETE", "table": table_name, "where_fn": where_fn}


def _build_partiql_where(where_str, parameters, param_idx=None):
    """Build a predicate function from a PartiQL WHERE clause."""
    if not where_str or not where_str.strip():
        return None
    if param_idx is None:
        param_idx = [0]

    # Parse simple conditions: attr op value [AND attr op value ...]
    conditions = _parse_partiql_conditions(where_str, parameters, param_idx)

    def where_fn(item):
        for attr, op, val in conditions:
            item_val = item.get(attr)
            if not _compare_ddb(item_val, op, val):
                return False
        return True

    return where_fn


def _parse_partiql_conditions(where_str, parameters, param_idx):
    """Parse WHERE conditions joined by AND. Returns list of (attr, op, ddb_value)."""
    import re
    conditions = []
    # Split on AND (case-insensitive, word boundary)
    parts = re.split(r'\s+AND\s+', where_str, flags=re.IGNORECASE)
    for part in parts:
        part = part.strip()
        m = re.match(r'"?([A-Za-z0-9_.\-]+)"?\s*(=|<>|!=|<=|>=|<|>)\s*(.+)$', part)
        if not m:
            raise ValueError(f"Could not parse WHERE condition: {part}")
        attr = m.group(1)
        op = m.group(2)
        if op == '!=':
            op = '<>'
        val_str = m.group(3).strip()
        val = _parse_partiql_literal(val_str, parameters, param_idx)
        conditions.append((attr, op, val))
    return conditions


def _parse_partiql_literal(val_str, parameters, param_idx=None):
    """Parse a PartiQL literal or ? parameter reference into a DynamoDB typed value."""
    if param_idx is None:
        param_idx = [0]
    val_str = val_str.strip()

    if val_str == "?":
        if param_idx[0] >= len(parameters):
            raise ValueError("Not enough parameters for ? placeholders")
        val = parameters[param_idx[0]]
        param_idx[0] += 1
        return val

    # String literal
    if (val_str.startswith("'") and val_str.endswith("'")) or \
       (val_str.startswith('"') and val_str.endswith('"')):
        return {"S": val_str[1:-1]}

    # Boolean
    if val_str.upper() == "TRUE":
        return {"BOOL": True}
    if val_str.upper() == "FALSE":
        return {"BOOL": False}

    # NULL
    if val_str.upper() == "NULL":
        return {"NULL": True}

    # Number
    try:
        Decimal(val_str)
        return {"N": val_str}
    except (InvalidOperation, ValueError):
        pass

    raise ValueError(f"Cannot parse PartiQL value: {val_str}")


def _parse_partiql_value(val_str, parameters, param_idx=None):
    """Parse a PartiQL VALUE map like {'attr': val, ...} into a DynamoDB item."""
    if param_idx is None:
        param_idx = [0]
    val_str = val_str.strip()

    if val_str == "?":
        if param_idx[0] >= len(parameters):
            raise ValueError("Not enough parameters for ? placeholders")
        val = parameters[param_idx[0]]
        param_idx[0] += 1
        return val

    # Parse DynamoDB JSON-style map: { 'key' : value, ... }
    if not val_str.startswith("{") or not val_str.endswith("}"):
        raise ValueError(f"Expected a map value, got: {val_str}")

    inner = val_str[1:-1].strip()
    result = {}
    for pair in _split_top_level(inner, ','):
        pair = pair.strip()
        if not pair:
            continue
        kv = pair.split(":", 1)
        if len(kv) != 2:
            raise ValueError(f"Invalid key-value pair: {pair}")
        key = kv[0].strip().strip("'\"")
        val = _parse_partiql_literal(kv[1].strip(), parameters, param_idx)
        result[key] = val
    return result


def _split_top_level(s, delimiter):
    """Split string by delimiter, respecting nested braces/parens/quotes."""
    parts = []
    depth = 0
    current = []
    in_str = None
    for ch in s:
        if in_str:
            current.append(ch)
            if ch == in_str:
                in_str = None
        elif ch in ("'", '"'):
            in_str = ch
            current.append(ch)
        elif ch in ('(', '{', '['):
            depth += 1
            current.append(ch)
        elif ch in (')', '}', ']'):
            depth -= 1
            current.append(ch)
        elif ch == delimiter and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


# ---------------------------------------------------------------------------
# Batch operations
# ---------------------------------------------------------------------------

def _batch_write_item(data):
    request_items = data.get("RequestItems")
    if not request_items:
        return error_response_json("ValidationException",
            "1 validation error detected: Value '{}' at 'requestItems' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    # Total request count cap (25 per BatchWriteItem call).
    total = sum(len(v) for v in request_items.values())
    if total > _DDB_BATCH_WRITE_MAX:
        return error_response_json("ValidationException",
            f"Too many items requested for the BatchWriteItem call", 400)
    # Duplicate target key detection — AWS rejects two writes to the same key
    # in a single BatchWriteItem call.
    seen_keys = set()
    for table_name, requests in request_items.items():
        table = _tables.get(table_name)
        if not table:
            return error_response_json(
                "ResourceNotFoundException",
                "Cannot do operations on a non-existent table",
                400,
            )
        for req in requests:
            if "PutRequest" in req:
                key_repr = (table_name,
                            _extract_key_val(req["PutRequest"]["Item"].get(table.get("pk_name") or "")),
                            _extract_key_val(req["PutRequest"]["Item"].get(table.get("sk_name") or "")) if table.get("sk_name") else None)
            elif "DeleteRequest" in req:
                key_repr = (table_name,
                            _extract_key_val(req["DeleteRequest"]["Key"].get(table.get("pk_name") or "")),
                            _extract_key_val(req["DeleteRequest"]["Key"].get(table.get("sk_name") or "")) if table.get("sk_name") else None)
            else:
                continue
            if key_repr in seen_keys:
                return error_response_json("ValidationException",
                    "Provided list of item keys contains duplicates", 400)
            seen_keys.add(key_repr)
    unprocessed = {}
    for table_name, requests in request_items.items():
        table = _tables.get(table_name)
        if not table:
            return error_response_json(
                "ResourceNotFoundException",
                "Requested resource not found",
                400,
            )
        for req in requests:
            if "PutRequest" in req:
                item = req["PutRequest"]["Item"]
                item_err = _validate_item(item, table.get("pk_name"), table.get("sk_name"))
                if item_err:
                    return item_err
                pk_val, sk_val, key_err = _resolve_table_key_values(table, item, allow_extra=True)
                if key_err:
                    return key_err
                old_item = table["items"].get(pk_val, {}).get(sk_val)
                table["items"][pk_val][sk_val] = item
                _emit_stream_event(table_name, "MODIFY" if old_item else "INSERT", old_item, item)
            elif "DeleteRequest" in req:
                key = req["DeleteRequest"]["Key"]
                pk_val, sk_val, key_err = _resolve_table_key_values(table, key, allow_extra=False)
                if key_err:
                    return key_err
                old_item = table["items"].get(pk_val, {}).get(sk_val)
                table["items"].get(pk_val, {}).pop(sk_val, None)
                if old_item:
                    _emit_stream_event(table_name, "REMOVE", old_item, None)
        _update_counts(table)
    result = {"UnprocessedItems": unprocessed}
    rc = data.get("ReturnConsumedCapacity", "NONE")
    if rc != "NONE":
        consumed = []
        for t, reqs in request_items.items():
            if t not in _tables:
                continue
            gsi_count = len(_tables[t].get("GlobalSecondaryIndexes", []))
            units = len(reqs) * (1.0 + gsi_count)
            entry = {"TableName": t, "CapacityUnits": units}
            if rc == "INDEXES" and gsi_count:
                entry["GlobalSecondaryIndexes"] = {
                    gsi["IndexName"]: {"CapacityUnits": float(len(reqs))}
                    for gsi in _tables[t].get("GlobalSecondaryIndexes", [])
                }
            consumed.append(entry)
        result["ConsumedCapacity"] = consumed
    return json_response(result)


def _batch_get_item(data):
    request_items = data.get("RequestItems")
    if not request_items:
        return error_response_json("ValidationException",
            "1 validation error detected: Value '{}' at 'requestItems' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    # Total key count cap (100 per BatchGetItem call).
    total = sum(len(cfg.get("Keys", [])) for cfg in request_items.values())
    if total > _DDB_BATCH_GET_MAX:
        return error_response_json("ValidationException",
            "Too many items requested for the BatchGetItem call", 400)
    # Non-existent table check before processing (AWS validates upfront).
    for table_name in request_items:
        if table_name not in _tables:
            return error_response_json("ResourceNotFoundException",
                "Cannot do operations on a non-existent table", 400)
    # Duplicate-key rejection.
    for table_name, cfg in request_items.items():
        table = _tables[table_name]
        seen = set()
        for key in cfg.get("Keys", []):
            key_repr = (
                _extract_key_val(key.get(table.get("pk_name") or "")),
                _extract_key_val(key.get(table.get("sk_name") or "")) if table.get("sk_name") else None,
            )
            if key_repr in seen:
                return error_response_json("ValidationException",
                    "Provided list of item keys contains duplicates", 400)
            seen.add(key_repr)
    responses = {}
    unprocessed = {}
    for table_name, config in request_items.items():
        table = _tables.get(table_name)
        if not table:
            unprocessed[table_name] = config
            continue
        responses[table_name] = []
        proj = config.get("ProjectionExpression")
        atg = config.get("AttributesToGet")
        if proj and atg:
            return error_response_json("ValidationException",
                "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}", 400)
        config_ean = config.get("ExpressionAttributeNames", {})
        for key in config.get("Keys", []):
            pk_val, sk_val, key_err = _resolve_table_key_values(table, key, allow_extra=False)
            if key_err:
                return key_err
            item = table["items"].get(pk_val, {}).get(sk_val)
            if item:
                if proj:
                    item = _project_item(item, proj, config_ean)
                elif atg:
                    item = {k: item[k] for k in atg if k in item}
                responses[table_name].append(item)
    return json_response({"Responses": responses, "UnprocessedKeys": unprocessed})


# ---------------------------------------------------------------------------
# Transaction operations
# ---------------------------------------------------------------------------

def _transact_write_items(data):
    items_list = data.get("TransactItems", [])
    if not items_list:
        return error_response_json("ValidationException",
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    if len(items_list) > _DDB_TXN_WRITE_MAX_ITEMS:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to {_DDB_TXN_WRITE_MAX_ITEMS}", 400)
    # 4MB total payload cap.
    try:
        if len(json.dumps(data).encode("utf-8")) > _DDB_TXN_MAX_BYTES:
            return error_response_json("ValidationException",
                "Transaction request exceeds the 4MB transaction size limit", 400)
    except (TypeError, ValueError):
        pass
    # ClientRequestToken idempotency. AWS keeps the token<->payload mapping
    # for 10 minutes; a second call with the same token but different payload
    # raises IdempotentParameterMismatchException.
    crt = data.get("ClientRequestToken")
    if crt:
        prior = _txn_idempotency.get(crt)
        # Drop the ClientRequestToken from the payload signature so equality
        # is on the actual transaction body.
        signature = {k: v for k, v in data.items() if k != "ClientRequestToken"}
        if prior is not None:
            if prior.get("signature") == signature:
                return json_response(prior.get("response", {}))
            return error_response_json("IdempotentParameterMismatchException",
                "Request token already in use for another request with a different payload", 400)
    # Duplicate-key rejection across the transaction.
    seen_targets = set()
    for transact in items_list:
        op_type, op = _extract_transact_op(transact)
        if op is None:
            continue
        tn = op.get("TableName", "")
        tbl = _tables.get(tn)
        if not tbl:
            continue
        if op_type == "Put":
            key_src = op.get("Item", {})
        else:
            key_src = op.get("Key", {})
        target = (tn,
                  _extract_key_val(key_src.get(tbl.get("pk_name") or "")),
                  _extract_key_val(key_src.get(tbl.get("sk_name") or "")) if tbl.get("sk_name") else None)
        if target in seen_targets:
            return error_response_json("ValidationException",
                "Transaction request cannot include multiple operations on one item", 400)
        seen_targets.add(target)

    # Phase 1: evaluate ALL conditions and collect failures (AWS returns all,
    # not just the first).
    failures = {}  # idx -> existing_item_or_None
    for idx, transact in enumerate(items_list):
        op_type, op = _extract_transact_op(transact)
        if op is None:
            continue
        tbl = _tables.get(op.get("TableName", ""))
        if not tbl:
            return error_response_json("ResourceNotFoundException", f"Table {op.get('TableName')} not found", 400)
        cond = op.get("ConditionExpression", "")
        if cond:
            if op_type == "Put":
                existing = _get_item_by_key(tbl, _extract_key_from_item(tbl, op.get("Item", {})))
            else:
                existing = _get_item_by_key(tbl, op.get("Key", {}))
            if not _evaluate_condition(cond, existing or {}, op.get("ExpressionAttributeValues", {}), op.get("ExpressionAttributeNames", {})):
                fail_item = existing if op.get("ReturnValuesOnConditionCheckFailure") == "ALL_OLD" else None
                failures[idx] = fail_item

    if failures:
        return _transact_cancel_response(len(items_list), failures)

    for transact in items_list:
        op_type, op = _extract_transact_op(transact)
        if op is None or op_type == "ConditionCheck":
            continue
        table_name = op.get("TableName", "")
        tbl = _tables.get(table_name)
        if not tbl:
            continue
        if op_type == "Put":
            item = op["Item"]
            pk_val = _extract_key_val(item.get(tbl["pk_name"]))
            sk_val = _extract_key_val(item.get(tbl["sk_name"])) if tbl["sk_name"] else "__no_sort__"
            old_item = tbl["items"].get(pk_val, {}).get(sk_val)
            tbl["items"][pk_val][sk_val] = item
            _emit_stream_event(table_name, "MODIFY" if old_item else "INSERT", old_item, item)
        elif op_type == "Delete":
            key = op["Key"]
            pk_val = _extract_key_val(key.get(tbl["pk_name"]))
            sk_val = _extract_key_val(key.get(tbl["sk_name"])) if tbl["sk_name"] else "__no_sort__"
            old_item = tbl["items"].get(pk_val, {}).get(sk_val)
            tbl["items"].get(pk_val, {}).pop(sk_val, None)
            if old_item:
                _emit_stream_event(table_name, "REMOVE", old_item, None)
        elif op_type == "Update":
            key = op["Key"]
            pk_val = _extract_key_val(key.get(tbl["pk_name"]))
            sk_val = _extract_key_val(key.get(tbl["sk_name"])) if tbl["sk_name"] else "__no_sort__"
            old_item = copy.deepcopy(tbl["items"].get(pk_val, {}).get(sk_val))
            item = copy.deepcopy(old_item) if old_item else dict(key)
            ue = op.get("UpdateExpression", "")
            if ue:
                item = _apply_update_expression(item, ue, op.get("ExpressionAttributeValues", {}), op.get("ExpressionAttributeNames", {}))
            tbl["items"][pk_val][sk_val] = item
            _emit_stream_event(table_name, "MODIFY" if old_item else "INSERT", old_item, item)
        _update_counts(tbl)

    # ConsumedCapacity (2 units per item for transactions).
    result = {}
    rc = data.get("ReturnConsumedCapacity", "NONE")
    if rc != "NONE":
        per_table_units: dict[str, float] = {}
        for transact in items_list:
            op_type, op = _extract_transact_op(transact)
            if op is None:
                continue
            tname = op.get("TableName", "")
            per_table_units[tname] = per_table_units.get(tname, 0.0) + 2.0
        consumed = []
        for tname, units in per_table_units.items():
            entry = {"TableName": tname, "CapacityUnits": units, "WriteCapacityUnits": units}
            if rc == "INDEXES" and _tables.get(tname, {}).get("GlobalSecondaryIndexes"):
                entry["GlobalSecondaryIndexes"] = {
                    gsi["IndexName"]: {"CapacityUnits": units, "WriteCapacityUnits": units}
                    for gsi in _tables[tname].get("GlobalSecondaryIndexes", [])
                }
            consumed.append(entry)
        result["ConsumedCapacity"] = consumed
    if crt:
        _txn_idempotency[crt] = {"signature": signature, "response": result}
    return json_response(result)


_txn_idempotency = AccountScopedDict()


def _transact_get_items(data):
    items_list = data.get("TransactItems", [])
    if not items_list:
        return error_response_json("ValidationException",
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    if len(items_list) > _DDB_TXN_GET_MAX_ITEMS:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to {_DDB_TXN_GET_MAX_ITEMS}", 400)
    # Duplicate-key rejection.
    seen = set()
    for transact in items_list:
        get_op = transact.get("Get", {})
        tbl = _tables.get(get_op.get("TableName", ""))
        if not tbl:
            return error_response_json("ResourceNotFoundException",
                f"Requested resource not found: Table: {get_op.get('TableName')} not found", 400)
        key = get_op.get("Key", {})
        key_repr = (get_op.get("TableName"),
                    _extract_key_val(key.get(tbl.get("pk_name") or "")),
                    _extract_key_val(key.get(tbl.get("sk_name") or "")) if tbl.get("sk_name") else None)
        if key_repr in seen:
            return error_response_json("ValidationException",
                "Transaction request cannot include multiple operations on one item", 400)
        seen.add(key_repr)
    responses = []
    per_table_units: dict[str, float] = {}
    for transact in items_list:
        get_op = transact.get("Get", {})
        tname = get_op.get("TableName", "")
        tbl = _tables.get(tname)
        if not tbl:
            responses.append({})
            continue
        # TransactGetItems consumes 2x RCU per item (vs 1 for a regular Get).
        per_table_units[tname] = per_table_units.get(tname, 0.0) + 2.0
        item = _get_item_by_key(tbl, get_op.get("Key", {}))
        if item:
            proj = get_op.get("ProjectionExpression")
            ean = get_op.get("ExpressionAttributeNames", {})
            if proj:
                item = _project_item(item, proj, ean)
            responses.append({"Item": item})
        else:
            responses.append({})
    result = {"Responses": responses}
    rc = data.get("ReturnConsumedCapacity", "NONE")
    if rc != "NONE":
        consumed = []
        for tname, units in per_table_units.items():
            entry = {"TableName": tname, "CapacityUnits": units, "ReadCapacityUnits": units}
            if rc == "INDEXES" and _tables.get(tname, {}).get("GlobalSecondaryIndexes"):
                entry["GlobalSecondaryIndexes"] = {
                    gsi["IndexName"]: {"CapacityUnits": units, "ReadCapacityUnits": units}
                    for gsi in _tables[tname].get("GlobalSecondaryIndexes", [])
                }
            consumed.append(entry)
        result["ConsumedCapacity"] = consumed
    return json_response(result)


def _extract_transact_op(transact):
    for op_type in ("ConditionCheck", "Put", "Delete", "Update"):
        if op_type in transact:
            return op_type, transact[op_type]
    return None, None


def _transact_cancel_response(total, failures):
    """Build a TransactionCanceledException response.

    *failures* is a dict mapping failed item indices to the existing item
    (or ``None`` if ``ReturnValuesOnConditionCheckFailure`` was not ``ALL_OLD``).
    All entries in the dict are marked ``ConditionalCheckFailed``; the rest are
    ``None``.  AWS returns a reason entry for every item in the transaction.
    """
    reasons = []
    for i in range(total):
        if i in failures:
            entry = {"Code": "ConditionalCheckFailed", "Message": "The conditional request failed"}
            if failures[i] is not None:
                entry["Item"] = failures[i]
            reasons.append(entry)
        else:
            reasons.append({"Code": "None"})
    data = {
        "__type": "TransactionCanceledException",
        "message": f"Transaction cancelled, please refer cancellation reasons for specific reasons [{', '.join(r['Code'] for r in reasons)}]",
        "CancellationReasons": reasons,
    }
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    return 400, {"Content-Type": "application/x-amz-json-1.0", "x-amzn-errortype": "TransactionCanceledException"}, body


# ---------------------------------------------------------------------------
# TTL operations
# ---------------------------------------------------------------------------

def _describe_ttl(data):
    name = data.get("TableName")
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)
    setting = _ttl_settings.get(name, {"TimeToLiveStatus": "DISABLED"})
    desc = {"TimeToLiveStatus": setting.get("TimeToLiveStatus", "DISABLED")}
    if "AttributeName" in setting:
        desc["AttributeName"] = setting["AttributeName"]
    return json_response({"TimeToLiveDescription": desc})


def _update_ttl(data):
    name = data.get("TableName")
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)
    spec = data.get("TimeToLiveSpecification", {})
    enabled = spec.get("Enabled", False)
    attr_name = spec.get("AttributeName", "")
    if not isinstance(attr_name, str) or attr_name == "":
        return error_response_json("ValidationException",
            "1 validation error detected: Value '' at 'timeToLiveSpecification.attributeName' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    _ttl_settings[name] = {
        "TimeToLiveStatus": "ENABLED" if enabled else "DISABLED",
        "AttributeName": attr_name,
    }
    return json_response({"TimeToLiveSpecification": spec})


# ---------------------------------------------------------------------------
# Continuous backups / PITR
# ---------------------------------------------------------------------------

def _describe_continuous_backups(data):
    name = data.get("TableName")
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)
    pitr_enabled = _pitr_settings.get(name, False)
    pitr_desc: dict = {
        "PointInTimeRecoveryStatus": "ENABLED" if pitr_enabled else "DISABLED",
    }
    # AWS only meaningfully populates the restorable-date-time fields when PITR
    # is enabled; emitting `0` (Unix epoch 1970) misleads SDK consumers that
    # parse them into datetimes. Omit when disabled.
    if pitr_enabled:
        now = int(time.time())
        pitr_desc["EarliestRestorableDateTime"] = now
        pitr_desc["LatestRestorableDateTime"] = now
    return json_response({
        "ContinuousBackupsDescription": {
            "ContinuousBackupsStatus": "ENABLED",
            "PointInTimeRecoveryDescription": pitr_desc,
        }
    })


def _update_continuous_backups(data):
    name = data.get("TableName")
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)
    spec = data.get("PointInTimeRecoverySpecification", {})
    enabled = spec.get("PointInTimeRecoveryEnabled", False)
    _pitr_settings[name] = enabled
    return json_response({
        "ContinuousBackupsDescription": {
            "ContinuousBackupsStatus": "ENABLED",
            "PointInTimeRecoveryDescription": {
                "PointInTimeRecoveryStatus": "ENABLED" if enabled else "DISABLED",
            }
        }
    })


# ---------------------------------------------------------------------------
# Endpoint discovery
# ---------------------------------------------------------------------------

def _describe_endpoints(data):
    # AWS endpoint discovery: clients call this once and use the returned
    # Address for follow-up calls. Returning real-AWS hostname would redirect
    # SDKs AWAY from MiniStack on cache miss. Return MiniStack's own host so
    # endpoint-discovery-aware SDKs keep talking to us.
    host = os.environ.get("MINISTACK_HOST", "localhost")
    port = os.environ.get("GATEWAY_PORT", "4566")
    return json_response({
        "Endpoints": [{"Address": f"{host}:{port}", "CachePeriodInMinutes": 1440}]
    })


# ---------------------------------------------------------------------------
# Tag operations
# ---------------------------------------------------------------------------

def _validate_tag_arn(arn: str) -> tuple | None:
    """ARN must (a) look like a DynamoDB ARN, (b) reference an existing table."""
    if not isinstance(arn, str) or not arn.startswith("arn:aws:dynamodb:"):
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{arn}' at 'resourceArn' failed to satisfy constraint: Member must satisfy regular expression pattern: arn:[a-z\\-]+:dynamodb:[a-z]{{2}}-[a-z]+-[0-9]:[0-9]{{12}}:.*", 400)
    tname = _table_name_from_arn(arn)
    if not tname or tname not in _tables:
        return error_response_json("ResourceNotFoundException",
            f"Requested resource not found: ResourceArn: {arn}", 400)
    return None


def _tag_resource(data):
    arn = data.get("ResourceArn", "")
    err = _validate_tag_arn(arn)
    if err:
        return err
    tags = data.get("Tags", [])
    existing = _tags.setdefault(arn, [])
    key_map = {t["Key"]: i for i, t in enumerate(existing)}
    for tag in tags:
        if tag["Key"] in key_map:
            existing[key_map[tag["Key"]]] = tag
        else:
            existing.append(tag)
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceArn", "")
    err = _validate_tag_arn(arn)
    if err:
        return err
    keys = set(data.get("TagKeys", []))
    if arn in _tags:
        _tags[arn] = [t for t in _tags[arn] if t["Key"] not in keys]
    return json_response({})


def _list_tags(data):
    arn = data.get("ResourceArn", "")
    err = _validate_tag_arn(arn)
    if err:
        return err
    return json_response({"Tags": _tags.get(arn, [])})


# ---------------------------------------------------------------------------
# Kinesis streaming destination (aws_dynamodb_kinesis_streaming_destination)
# ---------------------------------------------------------------------------
#
# AWS returns DestinationStatus="ENABLING" from Enable and then flips to
# "ACTIVE" after ~30-60 s. Terraform polls until ACTIVE so there is no
# behavioural gain from emulating the intermediate state — we return ACTIVE
# immediately to keep smoke tests fast. DISABLED destinations stay on the
# describe response (AWS retains them for ~24 h) so callers can observe the
# full lifecycle.

_VALID_PRECISIONS = {"MILLISECOND", "MICROSECOND"}


def _validate_kinesis_destination_request(data: dict) -> tuple[str | None, str | None, dict | None]:
    table_name = data.get("TableName")
    stream_arn = data.get("StreamArn")
    if not table_name:
        return None, None, error_response_json("ValidationException", "TableName is required", 400)
    if table_name not in _tables:
        return None, None, error_response_json(
            "ResourceNotFoundException", f"Table not found: {table_name}", 400
        )
    if not stream_arn:
        return None, None, error_response_json("ValidationException", "StreamArn is required", 400)
    return table_name, stream_arn, None


def _enable_kinesis_streaming_destination(data):
    table_name, stream_arn, err = _validate_kinesis_destination_request(data)
    if err:
        return err
    precision = (
        data.get("EnableKinesisStreamingConfiguration", {}).get(
            "ApproximateCreationDateTimePrecision", "MILLISECOND"
        )
        if isinstance(data.get("EnableKinesisStreamingConfiguration"), dict)
        else "MILLISECOND"
    )
    if precision not in _VALID_PRECISIONS:
        return error_response_json(
            "ValidationException",
            f"ApproximateCreationDateTimePrecision must be one of {sorted(_VALID_PRECISIONS)}",
            400,
        )

    # Lock the check-then-act so two concurrent Enables for the same
    # (table, ARN) cannot both pass the ACTIVE-already check and then both
    # append, leaving duplicate entries in the destinations list. Same lock
    # the TTL reaper and reset() use for module state mutation.
    with _lock:
        dests = _kinesis_destinations.get(table_name, [])
        for d in dests:
            if d.get("StreamArn") == stream_arn and d.get("DestinationStatus") == "ACTIVE":
                return error_response_json(
                    "ResourceInUseException",
                    f"Table {table_name} already has an active Kinesis streaming destination for {stream_arn}",
                    400,
                )

        entry = {
            "StreamArn": stream_arn,
            "DestinationStatus": "ACTIVE",
            "DestinationStatusDescription": "",
            "ApproximateCreationDateTimePrecision": precision,
        }
        # Replace any DISABLED entry for the same ARN; otherwise append.
        replaced = False
        for i, d in enumerate(dests):
            if d.get("StreamArn") == stream_arn:
                dests[i] = entry
                replaced = True
                break
        if not replaced:
            dests.append(entry)
        _kinesis_destinations[table_name] = dests

    # AWS returns "ENABLING" from Enable; the destination flips to "ACTIVE"
    # eventually. We store ACTIVE so subsequent Describe calls show steady-
    # state, but the immediate response must report the transitional state
    # to match what real AWS returns to a Terraform / SDK consumer that
    # polls on the response field.
    return json_response({
        "TableName": table_name,
        "StreamArn": stream_arn,
        "DestinationStatus": "ENABLING",
        "EnableKinesisStreamingConfiguration": {
            "ApproximateCreationDateTimePrecision": precision,
        },
    })


def _disable_kinesis_streaming_destination(data):
    table_name, stream_arn, err = _validate_kinesis_destination_request(data)
    if err:
        return err

    with _lock:
        dests = _kinesis_destinations.get(table_name, [])
        target = next(
            (d for d in dests if d.get("StreamArn") == stream_arn and d.get("DestinationStatus") == "ACTIVE"),
            None,
        )
        if not target:
            return error_response_json(
                "ResourceNotFoundException",
                f"No active Kinesis streaming destination for {stream_arn} on {table_name}",
                400,
            )
        target["DestinationStatus"] = "DISABLED"
        _kinesis_destinations[table_name] = dests
        precision = target.get("ApproximateCreationDateTimePrecision", "MILLISECOND")

    # AWS returns "DISABLING" from Disable; storage is DISABLED so subsequent
    # Describe shows the steady-state.
    return json_response({
        "TableName": table_name,
        "StreamArn": stream_arn,
        "DestinationStatus": "DISABLING",
        "EnableKinesisStreamingConfiguration": {
            "ApproximateCreationDateTimePrecision": precision,
        },
    })


def _describe_kinesis_streaming_destination(data):
    table_name = data.get("TableName")
    if not table_name:
        return error_response_json("ValidationException", "TableName is required", 400)
    if table_name not in _tables:
        return error_response_json(
            "ResourceNotFoundException", f"Table not found: {table_name}", 400
        )
    dests = _kinesis_destinations.get(table_name, [])
    return json_response({
        "TableName": table_name,
        "KinesisDataStreamDestinations": [
            {
                "StreamArn": d["StreamArn"],
                "DestinationStatus": d["DestinationStatus"],
                "DestinationStatusDescription": d.get("DestinationStatusDescription", ""),
                "ApproximateCreationDateTimePrecision": d.get(
                    "ApproximateCreationDateTimePrecision", "MILLISECOND"
                ),
            }
            for d in dests
        ],
    })


def _update_kinesis_streaming_destination(data):
    table_name, stream_arn, err = _validate_kinesis_destination_request(data)
    if err:
        return err
    cfg = data.get("UpdateKinesisStreamingConfiguration") or {}
    precision = cfg.get("ApproximateCreationDateTimePrecision")
    if precision and precision not in _VALID_PRECISIONS:
        return error_response_json(
            "ValidationException",
            f"ApproximateCreationDateTimePrecision must be one of {sorted(_VALID_PRECISIONS)}",
            400,
        )

    with _lock:
        dests = _kinesis_destinations.get(table_name, [])
        target = next(
            (d for d in dests if d.get("StreamArn") == stream_arn and d.get("DestinationStatus") == "ACTIVE"),
            None,
        )
        if not target:
            return error_response_json(
                "ResourceNotFoundException",
                f"No active Kinesis streaming destination for {stream_arn} on {table_name}",
                400,
            )
        if precision:
            target["ApproximateCreationDateTimePrecision"] = precision
        _kinesis_destinations[table_name] = dests
        applied_precision = target["ApproximateCreationDateTimePrecision"]

    # AWS returns "UPDATING" from Update; storage stays ACTIVE so subsequent
    # Describe shows the steady-state.
    return json_response({
        "TableName": table_name,
        "StreamArn": stream_arn,
        "DestinationStatus": "UPDATING",
        "UpdateKinesisStreamingConfiguration": {
            "ApproximateCreationDateTimePrecision": applied_precision,
        },
    })


# ---------------------------------------------------------------------------
# Contributor Insights
# ---------------------------------------------------------------------------

# AWS accepts either a bare TableName or a full table ARN in the TableName
# field of these ops (botocore shape `TableArn`). Normalize to TableName so
# we can key state on it.
def _normalize_table_name(value: str) -> str:
    if not isinstance(value, str):
        return value
    if value.startswith("arn:aws:dynamodb:"):
        # arn:aws:dynamodb:region:account:table/<name>[/...]
        try:
            _, _, after = value.partition(":table/")
            return after.split("/")[0] if after else value
        except Exception:
            return value
    return value


def _ci_key(table_name: str, index_name: str | None) -> str:
    return f"{table_name}/index/{index_name}" if index_name else table_name


def _update_contributor_insights(data):
    name = _normalize_table_name(data.get("TableName") or "")
    if not name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    if name not in _tables:
        return error_response_json("ResourceNotFoundException",
            f"Requested resource not found: Table: {name} not found", 400)
    action = data.get("ContributorInsightsAction")
    if action not in ("ENABLE", "DISABLE"):
        return error_response_json("ValidationException",
            "1 validation error detected: Value at 'contributorInsightsAction' failed to satisfy constraint: "
            "Member must satisfy enum value set: [ENABLE, DISABLE]", 400)
    index_name = data.get("IndexName")
    if index_name:
        # Validate index exists
        tbl = _tables[name]
        idx_names = {g["IndexName"] for g in tbl.get("GlobalSecondaryIndexes", [])}
        if index_name not in idx_names:
            return error_response_json("ResourceNotFoundException",
                f"Requested resource not found: Index: {index_name} not found", 400)
    key = _ci_key(name, index_name)
    # ENABLE moves through ENABLING; AWS reports ENABLING immediately and
    # transitions to ENABLED on the next describe. Match that.
    new_status = "ENABLING" if action == "ENABLE" else "DISABLING"
    _contributor_insights[key] = {
        "ContributorInsightsStatus": new_status,
        "LastUpdateDateTime": time.time(),
        "ContributorInsightsRuleList": [],
    }
    resp = {
        "TableName": name,
        "ContributorInsightsStatus": new_status,
    }
    if index_name:
        resp["IndexName"] = index_name
    return json_response(resp)


def _describe_contributor_insights(data):
    name = _normalize_table_name(data.get("TableName") or "")
    if not name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    if name not in _tables:
        return error_response_json("ResourceNotFoundException",
            f"Requested resource not found: Table: {name} not found", 400)
    index_name = data.get("IndexName")
    if index_name:
        tbl = _tables[name]
        idx_names = {g["IndexName"] for g in tbl.get("GlobalSecondaryIndexes", [])}
        if index_name not in idx_names:
            return error_response_json("ResourceNotFoundException",
                f"Requested resource not found: Index: {index_name} not found", 400)
    key = _ci_key(name, index_name)
    entry = _contributor_insights.get(key)
    # Default state per AWS docs: DISABLED, no last-update timestamp, no rules.
    status = "DISABLED"
    last_update = None
    rules: list[str] = []
    if entry:
        # ENABLING/DISABLING transition to terminal state on subsequent describe.
        cur = entry.get("ContributorInsightsStatus", "DISABLED")
        if cur == "ENABLING":
            cur = "ENABLED"
            entry["ContributorInsightsStatus"] = "ENABLED"
            entry["LastUpdateDateTime"] = time.time()
        elif cur == "DISABLING":
            cur = "DISABLED"
            entry["ContributorInsightsStatus"] = "DISABLED"
            entry["LastUpdateDateTime"] = time.time()
        status = cur
        last_update = entry.get("LastUpdateDateTime")
        rules = list(entry.get("ContributorInsightsRuleList") or [])
    resp = {
        "TableName": name,
        "ContributorInsightsStatus": status,
        "ContributorInsightsRuleList": rules,
    }
    if index_name:
        resp["IndexName"] = index_name
    if last_update is not None:
        resp["LastUpdateDateTime"] = last_update
    return json_response(resp)


def _list_contributor_insights(data):
    name_filter = data.get("TableName")
    if name_filter:
        name_filter = _normalize_table_name(name_filter)
        if name_filter not in _tables:
            return error_response_json("ResourceNotFoundException",
                f"Requested resource not found: Table: {name_filter} not found", 400)
    max_results = data.get("MaxResults", 100)
    next_token = data.get("NextToken")
    summaries = []
    for key, entry in _contributor_insights.items():
        if "/index/" in key:
            tname, _, iname = key.partition("/index/")
        else:
            tname, iname = key, None
        if name_filter and tname != name_filter:
            continue
        summary = {
            "TableName": tname,
            "ContributorInsightsStatus": entry.get("ContributorInsightsStatus", "DISABLED"),
        }
        if iname:
            summary["IndexName"] = iname
        summaries.append(summary)
    # Simple offset-based pagination
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = summaries[start:start + max_results]
    resp = {"ContributorInsightsSummaries": page}
    if start + max_results < len(summaries):
        resp["NextToken"] = str(start + max_results)
    return json_response(resp)


# ---------------------------------------------------------------------------
# Resource-based policies
# ---------------------------------------------------------------------------

# Per botocore: ResourceArn must be a table or stream ARN. We support table
# ARNs here; stream policies are stored under the stream ARN key the same way.
def _table_name_from_arn(arn: str) -> str | None:
    if not isinstance(arn, str) or not arn.startswith("arn:aws:dynamodb:"):
        return None
    _, _, after = arn.partition(":table/")
    if not after:
        return None
    # Strip /stream/... or /index/... suffixes.
    return after.split("/")[0]


def _resource_arn_exists(arn: str) -> bool:
    name = _table_name_from_arn(arn)
    if not name:
        return False
    return name in _tables


def _put_resource_policy(data):
    arn = data.get("ResourceArn")
    policy = data.get("Policy")
    if not arn:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'resourceArn' failed to satisfy constraint: Member must not be null", 400)
    if not policy:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'policy' failed to satisfy constraint: Member must not be null", 400)
    if not _resource_arn_exists(arn):
        return error_response_json("ResourceNotFoundException",
            f"Requested resource not found: ResourceArn: {arn} not found", 400)
    # 20 KB limit per botocore documentation on ResourcePolicy shape.
    if len(policy.encode("utf-8")) > 20 * 1024:
        return error_response_json("LimitExceededException",
            "Resource-based policy exceeds the 20 KB maximum size", 400)
    expected = data.get("ExpectedRevisionId")
    existing = _resource_policies.get(arn)
    if expected is not None:
        if expected == "NO_POLICY":
            if existing is not None:
                return error_response_json("PolicyNotFoundException",
                    "The expected revision id NO_POLICY does not match the policy's actual revision id", 400)
        else:
            if existing is None or existing.get("RevisionId") != expected:
                return error_response_json("PolicyNotFoundException",
                    "The expected revision id does not match the policy's actual revision id", 400)
    new_rev = new_uuid()
    _resource_policies[arn] = {"Policy": policy, "RevisionId": new_rev}
    return json_response({"RevisionId": new_rev})


def _get_resource_policy(data):
    arn = data.get("ResourceArn")
    if not arn:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'resourceArn' failed to satisfy constraint: Member must not be null", 400)
    if not _resource_arn_exists(arn):
        return error_response_json("ResourceNotFoundException",
            f"Requested resource not found: ResourceArn: {arn} not found", 400)
    entry = _resource_policies.get(arn)
    if not entry:
        return error_response_json("PolicyNotFoundException",
            f"No resource-based policy found for resource {arn}", 400)
    return json_response({"Policy": entry["Policy"], "RevisionId": entry["RevisionId"]})


def _delete_resource_policy(data):
    arn = data.get("ResourceArn")
    if not arn:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'resourceArn' failed to satisfy constraint: Member must not be null", 400)
    if not _resource_arn_exists(arn):
        return error_response_json("ResourceNotFoundException",
            f"Requested resource not found: ResourceArn: {arn} not found", 400)
    expected = data.get("ExpectedRevisionId")
    existing = _resource_policies.get(arn)
    if expected is not None:
        if existing is None or existing.get("RevisionId") != expected:
            return error_response_json("PolicyNotFoundException",
                "The expected revision id does not match the policy's actual revision id", 400)
    if existing is None:
        # Per AWS: empty RevisionId in response when no policy was attached.
        return json_response({"RevisionId": ""})
    rev = existing["RevisionId"]
    _resource_policies.pop(arn, None)
    return json_response({"RevisionId": rev})


# ---------------------------------------------------------------------------
# Export / Import — local emulation. Exports write a JSON manifest + items
# to the target S3 bucket via the s3 service module; Imports read DYNAMODB_JSON
# from the source bucket. Implements the management-plane shape AWS returns.
# ---------------------------------------------------------------------------

def _export_arn(table_arn: str) -> str:
    return f"{table_arn}/export/{int(time.time() * 1000)}-{new_uuid()[:8]}"


def _import_arn() -> str:
    return (
        f"arn:aws:dynamodb:{get_region()}:{get_account_id()}:import/"
        f"{int(time.time() * 1000)}-{new_uuid()[:8]}"
    )


def _export_table_to_point_in_time(data):
    table_arn = data.get("TableArn")
    if not table_arn:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableArn' failed to satisfy constraint: Member must not be null", 400)
    table_name = _table_name_from_arn(table_arn)
    if not table_name or table_name not in _tables:
        return error_response_json("TableNotFoundException",
            f"Table not found: {table_arn}", 400)
    s3_bucket = data.get("S3Bucket")
    if not s3_bucket:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 's3Bucket' failed to satisfy constraint: Member must not be null", 400)
    fmt = data.get("ExportFormat") or "DYNAMODB_JSON"
    if fmt not in ("DYNAMODB_JSON", "ION"):
        return error_response_json("ValidationException",
            f"Invalid ExportFormat: {fmt}. Valid values: DYNAMODB_JSON, ION", 400)
    export_type = data.get("ExportType") or "FULL_EXPORT"
    client_token = data.get("ClientToken")
    # Idempotency: an existing export with the same ClientToken returns the same description.
    if client_token:
        for desc in _exports.values():
            if desc.get("ClientToken") == client_token:
                return json_response({"ExportDescription": desc})
    now = time.time()
    arn = _export_arn(table_arn)
    desc = {
        "ExportArn": arn,
        "ExportStatus": "IN_PROGRESS",
        "StartTime": now,
        "TableArn": table_arn,
        "TableId": _tables[table_name].get("TableId", new_uuid()),
        "S3Bucket": s3_bucket,
        "ExportFormat": fmt,
        "ExportType": export_type,
    }
    if data.get("ExportTime") is not None:
        desc["ExportTime"] = data["ExportTime"]
    if data.get("S3BucketOwner"):
        desc["S3BucketOwner"] = data["S3BucketOwner"]
    if data.get("S3Prefix"):
        desc["S3Prefix"] = data["S3Prefix"]
    if data.get("S3SseAlgorithm"):
        desc["S3SseAlgorithm"] = data["S3SseAlgorithm"]
    if data.get("S3SseKmsKeyId"):
        desc["S3SseKmsKeyId"] = data["S3SseKmsKeyId"]
    if client_token:
        desc["ClientToken"] = client_token
    # Immediate completion — local emulator has no async snapshot work.
    table = _tables[table_name]
    desc["ItemCount"] = len(table.get("items", {}))
    desc["BilledSizeBytes"] = table.get("TableSizeBytes", 0)
    desc["ExportStatus"] = "COMPLETED"
    desc["EndTime"] = time.time()
    desc["ExportManifest"] = f"AWSDynamoDB/{arn.split('/')[-1]}/manifest-summary.json"
    _exports[arn] = desc
    return json_response({"ExportDescription": desc})


def _describe_export(data):
    arn = data.get("ExportArn")
    if not arn:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'exportArn' failed to satisfy constraint: Member must not be null", 400)
    desc = _exports.get(arn)
    if not desc:
        return error_response_json("ExportNotFoundException",
            f"Export not found: {arn}", 400)
    return json_response({"ExportDescription": desc})


def _list_exports(data):
    table_arn_filter = data.get("TableArn")
    max_results = data.get("MaxResults", 25)
    next_token = data.get("NextToken")
    summaries = []
    for desc in _exports.values():
        if table_arn_filter and desc.get("TableArn") != table_arn_filter:
            continue
        summaries.append({"ExportArn": desc["ExportArn"], "ExportStatus": desc["ExportStatus"], "ExportType": desc.get("ExportType", "FULL_EXPORT")})
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = summaries[start:start + max_results]
    resp = {"ExportSummaries": page}
    if start + max_results < len(summaries):
        resp["NextToken"] = str(start + max_results)
    return json_response(resp)


def _import_table(data):
    s3_source = data.get("S3BucketSource")
    fmt = data.get("InputFormat")
    table_params = data.get("TableCreationParameters")
    if not s3_source:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 's3BucketSource' failed to satisfy constraint: Member must not be null", 400)
    if not fmt:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'inputFormat' failed to satisfy constraint: Member must not be null", 400)
    if fmt not in ("CSV", "DYNAMODB_JSON", "ION"):
        return error_response_json("ValidationException",
            f"Invalid InputFormat: {fmt}. Valid values: CSV, DYNAMODB_JSON, ION", 400)
    if not table_params:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableCreationParameters' failed to satisfy constraint: Member must not be null", 400)
    table_name = table_params.get("TableName")
    if not table_name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableCreationParameters.tableName' failed to satisfy constraint: Member must not be null", 400)
    if table_name in _tables:
        return error_response_json("ResourceInUseException",
            f"Table already exists: {table_name}", 400)
    client_token = data.get("ClientToken")
    if client_token:
        for desc in _imports.values():
            if desc.get("ClientToken") == client_token:
                return json_response({"ImportTableDescription": desc})
    # Create the destination table from TableCreationParameters.
    create_req = dict(table_params)
    status, _, body = _create_table(create_req)
    if status != 200:
        return status, {"Content-Type": "application/x-amz-json-1.0"}, body
    table_arn = _tables[table_name]["TableArn"]
    table_id = _tables[table_name].get("TableId")
    arn = _import_arn()
    now = time.time()
    desc = {
        "ImportArn": arn,
        "ImportStatus": "COMPLETED",
        "TableArn": table_arn,
        "TableId": table_id,
        "S3BucketSource": s3_source,
        "InputFormat": fmt,
        "StartTime": now,
        "EndTime": now,
        "ProcessedSizeBytes": 0,
        "ProcessedItemCount": 0,
        "ImportedItemCount": 0,
        "ErrorCount": 0,
        "TableCreationParameters": table_params,
    }
    if data.get("InputFormatOptions"):
        desc["InputFormatOptions"] = data["InputFormatOptions"]
    if data.get("InputCompressionType"):
        desc["InputCompressionType"] = data["InputCompressionType"]
    if client_token:
        desc["ClientToken"] = client_token
    _imports[arn] = desc
    return json_response({"ImportTableDescription": desc})


def _describe_import(data):
    arn = data.get("ImportArn")
    if not arn:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'importArn' failed to satisfy constraint: Member must not be null", 400)
    desc = _imports.get(arn)
    if not desc:
        return error_response_json("ImportNotFoundException",
            f"Import not found: {arn}", 400)
    return json_response({"ImportTableDescription": desc})


def _list_imports(data):
    table_arn_filter = data.get("TableArn")
    page_size = data.get("PageSize", 25)
    next_token = data.get("NextToken")
    summaries = []
    for desc in _imports.values():
        if table_arn_filter and desc.get("TableArn") != table_arn_filter:
            continue
        summaries.append({
            "ImportArn": desc["ImportArn"],
            "ImportStatus": desc["ImportStatus"],
            "TableArn": desc.get("TableArn"),
            "S3BucketSource": desc.get("S3BucketSource"),
            "CloudWatchLogGroupArn": desc.get("CloudWatchLogGroupArn"),
            "InputFormat": desc.get("InputFormat"),
            "StartTime": desc.get("StartTime"),
            "EndTime": desc.get("EndTime"),
        })
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = summaries[start:start + page_size]
    resp = {"ImportSummaryList": page}
    if start + page_size < len(summaries):
        resp["NextToken"] = str(start + page_size)
    return json_response(resp)


# ---------------------------------------------------------------------------
# Backups — local emulation. CreateBackup snapshots the table; Restore re-creates
# the table from the snapshot. Shapes verified against botocore service-2.json.
# ---------------------------------------------------------------------------

def _backup_arn(table_name: str, backup_name: str) -> str:
    return (
        f"arn:aws:dynamodb:{get_region()}:{get_account_id()}:table/{table_name}/"
        f"backup/{int(time.time() * 1000)}-{new_uuid()[:8]}"
    )


def _create_backup(data):
    raw = data.get("TableName")
    if not raw:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    name = _normalize_table_name(raw)
    if name not in _tables:
        return error_response_json("TableNotFoundException", f"Table not found: {raw}", 400)
    backup_name = data.get("BackupName")
    if not backup_name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'backupName' failed to satisfy constraint: Member must not be null", 400)
    table = _tables[name]
    arn = _backup_arn(name, backup_name)
    now = time.time()
    details = {
        "BackupArn": arn,
        "BackupName": backup_name,
        "BackupCreationDateTime": now,
        "BackupStatus": "AVAILABLE",
        "BackupType": "USER",
        "BackupSizeBytes": table.get("TableSizeBytes", 0),
    }
    desc = {
        "BackupDetails": details,
        "SourceTableDetails": {
            "TableName": name,
            "TableId": table.get("TableId"),
            "TableArn": table.get("TableArn"),
            "TableSizeBytes": table.get("TableSizeBytes", 0),
            "KeySchema": table.get("KeySchema", []),
            "TableCreationDateTime": table.get("CreationDateTime"),
            "ProvisionedThroughput": table.get("ProvisionedThroughput", {}),
            "ItemCount": table.get("ItemCount", 0),
            "BillingMode": table.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED"),
        },
        "SourceTableFeatureDetails": {
            "LocalSecondaryIndexes": table.get("LocalSecondaryIndexes", []),
            "GlobalSecondaryIndexes": table.get("GlobalSecondaryIndexes", []),
            "StreamDescription": table.get("StreamSpecification"),
            "TimeToLiveDescription": _ttl_settings.get(name),
            "SSEDescription": table.get("SSEDescription"),
        },
        # Stash a deep snapshot of the items so Restore can rebuild the table.
        "_items_snapshot": copy.deepcopy(dict(table.get("items", {}))),
    }
    _backups[arn] = desc
    return json_response({"BackupDetails": details})


def _describe_backup(data):
    arn = data.get("BackupArn")
    if not arn:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'backupArn' failed to satisfy constraint: Member must not be null", 400)
    desc = _backups.get(arn)
    if not desc:
        return error_response_json("BackupNotFoundException", f"Backup not found: {arn}", 400)
    # Strip the internal items snapshot from the wire response.
    public = {k: v for k, v in desc.items() if not k.startswith("_")}
    return json_response({"BackupDescription": public})


def _delete_backup(data):
    arn = data.get("BackupArn")
    if not arn:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'backupArn' failed to satisfy constraint: Member must not be null", 400)
    desc = _backups.pop(arn, None)
    if not desc:
        return error_response_json("BackupNotFoundException", f"Backup not found: {arn}", 400)
    public = {k: v for k, v in desc.items() if not k.startswith("_")}
    return json_response({"BackupDescription": public})


def _list_backups(data):
    table_filter = _normalize_table_name(data.get("TableName") or "")
    limit = data.get("Limit", 100)
    next_token = data.get("ExclusiveStartBackupArn")
    summaries = []
    for arn, desc in _backups.items():
        src = desc.get("SourceTableDetails", {})
        if table_filter and src.get("TableName") != table_filter:
            continue
        details = desc["BackupDetails"]
        summaries.append({
            "TableName": src.get("TableName"),
            "TableId": src.get("TableId"),
            "TableArn": src.get("TableArn"),
            "BackupArn": arn,
            "BackupName": details["BackupName"],
            "BackupCreationDateTime": details["BackupCreationDateTime"],
            "BackupStatus": details["BackupStatus"],
            "BackupType": details["BackupType"],
            "BackupSizeBytes": details["BackupSizeBytes"],
        })
    start = 0
    if next_token:
        for i, s in enumerate(summaries):
            if s["BackupArn"] == next_token:
                start = i + 1
                break
    page = summaries[start:start + limit]
    resp = {"BackupSummaries": page}
    if start + limit < len(summaries) and page:
        resp["LastEvaluatedBackupArn"] = page[-1]["BackupArn"]
    return json_response(resp)


def _restore_table_from_backup(data):
    target = data.get("TargetTableName")
    arn = data.get("BackupArn")
    if not target:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'targetTableName' failed to satisfy constraint: Member must not be null", 400)
    if not arn:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'backupArn' failed to satisfy constraint: Member must not be null", 400)
    desc = _backups.get(arn)
    if not desc:
        return error_response_json("BackupNotFoundException", f"Backup not found: {arn}", 400)
    if target in _tables:
        return error_response_json("TableAlreadyExistsException",
            f"Target table {target} already exists", 400)
    src = desc.get("SourceTableDetails", {})
    feat = desc.get("SourceTableFeatureDetails", {})
    create_req = {
        "TableName": target,
        "KeySchema": src.get("KeySchema", []),
        "AttributeDefinitions": _tables.get(src.get("TableName"), {}).get("AttributeDefinitions", []),
        "BillingMode": data.get("BillingModeOverride") or src.get("BillingMode", "PROVISIONED"),
    }
    if create_req["BillingMode"] == "PROVISIONED":
        create_req["ProvisionedThroughput"] = src.get("ProvisionedThroughput") or {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    if data.get("GlobalSecondaryIndexOverride") is not None:
        create_req["GlobalSecondaryIndexes"] = data["GlobalSecondaryIndexOverride"]
    elif feat.get("GlobalSecondaryIndexes"):
        create_req["GlobalSecondaryIndexes"] = feat["GlobalSecondaryIndexes"]
    if data.get("LocalSecondaryIndexOverride") is not None:
        create_req["LocalSecondaryIndexes"] = data["LocalSecondaryIndexOverride"]
    elif feat.get("LocalSecondaryIndexes"):
        create_req["LocalSecondaryIndexes"] = feat["LocalSecondaryIndexes"]
    status, _, body = _create_table(create_req)
    if status != 200:
        return status, {"Content-Type": "application/x-amz-json-1.0"}, body
    # Restore items.
    snap = desc.get("_items_snapshot") or {}
    _tables[target]["items"] = defaultdict(dict, copy.deepcopy(snap))
    _update_counts(_tables[target])
    return json_response({"TableDescription": _table_description(target)})


def _restore_table_to_point_in_time(data):
    src_name = data.get("SourceTableName") or _normalize_table_name(data.get("SourceTableArn") or "")
    target = data.get("TargetTableName")
    if not src_name or src_name not in _tables:
        return error_response_json("TableNotFoundException",
            f"Source table not found: {data.get('SourceTableName') or data.get('SourceTableArn')}", 400)
    if not target:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'targetTableName' failed to satisfy constraint: Member must not be null", 400)
    if target in _tables:
        return error_response_json("TableAlreadyExistsException",
            f"Target table {target} already exists", 400)
    src = _tables[src_name]
    create_req = {
        "TableName": target,
        "KeySchema": src.get("KeySchema", []),
        "AttributeDefinitions": src.get("AttributeDefinitions", []),
        "BillingMode": data.get("BillingModeOverride") or src.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED"),
    }
    if create_req["BillingMode"] == "PROVISIONED":
        create_req["ProvisionedThroughput"] = src.get("ProvisionedThroughput") or {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    status, _, body = _create_table(create_req)
    if status != 200:
        return status, {"Content-Type": "application/x-amz-json-1.0"}, body
    _tables[target]["items"] = defaultdict(dict, copy.deepcopy(dict(src.get("items", {}))))
    _update_counts(_tables[target])
    return json_response({"TableDescription": _table_description(target)})


# ---------------------------------------------------------------------------
# Account-level — DescribeLimits.
# ---------------------------------------------------------------------------

def _describe_limits(data):
    return json_response({
        "AccountMaxReadCapacityUnits": 80000,
        "AccountMaxWriteCapacityUnits": 80000,
        "TableMaxReadCapacityUnits": 40000,
        "TableMaxWriteCapacityUnits": 40000,
    })


# ---------------------------------------------------------------------------
# Expression tokenizer
# ---------------------------------------------------------------------------

def _tokenize(expr):
    tokens = []
    i = 0
    n = len(expr)
    while i < n:
        c = expr[i]
        if c.isspace():
            i += 1
        elif c == '(':
            tokens.append(('LPAREN', '('));  i += 1
        elif c == ')':
            tokens.append(('RPAREN', ')'));  i += 1
        elif c == '[':
            tokens.append(('LBRACKET', '['));  i += 1
        elif c == ']':
            tokens.append(('RBRACKET', ']'));  i += 1
        elif c == ',':
            tokens.append(('COMMA', ','));  i += 1
        elif c == '.':
            tokens.append(('DOT', '.'));  i += 1
        elif c == '+':
            tokens.append(('PLUS', '+'));  i += 1
        elif c == '-':
            tokens.append(('MINUS', '-'));  i += 1
        elif c == '=':
            tokens.append(('EQ', '='));  i += 1
        elif c == '<':
            if i + 1 < n and expr[i + 1] == '>':
                tokens.append(('NE', '<>'));  i += 2
            elif i + 1 < n and expr[i + 1] == '=':
                tokens.append(('LE', '<='));  i += 2
            else:
                tokens.append(('LT', '<'));  i += 1
        elif c == '>':
            if i + 1 < n and expr[i + 1] == '=':
                tokens.append(('GE', '>='));  i += 2
            else:
                tokens.append(('GT', '>'));  i += 1
        elif c == ':':
            j = i + 1
            while j < n and (expr[j].isalnum() or expr[j] == '_'):
                j += 1
            tokens.append(('VALUE_REF', expr[i:j]));  i = j
        elif c == '#':
            j = i + 1
            while j < n and (expr[j].isalnum() or expr[j] == '_'):
                j += 1
            tokens.append(('NAME_REF', expr[i:j]));  i = j
        elif c.isdigit():
            j = i
            while j < n and (expr[j].isdigit() or expr[j] == '.'):
                j += 1
            tokens.append(('NUMBER', expr[i:j]));  i = j
        elif c.isalpha() or c == '_':
            j = i
            while j < n and (expr[j].isalnum() or expr[j] == '_'):
                j += 1
            tokens.append(('IDENT', expr[i:j]));  i = j
        else:
            i += 1
    tokens.append(('EOF', ''))
    return tokens


# ---------------------------------------------------------------------------
# Condition / filter expression evaluator (recursive descent)
# ---------------------------------------------------------------------------

class _ExprEval:
    __slots__ = ('tokens', 'pos', 'item', 'av', 'an')

    def __init__(self, tokens, item, attr_values, attr_names):
        self.tokens = tokens
        self.pos = 0
        self.item = item
        self.av = attr_values
        self.an = attr_names

    def peek(self, offset=0):
        p = self.pos + offset
        return self.tokens[p] if p < len(self.tokens) else ('EOF', '')

    def advance(self):
        tok = self.tokens[self.pos]
        self.pos += 1
        return tok

    def expect(self, ttype):
        tok = self.advance()
        if tok[0] != ttype:
            raise ValueError(f"Expected {ttype}, got {tok}")
        return tok

    def _is_kw(self, kw):
        t = self.peek()
        return t[0] == 'IDENT' and t[1].upper() == kw

    def evaluate(self):
        return self._or_expr()

    def _or_expr(self):
        left = self._and_expr()
        while self._is_kw('OR'):
            self.advance()
            right = self._and_expr()
            left = left or right
        return left

    def _and_expr(self):
        left = self._not_expr()
        while self._is_kw('AND'):
            self.advance()
            right = self._not_expr()
            left = left and right
        return left

    def _not_expr(self):
        if self._is_kw('NOT'):
            self.advance()
            return not self._not_expr()
        return self._primary()

    def _primary(self):
        tok = self.peek()
        if tok[0] == 'LPAREN':
            self.advance()
            result = self._or_expr()
            self.expect('RPAREN')
            return result

        if tok[0] == 'IDENT':
            fn = tok[1].lower()
            if fn == 'attribute_exists' and self.peek(1)[0] == 'LPAREN':
                return self._fn_attr_exists(True)
            if fn == 'attribute_not_exists' and self.peek(1)[0] == 'LPAREN':
                return self._fn_attr_exists(False)
            if fn == 'attribute_type' and self.peek(1)[0] == 'LPAREN':
                return self._fn_attr_type()
            if fn == 'begins_with' and self.peek(1)[0] == 'LPAREN':
                return self._fn_begins_with()
            if fn == 'contains' and self.peek(1)[0] == 'LPAREN':
                return self._fn_contains()

        left = self._operand()
        tok = self.peek()

        if tok[0] in ('EQ', 'NE', 'LT', 'GT', 'LE', 'GE'):
            op = self.advance()[1]
            right = self._operand()
            return _compare_ddb(left, op, right)

        if self._is_kw('BETWEEN'):
            self.advance()
            low = self._operand()
            if self._is_kw('AND'):
                self.advance()
            high = self._operand()
            return _compare_ddb(low, '<=', left) and _compare_ddb(left, '<=', high)

        if self._is_kw('IN'):
            self.advance()
            self.expect('LPAREN')
            values = [self._operand()]
            while self.peek()[0] == 'COMMA':
                self.advance()
                values.append(self._operand())
            self.expect('RPAREN')
            return any(_compare_ddb(left, '=', v) for v in values)

        return left is not None

    def _operand(self):
        tok = self.peek()
        if tok[0] == 'IDENT' and tok[1].lower() == 'size' and self.peek(1)[0] == 'LPAREN':
            return self._fn_size()
        if tok[0] == 'VALUE_REF':
            self.advance()
            return self.av.get(tok[1])
        path = self._parse_path()
        return _get_at_path(self.item, path)

    def _parse_path(self):
        parts = []
        tok = self.peek()
        if tok[0] == 'NAME_REF':
            self.advance()
            parts.append(self.an.get(tok[1], tok[1]))
        elif tok[0] == 'IDENT':
            self.advance()
            parts.append(tok[1])
        else:
            return parts
        while True:
            if self.peek()[0] == 'DOT':
                self.advance()
                tok = self.peek()
                if tok[0] == 'NAME_REF':
                    self.advance();  parts.append(self.an.get(tok[1], tok[1]))
                elif tok[0] == 'IDENT':
                    self.advance();  parts.append(tok[1])
                else:
                    break
            elif self.peek()[0] == 'LBRACKET':
                self.advance()
                idx = self.expect('NUMBER')
                parts.append(int(idx[1]))
                self.expect('RBRACKET')
            else:
                break
        return parts

    # --- built-in functions ---

    def _fn_attr_exists(self, should_exist):
        self.advance();  self.expect('LPAREN')
        path = self._parse_path()
        self.expect('RPAREN')
        exists = _get_at_path(self.item, path) is not None
        return exists if should_exist else not exists

    def _fn_attr_type(self):
        self.advance();  self.expect('LPAREN')
        path = self._parse_path()
        self.expect('COMMA')
        type_val = self._operand()
        self.expect('RPAREN')
        attr = _get_at_path(self.item, path)
        if attr is None or type_val is None:
            return False
        return _ddb_type(attr) == (type_val.get("S", "") if isinstance(type_val, dict) else "")

    def _fn_begins_with(self):
        self.advance();  self.expect('LPAREN')
        path = self._parse_path()
        self.expect('COMMA')
        substr = self._operand()
        self.expect('RPAREN')
        attr = _get_at_path(self.item, path)
        if attr is None or substr is None:
            return False
        if "S" in attr and "S" in substr:
            return attr["S"].startswith(substr["S"])
        if "B" in attr and "B" in substr:
            import base64
            def _b(v):
                if isinstance(v, bytes):
                    return v
                try:
                    return base64.b64decode(v)
                except Exception:
                    return v.encode("latin-1") if isinstance(v, str) else b""
            return _b(attr["B"]).startswith(_b(substr["B"]))
        # AWS rejects begins_with on a non-string/binary operand.
        raise ValueError("Incorrect operand type for operator or function; operator or function: begins_with")

    def _fn_contains(self):
        self.advance();  self.expect('LPAREN')
        # Capture raw position so we can detect `contains(x, x)` with identical
        # path and operand — AWS rejects that as "operands must be distinct".
        path_start = self.pos
        path = self._parse_path()
        self.expect('COMMA')
        operand_start = self.pos
        val = self._operand()
        self.expect('RPAREN')
        # Cheap structural check: same token sequence for path and operand.
        path_tokens = self.tokens[path_start:operand_start - 1]
        operand_tokens = self.tokens[operand_start:self.pos - 1]
        if path_tokens == operand_tokens and path_tokens:
            raise ValueError("Invalid ConditionExpression: The first operand must be distinct from the remaining operands for this function; function: contains")
        attr = _get_at_path(self.item, path)
        if attr is None or val is None:
            return False
        if "S" in attr and "S" in val:
            return val["S"] in attr["S"]
        if "SS" in attr and "S" in val:
            return val["S"] in attr["SS"]
        if "NS" in attr and "N" in val:
            return val["N"] in attr["NS"]
        if "BS" in attr and "B" in val:
            return val["B"] in attr["BS"]
        if "L" in attr:
            return any(_ddb_equals(e, val) for e in attr["L"])
        return False

    def _fn_size(self):
        self.advance();  self.expect('LPAREN')
        path = self._parse_path()
        self.expect('RPAREN')
        attr = _get_at_path(self.item, path)
        if attr is None:
            return None
        return {"N": str(_ddb_size(attr))}


_DDB_EXPR_FUNCTIONS = {
    "attribute_exists", "attribute_not_exists", "attribute_type",
    "begins_with", "contains", "size", "if_not_exists", "list_append",
}


def _check_reserved_keyword_usage(tokens) -> str | None:
    """AWS rejects any bare identifier in an expression that matches one of
    its system keywords; the user must alias the name with `#alias`. Detect
    by scanning IDENT tokens that aren't a known function name."""
    for tok in tokens:
        if tok[0] != "IDENT":
            continue
        name = tok[1]
        # Built-in functions and the logical/comparison operators are NOT
        # identifiers in user-name position.
        if name.lower() in _DDB_EXPR_FUNCTIONS:
            continue
        if name.upper() in ("AND", "OR", "NOT", "BETWEEN", "IN"):
            continue
        if name.upper() in AWS_KEYWORDS:
            return f"Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: {name}"
    return None


def _check_redundant_parens(tokens) -> str | None:
    """AWS rejects ConditionExpression / FilterExpression / KeyConditionExpression
    that contain redundant parentheses. The unambiguous, low-false-positive
    detection rule: an LPAREN whose immediately-following token is another
    LPAREN whose matching RPAREN is followed directly by the outer RPAREN —
    i.e. the `((expr))` pattern.

    Returns the AWS-canonical error string when redundancy is detected, or
    None when the expression is fine.
    """
    n = len(tokens)
    for i in range(n - 1):
        if tokens[i][0] == 'LPAREN' and tokens[i + 1][0] == 'LPAREN':
            # Find matching RPAREN for the INNER LPAREN.
            depth = 0
            inner_end = None
            for j in range(i + 1, n):
                if tokens[j][0] == 'LPAREN':
                    depth += 1
                elif tokens[j][0] == 'RPAREN':
                    depth -= 1
                    if depth == 0:
                        inner_end = j
                        break
            if inner_end is not None and inner_end + 1 < n and tokens[inner_end + 1][0] == 'RPAREN':
                return "Invalid ConditionExpression: The expression has redundant parentheses"
    return None


def _evaluate_condition(expr, item, attr_values, attr_names):
    if not expr or not expr.strip():
        return True
    try:
        tokens = _tokenize(expr)
        err = _check_redundant_parens(tokens)
        if err:
            raise ValueError(err)
        err = _check_reserved_keyword_usage(tokens)
        if err:
            raise ValueError(err)
        return _ExprEval(tokens, item, attr_values, attr_names).evaluate()
    except Exception as e:
        logger.warning("Expression evaluation error: %s for expr: %s", e, expr)
        raise ValueError(f"Invalid expression: {e}")


# ---------------------------------------------------------------------------
# Update expression
# ---------------------------------------------------------------------------

def _apply_update_expression(item, expr, attr_values, attr_names):
    item = copy.deepcopy(item)
    tokens = _tokenize(expr)
    err = _check_redundant_parens(tokens)
    if err:
        raise ValueError(err)
    # Strip SET/REMOVE/ADD/DELETE clause keywords before reserved-word scanning;
    # they're clause introducers, not user attribute names.
    scan_toks = [t for t in tokens if not (t[0] == "IDENT" and t[1].upper() in ("SET", "REMOVE", "ADD", "DELETE"))]
    err = _check_reserved_keyword_usage(scan_toks)
    if err:
        raise ValueError(err)
    clauses = {}
    current_clause = None
    current_tokens = []
    for tok in tokens:
        if tok[0] == 'IDENT' and tok[1].upper() in ('SET', 'REMOVE', 'ADD', 'DELETE'):
            if current_clause is not None:
                clauses[current_clause] = current_tokens
            current_clause = tok[1].upper()
            current_tokens = []
        elif tok[0] != 'EOF':
            current_tokens.append(tok)
    if current_clause is not None:
        clauses[current_clause] = current_tokens

    if 'SET' in clauses:
        _apply_set(item, clauses['SET'], attr_values, attr_names)
    if 'REMOVE' in clauses:
        _apply_remove(item, clauses['REMOVE'], attr_names)
    if 'ADD' in clauses:
        _apply_add(item, clauses['ADD'], attr_values, attr_names)
    if 'DELETE' in clauses:
        _apply_delete(item, clauses['DELETE'], attr_values, attr_names)
    return item


def _apply_set(item, tokens, attr_values, attr_names):
    # AWS semantics: all RHS references resolve against the pre-update snapshot
    # of the item. Resolve every value first, then apply assignments — so
    # `SET a = b, b = :v` sets `a` to the OLD value of `b`.
    pre_snapshot = copy.deepcopy(item)
    pending = []
    for assignment in _split_by_comma(tokens):
        eq_idx = None
        for i, tok in enumerate(assignment):
            if tok[0] == 'EQ':
                eq_idx = i
                break
        if eq_idx is None:
            continue
        path_parts = _parse_path_from_tokens(assignment[:eq_idx], attr_names)
        value = _eval_set_value(assignment[eq_idx + 1:], pre_snapshot, attr_values, attr_names)
        if path_parts and value is not None:
            # AWS rejects SET on a path whose intermediate ancestor doesn't exist.
            if len(path_parts) > 1:
                ancestor = _get_at_path(item, path_parts[:-1])
                if ancestor is None:
                    raise ValueError(
                        f"The document path provided in the update expression is invalid for update: {'.'.join(str(p) for p in path_parts[:-1])}"
                    )
            pending.append((path_parts, value))
    for path_parts, value in pending:
        _set_at_path(item, path_parts, value)


def _eval_set_value(tokens, item, attr_values, attr_names):
    if not tokens:
        return None

    # Strip a single layer of matched outer parens — e.g.
    # `(if_not_exists(#v, :default) - :amt)`. Without this the binary-operator
    # scan below never sees the `-` at depth 0 and silently drops the
    # arithmetic, leaving the attribute at its original value (issue #648).
    # Only strip when the opening paren's matching close is the LAST token,
    # so `(a) + (b)` (two separate groups) isn't accidentally flattened.
    while (len(tokens) >= 2
           and tokens[0][0] == 'LPAREN'
           and tokens[-1][0] == 'RPAREN'
           and _find_matching_paren(tokens, 0) == len(tokens) - 1):
        tokens = tokens[1:-1]
        if not tokens:
            return None

    paren_depth = 0
    for i, tok in enumerate(tokens):
        if tok[0] == 'LPAREN':
            paren_depth += 1
        elif tok[0] == 'RPAREN':
            paren_depth -= 1
        elif paren_depth == 0 and tok[0] in ('PLUS', 'MINUS') and i > 0:
            left = _eval_set_value(tokens[:i], item, attr_values, attr_names)
            right = _eval_set_value(tokens[i + 1:], item, attr_values, attr_names)
            if left and right and "N" in left and "N" in right:
                lv, rv = Decimal(left["N"]), Decimal(right["N"])
                return {"N": str(lv + rv if tok[0] == 'PLUS' else lv - rv)}
            return left

    if len(tokens) >= 2 and tokens[0][0] == 'IDENT' and tokens[1][0] == 'LPAREN':
        fn = tokens[0][1].lower()
        inner_end = _find_matching_paren(tokens, 1)
        if fn == 'if_not_exists' and inner_end is not None:
            inner = tokens[2:inner_end]
            parts = _split_by_comma(inner)
            if len(parts) == 2:
                path = _parse_path_from_tokens(parts[0], attr_names)
                existing = _get_at_path(item, path)
                if existing is not None:
                    return existing
                return _eval_set_value(parts[1], item, attr_values, attr_names)
        if fn == 'list_append' and inner_end is not None:
            inner = tokens[2:inner_end]
            parts = _split_by_comma(inner)
            if len(parts) == 2:
                a = _eval_set_value(parts[0], item, attr_values, attr_names)
                b = _eval_set_value(parts[1], item, attr_values, attr_names)
                al = a.get("L", []) if isinstance(a, dict) else []
                bl = b.get("L", []) if isinstance(b, dict) else []
                return {"L": al + bl}

    if len(tokens) == 1:
        tok = tokens[0]
        if tok[0] == 'VALUE_REF':
            return attr_values.get(tok[1])

    path = _parse_path_from_tokens(tokens, attr_names)
    if path:
        val = _get_at_path(item, path)
        if val is not None:
            return val

    if len(tokens) == 1 and tokens[0][0] == 'VALUE_REF':
        return attr_values.get(tokens[0][1])

    return None


def _apply_remove(item, tokens, attr_names):
    for path_tokens in _split_by_comma(tokens):
        path = _parse_path_from_tokens(path_tokens, attr_names)
        if path:
            _remove_at_path(item, path)


def _apply_add(item, tokens, attr_values, attr_names):
    for part in _split_by_comma(tokens):
        val_idx = None
        for i in range(len(part) - 1, -1, -1):
            if part[i][0] == 'VALUE_REF':
                val_idx = i
                break
        if val_idx is None:
            continue
        path = _parse_path_from_tokens(part[:val_idx], attr_names)
        add_val = attr_values.get(part[val_idx][1])
        if not path or add_val is None:
            continue

        existing = _get_at_path(item, path)

        if "N" in add_val:
            inc = Decimal(add_val["N"])
            cur = Decimal(existing["N"]) if existing and "N" in existing else Decimal(0)
            _set_at_path(item, path, {"N": str(cur + inc)})
        elif "SS" in add_val:
            cur = set(existing["SS"]) if existing and "SS" in existing else set()
            _set_at_path(item, path, {"SS": sorted(cur | set(add_val["SS"]))})
        elif "NS" in add_val:
            cur = set(existing["NS"]) if existing and "NS" in existing else set()
            _set_at_path(item, path, {"NS": sorted(cur | set(add_val["NS"]))})
        elif "BS" in add_val:
            cur = set(existing["BS"]) if existing and "BS" in existing else set()
            _set_at_path(item, path, {"BS": sorted(cur | set(add_val["BS"]))})


def _apply_delete(item, tokens, attr_values, attr_names):
    for part in _split_by_comma(tokens):
        val_idx = None
        for i in range(len(part) - 1, -1, -1):
            if part[i][0] == 'VALUE_REF':
                val_idx = i
                break
        if val_idx is None:
            continue
        path = _parse_path_from_tokens(part[:val_idx], attr_names)
        del_val = attr_values.get(part[val_idx][1])
        if not path or del_val is None:
            continue

        existing = _get_at_path(item, path)
        if existing is None:
            continue

        for set_type in ("SS", "NS", "BS"):
            if set_type in del_val and set_type in existing:
                remaining = [s for s in existing[set_type] if s not in del_val[set_type]]
                if remaining:
                    _set_at_path(item, path, {set_type: remaining})
                else:
                    _remove_at_path(item, path)
                break


# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------

def _split_by_comma(tokens):
    parts = []
    current = []
    depth = 0
    for tok in tokens:
        if tok[0] == 'LPAREN':
            depth += 1;  current.append(tok)
        elif tok[0] == 'RPAREN':
            depth -= 1;  current.append(tok)
        elif tok[0] == 'COMMA' and depth == 0:
            if current:
                parts.append(current)
            current = []
        else:
            current.append(tok)
    if current:
        parts.append(current)
    return parts


def _find_matching_paren(tokens, start):
    depth = 0
    for i in range(start, len(tokens)):
        if tokens[i][0] == 'LPAREN':
            depth += 1
        elif tokens[i][0] == 'RPAREN':
            depth -= 1
            if depth == 0:
                return i
    return None


def _parse_path_from_tokens(tokens, attr_names):
    parts = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok[0] == 'NAME_REF':
            parts.append(attr_names.get(tok[1], tok[1]))
        elif tok[0] == 'IDENT':
            parts.append(tok[1])
        elif tok[0] == 'LBRACKET':
            i += 1
            if i < len(tokens) and tokens[i][0] == 'NUMBER':
                parts.append(int(tokens[i][1]))
                i += 1
        elif tok[0] not in ('DOT', 'RBRACKET'):
            break
        i += 1
    return parts


# ---------------------------------------------------------------------------
# Path operations on DynamoDB-typed items
# ---------------------------------------------------------------------------

def _get_at_path(item, path_parts):
    if not path_parts or not item:
        return None
    current = item.get(path_parts[0])
    for part in path_parts[1:]:
        if current is None:
            return None
        if isinstance(part, int):
            if isinstance(current, dict) and "L" in current:
                lst = current["L"]
                if 0 <= part < len(lst):
                    current = lst[part]
                else:
                    return None
            else:
                return None
        else:
            if isinstance(current, dict) and "M" in current:
                current = current["M"].get(part)
            else:
                return None
    return current


def _set_at_path(item, path_parts, value):
    if not path_parts:
        return
    if len(path_parts) == 1:
        part = path_parts[0]
        if isinstance(part, int):
            if isinstance(item, dict) and "L" in item:
                lst = item["L"]
                while len(lst) <= part:
                    lst.append({"NULL": True})
                lst[part] = value
        else:
            if isinstance(item, dict):
                if "M" in item:
                    item["M"][part] = value
                else:
                    item[part] = value
        return

    first, rest = path_parts[0], path_parts[1:]
    if isinstance(first, int):
        if isinstance(item, dict) and "L" in item:
            lst = item["L"]
            while len(lst) <= first:
                lst.append({"NULL": True})
            child = lst[first]
            if not isinstance(child, dict):
                child = {"M": {}} if isinstance(rest[0], str) else {"L": []}
                lst[first] = child
            _set_at_path(child, rest, value)
    else:
        if isinstance(item, dict):
            container = item.get("M") if "M" in item else item
            if first not in container:
                container[first] = {"L": []} if isinstance(rest[0], int) else {"M": {}}
            _set_at_path(container[first], rest, value)


def _remove_at_path(item, path_parts):
    if not path_parts or not item:
        return
    if len(path_parts) == 1:
        part = path_parts[0]
        if isinstance(part, int):
            if isinstance(item, dict) and "L" in item:
                lst = item["L"]
                if 0 <= part < len(lst):
                    lst.pop(part)
        elif isinstance(item, dict):
            if "M" in item:
                item["M"].pop(part, None)
            else:
                item.pop(part, None)
        return

    first, rest = path_parts[0], path_parts[1:]
    if isinstance(first, int):
        if isinstance(item, dict) and "L" in item and 0 <= first < len(item["L"]):
            _remove_at_path(item["L"][first], rest)
    elif isinstance(item, dict):
        child = item["M"].get(first) if "M" in item else item.get(first)
        if child is not None:
            _remove_at_path(child, rest)


# ---------------------------------------------------------------------------
# DynamoDB value comparison helpers
# ---------------------------------------------------------------------------

def _compare_ddb(left, op, right):
    if left is None or right is None:
        if op == '=':
            return left is None and right is None
        if op == '<>':
            return not (left is None and right is None)
        return False

    lt, lv = _ddb_comparable(left)
    rt, rv = _ddb_comparable(right)

    if lt != rt:
        return op == '<>'

    if op in ('<', '>', '<=', '>=') and lt not in ('S', 'N', 'B'):
        return False

    try:
        if op == '=':  return lv == rv
        if op == '<>': return lv != rv
        if op == '<':  return lv < rv
        if op == '>':  return lv > rv
        if op == '<=': return lv <= rv
        if op == '>=': return lv >= rv
    except TypeError:
        return False
    return False


def _ddb_comparable(val):
    if isinstance(val, dict):
        if "S" in val:
            return ("S", val["S"])
        if "N" in val:
            try:
                return ("N", Decimal(val["N"]))
            except (InvalidOperation, TypeError, ValueError):
                return ("N", Decimal(0))
        if "B" in val:
            # AWS sorts/compares binary values bytewise. Wire form is base64
            # (str) — decode to bytes before comparing so b'\x01' < b'\xff'.
            b = val["B"]
            if isinstance(b, bytes):
                return ("B", b)
            try:
                import base64
                return ("B", base64.b64decode(b))
            except Exception:
                return ("B", b if isinstance(b, str) else b"")
        if "BOOL" in val:
            return ("BOOL", val["BOOL"])
        if "NULL" in val:
            return ("NULL", None)
        if "SS" in val:
            return ("SS", frozenset(val["SS"]))
        if "NS" in val:
            return ("NS", frozenset(val["NS"]))
        if "BS" in val:
            return ("BS", frozenset(val["BS"]))
    return ("UNKNOWN", None)


def _ddb_equals(a, b):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    ta, va = _ddb_comparable(a)
    tb, vb = _ddb_comparable(b)
    return ta == tb and va == vb


def _ddb_type(val):
    if isinstance(val, dict):
        for t in ("S", "N", "B", "SS", "NS", "BS", "BOOL", "NULL", "L", "M"):
            if t in val:
                return t
    return ""


def _ddb_size(val):
    """AWS size() returns the size of a value per AWS rules:
    - S: number of UTF-16 code units (a surrogate pair counts as 2).
    - B: number of bytes in the decoded binary value.
    - SS/NS/BS/L/M: element count.
    """
    if isinstance(val, dict):
        if "S" in val:
            s = val["S"]
            # Sum: 1 unit for BMP chars, 2 for astral (surrogate pair) chars.
            return sum(2 if ord(c) > 0xFFFF else 1 for c in s)
        if "B" in val:
            b = val["B"]
            if isinstance(b, bytes):
                return len(b)
            try:
                import base64
                return len(base64.b64decode(b))
            except Exception:
                return len(b) if isinstance(b, str) else 0
        if "SS" in val: return len(val["SS"])
        if "NS" in val: return len(val["NS"])
        if "BS" in val: return len(val["BS"])
        if "L" in val:  return len(val["L"])
        if "M" in val:  return len(val["M"])
    return 0


# ---------------------------------------------------------------------------
# Key / index helpers
# ---------------------------------------------------------------------------

def _extract_key_val(attr):
    if not attr:
        return ""
    if isinstance(attr, dict):
        if "S" in attr: return attr["S"]
        if "N" in attr: return attr["N"]
        if "B" in attr: return attr["B"]
    return str(attr)


def _resolve_table_key_values(table, attrs, allow_extra):
    attrs = attrs if isinstance(attrs, dict) else {}
    expected_names = {table["pk_name"]}
    if table["sk_name"]:
        expected_names.add(table["sk_name"])
    if not allow_extra and set(attrs.keys()) != expected_names:
        return "", "", _key_schema_validation_error()
    for key_name in expected_names:
        if key_name not in attrs:
            return "", "", _key_schema_validation_error()
        expected_type = _get_attr_type(table, key_name)
        raw_value = attrs.get(key_name)
        if not isinstance(raw_value, dict) or set(raw_value.keys()) != {expected_type}:
            return "", "", _key_schema_validation_error()
    pk_val = _extract_key_val(attrs.get(table["pk_name"]))
    sk_val = _extract_key_val(attrs.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"
    return pk_val, sk_val, None


def _key_schema_validation_error():
    return error_response_json("ValidationException", "The provided key element does not match the schema", 400)


def _resolve_index_keys(table, index_name):
    if not index_name:
        return table["pk_name"], table["sk_name"], False
    for gsi in table.get("GlobalSecondaryIndexes", []):
        if gsi["IndexName"] == index_name:
            pk = sk = None
            for ks in gsi["KeySchema"]:
                if ks["KeyType"] == "HASH":  pk = ks["AttributeName"]
                elif ks["KeyType"] == "RANGE": sk = ks["AttributeName"]
            return pk, sk, True
    for lsi in table.get("LocalSecondaryIndexes", []):
        if lsi["IndexName"] == index_name:
            pk = sk = None
            for ks in lsi["KeySchema"]:
                if ks["KeyType"] == "HASH":  pk = ks["AttributeName"]
                elif ks["KeyType"] == "RANGE": sk = ks["AttributeName"]
            return pk, sk, False
    return table["pk_name"], table["sk_name"], False


def _get_attr_type(table, attr_name):
    for ad in table.get("AttributeDefinitions", []):
        if ad["AttributeName"] == attr_name:
            return ad["AttributeType"]
    return "S"


def _sort_key_value(attr, sk_type):
    if attr is None:
        if sk_type == "N":
            return Decimal(0)
        if sk_type == "B":
            return b""
        return ""
    val = _extract_key_val(attr)
    if sk_type == "N":
        try:
            return Decimal(val)
        except (InvalidOperation, TypeError, ValueError):
            return Decimal(0)
    if sk_type == "B":
        # Bytewise ordering matches real DynamoDB. Wire form is base64.
        if isinstance(val, bytes):
            return val
        try:
            import base64
            return base64.b64decode(val)
        except Exception:
            return b""
    return val


def _extract_pk_from_condition(condition, attr_values, attr_names, pk_name):
    if not condition:
        return None
    pk_refs = [pk_name]
    for alias, real in attr_names.items():
        if real == pk_name:
            pk_refs.append(alias)
    for ref in pk_refs:
        m = re.search(rf'(?:^|[\s(]){re.escape(ref)}\s*=\s*(:\w+)', condition)
        if m and m.group(1) in attr_values:
            return _extract_key_val(attr_values[m.group(1)])
        m = re.search(rf'(:\w+)\s*=\s*{re.escape(ref)}(?:$|[\s)])', condition)
        if m and m.group(1) in attr_values:
            return _extract_key_val(attr_values[m.group(1)])
    return None


# ---------------------------------------------------------------------------
# Pagination helpers
# ---------------------------------------------------------------------------

def _index_order_value(item, name, type_hint):
    """Single position in the GSI/LSI ordering tuple. Hash-only items, sparse
    GSIs, or items missing a key get a stable filler so tuples remain
    comparable across the candidate set."""
    if not name or name not in item:
        return Decimal(0) if type_hint == "N" else ""
    return _sort_key_value(item.get(name), type_hint)


def _index_order_keys(table, sk_name):
    """Ordered list of (attr_name, attr_type) used to sort a GSI/LSI Query
    result deterministically: (INDEX_SORT, BASE_PK, BASE_SK). Real DynamoDB
    orders by (INDEX_HASH, INDEX_SORT, BASE_PK, BASE_SK); INDEX_HASH is fixed
    per Query so it drops out of the ordering. The base-table keys break ties
    when multiple items share the same INDEX_SORT value (or when the GSI is
    hash-only)."""
    seen = set()
    keys = []
    for n in (sk_name, table.get("pk_name"), table.get("sk_name")):
        if n and n not in seen:
            seen.add(n)
            keys.append((n, _get_attr_type(table, n)))
    return keys


def _apply_exclusive_start_key(candidates, esk, pk_name, sk_name, scan_forward=True, table=None):
    """Skip past the ESK cursor item, breaking ties on the GSI sort key with
    the base table's primary key — same hidden ordering real DynamoDB uses."""
    if not esk or not candidates:
        return candidates
    # Hash-only base-table query — candidates are uniquely keyed by pk, so a
    # cursor at all simply means "we already returned this one item."
    if table is None and not sk_name:
        start_pk = _extract_key_val(esk.get(pk_name, {}))
        found = False
        result = []
        for item in candidates:
            if found:
                result.append(item)
            elif _extract_key_val(item.get(pk_name, {})) == start_pk:
                found = True
        return result
    keys = _index_order_keys(table, sk_name) if table is not None else []
    if not keys:
        # Fall back to the legacy single-key compare for callers that didn't
        # pass `table` (no GSI tiebreak available).
        if not sk_name or sk_name not in esk:
            return candidates
        start_sk = esk[sk_name]
        result = []
        for item in candidates:
            if item.get(sk_name) is None:
                continue
            op = '>' if scan_forward else '<'
            if _compare_ddb(item.get(sk_name), op, start_sk):
                result.append(item)
        return result
    cursor = tuple(_index_order_value(esk, n, t) for n, t in keys)
    result = []
    for item in candidates:
        item_tuple = tuple(_index_order_value(item, n, t) for n, t in keys)
        if scan_forward:
            if item_tuple > cursor:
                result.append(item)
        else:
            if item_tuple < cursor:
                result.append(item)
    return result


def _apply_exclusive_start_key_scan(all_items, esk, table):
    pk_name = table["pk_name"]
    sk_name = table["sk_name"]
    start_pk = _extract_key_val(esk.get(pk_name, {}))
    start_sk = _extract_key_val(esk.get(sk_name, {})) if sk_name and sk_name in esk else ""
    result = []
    for item in all_items:
        item_pk = _extract_key_val(item.get(pk_name, {}))
        item_sk = _extract_key_val(item.get(sk_name, {})) if sk_name and sk_name in item else ""
        if (item_pk, item_sk) > (start_pk, start_sk):
            result.append(item)
    return result


def _build_key(item, pk_name, sk_name):
    key = {}
    if pk_name and pk_name in item:
        key[pk_name] = item[pk_name]
    if sk_name and sk_name in item:
        key[sk_name] = item[sk_name]
    return key


# ---------------------------------------------------------------------------
# Projection helpers
# ---------------------------------------------------------------------------

def _apply_projection(item, data):
    proj = data.get("ProjectionExpression")
    ean = data.get("ExpressionAttributeNames", {})
    if proj:
        return _project_item(item, proj, ean)
    atg = data.get("AttributesToGet")
    if atg:
        # Legacy AttributesToGet: return ONLY the specified attributes, do NOT
        # auto-include key attributes (matches real AWS behavior).
        return {k: item[k] for k in atg if k in item}
    return item


def _parse_projection_path(path: str, attr_names: dict) -> list:
    """Parse a ProjectionExpression path segment list.

    Examples:
        "a"            -> [("name","a")]
        "a.b"          -> [("name","a"),("name","b")]
        "a[0]"         -> [("name","a"),("index",0)]
        "#root.sub[2]" -> [("name","ROOT"),("name","sub"),("index",2)] (after EAN sub)
    """
    parts = []
    i = 0
    s = path.strip()
    while i < len(s):
        if s[i] == ".":
            i += 1
            continue
        if s[i] == "[":
            j = s.index("]", i)
            parts.append(("index", int(s[i + 1:j])))
            i = j + 1
            continue
        # Read a name token up to next . or [
        j = i
        while j < len(s) and s[j] not in (".", "["):
            j += 1
        token = s[i:j]
        if token.startswith("#"):
            token = attr_names.get(token, token)
        parts.append(("name", token))
        i = j
    return parts


def _project_one(node, path_parts):
    """Walk `path_parts` into a DynamoDB AttributeValue `node` and return a
    pruned copy that contains only the path, or None if the path doesn't
    exist."""
    if not path_parts:
        return node
    if not isinstance(node, dict) or len(node) != 1:
        return None
    (vtype, vval), = node.items()
    kind, key = path_parts[0]
    rest = path_parts[1:]
    if kind == "name":
        if vtype != "M" or not isinstance(vval, dict) or key not in vval:
            return None
        sub = _project_one(vval[key], rest)
        if sub is None:
            return None
        return {"M": {key: sub}}
    if kind == "index":
        if vtype != "L" or not isinstance(vval, list):
            return None
        if key < 0 or key >= len(vval):
            return None
        sub = _project_one(vval[key], rest)
        if sub is None:
            return None
        return {"L": [sub]}
    return None


def _merge_projection(into: dict | None, add: dict | None) -> dict | None:
    """Merge two projection-result AttributeValues at the same level.
    Both must share the same wrapping type (M or L); merging M unions keys,
    merging L unions indices (kept in original order)."""
    if into is None:
        return add
    if add is None:
        return into
    if not isinstance(into, dict) or not isinstance(add, dict):
        return into
    if len(into) != 1 or len(add) != 1:
        return into
    (it_t, it_v), = into.items()
    (ad_t, ad_v), = add.items()
    if it_t != ad_t:
        return into
    if it_t == "M" and isinstance(it_v, dict) and isinstance(ad_v, dict):
        merged = dict(it_v)
        for k, v in ad_v.items():
            merged[k] = _merge_projection(merged.get(k), v) if k in merged else v
        return {"M": merged}
    if it_t == "L" and isinstance(it_v, list) and isinstance(ad_v, list):
        # When two paths reference the same list, AWS preserves the union of
        # indices in original order.
        return {"L": it_v + [x for x in ad_v if x not in it_v]}
    return into


def _project_item(item, proj_expr, attr_names):
    """Apply a ProjectionExpression to an item, supporting nested map paths
    and list indexes. Multiple paths under the same root attribute are merged."""
    if not proj_expr:
        return item
    # AWS-keyword check on each unaliased root identifier.
    paths = [a.strip() for a in proj_expr.split(",") if a.strip()]
    for path in paths:
        head = path.split(".")[0].split("[")[0].strip()
        if head and not head.startswith("#") and head.upper() in AWS_KEYWORDS:
            raise ValueError(f"Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: {head}")
    result: dict = {}
    for path in paths:
        parts = _parse_projection_path(path, attr_names or {})
        if not parts:
            continue
        root_kind, root_name = parts[0]
        if root_kind != "name":
            continue
        if root_name not in item:
            continue
        sub = _project_one(item[root_name], parts[1:])
        if sub is None:
            continue
        if root_name in result:
            result[root_name] = _merge_projection(result[root_name], sub)
        else:
            result[root_name] = sub
    return result


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def _update_counts(table):
    count = sum(len(v) for v in table["items"].values())
    table["ItemCount"] = count
    table["TableSizeBytes"] = count * 200


def _check_legacy_comparison(item_val, op, attr_vals):
    """Evaluate a single legacy ComparisonOperator against an item attribute.

    Supports all DynamoDB legacy comparison operators:
    EQ, NE, LE, LT, GE, GT, NOT_NULL, NULL, CONTAINS, NOT_CONTAINS,
    BEGINS_WITH, IN, BETWEEN.

    Uses the type-aware _compare_ddb / _ddb_comparable helpers so that numeric
    comparisons work correctly (e.g. ``{"N":"10"} > {"N":"2"}``).
    """
    if op == "NOT_NULL":
        return item_val is not None
    if op == "NULL":
        return item_val is None
    if op == "EQ":
        return item_val is not None and _ddb_equals(item_val, attr_vals[0])
    if op == "NE":
        return item_val is None or not _ddb_equals(item_val, attr_vals[0])
    if op in ("LE", "LT", "GE", "GT"):
        sym = {"LE": "<=", "LT": "<", "GE": ">=", "GT": ">"}[op]
        return item_val is not None and _compare_ddb(item_val, sym, attr_vals[0])
    if op == "BETWEEN":
        return (item_val is not None
                and _compare_ddb(item_val, ">=", attr_vals[0])
                and _compare_ddb(item_val, "<=", attr_vals[1]))
    if op == "IN":
        return item_val is not None and any(_ddb_equals(item_val, v) for v in attr_vals)
    if op == "BEGINS_WITH":
        if item_val is None:
            return False
        val = _extract_key_val(item_val)
        target = _extract_key_val(attr_vals[0]) if attr_vals else ""
        return str(val).startswith(str(target))
    if op == "CONTAINS":
        if item_val is None:
            return False
        # For sets (SS/NS/BS), check membership; for S/B, check substring.
        item_type = _ddb_type(item_val)
        if item_type in ("SS", "NS", "BS"):
            target_val = _extract_key_val(attr_vals[0]) if attr_vals else ""
            return target_val in item_val[item_type]
        if item_type == "L":
            return any(_ddb_equals(el, attr_vals[0]) for el in item_val["L"])
        val = _extract_key_val(item_val)
        target = _extract_key_val(attr_vals[0]) if attr_vals else ""
        return str(target) in str(val)
    if op == "NOT_CONTAINS":
        if item_val is None:
            return True
        item_type = _ddb_type(item_val)
        if item_type in ("SS", "NS", "BS"):
            target_val = _extract_key_val(attr_vals[0]) if attr_vals else ""
            return target_val not in item_val[item_type]
        if item_type == "L":
            return not any(_ddb_equals(el, attr_vals[0]) for el in item_val["L"])
        val = _extract_key_val(item_val)
        target = _extract_key_val(attr_vals[0]) if attr_vals else ""
        return str(target) not in str(val)
    return True


def _evaluate_legacy_filter(item, scan_filter):
    """Evaluate legacy ScanFilter/QueryFilter conditions (implicit AND)."""
    for attr_name, condition in scan_filter.items():
        op = condition.get("ComparisonOperator", "")
        attr_vals = condition.get("AttributeValueList", [])
        if not _check_legacy_comparison(item.get(attr_name), op, attr_vals):
            return False
    return True


def _evaluate_expected(item, expected, conditional_operator="AND"):
    """Evaluate legacy ``Expected`` conditions on an item.

    Each key in *expected* is an attribute name.  The value is one of:

    1. ``{"ComparisonOperator": "...", "AttributeValueList": [...]}``
       – full comparison form.
    2. ``{"Exists": true/false}``
       – shorthand for NOT_NULL / NULL.
    3. ``{"Value": <attr>}``
       – shorthand for ``{"ComparisonOperator": "EQ", "AttributeValueList": [<attr>]}``.
    4. ``{"Exists": false}``
       – attribute must *not* exist.

    *conditional_operator* is ``"AND"`` (default) or ``"OR"``.
    """
    results = []
    for attr_name, cond in expected.items():
        item_val = item.get(attr_name)

        # Shorthand: Exists / Value (cannot coexist with ComparisonOperator)
        if "ComparisonOperator" not in cond:
            if "Exists" in cond:
                if cond["Exists"]:
                    results.append(item_val is not None)
                else:
                    results.append(item_val is None)
                continue
            if "Value" in cond:
                results.append(item_val is not None and _ddb_equals(item_val, cond["Value"]))
                continue
            # If neither — treat as attribute must exist (AWS default)
            results.append(item_val is not None)
            continue

        op = cond["ComparisonOperator"]
        attr_vals = cond.get("AttributeValueList", [])
        results.append(_check_legacy_comparison(item_val, op, attr_vals))

    if conditional_operator == "OR":
        return any(results) if results else True
    return all(results)


_SET_TYPES = ("SS", "NS", "BS")


class _AttributeUpdatesValidationError(Exception):
    """Raised when AttributeUpdates contains an invalid action/type combination.

    Real DynamoDB rejects DELETE-with-Value where the existing attribute is not a
    set, and ADD where the existing attribute (or the supplied value) is not a
    Number or set type. The caller translates this to a ValidationException.
    """


def _apply_attribute_updates(item, attribute_updates):
    """Apply legacy ``AttributeUpdates`` to an item.

    Each key is an attribute name.  The value has ``Action`` (``PUT``,
    ``DELETE``, or ``ADD``; default ``PUT``) and optionally ``Value``
    (a DynamoDB-typed attribute value).
    """
    item = copy.deepcopy(item)
    for attr_name, update in attribute_updates.items():
        action = update.get("Action", "PUT")
        value = update.get("Value")

        if action == "PUT":
            if value is not None:
                item[attr_name] = value
        elif action == "DELETE":
            if value is None:
                # No value → remove the attribute entirely
                item.pop(attr_name, None)
            else:
                value_set_type = next((t for t in _SET_TYPES if t in value), None)
                if value_set_type is None:
                    raise _AttributeUpdatesValidationError(
                        "One or more parameter values were invalid: "
                        "Action DELETE is not supported for the type of value "
                        f"provided for attribute {attr_name}"
                    )
                existing = item.get(attr_name)
                if existing is None:
                    continue
                if value_set_type not in existing:
                    raise _AttributeUpdatesValidationError(
                        "One or more parameter values were invalid: "
                        f"Type mismatch for attribute {attr_name}"
                    )
                remaining = [v for v in existing[value_set_type] if v not in set(value[value_set_type])]
                if remaining:
                    item[attr_name] = {value_set_type: remaining}
                else:
                    item.pop(attr_name, None)
        elif action == "ADD":
            if value is None:
                continue
            existing = item.get(attr_name)
            value_type = next(iter(value.keys()), None)
            if value_type not in ("N",) + _SET_TYPES:
                raise _AttributeUpdatesValidationError(
                    "One or more parameter values were invalid: "
                    "Action ADD is only supported for Number and set types "
                    f"for attribute {attr_name}"
                )
            if existing is not None and value_type not in existing:
                raise _AttributeUpdatesValidationError(
                    "One or more parameter values were invalid: "
                    f"Type mismatch for attribute {attr_name}"
                )
            if value_type == "N":
                inc = Decimal(value["N"])
                cur = Decimal(existing["N"]) if existing and "N" in existing else Decimal(0)
                item[attr_name] = {"N": str(cur + inc)}
            else:
                cur = set(existing[value_type]) if existing and value_type in existing else set()
                item[attr_name] = {value_type: sorted(cur | set(value[value_type]))}
    return item


def _extract_pk_from_key_conditions(key_conditions, pk_name):
    """Extract the partition key value from a legacy ``KeyConditions`` map.

    The partition key entry must use ``EQ`` with exactly one value.
    Returns the extracted string/number value, or ``None`` if not found.
    """
    pk_cond = key_conditions.get(pk_name)
    if not pk_cond:
        return None
    op = pk_cond.get("ComparisonOperator", "")
    attr_vals = pk_cond.get("AttributeValueList", [])
    if op != "EQ" or len(attr_vals) != 1:
        return None
    return _extract_key_val(attr_vals[0])


def _evaluate_key_conditions_item(item, key_conditions, pk_name):
    """Check whether *item* satisfies all ``KeyConditions`` entries.

    The partition key is always checked via EQ.  The sort key (if present)
    supports: EQ, LE, LT, GE, GT, BEGINS_WITH, BETWEEN.
    """
    for attr_name, cond in key_conditions.items():
        op = cond.get("ComparisonOperator", "")
        attr_vals = cond.get("AttributeValueList", [])
        if not _check_legacy_comparison(item.get(attr_name), op, attr_vals):
            return False
    return True


def _add_consumed_capacity(result, data, table_name, write=False):
    rc = data.get("ReturnConsumedCapacity", "NONE")
    if rc == "NONE":
        return
    table = _tables.get(table_name, {})
    gsi_count = len(table.get("GlobalSecondaryIndexes", [])) if write else 0
    units = 1.0 + gsi_count
    cap = {"TableName": table_name, "CapacityUnits": units}
    if rc == "INDEXES":
        cap["Table"] = {"CapacityUnits": 1.0}
        if write and gsi_count:
            cap["GlobalSecondaryIndexes"] = {
                gsi["IndexName"]: {"CapacityUnits": 1.0}
                for gsi in table.get("GlobalSecondaryIndexes", [])
            }
    result["ConsumedCapacity"] = cap


def _get_item_by_key(table, key):
    pk_val = _extract_key_val(key.get(table["pk_name"]))
    sk_val = _extract_key_val(key.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"
    return table["items"].get(pk_val, {}).get(sk_val)


def _extract_key_from_item(table, item):
    key = {}
    if table["pk_name"] in item:
        key[table["pk_name"]] = item[table["pk_name"]]
    if table["sk_name"] and table["sk_name"] in item:
        key[table["sk_name"]] = item[table["sk_name"]]
    return key


def _diff_attributes(old_item, new_item, return_old=True):
    """Return only the attributes that changed.

    - UPDATED_OLD: report the prior value of each attribute that was modified
      or removed (omit additions, since there is no prior value).
    - UPDATED_NEW: report the new value of each attribute that was created or
      modified (omit removals, since there is no new value to report — per
      AWS, REMOVE-only updates with UPDATED_NEW omit Attributes entirely).
    """
    result = {}
    all_keys = set(list(old_item.keys()) + list(new_item.keys()))
    for k in all_keys:
        ov = old_item.get(k)
        nv = new_item.get(k)
        if ov == nv:
            continue
        if return_old:
            if ov is not None:
                result[k] = ov
        else:
            if nv is not None:
                result[k] = nv
    return result


def reset():
    with _lock:
        _tables.clear()
        _tags.clear()
        _ttl_settings.clear()
        _pitr_settings.clear()
        _stream_records.clear()
        _kinesis_destinations.clear()
