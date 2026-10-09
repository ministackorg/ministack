# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
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

import base64
import binascii
import copy
import csv
import gzip
import io
import json
import logging
import math
import os
import re
import struct
import threading
import time
from collections import defaultdict
from decimal import Decimal, InvalidOperation

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import esm_wake, spawn_background
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_iso,
    request_scope,
)
from ministack.services._dynamodb_keywords import AWS_KEYWORDS

logger = logging.getLogger("dynamodb")

_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")


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
_DDB_PARTITION_RE = re.compile(r"^aws(?:-[a-z]+)*$")
_DDB_REGION_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)+-[0-9]+$")
_DDB_ACCOUNT_RE = re.compile(r"^[0-9]{12}$")
# CsvOptions.Delimiter: {"max": 1, "min": 1, "pattern": "[,;:|\\t ]"}.
_CSV_DELIMITER_RE = re.compile(r"[,;:|\t ]")

# Real AWS reports Export/Import as IN_PROGRESS at submit time; the flip to
# COMPLETED happens asynchronously. We simulate that by holding IN_PROGRESS
# on the first DescribeExport/DescribeImport calls within this grace window.
_EXPORT_COMPLETE_AFTER_SEC = float(os.environ.get("MINISTACK_DDB_EXPORT_COMPLETE_AFTER_SEC", "1"))
_IMPORT_COMPLETE_AFTER_SEC = 1.0
# "The log group name is /aws-dynamodb/imports. The error log stream name is
# import-id/error."
_IMPORT_LOG_GROUP = "/aws-dynamodb/imports"
# "DynamoDB Import from Amazon S3 can support up to 50 concurrent import jobs".
_IMPORT_CONCURRENCY_LIMIT = 50


# Region-scoped: DynamoDB tables are region-specific in AWS. Account-only
# keying made name lookups find cross-region tables while ARN ops (which
# validate spec.region == request region) rejected them — a self-contradiction
# (B7). Legacy account-scoped persistence migrates via the table's TableArn.
_tables = AccountRegionScopedDict()
_tags = AccountRegionScopedDict()
_ttl_settings = AccountRegionScopedDict()
_pitr_settings = AccountRegionScopedDict()
# Kinesis streaming destinations — TableName -> list of
# {"StreamArn": str, "DestinationStatus": "ACTIVE"|"DISABLED",
#  "ApproximateCreationDateTimePrecision": "MILLISECOND"|"MICROSECOND"}.
# ACTIVE entries get each _emit_stream_event record fanned out via
# kinesis.put_record_internal; DISABLED entries stay on the describe
# response (matching the ~24 h AWS retention window for readability).
_kinesis_destinations = AccountRegionScopedDict()
# Contributor Insights — key is "TableName" or "TableName/index/IndexName".
# Value: {"ContributorInsightsStatus": "ENABLED"|"DISABLED",
#         "LastUpdateDateTime": int epoch, "ContributorInsightsRuleList": [str, ...]}.
_backups = AccountRegionScopedDict()  # BackupArn -> BackupDescription dict
_contributor_insights = AccountRegionScopedDict()
# Resource-based policies — ResourceArn -> {"Policy": str, "RevisionId": str}.
_resource_policies = AccountRegionScopedDict()
# Export tasks — ExportArn -> ExportDescription dict.
_exports = AccountRegionScopedDict()
# Import tasks — ImportArn -> ImportTableDescription dict.
_imports = AccountRegionScopedDict()
_lock = threading.Lock()


# ── Persistence ────────────────────────────────────────────

def get_state():
    tables = AccountRegionScopedDict()
    for (account_id, region, table_name), table in _tables.all_items():
        tables.set_scoped(account_id, region, table_name, copy.deepcopy(
            {k: v for k, v in table.items() if k != _INDEX_MEMBERS}))
    return {
        "tables": tables,
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


def _table_name_from_metadata_key(key) -> str:
    if not isinstance(key, str):
        return str(key)
    return key.split("/index/", 1)[0]


def _metadata_value_region(value) -> str | None:
    if isinstance(value, str) and value.startswith("arn:"):
        try:
            spec = parse_arn(value)
        except ArnParseError:
            return None
        return spec.region or None
    if isinstance(value, dict):
        for nested in value.values():
            region = _metadata_value_region(nested)
            if region:
                return region
    if isinstance(value, (list, tuple, set)):
        for nested in value:
            region = _metadata_value_region(nested)
            if region:
                return region
    return None


def _legacy_regions_for_table_metadata(account_id: str, key, value=None) -> list[str]:
    table_name = _table_name_from_metadata_key(key)
    regions = sorted({
        region
        for (stored_account_id, region, stored_table_name), _table in _tables.all_items()
        if stored_account_id == account_id and stored_table_name == table_name
    })
    value_region = _metadata_value_region(value)
    if value_region and (not regions or value_region in regions):
        return [value_region]
    if len(regions) == 1:
        return regions
    if regions:
        return regions
    return [get_region()]


def _restore_table_name_metadata(store: AccountRegionScopedDict, data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        store.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, key), value in data._data.items():
            for region in _legacy_regions_for_table_metadata(account_id, key, value):
                store.set_scoped(account_id, region, key, copy.deepcopy(value))
        return
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, _region, original_key = key
                regions = [_region]
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, original_key = key
                regions = _legacy_regions_for_table_metadata(account_id, original_key, value)
            else:
                account_id = get_account_id()
                original_key = key
                regions = _legacy_regions_for_table_metadata(account_id, original_key, value)
            for region in regions:
                store.set_scoped(account_id, region, original_key, copy.deepcopy(value))


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if data:
        _tables.update(data.get("tables", {}))
        # Restore items as defaultdict(dict) — JSON deserializes as plain dict
        for tbl in _tables.all_values():
            if isinstance(tbl.get("items"), dict) and not isinstance(tbl["items"], defaultdict):
                tbl["items"] = defaultdict(dict, tbl["items"])
            # Migrate legacy SSEDescription shape (pre-#411): convert
            # {Enabled, KMSMasterKeyId} → {Status, KMSMasterKeyArn, SSEType}.
            sse = tbl.get("SSEDescription")
            if sse and "Status" not in sse and ("Enabled" in sse or "KMSMasterKeyId" in sse):
                tbl["SSEDescription"] = _sse_description_from_spec(sse)
        _tags.update(data.get("tags", {}))
        _restore_table_name_metadata(_ttl_settings, data.get("ttl_settings", {}))
        _restore_table_name_metadata(_pitr_settings, data.get("pitr_settings", {}))
        _restore_table_name_metadata(_kinesis_destinations, data.get("kinesis_destinations", {}))
        _restore_table_name_metadata(_contributor_insights, data.get("contributor_insights", {}))
        _backups.update(data.get("backups", {}))
        _resource_policies.update(data.get("resource_policies", {}))
        _exports.update(data.get("exports", {}))
        _imports.update(data.get("imports", {}))
        # No worker on this side of the restart; DescribeImport closes it out.
        for _desc in _imports._data.values():
            if isinstance(_desc, dict) and _desc.get("ImportStatus") == "IN_PROGRESS":
                _desc["_orphaned"] = True




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
# Expression size limit (bytes of the expression string).
_DDB_EXPR_MAX_BYTES = 4096
# Key attribute value length limits.
_DDB_KEY_MAX_BYTES = 2048
_DDB_SORT_KEY_MAX_BYTES = 1024
# Document nesting depth limit (32 levels, leaf is at level 32 means 31 nesting levels for containers).
_DDB_MAX_NESTING_DEPTH = 32


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
    # AWS rejects numbers with whitespace or underscores even though Python's
    # Decimal() accepts them (e.g. " 5", "5 ", "1_000").
    if s != s.strip() or '_' in s:
        return None
    raw = s
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


_NESTING_MSG = ("Nesting Levels have exceeded supported limits: "
                "Attributes in the item have nested levels beyond supported limit")


def _nesting_exceeded(value, depth: int = 1) -> bool:
    if depth > _DDB_MAX_NESTING_DEPTH:
        return True
    if not isinstance(value, dict) or len(value) != 1:
        return False
    (vtype, vval), = value.items()
    if vtype == "M" and isinstance(vval, dict):
        return any(_nesting_exceeded(v, depth + 1) for v in vval.values())
    if vtype == "L" and isinstance(vval, list):
        return any(_nesting_exceeded(v, depth + 1) for v in vval)
    return False


def _validate_attribute_value(attr_name: str, value: dict, _depth: int = 1) -> tuple | None:
    """Recursively validate a single attribute value. Returns an error response
    tuple if invalid, or None if OK. May mutate `value` to canonicalize numbers.
    _depth starts at 1 (top-level attribute value); containers increment before
    recursing. AWS enforces a maximum nesting depth of 32 levels.
    """
    if _depth > _DDB_MAX_NESTING_DEPTH:
        return error_response_json("ValidationException", _NESTING_MSG, 400)
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
                "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes", 400)
    elif vtype == "N":
        canon = _ddb_canonicalize_number(vval)
        if canon is None:
            return error_response_json("ValidationException",
                f"The parameter cannot be converted to a numeric value: {vval}", 400)
        value["N"] = canon
    elif vtype == "B":
        # Per AWS docs: "An attribute value can be an empty string or empty
        # binary value if the attribute is not used for a table or index key."
        # The key-specific empty-binary rejection is enforced separately at
        # PutItem / UpdateItem level, so accept empty binary for non-key here.
        if not isinstance(vval, (str, bytes)):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: Binary attribute value type mismatch", 400)
    elif vtype == "BOOL":
        if not isinstance(vval, bool):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: BOOL value must be true or false", 400)
    elif vtype == "NULL":
        if vval is not True:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: Null attribute value types must have the value of true", 400)
    elif vtype == "SS":
        if not isinstance(vval, list) or not vval:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: An string set  may not be empty", 400)
        if len(set(vval)) != len(vval):
            return error_response_json("ValidationException",
                f"One or more parameter values were invalid: Input collection [{', '.join(str(v) for v in vval)}] contains duplicates.", 400)
        for s in vval:
            if not isinstance(s, str):
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
                    f"One or more parameter values were invalid: Input collection [{', '.join(str(v) for v in vval)}] contains duplicates.", 400)
            seen.add(canon)
            new_vals.append(canon)
        value["NS"] = new_vals
    elif vtype == "BS":
        if not isinstance(vval, list) or not vval:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: Binary sets should not be empty", 400)
        seen = set()
        for b in vval:
            # Empty binary members (zero-length) ARE allowed in BS sets — only
            # empty SS members (empty strings) are forbidden. Binary key attributes
            # are forbidden separately; here we only validate the set value.
            key = b if isinstance(b, str) else (b.decode("latin-1") if isinstance(b, bytes) else None)
            if key is None:
                return error_response_json("ValidationException",
                    "One or more parameter values were invalid: Binary set element must be binary type", 400)
            if key in seen:
                return error_response_json("ValidationException",
                    f"One or more parameter values were invalid: Input collection [{', '.join(str(v) for v in vval)}]of type BS contains duplicates.", 400)
            seen.add(key)
    elif vtype == "L":
        if not isinstance(vval, list):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: List must be a list", 400)
        for sub in vval:
            err = _validate_attribute_value(attr_name, sub, _depth + 1)
            if err:
                return err
    elif vtype == "M":
        if not isinstance(vval, dict):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: Map must be a map", 400)
        for k, sub in vval.items():
            err = _validate_attribute_value(attr_name, sub, _depth + 1)
            if err:
                return err
    else:
        return error_response_json("ValidationException",
            "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes", 400)
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
        return _number_size_bytes(vval)
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
        return sum(_number_size_bytes(n) for n in vval)
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


# A Query/Scan page holds at most 1 MB of items read, before any filter.
_DDB_PAGE_SIZE_BYTES = 1_048_576


def _paginate_evaluated_items(items, limit, table, index_name=None):
    """One Query/Scan page: up to ``Limit`` items, stopping before the item
    that would take the data read past 1 MB. Index reads count the index
    entry (its projected attributes). Returns (page, stopped_at_boundary)."""
    page = []
    page_bytes = 0
    for item in items:
        entry = _apply_index_projection(item, table, index_name) if index_name else item
        item_bytes = _item_size_bytes(entry)
        if page and page_bytes + item_bytes > _DDB_PAGE_SIZE_BYTES:
            return page, True
        page.append(item)
        page_bytes += item_bytes
        if (limit is not None and len(page) >= int(limit)) or page_bytes >= _DDB_PAGE_SIZE_BYTES:
            return page, True
    return page, False


def _number_size_bytes(text) -> int:
    """Base-100 digit pairs aligned on the decimal point, plus 1, plus 1 if negative."""
    s = str(text).strip()
    negative = s.startswith("-")
    s = s.lstrip("+-")
    mantissa, _, exp_text = s.lower().partition("e")
    whole, _, fraction = mantissa.partition(".")
    digits = whole + fraction
    if not digits.isdigit() or (exp_text and not exp_text.lstrip("+-").isdigit()):
        return 1
    exponent = (int(exp_text) if exp_text else 0) - len(fraction)
    stripped = digits.lstrip("0").rstrip("0")
    if not stripped:
        return 1
    exponent += len(digits.lstrip("0")) - len(stripped)
    if exponent % 2:
        stripped += "0"
    if len(stripped) % 2:
        stripped = "0" + stripped
    return 1 + len(stripped) // 2 + (1 if negative else 0)


def _item_size_bytes(item: dict) -> int:
    total = 0
    if not isinstance(item, dict):
        return 0
    for name, value in item.items():
        total += len(name.encode("utf-8"))
        total += _attribute_value_size(value)
    return total


_ITEM_SIZE_MSG = "Item size has exceeded the maximum allowed size"
_UPDATE_SIZE_MSG = "Item size to update has exceeded the maximum allowed size"


def _update_statement_size(item: dict, expr: str, attr_names: dict) -> int:
    """UpdateItem's size: the top-level attributes it writes plus 3, 19 per SET/ADD
    action (20 through a list index) and 2 per REMOVE/DELETE action."""
    clauses = []
    for tok in _tokenize(expr):
        if tok[0] == "IDENT" and tok[1].upper() in ("SET", "REMOVE", "ADD", "DELETE"):
            clauses.append((tok[1].upper(), []))
        elif tok[0] != "EOF" and clauses:
            clauses[-1][1].append(tok)
    size = 3
    written = set()
    for clause, tokens in clauses:
        for action in _split_by_comma(tokens):
            path = _parse_path_from_tokens(action, attr_names or {})
            if not path:
                continue
            size += 19 if clause in ("SET", "ADD") else 2
            if clause == "SET" and any(isinstance(p, int) for p in path):
                size += 1
            written.add(path[0])
    return size + sum(len(n.encode("utf-8")) + _attribute_value_size(item[n]) for n in written if n in item)


def _key_length_error(item: dict, pk_name: str | None, sk_name: str | None) -> tuple | None:
    # Key attribute value length limits: partition key max 2048 bytes, sort key max 1024 bytes.
    for key_name, max_bytes in ((pk_name, _DDB_KEY_MAX_BYTES), (sk_name, _DDB_SORT_KEY_MAX_BYTES)):
        if not key_name or key_name not in item:
            continue
        raw = item[key_name]
        if not isinstance(raw, dict) or len(raw) != 1:
            continue
        (ktype, kval), = raw.items()
        key_bytes = 0
        if ktype == "S" and isinstance(kval, str):
            key_bytes = len(kval.encode("utf-8"))
        elif ktype == "B":
            try:
                key_bytes = len(base64.b64decode(kval)) if isinstance(kval, str) else len(kval)
            except Exception:
                key_bytes = len(kval) if isinstance(kval, (str, bytes)) else 0
        if key_bytes > max_bytes:
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: Aggregated size of all range keys has exceeded the size limit of 1024 bytes" if key_name == sk_name else
                "One or more parameter values were invalid: Aggregated size of all hash keys has exceeded the size limit of 2048 bytes", 400)
    return None


def _validate_item(item: dict, pk_name: str | None = None, sk_name: str | None = None,
                   size_msg: str = _ITEM_SIZE_MSG) -> tuple | None:
    """Validate a full item: each attribute, then total size cap. Mutates
    Number values to canonical form."""
    if not isinstance(item, dict):
        return error_response_json("ValidationException",
            "Item must be a structure", 400)
    for name, value in item.items():
        # Empty string/binary not allowed for hash/sort key attributes
        if name in (pk_name, sk_name):
            err = _empty_key_value_error(name, value)
            if err:
                return err
        err = _validate_attribute_value(name, value)
        if err:
            return err
    err = _key_length_error(item, pk_name, sk_name)
    if err:
        return err
    if _item_size_bytes(item) > _DDB_ITEM_MAX_BYTES:
        return error_response_json("ValidationException", size_msg, 400)
    return None

# DynamoDB Streams: table_name -> list of stream records
# Each record follows the DynamoDB Streams event format consumed by Lambda ESMs.
_stream_records = AccountRegionScopedDict()


def _stream_label() -> str:
    """A DynamoDB stream label: ISO 8601 with millisecond precision and NO
    trailing ``Z``, matching the form AWS puts in a stream ARN — e.g.
    ``2015-05-11T21:21:33.291`` (not ``now_iso()``'s ``...291Z``)."""
    return now_iso()[:-1]
# Records dropped off the front of each table's stream, so a consumer's read
# position stays an absolute index into the stream rather than an offset into
# the surviving slice.
_stream_trimmed = AccountRegionScopedDict()
_stream_seq_counter = 0
_stream_seq_lock = threading.Lock()

# AWS DynamoDB Streams keeps records for 24 hours; retention is purely
# time-based, so records are expired by age and by nothing else.
_STREAM_RETENTION_SECONDS = 24 * 60 * 60


def _record_age_cutoff(record: dict, cutoff: float) -> bool:
    created = (record.get("dynamodb") or {}).get("ApproximateCreationDateTime")
    if not created:
        # No creation time to age against (restored or hand-built records) —
        # keep it rather than treating it as infinitely old.
        return False
    return created < cutoff


def _trim_stream_records(table_name: str, *, account_id=None, region=None) -> None:
    """Expire a table's stream records older than the retention window.

    Called from the read paths as well as on write, so a stream that stopped
    receiving records still ages out of its retention window.
    """
    if account_id is None or region is None:
        records = _stream_records.get(table_name)
    else:
        records = _stream_records.get_scoped(account_id, region, table_name)
    if not records:
        return
    cutoff = time.time() - _STREAM_RETENTION_SECONDS
    expired = 0
    while expired < len(records) and _record_age_cutoff(records[expired], cutoff):
        expired += 1
    if not expired:
        return
    del records[:expired]
    if account_id is None or region is None:
        _stream_trimmed[table_name] = _stream_trimmed.get(table_name, 0) + expired
    else:
        _stream_trimmed.set_scoped(
            account_id, region, table_name,
            _stream_trimmed.get_scoped(account_id, region, table_name, 0) + expired,
        )


def _live_stream_records(table_name: str, account_id=None, region=None) -> list[dict]:
    if account_id is None or region is None:
        return _stream_records.get(table_name) or []
    return _stream_records.get_scoped(account_id, region, table_name) or []


def _trimmed_count(table_name: str, account_id=None, region=None) -> int:
    if account_id is None or region is None:
        return _stream_trimmed.get(table_name, 0)
    return _stream_trimmed.get_scoped(account_id, region, table_name, 0)


def stream_start_position(table_name: str, *, account_id=None, region=None) -> int:
    """Oldest position still readable from a table's stream (TRIM_HORIZON)."""
    _trim_stream_records(table_name, account_id=account_id, region=region)
    return _trimmed_count(table_name, account_id, region)


def stream_end_position(table_name: str, *, account_id=None, region=None) -> int:
    """Position just past the newest record in a table's stream (LATEST)."""
    _trim_stream_records(table_name, account_id=account_id, region=region)
    return (
        _trimmed_count(table_name, account_id, region)
        + len(_live_stream_records(table_name, account_id, region))
    )


def stream_records_since(
    table_name: str, position: int, limit: int, *, account_id=None, region=None
) -> list[dict]:
    """Read up to ``limit`` records from an absolute stream position. A position
    behind the trim horizon resumes at the horizon, as an AWS shard iterator
    does once its records expire."""
    _trim_stream_records(table_name, account_id=account_id, region=region)
    records = _live_stream_records(table_name, account_id, region)
    offset = max(0, position - _trimmed_count(table_name, account_id, region))
    return records[offset:offset + limit]


def stream_live_records(table_name: str, *, account_id=None, region=None) -> list[dict]:
    """The table's unexpired records, oldest first. Index ``i`` in this list is
    absolute position ``stream_start_position(...) + i``."""
    _trim_stream_records(table_name, account_id=account_id, region=region)
    return _live_stream_records(table_name, account_id, region)


# Streams closed by disabling them or deleting their table, by stream ARN.
# "If you disable a stream on a table, the data in the stream continues to be
# readable for 24 hours."
_closed_streams = AccountRegionScopedDict()


def _sweep_closed_streams() -> None:
    cutoff = time.time() - _STREAM_RETENTION_SECONDS
    for (account_id, region, arn), closed in _closed_streams.all_items():
        if closed["ClosedAt"] < cutoff:
            _closed_streams.pop_scoped(account_id, region, arn, None)


def closed_stream(stream_arn: str) -> dict | None:
    """The closed stream ``stream_arn`` with its unexpired records, or None."""
    _sweep_closed_streams()
    closed = _closed_streams.get(stream_arn)
    if closed is None:
        return None
    records = closed["records"]
    cutoff = time.time() - _STREAM_RETENTION_SECONDS
    expired = 0
    while expired < len(records) and _record_age_cutoff(records[expired], cutoff):
        expired += 1
    if expired:
        del records[:expired]
        closed["trimmed"] += expired
    return closed


def drop_stream_records(table_name: str, table: dict | None = None) -> None:
    """Detach the table's current stream; an enabled one stays readable as closed."""
    records = _stream_records.pop(table_name, None) or []
    trimmed = _stream_trimmed.pop(table_name, 0)
    spec = (table or {}).get("StreamSpecification") or {}
    arn = (table or {}).get("LatestStreamArn")
    if not spec.get("StreamEnabled") or not arn:
        return
    _sweep_closed_streams()
    _closed_streams[arn] = {
        "TableName": table_name,
        "StreamLabel": table.get("LatestStreamLabel", ""),
        "StreamViewType": spec.get("StreamViewType", "NEW_AND_OLD_IMAGES"),
        "KeySchema": copy.deepcopy(table.get("KeySchema", [])),
        "CreationRequestDateTime": table.get("CreationDateTime", 0),
        "ClosedAt": time.time(),
        "records": records,
        "trimmed": trimmed,
    }


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
        "eventSourceARN": table.get("LatestStreamArn") or f"{table['TableArn']}/stream/{_stream_label()}",
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


def _emit_stream_event(table_name: str, event_name: str, old_item: dict | None, new_item: dict | None,
                       replicate: bool = True):
    """Emit a change to DynamoDB Streams (if enabled) and to any ACTIVE Kinesis
    streaming destinations registered for this table.

    AWS treats DynamoDB Streams and Kinesis streaming destination as
    independent subscriptions — a table can have either, both, or neither.
    Each path is gated independently here; the function name is kept for
    backwards compatibility with existing call sites."""
    table = _tables.get(table_name)
    if not table:
        return
    if replicate:
        _replicate_write(table, event_name, old_item, new_item)

    spec = table.get("StreamSpecification") or {}
    streams_enabled = bool(spec.get("StreamEnabled"))
    has_kinesis = bool(_kinesis_destinations.get(table_name))
    if not streams_enabled and not has_kinesis:
        return

    # A PutItem/UpdateItem that changes no data writes no stream record.
    if streams_enabled and not (event_name == "MODIFY" and old_item == new_item):
        view_type = spec.get("StreamViewType", "NEW_AND_OLD_IMAGES")
        record = _build_change_record(table, event_name, old_item, new_item, view_type)
        if table_name not in _stream_records:
            _stream_records[table_name] = []
        _stream_records[table_name].append(record)
        esm_wake.set()
        _trim_stream_records(table_name)

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
                for (account_id, region, table_name), setting in list(_ttl_settings.all_items()):
                    if setting.get("TimeToLiveStatus") != "ENABLED":
                        continue
                    attr = setting.get("AttributeName", "")
                    if not attr:
                        continue
                    table = _tables.get_scoped(account_id, region, table_name)
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
                                    _remove_item(table, pk_val, sk_val)
                                    logger.debug("TTL expired item %s/%s from %s", pk_val, sk_val, table_name)
                            except (ValueError, TypeError):
                                pass
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
        "SearchVectors": _search_vectors,
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


def _validate_data_plane_table_name(name) -> tuple | None:
    """Used by PutItem/GetItem/DeleteItem/UpdateItem/Query/Scan: returns a
    ValidationException for null, empty, too-long, or pattern-violating
    table names — BEFORE any other validators fire so the conformance
    'reports only tableName' tests match. Data-plane length range is 1..255
    (the 3-char minimum is a control-plane rule)."""
    if name is None:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    if not isinstance(name, str):
        return error_response_json("ValidationException",
            "1 validation error detected: Value at 'tableName' failed to satisfy constraint: Member must be a string", 400)
    if len(name) < 1:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{name}' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    if name.startswith("arn:"):
        return None  # A table ARN not in this account and region answers ResourceNotFoundException.
    if len(name) > 255:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{name}' at 'tableName' failed to satisfy constraint: Member must have length less than or equal to 255", 400)
    if not re.match(r"^[A-Za-z0-9_.\-]+$", name):
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{name}' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", 400)
    return None


def _multi_validation_error(failures: list[tuple]) -> tuple | None:
    """AWS returns ONE ValidationException with all simultaneously-invalid
    parameters concatenated:
        '2 validation errors detected: Value ... at 'p1' ...; Value ... at 'p2' ...'
    `failures` is a list of (value, param_path, constraint) tuples.
    """
    if not failures:
        return None
    pieces = [
        f"Value '{val}' at '{path}' failed to satisfy constraint: {constraint}"
        for val, path, constraint in failures
    ]
    return error_response_json("ValidationException",
        f"{len(pieces)} validation error{'s' if len(pieces) > 1 else ''} detected: " + "; ".join(pieces), 400)


def _check_per_op_param_enums(data: dict, rv_allowed: set | None, first_only: bool = False) -> tuple | None:
    """Collect every enum-style validation error in one envelope so the
    'reports X and Y together' conformance tests match AWS exactly."""
    failures: list[tuple] = []
    rv = data.get("ReturnValues")
    if rv is not None and rv_allowed is not None and rv not in rv_allowed:
        failures.append((rv, "returnValues", f"Member must satisfy enum value set: {sorted(rv_allowed)}"))
    rcc = data.get("ReturnConsumedCapacity")
    if rcc is not None and rcc not in _RETURN_CONSUMED_CAPACITY_VALUES:
        failures.append((rcc, "returnConsumedCapacity", "Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"))
    ricm = data.get("ReturnItemCollectionMetrics")
    if ricm is not None and ricm not in _RETURN_ITEM_COLLECTION_METRICS:
        failures.append((ricm, "returnItemCollectionMetrics", "Member must satisfy enum value set: [NONE, SIZE]"))
    return _multi_validation_error(failures[:1] if first_only else failures)


_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_INDEX_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_VALID_ATTR_TYPES = {"S", "N", "B"}
_VALID_KEY_TYPES = {"HASH", "RANGE"}
_VALID_BILLING_MODES = {"PROVISIONED", "PAY_PER_REQUEST"}
_VALID_TABLE_CLASSES = {"STANDARD", "STANDARD_INFREQUENT_ACCESS"}
_VALID_PROJECTION_TYPES = {"ALL", "KEYS_ONLY", "INCLUDE"}


def _validate_table_name(name: str) -> tuple | None:
    """Per AWS DynamoDB Developer Guide: 3-255 chars, pattern [A-Za-z0-9_.-].
    Used by CreateTable; data-plane uses a wider 1..255 range via
    `_validate_data_plane_table_name`."""
    if not isinstance(name, str) or not name:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null", 400)
    if len(name) < 3:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{name}' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 3", 400)
    if len(name) > 255:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{name}' at 'tableName' failed to satisfy constraint: Member must have length less than or equal to 255", 400)
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
        # AWS dumps the key_schema as a Java-toString list of
        # `KeySchemaElement(attributeName=…, keyType=…)` entries.
        if isinstance(key_schema, list):
            _dump = "[" + ", ".join(
                f"KeySchemaElement(attributeName={(ks or {}).get('AttributeName', '')}, keyType={(ks or {}).get('KeyType', '')})"
                for ks in key_schema
            ) + "]"
        else:
            _dump = str(key_schema)
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{_dump}' at 'keySchema' failed to satisfy constraint: Member must have length less than or equal to 2", 400)
    hash_count = 0
    range_count = 0
    seen_names = set()
    for idx, ks in enumerate(key_schema, start=1):
        attr = ks.get("AttributeName")
        kt = ks.get("KeyType")
        if not attr:
            return error_response_json("ValidationException",
                "1 validation error detected: Value null at 'keySchema.member.attributeName' failed to satisfy constraint: Member must not be null", 400)
        if kt not in _VALID_KEY_TYPES:
            return error_response_json("ValidationException",
                f"1 validation error detected: Value '{kt}' at 'keySchema.{idx}.member.keyType' failed to satisfy constraint: Member must satisfy enum value set: [HASH, RANGE]", 400)
        if attr in seen_names:
            return error_response_json("ValidationException",
                "Invalid KeySchema: Some index key attribute have no definition", 400)
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
    # AWS distinguishes absent-TableName (parameter required) from null/empty
    # TableName (length / pattern violations). Match both message classes.
    if "TableName" not in data:
        return error_response_json("ValidationException",
            "The parameter 'TableName' is required but was not present in the request", 400)
    name = data.get("TableName")
    err = _validate_table_name(name)
    if err:
        return err
    vector_indexes = data.get("VectorIndexes") or []
    for position, vix in enumerate(vector_indexes, start=1):
        err = _vector_index_request_error(vix, f"vectorIndexes.{position}.member")
        if err:
            return err
    if name in _tables:
        return error_response_json("ResourceInUseException", f"Table already exists: {name}", 400)

    key_schema = data.get("KeySchema")
    attr_defs = data.get("AttributeDefinitions") or []
    # Validate AttributeDefinitions structure.
    seen_attr_names = set()
    for idx, ad in enumerate(attr_defs, start=1):
        an = ad.get("AttributeName")
        at = ad.get("AttributeType")
        if not an:
            return error_response_json("ValidationException",
                "1 validation error detected: AttributeDefinitions element missing AttributeName", 400)
        if at not in _VALID_ATTR_TYPES:
            return error_response_json("ValidationException",
                f"1 validation error detected: Value '{at}' at 'attributeDefinitions.{idx}.member.attributeType' failed to satisfy constraint: Member must satisfy enum value set: [B, N, S]", 400)
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
            f"1 validation error detected: Value '{billing_mode}' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]", 400)
    # TableClass validation.
    table_class = data.get("TableClass")
    if table_class is not None and table_class not in _VALID_TABLE_CLASSES:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{table_class}' at 'tableClass' failed to satisfy constraint: Member must satisfy enum value set: [STANDARD, STANDARD_INFREQUENT_ACCESS]", 400)
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
            "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST", 400)

    gsis = copy.deepcopy(data.get("GlobalSecondaryIndexes", []))
    lsis = copy.deepcopy(data.get("LocalSecondaryIndexes", []))

    # LSI validation: requires the base table to have a RANGE key, and each LSI
    # must have the same HASH key as the base table.
    if lsis and not sk_name:
        # AWS-canonical (dynamodb-conformance.org capture): the
        # documented LSI rejection on a hash-only table is the
        # generic "One or more parameter values were invalid:" prefix.
        return error_response_json("ValidationException",
            "One or more parameter values were invalid: Table KeySchema does not have a range key, which is required when specifying a LocalSecondaryIndex", 400)
    for lsi in lsis:
        lks = lsi.get("KeySchema") or []
        if not any(k.get("KeyType") == "HASH" and k.get("AttributeName") == pk_name for k in lks):
            return error_response_json("ValidationException",
                f"Local Secondary Index '{lsi.get('IndexName')}' must use the table's hash key", 400)

    # Duplicate-index-name detection across LSI + GSI.
    seen_index_names = set()
    for idx in lsis + gsis + vector_indexes:
        iname = idx.get("IndexName")
        if iname and iname in seen_index_names:
            return error_response_json("ValidationException",
                f"One or more parameter values were invalid: Duplicate index name: {iname}", 400)
        if iname:
            seen_index_names.add(iname)

    # Validate index Projection settings.
    for idx in gsis + lsis:
        idx_name = idx.get("IndexName", "<unnamed>")
        proj = idx.get("Projection") or {}
        ptype = proj.get("ProjectionType", "ALL")
        if ptype not in _VALID_PROJECTION_TYPES:
            return error_response_json("ValidationException",
                f"One or more parameter values were invalid: Unknown ProjectionType: {ptype}", 400)
        if ptype == "INCLUDE" and not proj.get("NonKeyAttributes"):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: ProjectionType is INCLUDE, but NonKeyAttributes is not specified", 400)
        if ptype == "KEYS_ONLY" and proj.get("NonKeyAttributes"):
            return error_response_json("ValidationException",
                "One or more parameter values were invalid: KEYS_ONLY projection type is not compatible with NonKeyAttributes", 400)

    # Validate StreamSpecification: StreamEnabled:false with StreamViewType is invalid.
    stream_spec = data.get("StreamSpecification")
    if stream_spec and stream_spec.get("StreamEnabled") is False and stream_spec.get("StreamViewType"):
        return error_response_json("ValidationException",
            "One or more parameter values were invalid: Table is being created with a stream disabled, UpdateViewType should not be specified", 400)

    # Every attribute referenced in any index KeySchema must be in AttributeDefinitions.
    referenced_attrs = set()
    for ks in key_schema:
        referenced_attrs.add(ks["AttributeName"])
    for idx_group in (gsis, lsis):
        for idx in idx_group:
            for k in idx.get("KeySchema") or []:
                referenced_attrs.add(k.get("AttributeName"))
    err = _vector_indexes_error(vector_indexes, [], billing_mode, seen_attr_names)
    if err:
        return err
    for vix in vector_indexes:
        referenced_attrs |= _vector_search_attrs(vix)
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
    if vector_indexes:
        _tables[name]["VectorIndexes"] = [_vector_index_record(name, v, online=False) for v in vector_indexes]
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
        stream_label = _stream_label()
        _tables[name]["LatestStreamLabel"] = stream_label
        _tables[name]["LatestStreamArn"] = f"{_tables[name]['TableArn']}/stream/{stream_label}"
    if data.get("Tags"):
        _tags[_tables[name]["TableArn"]] = data["Tags"]
    logger.info("DynamoDB table created: %s", name)
    return json_response({"TableDescription": _table_description(name)})


def _delete_table(data):
    name = _normalize_table_name(data.get("TableName"))
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)
    if _tables[name].get("DeletionProtectionEnabled"):
        return error_response_json("ValidationException",
            "Table is protected against deletion. To delete the table, disable deletion protection.", 400)
    if any(_vector_phase(v) != "active" for v in _tables[name].get("VectorIndexes") or []):
        return error_response_json("ResourceInUseException",
            "Attempt to change a resource which is still in use: Cannot delete table while indexes are being "
            "created, updated, or deleted.", 400)
    desc = _table_description(name)
    desc["TableStatus"] = "DELETING"
    remaining = [r for r in _replica_group(_tables[name]) if r != get_region()]
    deleted = _tables.pop(name)
    if remaining:
        _set_replica_group(name, remaining)
    _tags.pop(desc.get("TableArn", ""), None)
    _ttl_settings.pop(name, None)
    _pitr_settings.pop(name, None)
    _kinesis_destinations.pop(name, None)
    drop_stream_records(name, deleted)
    return json_response({"TableDescription": desc})


def _describe_table(data):
    name = _normalize_table_name(data.get("TableName"))
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


_GLOBAL_TABLE_VERSION = "2019.11.21"


def _replica_group(table) -> list:
    """Every region of the global table ``table`` belongs to, its own included."""
    return table.get("_global_regions") or []


def _set_replica_group(name, regions):
    account = get_account_id()
    for region in regions:
        member = _tables.get_scoped(account, region, name)
        if member is not None:
            member["_global_regions"] = sorted(regions) if len(regions) > 1 else []


def _create_replica(name, table, region):
    """Copy ``table`` into ``region`` as a replica, with its items."""
    account = get_account_id()
    replica = copy.deepcopy({k: v for k, v in table.items() if k not in ("items", _INDEX_MEMBERS)})
    replica["items"] = defaultdict(dict, copy.deepcopy(dict(table["items"])))
    arn = f"arn:aws:dynamodb:{region}:{account}:table/{name}"
    replica.update({
        "TableArn": arn, "TableId": new_uuid(), "CreationDateTime": int(time.time()),
        "TableStatus": "ACTIVE", "DeletionProtectionEnabled": False,
    })
    for index in replica.get("GlobalSecondaryIndexes", []) + replica.get("LocalSecondaryIndexes", []):
        index["IndexArn"] = f"{arn}/index/{index['IndexName']}"
    label = _stream_label()
    replica["LatestStreamLabel"] = label
    replica["LatestStreamArn"] = f"{arn}/stream/{label}"
    _tables.set_scoped(account, region, name, replica)
    ttl = _ttl_settings.get(name)
    if ttl:
        _ttl_settings.set_scoped(account, region, name, copy.deepcopy(ttl))


def _apply_replica_updates(name, table, updates):
    """UpdateTable ReplicaUpdates (global tables version 2019.11.21)."""
    account, home = get_account_id(), get_region()
    regions = set(_replica_group(table)) or {home}
    for update in updates:
        action = next(iter(update), None)
        region = (update.get(action) or {}).get("RegionName", "")
        if region == home:
            return error_response_json("ValidationException",
                "Cannot add, delete, or update the local region through ReplicaUpdates. "
                "Use CreateTable, DeleteTable, or UpdateTable as required.", 400)
        if action == "Create":
            if _tables.get_scoped(account, region, name) is not None:
                return error_response_json("ValidationException",
                    f"Failed to create a the new replica of table with name: '{name}' "
                    "because one or more replicas already existed as tables.", 400)
            # MREC replicates through Streams, so they are on for every replica.
            if not (table.get("StreamSpecification") or {}).get("StreamEnabled"):
                table["StreamSpecification"] = {"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"}
                label = _stream_label()
                table["LatestStreamLabel"] = label
                table["LatestStreamArn"] = f"{table['TableArn']}/stream/{label}"
            _create_replica(name, table, region)
            regions.add(region)
        elif action in ("Update", "Delete"):
            if region not in regions:
                return error_response_json("ValidationException",
                    "Replica specified in the Replica Update or Replica Delete action of the request was not found.", 400)
            if action == "Delete":
                deleted = _tables.pop_scoped(account, region, name, None)
                _ttl_settings.pop_scoped(account, region, name, None)
                _pitr_settings.pop_scoped(account, region, name, None)
                with request_scope(account, region):
                    drop_stream_records(name, deleted)
                regions.discard(region)
    _set_replica_group(name, regions)
    if len(regions) <= 1:
        table["_global_regions"] = []
    return None


def _replicate_write(table, event_name, old_item, new_item):
    """Apply a write to the table's other replicas, as MREC global tables do."""
    regions = _replica_group(table)
    if not regions:
        return
    account, home, name = get_account_id(), get_region(), table["TableName"]
    item = new_item if new_item is not None else old_item
    for region in regions:
        if region == home:
            continue
        with request_scope(account, region):
            replica = _tables.get(name)
            if replica is None:
                continue
            pk_val = _extract_key_val(item.get(replica["pk_name"]))
            sk_val = _extract_key_val(item.get(replica["sk_name"])) if replica["sk_name"] else "__no_sort__"
            previous = replica["items"].get(pk_val, {}).get(sk_val)
            if new_item is None:
                _remove_item(replica, pk_val, sk_val)
            else:
                _set_item(replica, pk_val, sk_val, copy.deepcopy(new_item))
            if new_item is None and previous is None:
                continue
            replica_event = "REMOVE" if new_item is None else ("MODIFY" if previous else "INSERT")
            _emit_stream_event(name, replica_event, previous,
                               copy.deepcopy(new_item) if new_item is not None else None, replicate=False)


def _update_table(data):
    name = _normalize_table_name(data.get("TableName"))
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)
    table = _tables[name]

    # UpdateTable must carry at least one actionable change. AttributeDefinitions
    # on its own is rejected by AWS (and no API can change a key attribute's
    # type — MiniStack used to merge it in and silently mutate the key). #1432
    _ACTIONABLE_UPDATE_PARAMS = (
        "ProvisionedThroughput", "BillingMode", "GlobalSecondaryIndexUpdates",
        "StreamSpecification", "SSESpecification", "ReplicaUpdates", "TableClass",
        "OnDemandThroughput", "DeletionProtectionEnabled", "WarmThroughput", "VectorIndexUpdates",
    )
    if not any(k in data for k in _ACTIONABLE_UPDATE_PARAMS):
        return error_response_json("ValidationException",
            "At least one of ProvisionedThroughput, BillingMode, UpdateStreamEnabled, "
            "GlobalSecondaryIndexUpdates, SSESpecification, ReplicaUpdates or TableClass "
            "is required", 400)

    current_billing = table.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED")
    new_billing = data.get("BillingMode")
    err = _apply_vector_index_updates(name, table, data, new_billing or current_billing)
    if err:
        return err
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
        _effective_billing = new_billing or current_billing
        if (
            _effective_billing == "PROVISIONED"
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
                f"1 validation error detected: Value '{new_billing}' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]", 400)
        table["BillingModeSummary"] = {"BillingMode": new_billing, "LastUpdateToPayPerRequestDateTime": int(time.time())}
        if new_billing == "PAY_PER_REQUEST":
            table["ProvisionedThroughput"] = {"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
    if "AttributeDefinitions" in data:
        # Merge incoming AttributeDefinitions with existing ones (union by name).
        # A redeclaration of an existing attribute — even with a conflicting
        # type — is accepted and the STORED type wins; real DynamoDB neither
        # rejects nor overwrites (measured eu-west-2, 2026-07-12, paritysuite).
        existing_ad = {ad["AttributeName"]: ad for ad in table.get("AttributeDefinitions", [])}
        for ad in data["AttributeDefinitions"]:
            existing_ad.setdefault(ad["AttributeName"], ad)
        table["AttributeDefinitions"] = list(existing_ad.values())
    if "StreamSpecification" in data:
        stream_spec = data["StreamSpecification"]
        stream_was_enabled = bool((table.get("StreamSpecification") or {}).get("StreamEnabled"))
        if stream_was_enabled and not stream_spec.get("StreamEnabled"):
            drop_stream_records(name, table)
        table["StreamSpecification"] = stream_spec
        if stream_spec.get("StreamEnabled") and not stream_was_enabled:
            stream_label = _stream_label()
            table["LatestStreamLabel"] = stream_label
            table["LatestStreamArn"] = f"{table['TableArn']}/stream/{stream_label}"
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
                f"1 validation error detected: Value '{tc}' at 'tableClass' failed to satisfy constraint: Member must satisfy enum value set: [STANDARD, STANDARD_INFREQUENT_ACCESS]", 400)
        table["TableClassSummary"] = {"TableClass": tc, "LastUpdateDateTime": int(time.time())}
    # OnDemandThroughput change round-trip.
    if "OnDemandThroughput" in data:
        odt = data["OnDemandThroughput"] or {}
        table["OnDemandThroughput"] = {
            "MaxReadRequestUnits": int(odt.get("MaxReadRequestUnits", -1)),
            "MaxWriteRequestUnits": int(odt.get("MaxWriteRequestUnits", -1)),
        }

    existing_idx_names = {g["IndexName"] for g in table.get("GlobalSecondaryIndexes", [])}
    # A new index's key attributes must all appear in the REQUEST's own
    # AttributeDefinitions — stored definitions do not satisfy the check
    # (measured eu-west-2, 2026-07-12, paritysuite).
    defined_attrs = {a["AttributeName"] for a in data.get("AttributeDefinitions", [])}
    if data.get("GlobalSecondaryIndexUpdates"):
        table.pop(_INDEX_MEMBERS, None)
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
                    f"Requested resource not found: Index: {idx_name}", 400)
            table["GlobalSecondaryIndexes"] = [g for g in table["GlobalSecondaryIndexes"] if g["IndexName"] != idx_name]
            existing_idx_names.discard(idx_name)
        elif "Update" in update:
            idx_name = update["Update"]["IndexName"]
            if idx_name not in existing_idx_names:
                return error_response_json("ResourceNotFoundException",
                    f"Requested resource not found: Index: {idx_name}", 400)
            for gsi in table["GlobalSecondaryIndexes"]:
                if gsi["IndexName"] == idx_name:
                    if "ProvisionedThroughput" in update["Update"]:
                        gsi["ProvisionedThroughput"] = update["Update"]["ProvisionedThroughput"]

    # After all GSI changes, prune AttributeDefinitions to only those attributes
    # still referenced by the table's KeySchema or remaining indexes.
    referenced = set()
    for ks in table.get("KeySchema", []):
        referenced.add(ks.get("AttributeName"))
    for idx in table.get("GlobalSecondaryIndexes", []) + table.get("LocalSecondaryIndexes", []):
        for ks in idx.get("KeySchema", []):
            referenced.add(ks.get("AttributeName"))
    for vix in table.get("VectorIndexes") or []:
        referenced |= _vector_search_attrs(vix)
    table["AttributeDefinitions"] = [
        ad for ad in table.get("AttributeDefinitions", [])
        if ad["AttributeName"] in referenced
    ]

    if data.get("ReplicaUpdates"):
        err = _apply_replica_updates(name, table, data["ReplicaUpdates"])
        if err:
            return err

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
    if t.get("VectorIndexes"):
        desc["VectorIndexes"] = [_vector_index_description(v) for v in t["VectorIndexes"]]
        if desc["TableStatus"] == "ACTIVE" and any(_vector_phase(v) == "allocating" for v in t["VectorIndexes"]):
            desc["TableStatus"] = "UPDATING"
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
    if _replica_group(t):
        desc["GlobalTableVersion"] = _GLOBAL_TABLE_VERSION
        desc["Replicas"] = [{"RegionName": r, "ReplicaStatus": "ACTIVE"}
                            for r in _replica_group(t) if r != get_region()]
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


def _validate_projection_expression_syntax(expr: str) -> str | None:
    """AWS-shape syntax + reserved-keyword validator for ProjectionExpression.

    Surfaces two AWS-canonical errors before per-item processing:
      - `"Invalid ProjectionExpression: Syntax error; token: <c>, near: <ctx>"`
        when the expression starts with a non-path-start character.
      - `"Invalid ProjectionExpression: Attribute name is a reserved keyword;
        reserved keyword: <kw>"` when any unaliased root identifier is a
        reserved DynamoDB keyword.
    """
    s = expr.lstrip()
    if not s:
        return None
    c = s[0]
    if not (c.isalnum() or c == "#" or c == "_"):
        near = s[: min(len(s), 2)]
        return f'Invalid ProjectionExpression: Syntax error; token: "{c}", near: "{near}"'
    # Reserved-keyword scan on each unaliased root identifier.
    for path in (p.strip() for p in expr.split(",")):
        if not path:
            continue
        head = path.split(".")[0].split("[")[0].strip()
        if head and not head.startswith("#") and head.upper() in AWS_KEYWORDS:
            return f"Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: {head}"
    return None


def _projection_overlap_error(expr: str, attr_names: dict) -> str | None:
    """AWS rejects a ProjectionExpression whose paths overlap (equal, or one a
    prefix of another), on the paths after alias resolution, naming the first
    overlapping pair in request order."""
    paths = [_parse_projection_path(p, attr_names or {}) for p in expr.split(",") if p.strip()]
    for j, later in enumerate(paths):
        for earlier in paths[:j]:
            n = min(len(earlier), len(later))
            if earlier[:n] == later[:n]:
                def show(path):
                    return "[" + ", ".join(f"[{v}]" if kind == "index" else str(v) for kind, v in path) + "]"
                return ("Invalid ProjectionExpression: Two document paths overlap with each other; "
                        f"must remove or rewrite one of these paths; path one: {show(earlier)}, path two: {show(later)}")
    return None


def _projection_overlap_response(data) -> tuple | None:
    expr = (data.get("ProjectionExpression") or "").strip()
    message = _projection_overlap_error(expr, data.get("ExpressionAttributeNames")) if expr else None
    return error_response_json("ValidationException", message, 400) if message else None


def _expression_size_error(data, expression_fields: tuple) -> tuple | None:
    """Each expression is capped at 4096 bytes of its raw text."""
    for fname in expression_fields:
        body = data.get(fname) or ""
        size = len(body.encode("utf-8")) if isinstance(body, str) else 0
        if size > _DDB_EXPR_MAX_BYTES:
            return error_response_json("ValidationException",
                f"Invalid {fname}: Expression size has exceeded the maximum allowed size; expression size: {size}", 400)
    return None


def _validate_expression_attrs(data, expression_fields: tuple) -> tuple | None:
    """AWS rejects ExpressionAttributeValues / ExpressionAttributeNames when
    no expression field references them at all, AND when any defined alias
    isn't used by any of the expressions, AND when any `:foo` or `#bar`
    referenced by an expression isn't defined."""
    # Expression string length limit: 4096 bytes each.
    err = _expression_size_error(data, expression_fields)
    if err:
        return err
    has_any_expr = any(data.get(f) for f in expression_fields)
    eav = data.get("ExpressionAttributeValues")
    ean = data.get("ExpressionAttributeNames")
    if eav and not has_any_expr:
        # AWS-canonical: EAV without any expression is rejected with the
        # "can only be used when..." wording. Real AWS appends the first
        # expression-slot name ("<Field> is null") for write ops; for ops
        # with multiple expression slots the suffix is omitted.
        suffix = ""
        if len(expression_fields) == 1:
            suffix = f": {expression_fields[0]} is null"
        return error_response_json("ValidationException",
            f"ExpressionAttributeValues can only be specified when using expressions{suffix}", 400)
    if ean and not has_any_expr:
        return error_response_json("ValidationException",
            "ExpressionAttributeNames can only be specified when using expressions: KeyConditionExpression, ConditionExpression, ProjectionExpression, FilterExpression, UpdateExpression", 400)
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
    # AWS scopes the error to the *specific* expression that contains the bad
    # reference: "Invalid FilterExpression: An expression attribute value
    # used in expression is not defined; attribute value: :v".
    for fname in expression_fields:
        body = data.get(fname) or ""
        if not body:
            continue
        for ref in re.findall(r":[A-Za-z_][A-Za-z0-9_]*", body):
            if not eav or ref not in eav:
                return error_response_json("ValidationException",
                    f"Invalid {fname}: An expression attribute value used in expression is not defined; attribute value: {ref}", 400)
        for ref in re.findall(r"#[A-Za-z_][A-Za-z0-9_]*", body):
            if not ean or ref not in ean:
                return error_response_json("ValidationException",
                    f"Invalid {fname}: An expression attribute name used in the document path is not defined; attribute name: {ref}", 400)
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
    name = _normalize_table_name(data.get("TableName"))
    err = _validate_data_plane_table_name(name)
    if err:
        return err
    err = _check_per_op_param_enums(data, _PUT_DELETE_RV_VALUES)
    if err:
        return err
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            "Requested resource not found", 400)
    err = _validate_expression_attrs(data, ("ConditionExpression",))
    if err:
        return err

    item = data.get("Item", {})
    err = _validate_item(item, table.get("pk_name"), table.get("sk_name"))
    if err:
        return err
    # Validate GSI/LSI key attribute types and values.
    err = _validate_index_key_values(table, item)
    if err:
        return err
    # Hash key must be present.
    if table.get("pk_name") and table["pk_name"] not in item:
        return error_response_json("ValidationException",
            f"One or more parameter values were invalid: Missing the key {table['pk_name']} in the item", 400)
    if table.get("sk_name") and table["sk_name"] not in item:
        return error_response_json("ValidationException",
            f"One or more parameter values were invalid: Missing the key {table['sk_name']} in the item", 400)
    # A key attribute present with the wrong type is a ValidationException on
    # PutItem, as on UpdateItem/GetItem/TransactWrite (real AWS: "Type mismatch
    # for key <name> expected: <S> actual: <N>").
    type_msg = _key_type_mismatch_reason(table, item)
    if type_msg:
        return error_response_json("ValidationException", type_msg, 400)
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

    _set_item(table, pk_val, sk_val, item)

    event_name = "MODIFY" if old_item else "INSERT"
    _emit_stream_event(name, event_name, old_item, item)

    result = {}
    if data.get("ReturnValues") == "ALL_OLD" and old_item:
        result["Attributes"] = old_item
    _add_consumed_capacity(result, data, name, write=True, old_item=old_item, new_item=item)
    _add_item_collection_metrics(result, data, table, item, None)
    return json_response(result)


def _get_item(data):
    name = _normalize_table_name(data.get("TableName"))
    err = _validate_data_plane_table_name(name)
    if err:
        return err
    err = _check_per_op_param_enums(data, None)
    if err:
        return err
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            "Requested resource not found", 400)
    if data.get("ProjectionExpression") and data.get("AttributesToGet"):
        return error_response_json("ValidationException",
            "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}", 400)
    _pe = (data.get("ProjectionExpression") or "").strip()
    if _pe:
        _pe_err = _validate_projection_expression_syntax(_pe)
        if _pe_err:
            return error_response_json("ValidationException", _pe_err, 400)
    err = _validate_expression_attrs(data, ("ProjectionExpression",)) or _projection_overlap_response(data)
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
    name = _normalize_table_name(data.get("TableName"))
    err = _validate_data_plane_table_name(name)
    if err:
        return err
    err = _check_per_op_param_enums(data, _PUT_DELETE_RV_VALUES)
    if err:
        return err
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            "Requested resource not found", 400)
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
        _remove_item(table, pk_val, sk_val)
        _emit_stream_event(name, "REMOVE", old_item, None)

    result = {}
    if data.get("ReturnValues") == "ALL_OLD" and old_item:
        result["Attributes"] = old_item
    _add_consumed_capacity(result, data, name, write=True, old_item=old_item)
    _add_item_collection_metrics(result, data, table, None, key)
    return json_response(result)


def _key_attribute_update_error(table, updated_attrs):
    """AWS error for an update whose targets include a hash or range key."""
    tops = {p[0] if isinstance(p, tuple) else p for p in updated_attrs}
    for key_name in (table.get("pk_name"), table.get("sk_name")):
        if key_name and key_name in tops:
            return error_response_json("ValidationException",
                f"One or more parameter values were invalid: Cannot update attribute {key_name}. This attribute is part of the key", 400)
    return None


def _update_item(data):
    name = _normalize_table_name(data.get("TableName"))
    err = _validate_data_plane_table_name(name)
    if err:
        return err
    # UpdateItem stops at the first invalid enum, unlike the other writes.
    err = _check_per_op_param_enums(data, _UPDATE_RV_VALUES, first_only=True)
    if err:
        return err
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            "Requested resource not found", 400)
    # Pre-validate UpdateExpression syntax BEFORE the unused-EAV check.
    # AWS reports `"Invalid UpdateExpression: Syntax error; token: <first>,
    # near: <first second>"` for a body that doesn't start with a clause
    # keyword (SET / ADD / REMOVE / DELETE), regardless of EAV usage.
    _ue_pre = (data.get("UpdateExpression") or "").strip()
    if _ue_pre:
        _ue_tokens = _ue_pre.split()
        if _ue_tokens and _ue_tokens[0].upper() not in ("SET", "ADD", "REMOVE", "DELETE"):
            _near = " ".join(_ue_tokens[:2])
            return error_response_json("ValidationException",
                f'Invalid UpdateExpression: Syntax error; token: "{_ue_tokens[0]}", near: "{_near}"', 400)
    err = _validate_expression_attrs(data, ("ConditionExpression", "UpdateExpression"))
    if err:
        return err
    if any(_nesting_exceeded(v) for v in (data.get("ExpressionAttributeValues") or {}).values()):
        return error_response_json("ValidationException", f"1 validation error detected: {_NESTING_MSG}", 400)

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
            "Invalid UpdateExpression: The expression can not be empty;", 400)

    # AWS pre-validates UpdateExpression syntax — the first identifier must be
    # a clause keyword (SET / ADD / REMOVE / DELETE). Anything else is
    # `"Invalid UpdateExpression: Syntax error; token: <first>, near: <first second>"`.
    if update_expr.strip():
        _tokens_pre = update_expr.strip().split()
        if _tokens_pre and _tokens_pre[0].upper() not in ("SET", "ADD", "REMOVE", "DELETE"):
            _near = " ".join(_tokens_pre[:2])
            return error_response_json("ValidationException",
                f'Invalid UpdateExpression: Syntax error; token: "{_tokens_pre[0]}", near: "{_near}"', 400)

    updated_attrs = set()
    if update_expr:
        # AWS rejects an UpdateExpression that targets a key attribute, even
        # when the item doesn't exist yet or the value is unchanged — the
        # rejection is parse-time, and precedes the runtime operand-type
        # errors the evaluator raises. `updated_attrs` is owned here so the
        # targets resolved before a raise are still available; they are already
        # alias-resolved, so the expression is never scanned a second time.
        try:
            item, updated_attrs = _apply_update_expression(item, update_expr, eav, ean, updated_attrs)
        except ValueError as exc:
            return (_key_attribute_update_error(table, updated_attrs)
                    or error_response_json("ValidationException", str(exc), 400))
        key_err = _key_attribute_update_error(table, updated_attrs)
        if key_err:
            return key_err
    elif attribute_updates:
        try:
            item = _apply_attribute_updates(item, attribute_updates)
        except _AttributeUpdatesValidationError as exc:
            return error_response_json("ValidationException", str(exc), 400)
        updated_attrs = set(attribute_updates.keys())
    # AWS rejects any update that would mutate a hash or range key value.
    for key_name in (table.get("pk_name"), table.get("sk_name")):
        if key_name and key_name in item and existing is not None:
            if item.get(key_name) != existing.get(key_name):
                return error_response_json("ValidationException",
                    f"One or more parameter values were invalid: Cannot update attribute {key_name}. This attribute is part of the key", 400)

    # AWS rejects updates with invalid values
    err = _validate_item(item, table.get("pk_name"), table.get("sk_name"), _UPDATE_SIZE_MSG)
    if err:
        return err
    if update_expr and _update_statement_size(item, update_expr, ean) > _DDB_ITEM_MAX_BYTES:
        return error_response_json("ValidationException", _UPDATE_SIZE_MSG, 400)
    # Validate GSI/LSI key attribute types and values after the update is applied.
    err = _validate_index_key_values(table, item, update_expr=bool(data.get("UpdateExpression")))
    if err:
        return err

    _set_item(table, pk_val, sk_val, item)

    event_name = "MODIFY" if old_item else "INSERT"
    _emit_stream_event(name, event_name, old_item, item)

    result = {}
    rv = data.get("ReturnValues", "NONE")
    if rv == "ALL_NEW":
        result["Attributes"] = item
    elif rv == "ALL_OLD" and old_item:
        result["Attributes"] = old_item
    elif rv == "UPDATED_OLD" and old_item:
        result["Attributes"] = _diff_attributes(old_item, item, updated_attrs, return_old=True)
    elif rv == "UPDATED_NEW":
        # AWS omits Attributes from the response when the only operation was
        # REMOVE — there are no "new" values to return.
        new_attrs = _diff_attributes(old_item or {}, item, updated_attrs, return_old=False)
        if new_attrs:
            result["Attributes"] = new_attrs
    _add_consumed_capacity(result, data, name, write=True, old_item=old_item, new_item=item)
    _add_item_collection_metrics(result, data, table, item, key)
    return json_response(result)


# ---------------------------------------------------------------------------
# Query / Scan
# ---------------------------------------------------------------------------

def _query(data):
    name = _normalize_table_name(data.get("TableName"))
    err = _validate_data_plane_table_name(name)
    if err:
        return err
    err = _check_per_op_param_enums(data, None)
    if err:
        return err
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            "Requested resource not found", 400)
    # Non-existent IndexName → ValidationException (not ResourceNotFoundException).
    idx_req = data.get("IndexName")
    if idx_req:
        known = {g["IndexName"] for g in table.get("GlobalSecondaryIndexes", [])} | \
                {l["IndexName"] for l in table.get("LocalSecondaryIndexes", [])}
        if idx_req not in known:
            return error_response_json("ValidationException",
                f"The table does not have the specified index: {idx_req}", 400)
    # AWS-canonical: empty KeyConditionExpression is rejected BEFORE the
    # unused-EAV check (EAV is presumed valid in this case — the empty
    # expression is the load-bearing error).
    if "KeyConditionExpression" in data and not (data["KeyConditionExpression"] or "").strip():
        return error_response_json("ValidationException",
            "Invalid KeyConditionExpression: The expression can not be empty;", 400)
    # AWS pre-validates redundant parentheses at parse time, not at evaluation
    # time — so the check must fire even when no items match the query.
    for _slot in ("KeyConditionExpression", "FilterExpression"):
        _expr = (data.get(_slot) or "").strip()
        if _expr:
            try:
                _err = _check_redundant_parens(_tokenize(_expr), _slot)
                if _err:
                    return error_response_json("ValidationException", _err, 400)
            except Exception:
                pass
    err = (_validate_expression_attrs(data, ("KeyConditionExpression", "FilterExpression", "ProjectionExpression"))
           or _projection_overlap_response(data))
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
    # A ProjectionExpression / AttributesToGet without an explicit Select is
    # equivalent to Select=SPECIFIC_ATTRIBUTES (AWS Query docs); only default to
    # ALL_ATTRIBUTES when no projection is requested.
    select = data.get("Select")
    if select is None:
        select = "SPECIFIC_ATTRIBUTES" if (data.get("ProjectionExpression") or data.get("AttributesToGet")) else "ALL_ATTRIBUTES"

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
        # AWS-canonical (dynamodb-conformance.org capture).
        return error_response_json("ValidationException",
            "Invalid KeyConditionExpression: The expression can not be empty;", 400)
    # Select must be one of the canonical enum values when supplied.
    if "Select" in data and data["Select"] not in {"ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "COUNT"}:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{data['Select']}' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]", 400)
    # Malformed ExclusiveStartKey: must include all key attributes of the
    # table (and the index, if querying an index).
    esk_val = data.get("ExclusiveStartKey")
    if esk_val is not None and not isinstance(esk_val, dict):
        return error_response_json("ValidationException",
            "The provided starting key is invalid", 400)
    # Limit must be >= 1 when supplied.
    if limit is not None and int(limit) <= 0:
        return error_response_json("ValidationException",
            "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1", 400)
    # Select validation per AWS (messages measured against real DynamoDB by
    # paritysuite). A ProjectionExpression with a non-SPECIFIC Select is
    # reported first, even when the IndexName rule is also broken.
    if "Select" in data and data.get("ProjectionExpression"):
        if select == "ALL_ATTRIBUTES":
            return error_response_json("ValidationException",
                "Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES", 400)
        if select == "COUNT":
            return error_response_json("ValidationException",
                "Cannot specify the ProjectionExpression when choosing to get only the Count", 400)
        if select == "ALL_PROJECTED_ATTRIBUTES":
            return error_response_json("ValidationException",
                "Cannot specify the ProjectionExpression when choosing to get ALL_PROJECTED_ATTRIBUTES", 400)
    if select == "ALL_PROJECTED_ATTRIBUTES" and not index_name:
        return error_response_json("ValidationException",
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName", 400)
    if select == "SPECIFIC_ATTRIBUTES" and not data.get("ProjectionExpression") and not data.get("AttributesToGet"):
        return error_response_json("ValidationException",
            "1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES", 400)

    pk_name, sk_name, is_gsi = _resolve_index_keys(table, index_name)
    index = _index_def(table, index_name) if index_name else None
    hash_names, range_names = _index_key_lists(index) if index else ([pk_name], [sk_name] if sk_name else [])
    multi_key = len(hash_names) > 1 or len(range_names) > 1
    if multi_key:
        pk_name, sk_name = hash_names[0], (range_names[0] if range_names else None)
    # ConsistentRead on a GSI is invalid (only LSIs support strongly-consistent reads).
    if data.get("ConsistentRead") and is_gsi:
        return error_response_json("ValidationException",
            "Consistent reads are not supported on global secondary indexes", 400)

    # ExclusiveStartKey must contain the base table's key attributes; when
    # querying an index, it must also contain the index's key attributes.
    # Real DynamoDB's LastEvaluatedKey always carries both sets, so a missing
    # attribute means the cursor wasn't issued by a previous Query response.
    if esk:
        required = {table["pk_name"]}
        if table.get("sk_name"):
            required.add(table["sk_name"])
        if index_name:
            required.update(hash_names + range_names)
        if not required.issubset(esk.keys()):
            return error_response_json("ValidationException",
                "The provided starting key is invalid", 400)

    # Every partition-key attribute needs an equality condition; a
    # multi-attribute partition is the tuple of their values.
    pk_vals = []
    for _hn in hash_names:
        if key_conditions:
            _hv = _extract_pk_from_key_conditions(key_conditions, _hn)
        else:
            _hv = _extract_pk_from_condition(key_cond, eav, ean, _hn)
        if _hv is None:
            return error_response_json("ValidationException",
                f"Query condition missed key schema element: {_hn}", 400)
        pk_vals.append(_hv)
    pk_val = pk_vals[0] if len(pk_vals) == 1 else tuple(pk_vals)
    # A key-condition value whose type does not match the key's schema type is
    # rejected, as on real AWS ("Condition parameter type does not match schema
    # type") — MiniStack used to accept N for an S key and return the row.
    for _kn in hash_names + range_names:
        if not _kn:
            continue
        _av = _key_condition_av(key_cond, key_conditions, eav, ean, _kn)
        if isinstance(_av, dict) and len(_av) == 1:
            _expected = _get_attr_type(table, _kn)
            if _expected and next(iter(_av)) != _expected:
                return error_response_json("ValidationException",
                    "One or more parameter values were invalid: Condition "
                    "parameter type does not match schema type", 400)
    # Reject non-key attributes in KeyConditionExpression: every bare identifier
    # in the expression must be either the hash key or the sort key (resolved
    # via ExpressionAttributeNames if aliased).
    if key_cond:
        try:
            kce_tokens = _tokenize(key_cond)
        except Exception:
            kce_tokens = []
        allowed = set(hash_names + range_names)
        # A document path on a key attribute is rejected up front (AWS).
        if any(tok[0] == "DOT" for tok in kce_tokens):
            return error_response_json("ValidationException",
                "KeyConditionExpressions cannot have conditions on nested attributes", 400)
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
        # Multi-attribute sort keys are queried left to right without gaps,
        # and only the last one queried may use a range condition.
        if len(range_names) > 1 and not _multi_sort_condition_ok(kce_tokens, ean, range_names):
            return error_response_json("ValidationException",
                "Query key condition not supported", 400)
        # Key-condition operands are validated like key values themselves:
        # an empty string/binary operand is rejected with the same error AWS
        # raises for empty key attribute values.
        cur_attr = pk_name
        for tok in kce_tokens:
            if tok[0] == "IDENT" and tok[1] in allowed:
                cur_attr = tok[1]
            elif tok[0] == "NAME_REF" and ean.get(tok[1]) in allowed:
                cur_attr = ean[tok[1]]
            elif tok[0] == "VALUE_REF":
                err = _empty_key_value_error(cur_attr, eav.get(tok[1]))
                if err:
                    return err
        # AWS validates BETWEEN bounds at parse time: lower must be <= upper,
        # even when the partition holds no items.
        for i, tok in enumerate(kce_tokens):
            if (tok[0] == "IDENT" and tok[1].upper() == "BETWEEN"
                    and i + 3 < len(kce_tokens)
                    and kce_tokens[i + 1][0] == "VALUE_REF"
                    and kce_tokens[i + 2][0] == "IDENT" and kce_tokens[i + 2][1].upper() == "AND"
                    and kce_tokens[i + 3][0] == "VALUE_REF"):
                err = _between_bounds_error(eav.get(kce_tokens[i + 1][1]), eav.get(kce_tokens[i + 3][1]))
                if err:
                    return err
        # An ExclusiveStartKey must itself satisfy the key condition — AWS
        # rejects a cursor whose sort value falls outside the range predicate
        # (it could never have been issued by a previous page of this query).
        if esk and sk_name:
            try:
                esk_matches = _evaluate_condition(key_cond, esk, eav, ean, slot="KeyConditionExpression")
            except ValueError:
                esk_matches = True
            if not esk_matches:
                return error_response_json("ValidationException",
                    "The provided starting key does not match the range key predicate", 400)

    members = _index_members(table).get(index_name) if index_name else None
    if members is not None:
        # Only this index partition; sparse items are never members.
        bucket = table["items"]
        candidates = [bucket[pk][sk] for pk, sk in members.get(pk_val, ()) if sk in bucket.get(pk, ())]
    elif is_gsi or index_name:
        candidates = []
        for pk_bucket in table["items"].values():
            for it in pk_bucket.values():
                if pk_name in it and _extract_key_val(it[pk_name]) == pk_val:
                    if sk_name and sk_name not in it:
                        continue
                    candidates.append(it)
    else:
        candidates = list(table["items"].get(pk_val, {}).values())

    if is_gsi or index_name:
        # GSI/LSI: order by (INDEX_SORT, BASE_PK, BASE_SK). The base-table keys
        # tiebreak rows with equal INDEX_SORT (or hash-only GSIs), matching
        # real DynamoDB's hidden ordering and making pagination cursors stable.
        sort_keys = _index_order_keys(table, range_names)
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
            candidates = [it for it in candidates if _evaluate_condition(key_cond, it, eav, ean, slot="KeyConditionExpression")]
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)

    if esk:
        candidates = _apply_exclusive_start_key(
            candidates, esk, pk_name, range_names if multi_key else sk_name, scan_forward, table=table)

    # AWS returns a LastEvaluatedKey whenever it stopped *because of* the
    # limit — including when the results end exactly at the limit, since it
    # doesn't look ahead. The follow-up page then returns 0 items and no key.
    candidates, has_more = _paginate_evaluated_items(candidates, limit, table, index_name)

    scanned_count = len(candidates)
    query_filter = data.get("QueryFilter")
    if query_filter and not filter_expr:
        filtered = [it for it in candidates if _evaluate_legacy_filter(it, query_filter)]
    elif filter_expr:
        try:
            filtered = [it for it in candidates if _evaluate_condition(filter_expr, it, eav, ean, slot="FilterExpression")]
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
    else:
        filtered = candidates

    if select == "COUNT":
        result = {"Count": len(filtered), "ScannedCount": scanned_count}
    else:
        try:
            # Two-stage projection: first restrict to what the index projects
            # (when querying a GSI/LSI), then apply any user ProjectionExpression
            # / AttributesToGet. AWS only exposes attributes the index actually
            # projects through index reads.
            stage1 = [_apply_index_projection(it, table, index_name) for it in filtered]
            projected = [_apply_projection(it, data) for it in stage1]
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
            for k in hash_names + range_names:
                if k in candidates[-1]:
                    lek.setdefault(k, candidates[-1][k])
        result["LastEvaluatedKey"] = lek

    _add_consumed_capacity(result, data, name, index_name=data.get("IndexName"))
    return json_response(result)


def _scan(data):
    name = _normalize_table_name(data.get("TableName"))
    err = _validate_data_plane_table_name(name)
    if err:
        return err
    err = _check_per_op_param_enums(data, None)
    if err:
        return err
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException",
            "Requested resource not found", 400)
    # Non-existent IndexName → ValidationException.
    idx_req = data.get("IndexName")
    if idx_req:
        known = {g["IndexName"] for g in table.get("GlobalSecondaryIndexes", [])} | \
                {l["IndexName"] for l in table.get("LocalSecondaryIndexes", [])}
        if idx_req in {v["IndexName"] for v in table.get("VectorIndexes") or []}:
            return error_response_json("ValidationException",
                "Scan operation not supported on this index type", 400)
        if idx_req not in known:
            return error_response_json("ValidationException",
                f"The table does not have the specified index: {idx_req}", 400)
    # Pre-validate redundant parens on FilterExpression — AWS rejects at parse
    # time, so the error must fire even when the table is empty.
    _fexpr = (data.get("FilterExpression") or "").strip()
    if _fexpr:
        try:
            _err = _check_redundant_parens(_tokenize(_fexpr), "FilterExpression")
            if _err:
                return error_response_json("ValidationException", _err, 400)
        except Exception:
            pass
        # AWS rejects begins_with with a non-string/binary operand at parse
        # time. The 2nd argument is referenced by EAV placeholder (:foo);
        # peek at its declared type.
        _eav = data.get("ExpressionAttributeValues") or {}
        for _m in re.finditer(r"begins_with\s*\([^,]+,\s*(:[A-Za-z0-9_]+)\s*\)", _fexpr):
            _ph = _m.group(1)
            _av = _eav.get(_ph) or {}
            _t = next(iter(_av.keys()), None) if isinstance(_av, dict) and _av else None
            if _t and _t not in ("S", "B"):
                return error_response_json("ValidationException",
                    f"Invalid FilterExpression: Incorrect operand type for operator or function; operator or function: begins_with, operand type: {_t}", 400)
    err = _validate_expression_attrs(data, ("FilterExpression", "ProjectionExpression")) or _projection_overlap_response(data)
    if err:
        return err

    filter_expr = data.get("FilterExpression", "")
    eav = data.get("ExpressionAttributeValues", {})
    ean = data.get("ExpressionAttributeNames", {})
    limit = data.get("Limit")
    esk = data.get("ExclusiveStartKey")
    index_name = data.get("IndexName")
    # A ProjectionExpression / AttributesToGet without an explicit Select is
    # equivalent to Select=SPECIFIC_ATTRIBUTES (AWS Scan docs); only default to
    # ALL_ATTRIBUTES when no projection is requested.
    select = data.get("Select")
    if select is None:
        select = "SPECIFIC_ATTRIBUTES" if (data.get("ProjectionExpression") or data.get("AttributesToGet")) else "ALL_ATTRIBUTES"

    # Limit must be > 0 when provided (AWS rejects Limit=0).
    if limit is not None and int(limit) <= 0:
        return error_response_json("ValidationException",
            "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1", 400)
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
            "The TotalSegments parameter is required but was not present in the request when Segment parameter is present", 400)
    if total_segments is not None and segment is None:
        return error_response_json("ValidationException",
            "The Segment parameter is required but was not present in the request when parameter TotalSegments is present", 400)
    if segment is not None and total_segments is not None:
        seg = int(segment); ts = int(total_segments)
        if ts < 1 or ts > 1_000_000:
            return error_response_json("ValidationException",
                "TotalSegments must be between 1 and 1000000", 400)
        # Negative segment uses the standard "1 validation error detected"
        # envelope with the lowercase 'segment' slot and "greater than or
        # equal to 0" floor — distinct from the segment>=totalSegments error.
        if seg < 0:
            return error_response_json("ValidationException",
                "1 validation error detected: Value at 'Segment' failed to satisfy constraint: Member must have value greater than or equal to 0", 400)
        if seg >= ts:
            return error_response_json("ValidationException",
                "The Segment parameter is zero-based and must be less than parameter TotalSegments", 400)
    # Select validation per AWS (messages measured against real DynamoDB by
    # paritysuite — Scan reuses the Query-worded IndexName message verbatim).
    if "Select" in data and data.get("ProjectionExpression"):
        if select == "ALL_ATTRIBUTES":
            return error_response_json("ValidationException",
                "Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES", 400)
        if select == "COUNT":
            return error_response_json("ValidationException",
                "Cannot specify the ProjectionExpression when choosing to get only the Count", 400)
        if select == "ALL_PROJECTED_ATTRIBUTES":
            return error_response_json("ValidationException",
                "Cannot specify the ProjectionExpression when choosing to get ALL_PROJECTED_ATTRIBUTES", 400)
    if select == "ALL_PROJECTED_ATTRIBUTES" and not index_name:
        return error_response_json("ValidationException",
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName", 400)
    if select == "SPECIFIC_ATTRIBUTES" and not data.get("ProjectionExpression") and not data.get("AttributesToGet"):
        return error_response_json("ValidationException",
            "1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES", 400)
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
        pk_name_idx, sk_name_idx, is_gsi = _resolve_index_keys(table, index_name)
        idx_hashes, idx_ranges = _index_key_lists(_index_def(table, index_name) or {})
        if is_gsi:
            # Sparse GSI semantics: items lacking ANY of the index's key
            # attributes (hash, or range on a composite GSI) don't appear.
            all_items = [it for it in all_items if all(n in it for n in idx_hashes + idx_ranges)]
        else:
            # LSI: items lacking the index's RANGE attribute don't appear.
            if sk_name_idx:
                all_items = [it for it in all_items if sk_name_idx in it]
        # Sort by index key ordering so ESK/LEK pagination is consistent.
        sort_keys = _index_order_keys(table, idx_ranges or sk_name_idx)
        all_items.sort(key=lambda it: tuple(_index_order_value(it, n, t) for n, t in sort_keys))

    # Parallel scan: partition items deterministically across segments by
    # hashing the partition key. AWS guarantees segments return disjoint
    # subsets and their union equals the full table scan.
    if segment is not None and total_segments is not None:
        import hashlib as _hl
        seg_n = int(segment); ts_n = int(total_segments)
        if index_name:
            pk_attr = pk_name_idx
        else:
            pk_attr = table.get("pk_name") or "pk"
        def _seg_match(it):
            pk_val = _extract_key_val(it.get(pk_attr, {}))
            h = _hl.sha1(str(pk_val).encode("utf-8")).digest()
            return (int.from_bytes(h[:4], "big") % ts_n) == seg_n
        all_items = [it for it in all_items if _seg_match(it)]

    if esk:
        # ESK must contain the base-table key attributes (and the index's keys
        # when scanning an index). AWS LastEvaluatedKey always carries both sets;
        # a missing attribute indicates the cursor wasn't issued by Scan.
        required = {table["pk_name"]}
        if table.get("sk_name"):
            required.add(table["sk_name"])
        if index_name:
            required.update(idx_hashes + idx_ranges)
        if not required.issubset(esk.keys()):
            return error_response_json("ValidationException",
                "The provided starting key is invalid", 400)
        if index_name:
            # For index scans, use index-aware ordering (same as Query pagination).
            all_items = _apply_exclusive_start_key(
                all_items, esk, pk_name_idx, idx_ranges if len(idx_ranges) > 1 else sk_name_idx,
                scan_forward=True, table=table
            )
        else:
            all_items = _apply_exclusive_start_key_scan(all_items, esk, table)

    # Same LastEvaluatedKey semantics as Query: stopping exactly at the limit
    # still yields a key, because AWS doesn't look ahead.
    all_items, has_more = _paginate_evaluated_items(all_items, limit, table, index_name)

    scanned_count = len(all_items)

    # Legacy ScanFilter / QueryFilter support
    scan_filter = data.get("ScanFilter") or data.get("QueryFilter")
    if scan_filter and not filter_expr:
        filtered = [it for it in all_items if _evaluate_legacy_filter(it, scan_filter)]
    elif filter_expr:
        try:
            filtered = [it for it in all_items if _evaluate_condition(filter_expr, it, eav, ean, slot="FilterExpression")]
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
    else:
        filtered = all_items

    if select == "COUNT":
        result = {"Count": len(filtered), "ScannedCount": scanned_count}
    else:
        try:
            stage1 = [_apply_index_projection(it, table, index_name) for it in filtered]
            projected = [_apply_projection(it, data) for it in stage1]
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
        result = {
            "Items": projected,
            "Count": len(filtered),
            "ScannedCount": scanned_count,
        }

    if has_more and all_items:
        lek = _build_key(all_items[-1], table["pk_name"], table["sk_name"])
        if index_name:
            for k in idx_hashes + idx_ranges:
                if k in all_items[-1]:
                    lek.setdefault(k, all_items[-1][k])
        result["LastEvaluatedKey"] = lek

    _add_consumed_capacity(result, data, name, index_name=data.get("IndexName"))
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
        if parsed.get("index"):
            status, headers, body = _partiql_select_index(table, parsed, data)
        else:
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
        if isinstance(payload, dict) and "ConsumedCapacity" not in payload:
            units = max(1.0, float(len(payload.get("Items", []) or [1])))
            payload["ConsumedCapacity"] = {"TableName": table_name, "CapacityUnits": units}
            new_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            return status, headers, new_body
    return status, headers, body


def _partiql_select_index(table, parsed, data):
    """Serve SELECT ... FROM "table"."index" per real DynamoDB (paritysuite):
    membership and projection follow the index; an LSI reaches back to the
    base table for unprojected attributes (charged on the table arm per row
    walked) while a GSI rejects them; a keyed read refuses filters on
    unprojected attributes; capacity lands on the index arm; NextTokens are
    bound to the index that minted them."""
    index_name = parsed["index"]
    gsis = {i.get("IndexName"): (i, True) for i in table.get("GlobalSecondaryIndexes", []) or []}
    lsis = {i.get("IndexName"): (i, False) for i in table.get("LocalSecondaryIndexes", []) or []}
    idx, is_gsi = (gsis.get(index_name) or lsis.get(index_name) or (None, None))
    if idx is None and index_name in {v["IndexName"] for v in table.get("VectorIndexes") or []}:
        return error_response_json("ValidationException",
            "Scan operation not supported on this index type", 400)
    if idx is None:
        return error_response_json("ValidationException",
            "The table does not have the specified index", 400)

    consistent = bool(data.get("ConsistentRead"))
    if consistent and is_gsi:
        return error_response_json("ValidationException",
            "Strongly consistent read is not supported on Global Secondary Indexes", 400)
    rate = 1.0 if consistent else 0.5

    key_names = [ks.get("AttributeName") for ks in idx.get("KeySchema", []) or []]
    table_keys = [table.get("pk_name")] + ([table.get("sk_name")] if table.get("sk_name") else [])
    proj = idx.get("Projection") or {}
    ptype = proj.get("ProjectionType", "ALL")
    view_attrs = None  # None == ALL
    if ptype != "ALL":
        view_attrs = set(key_names) | set(table_keys)
        if ptype == "INCLUDE":
            view_attrs |= set(proj.get("NonKeyAttributes") or [])

    conditions = parsed.get("conditions") or []
    keyed = any(attr in key_names for attr, _op, _v in conditions)
    if keyed and view_attrs is not None:
        unprojected_filters = list(dict.fromkeys(a for a, _op, _v in conditions if a not in view_attrs))
        if unprojected_filters:
            return error_response_json("ValidationException",
                f"One or more parameter values were invalid: Secondary index {index_name} does not project "
                f"one or more filter attributes: [{', '.join(unprojected_filters)}]", 400)

    projections = parsed.get("projections")
    reach_back = False
    if projections and view_attrs is not None:
        unprojected = [a for a in projections if a not in view_attrs]
        if unprojected:
            if is_gsi:
                return error_response_json("ValidationException",
                    f"One or more parameter values were invalid: Global secondary index {index_name} does not project [{', '.join(unprojected)}]", 400)
            reach_back = True

    def _index_view(item):
        if view_attrs is None:
            return dict(item)
        return {k: v for k, v in item.items() if k in view_attrs}

    # Membership + index ordering.
    members = []
    for pk in table["items"]:
        for sk in table["items"][pk]:
            it = table["items"][pk][sk]
            if all(k in it for k in key_names):
                members.append(it)
    def _order_key(it):
        return tuple(json.dumps(it.get(k), sort_keys=True) for k in key_names + table_keys)
    members.sort(key=_order_key)

    # Rows walked = rows matching the key conditions (filters apply later).
    key_conds = [(a, o, v) for a, o, v in conditions if a in key_names]
    walked = [it for it in members
              if all(_eval_partiql_pred(_index_view(it), a, o, v) for a, o, v in key_conds)]

    # Pagination: tokens are bound to the index that minted them.
    offset = 0
    token = data.get("NextToken")
    if token:
        try:
            import base64 as _b64
            tk = json.loads(_b64.b64decode(token).decode("utf-8"))
            assert tk.get("t") == table.get("TableName") and tk.get("i") == index_name
            offset = int(tk.get("o", 0))
        except Exception:
            return error_response_json("ValidationException",
                "The provided NextToken is invalid", 400)
    limit = data.get("Limit")
    page = walked[offset:]
    next_token = None
    if limit is not None and int(limit) < len(page):
        page = page[: int(limit)]
        import base64 as _b64
        next_token = _b64.b64encode(json.dumps(
            {"t": table.get("TableName"), "i": index_name, "o": offset + len(page)}
        ).encode("utf-8")).decode("ascii")

    where_fn = parsed.get("where_fn")
    kept = [it for it in page if (where_fn is None or where_fn(_index_view(it)))]

    items = []
    for it in kept:
        if projections:
            src = it if reach_back else _index_view(it)
            row = {}
            for attr in projections:
                if attr in src:
                    row[attr] = src[attr]
                elif ("." in attr or "[" in attr):
                    try:
                        parts = _partiql_parse_path(attr)
                    except ValueError:
                        continue
                    leaf = _partiql_get_path(src, parts)
                    if leaf is not None:
                        key = next((seg for seg in reversed(parts) if isinstance(seg, str)), attr)
                        row[key] = leaf
            items.append(row)
        else:
            items.append(_index_view(it))

    payload = {"Items": items}
    if next_token:
        payload["NextToken"] = next_token
    rc = data.get("ReturnConsumedCapacity", "NONE")
    if rc != "NONE":
        table_units = rate * len(page) if reach_back else 0.0
        total = rate + table_units
        cap = {"TableName": table.get("TableName"), "CapacityUnits": total}
        if rc == "INDEXES":
            cap["Table"] = {"CapacityUnits": table_units}
            arm = {"GlobalSecondaryIndexes" if is_gsi else "LocalSecondaryIndexes": {index_name: {"CapacityUnits": rate}}}
            cap.update(arm)
        payload["ConsumedCapacity"] = cap
    return 200, {"Content-Type": "application/x-amz-json-1.0"}, json.dumps(payload, ensure_ascii=False).encode("utf-8")


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
                elif "." in attr or "[" in attr:
                    # Document path: the result column is named by the final
                    # path segment (SELECT mymap.nested -> {nested: ...}).
                    try:
                        parts = _partiql_parse_path(attr)
                    except ValueError:
                        continue
                    leaf = _partiql_get_path(it, parts)
                    if leaf is not None:
                        key = next((seg for seg in reversed(parts) if isinstance(seg, str)), attr)
                        proj[key] = leaf
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
    if _item_size_bytes(item) > _DDB_ITEM_MAX_BYTES:
        return error_response_json("ValidationException", _ITEM_SIZE_MSG, 400)
    _set_item(table, pk_val, sk_val, item)
    return json_response({})


def _partiql_key_target(table, parsed):
    """Return (pk_key, sk_key, non_key_fn, error_resp).

    AWS PartiQL UPDATE/DELETE require equality conditions on every primary key
    attribute. Non-key clauses act as a conditional check on the targeted item
    — failure → ConditionalCheckFailedException, not a silent no-op.
    """
    conditions = parsed.get("conditions") or []
    pk_attr = table.get("pk_name")
    sk_attr = table.get("sk_name")

    pk_typed = None
    sk_typed = None
    rest = []
    for attr, op, val in conditions:
        if attr == pk_attr and op == "=":
            pk_typed = val
        elif sk_attr and attr == sk_attr and op == "=":
            sk_typed = val
        else:
            rest.append((attr, op, val))

    if pk_typed is None or (sk_attr and sk_typed is None):
        return None, None, None, error_response_json(
            "ValidationException",
            "Where clause does not contain a mandatory equality on all key attributes",
            400,
        )

    def non_key_fn(item):
        # Each predicate is a fail-fast check: on a pass we fall through to the
        # next condition rather than returning, so every predicate in a
        # multi-condition WHERE is evaluated (not just the first special op).
        for attr, op, val in rest:
            if op == "begins_with":
                iv = item.get(attr)
                if iv is None or "S" not in iv:
                    return False
                prefix = val.get("S", "") if isinstance(val, dict) else str(val)
                if not iv["S"].startswith(prefix):
                    return False
            elif op == "not_begins_with":
                iv = item.get(attr)
                if iv is not None and "S" in iv:
                    prefix = val.get("S", "") if isinstance(val, dict) else str(val)
                    if iv["S"].startswith(prefix):
                        return False
            elif op == "is_missing":
                if item.get(attr) is not None:
                    return False
            elif op == "is_not_missing":
                if item.get(attr) is None:
                    return False
            elif op == "IN":
                iv = item.get(attr)
                if iv is None or not any(_ddb_equals(iv, v) for v in val):
                    return False
            else:
                if not _compare_ddb(item.get(attr), op, val):
                    return False
        return True

    pk_key = _extract_key_val(pk_typed)
    sk_key = _extract_key_val(sk_typed) if sk_attr else "__no_sort__"
    return pk_key, sk_key, non_key_fn, None


def _partiql_parse_path(raw):
    """Parse a PartiQL document path: 'profile.sub' -> ["profile", "sub"],
    'tags[0]' -> ["tags", 0]. Attribute names may be double-quoted."""
    import re as _re
    parts = []
    for seg in raw.split('.'):
        seg = seg.strip().strip('"')
        m = _re.match(r'^([^\[\]]+)((?:\[\d+\])*)$', seg)
        if not m:
            raise ValueError(f"Invalid document path: {raw}")
        parts.append(m.group(1))
        for idx in _re.findall(r'\[(\d+)\]', m.group(2)):
            parts.append(int(idx))
    return parts


def _partiql_get_path(item, parts):
    if item is None:
        return None
    node = item.get(parts[0])
    for part in parts[1:]:
        if node is None:
            return None
        if isinstance(part, str):
            node = node.get("M", {}).get(part) if isinstance(node, dict) else None
        else:
            lst = node.get("L") if isinstance(node, dict) else None
            node = lst[part] if lst is not None and part < len(lst) else None
    return node


def _partiql_walk_parent(item, parts):
    """The AV container holding the leaf of `parts`, or None if the path's
    ancestors don't exist / have the wrong shape."""
    node = None
    for i, part in enumerate(parts[:-1]):
        if i == 0:
            node = item.get(part)
        elif isinstance(part, str):
            node = node["M"].get(part) if (isinstance(node, dict) and "M" in node) else None
        else:
            lst = node.get("L") if isinstance(node, dict) else None
            node = lst[part] if lst is not None and part < len(lst) else None
        if node is None:
            return None
    return node


_PARTIQL_BAD_PATH = "The document path provided in the update expression is invalid for update"


def _partiql_set_path(item, parts, val):
    """Apply a SET along a document path. Returns an error message or None.
    A list index at or past the end appends (AWS clamps); a missing ancestor
    (including SET tags[0] on an absent attribute) is rejected, not created."""
    if len(parts) == 1:
        item[parts[0]] = val
        return None
    parent = _partiql_walk_parent(item, parts)
    if parent is None:
        return _PARTIQL_BAD_PATH
    leaf = parts[-1]
    if isinstance(leaf, str):
        if not (isinstance(parent, dict) and "M" in parent):
            return _PARTIQL_BAD_PATH
        parent["M"][leaf] = val
    else:
        if not (isinstance(parent, dict) and "L" in parent):
            return _PARTIQL_BAD_PATH
        lst = parent["L"]
        if leaf < len(lst):
            lst[leaf] = val
        else:
            lst.append(val)
    return None


def _partiql_remove_path(item, parts):
    """Apply a REMOVE along a document path (missing paths are no-ops)."""
    if len(parts) == 1:
        item.pop(parts[0], None)
        return None
    parent = _partiql_walk_parent(item, parts)
    if parent is None:
        return None
    leaf = parts[-1]
    if isinstance(leaf, str):
        if isinstance(parent, dict) and "M" in parent:
            parent["M"].pop(leaf, None)
    else:
        if isinstance(parent, dict) and "L" in parent and leaf < len(parent["L"]):
            del parent["L"][leaf]
    return None


def _partiql_modified_items(item_before, item_after, paths, want_old):
    """RETURNING MODIFIED OLD/NEW * per real DynamoDB: only the changed leaf
    comes back — nested map paths as a one-leaf fragment, list indices packed
    densely in ascending index order (measured by paritysuite). Reading the
    post-image at the written path naturally yields the shifted element after
    a list REMOVE and nothing for a clamped out-of-range append."""
    frag = {}
    list_packs = {}
    for parts in paths:
        top = parts[0]
        old_leaf = _partiql_get_path(item_before, parts)
        new_leaf = _partiql_get_path(item_after, parts)
        if old_leaf == new_leaf:
            continue
        leaf = old_leaf if want_old else new_leaf
        if leaf is None:
            continue
        if len(parts) == 1:
            frag[top] = leaf
        elif len(parts) == 2 and isinstance(parts[1], int):
            list_packs.setdefault(top, []).append((parts[1], leaf))
        elif all(isinstance(part, str) for part in parts):
            node = frag.setdefault(top, {"M": {}})
            if not (isinstance(node, dict) and set(node.keys()) == {"M"}):
                continue
            cur = node
            for part in parts[1:-1]:
                cur = cur["M"].setdefault(part, {"M": {}})
            cur["M"][parts[-1]] = leaf
        else:
            src = item_before if want_old else item_after
            v = (src or {}).get(top)
            if v is not None:
                frag[top] = v
    for top, pairs in list_packs.items():
        pairs.sort(key=lambda x: x[0])
        frag[top] = {"L": [leaf for _, leaf in pairs]}
    return [frag] if frag else []


def _build_partiql_returning(item_before, item_after, returning_clause):
    """Build the Items list for a RETURNING clause response.

    Supported: ALL OLD *, ALL NEW *, MODIFIED OLD *, MODIFIED NEW *
    Returns (items_list, error_or_None)
    """
    if not returning_clause:
        return [], None
    r = returning_clause.upper().strip()
    if r == "ALL OLD *":
        return [item_before] if item_before else [], None
    elif r == "ALL NEW *":
        return [item_after] if item_after else [], None
    elif r == "MODIFIED NEW *":
        if not item_before or not item_after:
            return [], None
        changed = {k: v for k, v in item_after.items()
                   if k not in item_before or item_before[k] != v}
        return [changed] if changed else [], None
    elif r == "MODIFIED OLD *":
        if not item_before or not item_after:
            return [], None
        changed = {k: item_before[k] for k in item_before
                   if k not in item_after or item_after[k] != item_before[k]}
        return [changed] if changed else [], None
    else:
        return None, f"Invalid returning clause: {returning_clause}"


def _partiql_update(table, parsed):
    set_attrs = parsed.get("set_attrs", {})
    remove_attrs = parsed.get("remove_attrs", [])
    returning = parsed.get("returning")

    if not parsed.get("where_fn") and not parsed.get("conditions"):
        return error_response_json("ValidationException",
                                   "UPDATE requires a WHERE clause", 400)
    if not set_attrs and not remove_attrs:
        return error_response_json("ValidationException",
                                   "UPDATE requires SET or REMOVE clause", 400)

    # Validate RETURNING clause
    if returning:
        valid_returning = {"ALL OLD *", "ALL NEW *", "MODIFIED OLD *", "MODIFIED NEW *"}
        if returning.upper().strip() not in valid_returning:
            return error_response_json("ValidationException",
                f"Invalid returning clause: RETURNING {returning} is not supported", 400)

    pk_key, sk_key, non_key_fn, err = _partiql_key_target(table, parsed)
    if err:
        return err

    item = table["items"].get(pk_key, {}).get(sk_key)
    # UPDATE on a missing key → ConditionalCheckFailedException (not upsert).
    if item is None:
        return _conditional_check_failed({}, None)
    if not non_key_fn(item):
        return _conditional_check_failed({}, item)

    item_before = copy.deepcopy(item)
    work = copy.deepcopy(item)
    applied_paths = []
    for attr, val in set_attrs.items():
        # Handle arithmetic: {"__partiql_arith": {"attr": "n", "op": "+", "val": {"N": "1"}}}
        if isinstance(val, dict) and "__partiql_arith" in val:
            arith = val["__partiql_arith"]
            src_attr = arith["attr"]
            op = arith["op"]
            operand = arith["val"]
            cur = work.get(src_attr)
            if cur and "N" in cur and "N" in operand:
                from decimal import Decimal
                result_n = Decimal(cur["N"]) + Decimal(operand["N"]) if op == "+" else Decimal(cur["N"]) - Decimal(operand["N"])
                work[attr] = {"N": str(result_n)}
            else:
                work[attr] = operand  # fallback: just set to operand
            applied_paths.append([attr])
            continue
        try:
            parts = _partiql_parse_path(attr)
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
        err_msg = _partiql_set_path(work, parts, val)
        if err_msg:
            return error_response_json("ValidationException", err_msg, 400)
        applied_paths.append(parts)
    for attr in remove_attrs:
        try:
            parts = _partiql_parse_path(attr)
        except ValueError as exc:
            return error_response_json("ValidationException", str(exc), 400)
        _partiql_remove_path(work, parts)
        applied_paths.append(parts)
    if _item_size_bytes(work) > _DDB_ITEM_MAX_BYTES:
        return error_response_json("ValidationException", _UPDATE_SIZE_MSG, 400)
    # Nothing failed — commit the working copy.
    _set_item(table, pk_key, sk_key, work)
    item_after = work

    if returning:
        r = returning.upper().strip()
        if r in ("MODIFIED OLD *", "MODIFIED NEW *"):
            items_list = _partiql_modified_items(item_before, item_after, applied_paths,
                                                 want_old=(r == "MODIFIED OLD *"))
        elif r == "ALL OLD *":
            items_list = [item_before] if item_before else []
        else:
            items_list = [copy.deepcopy(item_after)]
        return json_response({"Items": items_list})
    return json_response({"Items": []})


def _partiql_delete(table, parsed):
    returning = parsed.get("returning")
    if not parsed.get("where_fn") and not parsed.get("conditions"):
        return error_response_json("ValidationException",
                                   "DELETE requires a WHERE clause", 400)

    # Validate RETURNING clause — DELETE only supports ALL OLD *
    if returning:
        r = returning.upper().strip()
        if r not in ("ALL OLD *",):
            return error_response_json("ValidationException",
                f"Invalid returning clause: RETURNING {returning.strip()}. Only RETURNING ALL OLD * is allowed in DELETE statements.", 400)

    pk_key, sk_key, non_key_fn, err = _partiql_key_target(table, parsed)
    if err:
        return err

    item = table["items"].get(pk_key, {}).get(sk_key)

    # If item doesn't exist and there are non-key predicates, that's a silent no-op.
    # If item doesn't exist and there are NO non-key predicates, also a silent no-op.
    if item is None:
        if returning and returning.upper().strip() == "ALL OLD *":
            return json_response({"Items": []})
        return json_response({"Items": []})

    # Non-key predicate fails → ConditionalCheckFailed
    if not non_key_fn(item):
        return _conditional_check_failed({}, item)

    item_before = copy.deepcopy(item) if returning else None
    _remove_item(table, pk_key, sk_key)

    if returning and returning.upper().strip() == "ALL OLD *":
        return json_response({"Items": [item_before] if item_before else []})
    return json_response({"Items": []})


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
        raw_stmt = stmt.get("Statement", "")
        # Pre-check: RETURNING clause in batch is surfaced as per-statement error.
        import re as _re
        _ret_match = _re.search(r'\s+RETURNING\s+', raw_stmt, _re.IGNORECASE)
        if _ret_match:
            _ret_orig = raw_stmt[_ret_match.end():].strip().rstrip(';').strip()
            _ret_clause = _ret_orig.upper()
            _valid_ret = {"ALL OLD *", "ALL NEW *", "MODIFIED OLD *", "MODIFIED NEW *"}
            # For DELETE, only ALL OLD * is valid (verbatim AWS message,
            # measured by paritysuite).
            _is_delete = raw_stmt.strip().upper().startswith("DELETE")
            if _is_delete and _ret_clause not in ("ALL OLD *",):
                responses.append({"Error": {"Code": "ValidationError",
                    "Message": f"Invalid returning clause: RETURNING {_ret_orig}. Only RETURNING ALL OLD * is allowed in DELETE statements."}})
                continue
            if _ret_clause not in _valid_ret:
                responses.append({"Error": {"Code": "ValidationError",
                    "Message": f"Invalid RETURNING clause: {_ret_clause}"}})
                continue
        # Parse up front — parse failures and batch-only SELECT constraints are
        # per-member rejections that never reach a table (no TableName echoed,
        # no capacity charged).
        try:
            parsed = _parse_partiql(raw_stmt, stmt.get("Parameters", []))
        except ValueError as exc:
            msg = str(exc)
            if msg.startswith(("Unsupported PartiQL statement", "Could not parse")):
                msg = "Statement wasn't well formed, can't be processed: Expected data manipulation"
            responses.append({"Error": {"Code": "ValidationError", "Message": msg}})
            continue
        if parsed["op"] == "SELECT":
            # A batch SELECT must name the full table primary key with equality,
            # and an index-qualified read is not reachable from a batch at all.
            key_err = None
            tbl = _tables.get(parsed["table"])
            if parsed.get("index"):
                key_err = True
            elif tbl is not None:
                eq_attrs = {attr for attr, op_, _ in (parsed.get("conditions") or []) if op_ == "="}
                need = {tbl.get("pk_name")} | ({tbl.get("sk_name")} if tbl.get("sk_name") else set())
                key_err = not need.issubset(eq_attrs)
            if key_err:
                responses.append({"Error": {"Code": "ValidationError",
                    "Message": "Select statements within BatchExecuteStatement must specify the primary key in the where clause"}})
                continue
        sub = {"Statement": raw_stmt, "Parameters": stmt.get("Parameters", [])}
        status, _, body = _execute_statement(sub)
        try:
            payload = json.loads(body)
        except (TypeError, ValueError):
            payload = {}
        ran = False
        if status == 200:
            entry: dict = {"TableName": parsed["table"]}
            items = payload.get("Items")
            if items:
                entry["Item"] = items[0]
            responses.append(entry)
            ran = True
        else:
            err_code = payload.get("__type", "ValidationException")
            err_msg = payload.get("message", "")
            # Per-statement error code: AWS uses "ValidationError" for validation
            # errors, and the short name (sans Exception) for other errors.
            short = err_code.split("#")[-1]
            if short == "ValidationException":
                short = "ValidationError"
            elif short.endswith("Exception"):
                short = short[: -len("Exception")]
            err_entry: dict = {"Error": {"Code": short, "Message": err_msg}}
            # TableName is echoed only on a member that ran and failed during
            # execution — not on one rejected before it reached its table.
            if short in ("ConditionalCheckFailed", "DuplicateItem"):
                err_entry["TableName"] = parsed["table"]
                ran = True
            responses.append(err_entry)
        if ran:
            # Reads rate by the member's own ConsistentRead; writes cost 1 WCU.
            if parsed["op"] == "SELECT":
                units = 1.0 if stmt.get("ConsistentRead") else 0.5
            else:
                units = 1.0
            per_table_units[parsed["table"]] = per_table_units.get(parsed["table"], 0.0) + units
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
        prior = _txn_token_lookup(crt)
        signature = {k: v for k, v in data.items() if k != "ClientRequestToken"}
        if prior is not None:
            if prior.get("signature") == signature:
                replay = {k: v for k, v in prior.get("response", {}).items() if k != "ConsumedCapacity"}
                if data.get("ReturnConsumedCapacity", "NONE") != "NONE":
                    consumed = []
                    for tname, sizes in (prior.get("sizes") or {}).items():
                        read_units = sum(2.0 * max(1.0, float(-(-sz // 4096))) for sz in sizes)
                        consumed.append({"TableName": tname, "CapacityUnits": read_units,
                                         "ReadCapacityUnits": read_units})
                    if consumed:
                        replay["ConsumedCapacity"] = consumed
                return json_response(replay)
            return error_response_json("IdempotentParameterMismatchException",
                "Request token already in use for another request with a different payload", 400)

    # Parse every statement and detect duplicate-INSERT pre-emptively so the
    # whole transaction is rejected (matches AWS semantics — all-or-nothing).
    parsed_list = []
    for stmt_index, stmt in enumerate(statements):
        statement = stmt.get("Statement", "")
        parameters = stmt.get("Parameters", [])
        # RETURNING clause is not allowed in transaction statements.
        import re as _re2
        if _re2.search(r'\s+RETURNING\s+', statement, _re2.IGNORECASE):
            return error_response_json("ValidationException",
                f"Validation failed in TransactStatements[{stmt_index}]: RETURNING clause is not supported in ExecuteTransaction.", 400)
        try:
            parsed = _parse_partiql(statement, parameters)
        except ValueError as e:
            return error_response_json("ValidationException", str(e), 400)
        if parsed.get("op") == "SELECT" and parsed.get("index"):
            return error_response_json("ValidationException",
                f"Validation failed in TransactStatements[{stmt_index}]: Reads on indices are not supported within transactions.", 400)
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
                _items_replaced(tbl)
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
    # Per-table item sizes: a transactional write costs 2 x ceil(size/1KB) WCU,
    # a same-token replay 2 x ceil(size/4KB) RCU (measured, paritysuite).
    txn_sizes: dict = {}
    for parsed, _ in parsed_list:
        tname = parsed["table"]
        tbl = _tables.get(tname)
        sz = 0
        if tbl is not None and parsed["op"] != "SELECT":
            try:
                pk_key, sk_key, _fn, _err = _partiql_key_target(tbl, parsed)
                if _err is None:
                    it = tbl["items"].get(pk_key, {}).get(sk_key)
                    sz = _item_size_bytes(it) if it else 0
            except Exception:
                sz = 0
        txn_sizes.setdefault(tname, []).append(sz)
    if rc != "NONE":
        consumed = []
        for tname, sizes in txn_sizes.items():
            write_units = sum(2.0 * _capacity_kb(sz) for sz in sizes)
            consumed.append({"TableName": tname, "CapacityUnits": write_units,
                             "WriteCapacityUnits": write_units})
        result["ConsumedCapacity"] = consumed
    if crt:
        _txn_token_store(crt, {"signature": signature, "response": result, "sizes": txn_sizes})
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


def _partiql_from_path_error(s):
    """AWS checks the FROM path before resolving it: no empty component, at most two."""
    import re
    m = re.search(r'\bFROM\s+((?:"[^"]*"|[A-Za-z0-9_\-]+)(?:\s*\.\s*(?:"[^"]*"|[A-Za-z0-9_\-]+))*)', s, re.IGNORECASE)
    if not m:
        return None
    components = re.findall(r'"[^"]*"|[A-Za-z0-9_\-]+', m.group(1))
    if any(c == '""' for c in components):
        return "Path component cannot be an empty string"
    if len(components) > 2:
        return "A path may contain at most 2 components in the FROM clause"
    return None


def _parse_partiql_select(s, parameters):
    import re
    path_error = _partiql_from_path_error(s)
    if path_error:
        raise ValueError(path_error)
    # SELECT <projections> FROM <table> [WHERE <condition>]
    m = re.match(
        r'SELECT\s+(.*?)\s+FROM\s+("?[A-Za-z0-9_.\-]+"?(?:\s*\.\s*"[A-Za-z0-9_.\-]+")?)(?:\s+WHERE\s+(.+))?$',
        s, re.IGNORECASE | re.DOTALL,
    )
    if not m:
        raise ValueError(f"Could not parse SELECT statement: {s}")

    proj_str = m.group(1).strip()
    from_str = m.group(2).strip()
    where_str = m.group(3)
    # FROM "table"."index" — the index qualifier must be quoted, so a plain
    # table name containing dots keeps its existing meaning.
    index_name = None
    qm = re.match(r'^"?([A-Za-z0-9_.\-]+?)"?\s*\.\s*"([A-Za-z0-9_.\-]+)"$', from_str)
    if '"' in from_str and qm:
        table_name = qm.group(1)
        index_name = qm.group(2)
    else:
        table_name = from_str.strip('"')

    projections = None
    if proj_str != "*":
        projections = [p.strip().strip('"') for p in proj_str.split(",")]

    where_fn, conditions = (_build_partiql_where(where_str, parameters)
                            if where_str else (None, []))

    return {"op": "SELECT", "table": table_name, "index": index_name,
            "projections": projections, "where_fn": where_fn, "conditions": conditions}


def _parse_partiql_insert(s, parameters):
    import re
    # INSERT INTO <table> VALUE { ... }
    if re.search(r'INTO\s+"[^"]+"\s*\.\s*"', s, re.IGNORECASE):
        raise ValueError(
            "Statement wasn't well formed, can't be processed: FROM clause may only contain a single table name")
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
    # UPDATE <table> SET <assignments> [REMOVE <paths>] WHERE <condition> [RETURNING <clause>]
    # Strip RETURNING clause first (it's always at the end).
    returning = None
    ret_match = re.search(r'\s+RETURNING\s+(.+)$', s, re.IGNORECASE)
    if ret_match:
        returning = ret_match.group(1).strip()
        s = s[:ret_match.start()].strip()

    if re.match(r'UPDATE\s+"[^"]+"\s*\.\s*"', s, re.IGNORECASE):
        raise ValueError("This operation is not supported on an index")
    # Try SET ... WHERE form first.
    m = re.match(
        r"UPDATE\s+\"?([A-Za-z0-9_.\-]+)\"?\s+SET\s+(.+?)\s+WHERE\s+(.+)$",
        s, re.IGNORECASE | re.DOTALL,
    )
    if m:
        table_name = m.group(1).strip()
        set_str = m.group(2).strip()
        where_str = m.group(3).strip()
        remove_str = None
        # Check for embedded REMOVE clause within SET string (SET x=v REMOVE y WHERE ...)
        rem_in_set = re.search(r'\s+REMOVE\s+(.+)$', set_str, re.IGNORECASE)
        if rem_in_set:
            remove_str = rem_in_set.group(1).strip()
            set_str = set_str[:rem_in_set.start()].strip()
        set_attrs = {}
        param_idx = [0]
        for assignment in _split_top_level(set_str, ','):
            parts = assignment.split("=", 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid SET assignment: {assignment}")
            attr = parts[0].strip().strip('"')
            val_str = parts[1].strip()
            set_attrs[attr] = _parse_partiql_literal(val_str, parameters, param_idx)
        where_fn, conditions = _build_partiql_where(where_str, parameters, param_idx)
        return {"op": "UPDATE", "table": table_name, "set_attrs": set_attrs,
                "remove_attrs": [r.strip() for r in remove_str.split(",")] if remove_str else [],
                "where_fn": where_fn, "conditions": conditions, "returning": returning}

    # Try REMOVE ... WHERE form.
    m = re.match(
        r"UPDATE\s+\"?([A-Za-z0-9_.\-]+)\"?\s+REMOVE\s+(.+?)\s+WHERE\s+(.+)$",
        s, re.IGNORECASE | re.DOTALL,
    )
    if m:
        table_name = m.group(1).strip()
        remove_str = m.group(2).strip()
        where_str = m.group(3).strip()
        param_idx = [0]
        where_fn, conditions = _build_partiql_where(where_str, parameters, param_idx)
        return {"op": "UPDATE", "table": table_name, "set_attrs": {},
                "remove_attrs": [r.strip() for r in remove_str.split(",")],
                "where_fn": where_fn, "conditions": conditions, "returning": returning}

    raise ValueError(f"Could not parse UPDATE statement: {s}")


def _parse_partiql_delete(s, parameters):
    import re
    # Strip RETURNING clause first.
    returning = None
    ret_match = re.search(r'\s+RETURNING\s+(.+)$', s, re.IGNORECASE)
    if ret_match:
        returning = ret_match.group(1).strip()
        s = s[:ret_match.start()].strip()

    if re.match(r'DELETE\s+FROM\s+"[^"]+"\s*\.\s*"', s, re.IGNORECASE):
        raise ValueError("This operation is not supported on an index")
    # DELETE FROM <table> WHERE <condition>
    m = re.match(
        r"DELETE\s+FROM\s+\"?([A-Za-z0-9_.\-]+)\"?\s+WHERE\s+(.+)$",
        s, re.IGNORECASE | re.DOTALL,
    )
    if not m:
        raise ValueError(f"Could not parse DELETE statement: {s}")
    table_name = m.group(1).strip()
    where_str = m.group(2).strip()
    where_fn, conditions = _build_partiql_where(where_str, parameters)
    return {"op": "DELETE", "table": table_name, "where_fn": where_fn,
            "conditions": conditions, "returning": returning}


def _eval_partiql_pred(item, attr, op, val):
    """Evaluate one PartiQL predicate against an item. `op` may carry a
    "NOT:" prefix for a negated comparison."""
    if isinstance(op, str) and op.startswith("NOT:"):
        return not _eval_partiql_pred(item, attr, op[4:], val)
    item_val = item.get(attr)
    if op == "begins_with":
        if item_val is None or not isinstance(item_val, dict) or "S" not in item_val:
            return False
        prefix = val.get("S", "") if isinstance(val, dict) else str(val)
        return item_val["S"].startswith(prefix)
    if op == "not_begins_with":
        if item_val is not None and isinstance(item_val, dict) and "S" in item_val:
            prefix = val.get("S", "") if isinstance(val, dict) else str(val)
            return not item_val["S"].startswith(prefix)
        return True
    if op == "is_missing":
        return item_val is None
    if op == "is_not_missing":
        return item_val is not None
    if op == "IN":
        return item_val is not None and any(_ddb_equals(item_val, v) for v in val)
    return _compare_ddb(item_val, op, val)


def _build_partiql_where(where_str, parameters, param_idx=None):
    """Build a predicate function + structural conditions list for a PartiQL
    WHERE. Top-level OR splits into branches (DNF); the conditions list is
    only populated for the single-branch form, which is what key targeting
    and batch key validation consume."""
    if not where_str or not where_str.strip():
        return None, []
    if param_idx is None:
        param_idx = [0]

    branches = []
    for branch_str in _split_top_level_by_or(where_str):
        b = branch_str.strip()
        # Strip one pair of parens wrapping the whole branch.
        if b.startswith("(") and b.endswith(")"):
            depth = 0
            wraps = True
            for k, ch in enumerate(b):
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0 and k != len(b) - 1:
                        wraps = False
                        break
            if wraps:
                b = b[1:-1].strip()
        branches.append(_parse_partiql_conditions(b, parameters, param_idx))

    conditions = branches[0] if len(branches) == 1 else []

    def where_fn(item):
        return any(all(_eval_partiql_pred(item, attr, op, val) for attr, op, val in branch)
                   for branch in branches)

    return where_fn, conditions


def _split_top_level_by_or(s):
    """Split a WHERE string on OR at the top level (respecting parens/quotes)."""
    parts = []
    depth = 0
    in_str = None
    current = []
    i = 0
    s_up = s.upper()
    while i < len(s):
        ch = s[i]
        if in_str:
            current.append(ch)
            if ch == in_str:
                in_str = None
            i += 1
            continue
        if ch in ("'", '"'):
            in_str = ch
            current.append(ch)
            i += 1
            continue
        if ch in ('(', '[', '{'):
            depth += 1
            current.append(ch)
            i += 1
            continue
        if ch in (')', ']', '}'):
            depth -= 1
            current.append(ch)
            i += 1
            continue
        if depth == 0 and s_up[i:i+3] == ' OR' and (i + 3 >= len(s) or s[i+3] == ' '):
            parts.append("".join(current))
            current = []
            i += 3
            continue
        current.append(ch)
        i += 1
    if current:
        parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]

def _parse_partiql_conditions(where_str, parameters, param_idx):
    """Parse WHERE conditions joined by AND. Returns list of (attr, op, ddb_value)."""
    import re
    conditions = []
    # Split on AND (case-insensitive, word boundary) — but not inside parens.
    parts = _split_top_level_by_and(where_str)
    for part in parts:
        part = part.strip()
        # begins_with(attr, val)
        m = re.match(r'begins_with\s*\(\s*"?([A-Za-z0-9_.\-]+)"?\s*,\s*(.+)\s*\)\s*$', part, re.IGNORECASE)
        if m:
            attr = m.group(1)
            val = _parse_partiql_literal(m.group(2).strip(), parameters, param_idx)
            conditions.append((attr, "begins_with", val))
            continue
        # NOT begins_with(attr, val)
        m = re.match(r'NOT\s+begins_with\s*\(\s*"?([A-Za-z0-9_.\-]+)"?\s*,\s*(.+)\s*\)\s*$', part, re.IGNORECASE)
        if m:
            attr = m.group(1)
            val = _parse_partiql_literal(m.group(2).strip(), parameters, param_idx)
            conditions.append((attr, "not_begins_with", val))
            continue
        # attr IS MISSING
        m = re.match(r'"?([A-Za-z0-9_.\-]+)"?\s+IS\s+MISSING\s*$', part, re.IGNORECASE)
        if m:
            conditions.append((m.group(1), "is_missing", None))
            continue
        # attr IS NOT MISSING
        m = re.match(r'"?([A-Za-z0-9_.\-]+)"?\s+IS\s+NOT\s+MISSING\s*$', part, re.IGNORECASE)
        if m:
            conditions.append((m.group(1), "is_not_missing", None))
            continue
        # attr IN ('v1', 'v2', ...)
        m = re.match(r'"?([A-Za-z0-9_.\-]+)"?\s+IN\s*\[(.+)\]\s*$', part, re.IGNORECASE)
        if not m:
            m = re.match(r'"?([A-Za-z0-9_.\-]+)"?\s+IN\s*\((.+)\)\s*$', part, re.IGNORECASE)
        if m:
            attr = m.group(1)
            vals = [_parse_partiql_literal(v.strip(), parameters, param_idx)
                    for v in _split_top_level(m.group(2), ',')]
            conditions.append((attr, "IN", vals))
            continue
        # NOT <comparison> — negated predicate (begins_with has its own form).
        mnot = re.match(r'NOT\s+("?[A-Za-z0-9_.\-]+"?\s*(?:=|<>|!=|<=|>=|<|>).+)$', part, re.IGNORECASE)
        if mnot and not re.match(r'NOT\s+begins_with', part, re.IGNORECASE):
            inner = _parse_partiql_conditions(mnot.group(1), parameters, param_idx)
            for attr_i, op_i, val_i in inner:
                conditions.append((attr_i, f"NOT:{op_i}", val_i))
            continue
        # attr BETWEEN lo AND hi (the inner AND is re-joined by the splitter)
        m = re.match(r'"?([A-Za-z0-9_.\-]+)"?\s+BETWEEN\s+(.+?)\s+AND\s+(.+)$', part, re.IGNORECASE)
        if m:
            attr = m.group(1)
            lo = _parse_partiql_literal(m.group(2).strip(), parameters, param_idx)
            hi = _parse_partiql_literal(m.group(3).strip(), parameters, param_idx)
            for bound in (lo, hi):
                bt = _ddb_type(bound)
                if bt and bt not in ("S", "N", "B"):
                    raise ValueError(
                        f"Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: {bt}")
            conditions.append((attr, ">=", lo))
            conditions.append((attr, "<=", hi))
            continue
        # Standard comparison
        m = re.match(r'"?([A-Za-z0-9_.\-]+)"?\s*(=|<>|!=|<=|>=|<|>)\s*(.+)$', part)
        if not m:
            raise ValueError(f"Could not parse WHERE condition: {part}")
        attr = m.group(1)
        op = m.group(2)
        if op == '!=':
            op = '<>'
        val_str = m.group(3).strip()
        val = _parse_partiql_literal(val_str, parameters, param_idx)
        if op in ("<", "<=", ">", ">="):
            # Ordering only exists for S / N / B operands; AWS rejects the
            # statement naming the operator as written (measured, paritysuite).
            vt = _ddb_type(val)
            if vt and vt not in ("S", "N", "B"):
                raise ValueError(
                    f"Incorrect operand type for operator or function; operator or function: {op}, operand type: {vt}")
        conditions.append((attr, op, val))
    return conditions


def _split_top_level_by_and(s):
    """Split a WHERE string on AND at the top level (respecting parens/quotes)."""
    # Use the existing _split_top_level approach but with AND as delimiter
    parts = []
    depth = 0
    in_str = None
    current = []
    i = 0
    s_up = s.upper()
    while i < len(s):
        ch = s[i]
        if in_str:
            current.append(ch)
            if ch == in_str:
                in_str = None
            i += 1
            continue
        if ch in ("'", '"'):
            in_str = ch
            current.append(ch)
            i += 1
            continue
        if ch in ('(', '[', '{'):
            depth += 1
            current.append(ch)
            i += 1
            continue
        if ch in (')', ']', '}'):
            depth -= 1
            current.append(ch)
            i += 1
            continue
        # Check for AND keyword at depth 0 — but the AND that separates a
        # BETWEEN's bounds belongs to the BETWEEN, not the conjunction.
        if depth == 0 and s_up[i:i+4] == ' AND' and (i + 4 >= len(s) or s[i+4] == ' '):
            cur_up = "".join(current).upper()
            between_pending = (' BETWEEN ' in cur_up
                               and ' AND ' not in cur_up.split(' BETWEEN ', 1)[1] + ' ')
            if not between_pending:
                parts.append("".join(current))
                current = []
                i += 4  # skip " AND"
                continue
        current.append(ch)
        i += 1
    if current:
        parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]


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

    # Arithmetic expression: attr +/- number (e.g. "n + 1")
    arith_m = re.match(r'^"?([A-Za-z0-9_.\-]+)"?\s*([+\-])\s*(.+)$', val_str)
    if arith_m:
        left_attr = arith_m.group(1)
        operator = arith_m.group(2)
        right_str = arith_m.group(3).strip()
        right_val = _parse_partiql_literal(right_str, parameters, param_idx)
        return {"__partiql_arith": {"attr": left_attr, "op": operator, "val": right_val}}

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

def _accumulate_write_capacity(cap, table, old_item, new_item):
    """Fold one write's table + per-index units into a batch accumulator."""
    old_size = _item_size_bytes(old_item) if old_item else 0
    new_size = _item_size_bytes(new_item) if new_item else 0
    cap["table"] += _capacity_kb(max(old_size, new_size))
    for kind, key in (("GlobalSecondaryIndexes", "gsi"), ("LocalSecondaryIndexes", "lsi")):
        for idx in table.get(kind, []) or []:
            u = _index_write_units(table, idx, old_item, new_item)
            if u:
                cap[key][idx["IndexName"]] = cap[key].get(idx["IndexName"], 0.0) + u
    for index_name, size in _vector_write_bytes(table, old_item, new_item).items():
        vector = cap.setdefault("vector", {})
        vector[index_name] = vector.get(index_name, 0.0) + size


def _batch_write_item(data):
    _batch_capacity: dict = {}
    request_items = data.get("RequestItems")
    if not request_items:
        return error_response_json("ValidationException",
            "1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    # Total request count cap (25 per BatchWriteItem call).
    total = sum(len(v) for v in request_items.values())
    if total > _DDB_BATCH_WRITE_MAX:
        # AWS dumps the full RequestItems map in Java-toString shape:
        # `{<tableName>=[<WriteRequest>, ...]}`. Match the structural envelope.
        parts = [f"{tn}=[{', '.join(repr(r) for r in reqs)}]" for tn, reqs in request_items.items()]
        dump = "{" + ", ".join(parts) + "}"
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{dump}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: [Member must have length less than or equal to {_DDB_BATCH_WRITE_MAX}, Member must have length greater than or equal to 1]", 400)
    # Duplicate target key detection — AWS rejects two writes to the same key
    # in a single BatchWriteItem call.
    seen_keys = set()
    for table_name, requests in request_items.items():
        table = _tables.get(_normalize_table_name(table_name))
        if not table:
            return error_response_json(
                "ResourceNotFoundException",
                "Requested resource not found",
                400,
            )
        for req in requests:
            # Validate every member up front: AWS rejects the whole
            # BatchWriteItem call before applying anything, so a bad member
            # must not leave earlier members written.
            if "PutRequest" in req:
                item = req["PutRequest"].get("Item", {})
                err = _validate_item(item, table.get("pk_name"), table.get("sk_name"))
                if err:
                    return err
                _, _, key_err = _resolve_table_key_values(table, item, allow_extra=True)
                if key_err:
                    return key_err
                err = _validate_index_key_values(table, item)
                if err:
                    return err
                key_repr = (table_name,
                            _extract_key_val(item.get(table.get("pk_name") or "")),
                            _extract_key_val(item.get(table.get("sk_name") or "")) if table.get("sk_name") else None)
            elif "DeleteRequest" in req:
                key = req["DeleteRequest"].get("Key", {})
                _, _, key_err = _resolve_table_key_values(table, key, allow_extra=False)
                if key_err:
                    return key_err
                key_repr = (table_name,
                            _extract_key_val(key.get(table.get("pk_name") or "")),
                            _extract_key_val(key.get(table.get("sk_name") or "")) if table.get("sk_name") else None)
            else:
                continue
            if key_repr in seen_keys:
                return error_response_json("ValidationException",
                    "Provided list of item keys contains duplicates", 400)
            seen_keys.add(key_repr)
    unprocessed = {}
    for table_name, requests in request_items.items():
        table = _tables.get(_normalize_table_name(table_name))
        if not table:
            return error_response_json(
                "ResourceNotFoundException",
                "Requested resource not found",
                400,
            )
        cap = _batch_capacity.setdefault(table_name, {"table": 0.0, "gsi": {}, "lsi": {}})
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
                _set_item(table, pk_val, sk_val, item)
                _emit_stream_event(_normalize_table_name(table_name), "MODIFY" if old_item else "INSERT", old_item, item)
                _accumulate_write_capacity(cap, table, old_item, item)
            elif "DeleteRequest" in req:
                key = req["DeleteRequest"]["Key"]
                pk_val, sk_val, key_err = _resolve_table_key_values(table, key, allow_extra=False)
                if key_err:
                    return key_err
                old_item = table["items"].get(pk_val, {}).get(sk_val)
                _remove_item(table, pk_val, sk_val)
                if old_item:
                    _emit_stream_event(_normalize_table_name(table_name), "REMOVE", old_item, None)
                _accumulate_write_capacity(cap, table, old_item, None)
    result = {"UnprocessedItems": unprocessed}
    rc = data.get("ReturnConsumedCapacity", "NONE")
    if rc != "NONE":
        consumed = []
        for t in request_items:
            cap = _batch_capacity.get(t)
            if cap is None:
                continue
            total = cap["table"] + sum(cap["gsi"].values()) + sum(cap["lsi"].values())
            entry = {"TableName": t, "CapacityUnits": total}
            if rc == "INDEXES":
                entry["Table"] = {"CapacityUnits": cap["table"]}
                if cap["gsi"]:
                    entry["GlobalSecondaryIndexes"] = {n: {"CapacityUnits": u} for n, u in cap["gsi"].items()}
                if cap["lsi"]:
                    entry["LocalSecondaryIndexes"] = {n: {"CapacityUnits": u} for n, u in cap["lsi"].items()}
                if cap.get("vector"):
                    entry["VectorIndexes"] = {n: {"VectorWriteRequestBytes": b} for n, b in cap["vector"].items()}
            consumed.append(entry)
        result["ConsumedCapacity"] = consumed
    return json_response(result)


def _batch_get_item(data):
    request_items = data.get("RequestItems")
    if not request_items:
        return error_response_json("ValidationException",
            "1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    # Per-table key cap (100 per BatchGetItem call, per-table path in the error).
    for _bg_table_name, _bg_cfg in request_items.items():
        _bg_keys = _bg_cfg.get("Keys", [])
        if len(_bg_keys) > _DDB_BATCH_GET_MAX:
            return error_response_json("ValidationException",
                f"1 validation error detected: Value at 'RequestItems.{_bg_table_name}.member.Keys' failed to satisfy constraint: Member must have length less than or equal to {_DDB_BATCH_GET_MAX}", 400)
    # Non-existent table check before processing (AWS validates upfront).
    for table_name in request_items:
        if _normalize_table_name(table_name) not in _tables:
            return error_response_json("ResourceNotFoundException",
                "Requested resource not found", 400)
    # Duplicate-key rejection.
    for table_name, cfg in request_items.items():
        table = _tables[_normalize_table_name(table_name)]
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
    # One bad entry rejects the whole batch, and expression and non-expression
    # projections cannot be mixed even across tables.
    configs = [c for c in request_items.values() if isinstance(c, dict)]
    if any(c.get("ProjectionExpression") for c in configs) and any(c.get("AttributesToGet") for c in configs):
        return error_response_json("ValidationException",
            "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}", 400)
    for config in configs:
        err = _projection_overlap_response(config)
        if err:
            return err
    responses = {}
    unprocessed = {}
    for table_name, config in request_items.items():
        table = _tables.get(_normalize_table_name(table_name))
        if not table:
            unprocessed[table_name] = config
            continue
        responses[table_name] = []
        proj = config.get("ProjectionExpression")
        atg = config.get("AttributesToGet")
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
    for _txn_item in items_list if isinstance(items_list, list) else []:
        for _txn_op in (_txn_item.values() if isinstance(_txn_item, dict) else []):
            if isinstance(_txn_op, dict) and "TableName" in _txn_op:
                _txn_op["TableName"] = _normalize_table_name(_txn_op["TableName"])
    if not items_list:
        return error_response_json("ValidationException",
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    if len(items_list) > _DDB_TXN_WRITE_MAX_ITEMS:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '[{', '.join(repr(i) for i in items_list)}]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to {_DDB_TXN_WRITE_MAX_ITEMS}", 400)
    # An oversized expression in any member rejects the whole request up front.
    for _member in items_list:
        for _op in (_member.values() if isinstance(_member, dict) else []):
            if isinstance(_op, dict):
                err = _expression_size_error(_op, ("ConditionExpression", "UpdateExpression"))
                if err:
                    return err
                values = list((_op.get("ExpressionAttributeValues") or {}).values())
                values += list((_op.get("Item") or {}).values()) if isinstance(_op.get("Item"), dict) else []
                if any(_nesting_exceeded(v) for v in values):
                    return error_response_json("ValidationException", _NESTING_MSG, 400)
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
        prior = _txn_token_lookup(crt)
        # Drop the ClientRequestToken from the payload signature so equality
        # is on the actual transaction body.
        signature = {k: v for k, v in data.items() if k != "ClientRequestToken"}
        if prior is not None:
            if prior.get("signature") == signature:
                # A same-token replay does not re-apply the writes; AWS reports
                # a transactional READ of the stored result, recomputed against
                # the item sizes (2 x ceil(size/4KB) per item) — measured
                # against real DynamoDB (eu-west-2) by paritysuite.
                replay = {k: v for k, v in prior.get("response", {}).items() if k != "ConsumedCapacity"}
                rc_replay = data.get("ReturnConsumedCapacity", "NONE")
                if rc_replay != "NONE":
                    consumed = []
                    for tname, sizes in (prior.get("sizes") or {}).items():
                        read_units = sum(2.0 * max(1.0, float(-(-sz // 4096))) for sz in sizes)
                        consumed.append({"TableName": tname, "CapacityUnits": read_units,
                                         "ReadCapacityUnits": read_units})
                    if consumed:
                        replay["ConsumedCapacity"] = consumed
                return json_response(replay)
            return error_response_json("IdempotentParameterMismatchException",
                "Request token already in use for another request with a different payload", 400)
    # Member validation across the transaction. AWS validates every member
    # before applying anything, but splits the failure into two shapes
    # (verified against real DynamoDB):
    #   * up-front input errors -> top-level ValidationException (Phase 0):
    #     empty-string/binary key values, malformed item attrs, item size,
    #     and duplicate target keys.
    #   * per-item semantic errors -> TransactionCanceledException with a
    #     positional ValidationError reason (Phase 1): wrong-typed keys and
    #     update-expression type errors.
    # Nothing is applied in either case.
    seen_targets = set()
    for transact in items_list:
        op_type, op = _extract_transact_op(transact)
        if op is None:
            continue
        tn = op.get("TableName", "")
        tbl = _tables.get(tn)
        if not tbl:
            continue
        key_src = op.get("Item", {}) if op_type == "Put" else op.get("Key", {})
        # Empty-string/binary key value -> top-level ValidationException.
        for kn in (tbl.get("pk_name"), tbl.get("sk_name")):
            if kn and kn in key_src:
                err = _empty_key_value_error(kn, key_src[kn])
                if err:
                    return err
        # Non-key attribute value / item-size validation is also up-front.
        if op_type == "Put":
            item_err = _validate_item(key_src, tbl.get("pk_name"), tbl.get("sk_name"))
            if item_err:
                return item_err
            # Empty-string/binary secondary-index key -> top-level
            # ValidationException; wrong-typed index keys instead cancel in
            # Phase 1 (measured against real DynamoDB by paritysuite).
            msg = _index_key_empty_reason(tbl, key_src) or _vector_write_reason(tbl, key_src)
            if msg:
                return error_response_json("ValidationException", msg, 400)
        elif op_type == "Update" and op.get("UpdateExpression"):
            pk_val = _extract_key_val(key_src.get(tbl["pk_name"]))
            sk_val = _extract_key_val(key_src.get(tbl["sk_name"])) if tbl["sk_name"] else "__no_sort__"
            existing = tbl["items"].get(pk_val, {}).get(sk_val)
            probe = copy.deepcopy(existing) if existing else dict(key_src)
            try:
                probe, _ = _apply_update_expression(probe, op.get("UpdateExpression", ""),
                                                    op.get("ExpressionAttributeValues", {}),
                                                    op.get("ExpressionAttributeNames", {}))
            except ValueError:
                probe = None  # type errors surface in Phase 1 as cancellations
            if probe is not None:
                msg = _index_key_empty_reason(tbl, probe, update_expr=True)
                if msg:
                    return error_response_json("ValidationException", msg, 400)
        target = (tn,
                  _extract_key_val(key_src.get(tbl.get("pk_name") or "")),
                  _extract_key_val(key_src.get(tbl.get("sk_name") or "")) if tbl.get("sk_name") else None)
        if target in seen_targets:
            return error_response_json("ValidationException",
                "Transaction request cannot include multiple operations on one item", 400)
        seen_targets.add(target)

    # Phase 1: wrong-typed keys and update-expression type errors surface as a
    # per-item ValidationError cancellation reason, not a top-level exception.
    val_reasons = {}
    for idx, transact in enumerate(items_list):
        op_type, op = _extract_transact_op(transact)
        if op is None:
            continue
        tbl = _tables.get(op.get("TableName", ""))
        if not tbl:
            continue
        key_src = op.get("Item", {}) if op_type == "Put" else op.get("Key", {})
        type_msg = _key_type_mismatch_reason(tbl, key_src)
        if type_msg:
            # A Put names the mismatch; a Key-addressed member reports a schema mismatch.
            val_reasons[idx] = (type_msg if op_type == "Put"
                                else "The provided key element does not match the schema")
            continue
        if op_type == "Put":
            imsg = _index_key_type_reason(tbl, key_src)
            if imsg:
                val_reasons[idx] = imsg
                continue
        if op_type == "Update":
            ue = op.get("UpdateExpression", "")
            if ue:
                pk_val = _extract_key_val(key_src.get(tbl["pk_name"]))
                sk_val = _extract_key_val(key_src.get(tbl["sk_name"])) if tbl["sk_name"] else "__no_sort__"
                existing = tbl["items"].get(pk_val, {}).get(sk_val)
                probe = copy.deepcopy(existing) if existing else dict(key_src)
                try:
                    probe, _ = _apply_update_expression(probe, ue, op.get("ExpressionAttributeValues", {}), op.get("ExpressionAttributeNames", {}))
                except ValueError as exc:
                    val_reasons[idx] = str(exc)
                else:
                    imsg = _index_key_type_reason(tbl, probe)
                    if imsg:
                        val_reasons[idx] = imsg
                    elif _item_size_bytes(probe) > _DDB_ITEM_MAX_BYTES:
                        val_reasons[idx] = _UPDATE_SIZE_MSG
    if val_reasons:
        return _transact_validation_cancel_response(len(items_list), val_reasons)

    # Phase 1: evaluate ALL conditions and collect failures (AWS returns all,
    # not just the first).
    failures = {}  # idx -> existing_item_or_None
    for idx, transact in enumerate(items_list):
        op_type, op = _extract_transact_op(transact)
        if op is None:
            continue
        tbl = _tables.get(op.get("TableName", ""))
        if not tbl:
            return error_response_json("ResourceNotFoundException", "Requested resource not found", 400)
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

    _txn_write_effects: dict = {}

    for transact in items_list:
        op_type, op = _extract_transact_op(transact)
        if op is None or op_type == "ConditionCheck":
            # A ConditionCheck writes nothing but still consumes transactional
            # write capacity (2 x ceil(size/1KB), measured by paritysuite).
            if op_type == "ConditionCheck" and op is not None:
                cc_tbl = _tables.get(op.get("TableName", ""))
                if cc_tbl:
                    cc_key = op.get("Key", {})
                    cc_pk = _extract_key_val(cc_key.get(cc_tbl["pk_name"]))
                    cc_sk = _extract_key_val(cc_key.get(cc_tbl["sk_name"])) if cc_tbl["sk_name"] else "__no_sort__"
                    cc_ref = cc_tbl["items"].get(cc_pk, {}).get(cc_sk) or cc_key
                    _txn_write_effects.setdefault(op.get("TableName", ""), []).append((cc_ref, cc_ref))
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
            _set_item(tbl, pk_val, sk_val, item)
            _emit_stream_event(table_name, "MODIFY" if old_item else "INSERT", old_item, item)
            _txn_write_effects.setdefault(table_name, []).append((old_item, item))
        elif op_type == "Delete":
            key = op["Key"]
            pk_val = _extract_key_val(key.get(tbl["pk_name"]))
            sk_val = _extract_key_val(key.get(tbl["sk_name"])) if tbl["sk_name"] else "__no_sort__"
            old_item = tbl["items"].get(pk_val, {}).get(sk_val)
            _remove_item(tbl, pk_val, sk_val)
            if old_item:
                _emit_stream_event(table_name, "REMOVE", old_item, None)
            _txn_write_effects.setdefault(table_name, []).append((old_item, None))
        elif op_type == "Update":
            key = op["Key"]
            pk_val = _extract_key_val(key.get(tbl["pk_name"]))
            sk_val = _extract_key_val(key.get(tbl["sk_name"])) if tbl["sk_name"] else "__no_sort__"
            old_item = copy.deepcopy(tbl["items"].get(pk_val, {}).get(sk_val))
            item = copy.deepcopy(old_item) if old_item else dict(key)
            ue = op.get("UpdateExpression", "")
            if ue:
                item, _ = _apply_update_expression(item, ue, op.get("ExpressionAttributeValues", {}), op.get("ExpressionAttributeNames", {}))
            _set_item(tbl, pk_val, sk_val, item)
            _emit_stream_event(table_name, "MODIFY" if old_item else "INSERT", old_item, item)
            _txn_write_effects.setdefault(table_name, []).append((old_item, item))

    # ConsumedCapacity: a transactional write costs 2 x ceil(size/1KB) WCU per
    # item on the table (measured against real DynamoDB, eu-west-2, by
    # paritysuite); index replication is asynchronous, so index arms carry the
    # standard 1x units.
    result = {}
    txn_sizes: dict = {}
    rc = data.get("ReturnConsumedCapacity", "NONE")
    consumed = []
    for tname, effects in _txn_write_effects.items():
        tbl = _tables.get(tname, {})
        sizes = []
        write_units = 0.0
        gsi_units: dict = {}
        lsi_units: dict = {}
        vector_bytes: dict = {}
        for old_it, new_it in effects:
            sz = max(_item_size_bytes(old_it) if old_it else 0,
                     _item_size_bytes(new_it) if new_it else 0)
            sizes.append(sz)
            write_units += 2.0 * _capacity_kb(sz)
            for kind, acc in (("GlobalSecondaryIndexes", gsi_units), ("LocalSecondaryIndexes", lsi_units)):
                for idx in tbl.get(kind, []) or []:
                    u = _index_write_units(tbl, idx, old_it, new_it)
                    if u:
                        acc[idx["IndexName"]] = acc.get(idx["IndexName"], 0.0) + u
            for index_name, size in _vector_write_bytes(tbl, old_it, new_it).items():
                vector_bytes[index_name] = vector_bytes.get(index_name, 0.0) + size
        txn_sizes[tname] = sizes
        if rc != "NONE":
            total = write_units + sum(gsi_units.values()) + sum(lsi_units.values())
            entry = {"TableName": tname, "CapacityUnits": total, "WriteCapacityUnits": write_units}
            if rc == "INDEXES":
                if gsi_units:
                    entry["GlobalSecondaryIndexes"] = {n: {"CapacityUnits": u, "WriteCapacityUnits": u} for n, u in gsi_units.items()}
                if lsi_units:
                    entry["LocalSecondaryIndexes"] = {n: {"CapacityUnits": u, "WriteCapacityUnits": u} for n, u in lsi_units.items()}
                if vector_bytes:
                    entry["VectorIndexes"] = {n: {"VectorWriteRequestBytes": b} for n, b in vector_bytes.items()}
            consumed.append(entry)
    if rc != "NONE" and consumed:
        result["ConsumedCapacity"] = consumed
    if crt:
        _txn_token_store(crt, {"signature": signature, "response": result, "sizes": txn_sizes})
    return json_response(result)


_txn_idempotency = AccountRegionScopedDict()
_TXN_TOKEN_TTL = 600  # "valid for 10 minutes after the first request that uses it is completed"
_txn_token_sweep_at = 0.0


def _txn_token_lookup(crt):
    prior = _txn_idempotency.get(crt)
    if prior is not None and time.time() - prior["completed_at"] >= _TXN_TOKEN_TTL:
        _txn_idempotency.pop(crt, None)
        return None
    return prior


def _txn_token_store(crt, entry):
    global _txn_token_sweep_at
    now = time.time()
    entry["completed_at"] = now
    _txn_idempotency[crt] = entry
    if now >= _txn_token_sweep_at:
        _txn_token_sweep_at = now + 60
        for (account_id, region, token), prior in _txn_idempotency.all_items():
            if now - prior["completed_at"] >= _TXN_TOKEN_TTL:
                _txn_idempotency.pop_scoped(account_id, region, token, None)


def _transact_get_items(data):
    items_list = data.get("TransactItems", [])
    for _txn_item in items_list if isinstance(items_list, list) else []:
        for _txn_op in (_txn_item.values() if isinstance(_txn_item, dict) else []):
            if isinstance(_txn_op, dict) and "TableName" in _txn_op:
                _txn_op["TableName"] = _normalize_table_name(_txn_op["TableName"])
    if not items_list:
        return error_response_json("ValidationException",
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1", 400)
    if len(items_list) > _DDB_TXN_GET_MAX_ITEMS:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '[{', '.join(repr(i) for i in items_list)}]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to {_DDB_TXN_GET_MAX_ITEMS}", 400)
    # AWS pre-validates ProjectionExpression syntax + reserved keywords at the
    # request level for TransactGetItems — surfaces as a ValidationException,
    # not via the cancellation channel.
    for transact in items_list:
        _pe = (transact.get("Get", {}).get("ProjectionExpression") or "").strip()
        if _pe:
            _err = _validate_projection_expression_syntax(_pe)
            if _err:
                return error_response_json("ValidationException", _err, 400)
    # Per-action ValidationError (e.g. empty Key) surfaces as
    # TransactionCanceledException with reason "ValidationError" per AWS.
    _cancel_reasons = []
    _has_cancel = False
    for transact in items_list:
        get_op = transact.get("Get") or {}
        tbl = _tables.get(get_op.get("TableName", ""))
        if not tbl:
            _cancel_reasons.append({"Code": "None"})
            continue
        key = get_op.get("Key") or {}
        if not key:
            _cancel_reasons.append({"Code": "ValidationError",
                                    "Message": "The provided key element does not match the schema"})
            _has_cancel = True
        else:
            _cancel_reasons.append({"Code": "None"})
    if _has_cancel:
        _msg = ("Transaction cancelled, please refer cancellation reasons for specific reasons [" +
                ", ".join(r["Code"] for r in _cancel_reasons) + "]")
        _body = json.dumps({
            "__type": "TransactionCanceledException",
            "message": _msg,
            "CancellationReasons": _cancel_reasons,
        }, ensure_ascii=False).encode("utf-8")
        return 400, {
            "Content-Type": "application/x-amz-json-1.0",
            "x-amzn-errortype": "TransactionCanceledException",
        }, _body
    # Duplicate-key rejection.
    seen = set()
    for transact in items_list:
        get_op = transact.get("Get", {})
        tbl = _tables.get(get_op.get("TableName", ""))
        if not tbl:
            return error_response_json("ResourceNotFoundException",
                "Requested resource not found", 400)
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
                # AWS omits Item entirely when the projection matches no
                # attribute on a present item (rather than returning {}).
                if not item:
                    responses.append({})
                    continue
            responses.append({"Item": item})
        else:
            responses.append({})
    result = {"Responses": responses}
    rc = data.get("ReturnConsumedCapacity", "NONE")
    if rc != "NONE":
        consumed = []
        for tname, units in per_table_units.items():
            entry = {"TableName": tname, "CapacityUnits": units, "ReadCapacityUnits": units}
            # AWS's INDEXES breakdown always includes the base Table block —
            # the ReadCapacityUnits attributed to the table itself, distinct
            # from index-side reads (which TransactGetItems never has, since
            # it can only read from the base table).
            if rc == "INDEXES":
                entry["Table"] = {"CapacityUnits": units, "ReadCapacityUnits": units}
                if _tables.get(tname, {}).get("GlobalSecondaryIndexes"):
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
    name = _normalize_table_name(data.get("TableName"))
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Table {name} not found", 400)
    setting = _ttl_settings.get(name, {"TimeToLiveStatus": "DISABLED"})
    desc = {"TimeToLiveStatus": setting.get("TimeToLiveStatus", "DISABLED")}
    if "AttributeName" in setting:
        desc["AttributeName"] = setting["AttributeName"]
    return json_response({"TimeToLiveDescription": desc})


def _update_ttl(data):
    name = _normalize_table_name(data.get("TableName"))
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
    # A global table's TTL settings are synchronized to every replica.
    for region in _replica_group(_tables[name]):
        if region != get_region():
            _ttl_settings.set_scoped(get_account_id(), region, name, dict(_ttl_settings[name]))
    return json_response({"TimeToLiveSpecification": spec})


# ---------------------------------------------------------------------------
# Continuous backups / PITR
# ---------------------------------------------------------------------------

def _describe_continuous_backups(data):
    name = _normalize_table_name(data.get("TableName"))
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
    name = _normalize_table_name(data.get("TableName"))
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
    port = os.environ.get("GATEWAY_PORT", "4566")
    return json_response({
        "Endpoints": [{"Address": f"{_MINISTACK_HOST}:{port}", "CachePeriodInMinutes": 1440}]
    })


# ---------------------------------------------------------------------------
# Tag operations
# ---------------------------------------------------------------------------

def _dynamodb_arn_spec(arn: str):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None
    if (
        not _DDB_PARTITION_RE.match(spec.partition)
        or spec.service != "dynamodb"
        or not _DDB_REGION_RE.match(spec.region)
        or not _DDB_ACCOUNT_RE.match(spec.account_id)
    ):
        return None
    return spec


def _validate_tag_arn(arn: str) -> tuple | None:
    """ARN must (a) look like a DynamoDB ARN, (b) reference an existing table."""
    if not isinstance(arn, str) or _dynamodb_arn_spec(arn) is None:
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
    # ListTagsOfResource on a non-existent (but syntactically valid) ARN
    # returns AccessDeniedException on AWS — the API does not reveal whether
    # the resource exists. Syntactic validation still uses ValidationException.
    if not isinstance(arn, str) or _dynamodb_arn_spec(arn) is None:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{arn}' at 'resourceArn' failed to satisfy constraint: Member must satisfy regular expression pattern: arn:[a-z\\-]+:dynamodb:[a-z]{{2}}-[a-z]+-[0-9]:[0-9]{{12}}:.*", 400)
    tname = _table_name_from_arn(arn)
    if not tname or tname not in _tables:
        return error_response_json("AccessDeniedException",
            "Access denied. The user is not authorized to access this resource.", 400)
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
    table_name = _normalize_table_name(data.get("TableName"))
    stream_arn = data.get("StreamArn")
    if not table_name:
        return None, None, error_response_json("ValidationException", "The parameter 'TableName' is required but was not present in the request", 400)
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
    table_name = _normalize_table_name(data.get("TableName"))
    if not table_name:
        return error_response_json("ValidationException", "The parameter 'TableName' is required but was not present in the request", 400)
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
    if value.startswith("arn:"):
        return _table_name_from_arn(value) or value
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
        "LastUpdateDateTime": int(time.time()),
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
            entry["LastUpdateDateTime"] = int(time.time())
        elif cur == "DISABLING":
            cur = "DISABLED"
            entry["ContributorInsightsStatus"] = "DISABLED"
            entry["LastUpdateDateTime"] = int(time.time())
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
    if not isinstance(arn, str):
        return None
    spec = _dynamodb_arn_spec(arn)
    if (
        spec is None
        or spec.region != get_region()
        or spec.account_id != get_account_id()
    ):
        return None
    prefix = "table/"
    if not spec.resource.startswith(prefix):
        return None
    after = spec.resource[len(prefix):]
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


def _import_arn(table_arn: str) -> str:
    """``<table arn>/import/<epoch millis>-<8 hex>``, the documented shape:
    ``arn:aws:dynamodb:us-east-1:ACCOUNT:table/target-table/import/01658528578619-c4d4e311``.
    """
    return f"{table_arn}/import/{int(time.time() * 1000):014d}-{new_uuid()[:8]}"


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
    if desc.get("ExportStatus") == "IN_PROGRESS" and (time.time() - desc.get("StartTime", 0)) >= _EXPORT_COMPLETE_AFTER_SEC:
        table_name = _table_name_from_arn(desc["TableArn"])
        table = _tables.get(table_name)
        if table:
            desc["ItemCount"] = len(table.get("items", {}))
            desc["BilledSizeBytes"] = table.get("TableSizeBytes", 0)
        desc["ExportStatus"] = "COMPLETED"
        desc["EndTime"] = time.time()
        desc["ExportManifest"] = f"AWSDynamoDB/{arn.split('/')[-1]}/manifest-summary.json"
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


def _validate_csv_import_options(data):
    """``InputFormatOptions.Csv`` as the model bounds it.

    ``Delimiter`` is ``{"max": 1, "min": 1, "pattern": "[,;:|\\t ]"}``, and
    ``HeaderList`` names the attributes each column carries, so a blank or a
    repeat has no reading.
    """
    options = (data.get("InputFormatOptions") or {}).get("Csv")
    if not isinstance(options, dict):
        return None
    delimiter = options.get("Delimiter")
    if delimiter is not None and not _CSV_DELIMITER_RE.fullmatch(str(delimiter)):
        return error_response_json(
            "ValidationException",
            f"1 validation error detected: Value '{delimiter}' at "
            "'inputFormatOptions.csv.delimiter' failed to satisfy constraint: "
            "Member must satisfy regular expression pattern: [,;:|\\t ]", 400)
    header = options.get("HeaderList")
    if header is None:
        return None
    if (not isinstance(header, list) or not header
            or any(not isinstance(name, str) or not name for name in header)
            or len(set(header)) != len(header)):
        return error_response_json(
            "ValidationException",
            "1 validation error detected: Value at 'inputFormatOptions.csv.headerList' "
            "failed to satisfy constraint: Member must contain distinct non-empty "
            "attribute names", 400)
    return None


def _import_description(desc):
    """The stored import without the result the grace window still holds back."""
    return {k: v for k, v in desc.items() if not k.startswith("_")}


class _ImportFailed(Exception):
    """An import that cannot run, carrying the ``FailureCode`` it reports."""

    def __init__(self, code, message, *, table_created=True):
        super().__init__(message)
        self.code = code
        self.message = message
        # "Since the error was caught before the data was imported into the
        # table, a new DynamoDB table is not created."
        self.table_created = table_created


def _import_log_stream(import_arn):
    """The error stream of one import: log group ``/aws-dynamodb/imports``,
    stream ``<import id>/error`` where "The import ID is the last path element
    of the ImportArn field"."""
    from ministack.services import cloudwatch_logs as _cwl

    now_ms = int(time.time() * 1000)
    if _IMPORT_LOG_GROUP not in _cwl._log_groups:
        _cwl._log_groups[_IMPORT_LOG_GROUP] = {
            "arn": _cwl._make_group_arn(_IMPORT_LOG_GROUP),
            "creationTime": now_ms,
            "retentionInDays": None,
            "tags": {},
            "subscriptionFilters": {},
            "streams": {},
        }
    group = _cwl._log_groups[_IMPORT_LOG_GROUP]
    stream_name = f"{import_arn.rsplit('/', 1)[-1]}/error"
    if stream_name not in group["streams"]:
        group["streams"][stream_name] = {
            "events": [],
            "uploadSequenceToken": "1",
            "creationTime": now_ms,
            "firstEventTimestamp": None,
            "lastEventTimestamp": None,
            "lastIngestionTime": None,
        }
    return group["streams"][stream_name]


def _log_import_error(import_arn, bucket, key, item_index, message):
    """One item-level error, in the shape the documented log event carries:
    ``{"itemS3Pointer": {...}, "importArn": ..., "errorMessages": [...]}``."""
    try:
        stream = _import_log_stream(import_arn)
    except Exception:
        logger.debug("DynamoDB import %s: error log unavailable", import_arn, exc_info=True)
        return
    ts = int(time.time() * 1000)
    stream["events"].append({
        "timestamp": ts,
        "message": json.dumps({
            "itemS3Pointer": {"bucket": bucket, "key": key, "itemIndex": item_index},
            "importArn": import_arn,
            "errorMessages": [message],
        }),
        "ingestionTime": ts,
    })
    if stream["firstEventTimestamp"] is None:
        stream["firstEventTimestamp"] = ts
    stream["lastEventTimestamp"] = ts
    stream["lastIngestionTime"] = ts


def _import_source_objects(s3_source, compression):
    """``(key, bytes)`` for every object under the source prefix, key order.

    ``ProcessedSizeBytes`` counts what the import reads, and pricing "is based
    on the uncompressed size of the source data", so the caller measures the
    decompressed blob.
    """
    from ministack.services import s3 as s3_svc

    bucket_name = (s3_source or {}).get("S3Bucket")
    bucket = s3_svc._buckets.get(bucket_name)
    if bucket is None:
        raise _ImportFailed(
            "S3NoSuchBucket",
            "The specified bucket does not exist (Service: Amazon S3; Status Code: 404; "
            "Error Code: NoSuchBucket)",
            table_created=False)
    prefix = (s3_source or {}).get("S3KeyPrefix") or ""
    keys = sorted(key for key in bucket["objects"]
                  if key.startswith(prefix) and not key.endswith("/"))
    if not keys:
        raise _ImportFailed(
            "S3NoSuchKey",
            f"No objects were found under s3://{bucket_name}/{prefix}",
            table_created=False)
    objects = []
    for key in keys:
        raw = s3_svc._get_object_data(bucket_name, key)
        if raw is None:
            continue
        if compression == "GZIP":
            try:
                raw = gzip.decompress(raw)
            except OSError as exc:
                raise _ImportFailed("InputFormatError",
                                    f"{key} is not valid GZIP data: {exc}") from None
        objects.append((key, raw))
    return bucket_name, objects


def _import_csv_items(blob, options, attribute_types):
    """``(items, malformed_row_count)`` for one CSV object.

    "If this field is specified then the first line of each CSV file is treated
    as data instead of the header. If this field is not specified the the first
    line of each CSV file is treated as the header" (CsvOptions.HeaderList).
    A key column takes the type its AttributeDefinition declares; every other
    column is imported as a string, and an empty one is left off the item.
    """
    delimiter = options.get("Delimiter") or ","
    header = options.get("HeaderList")
    try:
        # utf-8-sig: a BOM would otherwise become part of the first header,
        # and every item would carry an attribute nothing can query.
        rows = list(csv.reader(io.StringIO(blob.decode("utf-8-sig")), delimiter=delimiter))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise _ImportFailed("InputFormatError", f"The CSV source is malformed: {exc}") from None
    if not header:
        if not rows:
            return [], []
        header, rows = rows[0], rows[1:]
    items, malformed = [], []
    for index, row in enumerate(rows):
        if not any(cell.strip() for cell in row):
            continue
        if len(row) != len(header):
            malformed.append((index, f"CSV row has {len(row)} columns but the header "
                                     f"has {len(header)} columns"))
            continue
        item = {}
        for column, value in zip(header, row):
            if value == "":
                continue
            item[column] = {attribute_types.get(column, "S"): value}
        items.append((index, item))
    return items, malformed


def _import_dynamodb_json_items(blob):
    """``(items, malformed)`` for one DYNAMODB_JSON object: one
    ``{"Item": {...}}`` per line."""
    items, malformed = [], []
    for index, line in enumerate(blob.decode("utf-8", errors="replace").splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            malformed.append((index, f"The item is not valid JSON: {exc}"))
            continue
        item = record.get("Item") if isinstance(record, dict) else None
        if isinstance(item, dict):
            items.append((index, item))
        else:
            malformed.append((index, "The item is not in DYNAMODB_JSON format"))
    return items, malformed


def _drop_import_table(table_name):
    """Undo the table a failed import created, through the service's own path."""
    try:
        _delete_table({"TableName": table_name})
    except Exception:
        logger.debug("DynamoDB import: could not drop %s", table_name, exc_info=True)


def _run_import(import_arn, table_name):
    """Read the source and write its items, updating the description as it goes.

    Runs on a background thread with the submitting request's account and
    region snapshotted, so ``DescribeImport`` reports the counters climbing
    the way a real import does.
    """
    if _IMPORT_COMPLETE_AFTER_SEC > 0:
        time.sleep(_IMPORT_COMPLETE_AFTER_SEC)
    desc = _imports.get(import_arn)
    if not desc or desc.get("ImportStatus") != "IN_PROGRESS":
        return
    compression = desc.get("InputCompressionType") or "NONE"
    try:
        if compression not in ("NONE", "GZIP"):
            raise _ImportFailed(
                "InputCompressionTypeNotSupported",
                f"{compression} decompression needs a library MiniStack does not ship; "
                "use NONE or GZIP", table_created=False)
        if desc["InputFormat"] == "ION":
            raise _ImportFailed(
                "InputFormatNotSupported",
                "ION parsing needs a library MiniStack does not ship; "
                "use CSV or DYNAMODB_JSON", table_created=False)
        attribute_types = {
            defn.get("AttributeName"): defn.get("AttributeType", "S")
            for defn in (desc.get("TableCreationParameters") or {}).get(
                "AttributeDefinitions", [])
            if defn.get("AttributeName")
        }
        options = (desc.get("InputFormatOptions") or {}).get("Csv") or {}
        bucket_name, objects = _import_source_objects(desc.get("S3BucketSource"), compression)
        for key, blob in objects:
            desc["ProcessedSizeBytes"] += len(blob)
            try:
                if desc["InputFormat"] == "CSV":
                    items, malformed = _import_csv_items(blob, options, attribute_types)
                else:
                    items, malformed = _import_dynamodb_json_items(blob)
            except _ImportFailed as unreadable:
                # "If the Amazon S3 object itself is malformed ... we may skip
                # processing the remaining portion of the object."
                desc["ErrorCount"] += 1
                _log_import_error(import_arn, bucket_name, key, 0, unreadable.message)
                continue
            for index, reason in malformed:
                desc["ProcessedItemCount"] += 1
                desc["ErrorCount"] += 1
                _log_import_error(import_arn, bucket_name, key, index, reason)
            for index, item in items:
                desc["ProcessedItemCount"] += 1
                status, _headers, body = _put_item({"TableName": table_name, "Item": item})
                if status == 200:
                    # Counts loads, not the table's ItemCount: "the number of
                    # items processed in the import table description will not
                    # match the number of items in the target table".
                    desc["ImportedItemCount"] += 1
                else:
                    desc["ErrorCount"] += 1
                    _log_import_error(import_arn, bucket_name, key, index,
                                      _import_error_message(body))
    except _ImportFailed as failure:
        if not failure.table_created:
            _drop_import_table(table_name)
        _finish_import(desc, "FAILED", failure.code, failure.message)
        return
    except Exception:
        logger.exception("DynamoDB import %s crashed", import_arn)
        _finish_import(desc, "FAILED", "InternalServerError",
                       "Internal Failure. Please try your request again.")
        return

    if desc["ErrorCount"]:
        # "At the end of job, the import status is set to FAILED with a
        # FailureCode, ItemValidationError and the FailureMessage ..."
        _finish_import(
            desc, "FAILED", "ItemValidationError",
            "Some of the items failed validation checks and were not imported. "
            "Please check CloudWatch error logs for more details.")
        return
    _finish_import(desc, "COMPLETED")


def _import_error_message(body):
    """The message of the error response ``_put_item`` answered."""
    try:
        return json.loads(body.decode("utf-8")).get("message", "")
    except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
        return ""


def _finish_import(desc, status, failure_code=None, failure_message=None):
    desc["ImportStatus"] = status
    desc["EndTime"] = int(time.time())
    if failure_code:
        desc["FailureCode"] = failure_code
        desc["FailureMessage"] = failure_message


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
    if fmt == "CSV":
        err = _validate_csv_import_options(data)
        if err:
            return err
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
                return json_response({"ImportTableDescription": _import_description(desc)})
    running = sum(1 for desc in _imports.values()
                  if desc.get("ImportStatus") == "IN_PROGRESS")
    if running >= _IMPORT_CONCURRENCY_LIMIT:
        return error_response_json(
            "LimitExceededException",
            f"Subscriber limit exceeded: There is a limit of "
            f"{_IMPORT_CONCURRENCY_LIMIT} concurrent import jobs per account", 400)
    # Create the destination table from TableCreationParameters.
    create_req = dict(table_params)
    status, _, body = _create_table(create_req)
    if status != 200:
        return status, {"Content-Type": "application/x-amz-json-1.0"}, body
    table_arn = _tables[table_name]["TableArn"]
    table_id = _tables[table_name].get("TableId")
    arn = _import_arn(table_arn)
    now = int(time.time())
    desc = {
        "ImportArn": arn,
        "ImportStatus": "IN_PROGRESS",
        "TableArn": table_arn,
        "TableId": table_id,
        "S3BucketSource": s3_source,
        "CloudWatchLogGroupArn": (
            f"arn:aws:logs:{get_region()}:{get_account_id()}:log-group:{_IMPORT_LOG_GROUP}:*"),
        "InputFormat": fmt,
        "InputCompressionType": data.get("InputCompressionType") or "NONE",
        "StartTime": now,
        "ProcessedSizeBytes": 0,
        "ProcessedItemCount": 0,
        "ImportedItemCount": 0,
        "ErrorCount": 0,
        "TableCreationParameters": table_params,
    }
    if data.get("InputFormatOptions"):
        desc["InputFormatOptions"] = data["InputFormatOptions"]
    if client_token:
        desc["ClientToken"] = client_token
    _imports[arn] = desc
    # spawn_background snapshots this request's account and region.
    spawn_background(_run_import, arn, table_name, thread_name="ministack-ddb-import")
    return json_response({"ImportTableDescription": _import_description(desc)})


def _describe_import(data):
    arn = data.get("ImportArn")
    if not arn:
        return error_response_json("ValidationException",
            "1 validation error detected: Value null at 'importArn' failed to satisfy constraint: Member must not be null", 400)
    desc = _imports.get(arn)
    if not desc:
        return error_response_json("ImportNotFoundException",
            f"Import not found: {arn}", 400)
    # An import restored from a snapshot has no worker behind it.
    if (desc.get("ImportStatus") == "IN_PROGRESS" and desc.get("_orphaned")
            and (time.time() - desc.get("StartTime", 0)) >= _IMPORT_COMPLETE_AFTER_SEC):
        _finish_import(desc, "FAILED", "InternalServerError",
                       "Internal Failure. Please try your request again.")
    return json_response({"ImportTableDescription": _import_description(desc)})


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
        "_attribute_definitions": copy.deepcopy(table.get("AttributeDefinitions", [])),
    }
    if table.get("VectorIndexes"):
        desc["SourceTableFeatureDetails"]["VectorIndexes"] = [_vector_index_info(v) for v in table["VectorIndexes"]]
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
    attr_defs = desc.get("_attribute_definitions") or _tables.get(src.get("TableName"), {}).get("AttributeDefinitions", [])
    create_req = _restore_create_request(
        target, src.get("KeySchema", []), attr_defs,
        data.get("BillingModeOverride") or src.get("BillingMode", "PROVISIONED"),
        src.get("ProvisionedThroughput"), data,
        feat.get("GlobalSecondaryIndexes"), feat.get("LocalSecondaryIndexes"), feat.get("VectorIndexes"))
    status, _, body = _create_table(create_req)
    if status != 200:
        return status, {"Content-Type": "application/x-amz-json-1.0"}, body
    # Restore items.
    snap = desc.get("_items_snapshot") or {}
    _tables[target]["items"] = defaultdict(dict, copy.deepcopy(snap))
    _items_replaced(_tables[target])
    # AWS attaches a RestoreSummary to the response — clients (Terraform, the
    # AWS SDK) read SourceBackupArn / RestoreInProgress to track the restore.
    _tables[target]["RestoreSummary"] = {
        "SourceBackupArn": arn,
        "SourceTableArn": _tables[target].get("TableArn", ""),
        "RestoreDateTime": int(time.time()),
        "RestoreInProgress": True,
    }
    td = _table_description(target)
    td["RestoreSummary"] = _tables[target]["RestoreSummary"]
    return json_response({"TableDescription": td})


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
    create_req = _restore_create_request(
        target, src.get("KeySchema", []), src.get("AttributeDefinitions", []),
        data.get("BillingModeOverride") or src.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED"),
        src.get("ProvisionedThroughput"), data,
        src.get("GlobalSecondaryIndexes"), src.get("LocalSecondaryIndexes"),
        [_vector_index_info(v) for v in src.get("VectorIndexes") or []])
    status, _, body = _create_table(create_req)
    if status != 200:
        return status, {"Content-Type": "application/x-amz-json-1.0"}, body
    _tables[target]["items"] = defaultdict(dict, copy.deepcopy(dict(src.get("items", {}))))
    _items_replaced(_tables[target])
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
        # AWS rejects begins_with on a non-string/binary operand. The caller's
        # error wrapper prepends "Invalid <slot>:". Report the operand's type
        # (first key of the AttributeValue map: N, BOOL, NULL, …).
        op_type = next(iter(substr.keys())) if isinstance(substr, dict) and substr else "?"
        raise ValueError(f"Incorrect operand type for operator or function; operator or function: begins_with, operand type: {op_type}")

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
            # AWS reports the alias-resolved path. `path` is the parser's
            # resolved component list (e.g. ['data'] for #a -> data).
            try:
                path_text = ".".join(str(c) for c in path) if isinstance(path, list) else str(path)
            except Exception:
                path_text = "".join(t for t in path_tokens)
            raise ValueError(f"Invalid ConditionExpression: The first operand must be distinct from the remaining operands for this operator or function; operator: contains, first operand: [{path_text}]")
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


def _check_redundant_parens(tokens, slot: str = "ConditionExpression") -> str | None:
    """AWS rejects ConditionExpression / FilterExpression / KeyConditionExpression
    that contain redundant parentheses. The unambiguous, low-false-positive
    detection rule: an LPAREN whose immediately-following token is another
    LPAREN whose matching RPAREN is followed directly by the outer RPAREN —
    i.e. the `((expr))` pattern.

    Returns the AWS-canonical error string (`"Invalid <slot>: The expression
    has redundant parentheses;"`) when redundancy is detected, or None when
    the expression is fine.
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
                return f"Invalid {slot}: The expression has redundant parentheses;"
    return None


def _evaluate_condition(expr, item, attr_values, attr_names,
                        slot: str = "ConditionExpression"):
    """Evaluate a DynamoDB expression. ``slot`` controls the "Invalid <X>:"
    prefix on error strings — pass "FilterExpression" / "KeyConditionExpression"
    / "ConditionExpression" so AWS-canonical wrapping matches the request
    parameter the expression came from."""
    if not expr or not expr.strip():
        return True
    try:
        tokens = _tokenize(expr)
        err = _check_redundant_parens(tokens, slot)
        if err:
            raise ValueError(err)
        err = _check_reserved_keyword_usage(tokens)
        if err:
            raise ValueError(err)
        # AWS validates BETWEEN bounds at parse time for ConditionExpression too:
        # lower must be <= upper, or it's a ValidationException (not ConditionalCheckFailed).
        for i, tok in enumerate(tokens):
            if (tok[0] == "IDENT" and tok[1].upper() == "BETWEEN"
                    and i + 3 < len(tokens)
                    and tokens[i + 1][0] == "VALUE_REF"
                    and tokens[i + 2][0] == "IDENT" and tokens[i + 2][1].upper() == "AND"
                    and tokens[i + 3][0] == "VALUE_REF"):
                _lo = attr_values.get(tokens[i + 1][1])
                _hi = attr_values.get(tokens[i + 3][1])
                _berr = _between_bounds_error(_lo, _hi)
                if _berr:
                    raise ValueError(
                        f"Invalid {slot}: The BETWEEN operator requires upper bound to be greater than or equal to lower bound")
        return _ExprEval(tokens, item, attr_values, attr_names).evaluate()
    except ValueError as e:
        msg = str(e)
        # If the inner error is already an AWS-canonical message, propagate.
        if msg.startswith("Invalid ConditionExpression:") \
                or msg.startswith("Invalid UpdateExpression:") \
                or msg.startswith("Invalid FilterExpression:") \
                or msg.startswith("Invalid KeyConditionExpression:") \
                or msg.startswith("Invalid ProjectionExpression:"):
            raise
        logger.warning("Expression evaluation error: %s for expr: %s", e, expr)
        raise ValueError(f"Invalid {slot}: {e}")
    except Exception as e:
        logger.warning("Expression evaluation error: %s for expr: %s", e, expr)
        raise ValueError(f"Invalid {slot}: {e}")


# ---------------------------------------------------------------------------
# Update expression
# ---------------------------------------------------------------------------

def _apply_update_expression(item, expr, attr_values, attr_names, updated_attrs=None):
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

    updated_attrs = set() if updated_attrs is None else updated_attrs

    if 'SET' in clauses:
        _apply_set(item, clauses['SET'], attr_values, attr_names, updated_attrs)
    if 'REMOVE' in clauses:
        _apply_remove(item, clauses['REMOVE'], attr_names, updated_attrs)
    if 'ADD' in clauses:
        _apply_add(item, clauses['ADD'], attr_values, attr_names, updated_attrs)
    if 'DELETE' in clauses:
        _apply_delete(item, clauses['DELETE'], attr_values, attr_names, updated_attrs)
    return item, updated_attrs


def _apply_set(item, tokens, attr_values, attr_names, updated_attrs):
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
            updated_attrs.add(tuple(path_parts))
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
                result = lv + rv if tok[0] == 'PLUS' else lv - rv
                # Validate AWS magnitude bounds on the result — anything past
                # 9.9999E+125 / below 1E-130 raises "Number overflow".
                canon = _ddb_canonicalize_number(str(result))
                if canon is None:
                    raise ValueError("Number overflow. Attempting to store a number with magnitude larger than supported range")
                return {"N": canon}
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
        # A document path in a SET value must resolve — AWS rejects e.g.
        # `SET a = list_append(a, :v)` when `a` doesn't exist on the item
        # (if_not_exists is the sanctioned way to handle absence).
        raise ValueError("The provided expression refers to an attribute that does not exist in the item")

    if len(tokens) == 1 and tokens[0][0] == 'VALUE_REF':
        return attr_values.get(tokens[0][1])

    return None


def _apply_remove(item, tokens, attr_names, updated_attrs):
    for path_tokens in _split_by_comma(tokens):
        path = _parse_path_from_tokens(path_tokens, attr_names)
        if path:
            updated_attrs.add(tuple(path))
            _remove_at_path(item, path)


_AV_TYPE_NAMES = {
    "S": "STRING", "N": "NUMBER", "B": "BINARY",
    "SS": "STRING SET", "NS": "NUMBER SET", "BS": "BINARY SET",
    "M": "MAP", "L": "LIST", "BOOL": "BOOLEAN", "NULL": "NULL",
}


def _operand_type(av):
    if isinstance(av, dict) and len(av) == 1:
        return next(iter(av))
    return None


def _apply_add(item, tokens, attr_values, attr_names, updated_attrs):
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
        updated_attrs.add(tuple(path))

        # ADD only accepts Number and set operands (parse-time in AWS), and the
        # existing attribute must carry the same type (runtime in AWS).
        op_type = _operand_type(add_val)
        if op_type not in ("N", "SS", "NS", "BS"):
            raise ValueError(
                "Invalid UpdateExpression: Incorrect operand type for operator or function; "
                f"operator: ADD, operand type: {_AV_TYPE_NAMES.get(op_type, op_type)}, typeSet: ALLOWED_FOR_ADD_OPERAND")
        existing = _get_at_path(item, path)
        if existing is not None and _operand_type(existing) != op_type:
            raise ValueError("An operand in the update expression has an incorrect data type")

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


def _apply_delete(item, tokens, attr_values, attr_names, updated_attrs):
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
        updated_attrs.add(tuple(path))

        # DELETE only accepts set operands (parse-time in AWS), and the
        # existing attribute must be a set of the same type (runtime in AWS).
        op_type = _operand_type(del_val)
        if op_type not in ("SS", "NS", "BS"):
            # NOTE: real DynamoDB reports typeSet ALLOWED_FOR_ADD_OPERAND even for
            # the DELETE operator (verified against real AWS), so we match that.
            raise ValueError(
                "Invalid UpdateExpression: Incorrect operand type for operator or function; "
                f"operator: DELETE, operand type: {_AV_TYPE_NAMES.get(op_type, op_type)}, typeSet: ALLOWED_FOR_ADD_OPERAND")

        existing = _get_at_path(item, path)
        if existing is None:
            continue
        if _operand_type(existing) != op_type:
            raise ValueError("An operand in the update expression has an incorrect data type")

        remaining = [s for s in existing[op_type] if s not in del_val[op_type]]
        if remaining:
            _set_at_path(item, path, {op_type: remaining})
        else:
            _remove_at_path(item, path)


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
            return ("NS", frozenset(_canonical_number(n) for n in val["NS"]))
        if "BS" in val:
            return ("BS", frozenset(_canonical_binary(b) for b in val["BS"]))
        if "L" in val:
            # Lists compare element-wise, in order.
            return ("L", tuple(_ddb_comparable(el) for el in val["L"]))
        if "M" in val:
            # Maps compare by content, not key arrival order.
            return ("M", tuple(sorted((k, _ddb_comparable(v)) for k, v in val["M"].items())))
    return ("UNKNOWN", object())


def _canonical_number(n):
    try:
        return Decimal(n)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def _canonical_binary(b):
    if isinstance(b, bytes):
        return b
    try:
        import base64
        return base64.b64decode(b)
    except Exception:
        return b if isinstance(b, str) else b""


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
        err = _empty_key_value_error(key_name, raw_value)
        if err:
            return "", "", err
    err = None if allow_extra else _key_length_error(attrs, table["pk_name"], table["sk_name"])
    if err:
        return "", "", err
    pk_val = _extract_key_val(attrs.get(table["pk_name"]))
    sk_val = _extract_key_val(attrs.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"
    return pk_val, sk_val, None


def _key_schema_validation_error():
    return error_response_json("ValidationException", "The provided key element does not match the schema", 400)


def _index_key_type_reason(table: dict, item: dict) -> str | None:
    """AWS's type-mismatch message for a wrong-typed / non-scalar secondary
    index key attribute in `item`, or None. Used both as a top-level
    ValidationException (single writes) and as a per-item TransactionCanceled
    ValidationError reason (transactions)."""
    attr_defs = {ad["AttributeName"]: ad["AttributeType"]
                 for ad in (table.get("AttributeDefinitions") or [])}
    for idx in (table.get("GlobalSecondaryIndexes") or []) + (table.get("LocalSecondaryIndexes") or []):
        idx_name = idx.get("IndexName")
        for ks in (idx.get("KeySchema") or []):
            key_name = ks.get("AttributeName")
            if not key_name or key_name not in item:
                continue
            expected_type = attr_defs.get(key_name)
            if not expected_type:
                continue
            raw = item[key_name]
            if not isinstance(raw, dict) or len(raw) != 1:
                return f"One or more parameter values were invalid: Type mismatch for Index Key {key_name} Expected: {expected_type} Actual: map IndexName: {idx_name}"
            actual_type = next(iter(raw))
            if actual_type != expected_type:
                return f"One or more parameter values were invalid: Type mismatch for Index Key {key_name} Expected: {expected_type} Actual: {actual_type} IndexName: {idx_name}"
    return None


def _index_key_empty_reason(table: dict, item: dict, update_expr: bool = False) -> str | None:
    """AWS's empty-value message for a secondary index key attribute in `item`
    holding an empty string/binary, or None. UpdateItem words it differently
    and omits the index name (measured by paritysuite, 2026-06-23)."""
    attr_defs = {ad["AttributeName"]: ad["AttributeType"]
                 for ad in (table.get("AttributeDefinitions") or [])}
    for idx in (table.get("GlobalSecondaryIndexes") or []) + (table.get("LocalSecondaryIndexes") or []):
        idx_name = idx.get("IndexName")
        for ks in (idx.get("KeySchema") or []):
            key_name = ks.get("AttributeName")
            if not key_name or key_name not in item:
                continue
            expected_type = attr_defs.get(key_name)
            raw = item[key_name]
            if not isinstance(raw, dict) or len(raw) != 1:
                continue
            actual_type = next(iter(raw))
            if actual_type != expected_type:
                continue
            if (actual_type == "S" and raw.get("S") == "") or (actual_type == "B" and not raw.get("B")):
                kind = "string" if actual_type == "S" else "binary"
                if update_expr:
                    return f"One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. The AttributeValue for a key attribute cannot contain an empty {kind} value."
                return f"One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty {kind} value. IndexName: {idx_name}, IndexKey: {key_name}"
    return None


def _validate_index_key_values(table: dict, item: dict, update_expr: bool = False) -> tuple | None:
    """Validate that any GSI/LSI key attributes present in `item` have the
    correct type and are not empty. AWS rejects puts/updates that would write
    a wrong-typed, non-scalar, or empty-string/binary value into a secondary
    index key attribute. Items missing a GSI key attribute entirely are fine —
    they just won't appear in that sparse GSI."""
    msg = _index_key_type_reason(table, item)
    if msg:
        return error_response_json("ValidationException", msg, 400)
    msg = _index_key_empty_reason(table, item, update_expr=update_expr)
    if msg:
        return error_response_json("ValidationException", msg, 400)
    msg = _vector_write_reason(table, item, update_expr=update_expr)
    if msg:
        return error_response_json("ValidationException", msg, 400)
    return None


# ---------------------------------------------------------------------------
# Vector indexes (CreateTable/UpdateTable VectorIndexes, SearchVectors)
# ---------------------------------------------------------------------------

_VECTOR_MAX_DIMENSIONS = 4096
_VECTOR_MAX_INDEXES = 5
_VECTOR_TOPK_MAX = 100
_VECTOR_WRITE_FLOOR_BYTES = 1024
# An index added by UpdateTable allocates (table UPDATING, Backfilling false),
# then backfills (table ACTIVE, Backfilling true), then turns ACTIVE.
_VECTOR_ALLOCATION_SECONDS = 3.0
_VECTOR_BACKFILL_SECONDS = 9.0
_VECTOR_DISTANCE_FUNCTIONS = ("COSINE", "DOT_PRODUCT", "EUCLIDEAN")
_VECTOR_INVALID = "One or more parameter values were invalid: "


def _vector_phase(vix: dict) -> str:
    """'allocating', 'backfilling' or 'active'."""
    started = vix.get("_online_since")
    if started is None:
        return "active"
    elapsed = time.time() - started
    if elapsed < _VECTOR_ALLOCATION_SECONDS:
        return "allocating"
    if elapsed < _VECTOR_ALLOCATION_SECONDS + _VECTOR_BACKFILL_SECONDS:
        return "backfilling"
    vix.pop("_online_since", None)
    return "active"


def _vector_index_request_error(vix, member: str) -> tuple | None:
    """Request-model constraints from botocore's CreateVectorIndexAction / VectorIndex."""
    def bad(field, value, constraint):
        return error_response_json("ValidationException",
            f"1 validation error detected: Value {value} at '{member}.{field}' "
            f"failed to satisfy constraint: {constraint}", 400)
    if not isinstance(vix, dict):
        return error_response_json("ValidationException", "VectorIndex must be a structure", 400)
    name = vix.get("IndexName")
    if not isinstance(name, str) or len(name) < 3:
        return bad("indexName", f"'{name}'", "Member must have length greater than or equal to 3")
    if len(name) > 255:
        return bad("indexName", f"'{name}'", "Member must have length less than or equal to 255")
    if not re.fullmatch(r"[a-zA-Z0-9_.-]+", name):
        return bad("indexName", f"'{name}'", "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+")
    dims = vix.get("Dimensions")
    if not isinstance(dims, int) or isinstance(dims, bool) or dims < 1:
        return bad("dimensions", f"'{dims}'", "Member must have value greater than or equal to 1")
    fn = vix.get("DistanceFunction")
    if fn not in _VECTOR_DISTANCE_FUNCTIONS:
        return bad("distanceFunction", f"'{fn}'", "Member must satisfy enum value set: [COSINE, DOT_PRODUCT, EUCLIDEAN]")
    attr = (vix.get("VectorAttribute") or {}).get("AttributeName")
    if not isinstance(attr, str) or not attr:
        return bad("vectorAttribute.attributeName", f"'{attr}'", "Member must have length greater than or equal to 1")
    ptype = (vix.get("Projection") or {}).get("ProjectionType")
    if ptype is not None and ptype not in _VALID_PROJECTION_TYPES:
        return bad("projection.projectionType", f"'{ptype}'", "Member must satisfy enum value set: [ALL, KEYS_ONLY, INCLUDE]")
    for element in vix.get("SearchSchema") or []:
        etype = (element or {}).get("SearchSchemaElementType")
        if etype not in ("HASH", "INLINE_FILTER"):
            return error_response_json("ValidationException",
                f"1 validation error detected: Value '{etype}' at '{member}.searchSchema' "
                "failed to satisfy constraint: Member must satisfy enum value set: [HASH, INLINE_FILTER]", 400)
    return None


def _vector_indexes_error(new: list, existing: list, billing_mode: str, defined_attrs: set) -> tuple | None:
    """Service-layer checks for vector indexes being added next to `existing`."""
    if not new:
        return None
    if billing_mode != "PAY_PER_REQUEST":
        return error_response_json("ValidationException",
            _VECTOR_INVALID + "Vector indexes are only supported for PAY_PER_REQUEST tables", 400)
    if len(existing) + len(new) > _VECTOR_MAX_INDEXES:
        return error_response_json("ValidationException",
            _VECTOR_INVALID + f"VectorIndex count exceeds the per-table limit of {_VECTOR_MAX_INDEXES}", 400)
    dims_by_attr = {v["VectorAttribute"]["AttributeName"]: v["Dimensions"] for v in existing}
    for vix in new:
        if vix["Dimensions"] > _VECTOR_MAX_DIMENSIONS:
            return error_response_json("ValidationException",
                _VECTOR_INVALID + f"Number of dimensions must be between 1 and {_VECTOR_MAX_DIMENSIONS} inclusive.", 400)
        for element in vix.get("SearchSchema") or []:
            if element.get("AttributeName") not in defined_attrs:
                return error_response_json("ValidationException",
                    _VECTOR_INVALID + "One element in SearchSchema is not defined in attribute definitions", 400)
        attr = vix["VectorAttribute"]["AttributeName"]
        if dims_by_attr.setdefault(attr, vix["Dimensions"]) != vix["Dimensions"]:
            return error_response_json("ValidationException",
                _VECTOR_INVALID + f"Conflicting attribute definition for '{attr}'. "
                "All VectorIndexes on the same vector attribute must use the same dimensions.", 400)
    return None


def _vector_index_record(table_name: str, vix: dict, online: bool) -> dict:
    record = {
        "IndexName": vix["IndexName"],
        "VectorAttribute": {"AttributeName": vix["VectorAttribute"]["AttributeName"]},
        "Projection": copy.deepcopy(vix.get("Projection") or {"ProjectionType": "ALL"}),
        "Dimensions": vix["Dimensions"],
        "DistanceFunction": vix["DistanceFunction"],
        "IndexArn": f"arn:aws:dynamodb:{get_region()}:{get_account_id()}:table/{table_name}/index/{vix['IndexName']}",
    }
    if vix.get("SearchSchema"):
        record["SearchSchema"] = [{"AttributeName": e["AttributeName"],
                                   "SearchSchemaElementType": e["SearchSchemaElementType"]}
                                  for e in vix["SearchSchema"]]
    if online:
        record["_online_since"] = time.time()
    return record


def _apply_vector_index_updates(name: str, table: dict, data: dict, billing_mode: str) -> tuple | None:
    updates = data.get("VectorIndexUpdates") or []
    if not updates:
        return None
    limit_error = error_response_json("LimitExceededException",
        "Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table", 400)
    if len(updates) > 1:
        return limit_error
    update = updates[0] if isinstance(updates[0], dict) else {}
    existing = table.get("VectorIndexes") or []
    if "Create" in update:
        vix = update["Create"]
        err = _vector_index_request_error(vix, "vectorIndexUpdates.1.member.create")
        if err:
            return err
        if any(_vector_phase(v) != "active" for v in existing):
            return limit_error
        index_names = {i.get("IndexName") for i in (table.get("GlobalSecondaryIndexes") or [])
                       + (table.get("LocalSecondaryIndexes") or []) + existing}
        if vix["IndexName"] in index_names:
            return error_response_json("ValidationException",
                f"Attempting to create a duplicate index: {vix['IndexName']}", 400)
        defined = {a.get("AttributeName") for a in data.get("AttributeDefinitions") or []}
        err = _vector_indexes_error([vix], existing, billing_mode, defined)
        if err:
            return err
        table["VectorIndexes"] = existing + [_vector_index_record(name, vix, online=True)]
    elif "Delete" in update:
        index_name = (update["Delete"] or {}).get("IndexName")
        vix = next((v for v in existing if v["IndexName"] == index_name), None)
        if vix is None:
            return error_response_json("ResourceNotFoundException",
                f"Requested resource not found: Index: {index_name}", 400)
        if _vector_phase(vix) == "allocating":
            return error_response_json("ResourceInUseException",
                "Attempt to change a resource which is still in use: Index creation is in resource allocation "
                "phase. Retry deletion during backfilling phase or when the index is active. "
                f"Table: {name} Index: {index_name}", 400)
        table["VectorIndexes"] = [v for v in existing if v is not vix]
    return None


def _vector_index_info(vix: dict) -> dict:
    return {k: copy.deepcopy(v) for k, v in vix.items()
            if k in ("IndexName", "VectorAttribute", "SearchSchema", "Projection", "Dimensions", "DistanceFunction")}


def _restore_create_request(target, key_schema, attr_defs, billing_mode, throughput, data, gsis, lsis, vector_indexes):
    """CreateTable input for a restore, applying the request's index overrides."""
    create_req = {"TableName": target, "KeySchema": key_schema, "BillingMode": billing_mode}
    if billing_mode == "PROVISIONED":
        create_req["ProvisionedThroughput"] = throughput or {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    for field, override, source in (("GlobalSecondaryIndexes", "GlobalSecondaryIndexOverride", gsis),
                                    ("LocalSecondaryIndexes", "LocalSecondaryIndexOverride", lsis),
                                    ("VectorIndexes", "VectorIndexOverride", vector_indexes)):
        chosen = data[override] if data.get(override) is not None else source
        if chosen:
            create_req[field] = copy.deepcopy(chosen)
    referenced = {k.get("AttributeName") for k in key_schema}
    for idx in create_req.get("GlobalSecondaryIndexes", []) + create_req.get("LocalSecondaryIndexes", []):
        referenced |= {k.get("AttributeName") for k in idx.get("KeySchema") or []}
    for vix in create_req.get("VectorIndexes", []):
        referenced |= _vector_search_attrs(vix)
    create_req["AttributeDefinitions"] = [ad for ad in attr_defs if ad.get("AttributeName") in referenced]
    return create_req


def _vector_index_description(vix: dict) -> dict:
    phase = _vector_phase(vix)
    desc = {k: copy.deepcopy(v) for k, v in vix.items() if not k.startswith("_")}
    desc["IndexStatus"] = "ACTIVE" if phase == "active" else "CREATING"
    if phase != "active":
        desc["Backfilling"] = phase == "backfilling"
    desc["IndexSizeBytes"] = 0
    desc["ItemCount"] = 0
    return desc


def _vector_search_attrs(vix: dict) -> set:
    return {e["AttributeName"] for e in vix.get("SearchSchema") or []}


def _vector_hash_attr(vix: dict) -> str | None:
    for e in vix.get("SearchSchema") or []:
        if e.get("SearchSchemaElementType") == "HASH":
            return e.get("AttributeName")
    return None


def _to_f32(text) -> float | None:
    try:
        value = struct.unpack("f", struct.pack("f", float(text)))[0]
    except (OverflowError, ValueError, TypeError):
        return None
    return value if math.isfinite(value) else None


def _f32_number_text(value: float) -> str:
    packed = struct.pack("f", value)
    for digits in range(1, 10):
        text = f"{value:.{digits}g}"
        if struct.pack("f", float(text)) == packed:
            break
    return _ddb_canonicalize_number(text) or text


def _vector_write_reason(table: dict, item: dict, update_expr: bool = False) -> str | None:
    """AWS's message for an item a vector index on `table` cannot take, or None."""
    attr_defs = {ad["AttributeName"]: ad["AttributeType"] for ad in (table.get("AttributeDefinitions") or [])}
    for vix in table.get("VectorIndexes") or []:
        name = vix["IndexName"]
        attr = vix["VectorAttribute"]["AttributeName"]
        if attr in item:
            raw = item[attr]
            values = raw.get("L") if isinstance(raw, dict) and len(raw) == 1 else None
            if not isinstance(values, list):
                return ("One or more parameter values were invalid. Invalid type for parameter "
                        f"{attr}, Expected: 32-bit floating point number list IndexName: {name}")
            for i, element in enumerate(values):
                etype = next(iter(element)) if isinstance(element, dict) and len(element) == 1 else "M"
                if etype != "N" or _to_f32(element["N"]) is None:
                    return ("One or more parameter values were invalid. Invalid type for parameter "
                            f"{attr}[{i}], Expected: 32-bit floating point number, Actual: {etype}. IndexName: {name}")
            if len(values) != vix["Dimensions"]:
                return ("One or more parameter values were invalid. Invalid size for parameter "
                        f"{attr}, Expected: {vix['Dimensions']}, Actual: {len(values)} IndexName: {name}")
        for key_name in _vector_search_attrs(vix):
            raw = item.get(key_name)
            if not isinstance(raw, dict) or len(raw) != 1:
                continue
            actual_type = next(iter(raw))
            expected_type = attr_defs.get(key_name)
            if expected_type and actual_type != expected_type:
                return (_VECTOR_INVALID + f"Type mismatch for Index Key {key_name} Expected: {expected_type} "
                        f"Actual: {actual_type} IndexName: {name}")
            if (actual_type == "S" and raw["S"] == "") or (actual_type == "B" and not raw["B"]):
                kind = "string" if actual_type == "S" else "binary"
                if update_expr:
                    return ("One or more parameter values are not valid. The update expression attempted to update a "
                            "secondary index key to a value that is not supported. The AttributeValue for a key "
                            f"attribute cannot contain an empty {kind} value.")
                return ("One or more parameter values are not valid. A value specified for a secondary index key is "
                        f"not supported. The AttributeValue for a key attribute cannot contain an empty {kind} value. "
                        f"IndexName: {name}, IndexKey: {key_name}")
    return None


def _vector_entry(table: dict, vix: dict, item: dict | None) -> dict | None:
    """The item as `vix` stores it, or None when the item is not in the index."""
    if not item:
        return None
    attr = vix["VectorAttribute"]["AttributeName"]
    values = (item.get(attr) or {}).get("L")
    if not isinstance(values, list) or len(values) != vix["Dimensions"]:
        return None
    vector = [_to_f32((v or {}).get("N")) for v in values]
    if any(v is None for v in vector):
        return None
    hash_attr = _vector_hash_attr(vix)
    if hash_attr and hash_attr not in item:
        return None
    projection = vix.get("Projection") or {}
    ptype = projection.get("ProjectionType", "ALL")
    keep = {table.get("pk_name"), table.get("sk_name"), attr} | _vector_search_attrs(vix)
    if ptype == "INCLUDE":
        keep |= set(projection.get("NonKeyAttributes") or [])
    entry = {k: v for k, v in item.items() if ptype == "ALL" or k in keep}
    entry[attr] = {"L": [{"N": _f32_number_text(v)} for v in vector]}
    return entry


def _vector_entry_bytes(vix: dict, entry: dict) -> int:
    attr = vix["VectorAttribute"]["AttributeName"]
    size = sum(len(k.encode("utf-8")) + _attribute_value_size(v) for k, v in entry.items() if k != attr)
    return size + len(attr.encode("utf-8")) + 4 * vix["Dimensions"]


def _vector_write_bytes(table: dict, old_item: dict | None, new_item: dict | None) -> dict:
    """VectorWriteRequestBytes per index whose stored entry the write changes."""
    charged = {}
    for vix in table.get("VectorIndexes") or []:
        old_entry = _vector_entry(table, vix, old_item)
        new_entry = _vector_entry(table, vix, new_item)
        if old_entry == new_entry:
            continue
        entry = new_entry if new_entry is not None else old_entry
        charged[vix["IndexName"]] = float(max(_VECTOR_WRITE_FLOOR_BYTES, _vector_entry_bytes(vix, entry)))
    return charged


def _vector_condition_terms(expr: str, attr_names: dict, attr_values: dict, vix: dict):
    """Parse `a = :v AND b = :w` into [(name, value)], or return an error message."""
    comparator_error = "Invalid SearchConditionExpression: Invalid comparator used in SearchConditionExpression"
    terms = []
    for part in re.split(r"\s+AND\s+", expr.strip(), flags=re.IGNORECASE):
        match = re.fullmatch(r"\s*(#?[A-Za-z0-9_]+)\s*(<>|<=|>=|=|<|>)\s*(:[A-Za-z0-9_]+)\s*", part)
        lhs = match.group(1) if match else re.split(r"[\s<>=(]", part.strip(), maxsplit=1)[0]
        name = attr_names.get(lhs, lhs) if lhs.startswith("#") else lhs
        if name and name not in _vector_search_attrs(vix):
            return None, ("SearchConditionExpression must not contain any attributes that is not in SearchSchema. "
                          f"Invalid attribute: {name}")
        if not match or match.group(2) != "=":
            return None, comparator_error
        if match.group(3) not in attr_values:
            return None, ("Invalid SearchConditionExpression: An expression attribute value used in expression "
                          f"is not defined; attribute value: {match.group(3)}")
        terms.append((name, attr_values[match.group(3)]))
    return terms, None


def _vector_score(fn: str, query: list, vector: list) -> float:
    dot = sum(a * b for a, b in zip(query, vector))
    if fn == "DOT_PRODUCT":
        score = dot
    elif fn == "EUCLIDEAN":
        score = math.sqrt(sum((a - b) ** 2 for a, b in zip(query, vector)))
    else:
        norms = math.sqrt(sum(a * a for a in query)) * math.sqrt(sum(b * b for b in vector))
        score = 1.0 - (dot / norms if norms else 0.0)
    return _to_f32(score) or 0.0


def _search_vectors(data):
    name = _normalize_table_name(data.get("TableName"))
    err = _validate_data_plane_table_name(name)
    if err:
        return err
    top_k = data.get("TopK")
    if not isinstance(top_k, int) or isinstance(top_k, bool) or top_k < 1:
        return error_response_json("ValidationException",
            f"1 validation error detected: Value '{top_k}' at 'topK' failed to satisfy constraint: "
            "Member must have value greater than or equal to 1", 400)
    search_vector = data.get("SearchVector")
    if not isinstance(search_vector, list) or not search_vector:
        return error_response_json("ValidationException",
            "1 validation error detected: Value '[]' at 'searchVector' failed to satisfy constraint: "
            "Member must have length greater than or equal to 1", 400)
    if len(search_vector) > _VECTOR_MAX_DIMENSIONS:
        return error_response_json("ValidationException",
            "1 validation error detected: Value at 'searchVector' failed to satisfy constraint: "
            f"Member must have length less than or equal to {_VECTOR_MAX_DIMENSIONS}", 400)
    err = _validate_return_consumed_capacity(data)
    if err:
        return err
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", "Requested resource not found", 400)
    if top_k > _VECTOR_TOPK_MAX:
        return error_response_json("ValidationException",
            f"Provided TopK value '{top_k}' is out of valid range. The value must be between 1 and {_VECTOR_TOPK_MAX} inclusive", 400)
    index_name = data.get("IndexName")
    vix = next((v for v in table.get("VectorIndexes") or [] if v["IndexName"] == index_name), None)
    phase = _vector_phase(vix) if vix else None
    if vix is None or phase == "allocating":
        return error_response_json("ValidationException",
            f"The table does not have the specified index: {index_name}", 400)
    if phase == "backfilling":
        return error_response_json("ValidationException",
            f"Cannot search backfilling vector index: {index_name}", 400)
    query = []
    for element in search_vector:
        value = _to_f32(element.get("N")) if isinstance(element, dict) and set(element) == {"N"} else None
        if value is None:
            return error_response_json("ValidationException",
                "Search vector contains invalid values. All values in the search vector must be a 32-bit "
                "floating-point number attribute", 400)
        query.append(value)
    if len(query) != vix["Dimensions"]:
        return error_response_json("ValidationException",
            f"Input search vector dimension {len(query)} does not match vector index dimension {vix['Dimensions']}", 400)
    condition = data.get("SearchConditionExpression")
    terms = []
    if condition:
        terms, msg = _vector_condition_terms(condition, data.get("ExpressionAttributeNames") or {},
                                             data.get("ExpressionAttributeValues") or {}, vix)
        if msg:
            return error_response_json("ValidationException", msg, 400)
    elif _vector_hash_attr(vix):
        return error_response_json("ValidationException",
            "SearchConditionExpression must be provided when SearchSchema has a HASH key", 400)

    attr = vix["VectorAttribute"]["AttributeName"]
    scored = []
    candidates = 0
    for item in (it for bucket in list(table["items"].values()) for it in list(bucket.values())):
        entry = _vector_entry(table, vix, item)
        if entry is None or any(entry.get(k) != v for k, v in terms):
            continue
        candidates += 1
        vector = [float(v["N"]) for v in entry[attr]["L"]]
        scored.append((_vector_score(vix["DistanceFunction"], query, vector), entry))
    scored.sort(key=lambda pair: -pair[0] if vix["DistanceFunction"] == "DOT_PRODUCT" else pair[0])
    results = []
    for score, entry in scored[:top_k]:
        if data.get("ProjectionExpression"):
            shown = _project_item(entry, data["ProjectionExpression"], data.get("ExpressionAttributeNames") or {})
        else:
            shown = {k: v for k, v in entry.items() if k != attr}
        results.append({"Item": shown, "Score": score})
    result = {"SearchResults": results}
    if data.get("ReturnConsumedCapacity", "NONE") != "NONE":
        result["ConsumedCapacity"] = {
            "VectorSearchRequestBytes": float(4 * vix["Dimensions"] * (candidates + 1))}
    return json_response(result)


def _key_type_mismatch_reason(table, attrs):
    """Return the AWS message for a key attribute present with the wrong type,
    else None. Unlike an empty key value (which real AWS rejects up front with a
    ValidationException), a wrong-typed key inside a transaction is surfaced as a
    per-item ValidationError cancellation reason."""
    if not isinstance(attrs, dict):
        return None
    for key_name in (table.get("pk_name"), table.get("sk_name")):
        if not key_name or key_name not in attrs:
            continue
        expected = _get_attr_type(table, key_name)
        raw = attrs.get(key_name)
        if isinstance(raw, dict) and len(raw) == 1:
            actual = next(iter(raw))
            if actual != expected:
                return (f"One or more parameter values were invalid: "
                        f"Type mismatch for key {key_name} expected: {expected} actual: {actual}")
    return None


def _transact_validation_cancel_response(total, val_reasons):
    """Build a TransactionCanceledException whose CancellationReasons carry a
    positional ValidationError for each failing member (Code "None" otherwise),
    matching how real DynamoDB reports per-item validation failures in a
    transaction (wrong-typed keys, update-expression type errors)."""
    reasons = []
    for i in range(total):
        if i in val_reasons:
            reasons.append({"Code": "ValidationError", "Message": val_reasons[i]})
        else:
            reasons.append({"Code": "None"})
    msg = ("Transaction cancelled, please refer cancellation reasons for specific reasons ["
           + ", ".join(r["Code"] for r in reasons) + "]")
    body = json.dumps({
        "__type": "TransactionCanceledException",
        "message": msg,
        "CancellationReasons": reasons,
    }, ensure_ascii=False).encode("utf-8")
    return 400, {
        "Content-Type": "application/x-amz-json-1.0",
        "x-amzn-errortype": "TransactionCanceledException",
    }, body


def _between_bounds_error(lo_av, hi_av):
    """Static BETWEEN bounds check for KeyConditionExpression. AWS rejects an
    inverted range (lower > upper) with a ValidationException at parse time."""
    if not isinstance(lo_av, dict) or not isinstance(hi_av, dict):
        return None
    if len(lo_av) != 1 or len(hi_av) != 1:
        return None
    (lt, lv), = lo_av.items()
    (ht, hv), = hi_av.items()
    if lt != ht:
        return None
    try:
        if lt == "N":
            inverted = Decimal(lv) > Decimal(hv)
        elif lt == "B":
            inverted = base64.b64decode(lv) > base64.b64decode(hv)
        else:
            inverted = str(lv) > str(hv)
    except (InvalidOperation, TypeError, ValueError, binascii.Error):
        return None
    if not inverted:
        return None
    return error_response_json("ValidationException",
        "Invalid KeyConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; "
        f"lower bound operand: AttributeValue: {{{lt}:{lv}}}, upper bound operand: AttributeValue: {{{ht}:{hv}}}", 400)


def _empty_key_value_error(key_name, raw_value):
    """AWS rejects empty string/binary values for key attributes on every data
    plane operation (reads and deletes included), not just writes."""
    if not isinstance(raw_value, dict) or len(raw_value) != 1:
        return None
    (vtype, vval), = raw_value.items()
    if vtype == "S" and vval == "":
        return error_response_json("ValidationException",
            f"One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: {key_name}", 400)
    if vtype == "B" and vval in ("", b""):
        return error_response_json("ValidationException",
            f"One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: {key_name}", 400)
    return None


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


def _key_condition_av(condition, key_conditions, eav, attr_names, key_name):
    """The typed AttributeValue bound to ``key_name`` in a Query key condition,
    for the type-vs-schema check. Covers legacy KeyConditions and the
    KeyConditionExpression forms (``key <op> :v`` / ``:v = key``,
    ``begins_with(key, :v)``, ``key BETWEEN :lo AND :hi``); returns the first
    bound value, which is enough to catch a type mismatch."""
    if key_conditions:
        cond = key_conditions.get(key_name)
        if isinstance(cond, dict):
            avl = cond.get("AttributeValueList") or []
            return avl[0] if avl else None
        return None
    if not condition:
        return None
    refs = [key_name] + [a for a, r in attr_names.items() if r == key_name]
    for ref in refs:
        for pat in (rf'(?:^|[\s(]){re.escape(ref)}\s*(?:<=|>=|=|<|>)\s*(:\w+)',
                    rf'(:\w+)\s*(?:<=|>=|=|<|>)\s*{re.escape(ref)}(?:$|[\s)])',
                    rf'begins_with\s*\(\s*{re.escape(ref)}\s*,\s*(:\w+)\s*\)',
                    rf'{re.escape(ref)}\s+BETWEEN\s+(:\w+)'):
            m = re.search(pat, condition, re.I)
            if m and m.group(1) in eav:
                return eav[m.group(1)]
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


def _multi_sort_condition_ok(tokens, ean, range_names):
    """Sort-key attributes of a multi-attribute key must be queried left to
    right without skipping any, with equality on all but the last."""
    ops = {}
    i = 0
    while i < len(tokens):
        kind, val = tokens[i]
        if kind == "IDENT" and val.lower() == "begins_with" and i + 2 < len(tokens):
            arg = tokens[i + 2]
            name = ean.get(arg[1]) if arg[0] == "NAME_REF" else arg[1]
            ops.setdefault(name, []).append("range")
            i += 3
            continue
        name = ean.get(val) if kind == "NAME_REF" else (val if kind == "IDENT" else None)
        if name in range_names and i + 1 < len(tokens):
            eq = tokens[i + 1][0] == "EQ" or (i >= 2 and tokens[i - 1][0] == "EQ" and tokens[i - 2][0] == "VALUE_REF")
            ops.setdefault(name, []).append("eq" if eq else "range")
        i += 1
    used = [n for n in range_names if n in ops]
    if used != range_names[:len(used)] or any(len(ops[n]) != 1 for n in used):
        return False
    return all(ops[n] == ["eq"] for n in used[:-1])


def _index_order_keys(table, sk_name):
    """Ordered list of (attr_name, attr_type) used to sort a GSI/LSI Query
    result deterministically: (INDEX_SORT, BASE_PK, BASE_SK). Real DynamoDB
    orders by (INDEX_HASH, INDEX_SORT, BASE_PK, BASE_SK); INDEX_HASH is fixed
    per Query so it drops out of the ordering. The base-table keys break ties
    when multiple items share the same INDEX_SORT value (or when the GSI is
    hash-only)."""
    seen = set()
    keys = []
    index_sorts = list(sk_name) if isinstance(sk_name, (list, tuple)) else [sk_name]
    for n in (*index_sorts, table.get("pk_name"), table.get("sk_name")):
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


def _apply_index_projection(item, table, index_name):
    """Restrict an item to the attributes that the queried index actually
    projects. AWS GSIs/LSIs declare a Projection of ALL / KEYS_ONLY / INCLUDE
    [NonKeyAttributes]; only those attributes are visible through the index.
    Returns the trimmed dict (with original wrapping)."""
    if not index_name:
        return item
    idx_def = None
    for collection in ("GlobalSecondaryIndexes", "LocalSecondaryIndexes"):
        for idx in (table.get(collection) or []):
            if idx.get("IndexName") == index_name:
                idx_def = idx
                break
        if idx_def:
            break
    if not idx_def:
        return item
    proj_cfg = idx_def.get("Projection") or {}
    ptype = proj_cfg.get("ProjectionType", "ALL")
    if ptype == "ALL":
        return item
    # Always keep base-table keys + the index's own keys.
    keep = set()
    keep.add(table.get("pk_name"))
    if table.get("sk_name"):
        keep.add(table["sk_name"])
    for ks in (idx_def.get("KeySchema") or []):
        an = ks.get("AttributeName")
        if an:
            keep.add(an)
    if ptype == "INCLUDE":
        for nk in (proj_cfg.get("NonKeyAttributes") or []):
            keep.add(nk)
    # KEYS_ONLY: only the keys (already added).
    return {k: v for k, v in item.items() if k in keep}


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
    exist. For list indices, returns a sparse dict-based representation
    so that _merge_projection can identify and merge same-index paths."""
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
        # Represent as a sparse list with the element at position `key`.
        # This allows _merge_projection to correctly merge same-index paths.
        sparse = [None] * (key + 1)
        sparse[key] = sub
        return {"L": sparse}
    return None


def _merge_projection(into: dict | None, add: dict | None) -> dict | None:
    """Merge two projection-result AttributeValues at the same level.
    Both must share the same wrapping type (M or L); merging M unions keys,
    merging L merges elements that share the same index (AWS behaviour: two
    paths referencing sub-attributes of the same list index produce one element,
    not two separate single-element lists)."""
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
        # AWS merges sub-attributes of the same list index into one element.
        # We reconstruct a merged list: for each index present in either side,
        # deep-merge the elements if both sides provide it, otherwise take
        # whichever side has it.
        # Represent each list as a dict[index -> value] for merging.
        def _to_indexed(lst):
            return {i: v for i, v in enumerate(lst)}
        into_idx = _to_indexed(it_v)
        add_idx = _to_indexed(ad_v)
        all_indices = sorted(set(into_idx) | set(add_idx))
        merged_list = []
        for idx in all_indices:
            if idx in into_idx and idx in add_idx:
                merged_list.append(_merge_projection(into_idx[idx], add_idx[idx]))
            elif idx in into_idx:
                merged_list.append(into_idx[idx])
            else:
                merged_list.append(add_idx[idx])
        return {"L": merged_list}
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

    # Resolve all paths to their canonical (alias-substituted) string forms
    # and check for duplicates or parent/child overlaps — AWS rejects these.
    resolved_paths = []
    for path in paths:
        parts = _parse_projection_path(path, attr_names or {})
        resolved_paths.append(parts)

    def _path_str(parts):
        s = ""
        for kind, key in parts:
            if kind == "name":
                s += ("." if s else "") + key
            else:
                s += f"[{key}]"
        return s

    canonical = [_path_str(p) for p in resolved_paths]
    # Check for duplicates and parent/child overlaps.
    for i, p1 in enumerate(canonical):
        for j, p2 in enumerate(canonical):
            if i >= j:
                continue
            if p1 == p2:
                raise ValueError(
                    f"Invalid ProjectionExpression: Two document paths overlap with each other; "
                    f"must remove or rewrite one of these paths; path one: [{p1}], path two: [{p2}]"
                )
            # Check parent/child: p1 is ancestor of p2 if p2 starts with p1 followed by . or [
            p1_prefix_dot = p1 + "."
            p1_prefix_bracket = p1 + "["
            if p2.startswith(p1_prefix_dot) or p2.startswith(p1_prefix_bracket):
                raise ValueError(
                    f"Invalid ProjectionExpression: Two document paths overlap with each other; "
                    f"must remove or rewrite one of these paths; path one: [{p1}], path two: [{p2}]"
                )
            if p1.startswith(p2 + ".") or p1.startswith(p2 + "["):
                raise ValueError(
                    f"Invalid ProjectionExpression: Two document paths overlap with each other; "
                    f"must remove or rewrite one of these paths; path one: [{p2}], path two: [{p1}]"
                )

    result: dict = {}
    for parts in resolved_paths:
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
    # Compact sparse lists per attribute: `result` maps attribute names to
    # AttributeValues, so compaction runs on each value (never on the outer
    # map, whose keys are attribute names, not type tags).
    return {name: _compact_projection(av) for name, av in result.items()}


def _compact_projection(value):
    """Recursively compact projection results: remove None placeholders from
    sparse L lists, preserving order of non-None elements."""
    if not isinstance(value, dict):
        return value
    if len(value) == 1:
        (vtype, vval), = value.items()
        if vtype == "L" and isinstance(vval, list):
            compacted = [_compact_projection(x) for x in vval if x is not None]
            return {"L": compacted}
        if vtype == "M" and isinstance(vval, dict):
            return {"M": {k: _compact_projection(v) for k, v in vval.items()}}
    return value


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

# Derived per table, never persisted: {index_name: {index hash value:
# {(base_pk, base_sk): None}}} (insertion-ordered), so a GSI/LSI Query reads its own partition
# instead of every item. Built on first use; dropped whenever items are
# replaced wholesale or the index set changes.
_INDEX_MEMBERS = "_index_members"


def _index_key_lists(index):
    """(HASH names, RANGE names) of an index KeySchema, in order; a GSI may
    have up to four of each (multi-attribute keys)."""
    schema = index.get("KeySchema", [])
    return ([k.get("AttributeName") for k in schema if k.get("KeyType") == "HASH"],
            [k.get("AttributeName") for k in schema if k.get("KeyType") == "RANGE"])


def _index_def(table, index_name):
    for index in table.get("GlobalSecondaryIndexes", []) + table.get("LocalSecondaryIndexes", []):
        if index.get("IndexName") == index_name:
            return index
    return None


def _index_key_schemas(table):
    for index in table.get("GlobalSecondaryIndexes", []) + table.get("LocalSecondaryIndexes", []):
        hashes, ranges = _index_key_lists(index)
        yield index.get("IndexName"), hashes, ranges


def _index_partition(item, hashes, ranges):
    """The index partition of an item: its HASH value, or the tuple of them
    for a multi-attribute key; None when any key attribute is missing."""
    if not hashes or any(n not in item for n in hashes + ranges):
        return None
    if len(hashes) == 1:
        return _extract_key_val(item[hashes[0]])
    return tuple(_extract_key_val(item[n]) for n in hashes)


def _index_members(table):
    members = table.get(_INDEX_MEMBERS)
    if members is None:
        members = {}
        for name, hash_key, range_key in _index_key_schemas(table):
            partitions = members.setdefault(name, {})
            for base_pk, sk_map in table["items"].items():
                for base_sk, item in sk_map.items():
                    partition = _index_partition(item, hash_key, range_key)
                    if partition is not None:
                        partitions.setdefault(partition, {})[(base_pk, base_sk)] = None
        table[_INDEX_MEMBERS] = members
    return members


def _index_item(table, base_pk, base_sk, item, add):
    members = table.get(_INDEX_MEMBERS)
    if members is None:
        return
    for name, hash_key, range_key in _index_key_schemas(table):
        if name not in members:
            table.pop(_INDEX_MEMBERS, None)
            return
        partition = _index_partition(item, hash_key, range_key)
        if partition is None:
            continue
        partitions = members[name]
        if add:
            partitions.setdefault(partition, {})[(base_pk, base_sk)] = None
        elif partition in partitions:
            partitions[partition].pop((base_pk, base_sk), None)
            if not partitions[partition]:
                del partitions[partition]


def _set_item(table, pk_val, sk_val, item):
    """Store an item, keeping ItemCount and index membership current."""
    old_item = table["items"].get(pk_val, {}).get(sk_val)
    if old_item is not None:
        _index_item(table, pk_val, sk_val, old_item, add=False)
    else:
        table["ItemCount"] = table.get("ItemCount", 0) + 1
        table["TableSizeBytes"] = table["ItemCount"] * 200
    table["items"].setdefault(pk_val, {})[sk_val] = item
    _index_item(table, pk_val, sk_val, item, add=True)
    return old_item


def _remove_item(table, pk_val, sk_val):
    """Delete an item, keeping ItemCount and index membership current."""
    sk_map = table["items"].get(pk_val)
    old_item = sk_map.pop(sk_val, None) if sk_map is not None else None
    if sk_map is not None and not sk_map:
        del table["items"][pk_val]
    if old_item is not None:
        _index_item(table, pk_val, sk_val, old_item, add=False)
        table["ItemCount"] = max(0, table.get("ItemCount", 0) - 1)
        table["TableSizeBytes"] = table["ItemCount"] * 200
    return old_item


def _items_replaced(table):
    """After table["items"] is swapped wholesale: recount and rebuild lazily."""
    table.pop(_INDEX_MEMBERS, None)
    _update_counts(table)


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


def _capacity_kb(nbytes: int) -> float:
    """Write units for an entry of `nbytes` bytes: 1 WCU per started KB."""
    return max(1.0, float(-(-nbytes // 1024)))


def _index_write_view(table, idx, item):
    """The (key signature, projected view) of `item` in index `idx`, or None
    when the item is absent from the index (missing an index key attribute)."""
    if not item:
        return None
    key_names = [ks.get("AttributeName") for ks in idx.get("KeySchema", []) or []]
    for k in key_names:
        if k not in item:
            return None
    proj = idx.get("Projection") or {}
    ptype = proj.get("ProjectionType", "ALL")
    if ptype == "ALL":
        view = dict(item)
    else:
        view = {}
        table_keys = [table.get("pk_name")] + ([table.get("sk_name")] if table.get("sk_name") else [])
        for k in list(key_names) + table_keys:
            if k and k in item:
                view[k] = item[k]
        if ptype == "INCLUDE":
            for k in proj.get("NonKeyAttributes") or []:
                if k in item:
                    view[k] = item[k]
    key_sig = json.dumps([item.get(k) for k in key_names], sort_keys=True)
    return key_sig, view


def _index_write_units(table, idx, old_item, new_item):
    """Write units a Put/Update/Delete costs one index, per real DynamoDB:
    nothing when the index's stored view is unchanged, one write to insert or
    delete an entry or rewrite it in place, two (delete + insert) when the
    index key changes so the entry moves."""
    ov = _index_write_view(table, idx, old_item)
    nv = _index_write_view(table, idx, new_item)
    if ov is None and nv is None:
        return 0.0
    if ov is None:
        return _capacity_kb(_item_size_bytes(nv[1]))
    if nv is None:
        return _capacity_kb(_item_size_bytes(ov[1]))
    if ov[0] != nv[0]:
        return _capacity_kb(_item_size_bytes(ov[1])) + _capacity_kb(_item_size_bytes(nv[1]))
    if ov[1] == nv[1]:
        return 0.0
    return max(_capacity_kb(_item_size_bytes(ov[1])), _capacity_kb(_item_size_bytes(nv[1])))


def _add_consumed_capacity(result, data, table_name, write=False, old_item=None,
                           new_item=None, index_name=None):
    rc = data.get("ReturnConsumedCapacity", "NONE")
    if rc == "NONE":
        return
    table = _tables.get(table_name, {})
    gsi_units: dict = {}
    lsi_units: dict = {}
    if write:
        old_size = _item_size_bytes(old_item) if old_item else 0
        new_size = _item_size_bytes(new_item) if new_item else 0
        table_units = _capacity_kb(max(old_size, new_size))
        for idx in table.get("GlobalSecondaryIndexes", []) or []:
            u = _index_write_units(table, idx, old_item, new_item)
            if u:
                gsi_units[idx["IndexName"]] = u
        for idx in table.get("LocalSecondaryIndexes", []) or []:
            u = _index_write_units(table, idx, old_item, new_item)
            if u:
                lsi_units[idx["IndexName"]] = u
    else:
        # Eventually-consistent reads cost 0.5 RCU; strongly-consistent 1.0.
        consistent = data.get("ConsistentRead", False)
        table_units = 1.0 if consistent else 0.5
        if index_name:
            # The index carries the read; the base table's share is 0.
            names_gsi = {i.get("IndexName") for i in table.get("GlobalSecondaryIndexes", []) or []}
            names_lsi = {i.get("IndexName") for i in table.get("LocalSecondaryIndexes", []) or []}
            if index_name in names_gsi:
                gsi_units[index_name] = table_units
                table_units = 0.0
            elif index_name in names_lsi:
                lsi_units[index_name] = table_units
                table_units = 0.0
    total = table_units + sum(gsi_units.values()) + sum(lsi_units.values())
    cap = {"TableName": table_name, "CapacityUnits": total}
    if rc == "INDEXES":
        cap["Table"] = {"CapacityUnits": table_units}
        if gsi_units:
            cap["GlobalSecondaryIndexes"] = {n: {"CapacityUnits": u} for n, u in gsi_units.items()}
        if lsi_units:
            cap["LocalSecondaryIndexes"] = {n: {"CapacityUnits": u} for n, u in lsi_units.items()}
        vector_bytes = _vector_write_bytes(table, old_item, new_item) if write else {}
        if vector_bytes:
            cap["VectorIndexes"] = {n: {"VectorWriteRequestBytes": b} for n, b in vector_bytes.items()}
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


def _diff_attributes(old_item, new_item, updated_attrs, return_old=True):
    """Return the old or new version of attributes that were updated.

    - UPDATED_OLD: report the prior value of each updated attribute, omitting
      additions where there was no prior value.
    - UPDATED_NEW: report the new value of each updated attribute, omitting
      removals where there is no new value (per AWS, REMOVE-only updates with
      UPDATED_NEW omit Attributes entirely).
    """
    src = old_item if return_old else new_item
    result = {}
    for path in updated_attrs:
        if isinstance(path, str):
            path = (path,)
        top = path[0]
        # Whole-attribute update, or a path with list indices: return the
        # top-level attribute (AWS only fragments plain map paths).
        if len(path) == 1 or not all(isinstance(part, str) for part in path):
            v = src.get(top)
            if v is not None:
                result[top] = v
            continue
        # Nested map path: AWS returns only the changed fragment, e.g.
        # SET parent.child = :v with UPDATED_NEW -> {parent: {M: {child: v}}}.
        v = _get_at_path(src, list(path))
        if v is None:
            continue
        node = result.setdefault(top, {"M": {}})
        if not (isinstance(node, dict) and set(node.keys()) == {"M"}):
            continue  # whole attribute already reported
        cur = node
        for part in path[1:-1]:
            cur = cur["M"].setdefault(part, {"M": {}})
        cur["M"][path[-1]] = v
    return result


def reset():
    with _lock:
        _tables.clear()
        _tags.clear()
        _ttl_settings.clear()
        _pitr_settings.clear()
        _stream_records.clear()
        _stream_trimmed.clear()
        _closed_streams.clear()
        _kinesis_destinations.clear()
        _backups.clear()
        _contributor_insights.clear()
        _resource_policies.clear()
        _exports.clear()
        _imports.clear()
        _txn_idempotency.clear()
