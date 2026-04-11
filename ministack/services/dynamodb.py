"""
DynamoDB Service Emulator.
Supports: CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable,
          PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan,
          BatchWriteItem, BatchGetItem, TransactWriteItems, TransactGetItems,
          DescribeTimeToLive, UpdateTimeToLive,
          DescribeContinuousBackups, UpdateContinuousBackups, DescribeEndpoints,
          TagResource, UntagResource, ListTagsOfResource.
Uses X-Amz-Target header for action routing (JSON API).
"""

import copy
import os
import json
import logging
import re
import threading
import time
from collections import defaultdict
from decimal import Decimal, InvalidOperation

from ministack.core.responses import (
    AccountScopedDict,
    get_account_id,
    error_response_json,
    json_response,
    new_uuid,
    now_iso,
)

logger = logging.getLogger("dynamodb")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

from ministack.core.persistence import load_state, PERSIST_STATE

_tables = AccountScopedDict()
_tags = AccountScopedDict()
_ttl_settings = AccountScopedDict()
_pitr_settings = AccountScopedDict()
_lock = threading.Lock()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {"tables": copy.deepcopy(_tables), "tags": copy.deepcopy(_tags), "ttl_settings": copy.deepcopy(_ttl_settings), "pitr_settings": copy.deepcopy(_pitr_settings)}


def restore_state(data):
    if data:
        _tables.update(data.get("tables", {}))
        # Restore items as defaultdict(dict) — JSON deserializes as plain dict
        for tbl in _tables.values():
            if isinstance(tbl.get("items"), dict) and not isinstance(tbl["items"], defaultdict):
                tbl["items"] = defaultdict(dict, tbl["items"])
        _tags.update(data.get("tags", {}))
        _ttl_settings.update(data.get("ttl_settings", {}))
        _pitr_settings.update(data.get("pitr_settings", {}))


_restored = load_state("dynamodb")
if _restored:
    restore_state(_restored)

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


def _emit_stream_event(table_name: str, event_name: str, old_item: dict | None, new_item: dict | None):
    """Emit a DynamoDB Streams record if the table has StreamSpecification enabled."""
    table = _tables.get(table_name)
    if not table:
        return
    spec = table.get("StreamSpecification")
    if not spec or not spec.get("StreamEnabled"):
        return

    view_type = spec.get("StreamViewType", "NEW_AND_OLD_IMAGES")
    record: dict = {
        "eventID": new_uuid(),
        "eventName": event_name,
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodb",
        "awsRegion": REGION,
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

    if table_name not in _stream_records:
        _stream_records[table_name] = []
    _stream_records[table_name].append(record)

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
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("UnknownOperationException", f"Unknown operation: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# Table operations
# ---------------------------------------------------------------------------

def _create_table(data):
    name = data.get("TableName")
    if not name:
        return error_response_json("ValidationException", "TableName is required", 400)
    if name in _tables:
        return error_response_json("ResourceInUseException", f"Table already exists: {name}", 400)

    key_schema = data.get("KeySchema", [])
    attr_defs = data.get("AttributeDefinitions", [])
    pk_name = sk_name = None
    for ks in key_schema:
        if ks["KeyType"] == "HASH":
            pk_name = ks["AttributeName"]
        elif ks["KeyType"] == "RANGE":
            sk_name = ks["AttributeName"]

    gsis = copy.deepcopy(data.get("GlobalSecondaryIndexes", []))
    lsis = copy.deepcopy(data.get("LocalSecondaryIndexes", []))
    billing_mode = data.get("BillingMode", "PROVISIONED")
    gsi_default_throughput = (
        {"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
        if billing_mode == "PAY_PER_REQUEST"
        else {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    )
    for gsi in gsis:
        gsi.setdefault("IndexStatus", "ACTIVE")
        gsi.setdefault("ProvisionedThroughput", gsi_default_throughput)
        gsi["IndexArn"] = f"arn:aws:dynamodb:{REGION}:{get_account_id()}:table/{name}/index/{gsi['IndexName']}"
        gsi["IndexSizeBytes"] = 0
        gsi["ItemCount"] = 0
    for lsi in lsis:
        lsi["IndexArn"] = f"arn:aws:dynamodb:{REGION}:{get_account_id()}:table/{name}/index/{lsi['IndexName']}"
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
        "CreationDateTime": time.time(),
        "ItemCount": 0,
        "TableSizeBytes": 0,
        "TableArn": f"arn:aws:dynamodb:{REGION}:{get_account_id()}:table/{name}",
        "TableId": new_uuid(),
        "GlobalSecondaryIndexes": gsis,
        "LocalSecondaryIndexes": lsis,
        "ProvisionedThroughput": {"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
            if data.get("BillingMode") == "PAY_PER_REQUEST"
            else data.get("ProvisionedThroughput", {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}),
        "BillingModeSummary": {"BillingMode": data.get("BillingMode", "PROVISIONED")},
        "StreamSpecification": data.get("StreamSpecification"),
        "SSEDescription": data.get("SSESpecification"),
    }
    return json_response({"TableDescription": _table_description(name)})


def _delete_table(data):
    name = data.get("TableName")
    if name not in _tables:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)
    desc = _table_description(name)
    desc["TableStatus"] = "DELETING"
    del _tables[name]
    _tags.pop(desc.get("TableArn", ""), None)
    _ttl_settings.pop(name, None)
    _pitr_settings.pop(name, None)
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

    if "ProvisionedThroughput" in data:
        table["ProvisionedThroughput"] = data["ProvisionedThroughput"]
    if "BillingMode" in data:
        table["BillingModeSummary"] = {"BillingMode": data["BillingMode"]}
        if data["BillingMode"] == "PAY_PER_REQUEST":
            table["ProvisionedThroughput"] = {"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
    if "AttributeDefinitions" in data:
        table["AttributeDefinitions"] = data["AttributeDefinitions"]
    if "StreamSpecification" in data:
        table["StreamSpecification"] = data["StreamSpecification"]

    for update in data.get("GlobalSecondaryIndexUpdates", []):
        if "Create" in update:
            gsi_def = copy.deepcopy(update["Create"])
            gsi_def.setdefault("IndexStatus", "ACTIVE")
            current_billing = table.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED")
            gsi_def.setdefault(
                "ProvisionedThroughput",
                {"ReadCapacityUnits": 0, "WriteCapacityUnits": 0}
                if current_billing == "PAY_PER_REQUEST"
                else {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
            gsi_def["IndexArn"] = f"arn:aws:dynamodb:{REGION}:{get_account_id()}:table/{name}/index/{gsi_def['IndexName']}"
            gsi_def["IndexSizeBytes"] = 0
            gsi_def["ItemCount"] = 0
            table["GlobalSecondaryIndexes"].append(gsi_def)
        elif "Delete" in update:
            idx_name = update["Delete"]["IndexName"]
            table["GlobalSecondaryIndexes"] = [g for g in table["GlobalSecondaryIndexes"] if g["IndexName"] != idx_name]
        elif "Update" in update:
            idx_name = update["Update"]["IndexName"]
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
        desc["LatestStreamLabel"] = now_iso()
        desc["LatestStreamArn"] = f"{t['TableArn']}/stream/{now_iso()}"
    if t.get("SSEDescription"):
        desc["SSEDescription"] = t["SSEDescription"]
    desc["WarmThroughput"] = t.get("WarmThroughput", {
        "ReadUnitsPerSecond": 0,
        "WriteUnitsPerSecond": 0,
        "Status": "ACTIVE",
    })
    return desc


# ---------------------------------------------------------------------------
# Item operations
# ---------------------------------------------------------------------------

def _put_item(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)

    item = data.get("Item", {})
    pk_val = _extract_key_val(item.get(table["pk_name"]))
    sk_val = _extract_key_val(item.get(table["sk_name"])) if table["sk_name"] else "__no_sort__"
    old_item = table["items"].get(pk_val, {}).get(sk_val)

    cond_expr = data.get("ConditionExpression")
    if cond_expr:
        if not _evaluate_condition(cond_expr, old_item or {}, data.get("ExpressionAttributeValues", {}), data.get("ExpressionAttributeNames", {})):
            return error_response_json("ConditionalCheckFailedException", "The conditional request failed", 400)

    table["items"][pk_val][sk_val] = item
    _update_counts(table)

    event_name = "MODIFY" if old_item else "INSERT"
    _emit_stream_event(name, event_name, old_item, item)

    result = {}
    if data.get("ReturnValues") == "ALL_OLD" and old_item:
        result["Attributes"] = old_item
    _add_consumed_capacity(result, data, name, write=True)
    return json_response(result)


def _get_item(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)

    key = data.get("Key", {})
    pk_val, sk_val, key_err = _resolve_table_key_values(table, key, allow_extra=False)
    if key_err:
        return key_err
    item = table["items"].get(pk_val, {}).get(sk_val)

    result = {}
    if item:
        result["Item"] = _apply_projection(item, data)
    _add_consumed_capacity(result, data, name)
    return json_response(result)


def _delete_item(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)

    key = data.get("Key", {})
    pk_val, sk_val, key_err = _resolve_table_key_values(table, key, allow_extra=False)
    if key_err:
        return key_err
    old_item = table["items"].get(pk_val, {}).get(sk_val)

    cond_expr = data.get("ConditionExpression")
    if cond_expr:
        if not _evaluate_condition(cond_expr, old_item or {}, data.get("ExpressionAttributeValues", {}), data.get("ExpressionAttributeNames", {})):
            return error_response_json("ConditionalCheckFailedException", "The conditional request failed", 400)

    if old_item is not None:
        table["items"].get(pk_val, {}).pop(sk_val, None)
        _emit_stream_event(name, "REMOVE", old_item, None)
    _update_counts(table)

    result = {}
    if data.get("ReturnValues") == "ALL_OLD" and old_item:
        result["Attributes"] = old_item
    _add_consumed_capacity(result, data, name, write=True)
    return json_response(result)


def _update_item(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)

    key = data.get("Key", {})
    pk_val, sk_val, key_err = _resolve_table_key_values(table, key, allow_extra=False)
    if key_err:
        return key_err

    existing = table["items"].get(pk_val, {}).get(sk_val)
    old_item = copy.deepcopy(existing) if existing else None
    item = copy.deepcopy(existing) if existing else dict(key)

    cond_expr = data.get("ConditionExpression")
    if cond_expr:
        cond_target = existing or {}
        if not _evaluate_condition(cond_expr, cond_target, data.get("ExpressionAttributeValues", {}), data.get("ExpressionAttributeNames", {})):
            return error_response_json("ConditionalCheckFailedException", "The conditional request failed", 400)

    update_expr = data.get("UpdateExpression", "")
    eav = data.get("ExpressionAttributeValues", {})
    ean = data.get("ExpressionAttributeNames", {})

    if update_expr:
        item = _apply_update_expression(item, update_expr, eav, ean)

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
        result["Attributes"] = _diff_attributes(old_item or {}, item, return_old=False)
    _add_consumed_capacity(result, data, name, write=True)
    return json_response(result)


# ---------------------------------------------------------------------------
# Query / Scan
# ---------------------------------------------------------------------------

def _query(data):
    name = data.get("TableName")
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)

    eav = data.get("ExpressionAttributeValues", {})
    ean = data.get("ExpressionAttributeNames", {})
    key_cond = data.get("KeyConditionExpression", "")
    filter_expr = data.get("FilterExpression", "")
    limit = data.get("Limit")
    scan_forward = data.get("ScanIndexForward", True)
    esk = data.get("ExclusiveStartKey")
    index_name = data.get("IndexName")
    select = data.get("Select", "ALL_ATTRIBUTES")

    pk_name, sk_name, is_gsi = _resolve_index_keys(table, index_name)
    pk_val = _extract_pk_from_condition(key_cond, eav, ean, pk_name)
    if pk_val is None:
        return error_response_json("ValidationException", "Query condition missed key schema element", 400)

    if is_gsi or index_name:
        candidates = []
        for pk_bucket in table["items"].values():
            for it in pk_bucket.values():
                if pk_name in it and _extract_key_val(it[pk_name]) == pk_val:
                    candidates.append(it)
    else:
        candidates = list(table["items"].get(pk_val, {}).values())

    if sk_name:
        sk_type = _get_attr_type(table, sk_name)
        candidates.sort(key=lambda it: _sort_key_value(it.get(sk_name), sk_type), reverse=not scan_forward)

    if key_cond:
        candidates = [it for it in candidates if _evaluate_condition(key_cond, it, eav, ean)]

    if esk:
        candidates = _apply_exclusive_start_key(candidates, esk, pk_name, sk_name, scan_forward)

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
        result = {
            "Items": [_apply_projection(it, data) for it in filtered],
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
    table = _tables.get(name)
    if not table:
        return error_response_json("ResourceNotFoundException", f"Requested resource not found: Table: {name} not found", 400)

    filter_expr = data.get("FilterExpression", "")
    eav = data.get("ExpressionAttributeValues", {})
    ean = data.get("ExpressionAttributeNames", {})
    limit = data.get("Limit")
    esk = data.get("ExclusiveStartKey")
    index_name = data.get("IndexName")
    select = data.get("Select", "ALL_ATTRIBUTES")

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
        filtered = [it for it in all_items if _evaluate_condition(filter_expr, it, eav, ean)]
    else:
        filtered = all_items

    if select == "COUNT":
        result = {"Count": len(filtered), "ScannedCount": scanned_count}
    else:
        result = {
            "Items": [_apply_projection(it, data) for it in filtered],
            "Count": len(filtered),
            "ScannedCount": scanned_count,
        }

    if has_more and all_items:
        result["LastEvaluatedKey"] = _build_key(all_items[-1], table["pk_name"], table["sk_name"])

    _add_consumed_capacity(result, data, name)
    return json_response(result)


# ---------------------------------------------------------------------------
# Batch operations
# ---------------------------------------------------------------------------

def _batch_write_item(data):
    request_items = data.get("RequestItems", {})
    unprocessed = {}
    for table_name, requests in request_items.items():
        table = _tables.get(table_name)
        if not table:
            unprocessed[table_name] = requests
            continue
        for req in requests:
            if "PutRequest" in req:
                item = req["PutRequest"]["Item"]
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
    request_items = data.get("RequestItems", {})
    responses = {}
    unprocessed = {}
    for table_name, config in request_items.items():
        table = _tables.get(table_name)
        if not table:
            unprocessed[table_name] = config
            continue
        responses[table_name] = []
        proj = config.get("ProjectionExpression")
        config_ean = config.get("ExpressionAttributeNames", {})
        for key in config.get("Keys", []):
            pk_val, sk_val, key_err = _resolve_table_key_values(table, key, allow_extra=False)
            if key_err:
                return key_err
            item = table["items"].get(pk_val, {}).get(sk_val)
            if item:
                if proj:
                    item = _project_item(item, proj, config_ean)
                responses[table_name].append(item)
    return json_response({"Responses": responses, "UnprocessedKeys": unprocessed})


# ---------------------------------------------------------------------------
# Transaction operations
# ---------------------------------------------------------------------------

def _transact_write_items(data):
    items_list = data.get("TransactItems", [])

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
                return _transact_cancel_response(len(items_list), idx, "ConditionalCheckFailed")

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

    return json_response({})


def _transact_get_items(data):
    items_list = data.get("TransactItems", [])
    responses = []
    for transact in items_list:
        get_op = transact.get("Get", {})
        tbl = _tables.get(get_op.get("TableName", ""))
        if not tbl:
            responses.append({})
            continue
        item = _get_item_by_key(tbl, get_op.get("Key", {}))
        if item:
            proj = get_op.get("ProjectionExpression")
            ean = get_op.get("ExpressionAttributeNames", {})
            if proj:
                item = _project_item(item, proj, ean)
            responses.append({"Item": item})
        else:
            responses.append({})
    return json_response({"Responses": responses})


def _extract_transact_op(transact):
    for op_type in ("ConditionCheck", "Put", "Delete", "Update"):
        if op_type in transact:
            return op_type, transact[op_type]
    return None, None


def _transact_cancel_response(total, failed_idx, reason):
    reasons = []
    for i in range(total):
        if i == failed_idx:
            reasons.append({"Code": reason, "Message": "The conditional request failed"})
        else:
            reasons.append({"Code": "None"})
    data = {
        "__type": "TransactionCanceledException",
        "message": f"Transaction cancelled, please refer cancellation reasons for specific reasons [{', '.join(r['Code'] for r in reasons)}]",
        "CancellationReasons": reasons,
    }
    return json_response(data, 400)


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
    _ttl_settings[name] = {
        "TimeToLiveStatus": "ENABLED" if enabled else "DISABLED",
        "AttributeName": spec.get("AttributeName", ""),
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
    return json_response({
        "ContinuousBackupsDescription": {
            "ContinuousBackupsStatus": "ENABLED",
            "PointInTimeRecoveryDescription": {
                "PointInTimeRecoveryStatus": "ENABLED" if pitr_enabled else "DISABLED",
                "EarliestRestorableDateTime": 0,
                "LatestRestorableDateTime": 0,
            }
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
    return json_response({
        "Endpoints": [{"Address": "dynamodb.us-east-1.amazonaws.com", "CachePeriodInMinutes": 1440}]
    })


# ---------------------------------------------------------------------------
# Tag operations
# ---------------------------------------------------------------------------

def _tag_resource(data):
    arn = data.get("ResourceArn", "")
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
    keys = set(data.get("TagKeys", []))
    if arn in _tags:
        _tags[arn] = [t for t in _tags[arn] if t["Key"] not in keys]
    return json_response({})


def _list_tags(data):
    arn = data.get("ResourceArn", "")
    return json_response({"Tags": _tags.get(arn, [])})


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
            return str(attr["B"]).startswith(str(substr["B"]))
        return False

    def _fn_contains(self):
        self.advance();  self.expect('LPAREN')
        path = self._parse_path()
        self.expect('COMMA')
        val = self._operand()
        self.expect('RPAREN')
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


def _evaluate_condition(expr, item, attr_values, attr_names):
    if not expr or not expr.strip():
        return True
    try:
        tokens = _tokenize(expr)
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
    for assignment in _split_by_comma(tokens):
        eq_idx = None
        for i, tok in enumerate(assignment):
            if tok[0] == 'EQ':
                eq_idx = i
                break
        if eq_idx is None:
            continue
        path_parts = _parse_path_from_tokens(assignment[:eq_idx], attr_names)
        value = _eval_set_value(assignment[eq_idx + 1:], item, attr_values, attr_names)
        if path_parts and value is not None:
            _set_at_path(item, path_parts, value)


def _eval_set_value(tokens, item, attr_values, attr_names):
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
            return ("B", val["B"])
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
    if isinstance(val, dict):
        if "S" in val:  return len(val["S"])
        if "B" in val:  return len(val["B"])
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
        return "" if sk_type != "N" else Decimal(0)
    val = _extract_key_val(attr)
    if sk_type == "N":
        try:
            return Decimal(val)
        except (InvalidOperation, TypeError, ValueError):
            return Decimal(0)
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

def _apply_exclusive_start_key(candidates, esk, pk_name, sk_name, scan_forward=True):
    if not esk or not candidates:
        return candidates
    # Hash-only table: no sort key — find the item matching the PK and return everything after it
    if not sk_name or sk_name not in esk:
        start_pk = _extract_key_val(esk.get(pk_name, {}))
        found = False
        result = []
        for item in candidates:
            if found:
                result.append(item)
            elif _extract_key_val(item.get(pk_name, {})) == start_pk:
                found = True
        return result
    start_sk = esk[sk_name]
    result = []
    for item in candidates:
        item_sk = item.get(sk_name)
        if item_sk is None:
            continue
        if scan_forward:
            if _compare_ddb(item_sk, '>', start_sk):
                result.append(item)
        else:
            if _compare_ddb(item_sk, '<', start_sk):
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
    if not proj:
        return item
    return _project_item(item, proj, ean)


def _project_item(item, proj_expr, attr_names):
    attrs = [a.strip() for a in proj_expr.split(",")]
    result = {}
    for attr in attrs:
        first = attr.split(".")[0].split("[")[0]
        resolved = attr_names.get(first, first) if first.startswith("#") else first
        if resolved in item:
            result[resolved] = item[resolved]
    return result


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def _update_counts(table):
    count = sum(len(v) for v in table["items"].values())
    table["ItemCount"] = count
    table["TableSizeBytes"] = count * 200


def _evaluate_legacy_filter(item, scan_filter):
    """Evaluate legacy ScanFilter/QueryFilter conditions."""
    for attr_name, condition in scan_filter.items():
        op = condition.get("ComparisonOperator", "")
        attr_vals = condition.get("AttributeValueList", [])
        item_val = item.get(attr_name)
        if op == "EQ":
            if item_val is None or item_val != attr_vals[0]:
                return False
        elif op == "NE":
            if item_val is not None and item_val == attr_vals[0]:
                return False
        elif op == "NOT_NULL":
            if item_val is None:
                return False
        elif op == "NULL":
            if item_val is not None:
                return False
        elif op == "CONTAINS":
            val = _extract_key_val(item_val) if item_val else ""
            target = _extract_key_val(attr_vals[0]) if attr_vals else ""
            if target not in str(val):
                return False
        elif op == "BEGINS_WITH":
            val = _extract_key_val(item_val) if item_val else ""
            target = _extract_key_val(attr_vals[0]) if attr_vals else ""
            if not str(val).startswith(str(target)):
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
    result = {}
    all_keys = set(list(old_item.keys()) + list(new_item.keys()))
    for k in all_keys:
        ov = old_item.get(k)
        nv = new_item.get(k)
        if ov != nv:
            result[k] = ov if return_old and ov is not None else nv if nv is not None else {}
    return result


def reset():
    with _lock:
        _tables.clear()
        _tags.clear()
        _ttl_settings.clear()
        _pitr_settings.clear()
        _stream_records.clear()
