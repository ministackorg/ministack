"""
AWS DocumentDB Emulator.
JSON-based API via X-Amz-Target.
Supports (admin): collMod, create, createIndexes, currentOp, drop, dropDatabase, dropIndexes,
getAuditConfig, killCursors, killOp, listCollections, listDatabases, listIndexes, reIndex,
renameCollection, setAuditConfig.
Supports (agg/auth/diag/query/role): aggregate, count, distinct, authenticate, logout,
buildInfo, collStats, connectionStatus, dataSize, dbStats, explain, hostInfo, listCommands,
profiler, serverStatus, top, find, insert, update, delete, findAndModify, getMore, ReplaceOne,
createRole, dropRole, dropAllRolesFromDatabase, grantRolesToRole, revokeRolesFromRole,
revokePrivilegesFromRole, rolesInfo, updateRole.
Supports (sessions/user/shard + operators): startSession, abortTransaction, commitTransaction,
killSessions, killAllSessions, createUser, dropUser, dropAllUsersFromDatabase, grantRolesToUser,
revokeRolesFromUser, updateUser, usersInfo, enableSharding, shardCollection.
Array: $all, $elemMatch, $size. Bitwise: $bitsAllSet, $bitsAnySet, $bitsAllClear, $bitsAnyClear.
Comment: $comment.

Mongo APIs: https://docs.aws.amazon.com/documentdb/latest/developerguide/mongo-apis.html
https://docs.aws.amazon.com/documentdb/latest/developerguide/connect_programmatically.html
"""

import json
import logging

from ministack.core.responses import error_response_json, get_account_id, json_response, new_uuid

logger = logging.getLogger("documentdb")

_state: dict = {"users": {}, "collections": {}, "roles": {}, "sessions": {}, "sharded": {}}


def _get_state():
    """Return account-scoped state dicts (users, collections, roles, sessions, sharded)."""
    acct = get_account_id()
    if acct not in _state["users"]:
        _state["users"][acct] = []
        _state["collections"][acct] = {}
        _state["roles"][acct] = {}
        _state["sessions"][acct] = {}
        _state["sharded"][acct] = {}
    return (
        _state["users"][acct],
        _state["collections"][acct],
        _state["roles"][acct],
        _state["sessions"][acct],
        _state["sharded"][acct],
    )


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        # administrative
        "collmod": _coll_mod,
        "create": _create,
        "createindexes": _create_indexes,
        "currentop": _current_op,
        "drop": _drop,
        "dropdatabase": _drop_database,
        "dropindexes": _drop_indexes,
        "getauditconfig": _get_audit_config,
        "killcursors": _kill_cursors,
        "killop": _kill_op,
        "listcollections": _list_collections,
        "listdatabases": _list_databases,
        "listindexes": _list_indexes,
        "reindex": _re_index,
        "renamecollection": _rename_collection,
        "setauditconfig": _set_audit_config,
        # aggregation
        "aggregate": _aggregate,
        "count": _count,
        "distinct": _distinct,
        # authentication
        "authenticate": _authenticate,
        "logout": _logout,
        # diagnostic
        "buildinfo": _build_info,
        "collstats": _coll_stats,
        "connectionstatus": _connection_status,
        "datasize": _data_size,
        "dbstats": _db_stats,
        "explain": _explain,
        "hostinfo": _host_info,
        "listcommands": _list_commands,
        "profiler": _profiler,
        "serverstatus": _server_status,
        "top": _top,
        # query/write
        "find": _find,
        "insert": _insert,
        "update": _update,
        "delete": _delete,
        "findandmodify": _find_and_modify,
        "getmore": _get_more,
        "replaceone": _replace_one,
        # role management
        "createrole": _create_role,
        "droprole": _drop_role,
        "dropallrolesfromdatabase": _drop_all_roles_from_database,
        "grantrolestorole": _grant_roles_to_role,
        "revokerolesfromrole": _revoke_roles_from_role,
        "revokeprivilegesfromrole": _revoke_privileges_from_role,
        "rolesinfo": _roles_info,
        "updaterole": _update_role,
        # sessions
        "startsession": _start_session,
        "aborttransaction": _abort_transaction,
        "committransaction": _commit_transaction,
        "killsessions": _kill_sessions,
        "killallsessions": _kill_all_sessions,
        # user management
        "createuser": _create_user,
        "dropuser": _drop_user,
        "dropallusersfromdatabase": _drop_all_users_from_database,
        "grantrolestouser": _grant_roles_to_user,
        "revokerolesfromuser": _revoke_roles_from_user,
        "updateuser": _update_user,
        "usersinfo": _users_info,
        # sharding
        "enablesharding": _enable_sharding,
        "shardcollection": _shard_collection,
    }

    key = action.lower().replace("_", "")
    handler = handlers.get(key)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


def _find(data):
    db = data.get("db") or data.get("database")
    coll = data.get("find") or data.get("collection")
    filt = data.get("filter") or data.get("query") or {}
    if not db or not coll:
        # legacy fallback
        coll = data.get("collection", "default")
        return json_response({"result": "ok", "documents": [], "collection": coll})
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll) or {"documents": []}
    docs = [d for d in col.get("documents", []) if _doc_matches(d, filt)]
    return json_response({"ok": 1, "cursor": {"firstBatch": docs, "id": 0, "ns": f"{db}.{coll}"}})


def _create_user(data):
    username = (
        data.get("createUser")
        or data.get("user")
        or data.get("username")
        or data.get("UserName")
    )
    if not username:
        return error_response_json("InvalidParameter", "createUser/user required", 400)
    users, _, _, _, _ = _get_state()
    if any(
        (u.get("user") == username or u.get("username") == username) for u in users
    ):
        return error_response_json("UserAlreadyExists", f"User {username} exists", 400)
    uobj = {
        "user": username,
        "db": data.get("db") or data.get("database") or "admin",
        "roles": data.get("roles", []),
    }
    users.append(uobj)
    return json_response({"ok": 1})


# --- Administrative commands (from documentdb-apis.md) ---

def _coll_mod(data):
    db = data.get("db") or data.get("database")
    coll = data.get("collMod") or data.get("collection")
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and collMod required", 400)
    _, cols, _, _, _ = _get_state()
    db_colls = cols.setdefault(db, {})
    if coll not in db_colls:
        return error_response_json("NamespaceNotFound", f"Collection {db}.{coll} does not exist", 404)
    expire = data.get("expireAfterSeconds")
    if expire is not None:
        db_colls[coll]["expireAfterSeconds"] = expire
    return json_response({"ok": 1})


def _create(data):
    db = data.get("db") or data.get("database")
    coll = data.get("create") or data.get("collection")
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and create required", 400)
    _, cols, _, _, _ = _get_state()
    db_colls = cols.setdefault(db, {})
    if coll in db_colls:
        return error_response_json("NamespaceExists", f"Collection {db}.{coll} already exists", 400)
    db_colls[coll] = {"name": coll, "indexes": [], "options": data.get("options", {}), "documents": []}
    return json_response({"ok": 1})


def _create_indexes(data):
    db = data.get("db") or data.get("database")
    coll = data.get("createIndexes") or data.get("collection")
    indexes = data.get("indexes") or []
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and createIndexes required", 400)
    _, cols, _, _, _ = _get_state()
    db_colls = cols.setdefault(db, {})
    col = db_colls.setdefault(coll, {"name": coll, "indexes": [], "options": {}, "documents": []})
    for idx in indexes:
        name = idx.get("name") or "_".join(str(k) for k in idx.get("key", {}).keys())
        col["indexes"].append({"name": name, "key": idx.get("key", {}), "options": idx})
    return json_response({"ok": 1, "numIndexesBefore": len(col["indexes"]) - len(indexes), "numIndexesAfter": len(col["indexes"])})


def _current_op(data):
    return json_response({"inprog": []})


def _drop(data):
    db = data.get("db") or data.get("database")
    coll = data.get("drop") or data.get("collection")
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and drop required", 400)
    _, cols, _, _, _ = _get_state()
    db_colls = cols.get(db, {})
    if coll not in db_colls:
        return error_response_json("NamespaceNotFound", f"Collection {db}.{coll} does not exist", 404)
    del db_colls[coll]
    return json_response({"ok": 1})


def _drop_database(data):
    db = data.get("dropDatabase") or data.get("db")
    if not db:
        return error_response_json("InvalidParameter", "dropDatabase required", 400)
    _, cols, _, _, _ = _get_state()
    if db in cols:
        del cols[db]
    return json_response({"ok": 1, "dropped": db})


def _drop_indexes(data):
    db = data.get("db") or data.get("database")
    coll = data.get("dropIndexes") or data.get("collection")
    index = data.get("index") or data.get("indexName")
    if not db or not coll or not index:
        return error_response_json("InvalidParameter", "db, dropIndexes and index required", 400)
    _, cols, _, _, _ = _get_state()
    db_colls = cols.get(db, {})
    col = db_colls.get(coll)
    if not col:
        return error_response_json("NamespaceNotFound", f"Collection {db}.{coll} does not exist", 404)
    before = len(col["indexes"])
    col["indexes"] = [i for i in col["indexes"] if i.get("name") != index]
    return json_response({"ok": 1, "nIndexesWas": before, "nIndexes": len(col["indexes"])})


def _get_audit_config(data):
    return json_response({"auditConfig": {}})


def _kill_cursors(data):
    return json_response({"cursorsKilled": []})


def _kill_op(data):
    return json_response({"ok": 1})


def _list_collections(data):
    db = data.get("db") or data.get("database")
    if not db:
        return error_response_json("InvalidParameter", "db required", 400)
    _, cols, _, _, _ = _get_state()
    col_list = list(cols.get(db, {}).values())
    return json_response({"ok": 1, "cursor": {"firstBatch": [{"name": c["name"]} for c in col_list], "id": 0, "ns": f"{db}.$cmd.listCollections"}})


def _list_databases(data):
    _, cols, _, _, _ = _get_state()
    dbs = [{"name": name, "sizeOnDisk": 0, "empty": False} for name in cols.keys()]
    return json_response({"ok": 1, "databases": dbs, "totalSize": 0})


def _list_indexes(data):
    db = data.get("db") or data.get("database")
    coll = data.get("listIndexes") or data.get("collection")
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and listIndexes required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll)
    if not col:
        return error_response_json("NamespaceNotFound", f"Collection {db}.{coll} does not exist", 404)
    return json_response({"ok": 1, "cursor": {"firstBatch": col.get("indexes", []), "id": 0, "ns": f"{db}.{coll}"}})


def _re_index(data):
    db = data.get("db") or data.get("database")
    coll = data.get("reIndex") or data.get("collection")
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and reIndex required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll)
    if not col:
        return error_response_json("NamespaceNotFound", f"Collection {db}.{coll} does not exist", 404)
    return json_response({"ok": 1, "nIndexes": len(col.get("indexes", []))})


def _rename_collection(data):
    from_db = data.get("from")
    to_coll = data.get("to")
    if not from_db or not to_coll or "." not in from_db:
        return error_response_json("InvalidParameter", "from (db.coll) and to required", 400)
    db, coll = from_db.split(".", 1)
    _, cols, _, _, _ = _get_state()
    db_colls = cols.get(db, {})
    if coll not in db_colls:
        return error_response_json("NamespaceNotFound", f"Collection {db}.{coll} does not exist", 404)
    del db_colls[coll]
    return json_response({"ok": 1})


def _set_audit_config(data):
    return json_response({"ok": 1})


# --- helpers for query/agg over in-memory documents ---

def _match_value(doc_val, cond):
    if isinstance(cond, dict):
        for op, val in cond.items():
            if op == "$all":
                if not isinstance(doc_val, (list, tuple)):
                    return False
                for item in val or []:
                    if item not in doc_val:
                        return False
                continue
            if op == "$elemMatch":
                if not isinstance(doc_val, (list, tuple)):
                    return False
                matched = False
                em = val or {}
                for item in doc_val:
                    if isinstance(item, dict) and _doc_matches(item, em):
                        matched = True
                        break
                    # also allow scalar elemMatch? treat as equality if not dict
                    if not isinstance(em, dict) and item == em:
                        matched = True
                        break
                if not matched:
                    return False
                continue
            if op == "$size":
                if not isinstance(doc_val, (list, tuple)):
                    return False
                try:
                    if len(doc_val) != int(val):
                        return False
                except (TypeError, ValueError):
                    return False
                continue
            if op in ("$bitsAllSet", "$bitsAnySet", "$bitsAllClear", "$bitsAnyClear"):
                try:
                    dv = int(doc_val) if doc_val is not None else 0
                    mask = int(val)
                except (TypeError, ValueError):
                    return False
                if op == "$bitsAllSet":
                    if (dv & mask) != mask:
                        return False
                elif op == "$bitsAnySet":
                    if (dv & mask) == 0:
                        return False
                elif op == "$bitsAllClear":
                    if (dv & mask) != 0:
                        return False
                elif op == "$bitsAnyClear":
                    if (dv & mask) == mask:
                        return False
                continue
            # unknown operator in this context: fallthrough to direct compare below
        # if cond was operator-only dict, consider matched unless a check failed above
        return True
    # direct value equality (or regex etc not required here)
    return doc_val == cond


def _doc_matches(doc, filt):
    if not filt:
        return True
    for k, v in filt.items():
        if k.startswith("$"):
            if k == "$comment":
                continue
            if k == "$and":
                if not isinstance(v, list):
                    return False
                for sub in v:
                    if not _doc_matches(doc, sub):
                        return False
                continue
            if k == "$or":
                if not isinstance(v, list):
                    return False
                if not any(_doc_matches(doc, sub) for sub in v):
                    return False
                continue
            if k == "$nor":
                if not isinstance(v, list):
                    return False
                if any(_doc_matches(doc, sub) for sub in v):
                    return False
                continue
            if k == "$not":
                if not isinstance(v, dict):
                    return False
                if _doc_matches(doc, v):
                    return False
                continue
            # other top-level $ (e.g. $expr) ignored for matching in this emulator
            continue
        # field condition
        doc_val = doc.get(k)
        if not _match_value(doc_val, v):
            return False
    return True


def _run_pipeline(docs, pipeline):
    res = list(docs)
    for stage in pipeline or []:
        if "$match" in stage:
            m = stage["$match"]
            res = [d for d in res if _doc_matches(d, m)]
        elif "$project" in stage:
            p = stage["$project"]
            res = [{k: d.get(k) for k in p if p.get(k)} for d in res]
        elif "$limit" in stage:
            res = res[: int(stage["$limit"])]
        elif "$skip" in stage:
            res = res[int(stage["$skip"]):]
        elif "$sort" in stage:
            s = stage["$sort"]
            for k, direc in reversed(list(s.items())):
                res = sorted(res, key=lambda d: (d.get(k) if d.get(k) is not None else ""), reverse=(direc < 0))
    return res


# --- Aggregation ---

def _aggregate(data):
    db = data.get("db") or data.get("database")
    coll = data.get("aggregate") or data.get("collection")
    pipeline = data.get("pipeline", [])
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and aggregate required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll) or {"documents": []}
    docs = _run_pipeline(col.get("documents", []), pipeline)
    return json_response({"ok": 1, "cursor": {"firstBatch": docs, "id": 0, "ns": f"{db}.{coll}"}})


def _count(data):
    db = data.get("db") or data.get("database")
    coll = data.get("count") or data.get("collection")
    query = data.get("query", {})
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and count required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll) or {"documents": []}
    n = sum(1 for d in col.get("documents", []) if _doc_matches(d, query))
    return json_response({"ok": 1, "n": n})


def _distinct(data):
    db = data.get("db") or data.get("database")
    coll = data.get("distinct") or data.get("collection")
    key = data.get("key")
    if not db or not coll or not key:
        return error_response_json("InvalidParameter", "db, distinct and key required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll) or {"documents": []}
    vals = []
    seen = set()
    for d in col.get("documents", []):
        v = d.get(key)
        if v not in seen:
            seen.add(v)
            vals.append(v)
    return json_response({"ok": 1, "values": vals})


# --- Authentication ---

def _authenticate(data):
    return json_response({"ok": 1})


def _logout(data):
    return json_response({"ok": 1})


# --- Diagnostic commands ---

def _build_info(data):
    return json_response({"ok": 1, "version": "5.0.0", "versionArray": [5, 0, 0], "storageEngines": ["docdb"]})


def _coll_stats(data):
    db = data.get("db") or data.get("database")
    coll = data.get("collStats") or data.get("collection")
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and collStats required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll) or {"documents": [], "indexes": []}
    n = len(col.get("documents", []))
    return json_response({"ok": 1, "ns": f"{db}.{coll}", "count": n, "size": n * 128, "nindexes": len(col.get("indexes", []))})


def _connection_status(data):
    return json_response({"ok": 1, "authInfo": {"authenticatedUsers": [], "authenticatedUserRoles": []}})


def _data_size(data):
    db = data.get("db") or data.get("database")
    coll = data.get("dataSize") or data.get("collection")
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and dataSize required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll) or {"documents": []}
    n = len(col.get("documents", []))
    return json_response({"ok": 1, "size": n * 128, "numObjects": n})


def _db_stats(data):
    db = data.get("db") or data.get("database") or data.get("dbStats")
    _, cols, _, _, _ = _get_state()
    dcols = cols.get(db, {}) if db else {}
    total = 0
    for c in dcols.values():
        total += len(c.get("documents", []))
    return json_response({"ok": 1, "db": db or "admin", "collections": len(dcols), "objects": total, "dataSize": total * 128})


def _explain(data):
    return json_response({"ok": 1, "queryPlanner": {"plannerVersion": 1, "winningPlan": {"stage": "COLLSCAN"}}})


def _host_info(data):
    return json_response({"ok": 1, "system": {"currentTime": "2026-01-01T00:00:00.000Z"}, "os": {}, "extra": {}})


def _list_commands(data):
    return json_response({"ok": 1, "commands": {}})


def _profiler(data):
    return json_response({"ok": 1, "was": 0, "slowms": 100})


def _server_status(data):
    return json_response({"ok": 1, "version": "5.0.0", "process": "docdb", "connections": {"current": 1}})


def _top(data):
    return json_response({"ok": 1, "totals": {}})


# --- Query and write operations ---

def _insert(data):
    db = data.get("db") or data.get("database")
    coll = data.get("insert") or data.get("collection")
    docs = data.get("documents") or []
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and insert required", 400)
    _, cols, _, _, _ = _get_state()
    db_colls = cols.setdefault(db, {})
    col = db_colls.setdefault(coll, {"name": coll, "indexes": [], "options": {}, "documents": []})
    col["documents"].extend([dict(x) for x in docs])
    return json_response({"ok": 1, "n": len(docs)})


def _update(data):
    db = data.get("db") or data.get("database")
    coll = data.get("update") or data.get("collection")
    q = data.get("q") or data.get("filter") or {}
    u = data.get("u") or data.get("update") or {}
    multi = bool(data.get("multi"))
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and update required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll)
    if not col:
        return json_response({"ok": 1, "nModified": 0})
    n = 0
    for d in col.get("documents", []):
        if _doc_matches(d, q):
            d.update(u)
            n += 1
            if not multi:
                break
    return json_response({"ok": 1, "nModified": n})


def _delete(data):
    db = data.get("db") or data.get("database")
    coll = data.get("delete") or data.get("collection")
    q = data.get("q") or data.get("filter") or {}
    limit = int(data.get("limit") or 0)
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and delete required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll)
    if not col:
        return json_response({"ok": 1, "n": 0})
    kept = []
    removed = 0
    for d in col.get("documents", []):
        if _doc_matches(d, q) and (limit == 0 or removed < limit):
            removed += 1
            continue
        kept.append(d)
    col["documents"] = kept
    return json_response({"ok": 1, "n": removed})


def _find_and_modify(data):
    db = data.get("db") or data.get("database")
    coll = data.get("findAndModify") or data.get("collection")
    q = data.get("query") or data.get("filter") or {}
    u = data.get("update") or {}
    upsert = bool(data.get("upsert"))
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and findAndModify required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll)
    if not col:
        col = cols.setdefault(db, {}).setdefault(coll, {"name": coll, "indexes": [], "options": {}, "documents": []})
    for d in col.get("documents", []):
        if _doc_matches(d, q):
            old = dict(d)
            d.update(u)
            return json_response({"ok": 1, "value": old})
    if upsert:
        newd = dict(q)
        newd.update(u)
        col.setdefault("documents", []).append(newd)
        return json_response({"ok": 1, "value": None, "lastErrorObject": {"updatedExisting": False}})
    return json_response({"ok": 1, "value": None})


def _get_more(data):
    return json_response({"ok": 1, "cursor": {"id": 0, "nextBatch": []}})


def _replace_one(data):
    db = data.get("db") or data.get("database")
    coll = data.get("replaceOne") or data.get("collection") or data.get("replace")
    q = data.get("filter") or data.get("q") or {}
    repl = data.get("replacement") or data.get("u") or {}
    if not db or not coll:
        return error_response_json("InvalidParameter", "db and replaceOne required", 400)
    _, cols, _, _, _ = _get_state()
    col = cols.get(db, {}).get(coll)
    if not col:
        col = cols.setdefault(db, {}).setdefault(coll, {"name": coll, "indexes": [], "options": {}, "documents": []})
    for i, d in enumerate(col.get("documents", [])):
        if _doc_matches(d, q):
            col["documents"][i] = dict(repl)
            return json_response({"ok": 1, "nModified": 1})
    col.setdefault("documents", []).append(dict(repl))
    return json_response({"ok": 1, "nModified": 0, "nUpserted": 1})


# --- Role management commands ---

def _create_role(data):
    db = data.get("db") or data.get("database")
    role = data.get("createRole") or data.get("role")
    if not db or not role:
        return error_response_json("InvalidParameter", "db and createRole required", 400)
    _, _, roles, _, _ = _get_state()
    db_roles = roles.setdefault(db, {})
    if role in db_roles:
        return error_response_json("RoleAlreadyExists", f"Role {role} already exists", 400)
    db_roles[role] = {
        "role": role,
        "db": db,
        "privileges": data.get("privileges", []),
        "roles": data.get("roles", []),
    }
    return json_response({"ok": 1})


def _drop_role(data):
    db = data.get("db") or data.get("database")
    role = data.get("dropRole") or data.get("role")
    if not db or not role:
        return error_response_json("InvalidParameter", "db and dropRole required", 400)
    _, _, roles, _, _ = _get_state()
    db_roles = roles.get(db, {})
    if role not in db_roles:
        return error_response_json("RoleNotFound", f"Role {role} not found", 404)
    del db_roles[role]
    return json_response({"ok": 1})


def _drop_all_roles_from_database(data):
    db = data.get("dropAllRolesFromDatabase") or data.get("db")
    if not db:
        return error_response_json("InvalidParameter", "dropAllRolesFromDatabase required", 400)
    _, _, roles, _, _ = _get_state()
    roles.pop(db, None)
    return json_response({"ok": 1})


def _grant_roles_to_role(data):
    db = data.get("db") or data.get("database")
    role = data.get("grantRolesToRole") or data.get("role")
    to_grant = data.get("roles", [])
    if not db or not role:
        return error_response_json("InvalidParameter", "db and grantRolesToRole required", 400)
    _, _, roles, _, _ = _get_state()
    db_roles = roles.setdefault(db, {})
    if role not in db_roles:
        db_roles[role] = {"role": role, "db": db, "privileges": [], "roles": []}
    db_roles[role].setdefault("roles", []).extend(to_grant)
    return json_response({"ok": 1})


def _revoke_roles_from_role(data):
    db = data.get("db") or data.get("database")
    role = data.get("revokeRolesFromRole") or data.get("role")
    to_revoke = data.get("roles", [])
    if not db or not role:
        return error_response_json("InvalidParameter", "db and revokeRolesFromRole required", 400)
    _, _, roles, _, _ = _get_state()
    r = roles.get(db, {}).get(role)
    if r and "roles" in r:
        r["roles"] = [x for x in r.get("roles", []) if x not in to_revoke]
    return json_response({"ok": 1})


def _revoke_privileges_from_role(data):
    db = data.get("db") or data.get("database")
    role = data.get("revokePrivilegesFromRole") or data.get("role")
    if not db or not role:
        return error_response_json("InvalidParameter", "db and revokePrivilegesFromRole required", 400)
    _, _, roles, _, _ = _get_state()
    r = roles.get(db, {}).get(role)
    if r:
        r["privileges"] = []
    return json_response({"ok": 1})


def _roles_info(data):
    db = data.get("db") or data.get("database")
    role = data.get("role")
    _, _, roles, _, _ = _get_state()
    if db:
        if role:
            r = roles.get(db, {}).get(role)
            return json_response({"ok": 1, "roles": [r] if r else []})
        return json_response({"ok": 1, "roles": list(roles.get(db, {}).values())})
    allr = []
    for dbr in roles.values():
        allr.extend(dbr.values())
    return json_response({"ok": 1, "roles": allr})


def _update_role(data):
    db = data.get("db") or data.get("database")
    role = data.get("updateRole") or data.get("role")
    if not db or not role:
        return error_response_json("InvalidParameter", "db and updateRole required", 400)
    _, _, roles, _, _ = _get_state()
    db_roles = roles.setdefault(db, {})
    if role not in db_roles:
        db_roles[role] = {"role": role, "db": db, "privileges": [], "roles": []}
    if "privileges" in data:
        db_roles[role]["privileges"] = data["privileges"]
    if "roles" in data:
        db_roles[role]["roles"] = data["roles"]
    return json_response({"ok": 1})


# --- Sessions commands ---

def _start_session(data):
    _, _, _, sessions, _ = _get_state()
    sid = new_uuid()
    sessions[sid] = {"id": sid, "active": True}
    return json_response({"ok": 1, "id": {"id": sid, "uid": "0"}})


def _abort_transaction(data):
    return json_response({"ok": 1})


def _commit_transaction(data):
    return json_response({"ok": 1})


def _kill_sessions(data):
    _, _, _, sessions, _ = _get_state()
    ids = data.get("killSessions") or data.get("sessions") or []
    for sid in ids:
        sessions.pop(sid, None)
    return json_response({"ok": 1})


def _kill_all_sessions(data):
    _, _, _, sessions, _ = _get_state()
    sessions.clear()
    return json_response({"ok": 1})


# --- User management commands ---

def _drop_user(data):
    username = data.get("dropUser") or data.get("user")
    if not username:
        return error_response_json("InvalidParameter", "dropUser/user required", 400)
    users, _, _, _, _ = _get_state()
    before = len(users)
    users[:] = [u for u in users if not (u.get("user") == username or u.get("username") == username)]
    if len(users) == before:
        return error_response_json("UserNotFound", f"User {username} not found", 404)
    return json_response({"ok": 1})


def _drop_all_users_from_database(data):
    db = data.get("dropAllUsersFromDatabase") or data.get("db")
    if not db:
        return error_response_json("InvalidParameter", "dropAllUsersFromDatabase required", 400)
    users, _, _, _, _ = _get_state()
    users[:] = [u for u in users if u.get("db") != db]
    return json_response({"ok": 1})


def _grant_roles_to_user(data):
    username = data.get("grantRolesToUser") or data.get("user")
    to_grant = data.get("roles", [])
    if not username:
        return error_response_json("InvalidParameter", "grantRolesToUser/user required", 400)
    users, _, _, _, _ = _get_state()
    for u in users:
        if u.get("user") == username or u.get("username") == username:
            u.setdefault("roles", []).extend(to_grant)
            return json_response({"ok": 1})
    return error_response_json("UserNotFound", f"User {username} not found", 404)


def _revoke_roles_from_user(data):
    username = data.get("revokeRolesFromUser") or data.get("user")
    to_revoke = data.get("roles", [])
    if not username:
        return error_response_json("InvalidParameter", "revokeRolesFromUser/user required", 400)
    users, _, _, _, _ = _get_state()
    for u in users:
        if u.get("user") == username or u.get("username") == username:
            if "roles" in u:
                u["roles"] = [r for r in u.get("roles", []) if r not in to_revoke]
            return json_response({"ok": 1})
    return error_response_json("UserNotFound", f"User {username} not found", 404)


def _update_user(data):
    username = data.get("updateUser") or data.get("user")
    if not username:
        return error_response_json("InvalidParameter", "updateUser/user required", 400)
    users, _, _, _, _ = _get_state()
    for u in users:
        if u.get("user") == username or u.get("username") == username:
            if "roles" in data:
                u["roles"] = data["roles"]
            if "pwd" in data or "password" in data:
                u["password"] = data.get("pwd") or data.get("password")
            return json_response({"ok": 1})
    return error_response_json("UserNotFound", f"User {username} not found", 404)


def _users_info(data):
    username = data.get("usersInfo") or data.get("user")
    users, _, _, _, _ = _get_state()
    if username:
        matches = [u for u in users if (u.get("user") == username or u.get("username") == username)]
        return json_response({"ok": 1, "users": matches})
    return json_response({"ok": 1, "users": list(users)})


# --- Sharding commands ---

def _enable_sharding(data):
    db = data.get("enableSharding") or data.get("db") or data.get("database")
    if not db:
        return error_response_json("InvalidParameter", "enableSharding/db required", 400)
    _, _, _, _, sharded = _get_state()
    sharded[db] = {"sharded": True, "collections": {}}
    return json_response({"ok": 1})


def _shard_collection(data):
    coll = data.get("shardCollection") or data.get("collection")
    key = data.get("key")
    if not coll:
        return error_response_json("InvalidParameter", "shardCollection required", 400)
    _, _, _, _, sharded = _get_state()
    if "." in coll:
        db, cname = coll.split(".", 1)
    else:
        db = "admin"
        cname = coll
    db_shard = sharded.setdefault(db, {"sharded": True, "collections": {}})
    db_shard.setdefault("collections", {})[cname] = {"key": key or {}, "sharded": True}
    return json_response({"ok": 1})


def reset():
    _state["users"].clear()
    _state["collections"].clear()
    _state["roles"].clear()
    _state["sessions"].clear()
    _state["sharded"].clear()
