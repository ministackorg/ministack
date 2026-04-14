"""
RDS Data API Service Emulator.
REST-style JSON API (POST /Execute, /BeginTransaction, etc.)
Routes SQL to real database containers managed by the RDS service emulator.
"""

import json
import logging
import os
import re
import threading
import uuid

from ministack.core.responses import AccountScopedDict, get_account_id, error_response_json, json_response

logger = logging.getLogger("rds-data")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# Active transactions: txn_id -> {conn, engine, resourceArn, database}
_transactions: dict = {}
_lock = threading.Lock()

# In-memory tracking for stub mode: remember databases/users created via SQL.
# Keyed by cluster identifier.
_stub_databases = AccountScopedDict()   # cluster_id -> set of database names
_stub_users = AccountScopedDict()       # cluster_id -> set of usernames
_stub_grants = AccountScopedDict()      # cluster_id -> {username -> list of grant strings}


def _error(code, message, status=400):
    return error_response_json(code, message, status)


def _stub_success():
    """Return a minimal successful ExecuteStatement response for mock environments."""
    return json_response({
        "numberOfRecordsUpdated": 0,
        "generatedFields": [],
        "records": [],
    })


def _cluster_id_from_arn(resource_arn):
    """Extract cluster identifier from an ARN."""
    parts = resource_arn.split(":")
    if len(parts) >= 7:
        return parts[6]
    return resource_arn


_CREATE_DB_RE = re.compile(
    r"CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?", re.IGNORECASE)
_CREATE_USER_RE = re.compile(
    r"CREATE\s+USER\s+(?:IF\s+NOT\s+EXISTS\s+)?'([^']+)'", re.IGNORECASE)
_DROP_USER_RE = re.compile(
    r"DROP\s+USER\s+(?:IF\s+EXISTS\s+)?'([^']+)'", re.IGNORECASE)
_DROP_DB_RE = re.compile(
    r"DROP\s+DATABASE\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?", re.IGNORECASE)
_GRANT_RE = re.compile(
    r"(GRANT\s+.+?\s+TO\s+'([^']+)'.*)", re.IGNORECASE | re.DOTALL)
_REVOKE_RE = re.compile(
    r"REVOKE\s+.+?\s+FROM\s+'([^']+)'", re.IGNORECASE | re.DOTALL)
_SHOW_DATABASES_RE = re.compile(
    r"SHOW\s+DATABASES", re.IGNORECASE)
_SELECT_SCHEMATA_RE = re.compile(
    r"SELECT\s+schema_name\s+FROM\s+information_schema\.schemata", re.IGNORECASE)
_SELECT_USER_RE = re.compile(
    r"SELECT\s+.*FROM\s+mysql\.user\s+WHERE\s+User\s*=\s*'([^']+)'", re.IGNORECASE)
_SHOW_GRANTS_RE = re.compile(
    r"SHOW\s+GRANTS\s+FOR\s+'([^']+)'", re.IGNORECASE)


def _stub_execute(resource_arn, sql):
    """Handle SQL in stub mode: track creates, respond to queries."""
    cid = _cluster_id_from_arn(resource_arn)

    # Track CREATE DATABASE
    m = _CREATE_DB_RE.search(sql)
    if m:
        _stub_databases.setdefault(cid, set()).add(m.group(1))
        logger.info("Stub: tracked CREATE DATABASE %s on %s", m.group(1), cid)
        return _stub_success()

    # Track CREATE USER
    m = _CREATE_USER_RE.search(sql)
    if m:
        _stub_users.setdefault(cid, set()).add(m.group(1))
        logger.info("Stub: tracked CREATE USER %s on %s", m.group(1), cid)
        return _stub_success()

    # Track DROP USER
    m = _DROP_USER_RE.search(sql)
    if m:
        _stub_users.get(cid, set()).discard(m.group(1))
        _stub_grants.get(cid, {}).pop(m.group(1), None)
        logger.info("Stub: tracked DROP USER %s on %s", m.group(1), cid)
        return _stub_success()

    # Track DROP DATABASE
    m = _DROP_DB_RE.search(sql)
    if m:
        _stub_databases.get(cid, set()).discard(m.group(1))
        logger.info("Stub: tracked DROP DATABASE %s on %s", m.group(1), cid)
        return _stub_success()

    # Track GRANT
    m = _GRANT_RE.search(sql)
    if m:
        grant_str, username = m.group(1).strip(), m.group(2)
        _stub_grants.setdefault(cid, {}).setdefault(username, []).append(grant_str)
        logger.info("Stub: tracked GRANT for %s on %s", username, cid)
        return _stub_success()

    # Track REVOKE
    m = _REVOKE_RE.search(sql)
    if m:
        username = m.group(1)
        _stub_grants.get(cid, {}).pop(username, None)
        logger.info("Stub: tracked REVOKE for %s on %s", username, cid)
        return _stub_success()

    # Respond to SHOW DATABASES
    if _SHOW_DATABASES_RE.search(sql):
        dbs = _stub_databases.get(cid, set())
        # Always include system databases
        all_dbs = {"information_schema", "mysql", "performance_schema", "sys"} | dbs
        records = [[{"stringValue": db}] for db in sorted(all_dbs)]
        return json_response({
            "numberOfRecordsUpdated": 0,
            "generatedFields": [],
            "records": records,
        })

    # Respond to SELECT schema_name FROM information_schema.schemata ...
    if _SELECT_SCHEMATA_RE.search(sql):
        dbs = _stub_databases.get(cid, set())
        all_dbs = {"information_schema", "mysql", "performance_schema", "sys"} | dbs
        # Filter by WHERE clause if present
        in_match = re.search(r"WHERE\s+schema_name\s+IN\s*\(([^)]+)\)", sql, re.IGNORECASE)
        eq_match = re.search(r"WHERE\s+schema_name\s*=\s*'([^']+)'", sql, re.IGNORECASE)
        if in_match:
            requested = {s.strip().strip("'\"") for s in in_match.group(1).split(",")}
            matching = all_dbs & requested
        elif eq_match:
            name = eq_match.group(1)
            matching = {name} if name in all_dbs else set()
        else:
            matching = all_dbs
        records = [[{"stringValue": db}] for db in sorted(matching)]
        return json_response({
            "numberOfRecordsUpdated": 0,
            "generatedFields": [],
            "records": records,
        })

    # Respond to SELECT ... FROM mysql.user WHERE User = '...'
    m = _SELECT_USER_RE.search(sql)
    if m:
        username = m.group(1)
        users = _stub_users.get(cid, set())
        if username in users:
            # Check if it's asking for a specific column (privilege check)
            col_match = re.match(r"SELECT\s+(\w+)\s+FROM", sql, re.IGNORECASE)
            if col_match and col_match.group(1).lower() != "user":
                # Privilege column query — return "Y" for any privilege
                return json_response({
                    "numberOfRecordsUpdated": 0,
                    "generatedFields": [],
                    "records": [[{"stringValue": "Y"}]],
                })
            return json_response({
                "numberOfRecordsUpdated": 0,
                "generatedFields": [],
                "records": [[{"stringValue": username}]],
            })
        return _stub_success()

    # Respond to SHOW GRANTS FOR '...'
    m = _SHOW_GRANTS_RE.search(sql)
    if m:
        username = m.group(1)
        grants = _stub_grants.get(cid, {}).get(username, [])
        records = [[{"stringValue": g}] for g in grants]
        return json_response({
            "numberOfRecordsUpdated": 0,
            "generatedFields": [],
            "records": records,
        })

    # Default stub
    return _stub_success()


def _resolve_cluster(resource_arn):
    """Find RDS cluster and a member instance from a resourceArn."""
    from ministack.services import rds

    # Parse ARN: arn:aws:rds:REGION:ACCOUNT:cluster:IDENTIFIER
    parts = resource_arn.split(":")
    if len(parts) >= 7 and parts[5] == "cluster":
        cluster_id = parts[6]
    elif len(parts) >= 7 and parts[5] == "db":
        # Instance ARN: arn:aws:rds:REGION:ACCOUNT:db:IDENTIFIER
        instance_id = parts[6]
        instance = rds._instances.get(instance_id)
        if instance:
            return instance, instance.get("Engine", "postgres")
        return None, None
    else:
        return None, None

    cluster = rds._clusters.get(cluster_id)
    if not cluster:
        return None, None

    engine = cluster.get("Engine", "postgres")

    # Find an instance belonging to this cluster
    for inst in rds._instances.values():
        if inst.get("DBClusterIdentifier") == cluster_id:
            return inst, engine

    # No instance found — return cluster info but no connectable instance
    return cluster, engine


def _get_secret_credentials(secret_arn):
    """Extract username and password from a Secrets Manager secret.

    Returns (username, password) where username may be None if the secret
    doesn't contain one.
    """
    from ministack.services import secretsmanager

    for _name, secret in secretsmanager._secrets.items():
        if secret.get("ARN") == secret_arn or _name == secret_arn:
            # Find the AWSCURRENT version
            for _vid, ver in secret.get("Versions", {}).items():
                if "AWSCURRENT" in ver.get("Stages", []):
                    secret_string = ver.get("SecretString")
                    if secret_string:
                        try:
                            parsed = json.loads(secret_string)
                            return (parsed.get("username"),
                                    parsed.get("password", secret_string))
                        except (json.JSONDecodeError, TypeError):
                            return None, secret_string
            # Fallback to any version
            for _vid, ver in secret.get("Versions", {}).items():
                secret_string = ver.get("SecretString")
                if secret_string:
                    try:
                        parsed = json.loads(secret_string)
                        return (parsed.get("username"),
                                parsed.get("password", secret_string))
                    except (json.JSONDecodeError, TypeError):
                        return None, secret_string
    return None, None


def _connect(instance, engine, database=None, password=None,
             username=None):
    """Create a database connection to the real container."""
    # Prefer the internal (Docker-network) address when available so the
    # Data API can reach sibling containers.  Fall back to the public
    # endpoint for host-mode or non-Docker setups.
    host = (instance.get("_internal_address")
            or instance.get("Endpoint", {}).get("Address", "localhost"))
    port = (instance.get("_internal_port")
            or instance.get("Endpoint", {}).get("Port", 5432))
    db = database or ""
    pw = password or "password"

    if "mysql" in engine or "aurora-mysql" in engine or "mariadb" in engine:
        try:
            import pymysql
        except ImportError:
            raise ImportError(
                "pymysql is required for MySQL/Aurora MySQL rds-data support. "
                "Install with: pip install pymysql"
            )
        # In RDS the master user has full privileges.  In a Docker MySQL
        # container the equivalent is 'root'.  When the secret identifies
        # the master user (username matches MasterUsername or is absent),
        # connect as root.  Otherwise use the actual username from the
        # secret so user-level operations (e.g. ALTER USER … REPLACE)
        # authenticate correctly.
        master = instance.get("MasterUsername", "admin")
        connect_user = username or "root"
        if username and username == master:
            connect_user = "root"
        return pymysql.connect(
            host=host, port=int(port), user=connect_user,
            password=pw, database=db or None, autocommit=True,
        )
    else:
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required for PostgreSQL/Aurora PostgreSQL rds-data support. "
                "Install with: pip install psycopg2-binary"
            )
        pg_user = username or instance.get("MasterUsername", "admin")
        return psycopg2.connect(
            host=host, port=int(port), user=pg_user,
            password=pw, dbname=db or "postgres",
        )


def _field_value(val, type_name=None):
    """Convert a Python value to an RDS Data API Field object."""
    if val is None:
        return {"isNull": True}
    if isinstance(val, bool):
        return {"booleanValue": val}
    if isinstance(val, int):
        return {"longValue": val}
    if isinstance(val, float):
        return {"doubleValue": val}
    if isinstance(val, bytes):
        import base64
        return {"blobValue": base64.b64encode(val).decode()}
    return {"stringValue": str(val)}


def _column_metadata(description, engine):
    """Convert DB-API cursor.description to RDS Data API columnMetadata."""
    if not description:
        return []
    metadata = []
    for col in description:
        name = col[0]
        type_code = col[1]
        metadata.append({
            "arrayBaseColumnType": 0,
            "isAutoIncrement": False,
            "isCaseSensitive": True,
            "isCurrency": False,
            "isSigned": True,
            "label": name,
            "name": name,
            "nullable": 1,
            "precision": col[4] if col[4] else 0,
            "scale": col[5] if col[5] else 0,
            "schemaName": "",
            "tableName": "",
            "type": type_code if isinstance(type_code, int) else 12,
            "typeName": "VARCHAR",
        })
    return metadata


def _convert_parameters(parameters):
    """Convert RDS Data API parameters to DB-API named params dict."""
    if not parameters:
        return {}
    result = {}
    for param in parameters:
        name = param.get("name")
        if not name:
            continue
        value = param.get("value", {})
        if "isNull" in value and value["isNull"]:
            result[name] = None
        elif "stringValue" in value:
            result[name] = value["stringValue"]
        elif "longValue" in value:
            result[name] = value["longValue"]
        elif "doubleValue" in value:
            result[name] = value["doubleValue"]
        elif "booleanValue" in value:
            result[name] = value["booleanValue"]
        elif "blobValue" in value:
            import base64
            result[name] = base64.b64decode(value["blobValue"])
        else:
            result[name] = None
    return result


async def handle_request(method, path, headers, body, query_params):
    """Route RDS Data API requests by path."""
    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, TypeError):
        return _error("BadRequestException", "Invalid JSON in request body")

    handlers = {
        "/Execute": _execute_statement,
        "/BeginTransaction": _begin_transaction,
        "/CommitTransaction": _commit_transaction,
        "/RollbackTransaction": _rollback_transaction,
        "/BatchExecute": _batch_execute_statement,
    }

    handler = handlers.get(path)
    if not handler:
        return _error("BadRequestException", f"Unknown RDS Data API path: {path}")
    return handler(data)


def _execute_statement(data):
    resource_arn = data.get("resourceArn")
    secret_arn = data.get("secretArn")
    sql = data.get("sql")
    database = data.get("database")
    txn_id = data.get("transactionId")
    parameters = data.get("parameters", [])
    include_metadata = data.get("includeResultMetadata", False)

    if not resource_arn:
        return _error("BadRequestException", "resourceArn is required")
    if not secret_arn:
        return _error("BadRequestException", "secretArn is required")
    if not sql:
        return _error("BadRequestException", "sql is required")

    instance, engine = _resolve_cluster(resource_arn)
    if not instance:
        return _error("BadRequestException",
                       f"Database cluster not found for ARN: {resource_arn}")

    # Check if instance has a real endpoint (Docker container running).
    # Clusters have Endpoint as a string (hostname); instances have it as a dict.
    endpoint = instance.get("Endpoint", {})
    if isinstance(endpoint, str) or not endpoint.get("Port"):
        logger.info("No endpoint for %s, using stub mode", resource_arn)
        return _stub_execute(resource_arn, sql)

    secret_user, password = _get_secret_credentials(secret_arn)

    # Convert :name placeholders to %(name)s for DB-API
    params = _convert_parameters(parameters)
    exec_sql = sql
    if params:
        for name in params:
            exec_sql = exec_sql.replace(f":{name}", f"%({name})s")

    own_conn = False
    conn = None
    try:
        with _lock:
            if txn_id and txn_id in _transactions:
                conn = _transactions[txn_id]["conn"]
            else:
                conn = _connect(instance, engine, database, password,
                                username=secret_user)
                own_conn = True

        cursor = conn.cursor()
        cursor.execute(exec_sql, params or None)

        response = {
            "numberOfRecordsUpdated": cursor.rowcount if cursor.rowcount >= 0 else 0,
            "generatedFields": [],
        }

        if cursor.description:
            rows = cursor.fetchall()
            records = []
            for row in rows:
                record = [_field_value(val) for val in row]
                records.append(record)
            response["records"] = records

            if include_metadata:
                response["columnMetadata"] = _column_metadata(
                    cursor.description, engine)
        else:
            response["records"] = []

        cursor.close()
        if own_conn:
            conn.close()

        return json_response(response)

    except ImportError as e:
        if own_conn and conn:
            conn.close()
        if not getattr(_execute_statement, "_import_warned", False):
            logger.warning("DB driver not available, using stub: %s", e)
            _execute_statement._import_warned = True
        return _stub_execute(resource_arn, sql)
    except Exception as e:
        if own_conn and conn:
            conn.close()
        # Connection errors (e.g. when RDS containers are not reachable from
        # within the MiniStack container) should fall back to stubs rather than
        # failing the caller.  This covers LAMBDA_EXECUTOR=local where the
        # MySQL sidecar container is not network-accessible.
        err_str = str(e)
        if "Can't connect" in err_str or "Connection refused" in err_str:
            logger.warning("DB connection failed, using stub: %s", e)
            return _stub_execute(resource_arn, sql)
        return _error("BadRequestException", f"Database error: {e}")


def _begin_transaction(data):
    resource_arn = data.get("resourceArn")
    secret_arn = data.get("secretArn")
    database = data.get("database")

    if not resource_arn:
        return _error("BadRequestException", "resourceArn is required")
    if not secret_arn:
        return _error("BadRequestException", "secretArn is required")

    instance, engine = _resolve_cluster(resource_arn)
    if not instance:
        return _error("BadRequestException",
                       f"Database cluster not found for ARN: {resource_arn}")

    secret_user, password = _get_secret_credentials(secret_arn)

    try:
        conn = _connect(instance, engine, database, password,
                        username=secret_user)
        if "mysql" in engine or "aurora-mysql" in engine:
            conn.autocommit(False)
        else:
            conn.autocommit = False
    except ImportError as e:
        return _error("BadRequestException", str(e))
    except Exception as e:
        return _error("BadRequestException", f"Database connection error: {e}")

    txn_id = str(uuid.uuid4())
    with _lock:
        _transactions[txn_id] = {
            "conn": conn,
            "engine": engine,
            "resourceArn": resource_arn,
            "database": database,
        }

    return json_response({"transactionId": txn_id})


def _commit_transaction(data):
    txn_id = data.get("transactionId")
    if not txn_id:
        return _error("BadRequestException", "transactionId is required")

    with _lock:
        txn = _transactions.pop(txn_id, None)
    if not txn:
        return _error("NotFoundException",
                       f"Transaction {txn_id} not found", 404)

    try:
        txn["conn"].commit()
        txn["conn"].close()
    except Exception as e:
        return _error("BadRequestException", f"Commit failed: {e}")

    return json_response({"transactionStatus": "Transaction Committed"})


def _rollback_transaction(data):
    txn_id = data.get("transactionId")
    if not txn_id:
        return _error("BadRequestException", "transactionId is required")

    with _lock:
        txn = _transactions.pop(txn_id, None)
    if not txn:
        return _error("NotFoundException",
                       f"Transaction {txn_id} not found", 404)

    try:
        txn["conn"].rollback()
        txn["conn"].close()
    except Exception as e:
        return _error("BadRequestException", f"Rollback failed: {e}")

    return json_response({"transactionStatus": "Transaction Rolled Back"})


def _batch_execute_statement(data):
    resource_arn = data.get("resourceArn")
    secret_arn = data.get("secretArn")
    sql = data.get("sql")
    parameter_sets = data.get("parameterSets", [])
    database = data.get("database")
    txn_id = data.get("transactionId")

    if not resource_arn:
        return _error("BadRequestException", "resourceArn is required")
    if not secret_arn:
        return _error("BadRequestException", "secretArn is required")
    if not sql:
        return _error("BadRequestException", "sql is required")

    instance, engine = _resolve_cluster(resource_arn)
    if not instance:
        return _error("BadRequestException",
                       f"Database cluster not found for ARN: {resource_arn}")

    secret_user, password = _get_secret_credentials(secret_arn)

    own_conn = False
    conn = None
    try:
        with _lock:
            if txn_id and txn_id in _transactions:
                conn = _transactions[txn_id]["conn"]
            else:
                conn = _connect(instance, engine, database, password,
                                username=secret_user)
                own_conn = True

        cursor = conn.cursor()
        update_results = []

        if not parameter_sets:
            cursor.execute(sql)
            update_results.append({"generatedFields": []})
        else:
            # Convert :name placeholders to %(name)s for DB-API
            exec_sql = sql
            if parameter_sets:
                sample = _convert_parameters(parameter_sets[0])
                for name in sample:
                    exec_sql = exec_sql.replace(f":{name}", f"%({name})s")

            for param_set in parameter_sets:
                params = _convert_parameters(param_set)
                cursor.execute(exec_sql, params or None)
                update_results.append({"generatedFields": []})

        cursor.close()
        if own_conn:
            conn.close()

        return json_response({"updateResults": update_results})

    except ImportError as e:
        if own_conn and conn:
            conn.close()
        return _error("BadRequestException", str(e))
    except Exception as e:
        if own_conn and conn:
            conn.close()
        return _error("BadRequestException", f"Database error: {e}")


def reset():
    with _lock:
        for txn in _transactions.values():
            try:
                txn["conn"].close()
            except Exception:
                pass
        _transactions.clear()
    _stub_databases.clear()
    _stub_users.clear()
    _stub_grants.clear()
