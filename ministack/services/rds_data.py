"""
RDS Data API Service Emulator.
REST-style JSON API (POST /Execute, /BeginTransaction, etc.)
Routes SQL to real database containers managed by the RDS service emulator.
"""

import json
import logging
import os
import uuid

from ministack.core.responses import error_response_json, json_response

logger = logging.getLogger("rds-data")

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# Active transactions: txn_id -> {conn, engine, resourceArn, database}
_transactions: dict = {}


def _error(code, message, status=400):
    return error_response_json(code, message, status)


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


def _get_secret_password(secret_arn):
    """Extract password from a Secrets Manager secret."""
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
                            return parsed.get("password", secret_string)
                        except (json.JSONDecodeError, TypeError):
                            return secret_string
            # Fallback to any version
            for _vid, ver in secret.get("Versions", {}).items():
                secret_string = ver.get("SecretString")
                if secret_string:
                    try:
                        parsed = json.loads(secret_string)
                        return parsed.get("password", secret_string)
                    except (json.JSONDecodeError, TypeError):
                        return secret_string
    return None


def _connect(instance, engine, database=None, password=None):
    """Create a database connection to the real container."""
    host = instance.get("Endpoint", {}).get("Address", "localhost")
    port = instance.get("Endpoint", {}).get("Port", 5432)
    user = instance.get("MasterUsername", "admin")
    db = database or instance.get("DBName", "")
    pw = password or "password"

    if "mysql" in engine or "aurora-mysql" in engine or "mariadb" in engine:
        try:
            import pymysql
        except ImportError:
            raise ImportError(
                "pymysql is required for MySQL/Aurora MySQL rds-data support. "
                "Install with: pip install pymysql"
            )
        return pymysql.connect(
            host=host, port=int(port), user=user,
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
        return psycopg2.connect(
            host=host, port=int(port), user=user,
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

    # Check if instance has a real endpoint (Docker container running)
    endpoint = instance.get("Endpoint", {})
    if not endpoint.get("Port"):
        return _error("BadRequestException",
                       "No database endpoint available. "
                       "RDS Data API requires Docker-backed RDS instances.")

    password = _get_secret_password(secret_arn)

    try:
        if txn_id and txn_id in _transactions:
            conn = _transactions[txn_id]["conn"]
            cursor = conn.cursor()
        else:
            conn = _connect(instance, engine, database, password)
            cursor = conn.cursor()

        cursor.execute(sql)

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
        if not txn_id or txn_id not in _transactions:
            conn.close()

        return json_response(response)

    except ImportError as e:
        return _error("BadRequestException", str(e))
    except Exception as e:
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

    password = _get_secret_password(secret_arn)

    try:
        conn = _connect(instance, engine, database, password)
        if "mysql" in engine or "aurora-mysql" in engine:
            conn.autocommit(False)
        else:
            conn.autocommit = False
    except ImportError as e:
        return _error("BadRequestException", str(e))
    except Exception as e:
        return _error("BadRequestException", f"Database connection error: {e}")

    txn_id = str(uuid.uuid4())
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

    password = _get_secret_password(secret_arn)

    try:
        if txn_id and txn_id in _transactions:
            conn = _transactions[txn_id]["conn"]
        else:
            conn = _connect(instance, engine, database, password)

        cursor = conn.cursor()
        update_results = []

        if not parameter_sets:
            cursor.execute(sql)
            update_results.append({"generatedFields": []})
        else:
            for _params in parameter_sets:
                cursor.execute(sql)
                update_results.append({"generatedFields": []})

        cursor.close()
        if not txn_id or txn_id not in _transactions:
            conn.close()

        return json_response({"updateResults": update_results})

    except ImportError as e:
        return _error("BadRequestException", str(e))
    except Exception as e:
        return _error("BadRequestException", f"Database error: {e}")


def reset():
    for txn in _transactions.values():
        try:
            txn["conn"].close()
        except Exception:
            pass
    _transactions.clear()
