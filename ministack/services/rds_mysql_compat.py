"""Aurora MySQL compatibility objects installed on MiniStack compute."""

import logging

logger = logging.getLogger("rds")

_PREDEFINED_ROLES = (
    "AWS_SELECT_S3_ACCESS",
    "AWS_LOAD_S3_ACCESS",
)

_CONFIG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS mysql.ministack_rds_configuration (
  name VARCHAR(128) NOT NULL PRIMARY KEY,
  value VARCHAR(128) NOT NULL,
  description VARCHAR(255) NOT NULL
)
"""

_CONFIG_DEFAULT_SQL = """
INSERT IGNORE INTO mysql.ministack_rds_configuration
  (name, value, description)
VALUES
  ('binlog retention hours', '0',
   'Number of hours that binary logs are retained')
"""

_PROCEDURES = {
    "rds_kill": """
CREATE DEFINER = 'root'@'%' PROCEDURE mysql.rds_kill(IN thread_id BIGINT UNSIGNED)
SQL SECURITY DEFINER
KILL CONNECTION thread_id
""",
    "rds_kill_query": """
CREATE DEFINER = 'root'@'%' PROCEDURE mysql.rds_kill_query(IN thread_id BIGINT UNSIGNED)
SQL SECURITY DEFINER
KILL QUERY thread_id
""",
    "rds_show_configuration": """
CREATE DEFINER = 'root'@'%' PROCEDURE mysql.rds_show_configuration()
SQL SECURITY DEFINER
SELECT name, value, description
FROM mysql.ministack_rds_configuration
ORDER BY name
""",
    "rds_set_configuration": """
CREATE DEFINER = 'root'@'%' PROCEDURE mysql.rds_set_configuration(
  IN configuration_name VARCHAR(128),
  IN configuration_value VARCHAR(128)
)
SQL SECURITY DEFINER
BEGIN
  IF LOWER(configuration_name) <> 'binlog retention hours' THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Unsupported RDS configuration';
  END IF;
  INSERT INTO mysql.ministack_rds_configuration
    (name, value, description)
  VALUES
    ('binlog retention hours', configuration_value,
     'Number of hours that binary logs are retained')
  ON DUPLICATE KEY UPDATE value = VALUES(value);
END
""",
}


def _execute_object(cursor, statement, object_name, resource_id):
    try:
        cursor.execute(statement)
        return True
    except Exception as e:
        logger.warning(
            "RDS: failed to ensure Aurora compatibility object %s for %s: %s",
            object_name,
            resource_id,
            e,
        )
        return False


def _supports_predefined_roles(engine, engine_series):
    if engine != "aurora-mysql":
        return False
    try:
        major = int(str(engine_series).split(".", 1)[0])
    except (TypeError, ValueError):
        return False
    return major >= 8


def ensure_rds_compatibility_procedures(
    connection_factory,
    resource_id,
    engine,
    engine_series,
):
    """Create the Aurora procedures needed by provider user-set grants.

    The session disables binary logging so each MySQL compute node owns its
    local DEFINER objects. Failures are loud but never fail RDS provisioning.
    """
    connection = None
    cursor = None
    try:
        connection = connection_factory()
        if connection is None:
            raise RuntimeError("admin connection is unavailable")
        cursor = connection.cursor()
        cursor.execute("SET SESSION sql_log_bin = 0")
        all_ready = _execute_object(
            cursor,
            _CONFIG_TABLE_SQL,
            "mysql.ministack_rds_configuration table",
            resource_id,
        )
        all_ready = _execute_object(
            cursor,
            _CONFIG_DEFAULT_SQL,
            "binlog retention hours default",
            resource_id,
        ) and all_ready
        if _supports_predefined_roles(engine, engine_series):
            for role in _PREDEFINED_ROLES:
                all_ready = _execute_object(
                    cursor,
                    f"CREATE ROLE IF NOT EXISTS `{role}`@'%'",
                    f"`{role}`@'%' role",
                    resource_id,
                ) and all_ready

        existing = set()
        try:
            cursor.execute(
                "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES "
                "WHERE ROUTINE_SCHEMA = 'mysql' "
                "AND ROUTINE_TYPE = 'PROCEDURE' "
                "AND ROUTINE_NAME IN (%s, %s, %s, %s)",
                tuple(_PROCEDURES),
            )
            existing = {row[0].lower() for row in cursor.fetchall()}
        except Exception as e:
            logger.warning(
                "RDS: failed to inspect Aurora compatibility procedures "
                "for %s; attempting each procedure independently: %s",
                resource_id,
                e,
            )
            all_ready = False
        for name, statement in _PROCEDURES.items():
            if name not in existing:
                all_ready = _execute_object(
                    cursor,
                    statement,
                    f"mysql.{name} procedure",
                    resource_id,
                ) and all_ready
        logger.info(
            "RDS: ensured Aurora compatibility objects for %s",
            resource_id,
        )
        return all_ready
    except Exception as e:
        logger.warning(
            "RDS: failed to ensure Aurora compatibility objects for %s: %s",
            resource_id,
            e,
        )
        return False
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
