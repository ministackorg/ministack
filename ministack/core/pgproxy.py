"""
Asyncio Postgres wire-protocol proxy with Aurora DSQL validation.

One proxy per DSQL cluster: clients connect to the proxy on the cluster's
localhost port; the proxy forwards frames to a real Postgres backend after
enforcing DSQL's PostgreSQL-compatibility subset.

Validator behavior is gated by ``DSQL_STRICT`` (default "1"); "0" turns the
proxy into a transparent passthrough.

v1 documented limitations:
- Extended-protocol ``Parse`` messages are relayed UNVALIDATED. psycopg2 sends
  DDL via the simple ``Q`` protocol when no parameters are involved, and psql
  always uses ``Q``, so the common paths are covered.
- Multi-statement ``Q`` batches are split naively on ``;`` for validation
  (a semicolon inside a string literal or dollar-quoted body can confuse the
  splitter) and ``CREATE INDEX`` probing / ``ASYNC`` rewriting only apply to
  single-statement queries.
- Type/name parsing is heuristic regex work, not a real SQL parser.
"""

import asyncio
import logging
import os
import re
import secrets
import string
import struct
import time
from datetime import datetime, timezone

logger = logging.getLogger("pgproxy")

STRICT = os.environ.get("DSQL_STRICT", "1").strip().lower() not in ("0", "false", "no", "off")

# ---------------------------------------------------------------------------
# Wire-protocol constants and frame helpers
# ---------------------------------------------------------------------------

_SSL_REQUEST = 80877103
_GSSENC_REQUEST = 80877104
_CANCEL_REQUEST = 80877102
_PROTOCOL_3 = 196608

_STARTUP_PARAMS = (
    ("server_version", "16.4"),
    ("server_encoding", "UTF8"),
    ("client_encoding", "UTF8"),
    ("DateStyle", "ISO"),
    ("integer_datetimes", "on"),
    ("standard_conforming_strings", "on"),
)

_TEXT_OID = 25


def _frame(type_byte, payload):
    return type_byte + struct.pack("!I", len(payload) + 4) + payload


def _error_response(sqlstate, message):
    payload = (
        b"SERROR\0VERROR\0"
        + b"C" + sqlstate.encode() + b"\0"
        + b"M" + message.encode() + b"\0"
        + b"\0"
    )
    return _frame(b"E", payload)


def _ready(status=b"I"):
    return _frame(b"Z", status)


def _command_complete(tag):
    return _frame(b"C", tag.encode() + b"\0")


def _row_description(cols):
    """cols: names, or (name, type_oid) tuples."""
    out = struct.pack("!H", len(cols))
    for col in cols:
        name, oid = col if isinstance(col, tuple) else (col, _TEXT_OID)
        # table oid, column attnum, type oid, typlen, typmod, format
        out += name.encode() + b"\0" + struct.pack("!IhIhih", 0, 0, oid, -1, -1, 0)
    return _frame(b"T", out)


def _data_row(values):
    out = struct.pack("!H", len(values))
    for v in values:
        if v is None:
            out += struct.pack("!i", -1)
        else:
            b = str(v).encode()
            out += struct.pack("!I", len(b)) + b
    return _frame(b"D", out)


def _client_greeting():
    out = _frame(b"R", struct.pack("!I", 0))  # AuthenticationOk
    for key, value in _STARTUP_PARAMS:
        out += _frame(b"S", key.encode() + b"\0" + value.encode() + b"\0")
    out += _frame(b"K", struct.pack("!II", secrets.randbelow(2**31), secrets.randbelow(2**31)))
    out += _ready(b"I")
    return out


async def _read_frame(reader):
    type_byte = await reader.readexactly(1)
    (length,) = struct.unpack("!I", await reader.readexactly(4))
    payload = await reader.readexactly(length - 4)
    return type_byte, payload


# ---------------------------------------------------------------------------
# Job registry (CREATE INDEX ASYNC / sys.jobs emulation)
# ---------------------------------------------------------------------------

_proxies = {}  # cluster_id -> asyncio.Server
_jobs = {}  # cluster_id -> [job dict]
_catalog_versions = {}  # cluster_id -> int, bumped on every DDL statement

_ID_ALPHABET = string.ascii_lowercase + string.digits

_JOB_COLUMNS = (
    "job_id", "status", "details", "job_type", "class_id", "object_id",
    "object_name", "start_time", "update_time",
)


def get_jobs(cluster_id):
    return list(_jobs.get(cluster_id, []))


def clear_jobs(cluster_id=None):
    """Drop job registry entries — one cluster, or all when no id is given."""
    if cluster_id is None:
        _jobs.clear()
        _catalog_versions.clear()
    else:
        _jobs.pop(cluster_id, None)
        _catalog_versions.pop(cluster_id, None)


def _bump_catalog(cluster_id):
    """Record a catalog change (any DDL) for a cluster."""
    _catalog_versions[cluster_id] = _catalog_versions.get(cluster_id, 0) + 1
    return _catalog_versions[cluster_id]


def _register_job(cluster_id, object_name, job_type="INDEX_BUILD"):
    now = datetime.now(timezone.utc).isoformat()
    job = {
        "job_id": "".join(secrets.choice(_ID_ALPHABET) for _ in range(26)),
        "status": "completed",
        "details": "",
        "job_type": job_type,
        "class_id": "1259",
        "object_id": str(16384 + secrets.randbelow(100000)),
        "object_name": object_name,
        "start_time": now,
        "update_time": now,
    }
    _jobs.setdefault(cluster_id, []).append(job)
    return job


# ---------------------------------------------------------------------------
# DSQL validator (pure, unit-testable)
# ---------------------------------------------------------------------------


class DsqlError:
    """Validator rejection: rendered as a PG ErrorResponse."""

    def __init__(self, sqlstate, message):
        self.sqlstate = sqlstate
        self.message = message

    def __repr__(self):
        return f"DsqlError({self.sqlstate!r}, {self.message!r})"


class Rewrite:
    """Statement must be rewritten before forwarding to the backend."""

    def __init__(self, sql, kind, object_name="", table="", job_type="INDEX_BUILD"):
        self.sql = sql
        self.kind = kind
        self.object_name = object_name
        self.table = table
        self.job_type = job_type

    def __repr__(self):
        return f"Rewrite({self.sql!r}, kind={self.kind!r})"


class TxnState:
    """Per-connection transaction discipline tracking."""

    def __init__(self):
        self.in_txn = False
        self.ddl_seen = False
        self.dml_seen = False
        self.started_at = None  # monotonic ts at BEGIN (duration limit)
        self.rows_est = 0  # static row estimate (VALUES tuples) in this txn
        self.bytes_est = 0  # cumulative DML payload bytes in this txn

    def copy(self):
        other = TxnState()
        other.in_txn = self.in_txn
        other.ddl_seen = self.ddl_seen
        other.dml_seen = self.dml_seen
        other.started_at = self.started_at
        other.rows_est = self.rows_est
        other.bytes_est = self.bytes_est
        return other

    def apply(self, sql):
        cls = classify_statement(sql)
        if cls == "tcl_begin":
            self.in_txn = True
            self.ddl_seen = self.dml_seen = False
            self.started_at = time.monotonic()
            self.rows_est = self.bytes_est = 0
        elif cls == "tcl_end":
            self.in_txn = False
            self.ddl_seen = self.dml_seen = False
            self.started_at = None
            self.rows_est = self.bytes_est = 0
        elif cls == "ddl":
            self.ddl_seen = True
        elif cls == "dml":
            self.dml_seen = True
            if self.in_txn:
                self.rows_est += _values_tuple_count(sql)
                self.bytes_est += len(sql.encode("utf-8", "replace"))


_DDL_KEYWORDS = {"CREATE", "ALTER", "DROP"}
_DML_KEYWORDS = {"INSERT", "UPDATE", "DELETE"}
_TCL_BEGIN = {"BEGIN", "START"}
_TCL_END = {"COMMIT", "END", "ROLLBACK", "ABORT"}


def classify_statement(sql):
    """Classify one statement: ddl | dml | tcl_begin | tcl_end | other."""
    m = re.match(r"\s*([A-Za-z]+)", sql)
    if not m:
        return "other"
    kw = m.group(1).upper()
    if kw in _DDL_KEYWORDS:
        return "ddl"
    if kw in _DML_KEYWORDS:
        return "dml"
    if kw in _TCL_BEGIN:
        return "tcl_begin"
    if kw in _TCL_END:
        return "tcl_end"
    return "other"


def split_statements(sql):
    """Naive batch split on ';' (heuristic — strings are not parsed)."""
    return [s.strip() for s in sql.split(";") if s.strip()]


def _values_tuple_count(sql):
    """Count row tuples in an INSERT ... VALUES (...), (...), ... statement.

    Static estimate for the 3,000-rows-per-transaction limit; INSERT ...
    SELECT / UPDATE / DELETE row counts can't be known without executing.
    """
    m = re.search(r"\bVALUES\b", sql, re.I)
    if not m or not re.match(r"\s*INSERT\b", sql, re.I):
        return 0
    start = sql.find("(", m.end())
    if start < 0:
        return 0
    count, depth = 0, 0
    in_str = False
    for i in range(start, len(sql)):
        ch = sql[i]
        if ch == "'":
            in_str = not in_str
        elif in_str:
            continue
        elif ch == "(":
            if depth == 0:
                count += 1
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0 and i + 1 < len(sql) and sql[i + 1:].lstrip()[:1] not in (",",):
                break
    return count


# --- Denylist (rule 1) ------------------------------------------------------

_DENYLIST = (
    (r"CREATE\s+DATABASE\b", "CREATE DATABASE"),
    (r"CREATE\s+TYPE\b", "CREATE TYPE"),
    (r"CREATE\s+EXTENSION\b", "CREATE EXTENSION"),
    (r"CREATE\s+TRIGGER\b", "CREATE TRIGGER"),
    (r"CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\b", "CREATE PROCEDURE"),
    (r"CREATE\s+TABLESPACE\b", "CREATE TABLESPACE"),
    (r"CREATE\s+MATERIALIZED\s+VIEW\b", "materialized views"),
    (r"CREATE\s+(TEMP|TEMPORARY|UNLOGGED)\s+TABLE\b", "temporary/unlogged tables"),
    (r"COPY\b", "COPY"),
    (r"LISTEN\b", "LISTEN"),
    (r"NOTIFY\b", "NOTIFY"),
    (r"UNLISTEN\b", "UNLISTEN"),
    (r"TRUNCATE\b", "TRUNCATE"),
    (r"DO\b", "DO"),
    (r"SAVEPOINT\b", "SAVEPOINT"),
    (r"LOCK\b", "LOCK TABLE"),
)


def _check_denylist(sql):
    for pattern, thing in _DENYLIST:
        if re.match(r"\s*" + pattern, sql, re.I):
            return DsqlError("0A000", f"{thing} is not supported")
    m = re.match(r"\s*CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\b", sql, re.I)
    if m:
        lang = re.search(r"\bLANGUAGE\s+['\"]?([A-Za-z0-9_]+)", sql, re.I)
        if lang and lang.group(1).lower() != "sql":
            return DsqlError("0A000", f"LANGUAGE {lang.group(1)} is not supported")
    if re.match(r"\s*(CREATE|ALTER)\s+TABLE\b", sql, re.I) or re.match(
        r"\s*CREATE\s+(TEMP|TEMPORARY|UNLOGGED)\s+TABLE\b", sql, re.I
    ):
        if re.search(r"\bFOREIGN\s+KEY\b|\bREFERENCES\b", sql, re.I):
            return DsqlError("0A000", "foreign key constraints are not supported")
        if re.search(r"\bPARTITION\s+BY\b|\bPARTITION\s+OF\b|\bINHERITS\b", sql, re.I):
            return DsqlError("0A000", "table partitioning is not supported")
        if re.search(r"\bEXCLUDE\b", sql, re.I):
            return DsqlError("0A000", "the EXCLUDE constraint is not supported")
    if re.match(r"\s*ALTER\s+TABLE\b", sql, re.I):
        add = re.search(r"\bADD\s+COLUMN\b", sql, re.I)
        # STORAGE DEFAULT is a storage option and BY DEFAULT an identity
        # option — neither is an inline column default. Generated columns
        # (GENERATED ALWAYS AS ... STORED / identity) are allowed.
        if add and re.search(
            r"(?<!STORAGE )(?<!BY )\bDEFAULT\b|\bNOT\s+NULL\b", sql[add.end():], re.I
        ):
            return DsqlError(
                "0A000",
                "ALTER TABLE ADD COLUMN with DEFAULT or NOT NULL is not supported",
            )
    return None


# --- Column types (rule 2) --------------------------------------------------

# Normalized allowlist of DSQL-supported column types.
_TYPE_ALIASES = {
    "smallint": "smallint", "int2": "smallint",
    "integer": "integer", "int": "integer", "int4": "integer",
    "bigint": "bigint", "int8": "bigint",
    "real": "real", "float4": "real",
    "double precision": "double precision", "float8": "double precision",
    "numeric": "numeric", "decimal": "numeric", "dec": "numeric",
    "character": "character", "char": "character", "bpchar": "character",
    "character varying": "character varying", "varchar": "character varying",
    "text": "text",
    "date": "date",
    "time": "time", "time without time zone": "time",
    "time with time zone": "time with time zone", "timetz": "time with time zone",
    "timestamp": "timestamp", "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamp with time zone",
    "timestamptz": "timestamp with time zone",
    "interval": "interval",
    "boolean": "boolean", "bool": "boolean",
    "bytea": "bytea", "uuid": "uuid", "json": "json", "jsonb": "jsonb",
}

# Keywords that terminate the type portion of a column definition.
_COL_CONSTRAINT_KEYWORDS = {
    "not", "null", "default", "primary", "unique", "check", "references",
    "collate", "generated", "constraint", "storage",
}

# Field-qualified interval types: interval year, interval day to second, ...
_INTERVAL_RE = re.compile(
    r"interval(\s+(year|month|day|hour|minute|second)"
    r"(\s+to\s+(year|month|day|hour|minute|second))?)?$"
)

# Table-level constraint starters inside CREATE TABLE (...).
_TABLE_CONSTRAINT_STARTERS = {
    "primary", "unique", "check", "constraint", "foreign", "like", "exclude",
}


def _split_top_level(body):
    """Split a parenthesised body on commas at paren depth 0."""
    parts, depth, current = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _normalize_type(raw):
    """Return (normalized, display) for a raw type string, or None."""
    raw = raw.strip()
    if not raw:
        return None
    is_array = False
    while raw.rstrip().endswith("[]"):
        is_array = True
        raw = raw.rstrip()[:-2]
    display = re.sub(r"\s+", " ", raw.strip())
    # strip precision/scale and trailing modifiers: numeric(18,6), varchar(10)
    base = re.sub(r"\(.*?\)", "", display)
    base = re.sub(r"\s+", " ", base).strip().lower()
    if is_array:
        return None, display + "[]"
    if _INTERVAL_RE.match(base):
        return "interval", display
    normalized = _TYPE_ALIASES.get(base)
    return (normalized, display) if normalized else (None, display)


def _type_from_tokens(tokens):
    """Given column-def tokens after the column name, extract the type."""
    type_tokens = []
    for tok in tokens:
        word = re.sub(r"\(.*", "", tok).lower()
        if word in _COL_CONSTRAINT_KEYWORDS:
            break
        type_tokens.append(tok)
    return " ".join(type_tokens)


def _check_type(raw_type):
    """Return a DsqlError if the raw type string is not DSQL-supported."""
    result = _normalize_type(raw_type)
    if result is None:
        return None
    normalized, display = result
    if normalized is None:
        return DsqlError("0A000", f'type "{display}" is not supported')
    return None


def _check_identity(col_def, raw_type):
    """DSQL supports identity columns on bigint columns only."""
    if re.search(r"\bGENERATED\b", col_def, re.I) and re.search(
        r"\bAS\s+IDENTITY\b", col_def, re.I
    ):
        result = _normalize_type(raw_type)
        if result and result[0] is not None and result[0] != "bigint":
            return DsqlError(
                "0A000", "identity columns are only supported on bigint columns"
            )
    return None


def _check_column_types(sql):
    m = re.match(
        r"\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\w\".]+\s*", sql, re.I
    )
    if m:
        rest = sql[m.end():]
        if not rest.lstrip().startswith("("):
            return None  # CREATE TABLE ... AS / PARTITION OF / LIKE-only
        start = sql.index("(", m.end())
        depth, end = 0, -1
        for i in range(start, len(sql)):
            if sql[i] == "(":
                depth += 1
            elif sql[i] == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end < 0:
            return None
        for part in _split_top_level(sql[start + 1 : end]):
            tokens = part.strip().split()
            if not tokens:
                continue
            if tokens[0].strip('"').lower() in _TABLE_CONSTRAINT_STARTERS:
                continue
            raw_type = _type_from_tokens(tokens[1:])
            err = _check_type(raw_type)
            if err:
                return err
            err = _check_identity(part, raw_type)
            if err:
                return err
        return None
    m = re.match(r"\s*ALTER\s+TABLE\b", sql, re.I)
    if m:
        add = re.search(r"\bADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\"]+)\s+", sql, re.I)
        if add:
            tail = sql[add.end():]
            tokens = tail.split()
            raw_type = _type_from_tokens(tokens)
            err = _check_type(raw_type)
            if err:
                return err
            err = _check_identity(tail, raw_type)
            if err:
                return err
    return None


# --- ALTER TABLE subset (AWS alter-table-syntax-support) ---------------------

# DSQL supports only a subset of ALTER TABLE actions: ADD COLUMN (no inline
# DEFAULT/NOT NULL — handled in _check_denylist), DROP COLUMN, SET/DROP
# DEFAULT, DROP NOT NULL, DROP EXPRESSION, identity actions, SET STORAGE,
# ADD CONSTRAINT ... CHECK ... NOT VALID, ADD CONSTRAINT ... UNIQUE USING
# INDEX, DROP CONSTRAINT, RENAME, SET SCHEMA, OWNER TO, and the async
# VALIDATE CONSTRAINT form (handled as a Rewrite in validate()).
def _check_alter_table(sql):
    if not re.match(r"\s*ALTER\s+TABLE\b", sql, re.I):
        return None
    if re.search(
        r"\bALTER\s+COLUMN\s+[\w\"]+\s+(?:SET\s+DATA\s+)?TYPE\b", sql, re.I
    ):
        return DsqlError("0A000", "ALTER COLUMN TYPE is not supported")
    if re.search(r"\bSET\s+NOT\s+NULL\b", sql, re.I):
        return DsqlError(
            "0A000", "SET NOT NULL is not supported (only DROP NOT NULL)"
        )
    add = re.search(r"\bADD\s+(?:CONSTRAINT\s+[\w\"]+\s+)?(\w+)", sql, re.I)
    if add and add.group(1).upper() != "COLUMN":
        what = add.group(1).upper()
        if what == "PRIMARY":
            return DsqlError(
                "0A000", "ADD CONSTRAINT PRIMARY KEY is not supported"
            )
        if what == "UNIQUE" and not re.search(
            r"\bUNIQUE\s+USING\s+INDEX\b", sql, re.I
        ):
            return DsqlError(
                "0A000",
                "ADD CONSTRAINT UNIQUE (...) is not supported — use "
                "CREATE UNIQUE INDEX ASYNC, then ADD CONSTRAINT ... "
                "UNIQUE USING INDEX",
            )
        if what == "CHECK" and not re.search(r"\bNOT\s+VALID\b", sql, re.I):
            return DsqlError(
                "0A000",
                "CHECK constraints added via ALTER TABLE must use NOT VALID",
            )
    # ADD GENERATED ... AS IDENTITY requires an explicit CACHE value.
    if re.search(r"\bADD\s+GENERATED\b", sql, re.I) and re.search(
        r"\bIDENTITY\b", sql, re.I
    ):
        if not re.search(r"\bCACHE\s+\d+", sql, re.I):
            return DsqlError(
                "0A000",
                "ADD GENERATED AS IDENTITY requires an explicit CACHE value",
            )
    return None


# IDENTITY columns and sequences: CACHE must be 1 or >= 65536.
def _check_cache_value(sql):
    if not re.match(r"\s*(CREATE|ALTER)\s+(TABLE|SEQUENCE)\b", sql, re.I):
        return None
    m = re.search(r"\bCACHE\s+(\d+)", sql, re.I)
    if m:
        n = int(m.group(1))
        if n != 1 and n < 65536:
            return DsqlError("0A000", "CACHE must be 1 or at least 65536")
    return None


# DSQL index rules: btree only (no USING gin/gist/brin/hash/spgist), no
# CONCURRENTLY (use ASYNC), no partial (WHERE) or expression indexes, and at
# most 8 columns per index (24 indexes per table is enforced at runtime).
def _check_index_rules(sql):
    m = _INDEX_RE.match(sql) or _INDEX_ASYNC_RE.match(sql)
    if not m:
        return None
    using = re.search(r"\bUSING\s+(\w+)", sql, re.I)
    if using and using.group(1).lower() not in ("btree", "btree_index"):
        return DsqlError(
            "0A000", f"USING {using.group(1)} is not supported (btree only)"
        )
    if re.search(r"\bCONCURRENTLY\b", sql, re.I):
        return DsqlError(
            "0A000", "CREATE INDEX CONCURRENTLY is not supported — use CREATE INDEX ASYNC"
        )
    if re.search(r"\bWHERE\b", sql, re.I):
        return DsqlError("0A000", "partial indexes (WHERE) are not supported")
    start = sql.find("(", m.end())
    if start < 0:
        return None
    depth, end = 0, -1
    for i in range(start, len(sql)):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end < 0:
        return None
    parts = _split_top_level(sql[start + 1 : end])
    if any("(" in part for part in parts):
        return DsqlError("0A000", "expression indexes are not supported")
    if len(parts) > 8:
        return DsqlError(
            "0A000", f"indexes cannot have more than 8 columns ({len(parts)} given)"
        )
    return None


_ALTER_ASYNC_VALIDATE_RE = re.compile(
    r"\s*ALTER\s+TABLE\s+ASYNC\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?"
    r"([\w\".]+)\s*\*?\s*VALIDATE\s+CONSTRAINT\s+([\w\"]+)",
    re.I,
)


def rewrite_alter_async(sql):
    """Strip the DSQL-only ASYNC keyword from ALTER TABLE ASYNC."""
    return re.sub(r"(ALTER\s+TABLE)\s+ASYNC", r"\1", sql, count=1, flags=re.I)


# --- CREATE INDEX ASYNC (rules 3 & 4) ---------------------------------------

_INDEX_ASYNC_RE = re.compile(
    r"\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+ASYNC\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:([\w\".]+)\s+)?ON\s+(?:ONLY\s+)?(?:USING\s+\w+\s+)?([\w\".]+)",
    re.I,
)

_INDEX_RE = re.compile(
    r"\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(?:([\w\".]+)\s+)?ON\s+(?:ONLY\s+)?"
    r"(?:USING\s+\w+\s+)?([\w\".]+)",
    re.I,
)


def rewrite_index_async(sql):
    """Strip the DSQL-only ASYNC keyword from CREATE [UNIQUE] INDEX ASYNC."""
    return re.sub(
        r"(CREATE\s+(?:UNIQUE\s+)?INDEX)\s+ASYNC", r"\1", sql, count=1, flags=re.I
    )


def is_plain_create_index(sql):
    return bool(_INDEX_RE.match(sql)) and not _INDEX_ASYNC_RE.match(sql)


def create_index_table(sql):
    m = _INDEX_RE.match(sql) or _INDEX_ASYNC_RE.match(sql)
    return m.group(2) if m else None


def _index_object_name(sql, on_end, index_name, table):
    """Schema-qualified index name for the sys.jobs object_name field.

    Unnamed indexes get DSQL's documented auto-name ``<table>_<col>_..._idx``
    in the table's schema (default ``public``).
    """
    schema, _, bare_table = table.rpartition(".")
    schema = schema.strip('"') or "public"
    bare_table = bare_table.strip('"')
    if index_name:
        if "." in index_name:
            return index_name
        return f"{schema}.{index_name.strip(chr(34))}"
    cols = []
    start = sql.find("(", on_end)
    if start >= 0:
        depth, end = 0, -1
        for i in range(start, len(sql)):
            if sql[i] == "(":
                depth += 1
            elif sql[i] == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end > 0:
            for part in _split_top_level(sql[start + 1 : end]):
                tokens = part.strip().split()
                if tokens:
                    ident = re.match(r'[A-Za-z_"][\w"]*', tokens[0])
                    if ident:
                        cols.append(ident.group(0).strip('"'))
    suffix = "_" + "_".join(cols) if cols else ""
    return f"{schema}.{bare_table}{suffix}_idx"


# --- sys.jobs / sys.wait_for_job (rules 5 & 6) ------------------------------

_SYS_JOBS_RE = re.compile(r"\s*SELECT\s+(.+?)\s+FROM\s+sys\.jobs\b", re.I | re.S)
_JOB_ID_FILTER_RE = re.compile(r"job_id\s*=\s*'([^']*)'", re.I)
_WAIT_JOB_RE = re.compile(
    r"\s*(?:SELECT|CALL)\s+sys\.wait_for_job\s*\(\s*"
    r"(?:job_id\s*\)\s*'([^']*)'|'([^']*)'\s*\))",
    re.I,
)


def match_sys_jobs(sql):
    """Return (columns, job_id_filter) if this queries sys.jobs, else False.

    ``columns`` is None for ``SELECT *``; ``job_id_filter`` is None when
    there is no ``WHERE job_id = '<id>'`` clause.
    """
    m = _SYS_JOBS_RE.match(sql)
    if not m:
        return False
    select_list = m.group(1).strip()
    columns = None
    if select_list != "*":
        columns = [c.strip().strip('"') for c in select_list.split(",")]
    f = _JOB_ID_FILTER_RE.search(sql)
    return columns, (f.group(1) if f else None)


def match_wait_for_job(sql):
    """Match both documented call forms: ``sys.wait_for_job(job_id) '<id>'``
    and the conventional ``sys.wait_for_job('<id>')``."""
    m = _WAIT_JOB_RE.match(sql)
    if not m:
        return None
    return m.group(1) or m.group(2)


# --- Top-level validator -----------------------------------------------------


def validate(sql, txn_state=None):
    """Validate one statement. Returns DsqlError | Rewrite | None.

    Read-only with respect to ``txn_state`` — the caller applies state
    changes (``TxnState.apply``) once the statement is actually forwarded.
    """
    if not STRICT:
        return None
    s = sql.strip()
    if not s:
        return None

    if txn_state is not None and txn_state.in_txn:
        cls = classify_statement(s)
        if cls == "ddl":
            if txn_state.ddl_seen:
                return DsqlError(
                    "25006", "only one DDL statement can be run in a transaction"
                )
            if txn_state.dml_seen:
                return DsqlError(
                    "25006", "DDL and DML statements cannot be mixed in a transaction"
                )
        elif cls == "dml" and txn_state.ddl_seen:
            return DsqlError(
                "25006", "DDL and DML statements cannot be mixed in a transaction"
            )
        # Documented DSQL transaction limits (messages are approximations).
        if (
            txn_state.started_at is not None
            and time.monotonic() - txn_state.started_at > 300
        ):
            return DsqlError(
                "25006", "transaction exceeded the 5 minute duration limit"
            )

    # Row/size limits apply to any transaction, including an implicit
    # single-statement one. Row counting is static (VALUES tuples) —
    # INSERT ... SELECT and UPDATE/DELETE affected-row counts are not
    # enforced.
    if txn_state is not None and classify_statement(s) == "dml":
        rows_est = txn_state.rows_est if txn_state.in_txn else 0
        bytes_est = txn_state.bytes_est if txn_state.in_txn else 0
        if rows_est + _values_tuple_count(s) > 3000:
            return DsqlError(
                "25006", "transaction exceeds the 3,000 row limit"
            )
        if bytes_est + len(s.encode("utf-8", "replace")) > 10 * 1024 * 1024:
            return DsqlError(
                "25006", "transaction exceeds the 10 MiB size limit"
            )

    err = _check_denylist(s)
    if err:
        return err
    err = _check_alter_table(s)
    if err:
        return err
    err = _check_cache_value(s)
    if err:
        return err
    err = _check_column_types(s)
    if err:
        return err
    err = _check_index_rules(s)
    if err:
        return err

    m = _ALTER_ASYNC_VALIDATE_RE.match(s)
    if m:
        table = m.group(1)
        schema, _, bare = table.rpartition(".")
        object_name = (schema.strip('"') or "public") + "." + bare.strip('"')
        return Rewrite(
            rewrite_alter_async(s),
            kind="index_async",
            object_name=object_name,
            table=table,
            job_type="VALIDATE_CONSTRAINT",
        )

    m = _INDEX_ASYNC_RE.match(s)
    if m:
        index_name, table = m.group(1), m.group(2)
        return Rewrite(
            rewrite_index_async(s),
            kind="index_async",
            object_name=_index_object_name(s, m.end(), index_name, table),
            table=table,
        )
    return None


# ---------------------------------------------------------------------------
# Proxy connection handling
# ---------------------------------------------------------------------------


class _Conn:
    def __init__(self, cluster_id):
        self.cluster_id = cluster_id
        self.txn = TxnState()
        self.capture = None  # asyncio.Queue while swallowing backend frames
        self.catalog_version = None  # last catalog version this conn has seen


async def _read_startup(reader):
    (length,) = struct.unpack("!I", await reader.readexactly(4))
    body = await reader.readexactly(length - 4)
    (code,) = struct.unpack("!I", body[:4])
    return code, body[4:]


def _parse_startup_params(payload):
    parts = payload.split(b"\0")
    params = {}
    for i in range(0, len(parts) - 1, 2):
        if parts[i]:
            params[parts[i].decode("utf-8", "replace")] = parts[i + 1].decode(
                "utf-8", "replace"
            )
    return params


async def _connect_backend(host, port, database, attempts=20, delay=0.5):
    """Open a backend connection with trust auth and complete its startup.

    Retries briefly: docker-proxy accepts (then drops) connections while the
    postgres entrypoint is still initialising, and containers can bounce.
    """
    last_err = None
    for _ in range(attempts):
        try:
            return await _connect_backend_once(host, port, database)
        except Exception as e:
            last_err = e
            await asyncio.sleep(delay)
    raise RuntimeError(f"backend {host}:{port} not reachable: {last_err}")


async def _connect_backend_once(host, port, database):
    reader, writer = await asyncio.open_connection(host, port)
    params = b"user\0postgres\0database\0" + database.encode() + b"\0\0"
    payload = struct.pack("!I", _PROTOCOL_3) + params
    writer.write(struct.pack("!I", len(payload) + 4) + payload)
    await writer.drain()
    try:
        while True:
            type_byte, payload = await _read_frame(reader)
            if type_byte == b"E":
                raise RuntimeError(f"backend startup failed: {payload!r}")
            if type_byte == b"R":
                (code,) = struct.unpack("!I", payload[:4])
                if code != 0:
                    raise RuntimeError(f"backend requested unsupported auth method {code}")
            elif type_byte == b"Z":
                return reader, writer
    except Exception:
        writer.close()
        raise


async def _backend_relay(conn, b_reader, c_writer):
    """Forward backend frames to the client (or to the capture queue)."""
    while True:
        type_byte, payload = await _read_frame(b_reader)
        if conn.capture is not None:
            conn.capture.put_nowait((type_byte, payload))
        else:
            c_writer.write(_frame(type_byte, payload))
            await c_writer.drain()


async def _run_backend_capture(conn, b_writer, sql):
    """Send a query to the backend and swallow its frames until ReadyForQuery."""
    queue = asyncio.Queue()
    conn.capture = queue
    try:
        b_writer.write(_frame(b"Q", sql.encode() + b"\0"))
        await b_writer.drain()
        frames = []
        while True:
            type_byte, payload = await queue.get()
            frames.append((type_byte, payload))
            if type_byte == b"Z":
                return frames
    finally:
        conn.capture = None


async def _table_has_rows(conn, b_writer, table):
    """Probe SELECT 1 FROM <table> LIMIT 1 on the client's backend connection."""
    try:
        frames = await _run_backend_capture(
            conn, b_writer, f"SELECT 1 FROM {table} LIMIT 1"
        )
    except Exception:
        return False
    for type_byte, _ in frames:
        if type_byte == b"E":
            return False  # probe failed — forward original; backend errors
        if type_byte == b"D":
            return True
    return False


async def _index_count(conn, b_writer, table):
    """Count the table's existing indexes via pg_indexes. None on probe error."""
    schema, _, bare = table.rpartition(".")
    schema = schema.strip('"') or "public"
    bare = bare.strip('"')
    try:
        frames = await _run_backend_capture(
            conn,
            b_writer,
            "SELECT count(*) FROM pg_indexes "
            f"WHERE schemaname = '{schema}' AND tablename = '{bare}'",
        )
    except Exception:
        return None
    for type_byte, payload in frames:
        if type_byte == b"E":
            return None
        if type_byte == b"D":
            try:
                (nfields,) = struct.unpack("!H", payload[:2])
                if nfields != 1:
                    return None
                (vlen,) = struct.unpack("!i", payload[2:6])
                return int(payload[6 : 6 + vlen])
            except Exception:
                return None
    return None


def _reject(conn, c_writer, err):
    status = b"T" if conn.txn.in_txn else b"I"
    c_writer.write(_error_response(err.sqlstate, err.message) + _ready(status))


_FOR_UPDATE_MULTI = (
    "locking clause such as FOR UPDATE can be applied on a single table"
)
_FOR_UPDATE_EQ = (
    "locking clause such as FOR UPDATE can be applied only on tables "
    "with equality predicates on the key"
)
_FOR_UPDATE_RE = re.compile(r"\bFOR\s+(?:NO\s+KEY\s+)?UPDATE\b", re.I)


async def _check_for_update(conn, b_writer, sql):
    """DSQL locking-clause rules (exact messages from the AWS features doc).

    FOR UPDATE needs a single table and equality predicates on all primary
    key columns. Returns a DsqlError, or None to forward.
    """
    body = _FOR_UPDATE_RE.split(sql)[0]
    m = re.search(
        r"\bFROM\b(.+?)(?:\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|$)",
        body, re.I | re.S,
    )
    if not m:
        return DsqlError("0A000", _FOR_UPDATE_EQ)
    from_part = m.group(1)
    if "," in from_part or re.search(r"\bJOIN\b", from_part, re.I):
        return DsqlError("0A000", _FOR_UPDATE_MULTI)
    table = from_part.strip().split()[0].strip('"')
    try:
        frames = await _run_backend_capture(
            conn,
            b_writer,
            "SELECT a.attname FROM pg_index i JOIN pg_attribute a "
            "ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            f"WHERE i.indrelid = '{table}'::regclass AND i.indisprimary",
        )
    except Exception:
        return None  # probe failed — let the backend answer
    pk_cols = []
    for type_byte, payload in frames:
        if type_byte == b"E":
            return None
        if type_byte == b"D":
            (nfields,) = struct.unpack("!H", payload[:2])
            if nfields == 1:
                (vlen,) = struct.unpack("!i", payload[2:6])
                pk_cols.append(payload[6 : 6 + vlen].decode("utf-8", "replace"))
    wm = re.search(r"\bWHERE\b(.+)$", body, re.I | re.S)
    if not pk_cols or not wm:
        return DsqlError("0A000", _FOR_UPDATE_EQ)
    where = wm.group(1)
    if re.search(
        r"<>|!=|<=|>=|<|>|\bOR\b|\bIN\s*\(|\bBETWEEN\b|\bLIKE\b", where, re.I
    ):
        return DsqlError("0A000", _FOR_UPDATE_EQ)
    for col in pk_cols:
        if not (
            re.search(rf"\b{re.escape(col)}\b\s*=[^=]", where, re.I)
            or re.search(rf"=[^=][^;]*\b{re.escape(col)}\b", where, re.I)
        ):
            return DsqlError("0A000", _FOR_UPDATE_EQ)
    return None


async def _handle_query(conn, sql, b_writer, c_writer):
    txn = conn.txn
    stmts = split_statements(sql)

    if len(stmts) == 1 and STRICT:
        s = stmts[0]

        # OC001: this connection's cached catalog is behind a DDL change made
        # by another session. Abort like real DSQL (40001); the next attempt
        # sees the fresh catalog and succeeds.
        if (
            txn.in_txn
            and conn.catalog_version is not None
            and conn.catalog_version < _catalog_versions.get(conn.cluster_id, 0)
        ):
            await _run_backend_capture(conn, b_writer, "ROLLBACK")
            txn.apply("ROLLBACK")
            conn.catalog_version = _catalog_versions.get(conn.cluster_id, 0)
            _reject(
                conn,
                c_writer,
                DsqlError(
                    "40001",
                    "schema has been updated by another transaction (OC001)",
                ),
            )
            await c_writer.drain()
            return

        matched = match_sys_jobs(s)
        if matched is not False:
            columns, job_filter = matched
            cols = list(columns) if columns else list(_JOB_COLUMNS)
            bad = [c for c in cols if c not in _JOB_COLUMNS]
            if bad:
                _reject(
                    conn,
                    c_writer,
                    DsqlError("42703", f'column "{bad[0]}" does not exist'),
                )
                await c_writer.drain()
                return
            jobs = get_jobs(conn.cluster_id)
            if job_filter:
                jobs = [j for j in jobs if j["job_id"] == job_filter]
            out = _row_description(cols)
            for job in jobs:
                out += _data_row([job[c] for c in cols])
            out += _command_complete(f"SELECT {len(jobs)}")
            out += _ready(b"T" if txn.in_txn else b"I")
            c_writer.write(out)
            await c_writer.drain()
            return

        job_id = match_wait_for_job(s)
        if job_id is not None:
            known = any(
                j["job_id"] == job_id and j["status"] == "completed"
                for j in get_jobs(conn.cluster_id)
            )
            out = _row_description([("wait_for_job", 16)])  # bool oid
            out += _data_row(["t" if known else "f"])
            out += _command_complete("SELECT 1")
            out += _ready(b"T" if txn.in_txn else b"I")
            c_writer.write(out)
            await c_writer.drain()
            return

        result = validate(s, txn)
        if isinstance(result, DsqlError):
            _reject(conn, c_writer, result)
            await c_writer.drain()
            return
        if isinstance(result, Rewrite) and result.kind == "index_async":
            if result.job_type == "INDEX_BUILD" and result.table:
                count = await _index_count(conn, b_writer, result.table)
                if count is not None and count >= 24:
                    _reject(
                        conn,
                        c_writer,
                        DsqlError(
                            "0A000",
                            f"table {result.table} already has the maximum of 24 indexes",
                        ),
                    )
                    await c_writer.drain()
                    return
            txn.apply(s)
            # Run the rewritten statement on the backend, swallow its
            # response, and synthesize the DSQL job_id result set.
            frames = await _run_backend_capture(conn, b_writer, result.sql)
            backend_err = next(
                (p for t, p in frames if t == b"E"), None
            )
            if backend_err is not None:
                c_writer.write(
                    _frame(b"E", backend_err)
                    + _ready(b"T" if txn.in_txn else b"I")
                )
                await c_writer.drain()
                return
            conn.catalog_version = _bump_catalog(conn.cluster_id)
            job = _register_job(conn.cluster_id, result.object_name, result.job_type)
            out = _row_description(["job_id"])
            out += _data_row([job["job_id"]])
            out += _command_complete("SELECT 1")
            out += _ready(b"T" if txn.in_txn else b"I")
            c_writer.write(out)
            await c_writer.drain()
            return

        if is_plain_create_index(s):
            table = create_index_table(s)
            if table:
                count = await _index_count(conn, b_writer, table)
                if count is not None and count >= 24:
                    _reject(
                        conn,
                        c_writer,
                        DsqlError(
                            "0A000",
                            f"table {table} already has the maximum of 24 indexes",
                        ),
                    )
                    await c_writer.drain()
                    return
            if table and await _table_has_rows(conn, b_writer, table):
                _reject(
                    conn,
                    c_writer,
                    DsqlError("0A000", "use CREATE INDEX ASYNC instead"),
                )
                await c_writer.drain()
                return
            txn.apply(s)
            conn.catalog_version = _bump_catalog(conn.cluster_id)
            b_writer.write(_frame(b"Q", sql.encode() + b"\0"))
            await b_writer.drain()
            return

        if _FOR_UPDATE_RE.search(s):
            err = await _check_for_update(conn, b_writer, s)
            if err:
                _reject(conn, c_writer, err)
                await c_writer.drain()
                return

        if classify_statement(s) == "ddl":
            conn.catalog_version = _bump_catalog(conn.cluster_id)
        else:
            conn.catalog_version = _catalog_versions.get(conn.cluster_id, 0)
        txn.apply(s)
        b_writer.write(_frame(b"Q", sql.encode() + b"\0"))
        await b_writer.drain()
        return

    # Multi-statement batch (implicit transaction) or non-strict mode:
    # validate every statement up front, then forward the batch verbatim.
    if STRICT and stmts:
        sim = txn.copy()
        explicit = False
        if not sim.in_txn and len(stmts) > 1:
            sim.in_txn = True  # implicit transaction
        for s in stmts:
            if classify_statement(s) == "tcl_begin":
                explicit = True
            result = validate(s, sim)
            if isinstance(result, (DsqlError, Rewrite)):
                if isinstance(result, Rewrite):
                    # ASYNC rewrites only supported for single statements.
                    result = DsqlError(
                        "0A000",
                        "asynchronous DDL is not supported in multi-statement queries",
                    )
                _reject(conn, c_writer, result)
                await c_writer.drain()
                return
            sim.apply(s)
        if txn.in_txn or explicit:
            txn.in_txn, txn.ddl_seen, txn.dml_seen = (
                sim.in_txn,
                sim.ddl_seen,
                sim.dml_seen,
            )
            txn.started_at, txn.rows_est, txn.bytes_est = (
                sim.started_at,
                sim.rows_est,
                sim.bytes_est,
            )
        # else: implicit batch committed — connection stays idle.
        if any(classify_statement(st) == "ddl" for st in stmts):
            conn.catalog_version = _bump_catalog(conn.cluster_id)
        else:
            conn.catalog_version = _catalog_versions.get(conn.cluster_id, 0)

    b_writer.write(_frame(b"Q", sql.encode() + b"\0"))
    await b_writer.drain()


async def _handle_client(cluster_id, backend_host, backend_port, c_reader, c_writer):
    conn = _Conn(cluster_id)
    b_writer = None
    relay = None
    try:
        # Startup phase (no type byte): SSL/GSS negotiation, then StartupMessage.
        while True:
            code, payload = await _read_startup(c_reader)
            if code in (_SSL_REQUEST, _GSSENC_REQUEST):
                c_writer.write(b"N")
                await c_writer.drain()
                continue
            if code == _CANCEL_REQUEST:
                return
            if code == _PROTOCOL_3:
                params = _parse_startup_params(payload)
                break
            return  # unknown startup packet — close

        # Authenticate the client ourselves: any user/password is accepted
        # (IAM auth tokens are client-side SigV4 presigning).
        c_writer.write(_client_greeting())
        await c_writer.drain()

        database = params.get("database") or "postgres"
        b_reader, b_writer = await _connect_backend(
            backend_host, backend_port, database
        )
        relay = asyncio.create_task(_backend_relay(conn, b_reader, c_writer))

        while True:
            type_byte, payload = await _read_frame(c_reader)
            if type_byte == b"X":  # Terminate
                break
            if type_byte == b"Q":
                sql = payload[:-1].decode("utf-8", "replace") if payload.endswith(b"\0") else payload.decode("utf-8", "replace")
                await _handle_query(conn, sql, b_writer, c_writer)
            else:
                # Everything else relays verbatim (P/B/E/C/d/c/f...).
                b_writer.write(_frame(type_byte, payload))
                await b_writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, OSError):
        pass
    except Exception:
        logger.exception("pgproxy: error in client connection (cluster %s)", cluster_id)
    finally:
        if relay is not None:
            relay.cancel()
        for w in (b_writer, c_writer):
            if w is not None:
                try:
                    w.close()
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Public proxy lifecycle
# ---------------------------------------------------------------------------


async def start_proxy(cluster_id, listen_port, backend_host, backend_port):
    """Bind one proxy listener for a cluster. Idempotent per cluster."""
    if cluster_id in _proxies:
        return

    async def handler(reader, writer):
        await _handle_client(cluster_id, backend_host, backend_port, reader, writer)

    server = await asyncio.start_server(handler, "0.0.0.0", listen_port)
    _proxies[cluster_id] = server
    logger.info(
        "pgproxy: cluster %s listening on port %d -> %s:%d",
        cluster_id, listen_port, backend_host, backend_port,
    )


async def stop_proxy(cluster_id):
    server = _proxies.pop(cluster_id, None)
    if server is not None:
        server.close()
        await server.wait_closed()
    _jobs.pop(cluster_id, None)


async def stop_all_proxies():
    for cluster_id in list(_proxies):
        await stop_proxy(cluster_id)
