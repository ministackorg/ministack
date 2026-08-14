"""
Asyncio Postgres wire-protocol proxy with Aurora DSQL validation.

One proxy per DSQL cluster: clients connect to the proxy on the cluster's
localhost port; the proxy forwards frames to a real Postgres backend after
enforcing DSQL's PostgreSQL-compatibility subset.

Both wire protocols are validated: the simple ``Q`` protocol (psql, psycopg2)
and the extended ``Parse``/``Bind``/``Execute`` protocol (pgjdbc, pgx, asyncpg,
psycopg3, and every ORM that uses prepared statements).

Transaction state is taken from the backend's ``ReadyForQuery`` status byte
rather than inferred from statement text, so it stays correct no matter which
protocol a client uses to send ``BEGIN``/``COMMIT``.

Documented limitations:
- ``CREATE INDEX ASYNC`` / ``ALTER TABLE ASYNC`` executed over the extended
  protocol run their rewritten DDL at ``Execute`` time on the proxy's own
  backend connection; a client that ``Parse``s such a statement and never
  ``Execute``s it registers no job.
- Type/name parsing is heuristic regex work, not a real SQL parser. Statement
  splitting, comment stripping and parenthesis matching are lexer-aware
  (string literals, dollar quotes, quoted identifiers and nested comments),
  but clause-level analysis is still pattern matching.
- Transaction row counting is static (``VALUES`` tuples only); ``INSERT ...
  SELECT`` / ``UPDATE`` / ``DELETE`` affected-row counts are not tracked.
- OC001 emulation is optimistic: the catalog version bumps on forwarded DDL,
  not on successful commit.
"""

import asyncio
import logging
import re
import secrets
import string
import struct
import time
from datetime import datetime, timezone

logger = logging.getLogger("pgproxy")


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
# SQL lexing primitives
#
# Everything that scans SQL text (statement splitting, comment stripping,
# parenthesis matching) goes through _skip_noise so that string literals,
# dollar-quoted bodies, quoted identifiers and comments can never be mistaken
# for structure. Without this a ';' or ')' inside a literal silently truncates
# the text a validation rule sees, and the rule passes on the fragment.
# ---------------------------------------------------------------------------

_DOLLAR_TAG_RE = re.compile(r"\$(?:[A-Za-z_]\w*)?\$")


def _skip_noise(sql, i):
    """If ``sql[i]`` opens a literal/identifier/comment, return the index just
    past it; otherwise return ``i`` unchanged."""
    n = len(sql)
    ch = sql[i]
    if ch == "'":
        # E'...' uses backslash escapes; ordinary literals only double the quote
        # (standard_conforming_strings is on).
        escapes = i > 0 and sql[i - 1] in "Ee" and (
            i == 1 or not (sql[i - 2].isalnum() or sql[i - 2] == "_")
        )
        j = i + 1
        while j < n:
            if escapes and sql[j] == "\\":
                j += 2
                continue
            if sql[j] == "'":
                if j + 1 < n and sql[j + 1] == "'":
                    j += 2
                    continue
                return j + 1
            j += 1
        return n
    if ch == '"':
        j = i + 1
        while j < n:
            if sql[j] == '"':
                if j + 1 < n and sql[j + 1] == '"':
                    j += 2
                    continue
                return j + 1
            j += 1
        return n
    if ch == "$":
        m = _DOLLAR_TAG_RE.match(sql, i)
        if m:
            tag = m.group(0)
            close = sql.find(tag, m.end())
            return close + len(tag) if close >= 0 else n
        return i
    if sql.startswith("--", i):
        nl = sql.find("\n", i)
        return n if nl < 0 else nl + 1
    if sql.startswith("/*", i):
        depth, j = 0, i
        while j < n:
            if sql.startswith("/*", j):
                depth += 1
                j += 2
            elif sql.startswith("*/", j):
                depth -= 1
                j += 2
                if depth == 0:
                    return j
            else:
                j += 1
        return n
    return i


def strip_leading_comments(sql):
    """Drop leading whitespace and comments.

    Every validation rule anchors on the first keyword, so a leading ``--`` or
    ``/* */`` comment would otherwise disable the validator wholesale. Migration
    tools routinely prefix their SQL with comments.
    """
    i, n = 0, len(sql)
    while i < n:
        if sql[i].isspace():
            i += 1
            continue
        j = _skip_noise(sql, i) if sql.startswith(("--", "/*"), i) else i
        if j == i:
            break
        i = j
    return sql[i:]


def _match_paren(sql, start):
    """Index of the ``)`` matching the ``(`` at ``start``, or -1."""
    depth, i, n = 0, start, len(sql)
    while i < n:
        j = _skip_noise(sql, i)
        if j != i:
            i = j
            continue
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _find_char(sql, i, chars):
    """Index of the next character in ``chars`` outside literals/comments."""
    n = len(sql)
    while i < n:
        j = _skip_noise(sql, i)
        if j != i:
            i = j
            continue
        if sql[i] in chars:
            return i
        i += 1
    return -1


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
    """Per-connection transaction discipline tracking.

    ``in_txn`` is authoritative from the backend's ReadyForQuery status byte
    (see ``note_backend_status``) — inferring it from statement text alone goes
    wrong the moment a client sends BEGIN or COMMIT over the extended protocol,
    and a stranded ``in_txn`` makes the validator reject perfectly valid
    autocommit statements. The per-transaction counters below still come from
    statement text, since the wire protocol does not report them.
    """

    def __init__(self):
        self.in_txn = False
        self.ddl_seen = False
        self.dml_seen = False
        self.started_at = None  # monotonic ts at BEGIN (duration limit)
        self.rows_est = 0  # static row estimate (VALUES tuples) in this txn
        self.bytes_est = 0  # cumulative DML payload bytes in this txn
        self.aborted = False  # backend reported a failed transaction block
        self.synthetic_abort = False  # *we* rejected a statement mid-transaction

    def copy(self):
        other = TxnState()
        other.in_txn = self.in_txn
        other.ddl_seen = self.ddl_seen
        other.dml_seen = self.dml_seen
        other.started_at = self.started_at
        other.rows_est = self.rows_est
        other.bytes_est = self.bytes_est
        other.aborted = self.aborted
        other.synthetic_abort = self.synthetic_abort
        return other

    def begin(self):
        self.in_txn = True
        self.ddl_seen = self.dml_seen = False
        self.started_at = time.monotonic()
        self.rows_est = self.bytes_est = 0
        self.aborted = self.synthetic_abort = False

    def reset(self):
        self.in_txn = False
        self.ddl_seen = self.dml_seen = False
        self.started_at = None
        self.rows_est = self.bytes_est = 0
        self.aborted = self.synthetic_abort = False

    def note_backend_status(self, status):
        """Reconcile with a backend ReadyForQuery status byte (I / T / E)."""
        if status == b"I":
            self.reset()
        elif status == b"T":
            if not self.in_txn:
                self.begin()
            self.aborted = False
        elif status == b"E":
            self.in_txn = True
            self.aborted = True

    def apply(self, sql):
        cls = classify_statement(sql)
        if cls == "tcl_begin":
            self.begin()
        elif cls == "tcl_end":
            self.reset()
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


def _cte_main_keyword(sql):
    """Leading keyword of the statement a ``WITH`` clause feeds.

    ``WITH x AS (...) INSERT INTO ...`` is an INSERT, not an unclassified
    statement — otherwise CTE-leading writes escape every DML rule.
    """
    m = re.match(r"\s*WITH\s+(?:RECURSIVE\s+)?", sql, re.I)
    if not m:
        return None
    i = m.end()
    while True:
        opening = _find_char(sql, i, "(")
        if opening < 0:
            return None
        close = _match_paren(sql, opening)
        if close < 0:
            return None
        tail = sql[close + 1:].lstrip()
        if tail.startswith(","):
            i = close + 1
            continue
        km = re.match(r"([A-Za-z]+)", tail)
        if not km:
            return None
        word = km.group(1).upper()
        # Column list or AS [NOT] MATERIALIZED — the body is still ahead.
        if word in ("AS", "NOT", "MATERIALIZED"):
            i = close + 1
            continue
        return word


def _explain_inner(sql):
    """Statement executed by ``EXPLAIN ANALYZE``, or None.

    Plain EXPLAIN only plans, so it stays unclassified; EXPLAIN ANALYZE really
    does run the underlying DML.
    """
    m = re.match(r"\s*EXPLAIN\s*", sql, re.I)
    if not m:
        return None
    i, analyze = m.end(), False
    if i < len(sql) and sql[i] == "(":
        close = _match_paren(sql, i)
        if close < 0:
            return None
        opts = sql[i + 1:close]
        analyze = bool(
            re.search(r"\bANALYZ?[ES]E?\b(?!\s+(?:false|off|0))", opts, re.I)
        )
        i = close + 1
    else:
        while True:
            wm = re.match(r"\s*(ANALYZE|ANALYSE|VERBOSE)\b", sql[i:], re.I)
            if not wm:
                break
            if wm.group(1).upper() in ("ANALYZE", "ANALYSE"):
                analyze = True
            i += wm.end()
    return sql[i:] if analyze else None


def classify_statement(sql):
    """Classify one statement: ddl | dml | tcl_begin | tcl_end | other."""
    s = strip_leading_comments(sql)
    m = re.match(r"\s*([A-Za-z]+)", s)
    if not m:
        return "other"
    kw = m.group(1).upper()
    if kw == "WITH":
        kw = _cte_main_keyword(s) or kw
    elif kw == "EXPLAIN":
        inner = _explain_inner(s)
        return classify_statement(inner) if inner is not None else "other"
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
    """Split a ``Q`` batch on ';' outside literals, comments and quoted names."""
    out, last, i, n = [], 0, 0, len(sql)
    while i < n:
        j = _skip_noise(sql, i)
        if j != i:
            i = j
            continue
        if sql[i] == ";":
            out.append(sql[last:i])
            last = i + 1
        i += 1
    out.append(sql[last:])
    return [s.strip() for s in out if s.strip()]


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
    (r"CREATE\s+RULE\b", "CREATE RULE"),
    (r"CREATE\s+(OR\s+REPLACE\s+)?AGGREGATE\b", "CREATE AGGREGATE"),
    (r"CREATE\s+DOMAIN\b", "CREATE DOMAIN"),
    (r"CREATE\s+CAST\b", "CREATE CAST"),
    (r"CREATE\s+COLLATION\b", "CREATE COLLATION"),
    (r"CREATE\s+(OR\s+REPLACE\s+)?OPERATOR\b", "CREATE OPERATOR"),
    (r"CREATE\s+PUBLICATION\b", "CREATE PUBLICATION"),
    (r"CREATE\s+SUBSCRIPTION\b", "CREATE SUBSCRIPTION"),
    (r"CREATE\s+(FOREIGN\s+TABLE|SERVER|FOREIGN\s+DATA\s+WRAPPER)\b",
     "foreign data wrappers"),
    (r"VACUUM\b", "VACUUM"),
    (r"CLUSTER\b", "CLUSTER"),
    (r"REINDEX\b", "REINDEX"),
    (r"ALTER\s+SYSTEM\b", "ALTER SYSTEM"),
    (r"PREPARE\s+TRANSACTION\b", "prepared transactions"),
    (r"(COMMIT|ROLLBACK)\s+PREPARED\b", "prepared transactions"),
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
    parts, depth, last, i, n = [], 0, 0, 0, len(body)
    while i < n:
        j = _skip_noise(body, i)
        if j != i:
            i = j
            continue
        ch = body[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(body[last:i])
            last = i + 1
        i += 1
    if body[last:]:
        parts.append(body[last:])
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
        end = _match_paren(sql, start)
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
# DEFAULT/NOT NULL — handled in _check_denylist), DROP COLUMN (one or more
# columns, but not a primary key column — needs a catalog probe, so it lives in
# _check_drop_column rather than here), SET/DROP
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


# DSQL index rules, verified against a live Aurora DSQL cluster (Aug 2026):
# index creation is always asynchronous — plain CREATE INDEX fails with
# "unsupported mode". No CONCURRENTLY, no USING (not even btree), no partial
# (WHERE). IF NOT EXISTS requires a name (grammar). Expression index keys are
# supported, but every function must be immutable (42P17) and INCLUDE columns
# can't be expressions. At most 8 key columns per index (54011); 24 indexes
# per table is enforced at runtime.
#
# Error precedence (observed on real DSQL): name-grammar → CONCURRENTLY →
# USING → WHERE → mode → key-expression rules → key count.
_VOLATILE_FUNCTIONS = frozenset({
    "now", "random", "setseed", "nextval", "currval", "lastval", "setval",
    "gen_random_uuid", "uuid_generate_v1", "uuid_generate_v4",
    "txid_current", "pg_backend_pid", "pg_notification_queue_usage",
    "current_setting", "set_config", "version", "clock_timestamp",
    "statement_timestamp", "transaction_timestamp", "timeofday",
})


def _check_index_rules(sql):
    m = _INDEX_RE.match(sql) or _INDEX_ASYNC_RE.match(sql)
    if not m:
        return None
    # IF NOT EXISTS without a name is a grammar error on real DSQL.
    if re.search(r"\bIF\s+NOT\s+EXISTS\b", sql, re.I) and not m.group(1):
        return DsqlError("42601", 'syntax error at or near "ON"')
    if re.search(r"\bCONCURRENTLY\b", sql, re.I):
        return DsqlError("0A000", "CONCURRENTLY not supported for CREATE INDEX")
    if re.search(r"\bUSING\s+\w+", sql, re.I):
        return DsqlError("0A000", "USING not supported for CREATE INDEX")
    if re.search(r"\bWHERE\b", sql, re.I):
        return DsqlError("0A000", "WHERE not supported for CREATE INDEX")
    if not _INDEX_ASYNC_RE.match(sql):
        return DsqlError(
            "0A000", "unsupported mode. please use CREATE INDEX ASYNC."
        )
    start = sql.find("(", m.end())
    if start < 0:
        return None
    end = _match_paren(sql, start)
    if end < 0:
        return None
    parts = _split_top_level(sql[start + 1 : end])
    for part in parts:
        for fn in re.findall(r"(\w+)\s*\(", part):
            if fn.lower() in _VOLATILE_FUNCTIONS:
                # Same code/message the backend (and real DSQL) produce; the
                # proxy fails fast so CREATE INDEX ASYNC rejects at submit
                # time instead of returning a job_id.
                return DsqlError(
                    "42P17",
                    "functions in index expression must be marked IMMUTABLE",
                )
    # INCLUDE columns are non-key: expressions are not supported there.
    inc = re.search(r"\bINCLUDE\s*\(", sql[end:], re.I)
    if inc:
        istart = end + inc.end() - 1
        iend = _match_paren(sql, istart)
        if iend > 0 and any(
            "(" in p for p in _split_top_level(sql[istart + 1 : iend])
        ):
            return DsqlError(
                "0A000", "expressions are not supported in included columns"
            )
    if len(parts) > 8:
        return DsqlError(
            "54011", "more than 8 column keys in an index are not supported"
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
        end = _match_paren(sql, start)
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
    s = strip_leading_comments(sql).strip()
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
        # Extended-protocol state, reset at each Sync.
        self.ext_skip = False  # dropping messages until Sync after an error
        self.ext_forwarded = False  # any message reached the backend this cycle
        self.ext_stmts = {}  # prepared statement name -> {"sql", "synth"}
        self.ext_portals = {}  # portal name -> the same entry


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
    """Forward backend frames to the client (or to the capture queue).

    ReadyForQuery carries the backend's own view of the transaction block, so
    it is the authoritative source for ``in_txn`` regardless of which protocol
    the client used to open or close the transaction.
    """
    while True:
        type_byte, payload = await _read_frame(b_reader)
        if type_byte == b"Z" and payload:
            conn.txn.note_backend_status(payload[:1])
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


def _status(conn):
    """ReadyForQuery status byte for the client's current transaction block."""
    if conn.txn.aborted or conn.txn.synthetic_abort:
        return b"E"
    return b"T" if conn.txn.in_txn else b"I"


def _reject(conn, c_writer, err):
    """Send an ErrorResponse for a statement the backend never saw.

    Inside an explicit transaction this has to poison the block the way a real
    error would: Postgres (and DSQL) refuse every later statement with 25P02
    until the transaction ends. The backend is still in a clean transaction, so
    the proxy tracks the abort itself.
    """
    if conn.txn.in_txn:
        conn.txn.synthetic_abort = True
    c_writer.write(_error_response(err.sqlstate, err.message) + _ready(_status(conn)))


_ABORTED_ERR = DsqlError(
    "25P02",
    "current transaction is aborted, commands ignored until end of "
    "transaction block",
)


def _abort_gate(conn, sql):
    """After a synthetic abort, only the statement that ends the block runs.

    Returns (error, forward_sql). ``forward_sql`` replaces COMMIT with ROLLBACK
    so the aborted work is discarded and the client sees the ``ROLLBACK``
    command tag, exactly as Postgres reports a committed-but-failed block.
    """
    if not conn.txn.synthetic_abort:
        return None, sql
    if classify_statement(sql) != "tcl_end":
        return _ABORTED_ERR, sql
    conn.txn.synthetic_abort = False
    if re.match(r"\s*(COMMIT|END)\b", strip_leading_comments(sql), re.I):
        return None, "ROLLBACK"
    return None, sql


async def _primary_key_columns(conn, b_writer, table):
    """Primary key column names for ``table``; None if the probe failed."""
    try:
        frames = await _run_backend_capture(
            conn,
            b_writer,
            "SELECT a.attname FROM pg_index i JOIN pg_attribute a "
            "ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            f"WHERE i.indrelid = '{table}'::regclass AND i.indisprimary",
        )
    except Exception:
        return None
    cols = []
    for type_byte, payload in frames:
        if type_byte == b"E":
            return None
        if type_byte == b"D":
            (nfields,) = struct.unpack("!H", payload[:2])
            if nfields == 1:
                (vlen,) = struct.unpack("!i", payload[2:6])
                cols.append(payload[6 : 6 + vlen].decode("utf-8", "replace"))
    return cols


# ALTER TABLE ... DROP COLUMN is supported (multiple columns in one statement
# are fine), but dropping a primary key column is not. The COLUMN keyword is
# optional in Postgres, so the action keywords that merely start with DROP —
# DROP CONSTRAINT / DEFAULT / NOT NULL / EXPRESSION / IDENTITY — have to be
# told apart from a bare column name.
_DROP_COLUMN_RE = re.compile(
    r"\bDROP\s+(?:COLUMN\s+)?(?:IF\s+EXISTS\s+)?([\w\".]+)", re.I
)
_DROP_NON_COLUMN_ACTIONS = frozenset(
    ("constraint", "default", "not", "expression", "identity")
)
_ALTER_TABLE_TARGET_RE = re.compile(
    r"\s*ALTER\s+TABLE\s+(?:ASYNC\s+)?(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?([\w\".]+)", re.I
)


def dropped_columns(sql):
    """Column names an ALTER TABLE statement drops (may be several)."""
    if not re.match(r"\s*ALTER\s+TABLE\b", sql, re.I):
        return []
    cols = []
    for m in _DROP_COLUMN_RE.finditer(sql):
        raw = m.group(1)
        quoted = raw.startswith('"')
        name = raw.strip('"')
        if not quoted and name.lower() in _DROP_NON_COLUMN_ACTIONS:
            continue
        cols.append(name)
    return cols


async def _check_drop_column(conn, b_writer, sql):
    """Refuse to drop a primary key column (AWS DSQL, 2026-08-03)."""
    cols = dropped_columns(sql)
    if not cols:
        return None
    m = _ALTER_TABLE_TARGET_RE.match(sql)
    if not m:
        return None
    pk_cols = await _primary_key_columns(conn, b_writer, m.group(1).strip('"'))
    if not pk_cols:
        return None  # probe failed or no primary key — let the backend answer
    lowered = {c.lower() for c in pk_cols}
    for col in cols:
        if col.lower() in lowered:
            # Exact message from real DSQL (verified in eu-west-2, Aug 2026).
            return DsqlError("0A000", f"cannot drop primary key column {col}")
    return None


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
    pk_cols = await _primary_key_columns(conn, b_writer, table)
    if pk_cols is None:
        return None  # probe failed — let the backend answer
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


_ASYNC_BATCH_ERR = DsqlError(
    "0A000", "asynchronous DDL is not supported in multi-statement queries"
)


async def _plan_statement(conn, s, b_writer, allow_probe=True):
    """Decide what to do with one statement, for either wire protocol.

    Returns ``(kind, payload)`` where kind is one of:
      "error"    — payload is a DsqlError to report
      "rows"     — payload describes a synthetic result set (sys.jobs etc.)
      "rewrite"  — payload is a Rewrite to run on the backend as a job
      "forward"  — payload is None; relay the statement untouched

    ``allow_probe`` is False when the backend is mid extended-protocol
    sequence, where injecting a probe query would break the protocol. The
    text-only rules still apply; only the rules that need to look at backend
    state are skipped.
    """
    matched = match_sys_jobs(s)
    if matched is not False:
        columns, job_filter = matched
        cols = list(columns) if columns else list(_JOB_COLUMNS)
        bad = [c for c in cols if c not in _JOB_COLUMNS]
        if bad:
            return "error", DsqlError("42703", f'column "{bad[0]}" does not exist')
        jobs = get_jobs(conn.cluster_id)
        if job_filter:
            jobs = [j for j in jobs if j["job_id"] == job_filter]
        return "rows", {
            "cols": cols,
            "rows": [[job[c] for c in cols] for job in jobs],
            "tag": f"SELECT {len(jobs)}",
        }

    job_id = match_wait_for_job(s)
    if job_id is not None:
        known = any(
            j["job_id"] == job_id and j["status"] == "completed"
            for j in get_jobs(conn.cluster_id)
        )
        return "rows", {
            "cols": [("wait_for_job", 16)],  # bool oid
            "rows": [["t" if known else "f"]],
            "tag": "SELECT 1",
        }

    result = validate(s, conn.txn)
    if isinstance(result, DsqlError):
        return "error", result
    if isinstance(result, Rewrite) and result.kind == "index_async":
        if not allow_probe:
            return "error", _ASYNC_BATCH_ERR
        if result.job_type == "INDEX_BUILD" and result.table:
            count = await _index_count(conn, b_writer, result.table)
            if count is not None and count >= 24:
                return "error", DsqlError(
                    "0A000",
                    f"table {result.table} already has the maximum of 24 indexes",
                )
        return "rewrite", result

    if allow_probe:
        err = await _check_drop_column(conn, b_writer, s)
        if err:
            return "error", err

    if _FOR_UPDATE_RE.search(s) and allow_probe:
        err = await _check_for_update(conn, b_writer, s)
        if err:
            return "error", err
    return "forward", None


def _note_catalog(conn, s):
    if classify_statement(s) == "ddl":
        conn.catalog_version = _bump_catalog(conn.cluster_id)
    else:
        conn.catalog_version = _catalog_versions.get(conn.cluster_id, 0)


def _stale_catalog(conn):
    """OC001: this connection's cached catalog is behind another session's DDL."""
    return (
        conn.txn.in_txn
        and conn.catalog_version is not None
        and conn.catalog_version < _catalog_versions.get(conn.cluster_id, 0)
    )


_OC001_ERR = DsqlError(
    "40001", "schema has been updated by another transaction (OC001)"
)


async def _rollback_stale(conn, b_writer):
    await _run_backend_capture(conn, b_writer, "ROLLBACK")
    conn.txn.apply("ROLLBACK")
    conn.catalog_version = _catalog_versions.get(conn.cluster_id, 0)


def _synth_columns(kind, result):
    return result["cols"] if kind == "rows" else ["job_id"]


async def _run_rewrite(conn, b_writer, rewrite):
    """Execute a rewritten ASYNC statement; return (job, backend_error)."""
    frames = await _run_backend_capture(conn, b_writer, rewrite.sql)
    backend_err = next((p for t, p in frames if t == b"E"), None)
    if backend_err is not None:
        return None, backend_err
    conn.catalog_version = _bump_catalog(conn.cluster_id)
    job = _register_job(conn.cluster_id, rewrite.object_name, rewrite.job_type)
    return job, None


async def _handle_query(conn, sql, b_writer, c_writer):
    txn = conn.txn
    stmts = split_statements(sql)

    if len(stmts) == 1:
        s = strip_leading_comments(stmts[0])

        # A statement rejected earlier in this transaction poisoned the block.
        err, forward_sql = _abort_gate(conn, s)
        if err:
            c_writer.write(
                _error_response(err.sqlstate, err.message) + _ready(_status(conn))
            )
            await c_writer.drain()
            return
        if forward_sql != s:  # COMMIT of an aborted block -> ROLLBACK
            txn.apply(forward_sql)
            b_writer.write(_frame(b"Q", forward_sql.encode() + b"\0"))
            await b_writer.drain()
            return

        # OC001: abort like real DSQL (40001); the next attempt sees the fresh
        # catalog and succeeds.
        if _stale_catalog(conn):
            await _rollback_stale(conn, b_writer)
            _reject(conn, c_writer, _OC001_ERR)
            await c_writer.drain()
            return

        kind, result = await _plan_statement(conn, s, b_writer)
        if kind == "error":
            _reject(conn, c_writer, result)
            await c_writer.drain()
            return
        if kind == "rows":
            out = _row_description(result["cols"])
            for row in result["rows"]:
                out += _data_row(row)
            out += _command_complete(result["tag"])
            c_writer.write(out + _ready(_status(conn)))
            await c_writer.drain()
            return
        if kind == "rewrite":
            txn.apply(s)
            # Run the rewritten statement on the backend, swallow its
            # response, and synthesize the DSQL job_id result set.
            job, backend_err = await _run_rewrite(conn, b_writer, result)
            if backend_err is not None:
                c_writer.write(_frame(b"E", backend_err) + _ready(_status(conn)))
                await c_writer.drain()
                return
            out = _row_description(["job_id"])
            out += _data_row([job["job_id"]])
            out += _command_complete("SELECT 1")
            c_writer.write(out + _ready(_status(conn)))
            await c_writer.drain()
            return

        _note_catalog(conn, s)
        txn.apply(s)
        b_writer.write(_frame(b"Q", sql.encode() + b"\0"))
        await b_writer.drain()
        return

    # Multi-statement batch (implicit transaction): validate every statement
    # up front, then forward the batch verbatim.
    if stmts:
        # An aborted block only accepts an explicit rollback; forwarding a
        # batch that opened with COMMIT would commit work the client was told
        # had failed.
        if conn.txn.synthetic_abort:
            if re.match(
                r"\s*(ROLLBACK|ABORT)\b", strip_leading_comments(stmts[0]), re.I
            ):
                conn.txn.synthetic_abort = False
            else:
                c_writer.write(
                    _error_response(_ABORTED_ERR.sqlstate, _ABORTED_ERR.message)
                    + _ready(_status(conn))
                )
                await c_writer.drain()
                return
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


# ---------------------------------------------------------------------------
# Extended query protocol (Parse / Bind / Describe / Execute / Close / Sync)
#
# pgjdbc, pgx, asyncpg, psycopg3 and every ORM built on prepared statements
# send SQL this way. Validating only the simple 'Q' protocol would let all of
# them past the DSQL subset entirely.
# ---------------------------------------------------------------------------


def _two_strings(payload):
    """The two leading NUL-terminated strings of a Parse or Bind payload."""
    first = payload.index(b"\0")
    second = payload.index(b"\0", first + 1)
    return (
        payload[:first].decode("utf-8", "replace"),
        payload[first + 1 : second].decode("utf-8", "replace"),
    )


def _target_name(payload):
    """('S'|'P', name) of a Describe or Close payload."""
    return payload[:1], payload[1:].split(b"\0")[0].decode("utf-8", "replace")


def _parse_payload(name, sql):
    return name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0)


async def _ext_forward(conn, b_writer, type_byte, payload):
    conn.ext_forwarded = True
    b_writer.write(_frame(type_byte, payload))
    await b_writer.drain()


async def _ext_error(conn, c_writer, err):
    """Report an error and enter skip-until-Sync, per the protocol spec."""
    if conn.txn.in_txn:
        conn.txn.synthetic_abort = True
    conn.ext_skip = True
    c_writer.write(_error_response(err.sqlstate, err.message))
    await c_writer.drain()


async def _ext_parse(conn, payload, b_writer, c_writer):
    try:
        name, sql = _two_strings(payload)
    except ValueError:
        await _ext_forward(conn, b_writer, b"P", payload)
        return
    s = strip_leading_comments(sql).strip()
    entry = {"sql": s, "synth": None}
    if not s:
        conn.ext_stmts[name] = entry
        await _ext_forward(conn, b_writer, b"P", payload)
        return

    err, forward_sql = _abort_gate(conn, s)
    if err:
        await _ext_error(conn, c_writer, err)
        return
    if forward_sql != s:  # COMMIT of an aborted block -> ROLLBACK
        entry["sql"] = forward_sql
        conn.ext_stmts[name] = entry
        await _ext_forward(conn, b_writer, b"P", _parse_payload(name, forward_sql))
        return

    # Probes and rewrites need the backend to be idle; it is not once anything
    # in this pipelined batch has been forwarded.
    allow_probe = not conn.ext_forwarded
    if allow_probe and _stale_catalog(conn):
        await _rollback_stale(conn, b_writer)
        await _ext_error(conn, c_writer, _OC001_ERR)
        return

    kind, result = await _plan_statement(conn, s, b_writer, allow_probe=allow_probe)
    if kind == "error":
        await _ext_error(conn, c_writer, result)
        return
    if kind in ("rows", "rewrite"):
        entry["synth"] = (kind, result)
        conn.ext_stmts[name] = entry
        c_writer.write(_frame(b"1", b""))  # ParseComplete
        await c_writer.drain()
        return
    conn.ext_stmts[name] = entry
    await _ext_forward(conn, b_writer, b"P", payload)


async def _ext_bind(conn, payload, b_writer, c_writer):
    try:
        portal, stmt = _two_strings(payload)
    except ValueError:
        await _ext_forward(conn, b_writer, b"B", payload)
        return
    entry = conn.ext_stmts.get(stmt)
    if entry is None:
        await _ext_forward(conn, b_writer, b"B", payload)
        return
    conn.ext_portals[portal] = entry
    if entry["synth"] is None:
        await _ext_forward(conn, b_writer, b"B", payload)
        return
    c_writer.write(_frame(b"2", b""))  # BindComplete
    await c_writer.drain()


async def _ext_describe(conn, payload, b_writer, c_writer):
    target, name = _target_name(payload)
    store = conn.ext_stmts if target == b"S" else conn.ext_portals
    entry = store.get(name)
    if entry is None or entry["synth"] is None:
        await _ext_forward(conn, b_writer, b"D", payload)
        return
    out = b""
    if target == b"S":
        out += _frame(b"t", struct.pack("!H", 0))  # ParameterDescription
    out += _row_description(_synth_columns(*entry["synth"]))
    c_writer.write(out)
    await c_writer.drain()


async def _ext_execute(conn, payload, b_writer, c_writer):
    name = payload.split(b"\0")[0].decode("utf-8", "replace")
    entry = conn.ext_portals.get(name)
    if entry is None or entry["synth"] is None:
        if entry is not None and entry["sql"]:
            _note_catalog(conn, entry["sql"])
            conn.txn.apply(entry["sql"])
        await _ext_forward(conn, b_writer, b"E", payload)
        return

    kind, result = entry["synth"]
    if kind == "rows":
        out = b""
        for row in result["rows"]:
            out += _data_row(row)
        c_writer.write(out + _command_complete(result["tag"]))
        await c_writer.drain()
        return

    conn.txn.apply(entry["sql"])
    job, backend_err = await _run_rewrite(conn, b_writer, result)
    if backend_err is not None:
        conn.ext_skip = True
        c_writer.write(_frame(b"E", backend_err))
        await c_writer.drain()
        return
    c_writer.write(_data_row([job["job_id"]]) + _command_complete("SELECT 1"))
    await c_writer.drain()


async def _ext_close(conn, payload, b_writer, c_writer):
    target, name = _target_name(payload)
    store = conn.ext_stmts if target == b"S" else conn.ext_portals
    entry = store.pop(name, None)
    if entry is None or entry["synth"] is None:
        await _ext_forward(conn, b_writer, b"C", payload)
        return
    c_writer.write(_frame(b"3", b""))  # CloseComplete
    await c_writer.drain()


async def _ext_sync(conn, b_writer, c_writer):
    forwarded = conn.ext_forwarded
    conn.ext_skip = False
    conn.ext_forwarded = False
    conn.ext_portals.pop("", None)  # the unnamed portal dies at Sync
    if forwarded:
        # The backend answers with its own ReadyForQuery, which also refreshes
        # our transaction state via _backend_relay.
        b_writer.write(_frame(b"S", b""))
        await b_writer.drain()
        return
    c_writer.write(_ready(_status(conn)))
    await c_writer.drain()


async def _handle_extended(conn, type_byte, payload, b_writer, c_writer):
    if conn.ext_skip and type_byte != b"S":
        return  # after an error, everything is ignored until Sync
    if type_byte == b"P":
        await _ext_parse(conn, payload, b_writer, c_writer)
    elif type_byte == b"B":
        await _ext_bind(conn, payload, b_writer, c_writer)
    elif type_byte == b"D":
        await _ext_describe(conn, payload, b_writer, c_writer)
    elif type_byte == b"E":
        await _ext_execute(conn, payload, b_writer, c_writer)
    elif type_byte == b"C":
        await _ext_close(conn, payload, b_writer, c_writer)
    elif type_byte == b"S":
        await _ext_sync(conn, b_writer, c_writer)
    elif type_byte == b"H":  # Flush
        if conn.ext_forwarded:
            await _ext_forward(conn, b_writer, b"H", payload)
        else:
            await c_writer.drain()


_EXTENDED_TYPES = frozenset((b"P", b"B", b"D", b"E", b"C", b"S", b"H"))


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
            elif type_byte in _EXTENDED_TYPES:
                await _handle_extended(conn, type_byte, payload, b_writer, c_writer)
            else:
                # Everything else relays verbatim (d/c/f COPY frames, F...).
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
