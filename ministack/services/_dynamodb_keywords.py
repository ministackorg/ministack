# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""DynamoDB reserved words, and the PartiQL statement parser.

AWS_KEYWORDS must be escaped with `#alias` in the expression parameters (AWS DynamoDB Developer Guide).
"""

import re
from decimal import Decimal

AWS_KEYWORDS = frozenset((
"ABORT","ABSOLUTE","ACTION","ADD","AFTER","AGENT","AGGREGATE","ALL","ALLOCATE","ALTER",
"ANALYZE","AND","ANY","ARCHIVE","ARE","ARRAY","AS","ASC","ASCII","ASENSITIVE","ASSERTION",
"ASYMMETRIC","AT","ATOMIC","ATTACH","ATTRIBUTE","AUTH","AUTHORIZATION","AUTHORIZE","AUTO",
"AVG","BACK","BACKUP","BASE","BATCH","BEFORE","BEGIN","BETWEEN","BIGINT","BINARY","BIT",
"BLOB","BLOCK","BOOLEAN","BOTH","BREADTH","BUCKET","BULK","BY","BYTE","CALL","CALLED",
"CALLING","CAPACITY","CASCADE","CASCADED","CASE","CAST","CATALOG","CHAR","CHARACTER",
"CHECK","CLASS","CLOB","CLOSE","CLUSTER","CLUSTERED","CLUSTERING","CLUSTERS","COALESCE",
"COLLATE","COLLATION","COLLECTION","COLUMN","COLUMNS","COMBINE","COMMENT","COMMIT",
"COMPACT","COMPILE","COMPRESS","CONDITION","CONFLICT","CONNECT","CONNECTION","CONSISTENCY",
"CONSISTENT","CONSTRAINT","CONSTRAINTS","CONSTRUCTOR","CONSUMED","CONTINUE","CONVERT",
"COPY","CORRESPONDING","COUNT","COUNTER","CREATE","CROSS","CUBE","CURRENT","CURSOR","CYCLE",
"DATA","DATABASE","DATE","DATETIME","DAY","DEALLOCATE","DEC","DECIMAL","DECLARE","DEFAULT",
"DEFERRABLE","DEFERRED","DEFINE","DEFINED","DEFINITION","DELETE","DELIMITED","DEPTH","DEREF",
"DESC","DESCRIBE","DESCRIPTOR","DETACH","DETERMINISTIC","DIAGNOSTICS","DIRECTORIES",
"DISABLE","DISCONNECT","DISTINCT","DISTRIBUTE","DO","DOMAIN","DOUBLE","DROP","DUMP",
"DURATION","DYNAMIC","EACH","ELEMENT","ELSE","ELSEIF","EMPTY","ENABLE","END","EQUAL",
"EQUALS","ERROR","ESCAPE","ESCAPED","EVAL","EVALUATE","EXCEEDED","EXCEPT","EXCEPTION",
"EXCEPTIONS","EXCLUSIVE","EXEC","EXECUTE","EXISTS","EXIT","EXPLAIN","EXPLODE","EXPORT",
"EXPRESSION","EXTENDED","EXTERNAL","EXTRACT","FAIL","FALSE","FAMILY","FETCH","FIELDS",
"FILE","FILTER","FILTERING","FINAL","FINISH","FIRST","FIXED","FLATTERN","FLOAT","FOR",
"FORCE","FOREIGN","FORMAT","FORWARD","FOUND","FREE","FROM","FULL","FUNCTION","FUNCTIONS",
"GENERAL","GENERATE","GET","GLOB","GLOBAL","GO","GOTO","GRANT","GREATER","GROUP","GROUPING",
"HANDLER","HASH","HAVE","HAVING","HEAP","HIDDEN","HOLD","HOUR","IDENTIFIED","IDENTITY",
"IF","IGNORE","IMMEDIATE","IMPORT","IN","INCLUDING","INCLUSIVE","INCREMENT","INCREMENTAL",
"INDEX","INDEXED","INDEXES","INDICATOR","INFINITE","INITIALLY","INLINE","INNER","INNTER",
"INOUT","INPUT","INSENSITIVE","INSERT","INSTEAD","INT","INTEGER","INTERSECT","INTERVAL",
"INTO","INVALIDATE","IS","ISOLATION","ITEM","ITEMS","ITERATE","JOIN","KEY","KEYS","LAG",
"LANGUAGE","LARGE","LAST","LATERAL","LEAD","LEADING","LEAVE","LEFT","LENGTH","LESS","LEVEL",
"LIKE","LIMIT","LIMITED","LINES","LIST","LOAD","LOCAL","LOCALTIME","LOCALTIMESTAMP",
"LOCATION","LOCATOR","LOCK","LOCKS","LOG","LOGED","LONG","LOOP","LOWER","MAP","MATCH",
"MATERIALIZED","MAX","MAXLEN","MEMBER","MERGE","METHOD","METRICS","MIN","MINUS","MINUTE",
"MISSING","MOD","MODE","MODIFIES","MODIFY","MODULE","MONTH","MULTI","MULTISET","NAME",
"NAMES","NATIONAL","NATURAL","NCHAR","NCLOB","NEW","NEXT","NO","NONE","NOT","NULL","NULLIF",
"NUMBER","NUMERIC","OBJECT","OF","OFFLINE","OFFSET","OLD","ON","ONLINE","ONLY","OPAQUE",
"OPEN","OPERATOR","OPTION","OR","ORDER","ORDINALITY","OTHER","OTHERS","OUT","OUTER",
"OUTPUT","OVER","OVERLAPS","OVERRIDE","OWNER","PAD","PARALLEL","PARAMETER","PARAMETERS",
"PARTIAL","PARTITION","PARTITIONED","PARTITIONS","PATH","PERCENT","PERCENTILE","PERMISSION",
"PERMISSIONS","PIPE","PIPELINED","PLAN","POOL","POSITION","PRECISION","PREPARE","PRESERVE",
"PRIMARY","PRIOR","PRIVATE","PRIVILEGES","PROCEDURE","PROCESSED","PROJECT","PROJECTION",
"PROPERTY","PROVISIONING","PUBLIC","PUT","QUERY","QUIT","QUORUM","RAISE","RANDOM","RANGE",
"RANK","RAW","READ","READS","REAL","REBUILD","RECORD","RECURSIVE","REDUCE","REF","REFERENCE",
"REFERENCES","REFERENCING","REGEXP","REGION","REINDEX","RELATIVE","RELEASE","REMAINDER",
"RENAME","REPEAT","REPLACE","REQUEST","RESET","RESIGNAL","RESOURCE","RESPONSE","RESTORE",
"RESTRICT","RESULT","RETURN","RETURNING","RETURNS","REVERSE","REVOKE","RIGHT","ROLE",
"ROLES","ROLLBACK","ROLLUP","ROUTINE","ROW","ROWS","RULE","RULES","SAMPLE","SATISFIES",
"SAVE","SAVEPOINT","SCAN","SCHEMA","SCOPE","SCROLL","SEARCH","SECOND","SECTION","SEGMENT",
"SEGMENTS","SELECT","SELF","SEMI","SENSITIVE","SEPARATE","SEQUENCE","SERIALIZABLE","SESSION",
"SET","SETS","SHARD","SHARE","SHARED","SHORT","SHOW","SIGNAL","SIMILAR","SIZE","SKEWED",
"SMALLINT","SNAPSHOT","SOME","SOURCE","SPACE","SPACES","SPARSE","SPECIFIC","SPECIFICTYPE",
"SPLIT","SQL","SQLCODE","SQLERROR","SQLEXCEPTION","SQLSTATE","SQLWARNING","START","STATE",
"STATIC","STATUS","STORAGE","STORE","STORED","STREAM","STRING","STRUCT","STYLE","SUB",
"SUBMULTISET","SUBPARTITION","SUBSTRING","SUBTYPE","SUM","SUPER","SYMMETRIC","SYNONYM",
"SYSTEM","TABLE","TABLESAMPLE","TEMP","TEMPORARY","TERMINATED","TEXT","THAN","THEN",
"THROUGHPUT","TIME","TIMESTAMP","TIMEZONE","TINYINT","TO","TOKEN","TOTAL","TOUCH",
"TRAILING","TRANSACTION","TRANSFORM","TRANSLATE","TRANSLATION","TREAT","TRIGGER","TRIM",
"TRUE","TRUNCATE","TTL","TUPLE","TYPE","UNDER","UNDO","UNION","UNIQUE","UNIT","UNKNOWN",
"UNLOGGED","UNNEST","UNPROCESSED","UNSIGNED","UNTIL","UPDATE","UPPER","URL","USAGE","USE",
"USER","USERS","USING","UUID","VACUUM","VALUE","VALUED","VALUES","VARCHAR","VARIABLE",
"VARIANCE","VARINT","VARYING","VIEW","VIEWS","VIRTUAL","VOID","WAIT","WHEN","WHENEVER",
"WHERE","WHILE","WINDOW","WITH","WITHIN","WITHOUT","WORK","WRAPPED","WRITE","YEAR","ZONE",
))


# ---------------------------------------------------------------------------
# PartiQL
# ---------------------------------------------------------------------------

NOT_WELL_FORMED = "Statement wasn't well formed, can't be processed: "
_LEFTOVER = "Unexpected token after expression"


class PartiqlError(ValueError):
    """A statement DynamoDB refuses before it runs. ``statement_level`` errors carry the
    ``Validation failed in TransactStatements[i]: `` prefix inside a transaction."""

    def __init__(self, message, statement_level=True):
        super().__init__(message)
        self.statement_level = statement_level


def _malformed(detail=""):
    return PartiqlError(NOT_WELL_FORMED + detail)


_TOKEN_RE = re.compile(r"""
    (?P<ws>\s+)
  | (?P<line>--[^\n]*)
  | (?P<block>/\*.*?\*/)
  | (?P<open>/\*|\#)
  | (?P<str>'(?:[^']|'')*')
  | (?P<qid>"(?:[^"]|"")*")
  | (?P<num>(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)
  | (?P<id>[A-Za-z_][A-Za-z0-9_$]*)
  | (?P<op><<|>>|<=|>=|<>|!=|\|\||[-+*/%=<>(),.\[\]{}:;?])
""", re.VERBOSE | re.DOTALL)
_INT32 = (-(2 ** 31), 2 ** 31 - 1)
_RESERVED = frozenset((
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "BETWEEN", "LIKE", "INSERT", "INTO",
    "UPDATE", "SET", "REMOVE", "DELETE", "RETURNING", "ORDER", "AS", "EXISTS", "INNER", "NULL",
    "MISSING", "TRUE", "FALSE"))
_OPERATOR_WORDS = frozenset(("AND", "OR", "IS", "LIKE", "BETWEEN", "IN"))
_COMPARATORS = frozenset(("=", "<>", "!=", "<", "<=", ">", ">="))
_WHERE_FUNCTIONS = frozenset(("attribute_exists", "attribute_not_exists", "attribute_type",
                              "begins_with", "contains", "size"))
_TYPE_NAMES = {
    "NULL": "NULL", "MISSING": "MISSING", "BOOL": "BOOL", "BOOLEAN": "BOOL",
    "SMALLINT": "N", "INT": "N", "INTEGER": "N", "INT2": "N", "INT4": "N", "INT8": "N", "BIGINT": "N",
    "FLOAT": "N", "REAL": "N", "DECIMAL": "N", "NUMERIC": "N", "DOUBLE PRECISION": "N",
    "STRING": "S", "CHAR": "S", "CHARACTER": "S", "VARCHAR": "S", "CHARACTER VARYING": "S",
    "SYMBOL": "S", "CLOB": "S", "BLOB": "B", "LIST": "L", "SEXP": "L", "STRUCT": "M", "TUPLE": "M",
    "BAG": "BAG",
}
_NUM_LIMITS = (
    "Number overflow. Attempting to store a number with magnitude larger than supported range",
    "Number underflow. Attempting to store a number with magnitude smaller than supported range",
    "Attempting to store more than 38 significant digits in a Number",
)


def _tokenize(text):
    """(kind, value, start, end) tokens; a lone `#` or an unclosed block comment is unreadable."""
    tokens, pos, n = [], 0, len(text)
    while pos < n:
        m = _TOKEN_RE.match(text, pos)
        if not m or m.lastgroup == "open":
            raise _malformed()
        kind = m.lastgroup
        if kind == "num" and text[pos] == "." and tokens and (
                tokens[-1][0] in ("id", "qid") or tokens[-1][1] in ("]", ")")):
            kind, m = "op", re.compile(r"\.").match(text, pos)
        pos = m.end()
        if kind in ("ws", "line", "block"):
            continue
        value = m.group(0)
        if kind == "num":
            _check_number_text(value)
        tokens.append((kind, value, m.start(), m.end()))
    tokens.append(("eof", "", n, n))
    return tokens


def _check_number_text(text):
    mantissa, _, exponent = text.lower().partition("e")
    exp = int(exponent or 0)
    scale = len(mantissa.partition(".")[2]) - exp
    if not _INT32[0] <= exp <= _INT32[1] or not _INT32[0] <= scale <= _INT32[1]:
        raise _malformed()


def number_limit_error(text):
    """Why a number literal breaks DynamoDB's number rules, or None."""
    mantissa, _, exponent = text.lower().lstrip("+-").partition("e")
    integer, _, fraction = mantissa.partition(".")
    digits = (integer + fraction).lstrip("0")
    adjusted = max(len(digits), 1) - (len(fraction) - int(exponent or 0)) - 1
    if digits and adjusted > 125:
        return _NUM_LIMITS[0]
    if digits and adjusted < -130:
        return _NUM_LIMITS[1]
    if len(digits.rstrip("0")) > 38:
        return _NUM_LIMITS[2]
    return None


def canonical_number(text):
    d = Decimal(text)
    return "0" if d == 0 else format(d.normalize(), "f")


def _bag_av(members, source):
    """A set literal: duplicates are reported as written, an empty bag is refused."""
    if not members:
        raise PartiqlError("Empty bags are not supported", statement_level=False)
    kinds = {next(iter(m)) for m in members}
    if len(kinds) != 1 or kinds - {"S", "N", "B"}:
        raise PartiqlError("An operand in the update expression has an incorrect data type", statement_level=False)
    kind = kinds.pop()
    values = [m[kind] for m in members]
    seen = set()
    for v in values:
        key = canonical_number(v) if kind == "N" else v
        if key in seen:
            raise PartiqlError("One or more parameter values were invalid: Input collection "
                               f"[{', '.join(source)}] contains duplicates.", statement_level=False)
        seen.add(key)
    return {kind + "S": values}


class _Parser:
    __slots__ = ("text", "tokens", "pos", "params", "next_param")

    def __init__(self, text, parameters):
        self.text = text
        self.tokens = _tokenize(text)
        self.pos = 0
        self.params = parameters or []
        self.next_param = 0

    # -- tokens ----------------------------------------------------------
    def peek(self, offset=0):
        return self.tokens[min(self.pos + offset, len(self.tokens) - 1)]

    def take(self):
        tok = self.tokens[self.pos]
        self.pos = min(self.pos + 1, len(self.tokens) - 1)
        return tok

    def word(self, tok=None):
        tok = tok or self.peek()
        return tok[1].upper() if tok[0] == "id" else None

    def at_word(self, *words):
        return self.word() in words

    def at_op(self, *ops):
        tok = self.peek()
        return tok[0] == "op" and tok[1] in ops

    def at_end(self):
        return self.peek()[0] == "eof" or self.at_op(";")

    def expect_word(self, word, detail=_LEFTOVER):
        if not self.at_word(word):
            raise _malformed(detail)
        return self.take()

    def expect_op(self, op, detail="Unexpected term"):
        if not self.at_op(op):
            raise _malformed(detail)
        return self.take()

    def position(self, start, length):
        line = self.text.count("\n", 0, start) + 1
        column = start - (self.text.rfind("\n", 0, start) + 1) + 1
        return f"{line}:{column}:{length}"

    def finish(self):
        if self.at_op(";"):
            self.take()
        if self.peek()[0] != "eof":
            raise _malformed(_LEFTOVER)

    # -- statements ------------------------------------------------------
    def statement(self):
        head = self.word()
        if head == "SELECT":
            out = self.select()
        elif head == "INSERT":
            out = self.insert()
        elif head == "UPDATE":
            out = self.update()
        elif head == "DELETE":
            out = self.delete()
        elif head == "FROM":
            out = self.from_first()
        elif head == "EXISTS" and self.peek(1)[1] == "(":
            self.take()
            self.take()
            if not self.at_word("SELECT"):
                raise _malformed("Unexpected term")
            inner = self.select(nested=True)
            self.expect_op(")", _LEFTOVER)
            out = {"op": "EXISTS", "select": inner, "table": inner["table"], "index": inner["index"]}
        else:
            return self.bare_expression()
        self.finish()
        return out

    def bare_expression(self):
        try:
            node = self.expression()
            self.finish()
        except PartiqlError:
            raise _malformed("Expected data manipulation")
        if node[0] == "is" and node[2] not in ("NULL", "MISSING"):
            raise PartiqlError("Unsupported operation: IsType")
        raise _malformed("Expected data manipulation")

    def table_path(self, missing="Unexpected term"):
        parts = []
        while True:
            tok = self.peek()
            if tok[0] == "qid":
                name = tok[1][1:-1].replace('""', '"')
                if not name:
                    raise PartiqlError("Path component cannot be an empty string")
                parts.append((name, True))
            elif tok[0] == "id" and tok[1].upper() not in _RESERVED:
                parts.append((tok[1], False))
            else:
                raise _malformed(missing if not parts else "Unexpected term")
            self.take()
            if not self.at_op("."):
                break
            self.take()
        if len(parts) > 2:
            raise PartiqlError("A path may contain at most 2 components in the FROM clause")
        if len(parts) == 2 and (parts[0][1] or parts[1][1]):
            return parts[0][0], parts[1][0]
        return ".".join(p[0] for p in parts), None

    def refuse_alias(self, leftover=False):
        tok = self.peek()
        if self.at_word("AS") or (tok[0] == "id" and tok[1].upper() not in _RESERVED):
            raise _malformed(_LEFTOVER) if leftover else PartiqlError("Aliasing is not supported")

    def select(self, nested=False):
        self.take()
        projections, count = None, False
        if self.at_op("*"):
            self.take()
        else:
            projections = []
            while True:
                tok = self.peek()
                if tok[0] == "id" and self.peek(1)[1] == "(":
                    if tok[1].upper() == "COUNT" and self.peek(2)[1] == "*" and self.peek(3)[1] == ")":
                        for _ in range(4):
                            self.take()
                        count = True
                    else:
                        raise PartiqlError(f"Unexpected path component at {self.position(tok[2], len(tok[1]))}")
                else:
                    projections.append(self.path()[1])
                if not self.at_op(","):
                    break
                self.take()
        self.expect_word("FROM", "Unexpected term")
        table, index = self.table_path()
        self.refuse_alias()
        where = order = None
        if self.at_word("WHERE"):
            self.take()
            where = self.expression()
        if self.at_word("ORDER") and not nested:
            self.take()
            self.expect_word("BY", "Unexpected term")
            path = self.path()[1]
            desc = False
            if self.at_word("ASC", "DESC"):
                desc = self.take()[1].upper() == "DESC"
            order = (path, desc)
        return {"op": "SELECT", "table": table, "index": index, "projections": projections,
                "count": count, "where": where, "order": order}

    def insert(self):
        self.take()
        self.expect_word("INTO", "Unexpected term")
        table, index = self.table_path()
        if index is not None:
            raise _malformed("FROM clause may only contain a single table name")
        self.expect_word("VALUE")
        tok = self.peek()
        if self.at_op("?"):
            item = self.parameter()
        elif self.at_op("{"):
            node = self.map_or_list()
            if node[0] != "map":
                raise _malformed("Unexpected term")
            item = {key: self.literal_value(value, "root") for key, value in node[1]}
        else:
            raise _malformed("Unexpected term")
        return {"op": "INSERT", "table": table, "index": None, "item": item, "item_token": tok}

    def write_clauses(self):
        assignments, removes = [], []
        while self.at_word("SET", "REMOVE"):
            if self.take()[1].upper() == "SET":
                while True:
                    target = self.path()
                    self.expect_op("=", "Unexpected term")
                    assignments.append((target[1], self.assignment_value(target)))
                    if not self.at_op(","):
                        break
                    self.take()
            else:
                while True:
                    removes.append(self.path()[1])
                    if not self.at_op(","):
                        break
                    self.take()
        return assignments, removes

    def returning(self):
        if not self.at_word("RETURNING"):
            return None
        start = self.take()[3]
        end = len(self.text)
        while self.peek()[0] != "eof" and not self.at_op(";"):
            end = self.take()[3]
        return self.text[start:end].strip()

    def update(self):
        self.take()
        table, index = self.table_path("Expected identifier for simple path")
        if index is not None:
            raise PartiqlError("This operation is not supported on an index")
        self.refuse_alias()
        assignments, removes = self.write_clauses()
        if not assignments and not removes:
            raise _malformed("Unexpected term")
        where = None
        if self.at_word("WHERE"):
            self.take()
            where = self.expression()
        return {"op": "UPDATE", "table": table, "index": None, "assignments": assignments,
                "removes": removes, "where": where, "returning": self.returning(), "from_first": False}

    def from_first(self):
        self.take()
        table, index = self.table_path()
        if index is not None:
            raise PartiqlError("This operation is not supported on an index")
        self.refuse_alias()
        where = None
        if self.at_word("WHERE"):
            self.take()
            where = self.expression()
        assignments, removes = self.write_clauses()
        if not assignments and not removes:
            raise _malformed(_LEFTOVER if not self.at_end() else "Unexpected term")
        return {"op": "UPDATE", "table": table, "index": None, "assignments": assignments,
                "removes": removes, "where": where, "returning": self.returning(), "from_first": True}

    def delete(self):
        self.take()
        self.expect_word("FROM", "Expected identifier for simple path")
        table, index = self.table_path("Expected identifier for simple path")
        if index is not None:
            raise PartiqlError("This operation is not supported on an index")
        self.refuse_alias()
        where = None
        if self.at_word("WHERE"):
            self.take()
            where = self.expression()
        return {"op": "DELETE", "table": table, "index": None, "where": where, "returning": self.returning()}

    # -- values ----------------------------------------------------------
    def parameter(self):
        self.take()
        if self.next_param >= len(self.params):
            raise PartiqlError("Number of parameters in request and statement don't match.", statement_level=False)
        value = self.params[self.next_param]
        self.next_param += 1
        return value

    def path(self):
        """(start, parts, end) of a document path; a keyword after a dot is not a component."""
        tok = self.peek()
        if tok[0] == "qid":
            parts = [tok[1][1:-1].replace('""', '"')]
        elif tok[0] == "id" and tok[1].upper() not in _RESERVED:
            parts = [tok[1]]
        else:
            raise self.term_error()
        start = self.take()[2]
        end = tok[3]
        while self.at_op(".", "["):
            if self.take()[1] == ".":
                part = self.peek()
                if part[0] == "qid":
                    parts.append(part[1][1:-1].replace('""', '"'))
                elif part[0] == "id" and part[1].upper() not in _RESERVED:
                    parts.append(part[1])
                else:
                    raise _malformed("Invalid path dot component")
                end = self.take()[3]
            else:
                part = self.take()
                if part[0] == "num" and part[1].isdigit():
                    parts.append(int(part[1]))
                elif part[0] == "str":
                    parts.append(part[1][1:-1].replace("''", "'"))
                else:
                    raise _malformed("Unexpected term")
                end = self.expect_op("]")[3]
        return start, parts, end

    def term_error(self):
        tok = self.peek()
        word = self.word(tok)
        if tok[0] == "eof" or tok[1] == ";":
            return _malformed("Unexpected term")
        if word in _OPERATOR_WORDS:
            return _malformed("Unexpected operator")
        if word in _RESERVED:
            return _malformed("Unexpected keyword")
        return _malformed("Unexpected term")

    def map_or_list(self):
        """A literal map, list or bag, as an expression node."""
        tok = self.take()
        if tok[1] == "{":
            entries = []
            while not self.at_op("}"):
                key = self.take()
                if key[0] != "str":
                    raise _malformed("Unexpected term")
                self.expect_op(":")
                entries.append((key[1][1:-1].replace("''", "'"), self.expression()))
                if not self.at_op(","):
                    break
                self.take()
            self.expect_op("}")
            return ("map", entries, tok)
        close = "]" if tok[1] == "[" else ">>"
        items = []
        while not self.at_op(close):
            items.append(self.expression())
            if not self.at_op(","):
                break
            self.take()
        end = self.expect_op(close)
        kind = "list" if close == "]" else "bag"
        return (kind, items, tok, self.text[tok[3]:end[2]])

    def literal_value(self, node, path):
        """The attribute value a literal denotes; numbers are checked where they sit."""
        kind = node[0]
        if kind == "lit":
            value = node[1]
            if "N" in value and node[2] == "num":
                err = number_limit_error(value["N"])
                if err:
                    raise PartiqlError(f"{err} under {path}", statement_level=False)
                return {"N": canonical_number(value["N"])}
            return value
        if kind == "map":
            return {"M": {k: self.literal_value(v, f"{path}.{k}") for k, v in node[1]}}
        if kind == "list":
            return {"L": [self.literal_value(v, f"{path}[{i}]") for i, v in enumerate(node[1])]}
        if kind == "bag":
            members = [self.literal_value(v, path) for v in node[1]]
            return _bag_av(members, [t.strip().strip("'") for t in node[3].split(",")] if node[3].strip() else [])
        if kind == "call":
            tok = node[3]
            raise PartiqlError(f"Unsupported data type: {node[1]} under key root at {self.position(tok[2], len(tok[1]))}")
        raise _malformed("Unexpected term")

    def assignment_value(self, target):
        node = self.expression()
        if node[0] == "call" and node[1] in ("set_add", "set_delete"):
            name, args, tok = node[1].upper(), node[2], node[3]
            if len(args) != 2:
                raise PartiqlError(f"{name} must have exactly two arguments at {self.position(tok[2], len(tok[1]))}")
            first, second = args
            if first[0] != "path" or first[1] != target[1]:
                raise PartiqlError(f"The first argument to {name} must equal the assignment value at "
                                   f"{self.position(tok[2], len(tok[1]))}")
            if second[0] not in ("bag", "lit") or (second[0] == "lit" and second[2] != "param"):
                raise PartiqlError(f"The second argument to {name} must be a value with type SET at "
                                   f"{self.position(first[2], first[3] - first[2])}")
            members = self.literal_value(second, "root") if second[0] == "bag" else second[1]
            return ("setfn", node[1], members)
        return self.value_tree(node)

    def value_tree(self, node):
        """Fold literal sub-trees into attribute values; paths and arithmetic stay as nodes."""
        kind = node[0]
        if kind in ("lit", "map", "list", "bag") and self.is_literal(node):
            return ("lit", self.literal_value(node, "root"), "value")
        if kind == "arith":
            return ("arith", node[1], self.value_tree(node[2]), self.value_tree(node[3]))
        if kind == "call":
            return ("call", node[1], [self.value_tree(a) for a in node[2]], node[3])
        return node

    def is_literal(self, node):
        if node[0] == "lit":
            return True
        if node[0] == "map":
            return all(self.is_literal(v) for _, v in node[1])
        if node[0] in ("list", "bag"):
            return all(self.is_literal(v) for v in node[1])
        return False

    # -- expressions -----------------------------------------------------
    def expression(self):
        node = self.conjunction()
        while self.at_word("OR"):
            self.take()
            self.require_operand()
            node = ("or", node, self.conjunction())
        return node

    def require_operand(self):
        if self.at_end():
            raise _malformed("Missing right-hand side expression of infix operator")

    def conjunction(self):
        node = self.negation()
        while self.at_word("AND"):
            self.take()
            self.require_operand()
            node = ("and", node, self.negation())
        return node

    def negation(self):
        if self.at_word("NOT"):
            self.take()
            return ("not", self.negation())
        return self.predicate()

    def predicate(self):
        left = self.additive()
        tok = self.peek()
        if tok[0] == "op" and tok[1] in _COMPARATORS:
            self.take()
            self.require_operand()
            right = self.additive()
            op = "<>" if tok[1] == "!=" else tok[1]
            left, right = self.where_operand(left), self.where_operand(right)
            if op in ("<", "<=", ">", ">="):
                _check_ordered(op, left, right)
            return ("cmp", op, left, right)
        negated = False
        if self.at_word("NOT") and self.word(self.peek(1)) in ("BETWEEN", "IN", "LIKE"):
            self.take()
            negated = True
        word = self.word()
        if word == "BETWEEN":
            self.take()
            low = self.additive()
            self.expect_word("AND", "Unexpected term")
            node = ("between", self.where_operand(left), self.where_operand(low), self.where_operand(self.additive()))
            _check_ordered("BETWEEN", node[2], node[3])
        elif word == "IN":
            self.take()
            if self.at_op("?"):
                options = ("lit", self.parameter(), "param")
            else:
                opener = self.peek()
                if opener[1] not in ("(", "["):
                    raise self.term_error()
                self.take()
                close = ")" if opener[1] == "(" else "]"
                items = []
                while not self.at_op(close):
                    items.append(self.where_operand(self.additive()))
                    if not self.at_op(","):
                        break
                    self.take()
                self.expect_op(close)
                options = ("options", items)
            node = ("in", self.where_operand(left), options)
        elif word == "LIKE":
            self.take()
            node = ("like", left, self.additive())
        elif word == "IS":
            self.take()
            is_not = False
            if self.at_word("NOT"):
                self.take()
                is_not = True
            node = ("is", self.where_operand(left), self.type_name(), is_not)
        else:
            return left
        return ("not", node) if negated else node

    def type_name(self):
        first = self.word()
        if first in ("DOUBLE", "CHARACTER") and self.word(self.peek(1)) in ("PRECISION", "VARYING"):
            self.take()
            first = f"{first} {self.take()[1].upper()}"
        elif first in _TYPE_NAMES:
            self.take()
        else:
            raise _malformed("Expected type name")
        return _TYPE_NAMES[first]

    def where_operand(self, node):
        if node[0] == "lit" and node[2] == "num":
            err = number_limit_error(node[1]["N"])
            if err:
                raise PartiqlError(f"{err} under root", statement_level=False)
            return ("lit", {"N": canonical_number(node[1]["N"])}, "value")
        return node

    def additive(self):
        node = self.unary()
        while self.at_op("+", "-"):
            op = self.take()[1]
            self.require_operand()
            node = ("arith", op, node, self.unary())
        return node

    def unary(self):
        if self.at_op("-"):
            self.take()
            node = self.unary()
            if node[0] == "lit" and node[2] == "num":
                return ("lit", {"N": "-" + node[1]["N"]}, "num")
            return ("arith", "-", ("lit", {"N": "0"}, "value"), node)
        return self.primary()

    def primary(self):
        tok = self.peek()
        kind, value = tok[0], tok[1]
        if kind == "str":
            self.take()
            return ("lit", {"S": value[1:-1].replace("''", "'")}, "str")
        if kind == "num":
            self.take()
            return ("lit", {"N": value}, "num")
        if kind == "op":
            if value == "?":
                return ("lit", self.parameter(), "param")
            if value == "(":
                self.take()
                node = self.expression()
                self.expect_op(")")
                return node
            if value in ("{", "[", "<<"):
                return self.map_or_list()
            raise self.term_error()
        word = self.word(tok)
        if word in ("TRUE", "FALSE"):
            self.take()
            return ("lit", {"BOOL": word == "TRUE"}, "bool")
        if word == "NULL":
            self.take()
            return ("lit", {"NULL": True}, "null")
        if word == "MISSING":
            self.take()
            return ("missing",)
        if kind == "id" and self.peek(1)[1] == "(":
            return self.call()
        if kind in ("id", "qid"):
            start, parts, end = self.path()
            return ("path", parts, start, end)
        raise self.term_error()

    def call(self):
        tok = self.take()
        self.take()
        args = []
        while not self.at_op(")"):
            args.append(self.expression())
            if not self.at_op(","):
                break
            self.take()
        self.expect_op(")")
        return ("call", tok[1].lower(), args, tok)


def _check_ordered(op, *operands):
    """Ordering exists only for strings, numbers and binaries."""
    for operand in operands:
        if operand[0] == "lit" and isinstance(operand[1], dict) and operand[1]:
            vtype = next(iter(operand[1]))
            if vtype not in ("S", "N", "B"):
                raise PartiqlError("Incorrect operand type for operator or function; "
                                   f"operator or function: {op}, operand type: {vtype}", statement_level=False)


def check_where(parser, node):
    """Functions a WHERE clause accepts, and their operands."""
    kind = node[0]
    if kind == "call":
        name, args, tok = node[1], node[2], node[3]
        if name not in _WHERE_FUNCTIONS:
            raise PartiqlError(f"Unrecognized function: {name} at {parser.position(tok[2], len(tok[1]))}")
        if name in ("attribute_exists", "attribute_not_exists"):
            if len(args) != 1:
                raise PartiqlError("Incorrect number of operands for operator or function; "
                                   f"operator or function: {name}, number of operands: {len(args)}")
            if args[0][0] != "path":
                raise PartiqlError(f"Operator or function requires a document path; operator or function: {name}")
        for arg in args:
            check_where(parser, arg)
        return
    for child in node[1:]:
        if isinstance(child, tuple) and child and isinstance(child[0], str):
            check_where(parser, child)
        elif isinstance(child, list):
            for item in child:
                if isinstance(item, tuple) and item and isinstance(item[0], str):
                    check_where(parser, item)


def parse_statement(text, parameters):
    """Parse one PartiQL statement into the shape the executors run."""
    parser = _Parser(text, parameters)
    out = parser.statement()
    for clause in (out.get("where"), (out.get("select") or {}).get("where")):
        if clause is not None:
            check_where(parser, clause)
    return out
