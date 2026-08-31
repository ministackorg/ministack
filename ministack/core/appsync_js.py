"""APPSYNC_JS resolver evaluation.

AppSync resolvers are ES modules exporting `request(ctx)` and `response(ctx)`.
Evaluating them needs a JavaScript engine, and the image already ships Node for
the `nodejs*` Lambda runtimes (`Dockerfile`, `core/lambda_runtime.py`), so this
reuses it rather than adding a dependency.

A pool of workers handles evaluations over a JSON-line protocol, the same
shape `lambda_runtime` uses: a request per line on stdin, a reply per line on
stdout, one evaluation in flight per worker. A free worker is leased per
evaluation and one is spawned when none is free — never queued behind, which
would recreate the re-entrancy deadlock `lambda_runtime` documents. Each
worker caches compiled modules by the hash of their source, evaluations are
bounded by a timeout that kills and respawns a stuck process, and resolver
stderr is surfaced through the service logger.

Fidelity note: Node is more permissive than the real APPSYNC_JS sandbox, which
forbids `async`/`await`, `try`/`catch` and most globals. A resolver that runs
here can still be refused on deploy. Enforcing the sandbox is a separate piece of
work; `aws appsync evaluate-code` is the check for that.
"""

from __future__ import annotations

import json
import logging
import subprocess
import threading

logger = logging.getLogger("appsync")

_NODE_BINARY = "node"
# AppSync ends a request after 30 seconds, so no single evaluation may outlive
# that. An infinite loop cannot be interrupted from inside Node (vm timeouts do
# not cover async continuations); on expiry the process is killed and respawned.
_EVAL_TIMEOUT = 30.0
# Heap ceiling per worker (MB), so a runaway resolver is contained rather than
# taking the host's memory with it.
_MAX_OLD_SPACE_MB = 256
# Evaluations a worker serves before its process is recycled.
_RECYCLE_AFTER = 1000
# Idle workers kept warm beyond the first; a burst's surplus is folded on release.
_MAX_IDLE = 2

# stdout carries the protocol, so anything the resolver logs goes to stderr.
_WORKER_SCRIPT = r"""
const readline = require("readline");
const vm = require("vm");
const crypto = require("crypto");

const _stderrWrite = process.stderr.write.bind(process.stderr);
const _stdoutWrite = process.stdout.write.bind(process.stdout);
process.stdout.write = (chunk, enc, cb) => _stderrWrite(chunk, enc, cb);

// Signals a resolver raised through util.error / util.appendError, and the
// early return that ends a pipeline. Carried back to Python as structured
// results rather than thrown, so the caller can tell them apart from a bug.
class AppSyncError extends Error {
  constructor(message, errorType, data, errorInfo) {
    super(message);
    this.errorType = errorType || "UnknownError";
    this.data = data ?? null;
    this.errorInfo = errorInfo ?? null;
  }
}
class EarlyReturn extends Error {
  // skipTo is 'NEXT' (skip this function, run the next) or 'END' (skip the rest
  // of the pipeline and go to the resolver's response handler).
  constructor(value, skipTo) { super("early return"); this.value = value; this.skipTo = skipTo; }
}

function makeGlobals(state) {
  // AWS's util surface, from @aws-appsync/utils' own type definitions. What is
  // not implemented throws naming itself, rather than returning undefined and
  // failing somewhere later in the resolver.
  const notImplemented = (name) => () => {
    throw new Error(`util.${name} is not implemented by ministack yet`);
  };
  const b64 = (v) => Buffer.from(typeof v === "string" ? v : String(v));

  const util = {
    error(message, errorType, data, errorInfo) {
      throw new AppSyncError(message, errorType, data, errorInfo);
    },
    appendError(message, errorType, data, errorInfo) {
      state.appended.push({ message, errorType: errorType || "UnknownError",
                           data: data ?? null, errorInfo: errorInfo ?? null });
    },
    unauthorized() { throw new AppSyncError("Unauthorized", "Unauthorized"); },

    autoId: () => crypto.randomUUID(),
    // ULID and KSUID are sortable ids with their own alphabets — a UUID would
    // be the wrong shape for anything that parses or orders them.
    autoUlid: () => {
      const A = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
      let t = Date.now(), time = "";
      for (let i = 9; i >= 0; i--) { time = A[t % 32] + time; t = Math.floor(t / 32); }
      let rand = "";
      const bytes = crypto.randomBytes(16);
      for (let i = 0; i < 16; i++) rand += A[bytes[i] % 32];
      return time + rand;
    },
    autoKsuid: () => {
      const A = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
      const buf = Buffer.concat([
        (() => { const b = Buffer.alloc(4);
                 b.writeUInt32BE(Math.floor(Date.now() / 1000) - 1400000000); return b; })(),
        crypto.randomBytes(16),
      ]);
      let n = BigInt("0x" + buf.toString("hex")), out = "";
      while (n > 0n) { out = A[Number(n % 62n)] + out; n /= 62n; }
      return out.padStart(27, "0");
    },

    base64Encode: (v) => b64(v).toString("base64"),
    base64Decode: (v) => Buffer.from(String(v), "base64").toString("utf8"),
    urlEncode: (v) => encodeURIComponent(String(v)).replace(/[!'()*]/g,
      (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase()),
    urlDecode: (v) => decodeURIComponent(String(v)),
    escapeJavaScript: (v) => String(v)
      .replace(/\\/g, "\\\\").replace(/'/g, "\\'").replace(/"/g, '\\"')
      .replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/\t/g, "\\t"),
    matches: (pattern, value) => new RegExp(pattern).test(String(value)),
    authType: () => state.authType || "API Key Authorization",

    str: { normalize: (s2, form) => String(s2).normalize(form || "NFC") },

    math: {
      roundNum: (n) => Math.round(n),
      minVal: (a, b) => Math.min(a, b),
      maxVal: (a, b) => Math.max(a, b),
      randomDouble: () => Math.random(),
      randomWithinRange: (start, end) =>
        Math.floor(Math.random() * (end - start)) + start,
    },

    time: {
      nowISO8601: () => new Date().toISOString(),
      nowEpochSeconds: () => Math.floor(Date.now() / 1000),
      nowEpochMilliSeconds: () => Date.now(),
      nowFormatted: (fmt) => _fmt(new Date(), fmt),
      epochMilliSecondsToSeconds: (ms) => Math.floor(ms / 1000),
      epochMilliSecondsToISO8601: (ms) => new Date(ms).toISOString(),
      epochMilliSecondsToFormatted: (ms, fmt) => _fmt(new Date(ms), fmt),
      parseISO8601ToEpochMilliSeconds: (v) => Date.parse(v),
      parseFormattedToEpochMilliSeconds: (v) => Date.parse(v),
    },

    dynamodb: {
      toString: (v) => ({ S: String(v) }),
      toStringSet: (v) => ({ SS: (v || []).map(String) }),
      toNumber: (v) => ({ N: String(v) }),
      toNumberSet: (v) => ({ NS: (v || []).map(String) }),
      toBinary: (v) => ({ B: v }),
      toBinarySet: (v) => ({ BS: v || [] }),
      toBoolean: (v) => ({ BOOL: Boolean(v) }),
      toNull: () => ({ NULL: true }),
      toList: (v) => ({ L: (v || []).map(util.dynamodb.toDynamoDB) }),
      toMap: (v) => ({ M: util.dynamodb.toMapValues(v) }),
      toMapValues: (v) => Object.fromEntries(
        Object.entries(v || {}).map(([k, x]) => [k, util.dynamodb.toDynamoDB(x)])),
      toDynamoDB: (v) => {
        if (v === null || v === undefined) return { NULL: true };
        if (typeof v === "string") return { S: v };
        if (typeof v === "number") return { N: String(v) };
        if (typeof v === "boolean") return { BOOL: v };
        if (Array.isArray(v)) return { L: v.map(util.dynamodb.toDynamoDB) };
        return { M: util.dynamodb.toMapValues(v) };
      },
      toS3Object: (key, bucket, region, version) => ({
        s3: { key, bucket, region, ...(version ? { version } : {}) } }),
      fromS3ObjectJson: (v) => {
        const o = typeof v === "string" ? JSON.parse(v) : v;
        return (o && o.s3) || null;
      },
    },

    rds: {
      // The shape RDS Data API returns, flattened the way AppSync flattens it.
      toJsonObject: (result) => {
        const out = [];
        for (const set of (result && result.sqlStatementResults) || []) {
          const cols = ((set.resultFrame || {}).resultSetMetadata || {}).columnMetadata || [];
          const rows = ((set.resultFrame || {}).records) || [];
          out.push(rows.map((r) => Object.fromEntries(
            (r.values || []).map((cell, i) => {
              const name = (cols[i] || {}).name || String(i);
              const [, value] = Object.entries(cell || {})[0] || [null, null];
              return [name, value];
            }))));
        }
        return out;
      },
    },

    // Response headers need the response AppSync is building, which the
    // evaluator does not own; xml and transform need a parser and a DynamoDB
    // expression builder. Refuse by name rather than silently do nothing.
    http: {
      addResponseHeader: notImplemented("http.addResponseHeader"),
      addResponseHeaders: notImplemented("http.addResponseHeaders"),
      copyHeaders: (headers) => ({ ...(headers || {}) }),
    },
    xml: {
      toJsonString: notImplemented("xml.toJsonString"),
      toMap: notImplemented("xml.toMap"),
    },
    transform: {
      toDynamoDBFilterExpression: (filter) => _ddbExpression(filter),
      // A condition expression is the same grammar; DynamoDB just applies it to
      // the item being written rather than to the items being read.
      toDynamoDBConditionExpression: (filter) => _ddbExpression(filter),
      toElasticsearchQueryDSL: notImplemented("transform.toElasticsearchQueryDSL"),
      toSubscriptionFilter: notImplemented("transform.toSubscriptionFilter"),
    },
  };

  const runtime = {
    earlyReturn(value, returnOptions) {
      throw new EarlyReturn(value, returnOptions && returnOptions.skipTo);
    },
  };

  // `extensions` is the third global AppSync injects, alongside util and
  // runtime. Its calls are side effects on AppSync's own machinery, so they are
  // recorded and handed back rather than executed here — the cache lives in the
  // service, not in this worker.
  const extensions = {
    evictFromApiCache(typeName, fieldName, keys) {
      state.extensions.push({ op: "evictFromApiCache", typeName, fieldName, keys });
      return {};
    },
    setSubscriptionFilter(filter) {
      state.extensions.push({ op: "setSubscriptionFilter", filter });
    },
    setSubscriptionInvalidationFilter(filter) {
      state.extensions.push({ op: "setSubscriptionInvalidationFilter", filter });
    },
    invalidateSubscriptions(payload) {
      state.extensions.push({ op: "invalidateSubscriptions", payload });
    },
  };
  return { util, runtime, extensions };
}

// @aws-appsync/utils/dynamodb — unlike the root package, this one is not
// types-only: its helpers build the request objects a resolver returns. They
// are pure functions over their arguments, so they are reimplemented here
// rather than resolved from node_modules the resolver does not ship with.
function makeDdbHelpers() {
  const cond = (c) => (c === undefined ? undefined : c);
  return {
    get: ({ key }) => ({ operation: "GetItem", key }),
    put: ({ key, item, condition }) => ({
      operation: "PutItem", key, attributeValues: item || {},
      ...(cond(condition) ? { condition } : {}),
    }),
    update: ({ key, update, condition }) => ({
      operation: "UpdateItem", key, update: update || {},
      ...(cond(condition) ? { condition } : {}),
    }),
    remove: ({ key, condition }) => ({
      operation: "DeleteItem", key,
      ...(cond(condition) ? { condition } : {}),
    }),
    scan: ({ filter, index, limit, nextToken, consistentRead, segment, totalSegments } = {}) => ({
      operation: "Scan",
      ...(filter ? { filter } : {}), ...(index ? { index } : {}),
      ...(limit ? { limit } : {}), ...(nextToken ? { nextToken } : {}),
      ...(consistentRead ? { consistentRead } : {}),
      ...(segment !== undefined ? { segment } : {}),
      ...(totalSegments !== undefined ? { totalSegments } : {}),
    }),
    query: ({ query, filter, index, limit, nextToken, scanIndexForward, consistentRead, select } = {}) => ({
      operation: "Query", query,
      ...(filter ? { filter } : {}), ...(index ? { index } : {}),
      ...(limit ? { limit } : {}), ...(nextToken ? { nextToken } : {}),
      ...(scanIndexForward !== undefined ? { scanIndexForward } : {}),
      ...(consistentRead ? { consistentRead } : {}),
      ...(select ? { select } : {}),
    }),
    sync: (args = {}) => ({ operation: "Sync", ...args }),
    // Condition/update expression builders.
    operations: {
      add: (value) => ({ __op: "add", value }),
      append: (value) => ({ __op: "append", value }),
      prepend: (value) => ({ __op: "prepend", value }),
      increment: (by) => ({ __op: "increment", by }),
      decrement: (by) => ({ __op: "decrement", by }),
      replace: (value) => ({ __op: "replace", value }),
      updateListItem: (value, index) => ({ __op: "updateListItem", value, index }),
      remove: () => ({ __op: "remove" }),
    },
  };
}

// util.transform.toDynamoDB{Filter,Condition}Expression
//
// Turns the filter object a GraphQL argument carries into the
// {expression, expressionNames, expressionValues} triple DynamoDB wants.
// DynamoDB is the data source most AppSync APIs use, so this is the transform
// function that actually gets called.
function _ddbExpression(filter) {
  const names = {};
  const values = {};

  const toAttr = (v) => {
    if (v === null || v === undefined) return { NULL: true };
    if (typeof v === "string") return { S: v };
    if (typeof v === "number") return { N: String(v) };
    if (typeof v === "boolean") return { BOOL: v };
    if (Array.isArray(v)) return { L: v.map(toAttr) };
    return { M: Object.fromEntries(Object.entries(v).map(([k, x]) => [k, toAttr(x)])) };
  };
  const nameRef = (field) => { names[`#${field}`] = field; return `#${field}`; };
  const valueRef = (field, op, v, suffix) => {
    const k = `:${field}_${op}${suffix === undefined ? "" : "_" + suffix}`;
    values[k] = toAttr(v);
    return k;
  };

  // AppSync's documented operator set.
  const OPS = {
    eq: (n, f, v) => `${n} = ${valueRef(f, "eq", v)}`,
    ne: (n, f, v) => `${n} <> ${valueRef(f, "ne", v)}`,
    lt: (n, f, v) => `${n} < ${valueRef(f, "lt", v)}`,
    le: (n, f, v) => `${n} <= ${valueRef(f, "le", v)}`,
    gt: (n, f, v) => `${n} > ${valueRef(f, "gt", v)}`,
    ge: (n, f, v) => `${n} >= ${valueRef(f, "ge", v)}`,
    contains: (n, f, v) => `contains(${n}, ${valueRef(f, "contains", v)})`,
    notContains: (n, f, v) => `NOT contains(${n}, ${valueRef(f, "notContains", v)})`,
    beginsWith: (n, f, v) => `begins_with(${n}, ${valueRef(f, "beginsWith", v)})`,
    between: (n, f, v) =>
      `${n} BETWEEN ${valueRef(f, "between", v[0], 0)} AND ${valueRef(f, "between", v[1], 1)}`,
    in: (n, f, v) =>
      `${n} IN (${v.map((x, i) => valueRef(f, "in", x, i)).join(",")})`,
    attributeExists: (n, _f, v) => (v ? `attribute_exists(${n})` : `attribute_not_exists(${n})`),
    attributeType: (n, f, v) => `attribute_type(${n}, ${valueRef(f, "attributeType", v)})`,
  };

  const walk = (node) => {
    const parts = [];
    for (const [key, value] of Object.entries(node || {})) {
      if (value === null || value === undefined) continue;
      if (key === "and" || key === "or") {
        const list = Array.isArray(value) ? value : [value];
        const sub = list.map(walk).filter(Boolean);
        if (sub.length) parts.push(`(${sub.join(key === "and" ? " AND " : " OR ")})`);
        continue;
      }
      if (key === "not") {
        const sub = walk(value);
        if (sub) parts.push(`NOT (${sub})`);
        continue;
      }
      const n = nameRef(key);
      for (const [op, operand] of Object.entries(value || {})) {
        if (operand === null || operand === undefined) continue;
        const build = OPS[op];
        if (!build) {
          throw new Error(`util.transform: unsupported operator ${op!==undefined?op:""}`);
        }
        parts.push(build(n, key, operand));
      }
    }
    return parts.join(" AND ");
  };

  return { expression: walk(filter), expressionNames: names, expressionValues: values };
}

function _fmt(d, fmt) {
  // AppSync uses Java's DateTimeFormatter patterns; the handful that appear in
  // practice are supported and anything else falls back to ISO-8601.
  if (!fmt) return d.toISOString();
  const p = (n, w = 2) => String(n).padStart(w, "0");
  return fmt
    .replace(/yyyy/g, d.getUTCFullYear())
    .replace(/MM/g, p(d.getUTCMonth() + 1))
    .replace(/dd/g, p(d.getUTCDate()))
    .replace(/HH/g, p(d.getUTCHours()))
    .replace(/mm/g, p(d.getUTCMinutes()))
    .replace(/ss/g, p(d.getUTCSeconds()))
    .replace(/SSS/g, p(d.getUTCMilliseconds(), 3));
}

const compiled = new Map();

/**
 * Turn a resolver's ES module source into { request, response }.
 *
 * The `@aws-appsync/utils` import is stripped rather than resolved: AppSync
 * injects util/runtime natively, and the npm package is types-only — its
 * functions are undefined at runtime — so importing it would break a resolver
 * that runs correctly on AWS.
 */
function compile(code) {
  const key = crypto.createHash("sha256").update(code).digest("hex");
  const hit = compiled.get(key);
  if (hit) return hit;

  // Imports are stripped because AppSync injects util/runtime natively and the
  // npm package is types-only. The dynamodb sub-module is the exception — its
  // helpers are real — so whatever name it was bound to is declared instead.
  const ddbBindings = [];
  const stripped = code.replace(
    /^\s*import\s+(?:(\*\s*as\s*\w+)|\{([^}]*)\}|(\w+))?\s*(?:from\s*)?['"]([^'"]+)['"]\s*;?\s*$/gm,
    (_m, star, named, deflt, source) => {
      // The root package: util and runtime are injected as globals, but a
      // bundler renames on collision — esbuild emits `import { util as util2 }`
      // when several modules import it — so an alias has to be bound or the
      // resolver dies with "util2 is not defined".
      if (/@aws-appsync\/utils$/.test(source)) {
        if (named) {
          for (const part of named.split(",")) {
            const [orig, alias] = part.split(/\s+as\s+/).map((x) => x.trim());
            if (orig && alias && alias !== orig) {
              ddbBindings.push(`const ${alias} = ${orig};`);
            }
          }
        }
        return "";
      }
      if (!/@aws-appsync\/utils\/dynamodb$/.test(source)) return "";
      if (star) {
        ddbBindings.push(`const ${star.replace(/^\*\s*as\s*/, "")} = __ddb;`);
      } else if (named) {
        for (const part of named.split(",")) {
          const [orig, alias] = part.split(/\s+as\s+/).map((x) => x.trim());
          if (orig) ddbBindings.push(`const ${alias || orig} = __ddb.${orig};`);
        }
      } else if (deflt) {
        ddbBindings.push(`const ${deflt} = __ddb;`);
      }
      return "";
    },
  ).replace(/^\s*export\s+/gm, "");

  const src = `${ddbBindings.join("\n")}
${stripped}
;__exports.request = typeof request === "function" ? request : null;
;__exports.response = typeof response === "function" ? response : null;`;

  const handlers = {};
  // One mutable holder the injected globals close over, so the module is
  // evaluated once and each call just swaps what util/runtime see. Re-running
  // the module per call would throw on any top-level const it declares —
  // including the binding for the dynamodb helpers.
  const state = { appended: [], extensions: [] };
  const sandbox = { __exports: handlers, __ddb: makeDdbHelpers(), console, JSON,
                    Math, Date, Object, Array, String, Number, Boolean, RegExp,
                    Map, Set, Error, ...makeGlobals(state) };
  vm.createContext(sandbox);
  new vm.Script(src, { filename: "resolver.js" }).runInContext(sandbox);
  const entry = { sandbox, handlers, state };
  compiled.set(key, entry);
  return entry;
}

function run(req) {
  const { code, fn, ctx } = req;
  const entry = compile(code);
  entry.state.appended = [];
  entry.state.extensions = [];
  const appended = entry.state.appended;

  const handler = entry.handlers[fn];
  if (typeof handler !== "function") {
    // A resolver may legitimately omit response(); the caller decides.
    return { status: "missing", appended, stash: ctx.stash ?? {} };
  }
  try {
    const value = handler(ctx) ?? null;
    // A resolver stashes by mutating ctx.stash; that mutation happens in this
    // process, so it has to travel back or a pipeline loses everything a
    // function put there.
    return { status: "ok", value, appended, stash: ctx.stash ?? {},
             extensions: entry.state.extensions };
  } catch (e) {
    if (e instanceof EarlyReturn) {
      return { status: "earlyReturn", value: e.value ?? null, appended,
               skipTo: e.skipTo ?? null, stash: ctx.stash ?? {} };
    }
    if (e instanceof AppSyncError) {
      return { status: "error", error: {
        message: e.message, errorType: e.errorType,
        data: e.data, errorInfo: e.errorInfo }, appended };
    }
    return { status: "error", error: {
      message: String(e && e.message || e), errorType: "InternalError",
      data: null, errorInfo: null }, appended };
  }
}

const rl = readline.createInterface({ input: process.stdin });
rl.on("line", (line) => {
  if (!line.trim()) return;
  let out;
  try {
    out = run(JSON.parse(line));
  } catch (e) {
    out = { status: "error", appended: [], error: {
      message: String(e && e.message || e), errorType: "WorkerError",
      data: null, errorInfo: null } };
  }
  _stdoutWrite(JSON.stringify(out) + "\n");
});
"""


class AppSyncJsError(Exception):
    """A resolver raised through util.error, or failed to evaluate."""

    def __init__(self, message, error_type="UnknownError", data=None, error_info=None):
        super().__init__(message)
        self.error_type = error_type
        self.data = data
        self.error_info = error_info


class AppSyncJsTimeout(RuntimeError):
    """An evaluation outlived its deadline; the worker process was killed."""


class _Worker:
    """One Node process, running one evaluation at a time.

    A worker holds no evaluation-independent state other than its compiled-
    module cache, so any free worker can serve any resolver. Single-flight per
    worker matters beyond throughput: the compiled module's mutable holder
    (``state`` in the worker script) is per-process, so two evaluations of the
    same resolver in one process would corrupt each other's appended errors.
    """

    def __init__(self):
        self._proc = None
        self._lock = threading.Lock()
        self.in_use = False
        self.evals = 0

    def _ensure(self):
        if self._proc is not None and self._proc.poll() is None:
            return self._proc
        self._proc = subprocess.Popen(
            [_NODE_BINARY, f"--max-old-space-size={_MAX_OLD_SPACE_MB}",
             "-e", _WORKER_SCRIPT],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )
        # Resolver console.log lands on stderr (stdout carries the protocol);
        # surface it through the service logger rather than dropping it.
        threading.Thread(target=_pump_stderr, args=(self._proc,), daemon=True).start()
        return self._proc

    def _take_proc(self):
        proc, self._proc = self._proc, None
        return proc

    def evaluate(self, code, fn, ctx, timeout=None):
        """Run `fn` (request/response) from `code` against `ctx`.

        Returns (status, value, appended_errors, stash, skip_to) where status is
        "ok", "earlyReturn" or "missing". Raises AppSyncJsError on a resolver
        error, AppSyncJsTimeout when the evaluation outlives `timeout` (the
        worker is killed — an infinite loop cannot be interrupted from inside
        Node — and a fresh process spawns on the next call), and RuntimeError
        when the worker cannot be used at all.
        """
        timeout = _EVAL_TIMEOUT if timeout is None else timeout
        payload = json.dumps({"code": code, "fn": fn, "ctx": ctx}) + "\n"
        with self._lock:
            self.evals += 1
            try:
                proc = self._ensure()
                proc.stdin.write(payload)
                proc.stdin.flush()
            except FileNotFoundError as exc:  # no node on PATH
                raise RuntimeError(
                    "APPSYNC_JS resolvers need Node, which was not found") from exc
            except (BrokenPipeError, OSError) as exc:
                self._proc = None
                raise RuntimeError(f"APPSYNC_JS worker failed: {exc}") from exc

            # readline on a thread so a stuck resolver is bounded by `timeout`
            # rather than holding this worker forever — the same shape
            # core/lambda_runtime.py uses for a handler that never returns.
            box = []

            def _read():
                try:
                    box.append(proc.stdout.readline())
                except Exception:
                    box.append("")

            reader = threading.Thread(target=_read, daemon=True)
            reader.start()
            reader.join(timeout)
            if reader.is_alive():
                _terminate(self._take_proc())
                raise AppSyncJsTimeout(
                    f"APPSYNC_JS evaluation exceeded {int(timeout)} seconds and was cancelled")
            line = box[0] if box else ""
            if not line:
                self._proc = None
                raise RuntimeError("APPSYNC_JS worker closed unexpectedly")

        out = json.loads(line)
        if out["status"] == "error":
            err = out["error"]
            raise AppSyncJsError(err["message"], err.get("errorType"),
                                 err.get("data"), err.get("errorInfo"))
        return (out["status"], out.get("value"), out.get("appended") or [],
                out.get("stash") or {}, out.get("skipTo"))

    def shutdown(self):
        with self._lock:
            _terminate(self._take_proc())


def _pump_stderr(proc):
    try:
        for line in proc.stderr:
            line = line.rstrip("\n")
            if line:
                logger.info("[appsync-js] %s", line)
    except Exception:
        pass


def _terminate(proc):
    if proc is None:
        return
    try:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Worker pool
#
# Lease a free worker or spawn one; never queue behind a busy worker. Waiting
# would recreate the re-entrancy deadlock core/lambda_runtime.py documents: a
# resolver whose data source calls back into ministack needs a second worker
# while its own request still holds the first. Concurrency is already bounded
# upstream by the asyncio.to_thread pool the data plane runs on, so unbounded
# spawn here cannot run away.
# ---------------------------------------------------------------------------

_pool_lock = threading.Lock()
_pool: list = []


def _acquire():
    with _pool_lock:
        for worker in _pool:
            if not worker.in_use:
                worker.in_use = True
                return worker
        worker = _Worker()
        worker.in_use = True
        _pool.append(worker)
        return worker


def _release(worker):
    to_kill = None
    with _pool_lock:
        worker.in_use = False
        if worker.evals >= _RECYCLE_AFTER:
            # Recycling bounds whatever a long-lived Node process accumulates
            # (compiled modules, heap fragmentation). The worker stays pooled
            # and respawns lazily on its next lease.
            worker.evals = 0
            to_kill = worker._take_proc()
        else:
            idle = [w for w in _pool if not w.in_use]
            if len(idle) > _MAX_IDLE and worker is not _pool[0]:
                # A burst leaves surplus processes behind; keep a couple warm
                # beyond the first and fold the rest.
                _pool.remove(worker)
                to_kill = worker._take_proc()
    _terminate(to_kill)


def evaluate(code, fn, ctx, timeout=None):
    worker = _acquire()
    try:
        return worker.evaluate(code, fn, ctx, timeout=timeout)
    finally:
        _release(worker)


def reset():
    """Drop every worker, so a reset leaves no compiled-module cache behind."""
    with _pool_lock:
        workers, _pool[:] = _pool[:], []
    for worker in workers:
        worker.shutdown()


def available():
    """Whether APPSYNC_JS can be evaluated at all in this environment."""
    try:
        subprocess.run([_NODE_BINARY, "--version"], capture_output=True, timeout=5)
        return True
    except Exception:
        return False
