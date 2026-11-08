# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""CloudFront Functions on the shared Node worker pool (``core.node_pool``).

Node is more permissive than the CloudFront runtime; only ``crypto``, ``querystring`` and ``buffer`` can be required,
plus ``cloudfront`` (the key value store helpers) on runtime 2.0.
"""

from __future__ import annotations

import logging

from ministack.core import node_pool

logger = logging.getLogger("cloudfront_functions")

# CloudFront Functions run in ~1ms; this timeout only stops a runaway function.
_EVAL_TIMEOUT = 5.0
_MAX_OLD_SPACE_MB = 256
_RECYCLE_AFTER = 1000
_MAX_IDLE = 2

# stdout carries the protocol, so anything the function logs goes to stderr.
_WORKER_SCRIPT = r"""
const readline = require("readline");
const vm = require("vm");
const crypto = require("crypto");
const querystring = require("querystring");
const bufferModule = require("buffer");

const _stderrWrite = process.stderr.write.bind(process.stderr);
const _stdoutWrite = process.stdout.write.bind(process.stdout);
process.stdout.write = (chunk, enc, cb) => _stderrWrite(chunk, enc, cb);

// CloudFront Functions documents a fixed require()/import surface (CloudFront
// Developer Guide, "JavaScript runtime 2.0 features for CloudFront
// Functions"): crypto, querystring, and the Buffer module. Node exposes a
// much larger module graph; refusing everything else here makes a function
// that imports e.g. "fs" or "http" fail the way it would on the real edge
// runtime, instead of silently succeeding against Node's surface.
// One request per worker at a time, so the call's key value store snapshot can live here.
let _currentKvs = null;
let _currentRuntime = "";

// Observed on AWS: these error names and messages.
function _awsError(name, message) {
  const err = new Error(message);
  err.name = name;
  return err;
}

// cf.kvs() helpers (CloudFront Developer Guide, "Helper methods for key value stores").
const cloudfrontModule = {
  kvs() {
    // A top-level cf.kvs() runs once per compiled function, so the methods read the current call's store.
    const current = () => {
      if (!_currentKvs) throw _awsError("InternalError", '"cloudfront.kvs" module is not configured');
      return _currentKvs;
    };
    current();
    return {
      async get(key, options) {
        const items = current().items;
        if (!Object.prototype.hasOwnProperty.call(items, key)) throw _awsError("Error", "KeyNotFound");
        const format = (options && options.format) || "string";
        if (format === "json") return JSON.parse(items[key]);
        if (format === "bytes") return bufferModule.Buffer.from(items[key], "utf8");
        return items[key];
      },
      async exists(key) {
        return Object.prototype.hasOwnProperty.call(current().items, key);
      },
      async meta() {
        const store = current();
        return { creationDateTime: new Date(store.created), lastUpdatedDateTime: new Date(store.updated),
                 keyCount: Object.keys(store.items).length };
      },
    };
  },
};

const _ALLOWED_MODULES = { crypto, querystring, buffer: bufferModule };
function sandboxRequire(name) {
  if (Object.prototype.hasOwnProperty.call(_ALLOWED_MODULES, name)) return _ALLOWED_MODULES[name];
  if (name === "cloudfront" && _currentRuntime === "cloudfront-js-2.0") return cloudfrontModule;
  throw new Error("require('" + name + "') is not supported by CloudFront Functions");
}

const compiled = new Map();

function compile(code) {
  const key = crypto.createHash("sha256").update(_currentRuntime + "\0" + code).digest("hex");
  const hit = compiled.get(key);
  if (hit) return hit;

  // Runtime 1.0 code calls require() directly; runtime 2.0 code uses ES
  // module `import` statements for the same three modules (CloudFront
  // Developer Guide, "JavaScript runtime 2.0 features for CloudFront
  // Functions"). Rather than a full ESM loader, a default or namespace
  // import is rewritten to the equivalent `require()` call — sandboxRequire
  // still enforces the allowed module set either way, and a named import
  // (e.g. `import { createHash } from 'crypto'`) is rewritten the same way.
  const stripped = code.replace(
    /^\s*import\s+(?:(\*\s*as\s*(\w+))|\{([^}]*)\}|(\w+))\s+from\s+['"]([^'"]+)['"]\s*;?\s*$/gm,
    (_m, _star, starName, named, deflt, source) => {
      if (named) {
        return named.split(",").map((part) => {
          const [orig, alias] = part.split(/\s+as\s+/).map((x) => x.trim());
          return orig ? `const ${alias || orig} = require(${JSON.stringify(source)}).${orig};` : "";
        }).join("\n");
      }
      const name = starName || deflt;
      return name ? `const ${name} = require(${JSON.stringify(source)});` : "";
    },
  );

  const src = `${stripped}\n;globalThis.__handler = typeof handler === "function" ? handler : null;`;
  const sandbox = {
    require: sandboxRequire, console, JSON, Math, Date, Object, Array, String,
    Number, Boolean, RegExp, Map, Set, Promise, Error, TypeError, RangeError,
    TextEncoder, TextDecoder, atob, btoa, Buffer: bufferModule.Buffer,
  };
  vm.createContext(sandbox);
  new vm.Script(src, { filename: "function.js" }).runInContext(sandbox);
  const entry = { handler: sandbox.__handler };
  compiled.set(key, entry);
  return entry;
}

async function run(req) {
  const { code, event } = req;
  _currentKvs = req.kvs || null;
  _currentRuntime = req.runtime || "";
  const entry = compile(code);
  if (typeof entry.handler !== "function") {
    return { status: "error", message: "function code does not define a top-level handler(event) function" };
  }
  try {
    let value = entry.handler(event);
    // cloudfront-js-2.0 allows async handlers (CloudFront Developer Guide,
    // "JavaScript runtime 2.0 features"); 1.0 handlers return a plain value.
    if (value && typeof value.then === "function") {
      value = await value;
    }
    return { status: "ok", value: value === undefined ? null : value };
  } catch (err) {
    return { status: "error", message: String((err && err.stack) || err) };
  }
}

const rl = readline.createInterface({ input: process.stdin });
rl.on("line", async (line) => {
  if (!line.trim()) return;
  let out;
  try {
    out = await run(JSON.parse(line));
  } catch (e) {
    out = { status: "error", message: String((e && e.stack) || e) };
  }
  _stdoutWrite(JSON.stringify(out) + "\n");
});
"""


class CloudFrontFunctionError(Exception):
    """A function threw or could not run; CloudFront answers 503."""


_pool = node_pool.NodeWorkerPool(
    _WORKER_SCRIPT, log_prefix="cloudfront-js", logger=logger, timeout=_EVAL_TIMEOUT,
    max_old_space_mb=_MAX_OLD_SPACE_MB, recycle_after=_RECYCLE_AFTER, max_idle=_MAX_IDLE,
)


def evaluate(code: bytes, event: dict, runtime: str = "", kvs: dict | None = None):
    """Run a published function's code against `event`; return its returned
    request/response object (a dict), or raise CloudFrontFunctionError.
    `kvs` is the associated key value store: {"items", "created", "updated"}."""
    try:
        out = _pool.call({"code": code.decode("utf-8"), "event": event, "runtime": runtime, "kvs": kvs},
                         timeout=_EVAL_TIMEOUT)
    except node_pool.NodeWorkerTimeout as exc:
        raise CloudFrontFunctionError(
            f"function evaluation exceeded {int(_EVAL_TIMEOUT)} seconds and was cancelled"
        ) from exc
    except node_pool.NodeWorkerError as exc:
        if exc.reason == "missing_node":
            raise CloudFrontFunctionError("CloudFront Functions need Node, which was not found") from exc
        if exc.reason == "closed":
            raise CloudFrontFunctionError("CloudFront Functions worker closed unexpectedly") from exc
        raise CloudFrontFunctionError(f"CloudFront Functions worker failed: {exc.cause_text}") from exc

    if out["status"] == "error":
        raise CloudFrontFunctionError(out.get("message", "function execution error"))
    return out.get("value")


def reset():
    """Drop every worker, so a reset leaves no compiled-function cache behind."""
    _pool.reset()


def available():
    """Whether CloudFront Functions can be evaluated at all in this environment."""
    return node_pool.available()
