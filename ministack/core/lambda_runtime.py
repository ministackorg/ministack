"""
Lambda warm/cold start worker pool.
Each function gets a persistent worker process (Python or Node.js) that imports
the handler once (cold start) and then handles subsequent invocations without
re-importing (warm).
"""

import base64
import json
import logging
import os
import shutil
import subprocess
import sys
import queue
import tempfile
import threading
import time
import zipfile
import queue

logger = logging.getLogger("lambda_runtime")

_workers: dict = {}
_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Python worker script (runs inside a persistent subprocess)
# ---------------------------------------------------------------------------

_PYTHON_WORKER_SCRIPT = '''
import sys, json, importlib, traceback, os

def run():
    # Redirect print() to stderr so stdout stays clean for JSON-line protocol
    _real_stdout = sys.stdout
    sys.stdout = sys.stderr

    init = json.loads(sys.stdin.readline())
    code_dir = init["code_dir"]
    module_name = init["module"]
    handler_name = init["handler"]
    env = init.get("env", {})
    os.environ.update(env)
    sys.path.insert(0, code_dir)
    for _ld in filter(None, os.environ.get("_LAMBDA_LAYERS_DIRS", "").split(os.pathsep)):
        _py = os.path.join(_ld, "python")
        if os.path.isdir(_py):
            sys.path.insert(0, _py)
        sys.path.insert(0, _ld)
    try:
        mod = importlib.import_module(module_name)
        handler_fn = getattr(mod, handler_name)
        _real_stdout.write(json.dumps({"status": "ready", "cold": True}) + "\\n")
        _real_stdout.flush()
    except Exception as e:
        _real_stdout.write(json.dumps({"status": "error", "error": str(e)}) + "\\n")
        _real_stdout.flush()
        return

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        event = json.loads(line)
        context = type("Context", (), {
            "function_name": init.get("function_name", ""),
            "memory_limit_in_mb": init.get("memory", 128),
            "invoked_function_arn": init.get("arn", ""),
            "aws_request_id": event.pop("_request_id", ""),
        })()
        try:
            result = handler_fn(event, context)
            _real_stdout.write(json.dumps({"status": "ok", "result": result}) + "\\n")
        except Exception as e:
            _real_stdout.write(json.dumps({"status": "error", "error": str(e), "trace": traceback.format_exc()}) + "\\n")
        _real_stdout.flush()

run()
'''

# ---------------------------------------------------------------------------
# Node.js worker script (runs inside a persistent subprocess)
# ---------------------------------------------------------------------------

_NODEJS_WORKER_SCRIPT = r'''
const readline = require("readline");
const path = require("path");
const http = require("http");
const https = require("https");
const url = require("url");

// Redirect stdout to stderr so stdout stays clean for JSON-line protocol
const _realStdoutWrite = process.stdout.write.bind(process.stdout);
const _stderrWrite = process.stderr.write.bind(process.stderr);
process.stdout.write = function(chunk, encoding, callback) {
  return _stderrWrite(chunk, encoding, callback);
};

function patchAwsSdk() {
  const endpoint = process.env.AWS_ENDPOINT_URL
    || process.env.LOCALSTACK_ENDPOINT
    || process.env.MINISTACK_ENDPOINT;
  if (!endpoint) return;

  const parsed = url.parse(endpoint);
  const msHost = parsed.hostname;
  const msPort = parseInt(parsed.port || "4566", 10);

  // Patch aws-sdk v2 global config
  try {
    const AWS = require("aws-sdk");
    AWS.config.update({
      endpoint: endpoint,
      region: process.env.AWS_REGION || process.env.FBT_AWS_REGION || "us-east-1",
      s3ForcePathStyle: true,
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || "test",
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "test",
    });
    const origHandle = AWS.NodeHttpClient.prototype.handleRequest;
    AWS.NodeHttpClient.prototype.handleRequest = function(req, opts, cb, errCb) {
      if (req.endpoint && req.endpoint.protocol === "http:") {
        if (opts && opts.agent instanceof https.Agent) {
          opts = Object.assign({}, opts, { agent: new http.Agent({ keepAlive: true }) });
        }
      }
      return origHandle.call(this, req, opts, cb, errCb);
    };
  } catch (_) {}

  // Patch https.request for bundled SDK
  const origHttpsReq = https.request;
  https.request = function(options, callback) {
    if (typeof options === "string") options = url.parse(options);
    else if (options instanceof url.URL) options = url.parse(options.toString());
    else options = Object.assign({}, options);

    const host = options.hostname || options.host || "";
    if (host.endsWith(".amazonaws.com") || host.endsWith(".amazonaws.com.cn")) {
      options.protocol = "http:";
      options.hostname = msHost;
      options.host = msHost + ":" + msPort;
      options.port = msPort;
      options.path = options.path || "/";
      if (options.agent instanceof https.Agent) {
        options.agent = new http.Agent({ keepAlive: true });
      } else if (options.agent === undefined) {
        options.agent = new http.Agent({ keepAlive: true });
      }
      delete options._defaultAgent;
      return http.request(options, callback);
    }

    // Downgrade ES HTTPS to HTTP for local Elasticsearch
    var esHost = process.env.ES_ENDPOINT ? process.env.ES_ENDPOINT.split(":")[0] : null;
    if (esHost && (host === esHost || host.startsWith(esHost + ":"))) {
      var esPort = process.env.ES_ENDPOINT ? parseInt(process.env.ES_ENDPOINT.split(":")[1] || "9200", 10) : 9200;
      options.protocol = "http:";
      options.hostname = esHost;
      options.host = esHost + ":" + esPort;
      options.port = esPort;
      options.rejectUnauthorized = false;
      if (options.agent instanceof https.Agent) {
        options.agent = new http.Agent({ keepAlive: true });
      } else if (options.agent === undefined) {
        options.agent = new http.Agent({ keepAlive: true });
      }
      delete options._defaultAgent;
      return http.request(options, callback);
    }

    return origHttpsReq.call(https, options, callback);
  };
  https.get = function(options, callback) {
    var req = https.request(options, callback);
    req.end();
    return req;
  };
}

let handlerFn = null;

const rl = readline.createInterface({ input: process.stdin, terminal: false });
let lineNum = 0;

rl.on("line", async (line) => {
  lineNum++;
  try {
    const msg = JSON.parse(line);

    // First line is the init payload
    if (lineNum === 1) {
      const { code_dir, module: modPath, handler: handlerName, env } = msg;
      Object.assign(process.env, env || {});
      process.env.LAMBDA_TASK_ROOT = code_dir;
      process.env.AWS_LAMBDA_FUNCTION_NAME = msg.function_name || process.env.AWS_LAMBDA_FUNCTION_NAME || "";
      process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE = String(msg.memory || process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE || "128");
      process.env._LAMBDA_FUNCTION_ARN = msg.arn || process.env._LAMBDA_FUNCTION_ARN || "";
      patchAwsSdk();
      try {
        const fullPath = path.resolve(code_dir, modPath);
        let mod;
        let resolvedPath;
        try {
          resolvedPath = require.resolve(fullPath);
        } catch (resolveErr) {
          if (resolveErr.code === "MODULE_NOT_FOUND") {
            const fs = require("fs");
            const mjsPath = fullPath + ".mjs";
            if (fs.existsSync(mjsPath)) {
              resolvedPath = mjsPath;
            } else {
              throw resolveErr;
            }
          } else {
            throw resolveErr;
          }
        }
        try {
          mod = require(resolvedPath);
        } catch (reqErr) {
          if (reqErr.code === "ERR_REQUIRE_ESM") {
            const { pathToFileURL } = require("url");
            mod = await import(pathToFileURL(resolvedPath).href);
          } else {
            throw reqErr;
          }
        }
        handlerFn = mod[handlerName] || (mod.default && mod.default[handlerName]) || mod.default;
        if (typeof handlerFn !== "function") {
          _realStdoutWrite(JSON.stringify({
            status: "error",
            error: `Handler ${handlerName} is not a function in ${modPath}`
          }) + "\n");
          return;
        }
        _realStdoutWrite(JSON.stringify({ status: "ready", cold: true }) + "\n");
      } catch (e) {
        _realStdoutWrite(JSON.stringify({
          status: "error", error: e.message
        }) + "\n");
      }
      return;
    }

    // Subsequent lines are event invocations
    const event = msg;
    const context = {
      functionName: event._function_name || "",
      memoryLimitInMB: event._memory || "128",
      invokedFunctionArn: event._arn || "",
      awsRequestId: event._request_id || "",
      getRemainingTimeInMillis: () => 300000,
      done: () => {},
      succeed: () => {},
      fail: () => {},
    };
    delete event._request_id;
    delete event._function_name;
    delete event._memory;
    delete event._arn;

    try {
      let settled = false;
      const settle = (err, res) => {
        if (settled) return;
        settled = true;
        if (err) {
          _realStdoutWrite(JSON.stringify({
            status: "error", error: String(err.message || err), trace: err.stack || ""
          }) + "\n");
        } else {
          _realStdoutWrite(JSON.stringify({ status: "ok", result: res }) + "\n");
        }
      };
      const callback = (err, res) => settle(err, res);
      context.done = (err, res) => settle(err, res);
      context.succeed = (res) => settle(null, res);
      context.fail = (err) => settle(err || new Error("fail"));

      const result = handlerFn(event, context, callback);
      if (result && typeof result.then === "function") {
        // Async/Promise handler
        result.then(res => settle(null, res), err => settle(err));
      } else if (handlerFn.length < 3 && result !== undefined) {
        // Sync handler that doesn't accept callback and returned a value
        settle(null, result);
      }
      // If handler accepts callback (arity >= 3) or returned undefined,
      // we wait for callback/context.done/context.succeed/context.fail
    } catch (e) {
      _realStdoutWrite(JSON.stringify({
        status: "error", error: e.message, trace: e.stack
      }) + "\n");
    }
  } catch (e) {
    _realStdoutWrite(JSON.stringify({
      status: "error", error: "JSON parse error: " + e.message
    }) + "\n");
  }
});
'''


def _detect_runtime_binary(runtime: str) -> tuple[str, str]:
    """Return (binary, worker_script_content) for the given Lambda runtime string."""
    if runtime.startswith("python"):
        return sys.executable, _PYTHON_WORKER_SCRIPT
    if runtime.startswith("nodejs"):
        return "node", _NODEJS_WORKER_SCRIPT
    return "", ""


def _worker_script_extension(runtime: str) -> str:
    if runtime.startswith("python"):
        return ".py"
    if runtime.startswith("nodejs"):
        return ".js"
    return ".py"


class Worker:
    def __init__(self, func_name: str, config: dict, code_zip: bytes):
        self.func_name = func_name
        self.config = config
        self.code_zip = code_zip
        self._proc = None
        self._tmpdir = None
        self._lock = threading.Lock()
        self._cold = True
        self._start_time = None
        self._stderr_queue: queue.Queue = queue.Queue()
        self._stderr_thread: threading.Thread | None = None

    def _read_stderr(self):
        """Background daemon thread: continuously drain stderr into queue."""
        try:
            for line in self._proc.stderr:
                self._stderr_queue.put(line.rstrip("\n"))
        except Exception:
            pass

    def _spawn(self):
        """Extract zip and start worker process."""
        self._tmpdir = tempfile.mkdtemp(prefix=f"ministack-lambda-{self.func_name}-")
        runtime = self.config.get("Runtime", "python3.12")
        binary, worker_script = _detect_runtime_binary(runtime)
        if not binary:
            raise RuntimeError(f"Unsupported runtime: {runtime}")

        ext = _worker_script_extension(runtime)
        worker_path = os.path.join(self._tmpdir, f"_worker{ext}")
        with open(worker_path, "w") as f:
            f.write(worker_script)

        code_dir = os.path.join(self._tmpdir, "code")
        os.makedirs(code_dir)
        with open(os.path.join(self._tmpdir, "code.zip"), "wb") as f:
            f.write(self.code_zip)
        with zipfile.ZipFile(os.path.join(self._tmpdir, "code.zip")) as zf:
            zf.extractall(code_dir)

        # Extract Lambda Layers and build search paths for the worker process.
        # This mirrors the layer handling in lambda_svc._execute_function_local().
        layers_dirs: list[str] = []
        layer_refs = self.config.get("Layers", [])
        if layer_refs:
            from ministack.services.lambda_svc import _resolve_layer_zip
        for layer_ref in layer_refs:
            layer_arn = layer_ref if isinstance(layer_ref, str) else layer_ref.get("Arn", "")
            if not layer_arn:
                continue
            try:
                layer_data = _resolve_layer_zip(layer_arn)
                if layer_data:
                    layer_dir = os.path.join(self._tmpdir, f"layer_{len(layers_dirs)}")
                    os.makedirs(layer_dir)
                    lzip = os.path.join(self._tmpdir, f"layer_{len(layers_dirs)}.zip")
                    try:
                        with open(lzip, "wb") as lf:
                            lf.write(layer_data)
                        with zipfile.ZipFile(lzip) as lzf:
                            # Validate paths to prevent zip-slip attacks
                            for member in lzf.namelist():
                                resolved = os.path.realpath(os.path.join(layer_dir, member))
                                if not resolved.startswith(os.path.realpath(layer_dir) + os.sep) and resolved != os.path.realpath(layer_dir):
                                    raise RuntimeError(f"Zip entry escapes target dir: {member}")
                            lzf.extractall(layer_dir)
                    except (OSError, zipfile.BadZipFile, zipfile.LargeFileError) as e:
                        logger.error("Failed to extract layer %s", layer_arn, exc_info=True)
                        raise RuntimeError(f"Failed to extract layer {layer_arn}") from e
                    layers_dirs.append(layer_dir)
            except RuntimeError:
                raise
            except Exception as e:
                logger.error("Unexpected error resolving layer %s: %s", layer_arn, e)
                raise RuntimeError(f"Failed to resolve layer {layer_arn}") from e

        # Symlink layer node_modules packages into the code directory so that
        # Node.js ESM import() can resolve them via ancestor-tree lookup.
        # ESM does not use NODE_PATH, so packages must be physically reachable
        # from the handler file's directory tree.
        if layers_dirs and runtime.startswith("nodejs"):
            code_nm = os.path.join(code_dir, "node_modules")
            os.makedirs(code_nm, exist_ok=True)
            for ld in layers_dirs:
                layer_nm = os.path.join(ld, "nodejs", "node_modules")
                if os.path.isdir(layer_nm):
                    for pkg in os.listdir(layer_nm):
                        src = os.path.join(layer_nm, pkg)
                        dst = os.path.join(code_nm, pkg)
                        if not os.path.exists(dst):
                            os.symlink(src, dst)

        handler = self.config.get("Handler", "index.handler")
        module_name, handler_name = handler.rsplit(".", 1)
        env_vars = self.config.get("Environment", {}).get("Variables", {})
        spawn_env = {**os.environ, **env_vars}
        # Restore the internal endpoint URL so Lambda SDK calls reach
        # this MiniStack instance, not a host-mapped port that may be
        # unreachable from inside the container.
        for key in ("AWS_ENDPOINT_URL", "LOCALSTACK_HOSTNAME"):
            if key in os.environ:
                spawn_env[key] = os.environ[key]
        spawn_env.setdefault("LAMBDA_TASK_ROOT", code_dir)
        spawn_env.setdefault("AWS_LAMBDA_FUNCTION_NAME", self.config.get("FunctionName", ""))
        spawn_env.setdefault("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", str(self.config.get("MemorySize", 128)))
        spawn_env.setdefault("_LAMBDA_FUNCTION_ARN", self.config.get("FunctionArn", ""))

        # Set layer paths so worker runtimes can find packages from extracted layers.
        # _LAMBDA_LAYERS_DIRS is consumed by the Python worker; Node.js layer resolution
        # is handled via NODE_PATH populated from each layer's nodejs paths below.
        if layers_dirs:
            spawn_env["_LAMBDA_LAYERS_DIRS"] = os.pathsep.join(layers_dirs)
            # NODE_PATH is used by the CJS require() resolver in Node.js workers.
            # ESM import() does not use NODE_PATH — layer packages are instead
            # symlinked into code/node_modules/ above for ancestor-tree resolution.
            node_paths = []
            for ld in layers_dirs:
                nm = os.path.join(ld, "nodejs", "node_modules")
                if os.path.isdir(nm):
                    node_paths.append(nm)
                nj = os.path.join(ld, "nodejs")
                if os.path.isdir(nj):
                    node_paths.append(nj)
            if node_paths:
                existing = spawn_env.get("NODE_PATH")
                if existing:
                    spawn_env["NODE_PATH"] = os.pathsep.join(node_paths + [existing])
                else:
                    spawn_env["NODE_PATH"] = os.pathsep.join(node_paths)

        self._proc = subprocess.Popen(
            [binary, worker_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=spawn_env,
        )

        self._stderr_queue = queue.Queue()
        self._stderr_thread = threading.Thread(
            target=self._read_stderr, daemon=True, name=f"stderr-{self.func_name}"
        )
        self._stderr_thread.start()

        init = {
            "code_dir": code_dir,
            "module": module_name,
            "handler": handler_name,
            "env": env_vars,
            "function_name": self.config.get("FunctionName", ""),
            "memory": self.config.get("MemorySize", 128),
            "arn": self.config.get("FunctionArn", ""),
        }
        self._proc.stdin.write(json.dumps(init) + "\n")
        self._proc.stdin.flush()

        # Read init response, skipping non-JSON lines (stray console output from modules)
        response = None
        for _ in range(200):
            response_line = self._proc.stdout.readline()
            if not response_line:
                stderr_out = ""
                try:
                    stderr_out = self._proc.stderr.read(4096)
                except Exception:
                    pass
                raise RuntimeError(f"Worker process exited immediately. stderr: {stderr_out}")
            response_line = response_line.strip()
            if not response_line or not response_line.startswith("{"):
                continue
            try:
                response = json.loads(response_line)
                break
            except json.JSONDecodeError:
                continue
        if response is None:
            raise RuntimeError("No JSON init response from worker")
        if response.get("status") != "ready":
            raise RuntimeError(f"Worker init failed: {response.get('error')}")

        self._start_time = time.time()
        logger.info("Lambda worker spawned for %s (%s, cold start)", self.func_name, runtime)

    def _drain_stderr(self) -> str:
        """Collect all currently available stderr lines (non-blocking)."""
        lines = []
        try:
            while True:
                lines.append(self._stderr_queue.get_nowait())
        except queue.Empty:
            pass
        return "\n".join(lines)

    def invoke(self, event: dict, request_id: str) -> dict:
        with self._lock:
            cold = self._cold

            if self._proc is None or self._proc.poll() is not None:
                self._spawn()
                cold = True
                self._cold = False
            else:
                cold = False

            timeout = self.config.get("Timeout", 30)
            event["_request_id"] = request_id
            result_box: list = []

            def _read_response():
                try:
                    self._proc.stdin.write(json.dumps(event) + "\n")
                    self._proc.stdin.flush()
                    for _ in range(200):
                        response_line = self._proc.stdout.readline()
                        if not response_line:
                            result_box.append({"status": "error", "error": "Worker process died"})
                            return
                        response_line = response_line.strip()
                        if not response_line:
                            continue
                        if response_line.startswith("{"):
                            try:
                                response = json.loads(response_line)
                                result_box.append(response)
                                return
                            except json.JSONDecodeError:
                                continue
                    result_box.append({"status": "error", "error": "No JSON response from worker after 200 lines"})
                except Exception as e:
                    result_box.append({"status": "error", "error": str(e)})

            reader = threading.Thread(target=_read_response, daemon=True)
            reader.start()
            reader.join(timeout=timeout)

            if reader.is_alive():
                # Timeout — kill the worker process
                logger.warning("Lambda %s timed out after %ds — killing worker", self.func_name, timeout)
                if self._proc:
                    self._proc.kill()
                self._proc = None
                return {
                    "status": "error",
                    "error": f"Task timed out after {timeout}.00 seconds",
                    "cold_start": cold,
                    "log": self._drain_stderr(),
                }

            if not result_box:
                self._proc = None
                return {"status": "error", "error": "Worker returned no response", "cold_start": cold}

            response = result_box[0]
            if response.get("status") == "error":
                self._proc = None
            response["cold_start"] = cold
            # Brief pause so in-flight stderr lines from the OS pipe buffer
            # reach the queue before we drain it.
            time.sleep(0.05)
            response["log"] = self._drain_stderr()
            return response

    def kill(self):
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            self._proc = None
        if self._tmpdir and os.path.exists(self._tmpdir):
            shutil.rmtree(self._tmpdir, ignore_errors=True)


def get_or_create_worker(func_name: str, config: dict, code_zip: bytes,
                         qualifier: str = "$LATEST") -> Worker:
    key = f"{func_name}:{qualifier}"
    with _lock:
        worker = _workers.get(key)
        if worker is not None:
            return worker
        worker = Worker(func_name, config, code_zip)
        _workers[key] = worker
        return worker


def invalidate_worker(func_name: str, qualifier: str = None):
    """Kill and remove workers for a function.

    If qualifier is provided, only kill that specific version/alias worker.
    Otherwise kill all workers for the function (used on delete).
    """
    with _lock:
        if qualifier is not None:
            key = f"{func_name}:{qualifier}"
            worker = _workers.pop(key, None)
            if worker:
                worker.kill()
        else:
            to_remove = [k for k in _workers if k.startswith(f"{func_name}:")]
            for k in to_remove:
                worker = _workers.pop(k, None)
                if worker:
                    worker.kill()


def reset():
    """Terminate all warm workers, clean up temp dirs, and clear the pool."""
    with _lock:
        for worker in list(_workers.values()):
            worker.kill()
        _workers.clear()
