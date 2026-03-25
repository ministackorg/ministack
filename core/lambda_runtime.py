"""
Lambda warm/cold start worker pool.
Each function gets a persistent Python worker process that imports the handler
once (cold start) and then handles subsequent invocations without re-importing (warm).
"""

import os
import sys
import json
import time
import base64
import zipfile
import tempfile
import threading
import subprocess
import logging

logger = logging.getLogger("lambda_runtime")

# function_name -> Worker
_workers: dict = {}
_lock = threading.Lock()

# Worker script that runs inside the subprocess
_WORKER_SCRIPT = '''
import sys, json, importlib, traceback, os

def run():
    # Read init payload: {"code_dir": "...", "module": "...", "handler": "...", "env": {...}}
    init = json.loads(sys.stdin.readline())
    code_dir = init["code_dir"]
    module_name = init["module"]
    handler_name = init["handler"]
    env = init.get("env", {})

    # Apply env vars
    os.environ.update(env)

    # Import handler module (cold start)
    sys.path.insert(0, code_dir)
    try:
        mod = importlib.import_module(module_name)
        handler_fn = getattr(mod, handler_name)
        sys.stdout.write(json.dumps({"status": "ready", "cold": True}) + "\\n")
        sys.stdout.flush()
    except Exception as e:
        sys.stdout.write(json.dumps({"status": "error", "error": str(e)}) + "\\n")
        sys.stdout.flush()
        return

    # Event loop
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        event = json.loads(line)
        context = type("Context", (), {
            "function_name": init.get("function_name", ""),
            "memory_limit_in_mb": init.get("memory", 128),
            "invoked_function_arn": init.get("arn", ""),
            "aws_request_id": event.get("_request_id", ""),
        })()
        try:
            result = handler_fn(event, context)
            sys.stdout.write(json.dumps({"status": "ok", "result": result}) + "\\n")
        except Exception as e:
            sys.stdout.write(json.dumps({"status": "error", "error": str(e), "trace": traceback.format_exc()}) + "\\n")
        sys.stdout.flush()

run()
'''


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

    def _spawn(self):
        """Extract zip and start worker process."""
        self._tmpdir = tempfile.mkdtemp(prefix=f"ministack-lambda-{self.func_name}-")

        # Write worker script
        worker_path = os.path.join(self._tmpdir, "_worker.py")
        with open(worker_path, "w") as f:
            f.write(_WORKER_SCRIPT)

        # Extract function code
        code_dir = os.path.join(self._tmpdir, "code")
        os.makedirs(code_dir)
        with open(os.path.join(self._tmpdir, "code.zip"), "wb") as f:
            f.write(self.code_zip)
        with zipfile.ZipFile(os.path.join(self._tmpdir, "code.zip")) as zf:
            zf.extractall(code_dir)

        handler = self.config.get("Handler", "index.handler")
        module_name, handler_name = handler.rsplit(".", 1)
        env_vars = self.config.get("Environment", {}).get("Variables", {})

        self._proc = subprocess.Popen(
            [sys.executable, worker_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        # Send init payload
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

        # Wait for ready
        response_line = self._proc.stdout.readline()
        response = json.loads(response_line)
        if response.get("status") != "ready":
            raise RuntimeError(f"Worker init failed: {response.get('error')}")

        self._start_time = time.time()
        logger.info(f"Lambda worker spawned for {self.func_name} (cold start)")

    def invoke(self, event: dict, request_id: str) -> dict:
        with self._lock:
            cold = self._cold

            # Spawn if not running
            if self._proc is None or self._proc.poll() is not None:
                self._spawn()
                cold = True
                self._cold = False
            else:
                cold = False

            event["_request_id"] = request_id
            try:
                self._proc.stdin.write(json.dumps(event) + "\n")
                self._proc.stdin.flush()
                response_line = self._proc.stdout.readline()
                if not response_line:
                    raise RuntimeError("Worker process died")
                response = json.loads(response_line)
                response["cold_start"] = cold
                return response
            except Exception as e:
                self._proc = None  # force respawn next time
                return {"status": "error", "error": str(e), "cold_start": cold}

    def kill(self):
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            self._proc = None
        if self._tmpdir and os.path.exists(self._tmpdir):
            import shutil
            shutil.rmtree(self._tmpdir, ignore_errors=True)


def get_or_create_worker(func_name: str, config: dict, code_zip: bytes) -> Worker:
    with _lock:
        worker = _workers.get(func_name)
        if worker is None:
            worker = Worker(func_name, config, code_zip)
            _workers[func_name] = worker
        return worker


def invalidate_worker(func_name: str):
    """Kill and remove worker when function is updated or deleted."""
    with _lock:
        worker = _workers.pop(func_name, None)
        if worker:
            worker.kill()


def reset():
    """Terminate all warm workers and clear the pool."""
    for worker in list(_workers.values()):
        try:
            worker.proc.terminate()
        except Exception:
            pass
    _workers.clear()
