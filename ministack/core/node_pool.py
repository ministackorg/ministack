# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""A pool of Node worker processes evaluating one script over JSON lines.

Evaluating JS (AppSync resolvers, CloudFront Functions) needs a JavaScript
engine, and the image already ships Node for the `nodejs*` Lambda runtimes
(`Dockerfile`, `core/lambda_runtime.py`), so this reuses it rather than adding
a dependency.

A pool of workers handles calls over a JSON-line protocol, the same shape
`lambda_runtime` uses: a request per line on stdin, a reply per line on
stdout, one call in flight per worker. A free worker is leased per call and
one is spawned when none is free — never queued behind, which would recreate
the re-entrancy deadlock `lambda_runtime` documents. Calls are bounded by a
timeout that kills and respawns a stuck process, worker processes are
recycled after serving many calls, and the script's own stderr is surfaced
through a caller-supplied logger.

A `NodeWorkerPool` is bound to one worker script; two callers needing
different scripts (AppSync resolvers vs. CloudFront Functions) each keep
their own pool instance.
"""

from __future__ import annotations

import json
import subprocess
import threading

_NODE_BINARY = "node"


class NodeWorkerTimeout(RuntimeError):
    """A call outlived its deadline; the worker process was killed."""


class NodeWorkerError(RuntimeError):
    """The worker could not be used at all: ``reason`` is ``"missing_node"``
    (no node on PATH), ``"io_error"`` (write failed; ``cause_text`` holds the
    underlying error), or ``"closed"`` (the process ended without a reply)."""

    def __init__(self, message: str, reason: str, cause_text: str | None = None):
        super().__init__(message)
        self.reason = reason
        self.cause_text = cause_text


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


def _pump_stderr(proc, logger, log_prefix):
    try:
        for line in proc.stderr:
            line = line.rstrip("\n")
            if line:
                logger.info("[%s] %s", log_prefix, line)
    except Exception:
        pass


class _Worker:
    """One Node process running `pool.script`, one call at a time.

    A worker holds no Python-side state beyond what the script itself caches
    within its process, so any free worker can serve any call. Single-flight
    per worker matters beyond throughput whenever the script keeps call-scoped
    mutable state in its process-level JS runtime (e.g. a compiled module's
    closure): two calls sharing one process at once would corrupt each
    other's view of it.
    """

    def __init__(self, pool):
        self._pool = pool
        self._proc = None
        self._lock = threading.Lock()
        self.in_use = False
        self.evals = 0

    def _ensure(self):
        if self._proc is not None and self._proc.poll() is None:
            return self._proc
        self._proc = subprocess.Popen(
            [_NODE_BINARY, f"--max-old-space-size={self._pool.max_old_space_mb}",
             "-e", self._pool.script],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )
        # The script's console.log lands on stderr (stdout carries the
        # protocol); surface it through the caller's logger rather than
        # dropping it.
        threading.Thread(
            target=_pump_stderr, args=(self._proc, self._pool.logger, self._pool.log_prefix),
            daemon=True,
        ).start()
        return self._proc

    def _take_proc(self):
        proc, self._proc = self._proc, None
        return proc

    def call(self, request, timeout):
        """Send `request` as one JSON line, return the decoded reply line.

        Raises NodeWorkerTimeout when the call outlives `timeout` (the worker
        is killed — an infinite loop cannot be interrupted from inside Node —
        and a fresh process spawns on the next call), and NodeWorkerError
        when the worker cannot be used at all.
        """
        payload = json.dumps(request) + "\n"
        with self._lock:
            self.evals += 1
            try:
                proc = self._ensure()
                proc.stdin.write(payload)
                proc.stdin.flush()
            except FileNotFoundError as cause:  # no node on PATH
                raise NodeWorkerError("node was not found on PATH", reason="missing_node") from cause
            except (BrokenPipeError, OSError) as cause:
                self._proc = None
                raise NodeWorkerError(f"node worker failed: {cause}", reason="io_error",
                                      cause_text=str(cause)) from cause

            # readline on a thread so a stuck script is bounded by `timeout`
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
                raise NodeWorkerTimeout(
                    f"evaluation exceeded {int(timeout)} seconds and was cancelled")
            line = box[0] if box else ""
            if not line:
                self._proc = None
                raise NodeWorkerError("node worker closed unexpectedly", reason="closed")

        return json.loads(line)

    def shutdown(self):
        with self._lock:
            _terminate(self._take_proc())


class NodeWorkerPool:
    """Lease-or-spawn pool of `_Worker`s, all running the same script.

    Never queues: see the module docstring's re-entrancy note. Idle workers
    beyond `max_idle` are folded on release, and a worker is recycled
    (process killed, respawned lazily on its next lease) after serving
    `recycle_after` calls, bounding whatever a long-lived Node process
    accumulates.
    """

    def __init__(self, script, *, log_prefix, logger, timeout,
                 max_old_space_mb=256, recycle_after=1000, max_idle=2):
        self.script = script
        self.log_prefix = log_prefix
        self.logger = logger
        self.timeout = timeout
        self.max_old_space_mb = max_old_space_mb
        self.recycle_after = recycle_after
        self.max_idle = max_idle
        self._lock = threading.Lock()
        self._workers: list = []

    def _acquire(self):
        with self._lock:
            for worker in self._workers:
                if not worker.in_use:
                    worker.in_use = True
                    return worker
            worker = _Worker(self)
            worker.in_use = True
            self._workers.append(worker)
            return worker

    def _release(self, worker):
        to_kill = None
        with self._lock:
            worker.in_use = False
            if worker.evals >= self.recycle_after:
                worker.evals = 0
                to_kill = worker._take_proc()
            else:
                idle = [w for w in self._workers if not w.in_use]
                if len(idle) > self.max_idle and worker is not self._workers[0]:
                    # A burst leaves surplus processes behind; keep a couple
                    # warm beyond the first and fold the rest.
                    self._workers.remove(worker)
                    to_kill = worker._take_proc()
        _terminate(to_kill)

    def call(self, request, timeout=None):
        """Lease a worker, send `request`, return its decoded JSON reply."""
        worker = self._acquire()
        try:
            return worker.call(request, self.timeout if timeout is None else timeout)
        finally:
            self._release(worker)

    def reset(self):
        """Drop every worker, so a reset leaves no per-process cache behind."""
        with self._lock:
            workers, self._workers[:] = self._workers[:], []
        for worker in workers:
            worker.shutdown()


def available():
    """Whether a Node binary is on PATH and runnable at all."""
    try:
        subprocess.run([_NODE_BINARY, "--version"], capture_output=True, timeout=5)
        return True
    except Exception:
        return False
