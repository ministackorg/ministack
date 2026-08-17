"""Periodic reclamation of MiniStack's Docker containers.

Without this, a long-running MiniStack accumulates containers — and the
anonymous volumes they carry — for its whole lifetime. Only ECS had a reaper;
RDS, EKS, ElastiCache, MWAA and OpenSearch containers survived until the process
restarted, and the boot sweep then paid for all of them at once (measured: 119s
on a healthy daemon with a session's worth of orphans).

The rule is deliberately conservative, because a *stopped* container is not
necessarily garbage: ``StopDBInstance`` leaves an exited container that
``StartDBInstance`` must be able to restart. So a container is only reclaimed
when it is unambiguously garbage:

- state is ``created`` or ``dead`` — never actually ran, or the daemon lost it;
- or it is ``exited`` **and** no live service record still references its id.

Services publish the ids they still own via ``register_live_ids``. A service
that does not register is never reaped from here — silence means "unknown", not
"garbage".

Reaping is also scoped to *this* MiniStack. Several instances can share a Docker
daemon, and "not in my records" says nothing about another instance's containers:
a ``StopDBCluster``-ed database is exited by design and expected to restart, so a
second gateway would delete it after the grace period. Every container carries
``ministack.instance`` — the gateway address, which only one process can hold, so
it is an identity rather than a guess — and ``ministack.boot``, a per-process
nonce. The periodic pass matches both, so it cannot see another instance's
containers at all; the boot sweep matches the address only, since holding the
port proves the previous owner is gone.
"""

import concurrent.futures
import logging
import os
import socket
import threading
import time
import uuid

logger = logging.getLogger("container_reaper")

REAP_INTERVAL = 60.0
# An exited container gets this long before it is considered abandoned, so a
# service that is mid-restart is not raced.
EXITED_GRACE = 120.0

# Label carrying which MiniStack owns a container, and which run of it.
INSTANCE_LABEL = "ministack.instance"
BOOT_LABEL = "ministack.boot"

_providers: dict = {}
_lock = threading.Lock()
_started = False
_boot_nonce = uuid.uuid4().hex[:12]


def instance_id() -> str:
    """Identity of this MiniStack: the address it serves on.

    The gateway port is the one thing that is provably exclusive — two instances
    cannot bind it — so it identifies an instance without any coordination.
    """
    host = os.environ.get("MINISTACK_HOSTNAME") or socket.gethostname()
    port = os.environ.get("GATEWAY_PORT") or os.environ.get("EDGE_PORT") or "4566"
    return f"{host}:{port}"


def own_labels(service: str, **extra) -> dict:
    """Labels every MiniStack-created container must carry.

    ``service`` is the value of the ``ministack`` label (e.g. "rds"). Anything in
    ``extra`` is merged, so callers keep their own labels.
    """
    labels = {
        "ministack": service,
        INSTANCE_LABEL: instance_id(),
        BOOT_LABEL: _boot_nonce,
    }
    labels.update({k: v for k, v in extra.items() if v is not None})
    return labels


def register_live_ids(label: str, provider) -> None:
    """Register a callable returning the container ids ``label`` still owns.

    ``label`` is the value of the container's ``ministack`` label (e.g. "rds").
    The provider must return an iterable of container ids; anything it omits
    with an exited container is treated as abandoned.
    """
    with _lock:
        _providers[label] = provider


def _live_ids() -> tuple[set, set]:
    """(ids still owned, labels that answered) — labels that don't answer are skipped."""
    live, known = set(), set()
    with _lock:
        providers = dict(_providers)
    for label, provider in providers.items():
        try:
            ids = provider() or ()
        except Exception as exc:
            logger.debug("reaper: provider for %s failed: %s", label, exc)
            continue
        known.add(label)
        live.update(i for i in ids if i)
    return live, known


def reap_abandoned(docker_client) -> int:
    """Remove abandoned containers. Returns how many were reclaimed."""
    if docker_client is None:
        return 0
    live, known = _live_ids()
    now = time.time()
    removed = 0
    try:
        # Scoped to this instance *and* this run of it. Another MiniStack's
        # containers are not merely skipped later — they are never listed, so no
        # amount of downstream logic can reclaim them. Containers predating the
        # labels are likewise invisible here; the boot sweep still gets them.
        containers = docker_client.containers.list(all=True, filters={"label": [
            f"{INSTANCE_LABEL}={instance_id()}",
            f"{BOOT_LABEL}={_boot_nonce}",
        ]})
    except Exception as exc:
        logger.debug("reaper: listing containers failed: %s", exc)
        return 0

    for c in containers:
        try:
            label = (c.labels or {}).get("ministack", "")
            status = c.status
            if status not in ("created", "dead", "exited"):
                continue              # running / restarting / paused: not ours to judge
            if label not in known:
                # No provider for this service, so nothing can vouch for the
                # container. Silence means "unknown", not "garbage" — a service
                # that creates a container and starts it a moment later (ECS
                # tasks pass through `created`) must never be raced.
                continue
            if c.id in live:
                continue              # a stopped-but-live resource (e.g. StopDBInstance)
            finished = (c.attrs.get("State", {}) or {}).get("FinishedAt", "")
            if finished and _age_seconds(finished, now) < EXITED_GRACE:
                continue
            if status == "created":
                # `created` has no FinishedAt; use Created to age it out so a
                # container mid-start is never taken from under its service.
                created_at = c.attrs.get("Created", "")
                if created_at and _age_seconds(created_at, now) < EXITED_GRACE:
                    continue
            c.remove(force=True, v=True)   # v=True: reclaim the anonymous volume too
            removed += 1
        except Exception:
            continue
    if removed:
        logger.info("Reaped %d abandoned container(s)", removed)
    return removed


def _age_seconds(finished_at: str, now: float) -> float:
    """Seconds since a Docker RFC3339 timestamp; 0 if it cannot be parsed."""
    import datetime
    try:
        ts = finished_at.replace("Z", "+00:00")
        if "." in ts:
            head, rest = ts.split(".", 1)
            frac, _, tz = rest.partition("+")
            ts = f"{head}.{frac[:6]}+{tz}" if tz else f"{head}.{frac[:6]}"
        return max(0.0, now - datetime.datetime.fromisoformat(ts).timestamp())
    except Exception:
        return 0.0


def start(get_docker) -> None:
    """Start the reaper thread once. ``get_docker`` is called each pass."""
    global _started
    with _lock:
        if _started:
            return
        _started = True

    def _loop():
        while True:
            time.sleep(REAP_INTERVAL)
            try:
                reap_abandoned(get_docker())
            except Exception as exc:
                logger.debug("reaper iteration failed: %s", exc)

    threading.Thread(target=_loop, daemon=True, name="ministack-container-reaper").start()

# Every label a service stamps on a container, plus the key-only backstops so a
# new service (or an older container predating its label) is still reclaimed.
# One list, one place: the boot/shutdown sweep and the periodic pass both use it.
SERVICE_LABELS = (
    "ministack=rds", "ministack=ecs", "ministack=elasticache", "ministack=eks",
    "ministack=lambda", "ministack=dsql", "ministack=mwaa", "ministack=glue",
    "ministack=codebuild", "ministack=opensearch",
    "ministack", "com.ministack.service",
)


def reap_all(docker_client, stop_timeout: int = 2, include_unlabelled: bool = False) -> int:
    """Remove every MiniStack container, whatever its state.

    For process boundaries only — boot and shutdown. At boot any surviving
    container **of ours** is by definition an orphan of a dead process: we hold
    the gateway port now, so whichever process created them is gone (persistence
    strips container ids from snapshots, so nothing can still own one). At
    shutdown everything is going away regardless. Neither assumption holds while
    the process is live, which is why :func:`reap_abandoned` exists separately
    and is far more conservative.

    Scoped by ``ministack.instance`` but *not* by boot nonce — the whole point is
    to reclaim a previous run's containers, which carry a different nonce.

    A container owned by a *different* instance is never reclaimed here, at boot
    or at shutdown. Shutdown is not a licence to sweep broadly: another MiniStack
    may be running on the same daemon, and taking its containers down as we exit
    is the very failure this scoping exists to prevent.

    ``include_unlabelled`` additionally matches containers that carry a MiniStack
    service label but *no* instance label — those predate this scheme and cannot
    be attributed to anyone. Boot uses it, because holding the gateway port means
    any such leftover is ours to clean; shutdown does not.
    """
    if docker_client is None:
        return 0
    mine = instance_id()
    seen, targets = set(), []

    def _collect(selector, accept):
        try:
            found = docker_client.containers.list(all=True, filters={"label": selector})
        except Exception as exc:
            logger.debug("reap_all: listing %s failed: %s", selector, exc)
            return
        for c in found:
            if c.id in seen or not accept(c):
                continue
            seen.add(c.id)
            targets.append(c)

    _collect([f"{INSTANCE_LABEL}={mine}"], lambda c: True)
    if include_unlabelled:
        for service_label in SERVICE_LABELS:
            # Only ownerless leftovers. A container carrying someone else's
            # instance label is off limits however it was found.
            _collect([service_label], lambda c: not (c.labels or {}).get(INSTANCE_LABEL))
    return drop_containers(targets, stop_timeout=stop_timeout)


def drop_containers(containers, stop_timeout: int = 2, force: bool = False) -> int:
    """Stop and remove containers concurrently. Returns how many went away.

    Concurrent because ``stop`` is a per-container round trip that blocks until
    the container actually dies — serially that is O(n) seconds of wall clock.
    It matters because this runs on the ``/_ministack/reset`` path while the
    reset lock is held, so every other request queues behind it. Measured: a
    run that left 30 running ECS tasks took 54.4s serially, past the 45s the
    caller was willing to wait. The daemon parallelises stops fine; the cap only
    keeps a huge sweep from opening hundreds of sockets at once.
    """
    targets = [c for c in containers if c is not None]
    if not targets:
        return 0

    def _drop(c):
        try:
            c.stop(timeout=stop_timeout)
            c.remove(v=True, force=force)   # v=True: reclaim the anonymous volume too
            return True
        except Exception:
            return False

    with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(16, len(targets)), thread_name_prefix="ministack-reap") as pool:
        return sum(1 for ok in pool.map(_drop, targets) if ok)

