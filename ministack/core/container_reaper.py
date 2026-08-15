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
"""

import logging
import threading
import time

logger = logging.getLogger("container_reaper")

REAP_INTERVAL = 60.0
# An exited container gets this long before it is considered abandoned, so a
# service that is mid-restart is not raced.
EXITED_GRACE = 120.0

_providers: dict = {}
_lock = threading.Lock()
_started = False


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


def reap_once(docker_client) -> int:
    """Remove abandoned containers. Returns how many were reclaimed."""
    if docker_client is None:
        return 0
    live, known = _live_ids()
    now = time.time()
    removed = 0
    try:
        containers = docker_client.containers.list(all=True, filters={"label": "ministack"})
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
                reap_once(get_docker())
            except Exception as exc:
                logger.debug("reaper iteration failed: %s", exc)

    threading.Thread(target=_loop, daemon=True, name="ministack-container-reaper").start()
