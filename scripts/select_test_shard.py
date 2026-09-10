import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def collect_by_file(mode: str | None = None) -> dict[str, dict[str, int]] | dict[str, int]:
    """Collect test counts per file, split by mode, in a single pytest pass.

    Returns ``{file: {"parallel": n, "serial": n}}``. When ``mode`` is given,
    returns the flat ``{file: n}`` for that mode only (files with 0 omitted),
    which is the shape the sharding helpers consume.

    One pass rather than one per ``-m`` filter: collection dominates the
    shard-planning job, and both counts come out of the same collected set.
    """
    out_path = REPO_ROOT / ".pytest_collect_plan.json"
    env = {**os.environ, "MINISTACK_COLLECT_OUT": str(out_path)}
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (str(Path(__file__).resolve().parent), env.get("PYTHONPATH", "")) if p
    )
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "--no-header",
         "-p", "_collect_plan_plugin", "tests/"],
        capture_output=True, text=True, env=env, cwd=REPO_ROOT,
    )
    # 0 = ok, 5 = no tests collected (valid for a suite with no matching tests)
    if result.returncode not in (0, 5) or not out_path.exists():
        print(result.stdout[-2000:] if result.stdout else "", file=sys.stderr)
        print(result.stderr[-2000:] if result.stderr else "", file=sys.stderr)
        raise SystemExit(
            f"pytest --collect-only failed (exit {result.returncode}) "
            f"— aborting to avoid silently skipping tests"
        )

    counts: dict[str, dict[str, int]] = json.loads(out_path.read_text())
    out_path.unlink(missing_ok=True)

    if mode is None:
        return counts
    return {f: c[mode] for f, c in counts.items() if c.get(mode)}


def build_shards(counts: dict[str, int], n: int) -> list[list[str]]:
    shards, totals = [[] for _ in range(n)], [0] * n
    for f in sorted(counts, key=counts.__getitem__, reverse=True):
        i = min(range(n), key=totals.__getitem__)
        shards[i].append(f)
        totals[i] += counts[f]
    return shards


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--shard-index", type=int, required=True)
    p.add_argument("--shard-count", type=int, required=True)
    p.add_argument("--mode", choices=["parallel", "serial"], required=True)
    p.add_argument("--exclude", action="append", default=[])
    p.add_argument("--format", choices=["shell", "json"], default="shell")
    args = p.parse_args()

    if args.shard_count <= 0:
        raise SystemExit("--shard-count must be greater than 0")
    if not 0 <= args.shard_index < args.shard_count:
        raise SystemExit("--shard-index must be in range [0, --shard-count)")

    counts = collect_by_file(args.mode)
    counts = {f: c for f, c in counts.items() if f not in args.exclude}
    shards = build_shards(counts, args.shard_count)
    selected = shards[args.shard_index]

    if args.format == "json":
        print(json.dumps(selected))
    else:
        print(" ".join(selected))


if __name__ == "__main__":
    main()
