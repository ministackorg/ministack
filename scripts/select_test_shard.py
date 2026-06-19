import argparse
import subprocess
import sys
from collections import defaultdict


def collect_by_file(mode: str) -> dict[str, int]:
    marker = "serial" if mode == "serial" else "not serial"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "--no-header",
         "-m", marker, "tests/"],
        capture_output=True, text=True,
    )
    # 0 = ok, 5 = no tests collected (valid for a mode with no matching tests)
    if result.returncode not in (0, 5):
        print(result.stdout[-2000:] if result.stdout else "", file=sys.stderr)
        print(result.stderr[-2000:] if result.stderr else "", file=sys.stderr)
        raise SystemExit(
            f"pytest --collect-only failed (exit {result.returncode}) "
            f"for mode '{mode}' — aborting to avoid silently skipping tests"
        )

    counts: dict[str, int] = defaultdict(int)
    for line in result.stdout.splitlines():
        line = line.strip()
        if "::" in line:
            counts[line.split("::")[0]] += 1
    return dict(counts)


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
        import json
        print(json.dumps(selected))
    else:
        print(" ".join(selected))


if __name__ == "__main__":
    main()
