#!/usr/bin/env python3
"""
Compute the full test sharding plan and emit a GitHub Actions matrix.

Shard counts are read from env vars (set at workflow level):
  PARALLEL_SHARDS  (default: 3)
  SERIAL_SHARDS    (default: 1)

Writes the matrix JSON to $GITHUB_OUTPUT when running in CI,
or prints it to stdout when run locally.
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from select_test_shard import build_shards, collect_by_file


def main() -> None:
    parallel_shards = int(os.environ.get("PARALLEL_SHARDS", 3))
    serial_shards = int(os.environ.get("SERIAL_SHARDS", 1))

    parallel_files = collect_by_file("parallel")
    serial_files = collect_by_file("serial")

    total_parallel = sum(parallel_files.values())
    total_serial = sum(serial_files.values())
    # files can appear in both modes; count unique files across both
    all_files = set(parallel_files) | set(serial_files)

    parallel_sharded = build_shards(parallel_files, parallel_shards)
    serial_sharded = build_shards(serial_files, serial_shards)

    # ── human-readable plan ───────────────────────────────────────────────────

    w = 60
    print("=" * w)
    print(
        f"  Test plan: {total_parallel + total_serial} tests"
        f"  ({total_parallel} parallel + {total_serial} serial)"
        f"  across {len(all_files)} file(s)"
    )
    print("=" * w)

    for label, sharded, file_counts in (
        (f"Parallel — {parallel_shards} shard(s)", parallel_sharded, parallel_files),
        (f"Serial   — {serial_shards} shard(s)", serial_sharded, serial_files),
    ):
        print(f"\n{label}")
        for i, shard_files in enumerate(sharded):
            count = sum(file_counts[f] for f in shard_files)
            print(f"  shard {i}: {count:>3} tests  {len(shard_files):>2} file(s)")
            for f in sorted(shard_files):
                print(f"    {file_counts[f]:>3}  {f}")

    print()

    # ── build matrix ─────────────────────────────────────────────────────────

    includes = []

    for i, shard_files in enumerate(parallel_sharded):
        includes.append({
            "mode": "parallel",
            "shard_index": i,
            "shard_count": parallel_shards,
            "files": " ".join(sorted(shard_files)),
            "test_count": sum(parallel_files[f] for f in shard_files),
        })

    for i, shard_files in enumerate(serial_sharded):
        includes.append({
            "mode": "serial",
            "shard_index": i,
            "shard_count": serial_shards,
            "files": " ".join(sorted(shard_files)),
            "test_count": sum(serial_files[f] for f in shard_files),
        })

    # files map: "parallel_0" -> "tests/a.py tests/b.py", used by test jobs
    files_map = {f"{e['mode']}_{e['shard_index']}": e.pop("files") for e in includes}
    for e in includes:
        e.pop("test_count", None)  # not needed in matrix, already printed above

    matrix = {"include": includes}

    # ── emit ──────────────────────────────────────────────────────────────────

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as fh:
            fh.write(f"matrix={json.dumps(matrix)}\n")
            fh.write(f"files={json.dumps(files_map)}\n")
        print("matrix + files written to $GITHUB_OUTPUT")
    else:
        print("Matrix JSON (GITHUB_OUTPUT not set, dry run):")
        print(json.dumps(matrix, indent=2))
        print("\nFiles map:")
        print(json.dumps(files_map, indent=2))


if __name__ == "__main__":
    main()
