"""pytest plugin: dump per-file test counts, split by run mode, to JSON.

Loaded with ``-p _collect_plan_plugin`` by scripts/select_test_shard.py so a
single ``--collect-only`` pass yields both the ``serial`` and ``parallel``
counts. Collecting twice (once per ``-m`` filter) doubled the cost of the
shard-planning job for data one pass already has.

Writes ``{"tests/test_x.py": {"parallel": 12, "serial": 3}, ...}`` to the path
in $MINISTACK_COLLECT_OUT.
"""

import json
import os


def pytest_collection_finish(session):
    out = os.environ.get("MINISTACK_COLLECT_OUT")
    if not out:
        return

    rootpath = session.config.rootpath
    counts: dict[str, dict[str, int]] = {}
    for item in session.items:
        try:
            path = item.path.relative_to(rootpath).as_posix()
        except ValueError:
            path = item.path.as_posix()
        mode = "serial" if item.get_closest_marker("serial") else "parallel"
        counts.setdefault(path, {"parallel": 0, "serial": 0})[mode] += 1

    with open(out, "w") as fh:
        json.dump(dict(sorted(counts.items())), fh)
