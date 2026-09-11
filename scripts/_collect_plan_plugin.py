"""pytest plugin: dump per-file test counts, split by run mode, to JSON.

Loaded with ``-p _collect_plan_plugin`` by scripts/select_test_shard.py so a
single ``--collect-only`` pass yields both the ``serial`` and ``parallel``
counts. Collecting twice (once per ``-m`` filter) doubled the cost of the
shard-planning job for data one pass already has.

Writes ``{"tests/test_x.py": {"parallel": 12, "serial": 3}, ...}`` to
``<rootpath>/.pytest_collect_plan.json``. The path is derived from the pytest
session rather than handed in through the environment: the plugin and its one
caller already agree on the repo root, so an env var would only be a second
way to say the same thing.
"""

import json

#: Where the plan lands, relative to the pytest rootpath. Kept here so the
#: caller imports the name instead of duplicating the literal.
COLLECT_PLAN_FILENAME = ".pytest_collect_plan.json"


def pytest_collection_finish(session):
    rootpath = session.config.rootpath
    out = rootpath / COLLECT_PLAN_FILENAME
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
