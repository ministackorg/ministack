"""Exercise CI collection with real pytest markers, including auth exclusions."""

import json
import os
import subprocess
import sys
from pathlib import Path


def test_auth_contracts_are_excluded_from_existing_shards(tmp_path):
    (tmp_path / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    serial\n    data_plane\n    auth_contract\n"
    )
    (tmp_path / "test_lanes.py").write_text(
        "import pytest\n"
        "def test_parallel(): pass\n"
        "@pytest.mark.serial\ndef test_serial(): pass\n"
        "@pytest.mark.data_plane\ndef test_data_plane(): pass\n"
        "@pytest.mark.auth_contract\n"
        "@pytest.mark.parametrize('value', [1, 2])\ndef test_auth(value): pass\n"
        "@pytest.mark.auth_contract\n@pytest.mark.serial\ndef test_auth_serial(): pass\n"
        "@pytest.mark.auth_contract\n@pytest.mark.data_plane\ndef test_auth_data_plane(): pass\n"
    )
    (tmp_path / "test_auth_only.py").write_text(
        "import pytest\n@pytest.mark.auth_contract\ndef test_only_auth(): pass\n"
    )
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    env = {**os.environ, "PYTHONPATH": str(scripts)}
    # Parent pytest flags must not change the nested collection's selection.
    env.pop("PYTEST_ADDOPTS", None)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "_collect_plan_plugin"],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    counts = json.loads((tmp_path / ".pytest_collect_plan.json").read_text())
    assert counts == {"test_lanes.py": {"parallel": 1, "serial": 1, "data_plane": 1}}
