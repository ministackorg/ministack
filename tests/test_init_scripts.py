"""Tests for init script collection from multiple directories."""

from ministack.app import _collect_scripts


def test_collect_scripts_single_dir(tmp_path):
    (tmp_path / "01-seed.sh").write_text("#!/bin/sh\necho seed")
    (tmp_path / "02-setup.sh").write_text("#!/bin/sh\necho setup")
    (tmp_path / "notes.txt").write_text("not a script")

    result = _collect_scripts(str(tmp_path))
    assert len(result) == 2
    assert result[0].endswith("01-seed.sh")
    assert result[1].endswith("02-setup.sh")


def test_collect_scripts_multiple_dirs(tmp_path):
    dir1 = tmp_path / "native"
    dir2 = tmp_path / "compat"
    dir1.mkdir()
    dir2.mkdir()

    (dir1 / "01-seed.sh").write_text("#!/bin/sh\necho native")
    (dir2 / "02-extra.sh").write_text("#!/bin/sh\necho compat")

    result = _collect_scripts(str(dir1), str(dir2))
    assert len(result) == 2
    assert result[0].endswith("01-seed.sh")
    assert result[1].endswith("02-extra.sh")


def test_collect_scripts_dedup_first_dir_wins(tmp_path):
    dir1 = tmp_path / "native"
    dir2 = tmp_path / "compat"
    dir1.mkdir()
    dir2.mkdir()

    (dir1 / "01-seed.sh").write_text("#!/bin/sh\necho native")
    (dir2 / "01-seed.sh").write_text("#!/bin/sh\necho compat")

    result = _collect_scripts(str(dir1), str(dir2))
    assert len(result) == 1
    assert str(dir1) in result[0]  # native path wins


def test_collect_scripts_missing_dir(tmp_path):
    existing = tmp_path / "exists"
    existing.mkdir()
    (existing / "01-seed.sh").write_text("#!/bin/sh\necho hi")

    result = _collect_scripts("/nonexistent/path", str(existing))
    assert len(result) == 1
    assert result[0].endswith("01-seed.sh")


def test_collect_scripts_empty_dirs(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()

    result = _collect_scripts(str(empty))
    assert result == []


def test_collect_scripts_no_dirs():
    result = _collect_scripts("/nonexistent/a", "/nonexistent/b")
    assert result == []


def test_collect_scripts_alphabetical_order(tmp_path):
    (tmp_path / "03-last.sh").write_text("")
    (tmp_path / "01-first.sh").write_text("")
    (tmp_path / "02-middle.sh").write_text("")

    result = _collect_scripts(str(tmp_path))
    names = [r.split("/")[-1] for r in result]
    assert names == ["01-first.sh", "02-middle.sh", "03-last.sh"]
