"""Tests for storage stats helpers (disk measurement used by /api/storage-stats)."""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from app import routes


def test_dir_size_bytes_missing_path(tmp_path: Path) -> None:
    missing = tmp_path / "nope"
    assert routes._dir_size_bytes(missing) is None


def test_dir_flat_file_stats_missing_path(tmp_path: Path) -> None:
    assert routes._dir_flat_file_stats(tmp_path / "missing") == (None, None)


def test_dir_flat_file_stats_empty_directory(tmp_path: Path) -> None:
    d = tmp_path / "e"
    d.mkdir()
    assert routes._dir_flat_file_stats(d) == (0, 0)


def test_dir_flat_file_stats_bytes_and_count(tmp_path: Path) -> None:
    root = tmp_path / "logs"
    root.mkdir()
    (root / "a").write_bytes(b"xx")
    (root / "b").write_bytes(b"y")
    assert routes._dir_flat_file_stats(root) == (3, 2)


def test_dir_size_bytes_delegates_to_flat_stats(tmp_path: Path) -> None:
    root = tmp_path / "x"
    root.mkdir()
    (root / "f").write_bytes(b"ab")
    assert routes._dir_size_bytes(root) == 2


def test_dir_size_bytes_not_a_directory(tmp_path: Path) -> None:
    f = tmp_path / "file.txt"
    f.write_bytes(b"x")
    assert routes._dir_size_bytes(f) is None


def test_dir_size_bytes_empty_directory(tmp_path: Path) -> None:
    d = tmp_path / "empty"
    d.mkdir()
    assert routes._dir_size_bytes(d) == 0


def test_dir_size_bytes_sums_only_top_level_files(tmp_path: Path) -> None:
    root = tmp_path / "logs"
    root.mkdir()
    (root / "a.json").write_bytes(b"aaa")
    (root / "b.json").write_bytes(b"bb")
    nested = root / "nested"
    nested.mkdir()
    (nested / "hidden.bin").write_bytes(b"should-not-count")
    assert routes._dir_size_bytes(root) == 5


def test_dir_size_bytes_skips_subdirectories_entries(tmp_path: Path) -> None:
    """Only regular files directly under path are summed (flat log dirs)."""
    root = tmp_path / "raw"
    root.mkdir()
    sub = root / "subdir_only"
    sub.mkdir()
    (sub / "x.zip").write_bytes(b"xyz")
    assert routes._dir_size_bytes(root) == 0


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="os.symlink not available")
def test_dir_size_bytes_does_not_follow_symlink_files(tmp_path: Path) -> None:
    """Symlinks are not counted as regular files (follow_symlinks=False)."""
    root = tmp_path / "logs"
    root.mkdir()
    real = root / "real.json"
    real.write_bytes(b"12")
    link = root / "via_link.json"
    try:
        link.symlink_to(real)
    except OSError:
        pytest.skip("cannot create symlink in this environment")
    assert routes._dir_size_bytes(root) == 2


def test_file_size_bytes_missing(tmp_path: Path) -> None:
    assert routes._file_size_bytes(tmp_path / "missing.db") is None


def test_file_size_bytes_not_a_file(tmp_path: Path) -> None:
    d = tmp_path / "dir"
    d.mkdir()
    assert routes._file_size_bytes(d) is None


def test_file_size_bytes_regular_file(tmp_path: Path) -> None:
    p = tmp_path / "stats.db"
    p.write_bytes(b"hello-world")
    assert routes._file_size_bytes(p) == 11


def test_compute_storage_stats_raw_enabled_true(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    raw_logs = tmp_path / "raw_logs"
    state = tmp_path / "state"
    logs.mkdir()
    raw_logs.mkdir()
    state.mkdir()
    (logs / "1.json").write_bytes(b"12")
    (logs / "2.json").write_bytes(b"345")
    (raw_logs / "log_1.log.zip").write_bytes(b"ab")
    stats_db = state / "stats.db"
    chat_db = state / "chat.db"
    raw_db = state / "raw_events.db"
    avatar_db = state / "avatars.db"
    stats_db.write_bytes(b"_" * 10)
    chat_db.write_bytes(b"_" * 4)
    raw_db.write_bytes(b"_" * 6)
    avatar_db.write_bytes(b"x")

    with (
        patch.object(routes, "LOGS_DIR", logs),
        patch.object(routes, "RAW_LOGS_DIR", raw_logs),
        patch.object(routes, "STATS_DB_PATH", stats_db),
        patch.object(routes, "CHAT_DB_PATH", chat_db),
        patch.object(routes, "RAW_EVENTS_DB_PATH", raw_db),
        patch.object(routes, "AVATAR_DB_PATH", avatar_db),
        patch.object(routes, "DOWNLOAD_RAW_ENABLED", True),
    ):
        out = routes._compute_storage_stats()

    assert out["enabled"] is True
    assert out["download_raw_enabled"] is True
    assert out["json_logs_bytes"] == 5
    assert out["json_log_files_count"] == 2
    assert out["raw_logs_bytes"] == 2
    assert out["raw_log_files_count"] == 1
    dbs = out["db_files"]
    assert dbs["stats_db"] == 10
    assert dbs["chat_db"] == 4
    assert dbs["raw_events_db"] == 6
    assert dbs["avatar_db"] == 1
    assert out["db_total_bytes"] == 21
    assert out["total_bytes"] == 5 + 2 + 21


def test_compute_storage_stats_raw_disabled_skips_raw_paths(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    raw_logs = tmp_path / "raw_logs"
    state = tmp_path / "state"
    logs.mkdir()
    raw_logs.mkdir()
    state.mkdir()
    (logs / "x.json").write_bytes(b"yz")
    (raw_logs / "big.zip").write_bytes(b"unused_raw")
    stats_db = state / "stats.db"
    chat_db = state / "chat.db"
    raw_db = state / "raw_events.db"
    avatar_db = state / "avatars.db"
    for p, n in ((stats_db, 3), (chat_db, 2), (raw_db, 99), (avatar_db, 1)):
        p.write_bytes(b"0" * n)

    with (
        patch.object(routes, "LOGS_DIR", logs),
        patch.object(routes, "RAW_LOGS_DIR", raw_logs),
        patch.object(routes, "STATS_DB_PATH", stats_db),
        patch.object(routes, "CHAT_DB_PATH", chat_db),
        patch.object(routes, "RAW_EVENTS_DB_PATH", raw_db),
        patch.object(routes, "AVATAR_DB_PATH", avatar_db),
        patch.object(routes, "DOWNLOAD_RAW_ENABLED", False),
    ):
        out = routes._compute_storage_stats()

    assert out["download_raw_enabled"] is False
    assert out["json_logs_bytes"] == 2
    assert out["json_log_files_count"] == 1
    assert out["raw_logs_bytes"] is None
    assert out["raw_log_files_count"] is None
    assert out["db_files"]["raw_events_db"] is None
    db_total = 3 + 2 + 1
    assert out["db_total_bytes"] == db_total
    assert out["total_bytes"] == 2 + db_total


def test_compute_storage_stats_partial_db_files(tmp_path: Path) -> None:
    """Missing DB files yield None for those keys; totals use only present sizes."""
    logs = tmp_path / "logs"
    raw_logs = tmp_path / "raw_logs"
    state = tmp_path / "state"
    logs.mkdir()
    raw_logs.mkdir()
    state.mkdir()
    (logs / "a.json").write_bytes(b"x")
    stats_db = state / "stats.db"
    stats_db.write_bytes(b"12")
    chat_db = state / "chat.db"
    raw_db = state / "raw_events.db"
    avatar_db = state / "avatars.db"

    with (
        patch.object(routes, "LOGS_DIR", logs),
        patch.object(routes, "RAW_LOGS_DIR", raw_logs),
        patch.object(routes, "STATS_DB_PATH", stats_db),
        patch.object(routes, "CHAT_DB_PATH", chat_db),
        patch.object(routes, "RAW_EVENTS_DB_PATH", raw_db),
        patch.object(routes, "AVATAR_DB_PATH", avatar_db),
        patch.object(routes, "DOWNLOAD_RAW_ENABLED", True),
    ):
        out = routes._compute_storage_stats()

    assert out["json_logs_bytes"] == 1
    assert out["json_log_files_count"] == 1
    assert out["raw_logs_bytes"] == 0
    assert out["raw_log_files_count"] == 0
    assert out["db_files"]["stats_db"] == 2
    assert out["db_files"]["chat_db"] is None
    assert out["db_files"]["raw_events_db"] is None
    assert out["db_files"]["avatar_db"] is None
    assert out["db_total_bytes"] == 2
    assert out["total_bytes"] == 3
