"""Tests for raw_events.db backfill helpers."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from app.raw_backfill import (
    _MAX_SQLITE_CORRUPT_RETRIES,
    _filter_log_ids,
    _queue_rolled_back_logs,
    _read_retry_log_ids,
)
from app.raw_db import clamp_sqlite_int, is_sqlite_corrupt_error


def test_sqlite_corrupt_retry_limit() -> None:
    assert _MAX_SQLITE_CORRUPT_RETRIES >= 1


def test_filter_log_ids_min_max_skip() -> None:
    ids = [100, 200, 300, 400, 500]
    assert _filter_log_ids(ids, min_log_id=200, max_log_id=400, skip_files=0) == [200, 300, 400]
    assert _filter_log_ids(ids, min_log_id=None, max_log_id=None, skip_files=2) == [300, 400, 500]
    assert _filter_log_ids(ids, min_log_id=250, max_log_id=450, skip_files=1) == [400]


def test_queue_rolled_back_logs(tmp_path: Path) -> None:
    p = tmp_path / "failed.txt"
    assert _queue_rolled_back_logs(p, [100, 200, 300]) == 3
    assert _read_retry_log_ids(p) == [100, 200, 300]
    assert _queue_rolled_back_logs(None, [400]) == 1
    assert _read_retry_log_ids(p) == [100, 200, 300]
    assert _queue_rolled_back_logs(p, []) == 0


def test_read_retry_log_ids(tmp_path: Path) -> None:
    p = tmp_path / "failed.txt"
    p.write_text("758770\n# comment\n758859\n\n758904\n", encoding="utf-8")
    assert _read_retry_log_ids(p) == [758770, 758859, 758904]


def test_clamp_sqlite_int_drops_overflow() -> None:
    huge = 1 << 63
    assert clamp_sqlite_int(huge) is None
    assert clamp_sqlite_int((1 << 63) - 1) == (1 << 63) - 1


def test_is_sqlite_corrupt_error() -> None:
    assert is_sqlite_corrupt_error(sqlite3.DatabaseError("database disk image is malformed"))
    assert not is_sqlite_corrupt_error(ValueError("Python int too large to convert to SQLite INTEGER"))
