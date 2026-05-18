"""Resume / range filters for stats backfill from local JSON."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from app import stats_backfill
from app.stats_backfill import _filter_log_paths_for_backfill, _iter_log_files, run_backfill

_MIN_LOGTEXT = {
    "info": {"map": "cp_x", "date": 1, "total_length": 60},
    "players": {
        "[U:1:10]": {
            "team": "Red",
            "kills": 1,
            "assists": 0,
            "deaths": 0,
            "dmg": 100,
            "class_stats": [
                {
                    "type": "soldier",
                    "total_time": 60,
                    "kills": 1,
                    "dmg": 100,
                },
            ],
        },
    },
}


def test_filter_log_paths_skip_files(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    for lid in (5, 10, 15, 20):
        (logs / f"{lid}.json").write_text(json.dumps(_MIN_LOGTEXT), encoding="utf-8")
    files = _iter_log_files(logs)
    assert [p.stem for p in files] == ["5", "10", "15", "20"]
    out = _filter_log_paths_for_backfill(files, min_log_id=None, max_log_id=None, skip_files=2)
    assert [p.stem for p in out] == ["15", "20"]


def test_filter_log_paths_min_max_id(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    for lid in (1, 100, 200, 999):
        (logs / f"{lid}.json").write_text("{}", encoding="utf-8")
    files = _iter_log_files(logs)
    out = _filter_log_paths_for_backfill(files, min_log_id=100, max_log_id=200, skip_files=0)
    assert [p.stem for p in out] == ["100", "200"]


def test_run_backfill_skip_files_inserts_only_tail(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    db = tmp_path / "stats.db"
    for lid in (100, 200, 300):
        (logs / f"{lid}.json").write_text(json.dumps(_MIN_LOGTEXT), encoding="utf-8")
    run_backfill(logs, db, batch_size=50, skip_files=2)
    conn = sqlite3.connect(str(db))
    try:
        rows = conn.execute("SELECT log_id FROM logs ORDER BY log_id").fetchall()
    finally:
        conn.close()
    assert rows == [(300,)]


def test_main_rejects_skip_files_negative(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["stats_backfill", "--skip-files", "-1"])
    with pytest.raises(SystemExit, match="--skip-files must be >= 0"):
        stats_backfill.main()
