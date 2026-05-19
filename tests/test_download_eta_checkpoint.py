"""Tests for persistent downloader ETA checkpoint JSON."""
from __future__ import annotations

import json
import time
from pathlib import Path

from app.download_eta_checkpoint import (
    ETA_CHECKPOINT_FILENAME,
    load_eta_checkpoint,
    maybe_save_eta_checkpoint,
    rate_from_active_segments,
    rate_from_recent_segments,
    reconcile_aggregate_window,
    save_eta_checkpoint,
    split_recent_write_segments,
)


def test_checkpoint_roundtrip(tmp_path: Path) -> None:
    st = tmp_path / "state"
    st.mkdir()
    t0 = 1_700_000_000.0
    save_eta_checkpoint(st, t0, 42, [(t0 + 1, 100), (t0 + 2, 99)])
    w, n, rw = load_eta_checkpoint(st)
    assert w == t0
    assert n == 42
    assert rw == [(t0 + 1, 100.0), (t0 + 2, 99.0)]


def test_checkpoint_corrupt_file_starts_fresh(tmp_path: Path) -> None:
    st = tmp_path / "state"
    st.mkdir()
    (st / ETA_CHECKPOINT_FILENAME).write_text("not json {{{", encoding="utf-8")
    w, n, rw = load_eta_checkpoint(st)
    assert n == 0
    assert rw == []
    assert abs(time.time() - w) < 5.0


def test_checkpoint_wrong_version_starts_fresh(tmp_path: Path) -> None:
    st = tmp_path / "state"
    st.mkdir()
    (st / ETA_CHECKPOINT_FILENAME).write_text(
        json.dumps({"v": 999, "wall_start": 1.0, "total_downloads": 9, "recent_writes": []}),
        encoding="utf-8",
    )
    _w, n, rw = load_eta_checkpoint(st)
    assert n == 0 and rw == []


def test_split_segments_on_idle_gap() -> None:
    writes = [
        (100.0, 10),
        (101.0, 9),
        (250.0, 8),
        (251.0, 7),
    ]
    segments = split_recent_write_segments(writes, gap_threshold_sec=120.0)
    assert len(segments) == 2
    assert len(segments[0]) == 2
    assert len(segments[1]) == 2


def test_rate_ignores_idle_gap_in_checkpoint_sample() -> None:
    """Regression: long pause must not drag ETA to multi-day averages."""
    t0 = 1_700_000_000.0
    active_burst = [(t0 + i * 1.5, 1000 - i) for i in range(40)]
    after_pause = [
        (t0 + 5000.0, 900),
        (t0 + 5002.0, 899),
        (t0 + 5100.0, 898),
    ]
    recent = active_burst + after_pause
    rate = rate_from_recent_segments(recent)
    assert rate is not None
    assert rate > 0.4
    active = rate_from_active_segments(recent)
    assert active is not None
    assert active > 0.4
    wall_start, total = reconcile_aggregate_window(1.0, 99_999, recent, now=t0 + 5200.0)
    assert total == 99_999
    assert wall_start == t0 + 5000.0


def test_reconcile_never_lowers_total_downloads() -> None:
    t0 = 1_700_000_000.0
    active_burst = [(t0 + i * 1.5, 1000 - i) for i in range(40)]
    after_pause = [(t0 + 5000.0, 900), (t0 + 5002.0, 899)]
    recent = active_burst + after_pause
    wall_start, total = reconcile_aggregate_window(1.0, 50_000, recent, now=t0 + 5200.0)
    assert total == 50_000
    assert wall_start == t0 + 5000.0


def test_reconcile_preserves_count_for_single_write_tail() -> None:
    """After a gap, the newest write is a 1-item segment; do not drop the live counter."""
    recent = [(100.0, 10), (101.0, 9), (500.0, 8)]
    wall, total = reconcile_aggregate_window(50.0, 51, recent, now=500.0)
    assert total == 51
    assert wall == 500.0


def test_maybe_save_respects_interval(tmp_path: Path) -> None:
    st = tmp_path / "state"
    st.mkdir()
    last = [0.0]
    recent: list[tuple[float, int]] = []
    maybe_save_eta_checkpoint(st, 1000.0, 1, recent, last, force=False)
    assert (st / ETA_CHECKPOINT_FILENAME).is_file()
    mtime = (st / ETA_CHECKPOINT_FILENAME).stat().st_mtime
    maybe_save_eta_checkpoint(st, 1000.0, 2, recent, last, force=False)
    assert (st / ETA_CHECKPOINT_FILENAME).stat().st_mtime == mtime
    maybe_save_eta_checkpoint(st, 1000.0, 3, recent, last, force=True)
    assert (st / ETA_CHECKPOINT_FILENAME).stat().st_mtime > mtime
    _w, n, _r = load_eta_checkpoint(st)
    assert n == 3
