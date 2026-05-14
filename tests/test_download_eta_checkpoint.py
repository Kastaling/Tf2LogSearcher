"""Tests for persistent downloader ETA checkpoint JSON."""
from __future__ import annotations

import json
import time
from pathlib import Path

from app.download_eta_checkpoint import (
    ETA_CHECKPOINT_FILENAME,
    load_eta_checkpoint,
    maybe_save_eta_checkpoint,
    save_eta_checkpoint,
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
