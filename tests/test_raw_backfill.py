"""Tests for raw_events.db backfill helpers."""
from __future__ import annotations

from app.raw_backfill import _filter_log_ids


def test_filter_log_ids_min_max_skip() -> None:
    ids = [100, 200, 300, 400, 500]
    assert _filter_log_ids(ids, min_log_id=200, max_log_id=400, skip_files=0) == [200, 300, 400]
    assert _filter_log_ids(ids, min_log_id=None, max_log_id=None, skip_files=2) == [300, 400, 500]
    assert _filter_log_ids(ids, min_log_id=250, max_log_id=450, skip_files=1) == [400]
