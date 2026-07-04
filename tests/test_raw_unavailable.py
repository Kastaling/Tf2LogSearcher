"""Tests for persistent raw zip unavailable IDs."""
import json

from app.raw_unavailable import (
    RAW_UNAVAILABLE_FILE,
    load_raw_unavailable,
    mark_raw_unavailable,
    save_raw_unavailable,
    try_save_raw_unavailable,
)


def test_load_empty_when_missing(tmp_path):
    assert load_raw_unavailable(tmp_path) == set()


def test_save_and_load_roundtrip(tmp_path):
    ids = {499624, 499623, 12}
    save_raw_unavailable(tmp_path, ids)
    assert load_raw_unavailable(tmp_path) == ids
    raw = json.loads((tmp_path / RAW_UNAVAILABLE_FILE).read_text(encoding="utf-8"))
    assert raw == [12, 499623, 499624]


def test_mark_raw_unavailable_idempotent():
    unavailable = {1}
    assert mark_raw_unavailable(unavailable, 2) is True
    assert mark_raw_unavailable(unavailable, 2) is False
    assert unavailable == {1, 2}


def test_load_ignores_invalid_entries(tmp_path):
    (tmp_path / RAW_UNAVAILABLE_FILE).write_text("[1, \"bad\", -5, 9999999999]", encoding="utf-8")
    assert load_raw_unavailable(tmp_path) == {1}


def test_try_save_raw_unavailable_handles_os_error(tmp_path, monkeypatch):
    def boom(_state_dir, _unavailable):
        raise OSError("disk full")

    monkeypatch.setattr("app.raw_unavailable.save_raw_unavailable", boom)
    assert try_save_raw_unavailable(tmp_path, {123}) is False
