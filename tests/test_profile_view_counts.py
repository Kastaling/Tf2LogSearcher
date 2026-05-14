"""Tests for profile page view counters."""
from __future__ import annotations

import importlib

import pytest

import app.config
import app.profile_views as pv


def test_record_profile_view_increments_total_and_unique(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(pv, "PROFILE_VIEWS_DB_PATH", tmp_path / "views.db")
    sid = "76561198000000000"
    t1, u1 = pv.record_profile_view(sid, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    assert t1 == 1 and u1 == 1
    t2, u2 = pv.record_profile_view(sid, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    assert t2 == 2 and u2 == 1
    t3, u3 = pv.record_profile_view(sid, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    assert t3 == 3 and u3 == 2


def test_record_profile_view_rejects_bad_steamid(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(pv, "PROFILE_VIEWS_DB_PATH", tmp_path / "views.db")
    assert pv.record_profile_view("not_a_steamid", "abababababababababababababababab") == (0, 0)


def test_visitor_fingerprint_stable_per_ip_ua(monkeypatch) -> None:
    monkeypatch.delenv("PROFILE_VIEW_HASH_SECRET", raising=False)
    importlib.reload(app.config)
    importlib.reload(pv)
    a = pv.visitor_fingerprint("203.0.113.1", "Mozilla/5.0 Test")
    b = pv.visitor_fingerprint("203.0.113.1", "Mozilla/5.0 Test")
    c = pv.visitor_fingerprint("203.0.113.2", "Mozilla/5.0 Test")
    assert a == b
    assert a != c
    assert len(a) == 32


def test_visitor_fingerprint_with_hmac_secret(monkeypatch) -> None:
    monkeypatch.setenv("PROFILE_VIEW_HASH_SECRET", "unit-test-secret")
    importlib.reload(app.config)
    importlib.reload(pv)
    fp = pv.visitor_fingerprint("10.0.0.1", "UA")
    assert len(fp) == 32
