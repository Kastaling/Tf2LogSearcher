"""Tests for default profile disk cache."""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from app import downloader
from app.logs_tf import steamid3_to_steamid64
from app.stats_db import stats_player_stats_cache_token_frozenset_from_parts
from app.profile_cache_db import (
    connect_profile_cache_db,
    get_default_coplayers_cache,
    get_default_profile_cache,
    init_profile_cache_db,
    invalidate_default_profile_caches,
    is_default_coplayers_scope,
    is_default_profile_scope,
    set_default_coplayers_cache,
    set_default_profile_cache,
)
from app.search_cache import (
    get as cache_get,
    invalidate_coplayers_for_steamid,
    invalidate_profile_for_steamid,
    set_ as cache_set,
    singleflight_build,
)


@pytest.fixture()
def profile_cache_conn(tmp_path):
    db = tmp_path / "profile_cache.db"
    conn = connect_profile_cache_db(db)
    init_profile_cache_db(conn)
    yield conn, db
    conn.close()


def test_is_default_profile_scope() -> None:
    assert is_default_profile_scope(gamemode="", date_from=None, date_to=None, map_query="")
    assert not is_default_profile_scope(gamemode="cp", date_from=None, date_to=None, map_query="")
    assert not is_default_profile_scope(gamemode="", date_from=None, date_to=None, map_query="cp_")


def test_set_get_and_token_mismatch(profile_cache_conn) -> None:
    conn, _db = profile_cache_conn
    sid = "76561197960287930"
    payload = {"steamid64": sid, "logs_count": 3, "overview": {"total_kills": 1}}
    token_a = (3, 100, 500)
    token_b = (4, 100, 500)
    assert set_default_profile_cache(conn, sid, payload, token_a)
    assert get_default_profile_cache(conn, sid, token_a) == payload
    assert get_default_profile_cache(conn, sid, token_b) is None


def test_cache_token_frozenset_empty_for_zero_parts() -> None:
    """Regression: frozenset((0,0,0)) must not become frozenset({0})."""
    assert stats_player_stats_cache_token_frozenset_from_parts((0, 0, 0)) == frozenset()
    assert stats_player_stats_cache_token_frozenset_from_parts((0, 0, 0)) != frozenset({0})
    assert stats_player_stats_cache_token_frozenset_from_parts((3, 100, 500)) == frozenset((3, 100, 500))


def test_in_memory_profile_cache_validates_zero_token(tmp_path) -> None:
    """Stored (0,0,0) token must match stats_player_stats_cache_token validation."""
    sid = "76561198000000099"
    ck = (sid, "", "", "", "")
    token = (0, 0, 0)
    cache_set(
        "profile",
        ck,
        {"steamid64": sid},
        stats_player_stats_cache_token_frozenset_from_parts(token),
    )
    stats_stub = tmp_path / "stats.db"
    stats_stub.write_bytes(b"x")
    with (
        patch("app.search_cache.STATS_DB_PATH", stats_stub),
        patch("app.search_cache.stats_player_stats_cache_token", return_value=frozenset()),
    ):
        assert cache_get("profile", ck) is not None


def test_disk_cache_token_uses_semantic_tuple_order(profile_cache_conn) -> None:
    """Regression: frozenset iteration order must not affect disk token columns."""
    conn, _db = profile_cache_conn
    sid = "76561197960287930"
    payload = {"steamid64": sid, "logs_count": 1}
    # count=3, max_log_id=100, sum_imported_at=500
    token_parts = (3, 100, 500)
    assert set_default_profile_cache(conn, sid, payload, token_parts)
    row = conn.execute(
        "SELECT token_count, token_max_log_id, token_sum_imported_at FROM profile_cache WHERE steamid64 = ?",
        (sid,),
    ).fetchone()
    assert row == (3, 100, 500)
    assert get_default_profile_cache(conn, sid, token_parts) == payload


def test_invalidate_removes_disk_row(profile_cache_conn) -> None:
    conn, db_path = profile_cache_conn
    sid = "76561198000000001"
    token = (1, 2, 3)
    set_default_profile_cache(conn, sid, {"steamid64": sid}, token)
    conn.close()
    with patch("app.profile_cache_db.PROFILE_CACHE_DB_PATH", db_path):
        invalidate_default_profile_caches([sid])
    conn2 = connect_profile_cache_db(db_path)
    try:
        assert get_default_profile_cache(conn2, sid, token) is None
    finally:
        conn2.close()


def test_invalidate_clears_in_memory_profile(profile_cache_conn, tmp_path) -> None:
    conn, db_path = profile_cache_conn
    sid = "76561198000000002"
    token = (2, 20, 200)
    ck = (sid, "", "", "", "")
    cache_set(
        "profile",
        ck,
        {"steamid64": sid, "cached": True},
        stats_player_stats_cache_token_frozenset_from_parts(token),
    )
    stats_stub = tmp_path / "stats.db"
    stats_stub.write_bytes(b"x")
    with (
        patch("app.search_cache.STATS_DB_PATH", stats_stub),
        patch(
            "app.search_cache.stats_player_stats_cache_token",
            return_value=stats_player_stats_cache_token_frozenset_from_parts(token),
        ),
    ):
        assert cache_get("profile", ck) is not None
    conn.close()
    with patch("app.profile_cache_db.PROFILE_CACHE_DB_PATH", db_path):
        invalidate_default_profile_caches([sid])
    assert cache_get("profile", ck) is None


def test_collect_pending_agg_invalidates_profile_cache(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "profile_cache.db"
    monkeypatch.setattr("app.profile_cache_db.PROFILE_CACHE_DB_PATH", db_path)
    conn = connect_profile_cache_db(db_path)
    init_profile_cache_db(conn)
    sid3 = "[U:1:39734273]"
    sid = steamid3_to_steamid64(sid3)
    assert sid
    token = (1, 1, 1)
    set_default_profile_cache(conn, sid, {"steamid64": sid}, token)
    conn.close()

    pending: set[str] = set()
    logtext = {
        "players": {
            sid3: {"team": "Red", "kills": 1},
            "[U:1:9999999]": {"team": "Spectator", "kills": 0},
        }
    }
    downloader._collect_pending_agg_steamids_from_log(logtext, pending, 42, state_dir=None)
    assert sid in pending

    conn2 = connect_profile_cache_db(db_path)
    try:
        assert get_default_profile_cache(conn2, sid, token) is None
    finally:
        conn2.close()


def test_rejects_oversized_payload(profile_cache_conn) -> None:
    conn, _db = profile_cache_conn
    sid = "76561197960287930"
    huge = {"steamid64": sid, "blob": "x" * (52_428_801)}
    token = (1, 0, 0)
    assert not set_default_profile_cache(conn, sid, huge, token)
    row = conn.execute(
        "SELECT 1 FROM profile_cache WHERE steamid64 = ?", (sid,)
    ).fetchone()
    assert row is None


def test_is_default_profile_scope() -> None:
    assert is_default_profile_scope(gamemode="", date_from=None, date_to=None, map_query="")
    assert not is_default_profile_scope(gamemode="cp", date_from=None, date_to=None, map_query="")
    assert not is_default_profile_scope(gamemode="", date_from=None, date_to=None, map_query="cp_")


def test_is_default_coplayers_scope() -> None:
    assert is_default_coplayers_scope(gamemode="", map_query="")
    assert not is_default_coplayers_scope(gamemode="cp", map_query="")
    assert not is_default_coplayers_scope(gamemode="", map_query="cp_")


def test_coplayers_set_get_and_token_mismatch(profile_cache_conn) -> None:
    conn, _db = profile_cache_conn
    sid = "76561197960287930"
    payload = {"rows": [{"steamid64": "76561198000000001", "total_games": 5}], "logs_searched": 10}
    token_a = (10, 100, 500)
    token_b = (11, 100, 500)
    assert set_default_coplayers_cache(conn, sid, "", "", payload, token_a)
    assert get_default_coplayers_cache(conn, sid, "", "", token_a) == payload
    assert get_default_coplayers_cache(conn, sid, "", "", token_b) is None


def test_invalidate_removes_coplayers_disk_row(profile_cache_conn) -> None:
    conn, db_path = profile_cache_conn
    sid = "76561198000000003"
    token = (5, 6, 7)
    payload = {"rows": [], "logs_searched": 5}
    set_default_coplayers_cache(conn, sid, "", "", payload, token)
    conn.close()
    with patch("app.profile_cache_db.PROFILE_CACHE_DB_PATH", db_path):
        invalidate_default_profile_caches([sid])
    conn2 = connect_profile_cache_db(db_path)
    try:
        assert get_default_coplayers_cache(conn2, sid, "", "", token) is None
    finally:
        conn2.close()


def test_invalidate_coplayers_for_steamid_only_matching_player(tmp_path) -> None:
    a = "76561198000000012"
    b = "76561198000000013"
    tok_a = (1, 1, 1)
    tok_b = (2, 2, 2)
    cache_set(
        "coplayers",
        (a, "", ""),
        {"rows": []},
        stats_player_stats_cache_token_frozenset_from_parts(tok_a),
    )
    cache_set(
        "coplayers",
        (b, "cp", ""),
        {"rows": []},
        stats_player_stats_cache_token_frozenset_from_parts(tok_b),
    )
    stats_stub = tmp_path / "stats.db"
    stats_stub.write_bytes(b"x")

    def _token(_path: object, sid: str) -> frozenset[int]:
        parts = tok_a if sid == a else tok_b
        return stats_player_stats_cache_token_frozenset_from_parts(parts)

    invalidate_coplayers_for_steamid(a)
    assert cache_get("coplayers", (a, "", "")) is None
    with (
        patch("app.search_cache.STATS_DB_PATH", stats_stub),
        patch("app.search_cache.stats_player_stats_cache_token", side_effect=_token),
    ):
        assert cache_get("coplayers", (b, "cp", "")) is not None


def test_singleflight_build_coalesces_concurrent_misses(tmp_path) -> None:
    calls = {"n": 0}
    sid = "76561198000000099"
    ck = (sid, "", "")
    token = frozenset((1, 2, 3))
    stats_stub = tmp_path / "stats.db"
    stats_stub.write_bytes(b"x")

    def builder() -> dict[str, object]:
        calls["n"] += 1
        payload = {"value": calls["n"]}
        cache_set("coplayers", ck, payload, token)
        return payload

    import threading

    results: list[dict[str, object]] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            with (
                patch("app.search_cache.STATS_DB_PATH", stats_stub),
                patch("app.search_cache.stats_player_stats_cache_token", return_value=token),
            ):
                results.append(singleflight_build("coplayers", ck, builder))
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert not errors
    assert calls["n"] == 1
    assert len(results) == 4
    assert all(r == {"value": 1} for r in results)


def test_singleflight_timeout_waiter_does_not_start_duplicate_leader(
    tmp_path, monkeypatch
) -> None:
    """Regression: timed-out waiters must not become a second leader during leader teardown."""
    import threading

    monkeypatch.setattr("app.search_cache._SINGLEFLIGHT_WAIT_SEC", 0.05)
    calls = {"n": 0}
    sid = "76561198000000100"
    ck = (sid, "", "")
    token = frozenset((9, 8, 7))
    stats_stub = tmp_path / "stats.db"
    stats_stub.write_bytes(b"x")
    monkeypatch.setattr("app.search_cache.STATS_DB_PATH", stats_stub)
    monkeypatch.setattr(
        "app.search_cache.stats_player_stats_cache_token",
        lambda _path, _sid: token,
    )
    allow_finish = threading.Event()
    leader_started = threading.Event()

    def builder() -> dict[str, object]:
        calls["n"] += 1
        leader_started.set()
        assert allow_finish.wait(timeout=2)
        payload = {"value": 1}
        cache_set("coplayers", ck, payload, token)
        return payload

    errors: list[BaseException] = []
    results: list[dict[str, object]] = []

    def worker() -> None:
        try:
            results.append(singleflight_build("coplayers", ck, builder))
        except BaseException as e:
            errors.append(e)

    leader = threading.Thread(target=worker)
    waiter = threading.Thread(target=worker)
    leader.start()
    assert leader_started.wait(timeout=2)
    waiter.start()
    # Waiter will time out several times while the leader is still building.
    threading.Event().wait(0.2)
    allow_finish.set()
    leader.join(timeout=5)
    waiter.join(timeout=5)
    assert not errors
    assert calls["n"] == 1
    assert len(results) == 2
    assert all(r == {"value": 1} for r in results)


def test_invalidate_profile_for_steamid_only_matching_player(tmp_path) -> None:
    a = "76561198000000010"
    b = "76561198000000011"
    tok_a = (1, 1, 1)
    tok_b = (2, 2, 2)
    cache_set(
        "profile",
        (a, "", "", "", ""),
        {"who": "a"},
        stats_player_stats_cache_token_frozenset_from_parts(tok_a),
    )
    cache_set(
        "profile",
        (b, "cp", "", "", ""),
        {"who": "b"},
        stats_player_stats_cache_token_frozenset_from_parts(tok_b),
    )
    stats_stub = tmp_path / "stats.db"
    stats_stub.write_bytes(b"x")

    def _token(_path: object, sid: str) -> frozenset[int]:
        parts = tok_a if sid == a else tok_b
        return stats_player_stats_cache_token_frozenset_from_parts(parts)

    invalidate_profile_for_steamid(a)
    assert cache_get("profile", (a, "", "", "", "")) is None
    with (
        patch("app.search_cache.STATS_DB_PATH", stats_stub),
        patch("app.search_cache.stats_player_stats_cache_token", side_effect=_token),
    ):
        assert cache_get("profile", (b, "cp", "", "", "")) is not None
