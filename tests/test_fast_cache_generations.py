"""Regression coverage for transactional cache versions and latest-name lookup."""
from __future__ import annotations

import sqlite3

from app.chat_db import chat_player_fingerprint, connect_chat_db, init_chat_db, replace_chat_for_log
from app.latest_names_backfill import run as backfill_latest_names
from app.profile_cache_db import connect_profile_cache_db, get_default_profile_cache, init_profile_cache_db
from app.search.search import chat_search_sqlite
from app.stats_db import (
    connect_stats_db,
    init_stats_db,
    lookup_player_names,
    replace_stats_for_log,
    stats_db_fingerprint,
)


SID3 = "[U:1:101]"
SID64 = "76561197960265829"


def _log(alias: str, date_ts: int) -> dict:
    return {
        "info": {"map": "cp_process", "date": date_ts, "title": "Test"},
        "names": {SID3: alias},
        "players": {SID3: {"team": "Red", "kills": 1, "class_stats": []}},
    }


def test_chat_player_generation_advances_with_replacement(tmp_path) -> None:
    path = tmp_path / "chat.db"
    conn = connect_chat_db(path)
    init_chat_db(conn)
    before = chat_player_fingerprint(path, SID64)
    first = _log("One", 100)
    first["chat"] = [{"steamid": SID3, "name": "One", "msg": "hello"}]
    with conn:
        replace_chat_for_log(conn, 1, first)
    after = chat_player_fingerprint(path, SID64)
    assert after != before
    second = _log("Two", 100)
    second["chat"] = [{"steamid": SID3, "name": "Two", "msg": "changed"}]
    with conn:
        replace_chat_for_log(conn, 1, second)
    assert chat_player_fingerprint(path, SID64) != after
    conn.close()


def test_chat_search_count_only_preserves_log_count(tmp_path) -> None:
    path = tmp_path / "chat.db"
    conn = connect_chat_db(path)
    init_chat_db(conn)
    log = _log("One", 100)
    log["chat"] = [{"steamid": SID3, "name": "One", "msg": "hello"}]
    with conn:
        replace_chat_for_log(conn, 1, log)
    conn.close()
    full = chat_search_sqlite("hello", SID64, path)
    count_only = chat_search_sqlite("hello", SID64, path, count_only_log_ids=True)
    assert count_only[:3] == full[:3]
    assert count_only[3] == len(full[3]) == 1


def test_latest_name_backfill_and_reimport(tmp_path) -> None:
    path = tmp_path / "stats.db"
    conn = connect_stats_db(path)
    init_stats_db(conn)
    with conn:
        replace_stats_for_log(conn, 1, _log("Old", 100))
        replace_stats_for_log(conn, 2, _log("New", 200))
    before = stats_db_fingerprint(path)
    assert lookup_player_names(path, [SID64])[SID64] == "New"
    conn.close()


    assert backfill_latest_names(path, batch_size=1) == 2
    assert backfill_latest_names(path, batch_size=1) == 0
    conn = connect_stats_db(path)
    assert conn.execute(
        "SELECT row_count FROM stats_cache_generation WHERE name = 'latest_names_ready'"
    ).fetchone()[0] == 1
    assert lookup_player_names(path, [SID64])[SID64] == "New"
    with conn:
        replace_stats_for_log(conn, 2, _log("Changed", 50))
    assert lookup_player_names(path, [SID64])[SID64] == "Old"
    assert stats_db_fingerprint(path) != before
    conn.close()


def test_repeat_stats_init_skips_completed_imported_at_scan(tmp_path) -> None:
    conn = connect_stats_db(tmp_path / "stats.db")
    init_stats_db(conn)
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    init_stats_db(conn)
    conn.set_trace_callback(None)
    assert not any("lp.imported_at != l.imported_at" in sql for sql in statements)
    conn.close()


def test_profile_cache_migration_invalidates_old_chat_token(tmp_path) -> None:
    path = tmp_path / "profile_cache.db"
    old = sqlite3.connect(path)
    old.execute(
        "CREATE TABLE profile_cache (steamid64 TEXT PRIMARY KEY, payload_json TEXT NOT NULL, "
        "token_count INTEGER NOT NULL, token_max_log_id INTEGER NOT NULL, "
        "token_sum_imported_at INTEGER NOT NULL, built_at INTEGER NOT NULL)"
    )
    old.commit()
    old.close()
    conn = connect_profile_cache_db(path)
    init_profile_cache_db(conn)
    assert "token_chat_generation" in {
        row[1] for row in conn.execute("PRAGMA table_info(profile_cache)")
    }
    assert get_default_profile_cache(conn, SID64, (1, 1, 1, 0)) is None
    conn.close()
