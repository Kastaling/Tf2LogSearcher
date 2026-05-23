"""Tests for curated poisoned log blocklist."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.chat_db import connect_chat_db, init_chat_db, replace_chat_for_log
from app.poisoned_logs import (
    is_poisoned,
    poisoned_log_exclusion_sql,
    poisoned_log_ids,
    purge_poisoned_from_chat_db,
)
from app.search.search import chat_leaderboard_search_sqlite


@pytest.fixture()
def poisoned_file(tmp_path, monkeypatch):
    path = tmp_path / "poisoned_log_ids.json"
    monkeypatch.setattr("app.poisoned_logs.POISONED_LOG_IDS_PATH", path)
    monkeypatch.setattr("app.config.POISONED_LOG_IDS_PATH", path)
    return path


def test_loads_log_ids_from_json(poisoned_file) -> None:
    poisoned_file.write_text(
        json.dumps({"log_ids": [3944070, "5001"], "notes": {"3944070": "spam"}}),
        encoding="utf-8",
    )
    ids = poisoned_log_ids()
    assert ids == frozenset({3944070, 5001})
    assert is_poisoned(3944070)
    assert not is_poisoned(999)


def test_poisoned_exclusion_sql(poisoned_file) -> None:
    poisoned_file.write_text(json.dumps({"log_ids": [3944070, 5001]}), encoding="utf-8")
    frag = poisoned_log_exclusion_sql("cl")
    assert "cl.log_id NOT IN" in frag
    assert "3944070" in frag and "5001" in frag


def test_replace_chat_skips_poisoned_log(poisoned_file) -> None:
    poisoned_file.write_text(json.dumps({"log_ids": [9001]}), encoding="utf-8")
    db = poisoned_file.parent / "chat.db"
    conn = connect_chat_db(db)
    init_chat_db(conn)
    logtext = {
        "info": {"date": 1_700_000_000, "map": "cp_badlands"},
        "chat": [{"steamid": "[U:1:1]", "name": "A", "msg": "hello"}],
        "players": {"[U:1:1]": {"team": "Red"}},
    }
    n = replace_chat_for_log(conn, 9001, logtext)
    conn.commit()
    assert n == 0
    assert conn.execute("SELECT COUNT(*) FROM chat_logs WHERE log_id = 9001").fetchone()[0] == 0
    n2 = replace_chat_for_log(conn, 9002, logtext)
    conn.commit()
    assert n2 == 1
    conn.close()


def test_purge_removes_poisoned_chat_rows(poisoned_file) -> None:
    poisoned_file.write_text(json.dumps({"log_ids": [9001]}), encoding="utf-8")
    db = poisoned_file.parent / "chat.db"
    conn = connect_chat_db(db)
    init_chat_db(conn)
    logtext = {
        "info": {"date": 1_700_000_000, "map": "cp_badlands"},
        "chat": [{"steamid": "[U:1:1]", "name": "A", "msg": "hello"}],
        "players": {"[U:1:1]": {"team": "Red"}},
    }
    # Force insert by temporarily empty blocklist
    poisoned_file.write_text(json.dumps({"log_ids": []}), encoding="utf-8")
    replace_chat_for_log(conn, 9001, logtext)
    conn.commit()
    poisoned_file.write_text(json.dumps({"log_ids": [9001]}), encoding="utf-8")
    n = purge_poisoned_from_chat_db(conn)
    conn.commit()
    assert n == 1
    assert conn.execute("SELECT COUNT(*) FROM chat_logs").fetchone()[0] == 0
    conn.close()


def test_chat_leaderboard_excludes_poisoned_log(poisoned_file, tmp_path, monkeypatch) -> None:
    poisoned_file.write_text(json.dumps({"log_ids": [9001]}), encoding="utf-8")
    db = tmp_path / "chat.db"
    conn = connect_chat_db(db)
    init_chat_db(conn)
    good = {
        "info": {"date": 1_700_000_000, "map": "cp_badlands"},
        "chat": [
            {"steamid": "[U:1:10]", "name": "Good", "msg": "badword once"},
        ],
        "players": {"[U:1:10]": {"team": "Red"}},
    }
    bad = {
        "info": {"date": 1_700_000_100, "map": "cp_badlands"},
        "chat": [
            {"steamid": "[U:1:20]", "name": "Bad", "msg": "badword spam"},
        ]
        * 100,
        "players": {"[U:1:20]": {"team": "Blue"}},
    }
    replace_chat_for_log(conn, 9001, bad)
    replace_chat_for_log(conn, 9002, good)
    conn.commit()
    conn.close()

    rows, _total, _logs = chat_leaderboard_search_sqlite("badword", db)
    assert len(rows) == 1
    assert rows[0]["name"] == "Good"
    assert rows[0]["occurrences"] == 1
