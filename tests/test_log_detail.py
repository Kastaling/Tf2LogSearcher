"""Log detail page: cache, links, and payload builders."""
from __future__ import annotations

import json
import time
from unittest.mock import patch

import pytest

from app.log_detail import (
    SECTION_VERSIONS,
    compute_source_fingerprint,
    get_log_detail_payload,
    load_log_json,
)
from app.log_detail_cache_db import (
    connect_log_detail_cache_db,
    get_cached_section,
    init_log_detail_cache_db,
    set_cached_section,
)
from app.log_links import external_log_url, log_url_for_id, parse_log_id
from tests.conftest import make_log, write_log

PLAYER_A_3 = "[U:1:39734273]"
PLAYER_B_3 = "[U:1:39734274]"


def test_parse_log_id() -> None:
    assert parse_log_id("12345") == 12345
    assert parse_log_id(99) == 99
    assert parse_log_id("12abc") is None
    assert parse_log_id("../1") is None


def test_log_url_for_id_modes() -> None:
    with patch("app.log_links.LOG_DETAIL_LINK_MODE", "external"):
        assert log_url_for_id(42) == external_log_url(42)
    with patch("app.log_links.LOG_DETAIL_LINK_MODE", "internal"):
        assert log_url_for_id(42) == "/log/42"


def test_compute_source_fingerprint_missing_log(logs_dir, monkeypatch) -> None:
    monkeypatch.setenv("LOGS_DIR", str(logs_dir))
    with patch("app.log_detail.LOGS_DIR", logs_dir):
        assert compute_source_fingerprint(9999) is None


def test_get_log_detail_payload_minimal_log(logs_dir, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("LOGS_DIR", str(logs_dir))
    cache_db = tmp_path / "log_detail_cache.db"
    monkeypatch.setenv("LOG_DETAIL_CACHE_DB_PATH", str(cache_db))

    logtext = make_log(PLAYER_A_3, PLAYER_B_3)
    logtext["chat"] = [
        {"steamid": PLAYER_A_3, "name": "A", "msg": "gg"},
    ]
    logtext["rounds"] = [
        {"duration": 90, "winner": "Red", "kills": {"Red": 5, "Blue": 3}, "events": []},
    ]
    logtext["killstreaks"] = [{"steamid": PLAYER_A_3, "killstreak": 5}]
    write_log(logs_dir, 5001, logtext)

    with (
        patch("app.log_detail.LOGS_DIR", logs_dir),
        patch("app.log_detail.LOG_DETAIL_CACHE_DB_PATH", cache_db),
    ):
        payload = get_log_detail_payload(5001)

    assert payload is not None
    assert payload["log_id"] == 5001
    assert payload["summary"]["map"] == "cp_process_final"
    assert len(payload["players"]["players"]) == 2
    assert len(payload["rounds"]["rounds"]) == 1
    assert payload["chat"]["messages"][0]["msg"] == "gg"
    assert payload["killstreaks"]["killstreaks"][0]["streak"] == 5


def test_section_cache_invalidates_on_json_change(logs_dir, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("LOGS_DIR", str(logs_dir))
    cache_db = tmp_path / "log_detail_cache.db"
    write_log(logs_dir, 5002, make_log(PLAYER_A_3, PLAYER_B_3))

    with (
        patch("app.log_detail.LOGS_DIR", logs_dir),
        patch("app.log_detail.LOG_DETAIL_CACHE_DB_PATH", cache_db),
    ):
        fp1 = compute_source_fingerprint(5002)
        assert fp1
        conn = connect_log_detail_cache_db(cache_db)
        init_log_detail_cache_db(conn)
        set_cached_section(
            conn,
            5002,
            "summary",
            SECTION_VERSIONS["summary"],
            fp1,
            {"stale": True},
        )
        assert get_cached_section(
            conn, 5002, "summary", SECTION_VERSIONS["summary"], fp1
        ) == {"stale": True}
        conn.close()

        path = logs_dir / "5002.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        data["info"]["title"] = "Updated title"
        path.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(0.01)

        fp2 = compute_source_fingerprint(5002)
        assert fp2 != fp1

        conn2 = connect_log_detail_cache_db(cache_db)
        assert get_cached_section(
            conn2, 5002, "summary", SECTION_VERSIONS["summary"], fp2
        ) is None
        conn2.close()

        payload = get_log_detail_payload(5002)
        assert payload["summary"]["title"] == "Updated title"


def test_section_version_bump_misses_cache(logs_dir, monkeypatch, tmp_path) -> None:
    cache_db = tmp_path / "log_detail_cache.db"
    write_log(logs_dir, 5003, make_log(PLAYER_A_3, PLAYER_B_3))
    with patch("app.log_detail.LOGS_DIR", logs_dir):
        fp = compute_source_fingerprint(5003)
    assert fp

    conn = connect_log_detail_cache_db(cache_db)
    init_log_detail_cache_db(conn)
    set_cached_section(conn, 5003, "teams", "teams:v0", fp, {"old": 1})
    assert get_cached_section(conn, 5003, "teams", SECTION_VERSIONS["teams"], fp) is None
    conn.close()


def test_fingerprint_computed_after_json_load(logs_dir, monkeypatch, tmp_path) -> None:
    """Cache fingerprint must reflect sources after JSON is read, not before."""
    cache_db = tmp_path / "log_detail_cache.db"
    write_log(logs_dir, 5010, make_log(PLAYER_A_3, PLAYER_B_3))
    order: list[str] = []
    real_load = load_log_json
    real_fp = compute_source_fingerprint

    def load_track(lid: int):
        order.append("load")
        return real_load(lid)

    def fp_track(lid: int):
        order.append("fp")
        return real_fp(lid)

    with (
        patch("app.log_detail.LOGS_DIR", logs_dir),
        patch("app.log_detail.LOG_DETAIL_CACHE_DB_PATH", cache_db),
        patch("app.log_detail.load_log_json", side_effect=load_track),
        patch("app.log_detail.compute_source_fingerprint", side_effect=fp_track),
    ):
        get_log_detail_payload(5010)

    assert order[:2] == ["load", "fp"]


def test_load_log_json(logs_dir, monkeypatch) -> None:
    write_log(logs_dir, 77, make_log(PLAYER_A_3, PLAYER_B_3))
    with patch("app.log_detail.LOGS_DIR", logs_dir):
        lt = load_log_json(77)
    assert lt is not None
    assert "players" in lt
