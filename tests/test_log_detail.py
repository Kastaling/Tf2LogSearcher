"""Log detail page: cache, links, and payload builders."""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from app.log_detail import (
    SECTION_VERSIONS,
    _build_chat,
    _build_events,
    _build_raw_availability,
    _build_rounds,
    _chat_elapsed_seconds_map,
    _event_sort_key,
    _round_scores_from_json,
    compute_source_fingerprint,
    get_log_detail_payload,
    load_log_json,
)
from app.logs_tf import steamid3_to_steamid64
from app.raw_db import connect_raw_db, init_raw_db, replace_raw_events_for_log
from app.raw_log_parser import parse_raw_log
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
    assert payload["raw_availability"]["heatmaps_available"] is False


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


def test_build_rounds_team_score_and_medic_death_players() -> None:
    """logs.tf v3 rounds use team.*.score (not top-level kills); medic_death uses steamid + killer."""
    examples = Path(__file__).resolve().parents[1] / "examples"
    path = examples / "3752295.json"
    if not path.is_file():
        pytest.skip("example log missing")
    logtext = json.loads(path.read_text(encoding="utf-8"))
    out = _build_rounds(3752295, logtext)
    rounds = out.get("rounds") or []
    assert len(rounds) >= 2
    assert rounds[0]["score"] == {"Red": 0, "Blue": 1}
    assert rounds[1]["score"] == {"Red": 1, "Blue": 1}
    ev0 = rounds[0]["events"][0]
    assert ev0["type"] == "medic_death"
    assert isinstance(ev0.get("killer"), dict)
    assert ev0["killer"].get("alias")
    assert isinstance(ev0.get("victim"), dict)
    assert ev0["victim"].get("alias")
    assert ev0["killer"]["alias"] != ev0["killer"]["steamid3"]


def test_round_scores_db_fallback_when_json_has_no_scores() -> None:
    rnd = {"duration": 90, "winner": "Red", "events": []}
    db = {"red_kills": 12, "blue_kills": 8}
    assert _round_scores_from_json(rnd, db) == {"Red": 12, "Blue": 8}


def test_round_scores_prefers_team_score_over_db() -> None:
    rnd = {
        "team": {"Red": {"score": 2}, "Blue": {"score": 3}},
    }
    db = {"red_kills": 99, "blue_kills": 99}
    assert _round_scores_from_json(rnd, db) == {"Red": 2, "Blue": 3}


def test_chat_elapsed_secs_from_json_time() -> None:
    examples = Path(__file__).resolve().parents[1] / "examples"
    path = examples / "12916.json"
    if not path.is_file():
        pytest.skip("example log missing")
    logtext = json.loads(path.read_text(encoding="utf-8"))
    elapsed = _chat_elapsed_seconds_map(12916, logtext)
    assert elapsed.get(4) is not None
    out = _build_chat(12916, logtext)
    msgs = out.get("messages") or []
    assert any(m.get("elapsed_secs") is not None for m in msgs)


def test_players_include_all_class_playtime(logs_dir, monkeypatch, tmp_path) -> None:
    logtext = make_log(PLAYER_A_3, PLAYER_B_3)
    logtext["players"][PLAYER_A_3]["class_stats"] = [
        {"type": "soldier", "total_time": 400, "kills": 5, "deaths": 2, "dmg": 2000},
        {"type": "pyro", "total_time": 27, "kills": 0, "deaths": 1, "dmg": 50},
    ]
    write_log(logs_dir, 5012, logtext)
    cache_db = tmp_path / "log_detail_cache.db"
    with (
        patch("app.log_detail.LOGS_DIR", logs_dir),
        patch("app.log_detail.LOG_DETAIL_CACHE_DB_PATH", cache_db),
    ):
        payload = get_log_detail_payload(5012)
    players = {p["steamid3"]: p for p in payload["players"]["players"]}
    cpt = players[PLAYER_A_3]["class_playtime"]
    assert len(cpt) == 2
    assert cpt[0]["class"] == "soldier"
    assert cpt[1]["class"] == "pyro"


def test_raw_availability_heatmaps_gated(logs_dir, monkeypatch) -> None:
    write_log(logs_dir, 5011, make_log(PLAYER_A_3, PLAYER_B_3))
    with patch("app.log_detail.LOGS_DIR", logs_dir):
        logtext = load_log_json(5011)
    assert logtext is not None
    with patch("app.log_detail.LOG_DETAIL_HEATMAPS_ENABLED", False):
        raw = _build_raw_availability(5011, logtext)
    assert raw["heatmaps_available"] is False


def test_event_sort_key_orders_round_start_before_same_tick_kill() -> None:
    """round_start at match tick T must precede intra-round events at the same T."""
    rows = [
        {"kind": "kill", "tick": 100, "round_tick": 1},
        {"kind": "round_start", "tick": 100, "round_tick": None},
        {"kind": "round_win", "tick": 100, "round_tick": None},
    ]
    ordered = sorted(rows, key=_event_sort_key)
    assert [r["kind"] for r in ordered] == ["round_start", "kill", "round_win"]


def test_build_events_unavailable_without_raw_db(logs_dir, monkeypatch, tmp_path) -> None:
    write_log(logs_dir, 5020, make_log(PLAYER_A_3, PLAYER_B_3))
    missing_raw = tmp_path / "no_raw_events.db"
    with patch("app.log_detail.LOGS_DIR", logs_dir), patch(
        "app.log_detail.RAW_EVENTS_DB_PATH", missing_raw
    ):
        logtext = load_log_json(5020)
        assert logtext is not None
        out = _build_events(5020, logtext)
        assert out["available"] is False
        assert out["events"] == []


def test_build_events_from_raw_log(tmp_path, logs_dir, monkeypatch) -> None:
    log_id = 5021
    write_log(logs_dir, log_id, make_log(PLAYER_A_3, PLAYER_B_3))
    raw_db = tmp_path / "raw_events.db"
    raw_conn = connect_raw_db(raw_db)
    init_raw_db(raw_conn)
    sid_a = steamid3_to_steamid64(PLAYER_A_3)
    sid_b = steamid3_to_steamid64(PLAYER_B_3)
    assert sid_a and sid_b
    a_ent = f"PlayerA<1><{PLAYER_A_3}><Red>"
    b_ent = f"PlayerB<1><{PLAYER_B_3}><Blue>"
    raw_log = "\n".join(
        [
            "L 01/01/2024 - 12:00:00: World triggered \"Round_Start\"",
            f'L 01/01/2024 - 12:00:05: "{a_ent}" killed "{b_ent}" with "scattergun"',
            'L 01/01/2024 - 12:02:00: World triggered "Round_Win" (winner "Red")',
        ]
    )
    parsed = parse_raw_log(log_id, raw_log)
    with raw_conn:
        replace_raw_events_for_log(raw_conn, log_id, parsed)
    raw_conn.close()

    with patch("app.log_detail.LOGS_DIR", logs_dir), patch(
        "app.log_detail.RAW_EVENTS_DB_PATH", raw_db
    ):
        logtext = load_log_json(log_id)
        assert logtext is not None
        out = _build_events(log_id, logtext)
        assert out["available"] is True
        assert out["total_count"] >= 3
        kinds = [ev["kind"] for ev in out["events"]]
        assert "round_start" in kinds
        assert "kill" in kinds
        assert "round_win" in kinds
        kill = next(ev for ev in out["events"] if ev["kind"] == "kill")
        assert kill["attacker"]["steamid64"] == sid_a
        assert kill["attacker"]["team"] == "Red"
        assert kill["victim"]["steamid64"] == sid_b
        assert kill["victim"]["team"] == "Blue"
        assert kill["weapon"] == "Scattergun"


def test_fingerprint_changes_when_raw_events_indexed(
    logs_dir, monkeypatch, tmp_path
) -> None:
    log_id = 5022
    write_log(logs_dir, log_id, make_log(PLAYER_A_3, PLAYER_B_3))
    raw_db = tmp_path / "raw_events.db"
    with patch("app.log_detail.LOGS_DIR", logs_dir), patch(
        "app.log_detail.RAW_EVENTS_DB_PATH", raw_db
    ):
        fp_before = compute_source_fingerprint(log_id)
    assert fp_before

    raw_conn = connect_raw_db(raw_db)
    init_raw_db(raw_conn)
    parsed = parse_raw_log(
        log_id,
        'L 01/01/2024 - 12:00:00: World triggered "Round_Start"',
    )
    with raw_conn:
        replace_raw_events_for_log(raw_conn, log_id, parsed)
    raw_conn.close()

    with patch("app.log_detail.LOGS_DIR", logs_dir), patch(
        "app.log_detail.RAW_EVENTS_DB_PATH", raw_db
    ):
        fp_after = compute_source_fingerprint(log_id)
    assert fp_after
    assert fp_after != fp_before


def test_get_log_detail_payload_includes_events_section(
    logs_dir, monkeypatch, tmp_path
) -> None:
    log_id = 5023
    write_log(logs_dir, log_id, make_log(PLAYER_A_3, PLAYER_B_3))
    cache_db = tmp_path / "log_detail_cache.db"
    raw_db = tmp_path / "raw_events.db"
    raw_conn = connect_raw_db(raw_db)
    init_raw_db(raw_conn)
    parsed = parse_raw_log(
        log_id,
        'L 01/01/2024 - 12:00:00: World triggered "Round_Start"',
    )
    with raw_conn:
        replace_raw_events_for_log(raw_conn, log_id, parsed)
    raw_conn.close()

    with (
        patch("app.log_detail.LOGS_DIR", logs_dir),
        patch("app.log_detail.LOG_DETAIL_CACHE_DB_PATH", cache_db),
        patch("app.log_detail.RAW_EVENTS_DB_PATH", raw_db),
    ):
        payload = get_log_detail_payload(log_id)
        assert payload is not None
        events = payload.get("events") or {}
        assert events.get("available") is True
        assert len(events.get("events") or []) >= 1


def test_build_rounds_legacy_kills_block(logs_dir, monkeypatch) -> None:
    logtext = make_log(PLAYER_A_3, PLAYER_B_3)
    logtext["rounds"] = [
        {"duration": 90, "winner": "Red", "kills": {"Red": 5, "Blue": 3}, "events": []},
    ]
    out = _build_rounds(1, logtext)
    assert out["rounds"][0]["score"] == {"Red": 5, "Blue": 3}
