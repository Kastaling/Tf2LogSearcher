"""Integration-style tests for log_match (reads from real temp files, no HTTP)."""
import json

import pytest

from app.logs_tf import steamid64_to_steamid3
from app.search.search import log_match

# Use fixed SteamID64s that map to known SteamID3s
PLAYER_A_64 = "76561198000000001"
PLAYER_B_64 = "76561198000000002"
PLAYER_C_64 = "76561198000000003"

PLAYER_A_3 = steamid64_to_steamid3(PLAYER_A_64)
PLAYER_B_3 = steamid64_to_steamid3(PLAYER_B_64)
PLAYER_C_3 = steamid64_to_steamid3(PLAYER_C_64)


def _write_log(logs_dir, log_id, logtext):
    p = logs_dir / f"{log_id}.json"
    p.write_text(json.dumps(logtext), encoding="utf-8")
    return p


def _make_player(team, kills=10, deaths=5, assists=2, dmg=3000):
    return {
        "team": team,
        "kills": kills,
        "deaths": deaths,
        "assists": assists,
        "dmg": dmg,
        "dapm": dmg / 300,
        "ubers": 0,
        "drops": 0,
        "headshots_hit": 0,
        "backstabs": 0,
        "class_stats": [
            {
                "type": "soldier",
                "total_time": 300,
                "kills": kills,
                "assists": assists,
                "deaths": deaths,
                "dmg": dmg,
            }
        ],
    }


def _make_log(
    a3,
    b3,
    red_score=3,
    blue_score=1,
    map_name="cp_process_final",
    date_ts=1_700_000_000,
    a_team="Red",
    b_team="Blue",
):
    return {
        "info": {
            "map": map_name,
            "date": date_ts,
            "total_length": 300,
            "title": "Test",
            "winner": None,
        },
        "teams": {"Red": {"score": red_score}, "Blue": {"score": blue_score}},
        "players": {a3: _make_player(a_team), b3: _make_player(b_team)},
        "names": {a3: "PlayerA", b3: "PlayerB"},
    }


def test_log_match_both_players_present(logs_dir, monkeypatch):
    log = _make_log(PLAYER_A_3, PLAYER_B_3)
    _write_log(logs_dir, 1001, log)

    monkeypatch.setattr(
        "app.search.search.get_log_list_for_player",
        lambda sid64: [1001],
    )

    results, total, matched_ids, h2h = log_match([PLAYER_A_64, PLAYER_B_64], logs_dir)
    assert total == 1
    assert results[0]["log_id"] == 1001
    assert len(results[0]["player_stats"]) == 2
    assert 1001 in matched_ids
    assert h2h is not None
    assert "_winner_team" not in results[0]


def test_log_match_missing_player_excluded(logs_dir, monkeypatch):
    # Log only has player A, not B
    log = _make_log(PLAYER_A_3, PLAYER_C_3)
    _write_log(logs_dir, 1002, log)

    monkeypatch.setattr(
        "app.search.search.get_log_list_for_player",
        lambda sid64: [1002],
    )

    results, total, _, _ = log_match([PLAYER_A_64, PLAYER_B_64], logs_dir)
    assert total == 0


def test_log_match_map_filter(logs_dir, monkeypatch):
    log_process = _make_log(PLAYER_A_3, PLAYER_B_3, map_name="cp_process_final")
    log_granary = _make_log(
        PLAYER_A_3,
        PLAYER_B_3,
        map_name="cp_granary_pro_rc8",
        date_ts=1_700_100_000,
    )
    _write_log(logs_dir, 2001, log_process)
    _write_log(logs_dir, 2002, log_granary)

    monkeypatch.setattr(
        "app.search.search.get_log_list_for_player",
        lambda sid64: [2001, 2002],
    )

    results, total, _, _ = log_match(
        [PLAYER_A_64, PLAYER_B_64], logs_dir, map_query="granary"
    )
    assert total == 1
    assert results[0]["map"] == "cp_granary_pro_rc8"


def test_log_match_empty_steamids(logs_dir):
    results, total, matched, h2h = log_match([], logs_dir)
    assert results == []
    assert total == 0
    assert matched == frozenset()
    assert h2h is None


def test_log_match_player_stats_fields(logs_dir, monkeypatch):
    log = _make_log(PLAYER_A_3, PLAYER_B_3)
    _write_log(logs_dir, 3001, log)

    monkeypatch.setattr(
        "app.search.search.get_log_list_for_player",
        lambda sid64: [3001],
    )

    results, _, _, _ = log_match([PLAYER_A_64, PLAYER_B_64], logs_dir)
    row = results[0]["player_stats"][0]
    for field in (
        "alias",
        "team",
        "kills",
        "deaths",
        "assists",
        "kdr",
        "kadr",
        "dpm",
        "dmg",
    ):
        assert field in row, f"Missing field: {field}"
