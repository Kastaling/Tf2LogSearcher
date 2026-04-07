"""Tests for head-to-head summary computed from log_match results."""
import pytest

from app.search.search import compute_head_to_head_summary


def _make_result(
    a_team,
    b_team,
    winner_team,
    a_kills=10,
    b_kills=8,
    a_dpm=200.0,
    b_dpm=180.0,
):
    """Minimal log_match result entry for two players."""
    return {
        "log_id": 1,
        "player_stats": [
            {
                "search_input": "PlayerA",
                "team": a_team,
                "kills": a_kills,
                "deaths": 5,
                "assists": 2,
                "dpm": a_dpm,
                "dmg": 3000,
                "kdr": 2.0,
                "kadr": 2.4,
                "ubers": 0,
                "drops": 0,
            },
            {
                "search_input": "PlayerB",
                "team": b_team,
                "kills": b_kills,
                "deaths": 5,
                "assists": 1,
                "dpm": b_dpm,
                "dmg": 2700,
                "kdr": 1.6,
                "kadr": 1.8,
                "ubers": 0,
                "drops": 0,
            },
        ],
        "_winner_team": winner_team,
    }


def test_hth_opposing_win_count():
    results = [
        _make_result("Red", "Blue", "Red"),  # A wins
        _make_result("Red", "Blue", "Blue"),  # B wins
        _make_result("Red", "Blue", "Red"),  # A wins
    ]
    summary = compute_head_to_head_summary(results, "PlayerA", "PlayerB")
    assert summary["opposing"]["player_a_wins"] == 2
    assert summary["opposing"]["player_b_wins"] == 1
    assert summary["opposing"]["draws"] == 0
    assert summary["opposing"]["logs_count"] == 3


def test_hth_same_team_win_count():
    results = [
        _make_result("Red", "Red", "Red"),  # together, won
        _make_result("Blue", "Blue", "Blue"),  # together, won
        _make_result("Red", "Red", "Blue"),  # together, lost
    ]
    summary = compute_head_to_head_summary(results, "PlayerA", "PlayerB")
    assert summary["same_team"]["wins"] == 2
    assert summary["same_team"]["losses"] == 1
    assert summary["same_team"]["logs_count"] == 3


def test_hth_stat_differentials_opposing():
    results = [
        _make_result("Red", "Blue", "Red", a_kills=12, b_kills=8, a_dpm=250.0, b_dpm=150.0),
    ]
    summary = compute_head_to_head_summary(results, "PlayerA", "PlayerB")
    diff = summary["opposing"]["avg_stat_diff"]
    assert diff["kills"] > 0  # A has more kills
    assert diff["dpm"] > 0  # A has higher DPM


def test_hth_no_results():
    summary = compute_head_to_head_summary([], "PlayerA", "PlayerB")
    assert summary["opposing"]["logs_count"] == 0
    assert summary["same_team"]["logs_count"] == 0


def test_hth_mixed_logs():
    results = [
        _make_result("Red", "Blue", "Red"),  # opposing
        _make_result("Red", "Red", "Red"),  # same team
        _make_result("Blue", "Red", "Blue"),  # opposing, B wins
    ]
    summary = compute_head_to_head_summary(results, "PlayerA", "PlayerB")
    assert summary["opposing"]["logs_count"] == 2
    assert summary["same_team"]["logs_count"] == 1
