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


def test_hth_unmatched_search_input_is_skipped():
    """A result whose player_stats labels don't match A or B should be silently ignored."""
    results = [
        _make_result("Red", "Blue", "Red"),  # normal, counted
        {
            "log_id": 99,
            "player_stats": [
                {
                    "search_input": "SomeOtherPlayer",
                    "team": "Red",
                    "kills": 5,
                    "deaths": 3,
                    "assists": 1,
                    "dpm": 100.0,
                    "dmg": 1000,
                    "kdr": 1.6,
                    "kadr": 2.0,
                    "ubers": 0,
                    "drops": 0,
                },
            ],
            "_winner_team": "Red",
        },
    ]
    summary = compute_head_to_head_summary(results, "PlayerA", "PlayerB")
    # Only the first result should be counted; the second has no matching labels
    assert summary["opposing"]["logs_count"] == 1
    assert summary["same_team"]["logs_count"] == 0


def test_hth_unexpected_winner_value_treated_as_draw():
    """An unrecognised winner string should not raise and should count as a draw."""
    results = [
        {
            "log_id": 1,
            "player_stats": [
                {
                    "search_input": "PlayerA",
                    "team": "Red",
                    "kills": 10,
                    "deaths": 5,
                    "assists": 2,
                    "dpm": 200.0,
                    "dmg": 3000,
                    "kdr": 2.0,
                    "kadr": 2.4,
                    "ubers": 0,
                    "drops": 0,
                },
                {
                    "search_input": "PlayerB",
                    "team": "Blue",
                    "kills": 8,
                    "deaths": 5,
                    "assists": 1,
                    "dpm": 180.0,
                    "dmg": 2700,
                    "kdr": 1.6,
                    "kadr": 1.8,
                    "ubers": 0,
                    "drops": 0,
                },
            ],
            "_winner_team": "Stalemate",  # unexpected value
        },
    ]
    summary = compute_head_to_head_summary(results, "PlayerA", "PlayerB")
    assert summary["opposing"]["logs_count"] == 1
    # Neither Red nor Blue won, so it should not increment a_wins or b_wins
    assert summary["opposing"]["player_a_wins"] == 0
    assert summary["opposing"]["player_b_wins"] == 0
    # Depending on implementation it may count as a draw — either 0 or 1 is acceptable,
    # but it must not raise an exception.
    assert (
        summary["opposing"]["draws"]
        + summary["opposing"]["player_a_wins"]
        + summary["opposing"]["player_b_wins"]
        == 1
    )
