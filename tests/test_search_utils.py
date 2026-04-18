"""Unit tests for search utility helpers (no I/O, no HTTP)."""
from datetime import date, datetime, timezone

import pytest

from app.search.search import (
    _date_range_to_unix_bounds,
    _fts_phrase_query,
    _leaderboard_agg_order_clause,
    _leaderboard_resolve_spec,
    _log_in_date_range,
    _map_matches_query,
    _player_count_filter,
    _winner_team_from_log,
)


# --- leaderboard win rate scopes ---


def test_leaderboard_resolve_spec_winrate_highest_lowest():
    hi = _leaderboard_resolve_spec("winrate", "highest")
    lo = _leaderboard_resolve_spec("winrate", "lowest")
    assert "DESC" in hi["order_expr"].upper()
    assert "ASC" in lo["order_expr"].upper()
    assert hi["value_key"] == "win_rate"
    legacy = _leaderboard_resolve_spec("winrate", "total")
    assert "DESC" in legacy["order_expr"].upper()


def test_leaderboard_agg_order_clause_winrate():
    assert "DESC" in (_leaderboard_agg_order_clause("winrate", "highest") or "").upper()
    assert "ASC" in (_leaderboard_agg_order_clause("winrate", "lowest") or "").upper()


# --- _log_in_date_range ---


def test_date_range_no_bounds():
    assert _log_in_date_range(1_700_000_000, None, None) is True


def test_date_range_within():
    ts = int(datetime(2023, 11, 14, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    assert _log_in_date_range(ts, date(2023, 11, 1), date(2023, 11, 30)) is True


def test_date_range_before_from():
    ts = int(datetime(2023, 10, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    assert _log_in_date_range(ts, date(2023, 11, 1), None) is False


def test_date_range_after_to():
    ts = int(datetime(2023, 12, 1, tzinfo=timezone.utc).timestamp())
    assert _log_in_date_range(ts, None, date(2023, 11, 30)) is False


def test_date_range_invalid_ts():
    assert _log_in_date_range(None, date(2023, 1, 1), None) is False
    assert _log_in_date_range("bad", date(2023, 1, 1), None) is False
    assert _log_in_date_range(0, date(2023, 1, 1), None) is False


# --- _map_matches_query ---


def test_map_matches_empty_query():
    assert _map_matches_query("cp_process_final", None) is True
    assert _map_matches_query("cp_process_final", "") is True


def test_map_matches_substring():
    assert _map_matches_query("cp_process_final", "process") is True


def test_map_matches_case_insensitive():
    assert _map_matches_query("cp_Badlands", "badlands") is True


def test_map_no_match():
    assert _map_matches_query("cp_process_final", "granary") is False


def test_map_empty_name():
    assert _map_matches_query("", "process") is False
    assert _map_matches_query(None, "process") is False


# --- _player_count_filter ---


@pytest.mark.parametrize(
    "count,mode,expected",
    [
        (24, "hl", True),
        (18, "hl", True),
        (17, "hl", False),
        (14, "7s", True),
        (17, "7s", True),
        (13, "7s", False),
        (12, "6s", True),
        (13, "6s", True),
        (11, "6s", False),
        (4, "ud", True),
        (6, "ud", True),
        (7, "ud", False),
        (12, "hl", False),
    ],
)
def test_player_count_filter(count, mode, expected):
    assert _player_count_filter(count, mode) is expected


# --- _winner_team_from_log ---


def test_winner_from_info_field():
    log = {"info": {"winner": "Red"}, "teams": {}}
    assert _winner_team_from_log(log) == "Red"


def test_winner_inferred_from_score():
    log = {
        "info": {"winner": None},
        "teams": {"Red": {"score": 5}, "Blue": {"score": 2}},
    }
    assert _winner_team_from_log(log) == "Red"


def test_winner_blue_wins():
    log = {
        "info": {},
        "teams": {"Red": {"score": 0}, "Blue": {"score": 3}},
    }
    assert _winner_team_from_log(log) == "Blue"


def test_winner_tie_returns_none():
    log = {
        "info": {},
        "teams": {"Red": {"score": 2}, "Blue": {"score": 2}},
    }
    assert _winner_team_from_log(log) is None


def test_winner_missing_scores_returns_none():
    assert _winner_team_from_log({"info": {}, "teams": {}}) is None


# --- _fts_phrase_query ---


def test_fts_phrase_wraps_in_quotes():
    result = _fts_phrase_query("hello world")
    assert result.startswith('"')
    assert result.endswith('"')
    assert "hello world" in result


def test_fts_phrase_empty_returns_empty():
    assert _fts_phrase_query("") == ""
    assert _fts_phrase_query("   ") == ""


def test_fts_phrase_escapes_double_quotes():
    result = _fts_phrase_query('say "hi"')
    # Inner double quotes must be escaped for FTS5
    assert '""' in result


# --- _date_range_to_unix_bounds ---


def test_date_range_to_unix_both_set():
    start, end = _date_range_to_unix_bounds(date(2023, 1, 1), date(2023, 1, 31))
    assert start is not None and end is not None
    assert end > start
    # end should be 23:59:59 of Jan 31
    assert end - start > 86400 * 29


def test_date_range_to_unix_none():
    start, end = _date_range_to_unix_bounds(None, None)
    assert start is None and end is None
