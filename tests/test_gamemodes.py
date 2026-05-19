"""Tests for competitive gamemode player-count filters."""

import pytest

from app.gamemodes import (
    gamemode_sql_filter,
    is_valid_gamemode_optional,
    normalize_gamemode,
    player_count_matches_gamemode,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("4s", "4s"),
        (" 4s ", "4s"),
        ("hl", "hl"),
        ("", ""),
        ("invalid", ""),
        ("4v4", ""),
    ],
)
def test_normalize_gamemode(raw, expected):
    assert normalize_gamemode(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", True),
        ("4s", True),
        ("hl", True),
        ("nope", False),
    ],
)
def test_is_valid_gamemode_optional(raw, expected):
    assert is_valid_gamemode_optional(raw) is expected


@pytest.mark.parametrize(
    "count,mode,expected",
    [
        (8, "4s", True),
        (7, "4s", True),
        (11, "4s", True),
        (12, "4s", False),
        (6, "4s", False),
        (6, "ud", True),
        (7, "ud", False),
    ],
)
def test_player_count_matches_4s(count, mode, expected):
    assert player_count_matches_gamemode(count, mode) is expected


def test_gamemode_sql_filter_4s():
    sql, params = gamemode_sql_filter("4s")
    assert "BETWEEN" in sql
    assert params == [7, 11]
