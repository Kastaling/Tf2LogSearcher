"""Whitelisted competitive gamemode filters by player count in a log."""

from __future__ import annotations

from typing import Any

# Internal API / URL keys. UI labels live in static HTML (display name for 4v4 is "4s").
VALID_GAMEMODES: frozenset[str] = frozenset({"hl", "7s", "6s", "ud", "4s"})
VALID_GAMEMODES_OPTIONAL: frozenset[str] = frozenset({"", *VALID_GAMEMODES})


def normalize_gamemode(gamemode: str | None) -> str:
    """Return whitelisted gamemode key, or '' if unknown or empty."""
    gm = (gamemode or "").strip()
    return gm if gm in VALID_GAMEMODES else ""


def is_valid_gamemode_optional(gamemode: str | None) -> bool:
    """True for '' (all modes) or a known gamemode key."""
    return (gamemode or "").strip() in VALID_GAMEMODES_OPTIONAL


def player_count_matches_gamemode(player_count: int, gamemode: str) -> bool:
    """True if player_count (len(names) / logs.num_players) matches gamemode."""
    gm = (gamemode or "").strip()
    if gm == "hl":
        return player_count >= 18
    if gm == "7s":
        return 14 <= player_count <= 17
    if gm == "6s":
        return 12 <= player_count <= 13
    if gm == "4s":
        return 7 <= player_count <= 11
    if gm == "ud":
        return 4 <= player_count <= 6
    return False


def gamemode_sql_filter(gamemode: str) -> tuple[str, list[Any]]:
    """
    Parameterized AND clause for logs aliased as ``l`` (stats DB).
    Unknown or empty gamemode → no filter.
    """
    gm = (gamemode or "").strip()
    if gm == "hl":
        return " AND l.num_players >= ?", [18]
    if gm == "7s":
        return " AND l.num_players BETWEEN ? AND ?", [14, 17]
    if gm == "6s":
        return " AND l.num_players BETWEEN ? AND ?", [12, 13]
    if gm == "4s":
        return " AND l.num_players BETWEEN ? AND ?", [7, 11]
    if gm == "ud":
        return " AND l.num_players BETWEEN ? AND ?", [4, 6]
    return "", []
