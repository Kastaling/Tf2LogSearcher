"""Shared log-parsing utilities used by both search and stats_db."""
from __future__ import annotations

from typing import Any


def winner_team_from_info_field(w: Any) -> str | None:
    """Normalize ``info.winner`` to Red / Blue when unambiguous."""
    if w is None:
        return None
    if isinstance(w, str):
        s = w.strip()
        if not s:
            return None
        if s in ("Red", "Blue"):
            return s
        low = s.casefold()
        if low == "red":
            return "Red"
        if low in ("blue", "blu"):
            return "Blue"
    return None


def winner_team_from_log(logtext: dict[str, Any]) -> str | None:
    """
    Winning team as Red or Blue.

    Prefer ``info.winner`` when it maps cleanly; otherwise infer from
    ``teams.Red.score`` vs ``teams.Blue.score`` (logs.tf often leaves winner null).
    Ties or missing scores => unknown (None).
    """
    info = logtext.get("info")
    if isinstance(info, dict):
        parsed = winner_team_from_info_field(info.get("winner"))
        if parsed is not None:
            return parsed
    teams = logtext.get("teams")
    rs = _team_score(teams, "Red")
    bs = _team_score(teams, "Blue")
    if rs is None or bs is None:
        return None
    if rs > bs:
        return "Red"
    if bs > rs:
        return "Blue"
    return None


def _team_score(teams: Any, key: str) -> int | None:
    if not isinstance(teams, dict):
        return None
    block = teams.get(key)
    if not isinstance(block, dict):
        return None
    raw = block.get("score")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None
