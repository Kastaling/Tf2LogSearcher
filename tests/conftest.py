"""Shared fixtures for Tf2LogSearcher tests."""
import json
import os
import tempfile
from pathlib import Path

import pytest


def pytest_configure(config):
    """Set safe data dirs before any test module imports app.* (config reads env at import)."""
    if os.environ.get("_TF2LS_PYTEST_ENV"):
        return
    base = Path(tempfile.mkdtemp(prefix="tf2ls_test_"))
    logs = base / "logs"
    state = base / "state"
    logs.mkdir(parents=True)
    state.mkdir(parents=True)
    os.environ["LOGS_DIR"] = str(logs)
    os.environ["DOWNLOADER_STATE_DIR"] = str(state)
    os.environ["CHAT_DB_PATH"] = str(state / "chat.db")
    os.environ["AVATAR_DB_PATH"] = str(state / "avatars.db")
    os.environ["REQUEST_LOG_PATH"] = str(base / "req.csv")
    os.environ["_TF2LS_PYTEST_ENV"] = "1"


@pytest.fixture()
def logs_dir(tmp_path) -> Path:
    d = tmp_path / "logs"
    d.mkdir()
    return d


def write_log(logs_dir: Path, log_id: int, logtext: dict) -> Path:
    """Write a fake log JSON to logs_dir and return the path."""
    p = logs_dir / f"{log_id}.json"
    p.write_text(json.dumps(logtext), encoding="utf-8")
    return p


def make_player_block(
    team: str,
    kills: int = 10,
    assists: int = 2,
    deaths: int = 5,
    dmg: int = 3000,
    ubers: int = 0,
    drops: int = 0,
    headshots_hit: int = 0,
    backstabs: int = 0,
) -> dict:
    return {
        "team": team,
        "kills": kills,
        "assists": assists,
        "deaths": deaths,
        "dmg": dmg,
        "dapm": round(dmg / 300, 2),  # assume 5-min log
        "ubers": ubers,
        "drops": drops,
        "headshots_hit": headshots_hit,
        "backstabs": backstabs,
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


def make_log(
    red_steamid3: str,
    blue_steamid3: str,
    red_score: int = 3,
    blue_score: int = 1,
    map_name: str = "cp_process_final",
    date_ts: int = 1_700_000_000,
    length: int = 300,
    extra_players: dict | None = None,
) -> dict:
    """Minimal logs.tf-shaped log with two players on opposing teams."""
    players = {
        red_steamid3: make_player_block("Red"),
        blue_steamid3: make_player_block("Blue"),
    }
    if extra_players:
        players.update(extra_players)
    return {
        "info": {
            "map": map_name,
            "date": date_ts,
            "total_length": length,
            "title": f"Test log on {map_name}",
            "winner": None,
        },
        "teams": {
            "Red": {"score": red_score},
            "Blue": {"score": blue_score},
        },
        "players": players,
        "names": {k: f"Player_{k}" for k in players},
    }
