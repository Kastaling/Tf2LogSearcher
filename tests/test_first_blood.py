"""First blood extraction from logs.tf JSON and raw kill events."""

from __future__ import annotations

import json
from pathlib import Path

from app.logs_tf import steamid3_to_steamid64
from app.raw_db import connect_raw_db, init_raw_db, replace_raw_events_for_log
from app.raw_log_parser import parse_raw_log
from app.stats_db import (
    _first_blood_steamid64_from_round,
    connect_stats_db,
    extract_log_stats,
    init_stats_db,
    replace_stats_for_log,
    update_log_rounds_first_blood_from_raw,
)
from app.search.search import player_profile

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"


def test_first_blood_from_logs_tf_medic_death_event() -> None:
    """logs.tf v3 rounds use medic_death with killer, not generic kill events."""
    rnd = {
        "events": [
            {"type": "pointcap", "time": 10, "team": "Red"},
            {
                "type": "medic_death",
                "time": 28,
                "team": "Red",
                "steamid": "[U:1:89717288]",
                "killer": "[U:1:1562952970]",
            },
        ],
    }
    fb = _first_blood_steamid64_from_round(rnd)
    assert fb == steamid3_to_steamid64("[U:1:1562952970]")


def test_first_blood_from_example_json_rounds() -> None:
    path = EXAMPLES_DIR / "3752295.json"
    if not path.is_file():
        return
    data = json.loads(path.read_text(encoding="utf-8"))
    rounds = data.get("rounds") or []
    assert rounds
    with_fb = sum(1 for r in rounds if _first_blood_steamid64_from_round(r))
    assert with_fb >= len(rounds) * 0.8


def test_first_blood_from_raw_kills_overrides_json(tmp_path) -> None:
    sid_a = steamid3_to_steamid64("[U:1:39734273]")
    sid_b = steamid3_to_steamid64("[U:1:39734274]")
    assert sid_a and sid_b

    stats_path = tmp_path / "stats.db"
    raw_path = tmp_path / "raw.db"
    stats_conn = connect_stats_db(stats_path)
    init_stats_db(stats_conn)
    raw_conn = connect_raw_db(raw_path)
    init_raw_db(raw_conn)

    logtext = {
        "info": {"map": "cp_process_final", "date": 1_700_000_000, "total_length": 300},
        "teams": {"Red": {"score": 1}, "Blue": {"score": 0}},
        "players": {
            "[U:1:39734273]": {"team": "Red", "kills": 1, "deaths": 0, "dmg": 100, "class_stats": []},
            "[U:1:39734274]": {"team": "Blue", "kills": 0, "deaths": 1, "dmg": 0, "class_stats": []},
        },
        "rounds": [
            {
                "duration": 120,
                "winner": "Red",
                "kills": {"Red": 1, "Blue": 0},
                "events": [],
            }
        ],
    }
    with stats_conn:
        replace_stats_for_log(stats_conn, 9001, logtext)

    a_ent = f'PlayerA<1><[U:1:39734273]><Red>'
    b_ent = f'PlayerB<1><[U:1:39734274]><Blue>'
    raw_log = "\n".join(
        [
            "L 01/01/2024 - 12:00:00: World triggered \"Round_Start\"",
            f'L 01/01/2024 - 12:00:05: "{a_ent}" killed "{b_ent}" with "scattergun"',
            'L 01/01/2024 - 12:02:00: World triggered "Round_Win" (winner "Red")',
        ]
    )
    parsed = parse_raw_log(9001, raw_log)
    with raw_conn:
        replace_raw_events_for_log(raw_conn, 9001, parsed)
        n = update_log_rounds_first_blood_from_raw(stats_conn, raw_conn, 9001)
    assert n == 1
    row = stats_conn.execute(
        "SELECT first_blood_steamid64 FROM log_rounds WHERE log_id = 9001 AND round_idx = 0"
    ).fetchone()
    stats_conn.close()
    raw_conn.close()
    assert row and row[0] == sid_a


def test_profile_first_bloods_from_medic_death(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("app.search.search.STATS_DB_PATH", tmp_path / "stats.db")
    monkeypatch.setattr("app.search.search._lookup_aliases_from_chat_db", lambda sids: {})
    sid_a = "[U:1:39734273]"
    sid_b = "[U:1:39734274]"
    sid64_a = steamid3_to_steamid64(sid_a)
    assert sid64_a

    logtext = {
        "info": {"map": "cp_process_final", "date": 1_700_000_000, "total_length": 300},
        "teams": {"Red": {"score": 1}, "Blue": {"score": 0}},
        "players": {
            sid_a: {"team": "Red", "kills": 1, "deaths": 0, "dmg": 100, "class_stats": []},
            sid_b: {"team": "Blue", "kills": 0, "deaths": 1, "dmg": 0, "class_stats": []},
        },
        "rounds": [
            {
                "duration": 120,
                "winner": "Red",
                "kills": {"Red": 1, "Blue": 0},
                "events": [{"type": "kill", "time": 5.0, "killer": sid_a}],
            }
        ],
    }
    conn = connect_stats_db(tmp_path / "stats.db")
    init_stats_db(conn)
    with conn:
        replace_stats_for_log(conn, 9002, logtext)
    conn.close()

    profile, _ = player_profile(sid64_a)
    assert profile["rounds"]["first_bloods"] >= 1
